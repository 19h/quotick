//! Durable call-auction checkpoint recovery through freeze and uncross.

mod support;

use std::fs;

use quotick::auction::{AuctionAllocationPolicy, AuctionOrderConstraint, AuctionPricePolicy};
use quotick::auction_book::{
    CallAuctionOrder, CallAuctionRemainderPolicy, CallAuctionSelfTradePolicy,
    CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionCommand, CallAuctionEventKind, CallAuctionExecutionReport, CallAuctionPhase,
    CallAuctionPhaseControl, CallAuctionSubmitOrder, CallAuctionUncrossCommand,
};
use quotick::durable_auction::DurableCallAuctionEngine;
use quotick::instrument::InstrumentDefinition;
use quotick::ledger::{CallAuctionSettlement, Ledger};
use quotick::snapshot::{CheckpointSlot, SnapshotOptions};
use quotick::{
    AccountingDate, AuctionId, CommandId, OrderId, Price, Quantity, Side, TransactionId,
};

use support::{
    ScratchArea, account, auction_engine_limits, definition, journal_options, timestamp,
};

fn phase(
    definition: quotick::instrument::InstrumentDefinition,
    command_id: u64,
    auction_id: AuctionId,
    expected_revision: u64,
    target: CallAuctionPhase,
) -> CallAuctionCommand {
    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        auction_id,
        expected_phase_revision: expected_revision,
        target_phase: target,
        received_at: timestamp(command_id),
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "the auction command fixture keeps every routing field explicit"
)]
fn order(
    definition: quotick::instrument::InstrumentDefinition,
    command_id: u64,
    auction_id: AuctionId,
    order_id: u64,
    account_id: u64,
    side: Side,
    price: i64,
    quantity: u64,
) -> CallAuctionCommand {
    CallAuctionCommand::Submit(CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id,
        expected_phase_revision: 1,
        order: CallAuctionOrder::new(
            OrderId::new(order_id).unwrap(),
            account(account_id),
            definition.instrument_id(),
            definition.version(),
            side,
            AuctionOrderConstraint::Limit(Price::from_raw(price)),
            Quantity::new(quantity).unwrap(),
            quotick::auction::AuctionPriorityClass::HIGHEST,
        ),
        received_at: timestamp(command_id),
    })
}

fn settle_report(
    report: &CallAuctionExecutionReport,
    definition: InstrumentDefinition,
) -> (usize, usize) {
    let trade_count = report
        .events
        .iter()
        .filter(|event| matches!(event.kind, CallAuctionEventKind::Trade(_)))
        .count();
    let settlement = CallAuctionSettlement::from_report(
        vec![TransactionId::new(1).unwrap()],
        AccountingDate::UNIX_EPOCH,
        timestamp(7),
        report,
        definition,
    )
    .unwrap();
    let mut ledger = Ledger::new();
    let receipt = ledger.settle_call_auction(settlement).unwrap();
    assert_eq!(receipt.transaction_count(), trade_count);
    ledger.validate().unwrap();
    (trade_count, receipt.transaction_count())
}

fn main() {
    let area = ScratchArea::new("auction-restart");
    let wal = area.join("auction.qwal");
    let checkpoint = area.join("auction.qsnp");
    let definition = definition("AUCTION-RESTART");
    let auction_id = AuctionId::new(1).unwrap();

    let mut durable = DurableCallAuctionEngine::open_with_limits(
        &wal,
        definition,
        auction_engine_limits(),
        journal_options(),
    )
    .unwrap();
    durable
        .submit(phase(
            definition,
            1,
            auction_id,
            0,
            CallAuctionPhase::Collecting,
        ))
        .unwrap();
    for command in [
        order(definition, 2, auction_id, 1, 11, Side::Buy, 10_100, 7),
        order(definition, 3, auction_id, 2, 21, Side::Sell, 10_000, 5),
        order(definition, 4, auction_id, 3, 22, Side::Sell, 10_200, 4),
    ] {
        durable.submit(command).unwrap();
    }

    let cutover = durable
        .compact_to_checkpoint(&checkpoint, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    assert_eq!(cutover.snapshot().generation(), 9);

    durable
        .submit(phase(
            definition,
            5,
            auction_id,
            1,
            CallAuctionPhase::Frozen,
        ))
        .unwrap();
    let uncross = CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
        command_id: CommandId::new(6).unwrap(),
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        auction_id,
        expected_phase_revision: 2,
        price_band: durable.engine().book().instrument_price_band(),
        reference_price: Price::from_raw(10_100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        uncross_policy: CallAuctionUncrossPolicy::new(
            AuctionAllocationPolicy::PriceTime,
            CallAuctionRemainderPolicy::RetainAll,
            CallAuctionSelfTradePolicy::Permit,
        ),
        received_at: timestamp(6),
    });
    let report = durable.submit(uncross).unwrap();
    let (trade_count, settled_transactions) = settle_report(&report, definition);
    assert_eq!(trade_count, 1);
    durable.close().unwrap();

    let mut recovered = DurableCallAuctionEngine::open_with_checkpoint_and_limits(
        &wal,
        &checkpoint,
        definition,
        auction_engine_limits(),
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    let recovery = recovered.recovery();
    assert_eq!(recovery.checkpointed_commands, 4);
    assert_eq!(recovery.replayed_commands, 2);
    assert_eq!(
        recovered.engine().phase_snapshot().phase(),
        CallAuctionPhase::Closed
    );
    assert_eq!(recovered.engine().book().active_order_count(), 2);
    recovered.engine().validate().unwrap();

    let bytes_before_retry = fs::metadata(&wal).unwrap().len();
    assert!(recovered.submit(uncross).unwrap().replayed);
    let bytes_after_retry = fs::metadata(&wal).unwrap().len();
    assert_eq!(bytes_after_retry, bytes_before_retry);
    let retained_orders = recovered.engine().book().active_order_count();
    recovered.close().unwrap();

    println!(
        "checkpointed_commands={} replayed_suffix={} trades={} settled_transactions={} retained_orders={} retry_wal_growth={}",
        recovery.checkpointed_commands,
        recovery.replayed_commands,
        trade_count,
        settled_transactions,
        retained_orders,
        bytes_after_retry - bytes_before_retry
    );
}
