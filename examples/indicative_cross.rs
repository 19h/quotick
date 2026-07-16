//! Risk-managed call-auction collection, publication, and deterministic uncross.

mod support;

use quotick::auction::{AuctionAllocationPolicy, AuctionOrderConstraint, AuctionPricePolicy};
use quotick::auction_book::{
    CallAuctionOrder, CallAuctionRemainderPolicy, CallAuctionSelfTradePolicy,
    CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionCommand, CallAuctionCommandOutcome, CallAuctionEventKind,
    CallAuctionExecutionReport, CallAuctionIndicativeCommand, CallAuctionPhase,
    CallAuctionPhaseControl, CallAuctionRejectReason, CallAuctionSubmitOrder,
    CallAuctionUncrossCommand,
};
use quotick::auction_market_data::{
    CallAuctionMarketDataBatch, CallAuctionMarketDataLimits, CallAuctionMarketDataPublisher,
    CallAuctionMarketDataReplayBuffer, CallAuctionMarketDataReplica,
};
use quotick::auction_risk::CallAuctionRiskManagedEngine;
use quotick::{AuctionId, CommandId, OrderId, Price, Quantity, Side};

use support::{account, auction_risk_limits, definition, profile, timestamp};

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
    reason = "the auction command factory keeps every routing dimension explicit"
)]
fn order(
    definition: quotick::instrument::InstrumentDefinition,
    command_id: u64,
    auction_id: AuctionId,
    order_id: u64,
    account_id: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
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
            constraint,
            Quantity::new(quantity).unwrap(),
            quotick::auction::AuctionPriorityClass::HIGHEST,
        ),
        received_at: timestamp(command_id),
    })
}

fn indicative(
    definition: quotick::instrument::InstrumentDefinition,
    command_id: u64,
    auction_id: AuctionId,
    expected_revision: u64,
    price_band: quotick::auction::AuctionPriceBand,
) -> CallAuctionCommand {
    CallAuctionCommand::Indicative(CallAuctionIndicativeCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        auction_id,
        expected_phase_revision: expected_revision,
        price_band,
        reference_price: Price::from_raw(10_100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        received_at: timestamp(command_id),
    })
}

fn execute(
    managed: &mut CallAuctionRiskManagedEngine,
    publisher: &mut CallAuctionMarketDataPublisher,
    replica: &mut CallAuctionMarketDataReplica,
    replay: &mut CallAuctionMarketDataReplayBuffer,
    command: CallAuctionCommand,
) -> (CallAuctionExecutionReport, CallAuctionMarketDataBatch) {
    let report = managed.submit(command).unwrap();
    let batch = publisher
        .publish(command, &report, managed.engine())
        .unwrap();
    replay.push_batch(&batch).unwrap();
    replica.apply_batch(&batch).unwrap();
    (report, batch)
}

#[allow(
    clippy::too_many_lines,
    reason = "one chronological example keeps collection, risk, publication, and uncross contiguous"
)]
fn main() {
    let definition = definition("INDICATIVE-CROSS");
    let auction_id = AuctionId::new(1).unwrap();
    let mut managed =
        CallAuctionRiskManagedEngine::try_with_limits(definition, auction_risk_limits()).unwrap();
    for account_id in [11, 12, 21, 22] {
        managed
            .register_account(account(account_id), profile(0, 100))
            .unwrap();
    }
    managed
        .register_account(account(99), profile(0, 2))
        .unwrap();

    let mut publisher = CallAuctionMarketDataPublisher::from_engine(managed.engine()).unwrap();
    let replica_limits = CallAuctionMarketDataLimits::from_engine(managed.engine().limits()).spec();
    let mut replica =
        CallAuctionMarketDataReplica::try_with_limits(definition, replica_limits).unwrap();
    let initial = publisher.snapshot();
    replica.apply_snapshot(&initial).unwrap();
    let mut replay = CallAuctionMarketDataReplayBuffer::try_new(
        definition.instrument_id(),
        definition.version(),
        initial.as_of_sequence(),
        replica_limits.max_batch_updates,
    )
    .unwrap();
    let price_band = managed.engine().book().instrument_price_band();

    execute(
        &mut managed,
        &mut publisher,
        &mut replica,
        &mut replay,
        phase(definition, 1, auction_id, 0, CallAuctionPhase::Collecting),
    );
    for command in [
        order(
            definition,
            2,
            auction_id,
            1,
            11,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(10_100)),
            10,
        ),
        order(
            definition,
            3,
            auction_id,
            2,
            12,
            Side::Buy,
            AuctionOrderConstraint::Market,
            2,
        ),
        order(
            definition,
            4,
            auction_id,
            3,
            21,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(10_000)),
            6,
        ),
        order(
            definition,
            5,
            auction_id,
            4,
            22,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(10_200)),
            8,
        ),
    ] {
        execute(
            &mut managed,
            &mut publisher,
            &mut replica,
            &mut replay,
            command,
        );
    }

    let (indicative_report, _) = execute(
        &mut managed,
        &mut publisher,
        &mut replica,
        &mut replay,
        indicative(definition, 6, auction_id, 1, price_band),
    );
    let CallAuctionEventKind::IndicativePublished(indicative_state) =
        indicative_report.events[0].kind
    else {
        panic!("indicative command must publish one indicative state");
    };
    let indicative_clearing = indicative_state.clearing().unwrap();
    assert_eq!(indicative_clearing.price(), Price::from_raw(10_100));
    assert_eq!(indicative_clearing.executable_quantity(), 6);
    assert_eq!(managed.engine().last_indicative(), Some(indicative_state));
    assert_eq!(replica.indicative(), Some(indicative_state));

    let (risk_rejection, _) = execute(
        &mut managed,
        &mut publisher,
        &mut replica,
        &mut replay,
        order(
            definition,
            7,
            auction_id,
            5,
            99,
            Side::Buy,
            AuctionOrderConstraint::Market,
            3,
        ),
    );
    assert_eq!(
        risk_rejection.outcome,
        CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::RiskOrderQuantityLimit)
    );
    assert_eq!(managed.engine().last_indicative(), Some(indicative_state));
    assert_eq!(replica.indicative(), Some(indicative_state));

    execute(
        &mut managed,
        &mut publisher,
        &mut replica,
        &mut replay,
        phase(definition, 8, auction_id, 1, CallAuctionPhase::Frozen),
    );
    assert_eq!(managed.engine().last_indicative(), None);
    assert_eq!(replica.indicative(), None);

    let (frozen_indicative_report, _) = execute(
        &mut managed,
        &mut publisher,
        &mut replica,
        &mut replay,
        indicative(definition, 9, auction_id, 2, price_band),
    );
    let CallAuctionEventKind::IndicativePublished(frozen_indicative_state) =
        frozen_indicative_report.events[0].kind
    else {
        panic!("frozen indicative command must publish one indicative state");
    };
    assert_eq!(
        frozen_indicative_state.clearing(),
        indicative_state.clearing()
    );
    assert_eq!(
        managed.engine().last_indicative(),
        Some(frozen_indicative_state)
    );
    assert_eq!(replica.indicative(), Some(frozen_indicative_state));

    let uncross = CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
        command_id: CommandId::new(10).unwrap(),
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        auction_id,
        expected_phase_revision: 2,
        price_band,
        reference_price: Price::from_raw(10_100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        uncross_policy: CallAuctionUncrossPolicy::new(
            AuctionAllocationPolicy::PriceTime,
            CallAuctionRemainderPolicy::RetainAll,
            CallAuctionSelfTradePolicy::Permit,
        ),
        received_at: timestamp(10),
    });
    let (report, _) = execute(
        &mut managed,
        &mut publisher,
        &mut replica,
        &mut replay,
        uncross,
    );

    let trades: Vec<_> = report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            CallAuctionEventKind::Trade(trade) => Some(trade),
            _ => None,
        })
        .collect();
    assert_eq!(trades.len(), 2);
    assert_eq!(
        trades
            .iter()
            .map(|trade| trade.quantity().lots())
            .sum::<u64>(),
        6
    );
    let clearing = report
        .events
        .iter()
        .find_map(|event| match event.kind {
            CallAuctionEventKind::UncrossCompleted { clearing, .. } => Some(clearing),
            _ => None,
        })
        .unwrap();
    assert_eq!(clearing.price(), Price::from_raw(10_100));
    assert_eq!(clearing.executable_quantity(), 6);

    assert_eq!(
        managed
            .risk()
            .snapshot(account(11))
            .unwrap()
            .position_lots(),
        4
    );
    assert_eq!(
        managed
            .risk()
            .snapshot(account(12))
            .unwrap()
            .position_lots(),
        2
    );
    assert_eq!(
        managed
            .risk()
            .snapshot(account(21))
            .unwrap()
            .position_lots(),
        -6
    );
    assert_eq!(
        managed
            .risk()
            .snapshot(account(22))
            .unwrap()
            .position_lots(),
        0
    );
    assert_eq!(managed.risk().reservation_count(), 2);
    assert_eq!(
        managed.engine().phase_snapshot().phase(),
        CallAuctionPhase::Closed
    );
    assert_eq!(managed.engine().last_indicative(), None);
    assert_eq!(replica.indicative(), None);
    assert_eq!(replica.phase(), managed.engine().phase_snapshot());
    assert_eq!(
        replica.limit_depth(Side::Buy, usize::MAX),
        managed.engine().book().limit_depth(Side::Buy, usize::MAX)
    );
    assert_eq!(
        replica.limit_depth(Side::Sell, usize::MAX),
        managed.engine().book().limit_depth(Side::Sell, usize::MAX)
    );

    managed.validate().unwrap();
    publisher.validate_against(managed.engine()).unwrap();
    replica.validate().unwrap();

    let replay_page = replay
        .replay_batches_after(initial.as_of_sequence(), replay.maximum_updates())
        .unwrap();
    let replayed_batches = replay_page.len();
    let mut repaired =
        CallAuctionMarketDataReplica::try_with_limits(definition, replica_limits).unwrap();
    repaired.apply_snapshot(&initial).unwrap();
    for batch in replay_page {
        repaired.apply_replay_batch(&batch).unwrap();
    }
    assert_eq!(repaired.last_sequence(), replica.last_sequence());
    assert_eq!(
        repaired.last_command_sequence(),
        replica.last_command_sequence()
    );
    assert_eq!(
        repaired.limit_depth(Side::Buy, usize::MAX),
        replica.limit_depth(Side::Buy, usize::MAX)
    );
    assert_eq!(
        repaired.limit_depth(Side::Sell, usize::MAX),
        replica.limit_depth(Side::Sell, usize::MAX)
    );
    replay.validate().unwrap();
    repaired.validate().unwrap();

    println!(
        "clearing_price={} executed_lots={} trade_pairs={} retained_orders={} replayed_batches={}",
        clearing.price().raw(),
        clearing.executable_quantity(),
        trades.len(),
        managed.engine().book().active_order_count(),
        replayed_batches
    );
}
