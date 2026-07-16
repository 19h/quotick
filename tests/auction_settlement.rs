use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::auction::{
    AuctionAllocationPolicy, AuctionClearing, AuctionOrderConstraint, AuctionPricePolicy,
};
use quotick::auction_book::{
    CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder, CallAuctionRemainderPolicy,
    CallAuctionSelfTradePolicy, CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionCommand, CallAuctionEngine, CallAuctionEngineLimits, CallAuctionEngineLimitsSpec,
    CallAuctionEventKind, CallAuctionExecutionReport, CallAuctionPhase, CallAuctionPhaseControl,
    CallAuctionSubmitOrder, CallAuctionUncrossCommand,
};
use quotick::durable_ledger::DurableLedger;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions, JournalReader, RecordKind, RecoveryMode};
use quotick::ledger::{
    CallAuctionSettlement, JournalEntry, Ledger, LedgerError, LedgerRecord, Posting,
};
use quotick::snapshot::{CheckpointSlot, SnapshotFile, SnapshotOptions};
use quotick::{
    AccountId, AccountingDate, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion,
    OrderId, Price, Quantity, Side, TimestampNs, TransactionId,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new() -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-auction-settlement-{}-{nonce}.qwal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).unwrap()
}

fn instrument() -> InstrumentId {
    InstrumentId::new(17).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(4).unwrap()
}

fn definition_with_identity(
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id,
        version: instrument_version,
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("AUCTION-SETTLE").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(0), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 10,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Halted,
    })
    .unwrap()
}

fn definition() -> InstrumentDefinition {
    definition_with_identity(instrument(), version())
}

fn engine() -> CallAuctionEngine {
    let book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: 8,
        max_price_levels_per_side: 8,
        max_accepted_order_ids: 16,
        max_prepared_uncrosses: 2,
    })
    .unwrap();
    let limits = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: 16,
        terminal_command_reserve: 10,
        max_retained_events: 40,
        max_report_events: 17,
    })
    .unwrap();
    CallAuctionEngine::try_with_limits(definition(), limits).unwrap()
}

fn phase(command_id: u64, expected_revision: u64, target: CallAuctionPhase) -> CallAuctionCommand {
    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: AuctionId::new(1).unwrap(),
        expected_phase_revision: expected_revision,
        target_phase: target,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn order(order_id: u64, account_id: u64, side: Side, quantity: u64) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        account(account_id),
        instrument(),
        version(),
        side,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        Quantity::new(quantity).unwrap(),
        quotick::auction::AuctionPriorityClass::HIGHEST,
    )
}

fn uncross_report(
    buys: &[(u64, u64, u64)],
    sells: &[(u64, u64, u64)],
) -> CallAuctionExecutionReport {
    let mut engine = engine();
    engine
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    let mut command_id = 2_u64;
    for &(order_id, account_id, quantity) in buys {
        engine
            .submit(CallAuctionCommand::Submit(CallAuctionSubmitOrder {
                command_id: CommandId::new(command_id).unwrap(),
                auction_id: AuctionId::new(1).unwrap(),
                expected_phase_revision: 1,
                order: order(order_id, account_id, Side::Buy, quantity),
                received_at: TimestampNs::from_unix_nanos(command_id),
            }))
            .unwrap();
        command_id += 1;
    }
    for &(order_id, account_id, quantity) in sells {
        engine
            .submit(CallAuctionCommand::Submit(CallAuctionSubmitOrder {
                command_id: CommandId::new(command_id).unwrap(),
                auction_id: AuctionId::new(1).unwrap(),
                expected_phase_revision: 1,
                order: order(order_id, account_id, Side::Sell, quantity),
                received_at: TimestampNs::from_unix_nanos(command_id),
            }))
            .unwrap();
        command_id += 1;
    }
    engine
        .submit(phase(command_id, 1, CallAuctionPhase::Frozen))
        .unwrap();
    command_id += 1;
    engine
        .submit(CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
            command_id: CommandId::new(command_id).unwrap(),
            instrument_id: instrument(),
            instrument_version: version(),
            auction_id: AuctionId::new(1).unwrap(),
            expected_phase_revision: 2,
            price_band: engine.book().instrument_price_band(),
            reference_price: Price::from_raw(100),
            price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
            uncross_policy: CallAuctionUncrossPolicy::new(
                AuctionAllocationPolicy::PriceTime,
                CallAuctionRemainderPolicy::RetainAll,
                CallAuctionSelfTradePolicy::Permit,
            ),
            received_at: TimestampNs::from_unix_nanos(command_id),
        }))
        .unwrap()
}

fn multi_trade_report() -> CallAuctionExecutionReport {
    uncross_report(&[(1, 11, 4), (2, 12, 6)], &[(3, 21, 3), (4, 22, 7)])
}

fn settlement(report: &CallAuctionExecutionReport) -> CallAuctionSettlement {
    CallAuctionSettlement::from_report(
        vec![transaction(101), transaction(102), transaction(103)],
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(100),
        report,
        definition(),
    )
    .unwrap()
}

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        recovery: RecoveryMode::Strict,
        ..JournalOptions::default()
    }
}

#[test]
fn complete_uncross_settles_as_one_atomic_idempotent_ledger_event() {
    let report = multi_trade_report();
    let value = settlement(&report);
    assert_eq!(value.transaction_count(), 3);

    let mut ledger = Ledger::new();
    let receipt = ledger.settle_call_auction(value.clone()).unwrap();
    assert_eq!(receipt.sequence(), 1);
    assert_eq!(receipt.transaction_count(), 3);
    assert!(!receipt.replayed());
    assert!(ledger.settle_call_auction(value).unwrap().replayed());

    assert_eq!(ledger.record_count(), 1);
    assert_eq!(ledger.entry_count(), 3);
    assert_eq!(ledger.balance(account(11), AssetId::new(1).unwrap()), 40);
    assert_eq!(ledger.balance(account(12), AssetId::new(1).unwrap()), 60);
    assert_eq!(ledger.balance(account(21), AssetId::new(1).unwrap()), -30);
    assert_eq!(ledger.balance(account(22), AssetId::new(1).unwrap()), -70);
    assert_eq!(ledger.balance(account(11), AssetId::new(2).unwrap()), -400);
    assert_eq!(ledger.balance(account(12), AssetId::new(2).unwrap()), -600);
    assert_eq!(ledger.balance(account(21), AssetId::new(2).unwrap()), 300);
    assert_eq!(ledger.balance(account(22), AssetId::new(2).unwrap()), 700);
    ledger.validate().unwrap();
}

#[test]
fn one_pair_uncross_uses_one_entry_and_report_grammar_is_rechecked() {
    let report = uncross_report(&[(1, 11, 5)], &[(2, 21, 5)]);
    let value = CallAuctionSettlement::from_report(
        vec![transaction(201)],
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(100),
        &report,
        definition(),
    )
    .unwrap();
    let mut ledger = Ledger::new();
    let receipt = ledger.settle_call_auction(value).unwrap();
    assert_eq!(receipt.transaction_count(), 1);
    assert!(matches!(ledger.record(1), Some(LedgerRecord::Entry(_))));

    let mut non_uncross_engine = engine();
    let non_uncross = non_uncross_engine
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    assert_eq!(
        CallAuctionSettlement::from_report(
            vec![],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &non_uncross,
            definition(),
        ),
        Err(LedgerError::CallAuctionSettlementReportInvalid)
    );

    let mut rejected_engine = engine();
    let rejected = rejected_engine
        .submit(phase(1, 1, CallAuctionPhase::Collecting))
        .unwrap();
    assert_eq!(
        CallAuctionSettlement::from_report(
            vec![],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &rejected,
            definition(),
        ),
        Err(LedgerError::CallAuctionSettlementReportInvalid)
    );

    let mut rebound = report;
    rebound.command_id = CommandId::new(999).unwrap();
    assert_eq!(
        CallAuctionSettlement::from_report(
            vec![transaction(202)],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &rebound,
            definition(),
        ),
        Err(LedgerError::CallAuctionSettlementReportInvalid)
    );

    let mut discontinuous = uncross_report(&[(1, 11, 5)], &[(2, 21, 5)]);
    discontinuous.events.make_mut()[1].sequence += 1;
    assert_eq!(
        CallAuctionSettlement::from_report(
            vec![transaction(203)],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &discontinuous,
            definition(),
        ),
        Err(LedgerError::CallAuctionSettlementReportInvalid)
    );

    let mut wrong_total = uncross_report(&[(1, 11, 5)], &[(2, 21, 5)]);
    let CallAuctionEventKind::UncrossCompleted { clearing, .. } =
        &mut wrong_total.events.make_mut()[1].kind
    else {
        panic!("representative uncross must end with completion");
    };
    *clearing = AuctionClearing::from_quantities(clearing.price(), 4, 5);
    assert_eq!(
        CallAuctionSettlement::from_report(
            vec![transaction(204)],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &wrong_total,
            definition(),
        ),
        Err(LedgerError::CallAuctionSettlementReportInvalid)
    );
}

#[test]
fn settlement_rejects_incomplete_identity_wrong_definition_and_self_pairs() {
    let report = multi_trade_report();
    assert!(matches!(
        CallAuctionSettlement::from_report(
            vec![transaction(1), transaction(2)],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &report,
            definition(),
        ),
        Err(LedgerError::CallAuctionSettlementTransactionCountMismatch {
            transaction_count: 2,
            trade_count: 3,
        })
    ));
    assert_eq!(
        CallAuctionSettlement::from_report(
            vec![transaction(1), transaction(2), transaction(3)],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &report,
            definition_with_identity(InstrumentId::new(18).unwrap(), version()),
        ),
        Err(LedgerError::SettlementInstrumentMismatch)
    );
    assert_eq!(
        CallAuctionSettlement::from_report(
            vec![transaction(1), transaction(2), transaction(3)],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &report,
            definition_with_identity(instrument(), InstrumentVersion::new(5).unwrap()),
        ),
        Err(LedgerError::SettlementVersionMismatch)
    );

    let self_report = uncross_report(&[(1, 11, 5)], &[(2, 11, 5)]);
    assert_eq!(
        CallAuctionSettlement::from_report(
            vec![transaction(1)],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &self_report,
            definition(),
        ),
        Err(LedgerError::SelfSettlement)
    );
}

#[test]
fn partial_prior_commit_and_balance_overflow_leave_the_uncross_unapplied() {
    let report = multi_trade_report();
    let value = settlement(&report);
    let first_trade = CallAuctionSettlement::from_report(
        vec![transaction(101)],
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(100),
        &uncross_report(&[(1, 11, 3)], &[(3, 21, 3)]),
        definition(),
    )
    .unwrap();
    let mut partially_committed = Ledger::new();
    partially_committed
        .settle_call_auction(first_trade)
        .unwrap();
    assert_eq!(
        partially_committed.settle_call_auction(value.clone()),
        Err(LedgerError::BatchAlreadyPartiallyCommitted(transaction(
            101
        )))
    );
    assert_eq!(partially_committed.record_count(), 1);
    assert_eq!(partially_committed.entry_count(), 1);
    assert!(partially_committed.transaction(transaction(102)).is_none());
    assert!(partially_committed.transaction(transaction(103)).is_none());

    let initial = i128::MAX - 20;
    let funding = JournalEntry::new(
        transaction(1),
        1,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(99),
        vec![
            Posting {
                account_id: account(11),
                asset_id: AssetId::new(1).unwrap(),
                amount: initial,
            },
            Posting {
                account_id: account(99),
                asset_id: AssetId::new(1).unwrap(),
                amount: -initial,
            },
        ],
    )
    .unwrap();
    let mut overflowing = Ledger::new();
    overflowing.post(funding).unwrap();
    assert_eq!(
        overflowing.settle_call_auction(value),
        Err(LedgerError::ArithmeticOverflow)
    );
    assert_eq!(overflowing.record_count(), 1);
    assert_eq!(overflowing.entry_count(), 1);
    assert_eq!(
        overflowing.balance(account(11), AssetId::new(1).unwrap()),
        initial
    );
    assert_eq!(
        overflowing.balance(account(12), AssetId::new(1).unwrap()),
        0
    );
    assert_eq!(
        overflowing.balance(account(21), AssetId::new(2).unwrap()),
        0
    );
    overflowing.validate().unwrap();
}

#[test]
fn durable_uncross_settlement_is_one_frame_and_recovers_exactly() {
    let file = TestFile::new();
    let value = settlement(&multi_trade_report());
    let mut durable = DurableLedger::open(&file.0, options()).unwrap();
    let receipt = durable.settle_call_auction(value.clone()).unwrap();
    assert_eq!(receipt.transaction_count(), 3);
    let length = fs::metadata(&file.0).unwrap().len();
    assert!(
        durable
            .settle_call_auction(value.clone())
            .unwrap()
            .replayed()
    );
    assert_eq!(fs::metadata(&file.0).unwrap().len(), length);
    durable.close().unwrap();

    let frames = JournalReader::open(&file.0, options())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].kind(), RecordKind::LedgerBatch);

    let mut recovered = DurableLedger::open(&file.0, options()).unwrap();
    assert_eq!(recovered.ledger().record_count(), 1);
    assert_eq!(recovered.ledger().entry_count(), 3);
    assert!(recovered.settle_call_auction(value).unwrap().replayed());
    recovered.ledger().validate().unwrap();
}

#[test]
fn durable_uncross_settlement_survives_checkpoint_cutover_and_exact_retry() {
    let file = TestFile::new();
    let checkpoint_base = file.0.with_extension("qsnp");
    let value = settlement(&multi_trade_report());
    let mut durable = DurableLedger::open(&file.0, options()).unwrap();
    durable.settle_call_auction(value.clone()).unwrap();
    let cutover = durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    durable.close().unwrap();

    let mut recovered = DurableLedger::open_with_checkpoint(
        &file.0,
        &checkpoint_base,
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_records, 1);
    assert_eq!(recovered.recovery().replayed_records, 0);
    let length = fs::metadata(&file.0).unwrap().len();
    assert!(recovered.settle_call_auction(value).unwrap().replayed());
    assert_eq!(fs::metadata(&file.0).unwrap().len(), length);
    recovered.ledger().validate().unwrap();
    recovered.close().unwrap();

    for slot in [CheckpointSlot::A, CheckpointSlot::B] {
        let _ = fs::remove_file(SnapshotFile::slot_path(&checkpoint_base, slot));
    }
}
