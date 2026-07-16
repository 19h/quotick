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
    CallAuctionFee, CallAuctionSettlement, CallAuctionSettlementCorrection, JournalEntry, Ledger,
    LedgerBatch, LedgerEntryKind, LedgerError, LedgerLimitsSpec, LedgerRecord, LedgerResource,
    Posting,
};
use quotick::snapshot::{CheckpointSlot, SnapshotFile, SnapshotOptions};
use quotick::{
    AccountId, AccountingDate, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion,
    OrderId, Price, Quantity, Side, TimestampNs, TradeId, TransactionId,
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

fn report_trade_id(report: &CallAuctionExecutionReport, index: usize) -> TradeId {
    let CallAuctionEventKind::Trade(trade) = report.events[index].kind else {
        panic!("requested report event must be a trade");
    };
    trade.trade_id()
}

fn fee(
    transaction_id: u64,
    trade_id: TradeId,
    debit_account_id: u64,
    credit_account_id: u64,
    amount: i128,
) -> CallAuctionFee {
    fee_in_asset(
        transaction_id,
        trade_id,
        debit_account_id,
        credit_account_id,
        AssetId::new(2).unwrap(),
        amount,
    )
}

fn fee_in_asset(
    transaction_id: u64,
    trade_id: TradeId,
    debit_account_id: u64,
    credit_account_id: u64,
    asset_id: AssetId,
    amount: i128,
) -> CallAuctionFee {
    CallAuctionFee::new(
        transaction(transaction_id),
        trade_id,
        account(debit_account_id),
        account(credit_account_id),
        asset_id,
        amount,
    )
    .unwrap()
}

fn fee_settlement(report: &CallAuctionExecutionReport) -> CallAuctionSettlement {
    CallAuctionSettlement::from_report_with_fees(
        vec![transaction(101), transaction(102), transaction(103)],
        vec![
            fee(201, report_trade_id(report, 0), 11, 90, 7),
            fee_in_asset(
                202,
                report_trade_id(report, 0),
                90,
                11,
                AssetId::new(3).unwrap(),
                2,
            ),
            fee(203, report_trade_id(report, 2), 22, 90, 11),
        ],
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(100),
        report,
        definition(),
    )
    .unwrap()
}

fn correction_transaction_ids() -> Vec<TransactionId> {
    (301..=306).map(transaction).collect()
}

fn replacement_settlement() -> CallAuctionSettlement {
    CallAuctionSettlement::from_report(
        vec![transaction(401)],
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(300),
        &uncross_report(&[(1, 11, 5)], &[(2, 21, 5)]),
        definition(),
    )
    .unwrap()
}

fn replacement_correction(original: &CallAuctionSettlement) -> CallAuctionSettlementCorrection {
    CallAuctionSettlementCorrection::replace(
        correction_transaction_ids(),
        9_001,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(200),
        replacement_settlement(),
        original,
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
fn explicit_trade_fees_settle_in_the_same_atomic_event_as_dvp() {
    let report = multi_trade_report();
    let value = fee_settlement(&report);
    assert_eq!(value.transaction_count(), 6);

    let mut ledger = Ledger::new();
    let receipt = ledger.settle_call_auction(value.clone()).unwrap();
    assert_eq!(receipt.sequence(), 1);
    assert_eq!(receipt.transaction_count(), 6);
    assert!(!receipt.replayed());
    assert!(ledger.settle_call_auction(value).unwrap().replayed());

    assert_eq!(ledger.record_count(), 1);
    assert_eq!(ledger.entry_count(), 6);
    assert!(matches!(ledger.record(1), Some(LedgerRecord::Batch(_))));
    assert_eq!(ledger.balance(account(11), AssetId::new(2).unwrap()), -407);
    assert_eq!(ledger.balance(account(22), AssetId::new(2).unwrap()), 689);
    assert_eq!(ledger.balance(account(90), AssetId::new(2).unwrap()), 18);
    assert_eq!(ledger.balance(account(11), AssetId::new(3).unwrap()), 2);
    assert_eq!(ledger.balance(account(90), AssetId::new(3).unwrap()), -2);

    let buyer_fee = ledger.transaction(transaction(201)).unwrap();
    assert_eq!(buyer_fee.reference(), report_trade_id(&report, 0).get());
    assert_eq!(
        buyer_fee.postings(),
        [
            Posting {
                account_id: account(11),
                asset_id: AssetId::new(2).unwrap(),
                amount: -7,
            },
            Posting {
                account_id: account(90),
                asset_id: AssetId::new(2).unwrap(),
                amount: 7,
            },
        ]
    );
    let rebate = ledger.transaction(transaction(202)).unwrap();
    assert_eq!(rebate.reference(), report_trade_id(&report, 0).get());
    assert!(
        rebate
            .postings()
            .iter()
            .all(|posting| posting.asset_id == AssetId::new(3).unwrap())
    );
    assert_eq!(rebate.postings()[0].account_id, account(11));
    assert_eq!(rebate.postings()[0].amount, 2);
    assert_eq!(rebate.postings()[1].account_id, account(90));
    assert_eq!(rebate.postings()[1].amount, -2);
    ledger.validate().unwrap();
}

#[test]
fn fee_instructions_are_positive_distinct_and_canonically_trade_bound() {
    let trade_id = TradeId::new(1).unwrap();
    assert_eq!(
        CallAuctionFee::new(
            transaction(1),
            trade_id,
            account(11),
            account(90),
            AssetId::new(2).unwrap(),
            0,
        ),
        Err(LedgerError::FeeAmountNotPositive(0))
    );
    assert_eq!(
        CallAuctionFee::new(
            transaction(1),
            trade_id,
            account(11),
            account(90),
            AssetId::new(2).unwrap(),
            -1,
        ),
        Err(LedgerError::FeeAmountNotPositive(-1))
    );
    assert_eq!(
        CallAuctionFee::new(
            transaction(1),
            trade_id,
            account(11),
            account(11),
            AssetId::new(2).unwrap(),
            1,
        ),
        Err(LedgerError::FeeAccountsIdentical(account(11)))
    );

    let report = multi_trade_report();
    let unknown_trade = TradeId::new(999).unwrap();
    assert_eq!(
        CallAuctionSettlement::from_report_with_fees(
            vec![transaction(101), transaction(102), transaction(103)],
            vec![fee(201, unknown_trade, 11, 90, 1)],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &report,
            definition(),
        ),
        Err(LedgerError::CallAuctionFeeTradeMismatch(unknown_trade))
    );

    let first_trade = report_trade_id(&report, 0);
    let last_trade = report_trade_id(&report, 2);
    assert_eq!(
        CallAuctionSettlement::from_report_with_fees(
            vec![transaction(101), transaction(102), transaction(103)],
            vec![
                fee(201, last_trade, 22, 90, 1),
                fee(202, first_trade, 11, 90, 1),
            ],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &report,
            definition(),
        ),
        Err(LedgerError::CallAuctionFeeTradeMismatch(first_trade))
    );

    assert_eq!(
        CallAuctionSettlement::from_report_with_fees(
            vec![transaction(101), transaction(102), transaction(103)],
            vec![fee(101, first_trade, 11, 90, 1)],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &report,
            definition(),
        ),
        Err(LedgerError::BatchDuplicateTransaction(transaction(101)))
    );
    assert_eq!(
        CallAuctionSettlement::from_report_with_fees(
            vec![transaction(101), transaction(102), transaction(103)],
            vec![
                fee(201, first_trade, 11, 90, 1),
                fee(201, first_trade, 90, 11, 1),
            ],
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(100),
            &report,
            definition(),
        ),
        Err(LedgerError::BatchDuplicateTransaction(transaction(201)))
    );
}

#[test]
fn fee_enriched_settlement_respects_atomic_record_capacity() {
    let report = multi_trade_report();
    let value = fee_settlement(&report);
    let mut ledger = Ledger::try_with_limits(LedgerLimitsSpec {
        max_balance_keys: 16,
        max_transactions: 16,
        max_reversals: 8,
        max_records: 16,
        max_postings_per_transaction: 4,
        max_transactions_per_record: 4,
        max_retained_postings: 64,
    })
    .unwrap();

    assert_eq!(
        ledger.settle_call_auction(value),
        Err(LedgerError::CapacityExceeded {
            resource: LedgerResource::TransactionsPerRecord,
            maximum: 4,
            attempted: 6,
        })
    );
    assert_eq!(ledger.record_count(), 0);
    assert_eq!(ledger.entry_count(), 0);
    assert_eq!(ledger.nonzero_balance_count(), 0);
    ledger.validate().unwrap();
}

#[test]
fn separately_committed_fee_prevents_the_complete_settlement_without_a_prefix() {
    let report = multi_trade_report();
    let value = fee_settlement(&report);
    let standalone_fee = JournalEntry::new(
        transaction(201),
        report_trade_id(&report, 0).get(),
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(100),
        vec![
            Posting {
                account_id: account(11),
                asset_id: AssetId::new(2).unwrap(),
                amount: -7,
            },
            Posting {
                account_id: account(90),
                asset_id: AssetId::new(2).unwrap(),
                amount: 7,
            },
        ],
    )
    .unwrap();
    let mut ledger = Ledger::new();
    ledger.post(standalone_fee).unwrap();

    assert_eq!(
        ledger.settle_call_auction(value),
        Err(LedgerError::BatchAlreadyPartiallyCommitted(transaction(
            201
        )))
    );
    assert_eq!(ledger.record_count(), 1);
    assert_eq!(ledger.entry_count(), 1);
    assert!(ledger.transaction(transaction(101)).is_none());
    assert!(ledger.transaction(transaction(102)).is_none());
    assert!(ledger.transaction(transaction(103)).is_none());
    assert!(ledger.transaction(transaction(202)).is_none());
    assert!(ledger.transaction(transaction(203)).is_none());
    assert_eq!(ledger.balance(account(90), AssetId::new(2).unwrap()), 7);
    ledger.validate().unwrap();
}

#[test]
fn complete_fee_enriched_settlement_bust_is_one_atomic_inverse_event() {
    let original = fee_settlement(&multi_trade_report());
    let bust = CallAuctionSettlementCorrection::bust(
        correction_transaction_ids(),
        9_000,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(200),
        &original,
    )
    .unwrap();
    assert_eq!(bust.reversal_transaction_count(), 6);
    assert_eq!(bust.replacement_transaction_count(), 0);
    assert_eq!(bust.transaction_count(), 6);

    let mut ledger = Ledger::new();
    ledger.settle_call_auction(original).unwrap();
    let receipt = ledger.correct_call_auction(bust.clone()).unwrap();
    assert_eq!(receipt.sequence(), 2);
    assert!(!receipt.replayed());
    assert_eq!(receipt.reversal_transaction_count(), 6);
    assert_eq!(receipt.replacement_transaction_count(), 0);
    assert_eq!(receipt.transaction_count(), 6);
    assert!(ledger.correct_call_auction(bust).unwrap().replayed());

    assert_eq!(ledger.record_count(), 2);
    assert_eq!(ledger.entry_count(), 12);
    assert!(matches!(ledger.record(2), Some(LedgerRecord::Batch(_))));
    for (original_transaction_id, reversal_transaction_id) in [
        (101, 301),
        (201, 302),
        (202, 303),
        (102, 304),
        (103, 305),
        (203, 306),
    ] {
        assert_eq!(
            ledger.reversal_for(transaction(original_transaction_id)),
            Some(transaction(reversal_transaction_id))
        );
        assert_eq!(
            ledger
                .transaction(transaction(reversal_transaction_id))
                .unwrap()
                .kind(),
            LedgerEntryKind::Reversal {
                reversed_transaction_id: transaction(original_transaction_id),
            }
        );
    }
    for account_id in [11, 12, 21, 22, 90] {
        for asset_id in [1, 2, 3] {
            assert_eq!(
                ledger.balance(account(account_id), AssetId::new(asset_id).unwrap()),
                0
            );
        }
    }
    ledger.validate().unwrap();
}

#[test]
fn replacement_correction_reverses_the_original_before_new_settlement() {
    let original = fee_settlement(&multi_trade_report());
    let correction = replacement_correction(&original);
    assert_eq!(correction.reversal_transaction_count(), 6);
    assert_eq!(correction.replacement_transaction_count(), 1);
    assert_eq!(correction.transaction_count(), 7);

    let mut ledger = Ledger::new();
    ledger.settle_call_auction(original).unwrap();
    let receipt = ledger.correct_call_auction(correction.clone()).unwrap();
    assert_eq!(receipt.sequence(), 2);
    assert_eq!(receipt.reversal_transaction_count(), 6);
    assert_eq!(receipt.replacement_transaction_count(), 1);
    assert_eq!(receipt.transaction_count(), 7);
    assert!(!receipt.replayed());
    assert!(
        ledger
            .correct_call_auction(correction.clone())
            .unwrap()
            .replayed()
    );

    let Some(LedgerRecord::Batch(batch)) = ledger.record(2) else {
        panic!("replacement correction must be one ledger batch");
    };
    assert_eq!(batch.entries().len(), 7);
    assert!(
        batch.entries()[..6]
            .iter()
            .all(|entry| matches!(entry.kind(), LedgerEntryKind::Reversal { .. }))
    );
    assert_eq!(batch.entries()[6].transaction_id(), transaction(401));
    assert_eq!(batch.entries()[6].kind(), LedgerEntryKind::Standard);

    assert_eq!(ledger.balance(account(11), AssetId::new(1).unwrap()), 50);
    assert_eq!(ledger.balance(account(21), AssetId::new(1).unwrap()), -50);
    assert_eq!(ledger.balance(account(11), AssetId::new(2).unwrap()), -500);
    assert_eq!(ledger.balance(account(21), AssetId::new(2).unwrap()), 500);
    for account_id in [12, 22, 90] {
        for asset_id in [1, 2, 3] {
            assert_eq!(
                ledger.balance(account(account_id), AssetId::new(asset_id).unwrap()),
                0
            );
        }
    }

    let second = CallAuctionSettlementCorrection::bust(
        (501..=506).map(transaction).collect(),
        9_002,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(400),
        correction.original_settlement(),
    )
    .unwrap();
    assert!(matches!(
        ledger.correct_call_auction(second),
        Err(LedgerError::TransactionAlreadyReversed {
            original_transaction_id,
            ..
        }) if original_transaction_id == transaction(101)
    ));
    assert_eq!(ledger.record_count(), 2);
    assert!(ledger.transaction(transaction(501)).is_none());
    ledger.validate().unwrap();
}

#[test]
fn settlement_correction_rejects_count_identity_grouping_and_capacity_failures() {
    let original = fee_settlement(&multi_trade_report());
    assert_eq!(
        CallAuctionSettlementCorrection::bust(
            vec![transaction(301)],
            9_000,
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(200),
            &original,
        ),
        Err(LedgerError::CallAuctionCorrectionTransactionCountMismatch {
            reversal_transaction_count: 1,
            settlement_transaction_count: 6,
        })
    );
    let mut duplicate_ids = correction_transaction_ids();
    duplicate_ids[5] = transaction(301);
    assert_eq!(
        CallAuctionSettlementCorrection::bust(
            duplicate_ids,
            9_000,
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(200),
            &original,
        ),
        Err(LedgerError::BatchDuplicateTransaction(transaction(301)))
    );

    let colliding_replacement = CallAuctionSettlement::from_report(
        vec![transaction(301)],
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(300),
        &uncross_report(&[(1, 11, 5)], &[(2, 21, 5)]),
        definition(),
    )
    .unwrap();
    assert_eq!(
        CallAuctionSettlementCorrection::replace(
            correction_transaction_ids(),
            9_001,
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(200),
            colliding_replacement,
            &original,
        ),
        Err(LedgerError::BatchDuplicateTransaction(transaction(301)))
    );

    let regressing_replacement = CallAuctionSettlement::from_report(
        vec![transaction(402)],
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(199),
        &uncross_report(&[(1, 11, 5)], &[(2, 21, 5)]),
        definition(),
    )
    .unwrap();
    assert_eq!(
        CallAuctionSettlementCorrection::replace(
            correction_transaction_ids(),
            9_001,
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(200),
            regressing_replacement,
            &original,
        ),
        Err(LedgerError::RecordedTimestampRegression {
            previous: TimestampNs::from_unix_nanos(200),
            proposed: TimestampNs::from_unix_nanos(199),
        })
    );

    let correction = replacement_correction(&original);
    let mut absent = Ledger::new();
    assert_eq!(
        absent.correct_call_auction(correction.clone()),
        Err(LedgerError::ReversalTargetMissing(transaction(101)))
    );
    assert_eq!(absent.record_count(), 0);

    let single_report = uncross_report(&[(1, 11, 5)], &[(2, 21, 5)]);
    let single = CallAuctionSettlement::from_report(
        vec![transaction(701)],
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(100),
        &single_report,
        definition(),
    )
    .unwrap();
    let embedded_entry = JournalEntry::new(
        transaction(701),
        report_trade_id(&single_report, 0).get(),
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(100),
        vec![
            Posting {
                account_id: account(11),
                asset_id: AssetId::new(1).unwrap(),
                amount: 50,
            },
            Posting {
                account_id: account(21),
                asset_id: AssetId::new(1).unwrap(),
                amount: -50,
            },
            Posting {
                account_id: account(11),
                asset_id: AssetId::new(2).unwrap(),
                amount: -500,
            },
            Posting {
                account_id: account(21),
                asset_id: AssetId::new(2).unwrap(),
                amount: 500,
            },
        ],
    )
    .unwrap();
    let auxiliary = JournalEntry::new(
        transaction(799),
        799,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(100),
        vec![
            Posting {
                account_id: account(91),
                asset_id: AssetId::new(3).unwrap(),
                amount: -1,
            },
            Posting {
                account_id: account(92),
                asset_id: AssetId::new(3).unwrap(),
                amount: 1,
            },
        ],
    )
    .unwrap();
    let mut wrong_group = Ledger::new();
    wrong_group
        .post_batch(LedgerBatch::new(vec![embedded_entry, auxiliary]).unwrap())
        .unwrap();
    let single_bust = CallAuctionSettlementCorrection::bust(
        vec![transaction(801)],
        9_003,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(200),
        &single,
    )
    .unwrap();
    assert_eq!(
        wrong_group.correct_call_auction(single_bust),
        Err(LedgerError::CallAuctionCorrectionOriginalGroupingMismatch(
            transaction(701)
        ))
    );
    assert!(wrong_group.transaction(transaction(801)).is_none());

    let mut capacity = Ledger::try_with_limits(LedgerLimitsSpec {
        max_balance_keys: 16,
        max_transactions: 16,
        max_reversals: 8,
        max_records: 16,
        max_postings_per_transaction: 4,
        max_transactions_per_record: 6,
        max_retained_postings: 64,
    })
    .unwrap();
    capacity.settle_call_auction(original).unwrap();
    assert_eq!(
        capacity.correct_call_auction(correction),
        Err(LedgerError::CapacityExceeded {
            resource: LedgerResource::TransactionsPerRecord,
            maximum: 6,
            attempted: 7,
        })
    );
    assert_eq!(capacity.record_count(), 1);
    assert_eq!(capacity.entry_count(), 6);
    assert!(capacity.transaction(transaction(301)).is_none());
    assert_eq!(capacity.balance(account(90), AssetId::new(2).unwrap()), 18);
    capacity.validate().unwrap();
}

#[test]
fn one_entry_settlement_bust_retains_the_ordinary_entry_path() {
    let report = uncross_report(&[(1, 11, 5)], &[(2, 21, 5)]);
    let original = CallAuctionSettlement::from_report(
        vec![transaction(201)],
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(100),
        &report,
        definition(),
    )
    .unwrap();
    let bust = CallAuctionSettlementCorrection::bust(
        vec![transaction(301)],
        9_000,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(200),
        &original,
    )
    .unwrap();

    let mut ledger = Ledger::new();
    ledger.settle_call_auction(original).unwrap();
    let receipt = ledger.correct_call_auction(bust).unwrap();
    assert_eq!(receipt.transaction_count(), 1);
    assert_eq!(receipt.reversal_transaction_count(), 1);
    assert_eq!(receipt.replacement_transaction_count(), 0);
    assert!(matches!(ledger.record(2), Some(LedgerRecord::Entry(_))));
    assert_eq!(ledger.balance(account(11), AssetId::new(1).unwrap()), 0);
    assert_eq!(ledger.balance(account(21), AssetId::new(2).unwrap()), 0);
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
fn durable_fee_enriched_settlement_is_one_frame_and_recovers_exactly() {
    let file = TestFile::new();
    let checkpoint_base = file.0.with_extension("qsnp");
    let report = multi_trade_report();
    let value = fee_settlement(&report);
    let mut durable = DurableLedger::open(&file.0, options()).unwrap();
    let receipt = durable.settle_call_auction(value.clone()).unwrap();
    assert_eq!(receipt.transaction_count(), 6);
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
    assert_eq!(recovered.ledger().entry_count(), 6);
    assert_eq!(
        recovered
            .ledger()
            .balance(account(90), AssetId::new(2).unwrap()),
        18
    );
    assert!(recovered.settle_call_auction(value).unwrap().replayed());
    recovered.ledger().validate().unwrap();
    let cutover = recovered
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    recovered.close().unwrap();

    let value = fee_settlement(&report);
    let mut checkpointed = DurableLedger::open_with_checkpoint(
        &file.0,
        &checkpoint_base,
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(checkpointed.recovery().checkpointed_records, 1);
    assert_eq!(checkpointed.recovery().replayed_records, 0);
    assert_eq!(
        checkpointed
            .ledger()
            .balance(account(90), AssetId::new(2).unwrap()),
        18
    );
    assert!(checkpointed.settle_call_auction(value).unwrap().replayed());
    checkpointed.ledger().validate().unwrap();
    checkpointed.close().unwrap();

    for slot in [CheckpointSlot::A, CheckpointSlot::B] {
        let _ = fs::remove_file(SnapshotFile::slot_path(&checkpoint_base, slot));
    }
}

#[test]
fn durable_replacement_correction_is_one_frame_and_recovers_exactly() {
    let file = TestFile::new();
    let checkpoint_base = file.0.with_extension("qsnp");
    let original = fee_settlement(&multi_trade_report());
    let correction = replacement_correction(&original);
    let mut durable = DurableLedger::open(&file.0, options()).unwrap();
    durable.settle_call_auction(original).unwrap();
    let receipt = durable.correct_call_auction(correction.clone()).unwrap();
    assert_eq!(receipt.sequence(), 2);
    assert_eq!(receipt.reversal_transaction_count(), 6);
    assert_eq!(receipt.replacement_transaction_count(), 1);
    let length = fs::metadata(&file.0).unwrap().len();
    assert!(
        durable
            .correct_call_auction(correction.clone())
            .unwrap()
            .replayed()
    );
    assert_eq!(fs::metadata(&file.0).unwrap().len(), length);
    durable.close().unwrap();

    let frames = JournalReader::open(&file.0, options())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames.len(), 2);
    assert!(
        frames
            .iter()
            .all(|frame| frame.kind() == RecordKind::LedgerBatch)
    );

    let mut recovered = DurableLedger::open(&file.0, options()).unwrap();
    assert_eq!(recovered.ledger().record_count(), 2);
    assert_eq!(recovered.ledger().entry_count(), 13);
    assert_eq!(
        recovered
            .ledger()
            .balance(account(11), AssetId::new(1).unwrap()),
        50
    );
    assert_eq!(
        recovered
            .ledger()
            .balance(account(90), AssetId::new(2).unwrap()),
        0
    );
    assert!(
        recovered
            .correct_call_auction(correction.clone())
            .unwrap()
            .replayed()
    );
    recovered.ledger().validate().unwrap();
    let cutover = recovered
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    recovered.close().unwrap();

    let mut checkpointed = DurableLedger::open_with_checkpoint(
        &file.0,
        &checkpoint_base,
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(checkpointed.recovery().checkpointed_records, 2);
    assert_eq!(checkpointed.recovery().replayed_records, 0);
    assert_eq!(
        checkpointed
            .ledger()
            .balance(account(11), AssetId::new(2).unwrap()),
        -500
    );
    assert!(
        checkpointed
            .correct_call_auction(correction)
            .unwrap()
            .replayed()
    );
    checkpointed.ledger().validate().unwrap();
    checkpointed.close().unwrap();

    for slot in [CheckpointSlot::A, CheckpointSlot::B] {
        let _ = fs::remove_file(SnapshotFile::slot_path(&checkpoint_base, slot));
    }
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
