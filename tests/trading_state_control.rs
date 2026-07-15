use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::BinaryCodec;
use quotick::durable::DurableOrderBook;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions};
use quotick::market_data::{MarketDataKind, MarketDataPublisher, MarketDataReplica};
use quotick::matching::{
    CancelReason, Command, CommandOutcome, EventKind, MatchingCapacity, MatchingError, NewOrder,
    OrderBook, OrderBookCheckpoint, OrderBookLimits, OrderBookLimitsSpec, OrderDisplay, OrderType,
    RejectReason, SelfTradePrevention, TimeInForce, TradingStateControl, TradingStateControlAction,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_WAL: AtomicU64 = AtomicU64::new(1);

struct TestWal(PathBuf);

impl TestWal {
    fn new() -> Self {
        let nonce = NEXT_WAL.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-trading-state-{}-{nonce}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestWal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn instrument_id() -> InstrumentId {
    InstrumentId::new(1).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(1).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument_id(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("SESSION-CONTROL").unwrap(),
        kind: InstrumentKind::Future,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::new(100).unwrap(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn order(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    price: i64,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: instrument_id(),
        instrument_version: version(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn control(
    command_id: u64,
    expected_revision: u64,
    target_state: TradingState,
    action: TradingStateControlAction,
) -> Command {
    Command::TradingStateControl(TradingStateControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument_id(),
        instrument_version: version(),
        expected_revision,
        target_state,
        action,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

#[test]
fn transition_and_cancel_is_revisioned_canonical_atomic_and_public() {
    let mut book = OrderBook::new(definition());
    book.submit(order(1, 30, 11, Side::Buy, 5, 90)).unwrap();
    book.submit(order(2, 10, 12, Side::Sell, 7, 110)).unwrap();
    book.submit(order(3, 20, 13, Side::Buy, 3, 80)).unwrap();
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::new(instrument_id(), version(), TradingState::Open);
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    assert_eq!(book.trading_state().state(), TradingState::Open);
    assert_eq!(book.trading_state().revision(), 0);
    let halt = control(
        4,
        0,
        TradingState::Halted,
        TradingStateControlAction::TransitionAndCancel,
    );
    let report = book.submit(halt).unwrap();
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(report.events.len(), 4);
    assert_eq!(
        report
            .events
            .iter()
            .take(3)
            .map(|event| match event.kind {
                EventKind::OrderCancelled {
                    order_id,
                    reason: CancelReason::TradingStateControl,
                    ..
                } => order_id.get(),
                _ => panic!("state cancel emits only canonical cancellation events first"),
            })
            .collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
    assert!(matches!(
        report.events[3].kind,
        EventKind::TradingStateControlApplied {
            previous_state: TradingState::Open,
            current_state: TradingState::Halted,
            revision: 1,
            cancelled_order_count: 3,
            cancelled_quantity_lots: 15,
        }
    ));
    assert_eq!(book.active_order_count(), 0);
    assert_eq!(book.trading_state().state(), TradingState::Halted);
    assert_eq!(book.trading_state().revision(), 1);
    book.validate().unwrap();

    let batch = publisher.publish(halt, &report, &book).unwrap();
    assert!(matches!(
        batch.updates().last().unwrap().kind(),
        MarketDataKind::TradingState {
            previous_state: TradingState::Open,
            current_state: TradingState::Halted,
            revision: 1,
        }
    ));
    replica.apply_batch(&batch).unwrap();
    assert_eq!(replica.trading_state().state(), TradingState::Halted);
    assert_eq!(replica.trading_state().revision(), 1);
    assert!(replica.depth(Side::Buy, usize::MAX).is_empty());
    assert!(replica.depth(Side::Sell, usize::MAX).is_empty());

    let replay = book.submit(halt).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.events, report.events);
    assert!(publisher.publish(halt, &replay, &book).unwrap().replayed());
    assert_eq!(book.trading_state().revision(), 1);
}

#[test]
fn state_control_rejections_do_not_change_state_and_reopen_restores_entry() {
    let mut book = OrderBook::new(definition());
    book.submit(control(
        1,
        0,
        TradingState::Halted,
        TradingStateControlAction::Transition,
    ))
    .unwrap();

    let rejected_entry = book.submit(order(2, 1, 11, Side::Buy, 5, 90)).unwrap();
    assert_eq!(
        rejected_entry.outcome,
        CommandOutcome::Rejected(RejectReason::InstrumentNotOpen)
    );
    let stale = book
        .submit(control(
            3,
            0,
            TradingState::Closed,
            TradingStateControlAction::Transition,
        ))
        .unwrap();
    assert_eq!(
        stale.outcome,
        CommandOutcome::Rejected(RejectReason::TradingStateControlRevisionMismatch)
    );
    let invalid_cancel_reopen = book
        .submit(control(
            4,
            1,
            TradingState::Open,
            TradingStateControlAction::TransitionAndCancel,
        ))
        .unwrap();
    assert_eq!(
        invalid_cancel_reopen.outcome,
        CommandOutcome::Rejected(RejectReason::TradingStateControlCannotCancelIntoOpen)
    );
    assert_eq!(book.trading_state().state(), TradingState::Halted);
    assert_eq!(book.trading_state().revision(), 1);

    let reopen = book
        .submit(control(
            5,
            1,
            TradingState::Open,
            TradingStateControlAction::Transition,
        ))
        .unwrap();
    assert_eq!(reopen.outcome, CommandOutcome::Accepted);
    assert_eq!(book.trading_state().state(), TradingState::Open);
    assert_eq!(book.trading_state().revision(), 2);
    assert_eq!(
        book.submit(order(6, 2, 11, Side::Buy, 5, 90))
            .unwrap()
            .outcome,
        CommandOutcome::Accepted
    );

    let unchanged = book
        .submit(control(
            7,
            2,
            TradingState::Open,
            TradingStateControlAction::Transition,
        ))
        .unwrap();
    assert_eq!(
        unchanged.outcome,
        CommandOutcome::Rejected(RejectReason::TradingStateUnchanged)
    );
    assert_eq!(book.trading_state().revision(), 2);
    book.validate().unwrap();
}

#[test]
fn checkpoint_codec_and_restore_derive_effective_state_from_control_history() {
    let mut book = OrderBook::new(definition());
    book.submit(order(1, 1, 11, Side::Buy, 5, 90)).unwrap();
    let halt = control(
        2,
        0,
        TradingState::Halted,
        TradingStateControlAction::TransitionAndCancel,
    );
    let halt_report = book.submit(halt).unwrap();
    let checkpoint = book.checkpoint(1, 5).unwrap();
    let encoded = checkpoint.encode().unwrap();
    let decoded = OrderBookCheckpoint::decode(&encoded).unwrap();
    let mut restored = OrderBook::from_checkpoint(&decoded).unwrap();

    assert_eq!(restored.trading_state().state(), TradingState::Halted);
    assert_eq!(restored.trading_state().revision(), 1);
    assert_eq!(restored.active_order_count(), 0);
    assert_eq!(restored.submit(halt).unwrap().events, halt_report.events);
    assert!(restored.submit(halt).unwrap().replayed);
    restored.validate().unwrap();
}

#[test]
fn wal_recovery_reconstructs_state_revision_and_exact_retry() {
    let wal = TestWal::new();
    let options = JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let halt = control(
        2,
        0,
        TradingState::Closed,
        TradingStateControlAction::TransitionAndCancel,
    );
    let original = {
        let mut durable = DurableOrderBook::open(wal.path(), definition(), options).unwrap();
        durable.submit(order(1, 1, 11, Side::Buy, 5, 90)).unwrap();
        let report = durable.submit(halt).unwrap();
        durable.close().unwrap();
        report
    };

    let mut recovered = DurableOrderBook::open(wal.path(), definition(), options).unwrap();
    assert_eq!(
        recovered.book().trading_state().state(),
        TradingState::Closed
    );
    assert_eq!(recovered.book().trading_state().revision(), 1);
    assert_eq!(recovered.book().active_order_count(), 0);
    let replay = recovered.submit(halt).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.events, original.events);
    recovered.book().validate().unwrap();
    recovered.close().unwrap();
}

#[test]
fn entry_closing_cancel_all_uses_protected_history_but_transition_only_does_not() {
    let limits = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 4,
        max_account_controls: 2,
        max_retained_commands: 3,
        cancellation_reserve: 1,
        max_report_events: 2,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let mut book = OrderBook::with_limits(definition(), limits);
    book.submit(order(1, 1, 11, Side::Buy, 5, 90)).unwrap();
    assert_eq!(
        book.submit(order(2, 1, 11, Side::Buy, 5, 90))
            .unwrap()
            .outcome,
        CommandOutcome::Rejected(RejectReason::DuplicateOrder)
    );
    assert_eq!(
        book.submit(control(
            3,
            0,
            TradingState::Halted,
            TradingStateControlAction::Transition,
        )),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AdmissionCommandHistory
        ))
    );

    let report = book
        .submit(control(
            3,
            0,
            TradingState::Halted,
            TradingStateControlAction::TransitionAndCancel,
        ))
        .unwrap();
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(book.active_order_count(), 0);
    assert_eq!(book.trading_state().state(), TradingState::Halted);
    assert_eq!(book.retained_command_count(), 3);
    book.validate().unwrap();
}
