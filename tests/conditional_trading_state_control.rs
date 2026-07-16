use std::cell::Cell;
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable::DurableOrderBook;
use quotick::durable_risk::DurableRiskOrderBook;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions, JournalReader};
use quotick::matching::{
    ActiveOrderSnapshot, CancelReason, Command, CommandOutcome, ConditionalCommandOutcome,
    ConditionalOrderError, EventKind, NewOrder, OrderBook, OrderDisplay, OrderType, RejectReason,
    SelfTradePrevention, StopActivation, StopReference, StopReferenceCursor, StopTriggerSweep,
    TimeInForce, TradingStateControl, TradingStateControlAction,
    TradingStateControlSubmissionObservation,
};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedOrderBook,
    RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    StopReferenceSequence, StopReferenceSourceId, StopReferenceSourceVersion, TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-conditional-trading-state-control-{label}-{}-{nonce}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
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

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("CONDITIONAL-TRADING-STATE").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(-1_000), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 100).unwrap(),
        reserve: ReserveOrderRules::new(32).unwrap(),
        hidden_orders_supported: true,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn order(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    price: i64,
    display: OrderDisplay,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display,
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn dormant_stop(command_id: u64, order_id: u64, owner: u64, quantity: u64) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side: Side::Buy,
        quantity: Quantity::new(quantity).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Stop {
            trigger_price: Price::from_raw(120),
            activation: StopActivation::Limit(Price::from_raw(115)),
        },
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn reference(command_id: u64) -> Command {
    Command::StopTriggerSweep(StopTriggerSweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        reference: StopReference::new(
            StopReferenceCursor::new(
                StopReferenceSourceId::new(1).unwrap(),
                StopReferenceSourceVersion::new(1).unwrap(),
                StopReferenceSequence::new(1).unwrap(),
            ),
            Price::from_raw(100),
        ),
        maximum_orders: 1,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn control(
    command_id: u64,
    expected_revision: u64,
    target_state: TradingState,
    action: TradingStateControlAction,
) -> TradingStateControl {
    TradingStateControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        expected_revision,
        target_state,
        action,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn wrong_version_control(command_id: u64) -> TradingStateControl {
    TradingStateControl {
        instrument_version: InstrumentVersion::new(2).unwrap(),
        ..control(
            command_id,
            0,
            TradingState::Halted,
            TradingStateControlAction::TransitionAndCancel,
        )
    }
}

fn profile() -> RiskProfile {
    RiskProfile::new(
        AccountRiskState::Active,
        0,
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots: 100,
            max_order_notional: 100_000,
            max_open_orders: 16,
            max_open_quantity_lots: 1_000,
            max_open_notional: 1_000_000,
            max_long_position_lots: 1_000,
            max_short_position_lots: 1_000,
        })
        .unwrap(),
    )
    .unwrap()
}

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn frame_count(path: &Path) -> usize {
    JournalReader::open(path, options())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .len()
}

fn selected_ids(observation: &TradingStateControlSubmissionObservation) -> Vec<u64> {
    observation
        .orders()
        .iter()
        .map(|order| order.order_id().get())
        .collect()
}

fn assert_plain_type(
    result: Result<
        ConditionalCommandOutcome<TradingStateControlSubmissionObservation>,
        ConditionalOrderError,
    >,
) -> Result<
    ConditionalCommandOutcome<TradingStateControlSubmissionObservation>,
    ConditionalOrderError,
> {
    result
}

fn seed_mixed_book(book: &mut OrderBook) {
    book.submit(order(
        1,
        30,
        11,
        Side::Buy,
        3,
        90,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    book.submit(order(2, 10, 12, Side::Sell, 4, 110, OrderDisplay::Hidden))
        .unwrap();
    book.submit(order(
        3,
        20,
        11,
        Side::Buy,
        5,
        89,
        OrderDisplay::Reserve {
            peak: Quantity::new(2).unwrap(),
        },
    ))
    .unwrap();
    book.submit(order(
        4,
        40,
        13,
        Side::Sell,
        7,
        111,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    book.submit(reference(5)).unwrap();
    book.submit(dormant_stop(6, 5, 14, 6)).unwrap();
}

#[test]
fn trading_state_control_submission_observation_is_send_and_sync() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TradingStateControlSubmissionObservation>();
}

#[test]
fn transition_and_cancel_decline_unwind_accept_and_replay_share_one_selection() {
    let mut book = OrderBook::new(definition());
    seed_mixed_book(&mut book);
    let command = control(
        7,
        0,
        TradingState::Halted,
        TradingStateControlAction::TransitionAndCancel,
    );
    let sequence_before = book.last_event_sequence();
    let pool_before = book.order_selection_pool_status();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.try_submit_trading_state_control_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert_eq!(book.order_selection_pool_status(), pool_before);
    assert_eq!(book.active_order_count(), 5);

    let declined = assert_plain_type(book.try_submit_trading_state_control_if(
        command,
        |observation| {
            assert_eq!(observation.instrument_id(), InstrumentId::new(1).unwrap());
            assert_eq!(
                observation.instrument_version(),
                InstrumentVersion::new(1).unwrap()
            );
            assert_eq!(observation.book_event_sequence(), sequence_before);
            assert_eq!(observation.control().state(), TradingState::Open);
            assert_eq!(observation.control().revision(), 0);
            assert_eq!(observation.target_state(), TradingState::Halted);
            assert_eq!(
                observation.action(),
                TradingStateControlAction::TransitionAndCancel
            );
            assert_eq!(observation.resulting_state(), TradingState::Halted);
            assert_eq!(observation.selected_order_count(), 5);
            assert_eq!(observation.selected_quantity_lots(), 25);
            assert_eq!(selected_ids(observation), [5, 10, 20, 30, 40]);
            assert!(matches!(
                observation.orders()[0],
                ActiveOrderSnapshot::DormantStop(_)
            ));
            false
        },
    ))
    .unwrap();
    let ConditionalCommandOutcome::Declined(original) = declined else {
        panic!("predicate decline retains the complete state-control observation");
    };
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert_eq!(book.order_selection_pool_status(), pool_before);

    let accepted = book
        .try_submit_trading_state_control_if(command, |observation| observation == &original)
        .unwrap();
    let ConditionalCommandOutcome::Reported {
        observation: Some(committed_observation),
        report,
    } = accepted
    else {
        panic!("accepted state control retains its observation");
    };
    assert_eq!(committed_observation, original);
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    let cancelled = report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::OrderCancelled {
                order_id,
                reason: CancelReason::TradingStateControl,
                ..
            } => Some(order_id.get()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(cancelled, [5, 10, 20, 30, 40]);
    assert_eq!(book.order_selection_pool_status(), pool_before);
    assert_eq!(book.trading_state().state(), TradingState::Halted);
    assert_eq!(book.trading_state().revision(), 1);
    assert_eq!(book.active_order_count(), 0);

    assert!(matches!(
        book.try_submit_trading_state_control_if(command, |_| {
            panic!("replay bypasses observation")
        })
        .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));
    assert_eq!(book.order_selection_pool_status(), pool_before);
    book.validate().unwrap();
}

#[test]
fn transition_only_observes_state_without_selection_or_lease() {
    let mut book = OrderBook::new(definition());
    book.submit(order(
        1,
        1,
        11,
        Side::Buy,
        3,
        90,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    let command = control(
        2,
        0,
        TradingState::CancelOnly,
        TradingStateControlAction::Transition,
    );
    let sequence_before = book.last_event_sequence();
    let pool_before = book.order_selection_pool_status();

    assert!(matches!(
        book.try_submit_trading_state_control_if(command, |observation| {
            assert_eq!(observation.book_event_sequence(), sequence_before);
            assert_eq!(observation.control().state(), TradingState::Open);
            assert_eq!(observation.control().revision(), 0);
            assert_eq!(observation.target_state(), TradingState::CancelOnly);
            assert_eq!(observation.action(), TradingStateControlAction::Transition);
            assert_eq!(observation.resulting_state(), TradingState::CancelOnly);
            assert!(observation.orders().is_empty());
            assert_eq!(observation.selected_order_count(), 0);
            assert_eq!(observation.selected_quantity_lots(), 0);
            false
        })
        .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert_eq!(book.order_selection_pool_status(), pool_before);
    assert!(book.order(OrderId::new(1).unwrap()).is_some());

    assert!(matches!(
        book.try_submit_trading_state_control_if(command, |_| true)
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(book.trading_state().state(), TradingState::CancelOnly);
    assert_eq!(book.trading_state().revision(), 1);
    assert!(book.order(OrderId::new(1).unwrap()).is_some());
    assert_eq!(book.order_selection_pool_status(), pool_before);
    book.validate().unwrap();
}

#[test]
fn empty_transition_and_cancel_still_invokes_the_predicate() {
    let mut book = OrderBook::new(definition());
    let command = control(
        1,
        0,
        TradingState::Closed,
        TradingStateControlAction::TransitionAndCancel,
    );
    let pool_before = book.order_selection_pool_status();
    let called = Cell::new(false);

    assert!(matches!(
        book.try_submit_trading_state_control_if(command, |observation| {
            called.set(true);
            assert_eq!(observation.control().state(), TradingState::Open);
            assert!(observation.orders().is_empty());
            assert_eq!(observation.selected_order_count(), 0);
            assert_eq!(observation.selected_quantity_lots(), 0);
            false
        })
        .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert!(called.get());
    assert_eq!(book.order_selection_pool_status(), pool_before);

    let accepted = book
        .try_submit_trading_state_control_if(command, |_| true)
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted && report.events.len() == 1
    ));
    assert_eq!(book.trading_state().state(), TradingState::Closed);
    assert_eq!(book.order_selection_pool_status(), pool_before);
    book.validate().unwrap();
}

#[test]
fn core_rejections_bypass_observation_predicate_and_selection() {
    let mut book = OrderBook::new(definition());
    book.submit(order(
        1,
        1,
        11,
        Side::Buy,
        3,
        90,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    let pool_before = book.order_selection_pool_status();
    for (command, reason) in [
        (
            wrong_version_control(2),
            RejectReason::WrongInstrumentVersion,
        ),
        (
            control(
                3,
                1,
                TradingState::Halted,
                TradingStateControlAction::TransitionAndCancel,
            ),
            RejectReason::TradingStateControlRevisionMismatch,
        ),
        (
            control(
                4,
                0,
                TradingState::Open,
                TradingStateControlAction::Transition,
            ),
            RejectReason::TradingStateUnchanged,
        ),
    ] {
        let called = Cell::new(false);
        let rejected = book
            .try_submit_trading_state_control_if(command, |_| {
                called.set(true);
                true
            })
            .unwrap();
        assert!(!called.get());
        assert!(matches!(
            rejected,
            ConditionalCommandOutcome::Reported {
                observation: None,
                report
            } if matches!(report.outcome, CommandOutcome::Rejected(actual) if actual == reason)
        ));
        assert_eq!(book.order_selection_pool_status(), pool_before);
        assert!(book.order(OrderId::new(1).unwrap()).is_some());
    }

    book.submit(Command::TradingStateControl(control(
        5,
        0,
        TradingState::Halted,
        TradingStateControlAction::Transition,
    )))
    .unwrap();
    let called = Cell::new(false);
    let rejected = book
        .try_submit_trading_state_control_if(
            control(
                6,
                1,
                TradingState::Open,
                TradingStateControlAction::TransitionAndCancel,
            ),
            |_| {
                called.set(true);
                true
            },
        )
        .unwrap();
    assert!(!called.get());
    assert!(matches!(
        rejected,
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(
            RejectReason::TradingStateControlCannotCancelIntoOpen
        )
    ));
    assert_eq!(book.order_selection_pool_status(), pool_before);
    assert!(book.order(OrderId::new(1).unwrap()).is_some());
    assert_eq!(book.trading_state().state(), TradingState::Halted);
    book.validate().unwrap();
}

#[test]
fn coupled_risk_decline_retains_all_reservations_and_acceptance_releases_them() {
    let mut managed = RiskManagedOrderBook::new(definition());
    managed.register_account(account(11), profile()).unwrap();
    managed.register_account(account(12), profile()).unwrap();
    managed
        .submit(order(
            1,
            1,
            11,
            Side::Buy,
            3,
            90,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    managed
        .submit(order(
            2,
            2,
            12,
            Side::Sell,
            5,
            110,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    let command = control(
        3,
        0,
        TradingState::Closed,
        TradingStateControlAction::TransitionAndCancel,
    );

    assert!(matches!(
        managed
            .try_submit_trading_state_control_if(command, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert_eq!(managed.risk().reservation_count(), 2);

    assert!(matches!(
        managed
            .try_submit_trading_state_control_if(command, |observation| {
                selected_ids(observation) == [1, 2]
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(managed.risk().reservation_count(), 0);
    assert_eq!(managed.book().trading_state().state(), TradingState::Closed);
    managed.validate().unwrap();
}

#[test]
fn durable_plain_noncommit_is_wal_free_and_acceptance_recovers() {
    let file = TestFile::new("plain");
    let mut durable = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    durable
        .submit(order(
            1,
            1,
            11,
            Side::Buy,
            3,
            90,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    let frames_after_order = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_trading_state_control_if(wrong_version_control(2), |_| {
                panic!("business rejection bypasses observation")
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::WrongInstrumentVersion)
    ));
    assert_eq!(frame_count(file.path()), frames_after_order + 2);

    let command = control(
        3,
        0,
        TradingState::Halted,
        TradingStateControlAction::TransitionAndCancel,
    );
    let frames_before = frame_count(file.path());
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.try_submit_trading_state_control_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable
            .try_submit_trading_state_control_if(command, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);

    assert!(matches!(
        durable
            .try_submit_trading_state_control_if(command, |_| true)
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_trading_state_control_if(command, |_| {
                panic!("replay bypasses observation")
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));
    assert_eq!(frame_count(file.path()), frames_after);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    assert!(reopened.book().order(OrderId::new(1).unwrap()).is_none());
    assert_eq!(
        reopened.book().trading_state().state(),
        TradingState::Halted
    );
    assert_eq!(reopened.book().trading_state().revision(), 1);
    reopened.book().validate().unwrap();
}

#[test]
fn durable_risk_noncommit_and_acceptance_preserve_coupled_recovery() {
    let file = TestFile::new("risk");
    let profiles = [AccountRiskDefinition::new(account(11), profile())];
    let mut durable =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles, options()).unwrap();
    durable
        .submit(order(
            1,
            1,
            11,
            Side::Sell,
            4,
            110,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    let command = control(
        2,
        0,
        TradingState::Closed,
        TradingStateControlAction::TransitionAndCancel,
    );
    let frames_before = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_trading_state_control_if(command, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert_eq!(durable.managed().risk().reservation_count(), 1);

    assert!(matches!(
        durable
            .try_submit_trading_state_control_if(command, |_| true)
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    assert_eq!(durable.managed().risk().reservation_count(), 0);
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_trading_state_control_if(command, |_| {
                panic!("replay bypasses observation")
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));
    assert_eq!(frame_count(file.path()), frames_after);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles, options()).unwrap();
    assert_eq!(reopened.managed().risk().reservation_count(), 0);
    assert!(
        reopened
            .managed()
            .book()
            .order(OrderId::new(1).unwrap())
            .is_none()
    );
    assert_eq!(
        reopened.managed().book().trading_state().state(),
        TradingState::Closed
    );
    assert_eq!(reopened.managed().book().trading_state().revision(), 1);
    reopened.managed().validate().unwrap();
}
