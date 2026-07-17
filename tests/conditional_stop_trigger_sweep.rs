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
    Command, CommandOutcome, CommandPreparation, ConditionalCommandOutcome, EventKind, MassCancel,
    MassCancelScope, NewOrder, OrderBook, OrderDisplay, OrderType, RejectReason,
    SelfTradePrevention, StopActivation, StopReference, StopReferenceCursor, StopTriggerSweep,
    StopTriggerSweepObservation, TimeInForce,
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
            "quotick-conditional-stop-trigger-sweep-{label}-{}-{nonce}.wal",
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
        symbol: InstrumentSymbol::new("CONDITIONAL-STOP-SWEEP").unwrap(),
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

fn cursor(sequence: u64) -> StopReferenceCursor {
    StopReferenceCursor::new(
        StopReferenceSourceId::new(1).unwrap(),
        StopReferenceSourceVersion::new(1).unwrap(),
        StopReferenceSequence::new(sequence).unwrap(),
    )
}

fn reference(sequence: u64, price: i64) -> StopReference {
    StopReference::new(cursor(sequence), Price::from_raw(price))
}

fn sweep(command_id: u64, sequence: u64, price: i64, maximum_orders: u32) -> StopTriggerSweep {
    StopTriggerSweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        reference: reference(sequence, price),
        maximum_orders,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn wrong_version_sweep(command_id: u64) -> StopTriggerSweep {
    StopTriggerSweep {
        instrument_version: InstrumentVersion::new(2).unwrap(),
        ..sweep(command_id, 2, 105, 1)
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "stop fixtures keep trigger, activation, display, and lifetime fields explicit"
)]
fn stop_order(
    command_id: u64,
    order_id: u64,
    owner: u64,
    quantity: u64,
    trigger_price: i64,
    activation: StopActivation,
    display: OrderDisplay,
    time_in_force: TimeInForce,
) -> Command {
    stop_order_on_side(
        command_id,
        order_id,
        owner,
        Side::Buy,
        quantity,
        trigger_price,
        activation,
        display,
        time_in_force,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "stop fixtures keep side, trigger, activation, display, and lifetime fields explicit"
)]
fn stop_order_on_side(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    trigger_price: i64,
    activation: StopActivation,
    display: OrderDisplay,
    time_in_force: TimeInForce,
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
        order_type: OrderType::Stop {
            trigger_price: Price::from_raw(trigger_price),
            activation,
        },
        time_in_force,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn gtc_order(command_id: u64, order_id: u64, owner: u64) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side: Side::Sell,
        quantity: Quantity::new(2).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(150)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
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

fn stop_ids(observation: &StopTriggerSweepObservation) -> Vec<u64> {
    observation
        .stops()
        .iter()
        .map(|stop| stop.order_id.get())
        .collect()
}

fn trigger_prices(observation: &StopTriggerSweepObservation) -> Vec<i64> {
    observation
        .stops()
        .iter()
        .map(|stop| stop.trigger_price.raw())
        .collect()
}

fn initialize_reference(book: &mut OrderBook) {
    book.submit(Command::StopTriggerSweep(sweep(1, 1, 100, 1)))
        .unwrap();
}

fn seed_mixed_stops(book: &mut OrderBook) {
    initialize_reference(book);
    book.submit(stop_order(
        2,
        30,
        11,
        3,
        105,
        StopActivation::Limit(Price::from_raw(90)),
        OrderDisplay::FullyDisplayed,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    book.submit(stop_order(
        3,
        20,
        12,
        4,
        110,
        StopActivation::Limit(Price::from_raw(91)),
        OrderDisplay::Hidden,
        TimeInForce::GoodTilTimestamp {
            expires_at: TimestampNs::from_unix_nanos(1_000),
        },
    ))
    .unwrap();
    book.submit(stop_order(
        4,
        5,
        13,
        5,
        110,
        StopActivation::Limit(Price::from_raw(92)),
        OrderDisplay::Reserve {
            peak: Quantity::new(2).unwrap(),
        },
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    book.submit(stop_order(
        5,
        10,
        14,
        6,
        115,
        StopActivation::Market,
        OrderDisplay::FullyDisplayed,
        TimeInForce::ImmediateOrCancel,
    ))
    .unwrap();
    book.submit(stop_order(
        6,
        40,
        15,
        7,
        120,
        StopActivation::Limit(Price::from_raw(93)),
        OrderDisplay::FullyDisplayed,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
}

#[test]
fn stop_trigger_sweep_observation_is_send_and_sync() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<StopTriggerSweepObservation>();
}

#[test]
fn decline_unwind_accept_replay_and_continuation_share_one_canonical_stop_prefix() {
    let mut book = OrderBook::new(definition());
    seed_mixed_stops(&mut book);
    let command = sweep(7, 2, 115, 3);
    let sequence_before = book.last_event_sequence();
    let pool_before = book.order_selection_pool_status();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.try_submit_stop_trigger_sweep_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert_eq!(book.stop_reference(), Some(reference(1, 100)));
    assert_eq!(book.order_selection_pool_status(), pool_before);

    let declined = book
        .try_submit_stop_trigger_sweep_if(command, |observation| {
            assert_eq!(observation.instrument_id(), InstrumentId::new(1).unwrap());
            assert_eq!(
                observation.instrument_version(),
                InstrumentVersion::new(1).unwrap()
            );
            assert_eq!(observation.book_event_sequence(), sequence_before);
            assert_eq!(observation.previous_reference(), Some(reference(1, 100)));
            assert_eq!(observation.reference(), reference(2, 115));
            assert_eq!(observation.current_reference(), reference(2, 115));
            assert_eq!(observation.maximum_orders(), 3);
            assert_eq!(observation.triggered_order_count(), 3);
            assert_eq!(observation.triggered_quantity_lots(), 12);
            assert_eq!(observation.remaining_eligible_order_count(), 1);
            assert_eq!(stop_ids(observation), [30, 20, 5]);
            assert_eq!(trigger_prices(observation), [105, 110, 110]);
            false
        })
        .unwrap();
    let ConditionalCommandOutcome::Declined(original) = declined else {
        panic!("predicate decline retains the complete stop observation");
    };
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert_eq!(book.stop_reference(), Some(reference(1, 100)));
    assert_eq!(book.order_selection_pool_status(), pool_before);

    let accepted = book
        .try_submit_stop_trigger_sweep_if(command, |observation| observation == &original)
        .unwrap();
    let ConditionalCommandOutcome::Reported {
        observation: Some(committed_observation),
        report,
    } = accepted
    else {
        panic!("accepted stop sweep retains its observation");
    };
    assert_eq!(committed_observation, original);
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    let triggered = report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::StopOrderTriggered { order_id, .. } => Some(order_id.get()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(triggered, [30, 20, 5]);
    assert!(matches!(
        report.events.last().unwrap().kind,
        EventKind::StopTriggerSweepCompleted {
            triggered_order_count: 3,
            remaining_eligible_order_count: 1,
            ..
        }
    ));
    assert_eq!(book.stop_reference(), Some(reference(2, 115)));
    assert!(book.dormant_stop(OrderId::new(10).unwrap()).is_some());
    assert!(book.dormant_stop(OrderId::new(40).unwrap()).is_some());
    assert_eq!(book.order_selection_pool_status(), pool_before);

    assert!(matches!(
        book.try_submit_stop_trigger_sweep_if(command, |_| {
            panic!("replay bypasses observation")
        })
        .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));

    let continuation = sweep(8, 2, 115, 3);
    assert!(matches!(
        book.try_submit_stop_trigger_sweep_if(continuation, |observation| {
            assert_eq!(observation.previous_reference(), Some(reference(2, 115)));
            assert_eq!(observation.current_reference(), reference(2, 115));
            assert_eq!(stop_ids(observation), [10]);
            assert_eq!(observation.remaining_eligible_order_count(), 0);
            true
        })
        .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert!(book.dormant_stop(OrderId::new(10).unwrap()).is_none());
    assert!(book.dormant_stop(OrderId::new(40).unwrap()).is_some());
    book.validate().unwrap();
}

#[test]
fn sell_stop_prefix_is_canonical_by_descending_trigger_then_time_priority() {
    let mut book = OrderBook::new(definition());
    initialize_reference(&mut book);
    for command in [
        stop_order_on_side(
            2,
            30,
            11,
            Side::Sell,
            3,
            95,
            StopActivation::Market,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ),
        stop_order_on_side(
            3,
            5,
            12,
            Side::Sell,
            5,
            95,
            StopActivation::Market,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ),
        stop_order_on_side(
            4,
            20,
            13,
            Side::Sell,
            7,
            90,
            StopActivation::Market,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ),
    ] {
        book.submit(command).unwrap();
    }

    let outcome = book
        .try_submit_stop_trigger_sweep_if(sweep(5, 2, 90, 3), |observation| {
            assert_eq!(stop_ids(observation), [30, 5, 20]);
            assert_eq!(trigger_prices(observation), [95, 95, 90]);
            assert_eq!(observation.triggered_quantity_lots(), 15);
            assert_eq!(observation.remaining_eligible_order_count(), 0);
            assert!(
                observation
                    .stops()
                    .iter()
                    .all(|stop| stop.side == Side::Sell)
            );
            true
        })
        .unwrap();
    let ConditionalCommandOutcome::Reported {
        observation: Some(observation),
        report,
    } = outcome
    else {
        panic!("accepted sell-side sweep retains its observation");
    };
    assert_eq!(stop_ids(&observation), [30, 5, 20]);
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(book.stop_reference(), Some(reference(2, 90)));
    assert_eq!(book.dormant_stop_count(), 0);
    book.validate().unwrap();
}

#[test]
fn empty_stop_prefix_invokes_the_predicate_without_a_selection_lease() {
    let mut book = OrderBook::new(definition());
    initialize_reference(&mut book);
    book.submit(gtc_order(2, 1, 11)).unwrap();
    let first = book
        .prepare(Command::MassCancel(MassCancel {
            command_id: CommandId::new(3).unwrap(),
            account_id: account(11),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            scope: MassCancelScope::All,
            received_at: TimestampNs::from_unix_nanos(3),
        }))
        .unwrap();
    let second = book
        .prepare(Command::MassCancel(MassCancel {
            command_id: CommandId::new(4).unwrap(),
            account_id: account(11),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            scope: MassCancelScope::All,
            received_at: TimestampNs::from_unix_nanos(4),
        }))
        .unwrap();
    assert!(matches!(first, CommandPreparation::Ready(_)));
    assert!(matches!(second, CommandPreparation::Ready(_)));
    assert_eq!(book.order_selection_pool_status().available_leases, 0);

    let command = sweep(5, 2, 101, 1);
    let called = Cell::new(false);
    assert!(matches!(
        book.try_submit_stop_trigger_sweep_if(command, |observation| {
            called.set(true);
            assert!(observation.stops().is_empty());
            assert_eq!(observation.triggered_order_count(), 0);
            assert_eq!(observation.triggered_quantity_lots(), 0);
            assert_eq!(observation.remaining_eligible_order_count(), 0);
            false
        })
        .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert!(called.get());
    assert_eq!(book.order_selection_pool_status().available_leases, 0);
    drop((first, second));
    assert_eq!(book.order_selection_pool_status().available_leases, 2);

    assert!(matches!(
        book.try_submit_stop_trigger_sweep_if(command, |_| true).unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted && report.events.len() == 1
    ));
    assert_eq!(book.stop_reference(), Some(reference(2, 101)));
    book.validate().unwrap();
}

#[test]
fn core_rejections_bypass_stop_observation_predicate_and_selection() {
    let mut book = OrderBook::new(definition());
    initialize_reference(&mut book);
    let pool_before = book.order_selection_pool_status();
    for (command, reason) in [
        (wrong_version_sweep(2), RejectReason::WrongInstrumentVersion),
        (sweep(3, 2, 105, 0), RejectReason::StopTriggerBatchEmpty),
        (
            sweep(4, 3, 105, 1),
            RejectReason::StopReferenceSequenceDiscontinuity,
        ),
    ] {
        let called = Cell::new(false);
        let rejected = book
            .try_submit_stop_trigger_sweep_if(command, |_| {
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
            } if report.outcome == CommandOutcome::Rejected(reason)
        ));
        assert_eq!(book.stop_reference(), Some(reference(1, 100)));
        assert_eq!(book.order_selection_pool_status(), pool_before);
    }
    book.validate().unwrap();
}

#[test]
fn coupled_risk_decline_retains_and_acceptance_transitions_only_selected_reservations() {
    let mut managed = RiskManagedOrderBook::new(definition());
    for owner in [11, 12, 13] {
        managed.register_account(account(owner), profile()).unwrap();
    }
    managed
        .submit(Command::StopTriggerSweep(sweep(1, 1, 100, 1)))
        .unwrap();
    managed
        .submit(stop_order(
            2,
            1,
            11,
            3,
            110,
            StopActivation::Limit(Price::from_raw(90)),
            OrderDisplay::FullyDisplayed,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    managed
        .submit(stop_order(
            3,
            2,
            12,
            4,
            110,
            StopActivation::Market,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ))
        .unwrap();
    managed
        .submit(stop_order(
            4,
            3,
            13,
            5,
            120,
            StopActivation::Limit(Price::from_raw(91)),
            OrderDisplay::FullyDisplayed,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    let command = sweep(5, 2, 110, 2);

    assert!(matches!(
        managed
            .try_submit_stop_trigger_sweep_if(command, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    for order_id in [1, 2, 3] {
        assert!(
            managed
                .risk()
                .reservation(OrderId::new(order_id).unwrap())
                .unwrap()
                .is_dormant_stop()
        );
    }

    assert!(matches!(
        managed
            .try_submit_stop_trigger_sweep_if(command, |observation| {
                stop_ids(observation) == [1, 2]
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert!(
        !managed
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .is_dormant_stop()
    );
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .is_none()
    );
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(3).unwrap())
            .unwrap()
            .is_dormant_stop()
    );
    assert_eq!(managed.risk().reservation_count(), 2);
    managed.validate().unwrap();
}

#[test]
fn durable_plain_noncommit_is_wal_free_and_acceptance_recovers() {
    let file = TestFile::new("plain");
    let mut durable = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    durable
        .submit(Command::StopTriggerSweep(sweep(1, 1, 100, 1)))
        .unwrap();
    durable
        .submit(stop_order(
            2,
            1,
            11,
            3,
            110,
            StopActivation::Limit(Price::from_raw(90)),
            OrderDisplay::FullyDisplayed,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    let frames_after_order = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_stop_trigger_sweep_if(wrong_version_sweep(3), |_| {
                panic!("business rejection bypasses observation")
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::WrongInstrumentVersion)
    ));
    assert_eq!(frame_count(file.path()), frames_after_order + 2);

    let command = sweep(4, 2, 110, 1);
    let frames_before = frame_count(file.path());
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.try_submit_stop_trigger_sweep_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable
            .try_submit_stop_trigger_sweep_if(command, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);

    assert!(matches!(
        durable
            .try_submit_stop_trigger_sweep_if(command, |_| true)
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
            .try_submit_stop_trigger_sweep_if(command, |_| {
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
    assert_eq!(reopened.book().stop_reference(), Some(reference(2, 110)));
    assert!(reopened.book().order(OrderId::new(1).unwrap()).is_some());
    assert!(
        reopened
            .book()
            .dormant_stop(OrderId::new(1).unwrap())
            .is_none()
    );
    reopened.book().validate().unwrap();
}

#[test]
fn durable_risk_noncommit_and_acceptance_preserve_coupled_recovery() {
    let file = TestFile::new("risk");
    let profiles = [AccountRiskDefinition::new(account(11), profile())];
    let mut durable =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles, options()).unwrap();
    durable
        .submit(Command::StopTriggerSweep(sweep(1, 1, 100, 1)))
        .unwrap();
    durable
        .submit(stop_order(
            2,
            1,
            11,
            4,
            110,
            StopActivation::Limit(Price::from_raw(90)),
            OrderDisplay::FullyDisplayed,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    let command = sweep(3, 2, 110, 1);
    let frames_before = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_stop_trigger_sweep_if(command, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(
        durable
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .is_dormant_stop()
    );

    assert!(matches!(
        durable
            .try_submit_stop_trigger_sweep_if(command, |_| true)
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    assert!(
        !durable
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .is_dormant_stop()
    );
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles, options()).unwrap();
    assert_eq!(
        reopened.managed().book().stop_reference(),
        Some(reference(2, 110))
    );
    assert!(
        !reopened
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .is_dormant_stop()
    );
    reopened.managed().validate().unwrap();
}
