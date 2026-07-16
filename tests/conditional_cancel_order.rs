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
    ActiveOrderObservation, ActiveOrderSnapshot, CancelOrder, Command, CommandOutcome,
    ConditionalCommandOutcome, ConditionalOrderError, NewOrder, OrderBook, OrderDisplay, OrderType,
    RejectReason, SelfTradePrevention, StopActivation, StopReference, StopReferenceCursor,
    StopTriggerSweep, TimeInForce,
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
            "quotick-conditional-cancel-{label}-{}-{nonce}.wal",
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
        symbol: InstrumentSymbol::new("CONDITIONAL-CANCEL").unwrap(),
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

fn new_order(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    price: i64,
) -> NewOrder {
    NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn limit(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    price: i64,
) -> Command {
    Command::New(new_order(
        command_id, order_id, owner, side, quantity, price,
    ))
}

fn cancel(command_id: u64, order_id: u64, owner: u64) -> CancelOrder {
    CancelOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn stop_reference(command_id: u64, price: i64) -> Command {
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
            Price::from_raw(price),
        ),
        maximum_orders: 1,
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
            max_open_orders: 10,
            max_open_quantity_lots: 100,
            max_open_notional: 100_000,
            max_long_position_lots: 100,
            max_short_position_lots: 100,
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

fn assert_plain_type(
    result: Result<ConditionalCommandOutcome<ActiveOrderObservation>, ConditionalOrderError>,
) -> Result<ConditionalCommandOutcome<ActiveOrderObservation>, ConditionalOrderError> {
    result
}

#[test]
fn resting_cancel_decline_unwind_accept_and_replay_are_atomic() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Buy, 5, 100)).unwrap();
    let cancellation = cancel(2, 1, 11);
    let original = book
        .try_active_order_observation(cancellation.order_id)
        .unwrap();
    let sequence_before = book.last_event_sequence();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.submit_cancel_order_if(cancellation, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(
        book.try_active_order_observation(cancellation.order_id)
            .unwrap(),
        original
    );
    assert_eq!(book.last_event_sequence(), sequence_before);

    let declined = assert_plain_type(book.submit_cancel_order_if(cancellation, |observation| {
        assert_eq!(observation, &original);
        let ActiveOrderSnapshot::Resting(order) = observation.state().unwrap() else {
            panic!("resting state is observed before cancellation");
        };
        assert_eq!(order.leaves_quantity.lots(), 5);
        false
    }))
    .unwrap();
    assert_eq!(declined, ConditionalCommandOutcome::Declined(original));
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert!(book.order(cancellation.order_id).is_some());

    let accepted = book
        .submit_cancel_order_if(cancellation, |observation| {
            observation.book_event_sequence() == sequence_before
        })
        .unwrap();
    let ConditionalCommandOutcome::Reported {
        observation: Some(committed_observation),
        report,
    } = accepted
    else {
        panic!("accepted cancellation retains its observation");
    };
    assert_eq!(committed_observation, original);
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert!(book.order(cancellation.order_id).is_none());

    assert!(matches!(
        book.submit_cancel_order_if(cancellation, |_| panic!("replay bypasses observation"))
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));
    book.validate().unwrap();
}

#[test]
fn core_rejections_bypass_the_predicate_and_preserve_the_target() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Sell, 4, 100)).unwrap();
    let original = book
        .try_active_order_observation(OrderId::new(1).unwrap())
        .unwrap();
    let called = Cell::new(false);

    let unknown = book
        .submit_cancel_order_if(cancel(2, 99, 11), |_| {
            called.set(true);
            true
        })
        .unwrap();
    assert!(!called.get());
    assert!(matches!(
        unknown,
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::UnknownOrder)
    ));

    let wrong_owner = book
        .submit_cancel_order_if(cancel(3, 1, 12), |_| {
            panic!("ownership rejection bypasses the predicate")
        })
        .unwrap();
    assert!(matches!(
        wrong_owner,
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::NotOrderOwner)
    ));
    let retained = book
        .try_active_order_observation(OrderId::new(1).unwrap())
        .unwrap();
    assert_eq!(retained.instrument_id(), original.instrument_id());
    assert_eq!(retained.instrument_version(), original.instrument_version());
    assert_eq!(retained.order_id(), original.order_id());
    assert_eq!(retained.state(), original.state());
    assert!(retained.book_event_sequence() > original.book_event_sequence());
    book.validate().unwrap();
}

#[test]
fn dormant_stop_cancel_has_explicit_provenance_bound_state() {
    let mut book = OrderBook::new(definition());
    book.submit(stop_reference(1, 100)).unwrap();
    let mut stop = new_order(2, 1, 11, Side::Buy, 3, 105);
    stop.order_type = OrderType::Stop {
        trigger_price: Price::from_raw(110),
        activation: StopActivation::Limit(Price::from_raw(105)),
    };
    book.submit(Command::New(stop)).unwrap();
    let cancellation = cancel(3, 1, 11);
    let original = book
        .try_active_order_observation(cancellation.order_id)
        .unwrap();

    let declined = book
        .submit_cancel_order_if(cancellation, |observation| {
            let ActiveOrderSnapshot::DormantStop(stop) = observation.state().unwrap() else {
                panic!("dormant state is observed before cancellation");
            };
            assert_eq!(stop.trigger_price, Price::from_raw(110));
            assert_eq!(stop.activation, StopActivation::Limit(Price::from_raw(105)));
            false
        })
        .unwrap();
    assert_eq!(declined, ConditionalCommandOutcome::Declined(original));
    assert!(book.dormant_stop(cancellation.order_id).is_some());

    assert!(matches!(
        book.submit_cancel_order_if(cancellation, |_| true).unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(observation),
            report
        } if observation == original && report.outcome == CommandOutcome::Accepted
    ));
    assert!(book.dormant_stop(cancellation.order_id).is_none());
    book.validate().unwrap();
}

#[test]
fn coupled_risk_decline_retains_and_acceptance_releases_the_reservation() {
    let mut managed = RiskManagedOrderBook::new(definition());
    managed.register_account(account(11), profile()).unwrap();
    managed.submit(limit(1, 1, 11, Side::Buy, 5, 100)).unwrap();
    let cancellation = cancel(2, 1, 11);
    let original = managed
        .book()
        .try_active_order_observation(cancellation.order_id)
        .unwrap();

    assert_eq!(
        managed
            .submit_cancel_order_if(cancellation, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(original)
    );
    assert_eq!(
        managed
            .risk()
            .reservation(cancellation.order_id)
            .unwrap()
            .quantity_lots(),
        5
    );

    assert!(matches!(
        managed.submit_cancel_order_if(cancellation, |_| true).unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(observation),
            report
        } if observation == original && report.outcome == CommandOutcome::Accepted
    ));
    assert!(managed.risk().reservation(cancellation.order_id).is_none());
    managed.validate().unwrap();
}

#[test]
fn durable_plain_nonacceptance_is_wal_free_and_acceptance_recovers() {
    let file = TestFile::new("plain");
    let mut durable = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    durable.submit(limit(1, 1, 11, Side::Sell, 5, 100)).unwrap();
    let frames_after_order = frame_count(file.path());
    assert!(matches!(
        durable
            .submit_cancel_order_if(cancel(2, 99, 11), |_| {
                panic!("business rejection bypasses observation")
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::UnknownOrder)
    ));
    assert_eq!(frame_count(file.path()), frames_after_order + 2);

    let cancellation = cancel(3, 1, 11);
    let frames_before = frame_count(file.path());

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.submit_cancel_order_if(cancellation, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(durable.book().order(cancellation.order_id).is_some());

    assert!(matches!(
        durable
            .submit_cancel_order_if(cancellation, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);

    assert!(matches!(
        durable
            .submit_cancel_order_if(cancellation, |_| true)
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
            .submit_cancel_order_if(cancellation, |_| panic!("replay bypasses observation"))
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
    assert!(reopened.book().order(cancellation.order_id).is_none());
    reopened.book().validate().unwrap();
}

#[test]
fn durable_risk_nonacceptance_is_wal_free_and_acceptance_recovers() {
    let file = TestFile::new("risk");
    let profiles = [AccountRiskDefinition::new(account(11), profile())];
    let mut durable =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles, options()).unwrap();
    durable.submit(limit(1, 1, 11, Side::Buy, 5, 100)).unwrap();
    let frames_after_order = frame_count(file.path());
    assert!(matches!(
        durable
            .submit_cancel_order_if(cancel(2, 99, 11), |_| {
                panic!("business rejection bypasses observation")
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::UnknownOrder)
    ));
    assert_eq!(frame_count(file.path()), frames_after_order + 2);

    let cancellation = cancel(3, 1, 11);
    let frames_before = frame_count(file.path());

    assert!(matches!(
        durable
            .submit_cancel_order_if(cancellation, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(
        durable
            .managed()
            .risk()
            .reservation(cancellation.order_id)
            .is_some()
    );

    assert!(matches!(
        durable
            .submit_cancel_order_if(cancellation, |_| true)
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
            .submit_cancel_order_if(cancellation, |_| panic!("replay bypasses observation"))
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
    assert!(
        reopened
            .managed()
            .risk()
            .reservation(cancellation.order_id)
            .is_none()
    );
    assert!(
        reopened
            .managed()
            .book()
            .order(cancellation.order_id)
            .is_none()
    );
    reopened.managed().validate().unwrap();
}
