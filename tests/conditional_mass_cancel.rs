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
    ConditionalOrderError, EventKind, MassCancel, MassCancelObservation, MassCancelScope, NewOrder,
    OrderBook, OrderDisplay, OrderType, RejectReason, SelfTradePrevention, StopActivation,
    StopReference, StopReferenceCursor, StopTriggerSweep, TimeInForce,
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
            "quotick-conditional-mass-cancel-{label}-{}-{nonce}.wal",
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
        symbol: InstrumentSymbol::new("CONDITIONAL-MASS-CANCEL").unwrap(),
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

fn mass_cancel(command_id: u64, owner: u64, scope: MassCancelScope) -> MassCancel {
    MassCancel {
        command_id: CommandId::new(command_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        scope,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn wrong_version_mass_cancel(command_id: u64, owner: u64) -> MassCancel {
    MassCancel {
        instrument_version: InstrumentVersion::new(2).unwrap(),
        ..mass_cancel(command_id, owner, MassCancelScope::All)
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

fn selected_ids(observation: &MassCancelObservation) -> Vec<u64> {
    observation
        .orders()
        .iter()
        .map(|order| order.order_id().get())
        .collect()
}

fn assert_plain_type(
    result: Result<ConditionalCommandOutcome<MassCancelObservation>, ConditionalOrderError>,
) -> Result<ConditionalCommandOutcome<MassCancelObservation>, ConditionalOrderError> {
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
    book.submit(order(2, 10, 11, Side::Sell, 4, 110, OrderDisplay::Hidden))
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
        12,
        Side::Sell,
        7,
        111,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    book.submit(reference(5)).unwrap();
    book.submit(dormant_stop(6, 5, 11, 6)).unwrap();
}

#[test]
fn all_scope_decline_unwind_accept_and_replay_use_one_canonical_selection() {
    let mut book = OrderBook::new(definition());
    seed_mixed_book(&mut book);
    let command = mass_cancel(7, 11, MassCancelScope::All);
    let sequence_before = book.last_event_sequence();
    let pool_before = book.order_selection_pool_status();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.try_submit_mass_cancel_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert_eq!(book.order_selection_pool_status(), pool_before);
    assert_eq!(
        book.account_active_order_count(account(11), MassCancelScope::All),
        4
    );

    let declined = assert_plain_type(book.try_submit_mass_cancel_if(command, |observation| {
        assert_eq!(observation.instrument_id(), InstrumentId::new(1).unwrap());
        assert_eq!(
            observation.instrument_version(),
            InstrumentVersion::new(1).unwrap()
        );
        assert_eq!(observation.book_event_sequence(), sequence_before);
        assert_eq!(observation.account_id(), account(11));
        assert_eq!(observation.scope(), MassCancelScope::All);
        assert_eq!(observation.selected_order_count(), 4);
        assert_eq!(observation.selected_quantity_lots(), 18);
        assert_eq!(selected_ids(observation), [5, 10, 20, 30]);
        assert!(matches!(
            observation.orders()[0],
            ActiveOrderSnapshot::DormantStop(_)
        ));
        false
    }))
    .unwrap();
    let ConditionalCommandOutcome::Declined(original) = declined else {
        panic!("predicate decline retains the complete selection");
    };
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert_eq!(book.order_selection_pool_status(), pool_before);

    let accepted = book
        .try_submit_mass_cancel_if(command, |observation| observation == &original)
        .unwrap();
    let ConditionalCommandOutcome::Reported {
        observation: Some(committed_observation),
        report,
    } = accepted
    else {
        panic!("accepted mass cancellation retains its observation");
    };
    assert_eq!(committed_observation, original);
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    let cancelled = report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::OrderCancelled {
                order_id,
                reason: CancelReason::MassCancel,
                ..
            } => Some(order_id.get()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(cancelled, [5, 10, 20, 30]);
    assert_eq!(book.order_selection_pool_status(), pool_before);
    assert_eq!(
        book.account_active_order_count(account(11), MassCancelScope::All),
        0
    );
    assert!(book.order(OrderId::new(40).unwrap()).is_some());

    assert!(matches!(
        book.try_submit_mass_cancel_if(command, |_| panic!("replay bypasses observation"))
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
fn side_scope_and_empty_selection_are_explicit() {
    let mut book = OrderBook::new(definition());
    book.submit(order(
        1,
        3,
        11,
        Side::Buy,
        3,
        90,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    book.submit(order(
        2,
        1,
        11,
        Side::Sell,
        5,
        110,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    book.submit(order(
        3,
        2,
        11,
        Side::Buy,
        4,
        89,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();

    let buy_cancel = mass_cancel(4, 11, MassCancelScope::Side(Side::Buy));
    assert!(matches!(
        book.try_submit_mass_cancel_if(buy_cancel, |observation| {
            assert_eq!(selected_ids(observation), [2, 3]);
            assert_eq!(observation.selected_quantity_lots(), 7);
            true
        })
        .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert!(book.order(OrderId::new(1).unwrap()).is_some());

    let empty_cancel = mass_cancel(5, 11, MassCancelScope::Side(Side::Buy));
    let called = Cell::new(false);
    assert!(matches!(
        book.try_submit_mass_cancel_if(empty_cancel, |observation| {
            called.set(true);
            assert!(observation.orders().is_empty());
            assert_eq!(observation.selected_order_count(), 0);
            assert_eq!(observation.selected_quantity_lots(), 0);
            true
        })
        .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert!(called.get());
    assert!(book.order(OrderId::new(1).unwrap()).is_some());
    book.validate().unwrap();
}

#[test]
fn core_rejection_bypasses_selection_and_predicate() {
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
    let called = Cell::new(false);
    let rejected = book
        .try_submit_mass_cancel_if(wrong_version_mass_cancel(2, 11), |_| {
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
        } if report.outcome == CommandOutcome::Rejected(RejectReason::WrongInstrumentVersion)
    ));
    assert_eq!(book.order_selection_pool_status(), pool_before);
    assert!(book.order(OrderId::new(1).unwrap()).is_some());
    book.validate().unwrap();
}

#[test]
fn coupled_risk_decline_retains_and_acceptance_releases_only_selected_reservations() {
    let mut managed = RiskManagedOrderBook::new(definition());
    managed.register_account(account(11), profile()).unwrap();
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
            11,
            Side::Sell,
            5,
            110,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    let command = mass_cancel(3, 11, MassCancelScope::Side(Side::Buy));

    assert!(matches!(
        managed
            .try_submit_mass_cancel_if(command, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .is_some()
    );
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .is_some()
    );

    assert!(matches!(
        managed
            .try_submit_mass_cancel_if(command, |observation| {
                selected_ids(observation) == [1]
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .is_none()
    );
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .is_some()
    );
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
            .try_submit_mass_cancel_if(wrong_version_mass_cancel(2, 11), |_| {
                panic!("business rejection bypasses selection")
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::WrongInstrumentVersion)
    ));
    assert_eq!(frame_count(file.path()), frames_after_order + 2);

    let command = mass_cancel(3, 11, MassCancelScope::All);
    let frames_before = frame_count(file.path());

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.try_submit_mass_cancel_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable
            .try_submit_mass_cancel_if(command, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);

    assert!(matches!(
        durable
            .try_submit_mass_cancel_if(command, |_| true)
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
            .try_submit_mass_cancel_if(command, |_| panic!("replay bypasses observation"))
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
    reopened.book().validate().unwrap();
}

#[test]
fn durable_risk_noncommit_is_wal_free_and_acceptance_recovers() {
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
    let frames_after_order = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_mass_cancel_if(wrong_version_mass_cancel(2, 11), |_| {
                panic!("business rejection bypasses selection")
            })
            .unwrap(),
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::WrongInstrumentVersion)
    ));
    assert_eq!(frame_count(file.path()), frames_after_order + 2);

    let command = mass_cancel(3, 11, MassCancelScope::All);
    let frames_before = frame_count(file.path());

    assert!(matches!(
        durable
            .try_submit_mass_cancel_if(command, |_| false)
            .unwrap(),
        ConditionalCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(
        durable
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .is_some()
    );

    assert!(matches!(
        durable
            .try_submit_mass_cancel_if(command, |_| true)
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
            .try_submit_mass_cancel_if(command, |_| panic!("replay bypasses observation"))
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
            .reservation(OrderId::new(1).unwrap())
            .is_none()
    );
    assert!(
        reopened
            .managed()
            .book()
            .order(OrderId::new(1).unwrap())
            .is_none()
    );
    reopened.managed().validate().unwrap();
}
