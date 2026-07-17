use std::cell::Cell;
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::auction::{
    AuctionOrderConstraint, AuctionPriceBand, AuctionPriceGrid, AuctionPricePolicy,
    AuctionPriorityClass,
};
use quotick::auction_book::{
    CallAuctionBook, CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder,
    CallAuctionOrderObservation,
};
use quotick::auction_engine::{
    CallAuctionCancelObservation, CallAuctionCancelOrder, CallAuctionCommand,
    CallAuctionCommandOutcome, CallAuctionEngine, CallAuctionEngineLimits,
    CallAuctionEngineLimitsSpec, CallAuctionEventKind, CallAuctionIndicativeCommand,
    CallAuctionPhase, CallAuctionPhaseControl, CallAuctionRejectReason, CallAuctionSubmitOrder,
    ConditionalCallAuctionCommandOutcome,
};
use quotick::auction_risk::{
    CallAuctionRiskLimits, CallAuctionRiskLimitsSpec, CallAuctionRiskManagedEngine,
};
use quotick::durable_auction::{DurableCallAuctionEngine, DurableCallAuctionRiskEngine};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions, JournalReader};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskLimitSpec, RiskLimits, RiskProfile,
};
use quotick::{
    AccountId, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, Side, TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-conditional-call-auction-cancel-{label}-{}-{nonce}.wal",
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

fn instrument() -> InstrumentId {
    InstrumentId::new(92).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(7).unwrap()
}

fn auction() -> AuctionId {
    AuctionId::new(1).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("CONDITIONAL-CANCEL").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Halted,
    })
    .unwrap()
}

fn book_limits() -> CallAuctionBookLimits {
    CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: 8,
        max_price_levels_per_side: 8,
        max_accepted_order_ids: 32,
        max_prepared_uncrosses: 2,
    })
    .unwrap()
}

fn engine_limits() -> CallAuctionEngineLimits {
    CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book: book_limits(),
        max_retained_commands: 48,
        terminal_command_reserve: 10,
        max_retained_events: 96,
        max_report_events: 17,
    })
    .unwrap()
}

fn order(order_id: u64, owner: u64, side: Side, price: i64, lots: u64) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        account(owner),
        instrument(),
        version(),
        side,
        AuctionOrderConstraint::Limit(Price::from_raw(price)),
        Quantity::new(lots).unwrap(),
        AuctionPriorityClass::new(u16::try_from(owner).unwrap()),
    )
}

fn phase() -> CallAuctionCommand {
    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
        command_id: CommandId::new(1).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(),
        expected_phase_revision: 0,
        target_phase: CallAuctionPhase::Collecting,
        received_at: TimestampNs::from_unix_nanos(1),
    })
}

fn submit(command_id: u64, order: CallAuctionOrder) -> CallAuctionCommand {
    CallAuctionCommand::Submit(CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(),
        expected_phase_revision: 1,
        order,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn price_band() -> AuctionPriceBand {
    AuctionPriceBand::new(
        AuctionPriceGrid::new(1).unwrap(),
        Price::from_raw(1),
        Price::from_raw(1_000),
    )
    .unwrap()
}

fn indicative() -> CallAuctionCommand {
    CallAuctionCommand::Indicative(CallAuctionIndicativeCommand {
        command_id: CommandId::new(4).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(),
        expected_phase_revision: 1,
        price_band: price_band(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        received_at: TimestampNs::from_unix_nanos(4),
    })
}

fn seed_commands() -> [CallAuctionCommand; 4] {
    [
        phase(),
        submit(2, order(10, 1, Side::Buy, 100, 8)),
        submit(3, order(20, 2, Side::Sell, 99, 6)),
        indicative(),
    ]
}

fn cancel(command_id: u64) -> CallAuctionCancelOrder {
    CallAuctionCancelOrder {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        account_id: account(1),
        order_id: OrderId::new(10).unwrap(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn seed_engine(engine: &mut CallAuctionEngine) {
    for command in seed_commands() {
        engine.submit(command).unwrap();
    }
}

fn profiles() -> [AccountRiskDefinition; 2] {
    [1_u64, 2].map(|owner| {
        AccountRiskDefinition::new(
            account(owner),
            RiskProfile::new(
                AccountRiskState::Active,
                0,
                RiskLimits::new(RiskLimitSpec {
                    max_order_quantity_lots: 100,
                    max_order_notional: 100_000,
                    max_open_orders: 8,
                    max_open_quantity_lots: 100,
                    max_open_notional: 100_000,
                    max_long_position_lots: 100,
                    max_short_position_lots: 100,
                })
                .unwrap(),
            )
            .unwrap(),
        )
    })
}

fn managed() -> CallAuctionRiskManagedEngine {
    let limits = CallAuctionRiskLimits::new(CallAuctionRiskLimitsSpec {
        auction: engine_limits(),
        max_registered_accounts: 2,
    })
    .unwrap();
    let mut managed = CallAuctionRiskManagedEngine::try_with_limits(definition(), limits).unwrap();
    for profile in profiles() {
        managed
            .register_account(profile.account_id(), profile.profile())
            .unwrap();
    }
    managed
}

fn seed_managed(managed: &mut CallAuctionRiskManagedEngine) {
    for command in seed_commands() {
        managed.submit(command).unwrap();
    }
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

fn assert_book_observation(observation: CallAuctionOrderObservation, present: bool) {
    assert_eq!(observation.instrument_id(), instrument());
    assert_eq!(observation.instrument_version(), version());
    assert_eq!(observation.order_id(), OrderId::new(10).unwrap());
    if present {
        let state = observation.state().unwrap();
        assert_eq!(state.order_id, OrderId::new(10).unwrap());
        assert_eq!(state.account_id, account(1));
        assert_eq!(state.side, Side::Buy);
        assert_eq!(
            state.constraint,
            AuctionOrderConstraint::Limit(Price::from_raw(100))
        );
        assert_eq!(state.quantity, Quantity::new(8).unwrap());
        assert_eq!(state.priority_class, AuctionPriorityClass::new(1));
        assert_eq!(state.priority_sequence, 1);
    } else {
        assert_eq!(observation.state(), None);
    }
}

fn assert_cancel_observation(observation: CallAuctionCancelObservation, sequence: u64) {
    assert_eq!(observation.command_id(), CommandId::new(sequence).unwrap());
    assert_eq!(observation.instrument_id(), instrument());
    assert_eq!(observation.instrument_version(), version());
    assert_eq!(observation.command_sequence(), sequence);
    assert_eq!(observation.first_event_sequence(), sequence);
    assert_eq!(observation.phase().phase(), CallAuctionPhase::Collecting);
    assert_eq!(observation.phase().revision(), 1);
    assert_eq!(observation.phase().active_auction_id(), Some(auction()));
    assert_eq!(observation.book_revision(), 2);
    assert_eq!(observation.resulting_book_revision(), 3);
    assert_eq!(observation.account_id(), account(1));
    assert_eq!(observation.order_id(), OrderId::new(10).unwrap());
    assert_eq!(
        observation.received_at(),
        TimestampNs::from_unix_nanos(sequence)
    );
    let order = observation.order();
    assert_eq!(order.order_id, OrderId::new(10).unwrap());
    assert_eq!(order.account_id, account(1));
    assert_eq!(order.quantity, Quantity::new(8).unwrap());
    let prior = observation.previous_indicative().unwrap();
    assert_eq!(prior.auction_id(), auction());
    assert_eq!(prior.phase_revision(), 1);
    assert_eq!(prior.book_revision(), 2);
    let clearing = prior.clearing().unwrap();
    assert_eq!(clearing.price(), Price::from_raw(100));
    assert_eq!(clearing.executable_quantity(), 6);
    assert_eq!(clearing.buy_quantity(), 8);
    assert_eq!(clearing.sell_quantity(), 6);
}

#[test]
fn call_auction_order_observation_is_state_bound_and_fail_closed() {
    let mut book = CallAuctionBook::try_with_limits(definition(), book_limits()).unwrap();
    let missing = book
        .try_order_observation(OrderId::new(10).unwrap())
        .unwrap();
    assert_eq!(missing.book_revision(), 0);
    assert_book_observation(missing, false);

    book.admit(order(10, 1, Side::Buy, 100, 8)).unwrap();
    let present = book
        .try_order_observation(OrderId::new(10).unwrap())
        .unwrap();
    assert_eq!(present.book_revision(), 1);
    assert_book_observation(present, true);

    book.cancel(account(1), OrderId::new(10).unwrap()).unwrap();
    let removed = book
        .try_order_observation(OrderId::new(10).unwrap())
        .unwrap();
    assert_eq!(removed.book_revision(), 2);
    assert_book_observation(removed, false);
    book.validate().unwrap();
}

#[test]
fn conditional_cancel_is_fixed_size_atomic_and_exactly_replayable() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CallAuctionCancelObservation>();
    assert!(!std::mem::needs_drop::<CallAuctionCancelObservation>());
    assert!(
        std::mem::size_of::<CallAuctionCancelObservation>() < 256,
        "observation size is {} bytes",
        std::mem::size_of::<CallAuctionCancelObservation>(),
    );

    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let command = cancel(5);
    let phase_before = engine.phase_snapshot();
    let indicative_before = engine.last_indicative();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = engine.try_submit_cancel_order_if(command, |observation| {
            assert_cancel_observation(*observation, 5);
            panic!("policy failure")
        });
    }));
    assert!(panicked.is_err());
    assert_eq!(engine.next_command_sequence(), 5);
    assert_eq!(engine.next_event_sequence(), 5);
    assert_eq!(engine.phase_snapshot(), phase_before);
    assert_eq!(engine.last_indicative(), indicative_before);
    assert!(engine.book().order(OrderId::new(10).unwrap()).is_some());

    let declined = engine
        .try_submit_cancel_order_if(command, |observation| {
            assert_cancel_observation(*observation, 5);
            false
        })
        .unwrap();
    let ConditionalCallAuctionCommandOutcome::Declined(observation) = declined else {
        panic!("decline must retain its owned observation");
    };
    assert_cancel_observation(observation, 5);
    assert_eq!(engine.next_command_sequence(), 5);
    assert_eq!(engine.book().state_revision(), 2);

    let accepted = engine
        .try_submit_cancel_order_if(command, |observation| {
            assert_cancel_observation(*observation, 5);
            true
        })
        .unwrap();
    let ConditionalCallAuctionCommandOutcome::Reported {
        observation: Some(observation),
        report,
    } = accepted
    else {
        panic!("acceptance must retain its exact observation and report");
    };
    assert_cancel_observation(observation, 5);
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(report.events.len(), 1);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::OrderCancelled { order, .. } if order == observation.order()
    ));
    assert_eq!(engine.book().state_revision(), 3);
    assert_eq!(engine.book().order(OrderId::new(10).unwrap()), None);
    assert!(engine.book().order(OrderId::new(20).unwrap()).is_some());
    assert_eq!(engine.last_indicative(), None);

    assert!(matches!(
        engine
            .try_submit_cancel_order_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.replayed
    ));
    engine.validate().unwrap();
}

#[test]
fn cancel_business_rejections_bypass_observation_and_predicate() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let called = Cell::new(0_u32);

    let mut wrong_version = cancel(5);
    wrong_version.instrument_version = InstrumentVersion::new(8).unwrap();
    let result = engine
        .try_submit_cancel_order_if(wrong_version, |_| {
            called.set(called.get() + 1);
            true
        })
        .unwrap();
    assert!(matches!(
        result,
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome == CallAuctionCommandOutcome::Rejected(
            CallAuctionRejectReason::WrongInstrumentVersion
        )
    ));

    let mut wrong_owner = cancel(6);
    wrong_owner.account_id = account(2);
    assert!(matches!(
        engine
            .try_submit_cancel_order_if(wrong_owner, |_| {
                called.set(called.get() + 1);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome == CallAuctionCommandOutcome::Rejected(
            CallAuctionRejectReason::NotOrderOwner
        )
    ));

    let mut unknown = cancel(7);
    unknown.order_id = OrderId::new(99).unwrap();
    assert!(matches!(
        engine
            .try_submit_cancel_order_if(unknown, |_| {
                called.set(called.get() + 1);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome == CallAuctionCommandOutcome::Rejected(
            CallAuctionRejectReason::UnknownOrder
        )
    ));
    assert_eq!(called.get(), 0);
    assert!(engine.book().order(OrderId::new(10).unwrap()).is_some());
    assert_eq!(engine.last_indicative().unwrap().book_revision(), 2);
    engine.validate().unwrap();
}

#[test]
fn coupled_risk_releases_only_the_accepted_observed_reservation() {
    let mut managed = managed();
    seed_managed(&mut managed);
    assert_eq!(managed.risk().reservation_count(), 2);

    let declined = managed
        .try_submit_cancel_order_if(cancel(5), |observation| {
            assert_cancel_observation(*observation, 5);
            false
        })
        .unwrap();
    assert!(matches!(
        declined,
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(managed.risk().reservation_count(), 2);

    let accepted = managed
        .try_submit_cancel_order_if(cancel(5), |observation| {
            assert_cancel_observation(*observation, 5);
            true
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(managed.risk().reservation_count(), 1);
    assert_eq!(managed.risk().reservation(OrderId::new(10).unwrap()), None);
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(20).unwrap())
            .is_some()
    );
    managed.validate().unwrap();
}

#[test]
fn durable_plain_cancel_noncommit_is_wal_free_and_acceptance_recovers() {
    let file = TestFile::new("plain");
    let mut durable = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let command = cancel(5);
    let frames_before = frame_count(file.path());
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.try_submit_cancel_order_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable
            .try_submit_cancel_order_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable
            .try_submit_cancel_order_if(command, |_| true)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_cancel_order_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.replayed
    ));
    assert_eq!(frame_count(file.path()), frames_after);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    assert_eq!(
        reopened
            .engine()
            .book()
            .try_order_observation(OrderId::new(10).unwrap())
            .unwrap()
            .state(),
        None
    );
    assert!(
        reopened
            .engine()
            .book()
            .order(OrderId::new(20).unwrap())
            .is_some()
    );
    reopened.engine().validate().unwrap();
}

#[test]
fn durable_risk_cancel_noncommit_is_neutral_and_acceptance_recovers() {
    let file = TestFile::new("risk");
    let profiles = profiles();
    let mut durable =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let command = cancel(5);
    let frames_before = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_cancel_order_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert_eq!(durable.managed().risk().reservation_count(), 2);
    assert!(matches!(
        durable
            .try_submit_cancel_order_if(command, |_| true)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    assert_eq!(durable.managed().risk().reservation_count(), 1);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    assert_eq!(reopened.managed().risk().reservation_count(), 1);
    assert_eq!(
        reopened
            .managed()
            .engine()
            .book()
            .try_order_observation(OrderId::new(10).unwrap())
            .unwrap()
            .state(),
        None
    );
    reopened.managed().validate().unwrap();
}
