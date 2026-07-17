use std::cell::Cell;
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::auction::{
    AuctionOrderConstraint, AuctionPriceBand, AuctionPriceGrid, AuctionPricePolicy,
    AuctionPriorityClass,
};
use quotick::auction_book::{CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder};
use quotick::auction_engine::{
    CallAuctionCommand, CallAuctionCommandOutcome, CallAuctionEngine, CallAuctionEngineLimits,
    CallAuctionEngineLimitsSpec, CallAuctionEventKind, CallAuctionIndicativeCommand,
    CallAuctionMassCancel, CallAuctionMassCancelObservation, CallAuctionPhase,
    CallAuctionPhaseControl, CallAuctionRejectReason, CallAuctionSubmitOrder,
    ConditionalCallAuctionOutcome,
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
use quotick::matching::MassCancelScope;
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
            "quotick-conditional-call-auction-mass-cancel-{label}-{}-{nonce}.wal",
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
    InstrumentId::new(95).unwrap()
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
        symbol: InstrumentSymbol::new("CONDITIONAL-MASS-CANCEL").unwrap(),
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

fn price_band() -> AuctionPriceBand {
    AuctionPriceBand::new(
        AuctionPriceGrid::new(1).unwrap(),
        Price::from_raw(1),
        Price::from_raw(1_000),
    )
    .unwrap()
}

fn seed_commands() -> [CallAuctionCommand; 5] {
    [
        CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
            command_id: CommandId::new(1).unwrap(),
            instrument_id: instrument(),
            instrument_version: version(),
            auction_id: auction(),
            expected_phase_revision: 0,
            target_phase: CallAuctionPhase::Collecting,
            received_at: TimestampNs::from_unix_nanos(1),
        }),
        submit(2, order(30, 1, Side::Sell, 101, 4)),
        submit(3, order(20, 2, Side::Buy, 100, 5)),
        submit(4, order(10, 1, Side::Buy, 102, 3)),
        CallAuctionCommand::Indicative(CallAuctionIndicativeCommand {
            command_id: CommandId::new(5).unwrap(),
            instrument_id: instrument(),
            instrument_version: version(),
            auction_id: auction(),
            expected_phase_revision: 1,
            price_band: price_band(),
            reference_price: Price::from_raw(100),
            price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
            received_at: TimestampNs::from_unix_nanos(5),
        }),
    ]
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

fn mass_cancel(command_id: u64, owner: u64, scope: MassCancelScope) -> CallAuctionMassCancel {
    CallAuctionMassCancel {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        account_id: account(owner),
        scope,
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
    for command in seed_commands() {
        managed.submit(command).unwrap();
    }
    managed
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

fn assert_observation(observation: &CallAuctionMassCancelObservation<'_>) {
    assert_eq!(observation.command_id(), CommandId::new(6).unwrap());
    assert_eq!(observation.instrument_id(), instrument());
    assert_eq!(observation.instrument_version(), version());
    assert_eq!(observation.command_sequence(), 6);
    assert_eq!(observation.first_event_sequence(), 6);
    assert_eq!(observation.completion_event_sequence(), 8);
    assert_eq!(observation.phase().phase(), CallAuctionPhase::Collecting);
    assert_eq!(observation.phase().revision(), 1);
    assert_eq!(observation.phase().active_auction_id(), Some(auction()));
    assert_eq!(observation.book_revision(), 3);
    assert_eq!(observation.resulting_book_revision(), 4);
    assert_eq!(observation.account_id(), account(1));
    assert_eq!(observation.scope(), MassCancelScope::All);
    assert_eq!(observation.cancelled_order_count(), 2);
    assert_eq!(observation.cancelled_quantity_lots(), 7);
    assert_eq!(observation.received_at(), TimestampNs::from_unix_nanos(6));
    assert_eq!(
        observation
            .orders()
            .iter()
            .map(|order| order.order_id)
            .collect::<Vec<_>>(),
        [OrderId::new(10).unwrap(), OrderId::new(30).unwrap()]
    );
    assert_eq!(
        observation
            .orders()
            .iter()
            .map(|order| order.quantity.lots())
            .sum::<u64>(),
        7
    );
    let previous = observation.previous_indicative().unwrap();
    assert_eq!(previous.auction_id(), auction());
    assert_eq!(previous.book_revision(), 3);
}

#[test]
fn conditional_mass_cancel_is_zero_copy_atomic_and_exactly_replayable() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CallAuctionMassCancelObservation<'static>>();
    assert!(!std::mem::needs_drop::<
        CallAuctionMassCancelObservation<'static>,
    >());
    assert!(std::mem::size_of::<CallAuctionMassCancelObservation<'static>>() < 320);

    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let command = mass_cancel(6, 1, MassCancelScope::All);
    let phase_before = engine.phase_snapshot();
    let indication_before = engine.last_indicative();
    let scratch_capacity = engine.resource_status().mass_cancel_scratch_capacity;

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = engine.try_submit_mass_cancel_if(command, |observation| {
            assert_observation(observation);
            panic!("policy failure")
        });
    }));
    assert!(panicked.is_err());
    assert_eq!(engine.resource_status().mass_cancel_scratch_len, 0);
    assert_eq!(
        engine.resource_status().mass_cancel_scratch_capacity,
        scratch_capacity
    );
    assert_eq!(engine.phase_snapshot(), phase_before);
    assert_eq!(engine.last_indicative(), indication_before);
    assert_eq!(engine.book().state_revision(), 3);

    assert_eq!(
        engine
            .try_submit_mass_cancel_if(command, |observation| {
                assert_observation(observation);
                false
            })
            .unwrap(),
        ConditionalCallAuctionOutcome::Declined
    );
    assert_eq!(engine.resource_status().mass_cancel_scratch_len, 0);
    assert_eq!(
        engine.resource_status().mass_cancel_scratch_capacity,
        scratch_capacity
    );
    assert_eq!(engine.book().active_order_count(), 3);

    let accepted = engine
        .try_submit_mass_cancel_if(command, |observation| {
            assert_observation(observation);
            true
        })
        .unwrap();
    let ConditionalCallAuctionOutcome::Reported(report) = accepted else {
        panic!("acceptance must report");
    };
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(report.events.len(), 3);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::OrderCancelled { order, .. }
            if order.order_id == OrderId::new(10).unwrap()
    ));
    assert!(matches!(
        report.events[1].kind,
        CallAuctionEventKind::OrderCancelled { order, .. }
            if order.order_id == OrderId::new(30).unwrap()
    ));
    assert!(matches!(
        report.events[2].kind,
        CallAuctionEventKind::MassCancelCompleted {
            cancelled_order_count: 2,
            cancelled_quantity_lots: 7,
            book_revision: 4,
            ..
        }
    ));
    assert!(engine.book().order(OrderId::new(10).unwrap()).is_none());
    assert!(engine.book().order(OrderId::new(30).unwrap()).is_none());
    assert!(engine.book().order(OrderId::new(20).unwrap()).is_some());
    assert_eq!(engine.last_indicative(), None);
    assert_eq!(engine.resource_status().mass_cancel_scratch_len, 0);
    assert_eq!(
        engine.resource_status().mass_cancel_scratch_capacity,
        scratch_capacity
    );

    assert!(matches!(
        engine
            .try_submit_mass_cancel_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report) if report.replayed
    ));
    engine.validate().unwrap();
}

#[test]
fn empty_selection_is_observed_and_rejection_bypasses_the_predicate() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let called = Cell::new(0_u32);
    assert_eq!(
        engine
            .try_submit_mass_cancel_if(
                mass_cancel(6, 1, MassCancelScope::Side(Side::Sell)),
                |observation| {
                    assert_eq!(observation.scope(), MassCancelScope::Side(Side::Sell));
                    assert_eq!(observation.cancelled_order_count(), 1);
                    assert_eq!(observation.cancelled_quantity_lots(), 4);
                    assert_eq!(observation.completion_event_sequence(), 7);
                    assert_eq!(observation.orders().len(), 1);
                    assert_eq!(observation.orders()[0].order_id, OrderId::new(30).unwrap());
                    false
                },
            )
            .unwrap(),
        ConditionalCallAuctionOutcome::Declined
    );
    let empty = mass_cancel(6, 99, MassCancelScope::Side(Side::Sell));
    let outcome = engine
        .try_submit_mass_cancel_if(empty, |observation| {
            called.set(called.get() + 1);
            assert_eq!(observation.orders(), []);
            assert_eq!(observation.cancelled_order_count(), 0);
            assert_eq!(observation.cancelled_quantity_lots(), 0);
            assert_eq!(observation.book_revision(), 3);
            assert_eq!(observation.resulting_book_revision(), 3);
            assert_eq!(observation.first_event_sequence(), 6);
            assert_eq!(observation.completion_event_sequence(), 6);
            true
        })
        .unwrap();
    let ConditionalCallAuctionOutcome::Reported(report) = outcome else {
        panic!("accepted empty selection must report");
    };
    assert_eq!(report.events.len(), 1);
    assert_eq!(engine.book().state_revision(), 3);
    assert_eq!(engine.book().active_order_count(), 3);
    assert_eq!(engine.last_indicative(), None);

    let mut wrong_version = mass_cancel(7, 1, MassCancelScope::All);
    wrong_version.instrument_version = InstrumentVersion::new(8).unwrap();
    assert!(matches!(
        engine
            .try_submit_mass_cancel_if(wrong_version, |_| {
                called.set(called.get() + 1);
                true
            })
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Rejected(
                CallAuctionRejectReason::WrongInstrumentVersion
            )
    ));
    assert_eq!(called.get(), 1);
    engine.validate().unwrap();
}

#[test]
fn coupled_risk_releases_exactly_the_selected_reservations() {
    let mut managed = managed();
    let command = mass_cancel(6, 1, MassCancelScope::All);
    assert_eq!(managed.risk().reservation_count(), 3);
    assert_eq!(
        managed
            .try_submit_mass_cancel_if(command, |observation| {
                assert_observation(observation);
                false
            })
            .unwrap(),
        ConditionalCallAuctionOutcome::Declined
    );
    assert_eq!(managed.risk().reservation_count(), 3);
    assert!(matches!(
        managed
            .try_submit_mass_cancel_if(command, |observation| {
                assert_observation(observation);
                true
            })
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(managed.risk().reservation_count(), 1);
    assert_eq!(managed.risk().reservation(OrderId::new(10).unwrap()), None);
    assert_eq!(managed.risk().reservation(OrderId::new(30).unwrap()), None);
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(20).unwrap())
            .is_some()
    );
    managed.validate().unwrap();
}

#[test]
fn durable_plain_noncommit_is_wal_free_and_acceptance_recovers() {
    let file = TestFile::new("plain");
    let mut durable = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let command = mass_cancel(6, 1, MassCancelScope::All);
    let frames_before = frame_count(file.path());
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.try_submit_mass_cancel_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before);
    assert_eq!(
        durable.engine().resource_status().mass_cancel_scratch_len,
        0
    );
    assert_eq!(
        durable
            .try_submit_mass_cancel_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionOutcome::Declined
    );
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable
            .try_submit_mass_cancel_if(command, |_| true)
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_mass_cancel_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report) if report.replayed
    ));
    assert_eq!(frame_count(file.path()), frames_after);
    let mut wrong_version = mass_cancel(7, 1, MassCancelScope::All);
    wrong_version.instrument_version = InstrumentVersion::new(8).unwrap();
    assert!(matches!(
        durable
            .try_submit_mass_cancel_if(wrong_version, |_| {
                panic!("business rejection bypasses predicate")
            })
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Rejected(
                CallAuctionRejectReason::WrongInstrumentVersion
            )
    ));
    assert_eq!(frame_count(file.path()), frames_after + 2);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    assert_eq!(reopened.engine().book().active_order_count(), 1);
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
fn durable_risk_noncommit_is_neutral_and_acceptance_recovers() {
    let file = TestFile::new("risk");
    let profiles = profiles();
    let mut durable =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let command = mass_cancel(6, 1, MassCancelScope::All);
    let frames_before = frame_count(file.path());
    assert_eq!(
        durable
            .try_submit_mass_cancel_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionOutcome::Declined
    );
    assert_eq!(frame_count(file.path()), frames_before);
    assert_eq!(durable.managed().risk().reservation_count(), 3);
    assert!(matches!(
        durable
            .try_submit_mass_cancel_if(command, |_| true)
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    assert_eq!(durable.managed().risk().reservation_count(), 1);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    assert_eq!(reopened.managed().risk().reservation_count(), 1);
    assert_eq!(reopened.managed().engine().book().active_order_count(), 1);
    reopened.managed().validate().unwrap();
}
