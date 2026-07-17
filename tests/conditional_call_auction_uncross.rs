use std::cell::Cell;
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::auction::{
    AuctionAllocationPolicy, AuctionOrderConstraint, AuctionPriceBand, AuctionPriceGrid,
    AuctionPricePolicy,
};
use quotick::auction_book::{
    CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder, CallAuctionPrepareError,
    CallAuctionRemainderPolicy, CallAuctionSelfTradePolicy, CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionCommand, CallAuctionCommandOutcome, CallAuctionCommandPreparation,
    CallAuctionEngine, CallAuctionEngineError, CallAuctionEngineLimits,
    CallAuctionEngineLimitsSpec, CallAuctionEventKind, CallAuctionIndicativeCommand,
    CallAuctionPhase, CallAuctionPhaseControl, CallAuctionRejectReason, CallAuctionSubmitOrder,
    CallAuctionUncrossCommand, CallAuctionUncrossObservation, ConditionalCallAuctionOutcome,
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
            "quotick-conditional-call-auction-uncross-{label}-{}-{nonce}.wal",
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
    InstrumentId::new(91).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(4).unwrap()
}

fn auction() -> AuctionId {
    AuctionId::new(1).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("CONDITIONAL-AUCTION").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(0), Price::from_raw(200)).unwrap(),
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
        max_active_orders: 16,
        max_price_levels_per_side: 16,
        max_accepted_order_ids: 64,
        max_prepared_uncrosses: 2,
    })
    .unwrap()
}

fn engine_limits() -> CallAuctionEngineLimits {
    CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book: book_limits(),
        max_retained_commands: 96,
        terminal_command_reserve: 18,
        max_retained_events: 192,
        max_report_events: 33,
    })
    .unwrap()
}

fn phase(command_id: u64, revision: u64, target: CallAuctionPhase) -> CallAuctionCommand {
    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(),
        expected_phase_revision: revision,
        target_phase: target,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn order(
    order_id: u64,
    owner: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    lots: u64,
) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        account(owner),
        instrument(),
        version(),
        side,
        constraint,
        Quantity::new(lots).unwrap(),
        quotick::auction::AuctionPriorityClass::HIGHEST,
    )
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

fn indicative(command_id: u64) -> CallAuctionCommand {
    CallAuctionCommand::Indicative(CallAuctionIndicativeCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(),
        expected_phase_revision: 2,
        price_band: price_band(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn price_band() -> AuctionPriceBand {
    AuctionPriceBand::new(
        AuctionPriceGrid::new(1).unwrap(),
        Price::from_raw(0),
        Price::from_raw(200),
    )
    .unwrap()
}

fn uncross(command_id: u64) -> CallAuctionUncrossCommand {
    CallAuctionUncrossCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(),
        expected_phase_revision: 2,
        price_band: price_band(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        uncross_policy: CallAuctionUncrossPolicy::new(
            AuctionAllocationPolicy::PriceTime,
            CallAuctionRemainderPolicy::CancelAll,
            CallAuctionSelfTradePolicy::Permit,
        ),
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn seed_commands() -> [CallAuctionCommand; 7] {
    [
        phase(1, 0, CallAuctionPhase::Collecting),
        submit(
            2,
            order(
                30,
                1,
                Side::Buy,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                8,
            ),
        ),
        submit(
            3,
            order(
                10,
                2,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(90)),
                3,
            ),
        ),
        submit(
            4,
            order(
                20,
                3,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                2,
            ),
        ),
        phase(5, 1, CallAuctionPhase::Frozen),
        indicative(6),
        indicative(7),
    ]
}

fn seed_engine(engine: &mut CallAuctionEngine) {
    for command in seed_commands() {
        engine.submit(command).unwrap();
    }
}

fn broad_profile() -> RiskProfile {
    RiskProfile::new(
        AccountRiskState::Active,
        0,
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots: 1_000,
            max_order_notional: 200_000,
            max_open_orders: 16,
            max_open_quantity_lots: 16_000,
            max_open_notional: 3_200_000,
            max_long_position_lots: 16_000,
            max_short_position_lots: 16_000,
        })
        .unwrap(),
    )
    .unwrap()
}

fn profiles() -> [AccountRiskDefinition; 3] {
    [
        AccountRiskDefinition::new(account(1), broad_profile()),
        AccountRiskDefinition::new(account(2), broad_profile()),
        AccountRiskDefinition::new(account(3), broad_profile()),
    ]
}

fn managed() -> CallAuctionRiskManagedEngine {
    let limits = CallAuctionRiskLimits::new(CallAuctionRiskLimitsSpec {
        auction: engine_limits(),
        max_registered_accounts: 3,
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

fn assert_observation(observation: &CallAuctionUncrossObservation<'_>) {
    assert_eq!(observation.command_id(), CommandId::new(8).unwrap());
    assert_eq!(observation.instrument_id(), instrument());
    assert_eq!(observation.instrument_version(), version());
    assert_eq!(observation.command_sequence(), 8);
    assert_eq!(observation.first_event_sequence(), 8);
    assert_eq!(observation.auction_id(), auction());
    assert_eq!(observation.phase(), CallAuctionPhase::Frozen);
    assert_eq!(observation.phase_revision(), 2);
    assert_eq!(observation.resulting_phase(), CallAuctionPhase::Closed);
    assert_eq!(observation.resulting_phase_revision(), 3);
    assert_eq!(observation.book_revision(), 3);
    assert_eq!(observation.resulting_book_revision(), 4);
    assert_eq!(observation.price_band(), price_band());
    assert_eq!(observation.reference_price(), Price::from_raw(100));
    assert_eq!(observation.received_at(), TimestampNs::from_unix_nanos(8));
    assert_eq!(
        observation.price_policy(),
        AuctionPricePolicy::REFERENCE_THEN_LOWER
    );
    assert_eq!(
        observation.uncross_policy(),
        CallAuctionUncrossPolicy::new(
            AuctionAllocationPolicy::PriceTime,
            CallAuctionRemainderPolicy::CancelAll,
            CallAuctionSelfTradePolicy::Permit,
        )
    );
    let clearing = observation.allocation_plan().clearing();
    assert_eq!(clearing.price(), Price::from_raw(100));
    assert_eq!(clearing.executable_quantity(), 5);
    assert_eq!(observation.allocation_plan().buy_fills().len(), 1);
    assert_eq!(observation.allocation_plan().sell_fills().len(), 2);
    assert_eq!(observation.trades().len(), 2);
    assert_eq!(
        observation.trades()[0].buy_order_id(),
        OrderId::new(30).unwrap()
    );
    assert_eq!(
        observation.trades()[0].sell_order_id(),
        OrderId::new(10).unwrap()
    );
    assert_eq!(observation.trades()[0].quantity().lots(), 3);
    assert_eq!(
        observation.trades()[1].sell_order_id(),
        OrderId::new(20).unwrap()
    );
    assert_eq!(observation.trades()[1].quantity().lots(), 2);
    assert_eq!(observation.cancellations().len(), 1);
    assert_eq!(
        observation.cancellations()[0].order_id(),
        OrderId::new(30).unwrap()
    );
    assert_eq!(observation.cancellations()[0].quantity().lots(), 3);
    let prior = observation.previous_indicative().unwrap();
    assert_eq!(prior.auction_id(), auction());
    assert_eq!(prior.phase_revision(), 2);
    assert_eq!(prior.book_revision(), 3);
    assert_eq!(prior.clearing(), Some(clearing));
}

#[test]
fn uncross_observation_is_send_sync_and_fixed_size() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CallAuctionUncrossObservation<'static>>();
    assert!(!std::mem::needs_drop::<
        CallAuctionUncrossObservation<'static>,
    >());
    assert!(std::mem::size_of::<CallAuctionUncrossObservation<'static>>() < 512);
}

#[test]
fn decline_unwind_accept_and_replay_use_one_exact_zero_copy_uncross() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let command = uncross(8);
    let phase_before = engine.phase_snapshot();
    let book_revision_before = engine.book().state_revision();
    let indicative_before = engine.last_indicative();
    let pool_before = engine.book().resource_status();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = engine.try_submit_uncross_if(command, |observation| {
            assert_observation(observation);
            panic!("policy failure")
        });
    }));
    assert!(panicked.is_err());
    assert_eq!(engine.phase_snapshot(), phase_before);
    assert_eq!(engine.book().state_revision(), book_revision_before);
    assert_eq!(engine.last_indicative(), indicative_before);
    assert_eq!(
        engine.book().resource_status().available_prepared_uncrosses,
        pool_before.available_prepared_uncrosses
    );

    assert_eq!(
        engine
            .try_submit_uncross_if(command, |observation| {
                assert_observation(observation);
                false
            })
            .unwrap(),
        ConditionalCallAuctionOutcome::Declined
    );
    assert_eq!(engine.next_command_sequence(), 8);
    assert_eq!(engine.next_event_sequence(), 8);
    assert_eq!(engine.phase_snapshot(), phase_before);
    assert_eq!(engine.book().state_revision(), book_revision_before);
    assert_eq!(engine.last_indicative(), indicative_before);
    assert_eq!(engine.book().resource_status(), pool_before);

    let accepted = engine
        .try_submit_uncross_if(command, |observation| {
            assert_observation(observation);
            true
        })
        .unwrap();
    let ConditionalCallAuctionOutcome::Reported(report) = accepted else {
        panic!("accepted uncross returns its sequenced report");
    };
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(report.events.len(), 4);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::Trade(_)
    ));
    assert!(matches!(
        report.events[1].kind,
        CallAuctionEventKind::Trade(_)
    ));
    assert!(matches!(
        report.events[2].kind,
        CallAuctionEventKind::RemainderCancelled(_)
    ));
    assert!(matches!(
        report.events[3].kind,
        CallAuctionEventKind::UncrossCompleted { .. }
    ));
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Closed);
    assert_eq!(engine.phase_snapshot().revision(), 3);
    assert_eq!(engine.book().state_revision(), 4);
    assert_eq!(engine.book().active_order_count(), 0);
    assert_eq!(engine.last_indicative(), None);
    assert_eq!(
        engine.book().resource_status().available_prepared_uncrosses,
        pool_before.available_prepared_uncrosses
    );

    assert!(matches!(
        engine
            .try_submit_uncross_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report) if report.replayed
    ));
    engine.validate().unwrap();
}

#[test]
fn core_rejections_and_self_trade_abort_bypass_uncross_predicate() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let pool_before = engine.book().resource_status();
    let mut wrong_version = uncross(8);
    wrong_version.instrument_version = InstrumentVersion::new(5).unwrap();
    let called = Cell::new(false);
    let rejected = engine
        .try_submit_uncross_if(wrong_version, |_| {
            called.set(true);
            true
        })
        .unwrap();
    assert!(!called.get());
    assert!(matches!(
        rejected,
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Rejected(
                CallAuctionRejectReason::WrongInstrumentVersion
            )
    ));
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Frozen);
    assert_eq!(engine.book().resource_status(), pool_before);

    let mut self_trade = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    self_trade
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    self_trade
        .submit(submit(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    self_trade
        .submit(submit(
            3,
            order(2, 1, Side::Sell, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    self_trade
        .submit(phase(4, 1, CallAuctionPhase::Frozen))
        .unwrap();
    let mut command = uncross(5);
    command.uncross_policy = CallAuctionUncrossPolicy::new(
        AuctionAllocationPolicy::PriceTime,
        CallAuctionRemainderPolicy::CancelAll,
        CallAuctionSelfTradePolicy::Abort,
    );
    let pool_before = self_trade.book().resource_status();
    assert!(matches!(
        self_trade
            .try_submit_uncross_if(command, |_| panic!("self-trade rejection bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Rejected(
                CallAuctionRejectReason::SelfTradeWouldOccur
            )
    ));
    assert_eq!(self_trade.book().resource_status(), pool_before);
    self_trade.validate().unwrap();
}

#[test]
fn conditional_uncross_pool_exhaustion_is_typed_and_nonmutating() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let first = engine
        .prepare(CallAuctionCommand::Uncross(uncross(8)))
        .unwrap();
    let second = engine
        .prepare(CallAuctionCommand::Uncross(uncross(9)))
        .unwrap();
    assert!(matches!(first, CallAuctionCommandPreparation::Ready(_)));
    assert!(matches!(second, CallAuctionCommandPreparation::Ready(_)));
    assert_eq!(
        engine.book().resource_status().available_prepared_uncrosses,
        0
    );
    let called = Cell::new(false);
    let error = engine
        .try_submit_uncross_if(uncross(10), |_| {
            called.set(true);
            true
        })
        .unwrap_err();
    assert!(!called.get());
    assert!(matches!(
        error,
        CallAuctionEngineError::UncrossPreparation(
            CallAuctionPrepareError::PreparationCapacityExhausted { maximum: 2 }
        )
    ));
    assert_eq!(engine.next_command_sequence(), 8);
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Frozen);
    drop((first, second));
    assert_eq!(
        engine.book().resource_status().available_prepared_uncrosses,
        2
    );
}

#[test]
fn coupled_risk_decline_is_neutral_and_acceptance_applies_exact_uncross_trace() {
    let mut managed = managed();
    seed_managed(&mut managed);
    let command = uncross(8);
    assert_eq!(managed.risk().reservation_count(), 3);

    assert_eq!(
        managed
            .try_submit_uncross_if(command, |observation| {
                assert_observation(observation);
                false
            })
            .unwrap(),
        ConditionalCallAuctionOutcome::Declined
    );
    assert_eq!(managed.risk().reservation_count(), 3);
    for owner in 1..=3 {
        assert_eq!(
            managed
                .risk()
                .snapshot(account(owner))
                .unwrap()
                .position_lots(),
            0
        );
    }

    assert!(matches!(
        managed
            .try_submit_uncross_if(command, |observation| {
                assert_observation(observation);
                true
            })
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(managed.risk().reservation_count(), 0);
    assert_eq!(
        managed.risk().snapshot(account(1)).unwrap().position_lots(),
        5
    );
    assert_eq!(
        managed.risk().snapshot(account(2)).unwrap().position_lots(),
        -3
    );
    assert_eq!(
        managed.risk().snapshot(account(3)).unwrap().position_lots(),
        -2
    );
    managed.validate().unwrap();
}

#[test]
fn durable_plain_decline_is_wal_free_and_acceptance_recovers() {
    let file = TestFile::new("plain");
    let mut durable = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let mut wrong_version = uncross(8);
    wrong_version.instrument_version = InstrumentVersion::new(5).unwrap();
    let frames_before_rejection = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_uncross_if(wrong_version, |_| panic!("rejection bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Rejected(
                CallAuctionRejectReason::WrongInstrumentVersion
            )
    ));
    assert_eq!(frame_count(file.path()), frames_before_rejection + 2);

    let command = uncross(9);
    let frames_before = frame_count(file.path());
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.try_submit_uncross_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before);
    assert_eq!(
        durable.try_submit_uncross_if(command, |_| false).unwrap(),
        ConditionalCallAuctionOutcome::Declined
    );
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable.try_submit_uncross_if(command, |_| true).unwrap(),
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_uncross_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionOutcome::Reported(report) if report.replayed
    ));
    assert_eq!(frame_count(file.path()), frames_after);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    assert_eq!(
        reopened.engine().phase_snapshot().phase(),
        CallAuctionPhase::Closed
    );
    assert_eq!(reopened.engine().book().active_order_count(), 0);
    reopened.engine().validate().unwrap();
}

#[test]
fn durable_risk_decline_is_wal_free_and_acceptance_recovers_coupled_state() {
    let file = TestFile::new("risk");
    let profiles = profiles();
    let mut durable =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let command = uncross(8);
    let frames_before = frame_count(file.path());
    assert_eq!(
        durable.try_submit_uncross_if(command, |_| false).unwrap(),
        ConditionalCallAuctionOutcome::Declined
    );
    assert_eq!(frame_count(file.path()), frames_before);
    assert_eq!(durable.managed().risk().reservation_count(), 3);
    assert!(matches!(
        durable.try_submit_uncross_if(command, |_| true).unwrap(),
        ConditionalCallAuctionOutcome::Reported(report)
            if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    assert_eq!(durable.managed().risk().reservation_count(), 0);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    assert_eq!(
        reopened.managed().engine().phase_snapshot().phase(),
        CallAuctionPhase::Closed
    );
    assert_eq!(reopened.managed().risk().reservation_count(), 0);
    assert_eq!(
        reopened
            .managed()
            .risk()
            .snapshot(account(1))
            .unwrap()
            .position_lots(),
        5
    );
    assert_eq!(
        reopened
            .managed()
            .risk()
            .snapshot(account(2))
            .unwrap()
            .position_lots(),
        -3
    );
    assert_eq!(
        reopened
            .managed()
            .risk()
            .snapshot(account(3))
            .unwrap()
            .position_lots(),
        -2
    );
    reopened.managed().validate().unwrap();
}
