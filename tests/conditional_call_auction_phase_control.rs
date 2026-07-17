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
    CallAuctionPhase, CallAuctionPhaseControl, CallAuctionPhaseControlObservation,
    CallAuctionRejectReason, CallAuctionSubmitOrder, ConditionalCallAuctionCommandOutcome,
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
            "quotick-conditional-call-auction-phase-{label}-{}-{nonce}.wal",
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
    InstrumentId::new(98).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(9).unwrap()
}

fn auction(value: u64) -> AuctionId {
    AuctionId::new(value).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("CONDITIONAL-PHASE").unwrap(),
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
        max_accepted_order_ids: 16,
        max_prepared_uncrosses: 2,
    })
    .unwrap()
}

fn engine_limits() -> CallAuctionEngineLimits {
    CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book: book_limits(),
        max_retained_commands: 32,
        terminal_command_reserve: 10,
        max_retained_events: 64,
        max_report_events: 17,
    })
    .unwrap()
}

fn control(
    command_id: u64,
    auction_id: u64,
    expected_phase_revision: u64,
    target_phase: CallAuctionPhase,
) -> CallAuctionPhaseControl {
    CallAuctionPhaseControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(auction_id),
        expected_phase_revision,
        target_phase,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
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
        AuctionPriorityClass::new(u16::try_from(owner).unwrap()),
    )
}

fn submit(command_id: u64, order: CallAuctionOrder) -> CallAuctionSubmitOrder {
    CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(1),
        expected_phase_revision: 1,
        order,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn price_band() -> AuctionPriceBand {
    AuctionPriceBand::new(
        AuctionPriceGrid::new(1).unwrap(),
        Price::from_raw(90),
        Price::from_raw(110),
    )
    .unwrap()
}

fn indicative(command_id: u64) -> CallAuctionIndicativeCommand {
    CallAuctionIndicativeCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(1),
        expected_phase_revision: 1,
        price_band: price_band(),
        reference_price: Price::from_raw(101),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn seed_commands() -> [CallAuctionCommand; 5] {
    [
        CallAuctionCommand::PhaseControl(control(1, 1, 0, CallAuctionPhase::Collecting)),
        CallAuctionCommand::Submit(submit(
            2,
            order(20, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
        )),
        CallAuctionCommand::Submit(submit(
            3,
            order(
                30,
                2,
                Side::Buy,
                AuctionOrderConstraint::Limit(Price::from_raw(102)),
                5,
            ),
        )),
        CallAuctionCommand::Submit(submit(
            4,
            order(
                40,
                3,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(101)),
                4,
            ),
        )),
        CallAuctionCommand::Indicative(indicative(5)),
    ]
}

fn seed_engine(engine: &mut CallAuctionEngine) {
    for command in seed_commands() {
        engine.submit(command).unwrap();
    }
}

fn profiles() -> [AccountRiskDefinition; 3] {
    [1_u64, 2, 3].map(|owner| {
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
        max_registered_accounts: 3,
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

fn assert_freeze_observation(observation: &CallAuctionPhaseControlObservation, sequence: u64) {
    assert_eq!(observation.command_id(), CommandId::new(sequence).unwrap());
    assert_eq!(observation.instrument_id(), instrument());
    assert_eq!(observation.instrument_version(), version());
    assert_eq!(observation.auction_id(), auction(1));
    assert_eq!(observation.command_sequence(), sequence);
    assert_eq!(observation.first_event_sequence(), sequence);
    assert_eq!(
        observation.received_at(),
        TimestampNs::from_unix_nanos(sequence)
    );
    assert_eq!(observation.phase().phase(), CallAuctionPhase::Collecting);
    assert_eq!(observation.phase().revision(), 1);
    assert_eq!(observation.phase().active_auction_id(), Some(auction(1)));
    assert_eq!(observation.phase().last_auction_id(), Some(auction(1)));
    assert_eq!(
        observation.resulting_phase().phase(),
        CallAuctionPhase::Frozen
    );
    assert_eq!(observation.resulting_phase().revision(), 2);
    assert_eq!(
        observation.resulting_phase().active_auction_id(),
        Some(auction(1))
    );
    assert_eq!(
        observation.resulting_phase().last_auction_id(),
        Some(auction(1))
    );
    assert_eq!(observation.book_revision(), 3);
    assert_eq!(observation.resulting_book_revision(), 3);
    assert_eq!(observation.active_order_count(), 3);
    assert_eq!(observation.accepted_order_count(), 3);
    assert_eq!(observation.bid_price_level_count(), 1);
    assert_eq!(observation.ask_price_level_count(), 1);
    assert_eq!(observation.buy_market_order_count(), 1);
    assert_eq!(observation.sell_market_order_count(), 0);
    assert_eq!(observation.buy_market_quantity_lots(), 2);
    assert_eq!(observation.sell_market_quantity_lots(), 0);
    let previous = observation.previous_indicative().unwrap();
    assert_eq!(previous.auction_id(), auction(1));
    assert_eq!(previous.phase_revision(), 1);
    assert_eq!(previous.book_revision(), 3);
    assert_eq!(previous.reference_price(), Price::from_raw(101));
    assert!(previous.clearing().is_some());
    assert_eq!(observation.resulting_indicative(), None);
}

#[test]
fn conditional_phase_control_is_fixed_size_atomic_and_exactly_replayable() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CallAuctionPhaseControlObservation>();
    assert!(!std::mem::needs_drop::<CallAuctionPhaseControlObservation>());
    assert!(std::mem::size_of::<CallAuctionPhaseControlObservation>() < 384);

    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let command = control(6, 1, 1, CallAuctionPhase::Frozen);
    let prior_indicative = engine.last_indicative();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = engine.try_submit_phase_control_if(command, |observation| {
            assert_freeze_observation(observation, 6);
            panic!("policy failure")
        });
    }));
    assert!(panicked.is_err());
    assert_eq!(
        engine.phase_snapshot().phase(),
        CallAuctionPhase::Collecting
    );
    assert_eq!(engine.last_indicative(), prior_indicative);

    let declined = engine
        .try_submit_phase_control_if(command, |observation| {
            assert_freeze_observation(observation, 6);
            false
        })
        .unwrap();
    let ConditionalCallAuctionCommandOutcome::Declined(observation) = declined else {
        panic!("decline must retain the exact phase-control observation");
    };
    assert_freeze_observation(&observation, 6);
    assert_eq!(
        engine.phase_snapshot().phase(),
        CallAuctionPhase::Collecting
    );
    assert_eq!(engine.last_indicative(), prior_indicative);

    let accepted = engine
        .try_submit_phase_control_if(command, |candidate| candidate == &observation)
        .unwrap();
    let ConditionalCallAuctionCommandOutcome::Reported {
        observation: Some(committed),
        report,
    } = accepted
    else {
        panic!("accepted phase control must retain its observation");
    };
    assert_eq!(committed, observation);
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(report.events.len(), 1);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::PhaseChanged {
            auction_id,
            previous: CallAuctionPhase::Collecting,
            current: CallAuctionPhase::Frozen,
            revision: 2,
        } if auction_id == auction(1)
    ));
    assert_eq!(engine.phase_snapshot(), observation.resulting_phase());
    assert_eq!(engine.book().state_revision(), 3);
    assert_eq!(engine.last_indicative(), None);

    assert!(matches!(
        engine
            .try_submit_phase_control_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.replayed
    ));
    engine.validate().unwrap();
}

#[test]
fn conditional_phase_control_covers_cycle_lineage_and_nullable_indicative_state() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();

    let started = engine
        .try_submit_phase_control_if(control(1, 1, 0, CallAuctionPhase::Collecting), |value| {
            assert_eq!(value.phase().phase(), CallAuctionPhase::Closed);
            assert_eq!(value.phase().active_auction_id(), None);
            assert_eq!(value.phase().last_auction_id(), None);
            assert_eq!(
                value.resulting_phase().phase(),
                CallAuctionPhase::Collecting
            );
            assert_eq!(
                value.resulting_phase().active_auction_id(),
                Some(auction(1))
            );
            assert_eq!(value.resulting_phase().last_auction_id(), Some(auction(1)));
            assert_eq!(value.previous_indicative(), None);
            assert_eq!(value.resulting_indicative(), None);
            true
        })
        .unwrap();
    assert!(matches!(
        started,
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));

    for (command_id, expected_revision, target, resulting_revision) in [
        (2, 1, CallAuctionPhase::Frozen, 2),
        (3, 2, CallAuctionPhase::Collecting, 3),
        (4, 3, CallAuctionPhase::Frozen, 4),
        (5, 4, CallAuctionPhase::Closed, 5),
    ] {
        let outcome = engine
            .try_submit_phase_control_if(
                control(command_id, 1, expected_revision, target),
                |value| {
                    assert_eq!(value.resulting_phase().phase(), target);
                    assert_eq!(value.resulting_phase().revision(), resulting_revision);
                    true
                },
            )
            .unwrap();
        assert!(matches!(
            outcome,
            ConditionalCallAuctionCommandOutcome::Reported {
                observation: Some(_),
                report,
            } if report.outcome == CallAuctionCommandOutcome::Accepted
        ));
    }
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Closed);
    assert_eq!(engine.phase_snapshot().active_auction_id(), None);
    assert_eq!(engine.phase_snapshot().last_auction_id(), Some(auction(1)));

    engine
        .try_submit_phase_control_if(control(6, 2, 5, CallAuctionPhase::Collecting), |value| {
            assert_eq!(value.phase().last_auction_id(), Some(auction(1)));
            assert_eq!(
                value.resulting_phase().active_auction_id(),
                Some(auction(2))
            );
            assert_eq!(value.resulting_phase().last_auction_id(), Some(auction(2)));
            true
        })
        .unwrap();
    engine
        .try_submit_phase_control_if(control(7, 2, 6, CallAuctionPhase::Closed), |_| true)
        .unwrap();
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Closed);
    assert_eq!(engine.phase_snapshot().last_auction_id(), Some(auction(2)));
    engine.validate().unwrap();
}

#[test]
fn phase_control_business_rejections_bypass_observation_and_predicate() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let called = Cell::new(0);

    let mut wrong_version = control(6, 1, 1, CallAuctionPhase::Frozen);
    wrong_version.instrument_version = InstrumentVersion::new(10).unwrap();
    assert!(matches!(
        engine
            .try_submit_phase_control_if(wrong_version, |_| {
                called.set(called.get() + 1);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome == CallAuctionCommandOutcome::Rejected(
            CallAuctionRejectReason::WrongInstrumentVersion
        )
    ));

    assert!(matches!(
        engine
            .try_submit_phase_control_if(control(7, 1, 0, CallAuctionPhase::Frozen), |_| {
                called.set(called.get() + 1);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome == CallAuctionCommandOutcome::Rejected(
            CallAuctionRejectReason::PhaseRevisionMismatch {
                observed: 0,
                current: 1,
            }
        )
    ));

    assert!(matches!(
        engine
            .try_submit_phase_control_if(
                control(8, 1, 1, CallAuctionPhase::Collecting),
                |_| {
                    called.set(called.get() + 1);
                    true
                },
            )
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome == CallAuctionCommandOutcome::Rejected(
            CallAuctionRejectReason::InvalidPhaseTransition {
                from: CallAuctionPhase::Collecting,
                to: CallAuctionPhase::Collecting,
            }
        )
    ));
    assert_eq!(called.get(), 0);
    assert_eq!(
        engine.phase_snapshot().phase(),
        CallAuctionPhase::Collecting
    );
    assert!(engine.last_indicative().is_some());
    engine.validate().unwrap();
}

#[test]
fn coupled_phase_control_is_risk_neutral_on_decline_acceptance_and_replay() {
    let mut managed = managed();
    let command = control(6, 1, 1, CallAuctionPhase::Frozen);
    let before_accounts = [1_u64, 2, 3].map(|owner| managed.risk().snapshot(account(owner)));
    let before_reservations = [20_u64, 30, 40]
        .map(|order_id| managed.risk().reservation(OrderId::new(order_id).unwrap()));

    assert!(matches!(
        managed
            .try_submit_phase_control_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(managed.risk().reservation_count(), 3);
    assert_eq!(
        [1_u64, 2, 3].map(|owner| managed.risk().snapshot(account(owner))),
        before_accounts
    );

    assert!(matches!(
        managed
            .try_submit_phase_control_if(command, |observation| {
                assert_freeze_observation(observation, 6);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(managed.risk().reservation_count(), 3);
    assert_eq!(
        [20_u64, 30, 40]
            .map(|order_id| managed.risk().reservation(OrderId::new(order_id).unwrap())),
        before_reservations
    );
    assert!(matches!(
        managed
            .try_submit_phase_control_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.replayed
    ));
    managed.validate().unwrap();
}

#[test]
fn durable_plain_noncommit_is_wal_free_and_phase_transition_recovers() {
    let file = TestFile::new("plain");
    let mut durable = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let frames_before = frame_count(file.path());
    let mut wrong_version = control(6, 1, 1, CallAuctionPhase::Frozen);
    wrong_version.instrument_version = InstrumentVersion::new(10).unwrap();
    assert!(matches!(
        durable
            .try_submit_phase_control_if(wrong_version, |_| {
                panic!("core rejection bypasses predicate")
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome == CallAuctionCommandOutcome::Rejected(
            CallAuctionRejectReason::WrongInstrumentVersion
        )
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);

    let command = control(7, 1, 1, CallAuctionPhase::Frozen);
    let frames_before_noncommit = frame_count(file.path());
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.try_submit_phase_control_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before_noncommit);
    assert!(matches!(
        durable
            .try_submit_phase_control_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before_noncommit);
    assert!(matches!(
        durable
            .try_submit_phase_control_if(command, |_| true)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before_noncommit + 2);
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_phase_control_if(command, |_| panic!("replay bypasses predicate"))
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
        reopened.engine().phase_snapshot().phase(),
        CallAuctionPhase::Frozen
    );
    assert_eq!(reopened.engine().phase_snapshot().revision(), 2);
    assert_eq!(reopened.engine().book().state_revision(), 3);
    assert_eq!(reopened.engine().book().active_order_count(), 3);
    assert_eq!(reopened.engine().last_indicative(), None);
    reopened.engine().validate().unwrap();
}

#[test]
fn durable_risk_noncommit_and_phase_recovery_preserve_risk_exactly() {
    let file = TestFile::new("risk");
    let profiles = profiles();
    let mut durable =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let command = control(6, 1, 1, CallAuctionPhase::Frozen);
    let frames_before = frame_count(file.path());
    let before_accounts =
        [1_u64, 2, 3].map(|owner| durable.managed().risk().snapshot(account(owner)));
    let before_reservations = [20_u64, 30, 40].map(|order_id| {
        durable
            .managed()
            .risk()
            .reservation(OrderId::new(order_id).unwrap())
    });

    assert!(matches!(
        durable
            .try_submit_phase_control_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable
            .try_submit_phase_control_if(command, |observation| {
                assert_freeze_observation(observation, 6);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    assert_eq!(
        [1_u64, 2, 3].map(|owner| durable.managed().risk().snapshot(account(owner))),
        before_accounts
    );
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    assert_eq!(
        reopened.managed().engine().phase_snapshot().phase(),
        CallAuctionPhase::Frozen
    );
    assert_eq!(reopened.managed().engine().last_indicative(), None);
    assert_eq!(reopened.managed().risk().reservation_count(), 3);
    assert_eq!(
        [20_u64, 30, 40].map(|order_id| reopened
            .managed()
            .risk()
            .reservation(OrderId::new(order_id).unwrap())),
        before_reservations
    );
    reopened.managed().validate().unwrap();
}
