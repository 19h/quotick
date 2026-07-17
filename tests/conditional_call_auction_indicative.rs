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
    CallAuctionIndicativeObservation, CallAuctionPhase, CallAuctionPhaseControl,
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
            "quotick-conditional-call-auction-indicative-{label}-{}-{nonce}.wal",
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
    InstrumentId::new(97).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(8).unwrap()
}

fn auction() -> AuctionId {
    AuctionId::new(1).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("CONDITIONAL-INDICATIVE").unwrap(),
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
        Price::from_raw(90),
        Price::from_raw(110),
    )
    .unwrap()
}

fn submit(command_id: u64, order: CallAuctionOrder) -> CallAuctionSubmitOrder {
    CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(),
        expected_phase_revision: 1,
        order,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn indicative(command_id: u64, reference_price: i64) -> CallAuctionIndicativeCommand {
    CallAuctionIndicativeCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(),
        expected_phase_revision: 1,
        price_band: price_band(),
        reference_price: Price::from_raw(reference_price),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn seed_commands() -> [CallAuctionCommand; 4] {
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
        CallAuctionCommand::Submit(submit(2, order(20, 1, Side::Buy, 102, 5))),
        CallAuctionCommand::Submit(submit(3, order(30, 2, Side::Sell, 101, 4))),
        CallAuctionCommand::Indicative(indicative(4, 101)),
    ]
}

fn candidate(command_id: u64) -> CallAuctionIndicativeCommand {
    indicative(command_id, 102)
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

fn assert_observation(observation: &CallAuctionIndicativeObservation, sequence: u64) {
    assert_eq!(observation.command_id(), CommandId::new(sequence).unwrap());
    assert_eq!(observation.instrument_id(), instrument());
    assert_eq!(observation.instrument_version(), version());
    assert_eq!(observation.command_sequence(), sequence);
    assert_eq!(observation.first_event_sequence(), sequence);
    assert_eq!(observation.phase().phase(), CallAuctionPhase::Collecting);
    assert_eq!(observation.phase().revision(), 1);
    assert_eq!(observation.phase().active_auction_id(), Some(auction()));
    assert_eq!(observation.phase().last_auction_id(), Some(auction()));
    assert_eq!(observation.book_revision(), 2);
    assert_eq!(observation.resulting_book_revision(), 2);
    assert_eq!(
        observation.received_at(),
        TimestampNs::from_unix_nanos(sequence)
    );

    let previous = observation.previous_indicative().unwrap();
    assert_eq!(previous.auction_id(), auction());
    assert_eq!(previous.phase_revision(), 1);
    assert_eq!(previous.book_revision(), 2);
    assert_eq!(previous.reference_price(), Price::from_raw(101));
    assert_eq!(previous.clearing().unwrap().price(), Price::from_raw(101));

    let published = observation.published_indicative();
    assert_eq!(published.auction_id(), auction());
    assert_eq!(published.phase_revision(), 1);
    assert_eq!(published.book_revision(), 2);
    assert_eq!(published.price_band(), price_band());
    assert_eq!(published.reference_price(), Price::from_raw(102));
    assert_eq!(
        published.price_policy(),
        AuctionPricePolicy::REFERENCE_THEN_LOWER
    );
    let clearing = published.clearing().unwrap();
    assert_eq!(clearing.price(), Price::from_raw(102));
    assert_eq!(clearing.buy_quantity(), 5);
    assert_eq!(clearing.sell_quantity(), 4);
    assert_eq!(clearing.executable_quantity(), 4);
}

#[test]
fn conditional_indicative_is_fixed_size_atomic_and_exactly_replayable() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CallAuctionIndicativeObservation>();
    assert!(!std::mem::needs_drop::<CallAuctionIndicativeObservation>());
    assert!(std::mem::size_of::<CallAuctionIndicativeObservation>() < 384);

    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let command = candidate(5);
    let previous = engine.last_indicative();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = engine.try_submit_indicative_if(command, |observation| {
            assert_observation(observation, 5);
            panic!("policy failure")
        });
    }));
    assert!(panicked.is_err());
    assert_eq!(engine.next_command_sequence(), 5);
    assert_eq!(engine.next_event_sequence(), 5);
    assert_eq!(engine.book().state_revision(), 2);
    assert_eq!(engine.last_indicative(), previous);

    let declined = engine
        .try_submit_indicative_if(command, |observation| {
            assert_observation(observation, 5);
            false
        })
        .unwrap();
    let ConditionalCallAuctionCommandOutcome::Declined(observation) = declined else {
        panic!("decline must retain its owned observation");
    };
    assert_observation(&observation, 5);
    assert_eq!(engine.last_indicative(), previous);

    let accepted = engine
        .try_submit_indicative_if(command, |observation| {
            assert_observation(observation, 5);
            true
        })
        .unwrap();
    let ConditionalCallAuctionCommandOutcome::Reported {
        observation: Some(observation),
        report,
    } = accepted
    else {
        panic!("acceptance must retain its exact observation");
    };
    assert_observation(&observation, 5);
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(report.events.len(), 1);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::IndicativePublished(state)
            if state == observation.published_indicative()
    ));
    assert_eq!(
        engine.last_indicative(),
        Some(observation.published_indicative())
    );
    assert_eq!(engine.book().state_revision(), 2);

    assert!(matches!(
        engine
            .try_submit_indicative_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.replayed
    ));
    engine.validate().unwrap();
}

#[test]
fn conditional_indicative_preserves_nullable_frozen_state() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    engine
        .submit(CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
            command_id: CommandId::new(1).unwrap(),
            instrument_id: instrument(),
            instrument_version: version(),
            auction_id: auction(),
            expected_phase_revision: 0,
            target_phase: CallAuctionPhase::Collecting,
            received_at: TimestampNs::from_unix_nanos(1),
        }))
        .unwrap();
    engine
        .submit(CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
            command_id: CommandId::new(2).unwrap(),
            instrument_id: instrument(),
            instrument_version: version(),
            auction_id: auction(),
            expected_phase_revision: 1,
            target_phase: CallAuctionPhase::Frozen,
            received_at: TimestampNs::from_unix_nanos(2),
        }))
        .unwrap();
    let mut command = candidate(3);
    command.expected_phase_revision = 2;
    command.received_at = TimestampNs::from_unix_nanos(3);

    let outcome = engine
        .try_submit_indicative_if(command, |observation| {
            assert_eq!(observation.command_sequence(), 3);
            assert_eq!(observation.first_event_sequence(), 3);
            assert_eq!(observation.phase().phase(), CallAuctionPhase::Frozen);
            assert_eq!(observation.phase().revision(), 2);
            assert_eq!(observation.book_revision(), 0);
            assert_eq!(observation.resulting_book_revision(), 0);
            assert_eq!(observation.previous_indicative(), None);
            assert_eq!(observation.published_indicative().clearing(), None);
            true
        })
        .unwrap();
    let ConditionalCallAuctionCommandOutcome::Reported {
        observation: Some(observation),
        report,
    } = outcome
    else {
        panic!("nullable frozen publication must retain its observation");
    };
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::IndicativePublished(state)
            if state == observation.published_indicative() && state.clearing().is_none()
    ));
    assert_eq!(
        engine.last_indicative(),
        Some(observation.published_indicative())
    );
    assert_eq!(engine.book().state_revision(), 0);
    engine.validate().unwrap();
}

#[test]
fn indicative_business_rejections_bypass_observation_and_predicate() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let called = Cell::new(0_u32);

    let mut wrong_version = candidate(5);
    wrong_version.instrument_version = InstrumentVersion::new(9).unwrap();
    assert!(matches!(
        engine
            .try_submit_indicative_if(wrong_version, |_| {
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

    let mut stale_phase = candidate(6);
    stale_phase.expected_phase_revision = 0;
    assert!(matches!(
        engine
            .try_submit_indicative_if(stale_phase, |_| {
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
    assert_eq!(called.get(), 0);
    assert_eq!(
        engine.last_indicative().unwrap().reference_price(),
        Price::from_raw(101)
    );
    engine.validate().unwrap();
}

#[test]
fn coupled_indicative_is_risk_neutral_on_decline_acceptance_and_replay() {
    let mut managed = managed();
    let command = candidate(5);
    let before_accounts = [
        managed.risk().snapshot(account(1)),
        managed.risk().snapshot(account(2)),
    ];
    let before_reservations = [
        managed.risk().reservation(OrderId::new(20).unwrap()),
        managed.risk().reservation(OrderId::new(30).unwrap()),
    ];

    assert!(matches!(
        managed
            .try_submit_indicative_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(managed.risk().reservation_count(), 2);
    assert_eq!(
        [
            managed.risk().snapshot(account(1)),
            managed.risk().snapshot(account(2)),
        ],
        before_accounts
    );

    assert!(matches!(
        managed
            .try_submit_indicative_if(command, |observation| {
                assert_observation(observation, 5);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(managed.risk().reservation_count(), 2);
    assert_eq!(
        [
            managed.risk().reservation(OrderId::new(20).unwrap()),
            managed.risk().reservation(OrderId::new(30).unwrap()),
        ],
        before_reservations
    );
    assert!(matches!(
        managed
            .try_submit_indicative_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.replayed
    ));
    managed.validate().unwrap();
}

#[test]
fn durable_plain_noncommit_is_wal_free_and_publication_recovers() {
    let file = TestFile::new("plain");
    let mut durable = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let frames_before = frame_count(file.path());
    let mut wrong_version = candidate(5);
    wrong_version.instrument_version = InstrumentVersion::new(9).unwrap();
    assert!(matches!(
        durable
            .try_submit_indicative_if(wrong_version, |_| {
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

    let command = candidate(6);
    let frames_before_noncommit = frame_count(file.path());
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.try_submit_indicative_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before_noncommit);
    assert!(matches!(
        durable
            .try_submit_indicative_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before_noncommit);
    assert!(matches!(
        durable
            .try_submit_indicative_if(command, |_| true)
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
            .try_submit_indicative_if(command, |_| panic!("replay bypasses predicate"))
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
    let recovered = reopened.engine().last_indicative().unwrap();
    assert_eq!(recovered.reference_price(), Price::from_raw(102));
    assert_eq!(recovered.clearing().unwrap().price(), Price::from_raw(102));
    assert_eq!(reopened.engine().book().state_revision(), 2);
    reopened.engine().validate().unwrap();
}

#[test]
fn durable_risk_noncommit_and_publication_recover_without_risk_change() {
    let file = TestFile::new("risk");
    let profiles = profiles();
    let mut durable =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let command = candidate(5);
    let frames_before = frame_count(file.path());
    let before_accounts = [
        durable.managed().risk().snapshot(account(1)),
        durable.managed().risk().snapshot(account(2)),
    ];
    let before_reservations = [
        durable
            .managed()
            .risk()
            .reservation(OrderId::new(20).unwrap()),
        durable
            .managed()
            .risk()
            .reservation(OrderId::new(30).unwrap()),
    ];

    assert!(matches!(
        durable
            .try_submit_indicative_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable
            .try_submit_indicative_if(command, |observation| {
                assert_observation(observation, 5);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    assert_eq!(durable.managed().risk().reservation_count(), 2);
    assert_eq!(
        [
            durable.managed().risk().snapshot(account(1)),
            durable.managed().risk().snapshot(account(2)),
        ],
        before_accounts
    );
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    assert_eq!(reopened.managed().risk().reservation_count(), 2);
    assert_eq!(
        [
            reopened
                .managed()
                .risk()
                .reservation(OrderId::new(20).unwrap()),
            reopened
                .managed()
                .risk()
                .reservation(OrderId::new(30).unwrap()),
        ],
        before_reservations
    );
    let recovered = reopened.managed().engine().last_indicative().unwrap();
    assert_eq!(recovered.reference_price(), Price::from_raw(102));
    assert_eq!(recovered.clearing().unwrap().price(), Price::from_raw(102));
    reopened.managed().validate().unwrap();
}
