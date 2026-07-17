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
    CallAuctionPhase, CallAuctionPhaseControl, CallAuctionRejectReason,
    CallAuctionSubmitObservation, CallAuctionSubmitOrder, ConditionalCallAuctionCommandOutcome,
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
            "quotick-conditional-call-auction-submit-{label}-{}-{nonce}.wal",
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
    InstrumentId::new(96).unwrap()
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
        symbol: InstrumentSymbol::new("CONDITIONAL-SUBMIT").unwrap(),
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

fn submit(command_id: u64, order: CallAuctionOrder) -> CallAuctionSubmitOrder {
    CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(),
        expected_phase_revision: 1,
        order,
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
        CallAuctionCommand::Submit(submit(2, order(20, 2, Side::Buy, 102, 5))),
        CallAuctionCommand::Submit(submit(3, order(30, 3, Side::Sell, 101, 4))),
        CallAuctionCommand::Indicative(CallAuctionIndicativeCommand {
            command_id: CommandId::new(4).unwrap(),
            instrument_id: instrument(),
            instrument_version: version(),
            auction_id: auction(),
            expected_phase_revision: 1,
            price_band: price_band(),
            reference_price: Price::from_raw(101),
            price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
            received_at: TimestampNs::from_unix_nanos(4),
        }),
    ]
}

fn candidate(command_id: u64, owner: u64) -> CallAuctionSubmitOrder {
    submit(command_id, order(10, owner, Side::Buy, 103, 3))
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

fn assert_observation(observation: CallAuctionSubmitObservation, sequence: u64) {
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
    let accepted = observation.accepted();
    assert_eq!(accepted.order_id, OrderId::new(10).unwrap());
    assert_eq!(accepted.account_id, account(1));
    assert_eq!(accepted.side, Side::Buy);
    assert_eq!(
        accepted.constraint,
        AuctionOrderConstraint::Limit(Price::from_raw(103))
    );
    assert_eq!(accepted.quantity, Quantity::new(3).unwrap());
    assert_eq!(accepted.priority_class, AuctionPriorityClass::new(1));
    assert_eq!(accepted.priority_sequence, 3);
    let previous = observation.previous_indicative().unwrap();
    assert_eq!(previous.auction_id(), auction());
    assert_eq!(previous.book_revision(), 2);
    assert_eq!(previous.clearing().unwrap().executable_quantity(), 4);
}

#[test]
fn conditional_submission_is_fixed_size_atomic_and_exactly_replayable() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CallAuctionSubmitObservation>();
    assert!(!std::mem::needs_drop::<CallAuctionSubmitObservation>());
    assert!(std::mem::size_of::<CallAuctionSubmitObservation>() < 256);

    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let command = candidate(5, 1);
    let indication_before = engine.last_indicative();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = engine.try_submit_order_if(command, |observation| {
            assert_observation(*observation, 5);
            panic!("policy failure")
        });
    }));
    assert!(panicked.is_err());
    assert_eq!(engine.next_command_sequence(), 5);
    assert_eq!(engine.next_event_sequence(), 5);
    assert_eq!(engine.book().state_revision(), 2);
    assert_eq!(engine.last_indicative(), indication_before);
    assert!(engine.book().order(OrderId::new(10).unwrap()).is_none());

    let declined = engine
        .try_submit_order_if(command, |observation| {
            assert_observation(*observation, 5);
            false
        })
        .unwrap();
    let ConditionalCallAuctionCommandOutcome::Declined(observation) = declined else {
        panic!("decline must retain its owned observation");
    };
    assert_observation(observation, 5);
    assert_eq!(engine.book().state_revision(), 2);
    assert!(engine.book().order(OrderId::new(10).unwrap()).is_none());

    let accepted = engine
        .try_submit_order_if(command, |observation| {
            assert_observation(*observation, 5);
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
    assert_observation(observation, 5);
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(report.events.len(), 1);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::OrderAccepted(order) if order == observation.accepted()
    ));
    assert_eq!(
        engine.book().order(OrderId::new(10).unwrap()),
        Some(observation.accepted())
    );
    assert_eq!(engine.last_indicative(), None);

    assert!(matches!(
        engine
            .try_submit_order_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.replayed
    ));
    engine.validate().unwrap();
}

#[test]
fn submission_business_rejections_bypass_observation_and_predicate() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let called = Cell::new(0_u32);

    let mut wrong_version = candidate(5, 1);
    wrong_version.order = CallAuctionOrder::new(
        OrderId::new(10).unwrap(),
        account(1),
        instrument(),
        InstrumentVersion::new(8).unwrap(),
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(103)),
        Quantity::new(3).unwrap(),
        AuctionPriorityClass::new(1),
    );
    assert!(matches!(
        engine
            .try_submit_order_if(wrong_version, |_| {
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

    let duplicate = submit(6, order(20, 1, Side::Buy, 103, 3));
    assert!(matches!(
        engine
            .try_submit_order_if(duplicate, |_| {
                called.set(called.get() + 1);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome == CallAuctionCommandOutcome::Rejected(
            CallAuctionRejectReason::DuplicateOrder
        )
    ));
    assert_eq!(called.get(), 0);
    assert_eq!(engine.book().active_order_count(), 2);
    engine.validate().unwrap();
}

#[test]
fn coupled_risk_rejection_bypasses_observation_and_acceptance_reserves_exactly() {
    let mut rejected = managed();
    let called = Cell::new(false);
    assert!(matches!(
        rejected
            .try_submit_order_if(candidate(5, 4), |_| {
                called.set(true);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome == CallAuctionCommandOutcome::Rejected(
            CallAuctionRejectReason::RiskProfileMissing
        )
    ));
    assert!(!called.get());
    assert_eq!(rejected.risk().reservation_count(), 2);

    let mut managed = managed();
    let command = candidate(5, 1);
    assert!(matches!(
        managed.try_submit_order_if(command, |_| false).unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(managed.risk().reservation_count(), 2);
    assert!(matches!(
        managed
            .try_submit_order_if(command, |observation| {
                assert_observation(*observation, 5);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(managed.risk().reservation_count(), 3);
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(10).unwrap())
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
    let frames_before = frame_count(file.path());
    let mut wrong_version = candidate(5, 1);
    wrong_version.order = CallAuctionOrder::new(
        OrderId::new(10).unwrap(),
        account(1),
        instrument(),
        InstrumentVersion::new(8).unwrap(),
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(103)),
        Quantity::new(3).unwrap(),
        AuctionPriorityClass::new(1),
    );
    assert!(matches!(
        durable
            .try_submit_order_if(wrong_version, |_| {
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

    let command = candidate(6, 1);
    let frames_before_noncommit = frame_count(file.path());
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.try_submit_order_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before_noncommit);
    assert!(matches!(
        durable.try_submit_order_if(command, |_| false).unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before_noncommit);
    assert!(matches!(
        durable.try_submit_order_if(command, |_| true).unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before_noncommit + 2);
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_order_if(command, |_| panic!("replay bypasses predicate"))
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
    assert!(
        reopened
            .engine()
            .book()
            .order(OrderId::new(10).unwrap())
            .is_some()
    );
    reopened.engine().validate().unwrap();
}

#[test]
fn durable_risk_rejection_noncommit_and_acceptance_recover_exactly() {
    let file = TestFile::new("risk");
    let profiles = profiles();
    let mut durable =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let frames_before = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_order_if(candidate(5, 4), |_| {
                panic!("risk rejection bypasses predicate")
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome == CallAuctionCommandOutcome::Rejected(
            CallAuctionRejectReason::RiskProfileMissing
        )
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    let command = candidate(6, 1);
    assert!(matches!(
        durable.try_submit_order_if(command, |_| false).unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    assert_eq!(durable.managed().risk().reservation_count(), 2);
    assert!(matches!(
        durable
            .try_submit_order_if(command, |observation| {
                assert_observation(*observation, 6);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 4);
    assert_eq!(durable.managed().risk().reservation_count(), 3);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    assert_eq!(reopened.managed().risk().reservation_count(), 3);
    assert!(
        reopened
            .managed()
            .risk()
            .reservation(OrderId::new(10).unwrap())
            .is_some()
    );
    reopened.managed().validate().unwrap();
}
