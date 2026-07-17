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
    CallAuctionReplaceObservation, CallAuctionReplaceOrder, CallAuctionSubmitOrder,
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
            "quotick-conditional-call-auction-replace-{label}-{}-{nonce}.wal",
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
    InstrumentId::new(94).unwrap()
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
        symbol: InstrumentSymbol::new("CONDITIONAL-REPLACE").unwrap(),
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
        max_retained_commands: 64,
        terminal_command_reserve: 12,
        max_retained_events: 128,
        max_report_events: 17,
    })
    .unwrap()
}

fn order(
    order_id: u64,
    owner: u64,
    side: Side,
    price: i64,
    lots: u64,
    class: u16,
) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        account(owner),
        instrument(),
        version(),
        side,
        AuctionOrderConstraint::Limit(Price::from_raw(price)),
        Quantity::new(lots).unwrap(),
        AuctionPriorityClass::new(class),
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
        command_id: CommandId::new(5).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(),
        expected_phase_revision: 1,
        price_band: price_band(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        received_at: TimestampNs::from_unix_nanos(5),
    })
}

fn seed_commands() -> [CallAuctionCommand; 5] {
    [
        phase(),
        submit(2, order(10, 1, Side::Buy, 100, 5, 1)),
        submit(3, order(11, 2, Side::Buy, 100, 4, 2)),
        submit(4, order(20, 2, Side::Sell, 99, 4, 2)),
        indicative(),
    ]
}

fn replacement_order(lots: u64) -> CallAuctionOrder {
    order(30, 1, Side::Sell, 101, lots, 0)
}

fn replace(command_id: u64, lots: u64) -> CallAuctionReplaceOrder {
    CallAuctionReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(),
        expected_phase_revision: 1,
        account_id: account(1),
        target_order_id: OrderId::new(10).unwrap(),
        replacement: replacement_order(lots),
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
        let maximum_order_quantity = if owner == 1 { 6 } else { 100 };
        AccountRiskDefinition::new(
            account(owner),
            RiskProfile::new(
                AccountRiskState::Active,
                0,
                RiskLimits::new(RiskLimitSpec {
                    max_order_quantity_lots: maximum_order_quantity,
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

fn assert_observation(
    observation: &CallAuctionReplaceObservation,
    command_id: u64,
    command_sequence: u64,
    first_event_sequence: u64,
) {
    assert_eq!(
        observation.command_id(),
        CommandId::new(command_id).unwrap()
    );
    assert_eq!(observation.instrument_id(), instrument());
    assert_eq!(observation.instrument_version(), version());
    assert_eq!(observation.command_sequence(), command_sequence);
    assert_eq!(observation.first_event_sequence(), first_event_sequence);
    assert_eq!(
        observation.second_event_sequence(),
        first_event_sequence + 1
    );
    assert_eq!(observation.phase().phase(), CallAuctionPhase::Collecting);
    assert_eq!(observation.phase().revision(), 1);
    assert_eq!(observation.phase().active_auction_id(), Some(auction()));
    assert_eq!(observation.book_revision(), 3);
    assert_eq!(observation.resulting_book_revision(), 4);
    assert_eq!(observation.account_id(), account(1));
    assert_eq!(observation.target_order_id(), OrderId::new(10).unwrap());
    assert_eq!(
        observation.replacement_order_id(),
        OrderId::new(30).unwrap()
    );
    assert_eq!(
        observation.received_at(),
        TimestampNs::from_unix_nanos(command_id)
    );
    let cancelled = observation.cancelled();
    let accepted = observation.accepted();
    assert_eq!(cancelled.order_id, OrderId::new(10).unwrap());
    assert_eq!(cancelled.account_id, account(1));
    assert_eq!(cancelled.side, Side::Buy);
    assert_eq!(
        cancelled.constraint,
        AuctionOrderConstraint::Limit(Price::from_raw(100))
    );
    assert_eq!(cancelled.quantity, Quantity::new(5).unwrap());
    assert_eq!(cancelled.priority_class, AuctionPriorityClass::new(1));
    assert_eq!(cancelled.priority_sequence, 1);
    assert_eq!(accepted.order_id, OrderId::new(30).unwrap());
    assert_eq!(accepted.account_id, account(1));
    assert_eq!(accepted.side, Side::Sell);
    assert_eq!(
        accepted.constraint,
        AuctionOrderConstraint::Limit(Price::from_raw(101))
    );
    assert_eq!(accepted.quantity, Quantity::new(3).unwrap());
    assert_eq!(accepted.priority_class, AuctionPriorityClass::HIGHEST);
    assert_eq!(accepted.priority_sequence, 4);
    let prior = observation.previous_indicative().unwrap();
    assert_eq!(prior.book_revision(), 3);
    let clearing = prior.clearing().unwrap();
    assert_eq!(clearing.price(), Price::from_raw(100));
    assert_eq!(clearing.executable_quantity(), 4);
    assert_eq!(clearing.buy_quantity(), 9);
    assert_eq!(clearing.sell_quantity(), 4);
}

#[test]
fn conditional_replace_is_fixed_size_atomic_fresh_priority_and_replayable() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<CallAuctionReplaceObservation>();
    assert!(!std::mem::needs_drop::<CallAuctionReplaceObservation>());
    assert!(
        std::mem::size_of::<CallAuctionReplaceObservation>() < 320,
        "observation size is {} bytes",
        std::mem::size_of::<CallAuctionReplaceObservation>(),
    );

    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let command = replace(6, 3);
    let phase_before = engine.phase_snapshot();
    let indicative_before = engine.last_indicative();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = engine.try_submit_replace_order_if(command, |observation| {
            assert_observation(observation, 6, 6, 6);
            panic!("policy failure")
        });
    }));
    assert!(panicked.is_err());
    assert_eq!(engine.next_command_sequence(), 6);
    assert_eq!(engine.next_event_sequence(), 6);
    assert_eq!(engine.phase_snapshot(), phase_before);
    assert_eq!(engine.last_indicative(), indicative_before);
    assert!(engine.book().order(OrderId::new(30).unwrap()).is_none());
    assert_eq!(
        engine
            .book()
            .order(OrderId::new(10).unwrap())
            .unwrap()
            .quantity,
        Quantity::new(5).unwrap()
    );

    let declined = engine
        .try_submit_replace_order_if(command, |observation| {
            assert_observation(observation, 6, 6, 6);
            false
        })
        .unwrap();
    let ConditionalCallAuctionCommandOutcome::Declined(observation) = declined else {
        panic!("decline must retain its owned observation");
    };
    assert_observation(&observation, 6, 6, 6);
    assert_eq!(engine.next_command_sequence(), 6);
    assert_eq!(engine.book().state_revision(), 3);

    let accepted = engine
        .try_submit_replace_order_if(command, |observation| {
            assert_observation(observation, 6, 6, 6);
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
    assert_observation(&observation, 6, 6, 6);
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(report.events.len(), 2);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::OrderCancelled { order, reason }
            if order == observation.cancelled()
                && reason == quotick::auction_engine::CallAuctionCancellationReason::Replaced
    ));
    assert!(matches!(
        report.events[1].kind,
        CallAuctionEventKind::OrderAccepted(order) if order == observation.accepted()
    ));
    assert_eq!(engine.book().state_revision(), 4);
    assert!(engine.book().order(OrderId::new(10).unwrap()).is_none());
    assert_eq!(
        engine.book().order(OrderId::new(30).unwrap()).unwrap(),
        observation.accepted()
    );
    assert_eq!(engine.last_indicative(), None);

    assert!(matches!(
        engine
            .try_submit_replace_order_if(command, |_| panic!("replay bypasses predicate"))
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.replayed
    ));
    engine.validate().unwrap();
}

#[test]
fn replace_business_rejections_bypass_observation_and_predicate() {
    let mut engine = CallAuctionEngine::try_with_limits(definition(), engine_limits()).unwrap();
    seed_engine(&mut engine);
    let called = Cell::new(0_u32);

    let cases = [
        {
            let mut command = replace(6, 3);
            command.replacement = CallAuctionOrder::new(
                OrderId::new(30).unwrap(),
                account(1),
                instrument(),
                InstrumentVersion::new(8).unwrap(),
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(101)),
                Quantity::new(3).unwrap(),
                AuctionPriorityClass::HIGHEST,
            );
            (command, CallAuctionRejectReason::WrongInstrumentVersion)
        },
        {
            let mut command = replace(7, 3);
            command.expected_phase_revision = 2;
            (
                command,
                CallAuctionRejectReason::PhaseRevisionMismatch {
                    observed: 2,
                    current: 1,
                },
            )
        },
        {
            let mut command = replace(8, 3);
            command.auction_id = AuctionId::new(2).unwrap();
            (
                command,
                CallAuctionRejectReason::AuctionIdMismatch {
                    observed: AuctionId::new(2).unwrap(),
                    current: Some(auction()),
                },
            )
        },
        {
            let mut command = replace(9, 3);
            command.account_id = account(2);
            (command, CallAuctionRejectReason::NotOrderOwner)
        },
        {
            let mut command = replace(10, 3);
            command.target_order_id = OrderId::new(99).unwrap();
            (command, CallAuctionRejectReason::UnknownOrder)
        },
        {
            let mut command = replace(11, 3);
            command.replacement = order(30, 2, Side::Sell, 101, 3, 0);
            (command, CallAuctionRejectReason::NotOrderOwner)
        },
        {
            let mut command = replace(12, 3);
            command.replacement = order(11, 1, Side::Sell, 101, 3, 0);
            (command, CallAuctionRejectReason::DuplicateOrder)
        },
    ];
    for (command, expected) in cases {
        let result = engine
            .try_submit_replace_order_if(command, |_| {
                called.set(called.get() + 1);
                true
            })
            .unwrap();
        assert!(matches!(
            result,
            ConditionalCallAuctionCommandOutcome::Reported {
                observation: None,
                report,
            } if report.outcome == CallAuctionCommandOutcome::Rejected(expected)
        ));
    }
    assert_eq!(called.get(), 0);
    assert!(engine.book().order(OrderId::new(30).unwrap()).is_none());
    assert_eq!(
        engine
            .book()
            .order(OrderId::new(10).unwrap())
            .unwrap()
            .quantity,
        Quantity::new(5).unwrap()
    );
    engine.validate().unwrap();
}

#[test]
fn coupled_replace_bypasses_risk_rejection_and_nets_the_accepted_reservation() {
    let mut managed = managed();
    seed_managed(&mut managed);
    let target_before = managed
        .risk()
        .reservation(OrderId::new(10).unwrap())
        .unwrap();
    let other_before = managed
        .risk()
        .reservation(OrderId::new(11).unwrap())
        .unwrap();
    let indicative_before = managed.engine().last_indicative();
    let called = Cell::new(false);

    let rejected = managed
        .try_submit_replace_order_if(replace(6, 7), |_| {
            called.set(true);
            true
        })
        .unwrap();
    assert!(!called.get());
    assert!(matches!(
        rejected,
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome
            == CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::RiskOrderQuantityLimit)
    ));
    assert_eq!(
        managed.risk().reservation(OrderId::new(10).unwrap()),
        Some(target_before)
    );
    assert_eq!(managed.engine().last_indicative(), indicative_before);
    assert!(
        managed
            .engine()
            .book()
            .order(OrderId::new(30).unwrap())
            .is_none()
    );

    assert!(matches!(
        managed
            .try_submit_replace_order_if(replace(7, 3), |observation| {
                assert_observation(observation, 7, 7, 7);
                false
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(
        managed.risk().reservation(OrderId::new(10).unwrap()),
        Some(target_before)
    );

    assert!(matches!(
        managed
            .try_submit_replace_order_if(replace(7, 3), |observation| {
                assert_observation(observation, 7, 7, 7);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(10).unwrap())
            .is_none()
    );
    let replacement = managed
        .risk()
        .reservation(OrderId::new(30).unwrap())
        .unwrap();
    assert_eq!(replacement.quantity_lots(), 3);
    assert_eq!(
        replacement.notional(),
        u128::from(replacement.valuation_per_lot()) * 3
    );
    assert_eq!(
        managed.risk().reservation(OrderId::new(11).unwrap()),
        Some(other_before)
    );
    assert_eq!(managed.risk().reservation_count(), 3);
    managed.validate().unwrap();
}

#[test]
fn durable_plain_replace_noncommit_is_wal_free_and_acceptance_recovers() {
    let file = TestFile::new("plain");
    let mut durable = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let command = replace(6, 3);
    let frames_before = frame_count(file.path());
    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.try_submit_replace_order_if(command, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable
            .try_submit_replace_order_if(command, |_| false)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert!(matches!(
        durable
            .try_submit_replace_order_if(command, |_| true)
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
            .try_submit_replace_order_if(command, |_| panic!("replay bypasses predicate"))
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
            .is_none()
    );
    let order = reopened
        .engine()
        .book()
        .order(OrderId::new(30).unwrap())
        .unwrap();
    assert_eq!(order.quantity, Quantity::new(3).unwrap());
    assert_eq!(order.priority_sequence, 4);
    reopened.engine().validate().unwrap();
}

#[test]
fn durable_risk_replace_rejection_noncommit_and_acceptance_recover() {
    let file = TestFile::new("risk");
    let profiles = profiles();
    let mut durable =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    for command in seed_commands() {
        durable.submit(command).unwrap();
    }
    let frames_before = frame_count(file.path());
    let reservation_before = durable
        .managed()
        .risk()
        .reservation(OrderId::new(10).unwrap())
        .unwrap();
    let called = Cell::new(false);
    assert!(matches!(
        durable
            .try_submit_replace_order_if(replace(6, 7), |_| {
                called.set(true);
                true
            })
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: None,
            report,
        } if report.outcome
            == CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::RiskOrderQuantityLimit)
    ));
    assert!(!called.get());
    assert_eq!(frame_count(file.path()), frames_before + 2);
    let frames_after_rejection = frame_count(file.path());
    assert_eq!(
        durable
            .managed()
            .risk()
            .reservation(OrderId::new(10).unwrap()),
        Some(reservation_before)
    );

    assert!(matches!(
        durable
            .try_submit_replace_order_if(replace(7, 3), |_| false)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Declined(_)
    ));
    assert_eq!(frame_count(file.path()), frames_after_rejection);
    assert!(matches!(
        durable
            .try_submit_replace_order_if(replace(7, 3), |_| true)
            .unwrap(),
        ConditionalCallAuctionCommandOutcome::Reported {
            observation: Some(_),
            report,
        } if report.outcome == CallAuctionCommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_after_rejection + 2);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableCallAuctionRiskEngine::open(file.path(), definition(), &profiles, options())
            .unwrap();
    assert!(
        reopened
            .managed()
            .risk()
            .reservation(OrderId::new(10).unwrap())
            .is_none()
    );
    assert_eq!(
        reopened
            .managed()
            .risk()
            .reservation(OrderId::new(30).unwrap())
            .unwrap()
            .quantity_lots(),
        3
    );
    assert_eq!(
        reopened
            .managed()
            .engine()
            .book()
            .order(OrderId::new(30).unwrap())
            .unwrap()
            .priority_sequence,
        4
    );
    reopened.managed().validate().unwrap();
}
