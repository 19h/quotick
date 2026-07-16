use quotick::auction::{AuctionOrderConstraint, AuctionPricePolicy};
use quotick::auction_book::{
    CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder, CallAuctionPrepareError,
    CallAuctionRemainderPolicy, CallAuctionSelfTradePolicy, CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionAction, CallAuctionCancelOrder, CallAuctionCommand, CallAuctionCommandOutcome,
    CallAuctionEngine, CallAuctionEngineCapacity, CallAuctionEngineConstructionError,
    CallAuctionEngineError, CallAuctionEngineLimits, CallAuctionEngineLimitsError,
    CallAuctionEngineLimitsSpec, CallAuctionEventKind, CallAuctionMassCancel, CallAuctionPhase,
    CallAuctionPhaseControl, CallAuctionRejectReason, CallAuctionReplaceOrder,
    CallAuctionSubmitOrder, CallAuctionUncrossCommand,
};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::MassCancelScope;
use quotick::{
    AccountId, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, Side, TimestampNs,
};

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn instrument() -> InstrumentId {
    InstrumentId::new(41).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(7).unwrap()
}

fn auction(value: u64) -> AuctionId {
    AuctionId::new(value).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("ENGINE").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 5, Price::from_raw(-100), Price::from_raw(200)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Halted,
    })
    .unwrap()
}

fn book_limits(active: usize, accepted: usize) -> CallAuctionBookLimits {
    CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: active,
        max_price_levels_per_side: active,
        max_accepted_order_ids: accepted,
        max_prepared_uncrosses: 2,
    })
    .unwrap()
}

fn engine_limits(active: usize, accepted: usize, retained: usize) -> CallAuctionEngineLimits {
    let book = book_limits(active, accepted);
    CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: retained,
        terminal_command_reserve: active + 2,
        max_retained_events: retained + active * 2 + 2,
        max_report_events: active * 2 + 1,
    })
    .unwrap()
}

fn phase_command(
    command_id: u64,
    auction_id: u64,
    expected_revision: u64,
    target_phase: CallAuctionPhase,
) -> CallAuctionCommand {
    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(auction_id),
        expected_phase_revision: expected_revision,
        target_phase,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn order(
    order_id: u64,
    owner: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u64,
) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        account(owner),
        instrument(),
        version(),
        side,
        constraint,
        Quantity::new(quantity).unwrap(),
    )
}

fn submit_command(command_id: u64, order: CallAuctionOrder) -> CallAuctionCommand {
    submit_command_at(command_id, 1, 1, order)
}

fn submit_command_at(
    command_id: u64,
    auction_id: u64,
    expected_phase_revision: u64,
    order: CallAuctionOrder,
) -> CallAuctionCommand {
    CallAuctionCommand::Submit(CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(auction_id),
        expected_phase_revision,
        order,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn cancel_command(command_id: u64, owner: u64, order_id: u64) -> CallAuctionCommand {
    CallAuctionCommand::Cancel(CallAuctionCancelOrder {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        account_id: account(owner),
        order_id: OrderId::new(order_id).unwrap(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn mass_cancel_command(command_id: u64, owner: u64, scope: MassCancelScope) -> CallAuctionCommand {
    CallAuctionCommand::MassCancel(CallAuctionMassCancel {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        account_id: account(owner),
        scope,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn replace_command(
    command_id: u64,
    target_order_id: u64,
    replacement: CallAuctionOrder,
) -> CallAuctionCommand {
    CallAuctionCommand::Replace(CallAuctionReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(1),
        expected_phase_revision: 1,
        account_id: replacement.account_id(),
        target_order_id: OrderId::new(target_order_id).unwrap(),
        replacement,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn uncross_command(
    engine: &CallAuctionEngine,
    command_id: u64,
    auction_id: u64,
    expected_revision: u64,
    remainder: CallAuctionRemainderPolicy,
) -> CallAuctionCommand {
    CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(auction_id),
        expected_phase_revision: expected_revision,
        price_band: engine.book().instrument_price_band(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        uncross_policy: CallAuctionUncrossPolicy::new(
            remainder,
            CallAuctionSelfTradePolicy::Permit,
        ),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

#[test]
fn lifecycle_sequences_collection_freeze_uncross_and_exact_replay() {
    let mut engine =
        CallAuctionEngine::try_with_limits(definition(), engine_limits(8, 16, 32)).unwrap();
    assert_eq!(
        engine
            .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
            .unwrap()
            .outcome,
        CallAuctionCommandOutcome::Accepted
    );
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 6),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(
                2,
                2,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                5,
            ),
        ))
        .unwrap();
    let indicative = engine
        .indicative(
            auction(1),
            engine.book().instrument_price_band(),
            Price::from_raw(100),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        )
        .unwrap()
        .unwrap();
    assert_eq!(indicative.clearing().executable_quantity(), 5);
    engine
        .submit(phase_command(4, 1, 1, CallAuctionPhase::Frozen))
        .unwrap();
    let command = uncross_command(&engine, 5, 1, 2, CallAuctionRemainderPolicy::RetainAll);
    let report = engine.submit(command).unwrap();
    assert_eq!(report.command_sequence, 5);
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(report.events.len(), 2);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::Trade(_)
    ));
    assert!(matches!(
        report.events[1].kind,
        CallAuctionEventKind::UncrossCompleted {
            auction_id,
            trade_count: 1,
            cancellation_count: 0,
            phase_revision: 3,
            ..
        } if auction_id == auction(1)
    ));
    assert_eq!(report.events[0].sequence, 5);
    assert_eq!(report.events[1].sequence, 6);
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Closed);
    assert_eq!(engine.phase_snapshot().revision(), 3);
    assert_eq!(engine.book().active_order_count(), 1);
    assert_eq!(
        engine
            .book()
            .order(OrderId::new(1).unwrap())
            .unwrap()
            .quantity
            .lots(),
        1
    );

    let closed_cancel = engine.submit(cancel_command(6, 1, 1)).unwrap();
    assert_eq!(closed_cancel.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(engine.book().active_order_count(), 0);

    let replay = engine.submit(command).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.command_sequence, report.command_sequence);
    assert!(replay.events.shares_storage_with(&report.events));
    assert_eq!(engine.next_command_sequence(), 7);
    assert_eq!(engine.next_event_sequence(), 8);
    engine.validate().unwrap();
}

#[test]
fn business_rejections_are_sequenced_and_collisions_are_operational() {
    let mut engine =
        CallAuctionEngine::try_with_limits(definition(), engine_limits(4, 8, 16)).unwrap();
    let command = submit_command_at(
        1,
        1,
        0,
        order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
    );
    let rejected = engine.submit(command).unwrap();
    assert_eq!(
        rejected.outcome,
        CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::ActionNotAllowed {
            action: CallAuctionAction::Submit,
            phase: CallAuctionPhase::Closed,
        })
    );
    let replay = engine.submit(command).unwrap();
    assert!(replay.replayed);
    assert!(replay.events.shares_storage_with(&rejected.events));

    let collision = submit_command(1, order(2, 1, Side::Buy, AuctionOrderConstraint::Market, 2));
    assert!(matches!(
        engine.submit(collision),
        Err(CallAuctionEngineError::CommandIdCollision(command_id))
            if command_id == CommandId::new(1).unwrap()
    ));
    let wrong_cycle = engine
        .submit(phase_command(2, 2, 0, CallAuctionPhase::Collecting))
        .unwrap();
    assert_eq!(
        wrong_cycle.outcome,
        CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::AuctionIdNotNext {
            expected: auction(1),
            observed: auction(2),
        })
    );
    engine
        .submit(phase_command(3, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    let stale = engine
        .submit(phase_command(4, 1, 0, CallAuctionPhase::Frozen))
        .unwrap();
    assert_eq!(
        stale.outcome,
        CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::PhaseRevisionMismatch {
            observed: 0,
            current: 1,
        })
    );
    let stale_entry = engine
        .submit(submit_command_at(
            5,
            1,
            0,
            order(3, 1, Side::Buy, AuctionOrderConstraint::Market, 1),
        ))
        .unwrap();
    assert_eq!(
        stale_entry.outcome,
        CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::PhaseRevisionMismatch {
            observed: 0,
            current: 1,
        })
    );
    let wrong_entry_cycle = engine
        .submit(submit_command_at(
            6,
            2,
            1,
            order(4, 1, Side::Buy, AuctionOrderConstraint::Market, 1),
        ))
        .unwrap();
    assert_eq!(
        wrong_entry_cycle.outcome,
        CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::AuctionIdMismatch {
            observed: auction(2),
            current: Some(auction(1)),
        })
    );
    assert_eq!(engine.book().active_order_count(), 0);
    engine.validate().unwrap();
}

#[test]
fn cancel_replace_is_one_idempotent_command_with_two_order_events() {
    let mut engine =
        CallAuctionEngine::try_with_limits(definition(), engine_limits(2, 4, 16)).unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(
                1,
                7,
                Side::Buy,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                5,
            ),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(
                2,
                8,
                Side::Buy,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                4,
            ),
        ))
        .unwrap();
    let command = replace_command(
        4,
        1,
        order(
            3,
            7,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(105)),
            3,
        ),
    );
    let report = engine.submit(command).unwrap();
    assert_eq!(report.command_sequence, 4);
    assert_eq!(report.events.len(), 2);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::OrderCancelled {
            order,
            reason: quotick::auction_engine::CallAuctionCancellationReason::Replaced,
        } if order.order_id == OrderId::new(1).unwrap()
    ));
    assert!(matches!(
        report.events[1].kind,
        CallAuctionEventKind::OrderAccepted(order)
            if order.order_id == OrderId::new(3).unwrap() && order.priority_sequence == 3
    ));
    assert_eq!(
        (report.events[0].sequence, report.events[1].sequence),
        (4, 5)
    );
    assert!(engine.book().order(OrderId::new(1).unwrap()).is_none());
    assert!(engine.book().order(OrderId::new(3).unwrap()).is_some());
    let replay = engine.submit(command).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.events, report.events);
    assert_eq!(engine.next_command_sequence(), 5);
    engine.validate().unwrap();
}

#[test]
fn protected_history_accepts_only_valid_terminal_controls() {
    let mut engine =
        CallAuctionEngine::try_with_limits(definition(), engine_limits(2, 4, 7)).unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    assert_eq!(engine.resource_status().ordinary_command_capacity, 3);

    assert!(matches!(
        engine.submit(cancel_command(4, 99, 1)),
        Err(CallAuctionEngineError::CapacityExhausted(
            CallAuctionEngineCapacity::AdmissionCommandHistory
        ))
    ));
    assert_eq!(engine.resource_status().retained_commands, 3);
    engine
        .submit(phase_command(5, 1, 1, CallAuctionPhase::Frozen))
        .unwrap();
    engine
        .submit(uncross_command(
            &engine,
            6,
            1,
            2,
            CallAuctionRemainderPolicy::CancelAll,
        ))
        .unwrap();
    assert_eq!(engine.resource_status().retained_commands, 5);
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Closed);
    assert_eq!(engine.book().active_order_count(), 0);
    engine.validate().unwrap();
}

#[test]
fn mass_cancel_sequences_canonical_events_and_exact_empty_replay_in_every_phase() {
    let mut engine =
        CallAuctionEngine::try_with_limits(definition(), engine_limits(8, 16, 24)).unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    for (command_id, order) in [
        (
            2,
            order(50, 7, Side::Sell, AuctionOrderConstraint::Market, 5),
        ),
        (
            3,
            order(10, 7, Side::Buy, AuctionOrderConstraint::Market, 3),
        ),
        (
            4,
            order(30, 8, Side::Buy, AuctionOrderConstraint::Market, 11),
        ),
        (
            5,
            order(40, 7, Side::Sell, AuctionOrderConstraint::Market, 7),
        ),
    ] {
        engine.submit(submit_command(command_id, order)).unwrap();
    }
    engine
        .submit(phase_command(6, 1, 1, CallAuctionPhase::Frozen))
        .unwrap();

    let command = mass_cancel_command(7, 7, MassCancelScope::Side(Side::Sell));
    let report = engine.submit(command).unwrap();
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(report.events.len(), 3);
    assert!(matches!(
        report.events[0].kind,
        CallAuctionEventKind::OrderCancelled { order, reason: quotick::auction_engine::CallAuctionCancellationReason::MassCancel }
            if order.order_id == OrderId::new(40).unwrap()
    ));
    assert!(matches!(
        report.events[1].kind,
        CallAuctionEventKind::OrderCancelled { order, reason: quotick::auction_engine::CallAuctionCancellationReason::MassCancel }
            if order.order_id == OrderId::new(50).unwrap()
    ));
    assert!(matches!(
        report.events[2].kind,
        CallAuctionEventKind::MassCancelCompleted {
            account_id,
            scope: MassCancelScope::Side(Side::Sell),
            cancelled_order_count: 2,
            cancelled_quantity_lots: 12,
            book_revision: 5,
        } if account_id == account(7)
    ));
    assert_eq!(engine.book().active_order_count(), 2);
    let replay = engine.submit(command).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.events, report.events);

    engine
        .submit(phase_command(8, 1, 2, CallAuctionPhase::Closed))
        .unwrap();
    let empty_command = mass_cancel_command(9, 77, MassCancelScope::All);
    let empty = engine.submit(empty_command).unwrap();
    assert_eq!(empty.events.len(), 1);
    assert!(matches!(
        empty.events[0].kind,
        CallAuctionEventKind::MassCancelCompleted {
            account_id,
            scope: MassCancelScope::All,
            cancelled_order_count: 0,
            cancelled_quantity_lots: 0,
            book_revision: 5,
        } if account_id == account(77)
    ));
    let replay = engine.submit(empty_command).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.events, empty.events);
    engine.validate().unwrap();
}

#[test]
fn protected_lane_accepts_only_nonempty_mass_cancel() {
    let mut engine =
        CallAuctionEngine::try_with_limits(definition(), engine_limits(2, 4, 7)).unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();

    assert!(matches!(
        engine.submit(mass_cancel_command(4, 99, MassCancelScope::All)),
        Err(CallAuctionEngineError::CapacityExhausted(
            CallAuctionEngineCapacity::AdmissionCommandHistory
        ))
    ));
    let report = engine
        .submit(mass_cancel_command(5, 1, MassCancelScope::All))
        .unwrap();
    assert_eq!(report.events.len(), 2);
    assert!(engine.book().order(OrderId::new(1).unwrap()).is_none());
    engine.validate().unwrap();
}

#[test]
fn closed_cancellation_prevents_retained_interest_from_becoming_stranded() {
    let mut engine =
        CallAuctionEngine::try_with_limits(definition(), engine_limits(2, 4, 7)).unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();

    engine
        .submit(phase_command(4, 1, 1, CallAuctionPhase::Closed))
        .unwrap();
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Closed);
    assert_eq!(engine.book().active_order_count(), 2);
    assert!(matches!(
        engine.submit(cancel_command(5, 99, 1)),
        Err(CallAuctionEngineError::CapacityExhausted(
            CallAuctionEngineCapacity::AdmissionCommandHistory
        ))
    ));
    engine.submit(cancel_command(6, 1, 1)).unwrap();
    engine.submit(cancel_command(7, 2, 2)).unwrap();
    assert_eq!(engine.book().active_order_count(), 0);
    assert_eq!(engine.resource_status().retained_commands, 6);
    engine.validate().unwrap();
}

#[test]
fn prepared_commands_reject_foreign_and_stale_state_but_exact_races_replay() {
    let limits = engine_limits(4, 8, 16);
    let mut first = CallAuctionEngine::try_with_limits(definition(), limits).unwrap();
    let mut second = CallAuctionEngine::try_with_limits(definition(), limits).unwrap();
    let command = phase_command(1, 1, 0, CallAuctionPhase::Collecting);
    let prepared = match first.prepare(command).unwrap() {
        quotick::auction_engine::CallAuctionCommandPreparation::Ready(value) => value,
        quotick::auction_engine::CallAuctionCommandPreparation::Replay(_) => unreachable!(),
    };
    assert_eq!(first.resource_status().retained_events, 0);
    assert!(matches!(
        second.commit(prepared),
        Err(CallAuctionEngineError::ForeignPreparation)
    ));
    assert_eq!(first.resource_status().retained_events, 0);
    assert_eq!(second.resource_status().retained_events, 0);

    let prepared = match first.prepare(command).unwrap() {
        quotick::auction_engine::CallAuctionCommandPreparation::Ready(value) => value,
        quotick::auction_engine::CallAuctionCommandPreparation::Replay(_) => unreachable!(),
    };
    assert_eq!(first.resource_status().retained_events, 0);
    first
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 1),
        ))
        .unwrap();
    assert_eq!(first.resource_status().retained_events, 1);
    assert!(matches!(
        first.commit(prepared),
        Err(CallAuctionEngineError::StalePreparation)
    ));
    assert_eq!(first.resource_status().retained_events, 1);

    let command = phase_command(3, 1, 0, CallAuctionPhase::Collecting);
    let prepared = match first.prepare(command).unwrap() {
        quotick::auction_engine::CallAuctionCommandPreparation::Ready(value) => value,
        quotick::auction_engine::CallAuctionCommandPreparation::Replay(_) => unreachable!(),
    };
    let committed = first.submit(command).unwrap();
    let replay = first.commit(prepared).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.command_sequence, committed.command_sequence);
    assert!(replay.events.shares_storage_with(&committed.events));
    assert_eq!(first.resource_status().retained_events, 2);
    first.validate().unwrap();
}

#[test]
fn protected_event_history_admits_only_valid_terminal_actions() {
    let book = book_limits(2, 4);
    let limits = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: 12,
        terminal_command_reserve: 4,
        max_retained_events: 9,
        max_report_events: 5,
    })
    .unwrap();
    let mut engine = CallAuctionEngine::try_with_limits(definition(), limits).unwrap();
    let allocated = engine.resource_status().allocated_retained_events;

    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    let status = engine.resource_status();
    assert_eq!(status.retained_events, 3);
    assert_eq!(status.ordinary_event_capacity, 3);
    assert_eq!(status.terminal_event_reserve, 6);

    assert!(matches!(
        engine.submit(cancel_command(4, 99, 1)),
        Err(CallAuctionEngineError::CapacityExhausted(
            CallAuctionEngineCapacity::AdmissionEventHistory
        ))
    ));
    assert_eq!(engine.resource_status().retained_events, 3);

    engine
        .submit(phase_command(5, 1, 1, CallAuctionPhase::Frozen))
        .unwrap();
    let uncross = uncross_command(&engine, 6, 1, 2, CallAuctionRemainderPolicy::CancelAll);
    let report = engine.submit(uncross).unwrap();
    assert_eq!(report.events.len(), 2);
    assert_eq!(engine.resource_status().retained_events, 6);
    assert_eq!(
        engine.resource_status().allocated_retained_events,
        allocated
    );

    let replay = engine.submit(uncross).unwrap();
    assert!(replay.replayed);
    assert!(replay.events.shares_storage_with(&report.events));
    assert_eq!(engine.resource_status().retained_events, 6);
    engine.validate().unwrap();
}

#[test]
fn total_event_exhaustion_preserves_exact_retry_after_checkpoint_rebinding() {
    let source_limits = engine_limits(2, 2, 20);
    let mut source = CallAuctionEngine::try_with_limits(definition(), source_limits).unwrap();
    let first = submit_command_at(
        1,
        1,
        0,
        order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 1),
    );
    for command_id in 1..=9 {
        let command = submit_command_at(
            command_id,
            1,
            0,
            order(command_id, 1, Side::Buy, AuctionOrderConstraint::Market, 1),
        );
        source.submit(command).unwrap();
    }
    let checkpoint = source.checkpoint(1, 19).unwrap();
    let book = book_limits(2, 2);
    let selected = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: 14,
        terminal_command_reserve: 4,
        max_retained_events: 9,
        max_report_events: 5,
    })
    .unwrap();
    let mut restored =
        CallAuctionEngine::from_checkpoint_with_limits(&checkpoint, selected).unwrap();
    assert_eq!(restored.resource_status().retained_events, 9);
    restored.validate().unwrap();

    let replay = restored.submit(first).unwrap();
    assert!(replay.replayed);
    assert_eq!(restored.resource_status().retained_events, 9);
    assert!(matches!(
        restored.submit(submit_command_at(
            10,
            1,
            0,
            order(10, 1, Side::Buy, AuctionOrderConstraint::Market, 1),
        )),
        Err(CallAuctionEngineError::CapacityExhausted(
            CallAuctionEngineCapacity::RetainedEvents
        ))
    ));
    assert_eq!(restored.resource_status().retained_events, 9);
}

#[test]
fn uncross_pool_exhaustion_is_unsequenced_and_drop_releases_the_lease() {
    let book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 4,
        max_prepared_uncrosses: 1,
    })
    .unwrap();
    let limits = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: 12,
        terminal_command_reserve: 4,
        max_retained_events: 18,
        max_report_events: 5,
    })
    .unwrap();
    let mut engine = CallAuctionEngine::try_with_limits(definition(), limits).unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    engine
        .submit(phase_command(4, 1, 1, CallAuctionPhase::Frozen))
        .unwrap();

    let first_command = uncross_command(&engine, 5, 1, 2, CallAuctionRemainderPolicy::CancelAll);
    let quotick::auction_engine::CallAuctionCommandPreparation::Ready(first) =
        engine.prepare(first_command).unwrap()
    else {
        unreachable!();
    };
    assert_eq!(
        engine.book().resource_status().available_prepared_uncrosses,
        0
    );
    let second_command = uncross_command(&engine, 6, 1, 2, CallAuctionRemainderPolicy::CancelAll);
    assert!(matches!(
        engine.prepare(second_command),
        Err(CallAuctionEngineError::UncrossPreparation(
            CallAuctionPrepareError::PreparationCapacityExhausted { maximum: 1 }
        ))
    ));
    assert_eq!(engine.resource_status().retained_commands, 4);
    assert_eq!(engine.resource_status().retained_events, 4);

    drop(first);
    assert_eq!(
        engine.book().resource_status().available_prepared_uncrosses,
        1
    );
    let quotick::auction_engine::CallAuctionCommandPreparation::Ready(second) =
        engine.prepare(second_command).unwrap()
    else {
        unreachable!();
    };
    engine.commit(second).unwrap();
    assert_eq!(
        engine.book().resource_status().available_prepared_uncrosses,
        1
    );
    engine.validate().unwrap();
}

#[test]
fn unrepresentable_event_arena_is_a_typed_constructor_failure() {
    let book = book_limits(1, 1);
    let limits = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: 4,
        terminal_command_reserve: 3,
        max_retained_events: usize::MAX,
        max_report_events: 3,
    })
    .unwrap();
    assert!(matches!(
        CallAuctionEngine::try_with_limits(definition(), limits),
        Err(CallAuctionEngineConstructionError::EventArenaReservationFailed)
    ));
}

#[test]
fn empty_uncross_is_rejected_and_frozen_cancellation_enables_explicit_close() {
    let mut engine =
        CallAuctionEngine::try_with_limits(definition(), engine_limits(4, 8, 16)).unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 3),
        ))
        .unwrap();
    engine
        .submit(phase_command(3, 1, 1, CallAuctionPhase::Frozen))
        .unwrap();
    let rejected = engine
        .submit(uncross_command(
            &engine,
            4,
            1,
            2,
            CallAuctionRemainderPolicy::CancelAll,
        ))
        .unwrap();
    assert_eq!(
        rejected.outcome,
        CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::NoExecutableInterest)
    );
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Frozen);
    engine.submit(cancel_command(5, 1, 1)).unwrap();
    engine
        .submit(phase_command(6, 1, 2, CallAuctionPhase::Closed))
        .unwrap();
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Closed);
    assert_eq!(engine.phase_snapshot().revision(), 3);
    engine.validate().unwrap();
}

#[test]
fn phase_controller_matches_literal_model_for_ten_thousand_commands() {
    let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
    let mut phase = CallAuctionPhase::Closed;
    let mut revision = 0_u64;
    let mut active: Option<AuctionId> = None;
    let mut last = 0_u64;
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;

    for command_index in 1_u64..=10_000 {
        state ^= state << 7;
        state ^= state >> 9;
        state ^= state << 8;
        let target = match state % 3 {
            0 => CallAuctionPhase::Closed,
            1 => CallAuctionPhase::Collecting,
            _ => CallAuctionPhase::Frozen,
        };
        let expected_revision = if state & 8 == 0 {
            revision
        } else {
            revision.wrapping_add(1)
        };
        let supplied_id = if phase == CallAuctionPhase::Closed {
            last + 1 + u64::from(state & 16 != 0)
        } else {
            active.map_or(1, AuctionId::get) + u64::from(state & 16 != 0)
        };
        let command = phase_command(command_index, supplied_id, expected_revision, target);

        let expected_accept = if expected_revision == revision {
            match (phase, target) {
                (CallAuctionPhase::Closed, CallAuctionPhase::Collecting) => supplied_id == last + 1,
                (CallAuctionPhase::Collecting, CallAuctionPhase::Frozen)
                | (CallAuctionPhase::Frozen, CallAuctionPhase::Collecting)
                | (
                    CallAuctionPhase::Collecting | CallAuctionPhase::Frozen,
                    CallAuctionPhase::Closed,
                ) => Some(auction(supplied_id)) == active,
                _ => false,
            }
        } else {
            false
        };
        let report = engine.submit(command).unwrap();
        assert_eq!(
            report.outcome == CallAuctionCommandOutcome::Accepted,
            expected_accept,
            "command {command_index}"
        );
        assert_eq!(report.command_sequence, command_index);
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.events[0].sequence, command_index);
        if expected_accept {
            revision += 1;
            if phase == CallAuctionPhase::Closed {
                last = supplied_id;
                active = Some(auction(supplied_id));
            } else if target == CallAuctionPhase::Closed {
                active = None;
            }
            phase = target;
        }
        assert_eq!(engine.phase_snapshot().phase(), phase);
        assert_eq!(engine.phase_snapshot().revision(), revision);
        assert_eq!(engine.phase_snapshot().active_auction_id(), active);
    }
    engine.validate().unwrap();
}

#[test]
fn engine_limits_derive_terminal_and_report_bounds() {
    let book = book_limits(2, 4);
    let make = |retained, reserve, retained_events, report_events| {
        CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
            book,
            max_retained_commands: retained,
            terminal_command_reserve: reserve,
            max_retained_events: retained_events,
            max_report_events: report_events,
        })
    };
    assert_eq!(
        make(7, 3, 9, 5),
        Err(CallAuctionEngineLimitsError::TerminalReserveBelowRequired {
            configured: 3,
            required: 4,
        })
    );
    assert_eq!(
        make(4, 4, 9, 5),
        Err(CallAuctionEngineLimitsError::TerminalReserveExhaustsHistory)
    );
    assert_eq!(
        make(7, 4, 9, 4),
        Err(CallAuctionEngineLimitsError::ReportEventsBelowRequired {
            configured: 4,
            required: 5,
        })
    );
    assert_eq!(
        make(7, 4, 0, 5),
        Err(CallAuctionEngineLimitsError::ZeroRetainedEvents)
    );
    assert_eq!(
        make(7, 4, 8, 5),
        Err(CallAuctionEngineLimitsError::RetainedEventsBelowRequired {
            configured: 8,
            required: 9,
        })
    );
    assert!(make(7, 4, 9, 5).is_ok());
}
