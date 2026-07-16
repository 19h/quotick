use quotick::auction::{AuctionAllocationPolicy, AuctionOrderConstraint, AuctionPricePolicy};
use quotick::auction_book::{
    CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder, CallAuctionRemainderPolicy,
    CallAuctionSelfTradePolicy, CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionAmendOrder, CallAuctionCancelOrder, CallAuctionCommand, CallAuctionEngine,
    CallAuctionEngineLimits, CallAuctionEngineLimitsSpec, CallAuctionIndicativeCommand,
    CallAuctionMassCancel, CallAuctionPhase, CallAuctionPhaseControl, CallAuctionReplaceOrder,
    CallAuctionSubmitOrder, CallAuctionUncrossCommand,
};
use quotick::auction_market_data::{
    CallAuctionBookChangeReason, CallAuctionMarketDataBatch, CallAuctionMarketDataError,
    CallAuctionMarketDataKind, CallAuctionMarketDataPublisher, CallAuctionMarketDataReplica,
    CallAuctionMarketDataSnapshot, CallAuctionMarketDataUpdate,
};
use quotick::codec::BinaryCodec;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::MassCancelScope;
use quotick::{
    AccountId, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, Side, TimestampNs,
};

fn instrument() -> InstrumentId {
    InstrumentId::new(81).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(3).unwrap()
}

fn auction(value: u64) -> AuctionId {
    AuctionId::new(value).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("AUCT-MD").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 5, Price::from_raw(0), Price::from_raw(200)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Halted,
    })
    .unwrap()
}

fn engine() -> CallAuctionEngine {
    let book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: 16,
        max_price_levels_per_side: 16,
        max_accepted_order_ids: 64,
        max_prepared_uncrosses: 2,
    })
    .unwrap();
    let limits = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: 64,
        terminal_command_reserve: 18,
        max_retained_events: 98,
        max_report_events: 33,
    })
    .unwrap();
    CallAuctionEngine::try_with_limits(definition(), limits).unwrap()
}

fn phase(
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

#[allow(
    clippy::too_many_arguments,
    reason = "test command factory keeps every auction identity dimension explicit"
)]
fn submit(
    command_id: u64,
    auction_id: u64,
    phase_revision: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u64,
) -> CallAuctionCommand {
    submit_with_class(
        command_id,
        auction_id,
        phase_revision,
        order_id,
        account_id,
        side,
        constraint,
        quantity,
        0,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "test command factory keeps every auction identity dimension explicit"
)]
fn submit_with_class(
    command_id: u64,
    auction_id: u64,
    phase_revision: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u64,
    priority_class: u16,
) -> CallAuctionCommand {
    CallAuctionCommand::Submit(CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(auction_id),
        expected_phase_revision: phase_revision,
        order: CallAuctionOrder::new(
            OrderId::new(order_id).unwrap(),
            AccountId::new(account_id).unwrap(),
            instrument(),
            version(),
            side,
            constraint,
            Quantity::new(quantity).unwrap(),
            quotick::auction::AuctionPriorityClass::new(priority_class),
        ),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn uncross(
    engine: &CallAuctionEngine,
    command_id: u64,
    auction_id: u64,
    phase_revision: u64,
    remainder: CallAuctionRemainderPolicy,
) -> CallAuctionCommand {
    uncross_with_allocation(
        engine,
        command_id,
        auction_id,
        phase_revision,
        AuctionAllocationPolicy::PriceTime,
        remainder,
    )
}

fn indicative(
    engine: &CallAuctionEngine,
    command_id: u64,
    auction_id: u64,
    phase_revision: u64,
) -> CallAuctionCommand {
    CallAuctionCommand::Indicative(CallAuctionIndicativeCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(auction_id),
        expected_phase_revision: phase_revision,
        price_band: engine.book().instrument_price_band(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn uncross_with_allocation(
    engine: &CallAuctionEngine,
    command_id: u64,
    auction_id: u64,
    phase_revision: u64,
    allocation: AuctionAllocationPolicy,
    remainder: CallAuctionRemainderPolicy,
) -> CallAuctionCommand {
    uncross_with_policy(
        engine,
        command_id,
        auction_id,
        phase_revision,
        allocation,
        remainder,
        CallAuctionSelfTradePolicy::Permit,
    )
}

#[allow(
    clippy::too_many_arguments,
    reason = "test command factory keeps every uncross policy dimension explicit"
)]
fn uncross_with_policy(
    engine: &CallAuctionEngine,
    command_id: u64,
    auction_id: u64,
    phase_revision: u64,
    allocation: AuctionAllocationPolicy,
    remainder: CallAuctionRemainderPolicy,
    self_trade: CallAuctionSelfTradePolicy,
) -> CallAuctionCommand {
    CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(auction_id),
        expected_phase_revision: phase_revision,
        price_band: engine.book().instrument_price_band(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        uncross_policy: CallAuctionUncrossPolicy::new(allocation, remainder, self_trade),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn cancel(command_id: u64, account_id: u64, order_id: u64) -> CallAuctionCommand {
    CallAuctionCommand::Cancel(CallAuctionCancelOrder {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        account_id: AccountId::new(account_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn mass_cancel(command_id: u64, account_id: u64, scope: MassCancelScope) -> CallAuctionCommand {
    CallAuctionCommand::MassCancel(CallAuctionMassCancel {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        account_id: AccountId::new(account_id).unwrap(),
        scope,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn amend(
    command_id: u64,
    auction_id: u64,
    phase_revision: u64,
    account_id: u64,
    order_id: u64,
    quantity: u64,
) -> CallAuctionCommand {
    CallAuctionCommand::Amend(CallAuctionAmendOrder {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(auction_id),
        expected_phase_revision: phase_revision,
        account_id: AccountId::new(account_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        new_quantity: Quantity::new(quantity).unwrap(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "test command factory keeps target and replacement identity explicit"
)]
fn replace(
    command_id: u64,
    auction_id: u64,
    phase_revision: u64,
    target_order_id: u64,
    account_id: u64,
    replacement_order_id: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u64,
) -> CallAuctionCommand {
    CallAuctionCommand::Replace(CallAuctionReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(auction_id),
        expected_phase_revision: phase_revision,
        account_id: AccountId::new(account_id).unwrap(),
        target_order_id: OrderId::new(target_order_id).unwrap(),
        replacement: CallAuctionOrder::new(
            OrderId::new(replacement_order_id).unwrap(),
            AccountId::new(account_id).unwrap(),
            instrument(),
            version(),
            side,
            constraint,
            Quantity::new(quantity).unwrap(),
            quotick::auction::AuctionPriorityClass::HIGHEST,
        ),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn assert_mirrors(engine: &CallAuctionEngine, replica: &CallAuctionMarketDataReplica) {
    assert_eq!(engine.phase_snapshot(), replica.phase());
    assert_eq!(engine.book().state_revision(), replica.book_revision());
    assert_eq!(engine.last_indicative(), replica.indicative());
    assert_eq!(
        engine.book().market_quantity(Side::Buy),
        replica.market_quantity(Side::Buy)
    );
    assert_eq!(
        engine.book().market_quantity(Side::Sell),
        replica.market_quantity(Side::Sell)
    );
    assert_eq!(
        engine.book().market_order_count(Side::Buy),
        replica.market_order_count(Side::Buy)
    );
    assert_eq!(
        engine.book().market_order_count(Side::Sell),
        replica.market_order_count(Side::Sell)
    );
    assert_eq!(
        engine.book().limit_depth(Side::Buy, usize::MAX),
        replica.limit_depth(Side::Buy, usize::MAX)
    );
    assert_eq!(
        engine.book().limit_depth(Side::Sell, usize::MAX),
        replica.limit_depth(Side::Sell, usize::MAX)
    );
}

fn populate_replica_limit_depth(
    engine: &mut CallAuctionEngine,
    publisher: &mut CallAuctionMarketDataPublisher,
    replica: &mut CallAuctionMarketDataReplica,
) {
    execute(
        engine,
        publisher,
        replica,
        phase(1, 1, 0, CallAuctionPhase::Collecting),
    );
    for (index, (side, constraint)) in [
        (
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(80)),
        ),
        (
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
        ),
        (
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(90)),
        ),
        (
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(140)),
        ),
        (
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(120)),
        ),
        (
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(130)),
        ),
        (Side::Buy, AuctionOrderConstraint::Market),
    ]
    .into_iter()
    .enumerate()
    {
        let identity = u64::try_from(index).unwrap() + 2;
        execute(
            engine,
            publisher,
            replica,
            submit(
                identity, 1, 1, identity, identity, side, constraint, identity,
            ),
        );
    }
}

#[test]
fn replica_limit_depth_ranges_are_inclusive_ordered_and_market_exclusive() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica = CallAuctionMarketDataReplica::new(definition());
    replica.apply_snapshot(&publisher.snapshot()).unwrap();
    populate_replica_limit_depth(&mut engine, &mut publisher, &mut replica);

    assert_eq!(replica.market_order_count(Side::Buy), 1);
    assert_eq!(
        replica
            .limit_depth_iter(Side::Buy)
            .map(|level| level.price().raw())
            .collect::<Vec<_>>(),
        [100, 90, 80]
    );
    assert_eq!(replica.limit_depth_iter(Side::Buy).len(), 3);
    assert_eq!(
        replica
            .limit_depth_range_iter(Side::Buy, Price::from_raw(90)..=Price::from_raw(100),)
            .map(|level| level.price().raw())
            .collect::<Vec<_>>(),
        [100, 90]
    );
    assert_eq!(
        replica
            .limit_depth_range_iter(Side::Buy, Price::from_raw(90)..=Price::from_raw(100),)
            .rev()
            .map(|level| level.price().raw())
            .collect::<Vec<_>>(),
        [90, 100]
    );
    assert_eq!(
        replica
            .try_limit_depth_range(Side::Buy, Price::from_raw(90)..=Price::from_raw(100), 1,)
            .unwrap()
            .iter()
            .map(|level| level.price().raw())
            .collect::<Vec<_>>(),
        [100]
    );
    assert_eq!(
        replica.limit_depth_range(
            Side::Sell,
            Price::from_raw(120)..=Price::from_raw(130),
            usize::MAX,
        ),
        engine.book().limit_depth_range(
            Side::Sell,
            Price::from_raw(120)..=Price::from_raw(130),
            usize::MAX,
        )
    );
    assert!(
        replica
            .limit_depth_range_iter(Side::Sell, Price::from_raw(135)..=Price::from_raw(115),)
            .next()
            .is_none()
    );
    assert!(
        replica
            .try_limit_depth_range(Side::Buy, Price::from_raw(90)..=Price::from_raw(100), 0,)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        replica.limit_depth_iter(Side::Sell).collect::<Vec<_>>(),
        engine
            .book()
            .limit_depth_iter(Side::Sell)
            .collect::<Vec<_>>()
    );
    replica.validate().unwrap();
}

#[test]
fn indicative_updates_round_trip_replay_snapshot_and_invalidate_on_book_change() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica = CallAuctionMarketDataReplica::new(definition());
    let genesis = publisher.snapshot();
    replica.apply_snapshot(&genesis).unwrap();

    let phase_batch = execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(1, 1, 0, CallAuctionPhase::Collecting),
    );
    let empty_command = indicative(&engine, 2, 1, 1);
    let empty = execute(&mut engine, &mut publisher, &mut replica, empty_command);
    let CallAuctionMarketDataKind::Indicative(empty_state) = empty.updates()[0].kind() else {
        panic!("indicative command must publish one indicative update");
    };
    assert_eq!(empty_state.clearing(), None);
    assert_eq!(replica.indicative(), Some(empty_state));

    let encoded = empty.updates()[0].encode().unwrap();
    assert_eq!(encoded.len(), 84);
    assert_eq!(encoded[32], 6);
    assert_eq!(
        CallAuctionMarketDataUpdate::decode(&encoded).unwrap(),
        empty.updates()[0]
    );

    let retry = engine.submit(empty_command).unwrap();
    let retry_batch = publisher.publish(empty_command, &retry, &engine).unwrap();
    assert!(retry_batch.replayed());
    assert!(retry_batch.updates().is_empty());
    assert_eq!(replica.indicative(), Some(empty_state));

    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        submit(
            3,
            1,
            1,
            1,
            7,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            5,
        ),
    );
    assert_eq!(engine.last_indicative(), None);
    assert_eq!(replica.indicative(), None);

    let current_command = indicative(&engine, 4, 1, 1);
    let current = execute(&mut engine, &mut publisher, &mut replica, current_command);
    let CallAuctionMarketDataKind::Indicative(current_state) = current.updates()[0].kind() else {
        panic!("indicative command must publish one indicative update");
    };
    assert_eq!(current_state.clearing(), None);

    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        submit(
            5,
            1,
            1,
            2,
            8,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            3,
        ),
    );
    assert_eq!(engine.last_indicative(), None);
    assert_eq!(replica.indicative(), None);

    let crossed_command = indicative(&engine, 6, 1, 1);
    let crossed = execute(&mut engine, &mut publisher, &mut replica, crossed_command);
    let CallAuctionMarketDataKind::Indicative(crossed_state) = crossed.updates()[0].kind() else {
        panic!("crossed book must publish one indicative update");
    };
    assert_eq!(crossed_state.clearing().unwrap().executable_quantity(), 3);
    let crossed_bytes = crossed.updates()[0].encode().unwrap();
    assert_eq!(crossed_bytes.len(), 124);
    assert_eq!(
        CallAuctionMarketDataUpdate::decode(&crossed_bytes).unwrap(),
        crossed.updates()[0]
    );

    let snapshot = publisher.snapshot();
    assert_eq!(snapshot.indicative(), Some(crossed_state));
    let snapshot_bytes = snapshot.encode().unwrap();
    let decoded = CallAuctionMarketDataSnapshot::decode(&snapshot_bytes).unwrap();
    assert_eq!(decoded, snapshot);
    let mut repaired = CallAuctionMarketDataReplica::new(definition());
    repaired.apply_snapshot(&decoded).unwrap();
    assert_eq!(repaired.indicative(), Some(crossed_state));

    let mut replayed = CallAuctionMarketDataReplica::new(definition());
    replayed.apply_snapshot(&genesis).unwrap();
    replayed.apply_batch(&phase_batch).unwrap();
    replayed.apply_batch(&empty).unwrap();
    assert_eq!(replayed.indicative(), Some(empty_state));
}

fn execute(
    engine: &mut CallAuctionEngine,
    publisher: &mut CallAuctionMarketDataPublisher,
    replica: &mut CallAuctionMarketDataReplica,
    command: CallAuctionCommand,
) -> CallAuctionMarketDataBatch {
    let report = engine.submit(command).unwrap();
    let batch = publisher.publish(command, &report, engine).unwrap();
    for update in batch.updates() {
        let encoded = update.encode().unwrap();
        assert_eq!(
            *update,
            CallAuctionMarketDataUpdate::decode(&encoded).unwrap()
        );
    }
    replica.apply_batch(&batch).unwrap();
    assert_eq!(publisher.last_sequence(), replica.last_sequence());
    assert_eq!(
        publisher.last_command_sequence(),
        replica.last_command_sequence()
    );
    publisher.validate_against(engine).unwrap();
    assert_mirrors(engine, replica);
    batch
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one chronological integration test audits two complete auction cycles"
)]
fn crossed_depth_market_interest_uncross_replay_and_two_cycle_remainders_reconcile() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica = CallAuctionMarketDataReplica::new(definition());

    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(1, 1, 0, CallAuctionPhase::Collecting),
    );
    let market_buy = submit(2, 1, 1, 1, 11, Side::Buy, AuctionOrderConstraint::Market, 6);
    execute(&mut engine, &mut publisher, &mut replica, market_buy);
    let replayed = engine.submit(market_buy).unwrap();
    assert!(replayed.replayed);
    let replay_batch = publisher.publish(market_buy, &replayed, &engine).unwrap();
    assert!(replay_batch.replayed());
    assert!(replay_batch.updates().is_empty());
    replica.apply_batch(&replay_batch).unwrap();

    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        submit(
            3,
            1,
            1,
            2,
            12,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            4,
        ),
    );
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        submit(
            4,
            1,
            1,
            3,
            13,
            Side::Sell,
            AuctionOrderConstraint::Market,
            2,
        ),
    );
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        submit(
            5,
            1,
            1,
            4,
            14,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(95)),
            5,
        ),
    );
    assert_eq!(
        engine.book().best_limit_price(Side::Buy),
        Some(Price::from_raw(100))
    );
    assert_eq!(
        engine.book().best_limit_price(Side::Sell),
        Some(Price::from_raw(95))
    );

    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(6, 1, 1, CallAuctionPhase::Frozen),
    );
    let first_uncross = uncross(&engine, 7, 1, 2, CallAuctionRemainderPolicy::RetainAll);
    let batch = execute(&mut engine, &mut publisher, &mut replica, first_uncross);
    assert_eq!(
        batch
            .updates()
            .iter()
            .filter(|update| matches!(update.kind(), CallAuctionMarketDataKind::Trade { .. }))
            .count(),
        3
    );
    assert!(matches!(
        batch.updates().last().unwrap().kind(),
        CallAuctionMarketDataKind::UncrossCompleted {
            trade_count: 3,
            cancellation_count: 0,
            ..
        }
    ));
    assert_eq!(
        engine.book().limit_depth(Side::Buy, usize::MAX)[0].quantity(),
        3
    );

    let image = publisher.snapshot();
    let encoded = image.encode().unwrap();
    let decoded = CallAuctionMarketDataSnapshot::decode(&encoded).unwrap();
    assert_eq!(image, decoded);
    let mut repaired = CallAuctionMarketDataReplica::new(definition());
    repaired.apply_snapshot(&decoded).unwrap();
    assert_mirrors(&engine, &repaired);

    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(8, 2, 3, CallAuctionPhase::Collecting),
    );
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        submit(
            9,
            2,
            4,
            5,
            15,
            Side::Sell,
            AuctionOrderConstraint::Market,
            2,
        ),
    );
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(10, 2, 4, CallAuctionPhase::Frozen),
    );
    let second_uncross = uncross(&engine, 11, 2, 5, CallAuctionRemainderPolicy::CancelAll);
    let batch = execute(&mut engine, &mut publisher, &mut replica, second_uncross);
    assert!(matches!(
        batch.updates().last().unwrap().kind(),
        CallAuctionMarketDataKind::UncrossCompleted {
            trade_count: 1,
            cancellation_count: 1,
            ..
        }
    ));
    assert_eq!(engine.book().active_order_count(), 0);
}

#[test]
fn pro_rata_trade_trace_reconciles_public_depth_without_policy_disclosure() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica = CallAuctionMarketDataReplica::new(definition());
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(1, 1, 0, CallAuctionPhase::Collecting),
    );
    for command in [
        submit(
            2,
            1,
            1,
            1,
            1,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            10,
        ),
        submit(
            3,
            1,
            1,
            2,
            2,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            20,
        ),
        submit(
            4,
            1,
            1,
            3,
            3,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            15,
        ),
    ] {
        execute(&mut engine, &mut publisher, &mut replica, command);
    }
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(5, 1, 1, CallAuctionPhase::Frozen),
    );
    let command = uncross_with_allocation(
        &engine,
        6,
        1,
        2,
        AuctionAllocationPolicy::ProRataTime,
        CallAuctionRemainderPolicy::RetainAll,
    );

    let batch = execute(&mut engine, &mut publisher, &mut replica, command);

    assert_eq!(
        batch
            .updates()
            .iter()
            .filter_map(|update| match update.kind() {
                CallAuctionMarketDataKind::Trade { print, .. } => {
                    Some(print.quantity().lots())
                }
                _ => None,
            })
            .collect::<Vec<_>>(),
        vec![5, 10]
    );
    assert!(matches!(
        batch.updates().last().unwrap().kind(),
        CallAuctionMarketDataKind::UncrossCompleted {
            trade_count: 2,
            cancellation_count: 0,
            ..
        }
    ));
    assert_eq!(
        engine.book().limit_depth(Side::Buy, usize::MAX)[0].quantity(),
        15
    );
    assert!(engine.book().limit_depth(Side::Sell, usize::MAX).is_empty());
    assert_mirrors(&engine, &replica);
}

#[test]
fn snapshot_repairs_gaps_and_corrupt_absolute_transition_poisoning_is_fail_closed() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut source_replica = CallAuctionMarketDataReplica::new(definition());
    execute(
        &mut engine,
        &mut publisher,
        &mut source_replica,
        phase(1, 1, 0, CallAuctionPhase::Collecting),
    );
    let phase_snapshot = publisher.snapshot();
    let accepted = execute(
        &mut engine,
        &mut publisher,
        &mut source_replica,
        submit(
            2,
            1,
            1,
            1,
            1,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            4,
        ),
    );
    let update = accepted.updates()[0];

    let mut missing = CallAuctionMarketDataReplica::new(definition());
    assert_eq!(
        missing.apply(update),
        Err(CallAuctionMarketDataError::SequenceGap {
            expected: 1,
            actual: 2,
        })
    );
    assert!(!missing.is_poisoned());
    missing.apply_snapshot(&phase_snapshot).unwrap();

    let mut bytes = update.encode().unwrap();
    // Header (32), kind (1), reason (1), changed quantity (8), side (1),
    // limit tag (1), price (8), then the absolute u128 quantity.
    let absolute_quantity_offset = 52;
    bytes[absolute_quantity_offset] = 9;
    let corrupted = CallAuctionMarketDataUpdate::decode(&bytes).unwrap();
    assert_eq!(
        missing.apply(corrupted),
        Err(CallAuctionMarketDataError::InvalidUpdate(
            "auction book change does not reconcile to its quantity and absolute aggregate"
        ))
    );
    assert!(missing.is_poisoned());
    assert_eq!(
        missing.apply(update),
        Err(CallAuctionMarketDataError::Poisoned)
    );

    let current = publisher.snapshot();
    missing.apply_snapshot(&current).unwrap();
    assert!(!missing.is_poisoned());
    assert_mirrors(&engine, &missing);
    assert_eq!(
        missing.apply_snapshot(&phase_snapshot),
        Err(CallAuctionMarketDataError::StaleSnapshot {
            current: current.as_of_sequence(),
            snapshot: phase_snapshot.as_of_sequence(),
        })
    );
}

#[test]
fn rejection_user_cancel_and_live_bootstrap_preserve_public_boundaries() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica = CallAuctionMarketDataReplica::new(definition());

    let rejected = execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        submit(
            1,
            1,
            0,
            1,
            7,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(105)),
            9,
        ),
    );
    assert!(matches!(
        rejected.updates()[0].kind(),
        CallAuctionMarketDataKind::NoPublicChange
    ));

    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(2, 1, 0, CallAuctionPhase::Collecting),
    );
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        submit(
            3,
            1,
            1,
            2,
            7,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(105)),
            9,
        ),
    );

    let recovered_publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    assert_eq!(
        recovered_publisher.last_sequence(),
        publisher.last_sequence()
    );
    assert_eq!(recovered_publisher.snapshot(), publisher.snapshot());

    let cancelled = execute(&mut engine, &mut publisher, &mut replica, cancel(4, 7, 2));
    assert!(matches!(
        cancelled.updates()[0].kind(),
        CallAuctionMarketDataKind::Book {
            reason: quotick::auction_market_data::CallAuctionBookChangeReason::UserCancelled,
            quantity,
            level,
        } if quantity.lots() == 9 && level.quantity() == 0 && level.order_count() == 0
    ));
    assert_eq!(engine.book().active_order_count(), 0);
}

#[test]
fn aborted_self_trade_publishes_no_change_and_retry_publishes_nothing() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica = CallAuctionMarketDataReplica::new(definition());
    replica.apply_snapshot(&publisher.snapshot()).unwrap();
    for command in [
        phase(1, 1, 0, CallAuctionPhase::Collecting),
        submit(
            2,
            1,
            1,
            1,
            7,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            5,
        ),
        submit(
            3,
            1,
            1,
            2,
            7,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            5,
        ),
        phase(4, 1, 1, CallAuctionPhase::Frozen),
    ] {
        execute(&mut engine, &mut publisher, &mut replica, command);
    }
    let command = uncross_with_policy(
        &engine,
        5,
        1,
        2,
        AuctionAllocationPolicy::PriceTime,
        CallAuctionRemainderPolicy::RetainAll,
        CallAuctionSelfTradePolicy::Abort,
    );
    let rejected = execute(&mut engine, &mut publisher, &mut replica, command);

    assert_eq!(rejected.updates().len(), 1);
    assert_eq!(
        rejected.updates()[0].kind(),
        CallAuctionMarketDataKind::NoPublicChange
    );
    assert_eq!(engine.phase_snapshot().phase(), CallAuctionPhase::Frozen);
    assert_eq!(engine.book().active_order_count(), 2);
    assert_mirrors(&engine, &replica);

    let report = engine.submit(command).unwrap();
    let retry = publisher.publish(command, &report, &engine).unwrap();
    assert!(retry.replayed());
    assert!(retry.updates().is_empty());
    assert_mirrors(&engine, &replica);
}

#[test]
fn cancel_replace_projects_two_updates_but_one_command_boundary() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica = CallAuctionMarketDataReplica::new(definition());
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(1, 1, 0, CallAuctionPhase::Collecting),
    );
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        submit(
            2,
            1,
            1,
            1,
            7,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            5,
        ),
    );
    let pre_replace_snapshot = publisher.try_snapshot().unwrap();
    let mut split_replica = CallAuctionMarketDataReplica::new(definition());
    split_replica.apply_snapshot(&pre_replace_snapshot).unwrap();
    let batch = execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        replace(
            3,
            1,
            1,
            1,
            7,
            2,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(105)),
            3,
        ),
    );
    assert_eq!(batch.updates().len(), 2);
    assert!(matches!(
        batch.updates()[0].kind(),
        CallAuctionMarketDataKind::Book {
            reason: quotick::auction_market_data::CallAuctionBookChangeReason::Replaced,
            quantity,
            level,
        } if quantity.lots() == 5 && level.quantity() == 0 && level.order_count() == 0
    ));
    assert!(matches!(
        batch.updates()[1].kind(),
        CallAuctionMarketDataKind::Book {
            reason: quotick::auction_market_data::CallAuctionBookChangeReason::Accepted,
            quantity,
            level,
        } if quantity.lots() == 3 && level.quantity() == 3 && level.order_count() == 1
    ));
    assert_eq!(
        split_replica.apply(batch.updates()[0]),
        Err(CallAuctionMarketDataError::InvalidUpdate(
            "auction replacement removal requires a complete command batch"
        ))
    );
    assert_eq!(split_replica.last_sequence(), 2);
    assert_eq!(split_replica.book_revision(), 1);
    split_replica.validate().unwrap();
    assert_eq!(replica.last_command_sequence(), 3);
    assert_eq!(engine.book().best_limit_price(Side::Buy), None);
    assert_eq!(
        engine.book().best_limit_price(Side::Sell),
        Some(Price::from_raw(105))
    );
    assert_mirrors(&engine, &replica);
}

#[test]
fn amendment_projects_one_anonymized_delta_and_retained_aggregate_count() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica = CallAuctionMarketDataReplica::new(definition());
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(1, 1, 0, CallAuctionPhase::Collecting),
    );
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        submit_with_class(
            2,
            1,
            1,
            10,
            7,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            8,
            9,
        ),
    );
    let snapshot = publisher.snapshot();
    let mut single = CallAuctionMarketDataReplica::new(definition());
    single.apply_snapshot(&snapshot).unwrap();

    let batch = execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        amend(3, 1, 1, 7, 10, 3),
    );
    assert_eq!(batch.updates().len(), 1);
    let update = batch.updates()[0];
    assert!(matches!(
        update.kind(),
        CallAuctionMarketDataKind::Book {
            reason: CallAuctionBookChangeReason::Amended,
            quantity,
            level,
        } if quantity.lots() == 5
            && level.side() == Side::Buy
            && level.constraint()
                == AuctionOrderConstraint::Limit(Price::from_raw(100))
            && level.quantity() == 3
            && level.order_count() == 1
    ));
    let bytes = update.encode().unwrap();
    assert_eq!(CallAuctionMarketDataUpdate::decode(&bytes).unwrap(), update);
    assert_eq!(
        engine
            .book()
            .order(OrderId::new(10).unwrap())
            .unwrap()
            .priority_class,
        quotick::auction::AuctionPriorityClass::new(9)
    );
    single.apply(update).unwrap();
    assert_mirrors(&engine, &single);
    assert_mirrors(&engine, &replica);

    let mut count_drift = bytes;
    let count_offset = count_drift.len() - std::mem::size_of::<u64>();
    count_drift[count_offset..].copy_from_slice(&2_u64.to_le_bytes());
    let count_drift = CallAuctionMarketDataUpdate::decode(&count_drift).unwrap();
    let mut corrupted = CallAuctionMarketDataReplica::new(definition());
    corrupted.apply_snapshot(&snapshot).unwrap();
    assert!(matches!(
        corrupted.apply(count_drift),
        Err(CallAuctionMarketDataError::InvalidUpdate(
            "auction book change does not reconcile to its quantity and absolute aggregate"
        ))
    ));
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end batch scenario proves privacy, atomicity, empty selection, and closed-phase cancellation"
)]
fn mass_cancel_public_batch_is_atomic_anonymized_and_empty_safe() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica = CallAuctionMarketDataReplica::new(definition());
    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(1, 1, 0, CallAuctionPhase::Collecting),
    );
    for command in [
        submit(
            2,
            1,
            1,
            50,
            7,
            Side::Sell,
            AuctionOrderConstraint::Market,
            5,
        ),
        submit(
            3,
            1,
            1,
            10,
            7,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            3,
        ),
        submit(
            4,
            1,
            1,
            40,
            7,
            Side::Sell,
            AuctionOrderConstraint::Market,
            7,
        ),
        submit(
            5,
            1,
            1,
            30,
            8,
            Side::Sell,
            AuctionOrderConstraint::Market,
            11,
        ),
    ] {
        execute(&mut engine, &mut publisher, &mut replica, command);
    }
    let before = publisher.snapshot();
    let mut split = CallAuctionMarketDataReplica::new(definition());
    split.apply_snapshot(&before).unwrap();

    let batch = execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        mass_cancel(6, 7, MassCancelScope::Side(Side::Sell)),
    );
    assert_eq!(batch.updates().len(), 3);
    assert!(batch.updates()[..2].iter().all(|update| matches!(
        update.kind(),
        CallAuctionMarketDataKind::Book {
            reason: CallAuctionBookChangeReason::MassCancelled,
            ..
        }
    )));
    assert!(matches!(
        batch.updates()[2].kind(),
        CallAuctionMarketDataKind::MassCancelCompleted {
            cancelled_order_count: 2,
            cancelled_quantity_lots: 12,
            book_revision: 5,
        }
    ));
    assert_eq!(
        split.apply(batch.updates()[0]),
        Err(CallAuctionMarketDataError::InvalidUpdate(
            "auction mass cancellation requires a complete command batch"
        ))
    );
    assert_eq!(split.last_sequence(), before.as_of_sequence());
    assert_eq!(split.book_revision(), before.book_revision());
    split.apply_batch(&batch).unwrap();
    assert_mirrors(&engine, &split);

    execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        phase(7, 1, 1, CallAuctionPhase::Closed),
    );
    let empty = execute(
        &mut engine,
        &mut publisher,
        &mut replica,
        mass_cancel(8, 77, MassCancelScope::All),
    );
    assert_eq!(empty.updates().len(), 1);
    assert!(matches!(
        empty.updates()[0].kind(),
        CallAuctionMarketDataKind::MassCancelCompleted {
            cancelled_order_count: 0,
            cancelled_quantity_lots: 0,
            book_revision: 5,
        }
    ));

    let closed_cancel = execute(&mut engine, &mut publisher, &mut replica, cancel(9, 7, 10));
    assert!(matches!(
        closed_cancel.updates()[0].kind(),
        CallAuctionMarketDataKind::Book {
            reason: CallAuctionBookChangeReason::UserCancelled,
            ..
        }
    ));
    assert_mirrors(&engine, &replica);
}

#[test]
fn phase_update_and_genesis_snapshot_have_stable_little_endian_layouts() {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();

    let genesis = publisher.snapshot().encode().unwrap();
    let mut expected_genesis = Vec::new();
    expected_genesis.extend_from_slice(&81_u64.to_le_bytes());
    expected_genesis.extend_from_slice(&3_u64.to_le_bytes());
    expected_genesis.extend_from_slice(&0_u64.to_le_bytes());
    expected_genesis.extend_from_slice(&0_u64.to_le_bytes());
    expected_genesis.push(0); // Closed.
    expected_genesis.extend_from_slice(&0_u64.to_le_bytes());
    expected_genesis.push(0); // No active auction.
    expected_genesis.push(0); // No last auction.
    expected_genesis.extend_from_slice(&0_u64.to_le_bytes());
    expected_genesis.push(0); // No indicative state.
    expected_genesis.push(0); // No last trade.
    expected_genesis.push(0); // Buy side.
    expected_genesis.push(0); // Market constraint.
    expected_genesis.extend_from_slice(&0_u128.to_le_bytes());
    expected_genesis.extend_from_slice(&0_u64.to_le_bytes());
    expected_genesis.push(1); // Sell side.
    expected_genesis.push(0); // Market constraint.
    expected_genesis.extend_from_slice(&0_u128.to_le_bytes());
    expected_genesis.extend_from_slice(&0_u64.to_le_bytes());
    expected_genesis.extend_from_slice(&0_u32.to_le_bytes());
    expected_genesis.extend_from_slice(&0_u32.to_le_bytes());
    assert_eq!(genesis, expected_genesis);
    assert_eq!(genesis.len(), 113);

    let command = phase(1, 1, 0, CallAuctionPhase::Collecting);
    let report = engine.submit(command).unwrap();
    let update = publisher
        .publish(command, &report, &engine)
        .unwrap()
        .updates()[0];
    let encoded = update.encode().unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&81_u64.to_le_bytes());
    expected.extend_from_slice(&3_u64.to_le_bytes());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.push(3); // PhaseChanged.
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.push(0); // Closed.
    expected.push(1); // Collecting.
    expected.extend_from_slice(&1_u64.to_le_bytes());
    assert_eq!(encoded, expected);
    assert_eq!(encoded.len(), 51);

    let mut invalid_tag = encoded;
    invalid_tag[32] = u8::MAX;
    assert!(matches!(
        CallAuctionMarketDataUpdate::decode(&invalid_tag),
        Err(quotick::codec::CodecError::InvalidTag {
            type_name: "CallAuctionMarketDataKind",
            tag: u8::MAX,
        })
    ));
}
