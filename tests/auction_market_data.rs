use quotick::auction::{AuctionOrderConstraint, AuctionPricePolicy};
use quotick::auction_book::{
    CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder, CallAuctionRemainderPolicy,
    CallAuctionSelfTradePolicy, CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionCancelOrder, CallAuctionCommand, CallAuctionEngine, CallAuctionEngineLimits,
    CallAuctionEngineLimitsSpec, CallAuctionPhase, CallAuctionPhaseControl, CallAuctionSubmitOrder,
    CallAuctionUncrossCommand,
};
use quotick::auction_market_data::{
    CallAuctionMarketDataBatch, CallAuctionMarketDataError, CallAuctionMarketDataKind,
    CallAuctionMarketDataPublisher, CallAuctionMarketDataReplica, CallAuctionMarketDataSnapshot,
    CallAuctionMarketDataUpdate,
};
use quotick::codec::BinaryCodec;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
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
    })
    .unwrap();
    let limits = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: 64,
        terminal_command_reserve: 18,
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
    CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(auction_id),
        expected_phase_revision: phase_revision,
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

fn assert_mirrors(engine: &CallAuctionEngine, replica: &CallAuctionMarketDataReplica) {
    assert_eq!(engine.phase_snapshot(), replica.phase());
    assert_eq!(engine.book().state_revision(), replica.book_revision());
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
    assert_eq!(genesis.len(), 112);

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
