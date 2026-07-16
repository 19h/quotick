use quotick::auction::{AuctionAllocationPolicy, AuctionOrderConstraint, AuctionPricePolicy};
use quotick::auction_book::{
    CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder, CallAuctionRemainderPolicy,
    CallAuctionSelfTradePolicy, CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionCommand, CallAuctionEngine, CallAuctionEngineLimits, CallAuctionEngineLimitsSpec,
    CallAuctionPhase, CallAuctionPhaseControl, CallAuctionSubmitOrder, CallAuctionUncrossCommand,
};
use quotick::auction_market_data::{
    CallAuctionMarketDataError, CallAuctionMarketDataPublisher, CallAuctionMarketDataReplayBuffer,
    CallAuctionMarketDataReplayError, CallAuctionMarketDataReplica,
};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::{
    AccountId, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, Side, TimestampNs,
};

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(91).unwrap(),
        version: InstrumentVersion::new(4).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("AUCT-REPLAY").unwrap(),
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

fn engine(definition: InstrumentDefinition) -> CallAuctionEngine {
    let book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: 8,
        max_price_levels_per_side: 8,
        max_accepted_order_ids: 16,
        max_prepared_uncrosses: 1,
    })
    .unwrap();
    let limits = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: 16,
        terminal_command_reserve: 10,
        max_retained_events: 27,
        max_report_events: 17,
    })
    .unwrap();
    CallAuctionEngine::try_with_limits(definition, limits).unwrap()
}

fn phase(definition: InstrumentDefinition) -> CallAuctionCommand {
    phase_control(definition, 1, 1, 0, CallAuctionPhase::Collecting)
}

fn phase_control(
    definition: InstrumentDefinition,
    command_id: u64,
    auction_id: u64,
    expected_phase_revision: u64,
    target_phase: CallAuctionPhase,
) -> CallAuctionCommand {
    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        auction_id: AuctionId::new(auction_id).unwrap(),
        expected_phase_revision,
        target_phase,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn uncross(definition: InstrumentDefinition, engine: &CallAuctionEngine) -> CallAuctionCommand {
    CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
        command_id: CommandId::new(5).unwrap(),
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        auction_id: AuctionId::new(1).unwrap(),
        expected_phase_revision: 2,
        price_band: engine.book().instrument_price_band(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        uncross_policy: CallAuctionUncrossPolicy::new(
            AuctionAllocationPolicy::PriceTime,
            CallAuctionRemainderPolicy::RetainAll,
            CallAuctionSelfTradePolicy::Permit,
        ),
        received_at: TimestampNs::from_unix_nanos(5),
    })
}

fn publish(
    engine: &mut CallAuctionEngine,
    publisher: &mut CallAuctionMarketDataPublisher,
    command: CallAuctionCommand,
) -> quotick::auction_market_data::CallAuctionMarketDataBatch {
    let report = engine.submit(command).unwrap();
    publisher.publish(command, &report, engine).unwrap()
}

fn order(
    definition: InstrumentDefinition,
    command_id: u64,
    order_id: u64,
    side: Side,
    price: i64,
) -> CallAuctionCommand {
    CallAuctionCommand::Submit(CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: AuctionId::new(1).unwrap(),
        expected_phase_revision: 1,
        order: CallAuctionOrder::new(
            OrderId::new(order_id).unwrap(),
            AccountId::new(order_id).unwrap(),
            definition.instrument_id(),
            definition.version(),
            side,
            AuctionOrderConstraint::Limit(Price::from_raw(price)),
            Quantity::new(order_id).unwrap(),
            quotick::auction::AuctionPriorityClass::HIGHEST,
        ),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

#[test]
fn retained_batches_repair_a_gap_without_losing_command_boundaries() {
    let definition = definition();
    let mut engine = engine(definition);
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let initial = publisher.snapshot();
    let mut batches = Vec::new();
    for command in [
        phase(definition),
        order(definition, 2, 1, Side::Buy, 100),
        order(definition, 3, 2, Side::Sell, 101),
        order(definition, 4, 3, Side::Buy, 99),
    ] {
        let report = engine.submit(command).unwrap();
        batches.push(publisher.publish(command, &report, &engine).unwrap());
    }

    let mut replay = CallAuctionMarketDataReplayBuffer::try_new(
        definition.instrument_id(),
        definition.version(),
        initial.as_of_sequence(),
        3,
    )
    .unwrap();
    let allocation_capacity = replay.status().allocation_capacity;
    for batch in &batches {
        assert_eq!(replay.push_batch(batch).unwrap(), 1);
    }
    assert_eq!(replay.status().allocation_capacity, allocation_capacity);
    assert_eq!(replay.push_batch(&batches[3]).unwrap(), 0);
    assert_eq!(replay.earliest_sequence(), Some(2));

    let mut replica = CallAuctionMarketDataReplica::new(definition);
    replica.apply_snapshot(&initial).unwrap();
    replica.apply_batch(&batches[0]).unwrap();
    assert_eq!(
        replica.apply_batch(&batches[2]),
        Err(CallAuctionMarketDataError::SequenceGap {
            expected: 2,
            actual: 3,
        })
    );

    let first_page = replay
        .replay_batches_after(replica.last_sequence(), 2)
        .unwrap();
    assert_eq!(first_page.first_sequence(), Some(2));
    assert_eq!(first_page.last_sequence(), Some(3));
    assert!(first_page.has_more());
    for batch in first_page {
        replica.apply_replay_batch(&batch).unwrap();
    }
    assert_eq!(replica.last_command_sequence(), 3);

    let second_page = replay
        .replay_batches_after(replica.last_sequence(), 1)
        .unwrap();
    assert_eq!(second_page.len(), 1);
    assert!(!second_page.has_more());
    for batch in second_page {
        replica.apply_replay_batch(&batch).unwrap();
    }

    assert_eq!(replica.last_sequence(), publisher.last_sequence());
    assert_eq!(
        replica.last_command_sequence(),
        publisher.last_command_sequence()
    );
    assert_eq!(replica.phase(), engine.phase_snapshot());
    assert_eq!(replica.book_revision(), engine.book().state_revision());
    assert_eq!(
        replica.limit_depth(Side::Buy, usize::MAX),
        engine.book().limit_depth(Side::Buy, usize::MAX)
    );
    assert_eq!(
        replica.limit_depth(Side::Sell, usize::MAX),
        engine.book().limit_depth(Side::Sell, usize::MAX)
    );
    replay.validate().unwrap();
    replica.validate().unwrap();
}

#[test]
fn replay_pages_never_split_uncross_batches_and_eviction_is_explicit() {
    let definition = definition();
    let mut engine = engine(definition);
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    for command in [
        phase(definition),
        order(definition, 2, 1, Side::Buy, 101),
        order(definition, 3, 2, Side::Sell, 99),
        phase_control(definition, 4, 1, 1, CallAuctionPhase::Frozen),
    ] {
        publish(&mut engine, &mut publisher, command);
    }
    let before_uncross = publisher.snapshot();
    let uncross_command = uncross(definition, &engine);
    let uncross_batch = publish(&mut engine, &mut publisher, uncross_command);
    let batch_updates = uncross_batch.updates().len();
    assert!(batch_updates > 1);

    let mut replay = CallAuctionMarketDataReplayBuffer::try_new(
        definition.instrument_id(),
        definition.version(),
        before_uncross.as_of_sequence(),
        batch_updates,
    )
    .unwrap();
    assert_eq!(replay.push_batch(&uncross_batch).unwrap(), batch_updates);
    assert_eq!(replay.push_batch(&uncross_batch).unwrap(), 0);
    assert_eq!(
        replay.replay_batches_after(before_uncross.as_of_sequence(), batch_updates - 1),
        Err(CallAuctionMarketDataReplayError::BatchExceedsRequestLimit {
            maximum: batch_updates - 1,
            required: batch_updates,
        })
    );
    assert_eq!(
        replay.replay_batches_after(uncross_batch.first_sequence().unwrap(), batch_updates),
        Err(CallAuctionMarketDataReplayError::CursorInsideBatch {
            requested_after: uncross_batch.first_sequence().unwrap(),
        })
    );

    let page = replay
        .replay_batches_after(before_uncross.as_of_sequence(), batch_updates)
        .unwrap();
    assert_eq!(page.len(), 1);
    assert!(!page.has_more());
    let mut replayed_batch = page.into_iter().next().unwrap();
    assert_eq!(replayed_batch.len(), batch_updates);
    assert_eq!(
        replayed_batch.first_sequence(),
        uncross_batch.first_sequence().unwrap()
    );
    assert_eq!(
        replayed_batch.last_sequence(),
        uncross_batch.last_sequence().unwrap()
    );
    assert_eq!(
        replayed_batch.next().unwrap().sequence(),
        uncross_batch.first_sequence().unwrap()
    );
    assert_eq!(replayed_batch.len(), batch_updates - 1);
    let mut replica = CallAuctionMarketDataReplica::new(definition);
    replica.apply_snapshot(&before_uncross).unwrap();
    replica.apply_replay_batch(&replayed_batch).unwrap();
    assert_eq!(replica.last_sequence(), publisher.last_sequence());
    assert_eq!(
        replica.last_command_sequence(),
        publisher.last_command_sequence()
    );

    let next_batch = publish(
        &mut engine,
        &mut publisher,
        phase_control(definition, 6, 2, 3, CallAuctionPhase::Collecting),
    );
    replay.push_batch(&next_batch).unwrap();
    assert_eq!(
        replay.replay_batches_after(before_uncross.as_of_sequence(), batch_updates),
        Err(CallAuctionMarketDataReplayError::Unavailable {
            earliest_available: next_batch.first_sequence(),
            requested_sequence: uncross_batch.first_sequence().unwrap(),
        })
    );
    replay.validate().unwrap();
}
