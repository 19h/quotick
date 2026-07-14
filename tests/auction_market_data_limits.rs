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
    CallAuctionMarketDataConstructionError, CallAuctionMarketDataError,
    CallAuctionMarketDataHashIndex, CallAuctionMarketDataLimits, CallAuctionMarketDataLimitsError,
    CallAuctionMarketDataLimitsSpec, CallAuctionMarketDataPublisher, CallAuctionMarketDataReplica,
    CallAuctionMarketDataResource,
};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::{
    AccountId, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, Side, TimestampNs,
};

fn instrument() -> InstrumentId {
    InstrumentId::new(191).unwrap()
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
        symbol: InstrumentSymbol::new("AUCT-MD-LIMITS").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(0), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Halted,
    })
    .unwrap()
}

fn engine_limits(
    active: usize,
    levels: usize,
    accepted: usize,
    retained: usize,
    report_events: usize,
) -> CallAuctionEngineLimits {
    let book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: active,
        max_price_levels_per_side: levels,
        max_accepted_order_ids: accepted,
        max_prepared_uncrosses: 2,
    })
    .unwrap();
    CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: retained,
        terminal_command_reserve: active + 2,
        max_retained_events: retained + active * 2 + 2,
        max_report_events: report_events,
    })
    .unwrap()
}

fn market_limits(
    active: usize,
    levels: usize,
    batch_updates: usize,
) -> CallAuctionMarketDataLimitsSpec {
    CallAuctionMarketDataLimitsSpec {
        max_active_orders: active,
        max_price_levels_per_side: levels,
        max_batch_updates: batch_updates,
    }
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
    reason = "test command construction keeps every capacity dimension explicit"
)]
fn submit(
    command_id: u64,
    auction_id: u64,
    phase_revision: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    price: i64,
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
            AuctionOrderConstraint::Limit(Price::from_raw(price)),
            Quantity::new(quantity).unwrap(),
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

fn uncross(
    engine: &CallAuctionEngine,
    command_id: u64,
    auction_id: u64,
    phase_revision: u64,
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
            CallAuctionRemainderPolicy::RetainAll,
            CallAuctionSelfTradePolicy::Permit,
        ),
        received_at: TimestampNs::from_unix_nanos(command_id),
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

#[test]
fn invalid_and_unrepresentable_limits_identify_the_exact_resource() {
    assert_eq!(
        CallAuctionMarketDataLimits::new(market_limits(0, 1, 3)),
        Err(CallAuctionMarketDataLimitsError::ZeroActiveOrders)
    );
    assert_eq!(
        CallAuctionMarketDataLimits::new(market_limits(1, 0, 3)),
        Err(CallAuctionMarketDataLimitsError::ZeroPriceLevels)
    );
    assert_eq!(
        CallAuctionMarketDataLimits::new(market_limits(1, 1, 0)),
        Err(CallAuctionMarketDataLimitsError::ZeroBatchUpdates)
    );
    assert_eq!(
        CallAuctionMarketDataLimits::new(market_limits(1, 2, 3)),
        Err(CallAuctionMarketDataLimitsError::PriceLevelsExceedActiveOrders)
    );
    assert_eq!(
        CallAuctionMarketDataLimits::new(market_limits(2, 1, 4)),
        Err(CallAuctionMarketDataLimitsError::BatchUpdatesBelowUncrossMaximum)
    );
    assert_eq!(
        CallAuctionMarketDataLimits::new(market_limits(usize::MAX, 1, usize::MAX)),
        Err(CallAuctionMarketDataLimitsError::DerivedBoundOverflow)
    );

    let maximum = usize::MAX;
    assert_eq!(
        CallAuctionMarketDataReplica::try_with_limits(
            definition(),
            market_limits(maximum / 2, 1, maximum),
        )
        .unwrap_err(),
        CallAuctionMarketDataConstructionError::ReservationFailed {
            resource: CallAuctionMarketDataResource::BatchLevelIdentities,
            maximum,
        }
    );
}

#[test]
fn publisher_policy_must_cover_every_configured_source_dimension() {
    let limits = engine_limits(4, 2, 16, 32, 12);
    let engine = CallAuctionEngine::try_with_limits(definition(), limits).unwrap();

    for (spec, resource, selected, required) in [
        (
            market_limits(3, 2, 7),
            CallAuctionMarketDataResource::TrackedOrders,
            3,
            4,
        ),
        (
            market_limits(4, 1, 9),
            CallAuctionMarketDataResource::BidPriceLevels,
            1,
            2,
        ),
        (
            market_limits(4, 2, 11),
            CallAuctionMarketDataResource::BatchUpdates,
            11,
            12,
        ),
    ] {
        assert_eq!(
            CallAuctionMarketDataPublisher::try_from_engine_with_limits(&engine, spec).unwrap_err(),
            CallAuctionMarketDataConstructionError::LimitsBelowSource {
                resource,
                selected,
                required,
            }
        );
    }
}

#[test]
fn full_replica_rejects_a_new_price_without_mutation_poison_or_scratch_residue() {
    let source_limits = engine_limits(2, 2, 8, 16, 5);
    let mut engine = CallAuctionEngine::try_with_limits(definition(), source_limits).unwrap();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica =
        CallAuctionMarketDataReplica::try_with_limits(definition(), market_limits(1, 1, 3))
            .unwrap();

    let collecting = phase(1, 1, 0, CallAuctionPhase::Collecting);
    replica
        .apply_batch(&publish(&mut engine, &mut publisher, collecting))
        .unwrap();
    let first = submit(2, 1, 1, 1, 1, Side::Buy, 100, 1);
    replica
        .apply_batch(&publish(&mut engine, &mut publisher, first))
        .unwrap();
    let depth_before = replica.limit_depth(Side::Buy, usize::MAX);
    let sequence_before = replica.last_sequence();

    let second = submit(3, 1, 1, 2, 2, Side::Buy, 101, 1);
    let batch = publish(&mut engine, &mut publisher, second);
    assert_eq!(
        replica.apply_batch(&batch),
        Err(CallAuctionMarketDataError::CapacityExceeded {
            resource: CallAuctionMarketDataResource::BidPriceLevels,
            maximum: 1,
            attempted: 2,
        })
    );
    assert_eq!(replica.limit_depth(Side::Buy, usize::MAX), depth_before);
    assert_eq!(replica.last_sequence(), sequence_before);
    assert!(!replica.is_poisoned());
    assert_eq!(replica.batch_level_hash_status().occupied_entries, 0);
    replica.validate().unwrap();
}

#[test]
fn single_update_mode_validates_without_inventing_command_boundaries() {
    let limits = engine_limits(1, 1, 8, 16, 3);
    let mut engine = CallAuctionEngine::try_with_limits(definition(), limits).unwrap();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica =
        CallAuctionMarketDataReplica::try_with_limits(definition(), market_limits(1, 1, 3))
            .unwrap();

    let collecting = phase(1, 1, 0, CallAuctionPhase::Collecting);
    let phase_batch = publish(&mut engine, &mut publisher, collecting);
    assert_eq!(phase_batch.updates().len(), 1);
    replica.apply(phase_batch.updates()[0]).unwrap();
    assert_eq!(replica.last_command_sequence(), 0);
    replica.validate().unwrap();

    let order = submit(2, 1, 1, 1, 1, Side::Buy, 100, 1);
    let order_batch = publish(&mut engine, &mut publisher, order);
    assert_eq!(order_batch.updates().len(), 1);
    replica.apply(order_batch.updates()[0]).unwrap();
    assert_eq!(replica.last_command_sequence(), 0);
    assert_eq!(replica.book_revision(), 1);
    replica.validate().unwrap();
}

#[test]
fn oversized_batch_is_rejected_before_any_replica_transition() {
    let source_limits = engine_limits(5, 1, 8, 32, 11);
    let mut engine = CallAuctionEngine::try_with_limits(definition(), source_limits).unwrap();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();

    let mut command_id = 1_u64;
    publish(
        &mut engine,
        &mut publisher,
        phase(command_id, 1, 0, CallAuctionPhase::Collecting),
    );
    command_id += 1;
    for (order_id, account_id, side, quantity) in [
        (1, 1, Side::Buy, 2),
        (2, 2, Side::Buy, 2),
        (3, 3, Side::Sell, 1),
        (4, 4, Side::Sell, 2),
        (5, 5, Side::Sell, 1),
    ] {
        let command = submit(command_id, 1, 1, order_id, account_id, side, 100, quantity);
        publish(&mut engine, &mut publisher, command);
        command_id += 1;
    }
    publish(
        &mut engine,
        &mut publisher,
        phase(command_id, 1, 1, CallAuctionPhase::Frozen),
    );
    command_id += 1;

    let mut replica =
        CallAuctionMarketDataReplica::try_with_limits(definition(), market_limits(1, 1, 3))
            .unwrap();
    replica.apply_snapshot(&publisher.snapshot()).unwrap();
    let before_sequence = replica.last_sequence();
    let before_bids = replica.limit_depth(Side::Buy, usize::MAX);
    let before_asks = replica.limit_depth(Side::Sell, usize::MAX);
    let command = uncross(&engine, command_id, 1, 2);
    let batch = publish(&mut engine, &mut publisher, command);
    assert!(batch.updates().len() > 3);
    assert_eq!(
        replica.apply_batch(&batch),
        Err(CallAuctionMarketDataError::CapacityExceeded {
            resource: CallAuctionMarketDataResource::BatchUpdates,
            maximum: 3,
            attempted: batch.updates().len(),
        })
    );
    assert_eq!(replica.last_sequence(), before_sequence);
    assert_eq!(replica.limit_depth(Side::Buy, usize::MAX), before_bids);
    assert_eq!(replica.limit_depth(Side::Sell, usize::MAX), before_asks);
    assert!(!replica.is_poisoned());
    assert_eq!(replica.batch_level_hash_status().occupied_entries, 0);
}

#[test]
fn oversized_snapshot_is_atomic_and_valid_repair_reuses_both_arenas() {
    let source_limits = engine_limits(2, 2, 8, 16, 5);
    let mut engine = CallAuctionEngine::try_with_limits(definition(), source_limits).unwrap();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let collecting = phase(1, 1, 0, CallAuctionPhase::Collecting);
    publish(&mut engine, &mut publisher, collecting);
    let first = submit(2, 1, 1, 1, 1, Side::Buy, 100, 1);
    publish(&mut engine, &mut publisher, first);
    let valid = publisher.snapshot();
    let second = submit(3, 1, 1, 2, 2, Side::Buy, 101, 1);
    publish(&mut engine, &mut publisher, second);
    let oversized = publisher.snapshot();

    let mut replica =
        CallAuctionMarketDataReplica::try_with_limits(definition(), market_limits(1, 1, 3))
            .unwrap();
    let active_before = replica.price_level_arena_status(Side::Buy);
    let standby_before = replica.standby_price_level_arena_status(Side::Buy);
    assert_eq!(
        replica.apply_snapshot(&oversized),
        Err(CallAuctionMarketDataError::CapacityExceeded {
            resource: CallAuctionMarketDataResource::BidPriceLevels,
            maximum: 1,
            attempted: 2,
        })
    );
    assert_eq!(replica.last_sequence(), 0);
    assert!(replica.limit_depth(Side::Buy, usize::MAX).is_empty());

    replica.apply_snapshot(&valid).unwrap();
    assert_eq!(replica.last_sequence(), valid.as_of_sequence());
    assert_eq!(replica.limit_depth(Side::Buy, usize::MAX).len(), 1);
    assert_eq!(
        replica.price_level_arena_status(Side::Buy).allocated_slots,
        active_before.allocated_slots
    );
    assert_eq!(
        replica
            .standby_price_level_arena_status(Side::Buy)
            .allocated_slots,
        standby_before.allocated_slots
    );
    replica.validate().unwrap();
}

#[test]
fn one_thousand_identity_and_price_cycles_keep_all_public_state_allocations_fixed() {
    const CYCLES: u64 = 1_000;
    let retained = usize::try_from(CYCLES * 2 + 8).unwrap();
    let limits = engine_limits(1, 1, usize::try_from(CYCLES).unwrap(), retained, 3);
    let mut engine = CallAuctionEngine::try_with_limits(definition(), limits).unwrap();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica =
        CallAuctionMarketDataReplica::try_with_limits(definition(), market_limits(1, 1, 3))
            .unwrap();

    let publisher_orders = publisher
        .hash_index_status(CallAuctionMarketDataHashIndex::TrackedOrders)
        .unwrap();
    let publisher_uncross = publisher
        .hash_index_status(CallAuctionMarketDataHashIndex::PublisherUncrossSources)
        .unwrap();
    let publisher_bid = publisher.price_level_arena_status(Side::Buy);
    let replica_bid = replica.price_level_arena_status(Side::Buy);
    let replica_standby = replica.standby_price_level_arena_status(Side::Buy);
    let replica_scratch = replica.batch_level_hash_status();

    let opening = phase(1, 1, 0, CallAuctionPhase::Collecting);
    replica
        .apply_batch(&publish(&mut engine, &mut publisher, opening))
        .unwrap();
    let mut command_id = 2_u64;
    for cycle in 1..=CYCLES {
        let price = 100 + i64::try_from(cycle % 2).unwrap();
        let add = submit(command_id, 1, 1, cycle, cycle, Side::Buy, price, 1);
        command_id += 1;
        replica
            .apply_batch(&publish(&mut engine, &mut publisher, add))
            .unwrap();
        let remove = cancel(command_id, cycle, cycle);
        command_id += 1;
        replica
            .apply_batch(&publish(&mut engine, &mut publisher, remove))
            .unwrap();
        if cycle % 100 == 0 {
            publisher.validate_against(&engine).unwrap();
            replica.apply_snapshot(&publisher.snapshot()).unwrap();
            replica.validate().unwrap();
        }
    }

    let final_orders = publisher
        .hash_index_status(CallAuctionMarketDataHashIndex::TrackedOrders)
        .unwrap();
    assert_eq!(
        final_orders.allocated_entries,
        publisher_orders.allocated_entries
    );
    assert_eq!(
        final_orders.initialized_buckets,
        publisher_orders.initialized_buckets
    );
    assert_eq!(final_orders.occupied_entries, 0);
    let final_uncross = publisher
        .hash_index_status(CallAuctionMarketDataHashIndex::PublisherUncrossSources)
        .unwrap();
    assert_eq!(
        final_uncross.allocated_entries,
        publisher_uncross.allocated_entries
    );
    assert_eq!(final_uncross.occupied_entries, 0);
    assert_eq!(
        publisher
            .price_level_arena_status(Side::Buy)
            .allocated_slots,
        publisher_bid.allocated_slots
    );
    assert_eq!(
        replica.price_level_arena_status(Side::Buy).allocated_slots,
        replica_bid.allocated_slots
    );
    assert_eq!(
        replica
            .standby_price_level_arena_status(Side::Buy)
            .allocated_slots,
        replica_standby.allocated_slots
    );
    assert_eq!(
        replica.batch_level_hash_status().allocated_entries,
        replica_scratch.allocated_entries
    );
    assert_eq!(replica.batch_level_hash_status().occupied_entries, 0);
    assert!(replica.limit_depth(Side::Buy, usize::MAX).is_empty());
    publisher.validate_against(&engine).unwrap();
    replica.validate().unwrap();
}
