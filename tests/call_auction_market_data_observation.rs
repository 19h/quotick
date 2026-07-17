use std::mem::{needs_drop, size_of};
use std::panic::{AssertUnwindSafe, catch_unwind};

use quotick::auction::{AuctionOrderConstraint, AuctionPricePolicy};
use quotick::auction_book::{CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder};
use quotick::auction_engine::{
    CallAuctionCommand, CallAuctionEngine, CallAuctionEngineLimits, CallAuctionEngineLimitsSpec,
    CallAuctionIndicativeCommand, CallAuctionPhase, CallAuctionPhaseControl,
    CallAuctionSubmitOrder,
};
use quotick::auction_market_data::{
    CallAuctionMarketDataError, CallAuctionMarketDataObservation, CallAuctionMarketDataPublisher,
    CallAuctionMarketDataReplica, CallAuctionMarketDataUpdate,
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
    InstrumentId::new(211).unwrap()
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
        symbol: InstrumentSymbol::new("AUCT-OBS").unwrap(),
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
        max_accepted_order_ids: 32,
        max_prepared_uncrosses: 1,
    })
    .unwrap();
    let limits = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: 32,
        terminal_command_reserve: 18,
        max_retained_events: 98,
        max_report_events: 33,
    })
    .unwrap();
    CallAuctionEngine::try_with_limits(definition(), limits).unwrap()
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

#[allow(
    clippy::too_many_arguments,
    reason = "test factory keeps every order identity and economic field explicit"
)]
fn submit(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u64,
) -> CallAuctionCommand {
    CallAuctionCommand::Submit(CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(),
        expected_phase_revision: 1,
        order: CallAuctionOrder::new(
            OrderId::new(order_id).unwrap(),
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

fn indicative(engine: &CallAuctionEngine) -> CallAuctionCommand {
    CallAuctionCommand::Indicative(CallAuctionIndicativeCommand {
        command_id: CommandId::new(6).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(),
        expected_phase_revision: 1,
        price_band: engine.book().instrument_price_band(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        received_at: TimestampNs::from_unix_nanos(6),
    })
}

fn execute(
    engine: &mut CallAuctionEngine,
    publisher: &mut CallAuctionMarketDataPublisher,
    replica: &mut CallAuctionMarketDataReplica,
    command: CallAuctionCommand,
) {
    let report = engine.submit(command).unwrap();
    let batch = publisher.publish(command, &report, engine).unwrap();
    replica.apply_batch(&batch).unwrap();
}

fn populated() -> (
    CallAuctionEngine,
    CallAuctionMarketDataPublisher,
    CallAuctionMarketDataReplica,
) {
    let mut engine = engine();
    let mut publisher = CallAuctionMarketDataPublisher::from_engine(&engine).unwrap();
    let mut replica = CallAuctionMarketDataReplica::new(definition());
    replica.apply_snapshot(&publisher.snapshot()).unwrap();
    execute(&mut engine, &mut publisher, &mut replica, phase());
    for command in [
        submit(2, 1, 11, Side::Buy, AuctionOrderConstraint::Market, 3),
        submit(
            3,
            2,
            12,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            5,
        ),
        submit(
            4,
            3,
            13,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(90)),
            7,
        ),
        submit(
            5,
            4,
            14,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(110)),
            4,
        ),
    ] {
        execute(&mut engine, &mut publisher, &mut replica, command);
    }
    let command = indicative(&engine);
    execute(&mut engine, &mut publisher, &mut replica, command);
    (engine, publisher, replica)
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn healthy_observation_is_fixed_size_coherent_and_depth_streams_are_fallible() {
    assert_send_sync::<CallAuctionMarketDataObservation>();
    assert!(!needs_drop::<CallAuctionMarketDataObservation>());
    assert!(
        size_of::<CallAuctionMarketDataObservation>() < 512,
        "observation occupies {} bytes",
        size_of::<CallAuctionMarketDataObservation>()
    );

    let (engine, _, replica) = populated();
    let observation = replica.try_observation().unwrap();
    let source = engine.book().try_observation().unwrap();
    assert_eq!(observation, replica.observation());
    assert_eq!(observation.instrument_id(), instrument());
    assert_eq!(observation.instrument_version(), version());
    assert_eq!(observation.last_sequence(), 6);
    assert_eq!(observation.last_command_sequence(), 6);
    assert_eq!(observation.phase(), engine.phase_snapshot());
    assert_eq!(observation.book_revision(), 4);
    assert_eq!(observation.indicative(), engine.last_indicative());
    assert_eq!(observation.market_buy().quantity(), 3);
    assert_eq!(observation.market_buy().order_count(), 1);
    assert_eq!(observation.market_sell().quantity(), 0);
    assert_eq!(observation.market_sell().order_count(), 0);
    assert_eq!(
        observation.best_bid().unwrap().price(),
        Price::from_raw(100)
    );
    assert_eq!(observation.best_bid().unwrap().quantity(), 5);
    assert_eq!(
        observation.best_ask().unwrap().price(),
        Price::from_raw(110)
    );
    assert_eq!(observation.best_ask().unwrap().quantity(), 4);
    assert_eq!(source.instrument_id(), observation.instrument_id());
    assert_eq!(
        source.instrument_version(),
        observation.instrument_version()
    );
    assert_eq!(source.book_revision(), observation.book_revision());
    for side in [Side::Buy, Side::Sell] {
        let replica_market = match side {
            Side::Buy => observation.market_buy(),
            Side::Sell => observation.market_sell(),
        };
        assert_eq!(source.market_quantity(side), replica_market.quantity());
        assert_eq!(
            source.market_order_count(side),
            replica_market.order_count()
        );
    }
    assert_eq!(source.best_bid(), observation.best_bid());
    assert_eq!(source.best_ask(), observation.best_ask());

    let mut bids = replica.try_limit_depth_iter(Side::Buy).unwrap();
    assert_eq!(bids.len(), 2);
    assert_eq!(bids.next().unwrap().unwrap().price(), Price::from_raw(100));
    assert_eq!(
        bids.next_back().unwrap().unwrap().price(),
        Price::from_raw(90)
    );
    assert!(bids.next().is_none());

    assert_eq!(
        replica
            .try_limit_depth_range_iter(Side::Buy, Price::from_raw(90)..=Price::from_raw(95),)
            .unwrap()
            .map(Result::unwrap)
            .collect::<Vec<_>>(),
        engine.book().limit_depth_range(
            Side::Buy,
            Price::from_raw(90)..=Price::from_raw(95),
            usize::MAX,
        )
    );
    assert_eq!(replica.market_quantity(Side::Buy), 3);
    assert_eq!(replica.phase(), engine.phase_snapshot());
    assert_eq!(replica.book_revision(), 4);
    assert_eq!(replica.indicative(), engine.last_indicative());
}

#[test]
fn poisoned_replica_rejects_all_economic_output_until_snapshot_repair() {
    let (mut engine, mut publisher, mut replica) = populated();
    let diagnostic_sequence = replica.last_sequence();
    let diagnostic_command_sequence = replica.last_command_sequence();
    let limits = replica.limits();

    let command = submit(
        7,
        5,
        15,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(95)),
        2,
    );
    let report = engine.submit(command).unwrap();
    let batch = publisher.publish(command, &report, &engine).unwrap();
    let mut bytes = batch.updates()[0].encode().unwrap();
    let absolute_quantity_offset = 52;
    bytes[absolute_quantity_offset] = bytes[absolute_quantity_offset].wrapping_add(1);
    let corrupted = CallAuctionMarketDataUpdate::decode(&bytes).unwrap();
    assert!(replica.apply(corrupted).is_err());
    assert!(replica.is_poisoned());

    assert_eq!(
        replica.try_observation(),
        Err(CallAuctionMarketDataError::Poisoned)
    );
    assert!(matches!(
        replica.try_limit_depth_iter(Side::Buy),
        Err(CallAuctionMarketDataError::Poisoned)
    ));
    assert!(matches!(
        replica.try_limit_depth_range_iter(Side::Buy, Price::from_raw(90)..=Price::from_raw(100),),
        Err(CallAuctionMarketDataError::Poisoned)
    ));
    assert_eq!(
        replica.try_limit_depth(Side::Buy, usize::MAX),
        Err(CallAuctionMarketDataError::Poisoned)
    );
    assert_eq!(
        replica.try_limit_depth_range(
            Side::Buy,
            Price::from_raw(90)..=Price::from_raw(100),
            usize::MAX,
        ),
        Err(CallAuctionMarketDataError::Poisoned)
    );

    assert!(catch_unwind(AssertUnwindSafe(|| replica.observation())).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| replica.limit_depth(Side::Buy, 1))).is_err());
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            replica.limit_depth_iter(Side::Buy).next()
        }))
        .is_err()
    );
    assert!(catch_unwind(AssertUnwindSafe(|| replica.market_quantity(Side::Buy))).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| replica.phase())).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| replica.book_revision())).is_err());
    assert!(catch_unwind(AssertUnwindSafe(|| replica.indicative())).is_err());

    assert_eq!(replica.last_sequence(), diagnostic_sequence);
    assert_eq!(replica.last_command_sequence(), diagnostic_command_sequence);
    assert_eq!(replica.limits(), limits);
    let _ = replica.price_level_arena_status(Side::Buy);
    let _ = replica.standby_price_level_arena_status(Side::Sell);
    let _ = replica.batch_level_hash_status();

    let snapshot = publisher.snapshot();
    replica.apply_snapshot(&snapshot).unwrap();
    assert!(!replica.is_poisoned());
    let repaired = replica.try_observation().unwrap();
    assert_eq!(repaired.last_sequence(), publisher.last_sequence());
    assert_eq!(
        repaired.last_command_sequence(),
        publisher.last_command_sequence()
    );
    assert_eq!(repaired.phase(), engine.phase_snapshot());
    assert_eq!(repaired.book_revision(), engine.book().state_revision());
    assert_eq!(repaired.indicative(), engine.last_indicative());
    assert_eq!(
        replica.try_limit_depth(Side::Buy, usize::MAX).unwrap(),
        engine.book().limit_depth(Side::Buy, usize::MAX)
    );
}
