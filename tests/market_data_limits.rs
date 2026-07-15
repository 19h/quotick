use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::market_data::{
    MarketDataConstructionError, MarketDataError, MarketDataHashIndex, MarketDataLimits,
    MarketDataLimitsError, MarketDataLimitsSpec, MarketDataPublisher, MarketDataReplica,
    MarketDataResource,
};
use quotick::matching::{
    CancelOrder, Command, MassCancel, MassCancelScope, NewOrder, OrderBook, OrderBookLimits,
    OrderBookLimitsSpec, OrderDisplay, OrderType, ReplaceOrder, SelfTradePrevention, TimeInForce,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

fn instrument() -> InstrumentId {
    InstrumentId::new(1).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(1).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("MDLIMIT").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn matching_limits(
    active: usize,
    levels: usize,
    controls: usize,
    accepted: usize,
    history: usize,
    max_report_events: usize,
) -> OrderBookLimits {
    OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: active,
        max_active_accounts: active,
        max_price_levels_per_side: levels,
        max_accepted_order_ids: accepted,
        max_account_controls: controls,
        max_retained_commands: history,
        cancellation_reserve: active,
        max_report_events,
        max_retained_events: history
            .saturating_mul(max_report_events)
            .max(active.saturating_mul(3).saturating_add(1)),
        max_prepared_order_selections: 2,
    })
    .unwrap()
}

fn market_limits(
    active: usize,
    levels: usize,
    controls: usize,
    batch: usize,
) -> MarketDataLimitsSpec {
    MarketDataLimitsSpec {
        max_active_orders: active,
        max_price_levels_per_side: levels,
        max_account_controls: controls,
        max_batch_updates: batch,
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "test command construction keeps every capacity dimension explicit"
)]
fn resting(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    price: i64,
    quantity: u64,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn cancel(command_id: u64, order_id: u64, account_id: u64) -> Command {
    Command::Cancel(CancelOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn replace(command_id: u64, order_id: u64, account_id: u64, price: i64, quantity: u64) -> Command {
    Command::Replace(ReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        new_quantity: Quantity::new(quantity).unwrap(),
        new_price: Price::from_raw(price),
        new_display: OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn mass_cancel(command_id: u64, account_id: u64) -> Command {
    Command::MassCancel(MassCancel {
        command_id: CommandId::new(command_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        scope: MassCancelScope::All,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn publish(
    book: &mut OrderBook,
    publisher: &mut MarketDataPublisher,
    command: Command,
) -> quotick::market_data::MarketDataBatch {
    let report = book.submit(command).unwrap();
    publisher.publish(command, &report, book).unwrap()
}

#[test]
fn invalid_and_unrepresentable_limits_fail_before_state_exists() {
    let base = market_limits(2, 2, 2, 3);
    for (spec, expected) in [
        (
            MarketDataLimitsSpec {
                max_active_orders: 0,
                ..base
            },
            MarketDataLimitsError::ZeroActiveOrders,
        ),
        (
            MarketDataLimitsSpec {
                max_price_levels_per_side: 0,
                ..base
            },
            MarketDataLimitsError::ZeroPriceLevels,
        ),
        (
            MarketDataLimitsSpec {
                max_account_controls: 0,
                ..base
            },
            MarketDataLimitsError::ZeroAccountControls,
        ),
        (
            MarketDataLimitsSpec {
                max_batch_updates: 0,
                ..base
            },
            MarketDataLimitsError::ZeroBatchUpdates,
        ),
        (
            market_limits(1, 2, 1, 2),
            MarketDataLimitsError::PriceLevelsExceedActiveOrders,
        ),
        (
            market_limits(2, 1, 1, 2),
            MarketDataLimitsError::BatchUpdatesBelowMassCancelMaximum,
        ),
    ] {
        assert_eq!(MarketDataLimits::new(spec), Err(expected));
        assert!(matches!(
            MarketDataReplica::try_with_limits(instrument(), version(), TradingState::Open, spec,),
            Err(MarketDataConstructionError::InvalidLimits(observed)) if observed == expected
        ));
    }

    let maximum = usize::MAX - 1;
    assert!(matches!(
        MarketDataReplica::try_with_limits(
            instrument(),
            version(),
            TradingState::Open,
            market_limits(maximum, maximum, 1, usize::MAX),
        ),
        Err(MarketDataConstructionError::ReservationFailed {
            resource: MarketDataResource::BidPriceLevels,
            maximum: observed,
        }) if observed == maximum
    ));
}

#[test]
fn publisher_policy_must_cover_every_source_dimension() {
    let book =
        OrderBook::try_with_limits(definition(), matching_limits(4, 4, 4, 16, 16, 8)).unwrap();
    for (spec, resource, selected, required) in [
        (
            market_limits(3, 3, 4, 8),
            MarketDataResource::TrackedOrders,
            3,
            4,
        ),
        (
            market_limits(4, 3, 4, 8),
            MarketDataResource::BidPriceLevels,
            3,
            4,
        ),
        (
            market_limits(4, 4, 3, 8),
            MarketDataResource::AccountControls,
            3,
            4,
        ),
        (
            market_limits(4, 4, 4, 5),
            MarketDataResource::BatchUpdates,
            5,
            8,
        ),
    ] {
        assert!(matches!(
            MarketDataPublisher::try_from_book_with_limits(&book, spec),
            Err(MarketDataConstructionError::LimitsBelowSource {
                resource: observed_resource,
                selected: observed_selected,
                required: observed_required,
            }) if observed_resource == resource
                && observed_selected == selected
                && observed_required == required
        ));
    }
}

#[test]
fn full_replica_rejects_a_new_level_without_mutation_or_poisoning() {
    let mut book =
        OrderBook::try_with_limits(definition(), matching_limits(2, 2, 2, 8, 8, 4)).unwrap();
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::try_with_limits(
        instrument(),
        version(),
        TradingState::Open,
        market_limits(1, 1, 1, 2),
    )
    .unwrap();

    let first = publish(
        &mut book,
        &mut publisher,
        resting(1, 1, 1, Side::Buy, 100, 1),
    );
    replica.apply_batch(&first).unwrap();
    let before = replica.depth(Side::Buy, usize::MAX);
    let sequence = replica.last_sequence();

    let second = publish(
        &mut book,
        &mut publisher,
        resting(2, 2, 2, Side::Buy, 99, 1),
    );
    assert_eq!(
        replica.apply_batch(&second),
        Err(MarketDataError::CapacityExceeded {
            resource: MarketDataResource::BidPriceLevels,
            maximum: 1,
            attempted: 2,
        })
    );
    assert_eq!(replica.depth(Side::Buy, usize::MAX), before);
    assert_eq!(replica.last_sequence(), sequence);
    assert!(!replica.is_poisoned());
    assert_eq!(replica.batch_level_hash_status().occupied_entries, 0);
}

#[test]
fn full_replica_reuses_a_deleted_level_within_one_replacement_batch() {
    let mut book =
        OrderBook::try_with_limits(definition(), matching_limits(1, 1, 1, 8, 8, 3)).unwrap();
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::try_with_limits(
        instrument(),
        version(),
        TradingState::Open,
        market_limits(1, 1, 1, 2),
    )
    .unwrap();

    replica
        .apply_batch(&publish(
            &mut book,
            &mut publisher,
            resting(1, 1, 1, Side::Buy, 100, 2),
        ))
        .unwrap();
    let replacement = publish(&mut book, &mut publisher, replace(2, 1, 1, 99, 2));
    assert_eq!(replacement.updates().len(), 2);
    replica.apply_batch(&replacement).unwrap();
    assert_eq!(
        replica.depth(Side::Buy, usize::MAX),
        book.depth(Side::Buy, usize::MAX)
    );
    assert_eq!(
        replica.price_level_arena_status(Side::Buy).occupied_slots,
        1
    );
}

#[test]
fn batch_update_limit_is_checked_before_any_replica_transition() {
    let mut book =
        OrderBook::try_with_limits(definition(), matching_limits(2, 1, 2, 8, 8, 4)).unwrap();
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::try_with_limits(
        instrument(),
        version(),
        TradingState::Open,
        market_limits(1, 1, 1, 2),
    )
    .unwrap();

    publish(
        &mut book,
        &mut publisher,
        resting(1, 1, 7, Side::Buy, 100, 1),
    );
    publish(
        &mut book,
        &mut publisher,
        resting(2, 2, 7, Side::Buy, 100, 1),
    );
    replica.apply_snapshot(&publisher.snapshot()).unwrap();
    let before = replica.depth(Side::Buy, usize::MAX);
    let sequence = replica.last_sequence();
    let cancellation = publish(&mut book, &mut publisher, mass_cancel(3, 7));
    assert_eq!(cancellation.updates().len(), 3);
    assert_eq!(
        replica.apply_batch(&cancellation),
        Err(MarketDataError::CapacityExceeded {
            resource: MarketDataResource::BatchUpdates,
            maximum: 2,
            attempted: 3,
        })
    );
    assert_eq!(replica.depth(Side::Buy, usize::MAX), before);
    assert_eq!(replica.last_sequence(), sequence);
    assert!(!replica.is_poisoned());
}

#[test]
fn oversized_snapshot_is_atomic_and_a_valid_snapshot_reuses_standby_storage() {
    let mut source =
        OrderBook::try_with_limits(definition(), matching_limits(2, 2, 2, 8, 8, 4)).unwrap();
    source.submit(resting(1, 1, 1, Side::Sell, 101, 1)).unwrap();
    source.submit(resting(2, 2, 2, Side::Sell, 102, 1)).unwrap();
    let oversized = MarketDataPublisher::from_book(&source).unwrap().snapshot();

    let mut replica = MarketDataReplica::try_with_limits(
        instrument(),
        version(),
        TradingState::Open,
        market_limits(1, 1, 1, 2),
    )
    .unwrap();
    let active_before = replica.price_level_arena_status(Side::Sell);
    let standby_before = replica.standby_price_level_arena_status(Side::Sell);
    assert_eq!(
        replica.apply_snapshot(&oversized),
        Err(MarketDataError::CapacityExceeded {
            resource: MarketDataResource::AskPriceLevels,
            maximum: 1,
            attempted: 2,
        })
    );
    assert_eq!(replica.last_sequence(), 0);
    assert!(replica.depth(Side::Sell, usize::MAX).is_empty());

    let mut valid_source =
        OrderBook::try_with_limits(definition(), matching_limits(1, 1, 1, 4, 4, 2)).unwrap();
    valid_source
        .submit(resting(1, 11, 11, Side::Sell, 103, 1))
        .unwrap();
    let valid = MarketDataPublisher::from_book(&valid_source)
        .unwrap()
        .snapshot();
    replica.apply_snapshot(&valid).unwrap();
    assert_eq!(
        replica.depth(Side::Sell, usize::MAX),
        valid_source.depth(Side::Sell, usize::MAX)
    );
    assert_eq!(
        replica.price_level_arena_status(Side::Sell).allocated_slots,
        active_before.allocated_slots
    );
    assert_eq!(
        replica
            .standby_price_level_arena_status(Side::Sell)
            .allocated_slots,
        standby_before.allocated_slots
    );
}

#[test]
fn one_thousand_identity_and_price_cycles_never_move_authoritative_storage() {
    const CYCLES: u64 = 1_000;
    let accepted = usize::try_from(CYCLES).unwrap();
    let history = usize::try_from(CYCLES * 2 + 1).unwrap();
    let mut book =
        OrderBook::try_with_limits(definition(), matching_limits(1, 1, 1, accepted, history, 2))
            .unwrap();
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::try_with_limits(
        instrument(),
        version(),
        TradingState::Open,
        market_limits(1, 1, 1, 2),
    )
    .unwrap();
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    let publisher_order = publisher
        .hash_index_status(MarketDataHashIndex::TrackedOrders)
        .unwrap();
    let publisher_scratch = publisher
        .hash_index_status(MarketDataHashIndex::PublisherBatchLevels)
        .unwrap();
    let publisher_bid = publisher.price_level_arena_status(Side::Buy);
    let replica_bid = replica.price_level_arena_status(Side::Buy);
    let replica_standby = replica.standby_price_level_arena_status(Side::Buy);
    let replica_scratch = replica.batch_level_hash_status();

    let mut command_id = 1_u64;
    for cycle in 1..=CYCLES {
        let price = 100 + i64::try_from(cycle % 2).unwrap();
        let add = resting(command_id, cycle, 1, Side::Buy, price, 1);
        command_id += 1;
        replica
            .apply_batch(&publish(&mut book, &mut publisher, add))
            .unwrap();
        let remove = cancel(command_id, cycle, 1);
        command_id += 1;
        replica
            .apply_batch(&publish(&mut book, &mut publisher, remove))
            .unwrap();
        if cycle % 100 == 0 {
            publisher.validate_against(&book).unwrap();
            replica.apply_snapshot(&publisher.snapshot()).unwrap();
            replica.validate().unwrap();
        }
    }

    let final_order = publisher
        .hash_index_status(MarketDataHashIndex::TrackedOrders)
        .unwrap();
    assert_eq!(
        final_order.allocated_entries,
        publisher_order.allocated_entries
    );
    assert_eq!(
        final_order.initialized_buckets,
        publisher_order.initialized_buckets
    );
    let final_publisher_scratch = publisher
        .hash_index_status(MarketDataHashIndex::PublisherBatchLevels)
        .unwrap();
    assert_eq!(
        final_publisher_scratch.allocated_entries,
        publisher_scratch.allocated_entries
    );
    assert_eq!(final_publisher_scratch.occupied_entries, 0);
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
    assert!(replica.depth(Side::Buy, usize::MAX).is_empty());
}
