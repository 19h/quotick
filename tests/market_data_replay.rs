use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::market_data::{
    MarketDataBatch, MarketDataPublisher, MarketDataReplayBuffer,
    MarketDataReplayConstructionError, MarketDataReplayError, MarketDataReplica, MarketDataUpdate,
};
use quotick::matching::{
    Command, NewOrder, OrderBook, OrderDisplay, OrderType, SelfTradePrevention, TimeInForce,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

fn instrument(value: u64) -> InstrumentId {
    InstrumentId::new(value).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(1).unwrap()
}

fn instrument_definition(instrument_id: u64) -> InstrumentDefinition {
    instrument_definition_at_version(instrument_id, 1)
}

fn instrument_definition_at_version(
    instrument_id: u64,
    instrument_version: u64,
) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(instrument_id),
        version: InstrumentVersion::new(instrument_version).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("REPLAY").unwrap(),
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

fn order(definition: InstrumentDefinition, command_id: u64, order_id: u64, price: i64) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(order_id).unwrap(),
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        side: Side::Buy,
        quantity: Quantity::new(1).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn publish(
    book: &mut OrderBook,
    publisher: &mut MarketDataPublisher,
    command: Command,
) -> MarketDataBatch {
    let report = book.submit(command).unwrap();
    publisher.publish(command, &report, book).unwrap()
}

fn three_batches(
    definition: InstrumentDefinition,
) -> (OrderBook, MarketDataPublisher, [MarketDataBatch; 3]) {
    let mut book = OrderBook::new(definition);
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let batches = [
        publish(&mut book, &mut publisher, order(definition, 1, 1, 100)),
        publish(&mut book, &mut publisher, order(definition, 2, 2, 99)),
        publish(&mut book, &mut publisher, order(definition, 3, 3, 98)),
    ];
    (book, publisher, batches)
}

#[test]
fn replay_ring_wraps_without_growth_and_paginates_exact_sequences() {
    assert!(matches!(
        MarketDataReplayBuffer::try_new(instrument(1), version(), 0, 0),
        Err(MarketDataReplayConstructionError::ZeroCapacity)
    ));
    assert!(matches!(
        MarketDataReplayBuffer::try_new(instrument(1), version(), 0, usize::MAX),
        Err(MarketDataReplayConstructionError::ReservationFailed { maximum })
            if maximum == usize::MAX
    ));

    let definition = instrument_definition(1);
    let (_, _, batches) = three_batches(definition);
    let mut replay = MarketDataReplayBuffer::try_new(instrument(1), version(), 0, 4).unwrap();
    let initial_allocation = replay.status().allocation_capacity;
    assert_eq!(replay.push_batch(&batches[0]).unwrap(), 2);
    assert_eq!(replay.push_batch(&batches[0]).unwrap(), 0);
    assert_eq!(replay.push_batch(&batches[1]).unwrap(), 2);
    assert_eq!(replay.push_batch(&batches[2]).unwrap(), 2);

    let status = replay.status();
    assert_eq!(status.maximum, 4);
    assert_eq!(status.allocation_capacity, initial_allocation);
    assert_eq!(status.retained, 4);
    assert_eq!(status.earliest_sequence, Some(3));
    assert_eq!(status.latest_sequence, 6);

    let complete = replay.replay_after(2, 8).unwrap();
    assert_eq!(complete.first_sequence(), Some(3));
    assert_eq!(complete.last_sequence(), Some(6));
    assert!(!complete.has_more());
    assert_eq!(
        complete.map(MarketDataUpdate::sequence).collect::<Vec<_>>(),
        vec![3, 4, 5, 6]
    );

    let page = replay.replay_after(4, 1).unwrap();
    assert_eq!(page.len(), 1);
    assert!(page.has_more());
    assert_eq!(
        page.map(MarketDataUpdate::sequence).collect::<Vec<_>>(),
        vec![5]
    );
    assert_eq!(replay.replay_after(6, 4).unwrap().len(), 0);
    assert_eq!(
        replay.replay_after(1, 4),
        Err(MarketDataReplayError::Unavailable {
            earliest_available: Some(3),
            requested_sequence: 2,
        })
    );
    assert_eq!(
        replay.replay_after(7, 4),
        Err(MarketDataReplayError::CursorAhead {
            latest_sequence: 6,
            requested_after: 7,
        })
    );
    assert_eq!(
        replay.replay_after(6, 0),
        Err(MarketDataReplayError::ZeroRequestLimit)
    );
    replay.validate().unwrap();
}

#[test]
fn replay_admission_is_atomic_for_gaps_collisions_identity_and_capacity() {
    let definition = instrument_definition(1);
    let (_, _, batches) = three_batches(definition);

    let mut alternate_book = OrderBook::new(definition);
    let mut alternate_publisher = MarketDataPublisher::from_book(&alternate_book).unwrap();
    let conflicting = publish(
        &mut alternate_book,
        &mut alternate_publisher,
        order(definition, 1, 1, 101),
    );
    let mut replay = MarketDataReplayBuffer::try_new(instrument(1), version(), 0, 4).unwrap();
    replay.push_batch(&batches[0]).unwrap();
    let before = replay.status();
    assert_eq!(
        replay.push_batch(&conflicting),
        Err(MarketDataReplayError::SequenceCollision { sequence: 2 })
    );
    assert_eq!(replay.status(), before);

    let foreign_definition = instrument_definition(2);
    let (_, _, foreign_batches) = three_batches(foreign_definition);
    assert_eq!(
        replay.push_batch(&foreign_batches[0]),
        Err(MarketDataReplayError::WrongInstrument)
    );
    assert_eq!(replay.status(), before);

    let foreign_version_definition = instrument_definition_at_version(1, 2);
    let (_, _, foreign_version_batches) = three_batches(foreign_version_definition);
    assert_eq!(
        replay.push_batch(&foreign_version_batches[0]),
        Err(MarketDataReplayError::WrongInstrumentVersion)
    );
    assert_eq!(replay.status(), before);

    let mut missing = MarketDataReplayBuffer::try_new(instrument(1), version(), 0, 4).unwrap();
    assert_eq!(
        missing.push_batch(&batches[1]),
        Err(MarketDataReplayError::SequenceGap {
            expected: 1,
            actual: 3,
        })
    );
    assert_eq!(missing.status().retained, 0);

    let mut undersized = MarketDataReplayBuffer::try_new(instrument(1), version(), 0, 1).unwrap();
    assert_eq!(
        undersized.push_batch(&foreign_batches[0]),
        Err(MarketDataReplayError::WrongInstrument)
    );
    assert_eq!(
        undersized.push_batch(&batches[0]),
        Err(MarketDataReplayError::BatchExceedsCapacity {
            maximum: 1,
            attempted: 2,
        })
    );
    assert_eq!(undersized.status().retained, 0);
}

#[test]
fn recovered_boundary_accepts_individual_updates_and_rejects_unprovable_history() {
    let definition = instrument_definition(1);
    let (_, _, batches) = three_batches(definition);
    let mut replay = MarketDataReplayBuffer::try_new(instrument(1), version(), 2, 2).unwrap();

    assert_eq!(replay.push(batches[1].updates()[0]).unwrap(), 1);
    assert_eq!(replay.push(batches[1].updates()[0]).unwrap(), 0);
    assert_eq!(replay.push_batch(&batches[1]).unwrap(), 1);
    assert_eq!(replay.push(batches[1].updates()[1]).unwrap(), 0);
    assert_eq!(replay.push(batches[1].updates()[0]).unwrap(), 0);
    assert_eq!(
        replay
            .replay_after(2, 2)
            .unwrap()
            .map(MarketDataUpdate::sequence)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert_eq!(
        replay.push(batches[0].updates()[0]),
        Err(MarketDataReplayError::Unavailable {
            earliest_available: Some(3),
            requested_sequence: 1,
        })
    );
    replay.validate().unwrap();
}

#[test]
fn retained_replay_repairs_a_replica_without_a_snapshot() {
    let definition = instrument_definition(1);
    let (book, publisher, batches) = three_batches(definition);
    let mut replay = MarketDataReplayBuffer::try_new(instrument(1), version(), 0, 6).unwrap();
    let mut replica = MarketDataReplica::new(instrument(1), version(), TradingState::Open);
    replica
        .apply_snapshot(
            &MarketDataPublisher::from_book(&OrderBook::new(definition))
                .unwrap()
                .snapshot(),
        )
        .unwrap();

    for batch in &batches {
        replay.push_batch(batch).unwrap();
    }
    replica.apply_batch(&batches[0]).unwrap();
    assert_eq!(
        replica.apply_batch(&batches[2]),
        Err(quotick::market_data::MarketDataError::SequenceGap {
            expected: 3,
            actual: 5,
        })
    );
    for update in replay.replay_after(replica.last_sequence(), 6).unwrap() {
        replica.apply(update).unwrap();
    }

    assert_eq!(replica.last_sequence(), book.last_event_sequence());
    assert_eq!(
        replica.depth(Side::Buy, usize::MAX),
        book.depth(Side::Buy, usize::MAX)
    );
    assert_eq!(
        replica.depth(Side::Sell, usize::MAX),
        book.depth(Side::Sell, usize::MAX)
    );
    publisher.validate_against(&book).unwrap();
    replica.validate().unwrap();
}
