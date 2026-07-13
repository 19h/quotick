use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::BinaryCodec;
use quotick::durable::DurableOrderBook;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions};
use quotick::market_data::{
    MarketDataError, MarketDataKind, MarketDataPublisher, MarketDataReplica, MarketDataUpdate,
};
use quotick::matching::{
    CancelOrder, Command, NewOrder, OrderBook, OrderType, ReplaceOrder, SelfTradePrevention,
    TimeInForce,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_WAL: AtomicU64 = AtomicU64::new(1);

fn instrument() -> InstrumentId {
    InstrumentId::new(1).expect("instrument")
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(1).expect("version")
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("TEST").expect("symbol"),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).expect("base"),
        quote_asset_id: AssetId::new(2).expect("quote"),
        price: PriceRules::new(0, 1, Price::from_raw(-1_000), Price::from_raw(1_000))
            .expect("price rules"),
        quantity: QuantityRules::new(1, 1, 1_000).expect("quantity rules"),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .expect("definition")
}

#[allow(
    clippy::too_many_arguments,
    reason = "test command factory keeps state-machine cases explicit"
)]
fn order(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    price: i64,
    tif: TimeInForce,
    stp: SelfTradePrevention,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).expect("command"),
        order_id: OrderId::new(order_id).expect("order"),
        account_id: AccountId::new(account_id).expect("account"),
        instrument_id: instrument(),
        instrument_version: version(),
        side,
        quantity: Quantity::new(quantity).expect("quantity"),
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force: tif,
        self_trade_prevention: stp,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn cancel(command_id: u64, order_id: u64, account_id: u64) -> Command {
    Command::Cancel(CancelOrder {
        command_id: CommandId::new(command_id).expect("command"),
        order_id: OrderId::new(order_id).expect("order"),
        account_id: AccountId::new(account_id).expect("account"),
        instrument_id: instrument(),
        instrument_version: version(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn replace(command_id: u64, order_id: u64, account_id: u64, quantity: u64, price: i64) -> Command {
    Command::Replace(ReplaceOrder {
        command_id: CommandId::new(command_id).expect("command"),
        order_id: OrderId::new(order_id).expect("order"),
        account_id: AccountId::new(account_id).expect("account"),
        instrument_id: instrument(),
        instrument_version: version(),
        new_quantity: Quantity::new(quantity).expect("quantity"),
        new_price: Price::from_raw(price),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn publish(
    book: &mut OrderBook,
    publisher: &mut MarketDataPublisher,
    command: Command,
) -> quotick::market_data::MarketDataBatch {
    let report = book.submit(command).expect("matching succeeds");
    let batch = publisher
        .publish(command, &report, book)
        .expect("publication succeeds");
    assert_eq!(batch.updates().len(), report.events.len());
    batch
}

fn assert_mirrors(book: &OrderBook, replica: &MarketDataReplica) {
    assert_eq!(
        replica.depth(Side::Buy, usize::MAX),
        book.depth(Side::Buy, usize::MAX)
    );
    assert_eq!(
        replica.depth(Side::Sell, usize::MAX),
        book.depth(Side::Sell, usize::MAX)
    );
    assert_eq!(replica.last_sequence(), book.last_event_sequence());
}

#[test]
fn incremental_updates_reconstruct_trades_replacements_and_cancellations() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
    let mut replica = MarketDataReplica::new(instrument(), version());
    let genesis = publisher.snapshot();
    replica
        .apply_snapshot(&genesis)
        .expect("genesis snapshot applies");

    let resting = order(
        1,
        11,
        101,
        Side::Sell,
        10,
        101,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    replica
        .apply_batch(&publish(&mut book, &mut publisher, resting))
        .expect("resting update applies");

    let taking = order(
        2,
        12,
        102,
        Side::Buy,
        4,
        101,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    );
    let trade_batch = publish(&mut book, &mut publisher, taking);
    let trade = trade_batch
        .updates()
        .iter()
        .find_map(|update| match update.kind() {
            MarketDataKind::Trade { print, .. } => Some(print),
            _ => None,
        })
        .expect("trade is published");
    assert_eq!(trade.price, Price::from_raw(101));
    assert_eq!(trade.quantity, Quantity::new(4).expect("quantity"));
    assert_eq!(trade.aggressor_side, Side::Buy);
    replica
        .apply_batch(&trade_batch)
        .expect("trade update applies");

    let amendment = replace(3, 11, 101, 3, 102);
    replica
        .apply_batch(&publish(&mut book, &mut publisher, amendment))
        .expect("replacement update applies");
    let cancellation = cancel(4, 11, 101);
    replica
        .apply_batch(&publish(&mut book, &mut publisher, cancellation))
        .expect("cancel update applies");

    assert_mirrors(&book, &replica);
    publisher
        .validate_against(&book)
        .expect("publisher cross-audit passes");
    assert!(replica.depth(Side::Buy, 10).is_empty());
    assert!(replica.depth(Side::Sell, 10).is_empty());
}

#[test]
fn gaps_are_nonmutating_and_a_newer_full_depth_snapshot_recovers_the_replica() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
    let mut replica = MarketDataReplica::new(instrument(), version());
    let genesis = publisher.snapshot();
    replica
        .apply_snapshot(&genesis)
        .expect("genesis snapshot applies");

    let first = publish(
        &mut book,
        &mut publisher,
        order(
            1,
            1,
            1,
            Side::Buy,
            2,
            99,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    );
    let second = publish(
        &mut book,
        &mut publisher,
        order(
            2,
            2,
            2,
            Side::Sell,
            3,
            101,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    );
    assert_eq!(first.first_sequence(), Some(1));
    assert_eq!(second.first_sequence(), Some(3));
    assert_eq!(
        replica.apply_batch(&second),
        Err(MarketDataError::SequenceGap {
            expected: 1,
            actual: 3,
        })
    );
    assert_eq!(replica.last_sequence(), 0);
    assert!(replica.depth(Side::Buy, 10).is_empty());

    replica
        .apply_snapshot(&publisher.snapshot())
        .expect("newer snapshot repairs the gap");
    assert_eq!(
        replica.apply_snapshot(&genesis),
        Err(MarketDataError::StaleSnapshot {
            current: 4,
            snapshot: 0,
        })
    );
    let third = publish(
        &mut book,
        &mut publisher,
        order(
            3,
            3,
            3,
            Side::Buy,
            1,
            98,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    );
    replica
        .apply_batch(&third)
        .expect("post-snapshot incrementals apply");
    assert_mirrors(&book, &replica);
}

#[test]
fn decrement_and_cancel_stp_publishes_the_changed_resting_level() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
    let mut replica = MarketDataReplica::new(instrument(), version());
    replica
        .apply_snapshot(&publisher.snapshot())
        .expect("snapshot applies");

    let maker = order(
        1,
        1,
        7,
        Side::Sell,
        10,
        100,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    replica
        .apply_batch(&publish(&mut book, &mut publisher, maker))
        .expect("maker applies");
    let aggressor = order(
        2,
        2,
        7,
        Side::Buy,
        4,
        100,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::DecrementAndCancel,
    );
    let batch = publish(&mut book, &mut publisher, aggressor);
    assert!(batch.updates().iter().any(|update| {
        matches!(
            update.kind(),
            MarketDataKind::Level(level)
                if level.side == Side::Sell
                    && level.price == Price::from_raw(100)
                    && level.quantity == 6
                    && level.order_count == 1
        )
    }));
    replica.apply_batch(&batch).expect("STP update applies");
    assert_mirrors(&book, &replica);
}

#[test]
fn exact_matching_retries_do_not_duplicate_market_data() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
    let command = order(
        1,
        1,
        1,
        Side::Buy,
        2,
        99,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let first_report = book.submit(command).expect("first command applies");
    let first = publisher
        .publish(command, &first_report, &book)
        .expect("first report publishes");
    assert!(!first.replayed());
    assert_eq!(first.updates().len(), 2);

    let retry_report = book.submit(command).expect("retry resolves");
    assert!(retry_report.replayed);
    let retry = publisher
        .publish(command, &retry_report, &book)
        .expect("retry is suppressed");
    assert!(retry.replayed());
    assert!(retry.updates().is_empty());
    assert_eq!(publisher.last_sequence(), 2);
}

#[test]
fn publisher_bootstraps_from_an_existing_book_and_fails_closed_on_a_trace_gap() {
    let mut book = OrderBook::new(definition());
    let first = order(
        1,
        1,
        1,
        Side::Buy,
        2,
        99,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    book.submit(first).expect("historical command applies");
    let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
    assert_eq!(publisher.last_sequence(), 2);
    assert_eq!(publisher.snapshot().as_of_sequence(), 2);

    let second = order(
        2,
        2,
        2,
        Side::Sell,
        3,
        101,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let mut report = book.submit(second).expect("second command applies");
    for event in &mut report.events {
        event.sequence += 1;
    }
    assert_eq!(
        publisher.publish(second, &report, &book),
        Err(MarketDataError::SequenceGap {
            expected: 3,
            actual: 4,
        })
    );
    assert!(publisher.is_poisoned());
    assert_eq!(
        publisher.publish(second, &report, &book),
        Err(MarketDataError::Poisoned)
    );
}

#[test]
fn every_self_trade_policy_reconstructs_public_depth() {
    for policy in [
        SelfTradePrevention::CancelAggressor,
        SelfTradePrevention::CancelResting,
        SelfTradePrevention::CancelBoth,
        SelfTradePrevention::DecrementAndCancel,
    ] {
        let mut book = OrderBook::new(definition());
        let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
        let mut replica = MarketDataReplica::new(instrument(), version());
        replica
            .apply_snapshot(&publisher.snapshot())
            .expect("snapshot applies");
        for command in [
            order(
                1,
                1,
                7,
                Side::Sell,
                10,
                100,
                TimeInForce::GoodTilCancelled,
                SelfTradePrevention::CancelAggressor,
            ),
            order(
                2,
                2,
                7,
                Side::Buy,
                4,
                100,
                TimeInForce::GoodTilCancelled,
                policy,
            ),
        ] {
            replica
                .apply_batch(&publish(&mut book, &mut publisher, command))
                .expect("STP batch applies");
        }
        assert_mirrors(&book, &replica);
        publisher
            .validate_against(&book)
            .expect("STP publisher audit passes");
    }
}

#[test]
fn multi_maker_negative_price_trades_reconstruct_public_depth() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
    let mut replica = MarketDataReplica::new(instrument(), version());
    replica
        .apply_snapshot(&publisher.snapshot())
        .expect("snapshot applies");
    for command in [
        order(
            1,
            1,
            1,
            Side::Sell,
            2,
            -5,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
        order(
            2,
            2,
            2,
            Side::Sell,
            3,
            -5,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    ] {
        replica
            .apply_batch(&publish(&mut book, &mut publisher, command))
            .expect("maker batch applies");
    }
    let taking = order(
        3,
        3,
        3,
        Side::Buy,
        4,
        -5,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    );
    let batch = publish(&mut book, &mut publisher, taking);
    let trades: Vec<_> = batch
        .updates()
        .iter()
        .filter_map(|update| match update.kind() {
            MarketDataKind::Trade { print, .. } => Some(print),
            _ => None,
        })
        .collect();
    assert_eq!(trades.len(), 2);
    assert!(
        trades
            .iter()
            .all(|trade| trade.price == Price::from_raw(-5))
    );
    replica
        .apply_batch(&batch)
        .expect("multi-fill batch applies");
    assert_mirrors(&book, &replica);
    assert_eq!(
        replica.depth(Side::Sell, 10),
        vec![quotick::matching::LevelSnapshot {
            price: Price::from_raw(-5),
            quantity: 1,
            order_count: 1,
        }]
    );
}

#[test]
fn replica_rejects_a_trade_that_does_not_reconcile_to_the_maker_level() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
    let mut replica = MarketDataReplica::new(instrument(), version());
    replica
        .apply_snapshot(&publisher.snapshot())
        .expect("snapshot applies");
    let maker = order(
        1,
        1,
        1,
        Side::Sell,
        10,
        100,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    replica
        .apply_batch(&publish(&mut book, &mut publisher, maker))
        .expect("maker batch applies");
    let taker = order(
        2,
        2,
        2,
        Side::Buy,
        4,
        100,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    );
    let batch = publish(&mut book, &mut publisher, taker);
    replica
        .apply(batch.updates()[0])
        .expect("accepted no-change event applies");
    let trade = batch
        .updates()
        .iter()
        .find(|update| matches!(update.kind(), MarketDataKind::Trade { .. }))
        .expect("trade update exists");
    let mut corrupted = trade.encode().expect("trade update encodes");
    corrupted[67..83].copy_from_slice(&7_u128.to_le_bytes());
    let corrupted = MarketDataUpdate::decode(&corrupted).expect("payload is structurally valid");
    assert!(matches!(
        replica.apply(corrupted),
        Err(MarketDataError::InvalidUpdate(
            "trade print does not reconcile to its maker-level transition"
        ))
    ));
    assert!(replica.is_poisoned());
}

#[test]
fn publisher_bootstraps_from_wal_recovery_and_continues_without_retransmission() {
    let nonce = NEXT_WAL.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "quotick-market-data-recovery-{}-{nonce}.wal",
        std::process::id()
    ));
    let _ = fs::remove_file(&path);
    let options = JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let maker = order(
        1,
        1,
        1,
        Side::Sell,
        10,
        100,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    {
        let mut durable =
            DurableOrderBook::open(&path, definition(), options).expect("durable book opens");
        durable.submit(maker).expect("maker persists");
        durable.sync_all().expect("journal synchronizes");
    }

    let mut durable =
        DurableOrderBook::open(&path, definition(), options).expect("durable book recovers");
    let mut publisher =
        MarketDataPublisher::from_book(durable.book()).expect("publisher bootstraps from replay");
    assert_eq!(publisher.last_sequence(), 2);
    let mut replica = MarketDataReplica::new(instrument(), version());
    replica
        .apply_snapshot(&publisher.snapshot())
        .expect("recovered snapshot applies");
    let taker = order(
        2,
        2,
        2,
        Side::Buy,
        4,
        100,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    );
    let report = durable.submit(taker).expect("live command persists");
    let batch = publisher
        .publish(taker, &report, durable.book())
        .expect("live report publishes");
    assert_eq!(batch.first_sequence(), Some(3));
    replica.apply_batch(&batch).expect("live batch applies");
    assert_mirrors(durable.book(), &replica);
    drop(durable);
    fs::remove_file(path).expect("test WAL removes");
}
