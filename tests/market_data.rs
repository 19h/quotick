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
    AccountAdmissionState, AccountControl, AccountControlAction, CancelOrder, CancelReason,
    Command, CommandOutcome, DisplayedLiquidityRequest, EventKind, ExecutionReport, ExpirySweep,
    NewOrder, OrderBook, OrderType, RejectReason, ReplaceOrder, SelfTradePrevention,
    StopActivation, StopReference, StopReferenceCursor, StopTriggerSweep, TimeInForce,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    StopReferenceSequence, StopReferenceSourceId, StopReferenceSourceVersion, TimestampNs,
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
        reserve: quotick::instrument::ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
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
        display: quotick::matching::OrderDisplay::FullyDisplayed,
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
        new_display: quotick::matching::OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn account_control(
    command_id: u64,
    account_id: u64,
    expected_revision: u64,
    action: AccountControlAction,
) -> Command {
    Command::AccountControl(AccountControl {
        command_id: CommandId::new(command_id).expect("command"),
        account_id: AccountId::new(account_id).expect("account"),
        instrument_id: instrument(),
        instrument_version: version(),
        expected_revision,
        action,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn expiry_sweep(command_id: u64, through: u64, received_at: u64) -> Command {
    Command::ExpirySweep(ExpirySweep {
        command_id: CommandId::new(command_id).expect("command"),
        instrument_id: instrument(),
        instrument_version: version(),
        through: TimestampNs::from_unix_nanos(through),
        received_at: TimestampNs::from_unix_nanos(received_at),
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "test fixtures expose every independent stop-order dimension at the call site"
)]
fn stop_order(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    trigger_price: i64,
    activation: StopActivation,
    stp: SelfTradePrevention,
) -> Command {
    let Command::New(mut value) = order(
        command_id,
        order_id,
        account_id,
        side,
        quantity,
        1,
        TimeInForce::ImmediateOrCancel,
        stp,
    ) else {
        unreachable!("order fixture is always new")
    };
    value.order_type = OrderType::Stop {
        trigger_price: Price::from_raw(trigger_price),
        activation,
    };
    Command::New(value)
}

fn stop_trigger(
    command_id: u64,
    source_sequence: u64,
    reference_price: i64,
    maximum_orders: u32,
) -> Command {
    Command::StopTriggerSweep(StopTriggerSweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        reference: StopReference::new(
            StopReferenceCursor::new(
                StopReferenceSourceId::new(1).unwrap(),
                StopReferenceSourceVersion::new(1).unwrap(),
                StopReferenceSequence::new(source_sequence).unwrap(),
            ),
            Price::from_raw(reference_price),
        ),
        maximum_orders,
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
    assert_eq!(replica.try_trading_state().unwrap(), book.trading_state());
    assert_eq!(
        replica.try_best_bid_offer().unwrap(),
        book.try_best_bid_offer().unwrap()
    );
    assert_eq!(replica.try_best_bid().unwrap(), book.best_bid());
    assert_eq!(replica.try_best_ask().unwrap(), book.best_ask());
    for side in [Side::Buy, Side::Sell] {
        let range = Price::from_raw(-1_000)..=Price::from_raw(1_000);
        assert_eq!(
            replica
                .try_depth_range_summary(side, range.clone())
                .unwrap(),
            book.try_depth_range_summary(side, range).unwrap()
        );
        assert_eq!(
            replica
                .try_depth_iter(side)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            book.depth_iter(side).collect::<Vec<_>>()
        );
        let exact_best = match side {
            Side::Buy => book.best_ask(),
            Side::Sell => book.best_bid(),
        }
        .map_or(Price::from_raw(0), |level| level.price);
        for constraint in [StopActivation::Market, StopActivation::Limit(exact_best)] {
            let request =
                DisplayedLiquidityRequest::new(side, Quantity::new(17).unwrap(), constraint);
            assert_eq!(
                replica.try_displayed_liquidity_quote(request).unwrap(),
                book.try_displayed_liquidity_quote(request).unwrap()
            );
        }
    }
}

#[test]
fn replica_depth_ranges_are_inclusive_market_ordered_and_book_equivalent() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    for (index, (side, price)) in [
        (Side::Buy, 80),
        (Side::Buy, 100),
        (Side::Buy, 90),
        (Side::Sell, 140),
        (Side::Sell, 120),
        (Side::Sell, 130),
    ]
    .into_iter()
    .enumerate()
    {
        let identity = u64::try_from(index).unwrap() + 1;
        let command = order(
            identity,
            identity,
            identity,
            side,
            identity,
            price,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        );
        replica
            .apply_batch(&publish(&mut book, &mut publisher, command))
            .unwrap();
    }

    assert_eq!(
        replica
            .depth_iter(Side::Buy)
            .map(|level| level.price.raw())
            .collect::<Vec<_>>(),
        [100, 90, 80]
    );
    assert_eq!(replica.depth_iter(Side::Buy).len(), 3);
    assert_eq!(
        replica
            .depth_range_iter(Side::Buy, Price::from_raw(90)..=Price::from_raw(100))
            .map(|level| level.price.raw())
            .collect::<Vec<_>>(),
        [100, 90]
    );
    assert_eq!(
        replica
            .depth_range_iter(Side::Buy, Price::from_raw(90)..=Price::from_raw(100))
            .rev()
            .map(|level| level.price.raw())
            .collect::<Vec<_>>(),
        [90, 100]
    );
    assert_eq!(
        replica
            .try_depth_range(Side::Buy, Price::from_raw(90)..=Price::from_raw(100), 1)
            .unwrap()
            .iter()
            .map(|level| level.price.raw())
            .collect::<Vec<_>>(),
        [100]
    );
    assert_eq!(
        replica.depth_range(
            Side::Sell,
            Price::from_raw(120)..=Price::from_raw(130),
            usize::MAX,
        ),
        book.depth_range(
            Side::Sell,
            Price::from_raw(120)..=Price::from_raw(130),
            usize::MAX,
        )
    );
    assert_eq!(
        replica
            .try_depth_range_summary(Side::Buy, Price::from_raw(90)..=Price::from_raw(100),)
            .unwrap(),
        book.try_depth_range_summary(Side::Buy, Price::from_raw(90)..=Price::from_raw(100),)
            .unwrap()
    );
    assert!(
        replica
            .depth_range_iter(Side::Buy, Price::from_raw(101)..=Price::from_raw(99))
            .next()
            .is_none()
    );
    assert!(
        replica
            .try_depth_range(Side::Buy, Price::from_raw(90)..=Price::from_raw(100), 0)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        replica.depth_iter(Side::Sell).collect::<Vec<_>>(),
        book.depth_iter(Side::Sell).collect::<Vec<_>>()
    );
    replica.validate().unwrap();
}

#[test]
fn incremental_updates_reconstruct_trades_replacements_and_cancellations() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
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
fn dormant_stop_publication_is_depth_neutral_and_triggered_trades_are_mirrored() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    for command in [
        stop_trigger(1, 1, 100, 1),
        order(
            2,
            1,
            21,
            Side::Sell,
            2,
            105,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    ] {
        replica
            .apply_batch(&publish(&mut book, &mut publisher, command))
            .unwrap();
    }
    let depth_before = replica.depth(Side::Sell, usize::MAX);
    let armed = stop_order(
        3,
        2,
        11,
        Side::Buy,
        3,
        110,
        StopActivation::Market,
        SelfTradePrevention::CancelAggressor,
    );
    let armed_batch = publish(&mut book, &mut publisher, armed);
    assert!(
        armed_batch
            .updates()
            .iter()
            .all(|update| matches!(update.kind(), MarketDataKind::NoBookChange))
    );
    replica.apply_batch(&armed_batch).unwrap();
    assert_eq!(replica.depth(Side::Sell, usize::MAX), depth_before);
    MarketDataPublisher::from_book(&book)
        .unwrap()
        .validate_against(&book)
        .unwrap();

    let triggered = publish(&mut book, &mut publisher, stop_trigger(4, 2, 110, 1));
    assert!(
        triggered
            .updates()
            .iter()
            .any(|update| matches!(update.kind(), MarketDataKind::Trade { .. }))
    );
    replica.apply_batch(&triggered).unwrap();
    assert_mirrors(&book, &replica);
    publisher.validate_against(&book).unwrap();
    assert_eq!(book.dormant_stop_count(), 0);
}

#[test]
fn publisher_rejects_noncanonical_stop_activation_and_handles_triggered_stp() {
    let mut book = OrderBook::new(definition());
    book.submit(stop_trigger(1, 1, 100, 1)).unwrap();
    for command in [
        stop_order(
            2,
            1,
            11,
            Side::Buy,
            1,
            105,
            StopActivation::Market,
            SelfTradePrevention::CancelAggressor,
        ),
        stop_order(
            3,
            2,
            12,
            Side::Buy,
            1,
            110,
            StopActivation::Market,
            SelfTradePrevention::CancelAggressor,
        ),
    ] {
        book.submit(command).unwrap();
    }
    let pre_trigger = OrderBook::from_checkpoint(&book.checkpoint(1, 7).unwrap()).unwrap();
    let mut rejecting = MarketDataPublisher::from_book(&pre_trigger).unwrap();
    let sweep = stop_trigger(4, 2, 110, 2);
    let report = book.submit(sweep).unwrap();
    let mut corrupted = report.clone();
    let first_trigger = corrupted
        .events
        .make_mut()
        .iter_mut()
        .find(|event| matches!(event.kind, EventKind::StopOrderTriggered { .. }))
        .unwrap();
    let EventKind::StopOrderTriggered { order_id, .. } = &mut first_trigger.kind else {
        unreachable!()
    };
    *order_id = OrderId::new(2).unwrap();
    assert_eq!(
        rejecting.publish(sweep, &corrupted, &book),
        Err(MarketDataError::TraceMismatch(
            "StopOrderTriggered is absent, ineligible, or not canonical"
        ))
    );

    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    publish(&mut book, &mut publisher, stop_trigger(10, 1, 100, 1));
    publish(
        &mut book,
        &mut publisher,
        order(
            11,
            11,
            7,
            Side::Sell,
            2,
            105,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    );
    publish(
        &mut book,
        &mut publisher,
        stop_order(
            12,
            12,
            7,
            Side::Buy,
            3,
            110,
            StopActivation::Market,
            SelfTradePrevention::CancelResting,
        ),
    );
    let batch = publish(&mut book, &mut publisher, stop_trigger(13, 2, 110, 1));
    assert!(batch.updates().iter().any(|update| matches!(
        update.kind(),
        MarketDataKind::Level(level) if level.price == Price::from_raw(105) && level.quantity == 0
    )));
    publisher.validate_against(&book).unwrap();
}

#[test]
fn expiry_sweep_reconstructs_depth_and_rejects_noncanonical_cancellation_order() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    for (command_id, order_id, account_id, price) in [(1, 11, 101, 100), (2, 12, 102, 99)] {
        let command = order(
            command_id,
            order_id,
            account_id,
            Side::Buy,
            order_id - 10,
            price,
            TimeInForce::GoodTilTimestamp {
                expires_at: TimestampNs::from_unix_nanos(10),
            },
            SelfTradePrevention::CancelAggressor,
        );
        replica
            .apply_batch(&publish(&mut book, &mut publisher, command))
            .unwrap();
    }

    let pre_sweep = OrderBook::from_checkpoint(&book.checkpoint(1, 5).unwrap()).unwrap();
    let mut rejecting_publisher = MarketDataPublisher::from_book(&pre_sweep).unwrap();
    let sweep = expiry_sweep(3, 10, 10);
    let report = book.submit(sweep).unwrap();
    assert!(matches!(
        report.events[0].kind,
        EventKind::OrderCancelled {
            order_id,
            reason: CancelReason::Expired,
            ..
        } if order_id == OrderId::new(11).unwrap()
    ));
    assert!(matches!(
        report.events[1].kind,
        EventKind::OrderCancelled {
            order_id,
            reason: CancelReason::Expired,
            ..
        } if order_id == OrderId::new(12).unwrap()
    ));

    let mut corrupted_events: Vec<_> = report.events.iter().copied().collect();
    let (first, remainder) = corrupted_events.split_at_mut(1);
    std::mem::swap(&mut first[0].kind, &mut remainder[0].kind);
    let corrupted = ExecutionReport {
        events: corrupted_events.into(),
        ..report.clone()
    };
    assert!(matches!(
        rejecting_publisher.publish(sweep, &corrupted, &book),
        Err(MarketDataError::TraceMismatch(
            "expiry cancellation is outside its sweep or not canonical"
        ))
    ));

    let batch = publisher.publish(sweep, &report, &book).unwrap();
    assert!(matches!(
        batch.updates().last().unwrap().kind(),
        MarketDataKind::NoBookChange
    ));
    replica.apply_batch(&batch).unwrap();
    assert_mirrors(&book, &replica);
    publisher.validate_against(&book).unwrap();
}

#[test]
fn account_control_cancellations_and_rejections_preserve_public_trace_continuity() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    for command in [
        order(
            1,
            1,
            7,
            Side::Buy,
            3,
            99,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
        order(
            2,
            2,
            7,
            Side::Sell,
            5,
            101,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    ] {
        replica
            .apply_batch(&publish(&mut book, &mut publisher, command))
            .unwrap();
    }

    let stale = account_control(3, 7, 1, AccountControlAction::Enable);
    let stale_batch = publish(&mut book, &mut publisher, stale);
    assert_eq!(stale_batch.updates().len(), 1);
    assert!(matches!(
        stale_batch.updates()[0].kind(),
        MarketDataKind::NoBookChange
    ));
    replica.apply_batch(&stale_batch).unwrap();

    let block = account_control(4, 7, 0, AccountControlAction::BlockAndCancel);
    let block_batch = publish(&mut book, &mut publisher, block);
    assert_eq!(block_batch.updates().len(), 3);
    replica.apply_batch(&block_batch).unwrap();
    assert!(book.depth(Side::Buy, usize::MAX).is_empty());
    assert!(book.depth(Side::Sell, usize::MAX).is_empty());
    assert_mirrors(&book, &replica);
    publisher.validate_against(&book).unwrap();

    let retry = book.submit(block).unwrap();
    assert!(retry.replayed);
    let retry_batch = publisher.publish(block, &retry, &book).unwrap();
    assert!(retry_batch.replayed());
    assert!(retry_batch.updates().is_empty());
}

#[test]
fn account_control_prior_state_corruption_poison_publisher() {
    let mut book = OrderBook::new(definition());
    let resting = order(
        1,
        1,
        7,
        Side::Buy,
        3,
        99,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    book.submit(resting).unwrap();
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let control = account_control(2, 7, 0, AccountControlAction::BlockAndCancel);
    let mut report = book.submit(control).unwrap();
    let completion = report.events.make_mut().last_mut().unwrap();
    let EventKind::AccountControlApplied { previous_state, .. } = &mut completion.kind else {
        panic!("control report must end in its completion event");
    };
    *previous_state = AccountAdmissionState::Blocked;

    assert_eq!(
        publisher.publish(control, &report, &book),
        Err(MarketDataError::TraceMismatch(
            "AccountControlApplied contradicts its command or cancellations"
        ))
    );
    assert!(publisher.is_poisoned());
}

#[test]
fn gaps_are_nonmutating_and_a_newer_full_depth_snapshot_recovers_the_replica() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
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
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
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
fn fok_decrement_and_cancel_publishes_only_committed_external_execution() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    for command in [
        order(
            1,
            1,
            12,
            Side::Sell,
            2,
            99,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
        order(
            2,
            2,
            11,
            Side::Sell,
            1,
            100,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    ] {
        replica
            .apply_batch(&publish(&mut book, &mut publisher, command))
            .unwrap();
    }

    let rejected_command = order(
        3,
        3,
        11,
        Side::Buy,
        3,
        100,
        TimeInForce::FillOrKill,
        SelfTradePrevention::DecrementAndCancel,
    );
    let rejected_report = book.submit(rejected_command).unwrap();
    assert_eq!(
        rejected_report.outcome,
        CommandOutcome::Rejected(RejectReason::InsufficientLiquidity)
    );
    let rejected_batch = publisher
        .publish(rejected_command, &rejected_report, &book)
        .unwrap();
    assert_eq!(rejected_batch.updates().len(), 1);
    assert!(matches!(
        rejected_batch.updates()[0].kind(),
        MarketDataKind::NoBookChange
    ));
    replica.apply_batch(&rejected_batch).unwrap();

    let accepted_command = order(
        4,
        4,
        11,
        Side::Buy,
        2,
        100,
        TimeInForce::FillOrKill,
        SelfTradePrevention::DecrementAndCancel,
    );
    let accepted_batch = publish(&mut book, &mut publisher, accepted_command);
    assert!(
        accepted_batch
            .updates()
            .iter()
            .any(|update| matches!(update.kind(), MarketDataKind::Trade { .. }))
    );
    assert!(!accepted_batch.updates().iter().any(|update| matches!(
        update.kind(),
        MarketDataKind::Level(level) if level.price == Price::from_raw(100)
    )));
    replica.apply_batch(&accepted_batch).unwrap();
    assert_mirrors(&book, &replica);
    assert!(book.order(OrderId::new(2).unwrap()).is_some());
    publisher.validate_against(&book).unwrap();
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
    for event in report.events.make_mut() {
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
        let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
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
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
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
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
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
    assert_eq!(
        replica.try_depth(Side::Sell, usize::MAX),
        Err(MarketDataError::Poisoned)
    );
    assert!(matches!(
        replica.try_depth_iter(Side::Sell),
        Err(MarketDataError::Poisoned)
    ));
    assert_eq!(replica.try_best_ask(), Err(MarketDataError::Poisoned));
    assert_eq!(replica.try_trading_state(), Err(MarketDataError::Poisoned));

    replica
        .apply_snapshot(&publisher.snapshot())
        .expect("verified snapshot repairs poisoned observation state");
    assert_mirrors(&book, &replica);
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
    let mut replica = MarketDataReplica::new(instrument(), version(), TradingState::Open);
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
