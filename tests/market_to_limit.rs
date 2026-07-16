use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::{
    Command, CommandOutcome, EventKind, MatchingCapacity, MatchingError, NewOrder, OrderBook,
    OrderBookLimits, OrderBookLimitsSpec, OrderDisplay, OrderType, RejectReason,
    SelfTradePrevention, TimeInForce,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("MARKET-TO-LIMIT").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(-1_000), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::new(32).unwrap(),
        hidden_orders_supported: true,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "market-to-limit fixtures expose every priority-affecting field"
)]
fn order(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    display: OrderDisplay,
    order_type: OrderType,
    time_in_force: TimeInForce,
    self_trade_prevention: SelfTradePrevention,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display,
        order_type,
        time_in_force,
        self_trade_prevention,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn limit(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    price: i64,
    display: OrderDisplay,
) -> Command {
    order(
        command_id,
        order_id,
        account_id,
        side,
        quantity,
        display,
        OrderType::Limit(Price::from_raw(price)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    )
}

fn market_to_limit(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    time_in_force: TimeInForce,
    self_trade_prevention: SelfTradePrevention,
) -> Command {
    order(
        command_id,
        order_id,
        account_id,
        side,
        quantity,
        OrderDisplay::FullyDisplayed,
        OrderType::MarketToLimit,
        time_in_force,
        self_trade_prevention,
    )
}

#[test]
fn market_to_limit_executes_only_at_the_captured_best_and_rests_its_residual() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(
        1,
        1,
        11,
        Side::Sell,
        2,
        100,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    book.submit(limit(
        2,
        2,
        12,
        Side::Sell,
        5,
        110,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();

    let command = market_to_limit(
        3,
        3,
        13,
        Side::Buy,
        5,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let report = book.submit(command).unwrap();
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert!(matches!(
        report.events[0].kind,
        EventKind::OrderAccepted { order_id, .. } if order_id == OrderId::new(3).unwrap()
    ));
    assert!(matches!(
        report.events[1].kind,
        EventKind::MarketToLimitPriced {
            order_id,
            limit_price,
        } if order_id == OrderId::new(3).unwrap() && limit_price == Price::from_raw(100)
    ));
    assert!(matches!(
        report.events[2].kind,
        EventKind::Trade(trade)
            if trade.maker_order_id == OrderId::new(1).unwrap()
                && trade.price == Price::from_raw(100)
                && trade.quantity == Quantity::new(2).unwrap()
    ));
    assert!(matches!(
        report.events[3].kind,
        EventKind::OrderRested {
            order_id,
            price,
            leaves_quantity,
            working_quantity,
        } if order_id == OrderId::new(3).unwrap()
            && price == Price::from_raw(100)
            && leaves_quantity == Quantity::new(3).unwrap()
            && working_quantity == Quantity::new(3).unwrap()
    ));
    assert_eq!(report.events.len(), 4);

    let residual = book.order(OrderId::new(3).unwrap()).unwrap();
    assert_eq!(residual.price, Price::from_raw(100));
    assert_eq!(residual.leaves_quantity, Quantity::new(3).unwrap());
    assert_eq!(book.best_bid().unwrap().price, Price::from_raw(100));
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(110));

    let replay = book.submit(command).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.events, report.events);
    book.validate().unwrap();
}

#[test]
fn market_to_limit_rejections_do_not_consume_the_order_identity() {
    let mut book = OrderBook::new(definition());
    let empty = market_to_limit(
        1,
        10,
        20,
        Side::Buy,
        2,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    assert_eq!(
        book.submit(empty).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::MarketToLimitBookEmpty)
    );

    book.submit(limit(
        2,
        1,
        11,
        Side::Sell,
        1,
        100,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    let accepted = market_to_limit(
        3,
        10,
        20,
        Side::Buy,
        2,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    assert_eq!(
        book.submit(accepted).unwrap().outcome,
        CommandOutcome::Accepted
    );

    for (offset, time_in_force) in [
        TimeInForce::ImmediateOrCancel,
        TimeInForce::ImmediateOrCancelWithMinimum {
            minimum_quantity: Quantity::new(1).unwrap(),
        },
        TimeInForce::FillOrKill,
        TimeInForce::PostOnly,
    ]
    .into_iter()
    .enumerate()
    {
        let raw = u64::try_from(offset).unwrap() + 20;
        let command = market_to_limit(
            raw,
            raw,
            raw,
            Side::Sell,
            1,
            time_in_force,
            SelfTradePrevention::CancelAggressor,
        );
        assert_eq!(
            book.submit(command).unwrap().outcome,
            CommandOutcome::Rejected(RejectReason::MarketToLimitRequiresRestingLifetime)
        );
    }
    book.validate().unwrap();
}

#[test]
fn market_to_limit_uses_hidden_best_liquidity_and_reserve_display_for_the_residual() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Sell, 2, 90, OrderDisplay::Hidden))
        .unwrap();
    book.submit(limit(
        2,
        2,
        12,
        Side::Sell,
        4,
        100,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(100));

    let report = book
        .submit(order(
            3,
            3,
            13,
            Side::Buy,
            5,
            OrderDisplay::Reserve {
                peak: Quantity::new(2).unwrap(),
            },
            OrderType::MarketToLimit,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
    assert!(matches!(
        report.events[1].kind,
        EventKind::MarketToLimitPriced { limit_price, .. }
            if limit_price == Price::from_raw(90)
    ));
    let residual = book.order(OrderId::new(3).unwrap()).unwrap();
    assert_eq!(residual.price, Price::from_raw(90));
    assert_eq!(residual.leaves_quantity, Quantity::new(3).unwrap());
    assert_eq!(residual.working_quantity, Quantity::new(2).unwrap());
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(100));
    assert_eq!(book.best_bid().unwrap().price, Price::from_raw(90));
    book.validate().unwrap();
}

#[test]
fn market_to_limit_rests_fully_hidden_residuals_at_non_positive_prices() {
    for price in [-10, 0] {
        let mut book = OrderBook::new(definition());
        book.submit(limit(
            1,
            1,
            11,
            Side::Sell,
            1,
            price,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
        let report = book
            .submit(order(
                2,
                2,
                12,
                Side::Buy,
                2,
                OrderDisplay::Hidden,
                OrderType::MarketToLimit,
                TimeInForce::GoodTilCancelled,
                SelfTradePrevention::CancelAggressor,
            ))
            .unwrap();
        assert!(matches!(
            report.events[1].kind,
            EventKind::MarketToLimitPriced { limit_price, .. }
                if limit_price == Price::from_raw(price)
        ));
        let residual = book.order(OrderId::new(2).unwrap()).unwrap();
        assert_eq!(residual.price, Price::from_raw(price));
        assert_eq!(residual.display, OrderDisplay::Hidden);
        assert_eq!(residual.visible_quantity(), None);
        assert!(book.best_bid().is_none());
        book.validate().unwrap();
    }
}

#[test]
fn market_to_limit_freezes_the_price_before_cancel_resting_self_trade_prevention() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(
        1,
        1,
        11,
        Side::Sell,
        2,
        100,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    book.submit(limit(
        2,
        2,
        12,
        Side::Sell,
        5,
        110,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();

    let report = book
        .submit(market_to_limit(
            3,
            3,
            11,
            Side::Buy,
            3,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelResting,
        ))
        .unwrap();
    assert!(matches!(
        report.events[1].kind,
        EventKind::MarketToLimitPriced { limit_price, .. }
            if limit_price == Price::from_raw(100)
    ));
    assert!(
        !report
            .events
            .iter()
            .any(|event| matches!(event.kind, EventKind::Trade(_)))
    );
    assert_eq!(
        book.order(OrderId::new(3).unwrap()).unwrap().price,
        Price::from_raw(100)
    );
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(110));
    book.validate().unwrap();
}

#[test]
fn market_to_limit_sell_and_gtd_residual_use_the_captured_best_bid() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(
        1,
        1,
        11,
        Side::Buy,
        1,
        100,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    book.submit(limit(
        2,
        2,
        12,
        Side::Buy,
        5,
        90,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();

    let report = book
        .submit(market_to_limit(
            3,
            3,
            13,
            Side::Sell,
            3,
            TimeInForce::GoodTilTimestamp {
                expires_at: TimestampNs::from_unix_nanos(50),
            },
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
    assert!(matches!(
        report.events[1].kind,
        EventKind::MarketToLimitPriced { limit_price, .. }
            if limit_price == Price::from_raw(100)
    ));
    let residual = book.order(OrderId::new(3).unwrap()).unwrap();
    assert_eq!(residual.price, Price::from_raw(100));
    assert_eq!(residual.leaves_quantity, Quantity::new(2).unwrap());
    assert_eq!(residual.expires_at, Some(TimestampNs::from_unix_nanos(50)));
    assert_eq!(book.best_bid().unwrap().price, Price::from_raw(90));
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(100));
    book.validate().unwrap();
}

#[test]
fn market_to_limit_cancel_both_keeps_the_frozen_price() {
    let mut cancel_both = OrderBook::new(definition());
    cancel_both
        .submit(limit(
            1,
            1,
            11,
            Side::Sell,
            1,
            100,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    cancel_both
        .submit(limit(
            2,
            2,
            12,
            Side::Sell,
            1,
            110,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    let report = cancel_both
        .submit(market_to_limit(
            3,
            3,
            11,
            Side::Buy,
            2,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelBoth,
        ))
        .unwrap();
    assert!(matches!(
        report.events[1].kind,
        EventKind::MarketToLimitPriced { limit_price, .. }
            if limit_price == Price::from_raw(100)
    ));
    assert!(
        !report
            .events
            .iter()
            .any(|event| matches!(event.kind, EventKind::Trade(_)))
    );
    assert!(cancel_both.order(OrderId::new(1).unwrap()).is_none());
    assert!(cancel_both.order(OrderId::new(3).unwrap()).is_none());
    assert_eq!(cancel_both.best_ask().unwrap().price, Price::from_raw(110));
    cancel_both.validate().unwrap();
}

#[test]
fn market_to_limit_decrement_and_cancel_keeps_the_frozen_price() {
    let mut decrement = OrderBook::new(definition());
    decrement
        .submit(limit(
            1,
            1,
            11,
            Side::Sell,
            1,
            100,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    decrement
        .submit(limit(
            2,
            2,
            12,
            Side::Sell,
            2,
            100,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    let report = decrement
        .submit(market_to_limit(
            3,
            3,
            11,
            Side::Buy,
            2,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::DecrementAndCancel,
        ))
        .unwrap();
    assert!(matches!(
        report.events[1].kind,
        EventKind::MarketToLimitPriced { limit_price, .. }
            if limit_price == Price::from_raw(100)
    ));
    assert!(report.events.iter().any(|event| matches!(
        event.kind,
        EventKind::SelfTradePrevented {
            quantity,
            policy: SelfTradePrevention::DecrementAndCancel,
            ..
        } if quantity == Quantity::new(1).unwrap()
    )));
    assert!(report.events.iter().any(|event| matches!(
        event.kind,
        EventKind::Trade(trade)
            if trade.price == Price::from_raw(100)
                && trade.quantity == Quantity::new(1).unwrap()
    )));
    assert!(decrement.order(OrderId::new(3).unwrap()).is_none());
    assert_eq!(decrement.best_ask().unwrap().price, Price::from_raw(100));
    decrement.validate().unwrap();
}

#[test]
fn market_to_limit_residual_capacity_is_checked_before_any_mutation() {
    let limits = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 3,
        max_active_accounts: 3,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 8,
        max_account_controls: 8,
        max_retained_commands: 16,
        cancellation_reserve: 3,
        max_report_events: 8,
        max_retained_events: 64,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let mut book = OrderBook::try_with_limits(definition(), limits).unwrap();
    book.submit(limit(
        1,
        1,
        11,
        Side::Buy,
        1,
        80,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    book.submit(limit(
        2,
        2,
        12,
        Side::Sell,
        1,
        100,
        OrderDisplay::FullyDisplayed,
    ))
    .unwrap();
    let sequence_before = book.last_event_sequence();

    assert_eq!(
        book.submit(market_to_limit(
            3,
            3,
            13,
            Side::Buy,
            2,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        )),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::BidPriceLevels
        ))
    );
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert_eq!(book.best_bid().unwrap().price, Price::from_raw(80));
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(100));
    assert!(book.order(OrderId::new(3).unwrap()).is_none());
    book.validate().unwrap();
}
