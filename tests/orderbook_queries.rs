use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::{
    BestBidOffer, Command, CommandOutcome, DepthSummary, EventKind, ImmediateExecutionRequest,
    ImmediateExecutionTermination, MassCancelScope, MatchingHashIndex, NewOrder, OrderBook,
    OrderBookQueryError, OrderBookQueryResource, OrderDisplay, OrderQueueClass, OrderType,
    PriceLevelOrders, RejectReason, SelfTradePrevention, StopActivation, TimeInForce,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

fn definition_with_price_bounds(minimum: Price, maximum: Price) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("QUERY").unwrap(),
        kind: InstrumentKind::Equity,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, minimum, maximum).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::new(32).unwrap(),
        hidden_orders_supported: true,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn definition() -> InstrumentDefinition {
    definition_with_price_bounds(Price::from_raw(-1_000), Price::from_raw(1_000))
}

#[allow(
    clippy::too_many_arguments,
    reason = "query fixtures keep identity, ownership, side, price, and display explicit"
)]
fn order(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    price: i64,
    quantity: u64,
    display: OrderDisplay,
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
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn populated_book() -> OrderBook {
    let mut book = OrderBook::new(definition());
    for command in [
        order(1, 30, 11, Side::Buy, 90, 3, OrderDisplay::FullyDisplayed),
        order(2, 10, 12, Side::Buy, 100, 5, OrderDisplay::FullyDisplayed),
        order(3, 20, 13, Side::Buy, 110, 7, OrderDisplay::Hidden),
        order(4, 40, 11, Side::Sell, 120, 11, OrderDisplay::FullyDisplayed),
        order(5, 50, 14, Side::Sell, 130, 13, OrderDisplay::FullyDisplayed),
    ] {
        book.submit(command).unwrap();
    }
    book
}

fn execution_quote_book() -> OrderBook {
    let mut book = OrderBook::new(definition());
    for command in [
        order(
            1,
            1,
            8,
            Side::Sell,
            100,
            10,
            OrderDisplay::Reserve {
                peak: Quantity::new(3).unwrap(),
            },
        ),
        order(2, 2, 7, Side::Sell, 100, 2, OrderDisplay::FullyDisplayed),
        order(3, 3, 9, Side::Sell, 100, 5, OrderDisplay::Hidden),
        order(4, 4, 10, Side::Sell, 101, 7, OrderDisplay::FullyDisplayed),
    ] {
        assert_eq!(
            book.submit(command).unwrap().outcome,
            CommandOutcome::Accepted
        );
    }
    book
}

fn queue_position_book() -> OrderBook {
    let mut book = OrderBook::new(definition());
    for command in [
        order(
            1,
            1,
            1,
            Side::Sell,
            100,
            10,
            OrderDisplay::Reserve {
                peak: Quantity::new(3).unwrap(),
            },
        ),
        order(2, 2, 2, Side::Sell, 100, 2, OrderDisplay::FullyDisplayed),
        order(
            3,
            3,
            3,
            Side::Sell,
            100,
            8,
            OrderDisplay::Reserve {
                peak: Quantity::new(2).unwrap(),
            },
        ),
        order(4, 4, 4, Side::Sell, 100, 5, OrderDisplay::Hidden),
        order(5, 5, 5, Side::Sell, 100, 4, OrderDisplay::Hidden),
    ] {
        assert_eq!(
            book.submit(command).unwrap().outcome,
            CommandOutcome::Accepted
        );
    }
    book
}

fn immediate_order(policy: SelfTradePrevention, quantity: u64) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(5).unwrap(),
        order_id: OrderId::new(100).unwrap(),
        account_id: AccountId::new(7).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side: Side::Buy,
        quantity: Quantity::new(quantity).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::ImmediateOrCancel,
        self_trade_prevention: policy,
        received_at: TimestampNs::from_unix_nanos(5),
    })
}

fn execution_totals(report: &quotick::matching::ExecutionReport) -> (u64, u64, u64, i128) {
    let mut executed = 0_u64;
    let mut decremented = 0_u64;
    let mut unfilled = 0_u64;
    let mut raw_notional = 0_i128;
    for event in &report.events {
        match event.kind {
            EventKind::Trade(trade) => {
                executed += trade.quantity.lots();
                raw_notional += i128::from(trade.price.raw()) * i128::from(trade.quantity.lots());
            }
            EventKind::SelfTradePrevented {
                quantity,
                policy: SelfTradePrevention::DecrementAndCancel,
                ..
            } => decremented += quantity.lots(),
            EventKind::OrderCancelled {
                order_id, quantity, ..
            } if order_id == OrderId::new(100).unwrap() => unfilled += quantity.lots(),
            _ => {}
        }
    }
    (executed, decremented, unfilled, raw_notional)
}

fn literal_depth_summary(
    book: &OrderBook,
    side: Side,
    range: std::ops::RangeInclusive<Price>,
) -> (Option<Price>, Option<Price>, usize, u128, u128) {
    let mut best = None;
    let mut worst = None;
    let mut levels = 0_usize;
    let mut orders = 0_u128;
    let mut quantity = 0_u128;
    for level in book.depth_range_iter(side, range) {
        if best.is_none() {
            best = Some(level.price);
        }
        worst = Some(level.price);
        levels = levels.checked_add(1).unwrap();
        orders = orders.checked_add(u128::from(level.order_count)).unwrap();
        quantity = quantity.checked_add(level.quantity).unwrap();
    }
    (best, worst, levels, orders, quantity)
}

#[test]
fn immediate_execution_quotes_match_reserve_hidden_and_stp_execution() {
    for (policy, expected, termination) in [
        (
            SelfTradePrevention::CancelAggressor,
            (3, 0, 12, 300),
            ImmediateExecutionTermination::SelfTradePrevention,
        ),
        (
            SelfTradePrevention::CancelResting,
            (15, 0, 0, 1_500),
            ImmediateExecutionTermination::Filled,
        ),
        (
            SelfTradePrevention::CancelBoth,
            (3, 0, 12, 300),
            ImmediateExecutionTermination::SelfTradePrevention,
        ),
        (
            SelfTradePrevention::DecrementAndCancel,
            (13, 2, 0, 1_300),
            ImmediateExecutionTermination::SelfTradePrevention,
        ),
    ] {
        let mut book = execution_quote_book();
        let before_depth = book.depth_iter(Side::Sell).collect::<Vec<_>>();
        let before_orders = book.active_orders().unwrap();
        let before_commands = book.retained_command_count();
        let request = ImmediateExecutionRequest::new(
            AccountId::new(7).unwrap(),
            Side::Buy,
            Quantity::new(15).unwrap(),
            StopActivation::Market,
            policy,
        );
        let quote = book.immediate_execution_quote(request);

        assert_eq!(quote.instrument_id(), InstrumentId::new(1).unwrap());
        assert_eq!(
            quote.instrument_version(),
            InstrumentVersion::new(1).unwrap()
        );
        assert_eq!(quote.book_event_sequence(), book.last_event_sequence());
        assert_eq!(quote.request(), request);
        assert_eq!(quote.requested_quantity(), Quantity::new(15).unwrap());
        assert_eq!(quote.executed_quantity_lots(), expected.0);
        assert_eq!(quote.self_trade_decrement_quantity_lots(), expected.1);
        assert_eq!(quote.unfilled_quantity_lots(), expected.2);
        assert_eq!(quote.raw_price_notional(), expected.3);
        assert_eq!(quote.worst_execution_price(), Some(Price::from_raw(100)));
        assert_eq!(quote.termination(), termination);
        assert_eq!(
            book.depth_iter(Side::Sell).collect::<Vec<_>>(),
            before_depth
        );
        assert_eq!(book.active_orders().unwrap(), before_orders);
        assert_eq!(book.retained_command_count(), before_commands);

        let report = book.submit(immediate_order(policy, 15)).unwrap();
        assert_eq!(execution_totals(&report), expected);
        book.validate().unwrap();
    }
}

#[test]
fn immediate_execution_quotes_distinguish_limits_exhaustion_and_signed_notional() {
    let book = execution_quote_book();
    let limited = book.immediate_execution_quote(ImmediateExecutionRequest::new(
        AccountId::new(99).unwrap(),
        Side::Buy,
        Quantity::new(20).unwrap(),
        StopActivation::Limit(Price::from_raw(100)),
        SelfTradePrevention::CancelAggressor,
    ));
    assert_eq!(limited.executed_quantity_lots(), 17);
    assert_eq!(limited.unfilled_quantity_lots(), 3);
    assert_eq!(limited.raw_price_notional(), 1_700);
    assert_eq!(limited.worst_execution_price(), Some(Price::from_raw(100)));
    assert_eq!(
        limited.termination(),
        ImmediateExecutionTermination::PriceLimit
    );

    let exhausted = book.immediate_execution_quote(ImmediateExecutionRequest::new(
        AccountId::new(99).unwrap(),
        Side::Buy,
        Quantity::new(30).unwrap(),
        StopActivation::Market,
        SelfTradePrevention::CancelAggressor,
    ));
    assert_eq!(exhausted.executed_quantity_lots(), 24);
    assert_eq!(exhausted.unfilled_quantity_lots(), 6);
    assert_eq!(exhausted.raw_price_notional(), 2_407);
    assert_eq!(
        exhausted.worst_execution_price(),
        Some(Price::from_raw(101))
    );
    assert_eq!(
        exhausted.termination(),
        ImmediateExecutionTermination::BookExhausted
    );

    let mut negative = OrderBook::new(definition());
    negative
        .submit(order(
            1,
            1,
            1,
            Side::Buy,
            -10,
            4,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    negative
        .submit(order(
            2,
            2,
            2,
            Side::Buy,
            -20,
            3,
            OrderDisplay::FullyDisplayed,
        ))
        .unwrap();
    let signed = negative.immediate_execution_quote(ImmediateExecutionRequest::new(
        AccountId::new(99).unwrap(),
        Side::Sell,
        Quantity::new(5).unwrap(),
        StopActivation::Market,
        SelfTradePrevention::CancelAggressor,
    ));
    assert_eq!(signed.executed_quantity_lots(), 5);
    assert_eq!(signed.raw_price_notional(), -60);
    assert_eq!(signed.worst_execution_price(), Some(Price::from_raw(-20)));
    assert_eq!(signed.termination(), ImmediateExecutionTermination::Filled);
    negative.validate().unwrap();
}

#[test]
fn queue_positions_model_current_slices_reserve_refresh_and_hidden_priority() {
    let mut book = queue_position_book();
    let before_commands = book.retained_command_count();
    let before_depth = book.depth_iter(Side::Sell).collect::<Vec<_>>();

    let displayed = book
        .try_order_queue_position(OrderId::new(3).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(displayed.instrument_id(), InstrumentId::new(1).unwrap());
    assert_eq!(
        displayed.instrument_version(),
        InstrumentVersion::new(1).unwrap()
    );
    assert_eq!(displayed.book_event_sequence(), book.last_event_sequence());
    assert_eq!(displayed.order().order_id, OrderId::new(3).unwrap());
    assert_eq!(displayed.queue_class(), OrderQueueClass::Displayed);
    assert_eq!(displayed.order_count_ahead(), 2);
    assert_eq!(displayed.executable_quantity_ahead_lots(), 5);

    let hidden = book
        .try_order_queue_position(OrderId::new(5).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(hidden.queue_class(), OrderQueueClass::Hidden);
    assert_eq!(hidden.order_count_ahead(), 4);
    assert_eq!(hidden.executable_quantity_ahead_lots(), 25);
    assert!(
        book.try_order_queue_position(OrderId::new(99).unwrap())
            .unwrap()
            .is_none()
    );
    assert_eq!(book.retained_command_count(), before_commands);
    assert_eq!(
        book.depth_iter(Side::Sell).collect::<Vec<_>>(),
        before_depth
    );

    let take = Command::New(NewOrder {
        command_id: CommandId::new(6).unwrap(),
        order_id: OrderId::new(100).unwrap(),
        account_id: AccountId::new(99).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side: Side::Buy,
        quantity: Quantity::new(3).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Market,
        time_in_force: TimeInForce::ImmediateOrCancel,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(6),
    });
    assert_eq!(book.submit(take).unwrap().outcome, CommandOutcome::Accepted);
    assert_eq!(
        book.try_price_level_orders(Side::Sell, Price::from_raw(100))
            .unwrap()
            .map(|order| order.order_id.get())
            .collect::<Vec<_>>(),
        [2, 3, 1, 4, 5],
        "the refreshed reserve slice must rejoin the displayed-class tail"
    );

    let refreshed = book
        .try_order_queue_position(OrderId::new(1).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(refreshed.queue_class(), OrderQueueClass::Displayed);
    assert_eq!(refreshed.order_count_ahead(), 2);
    assert_eq!(refreshed.executable_quantity_ahead_lots(), 4);
    assert_eq!(refreshed.order().leaves_quantity.lots(), 7);
    assert_eq!(refreshed.order().working_quantity.lots(), 3);

    let hidden_after_refresh = book
        .try_order_queue_position(OrderId::new(4).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(hidden_after_refresh.order_count_ahead(), 3);
    assert_eq!(hidden_after_refresh.executable_quantity_ahead_lots(), 17);
    book.validate().unwrap();
}

#[test]
fn private_price_level_orders_are_prevalidated_exact_and_double_ended() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<PriceLevelOrders<'static>>();

    let book = queue_position_book();
    let before_commands = book.retained_command_count();
    let before_depth = book.depth_iter(Side::Sell).collect::<Vec<_>>();
    let mut orders = book
        .try_price_level_orders(Side::Sell, Price::from_raw(100))
        .unwrap();
    assert_eq!(orders.len(), 5);
    assert_eq!(orders.next().unwrap().order_id, OrderId::new(1).unwrap());
    assert_eq!(orders.len(), 4);
    assert_eq!(
        orders.next_back().unwrap().order_id,
        OrderId::new(5).unwrap()
    );
    assert_eq!(orders.len(), 3);
    assert_eq!(orders.next().unwrap().order_id, OrderId::new(2).unwrap());
    assert_eq!(
        orders.next_back().unwrap().order_id,
        OrderId::new(4).unwrap()
    );
    let reserve = orders.next().unwrap();
    assert_eq!(reserve.order_id, OrderId::new(3).unwrap());
    assert_eq!(reserve.leaves_quantity, Quantity::new(8).unwrap());
    assert_eq!(reserve.working_quantity, Quantity::new(2).unwrap());
    assert!(orders.next().is_none());
    assert!(orders.next_back().is_none());

    let hidden_only = populated_book();
    let hidden = hidden_only
        .try_price_level_orders(Side::Buy, Price::from_raw(110))
        .unwrap()
        .collect::<Vec<_>>();
    assert_eq!(hidden.len(), 1);
    assert_eq!(hidden[0].order_id, OrderId::new(20).unwrap());
    assert_eq!(hidden[0].display, OrderDisplay::Hidden);
    assert_eq!(
        book.try_price_level_orders(Side::Buy, Price::from_raw(100))
            .unwrap()
            .len(),
        0
    );
    let restored = OrderBook::from_checkpoint(&book.checkpoint(1, 11).unwrap()).unwrap();
    assert_eq!(
        restored
            .try_price_level_orders(Side::Sell, Price::from_raw(100))
            .unwrap()
            .map(|order| order.order_id.get())
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5]
    );
    assert_eq!(book.retained_command_count(), before_commands);
    assert_eq!(
        book.depth_iter(Side::Sell).collect::<Vec<_>>(),
        before_depth
    );
    book.validate().unwrap();
}

#[test]
fn best_bid_offer_distinguishes_empty_one_sided_and_hidden_liquidity() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<BestBidOffer>();

    let empty_book = OrderBook::new(definition());
    let empty = empty_book.try_best_bid_offer().unwrap();
    assert_eq!(empty.instrument_id(), InstrumentId::new(1).unwrap());
    assert_eq!(
        empty.instrument_version(),
        InstrumentVersion::new(1).unwrap()
    );
    assert_eq!(empty.book_event_sequence(), 0);
    assert_eq!(empty.bid(), None);
    assert_eq!(empty.offer(), None);
    assert_eq!(empty.spread_raw(), None);
    assert_eq!(empty.midpoint_raw_numerator(), None);

    for (side, expected_bid, expected_offer) in
        [(Side::Buy, true, false), (Side::Sell, false, true)]
    {
        let mut one_sided_book = OrderBook::new(definition());
        one_sided_book
            .submit(order(1, 1, 1, side, 100, 2, OrderDisplay::FullyDisplayed))
            .unwrap();
        let one_sided = one_sided_book.try_best_bid_offer().unwrap();
        assert_eq!(one_sided.bid().is_some(), expected_bid);
        assert_eq!(one_sided.offer().is_some(), expected_offer);
        assert_eq!(one_sided.spread_raw(), None);
        assert_eq!(one_sided.midpoint_raw_numerator(), None);
    }

    let mut hidden_only_book = OrderBook::new(definition());
    hidden_only_book
        .submit(order(1, 1, 1, Side::Buy, 100, 2, OrderDisplay::Hidden))
        .unwrap();
    let hidden_only = hidden_only_book.try_best_bid_offer().unwrap();
    assert_eq!(hidden_only.bid(), None);
    assert_eq!(hidden_only.offer(), None);
}

#[test]
fn best_bid_offer_is_coherent_provenanced_and_exact_at_signed_extremes() {
    let multi_order_level = queue_position_book()
        .try_best_bid_offer()
        .unwrap()
        .offer()
        .unwrap();
    assert_eq!(multi_order_level.quantity, 7);
    assert_eq!(multi_order_level.order_count, 3);

    let book = populated_book();
    let before_commands = book.retained_command_count();
    let quote = book.try_best_bid_offer().unwrap();
    assert_eq!(quote.bid(), book.best_bid());
    assert_eq!(quote.bid().unwrap().price, Price::from_raw(100));
    assert_eq!(quote.offer(), book.best_ask());
    assert_eq!(quote.offer().unwrap().price, Price::from_raw(120));
    assert_eq!(quote.book_event_sequence(), book.last_event_sequence());
    assert_eq!(quote.spread_raw(), Some(20));
    assert_eq!(quote.midpoint_raw_numerator(), Some(220));
    assert_eq!(book.retained_command_count(), before_commands);
    let restored = OrderBook::from_checkpoint(&book.checkpoint(1, 11).unwrap()).unwrap();
    assert_eq!(restored.try_best_bid_offer().unwrap(), quote);

    let mut extreme = OrderBook::new(definition_with_price_bounds(
        Price::from_raw(i64::MIN),
        Price::from_raw(i64::MAX),
    ));
    for command in [
        order(
            1,
            1,
            1,
            Side::Buy,
            i64::MIN,
            1,
            OrderDisplay::FullyDisplayed,
        ),
        order(
            2,
            2,
            2,
            Side::Sell,
            i64::MAX,
            1,
            OrderDisplay::FullyDisplayed,
        ),
    ] {
        assert_eq!(
            extreme.submit(command).unwrap().outcome,
            CommandOutcome::Accepted
        );
    }
    let widest = extreme.try_best_bid_offer().unwrap();
    assert_eq!(widest.spread_raw(), Some(u64::MAX));
    assert_eq!(widest.midpoint_raw_numerator(), Some(-1));
    extreme.validate().unwrap();
}

#[test]
fn depth_summaries_are_provenanced_checked_and_market_ordered() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<DepthSummary>();

    let book = populated_book();
    let before_commands = book.retained_command_count();
    let before_bids = book.depth_iter(Side::Buy).collect::<Vec<_>>();
    let bids = book.try_depth_summary(Side::Buy).unwrap();
    assert_eq!(bids.instrument_id(), InstrumentId::new(1).unwrap());
    assert_eq!(
        bids.instrument_version(),
        InstrumentVersion::new(1).unwrap()
    );
    assert_eq!(bids.book_event_sequence(), book.last_event_sequence());
    assert_eq!(bids.side(), Side::Buy);
    assert_eq!(bids.range_start(), Price::from_raw(-1_000));
    assert_eq!(bids.range_end(), Price::from_raw(1_000));
    assert_eq!(bids.best_price(), Some(Price::from_raw(100)));
    assert_eq!(bids.worst_price(), Some(Price::from_raw(90)));
    assert_eq!(bids.level_count(), 2);
    assert_eq!(bids.displayed_order_count(), 2);
    assert_eq!(bids.displayed_quantity(), 8);
    assert_eq!(
        (
            bids.best_price(),
            bids.worst_price(),
            bids.level_count(),
            bids.displayed_order_count(),
            bids.displayed_quantity(),
        ),
        literal_depth_summary(
            &book,
            Side::Buy,
            Price::from_raw(-1_000)..=Price::from_raw(1_000),
        )
    );

    let offers = book.try_depth_summary(Side::Sell).unwrap();
    assert_eq!(offers.best_price(), Some(Price::from_raw(120)));
    assert_eq!(offers.worst_price(), Some(Price::from_raw(130)));
    assert_eq!(offers.level_count(), 2);
    assert_eq!(offers.displayed_order_count(), 2);
    assert_eq!(offers.displayed_quantity(), 24);

    let band = book
        .try_depth_range_summary(Side::Buy, Price::from_raw(95)..=Price::from_raw(110))
        .unwrap();
    assert_eq!(band.range_start(), Price::from_raw(95));
    assert_eq!(band.range_end(), Price::from_raw(110));
    assert_eq!(band.best_price(), Some(Price::from_raw(100)));
    assert_eq!(band.worst_price(), Some(Price::from_raw(100)));
    assert_eq!(band.level_count(), 1);
    assert_eq!(band.displayed_order_count(), 1);
    assert_eq!(band.displayed_quantity(), 5);
    assert_eq!(
        (
            band.best_price(),
            band.worst_price(),
            band.level_count(),
            band.displayed_order_count(),
            band.displayed_quantity(),
        ),
        literal_depth_summary(&book, Side::Buy, Price::from_raw(95)..=Price::from_raw(110),)
    );

    let inverted = book
        .try_depth_range_summary(Side::Buy, Price::from_raw(110)..=Price::from_raw(95))
        .unwrap();
    assert_eq!(inverted.range_start(), Price::from_raw(110));
    assert_eq!(inverted.range_end(), Price::from_raw(95));
    assert_eq!(inverted.best_price(), None);
    assert_eq!(inverted.worst_price(), None);
    assert_eq!(inverted.level_count(), 0);
    assert_eq!(inverted.displayed_order_count(), 0);
    assert_eq!(inverted.displayed_quantity(), 0);

    let restored = OrderBook::from_checkpoint(&book.checkpoint(1, 11).unwrap()).unwrap();
    assert_eq!(restored.try_depth_summary(Side::Buy).unwrap(), bids);
    assert_eq!(book.retained_command_count(), before_commands);
    assert_eq!(book.depth_iter(Side::Buy).collect::<Vec<_>>(), before_bids);
    book.validate().unwrap();
}

#[test]
fn fallible_depth_is_public_only_market_ordered_and_bounded() {
    let book = populated_book();

    let bids = book.try_depth(Side::Buy, usize::MAX).unwrap();
    assert_eq!(
        bids.iter()
            .map(|level| (level.price.raw(), level.quantity, level.order_count))
            .collect::<Vec<_>>(),
        [(100, 5, 1), (90, 3, 1)]
    );
    assert_eq!(book.try_depth(Side::Sell, 1).unwrap()[0].price.raw(), 120);
    assert!(book.try_depth(Side::Buy, 0).unwrap().is_empty());
    assert_eq!(bids, book.depth(Side::Buy, usize::MAX));
    assert_eq!(bids, book.depth_iter(Side::Buy).collect::<Vec<_>>());
    assert_eq!(book.depth_iter(Side::Buy).size_hint(), (0, Some(3)));
    assert_eq!(
        book.depth_iter(Side::Sell)
            .rev()
            .map(|level| level.price.raw())
            .collect::<Vec<_>>(),
        [130, 120]
    );
}

#[test]
fn fallible_depth_range_is_inclusive_market_ordered_and_public_only() {
    let book = populated_book();

    let bid_band = Price::from_raw(90)..=Price::from_raw(100);
    assert_eq!(
        book.depth_range_iter(Side::Buy, bid_band.clone())
            .map(|level| level.price.raw())
            .collect::<Vec<_>>(),
        [100, 90]
    );
    assert_eq!(
        book.depth_range_iter(Side::Buy, bid_band.clone())
            .rev()
            .map(|level| level.price.raw())
            .collect::<Vec<_>>(),
        [90, 100]
    );
    assert_eq!(
        book.try_depth_range(Side::Buy, bid_band.clone(), 1)
            .unwrap()
            .iter()
            .map(|level| level.price.raw())
            .collect::<Vec<_>>(),
        [100]
    );
    assert_eq!(
        book.depth_range(Side::Buy, bid_band.clone(), usize::MAX),
        book.depth_range_iter(Side::Buy, bid_band)
            .collect::<Vec<_>>()
    );
    assert_eq!(
        book.depth_range_iter(Side::Buy, Price::from_raw(100)..=Price::from_raw(110),)
            .map(|level| level.price.raw())
            .collect::<Vec<_>>(),
        [100],
        "the inclusive band must still omit hidden-only prices"
    );
    assert_eq!(
        book.depth_range_iter(Side::Sell, Price::from_raw(100)..=Price::from_raw(125),)
            .map(|level| level.price.raw())
            .collect::<Vec<_>>(),
        [120]
    );
    assert!(
        book.depth_range_iter(Side::Buy, Price::from_raw(101)..=Price::from_raw(99),)
            .next()
            .is_none()
    );
    assert!(
        book.try_depth_range(Side::Buy, Price::from_raw(90)..=Price::from_raw(100), 0,)
            .unwrap()
            .is_empty()
    );
    book.validate().unwrap();
}

#[test]
fn fallible_private_queries_retain_canonical_identity_order() {
    let book = populated_book();

    let active = book.try_active_orders().unwrap();
    assert_eq!(
        active
            .iter()
            .map(|order| order.order_id.get())
            .collect::<Vec<_>>(),
        [10, 20, 30, 40, 50]
    );
    assert_eq!(active, book.active_orders().unwrap());

    let account = AccountId::new(11).unwrap();
    let all = book
        .try_account_active_order_ids(account, MassCancelScope::All)
        .unwrap();
    assert_eq!(
        all.iter()
            .map(|order_id| order_id.get())
            .collect::<Vec<_>>(),
        [30, 40]
    );
    assert_eq!(
        book.try_account_active_order_ids(account, MassCancelScope::Side(Side::Sell))
            .unwrap(),
        [OrderId::new(40).unwrap()]
    );
    assert_eq!(
        all,
        book.account_active_order_ids(account, MassCancelScope::All)
    );
    assert!(
        book.try_account_active_order_ids(AccountId::new(99).unwrap(), MassCancelScope::All)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn query_failures_carry_the_exact_output_resource_and_bound() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OrderBookQueryError>();

    let error = OrderBookQueryError::ReservationFailed {
        resource: OrderBookQueryResource::ActiveOrders,
        maximum: 250_000,
    };
    assert_eq!(
        error.to_string(),
        "order-book active-order output could not reserve 250000 entries"
    );
    assert!(std::error::Error::source(&error).is_none());
}

#[test]
fn retained_command_history_is_zero_copy_chronological_and_retry_stable() {
    let first = order(1, 10, 11, Side::Buy, 100, 5, OrderDisplay::FullyDisplayed);
    let duplicate_order = order(2, 10, 12, Side::Buy, 90, 7, OrderDisplay::FullyDisplayed);
    let mut book = OrderBook::new(definition());
    let accepted = book.submit(first).unwrap();
    let rejected = book.submit(duplicate_order).unwrap();
    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::DuplicateOrder)
    );
    assert!(book.submit(first).unwrap().replayed);
    let history_index = book.hash_index_status(MatchingHashIndex::CommandHistory);

    let first_lookup = book
        .retained_command_report(CommandId::new(1).unwrap())
        .unwrap();
    assert_eq!(first_lookup.command(), &first);
    assert_eq!(first_lookup.report(), &accepted);
    assert!(!first_lookup.report().replayed);
    assert!(
        book.retained_command_report(CommandId::new(99).unwrap())
            .is_none()
    );

    let mut history = book.retained_history();
    assert_eq!(history.len(), 2);
    let first_history = history.next().unwrap();
    let second_history = history.next().unwrap();
    assert_eq!(first_history.command(), &first);
    assert_eq!(second_history.command(), &duplicate_order);
    assert_eq!(second_history.report(), &rejected);
    assert!(std::ptr::eq(
        first_lookup.command(),
        first_history.command()
    ));
    assert!(std::ptr::eq(first_lookup.report(), first_history.report()));
    assert!(history.next().is_none());
    assert_eq!(book.retained_command_count(), 2);
    assert_eq!(
        book.hash_index_status(MatchingHashIndex::CommandHistory),
        history_index
    );
    book.validate().unwrap();
}
