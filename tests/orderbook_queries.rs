use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::{
    Command, CommandOutcome, MassCancelScope, MatchingHashIndex, NewOrder, OrderBook,
    OrderBookQueryError, OrderBookQueryResource, OrderDisplay, OrderType, RejectReason,
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
        symbol: InstrumentSymbol::new("QUERY").unwrap(),
        kind: InstrumentKind::Equity,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
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
