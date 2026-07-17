use std::mem::{needs_drop, size_of};

use quotick::auction::{AuctionOrderConstraint, AuctionPriorityClass};
use quotick::auction_book::{
    CallAuctionBook, CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionBookObservation,
    CallAuctionOrder,
};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::{
    AccountId, AssetId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

fn instrument() -> InstrumentId {
    InstrumentId::new(307).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(11).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("AUCT-BOOK-OBS").unwrap(),
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

fn book() -> CallAuctionBook {
    let limits = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: 16,
        max_price_levels_per_side: 8,
        max_accepted_order_ids: 32,
        max_prepared_uncrosses: 1,
    })
    .unwrap();
    CallAuctionBook::try_with_limits(definition(), limits).unwrap()
}

fn order(
    order_id: u64,
    account_id: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u64,
) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        AccountId::new(account_id).unwrap(),
        instrument(),
        version(),
        side,
        constraint,
        Quantity::new(quantity).unwrap(),
        AuctionPriorityClass::HIGHEST,
    )
}

fn populated() -> CallAuctionBook {
    let mut book = book();
    for order in [
        order(1, 11, Side::Buy, AuctionOrderConstraint::Market, 3),
        order(
            2,
            12,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            5,
        ),
        order(
            3,
            13,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(90)),
            7,
        ),
        order(
            4,
            14,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(110)),
            4,
        ),
    ] {
        book.admit(order).unwrap();
    }
    book
}

fn assert_send_sync<T: Send + Sync>() {}

#[test]
fn authoritative_observation_is_fixed_size_coherent_and_fallible() {
    assert_send_sync::<CallAuctionBookObservation>();
    assert!(!needs_drop::<CallAuctionBookObservation>());
    assert!(
        size_of::<CallAuctionBookObservation>() < 256,
        "observation occupies {} bytes",
        size_of::<CallAuctionBookObservation>()
    );

    let genesis = book().try_observation().unwrap();
    assert_eq!(genesis.book_revision(), 0);
    assert_eq!(genesis.market_quantity(Side::Buy), 0);
    assert_eq!(genesis.market_order_count(Side::Sell), 0);
    assert_eq!(genesis.best_bid(), None);
    assert_eq!(genesis.best_ask(), None);

    let book = populated();
    let observation = book.try_observation().unwrap();
    assert_eq!(observation, book.observation());
    assert_eq!(observation.instrument_id(), instrument());
    assert_eq!(observation.instrument_version(), version());
    assert_eq!(observation.book_revision(), 4);
    assert_eq!(observation.market_quantity(Side::Buy), 3);
    assert_eq!(observation.market_order_count(Side::Buy), 1);
    assert_eq!(observation.market_quantity(Side::Sell), 0);
    assert_eq!(observation.market_order_count(Side::Sell), 0);
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

    assert_eq!(
        book.try_best_limit_price(Side::Buy).unwrap(),
        Some(Price::from_raw(100))
    );
    assert_eq!(
        book.try_best_limit_level(Side::Sell)
            .unwrap()
            .unwrap()
            .price(),
        Price::from_raw(110)
    );
    assert_eq!(
        book.try_limit_level(Side::Buy, Price::from_raw(90))
            .unwrap()
            .unwrap()
            .quantity(),
        7
    );
    assert_eq!(book.try_market_quantity(Side::Buy).unwrap(), 3);
    assert_eq!(book.try_market_order_count(Side::Buy).unwrap(), 1);
}

#[test]
fn authoritative_full_and_range_streams_are_typed_and_double_ended() {
    let populated_book = populated();
    let mut bids = populated_book.try_limit_depth_iter(Side::Buy).unwrap();
    assert_eq!(bids.len(), 2);
    assert_eq!(bids.next().unwrap().unwrap().price(), Price::from_raw(100));
    assert_eq!(
        bids.next_back().unwrap().unwrap().price(),
        Price::from_raw(90)
    );
    assert!(bids.next().is_none());

    let range = Price::from_raw(90)..=Price::from_raw(95);
    let mut selected = populated_book
        .try_limit_depth_range_iter(Side::Buy, range.clone())
        .unwrap();
    assert_eq!(
        selected.next_back().unwrap().unwrap().price(),
        Price::from_raw(90)
    );
    assert!(selected.next().is_none());

    assert_eq!(
        populated_book
            .try_limit_depth(Side::Buy, usize::MAX)
            .unwrap(),
        populated_book.limit_depth(Side::Buy, usize::MAX)
    );
    assert_eq!(
        populated_book
            .try_limit_depth_range(Side::Buy, range.clone(), usize::MAX)
            .unwrap(),
        populated_book.limit_depth_range(Side::Buy, range, usize::MAX)
    );

    let mut crossed = book();
    crossed
        .admit(order(
            20,
            20,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(120)),
            2,
        ))
        .unwrap();
    crossed
        .admit(order(
            21,
            21,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            2,
        ))
        .unwrap();
    let observation = crossed.try_observation().unwrap();
    assert_eq!(
        observation.best_bid().unwrap().price(),
        Price::from_raw(120)
    );
    assert_eq!(
        observation.best_ask().unwrap().price(),
        Price::from_raw(100)
    );
}
