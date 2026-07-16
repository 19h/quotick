//! Banded negative-price discovery and exact market/limit allocation.

use quotick::auction::{
    AuctionAllocationLimits, AuctionLevel, AuctionMarketInterest, AuctionOrder,
    AuctionOrderConstraint, AuctionPriceBand, AuctionPriceGrid, AuctionPricePolicy,
    allocate_clearing_price_time, discover_bounded_clearing_price,
};
use quotick::{OrderId, Price, Quantity, Side};

fn main() {
    let grid = AuctionPriceGrid::new(5).unwrap();
    let band = AuctionPriceBand::new(grid, Price::from_raw(-20), Price::from_raw(0)).unwrap();
    let bids = [AuctionLevel::new(Price::from_raw(-5), 8).unwrap()];
    let asks = [AuctionLevel::new(Price::from_raw(-15), 6).unwrap()];
    let market = AuctionMarketInterest::new(2, 0);
    let reference = Price::from_raw(-10);

    let reference_result = discover_bounded_clearing_price(
        &bids,
        &asks,
        market,
        grid,
        band,
        reference,
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap()
    .unwrap();
    assert_eq!(reference_result.price(), Price::from_raw(-10));
    assert_eq!(reference_result.executable_quantity(), 6);
    assert_eq!(reference_result.buy_quantity(), 10);
    assert_eq!(reference_result.sell_quantity(), 6);
    assert_eq!(reference_result.imbalance_side(), Some(Side::Buy));

    let pressure_result = discover_bounded_clearing_price(
        &bids,
        &asks,
        market,
        grid,
        band,
        reference,
        AuctionPricePolicy::PRESSURE_THEN_REFERENCE_LOWER,
    )
    .unwrap()
    .unwrap();
    assert_eq!(pressure_result.price(), Price::from_raw(-5));

    let buy_orders = [
        AuctionOrder::new(
            OrderId::new(1).unwrap(),
            AuctionOrderConstraint::Market,
            Quantity::new(2).unwrap(),
            quotick::auction::AuctionPriorityClass::HIGHEST,
            1,
        ),
        AuctionOrder::new(
            OrderId::new(2).unwrap(),
            AuctionOrderConstraint::Limit(Price::from_raw(-5)),
            Quantity::new(8).unwrap(),
            quotick::auction::AuctionPriorityClass::HIGHEST,
            2,
        ),
    ];
    let sell_orders = [AuctionOrder::new(
        OrderId::new(3).unwrap(),
        AuctionOrderConstraint::Limit(Price::from_raw(-15)),
        Quantity::new(6).unwrap(),
        quotick::auction::AuctionPriorityClass::HIGHEST,
        1,
    )];
    let allocation = allocate_clearing_price_time(
        &buy_orders,
        &sell_orders,
        grid,
        reference_result,
        AuctionAllocationLimits::new(8).unwrap(),
    )
    .unwrap();
    assert_eq!(
        allocation
            .buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        [(1, 2), (2, 4)]
    );
    assert_eq!(allocation.sell_fills()[0].quantity_lots(), 6);

    println!(
        "reference_price={} pressure_price={} executable_lots={} buy_fills={}",
        reference_result.price().raw(),
        pressure_result.price().raw(),
        allocation.executable_quantity(),
        allocation.buy_fills().len()
    );
}
