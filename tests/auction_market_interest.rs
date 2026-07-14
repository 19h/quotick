use quotick::auction::{
    AuctionAllocationLimits, AuctionClearing, AuctionError, AuctionLevel, AuctionMarketInterest,
    AuctionOrder, AuctionOrderConstraint, AuctionPriceBand, AuctionPriceGrid, AuctionPricePolicy,
    allocate_clearing_price_time, discover_bounded_clearing_price,
};
use quotick::{OrderId, Price, Quantity, Side};

fn level(price: i64, quantity: u128) -> AuctionLevel {
    AuctionLevel::new(Price::from_raw(price), quantity).unwrap()
}

fn grid() -> AuctionPriceGrid {
    AuctionPriceGrid::new(1).unwrap()
}

fn band(minimum: i64, maximum: i64) -> AuctionPriceBand {
    AuctionPriceBand::new(grid(), Price::from_raw(minimum), Price::from_raw(maximum)).unwrap()
}

fn bounded(
    bids: &[AuctionLevel],
    asks: &[AuctionLevel],
    market_buy: u128,
    market_sell: u128,
    price_band: AuctionPriceBand,
    reference: i64,
    policy: AuctionPricePolicy,
) -> Option<AuctionClearing> {
    discover_bounded_clearing_price(
        bids,
        asks,
        AuctionMarketInterest::new(market_buy, market_sell),
        grid(),
        price_band,
        Price::from_raw(reference),
        policy,
    )
    .unwrap()
}

#[test]
fn market_only_interest_clears_at_reference_or_pressure_edge() {
    let price_band = band(90, 110);
    let balanced = bounded(
        &[],
        &[],
        10,
        10,
        price_band,
        100,
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap();
    assert_eq!(balanced.price(), Price::from_raw(100));
    assert_eq!(balanced.executable_quantity(), 10);
    assert_eq!(balanced.imbalance_side(), None);

    let buy_pressure = bounded(
        &[],
        &[],
        15,
        10,
        price_band,
        100,
        AuctionPricePolicy::PRESSURE_THEN_REFERENCE_LOWER,
    )
    .unwrap();
    assert_eq!(buy_pressure.price(), Price::from_raw(110));
    assert_eq!(buy_pressure.imbalance_side(), Some(Side::Buy));

    let sell_pressure = bounded(
        &[],
        &[],
        10,
        15,
        price_band,
        100,
        AuctionPricePolicy::PRESSURE_THEN_REFERENCE_HIGHER,
    )
    .unwrap();
    assert_eq!(sell_pressure.price(), Price::from_raw(90));
    assert_eq!(sell_pressure.imbalance_side(), Some(Side::Sell));
}

#[test]
fn mixed_market_and_limit_interest_selects_an_unquoted_banded_price() {
    let result = bounded(
        &[level(105, 5), level(95, 10)],
        &[level(100, 8)],
        3,
        2,
        band(90, 110),
        102,
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap();

    assert_eq!(result.price(), Price::from_raw(102));
    assert_eq!(result.executable_quantity(), 8);
    assert_eq!(result.buy_quantity(), 8);
    assert_eq!(result.sell_quantity(), 10);
    assert_eq!(result.imbalance_side(), Some(Side::Sell));
    assert_eq!(result.imbalance_quantity(), 2);
}

#[test]
fn reference_and_outside_orders_are_clamped_by_the_candidate_band() {
    let clamped = bounded(
        &[level(120, 10)],
        &[level(80, 10)],
        0,
        0,
        band(90, 110),
        120,
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap();
    assert_eq!(clamped.price(), Price::from_raw(110));

    let outside = bounded(
        &[level(120, 4), level(80, 100)],
        &[level(70, 3), level(130, 100)],
        0,
        0,
        band(90, 110),
        100,
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap();
    assert_eq!(outside.price(), Price::from_raw(100));
    assert_eq!(outside.buy_quantity(), 4);
    assert_eq!(outside.sell_quantity(), 3);
    assert_eq!(outside.executable_quantity(), 3);
}

#[test]
fn band_grid_reference_and_market_aggregate_validation_is_fail_closed() {
    let five = AuctionPriceGrid::new(5).unwrap();
    assert_eq!(
        AuctionPriceBand::new(five, Price::from_raw(10), Price::from_raw(5)),
        Err(AuctionError::PriceBandInverted)
    );
    assert_eq!(
        AuctionPriceBand::new(five, Price::from_raw(1), Price::from_raw(10)),
        Err(AuctionError::PriceBandMinimumOffGrid)
    );
    assert_eq!(
        AuctionPriceBand::new(five, Price::from_raw(0), Price::from_raw(11)),
        Err(AuctionError::PriceBandMaximumOffGrid)
    );
    let valid_band = AuctionPriceBand::new(five, Price::from_raw(0), Price::from_raw(10)).unwrap();
    assert_eq!(
        discover_bounded_clearing_price(
            &[],
            &[],
            AuctionMarketInterest::new(1, 1),
            AuctionPriceGrid::new(3).unwrap(),
            valid_band,
            Price::from_raw(0),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        ),
        Err(AuctionError::PriceBandMaximumOffGrid)
    );
    assert_eq!(
        discover_bounded_clearing_price(
            &[],
            &[],
            AuctionMarketInterest::new(1, 1),
            five,
            valid_band,
            Price::from_raw(1),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        ),
        Err(AuctionError::ReferencePriceOffGrid)
    );
    assert_eq!(
        discover_bounded_clearing_price(
            &[level(10, 1)],
            &[],
            AuctionMarketInterest::new(u128::MAX, 1),
            five,
            valid_band,
            Price::from_raw(5),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        ),
        Err(AuctionError::QuantityOverflow(Side::Buy))
    );
    assert_eq!(
        discover_bounded_clearing_price(
            &[],
            &[level(10, 1)],
            AuctionMarketInterest::new(1, u128::MAX),
            five,
            valid_band,
            Price::from_raw(5),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        ),
        Err(AuctionError::QuantityOverflow(Side::Sell))
    );

    let full = AuctionPriceBand::full(five);
    assert!(five.contains(full.minimum()));
    assert!(five.contains(full.maximum()));
    assert!(full.minimum() <= full.maximum());

    let full_unit = AuctionPriceBand::full(grid());
    let full_market = bounded(
        &[],
        &[],
        1,
        1,
        full_unit,
        0,
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap();
    assert_eq!(full_market.price(), Price::from_raw(0));

    let widest_tick = AuctionPriceGrid::new(u64::try_from(i64::MAX).unwrap()).unwrap();
    let widest_band = AuctionPriceBand::full(widest_tick);
    assert_eq!(widest_band.minimum(), Price::from_raw(-i64::MAX));
    assert_eq!(widest_band.maximum(), Price::from_raw(i64::MAX));
    let widest_market = discover_bounded_clearing_price(
        &[],
        &[],
        AuctionMarketInterest::new(1, 1),
        widest_tick,
        widest_band,
        Price::from_raw(0),
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap()
    .unwrap();
    assert_eq!(widest_market.price(), Price::from_raw(0));
}

#[test]
fn one_sided_interest_has_no_clearing_and_u128_maximum_market_totals_are_exact() {
    assert_eq!(
        bounded(
            &[],
            &[],
            u128::MAX,
            0,
            band(-10, 10),
            0,
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        ),
        None
    );
    let maximum = bounded(
        &[],
        &[],
        u128::MAX,
        u128::MAX,
        band(-10, 10),
        0,
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap();
    assert_eq!(maximum.executable_quantity(), u128::MAX);
    assert_eq!(maximum.buy_quantity(), u128::MAX);
    assert_eq!(maximum.sell_quantity(), u128::MAX);
}

#[test]
fn mixed_market_discovery_reconciles_to_order_level_allocation() {
    let clearing = bounded(
        &[level(105, 5), level(95, 10)],
        &[level(100, 8)],
        3,
        2,
        band(90, 110),
        102,
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap();
    let bids = [
        AuctionOrder::new(
            OrderId::new(1).unwrap(),
            AuctionOrderConstraint::Market,
            Quantity::new(3).unwrap(),
            0,
            1,
        ),
        AuctionOrder::new(
            OrderId::new(2).unwrap(),
            AuctionOrderConstraint::Limit(Price::from_raw(105)),
            Quantity::new(5).unwrap(),
            0,
            2,
        ),
        AuctionOrder::new(
            OrderId::new(3).unwrap(),
            AuctionOrderConstraint::Limit(Price::from_raw(95)),
            Quantity::new(10).unwrap(),
            0,
            3,
        ),
    ];
    let asks = [
        AuctionOrder::new(
            OrderId::new(10).unwrap(),
            AuctionOrderConstraint::Market,
            Quantity::new(2).unwrap(),
            0,
            1,
        ),
        AuctionOrder::new(
            OrderId::new(11).unwrap(),
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            Quantity::new(8).unwrap(),
            0,
            2,
        ),
    ];

    let plan = allocate_clearing_price_time(
        &bids,
        &asks,
        grid(),
        clearing,
        AuctionAllocationLimits::new(3).unwrap(),
    )
    .unwrap();

    assert_eq!(plan.executable_quantity(), 8);
    assert_eq!(
        plan.buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(1, 3), (2, 5)]
    );
    assert_eq!(
        plan.sell_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(10, 2), (11, 6)]
    );
}

#[derive(Clone, Copy)]
struct Generator(u64);

impl Generator {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn bounded(&mut self, upper: u64) -> u64 {
        self.next() % upper
    }
}

fn exhaustive_bounded_discovery(
    bids: &[AuctionLevel],
    asks: &[AuctionLevel],
    market: AuctionMarketInterest,
    price_grid: AuctionPriceGrid,
    price_band: AuctionPriceBand,
    reference: Price,
    policy: AuctionPricePolicy,
) -> Option<AuctionClearing> {
    let tick = i64::try_from(price_grid.tick_size()).unwrap();
    let mut price = price_band.minimum();
    let mut best = None;
    loop {
        let buy_quantity = bids
            .iter()
            .filter(|level| level.price() >= price)
            .map(|level| level.quantity())
            .sum::<u128>()
            + market.buy_quantity();
        let sell_quantity = asks
            .iter()
            .filter(|level| level.price() <= price)
            .map(|level| level.quantity())
            .sum::<u128>()
            + market.sell_quantity();
        let candidate = AuctionClearing::from_quantities(price, buy_quantity, sell_quantity);
        if candidate.executable_quantity() != 0
            && best.is_none_or(|current| candidate.is_better_than(current, reference, policy))
        {
            best = Some(candidate);
        }
        if price == price_band.maximum() {
            break;
        }
        price = Price::from_raw(price.raw().checked_add(tick).unwrap());
    }
    best
}

#[test]
fn bounded_linear_sweep_matches_exhaustive_grid_enumeration() {
    let policies = [
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
        AuctionPricePolicy::REFERENCE_THEN_HIGHER,
        AuctionPricePolicy::PRESSURE_THEN_REFERENCE_LOWER,
        AuctionPricePolicy::PRESSURE_THEN_REFERENCE_HIGHER,
    ];
    let mut generator = Generator(0x9d52_38cf_78b4_a061);
    for _ in 0..20_000 {
        let tick_size = [1_u64, 2, 3, 5][usize::try_from(generator.bounded(4)).unwrap()];
        let price_grid = AuctionPriceGrid::new(tick_size).unwrap();
        let tick = i64::try_from(tick_size).unwrap();
        let lower_index = i64::try_from(generator.bounded(13)).unwrap() - 6;
        let width = i64::try_from(generator.bounded(7)).unwrap();
        let price_band = AuctionPriceBand::new(
            price_grid,
            Price::from_raw(lower_index * tick),
            Price::from_raw((lower_index + width) * tick),
        )
        .unwrap();
        let bid_count = usize::try_from(generator.bounded(7)).unwrap();
        let ask_count = usize::try_from(generator.bounded(7)).unwrap();
        let mut bid_prices: Vec<_> = (0..bid_count)
            .map(|_| (i64::try_from(generator.bounded(21)).unwrap() - 10) * tick)
            .collect();
        bid_prices.sort_unstable_by(|left, right| right.cmp(left));
        bid_prices.dedup();
        let mut ask_prices: Vec<_> = (0..ask_count)
            .map(|_| (i64::try_from(generator.bounded(21)).unwrap() - 10) * tick)
            .collect();
        ask_prices.sort_unstable();
        ask_prices.dedup();
        let bids: Vec<_> = bid_prices
            .into_iter()
            .map(|price| level(price, u128::from(generator.bounded(20) + 1)))
            .collect();
        let asks: Vec<_> = ask_prices
            .into_iter()
            .map(|price| level(price, u128::from(generator.bounded(20) + 1)))
            .collect();
        let market = AuctionMarketInterest::new(
            u128::from(generator.bounded(20)),
            u128::from(generator.bounded(20)),
        );
        let reference =
            Price::from_raw((i64::try_from(generator.bounded(25)).unwrap() - 12) * tick);
        for policy in policies {
            assert_eq!(
                discover_bounded_clearing_price(
                    &bids, &asks, market, price_grid, price_band, reference, policy,
                )
                .unwrap(),
                exhaustive_bounded_discovery(
                    &bids, &asks, market, price_grid, price_band, reference, policy,
                ),
                "bids={bids:?} asks={asks:?} market={market:?} band={price_band:?} reference={reference:?} policy={policy:?}"
            );
        }
    }
}
