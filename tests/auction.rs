use quotick::auction::{
    AuctionClearing, AuctionError, AuctionFinalTieBreak, AuctionLevel, AuctionPressureRule,
    AuctionPriceGrid, AuctionPricePolicy, discover_clearing_price,
};
use quotick::{Price, Side};

fn level(price: i64, quantity: u128) -> AuctionLevel {
    AuctionLevel::new(Price::from_raw(price), quantity).unwrap()
}

fn grid() -> AuctionPriceGrid {
    AuctionPriceGrid::new(1).unwrap()
}

fn clearing(
    bids: &[AuctionLevel],
    asks: &[AuctionLevel],
    reference: i64,
    policy: AuctionPricePolicy,
) -> AuctionClearing {
    discover_clearing_price(bids, asks, grid(), Price::from_raw(reference), policy)
        .unwrap()
        .unwrap()
}

#[test]
fn maximizes_volume_then_minimizes_imbalance() {
    let bids = [level(110, 5), level(100, 10), level(90, 20)];
    let asks = [level(80, 4), level(90, 8), level(100, 20)];
    let result = clearing(&bids, &asks, 95, AuctionPricePolicy::REFERENCE_THEN_LOWER);

    assert_eq!(result.price(), Price::from_raw(100));
    assert_eq!(result.executable_quantity(), 15);
    assert_eq!(result.buy_quantity(), 15);
    assert_eq!(result.sell_quantity(), 32);
    assert_eq!(result.imbalance_side(), Some(Side::Sell));
    assert_eq!(result.imbalance_quantity(), 17);
}

#[test]
fn reference_price_inside_a_clearing_interval_is_a_real_candidate() {
    let bids = [level(110, 10)];
    let asks = [level(90, 10)];
    let result = clearing(&bids, &asks, 100, AuctionPricePolicy::REFERENCE_THEN_LOWER);

    assert_eq!(result.price(), Price::from_raw(100));
    assert_eq!(result.executable_quantity(), 10);
    assert_eq!(result.imbalance_side(), None);
    assert_eq!(result.imbalance_quantity(), 0);
}

#[test]
fn clearing_price_can_be_an_unquoted_tick_between_order_prices() {
    let bids = [level(10, 6), level(7, 17), level(-4, 4), level(-5, 11)];
    let asks = [
        level(-10, 6),
        level(-8, 1),
        level(-7, 16),
        level(5, 11),
        level(9, 14),
    ];
    let result = clearing(&bids, &asks, 9, AuctionPricePolicy::REFERENCE_THEN_LOWER);

    assert_eq!(result.price(), Price::from_raw(4));
    assert_eq!(result.executable_quantity(), 23);
    assert_eq!(result.buy_quantity(), 23);
    assert_eq!(result.sell_quantity(), 23);
    assert_eq!(result.imbalance_side(), None);
}

#[test]
fn optional_market_pressure_selects_the_extreme_on_the_imbalanced_side() {
    let buy_heavy_bids = [level(110, 15)];
    let asks = [level(90, 10)];
    let reference = clearing(
        &buy_heavy_bids,
        &asks,
        100,
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    );
    let pressure = clearing(
        &buy_heavy_bids,
        &asks,
        100,
        AuctionPricePolicy::PRESSURE_THEN_REFERENCE_LOWER,
    );
    assert_eq!(reference.price(), Price::from_raw(100));
    assert_eq!(pressure.price(), Price::from_raw(110));
    assert_eq!(pressure.imbalance_side(), Some(Side::Buy));

    let bids = [level(110, 10)];
    let sell_heavy_asks = [level(90, 15)];
    let pressure = clearing(
        &bids,
        &sell_heavy_asks,
        100,
        AuctionPricePolicy::PRESSURE_THEN_REFERENCE_HIGHER,
    );
    assert_eq!(pressure.price(), Price::from_raw(90));
    assert_eq!(pressure.imbalance_side(), Some(Side::Sell));
}

#[test]
fn empty_or_uncrossed_sides_have_no_clearing_price() {
    let policy = AuctionPricePolicy::REFERENCE_THEN_LOWER;
    assert_eq!(
        discover_clearing_price(&[], &[level(100, 1)], grid(), Price::from_raw(100), policy)
            .unwrap(),
        None
    );
    assert_eq!(
        discover_clearing_price(
            &[level(90, 1)],
            &[level(100, 1)],
            grid(),
            Price::from_raw(95),
            policy,
        )
        .unwrap(),
        None
    );
}

#[test]
fn level_and_book_shape_validation_is_fail_closed() {
    assert_eq!(
        AuctionPriceGrid::new(0),
        Err(AuctionError::InvalidTickSize(0))
    );
    assert_eq!(
        AuctionPriceGrid::new(u64::MAX),
        Err(AuctionError::InvalidTickSize(u64::MAX))
    );
    assert_eq!(
        AuctionLevel::new(Price::from_raw(1), 0),
        Err(AuctionError::ZeroLevelQuantity)
    );
    let policy = AuctionPricePolicy::REFERENCE_THEN_LOWER;
    assert_eq!(
        discover_clearing_price(
            &[level(100, 1), level(101, 1)],
            &[level(90, 1)],
            grid(),
            Price::from_raw(95),
            policy,
        ),
        Err(AuctionError::NonCanonicalLevels {
            side: Side::Buy,
            index: 1,
        })
    );
    assert_eq!(
        discover_clearing_price(
            &[level(100, 1)],
            &[level(90, 1), level(90, 2)],
            grid(),
            Price::from_raw(95),
            policy,
        ),
        Err(AuctionError::NonCanonicalLevels {
            side: Side::Sell,
            index: 1,
        })
    );
    assert_eq!(
        discover_clearing_price(
            &[level(100, u128::MAX), level(90, 1)],
            &[level(80, 1)],
            grid(),
            Price::from_raw(90),
            policy,
        ),
        Err(AuctionError::QuantityOverflow(Side::Buy))
    );
    assert_eq!(
        discover_clearing_price(
            &[level(100, 1)],
            &[level(80, u128::MAX), level(90, 1)],
            grid(),
            Price::from_raw(90),
            policy,
        ),
        Err(AuctionError::QuantityOverflow(Side::Sell))
    );
    let five = AuctionPriceGrid::new(5).unwrap();
    assert_eq!(five.tick_size(), 5);
    assert!(five.contains(Price::from_raw(-10)));
    assert!(!five.contains(Price::from_raw(-9)));
    assert_eq!(
        discover_clearing_price(
            &[level(101, 1)],
            &[level(90, 1)],
            five,
            Price::from_raw(95),
            policy,
        ),
        Err(AuctionError::PriceOffGrid {
            side: Side::Buy,
            index: 0,
        })
    );
    assert_eq!(
        discover_clearing_price(
            &[level(100, 1)],
            &[level(90, 1)],
            five,
            Price::from_raw(96),
            policy,
        ),
        Err(AuctionError::ReferencePriceOffGrid)
    );
}

#[test]
fn signed_extreme_prices_do_not_overflow_reference_distance() {
    let result = clearing(
        &[level(i64::MAX, 1)],
        &[level(i64::MIN, 1)],
        0,
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    );
    assert_eq!(result.price(), Price::from_raw(0));
    assert_eq!(result.executable_quantity(), 1);
}

#[test]
fn policy_composition_is_explicit_and_round_trips() {
    for pressure in [
        AuctionPressureRule::Ignore,
        AuctionPressureRule::FavorImbalance,
    ] {
        for final_tie in [
            AuctionFinalTieBreak::LowerPrice,
            AuctionFinalTieBreak::HigherPrice,
        ] {
            let policy = AuctionPricePolicy::new(pressure, final_tie);
            assert_eq!(policy.pressure_rule(), pressure);
            assert_eq!(policy.final_tie_break(), final_tie);
        }
    }

    let lower = AuctionClearing::from_quantities(Price::from_raw(99), 10, 10);
    let higher = AuctionClearing::from_quantities(Price::from_raw(101), 10, 10);
    let reference = Price::from_raw(100);
    assert!(lower.is_better_than(higher, reference, AuctionPricePolicy::REFERENCE_THEN_LOWER,));
    assert!(higher.is_better_than(lower, reference, AuctionPricePolicy::REFERENCE_THEN_HIGHER,));
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

fn reference_discovery(
    bids: &[AuctionLevel],
    asks: &[AuctionLevel],
    grid: AuctionPriceGrid,
    reference: Price,
    policy: AuctionPricePolicy,
) -> Option<AuctionClearing> {
    if bids.is_empty() || asks.is_empty() {
        return None;
    }
    let minimum = asks
        .first()
        .unwrap()
        .price()
        .raw()
        .min(reference.raw())
        .min(bids.last().unwrap().price().raw());
    let maximum = bids
        .first()
        .unwrap()
        .price()
        .raw()
        .max(reference.raw())
        .max(asks.last().unwrap().price().raw());
    let mut best = None;
    for raw in minimum..=maximum {
        let price = Price::from_raw(raw);
        if !grid.contains(price) {
            continue;
        }
        let buy_quantity = bids
            .iter()
            .filter(|level| level.price() >= price)
            .map(|level| level.quantity())
            .sum();
        let sell_quantity = asks
            .iter()
            .filter(|level| level.price() <= price)
            .map(|level| level.quantity())
            .sum();
        let candidate = AuctionClearing::from_quantities(price, buy_quantity, sell_quantity);
        if candidate.executable_quantity() != 0
            && best.is_none_or(|current| candidate.is_better_than(current, reference, policy))
        {
            best = Some(candidate);
        }
    }
    best
}

#[test]
fn linear_kernel_matches_exhaustive_integer_price_search() {
    let policies = [
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
        AuctionPricePolicy::REFERENCE_THEN_HIGHER,
        AuctionPricePolicy::PRESSURE_THEN_REFERENCE_LOWER,
        AuctionPricePolicy::PRESSURE_THEN_REFERENCE_HIGHER,
    ];
    let mut generator = Generator(0x4a17_91c2_80dd_31e5);
    for _ in 0..20_000 {
        let tick_size = [1_u64, 2, 3, 5][usize::try_from(generator.bounded(4)).unwrap()];
        let price_grid = AuctionPriceGrid::new(tick_size).unwrap();
        let tick = i64::try_from(tick_size).unwrap();
        let bid_count = usize::try_from(generator.bounded(6)).unwrap();
        let ask_count = usize::try_from(generator.bounded(6)).unwrap();
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
        let reference =
            Price::from_raw((i64::try_from(generator.bounded(21)).unwrap() - 10) * tick);
        for policy in policies {
            assert_eq!(
                discover_clearing_price(&bids, &asks, price_grid, reference, policy).unwrap(),
                reference_discovery(&bids, &asks, price_grid, reference, policy),
                "bids={bids:?} asks={asks:?} reference={reference:?} policy={policy:?}"
            );
        }
    }
}
