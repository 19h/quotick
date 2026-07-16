use quotick::auction::{
    AuctionAllocationError, AuctionAllocationLimits, AuctionLevel, AuctionOrder,
    AuctionOrderConstraint, AuctionPriceGrid, AuctionPricePolicy, AuctionPriorityClass,
    allocate_clearing_price_time, allocate_clearing_pro_rata_time, discover_clearing_price,
};
use quotick::{OrderId, Price, Quantity, Side};

fn order(
    id: u64,
    constraint: AuctionOrderConstraint,
    quantity: u64,
    priority_class: u16,
    priority_sequence: u64,
) -> AuctionOrder {
    AuctionOrder::new(
        OrderId::new(id).unwrap(),
        constraint,
        Quantity::new(quantity).unwrap(),
        AuctionPriorityClass::new(priority_class),
        priority_sequence,
    )
}

fn limit(
    id: u64,
    price: i64,
    quantity: u64,
    priority_class: u16,
    priority_sequence: u64,
) -> AuctionOrder {
    order(
        id,
        AuctionOrderConstraint::Limit(Price::from_raw(price)),
        quantity,
        priority_class,
        priority_sequence,
    )
}

fn market(id: u64, quantity: u64, priority_class: u16, priority_sequence: u64) -> AuctionOrder {
    order(
        id,
        AuctionOrderConstraint::Market,
        quantity,
        priority_class,
        priority_sequence,
    )
}

fn level(price: i64, quantity: u128) -> AuctionLevel {
    AuctionLevel::new(Price::from_raw(price), quantity).unwrap()
}

fn grid() -> AuctionPriceGrid {
    AuctionPriceGrid::new(1).unwrap()
}

fn limits() -> AuctionAllocationLimits {
    AuctionAllocationLimits::new(128).unwrap()
}

#[test]
fn better_price_then_class_time_and_id_allocate_the_long_side() {
    let bids = [
        limit(1, 110, 3, 9, 100),
        limit(2, 100, 2, 0, 10),
        limit(3, 100, 2, 1, 5),
        limit(4, 100, 2, 1, 5),
        limit(5, 90, 50, 0, 0),
    ];
    let asks = [limit(10, 90, 5, 0, 1), limit(11, 100, 3, 0, 2)];
    let clearing = discover_clearing_price(
        &[level(110, 3), level(100, 6), level(90, 50)],
        &[level(90, 5), level(100, 3)],
        grid(),
        Price::from_raw(100),
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap()
    .unwrap();
    assert_eq!(clearing.price(), Price::from_raw(100));
    assert_eq!(clearing.executable_quantity(), 8);

    let plan = allocate_clearing_price_time(&bids, &asks, grid(), clearing, limits()).unwrap();

    assert_eq!(plan.clearing(), clearing);
    assert_eq!(plan.executable_quantity(), 8);
    assert_eq!(
        plan.buy_fills()
            .iter()
            .map(|fill| (
                fill.source_index(),
                fill.order_id().get(),
                fill.quantity_lots()
            ))
            .collect::<Vec<_>>(),
        vec![(0, 1, 3), (1, 2, 2), (2, 3, 2), (3, 4, 1)]
    );
    assert_eq!(
        plan.sell_fills()
            .iter()
            .map(|fill| (
                fill.source_index(),
                fill.order_id().get(),
                fill.quantity_lots()
            ))
            .collect::<Vec<_>>(),
        vec![(0, 10, 5), (1, 11, 3)]
    );
}

#[test]
fn market_interest_precedes_better_and_at_price_limits() {
    let bids = [
        market(1, 2, 0, 9),
        market(2, 1, 1, 1),
        limit(3, 110, 2, 0, 1),
        limit(4, 100, 5, 0, 2),
    ];
    let asks = [market(10, 4, 0, 1), limit(11, 90, 3, 0, 2)];
    let clearing = quotick::auction::AuctionClearing::from_quantities(Price::from_raw(100), 10, 7);

    let plan = allocate_clearing_price_time(&bids, &asks, grid(), clearing, limits()).unwrap();

    assert_eq!(
        plan.buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(1, 2), (2, 1), (3, 2), (4, 2)]
    );
    assert_eq!(
        plan.sell_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(10, 4), (11, 3)]
    );
}

#[test]
fn short_side_fills_completely_and_zero_quantity_never_appears() {
    let bids = [limit(1, 100, 2, 0, 1)];
    let asks = [limit(10, 100, 5, 0, 1)];
    let clearing = quotick::auction::AuctionClearing::from_quantities(Price::from_raw(100), 2, 5);

    let plan = allocate_clearing_price_time(&bids, &asks, grid(), clearing, limits()).unwrap();

    assert_eq!(plan.buy_fills()[0].quantity_lots(), 2);
    assert_eq!(plan.sell_fills()[0].quantity_lots(), 2);
    assert!(
        plan.buy_fills()
            .iter()
            .chain(plan.sell_fills())
            .all(|fill| fill.quantity_lots() != 0)
    );
}

#[test]
fn executable_quantity_can_exceed_one_order_level_u64() {
    let bids = [limit(1, 100, u64::MAX, 0, 1), limit(2, 100, u64::MAX, 0, 2)];
    let asks = [
        limit(10, 100, u64::MAX, 0, 1),
        limit(11, 100, u64::MAX, 0, 2),
    ];
    let total = u128::from(u64::MAX) * 2;
    let clearing =
        quotick::auction::AuctionClearing::from_quantities(Price::from_raw(100), total, total);

    let plan = allocate_clearing_price_time(&bids, &asks, grid(), clearing, limits()).unwrap();

    assert_eq!(plan.executable_quantity(), total);
    assert_eq!(
        plan.buy_fills()
            .iter()
            .map(|fill| u128::from(fill.quantity_lots()))
            .sum::<u128>(),
        total
    );
    assert_eq!(
        plan.sell_fills()
            .iter()
            .map(|fill| u128::from(fill.quantity_lots()))
            .sum::<u128>(),
        total
    );
}

#[test]
fn pro_rata_fills_only_the_marginal_price_class_and_assigns_fifo_residual() {
    let bids = [
        market(1, 2, 0, 1),
        limit(2, 110, 3, 0, 2),
        limit(3, 100, 10, 0, 3),
        limit(4, 100, 20, 0, 4),
        limit(5, 100, 30, 0, 5),
        limit(6, 100, 100, 1, 6),
    ];
    let asks = [limit(10, 100, 18, 0, 1)];
    let clearing =
        quotick::auction::AuctionClearing::from_quantities(Price::from_raw(100), 165, 18);

    let plan =
        allocate_clearing_pro_rata_time(&bids, &asks, grid(), clearing, 1, limits()).unwrap();

    assert_eq!(
        plan.buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(1, 2), (2, 3), (3, 3), (4, 4), (5, 6)]
    );
    assert_eq!(plan.sell_fills()[0].quantity_lots(), 18);
}

#[test]
fn pro_rata_uses_the_allocation_quantum_and_never_emits_off_grid_fills() {
    let bids = [
        limit(1, 100, 10, 0, 1),
        limit(2, 100, 20, 0, 2),
        limit(3, 100, 30, 0, 3),
    ];
    let asks = [limit(10, 100, 25, 0, 1)];
    let clearing = quotick::auction::AuctionClearing::from_quantities(Price::from_raw(100), 60, 25);

    let plan =
        allocate_clearing_pro_rata_time(&bids, &asks, grid(), clearing, 5, limits()).unwrap();

    assert_eq!(
        plan.buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(1, 5), (2, 10), (3, 10)]
    );
    assert!(
        plan.buy_fills()
            .iter()
            .chain(plan.sell_fills())
            .all(|fill| fill.quantity_lots() % 5 == 0)
    );
}

#[test]
fn pro_rata_handles_products_wider_than_u128_without_approximation() {
    let bids = [limit(1, 100, u64::MAX, 0, 1), limit(2, 100, u64::MAX, 0, 2)];
    let asks = [limit(10, 100, u64::MAX, 0, 1)];
    let clearing = quotick::auction::AuctionClearing::from_quantities(
        Price::from_raw(100),
        u128::from(u64::MAX) * 2,
        u128::from(u64::MAX),
    );

    let plan =
        allocate_clearing_pro_rata_time(&bids, &asks, grid(), clearing, 1, limits()).unwrap();

    assert_eq!(plan.buy_fills()[0].quantity_lots(), 1_u64 << 63);
    assert_eq!(plan.buy_fills()[1].quantity_lots(), (1_u64 << 63) - 1);
    assert_eq!(
        plan.buy_fills()
            .iter()
            .map(|fill| u128::from(fill.quantity_lots()))
            .sum::<u128>(),
        u128::from(u64::MAX)
    );
}

#[test]
fn pro_rata_rejects_zero_quantum_and_off_grid_order_quantity() {
    let clearing = quotick::auction::AuctionClearing::from_quantities(Price::from_raw(100), 6, 5);
    let bids = [limit(1, 100, 6, 0, 1)];
    let asks = [limit(10, 100, 5, 0, 1)];
    assert_eq!(
        allocate_clearing_pro_rata_time(&bids, &asks, grid(), clearing, 0, limits()),
        Err(AuctionAllocationError::InvalidAllocationQuantum(0))
    );
    assert_eq!(
        allocate_clearing_pro_rata_time(&bids, &asks, grid(), clearing, 5, limits()),
        Err(AuctionAllocationError::OrderQuantityOffAllocationGrid {
            side: Side::Buy,
            index: 0,
            quantity_lots: 6,
            allocation_quantum_lots: 5,
        })
    );
}

#[test]
fn malformed_market_price_class_time_and_id_priority_is_rejected() {
    assert_eq!(
        AuctionAllocationLimits::new(0),
        Err(AuctionAllocationError::InvalidOrderLimit(0))
    );
    let clearing = quotick::auction::AuctionClearing::from_quantities(Price::from_raw(100), 2, 2);
    let valid_ask = [limit(10, 100, 2, 0, 1)];

    assert_eq!(
        allocate_clearing_price_time(
            &[limit(1, 100, 1, 0, 1), limit(2, 110, 1, 0, 2)],
            &valid_ask,
            grid(),
            clearing,
            limits(),
        ),
        Err(AuctionAllocationError::NonCanonicalOrders {
            side: Side::Buy,
            index: 1,
        })
    );
    assert_eq!(
        allocate_clearing_price_time(
            &[limit(1, 100, 2, 0, 1)],
            &[limit(10, 100, 1, 0, 2), limit(11, 100, 1, 0, 1)],
            grid(),
            clearing,
            limits(),
        ),
        Err(AuctionAllocationError::NonCanonicalOrders {
            side: Side::Sell,
            index: 1,
        })
    );
    assert_eq!(
        allocate_clearing_price_time(
            &[limit(2, 100, 1, 0, 1), limit(1, 100, 1, 0, 1)],
            &valid_ask,
            grid(),
            clearing,
            limits(),
        ),
        Err(AuctionAllocationError::NonCanonicalOrders {
            side: Side::Buy,
            index: 1,
        })
    );
    assert_eq!(
        allocate_clearing_price_time(
            &[limit(1, 100, 1, 0, 2), limit(2, 100, 1, 0, 1)],
            &valid_ask,
            grid(),
            clearing,
            limits(),
        ),
        Err(AuctionAllocationError::NonCanonicalOrders {
            side: Side::Buy,
            index: 1,
        })
    );
    assert_eq!(
        allocate_clearing_price_time(
            &[limit(1, 100, 1, 0, 1), market(2, 1, 0, 2)],
            &valid_ask,
            grid(),
            clearing,
            limits(),
        ),
        Err(AuctionAllocationError::NonCanonicalOrders {
            side: Side::Buy,
            index: 1,
        })
    );
}

#[test]
fn grid_and_operational_limit_violations_are_rejected() {
    let clearing = quotick::auction::AuctionClearing::from_quantities(Price::from_raw(100), 2, 2);
    let valid_ask = [limit(10, 100, 2, 0, 1)];
    let five = AuctionPriceGrid::new(5).unwrap();
    assert_eq!(
        allocate_clearing_price_time(
            &[limit(1, 101, 2, 0, 1)],
            &valid_ask,
            five,
            clearing,
            limits(),
        ),
        Err(AuctionAllocationError::LimitPriceOffGrid {
            side: Side::Buy,
            index: 0,
        })
    );
    let off_grid_clearing =
        quotick::auction::AuctionClearing::from_quantities(Price::from_raw(101), 2, 2);
    assert_eq!(
        allocate_clearing_price_time(
            &[limit(1, 100, 2, 0, 1)],
            &valid_ask,
            five,
            off_grid_clearing,
            limits(),
        ),
        Err(AuctionAllocationError::ClearingPriceOffGrid)
    );

    let one = AuctionAllocationLimits::new(1).unwrap();
    assert_eq!(
        allocate_clearing_price_time(
            &[limit(1, 100, 1, 0, 1), limit(2, 100, 1, 0, 2)],
            &valid_ask,
            grid(),
            clearing,
            one,
        ),
        Err(AuctionAllocationError::OrderLimitExceeded {
            side: Side::Buy,
            count: 2,
            maximum: 1,
        })
    );
}

#[test]
fn eligible_aggregate_must_equal_the_discovery_result() {
    let clearing = quotick::auction::AuctionClearing::from_quantities(Price::from_raw(100), 3, 2);
    assert_eq!(
        allocate_clearing_price_time(
            &[limit(1, 100, 2, 0, 1)],
            &[limit(10, 100, 2, 0, 1)],
            grid(),
            clearing,
            limits(),
        ),
        Err(AuctionAllocationError::ClearingQuantityMismatch {
            expected_buy: 3,
            observed_buy: 2,
            expected_sell: 2,
            observed_sell: 2,
        })
    );

    let zero = quotick::auction::AuctionClearing::from_quantities(Price::from_raw(100), 0, 2);
    assert_eq!(
        allocate_clearing_price_time(&[], &[limit(10, 100, 2, 0, 1)], grid(), zero, limits()),
        Err(AuctionAllocationError::ZeroExecutableQuantity)
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

fn reference_fills(
    orders: &[AuctionOrder],
    side: Side,
    clearing_price: Price,
    mut remaining: u128,
) -> Vec<(usize, u64, u64)> {
    let mut fills = Vec::new();
    for (index, order) in orders.iter().copied().enumerate() {
        let eligible = match order.constraint() {
            AuctionOrderConstraint::Market => true,
            AuctionOrderConstraint::Limit(price) => match side {
                Side::Buy => price >= clearing_price,
                Side::Sell => price <= clearing_price,
            },
        };
        if !eligible || remaining == 0 {
            continue;
        }
        let quantity = u128::from(order.quantity().lots()).min(remaining);
        fills.push((
            index,
            order.order_id().get(),
            u64::try_from(quantity).unwrap(),
        ));
        remaining -= quantity;
    }
    assert_eq!(remaining, 0);
    fills
}

#[test]
fn linear_allocator_matches_a_literal_priority_walk() {
    let mut generator = Generator(0xf7c2_18a9_0d53_4be1);
    for _ in 0..20_000 {
        let bid_count = usize::try_from(generator.bounded(12) + 1).unwrap();
        let ask_count = usize::try_from(generator.bounded(12) + 1).unwrap();
        let mut bids = Vec::with_capacity(bid_count);
        let mut asks = Vec::with_capacity(ask_count);
        for index in 0..bid_count {
            let price = 105 - i64::try_from(index / 3).unwrap() * 5;
            bids.push(limit(
                u64::try_from(index + 1).unwrap(),
                price,
                generator.bounded(20) + 1,
                u16::try_from(index % 3).unwrap(),
                u64::try_from(index).unwrap(),
            ));
        }
        for index in 0..ask_count {
            let price = 95 + i64::try_from(index / 3).unwrap() * 5;
            asks.push(limit(
                1_000 + u64::try_from(index).unwrap(),
                price,
                generator.bounded(20) + 1,
                u16::try_from(index % 3).unwrap(),
                u64::try_from(index).unwrap(),
            ));
        }
        let clearing_price = Price::from_raw(100);
        let buy_quantity: u128 = bids
            .iter()
            .filter(|order| match order.constraint() {
                AuctionOrderConstraint::Market => true,
                AuctionOrderConstraint::Limit(price) => price >= clearing_price,
            })
            .map(|order| u128::from(order.quantity().lots()))
            .sum();
        let sell_quantity: u128 = asks
            .iter()
            .filter(|order| match order.constraint() {
                AuctionOrderConstraint::Market => true,
                AuctionOrderConstraint::Limit(price) => price <= clearing_price,
            })
            .map(|order| u128::from(order.quantity().lots()))
            .sum();
        let clearing = quotick::auction::AuctionClearing::from_quantities(
            clearing_price,
            buy_quantity,
            sell_quantity,
        );
        let plan = allocate_clearing_price_time(
            &bids,
            &asks,
            grid(),
            clearing,
            AuctionAllocationLimits::new(12).unwrap(),
        )
        .unwrap();
        let target = clearing.executable_quantity();
        assert_eq!(
            plan.buy_fills()
                .iter()
                .map(|fill| (
                    fill.source_index(),
                    fill.order_id().get(),
                    fill.quantity_lots()
                ))
                .collect::<Vec<_>>(),
            reference_fills(&bids, Side::Buy, clearing_price, target),
        );
        assert_eq!(
            plan.sell_fills()
                .iter()
                .map(|fill| (
                    fill.source_index(),
                    fill.order_id().get(),
                    fill.quantity_lots()
                ))
                .collect::<Vec<_>>(),
            reference_fills(&asks, Side::Sell, clearing_price, target),
        );
    }
}

#[test]
fn pro_rata_allocator_matches_direct_small_integer_arithmetic() {
    let mut generator = Generator(0x5e82_3ac7_1d49_b60f);
    for _ in 0..20_000 {
        let order_count = usize::try_from(generator.bounded(12) + 2).unwrap();
        let mut bids = Vec::with_capacity(order_count);
        let mut total = 0_u128;
        for index in 0..order_count {
            let quantity = generator.bounded(1_000_000) + 1;
            total += u128::from(quantity);
            bids.push(limit(
                u64::try_from(index + 1).unwrap(),
                100,
                quantity,
                0,
                u64::try_from(index).unwrap(),
            ));
        }
        let executable = u128::from(generator.next()) % (total - 1) + 1;
        let asks = [limit(1_000, 100, u64::try_from(executable).unwrap(), 0, 0)];
        let clearing = quotick::auction::AuctionClearing::from_quantities(
            Price::from_raw(100),
            total,
            executable,
        );

        let plan = allocate_clearing_pro_rata_time(
            &bids,
            &asks,
            grid(),
            clearing,
            1,
            AuctionAllocationLimits::new(order_count).unwrap(),
        )
        .unwrap();

        let mut expected = bids
            .iter()
            .map(|order| u128::from(order.quantity().lots()) * executable / total)
            .collect::<Vec<_>>();
        let allocated = expected.iter().sum::<u128>();
        for quantity in expected
            .iter_mut()
            .take(usize::try_from(executable - allocated).unwrap())
        {
            *quantity += 1;
        }
        assert_eq!(
            plan.buy_fills()
                .iter()
                .map(|fill| (fill.source_index(), u128::from(fill.quantity_lots())))
                .collect::<Vec<_>>(),
            expected
                .iter()
                .copied()
                .enumerate()
                .filter(|(_, quantity)| *quantity != 0)
                .collect::<Vec<_>>()
        );
    }
}
