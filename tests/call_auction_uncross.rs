use quotick::auction::{AuctionAllocationPolicy, AuctionOrderConstraint, AuctionPricePolicy};
use quotick::auction_book::{
    CallAuctionBook, CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionCommitError,
    CallAuctionOrder, CallAuctionPrepareError, CallAuctionRemainderPolicy,
    CallAuctionSelfTradePolicy, CallAuctionUncrossPolicy,
};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::{
    AccountId, AssetId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn instrument() -> InstrumentId {
    InstrumentId::new(17).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(4).unwrap()
}

fn definition_with_quantities(quantity: QuantityRules) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("UNCROSS").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 5, Price::from_raw(-100), Price::from_raw(200)).unwrap(),
        quantity,
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Halted,
    })
    .unwrap()
}

fn definition() -> InstrumentDefinition {
    definition_with_quantities(QuantityRules::new(1, 1, 1_000).unwrap())
}

fn limits(active: usize, accepted: usize) -> CallAuctionBookLimits {
    limits_with_prepared(active, accepted, 2)
}

fn limits_with_prepared(active: usize, accepted: usize, prepared: usize) -> CallAuctionBookLimits {
    CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: active,
        max_price_levels_per_side: active,
        max_accepted_order_ids: accepted,
        max_prepared_uncrosses: prepared,
    })
    .unwrap()
}

fn order(
    id: u64,
    owner: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u64,
) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(id).unwrap(),
        account(owner),
        instrument(),
        version(),
        side,
        constraint,
        Quantity::new(quantity).unwrap(),
        quotick::auction::AuctionPriorityClass::HIGHEST,
    )
}

fn policy(remainder: CallAuctionRemainderPolicy) -> CallAuctionUncrossPolicy {
    allocation_policy(AuctionAllocationPolicy::PriceTime, remainder)
}

fn allocation_policy(
    allocation: AuctionAllocationPolicy,
    remainder: CallAuctionRemainderPolicy,
) -> CallAuctionUncrossPolicy {
    CallAuctionUncrossPolicy::new(allocation, remainder, CallAuctionSelfTradePolicy::Permit)
}

fn indicative(
    book: &mut CallAuctionBook,
    reference: i64,
) -> quotick::auction_book::CallAuctionIndicative {
    book.indicative_clearing(
        book.instrument_price_band(),
        Price::from_raw(reference),
        AuctionPricePolicy::REFERENCE_THEN_LOWER,
    )
    .unwrap()
    .unwrap()
}

#[test]
fn prepare_pairs_priority_fills_and_commit_removes_every_filled_order_atomically() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(8, 16)).unwrap();
    book.admit(order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 4))
        .unwrap();
    book.admit(order(
        2,
        2,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(105)),
        6,
    ))
    .unwrap();
    book.admit(order(3, 1, Side::Sell, AuctionOrderConstraint::Market, 3))
        .unwrap();
    book.admit(order(
        4,
        4,
        Side::Sell,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        7,
    ))
    .unwrap();

    let indicative = indicative(&mut book, 100);
    let prepared = book
        .prepare_uncross(indicative, policy(CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    assert_eq!(book.active_order_count(), 4);
    assert_eq!(book.state_revision(), 4);
    assert_eq!(book.next_trade_id(), 1);
    assert!(prepared.cancellations().is_empty());
    assert_eq!(
        prepared
            .trades()
            .iter()
            .map(|trade| {
                (
                    trade.trade_id().get(),
                    trade.buy_order_id().get(),
                    trade.sell_order_id().get(),
                    trade.quantity().lots(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(1, 1, 3, 3), (2, 1, 4, 1), (3, 2, 4, 6)]
    );
    assert_eq!(prepared.trades()[0].buy_account_id(), account(1));
    assert_eq!(prepared.trades()[0].sell_account_id(), account(1));

    let result = book.commit_uncross(prepared).unwrap();
    assert_eq!(result.state_revision(), 5);
    assert_eq!(result.allocation_plan().executable_quantity(), 10);
    assert_eq!(result.trades().len(), 3);
    assert_eq!(book.active_order_count(), 0);
    assert_eq!(book.accepted_order_id_count(), 4);
    assert_eq!(book.next_trade_id(), 4);
    book.validate().unwrap();
}

#[test]
fn pro_rata_uncross_uses_instrument_quantity_increment_and_commits_exact_fills() {
    let definition = definition_with_quantities(QuantityRules::new(5, 5, 1_000).unwrap());
    let mut book = CallAuctionBook::try_with_limits(definition, limits(8, 16)).unwrap();
    book.admit(order(
        1,
        1,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        10,
    ))
    .unwrap();
    book.admit(order(
        2,
        2,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        20,
    ))
    .unwrap();
    book.admit(order(
        3,
        3,
        Side::Sell,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        15,
    ))
    .unwrap();

    let indicative = indicative(&mut book, 100);
    let prepared = book
        .prepare_uncross(
            indicative,
            allocation_policy(
                AuctionAllocationPolicy::ProRataTime,
                CallAuctionRemainderPolicy::RetainAll,
            ),
        )
        .unwrap();

    assert_eq!(
        prepared
            .allocation_plan()
            .buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(1, 5), (2, 10)]
    );
    assert_eq!(
        prepared
            .trades()
            .iter()
            .map(|trade| {
                (
                    trade.buy_order_id().get(),
                    trade.sell_order_id().get(),
                    trade.quantity().lots(),
                )
            })
            .collect::<Vec<_>>(),
        vec![(1, 3, 5), (2, 3, 10)]
    );
    let result = book.commit_uncross(prepared).unwrap();
    assert_eq!(
        result.policy().allocation(),
        AuctionAllocationPolicy::ProRataTime
    );
    assert_eq!(
        book.order(OrderId::new(1).unwrap())
            .unwrap()
            .quantity
            .lots(),
        5
    );
    assert_eq!(
        book.order(OrderId::new(2).unwrap())
            .unwrap()
            .quantity
            .lots(),
        10
    );
    assert!(book.order(OrderId::new(3).unwrap()).is_none());
    book.validate().unwrap();
}

fn remainder_book() -> CallAuctionBook {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(8, 16)).unwrap();
    book.admit(order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 10))
        .unwrap();
    book.admit(order(
        2,
        2,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        5,
    ))
    .unwrap();
    book.admit(order(3, 3, Side::Sell, AuctionOrderConstraint::Market, 7))
        .unwrap();
    book
}

#[test]
fn remainder_policies_distinguish_market_limit_and_complete_cancellation() {
    let cases = [
        (
            CallAuctionRemainderPolicy::RetainAll,
            vec![],
            vec![(1, 3), (2, 5)],
        ),
        (
            CallAuctionRemainderPolicy::CancelMarket,
            vec![(1, 3)],
            vec![(2, 5)],
        ),
        (
            CallAuctionRemainderPolicy::CancelAll,
            vec![(1, 3), (2, 5)],
            vec![],
        ),
    ];
    for (remainder, expected_cancellations, expected_active) in cases {
        let mut book = remainder_book();
        let indicative = indicative(&mut book, 150);
        let prepared = book.prepare_uncross(indicative, policy(remainder)).unwrap();
        assert_eq!(
            prepared
                .cancellations()
                .iter()
                .map(|cancellation| {
                    (
                        cancellation.order_id().get(),
                        cancellation.quantity().lots(),
                    )
                })
                .collect::<Vec<_>>(),
            expected_cancellations
        );
        let result = book.commit_uncross(prepared).unwrap();
        assert_eq!(result.policy().remainder(), remainder);
        assert_eq!(result.trades()[0].quantity().lots(), 7);
        for (id, quantity) in expected_active {
            assert_eq!(
                book.order(OrderId::new(id).unwrap())
                    .unwrap()
                    .quantity
                    .lots(),
                quantity
            );
        }
        assert_eq!(
            book.active_order_count(),
            match remainder {
                CallAuctionRemainderPolicy::RetainAll => 2,
                CallAuctionRemainderPolicy::CancelMarket => 1,
                CallAuctionRemainderPolicy::CancelAll => 0,
            }
        );
        book.validate().unwrap();
    }
}

#[test]
fn retained_partial_limit_keeps_priority_in_the_next_collection() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(8, 16)).unwrap();
    book.admit(order(
        1,
        1,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        10,
    ))
    .unwrap();
    book.admit(order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 6))
        .unwrap();
    let first = indicative(&mut book, 100);
    let first = book
        .prepare_uncross(first, policy(CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    book.commit_uncross(first).unwrap();
    assert_eq!(
        book.order(OrderId::new(1).unwrap())
            .unwrap()
            .quantity
            .lots(),
        4
    );

    book.admit(order(
        3,
        3,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        4,
    ))
    .unwrap();
    book.admit(order(4, 4, Side::Sell, AuctionOrderConstraint::Market, 4))
        .unwrap();
    let second = indicative(&mut book, 100);
    let second = book
        .prepare_uncross(second, policy(CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    assert_eq!(second.trades()[0].buy_order_id(), OrderId::new(1).unwrap());
    book.commit_uncross(second).unwrap();
    assert!(book.order(OrderId::new(1).unwrap()).is_none());
    assert_eq!(
        book.order(OrderId::new(3).unwrap())
            .unwrap()
            .quantity
            .lots(),
        4
    );
    book.validate().unwrap();
}

#[test]
fn partial_auction_leaves_may_fall_below_new_order_minimum() {
    let definition = definition_with_quantities(QuantityRules::new(5, 10, 1_000).unwrap());
    let mut book = CallAuctionBook::try_with_limits(definition, limits(4, 8)).unwrap();
    book.admit(order(
        1,
        1,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        15,
    ))
    .unwrap();
    book.admit(order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 10))
        .unwrap();
    let indicative = indicative(&mut book, 100);
    let prepared = book
        .prepare_uncross(indicative, policy(CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    book.commit_uncross(prepared).unwrap();
    assert_eq!(
        book.order(OrderId::new(1).unwrap())
            .unwrap()
            .quantity
            .lots(),
        5
    );
    book.validate().unwrap();
}

fn balanced_market_book() -> CallAuctionBook {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(8, 16)).unwrap();
    book.admit(order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 5))
        .unwrap();
    book.admit(order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 5))
        .unwrap();
    book
}

#[test]
fn constructor_owned_uncross_pool_is_bounded_reusable_and_drop_released() {
    let mut book =
        CallAuctionBook::try_with_limits(definition(), limits_with_prepared(8, 16, 1)).unwrap();
    book.admit(order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 5))
        .unwrap();
    book.admit(order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 5))
        .unwrap();
    let initial = book.resource_status();
    assert_eq!(initial.configured_prepared_uncrosses, 1);
    assert_eq!(initial.available_prepared_uncrosses, 1);
    assert!(initial.uncross_fill_capacity_per_side >= 8);
    assert!(initial.uncross_trade_capacity >= 8);
    assert!(initial.uncross_cancellation_capacity >= 8);

    let clearing = indicative(&mut book, 0);
    let prepared = book
        .prepare_uncross(clearing, policy(CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    assert_eq!(book.resource_status().available_prepared_uncrosses, 0);
    assert!(matches!(
        book.prepare_uncross(clearing, policy(CallAuctionRemainderPolicy::RetainAll)),
        Err(CallAuctionPrepareError::PreparationCapacityExhausted { maximum: 1 })
    ));
    drop(prepared);
    assert_eq!(book.resource_status().available_prepared_uncrosses, 1);

    let prepared = book
        .prepare_uncross(clearing, policy(CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    let result = book.commit_uncross(prepared).unwrap();
    assert_eq!(book.resource_status().available_prepared_uncrosses, 0);
    assert_eq!(result.trades().len(), 1);
    book.validate().unwrap();
    drop(result);
    let final_status = book.resource_status();
    assert_eq!(final_status.available_prepared_uncrosses, 1);
    assert_eq!(
        final_status.uncross_fill_capacity_per_side,
        initial.uncross_fill_capacity_per_side
    );
    assert_eq!(
        final_status.uncross_trade_capacity,
        initial.uncross_trade_capacity
    );
    assert_eq!(
        final_status.uncross_cancellation_capacity,
        initial.uncross_cancellation_capacity
    );
    book.validate().unwrap();
}

#[test]
fn concurrent_uncross_leases_are_isolated_and_returned_after_stale_commit() {
    let mut book = remainder_book();
    let clearing = indicative(&mut book, 150);
    let retain = book
        .prepare_uncross(clearing, policy(CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    let cancel = book
        .prepare_uncross(clearing, policy(CallAuctionRemainderPolicy::CancelAll))
        .unwrap();
    assert_eq!(book.resource_status().available_prepared_uncrosses, 0);
    assert!(retain.cancellations().is_empty());
    assert_eq!(cancel.cancellations().len(), 2);

    let retained_result = book.commit_uncross(retain).unwrap();
    assert_eq!(book.resource_status().available_prepared_uncrosses, 0);
    assert!(matches!(
        book.commit_uncross(cancel),
        Err(CallAuctionCommitError::StalePreparation {
            observed: 3,
            current: 4,
        })
    ));
    assert_eq!(book.resource_status().available_prepared_uncrosses, 1);
    drop(retained_result);
    assert_eq!(book.resource_status().available_prepared_uncrosses, 2);
    book.validate().unwrap();
}

#[test]
fn commit_rejects_foreign_and_stale_preparations_without_mutation() {
    let mut first = balanced_market_book();
    let foreign = indicative(&mut first, 0);
    let foreign = first
        .prepare_uncross(foreign, policy(CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    let mut second = balanced_market_book();
    assert_eq!(
        second.commit_uncross(foreign),
        Err(CallAuctionCommitError::ForeignPreparation)
    );
    assert_eq!(second.active_order_count(), 2);
    assert_eq!(second.next_trade_id(), 1);

    let stale = indicative(&mut first, 0);
    let stale = first
        .prepare_uncross(stale, policy(CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    first
        .admit(order(3, 3, Side::Buy, AuctionOrderConstraint::Market, 1))
        .unwrap();
    assert_eq!(
        first.commit_uncross(stale),
        Err(CallAuctionCommitError::StalePreparation {
            observed: 2,
            current: 3,
        })
    );
    assert_eq!(first.active_order_count(), 3);
    assert_eq!(first.next_trade_id(), 1);
    first.validate().unwrap();
    second.validate().unwrap();
}

#[derive(Clone, Copy)]
struct ModelOrder {
    id: OrderId,
    account: AccountId,
    quantity: u64,
}

fn next_random(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn model_fills(orders: &[ModelOrder], mut remaining: u128) -> Vec<(ModelOrder, u64)> {
    let mut fills = Vec::new();
    for order in orders {
        if remaining == 0 {
            break;
        }
        let quantity = u128::from(order.quantity).min(remaining);
        fills.push((*order, u64::try_from(quantity).unwrap()));
        remaining -= quantity;
    }
    fills
}

fn model_pairs(
    buys: &[(ModelOrder, u64)],
    sells: &[(ModelOrder, u64)],
) -> Vec<(OrderId, OrderId, u64)> {
    let mut pairs = Vec::new();
    let mut buy_index = 0_usize;
    let mut sell_index = 0_usize;
    let mut buy_remaining = buys[0].1;
    let mut sell_remaining = sells[0].1;
    while buy_index < buys.len() && sell_index < sells.len() {
        let quantity = buy_remaining.min(sell_remaining);
        pairs.push((buys[buy_index].0.id, sells[sell_index].0.id, quantity));
        buy_remaining -= quantity;
        sell_remaining -= quantity;
        if buy_remaining == 0 {
            buy_index += 1;
            buy_remaining = buys.get(buy_index).map_or(0, |fill| fill.1);
        }
        if sell_remaining == 0 {
            sell_index += 1;
            sell_remaining = sells.get(sell_index).map_or(0, |fill| fill.1);
        }
    }
    pairs
}

fn verify_active_market_remainders(
    book: &CallAuctionBook,
    orders: &[ModelOrder],
    fills: &[(ModelOrder, u64)],
    remainder_policy: CallAuctionRemainderPolicy,
) {
    for order in orders {
        let filled = fills
            .iter()
            .find(|(candidate, _)| candidate.id == order.id)
            .map_or(0, |(_, quantity)| *quantity);
        let remaining = order.quantity - filled;
        let expected =
            if remaining == 0 || remainder_policy != CallAuctionRemainderPolicy::RetainAll {
                None
            } else {
                Some(remaining)
            };
        assert_eq!(
            book.order(order.id)
                .map(|snapshot| snapshot.quantity.lots()),
            expected
        );
    }
}

#[test]
fn randomized_priority_pairing_and_post_state_match_literal_models() {
    let mut rng = 0xd1b5_4a32_d192_ed03_u64;
    for case in 0..10_000 {
        let buy_count = usize::try_from(next_random(&mut rng) % 5 + 1).unwrap();
        let sell_count = usize::try_from(next_random(&mut rng) % 5 + 1).unwrap();
        let mut book = CallAuctionBook::try_with_limits(definition(), limits(10, 10)).unwrap();
        let mut buys = Vec::new();
        let mut sells = Vec::new();
        let mut next_id = 1_u64;
        for _ in 0..buy_count {
            let model = ModelOrder {
                id: OrderId::new(next_id).unwrap(),
                account: account(next_random(&mut rng) % 3 + 1),
                quantity: next_random(&mut rng) % 50 + 1,
            };
            next_id += 1;
            book.admit(CallAuctionOrder::new(
                model.id,
                model.account,
                instrument(),
                version(),
                Side::Buy,
                AuctionOrderConstraint::Market,
                Quantity::new(model.quantity).unwrap(),
                quotick::auction::AuctionPriorityClass::HIGHEST,
            ))
            .unwrap();
            buys.push(model);
        }
        for _ in 0..sell_count {
            let model = ModelOrder {
                id: OrderId::new(next_id).unwrap(),
                account: account(next_random(&mut rng) % 3 + 1),
                quantity: next_random(&mut rng) % 50 + 1,
            };
            next_id += 1;
            book.admit(CallAuctionOrder::new(
                model.id,
                model.account,
                instrument(),
                version(),
                Side::Sell,
                AuctionOrderConstraint::Market,
                Quantity::new(model.quantity).unwrap(),
                quotick::auction::AuctionPriorityClass::HIGHEST,
            ))
            .unwrap();
            sells.push(model);
        }
        let buy_total = buys
            .iter()
            .map(|order| u128::from(order.quantity))
            .sum::<u128>();
        let sell_total = sells
            .iter()
            .map(|order| u128::from(order.quantity))
            .sum::<u128>();
        let executable = buy_total.min(sell_total);
        let buy_fills = model_fills(&buys, executable);
        let sell_fills = model_fills(&sells, executable);
        let expected_pairs = model_pairs(&buy_fills, &sell_fills);
        let remainder = match next_random(&mut rng) % 3 {
            0 => CallAuctionRemainderPolicy::RetainAll,
            1 => CallAuctionRemainderPolicy::CancelMarket,
            _ => CallAuctionRemainderPolicy::CancelAll,
        };
        let indicative = indicative(&mut book, 0);
        let prepared = book.prepare_uncross(indicative, policy(remainder)).unwrap();
        assert_eq!(
            prepared
                .trades()
                .iter()
                .map(|trade| {
                    (
                        trade.buy_order_id(),
                        trade.sell_order_id(),
                        trade.quantity().lots(),
                    )
                })
                .collect::<Vec<_>>(),
            expected_pairs,
            "pairing case {case}"
        );
        let result = book.commit_uncross(prepared).unwrap();
        assert_eq!(
            result
                .trades()
                .iter()
                .map(|trade| u128::from(trade.quantity().lots()))
                .sum::<u128>(),
            executable,
            "volume case {case}"
        );
        verify_active_market_remainders(&book, &buys, &buy_fills, remainder);
        verify_active_market_remainders(&book, &sells, &sell_fills, remainder);
        book.validate().unwrap();
    }
}
