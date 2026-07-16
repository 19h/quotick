use std::collections::{BTreeMap, HashMap};

use quotick::auction::{
    AuctionAllocationPolicy, AuctionLevel, AuctionMarketInterest, AuctionOrderConstraint,
    AuctionPricePolicy, AuctionPriorityClass, discover_bounded_clearing_price,
};
use quotick::auction_book::{
    CallAuctionAdmissionError, CallAuctionAmendError, CallAuctionBook, CallAuctionBookLimits,
    CallAuctionBookLimitsError, CallAuctionBookLimitsSpec, CallAuctionCancelError,
    CallAuctionCapacity, CallAuctionConstructionError, CallAuctionIndicative,
    CallAuctionMassCancelError, CallAuctionOrder, CallAuctionPlanError, CallAuctionReplaceError,
};
use quotick::instrument::{
    AdmissionError, InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol,
    PriceRules, QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::MassCancelScope;
use quotick::{
    AccountId, AssetId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn instrument() -> InstrumentId {
    InstrumentId::new(7).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(3).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("AUCTION").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 5, Price::from_raw(-100), Price::from_raw(200)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Halted,
    })
    .unwrap()
}

fn limits(active: usize, levels: usize, accepted: usize) -> CallAuctionBookLimits {
    CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: active,
        max_price_levels_per_side: levels,
        max_accepted_order_ids: accepted,
        max_prepared_uncrosses: 2,
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
    order_with_class(id, owner, side, constraint, quantity, 0)
}

fn order_with_class(
    id: u64,
    owner: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u64,
    priority_class: u16,
) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(id).unwrap(),
        account(owner),
        instrument(),
        version(),
        side,
        constraint,
        Quantity::new(quantity).unwrap(),
        AuctionPriorityClass::new(priority_class),
    )
}

#[test]
fn authoritative_priority_classes_precede_time_and_bound_pro_rata_tiers() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(8, 4, 16)).unwrap();
    let price = AuctionOrderConstraint::Limit(Price::from_raw(100));
    let lower_class = book
        .admit(order_with_class(1, 1, Side::Buy, price, 100, 1))
        .unwrap();
    let first_top_class = book
        .admit(order_with_class(2, 2, Side::Buy, price, 10, 0))
        .unwrap();
    let second_top_class = book
        .admit(order_with_class(3, 3, Side::Buy, price, 20, 0))
        .unwrap();
    book.admit(order_with_class(4, 4, Side::Sell, price, 15, 0))
        .unwrap();

    assert_eq!(lower_class.priority_class, AuctionPriorityClass::new(1));
    assert_eq!(first_top_class.priority_class, AuctionPriorityClass::new(0));
    assert!(lower_class.priority_sequence < first_top_class.priority_sequence);

    let indicative = book
        .indicative_clearing(
            book.instrument_price_band(),
            Price::from_raw(100),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        )
        .unwrap()
        .unwrap();
    let price_time = book
        .allocation_plan(indicative, AuctionAllocationPolicy::PriceTime)
        .unwrap();
    assert_eq!(
        price_time
            .buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(2, 10), (3, 5)]
    );
    let pro_rata = book
        .allocation_plan(indicative, AuctionAllocationPolicy::ProRataTime)
        .unwrap();
    assert_eq!(
        pro_rata
            .buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(2, 5), (3, 10)]
    );
    assert!(
        pro_rata
            .buy_fills()
            .iter()
            .all(|fill| fill.order_id() != lower_class.order_id)
    );
    assert_eq!(
        book.order(second_top_class.order_id),
        Some(second_top_class)
    );
    book.validate().unwrap();
}

#[test]
fn limits_reject_zero_and_incoherent_bounds() {
    let make = |active, levels, accepted, prepared| {
        CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
            max_active_orders: active,
            max_price_levels_per_side: levels,
            max_accepted_order_ids: accepted,
            max_prepared_uncrosses: prepared,
        })
    };
    assert_eq!(
        make(0, 1, 1, 1),
        Err(CallAuctionBookLimitsError::ZeroActiveOrders)
    );
    assert_eq!(
        make(1, 0, 1, 1),
        Err(CallAuctionBookLimitsError::ZeroPriceLevels)
    );
    assert_eq!(
        make(1, 1, 0, 1),
        Err(CallAuctionBookLimitsError::ZeroAcceptedOrderIds)
    );
    assert_eq!(
        make(1, 2, 2, 1),
        Err(CallAuctionBookLimitsError::PriceLevelsExceedActiveOrders)
    );
    assert_eq!(
        make(2, 1, 1, 1),
        Err(CallAuctionBookLimitsError::ActiveOrdersExceedAcceptedOrderIds)
    );
    assert_eq!(
        make(1, 1, 1, 0),
        Err(CallAuctionBookLimitsError::ZeroPreparedUncrosses)
    );
}

#[test]
fn unrepresentable_uncross_pool_is_a_typed_constructor_failure() {
    let limits = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 1,
        max_prepared_uncrosses: usize::MAX,
    })
    .unwrap();
    assert_eq!(
        CallAuctionBook::try_with_limits(definition(), limits).unwrap_err(),
        CallAuctionConstructionError::CapacityReservationFailed(
            CallAuctionCapacity::PreparedUncrosses
        )
    );
}

#[test]
fn crossed_limit_and_market_interest_clears_and_allocates_without_mutation() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(16, 8, 32)).unwrap();
    book.admit(order(
        1,
        1,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(110)),
        10,
    ))
    .unwrap();
    book.admit(order(2, 1, Side::Buy, AuctionOrderConstraint::Market, 4))
        .unwrap();
    book.admit(order(
        3,
        2,
        Side::Sell,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        8,
    ))
    .unwrap();
    book.admit(order(4, 2, Side::Sell, AuctionOrderConstraint::Market, 2))
        .unwrap();

    assert_eq!(book.best_limit_price(Side::Buy), Some(Price::from_raw(110)));
    assert_eq!(
        book.best_limit_price(Side::Sell),
        Some(Price::from_raw(100))
    );
    assert_eq!(book.market_quantity(Side::Buy), 4);
    assert_eq!(book.market_quantity(Side::Sell), 2);
    let indicative = book
        .indicative_clearing(
            book.instrument_price_band(),
            Price::from_raw(105),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        )
        .unwrap()
        .unwrap();
    let clearing = indicative.clearing();
    assert_eq!(clearing.price(), Price::from_raw(105));
    assert_eq!(clearing.buy_quantity(), 14);
    assert_eq!(clearing.sell_quantity(), 10);
    assert_eq!(clearing.executable_quantity(), 10);

    let plan = book
        .allocation_plan(indicative, AuctionAllocationPolicy::PriceTime)
        .unwrap();
    assert_eq!(
        plan.buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(2, 4), (1, 6)]
    );
    assert_eq!(
        plan.sell_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(4, 2), (3, 8)]
    );
    assert_eq!(book.active_order_count(), 4);
    book.validate().unwrap();
}

#[test]
fn shard_sequence_enforces_market_price_and_fifo_priority() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(16, 8, 32)).unwrap();
    let first = book
        .admit(order_with_class(
            10,
            1,
            Side::Buy,
            AuctionOrderConstraint::Market,
            2,
            u16::MAX,
        ))
        .unwrap();
    let second = book
        .admit(order_with_class(
            11,
            1,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(110)),
            4,
            u16::MAX,
        ))
        .unwrap();
    let third = book
        .admit(order_with_class(
            12,
            1,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(110)),
            4,
            u16::MAX,
        ))
        .unwrap();
    book.admit(order_with_class(
        13,
        1,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        4,
        0,
    ))
    .unwrap();
    book.admit(order(14, 2, Side::Sell, AuctionOrderConstraint::Market, 7))
        .unwrap();
    assert!(first.priority_sequence < second.priority_sequence);
    assert!(second.priority_sequence < third.priority_sequence);

    let indicative = book
        .indicative_clearing(
            book.instrument_price_band(),
            Price::from_raw(105),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        )
        .unwrap()
        .unwrap();
    let plan = book
        .allocation_plan(indicative, AuctionAllocationPolicy::PriceTime)
        .unwrap();
    assert_eq!(
        plan.buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(10, 2), (11, 4), (12, 1)]
    );
    book.validate().unwrap();
}

#[test]
fn allocation_policy_explicitly_selects_price_time_or_pro_rata_time() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(8, 4, 16)).unwrap();
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
    let indicative = book
        .indicative_clearing(
            book.instrument_price_band(),
            Price::from_raw(100),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        )
        .unwrap()
        .unwrap();

    let price_time = book
        .allocation_plan(indicative, AuctionAllocationPolicy::PriceTime)
        .unwrap();
    let pro_rata = book
        .allocation_plan(indicative, AuctionAllocationPolicy::ProRataTime)
        .unwrap();

    assert_eq!(
        price_time
            .buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(1, 10), (2, 5)]
    );
    assert_eq!(
        pro_rata
            .buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(1, 5), (2, 10)]
    );
    assert_eq!(book.active_order_count(), 3);
    book.validate().unwrap();
}

#[test]
fn allocation_rejects_foreign_and_stale_indicative_results() {
    let mut first = CallAuctionBook::try_with_limits(definition(), limits(8, 4, 16)).unwrap();
    first
        .admit(order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 5))
        .unwrap();
    first
        .admit(order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 5))
        .unwrap();
    let indicative = first
        .indicative_clearing(
            first.instrument_price_band(),
            Price::from_raw(0),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        )
        .unwrap()
        .unwrap();
    assert_eq!(indicative.state_revision(), 2);

    let mut second = CallAuctionBook::try_with_limits(definition(), limits(8, 4, 16)).unwrap();
    second
        .admit(order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 5))
        .unwrap();
    second
        .admit(order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 5))
        .unwrap();
    assert_eq!(
        second.allocation_plan(indicative, AuctionAllocationPolicy::PriceTime),
        Err(CallAuctionPlanError::ForeignBook)
    );

    first
        .admit(order(3, 1, Side::Buy, AuctionOrderConstraint::Market, 1))
        .unwrap();
    assert_eq!(
        first.allocation_plan(indicative, AuctionAllocationPolicy::PriceTime),
        Err(CallAuctionPlanError::StaleRevision {
            observed: 2,
            current: 3,
        })
    );
}

#[test]
fn cancellation_checks_owner_unlinks_middle_and_never_reuses_identity() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(8, 4, 8)).unwrap();
    for id in 1..=3 {
        book.admit(order(
            id,
            1,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            id,
        ))
        .unwrap();
    }
    assert_eq!(
        book.cancel(account(2), OrderId::new(2).unwrap()),
        Err(CallAuctionCancelError::AccountMismatch)
    );
    let canceled = book.cancel(account(1), OrderId::new(2).unwrap()).unwrap();
    assert_eq!(canceled.order_id.get(), 2);
    assert_eq!(book.active_order_count(), 2);
    assert_eq!(book.accepted_order_id_count(), 3);
    assert_eq!(
        book.admit(order(2, 1, Side::Sell, AuctionOrderConstraint::Market, 1,)),
        Err(CallAuctionAdmissionError::DuplicateOrderId)
    );
    assert_eq!(
        book.cancel(account(1), OrderId::new(2).unwrap()),
        Err(CallAuctionCancelError::UnknownOrder)
    );
    assert_eq!(
        book.order(OrderId::new(1).unwrap())
            .unwrap()
            .quantity
            .lots(),
        1
    );
    assert_eq!(
        book.order(OrderId::new(3).unwrap())
            .unwrap()
            .quantity
            .lots(),
        3
    );
    book.validate().unwrap();
}

#[test]
fn quantity_reduction_retains_identity_priority_and_reconciles_indexes() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(8, 4, 16)).unwrap();
    let first = book
        .admit(order_with_class(
            10,
            7,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            8,
            3,
        ))
        .unwrap();
    let second = book
        .admit(order(
            20,
            7,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            5,
        ))
        .unwrap();
    book.admit(order(30, 8, Side::Sell, AuctionOrderConstraint::Market, 9))
        .unwrap();
    let allocation = book.resource_status();

    let amendment = book
        .amend(account(7), first.order_id, Quantity::new(3).unwrap())
        .unwrap();
    assert_eq!(amendment.previous(), first);
    assert_eq!(amendment.current().order_id, first.order_id);
    assert_eq!(amendment.current().quantity.lots(), 3);
    assert_eq!(amendment.current().priority_class, first.priority_class);
    assert_eq!(
        amendment.current().priority_sequence,
        first.priority_sequence
    );
    assert_eq!(amendment.state_revision(), 4);
    assert_eq!(book.accepted_order_id_count(), 3);
    assert_eq!(book.active_order_count(), 3);
    assert_eq!(book.limit_depth(Side::Buy, 1)[0].quantity(), 8);
    assert_eq!(
        book.resource_status().allocated_active_orders,
        allocation.allocated_active_orders
    );
    assert_eq!(
        book.resource_status().allocated_active_accounts,
        allocation.allocated_active_accounts
    );

    let indicative = book
        .indicative_clearing(
            book.instrument_price_band(),
            Price::from_raw(100),
            AuctionPricePolicy::REFERENCE_THEN_LOWER,
        )
        .unwrap()
        .unwrap();
    let plan = book
        .allocation_plan(indicative, AuctionAllocationPolicy::PriceTime)
        .unwrap();
    assert_eq!(
        plan.buy_fills()
            .iter()
            .map(|fill| (fill.order_id().get(), fill.quantity_lots()))
            .collect::<Vec<_>>(),
        vec![(20, 5), (10, 3)]
    );
    assert_eq!(second.priority_sequence, 2);
    book.validate().unwrap();
}

#[test]
fn invalid_quantity_reduction_fails_before_mutation() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(4, 4, 8)).unwrap();
    let admitted = book
        .admit(order(1, 7, Side::Sell, AuctionOrderConstraint::Market, 5))
        .unwrap();
    let before = book.resource_status();

    for (owner, order_id, quantity, expected) in [
        (
            account(8),
            admitted.order_id,
            4,
            CallAuctionAmendError::AccountMismatch,
        ),
        (
            account(7),
            OrderId::new(99).unwrap(),
            4,
            CallAuctionAmendError::UnknownOrder,
        ),
        (
            account(7),
            admitted.order_id,
            5,
            CallAuctionAmendError::QuantityNotReduced,
        ),
        (
            account(7),
            admitted.order_id,
            6,
            CallAuctionAmendError::QuantityNotReduced,
        ),
    ] {
        assert_eq!(
            book.amend(owner, order_id, Quantity::new(quantity).unwrap()),
            Err(expected)
        );
        assert_eq!(book.order(admitted.order_id), Some(admitted));
        assert_eq!(book.resource_status(), before);
    }
    book.validate().unwrap();
}

#[test]
fn account_mass_cancel_is_indexed_canonical_and_revision_atomic() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(8, 8, 16)).unwrap();
    for command in [
        order(70, 7, Side::Buy, AuctionOrderConstraint::Market, 2),
        order(
            20,
            8,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(95)),
            11,
        ),
        order(
            50,
            7,
            Side::Sell,
            AuctionOrderConstraint::Limit(Price::from_raw(105)),
            5,
        ),
        order(
            10,
            7,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
            3,
        ),
        order(40, 7, Side::Sell, AuctionOrderConstraint::Market, 7),
        order(30, 9, Side::Buy, AuctionOrderConstraint::Market, 13),
    ] {
        book.admit(command).unwrap();
    }

    assert_eq!(
        book.account_active_order_count(account(7), MassCancelScope::All),
        4
    );
    assert_eq!(
        book.account_active_order_count(account(7), MassCancelScope::Side(Side::Sell)),
        2
    );
    let preflight = book
        .preflight_mass_cancel(account(7), MassCancelScope::Side(Side::Sell))
        .unwrap();
    assert_eq!(preflight.cancelled_order_count(), 2);
    assert_eq!(preflight.cancelled_quantity_lots(), 12);
    assert_eq!(preflight.state_revision(), 7);

    let allocation = book.resource_status();
    let mut removed = Vec::with_capacity(8);
    let result = book
        .mass_cancel_into(account(7), MassCancelScope::Side(Side::Sell), &mut removed)
        .unwrap();
    assert_eq!(result, preflight);
    assert_eq!(
        removed
            .iter()
            .map(|order| order.order_id.get())
            .collect::<Vec<_>>(),
        vec![40, 50]
    );
    assert_eq!(book.state_revision(), 7);
    assert_eq!(book.active_order_count(), 4);
    assert_eq!(book.accepted_order_id_count(), 6);
    assert_eq!(book.market_quantity(Side::Sell), 0);
    assert_eq!(book.price_level_count(Side::Sell), 1);
    assert_eq!(
        book.resource_status().allocated_active_accounts,
        allocation.allocated_active_accounts
    );
    assert_eq!(
        book.resource_status().account_index_buckets,
        allocation.account_index_buckets
    );
    book.validate().unwrap();

    removed.clear();
    let empty = book
        .mass_cancel_into(account(77), MassCancelScope::All, &mut removed)
        .unwrap();
    assert_eq!(empty.cancelled_order_count(), 0);
    assert_eq!(empty.cancelled_quantity_lots(), 0);
    assert_eq!(empty.state_revision(), 7);
    assert!(removed.is_empty());
    book.validate().unwrap();
}

#[test]
fn mass_cancel_output_failure_precedes_all_mutation() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(4, 4, 8)).unwrap();
    book.admit(order(2, 1, Side::Buy, AuctionOrderConstraint::Market, 3))
        .unwrap();
    book.admit(order(1, 1, Side::Sell, AuctionOrderConstraint::Market, 5))
        .unwrap();
    let before_revision = book.state_revision();
    let before_status = book.resource_status();
    let mut undersized = Vec::with_capacity(1);

    assert_eq!(
        book.mass_cancel_into(account(1), MassCancelScope::All, &mut undersized),
        Err(CallAuctionMassCancelError::OutputCapacityExceeded {
            required: 2,
            available: 1,
        })
    );
    assert_eq!(book.state_revision(), before_revision);
    assert_eq!(book.resource_status(), before_status);
    assert!(undersized.is_empty());

    let mut nonempty = Vec::with_capacity(4);
    nonempty.push(book.order(OrderId::new(1).unwrap()).unwrap());
    assert_eq!(
        book.mass_cancel_into(account(1), MassCancelScope::All, &mut nonempty),
        Err(CallAuctionMassCancelError::OutputNotEmpty)
    );
    assert_eq!(book.state_revision(), before_revision);
    assert_eq!(book.resource_status(), before_status);
    book.validate().unwrap();
}

#[test]
fn admission_validates_routing_version_price_and_quantity() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(8, 4, 8)).unwrap();
    let wrong_instrument = CallAuctionOrder::new(
        OrderId::new(1).unwrap(),
        account(1),
        InstrumentId::new(99).unwrap(),
        version(),
        Side::Buy,
        AuctionOrderConstraint::Market,
        Quantity::new(1).unwrap(),
        AuctionPriorityClass::HIGHEST,
    );
    assert_eq!(
        book.admit(wrong_instrument),
        Err(CallAuctionAdmissionError::Instrument(
            AdmissionError::WrongInstrument
        ))
    );
    let wrong_version = CallAuctionOrder::new(
        OrderId::new(2).unwrap(),
        account(1),
        instrument(),
        InstrumentVersion::new(99).unwrap(),
        Side::Buy,
        AuctionOrderConstraint::Market,
        Quantity::new(1).unwrap(),
        AuctionPriorityClass::HIGHEST,
    );
    assert_eq!(
        book.admit(wrong_version),
        Err(CallAuctionAdmissionError::Instrument(
            AdmissionError::WrongVersion
        ))
    );
    assert_eq!(
        book.admit(order(
            3,
            1,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(101)),
            1,
        )),
        Err(CallAuctionAdmissionError::Instrument(
            AdmissionError::PriceOffTickGrid
        ))
    );
    assert_eq!(
        book.admit(order(
            4,
            1,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(205)),
            1,
        )),
        Err(CallAuctionAdmissionError::Instrument(
            AdmissionError::PriceOutsideCollar
        ))
    );
    assert_eq!(
        book.admit(order(
            5,
            1,
            Side::Buy,
            AuctionOrderConstraint::Market,
            1_001
        )),
        Err(CallAuctionAdmissionError::Instrument(
            AdmissionError::QuantityOutsideLimits
        ))
    );
    assert_eq!(book.accepted_order_id_count(), 0);
}

#[test]
fn finite_capacities_fail_before_mutation_and_are_independent_per_side() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(3, 1, 4)).unwrap();
    book.admit(order(
        1,
        1,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        1,
    ))
    .unwrap();
    book.admit(order(
        2,
        1,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        1,
    ))
    .unwrap();
    assert_eq!(
        book.admit(order(
            3,
            1,
            Side::Buy,
            AuctionOrderConstraint::Limit(Price::from_raw(105)),
            1,
        )),
        Err(CallAuctionAdmissionError::CapacityExceeded(
            CallAuctionCapacity::BidPriceLevels
        ))
    );
    book.admit(order(4, 1, Side::Buy, AuctionOrderConstraint::Market, 1))
        .unwrap();
    assert_eq!(
        book.admit(order(5, 2, Side::Sell, AuctionOrderConstraint::Market, 1)),
        Err(CallAuctionAdmissionError::CapacityExceeded(
            CallAuctionCapacity::ActiveOrders
        ))
    );
    book.cancel(account(1), OrderId::new(2).unwrap()).unwrap();
    book.admit(order(
        6,
        2,
        Side::Sell,
        AuctionOrderConstraint::Limit(Price::from_raw(95)),
        1,
    ))
    .unwrap();
    assert_eq!(book.price_level_count(Side::Buy), 1);
    assert_eq!(book.price_level_count(Side::Sell), 1);
    book.cancel(account(1), OrderId::new(1).unwrap()).unwrap();
    assert_eq!(
        book.admit(order(7, 1, Side::Buy, AuctionOrderConstraint::Market, 1)),
        Err(CallAuctionAdmissionError::CapacityExceeded(
            CallAuctionCapacity::AcceptedOrderIds
        ))
    );
    book.validate().unwrap();
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one saturated replacement case keeps class, priority, capacity, and atomic-failure evidence contiguous"
)]
fn cancel_replace_reuses_full_capacity_loses_priority_and_fails_atomically() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(2, 1, 3)).unwrap();
    let first = order(
        1,
        7,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        5,
    );
    let second = order(
        2,
        8,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        4,
    );
    book.admit(first).unwrap();
    book.admit(second).unwrap();
    let allocation = book.resource_status();

    let replacement = order_with_class(
        3,
        7,
        Side::Buy,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        6,
        4,
    );
    let replaced = book
        .replace(account(7), first.order_id(), replacement)
        .unwrap();
    assert_eq!(replaced.cancelled().order_id, first.order_id());
    assert_eq!(replaced.accepted().order_id, replacement.order_id());
    assert_eq!(
        replaced.accepted().priority_class,
        AuctionPriorityClass::new(4)
    );
    assert_eq!(replaced.accepted().priority_sequence, 3);
    assert!(book.order(first.order_id()).is_none());
    assert_eq!(book.order(second.order_id()).unwrap().priority_sequence, 2);
    assert_eq!(
        book.order(replacement.order_id())
            .unwrap()
            .priority_sequence,
        3
    );
    assert_eq!(book.state_revision(), 3);
    assert_eq!(
        book.resource_status().allocated_active_orders,
        allocation.allocated_active_orders
    );
    assert_eq!(
        book.resource_status().allocated_accepted_order_ids,
        allocation.allocated_accepted_order_ids
    );

    let before_second = book.order(second.order_id());
    let before_replacement = book.order(replacement.order_id());
    let before_depth = book.limit_depth(Side::Buy, usize::MAX);
    assert_eq!(
        book.replace(
            account(8),
            second.order_id(),
            order(
                4,
                8,
                Side::Buy,
                AuctionOrderConstraint::Limit(Price::from_raw(105)),
                1,
            ),
        ),
        Err(CallAuctionReplaceError::Replacement(
            CallAuctionAdmissionError::CapacityExceeded(CallAuctionCapacity::AcceptedOrderIds)
        ))
    );
    assert_eq!(book.order(second.order_id()), before_second);
    assert_eq!(book.order(replacement.order_id()), before_replacement);
    assert_eq!(book.limit_depth(Side::Buy, usize::MAX), before_depth);
    assert_eq!(book.state_revision(), 3);
    book.validate().unwrap();

    let mut level_full = CallAuctionBook::try_with_limits(definition(), limits(1, 1, 2)).unwrap();
    let old = order(
        10,
        9,
        Side::Sell,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        3,
    );
    level_full.admit(old).unwrap();
    level_full
        .replace(
            account(9),
            old.order_id(),
            order(
                11,
                9,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(105)),
                2,
            ),
        )
        .unwrap();
    assert_eq!(
        level_full.best_limit_price(Side::Sell),
        Some(Price::from_raw(105))
    );
    assert_eq!(level_full.active_order_count(), 1);
    level_full.validate().unwrap();
}

#[test]
fn constructor_reservations_do_not_grow_across_mutation_and_analysis() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(4, 2, 8)).unwrap();
    let initial = book.resource_status();
    for id in 1..=8 {
        let side = if id % 2 == 0 { Side::Buy } else { Side::Sell };
        let price = if id % 4 < 2 { 100 } else { 105 };
        book.admit(order(
            id,
            1,
            side,
            AuctionOrderConstraint::Limit(Price::from_raw(price)),
            1,
        ))
        .unwrap();
        let _ = book
            .indicative_clearing(
                book.instrument_price_band(),
                Price::from_raw(100),
                AuctionPricePolicy::REFERENCE_THEN_LOWER,
            )
            .unwrap();
        book.cancel(account(1), OrderId::new(id).unwrap()).unwrap();
    }
    let final_status = book.resource_status();
    assert_eq!(
        final_status.allocated_active_orders,
        initial.allocated_active_orders
    );
    assert_eq!(
        final_status.allocated_accepted_order_ids,
        initial.allocated_accepted_order_ids
    );
    assert_eq!(
        final_status.allocated_bid_price_levels,
        initial.allocated_bid_price_levels
    );
    assert_eq!(
        final_status.allocated_ask_price_levels,
        initial.allocated_ask_price_levels
    );
    assert_eq!(final_status.bid_level_scratch, initial.bid_level_scratch);
    assert_eq!(final_status.ask_level_scratch, initial.ask_level_scratch);
    assert_eq!(final_status.buy_order_scratch, initial.buy_order_scratch);
    assert_eq!(final_status.sell_order_scratch, initial.sell_order_scratch);
    assert_eq!(final_status.active_orders, 0);
    assert_eq!(final_status.accepted_order_ids, 8);
    book.validate().unwrap();
}

#[derive(Clone, Copy)]
struct ModelOrder {
    account: AccountId,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u64,
    priority_class: AuctionPriorityClass,
    sequence: u64,
}

fn next_random(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state
}

fn expected_fills(
    model: &HashMap<OrderId, ModelOrder>,
    side: Side,
    clearing_price: Price,
    mut remaining: u128,
) -> Vec<(OrderId, u64)> {
    let mut orders = model
        .iter()
        .filter(|(_, order)| order.side == side)
        .map(|(id, order)| (*id, *order))
        .collect::<Vec<_>>();
    orders.sort_by_key(|(id, order)| {
        let (kind, price_rank) = match order.constraint {
            AuctionOrderConstraint::Market => (0_u8, 0_i64),
            AuctionOrderConstraint::Limit(price) => (
                1,
                if side == Side::Buy {
                    -price.raw()
                } else {
                    price.raw()
                },
            ),
        };
        (kind, price_rank, order.priority_class, order.sequence, *id)
    });
    let mut fills = Vec::new();
    for (id, order) in orders {
        let eligible = match order.constraint {
            AuctionOrderConstraint::Market => true,
            AuctionOrderConstraint::Limit(price) => match side {
                Side::Buy => price >= clearing_price,
                Side::Sell => price <= clearing_price,
            },
        };
        if remaining == 0 || !eligible {
            break;
        }
        let filled = u64::try_from(u128::from(order.quantity).min(remaining)).unwrap();
        fills.push((id, filled));
        remaining -= u128::from(filled);
    }
    fills
}

fn verify_model(book: &mut CallAuctionBook, model: &HashMap<OrderId, ModelOrder>, step: usize) {
    let mut bids = BTreeMap::<Price, u128>::new();
    let mut asks = BTreeMap::<Price, u128>::new();
    let mut market_buy = 0_u128;
    let mut market_sell = 0_u128;
    for order in model.values() {
        match (order.side, order.constraint) {
            (Side::Buy, AuctionOrderConstraint::Market) => {
                market_buy += u128::from(order.quantity);
            }
            (Side::Sell, AuctionOrderConstraint::Market) => {
                market_sell += u128::from(order.quantity);
            }
            (Side::Buy, AuctionOrderConstraint::Limit(price)) => {
                *bids.entry(price).or_default() += u128::from(order.quantity);
            }
            (Side::Sell, AuctionOrderConstraint::Limit(price)) => {
                *asks.entry(price).or_default() += u128::from(order.quantity);
            }
        }
    }
    let bid_levels = bids
        .iter()
        .rev()
        .map(|(price, quantity)| AuctionLevel::new(*price, *quantity).unwrap())
        .collect::<Vec<_>>();
    let ask_levels = asks
        .iter()
        .map(|(price, quantity)| AuctionLevel::new(*price, *quantity).unwrap())
        .collect::<Vec<_>>();
    let expected = discover_bounded_clearing_price(
        &bid_levels,
        &ask_levels,
        AuctionMarketInterest::new(market_buy, market_sell),
        book.price_grid(),
        book.instrument_price_band(),
        Price::from_raw(0),
        AuctionPricePolicy::PRESSURE_THEN_REFERENCE_LOWER,
    )
    .unwrap();
    let actual_indicative = book
        .indicative_clearing(
            book.instrument_price_band(),
            Price::from_raw(0),
            AuctionPricePolicy::PRESSURE_THEN_REFERENCE_LOWER,
        )
        .unwrap();
    let actual = actual_indicative.map(CallAuctionIndicative::clearing);
    assert_eq!(actual, expected, "step {step}");
    if let Some(indicative) = actual_indicative {
        let clearing = indicative.clearing();
        let plan = book
            .allocation_plan(indicative, AuctionAllocationPolicy::PriceTime)
            .unwrap();
        let actual_buys = plan
            .buy_fills()
            .iter()
            .map(|fill| (fill.order_id(), fill.quantity_lots()))
            .collect::<Vec<_>>();
        let actual_sells = plan
            .sell_fills()
            .iter()
            .map(|fill| (fill.order_id(), fill.quantity_lots()))
            .collect::<Vec<_>>();
        assert_eq!(
            actual_buys,
            expected_fills(
                model,
                Side::Buy,
                clearing.price(),
                clearing.executable_quantity()
            ),
            "buy allocation step {step}"
        );
        assert_eq!(
            actual_sells,
            expected_fills(
                model,
                Side::Sell,
                clearing.price(),
                clearing.executable_quantity()
            ),
            "sell allocation step {step}"
        );
    }
    assert_eq!(book.active_order_count(), model.len());
    book.validate().unwrap();
}

#[test]
fn randomized_mutations_match_independent_aggregate_and_priority_models() {
    let mut book = CallAuctionBook::try_with_limits(definition(), limits(64, 64, 10_000)).unwrap();
    let initial_resources = book.resource_status();
    let mut model = HashMap::<OrderId, ModelOrder>::new();
    let mut rng = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next_id = 1_u64;

    for step in 0..20_000 {
        let choose_add = model.is_empty() || (next_random(&mut rng) % 100 < 62 && model.len() < 64);
        if choose_add && next_id <= 10_000 {
            let id = OrderId::new(next_id).unwrap();
            next_id += 1;
            let owner = account(next_random(&mut rng) % 8 + 1);
            let side = if next_random(&mut rng) & 1 == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let constraint = if next_random(&mut rng) % 5 == 0 {
                AuctionOrderConstraint::Market
            } else {
                let raw = i64::try_from(next_random(&mut rng) % 61).unwrap() * 5 - 100;
                AuctionOrderConstraint::Limit(Price::from_raw(raw))
            };
            let quantity = next_random(&mut rng) % 25 + 1;
            let priority_class =
                AuctionPriorityClass::new(u16::try_from(next_random(&mut rng) % 4).unwrap());
            let accepted = book
                .admit(CallAuctionOrder::new(
                    id,
                    owner,
                    instrument(),
                    version(),
                    side,
                    constraint,
                    Quantity::new(quantity).unwrap(),
                    priority_class,
                ))
                .unwrap();
            model.insert(
                id,
                ModelOrder {
                    account: owner,
                    side,
                    constraint,
                    quantity,
                    priority_class,
                    sequence: accepted.priority_sequence,
                },
            );
        } else if !model.is_empty() {
            let model_len = u64::try_from(model.len()).unwrap();
            let index = usize::try_from(next_random(&mut rng) % model_len).unwrap();
            let id = *model.keys().nth(index).unwrap();
            let expected = model.remove(&id).unwrap();
            let canceled = book.cancel(expected.account, id).unwrap();
            assert_eq!(canceled.quantity.lots(), expected.quantity);
        }

        if step % 97 == 0 {
            verify_model(&mut book, &model, step);
        }
    }
    let final_resources = book.resource_status();
    assert_eq!(
        final_resources.allocated_active_orders,
        initial_resources.allocated_active_orders
    );
    assert_eq!(
        final_resources.allocated_accepted_order_ids,
        initial_resources.allocated_accepted_order_ids
    );
    assert_eq!(
        final_resources.allocated_bid_price_levels,
        initial_resources.allocated_bid_price_levels
    );
    assert_eq!(
        final_resources.allocated_ask_price_levels,
        initial_resources.allocated_ask_price_levels
    );
    assert_eq!(
        final_resources.bid_level_scratch,
        initial_resources.bid_level_scratch
    );
    assert_eq!(
        final_resources.ask_level_scratch,
        initial_resources.ask_level_scratch
    );
    assert_eq!(
        final_resources.buy_order_scratch,
        initial_resources.buy_order_scratch
    );
    assert_eq!(
        final_resources.sell_order_scratch,
        initial_resources.sell_order_scratch
    );
}
