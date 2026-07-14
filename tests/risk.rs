use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, TradingState,
};
use quotick::matching::{
    CancelOrder, Command, CommandOutcome, NewOrder, OrderType, RejectReason, ReplaceOrder,
    SelfTradePrevention, TimeInForce,
};
use quotick::risk::{
    AccountRiskState, RiskError, RiskLimitSpec, RiskLimits, RiskManagedOrderBook, RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn instrument() -> InstrumentId {
    InstrumentId::new(1).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(1).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("RISK-TEST").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(-1_000), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, u64::MAX).unwrap(),
        reserve: quotick::instrument::ReserveOrderRules::disabled(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn limits() -> RiskLimits {
    RiskLimits::new(RiskLimitSpec {
        max_order_quantity_lots: 100,
        max_order_notional: 50_000,
        max_open_orders: 4,
        max_open_quantity_lots: 200,
        max_open_notional: 200_000,
        max_long_position_lots: 200,
        max_short_position_lots: 200,
    })
    .unwrap()
}

fn profile(state: AccountRiskState, initial_position_lots: i128) -> RiskProfile {
    RiskProfile::new(state, initial_position_lots, limits()).unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "test order factory keeps risk scenarios explicit"
)]
fn order(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    order_type: OrderType,
    time_in_force: TimeInForce,
    stp: SelfTradePrevention,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: instrument(),
        instrument_version: version(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display: quotick::matching::OrderDisplay::FullyDisplayed,
        order_type,
        time_in_force,
        self_trade_prevention: stp,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn limit_order(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    price: i64,
    time_in_force: TimeInForce,
) -> Command {
    order(
        command_id,
        order_id,
        owner,
        side,
        quantity,
        OrderType::Limit(Price::from_raw(price)),
        time_in_force,
        SelfTradePrevention::CancelAggressor,
    )
}

fn cancel(command_id: u64, order_id: u64, owner: u64) -> Command {
    Command::Cancel(CancelOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: instrument(),
        instrument_version: version(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn replace(command_id: u64, order_id: u64, owner: u64, quantity: u64, price: i64) -> Command {
    Command::Replace(ReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: instrument(),
        instrument_version: version(),
        new_quantity: Quantity::new(quantity).unwrap(),
        new_price: Price::from_raw(price),
        new_display: quotick::matching::OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

#[test]
fn limits_and_initial_positions_are_validated() {
    assert_eq!(
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots: 0,
            ..limit_spec(limits())
        }),
        Err(RiskError::InvalidLimits)
    );
    assert_eq!(
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots: 201,
            ..limit_spec(limits())
        }),
        Err(RiskError::InvalidLimits)
    );
    assert_eq!(
        RiskProfile::new(AccountRiskState::Active, 201, limits()),
        Err(RiskError::InitialPositionOutsideLimits)
    );
}

#[test]
fn signed_price_authorization_uses_the_reachable_collar_interval() {
    let signed_limits = RiskLimits::new(RiskLimitSpec {
        max_order_quantity_lots: 10,
        max_order_notional: 500,
        max_open_orders: 10,
        max_open_quantity_lots: 100,
        max_open_notional: 10_000,
        max_long_position_lots: 100,
        max_short_position_lots: 100,
    })
    .unwrap();
    let mut book = RiskManagedOrderBook::new(definition());
    book.register_account(
        account(11),
        RiskProfile::new(AccountRiskState::Active, 0, signed_limits).unwrap(),
    )
    .unwrap();

    // A buy at -100 can execute as low as the -1,000 collar; a sell at 100
    // can execute as high as the 1,000 collar. Both reachable magnitudes are
    // larger than the submitted limit magnitude.
    for command in [
        limit_order(1, 1, 11, Side::Buy, 1, -100, TimeInForce::GoodTilCancelled),
        limit_order(2, 2, 11, Side::Sell, 1, 100, TimeInForce::GoodTilCancelled),
    ] {
        assert_eq!(
            book.submit(command).unwrap().outcome,
            CommandOutcome::Rejected(RejectReason::RiskOrderNotionalLimit)
        );
    }
    book.validate().unwrap();
}

fn managed_with_limits(limits: RiskLimits) -> RiskManagedOrderBook {
    let mut book = RiskManagedOrderBook::new(definition());
    book.register_account(
        account(11),
        RiskProfile::new(AccountRiskState::Active, 0, limits).unwrap(),
    )
    .unwrap();
    book
}

#[test]
fn open_order_count_excludes_nonresting_ioc_commands() {
    let mut book = managed_with_limits(
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots: 10,
            max_order_notional: 10_000,
            max_open_orders: 1,
            max_open_quantity_lots: 100,
            max_open_notional: 100_000,
            max_long_position_lots: 100,
            max_short_position_lots: 100,
        })
        .unwrap(),
    );
    book.submit(limit_order(
        1,
        1,
        11,
        Side::Buy,
        1,
        100,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    assert_eq!(
        book.submit(limit_order(
            2,
            2,
            11,
            Side::Buy,
            1,
            100,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap()
        .outcome,
        CommandOutcome::Rejected(RejectReason::RiskOpenOrderCountLimit)
    );
    // IOC cannot leave a reservation and therefore does not consume an open-order slot.
    assert_eq!(
        book.submit(order(
            3,
            3,
            11,
            Side::Buy,
            1,
            OrderType::Market,
            TimeInForce::ImmediateOrCancel,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap()
        .outcome,
        CommandOutcome::Accepted
    );
    book.validate().unwrap();
}

#[test]
fn aggregate_open_quantity_limit_is_independent_of_per_order_quantity() {
    let mut book = managed_with_limits(
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots: 3,
            max_order_notional: 10_000,
            max_open_orders: 5,
            max_open_quantity_lots: 3,
            max_open_notional: 100_000,
            max_long_position_lots: 100,
            max_short_position_lots: 100,
        })
        .unwrap(),
    );
    book.submit(limit_order(
        10,
        10,
        11,
        Side::Buy,
        2,
        100,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    assert_eq!(
        book.submit(limit_order(
            11,
            11,
            11,
            Side::Buy,
            2,
            100,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap()
        .outcome,
        CommandOutcome::Rejected(RejectReason::RiskOpenQuantityLimit)
    );
    book.validate().unwrap();
}

#[test]
fn aggregate_open_notional_accounts_for_existing_maker_reservations() {
    let mut book = managed_with_limits(
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots: 10,
            max_order_notional: 1_500,
            max_open_orders: 10,
            max_open_quantity_lots: 100,
            max_open_notional: 1_500,
            max_long_position_lots: 100,
            max_short_position_lots: 100,
        })
        .unwrap(),
    );
    for index in 20..26 {
        book.submit(limit_order(
            index,
            index,
            11,
            Side::Buy,
            1,
            100,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    }
    assert_eq!(
        book.submit(limit_order(
            26,
            26,
            11,
            Side::Buy,
            1,
            100,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap()
        .outcome,
        CommandOutcome::Rejected(RejectReason::RiskOpenNotionalLimit)
    );
    book.validate().unwrap();
}

fn limit_spec(value: RiskLimits) -> RiskLimitSpec {
    RiskLimitSpec {
        max_order_quantity_lots: value.max_order_quantity_lots(),
        max_order_notional: value.max_order_notional(),
        max_open_orders: value.max_open_orders(),
        max_open_quantity_lots: value.max_open_quantity_lots(),
        max_open_notional: value.max_open_notional(),
        max_long_position_lots: value.max_long_position_lots(),
        max_short_position_lots: value.max_short_position_lots(),
    }
}

#[test]
fn missing_blocked_and_per_order_limit_rejections_are_sequenced_and_idempotent() {
    let mut book = RiskManagedOrderBook::new(definition());
    book.register_account(account(11), profile(AccountRiskState::Active, 0))
        .unwrap();
    book.register_account(account(12), profile(AccountRiskState::Blocked, 0))
        .unwrap();

    let cases = [
        (
            limit_order(1, 1, 13, Side::Buy, 1, 10, TimeInForce::GoodTilCancelled),
            RejectReason::RiskProfileMissing,
        ),
        (
            limit_order(2, 2, 12, Side::Buy, 1, 10, TimeInForce::GoodTilCancelled),
            RejectReason::RiskAccountBlocked,
        ),
        (
            limit_order(3, 3, 11, Side::Buy, 101, 10, TimeInForce::GoodTilCancelled),
            RejectReason::RiskOrderQuantityLimit,
        ),
        (
            limit_order(
                4,
                4,
                11,
                Side::Buy,
                100,
                1_000,
                TimeInForce::GoodTilCancelled,
            ),
            RejectReason::RiskOrderNotionalLimit,
        ),
    ];
    for (command, expected) in cases {
        let report = book.submit(command).unwrap();
        assert_eq!(report.outcome, CommandOutcome::Rejected(expected));
        assert_eq!(report.events.len(), 1);
        let retry = book.submit(command).unwrap();
        assert!(retry.replayed);
        assert_eq!(retry.outcome, report.outcome);
    }
    assert_eq!(book.book().active_order_count(), 0);
    assert_eq!(book.risk().reservation_count(), 0);
    book.validate().unwrap();
}

#[test]
fn matching_business_rejections_precede_missing_risk_profiles() {
    let mut book = RiskManagedOrderBook::new(definition());
    let wrong_instrument =
        match limit_order(1, 1, 99, Side::Buy, 1, 100, TimeInForce::GoodTilCancelled) {
            Command::New(mut value) => {
                value.instrument_id = InstrumentId::new(2).unwrap();
                Command::New(value)
            }
            _ => unreachable!(),
        };
    assert_eq!(
        book.submit(wrong_instrument).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::WrongInstrument)
    );
    assert_eq!(
        book.submit(order(
            2,
            2,
            99,
            Side::Buy,
            1,
            OrderType::Market,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap()
        .outcome,
        CommandOutcome::Rejected(RejectReason::MarketOrderCannotRest)
    );
    book.validate().unwrap();
}

#[test]
fn fills_and_cancels_update_positions_and_release_resting_reservations() {
    let mut book = RiskManagedOrderBook::new(definition());
    book.register_account(account(11), profile(AccountRiskState::Active, 0))
        .unwrap();
    book.register_account(account(12), profile(AccountRiskState::Active, 10))
        .unwrap();

    book.submit(limit_order(
        1,
        1,
        12,
        Side::Sell,
        5,
        100,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    let seller = book.risk().snapshot(account(12)).unwrap();
    assert_eq!(seller.open_sell_lots(), 5);
    assert_eq!(seller.open_notional(), 500);

    book.submit(limit_order(
        2,
        2,
        11,
        Side::Buy,
        3,
        100,
        TimeInForce::ImmediateOrCancel,
    ))
    .unwrap();
    assert_eq!(
        book.risk().snapshot(account(11)).unwrap().position_lots(),
        3
    );
    let seller = book.risk().snapshot(account(12)).unwrap();
    assert_eq!(seller.position_lots(), 7);
    assert_eq!(seller.open_sell_lots(), 2);
    assert_eq!(seller.open_notional(), 200);
    assert_eq!(
        book.risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        2
    );

    book.submit(cancel(3, 1, 12)).unwrap();
    assert_eq!(book.risk().reservation_count(), 0);
    assert_eq!(book.book().active_order_count(), 0);
    book.validate().unwrap();
}

#[test]
fn worst_case_position_and_aggregate_open_limits_are_conservative() {
    let constrained = RiskLimits::new(RiskLimitSpec {
        max_order_quantity_lots: 10,
        max_order_notional: 10_000,
        max_open_orders: 2,
        max_open_quantity_lots: 10,
        max_open_notional: 10_000,
        max_long_position_lots: 10,
        max_short_position_lots: 10,
    })
    .unwrap();
    let mut book = RiskManagedOrderBook::new(definition());
    book.register_account(
        account(11),
        RiskProfile::new(AccountRiskState::Active, 8, constrained).unwrap(),
    )
    .unwrap();

    let rejected = book
        .submit(limit_order(
            1,
            1,
            11,
            Side::Buy,
            3,
            100,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::RiskPositionLimit)
    );
    book.submit(limit_order(
        2,
        2,
        11,
        Side::Buy,
        2,
        100,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    let rejected = book
        .submit(limit_order(
            3,
            3,
            11,
            Side::Buy,
            1,
            100,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::RiskPositionLimit)
    );
    assert_eq!(
        book.risk().snapshot(account(11)).unwrap().open_buy_lots(),
        2
    );
    book.validate().unwrap();
}

#[test]
fn rejected_replace_preserves_the_original_book_and_reservation() {
    let constrained = RiskLimits::new(RiskLimitSpec {
        max_order_quantity_lots: 10,
        max_order_notional: 5_000,
        max_open_orders: 1,
        max_open_quantity_lots: 10,
        max_open_notional: 5_000,
        max_long_position_lots: 10,
        max_short_position_lots: 10,
    })
    .unwrap();
    let mut book = RiskManagedOrderBook::new(definition());
    book.register_account(
        account(11),
        RiskProfile::new(AccountRiskState::Active, 0, constrained).unwrap(),
    )
    .unwrap();
    book.submit(limit_order(
        1,
        1,
        11,
        Side::Buy,
        5,
        100,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();

    let rejected = book.submit(replace(2, 1, 11, 10, 200)).unwrap();
    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::RiskOrderNotionalLimit)
    );
    let live = book.book().order(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(live.price, Price::from_raw(100));
    assert_eq!(live.leaves_quantity.lots(), 5);
    let reservation = book.risk().reservation(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(reservation.price(), Price::from_raw(100));
    assert_eq!(reservation.quantity_lots(), 5);

    book.submit(replace(3, 1, 11, 3, 100)).unwrap();
    assert_eq!(
        book.risk().snapshot(account(11)).unwrap().open_buy_lots(),
        3
    );
    assert_eq!(
        book.risk().snapshot(account(11)).unwrap().open_notional(),
        300
    );
    book.validate().unwrap();
}

#[test]
fn reduce_only_cannot_increase_or_reverse_position_exposure() {
    let mut book = RiskManagedOrderBook::new(definition());
    book.register_account(account(11), profile(AccountRiskState::ReduceOnly, 5))
        .unwrap();

    for (command, reason) in [
        (
            limit_order(1, 1, 11, Side::Buy, 1, 100, TimeInForce::GoodTilCancelled),
            RejectReason::RiskReduceOnly,
        ),
        (
            limit_order(2, 2, 11, Side::Sell, 6, 100, TimeInForce::GoodTilCancelled),
            RejectReason::RiskReduceOnly,
        ),
    ] {
        assert_eq!(
            book.submit(command).unwrap().outcome,
            CommandOutcome::Rejected(reason)
        );
    }
    book.submit(limit_order(
        3,
        3,
        11,
        Side::Sell,
        5,
        100,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    assert_eq!(
        book.risk().snapshot(account(11)).unwrap().open_sell_lots(),
        5
    );
    assert_eq!(
        book.submit(limit_order(
            4,
            4,
            11,
            Side::Sell,
            1,
            100,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap()
        .outcome,
        CommandOutcome::Rejected(RejectReason::RiskReduceOnly)
    );
    book.validate().unwrap();
}

#[test]
fn decrement_and_cancel_stp_releases_prevented_resting_quantity() {
    let mut book = RiskManagedOrderBook::new(definition());
    book.register_account(account(11), profile(AccountRiskState::Active, 0))
        .unwrap();
    book.submit(limit_order(
        1,
        1,
        11,
        Side::Sell,
        5,
        100,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    book.submit(order(
        2,
        2,
        11,
        Side::Buy,
        3,
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::DecrementAndCancel,
    ))
    .unwrap();

    let snapshot = book.risk().snapshot(account(11)).unwrap();
    assert_eq!(snapshot.position_lots(), 0);
    assert_eq!(snapshot.open_sell_lots(), 2);
    assert_eq!(
        book.risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        2
    );
    book.validate().unwrap();
}

#[test]
fn profile_registration_and_notional_accumulation_fail_closed() {
    let extreme_limits = RiskLimits::new(RiskLimitSpec {
        max_order_quantity_lots: u64::MAX,
        max_order_notional: u128::MAX,
        max_open_orders: 4,
        max_open_quantity_lots: u128::MAX,
        max_open_notional: u128::MAX,
        max_long_position_lots: u128::try_from(i128::MAX).unwrap(),
        max_short_position_lots: u128::try_from(i128::MAX).unwrap(),
    })
    .unwrap();
    let mut book = RiskManagedOrderBook::new(extreme_definition());
    let extreme_profile = RiskProfile::new(AccountRiskState::Active, 0, extreme_limits).unwrap();
    book.register_account(account(11), extreme_profile).unwrap();
    assert_eq!(
        book.register_account(account(11), extreme_profile),
        Err(RiskError::DuplicateProfile(account(11)))
    );

    for index in 1..=2 {
        book.submit(limit_order(
            index,
            index,
            11,
            Side::Buy,
            u64::MAX,
            i64::MIN,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    }
    let rejected = book
        .submit(limit_order(
            3,
            3,
            11,
            Side::Buy,
            u64::MAX,
            i64::MIN,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::RiskArithmeticOverflow)
    );
    assert_eq!(book.book().active_order_count(), 2);
    assert_eq!(book.risk().reservation_count(), 2);
    book.validate().unwrap();
}

fn extreme_definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        price: PriceRules::new(0, 1, Price::from_raw(i64::MIN), Price::from_raw(i64::MAX)).unwrap(),
        ..instrument_spec(definition())
    })
    .unwrap()
}

fn instrument_spec(value: InstrumentDefinition) -> InstrumentSpec {
    let settlement = value.settlement_convention();
    InstrumentSpec {
        instrument_id: value.instrument_id(),
        version: value.version(),
        effective_from: value.effective_from(),
        symbol: value.symbol(),
        kind: value.kind(),
        base_asset_id: value.base_asset_id(),
        quote_asset_id: value.quote_asset_id(),
        price: value.price_rules(),
        quantity: value.quantity_rules(),
        reserve: value.reserve_order_rules(),
        base_units_per_lot: settlement.base_units_per_lot,
        quote_units_per_price_unit: settlement.quote_units_per_price_unit,
        trading_state: value.trading_state(),
    }
}
