use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::BinaryCodec;
use quotick::durable::DurableOrderBook;
use quotick::durable_risk::DurableRiskOrderBook;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions};
use quotick::market_data::{MarketDataKind, MarketDataPublisher, MarketDataReplica};
use quotick::matching::{
    CancelOrder, CancelReason, Command, CommandOutcome, EventKind, NewOrder, OrderBook,
    OrderDisplay, OrderType, RejectReason, ReplaceOrder, SelfTradePrevention, TimeInForce,
};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedOrderBook,
    RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_WAL: AtomicU64 = AtomicU64::new(1);

fn definition(reserve: ReserveOrderRules) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("RESERVE").unwrap(),
        kind: InstrumentKind::Future,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(5, 5, 10_000).unwrap(),
        reserve,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "test command factory keeps reserve state-machine cases explicit"
)]
fn new_order(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    display: OrderDisplay,
    order_type: OrderType,
    time_in_force: TimeInForce,
    stp: SelfTradePrevention,
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
        order_type,
        time_in_force,
        self_trade_prevention: stp,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn reserve(peak: u64) -> OrderDisplay {
    OrderDisplay::Reserve {
        peak: Quantity::new(peak).unwrap(),
    }
}

fn maker_trades(report: &quotick::matching::ExecutionReport) -> Vec<(u64, u64)> {
    report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::Trade(trade) => Some((trade.maker_order_id.get(), trade.quantity.lots())),
            _ => None,
        })
        .collect()
}

#[test]
fn reserve_admission_is_canonical_bounded_and_resting_only() {
    let disabled = new_order(
        1,
        1,
        1,
        Side::Buy,
        100,
        reserve(10),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let mut book = OrderBook::new(definition(ReserveOrderRules::disabled()));
    assert_eq!(
        book.submit(disabled).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::ReserveOrderNotSupported)
    );

    let mut book = OrderBook::new(definition(ReserveOrderRules::new(9).unwrap()));
    let cases = [
        (
            new_order(
                2,
                2,
                1,
                Side::Buy,
                100,
                reserve(12),
                OrderType::Limit(Price::from_raw(100)),
                TimeInForce::GoodTilCancelled,
                SelfTradePrevention::CancelAggressor,
            ),
            RejectReason::DisplayQuantityOffGrid,
        ),
        (
            new_order(
                3,
                3,
                1,
                Side::Buy,
                100,
                reserve(100),
                OrderType::Limit(Price::from_raw(100)),
                TimeInForce::GoodTilCancelled,
                SelfTradePrevention::CancelAggressor,
            ),
            RejectReason::DisplayQuantityNotLessThanOrder,
        ),
        (
            new_order(
                4,
                4,
                1,
                Side::Buy,
                105,
                reserve(10),
                OrderType::Limit(Price::from_raw(100)),
                TimeInForce::GoodTilCancelled,
                SelfTradePrevention::CancelAggressor,
            ),
            RejectReason::ReserveReplenishmentLimit,
        ),
        (
            new_order(
                5,
                5,
                1,
                Side::Buy,
                100,
                reserve(10),
                OrderType::Market,
                TimeInForce::ImmediateOrCancel,
                SelfTradePrevention::CancelAggressor,
            ),
            RejectReason::ReserveOrderCannotBeImmediate,
        ),
    ];
    for (command, expected) in cases {
        assert_eq!(
            book.submit(command).unwrap().outcome,
            CommandOutcome::Rejected(expected)
        );
    }
    assert_eq!(book.active_order_count(), 0);
    book.validate().unwrap();
}

#[test]
fn exhausted_peak_refreshes_at_the_fifo_tail_under_the_same_order_id() {
    let mut book = OrderBook::new(definition(ReserveOrderRules::new(16).unwrap()));
    book.submit(new_order(
        1,
        1,
        11,
        Side::Sell,
        30,
        reserve(10),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    book.submit(new_order(
        2,
        2,
        12,
        Side::Sell,
        5,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    assert_eq!(
        book.level(Side::Sell, Price::from_raw(100))
            .unwrap()
            .quantity,
        15
    );

    let report = book
        .submit(new_order(
            3,
            3,
            13,
            Side::Buy,
            20,
            OrderDisplay::FullyDisplayed,
            OrderType::Market,
            TimeInForce::ImmediateOrCancel,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
    assert_eq!(maker_trades(&report), vec![(1, 10), (2, 5), (1, 5)]);
    let refreshes: Vec<_> = report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::OrderRefreshed {
                order_id,
                displayed_quantity,
                leaves_quantity,
                ..
            } => Some((
                order_id.get(),
                displayed_quantity.lots(),
                leaves_quantity.lots(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(refreshes, vec![(1, 10, 20)]);

    let order = book.order(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(order.leaves_quantity.lots(), 15);
    assert_eq!(order.displayed_quantity.lots(), 5);
    assert_eq!(order.display, reserve(10));
    let level = book.level(Side::Sell, Price::from_raw(100)).unwrap();
    assert_eq!(level.quantity, 5);
    assert_eq!(level.order_count, 1);
    book.validate().unwrap();
}

#[test]
fn marketable_reserve_uses_total_incoming_quantity_and_peaks_only_the_residual() {
    let mut book = OrderBook::new(definition(ReserveOrderRules::new(16).unwrap()));
    book.submit(new_order(
        1,
        1,
        11,
        Side::Sell,
        15,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    let report = book
        .submit(new_order(
            2,
            2,
            12,
            Side::Buy,
            30,
            reserve(10),
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
    assert_eq!(maker_trades(&report), vec![(1, 15)]);
    let residual = book.order(OrderId::new(2).unwrap()).unwrap();
    assert_eq!(residual.leaves_quantity.lots(), 15);
    assert_eq!(residual.displayed_quantity.lots(), 10);
    assert_eq!(book.best_bid().unwrap().quantity, 10);
    book.validate().unwrap();
}

#[test]
fn hidden_leaves_are_fok_eligible_but_public_depth_remains_displayed_only() {
    let mut book = OrderBook::new(definition(ReserveOrderRules::new(16).unwrap()));
    book.submit(new_order(
        1,
        1,
        11,
        Side::Sell,
        30,
        reserve(10),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    assert_eq!(book.best_ask().unwrap().quantity, 10);

    let report = book
        .submit(new_order(
            2,
            2,
            12,
            Side::Buy,
            25,
            OrderDisplay::FullyDisplayed,
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::FillOrKill,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(maker_trades(&report), vec![(1, 10), (1, 10), (1, 5)]);
    let order = book.order(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(order.leaves_quantity.lots(), 5);
    assert_eq!(order.displayed_quantity.lots(), 5);
    assert_eq!(book.best_ask().unwrap().quantity, 5);
    book.validate().unwrap();
}

#[test]
fn fok_preflight_simulates_reserve_requeue_before_self_trade_barriers() {
    let mut book = OrderBook::new(definition(ReserveOrderRules::new(16).unwrap()));
    for command in [
        new_order(
            1,
            1,
            11,
            Side::Sell,
            30,
            reserve(10),
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
        new_order(
            2,
            2,
            12,
            Side::Sell,
            5,
            OrderDisplay::FullyDisplayed,
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
        new_order(
            3,
            3,
            13,
            Side::Sell,
            10,
            OrderDisplay::FullyDisplayed,
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    ] {
        book.submit(command).unwrap();
    }

    let rejected = book
        .submit(new_order(
            4,
            4,
            12,
            Side::Buy,
            15,
            OrderDisplay::FullyDisplayed,
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::FillOrKill,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::InsufficientLiquidity)
    );
    assert!(maker_trades(&rejected).is_empty());
    let reserve_order = book.order(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(reserve_order.leaves_quantity.lots(), 30);
    assert_eq!(reserve_order.displayed_quantity.lots(), 10);
    assert_eq!(book.best_ask().unwrap().quantity, 25);

    let accepted = book
        .submit(new_order(
            5,
            5,
            12,
            Side::Buy,
            10,
            OrderDisplay::FullyDisplayed,
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::FillOrKill,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
    assert_eq!(accepted.outcome, CommandOutcome::Accepted);
    assert_eq!(maker_trades(&accepted), vec![(1, 10)]);
    book.validate().unwrap();
}

#[test]
fn fok_cancel_resting_counts_all_external_hidden_liquidity_across_self_orders() {
    let mut book = OrderBook::new(definition(ReserveOrderRules::new(16).unwrap()));
    for command in [
        new_order(
            1,
            1,
            11,
            Side::Sell,
            30,
            reserve(10),
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
        new_order(
            2,
            2,
            12,
            Side::Sell,
            5,
            OrderDisplay::FullyDisplayed,
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
        new_order(
            3,
            3,
            13,
            Side::Sell,
            20,
            reserve(5),
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    ] {
        book.submit(command).unwrap();
    }

    let report = book
        .submit(new_order(
            4,
            4,
            12,
            Side::Buy,
            45,
            OrderDisplay::FullyDisplayed,
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::FillOrKill,
            SelfTradePrevention::CancelResting,
        ))
        .unwrap();

    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert!(report.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            order_id,
            reason: CancelReason::SelfTradeResting,
            ..
        } if order_id == OrderId::new(2).unwrap()
    )));
    assert_eq!(
        maker_trades(&report)
            .iter()
            .map(|(_, quantity)| quantity)
            .sum::<u64>(),
        45
    );
    assert!(book.order(OrderId::new(2).unwrap()).is_none());
    assert_eq!(
        book.active_orders()
            .unwrap()
            .iter()
            .map(|order| order.leaves_quantity.lots())
            .sum::<u64>(),
        5
    );
    book.validate().unwrap();
}

#[test]
fn fok_exhausts_hidden_liquidity_at_a_better_price_before_a_worse_self_barrier() {
    for policy in [
        SelfTradePrevention::CancelAggressor,
        SelfTradePrevention::CancelBoth,
    ] {
        let mut book = OrderBook::new(definition(ReserveOrderRules::new(16).unwrap()));
        book.submit(new_order(
            1,
            1,
            11,
            Side::Sell,
            30,
            reserve(10),
            OrderType::Limit(Price::from_raw(99)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
        book.submit(new_order(
            2,
            2,
            12,
            Side::Sell,
            5,
            OrderDisplay::FullyDisplayed,
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();

        let rejected = book
            .submit(new_order(
                3,
                3,
                12,
                Side::Buy,
                35,
                OrderDisplay::FullyDisplayed,
                OrderType::Limit(Price::from_raw(100)),
                TimeInForce::FillOrKill,
                policy,
            ))
            .unwrap();
        assert_eq!(
            rejected.outcome,
            CommandOutcome::Rejected(RejectReason::InsufficientLiquidity)
        );

        let accepted = book
            .submit(new_order(
                4,
                4,
                12,
                Side::Buy,
                30,
                OrderDisplay::FullyDisplayed,
                OrderType::Limit(Price::from_raw(100)),
                TimeInForce::FillOrKill,
                policy,
            ))
            .unwrap();
        assert_eq!(accepted.outcome, CommandOutcome::Accepted);
        assert_eq!(maker_trades(&accepted), vec![(1, 10), (1, 10), (1, 10)]);
        assert!(book.order(OrderId::new(2).unwrap()).is_some());
        book.validate().unwrap();
    }
}

#[test]
fn cancellation_and_replacement_operate_on_total_leaves_and_display_mode_is_stable() {
    let mut book = OrderBook::new(definition(ReserveOrderRules::new(16).unwrap()));
    book.submit(new_order(
        1,
        1,
        11,
        Side::Sell,
        30,
        reserve(10),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();

    let retained = book
        .submit(Command::Replace(ReplaceOrder {
            command_id: CommandId::new(2).unwrap(),
            order_id: OrderId::new(1).unwrap(),
            account_id: AccountId::new(11).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            new_quantity: Quantity::new(25).unwrap(),
            new_price: Price::from_raw(100),
            new_display: reserve(10),
            received_at: TimestampNs::from_unix_nanos(2),
        }))
        .unwrap();
    assert!(retained.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderReplaced {
            priority_retained: true,
            ..
        }
    )));
    assert_eq!(
        book.order(OrderId::new(1).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        25
    );
    assert_eq!(book.best_ask().unwrap().quantity, 10);

    let changed_peak = book
        .submit(Command::Replace(ReplaceOrder {
            command_id: CommandId::new(3).unwrap(),
            order_id: OrderId::new(1).unwrap(),
            account_id: AccountId::new(11).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            new_quantity: Quantity::new(20).unwrap(),
            new_price: Price::from_raw(100),
            new_display: reserve(5),
            received_at: TimestampNs::from_unix_nanos(3),
        }))
        .unwrap();
    assert!(changed_peak.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderReplaced {
            priority_retained: false,
            ..
        }
    )));
    assert_eq!(book.best_ask().unwrap().quantity, 5);

    let conversion = book
        .submit(Command::Replace(ReplaceOrder {
            command_id: CommandId::new(4).unwrap(),
            order_id: OrderId::new(1).unwrap(),
            account_id: AccountId::new(11).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            new_quantity: Quantity::new(15).unwrap(),
            new_price: Price::from_raw(100),
            new_display: OrderDisplay::FullyDisplayed,
            received_at: TimestampNs::from_unix_nanos(4),
        }))
        .unwrap();
    assert_eq!(
        conversion.outcome,
        CommandOutcome::Rejected(RejectReason::OrderDisplayModeChangeNotAllowed)
    );

    let cancelled = book
        .submit(Command::Cancel(CancelOrder {
            command_id: CommandId::new(5).unwrap(),
            order_id: OrderId::new(1).unwrap(),
            account_id: AccountId::new(11).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            received_at: TimestampNs::from_unix_nanos(5),
        }))
        .unwrap();
    assert!(cancelled.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            quantity,
            reason: CancelReason::UserRequested,
            ..
        } if quantity.lots() == 20
    )));
    assert_eq!(book.active_order_count(), 0);
    assert!(book.best_ask().is_none());
    book.validate().unwrap();
}

#[test]
fn decrement_and_cancel_stp_consumes_displayed_slices_and_refreshes_deterministically() {
    let mut book = OrderBook::new(definition(ReserveOrderRules::new(16).unwrap()));
    book.submit(new_order(
        1,
        1,
        11,
        Side::Sell,
        30,
        reserve(10),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    let report = book
        .submit(new_order(
            2,
            2,
            11,
            Side::Buy,
            15,
            OrderDisplay::FullyDisplayed,
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::ImmediateOrCancel,
            SelfTradePrevention::DecrementAndCancel,
        ))
        .unwrap();
    let prevented: Vec<_> = report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::SelfTradePrevented { quantity, .. } => Some(quantity.lots()),
            _ => None,
        })
        .collect();
    assert_eq!(prevented, vec![10, 5]);
    assert_eq!(maker_trades(&report), Vec::<(u64, u64)>::new());
    let order = book.order(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(order.leaves_quantity.lots(), 15);
    assert_eq!(order.displayed_quantity.lots(), 5);
    assert_eq!(book.best_ask().unwrap().quantity, 5);
    book.validate().unwrap();
}

#[test]
fn public_incrementals_expose_only_each_active_peak_across_refresh() {
    let mut book = OrderBook::new(definition(ReserveOrderRules::new(16).unwrap()));
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::new(
        InstrumentId::new(1).unwrap(),
        InstrumentVersion::new(1).unwrap(),
        TradingState::Open,
    );
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    let resting = new_order(
        1,
        1,
        11,
        Side::Sell,
        30,
        reserve(10),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let report = book.submit(resting).unwrap();
    let batch = publisher.publish(resting, &report, &book).unwrap();
    replica.apply_batch(&batch).unwrap();
    assert_eq!(replica.depth(Side::Sell, 1)[0].quantity, 10);

    let taking = new_order(
        2,
        2,
        12,
        Side::Buy,
        15,
        OrderDisplay::FullyDisplayed,
        OrderType::Market,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    );
    let report = book.submit(taking).unwrap();
    let batch = publisher.publish(taking, &report, &book).unwrap();
    let visible_states: Vec<_> = batch
        .updates()
        .iter()
        .filter_map(|update| match update.kind() {
            MarketDataKind::NoBookChange | MarketDataKind::TradingState { .. } => None,
            MarketDataKind::Level(level) => Some((level.quantity, level.order_count)),
            MarketDataKind::Trade { maker_level, .. } => {
                Some((maker_level.quantity, maker_level.order_count))
            }
        })
        .collect();
    assert_eq!(visible_states, vec![(0, 0), (10, 1), (5, 1)]);
    replica.apply_batch(&batch).unwrap();

    let private = book.order(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(private.leaves_quantity.lots(), 15);
    assert_eq!(private.displayed_quantity.lots(), 5);
    assert_eq!(replica.depth(Side::Sell, 1)[0].quantity, 5);
    assert_eq!(replica.depth(Side::Sell, 1), book.depth(Side::Sell, 1));
    publisher.validate_against(&book).unwrap();
}

#[test]
fn risk_reserves_total_hidden_leaves_and_refresh_has_no_independent_risk_effect() {
    let mut book = RiskManagedOrderBook::new(definition(ReserveOrderRules::new(16).unwrap()));
    let limits = RiskLimits::new(RiskLimitSpec {
        max_order_quantity_lots: 100,
        max_order_notional: 100_000,
        max_open_orders: 4,
        max_open_quantity_lots: 100,
        max_open_notional: 100_000,
        max_long_position_lots: 100,
        max_short_position_lots: 100,
    })
    .unwrap();
    for owner in [11, 12] {
        book.register_account(
            AccountId::new(owner).unwrap(),
            RiskProfile::new(AccountRiskState::Active, 0, limits).unwrap(),
        )
        .unwrap();
    }

    book.submit(new_order(
        1,
        1,
        11,
        Side::Sell,
        30,
        reserve(10),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    assert_eq!(
        book.risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        30
    );
    assert_eq!(
        book.risk()
            .snapshot(AccountId::new(11).unwrap())
            .unwrap()
            .open_sell_lots(),
        30
    );
    assert_eq!(book.book().best_ask().unwrap().quantity, 10);

    book.submit(new_order(
        2,
        2,
        12,
        Side::Buy,
        15,
        OrderDisplay::FullyDisplayed,
        OrderType::Market,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    let seller = book.risk().snapshot(AccountId::new(11).unwrap()).unwrap();
    assert_eq!(seller.position_lots(), -15);
    assert_eq!(seller.open_sell_lots(), 15);
    assert_eq!(
        book.risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        15
    );
    assert_eq!(book.book().best_ask().unwrap().quantity, 5);
    book.validate().unwrap();
}

struct WalPath(PathBuf);

impl WalPath {
    fn new() -> Self {
        let nonce = NEXT_WAL.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-reserve-{}-{nonce}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }
}

impl Drop for WalPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn reserve_definition_commands_and_post_refresh_state_survive_wal_recovery() {
    let definition = definition(ReserveOrderRules::new(16).unwrap());
    assert_eq!(
        InstrumentDefinition::decode(&definition.encode().unwrap()).unwrap(),
        definition
    );
    let resting = new_order(
        1,
        1,
        11,
        Side::Sell,
        30,
        reserve(10),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    assert_eq!(
        Command::decode(&resting.encode().unwrap()).unwrap(),
        resting
    );
    let taking = new_order(
        2,
        2,
        12,
        Side::Buy,
        15,
        OrderDisplay::FullyDisplayed,
        OrderType::Market,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    );
    let path = WalPath::new();
    let options = JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let mut durable = DurableOrderBook::open(&path.0, definition, options).unwrap();
    durable.submit(resting).unwrap();
    let live_report = durable.submit(taking).unwrap();
    durable.sync_all().unwrap();
    let live_sequence = durable.book().last_event_sequence();
    let live_trade_id = durable.book().last_trade_id();
    drop(durable);

    let recovered = DurableOrderBook::open(&path.0, definition, options).unwrap();
    assert_eq!(recovered.recovery().replayed_commands, 2);
    assert_eq!(recovered.book().last_event_sequence(), live_sequence);
    assert_eq!(recovered.book().last_trade_id(), live_trade_id);
    let private = recovered.book().order(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(private.leaves_quantity.lots(), 15);
    assert_eq!(private.displayed_quantity.lots(), 5);
    assert_eq!(recovered.book().best_ask().unwrap().quantity, 5);
    assert!(
        live_report
            .events
            .iter()
            .any(|event| matches!(event.kind, EventKind::OrderRefreshed { .. }))
    );
    recovered.book().validate().unwrap();
    MarketDataPublisher::from_book(recovered.book()).unwrap();
}

#[test]
fn durable_risk_recovery_restores_total_reservation_not_visible_peak() {
    let definition = definition(ReserveOrderRules::new(16).unwrap());
    let limits = RiskLimits::new(RiskLimitSpec {
        max_order_quantity_lots: 100,
        max_order_notional: 100_000,
        max_open_orders: 4,
        max_open_quantity_lots: 100,
        max_open_notional: 100_000,
        max_long_position_lots: 100,
        max_short_position_lots: 100,
    })
    .unwrap();
    let profiles = [11, 12].map(|owner| {
        AccountRiskDefinition::new(
            AccountId::new(owner).unwrap(),
            RiskProfile::new(AccountRiskState::Active, 0, limits).unwrap(),
        )
    });
    let resting = new_order(
        1,
        1,
        11,
        Side::Sell,
        30,
        reserve(10),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let taking = new_order(
        2,
        2,
        12,
        Side::Buy,
        15,
        OrderDisplay::FullyDisplayed,
        OrderType::Market,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    );
    let path = WalPath::new();
    let options = JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let mut durable = DurableRiskOrderBook::open(&path.0, definition, &profiles, options).unwrap();
    durable.submit(resting).unwrap();
    durable.submit(taking).unwrap();
    durable.sync_all().unwrap();
    drop(durable);

    let recovered = DurableRiskOrderBook::open(&path.0, definition, &profiles, options).unwrap();
    assert_eq!(recovered.recovery().replayed_commands, 2);
    assert_eq!(
        recovered
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        15
    );
    assert_eq!(recovered.managed().book().best_ask().unwrap().quantity, 5);
    recovered.managed().validate().unwrap();
}
