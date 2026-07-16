use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::{
    Command, EventKind, ImmediateExecutionOutcome, ImmediateExecutionRequest,
    ImmediateExecutionSubmission, ImmediateExecutionTermination, NewOrder, OrderBook,
    OrderBookQueryError, OrderBookQueryResource, OrderDisplay, OrderType, SelfTradePrevention,
    StopActivation, TimeInForce,
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
        symbol: InstrumentSymbol::new("EXECUTION-CURVE").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(-1_000), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 100).unwrap(),
        reserve: ReserveOrderRules::new(32).unwrap(),
        hidden_orders_supported: true,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn limit(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    price: i64,
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

fn execution_levels(report: &quotick::matching::ExecutionReport) -> Vec<(Price, u64, i128)> {
    let mut levels = Vec::<(Price, u64, i128)>::new();
    for event in &report.events {
        let EventKind::Trade(trade) = event.kind else {
            continue;
        };
        let quantity = trade.quantity.lots();
        let notional = i128::from(trade.price.raw()) * i128::from(quantity);
        if let Some(last) = levels.last_mut()
            && last.0 == trade.price
        {
            last.1 += quantity;
            last.2 += notional;
        } else {
            levels.push((trade.price, quantity, notional));
        }
    }
    levels
}

#[test]
fn private_execution_curve_matches_the_quote_and_committed_price_trace() {
    let mut book = OrderBook::new(definition());
    for command in [
        limit(
            1,
            1,
            11,
            Side::Sell,
            6,
            99,
            OrderDisplay::Reserve {
                peak: Quantity::new(2).unwrap(),
            },
        ),
        limit(2, 2, 42, Side::Sell, 2, 99, OrderDisplay::FullyDisplayed),
        limit(3, 3, 12, Side::Sell, 3, 99, OrderDisplay::Hidden),
        limit(4, 4, 13, Side::Sell, 4, 100, OrderDisplay::FullyDisplayed),
        limit(5, 5, 14, Side::Sell, 5, 101, OrderDisplay::FullyDisplayed),
    ] {
        book.submit(command).unwrap();
    }

    let request = ImmediateExecutionRequest::new(
        AccountId::new(42).unwrap(),
        Side::Buy,
        Quantity::new(13).unwrap(),
        StopActivation::Limit(Price::from_raw(100)),
        SelfTradePrevention::CancelResting,
    );
    let curve = book.try_immediate_execution_curve(request).unwrap();
    assert_eq!(curve.quote(), book.immediate_execution_quote(request));
    assert_eq!(curve.quote().contributing_level_count(), 2);
    assert_eq!(curve.levels().len(), 2);
    assert_eq!(curve.levels()[0].price(), Price::from_raw(99));
    assert_eq!(curve.levels()[0].executed_quantity_lots(), 9);
    assert_eq!(curve.levels()[0].raw_price_notional(), 891);
    assert_eq!(curve.levels()[1].price(), Price::from_raw(100));
    assert_eq!(curve.levels()[1].executed_quantity_lots(), 4);
    assert_eq!(curve.levels()[1].raw_price_notional(), 400);
    assert_eq!(
        curve.quote().termination(),
        ImmediateExecutionTermination::Filled
    );

    let outcome = book
        .submit_immediate_execution_if(
            ImmediateExecutionSubmission::new(
                CommandId::new(6).unwrap(),
                OrderId::new(6).unwrap(),
                InstrumentId::new(1).unwrap(),
                InstrumentVersion::new(1).unwrap(),
                TimestampNs::from_unix_nanos(6),
                request,
            ),
            |_| true,
        )
        .unwrap();
    let ImmediateExecutionOutcome::Reported {
        quote: Some(committed_quote),
        report,
    } = outcome
    else {
        panic!("accepted conditional IOC must retain its quote and report");
    };
    assert_eq!(committed_quote, curve.quote());
    assert_eq!(
        execution_levels(&report),
        curve
            .levels()
            .iter()
            .map(|level| {
                (
                    level.price(),
                    level.executed_quantity_lots(),
                    level.raw_price_notional(),
                )
            })
            .collect::<Vec<_>>()
    );
    book.validate().unwrap();
}

#[test]
fn decrement_and_cancel_curve_excludes_self_consumption_and_retains_termination() {
    let mut book = OrderBook::new(definition());
    for command in [
        limit(1, 1, 11, Side::Buy, 2, 101, OrderDisplay::FullyDisplayed),
        limit(
            2,
            2,
            42,
            Side::Buy,
            4,
            101,
            OrderDisplay::Reserve {
                peak: Quantity::new(2).unwrap(),
            },
        ),
        limit(3, 3, 12, Side::Buy, 4, 101, OrderDisplay::Hidden),
    ] {
        book.submit(command).unwrap();
    }
    let request = ImmediateExecutionRequest::new(
        AccountId::new(42).unwrap(),
        Side::Sell,
        Quantity::new(5).unwrap(),
        StopActivation::Market,
        SelfTradePrevention::DecrementAndCancel,
    );
    let sequence_before = book.last_event_sequence();
    let curve = book.try_immediate_execution_curve(request).unwrap();
    assert_eq!(curve.quote().executed_quantity_lots(), 2);
    assert_eq!(curve.quote().self_trade_decrement_quantity_lots(), 3);
    assert_eq!(curve.quote().unfilled_quantity_lots(), 0);
    assert_eq!(
        curve.quote().termination(),
        ImmediateExecutionTermination::SelfTradePrevention
    );
    assert_eq!(curve.quote().contributing_level_count(), 1);
    assert_eq!(curve.levels().len(), 1);
    assert_eq!(curve.levels()[0].price(), Price::from_raw(101));
    assert_eq!(curve.levels()[0].executed_quantity_lots(), 2);
    assert_eq!(curve.levels()[0].raw_price_notional(), 202);
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert_eq!(book.best_bid().unwrap().quantity, 4);
    book.validate().unwrap();
}

#[test]
fn execution_curve_output_failure_names_its_exact_resource() {
    let error = OrderBookQueryError::ReservationFailed {
        resource: OrderBookQueryResource::ImmediateExecutionLevels,
        maximum: 250_000,
    };
    assert_eq!(
        error.to_string(),
        "order-book immediate-execution-level output could not reserve 250000 entries"
    );
}
