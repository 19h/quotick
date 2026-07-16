use std::cell::Cell;
use std::panic::{AssertUnwindSafe, catch_unwind};

use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::{
    Command, CommandOutcome, EventKind, ImmediateExecutionCurveOutcome, ImmediateExecutionOutcome,
    ImmediateExecutionRequest, ImmediateExecutionSubmission, ImmediateExecutionTermination,
    NewOrder, OrderBook, OrderDisplay, OrderType, RejectReason, SelfTradePrevention,
    StopActivation, TimeInForce,
};
use quotick::risk::{
    AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedOrderBook, RiskProfile,
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
        symbol: InstrumentSymbol::new("CONDITIONAL-IOC").unwrap(),
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

fn limit(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    price: i64,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn submission(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    instrument_id: u64,
    quantity: u64,
) -> ImmediateExecutionSubmission {
    ImmediateExecutionSubmission::new(
        CommandId::new(command_id).unwrap(),
        OrderId::new(order_id).unwrap(),
        InstrumentId::new(instrument_id).unwrap(),
        InstrumentVersion::new(1).unwrap(),
        TimestampNs::from_unix_nanos(command_id),
        ImmediateExecutionRequest::new(
            AccountId::new(account_id).unwrap(),
            Side::Buy,
            Quantity::new(quantity).unwrap(),
            StopActivation::Limit(Price::from_raw(100)),
            SelfTradePrevention::CancelAggressor,
        ),
    )
}

fn profile(max_order_quantity_lots: u64) -> RiskProfile {
    RiskProfile::new(
        AccountRiskState::Active,
        0,
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots,
            max_order_notional: 100_000,
            max_open_orders: 10,
            max_open_quantity_lots: 100,
            max_open_notional: 100_000,
            max_long_position_lots: 100,
            max_short_position_lots: 100,
        })
        .unwrap(),
    )
    .unwrap()
}

fn report_execution_totals(report: &quotick::matching::ExecutionReport) -> (u64, i128) {
    report.events.iter().fold((0_u64, 0_i128), |total, event| {
        let EventKind::Trade(trade) = event.kind else {
            return total;
        };
        (
            total.0 + trade.quantity.lots(),
            total.1 + i128::from(trade.price.raw()) * i128::from(trade.quantity.lots()),
        )
    })
}

fn report_execution_levels(report: &quotick::matching::ExecutionReport) -> Vec<(Price, u64, i128)> {
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
fn declined_quote_is_nonmutating_and_an_accepted_quote_matches_the_committed_trace() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Sell, 5, 100)).unwrap();
    let value = submission(2, 2, 12, 1, 3);
    let sequence_before = book.last_event_sequence();
    let Command::New(canonical) = value.command() else {
        panic!("submission maps only to a new order");
    };
    assert_eq!(canonical.display, OrderDisplay::FullyDisplayed);
    assert_eq!(canonical.time_in_force, TimeInForce::ImmediateOrCancel);
    assert_eq!(canonical.order_type, OrderType::Limit(Price::from_raw(100)));
    assert_eq!(canonical.account_id, value.request().account_id());

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.submit_immediate_execution_if(value, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert!(book.order(OrderId::new(2).unwrap()).is_none());

    let declined = book
        .submit_immediate_execution_if(value, |quote| {
            assert_eq!(quote.book_event_sequence(), sequence_before);
            assert_eq!(quote.executed_quantity_lots(), 3);
            assert_eq!(quote.raw_price_notional(), 300);
            assert_eq!(quote.termination(), ImmediateExecutionTermination::Filled);
            false
        })
        .unwrap();
    let ImmediateExecutionOutcome::Declined(quote) = declined else {
        panic!("the predicate declined the immutable quote");
    };
    assert_eq!(quote.request(), value.request());
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert!(book.order(OrderId::new(2).unwrap()).is_none());
    assert_eq!(book.best_ask().unwrap().quantity, 5);

    let accepted = book
        .submit_immediate_execution_if(value, |quote| quote.worst_execution_price().is_some())
        .unwrap();
    let ImmediateExecutionOutcome::Reported {
        quote: Some(committed_quote),
        report,
    } = accepted
    else {
        panic!("accepted conditional execution retains its exact quote");
    };
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(
        report_execution_totals(&report),
        (
            committed_quote.executed_quantity_lots(),
            committed_quote.raw_price_notional(),
        )
    );
    assert_eq!(book.best_ask().unwrap().quantity, 2);

    let replay = book
        .submit_immediate_execution_if(value, |_| panic!("replay must bypass the predicate"))
        .unwrap();
    assert!(matches!(
        replay,
        ImmediateExecutionOutcome::Reported {
            quote: None,
            report
        } if report.replayed
    ));
    book.validate().unwrap();
}

#[test]
fn core_and_risk_rejections_bypass_the_acceptance_predicate() {
    let mut book = OrderBook::new(definition());
    let called = Cell::new(false);
    let outcome = book
        .submit_immediate_execution_if(submission(1, 1, 12, 2, 3), |_| {
            called.set(true);
            true
        })
        .unwrap();
    assert!(!called.get());
    assert!(matches!(
        outcome,
        ImmediateExecutionOutcome::Reported {
            quote: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::WrongInstrument)
    ));

    let mut managed = RiskManagedOrderBook::new(definition());
    managed
        .register_account(AccountId::new(11).unwrap(), profile(10))
        .unwrap();
    managed
        .register_account(AccountId::new(12).unwrap(), profile(2))
        .unwrap();
    managed
        .register_account(AccountId::new(13).unwrap(), profile(10))
        .unwrap();
    managed.submit(limit(2, 2, 11, Side::Sell, 5, 100)).unwrap();

    called.set(false);
    let rejected = managed
        .submit_immediate_execution_if(submission(3, 3, 12, 1, 3), |_| {
            called.set(true);
            true
        })
        .unwrap();
    assert!(!called.get());
    assert!(matches!(
        rejected,
        ImmediateExecutionOutcome::Reported {
            quote: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::RiskOrderQuantityLimit)
    ));
    assert_eq!(managed.book().best_ask().unwrap().quantity, 5);

    let accepted = managed
        .submit_immediate_execution_if(submission(4, 4, 13, 1, 3), |_| true)
        .unwrap();
    assert!(matches!(
        accepted,
        ImmediateExecutionOutcome::Reported {
            quote: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(
        managed
            .risk()
            .snapshot(AccountId::new(13).unwrap())
            .unwrap()
            .position_lots(),
        3
    );
    managed.validate().unwrap();
}

#[test]
fn conditional_curve_is_atomic_and_matches_the_committed_price_trace() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Sell, 5, 99)).unwrap();
    book.submit(limit(2, 2, 12, Side::Sell, 4, 100)).unwrap();
    let value = submission(3, 3, 13, 1, 7);
    let sequence_before = book.last_event_sequence();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ =
            book.try_submit_immediate_execution_curve_if(value, |_| panic!("curve policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert!(book.order(OrderId::new(3).unwrap()).is_none());

    let declined = book
        .try_submit_immediate_execution_curve_if(value, |curve| {
            assert_eq!(curve.quote().book_event_sequence(), sequence_before);
            assert_eq!(curve.quote().executed_quantity_lots(), 7);
            assert_eq!(curve.levels().len(), 2);
            assert_eq!(curve.levels()[0].price(), Price::from_raw(99));
            assert_eq!(curve.levels()[0].executed_quantity_lots(), 5);
            assert_eq!(curve.levels()[1].price(), Price::from_raw(100));
            assert_eq!(curve.levels()[1].executed_quantity_lots(), 2);
            false
        })
        .unwrap();
    let ImmediateExecutionCurveOutcome::Declined(curve) = declined else {
        panic!("the predicate declined the exact curve");
    };
    assert_eq!(curve.quote().request(), value.request());
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert_eq!(book.best_ask().unwrap().quantity, 5);

    let accepted = book
        .try_submit_immediate_execution_curve_if(value, |curve| {
            curve.quote().termination() == ImmediateExecutionTermination::Filled
        })
        .unwrap();
    let ImmediateExecutionCurveOutcome::Reported {
        curve: Some(committed_curve),
        report,
    } = accepted
    else {
        panic!("accepted curve decision retains its observation and report");
    };
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(
        report_execution_levels(&report),
        committed_curve
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
    assert_eq!(book.best_ask().unwrap().quantity, 2);

    let replay = book
        .try_submit_immediate_execution_curve_if(value, |_| {
            panic!("replay must bypass curve allocation and predicate")
        })
        .unwrap();
    assert!(matches!(
        replay,
        ImmediateExecutionCurveOutcome::Reported {
            curve: None,
            report
        } if report.replayed
    ));
    book.validate().unwrap();
}

#[test]
fn core_and_risk_rejections_bypass_curve_construction_and_predicate() {
    let mut book = OrderBook::new(definition());
    let called = Cell::new(false);
    let outcome = book
        .try_submit_immediate_execution_curve_if(submission(1, 1, 12, 2, 3), |_| {
            called.set(true);
            true
        })
        .unwrap();
    assert!(!called.get());
    assert!(matches!(
        outcome,
        ImmediateExecutionCurveOutcome::Reported {
            curve: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::WrongInstrument)
    ));

    let mut managed = RiskManagedOrderBook::new(definition());
    managed
        .register_account(AccountId::new(11).unwrap(), profile(10))
        .unwrap();
    managed
        .register_account(AccountId::new(12).unwrap(), profile(2))
        .unwrap();
    managed
        .register_account(AccountId::new(13).unwrap(), profile(10))
        .unwrap();
    managed.submit(limit(2, 2, 11, Side::Sell, 5, 100)).unwrap();

    called.set(false);
    let rejected = managed
        .try_submit_immediate_execution_curve_if(submission(3, 3, 12, 1, 3), |_| {
            called.set(true);
            true
        })
        .unwrap();
    assert!(!called.get());
    assert!(matches!(
        rejected,
        ImmediateExecutionCurveOutcome::Reported {
            curve: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::RiskOrderQuantityLimit)
    ));
    assert_eq!(managed.book().best_ask().unwrap().quantity, 5);

    let accepted = managed
        .try_submit_immediate_execution_curve_if(submission(4, 4, 13, 1, 3), |curve| {
            curve.levels().len() == 1 && curve.quote().raw_price_notional() == 300
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ImmediateExecutionCurveOutcome::Reported {
            curve: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(
        managed
            .risk()
            .snapshot(AccountId::new(13).unwrap())
            .unwrap()
            .position_lots(),
        3
    );
    managed.validate().unwrap();
}
