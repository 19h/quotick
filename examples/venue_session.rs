//! Calendar-bound admission through risk, matching, public depth, and settlement.

mod support;

use quotick::calendar::{CalendarTimeInForce, TradingCalendar, TradingSession};
use quotick::ledger::Ledger;
use quotick::market_data::{
    MarketDataBatch, MarketDataLimits, MarketDataPublisher, MarketDataReplica,
};
use quotick::matching::{Command, ExecutionReport, TimeInForce};
use quotick::risk::RiskManagedOrderBook;
use quotick::{
    AccountingDate, CalendarId, CalendarVersion, CommandId, Side, TradingSessionId, TransactionId,
};

use support::{
    LimitOrder, account, asset, definition, ledger, profile, risk_limits, timestamp, trades,
};

fn execute(
    managed: &mut RiskManagedOrderBook,
    publisher: &mut MarketDataPublisher,
    replica: &mut MarketDataReplica,
    command: Command,
) -> (ExecutionReport, MarketDataBatch) {
    let report = managed
        .submit(command)
        .expect("command is operationally valid");
    let batch = publisher
        .publish(command, &report, managed.book())
        .expect("matching trace publishes");
    replica.apply_batch(&batch).expect("replica applies batch");
    (report, batch)
}

#[allow(
    clippy::too_many_lines,
    reason = "one chronological example retains the complete session-to-settlement flow"
)]
fn main() {
    let definition = definition("SESSION-CORE");
    let trading_date = AccountingDate::from_days_since_unix_epoch(20_000);
    let session_id = TradingSessionId::new(1).unwrap();
    let calendar = TradingCalendar::try_new(
        CalendarId::new(1).unwrap(),
        CalendarVersion::new(1).unwrap(),
        timestamp(90),
        vec![
            TradingSession::new(
                session_id,
                trading_date,
                timestamp(100),
                timestamp(400),
                timestamp(450),
                timestamp(500),
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let day = calendar
        .resolve_time_in_force(CalendarTimeInForce::Day, timestamp(110))
        .unwrap();
    assert_eq!(
        day.normalized(),
        TimeInForce::GoodTilTimestamp {
            expires_at: timestamp(500)
        }
    );

    let mut managed = RiskManagedOrderBook::try_with_limits(definition, risk_limits()).unwrap();
    managed
        .register_account(account(101), profile(0, 100))
        .unwrap();
    managed
        .register_account(account(202), profile(0, 100))
        .unwrap();

    let mut publisher = MarketDataPublisher::from_book(managed.book()).unwrap();
    let market_data_limits = MarketDataLimits::from_order_book(managed.book().limits()).spec();
    let mut replica = MarketDataReplica::try_with_limits(
        definition.instrument_id(),
        definition.version(),
        definition.trading_state(),
        market_data_limits,
    )
    .unwrap();
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    let mut ask = LimitOrder::resting(1, 1, 202, Side::Sell, 8, 10_000);
    ask.time_in_force = day.normalized();
    execute(
        &mut managed,
        &mut publisher,
        &mut replica,
        ask.command(definition),
    );

    let mut take = LimitOrder::resting(2, 2, 101, Side::Buy, 3, 10_000);
    take.time_in_force = TimeInForce::ImmediateOrCancel;
    let (report, _) = execute(
        &mut managed,
        &mut publisher,
        &mut replica,
        take.command(definition),
    );
    let executions = trades(&report);
    assert_eq!(executions.len(), 1);
    let trade = executions[0];

    let mut ledger: Ledger = ledger();
    ledger
        .settle_instrument_trade(
            TransactionId::new(1).unwrap(),
            day.trading_date().unwrap(),
            timestamp(200),
            &trade,
            definition,
        )
        .unwrap();

    assert_eq!(
        managed
            .risk()
            .snapshot(account(101))
            .unwrap()
            .position_lots(),
        3
    );
    assert_eq!(
        managed
            .risk()
            .snapshot(account(202))
            .unwrap()
            .position_lots(),
        -3
    );
    assert_eq!(ledger.balance(account(101), asset(1)), 3);
    assert_eq!(ledger.balance(account(202), asset(1)), -3);
    assert_eq!(ledger.balance(account(101), asset(2)), -30_000);
    assert_eq!(ledger.balance(account(202), asset(2)), 30_000);
    assert_eq!(replica.best_ask(), managed.book().best_ask());
    assert_eq!(
        replica.last_sequence(),
        managed.book().last_event_sequence()
    );

    let sweep = calendar
        .expiry_sweep(
            session_id,
            quotick::calendar::OrderExpiryBoundary::TradingDay,
            CommandId::new(3).unwrap(),
            definition.instrument_id(),
            definition.version(),
            timestamp(500),
        )
        .unwrap();
    execute(
        &mut managed,
        &mut publisher,
        &mut replica,
        Command::ExpirySweep(sweep),
    );
    assert_eq!(managed.book().active_order_count(), 0);
    assert_eq!(replica.best_ask(), None);

    managed.validate().unwrap();
    publisher.validate_against(managed.book()).unwrap();
    replica.validate().unwrap();
    ledger.validate().unwrap();

    println!(
        "trade={} lots={} price={} ledger_records={} final_sequence={}",
        trade.trade_id,
        trade.quantity.lots(),
        trade.price.raw(),
        ledger.record_count(),
        managed.book().last_event_sequence()
    );
}
