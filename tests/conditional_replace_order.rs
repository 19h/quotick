use std::cell::Cell;
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable::DurableOrderBook;
use quotick::durable_risk::DurableRiskOrderBook;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions, JournalReader};
use quotick::matching::{
    Command, CommandOutcome, ConditionalOrderOutcome, EventKind, ImmediateExecutionTermination,
    NewOrder, OrderBook, OrderDisplay, OrderExecution, OrderType, RejectReason, ReplaceOrder,
    SelfTradePrevention, StopActivation, StopReference, StopReferenceCursor, StopTriggerSweep,
    TimeInForce,
};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedOrderBook,
    RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    StopReferenceSequence, StopReferenceSourceId, StopReferenceSourceVersion, TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-conditional-replace-{label}-{}-{nonce}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("CONDITIONAL-REPLACE").unwrap(),
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

fn new_order(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    price: i64,
) -> NewOrder {
    NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn limit(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    price: i64,
) -> Command {
    Command::New(new_order(
        command_id, order_id, owner, side, quantity, price,
    ))
}

fn replace(command_id: u64, order_id: u64, owner: u64, quantity: u64, price: i64) -> ReplaceOrder {
    ReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        new_quantity: Quantity::new(quantity).unwrap(),
        new_price: Price::from_raw(price),
        new_display: OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn stop_reference(command_id: u64, price: i64) -> Command {
    Command::StopTriggerSweep(StopTriggerSweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        reference: StopReference::new(
            StopReferenceCursor::new(
                StopReferenceSourceId::new(1).unwrap(),
                StopReferenceSourceVersion::new(1).unwrap(),
                StopReferenceSequence::new(1).unwrap(),
            ),
            Price::from_raw(price),
        ),
        maximum_orders: 1,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
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

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn frame_count(path: &Path) -> usize {
    JournalReader::open(path, options())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .len()
}

fn report_levels(report: &quotick::matching::ExecutionReport) -> Vec<(Price, u64)> {
    let mut levels = Vec::<(Price, u64)>::new();
    for event in &report.events {
        let EventKind::Trade(trade) = event.kind else {
            continue;
        };
        if let Some(last) = levels.last_mut()
            && last.0 == trade.price
        {
            last.1 += trade.quantity.lots();
        } else {
            levels.push((trade.price, trade.quantity.lots()));
        }
    }
    levels
}

#[test]
fn active_replacement_curve_decline_unwind_accept_and_replay_are_atomic() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Buy, 5, 90)).unwrap();
    book.submit(limit(2, 2, 12, Side::Sell, 3, 99)).unwrap();
    book.submit(limit(3, 3, 13, Side::Sell, 5, 100)).unwrap();
    let amendment = replace(4, 1, 11, 7, 100);
    let original = book.order(OrderId::new(1).unwrap()).unwrap();
    let sequence_before = book.last_event_sequence();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.try_submit_replace_order_curve_if(amendment, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(book.order(OrderId::new(1).unwrap()).unwrap(), original);
    assert_eq!(book.last_event_sequence(), sequence_before);

    let declined = book
        .try_submit_replace_order_curve_if(amendment, |observation| {
            let OrderExecution::Active(curve) = observation else {
                panic!("an active replacement has an execution curve");
            };
            assert_eq!(curve.quote().book_event_sequence(), sequence_before);
            assert_eq!(curve.quote().executed_quantity_lots(), 7);
            assert_eq!(curve.quote().unfilled_quantity_lots(), 0);
            assert_eq!(curve.levels().len(), 2);
            assert_eq!(curve.levels()[0].price(), Price::from_raw(99));
            assert_eq!(curve.levels()[0].executed_quantity_lots(), 3);
            assert_eq!(curve.levels()[1].price(), Price::from_raw(100));
            assert_eq!(curve.levels()[1].executed_quantity_lots(), 4);
            false
        })
        .unwrap();
    assert!(matches!(
        declined,
        ConditionalOrderOutcome::Declined(OrderExecution::Active(_))
    ));
    assert_eq!(book.order(OrderId::new(1).unwrap()).unwrap(), original);

    let accepted = book
        .try_submit_replace_order_curve_if(amendment, |observation| {
            matches!(
                observation,
                OrderExecution::Active(curve)
                    if curve.quote().termination()
                        == ImmediateExecutionTermination::Filled
            )
        })
        .unwrap();
    let ConditionalOrderOutcome::Reported {
        observation: Some(OrderExecution::Active(curve)),
        report,
    } = accepted
    else {
        panic!("accepted replacement retains its curve");
    };
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(
        report_levels(&report),
        curve
            .levels()
            .iter()
            .map(|level| (level.price(), level.executed_quantity_lots()))
            .collect::<Vec<_>>()
    );
    assert!(book.order(OrderId::new(1).unwrap()).is_none());
    assert_eq!(book.best_ask().unwrap().quantity, 1);

    assert!(matches!(
        book.submit_replace_order_if(amendment, |_| panic!("replay bypasses observation"))
            .unwrap(),
        ConditionalOrderOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));
    book.validate().unwrap();
}

#[test]
fn priority_retaining_reduction_has_empty_active_quote() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Sell, 5, 100)).unwrap();
    book.submit(limit(2, 2, 12, Side::Sell, 5, 100)).unwrap();

    let accepted = book
        .submit_replace_order_if(replace(3, 1, 11, 3, 100), |observation| {
            let OrderExecution::Active(quote) = observation else {
                panic!("a resting order replacement remains active");
            };
            assert_eq!(quote.executed_quantity_lots(), 0);
            assert_eq!(quote.unfilled_quantity_lots(), 3);
            true
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalOrderOutcome::Reported {
            observation: Some(OrderExecution::Active(_)),
            report
        } if matches!(
            report.events[0].kind,
            EventKind::OrderReplaced {
                priority_retained: true,
                ..
            }
        )
    ));

    let fill = book
        .submit(Command::New({
            let mut order = new_order(4, 3, 13, Side::Buy, 3, 100);
            order.time_in_force = TimeInForce::ImmediateOrCancel;
            order
        }))
        .unwrap();
    assert!(fill.events.iter().any(|event| matches!(
        event.kind,
        EventKind::Trade(trade) if trade.maker_order_id == OrderId::new(1).unwrap()
    )));
    book.validate().unwrap();
}

#[test]
fn rejected_and_dormant_replacements_have_explicit_predicate_semantics() {
    let mut book = OrderBook::new(definition());
    let called = Cell::new(false);
    let rejected = book
        .submit_replace_order_if(replace(1, 99, 11, 1, 100), |_| {
            called.set(true);
            true
        })
        .unwrap();
    assert!(!called.get());
    assert!(matches!(
        rejected,
        ConditionalOrderOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::UnknownOrder)
    ));

    book.submit(stop_reference(2, 100)).unwrap();
    let mut stop = new_order(3, 1, 11, Side::Buy, 5, 101);
    stop.order_type = OrderType::Stop {
        trigger_price: Price::from_raw(110),
        activation: StopActivation::Limit(Price::from_raw(101)),
    };
    book.submit(Command::New(stop)).unwrap();
    let original = book.dormant_stop(OrderId::new(1).unwrap()).unwrap();

    assert_eq!(
        book.try_submit_replace_order_curve_if(replace(4, 1, 11, 3, 102), |observation| {
            assert_eq!(observation, &OrderExecution::DormantStop);
            false
        })
        .unwrap(),
        ConditionalOrderOutcome::Declined(OrderExecution::DormantStop)
    );
    assert_eq!(
        book.dormant_stop(OrderId::new(1).unwrap()).unwrap(),
        original
    );

    let accepted = book
        .submit_replace_order_if(replace(4, 1, 11, 3, 102), |observation| {
            assert_eq!(observation, &OrderExecution::DormantStop);
            true
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalOrderOutcome::Reported {
            observation: Some(OrderExecution::DormantStop),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    let replaced = book.dormant_stop(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(replaced.leaves_quantity.lots(), 3);
    assert_eq!(
        replaced.activation,
        StopActivation::Limit(Price::from_raw(102))
    );
    book.validate().unwrap();
}

#[test]
fn coupled_risk_rejection_bypasses_observation_and_acceptance_nets_reservation() {
    let mut managed = RiskManagedOrderBook::new(definition());
    managed.register_account(account(11), profile(10)).unwrap();
    managed.register_account(account(12), profile(10)).unwrap();
    managed.submit(limit(1, 1, 11, Side::Buy, 2, 90)).unwrap();
    managed.submit(limit(2, 2, 12, Side::Sell, 5, 100)).unwrap();

    let rejected = managed
        .submit_replace_order_if(replace(3, 1, 11, 11, 100), |_| {
            panic!("risk rejection bypasses observation")
        })
        .unwrap();
    assert!(matches!(
        rejected,
        ConditionalOrderOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::RiskOrderQuantityLimit)
    ));
    assert_eq!(
        managed
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        2
    );

    let accepted = managed
        .try_submit_replace_order_curve_if(replace(4, 1, 11, 7, 100), |observation| {
            matches!(
                observation,
                OrderExecution::Active(curve)
                    if curve.quote().executed_quantity_lots() == 5
                        && curve.quote().unfilled_quantity_lots() == 2
            )
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalOrderOutcome::Reported {
            observation: Some(OrderExecution::Active(_)),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(
        managed
            .risk()
            .snapshot(account(11))
            .unwrap()
            .position_lots(),
        5
    );
    assert_eq!(
        managed
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        2
    );
    managed.validate().unwrap();
}

#[test]
fn durable_plain_decline_accept_replay_and_reopen_preserve_frame_contract() {
    let file = TestFile::new("plain");
    let mut durable = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    durable.submit(limit(1, 1, 11, Side::Buy, 2, 90)).unwrap();
    durable.submit(limit(2, 2, 12, Side::Sell, 5, 100)).unwrap();
    let amendment = replace(3, 1, 11, 7, 100);
    let frames_before = frame_count(file.path());

    assert!(matches!(
        durable
            .try_submit_replace_order_curve_if(amendment, |_| false)
            .unwrap(),
        ConditionalOrderOutcome::Declined(OrderExecution::Active(_))
    ));
    assert_eq!(frame_count(file.path()), frames_before);

    assert!(matches!(
        durable
            .try_submit_replace_order_curve_if(amendment, |_| true)
            .unwrap(),
        ConditionalOrderOutcome::Reported {
            observation: Some(OrderExecution::Active(_)),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .submit_replace_order_if(amendment, |_| panic!("replay bypasses observation"))
            .unwrap(),
        ConditionalOrderOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));
    assert_eq!(frame_count(file.path()), frames_after);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    assert_eq!(
        reopened
            .book()
            .order(OrderId::new(1).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        2
    );
    reopened.book().validate().unwrap();
}

#[test]
fn durable_risk_decline_accept_replay_and_reopen_preserve_coupled_state() {
    let file = TestFile::new("risk");
    let profiles = [
        AccountRiskDefinition::new(account(11), profile(10)),
        AccountRiskDefinition::new(account(12), profile(10)),
    ];
    let mut durable =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles, options()).unwrap();
    durable.submit(limit(1, 1, 11, Side::Buy, 2, 90)).unwrap();
    durable.submit(limit(2, 2, 12, Side::Sell, 5, 100)).unwrap();
    let amendment = replace(3, 1, 11, 7, 100);
    let frames_before = frame_count(file.path());

    assert!(matches!(
        durable
            .try_submit_replace_order_curve_if(amendment, |_| false)
            .unwrap(),
        ConditionalOrderOutcome::Declined(OrderExecution::Active(_))
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert_eq!(
        durable
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        2
    );

    assert!(matches!(
        durable
            .try_submit_replace_order_curve_if(amendment, |_| true)
            .unwrap(),
        ConditionalOrderOutcome::Reported {
            observation: Some(OrderExecution::Active(_)),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .submit_replace_order_if(amendment, |_| panic!("replay bypasses observation"))
            .unwrap(),
        ConditionalOrderOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));
    assert_eq!(frame_count(file.path()), frames_after);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles, options()).unwrap();
    assert_eq!(
        reopened
            .managed()
            .risk()
            .snapshot(account(11))
            .unwrap()
            .position_lots(),
        5
    );
    assert_eq!(
        reopened
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        2
    );
    reopened.managed().validate().unwrap();
}
