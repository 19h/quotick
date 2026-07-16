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
    CancelReason, Command, CommandOutcome, ConditionalNewOrderOutcome, EventKind,
    ImmediateExecutionTermination, NewOrder, NewOrderExecution, OrderBook, OrderDisplay, OrderType,
    RejectReason, SelfTradePrevention, StopActivation, StopReference, StopReferenceCursor,
    StopTriggerSweep, TimeInForce,
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
            "quotick-conditional-new-order-{label}-{}-{nonce}.wal",
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
        symbol: InstrumentSymbol::new("CONDITIONAL-NEW").unwrap(),
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

fn new_order(command_id: u64, order_id: u64, owner: u64, side: Side, quantity: u64) -> NewOrder {
    NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(100)),
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
    let mut order = new_order(command_id, order_id, owner, side, quantity);
    order.order_type = OrderType::Limit(Price::from_raw(price));
    Command::New(order)
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
fn active_limit_curve_decline_unwind_accept_and_replay_are_atomic() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Sell, 5, 99)).unwrap();
    book.submit(limit(2, 2, 12, Side::Sell, 4, 100)).unwrap();
    let incoming = new_order(3, 3, 13, Side::Buy, 11);
    let sequence_before = book.last_event_sequence();

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = book.try_submit_new_order_curve_if(incoming, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert!(book.order(OrderId::new(3).unwrap()).is_none());

    let declined = book
        .try_submit_new_order_curve_if(incoming, |observation| {
            let NewOrderExecution::Active(curve) = observation else {
                panic!("an active limit order has an execution curve");
            };
            assert_eq!(curve.quote().book_event_sequence(), sequence_before);
            assert_eq!(curve.quote().executed_quantity_lots(), 9);
            assert_eq!(curve.quote().unfilled_quantity_lots(), 2);
            assert_eq!(curve.levels().len(), 2);
            assert_eq!(curve.levels()[0].price(), Price::from_raw(99));
            assert_eq!(curve.levels()[0].executed_quantity_lots(), 5);
            assert_eq!(curve.levels()[1].price(), Price::from_raw(100));
            assert_eq!(curve.levels()[1].executed_quantity_lots(), 4);
            false
        })
        .unwrap();
    let ConditionalNewOrderOutcome::Declined(NewOrderExecution::Active(curve)) = declined else {
        panic!("the active curve was declined");
    };
    assert_eq!(curve.quote().unfilled_quantity_lots(), 2);
    assert_eq!(book.last_event_sequence(), sequence_before);
    assert_eq!(book.best_ask().unwrap().quantity, 5);

    let accepted = book
        .try_submit_new_order_curve_if(incoming, |observation| {
            matches!(
                observation,
                NewOrderExecution::Active(curve)
                    if curve.quote().termination() == ImmediateExecutionTermination::BookExhausted
            )
        })
        .unwrap();
    let ConditionalNewOrderOutcome::Reported {
        observation: Some(NewOrderExecution::Active(committed_curve)),
        report,
    } = accepted
    else {
        panic!("accepted active order retains its curve");
    };
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(
        report_levels(&report),
        committed_curve
            .levels()
            .iter()
            .map(|level| (level.price(), level.executed_quantity_lots()))
            .collect::<Vec<_>>()
    );
    let residual = book.order(OrderId::new(3).unwrap()).unwrap();
    assert_eq!(
        (residual.price.raw(), residual.leaves_quantity.lots()),
        (100, 2)
    );

    let replay = book
        .try_submit_new_order_curve_if(incoming, |_| panic!("replay bypasses the predicate"))
        .unwrap();
    assert!(matches!(
        replay,
        ConditionalNewOrderOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));
    book.validate().unwrap();
}

#[test]
fn quote_observes_empty_post_only_and_core_rejections_bypass_the_predicate() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Sell, 5, 100)).unwrap();
    let mut post_only = new_order(2, 2, 12, Side::Buy, 3);
    post_only.order_type = OrderType::Limit(Price::from_raw(99));
    post_only.time_in_force = TimeInForce::PostOnly;

    let accepted = book
        .submit_new_order_if(post_only, |observation| {
            let NewOrderExecution::Active(quote) = observation else {
                panic!("a valid post-only order is active");
            };
            assert_eq!(quote.executed_quantity_lots(), 0);
            assert_eq!(quote.unfilled_quantity_lots(), 3);
            assert_eq!(quote.contributing_level_count(), 0);
            assert_eq!(
                quote.termination(),
                ImmediateExecutionTermination::PriceLimit
            );
            true
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalNewOrderOutcome::Reported {
            observation: Some(NewOrderExecution::Active(_)),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(book.best_bid().unwrap().price, Price::from_raw(99));

    let called = Cell::new(false);
    let mut wrong_instrument = new_order(3, 3, 13, Side::Buy, 1);
    wrong_instrument.instrument_id = InstrumentId::new(2).unwrap();
    let rejected = book
        .submit_new_order_if(wrong_instrument, |_| {
            called.set(true);
            true
        })
        .unwrap();
    assert!(!called.get());
    assert!(matches!(
        rejected,
        ConditionalNewOrderOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::WrongInstrument)
    ));

    let mut fok = new_order(4, 4, 14, Side::Buy, 10);
    fok.time_in_force = TimeInForce::FillOrKill;
    let rejected = book
        .submit_new_order_if(fok, |_| panic!("core FOK rejection bypasses the predicate"))
        .unwrap();
    assert!(matches!(
        rejected,
        ConditionalNewOrderOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::InsufficientLiquidity)
    ));

    let mut crossing_post_only = new_order(5, 5, 15, Side::Buy, 1);
    crossing_post_only.time_in_force = TimeInForce::PostOnly;
    let rejected = book
        .submit_new_order_if(crossing_post_only, |_| {
            panic!("crossing post-only rejection bypasses the predicate")
        })
        .unwrap();
    assert!(matches!(
        rejected,
        ConditionalNewOrderOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::PostOnlyWouldCross)
    ));
    book.validate().unwrap();
}

#[test]
fn market_to_limit_curve_uses_the_same_frozen_private_best_as_commit() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Sell, 5, 99)).unwrap();
    book.submit(limit(2, 2, 12, Side::Sell, 5, 100)).unwrap();
    let mut incoming = new_order(3, 3, 13, Side::Buy, 7);
    incoming.order_type = OrderType::MarketToLimit;

    let accepted = book
        .try_submit_new_order_curve_if(incoming, |observation| {
            let NewOrderExecution::Active(curve) = observation else {
                panic!("market-to-limit is immediately active");
            };
            assert_eq!(
                curve.quote().request().constraint(),
                StopActivation::Limit(Price::from_raw(99))
            );
            assert_eq!(curve.quote().executed_quantity_lots(), 5);
            assert_eq!(curve.quote().unfilled_quantity_lots(), 2);
            assert_eq!(curve.levels().len(), 1);
            assert_eq!(curve.levels()[0].price(), Price::from_raw(99));
            true
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalNewOrderOutcome::Reported {
            observation: Some(NewOrderExecution::Active(_)),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    let residual = book.order(OrderId::new(3).unwrap()).unwrap();
    assert_eq!(
        (residual.price.raw(), residual.leaves_quantity.lots()),
        (99, 2)
    );
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(100));
    book.validate().unwrap();
}

#[test]
fn every_immediately_active_new_order_shape_reaches_one_shared_observation_path() {
    let cases = [
        (
            OrderType::Market,
            TimeInForce::ImmediateOrCancel,
            OrderDisplay::FullyDisplayed,
            2,
        ),
        (
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            OrderDisplay::FullyDisplayed,
            2,
        ),
        (
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilTimestamp {
                expires_at: TimestampNs::from_unix_nanos(1_000),
            },
            OrderDisplay::FullyDisplayed,
            2,
        ),
        (
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::ImmediateOrCancel,
            OrderDisplay::FullyDisplayed,
            2,
        ),
        (
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::ImmediateOrCancelWithMinimum {
                minimum_quantity: Quantity::new(1).unwrap(),
            },
            OrderDisplay::FullyDisplayed,
            2,
        ),
        (
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::FillOrKill,
            OrderDisplay::FullyDisplayed,
            2,
        ),
        (
            OrderType::Limit(Price::from_raw(99)),
            TimeInForce::PostOnly,
            OrderDisplay::FullyDisplayed,
            0,
        ),
        (
            OrderType::MarketToLimit,
            TimeInForce::GoodTilCancelled,
            OrderDisplay::FullyDisplayed,
            2,
        ),
        (
            OrderType::Limit(Price::from_raw(99)),
            TimeInForce::GoodTilCancelled,
            OrderDisplay::Reserve {
                peak: Quantity::new(1).unwrap(),
            },
            0,
        ),
        (
            OrderType::Limit(Price::from_raw(99)),
            TimeInForce::GoodTilCancelled,
            OrderDisplay::Hidden,
            0,
        ),
    ];

    for (order_type, time_in_force, display, expected_execution_lots) in cases {
        let mut book = OrderBook::new(definition());
        book.submit(limit(1, 1, 11, Side::Sell, 5, 100)).unwrap();
        let mut incoming = new_order(2, 2, 12, Side::Buy, 2);
        incoming.order_type = order_type;
        incoming.time_in_force = time_in_force;
        incoming.display = display;
        let predicate_calls = Cell::new(0_u32);

        let outcome = book
            .submit_new_order_if(incoming, |observation| {
                predicate_calls.set(predicate_calls.get() + 1);
                let NewOrderExecution::Active(quote) = observation else {
                    panic!("every non-stop case is immediately active");
                };
                assert_eq!(quote.executed_quantity_lots(), expected_execution_lots);
                true
            })
            .unwrap();
        assert_eq!(predicate_calls.get(), 1);
        assert!(matches!(
            outcome,
            ConditionalNewOrderOutcome::Reported {
                observation: Some(NewOrderExecution::Active(_)),
                report
            } if report.outcome == CommandOutcome::Accepted
        ));
        book.validate().unwrap();
    }
}

#[test]
fn minimum_quantity_observation_reports_liquidity_while_tif_cancels_subthreshold_execution() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(1, 1, 11, Side::Sell, 1, 100)).unwrap();
    let mut incoming = new_order(2, 2, 12, Side::Buy, 3);
    incoming.time_in_force = TimeInForce::ImmediateOrCancelWithMinimum {
        minimum_quantity: Quantity::new(2).unwrap(),
    };

    let outcome = book
        .submit_new_order_if(incoming, |observation| {
            let NewOrderExecution::Active(quote) = observation else {
                panic!("minimum-quantity IOC is immediately active");
            };
            assert_eq!(quote.executed_quantity_lots(), 1);
            assert_eq!(quote.unfilled_quantity_lots(), 2);
            true
        })
        .unwrap();
    let ConditionalNewOrderOutcome::Reported {
        observation: Some(NewOrderExecution::Active(_)),
        report,
    } = outcome
    else {
        panic!("accepted minimum-quantity IOC retains its current observation");
    };
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert!(
        !report
            .events
            .iter()
            .any(|event| matches!(event.kind, EventKind::Trade(_)))
    );
    assert!(report.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            reason: CancelReason::MinimumQuantityUnavailable,
            ..
        }
    )));
    assert_eq!(book.best_ask().unwrap().quantity, 1);
    assert!(book.order(OrderId::new(2).unwrap()).is_none());
    book.validate().unwrap();
}

#[test]
fn dormant_stop_predicate_receives_explicit_non_execution_state() {
    let mut unreferenced = OrderBook::new(definition());
    let mut unavailable = new_order(1, 1, 12, Side::Buy, 3);
    unavailable.order_type = OrderType::Stop {
        trigger_price: Price::from_raw(110),
        activation: StopActivation::Market,
    };
    let rejected = unreferenced
        .submit_new_order_if(unavailable, |_| {
            panic!("a reference-less stop rejection bypasses the predicate")
        })
        .unwrap();
    assert!(matches!(
        rejected,
        ConditionalNewOrderOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::StopReferenceUnavailable)
    ));

    let mut book = OrderBook::new(definition());
    book.submit(stop_reference(1, 100)).unwrap();
    let mut stop = new_order(2, 2, 12, Side::Buy, 3);
    stop.order_type = OrderType::Stop {
        trigger_price: Price::from_raw(110),
        activation: StopActivation::Market,
    };

    let declined = book
        .submit_new_order_if(stop, |observation| {
            assert_eq!(observation, &NewOrderExecution::DormantStop);
            false
        })
        .unwrap();
    assert_eq!(
        declined,
        ConditionalNewOrderOutcome::Declined(NewOrderExecution::DormantStop)
    );
    assert!(book.dormant_stop(OrderId::new(2).unwrap()).is_none());

    let accepted = book
        .try_submit_new_order_curve_if(stop, |observation| {
            assert_eq!(observation, &NewOrderExecution::DormantStop);
            true
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalNewOrderOutcome::Reported {
            observation: Some(NewOrderExecution::DormantStop),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert!(book.dormant_stop(OrderId::new(2).unwrap()).is_some());

    let mut already_triggered = new_order(3, 3, 13, Side::Buy, 1);
    already_triggered.order_type = OrderType::Stop {
        trigger_price: Price::from_raw(90),
        activation: StopActivation::Market,
    };
    let rejected = book
        .submit_new_order_if(already_triggered, |_| {
            panic!("an already-triggered stop rejection bypasses the predicate")
        })
        .unwrap();
    assert!(matches!(
        rejected,
        ConditionalNewOrderOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::StopAlreadyTriggered)
    ));

    let mut stop_limit = new_order(4, 4, 14, Side::Buy, 2);
    stop_limit.order_type = OrderType::Stop {
        trigger_price: Price::from_raw(110),
        activation: StopActivation::Limit(Price::from_raw(105)),
    };
    let accepted = book
        .try_submit_new_order_curve_if(stop_limit, |observation| {
            assert_eq!(observation, &NewOrderExecution::DormantStop);
            true
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalNewOrderOutcome::Reported {
            observation: Some(NewOrderExecution::DormantStop),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert!(book.dormant_stop(OrderId::new(4).unwrap()).is_some());
    book.validate().unwrap();
}

#[test]
fn coupled_risk_rejection_bypasses_observation_and_acceptance_tracks_residual() {
    let mut managed = RiskManagedOrderBook::new(definition());
    managed.register_account(account(11), profile(10)).unwrap();
    managed.register_account(account(12), profile(2)).unwrap();
    managed.register_account(account(13), profile(10)).unwrap();
    managed.submit(limit(1, 1, 11, Side::Sell, 5, 100)).unwrap();

    let rejected_order = new_order(2, 2, 12, Side::Buy, 3);
    let rejected = managed
        .try_submit_new_order_curve_if(rejected_order, |_| {
            panic!("risk rejection bypasses observation")
        })
        .unwrap();
    assert!(matches!(
        rejected,
        ConditionalNewOrderOutcome::Reported {
            observation: None,
            report
        } if report.outcome == CommandOutcome::Rejected(RejectReason::RiskOrderQuantityLimit)
    ));

    let accepted_order = new_order(3, 3, 13, Side::Buy, 7);
    let accepted = managed
        .try_submit_new_order_curve_if(accepted_order, |observation| {
            matches!(
                observation,
                NewOrderExecution::Active(curve)
                    if curve.quote().executed_quantity_lots() == 5
                        && curve.quote().unfilled_quantity_lots() == 2
            )
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalNewOrderOutcome::Reported {
            observation: Some(NewOrderExecution::Active(_)),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    let risk = managed.risk();
    assert_eq!(risk.snapshot(account(13)).unwrap().position_lots(), 5);
    assert_eq!(risk.snapshot(account(13)).unwrap().open_buy_lots(), 2);
    assert_eq!(
        risk.reservation(OrderId::new(3).unwrap())
            .unwrap()
            .quantity_lots(),
        2
    );
    managed.validate().unwrap();
}

#[test]
fn durable_plain_decline_unwind_accept_replay_and_reopen_preserve_frame_contract() {
    let file = TestFile::new("plain");
    let mut durable = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    durable.submit(limit(1, 1, 11, Side::Sell, 5, 100)).unwrap();
    let incoming = new_order(2, 2, 12, Side::Buy, 7);
    let frames_before = frame_count(file.path());

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ = durable.submit_new_order_if(incoming, |_| panic!("policy failure"));
    }));
    assert!(panicked.is_err());
    assert_eq!(frame_count(file.path()), frames_before);

    assert!(matches!(
        durable.submit_new_order_if(incoming, |_| false).unwrap(),
        ConditionalNewOrderOutcome::Declined(NewOrderExecution::Active(_))
    ));
    assert_eq!(frame_count(file.path()), frames_before);

    let accepted = durable
        .try_submit_new_order_curve_if(incoming, |observation| {
            matches!(observation, NewOrderExecution::Active(_))
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalNewOrderOutcome::Reported {
            observation: Some(NewOrderExecution::Active(_)),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .try_submit_new_order_curve_if(incoming, |_| panic!("replay bypasses observation"))
            .unwrap(),
        ConditionalNewOrderOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));
    assert_eq!(frame_count(file.path()), frames_after);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    let residual = reopened.book().order(OrderId::new(2).unwrap()).unwrap();
    assert_eq!(
        (residual.price.raw(), residual.leaves_quantity.lots()),
        (100, 2)
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
    durable.submit(limit(1, 1, 11, Side::Sell, 5, 100)).unwrap();
    let incoming = new_order(2, 2, 12, Side::Buy, 7);
    let frames_before = frame_count(file.path());

    assert!(matches!(
        durable
            .try_submit_new_order_curve_if(incoming, |_| false)
            .unwrap(),
        ConditionalNewOrderOutcome::Declined(NewOrderExecution::Active(_))
    ));
    assert_eq!(frame_count(file.path()), frames_before);
    assert_eq!(
        durable
            .managed()
            .risk()
            .snapshot(account(12))
            .unwrap()
            .position_lots(),
        0
    );

    let accepted = durable
        .try_submit_new_order_curve_if(incoming, |observation| {
            matches!(observation, NewOrderExecution::Active(_))
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalNewOrderOutcome::Reported {
            observation: Some(NewOrderExecution::Active(_)),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(frame_count(file.path()), frames_before + 2);
    assert_eq!(
        durable
            .managed()
            .risk()
            .snapshot(account(12))
            .unwrap()
            .position_lots(),
        5
    );
    assert_eq!(
        durable
            .managed()
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .unwrap()
            .quantity_lots(),
        2
    );
    let frames_after = frame_count(file.path());
    assert!(matches!(
        durable
            .submit_new_order_if(incoming, |_| panic!("replay bypasses observation"))
            .unwrap(),
        ConditionalNewOrderOutcome::Reported {
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
            .snapshot(account(12))
            .unwrap()
            .position_lots(),
        5
    );
    assert_eq!(
        reopened
            .managed()
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .unwrap()
            .quantity_lots(),
        2
    );
    reopened.managed().validate().unwrap();
}
