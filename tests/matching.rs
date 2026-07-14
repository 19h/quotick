use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, TradingState,
};
use quotick::matching::{
    AccountAdmissionState, AccountControl, AccountControlAction, CancelOrder, CancelReason,
    Command, CommandOutcome, CommandPreparation, Event, EventKind, EventTrace, MatchingError,
    NewOrder, OrderBook, OrderType, RejectReason, ReplaceOrder, SelfTradePrevention, TimeInForce,
    Trade,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

fn account(value: u64) -> AccountId {
    AccountId::new(value).expect("non-zero account")
}

fn instrument() -> InstrumentId {
    InstrumentId::new(1).expect("non-zero instrument")
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(1).expect("non-zero version")
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("TEST").expect("symbol"),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).expect("base asset"),
        quote_asset_id: AssetId::new(2).expect("quote asset"),
        price: PriceRules::new(0, 1, Price::from_raw(i64::MIN), Price::from_raw(i64::MAX))
            .expect("price rules"),
        quantity: QuantityRules::new(1, 1, u64::MAX).expect("quantity rules"),
        reserve: quotick::instrument::ReserveOrderRules::disabled(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .expect("instrument definition")
}

#[allow(
    clippy::too_many_arguments,
    reason = "test command factory keeps cases legible"
)]
fn limit_order(
    command: u64,
    order: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    price: i64,
    time_in_force: TimeInForce,
    stp: SelfTradePrevention,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command).expect("non-zero command"),
        order_id: OrderId::new(order).expect("non-zero order"),
        account_id: account(owner),
        instrument_id: instrument(),
        instrument_version: version(),
        side,
        quantity: Quantity::new(quantity).expect("positive quantity"),
        display: quotick::matching::OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force,
        self_trade_prevention: stp,
        received_at: TimestampNs::from_unix_nanos(command),
    })
}

fn cancel(command: u64, order: u64, owner: u64) -> Command {
    Command::Cancel(CancelOrder {
        command_id: CommandId::new(command).expect("non-zero command"),
        order_id: OrderId::new(order).expect("non-zero order"),
        account_id: account(owner),
        instrument_id: instrument(),
        instrument_version: version(),
        received_at: TimestampNs::from_unix_nanos(command),
    })
}

fn replace(command: u64, order: u64, owner: u64, quantity: u64, price: i64) -> Command {
    Command::Replace(ReplaceOrder {
        command_id: CommandId::new(command).expect("non-zero command"),
        order_id: OrderId::new(order).expect("non-zero order"),
        account_id: account(owner),
        instrument_id: instrument(),
        instrument_version: version(),
        new_quantity: Quantity::new(quantity).expect("positive quantity"),
        new_price: Price::from_raw(price),
        new_display: quotick::matching::OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(command),
    })
}

fn account_control(
    command: u64,
    owner: u64,
    expected_revision: u64,
    action: AccountControlAction,
) -> Command {
    Command::AccountControl(AccountControl {
        command_id: CommandId::new(command).expect("non-zero command"),
        account_id: account(owner),
        instrument_id: instrument(),
        instrument_version: version(),
        expected_revision,
        action,
        received_at: TimestampNs::from_unix_nanos(command),
    })
}

fn trades(report: &quotick::matching::ExecutionReport) -> Vec<Trade> {
    report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::Trade(trade) => Some(trade),
            _ => None,
        })
        .collect()
}

#[test]
fn event_trace_is_send_and_sync() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<EventTrace>();
}

#[test]
fn event_trace_from_vec_retains_the_event_buffer() {
    let command_id = CommandId::new(1).expect("non-zero command");
    let mut events = Vec::with_capacity(8);
    events.push(Event {
        sequence: 1,
        command_id,
        occurred_at: TimestampNs::from_unix_nanos(1),
        kind: EventKind::CommandRejected(RejectReason::UnknownOrder),
    });
    let original_buffer = events.as_ptr();
    let trace = EventTrace::from(events);
    assert_eq!(trace.as_ptr(), original_buffer);
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one state-machine fixture proves atomic block, retry, stale revision, enable, and renewed admission"
)]
fn account_block_is_revision_checked_atomic_and_exactly_replayable() {
    let mut book = OrderBook::new(definition());
    for (command, order, side, price, quantity) in
        [(1, 1, Side::Buy, 100, 3), (2, 2, Side::Sell, 110, 5)]
    {
        assert_eq!(
            book.submit(limit_order(
                command,
                order,
                11,
                side,
                quantity,
                price,
                TimeInForce::GoodTilCancelled,
                SelfTradePrevention::CancelAggressor,
            ))
            .unwrap()
            .outcome,
            CommandOutcome::Accepted
        );
    }

    let block = account_control(3, 11, 0, AccountControlAction::BlockAndCancel);
    let report = book.submit(block).unwrap();
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(book.active_order_count(), 0);
    assert_eq!(
        book.account_control(account(11)).state(),
        AccountAdmissionState::Blocked
    );
    assert_eq!(book.account_control(account(11)).revision(), 1);
    assert!(report.events[..2].iter().all(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            reason: CancelReason::AccountControl,
            ..
        }
    )));
    assert!(matches!(
        report.events[2].kind,
        EventKind::AccountControlApplied {
            account_id,
            previous_state: AccountAdmissionState::Enabled,
            current_state: AccountAdmissionState::Blocked,
            revision: 1,
            cancelled_order_count: 2,
            cancelled_quantity_lots: 8,
        } if account_id == account(11)
    ));

    let blocked = book
        .submit(limit_order(
            4,
            3,
            11,
            Side::Buy,
            1,
            100,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
    assert_eq!(
        blocked.outcome,
        CommandOutcome::Rejected(RejectReason::AccountAdmissionBlocked)
    );

    let event_sequence = book.last_event_sequence();
    let replay = book.submit(block).unwrap();
    assert!(replay.replayed);
    assert_eq!(book.last_event_sequence(), event_sequence);
    assert_eq!(book.account_control(account(11)).revision(), 1);

    let stale = book
        .submit(account_control(5, 11, 0, AccountControlAction::Enable))
        .unwrap();
    assert_eq!(
        stale.outcome,
        CommandOutcome::Rejected(RejectReason::AccountControlRevisionMismatch)
    );
    assert_eq!(book.account_control(account(11)).revision(), 1);

    assert_eq!(
        book.submit(account_control(6, 11, 1, AccountControlAction::Enable,))
            .unwrap()
            .outcome,
        CommandOutcome::Accepted
    );
    assert_eq!(
        book.account_control(account(11)).state(),
        AccountAdmissionState::Enabled
    );
    assert_eq!(book.account_control(account(11)).revision(), 2);
    assert_eq!(
        book.submit(limit_order(
            7,
            4,
            11,
            Side::Buy,
            1,
            100,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap()
        .outcome,
        CommandOutcome::Accepted
    );
    book.validate().unwrap();
}

#[test]
fn matches_price_then_fifo_and_aggregates_depth() {
    let mut book = OrderBook::new(definition());
    book.submit(limit_order(
        1,
        1,
        11,
        Side::Sell,
        5,
        102,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .expect("first ask accepted");
    book.submit(limit_order(
        2,
        2,
        12,
        Side::Sell,
        5,
        101,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .expect("better ask accepted");
    book.submit(limit_order(
        3,
        3,
        13,
        Side::Sell,
        5,
        101,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .expect("same-price ask accepted");

    let report = book
        .submit(limit_order(
            4,
            4,
            14,
            Side::Buy,
            7,
            102,
            TimeInForce::ImmediateOrCancel,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("marketable bid accepted");
    let executions = trades(&report);

    assert_eq!(executions.len(), 2);
    assert_eq!(executions[0].maker_order_id.get(), 2);
    assert_eq!(executions[0].price, Price::from_raw(101));
    assert_eq!(executions[0].quantity.lots(), 5);
    assert_eq!(executions[1].maker_order_id.get(), 3);
    assert_eq!(executions[1].quantity.lots(), 2);
    assert_eq!(
        book.depth(Side::Sell, 10)
            .iter()
            .map(|level| (level.price.raw(), level.quantity, level.order_count))
            .collect::<Vec<_>>(),
        vec![(101, 3, 1), (102, 5, 1)]
    );
}

#[test]
fn cancellation_unlinks_a_middle_fifo_order_without_corrupting_priority() {
    let mut book = OrderBook::new(definition());
    for (command, order, owner) in [(1, 1, 11), (2, 2, 12), (3, 3, 13)] {
        book.submit(limit_order(
            command,
            order,
            owner,
            Side::Sell,
            1,
            100,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("resting ask accepted");
    }
    book.submit(cancel(4, 2, 12))
        .expect("middle order cancelled");
    let report = book
        .submit(limit_order(
            5,
            4,
            14,
            Side::Buy,
            2,
            100,
            TimeInForce::ImmediateOrCancel,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("bid accepted");

    assert_eq!(
        trades(&report)
            .iter()
            .map(|trade| trade.maker_order_id.get())
            .collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert_eq!(book.active_order_count(), 0);
}

#[test]
fn fill_or_kill_rejection_is_atomic() {
    let mut book = OrderBook::new(definition());
    book.submit(limit_order(
        1,
        1,
        11,
        Side::Sell,
        3,
        100,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .expect("ask accepted");
    let rejected = book
        .submit(limit_order(
            2,
            2,
            12,
            Side::Buy,
            4,
            100,
            TimeInForce::FillOrKill,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("business rejection is a report");

    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::InsufficientLiquidity)
    );
    assert_eq!(book.best_ask().expect("ask remains").quantity, 3);
    assert!(book.order(OrderId::new(2).expect("order id")).is_none());
}

#[test]
fn post_only_cross_is_rejected_without_consuming_order_identifier() {
    let mut book = OrderBook::new(definition());
    book.submit(limit_order(
        1,
        1,
        11,
        Side::Sell,
        1,
        100,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .expect("ask accepted");
    let command = limit_order(
        2,
        2,
        12,
        Side::Buy,
        1,
        100,
        TimeInForce::PostOnly,
        SelfTradePrevention::CancelAggressor,
    );
    let rejected = book.submit(command).expect("business rejection");
    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::PostOnlyWouldCross)
    );
    assert_eq!(book.active_order_count(), 1);
}

#[test]
fn self_trade_cancel_resting_continues_to_external_liquidity() {
    let mut book = OrderBook::new(definition());
    book.submit(limit_order(
        1,
        1,
        11,
        Side::Sell,
        2,
        100,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .expect("own ask accepted");
    book.submit(limit_order(
        2,
        2,
        12,
        Side::Sell,
        5,
        100,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .expect("external ask accepted");
    let report = book
        .submit(limit_order(
            3,
            3,
            11,
            Side::Buy,
            5,
            100,
            TimeInForce::ImmediateOrCancel,
            SelfTradePrevention::CancelResting,
        ))
        .expect("incoming order accepted");

    assert!(report.events.iter().any(|event| matches!(
        event.kind,
        EventKind::SelfTradePrevented {
            resting_order_id,
            ..
        } if resting_order_id.get() == 1
    )));
    assert_eq!(trades(&report).len(), 1);
    assert_eq!(trades(&report)[0].seller_account_id, account(12));
    assert_eq!(book.active_order_count(), 0);
}

#[test]
fn quantity_reduction_retains_priority_but_increase_loses_it() {
    let mut retained = OrderBook::new(definition());
    for (command, order, owner) in [(1, 1, 11), (2, 2, 12)] {
        retained
            .submit(limit_order(
                command,
                order,
                owner,
                Side::Sell,
                5,
                100,
                TimeInForce::GoodTilCancelled,
                SelfTradePrevention::CancelAggressor,
            ))
            .expect("ask accepted");
    }
    let reduction = retained
        .submit(replace(3, 1, 11, 3, 100))
        .expect("reduction accepted");
    assert!(matches!(
        reduction.events[0].kind,
        EventKind::OrderReplaced {
            priority_retained: true,
            ..
        }
    ));
    let fill = retained
        .submit(limit_order(
            4,
            3,
            13,
            Side::Buy,
            3,
            100,
            TimeInForce::ImmediateOrCancel,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("bid accepted");
    assert_eq!(trades(&fill)[0].maker_order_id.get(), 1);

    let mut lost = OrderBook::new(definition());
    for (command, order, owner) in [(1, 1, 11), (2, 2, 12)] {
        lost.submit(limit_order(
            command,
            order,
            owner,
            Side::Sell,
            1,
            100,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("ask accepted");
    }
    lost.submit(replace(3, 1, 11, 2, 100))
        .expect("increase accepted");
    let fill = lost
        .submit(limit_order(
            4,
            3,
            13,
            Side::Buy,
            1,
            100,
            TimeInForce::ImmediateOrCancel,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("bid accepted");
    assert_eq!(trades(&fill)[0].maker_order_id.get(), 2);
}

#[test]
fn exact_command_retries_are_idempotent_and_collisions_are_detected() {
    let mut book = OrderBook::new(definition());
    let command = limit_order(
        1,
        1,
        11,
        Side::Buy,
        2,
        99,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let first = book.submit(command).expect("first submit");
    let replay = book.submit(command).expect("exact replay");
    assert!(!first.replayed);
    assert!(replay.replayed);
    assert_eq!(first.events, replay.events);
    assert!(first.events.shares_storage_with(&replay.events));
    assert_eq!(book.active_order_count(), 1);

    let cached_events = replay.events.to_vec();
    let mut locally_corrupted = first;
    locally_corrupted.events.make_mut()[0].kind =
        EventKind::CommandRejected(RejectReason::UnknownOrder);
    assert!(!locally_corrupted.events.shares_storage_with(&replay.events));
    let replay_after_copy_on_write = book.submit(command).expect("second exact replay");
    assert_eq!(replay_after_copy_on_write.events.as_slice(), cached_events);
    assert!(
        replay_after_copy_on_write
            .events
            .shares_storage_with(&replay.events)
    );

    let collision = limit_order(
        1,
        2,
        11,
        Side::Buy,
        2,
        99,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    assert_eq!(
        book.submit(collision),
        Err(MatchingError::CommandIdCollision(
            CommandId::new(1).expect("command id")
        ))
    );
    assert_eq!(book.active_order_count(), 1);
}

#[test]
fn prepared_commands_are_generation_bound_and_exact_races_replay() {
    let command = limit_order(
        1,
        1,
        11,
        Side::Buy,
        2,
        99,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let unrelated = limit_order(
        2,
        2,
        12,
        Side::Sell,
        1,
        101,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );

    let mut direct = OrderBook::new(definition());
    let CommandPreparation::Ready(prepared) = direct.prepare(command).unwrap() else {
        panic!("a unique command must produce a ready preparation")
    };
    let committed = direct.commit(prepared).unwrap();
    assert_eq!(committed.outcome, CommandOutcome::Accepted);
    assert!(!committed.replayed);
    assert!(direct.order(OrderId::new(1).unwrap()).is_some());
    direct.validate().unwrap();

    let mut stale = OrderBook::new(definition());
    let CommandPreparation::Ready(prepared) = stale.prepare(command).unwrap() else {
        panic!("a unique command must produce a ready preparation")
    };
    assert_eq!(prepared.command(), command);
    stale.submit(unrelated).unwrap();
    assert_eq!(stale.commit(prepared), Err(MatchingError::StalePreparation));
    assert!(stale.order(OrderId::new(1).unwrap()).is_none());
    assert!(stale.order(OrderId::new(2).unwrap()).is_some());

    let mut exact_race = OrderBook::new(definition());
    let CommandPreparation::Ready(prepared) = exact_race.prepare(command).unwrap() else {
        panic!("a unique command must produce a ready preparation")
    };
    let first = exact_race.submit(command).unwrap();
    let replay = exact_race.commit(prepared).unwrap();
    assert!(!first.replayed);
    assert!(replay.replayed);
    assert_eq!(first.events, replay.events);
    assert!(matches!(
        exact_race.prepare(command).unwrap(),
        CommandPreparation::Replay(report) if report.replayed
    ));
    exact_race.validate().unwrap();

    let source = OrderBook::new(definition());
    let CommandPreparation::Ready(foreign) = source.prepare(command).unwrap() else {
        panic!("a unique command must produce a ready preparation")
    };
    let mut target = OrderBook::new(definition());
    assert_eq!(
        target.commit(foreign),
        Err(MatchingError::ForeignPreparation)
    );
    assert_eq!(target.active_order_count(), 0);
}

#[test]
fn every_new_event_has_a_strictly_increasing_trace_sequence() {
    let mut book = OrderBook::new(definition());
    let first = book
        .submit(limit_order(
            1,
            1,
            11,
            Side::Sell,
            1,
            -5,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("negative-priced ask accepted");
    let second = book
        .submit(limit_order(
            2,
            2,
            12,
            Side::Buy,
            1,
            -5,
            TimeInForce::ImmediateOrCancel,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("negative-priced bid accepted");
    let sequences = first
        .events
        .iter()
        .chain(&second.events)
        .map(|event| event.sequence)
        .collect::<Vec<_>>();
    assert!(sequences.windows(2).all(|window| window[0] < window[1]));
    book.validate().expect("book invariants hold");
}

#[test]
fn mixed_command_stream_preserves_all_structural_invariants() {
    let mut book = OrderBook::new(definition());
    for index in 1_u64..=40 {
        let side = if index % 2 == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let price = if side == Side::Buy {
            90 - i64::try_from(index % 5).expect("small value")
        } else {
            110 + i64::try_from(index % 5).expect("small value")
        };
        book.submit(limit_order(
            index,
            index,
            index + 100,
            side,
            index % 7 + 1,
            price,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::DecrementAndCancel,
        ))
        .expect("resting order accepted");
        book.validate().expect("invariants after insertion");
    }

    for index in (3_u64..=39).step_by(3) {
        book.submit(cancel(100 + index, index, index + 100))
            .expect("cancellation processed");
        book.validate().expect("invariants after cancellation");
    }
    for index in (2_u64..=40).step_by(4) {
        if book.order(OrderId::new(index).expect("order id")).is_some() {
            book.submit(replace(200 + index, index, index + 100, 9, 89))
                .expect("replacement processed");
            book.validate().expect("invariants after replacement");
        }
    }

    book.submit(limit_order(
        500,
        500,
        999,
        Side::Buy,
        100,
        120,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelBoth,
    ))
    .expect("sweep processed");
    book.validate().expect("invariants after sweep");
}

#[test]
fn best_level_survives_creation_non_best_removal_reprice_fill_stp_and_restore() {
    let mut book = OrderBook::new(definition());
    for (command, order, owner, price) in [(1, 1, 11, 100), (2, 2, 12, 110), (3, 3, 13, 120)] {
        book.submit(limit_order(
            command,
            order,
            owner,
            Side::Sell,
            1,
            price,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("ask accepted");
    }
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(100));

    book.submit(cancel(4, 2, 12))
        .expect("non-best cancellation accepted");
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(100));
    book.validate().expect("cache valid after non-best removal");

    book.submit(replace(5, 1, 11, 1, 130))
        .expect("best ask repriced");
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(120));
    book.validate().expect("cache valid after best reprice");

    book.submit(limit_order(
        6,
        6,
        20,
        Side::Buy,
        1,
        120,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    ))
    .expect("best ask fully executed");
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(130));
    book.validate().expect("cache valid after full execution");

    book.submit(limit_order(
        7,
        7,
        11,
        Side::Buy,
        1,
        130,
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelResting,
    ))
    .expect("self-trade prevention removed the final ask");
    assert!(book.best_ask().is_none());
    book.validate().expect("empty ask cache is valid");

    for (command, order, owner, price) in [(8, 8, 18, 90), (9, 9, 19, 80), (10, 10, 20, 70)] {
        book.submit(limit_order(
            command,
            order,
            owner,
            Side::Buy,
            1,
            price,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .expect("bid accepted");
    }
    assert_eq!(book.best_bid().unwrap().price, Price::from_raw(90));
    book.submit(cancel(11, 8, 18))
        .expect("best bid cancellation accepted");
    assert_eq!(book.best_bid().unwrap().price, Price::from_raw(80));

    let checkpoint = book
        .checkpoint(1, 23)
        .expect("eleven commands terminate at the declared WAL boundary");
    let restored = OrderBook::from_checkpoint(checkpoint).expect("checkpoint restores");
    assert_eq!(restored.best_bid(), book.best_bid());
    assert_eq!(restored.best_ask(), book.best_ask());
    restored.validate().expect("restored extrema are valid");
}
