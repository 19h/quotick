use quotick::codec::BinaryCodec;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::{
    CancelOrder, CancelReason, Command, CommandOutcome, CommandPreparation, EventKind, ExpirySweep,
    MatchingCapacity, MatchingError, NewOrder, OrderBook, OrderBookLimits, OrderBookLimitsSpec,
    OrderDisplay, OrderType, RejectReason, ReplaceOrder, SelfTradePrevention, StopActivation,
    StopTriggerSweep, TimeInForce,
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
        symbol: InstrumentSymbol::new("STOP").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(-1_000), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn order(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    order_type: OrderType,
    time_in_force: TimeInForce,
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
        order_type,
        time_in_force,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn stop_market(trigger_price: i64) -> OrderType {
    OrderType::Stop {
        trigger_price: Price::from_raw(trigger_price),
        activation: StopActivation::Market,
    }
}

fn stop_limit(trigger_price: i64, limit_price: i64) -> OrderType {
    OrderType::Stop {
        trigger_price: Price::from_raw(trigger_price),
        activation: StopActivation::Limit(Price::from_raw(limit_price)),
    }
}

fn trigger(command_id: u64, reference_price: i64, maximum_orders: u32) -> Command {
    Command::StopTriggerSweep(StopTriggerSweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        reference_price: Price::from_raw(reference_price),
        maximum_orders,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn expiry(command_id: u64, through: u64) -> Command {
    Command::ExpirySweep(ExpirySweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        through: TimestampNs::from_unix_nanos(through),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn limits(active: usize, levels: usize) -> OrderBookLimits {
    OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: active,
        max_active_accounts: active,
        max_price_levels_per_side: levels,
        max_accepted_order_ids: 32,
        max_account_controls: active,
        max_retained_commands: 32,
        cancellation_reserve: active,
        max_report_events: 64,
        max_retained_events: 2_048,
        max_prepared_order_selections: 2,
    })
    .unwrap()
}

fn replace(command_id: u64, order_id: u64, quantity: u64, price: i64) -> Command {
    Command::Replace(ReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(11).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        new_quantity: Quantity::new(quantity).unwrap(),
        new_price: Price::from_raw(price),
        new_display: OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

#[test]
fn stop_requires_a_reference_and_arms_without_public_depth() {
    let mut book = OrderBook::new(definition());
    let stop = order(
        1,
        10,
        100,
        Side::Buy,
        4,
        stop_market(110),
        TimeInForce::GoodTilCancelled,
    );
    assert_eq!(
        book.submit(stop).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::StopReferenceUnavailable)
    );

    let reference = book.submit(trigger(2, 100, 1)).unwrap();
    assert!(matches!(
        reference.events.last().unwrap().kind,
        EventKind::StopTriggerSweepCompleted {
            previous_reference_price: None,
            current_reference_price,
            triggered_order_count: 0,
            remaining_eligible_order_count: 0,
        } if current_reference_price == Price::from_raw(100)
    ));

    let armed = book
        .submit(order(
            3,
            10,
            100,
            Side::Buy,
            4,
            stop_market(110),
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    assert!(matches!(
        armed.events.last().unwrap().kind,
        EventKind::StopOrderArmed {
            order_id,
            trigger_price,
            activation: StopActivation::Market,
            ..
        } if order_id == OrderId::new(10).unwrap() && trigger_price == Price::from_raw(110)
    ));
    assert!(book.order(OrderId::new(10).unwrap()).is_none());
    assert_eq!(book.active_order_count(), 1);
    assert_eq!(book.dormant_stop_count(), 1);
    assert_eq!(book.depth(Side::Buy, usize::MAX), Vec::new());
    let dormant = book.dormant_stop(OrderId::new(10).unwrap()).unwrap();
    assert_eq!(dormant.trigger_price, Price::from_raw(110));
    assert_eq!(dormant.activation, StopActivation::Market);
    book.validate().unwrap();
}

#[test]
fn bounded_buy_activation_is_canonical_and_fences_reference_movement() {
    let mut book = OrderBook::new(definition());
    book.submit(trigger(1, 100, 1)).unwrap();
    for (command_id, order_id, trigger_price) in [(2, 30, 110), (3, 20, 105), (4, 10, 105)] {
        book.submit(order(
            command_id,
            order_id,
            100 + order_id,
            Side::Buy,
            1,
            stop_limit(trigger_price, 90),
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    }

    let first = book.submit(trigger(5, 110, 2)).unwrap();
    let triggered: Vec<_> = first
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::StopOrderTriggered { order_id, .. } => Some(order_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        triggered,
        [OrderId::new(20).unwrap(), OrderId::new(10).unwrap()]
    );
    assert!(matches!(
        first.events.last().unwrap().kind,
        EventKind::StopTriggerSweepCompleted {
            triggered_order_count: 2,
            remaining_eligible_order_count: 1,
            ..
        }
    ));
    assert_eq!(
        book.submit(trigger(6, 111, 1)).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::StopTriggerBacklog)
    );

    let second = book.submit(trigger(7, 110, 1)).unwrap();
    assert!(second.events.iter().any(|event| matches!(
        event.kind,
        EventKind::StopOrderTriggered { order_id, .. }
            if order_id == OrderId::new(30).unwrap()
    )));
    assert_eq!(book.dormant_stop_count(), 0);
    assert_eq!(book.stop_reference_price(), Some(Price::from_raw(110)));
    book.validate().unwrap();
}

#[test]
fn triggered_market_and_limit_orders_use_normal_matching_semantics() {
    let mut book = OrderBook::new(definition());
    book.submit(trigger(1, 100, 1)).unwrap();
    book.submit(order(
        2,
        1,
        11,
        Side::Sell,
        3,
        OrderType::Limit(Price::from_raw(105)),
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    book.submit(order(
        3,
        2,
        12,
        Side::Buy,
        5,
        stop_market(110),
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();

    let market = book.submit(trigger(4, 110, 1)).unwrap();
    assert!(market.events.iter().any(|event| matches!(
        event.kind,
        EventKind::Trade(trade)
            if trade.taker_order_id == OrderId::new(2).unwrap()
                && trade.quantity.lots() == 3
                && trade.price == Price::from_raw(105)
    )));
    assert!(market.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            order_id,
            quantity,
            reason: CancelReason::UnfilledRemainder,
        } if order_id == OrderId::new(2).unwrap() && quantity.lots() == 2
    )));

    book.submit(order(
        5,
        3,
        13,
        Side::Buy,
        4,
        stop_limit(120, 101),
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    book.submit(trigger(6, 120, 1)).unwrap();
    let residual = book.order(OrderId::new(3).unwrap()).unwrap();
    assert_eq!(residual.price, Price::from_raw(101));
    assert_eq!(residual.leaves_quantity.lots(), 4);
    assert!(book.dormant_stop(OrderId::new(3).unwrap()).is_none());
    book.validate().unwrap();
}

#[test]
fn dormant_stops_participate_in_cancel_expiry_and_exact_retry() {
    let mut book = OrderBook::new(definition());
    book.submit(trigger(1, 100, 1)).unwrap();
    let armed = order(
        2,
        1,
        11,
        Side::Sell,
        5,
        stop_limit(90, 89),
        TimeInForce::GoodTilTimestamp {
            expires_at: TimestampNs::from_unix_nanos(10),
        },
    );
    let first = book.submit(armed).unwrap();
    let retry = book.submit(armed).unwrap();
    assert!(retry.replayed);
    assert_eq!(first.events, retry.events);

    let expired = book.submit(expiry(11, 10)).unwrap();
    assert!(expired.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            order_id,
            reason: CancelReason::Expired,
            ..
        } if order_id == OrderId::new(1).unwrap()
    )));
    assert_eq!(book.active_order_count(), 0);

    book.submit(order(
        12,
        2,
        12,
        Side::Buy,
        2,
        stop_market(110),
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    let cancelled = book
        .submit(Command::Cancel(CancelOrder {
            command_id: CommandId::new(13).unwrap(),
            order_id: OrderId::new(2).unwrap(),
            account_id: AccountId::new(12).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            received_at: TimestampNs::from_unix_nanos(13),
        }))
        .unwrap();
    assert!(matches!(
        cancelled.events.last().unwrap().kind,
        EventKind::OrderCancelled {
            reason: CancelReason::UserRequested,
            ..
        }
    ));
    assert_eq!(book.active_order_count(), 0);
    book.validate().unwrap();
}

#[test]
fn satisfied_and_invalid_trigger_commands_are_nonmutating_rejections() {
    let mut book = OrderBook::new(definition());
    assert_eq!(
        book.submit(trigger(1, 100, 0)).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::StopTriggerBatchEmpty)
    );
    book.submit(trigger(2, 100, 1)).unwrap();
    assert_eq!(
        book.submit(order(
            3,
            1,
            11,
            Side::Buy,
            1,
            stop_market(100),
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap()
        .outcome,
        CommandOutcome::Rejected(RejectReason::StopAlreadyTriggered)
    );
    assert_eq!(book.active_order_count(), 0);
    assert_eq!(book.stop_reference_price(), Some(Price::from_raw(100)));
    assert_eq!(
        book.submit(trigger(4, 100, u32::MAX)).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::StopTriggerBatchExceedsActiveOrderCapacity)
    );
    book.validate().unwrap();
}

#[test]
fn bounded_sell_activation_uses_descending_trigger_then_acceptance_priority() {
    let mut book = OrderBook::new(definition());
    book.submit(trigger(1, 100, 1)).unwrap();
    for (command_id, order_id, trigger_price) in [(2, 30, 90), (3, 20, 95), (4, 10, 95)] {
        book.submit(order(
            command_id,
            order_id,
            100 + order_id,
            Side::Sell,
            1,
            stop_limit(trigger_price, 120),
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    }

    let first = book.submit(trigger(5, 90, 2)).unwrap();
    let triggered: Vec<_> = first
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::StopOrderTriggered { order_id, .. } => Some(order_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        triggered,
        [OrderId::new(20).unwrap(), OrderId::new(10).unwrap()]
    );
    assert_eq!(book.dormant_stop_count(), 1);
    book.submit(trigger(6, 90, 1)).unwrap();
    assert_eq!(book.dormant_stop_count(), 0);
    assert_eq!(book.stop_reference_price(), Some(Price::from_raw(90)));
    book.validate().unwrap();
}

#[test]
fn triggered_fok_and_post_only_fail_atomically_with_typed_reasons() {
    let mut book = OrderBook::new(definition());
    book.submit(trigger(1, 100, 1)).unwrap();
    book.submit(order(
        2,
        1,
        21,
        Side::Sell,
        3,
        OrderType::Limit(Price::from_raw(105)),
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    book.submit(order(
        3,
        2,
        11,
        Side::Buy,
        4,
        stop_limit(110, 110),
        TimeInForce::FillOrKill,
    ))
    .unwrap();
    let fok = book.submit(trigger(4, 110, 1)).unwrap();
    assert!(
        !fok.events
            .iter()
            .any(|event| matches!(event.kind, EventKind::Trade(_)))
    );
    assert!(fok.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            reason: CancelReason::TriggeredFokUnfilled,
            quantity,
            ..
        } if quantity.lots() == 4
    )));
    assert_eq!(
        book.order(OrderId::new(1).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        3
    );

    book.submit(order(
        5,
        3,
        11,
        Side::Buy,
        2,
        stop_limit(120, 110),
        TimeInForce::PostOnly,
    ))
    .unwrap();
    let post_only = book.submit(trigger(6, 120, 1)).unwrap();
    assert!(post_only.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            reason: CancelReason::TriggeredPostOnlyWouldCross,
            ..
        }
    )));
    assert_eq!(
        book.submit(order(
            7,
            4,
            11,
            Side::Buy,
            1,
            stop_market(130),
            TimeInForce::PostOnly,
        ))
        .unwrap()
        .outcome,
        CommandOutcome::Rejected(RejectReason::StopMarketCannotPost)
    );
    book.validate().unwrap();
}

#[test]
fn dormant_stop_limit_replacement_preserves_or_reprioritizes_trigger_priority() {
    let mut book = OrderBook::new(definition());
    book.submit(trigger(1, 100, 1)).unwrap();
    book.submit(order(
        2,
        1,
        11,
        Side::Buy,
        5,
        stop_limit(110, 101),
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    let initial_priority = book
        .dormant_stop(OrderId::new(1).unwrap())
        .unwrap()
        .priority_sequence;

    let retained = book.submit(replace(3, 1, 3, 101)).unwrap();
    assert!(matches!(
        retained.events[0].kind,
        EventKind::OrderReplaced {
            priority_retained: true,
            ..
        }
    ));
    let dormant = book.dormant_stop(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(dormant.priority_sequence, initial_priority);
    assert_eq!(dormant.leaves_quantity.lots(), 3);

    let reprioritized = book.submit(replace(4, 1, 4, 102)).unwrap();
    assert!(matches!(
        reprioritized.events[0].kind,
        EventKind::OrderReplaced {
            priority_retained: false,
            ..
        }
    ));
    let replacement_sequence = reprioritized.events[0].sequence;
    let dormant = book.dormant_stop(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(dormant.priority_sequence, replacement_sequence);
    assert_eq!(
        dormant.activation,
        StopActivation::Limit(Price::from_raw(102))
    );

    book.submit(trigger(5, 110, 1)).unwrap();
    assert_eq!(
        book.order(OrderId::new(1).unwrap()).unwrap().price,
        Price::from_raw(102)
    );

    book.submit(order(
        6,
        2,
        11,
        Side::Buy,
        1,
        stop_market(120),
        TimeInForce::ImmediateOrCancel,
    ))
    .unwrap();
    assert_eq!(
        book.submit(replace(7, 2, 1, 103)).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::StopMarketCannotBeReplaced)
    );
    book.validate().unwrap();
}

#[test]
fn dormant_stops_consume_active_capacity_and_capacity_failed_activation_cancels() {
    let selected = limits(2, 1);
    let mut book = OrderBook::with_limits(definition(), selected);
    book.submit(trigger(1, 100, 1)).unwrap();
    book.submit(order(
        2,
        1,
        11,
        Side::Buy,
        1,
        OrderType::Limit(Price::from_raw(90)),
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    book.submit(order(
        3,
        2,
        12,
        Side::Buy,
        1,
        stop_limit(110, 80),
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    assert_eq!(book.active_order_count(), 2);
    assert_eq!(
        book.submit(order(
            4,
            3,
            13,
            Side::Sell,
            1,
            OrderType::Limit(Price::from_raw(120)),
            TimeInForce::GoodTilCancelled,
        )),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::ActiveOrders
        ))
    );

    let activated = book.submit(trigger(5, 110, 1)).unwrap();
    assert!(activated.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            order_id,
            reason: CancelReason::TriggeredCapacityUnavailable,
            ..
        } if order_id == OrderId::new(2).unwrap()
    )));
    assert_eq!(book.active_order_count(), 1);
    assert!(book.order(OrderId::new(2).unwrap()).is_none());
    book.validate().unwrap();
}

#[test]
fn nonempty_trigger_preparation_uses_and_releases_the_bounded_selection_pool() {
    let selected = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 2,
        max_active_accounts: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 8,
        max_account_controls: 2,
        max_retained_commands: 8,
        cancellation_reserve: 2,
        max_report_events: 8,
        max_retained_events: 32,
        max_prepared_order_selections: 1,
    })
    .unwrap();
    let mut book = OrderBook::with_limits(definition(), selected);
    book.submit(trigger(1, 100, 1)).unwrap();
    book.submit(order(
        2,
        1,
        11,
        Side::Buy,
        1,
        stop_limit(110, 90),
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();

    let held = match book.prepare(trigger(3, 110, 1)).unwrap() {
        CommandPreparation::Ready(prepared) => prepared,
        CommandPreparation::Replay(_) => panic!("new trigger command cannot be a replay"),
    };
    assert!(matches!(
        book.prepare(trigger(4, 110, 1)),
        Err(MatchingError::PreparationCapacityExhausted { maximum: 1 })
    ));
    assert_eq!(book.stop_reference_price(), Some(Price::from_raw(100)));
    assert_eq!(book.dormant_stop_count(), 1);

    drop(held);
    let ready = match book.prepare(trigger(4, 110, 1)).unwrap() {
        CommandPreparation::Ready(prepared) => prepared,
        CommandPreparation::Replay(_) => panic!("uncommitted trigger command cannot be a replay"),
    };
    book.commit(ready).unwrap();
    assert_eq!(book.dormant_stop_count(), 0);
    book.validate().unwrap();
}

#[test]
fn dormant_stop_checkpoint_codec_restores_reference_priority_and_exact_retries() {
    let mut book = OrderBook::new(definition());
    let reference = trigger(1, 100, 2);
    let buy = order(
        2,
        1,
        11,
        Side::Buy,
        3,
        stop_limit(110, 105),
        TimeInForce::GoodTilCancelled,
    );
    let sell = order(
        3,
        2,
        12,
        Side::Sell,
        4,
        stop_market(90),
        TimeInForce::ImmediateOrCancel,
    );
    for command in [reference, buy, sell] {
        book.submit(command).unwrap();
    }
    let checkpoint = book.checkpoint(1, 7).unwrap();
    assert_eq!(checkpoint.dormant_stops().len(), 2);
    let encoded = checkpoint.encode().unwrap();
    let decoded = quotick::matching::OrderBookCheckpoint::decode(&encoded).unwrap();
    assert_eq!(decoded, checkpoint);
    let definition_length =
        usize::try_from(u32::from_le_bytes(encoded[16..20].try_into().unwrap())).unwrap();
    let first_stop = 20 + definition_length + 4 + 4;
    let mut corrupted_priority = encoded.clone();
    corrupted_priority[first_stop + 54..first_stop + 62].copy_from_slice(&999_u64.to_le_bytes());
    assert!(matches!(
        quotick::matching::OrderBookCheckpoint::decode(&corrupted_priority),
        Err(quotick::codec::CodecError::InvalidMatchingCheckpoint(_))
    ));

    let mut restored = OrderBook::from_checkpoint(&decoded).unwrap();
    assert_eq!(restored.stop_reference_price(), Some(Price::from_raw(100)));
    assert_eq!(restored.dormant_stop_count(), 2);
    assert_eq!(
        restored.dormant_stop(OrderId::new(1).unwrap()),
        book.dormant_stop(OrderId::new(1).unwrap())
    );
    assert!(restored.submit(buy).unwrap().replayed);
    let triggered = restored.submit(trigger(4, 110, 2)).unwrap();
    assert!(triggered.events.iter().any(|event| matches!(
        event.kind,
        EventKind::StopOrderTriggered { order_id, .. }
            if order_id == OrderId::new(1).unwrap()
    )));
    restored.validate().unwrap();
}
