use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::BinaryCodec;
use quotick::durable::DurableOrderBook;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions};
use quotick::market_data::{MarketDataKind, MarketDataPublisher, MarketDataReplica};
use quotick::matching::{
    Command, CommandOutcome, EventKind, ExpirySweep, NewOrder, OrderBook, OrderDisplay, OrderType,
    RejectReason, ReplaceOrder, SelfTradePrevention, StopActivation, StopReference,
    StopReferenceCursor, StopTriggerSweep, TimeInForce,
};
use quotick::risk::{
    AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedOrderBook, RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    StopReferenceSequence, StopReferenceSourceId, StopReferenceSourceVersion, TimestampNs,
};

static NEXT_WAL: AtomicU64 = AtomicU64::new(1);

fn stop_reference(source_sequence: u64, price: i64) -> StopReference {
    StopReference::new(
        StopReferenceCursor::new(
            StopReferenceSourceId::new(1).unwrap(),
            StopReferenceSourceVersion::new(1).unwrap(),
            StopReferenceSequence::new(source_sequence).unwrap(),
        ),
        Price::from_raw(price),
    )
}

fn definition(hidden_orders_supported: bool) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("HIDDEN").unwrap(),
        kind: InstrumentKind::Equity,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::new(1_000).unwrap(),
        hidden_orders_supported,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "hidden-order fixtures expose every queueing dimension at the call site"
)]
fn order(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    price: i64,
    display: OrderDisplay,
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
        display,
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn replace(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    quantity: u64,
    price: i64,
    display: OrderDisplay,
) -> Command {
    Command::Replace(ReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        new_quantity: Quantity::new(quantity).unwrap(),
        new_price: Price::from_raw(price),
        new_display: display,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn with_stp(mut command: Command, policy: SelfTradePrevention) -> Command {
    let Command::New(order) = &mut command else {
        unreachable!("STP fixture is a new order");
    };
    order.self_trade_prevention = policy;
    command
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

fn risk_profile() -> RiskProfile {
    RiskProfile::new(
        AccountRiskState::Active,
        0,
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots: 1_000,
            max_order_notional: 1_000_000,
            max_open_orders: 16,
            max_open_quantity_lots: 1_000,
            max_open_notional: 1_000_000,
            max_long_position_lots: 1_000,
            max_short_position_lots: 1_000,
        })
        .unwrap(),
    )
    .unwrap()
}

struct WalPath(PathBuf);

impl WalPath {
    fn new() -> Self {
        let nonce = NEXT_WAL.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("quotick-hidden-{}-{nonce}.wal", std::process::id()));
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
fn hidden_admission_is_instrument_gated_and_resting_only() {
    let hidden = order(
        1,
        1,
        11,
        Side::Buy,
        10,
        100,
        OrderDisplay::Hidden,
        TimeInForce::GoodTilCancelled,
    );
    let mut disabled = OrderBook::new(definition(false));
    assert_eq!(
        disabled.submit(hidden).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::HiddenOrderNotSupported)
    );

    let mut enabled = OrderBook::new(definition(true));
    for (command_id, tif) in [
        (2, TimeInForce::ImmediateOrCancel),
        (3, TimeInForce::FillOrKill),
    ] {
        assert_eq!(
            enabled
                .submit(order(
                    command_id,
                    command_id,
                    11,
                    Side::Buy,
                    10,
                    100,
                    OrderDisplay::Hidden,
                    tif,
                ))
                .unwrap()
                .outcome,
            CommandOutcome::Rejected(RejectReason::HiddenOrderCannotBeImmediate)
        );
    }
    enabled.validate().unwrap();
}

#[test]
fn hidden_only_levels_are_private_but_executable_at_price_priority() {
    let mut book = OrderBook::new(definition(true));
    book.submit(order(
        1,
        1,
        11,
        Side::Sell,
        5,
        100,
        OrderDisplay::Hidden,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    book.submit(order(
        2,
        2,
        12,
        Side::Sell,
        5,
        110,
        OrderDisplay::FullyDisplayed,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();

    assert!(book.level(Side::Sell, Price::from_raw(100)).is_none());
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(110));
    let hidden = book.order(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(hidden.working_quantity.lots(), 5);
    assert_eq!(hidden.visible_quantity(), None);

    let execution = book
        .submit(order(
            3,
            3,
            13,
            Side::Buy,
            5,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ))
        .unwrap();
    assert_eq!(maker_trades(&execution), vec![(1, 5)]);
    assert_eq!(book.best_ask().unwrap().price, Price::from_raw(110));
    book.validate().unwrap();
}

#[test]
fn displayed_class_precedes_older_hidden_orders_and_hidden_fifo_is_stable() {
    let mut book = OrderBook::new(definition(true));
    for command in [
        order(
            1,
            1,
            11,
            Side::Sell,
            2,
            100,
            OrderDisplay::Hidden,
            TimeInForce::GoodTilCancelled,
        ),
        order(
            2,
            2,
            12,
            Side::Sell,
            2,
            100,
            OrderDisplay::Hidden,
            TimeInForce::GoodTilCancelled,
        ),
        order(
            3,
            3,
            13,
            Side::Sell,
            2,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::GoodTilCancelled,
        ),
    ] {
        book.submit(command).unwrap();
    }
    assert_eq!(book.best_ask().unwrap().quantity, 2);
    assert_eq!(book.best_ask().unwrap().order_count, 1);

    let execution = book
        .submit(order(
            4,
            4,
            14,
            Side::Buy,
            6,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ))
        .unwrap();
    assert_eq!(maker_trades(&execution), vec![(3, 2), (1, 2), (2, 2)]);
    assert!(book.best_ask().is_none());
    book.validate().unwrap();
}

#[test]
fn reserve_refresh_requeues_inside_displayed_class_ahead_of_hidden() {
    let mut book = OrderBook::new(definition(true));
    for command in [
        order(
            1,
            1,
            11,
            Side::Sell,
            4,
            100,
            OrderDisplay::Hidden,
            TimeInForce::GoodTilCancelled,
        ),
        order(
            2,
            2,
            12,
            Side::Sell,
            6,
            100,
            reserve(2),
            TimeInForce::GoodTilCancelled,
        ),
        order(
            3,
            3,
            13,
            Side::Sell,
            2,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::GoodTilCancelled,
        ),
    ] {
        book.submit(command).unwrap();
    }

    let execution = book
        .submit(order(
            4,
            4,
            14,
            Side::Buy,
            12,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ))
        .unwrap();
    assert_eq!(
        maker_trades(&execution),
        vec![(2, 2), (3, 2), (2, 2), (2, 2), (1, 4)]
    );
    book.validate().unwrap();
}

#[test]
fn hidden_liquidity_is_crossing_and_fok_eligible_despite_zero_depth() {
    let mut book = OrderBook::new(definition(true));
    book.submit(order(
        1,
        1,
        11,
        Side::Sell,
        5,
        100,
        OrderDisplay::Hidden,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();

    let post_only = book
        .submit(order(
            2,
            2,
            12,
            Side::Buy,
            1,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::PostOnly,
        ))
        .unwrap();
    assert_eq!(
        post_only.outcome,
        CommandOutcome::Rejected(RejectReason::PostOnlyWouldCross)
    );
    let fok = book
        .submit(order(
            3,
            3,
            13,
            Side::Buy,
            5,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::FillOrKill,
        ))
        .unwrap();
    assert_eq!(fok.outcome, CommandOutcome::Accepted);
    assert_eq!(maker_trades(&fok), vec![(1, 5)]);
    book.validate().unwrap();
}

#[test]
fn fok_reaches_all_displayed_reserve_leaves_before_a_hidden_self_barrier() {
    let mut book = OrderBook::new(definition(true));
    book.submit(order(
        1,
        1,
        11,
        Side::Sell,
        6,
        100,
        reserve(2),
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    book.submit(order(
        2,
        2,
        12,
        Side::Sell,
        1,
        100,
        OrderDisplay::Hidden,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();

    let rejected = book
        .submit(order(
            3,
            3,
            12,
            Side::Buy,
            7,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::FillOrKill,
        ))
        .unwrap();
    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::InsufficientLiquidity)
    );
    assert!(maker_trades(&rejected).is_empty());

    let accepted = book
        .submit(order(
            4,
            4,
            12,
            Side::Buy,
            6,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::FillOrKill,
        ))
        .unwrap();
    assert_eq!(accepted.outcome, CommandOutcome::Accepted);
    assert_eq!(maker_trades(&accepted), vec![(1, 2), (1, 2), (1, 2)]);
    assert!(book.best_ask().is_none());
    assert!(book.order(OrderId::new(2).unwrap()).is_some());
    book.validate().unwrap();
}

#[test]
fn every_stp_policy_applies_to_hidden_liquidity_without_publishing_depth() {
    for (policy, expected_maker_leaves) in [
        (SelfTradePrevention::CancelAggressor, Some(5)),
        (SelfTradePrevention::CancelResting, None),
        (SelfTradePrevention::CancelBoth, None),
        (SelfTradePrevention::DecrementAndCancel, Some(2)),
    ] {
        let mut book = OrderBook::new(definition(true));
        let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
        let mut replica = MarketDataReplica::new(
            InstrumentId::new(1).unwrap(),
            InstrumentVersion::new(1).unwrap(),
            TradingState::Open,
        );
        replica.apply_snapshot(&publisher.snapshot()).unwrap();

        let maker = order(
            1,
            1,
            11,
            Side::Sell,
            5,
            100,
            OrderDisplay::Hidden,
            TimeInForce::GoodTilCancelled,
        );
        let report = book.submit(maker).unwrap();
        let batch = publisher.publish(maker, &report, &book).unwrap();
        replica.apply_batch(&batch).unwrap();

        let taker = with_stp(
            order(
                2,
                2,
                11,
                Side::Buy,
                3,
                100,
                OrderDisplay::FullyDisplayed,
                TimeInForce::ImmediateOrCancel,
            ),
            policy,
        );
        let report = book.submit(taker).unwrap();
        assert!(report.events.iter().any(|event| matches!(
            event.kind,
            EventKind::SelfTradePrevented {
                quantity,
                policy: observed,
                ..
            } if quantity == Quantity::new(3).unwrap() && observed == policy
        )));
        assert!(maker_trades(&report).is_empty());
        assert_eq!(
            book.order(OrderId::new(1).unwrap())
                .map(|state| state.leaves_quantity.lots()),
            expected_maker_leaves
        );
        assert!(book.best_ask().is_none());

        let batch = publisher.publish(taker, &report, &book).unwrap();
        assert!(
            batch
                .updates()
                .iter()
                .all(|update| update.kind() == MarketDataKind::NoBookChange)
        );
        replica.apply_batch(&batch).unwrap();
        assert_eq!(replica.last_sequence(), book.last_event_sequence());
        assert!(replica.depth(Side::Sell, usize::MAX).is_empty());
        publisher.validate_against(&book).unwrap();
        book.validate().unwrap();
    }
}

#[test]
fn hidden_replacement_obeys_mode_fence_and_priority_rules() {
    let mut book = OrderBook::new(definition(true));
    for command in [
        order(
            1,
            1,
            11,
            Side::Sell,
            5,
            100,
            OrderDisplay::Hidden,
            TimeInForce::GoodTilCancelled,
        ),
        order(
            2,
            2,
            12,
            Side::Sell,
            5,
            100,
            OrderDisplay::Hidden,
            TimeInForce::GoodTilCancelled,
        ),
    ] {
        book.submit(command).unwrap();
    }
    assert_eq!(
        book.submit(replace(3, 1, 11, 4, 100, OrderDisplay::FullyDisplayed,))
            .unwrap()
            .outcome,
        CommandOutcome::Rejected(RejectReason::OrderDisplayModeChangeNotAllowed)
    );
    book.submit(replace(4, 1, 11, 4, 100, OrderDisplay::Hidden))
        .unwrap();
    let retained = book
        .submit(order(
            5,
            3,
            13,
            Side::Buy,
            4,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ))
        .unwrap();
    assert_eq!(maker_trades(&retained), vec![(1, 4)]);

    book.submit(replace(6, 2, 12, 6, 100, OrderDisplay::Hidden))
        .unwrap();
    book.submit(order(
        7,
        4,
        14,
        Side::Sell,
        1,
        100,
        OrderDisplay::Hidden,
        TimeInForce::GoodTilCancelled,
    ))
    .unwrap();
    let reprioritized = book
        .submit(order(
            8,
            5,
            15,
            Side::Buy,
            7,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ))
        .unwrap();
    assert_eq!(maker_trades(&reprioritized), vec![(2, 6), (4, 1)]);
    book.validate().unwrap();
}

#[test]
fn hidden_gtd_and_stop_limit_state_remains_private_through_controls() {
    let mut book = OrderBook::new(definition(true));
    book.submit(Command::StopTriggerSweep(StopTriggerSweep {
        command_id: CommandId::new(1).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        reference: stop_reference(1, 100),
        maximum_orders: 1,
        received_at: TimestampNs::from_unix_nanos(1),
    }))
    .unwrap();

    book.submit(Command::New(NewOrder {
        command_id: CommandId::new(2).unwrap(),
        order_id: OrderId::new(1).unwrap(),
        account_id: AccountId::new(11).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side: Side::Buy,
        quantity: Quantity::new(5).unwrap(),
        display: OrderDisplay::Hidden,
        order_type: OrderType::Stop {
            trigger_price: Price::from_raw(110),
            activation: StopActivation::Limit(Price::from_raw(111)),
        },
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(2),
    }))
    .unwrap();
    assert!(book.dormant_stop(OrderId::new(1).unwrap()).is_some());

    let activation = book
        .submit(Command::StopTriggerSweep(StopTriggerSweep {
            command_id: CommandId::new(3).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            reference: stop_reference(2, 110),
            maximum_orders: 1,
            received_at: TimestampNs::from_unix_nanos(3),
        }))
        .unwrap();
    assert!(activation.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderRested {
            order_id,
            working_quantity,
            ..
        } if order_id == OrderId::new(1).unwrap() && working_quantity == Quantity::new(5).unwrap()
    )));
    assert!(book.best_bid().is_none());
    assert_eq!(
        book.order(OrderId::new(1).unwrap())
            .unwrap()
            .visible_quantity(),
        None
    );

    book.submit(order(
        4,
        2,
        12,
        Side::Sell,
        3,
        120,
        OrderDisplay::Hidden,
        TimeInForce::GoodTilTimestamp {
            expires_at: TimestampNs::from_unix_nanos(10),
        },
    ))
    .unwrap();
    let expiry = book
        .submit(Command::ExpirySweep(ExpirySweep {
            command_id: CommandId::new(5).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            through: TimestampNs::from_unix_nanos(10),
            received_at: TimestampNs::from_unix_nanos(10),
        }))
        .unwrap();
    assert!(expiry.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled { order_id, quantity, .. }
            if order_id == OrderId::new(2).unwrap() && quantity == Quantity::new(3).unwrap()
    )));
    assert!(book.order(OrderId::new(2).unwrap()).is_none());
    book.validate().unwrap();
}

#[test]
fn market_data_tracks_hidden_state_without_publishing_depth() {
    let mut book = OrderBook::new(definition(true));
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::new(
        InstrumentId::new(1).unwrap(),
        InstrumentVersion::new(1).unwrap(),
        TradingState::Open,
    );
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    let hidden = order(
        1,
        1,
        11,
        Side::Sell,
        5,
        100,
        OrderDisplay::Hidden,
        TimeInForce::GoodTilCancelled,
    );
    let report = book.submit(hidden).unwrap();
    let batch = publisher.publish(hidden, &report, &book).unwrap();
    assert!(
        batch
            .updates()
            .iter()
            .all(|update| update.kind() == MarketDataKind::NoBookChange)
    );
    replica.apply_batch(&batch).unwrap();
    assert!(replica.depth(Side::Sell, usize::MAX).is_empty());

    let taker = order(
        2,
        2,
        12,
        Side::Buy,
        5,
        100,
        OrderDisplay::FullyDisplayed,
        TimeInForce::ImmediateOrCancel,
    );
    let report = book.submit(taker).unwrap();
    let batch = publisher.publish(taker, &report, &book).unwrap();
    assert!(batch.updates().iter().any(|update| matches!(
        update.kind(),
        MarketDataKind::Trade { print, maker_level }
            if print.price == Price::from_raw(100)
                && print.quantity == Quantity::new(5).unwrap()
                && maker_level.quantity == 0
                && maker_level.order_count == 0
    )));
    replica.apply_batch(&batch).unwrap();
    assert_eq!(replica.last_sequence(), book.last_event_sequence());
    publisher.validate_against(&book).unwrap();
    assert!(replica.depth(Side::Sell, usize::MAX).is_empty());
    book.validate().unwrap();
}

#[test]
fn hidden_state_round_trips_through_checkpoint_codec_and_direct_restore() {
    let mut book = OrderBook::new(definition(true));
    let hidden = order(
        1,
        1,
        11,
        Side::Sell,
        5,
        100,
        OrderDisplay::Hidden,
        TimeInForce::GoodTilCancelled,
    );
    let displayed = order(
        2,
        2,
        12,
        Side::Sell,
        2,
        100,
        OrderDisplay::FullyDisplayed,
        TimeInForce::GoodTilCancelled,
    );
    for command in [hidden, displayed] {
        book.submit(command).unwrap();
    }

    let checkpoint = book.checkpoint(1, 5).unwrap();
    let encoded = checkpoint.encode().unwrap();
    let decoded = quotick::matching::OrderBookCheckpoint::decode(&encoded).unwrap();
    assert_eq!(decoded, checkpoint);
    let mut restored = OrderBook::from_checkpoint(&decoded).unwrap();
    assert_eq!(
        restored.depth(Side::Sell, usize::MAX),
        book.depth(Side::Sell, usize::MAX)
    );
    assert_eq!(
        restored.order(OrderId::new(1).unwrap()),
        book.order(OrderId::new(1).unwrap())
    );
    assert!(restored.submit(hidden).unwrap().replayed);

    let execution = restored
        .submit(order(
            3,
            3,
            13,
            Side::Buy,
            7,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ))
        .unwrap();
    assert_eq!(maker_trades(&execution), vec![(2, 2), (1, 5)]);
    restored.validate().unwrap();
}

#[test]
fn hidden_definition_commands_and_queue_state_survive_wal_recovery() {
    let definition = definition(true);
    assert_eq!(
        InstrumentDefinition::decode(&definition.encode().unwrap()).unwrap(),
        definition
    );
    let hidden = order(
        1,
        1,
        11,
        Side::Sell,
        5,
        100,
        OrderDisplay::Hidden,
        TimeInForce::GoodTilCancelled,
    );
    assert_eq!(Command::decode(&hidden.encode().unwrap()).unwrap(), hidden);
    let path = WalPath::new();
    let options = JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let mut durable = DurableOrderBook::open(&path.0, definition, options).unwrap();
    durable.submit(hidden).unwrap();
    durable.sync_all().unwrap();
    drop(durable);

    let mut recovered = DurableOrderBook::open(&path.0, definition, options).unwrap();
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert!(recovered.book().best_ask().is_none());
    assert_eq!(
        recovered
            .book()
            .order(OrderId::new(1).unwrap())
            .unwrap()
            .visible_quantity(),
        None
    );
    let execution = recovered
        .submit(order(
            2,
            2,
            12,
            Side::Buy,
            5,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ))
        .unwrap();
    assert_eq!(maker_trades(&execution), vec![(1, 5)]);
    recovered.book().validate().unwrap();
}

#[test]
fn risk_reserves_hidden_total_leaves_and_releases_on_execution() {
    let mut managed = RiskManagedOrderBook::new(definition(true));
    for owner in [11, 12] {
        managed
            .register_account(AccountId::new(owner).unwrap(), risk_profile())
            .unwrap();
    }
    managed
        .submit(order(
            1,
            1,
            11,
            Side::Sell,
            5,
            100,
            OrderDisplay::Hidden,
            TimeInForce::GoodTilCancelled,
        ))
        .unwrap();
    assert_eq!(managed.risk().reservation_count(), 1);
    assert_eq!(
        managed
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        5
    );
    assert!(managed.book().best_ask().is_none());

    managed
        .submit(order(
            2,
            2,
            12,
            Side::Buy,
            5,
            100,
            OrderDisplay::FullyDisplayed,
            TimeInForce::ImmediateOrCancel,
        ))
        .unwrap();
    assert_eq!(managed.risk().reservation_count(), 0);
    assert_eq!(
        managed
            .risk()
            .snapshot(AccountId::new(11).unwrap())
            .unwrap()
            .position_lots(),
        -5
    );
    assert_eq!(
        managed
            .risk()
            .snapshot(AccountId::new(12).unwrap())
            .unwrap()
            .position_lots(),
        5
    );
    managed.validate().unwrap();
}
