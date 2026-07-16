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
use quotick::market_data::{MarketDataKind, MarketDataPublisher};
use quotick::matching::{
    CancelReason, Command, CommandOutcome, EventKind, NewOrder, OrderBook, OrderBookCheckpoint,
    OrderDisplay, OrderType, RejectReason, ReplaceOrder, SelfTradePrevention, StopActivation,
    StopReference, StopReferenceCursor, StopTriggerSweep, TimeInForce,
};
use quotick::risk::{
    AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedCheckpoint, RiskManagedOrderBook,
    RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    StopReferenceSequence, StopReferenceSourceId, StopReferenceSourceVersion, TimestampNs,
};

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

fn stop_sweep(command_id: u64, source_sequence: u64, price: i64) -> Command {
    Command::StopTriggerSweep(StopTriggerSweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        reference: stop_reference(source_sequence, price),
        maximum_orders: 10,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

static NEXT_WAL: AtomicU64 = AtomicU64::new(1);

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("MIN-QTY").unwrap(),
        kind: InstrumentKind::Equity,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(5, 5, 10_000).unwrap(),
        reserve: ReserveOrderRules::new(1_000).unwrap(),
        hidden_orders_supported: true,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "minimum-quantity fixtures keep every execution constraint explicit"
)]
fn order(
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

fn limit(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    time_in_force: TimeInForce,
    stp: SelfTradePrevention,
) -> Command {
    order(
        command_id,
        order_id,
        account_id,
        side,
        quantity,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(100)),
        time_in_force,
        stp,
    )
}

fn minimum(quantity: u64) -> TimeInForce {
    TimeInForce::ImmediateOrCancelWithMinimum {
        minimum_quantity: Quantity::new(quantity).unwrap(),
    }
}

fn reserve(peak: u64) -> OrderDisplay {
    OrderDisplay::Reserve {
        peak: Quantity::new(peak).unwrap(),
    }
}

fn risk_profile() -> RiskProfile {
    RiskProfile::new(
        AccountRiskState::Active,
        0,
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots: 10_000,
            max_order_notional: 10_000_000,
            max_open_orders: 100,
            max_open_quantity_lots: 100_000,
            max_open_notional: 100_000_000,
            max_long_position_lots: 100_000,
            max_short_position_lots: 100_000,
        })
        .unwrap(),
    )
    .unwrap()
}

fn total_traded(report: &quotick::matching::ExecutionReport) -> u64 {
    report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::Trade(trade) => Some(trade.quantity.lots()),
            _ => None,
        })
        .sum()
}

#[test]
fn minimum_quantity_admission_is_grid_bounded() {
    let mut book = OrderBook::new(definition());
    for (command, reason) in [
        (
            limit(
                1,
                1,
                11,
                Side::Buy,
                10,
                minimum(15),
                SelfTradePrevention::CancelAggressor,
            ),
            RejectReason::InvalidMinimumQuantity,
        ),
        (
            limit(
                2,
                2,
                11,
                Side::Buy,
                10,
                minimum(7),
                SelfTradePrevention::CancelAggressor,
            ),
            RejectReason::InvalidMinimumQuantity,
        ),
    ] {
        assert_eq!(
            book.submit(command).unwrap().outcome,
            CommandOutcome::Rejected(reason)
        );
    }
    assert_eq!(book.active_order_count(), 0);
    book.validate().unwrap();
}

#[test]
fn decrement_and_cancel_minimum_counts_only_external_execution_after_self_prevention() {
    let mut book = OrderBook::new(definition());
    for command in [
        order(
            1,
            1,
            12,
            Side::Sell,
            15,
            reserve(5),
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
        order(
            2,
            2,
            11,
            Side::Sell,
            10,
            reserve(5),
            OrderType::Limit(Price::from_raw(100)),
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
        limit(
            3,
            3,
            13,
            Side::Sell,
            10,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ),
    ] {
        book.submit(command).unwrap();
    }
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();

    let command = limit(
        4,
        4,
        11,
        Side::Buy,
        25,
        minimum(15),
        SelfTradePrevention::DecrementAndCancel,
    );
    let report = book.submit(command).unwrap();
    let batch = publisher.publish(command, &report, &book).unwrap();

    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(total_traded(&report), 20);
    assert!(report.events.iter().any(|event| matches!(
        event.kind,
        EventKind::SelfTradePrevented {
            aggressor_order_id,
            resting_order_id,
            quantity,
            policy: SelfTradePrevention::DecrementAndCancel,
        } if aggressor_order_id == OrderId::new(4).unwrap()
            && resting_order_id == OrderId::new(2).unwrap()
            && quantity.lots() == 5
    )));
    assert_eq!(
        book.order(OrderId::new(1).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        5
    );
    assert_eq!(
        book.order(OrderId::new(2).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        5
    );
    assert!(book.order(OrderId::new(3).unwrap()).is_none());
    assert!(
        batch
            .updates()
            .iter()
            .any(|update| matches!(update.kind(), MarketDataKind::Trade { .. }))
    );
    publisher.validate_against(&book).unwrap();
    book.validate().unwrap();
}

#[test]
fn decrement_and_cancel_unmet_minimum_is_atomic_and_replay_exact() {
    let mut book = OrderBook::new(definition());
    for (command_id, order_id, account_id) in [(1, 1, 12), (2, 2, 11), (3, 3, 13)] {
        book.submit(limit(
            command_id,
            order_id,
            account_id,
            Side::Sell,
            5,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
    }
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let command = limit(
        4,
        4,
        11,
        Side::Buy,
        10,
        minimum(10),
        SelfTradePrevention::DecrementAndCancel,
    );

    let report = book.submit(command).unwrap();
    let batch = publisher.publish(command, &report, &book).unwrap();
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(report.events.len(), 2);
    assert!(matches!(
        report.events[1].kind,
        EventKind::OrderCancelled {
            order_id,
            quantity,
            reason: CancelReason::MinimumQuantityUnavailable,
        } if order_id == OrderId::new(4).unwrap() && quantity.lots() == 10
    ));
    assert_eq!(total_traded(&report), 0);
    assert!(
        report
            .events
            .iter()
            .all(|event| !matches!(event.kind, EventKind::SelfTradePrevented { .. }))
    );
    assert_eq!(book.active_order_count(), 3);
    for order_id in 1..=3 {
        assert_eq!(
            book.order(OrderId::new(order_id).unwrap())
                .unwrap()
                .leaves_quantity
                .lots(),
            5,
            "failed preflight must preserve maker {order_id}"
        );
    }
    assert!(
        batch
            .updates()
            .iter()
            .all(|update| update.kind() == MarketDataKind::NoBookChange)
    );
    publisher.validate_against(&book).unwrap();

    let replay = book.submit(command).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.events, report.events);
    book.validate().unwrap();
}

#[test]
fn reserve_refreshes_remain_ahead_of_hidden_self_liquidity() {
    let mut book = OrderBook::new(definition());
    book.submit(order(
        1,
        1,
        12,
        Side::Sell,
        10,
        reserve(5),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    book.submit(order(
        2,
        2,
        11,
        Side::Sell,
        5,
        OrderDisplay::Hidden,
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();

    let report = book
        .submit(limit(
            3,
            3,
            11,
            Side::Buy,
            10,
            minimum(10),
            SelfTradePrevention::DecrementAndCancel,
        ))
        .unwrap();

    assert_eq!(total_traded(&report), 10);
    assert!(
        report
            .events
            .iter()
            .all(|event| !matches!(event.kind, EventKind::SelfTradePrevented { .. }))
    );
    assert!(book.order(OrderId::new(1).unwrap()).is_none());
    assert_eq!(
        book.order(OrderId::new(2).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        5
    );
    book.validate().unwrap();
}

#[test]
fn decrement_and_cancel_minimum_checkpoint_preserves_coupled_risk_atomicity() {
    let mut managed = RiskManagedOrderBook::new(definition());
    for account_id in [11, 12, 13] {
        managed
            .register_account(AccountId::new(account_id).unwrap(), risk_profile())
            .unwrap();
    }
    for (command_id, order_id, account_id) in [(1, 1, 12), (2, 2, 11), (3, 3, 13)] {
        managed
            .submit(limit(
                command_id,
                order_id,
                account_id,
                Side::Sell,
                5,
                TimeInForce::GoodTilCancelled,
                SelfTradePrevention::CancelAggressor,
            ))
            .unwrap();
    }
    let unavailable = limit(
        4,
        4,
        11,
        Side::Buy,
        10,
        minimum(10),
        SelfTradePrevention::DecrementAndCancel,
    );
    let unavailable_report = managed.submit(unavailable).unwrap();
    assert_eq!(unavailable_report.outcome, CommandOutcome::Accepted);
    assert_eq!(total_traded(&unavailable_report), 0);
    for account_id in [11, 12, 13] {
        assert_eq!(
            managed
                .risk()
                .snapshot(AccountId::new(account_id).unwrap())
                .unwrap()
                .position_lots(),
            0
        );
    }
    assert_eq!(managed.risk().reservation_count(), 3);
    assert!(
        managed
            .risk()
            .reservation(OrderId::new(4).unwrap())
            .is_none()
    );

    let checkpoint = managed.checkpoint(1, 4, 12).unwrap();
    let decoded = RiskManagedCheckpoint::decode(&checkpoint.encode().unwrap()).unwrap();
    let mut restored = RiskManagedOrderBook::from_checkpoint(&decoded).unwrap();
    assert!(restored.submit(unavailable).unwrap().replayed);

    let available = limit(
        5,
        5,
        11,
        Side::Buy,
        15,
        minimum(10),
        SelfTradePrevention::DecrementAndCancel,
    );
    let available_report = restored.submit(available).unwrap();
    assert_eq!(available_report.outcome, CommandOutcome::Accepted);
    assert_eq!(total_traded(&available_report), 10);
    assert_eq!(
        restored
            .risk()
            .snapshot(AccountId::new(11).unwrap())
            .unwrap()
            .position_lots(),
        10
    );
    for account_id in [12, 13] {
        assert_eq!(
            restored
                .risk()
                .snapshot(AccountId::new(account_id).unwrap())
                .unwrap()
                .position_lots(),
            -5
        );
    }
    assert_eq!(restored.risk().reservation_count(), 0);
    restored.validate().unwrap();
}

#[test]
fn unmet_minimum_is_atomic_even_when_cancel_resting_would_remove_self() {
    let mut book = OrderBook::new(definition());
    book.submit(limit(
        1,
        1,
        11,
        Side::Sell,
        5,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    book.submit(limit(
        2,
        2,
        12,
        Side::Sell,
        5,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();

    let command = limit(
        3,
        3,
        11,
        Side::Buy,
        10,
        minimum(10),
        SelfTradePrevention::CancelResting,
    );
    let report = book.submit(command).unwrap();
    let batch = publisher.publish(command, &report, &book).unwrap();

    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(report.events.len(), 2);
    assert!(matches!(
        report.events[1].kind,
        EventKind::OrderCancelled {
            order_id,
            quantity,
            reason: CancelReason::MinimumQuantityUnavailable,
        } if order_id == OrderId::new(3).unwrap() && quantity.lots() == 10
    ));
    assert_eq!(total_traded(&report), 0);
    assert_eq!(book.active_order_count(), 2);
    assert_eq!(book.best_ask().unwrap().quantity, 10);
    assert!(book.order(OrderId::new(1).unwrap()).is_some());
    assert!(book.order(OrderId::new(2).unwrap()).is_some());
    assert!(
        batch
            .updates()
            .iter()
            .all(|update| update.kind() == MarketDataKind::NoBookChange)
    );
    publisher.validate_against(&book).unwrap();

    let replay = book.submit(command).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.events, report.events);
    book.validate().unwrap();
}

#[test]
fn satisfied_minimum_executes_all_available_quantity_then_cancels_remainder() {
    let mut book = OrderBook::new(definition());
    for (command_id, order_id, account_id, quantity) in [(1, 1, 11, 5), (2, 2, 12, 10)] {
        book.submit(limit(
            command_id,
            order_id,
            account_id,
            Side::Sell,
            quantity,
            TimeInForce::GoodTilCancelled,
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();
    }

    let report = book
        .submit(limit(
            3,
            3,
            13,
            Side::Buy,
            20,
            minimum(10),
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();

    assert_eq!(total_traded(&report), 15);
    assert!(matches!(
        report.events.last().unwrap().kind,
        EventKind::OrderCancelled {
            quantity,
            reason: CancelReason::UnfilledRemainder,
            ..
        } if quantity.lots() == 5
    ));
    assert_eq!(book.active_order_count(), 0);
    book.validate().unwrap();
}

#[test]
fn reserve_refresh_behind_a_self_barrier_does_not_satisfy_the_minimum() {
    let mut book = OrderBook::new(definition());
    book.submit(order(
        1,
        1,
        12,
        Side::Sell,
        30,
        reserve(5),
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    book.submit(limit(
        2,
        2,
        11,
        Side::Sell,
        5,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();

    let report = book
        .submit(limit(
            3,
            3,
            11,
            Side::Buy,
            20,
            minimum(10),
            SelfTradePrevention::CancelAggressor,
        ))
        .unwrap();

    assert_eq!(total_traded(&report), 0);
    assert!(matches!(
        report.events[1].kind,
        EventKind::OrderCancelled {
            reason: CancelReason::MinimumQuantityUnavailable,
            ..
        }
    ));
    assert_eq!(
        book.order(OrderId::new(1).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        30
    );
    assert_eq!(
        book.order(OrderId::new(2).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        5
    );
    book.validate().unwrap();
}

#[test]
fn dormant_decrement_and_cancel_minimum_is_atomic_after_checkpoint_restore() {
    let mut book = OrderBook::new(definition());
    book.submit(stop_sweep(1, 1, 90)).unwrap();
    let stop = order(
        2,
        2,
        11,
        Side::Buy,
        20,
        OrderDisplay::FullyDisplayed,
        OrderType::Stop {
            trigger_price: Price::from_raw(110),
            activation: StopActivation::Limit(Price::from_raw(100)),
        },
        minimum(10),
        SelfTradePrevention::DecrementAndCancel,
    );
    book.submit(stop).unwrap();
    book.submit(limit(
        3,
        3,
        11,
        Side::Sell,
        5,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();
    book.submit(limit(
        4,
        4,
        12,
        Side::Sell,
        5,
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    ))
    .unwrap();

    let replacement = Command::Replace(ReplaceOrder {
        command_id: CommandId::new(5).unwrap(),
        order_id: OrderId::new(2).unwrap(),
        account_id: AccountId::new(11).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        new_quantity: Quantity::new(5).unwrap(),
        new_price: Price::from_raw(100),
        new_display: OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(5),
    });
    assert_eq!(
        book.submit(replacement).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::InvalidMinimumQuantity)
    );

    let checkpoint = book.checkpoint(1, 11).unwrap();
    let decoded = OrderBookCheckpoint::decode(&checkpoint.encode().unwrap()).unwrap();
    let mut restored = OrderBook::from_checkpoint(&decoded).unwrap();
    assert_eq!(
        restored
            .dormant_stop(OrderId::new(2).unwrap())
            .unwrap()
            .time_in_force,
        minimum(10)
    );

    let report = restored.submit(stop_sweep(6, 2, 110)).unwrap();
    assert_eq!(total_traded(&report), 0);
    assert!(
        report
            .events
            .iter()
            .all(|event| !matches!(event.kind, EventKind::SelfTradePrevented { .. }))
    );
    assert!(report.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            order_id,
            reason: CancelReason::MinimumQuantityUnavailable,
            ..
        } if order_id == OrderId::new(2).unwrap()
    )));
    assert_eq!(
        restored
            .order(OrderId::new(3).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        5
    );
    assert_eq!(
        restored
            .order(OrderId::new(4).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        5
    );
    restored.validate().unwrap();
}

struct WalPath(PathBuf);

impl WalPath {
    fn new() -> Self {
        let nonce = NEXT_WAL.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-minimum-quantity-{}-{nonce}.wal",
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
fn minimum_quantity_command_and_report_recover_exactly_from_wal() {
    let wal = WalPath::new();
    let command = limit(
        1,
        1,
        11,
        Side::Buy,
        10,
        minimum(5),
        SelfTradePrevention::DecrementAndCancel,
    );
    let options = JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let mut durable = DurableOrderBook::open(&wal.0, definition(), options).unwrap();
    let original = durable.submit(command).unwrap();
    durable.close().unwrap();

    let mut recovered = DurableOrderBook::open(&wal.0, definition(), options).unwrap();
    assert_eq!(recovered.recovery().replayed_commands, 1);
    let replay = recovered.submit(command).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.outcome, original.outcome);
    assert_eq!(replay.events, original.events);
    recovered.close().unwrap();
}
