use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::BinaryCodec;
use quotick::durable::DurableOrderBook;
use quotick::durable_risk::DurableRiskOrderBook;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions};
use quotick::market_data::{MarketDataError, MarketDataPublisher, MarketDataReplica};
use quotick::matching::{
    CancelReason, Command, CommandOutcome, EventKind, MassCancel, MassCancelScope, NewOrder,
    OrderBook, OrderDisplay, OrderType, ReplaceOrder, SelfTradePrevention, TimeInForce,
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

fn definition(state: TradingState) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("MASS-CANCEL").unwrap(),
        kind: InstrumentKind::Future,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 10_000).unwrap(),
        reserve: ReserveOrderRules::new(32).unwrap(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: state,
    })
    .unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "test command factory keeps cancellation selection explicit"
)]
fn order(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    display: OrderDisplay,
    price: i64,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(owner).unwrap(),
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

fn mass_cancel(command_id: u64, owner: u64, scope: MassCancelScope) -> Command {
    Command::MassCancel(MassCancel {
        command_id: CommandId::new(command_id).unwrap(),
        account_id: AccountId::new(owner).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        scope,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn reserve(peak: u64) -> OrderDisplay {
    OrderDisplay::Reserve {
        peak: Quantity::new(peak).unwrap(),
    }
}

#[test]
fn side_scoped_mass_cancel_is_canonical_total_leaves_aware_and_idempotent() {
    let mut book = OrderBook::new(definition(TradingState::Open));
    for command in [
        order(1, 30, 11, Side::Sell, 5, OrderDisplay::FullyDisplayed, 110),
        order(2, 10, 11, Side::Buy, 10, OrderDisplay::FullyDisplayed, 90),
        order(3, 20, 12, Side::Buy, 7, OrderDisplay::FullyDisplayed, 90),
        order(4, 5, 11, Side::Buy, 30, reserve(10), 80),
    ] {
        book.submit(command).unwrap();
    }

    let command = mass_cancel(5, 11, MassCancelScope::Side(Side::Buy));
    let report = book.submit(command).unwrap();
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    let cancelled: Vec<_> = report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::OrderCancelled {
                order_id,
                quantity,
                reason: CancelReason::MassCancel,
            } => Some((order_id.get(), quantity.lots())),
            _ => None,
        })
        .collect();
    assert_eq!(cancelled, vec![(5, 30), (10, 10)]);
    assert!(matches!(
        report.events.last().unwrap().kind,
        EventKind::MassCancelCompleted {
            account_id,
            scope: MassCancelScope::Side(Side::Buy),
            cancelled_order_count: 2,
            cancelled_quantity_lots: 40,
        } if account_id == AccountId::new(11).unwrap()
    ));
    assert!(book.order(OrderId::new(5).unwrap()).is_none());
    assert!(book.order(OrderId::new(10).unwrap()).is_none());
    assert!(book.order(OrderId::new(20).unwrap()).is_some());
    assert!(book.order(OrderId::new(30).unwrap()).is_some());

    let retry = book.submit(command).unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.events, report.events);
    assert_eq!(book.active_order_count(), 2);
    book.validate().unwrap();
}

#[test]
fn sparse_account_selection_is_canonical_and_independent_of_total_book_cardinality() {
    let mut book = OrderBook::new(definition(TradingState::Open));
    for value in 1..=512 {
        book.submit(order(
            value,
            value,
            1_000 + value,
            Side::Buy,
            1,
            OrderDisplay::FullyDisplayed,
            80,
        ))
        .unwrap();
    }
    for command in [
        order(
            513,
            30_000,
            11,
            Side::Buy,
            3,
            OrderDisplay::FullyDisplayed,
            90,
        ),
        order(
            514,
            10_000,
            11,
            Side::Sell,
            5,
            OrderDisplay::FullyDisplayed,
            110,
        ),
        order(
            515,
            20_000,
            11,
            Side::Buy,
            7,
            OrderDisplay::FullyDisplayed,
            85,
        ),
    ] {
        book.submit(command).unwrap();
    }

    assert_eq!(
        book.account_active_order_ids(AccountId::new(11).unwrap(), MassCancelScope::All),
        vec![
            OrderId::new(10_000).unwrap(),
            OrderId::new(20_000).unwrap(),
            OrderId::new(30_000).unwrap(),
        ]
    );
    assert_eq!(
        book.account_active_order_ids(
            AccountId::new(11).unwrap(),
            MassCancelScope::Side(Side::Buy),
        ),
        vec![OrderId::new(20_000).unwrap(), OrderId::new(30_000).unwrap(),]
    );
    assert_eq!(
        book.account_active_order_count(AccountId::new(11).unwrap(), MassCancelScope::All),
        3
    );

    let report = book
        .submit(mass_cancel(516, 11, MassCancelScope::Side(Side::Buy)))
        .unwrap();
    let cancelled: Vec<_> = report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::OrderCancelled { order_id, .. } => Some(order_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        cancelled,
        vec![OrderId::new(20_000).unwrap(), OrderId::new(30_000).unwrap(),]
    );
    assert_eq!(book.active_order_count(), 513);
    assert_eq!(
        book.account_active_order_ids(AccountId::new(11).unwrap(), MassCancelScope::All),
        vec![OrderId::new(10_000).unwrap()]
    );
    book.validate().unwrap();
}

#[test]
fn account_index_tracks_reserve_refresh_priority_loss_and_full_execution() {
    let mut book = OrderBook::new(definition(TradingState::Open));
    book.submit(order(1, 100, 11, Side::Sell, 30, reserve(10), 110))
        .unwrap();
    book.submit(order(
        2,
        200,
        11,
        Side::Buy,
        5,
        OrderDisplay::FullyDisplayed,
        90,
    ))
    .unwrap();
    let immediate_buy = |command_id: u64, order_id: u64, quantity: u64| {
        Command::New(NewOrder {
            command_id: CommandId::new(command_id).unwrap(),
            order_id: OrderId::new(order_id).unwrap(),
            account_id: AccountId::new(12).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            side: Side::Buy,
            quantity: Quantity::new(quantity).unwrap(),
            display: OrderDisplay::FullyDisplayed,
            order_type: OrderType::Limit(Price::from_raw(110)),
            time_in_force: TimeInForce::ImmediateOrCancel,
            self_trade_prevention: SelfTradePrevention::CancelAggressor,
            received_at: TimestampNs::from_unix_nanos(command_id),
        })
    };

    book.submit(immediate_buy(3, 300, 10)).unwrap();
    assert_eq!(
        book.account_active_order_ids(AccountId::new(11).unwrap(), MassCancelScope::All),
        vec![OrderId::new(100).unwrap(), OrderId::new(200).unwrap()]
    );
    book.submit(Command::Replace(ReplaceOrder {
        command_id: CommandId::new(4).unwrap(),
        order_id: OrderId::new(200).unwrap(),
        account_id: AccountId::new(11).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        new_quantity: Quantity::new(7).unwrap(),
        new_price: Price::from_raw(95),
        new_display: OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(4),
    }))
    .unwrap();
    assert_eq!(
        book.account_active_order_count(AccountId::new(11).unwrap(), MassCancelScope::All),
        2
    );

    book.submit(immediate_buy(5, 500, 20)).unwrap();
    assert_eq!(
        book.account_active_order_ids(AccountId::new(11).unwrap(), MassCancelScope::All),
        vec![OrderId::new(200).unwrap()]
    );
    book.validate().unwrap();
}

#[test]
fn empty_mass_cancel_is_an_accepted_acknowledged_control_in_every_trading_state() {
    for (index, state) in [
        TradingState::Open,
        TradingState::CancelOnly,
        TradingState::Halted,
        TradingState::Closed,
    ]
    .into_iter()
    .enumerate()
    {
        let mut book = OrderBook::new(definition(state));
        let report = book
            .submit(mass_cancel(
                u64::try_from(index + 1).unwrap(),
                11,
                MassCancelScope::All,
            ))
            .unwrap();
        assert_eq!(report.outcome, CommandOutcome::Accepted);
        assert_eq!(report.events.len(), 1);
        assert!(matches!(
            report.events[0].kind,
            EventKind::MassCancelCompleted {
                cancelled_order_count: 0,
                cancelled_quantity_lots: 0,
                ..
            }
        ));
    }
}

fn limits() -> RiskLimits {
    RiskLimits::new(RiskLimitSpec {
        max_order_quantity_lots: 100,
        max_order_notional: 100_000,
        max_open_orders: 10,
        max_open_quantity_lots: 200,
        max_open_notional: 200_000,
        max_long_position_lots: 200,
        max_short_position_lots: 200,
    })
    .unwrap()
}

#[test]
fn mass_cancel_atomically_releases_total_risk_and_publishes_displayed_depth() {
    let mut managed = RiskManagedOrderBook::new(definition(TradingState::Open));
    for owner in [11, 12] {
        managed
            .register_account(
                AccountId::new(owner).unwrap(),
                RiskProfile::new(AccountRiskState::Active, 0, limits()).unwrap(),
            )
            .unwrap();
    }
    let mut publisher = MarketDataPublisher::from_book(managed.book()).unwrap();
    let mut replica = MarketDataReplica::new(
        InstrumentId::new(1).unwrap(),
        InstrumentVersion::new(1).unwrap(),
        TradingState::Open,
    );
    replica.apply_snapshot(&publisher.snapshot()).unwrap();

    for command in [
        order(1, 1, 11, Side::Sell, 30, reserve(10), 110),
        order(2, 2, 11, Side::Buy, 5, OrderDisplay::FullyDisplayed, 90),
        order(3, 3, 12, Side::Sell, 7, OrderDisplay::FullyDisplayed, 120),
    ] {
        let report = managed.submit(command).unwrap();
        let batch = publisher.publish(command, &report, managed.book()).unwrap();
        replica.apply_batch(&batch).unwrap();
    }
    assert_eq!(
        managed
            .risk()
            .snapshot(AccountId::new(11).unwrap())
            .unwrap()
            .open_sell_lots(),
        30
    );
    assert_eq!(managed.book().depth(Side::Sell, 10)[0].quantity, 10);

    let command = mass_cancel(4, 11, MassCancelScope::All);
    let report = managed.submit(command).unwrap();
    let batch = publisher.publish(command, &report, managed.book()).unwrap();
    replica.apply_batch(&batch).unwrap();
    let account = managed
        .risk()
        .snapshot(AccountId::new(11).unwrap())
        .unwrap();
    assert_eq!(account.open_orders(), 0);
    assert_eq!(account.open_buy_lots(), 0);
    assert_eq!(account.open_sell_lots(), 0);
    assert_eq!(account.open_notional(), 0);
    assert_eq!(managed.risk().reservation_count(), 1);
    assert_eq!(
        replica.depth(Side::Buy, 10),
        managed.book().depth(Side::Buy, 10)
    );
    assert_eq!(
        replica.depth(Side::Sell, 10),
        managed.book().depth(Side::Sell, 10)
    );
    publisher.validate_against(managed.book()).unwrap();
    managed.validate().unwrap();
}

#[test]
fn publisher_rejects_mass_cancel_summary_divergence() {
    let mut book = OrderBook::new(definition(TradingState::Open));
    book.submit(order(1, 1, 11, Side::Buy, 30, reserve(10), 90))
        .unwrap();
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let command = mass_cancel(2, 11, MassCancelScope::All);
    let mut report = book.submit(command).unwrap();
    let EventKind::MassCancelCompleted {
        cancelled_quantity_lots,
        ..
    } = &mut report.events.make_mut().last_mut().unwrap().kind
    else {
        panic!("mass cancel must end with its completion event");
    };
    *cancelled_quantity_lots += 1;
    assert!(matches!(
        publisher.publish(command, &report, &book),
        Err(MarketDataError::TraceMismatch(_))
    ));
    assert!(publisher.is_poisoned());
}

#[test]
fn publisher_rejects_noncanonical_mass_cancel_order_sequence() {
    let mut book = OrderBook::new(definition(TradingState::Open));
    for command in [
        order(1, 2, 11, Side::Buy, 5, OrderDisplay::FullyDisplayed, 90),
        order(2, 1, 11, Side::Sell, 7, OrderDisplay::FullyDisplayed, 110),
    ] {
        book.submit(command).unwrap();
    }
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let command = mass_cancel(3, 11, MassCancelScope::All);
    let mut report = book.submit(command).unwrap();
    let events = report.events.make_mut();
    let first = events[0].kind;
    events[0].kind = events[1].kind;
    events[1].kind = first;
    assert!(matches!(
        publisher.publish(command, &report, &book),
        Err(MarketDataError::TraceMismatch(_))
    ));
    assert!(publisher.is_poisoned());
}

struct TestWal(PathBuf);

impl TestWal {
    fn new() -> Self {
        let nonce = NEXT_WAL.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-mass-cancel-{}-{nonce}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestWal {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

#[test]
fn mass_cancel_command_report_and_final_state_survive_wal_recovery() {
    let file = TestWal::new();
    let options = JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let command = mass_cancel(3, 11, MassCancelScope::All);
    assert_eq!(
        Command::decode(&command.encode().unwrap()).unwrap(),
        command
    );
    let mut durable =
        DurableOrderBook::open(file.path(), definition(TradingState::Open), options).unwrap();
    durable
        .submit(order(1, 2, 11, Side::Buy, 30, reserve(10), 90))
        .unwrap();
    durable
        .submit(order(
            2,
            1,
            12,
            Side::Sell,
            7,
            OrderDisplay::FullyDisplayed,
            110,
        ))
        .unwrap();
    let live = durable.submit(command).unwrap();
    let encoded = live.encode().unwrap();
    assert_eq!(
        quotick::matching::ExecutionReport::decode(&encoded).unwrap(),
        live
    );
    durable.sync_all().unwrap();
    drop(durable);

    let recovered =
        DurableOrderBook::open(file.path(), definition(TradingState::Open), options).unwrap();
    assert_eq!(recovered.recovery().replayed_commands, 3);
    assert!(recovered.book().order(OrderId::new(2).unwrap()).is_none());
    assert!(recovered.book().order(OrderId::new(1).unwrap()).is_some());
    recovered.book().validate().unwrap();
}

#[test]
fn durable_risk_replay_releases_exactly_the_mass_cancelled_reservations() {
    let file = TestWal::new();
    let options = JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let profiles = [11, 12].map(|owner| {
        AccountRiskDefinition::new(
            AccountId::new(owner).unwrap(),
            RiskProfile::new(AccountRiskState::Active, 0, limits()).unwrap(),
        )
    });
    let mut durable = DurableRiskOrderBook::open(
        file.path(),
        definition(TradingState::Open),
        &profiles,
        options,
    )
    .unwrap();
    durable
        .submit(order(1, 1, 11, Side::Buy, 30, reserve(10), 90))
        .unwrap();
    durable
        .submit(order(
            2,
            2,
            12,
            Side::Sell,
            7,
            OrderDisplay::FullyDisplayed,
            110,
        ))
        .unwrap();
    durable
        .submit(mass_cancel(3, 11, MassCancelScope::All))
        .unwrap();
    durable.sync_all().unwrap();
    drop(durable);

    let recovered = DurableRiskOrderBook::open(
        file.path(),
        definition(TradingState::Open),
        &profiles,
        options,
    )
    .unwrap();
    assert_eq!(recovered.recovery().replayed_commands, 3);
    assert!(
        recovered
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .is_none()
    );
    assert_eq!(
        recovered
            .managed()
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .unwrap()
            .quantity_lots(),
        7
    );
    recovered.managed().validate().unwrap();
}
