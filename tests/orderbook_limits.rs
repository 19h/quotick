use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions};
use quotick::matching::{
    CancelOrder, Command, CommandOutcome, MassCancel, MassCancelScope, MatchingCapacity,
    MatchingError, NewOrder, OrderBook, OrderBookLimits, OrderBookLimitsError, OrderBookLimitsSpec,
    OrderDisplay, OrderType, RejectReason, ReplaceOrder, SelfTradePrevention, TimeInForce,
};
use quotick::risk::RiskManagedOrderBook;
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_WAL: AtomicU64 = AtomicU64::new(1);

struct TestWal(PathBuf);

impl TestWal {
    fn new() -> Self {
        let nonce = NEXT_WAL.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-orderbook-limits-{}-{nonce}.wal",
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

fn journal_options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("LIMITS").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, u64::MAX).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn limits(
    active: usize,
    accounts: usize,
    levels: usize,
    accepted_ids: usize,
    history: usize,
    cancellation_reserve: usize,
) -> OrderBookLimits {
    OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: active,
        max_active_accounts: accounts,
        max_price_levels_per_side: levels,
        max_accepted_order_ids: accepted_ids,
        max_retained_commands: history,
        cancellation_reserve,
        preallocate: false,
    })
    .unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "explicit order fields make capacity-boundary cases auditable"
)]
fn new_order(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
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
        quantity: Quantity::new(1).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type,
        time_in_force,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn resting(command_id: u64, order_id: u64, account_id: u64, side: Side, price: i64) -> Command {
    new_order(
        command_id,
        order_id,
        account_id,
        side,
        OrderType::Limit(Price::from_raw(price)),
        TimeInForce::GoodTilCancelled,
    )
}

fn ioc(command_id: u64, order_id: u64, account_id: u64, side: Side, price: i64) -> Command {
    new_order(
        command_id,
        order_id,
        account_id,
        side,
        OrderType::Limit(Price::from_raw(price)),
        TimeInForce::ImmediateOrCancel,
    )
}

fn invalid_market(command_id: u64, order_id: u64) -> Command {
    new_order(
        command_id,
        order_id,
        99,
        Side::Buy,
        OrderType::Market,
        TimeInForce::GoodTilCancelled,
    )
}

fn cancel(command_id: u64, order_id: u64, account_id: u64) -> Command {
    Command::Cancel(CancelOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn mass_cancel(command_id: u64, account_id: u64) -> Command {
    Command::MassCancel(MassCancel {
        command_id: CommandId::new(command_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        scope: MassCancelScope::All,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn replace(command_id: u64, order_id: u64, account_id: u64, price: i64) -> Command {
    Command::Replace(ReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        new_quantity: Quantity::new(1).unwrap(),
        new_price: Price::from_raw(price),
        new_display: OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

#[test]
fn limit_spec_rejects_zero_incoherent_and_under_reserved_configurations() {
    let base = OrderBookLimitsSpec {
        max_active_orders: 2,
        max_active_accounts: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 8,
        max_retained_commands: 8,
        cancellation_reserve: 2,
        preallocate: false,
    };
    assert_eq!(
        OrderBookLimits::new(base)
            .unwrap()
            .ordinary_command_capacity(),
        6
    );
    assert_eq!(
        OrderBookLimits::new(OrderBookLimitsSpec {
            max_active_orders: 0,
            ..base
        }),
        Err(OrderBookLimitsError::ZeroActiveOrders)
    );
    assert_eq!(
        OrderBookLimits::new(OrderBookLimitsSpec {
            max_active_accounts: 3,
            ..base
        }),
        Err(OrderBookLimitsError::ActiveAccountsExceedActiveOrders)
    );
    assert_eq!(
        OrderBookLimits::new(OrderBookLimitsSpec {
            cancellation_reserve: 1,
            ..base
        }),
        Err(OrderBookLimitsError::CancellationReserveBelowActiveOrders)
    );
    assert_eq!(
        OrderBookLimits::new(OrderBookLimitsSpec {
            cancellation_reserve: 8,
            ..base
        }),
        Err(OrderBookLimitsError::NoOrdinaryCommandCapacity)
    );
    let preallocated = OrderBookLimits::new(OrderBookLimitsSpec {
        preallocate: true,
        ..base
    })
    .unwrap();
    assert!(preallocated.preallocates());
    assert_eq!(
        OrderBook::with_limits(definition(), preallocated).limits(),
        preallocated
    );
    let impossible_reservation = OrderBookLimits::new(OrderBookLimitsSpec {
        max_accepted_order_ids: usize::MAX,
        preallocate: true,
        ..base
    })
    .unwrap();
    assert_eq!(
        OrderBook::try_with_limits(definition(), impossible_reservation),
        Err(MatchingError::CapacityReservationFailed(
            MatchingCapacity::AcceptedOrderIds
        ))
    );
}

#[test]
fn ordinary_lane_exhaustion_preserves_cancel_mass_cancel_and_exact_retries() {
    let mut book = OrderBook::with_limits(definition(), limits(2, 2, 2, 10, 5, 2));
    book.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    for (command_id, order_id) in [(2, 2), (3, 3)] {
        assert_eq!(
            book.submit(invalid_market(command_id, order_id))
                .unwrap()
                .outcome,
            CommandOutcome::Rejected(RejectReason::MarketOrderCannotRest)
        );
    }

    assert_eq!(
        book.submit(resting(4, 4, 11, Side::Sell, 100)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AdmissionCommandHistory
        ))
    );
    assert_eq!(
        book.submit(cancel(4, 999, 11)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AdmissionCommandHistory
        ))
    );

    let cancel_command = cancel(4, 1, 11);
    let cancelled = book.submit(cancel_command).unwrap();
    assert_eq!(
        book.submit(cancel_command).unwrap().events,
        cancelled.events
    );
    assert!(book.submit(cancel_command).unwrap().replayed);

    let mass_command = mass_cancel(5, 11);
    let mass_report = book.submit(mass_command).unwrap();
    assert!(book.submit(mass_command).unwrap().replayed);
    assert_eq!(
        book.submit(mass_command).unwrap().events,
        mass_report.events
    );
    assert_eq!(
        book.submit(cancel(6, 1, 11)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::CommandHistory
        ))
    );
    book.validate().unwrap();
}

#[test]
fn active_and_account_limits_gate_only_commands_that_can_rest() {
    let mut active = OrderBook::with_limits(definition(), limits(1, 1, 1, 10, 12, 1));
    active.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    assert_eq!(
        active.submit(resting(2, 2, 11, Side::Sell, 100)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::ActiveOrders
        ))
    );
    assert_eq!(
        active.submit(ioc(2, 2, 12, Side::Buy, 90)).unwrap().outcome,
        CommandOutcome::Accepted
    );
    active.validate().unwrap();

    let mut accounts = OrderBook::with_limits(definition(), limits(2, 1, 2, 10, 12, 2));
    accounts.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    assert_eq!(
        accounts.submit(resting(2, 2, 12, Side::Sell, 100)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::ActiveAccounts
        ))
    );
    assert_eq!(
        accounts
            .submit(ioc(2, 2, 12, Side::Buy, 90))
            .unwrap()
            .outcome,
        CommandOutcome::Accepted
    );
    accounts.validate().unwrap();
}

#[test]
fn side_level_limit_accounts_for_a_level_released_by_replacement() {
    let mut book = OrderBook::with_limits(definition(), limits(3, 1, 1, 10, 20, 3));
    book.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    book.submit(resting(2, 2, 11, Side::Sell, 100)).unwrap();
    assert_eq!(
        book.submit(resting(3, 3, 11, Side::Sell, 110)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AskPriceLevels
        ))
    );
    assert_eq!(
        book.submit(replace(3, 1, 11, 110)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AskPriceLevels
        ))
    );
    book.submit(cancel(3, 2, 11)).unwrap();
    book.submit(replace(4, 1, 11, 110)).unwrap();
    assert!(book.level(Side::Sell, Price::from_raw(100)).is_none());
    assert!(book.level(Side::Sell, Price::from_raw(110)).is_some());
    book.validate().unwrap();

    let mut bids = OrderBook::with_limits(definition(), limits(2, 1, 1, 4, 8, 2));
    bids.submit(resting(1, 1, 11, Side::Buy, 90)).unwrap();
    assert_eq!(
        bids.submit(resting(2, 2, 11, Side::Buy, 80)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::BidPriceLevels
        ))
    );
    bids.validate().unwrap();
}

#[test]
fn accepted_identifier_limit_does_not_mask_a_known_duplicate() {
    let mut book = OrderBook::with_limits(definition(), limits(1, 1, 1, 1, 8, 1));
    book.submit(ioc(1, 1, 11, Side::Buy, 100)).unwrap();
    assert_eq!(
        book.submit(ioc(2, 2, 11, Side::Buy, 100)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AcceptedOrderIds
        ))
    );
    assert_eq!(
        book.submit(ioc(2, 1, 11, Side::Buy, 100)).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::DuplicateOrder)
    );
    book.validate().unwrap();
}

#[test]
fn checkpoint_restoration_rejects_insufficient_limits_and_adopts_sufficient_limits() {
    let original_limits = limits(2, 2, 2, 4, 8, 2);
    let mut book = OrderBook::with_limits(definition(), original_limits);
    book.submit(resting(1, 1, 11, Side::Buy, 90)).unwrap();
    book.submit(resting(2, 2, 12, Side::Sell, 110)).unwrap();
    let checkpoint = book.checkpoint(1, 5).unwrap();

    let too_small = limits(1, 1, 1, 2, 4, 1);
    let error = OrderBook::from_checkpoint_with_limits(checkpoint.clone(), too_small).unwrap_err();
    assert!(error.detail().contains("active-order capacity"));

    let restored = OrderBook::from_checkpoint_with_limits(checkpoint, original_limits).unwrap();
    assert_eq!(restored.limits(), original_limits);
    assert_eq!(
        restored.active_orders().unwrap(),
        book.active_orders().unwrap()
    );
    restored.validate().unwrap();
}

#[test]
fn durable_capacity_failure_precedes_wal_append_and_preserves_cancel_recovery() {
    let wal = TestWal::new();
    let selected = limits(1, 1, 1, 8, 3, 1);
    let cancel_command = cancel(3, 1, 11);
    let mut durable =
        DurableOrderBook::open_with_limits(wal.path(), definition(), selected, journal_options())
            .unwrap();
    durable.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    durable.submit(invalid_market(2, 2)).unwrap();
    assert!(matches!(
        durable.submit(resting(3, 3, 11, Side::Sell, 100)),
        Err(DurableError::Matching(MatchingError::CapacityExhausted(
            MatchingCapacity::AdmissionCommandHistory
        )))
    ));
    let cancelled = durable.submit(cancel_command).unwrap();
    assert_eq!(durable.book().active_order_count(), 0);
    durable.close().unwrap();

    let mut recovered =
        DurableOrderBook::open_with_limits(wal.path(), definition(), selected, journal_options())
            .unwrap();
    let retry = recovered.submit(cancel_command).unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.events, cancelled.events);
    assert!(matches!(
        recovered.submit(mass_cancel(4, 11)),
        Err(DurableError::Matching(MatchingError::CapacityExhausted(
            MatchingCapacity::CommandHistory
        )))
    ));
    recovered.close().unwrap();
}

#[test]
fn coupled_risk_checkpoint_restores_under_the_selected_matching_limits() {
    let selected = limits(1, 1, 1, 4, 8, 1);
    let mut managed = RiskManagedOrderBook::with_limits(definition(), selected);
    assert_eq!(
        managed
            .submit(ioc(1, 1, 11, Side::Buy, 100))
            .unwrap()
            .outcome,
        CommandOutcome::Rejected(RejectReason::RiskProfileMissing)
    );
    let checkpoint = managed.checkpoint(1, 1, 3).unwrap();
    let restored =
        RiskManagedOrderBook::from_checkpoint_with_limits(&checkpoint, selected).unwrap();
    assert_eq!(restored.book().limits(), selected);
    restored.validate().unwrap();
}
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable::{DurableError, DurableOrderBook};
