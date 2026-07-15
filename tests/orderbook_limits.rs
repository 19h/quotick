use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions};
use quotick::matching::{
    AccountControl, AccountControlAction, CancelOrder, Command, CommandOutcome, CommandPreparation,
    ExpirySweep, MassCancel, MassCancelScope, MatchingCapacity, MatchingError, MatchingHashIndex,
    NewOrder, OrderBook, OrderBookLimits, OrderBookLimitsError, OrderBookLimitsSpec, OrderDisplay,
    OrderType, RejectReason, ReplaceOrder, SelfTradePrevention, TimeInForce,
};
use quotick::risk::{
    AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedLimits, RiskManagedLimitsSpec,
    RiskManagedOrderBook, RiskProfile,
};
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
        hidden_orders_supported: false,
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
        max_account_controls: accepted_ids,
        max_retained_commands: history,
        cancellation_reserve,
        max_report_events: 64,
        max_retained_events: history.saturating_mul(64),
        max_prepared_order_selections: 2,
    })
    .unwrap()
}

fn account_control(
    command_id: u64,
    owner: u64,
    expected_revision: u64,
    action: AccountControlAction,
) -> Command {
    Command::AccountControl(AccountControl {
        command_id: CommandId::new(command_id).unwrap(),
        account_id: AccountId::new(owner).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        expected_revision,
        action,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn coupled_limits(matching: OrderBookLimits, max_registered_accounts: usize) -> RiskManagedLimits {
    RiskManagedLimits::new(RiskManagedLimitsSpec {
        matching,
        max_registered_accounts,
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

fn gtd_resting(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    price: i64,
    expires_at: u64,
) -> Command {
    new_order(
        command_id,
        order_id,
        account_id,
        side,
        OrderType::Limit(Price::from_raw(price)),
        TimeInForce::GoodTilTimestamp {
            expires_at: TimestampNs::from_unix_nanos(expires_at),
        },
    )
}

fn expiry_sweep(command_id: u64, through: u64, received_at: u64) -> Command {
    Command::ExpirySweep(ExpirySweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        through: TimestampNs::from_unix_nanos(through),
        received_at: TimestampNs::from_unix_nanos(received_at),
    })
}

fn resting_quantity(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    price: i64,
    quantity: u64,
) -> Command {
    let Command::New(mut order) = resting(command_id, order_id, account_id, side, price) else {
        unreachable!("resting helper always constructs a new order")
    };
    order.quantity = Quantity::new(quantity).unwrap();
    Command::New(order)
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

fn prepare_read_only(
    book: &OrderBook,
    command: Command,
) -> Result<CommandPreparation, MatchingError> {
    book.prepare(command)
}

#[test]
fn complete_hash_headroom_exists_at_construction_and_never_changes_on_commit() {
    let selected = limits(2, 2, 2, 8, 8, 2);
    let mut book = OrderBook::try_with_limits(definition(), selected).unwrap();
    let indexes = [
        MatchingHashIndex::ActiveOrders,
        MatchingHashIndex::ActiveAccounts,
        MatchingHashIndex::AcceptedOrderIds,
        MatchingHashIndex::AccountControls,
        MatchingHashIndex::CommandHistory,
    ];
    let initial = indexes.map(|index| book.hash_index_status(index));
    for status in initial {
        assert_eq!(status.occupied_entries, 0);
        assert!(status.allocated_entries >= status.configured_entries);
    }

    let CommandPreparation::Ready(prepared) =
        prepare_read_only(&book, resting(1, 1, 11, Side::Buy, 100)).unwrap()
    else {
        panic!("a fresh command cannot be a replay")
    };
    assert_eq!(
        indexes.map(|index| book.hash_index_status(index).allocated_entries),
        initial.map(|status| status.allocated_entries)
    );
    book.commit(prepared).unwrap();
    assert_eq!(
        indexes.map(|index| book.hash_index_status(index).allocated_entries),
        initial.map(|status| status.allocated_entries)
    );
    assert_eq!(
        indexes.map(|index| book.hash_index_status(index).occupied_entries),
        [1, 1, 1, 0, 1]
    );

    book.submit(cancel(2, 1, 11)).unwrap();
    book.submit(resting(3, 2, 12, Side::Sell, 101)).unwrap();
    assert_eq!(
        indexes.map(|index| book.hash_index_status(index).allocated_entries),
        initial.map(|status| status.allocated_entries)
    );
    assert_eq!(
        indexes.map(|index| book.hash_index_status(index).occupied_entries),
        [1, 1, 2, 0, 3]
    );
    book.submit(account_control(4, 11, 0, AccountControlAction::Enable))
        .unwrap();
    assert_eq!(
        indexes.map(|index| book.hash_index_status(index).allocated_entries),
        initial.map(|status| status.allocated_entries)
    );
    assert_eq!(
        indexes.map(|index| book.hash_index_status(index).occupied_entries),
        [1, 1, 2, 1, 4]
    );
    book.validate().unwrap();
}

#[test]
fn account_control_capacity_is_exact_never_evicted_and_updates_existing_accounts_when_full() {
    let selected = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 4,
        max_account_controls: 1,
        max_retained_commands: 6,
        cancellation_reserve: 1,
        max_report_events: 2,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let mut book = OrderBook::with_limits(definition(), selected);
    let allocated = book
        .hash_index_status(MatchingHashIndex::AccountControls)
        .allocated_entries;
    let first = account_control(1, 11, 0, AccountControlAction::Enable);
    assert_eq!(
        book.submit(first).unwrap().outcome,
        CommandOutcome::Accepted
    );
    assert!(book.submit(first).unwrap().replayed);
    assert_eq!(
        book.submit(account_control(2, 12, 0, AccountControlAction::Enable,)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AccountControls
        ))
    );
    assert_eq!(
        book.account_control(AccountId::new(12).unwrap()).revision(),
        0
    );
    assert_eq!(
        book.submit(account_control(
            3,
            11,
            1,
            AccountControlAction::BlockAndCancel,
        ))
        .unwrap()
        .outcome,
        CommandOutcome::Accepted
    );
    let status = book.hash_index_status(MatchingHashIndex::AccountControls);
    assert_eq!(status.configured_entries, 1);
    assert_eq!(status.occupied_entries, 1);
    assert_eq!(status.allocated_entries, allocated);
    assert_eq!(
        book.account_control(AccountId::new(11).unwrap()).revision(),
        2
    );
    book.validate().unwrap();
}

#[test]
fn checkpoint_restoration_enforces_the_selected_account_control_capacity() {
    let mut book = OrderBook::new(definition());
    for (command_id, owner) in [(1, 11), (2, 12)] {
        book.submit(account_control(
            command_id,
            owner,
            0,
            AccountControlAction::Enable,
        ))
        .unwrap();
    }
    let checkpoint = book.checkpoint(1, 5).unwrap();
    let base = OrderBookLimits::default();
    let insufficient = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: base.max_active_orders(),
        max_active_accounts: base.max_active_accounts(),
        max_price_levels_per_side: base.max_price_levels_per_side(),
        max_accepted_order_ids: base.max_accepted_order_ids(),
        max_account_controls: 1,
        max_retained_commands: base.max_retained_commands(),
        cancellation_reserve: base.cancellation_reserve(),
        max_report_events: base.max_report_events(),
        max_retained_events: base.max_retained_events(),
        max_prepared_order_selections: base.max_prepared_order_selections(),
    })
    .unwrap();
    let error = OrderBook::from_checkpoint_with_limits(&checkpoint, insufficient).unwrap_err();
    assert!(
        error
            .detail()
            .contains("account-control capacity is insufficient")
    );
}

#[test]
fn block_and_cancel_uses_the_protected_history_lane_but_enable_does_not() {
    let selected = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 4,
        max_account_controls: 2,
        max_retained_commands: 3,
        cancellation_reserve: 1,
        max_report_events: 2,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let mut book = OrderBook::with_limits(definition(), selected);
    book.submit(resting(1, 1, 11, Side::Buy, 100)).unwrap();
    assert_eq!(
        book.submit(resting(2, 1, 11, Side::Buy, 100))
            .unwrap()
            .outcome,
        CommandOutcome::Rejected(RejectReason::DuplicateOrder)
    );
    assert_eq!(
        book.submit(account_control(3, 11, 0, AccountControlAction::Enable,)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AdmissionCommandHistory
        ))
    );
    let blocked = book
        .submit(account_control(
            3,
            11,
            0,
            AccountControlAction::BlockAndCancel,
        ))
        .unwrap();
    assert_eq!(blocked.outcome, CommandOutcome::Accepted);
    assert_eq!(book.active_order_count(), 0);
    assert_eq!(book.retained_command_count(), 3);
    book.validate().unwrap();
}

#[test]
fn limit_spec_rejects_zero_incoherent_and_under_reserved_configurations() {
    let base = OrderBookLimitsSpec {
        max_active_orders: 2,
        max_active_accounts: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 8,
        max_account_controls: 8,
        max_retained_commands: 8,
        cancellation_reserve: 2,
        max_report_events: 3,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
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
    assert_eq!(
        OrderBookLimits::new(OrderBookLimitsSpec {
            max_account_controls: 0,
            ..base
        }),
        Err(OrderBookLimitsError::ZeroAccountControls)
    );
    assert_eq!(
        OrderBookLimits::new(OrderBookLimitsSpec {
            max_report_events: 0,
            ..base
        }),
        Err(OrderBookLimitsError::ZeroReportEvents)
    );
    assert_eq!(
        OrderBookLimits::new(OrderBookLimitsSpec {
            max_prepared_order_selections: 0,
            ..base
        }),
        Err(OrderBookLimitsError::ZeroPreparedOrderSelections)
    );
    assert_eq!(
        OrderBookLimits::new(OrderBookLimitsSpec {
            max_report_events: 2,
            ..base
        }),
        Err(OrderBookLimitsError::ReportEventsBelowMassCancelMaximum)
    );
    let bounded = OrderBookLimits::new(base).unwrap();
    assert_eq!(
        OrderBook::with_limits(definition(), bounded).limits(),
        bounded
    );
}

#[test]
fn retained_event_limits_reject_zero_overflow_and_incoherent_reserves() {
    let base = OrderBookLimitsSpec {
        max_active_orders: 2,
        max_active_accounts: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 8,
        max_account_controls: 8,
        max_retained_commands: 8,
        cancellation_reserve: 2,
        max_report_events: 3,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
    };
    for (max_report_events, max_retained_events, expected) in [
        (3, 0, OrderBookLimitsError::ZeroRetainedEvents),
        (3, 3, OrderBookLimitsError::NoOrdinaryEventCapacity),
        (
            4,
            6,
            OrderBookLimitsError::ReportEventsExceedOrdinaryEventCapacity,
        ),
        (
            3,
            6,
            OrderBookLimitsError::ActiveOrdersExceedOrdinaryEventCapacity,
        ),
    ] {
        assert_eq!(
            OrderBookLimits::new(OrderBookLimitsSpec {
                max_report_events,
                max_retained_events,
                ..base
            }),
            Err(expected)
        );
    }
    assert_eq!(
        OrderBookLimits::new(OrderBookLimitsSpec {
            max_active_orders: usize::MAX,
            max_active_accounts: 1,
            max_price_levels_per_side: 1,
            max_accepted_order_ids: usize::MAX,
            max_account_controls: 1,
            max_retained_commands: usize::MAX,
            cancellation_reserve: usize::MAX,
            max_report_events: usize::MAX,
            max_retained_events: usize::MAX,
            max_prepared_order_selections: 2,
        }),
        Err(OrderBookLimitsError::RetainedEventReserveOverflow)
    );
}

#[test]
fn constructor_reservation_failures_name_the_exact_resource() {
    let base = OrderBookLimitsSpec {
        max_active_orders: 2,
        max_active_accounts: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 8,
        max_account_controls: 8,
        max_retained_commands: 8,
        cancellation_reserve: 2,
        max_report_events: 3,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
    };
    let impossible_reservation = OrderBookLimits::new(OrderBookLimitsSpec {
        max_accepted_order_ids: usize::MAX,
        ..base
    })
    .unwrap();
    assert_eq!(
        OrderBook::try_with_limits(definition(), impossible_reservation),
        Err(MatchingError::CapacityReservationFailed(
            MatchingCapacity::AcceptedOrderIds
        ))
    );

    let impossible_history = OrderBookLimits::new(OrderBookLimitsSpec {
        max_retained_commands: usize::MAX,
        ..base
    })
    .unwrap();
    assert_eq!(
        OrderBook::try_with_limits(definition(), impossible_history),
        Err(MatchingError::CapacityReservationFailed(
            MatchingCapacity::CommandHistory
        ))
    );

    let impossible_controls = OrderBookLimits::new(OrderBookLimitsSpec {
        max_account_controls: usize::MAX,
        ..base
    })
    .unwrap();
    assert_eq!(
        OrderBook::try_with_limits(definition(), impossible_controls),
        Err(MatchingError::CapacityReservationFailed(
            MatchingCapacity::AccountControls
        ))
    );

    let impossible_events = OrderBookLimits::new(OrderBookLimitsSpec {
        max_retained_events: usize::MAX,
        ..base
    })
    .unwrap();
    assert_eq!(
        OrderBook::try_with_limits(definition(), impossible_events),
        Err(MatchingError::CapacityReservationFailed(
            MatchingCapacity::RetainedEvents
        ))
    );

    let arena_bound = (usize::MAX - 1) / 4;
    let impossible_active_hash = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: arena_bound,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: arena_bound,
        max_account_controls: 1,
        max_retained_commands: usize::MAX,
        cancellation_reserve: arena_bound,
        max_report_events: arena_bound + 1,
        max_retained_events: usize::MAX,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    assert_eq!(
        OrderBook::try_with_limits(definition(), impossible_active_hash),
        Err(MatchingError::CapacityReservationFailed(
            MatchingCapacity::ActiveOrders
        ))
    );

    let impossible_price_arena = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: arena_bound,
        max_active_accounts: 1,
        max_price_levels_per_side: arena_bound,
        max_accepted_order_ids: arena_bound,
        max_account_controls: 1,
        max_retained_commands: usize::MAX,
        cancellation_reserve: arena_bound,
        max_report_events: arena_bound + 1,
        max_retained_events: usize::MAX,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    assert_eq!(
        OrderBook::try_with_limits(definition(), impossible_price_arena),
        Err(MatchingError::CapacityReservationFailed(
            MatchingCapacity::BidPriceLevels
        ))
    );
}

#[test]
fn unrepresentable_order_selection_pool_names_the_exact_resource() {
    let impossible = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 2,
        max_active_accounts: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 8,
        max_account_controls: 8,
        max_retained_commands: 8,
        cancellation_reserve: 2,
        max_report_events: 3,
        max_retained_events: 16,
        max_prepared_order_selections: usize::MAX,
    })
    .unwrap();
    assert_eq!(
        OrderBook::try_with_limits(definition(), impossible),
        Err(MatchingError::CapacityReservationFailed(
            MatchingCapacity::PreparedOrderSelections
        ))
    );
}

#[test]
fn constructor_owned_order_selection_pool_is_bounded_reusable_and_drop_released() {
    let selected = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 2,
        max_active_accounts: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 8,
        max_account_controls: 8,
        max_retained_commands: 12,
        cancellation_reserve: 2,
        max_report_events: 3,
        max_retained_events: 32,
        max_prepared_order_selections: 1,
    })
    .unwrap();
    let mut book = OrderBook::with_limits(definition(), selected);
    book.submit(resting(1, 1, 11, Side::Buy, 100)).unwrap();
    book.submit(resting(2, 2, 11, Side::Buy, 99)).unwrap();
    let initial = book.order_selection_pool_status();
    assert_eq!(initial.configured_leases, 1);
    assert_eq!(initial.available_leases, 1);
    assert!(initial.allocated_order_ids_per_lease >= 2);

    let first_command = mass_cancel(3, 11);
    let CommandPreparation::Ready(first) = book.prepare(first_command).unwrap() else {
        panic!("new mass cancellation cannot replay")
    };
    assert_eq!(book.order_selection_pool_status().available_leases, 0);

    let CommandPreparation::Ready(empty) = book.prepare(mass_cancel(4, 99)).unwrap() else {
        panic!("empty mass cancellation cannot replay")
    };
    assert_eq!(book.order_selection_pool_status().available_leases, 0);
    drop(empty);

    let retained_commands = book.retained_command_count();
    let retained_events = book.event_arena_status().retained_events;
    let second_command = mass_cancel(5, 11);
    assert!(matches!(
        book.prepare(second_command),
        Err(MatchingError::PreparationCapacityExhausted { maximum: 1 })
    ));
    assert_eq!(book.retained_command_count(), retained_commands);
    assert_eq!(book.event_arena_status().retained_events, retained_events);

    drop(first);
    assert_eq!(book.order_selection_pool_status().available_leases, 1);
    let CommandPreparation::Ready(second) = book.prepare(second_command).unwrap() else {
        panic!("new mass cancellation cannot replay")
    };
    let report = book.commit(second).unwrap();
    assert_eq!(report.events.len(), 3);
    assert_eq!(book.active_order_count(), 0);
    let restored = book.order_selection_pool_status();
    assert_eq!(restored.available_leases, 1);
    assert_eq!(
        restored.allocated_order_ids_per_lease,
        initial.allocated_order_ids_per_lease
    );
    book.validate().unwrap();
}

#[test]
fn order_selection_leases_return_after_exact_race_stale_and_foreign_commit() {
    let selected = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 2,
        max_active_accounts: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 8,
        max_account_controls: 8,
        max_retained_commands: 12,
        cancellation_reserve: 2,
        max_report_events: 3,
        max_retained_events: 32,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let mut book = OrderBook::with_limits(definition(), selected);
    book.submit(resting(1, 1, 11, Side::Buy, 100)).unwrap();
    let raced_command = mass_cancel(2, 11);
    let CommandPreparation::Ready(first) = book.prepare(raced_command).unwrap() else {
        panic!("new mass cancellation cannot replay")
    };
    let CommandPreparation::Ready(exact_race) = book.prepare(raced_command).unwrap() else {
        panic!("same-generation mass cancellation cannot replay before commit")
    };
    assert_eq!(book.order_selection_pool_status().available_leases, 0);
    let committed = book.commit(first).unwrap();
    assert!(!committed.replayed);
    assert_eq!(book.order_selection_pool_status().available_leases, 1);
    let replayed = book.commit(exact_race).unwrap();
    assert!(replayed.replayed);
    assert_eq!(book.order_selection_pool_status().available_leases, 2);

    book.submit(resting(3, 3, 11, Side::Buy, 100)).unwrap();
    let CommandPreparation::Ready(stale) = book.prepare(mass_cancel(4, 11)).unwrap() else {
        panic!("new mass cancellation cannot replay")
    };
    book.submit(resting(5, 5, 12, Side::Sell, 200)).unwrap();
    assert_eq!(book.commit(stale), Err(MatchingError::StalePreparation));
    assert_eq!(book.order_selection_pool_status().available_leases, 2);

    let CommandPreparation::Ready(foreign) = book.prepare(mass_cancel(6, 11)).unwrap() else {
        panic!("new mass cancellation cannot replay")
    };
    assert_eq!(book.order_selection_pool_status().available_leases, 1);
    let mut other = OrderBook::with_limits(definition(), selected);
    assert_eq!(
        other.commit(foreign),
        Err(MatchingError::ForeignPreparation)
    );
    assert_eq!(book.order_selection_pool_status().available_leases, 2);

    let active_before_gate = book.active_order_count();
    let gate_rejected = book
        .reject_by_gate(mass_cancel(7, 11), RejectReason::RiskAccountBlocked)
        .unwrap();
    assert_eq!(
        gate_rejected.outcome,
        CommandOutcome::Rejected(RejectReason::RiskAccountBlocked)
    );
    assert_eq!(book.active_order_count(), active_before_gate);
    assert_eq!(book.order_selection_pool_status().available_leases, 2);
    book.validate().unwrap();
    other.validate().unwrap();
}

#[test]
fn durable_order_selection_pool_exhaustion_precedes_wal_append() {
    let wal = TestWal::new();
    let selected = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 2,
        max_active_accounts: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 8,
        max_account_controls: 8,
        max_retained_commands: 12,
        cancellation_reserve: 2,
        max_report_events: 3,
        max_retained_events: 32,
        max_prepared_order_selections: 1,
    })
    .unwrap();
    let mut durable =
        DurableOrderBook::open_with_limits(wal.path(), definition(), selected, journal_options())
            .unwrap();
    durable.submit(resting(1, 1, 11, Side::Buy, 100)).unwrap();
    durable.submit(resting(2, 2, 11, Side::Buy, 99)).unwrap();
    let held_command = mass_cancel(3, 11);
    let CommandPreparation::Ready(held) = durable.book().prepare(held_command).unwrap() else {
        panic!("new mass cancellation cannot replay")
    };
    let wal_bytes = fs::metadata(wal.path()).unwrap().len();
    assert!(matches!(
        durable.submit(mass_cancel(4, 11)),
        Err(DurableError::Matching(
            MatchingError::PreparationCapacityExhausted { maximum: 1 }
        ))
    ));
    assert_eq!(fs::metadata(wal.path()).unwrap().len(), wal_bytes);

    drop(held);
    let report = durable.submit(mass_cancel(4, 11)).unwrap();
    assert_eq!(report.events.len(), 3);
    assert_eq!(durable.book().active_order_count(), 0);
    assert_eq!(
        durable
            .book()
            .order_selection_pool_status()
            .available_leases,
        1
    );
    durable.book().validate().unwrap();
    durable.close().unwrap();
}

#[test]
fn report_event_capacity_fails_before_matching_mutation() {
    let constrained = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 4,
        max_account_controls: 4,
        max_retained_commands: 8,
        cancellation_reserve: 1,
        max_report_events: 2,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let mut book = OrderBook::with_limits(definition(), constrained);
    book.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    let before = book.active_orders().unwrap();
    let last_sequence = book.last_event_sequence();

    assert_eq!(
        book.submit(resting_quantity(2, 2, 12, Side::Buy, 100, 2)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::ReportEvents
        ))
    );
    assert_eq!(book.active_orders().unwrap(), before);
    assert_eq!(book.last_event_sequence(), last_sequence);
    assert!(book.order(OrderId::new(2).unwrap()).is_none());
    book.validate().unwrap();

    let mass_cancelled = book.submit(mass_cancel(2, 11)).unwrap();
    assert_eq!(mass_cancelled.events.len(), 2);
    assert_eq!(book.active_order_count(), 0);
    book.validate().unwrap();
}

#[test]
fn durable_report_event_capacity_failure_precedes_command_append() {
    let wal = TestWal::new();
    let constrained = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 4,
        max_account_controls: 4,
        max_retained_commands: 8,
        cancellation_reserve: 1,
        max_report_events: 2,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let mut durable = DurableOrderBook::open_with_limits(
        wal.path(),
        definition(),
        constrained,
        journal_options(),
    )
    .unwrap();
    durable.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    let wal_bytes = fs::metadata(wal.path()).unwrap().len();

    assert!(matches!(
        durable.submit(resting_quantity(2, 2, 12, Side::Buy, 100, 2)),
        Err(DurableError::Matching(MatchingError::CapacityExhausted(
            MatchingCapacity::ReportEvents
        )))
    ));
    assert_eq!(fs::metadata(wal.path()).unwrap().len(), wal_bytes);
    assert_eq!(durable.book().active_order_count(), 1);
    durable.close().unwrap();

    let mut recovered = DurableOrderBook::open_with_limits(
        wal.path(),
        definition(),
        constrained,
        journal_options(),
    )
    .unwrap();
    assert_eq!(recovered.book().active_order_count(), 1);
    recovered.submit(cancel(2, 1, 11)).unwrap();
    recovered.close().unwrap();
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
fn ordinary_lane_exhaustion_preserves_only_business_valid_expiry_sweeps() {
    let mut book = OrderBook::with_limits(definition(), limits(1, 1, 1, 8, 4, 1));
    book.submit(gtd_resting(1, 1, 11, Side::Sell, 100, 10))
        .unwrap();
    for (command_id, order_id) in [(2, 2), (3, 3)] {
        assert_eq!(
            book.submit(invalid_market(command_id, order_id))
                .unwrap()
                .outcome,
            CommandOutcome::Rejected(RejectReason::MarketOrderCannotRest)
        );
    }

    assert_eq!(
        book.submit(expiry_sweep(4, 11, 10)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AdmissionCommandHistory
        ))
    );
    let sweep = expiry_sweep(4, 10, 10);
    let report = book.submit(sweep).unwrap();
    assert_eq!(report.events.len(), 2);
    assert_eq!(book.active_order_count(), 0);
    assert!(book.submit(sweep).unwrap().replayed);
    assert_eq!(book.retained_command_count(), 4);
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
fn resting_capacity_does_not_reject_gtc_without_a_resting_residual() {
    let mut filled = OrderBook::with_limits(definition(), limits(2, 2, 1, 8, 16, 2));
    filled.submit(resting(1, 1, 11, Side::Buy, 80)).unwrap();
    filled.submit(resting(2, 2, 12, Side::Sell, 100)).unwrap();
    let report = filled
        .submit(resting(3, 3, 13, Side::Buy, 100))
        .expect("fully executed GTC consumes no resting capacity");
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert!(filled.order(OrderId::new(3).unwrap()).is_none());
    assert_eq!(filled.active_order_count(), 1);
    filled.validate().unwrap();

    let mut stp = OrderBook::with_limits(definition(), limits(1, 1, 1, 4, 8, 1));
    stp.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    let report = stp
        .submit(resting(2, 2, 11, Side::Buy, 100))
        .expect("cancel-aggressor GTC consumes no resting capacity");
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert!(report.events.iter().any(|event| matches!(
        event.kind,
        quotick::matching::EventKind::OrderCancelled {
            order_id,
            reason: quotick::matching::CancelReason::SelfTradeAggressor,
            ..
        } if order_id == OrderId::new(2).unwrap()
    )));
    assert!(stp.order(OrderId::new(2).unwrap()).is_none());
    assert_eq!(stp.active_order_count(), 1);
    stp.validate().unwrap();
}

#[test]
fn resting_residual_reuses_capacity_released_by_fully_removed_makers() {
    let mut active = OrderBook::with_limits(definition(), limits(1, 1, 1, 8, 12, 1));
    active
        .submit(resting_quantity(1, 1, 11, Side::Sell, 100, 1))
        .unwrap();
    active
        .submit(resting_quantity(2, 2, 12, Side::Buy, 100, 2))
        .expect("maker removal must release both the active-order and account slot");
    let residual = active.order(OrderId::new(2).unwrap()).unwrap();
    assert_eq!(residual.account_id, AccountId::new(12).unwrap());
    assert_eq!(residual.leaves_quantity, Quantity::new(1).unwrap());
    assert_eq!(active.active_order_count(), 1);
    assert_eq!(
        active.account_active_order_count(AccountId::new(11).unwrap(), MassCancelScope::All,),
        0
    );
    active.validate().unwrap();

    let mut multi_order_account = OrderBook::with_limits(definition(), limits(3, 1, 3, 10, 16, 3));
    multi_order_account
        .submit(resting_quantity(1, 1, 11, Side::Sell, 100, 1))
        .unwrap();
    multi_order_account
        .submit(resting_quantity(2, 2, 11, Side::Sell, 101, 1))
        .unwrap();
    multi_order_account
        .submit(resting_quantity(3, 3, 12, Side::Buy, 101, 3))
        .expect("removing every maker for one account must release its account slot");
    assert!(
        multi_order_account
            .order(OrderId::new(1).unwrap())
            .is_none()
    );
    assert!(
        multi_order_account
            .order(OrderId::new(2).unwrap())
            .is_none()
    );
    assert_eq!(
        multi_order_account
            .order(OrderId::new(3).unwrap())
            .unwrap()
            .leaves_quantity,
        Quantity::new(1).unwrap()
    );
    assert_eq!(multi_order_account.active_order_count(), 1);
    multi_order_account.validate().unwrap();

    let mut cancel_resting = OrderBook::with_limits(definition(), limits(2, 1, 2, 8, 12, 2));
    cancel_resting
        .submit(resting_quantity(1, 1, 11, Side::Sell, 100, 1))
        .unwrap();
    cancel_resting
        .submit(resting_quantity(2, 2, 11, Side::Buy, 80, 1))
        .unwrap();
    let Command::New(mut replacement) = resting_quantity(3, 3, 11, Side::Buy, 100, 2) else {
        unreachable!("resting helper always constructs a new order")
    };
    replacement.self_trade_prevention = SelfTradePrevention::CancelResting;
    cancel_resting
        .submit(Command::New(replacement))
        .expect("cancel-resting must release a maker slot before residual append");
    assert!(cancel_resting.order(OrderId::new(1).unwrap()).is_none());
    assert_eq!(cancel_resting.active_order_count(), 2);
    cancel_resting.validate().unwrap();
}

#[test]
fn resting_residual_does_not_release_an_account_with_an_uncrossed_order() {
    let mut book = OrderBook::with_limits(definition(), limits(3, 1, 2, 8, 12, 3));
    book.submit(resting_quantity(1, 1, 11, Side::Sell, 100, 1))
        .unwrap();
    book.submit(resting_quantity(2, 2, 11, Side::Buy, 80, 1))
        .unwrap();
    assert_eq!(
        book.submit(resting_quantity(3, 3, 12, Side::Buy, 100, 2)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::ActiveAccounts
        ))
    );
    assert!(book.order(OrderId::new(1).unwrap()).is_some());
    assert!(book.order(OrderId::new(2).unwrap()).is_some());
    assert!(book.order(OrderId::new(3).unwrap()).is_none());
    book.validate().unwrap();
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
fn replacement_level_capacity_uses_the_exact_final_resting_state() {
    let mut filled = OrderBook::with_limits(definition(), limits(3, 2, 1, 8, 16, 3));
    filled.submit(resting(1, 1, 11, Side::Sell, 110)).unwrap();
    filled.submit(resting(2, 2, 11, Side::Sell, 110)).unwrap();
    filled.submit(resting(3, 3, 12, Side::Buy, 100)).unwrap();

    let report = filled
        .submit(replace(4, 1, 11, 100))
        .expect("a fully executed replacement creates no new ask level");
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert!(filled.order(OrderId::new(1).unwrap()).is_none());
    assert!(filled.order(OrderId::new(2).unwrap()).is_some());
    assert!(filled.order(OrderId::new(3).unwrap()).is_none());
    assert!(filled.level(Side::Sell, Price::from_raw(100)).is_none());
    assert!(filled.level(Side::Sell, Price::from_raw(110)).is_some());
    filled.validate().unwrap();

    let mut stp = OrderBook::with_limits(definition(), limits(3, 1, 1, 8, 16, 3));
    stp.submit(resting(1, 1, 11, Side::Sell, 110)).unwrap();
    stp.submit(resting(2, 2, 11, Side::Sell, 110)).unwrap();
    stp.submit(resting(3, 3, 11, Side::Buy, 100)).unwrap();

    let report = stp
        .submit(replace(4, 1, 11, 100))
        .expect("an STP-terminated replacement creates no new ask level");
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert!(report.events.iter().any(|event| matches!(
        event.kind,
        quotick::matching::EventKind::OrderCancelled {
            order_id,
            reason: quotick::matching::CancelReason::SelfTradeAggressor,
            ..
        } if order_id == OrderId::new(1).unwrap()
    )));
    assert!(stp.order(OrderId::new(1).unwrap()).is_none());
    assert!(stp.order(OrderId::new(2).unwrap()).is_some());
    assert!(stp.order(OrderId::new(3).unwrap()).is_some());
    assert!(stp.level(Side::Sell, Price::from_raw(100)).is_none());
    stp.validate().unwrap();

    let mut residual = OrderBook::with_limits(definition(), limits(3, 2, 1, 8, 16, 3));
    residual
        .submit(resting_quantity(1, 1, 11, Side::Sell, 110, 2))
        .unwrap();
    residual.submit(resting(2, 2, 11, Side::Sell, 110)).unwrap();
    residual.submit(resting(3, 3, 12, Side::Buy, 100)).unwrap();
    let Command::Replace(mut replacement) = replace(4, 1, 11, 100) else {
        unreachable!("replace helper always constructs a replacement")
    };
    replacement.new_quantity = Quantity::new(2).unwrap();
    assert_eq!(
        residual.submit(Command::Replace(replacement)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AskPriceLevels
        ))
    );
    assert_eq!(
        residual
            .order(OrderId::new(1).unwrap())
            .unwrap()
            .leaves_quantity,
        Quantity::new(2).unwrap()
    );
    assert!(residual.order(OrderId::new(3).unwrap()).is_some());
    residual.validate().unwrap();
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
    let error = OrderBook::from_checkpoint_with_limits(&checkpoint, too_small).unwrap_err();
    assert!(error.detail().contains("active-order capacity"));

    let restored = OrderBook::from_checkpoint_with_limits(&checkpoint, original_limits).unwrap();
    assert_eq!(restored.limits(), original_limits);
    assert_eq!(
        restored.active_orders().unwrap(),
        book.active_orders().unwrap()
    );
    restored.validate().unwrap();
}

#[test]
fn checkpoint_and_wal_recovery_reject_a_lower_report_event_limit() {
    let original_limits = limits(1, 1, 1, 4, 8, 1);
    let mut book = OrderBook::with_limits(definition(), original_limits);
    book.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    let report = book
        .submit(resting_quantity(2, 2, 12, Side::Buy, 100, 2))
        .unwrap();
    assert_eq!(report.events.len(), 3);
    let checkpoint = book.checkpoint(1, 5).unwrap();
    let lower_limit = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 4,
        max_account_controls: 4,
        max_retained_commands: 8,
        cancellation_reserve: 1,
        max_report_events: 2,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let error = OrderBook::from_checkpoint_with_limits(&checkpoint, lower_limit).unwrap_err();
    assert!(
        error
            .detail()
            .contains("report exceeds selected event capacity")
    );

    let wal = TestWal::new();
    let mut durable = DurableOrderBook::open_with_limits(
        wal.path(),
        definition(),
        original_limits,
        journal_options(),
    )
    .unwrap();
    durable.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    durable
        .submit(resting_quantity(2, 2, 12, Side::Buy, 100, 2))
        .unwrap();
    durable.close().unwrap();
    assert!(matches!(
        DurableOrderBook::open_with_limits(
            wal.path(),
            definition(),
            lower_limit,
            journal_options()
        ),
        Err(DurableError::Matching(MatchingError::CapacityExhausted(
            MatchingCapacity::ReportEvents
        )))
    ));
}

#[test]
fn checkpoint_restoration_rejects_a_lower_total_event_limit() {
    let source_limits = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 8,
        max_account_controls: 4,
        max_retained_commands: 8,
        cancellation_reserve: 1,
        max_report_events: 2,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let mut book = OrderBook::with_limits(definition(), source_limits);
    for command_id in 1..=3 {
        let report = book
            .submit(ioc(command_id, command_id, 10 + command_id, Side::Buy, 100))
            .unwrap();
        assert_eq!(report.events.len(), 2);
    }
    let checkpoint = book.checkpoint(1, 7).unwrap();
    let lower_total = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 8,
        max_account_controls: 4,
        max_retained_commands: 8,
        cancellation_reserve: 1,
        max_report_events: 2,
        max_retained_events: 4,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let error = OrderBook::from_checkpoint_with_limits(&checkpoint, lower_total).unwrap_err();
    assert!(error.detail().contains("retained events"));
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
fn durable_replacement_recovery_preserves_exact_final_level_admission() {
    let wal = TestWal::new();
    let selected = limits(3, 2, 1, 8, 16, 3);
    let replacement = replace(4, 1, 11, 100);
    let mut durable =
        DurableOrderBook::open_with_limits(wal.path(), definition(), selected, journal_options())
            .unwrap();
    durable.submit(resting(1, 1, 11, Side::Sell, 110)).unwrap();
    durable.submit(resting(2, 2, 11, Side::Sell, 110)).unwrap();
    durable.submit(resting(3, 3, 12, Side::Buy, 100)).unwrap();
    let committed = durable.submit(replacement).unwrap();
    assert_eq!(committed.outcome, CommandOutcome::Accepted);
    assert!(durable.book().order(OrderId::new(1).unwrap()).is_none());
    assert!(durable.book().order(OrderId::new(2).unwrap()).is_some());
    assert!(durable.book().order(OrderId::new(3).unwrap()).is_none());
    durable.close().unwrap();

    let mut recovered =
        DurableOrderBook::open_with_limits(wal.path(), definition(), selected, journal_options())
            .unwrap();
    let replay = recovered.submit(replacement).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.events, committed.events);
    assert!(recovered.book().order(OrderId::new(1).unwrap()).is_none());
    assert!(recovered.book().order(OrderId::new(2).unwrap()).is_some());
    assert!(recovered.book().order(OrderId::new(3).unwrap()).is_none());
    recovered.book().validate().unwrap();
    recovered.close().unwrap();
}

#[test]
fn risk_checkpoint_preserves_exact_final_replacement_level_admission() {
    let selected = limits(3, 2, 1, 8, 16, 3);
    let risk_limits = RiskLimits::new(RiskLimitSpec {
        max_order_quantity_lots: 10,
        max_order_notional: 10_000,
        max_open_orders: 3,
        max_open_quantity_lots: 10,
        max_open_notional: 10_000,
        max_long_position_lots: 10,
        max_short_position_lots: 10,
    })
    .unwrap();
    let profile = RiskProfile::new(AccountRiskState::Active, 0, risk_limits).unwrap();
    let selected_risk = coupled_limits(selected, 2);
    let mut managed = RiskManagedOrderBook::with_limits(definition(), selected_risk);
    managed
        .register_account(AccountId::new(11).unwrap(), profile)
        .unwrap();
    managed
        .register_account(AccountId::new(12).unwrap(), profile)
        .unwrap();
    managed.submit(resting(1, 1, 11, Side::Sell, 110)).unwrap();
    managed.submit(resting(2, 2, 11, Side::Sell, 110)).unwrap();
    managed.submit(resting(3, 3, 12, Side::Buy, 100)).unwrap();
    let replacement = replace(4, 1, 11, 100);
    let committed = managed.submit(replacement).unwrap();
    assert_eq!(committed.outcome, CommandOutcome::Accepted);
    managed.validate().unwrap();

    let checkpoint = managed.checkpoint(1, 3, 11).unwrap();
    let mut restored =
        RiskManagedOrderBook::from_checkpoint_with_limits(&checkpoint, selected_risk).unwrap();
    let replay = restored.submit(replacement).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.events, committed.events);
    assert!(restored.book().order(OrderId::new(1).unwrap()).is_none());
    assert!(restored.book().order(OrderId::new(2).unwrap()).is_some());
    assert!(restored.book().order(OrderId::new(3).unwrap()).is_none());
    restored.validate().unwrap();
}

#[test]
fn coupled_risk_checkpoint_restores_under_the_selected_matching_limits() {
    let selected = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 4,
        max_account_controls: 4,
        max_retained_commands: 8,
        cancellation_reserve: 1,
        max_report_events: 64,
        max_retained_events: 128,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let selected_risk = coupled_limits(selected, 1);
    let mut managed = RiskManagedOrderBook::with_limits(definition(), selected_risk);
    assert_eq!(
        managed
            .submit(ioc(1, 1, 11, Side::Buy, 100))
            .unwrap()
            .outcome,
        CommandOutcome::Rejected(RejectReason::RiskProfileMissing)
    );
    let checkpoint = managed.checkpoint(1, 1, 3).unwrap();
    let restored =
        RiskManagedOrderBook::from_checkpoint_with_limits(&checkpoint, selected_risk).unwrap();
    assert_eq!(restored.book().limits(), selected);
    assert!(restored.risk().reservation_capacity() >= selected.max_active_orders());
    restored.validate().unwrap();
}

#[test]
fn retained_event_arena_has_an_independent_protected_cancellation_lane() {
    let selected = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 8,
        max_account_controls: 8,
        max_retained_commands: 8,
        cancellation_reserve: 1,
        max_report_events: 2,
        max_retained_events: 4,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    assert_eq!(selected.cancellation_event_reserve(), 2);
    assert_eq!(selected.ordinary_event_capacity(), 2);

    let mut book = OrderBook::with_limits(definition(), selected);
    let initial = book.event_arena_status();
    assert_eq!(initial.configured_events, 4);
    assert_eq!(initial.retained_events, 0);
    let rested = book.submit(resting(1, 1, 11, Side::Sell, 100)).unwrap();
    assert_eq!(rested.events.len(), 2);
    assert_eq!(book.event_arena_status().retained_events, 2);

    assert_eq!(
        book.submit(resting(2, 1, 12, Side::Buy, 50)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::AdmissionEventHistory
        ))
    );
    let cancel_command = cancel(3, 1, 11);
    let cancelled = book.submit(cancel_command).unwrap();
    assert_eq!(cancelled.events.len(), 1);
    assert_eq!(book.event_arena_status().retained_events, 3);
    assert_eq!(book.submit(mass_cancel(4, 11)).unwrap().events.len(), 1);
    let full = book.event_arena_status();
    assert_eq!(full.retained_events, 4);
    assert_eq!(full.allocated_events, initial.allocated_events);
    assert!(book.submit(cancel_command).unwrap().replayed);
    assert_eq!(
        book.submit(mass_cancel(5, 11)),
        Err(MatchingError::CapacityExhausted(
            MatchingCapacity::RetainedEvents
        ))
    );
    book.validate().unwrap();
}

#[test]
fn preparation_and_stale_commit_do_not_consume_event_arena_storage() {
    let selected = OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 1,
        max_active_accounts: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 8,
        max_account_controls: 8,
        max_retained_commands: 8,
        cancellation_reserve: 1,
        max_report_events: 4,
        max_retained_events: 16,
        max_prepared_order_selections: 2,
    })
    .unwrap();
    let mut book = OrderBook::with_limits(definition(), selected);
    let CommandPreparation::Ready(first) = book.prepare(ioc(1, 1, 11, Side::Buy, 100)).unwrap()
    else {
        panic!("new command cannot replay")
    };
    let CommandPreparation::Ready(stale) = book.prepare(ioc(2, 2, 12, Side::Buy, 100)).unwrap()
    else {
        panic!("new command cannot replay")
    };
    assert_eq!(book.event_arena_status().retained_events, 0);
    let report = book.commit(first).unwrap();
    assert_eq!(
        book.event_arena_status().retained_events,
        report.events.len()
    );
    assert_eq!(book.commit(stale), Err(MatchingError::StalePreparation));
    assert_eq!(
        book.event_arena_status().retained_events,
        report.events.len()
    );
    book.validate().unwrap();
}
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable::{DurableError, DurableOrderBook};
