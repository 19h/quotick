use quotick::auction::{AuctionOrderConstraint, AuctionPricePolicy};
use quotick::auction_book::{
    CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder, CallAuctionRemainderPolicy,
    CallAuctionSelfTradePolicy, CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionCommand, CallAuctionCommandOutcome, CallAuctionEngineError, CallAuctionEngineLimits,
    CallAuctionEngineLimitsSpec, CallAuctionEventKind, CallAuctionMassCancel, CallAuctionPhase,
    CallAuctionPhaseControl, CallAuctionRejectReason, CallAuctionReplaceOrder,
    CallAuctionSubmitOrder, CallAuctionUncrossCommand,
};
use quotick::auction_risk::{
    CallAuctionRiskLimits, CallAuctionRiskLimitsSpec, CallAuctionRiskManagedEngine,
};
use quotick::codec::{BinaryCodec, CodecError};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::MassCancelScope;
use quotick::risk::{
    AccountRiskState, RiskError, RiskHashIndex, RiskLimitSpec, RiskLimits, RiskProfile,
};
use quotick::{
    AccountId, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, Side, TimestampNs,
};

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn instrument() -> InstrumentId {
    InstrumentId::new(41).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(7).unwrap()
}

fn auction(value: u64) -> AuctionId {
    AuctionId::new(value).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("AUCTION-RISK").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 5, Price::from_raw(-100), Price::from_raw(200)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Halted,
    })
    .unwrap()
}

fn engine_limits(active: usize, accepted: usize, retained: usize) -> CallAuctionEngineLimits {
    let book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: active,
        max_price_levels_per_side: active,
        max_accepted_order_ids: accepted,
        max_prepared_uncrosses: 2,
    })
    .unwrap();
    CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: retained,
        terminal_command_reserve: active + 2,
        max_retained_events: retained + active * 2 + 2,
        max_report_events: active * 2 + 1,
    })
    .unwrap()
}

fn managed(maximum_accounts: usize) -> CallAuctionRiskManagedEngine {
    let limits = CallAuctionRiskLimits::new(CallAuctionRiskLimitsSpec {
        auction: engine_limits(16, 64, 96),
        max_registered_accounts: maximum_accounts,
    })
    .unwrap();
    CallAuctionRiskManagedEngine::try_with_limits(definition(), limits).unwrap()
}

fn risk_profile(
    state: AccountRiskState,
    initial_position_lots: i128,
    limits: RiskLimitSpec,
) -> RiskProfile {
    RiskProfile::new(
        state,
        initial_position_lots,
        RiskLimits::new(limits).unwrap(),
    )
    .unwrap()
}

fn broad_profile(state: AccountRiskState, initial_position_lots: i128) -> RiskProfile {
    risk_profile(
        state,
        initial_position_lots,
        RiskLimitSpec {
            max_order_quantity_lots: 1_000,
            max_order_notional: 200_000,
            max_open_orders: 16,
            max_open_quantity_lots: 16_000,
            max_open_notional: 3_200_000,
            max_long_position_lots: i128::MAX.unsigned_abs(),
            max_short_position_lots: i128::MAX.unsigned_abs(),
        },
    )
}

fn phase_command(
    command_id: u64,
    auction_id: u64,
    expected_revision: u64,
    target_phase: CallAuctionPhase,
) -> CallAuctionCommand {
    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(auction_id),
        expected_phase_revision: expected_revision,
        target_phase,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn order(
    order_id: u64,
    owner: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u64,
) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        account(owner),
        instrument(),
        version(),
        side,
        constraint,
        Quantity::new(quantity).unwrap(),
    )
}

fn submit_command(command_id: u64, order: CallAuctionOrder) -> CallAuctionCommand {
    submit_command_at(command_id, 1, 1, order)
}

fn submit_command_at(
    command_id: u64,
    auction_id: u64,
    expected_revision: u64,
    order: CallAuctionOrder,
) -> CallAuctionCommand {
    CallAuctionCommand::Submit(CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(auction_id),
        expected_phase_revision: expected_revision,
        order,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn cancel_command(command_id: u64, owner: u64, order_id: u64) -> CallAuctionCommand {
    CallAuctionCommand::Cancel(quotick::auction_engine::CallAuctionCancelOrder {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        account_id: account(owner),
        order_id: OrderId::new(order_id).unwrap(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn mass_cancel_command(command_id: u64, owner: u64, scope: MassCancelScope) -> CallAuctionCommand {
    CallAuctionCommand::MassCancel(CallAuctionMassCancel {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        account_id: account(owner),
        scope,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn replace_command(
    command_id: u64,
    target_order_id: u64,
    replacement: CallAuctionOrder,
) -> CallAuctionCommand {
    CallAuctionCommand::Replace(CallAuctionReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(1),
        expected_phase_revision: 1,
        account_id: replacement.account_id(),
        target_order_id: OrderId::new(target_order_id).unwrap(),
        replacement,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn uncross_command(
    engine: &CallAuctionRiskManagedEngine,
    command_id: u64,
    remainder: CallAuctionRemainderPolicy,
) -> CallAuctionCommand {
    CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(1),
        expected_phase_revision: 2,
        price_band: engine.engine().book().instrument_price_band(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        uncross_policy: CallAuctionUncrossPolicy::new(
            remainder,
            CallAuctionSelfTradePolicy::Permit,
        ),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn assert_rejection(
    report: &quotick::auction_engine::CallAuctionExecutionReport,
    expected: CallAuctionRejectReason,
) {
    assert_eq!(
        report.outcome,
        CallAuctionCommandOutcome::Rejected(expected)
    );
    assert_eq!(report.events.len(), 1);
    assert_eq!(
        report.events[0].kind,
        CallAuctionEventKind::CommandRejected(expected)
    );
}

#[test]
fn core_rejections_precede_sequenced_risk_rejections_and_retries_are_exact() {
    let mut engine = managed(4);
    engine
        .register_account(account(1), broad_profile(AccountRiskState::Blocked, 0))
        .unwrap();
    engine
        .register_account(
            account(2),
            risk_profile(
                AccountRiskState::Active,
                0,
                RiskLimitSpec {
                    max_order_quantity_lots: 10,
                    max_order_notional: 199,
                    max_open_orders: 4,
                    max_open_quantity_lots: 40,
                    max_open_notional: 1_000,
                    max_long_position_lots: 40,
                    max_short_position_lots: 40,
                },
            ),
        )
        .unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();

    let stale = engine
        .submit(submit_command_at(
            2,
            1,
            0,
            order(1, 99, Side::Buy, AuctionOrderConstraint::Market, 1),
        ))
        .unwrap();
    assert_rejection(
        &stale,
        CallAuctionRejectReason::PhaseRevisionMismatch {
            observed: 0,
            current: 1,
        },
    );

    let missing_command = submit_command(
        3,
        order(2, 99, Side::Buy, AuctionOrderConstraint::Market, 1),
    );
    let missing = engine.submit(missing_command).unwrap();
    assert_rejection(&missing, CallAuctionRejectReason::RiskProfileMissing);
    let replay = engine.submit(missing_command).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.command_sequence, missing.command_sequence);
    assert!(replay.events.shares_storage_with(&missing.events));

    let blocked = engine
        .submit(submit_command(
            4,
            order(3, 1, Side::Buy, AuctionOrderConstraint::Market, 1),
        ))
        .unwrap();
    assert_rejection(&blocked, CallAuctionRejectReason::RiskAccountBlocked);

    let notional = engine
        .submit(submit_command(
            5,
            order(4, 2, Side::Buy, AuctionOrderConstraint::Market, 1),
        ))
        .unwrap();
    assert_rejection(&notional, CallAuctionRejectReason::RiskOrderNotionalLimit);
    assert_eq!(engine.engine().book().active_order_count(), 0);
    assert_eq!(engine.risk().reservation_count(), 0);
    assert_eq!(engine.engine().next_command_sequence(), 6);
    assert_eq!(engine.engine().next_event_sequence(), 6);
    engine.validate().unwrap();

    let encoded = notional.encode().unwrap();
    assert_eq!(
        quotick::auction_engine::CallAuctionExecutionReport::decode(&encoded).unwrap(),
        notional
    );
}

#[test]
fn signed_collar_valuation_is_conservative_and_owner_cancel_releases_exactly() {
    let mut engine = managed(2);
    engine
        .register_account(account(1), broad_profile(AccountRiskState::Active, 0))
        .unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(
                2,
                1,
                Side::Buy,
                AuctionOrderConstraint::Limit(Price::from_raw(50)),
                3,
            ),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            4,
            order(
                3,
                1,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(50)),
                4,
            ),
        ))
        .unwrap();

    let market = engine.risk().reservation(OrderId::new(1).unwrap()).unwrap();
    let buy = engine.risk().reservation(OrderId::new(2).unwrap()).unwrap();
    let sell = engine.risk().reservation(OrderId::new(3).unwrap()).unwrap();
    assert_eq!((market.valuation_per_lot(), market.notional()), (200, 400));
    assert_eq!((buy.valuation_per_lot(), buy.notional()), (100, 300));
    assert_eq!((sell.valuation_per_lot(), sell.notional()), (200, 800));
    let exposure = engine.risk().snapshot(account(1)).unwrap();
    assert_eq!(exposure.open_buy_lots(), 5);
    assert_eq!(exposure.open_sell_lots(), 4);
    assert_eq!(exposure.open_notional(), 1_500);
    assert_eq!(exposure.open_orders(), 3);

    engine.submit(cancel_command(5, 1, 2)).unwrap();
    let exposure = engine.risk().snapshot(account(1)).unwrap();
    assert_eq!(exposure.open_buy_lots(), 2);
    assert_eq!(exposure.open_sell_lots(), 4);
    assert_eq!(exposure.open_notional(), 1_200);
    assert_eq!(exposure.open_orders(), 2);
    assert!(
        engine
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .is_none()
    );
    engine.validate().unwrap();
}

#[test]
fn mass_cancel_releases_every_selected_reservation_once() {
    let mut engine = managed(4);
    engine
        .register_account(account(1), broad_profile(AccountRiskState::Active, 0))
        .unwrap();
    engine
        .register_account(account(2), broad_profile(AccountRiskState::Active, 0))
        .unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(10, 1, Side::Buy, AuctionOrderConstraint::Market, 3),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(
                20,
                1,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                5,
            ),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            4,
            order(30, 2, Side::Buy, AuctionOrderConstraint::Market, 7),
        ))
        .unwrap();
    assert_eq!(engine.risk().reservation_count(), 3);

    let command = mass_cancel_command(5, 1, MassCancelScope::All);
    let report = engine.submit(command).unwrap();
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(report.events.len(), 3);
    assert!(
        engine
            .risk()
            .reservation(OrderId::new(10).unwrap())
            .is_none()
    );
    assert!(
        engine
            .risk()
            .reservation(OrderId::new(20).unwrap())
            .is_none()
    );
    assert!(
        engine
            .risk()
            .reservation(OrderId::new(30).unwrap())
            .is_some()
    );
    let exposure = engine.risk().snapshot(account(1)).unwrap();
    assert_eq!(exposure.open_orders(), 0);
    assert_eq!(exposure.open_buy_lots(), 0);
    assert_eq!(exposure.open_sell_lots(), 0);
    assert_eq!(exposure.open_notional(), 0);

    let replay = engine.submit(command).unwrap();
    assert!(replay.replayed);
    assert_eq!(engine.risk().reservation_count(), 1);
    engine.validate().unwrap();
}

#[test]
fn cancel_replace_authorizes_the_net_reservation_and_rejection_keeps_the_old_order() {
    let mut engine = managed(1);
    engine
        .register_account(
            account(1),
            risk_profile(
                AccountRiskState::Active,
                0,
                RiskLimitSpec {
                    max_order_quantity_lots: 5,
                    max_order_notional: 10_000,
                    max_open_orders: 1,
                    max_open_quantity_lots: 5,
                    max_open_notional: 10_000,
                    max_long_position_lots: 100,
                    max_short_position_lots: 100,
                },
            ),
        )
        .unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(
                1,
                1,
                Side::Buy,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                5,
            ),
        ))
        .unwrap();

    let accepted = engine
        .submit(replace_command(
            3,
            1,
            order(
                2,
                1,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(50)),
                5,
            ),
        ))
        .unwrap();
    assert_eq!(accepted.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(accepted.events.len(), 2);
    assert!(
        engine
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .is_none()
    );
    let reservation = engine.risk().reservation(OrderId::new(2).unwrap()).unwrap();
    assert_eq!(reservation.side(), Side::Sell);
    assert_eq!(reservation.quantity_lots(), 5);
    assert_eq!(engine.risk().snapshot(account(1)).unwrap().open_orders(), 1);

    let rejected = engine
        .submit(replace_command(
            4,
            2,
            order(
                3,
                1,
                Side::Buy,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                6,
            ),
        ))
        .unwrap();
    assert_rejection(&rejected, CallAuctionRejectReason::RiskOrderQuantityLimit);
    assert!(
        engine
            .engine()
            .book()
            .order(OrderId::new(2).unwrap())
            .is_some()
    );
    assert!(
        engine
            .engine()
            .book()
            .order(OrderId::new(3).unwrap())
            .is_none()
    );
    assert!(
        engine
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .is_some()
    );
    assert!(
        engine
            .risk()
            .reservation(OrderId::new(3).unwrap())
            .is_none()
    );
    engine.validate().unwrap();
}

#[test]
fn uncross_updates_positions_and_retains_only_exact_partial_reservations() {
    let mut engine = managed(2);
    for owner in 1..=2 {
        engine
            .register_account(account(owner), broad_profile(AccountRiskState::Active, 0))
            .unwrap();
    }
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 6),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(
                2,
                2,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                5,
            ),
        ))
        .unwrap();
    engine
        .submit(phase_command(4, 1, 1, CallAuctionPhase::Frozen))
        .unwrap();
    let uncross = uncross_command(&engine, 5, CallAuctionRemainderPolicy::RetainAll);
    let report = engine.submit(uncross).unwrap();
    assert_eq!(report.outcome, CallAuctionCommandOutcome::Accepted);
    assert_eq!(
        engine.risk().snapshot(account(1)).unwrap().position_lots(),
        5
    );
    assert_eq!(
        engine.risk().snapshot(account(2)).unwrap().position_lots(),
        -5
    );
    let buyer = engine.risk().reservation(OrderId::new(1).unwrap()).unwrap();
    assert_eq!(buyer.quantity_lots(), 1);
    assert_eq!(buyer.notional(), 200);
    assert!(
        engine
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .is_none()
    );
    assert_eq!(engine.risk().snapshot(account(1)).unwrap().open_orders(), 1);
    assert_eq!(engine.risk().snapshot(account(2)).unwrap().open_orders(), 0);

    let replay = engine.submit(uncross).unwrap();
    assert!(replay.replayed);
    assert_eq!(
        engine.risk().snapshot(account(1)).unwrap().position_lots(),
        5
    );
    assert_eq!(engine.risk().reservation_count(), 1);
    engine.submit(cancel_command(6, 1, 1)).unwrap();
    assert_eq!(
        engine.risk().snapshot(account(1)).unwrap().open_notional(),
        0
    );
    assert_eq!(engine.risk().reservation_count(), 0);
    engine.validate().unwrap();
}

#[test]
fn cancel_all_releases_partial_remainders_and_same_account_fills_are_netted() {
    let mut engine = managed(1);
    let initial_position = i128::MAX - 1;
    engine
        .register_account(
            account(1),
            broad_profile(AccountRiskState::Active, initial_position),
        )
        .unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 1),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(2, 1, Side::Sell, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    engine
        .submit(phase_command(4, 1, 1, CallAuctionPhase::Frozen))
        .unwrap();
    let uncross = uncross_command(&engine, 5, CallAuctionRemainderPolicy::CancelAll);
    engine.submit(uncross).unwrap();
    assert_eq!(
        engine.risk().snapshot(account(1)).unwrap().position_lots(),
        initial_position
    );
    assert_eq!(engine.risk().reservation_count(), 0);
    let exposure = engine.risk().snapshot(account(1)).unwrap();
    assert_eq!(exposure.open_buy_lots(), 0);
    assert_eq!(exposure.open_sell_lots(), 0);
    assert_eq!(exposure.open_notional(), 0);
    assert_eq!(exposure.open_orders(), 0);
    engine.validate().unwrap();
}

#[test]
fn reduce_only_uses_aggregate_open_sides_and_position_bounds() {
    let mut engine = managed(1);
    engine
        .register_account(account(1), broad_profile(AccountRiskState::ReduceOnly, 5))
        .unwrap();
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    let buy = engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 1),
        ))
        .unwrap();
    assert_rejection(&buy, CallAuctionRejectReason::RiskReduceOnly);
    assert_eq!(engine.risk().reservation_count(), 0);

    engine
        .submit(submit_command(
            3,
            order(2, 1, Side::Sell, AuctionOrderConstraint::Market, 3),
        ))
        .unwrap();
    let crossing_zero = engine
        .submit(submit_command(
            4,
            order(3, 1, Side::Sell, AuctionOrderConstraint::Market, 3),
        ))
        .unwrap();
    assert_rejection(&crossing_zero, CallAuctionRejectReason::RiskReduceOnly);
    assert_eq!(engine.risk().reservation_count(), 1);
    assert_eq!(
        engine.risk().snapshot(account(1)).unwrap().open_sell_lots(),
        3
    );
    engine.validate().unwrap();
}

#[test]
fn fixed_hash_headroom_and_prepared_command_boundaries_remain_stable() {
    let mut engine = managed(2);
    let accounts_before = engine
        .risk()
        .hash_index_status(RiskHashIndex::AccountProfiles);
    let orders_before = engine
        .risk()
        .hash_index_status(RiskHashIndex::ActiveReservations);
    for owner in 1..=2 {
        engine
            .register_account(account(owner), broad_profile(AccountRiskState::Active, 0))
            .unwrap();
    }
    assert_eq!(
        engine.register_account(account(3), broad_profile(AccountRiskState::Active, 0)),
        Err(RiskError::ProfileCapacityExhausted { maximum: 2 })
    );
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    assert_eq!(
        engine.register_account(account(3), broad_profile(AccountRiskState::Active, 0)),
        Err(RiskError::ProfileRegistryLocked)
    );

    let command = submit_command(2, order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 1));
    let prepared = match engine.prepare(command).unwrap() {
        quotick::auction_engine::CallAuctionCommandPreparation::Ready(value) => value,
        quotick::auction_engine::CallAuctionCommandPreparation::Replay(_) => unreachable!(),
    };
    engine
        .submit(submit_command(
            3,
            order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 1),
        ))
        .unwrap();
    assert!(matches!(
        engine.commit(prepared),
        Err(CallAuctionEngineError::StalePreparation)
    ));
    assert!(
        engine
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .is_none()
    );
    assert!(
        engine
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .is_some()
    );
    let accounts_after = engine
        .risk()
        .hash_index_status(RiskHashIndex::AccountProfiles);
    let orders_after = engine
        .risk()
        .hash_index_status(RiskHashIndex::ActiveReservations);
    assert_eq!(
        accounts_after.allocated_entries,
        accounts_before.allocated_entries
    );
    assert_eq!(
        orders_after.allocated_entries,
        orders_before.allocated_entries
    );
    engine.validate().unwrap();
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one coupled checkpoint case keeps sharing, replay, exposure, suffix, and corruption evidence contiguous"
)]
fn checkpoint_reconstructs_reservations_positions_rejections_and_suffix_state() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<quotick::auction_risk::CallAuctionRiskCheckpoint>();
    let mut engine = managed(2);
    for owner in 1..=2 {
        engine
            .register_account(account(owner), broad_profile(AccountRiskState::Active, 0))
            .unwrap();
    }
    engine
        .submit(phase_command(1, 1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine
        .submit(submit_command(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 6),
        ))
        .unwrap();
    engine
        .submit(submit_command(
            3,
            order(
                2,
                2,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                5,
            ),
        ))
        .unwrap();
    let missing_command = submit_command(
        4,
        order(3, 99, Side::Buy, AuctionOrderConstraint::Market, 1),
    );
    assert_rejection(
        &engine.submit(missing_command).unwrap(),
        CallAuctionRejectReason::RiskProfileMissing,
    );
    engine
        .submit(phase_command(5, 1, 1, CallAuctionPhase::Frozen))
        .unwrap();
    let uncross = uncross_command(&engine, 6, CallAuctionRemainderPolicy::RetainAll);
    engine.submit(uncross).unwrap();

    let checkpoint = engine.checkpoint(1, 3, 15).unwrap();
    assert_eq!(checkpoint.wal_first_sequence(), 1);
    assert_eq!(checkpoint.auction().wal_metadata_sequence(), 3);
    assert_eq!(checkpoint.generation(), 15);
    assert_eq!(checkpoint.accounts().len(), 2);
    let shared = checkpoint.clone();
    assert!(shared.shares_account_storage_with(&checkpoint));
    assert!(
        shared
            .auction()
            .shares_accepted_order_storage_with(checkpoint.auction())
    );
    assert!(
        shared
            .auction()
            .shares_active_order_storage_with(checkpoint.auction())
    );
    assert!(
        shared
            .auction()
            .shares_history_storage_with(checkpoint.auction())
    );
    drop(checkpoint);
    let encoded = shared.encode().unwrap();
    let decoded = quotick::auction_risk::CallAuctionRiskCheckpoint::decode(&encoded).unwrap();
    assert_eq!(decoded, shared);
    assert!(!decoded.shares_account_storage_with(&shared));
    assert!(
        !decoded
            .auction()
            .shares_accepted_order_storage_with(shared.auction())
    );
    assert!(
        !decoded
            .auction()
            .shares_active_order_storage_with(shared.auction())
    );
    assert!(
        !decoded
            .auction()
            .shares_history_storage_with(shared.auction())
    );
    assert!(shared.is_successor_of(&shared));

    let mut restored = CallAuctionRiskManagedEngine::from_checkpoint(&shared).unwrap();
    assert_eq!(
        restored
            .risk()
            .snapshot(account(1))
            .unwrap()
            .position_lots(),
        5
    );
    assert_eq!(
        restored
            .risk()
            .snapshot(account(2))
            .unwrap()
            .position_lots(),
        -5
    );
    let reservation = restored
        .risk()
        .reservation(OrderId::new(1).unwrap())
        .unwrap();
    assert_eq!(reservation.quantity_lots(), 1);
    assert_eq!(reservation.notional(), 200);
    assert!(restored.submit(missing_command).unwrap().replayed);
    assert!(restored.submit(uncross).unwrap().replayed);

    let suffix = cancel_command(7, 1, 1);
    let expected = engine.submit(suffix).unwrap();
    assert_eq!(restored.submit(suffix).unwrap(), expected);
    assert_eq!(restored.risk().reservation_count(), 0);
    restored.validate().unwrap();

    let mut wrong_origin = encoded.clone();
    wrong_origin[0..8].copy_from_slice(&2_u64.to_le_bytes());
    assert!(matches!(
        quotick::auction_risk::CallAuctionRiskCheckpoint::decode(&wrong_origin),
        Err(CodecError::InvalidAuctionRiskCheckpoint(_))
    ));
    for end in 0..encoded.len() {
        assert!(quotick::auction_risk::CallAuctionRiskCheckpoint::decode(&encoded[..end]).is_err());
    }

    let insufficient_limits = CallAuctionRiskLimits::new(CallAuctionRiskLimitsSpec {
        auction: engine_limits(16, 64, 96),
        max_registered_accounts: 1,
    })
    .unwrap();
    assert!(
        CallAuctionRiskManagedEngine::from_checkpoint_with_limits(&shared, insufficient_limits)
            .is_err()
    );
}
