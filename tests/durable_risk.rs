use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable_risk::{DurableRiskError, DurableRiskOrderBook};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, TradingState,
};
use quotick::journal::{Durability, Journal, JournalOptions, JournalReader, RecordKind};
use quotick::matching::{
    AccountAdmissionState, AccountControl, AccountControlAction, Command, CommandOutcome,
    ImmediateExecutionCurveOutcome, ImmediateExecutionOutcome, ImmediateExecutionRequest,
    ImmediateExecutionSubmission, MatchingCapacity, MatchingError, NewOrder, OrderBookLimits,
    OrderBookLimitsSpec, OrderType, RejectReason, SelfTradePrevention, StopActivation, TimeInForce,
};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskError, RiskLimitSpec, RiskLimits,
    RiskManagedLimits, RiskManagedLimitsSpec, RiskManagedOrderBook, RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-durable-risk-{label}-{}-{nonce}.wal",
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
        symbol: InstrumentSymbol::new("DURABLE-RISK").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(-1_000), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 100).unwrap(),
        reserve: quotick::instrument::ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn risk_profile(state: AccountRiskState, max_order_quantity_lots: u64) -> RiskProfile {
    RiskProfile::new(
        state,
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

fn profiles() -> [AccountRiskDefinition; 2] {
    [
        AccountRiskDefinition::new(account(12), risk_profile(AccountRiskState::Active, 10)),
        AccountRiskDefinition::new(account(11), risk_profile(AccountRiskState::Active, 10)),
    ]
}

fn command(command_id: u64, order_id: u64, owner: u64, quantity: u64) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side: Side::Buy,
        quantity: Quantity::new(quantity).unwrap(),
        display: quotick::matching::OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(100)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn sell(command_id: u64, order_id: u64, owner: u64, quantity: u64) -> Command {
    let Command::New(mut order) = command(command_id, order_id, owner, quantity) else {
        unreachable!("fixture is a new order")
    };
    order.side = Side::Sell;
    Command::New(order)
}

fn immediate_submission(
    command_id: u64,
    order_id: u64,
    quantity: u64,
) -> ImmediateExecutionSubmission {
    ImmediateExecutionSubmission::new(
        CommandId::new(command_id).unwrap(),
        OrderId::new(order_id).unwrap(),
        InstrumentId::new(1).unwrap(),
        InstrumentVersion::new(1).unwrap(),
        TimestampNs::from_unix_nanos(command_id),
        ImmediateExecutionRequest::new(
            account(12),
            Side::Buy,
            Quantity::new(quantity).unwrap(),
            StopActivation::Limit(Price::from_raw(100)),
            SelfTradePrevention::CancelAggressor,
        ),
    )
}

fn account_control(
    command_id: u64,
    owner: u64,
    expected_revision: u64,
    action: AccountControlAction,
) -> Command {
    Command::AccountControl(AccountControl {
        command_id: CommandId::new(command_id).unwrap(),
        account_id: account(owner),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        expected_revision,
        action,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn managed_limits(matching: OrderBookLimits, max_registered_accounts: usize) -> RiskManagedLimits {
    RiskManagedLimits::new(RiskManagedLimitsSpec {
        matching,
        max_registered_accounts,
    })
    .unwrap()
}

fn frames(path: &Path) -> Vec<quotick::journal::JournalFrame> {
    JournalReader::open(path, options())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn profile_capacity_failure_precedes_wal_creation() {
    let file = TestFile::new("profile-capacity");
    let error = DurableRiskOrderBook::open_with_limits(
        file.path(),
        definition(),
        &profiles(),
        managed_limits(OrderBookLimits::default(), 1),
        options(),
    )
    .expect_err("two profiles cannot fit one configured registry slot");
    assert!(matches!(
        error,
        DurableRiskError::Risk(RiskError::ProfileCapacityExhausted { maximum: 1 })
    ));
    assert!(!file.path().exists());
}

#[test]
fn durable_risk_round_trip_binds_sorted_profiles_and_restores_reservations() {
    let file = TestFile::new("round-trip");
    let value = command(1, 1, 11, 5);
    let mut durable =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles(), options()).unwrap();
    let initial = frames(file.path());
    assert_eq!(
        initial
            .iter()
            .map(quotick::journal::JournalFrame::kind)
            .collect::<Vec<_>>(),
        vec![
            RecordKind::InstrumentDefinition,
            RecordKind::AccountRiskDefinition,
            RecordKind::AccountRiskDefinition,
        ]
    );
    let first: AccountRiskDefinition = initial[1].decode().unwrap();
    let second: AccountRiskDefinition = initial[2].decode().unwrap();
    assert_eq!(first.account_id(), account(11));
    assert_eq!(second.account_id(), account(12));

    let report = durable.submit(value).unwrap();
    assert_eq!(report.outcome, CommandOutcome::Accepted);
    assert_eq!(
        durable
            .managed()
            .risk()
            .snapshot(account(11))
            .unwrap()
            .open_buy_lots(),
        5
    );
    let frame_count = frames(file.path()).len();
    assert!(durable.submit(value).unwrap().replayed);
    assert_eq!(frames(file.path()).len(), frame_count);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles(), options()).unwrap();
    assert_eq!(reopened.recovery().replayed_commands, 1);
    assert_eq!(
        reopened
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        5
    );
    reopened.managed().validate().unwrap();
}

#[test]
fn durable_risk_conditional_execution_is_atomic_with_wal_and_positions() {
    let file = TestFile::new("conditional-immediate");
    let mut durable =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles(), options()).unwrap();
    durable.submit(sell(1, 1, 11, 5)).unwrap();
    let value = immediate_submission(2, 2, 3);
    let frames_before = frames(file.path()).len();

    assert!(matches!(
        durable
            .submit_immediate_execution_if(value, |_| false)
            .unwrap(),
        ImmediateExecutionOutcome::Declined(_)
    ));
    assert_eq!(frames(file.path()).len(), frames_before);
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
        .submit_immediate_execution_if(value, |quote| quote.raw_price_notional() <= 300)
        .unwrap();
    assert!(matches!(
        accepted,
        ImmediateExecutionOutcome::Reported {
            quote: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(frames(file.path()).len(), frames_before + 2);
    assert_eq!(
        durable
            .managed()
            .risk()
            .snapshot(account(12))
            .unwrap()
            .position_lots(),
        3
    );
    let frames_after = frames(file.path()).len();
    assert!(matches!(
        durable
            .submit_immediate_execution_if(value, |_| panic!("replay bypasses the predicate"))
            .unwrap(),
        ImmediateExecutionOutcome::Reported {
            quote: None,
            report
        } if report.replayed
    ));
    assert_eq!(frames(file.path()).len(), frames_after);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles(), options()).unwrap();
    assert_eq!(
        reopened
            .managed()
            .risk()
            .snapshot(account(12))
            .unwrap()
            .position_lots(),
        3
    );
    reopened.managed().validate().unwrap();
}

#[test]
fn durable_risk_conditional_curve_is_atomic_with_wal_and_positions() {
    let file = TestFile::new("conditional-immediate-curve");
    let mut durable =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles(), options()).unwrap();
    durable.submit(sell(1, 1, 11, 5)).unwrap();
    let value = immediate_submission(2, 2, 3);
    let frames_before = frames(file.path()).len();

    assert!(matches!(
        durable
            .try_submit_immediate_execution_curve_if(value, |_| false)
            .unwrap(),
        ImmediateExecutionCurveOutcome::Declined(_)
    ));
    assert_eq!(frames(file.path()).len(), frames_before);
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
        .try_submit_immediate_execution_curve_if(value, |curve| {
            curve.levels().len() == 1 && curve.quote().raw_price_notional() == 300
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ImmediateExecutionCurveOutcome::Reported {
            curve: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(frames(file.path()).len(), frames_before + 2);
    assert_eq!(
        durable
            .managed()
            .risk()
            .snapshot(account(12))
            .unwrap()
            .position_lots(),
        3
    );
    let frames_after = frames(file.path()).len();
    assert!(matches!(
        durable
            .try_submit_immediate_execution_curve_if(value, |_| {
                panic!("replay bypasses curve allocation and predicate")
            })
            .unwrap(),
        ImmediateExecutionCurveOutcome::Reported {
            curve: None,
            report
        } if report.replayed
    ));
    assert_eq!(frames(file.path()).len(), frames_after);
    durable.sync_all().unwrap();
    drop(durable);

    let reopened =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles(), options()).unwrap();
    assert_eq!(
        reopened
            .managed()
            .risk()
            .snapshot(account(12))
            .unwrap()
            .position_lots(),
        3
    );
    reopened.managed().validate().unwrap();
}

#[test]
fn durable_account_control_recovers_the_fence_revision_and_atomic_cancellation() {
    let file = TestFile::new("account-control");
    let block = account_control(2, 11, 0, AccountControlAction::BlockAndCancel);
    let mut durable =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles(), options()).unwrap();
    durable.submit(command(1, 1, 11, 5)).unwrap();
    assert_eq!(
        durable.submit(block).unwrap().outcome,
        CommandOutcome::Accepted
    );
    assert_eq!(durable.managed().risk().reservation_count(), 0);
    let expected_control = durable
        .managed()
        .book()
        .try_account_control_observation(account(11))
        .unwrap();
    let frame_count = frames(file.path()).len();
    durable.close().unwrap();

    let mut recovered =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles(), options()).unwrap();
    let control = recovered
        .managed()
        .book()
        .try_account_control_observation(account(11))
        .unwrap();
    assert_eq!(control, expected_control);
    assert_eq!(control.account_id(), account(11));
    assert_eq!(control.state(), AccountAdmissionState::Blocked);
    assert_eq!(control.revision(), 1);
    assert_eq!(recovered.managed().risk().reservation_count(), 0);
    assert!(recovered.submit(block).unwrap().replayed);
    assert_eq!(frames(file.path()).len(), frame_count);
    recovered.managed().validate().unwrap();
}

#[test]
fn report_event_capacity_failure_precedes_durable_risk_command_append() {
    let file = TestFile::new("report-events");
    let limits = OrderBookLimits::new(OrderBookLimitsSpec {
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
    let mut durable = DurableRiskOrderBook::open_with_limits(
        file.path(),
        definition(),
        &profiles(),
        managed_limits(limits, 2),
        options(),
    )
    .unwrap();
    let Command::New(mut maker) = command(1, 1, 11, 1) else {
        unreachable!("command helper constructs new orders")
    };
    maker.side = Side::Sell;
    durable.submit(Command::New(maker)).unwrap();
    let frame_count = frames(file.path()).len();

    assert!(matches!(
        durable.submit(command(2, 2, 12, 2)),
        Err(DurableRiskError::Matching(
            MatchingError::CapacityExhausted(MatchingCapacity::ReportEvents)
        ))
    ));
    assert_eq!(frames(file.path()).len(), frame_count);
    assert_eq!(durable.managed().book().active_order_count(), 1);
    assert!(
        durable
            .managed()
            .risk()
            .reservation(OrderId::new(2).unwrap())
            .is_none()
    );
    durable.close().unwrap();
}

#[test]
fn durable_risk_replays_sequenced_risk_rejections() {
    let file = TestFile::new("rejection");
    let profile = [AccountRiskDefinition::new(
        account(11),
        risk_profile(AccountRiskState::Active, 5),
    )];
    let mut durable =
        DurableRiskOrderBook::open(file.path(), definition(), &profile, options()).unwrap();
    let report = durable.submit(command(1, 1, 11, 6)).unwrap();
    assert_eq!(
        report.outcome,
        CommandOutcome::Rejected(RejectReason::RiskOrderQuantityLimit)
    );
    drop(durable);

    let reopened =
        DurableRiskOrderBook::open(file.path(), definition(), &profile, options()).unwrap();
    assert_eq!(reopened.recovery().replayed_commands, 1);
    assert_eq!(reopened.managed().book().active_order_count(), 0);
    assert_eq!(reopened.managed().risk().reservation_count(), 0);
}

#[test]
fn recovery_completes_profile_prefix_and_dangling_command() {
    let profile_values = profiles();
    let mut canonical = profile_values;
    canonical.sort_unstable_by_key(|value| value.account_id());

    let prefix_file = TestFile::new("profile-prefix");
    let mut prefix_journal = Journal::open(prefix_file.path(), options()).unwrap();
    prefix_journal.append(&definition()).unwrap();
    prefix_journal.append(&canonical[0]).unwrap();
    drop(prefix_journal);
    let recovered =
        DurableRiskOrderBook::open(prefix_file.path(), definition(), &profiles(), options())
            .unwrap();
    assert_eq!(recovered.recovery().completed_profile_records, 1);
    assert_eq!(frames(prefix_file.path()).len(), 3);

    let dangling_file = TestFile::new("dangling-command");
    let mut dangling_journal = Journal::open(dangling_file.path(), options()).unwrap();
    dangling_journal.append(&definition()).unwrap();
    for profile in &canonical {
        dangling_journal.append(profile).unwrap();
    }
    dangling_journal.append(&command(1, 1, 11, 5)).unwrap();
    drop(dangling_journal);
    let recovered =
        DurableRiskOrderBook::open(dangling_file.path(), definition(), &profiles(), options())
            .unwrap();
    assert!(recovered.recovery().completed_dangling_command);
    assert_eq!(frames(dangling_file.path()).len(), 5);
    assert_eq!(recovered.managed().book().active_order_count(), 1);
    recovered.managed().validate().unwrap();
}

#[test]
fn recovery_completes_a_dangling_account_control_without_exposure_drift() {
    let file = TestFile::new("dangling-account-control");
    let mut canonical = profiles();
    canonical.sort_unstable_by_key(|value| value.account_id());
    let resting = command(1, 1, 11, 5);
    let block = account_control(2, 11, 0, AccountControlAction::BlockAndCancel);
    let mut direct = RiskManagedOrderBook::new(definition());
    for profile in &canonical {
        direct
            .register_account(profile.account_id(), profile.profile())
            .unwrap();
    }
    let resting_report = direct.submit(resting).unwrap();

    let mut journal = Journal::open(file.path(), options()).unwrap();
    journal.append(&definition()).unwrap();
    for profile in &canonical {
        journal.append(profile).unwrap();
    }
    journal.append(&resting).unwrap();
    journal.append(&resting_report).unwrap();
    journal.append(&block).unwrap();
    drop(journal);

    let recovered =
        DurableRiskOrderBook::open(file.path(), definition(), &profiles(), options()).unwrap();
    assert!(recovered.recovery().completed_dangling_command);
    assert_eq!(frames(file.path()).len(), 7);
    assert_eq!(recovered.managed().book().active_order_count(), 0);
    assert_eq!(recovered.managed().risk().reservation_count(), 0);
    assert_eq!(
        recovered
            .managed()
            .book()
            .account_control(account(11))
            .revision(),
        1
    );
    recovered.managed().validate().unwrap();
}

#[test]
fn profile_drift_and_risk_report_divergence_are_rejected() {
    let mismatch_file = TestFile::new("profile-mismatch");
    let active = [AccountRiskDefinition::new(
        account(11),
        risk_profile(AccountRiskState::Active, 10),
    )];
    let blocked = [AccountRiskDefinition::new(
        account(11),
        risk_profile(AccountRiskState::Blocked, 10),
    )];
    let initialized =
        DurableRiskOrderBook::open(mismatch_file.path(), definition(), &active, options()).unwrap();
    drop(initialized);
    assert!(matches!(
        DurableRiskOrderBook::open(mismatch_file.path(), definition(), &blocked, options()),
        Err(DurableRiskError::RiskProfileSetMismatch {
            requested_count: 1,
            persisted_count: 1
        })
    ));

    let divergence_file = TestFile::new("divergence");
    let value = command(1, 1, 11, 5);
    let mut blocked_book = RiskManagedOrderBook::new(definition());
    blocked_book
        .register_account(account(11), blocked[0].profile())
        .unwrap();
    let different_report = blocked_book.submit(value).unwrap();
    let mut journal = Journal::open(divergence_file.path(), options()).unwrap();
    journal.append(&definition()).unwrap();
    journal.append(&active[0]).unwrap();
    journal.append(&value).unwrap();
    journal.append(&different_report).unwrap();
    drop(journal);
    assert!(matches!(
        DurableRiskOrderBook::open(divergence_file.path(), definition(), &active, options()),
        Err(DurableRiskError::ReplayDivergence {
            command_sequence: 3,
            report_sequence: 4
        })
    ));
}
