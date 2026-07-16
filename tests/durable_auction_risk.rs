use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::auction::{AuctionAllocationPolicy, AuctionOrderConstraint, AuctionPricePolicy};
use quotick::auction_book::{
    CallAuctionOrder, CallAuctionRemainderPolicy, CallAuctionSelfTradePolicy,
    CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionCommand, CallAuctionCommandOutcome, CallAuctionEngineLimits, CallAuctionPhase,
    CallAuctionPhaseControl, CallAuctionRejectReason, CallAuctionSubmitOrder,
    CallAuctionUncrossCommand,
};
use quotick::auction_risk::{
    CallAuctionRiskCheckpointCapture, CallAuctionRiskLimits, CallAuctionRiskLimitsSpec,
    CallAuctionRiskManagedEngine,
};
use quotick::durable_auction::{
    DurableCallAuctionEngine, DurableCallAuctionError, DurableCallAuctionRiskCheckpointCapture,
    DurableCallAuctionRiskEngine, VerifiedDurableCallAuctionRiskCheckpoint,
};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{
    Durability, Journal, JournalOptions, JournalReader, RecordKind, SegmentedJournalOptions,
    SegmentedJournalReader,
};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskLimitSpec, RiskLimits, RiskProfile,
};
use quotick::snapshot::{CheckpointSlot, SnapshotFile, SnapshotKind, SnapshotOptions};
use quotick::{
    AccountId, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, Side, TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestArea(PathBuf);

impl TestArea {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-durable-auction-risk-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestArea {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn instrument() -> InstrumentId {
    InstrumentId::new(81).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(3).unwrap()
}

fn auction() -> AuctionId {
    AuctionId::new(1).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("DUR-AUCT-RISK").unwrap(),
        kind: InstrumentKind::Future,
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

fn profile(max_order_quantity_lots: u64) -> RiskProfile {
    RiskProfile::new(
        AccountRiskState::Active,
        0,
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots,
            max_order_notional: 200_000,
            max_open_orders: 32,
            max_open_quantity_lots: 32_000,
            max_open_notional: 6_400_000,
            max_long_position_lots: 32_000,
            max_short_position_lots: 32_000,
        })
        .unwrap(),
    )
    .unwrap()
}

fn profiles() -> [AccountRiskDefinition; 2] {
    [
        AccountRiskDefinition::new(account(2), profile(1_000)),
        AccountRiskDefinition::new(account(1), profile(1_000)),
    ]
}

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn phase(command_id: u64, revision: u64, target: CallAuctionPhase) -> CallAuctionCommand {
    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(),
        expected_phase_revision: revision,
        target_phase: target,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn order(
    order_id: u64,
    owner: u64,
    side: Side,
    constraint: AuctionOrderConstraint,
    lots: u64,
) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        account(owner),
        instrument(),
        version(),
        side,
        constraint,
        Quantity::new(lots).unwrap(),
    )
}

fn submit(command_id: u64, order: CallAuctionOrder) -> CallAuctionCommand {
    CallAuctionCommand::Submit(CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(),
        expected_phase_revision: 1,
        order,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn uncross(command_id: u64, remainder: CallAuctionRemainderPolicy) -> CallAuctionCommand {
    CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        auction_id: auction(),
        expected_phase_revision: 2,
        price_band: quotick::auction::AuctionPriceBand::new(
            quotick::auction::AuctionPriceGrid::new(5).unwrap(),
            Price::from_raw(-100),
            Price::from_raw(200),
        )
        .unwrap(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        uncross_policy: CallAuctionUncrossPolicy::new(
            AuctionAllocationPolicy::PriceTime,
            remainder,
            CallAuctionSelfTradePolicy::Permit,
        ),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn frame_count(path: &Path) -> usize {
    JournalReader::open(path, options())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .len()
}

#[test]
fn staged_coupled_capture_verifies_old_phase_risk_state_after_uncross_suffix() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<CallAuctionRiskCheckpointCapture>();
    let mut managed = CallAuctionRiskManagedEngine::try_new(definition()).unwrap();
    for value in profiles() {
        managed
            .register_account(value.account_id(), value.profile())
            .unwrap();
    }
    let commands = [
        phase(1, 0, CallAuctionPhase::Collecting),
        submit(2, order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 6)),
        submit(
            3,
            order(
                2,
                2,
                Side::Sell,
                AuctionOrderConstraint::Limit(Price::from_raw(100)),
                5,
            ),
        ),
        submit(
            4,
            order(3, 99, Side::Buy, AuctionOrderConstraint::Market, 1),
        ),
        phase(5, 1, CallAuctionPhase::Frozen),
    ];
    for command in commands {
        managed.submit(command).unwrap();
    }
    let capture = managed.capture_checkpoint_candidate(1, 3, 13).unwrap();
    let shared = capture.clone();
    assert!(capture.shares_checkpoint_storage_with(&shared));
    assert_eq!(capture.account_count(), 2);
    assert_eq!(capture.command_count(), 5);
    assert_eq!(capture.active_order_count(), 2);
    let synchronous = managed.checkpoint(1, 3, 13).unwrap();
    let worker = std::thread::spawn(move || capture.verify().unwrap());
    managed
        .submit(uncross(6, CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    let verified = worker.join().unwrap();

    assert_eq!(verified, synchronous);
    let restored = CallAuctionRiskManagedEngine::from_checkpoint(&verified).unwrap();
    assert_eq!(restored.risk().reservation_count(), 2);
    assert_eq!(
        restored
            .risk()
            .snapshot(account(1))
            .unwrap()
            .position_lots(),
        0
    );
    assert_eq!(
        managed.risk().snapshot(account(1)).unwrap().position_lots(),
        5
    );
    assert_eq!(
        managed.risk().snapshot(account(2)).unwrap().position_lots(),
        -5
    );
    assert_eq!(managed.risk().reservation_count(), 1);
}

#[test]
fn single_file_recovery_reproduces_risk_rejections_positions_and_reservations() {
    let area = TestArea::new("round-trip");
    let wal = area.join("auction-risk.wal");
    let profile_values = profiles();
    let missing = submit(
        4,
        order(3, 99, Side::Buy, AuctionOrderConstraint::Market, 1),
    );
    let final_uncross = uncross(6, CallAuctionRemainderPolicy::RetainAll);

    let mut durable =
        DurableCallAuctionRiskEngine::open(&wal, definition(), &profile_values, options()).unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable
        .submit(submit(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 6),
        ))
        .unwrap();
    durable
        .submit(submit(
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
    assert_eq!(
        durable.submit(missing).unwrap().outcome,
        CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::RiskProfileMissing)
    );
    durable
        .submit(phase(5, 1, CallAuctionPhase::Frozen))
        .unwrap();
    durable.submit(final_uncross).unwrap();
    durable.close().unwrap();

    assert_eq!(frame_count(&wal), 15);
    let mut recovered =
        DurableCallAuctionRiskEngine::open(&wal, definition(), &profile_values, options()).unwrap();
    assert_eq!(recovered.recovery().replayed_commands, 6);
    assert_eq!(recovered.recovery().checkpointed_commands, 0);
    assert_eq!(
        recovered
            .managed()
            .risk()
            .snapshot(account(1))
            .unwrap()
            .position_lots(),
        5
    );
    assert_eq!(
        recovered
            .managed()
            .risk()
            .snapshot(account(2))
            .unwrap()
            .position_lots(),
        -5
    );
    let reservation = recovered
        .managed()
        .risk()
        .reservation(OrderId::new(1).unwrap())
        .unwrap();
    assert_eq!(reservation.quantity_lots(), 1);
    assert_eq!(reservation.notional(), 200);
    assert!(recovered.submit(missing).unwrap().replayed);
    assert!(recovered.submit(final_uncross).unwrap().replayed);
    assert_eq!(frame_count(&wal), 15);
    recovered.managed().validate().unwrap();
    recovered.close().unwrap();
}

#[test]
fn checkpoint_is_snapshot_kind_five_and_replays_only_the_suffix() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<DurableCallAuctionRiskCheckpointCapture>();
    assert_send_sync::<VerifiedDurableCallAuctionRiskCheckpoint>();
    let area = TestArea::new("checkpoint");
    let wal = area.join("auction-risk.wal");
    let snapshot = area.join("auction-risk.qsnp");
    let profile_values = profiles();
    let mut durable =
        DurableCallAuctionRiskEngine::open(&wal, definition(), &profile_values, options()).unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable
        .submit(submit(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    let capture = durable.capture_checkpoint_candidate().unwrap();
    let shared = capture.clone();
    assert!(capture.shares_checkpoint_storage_with(&shared));
    assert_eq!(capture.wal_first_sequence(), 1);
    assert_eq!(capture.wal_metadata_sequence(), 3);
    assert_eq!(capture.generation(), 7);
    let worker = std::thread::spawn(move || capture.verify().unwrap());
    durable
        .submit(submit(
            3,
            order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    let verified = worker.join().unwrap();
    let receipt = durable
        .write_verified_checkpoint(&snapshot, &verified, SnapshotOptions::default())
        .unwrap();
    assert_eq!(receipt.generation(), 7);
    durable.close().unwrap();

    let bytes = fs::read(&snapshot).unwrap();
    assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), 13);
    assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), 5);
    let mut recovered = DurableCallAuctionRiskEngine::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        &profile_values,
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.managed().risk().reservation_count(), 2);
    assert!(
        recovered
            .submit(submit(
                3,
                order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 2)
            ))
            .unwrap()
            .replayed
    );
    recovered.close().unwrap();

    let drifted = [
        AccountRiskDefinition::new(account(1), profile(1)),
        AccountRiskDefinition::new(account(2), profile(1_000)),
    ];
    assert!(matches!(
        DurableCallAuctionRiskEngine::open_with_checkpoint(
            &wal,
            &snapshot,
            definition(),
            &drifted,
            options(),
            SnapshotOptions::default(),
        ),
        Err(DurableCallAuctionError::RiskProfileSetMismatch { .. }
            | DurableCallAuctionError::CheckpointRiskProfileSetMismatch)
    ));
}

#[test]
fn verified_coupled_auction_checkpoint_rejects_other_reopen_and_cutover_epoch() {
    let area = TestArea::new("staged-fences");
    let source_wal = area.join("source.wal");
    let other_wal = area.join("other.wal");
    let other_output = area.join("other.qsnp");
    let reopen_output = area.join("reopen.qsnp");
    let stale_output = area.join("stale.qsnp");
    let cutover_base = area.join("cutover.qsnp");
    let profile_values = profiles();
    let mut source =
        DurableCallAuctionRiskEngine::open(&source_wal, definition(), &profile_values, options())
            .unwrap();
    source
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    let verified = source
        .capture_checkpoint_candidate()
        .unwrap()
        .verify()
        .unwrap();

    let mut other =
        DurableCallAuctionRiskEngine::open(&other_wal, definition(), &profile_values, options())
            .unwrap();
    assert!(matches!(
        other.write_verified_checkpoint(&other_output, &verified, SnapshotOptions::default()),
        Err(DurableCallAuctionError::CheckpointCaptureOriginMismatch)
    ));
    assert!(!other_output.exists());
    assert!(!other.is_poisoned());
    other.close().unwrap();

    source.close().unwrap();
    let mut reopened =
        DurableCallAuctionRiskEngine::open(&source_wal, definition(), &profile_values, options())
            .unwrap();
    assert!(matches!(
        reopened.write_verified_checkpoint(&reopen_output, &verified, SnapshotOptions::default()),
        Err(DurableCallAuctionError::CheckpointCaptureOriginMismatch)
    ));
    assert!(!reopen_output.exists());
    let stale = reopened
        .capture_checkpoint_candidate()
        .unwrap()
        .verify()
        .unwrap();
    reopened
        .compact_to_checkpoint(&cutover_base, SnapshotOptions::default())
        .unwrap();
    assert!(matches!(
        reopened.write_verified_checkpoint(&stale_output, &stale, SnapshotOptions::default()),
        Err(DurableCallAuctionError::CheckpointCaptureStale {
            captured_epoch: 0,
            current_epoch: 1
        })
    ));
    assert!(!stale_output.exists());
    assert!(!reopened.is_poisoned());
    reopened.close().unwrap();
}

#[test]
fn segmented_verified_coupled_cutover_replays_only_rotated_suffix() {
    let area = TestArea::new("segmented-staged");
    let segments = area.join("segments");
    let snapshot = area.join("auction-risk.qsnp");
    let profile_values = profiles();
    let segmented_options = SegmentedJournalOptions {
        maximum_segment_bytes: 256,
        journal: options(),
    };
    let mut durable = DurableCallAuctionRiskEngine::open_segmented(
        &segments,
        definition(),
        &profile_values,
        segmented_options,
    )
    .unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable
        .submit(submit(
            2,
            order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    let capture = durable.capture_checkpoint_candidate().unwrap();
    let worker = std::thread::spawn(move || capture.verify().unwrap());
    durable
        .submit(submit(
            3,
            order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 2),
        ))
        .unwrap();
    durable
        .submit(phase(4, 1, CallAuctionPhase::Frozen))
        .unwrap();
    durable
        .submit(uncross(5, CallAuctionRemainderPolicy::RetainAll))
        .unwrap();
    let verified = worker.join().unwrap();
    let cutover = durable
        .compact_verified_checkpoint(&snapshot, &verified, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.snapshot().generation(), 7);
    assert_eq!(cutover.wal_first_sequence(), 7);
    durable.close().unwrap();

    let frames = SegmentedJournalReader::open(&segments, segmented_options)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames.len(), 7);
    assert_eq!(frames[0].kind(), RecordKind::CheckpointAnchor);
    assert_eq!(frames[0].sequence(), 7);
    for (index, frame) in frames[1..].iter().enumerate() {
        let sequence = u64::try_from(index).unwrap() + 8;
        assert_eq!(frame.sequence(), sequence);
        assert_eq!(
            frame.kind(),
            if sequence % 2 == 0 {
                RecordKind::CallAuctionCommand
            } else {
                RecordKind::CallAuctionExecutionReport
            }
        );
    }

    let recovered = DurableCallAuctionRiskEngine::open_segmented_with_checkpoint(
        &segments,
        &snapshot,
        definition(),
        &profile_values,
        segmented_options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 3);
    assert_eq!(recovered.managed().risk().reservation_count(), 0);
    assert_eq!(
        recovered
            .managed()
            .risk()
            .snapshot(account(1))
            .unwrap()
            .position_lots(),
        2
    );
    assert_eq!(
        recovered
            .managed()
            .risk()
            .snapshot(account(2))
            .unwrap()
            .position_lots(),
        -2
    );
    recovered.managed().validate().unwrap();
}

#[test]
fn single_file_cutover_binds_kind_five_and_completes_a_dangling_risk_command() {
    let area = TestArea::new("cutover");
    let wal = area.join("auction-risk.wal");
    let checkpoint_base = area.join("auction-risk.qsnp");
    let profile_values = profiles();
    let dangling = submit(
        2,
        order(1, 99, Side::Buy, AuctionOrderConstraint::Market, 1),
    );
    let mut durable =
        DurableCallAuctionRiskEngine::open(&wal, definition(), &profile_values, options()).unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    let cutover = durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    assert_eq!(cutover.snapshot().generation(), 5);
    durable.close().unwrap();

    let anchor_frame = JournalReader::open(&wal, options())
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(anchor_frame.kind(), RecordKind::CheckpointAnchor);
    let anchor: quotick::snapshot::CheckpointAnchor = anchor_frame.decode().unwrap();
    assert_eq!(anchor.kind(), SnapshotKind::CallAuctionRiskCheckpoint);
    assert!(matches!(
        DurableCallAuctionRiskEngine::open(&wal, definition(), &profile_values, options()),
        Err(DurableCallAuctionError::CheckpointRequiredForCompactedWal { sequence: 5 })
    ));

    let selected = SnapshotFile::slot_path(&checkpoint_base, CheckpointSlot::A);
    let original = fs::read(&selected).unwrap();
    let mut corrupt = original.clone();
    *corrupt.last_mut().unwrap() ^= 0x80;
    fs::write(&selected, corrupt).unwrap();
    assert!(matches!(
        DurableCallAuctionRiskEngine::open_with_checkpoint(
            &wal,
            &checkpoint_base,
            definition(),
            &profile_values,
            options(),
            SnapshotOptions::default(),
        ),
        Err(DurableCallAuctionError::Snapshot(_))
    ));
    fs::write(&selected, original).unwrap();

    let mut raw = Journal::open(&wal, options()).unwrap();
    raw.append(&dangling).unwrap();
    raw.close().unwrap();
    let mut recovered = DurableCallAuctionRiskEngine::open_with_checkpoint(
        &wal,
        &checkpoint_base,
        definition(),
        &profile_values,
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert!(recovered.recovery().completed_dangling_command);
    let replay = recovered.submit(dangling).unwrap();
    assert!(replay.replayed);
    assert_eq!(
        replay.outcome,
        CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::RiskProfileMissing)
    );
    let second = recovered
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(second.slot(), CheckpointSlot::B);
    recovered.close().unwrap();
}

#[test]
fn recovery_rejects_a_dangling_persisted_retry_without_appending_a_report() {
    let area = TestArea::new("dangling-retry");
    let wal = area.join("auction-risk.wal");
    let profile_values = profiles();
    let command = phase(1, 0, CallAuctionPhase::Collecting);
    let mut durable =
        DurableCallAuctionRiskEngine::open(&wal, definition(), &profile_values, options()).unwrap();
    durable.submit(command).unwrap();
    durable.close().unwrap();

    let mut raw = Journal::open(&wal, options()).unwrap();
    raw.append(&command).unwrap();
    raw.close().unwrap();
    assert!(matches!(
        DurableCallAuctionRiskEngine::open(&wal, definition(), &profile_values, options()),
        Err(DurableCallAuctionError::PersistedDanglingRetry {
            command_sequence: 6
        })
    ));
    assert_eq!(frame_count(&wal), 6);
}

#[test]
fn profile_capacity_failure_precedes_wal_creation() {
    let area = TestArea::new("profile-capacity");
    let wal = area.join("auction-risk.wal");
    let profile_values = profiles();
    let limits = CallAuctionRiskLimits::new(CallAuctionRiskLimitsSpec {
        auction: CallAuctionEngineLimits::default(),
        max_registered_accounts: 1,
    })
    .unwrap();

    assert!(matches!(
        DurableCallAuctionRiskEngine::open_with_limits(
            &wal,
            definition(),
            &profile_values,
            limits,
            options(),
        ),
        Err(DurableCallAuctionError::Risk(
            quotick::risk::RiskError::ProfileCapacityExhausted { maximum: 1 }
        ))
    ));
    assert!(!wal.exists());
}

#[test]
fn segmented_cutover_replays_profile_bound_suffix_without_cross_generation_mixing() {
    let area = TestArea::new("segmented");
    let segments = area.join("segments");
    let checkpoint_base = area.join("auction-risk.qsnp");
    let profile_values = profiles();
    let segmented_options = SegmentedJournalOptions {
        maximum_segment_bytes: 256,
        journal: options(),
    };
    let first = phase(1, 0, CallAuctionPhase::Collecting);
    let second = submit(2, order(1, 1, Side::Buy, AuctionOrderConstraint::Market, 2));
    let suffix = submit(
        3,
        order(2, 2, Side::Sell, AuctionOrderConstraint::Market, 2),
    );
    let mut durable = DurableCallAuctionRiskEngine::open_segmented(
        &segments,
        definition(),
        &profile_values,
        segmented_options,
    )
    .unwrap();
    durable.submit(first).unwrap();
    durable.submit(second).unwrap();
    durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    durable.submit(suffix).unwrap();
    durable.close().unwrap();

    let frames = SegmentedJournalReader::open(&segments, segmented_options)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames[0].kind(), RecordKind::CheckpointAnchor);
    assert_eq!(frames.len(), 3);
    let recovered = DurableCallAuctionRiskEngine::open_segmented_with_checkpoint(
        &segments,
        &checkpoint_base,
        definition(),
        &profile_values,
        segmented_options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.managed().risk().reservation_count(), 2);
    recovered.managed().validate().unwrap();
    recovered.close().unwrap();

    let selected = SnapshotFile::slot_path(&checkpoint_base, CheckpointSlot::A);
    assert!(selected.exists());
}

#[test]
fn recovery_completes_only_an_exact_canonical_profile_prefix() {
    let area = TestArea::new("profile-prefix");
    let wal = area.join("auction-risk.wal");
    let profile_values = profiles();
    let canonical_first = AccountRiskDefinition::new(account(1), profile(1_000));
    let mut raw = Journal::open(&wal, options()).unwrap();
    raw.append(&definition()).unwrap();
    raw.append(&canonical_first).unwrap();
    raw.close().unwrap();

    let mut recovered =
        DurableCallAuctionRiskEngine::open(&wal, definition(), &profile_values, options()).unwrap();
    assert_eq!(recovered.recovery().completed_profile_records, 1);
    recovered
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    recovered.close().unwrap();

    let frames = JournalReader::open(&wal, options())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames[0].kind(), RecordKind::InstrumentDefinition);
    assert_eq!(frames[1].kind(), RecordKind::AccountRiskDefinition);
    assert_eq!(frames[2].kind(), RecordKind::AccountRiskDefinition);
    assert_eq!(
        frames[1]
            .decode::<AccountRiskDefinition>()
            .unwrap()
            .account_id(),
        account(1)
    );
    assert_eq!(
        frames[2]
            .decode::<AccountRiskDefinition>()
            .unwrap()
            .account_id(),
        account(2)
    );
}

#[test]
fn recovery_rejects_a_valid_report_from_a_different_risk_profile_set() {
    let area = TestArea::new("risk-divergence");
    let wal = area.join("auction-risk.wal");
    let requested_profiles = profiles();
    let phase_command = phase(1, 0, CallAuctionPhase::Collecting);
    let missing_command = submit(
        2,
        order(1, 99, Side::Buy, AuctionOrderConstraint::Market, 1),
    );

    let mut foreign = CallAuctionRiskManagedEngine::try_new(definition()).unwrap();
    for value in requested_profiles {
        foreign
            .register_account(value.account_id(), value.profile())
            .unwrap();
    }
    foreign
        .register_account(account(99), profile(1_000))
        .unwrap();
    let phase_report = foreign.submit(phase_command).unwrap();
    let incompatible_report = foreign.submit(missing_command).unwrap();
    assert_eq!(
        incompatible_report.outcome,
        CallAuctionCommandOutcome::Accepted
    );

    let mut raw = Journal::open(&wal, options()).unwrap();
    raw.append(&definition()).unwrap();
    raw.append(&AccountRiskDefinition::new(account(1), profile(1_000)))
        .unwrap();
    raw.append(&AccountRiskDefinition::new(account(2), profile(1_000)))
        .unwrap();
    raw.append(&phase_command).unwrap();
    raw.append(&phase_report).unwrap();
    raw.append(&missing_command).unwrap();
    raw.append(&incompatible_report).unwrap();
    raw.close().unwrap();

    assert!(matches!(
        DurableCallAuctionRiskEngine::open(&wal, definition(), &requested_profiles, options()),
        Err(DurableCallAuctionError::ReplayDivergence {
            command_sequence: 6,
            report_sequence: 7
        })
    ));
    assert!(matches!(
        DurableCallAuctionEngine::open(&wal, definition(), options()),
        Err(DurableCallAuctionError::UnexpectedRecord {
            sequence: 2,
            kind: RecordKind::AccountRiskDefinition
        })
    ));
}
