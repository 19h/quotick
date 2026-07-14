use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::{BinaryCodec, CodecError};
use quotick::durable_risk::{
    DurableRiskError, DurableRiskManagedCheckpointCapture, DurableRiskOrderBook,
    VerifiedDurableRiskManagedCheckpoint,
};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{
    Durability, Journal, JournalOptions, JournalReader, RecordKind, SegmentedJournal,
    SegmentedJournalOptions, SegmentedJournalReader,
};
use quotick::matching::{
    Command, CommandOutcome, NewOrder, OrderDisplay, OrderType, RejectReason, SelfTradePrevention,
    TimeInForce,
};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedCheckpoint,
    RiskManagedCheckpointCapture, RiskManagedOrderBook, RiskProfile,
};
use quotick::snapshot::{CheckpointSlot, SnapshotFile, SnapshotOptions};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestArea(PathBuf);

impl TestArea {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-risk-checkpoint-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).expect("test area creates");
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

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("RISK-CHECKPOINT").unwrap(),
        kind: InstrumentKind::Future,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 100).unwrap(),
        reserve: ReserveOrderRules::new(100).unwrap(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn profile(max_order_quantity_lots: u64) -> RiskProfile {
    RiskProfile::new(
        AccountRiskState::Active,
        0,
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots,
            max_order_notional: 1_000_000,
            max_open_orders: 100,
            max_open_quantity_lots: 1_000,
            max_open_notional: 1_000_000,
            max_long_position_lots: 1_000,
            max_short_position_lots: 1_000,
        })
        .unwrap(),
    )
    .unwrap()
}

fn profiles(max_order_quantity_lots: u64) -> [AccountRiskDefinition; 2] {
    [
        AccountRiskDefinition::new(account(12), profile(max_order_quantity_lots)),
        AccountRiskDefinition::new(account(11), profile(max_order_quantity_lots)),
    ]
}

#[allow(
    clippy::too_many_arguments,
    reason = "checkpoint fixtures make risk- and priority-affecting fields explicit"
)]
fn order(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    display: OrderDisplay,
    price: i64,
    time_in_force: TimeInForce,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(owner),
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

fn resting(command_id: u64, order_id: u64, owner: u64, quantity: u64) -> Command {
    order(
        command_id,
        order_id,
        owner,
        Side::Buy,
        quantity,
        OrderDisplay::FullyDisplayed,
        90,
        TimeInForce::GoodTilCancelled,
    )
}

fn journal_options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

#[test]
fn staged_risk_capture_is_shared_and_survives_source_suffix_growth() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<RiskManagedCheckpointCapture>();
    let profile_values = profiles(40);
    let reserve = order(
        1,
        1,
        11,
        Side::Sell,
        30,
        OrderDisplay::Reserve {
            peak: Quantity::new(10).unwrap(),
        },
        100,
        TimeInForce::GoodTilCancelled,
    );
    let execution = order(
        2,
        2,
        12,
        Side::Buy,
        10,
        OrderDisplay::FullyDisplayed,
        100,
        TimeInForce::ImmediateOrCancel,
    );
    let rejected = resting(3, 3, 12, 41);
    let suffix = resting(4, 4, 12, 5);
    let mut managed = RiskManagedOrderBook::new(definition());
    for value in profile_values {
        managed
            .register_account(value.account_id(), value.profile())
            .unwrap();
    }
    managed.submit(reserve).unwrap();
    managed.submit(execution).unwrap();
    assert_eq!(
        managed.submit(rejected).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::RiskOrderQuantityLimit)
    );

    let capture = managed.capture_checkpoint_candidate(1, 3, 9).unwrap();
    let shared = capture.clone();
    assert!(capture.shares_checkpoint_storage_with(&shared));
    assert_eq!(capture.account_count(), 2);
    assert_eq!(capture.command_count(), 3);
    assert_eq!(capture.active_order_count(), 1);
    let synchronous = managed.checkpoint(1, 3, 9).unwrap();
    let worker = std::thread::spawn(move || capture.verify().unwrap());
    managed.submit(suffix).unwrap();
    let verified = worker.join().unwrap();

    assert_eq!(verified, synchronous);
    assert_eq!(verified.generation(), 9);
    let restored = RiskManagedOrderBook::from_checkpoint(&verified).unwrap();
    assert_eq!(restored.risk().reservation_count(), 1);
    assert_eq!(
        restored
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        20
    );
    assert_eq!(
        restored
            .risk()
            .snapshot(account(11))
            .unwrap()
            .position_lots(),
        -10
    );
    assert_eq!(
        restored
            .risk()
            .snapshot(account(12))
            .unwrap()
            .position_lots(),
        10
    );
    assert_eq!(managed.book().active_order_count(), 2);
}

#[test]
fn staged_durable_risk_checkpoint_allows_suffix_and_recovers_exact_coupled_state() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<DurableRiskManagedCheckpointCapture>();
    assert_send_sync::<VerifiedDurableRiskManagedCheckpoint>();
    let area = TestArea::new("staged-durable");
    let wal = area.join("risk.wal");
    let snapshot = area.join("risk.qsnp");
    let profile_values = profiles(40);
    let reserve = order(
        1,
        1,
        11,
        Side::Sell,
        30,
        OrderDisplay::Reserve {
            peak: Quantity::new(10).unwrap(),
        },
        100,
        TimeInForce::GoodTilCancelled,
    );
    let execution = order(
        2,
        2,
        12,
        Side::Buy,
        10,
        OrderDisplay::FullyDisplayed,
        100,
        TimeInForce::ImmediateOrCancel,
    );
    let rejected = resting(3, 3, 12, 41);
    let suffix = resting(4, 4, 12, 5);
    let mut durable =
        DurableRiskOrderBook::open(&wal, definition(), &profile_values, journal_options()).unwrap();
    durable.submit(reserve).unwrap();
    durable.submit(execution).unwrap();
    durable.submit(rejected).unwrap();

    let capture = durable.capture_checkpoint_candidate().unwrap();
    let shared = capture.clone();
    assert!(capture.shares_checkpoint_storage_with(&shared));
    assert_eq!(capture.wal_first_sequence(), 1);
    assert_eq!(capture.wal_metadata_sequence(), 3);
    assert_eq!(capture.generation(), 9);
    let worker = std::thread::spawn(move || capture.verify().unwrap());
    durable.submit(suffix).unwrap();
    let verified = worker.join().unwrap();
    assert_eq!(verified.account_count(), 2);
    assert_eq!(verified.command_count(), 3);
    assert_eq!(verified.active_order_count(), 1);
    durable
        .write_verified_checkpoint(&snapshot, &verified, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let recovered = DurableRiskOrderBook::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        &profile_values,
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 3);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.managed().risk().reservation_count(), 2);
    assert_eq!(
        recovered
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        20
    );
    assert_eq!(
        recovered
            .managed()
            .risk()
            .snapshot(account(11))
            .unwrap()
            .position_lots(),
        -10
    );
    recovered.managed().validate().unwrap();
}

#[test]
fn verified_durable_risk_checkpoint_rejects_other_reopen_and_cutover_epoch() {
    let area = TestArea::new("staged-fences");
    let source_wal = area.join("source.wal");
    let other_wal = area.join("other.wal");
    let other_output = area.join("other.qsnp");
    let reopen_output = area.join("reopen.qsnp");
    let stale_output = area.join("stale.qsnp");
    let cutover_base = area.join("cutover.qsnp");
    let profile_values = profiles(40);
    let mut source = DurableRiskOrderBook::open(
        &source_wal,
        definition(),
        &profile_values,
        journal_options(),
    )
    .unwrap();
    source.submit(resting(1, 1, 11, 5)).unwrap();
    let verified = source
        .capture_checkpoint_candidate()
        .unwrap()
        .verify()
        .unwrap();

    let mut other =
        DurableRiskOrderBook::open(&other_wal, definition(), &profile_values, journal_options())
            .unwrap();
    assert!(matches!(
        other.write_verified_checkpoint(&other_output, &verified, SnapshotOptions::default()),
        Err(DurableRiskError::CheckpointCaptureOriginMismatch)
    ));
    assert!(!other_output.exists());
    assert!(!other.is_poisoned());
    other.close().unwrap();

    source.close().unwrap();
    let mut reopened = DurableRiskOrderBook::open(
        &source_wal,
        definition(),
        &profile_values,
        journal_options(),
    )
    .unwrap();
    assert!(matches!(
        reopened.write_verified_checkpoint(&reopen_output, &verified, SnapshotOptions::default()),
        Err(DurableRiskError::CheckpointCaptureOriginMismatch)
    ));
    assert!(!reopen_output.exists());
    assert!(!reopened.is_poisoned());

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
        Err(DurableRiskError::CheckpointCaptureStale {
            captured_epoch: 0,
            current_epoch: 1
        })
    ));
    assert!(!stale_output.exists());
    assert!(!reopened.is_poisoned());
    reopened.close().unwrap();
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one coupled scenario keeps rejection, execution, hidden leaves, retry, and suffix evidence contiguous"
)]
fn risk_checkpoint_preserves_rejections_positions_reserve_exposure_and_suffix_recovery() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<RiskManagedCheckpoint>();
    let area = TestArea::new("round-trip");
    let wal = area.join("risk.wal");
    let snapshot = area.join("risk.qsnp");
    let profile_values = profiles(40);
    let reserve = order(
        1,
        1,
        11,
        Side::Sell,
        30,
        OrderDisplay::Reserve {
            peak: Quantity::new(10).unwrap(),
        },
        100,
        TimeInForce::GoodTilCancelled,
    );
    let execution = order(
        2,
        2,
        12,
        Side::Buy,
        10,
        OrderDisplay::FullyDisplayed,
        100,
        TimeInForce::ImmediateOrCancel,
    );
    let rejected = resting(3, 3, 12, 41);
    let suffix = resting(4, 4, 12, 5);

    let mut durable =
        DurableRiskOrderBook::open(&wal, definition(), &profile_values, journal_options()).unwrap();
    durable.submit(reserve).unwrap();
    durable.submit(execution).unwrap();
    let rejection = durable.submit(rejected).unwrap();
    assert_eq!(
        rejection.outcome,
        CommandOutcome::Rejected(RejectReason::RiskOrderQuantityLimit)
    );
    let receipt = durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    assert_eq!(receipt.generation(), 9);
    let suffix_report = durable.submit(suffix).unwrap();
    durable.close().unwrap();

    let checkpoint: RiskManagedCheckpoint =
        SnapshotFile::read(&snapshot, SnapshotOptions::default()).unwrap();
    assert_eq!(checkpoint.wal_first_sequence(), 1);
    assert_eq!(checkpoint.matching().wal_metadata_sequence(), 3);
    assert_eq!(checkpoint.matching().command_count(), 3);
    let shared = checkpoint.clone();
    assert!(shared.shares_account_storage_with(&checkpoint));
    assert!(
        shared
            .matching()
            .shares_order_storage_with(checkpoint.matching())
    );
    assert!(
        shared
            .matching()
            .shares_history_storage_with(checkpoint.matching())
    );
    drop(checkpoint);
    let mut direct = RiskManagedOrderBook::from_checkpoint(&shared).unwrap();
    assert_eq!(
        direct
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        20
    );
    assert_eq!(
        direct.risk().snapshot(account(11)).unwrap().position_lots(),
        -10
    );
    assert_eq!(
        direct.risk().snapshot(account(12)).unwrap().position_lots(),
        10
    );
    assert_eq!(direct.submit(suffix).unwrap(), suffix_report);
    assert!(direct.submit(rejected).unwrap().replayed);

    let mut recovered = DurableRiskOrderBook::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        &profile_values,
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 3);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(
        recovered
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        20
    );
    assert!(recovered.submit(rejected).unwrap().replayed);
    recovered.managed().validate().unwrap();
}

#[test]
fn risk_checkpoint_has_stable_kind_codec_and_rejects_semantic_corruption_without_panics() {
    let area = TestArea::new("codec");
    let wal = area.join("risk.wal");
    let snapshot = area.join("risk.qsnp");
    let profile_values = profiles(40);
    let mut durable =
        DurableRiskOrderBook::open(&wal, definition(), &profile_values, journal_options()).unwrap();
    durable.submit(resting(1, 1, 11, 5)).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let bytes = fs::read(&snapshot).unwrap();
    assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), 3);
    let checkpoint: RiskManagedCheckpoint =
        SnapshotFile::read(&snapshot, SnapshotOptions::default()).unwrap();
    let encoded = checkpoint.encode().unwrap();
    let decoded = RiskManagedCheckpoint::decode(&encoded).unwrap();
    assert_eq!(decoded, checkpoint);
    assert!(!decoded.shares_account_storage_with(&checkpoint));
    assert!(
        !decoded
            .matching()
            .shares_order_storage_with(checkpoint.matching())
    );
    assert!(
        !decoded
            .matching()
            .shares_history_storage_with(checkpoint.matching())
    );

    let mut invalid_origin = encoded.clone();
    invalid_origin[0..8].copy_from_slice(&2_u64.to_le_bytes());
    assert!(matches!(
        RiskManagedCheckpoint::decode(&invalid_origin),
        Err(CodecError::InvalidRiskCheckpoint(_))
    ));

    let matching_length =
        usize::try_from(u32::from_le_bytes(encoded[8..12].try_into().unwrap())).unwrap();
    let first_account_definition = 12 + matching_length + 4 + 4;
    let mut missing_owner = encoded.clone();
    missing_owner[first_account_definition..first_account_definition + 8]
        .copy_from_slice(&10_u64.to_le_bytes());
    let result = std::panic::catch_unwind(|| RiskManagedCheckpoint::decode(&missing_owner));
    assert!(
        result.is_ok(),
        "malformed checkpoint decoding must not panic"
    );
    assert!(matches!(
        result.unwrap(),
        Err(CodecError::InvalidRiskCheckpoint(_))
    ));

    let account_definition_length = usize::try_from(u32::from_le_bytes(
        encoded[first_account_definition - 4..first_account_definition]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    let exposure = first_account_definition + account_definition_length;
    let mut inconsistent_exposure = encoded;
    inconsistent_exposure[exposure + 16..exposure + 32].copy_from_slice(&999_u128.to_le_bytes());
    assert!(matches!(
        RiskManagedCheckpoint::decode(&inconsistent_exposure),
        Err(CodecError::InvalidRiskCheckpoint(_))
    ));
}

#[test]
fn risk_checkpoint_binds_non_default_origin_profiles_and_same_generation_lineage() {
    let area = TestArea::new("lineage");
    let wal = area.join("risk.wal");
    let snapshot = area.join("risk.qsnp");
    let fork_wal = area.join("fork.wal");
    let fork_snapshot = area.join("fork.qsnp");
    let options = JournalOptions {
        initial_sequence: 10_000,
        ..journal_options()
    };
    let original_profiles = profiles(40);
    let fork_profiles = profiles(41);

    let mut durable =
        DurableRiskOrderBook::open(&wal, definition(), &original_profiles, options).unwrap();
    durable.submit(resting(1, 1, 11, 5)).unwrap();
    let receipt = durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    assert_eq!(receipt.generation(), 10_004);
    durable.close().unwrap();

    let mut fork =
        DurableRiskOrderBook::open(&fork_wal, definition(), &fork_profiles, options).unwrap();
    fork.submit(resting(1, 1, 11, 5)).unwrap();
    fork.write_checkpoint(&fork_snapshot, SnapshotOptions::default())
        .unwrap();
    fork.close().unwrap();
    let fork_checkpoint: RiskManagedCheckpoint =
        SnapshotFile::read(&fork_snapshot, SnapshotOptions::default()).unwrap();
    assert!(matches!(
        SnapshotFile::write(&snapshot, &fork_checkpoint, SnapshotOptions::default()),
        Err(quotick::snapshot::SnapshotError::SameGenerationDivergence { .. })
    ));
    assert!(matches!(
        DurableRiskOrderBook::open_with_checkpoint(
            &wal,
            &fork_snapshot,
            definition(),
            &original_profiles,
            options,
            SnapshotOptions::default(),
        ),
        Err(DurableRiskError::CheckpointRiskProfileSetMismatch)
    ));
}

#[test]
fn risk_checkpoint_must_equal_exact_wal_prefix_and_cannot_be_ahead() {
    let area = TestArea::new("prefix");
    let source_wal = area.join("source.wal");
    let divergent_wal = area.join("divergent.wal");
    let short_wal = area.join("short.wal");
    let snapshot = area.join("risk.qsnp");
    let profile_values = profiles(40);

    let mut source = DurableRiskOrderBook::open(
        &source_wal,
        definition(),
        &profile_values,
        journal_options(),
    )
    .unwrap();
    source.submit(resting(1, 1, 11, 5)).unwrap();
    source.submit(resting(2, 2, 12, 7)).unwrap();
    source
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    source.close().unwrap();

    let mut divergent = DurableRiskOrderBook::open(
        &divergent_wal,
        definition(),
        &profile_values,
        journal_options(),
    )
    .unwrap();
    divergent.submit(resting(1, 1, 11, 6)).unwrap();
    divergent.close().unwrap();
    assert!(matches!(
        DurableRiskOrderBook::open_with_checkpoint(
            &divergent_wal,
            &snapshot,
            definition(),
            &profile_values,
            journal_options(),
            SnapshotOptions::default(),
        ),
        Err(DurableRiskError::CheckpointPrefixDivergence { .. })
    ));

    let mut short =
        DurableRiskOrderBook::open(&short_wal, definition(), &profile_values, journal_options())
            .unwrap();
    short.submit(resting(1, 1, 11, 5)).unwrap();
    short.close().unwrap();
    assert!(matches!(
        DurableRiskOrderBook::open_with_checkpoint(
            &short_wal,
            &snapshot,
            definition(),
            &profile_values,
            journal_options(),
            SnapshotOptions::default(),
        ),
        Err(DurableRiskError::CheckpointAheadOfWal { .. })
    ));
}

#[test]
fn segmented_risk_checkpoint_recovers_across_physical_boundaries() {
    let area = TestArea::new("segmented");
    let segments = area.join("segments");
    let snapshot = area.join("risk.qsnp");
    let profile_values = profiles(40);
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 512,
        journal: journal_options(),
    };
    let mut durable =
        DurableRiskOrderBook::open_segmented(&segments, definition(), &profile_values, options)
            .unwrap();
    durable.submit(resting(1, 1, 11, 5)).unwrap();
    let capture = durable.capture_checkpoint_candidate().unwrap();
    let worker = std::thread::spawn(move || capture.verify().unwrap());
    durable.submit(resting(2, 2, 12, 7)).unwrap();
    let verified = worker.join().unwrap();
    durable
        .write_verified_checkpoint(&snapshot, &verified, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let recovered = DurableRiskOrderBook::open_segmented_with_checkpoint(
        &segments,
        &snapshot,
        definition(),
        &profile_values,
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert!(recovered.recovery().journal.segment_count >= 2);
    assert_eq!(recovered.managed().book().active_order_count(), 2);
}

#[test]
fn risk_checkpoint_output_cannot_alias_wal_storage() {
    let area = TestArea::new("alias");
    let wal = area.join("risk.wal");
    let profile_values = profiles(40);
    let mut durable =
        DurableRiskOrderBook::open(&wal, definition(), &profile_values, journal_options()).unwrap();
    assert!(matches!(
        durable.write_checkpoint(&wal, SnapshotOptions::default()),
        Err(DurableRiskError::CheckpointPathConflictsWithWal { .. })
    ));
    let cutover_pending = Journal::cutover_pending_path(&wal).unwrap();
    assert!(matches!(
        durable.write_checkpoint(&cutover_pending, SnapshotOptions::default()),
        Err(DurableRiskError::CheckpointPathConflictsWithWal { .. })
    ));

    let segments = area.join("segments");
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 1_024,
        journal: journal_options(),
    };
    let mut segmented =
        DurableRiskOrderBook::open_segmented(&segments, definition(), &profile_values, options)
            .unwrap();
    assert!(matches!(
        segmented.write_checkpoint(segments.join("risk.qsnp"), SnapshotOptions::default()),
        Err(DurableRiskError::CheckpointPathConflictsWithWal { .. })
    ));
}

#[test]
fn risk_checkpoint_cutover_preserves_profiles_positions_reservations_and_suffix() {
    let area = TestArea::new("single-cutover");
    let wal = area.join("risk.wal");
    let checkpoint_base = area.join("risk.qsnp");
    let profile_values = profiles(40);
    let first = resting(1, 1, 11, 5);
    let suffix = resting(2, 2, 12, 7);
    let mut durable =
        DurableRiskOrderBook::open(&wal, definition(), &profile_values, journal_options()).unwrap();
    durable.submit(first).unwrap();
    let cutover = durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    assert_eq!(cutover.snapshot().generation(), 5);
    let anchor = JournalReader::open(&wal, journal_options())
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(anchor.kind(), RecordKind::CheckpointAnchor);
    assert_eq!(anchor.sequence(), 5);
    durable.submit(suffix).unwrap();
    durable.close().unwrap();

    assert!(matches!(
        DurableRiskOrderBook::open(&wal, definition(), &profile_values, journal_options()),
        Err(DurableRiskError::CheckpointRequiredForCompactedWal { sequence: 5 })
    ));
    let mut recovered = DurableRiskOrderBook::open_with_checkpoint(
        &wal,
        &checkpoint_base,
        definition(),
        &profile_values,
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.managed().risk().reservation_count(), 2);
    assert!(recovered.submit(first).unwrap().replayed);
    recovered.managed().validate().unwrap();

    let second = recovered
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(second.slot(), CheckpointSlot::B);
    assert_eq!(second.snapshot().generation(), 7);
    recovered.close().unwrap();

    let final_state = DurableRiskOrderBook::open_with_checkpoint(
        &wal,
        &checkpoint_base,
        definition(),
        &profile_values,
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(final_state.recovery().checkpointed_commands, 2);
    assert_eq!(final_state.recovery().replayed_commands, 0);
    assert_eq!(final_state.managed().risk().reservation_count(), 2);
    final_state.managed().validate().unwrap();
}

#[test]
fn segmented_risk_cutover_preserves_metadata_state_and_generation_selection() {
    let area = TestArea::new("segmented-cutover");
    let segments = area.join("segments");
    let checkpoint_base = area.join("risk.qsnp");
    let profile_values = profiles(40);
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 512,
        journal: journal_options(),
    };
    let first = resting(1, 1, 11, 5);
    let suffix = resting(2, 2, 12, 7);
    let mut durable =
        DurableRiskOrderBook::open_segmented(&segments, definition(), &profile_values, options)
            .unwrap();
    durable.submit(first).unwrap();
    let cutover = durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    assert_eq!(cutover.snapshot().generation(), 5);
    durable.submit(suffix).unwrap();
    durable.close().unwrap();

    let frames = SegmentedJournalReader::open(&segments, options)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames[0].kind(), RecordKind::CheckpointAnchor);
    assert_eq!(frames.len(), 3);
    let mut recovered = DurableRiskOrderBook::open_segmented_with_checkpoint(
        &segments,
        &checkpoint_base,
        definition(),
        &profile_values,
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.managed().risk().reservation_count(), 2);
    assert!(recovered.submit(first).unwrap().replayed);
    let second = recovered
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(second.slot(), CheckpointSlot::B);
    recovered.close().unwrap();

    let manager = SegmentedJournal::open(&segments, options).unwrap();
    assert_eq!(manager.recovery().active_generation, 3);
    assert!(
        manager
            .segments()
            .iter()
            .all(|segment| segment.generation() == 3)
    );
    manager.close().unwrap();
    let final_state = DurableRiskOrderBook::open_segmented_with_checkpoint(
        &segments,
        &checkpoint_base,
        definition(),
        &profile_values,
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(final_state.recovery().checkpointed_commands, 2);
    assert_eq!(final_state.recovery().replayed_commands, 0);
    assert_eq!(final_state.managed().risk().reservation_count(), 2);
    final_state.managed().validate().unwrap();
}

#[test]
fn risk_checkpoint_supports_zero_profiles_and_missing_profile_rejections() {
    let area = TestArea::new("zero-profiles");
    let wal = area.join("risk.wal");
    let snapshot = area.join("risk.qsnp");
    let command = resting(1, 1, 11, 5);
    let mut durable =
        DurableRiskOrderBook::open(&wal, definition(), &[], journal_options()).unwrap();
    assert_eq!(
        durable.submit(command).unwrap().outcome,
        CommandOutcome::Rejected(RejectReason::RiskProfileMissing)
    );
    let receipt = durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    assert_eq!(receipt.generation(), 3);
    durable.close().unwrap();

    let checkpoint: RiskManagedCheckpoint =
        SnapshotFile::read(&snapshot, SnapshotOptions::default()).unwrap();
    assert_eq!(checkpoint.matching().wal_metadata_sequence(), 1);
    assert!(checkpoint.accounts().is_empty());
    let recovered = DurableRiskOrderBook::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        &[],
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert_eq!(recovered.managed().book().active_order_count(), 0);
}

#[test]
fn risk_checkpoint_recovery_completes_a_dangling_suffix_command() {
    let area = TestArea::new("dangling-suffix");
    let wal = area.join("risk.wal");
    let snapshot = area.join("risk.qsnp");
    let profile_values = profiles(40);
    let mut durable =
        DurableRiskOrderBook::open(&wal, definition(), &profile_values, journal_options()).unwrap();
    durable.submit(resting(1, 1, 11, 5)).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let suffix = resting(2, 2, 12, 7);
    let mut raw = Journal::open(&wal, journal_options()).unwrap();
    raw.append(&suffix).unwrap();
    raw.close().unwrap();

    let recovered = DurableRiskOrderBook::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        &profile_values,
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert_eq!(recovered.recovery().replayed_commands, 0);
    assert!(recovered.recovery().completed_dangling_command);
    assert_eq!(recovered.managed().book().active_order_count(), 2);
    recovered.managed().validate().unwrap();
}
