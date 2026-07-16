use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::auction::{
    AuctionAllocationPolicy, AuctionOrderConstraint, AuctionPriceBand, AuctionPriceGrid,
    AuctionPricePolicy,
};
use quotick::auction_book::{
    CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder, CallAuctionRemainderPolicy,
    CallAuctionSelfTradePolicy, CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionAmendOrder, CallAuctionCheckpoint, CallAuctionCheckpointCapture, CallAuctionCommand,
    CallAuctionEngine, CallAuctionEngineError, CallAuctionEngineLimits,
    CallAuctionEngineLimitsSpec, CallAuctionEventKind, CallAuctionExecutionReport,
    CallAuctionMassCancel, CallAuctionPhase, CallAuctionPhaseControl, CallAuctionReplaceOrder,
    CallAuctionSubmitOrder, CallAuctionUncrossCommand,
};
use quotick::codec::{BinaryCodec, CodecError};
use quotick::durable_auction::{
    DurableCallAuctionCheckpointCapture, DurableCallAuctionEngine, DurableCallAuctionError,
    VerifiedDurableCallAuctionCheckpoint,
};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{
    Durability, Journal, JournalLayout, JournalOptions, JournalReader, RecordKind, RecoveryMode,
    SegmentedJournalOptions, SegmentedJournalReader,
};
use quotick::matching::MassCancelScope;
use quotick::snapshot::{CheckpointSlot, SnapshotFile, SnapshotOptions};
use quotick::{
    AccountId, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, Side, TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestPath(PathBuf);

impl TestPath {
    fn file(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-durable-auction-{label}-{}-{nonce}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }

    fn directory(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-durable-auction-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn definition() -> InstrumentDefinition {
    definition_with_symbol("DURAUCT")
}

fn definition_with_symbol(symbol: &str) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(71).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new(symbol).unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(0), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Closed,
    })
    .unwrap()
}

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn auction() -> AuctionId {
    AuctionId::new(1).unwrap()
}

fn phase(command_id: u64, revision: u64, target: CallAuctionPhase) -> CallAuctionCommand {
    phase_for(command_id, auction(), revision, target)
}

fn phase_for(
    command_id: u64,
    auction_id: AuctionId,
    revision: u64,
    target: CallAuctionPhase,
) -> CallAuctionCommand {
    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(71).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        auction_id,
        expected_phase_revision: revision,
        target_phase: target,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn order(order_id: u64, account_id: u64, side: Side, lots: u64) -> CallAuctionOrder {
    order_with_class(order_id, account_id, side, lots, 0)
}

fn order_with_class(
    order_id: u64,
    account_id: u64,
    side: Side,
    lots: u64,
    priority_class: u16,
) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        AccountId::new(account_id).unwrap(),
        InstrumentId::new(71).unwrap(),
        InstrumentVersion::new(1).unwrap(),
        side,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        Quantity::new(lots).unwrap(),
        quotick::auction::AuctionPriorityClass::new(priority_class),
    )
}

fn submit(command_id: u64, order: CallAuctionOrder) -> CallAuctionCommand {
    submit_for(command_id, auction(), 1, order)
}

fn replace(
    command_id: u64,
    target_order_id: u64,
    replacement: CallAuctionOrder,
) -> CallAuctionCommand {
    CallAuctionCommand::Replace(CallAuctionReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction(),
        expected_phase_revision: 1,
        account_id: replacement.account_id(),
        target_order_id: OrderId::new(target_order_id).unwrap(),
        replacement,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn mass_cancel(command_id: u64, account_id: u64, scope: MassCancelScope) -> CallAuctionCommand {
    CallAuctionCommand::MassCancel(CallAuctionMassCancel {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(71).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        scope,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn amend(command_id: u64, account_id: u64, order_id: u64, lots: u64) -> CallAuctionCommand {
    CallAuctionCommand::Amend(CallAuctionAmendOrder {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(71).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        auction_id: auction(),
        expected_phase_revision: 1,
        account_id: AccountId::new(account_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        new_quantity: Quantity::new(lots).unwrap(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn submit_for(
    command_id: u64,
    auction_id: AuctionId,
    revision: u64,
    order: CallAuctionOrder,
) -> CallAuctionCommand {
    CallAuctionCommand::Submit(CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id,
        expected_phase_revision: revision,
        order,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn uncross(command_id: u64) -> CallAuctionCommand {
    uncross_for(
        command_id,
        auction(),
        2,
        CallAuctionRemainderPolicy::CancelAll,
    )
}

fn uncross_for(
    command_id: u64,
    auction_id: AuctionId,
    revision: u64,
    remainder: CallAuctionRemainderPolicy,
) -> CallAuctionCommand {
    uncross_for_allocation(
        command_id,
        auction_id,
        revision,
        AuctionAllocationPolicy::PriceTime,
        remainder,
    )
}

fn uncross_for_allocation(
    command_id: u64,
    auction_id: AuctionId,
    revision: u64,
    allocation: AuctionAllocationPolicy,
    remainder: CallAuctionRemainderPolicy,
) -> CallAuctionCommand {
    CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(71).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        auction_id,
        expected_phase_revision: revision,
        price_band: AuctionPriceBand::new(
            AuctionPriceGrid::new(1).unwrap(),
            Price::from_raw(0),
            Price::from_raw(1_000),
        )
        .unwrap(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
        uncross_policy: CallAuctionUncrossPolicy::new(
            allocation,
            remainder,
            CallAuctionSelfTradePolicy::Permit,
        ),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn frame_kinds(path: &Path) -> Vec<RecordKind> {
    JournalReader::open(path, options())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .map(|frame| frame.kind())
        .collect()
}

fn frame_wire_tags(path: &Path) -> Vec<u16> {
    let bytes = fs::read(path).unwrap();
    JournalReader::open(path, options())
        .unwrap()
        .map(|frame| {
            let offset = usize::try_from(frame.unwrap().offset()).unwrap();
            u16::from_le_bytes(bytes[offset + 6..offset + 8].try_into().unwrap())
        })
        .collect()
}

fn trade_triplets(report: &CallAuctionExecutionReport) -> Vec<(u64, u64, u64)> {
    report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            CallAuctionEventKind::Trade(trade) => Some((
                trade.buy_order_id().get(),
                trade.sell_order_id().get(),
                trade.quantity().lots(),
            )),
            _ => None,
        })
        .collect()
}

#[test]
fn staged_auction_capture_is_shared_and_survives_source_suffix_growth() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<CallAuctionCheckpointCapture>();
    let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
    engine
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine.submit(submit(2, order(1, 1, Side::Buy, 5))).unwrap();
    let capture = engine.capture_checkpoint_candidate(1, 5).unwrap();
    let shared = capture.clone();
    assert!(capture.shares_checkpoint_storage_with(&shared));
    assert_eq!(capture.command_count(), 2);
    assert_eq!(capture.active_order_count(), 1);
    assert_eq!(capture.accepted_order_count(), 1);
    let synchronous = engine.checkpoint(1, 5).unwrap();
    let worker = std::thread::spawn(move || capture.verify().unwrap());
    engine
        .submit(submit(3, order(2, 2, Side::Sell, 5)))
        .unwrap();
    let verified = worker.join().unwrap();

    assert_eq!(verified, synchronous);
    assert_eq!(verified.generation(), 5);
    assert_eq!(verified.active_orders().len(), 1);
    assert_eq!(engine.book().active_order_count(), 2);
    let restored = CallAuctionEngine::from_checkpoint(&verified).unwrap();
    assert_eq!(restored.phase_snapshot(), verified.phase());
    assert_eq!(restored.book().active_order_count(), 1);
}

#[test]
fn durable_staged_auction_checkpoint_replays_only_post_capture_suffix() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<DurableCallAuctionCheckpointCapture>();
    assert_send_sync::<VerifiedDurableCallAuctionCheckpoint>();
    let area = TestPath::directory("staged-checkpoint");
    fs::create_dir_all(area.path()).unwrap();
    let wal = area.join("auction.wal");
    let snapshot = area.join("auction.qsnp");
    let mut durable = DurableCallAuctionEngine::open(&wal, definition(), options()).unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable
        .submit(submit(2, order(1, 1, Side::Buy, 5)))
        .unwrap();
    let capture = durable.capture_checkpoint_candidate().unwrap();
    let shared = capture.clone();
    assert!(capture.shares_checkpoint_storage_with(&shared));
    assert_eq!(capture.generation(), 5);
    let worker = std::thread::spawn(move || capture.verify().unwrap());
    durable
        .submit(submit(3, order(2, 2, Side::Sell, 5)))
        .unwrap();
    let verified = worker.join().unwrap();
    durable
        .write_verified_checkpoint(&snapshot, &verified, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let recovered = DurableCallAuctionEngine::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.engine().book().active_order_count(), 2);
    recovered.engine().validate().unwrap();
}

#[test]
fn mass_cancel_recovers_across_checkpoint_and_wal_suffix() {
    let area = TestPath::directory("mass-cancel-recovery");
    fs::create_dir_all(area.path()).unwrap();
    let wal = area.join("auction.wal");
    let snapshot = area.join("auction.qsnp");
    let mut durable = DurableCallAuctionEngine::open(&wal, definition(), options()).unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable
        .submit(submit(2, order(10, 1, Side::Buy, 3)))
        .unwrap();
    durable
        .submit(submit(3, order(20, 1, Side::Sell, 5)))
        .unwrap();
    durable
        .submit(submit(4, order(30, 2, Side::Buy, 7)))
        .unwrap();
    durable
        .submit(mass_cancel(5, 1, MassCancelScope::Side(Side::Buy)))
        .unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    let suffix = mass_cancel(6, 1, MassCancelScope::All);
    durable.submit(suffix).unwrap();
    durable.close().unwrap();

    let mut recovered = DurableCallAuctionEngine::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.engine().book().active_order_count(), 1);
    assert!(
        recovered
            .engine()
            .book()
            .order(OrderId::new(30).unwrap())
            .is_some()
    );
    let replay = recovered.submit(suffix).unwrap();
    assert!(replay.replayed);
    recovered.engine().validate().unwrap();
    recovered.close().unwrap();
}

#[test]
fn amendment_recovers_across_checkpoint_and_wal_suffix_with_exact_retry() {
    let area = TestPath::directory("amend-recovery");
    fs::create_dir_all(area.path()).unwrap();
    let wal = area.join("auction.wal");
    let snapshot = area.join("auction.qsnp");
    let mut durable = DurableCallAuctionEngine::open(&wal, definition(), options()).unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable
        .submit(submit(2, order_with_class(10, 7, Side::Buy, 8, 9)))
        .unwrap();
    durable.submit(amend(3, 7, 10, 5)).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    let suffix = amend(4, 7, 10, 3);
    durable.submit(suffix).unwrap();
    durable.close().unwrap();

    let mut recovered = DurableCallAuctionEngine::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 3);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    let order = recovered
        .engine()
        .book()
        .order(OrderId::new(10).unwrap())
        .unwrap();
    assert_eq!(order.quantity.lots(), 3);
    assert_eq!(
        order.priority_class,
        quotick::auction::AuctionPriorityClass::new(9)
    );
    assert_eq!(order.priority_sequence, 1);
    assert_eq!(recovered.engine().book().state_revision(), 3);
    assert!(recovered.submit(suffix).unwrap().replayed);
    recovered.engine().validate().unwrap();
    recovered.close().unwrap();
}

#[test]
fn verified_auction_checkpoint_rejects_other_reopen_and_cutover_epoch() {
    let area = TestPath::directory("staged-fences");
    fs::create_dir_all(area.path()).unwrap();
    let source_wal = area.join("source.wal");
    let other_wal = area.join("other.wal");
    let other_output = area.join("other.qsnp");
    let reopen_output = area.join("reopen.qsnp");
    let stale_output = area.join("stale.qsnp");
    let cutover_base = area.join("cutover.qsnp");
    let mut source = DurableCallAuctionEngine::open(&source_wal, definition(), options()).unwrap();
    source
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    let verified = source
        .capture_checkpoint_candidate()
        .unwrap()
        .verify()
        .unwrap();

    let mut other = DurableCallAuctionEngine::open(&other_wal, definition(), options()).unwrap();
    assert!(matches!(
        other.write_verified_checkpoint(&other_output, &verified, SnapshotOptions::default()),
        Err(DurableCallAuctionError::CheckpointCaptureOriginMismatch)
    ));
    assert!(!other_output.exists());
    assert!(!other.is_poisoned());
    other.close().unwrap();

    source.close().unwrap();
    let mut reopened =
        DurableCallAuctionEngine::open(&source_wal, definition(), options()).unwrap();
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
fn segmented_verified_auction_cutover_recovers_the_rotated_suffix() {
    let area = TestPath::directory("segmented-staged");
    fs::create_dir_all(area.path()).unwrap();
    let segments = area.join("segments");
    let snapshot = area.join("auction.qsnp");
    let segmented_options = SegmentedJournalOptions {
        maximum_segment_bytes: 512,
        journal: options(),
    };
    let mut durable =
        DurableCallAuctionEngine::open_segmented(&segments, definition(), segmented_options)
            .unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable
        .submit(submit(2, order(1, 1, Side::Buy, 5)))
        .unwrap();
    let capture = durable.capture_checkpoint_candidate().unwrap();
    let worker = std::thread::spawn(move || capture.verify().unwrap());
    durable
        .submit(submit(3, order(2, 2, Side::Sell, 5)))
        .unwrap();
    durable
        .submit(phase(4, 1, CallAuctionPhase::Frozen))
        .unwrap();
    durable.submit(uncross(5)).unwrap();
    let verified = worker.join().unwrap();
    let cutover = durable
        .compact_verified_checkpoint(&snapshot, &verified, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.snapshot().generation(), 5);
    assert_eq!(cutover.wal_first_sequence(), 5);
    durable.close().unwrap();

    let frames = SegmentedJournalReader::open(&segments, segmented_options)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames.len(), 7);
    assert_eq!(frames[0].kind(), RecordKind::CheckpointAnchor);
    assert_eq!(frames[0].sequence(), 5);
    for (index, frame) in frames[1..].iter().enumerate() {
        let sequence = u64::try_from(index).unwrap() + 6;
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

    let recovered = DurableCallAuctionEngine::open_segmented_with_checkpoint(
        &segments,
        &snapshot,
        definition(),
        segmented_options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 3);
    assert_eq!(
        recovered.engine().phase_snapshot().phase(),
        CallAuctionPhase::Closed
    );
    assert_eq!(recovered.engine().book().active_order_count(), 0);
    assert_eq!(recovered.engine().book().next_trade_id(), 2);
}

#[test]
fn durable_auction_round_trip_recovers_phase_book_trades_and_exact_retry() {
    let file = TestPath::file("round-trip");
    let mut durable = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    let commands = [
        phase(1, 0, CallAuctionPhase::Collecting),
        submit(2, order(1, 1, Side::Buy, 5)),
        submit(3, order(2, 2, Side::Sell, 5)),
        phase(4, 1, CallAuctionPhase::Frozen),
        uncross(5),
    ];
    let mut last_report = None;
    for command in commands {
        last_report = Some(durable.submit(command).unwrap());
    }
    assert_eq!(
        durable.engine().phase_snapshot().phase(),
        CallAuctionPhase::Closed
    );
    assert_eq!(durable.engine().book().active_order_count(), 0);
    assert_eq!(durable.engine().book().next_trade_id(), 2);
    let frames_before_retry = frame_kinds(file.path());
    let retry = durable.submit(uncross(5)).unwrap();
    assert!(retry.replayed);
    assert_eq!(
        retry.command_sequence,
        last_report.unwrap().command_sequence
    );
    assert_eq!(frame_kinds(file.path()), frames_before_retry);
    assert_eq!(frames_before_retry.len(), 11);
    assert_eq!(frames_before_retry[0], RecordKind::InstrumentDefinition);
    assert!(frames_before_retry[1..].chunks_exact(2).all(|pair| {
        pair == [
            RecordKind::CallAuctionCommand,
            RecordKind::CallAuctionExecutionReport,
        ]
    }));
    durable.close().unwrap();

    assert_eq!(
        frame_wire_tags(file.path()),
        [4_u16]
            .into_iter()
            .chain([9_u16, 10_u16].into_iter().cycle().take(10))
            .collect::<Vec<_>>()
    );

    let mut reopened =
        DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    assert_eq!(reopened.recovery().replayed_commands, 5);
    assert!(!reopened.recovery().completed_dangling_command);
    assert_eq!(
        reopened.engine().phase_snapshot().phase(),
        CallAuctionPhase::Closed
    );
    assert_eq!(reopened.engine().book().active_order_count(), 0);
    assert_eq!(reopened.engine().book().next_trade_id(), 2);
    reopened.engine().validate().unwrap();
    let reopened_frames = frame_kinds(file.path());
    let reopened_retry = reopened.submit(uncross(5)).unwrap();
    assert!(reopened_retry.replayed);
    assert_eq!(frame_kinds(file.path()), reopened_frames);
    reopened.close().unwrap();
}

#[test]
fn pro_rata_uncross_recovers_identically_from_wal_and_snapshot_history() {
    let area = TestPath::directory("pro-rata-recovery");
    fs::create_dir_all(area.path()).unwrap();
    let wal = area.join("auction.wal");
    let snapshot = area.join("auction.qsnp");
    let mut durable = DurableCallAuctionEngine::open(&wal, definition(), options()).unwrap();
    for command in [
        phase(1, 0, CallAuctionPhase::Collecting),
        submit(2, order_with_class(1, 1, Side::Buy, 100, 1)),
        submit(3, order_with_class(2, 2, Side::Buy, 10, 0)),
        submit(4, order_with_class(3, 3, Side::Buy, 20, 0)),
        submit(5, order_with_class(4, 4, Side::Sell, 15, 0)),
        phase(6, 1, CallAuctionPhase::Frozen),
    ] {
        durable.submit(command).unwrap();
    }
    let command = uncross_for_allocation(
        7,
        auction(),
        2,
        AuctionAllocationPolicy::ProRataTime,
        CallAuctionRemainderPolicy::CancelAll,
    );
    let report = durable.submit(command).unwrap();
    assert_eq!(trade_triplets(&report), vec![(2, 4, 5), (3, 4, 10)]);
    let verified = durable
        .capture_checkpoint_candidate()
        .unwrap()
        .verify()
        .unwrap();
    durable
        .write_verified_checkpoint(&snapshot, &verified, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let mut wal_recovered = DurableCallAuctionEngine::open(&wal, definition(), options()).unwrap();
    let wal_replay = wal_recovered.submit(command).unwrap();
    assert!(wal_replay.replayed);
    assert_eq!(trade_triplets(&wal_replay), vec![(2, 4, 5), (3, 4, 10)]);
    wal_recovered.close().unwrap();

    let mut snapshot_recovered = DurableCallAuctionEngine::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(snapshot_recovered.recovery().checkpointed_commands, 7);
    assert_eq!(snapshot_recovered.recovery().replayed_commands, 0);
    let snapshot_replay = snapshot_recovered.submit(command).unwrap();
    assert!(snapshot_replay.replayed);
    assert_eq!(
        trade_triplets(&snapshot_replay),
        vec![(2, 4, 5), (3, 4, 10)]
    );
    snapshot_recovered.engine().validate().unwrap();
    snapshot_recovered.close().unwrap();
}

#[test]
fn durable_cancel_replace_recovers_new_identity_priority_and_exact_retry() {
    let file = TestPath::file("replace");
    let mut durable = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable
        .submit(submit(2, order(1, 7, Side::Buy, 5)))
        .unwrap();
    let command = replace(3, 1, order(2, 7, Side::Sell, 3));
    let report = durable.submit(command).unwrap();
    assert_eq!(report.events.len(), 2);
    assert!(
        durable
            .engine()
            .book()
            .order(OrderId::new(1).unwrap())
            .is_none()
    );
    assert_eq!(
        durable
            .engine()
            .book()
            .order(OrderId::new(2).unwrap())
            .unwrap()
            .priority_sequence,
        2
    );
    let frame_count = frame_kinds(file.path()).len();
    assert!(durable.submit(command).unwrap().replayed);
    assert_eq!(frame_kinds(file.path()).len(), frame_count);
    durable.close().unwrap();

    let mut reopened =
        DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    assert_eq!(reopened.recovery().replayed_commands, 3);
    assert!(
        reopened
            .engine()
            .book()
            .order(OrderId::new(1).unwrap())
            .is_none()
    );
    assert_eq!(
        reopened
            .engine()
            .book()
            .order(OrderId::new(2).unwrap())
            .unwrap()
            .quantity
            .lots(),
        3
    );
    assert!(reopened.submit(command).unwrap().replayed);
    reopened.engine().validate().unwrap();
    reopened.close().unwrap();
}

#[test]
fn durable_auction_completes_a_dangling_command_and_repairs_a_torn_report() {
    let file = TestPath::file("torn-report");
    let mut durable = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable.close().unwrap();
    let length = fs::metadata(file.path()).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(file.path())
        .unwrap()
        .set_len(length - 5)
        .unwrap();

    let repaired_options = JournalOptions {
        recovery: RecoveryMode::RepairTornTail,
        ..options()
    };
    let repaired =
        DurableCallAuctionEngine::open(file.path(), definition(), repaired_options).unwrap();
    assert!(repaired.recovery().journal.truncated_bytes > 0);
    assert!(repaired.recovery().completed_dangling_command);
    assert_eq!(
        repaired.engine().phase_snapshot().phase(),
        CallAuctionPhase::Collecting
    );
    assert_eq!(frame_kinds(file.path()).len(), 3);
    repaired.close().unwrap();
}

#[test]
fn durable_auction_rejects_report_divergence_and_definition_drift() {
    let divergence = TestPath::file("divergence");
    let command = phase(1, 0, CallAuctionPhase::Collecting);
    let mut model = CallAuctionEngine::try_new(definition()).unwrap();
    let mut bad_report = model.submit(command).unwrap();
    bad_report.command_sequence = 99;
    let mut journal = Journal::open(divergence.path(), options()).unwrap();
    journal.append(&definition()).unwrap();
    journal.append(&command).unwrap();
    journal.append(&bad_report).unwrap();
    journal.close().unwrap();
    assert!(matches!(
        DurableCallAuctionEngine::open(divergence.path(), definition(), options()),
        Err(DurableCallAuctionError::ReplayDivergence {
            command_sequence: 2,
            report_sequence: 3
        })
    ));

    let drift = TestPath::file("definition-drift");
    DurableCallAuctionEngine::open(drift.path(), definition(), options())
        .unwrap()
        .close()
        .unwrap();
    assert!(matches!(
        DurableCallAuctionEngine::open(
            drift.path(),
            definition_with_symbol("DIFFERENT"),
            options()
        ),
        Err(DurableCallAuctionError::DefinitionMismatch { .. })
    ));
}

#[test]
fn durable_auction_rejects_a_noncanonical_persisted_retry() {
    let file = TestPath::file("persisted-retry");
    let command = phase(1, 0, CallAuctionPhase::Collecting);
    let mut model = CallAuctionEngine::try_new(definition()).unwrap();
    let original = model.submit(command).unwrap();
    let replay = model.submit(command).unwrap();
    assert!(!original.replayed);
    assert!(replay.replayed);

    let mut journal = Journal::open(file.path(), options()).unwrap();
    journal.append(&definition()).unwrap();
    journal.append(&command).unwrap();
    journal.append(&original).unwrap();
    journal.append(&command).unwrap();
    journal.append(&replay).unwrap();
    journal.close().unwrap();

    assert!(matches!(
        DurableCallAuctionEngine::open(file.path(), definition(), options()),
        Err(DurableCallAuctionError::PersistedRetry {
            command_sequence: 4,
            report_sequence: 5
        })
    ));
}

#[test]
fn durable_auction_rejects_a_dangling_noncanonical_persisted_retry() {
    let file = TestPath::file("dangling-persisted-retry");
    let command = phase(1, 0, CallAuctionPhase::Collecting);
    let mut model = CallAuctionEngine::try_new(definition()).unwrap();
    let original = model.submit(command).unwrap();

    let mut journal = Journal::open(file.path(), options()).unwrap();
    journal.append(&definition()).unwrap();
    journal.append(&command).unwrap();
    journal.append(&original).unwrap();
    journal.append(&command).unwrap();
    journal.close().unwrap();

    assert!(matches!(
        DurableCallAuctionEngine::open(file.path(), definition(), options()),
        Err(DurableCallAuctionError::PersistedDanglingRetry {
            command_sequence: 4
        })
    ));
    assert_eq!(frame_kinds(file.path()).len(), 4);
}

#[test]
fn durable_auction_rejects_invalid_record_grammar() {
    let consecutive = TestPath::file("consecutive-commands");
    let mut journal = Journal::open(consecutive.path(), options()).unwrap();
    journal.append(&definition()).unwrap();
    journal
        .append(&phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    journal
        .append(&phase(2, 0, CallAuctionPhase::Collecting))
        .unwrap();
    journal.close().unwrap();
    assert!(matches!(
        DurableCallAuctionEngine::open(consecutive.path(), definition(), options()),
        Err(DurableCallAuctionError::ConsecutiveCommands {
            pending_sequence: 2,
            next_sequence: 3
        })
    ));

    let orphan = TestPath::file("orphan-report");
    let mut model = CallAuctionEngine::try_new(definition()).unwrap();
    let report = model
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    let mut journal = Journal::open(orphan.path(), options()).unwrap();
    journal.append(&definition()).unwrap();
    journal.append(&report).unwrap();
    journal.close().unwrap();
    assert!(matches!(
        DurableCallAuctionEngine::open(orphan.path(), definition(), options()),
        Err(DurableCallAuctionError::ReportWithoutCommand { sequence: 2 })
    ));

    let unexpected = TestPath::file("unexpected-definition");
    let mut journal = Journal::open(unexpected.path(), options()).unwrap();
    journal.append(&definition()).unwrap();
    journal.append(&definition()).unwrap();
    journal.close().unwrap();
    assert!(matches!(
        DurableCallAuctionEngine::open(unexpected.path(), definition(), options()),
        Err(DurableCallAuctionError::UnexpectedRecord {
            sequence: 2,
            kind: RecordKind::InstrumentDefinition
        })
    ));
}

#[test]
fn durable_auction_collision_and_operational_failure_precede_wal_growth() {
    let file = TestPath::file("preflight");
    let mut durable = DurableCallAuctionEngine::open(file.path(), definition(), options()).unwrap();
    let accepted = phase(1, 0, CallAuctionPhase::Collecting);
    durable.submit(accepted).unwrap();
    let frames = frame_kinds(file.path());
    assert!(matches!(
        durable.submit(phase(1, 1, CallAuctionPhase::Frozen)),
        Err(DurableCallAuctionError::Engine(
            CallAuctionEngineError::CommandIdCollision(_)
        ))
    ));
    assert_eq!(frame_kinds(file.path()), frames);
    durable.close().unwrap();
}

#[test]
fn segmented_durable_auction_rotates_and_replays_one_global_sequence() {
    let directory = TestPath::directory("segmented");
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 256,
        journal: options(),
    };
    let mut durable =
        DurableCallAuctionEngine::open_segmented(directory.path(), definition(), options).unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable
        .submit(phase(2, 1, CallAuctionPhase::Closed))
        .unwrap();
    durable.close().unwrap();

    let reopened =
        DurableCallAuctionEngine::open_segmented(directory.path(), definition(), options).unwrap();
    assert_eq!(reopened.recovery().journal.layout, JournalLayout::Segmented);
    assert!(reopened.recovery().journal.segment_count >= 2);
    assert_eq!(reopened.recovery().replayed_commands, 2);
    assert_eq!(
        reopened.engine().phase_snapshot().phase(),
        CallAuctionPhase::Closed
    );
    reopened.close().unwrap();
}

#[test]
fn auction_checkpoint_has_stable_kind_codec_and_direct_restore() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<CallAuctionCheckpoint>();
    let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
    let commands = [
        phase(1, 0, CallAuctionPhase::Collecting),
        submit(2, order(1, 1, Side::Buy, 5)),
        submit(3, order(2, 2, Side::Sell, 5)),
        phase(4, 1, CallAuctionPhase::Frozen),
        uncross(5),
    ];
    for command in commands {
        engine.submit(command).unwrap();
    }
    let checkpoint = engine.checkpoint(1, 11).unwrap();
    assert_eq!(checkpoint.command_count(), 5);
    assert_eq!(checkpoint.accepted_order_ids().len(), 2);
    assert_eq!(checkpoint.next_trade_id(), 2);

    let shared = checkpoint.clone();
    assert!(shared.shares_accepted_order_storage_with(&checkpoint));
    assert!(shared.shares_active_order_storage_with(&checkpoint));
    assert!(shared.shares_history_storage_with(&checkpoint));
    assert!(
        shared.history()[0]
            .report()
            .events
            .shares_storage_with(&checkpoint.history()[0].report().events)
    );
    drop(checkpoint);

    let encoded = shared.encode().unwrap();
    let decoded = CallAuctionCheckpoint::decode(&encoded).unwrap();
    assert_eq!(decoded, shared);
    assert!(!decoded.shares_accepted_order_storage_with(&shared));
    assert!(!decoded.shares_active_order_storage_with(&shared));
    assert!(!decoded.shares_history_storage_with(&shared));
    let mut invalid_boundary = encoded;
    invalid_boundary[8..16].copy_from_slice(&12_u64.to_le_bytes());
    assert!(matches!(
        CallAuctionCheckpoint::decode(&invalid_boundary),
        Err(CodecError::InvalidAuctionCheckpoint(_))
    ));

    let mut restored = CallAuctionEngine::from_checkpoint(&shared).unwrap();
    assert_eq!(restored.phase_snapshot(), engine.phase_snapshot());
    assert_eq!(restored.book().active_order_count(), 0);
    assert_eq!(restored.book().next_trade_id(), 2);
    assert!(restored.submit(uncross(5)).unwrap().replayed);
    restored.validate().unwrap();

    let area = TestPath::directory("checkpoint-envelope");
    fs::create_dir_all(area.path()).unwrap();
    let snapshot = area.join("auction.qsnp");
    SnapshotFile::write(&snapshot, &shared, SnapshotOptions::default()).unwrap();
    let bytes = fs::read(snapshot).unwrap();
    assert_eq!(&bytes[0..4], b"QSNP");
    assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), 14);
    assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), 4);
}

#[test]
fn auction_checkpoint_projection_is_independent_of_order_identity_order() {
    let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
    for command in [
        phase(1, 0, CallAuctionPhase::Collecting),
        submit(2, order(30, 1, Side::Buy, 5)),
        submit(3, order(10, 2, Side::Buy, 6)),
        submit(4, order(20, 3, Side::Buy, 7)),
    ] {
        engine.submit(command).unwrap();
    }

    let checkpoint = engine.checkpoint(1, 9).unwrap();
    assert_eq!(
        checkpoint
            .active_orders()
            .iter()
            .map(|order| order.order_id.get())
            .collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
    let restored = CallAuctionEngine::from_checkpoint(&checkpoint).unwrap();
    assert_eq!(restored.book().active_order_count(), 3);
    restored.validate().unwrap();
}

#[test]
fn auction_checkpoint_projects_remainders_relative_to_each_uncross_cycle() {
    let first_auction = AuctionId::new(1).unwrap();
    let second_auction = AuctionId::new(2).unwrap();
    let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
    let commands = [
        phase_for(1, first_auction, 0, CallAuctionPhase::Collecting),
        submit_for(2, first_auction, 1, order(1, 1, Side::Buy, 10)),
        submit_for(3, first_auction, 1, order(2, 2, Side::Sell, 4)),
        phase_for(4, first_auction, 1, CallAuctionPhase::Frozen),
        uncross_for(5, first_auction, 2, CallAuctionRemainderPolicy::RetainAll),
        phase_for(6, second_auction, 3, CallAuctionPhase::Collecting),
        submit_for(7, second_auction, 4, order(3, 3, Side::Sell, 2)),
        phase_for(8, second_auction, 4, CallAuctionPhase::Frozen),
        uncross_for(9, second_auction, 5, CallAuctionRemainderPolicy::CancelAll),
    ];
    for command in commands {
        engine.submit(command).unwrap();
    }
    assert_eq!(engine.book().active_order_count(), 0);
    assert_eq!(engine.book().next_trade_id(), 3);
    let checkpoint = engine.checkpoint(1, 19).unwrap();
    let restored = CallAuctionEngine::from_checkpoint(&checkpoint).unwrap();
    assert_eq!(restored.phase_snapshot(), engine.phase_snapshot());
    assert_eq!(restored.book().next_trade_id(), 3);
    restored.validate().unwrap();
}

#[test]
fn auction_checkpoint_restore_enforces_current_resource_limits() {
    let book_limits = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: 2,
        max_price_levels_per_side: 2,
        max_accepted_order_ids: 4,
        max_prepared_uncrosses: 2,
    })
    .unwrap();
    let limits = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book: book_limits,
        max_retained_commands: 8,
        terminal_command_reserve: 4,
        max_retained_events: 14,
        max_report_events: 5,
    })
    .unwrap();
    let mut engine = CallAuctionEngine::try_with_limits(definition(), limits).unwrap();
    engine
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    engine.submit(submit(2, order(1, 1, Side::Buy, 5))).unwrap();
    engine
        .submit(submit(3, order(2, 2, Side::Sell, 5)))
        .unwrap();
    let checkpoint = engine.checkpoint(1, 7).unwrap();

    let smaller_book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: 1,
        max_price_levels_per_side: 1,
        max_accepted_order_ids: 4,
        max_prepared_uncrosses: 2,
    })
    .unwrap();
    let smaller = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book: smaller_book,
        max_retained_commands: 8,
        terminal_command_reserve: 3,
        max_retained_events: 12,
        max_report_events: 3,
    })
    .unwrap();
    let error = CallAuctionEngine::from_checkpoint_with_limits(&checkpoint, smaller).unwrap_err();
    assert_eq!(
        error.detail(),
        "auction checkpoint active orders exceed selected capacity"
    );
}

#[test]
fn auction_checkpoint_replays_only_suffix_and_rejects_prefix_forks_and_ahead_state() {
    let area = TestPath::directory("checkpoint-prefix");
    fs::create_dir_all(area.path()).unwrap();
    let wal = area.join("source.wal");
    let snapshot = area.join("auction.qsnp");
    let divergent_wal = area.join("divergent.wal");
    let short_wal = area.join("short.wal");

    let first = phase(1, 0, CallAuctionPhase::Collecting);
    let second = submit(2, order(1, 1, Side::Buy, 5));
    let suffix = submit(3, order(2, 2, Side::Sell, 5));
    let mut source = DurableCallAuctionEngine::open(&wal, definition(), options()).unwrap();
    source.submit(first).unwrap();
    source.submit(second).unwrap();
    source
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    source.submit(suffix).unwrap();
    source.close().unwrap();

    let mut recovered = DurableCallAuctionEngine::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.engine().book().active_order_count(), 2);
    let before_retry = frame_kinds(&wal);
    assert!(recovered.submit(second).unwrap().replayed);
    assert_eq!(frame_kinds(&wal), before_retry);
    recovered.close().unwrap();

    let mut divergent =
        DurableCallAuctionEngine::open(&divergent_wal, definition(), options()).unwrap();
    divergent.submit(first).unwrap();
    divergent
        .submit(submit(2, order(1, 1, Side::Buy, 6)))
        .unwrap();
    divergent.close().unwrap();
    assert!(matches!(
        DurableCallAuctionEngine::open_with_checkpoint(
            &divergent_wal,
            &snapshot,
            definition(),
            options(),
            SnapshotOptions::default(),
        ),
        Err(DurableCallAuctionError::CheckpointPrefixDivergence { .. })
    ));

    let mut short = DurableCallAuctionEngine::open(&short_wal, definition(), options()).unwrap();
    short.submit(first).unwrap();
    short.close().unwrap();
    assert!(matches!(
        DurableCallAuctionEngine::open_with_checkpoint(
            &short_wal,
            &snapshot,
            definition(),
            options(),
            SnapshotOptions::default(),
        ),
        Err(DurableCallAuctionError::CheckpointAheadOfWal { .. })
    ));
}

#[test]
fn single_file_auction_cutover_switches_slots_and_completes_a_dangling_suffix() {
    let area = TestPath::directory("checkpoint-cutover");
    fs::create_dir_all(area.path()).unwrap();
    let wal = area.join("auction.wal");
    let checkpoint_base = area.join("auction.qsnp");
    let first = phase(1, 0, CallAuctionPhase::Collecting);
    let dangling = submit(2, order(1, 1, Side::Buy, 5));

    let mut durable = DurableCallAuctionEngine::open(&wal, definition(), options()).unwrap();
    assert!(matches!(
        durable.write_checkpoint(&wal, SnapshotOptions::default()),
        Err(DurableCallAuctionError::CheckpointPathConflictsWithWal { .. })
    ));
    durable.submit(first).unwrap();
    let cutover = durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    assert_eq!(cutover.snapshot().generation(), 3);
    assert_eq!(cutover.wal_first_sequence(), 3);
    durable.close().unwrap();

    let anchor_frame = JournalReader::open(&wal, options())
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(anchor_frame.kind(), RecordKind::CheckpointAnchor);
    let anchor: quotick::snapshot::CheckpointAnchor = anchor_frame.decode().unwrap();
    assert_eq!(
        anchor.kind(),
        quotick::snapshot::SnapshotKind::CallAuctionCheckpoint
    );
    assert!(matches!(
        DurableCallAuctionEngine::open(&wal, definition(), options()),
        Err(DurableCallAuctionError::CheckpointRequiredForCompactedWal { sequence: 3 })
    ));

    let mut raw = Journal::open(&wal, options()).unwrap();
    raw.append(&dangling).unwrap();
    raw.close().unwrap();
    let mut recovered = DurableCallAuctionEngine::open_with_checkpoint(
        &wal,
        &checkpoint_base,
        definition(),
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert_eq!(recovered.recovery().replayed_commands, 0);
    assert!(recovered.recovery().completed_dangling_command);
    assert_eq!(recovered.engine().book().active_order_count(), 1);
    assert!(recovered.submit(dangling).unwrap().replayed);

    let second = recovered
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(second.slot(), CheckpointSlot::B);
    assert_eq!(second.snapshot().generation(), 5);
    recovered.close().unwrap();

    let final_state = DurableCallAuctionEngine::open_with_checkpoint(
        &wal,
        &checkpoint_base,
        definition(),
        options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(final_state.recovery().checkpointed_commands, 2);
    assert_eq!(final_state.recovery().replayed_commands, 0);
    assert_eq!(final_state.engine().book().active_order_count(), 1);
    final_state.close().unwrap();
}

#[test]
fn compacted_auction_wal_rejects_corrupt_and_wrong_checkpoint_bytes() {
    let area = TestPath::directory("checkpoint-anchor-binding");
    fs::create_dir_all(area.path()).unwrap();
    let wal = area.join("auction.wal");
    let checkpoint_base = area.join("auction.qsnp");
    let mut durable = DurableCallAuctionEngine::open(&wal, definition(), options()).unwrap();
    durable
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let slot = SnapshotFile::slot_path(&checkpoint_base, CheckpointSlot::A);
    let original = fs::read(&slot).unwrap();
    let mut corrupt = original.clone();
    *corrupt.last_mut().unwrap() ^= 0x80;
    fs::write(&slot, corrupt).unwrap();
    assert!(matches!(
        DurableCallAuctionEngine::open_with_checkpoint(
            &wal,
            &checkpoint_base,
            definition(),
            options(),
            SnapshotOptions::default(),
        ),
        Err(DurableCallAuctionError::Snapshot(_))
    ));

    fs::write(&slot, original).unwrap();
    let fork_wal = area.join("fork.wal");
    let fork_snapshot = area.join("fork.qsnp");
    let mut fork = DurableCallAuctionEngine::open(&fork_wal, definition(), options()).unwrap();
    fork.submit(phase(1, 0, CallAuctionPhase::Closed)).unwrap();
    fork.write_checkpoint(&fork_snapshot, SnapshotOptions::default())
        .unwrap();
    fork.close().unwrap();
    fs::copy(&fork_snapshot, &slot).unwrap();
    assert!(matches!(
        DurableCallAuctionEngine::open_with_checkpoint(
            &wal,
            &checkpoint_base,
            definition(),
            options(),
            SnapshotOptions::default(),
        ),
        Err(DurableCallAuctionError::CheckpointAnchorMismatch {
            generation: 3,
            slot: CheckpointSlot::A
        })
    ));
}

#[test]
fn segmented_auction_cutover_replays_suffix_and_rejects_wal_aliases() {
    let area = TestPath::directory("segmented-checkpoint-cutover");
    fs::create_dir_all(area.path()).unwrap();
    let segments = area.join("segments");
    let checkpoint_base = area.join("auction.qsnp");
    let segmented_options = SegmentedJournalOptions {
        maximum_segment_bytes: 256,
        journal: options(),
    };
    let first = phase(1, 0, CallAuctionPhase::Collecting);
    let second = submit(2, order(1, 1, Side::Buy, 5));
    let suffix = submit(3, order(2, 2, Side::Sell, 5));
    let mut durable =
        DurableCallAuctionEngine::open_segmented(&segments, definition(), segmented_options)
            .unwrap();
    durable.submit(first).unwrap();
    durable.submit(second).unwrap();
    let cutover = durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    assert_eq!(cutover.snapshot().generation(), 5);
    durable.submit(suffix).unwrap();
    durable.close().unwrap();

    let frames = SegmentedJournalReader::open(&segments, segmented_options)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames[0].kind(), RecordKind::CheckpointAnchor);
    assert_eq!(frames[0].sequence(), 5);
    assert_eq!(frames.len(), 3);

    let recovered = DurableCallAuctionEngine::open_segmented_with_checkpoint(
        &segments,
        &checkpoint_base,
        definition(),
        segmented_options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.engine().book().active_order_count(), 2);
    recovered.close().unwrap();

    let mut alias = DurableCallAuctionEngine::open_segmented(
        area.join("alias-segments"),
        definition(),
        segmented_options,
    )
    .unwrap();
    assert!(matches!(
        alias.write_checkpoint(
            area.join("alias-segments/auction.qsnp"),
            SnapshotOptions::default()
        ),
        Err(DurableCallAuctionError::CheckpointPathConflictsWithWal { .. })
    ));
    alias.close().unwrap();
}
