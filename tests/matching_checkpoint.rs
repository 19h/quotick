use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::{BinaryCodec, CodecError};
use quotick::durable::{
    DurableError, DurableOrderBook, DurableOrderBookCheckpointCapture,
    VerifiedDurableOrderBookCheckpoint,
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
    AccountAdmissionState, AccountControl, AccountControlAction, CancelReason, Command, EventKind,
    NewOrder, OrderBook, OrderBookCheckpoint, OrderBookCheckpointCapture, OrderDisplay, OrderType,
    ReplaceOrder, SelfTradePrevention, TimeInForce,
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
            "quotick-matching-checkpoint-{label}-{}-{nonce}",
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

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("MATCHING-CHECKPOINT").unwrap(),
        kind: InstrumentKind::Future,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 10_000).unwrap(),
        reserve: ReserveOrderRules::new(100).unwrap(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "checkpoint fixtures make every priority-affecting field explicit"
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

fn resting(command_id: u64, order_id: u64, quantity: u64) -> Command {
    order(
        command_id,
        order_id,
        command_id + 100,
        Side::Buy,
        quantity,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(90)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    )
}

fn account_control(
    command_id: u64,
    account_id: u64,
    expected_revision: u64,
    action: AccountControlAction,
) -> Command {
    Command::AccountControl(AccountControl {
        command_id: CommandId::new(command_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        expected_revision,
        action,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

#[test]
fn immutable_matching_checkpoint_clones_share_row_and_event_storage() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<OrderBookCheckpoint>();

    let command = resting(1, 1, 5);
    let mut book = OrderBook::new(definition());
    let report = book.submit(command).unwrap();
    let checkpoint = book.checkpoint(1, 3).unwrap();

    assert_eq!(checkpoint.active_order_count(), 1);
    assert_eq!(checkpoint.command_count(), 1);
    assert!(
        checkpoint.history()[0]
            .report()
            .events
            .shares_storage_with(&report.events)
    );

    let clone = checkpoint.clone();
    assert!(clone.shares_order_storage_with(&checkpoint));
    assert!(clone.shares_history_storage_with(&checkpoint));
    assert!(
        clone.history()[0]
            .report()
            .events
            .shares_storage_with(&checkpoint.history()[0].report().events)
    );

    drop(checkpoint);
    drop(report);
    drop(book);
    let mut restored = OrderBook::from_checkpoint(&clone).unwrap();
    let retry = restored.submit(command).unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.events, clone.history()[0].report().events);
}

#[test]
fn staged_matching_capture_verifies_off_thread_at_its_exact_boundary() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<OrderBookCheckpointCapture>();

    let ask = order(
        1,
        1,
        11,
        Side::Sell,
        5,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let take = order(
        2,
        2,
        12,
        Side::Buy,
        5,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    );
    let block = account_control(4, 103, 0, AccountControlAction::BlockAndCancel);
    let suffix = resting(6, 6, 9);
    let mut book = OrderBook::new(definition());
    for command in [ask, take, resting(3, 3, 7), block, resting(5, 5, 8)] {
        book.submit(command).unwrap();
    }

    let capture = book.capture_checkpoint_candidate(1, 11).unwrap();
    let shared_capture = capture.clone();
    assert_eq!(capture.wal_metadata_sequence(), 1);
    assert_eq!(capture.generation(), 11);
    assert_eq!(capture.definition(), definition());
    assert_eq!(capture.limits(), book.limits());
    assert_eq!(capture.command_count(), 5);
    assert_eq!(capture.active_order_count(), 1);
    assert!(capture.shares_checkpoint_storage_with(&shared_capture));

    let synchronous = book.checkpoint(1, 11).unwrap();
    let expected_suffix = book.submit(suffix).unwrap();
    let verified = std::thread::spawn(move || shared_capture.verify().unwrap())
        .join()
        .unwrap();
    let independently_verified = capture.verify().unwrap();

    assert_eq!(verified, synchronous);
    assert_eq!(verified.encode().unwrap(), synchronous.encode().unwrap());
    assert!(verified.shares_order_storage_with(&independently_verified));
    assert!(verified.shares_history_storage_with(&independently_verified));
    let mut restored = OrderBook::from_checkpoint(&verified).unwrap();
    assert_eq!(restored.submit(suffix).unwrap(), expected_suffix);
}

#[test]
fn durable_staged_checkpoint_publishes_an_exact_prefix_after_suffix_growth() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<DurableOrderBookCheckpointCapture>();
    assert_send_sync::<VerifiedDurableOrderBookCheckpoint>();

    let area = TestArea::new("durable-staged-prefix");
    let wal = area.join("matching.wal");
    let snapshot = area.join("matching.qsnp");
    let first = resting(1, 1, 5);
    let second = resting(2, 2, 6);
    let suffix = resting(3, 3, 7);
    let mut durable = DurableOrderBook::open(&wal, definition(), journal_options()).unwrap();
    durable.submit(first).unwrap();
    durable.submit(second).unwrap();

    let capture = durable.capture_checkpoint_candidate().unwrap();
    let shared_capture = capture.clone();
    assert_eq!(capture.generation(), 5);
    assert_eq!(capture.wal_metadata_sequence(), 1);
    assert_eq!(capture.command_count(), 2);
    assert_eq!(capture.active_order_count(), 2);
    assert!(capture.shares_checkpoint_storage_with(&shared_capture));

    let expected_suffix = durable.submit(suffix).unwrap();
    let verified = std::thread::spawn(move || shared_capture.verify().unwrap())
        .join()
        .unwrap();
    let independently_verified = capture.verify().unwrap();
    assert!(verified.shares_checkpoint_storage_with(&independently_verified));
    assert_eq!(verified.generation(), 5);
    assert_eq!(verified.command_count(), 2);
    assert_eq!(verified.active_order_count(), 2);
    let receipt = durable
        .write_verified_checkpoint(&snapshot, &verified, SnapshotOptions::default())
        .unwrap();
    assert_eq!(receipt.generation(), 5);
    durable.close().unwrap();

    let mut recovered = DurableOrderBook::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.book().active_order_count(), 3);
    let retry = recovered.submit(suffix).unwrap();
    assert!(retry.replayed);
    let mut expected_retry = expected_suffix;
    expected_retry.replayed = true;
    assert_eq!(retry, expected_retry);
    recovered.close().unwrap();
}

#[test]
fn verified_matching_cutover_retires_prefix_and_preserves_live_suffix() {
    let area = TestArea::new("verified-cutover-suffix");
    let wal = area.join("matching.wal");
    let checkpoint_base = area.join("matching.qsnp");
    let first = resting(1, 1, 5);
    let second = resting(2, 2, 6);
    let suffix = resting(3, 3, 7);
    let mut durable = DurableOrderBook::open(&wal, definition(), journal_options()).unwrap();
    durable.submit(first).unwrap();
    durable.submit(second).unwrap();
    let capture = durable.capture_checkpoint_candidate().unwrap();
    let worker = std::thread::spawn(move || capture.verify().unwrap());
    let suffix_report = durable.submit(suffix).unwrap();
    let verified = worker.join().unwrap();

    let cutover = durable
        .compact_verified_checkpoint(&checkpoint_base, &verified, SnapshotOptions::default())
        .unwrap();

    assert_eq!(cutover.snapshot().generation(), 5);
    assert_eq!(cutover.wal_first_sequence(), 5);
    assert!(matches!(
        durable.compact_verified_checkpoint(
            &checkpoint_base,
            &verified,
            SnapshotOptions::default()
        ),
        Err(DurableError::CheckpointCaptureStale {
            captured_epoch: 0,
            current_epoch: 1
        })
    ));
    durable.close().unwrap();

    let frames = JournalReader::open(&wal, journal_options())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0].kind(), RecordKind::CheckpointAnchor);
    assert_eq!(frames[0].sequence(), 5);
    assert_eq!(frames[1].kind(), RecordKind::Command);
    assert_eq!(frames[1].sequence(), 6);
    assert_eq!(frames[2].kind(), RecordKind::ExecutionReport);
    assert_eq!(frames[2].sequence(), 7);

    let mut recovered = DurableOrderBook::open_with_checkpoint(
        &wal,
        &checkpoint_base,
        definition(),
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.book().active_order_count(), 3);
    let mut expected_retry = suffix_report;
    expected_retry.replayed = true;
    assert_eq!(recovered.submit(suffix).unwrap(), expected_retry);
    recovered.close().unwrap();
}

#[test]
fn durable_verified_checkpoint_rejects_another_reopen_or_cutover_epoch() {
    let area = TestArea::new("durable-staged-fences");
    let wal = area.join("matching.wal");
    let other_wal = area.join("other.wal");
    let snapshot = area.join("matching.qsnp");
    let other_snapshot = area.join("other.qsnp");
    let checkpoint_base = area.join("cutover.qsnp");
    let stale_snapshot = area.join("stale.qsnp");

    let mut source = DurableOrderBook::open(&wal, definition(), journal_options()).unwrap();
    source.submit(resting(1, 1, 5)).unwrap();
    let verified = source
        .capture_checkpoint_candidate()
        .unwrap()
        .verify()
        .unwrap();

    let mut other = DurableOrderBook::open(&other_wal, definition(), journal_options()).unwrap();
    assert!(matches!(
        other.write_verified_checkpoint(&other_snapshot, &verified, SnapshotOptions::default()),
        Err(DurableError::CheckpointCaptureOriginMismatch)
    ));
    assert!(!other.is_poisoned());
    assert!(!other_snapshot.exists());
    other.submit(resting(2, 2, 6)).unwrap();
    other.close().unwrap();

    source.close().unwrap();
    let mut reopened = DurableOrderBook::open(&wal, definition(), journal_options()).unwrap();
    assert!(matches!(
        reopened.write_verified_checkpoint(&snapshot, &verified, SnapshotOptions::default()),
        Err(DurableError::CheckpointCaptureOriginMismatch)
    ));
    assert!(!reopened.is_poisoned());
    assert!(!snapshot.exists());

    let stale = reopened
        .capture_checkpoint_candidate()
        .unwrap()
        .verify()
        .unwrap();
    reopened
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert!(matches!(
        reopened.write_verified_checkpoint(&stale_snapshot, &stale, SnapshotOptions::default()),
        Err(DurableError::CheckpointCaptureStale {
            captured_epoch: 0,
            current_epoch: 1
        })
    ));
    assert!(!reopened.is_poisoned());
    assert!(!stale_snapshot.exists());
    reopened.submit(resting(3, 3, 7)).unwrap();
    reopened.close().unwrap();
}

fn journal_options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn write_one_command_checkpoint(
    wal: &Path,
    snapshot: &Path,
    command: Command,
) -> OrderBookCheckpoint {
    let mut durable = DurableOrderBook::open(wal, definition(), journal_options()).unwrap();
    durable.submit(command).unwrap();
    durable
        .write_checkpoint(snapshot, SnapshotOptions::default())
        .unwrap();
    let checkpoint = SnapshotFile::read(snapshot, SnapshotOptions::default()).unwrap();
    durable.close().unwrap();
    checkpoint
}

#[test]
fn checkpoint_restores_account_fence_revision_and_atomic_cancellation() {
    let area = TestArea::new("account-control");
    let wal = area.join("matching.wal");
    let snapshot = area.join("matching.qsnp");
    let block = account_control(2, 101, 0, AccountControlAction::BlockAndCancel);
    let mut durable = DurableOrderBook::open(&wal, definition(), journal_options()).unwrap();
    durable.submit(resting(1, 1, 5)).unwrap();
    durable.submit(block).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let checkpoint: OrderBookCheckpoint =
        SnapshotFile::read(&snapshot, SnapshotOptions::default()).unwrap();
    let direct = OrderBook::from_checkpoint(&checkpoint).unwrap();
    assert_eq!(direct.active_order_count(), 0);
    assert_eq!(
        direct.account_control(AccountId::new(101).unwrap()).state(),
        AccountAdmissionState::Blocked
    );
    assert_eq!(
        direct
            .account_control(AccountId::new(101).unwrap())
            .revision(),
        1
    );
    direct.validate().unwrap();

    let recovered = DurableOrderBook::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(
        recovered
            .book()
            .account_control(AccountId::new(101).unwrap())
            .revision(),
        1
    );
    recovered.book().validate().unwrap();
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end checkpoint scenario keeps the complete FIFO, reserve, STP, retry, and suffix evidence contiguous"
)]
fn matching_checkpoint_preserves_fifo_reserve_stp_idempotency_and_replays_only_suffix() {
    let area = TestArea::new("round-trip");
    let wal = area.join("matching.wal");
    let snapshot = area.join("matching.qsnp");
    let reserve = order(
        1,
        1,
        11,
        Side::Sell,
        30,
        OrderDisplay::Reserve {
            peak: Quantity::new(10).unwrap(),
        },
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelBoth,
    );
    let displayed = order(
        2,
        2,
        12,
        Side::Sell,
        20,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelResting,
    );
    let refresh = order(
        3,
        3,
        13,
        Side::Buy,
        10,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    );
    let bid = order(
        4,
        5,
        12,
        Side::Buy,
        5,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(90)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let suffix = Command::Replace(ReplaceOrder {
        command_id: CommandId::new(5).unwrap(),
        order_id: OrderId::new(2).unwrap(),
        account_id: AccountId::new(12).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        new_quantity: Quantity::new(20).unwrap(),
        new_price: Price::from_raw(90),
        new_display: OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(5),
    });

    let mut durable =
        DurableOrderBook::open(&wal, definition(), journal_options()).expect("shard opens");
    durable.submit(reserve).unwrap();
    durable.submit(displayed).unwrap();
    durable.submit(refresh).unwrap();
    durable.submit(bid).unwrap();
    let receipt = durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .expect("checkpoint writes after WAL barrier");
    assert_eq!(receipt.generation(), 9);
    let expected_suffix = durable.submit(suffix).unwrap();
    assert!(expected_suffix.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            order_id,
            reason: CancelReason::SelfTradeResting,
            ..
        } if order_id == OrderId::new(5).unwrap()
    )));
    durable.close().unwrap();

    let checkpoint =
        SnapshotFile::read::<OrderBookCheckpoint>(&snapshot, SnapshotOptions::default())
            .expect("typed checkpoint reads");
    assert_eq!(checkpoint.command_count(), 4);
    assert_eq!(checkpoint.active_order_count(), 3);
    assert_eq!(
        checkpoint
            .orders()
            .iter()
            .filter(|order| order.side() == Side::Sell)
            .map(|order| order.order_id())
            .collect::<Vec<_>>(),
        vec![OrderId::new(2).unwrap(), OrderId::new(1).unwrap()]
    );
    let mut directly_restored = OrderBook::from_checkpoint(&checkpoint).unwrap();
    assert_eq!(directly_restored.submit(suffix).unwrap(), expected_suffix);

    let mut recovered = DurableOrderBook::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        journal_options(),
        SnapshotOptions::default(),
    )
    .expect("checkpoint and suffix recover");
    assert_eq!(recovered.recovery().checkpointed_commands, 4);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(
        recovered.book().active_orders().unwrap(),
        directly_restored.active_orders().unwrap()
    );
    let retry = recovered
        .submit(reserve)
        .expect("pre-checkpoint retry resolves");
    assert!(retry.replayed);
    assert_eq!(recovered.book().active_order_count(), 2);
}

#[test]
fn matching_checkpoint_has_a_distinct_stable_snapshot_kind_and_codec_round_trip() {
    let area = TestArea::new("codec");
    let wal = area.join("matching.wal");
    let snapshot = area.join("matching.qsnp");
    let checkpoint = write_one_command_checkpoint(&wal, &snapshot, resting(1, 1, 5));
    let bytes = fs::read(&snapshot).unwrap();
    assert_eq!(&bytes[0..4], b"QSNP");
    assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), 2);
    assert_eq!(
        OrderBookCheckpoint::decode(&checkpoint.encode().unwrap()).unwrap(),
        checkpoint
    );

    let encoded = checkpoint.encode().unwrap();
    let definition_length =
        usize::try_from(u32::from_le_bytes(encoded[16..20].try_into().unwrap())).unwrap();
    let first_order = 20 + definition_length + 4;

    let mut invalid_boundary = encoded.clone();
    invalid_boundary[8..16].copy_from_slice(&4_u64.to_le_bytes());
    assert!(matches!(
        OrderBookCheckpoint::decode(&invalid_boundary),
        Err(CodecError::InvalidMatchingCheckpoint(_))
    ));

    let mut impossible_display = encoded.clone();
    impossible_display[first_order + 33..first_order + 41].copy_from_slice(&6_u64.to_le_bytes());
    assert!(matches!(
        OrderBookCheckpoint::decode(&impossible_display),
        Err(CodecError::InvalidMatchingCheckpoint(_))
    ));

    let history_count = first_order + 43;
    let command_length = usize::try_from(u32::from_le_bytes(
        encoded[history_count + 4..history_count + 8]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    let report = history_count + 8 + command_length + 4;
    let first_event_sequence = report + 14;
    let mut non_genesis_trace = encoded;
    non_genesis_trace[first_event_sequence..first_event_sequence + 8]
        .copy_from_slice(&2_u64.to_le_bytes());
    let second_event_sequence = first_event_sequence + 42;
    non_genesis_trace[second_event_sequence..second_event_sequence + 8]
        .copy_from_slice(&3_u64.to_le_bytes());
    let non_genesis_result = OrderBookCheckpoint::decode(&non_genesis_trace);
    assert!(
        matches!(
            non_genesis_result,
            Err(CodecError::InvalidMatchingCheckpoint(_))
        ),
        "unexpected decode result: {non_genesis_result:?}"
    );
}

#[test]
fn matching_checkpoint_binds_non_default_wal_origin_and_rejects_lineage_forks() {
    let area = TestArea::new("lineage");
    let wal = area.join("matching.wal");
    let snapshot = area.join("matching.qsnp");
    let fork_wal = area.join("fork.wal");
    let fork_snapshot = area.join("fork.qsnp");
    let options = JournalOptions {
        initial_sequence: 10_000,
        ..journal_options()
    };

    let mut durable = DurableOrderBook::open(&wal, definition(), options).unwrap();
    durable.submit(resting(1, 1, 5)).unwrap();
    let receipt = durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    assert_eq!(receipt.generation(), 10_002);
    durable.submit(resting(2, 2, 7)).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let mut fork = DurableOrderBook::open(&fork_wal, definition(), options).unwrap();
    fork.submit(resting(1, 1, 6)).unwrap();
    fork.submit(resting(2, 2, 7)).unwrap();
    fork.write_checkpoint(&fork_snapshot, SnapshotOptions::default())
        .unwrap();
    fork.close().unwrap();
    let fork_checkpoint: OrderBookCheckpoint =
        SnapshotFile::read(&fork_snapshot, SnapshotOptions::default()).unwrap();
    assert!(matches!(
        SnapshotFile::write(&snapshot, &fork_checkpoint, SnapshotOptions::default()),
        Err(quotick::snapshot::SnapshotError::SameGenerationDivergence { .. })
    ));

    let recovered = DurableOrderBook::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 0);
}

#[test]
fn matching_checkpoint_must_equal_the_exact_wal_prefix_and_cannot_be_ahead() {
    let area = TestArea::new("prefix");
    let source_wal = area.join("source.wal");
    let divergent_wal = area.join("divergent.wal");
    let short_wal = area.join("short.wal");
    let snapshot = area.join("matching.qsnp");

    let mut source = DurableOrderBook::open(&source_wal, definition(), journal_options()).unwrap();
    source.submit(resting(1, 1, 5)).unwrap();
    source.submit(resting(2, 2, 7)).unwrap();
    source
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    source.close().unwrap();

    let mut divergent =
        DurableOrderBook::open(&divergent_wal, definition(), journal_options()).unwrap();
    divergent.submit(resting(1, 1, 6)).unwrap();
    divergent.close().unwrap();
    assert!(matches!(
        DurableOrderBook::open_with_checkpoint(
            &divergent_wal,
            &snapshot,
            definition(),
            journal_options(),
            SnapshotOptions::default(),
        ),
        Err(DurableError::CheckpointPrefixDivergence { .. })
    ));

    let mut short = DurableOrderBook::open(&short_wal, definition(), journal_options()).unwrap();
    short.submit(resting(1, 1, 5)).unwrap();
    short.close().unwrap();
    assert!(matches!(
        DurableOrderBook::open_with_checkpoint(
            &short_wal,
            &snapshot,
            definition(),
            journal_options(),
            SnapshotOptions::default(),
        ),
        Err(DurableError::CheckpointAheadOfWal { .. })
    ));
}

#[test]
fn segmented_matching_checkpoint_recovers_across_physical_boundaries() {
    let area = TestArea::new("segmented");
    let segments = area.join("segments");
    let snapshot = area.join("matching.qsnp");
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 512,
        journal: journal_options(),
    };
    let mut durable = DurableOrderBook::open_segmented(&segments, definition(), options).unwrap();
    durable.submit(resting(1, 1, 5)).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.submit(resting(2, 2, 7)).unwrap();
    durable.close().unwrap();

    let recovered = DurableOrderBook::open_segmented_with_checkpoint(
        &segments,
        &snapshot,
        definition(),
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert!(recovered.recovery().journal.segment_count >= 2);
    assert_eq!(recovered.book().active_order_count(), 2);
}

#[test]
fn segmented_durable_staged_checkpoint_replays_only_the_post_capture_suffix() {
    let area = TestArea::new("segmented-staged");
    let segments = area.join("segments");
    let snapshot = area.join("matching.qsnp");
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 512,
        journal: journal_options(),
    };
    let mut durable = DurableOrderBook::open_segmented(&segments, definition(), options).unwrap();
    durable.submit(resting(1, 1, 5)).unwrap();
    let capture = durable.capture_checkpoint_candidate().unwrap();
    durable.submit(resting(2, 2, 7)).unwrap();
    let verified = std::thread::spawn(move || capture.verify().unwrap())
        .join()
        .unwrap();
    durable
        .write_verified_checkpoint(&snapshot, &verified, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let recovered = DurableOrderBook::open_segmented_with_checkpoint(
        &segments,
        &snapshot,
        definition(),
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert!(recovered.recovery().journal.segment_count >= 2);
    assert_eq!(recovered.book().active_order_count(), 2);
}

#[test]
fn segmented_matching_cutover_switches_generations_and_replays_only_the_suffix() {
    let area = TestArea::new("segmented-cutover");
    let segments = area.join("segments");
    let checkpoint_base = area.join("matching.qsnp");
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 512,
        journal: journal_options(),
    };
    let mut durable = DurableOrderBook::open_segmented(&segments, definition(), options).unwrap();
    durable.submit(resting(1, 1, 5)).unwrap();
    durable.submit(resting(2, 2, 7)).unwrap();
    let first = durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(first.slot(), CheckpointSlot::A);
    assert_eq!(first.snapshot().generation(), 5);
    durable.submit(resting(3, 3, 11)).unwrap();
    durable.close().unwrap();

    let frames = SegmentedJournalReader::open(&segments, options)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames[0].kind(), RecordKind::CheckpointAnchor);
    assert_eq!(frames[0].sequence(), 5);
    assert_eq!(frames.len(), 3);
    assert!(matches!(
        DurableOrderBook::open_segmented(&segments, definition(), options),
        Err(DurableError::CheckpointRequiredForCompactedWal { sequence: 5 })
    ));

    let mut recovered = DurableOrderBook::open_segmented_with_checkpoint(
        &segments,
        &checkpoint_base,
        definition(),
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.book().active_order_count(), 3);
    assert!(recovered.submit(resting(1, 1, 5)).unwrap().replayed);
    let second = recovered
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(second.slot(), CheckpointSlot::B);
    assert_eq!(second.snapshot().generation(), 7);
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
    let final_state = DurableOrderBook::open_segmented_with_checkpoint(
        &segments,
        &checkpoint_base,
        definition(),
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(final_state.recovery().checkpointed_commands, 3);
    assert_eq!(final_state.recovery().replayed_commands, 0);
    assert_eq!(final_state.book().active_order_count(), 3);
}

#[test]
fn matching_checkpoint_output_cannot_alias_wal_storage() {
    let area = TestArea::new("alias");
    let wal = area.join("matching.wal");
    let mut durable = DurableOrderBook::open(&wal, definition(), journal_options()).unwrap();
    assert!(matches!(
        durable.write_checkpoint(&wal, SnapshotOptions::default()),
        Err(DurableError::CheckpointPathConflictsWithWal { .. })
    ));
    let cutover_pending = Journal::cutover_pending_path(&wal).unwrap();
    assert!(matches!(
        durable.write_checkpoint(&cutover_pending, SnapshotOptions::default()),
        Err(DurableError::CheckpointPathConflictsWithWal { .. })
    ));

    let segments = area.join("segments");
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 1_024,
        journal: journal_options(),
    };
    let mut segmented = DurableOrderBook::open_segmented(&segments, definition(), options).unwrap();
    assert!(matches!(
        segmented.write_checkpoint(segments.join("matching.qsnp"), SnapshotOptions::default()),
        Err(DurableError::CheckpointPathConflictsWithWal { .. })
    ));
}

#[test]
fn single_file_checkpoint_cutover_replaces_the_wal_prefix_and_reopens_its_suffix() {
    let area = TestArea::new("single-cutover");
    let wal = area.join("matching.wal");
    let checkpoint_base = area.join("matching.qsnp");
    let mut durable = DurableOrderBook::open(&wal, definition(), journal_options()).unwrap();
    durable.submit(resting(1, 1, 5)).unwrap();
    durable.submit(resting(2, 2, 7)).unwrap();
    let original_wal_bytes = fs::metadata(&wal).unwrap().len();

    let first_cutover = durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .expect("first cutover persists a checkpoint before replacing the WAL");
    assert_eq!(first_cutover.slot(), CheckpointSlot::A);
    assert_eq!(first_cutover.snapshot().generation(), 5);
    assert_eq!(first_cutover.wal_first_sequence(), 5);
    assert!(SnapshotFile::slot_path(&checkpoint_base, CheckpointSlot::A).exists());
    assert!(fs::metadata(&wal).unwrap().len() < original_wal_bytes);
    let first_frame = JournalReader::open(&wal, journal_options())
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(first_frame.kind(), RecordKind::CheckpointAnchor);
    assert_eq!(first_frame.sequence(), 5);

    durable.submit(resting(3, 3, 11)).unwrap();
    durable.close().unwrap();
    assert!(matches!(
        DurableOrderBook::open(&wal, definition(), journal_options()),
        Err(DurableError::CheckpointRequiredForCompactedWal { sequence: 5 })
    ));

    let mut recovered = DurableOrderBook::open_with_checkpoint(
        &wal,
        &checkpoint_base,
        definition(),
        journal_options(),
        SnapshotOptions::default(),
    )
    .expect("anchor-selected checkpoint and suffix recover");
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.book().active_order_count(), 3);
    assert!(recovered.submit(resting(1, 1, 5)).unwrap().replayed);

    let second_cutover = recovered
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .expect("a second cutover uses the inactive checkpoint slot");
    assert_eq!(second_cutover.slot(), CheckpointSlot::B);
    assert_eq!(second_cutover.snapshot().generation(), 7);
    assert!(SnapshotFile::slot_path(&checkpoint_base, CheckpointSlot::A).exists());
    assert!(SnapshotFile::slot_path(&checkpoint_base, CheckpointSlot::B).exists());
    recovered.close().unwrap();

    let twice_recovered = DurableOrderBook::open_with_checkpoint(
        &wal,
        &checkpoint_base,
        definition(),
        journal_options(),
        SnapshotOptions::default(),
    )
    .expect("second anchor selects the second checkpoint slot");
    assert_eq!(twice_recovered.recovery().checkpointed_commands, 3);
    assert_eq!(twice_recovered.recovery().replayed_commands, 0);
    assert_eq!(twice_recovered.book().active_order_count(), 3);
}

#[test]
fn compacted_wal_rejects_a_corrupt_or_wrong_checkpoint_slot() {
    let area = TestArea::new("cutover-binding");
    let wal = area.join("matching.wal");
    let checkpoint_base = area.join("matching.qsnp");
    let mut durable = DurableOrderBook::open(&wal, definition(), journal_options()).unwrap();
    durable.submit(resting(1, 1, 5)).unwrap();
    durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let slot = SnapshotFile::slot_path(&checkpoint_base, CheckpointSlot::A);
    let original = fs::read(&slot).unwrap();
    let mut bytes = original.clone();
    let final_byte = bytes.last_mut().unwrap();
    *final_byte ^= 0x80;
    fs::write(&slot, bytes).unwrap();
    assert!(matches!(
        DurableOrderBook::open_with_checkpoint(
            &wal,
            &checkpoint_base,
            definition(),
            journal_options(),
            SnapshotOptions::default(),
        ),
        Err(DurableError::Snapshot(_))
    ));

    fs::write(&slot, original).unwrap();
    let fork_wal = area.join("fork.wal");
    let fork_snapshot = area.join("fork.qsnp");
    let mut fork = DurableOrderBook::open(&fork_wal, definition(), journal_options()).unwrap();
    fork.submit(resting(1, 1, 6)).unwrap();
    fork.write_checkpoint(&fork_snapshot, SnapshotOptions::default())
        .unwrap();
    fork.close().unwrap();
    fs::copy(fork_snapshot, &slot).unwrap();
    assert!(matches!(
        DurableOrderBook::open_with_checkpoint(
            &wal,
            &checkpoint_base,
            definition(),
            journal_options(),
            SnapshotOptions::default(),
        ),
        Err(DurableError::CheckpointAnchorMismatch {
            generation: 3,
            slot: CheckpointSlot::A
        })
    ));
}
