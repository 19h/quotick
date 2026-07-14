use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::BinaryCodec;
use quotick::durable_ledger::{DurableLedger, DurableLedgerError};
use quotick::journal::{
    Durability, Journal, JournalOptions, JournalReader, RecordKind, SegmentedJournal,
    SegmentedJournalOptions, SegmentedJournalReader,
};
use quotick::ledger::{JournalEntry, Ledger, Posting};
use quotick::snapshot::{CheckpointSlot, SnapshotFile, SnapshotOptions};
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestArea(PathBuf);

impl TestArea {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-durable-checkpoint-{label}-{}-{nonce}",
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

fn asset() -> AssetId {
    AssetId::new(1).unwrap()
}

fn entry(transaction_id: u64, amount: i128) -> JournalEntry {
    JournalEntry::new(
        TransactionId::new(transaction_id).unwrap(),
        transaction_id,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(0),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(),
                amount,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(),
                amount: -amount,
            },
        ],
    )
    .unwrap()
}

fn journal_options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn segmented_options(value: &JournalEntry) -> SegmentedJournalOptions {
    SegmentedJournalOptions {
        maximum_segment_bytes: u64::try_from(24 + value.encode().unwrap().len()).unwrap(),
        journal: journal_options(),
    }
}

fn write_checkpoint(path: &Path, entries: &[JournalEntry]) {
    let mut ledger = Ledger::new();
    for value in entries {
        ledger.post(value.clone()).unwrap();
    }
    SnapshotFile::write(
        path,
        &ledger.checkpoint().unwrap(),
        SnapshotOptions::default(),
    )
    .unwrap();
}

#[test]
fn durable_ledger_restores_checkpoint_and_replays_only_the_wal_suffix() {
    let area = TestArea::new("single");
    let wal = area.join("ledger.qwal");
    let snapshot = area.join("ledger.qsnp");
    let first = entry(1, 10);
    let second = entry(2, 20);
    let third = entry(3, 30);
    let mut durable = DurableLedger::open(&wal, journal_options()).unwrap();
    durable.post(first.clone()).unwrap();
    durable.post(second).unwrap();
    let receipt = durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .expect("checkpoint persists after WAL barrier");
    assert_eq!(receipt.generation(), 2);
    durable.post(third).unwrap();
    durable.close().unwrap();

    let mut recovered = DurableLedger::open_with_checkpoint(
        &wal,
        &snapshot,
        journal_options(),
        SnapshotOptions::default(),
    )
    .expect("checkpoint and suffix recover");
    assert_eq!(recovered.recovery().checkpointed_records, 2);
    assert_eq!(recovered.recovery().replayed_records, 1);
    assert_eq!(recovered.ledger().balance(account(1), asset()), 60);
    assert!(recovered.post(first).unwrap().replayed);
}

#[test]
fn checkpoint_must_equal_the_exact_wal_prefix_and_cannot_be_ahead() {
    let area = TestArea::new("prefix-proof");
    let wal = area.join("ledger.qwal");
    let snapshot = area.join("ledger.qsnp");
    let mut durable = DurableLedger::open(&wal, journal_options()).unwrap();
    durable.post(entry(1, 100)).unwrap();
    durable.close().unwrap();

    write_checkpoint(&snapshot, &[entry(1, 200)]);
    assert!(matches!(
        DurableLedger::open_with_checkpoint(
            &wal,
            &snapshot,
            journal_options(),
            SnapshotOptions::default()
        ),
        Err(DurableLedgerError::CheckpointPrefixDivergence {
            wal_sequence: 1,
            checkpoint_record_index: 0
        })
    ));

    fs::remove_file(&snapshot).unwrap();
    write_checkpoint(&snapshot, &[entry(1, 100), entry(2, 50)]);
    assert!(matches!(
        DurableLedger::open_with_checkpoint(
            &wal,
            &snapshot,
            journal_options(),
            SnapshotOptions::default()
        ),
        Err(DurableLedgerError::CheckpointAheadOfWal {
            checkpoint_records: 2,
            wal_records: 1
        })
    ));
}

#[test]
fn segmented_ledger_checkpoint_recovery_crosses_physical_boundaries() {
    let area = TestArea::new("segmented");
    let wal = area.join("segments");
    let snapshot = area.join("ledger.qsnp");
    let first = entry(1, 10);
    let options = segmented_options(&first);
    let mut durable = DurableLedger::open_segmented(&wal, options).unwrap();
    durable.post(first).unwrap();
    durable.post(entry(2, 20)).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.post(entry(3, 30)).unwrap();
    durable.close().unwrap();

    let recovered = DurableLedger::open_segmented_with_checkpoint(
        &wal,
        &snapshot,
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().journal.segment_count, 3);
    assert_eq!(recovered.recovery().checkpointed_records, 2);
    assert_eq!(recovered.recovery().replayed_records, 1);
    assert_eq!(recovered.ledger().balance(account(1), asset()), 60);
}

#[test]
fn checkpoint_output_cannot_alias_wal_storage() {
    let area = TestArea::new("path-conflict");
    let wal = area.join("ledger.qwal");
    let mut single = DurableLedger::open(&wal, journal_options()).unwrap();
    assert!(matches!(
        single.write_checkpoint(&wal, SnapshotOptions::default()),
        Err(DurableLedgerError::CheckpointPathConflictsWithWal { .. })
    ));
    let cutover_pending = Journal::cutover_pending_path(&wal).unwrap();
    assert!(matches!(
        single.write_checkpoint(&cutover_pending, SnapshotOptions::default()),
        Err(DurableLedgerError::CheckpointPathConflictsWithWal { .. })
    ));
    single.close().unwrap();

    let segments = area.join("segments");
    let value = entry(1, 1);
    let mut segmented =
        DurableLedger::open_segmented(&segments, segmented_options(&value)).unwrap();
    assert!(matches!(
        segmented.write_checkpoint(segments.join("checkpoint.qsnp"), SnapshotOptions::default()),
        Err(DurableLedgerError::CheckpointPathConflictsWithWal { .. })
    ));
}

#[test]
fn ledger_checkpoint_cutover_separates_semantic_generation_from_physical_sequence() {
    let area = TestArea::new("single-cutover");
    let wal = area.join("ledger.qwal");
    let checkpoint_base = area.join("ledger.qsnp");
    let non_genesis = JournalOptions {
        initial_sequence: 10_000,
        ..journal_options()
    };
    let first = entry(1, 10);
    let second = entry(2, 20);
    let third = entry(3, 30);
    let mut durable = DurableLedger::open(&wal, non_genesis).unwrap();
    durable.post(first.clone()).unwrap();
    durable.post(second).unwrap();
    let first_cutover = durable
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(first_cutover.slot(), CheckpointSlot::A);
    assert_eq!(first_cutover.snapshot().generation(), 2);
    assert_eq!(first_cutover.wal_first_sequence(), 10_001);
    let anchor = JournalReader::open(&wal, non_genesis)
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(anchor.kind(), RecordKind::CheckpointAnchor);
    assert_eq!(anchor.sequence(), 10_001);
    durable.post(third).unwrap();
    durable.close().unwrap();

    assert!(matches!(
        DurableLedger::open(&wal, non_genesis),
        Err(DurableLedgerError::CheckpointRequiredForCompactedWal { sequence: 10_001 })
    ));
    let mut recovered = DurableLedger::open_with_checkpoint(
        &wal,
        &checkpoint_base,
        non_genesis,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_records, 2);
    assert_eq!(recovered.recovery().replayed_records, 1);
    assert_eq!(recovered.ledger().balance(account(1), asset()), 60);
    assert!(recovered.post(first).unwrap().replayed);

    let second_cutover = recovered
        .compact_to_checkpoint(&checkpoint_base, SnapshotOptions::default())
        .unwrap();
    assert_eq!(second_cutover.slot(), CheckpointSlot::B);
    assert_eq!(second_cutover.snapshot().generation(), 3);
    assert_eq!(second_cutover.wal_first_sequence(), 10_002);
    recovered.close().unwrap();

    let final_state = DurableLedger::open_with_checkpoint(
        &wal,
        &checkpoint_base,
        non_genesis,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(final_state.recovery().checkpointed_records, 3);
    assert_eq!(final_state.recovery().replayed_records, 0);
    assert_eq!(final_state.ledger().balance(account(1), asset()), 60);
}

#[test]
fn ledger_cutover_rejects_empty_storage_and_replaces_a_segment_generation() {
    let area = TestArea::new("cutover-boundary");
    let wal = area.join("ledger.qwal");
    let checkpoint = area.join("ledger.qsnp");
    let mut empty = DurableLedger::open(&wal, journal_options()).unwrap();
    assert!(matches!(
        empty.compact_to_checkpoint(&checkpoint, SnapshotOptions::default()),
        Err(DurableLedgerError::CompactionRequiresNonEmptyLedger)
    ));
    assert_eq!(fs::metadata(&wal).unwrap().len(), 0);
    empty.close().unwrap();

    let segments = area.join("segments-cutover");
    let value = entry(1, 1);
    let mut segmented =
        DurableLedger::open_segmented(&segments, segmented_options(&value)).unwrap();
    segmented.post(value.clone()).unwrap();
    let cutover = segmented
        .compact_to_checkpoint(&checkpoint, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    assert_eq!(cutover.snapshot().generation(), 1);
    segmented.post(entry(2, 2)).unwrap();
    segmented.close().unwrap();

    let frames = SegmentedJournalReader::open(&segments, segmented_options(&value))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(frames[0].kind(), RecordKind::CheckpointAnchor);
    assert_eq!(frames.len(), 2);
    let manager = SegmentedJournal::open(&segments, segmented_options(&value)).unwrap();
    assert_eq!(manager.recovery().active_generation, 2);
    assert!(
        manager
            .segments()
            .iter()
            .all(|segment| segment.generation() == 2)
    );
    manager.close().unwrap();

    let mut recovered = DurableLedger::open_segmented_with_checkpoint(
        &segments,
        &checkpoint,
        segmented_options(&value),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_records, 1);
    assert_eq!(recovered.recovery().replayed_records, 1);
    assert_eq!(recovered.ledger().balance(account(1), asset()), 3);
    let second = recovered
        .compact_to_checkpoint(&checkpoint, SnapshotOptions::default())
        .unwrap();
    assert_eq!(second.slot(), CheckpointSlot::B);
    assert_eq!(second.snapshot().generation(), 2);
    recovered.close().unwrap();
    let manager = SegmentedJournal::open(&segments, segmented_options(&value)).unwrap();
    assert_eq!(manager.recovery().active_generation, 3);
    assert_eq!(manager.segments().len(), 1);
    assert_eq!(manager.segments()[0].generation(), 3);
    manager.close().unwrap();
}
