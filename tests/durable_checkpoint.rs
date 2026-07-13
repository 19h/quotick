use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::BinaryCodec;
use quotick::durable_ledger::{DurableLedger, DurableLedgerError};
use quotick::journal::{Durability, JournalOptions, SegmentedJournalOptions};
use quotick::ledger::{JournalEntry, Ledger, Posting};
use quotick::snapshot::{SnapshotFile, SnapshotOptions};
use quotick::{AccountId, AssetId, TransactionId};

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
    assert_eq!(recovered.recovery().checkpointed_entries, 2);
    assert_eq!(recovered.recovery().replayed_entries, 1);
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
            checkpoint_index: 0
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
            checkpoint_entries: 2,
            wal_entries: 1
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
    assert_eq!(recovered.recovery().checkpointed_entries, 2);
    assert_eq!(recovered.recovery().replayed_entries, 1);
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
