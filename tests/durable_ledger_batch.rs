use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable_ledger::{DurableLedger, DurableLedgerError};
use quotick::journal::{Durability, JournalOptions, JournalReader, RecordKind, RecoveryMode};
use quotick::ledger::{JournalEntry, LedgerBatch, LedgerRecordView, Posting};
use quotick::snapshot::SnapshotOptions;
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-ledger-batch-{label}-{}-{nonce}.qwal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(path.with_extension("qsnp"));
        Self(path)
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(self.0.with_extension("qsnp"));
    }
}

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn asset() -> AssetId {
    AssetId::new(1).unwrap()
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).unwrap()
}

fn entry(transaction_id: u64, amount: i128, recorded_at: u64) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(recorded_at),
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

fn batch() -> LedgerBatch {
    LedgerBatch::new(vec![entry(1, 100, 10), entry(2, -30, 11)]).unwrap()
}

fn options(recovery: RecoveryMode) -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        recovery,
        ..JournalOptions::default()
    }
}

#[test]
fn durable_batch_is_one_frame_one_event_and_exact_retry_adds_no_frame() {
    let file = TestFile::new("round-trip");
    let value = batch();
    let mut durable = DurableLedger::open(&file.0, options(RecoveryMode::Strict)).unwrap();
    let receipt = durable.post_batch(value.clone()).unwrap();
    assert_eq!(receipt.sequence, 1);
    assert_eq!(receipt.transaction_count, 2);
    let length = fs::metadata(&file.0).unwrap().len();
    assert!(durable.post_batch(value.clone()).unwrap().replayed);
    assert_eq!(fs::metadata(&file.0).unwrap().len(), length);
    durable.close().unwrap();

    let frames: Vec<_> = JournalReader::open(&file.0, options(RecoveryMode::Strict))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].kind(), RecordKind::LedgerBatch);
    assert_eq!(frames[0].decode::<LedgerBatch>().unwrap(), value);

    let mut recovered = DurableLedger::open(&file.0, options(RecoveryMode::Strict)).unwrap();
    assert_eq!(recovered.recovery().replayed_records, 1);
    assert_eq!(recovered.ledger().record_count(), 1);
    assert_eq!(recovered.ledger().entry_count(), 2);
    assert_eq!(recovered.ledger().balance(account(1), asset()), 70);
    assert_eq!(
        recovered
            .ledger()
            .try_balance_at(0, account(1), asset())
            .unwrap(),
        0
    );
    assert_eq!(
        recovered
            .ledger()
            .try_balance_at(1, account(1), asset())
            .unwrap(),
        70
    );
    let history = recovered
        .ledger()
        .retained_history()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].sequence(), 1);
    let LedgerRecordView::Batch(batch_view) = history[0].record() else {
        panic!("full-WAL recovery must retain the batch grouping");
    };
    assert_eq!(
        batch_view
            .entries()
            .iter()
            .map(JournalEntry::transaction_id)
            .collect::<Vec<_>>(),
        [transaction(1), transaction(2)]
    );
    assert_eq!(
        recovered
            .ledger()
            .account_statement(account(1), asset())
            .map(|line| {
                let line = line.unwrap();
                (
                    line.sequence(),
                    line.transaction_index(),
                    line.entry().transaction_id(),
                    line.posting().amount,
                )
            })
            .collect::<Vec<_>>(),
        [(1, 0, transaction(1), 100), (1, 1, transaction(2), -30),]
    );
    assert!(recovered.post_batch(batch()).unwrap().replayed);
    recovered.ledger().validate().unwrap();
}

#[test]
fn torn_batch_frame_repairs_as_no_batch_effect() {
    let file = TestFile::new("torn");
    let mut durable = DurableLedger::open(&file.0, options(RecoveryMode::Strict)).unwrap();
    durable.post_batch(batch()).unwrap();
    durable.close().unwrap();
    let complete_length = fs::metadata(&file.0).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(&file.0)
        .unwrap()
        .set_len(complete_length - 1)
        .unwrap();

    assert!(DurableLedger::open(&file.0, options(RecoveryMode::Strict)).is_err());
    let recovered = DurableLedger::open(&file.0, options(RecoveryMode::RepairTornTail)).unwrap();
    assert!(recovered.recovery().journal.truncated_bytes > 0);
    assert_eq!(recovered.ledger().record_count(), 0);
    assert_eq!(recovered.ledger().entry_count(), 0);
    assert_eq!(recovered.ledger().balance(account(1), asset()), 0);
    recovered.ledger().validate().unwrap();
}

#[test]
fn partial_batch_and_collision_fail_before_wal_growth() {
    let file = TestFile::new("validation");
    let first = entry(1, 100, 10);
    let mut durable = DurableLedger::open(&file.0, options(RecoveryMode::Strict)).unwrap();
    durable.post(first.clone()).unwrap();
    let length = fs::metadata(&file.0).unwrap().len();

    assert!(matches!(
        durable.post_batch(LedgerBatch::new(vec![first, entry(2, -30, 11)]).unwrap()),
        Err(DurableLedgerError::Ledger(
            quotick::ledger::LedgerError::BatchAlreadyPartiallyCommitted(transaction_id)
        )) if transaction_id == transaction(1)
    ));
    assert_eq!(fs::metadata(&file.0).unwrap().len(), length);
    assert!(matches!(
        durable.post_batch(
            LedgerBatch::new(vec![entry(1, 99, 10), entry(3, 1, 12)]).unwrap()
        ),
        Err(DurableLedgerError::Ledger(
            quotick::ledger::LedgerError::TransactionIdCollision(transaction_id)
        )) if transaction_id == transaction(1)
    ));
    assert_eq!(fs::metadata(&file.0).unwrap().len(), length);
    assert_eq!(durable.ledger().record_count(), 1);
    assert_eq!(durable.ledger().entry_count(), 1);
}

#[test]
fn batch_grouping_survives_checkpoint_prefix_and_wal_suffix_recovery() {
    let file = TestFile::new("checkpoint");
    let snapshot = file.0.with_extension("qsnp");
    let value = batch();
    let mut durable = DurableLedger::open(&file.0, options(RecoveryMode::Strict)).unwrap();
    durable.post_batch(value.clone()).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.post(entry(3, 5, 12)).unwrap();
    durable.close().unwrap();

    let mut recovered = DurableLedger::open_with_checkpoint(
        &file.0,
        &snapshot,
        options(RecoveryMode::Strict),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_records, 1);
    assert_eq!(recovered.recovery().replayed_records, 1);
    assert_eq!(recovered.ledger().record_count(), 2);
    assert_eq!(recovered.ledger().entry_count(), 3);
    assert_eq!(recovered.ledger().balance(account(1), asset()), 75);
    assert_eq!(
        recovered
            .ledger()
            .try_balance_at(1, account(1), asset())
            .unwrap(),
        70
    );
    assert_eq!(
        recovered
            .ledger()
            .try_balance_at(2, account(1), asset())
            .unwrap(),
        75
    );
    let history = recovered
        .ledger()
        .retained_history()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].sequence(), 1);
    assert!(matches!(history[0].record(), LedgerRecordView::Batch(_)));
    assert_eq!(history[1].sequence(), 2);
    assert!(matches!(history[1].record(), LedgerRecordView::Entry(_)));
    assert_eq!(
        recovered
            .ledger()
            .account_statement(account(1), asset())
            .map(|line| {
                let line = line.unwrap();
                (
                    line.sequence(),
                    line.transaction_index(),
                    line.entry().transaction_id(),
                    line.posting().amount,
                )
            })
            .collect::<Vec<_>>(),
        [
            (1, 0, transaction(1), 100),
            (1, 1, transaction(2), -30),
            (2, 0, transaction(3), 5),
        ]
    );
    assert!(recovered.post_batch(value).unwrap().replayed);
    recovered.ledger().validate().unwrap();
}
