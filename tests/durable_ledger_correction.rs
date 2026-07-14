use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable_ledger::DurableLedger;
use quotick::journal::{Durability, JournalOptions, JournalReader, RecordKind, RecoveryMode};
use quotick::ledger::{JournalEntry, LedgerCorrection, Posting};
use quotick::snapshot::SnapshotOptions;
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-ledger-correction-{label}-{}-{nonce}.qwal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
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
                account_id: AccountId::new(1).unwrap(),
                asset_id: AssetId::new(1).unwrap(),
                amount,
            },
            Posting {
                account_id: AccountId::new(2).unwrap(),
                asset_id: AssetId::new(1).unwrap(),
                amount: -amount,
            },
        ],
    )
    .unwrap()
}

fn correction(original: &JournalEntry) -> LedgerCorrection {
    LedgerCorrection::new(
        transaction(2),
        2,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(20),
        entry(3, 70, 21),
        original,
    )
    .unwrap()
}

fn options(recovery: RecoveryMode) -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        recovery,
        ..JournalOptions::default()
    }
}

#[test]
fn durable_correction_is_one_frame_one_event_and_recovers_both_transactions() {
    let file = TestFile::new("round-trip");
    let original = entry(1, 100, 10);
    let value = correction(&original);
    let mut durable = DurableLedger::open(&file.0, options(RecoveryMode::Strict)).unwrap();
    durable.post(original).unwrap();
    durable.correct(value.clone()).unwrap();
    let length = fs::metadata(&file.0).unwrap().len();
    assert!(durable.correct(value).unwrap().replayed);
    assert_eq!(fs::metadata(&file.0).unwrap().len(), length);
    durable.close().unwrap();

    let frames: Vec<_> = JournalReader::open(&file.0, options(RecoveryMode::Strict))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].kind(), RecordKind::LedgerEntry);
    assert_eq!(frames[1].kind(), RecordKind::LedgerCorrection);
    assert_eq!(
        frames[1].decode::<LedgerCorrection>().unwrap(),
        correction(&entry(1, 100, 10))
    );

    let recovered = DurableLedger::open(&file.0, options(RecoveryMode::Strict)).unwrap();
    assert_eq!(recovered.recovery().replayed_records, 2);
    assert_eq!(recovered.ledger().record_count(), 2);
    assert_eq!(recovered.ledger().entry_count(), 3);
    assert_eq!(
        recovered
            .ledger()
            .balance(AccountId::new(1).unwrap(), AssetId::new(1).unwrap()),
        70
    );
    recovered.ledger().validate().unwrap();
}

#[test]
fn torn_correction_frame_repairs_as_no_correction_effect() {
    let file = TestFile::new("torn");
    let original = entry(1, 100, 10);
    let mut durable = DurableLedger::open(&file.0, options(RecoveryMode::Strict)).unwrap();
    durable.post(original.clone()).unwrap();
    durable.correct(correction(&original)).unwrap();
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
    assert_eq!(recovered.ledger().record_count(), 1);
    assert_eq!(recovered.ledger().entry_count(), 1);
    assert_eq!(recovered.ledger().reversal_for(transaction(1)), None);
    assert_eq!(
        recovered
            .ledger()
            .balance(AccountId::new(1).unwrap(), AssetId::new(1).unwrap()),
        100
    );
    recovered.ledger().validate().unwrap();
}

#[test]
fn correction_grouping_survives_checkpoint_prefix_and_wal_suffix_recovery() {
    let file = TestFile::new("checkpoint");
    let snapshot = file.0.with_extension("qsnp");
    let original = entry(1, 100, 10);
    let value = correction(&original);
    let mut durable = DurableLedger::open(&file.0, options(RecoveryMode::Strict)).unwrap();
    durable.post(original).unwrap();
    durable.correct(value.clone()).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.post(entry(4, 5, 30)).unwrap();
    durable.close().unwrap();

    let mut recovered = DurableLedger::open_with_checkpoint(
        &file.0,
        &snapshot,
        options(RecoveryMode::Strict),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_records, 2);
    assert_eq!(recovered.recovery().replayed_records, 1);
    assert_eq!(recovered.ledger().record_count(), 3);
    assert_eq!(recovered.ledger().entry_count(), 4);
    assert!(recovered.correct(value).unwrap().replayed);
    assert_eq!(
        recovered
            .ledger()
            .balance(AccountId::new(1).unwrap(), AssetId::new(1).unwrap()),
        75
    );
    recovered.close().unwrap();
    let _ = fs::remove_file(snapshot);
}
