use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable_ledger::{DurableLedger, DurableLedgerError};
use quotick::journal::{Durability, Journal, JournalOptions};
use quotick::ledger::{JournalEntry, LedgerError, Posting};
use quotick::snapshot::SnapshotOptions;
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-durable-ledger-lifecycle-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).unwrap()
}

fn date() -> AccountingDate {
    AccountingDate::UNIX_EPOCH
}

fn time() -> TimestampNs {
    TimestampNs::from_unix_nanos(0)
}

fn entry(transaction_id: u64, amount: i128) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        date(),
        time(),
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

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

#[test]
fn reversal_lineage_survives_wal_checkpoint_and_suffix_recovery() {
    let directory = TestDirectory::new("recovery");
    let wal = directory.join("ledger.qwal");
    let snapshot = directory.join("ledger.qsnp");
    let original = entry(1, 100);

    let mut durable = DurableLedger::open(&wal, options()).unwrap();
    durable.post(original).unwrap();
    durable
        .reverse(transaction(2), 2, date(), time(), transaction(1))
        .unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.post(entry(3, 25)).unwrap();
    durable.close().unwrap();

    let mut recovered =
        DurableLedger::open_with_checkpoint(&wal, &snapshot, options(), SnapshotOptions::default())
            .unwrap();
    assert_eq!(recovered.recovery().checkpointed_records, 2);
    assert_eq!(recovered.recovery().replayed_records, 1);
    assert_eq!(
        recovered.ledger().reversal_for(transaction(1)),
        Some(transaction(2))
    );
    assert_eq!(
        recovered.ledger().reversed_transaction(transaction(2)),
        Some(transaction(1))
    );
    assert!(matches!(
        recovered.reverse(transaction(4), 4, date(), time(), transaction(1)),
        Err(DurableLedgerError::Ledger(
            LedgerError::TransactionAlreadyReversed {
                original_transaction_id,
                reversal_transaction_id,
            }
        )) if original_transaction_id == transaction(1)
            && reversal_transaction_id == transaction(2)
    ));
}

#[test]
fn durable_recovery_rejects_a_reversal_without_its_target() {
    let directory = TestDirectory::new("missing-target");
    let wal = directory.join("ledger.qwal");
    let absent = entry(10, 100);
    let reversal = JournalEntry::reversal(transaction(11), 11, date(), time(), &absent).unwrap();
    let mut journal = Journal::open(&wal, options()).unwrap();
    journal.append(&reversal).unwrap();
    journal.close().unwrap();
    assert!(matches!(
        DurableLedger::open(&wal, options()),
        Err(DurableLedgerError::Ledger(
            LedgerError::ReversalTargetMissing(transaction_id)
        )) if transaction_id == transaction(10)
    ));
}
