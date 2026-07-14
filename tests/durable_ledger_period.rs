use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable_ledger::{DurableLedger, DurableLedgerError};
use quotick::journal::{Durability, JournalOptions};
use quotick::ledger::{JournalEntry, LedgerError, Posting};
use quotick::snapshot::SnapshotOptions;
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-durable-period-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn date(day: i32) -> AccountingDate {
    AccountingDate::from_days_since_unix_epoch(day)
}

fn timestamp(value: u64) -> TimestampNs {
    TimestampNs::from_unix_nanos(value)
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).unwrap()
}

fn entry(transaction_id: u64, day: i32, recorded_at: u64) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        date(day),
        timestamp(recorded_at),
        vec![
            Posting {
                account_id: AccountId::new(1).unwrap(),
                asset_id: AssetId::new(1).unwrap(),
                amount: 10,
            },
            Posting {
                account_id: AccountId::new(2).unwrap(),
                asset_id: AssetId::new(1).unwrap(),
                amount: -10,
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
fn period_fence_survives_checkpoint_and_wal_suffix_reopen() {
    let directory = TestDirectory::new();
    let wal = directory.0.join("ledger.qwal");
    let snapshot = directory.0.join("ledger.qsnp");
    let mut ledger = DurableLedger::open(&wal, options()).unwrap();
    ledger.post(entry(1, 10, 100)).unwrap();
    ledger
        .close_period(transaction(2), 2, timestamp(110), date(30))
        .unwrap();
    ledger
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    ledger
        .reopen_period(transaction(3), 3, timestamp(120), Some(date(15)))
        .unwrap();
    ledger.close().unwrap();

    let mut recovered =
        DurableLedger::open_with_checkpoint(&wal, &snapshot, options(), SnapshotOptions::default())
            .unwrap();
    assert_eq!(recovered.recovery().checkpointed_records, 2);
    assert_eq!(recovered.recovery().replayed_records, 1);
    assert_eq!(recovered.ledger().closed_through(), Some(date(15)));
    assert!(matches!(
        recovered.post(entry(4, 15, 130)),
        Err(DurableLedgerError::Ledger(
            LedgerError::AccountingPeriodClosed { .. }
        ))
    ));
    recovered.post(entry(5, 16, 130)).unwrap();
    recovered.close().unwrap();

    let reopened = DurableLedger::open(&wal, options()).unwrap();
    assert_eq!(reopened.ledger().closed_through(), Some(date(15)));
    assert_eq!(reopened.ledger().last_recorded_at(), Some(timestamp(130)));
    reopened.ledger().validate().unwrap();
}
