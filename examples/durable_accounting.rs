//! Atomic accounting records, checkpoint cutover, recovery, and retry.

mod support;

use std::fs;

use quotick::durable_ledger::DurableLedger;
use quotick::ledger::{JournalEntry, LedgerBatch, LedgerCorrection, Posting};
use quotick::snapshot::{CheckpointSlot, SnapshotOptions};
use quotick::{AccountingDate, TransactionId};

use support::{ScratchArea, account, asset, journal_options, ledger_limits_spec, timestamp};

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "the transfer fixture keeps accounting identity and both legs explicit"
)]
fn transfer(
    transaction_id: u64,
    recorded_at: u64,
    date: AccountingDate,
    from: u64,
    to: u64,
    asset_id: u64,
    amount: i128,
) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        date,
        timestamp(recorded_at),
        vec![
            Posting {
                account_id: account(from),
                asset_id: asset(asset_id),
                amount: -amount,
            },
            Posting {
                account_id: account(to),
                asset_id: asset(asset_id),
                amount,
            },
        ],
    )
    .unwrap()
}

fn main() {
    let area = ScratchArea::new("durable-accounting");
    let wal = area.join("ledger.qwal");
    let checkpoint = area.join("ledger.qsnp");
    let date = AccountingDate::from_days_since_unix_epoch(20_000);

    let cash = transfer(1, 10, date, 900, 1, 2, 1_000);
    let inventory = transfer(2, 11, date, 900, 2, 1, 50);
    let funding = LedgerBatch::new(vec![cash.clone(), inventory]).unwrap();
    let replacement = transfer(4, 30, date, 900, 1, 2, 1_200);
    let correction =
        LedgerCorrection::new(transaction(3), 3, date, timestamp(29), replacement, &cash).unwrap();
    let suffix = transfer(5, 40, date, 1, 2, 2, 250);

    let mut durable =
        DurableLedger::open_with_limits(&wal, journal_options(), ledger_limits_spec()).unwrap();
    let batch_receipt = durable.post_batch(funding).unwrap();
    assert_eq!(batch_receipt.transaction_count, 2);
    assert_eq!(durable.correct(correction).unwrap().sequence, 2);

    let cutover = durable
        .compact_to_checkpoint(&checkpoint, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    assert_eq!(cutover.snapshot().generation(), 2);
    durable.post(suffix.clone()).unwrap();
    durable.close().unwrap();

    let mut recovered = DurableLedger::open_with_checkpoint_and_limits(
        &wal,
        &checkpoint,
        journal_options(),
        SnapshotOptions::default(),
        ledger_limits_spec(),
    )
    .unwrap();
    let recovery = recovered.recovery();
    assert_eq!(recovery.checkpointed_records, 2);
    assert_eq!(recovery.replayed_records, 1);
    assert_eq!(recovered.ledger().record_count(), 3);
    assert_eq!(recovered.ledger().balance(account(1), asset(2)), 950);
    assert_eq!(recovered.ledger().balance(account(2), asset(2)), 250);
    assert_eq!(recovered.ledger().balance(account(900), asset(2)), -1_200);
    assert_eq!(recovered.ledger().balance(account(2), asset(1)), 50);
    assert_eq!(recovered.ledger().balance(account(900), asset(1)), -50);
    recovered.ledger().validate().unwrap();

    let bytes_before_retry = fs::metadata(&wal).unwrap().len();
    assert!(recovered.post(suffix).unwrap().replayed);
    let bytes_after_retry = fs::metadata(&wal).unwrap().len();
    assert_eq!(bytes_after_retry, bytes_before_retry);
    let records = recovered.ledger().record_count();
    recovered.close().unwrap();

    println!(
        "checkpointed_records={} replayed_suffix={} records={} retry_wal_growth={}",
        recovery.checkpointed_records,
        recovery.replayed_records,
        records,
        bytes_after_retry - bytes_before_retry
    );
}
