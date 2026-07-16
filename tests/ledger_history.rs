use quotick::ledger::{
    JournalEntry, Ledger, LedgerBatch, LedgerCorrection, LedgerHistoryError, LedgerRecord,
    LedgerRecordTransactions, LedgerRecordView, Posting, RetainedLedgerRecord,
};
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

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

#[test]
fn borrowed_history_is_zero_copy_grouped_ordered_and_nonmutating() {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<LedgerHistoryError>();
    assert_send_sync::<LedgerRecordView<'static>>();
    assert_send_sync::<LedgerRecordTransactions<'static>>();
    assert_send_sync::<RetainedLedgerRecord<'static>>();

    let original = entry(1, 100, 10);
    let correction = LedgerCorrection::new(
        transaction(2),
        2,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(20),
        entry(3, 70, 21),
        &original,
    )
    .unwrap();
    let batch = LedgerBatch::new(vec![entry(4, 5, 30), entry(5, -2, 31)]).unwrap();
    let mut ledger = Ledger::new();
    ledger.post(original).unwrap();
    ledger.correct(correction).unwrap();
    ledger.post_batch(batch.clone()).unwrap();
    let before = (
        ledger.record_count(),
        ledger.entry_count(),
        ledger.nonzero_balance_count(),
        ledger.balance(account(1), asset()),
    );

    assert!(ledger.try_record_view(0).unwrap().is_none());
    assert!(ledger.try_record_view(4).unwrap().is_none());
    let first = ledger.try_record_view(1).unwrap().unwrap();
    let LedgerRecordView::Entry(first_entry) = first else {
        panic!("first event must be an entry");
    };
    assert!(std::ptr::eq(
        first_entry,
        ledger.transaction(transaction(1)).unwrap()
    ));

    let mut history = ledger.retained_history();
    assert_eq!(history.len(), 3);
    let first = history.next().unwrap().unwrap();
    assert_eq!(first.sequence(), 1);
    assert_eq!(first.record().primary_transaction_id(), transaction(1));
    assert_eq!(history.len(), 2);
    let third = history.next_back().unwrap().unwrap();
    assert_eq!(third.sequence(), 3);
    let LedgerRecordView::Batch(batch_view) = third.record() else {
        panic!("third event must be a batch");
    };
    assert!(batch_view.shares_entry_storage_with(&batch));
    assert_eq!(
        third
            .record()
            .transactions()
            .map(JournalEntry::transaction_id)
            .collect::<Vec<_>>(),
        [transaction(4), transaction(5)]
    );

    let second = history.next().unwrap().unwrap();
    assert_eq!(second.sequence(), 2);
    assert_eq!(second.record().transaction_count(), 2);
    let mut correction_transactions = second.record().transactions();
    assert_eq!(correction_transactions.len(), 2);
    assert_eq!(
        correction_transactions
            .next_back()
            .unwrap()
            .transaction_id(),
        transaction(3)
    );
    assert_eq!(
        correction_transactions.next().unwrap().transaction_id(),
        transaction(2)
    );
    assert!(correction_transactions.next().is_none());
    assert!(history.next().is_none());

    assert_eq!(ledger.record(3), Some(LedgerRecord::Batch(batch)));
    assert_eq!(
        (
            ledger.record_count(),
            ledger.entry_count(),
            ledger.nonzero_balance_count(),
            ledger.balance(account(1), asset()),
        ),
        before
    );
    ledger.validate().unwrap();
}

#[test]
fn borrowed_history_survives_direct_checkpoint_restoration() {
    let mut ledger = Ledger::new();
    ledger.post(entry(1, 10, 10)).unwrap();
    ledger
        .post_batch(LedgerBatch::new(vec![entry(2, 5, 20), entry(3, -2, 21)]).unwrap())
        .unwrap();
    let checkpoint = ledger.checkpoint().unwrap();
    let restored = Ledger::from_checkpoint(&checkpoint).unwrap();

    let history = restored
        .retained_history()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        history
            .iter()
            .map(|record| (record.sequence(), record.record().transaction_count()))
            .collect::<Vec<_>>(),
        [(1, 1), (2, 2)]
    );
    assert_eq!(
        history[1]
            .record()
            .transactions()
            .map(JournalEntry::transaction_id)
            .collect::<Vec<_>>(),
        [transaction(2), transaction(3)]
    );
    restored.validate().unwrap();
}
