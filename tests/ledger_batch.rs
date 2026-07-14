use quotick::codec::BinaryCodec;
use quotick::ledger::{
    BatchPreparation, JournalEntry, Ledger, LedgerBatch, LedgerEntryKind, LedgerError,
    LedgerRecord, Posting,
};
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn asset(value: u64) -> AssetId {
    AssetId::new(value).unwrap()
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).unwrap()
}

fn date(day: i32) -> AccountingDate {
    AccountingDate::from_days_since_unix_epoch(day)
}

fn timestamp(value: u64) -> TimestampNs {
    TimestampNs::from_unix_nanos(value)
}

fn entry(
    transaction_id: u64,
    day: i32,
    recorded_at: u64,
    postings: &[(u64, u64, i128)],
) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        date(day),
        timestamp(recorded_at),
        postings
            .iter()
            .map(|&(account_id, asset_id, amount)| Posting {
                account_id: account(account_id),
                asset_id: asset(asset_id),
                amount,
            })
            .collect(),
    )
    .unwrap()
}

fn transfer(transaction_id: u64, amount: i128, recorded_at: u64) -> JournalEntry {
    entry(
        transaction_id,
        10,
        recorded_at,
        &[(1, 1, amount), (2, 1, -amount)],
    )
}

#[test]
fn batch_is_one_idempotent_multi_asset_event_with_canonical_transaction_order() {
    let cash = transfer(1, 100, 10);
    let delivery = entry(2, 10, 11, &[(1, 1, -30), (3, 1, 30), (1, 2, 7), (3, 2, -7)]);
    let batch = LedgerBatch::new(vec![cash, delivery]).unwrap();
    let mut ledger = Ledger::new();

    let receipt = ledger.post_batch(batch.clone()).unwrap();
    assert_eq!(receipt.sequence, 1);
    assert!(!receipt.replayed);
    assert_eq!(receipt.transaction_count, 2);
    assert_eq!(ledger.record_count(), 1);
    assert_eq!(ledger.entry_count(), 2);
    assert_eq!(
        ledger.transaction_ids().collect::<Vec<_>>(),
        vec![transaction(1), transaction(2)]
    );
    assert_eq!(ledger.balance(account(1), asset(1)), 70);
    assert_eq!(ledger.balance(account(2), asset(1)), -100);
    assert_eq!(ledger.balance(account(3), asset(1)), 30);
    assert_eq!(ledger.balance(account(1), asset(2)), 7);
    assert_eq!(ledger.balance(account(3), asset(2)), -7);
    assert_eq!(ledger.record(1), Some(LedgerRecord::Batch(batch.clone())));

    let replay = ledger.post_batch(batch.clone()).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.sequence, 1);
    assert_eq!(ledger.record_count(), 1);
    ledger.validate().unwrap();

    let checkpoint = ledger.checkpoint().unwrap();
    let decoded = quotick::ledger::LedgerCheckpoint::decode(&checkpoint.encode().unwrap()).unwrap();
    let mut restored = Ledger::from_checkpoint(decoded).unwrap();
    assert!(restored.post_batch(batch).unwrap().replayed);
    restored.validate().unwrap();
}

#[test]
fn batch_calculates_the_final_balance_without_materializing_fallible_intermediates() {
    let seed = transfer(1, i128::MAX, 10);
    let credit = entry(2, 10, 11, &[(1, 1, 1), (3, 1, -1)]);
    let debit = entry(3, 10, 12, &[(1, 1, -1), (3, 1, 1)]);
    let mut ledger = Ledger::new();
    ledger.post(seed).unwrap();

    ledger
        .post_batch(LedgerBatch::new(vec![credit.clone(), debit]).unwrap())
        .unwrap();
    assert_eq!(ledger.balance(account(1), asset(1)), i128::MAX);
    assert_eq!(ledger.balance(account(3), asset(1)), 0);

    let record_count = ledger.record_count();
    let balances = [
        ledger.balance(account(1), asset(1)),
        ledger.balance(account(2), asset(1)),
        ledger.balance(account(3), asset(1)),
        ledger.balance(account(4), asset(1)),
    ];
    let overflow = LedgerBatch::new(vec![
        entry(4, 10, 13, &[(1, 1, 1), (3, 1, -1)]),
        entry(5, 10, 14, &[(1, 1, 1), (4, 1, -1)]),
    ])
    .unwrap();
    assert_eq!(
        ledger.post_batch(overflow),
        Err(LedgerError::ArithmeticOverflow)
    );
    assert_eq!(ledger.record_count(), record_count);
    assert_eq!(
        [
            ledger.balance(account(1), asset(1)),
            ledger.balance(account(2), asset(1)),
            ledger.balance(account(3), asset(1)),
            ledger.balance(account(4), asset(1)),
        ],
        balances
    );
    ledger.validate().unwrap();
}

#[test]
fn batch_applies_period_and_reversal_rules_in_declared_order() {
    let original = transfer(1, 50, 10);
    let mut ledger = Ledger::new();
    ledger.post(original.clone()).unwrap();
    ledger
        .close_period(transaction(2), 2, timestamp(20), date(30))
        .unwrap();

    let reopen =
        JournalEntry::period_reopen(transaction(3), 3, timestamp(21), Some(date(10))).unwrap();
    let replacement = entry(4, 20, 22, &[(1, 1, 20), (2, 1, -20)]);
    let ordered = LedgerBatch::new(vec![reopen.clone(), replacement.clone()]).unwrap();
    ledger.post_batch(ordered).unwrap();
    assert_eq!(ledger.closed_through(), Some(date(10)));
    assert_eq!(ledger.balance(account(1), asset(1)), 70);

    let mut rejected = Ledger::new();
    rejected.post(original).unwrap();
    rejected
        .close_period(transaction(2), 2, timestamp(20), date(30))
        .unwrap();
    let financial_before_reopen = entry(7, 20, 21, &[(1, 1, 1), (2, 1, -1)]);
    let later_reopen =
        JournalEntry::period_reopen(transaction(8), 8, timestamp(22), Some(date(10))).unwrap();
    assert_eq!(
        rejected.post_batch(LedgerBatch::new(vec![financial_before_reopen, later_reopen]).unwrap()),
        Err(LedgerError::AccountingPeriodClosed {
            effective_date: date(20),
            closed_through: date(30),
        })
    );
    assert_eq!(rejected.closed_through(), Some(date(30)));
    assert_eq!(rejected.record_count(), 2);

    let created = entry(5, 11, 23, &[(1, 1, 9), (2, 1, -9)]);
    let reversal =
        JournalEntry::reversal(transaction(6), 6, date(11), timestamp(24), &created).unwrap();
    ledger
        .post_batch(LedgerBatch::new(vec![created, reversal]).unwrap())
        .unwrap();
    assert_eq!(ledger.balance(account(1), asset(1)), 70);
    assert_eq!(ledger.reversal_for(transaction(5)), Some(transaction(6)));
    assert_eq!(
        ledger.transaction(transaction(6)).unwrap().kind(),
        LedgerEntryKind::Reversal {
            reversed_transaction_id: transaction(5)
        }
    );

    let later_target = entry(9, 11, 26, &[(1, 1, 3), (2, 1, -3)]);
    let premature_reversal =
        JournalEntry::reversal(transaction(10), 10, date(11), timestamp(25), &later_target)
            .unwrap();
    let record_count = ledger.record_count();
    assert_eq!(
        ledger.post_batch(LedgerBatch::new(vec![premature_reversal, later_target]).unwrap()),
        Err(LedgerError::ReversalTargetMissing(transaction(9)))
    );
    assert_eq!(ledger.record_count(), record_count);
    ledger.validate().unwrap();
}

#[test]
fn batch_rejects_invalid_cardinality_duplicate_ids_and_partial_prior_commitment() {
    assert_eq!(
        LedgerBatch::new(vec![transfer(1, 1, 10)]),
        Err(LedgerError::BatchTooFewEntries)
    );
    assert_eq!(
        LedgerBatch::new(vec![transfer(1, 1, 10), transfer(1, 1, 10)]),
        Err(LedgerError::BatchDuplicateTransaction(transaction(1)))
    );
    assert_eq!(
        LedgerBatch::new(vec![transfer(1, 1, 11), transfer(2, 1, 10)]),
        Err(LedgerError::RecordedTimestampRegression {
            previous: timestamp(11),
            proposed: timestamp(10),
        })
    );

    let first = transfer(1, 10, 10);
    let second = transfer(2, 20, 11);
    let batch = LedgerBatch::new(vec![first.clone(), second.clone()]).unwrap();
    let mut ledger = Ledger::new();
    ledger.post(first).unwrap();
    assert_eq!(
        ledger.post_batch(batch.clone()),
        Err(LedgerError::BatchAlreadyPartiallyCommitted(transaction(1)))
    );
    assert_eq!(ledger.record_count(), 1);
    assert_eq!(ledger.entry_count(), 1);
    ledger.post(second).unwrap();
    assert_eq!(
        ledger.post_batch(batch),
        Err(LedgerError::BatchAlreadyPartiallyCommitted(transaction(1)))
    );
    assert_eq!(ledger.record_count(), 2);
    assert_eq!(ledger.entry_count(), 2);

    let collision = LedgerBatch::new(vec![transfer(1, 11, 10), transfer(3, 1, 12)]).unwrap();
    assert_eq!(
        ledger.post_batch(collision),
        Err(LedgerError::TransactionIdCollision(transaction(1)))
    );
}

#[test]
fn prepared_batch_is_generation_bound_and_commit_is_infallible_for_valid_generation() {
    let batch = LedgerBatch::new(vec![transfer(1, 10, 10), transfer(2, 20, 11)]).unwrap();
    let mut ledger = Ledger::new();
    let prepared = match ledger.prepare_batch(batch).unwrap() {
        BatchPreparation::Ready(prepared) => prepared,
        BatchPreparation::Replay(_) => panic!("new batch cannot replay"),
    };
    ledger.post(transfer(3, 1, 9)).unwrap();
    assert_eq!(
        ledger.commit_batch(prepared),
        Err(LedgerError::StalePreparation)
    );
    assert_eq!(ledger.record_count(), 1);
    assert_eq!(ledger.entry_count(), 1);
    assert_eq!(ledger.balance(account(1), asset(1)), 1);
}
