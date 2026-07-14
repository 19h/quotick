use std::collections::HashMap;
use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable_ledger::{DurableLedger, DurableLedgerError};
use quotick::journal::{Durability, JournalOptions};
use quotick::ledger::{
    JournalEntry, Ledger, LedgerBatch, LedgerCheckpointError, LedgerConstructionError, LedgerError,
    LedgerHashIndex, LedgerLimitError, LedgerLimitsSpec, LedgerResource, Posting,
};
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn asset(value: u64) -> AssetId {
    AssetId::new(value).unwrap()
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).unwrap()
}

fn transfer(
    transaction_id: u64,
    recorded_at: u64,
    debit: u64,
    credit: u64,
    amount: i128,
) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(recorded_at),
        vec![
            Posting {
                account_id: account(debit),
                asset_id: asset(1),
                amount,
            },
            Posting {
                account_id: account(credit),
                asset_id: asset(1),
                amount: -amount,
            },
        ],
    )
    .unwrap()
}

fn limits() -> LedgerLimitsSpec {
    LedgerLimitsSpec {
        max_balance_keys: 2,
        max_transactions: 2_048,
        max_reversals: 1_024,
        max_records: 2_048,
        max_postings_per_transaction: 4,
        max_transactions_per_record: 8,
        max_retained_postings: 4_096,
    }
}

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

#[test]
fn invalid_limits_are_typed_before_any_state_exists() {
    let mut spec = limits();
    spec.max_balance_keys = 0;
    assert!(matches!(
        Ledger::try_with_limits(spec),
        Err(LedgerConstructionError::InvalidLimits(
            LedgerLimitError::ZeroMaximum(LedgerResource::BalanceKeys)
        ))
    ));

    let mut spec = limits();
    spec.max_transactions_per_record = spec.max_transactions + 1;
    assert!(matches!(
        Ledger::try_with_limits(spec),
        Err(LedgerConstructionError::InvalidLimits(
            LedgerLimitError::TransactionsPerRecordExceedTransactions { .. }
        ))
    ));

    let mut spec = limits();
    spec.max_postings_per_transaction = spec.max_retained_postings + 1;
    assert!(matches!(
        Ledger::try_with_limits(spec),
        Err(LedgerConstructionError::InvalidLimits(
            LedgerLimitError::PostingsPerTransactionExceedRetainedPostings { .. }
        ))
    ));
}

#[test]
fn exact_final_balance_cardinality_reuses_zeroed_slots_atomically() {
    let mut ledger = Ledger::try_with_limits(limits()).unwrap();
    ledger.post(transfer(1, 1, 1, 2, 10)).unwrap();
    assert_eq!(ledger.nonzero_balance_count(), 2);

    let rotate = JournalEntry::new(
        transaction(2),
        2,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(2),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(1),
                amount: -10,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(1),
                amount: 10,
            },
            Posting {
                account_id: account(3),
                asset_id: asset(1),
                amount: 7,
            },
            Posting {
                account_id: account(4),
                asset_id: asset(1),
                amount: -7,
            },
        ],
    )
    .unwrap();
    ledger.post(rotate).unwrap();

    assert_eq!(ledger.nonzero_balance_count(), 2);
    assert_eq!(ledger.balance(account(1), asset(1)), 0);
    assert_eq!(ledger.balance(account(2), asset(1)), 0);
    assert_eq!(ledger.balance(account(3), asset(1)), 7);
    assert_eq!(ledger.balance(account(4), asset(1)), -7);
    ledger.validate().unwrap();
}

#[test]
fn every_semantic_capacity_fails_before_mutation_and_exact_retry_still_wins() {
    let mut record_limited = limits();
    record_limited.max_records = 1;
    let mut ledger = Ledger::try_with_limits(record_limited).unwrap();
    let first = transfer(1, 1, 1, 2, 10);
    ledger.post(first.clone()).unwrap();
    assert!(ledger.post(first).unwrap().replayed);
    assert_eq!(
        ledger.post(transfer(2, 2, 1, 2, 1)),
        Err(LedgerError::CapacityExceeded {
            resource: LedgerResource::Records,
            maximum: 1,
            attempted: 2,
        })
    );
    assert_eq!(ledger.record_count(), 1);
    assert_eq!(ledger.entry_count(), 1);
    assert!(matches!(
        ledger.post(transfer(1, 2, 1, 2, 11)),
        Err(LedgerError::TransactionIdCollision(_))
    ));

    let mut transaction_limited = limits();
    transaction_limited.max_transactions = 1;
    transaction_limited.max_reversals = 1;
    transaction_limited.max_records = 1;
    transaction_limited.max_transactions_per_record = 1;
    let mut ledger = Ledger::try_with_limits(transaction_limited).unwrap();
    ledger.post(transfer(1, 1, 1, 2, 10)).unwrap();
    assert_eq!(
        ledger.post(transfer(2, 2, 1, 2, 1)),
        Err(LedgerError::CapacityExceeded {
            resource: LedgerResource::Transactions,
            maximum: 1,
            attempted: 2,
        })
    );

    let mut posting_limited = limits();
    posting_limited.max_retained_postings = 2;
    posting_limited.max_postings_per_transaction = 2;
    let mut ledger = Ledger::try_with_limits(posting_limited).unwrap();
    ledger.post(transfer(1, 1, 1, 2, 10)).unwrap();
    assert_eq!(
        ledger.post(transfer(2, 2, 1, 2, 1)),
        Err(LedgerError::CapacityExceeded {
            resource: LedgerResource::RetainedPostings,
            maximum: 2,
            attempted: 4,
        })
    );
    assert_eq!(ledger.balance(account(1), asset(1)), 10);
    ledger.validate().unwrap();
}

#[test]
fn balance_and_per_transaction_limits_are_independent_and_atomic() {
    let mut ledger = Ledger::try_with_limits(limits()).unwrap();
    ledger.post(transfer(1, 1, 1, 2, 10)).unwrap();
    assert_eq!(
        ledger.post(transfer(2, 2, 3, 4, 1)),
        Err(LedgerError::CapacityExceeded {
            resource: LedgerResource::BalanceKeys,
            maximum: 2,
            attempted: 4,
        })
    );
    assert_eq!(ledger.record_count(), 1);
    assert_eq!(ledger.balance(account(1), asset(1)), 10);
    assert_eq!(ledger.balance(account(3), asset(1)), 0);

    let mut per_transaction = limits();
    per_transaction.max_postings_per_transaction = 2;
    let four_legs = JournalEntry::new(
        transaction(1),
        1,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(1),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(1),
                amount: 1,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(1),
                amount: -1,
            },
            Posting {
                account_id: account(1),
                asset_id: asset(2),
                amount: 1,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(2),
                amount: -1,
            },
        ],
    )
    .unwrap();
    let mut ledger = Ledger::try_with_limits(per_transaction).unwrap();
    assert_eq!(
        ledger.post(four_legs),
        Err(LedgerError::CapacityExceeded {
            resource: LedgerResource::PostingsPerTransaction,
            maximum: 2,
            attempted: 4,
        })
    );
    assert_eq!(ledger.record_count(), 0);
}

#[test]
fn record_and_reversal_limits_are_independent_and_atomic() {
    let mut per_record = limits();
    per_record.max_transactions_per_record = 1;
    let mut ledger = Ledger::try_with_limits(per_record).unwrap();
    let batch = LedgerBatch::new(vec![transfer(1, 1, 1, 2, 1), transfer(2, 2, 1, 2, 1)]).unwrap();
    assert_eq!(
        ledger.post_batch(batch),
        Err(LedgerError::CapacityExceeded {
            resource: LedgerResource::TransactionsPerRecord,
            maximum: 1,
            attempted: 2,
        })
    );
    assert_eq!(ledger.record_count(), 0);

    let mut reversal_limited = limits();
    reversal_limited.max_reversals = 1;
    let original_one = transfer(1, 1, 1, 2, 5);
    let original_two = transfer(2, 2, 1, 2, 7);
    let mut ledger = Ledger::try_with_limits(reversal_limited).unwrap();
    ledger.post(original_one.clone()).unwrap();
    ledger.post(original_two.clone()).unwrap();
    let reversal_one = JournalEntry::reversal(
        transaction(3),
        3,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(3),
        &original_one,
    )
    .unwrap();
    ledger.post(reversal_one).unwrap();
    let reversal_two = JournalEntry::reversal(
        transaction(4),
        4,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(4),
        &original_two,
    )
    .unwrap();
    assert_eq!(
        ledger.post(reversal_two),
        Err(LedgerError::CapacityExceeded {
            resource: LedgerResource::Reversals,
            maximum: 1,
            attempted: 2,
        })
    );
    assert_eq!(ledger.record_count(), 3);
    assert_eq!(ledger.balance(account(1), asset(1)), 7);
    ledger.validate().unwrap();
}

#[test]
fn checkpoint_restore_uses_the_selected_envelope_and_constructor_fails_exactly() {
    let mut ledger = Ledger::try_with_limits(limits()).unwrap();
    ledger.post(transfer(1, 1, 1, 2, 10)).unwrap();
    let checkpoint = ledger.checkpoint().unwrap();

    let mut insufficient = limits();
    insufficient.max_postings_per_transaction = 1;
    assert!(matches!(
        Ledger::from_checkpoint_with_limits(&checkpoint, insufficient),
        Err(LedgerCheckpointError::RecordReplay {
            index: 0,
            error: LedgerError::CapacityExceeded {
                resource: LedgerResource::PostingsPerTransaction,
                maximum: 1,
                attempted: 2,
            },
        })
    ));
    let restored = Ledger::from_checkpoint_with_limits(&checkpoint, limits()).unwrap();
    assert_eq!(restored.balance(account(1), asset(1)), 10);
    restored.validate().unwrap();

    let mut unrepresentable = limits();
    unrepresentable.max_balance_keys = usize::MAX;
    assert!(matches!(
        Ledger::try_with_limits(unrepresentable),
        Err(LedgerConstructionError::ReservationFailed {
            resource: LedgerResource::BalanceKeys,
            maximum: usize::MAX,
        })
    ));
}

#[test]
fn sustained_balance_identity_churn_never_moves_authoritative_allocations() {
    const CYCLES: u64 = 1_000;
    let mut ledger = Ledger::try_with_limits(limits()).unwrap();
    let indexes = [
        LedgerHashIndex::BalanceKeys,
        LedgerHashIndex::Transactions,
        LedgerHashIndex::Reversals,
    ];
    let initial = indexes.map(|index| ledger.hash_index_status(index));
    let journal = ledger.journal_status();

    for cycle in 0..CYCLES {
        let (left, right) = if cycle % 2 == 0 { (1, 2) } else { (2, 1) };
        ledger
            .post(transfer(cycle + 1, cycle + 1, left, right, 1))
            .unwrap();
        if cycle % 100 == 99 {
            ledger.validate().unwrap();
        }
    }

    for (index, expected) in indexes.into_iter().zip(initial) {
        let actual = ledger.hash_index_status(index);
        assert_eq!(actual.maximum_entries, expected.maximum_entries);
        assert_eq!(actual.allocated_entries, expected.allocated_entries);
        assert_eq!(actual.initialized_buckets, expected.initialized_buckets);
    }
    let actual_journal = ledger.journal_status();
    assert_eq!(actual_journal.maximum_records, journal.maximum_records);
    assert_eq!(actual_journal.allocated_records, journal.allocated_records);
    assert_eq!(ledger.nonzero_balance_count(), 0);
    assert_eq!(ledger.retained_posting_count(), 2_000);
    ledger.validate().unwrap();
}

#[test]
fn durable_capacity_failure_precedes_wal_growth_and_reopen_enforces_limits() {
    let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "quotick-ledger-limits-{}-{nonce}.wal",
        std::process::id()
    ));
    let _ = fs::remove_file(&path);

    let mut spec = limits();
    spec.max_records = 1;
    let first = transfer(1, 1, 1, 2, 10);
    let mut durable = DurableLedger::open_with_limits(&path, options(), spec).unwrap();
    durable.post(first.clone()).unwrap();
    let length = fs::metadata(&path).unwrap().len();
    assert!(matches!(
        durable.post(transfer(2, 2, 1, 2, 1)),
        Err(DurableLedgerError::Ledger(LedgerError::CapacityExceeded {
            resource: LedgerResource::Records,
            maximum: 1,
            attempted: 2,
        }))
    ));
    assert_eq!(fs::metadata(&path).unwrap().len(), length);
    drop(durable);

    let mut reopened = DurableLedger::open_with_limits(&path, options(), spec).unwrap();
    assert!(reopened.post(first).unwrap().replayed);
    drop(reopened);

    let mut insufficient = spec;
    insufficient.max_postings_per_transaction = 1;
    assert!(matches!(
        DurableLedger::open_with_limits(&path, options(), insufficient),
        Err(DurableLedgerError::Ledger(LedgerError::CapacityExceeded {
            resource: LedgerResource::PostingsPerTransaction,
            maximum: 1,
            attempted: 2,
        }))
    ));
    let _ = fs::remove_file(path);

    let invalid_path = std::env::temp_dir().join(format!(
        "quotick-ledger-invalid-limits-{}-{nonce}.wal",
        std::process::id()
    ));
    let _ = fs::remove_file(&invalid_path);
    let mut invalid = limits();
    invalid.max_balance_keys = 0;
    assert!(matches!(
        DurableLedger::open_with_limits(&invalid_path, options(), invalid),
        Err(DurableLedgerError::Construction(
            LedgerConstructionError::InvalidLimits(LedgerLimitError::ZeroMaximum(
                LedgerResource::BalanceKeys
            ))
        ))
    ));
    assert!(!invalid_path.exists());
}

#[test]
fn generated_atomic_batches_match_a_literal_balance_model_without_growth() {
    const BATCHES: u64 = 1_000;
    let spec = LedgerLimitsSpec {
        max_balance_keys: 16,
        max_transactions: 8_000,
        max_reversals: 1,
        max_records: 1_000,
        max_postings_per_transaction: 2,
        max_transactions_per_record: 8,
        max_retained_postings: 16_000,
    };
    let mut ledger = Ledger::try_with_limits(spec).unwrap();
    let indexes = [
        LedgerHashIndex::BalanceKeys,
        LedgerHashIndex::Transactions,
        LedgerHashIndex::Reversals,
    ];
    let initial = indexes.map(|index| ledger.hash_index_status(index));
    let journal = ledger.journal_status();
    let mut model = HashMap::<u64, i128>::new();
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next_transaction = 1_u64;

    for batch_index in 0..BATCHES {
        state ^= state << 7;
        state ^= state >> 9;
        state ^= state << 8;
        let transaction_count = usize::try_from(state % 7 + 2).unwrap();
        let mut entries = Vec::with_capacity(transaction_count);
        let mut effects = Vec::with_capacity(transaction_count);
        for _ in 0..transaction_count {
            state ^= state << 7;
            state ^= state >> 9;
            state ^= state << 8;
            let debit = state % 16 + 1;
            state ^= state << 7;
            state ^= state >> 9;
            state ^= state << 8;
            let mut credit = state % 16 + 1;
            if credit == debit {
                credit = credit % 16 + 1;
            }
            let amount = i128::from(state % 100 + 1);
            entries.push(transfer(
                next_transaction,
                next_transaction,
                debit,
                credit,
                amount,
            ));
            effects.push((debit, credit, amount));
            next_transaction += 1;
        }
        ledger
            .post_batch(LedgerBatch::new(entries).unwrap())
            .unwrap();
        for (debit, credit, amount) in effects {
            *model.entry(debit).or_default() += amount;
            *model.entry(credit).or_default() -= amount;
        }
        if batch_index % 100 == 99 {
            for account_id in 1..=16 {
                assert_eq!(
                    ledger.balance(account(account_id), asset(1)),
                    model.get(&account_id).copied().unwrap_or(0)
                );
            }
            ledger.validate().unwrap();
        }
    }

    for (index, expected) in indexes.into_iter().zip(initial) {
        let actual = ledger.hash_index_status(index);
        assert_eq!(actual.allocated_entries, expected.allocated_entries);
        assert_eq!(actual.initialized_buckets, expected.initialized_buckets);
    }
    assert_eq!(
        ledger.journal_status().allocated_records,
        journal.allocated_records
    );
    assert_eq!(ledger.record_count(), 1_000);
    ledger.validate().unwrap();
}
