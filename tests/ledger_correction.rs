use quotick::codec::BinaryCodec;
use quotick::ledger::{
    CorrectionPreparation, JournalEntry, Ledger, LedgerCorrection, LedgerEntryKind, LedgerError,
    Posting,
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

fn entry(transaction_id: u64, amount: i128, day: i32, recorded_at: u64) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        date(day),
        timestamp(recorded_at),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(1),
                amount,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(1),
                amount: -amount,
            },
        ],
    )
    .unwrap()
}

fn correction(
    reversal_transaction_id: u64,
    replacement_transaction_id: u64,
    replacement_amount: i128,
    original: &JournalEntry,
) -> LedgerCorrection {
    LedgerCorrection::new(
        transaction(reversal_transaction_id),
        reversal_transaction_id,
        date(20),
        timestamp(200),
        entry(replacement_transaction_id, replacement_amount, 20, 201),
        original,
    )
    .unwrap()
}

#[test]
fn correction_is_one_idempotent_event_with_exact_reversal_and_replacement() {
    let original = entry(1, 100, 10, 100);
    let value = correction(2, 3, 70, &original);
    let mut ledger = Ledger::new();
    ledger.post(original).unwrap();

    let receipt = ledger.correct(value.clone()).unwrap();
    assert_eq!(receipt.sequence, 2);
    assert!(!receipt.replayed);
    assert_eq!(receipt.reversal_transaction_id, transaction(2));
    assert_eq!(receipt.replacement_transaction_id, transaction(3));
    assert_eq!(ledger.record_count(), 2);
    assert_eq!(ledger.entry_count(), 3);
    assert_eq!(ledger.balance(account(1), asset(1)), 70);
    assert_eq!(ledger.balance(account(2), asset(1)), -70);
    assert_eq!(ledger.reversal_for(transaction(1)), Some(transaction(2)));
    assert_eq!(
        ledger.transaction(transaction(3)).unwrap().kind(),
        LedgerEntryKind::Standard
    );

    let replay = ledger.correct(value).unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.sequence, 2);
    assert_eq!(ledger.record_count(), 2);
    assert_eq!(ledger.entry_count(), 3);
    ledger.validate().unwrap();

    let checkpoint = ledger.checkpoint().unwrap();
    assert_eq!(checkpoint.generation(), 2);
    assert_eq!(checkpoint.records().len(), 2);
    let decoded = quotick::ledger::LedgerCheckpoint::decode(&checkpoint.encode().unwrap()).unwrap();
    let mut restored = Ledger::from_checkpoint(&decoded).unwrap();
    assert!(
        restored
            .correct(correction(2, 3, 70, &entry(1, 100, 10, 100)))
            .unwrap()
            .replayed
    );
    assert_eq!(restored.record_count(), 2);
    restored.validate().unwrap();
}

#[test]
fn correction_rejects_nonstandard_replacement_and_nonunique_transactions() {
    let original = entry(1, 100, 10, 100);
    let replacement_target = entry(4, 80, 20, 201);
    let reversal_replacement = JournalEntry::reversal(
        transaction(5),
        5,
        date(20),
        timestamp(202),
        &replacement_target,
    )
    .unwrap();
    assert_eq!(
        LedgerCorrection::new(
            transaction(2),
            2,
            date(20),
            timestamp(200),
            reversal_replacement,
            &original,
        ),
        Err(LedgerError::CorrectionReplacementNotStandard(transaction(
            5
        )))
    );
    assert_eq!(
        LedgerCorrection::new(
            transaction(2),
            2,
            date(20),
            timestamp(200),
            entry(2, 80, 20, 201),
            &original,
        ),
        Err(LedgerError::CorrectionTransactionIdsNotDistinct {
            reversal_transaction_id: transaction(2),
            replacement_transaction_id: transaction(2),
        })
    );
    assert_eq!(
        LedgerCorrection::new(
            transaction(2),
            2,
            date(20),
            timestamp(202),
            entry(3, 80, 20, 201),
            &original,
        ),
        Err(LedgerError::RecordedTimestampRegression {
            previous: timestamp(202),
            proposed: timestamp(201),
        })
    );
}

#[test]
fn correction_validation_is_atomic_for_missing_closed_reversed_and_partial_state() {
    let original = entry(1, 100, 10, 100);
    let value = correction(2, 3, 70, &original);
    let mut ledger = Ledger::new();
    assert_eq!(
        ledger.correct(value.clone()),
        Err(LedgerError::ReversalTargetMissing(transaction(1)))
    );
    assert_eq!(ledger.record_count(), 0);

    ledger.post(original.clone()).unwrap();
    ledger
        .close_period(transaction(4), 4, timestamp(150), date(30))
        .unwrap();
    assert_eq!(
        ledger.correct(value.clone()),
        Err(LedgerError::AccountingPeriodClosed {
            effective_date: date(20),
            closed_through: date(30),
        })
    );
    assert_eq!(ledger.balance(account(1), asset(1)), 100);
    assert_eq!(ledger.record_count(), 2);

    ledger
        .reopen_period(transaction(5), 5, timestamp(160), None)
        .unwrap();
    ledger.post(value.reversal().clone()).unwrap();
    assert_eq!(
        ledger.correct(value),
        Err(LedgerError::CorrectionAlreadyPartiallyCommitted(
            transaction(2)
        ))
    );
    assert_eq!(ledger.balance(account(1), asset(1)), 0);
    assert_eq!(ledger.record_count(), 4);
}

#[test]
fn correction_combines_deltas_without_a_fallible_intermediate_balance() {
    let seed = JournalEntry::new(
        transaction(1),
        1,
        date(10),
        timestamp(100),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(1),
                amount: -1,
            },
            Posting {
                account_id: account(3),
                asset_id: asset(1),
                amount: 1,
            },
        ],
    )
    .unwrap();
    let original = JournalEntry::new(
        transaction(2),
        2,
        date(11),
        timestamp(110),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(1),
                amount: -i128::MAX,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(1),
                amount: i128::MAX,
            },
        ],
    )
    .unwrap();
    let replacement = JournalEntry::new(
        transaction(4),
        4,
        date(12),
        timestamp(121),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(1),
                amount: i128::MAX,
            },
            Posting {
                account_id: account(4),
                asset_id: asset(1),
                amount: -i128::MAX,
            },
        ],
    )
    .unwrap();
    let value = LedgerCorrection::new(
        transaction(3),
        3,
        date(12),
        timestamp(120),
        replacement,
        &original,
    )
    .unwrap();
    let mut ledger = Ledger::new();
    ledger.post(seed).unwrap();
    ledger.post(original).unwrap();
    assert_eq!(ledger.balance(account(1), asset(1)), i128::MIN);

    ledger.correct(value).unwrap();
    assert_eq!(ledger.balance(account(1), asset(1)), i128::MAX - 1);
    assert_eq!(ledger.balance(account(2), asset(1)), 0);
    assert_eq!(ledger.balance(account(3), asset(1)), 1);
    assert_eq!(ledger.balance(account(4), asset(1)), -i128::MAX);
    ledger.validate().unwrap();
}

#[test]
fn prepared_correction_is_generation_bound_and_cannot_commit_after_intervention() {
    let original = entry(1, 100, 10, 100);
    let value = correction(2, 3, 70, &original);
    let mut ledger = Ledger::new();
    ledger.post(original).unwrap();
    let prepared = match ledger.prepare_correction(value).unwrap() {
        CorrectionPreparation::Ready(prepared) => prepared,
        CorrectionPreparation::Replay(_) => panic!("new correction cannot replay"),
    };
    ledger.post(entry(4, 10, 20, 199)).unwrap();

    assert_eq!(
        ledger.commit_correction(prepared),
        Err(LedgerError::StalePreparation)
    );
    assert_eq!(ledger.balance(account(1), asset(1)), 110);
    assert_eq!(ledger.reversal_for(transaction(1)), None);
    assert_eq!(ledger.record_count(), 2);
}
