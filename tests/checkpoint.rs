use quotick::codec::{BinaryCodec, CodecError};
use quotick::ledger::{JournalEntry, Ledger, LedgerCheckpoint, LedgerCheckpointError, Posting};
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

fn account(value: u64) -> AccountId {
    AccountId::new(value).expect("account ID")
}

fn asset(value: u64) -> AssetId {
    AssetId::new(value).expect("asset ID")
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).expect("transaction ID")
}

fn entry(
    transaction_id: u64,
    asset_id: u64,
    debit_account: u64,
    credit_account: u64,
    amount: i128,
) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(0),
        vec![
            Posting {
                account_id: account(debit_account),
                asset_id: asset(asset_id),
                amount,
            },
            Posting {
                account_id: account(credit_account),
                asset_id: asset(asset_id),
                amount: -amount,
            },
        ],
    )
    .expect("entry balances")
}

fn populated_ledger() -> Ledger {
    let mut ledger = Ledger::new();
    ledger.post(entry(1, 1, 1, 2, 500)).expect("entry posts");
    ledger.post(entry(2, 1, 1, 3, -125)).expect("entry posts");
    ledger.post(entry(3, 2, 2, 3, 900)).expect("entry posts");
    ledger
}

#[test]
fn trial_balance_and_checkpoint_are_canonical_and_independently_replayable() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<LedgerCheckpoint>();
    let ledger = populated_ledger();
    ledger.validate().expect("ledger cross-audits");
    let trial = ledger.trial_balance();
    assert_eq!(trial.len(), 2);
    assert_eq!(trial[0].asset_id(), asset(1));
    assert_eq!(trial[0].positive_total().as_u128(), Some(500));
    assert_eq!(trial[0].negative_total().as_u128(), Some(500));
    assert!(trial[0].is_balanced());
    assert_eq!(trial[1].asset_id(), asset(2));
    assert_eq!(trial[1].positive_total().as_u128(), Some(900));
    assert_eq!(trial[1].negative_total().as_u128(), Some(900));

    let checkpoint = ledger.checkpoint().expect("checkpoint captures");
    assert_eq!(checkpoint.generation(), 3);
    assert_eq!(checkpoint.records().len(), 3);
    assert_eq!(checkpoint.balances().len(), 5);
    assert!(checkpoint.balances().windows(2).all(|pair| (
        pair[0].asset_id(),
        pair[0].account_id()
    ) < (
        pair[1].asset_id(),
        pair[1].account_id()
    )));

    let shared = checkpoint.clone();
    assert!(shared.shares_balance_storage_with(&checkpoint));
    assert!(shared.shares_record_storage_with(&checkpoint));

    let encoded = checkpoint.encode().expect("checkpoint encodes");
    assert_eq!(encoded.len(), 524);
    let decoded = LedgerCheckpoint::decode(&encoded).expect("checkpoint decodes");
    assert_eq!(decoded, checkpoint);
    assert!(!decoded.shares_balance_storage_with(&checkpoint));
    assert!(!decoded.shares_record_storage_with(&checkpoint));
    drop(checkpoint);
    drop(ledger);
    let mut restored = Ledger::from_checkpoint(&shared).expect("checkpoint restores");
    restored.validate().expect("restored ledger validates");
    assert_eq!(restored.balance(account(1), asset(1)), 375);
    assert_eq!(restored.balance(account(2), asset(1)), -500);
    assert_eq!(restored.balance(account(3), asset(1)), 125);
    assert!(restored.post(entry(1, 1, 1, 2, 500)).unwrap().replayed);
}

#[test]
fn checkpoint_decoder_rejects_generation_balance_and_order_corruption() {
    let checkpoint = populated_ledger()
        .checkpoint()
        .expect("checkpoint captures");
    let encoded = checkpoint.encode().expect("checkpoint encodes");

    let mut generation_mismatch = encoded.clone();
    generation_mismatch[0..8].copy_from_slice(&4_u64.to_le_bytes());
    assert!(matches!(
        LedgerCheckpoint::decode(&generation_mismatch),
        Err(CodecError::InvalidLedgerCheckpoint(_))
    ));

    let mut balance_mismatch = encoded.clone();
    balance_mismatch[28] ^= 1;
    assert!(matches!(
        LedgerCheckpoint::decode(&balance_mismatch),
        Err(CodecError::InvalidLedgerCheckpoint(_))
    ));

    let mut impossible_entry_count = encoded.clone();
    impossible_entry_count[172..176].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        LedgerCheckpoint::decode(&impossible_entry_count),
        Err(CodecError::InvalidLength {
            field: "ledger checkpoint records",
            ..
        })
    ));

    let mut duplicate_transaction = encoded.clone();
    let first_entry = duplicate_transaction[180..292].to_vec();
    duplicate_transaction[296..408].copy_from_slice(&first_entry);
    assert!(matches!(
        LedgerCheckpoint::decode(&duplicate_transaction),
        Err(CodecError::InvalidLedgerCheckpoint(
            LedgerCheckpointError::DuplicateTransaction {
                index: 1,
                transaction_id
            }
        )) if transaction_id == transaction(1)
    ));

    let mut noncanonical = encoded;
    let first_balance = noncanonical[12..44].to_vec();
    let second_balance = noncanonical[44..76].to_vec();
    noncanonical[12..44].copy_from_slice(&second_balance);
    noncanonical[44..76].copy_from_slice(&first_balance);
    assert!(matches!(
        LedgerCheckpoint::decode(&noncanonical),
        Err(CodecError::InvalidLedgerCheckpoint(_))
    ));
}

#[test]
fn zero_balances_are_omitted_without_losing_idempotency_history() {
    let first = entry(1, 1, 1, 2, 10);
    let second = entry(2, 1, 2, 1, 10);
    let mut ledger = Ledger::new();
    ledger.post(first.clone()).expect("entry posts");
    ledger.post(second).expect("entry posts");
    let checkpoint = ledger.checkpoint().expect("checkpoint captures");
    assert!(checkpoint.balances().is_empty());
    assert_eq!(checkpoint.records().len(), 2);
    let mut restored = Ledger::from_checkpoint(&checkpoint).expect("checkpoint restores");
    assert!(restored.post(first).expect("retry resolves").replayed);
    assert_eq!(restored.entry_count(), 2);
}
