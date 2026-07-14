use quotick::codec::BinaryCodec;
use quotick::ledger::{
    JournalEntry, Ledger, LedgerCheckpoint, LedgerError, LedgerMagnitude, Posting,
    ReconciliationBalance, ReconciliationError, ReconciliationStatement,
};
use quotick::{AccountId, AccountingDate, AssetId, ReconciliationId, TimestampNs, TransactionId};

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn asset() -> AssetId {
    AssetId::new(1).unwrap()
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).unwrap()
}

fn posting(account_id: u64, amount: i128) -> Posting {
    Posting {
        account_id: account(account_id),
        asset_id: asset(),
        amount,
    }
}

fn entry(transaction_id: u64, postings: Vec<Posting>) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(transaction_id),
        postings,
    )
    .unwrap()
}

fn three_maximum_side_total() -> LedgerMagnitude {
    let mut total = LedgerMagnitude::from_u128(i128::MAX as u128);
    total.add_u128(i128::MAX as u128);
    total.add_u128(i128::MAX as u128);
    total
}

#[test]
fn magnitude_keeps_u128_inline_then_spills_exactly_and_formats_decimal() {
    let inline = LedgerMagnitude::from_u128(500);
    assert_eq!(inline.as_u128(), Some(500));
    assert_eq!(inline.significant_bits(), Some(9));
    assert_eq!(inline.to_string(), "500");

    let mut boundary = LedgerMagnitude::from_u128(u128::MAX);
    boundary.add_u128(1);
    assert_eq!(boundary.significant_bits(), Some(129));
    assert_eq!(
        boundary.to_string(),
        "340282366920938463463374607431768211456"
    );
    boundary.add_u128(u128::MAX);
    assert_eq!(
        boundary.to_string(),
        "680564733841876926926749214863536422911"
    );

    let wide = three_maximum_side_total();
    assert_eq!(wide.as_u128(), None);
    assert_eq!(wide.significant_bits(), Some(129));
    assert_eq!(wide.to_string(), "510423550381407695195061911147652317181");
    let mut wider = wide.clone();
    wider.add_u128(i128::MAX as u128);
    assert!(wider > wide);
    assert_eq!(wider.to_string(), "680564733841876926926749214863536422908");
}

#[test]
fn one_balanced_entry_can_exceed_u128_side_totals_without_false_overflow() {
    let value = entry(
        1,
        vec![
            posting(1, i128::MAX),
            posting(2, i128::MAX),
            posting(3, i128::MAX),
            posting(4, -i128::MAX),
            posting(5, -i128::MAX),
            posting(6, -i128::MAX),
        ],
    );
    let mut ledger = Ledger::new();
    ledger.post(value).unwrap();
    ledger.validate().unwrap();

    let trial = ledger.trial_balance();
    assert_eq!(trial.len(), 1);
    assert_eq!(trial[0].asset_id(), asset());
    assert_eq!(trial[0].positive_total(), &three_maximum_side_total());
    assert_eq!(trial[0].negative_total(), &three_maximum_side_total());
    assert!(trial[0].is_balanced());

    let checkpoint = ledger.checkpoint().unwrap();
    let decoded = LedgerCheckpoint::decode(&checkpoint.encode().unwrap()).unwrap();
    let restored = Ledger::from_checkpoint(decoded).unwrap();
    restored.validate().unwrap();
    assert_eq!(restored.trial_balance(), trial);
}

#[test]
fn separately_valid_transfers_cross_the_old_u128_audit_boundary() {
    let mut ledger = Ledger::new();
    for transaction_id in 1..=3 {
        let positive_account = transaction_id * 2 - 1;
        let negative_account = transaction_id * 2;
        ledger
            .post(entry(
                transaction_id,
                vec![
                    posting(positive_account, i128::MAX),
                    posting(negative_account, -i128::MAX),
                ],
            ))
            .unwrap();
    }

    ledger.validate().unwrap();
    let trial = ledger.trial_balance();
    assert_eq!(trial[0].positive_total(), &three_maximum_side_total());
    assert_eq!(trial[0].negative_total(), &three_maximum_side_total());
}

#[test]
fn unbalanced_large_entry_reports_both_exact_side_magnitudes() {
    let error = JournalEntry::new(
        transaction(1),
        1,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(1),
        vec![
            posting(1, i128::MAX),
            posting(2, i128::MAX),
            posting(3, i128::MAX),
            posting(4, -i128::MAX),
            posting(5, -i128::MAX),
        ],
    )
    .unwrap_err();

    let LedgerError::Unbalanced {
        asset_id,
        positive_total,
        negative_total,
    } = error
    else {
        panic!("unexpected error: {error}");
    };
    assert_eq!(asset_id, asset());
    assert_eq!(*positive_total, three_maximum_side_total());
    assert_eq!(negative_total.as_u128(), Some(u128::MAX - 1));
}

#[test]
fn reconciliation_accepts_exact_balanced_sides_larger_than_u128() {
    let postings = vec![
        posting(1, i128::MAX),
        posting(2, i128::MAX),
        posting(3, i128::MAX),
        posting(4, -i128::MAX),
        posting(5, -i128::MAX),
        posting(6, -i128::MAX),
    ];
    let mut ledger = Ledger::new();
    ledger.post(entry(1, postings.clone())).unwrap();
    let statement = ReconciliationStatement::new(
        ReconciliationId::new(1).unwrap(),
        1,
        TimestampNs::from_unix_nanos(1),
        1,
        postings
            .into_iter()
            .map(|value| ReconciliationBalance {
                account_id: value.account_id,
                asset_id: value.asset_id,
                amount: value.amount,
            })
            .collect(),
    )
    .unwrap();

    assert!(ledger.reconcile(&statement).unwrap().is_reconciled());
}

#[test]
fn reconciliation_reports_unbalanced_wide_sides_without_numeric_overflow() {
    let error = ReconciliationStatement::new(
        ReconciliationId::new(1).unwrap(),
        0,
        TimestampNs::from_unix_nanos(1),
        1,
        vec![
            posting(1, i128::MAX),
            posting(2, i128::MAX),
            posting(3, i128::MAX),
            posting(4, -i128::MAX),
            posting(5, -i128::MAX),
        ]
        .into_iter()
        .map(|value| ReconciliationBalance {
            account_id: value.account_id,
            asset_id: value.asset_id,
            amount: value.amount,
        })
        .collect(),
    )
    .unwrap_err();

    let ReconciliationError::Unbalanced {
        asset_id,
        positive_total,
        negative_total,
    } = error
    else {
        panic!("unexpected error: {error}");
    };
    assert_eq!(asset_id, asset());
    assert_eq!(*positive_total, three_maximum_side_total());
    assert_eq!(negative_total.as_u128(), Some(u128::MAX - 1));
}
