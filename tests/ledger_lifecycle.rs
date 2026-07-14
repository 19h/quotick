use quotick::ledger::{
    JournalEntry, Ledger, LedgerEntryKind, LedgerError, Posting, ReconciliationBalance,
    ReconciliationError, ReconciliationStatement,
};
use quotick::{AccountId, AccountingDate, AssetId, ReconciliationId, TimestampNs, TransactionId};

fn account(value: u64) -> AccountId {
    AccountId::new(value).unwrap()
}

fn asset(value: u64) -> AssetId {
    AssetId::new(value).unwrap()
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

fn entry(transaction_id: u64, asset_id: u64, amount: i128) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        date(),
        time(),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(asset_id),
                amount,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(asset_id),
                amount: -amount,
            },
        ],
    )
    .unwrap()
}

#[test]
fn reversal_is_an_exact_idempotent_lineage_transition() {
    let original = entry(1, 1, 100);
    let reversal = JournalEntry::reversal(transaction(2), 200, date(), time(), &original).unwrap();
    assert_eq!(
        reversal.kind(),
        LedgerEntryKind::Reversal {
            reversed_transaction_id: transaction(1)
        }
    );
    assert_eq!(reversal.postings()[0].amount, -100);
    assert_eq!(reversal.postings()[1].amount, 100);

    let mut ledger = Ledger::new();
    ledger.post(original.clone()).unwrap();
    let receipt = ledger.post(reversal.clone()).unwrap();
    assert_eq!(receipt.sequence, 2);
    assert_eq!(ledger.balance(account(1), asset(1)), 0);
    assert_eq!(ledger.balance(account(2), asset(1)), 0);
    assert_eq!(ledger.reversal_for(transaction(1)), Some(transaction(2)));
    assert_eq!(
        ledger.reversed_transaction(transaction(2)),
        Some(transaction(1))
    );
    assert!(ledger.post(reversal.clone()).unwrap().replayed);
    ledger.validate().unwrap();

    let second = JournalEntry::reversal(transaction(3), 300, date(), time(), &original).unwrap();
    assert_eq!(
        ledger.post(second),
        Err(LedgerError::TransactionAlreadyReversed {
            original_transaction_id: transaction(1),
            reversal_transaction_id: transaction(2),
        })
    );

    let reinstatement =
        JournalEntry::reversal(transaction(4), 400, date(), time(), &reversal).unwrap();
    ledger.post(reinstatement).unwrap();
    assert_eq!(ledger.balance(account(1), asset(1)), 100);
    assert_eq!(ledger.reversal_for(transaction(2)), Some(transaction(4)));
    assert_eq!(
        ledger.reversed_transaction(transaction(4)),
        Some(transaction(2))
    );
    ledger.validate().unwrap();
}

#[test]
fn reversal_requires_an_existing_target_and_its_exact_posting_inverse() {
    let absent = entry(10, 1, 50);
    let absent_reversal =
        JournalEntry::reversal(transaction(11), 11, date(), time(), &absent).unwrap();
    let mut ledger = Ledger::new();
    assert_eq!(
        ledger.post(absent_reversal),
        Err(LedgerError::ReversalTargetMissing(transaction(10)))
    );

    let authoritative = entry(1, 1, 100);
    ledger.post(authoritative).unwrap();
    let conflicting_image = entry(1, 1, 200);
    let invalid =
        JournalEntry::reversal(transaction(2), 2, date(), time(), &conflicting_image).unwrap();
    assert_eq!(
        ledger.post(invalid),
        Err(LedgerError::InvalidReversalPostings(transaction(1)))
    );
    assert_eq!(ledger.entry_count(), 1);
    assert_eq!(ledger.balance(account(1), asset(1)), 100);
}

#[test]
fn reversal_of_an_i128_minimum_leg_fails_without_partial_construction() {
    let original = JournalEntry::new(
        transaction(1),
        1,
        date(),
        time(),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(1),
                amount: i128::MIN,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(1),
                amount: 1_i128 << 126,
            },
            Posting {
                account_id: account(3),
                asset_id: asset(1),
                amount: 1_i128 << 126,
            },
        ],
    )
    .unwrap();
    assert_eq!(
        JournalEntry::reversal(transaction(2), 2, date(), time(), &original),
        Err(LedgerError::NonReversibleAmount {
            original_transaction_id: transaction(1),
            account_id: account(1),
            asset_id: asset(1),
        })
    );
}

fn statement(generation: u64, balances: Vec<ReconciliationBalance>) -> ReconciliationStatement {
    ReconciliationStatement::new(
        ReconciliationId::new(7).unwrap(),
        generation,
        TimestampNs::from_unix_nanos(123),
        99,
        balances,
    )
    .unwrap()
}

#[test]
fn reconciliation_is_generation_bound_canonical_and_complete_by_asset() {
    let mut ledger = Ledger::new();
    ledger.post(entry(1, 1, 100)).unwrap();
    ledger.post(entry(2, 2, 50)).unwrap();

    let exact = statement(
        2,
        vec![
            ReconciliationBalance {
                account_id: account(2),
                asset_id: asset(2),
                amount: -50,
            },
            ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(1),
                amount: 100,
            },
            ReconciliationBalance {
                account_id: account(2),
                asset_id: asset(1),
                amount: -100,
            },
            ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(2),
                amount: 50,
            },
        ],
    );
    assert!(exact.balances().windows(2).all(
        |pair| (pair[0].asset_id, pair[0].account_id) < (pair[1].asset_id, pair[1].account_id)
    ));
    let report = ledger.reconcile(&exact).unwrap();
    assert!(report.is_reconciled());
    assert_eq!(report.compared_balances(), 4);
    assert!(report.differences().is_empty());

    let mismatched = statement(
        2,
        vec![
            ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(1),
                amount: 90,
            },
            ReconciliationBalance {
                account_id: account(2),
                asset_id: asset(1),
                amount: -90,
            },
            ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(2),
                amount: 50,
            },
            ReconciliationBalance {
                account_id: account(2),
                asset_id: asset(2),
                amount: -50,
            },
        ],
    );
    let report = ledger.reconcile(&mismatched).unwrap();
    assert!(!report.is_reconciled());
    assert_eq!(report.differences().len(), 2);
    assert_eq!(report.differences()[0].asset_id(), asset(1));
    assert_eq!(report.differences()[0].account_id(), account(1));
    assert_eq!(report.differences()[0].ledger_amount(), 100);
    assert_eq!(report.differences()[0].statement_amount(), 90);
    assert_eq!(report.differences()[0].difference(), -10);
}

#[test]
fn reconciliation_rejects_unbalanced_duplicate_zero_and_stale_statements() {
    let metadata = (
        ReconciliationId::new(7).unwrap(),
        TimestampNs::from_unix_nanos(123),
        99,
    );
    assert!(matches!(
        ReconciliationStatement::new(
            metadata.0,
            0,
            metadata.1,
            metadata.2,
            vec![ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(1),
                amount: 1,
            }]
        ),
        Err(ReconciliationError::Unbalanced { asset_id, .. }) if asset_id == asset(1)
    ));
    assert!(matches!(
        ReconciliationStatement::new(
            metadata.0,
            0,
            metadata.1,
            metadata.2,
            vec![
                ReconciliationBalance {
                    account_id: account(1),
                    asset_id: asset(1),
                    amount: 1,
                },
                ReconciliationBalance {
                    account_id: account(1),
                    asset_id: asset(1),
                    amount: -1,
                }
            ]
        ),
        Err(ReconciliationError::DuplicateAccountAsset { .. })
    ));
    assert!(matches!(
        ReconciliationStatement::new(
            metadata.0,
            0,
            metadata.1,
            metadata.2,
            vec![ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(1),
                amount: 0,
            }]
        ),
        Err(ReconciliationError::ZeroBalance { .. })
    ));

    let mut ledger = Ledger::new();
    ledger.post(entry(1, 1, 10)).unwrap();
    let stale = statement(
        0,
        vec![
            ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(1),
                amount: 10,
            },
            ReconciliationBalance {
                account_id: account(2),
                asset_id: asset(1),
                amount: -10,
            },
        ],
    );
    assert_eq!(
        ledger.reconcile(&stale),
        Err(ReconciliationError::GenerationMismatch {
            ledger_generation: 1,
            statement_generation: 0,
        })
    );
}

#[test]
fn reconciliation_observation_cannot_precede_its_claimed_journal_generation() {
    let mut ledger = Ledger::new();
    ledger
        .post(
            JournalEntry::new(
                transaction(1),
                1,
                date(),
                TimestampNs::from_unix_nanos(200),
                vec![
                    Posting {
                        account_id: account(1),
                        asset_id: asset(1),
                        amount: 10,
                    },
                    Posting {
                        account_id: account(2),
                        asset_id: asset(1),
                        amount: -10,
                    },
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let statement = ReconciliationStatement::new(
        ReconciliationId::new(8).unwrap(),
        1,
        TimestampNs::from_unix_nanos(199),
        100,
        vec![
            ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(1),
                amount: 10,
            },
            ReconciliationBalance {
                account_id: account(2),
                asset_id: asset(1),
                amount: -10,
            },
        ],
    )
    .unwrap();

    assert_eq!(
        ledger.reconcile(&statement),
        Err(ReconciliationError::ObservationPrecedesLedger {
            last_recorded_at: TimestampNs::from_unix_nanos(200),
            observed_at: TimestampNs::from_unix_nanos(199),
        })
    );
}

#[test]
fn reconciliation_difference_overflow_is_explicit_and_nonmutating() {
    let extreme = JournalEntry::new(
        transaction(1),
        1,
        date(),
        time(),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(1),
                amount: i128::MIN,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(1),
                amount: 1_i128 << 126,
            },
            Posting {
                account_id: account(3),
                asset_id: asset(1),
                amount: 1_i128 << 126,
            },
        ],
    )
    .unwrap();
    let mut ledger = Ledger::new();
    ledger.post(extreme).unwrap();
    let external = statement(
        1,
        vec![
            ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(1),
                amount: i128::MAX,
            },
            ReconciliationBalance {
                account_id: account(4),
                asset_id: asset(1),
                amount: -i128::MAX,
            },
        ],
    );
    assert_eq!(
        ledger.reconcile(&external),
        Err(ReconciliationError::DifferenceOverflow {
            account_id: account(1),
            asset_id: asset(1),
            ledger_amount: i128::MIN,
            statement_amount: i128::MAX,
        })
    );
    assert_eq!(ledger.balance(account(1), asset(1)), i128::MIN);
    assert_eq!(ledger.entry_count(), 1);
}
