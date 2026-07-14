use quotick::ledger::{JournalEntry, Ledger, LedgerEntryKind, LedgerError, Posting};
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

fn date(day: i32) -> AccountingDate {
    AccountingDate::from_days_since_unix_epoch(day)
}

fn timestamp(value: u64) -> TimestampNs {
    TimestampNs::from_unix_nanos(value)
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).unwrap()
}

fn entry(transaction_id: u64, effective_day: i32, recorded_at: u64) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        date(effective_day),
        timestamp(recorded_at),
        vec![
            Posting {
                account_id: AccountId::new(1).unwrap(),
                asset_id: AssetId::new(1).unwrap(),
                amount: 100,
            },
            Posting {
                account_id: AccountId::new(2).unwrap(),
                asset_id: AssetId::new(1).unwrap(),
                amount: -100,
            },
        ],
    )
    .unwrap()
}

#[test]
fn period_close_blocks_backdating_and_reopen_moves_the_durable_fence() {
    let mut ledger = Ledger::new();
    ledger.post(entry(1, 10, 100)).unwrap();
    let close = ledger
        .close_period(transaction(2), 2, timestamp(110), date(30))
        .unwrap();
    assert_eq!(close.sequence, 2);
    assert_eq!(ledger.closed_through(), Some(date(30)));
    assert_eq!(
        ledger.transaction(transaction(2)).unwrap().kind(),
        LedgerEntryKind::PeriodClose {
            closed_through: date(30)
        }
    );
    assert_eq!(
        ledger.post(entry(3, 30, 120)),
        Err(LedgerError::AccountingPeriodClosed {
            effective_date: date(30),
            closed_through: date(30),
        })
    );
    assert_eq!(ledger.entry_count(), 2);
    ledger.post(entry(4, 31, 120)).unwrap();

    let reopen = ledger
        .reopen_period(transaction(5), 5, timestamp(130), Some(date(15)))
        .unwrap();
    assert_eq!(reopen.sequence, 4);
    assert_eq!(ledger.closed_through(), Some(date(15)));
    assert_eq!(
        ledger.post(entry(6, 15, 140)),
        Err(LedgerError::AccountingPeriodClosed {
            effective_date: date(15),
            closed_through: date(15),
        })
    );
    ledger.post(entry(7, 16, 140)).unwrap();

    ledger
        .reopen_period(transaction(8), 8, timestamp(150), None)
        .unwrap();
    assert_eq!(ledger.closed_through(), None);
    ledger.post(entry(9, -10, 160)).unwrap();
    ledger.validate().unwrap();
}

#[test]
fn close_and_reopen_controls_are_monotonic_idempotent_state_transitions() {
    let mut ledger = Ledger::new();
    let first = ledger
        .close_period(transaction(1), 1, timestamp(10), date(30))
        .unwrap();
    assert!(!first.replayed);
    assert!(
        ledger
            .close_period(transaction(1), 1, timestamp(10), date(30))
            .unwrap()
            .replayed
    );
    assert_eq!(
        ledger.close_period(transaction(2), 2, timestamp(11), date(30)),
        Err(LedgerError::PeriodCloseNotAdvancing {
            current_closed_through: Some(date(30)),
            proposed_closed_through: date(30),
        })
    );
    assert_eq!(
        ledger.reopen_period(transaction(3), 3, timestamp(11), Some(date(31))),
        Err(LedgerError::InvalidPeriodReopen {
            current_closed_through: date(30),
            proposed_closed_through: Some(date(31)),
        })
    );
    ledger
        .reopen_period(transaction(4), 4, timestamp(12), None)
        .unwrap();
    let stale_exact_retry = ledger
        .close_period(transaction(1), 1, timestamp(10), date(30))
        .unwrap();
    assert!(stale_exact_retry.replayed);
    assert_eq!(stale_exact_retry.sequence, 1);
    assert_eq!(ledger.closed_through(), None);
    assert_eq!(
        ledger.reopen_period(transaction(5), 5, timestamp(13), None),
        Err(LedgerError::AccountingPeriodAlreadyOpen)
    );
    assert_eq!(ledger.entry_count(), 2);
}

#[test]
fn recording_time_is_monotonic_and_reversal_uses_an_open_effective_date() {
    let mut ledger = Ledger::new();
    let original = entry(1, 10, 100);
    ledger.post(original).unwrap();
    ledger
        .close_period(transaction(2), 2, timestamp(110), date(30))
        .unwrap();
    assert_eq!(
        ledger.post(entry(3, 31, 109)),
        Err(LedgerError::RecordedTimestampRegression {
            previous: timestamp(110),
            proposed: timestamp(109),
        })
    );
    assert_eq!(
        ledger.reverse(transaction(4), 4, date(10), timestamp(120), transaction(1)),
        Err(LedgerError::AccountingPeriodClosed {
            effective_date: date(10),
            closed_through: date(30),
        })
    );
    ledger
        .reverse(transaction(5), 5, date(31), timestamp(120), transaction(1))
        .unwrap();
    assert_eq!(
        ledger.balance(AccountId::new(1).unwrap(), AssetId::new(1).unwrap()),
        0
    );
    assert_eq!(ledger.last_recorded_at(), Some(timestamp(120)));
    ledger.validate().unwrap();
}

#[test]
fn administrative_entries_cannot_be_reversed_as_financial_postings() {
    let mut ledger = Ledger::new();
    ledger
        .close_period(transaction(1), 1, timestamp(10), date(30))
        .unwrap();
    assert_eq!(
        ledger.reverse(transaction(2), 2, date(31), timestamp(11), transaction(1),),
        Err(LedgerError::NonFinancialReversalTarget(transaction(1)))
    );
}
