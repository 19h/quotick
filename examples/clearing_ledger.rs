//! Atomic funding, DVP settlement, correction, period control, and reconciliation.

mod support;

use quotick::ledger::{
    JournalEntry, LedgerBatch, LedgerCorrection, LedgerError, Posting, ReconciliationBalance,
    ReconciliationStatement,
};
use quotick::matching::Trade;
use quotick::{AccountingDate, OrderId, Price, Quantity, ReconciliationId, TradeId, TransactionId};

use support::{account, asset, definition, ledger, timestamp};

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).unwrap()
}

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

#[allow(
    clippy::too_many_lines,
    reason = "one chronological example retains the complete accounting lifecycle"
)]
fn main() {
    let definition = definition("CLEARING-LEDGER");
    let date = AccountingDate::from_days_since_unix_epoch(20_000);
    let prior_date = AccountingDate::from_days_since_unix_epoch(19_999);
    let mut ledger = ledger();

    let cash_funding = transfer(1, 10, date, 99, 1, 2, 100_000);
    let inventory_funding = transfer(2, 11, date, 99, 2, 1, 100);
    let funding = LedgerBatch::new(vec![cash_funding.clone(), inventory_funding]).unwrap();
    let funding_receipt = ledger.post_batch(funding.clone()).unwrap();
    assert_eq!(funding_receipt.sequence, 1);
    assert_eq!(funding_receipt.transaction_count, 2);
    assert!(ledger.post_batch(funding).unwrap().replayed);

    let trade = Trade {
        trade_id: TradeId::new(1).unwrap(),
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        price: Price::from_raw(100),
        quantity: Quantity::new(10).unwrap(),
        buy_order_id: OrderId::new(1).unwrap(),
        sell_order_id: OrderId::new(2).unwrap(),
        buyer_account_id: account(1),
        seller_account_id: account(2),
        maker_order_id: OrderId::new(2).unwrap(),
        taker_order_id: OrderId::new(1).unwrap(),
    };
    ledger
        .settle_instrument_trade(transaction(3), date, timestamp(20), &trade, definition)
        .unwrap();

    let corrected_funding = transfer(5, 31, date, 99, 1, 2, 95_000);
    let correction = LedgerCorrection::new(
        transaction(4),
        4,
        date,
        timestamp(30),
        corrected_funding,
        &cash_funding,
    )
    .unwrap();
    let correction_receipt = ledger.correct(correction.clone()).unwrap();
    assert_eq!(correction_receipt.sequence, 3);
    assert!(ledger.correct(correction).unwrap().replayed);

    ledger
        .close_period(transaction(6), 6, timestamp(40), date)
        .unwrap();
    let rejected = transfer(7, 50, date, 2, 1, 2, 100);
    assert_eq!(
        ledger.post(rejected),
        Err(LedgerError::AccountingPeriodClosed {
            effective_date: date,
            closed_through: date,
        })
    );
    ledger
        .reopen_period(transaction(8), 8, timestamp(50), Some(prior_date))
        .unwrap();
    ledger.post(transfer(9, 60, date, 2, 1, 2, 100)).unwrap();

    let statement = ReconciliationStatement::new(
        ReconciliationId::new(1).unwrap(),
        u64::try_from(ledger.record_count()).unwrap(),
        timestamp(70),
        7001,
        vec![
            ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(1),
                amount: 10,
            },
            ReconciliationBalance {
                account_id: account(2),
                asset_id: asset(1),
                amount: 90,
            },
            ReconciliationBalance {
                account_id: account(99),
                asset_id: asset(1),
                amount: -100,
            },
            ReconciliationBalance {
                account_id: account(1),
                asset_id: asset(2),
                amount: 94_100,
            },
            ReconciliationBalance {
                account_id: account(2),
                asset_id: asset(2),
                amount: 900,
            },
            ReconciliationBalance {
                account_id: account(99),
                asset_id: asset(2),
                amount: -95_000,
            },
        ],
    )
    .unwrap();
    let reconciliation = ledger.reconcile(&statement).unwrap();
    assert!(reconciliation.is_reconciled());
    assert_eq!(reconciliation.compared_balances(), 6);

    let trial_balance = ledger.try_trial_balance().unwrap();
    assert_eq!(trial_balance.len(), 2);
    assert!(
        trial_balance
            .iter()
            .all(quotick::ledger::AssetTrialBalance::is_balanced)
    );
    assert_eq!(ledger.closed_through(), Some(prior_date));
    ledger.validate().unwrap();

    println!(
        "records={} transactions={} reconciled={} trial_assets={}",
        ledger.record_count(),
        ledger.entry_count(),
        reconciliation.is_reconciled(),
        trial_balance.len()
    );
}
