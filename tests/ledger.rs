use quotick::ledger::{
    JournalEntry, Ledger, LedgerError, LedgerMagnitude, Posting, PostingPreparation,
    SettlementConvention,
};
use quotick::matching::Trade;
use quotick::{
    AccountId, AccountingDate, AssetId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity,
    TimestampNs, TradeId, TransactionId,
};

fn account(value: u64) -> AccountId {
    AccountId::new(value).expect("non-zero account")
}

fn asset(value: u64) -> AssetId {
    AssetId::new(value).expect("non-zero asset")
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).expect("non-zero transaction")
}

fn date() -> AccountingDate {
    AccountingDate::UNIX_EPOCH
}

fn time() -> TimestampNs {
    TimestampNs::from_unix_nanos(0)
}

fn trade(price: i64, quantity: u64) -> Trade {
    Trade {
        trade_id: TradeId::new(7).expect("trade id"),
        instrument_id: InstrumentId::new(9).expect("instrument id"),
        instrument_version: InstrumentVersion::new(1).expect("instrument version"),
        price: Price::from_raw(price),
        quantity: Quantity::new(quantity).expect("positive quantity"),
        buy_order_id: OrderId::new(1).expect("order id"),
        sell_order_id: OrderId::new(2).expect("order id"),
        buyer_account_id: account(11),
        seller_account_id: account(12),
        maker_order_id: OrderId::new(2).expect("order id"),
        taker_order_id: OrderId::new(1).expect("order id"),
    }
}

#[test]
fn unbalanced_entries_are_rejected_before_mutation() {
    let error = JournalEntry::new(
        transaction(1),
        1,
        date(),
        time(),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(1),
                amount: 10,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(1),
                amount: -9,
            },
        ],
    )
    .expect_err("residual must be rejected");
    assert_eq!(
        error,
        LedgerError::Unbalanced {
            asset_id: asset(1),
            positive_total: Box::new(LedgerMagnitude::from_u128(10)),
            negative_total: Box::new(LedgerMagnitude::from_u128(9)),
        }
    );
}

#[test]
fn trade_settlement_is_atomic_balanced_and_idempotent() {
    let mut ledger = Ledger::new();
    let convention = SettlementConvention {
        base_asset_id: asset(1),
        quote_asset_id: asset(2),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 100,
    };
    let execution = trade(25, 4);
    let receipt = ledger
        .settle_trade(transaction(7), date(), time(), &execution, convention)
        .expect("settlement succeeds");
    let replay = ledger
        .settle_trade(transaction(7), date(), time(), &execution, convention)
        .expect("settlement replay succeeds");

    assert_eq!(receipt.sequence, 1);
    assert!(!receipt.replayed);
    assert!(replay.replayed);
    assert_eq!(ledger.entry_count(), 1);
    assert_eq!(ledger.balance(account(11), asset(1)), 4);
    assert_eq!(ledger.balance(account(12), asset(1)), -4);
    assert_eq!(ledger.balance(account(11), asset(2)), -10_000);
    assert_eq!(ledger.balance(account(12), asset(2)), 10_000);
    assert_eq!(
        ledger.balance(account(11), asset(1)) + ledger.balance(account(12), asset(1)),
        0
    );
    assert_eq!(
        ledger.balance(account(11), asset(2)) + ledger.balance(account(12), asset(2)),
        0
    );
}

#[test]
fn negative_execution_price_reverses_quote_cashflow_without_losing_balance() {
    let mut ledger = Ledger::new();
    ledger
        .settle_trade(
            transaction(7),
            date(),
            time(),
            &trade(-5, 2),
            SettlementConvention {
                base_asset_id: asset(1),
                quote_asset_id: asset(2),
                base_units_per_lot: 1,
                quote_units_per_price_unit: 1,
            },
        )
        .expect("negative-price settlement succeeds");

    assert_eq!(ledger.balance(account(11), asset(1)), 2);
    assert_eq!(ledger.balance(account(12), asset(1)), -2);
    assert_eq!(ledger.balance(account(11), asset(2)), 10);
    assert_eq!(ledger.balance(account(12), asset(2)), -10);
}

#[test]
fn zero_price_execution_delivers_base_without_zero_quote_legs() {
    let mut ledger = Ledger::new();
    ledger
        .settle_trade(
            transaction(7),
            date(),
            time(),
            &trade(0, 3),
            SettlementConvention {
                base_asset_id: asset(1),
                quote_asset_id: asset(2),
                base_units_per_lot: 10,
                quote_units_per_price_unit: 1,
            },
        )
        .expect("zero-price settlement succeeds");

    assert_eq!(ledger.balance(account(11), asset(1)), 30);
    assert_eq!(ledger.balance(account(12), asset(1)), -30);
    assert_eq!(ledger.balance(account(11), asset(2)), 0);
    assert_eq!(ledger.entry_count(), 1);
}

#[test]
fn unrepresentable_opposite_notional_returns_error_without_panicking() {
    let mut ledger = Ledger::new();
    let extreme = trade(i64::MIN, 1_u64 << 63);
    let result = ledger.settle_trade(
        transaction(8),
        date(),
        time(),
        &extreme,
        SettlementConvention {
            base_asset_id: asset(1),
            quote_asset_id: asset(2),
            base_units_per_lot: 1,
            quote_units_per_price_unit: 2,
        },
    );

    assert_eq!(result, Err(LedgerError::ArithmeticOverflow));
    assert_eq!(ledger.entry_count(), 0);
}

#[test]
fn balance_overflow_aborts_all_legs() {
    let mut ledger = Ledger::new();
    let first = JournalEntry::new(
        transaction(1),
        1,
        date(),
        time(),
        vec![
            Posting {
                account_id: account(1),
                asset_id: asset(1),
                amount: i128::MAX,
            },
            Posting {
                account_id: account(2),
                asset_id: asset(1),
                amount: -i128::MAX,
            },
        ],
    )
    .expect("balanced seed entry");
    ledger.post(first).expect("seed entry posts");
    let overflowing = JournalEntry::new(
        transaction(2),
        2,
        date(),
        time(),
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
        ],
    )
    .expect("entry itself balances");

    assert_eq!(
        ledger.post(overflowing),
        Err(LedgerError::ArithmeticOverflow)
    );
    assert_eq!(ledger.balance(account(1), asset(1)), i128::MAX);
    assert_eq!(ledger.balance(account(2), asset(1)), -i128::MAX);
    assert_eq!(ledger.entry_count(), 1);
}

#[test]
fn transaction_id_collision_cannot_change_balances() {
    let mut ledger = Ledger::new();
    let make_entry = |amount| {
        JournalEntry::new(
            transaction(1),
            1,
            date(),
            time(),
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
        .expect("balanced entry")
    };
    ledger.post(make_entry(10)).expect("first entry posts");
    assert_eq!(
        ledger.post(make_entry(11)),
        Err(LedgerError::TransactionIdCollision(transaction(1)))
    );
    assert_eq!(ledger.balance(account(1), asset(1)), 10);
    assert_eq!(ledger.entry_count(), 1);
}

#[test]
fn prepared_posting_cannot_commit_after_ledger_generation_changes() {
    let mut ledger = Ledger::new();
    let pending = match ledger
        .prepare(
            JournalEntry::new(
                transaction(2),
                2,
                date(),
                time(),
                vec![
                    Posting {
                        account_id: account(1),
                        asset_id: asset(1),
                        amount: 20,
                    },
                    Posting {
                        account_id: account(2),
                        asset_id: asset(1),
                        amount: -20,
                    },
                ],
            )
            .expect("balanced pending entry"),
        )
        .expect("entry prepares")
    {
        PostingPreparation::Ready(prepared) => prepared,
        PostingPreparation::Replay(_) => panic!("new transaction cannot replay"),
    };
    let intervening = JournalEntry::new(
        transaction(1),
        1,
        date(),
        time(),
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
    .expect("balanced entry");
    ledger.post(intervening).expect("intervening entry commits");

    assert_eq!(ledger.commit(pending), Err(LedgerError::StalePreparation));
    assert_eq!(ledger.balance(account(1), asset(1)), 10);
    assert_eq!(ledger.entry_count(), 1);
}
