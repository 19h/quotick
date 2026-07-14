use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable_ledger::{DurableLedger, DurableLedgerError};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, TradingState,
};
use quotick::journal::{Durability, Journal, JournalOptions};
use quotick::ledger::{JournalEntry, LedgerError, Posting};
use quotick::matching::{Command, NewOrder, OrderType, SelfTradePrevention, TimeInForce};
use quotick::{
    AccountId, AccountingDate, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, Side, TimestampNs, TradeId, TransactionId,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-ledger-{label}-{}-{nonce}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn account(value: u64) -> AccountId {
    AccountId::new(value).expect("account id")
}

fn asset(value: u64) -> AssetId {
    AssetId::new(value).expect("asset id")
}

fn transaction(value: u64) -> TransactionId {
    TransactionId::new(value).expect("transaction id")
}

fn entry(transaction_id: u64, amount: i128) -> JournalEntry {
    JournalEntry::new(
        transaction(transaction_id),
        transaction_id,
        AccountingDate::UNIX_EPOCH,
        TimestampNs::from_unix_nanos(0),
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
}

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

#[test]
fn entry_before_balance_reopen_reconstructs_balances_and_idempotency() {
    let file = TestFile::new("round-trip");
    let value = entry(1, 500);
    let mut durable = DurableLedger::open(file.path(), options()).expect("ledger opens");
    let first = durable.post(value.clone()).expect("entry commits");
    assert_eq!(first.sequence, 1);
    assert_eq!(durable.ledger().balance(account(1), asset(1)), 500);
    let length = fs::metadata(file.path()).expect("metadata").len();

    let replay = durable.post(value).expect("exact retry");
    assert!(replay.replayed);
    assert_eq!(fs::metadata(file.path()).expect("metadata").len(), length);
    durable.sync_all().expect("sync");
    drop(durable);

    let recovered = DurableLedger::open(file.path(), options()).expect("ledger recovers");
    assert_eq!(recovered.recovery().replayed_records, 1);
    assert_eq!(recovered.ledger().entry_count(), 1);
    assert_eq!(recovered.ledger().balance(account(1), asset(1)), 500);
    assert_eq!(recovered.ledger().balance(account(2), asset(1)), -500);
}

#[test]
fn transaction_collision_is_rejected_before_wal_growth() {
    let file = TestFile::new("collision");
    let mut durable = DurableLedger::open(file.path(), options()).expect("ledger opens");
    durable.post(entry(1, 10)).expect("first entry commits");
    let length = fs::metadata(file.path()).expect("metadata").len();

    assert!(matches!(
        durable.post(entry(1, 11)),
        Err(DurableLedgerError::Ledger(
            LedgerError::TransactionIdCollision(_)
        ))
    ));
    assert_eq!(fs::metadata(file.path()).expect("metadata").len(), length);
    assert_eq!(durable.ledger().balance(account(1), asset(1)), 10);
}

#[test]
fn recovery_rejects_nonledger_records_and_duplicate_transactions() {
    let command_file = TestFile::new("unexpected-record");
    let command = Command::New(NewOrder {
        command_id: CommandId::new(1).expect("command id"),
        order_id: OrderId::new(1).expect("order id"),
        account_id: account(1),
        instrument_id: InstrumentId::new(1).expect("instrument id"),
        instrument_version: InstrumentVersion::new(1).expect("instrument version"),
        side: Side::Buy,
        quantity: Quantity::new(1).expect("positive quantity"),
        display: quotick::matching::OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(1)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(1),
    });
    let mut command_journal = Journal::open(command_file.path(), options()).expect("journal opens");
    command_journal.append(&command).expect("command appends");
    command_journal.sync_all().expect("sync");
    drop(command_journal);
    assert!(matches!(
        DurableLedger::open(command_file.path(), options()),
        Err(DurableLedgerError::UnexpectedRecord { sequence: 1, .. })
    ));

    let duplicate_file = TestFile::new("duplicate-record");
    let value = entry(1, 10);
    let mut duplicate_journal =
        Journal::open(duplicate_file.path(), options()).expect("journal opens");
    duplicate_journal.append(&value).expect("first append");
    duplicate_journal.append(&value).expect("duplicate append");
    duplicate_journal.sync_all().expect("sync");
    drop(duplicate_journal);
    assert!(matches!(
        DurableLedger::open(duplicate_file.path(), options()),
        Err(DurableLedgerError::DuplicateTransactionRecord {
            sequence: 2,
            transaction_id
        }) if transaction_id == transaction(1)
    ));
}

#[test]
fn durable_trade_settlement_reconstructs_delivery_versus_payment() {
    let file = TestFile::new("trade-settlement");
    let trade = quotick::matching::Trade {
        trade_id: TradeId::new(1).expect("trade id"),
        instrument_id: InstrumentId::new(1).expect("instrument id"),
        instrument_version: InstrumentVersion::new(1).expect("instrument version"),
        price: Price::from_raw(25),
        quantity: Quantity::new(4).expect("positive quantity"),
        buy_order_id: OrderId::new(1).expect("order id"),
        sell_order_id: OrderId::new(2).expect("order id"),
        buyer_account_id: account(1),
        seller_account_id: account(2),
        maker_order_id: OrderId::new(2).expect("order id"),
        taker_order_id: OrderId::new(1).expect("order id"),
    };
    let definition = InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).expect("instrument id"),
        version: InstrumentVersion::new(1).expect("instrument version"),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("TEST").expect("symbol"),
        kind: InstrumentKind::Spot,
        base_asset_id: asset(1),
        quote_asset_id: asset(2),
        price: PriceRules::new(0, 1, Price::from_raw(i64::MIN), Price::from_raw(i64::MAX))
            .expect("price rules"),
        quantity: QuantityRules::new(1, 1, u64::MAX).expect("quantity rules"),
        reserve: quotick::instrument::ReserveOrderRules::disabled(),
        base_units_per_lot: 10,
        quote_units_per_price_unit: 100,
        trading_state: TradingState::Open,
    })
    .expect("instrument definition");
    let mut durable = DurableLedger::open(file.path(), options()).expect("ledger opens");
    let wrong_version = quotick::matching::Trade {
        instrument_version: InstrumentVersion::new(2).expect("instrument version"),
        ..trade
    };
    assert!(matches!(
        durable.settle_instrument_trade(
            transaction(6),
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(0),
            &wrong_version,
            definition
        ),
        Err(DurableLedgerError::Ledger(
            LedgerError::SettlementVersionMismatch
        ))
    ));
    assert_eq!(durable.ledger().entry_count(), 0);
    durable
        .settle_instrument_trade(
            transaction(7),
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(0),
            &trade,
            definition,
        )
        .expect("trade settles");
    durable.sync_all().expect("sync");
    drop(durable);

    let recovered = DurableLedger::open(file.path(), options()).expect("ledger recovers");
    assert_eq!(recovered.ledger().balance(account(1), asset(1)), 40);
    assert_eq!(recovered.ledger().balance(account(2), asset(1)), -40);
    assert_eq!(recovered.ledger().balance(account(1), asset(2)), -10_000);
    assert_eq!(recovered.ledger().balance(account(2), asset(2)), 10_000);
}
