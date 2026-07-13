use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::BinaryCodec;
use quotick::durable::DurableOrderBook;
use quotick::durable_ledger::DurableLedger;
use quotick::durable_risk::DurableRiskOrderBook;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, TradingState,
};
use quotick::journal::{
    Durability, JournalLayout, JournalOptions, SegmentedJournal, SegmentedJournalOptions,
    SegmentedJournalReader,
};
use quotick::ledger::{JournalEntry, Posting};
use quotick::matching::{
    Command, NewOrder, OrderBook, OrderType, SelfTradePrevention, TimeInForce,
};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedOrderBook,
    RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs, TransactionId,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-segmented-durable-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn account(value: u64) -> AccountId {
    AccountId::new(value).expect("account ID")
}

fn asset(value: u64) -> AssetId {
    AssetId::new(value).expect("asset ID")
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).expect("instrument ID"),
        version: InstrumentVersion::new(1).expect("instrument version"),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("SEGMENTED").expect("symbol"),
        kind: InstrumentKind::Spot,
        base_asset_id: asset(1),
        quote_asset_id: asset(2),
        price: PriceRules::new(0, 1, Price::from_raw(-1_000), Price::from_raw(1_000))
            .expect("price rules"),
        quantity: QuantityRules::new(1, 1, 1_000).expect("quantity rules"),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .expect("instrument definition")
}

fn command(command_id: u64, order_id: u64) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).expect("command ID"),
        order_id: OrderId::new(order_id).expect("order ID"),
        account_id: account(11),
        instrument_id: InstrumentId::new(1).expect("instrument ID"),
        instrument_version: InstrumentVersion::new(1).expect("instrument version"),
        side: Side::Buy,
        quantity: Quantity::new(5).expect("quantity"),
        order_type: OrderType::Limit(Price::from_raw(100)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn frame_length<T: BinaryCodec>(record: &T) -> u64 {
    let payload = record.encode().expect("record encodes");
    u64::try_from(24 + payload.len()).expect("frame length fits u64")
}

fn options(maximum_segment_bytes: u64) -> SegmentedJournalOptions {
    SegmentedJournalOptions {
        maximum_segment_bytes,
        journal: JournalOptions {
            durability: Durability::Buffered,
            ..JournalOptions::default()
        },
    }
}

fn entry(transaction_id: u64, amount: i128) -> JournalEntry {
    JournalEntry::new(
        TransactionId::new(transaction_id).expect("transaction ID"),
        transaction_id,
        vec![
            Posting {
                account_id: account(11),
                asset_id: asset(1),
                amount,
            },
            Posting {
                account_id: account(12),
                asset_id: asset(1),
                amount: -amount,
            },
        ],
    )
    .expect("balanced entry")
}

fn risk_profile() -> AccountRiskDefinition {
    let limits = RiskLimits::new(RiskLimitSpec {
        max_order_quantity_lots: 100,
        max_order_notional: 1_000_000,
        max_open_orders: 100,
        max_open_quantity_lots: 1_000,
        max_open_notional: 1_000_000,
        max_long_position_lots: 1_000,
        max_short_position_lots: 1_000,
    })
    .expect("risk limits");
    AccountRiskDefinition::new(
        account(11),
        RiskProfile::new(AccountRiskState::Active, 0, limits).expect("risk profile"),
    )
}

#[test]
fn matching_replays_when_definition_command_and_report_are_separate_segments() {
    let directory = TestDirectory::new("matching-round-trip");
    let definition = definition();
    let command = command(1, 1);
    let mut model = OrderBook::new(definition);
    let expected = model.submit(command).expect("model executes");
    let capacity = [
        frame_length(&definition),
        frame_length(&command),
        frame_length(&expected),
    ]
    .into_iter()
    .max()
    .expect("capacity exists");
    let options = options(capacity);

    let mut durable =
        DurableOrderBook::open_segmented(directory.path(), definition, options).expect("opens");
    assert_eq!(durable.submit(command).expect("submits"), expected);
    durable.close().expect("closes");

    let recovered =
        DurableOrderBook::open_segmented(directory.path(), definition, options).expect("replays");
    assert_eq!(
        recovered.recovery().journal.layout,
        JournalLayout::Segmented
    );
    assert_eq!(recovered.recovery().journal.segment_count, 3);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(recovered.book().active_order_count(), 1);
}

#[test]
fn matching_completes_a_cross_segment_dangling_command() {
    let directory = TestDirectory::new("matching-dangling");
    let definition = definition();
    let command = command(1, 1);
    let mut model = OrderBook::new(definition);
    let report = model.submit(command).expect("model executes");
    let capacity = [
        frame_length(&definition),
        frame_length(&command),
        frame_length(&report),
    ]
    .into_iter()
    .max()
    .expect("capacity exists");
    let options = options(capacity);
    let mut journal = SegmentedJournal::open(directory.path(), options).expect("journal opens");
    journal.append(&definition).expect("definition appends");
    journal.append(&command).expect("command appends");
    journal.close().expect("journal closes");

    let recovered =
        DurableOrderBook::open_segmented(directory.path(), definition, options).expect("recovers");
    assert!(recovered.recovery().completed_dangling_command);
    assert_eq!(recovered.book().active_order_count(), 1);
    recovered.close().expect("runtime closes");
    assert_eq!(
        SegmentedJournalReader::open(directory.path(), options)
            .expect("reader opens")
            .count(),
        3
    );
}

#[test]
fn ledger_rotates_each_entry_and_reconstructs_balances() {
    let directory = TestDirectory::new("ledger");
    let first = entry(1, 100);
    let second = entry(2, 200);
    let third = entry(3, -50);
    let capacity = [
        frame_length(&first),
        frame_length(&second),
        frame_length(&third),
    ]
    .into_iter()
    .max()
    .expect("capacity exists");
    let options = options(capacity);
    let mut ledger = DurableLedger::open_segmented(directory.path(), options).expect("opens");
    ledger.post(first).expect("first entry posts");
    ledger.post(second).expect("second entry posts");
    ledger.post(third).expect("third entry posts");
    ledger.close().expect("closes");

    let recovered = DurableLedger::open_segmented(directory.path(), options).expect("replays");
    assert_eq!(recovered.recovery().journal.segment_count, 3);
    assert_eq!(recovered.recovery().replayed_entries, 3);
    assert_eq!(recovered.ledger().balance(account(11), asset(1)), 250);
    assert_eq!(recovered.ledger().balance(account(12), asset(1)), -250);
}

#[test]
fn risk_metadata_and_reservations_replay_across_segment_boundaries() {
    let directory = TestDirectory::new("risk");
    let definition = definition();
    let profile = risk_profile();
    let command = command(1, 1);
    let mut model = RiskManagedOrderBook::new(definition);
    model
        .register_account(profile.account_id(), profile.profile())
        .expect("account registers");
    let report = model.submit(command).expect("model executes");
    let capacity = [
        frame_length(&definition),
        frame_length(&profile),
        frame_length(&command),
        frame_length(&report),
    ]
    .into_iter()
    .max()
    .expect("capacity exists");
    let options = options(capacity);
    let mut durable =
        DurableRiskOrderBook::open_segmented(directory.path(), definition, &[profile], options)
            .expect("opens");
    assert_eq!(durable.submit(command).expect("submits"), report);
    durable.close().expect("closes");

    let recovered =
        DurableRiskOrderBook::open_segmented(directory.path(), definition, &[profile], options)
            .expect("replays");
    assert_eq!(recovered.recovery().journal.segment_count, 4);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(
        recovered
            .managed()
            .risk()
            .reservation(OrderId::new(1).expect("order ID"))
            .expect("reservation")
            .quantity_lots(),
        5
    );
}
