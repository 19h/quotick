use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::durable::{DurableError, DurableOrderBook};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, TradingState,
};
use quotick::journal::{
    Durability, Journal, JournalError, JournalOptions, JournalReader, RecordKind, RecoveryMode,
};
use quotick::matching::{
    CancelReason, Command, EventKind, ExpirySweep, MatchingError, NewOrder, OrderBook, OrderType,
    SelfTradePrevention, StopActivation, StopTriggerSweep, TimeInForce,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-durable-{label}-{}-{nonce}.wal",
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

fn instrument() -> InstrumentId {
    InstrumentId::new(1).expect("instrument id")
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(1).expect("version")
}

fn definition() -> InstrumentDefinition {
    definition_in_state(version(), TradingState::Open)
}

fn definition_in_state(
    instrument_version: InstrumentVersion,
    trading_state: TradingState,
) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: instrument_version,
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("TEST").expect("symbol"),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).expect("base asset"),
        quote_asset_id: AssetId::new(2).expect("quote asset"),
        price: PriceRules::new(0, 1, Price::from_raw(i64::MIN), Price::from_raw(i64::MAX))
            .expect("price rules"),
        quantity: QuantityRules::new(1, 1, u64::MAX).expect("quantity rules"),
        reserve: quotick::instrument::ReserveOrderRules::disabled(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state,
    })
    .expect("definition")
}

fn command(command_id: u64, order_id: u64, quantity: u64) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).expect("command id"),
        order_id: OrderId::new(order_id).expect("order id"),
        account_id: AccountId::new(10).expect("account id"),
        instrument_id: instrument(),
        instrument_version: version(),
        side: Side::Buy,
        quantity: Quantity::new(quantity).expect("positive quantity"),
        display: quotick::matching::OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(100)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn gtd_command(command_id: u64, order_id: u64, quantity: u64, expires_at: u64) -> Command {
    let Command::New(mut value) = command(command_id, order_id, quantity) else {
        unreachable!("command fixture is always new")
    };
    value.time_in_force = TimeInForce::GoodTilTimestamp {
        expires_at: TimestampNs::from_unix_nanos(expires_at),
    };
    Command::New(value)
}

fn expiry_sweep(command_id: u64, through: u64, received_at: u64) -> Command {
    Command::ExpirySweep(ExpirySweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        through: TimestampNs::from_unix_nanos(through),
        received_at: TimestampNs::from_unix_nanos(received_at),
    })
}

fn stop_trigger(command_id: u64, reference_price: i64) -> Command {
    Command::StopTriggerSweep(StopTriggerSweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        reference_price: Price::from_raw(reference_price),
        maximum_orders: 4,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn stop_limit(command_id: u64, order_id: u64) -> Command {
    let Command::New(mut value) = command(command_id, order_id, 5) else {
        unreachable!("command fixture is always new")
    };
    value.order_type = OrderType::Stop {
        trigger_price: Price::from_raw(110),
        activation: StopActivation::Limit(Price::from_raw(105)),
    };
    Command::New(value)
}

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn frame_kinds(path: &Path, options: JournalOptions) -> Vec<RecordKind> {
    JournalReader::open(path, options)
        .expect("reader opens")
        .collect::<Result<Vec<_>, _>>()
        .expect("journal verifies")
        .iter()
        .map(quotick::journal::JournalFrame::kind)
        .collect()
}

#[test]
fn submit_writes_command_then_report_and_reopen_reconstructs_state() {
    let file = TestFile::new("round-trip");
    let value = command(1, 1, 5);
    let mut durable =
        DurableOrderBook::open(file.path(), definition(), options()).expect("shard opens");
    let first = durable.submit(value).expect("command commits");
    durable.sync_all().expect("journal syncs");
    assert!(!first.replayed);
    assert_eq!(
        frame_kinds(file.path(), options()),
        vec![
            RecordKind::InstrumentDefinition,
            RecordKind::Command,
            RecordKind::ExecutionReport
        ]
    );

    let retry = durable.submit(value).expect("exact retry");
    assert!(retry.replayed);
    assert_eq!(frame_kinds(file.path(), options()).len(), 3);
    drop(durable);

    let reopened =
        DurableOrderBook::open(file.path(), definition(), options()).expect("shard recovers");
    assert_eq!(reopened.recovery().replayed_commands, 1);
    assert!(!reopened.recovery().completed_dangling_command);
    assert_eq!(
        reopened.book().best_bid().expect("bid restored").quantity,
        5
    );
    reopened.book().validate().expect("restored book validates");
}

#[test]
fn durable_gtd_expiry_restores_the_watermark_and_exact_retry() {
    let file = TestFile::new("gtd-expiry");
    let order = gtd_command(1, 1, 5, 10);
    let sweep = expiry_sweep(2, 10, 10);
    let mut durable = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    durable.submit(order).unwrap();
    let report = durable.submit(sweep).unwrap();
    assert!(matches!(
        report.events[0].kind,
        EventKind::OrderCancelled {
            reason: CancelReason::Expired,
            ..
        }
    ));
    durable.sync_all().unwrap();
    drop(durable);

    let mut reopened = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    assert_eq!(
        reopened.book().expiry_watermark(),
        Some(TimestampNs::from_unix_nanos(10))
    );
    assert_eq!(reopened.book().active_order_count(), 0);
    let frames_before_retry = frame_kinds(file.path(), options()).len();
    assert!(reopened.submit(sweep).unwrap().replayed);
    assert_eq!(
        frame_kinds(file.path(), options()).len(),
        frames_before_retry
    );
    reopened.book().validate().unwrap();
}

#[test]
fn durable_recovery_restores_dormant_stop_reference_and_activation() {
    let file = TestFile::new("stop-recovery");
    let reference = stop_trigger(1, 100);
    let stop = stop_limit(2, 1);
    let mut durable = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    durable.submit(reference).unwrap();
    let armed = durable.submit(stop).unwrap();
    durable.sync_all().unwrap();
    drop(durable);

    let mut reopened = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    assert_eq!(
        reopened.book().stop_reference_price(),
        Some(Price::from_raw(100))
    );
    assert_eq!(reopened.book().dormant_stop_count(), 1);
    assert!(reopened.submit(stop).unwrap().replayed);
    assert_eq!(reopened.submit(stop).unwrap().events, armed.events);
    reopened.submit(stop_trigger(3, 110)).unwrap();
    assert!(
        reopened
            .book()
            .dormant_stop(OrderId::new(1).unwrap())
            .is_none()
    );
    assert_eq!(
        reopened
            .book()
            .order(OrderId::new(1).unwrap())
            .unwrap()
            .price,
        Price::from_raw(105)
    );
    reopened.sync_all().unwrap();
    drop(reopened);

    let recovered = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
    assert_eq!(
        recovered.book().stop_reference_price(),
        Some(Price::from_raw(110))
    );
    assert_eq!(recovered.book().dormant_stop_count(), 0);
    assert_eq!(
        recovered
            .book()
            .order(OrderId::new(1).unwrap())
            .unwrap()
            .leaves_quantity
            .lots(),
        5
    );
    recovered.book().validate().unwrap();
}

#[test]
fn durable_shard_propagates_exclusive_journal_ownership() {
    let file = TestFile::new("exclusive-owner");
    let first =
        DurableOrderBook::open(file.path(), definition(), options()).expect("first shard opens");
    assert!(matches!(
        DurableOrderBook::open(file.path(), definition(), options()),
        Err(DurableError::Journal(JournalError::WriterLeaseHeld { .. }))
    ));
    first.close().expect("first shard closes durably");
    DurableOrderBook::open(file.path(), definition(), options())
        .expect("shard reopens after ownership release");
}

#[test]
fn recovery_completes_a_durable_command_without_a_report() {
    let file = TestFile::new("dangling-command");
    let mut journal = Journal::open(file.path(), options()).expect("journal opens");
    journal.append(&definition()).expect("definition persists");
    journal.append(&command(1, 1, 5)).expect("command persists");
    journal.sync_all().expect("command syncs");
    drop(journal);

    let recovered =
        DurableOrderBook::open(file.path(), definition(), options()).expect("command completes");
    assert!(recovered.recovery().completed_dangling_command);
    assert_eq!(recovered.book().active_order_count(), 1);
    assert_eq!(
        frame_kinds(file.path(), options()),
        vec![
            RecordKind::InstrumentDefinition,
            RecordKind::Command,
            RecordKind::ExecutionReport
        ]
    );
}

#[test]
fn deterministic_replay_rejects_a_valid_but_different_report() {
    let file = TestFile::new("divergence");
    let logged = command(1, 1, 5);
    let mut different_book = OrderBook::new(definition());
    let different_report = different_book
        .submit(command(1, 1, 6))
        .expect("different command executes");
    let mut journal = Journal::open(file.path(), options()).expect("journal opens");
    journal.append(&definition()).expect("definition persists");
    journal.append(&logged).expect("command persists");
    journal
        .append(&different_report)
        .expect("valid report persists");
    journal.sync_all().expect("journal syncs");
    drop(journal);

    assert!(matches!(
        DurableOrderBook::open(file.path(), definition(), options()),
        Err(DurableError::ReplayDivergence {
            command_sequence: 2,
            report_sequence: 3
        })
    ));
}

#[test]
fn command_identifier_collision_is_rejected_before_wal_growth() {
    let file = TestFile::new("collision");
    let mut durable =
        DurableOrderBook::open(file.path(), definition(), options()).expect("shard opens");
    durable
        .submit(command(1, 1, 5))
        .expect("first command commits");
    let before = fs::metadata(file.path()).expect("metadata").len();

    assert!(matches!(
        durable.submit(command(1, 2, 5)),
        Err(DurableError::Matching(MatchingError::CommandIdCollision(_)))
    ));
    assert_eq!(fs::metadata(file.path()).expect("metadata").len(), before);
}

#[test]
fn torn_report_is_repaired_and_recomputed_from_its_durable_command() {
    let file = TestFile::new("torn-report");
    let value = command(1, 1, 5);
    let mut book = OrderBook::new(definition());
    let report = book.submit(value).expect("command executes");
    let mut journal = Journal::open(file.path(), options()).expect("journal opens");
    journal.append(&definition()).expect("definition persists");
    journal.append(&value).expect("command persists");
    journal.append(&report).expect("report persists");
    journal.sync_all().expect("sync");
    drop(journal);
    let length = fs::metadata(file.path()).expect("metadata").len();
    OpenOptions::new()
        .write(true)
        .open(file.path())
        .expect("file opens")
        .set_len(length - 4)
        .expect("report torn");

    let repair_options = JournalOptions {
        recovery: RecoveryMode::RepairTornTail,
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let recovered = DurableOrderBook::open(file.path(), definition(), repair_options)
        .expect("torn report recovers");
    assert!(recovered.recovery().completed_dangling_command);
    assert!(recovered.recovery().journal.truncated_bytes > 0);
    assert_eq!(frame_kinds(file.path(), options()).len(), 3);
    assert_eq!(recovered.book().active_order_count(), 1);
}

#[test]
fn reopen_requires_an_exact_persisted_instrument_definition() {
    let file = TestFile::new("definition-binding");
    let durable =
        DurableOrderBook::open(file.path(), definition(), options()).expect("shard opens");
    drop(durable);

    let changed = definition_in_state(version(), TradingState::Halted);
    assert!(matches!(
        DurableOrderBook::open(file.path(), changed, options()),
        Err(DurableError::DefinitionMismatch {
            requested_instrument_id,
            requested_version,
            persisted_instrument_id,
            persisted_version,
        }) if requested_instrument_id == instrument()
            && requested_version == version()
            && persisted_instrument_id == instrument()
            && persisted_version == version()
    ));
}

#[test]
fn nonempty_matching_journal_must_begin_with_definition_metadata() {
    let file = TestFile::new("missing-definition");
    let mut journal = Journal::open(file.path(), options()).expect("journal opens");
    journal.append(&command(1, 1, 5)).expect("command persists");
    drop(journal);

    assert!(matches!(
        DurableOrderBook::open(file.path(), definition(), options()),
        Err(DurableError::DefinitionRecordRequired {
            sequence: 1,
            actual: RecordKind::Command
        })
    ));
}
