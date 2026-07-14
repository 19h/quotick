use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::{BinaryCodec, CodecError};
use quotick::durable::{DurableError, DurableOrderBook};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions, SegmentedJournalOptions};
use quotick::matching::{
    CancelReason, Command, EventKind, NewOrder, OrderBook, OrderBookCheckpoint, OrderDisplay,
    OrderType, ReplaceOrder, SelfTradePrevention, TimeInForce,
};
use quotick::snapshot::{SnapshotFile, SnapshotOptions};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestArea(PathBuf);

impl TestArea {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-matching-checkpoint-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).expect("test area creates");
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestArea {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("MATCHING-CHECKPOINT").unwrap(),
        kind: InstrumentKind::Future,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 10_000).unwrap(),
        reserve: ReserveOrderRules::new(100).unwrap(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

#[allow(
    clippy::too_many_arguments,
    reason = "checkpoint fixtures make every priority-affecting field explicit"
)]
fn order(
    command_id: u64,
    order_id: u64,
    account_id: u64,
    side: Side,
    quantity: u64,
    display: OrderDisplay,
    order_type: OrderType,
    time_in_force: TimeInForce,
    stp: SelfTradePrevention,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display,
        order_type,
        time_in_force,
        self_trade_prevention: stp,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn resting(command_id: u64, order_id: u64, quantity: u64) -> Command {
    order(
        command_id,
        order_id,
        command_id + 100,
        Side::Buy,
        quantity,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(90)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    )
}

fn journal_options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn write_one_command_checkpoint(
    wal: &Path,
    snapshot: &Path,
    command: Command,
) -> OrderBookCheckpoint {
    let mut durable = DurableOrderBook::open(wal, definition(), journal_options()).unwrap();
    durable.submit(command).unwrap();
    durable
        .write_checkpoint(snapshot, SnapshotOptions::default())
        .unwrap();
    let checkpoint = SnapshotFile::read(snapshot, SnapshotOptions::default()).unwrap();
    durable.close().unwrap();
    checkpoint
}

#[test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end checkpoint scenario keeps the complete FIFO, reserve, STP, retry, and suffix evidence contiguous"
)]
fn matching_checkpoint_preserves_fifo_reserve_stp_idempotency_and_replays_only_suffix() {
    let area = TestArea::new("round-trip");
    let wal = area.join("matching.wal");
    let snapshot = area.join("matching.qsnp");
    let reserve = order(
        1,
        1,
        11,
        Side::Sell,
        30,
        OrderDisplay::Reserve {
            peak: Quantity::new(10).unwrap(),
        },
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelBoth,
    );
    let displayed = order(
        2,
        2,
        12,
        Side::Sell,
        20,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelResting,
    );
    let refresh = order(
        3,
        3,
        13,
        Side::Buy,
        10,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(100)),
        TimeInForce::ImmediateOrCancel,
        SelfTradePrevention::CancelAggressor,
    );
    let bid = order(
        4,
        5,
        12,
        Side::Buy,
        5,
        OrderDisplay::FullyDisplayed,
        OrderType::Limit(Price::from_raw(90)),
        TimeInForce::GoodTilCancelled,
        SelfTradePrevention::CancelAggressor,
    );
    let suffix = Command::Replace(ReplaceOrder {
        command_id: CommandId::new(5).unwrap(),
        order_id: OrderId::new(2).unwrap(),
        account_id: AccountId::new(12).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        new_quantity: Quantity::new(20).unwrap(),
        new_price: Price::from_raw(90),
        new_display: OrderDisplay::FullyDisplayed,
        received_at: TimestampNs::from_unix_nanos(5),
    });

    let mut durable =
        DurableOrderBook::open(&wal, definition(), journal_options()).expect("shard opens");
    durable.submit(reserve).unwrap();
    durable.submit(displayed).unwrap();
    durable.submit(refresh).unwrap();
    durable.submit(bid).unwrap();
    let receipt = durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .expect("checkpoint writes after WAL barrier");
    assert_eq!(receipt.generation(), 9);
    let expected_suffix = durable.submit(suffix).unwrap();
    assert!(expected_suffix.events.iter().any(|event| matches!(
        event.kind,
        EventKind::OrderCancelled {
            order_id,
            reason: CancelReason::SelfTradeResting,
            ..
        } if order_id == OrderId::new(5).unwrap()
    )));
    durable.close().unwrap();

    let checkpoint =
        SnapshotFile::read::<OrderBookCheckpoint>(&snapshot, SnapshotOptions::default())
            .expect("typed checkpoint reads");
    assert_eq!(checkpoint.command_count(), 4);
    assert_eq!(checkpoint.active_order_count(), 3);
    assert_eq!(
        checkpoint
            .orders()
            .iter()
            .filter(|order| order.side() == Side::Sell)
            .map(|order| order.order_id())
            .collect::<Vec<_>>(),
        vec![OrderId::new(2).unwrap(), OrderId::new(1).unwrap()]
    );
    let mut directly_restored = OrderBook::from_checkpoint(checkpoint).unwrap();
    assert_eq!(directly_restored.submit(suffix).unwrap(), expected_suffix);

    let mut recovered = DurableOrderBook::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        journal_options(),
        SnapshotOptions::default(),
    )
    .expect("checkpoint and suffix recover");
    assert_eq!(recovered.recovery().checkpointed_commands, 4);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(
        recovered.book().active_orders().unwrap(),
        directly_restored.active_orders().unwrap()
    );
    let retry = recovered
        .submit(reserve)
        .expect("pre-checkpoint retry resolves");
    assert!(retry.replayed);
    assert_eq!(recovered.book().active_order_count(), 2);
}

#[test]
fn matching_checkpoint_has_a_distinct_stable_snapshot_kind_and_codec_round_trip() {
    let area = TestArea::new("codec");
    let wal = area.join("matching.wal");
    let snapshot = area.join("matching.qsnp");
    let checkpoint = write_one_command_checkpoint(&wal, &snapshot, resting(1, 1, 5));
    let bytes = fs::read(&snapshot).unwrap();
    assert_eq!(&bytes[0..4], b"QSNP");
    assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), 2);
    assert_eq!(
        OrderBookCheckpoint::decode(&checkpoint.encode().unwrap()).unwrap(),
        checkpoint
    );

    let encoded = checkpoint.encode().unwrap();
    let definition_length =
        usize::try_from(u32::from_le_bytes(encoded[16..20].try_into().unwrap())).unwrap();
    let first_order = 20 + definition_length + 4;

    let mut invalid_boundary = encoded.clone();
    invalid_boundary[8..16].copy_from_slice(&4_u64.to_le_bytes());
    assert!(matches!(
        OrderBookCheckpoint::decode(&invalid_boundary),
        Err(CodecError::InvalidMatchingCheckpoint(_))
    ));

    let mut impossible_display = encoded.clone();
    impossible_display[first_order + 33..first_order + 41].copy_from_slice(&6_u64.to_le_bytes());
    assert!(matches!(
        OrderBookCheckpoint::decode(&impossible_display),
        Err(CodecError::InvalidMatchingCheckpoint(_))
    ));

    let history_count = first_order + 43;
    let command_length = usize::try_from(u32::from_le_bytes(
        encoded[history_count + 4..history_count + 8]
            .try_into()
            .unwrap(),
    ))
    .unwrap();
    let report = history_count + 8 + command_length + 4;
    let first_event_sequence = report + 14;
    let mut non_genesis_trace = encoded;
    non_genesis_trace[first_event_sequence..first_event_sequence + 8]
        .copy_from_slice(&2_u64.to_le_bytes());
    let second_event_sequence = first_event_sequence + 42;
    non_genesis_trace[second_event_sequence..second_event_sequence + 8]
        .copy_from_slice(&3_u64.to_le_bytes());
    let non_genesis_result = OrderBookCheckpoint::decode(&non_genesis_trace);
    assert!(
        matches!(
            non_genesis_result,
            Err(CodecError::InvalidMatchingCheckpoint(_))
        ),
        "unexpected decode result: {non_genesis_result:?}"
    );
}

#[test]
fn matching_checkpoint_binds_non_default_wal_origin_and_rejects_lineage_forks() {
    let area = TestArea::new("lineage");
    let wal = area.join("matching.wal");
    let snapshot = area.join("matching.qsnp");
    let fork_wal = area.join("fork.wal");
    let fork_snapshot = area.join("fork.qsnp");
    let options = JournalOptions {
        initial_sequence: 10_000,
        ..journal_options()
    };

    let mut durable = DurableOrderBook::open(&wal, definition(), options).unwrap();
    durable.submit(resting(1, 1, 5)).unwrap();
    let receipt = durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    assert_eq!(receipt.generation(), 10_002);
    durable.submit(resting(2, 2, 7)).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let mut fork = DurableOrderBook::open(&fork_wal, definition(), options).unwrap();
    fork.submit(resting(1, 1, 6)).unwrap();
    fork.submit(resting(2, 2, 7)).unwrap();
    fork.write_checkpoint(&fork_snapshot, SnapshotOptions::default())
        .unwrap();
    fork.close().unwrap();
    let fork_checkpoint: OrderBookCheckpoint =
        SnapshotFile::read(&fork_snapshot, SnapshotOptions::default()).unwrap();
    assert!(matches!(
        SnapshotFile::write(&snapshot, &fork_checkpoint, SnapshotOptions::default()),
        Err(quotick::snapshot::SnapshotError::SameGenerationDivergence { .. })
    ));

    let recovered = DurableOrderBook::open_with_checkpoint(
        &wal,
        &snapshot,
        definition(),
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 2);
    assert_eq!(recovered.recovery().replayed_commands, 0);
}

#[test]
fn matching_checkpoint_must_equal_the_exact_wal_prefix_and_cannot_be_ahead() {
    let area = TestArea::new("prefix");
    let source_wal = area.join("source.wal");
    let divergent_wal = area.join("divergent.wal");
    let short_wal = area.join("short.wal");
    let snapshot = area.join("matching.qsnp");

    let mut source = DurableOrderBook::open(&source_wal, definition(), journal_options()).unwrap();
    source.submit(resting(1, 1, 5)).unwrap();
    source.submit(resting(2, 2, 7)).unwrap();
    source
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    source.close().unwrap();

    let mut divergent =
        DurableOrderBook::open(&divergent_wal, definition(), journal_options()).unwrap();
    divergent.submit(resting(1, 1, 6)).unwrap();
    divergent.close().unwrap();
    assert!(matches!(
        DurableOrderBook::open_with_checkpoint(
            &divergent_wal,
            &snapshot,
            definition(),
            journal_options(),
            SnapshotOptions::default(),
        ),
        Err(DurableError::CheckpointPrefixDivergence { .. })
    ));

    let mut short = DurableOrderBook::open(&short_wal, definition(), journal_options()).unwrap();
    short.submit(resting(1, 1, 5)).unwrap();
    short.close().unwrap();
    assert!(matches!(
        DurableOrderBook::open_with_checkpoint(
            &short_wal,
            &snapshot,
            definition(),
            journal_options(),
            SnapshotOptions::default(),
        ),
        Err(DurableError::CheckpointAheadOfWal { .. })
    ));
}

#[test]
fn segmented_matching_checkpoint_recovers_across_physical_boundaries() {
    let area = TestArea::new("segmented");
    let segments = area.join("segments");
    let snapshot = area.join("matching.qsnp");
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 512,
        journal: journal_options(),
    };
    let mut durable = DurableOrderBook::open_segmented(&segments, definition(), options).unwrap();
    durable.submit(resting(1, 1, 5)).unwrap();
    durable
        .write_checkpoint(&snapshot, SnapshotOptions::default())
        .unwrap();
    durable.submit(resting(2, 2, 7)).unwrap();
    durable.close().unwrap();

    let recovered = DurableOrderBook::open_segmented_with_checkpoint(
        &segments,
        &snapshot,
        definition(),
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert!(recovered.recovery().journal.segment_count >= 2);
    assert_eq!(recovered.book().active_order_count(), 2);
}

#[test]
fn matching_checkpoint_output_cannot_alias_wal_storage() {
    let area = TestArea::new("alias");
    let wal = area.join("matching.wal");
    let mut durable = DurableOrderBook::open(&wal, definition(), journal_options()).unwrap();
    assert!(matches!(
        durable.write_checkpoint(&wal, SnapshotOptions::default()),
        Err(DurableError::CheckpointPathConflictsWithWal { .. })
    ));

    let segments = area.join("segments");
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 1_024,
        journal: journal_options(),
    };
    let mut segmented = DurableOrderBook::open_segmented(&segments, definition(), options).unwrap();
    assert!(matches!(
        segmented.write_checkpoint(segments.join("matching.qsnp"), SnapshotOptions::default()),
        Err(DurableError::CheckpointPathConflictsWithWal { .. })
    ));
}
