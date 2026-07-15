//! Durable risk/matching recovery from an off-thread verified checkpoint plus WAL suffix.

mod support;

use std::fs;

use quotick::durable_risk::DurableRiskOrderBook;
use quotick::matching::TimeInForce;
use quotick::snapshot::SnapshotOptions;
use quotick::{OrderId, Side};

use support::{
    LimitOrder, ScratchArea, account, account_definition, definition, journal_options, risk_limits,
};

fn main() {
    let area = ScratchArea::new("wal-recovery");
    let wal = area.join("execution.wal");
    let snapshot = area.join("execution.qsnp");
    let definition = definition("WAL-RECOVERY");
    let profiles = [
        account_definition(11, 0, 100),
        account_definition(12, 0, 100),
    ];

    let ask = LimitOrder::resting(1, 1, 11, Side::Sell, 8, 10_000).command(definition);
    let mut take = LimitOrder::resting(2, 2, 12, Side::Buy, 3, 10_000);
    take.time_in_force = TimeInForce::ImmediateOrCancel;
    let take = take.command(definition);

    let mut durable = DurableRiskOrderBook::open_with_limits(
        &wal,
        definition,
        &profiles,
        risk_limits(),
        journal_options(),
    )
    .unwrap();
    durable.submit(ask).unwrap();

    let capture = durable.capture_checkpoint_candidate().unwrap();
    assert_eq!(capture.command_count(), 1);
    assert_eq!(capture.active_order_count(), 1);
    let worker = std::thread::spawn(move || capture.verify().unwrap());

    let suffix_report = durable.submit(take).unwrap();
    let verified_checkpoint = worker.join().expect("checkpoint verifier does not panic");
    assert_eq!(verified_checkpoint.command_count(), 1);
    durable
        .write_verified_checkpoint(&snapshot, &verified_checkpoint, SnapshotOptions::default())
        .unwrap();
    durable.close().unwrap();

    let mut recovered = DurableRiskOrderBook::open_with_checkpoint_and_limits(
        &wal,
        &snapshot,
        definition,
        &profiles,
        risk_limits(),
        journal_options(),
        SnapshotOptions::default(),
    )
    .unwrap();
    assert_eq!(recovered.recovery().checkpointed_commands, 1);
    assert_eq!(recovered.recovery().replayed_commands, 1);
    assert_eq!(
        recovered
            .managed()
            .risk()
            .snapshot(account(11))
            .unwrap()
            .position_lots(),
        -3
    );
    assert_eq!(
        recovered
            .managed()
            .risk()
            .snapshot(account(12))
            .unwrap()
            .position_lots(),
        3
    );
    assert_eq!(
        recovered
            .managed()
            .risk()
            .reservation(OrderId::new(1).unwrap())
            .unwrap()
            .quantity_lots(),
        5
    );

    let bytes_before_retry = fs::metadata(&wal).unwrap().len();
    let retry = recovered.submit(take).unwrap();
    assert!(retry.replayed);
    assert_eq!(retry.events, suffix_report.events);
    assert_eq!(fs::metadata(&wal).unwrap().len(), bytes_before_retry);
    recovered.managed().validate().unwrap();
    recovered.close().unwrap();

    println!(
        "checkpointed_commands=1 replayed_suffix=1 retry_wal_growth=0 wal_bytes={bytes_before_retry}"
    );
}
