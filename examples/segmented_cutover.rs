//! Segmented WAL rotation, checkpoint cutover, suffix replay, and retry.

mod support;

use quotick::Side;
use quotick::durable::DurableOrderBook;
use quotick::journal::{JournalLayout, SegmentedJournalOptions};
use quotick::snapshot::{CheckpointSlot, SnapshotOptions};

use support::{LimitOrder, ScratchArea, definition, journal_options, matching_limits};

fn main() {
    let area = ScratchArea::new("segmented-cutover");
    let segments = area.join("matching-segments");
    let checkpoint = area.join("matching.qsnp");
    let definition = definition("SEGMENTED-CUTOVER");
    let options = SegmentedJournalOptions {
        maximum_segment_bytes: 512,
        journal: journal_options(),
    };

    let first = LimitOrder::resting(1, 1, 101, Side::Buy, 1, 10_000);
    let mut durable = DurableOrderBook::open_segmented_with_limits(
        &segments,
        definition,
        matching_limits(),
        options,
    )
    .unwrap();
    durable.submit(first.command(definition)).unwrap();
    for value in 2..=6 {
        durable
            .submit(
                LimitOrder::resting(
                    value,
                    value,
                    100 + value,
                    Side::Buy,
                    1,
                    10_000 - i64::try_from(value).unwrap(),
                )
                .command(definition),
            )
            .unwrap();
    }
    durable.close().unwrap();

    let mut rotated = DurableOrderBook::open_segmented_with_limits(
        &segments,
        definition,
        matching_limits(),
        options,
    )
    .unwrap();
    let rotated_recovery = rotated.recovery();
    assert_eq!(rotated_recovery.journal.layout, JournalLayout::Segmented);
    assert!(rotated_recovery.journal.segment_count >= 2);
    assert_eq!(rotated_recovery.replayed_commands, 6);
    assert_eq!(rotated.book().active_order_count(), 6);

    let cutover = rotated
        .compact_to_checkpoint(&checkpoint, SnapshotOptions::default())
        .unwrap();
    assert_eq!(cutover.slot(), CheckpointSlot::A);
    assert_eq!(cutover.snapshot().generation(), 13);

    let suffix = LimitOrder::resting(7, 7, 107, Side::Buy, 1, 9_993);
    rotated.submit(suffix.command(definition)).unwrap();
    rotated.close().unwrap();

    let mut recovered = DurableOrderBook::open_segmented_with_checkpoint_and_limits(
        &segments,
        &checkpoint,
        definition,
        matching_limits(),
        options,
        SnapshotOptions::default(),
    )
    .unwrap();
    let recovery = recovered.recovery();
    assert_eq!(recovery.checkpointed_commands, 6);
    assert_eq!(recovery.replayed_commands, 1);
    assert_eq!(recovered.book().active_order_count(), 7);
    assert!(
        recovered
            .submit(first.command(definition))
            .unwrap()
            .replayed
    );
    recovered.book().validate().unwrap();
    let active_orders = recovered.book().active_order_count();
    recovered.close().unwrap();

    println!(
        "rotated_segments={} checkpointed_commands={} replayed_suffix={} active_orders={}",
        rotated_recovery.journal.segment_count,
        recovery.checkpointed_commands,
        recovery.replayed_commands,
        active_orders
    );
}
