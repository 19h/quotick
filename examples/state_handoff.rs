//! Off-thread state verification and deterministic continuation on a restored shard.

mod support;

use quotick::Side;
use quotick::codec::BinaryCodec;
use quotick::matching::{OrderBook, OrderBookCheckpoint, TimeInForce};

use support::{LimitOrder, definition, matching_limits};

fn main() {
    let definition = definition("STATE-HANDOFF");
    let limits = matching_limits();
    let mut source = OrderBook::try_with_limits(definition, limits).unwrap();

    let ask = LimitOrder::resting(1, 1, 11, Side::Sell, 5, 10_000).command(definition);
    source.submit(ask).unwrap();
    let mut partial_take = LimitOrder::resting(2, 2, 12, Side::Buy, 2, 10_000);
    partial_take.time_in_force = TimeInForce::ImmediateOrCancel;
    source.submit(partial_take.command(definition)).unwrap();
    source
        .submit(LimitOrder::resting(3, 3, 13, Side::Buy, 4, 9_900).command(definition))
        .unwrap();

    // A definition frame followed by three command/report pairs ends at WAL sequence 7.
    let capture = source.capture_checkpoint_candidate(1, 7).unwrap();
    let independent_verification = capture.clone();
    assert!(capture.shares_checkpoint_storage_with(&independent_verification));
    let worker = std::thread::spawn(move || capture.verify().unwrap());

    let mut suffix = LimitOrder::resting(4, 4, 14, Side::Sell, 1, 9_900);
    suffix.time_in_force = TimeInForce::ImmediateOrCancel;
    let suffix = suffix.command(definition);
    let expected_suffix = source.submit(suffix).unwrap();

    let verified = worker.join().expect("checkpoint verifier does not panic");
    let second_proof = independent_verification.verify().unwrap();
    assert_eq!(verified, second_proof);
    assert!(verified.shares_history_storage_with(&second_proof));
    assert_eq!(verified.generation(), 7);
    assert_eq!(verified.command_count(), 3);

    let encoded = verified.encode().unwrap();
    let decoded = OrderBookCheckpoint::decode(&encoded).unwrap();
    let mut restored = OrderBook::from_checkpoint_with_limits(&decoded, limits).unwrap();
    let reproduced_suffix = restored.submit(suffix).unwrap();
    assert_eq!(reproduced_suffix, expected_suffix);

    let prefix_retry = restored.submit(ask).unwrap();
    assert!(prefix_retry.replayed);
    assert_eq!(restored.best_bid(), source.best_bid());
    assert_eq!(restored.best_ask(), source.best_ask());
    assert_eq!(restored.last_event_sequence(), source.last_event_sequence());
    source.validate().unwrap();
    restored.validate().unwrap();

    println!(
        "checkpoint_bytes={} captured_commands={} continuation_sequence={}",
        encoded.len(),
        verified.command_count(),
        restored.last_event_sequence()
    );
}
