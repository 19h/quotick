//! Gap detection and atomic full-depth repair for a level-2 consumer.

mod support;

use quotick::Side;
use quotick::instrument::TradingState;
use quotick::market_data::{
    MarketDataBatch, MarketDataError, MarketDataLimits, MarketDataPublisher, MarketDataReplica,
};
use quotick::matching::{Command, OrderBook};

use support::{LimitOrder, definition, matching_limits};

fn publish(
    book: &mut OrderBook,
    publisher: &mut MarketDataPublisher,
    command: Command,
) -> MarketDataBatch {
    let report = book.submit(command).unwrap();
    publisher.publish(command, &report, book).unwrap()
}

fn main() {
    let definition = definition("FEED-REPAIR");
    let limits = matching_limits();
    let mut book = OrderBook::try_with_limits(definition, limits).unwrap();
    let mut publisher = MarketDataPublisher::from_book(&book).unwrap();
    let mut replica = MarketDataReplica::try_with_limits(
        definition.instrument_id(),
        definition.version(),
        TradingState::Open,
        MarketDataLimits::from_order_book(limits).spec(),
    )
    .unwrap();
    let genesis = publisher.snapshot();
    replica.apply_snapshot(&genesis).unwrap();

    let first = publish(
        &mut book,
        &mut publisher,
        LimitOrder::resting(1, 1, 11, Side::Buy, 2, 9_900).command(definition),
    );
    replica.apply_batch(&first).unwrap();

    let omitted = publish(
        &mut book,
        &mut publisher,
        LimitOrder::resting(2, 2, 12, Side::Sell, 3, 10_100).command(definition),
    );
    let next = publish(
        &mut book,
        &mut publisher,
        LimitOrder::resting(3, 3, 13, Side::Buy, 1, 9_800).command(definition),
    );
    let expected = omitted.first_sequence().unwrap();
    let actual = next.first_sequence().unwrap();
    assert_eq!(
        replica.apply_batch(&next),
        Err(MarketDataError::SequenceGap { expected, actual })
    );
    let sequence_before_repair = replica.last_sequence();

    let repair = publisher.snapshot();
    replica.apply_snapshot(&repair).unwrap();
    assert_eq!(replica.last_sequence(), book.last_event_sequence());
    assert_eq!(
        replica.depth(Side::Buy, usize::MAX),
        book.depth(Side::Buy, usize::MAX)
    );
    assert_eq!(
        replica.depth(Side::Sell, usize::MAX),
        book.depth(Side::Sell, usize::MAX)
    );
    assert_eq!(
        replica.apply_snapshot(&genesis),
        Err(MarketDataError::StaleSnapshot {
            current: repair.as_of_sequence(),
            snapshot: 0,
        })
    );

    let post_repair = publish(
        &mut book,
        &mut publisher,
        LimitOrder::resting(4, 4, 14, Side::Sell, 2, 10_200).command(definition),
    );
    replica.apply_batch(&post_repair).unwrap();
    publisher.validate_against(&book).unwrap();
    replica.validate().unwrap();

    println!(
        "gap_expected={} gap_actual={} pre_repair_sequence={} repaired_sequence={}",
        expected,
        actual,
        sequence_before_repair,
        replica.last_sequence()
    );
}
