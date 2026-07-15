//! Effective-time instrument selection and version-bound order routing.

mod support;

use quotick::Side;
use quotick::instrument::{
    AssetCode, AssetDefinition, InstrumentCatalog, InstrumentCatalogLimitsSpec, TradingState,
};
use quotick::matching::{CommandOutcome, OrderBook, RejectReason};

use support::{LimitOrder, asset, definition_version, matching_limits, timestamp};

fn main() {
    let mut catalog = InstrumentCatalog::try_with_limits(InstrumentCatalogLimitsSpec {
        max_assets: 4,
        max_instruments: 2,
        max_instrument_versions: 8,
    })
    .expect("catalog storage is reserved");
    catalog
        .register_asset(
            AssetDefinition::new(asset(1), AssetCode::new("ALPHA").unwrap(), 8).unwrap(),
        )
        .unwrap();
    catalog
        .register_asset(AssetDefinition::new(asset(2), AssetCode::new("USD").unwrap(), 2).unwrap())
        .unwrap();

    let first = definition_version("ALPHA-USD", 1, 1_000, 1, TradingState::Open);
    let second = definition_version("ALPHA-USD", 2, 2_000, 5, TradingState::Open);
    catalog.register_instrument(first).unwrap();
    catalog.register_instrument(second).unwrap();

    assert_eq!(
        catalog
            .effective(first.instrument_id(), timestamp(1_999))
            .unwrap()
            .version(),
        first.version()
    );
    let active = *catalog
        .effective(first.instrument_id(), timestamp(2_000))
        .unwrap();
    assert_eq!(active.version(), second.version());
    assert_eq!(active.price_rules().tick_size(), 5);

    let mut current_shard = OrderBook::try_with_limits(active, matching_limits()).unwrap();
    let stale_route = LimitOrder::resting(1, 1, 11, Side::Buy, 10, 10_000).command(first);
    let rejected = current_shard.submit(stale_route).unwrap();
    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::WrongInstrumentVersion)
    );

    let accepted = LimitOrder::resting(2, 2, 11, Side::Buy, 10, 10_000).command(active);
    assert_eq!(
        current_shard.submit(accepted).unwrap().outcome,
        CommandOutcome::Accepted
    );
    current_shard.validate().unwrap();
    catalog.validate().unwrap();

    println!(
        "effective version={} tick={} active_orders={}",
        active.version(),
        active.price_rules().tick_size(),
        current_shard.active_order_count()
    );
}
