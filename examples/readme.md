# Quotick executable examples

These programs compose Quotick's public modules into bounded, deterministic
workflows. They use fixed inputs and executable assertions, require no network
services, and print a compact result only after their cross-checks succeed.

| Program | Subsystems exercised |
| --- | --- |
| [`venue_session.rs`](venue_session.rs) | `calendar`, `risk`, `matching`, `market_data`, and `ledger` |
| [`versioned_universe.rs`](versioned_universe.rs) | `instrument` effective-time histories and version-bound matching |
| [`order_lifecycle.rs`](order_lifecycle.rs) | Reserve, hidden, stop, GTD, account-control, and trading-state transitions |
| [`indicative_cross.rs`](indicative_cross.rs) | `auction_engine`, `auction_risk`, and `auction_market_data` |
| [`signed_price_discovery.rs`](signed_price_discovery.rs) | Signed-price auction discovery and price-time allocation kernels |
| [`feed_repair.rs`](feed_repair.rs) | Continuous market-data publication, gap detection, and snapshot repair |
| [`clearing_ledger.rs`](clearing_ledger.rs) | Batches, DVP settlement, corrections, period controls, and reconciliation |
| [`durable_accounting.rs`](durable_accounting.rs) | Ledger checkpoint cutover, suffix replay, and exact-retry persistence |
| [`wal_recovery.rs`](wal_recovery.rs) | Durable coupled recovery, asynchronous checkpoint verification, and exact retry |
| [`segmented_cutover.rs`](segmented_cutover.rs) | Segmented matching WAL rotation, generation cutover, and suffix recovery |
| [`state_handoff.rs`](state_handoff.rs) | In-memory checkpoint capture, codec round trip, restore, and continuation |
| [`auction_restart.rs`](auction_restart.rs) | Auction checkpoint recovery through collection, freeze, and uncross |

Run one program from the repository root:

```sh
cargo run --example venue_session
```

Run the complete suite:

```sh
for example in \
  venue_session \
  versioned_universe \
  order_lifecycle \
  indicative_cross \
  signed_price_discovery \
  feed_repair \
  clearing_ledger \
  durable_accounting \
  wal_recovery \
  segmented_cutover \
  state_handoff \
  auction_restart
do
  cargo run --quiet --example "$example"
done
```

`examples/support/mod.rs` contains shared validated definitions, explicit
resource envelopes, risk profiles, order factories, and temporary-storage
cleanup used by the stateful programs. It is not a public Quotick module.
