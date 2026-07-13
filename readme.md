# Quotick

Quotick is a dependency-free Rust crate for deterministic, auditable financial
market infrastructure. It connects versioned instrument rules, pre-trade risk,
price-time-priority matching, level-2 market data, durable recovery, and
double-entry settlement in one replayable core.

Every financial state transition uses integer price quanta and lot quantities.
Every matching event is sequenced. Every durable runtime can reconstruct its
state from a verified local write-ahead log (WAL). The result is a foundation
for systems where the same validated input must produce the same observable
output during live execution, recovery, and historical analysis.

## Project goals

- **Deterministic execution:** make order priority, fills, risk reservations,
  market-data updates, and ledger postings reproducible from an ordered command
  stream.
- **Explicit financial semantics:** bind commands and trades to immutable,
  effective-time instrument definitions with checked fixed-point arithmetic.
- **Auditable recovery:** detect corruption, incomplete writes, sequence gaps,
  metadata drift, and replay divergence instead of silently accepting ambiguous
  state.
- **Composable infrastructure:** expose matching, risk, publication, journaling,
  and accounting as separate components with cross-auditable invariants.
- **Bounded behavior:** define supported inputs, resource complexity, storage
  assumptions, and system boundaries precisely.

## Architecture at a glance

```text
instrument definition
        |
validated command -> pre-trade risk -> WAL -> matching engine
        |                                      |
        |                                      +-> sequenced execution trace
        |                                                   |
        +--------------------------------------- reservations and positions
                                                            |
                                      L2 updates, trades, and snapshots
                                                            |
                                      gap-detecting market-data replica
                                                            |
                                                  version-bound settlement
                                                            |
                                         balanced multi-asset journal entry
                                                            |
                                                   durable account balances
```

An `OrderBook` represents one instrument-version shard and is mutated by one
execution thread. Parallelism is obtained by running independent shards. The
durable wrappers record commands or ledger entries before committing the
corresponding in-memory transition, then verify deterministic results during
recovery.

## Capabilities

### Instrument model and arithmetic

- Validated, fixed-capacity asset codes and instrument symbols.
- Append-only instrument versions selected by effective time.
- Tick grids, signed price collars, lot increments, order-size bounds, and
  trading-state admission.
- Integer price and quantity types, including valid zero and negative prices.
- Checked `i128` settlement arithmetic with explicit base and quote conversion
  multipliers.

### Matching and order lifecycle

- Deterministic price-time priority with ordered price levels and intrusive FIFO
  links.
- Market and limit orders with GTC, IOC, FOK, and post-only behavior.
- Cancel and replace with ownership checks and explicit priority-retention
  rules.
- Cancel-aggressor, cancel-resting, cancel-both, and decrement-and-cancel
  self-trade prevention.
- Atomic FOK preflight, exact-command idempotency, collision detection, and
  monotonic event and trade sequences.
- Complete execution traces suitable for replay, downstream publication, and
  risk reconstruction.

### Pre-trade risk

- Immutable account profiles with active, reduce-only, and blocked states.
- Per-order quantity and notional limits.
- Aggregate resting-order count, quantity, and notional limits.
- Worst-case long and short position limits across open exposure.
- Conservative reachable-price notional for positive, zero-crossing, and
  negative price collars.
- Trace-driven reservation release across fills, cancellation, replacement,
  and self-trade prevention.
- Cross-audits between active orders, reservations, aggregates, and positions.

### Market-data publication and recovery

- One public update for every non-replayed matching event, preserving the
  source sequence without exposing account, order, or command identifiers.
- Absolute level-2 quantity and order-count updates plus anonymized trade
  prints.
- Full-depth snapshots in canonical price order.
- Exact-retry suppression and publisher-to-book cross-audits.
- Consumer-side detection of missing, duplicated, and reordered updates.
- Snapshot-based replica recovery after an incremental sequence gap.

### Durability and codecs

- Stable little-endian codecs for definitions, risk profiles, commands,
  execution reports, market data, ledger entries, and ledger checkpoints.
- Versioned CRC-32C WAL frames with bounded payloads and contiguous sequences.
- Size-bounded physical WAL segments with automatic whole-frame and whole-batch
  rotation over one global logical sequence.
- Strict corruption detection and explicit repair of only a physically
  incomplete final frame; closed segments are always scanned strictly.
- Configurable buffered, flush, data-sync, and full-sync acknowledgements;
  `SyncAll` is the default.
- Grouped append barriers and poisoned-writer behavior after ambiguous I/O.
- Canonical-path exclusive writer and segment-manager leases with explicit
  abandoned-writer recovery.
- Versioned, bounded `QSNP` semantic snapshots with CRC-32C, monotonic
  exact-prefix lineage, synchronized same-filesystem replacement, and explicit
  abandoned-pending recovery.
- Definition-bound matching replay, profile-bound risk replay, interrupted
  report completion, and divergence detection.

### Accounting and settlement

- Atomic, balanced multi-asset journal entries with canonical leg order.
- Delivery-versus-payment trade settlement bound to the exact instrument
  version.
- Entry-before-balance durability with prepared, generation-checked commits.
- Exact-entry idempotency, transaction collision detection, and WAL-free exact
  retries.
- Full balance reconstruction from the canonical journal sequence.
- Independent journal/index replay and per-asset trial-balance audits.
- Canonical non-zero balance checkpoints retaining complete transaction
  history, plus durable recovery that proves the checkpoint against the exact
  WAL prefix and replays only its suffix.

The crate forbids `unsafe` code and has no runtime dependencies.

## System boundary

Quotick currently implements deterministic local state machines and local WAL
recovery for a single-instrument execution shard and a multi-asset ledger. It
does not implement gateways, authentication, distributed sequencing,
replication or consensus, portfolio collateral and margin, clearing lifecycle,
network market-data fanout, administrative interfaces, or reporting systems.

The exact invariants and environmental assumptions are documented in:

- [Architecture](docs/architecture.md)
- [Assumption register](docs/assumptions.md)
- [Local storage contract](docs/storage.md)
- [WAL format version 1](docs/wal-v1.md)
- [Semantic snapshot format version 1](docs/snapshot-v1.md)
- [Market-data payload format version 1](docs/market-data-v1.md)

## Build and verify

Quotick requires Rust 1.85 or later.

```sh
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

The test suites exercise financial-domain boundaries, price/FIFO priority,
order lifecycle behavior, all self-trade policies, risk rejection and
reservation release, market-data reconstruction, settlement and arithmetic
rollback, stable wire layouts, corruption and torn-tail handling, concurrent
writer exclusion, injected write and barrier failures, forced process
termination, recovery equivalence, and replay-divergence detection.
Segmented-storage tests additionally force record-by-record and whole-batch
rotation, cross-segment matching/risk/ledger replay, strict closed-segment
corruption handling, active-tail repair, manager exclusion, and interrupted
empty-segment recovery.
Checkpoint tests additionally exercise canonical trial balances, independent
entry replay, stable snapshot framing, corruption and generation forks,
pending-file recovery, WAL-prefix divergence, path ownership, and suffix replay
across physical segment boundaries.

## Complexity

For `P` occupied price levels and `M` matched resting orders, price discovery is
`O(log P)` and execution is `O((M + 1) log P)`. FIFO append is `O(log P)`;
cancellation is expected `O(1)` lookup plus `O(log P)` level maintenance. Active
matching state uses `O(O + P + C)` memory for `O` resting orders and `C` retained
idempotency reports.

Risk authorization and trace application are expected `O(1)` per order event.
A complete risk cross-audit is `O(O + A)` for `A` registered accounts, and risk
state uses `O(O + A)` memory.

Market-data trace and replica application are `O(log P)` for level-changing
events and `O(1)` for no-change events. Publisher bootstrap is `O(O log O + P)`,
a full-depth snapshot is `O(P)`, and a complete publisher cross-audit is
`O(O log O + P)`.

WAL scanning is `O(B + S)` for `B` persisted bytes across `S` physical segments.
A segmented reader retains `O(S)` descriptors and one bounded payload rather
than the complete WAL. Appending a frame performs `O(F)` checksum and copy work
for frame length `F`; a `JournalBatch` amortizes one write and one configured
durability barrier across multiple frames. Rotation adds one closing barrier,
exclusive file creation, and a parent-directory barrier at a size boundary.

For `E` retained ledger entries, `L` posting legs, and `A` non-zero account
balances, checkpoint capture/validation is linear in `E + L + A` apart from
ordered validation-map factors, and retained checkpoint state is
`O(E + L + A)`. Checkpoint-assisted durable open still scans all `B` WAL bytes
to prove the exact prefix; it does not yet bound restart time or authorize WAL
retention.
