# Quotick

Quotick is a dependency-free Rust crate for deterministic, auditable financial
market infrastructure. It connects versioned instrument rules, pre-trade risk,
price-time-priority matching, single-price call auctions, level-2 market data,
durable recovery, and double-entry settlement in one replayable core.

Every financial state transition uses integer price quanta and lot quantities.
Every matching event is sequenced. Every durable runtime reconstructs its
state from a verified local write-ahead log (WAL). The result is a foundation
for systems where the same validated input must produce the same observable
output during live execution, recovery, and historical analysis.

The crate forbids `unsafe` code, has no runtime or dev dependencies, and
requires Rust 1.85 or later.

## Project goals

- **Deterministic execution:** make order priority, fills, auction uncrosses,
  risk reservations, market-data updates, and ledger postings reproducible
  from an ordered command stream.
- **Explicit financial semantics:** bind commands and trades to immutable,
  effective-time instrument definitions with checked fixed-point arithmetic.
- **Auditable recovery:** detect corruption, incomplete writes, sequence gaps,
  metadata drift, and replay divergence instead of silently accepting
  ambiguous state.
- **Composable infrastructure:** expose matching, risk, publication,
  journaling, and accounting as separate components with cross-auditable
  invariants.
- **Bounded behavior:** reserve every resource at construction, validate every
  limit envelope, and define supported inputs, complexity, and system
  boundaries precisely.

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
                                                   ledger-event WAL
                                                            |
                                                  durable account balances
```

An `OrderBook` represents one instrument-version shard and is mutated by one
execution thread; parallelism is obtained by running independent shards. A
separate bounded call-auction path (collection book plus sequenced phase
engine) follows the same shard model. The durable wrappers record commands or
ledger events before committing the corresponding in-memory transition, then
verify deterministic replay during recovery.

| Module | Provides |
| --- | --- |
| `domain` | Validated identifiers, integer fixed-point `Price` and `Quantity`, timestamps, accounting dates |
| `instrument` | Effective-time-versioned instrument catalog, per-version admission rules, settlement conventions |
| `calendar` | Immutable versioned UTC session schedules, day/session TIF normalization, expiry controls |
| `matching` | Bounded price-time-priority `OrderBook` with prepare/commit split, event traces, and checkpoints |
| `auction`, `auction_book`, `auction_engine` | Pure clearing-price and allocation kernels, bounded collection book, sequenced phase engine |
| `risk`, `auction_risk` | Immutable account profiles, conservative reservations, coupled matching and auction shards |
| `market_data`, `auction_market_data` | Anonymized L2 publishers and gap-detecting replicas for both trading models |
| `ledger` | Atomic multi-asset double-entry ledger with DVP trade settlement |
| `journal`, `snapshot` | CRC-32C WAL (single-file and segmented), `QSNP` semantic snapshots, A/B checkpoint-cutover primitives |
| `durable`, `durable_risk`, `durable_auction`, `durable_ledger` | Crash-recoverable single-writer runtimes |
| `codec` | Stable little-endian `BinaryCodec` implementations for durable records and calendar images |

## Quick start

Quotick is a library crate. The following program defines an instrument,
matches two orders under default limits, and reads the sequenced trade events:

```rust
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::{
    Command, CommandOutcome, EventKind, NewOrder, OrderBook, OrderDisplay, OrderType,
    SelfTradePrevention, TimeInForce,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity,
    Side, TimestampNs,
};

fn main() {
    // One immutable, validated instrument version.
    let definition = InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("TEST").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: false,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap();

    // A bounded price-time-priority book under `OrderBookLimits::default()`.
    let mut book = OrderBook::new(definition);

    // Rest a sell limit order: 5 lots at raw price 101.
    let ask = book
        .submit(Command::New(NewOrder {
            command_id: CommandId::new(1).unwrap(),
            order_id: OrderId::new(1).unwrap(),
            account_id: AccountId::new(11).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            side: Side::Sell,
            quantity: Quantity::new(5).unwrap(),
            display: OrderDisplay::FullyDisplayed,
            order_type: OrderType::Limit(Price::from_raw(101)),
            time_in_force: TimeInForce::GoodTilCancelled,
            self_trade_prevention: SelfTradePrevention::CancelAggressor,
            received_at: TimestampNs::from_unix_nanos(1),
        }))
        .unwrap();
    assert_eq!(ask.outcome, CommandOutcome::Accepted);

    // A crossing buy for 3 lots executes at the resting price.
    let report = book
        .submit(Command::New(NewOrder {
            command_id: CommandId::new(2).unwrap(),
            order_id: OrderId::new(2).unwrap(),
            account_id: AccountId::new(12).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            side: Side::Buy,
            quantity: Quantity::new(3).unwrap(),
            display: OrderDisplay::FullyDisplayed,
            order_type: OrderType::Limit(Price::from_raw(101)),
            time_in_force: TimeInForce::ImmediateOrCancel,
            self_trade_prevention: SelfTradePrevention::CancelAggressor,
            received_at: TimestampNs::from_unix_nanos(2),
        }))
        .unwrap();
    assert_eq!(report.outcome, CommandOutcome::Accepted);

    // Every state transition is a sequenced event in the execution report.
    for event in report.events.iter() {
        if let EventKind::Trade(trade) = event.kind {
            println!(
                "trade {}: {} lots @ {}",
                trade.trade_id,
                trade.quantity.lots(),
                trade.price.raw(),
            );
        }
    }

    // The unfilled 2 lots remain at the best ask.
    let remaining = book.best_ask().unwrap();
    assert_eq!((remaining.price.raw(), remaining.quantity), (101, 2));
}
```

`OrderBook::new` panics if the default constructor-time reservation cannot be
satisfied; production code uses `OrderBook::try_with_limits` and the other
`try_with_limits`-style constructors, which report the exact failing resource.
To persist the same command stream, wrap the book in
`durable::DurableOrderBook`, which appends each command to a WAL before the
book applies it.

## Executable examples

The [`examples/`](examples/readme.md) directory contains complete deterministic
programs built only from Quotick's public API. Each stateful program uses an
explicit finite resource envelope and checks its resulting invariants before
printing a summary.

| Example | End-to-end workflow |
| --- | --- |
| [`venue_session`](examples/venue_session.rs) | Calendar-relative admission through pre-trade risk, matching, level-2 publication, DVP settlement, and expiry |
| [`versioned_universe`](examples/versioned_universe.rs) | Effective-time instrument selection and version-bound shard routing |
| [`order_lifecycle`](examples/order_lifecycle.rs) | Reserve priority, hidden liquidity, sourced stop activation, GTD expiry, account fences, and instrument controls |
| [`indicative_cross`](examples/indicative_cross.rs) | Risk-managed call-auction collection, deterministic uncross, retained remainders, and complete-batch public replay |
| [`auction_restart`](examples/auction_restart.rs) | Durable auction checkpoint cutover, phase-transition suffix replay, and exact retry |
| [`signed_price_discovery`](examples/signed_price_discovery.rs) | Banded negative-price discovery, pressure policy, and exact order allocation |
| [`feed_repair`](examples/feed_repair.rs) | Sequence-gap detection, retained replay, snapshot fallback, and incremental continuation |
| [`clearing_ledger`](examples/clearing_ledger.rs) | Atomic funding, trade settlement, correction, period controls, trial balance, and reconciliation |
| [`durable_accounting`](examples/durable_accounting.rs) | Atomic batch and correction recovery across ledger checkpoint cutover and suffix replay |
| [`wal_recovery`](examples/wal_recovery.rs) | Durable coupled matching/risk recovery from an off-thread checkpoint and WAL suffix |
| [`segmented_cutover`](examples/segmented_cutover.rs) | Automatic WAL rotation, A/B checkpoint cutover, retained suffix recovery, and exact retry |
| [`state_handoff`](examples/state_handoff.rs) | Off-thread checkpoint verification, stable encoding, direct restore, and deterministic continuation |

Run any program with `cargo run --example <name>`.

## Capabilities

### Instrument model and arithmetic

- Bounded, append-only `InstrumentCatalog` with effective-time version
  selection; symbol, kind, and asset identities are immutable across versions
  of one instrument.
- Validated fixed-capacity asset codes and instrument symbols, tick grids,
  signed price collars (zero and negative prices are valid), lot increments,
  order-size bounds, and trading-state admission gating.
- Integer `Price` (`i64` quanta) and `Quantity` (non-zero `u64` lots) with
  explicit base and quote settlement multipliers feeding checked `i128`
  settlement arithmetic in the ledger.
- Allocation telemetry and an allocation-free structural cross-audit of every
  index and version history.

### Trading calendars

- Immutable `TradingCalendar` generations bind non-zero calendar/version
  identities to canonical UTC session rows with caller-supplied accounting
  dates, half-open entry windows, and explicit session/day expiry boundaries.
- Binary-search lookup finds active, next, ID-selected, and date-selected
  sessions. `Day` and `GoodForSession` ingress qualifiers normalize to the
  matching engine's existing absolute GTD lifetime; native TIF values pass
  through without requiring an active session.
- Boundary-checked factories produce the existing sequenced `ExpirySweep`, so
  matching order, idempotency, risk release, market-data projection, and
  recovery semantics are reused. Calendar code reads no clock and infers no
  time zone, holiday, early close, or venue-hours rule.
- A stable little-endian calendar payload preserves the immutable schedule;
  clones share its row and ID-index storage in `O(1)`.

### Continuous matching

- Deterministic price-time priority with ordered price levels, intrusive FIFO
  links, and mutation-maintained best-level caches giving `O(1)` best-price
  reads and `O(1)` maker-level mutation.
- Market and limit orders with GTC, GTD, IOC, minimum-quantity IOC, FOK, and
  post-only behavior
  (market orders can neither rest nor post); native reserve (iceberg) orders with
  fixed displayed peaks, hidden total leaves, bounded replenishment, and
  displayed-class-tail priority on refresh; and instrument-gated fully hidden
  resting limit orders with zero public depth and deterministic priority behind
  every displayed or reserve order at the same price.
- GTD orders use absolute UTC nanosecond deadlines and expire only through an
  explicit sequenced inclusive watermark advance. Expiry order is canonical
  `(deadline, OrderId)` order; no wall clock is read by the matching engine.
- Stop-market and stop-limit orders remain dormant and absent from public
  depth until an explicit sequenced last-trade reference reaches their trigger.
  Each reference carries durable source identity, source version, and source
  sequence; gaps, regressions, cursor conflicts, and unannounced source changes
  are nonmutating typed rejections.
  Bounded sweeps activate a canonical `(trigger, priority, OrderId)` prefix;
  eligible backlog must drain at the exact reference before its cursor advances.
- Cancel and replace with ownership checks and explicit priority-retention
  rules; account-scoped mass cancellation emitted in ascending `OrderId`
  order with exact final counts and cancelled-lot totals.
- Revision-checked account controls (block-and-cancel, enable) and instrument
  trading-state controls across open, cancel-only, halted, and closed,
  optionally cancelling every resting order in the same report when entering
  an entry-closing state; effective state and revision survive recovery.
- Cancel-aggressor, cancel-resting, cancel-both, and decrement-and-cancel
  self-trade prevention; atomic allocation-free FOK preflight.
- Exact-command idempotency: retries replay the cached report without
  consuming capacity, and `CommandId` reuse with different content is a typed
  collision error; event and trade sequences are strictly monotonic.
- A `prepare()`/`commit()` split that validates a command once against an
  immutable borrow so it can be persisted before mutating the book; stale or
  foreign preparations cannot commit.
- Constructor-reserved everything: stable-slot AVL price, GTD-expiry, and
  buy/sell stop-trigger arenas, one append-only event arena shared in `O(1)` by
  reports, the retry cache, checkpoints, and fixed dense/open-addressed hash
  indexes — the matching hot path never allocates, grows, or rehashes,
  including under deletion churn.
- Protected command-history and event tails that keep cancellation, expiry
  sweeps, block-and-cancel, and entry-closing transitions available when
  ordinary capacity is exhausted; residual-aware admission proves whether a
  resting order actually needs a capacity slot before rejecting it.

### Call auctions

- Pure analytical kernels: allocation-free banded clearing-price discovery
  that maximizes executable quantity, then minimizes absolute imbalance, then
  applies an explicit pressure/reference/tie-break policy; and deterministic
  market/price/class/time/ID price-time allocation that fallibly reserves
  exactly its result fill vectors.
- A bounded `CallAuctionBook` collecting crossed market/limit interest with
  never-reusable order identities, owner-checked cancellation, account/side-
  scoped mass cancellation through a bounded intrusive owner index,
  revision-bound indicative results, atomic new-identity cancel/replace with
  complete priority loss and saturated active/price-level capacity reuse when
  accepted-ID headroom remains, strict retained-priority active-quantity
  reduction with exact aggregate reconciliation, and a two-phase prepare/commit
  uncross over constructor-owned leased buffers with explicit remainder policies
  (`RetainAll`, `CancelMarket`, `CancelAll`); the only represented self-trade
  policy is explicit `Permit`.
- A sequenced `CallAuctionEngine` with explicit `Closed`/`Collecting`/`Frozen`
  phases, contiguous `AuctionId` cycles, revision-checked controls, exact
  command idempotency, exact one-event amendment and two-event replacement
  reports, canonical mass-cancel reports with aggregate completion, and a
  protected terminal lane
  guaranteeing that currently valid individual/non-empty mass cancellation,
  freeze/close, and uncross commands remain possible for a full book even at
  exhausted ordinary capacity.

### Pre-trade risk

- Immutable per-account profiles (active, reduce-only, blocked) registered
  only before the first sequenced command; sequenced entry fences are
  independent of the numeric profile.
- Per-order quantity and notional limits; aggregate resting-order count,
  quantity, and notional limits; worst-case long and short position limits
  across open exposure.
- Conservative reachable-price notional under the signed collar, correct for
  positive, zero-crossing, and negative price ranges, with checked `u128`
  arithmetic.
- Coupled shards for both continuous matching and call auctions: core
  business rejections always precede risk, and risk rejections are ordinary
  sequenced reports, never errors.
- Reservation lifecycle derived from the sequenced trace across fills,
  cancellation, GTD expiry, stop arming/activation, replacement, mass
  cancellation, account controls, and self-trade prevention; dormant stops
  reserve against their activation constraint without appearing in depth.
  Auction uncross releases both paired reservations and nets buys against sells
  once per account. Auction replacement nets out the target reservation before
  authorizing the replacement and preserves the target on rejection. Auction
  amendment releases the exact quantity/notional delta without a new risk gate.
- Full cross-audits between active orders, reservations, aggregates, and
  positions.

### Market data

- Exactly one anonymized public update per non-replayed matching or auction
  event at the identical source sequence; no account, order, or command
  identifiers are ever published.
- Absolute displayed level-2 quantity and order-count updates plus trade
  prints; reserve hidden leaves and fully hidden orders are never published as
  depth. Fully hidden executions publish an anonymized trade with a canonical
  absent maker level when no public level existed.
- Full-depth snapshots in canonical market-priority order; replicas detect
  missing, duplicated, and reordered updates. A constructor-reserved
  per-instrument replay ring repairs retained short gaps without allocation;
  older gaps recover by atomically swapping double-buffered, pre-reserved
  snapshot arenas.
- Call-auction replay retains exact batch starts and ends, never splits a
  multi-update uncross, two-update replacement, or mass-cancel removal/
  completion trace across pages, and
  advances replica event and command boundaries through the same
  preflight/application path as live batches. Replacement publishes anonymized
  `Replaced` removal then `Accepted` addition while the book revision advances
  once. Mass cancellation publishes only anonymized aggregate removals and one
  count/quantity/revision completion; account and scope remain private.
  Amendment publishes one anonymous aggregate delta with unchanged order count.
- Continuous publishers mirror dormant stop identities, trigger indices, and
  the committed reference privately to validate canonical activation. Stop-only
  state changes publish `NoBookChange`; triggered execution publishes ordinary
  depth, refresh, cancellation, and trade updates.
- Independent publisher/replica stacks for the continuous and auction paths
  with validated limit envelopes; publisher construction proves its envelope
  covers the source shard, and publishers cross-audit against the source.

### Accounting and settlement

- Atomic, balanced multi-asset journal entries with canonical leg order and
  exact per-asset totals via `LedgerMagnitude` (inline through `u128`,
  spilling to canonical `u64` limbs — no numerical ceiling).
- Explicit signed epoch-day effective dates, nondecreasing UTC booking
  timestamps, and delivery-versus-payment settlement bound to the exact
  instrument version.
- Entry-before-balance durability with prepared, generation-checked commits;
  exact-entry idempotency and typed transaction-ID collision detection.
- First-class reversals with exact posting-inverse proof and append-only
  reinstatement chains; atomic reversal-plus-replacement corrections; ordered
  multi-entry batches — each correction or batch is one event sequence and one
  WAL frame, replayed all-or-nothing.
- Idempotent period close/reopen controls with an inclusive dated posting
  fence; independent replay audits; arbitrary-magnitude trial balances; and
  exact-generation reconciliation against complete external balance
  statements.

### Durability and recovery

- Versioned CRC-32C WAL frames (format 12) with bounded payloads and
  contiguous sequences, as a single-file `Journal` or a size-bounded
  `SegmentedJournal` rotating whole frames and batches under one global
  sequence.
- Configurable acknowledgement durability (`Buffered`, `Flush`, `SyncData`,
  and the default `SyncAll`); grouped batch appends with one write and one
  barrier; poisoned-writer semantics after ambiguous I/O; canonical-path
  writer leases with explicit abandoned-writer recovery.
- Strict corruption detection: only a physically incomplete final frame may be
  repaired, and closed segments are always scanned strictly.
- Versioned, bounded `QSNP` semantic snapshots (format 12) with monotonic
  exact-prefix lineage and synchronized atomic replacement.
- Durable runtimes for matching, coupled risk/matching, call auctions, and
  coupled auction/risk record every command before committing the in-memory
  transition, verify deterministic replay on recovery, complete at most one
  interrupted report, and rebuild exact-retry caches without appending retry
  frames. `DurableLedger` follows the same discipline per ledger event: each
  entry, correction, or batch is one atomic WAL frame recorded before balances
  commit.
- Staged checkpoints for every durable runtime except the ledger: a
  WAL-barriered `capture_checkpoint_candidate` returns an immutable,
  non-encodable candidate whose consuming `verify()` performs the full
  deterministic replay — on another thread if desired — while the source shard
  keeps appending; publication is fenced by shard incarnation and cutover
  epoch. Capture resource failures never poison the shard; semantic
  contradictions do. `DurableLedger` checkpoints are synchronous.
- Crash-safe two-slot A/B checkpoint cutover for all five durable runtimes
  retires the WAL prefix behind a checkpoint anchor frame binding checkpoint
  kind, slot, generation, payload length, CRC-32C, and retired physical
  sequence.
  Verified captures carry a private physical cursor, so
  `compact_verified_checkpoint` streams only the synchronized post-capture
  suffix without repeating replay under writer exclusion. Segmented cutover
  publishes a new generation through a checksummed selector marker;
  single-file cutover stages and atomically renames.
- Stable little-endian codecs for every durable record. Every decoded
  collection proves its wire count against the remaining payload before one
  exact fallible reservation; every encoder write passes through one fallible
  growth gate, and partial encoded bytes never escape.

## Default resource envelopes

Every bounded mutable component reserves its complete configured envelope at
construction and never grows it afterward. Immutable calendars own their
caller-supplied finite row image plus one exact derived ID index. The defaults:

| Component | Defaults |
| --- | --- |
| Instrument catalog | 4,096 assets; 16,384 instruments; 65,536 definitions |
| `OrderBookLimits` | 4,096 active orders, including dormant stops; 4,096 active accounts; 4,096 price levels per side; 65,536 accepted order IDs; 65,536 account controls; 65,536 retained commands (final 4,096 reserved for cancellation-capable commands); 65,536 events per report; 262,144 retained events (final 4,097 protected); 2 order-selection buffers |
| Risk | 65,536 registered accounts (continuous and auction) |
| Call auction | 4,096 active orders; 4,096 limit prices per side; 65,536 accepted order IDs; 2 prepared-uncross buffer sets; 65,536 retained commands (final 4,098 terminal); 73,730 retained events (final 8,194 terminal); 8,193 events per report; 65,536 orders per side in the allocation kernel |
| Market data | 65,536 updates per continuous batch; 8,193 per auction batch; mirror envelopes default to the source components' limits; replay retention is an explicit non-zero caller bound |
| Ledger | 65,536 non-zero balance keys; 65,536 transactions; 32,768 reversal links; 65,536 records; 256 postings per transaction; 1,024 transactions per record; 262,144 retained postings |
| Storage | 16 MiB WAL frame payload; 1 GiB WAL segment; 1 GiB snapshot payload; `SyncAll` durability; strict recovery |

Protected tails are carved out of the retained totals, not added to them.
Detailed derivations, per-index hash headroom, and asymptotic bounds are in
[docs/complexity.md](docs/complexity.md).

## System boundary

Quotick implements deterministic local state machines and local WAL recovery
for single-instrument execution shards and a multi-asset ledger. It does not
implement gateways, authentication, distributed sequencing, replication or
consensus, portfolio collateral and margin, clearing lifecycle, network
market-data transport, administrative interfaces, or reporting systems.
Continuous and call-auction market data include process-local bounded suffix
replay rings, but no remote request/session, authentication, fanout, or
entitlement layer.

The matching model is a continuous price-time-priority book with sequenced
instrument-wide trading-state controls, plus a separate bounded call-auction
path with banded aggregate discovery, price-time allocation, and a
process-local atomic uncross. Continuous stop orders require an authoritative
external source to submit each sequenced reference; matching never infers one
from local trades or wall time. Source coordinates are validated and retained,
but source authentication, transport recovery, and raw-feed normalization are
external. The platform does not implement pegged orders,
discretionary ranges, cross-instrument or multi-leg execution, volatility-
interruption trigger logic, or venue-specific priority rule sets. Immutable
calendar images and day/session-to-GTD normalization are implemented, but
authoritative calendar ingestion, signed distribution, atomic activation,
original-request audit durability, and sequenced session-state transitions are
external. The auction path additionally provides no ledger effects, reference
or dynamic-band derivation, preventive self-trade policies, calendar-driven
phase scheduling, or venue-specific uncross rules.

The complete boundary, the failure model, and the register of environmental
assumptions are documented in
[docs/architecture.md](docs/architecture.md) and
[docs/assumptions.md](docs/assumptions.md).

## Documentation

| Document | Contents |
| --- | --- |
| [Architecture](docs/architecture.md) | System boundary, per-subsystem invariants, failure model, standards provenance, required production increments |
| [Assumption register](docs/assumptions.md) | 109 tagged assumptions (A1–A109), each with dependent results and a falsification probe |
| [Local storage contract](docs/storage.md) | Writer ownership, segmented directories, checkpoint cutover, durability conditions, failure/recovery matrix |
| [Complexity and resource bounds](docs/complexity.md) | Asymptotic time/space bounds and fixed-memory derivations for every subsystem |
| [Trading-calendar payload v1](docs/trading-calendar-v1.md) | Stable immutable UTC schedule payload and canonical decoder rules |
| [WAL format v12](docs/wal-v12.md) | Current write-ahead-log frame and record schema |
| [Snapshot format v12](docs/snapshot-v12.md) | Current `QSNP` semantic snapshot envelope and payload kinds |
| [Market-data payload v3](docs/market-data-v3.md) | Current continuous market-data update/snapshot payloads |
| [Auction market-data payload v4](docs/auction-market-data-v4.md) | Current call-auction market-data payloads |
| [Auction-risk checkpoint payload v1](docs/auction-risk-checkpoint-v1.md) | Current coupled call-auction risk checkpoint payload |

Historical formats whose envelopes the runtime rejects are retained as
byte-level provenance: [docs/wal-v3.md](docs/wal-v3.md),
[docs/wal-v4.md](docs/wal-v4.md),
[docs/wal-v5.md](docs/wal-v5.md),
[docs/wal-v6.md](docs/wal-v6.md),
[docs/wal-v7.md](docs/wal-v7.md),
[docs/wal-v8.md](docs/wal-v8.md),
[docs/wal-v9.md](docs/wal-v9.md),
[docs/wal-v10.md](docs/wal-v10.md),
[docs/wal-v11.md](docs/wal-v11.md),
[docs/snapshot-v2.md](docs/snapshot-v2.md),
[docs/snapshot-v3.md](docs/snapshot-v3.md),
[docs/snapshot-v4.md](docs/snapshot-v4.md),
[docs/snapshot-v5.md](docs/snapshot-v5.md),
[docs/snapshot-v6.md](docs/snapshot-v6.md),
[docs/snapshot-v7.md](docs/snapshot-v7.md),
[docs/snapshot-v8.md](docs/snapshot-v8.md),
[docs/snapshot-v9.md](docs/snapshot-v9.md),
[docs/snapshot-v10.md](docs/snapshot-v10.md),
[docs/snapshot-v11.md](docs/snapshot-v11.md), continuous
[market-data v2](docs/market-data-v2.md), and call-auction
[market-data v1](docs/auction-market-data-v1.md) and
[market-data v2](docs/auction-market-data-v2.md) and
[market-data v3](docs/auction-market-data-v3.md).

## Build and verify

```sh
cargo fmt --all -- --check
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

The test suite is dependency-free and fully deterministic: model-based and
differential tests drive tens of thousands of generated operations from
fixed-seed PRNGs against independent in-test reference models. Coverage
includes:

- **Matching and risk:** displayed/hidden queue classes, hidden and reserve
  admission and replenishment, GTD intake and canonical expiry sweeps, dormant
  stop intake, canonical bounded trigger sweeps, activation-time failures,
  mass cancellation, account and trading-state controls, every self-trade
  policy, risk rejection and reservation release, and capacity behavior at
  every configured bound.
- **Trading calendars:** schedule chronology and identity validation, exact
  half-open entry boundaries, multi-session trading dates, day/session TIF
  normalization, boundary-checked expiry controls, malformed payload rejection,
  core GTD/replay composition, and immutable storage sharing.
- **Call auctions:** discovery differentially checked against exhaustive
  tick-grid enumeration, allocation against literal order-priority walks,
  20,000 mixed book mutations and 10,000 uncross cases against independent
  models, canonical account/side mass cancellation across book, engine, risk,
  public-feed, checkpoint, and WAL recovery, retained-priority amendment across
  the same paths, and a 10,000-command engine phase-
  model run.
- **Market data and accounting:** continuous and complete-batch auction depth
  reconstruction, replay-first gap repair, snapshot fallback,
  allocation-stable ring wrap, settlement, reversals, corrections, batches,
  period controls, reconciliation, and signed `i128` boundaries.
- **Storage, recovery, and checkpoints:** stable wire layouts, segment
  rotation, corruption and torn-tail handling, injected write/sync/cutover
  failures, concurrent-writer exclusion, replay-divergence detection,
  off-thread checkpoint verification, and repeated A/B cutover in both
  physical layouts.
