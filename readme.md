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
                                                   ledger-event WAL
                                                            |
                                                  durable account balances
```

An `OrderBook` represents one instrument-version shard and is mutated by one
execution thread. Parallelism is obtained by running independent shards. The
durable wrappers record commands or ledger events before committing the
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
- Mutation-maintained complete best-level caches providing allocation-free
  constant-time best price, FIFO head, displayed quantity, and order-count reads,
  with independent ordered-map cross-audit.
- Instrument-gated native reserve orders with fixed displayed peaks, hidden
  total leaves, bounded replenishment, and FIFO-tail priority on refresh.
- Market and limit orders with GTC, IOC, FOK, and post-only behavior.
- Cancel and replace with ownership checks and explicit priority-retention
  rules.
- Account-scoped mass cancellation across all active orders or one side, selected
  through mutation-maintained account/side indexes and emitted in ascending
  `OrderId` order with exact final order-count and cancelled-lot totals.
- Cancel-aggressor, cancel-resting, cancel-both, and decrement-and-cancel
  self-trade prevention.
- Atomic FOK preflight, exact-command idempotency, collision detection, and
  monotonic event and trade sequences.
- Allocation-free FOK liquidity inspection that uses total reserve leaves where
  reachable and current displayed slices before an aggressor-blocking self-order
  barrier, without constructing replenishment queues.
- Validated finite matching limits for active orders/accounts, occupied levels,
  accepted identifiers, retained reports, and events per report; optional
  constructor-time hash reservation prevents index growth/rehash through the
  selected maxima.
- A protected retained-history tail sized to at least the maximum active-order
  population. Ordinary admission closes first; only currently valid cancel and
  mass-cancel controls may consume the reserve, and exact retries bypass all
  capacity gates.
- Residual-aware capacity admission for GTC/post-only orders. A boundary scan
  proves whether fills or self-trade prevention leave anything to rest, without
  mutating the book or materializing reserve replenishment slices, and accounts
  exactly for maker-order and complete-account capacity released before append.
- Opaque generation-bound command preparation shared by direct matching, risk,
  and durable orchestration. Capacity, identifier, FOK, and core business checks
  execute once; the token owns the fallibly reserved report buffer, and durable
  paths append the prepared command only after that reservation succeeds.
- Immutable `EventTrace` storage shared in `O(1)` across the returned report,
  idempotency cache, exact retries, and in-memory checkpoints. Diagnostic trace
  mutation is explicit copy-on-write and cannot alter cached history.
- Mutation-maintained per-level, per-side, and per-account future event-work
  aggregates. Preparation derives safe command-specific event/trade bounds in
  constant time, checks identifier space, and allocates the complete report
  vector before matching; event insertion cannot grow it during mutation.
- Complete execution traces suitable for replay, downstream publication, and
  risk reconstruction.

### Pre-trade risk

- Immutable account profiles with active, reduce-only, and blocked states.
- Per-order quantity and notional limits.
- Aggregate resting-order count, quantity, and notional limits.
- Worst-case long and short position limits across open exposure.
- Conservative reachable-price notional for positive, zero-crossing, and
  negative price collars.
- Trace-driven reservation release across fills, individual and mass
  cancellation, replacement, and self-trade prevention.
- Cross-audits between active orders, reservations, aggregates, and positions.

### Market-data publication and recovery

- One public update for every non-replayed matching event, preserving the
  source sequence without exposing account, order, or command identifiers.
- Absolute displayed level-2 quantity and order-count updates plus anonymized
  trade prints; hidden reserve leaves are not published as depth.
- Full-depth snapshots in canonical price order.
- Exact-retry suppression and publisher-to-book cross-audits.
- Consumer-side detection of missing, duplicated, and reordered updates.
- Snapshot-based replica recovery after an incremental sequence gap.

### Durability and codecs

- Stable little-endian codecs for definitions, risk profiles, commands,
  execution reports, market data, ledger entries, atomic corrections,
  generalized ledger batches, and matching and ledger checkpoints.
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
- Definition-bound matching replay, direct matching-state checkpoint recovery,
  profile-bound coupled risk-state checkpoint recovery, interrupted report
  completion, exact WAL-prefix proof, suffix-only state transitions, and
  divergence detection.

### Accounting and settlement

- Atomic, balanced multi-asset journal entries with canonical leg order and
  exact, order-independent per-asset side totals beyond fixed-width aggregate
  limits.
- Explicit signed epoch-day effective dates and nondecreasing UTC nanosecond
  booking timestamps on every financial journal event.
- Delivery-versus-payment trade settlement bound to the exact instrument
  version.
- Entry-before-balance durability with prepared, generation-checked commits.
- Exact-entry idempotency, transaction collision detection, and WAL-free exact
  retries.
- First-class reversal entries with exact posting-inverse proof, one reversal
  per target, append-only reinstatement chains, and durable lineage recovery.
- Atomic reversal-plus-replacement corrections with one event sequence, one
  CRC-protected WAL frame, direct final-balance arithmetic, exact retries, and
  all-or-neither torn-tail recovery.
- Generalized ordered batches of two or more distinct entries with one event
  sequence and WAL frame, lifecycle/reversal validation against an in-batch
  overlay, direct aggregate final-balance arithmetic, exact grouped retries,
  and all-or-neither single/segmented recovery.
- Idempotent zero-posting period close/reopen controls with an inclusive dated
  posting fence reconstructed by WAL and checkpoint replay.
- Full balance reconstruction from the canonical journal sequence.
- Independent journal/index replay and arbitrary-magnitude per-asset
  trial-balance audits, retaining totals inline through `u128::MAX` and spilling
  to canonical `u64` limbs only when required.
- Exact-generation reconciliation against canonical complete external balance
  statements, including deterministic external-minus-ledger break reports.
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

The test suites cover:

- **Matching and risk:** financial-domain boundaries, price/FIFO priority,
  reserve admission and replenishment, hidden-versus-displayed quantity,
  canonical mass cancellation, every self-trade policy, risk rejection, and
  reservation release.
- **Market data and accounting:** displayed-depth reconstruction, settlement,
  arithmetic rollback, exact reversal and reinstatement chains, indivisible
  reversal-plus-replacement corrections, generalized multi-entry netting,
  ordered in-batch period/reversal transitions, grouped replay and
  partial-commitment rejection, period controls, external-statement
  reconciliation, and signed `i128` boundaries.
- **Storage and recovery:** stable wire layouts, record and whole-batch segment
  rotation, cross-segment matching/risk/ledger replay, strict closed-segment
  corruption handling, active-tail repair, concurrent-writer exclusion,
  injected write and barrier failures, forced termination, and replay-divergence
  detection.
- **Checkpoints:** canonical matching FIFO/reserve/STP state, coupled risk
  positions and total-leaves reservations, independent replay audits, stable
  snapshot framing, semantic corruption, immutable-profile binding, WAL-prefix
  divergence, generation forks, pending-file recovery, exact retries, and
  suffix replay across segment boundaries.

## Complexity

For `P` occupied price levels and `E` maker-slice interactions, cached
best-price, best-order, and best-level-snapshot discovery is `O(1)`. Ordered
level insertion, deletion, and next-worse traversal are `O(log P)`; execution is
`O((E + 1) log P)`. The cache removes one ordered-tree traversal from each
best-maker selection, while the authoritative level aggregate update remains
`O(log P)`. A reserve order can contribute more than one interaction through
replenishment. FIFO append and removal maintain one account/side `BTreeSet`,
giving `O(log P + log K_a)` work for an account with `K_a` active orders.
Reserve refresh preserves account membership and avoids that index churn. For
`K` selected orders, a mass cancel detaches the ordered set in `O(K)`, emits
already-canonical IDs without sorting, and performs `K` price-level removals in
`O(K log P)`, independent of total active-order count `O`. Active matching state
uses `O(O + P + C)` memory for `O` resting orders and `C` retained idempotency
reports; account indexing adds one ordered membership per active order within
the same `O(O)` bound, while the two complete best-level caches add `O(1)`
space.

FOK preflight over `O_c` active orders in `P_c` crossed levels is
`O(O_c + P_c log P)` time and `O(1)` auxiliary space. Each inspected order is
visited at most once; complexity is independent of the number of reserve slices
that subsequent execution emits.

Default matching limits are 4,096 active orders, 4,096 active accounts, 4,096
occupied prices per side, 65,536 accepted order IDs, 65,536 retained commands,
and 65,536 events per report, with the final 4,096 history slots reserved for
valid cancellation controls. The report limit must be at least
`max_active_orders + 1`, preserving one cancellation event per maximally active
order plus the mass-cancel completion event. Defaults are finite but do not
preallocate; production constructors accept an explicit `OrderBookLimits`, and
`preallocate=true` reserves all four hash indexes to their maxima.
`try_with_limits` reports the exact hash resource when a requested reservation
cannot be represented or allocated. Even when construction does not preallocate,
command preparation fallibly reserves the hash headroom needed for retained
history, a newly accepted identity, and any possible new active order/account;
it also reserves the complete event buffer and `K`-identifier mass-cancel
selection buffer before durable command append. Capacity preflight is expected
`O(1)` on the normal path, except for
validating a reserve-lane control through the ordinary core lookup. If an
active-order, active-account, or same-side price-level bound
is already full, a GTC/post-only limit order performs an allocation-free
residual preview in `O(O_c + P_c log P)` time and `O(1)` auxiliary space for
`O_c` orders in `P_c` crossed levels. A proved no-residual order bypasses the
resting-capacity gate. If a new account arrives at a full account bound, exact
release proof can additionally inspect all `O` active account memberships in
expected `O(O)` time and `O(1)` auxiliary space. Active-order, active-account,
and same-side price-level decisions use the exact final cardinalities. A
price-changing replacement whose old level remains occupied uses the same
allocation-free liquidity proof only at a full same-side level bound: a full
fill or aggressor-terminating STP result creates no target level, while a
resting residual is rejected when no level slot is released. Capacity
errors are not sequenced and durable wrappers reject them before WAL append.
IOC, FOK, and market orders do not consume a resting-capacity gate.
One `PreparedCommand` carries the completed operational/core proof through risk
authorization and WAL append, together with the already-reserved unique report
buffer and optional mass-cancel selection buffer. Matching hash-table insertion
cannot rehash after command persistence. Commit validates book identity and
retained-command generation in expected `O(1)` time; foreign and stale tokens
cannot mutate state. Ordered price/account tree nodes, the Arc control block,
and coupled risk reservation insertion remain outside this fallible preparation
boundary.
For `E` report events, construction and encoding are `O(E)`, while Arc-backed
builder finalization is `O(1)` and retains the original vector event buffer
without allocation or event copy. Cache, retry, and checkpoint trace clones are
`O(1)` time and space per handle and do not allocate or copy that buffer. Report
encoding emits the unchanged ordered event sequence. Preparation computes a safe
event/trade bound in `O(1)` from mutation-maintained side and account work
aggregates and allocates that vector capacity before the first matching
transition, so report insertion cannot reallocate. The bound includes uncrossed
opposite-side work and may retain unused capacity.

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

For `C` retained matching commands, `O` active orders, and `P` price levels, a
matching checkpoint retains `O(C + O + P)` state. Capture performs one
independent full-history matching replay plus structural audit synchronously
under exclusive shard access. Checkpoint-assisted open still scans all WAL
bytes and decodes the exact command/report prefix, but
reconstructs indices in `O(O log P)` and executes matching transitions only for
the suffix. It does not bound memory, WAL scan time, or authorize prefix
retention.

For `C` retained risk-managed commands, `O` active orders, `P` price levels,
and `A` accounts, a coupled risk checkpoint retains `O(C + O + P + A)` state.
Capture independently replays the complete coupled history and audits derived
total-leaves reservations and redundant exposure synchronously under exclusive
shard access. Checkpoint-assisted open still scans every WAL byte for exact
metadata/command/report lineage but executes matching/risk transitions only for
the suffix. It does not bound capture pause, memory, WAL scan time, or authorize
prefix retention.

For `R` retained ledger records, `E` contained transaction entries, `L` posting
legs, and `A` non-zero account balances, checkpoint capture/validation is linear
in `R + E + L + A` apart from ordered validation-map factors, and retained
checkpoint state is `O(R + E + L + A)`. Checkpoint-assisted durable open still
scans all `B` WAL bytes to prove the exact prefix; it does not yet bound restart
time or authorize WAL retention.

Reversal validation is `O(L)` for the target entry's posting legs plus expected
`O(1)` transaction/reversal-index access. Correction balance preparation is
`O(Lᵣ + Lₚ)` time and auxiliary state for the distinct posting keys in its
reversal and replacement. For a batch with `N` entries, `L` posting legs, and
`U` affected `(account, asset)` keys, construction proves unique transaction
IDs in `O(N log N)` time and `O(N)` memory. Preparation is expected `O(N + L)`
time after construction and uses `O(N + L + U)` auxiliary memory for lifecycle
overlays, exact signed terms, and final balance updates; it is independent of
unaffected ledger balances. Commit is expected `O(N + U)` time. For `A`
internal non-zero balances, `V` asset denominations, and `W` spilled `u64`
magnitude limbs, trial-balance construction is amortized `O(A log V)` time and
`O(V + W)` memory. Magnitude addition is allocation-free through
`u128::MAX`, amortized constant time after spill, and `O(W_v)` in a worst-case
carry chain for one asset's `W_v` limbs. For `S` external statement balances
and `D` reported breaks, reconciliation is
`O(A log A + S)` time and `O(A + D)` auxiliary memory; output is canonical and
contains no zero differences.
Period close/reopen validation and the effective-date fence are `O(1)` time and
state; their immutable journal history remains included in `E` above.
