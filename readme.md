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
  with independent indexed-AVL cross-audit.
- Finitely bounded stable-slot AVL price arenas reserved before a book exists,
  with allocation-free `O(log P)` level mutation, intrusive vacant-slot reuse,
  full structural auditing, and per-side allocation/high-water telemetry.
- Instrument-gated native reserve orders with fixed displayed peaks, hidden
  total leaves, bounded replenishment, and FIFO-tail priority on refresh.
- Market and limit orders with GTC, IOC, FOK, and post-only behavior.
- Cancel and replace with ownership checks and explicit priority-retention
  rules.
- Account-scoped mass cancellation across all active orders or one side, selected
  through mutation-maintained account/side indexes and emitted in ascending
  `OrderId` order with exact final order-count and cancelled-lot totals.
- Revision-checked account admission controls: block-and-cancel atomically
  closes entry and removes all resting orders in canonical order; enable reopens
  entry. Exact retries preserve the committed revision and stale revisions are
  sequenced rejections.
- Revision-checked instrument trading-state controls: transition among open,
  cancel-only, halted, and closed states, optionally cancelling every resting
  order in ascending `OrderId` order in the same report. Effective state and
  revision survive WAL/checkpoint recovery and are published to level-2 replicas.
- Cancel-aggressor, cancel-resting, cancel-both, and decrement-and-cancel
  self-trade prevention.
- Atomic FOK preflight, exact-command idempotency, collision detection, and
  monotonic event and trade sequences.
- Bounded crossed-interest call-auction order books with anonymized aggregate
  market/limit depth, phase and cycle updates, two-sided trade prints, final
  clearing summaries, full-depth snapshots, and gap-detecting replicas.
- Allocation-free FOK liquidity inspection that uses total reserve leaves where
  reachable and current displayed slices before an aggressor-blocking self-order
  barrier, without constructing replenishment queues.
- Validated finite matching limits for active orders/accounts, occupied levels,
  accepted identifiers, retained account controls, reports, and events per
  report. Mandatory constructor-time reservation establishes complete semantic
  headroom; stable price arenas do not grow, while standard-library active and
  risk hashes may still rehash under long deletion/insertion churn.
- A protected retained-history tail sized to at least the maximum active-order
  population. Ordinary admission closes first; only currently valid cancel and
  mass-cancel, block-and-cancel, and entry-closing instrument transition-and-cancel
  controls may consume the reserve; reopening controls remain ordinary admission,
  and exact retries bypass all capacity gates.
- Residual-aware capacity admission for GTC/post-only orders. A boundary scan
  proves whether fills or self-trade prevention leave anything to rest, without
  mutating the book or materializing reserve replenishment slices, and accounts
  exactly for maker-order and complete-account capacity released before append.
- Opaque generation-bound command preparation shared by direct matching, risk,
  and durable orchestration. Capacity, identifier, FOK, and core business checks
  execute once against an immutable book borrow; the token owns fallibly reserved
  report/mass-cancel-or-control buffers, all hash headroom already belongs to the constructed
  shard, and durable paths append the prepared command only after per-command
  reservations succeed.
- Immutable `EventTrace` storage shared in `O(1)` across the returned report,
  idempotency cache, exact retries, and in-memory checkpoints. Diagnostic trace
  mutation is explicit copy-on-write and cannot alter cached history.
- Mutation-maintained per-level, per-side, and per-account future event-work
  aggregates. Preparation derives safe command-specific event/trade bounds in
  constant time, checks identifier space, and allocates the complete report
  vector before matching; event insertion cannot grow it during mutation.
- Complete execution traces suitable for replay, downstream publication, and
  risk reconstruction.
- A venue-neutral, allocation-free call-auction analytical kernel over canonical
  aggregate depth and market interest. It evaluates complete demand/supply
  intervals inside an explicit aligned candidate-price band, maximizes
  executable quantity, minimizes absolute imbalance, and applies an explicit
  optional pressure/reference/final-price policy. A bounded linear
  allocator then reconciles eligible order totals and produces deterministic
  market/price/class/time/ID per-side fills. The kernels remain pure; the
  collection book consumes their revision-bound plan through a separate
  process-local uncross transition.
- A separate bounded `CallAuctionBook` admits crossed market/limit interest for
  one instrument version, assigns internal FIFO priority, retains never-reusable
  order identities, supports owner-checked cancellation, and supplies canonical
  aggregate and order input to both analytical kernels. Its identity and price
  indexes are constructor-reserved stable-slot AVL arenas, so bounded collection
  mutation and analysis scratch do not allocate under identity churn. Indicative
  results are bound to a process-local book identity and exact mutation revision,
  preventing stale or cross-book allocation. Move-only uncross preparation
  requests capacity for the complete deterministic buyer/seller trade pairs and
  selected remainder cancellations; allocation-free commit validates the preparation,
  applies every fill and remainder atomically, assigns contiguous book-local
  trade identifiers, and advances one revision. Remainder treatment is explicit
  (`RetainAll`, `CancelMarket`, or `CancelAll`); the only represented auction
  self-trade policy is explicit `Permit`.
- A bounded `CallAuctionEngine` owns that collection book and makes auction
  lifecycle explicit: `Closed`, `Collecting`, and `Frozen`, exact contiguous
  `AuctionId` cycles, revision-checked controls, cycle/revision-fenced entry,
  phase-gated uncross, sequenced business rejections, and exact-command
  idempotency with shared immutable traces. A protected history lane remains
  available only to currently valid cancellation, freeze/close, and executable
  uncross commands;
  successful uncross closes the cycle, while explicit close retains interest
  and owner cancellation remains available in every phase.
- Stable little-endian call-auction command/report codecs and a WAL-version-4
  `DurableCallAuctionEngine` for single-file or segmented storage. Recovery
  verifies every deterministic command/report pair, completes at most one final
  dangling non-retry command, rejects divergent or noncanonical retry history,
  and reconstructs exact-retry cache identity without appending retry frames.
  Snapshot-version-4 call-auction checkpoints retain canonical phase, cycle,
  book, counters, accepted identities, active orders, and complete exact-retry
  history. Uncut recovery proves the checkpoint against the exact WAL prefix;
  single-file and segmented A/B cutover replace that prefix with an
  anchor-bound checkpoint and replay only the suffix.

### Pre-trade risk

- Independent `RiskManagedLimits` policy with a finite registered-account
  maximum, complete constructor-time profile/reservation hash headroom, and
  per-index configured/allocated/occupied telemetry.
- Immutable profile bootstrap remains open only before the first sequenced
  command; subsequent registration fails without changing state.
- Immutable account profiles with active, reduce-only, and blocked states.
- Mutable shard-local entry fences are sequenced independently of immutable
  numerical profiles; coupled risk accepts controls only for registered accounts.
- Per-order quantity and notional limits.
- Aggregate resting-order count, quantity, and notional limits.
- Worst-case long and short position limits across open exposure.
- Conservative reachable-price notional for positive, zero-crossing, and
  negative price collars.
- Coupled call-auction admission with core-first sequenced risk rejections;
  every accepted market or limit order reserves the maximum reachable absolute
  signed-collar price magnitude for its active leaves.
- Auction uncross accounting that releases both paired reservations, applies
  explicit remainder cancellation, and nets all buys/sells once per account so
  permitted same-account pairs have zero position effect.
- Canonical coupled auction/risk checkpoints with independently replayed
  command lineage, active-order reservation reconstruction, stable
  [little-endian encoding](docs/auction-risk-checkpoint-v1.md), exact retry
  continuity, and explicit restore limits.
- Trace-driven reservation release across fills, individual, mass, and atomic
  account-control cancellation, replacement, and self-trade prevention.
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
- Crash-safe two-slot checkpoint cutover for single-file and segmented matching,
  coupled risk/matching, and ledger WALs. A versioned anchor binds the selected
  checkpoint kind, generation, payload length, CRC-32C, and retired physical
  sequence; segmented storage publishes it through a checksummed generation
  selector before suffix appends continue under the same manager lease.
- Definition-bound matching replay, direct matching-state checkpoint recovery,
  profile-bound coupled risk-state checkpoint recovery, interrupted report
  completion, exact uncut-WAL prefix proof, anchor-bound compacted-WAL recovery,
  suffix-only state transitions, and divergence detection.

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
  history, plus durable recovery that proves an uncut checkpoint against the
  exact WAL prefix or a compacted checkpoint against its anchor, then replays
  only the suffix.

The crate forbids `unsafe` code and has no runtime dependencies.

## System boundary

Quotick currently implements deterministic local state machines and local WAL
recovery for a single-instrument execution shard and a multi-asset ledger. It
does not implement gateways, authentication, distributed sequencing,
replication or consensus, portfolio collateral and margin, clearing lifecycle,
network market-data fanout, administrative interfaces, or reporting systems.
The matching model is a continuous price-time-priority book with sequenced
instrument-wide open/cancel-only/halted/closed controls. A separate bounded
call-auction collection book admits crossed market/limit interest and feeds
statically banded aggregate discovery plus price-time allocation and a
process-local atomic uncross. A sequenced engine adds phase
control, exact auction/revision checks, command idempotency, and immutable
execution reports. New entry is bound to the active cycle and exact phase
revision; cancellation is deliberately revision-independent. Explicit close
retains collection interest. The auction path keeps owner cancellation
available in every phase. Stable command/report codecs, semantic checkpoints,
exact-prefix verification, and single-file/segmented A/B WAL cutover are
implemented. The coupled auction-risk path provides profile admission,
conservative reservations, position effects, versioned snapshots, and durable
single-file/segmented recovery with A/B cutover. The auction
path also does not provide ledger effects, reference or dynamic-band
derivation, preventive self-trade policies, market-data transport, settlement,
controller authentication, calendar/session scheduling, or venue-specific
uncross rules.
The platform also does not implement stop or pegged orders, discretionary
ranges, day/GTD expiry, cross-instrument or multi-leg execution,
volatility-interruption trigger logic, or venue-specific priority rule sets.

The exact invariants and environmental assumptions are documented in:

- [Architecture](docs/architecture.md)
- [Assumption register](docs/assumptions.md)
- [Local storage contract](docs/storage.md)
- [WAL format version 4](docs/wal-v4.md)
- [Semantic snapshot format version 4](docs/snapshot-v4.md)
- [Market-data payload format version 2](docs/market-data-v2.md)
- [Coupled call-auction risk checkpoint payload version 1](docs/auction-risk-checkpoint-v1.md)

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
  canonical mass cancellation, revisioned account fencing and atomic kill
  cancellation, every self-trade policy, risk rejection, reservation release,
  full-domain and banded market-aware call-auction price discovery
  differentially checked against exhaustive tick-grid enumeration, and
  price-time allocation differentially checked against literal order-priority
  walks; crossed collection admission/cancellation and 20,000 mixed mutations
  differentially checked against independent aggregate and priority models;
  process-local atomic uncross pairing and post-state images checked against
  10,000 literal two-pointer and remainder-policy models; sequenced auction
  lifecycle checked against a 10,000-command literal phase model, including
  exact retry/collision, stale/foreign preparation, delayed-entry fencing,
  protected-history limits, invalid transitions, and sequence/revision
  exhaustion; coupled auction risk checked for core-first rejection, signed-
  collar reservation, reduce-only aggregate exposure, partial-fill/remainder
  release, same-account netting, capacity stability, and exact retry; and partial residual
  leaves below the entry minimum checked across both auction commit and
  continuous-book checkpoint restoration.
- **Market data and accounting:** displayed-depth reconstruction, settlement,
  arithmetic rollback, exact reversal and reinstatement chains, indivisible
  reversal-plus-replacement corrections, generalized multi-entry netting,
  ordered in-batch period/reversal transitions, grouped replay and
  partial-commitment rejection, period controls, external-statement
  reconciliation, and signed `i128` boundaries.
- **Storage and recovery:** stable wire layouts, record and whole-batch segment
  rotation, cross-segment matching/risk/ledger replay, strict closed-segment
  corruption handling, active-tail repair, concurrent-writer exclusion,
  injected write, acknowledgement, and cutover-directory barrier failures,
  explicit abandoned-cutover staging recovery, forced termination, and
  replay-divergence detection; stable call-auction command/report bytes,
  single/segmented full-prefix replay, torn final-report completion, exact-retry
  frame suppression, and rejection of divergent or noncanonical retry history.
- **Checkpoints:** canonical matching FIFO/reserve/STP state, coupled risk
  positions and total-leaves reservations, coupled auction-risk position and
  active-remainder reconstruction, independent replay audits, stable
  snapshot framing, semantic corruption, immutable-profile binding, WAL-prefix
  divergence, generation forks, pending-file recovery, exact retries, A/B anchor
  identity, repeated cutover in both physical layouts, generation-selection
  failure recovery, and suffix replay across segment boundaries.

## Complexity

For `P` occupied price levels and `E` maker-slice interactions, cached
best-price, best-order, and best-level-snapshot discovery is `O(1)`. Ordered
level insertion, deletion, and next-worse traversal are `O(log P)`; execution is
`O((E + 1) log P)`. The cache removes one ordered-tree traversal from each
best-maker selection, while the authoritative level aggregate update remains
`O(log P)`. A reserve order can contribute more than one interaction through
replenishment. FIFO append and removal also maintain intrusive
per-account/per-side links, so account membership changes add `O(1)` work and no
membership-node allocation to the `O(log P)` price-level operation. Reserve
refresh preserves both account links and membership. For `K` selected orders, a
mass cancel traverses exactly `K` linked members, sorts their unique identifiers
in place in `O(K log K)`, and performs `K` price-level removals in `O(K log P)`,
independent of total active-order count `O`. Total time is
`O(K(log K + log P))` with `O(K)` prepared scratch space. Block-and-cancel has
the identical bound; enable is expected `O(1)`. Active matching state uses
`O(O + P + C + T)` memory for `O` resting orders, `C` retained idempotency
reports, and `T` never-evicted controlled accounts; account indexing adds two links per active order plus fixed
head/tail/count/aggregate state per active account, within the same `O(O)`
bound. The two complete best-level caches add `O(1)` space.

For `B` canonical aggregate bid levels and `A` canonical aggregate ask levels,
call-auction discovery is `O(B + A)` time and `O(1)` auxiliary space. It merges
the monotone demand/supply transition streams, incorporates constant market
interest, and evaluates full constant-state intervals inside an aligned band
without enumerating the numeric price range or allocating.
For `O_b` supplied buy orders, `O_a` supplied sell orders, and `F_b + F_a`
positive fills, price-time allocation is `O(O_b + O_a)` time and
`O(F_b + F_a)` result space. Both fill-vector capacity requests use the exact
derived cardinalities and succeed before either side is constructed; the
allocator may grant more capacity, and no vector grows while fills are emitted.

For a call-auction collection with `I` accepted identities, `O` active orders,
and `P` occupied limit prices, admission is
`O(log I + log O + log P)` and owner-checked cancellation is
`O(log O + log P)`. Aggregate scratch construction and discovery are
`O(B + A)`. Canonical order scratch construction is `O(O log O + P)` because
intrusive FIFO identities are resolved through a stable AVL; allocation then
adds `O(O)` work and `O(F_b + F_a)` result memory. Constructor-reserved
collection state is `O(I_max + O_max + P_max)` and does not grow during bounded
mutation or analysis scratch reconstruction. For `T` buyer/seller trade pairs,
`C` remainder cancellations, and `M <= O` affected orders, uncross preparation
is `O(O log O + P + F_b + F_a + T)` time with
`O(F_b + F_a + T + C)` fallibly reserved result memory. The two-pointer pairing
bound is `T <= F_b + F_a - 1` when both fill vectors are non-empty. Commit is
`O(M(log O + log P))` and performs no heap allocation.

For `H` retained sequenced auction reports, phase controls, business rejection,
and monotonic idempotency lookup are expected `O(1)`; submit and cancel inherit
the collection-book bounds above. Uncross preparation has the preceding book
bound plus `O(T + C)` exact report-capacity derivation, and commit adds
`O(T + C)` event emission into the already reserved trace without vector
growth. The independent engine audit sorts retained command sequences and
validates the underlying book in `O(H log H + I + O + P)` time with
`O(H + O)` scratch. Engine state occupies
`O(H_max + I_max + O_max + P_max)` memory.

For `C` persisted auction commands, `E` persisted auction events, `B` WAL bytes,
and `S` physical segments, full-WAL durable auction reopen scans framing in
`O(B + S)` and replays the sum of the `C` engine command costs plus `O(E)`
report comparison. Checkpoint capture performs complete deterministic replay;
semantic event projection has a conservative `O((C + E) log O)` ordered-map
bound before the engine/book audit because complete idempotency history is
retained. After an A/B cutover, reopen scans the anchor and suffix WAL bytes,
validates and directly rebuilds the indexed engine, and executes only suffix
commands; cutover bounds WAL scan and command re-execution, not checkpoint size
or semantic validation time.

FOK preflight over `O_c` active orders in `P_c` crossed levels is
`O(O_c + P_c log P)` time and `O(1)` auxiliary space. Each inspected order is
visited at most once; complexity is independent of the number of reserve slices
that subsequent execution emits.

Default matching limits are 4,096 active orders, 4,096 active accounts, 4,096
occupied prices per side, 65,536 accepted order IDs, 65,536 controlled accounts,
65,536 retained commands, and 65,536 events per report, with the final 4,096 history slots reserved for
valid cancellation controls. The report limit must be at least
`max_active_orders + 1`, preserving one cancellation event per maximally active
order plus the mass-cancel completion event. Auction allocation independently
defaults to at most 65,536 supplied orders and therefore at most 65,536 positive
fill records per side; its exact byte footprint is target-layout dependent.
Uncross trade and remainder-cancellation capacity requests use their exact
derived cardinalities, although the allocator may grant more capacity; there is
no separate configured uncross-vector maximum beyond the bounded active-order
population. The sequenced call-auction engine defaults to 65,536 retained
reports, a final 4,098-report terminal lane (`O_max + 2` for `O_max = 4,096`),
and 8,193 events per report (`2 O_max + 1`). Its monotonic command-history hash
is reserved to the complete finite maximum and never deletes entries. A coupled
shard requests hash entry
headroom
`H = 2 O_max + A_max + I_max + T_max + C_max + R_max`
`= 2(4,096) + 4,096 + 4(65,536) = 274,432`
entries at the defaults: active matching orders plus equally bounded risk
reservations, active matching accounts, accepted IDs, retained account controls,
retained commands, and registered risk accounts. This is an entry-capacity calculation, not a byte-size
claim; allocator rounding and the standard-library bucket layout are target
dependent. Every constructor fallibly reserves
two stable-slot indexed AVL arenas, all five matching hash indexes, and the
coupled-risk profile and reservation indexes to their complete applicable bounds.
`try_with_limits` reports the exact price arena or hash resource when a requested
reservation cannot be represented or allocated. Command preparation therefore
borrows matching and coupled-risk state immutably; it fallibly reserves only the
complete event buffer and exact identifier selection buffer for mass-cancel,
block-and-cancel, or instrument transition-and-cancel before
durable command append. Capacity preflight is expected
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
buffer and optional mass-cancel/account-control/instrument-control selection buffer. Matching hash-table insertion
has constructor headroom but can rehash after sustained deletion/different-key
insertion churn. Commit validates book identity and
retained-command generation in expected `O(1)` time; foreign and stale tokens
cannot mutate state. The complete risk-reservation index is constructor-owned,
so profile registration between split preparation and commit cannot introduce
reservation-map growth. Price-level AVL nodes and account memberships allocate
nothing during commit: price slots are reserved at book construction and removed
slots are reused through an intrusive free list. The Arc control block and
unrelated codec/checkpoint allocations remain outside this fallible preparation boundary.
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
Monotonic profile insertion remains within constructor-reserved headroom;
the churning reservation hash can rehash under the A12 allocator boundary.
A complete risk cross-audit is `O(O + A)` for `A <= R_max` registered accounts,
and risk state uses `O(O + A)` memory. Profile registration is expected `O(1)`,
allocation-free within the constructor-owned bound, and disabled after the first
sequenced command.

Market-data trace and replica application are `O(log P)` for level-changing
events and `O(1)` for no-change events. Publisher bootstrap is `O(O log O + P + T)`,
a full-depth snapshot is `O(P)`, and a complete publisher cross-audit is
`O(O log O + P + T)`.

WAL scanning is `O(B + S)` for `B` persisted bytes across `S` physical segments.
A segmented reader retains `O(S)` descriptors and one bounded payload rather
than the complete WAL. Appending a frame performs `O(F)` checksum and copy work
for frame length `F`; a `JournalBatch` amortizes one write and one configured
durability barrier across multiple frames. Rotation adds one closing barrier,
exclusive file creation, and a parent-directory barrier at a size boundary.

For `C` retained matching commands, `O` active orders, and `P` price levels, a
matching checkpoint retains `O(C + O + P)` state. Capture performs one
independent full-history matching replay plus structural audit synchronously
under exclusive shard access. Uncut checkpoint open scans every WAL byte and
decodes the exact command/report prefix. Anchored open in either physical layout
instead validates the selected A/B checkpoint and scans only current anchor and
suffix bytes, then reconstructs indices in `O(O log P)` and executes matching
transitions only for the suffix. Cutover bounds physical WAL-prefix storage and
scan work per cutover; retained history and capture pause remain `O(C)`.

For `C` retained risk-managed commands, `O` active orders, `P` price levels,
and `A` accounts, a coupled risk checkpoint retains `O(C + O + P + A)` state.
Capture independently replays the complete coupled history and audits derived
total-leaves reservations and redundant exposure synchronously under exclusive
shard access. Uncut checkpoint open scans every WAL byte for exact
metadata/command/report lineage. Anchored open in either physical layout validates the
checkpoint-bound original metadata and scans only the anchor and suffix, then
executes matching/risk transitions only for the suffix. It does not bound the
`O(C)` capture pause, retained memory, or generation lifetime.

For `R` retained ledger records, `E` contained transaction entries, `L` posting
legs, and `A` non-zero account balances, checkpoint capture/validation is linear
in `R + E + L + A` apart from ordered validation-map factors, and retained
checkpoint state is `O(R + E + L + A)`. Checkpoint-assisted durable open still
scans all `B` WAL bytes for an uncut log. Anchored open in either physical layout
scans only the compacted anchor and suffix after validating the selected A/B
checkpoint. Physical WAL-prefix storage is retired, but complete checkpoint
history and its validation remain `O(R + E + L + A)`.

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
