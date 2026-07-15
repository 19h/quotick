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
- Append-only instrument versions selected by effective time through bounded
  asset/code/instrument indexes and one constructor-reserved definition arena.
- Explicit catalog limits, typed construction/capacity failures, fixed
  allocation telemetry, and structural cross-audit of every index and history.
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
  plus key-checked constant-time maker-level mutation, with independent
  indexed-AVL cross-audit.
- Finitely bounded stable-slot AVL price arenas reserved before a book exists,
  with allocation-free `O(log(P + 1))` ordered structural mutation, physical
  node relinking that preserves surviving handles, intrusive vacant-slot reuse,
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
  accepted identifiers, retained account controls, reports, events per report,
  total retained events, and simultaneously retained non-empty order selections.
  Mandatory constructor-time reservation establishes complete semantic headroom.
  Stable price/depth arenas, order-selection buffers, and fixed-capacity dense/open-addressed
  matching, risk, continuous-market-data, and auction-history indexes do not
  grow or rehash after construction, including under deletion/different-identity
  churn.
- A protected retained-history tail sized to at least the maximum active-order
  population. Ordinary admission closes first; only currently valid cancel and
  mass-cancel, block-and-cancel, and entry-closing instrument transition-and-cancel
  controls may consume the reserve; reopening controls remain ordinary admission,
  and exact retries bypass all capacity gates.
- An independent protected retained-event tail of `max_active_orders + 1`
  slots. Ordinary commands cannot cross its watermark; only the same currently
  business-valid cancellation-capable commands can enter it. Exact retries
  consume neither command nor event history.
- Residual-aware capacity admission for GTC/post-only orders. A boundary scan
  proves whether fills or self-trade prevention leave anything to rest, without
  mutating the book or materializing reserve replenishment slices, and accounts
  exactly for maker-order and complete-account capacity released before append.
- Opaque generation-bound command preparation shared by direct matching, risk,
  and durable orchestration. Capacity, identifier, FOK, and core business checks
  execute once against an immutable book borrow; the token carries a proved
  report bound and, for a non-empty mass-cancel-or-control selection, one
  isolated lease from the constructor-owned selection pool, while all
  event/hash headroom already belongs to the constructed
  shard. Pool exhaustion is typed and durable paths reject it before appending
  the prepared command.
- Immutable live `EventTrace` ranges share one constructor-owned append-only
  event arena in `O(1)` across the returned report, idempotency cache, exact
  retries, and in-memory checkpoints. Traces provide indexed and iterator access;
  decoded/caller-built traces retain an owned-vector fallback. Diagnostic trace
  mutation is explicit copy-on-write and cannot alter cached history.
- Mutation-maintained per-level, per-side, and per-account future event-work
  aggregates. Preparation derives safe command-specific event/trade bounds in
  constant time and checks identifier plus constructor-owned event-arena space.
  Preparation consumes no event slots; commit publishes only actual events and
  cannot allocate or grow event storage during mutation.
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
  leases one isolated constructor-owned set of fill, deterministic buyer/seller
  pair, and selected-remainder buffers; typed pool exhaustion precedes sequencing
  and mutation. Allocation-free commit validates the preparation, applies every
  fill and remainder atomically, assigns contiguous book-local trade identifiers,
  and advances one revision. Remainder treatment is explicit
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
- Immutable `MarketDataLimits` for publisher order/control mirrors, occupied
  prices per side, and updates per command. Publisher depth uses stable-slot
  AVL arenas; order, control, and affected-level indexes use fixed dense/open-
  addressed storage derived from or explicitly proven to cover the matching
  shard before publication begins.
- Replica depth uses constructor-owned active and standby AVL arenas. Snapshot
  admission validates both side cardinalities before populating the standby
  image and swaps buffers atomically; no snapshot tree allocation occurs after
  construction. Per-batch level-cardinality simulation uses fixed scratch,
  allowing a deleted full-bound slot to be reused later in the same batch while
  rejecting genuine overflow before sequence or depth mutation.
- Publication output, full-depth image output, and replica depth output expose
  fallible reservation paths. Every accepted publication owns complete update
  capacity before applying its first private event; fixed state and validation
  scratch neither grow nor rehash under price/order identity churn.
- Call-auction publication has an independent immutable
  `CallAuctionMarketDataLimits` envelope derived from or proven to cover the
  sequenced auction engine. Publisher limit depth uses stable-slot AVL arenas;
  active-order and uncross-source mirrors use bounded dense hashes.
- Call-auction replicas likewise own active/standby depth arenas and fixed batch
  scratch. Oversized batches, new prices at a full bound, and oversized
  snapshots fail before mutation or poisoning; accepted snapshots populate the
  standby image and swap both crossed-capable sides atomically.

### Durability and codecs

- Stable little-endian codecs for definitions, risk profiles, commands,
  execution reports, market data, ledger entries, atomic corrections,
  generalized ledger batches, and matching and ledger checkpoints.
- Every decoded collection proves its wire count against the remaining payload
  before exactly reserving output storage. Reservation failure is typed with the
  logical field and validated element maximum; no decoder uses infallible
  `Vec::with_capacity` on wire-derived cardinality.
- Every encoder scalar and byte-slice write passes through one amortized
  fallible growth gate. Address-space overflow reports the current and added
  lengths; allocation failure reports the minimum required byte length. The
  first failure suppresses later writes and partial encoded bytes never escape.
- Versioned CRC-32C WAL frames with bounded payloads and contiguous sequences.
- Single-frame bytes, batch bytes/receipts, read payloads, segmented receipts,
  and rotation inventory slots reserve fallibly with exact typed resource and
  cardinality. Batch append writes stack-built headers and payloads directly
  into one exact buffer, eliminating intermediate per-frame allocation/copy.
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
- Continuous matching/risk checkpoint capture and semantic/selected-limit
  validation use exactly reserved history, active-order, account, control, and
  price resources. Failure identifies the precise resource and cardinality;
  temporary replay-shard construction preserves returned typed matching causes;
  constructor `Arc` control blocks retain the A12 allocator boundary.
  Direct continuous matching/coupled-risk and WAL-synchronized durable capture
  may return immutable nonencodable candidates whose consuming verifier can
  replay on another thread. Durable publication accepts only its verified
  typestate, requires the same open-shard origin token and pre-cutover epoch,
  binds coupled profile metadata, and permits ordinary suffix growth. The
  synchronous APIs compose the same phases.
  Durable capture resource failure performs no snapshot/cutover mutation and
  leaves the synchronized shard unpoisoned, while semantic contradiction poisons it.
- Call-auction/risk and ledger checkpoint capture apply the same typed boundary:
  history, active-order, accepted-identifier, risk-account, ledger-record,
  balance, and ledger trial-audit vectors reserve exactly before use. Direct
  auction restoration borrows immutable checkpoints; coupled restoration does
  not clone embedded auction vectors. Returned constructor causes remain typed,
  and only semantic capture contradictions poison durable shards. Plain and
  coupled call-auction capture now return opaque nonencodable candidates;
  deterministic replay may run off-thread after a durable WAL barrier, with
  standalone publication fenced by shard incarnation, profile metadata where
  applicable, and the pre-cutover epoch.

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
- Validated finite `LedgerLimitsSpec` envelopes independently bound non-zero
  balance keys, retained transactions, reversal lineages, records, per-entry
  postings, per-record transactions, and total retained postings. Three dense
  fixed-capacity hash indexes and the complete journal vector are reserved
  before state or WAL creation. Exact retries precede capacity gates; new
  over-capacity events fail without balance, sequence, index, or WAL mutation.
- Zero balances are removed from authoritative state. Atomic entries may
  therefore release old balance identities and consume the same fixed slots for
  new identities using exact final cardinality. Prepared single entries,
  corrections, and batches own every temporary/update buffer before commit;
  commit performs no heap allocation.
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
  only the suffix. Explicit-limit restore and all durable open variants reject
  an undersized selected envelope during deterministic replay.

The default ledger generation permits 65,536 non-zero balance keys, 65,536
transactions, 32,768 reversal links, 65,536 records, 256 postings per
transaction, 1,024 transactions per record, and 262,144 retained postings. Its
three authoritative hashes reserve 163,840 dense entries and initialize
327,680 lookup buckets. These are semantic entry/slot counts; ABI byte layout,
allocator rounding, and resident pages are target-dependent. Production code
can select another validated envelope through `Ledger::try_with_limits` and
the explicit-limit durable open functions.

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

For `A` assets, `I` instruments, `D` total definitions, and `V` versions of one
instrument, catalog asset and instrument-range selection is expected `O(1)` and
exact/effective version lookup is expected `O(1) + O(log V)`. Registering an
asset or a new instrument is expected `O(1)` after validation. Appending a
version to an interleaved history can shift `O(D)` definitions and rebase
`O(I)` ranges inside the already reserved arena. Fixed catalog state is
`O(A_max + I_max + D_max)` and does not allocate after construction. Defaults
are 4,096 assets, 16,384 instruments, and 65,536 definitions. A full adversarial
hash collision cluster makes index access `O(A)` or `O(I)` without changing the
finite memory bound. The allocation-free structural audit is expected
`O(A + D + I²)` and deliberately favors exhaustive overlap detection over
control-plane audit latency.

For `P` occupied price levels, `E` maker-slice interactions, and `L <= E`
levels exhausted by one command, cached best-price, best-order, and
best-level-snapshot discovery is `O(1)`. The cache carries a key-checked stable
AVL slot handle into maker mutation, so partial fills, removal from a still-
occupied level, and reserve FIFO-tail refresh are `O(1)` and perform no ordered
price search. Ordered level insertion, empty-level deletion, and next-worse
traversal are `O(log(P + 1))`; execution is
`O(E + (L + 1) log(P + 1))`. AVL rotations and two-child deletion relink nodes
without moving surviving key/value pairs. A reserve order can contribute more
than one interaction through replenishment, but refresh splices it to the FIFO
tail in place without deleting and reinserting its level. FIFO append and removal also maintain intrusive
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
Successful full-book validation traverses all price FIFO and account lists in
`O(O)` and audits both initialized AVL arenas in `O(P log(P + 1))`, using `O(1)`
auxiliary space and no heap allocation. Human-readable failure-detail formatting
may allocate after corruption is detected.

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

A successful complete collection-book audit uses `O(1)` auxiliary space and no
heap allocation. For `R` active orders, `I` accepted identifiers, and `S` total
initialized slots across the active-order, accepted-identifier, and two price
arenas, queue and identity cross-checking is
`O(R(log R + log I))`; complete arena topology auditing is `O(S log S)`.
Failure-detail construction may allocate after corruption is detected.

For `H` retained sequenced auction reports, phase controls, business rejection,
and monotonic idempotency lookup are expected `O(1)`; submit and cancel inherit
the collection-book bounds above. Uncross preparation has the preceding book
bound plus `O(T + C)` exact report-capacity derivation, and commit adds
`O(T + C)` event emission into the already reserved trace without vector
growth. The never-evicted report cache retains insertion order, so the
independent engine audit validates `H` commands and `E` events directly in
`O(H + E)` time with `O(1)` auxiliary space and no successful-path allocation,
then applies the underlying collection-book bound above. Checkpoint capture
reuses the same canonical order and does not sort the retained history. Engine
state occupies `O(H_max + I_max + O_max + P_max)` memory.

Call-auction risk reservations maintain private intrusive per-account links in
the same constructor-owned bounded hash entries as their economic snapshots.
Reservation insertion, partial-fill replacement, and removal remain expected
`O(1)` and allocation-free. A successful standalone risk audit recomputes all
`A` account aggregates across `O` reservations in expected `O(A + O)` time and
`O(1)` auxiliary space; full coupled parity adds another expected `O(O)` pass
over active book orders. Adversarial full hash collisions can make these passes
quadratic but cannot grow storage.

For `C` persisted auction commands, `E` persisted auction events, `B` WAL bytes,
and `S` physical segments, full-WAL durable auction reopen scans framing in
`O(B + S)` and replays the sum of the `C` engine command costs plus `O(E)`
report comparison. Checkpoint capture first exactly reserves `C` history, `O`
active-order, and `I` accepted-identifier rows; coupled risk capture exactly
reserves `A` account rows. Direct row copying is `O(C + O + I)` and coupled
account canonicalization adds `O(A log A)`. The three direct row images and the
coupled account image are immutable shared values, so direct and coupled
checkpoint clones are `O(1)` and allocate no row or event storage. It then
performs complete deterministic replay;
semantic projection uses four fallibly constructed dense/open-addressed hashes
bounded through `C`, while selected-limit price cardinality uses two hashes
bounded through active-order count `O`. Expected validation is `O(C + E + O)`
with `O(max(C, O))` peak auxiliary storage; a full adversarial collision cluster
is bounded by `O(C(C + E) + O²)`. Every capture/validation reservation failure identifies
the exact resource and requested maximum. No growing standard map, set, or
temporary accepted-identity vector remains in this path. Temporary engine
construction errors retain their source; resource failure before snapshot or
cutover mutation leaves the durable shard unpoisoned. After an A/B cutover,
reopen scans the anchor and suffix WAL bytes, validates and directly rebuilds
the indexed engine, and executes only suffix commands; cutover bounds WAL scan
and command re-execution, not checkpoint size or semantic validation time.

FOK preflight over `O_c` active orders in `P_c` crossed levels is
`O(O_c + P_c log P)` time and `O(1)` auxiliary space. Each inspected order is
visited at most once; complexity is independent of the number of reserve slices
that subsequent execution emits.

Default matching limits are 4,096 active orders, 4,096 active accounts, 4,096
occupied prices per side, 65,536 accepted order IDs, 65,536 controlled accounts,
65,536 retained commands, and 65,536 events per report, with the final 4,096 history slots reserved for
valid cancellation controls. The report limit must be at least
`max_active_orders + 1`, preserving one cancellation event per maximally active
order plus the mass-cancel completion event. Continuous matching also defaults
to two simultaneously leased non-empty order-selection buffers. Each buffer
requests capacity for 4,096 `OrderId` values at construction; the measured
minimum element payload is `2 × 4,096 × 8 B = 65,536 B = 0.065536 MB`, before
vector headers, the pool vector, Arc/mutex, allocator rounding, and resident
pages. A zero-cardinality selection requires no lease. Holding two non-empty
preparations exhausts the pool and produces an unsequenced typed preparation
failure; dropping or consuming a preparation returns its cleared buffer.
Auction allocation independently
defaults to at most 65,536 supplied orders and therefore at most 65,536 positive
fill records per side; the generic caller-owned plan API retains those fallible
output allocations. A collection book instead defaults to two simultaneously
leased uncross buffer sets. Each set requests capacity for 4,096 elements for
each fill side, trade pairs, and remainder cancellations at construction.
Holding a preparation
or committed result pins one set; exhaustion is a typed unsequenced preparation
failure, and `Drop` returns the storage. On the current
`aarch64-apple-darwin` build, the minimum requested element payload is
`2 × 4,096 × (2 × 24 B + 56 B + 56 B) = 1,310,720 B = 1.310720 MB` before
vector headers, the pool vector, Arc/mutex, allocator rounding, and resident
pages. The sequenced call-auction engine defaults to 65,536 retained
reports, a final 4,098-report terminal lane (`O_max + 2` for `O_max = 4,096`),
73,730 retained events, a final 8,194-event terminal lane (`2 O_max + 2`),
and 8,193 events per report (`2 O_max + 1`). Its monotonic command-history hash
and append-only event arena are reserved to their complete finite maxima and
never delete entries. A coupled
shard requests hash entry
headroom
`H = 2 O_max + A_max + I_max + T_max + C_max + R_max`
`= 2(4,096) + 4,096 + 4(65,536) = 274,432`
entries at the defaults: active matching orders plus equally bounded risk
reservations, active matching accounts, accepted IDs, retained account controls,
retained commands, and registered risk accounts. The corresponding fixed lookup
layouts initialize 548,864 bucket slots because each listed power-of-two maximum
receives twice as many buckets. These are entry/slot counts, not byte-size claims;
ABI layout, allocator rounding, and resident-page behavior remain target-dependent.
On the current `aarch64-apple-darwin` build,
`size_of::<OnceLock<Event>>() = 144 B`, so the default event slots occupy
`262,144 × 144 B = 37,748,736 B = 37.748736 MB` before vector, Arc, and allocator
overhead. The corresponding call-auction layout is
`size_of::<OnceLock<CallAuctionEvent>>() = 176 B`, or
`73,730 × 176 B = 12,976,480 B = 12.976480 MB` before the same overheads.
Every constructor fallibly reserves
two stable-slot indexed AVL arenas, one 262,144-slot default continuous retained-event arena,
one 73,730-slot default call-auction retained-event arena, all five fixed-capacity matching hash indexes, and the
coupled-risk profile and reservation indexes to their complete applicable bounds,
plus every configured continuous order-selection and call-auction uncross lease.
`try_with_limits` reports the exact price/event/selection arena or hash resource when a requested
reservation cannot be represented or allocated. Command preparation therefore
borrows matching and coupled-risk state immutably; it proves the report bound
against existing event headroom and acquires one constructor-owned selection
lease for a non-empty mass-cancel, block-and-cancel, or instrument
transition-and-cancel before durable command append. Empty selections bypass the
pool; non-empty pool exhaustion is typed before sequencing or append. Capacity preflight is expected
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
authorization and WAL append, together with the safe report bound and optional
mass-cancel/account-control/instrument-control selection lease. Matching hash-table insertion
uses constructor-owned dense entries plus a fixed open-addressed bucket array;
backward-shift deletion and dense `swap_remove` reuse that storage without growth.
Commit validates book identity and
retained-command generation in expected `O(1)` time; foreign and stale tokens
cannot mutate state. The complete risk-reservation index is constructor-owned,
so profile registration between split preparation and commit cannot introduce
reservation-map growth. Price-level AVL nodes and account memberships allocate
nothing during commit: price slots are reserved at book construction and removed
slots are reused through an intrusive free list. The constructor-time arena Arc control block and
checkpoint operations remain outside this command-preparation boundary;
continuous matching/risk capture vectors are fallible under A88, while codec
output growth and decoded collection reservation are typed
separately.
For `E` report events, construction and encoding are `O(E)`. Safe `OnceLock<Event>`
slots are allocated once at book construction; builder finalization publishes
the exact adjacent arena range in `O(1)` without allocation or event copy. Cache,
retry, and checkpoint trace clones are `O(1)` time and space per handle and do
not allocate or copy events. Report encoding emits the unchanged ordered event
sequence. Preparation computes a safe event/trade bound in `O(1)` from
mutation-maintained side/account work aggregates and checks both the per-report
limit and total arena headroom. It consumes no slot; only actual commit events
advance the cursor. The protected event tail has `O_max + 1` slots, so ordinary
capacity is `E_max - (O_max + 1)`. The conservative bound includes uncrossed
opposite-side work and may reject early near a boundary, but retains no
per-command unused capacity.

Risk authorization and trace application are expected `O(1)` per order event.
Monotonic profile insertion and churning reservation replacement remain within
fixed constructor-owned dense and bucket storage. Private intrusive
per-account reservation links make a successful risk-only cross-audit expected
`O(A + O)` with `O(1)` auxiliary space and no heap allocation for `A <= R_max`
registered accounts and `O` reservations. Complete coupled validation adds the
allocation-free continuous-book `O(O + P log P)` audit and an expected `O(O)`
dense-book/risk parity pass. A full adversarial hash-collision cluster can make
the risk work quadratic but cannot grow its bounded storage. Risk state uses
`O(A + O)` memory. Profile registration is expected `O(1)`, allocation-free
within the constructor-owned bound, and disabled after the first sequenced
command.

Market-data trace and replica application are `O(log P)` for level-changing
events and `O(1)` for no-change events. For `E` updates and `U <= E` affected
prices, publication reserves `O(E)` output before mutation and validates unique
affected prices in expected `O(E + U log P)` time using fixed hash scratch.
Replica batch capacity preflight is expected `O(E)` before `O(E log P)`
application. Publisher bootstrap is expected `O(O + P log P + T)`, a full-depth
snapshot output is `O(P)`, allocation-free double-buffered snapshot application
is `O(P log P)`, and a complete publisher cross-audit is expected
`O(O + P + T)` outside adversarial hash collision clusters. State is
`O(O_max + T_max + P_max + E_max)` for one publisher and
`O(P_max + E_max)` for one replica, with the replica reserving four per-side
depth arenas in total (active and standby for bids and asks).

Call-auction public projection has the same constructor-owned storage boundary
while retaining valid crossed/locked collection depth. For `E` updates and `U`
unique affected limit identities, publisher output reservation is `O(E)`,
incremental state work is expected `O(E + U log P)`, and source audit without
transient order/depth collections is expected `O(O + P)` outside adversarial
hash clusters; structural AVL diagnostics are allocation-free `O(P log P)`. Replica capacity
simulation is expected `O(E + U)` before `O(E log P)` mutation. Snapshot output
is `O(P)`; double-buffered snapshot application is allocation-free after
construction and `O(P log P)`. Publisher fixed state is
`O(O_max + P_max)` and replica fixed state is `O(P_max + E_max)`, including four
active/standby side arenas in total. Default maxima are 4,096 active orders,
4,096 limit prices per side, and 8,193 updates per command.

WAL scanning is `O(B + S)` for `B` persisted bytes across `S` physical segments.
A segmented reader retains `O(S)` descriptors and one bounded payload rather
than the complete WAL. Appending a frame performs `O(F)` checksum and copy work
for frame length `F`; a `JournalBatch` reserves `O(F + R)` output for `R` frames,
assembles them directly in `O(F)`, and amortizes one write and one configured
durability barrier without `R` intermediate frame buffers. Rotation reserves
its inventory slot before one closing barrier, exclusive file creation, and a
parent-directory barrier at a size boundary.

For `C` retained matching commands containing `E` events, `O` active orders,
and `P` initialized price slots, a matching checkpoint retains
`O(C + E + O + P)` state. `OrderBook::capture_checkpoint_candidate` performs
canonical row copying plus structural and command-derived lineage audits in
expected `O(C + E + O + P log(P + 1))` under exclusive book access, without
re-executing matching history. Its immutable, nonencodable result can be moved
to another thread; consuming `verify` performs the independent full-history
replay and a fresh canonical projection before returning the stable
`OrderBookCheckpoint`. `OrderBook::checkpoint` invokes both phases
synchronously. `DurableOrderBook::capture_checkpoint_candidate` first
synchronizes the exact WAL prefix, then permits replay verification off-thread;
`write_verified_checkpoint` accepts only the same shard incarnation and
unchanged physical-cutover epoch. Ordinary suffix growth is valid, while
reopen or cutover invalidates the publication fence. Uncut checkpoint open scans every WAL byte and
decodes the exact command/report prefix. Anchored open in either physical layout
instead validates the selected A/B checkpoint and scans only current anchor and
suffix bytes, then reconstructs indices in `O(O log P)` and executes matching
transitions only for the suffix. Cutover bounds physical WAL-prefix storage and
scan work per cutover; retained history and writer-side capture audit/copy
remain history-dependent. All physical checkpoint cutovers still verify the
current head synchronously.
Capture exactly reserves `C` command/report rows and `O` canonical active-order
rows before their first push. Arena-backed event traces share their existing
storage without copying events. Append-only audited dense report order is
already chronological, so capture copies it in `O(C)` without sorting.
The immutable history and active-order images are retained behind shared
owners; cloning an `OrderBookCheckpoint` is `O(1)` and allocates no row or event
storage. Cloning the staged capture has the same property. Initial
capture/decoding still creates two shared-owner control blocks after the exactly
reserved vectors exist.
Temporary replay-book construction and every
capture/validation allocation preserve typed resource identity.
Checkpoint validation owns bounded dense/open-addressed scratch with peak
semantic maxima `3C + O` for history validation and `C + 3O` for selected-limit
cardinality validation. Expected validation work is `O(C + E + O)`; a full adversarial
collision cluster is bounded `O(C^2 + E + O^2)`. Resource failure before
snapshot or cutover mutation leaves durable matching unpoisoned; semantic
contradiction poisons it. Snapshot encoding ownership remains a separate typed
codec/framing boundary.

For `C` retained risk-managed commands containing `E` events, `O` active orders,
`P` price levels, and `A` accounts, a coupled risk checkpoint retains
`O(C + E + O + P + A)` state. Capture performs matching structural/lineage
audit, exactly reserves and sorts `A` account rows, reconstructs positions and
total-leaves reservations directly, and requires exact live equality without
re-executing command history. The immutable nonencodable candidate may then be
verified off-thread by complete coupled replay while the source accepts an
append-only suffix. Its account and embedded matching images are shared;
candidate and completed-checkpoint clones are `O(1)`. Durable capture first
synchronizes the represented WAL prefix and binds verified publication to the
same open shard, profile-metadata boundary, and pre-cutover epoch. Reopen or
cutover rejects the handle; synchronous checkpoint publication composes the
same stages.
Uncut checkpoint open scans every WAL byte for exact
metadata/command/report lineage. Anchored open in either physical layout validates the
checkpoint-bound original metadata and scans only the anchor and suffix, then
executes matching/risk transitions only for the suffix. It does not bound the
history-dependent structural/direct capture pause, worker replay memory,
retained history, or generation lifetime.

For `C` retained call-auction commands containing `E` events, `O` active
orders, `I` accepted identities, and `A` coupled accounts, plain capture audits
the live engine/book/event arena, exactly captures the three canonical row
images, and projects phase/cycle, revision, order, identity, priority, trade,
and event lineage without executing commands. Coupled capture additionally
sorts `A` account rows in `O(A log A)`, reconstructs positions/reservations/
exposures directly, and proves exact live equality. Complete plain or coupled
replay occurs once in a consuming off-thread verifier; prior nested replay in
the coupled path is eliminated. Durable capture synchronizes the represented
WAL prefix and accepts standalone publication only through the same open shard
and unchanged cutover epoch; ordinary suffix growth is valid. Candidate clones
are `O(1)` and share accepted/order/history/account images. Writer projection
and coupled direct reconstruction, worker memory, complete history retention,
and synchronous physical cutover remain history-dependent.

For `R` retained ledger records, `E` contained transaction entries, `L` posting
legs, and `A` non-zero account balances, checkpoint capture/validation is linear
in record/event replay apart from per-batch `O(L_b log L_b)` flat-term sorting
and canonical balance/trial sorting bounded by `O(A log A)`; no standard
map/set is used by ledger production code. The audit's exactly reserved `R`
record vector becomes the checkpoint record vector without a second
materialization; balances exactly reserve `A` rows before canonical sorting.
`JournalEntry` posting vectors and `LedgerBatch` entry vectors are immutable
shared values. Record materialization, capture, and borrowed restoration clone
only `Arc` handles and allocate no nested vectors. The live journal retains the
shared batch itself rather than a second transaction-ID vector.
The checkpoint's top-level balance and record images are shared as well, so a
complete `LedgerCheckpoint` clone is `O(1)` and allocates no row or nested
transaction storage.
Capture resource or replay-constructor failure is typed and leaves durable
state unpoisoned before snapshot/cutover mutation; semantic contradiction
poisons it. Retained checkpoint state is
`O(R + E + L + A)`. Checkpoint-assisted durable open still
scans all `B` WAL bytes for an uncut log. Anchored open in either physical layout
scans only the compacted anchor and suffix after validating the selected A/B
checkpoint. Physical WAL-prefix storage is retired, but complete checkpoint
history and its validation remain `O(R + E + L + A)`.

Reversal validation is `O(L)` for the target entry's posting legs plus expected
`O(1)` transaction/reversal-index access. Correction balance preparation is
`O(Lᵣ + Lₚ)` time and auxiliary state for the distinct posting keys in its
reversal and replacement. For a batch with `N` entries, `L` posting legs, and
`U` affected `(account, asset)` keys, construction proves unique transaction
IDs in expected `O(N)` time and `O(N)` memory using an exact bounded
dense/open-addressed set. Preparation uses two exact `N`-bounded hash overlays
plus one fallibly reserved flat signed-term array, sorts the latter in
`O(L log L)` time, and uses `O(N + L + U)` auxiliary memory for lifecycle
state, exact signed terms, and final balance updates;
it is independent of unaffected ledger balances. A full adversarial hash
collision cluster can make overlay work `O(N²)` without growing storage. Commit is
expected `O(N + U)` time, clones `N` small immutable entry handles without
allocation, and mutates only affected fixed-capacity indexes plus one
pre-reserved record slot. Initial entry/batch construction creates one shared-owner
control block after validation; that stable-Rust allocator boundary remains A12. For `A`
internal non-zero balances, `V` asset denominations, and `W` spilled `u64`
magnitude limbs, fallible trial-balance construction reserves one flat `A`-term
arena, sorts it in `O(A log A)`, and emits an exactly reserved `V`-asset vector
using `O(A + V + W)` memory. Canonically sorted reconciliation input is audited
in one streaming pass without a map. Magnitude addition is allocation-free through
`u128::MAX`, amortized constant time after spill, and `O(W_v)` in a worst-case
carry chain for one asset's `W_v` limbs. For `S` external statement balances
and `D` reported breaks, reconciliation is
`O(A log A + S)` time and `O(A + D)` auxiliary memory; output is canonical and
contains no zero differences.
Period close/reopen validation and the effective-date fence are `O(1)` time and
state; their immutable journal history remains included in `E` above.
