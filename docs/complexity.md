# Complexity and resource bounds

This document states the asymptotic time, auxiliary-space, and fixed-memory
bounds of every Quotick subsystem, together with the default resource
envelopes they follow from. Variable names (`O`, `P`, `C`, `E`, ...) are
introduced inline per paragraph. Entry and slot counts are semantic; ABI byte
layout, allocator rounding, and resident-page behavior are target-dependent.

Contents:

- [Instrument catalog](#instrument-catalog)
- [Trading calendars](#trading-calendars)
- [Continuous matching](#continuous-matching)
- [Call-auction discovery and allocation](#call-auction-discovery-and-allocation)
- [Call-auction collection book](#call-auction-collection-book)
- [Sequenced auction engine](#sequenced-auction-engine)
- [Auction risk](#auction-risk)
- [Durable auction recovery](#durable-auction-recovery)
- [Immediate-quantity preflight](#immediate-quantity-preflight)
- [Default matching limits and memory](#default-matching-limits-and-memory)
- [Risk engine](#risk-engine)
- [Market data](#market-data)
- [Call-auction market data](#call-auction-market-data)
- [WAL and journal](#wal-and-journal)
- [Matching checkpoints](#matching-checkpoints)
- [Coupled risk checkpoints](#coupled-risk-checkpoints)
- [Call-auction checkpoints](#call-auction-checkpoints)
- [Ledger checkpoints](#ledger-checkpoints)
- [Ledger operations](#ledger-operations)

## Instrument catalog

For `A` assets, `I` instruments, `D` total definitions, and `V` versions of
one instrument:

- Catalog asset and instrument-range selection is expected `O(1)` and
  exact/effective version lookup is expected `O(1) + O(log V)`.
- Registering an asset or a new instrument is expected `O(1)` after
  validation.
- Appending a version to an interleaved history can shift `O(D)` definitions
  and rebase `O(I)` ranges inside the already reserved arena.

Fixed catalog state is `O(A_max + I_max + D_max)` and does not allocate after
construction. Defaults are 4,096 assets, 16,384 instruments, and 65,536
definitions. A full adversarial hash collision cluster makes index access
`O(A)` or `O(I)` without changing the finite memory bound. The
allocation-free structural audit is expected `O(A + D + I²)` and deliberately
favors exhaustive overlap detection over control-plane audit latency.

## Trading calendars

For `S` sessions in one immutable `TradingCalendar` generation, construction
validates the already ordered schedule in `O(S)` time, exactly reserves one
`S`-entry session-ID index, and sorts that index in `O(S log S)` time. The
caller-supplied session vector and the derived index occupy `O(S)` storage.
Converting those vectors to shared owners creates two control blocks under A12;
the generation never mutates or grows afterward.

- Active-session and strictly-next-session lookup are `O(log S)` by entry-open
  order.
- Lookup by `TradingSessionId` is `O(log S)` in the derived index.
- `sessions_on` performs two partition searches and is `O(log S)` before
  returning a borrowed contiguous slice.
- Day or good-for-session lifetime resolution is `O(log S)` and `O(1)`
  auxiliary space. Constructing a boundary-checked `ExpirySweep` by session ID
  is `O(log S)`.
- Cloning a calendar is `O(1)` time and space per handle and copies no session
  or index rows. Equality remains `O(S)` in the worst case.

For an encoded payload of `S` sessions, byte length is exactly
`28 B + 44 S B`. Encoding is `O(S)` time and output space. Decoding proves the
row count before exactly reserving `O(S)` rows, then reconstructs the sorted ID
index, so it is `O(S log S)` time and `O(S)` owned space. Calendar resolution
adds no new matching hot-path structure: its output uses the existing GTD
expiry arena and sweep bounds.

## Continuous matching

For `P` occupied price levels, `E` maker-slice interactions, and `L <= E`
levels exhausted by one command:

- Cached private execution-best price/order and public-best level discovery is
  `O(1)`; the two prices can differ when the execution best is hidden-only.
- The cache carries a key-checked stable AVL slot handle into maker mutation,
  so partial fills, removal from a still-occupied level, and reserve
  displayed-class-tail refresh are `O(1)` and perform no ordered price search.
- Ordered level insertion, empty-level deletion, and next-worse traversal are
  `O(log(P + 1))`; execution is `O(E + (L + 1) log(P + 1))`.
- AVL rotations and two-child deletion relink nodes without moving surviving
  key/value pairs.
- At equal price, fully displayed/reserve orders precede fully hidden orders;
  FIFO applies within each class. A reserve order can contribute more than one
  interaction through replenishment, but refresh splices it to the displayed-
  class tail in place without deleting and reinserting its level.
- FIFO append and removal also maintain intrusive per-account/per-side
  links, so account membership changes add `O(1)` work and no
  membership-node allocation to the `O(log P)` price-level operation.
  Reserve refresh preserves both account links and membership.

For `K` selected orders, a mass cancel traverses exactly `K` linked members,
sorts their unique identifiers in place in `O(K log K)`, and performs `K`
price-level removals in `O(K log P)`, independent of total active-order count
`O`. Total time is `O(K(log K + log P))` with `O(K)` prepared scratch space.
Block-and-cancel has the identical bound; enable is expected `O(1)`.

For `K` GTD orders at or before one inclusive expiry watermark, preparation
counts the ordered expiry prefix in `O(K + 1)` time and leases `O(K)` existing
selection scratch. Commit traverses that prefix again in canonical
`(deadline, OrderId)` order and removes each order from its price level and
expiry AVL. With `P` occupied prices and `X` active GTD orders, commit is
`O(K(log(P + 1) + log(X + 1)))`; its report contains exactly `K + 1` events.
An empty sweep is `O(1)`, consumes no lease, and still advances the watermark.

For `K` stops activated by one explicit reference, preparation counts the
eligible prefix and derives matching work before mutation. It leases `O(K)`
selection scratch and selects canonical buy/sell trigger heads. If those
activations cause `E` maker-slice interactions and exhaust `L` price levels,
commit is bounded by
`O(K log(O + 1) + E + (L + K) log(P + 1))`; this includes trigger-index
removal and possible stop-limit residual insertion. FOK or minimum-quantity IOC
activation can add its ordinary crossed-order inspection. Counting retained
eligible backlog is
`O(R + 1)` for `R` stops remaining at the committed reference. A sweep with no
eligible stop is `O(1)` and still records the reference; a partial sweep
requires exact-reference continuation before cursor advancement. Source-ID,
source-version, and source-sequence transition validation is `O(1)` time and
space. Each command/event reference occupies 32 B in retained history; live
book and publisher state retain one optional reference independent of `O`.

Active matching state uses `O(O + P + C + T)` memory for `O` resting or
dormant orders, `C` retained idempotency reports, and `T` never-evicted
controlled accounts; the fixed GTD index and two stop-trigger indexes each
contribute at most one AVL entry per active order, and account indexing adds
two links per active order plus fixed
head/tail/count/aggregate state per active account, within the same `O(O)`
bound. The secondary public-price indexes add `O(P)` fixed space; execution and
public best-level caches add `O(1)` space.

Successful full-book validation traverses all price FIFO, dormant stop, and
account lists in `O(O)`, audits all four initialized execution/public price
AVL arenas in `O(P log(P + 1))`, audits `X_i` initialized expiry-arena slots in
`O(X_i log(X_i + 1))`, and audits `S_i` initialized slots across the two stop
arenas in `O(S_i log(S_i + 1))`, using `O(1)` auxiliary space and no heap
allocation. Human-readable failure-detail formatting may allocate after
corruption is detected.

## Call-auction discovery and allocation

For `B` canonical aggregate bid levels and `A` canonical aggregate ask
levels, call-auction discovery is `O(B + A)` time and `O(1)` auxiliary space.
It merges the monotone demand/supply transition streams, incorporates
constant market interest, and evaluates full constant-state intervals inside
an aligned band without enumerating the numeric price range or allocating.

For `O_b` supplied buy orders, `O_a` supplied sell orders, and `F_b + F_a`
positive fills, price-time allocation is `O(O_b + O_a)` time and
`O(F_b + F_a)` result space. Both fill-vector capacity requests use the exact
derived cardinalities and succeed before either side is constructed; the
allocator may grant more capacity, and no vector grows while fills are
emitted.

## Call-auction collection book

For a call-auction collection with `I` accepted identities, `O` active
orders, and `P` occupied limit prices:

- Admission and new-identity replacement are
  `O(log I + log O + log P)`; owner-checked cancellation is
  `O(log O + log P)`. A retained-priority active-quantity reduction is
  `O(log O + log P)` and changes neither identity nor queue links. Replacement
  and amendment use `O(1)` auxiliary space; replacement accounts for the
  released target slot and singleton price level during preflight.
- A bounded account index maintains separate intrusive buy/sell lanes with
  exact counts and `u128` quantities. Account/scope mass-cancel preflight is
  expected `O(1)`. For `K` selected orders, application traverses only those
  links, sorts snapshots in `O(K log K)`, and removes them in
  `O(K(log O + log P))`; it is independent of unrelated active orders.
- Aggregate scratch construction and discovery are `O(B + A)`.
- Canonical order scratch construction is `O(O log O + P)` because intrusive
  FIFO identities are resolved through a stable AVL; allocation then adds
  `O(O)` work and `O(F_b + F_a)` result memory.
- Constructor-reserved collection state, including the account index, is
  `O(I_max + O_max + P_max)` and does not grow during bounded mutation, mass
  cancellation, or analysis scratch reconstruction.

For `T` buyer/seller trade pairs, `C` remainder cancellations, and `M <= O`
affected orders, uncross preparation is `O(O log O + P + F_b + F_a + T)` time
with `O(F_b + F_a + T + C)` fallibly reserved result memory. The two-pointer
pairing bound is `T <= F_b + F_a - 1` when both fill vectors are non-empty.
Commit is `O(M(log O + log P))` and performs no heap allocation.

A successful complete collection-book audit uses `O(1)` auxiliary space and
no heap allocation. For `R` active orders, `I` accepted identifiers, and `S`
total initialized slots across the active-order, accepted-identifier, and two
price arenas, queue and identity cross-checking is `O(R(log R + log I))`;
complete arena topology auditing is `O(S log S)`. Failure-detail construction
may allocate after corruption is detected.

## Sequenced auction engine

For `H` retained sequenced auction reports, phase controls, business
rejection, and monotonic idempotency lookup are expected `O(1)`; submit,
replace, amend, and cancel inherit the collection-book bounds above. An
accepted replacement emits exactly two events; an accepted amendment emits
exactly one event. Both add `O(1)` event-construction time and space. Uncross
preparation has the preceding book bound plus `O(T + C)` exact report-capacity
derivation, and commit adds `O(T + C)` event emission into the already reserved
trace without vector growth.

Mass-cancel preflight inherits the expected `O(1)` account lookup. Commit adds
the collection-book `O(K(log K + log O + log P))` work plus `O(K)` event
emission. It emits `K` removals and one completion; `K = 0` emits only the
completion. One engine-owned `O_max` snapshot vector is reserved at
construction and never grows.

The never-evicted report cache retains insertion order, so the independent
engine audit validates `H` commands and `E` events directly in `O(H + E)`
time with `O(1)` auxiliary space and no successful-path allocation, then
applies the underlying collection-book bound above. Checkpoint capture reuses
the same canonical order and does not sort the retained history. Engine state
occupies `O(H_max + I_max + O_max + P_max)` memory, including the mass-cancel
scratch vector.

## Auction risk

Call-auction risk reservations maintain private intrusive per-account links
in the same constructor-owned bounded hash entries as their economic
snapshots. Reservation insertion, partial-fill replacement, and removal
remain expected `O(1)` and allocation-free. A successful standalone risk
audit recomputes all `A` account aggregates across `O` reservations in
expected `O(A + O)` time and `O(1)` auxiliary space; full coupled parity adds
another expected `O(O)` pass over active book orders. Adversarial full hash
collisions can make these passes quadratic but cannot grow storage.

Replacement authorization subtracts the target reservation and checks the
replacement in expected `O(1)` time and `O(1)` auxiliary space before the
underlying book transition. Applying the two-event trace performs one expected
`O(1)` removal and one expected `O(1)` insertion.

Amendment authorization requires no new exposure gate because a valid command
strictly reduces active leaves. Applying its one-event trace decreases the
reservation quantity, notional, and account exposure by the exact delta in
expected `O(1)` time and `O(1)` auxiliary space while retaining reservation
cardinality.

An accepted mass cancel applies `K` ordinary reservation removals in expected
`O(K)` risk time. Its aggregate completion has no second risk-state effect.

## Durable auction recovery

For `C` persisted auction commands, `E` persisted auction events, `B` WAL
bytes, and `S` physical segments, full-WAL durable auction reopen scans
framing in `O(B + S)` and replays the sum of the `C` engine command costs
plus `O(E)` report comparison.

Checkpoint capture first exactly reserves `C` history, `O` active-order, and
`I` accepted-identifier rows; coupled risk capture exactly reserves `A`
account rows. Direct row copying is `O(C + O + I)` and coupled account
canonicalization adds `O(A log A)`. The three direct row images and the
coupled account image are immutable shared values, so direct and coupled
checkpoint clones are `O(1)` and allocate no row or event storage. It then
performs complete deterministic replay; semantic projection uses four
fallibly constructed dense/open-addressed hashes bounded through `C`, while
selected-limit price cardinality uses two hashes bounded through active-order
count `O`. Expected validation is `O(C + E + O)` with `O(max(C, O))` peak
auxiliary storage; a full adversarial collision cluster is bounded by
`O(C(C + E) + O²)`.

Every capture/validation reservation failure identifies the exact resource
and requested maximum. No growing standard map, set, or temporary
accepted-identity vector remains in this path. Temporary engine construction
errors retain their source; resource failure before snapshot or cutover
mutation leaves the durable shard unpoisoned.

After an A/B cutover, reopen scans the anchor and suffix WAL bytes, validates
and directly rebuilds the indexed engine, and executes only suffix commands;
cutover bounds WAL scan and command re-execution, not checkpoint size or
semantic validation time.

## Immediate-quantity preflight

FOK or minimum-quantity IOC preflight over `O_c` active orders in `P_c` crossed
levels is
`O(O_c + P_c log P)` time and `O(1)` auxiliary space. Each inspected order is
visited at most once; complexity is independent of the number of reserve
slices that subsequent execution emits. A displayed-class self barrier admits
only preceding working slices; a hidden-class self barrier admits the total
leaves of the preceding displayed class and earlier hidden leaves. FOK scans
for original quantity; minimum-quantity IOC scans for its explicit threshold.

## Default matching limits and memory

This section states the default resource envelopes, the buffer pools, the
constructor arena and hash reservations, and the command-preparation
capacity rules that follow from them.

### Default limits

Default matching limits are:

| Resource                 | Default |
| ------------------------ | ------- |
| Active orders, including dormant stops | 4,096 |
| Active accounts          | 4,096   |
| Occupied prices per side | 4,096   |
| Accepted order IDs       | 65,536  |
| Controlled accounts      | 65,536  |
| Retained commands        | 65,536  |
| Events per report        | 65,536  |

The final 4,096 history slots are reserved for valid cancellation-capable
commands.
The report limit must be at least `max_active_orders + 1`, preserving one
cancellation event per maximally active order plus the mass-cancel completion
event or one complete expiry sweep. Stop-trigger sweeps remain ordinary-lane
commands and must pass their derived activation/matching report bound.

### Order-selection buffer pool

Continuous matching also defaults to two simultaneously leased non-empty
order-selection buffers.

- Each buffer requests capacity for 4,096 `OrderId` values at construction.
- The measured minimum element payload is
  `2 × 4,096 × 8 B = 65,536 B = 0.065536 MB`, before vector headers, the
  pool vector, Arc/mutex, allocator rounding, and resident pages.
- A zero-cardinality control, expiry, or stop-trigger selection requires no
  lease.
- Holding two non-empty preparations exhausts the pool and produces an
  unsequenced typed preparation failure; dropping or consuming a preparation
  returns its cleared buffer.

### Auction allocation defaults

Auction allocation independently defaults to at most 65,536 supplied orders
and therefore at most 65,536 positive fill records per side; the generic
caller-owned plan API retains those fallible output allocations.

### Collection-book uncross buffer pool

A collection book instead defaults to two simultaneously leased uncross
buffer sets.

- Each set requests capacity for 4,096 elements for each fill side, trade
  pairs, and remainder cancellations at construction.
- Holding a preparation or committed result pins one set; exhaustion is a
  typed unsequenced preparation failure, and `Drop` returns the storage.
- On the current `aarch64-apple-darwin` build, the minimum requested element
  payload is
  `2 × 4,096 × (2 × 24 B + 56 B + 56 B) = 1,310,720 B = 1.310720 MB`
  before vector headers, the pool vector, Arc/mutex, allocator rounding, and
  resident pages.

### Sequenced call-auction engine defaults

The sequenced call-auction engine defaults to:

- 65,536 retained reports,
- a final 4,098-report terminal lane (`O_max + 2` for `O_max = 4,096`),
- 73,730 retained events,
- a final 8,194-event terminal lane (`2 O_max + 2`), and
- 8,193 events per report (`2 O_max + 1`).

Its monotonic command-history hash and append-only event arena are reserved
to their complete finite maxima and never delete entries. The engine also
requests one 4,096-element `CallAuctionOrderSnapshot` mass-cancel scratch
vector at construction; its ABI byte size and allocator/page overhead are
target-dependent.

### Coupled shard hash headroom

A coupled shard requests hash entry headroom
`H = 2 O_max + A_max + I_max + T_max + C_max + R_max`
`= 2(4,096) + 4,096 + 4(65,536) = 274,432`
entries at the defaults:

- active matching orders plus equally bounded risk reservations,
- active matching accounts,
- accepted IDs,
- retained account controls,
- retained commands, and
- registered risk accounts.

The corresponding fixed lookup layouts initialize 548,864 bucket slots
because each listed power-of-two maximum receives twice as many buckets.
These are entry/slot counts, not byte-size claims; ABI layout, allocator
rounding, and resident-page behavior remain target-dependent.

### Retained-event arena sizes

On the current `aarch64-apple-darwin` build:

- `size_of::<OnceLock<Event>>() = 144 B`, so the default event slots occupy
  `262,144 × 144 B = 37,748,736 B = 37.748736 MB` before vector, Arc, and
  allocator overhead.
- The corresponding call-auction layout is
  `size_of::<OnceLock<CallAuctionEvent>>() = 176 B`, or
  `73,730 × 176 B = 12,976,480 B = 12.976480 MB` before the same overheads.

### Constructor reservations

Every constructor fallibly reserves seven stable-slot indexed AVL arenas
(four execution/public price indexes, one GTD-expiry index, and two
stop-trigger indexes), one 262,144-slot default continuous retained-event
arena, one 73,730-slot default
call-auction retained-event arena, all five fixed-capacity matching hash
indexes, and the coupled-risk profile and reservation indexes to their
complete applicable bounds, plus every configured continuous order-selection
and call-auction uncross lease. `try_with_limits` reports the exact
AVL/event/selection arena or hash resource when a requested reservation
cannot be represented or allocated.

### Command preparation and capacity preflight

Command preparation therefore borrows matching and coupled-risk state
immutably; it proves the report bound against existing event headroom and
acquires one constructor-owned selection lease for a non-empty mass-cancel,
expiry sweep, stop-trigger sweep, block-and-cancel, or instrument transition-
and-cancel before durable command append. Empty selections bypass the pool;
non-empty pool exhaustion is typed before sequencing or append.

- Capacity preflight is expected `O(1)` on the normal path, except for
  validating a reserve-lane control through the ordinary core lookup.
- If an active-order, active-account, or same-side price-level bound is
  already full, a GTC/GTD/post-only limit order performs an allocation-free
  residual preview in `O(O_c + P_c log P)` time and `O(1)` auxiliary space
  for `O_c` orders in `P_c` crossed levels. A proved no-residual order
  bypasses the resting-capacity gate.
- If a new account arrives at a full account bound, exact release proof can
  additionally inspect all `O` active account memberships in expected `O(O)`
  time and `O(1)` auxiliary space.
- Active-order, active-account, and same-side price-level decisions use the
  exact final cardinalities.
- Dormant stop intake consumes active-order/account capacity but no price-level
  slot. Trigger preparation removes each dormant identity before activation;
  a stop-limit residual that cannot fit its side's fixed price arena is fully
  cancelled with a typed event instead of partially executing.
- A price-changing replacement whose old level remains occupied uses the
  same allocation-free liquidity proof only at a full same-side level bound:
  a full fill or aggressor-terminating STP result creates no target level,
  while a resting residual is rejected when no level slot is released.
- Capacity errors are not sequenced and durable wrappers reject them before
  WAL append. IOC, FOK, and market orders do not consume a resting-capacity
  gate.

One `PreparedCommand` carries the completed operational/core proof through
risk authorization and WAL append, together with the safe report bound and
optional mass-cancel/expiry/stop-trigger/account-control/instrument-control
selection lease.
Matching hash-table insertion uses constructor-owned dense entries plus a
fixed open-addressed bucket array; backward-shift deletion and dense
`swap_remove` reuse that storage without growth. Commit validates book
identity and retained-command generation in expected `O(1)` time; foreign and
stale tokens cannot mutate state. The complete risk-reservation index is
constructor-owned, so profile registration between split preparation and
commit cannot introduce reservation-map growth. Price-level AVL nodes and
account memberships allocate nothing during commit: price slots are reserved
at book construction and removed slots are reused through an intrusive free
list. The constructor-time arena Arc control block and checkpoint operations
remain outside this command-preparation boundary; continuous matching/risk
capture vectors are fallible under A88, while codec output growth and decoded
collection reservation are typed separately.

### Report construction and event accounting

For `E` report events, construction and encoding are `O(E)`. Safe
`OnceLock<Event>` slots are allocated once at book construction; builder
finalization publishes the exact adjacent arena range in `O(1)` without
allocation or event copy. Cache, retry, and checkpoint trace clones are
`O(1)` time and space per handle and do not allocate or copy events. Report
encoding emits the unchanged ordered event sequence.

Incoming matching preparation computes a safe event/trade bound in `O(1)` from
mutation-maintained side/account work aggregates; expiry preparation uses the
`O(K + 1)` prefix count stated above, while stop-trigger preparation visits the
bounded eligible prefix and sums each activation's conservative matching work.
All check the per-report limit and total arena headroom. Preparation consumes no
slot; only actual commit events advance the cursor. The protected event tail has
`O_max + 1`
slots, so ordinary capacity is `E_max - (O_max + 1)`. The conservative bound
includes uncrossed opposite-side work and may reject early near a boundary,
but retains no per-command unused capacity. A sweep over `K <= O_max` active
GTD orders has the exact bound `K + 1` and therefore fits the same protected
tail. Stop-trigger sweeps cannot use that tail and may require more than
`K + 1` events because every activation can match or refresh reserve slices.

## Risk engine

Risk authorization and trace application are expected `O(1)` per order event.
Monotonic profile insertion and churning reservation replacement remain
within fixed constructor-owned dense and bucket storage. Private intrusive
per-account reservation links make a successful risk-only cross-audit
expected `O(A + O)` with `O(1)` auxiliary space and no heap allocation for
`A <= R_max` registered accounts and `O` reservations. Complete coupled
validation adds the allocation-free continuous-book `O(O + P log P)` audit
and an expected `O(O)` dense-book/risk parity pass. A full adversarial
hash-collision cluster can make the risk work quadratic but cannot grow its
bounded storage.

Risk state uses `O(A + O)` memory. Profile registration is expected `O(1)`,
allocation-free within the constructor-owned bound, and disabled after the
first sequenced command. Dormant stops retain one reservation valued from the
activation limit or market collar. Arming and triggering are expected `O(1)`
risk-map transitions; triggering changes the dormant flag without duplicating
account exposure.

## Market data

Market-data trace and replica application are `O(log P)` for level-changing
events. Fully hidden lifecycle events are private expected `O(1)` hash
transitions and project to `NoBookChange`; a fully hidden-maker trade advances
identifiers without public tree mutation when its price is absent. Ordinary
no-change events are `O(1)`; stop arm, removal, replacement,
and trigger validation are `O(log(O + 1))` in the private stop arenas. For `E`
updates and `U <= E` affected prices, publication reserves `O(E)` output before
mutation and validates unique affected prices in expected
`O(E + U log P)` time using fixed hash scratch. Replica batch capacity
preflight is expected `O(E)` before `O(E log P)` application.

- Publisher bootstrap is expected `O(O + P log P + S log(O + 1) + T)` for `S`
  dormant stops.
- A full-depth snapshot output is `O(P)`.
- Allocation-free double-buffered snapshot application is `O(P log P)`.
- A complete publisher cross-audit is expected
  `O(O + P + S log(O + 1) + T)` outside adversarial hash collision clusters.

State is `O(O_max + T_max + P_max + E_max)` for one publisher, including the
private dormant-stop map and two trigger arenas, and
`O(P_max + E_max)` for one replica, with the replica reserving four per-side
depth arenas in total (active and standby for bids and asks).

For replay capacity `N`, one `MarketDataReplayBuffer` initializes `N` optional
typed slots in `O(N)` time and retains `O(N)` state. An `E`-update admission
preflights identity, contiguity, overlap, and collision in `O(E)` time, then
writes only its new suffix with `O(1)` work per update and no allocation. Exact
retained duplicates also cost `O(E)`. Exclusive-cursor range setup is `O(1)`;
iterating `R` returned updates is `O(R)` time with `O(1)` borrowed iterator
state, including physical wrap. Typed in-memory slot bytes and allocator/page
rounding are target-dependent; version-3 encoded updates remain 33 B, 43 B,
66 B, or 91 B by payload kind.

## Call-auction market data

Call-auction public projection has the same constructor-owned storage
boundary while retaining valid crossed/locked collection depth. For `E`
updates and `U` unique affected limit identities:

- Publisher output reservation is `O(E)`, incremental state work is expected
  `O(E + U log P)`, and source audit without transient order/depth
  collections is expected `O(O + P)` outside adversarial hash clusters;
  structural AVL diagnostics are allocation-free `O(P log P)`.
- Replica capacity simulation is expected `O(E + U)` before `O(E log P)`
  mutation.
- An accepted replacement fixes `E = 2`; projection and replica application
  therefore retain the ordinary bounds while advancing the book revision once.
- An accepted retained-priority amendment fixes `E = 1`; it subtracts one
  anonymous aggregate quantity delta, preserves level order count, and advances
  the book revision once.
- A mass cancel with `K` selected orders fixes `E = K + 1`. Publisher and
  replica work is expected `O(E + U log P)` for `U` affected limit identities;
  the complete batch advances book revision once exactly when `K > 0`.
- Snapshot output is `O(P)`; double-buffered snapshot application is
  allocation-free after construction and `O(P log P)`.

Publisher fixed state is `O(O_max + P_max)` and replica fixed state is
`O(P_max + E_max)`, including four active/standby side arenas in total.
Default maxima are 4,096 active orders, 4,096 limit prices per side, and
8,193 updates per command.

For call-auction replay capacity `N`, one
`CallAuctionMarketDataReplayBuffer` initializes `N` typed slots in `O(N)`
time and retains `O(N)` state. Each slot includes one update and its original
batch-start/batch-end flags. Admission of an `E`-update batch validates its
identity, sequence, overlap, content, and boundary flags in `O(E)` time, then
writes its new suffix with `O(1)` work per update and no allocation.

A successful `replay_batches_after` page selects `R` updates in `O(R)` time
and returns `B` complete zero-copy batches. Iterating the outer batches and all
inner updates is another `O(B + R) = O(R)` time with `O(1)` iterator state.
The page never splits a batch. Diagnosing an evicted partial oldest batch can
scan up to `N` slots to report the earliest later complete boundary. Applying
each replay batch has the ordinary replica `O(E + U)` capacity-preflight and
`O(E log P)` mutation bounds. Typed slot bytes, allocator rounding, and page
residency are target-dependent; version-4 payload bytes are unchanged by the
process-local replay ring.

## WAL and journal

WAL scanning is `O(B + S)` for `B` persisted bytes across `S` physical
segments. A segmented reader retains `O(S)` descriptors and one bounded
payload rather than the complete WAL.

- Appending a frame performs `O(F)` checksum and copy work for frame length
  `F`.
- A `JournalBatch` reserves `O(F + R)` output for `R` frames, assembles them
  directly in `O(F)`, and amortizes one write and one configured durability
  barrier without `R` intermediate frame buffers.
- Rotation reserves its inventory slot before one closing barrier, exclusive
  file creation, and a parent-directory barrier at a size boundary.

## Matching checkpoints

For `C` retained matching commands containing `E` events, `O` active resting
or dormant orders, `P` initialized price slots, and `S` initialized stop-index
slots, a matching checkpoint retains `O(C + E + O + P + S)` state.
`OrderBook::capture_checkpoint_candidate` performs canonical row copying plus
structural and command-derived lineage audits in expected
`O(C + E + O + P log(P + 1) + S log(S + 1))` under exclusive book access,
without re-executing matching history. Its immutable, nonencodable result can be
moved to another thread; consuming `verify` performs the independent
full-history replay and a fresh canonical projection before returning the
stable `OrderBookCheckpoint`. `OrderBook::checkpoint` invokes both phases
synchronously.

`DurableOrderBook::capture_checkpoint_candidate` first synchronizes the exact
WAL prefix, then permits replay verification off-thread;
`write_verified_checkpoint` accepts only the same shard incarnation and
unchanged physical-cutover epoch. Ordinary suffix growth is valid, while
reopen or cutover invalidates the publication fence. The verified value also
retains the exact physical end cursor at its barrier;
`compact_verified_checkpoint` synchronizes the current head and streams only
the later suffix behind `anchor(G)`.

Uncut checkpoint open scans every WAL byte and decodes the exact
command/report prefix. Anchored open in either physical layout instead
validates the selected A/B checkpoint and scans only current anchor and
suffix bytes, then reconstructs execution-price/public-price/stop indices in
`O(O(log(P + 1) + log(O + 1)))` and executes matching
transitions only for the suffix. Cutover bounds physical WAL-prefix storage
and scan work per cutover; retained history and writer-side capture
audit/copy remain history-dependent. Physical migration is
`O(B_suffix + S_suffix)` in retained suffix bytes and segments, with one
bounded frame buffer; semantic replay remains off-thread.

Capture exactly reserves `C` command/report rows plus canonical resting and
dormant rows whose combined count is `O` before their first push. Arena-backed
event traces share
their existing storage without copying events. Append-only audited dense
report order is already chronological, so capture copies it in `O(C)` without
sorting. The immutable history, resting-order, and dormant-stop images are
retained behind shared owners; cloning an `OrderBookCheckpoint` is `O(1)` and
allocates no row or event storage. Cloning the staged capture has the same
property. Initial capture/decoding still creates three shared-owner control
blocks after the exactly reserved vectors exist.

Temporary replay-book construction and every capture/validation allocation
preserve typed resource identity. Checkpoint validation owns bounded
dense/open-addressed scratch with peak semantic maxima `4C + O` for history
validation and `C + 3O` for selected-limit cardinality validation. Expected
validation work is `O(C + E + O)`; a full adversarial collision cluster is
bounded `O(C^2 + E + O^2)`. Resource failure before snapshot or cutover
mutation leaves durable matching unpoisoned; semantic contradiction poisons
it. Snapshot encoding ownership remains a separate typed codec/framing
boundary.

## Coupled risk checkpoints

For `C` retained risk-managed commands containing `E` events, `O` active
resting or dormant orders, `P` price levels, and `A` accounts, a coupled risk checkpoint retains
`O(C + E + O + P + A)` state. Capture performs matching structural/lineage
audit, exactly reserves and sorts `A` account rows, reconstructs positions
and total-leaves reservations directly, and requires exact live equality
without re-executing command history. The immutable nonencodable candidate
may then be verified off-thread by complete coupled replay while the source
accepts an append-only suffix. Its account and embedded matching images are
shared; candidate and completed-checkpoint clones are `O(1)`.

Durable capture first synchronizes the represented WAL prefix and binds
verified publication to the same open shard, profile-metadata boundary, and
pre-cutover epoch. Reopen or cutover rejects the handle. Verified cutover
uses its private cursor to migrate only subsequent frames; synchronous
checkpoint publication composes the same stages.

Uncut checkpoint open scans every WAL byte for exact metadata/command/report
lineage. Anchored open in either physical layout validates the
checkpoint-bound original metadata and scans only the anchor and suffix, then
executes matching/risk transitions only for the suffix. It does not bound the
history-dependent structural/direct capture pause, worker replay memory,
retained history, or generation lifetime.

## Call-auction checkpoints

For `C` retained call-auction commands containing `E` events, `O` active
orders, `I` accepted identities, and `A` coupled accounts, plain capture
audits the live engine/book/event arena, exactly captures the three canonical
row images, and projects phase/cycle, revision, order, identity, priority,
trade, and event lineage without executing commands. Coupled capture
additionally sorts `A` account rows in `O(A log A)`, reconstructs
positions/reservations/exposures directly, and proves exact live equality.
Complete plain or coupled replay occurs once in a consuming off-thread
verifier; prior nested replay in the coupled path is eliminated.

Durable capture synchronizes the represented WAL prefix and accepts
publication only through the same open shard and unchanged cutover epoch;
ordinary suffix growth is valid, including cursor-streamed prefix retirement.
Candidate clones are `O(1)` and share accepted/order/history/account images.
Writer projection and coupled direct reconstruction, worker memory, complete
history retention, and unbounded post-capture suffix copy remain
history-dependent.

## Ledger checkpoints

For `R` retained ledger records, `E` contained transaction entries, `L`
posting legs, and `A` non-zero account balances, checkpoint
capture/validation is linear in record/event replay apart from per-batch
`O(L_b log L_b)` flat-term sorting and canonical balance/trial sorting
bounded by `O(A log A)`; no standard map/set is used by ledger production
code. The audit's exactly reserved `R` record vector becomes the checkpoint
record vector without a second materialization; balances exactly reserve `A`
rows before canonical sorting.

`JournalEntry` posting vectors and `LedgerBatch` entry vectors are immutable
shared values. Record materialization, capture, and borrowed restoration
clone only `Arc` handles and allocate no nested vectors. The live journal
retains the shared batch itself rather than a second transaction-ID vector.
The checkpoint's top-level balance and record images are shared as well, so a
complete `LedgerCheckpoint` clone is `O(1)` and allocates no row or nested
transaction storage.

Capture resource or replay-constructor failure is typed and leaves durable
state unpoisoned before snapshot/cutover mutation; semantic contradiction
poisons it. Retained checkpoint state is `O(R + E + L + A)`.
Checkpoint-assisted durable open still scans all `B` WAL bytes for an uncut
log. Anchored open in either physical layout scans only the compacted anchor
and suffix after validating the selected A/B checkpoint. Physical WAL-prefix
storage is retired, but complete checkpoint history and its validation remain
`O(R + E + L + A)`.

## Ledger operations

Reversal validation is `O(L)` for the target entry's posting legs plus
expected `O(1)` transaction/reversal-index access. Correction balance
preparation is `O(Lᵣ + Lₚ)` time and auxiliary state for the distinct posting
keys in its reversal and replacement.

For a batch with `N` entries, `L` posting legs, and `U` affected
`(account, asset)` keys, construction proves unique transaction IDs in
expected `O(N)` time and `O(N)` memory using an exact bounded
dense/open-addressed set. Preparation uses two exact `N`-bounded hash
overlays plus one fallibly reserved flat signed-term array, sorts the latter
in `O(L log L)` time, and uses `O(N + L + U)` auxiliary memory for lifecycle
state, exact signed terms, and final balance updates; it is independent of
unaffected ledger balances. A full adversarial hash collision cluster can
make overlay work `O(N²)` without growing storage. Commit is expected
`O(N + U)` time, clones `N` small immutable entry handles without allocation,
and mutates only affected fixed-capacity indexes plus one pre-reserved record
slot. Initial entry/batch construction creates one shared-owner control block
after validation; that stable-Rust allocator boundary remains A12.

For `A` internal non-zero balances, `V` asset denominations, and `W` spilled
`u64` magnitude limbs, fallible trial-balance construction reserves one flat
`A`-term arena, sorts it in `O(A log A)`, and emits an exactly reserved
`V`-asset vector using `O(A + V + W)` memory. Canonically sorted
reconciliation input is audited in one streaming pass without a map.
Magnitude addition is allocation-free through `u128::MAX`, amortized constant
time after spill, and `O(W_v)` in a worst-case carry chain for one asset's
`W_v` limbs. For `S` external statement balances and `D` reported breaks,
reconciliation is `O(A log A + S)` time and `O(A + D)` auxiliary memory;
output is canonical and contains no zero differences.

Period close/reopen validation and the effective-date fence are `O(1)` time
and state; their immutable journal history remains included in `E` above.
