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
- [Borrowed ledger history](#borrowed-ledger-history)
- [Account-and-asset ledger statements](#account-and-asset-ledger-statements)
- [Point-in-time ledger balances](#point-in-time-ledger-balances)

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
- Market-to-limit capture reads the private execution-best cache in `O(1)`
  time and freezes an ordinary one-price limit. Its matching and residual
  insertion therefore have the same bound as that effective limit order.
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

The conditional mass-cancel variant moves that exact `K`-member traversal and
sort into preparation, then performs `K` expected constant-time active-order
lookups and fixed selected-row validations while constructing exactly reserved
caller output. Accepted commit validates and drains the same prepared IDs
without another account-list traversal or sort. Its asymptotic matching bound
remains `O(K(log K + log P))`; the constructor-owned lease retains `O(K)` ID
scratch and the caller-owned observation adds `O(K)` rows. Decline and unwind
omit all price-level removals.

Conditional block-and-cancel reuses that same prepared selection and caller
output, while additionally binding the current revisioned account fence,
requested action, and resulting blocked state. Accepted commit drains the same
IDs without another account-list traversal or sort. Conditional enable performs
no account-list traversal, acquires no selection lease, and allocates no
selected-order output; its preparation and commit retain the ordinary expected
`O(1)` fence work.

Conditional transition-and-cancel moves the ordinary all-order scan and sort
into preparation, constructs complete caller-owned selected-state output, and
commits the same prepared IDs without another all-order scan or sort. For `O`
active orders it adds `O(O log O)` preparation time, `O(O)` leased ID scratch,
and `O(O)` caller output to the ordinary removal bound. Conditional transition
has one fixed-size state observation, no selection lease, and no selected-order
output.

For `K` GTD orders at or before one inclusive expiry watermark, preparation
counts the ordered expiry prefix in `O(K + 1)` time and leases `O(K)` existing
selection scratch. Commit traverses that prefix again in canonical
`(deadline, OrderId)` order and removes each order from its price level and
expiry AVL. With `P` occupied prices and `X` active GTD orders, commit is
`O(K(log(P + 1) + log(X + 1)))`; its report contains exactly `K + 1` events.
An empty sweep is `O(1)`, consumes no lease, and still advances the watermark.

Conditional expiry instead fills the ordinary lease during preparation and
constructs complete caller-owned selected-state output. Each selected ID is
validated by exact active-order and expiry-index lookup, adding
`O(K log(X + 1))` preparation work after the ordinary prefix count. Accepted
commit validates and drains those same IDs without a second ordered-prefix
traversal or sort. The lease and caller output each retain `O(K)` rows; a valid
empty prefix uses neither and still invokes its predicate.

For `K` stops activated by one explicit reference, preparation counts the
eligible prefix and derives matching work before mutation. It leases `O(K)`
selection scratch and selects canonical buy/sell trigger heads. If those
activations cause `E` maker-slice interactions and exhaust `L` price levels,
commit is bounded by
`O(K log(O + 1) + E + (L + K) log(P + 1))`; this includes trigger-index
removal and possible stop-limit residual insertion. FOK or minimum-quantity IOC
activation can add its ordinary crossed-order inspection; FOK decrement-and-
cancel uses the same nonmutating self-barrier scan as direct entry, while
minimum-quantity decrement-and-cancel uses its exact two-counter reserve-round
scan. Counting retained eligible backlog is
`O(R + 1)` for `R` stops remaining at the committed reference. A sweep with no
eligible stop is `O(1)` and still records the reference; a partial sweep
requires exact-reference continuation before cursor advancement. Source-ID,
source-version, and source-sequence transition validation is `O(1)` time and
space. Each command/event reference occupies 32 B in retained history; live
book and publisher state retain one optional reference independent of `O`.

Conditional stop-trigger sweeping fills the ordinary prepared selection during
preparation and constructs complete caller-owned dormant-state output. For `S`
dormant stops in the selected side index, exact trigger-index validation adds
`O(K log(S + 1))` work after ordinary preparation. Accepted commit validates
and drains those same IDs without a second eligible-prefix selection or sort.
The lease and caller output each retain `O(K)` rows; a valid empty prefix uses
neither and still invokes its predicate.

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

Read-only order-book output has an explicit caller-owned allocation boundary.
For `P` occupied execution prices, `V <= P` visible prices, requested limit
`L`, and `K_L <= P` occupied prices inspected to select
`S = min(V, L)` visible rows, `try_depth` performs one validation/count pass and
one copy pass in `O(log(P + 1) + K_L)` time overall, requests exactly `S`
slots, and owns `O(S)` output. Constant factors include the two traversals.
`try_depth_iter` has `O(log(P + 1))` gated setup, streams the same market-
priority public projection in `O(P)` complete-traversal time, and uses `O(1)`
iterator state without caller-owned output. Each selected row adds `O(1)`
aggregate validation. For `T` active identities including `S_o` dormant stops
and `R = T - S_o` resting orders,
`try_active_orders` costs `O(T + R log R)` time and `O(R)` output space. For
one account selection of `K` orders, `try_account_active_order_ids` costs
expected
`O(1) + O(K log K)` time and `O(K)` output space, independent of unrelated
orders. All three reserve before copying, perform no authoritative mutation,
and drop any private partial construction on an invariant failure.

For an inclusive price band, let `K` be the occupied execution-price levels
inspected inside the band and `V_b <= K` the selected visible rows after
applying the requested limit. `try_depth_range_iter` costs
`O(log(P + 1) + K)` total time and
`O(1)` auxiliary space; hidden-only in-band prices contribute to `K` but not
`V_b`. Each selected candidate adds `O(1)` validation. `try_depth_range`
performs one allocation-free validation/count pass and one validated copy pass,
so the constant doubles without changing the asymptotic bound. It requests
exactly `V_b` slots and owns `O(V_b)` caller output. An inverted band is empty
after the `O(1)` coherent-extrema gate. Full and band iterators each retain two
128-index stacks:
`2 × 128 × size_of::<usize>() = 2,048 B` on a 64-bit target, plus scalar
fields. The bound is independent of configured or occupied level count.

`try_depth_range_summary` performs the `O(1)` coherent-extrema gate and one
checked fold over the typed directional price-range descent. For `K` occupied
execution prices in the selected band,
including hidden-only prices, it is `O(log(P + 1) + K)` time and `O(1)` fixed
result/state, and allocates no output. Only visible rows contribute to the
level, displayed-order, and displayed-quantity totals. The full-side
`try_depth_summary` uses the same fold over the definition's price domain and
therefore has `K = P`. Each included row performs constant checked
`usize`/`u128` additions; overflow or a zero public aggregate/count discards
the partial local result. Empty and inverted bands retain the same bounded
setup and zero totals.

`try_public_depth_imbalance` first performs the authoritative `O(1)` coherent-
extrema check, then initializes both directional depth traversals. Let `K_b`
and `K_a` be the occupied execution prices traversed to select at most `N`
visible bid and ask levels independently. The query costs
`O(log(P + 1) + K_b + K_a)` time and `O(1)` fixed output/state. Hidden-only
prices can contribute to `K_b` or `K_a` but not to the output. Both sides reuse
the `DepthSummary` checked `usize`/`u128` accumulator; one additional checked
`u128` addition forms combined displayed quantity, and one comparison plus one
subtraction forms the exact signed-magnitude imbalance numerator. Per-side or
combined overflow, or an invalid selected candidate, discards the complete
local result. The successful path allocates no output and performs no mutation.

`try_best_bid_offer` reads two cached public extrema and performs a constant
number of aggregate, ordering, provenance, and arithmetic operations. It is
`O(1)` time and space with one fixed-size result and no successful-path
allocation. For raw bid `b` and offer `a`, exact spread `a - b` fits `u64`
across the complete signed `i64` price domain; exact midpoint numerator
`a + b` fits `i128` and retains denominator two. Empty and one-sided books
perform the same bounded work. Human-readable invariant detail may allocate
only after a zero aggregate/count or locked/crossed pair is detected.
`try_best_bid` and `try_best_ask` select one optional side from this same value
without another traversal, retaining `O(1)` time and space.

`try_trading_state_observation` applies that same `O(1)` coherent-extrema
check, validates `revision <= book_event_sequence`, and returns one fixed-size
instrument/version/sequence/state/revision value. `try_trading_state` selects
its snapshot without another traversal. Successful source queries allocate no
output, use `O(1)` space, and perform no mutation; failure-detail formatting
may allocate only after corruption is detected.

For `T` retained account controls and `A` active-account index entries,
`try_account_control_observation` performs one expected `O(1)` control lookup
and at most one expected `O(1)` active-account lookup. It returns one fixed-
size account/instrument/version/sequence/state/revision value without
successful-path allocation or mutation. A full adversarial collision cluster
can increase one query to `O(T + A)` without changing finite storage.
Publisher bootstrap and complete source cross-audit prevalidate all retained
controls in expected `O(T)` time; adversarial active-account collisions can
increase the blocked-control component to `O(T A)`.

`try_public_level` performs the authoritative `O(1)` coherent-extrema check,
one `O(log(P + 1))` execution-level lookup, and one
`O(log(P + 1))` redundant public-membership lookup at the exact key. Constant
factors combine into `O(log(P + 1))` time and `O(1)` fixed output/state.
Present and absent results have the same bound. A target candidate adds one
constant key/aggregate validation. Fully hidden-only levels are returned as
absence. The successful path allocates no output and performs no mutation;
human-readable invariant detail can allocate after target-key corruption.

For a displayed-liquidity request, let `K` be the occupied opposite-side
execution prices inspected through filled, price-limit, or public-book-
exhausted termination among `P` occupied prices. Hidden-only prices can be
inspected but do not enter the quote. `try_displayed_liquidity_quote` costs
`O(log(P + 1) + K)` time and `O(1)` fixed output/state, allocates no output,
and performs no mutation. Its coherent-extrema gate is `O(1)` before traversal.
Each contributing public price adds one constant-time
checked quantity/notional update; at most one level is partial. Quoted quantity
is bounded by the requested `u64`, so the exact signed `i128` notional covers
the full `i64` price domain. The shared accumulator is also used by the private
immediate-execution quote. Human-readable invariant detail can allocate only
after corrupt public aggregates are detected.

For `C` retained commands, `retained_command_report` performs one expected
`O(1)` bounded-hash lookup and returns one borrowed command/report view.
`retained_history` has `O(1)` setup and exact-size iterator state; consuming
all rows is `O(C)` time. Neither path allocates output, clones a command or
report, copies an event trace, or mutates the book. An adversarial full hash
collision cluster can make exact lookup `O(C)` without changing storage.

For one selected `OrderId` among `O` active identities,
`try_active_order_observation` performs one expected `O(1)` bounded-hash lookup
and constant local row validation. Present resting, present dormant-stop, and
absent results have `O(1)` fixed output/state, allocate nothing on success, and
do not mutate the book. `try_order` and `try_dormant_stop` have the same bound.
A full adversarial active-order hash collision cluster increases lookup to
`O(O)` without changing storage. Human-readable invariant detail may allocate
only after selected-row corruption is detected.

For one resting target with `K` predecessor orders at its price,
`try_order_queue_position` performs one expected `O(1)` active-order lookup,
one `O(log(P + 1))` price-level lookup, and `K` expected `O(1)` predecessor
resolutions. Total expected work is `O(log(P + 1) + K)` with `O(1)` auxiliary
space and one fixed-size, allocation-free result. A displayed target sums
predecessor working slices; a hidden target sums predecessor total leaves. A
full adversarial active-order hash collision cluster can increase traversal to
`O(K O)` for `O` active orders without growing storage. Human-readable
invariant detail may allocate only after relevant path corruption is detected.

For one selected private price level containing `K` orders among `P` occupied
prices and `O` active orders, `try_price_level_orders` first performs one
`O(log(P + 1))` level lookup and an expected `O(K)` allocation-free validation
pass. Complete forward, reverse, or mixed traversal then performs another `K`
expected `O(1)` active-order lookups. Total expected construction-plus-
consumption time is `O(log(P + 1) + K)` with `O(1)` iterator state and no
caller-owned output. A full adversarial active-order hash collision cluster can
increase each pass to `O(K O)` without storage growth. An absent level is
`O(log(P + 1))` and empty. Human-readable invariant detail may allocate only
after validation detects corruption and before an iterator is returned.

## Call-auction discovery and allocation

For `B` canonical aggregate bid levels and `A` canonical aggregate ask
levels, call-auction discovery is `O(B + A)` time and `O(1)` auxiliary space.
It merges the monotone demand/supply transition streams, incorporates
constant market interest, and evaluates full constant-state intervals inside
an aligned band without enumerating the numeric price range or allocating.

For `O_b` supplied buy orders, `O_a` supplied sell orders, and `F_b + F_a`
positive fills, both price-time and price/class-tier pro-rata-time allocation
are `O(O_b + O_a)` time and `O(F_b + F_a)` result space. Pro-rata allocation
uses at most four fixed 64-step exact multiply/divides per marginal order
across cardinality validation and fill construction; no product wider than
`u128` is constructed. Both fill-vector capacity requests use the exact derived
cardinalities and succeed before either side is constructed; the allocator may
grant more capacity, and no vector grows while fills are emitted.
Priority comparison is market/limit category, economic price,
`AuctionPriorityClass(u16)`, priority sequence, then `OrderId`. The scalar adds
constant storage per order and no asymptotic work.

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
- Read-only account-order extraction performs the same expected `O(1)` lookup,
  fallibly reserves `K` IDs, validates only those owner/side links and
  aggregates, and sorts them in `O(K log K)`. Its `O(K)` caller-owned output and
  runtime are independent of unrelated active orders; failures return no
  partial vector and mutate no collection state.
- `try_order_observation` performs one active-order AVL lookup. A present row
  adds accepted-identity, immediate-neighbor, and at most one price-level AVL
  lookup plus one expected `O(1)` active-account lookup. Its expected time is
  `O(log(O + 1) + log(P + 1))`; indexed absence is `O(log(O + 1))`. A fully
  colliding active-account hash can add `O(A)` time for `A` active accounts.
  The result and auxiliary state are `O(1)`, and success allocates nothing.
- `try_observation` validates scalar revision/cardinality, both market
  aggregates, and both best AVL extrema in `O(log(P + 1))` time and `O(1)`
  space. The fixed-size value and successful path allocate nothing. Direct and
  best aggregate limit-level lookup compose that gate and remain
  `O(log(P + 1))`. `try_limit_depth_iter` has `O(log(P + 1))` gated setup and
  streams all `P` levels in market priority using `O(1)` auxiliary space; each
  selected row adds `O(1)` price/aggregate/endpoint-shape validation. For
  requested limit `L` and `S = min(P, L)`, `try_limit_depth` validates and
  copies through two `O(log(P + 1) + S)` prefix passes, exactly reserves `S`
  rows, and owns `O(S)` output. Market-constrained interest is excluded from
  depth and retained in the fixed observation.
- For an inclusive band and requested output limit, let `K` be the returned
  occupied limit prices. `try_limit_depth_range_iter` costs
  `O(log(P + 1) + K)` total time and `O(1)` auxiliary space after the shared
  gate. `try_limit_depth_range` validates and copies in two such passes,
  exactly reserves `K` output rows, and owns `O(K)` caller output. A streamed
  valid prefix may precede a deeper typed failure; materializers expose no
  partial vector. An inverted band is empty, and market-constrained interest
  remains separate.
- Aggregate scratch construction and discovery are `O(B + A)`.
- Canonical order scratch construction is `O(O log O + P)`: intrusive arrival-
  FIFO identities are resolved through a stable AVL, then both caller-owned
  side slices are allocation-free unstable-sorted by the shared
  market/price/class/time/ID comparator. Allocation then adds `O(O)` work and
  `O(F_b + F_a)` result memory.
- Constructor-reserved collection state, including the account index, is
  `O(I_max + O_max + P_max)` and does not grow during bounded mutation, mass
  cancellation, or analysis scratch reconstruction.

For `T` buyer/seller trade pairs, `C` remainder cancellations, and `M <= O`
affected orders, uncross preparation is `O(O log O + P + F_b + F_a + T)` time
with `O(F_b + F_a + T + C)` fallibly reserved result memory. The two-pointer
pairing bound is `T <= F_b + F_a - 1` when both fill vectors are non-empty.
`Abort` self-trade policy adds one constant-time account comparison per
candidate pair. A conflict can occur after a strict prefix has been written to
the leased trade buffer; returning the preparation clears no capacity, drops
the lease, and leaves authoritative state unchanged. No alternative-pair
search or additional storage is performed.
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
An aborted self-trade uncross performs ordinary discovery/allocation and scans
only through the first conflicting canonical pair, then emits one fixed-size
business-rejection event. It performs no commit work or terminal-lane
admission.

Conditional phase control performs ordinary `O(1)` transition preparation
once. It binds fixed-size prior/resulting cycle snapshots, book revision,
active/accepted/limit-level/market-interest aggregates, and prior indication.
For predicate cost `F`, exact generation and transition validation adds `O(1)`
before the predicate and at accepted commit, so decline, unwind, and acceptance
are `O(1 + F)`. The prepared state, owned observation, and generic evaluator
add `O(1)` auxiliary space and allocate no output. Coupled authorization and
one-event risk-neutral application add `O(1)`. Durable acceptance and business
rejection append two existing frames; decline, unwind, and replay append zero.

Conditional call-auction submission first performs ordinary admission
preparation. Before the predicate it repeats exact admission validation and an
absent-identity observation. For `I` accepted identities, `O` active orders,
`P` occupied limit prices, and predicate cost `F`, decline or unwind is
`O(log(I + 1) + log(O + 1) + log(P + 1) + F)`. Acceptance repeats ordinary
validation and commit without changing that asymptotic bound. The fixed-size
owned observation and generic evaluator add `O(1)` auxiliary state and allocate
no output. Coupled authorization and accepted reservation insertion add
expected `O(1)` each. Durable acceptance and business rejection append two
existing frames; decline, unwind, and replay append zero.

For conditional uncross, let `A` be ordinary uncross preparation, `F_b + F_a`
the positive fill count, `T` trade pairs, `C` remainder cancellations, `O`
canonical source orders, `F` predicate cost, and `M` ordinary commit cost.
Before the predicate, exact observation validation adds
`V = O(O + (F_b + F_a + T + C) log(O + 1))`: source/cancellation topology is
scanned linearly and every referenced active identity is resolved through the
stable AVL. Acceptance costs `O(A + V + F + M)`; decline or unwind costs
`O(A + V + F)`. Ordinary `M` retains its own commit-time validation, so the
accepted conditional path deliberately pays that second fail-closed validation
before mutation. The observation itself is fixed-size, borrows the existing
A86 plan/trade/cancellation buffers, adds `O(1)` auxiliary state, and performs
no allocation. Coupled-risk acceptance retains expected `O(T + C)` trace
application. Durable acceptance and business rejection append two existing
frames; decline, unwind, and replay append zero.

Conditional owner cancellation first performs ordinary owner-cancel preflight,
then one fail-closed selected-order observation before the predicate. Decline
or unwind is `O(log(O + 1) + log(P + 1) + F)` for predicate cost `F` and adds
one fixed-size owned result. Acceptance repeats selected-state validation at
commit and performs the ordinary AVL cancellation; constant factors increase
without changing the `O(log(O + 1) + log(P + 1) + F)` asymptotic bound. The
generic evaluator and observation use `O(1)` auxiliary space and allocate
nothing on success. Coupled acceptance adds expected `O(1)` reservation
release. Durable acceptance and business rejection append two existing frames;
decline, unwind, and replay append zero.

Conditional call-auction mass cancellation first performs expected `O(1)`
aggregate preparation. For `K` selected orders and predicate cost `F`, exact
pre-predicate owner-lane traversal, in-place canonical sorting, and selected-row
validation add `O(K log K + F)` time. Decline or unwind has that bound and
returns the constructor-owned snapshot scratch to length zero. Acceptance
repeats ordinary selection/sorting and removes the unchanged generation in
`O(K(log K + log(O + 1) + log(P + 1)))` time for `O` active orders and `P`
occupied limit prices. The fixed-size observation borrows the existing
`O(O_max)` scratch and adds `O(1)` state; no successful path allocates. A valid
empty selection invokes the predicate and keeps source/resulting revision
equal. Coupled acceptance adds expected `O(K)` reservation release. Durable
acceptance and business rejection append two existing frames; decline, unwind,
and replay append zero.

One sequenced indicative publication reconstructs the canonical bid and ask
aggregates and applies the shared discovery kernel in `O(B + A)` time with
`O(1)` auxiliary space. It emits exactly one fixed-size event whether clearing
is present or absent. Retaining or invalidating the latest revision-bound
state is `O(1)` time and space; an exact retry and a rejection preserve it.

Conditional indicative publication performs that ordinary `O(B + A)`
preparation once. For predicate cost `F`, fixed-size generation and coordinate
validation adds `O(1)` before the predicate and at accepted commit, so decline,
unwind, and acceptance are `O(B + A + F)`. Preparation reuses the existing
constructor-owned `O(P_max)` aggregate scratch without growth; the owned
observation and evaluator add `O(1)` space and allocate no output. Coupled
authorization and one-event risk-neutral application add `O(1)`. Durable
acceptance and business rejection append two existing frames; decline, unwind,
and replay append zero.

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

Exact retained-report lookup performs one expected `O(1)` bounded-hash access
and returns one borrowed command/report view. Complete retained-history
iteration has `O(1)` setup, `O(H)` traversal time, and `O(1)` iterator state.
It allocates no output, clones no command or report, copies no event trace, and
does not mutate auction state. An adversarial full collision cluster can make
exact lookup `O(H)` without changing the finite cache bound.

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
Indicative publication requires no account lookup or reservation mutation and
adds `O(1)` risk authorization and trace-application work.
An aborted self-trade rejection likewise adds `O(1)` risk work and changes no
reservation, exposure, position, or netting scratch.

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

FOK and minimum-quantity IOC under permit, cancel-aggressor, cancel-resting, or
cancel-both preflight over `O_c` active orders in `P_c` crossed levels in
`O(O_c + P_c log(P + 1))` time and `O(1)` auxiliary space. Each inspected
order is visited at most once; complexity is independent of the number of
reserve slices that subsequent execution emits. A displayed-class self barrier
admits only preceding working slices; a hidden-class self barrier admits the
total leaves of the preceding displayed class and earlier hidden leaves. FOK
scans for original external-trade quantity and applies the barrier to cancel-
aggressor, cancel-both, and decrement-and-cancel.

Minimum-quantity IOC under decrement-and-cancel requires exact coupled
incoming-leaves and external-trade counters. For `D_p` displayed orders and a
maximum of `R_p` remaining reserve rounds at crossed price `p`, its preflight
is

```text
O(O_c + P_c log(P + 1) + sum_p D_p log(R_p + 1)) time
O(1) auxiliary space
```

The scanner visits initial displayed slices in FIFO order, binary-searches
monotone complete-round totals, visits at most one partial round exactly, and
then visits hidden orders. Admission bounds `R_p` by `u32`, so each per-price
binary search requires at most 32 aggregate passes. The scan allocates no
queue and does not mutate matching, risk, reservation, sequence, or public
state.

`OrderBook::immediate_execution_quote` returns exact private-book execution
economics for one hypothetical immediately active market-or-limit order. Under
permit, cancel-aggressor, cancel-resting, or cancel-both, it has the same
one-visit bound:

```text
O(O_c + P_c log(P + 1)) time
O(1) auxiliary space
```

Under decrement-and-cancel it composes the exact reserve-round scanner and has
the same `O(O_c + P_c log(P + 1) + sum_p D_p log(R_p + 1))` time bound stated
above. Each crossed price contributes one constant-time signed `i128` notional
update through the accumulator shared with displayed-liquidity quoting, and
output is one fixed-size value containing provenance, the exact quantity
partition, worst execution price, contributing-price count, and termination.
Both paths allocate nothing and do not mutate or reserve book state.

For `C` distinct prices contributing positive external execution,
`try_immediate_execution_curve` first performs the applicable quote scan `Q`,
fallibly requests capacity for exactly `C` rows, then repeats the identical
immutable scan while appending one aggregate per contributing price. The
allocator may grant more capacity. Since every row contains at least one lot,
`C <= min(P_c, q)` for requested quantity `q`. Total work is
`O(2Q + C) = O(Q)` and caller-owned output is `O(C)`; scanner auxiliary state
remains `O(1)`. No row append grows after the request succeeds. Allocation
failure precedes the second scan and returns no partial output. The embedded
quote, row quantities, derived signed notionals, and final worst price require
no additional book traversal.

`submit_immediate_execution_if` composes ordinary command preparation, the
same private quote, a caller predicate of cost `F`, and commit of the same
canonical fully displayed IOC command. If `Q` is the applicable quote bound
above and `M` is the ordinary IOC matching bound, acceptance is
`O(Q + M + F)` and decline is `O(Q + F)`. The second scan on acceptance changes
the constant factor, not the asymptotic bound. Core rejection and exact replay
bypass both quote and predicate after their existing preparation work. The
coupled-risk path adds two expected `O(1)` authorization checks on acceptance,
one before the predicate and the unchanged commit-time check; a risk rejection
bypasses the predicate. The operation uses `O(1)` auxiliary state, introduces
no allocation, and decline or predicate unwind mutates nothing. Durable
acceptance and core or risk rejection append the existing command and report
frames; decline, unwind, and exact replay append zero frames.

`try_submit_immediate_execution_curve_if` reuses that same command preflight
and its first quote scan `Q`, then requests exactly `C` curve rows and performs
one identical population scan before a predicate of cost `F`. For ordinary IOC
commit cost `M`, acceptance is

```text
O(2Q + C + F + M) = O(Q + F + M) time
O(C) retained caller-owned curve output
```

and decline is `O(2Q + C + F) = O(Q + F)` time with the same `O(C)` returned
curve. Scanner auxiliary state remains `O(1)`. The allocator may grant more
capacity than the exact `C`-row request, but population does not grow the
vector. Curve allocation failure occurs after ordinary immutable preparation
and the first scan but before predicate, identity, sequence, risk, matching, or
WAL mutation. Core/risk rejection and exact replay skip curve allocation, the
second scan, and `F`. Coupled risk retains the two expected `O(1)` checks on
acceptance. Durable acceptance and business rejection append the existing two
frames; allocation failure, decline, unwind, and replay append zero. All
quantity, count, and signed-notional reconciliation uses exact integer
arithmetic, so numerical approximation error is zero.

`submit_new_order_if` and `try_submit_new_order_curve_if` compose the same
conditional evaluator with ordinary `NewOrder` preparation rather than the
A140 canonical-IOC constructor. Let `A` be ordinary preparation cost, `Q` the
applicable A124 scan cost, `C` contributing active prices, `F` predicate cost,
and `M` the submitted order's ordinary commit cost.

- Active quote acceptance is `O(A + Q + F + M)` time; decline is
  `O(A + Q + F)`. Auxiliary observation state is `O(1)`.
- Active curve acceptance is
  `O(A + 2Q + C + F + M) = O(A + Q + F + M)` time; decline is
  `O(A + 2Q + C + F) = O(A + Q + F)`. The returned/retained curve owns
  `O(C)` caller output and scanner state remains `O(1)`.
- Dormant-stop acceptance is `O(A + F + M)` and decline is `O(A + F)`, with
  one fixed-size enum observation and no quote scan or curve allocation.
- Core/risk rejection and exact replay retain their existing preparation/gate
  bound and skip `Q`, `C`, and `F`. Coupled risk adds its expected `O(1)`
  authorization precheck and unchanged commit recheck.

Market-to-limit observation reads the same cached private best as the unchanged
commit in `O(1)`. A valid noncrossing post-only order still performs `Q`, but
its curve has `C = 0`; a crossing post-only order and insufficient FOK are core
rejections and skip observation. Minimum-quantity IOC observation performs the
ordinary A124 scan even when its submitted threshold later causes an accepted
zero-trade cancellation. Durable acceptance and business rejection append two
existing frames; allocation failure, decline, unwind, and replay append zero.
No command/report codec, event bound, or fixed authoritative state changes.

`try_resolve_pegged_price` reads one coherent displayed BBO and performs a
fixed number of checked `i128` tick-index operations, price validations, and
comparisons. Primary, market, and midpoint selection, signed offset, optional
buy cap/sell floor, and allow/reject/passive-slide crossing protection are
therefore `O(1)` time and space, allocate no output, and have zero numerical
approximation error.

`submit_pegged_new_order_if` reuses ordinary conditional new-order preparation
and private observation. Let `A` be ordinary preparation, `Q` the applicable
A124 quote scan, `V = O(1)` exact command/resolution/BBO validation, `F`
predicate cost, and `M` ordinary limit commit. Acceptance is
`O(A + Q + V + F + M)` and decline is `O(A + Q + V + F)`. A stale or
mismatched resolution fails in `O(A + Q + V)` before `F`, mutation, or WAL
append. Core/risk rejection and exact replay skip `Q`, `V`, and `F` under the
ordinary conditional contract. The resolution and paired observation are
fixed-size `O(1)` state. Durable acceptance and business rejection append the
existing two frames; resolution failure, decline, unwind, and replay append
zero. No command/report codec, event bound, or fixed authoritative state
changes.

`submit_replace_order_if` and `try_submit_replace_order_curve_if` reuse the
same evaluator with ordinary `ReplaceOrder` preparation. Let `R` be ordinary
replacement preparation cost, `Q` the applicable A124 scan cost, `C` the
contributing opposite-price count, `F` predicate cost, and `M` replacement
commit cost.

- Active quote acceptance is `O(R + Q + F + M)` time; decline is
  `O(R + Q + F)`. Auxiliary observation state is `O(1)`.
- Active curve acceptance is
  `O(R + 2Q + C + F + M) = O(R + Q + F + M)` time; decline is
  `O(R + 2Q + C + F) = O(R + Q + F)`. The returned/retained curve owns
  `O(C)` caller output and scanner state remains `O(1)`.
- Dormant stop-limit acceptance is `O(R + F + M)` and decline is
  `O(R + F)`, with one fixed-size enum observation and no quote scan or curve
  allocation.
- Core/risk rejection and exact replay retain their existing preparation/gate
  bound and skip `Q`, `C`, and `F`. Coupled risk adds its expected `O(1)` net-
  replacement authorization precheck and unchanged commit recheck.

The old order is same-side state and therefore does not change the opposite-
side A124 scan. A retained-priority same-price reduction still performs `Q`
and obtains `C = 0`; its indexed level, account, and order commit retains the
ordinary replacement bound represented by `M`. Price-changing replacement
retains its existing matching bound in `M`. Durable acceptance and business
rejection append two existing frames; allocation failure, decline, unwind, and
replay append zero. No command/report codec, event bound, or fixed
authoritative state changes.

`submit_cancel_order_if` composes ordinary `CancelOrder` preparation with one
fixed-size `ActiveOrderObservation`, a predicate, and the ordinary cancellation
commit. Let `A` be cancellation preparation cost, `V` the validated active-
order lookup cost, `F` predicate cost, and `M` cancellation commit cost.
Acceptance is `O(A + V + F + M)` time; decline is `O(A + V + F)` time.
Observation and evaluator auxiliary space are `O(1)`. `V` is one expected
`O(1)` order-index lookup plus fixed-size resting or dormant-stop validation.
Core rejection and exact replay retain their existing preparation bound and
skip `V` and `F`. Coupled cancellation authorization is expected `O(1)` and
ordinary commit releases the existing reservation. Durable acceptance and
business rejection append two existing frames; observation failure, decline,
unwind, and replay append zero. No command/report codec, event bound, or fixed
authoritative state changes.

`try_submit_mass_cancel_if` composes ordinary `MassCancel` preparation with
canonical selection into the preparation's fixed-capacity lease, exactly
reserved complete selected-state output, a predicate, and ordinary mass-cancel
commit. Let `A` be ordinary preparation cost, `K` selected orders, `F`
predicate cost, and `M` ordinary mass-cancel commit cost. The expected `O(1)`
account lookup, `K`-member traversal, in-place sort, and `K` selected-row
lookups/validation cost expected `O(K log K)`.

Acceptance is `O(A + K log K + F + M)` time; decline is
`O(A + K log K + F)` time. Accepted commit reuses the prepared IDs and does not
repeat account-list selection or sorting. The constructor-owned lease contains
`O(K)` ID scratch, caller output owns `O(K)` `ActiveOrderSnapshot` rows, and
evaluator auxiliary state is `O(1)`. Core rejection and exact replay skip
selection, output reservation, and `F`; a valid empty selection invokes `F`
with zero rows. Coupled acceptance releases `K` reservations in expected
`O(K)` time. Durable acceptance and business rejection append two existing
frames; output failure, decline, unwind, and replay append zero. Count and
quantity calculations use exact integer arithmetic with zero approximation
error. No command/report codec, event bound, or fixed authoritative state
changes.

`try_submit_account_control_if` composes ordinary `AccountControl` preparation
with the current fence/action/resulting-state observation and, for
block-and-cancel, the same canonical prepared selection and selected-state
output as conditional mass cancellation. Let `A` be ordinary account-control
preparation cost, `K` selected orders, `F` predicate cost, and `M` ordinary
account-control commit cost. Block-and-cancel acceptance is
`O(A + K log K + F + M)` time; decline is `O(A + K log K + F)`. The
constructor-owned lease contains `O(K)` ID scratch, caller output owns `O(K)`
`ActiveOrderSnapshot` rows, and evaluator auxiliary state is `O(1)`. Accepted
commit reuses the prepared IDs without another account-list selection or sort.

Enable acceptance is `O(A + F + M)` and decline is `O(A + F)`, with one
`O(1)` observation and no selected output or lease. Core rejection and exact
replay skip observation/output and `F`; coupled-risk rejection occurs before
selected-state construction and also skips `F`. Coupled acceptance releases
`K` reservations in expected `O(K)` time. Durable acceptance and business
rejection append the existing two frames; query failure, decline, unwind, and
replay append zero. Count, revision, and quantity arithmetic has zero
approximation error, and no wire value or fixed authoritative state changes.

`try_submit_trading_state_control_if` composes ordinary
`TradingStateControl` preparation with the exact current state, requested
target/action/resulting state and, for transition-and-cancel, one canonical
complete active-order selection. Let `A` be ordinary trading-state-control
preparation cost, `O` active selected orders, `F` predicate cost, and `M`
ordinary control commit cost. Selection, in-place ascending-ID sorting, exact
output reservation, and complete selected-state validation cost expected
`O(O log O)`.

Transition-and-cancel acceptance is `O(A + O log O + F + M)` time; decline is
`O(A + O log O + F)`. Accepted commit validates and drains the prepared IDs
without a second all-order scan or sort. The constructor-owned lease retains
`O(O)` ID scratch, caller output owns `O(O)` `ActiveOrderSnapshot` rows, and
evaluator auxiliary state is `O(1)`. Transition acceptance is `O(A + F + M)`
and decline is `O(A + F)`, with one `O(1)` observation and no selected output
or lease. Core rejection and exact replay skip observation/output and `F`;
coupled-risk state-control authorization is account-independent and precedes
observation. Coupled acceptance releases `O` reservations in expected `O(O)`
time. Durable acceptance and business rejection append the existing two
frames; query failure, decline, unwind, and replay append zero. Count,
revision, and quantity arithmetic has zero approximation error, and no wire
value or fixed authoritative state changes.

`try_submit_expiry_sweep_if` composes ordinary `ExpirySweep` preparation with
one canonical selected-ID pass into the existing lease, exactly reserved
complete selected-state output, a predicate, and ordinary expiry commit. Let
`A` be ordinary expiry preparation cost, `K` selected orders, `X` active GTD
orders, `F` predicate cost, and `M` ordinary expiry commit cost. Exact active-
order and expiry-index validation costs `O(K log(X + 1))` after `A`.

Acceptance is `O(A + K log(X + 1) + F + M)` time; decline is
`O(A + K log(X + 1) + F)`. Accepted commit reuses the prepared IDs and performs
no second ordered-prefix traversal or sort. The constructor-owned lease
retains `O(K)` ID scratch, caller output owns `O(K)`
`ActiveOrderSnapshot` rows, and evaluator auxiliary state is `O(1)`. Core
rejection and exact replay skip selection, output reservation, and `F`; a
valid empty prefix invokes `F` without a lease or selected output. Coupled
acceptance releases `K` reservations in expected `O(K)` time. Durable
acceptance and business rejection append the existing two frames; query
failure, decline, unwind, and replay append zero. Count, quantity, and
nanosecond-watermark arithmetic has zero approximation error, and no wire
value or fixed authoritative state changes.

`try_submit_stop_trigger_sweep_if` composes ordinary `StopTriggerSweep`
preparation with canonical selection into the existing lease, exactly reserved
complete dormant-stop output, a predicate, and ordinary trigger-sweep commit.
Let `A` be ordinary preparation cost, `K` selected stops, `S` dormant stops in
the selected side index, `F` predicate cost, and `M` ordinary trigger-sweep
commit cost. Exact trigger-index and dormant-state validation costs
`O(K log(S + 1))` after `A`.

Acceptance is `O(A + K log(S + 1) + F + M)` time; decline is
`O(A + K log(S + 1) + F)`. Accepted commit reuses the prepared IDs and performs
no second eligible-prefix selection or sort. The constructor-owned lease
retains `O(K)` ID scratch, caller output owns `O(K)`
`DormantStopSnapshot` rows, and evaluator auxiliary state is `O(1)`. Core
rejection and exact replay skip selection, output reservation, and `F`; a valid
empty prefix invokes `F` without a lease or selected output. Coupled acceptance
applies ordinary risk transitions for exactly the selected activations and
their execution/cancellation/residual traces. Durable acceptance and business
rejection append the existing two frames; query failure, decline, unwind, and
replay append zero. Count, quantity, price, and source-coordinate arithmetic
has zero approximation error, and no wire value or fixed authoritative state
changes.

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
  `2 × 4,096 × (2 × 24 B + 72 B + 56 B) = 1,441,792 B = 1.441792 MB`
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
  `size_of::<OnceLock<CallAuctionEvent>>() = 192 B`, or
  `73,730 × 192 B = 14,156,160 B = 14.156160 MB` before the same overheads.
  `size_of::<CallAuctionEvent>()` remains 176 B; the larger alignment-sensitive
  slot is the authoritative retained-arena capacity term.

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
An accepted market-to-limit command adds exactly one pricing event to its
ordinary effective-limit bound.

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
activation limit or market collar. Market-to-limit authorization uses the
market collar in `O(1)`; a residual reservation uses the captured limit.
Arming and triggering are expected `O(1)` risk-map transitions; triggering
changes the dormant flag without duplicating account exposure.

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
One market-to-limit pricing event scans `O` private tracked orders to derive an
execution best that includes hidden-only liquidity, uses `O(1)` auxiliary
space, and emits `NoBookChange`. Its later events retain their ordinary bounds.

- Publisher bootstrap is expected `O(O + P log P + S log(O + 1) + T)` for `S`
  dormant stops.
- A healthy full-depth publisher snapshot requests and owns `O(P)` output,
  makes one `O(P)` construction pass and one `O(P)` validation pass, and uses
  `O(1)` auxiliary state beyond the output. Poison rejects in `O(1)` before
  allocation or tree traversal.
- Snapshot validation is `O(P)`. A healthy equal-sequence retry adds one `O(P)`
  complete-image comparison and returns without mutation; a later subordinate-
  coordinate check is `O(1)`. Allocation-free double-buffered forward or poison
  recovery is `O(P log(P + 1))`.
- A complete publisher cross-audit is expected
  `O(O + P + S log(O + 1) + T)` outside adversarial hash collision clusters.

State is `O(O_max + T_max + P_max + E_max)` for one publisher, including the
private dormant-stop map and two trigger arenas, and
`O(P_max + E_max)` for one replica, with the replica reserving four per-side
depth arenas in total (active and standby for bids and asks).

Every continuous-replica economic query first rejects poison in `O(1)`, then
applies one coherent-state gate that descends at most two public AVL extremum
paths. The complete gate is `O(log(P + 1))` time and `O(1)` space for `P`
occupied public prices; it rejects zero extremum aggregates/counts and a
locked/crossed pair before exposing output. Diagnostic sequence, poison,
limits, and allocation telemetry bypass this economic-observation gate.

For `P` occupied public prices on one replica side and requested limit `L`,
`try_depth_iter` has `O(log(P + 1))` gated setup, `O(P)` complete traversal,
and `O(1)` auxiliary space. Each streamed row adds `O(1)` aggregate validation
and can return a typed item error after earlier valid rows. For
`S = min(P, L)`, `try_depth` costs `O(log(P + 1) + S)` time, reserves before
copying, owns `O(S)` output, and never returns a partial vector. An invalid
non-extremum row after the selected prefix is not inspected.

For an inclusive band containing `K` occupied prices and requested limit `L`,
let `S = min(K, L)`. `try_depth_range_iter` costs
`O(log(P + 1) + K)` for complete gated traversal and uses `O(1)` iterator
state. `try_depth_range` validates/counts and copies the selected `S` rows in
two immutable traversals, so it costs `O(log(P + 1) + S)` time with a doubled
constant, exactly reserves `S` rows, and owns `O(S)` output. An inverted band
is empty. Both iterator types are double-ended; the full iterator is exact-
size. The infallible compatibility iterators compose these paths and panic on
typed failure rather than return poisoned or invalid state.

On a healthy continuous replica, `try_best_bid_offer`, `try_best_bid`,
`try_best_ask`, `try_trading_state_observation`, and `try_trading_state`
compose the shared coherent-state gate in `O(log(P + 1))` time and `O(1)`
space. Trading-state observation adds one constant revision/source-sequence
comparison and returns fixed-size instrument/version/sequence/state/revision
provenance.
`try_depth_range_summary` performs one checked fold over the existing band
iterator in `O(log(P + 1) + K)` time and `O(1)` fixed result/state for `K`
selected public levels. Neither query allocates output. Poison rejection is
`O(1)` before traversal. The public corruption category is a static
`MarketDataError::SourceDivergence`; shared human-readable internal validation
detail may allocate only after corruption and is then discarded. No error path
returns a partial summary. The replica exposes no definition-wide summary
because it retains no price-rule endpoints.

Replica `try_public_level` composes the `O(log(P + 1))` shared coherent-state
gate with one exact `O(log(P + 1))` public-AVL lookup, retaining
`O(log(P + 1))` total time and `O(1)` fixed output/state. A present target adds
one constant shared key/aggregate validation. Poison fails in `O(1)` before
tree access; target corruption maps to static source divergence. Neither the
fallible query nor its successful convenience wrapper allocates or mutates
replica state.

For `B` public bid levels, `A` public ask levels, and one independently applied
limit `N`, replica `try_public_depth_imbalance` costs
`O(log(P + 1) + min(B, N) + min(A, N))` time and `O(1)` fixed output/state.
The first term is the shared poison/coherent-extrema gate. Every replica level
is public, so the two folds have no hidden-price traversal term. The query
reuses the authoritative checked per-side accumulator and combined-quantity
calculation. Invalid selected rows and arithmetic overflow return one static
source-divergence category without partial output; poison returns before tree
access. No successful path allocates or mutates replica state.

`try_displayed_liquidity_quote` performs one market-priority fold over the
opposite replica side. For `K` public prices inspected through termination
among `P` occupied prices, it is `O(log(P + 1) + K)` time and `O(1)` fixed
output/state with no successful-path allocation or mutation. It reuses the
authoritative checked fold and signed-notional accumulator. Poison rejection is
`O(1)` before traversal; invalid aggregate/count failure discards the shared
human-readable detail and returns a static source-divergence category without
partial output.

For `P` snapshot rows and `S <= P_max` initialized standby slots, applying one
valid non-stale snapshot costs `O(S + P log(P + 1))`: arena clearing visits
`S` slots, standby filling performs `P` ordered insertions, and the
active/standby swap is constant-time. It clears poison and re-enables the
complete observation boundary at the snapshot sequence.

For replay capacity `N`, one `MarketDataReplayBuffer` initializes `N` optional
typed slots in `O(N)` time and retains `O(N)` state. An `E`-update admission
preflights identity, contiguity, overlap, and collision in `O(E)` time, then
writes only its new suffix with `O(1)` work per update and no allocation. Exact
retained duplicates also cost `O(E)`. Exclusive-cursor range setup is `O(1)`;
iterating `R` returned updates is `O(R)` time with `O(1)` borrowed iterator
state, including physical wrap. Typed in-memory slot bytes and allocator/page
rounding are target-dependent; version-3 encoded updates remain 33 B, 43 B,
66 B, or 91 B by payload kind.

`MarketDataReplica::apply_snapshot_and_replay` validates `P` snapshot rows,
clears `S` initialized standby slots, fills the candidate image, and applies
`R` retained updates in `O(S + P log(P + 1) + R log(P + 1))` time. A healthy
final result at the original sequence adds an `O(P)` full-state comparison;
a later result adds `O(1)` coordinate checks. Success and ordinary rollback use
`O(1)` auxiliary state and allocate nothing. Rollback swaps the original active
arenas in `O(1)`; if the rejected continuous candidate is crossed, clearing its
initialized slots costs `O(S_c)`, where `S_c <= P_max`. Standby occupancy is
scratch telemetry and need not equal its pre-transaction value.

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
- An indicative publication fixes `E = 1`. Discovery is already charged to the
  engine as `O(B + A)`; publisher and replica retention, validation, and
  invalidation of its fixed-size optional state are `O(1)`.
- An aborted self-trade rejection fixes `E = 1` and projects one
  `NoPublicChange` in `O(1)`; its exact retry fixes `E = 0`.
- A healthy snapshot requests and owns `O(P)` output, performs `O(P)`
  construction plus `O(P)` validation, and uses `O(1)` auxiliary state beyond
  the output. Poison rejects in `O(1)` before allocation or tree traversal.
  Snapshot validation is `O(P)`. Healthy equal-event retry comparison is
  `O(P)` and nonmutating; later subordinate-coordinate checks are `O(1)`.
  For `S <= P_max` initialized standby slots, double-buffered forward or
  poisoned snapshot application is allocation-free after construction and
  `O(S + P log(P + 1))`.

Publisher fixed state is `O(O_max + P_max)` and replica fixed state is
`O(P_max + E_max)`, including four active/standby side arenas in total.
Default maxima are 4,096 active orders, 4,096 limit prices per side, and
8,193 updates per command.

Every call-auction replica economic query first constructs the fixed-size
`CallAuctionMarketDataObservation`. Poison and scalar chronology checks are
`O(1)`. Reading and validating both best AVL extrema is `O(log(P + 1))` for
`P` occupied limit prices, with `O(1)` state and no allocation. Market and
selected-limit aggregate validation is `O(1)`: for count `C`, quantity `Q`,
active-leaves increment `q_inc`, and per-order maximum `q_max`, it checks exact
`u128` bounds `C q_inc <= Q <= C q_max` and `Q mod q_inc = 0`. The increment
is the lower bound because a partial fill can leave active quantity below the
new-order admission minimum.

For one replica side and requested limit `L`, `try_limit_depth_iter` has
`O(log(P + 1))` gated setup, `O(P)` complete traversal, and `O(1)` auxiliary
space; each item adds `O(1)` price/aggregate validation. For
`S = min(P, L)`, `try_limit_depth` validates and copies through two
`O(log(P + 1) + S)` prefix passes, exactly reserves `S` rows, and owns `O(S)`
output. For an inclusive band containing `K` occupied limit prices,
`try_limit_depth_range_iter` costs `O(log(P + 1) + K)` total time and `O(1)`
auxiliary space. For `S = min(K, L)` selected range rows,
`try_limit_depth_range` makes the same two validated passes, exactly reserves
`S` rows, and owns `O(S)` output. An inverted band is empty, market-constrained
interest remains separate, and locked/crossed limit extrema are valid. Both
iterator types are double-ended; the full iterator is exact-size. A streamed
valid prefix may precede a typed deeper-row failure; materializers expose no
partial vector. Applying a current valid snapshot retains the existing
`O(P log(P + 1))` standby-fill and constant-time arena-swap repair bound.

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
residency are target-dependent; version-5 payload bytes are unchanged by the
process-local replay ring. Indicative updates are 84 B without executable
interest and 124 B with clearing; an empty closed snapshot is 113 B.

`CallAuctionMarketDataReplica::apply_snapshot_and_replay` adds snapshot
validation and installation cost `O(S + P log(P + 1))` to the ordinary
per-complete-batch replay bounds above. A healthy final result at the original
event boundary adds an `O(P)` comparison of limit depth and `O(1)` scalar/
cycle state; a later result adds `O(1)` coordinate checks. Success and rollback
use `O(1)` auxiliary state, allocate nothing, and restore active arenas through
constant-time swaps. A valid rejected candidate may remain in standby scratch.

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
trade, indicative-state, and event lineage without executing commands. The
optional current indication is derived from accepted history rather than
duplicated in a direct row. Coupled capture additionally sorts `A` account
rows in `O(A log A)`, reconstructs
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
resolve the journal through the same transaction-index consistency path, then
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

### Borrowed ledger history

For `R` retained records containing `T` transaction entries,
`try_record_view` converts and checks one one-based sequence in `O(1)` time,
then performs expected `O(1)` transaction-index work for an entry or correction
and expected `O(N)` work for an `N`-entry batch. Sequence zero and positions
beyond `R` return absence without index work. A retained journal/index
contradiction is a typed result.

`retained_history` has `O(1)` setup and exact-size, double-ended iterator state.
Consuming all records performs expected `O(T)` index work. Each
`LedgerRecordTransactions` iterator has `O(1)` setup and state and consumes one
record in `O(N)` time. Successful borrowed lookup and iteration allocate no
output, clone no entry or batch, and mutate no ledger or durable state. A full
adversarial transaction-hash collision cluster can increase complete traversal
to `O(T^2)` without storage growth. The compatibility `record` method adds one
immutable outer-handle clone after the same resolver succeeds.

### Account-and-asset ledger statements

For `R` retained records containing `T` transaction entries whose posting
counts are `L_i`, `account_statement` composes borrowed history resolution
with one `O(log(L_i + 1))` binary search over each entry's canonical
`(asset, account)` posting order. Complete traversal is expected
`O(T + sum(log(L_i + 1)))` time with `O(1)` iterator state. A full adversarial
transaction-index collision cluster can increase history resolution to
`O(T^2)` without storage growth.

Forward and reverse traversal borrow each selected entry and posting, allocate
no output or auxiliary storage, and mutate no ledger or durable state. The
iterator still resolves and checks records with no matching posting, so sparse
selection changes output cardinality but not the complete-history traversal
bound.

### Point-in-time ledger balances

For a requested prefix containing `R` records and `E` transaction entries
whose posting counts are `L_i`, `try_balance_at` performs expected
`O(E + sum(log(L_i + 1)))` time with `O(1)` auxiliary space. The selected key
is looked up through two sign-filtered passes per record; this is a constant
factor, and each pass visits every entry at most once. Generation zero and
future-generation rejection are `O(1)`. A current-generation query adds one
expected `O(1)` balance-index lookup and equality check.

The query returns one `i128`, allocates no output or auxiliary storage, and
mutates no ledger or durable state. Entry, correction, and batch records are
applied only at their final atomic boundary. A full adversarial transaction-
index collision cluster can increase the history-resolution component to
`O(E^2)` without storage growth.

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

For one accepted call-auction uncross with `T` trades, `C` remainder
cancellations, `F` explicit or calculated fee transfers, `N = T + F` entries,
and `L <= 4T + 2F` non-zero posting legs, report and canonical fee-binding
validation is `O(T + C + F)` time and `O(1)` auxiliary space. Settlement
construction fallibly reserves exactly `N` entry handles and two postings per
fee, uses checked `i128` DVP arithmetic, and owns `O(N + L)` result storage
before ledger mutation.

For an immutable fee schedule with `S` configured side rules,
`1 <= S <= 2` and `F = T * S`. Schedule validation is `O(1)`. Assessment is
`O(T + C + F)` time; each basis/rate calculation uses a fixed number of
checked `i128`/`u128` multiply, divide, remainder, comparison, and clamp
operations. `assess_report` fallibly reserves exactly `F` output rows and owns
`O(F)` result storage. Direct calculated settlement instead reserves exactly
`F` `CallAuctionFee` values before constructing the existing settlement; it
does not materialize the public assessment vector.

The `T = 1, F = 0` case uses ordinary entry preparation. Otherwise batch
construction adds expected `O(N)` time and `O(N)` identity storage, after which
the existing batch bounds above apply: `O(L log L)` preparation,
`O(N + L + U)` auxiliary memory, and expected `O(N + U)` commit. Durable
settlement adds one entry or batch frame; exact replay is resolved without
frame growth. The fee schedule and revision add no ledger, WAL, or checkpoint
storage.

For a correction over `N` original settlement entries with `L_o` posting legs
and a replacement of `M` entries with `L_r` posting legs, let
`K = N + M`, `L = L_o + L_r`, and `U` be the affected balance-key count.
Constructing the exact inverses costs `O(N + L_o)` time and storage; collecting
the complete correction fallibly reserves exactly `K` entry handles. A bust
with `N = 1` and `M = 0` uses ordinary entry preparation. Every other
correction uses the batch path: expected `O(K)` identity construction,
`O(L log L)` preparation, `O(K + L + U)` auxiliary storage, and expected
`O(K + U)` commit. Exact original entry/batch grouping is checked before
mutation. Durable correction adds one ordinary entry or kind-`7` batch frame
and has no intermediate balance image.

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
