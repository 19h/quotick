# Architecture

## System boundary

The implemented system is a deterministic state machine with local durable
matching and ledger runtimes. One `OrderBook` owns one instrument and accepts
commands from exactly one mutating thread. `DurableOrderBook` records each
command before matching and records its trace afterward. `DurableLedger`
records each prepared entry, indivisible correction, or ordered multi-entry
batch before committing calculated balances.

```text
implemented

immutable instrument definition -> definition-bound CRC-32C WAL
                                             |
validated command -> account risk -> command WAL -> instrument book
                         ^              ^                 |
                         |              +-- report WAL <--+-- sequenced trace
                         |                                   |
              positions/reservations <---------------- trade/order events
                                                             |
                                       absolute L2 updates / trade prints
                                                             |
                                            gap-detecting public replica
                                                             |
                                                           trade
                                                                       |
                                                   version-bound settlement rules
                                                                       |
                                                            balanced journal entry
                                                                       |
                                                               ledger-event WAL
                                                                       |
                                                                 account balances

required platform layers, not implemented

gateway -> authentication -> portfolio/collateral risk -> replicated sequencer and shards
   -> clearing lifecycle -> reporting
```

## Matching invariants

1. Every command matches the book's instrument identifier and immutable
   definition version before business-state access.
2. New and replacement quantities satisfy the configured lot increment and
   inclusive size bounds; limit prices satisfy the signed tick grid and collar.
3. New orders and replacements require `Open`; cancellation remains available
   in `Open`, `CancelOnly`, `Halted`, and `Closed` states after identity checks.
4. An active order appears in exactly one hash-index entry and one FIFO level.
5. A level head has no previous order; a level tail has no next order.
6. Every active order has non-zero total and displayed leaves. Fully displayed
   orders expose all total leaves; reserve orders expose at most their fixed
   peak. Level quantity is the `u128` sum of displayed leaves, not hidden total
   leaves.
7. Bids execute from highest price to lowest; asks execute from lowest price to
   highest; equal-price orders execute in insertion order.
8. Trade price is the resting order price and every trade carries the book's
   immutable instrument version.
9. FOK validation precedes every matching-state mutation.
10. Exact command replays reproduce the original event sequence and cannot
   reapply state.
11. A command identifier reused for different content cannot mutate state.
12. Event sequences are strictly increasing within a book.
13. Order identifiers cannot be reused after an accepted new order.
14. Reserve admission is immutable per instrument version. A reserve peak is
    lot-grid aligned, strictly smaller than total quantity, and the
    replenishment count implied by an admitted quantity/display state cannot
    exceed the configured `u32` cap.
15. A reserve qualifier is accepted only for a resting-capable limit order.
    A marketable GTC reserve order may execute from its total incoming leaves;
    the peak applies only to a residual that joins the book.
16. Maker execution and decrement-and-cancel STP consume at most the current
    displayed slice. When that slice reaches zero with hidden leaves remaining,
    the same order ID exposes `min(peak, total leaves)`, moves to the price-level
    FIFO tail, and emits a separately sequenced refresh event.
17. FOK liquidity inspection uses total resting leaves, including hidden
    reserve quantity, while public depth and visible order count use only active
    displayed slices.
18. Cancellation removes total leaves. A same-price quantity reduction retains
    priority only when the display policy is byte-for-byte unchanged; changing
    a reserve peak loses priority, and conversion between reserve and fully
    displayed modes is rejected.
19. Identifier-capacity preflight uses the instrument's replenishment cap to
    bound all possible trade and event identifiers before mutation in `O(1)`.
20. Mass cancellation is account-scoped within one instrument-version shard and
    optionally side-scoped. It remains admissible in every trading state,
    selects only active orders owned by that account, and cancels them in
    strictly ascending `OrderId` order.
21. Each selected order emits its ordinary cancellation event with total leaves
    and a mass-cancel reason. A final completion event reports the exact `u64`
    order count and `u128` total cancelled lots; an empty selection still emits
    one completion event.
22. Every active order appears exactly once in an intrusive account/side list,
    with consistent head, tail, forward, backward, count, and event-work state.
    Reserve FIFO-tail refresh preserves those links without membership churn.
    Mass-cancel preparation determines `K` and fallibly reserves capacity for
    `K` canonical IDs and `K + 1` events before durable command append or book
    mutation. Commit traverses exactly `K` members into that buffer, sorts the
    unique IDs in place, then detaches the selected side or account index.
    Neither vector grows during cancellation; execution removes only selected
    order IDs in ascending order and never scans unrelated active orders.
23. Each side stores occupied prices in a finitely bounded stable-slot AVL arena
    and caches its complete best `PriceLevel`. Strict key ordering, cached node
    heights, balance factors in `[-1, 1]`, occupied reachability, and the
    intrusive vacant-slot free list are independently audited. The cached price,
    FIFO head/tail, displayed-lot sum, and visible order count equal the
    corresponding extremal AVL entry. Every level aggregate mutation refreshes
    the cache when it targets the current best; deletion of the best recomputes
    the new extremum before control returns to matching.
24. FOK inspection never mutates state or materializes reserve slices. At a
    crossed price without self liquidity, every external total leaf is eligible.
    Cancel-resting excludes self orders and retains all external total leaves.
    For cancel-aggressor and cancel-both, the first self order is a FIFO barrier:
    only current displayed slices of earlier orders precede it because refreshed
    slices rejoin at the tail. The scan visits each inspected active order at
    most once and uses constant auxiliary space.
25. Every book has finite validated maxima for active orders, active accounts,
    occupied prices per side, accepted order IDs, retained account controls,
    retained commands, and events
    per execution report. Active accounts and per-side levels cannot exceed
    active-order capacity; accepted identity and ordinary history can establish
    every maximum active order. Report capacity is at least
    `max_active_orders + 1`, so one maximum-size mass cancellation always fits.
26. The tail of retained-command capacity reserves at least one slot per maximum
    active order. Once ordinary history fills, new and replace commands stop.
    Only a cancel, mass cancel, revision-valid block-and-cancel account control,
    or revision-valid instrument transition-and-cancel into an entry-closed
    state that passes current core business validation may enter the reserve;
    reopening remains ordinary admission; malformed, unknown, wrong-owner, or
    wrong-instrument controls cannot consume it. Exact cached retries remain
    available even at total exhaustion.
27. Construction fallibly reserves both price-level AVL arenas, all five
    matching hash indexes, and the coupled-risk profile and reservation indexes
    through their complete configured maxima. `PreparedCommand` owns the fallibly reserved
    unique event and optional mass-cancel/account-control/instrument-control vectors; preparation borrows matching
    and coupled-risk state immutably and cannot change semantic or allocator
    state. Durable wrappers complete that preflight before appending a command
    frame, so per-command vector failures are unsequenced operational results and
    cannot leave a dangling WAL command. Price-level and intrusive account-link
    mutation allocates no node after construction. Standard-library matching
    and risk hashes begin with complete cardinality headroom, but arbitrary
    deletion/different-key insertion churn can still rehash or allocate; A12
    therefore excludes allocator-failure continuation on those paths.
28. Checkpoint restoration rejects current matching, account-control, or retained report
    event counts above selected limits. Raw WAL replay reconstructs under the
    selected limits and fails explicitly if any retained historical transition
    exceeds them. Limits may be enlarged at restart; lowering them is valid only
    when the selected recovery path fits.
29. A GTC/post-only capacity preview is invoked only when an active-order,
    active-account, or same-side price-level bound is already full. It predicts
    whether a residual will rest without mutation or reserve-slice
    materialization: cancel-resting excludes self leaves; cancel-aggressor and
    cancel-both stop at the first self FIFO barrier; decrement-and-cancel consumes
    self and external total leaves through replenishment. A proved no-residual
    order bypasses resting-capacity rejection. A proved residual means every
    crossed opposite level was completely removed, so its cached order counts
    and the account index yield exact final active-order and active-account
    cardinalities before append; same-side price-level capacity is unchanged by
    opposite-side matching. A price-changing replacement invokes the same
    preview only when its target price is absent, its old level remains occupied
    after removal, and the same-side level bound is full. Full execution or an
    aggressor-terminating STP encounter proves that no target level is created;
    only a proved resting residual consumes the new level.
30. Command preparation binds the command, completed core business result,
    process-local non-reused book identity, retained-command cardinality, and
    fallibly reserved unique event buffer in one opaque token. Commit rejects a
    foreign token or an unrelated intervening command before mutation; an
    intervening exact command returns its cached replay. Direct, risk-managed,
    durable, and durable-risk submission consume this token without repeating
    capacity, identifier, FOK, core business, or report-buffer preparation.
    Durable paths append the token's command only after the buffer exists.
31. An account without retained administrative state is enabled at revision
    zero. Account control uses compare-and-increment revision semantics. A stale
    or exhausted revision is a nonmutating sequenced rejection. Block-and-cancel
    closes entry and cancels all account orders in ascending `OrderId` order in
    one command/report; enable reopens entry without cancellation. New and
    replace commands observe the fence, owner cancellation remains available,
    and exact retries cannot advance revision twice. The never-evicted control
    hash is constructor-reserved, finite, checkpoint-derived from history, and
    included in structural and market-data publisher cross-audits.
32. Effective instrument trading state starts from the immutable definition at
    revision zero. Each accepted state control compares the exact revision,
    increments it once without wrap, and changes to a different open,
    cancel-only, halted, or closed state. Transition-only changes admission but
    retains resting orders. Transition-and-cancel is invalid when reopening;
    otherwise it pre-reserves every active `OrderId`, sorts ascending, cancels
    all total leaves, and commits the state in one report. New and replace read
    effective state; owner cancel, mass cancel, account control, and state
    control remain identity-gated in every state. Exact retries cannot advance
    revision twice. Effective state is derived from retained history during
    checkpoint restoration and cross-audited by market-data publication.
33. Every completed report owns an immutable `EventTrace`. The first response,
    retained idempotency entry, exact retries, and in-memory checkpoint copies
    share one `Arc<Vec<Event>>` owner and its event buffer; cloning is `O(1)`.
    Preparation allocates the Arc control block and fallibly reserves the
    complete safe vector buffer before durable append or the first transition.
    Builder finalization moves the owner in `O(1)` without allocation or event
    copy. Explicit diagnostic mutation is
    copy-on-write and cannot modify cached history. Equality, validation, and
    encoding depend only on ordered event values, so pointer identity and spare
    vector capacity never enter replay or wire semantics.
34. Every resting order contributes mutation-maintained future event work. A
    fully displayed order contributes one unit. A reserve order with leaves
    `L`, displayed leaves `D`, and peak `p` has
    `s = 1 + ceil((L - D) / p)` remaining slices and contributes `2s - 1`
    interaction/refresh units. Each price level, side, and account/side index
    equals the independently recomputable sum of its orders. Preparation combines
    these aggregates with the incoming quantity in lot-increment units, STP
    policy, TIF terminal event, and command prefix to obtain a safe `O(1)` event
    and trade bound. Sequence/trade identifiers and the complete report-vector
    capacity are reserved against that bound before durable append or the first
    transition; an event push beyond it is an invariant failure. Side-wide aggregates include
    uncrossed prices and may therefore overestimate storage and reject earlier
    near sequence exhaustion, but cannot underestimate execution.

The book stores each side in a stable-slot indexed AVL arena with a
mutation-maintained complete best-level cache. The arena is fallibly reserved to
`max_price_levels_per_side` before the book exists; deletion links its slot into
an intrusive free list, and insertion reuses that slot without allocation. This
provides worst-case `O(log P)` lookup/insertion/deletion/successor/predecessor,
`O(P)` deterministic ordered traversal, and `O(1)` best-price, best-FIFO-head,
and best-snapshot discovery. It uses `HashMap<OrderId, RestingOrder>` for direct lookup,
`HashMap<AccountId, AccountOrderIndex>` containing side-specific intrusive
head/tail/count/aggregate state, and two independent pairs of doubly linked
order identifiers: price FIFO links and account/side membership links. Ordinary
account insertion/removal is `O(1)`; canonical account selection traverses the
selected links and sorts unique `OrderId` values in the already-prepared vector.
`OrderBookLimits` bounds all monotonic and active matching indexes;
`RiskManagedLimits` independently adds the registered-profile maximum. Mandatory
construction-time reservation covers both complete price arenas, active orders,
active accounts, accepted IDs, retained account controls, retained reports, registered profiles, and
coupled-risk reservations.
Fallible construction and durable recovery report the first failed arena/hash
resource before state exists. Read-only `hash_index_status` telemetry exposes
each matching index's configured, allocated, and occupied entry counts.
Normal capacity preflight is expected `O(1)`. Only at a full resting bound—or
the equivalent full same-side-level replacement boundary—does the residual
preview inspect `O_c` orders in `P_c` crossed levels in
`O(O_c + P_c log P)` time. At a full new-account bound, proving complete account
release can additionally inspect all `O` active account memberships in expected
`O(O)` time. Both paths use `O(1)` auxiliary space.
Preparation performs these costs at most once per composed submission and owns
the unique empty report vector after `try_reserve_exact` succeeds. Commit adds
expected `O(1)` identity, idempotency, and generation validation before the
already-prepared transition.
For `E` events, event construction and binary encoding remain `O(E)`, while
builder finalization is `O(1)` and retains the original event buffer. Preparation
derives a safe command-specific event/trade bound in `O(1)` from level, side,
and account aggregates. The complete vector capacity is allocated before
matching mutation, so no event insertion reallocates. Retained
cache/replay/checkpoint trace cloning is `O(1)` and adds only a shared-owner
handle; it neither allocates nor copies that buffer. Conservative inclusion of
uncrossed opposite prices can retain unused vector capacity.

## Call-auction clearing-price invariants

1. The analytical kernel consumes positive aggregate bid levels in strictly
   descending price order and positive aggregate ask levels in strictly
   ascending price order, plus zero-or-positive market buy and sell quantities.
   Every level, both endpoints of an inclusive finite candidate-price band, and
   the reference price must lie on one explicit positive tick grid anchored at
   raw price zero. The reference and limit levels may lie outside the candidate
   band; invalid shape, alignment, band ordering, or arithmetic is rejected
   before a result is returned.
2. At candidate price `p`, cumulative demand is
   `D(p) = market_buy + sum(quantity_i | bid_price_i >= p)`, cumulative supply is
   `S(p) = market_sell + sum(quantity_i | ask_price_i <= p)`, executable quantity is
   `E(p) = min(D(p), S(p))`, and absolute imbalance is
   `I(p) = |D(p) - S(p)|`. Quantities are instrument-defined lots and all
   aggregates use checked `u128` arithmetic.
3. Every in-band grid price with `E(p) > 0` is eligible, including an unquoted
   price between order levels and every price in a market-only auction. Ranking
   is lexicographic: maximize `E`, minimize `I`,
   optionally follow same-side imbalance pressure (higher for buy, lower for
   sell), minimize unsigned distance to the reference price, then apply the
   configured lower- or higher-price final tie break. Reference proximity is
   clamped to the selected interval when the reference lies outside the band.
   Any state with zero executable quantity returns no clearing price.
4. Demand changes one tick above a bid and supply changes at an ask. Merging
   those monotone transitions inside the band evaluates each complete
   constant-state grid interval once, including the reference or
   pressure-selected interval edge. Orders beyond a band endpoint contribute
   exactly when their limit remains executable inside the band.
   For `B` bid levels and `A` ask levels, discovery is `O(B + A)` time and
   `O(1)` auxiliary space, performs no heap allocation, and does not enumerate
   the numeric price range.
5. Discovery is a pure aggregate calculation. It represents market interest
   and an already-selected static candidate band, but does not derive either
   from order types, reference data, collars, dynamic thresholds, or a session
   state. It does not itself accept orders, allocate fills among orders, mutate
   matching or risk state, emit execution/market-data events, or persist/replay
   an uncross. A separate collection book now supplies canonical interest and
   can consume a plan process-locally; durable sequenced execution still
   requires an auction command state machine.
6. Price-time allocation consumes separate buy and sell order slices. Market
   constraints precede limits; limit prices are economically best-to-worst
   (descending buys and ascending sells); equal constraints are strictly
   ordered by caller-defined class, priority sequence, then `OrderId`. Every
   limit must use the discovery grid, and an explicit finite limit bounds each
   input side before scanning or allocation.
7. Eligible order quantities are reconstructed at the selected clearing price
   and must exactly equal the discovery result's buy and sell aggregates. A
   non-zero executable quantity is allocated independently on each side by one
   literal priority walk. Every emitted fill is positive, retains its source
   index and order identity, and both fill sums equal `E(p)`, including when the
   aggregate exceeds `u64::MAX` lots.
8. The allocator first determines exact fill cardinalities and fallibly reserves
   both complete result vectors before emitting either side. For `B` supplied
   buys, `A` supplied sells, and `F_b + F_a` positive fills, validation and
   allocation are `O(B + A)` time and result storage is `O(F_b + F_a)`; no
   vector grows during fill construction.
9. The allocation plan is immutable analytical output. It does not pair buyer
   and seller counterparties, apply self-trade prevention, implement pro-rata
   or size priority, mutate order leaves, assign trade/event identifiers,
   publish imbalance data, or provide command/WAL/checkpoint atomicity.

## Call-auction collection-book invariants

1. One `CallAuctionBook` owns one immutable instrument definition and is mutated
   by one serialized writer. Admission checks instrument identity/version,
   limit tick/collar rules, and lot/size rules. The book primitive does not
   interpret phase; `CallAuctionEngine` supplies the process-local controller,
   and the definition's continuous `TradingState` is not auction authority.
2. Active interest comprises fully active market orders and limit orders.
   Opposing limits may be locked or crossed; the continuous `OrderBook` invariant
   prohibiting retained crosses is not weakened. Reserve/display quantity,
   amendments, expiry, session eligibility, and venue-specific auction order
   categories are absent.
3. Constructor-validated limits bound active orders `O_max`, never-reusable
   accepted identities `I_max`, and occupied limit prices `P_max` independently
   per side. Stable-slot AVL arenas reserve all identity and price slots plus
   both aggregate-level and order-level scratch vectors before the book exists.
   Admission, cancellation, and scratch reconstruction do not allocate after
   construction, including under arbitrary bounded remove/insert churn.
4. The collection policy has priority class `0`. Market orders precede limits;
   limits are enumerated best-to-worse (descending buys, ascending sells); equal
   constraints follow the book-assigned strictly increasing admission sequence.
   `OrderId` remains unique and permanently consumed after acceptance.
5. Each market side and occupied limit price owns an intrusive FIFO with exact
   `u128` quantity and `u64` order-count aggregates. Cancellation checks account
   ownership, unlinks head/middle/tail state, removes an empty price slot, and
   does not release accepted identity. Offline validation independently audits
   both AVL arenas, every link, queue membership, aggregate, priority relation,
   instrument rule, finite bound, and constructor reservation.
6. Indicative discovery rebuilds canonical aggregate scratch and invokes the
   A60 kernel with current market totals. Allocation-plan construction rebuilds
   canonical order scratch and invokes A61. The indicative result carries a
   process-local book identity and exact mutation revision; allocation rejects
   a foreign or stale result before rebuilding scratch, then independently
   reconciles eligible totals. Analysis never changes active order state.
7. With `I` accepted identities, `O` active orders, and `P` occupied prices,
   admission is `O(log I + log O + log P)` and cancellation is
   `O(log O + log P)`. Aggregate scratch plus discovery is `O(B + A)`.
   Order scratch is `O(O log O + P)` because intrusive links contain stable
   order identities resolved through the AVL; A61 then adds `O(O)` validation
   and `O(F_b + F_a)` immutable result space. Collection state occupies
   `O(I_max + O_max + P_max)` reserved memory.
8. `prepare_uncross` first obtains the revision-bound A61 allocation, determines
   the exact number of buyer/seller pairs by a two-pointer priority walk, and
   fallibly reserves the complete trade vector. Every pair uses the common
   clearing price, positive `Quantity`, both order/account identities, and one
   contiguous book-local `TradeId`; neither trade identity nor book state
   advances during preparation.
9. Counterparty pairing preserves each side's allocation order and repeatedly
   transfers the smaller remaining side fill. If `F_b` and `F_a` are positive
   fill counts, pair count `T <= F_b + F_a - 1`; all paired quantities sum in
   `u128` to `E(p)`. The only represented self-trade policy is explicit
   `Permit`; preventive/cancel/decrement policies are not inferred.
10. Remainder policy is explicit: `RetainAll`, `CancelMarket`, or `CancelAll`.
    Preparation performs a linear merge of source-indexed fills with canonical
    orders, computes every positive post-fill remainder, and fallibly reserves
    its exact cancellation vector. A retained partial order keeps its original
    priority. Entry minimum quantity is not reapplied to residual leaves; leaves
    remain positive, no greater than the entry maximum, and lot-grid aligned.
11. The opaque preparation carries process-local book identity and exact source
    revision. Commit rejects foreign/stale input and validates every fill, pair,
    account, trade-ID range, aggregate, and cancellation before mutation. It
    then reduces/removes fills, removes selected remainders, and advances trade
    identity plus book revision as one allocation-free state transition. The
    preparation is move-only; a second commit is impossible without already
    stale state.
12. For `M <= O` affected orders, commit is
    `O(M(log O + log P))` because active identity and price indexes are AVL
    arenas. Preparation adds `O(O log O + P + F_b + F_a + T)` time and
    `O(F_b + F_a + T + C)` fallibly reserved result space for `C` cancellations.
    The book primitive itself has no command sequence, phase, idempotency, or
    durability semantics. `CallAuctionEngine` supplies the first three, and the
    version-4 durable wrapper supplies stable wire encoding, full-WAL recovery,
    and snapshot-version-4 checkpoint/cutover recovery. The separate
    `CallAuctionRiskManagedEngine` supplies optional profile admission,
    reservations, positions, and independently replayed coupled checkpoints;
    settlement, public/private auction transport, preventive self-trade
    policies, and venue conformance remain absent.

## Sequenced call-auction engine invariants

1. One `CallAuctionEngine` owns exactly one `CallAuctionBook`, one phase/cycle
   record, one never-evicted command cache, and contiguous command/event
   sequences. One serialized writer is authoritative; no internal lock or
   distributed ownership protocol is inferred.
2. The exact phase graph is `Closed -> Collecting <-> Frozen -> Closed`, with an
   additional explicit `Collecting -> Closed` edge. Starting a cycle requires
   the exact successor `AuctionId` (the first is `1`) and the exact phase
   revision. Every accepted phase transition advances the revision once without
   wrap. Closing retains any active interest; a successful uncross closes after
   applying its fills and selected remainder policy.
3. New interest presents the exact active `AuctionId` and phase revision and is
   valid only in `Collecting`, fencing delayed entry from an earlier cycle or
   freeze/reopen boundary. Owner-checked cancellation is valid in every phase
   and deliberately revision-independent so retained interest can never be
   stranded after an explicit close or remainder-retaining uncross. Uncross
   presents the exact cycle/revision and is valid only in `Frozen`. An empty or
   otherwise non-executable uncross is a sequenced rejection and leaves the
   phase frozen.
4. Routing, active-cycle identity, and presented phase revision are checked
   before phase-specific business semantics. Accepted commands and business
   rejections each consume one command sequence; a rejection changes neither
   phase nor collection state. History exhaustion, allocation failure, stale or
   foreign preparation, and counter exhaustion are operational errors and are
   not sequenced.
5. An exact `CommandId` retry is resolved before capacity gates, returns the
   original report with `replayed = true`, and shares the same immutable
   `Arc<Vec<CallAuctionEvent>>` storage in `O(1)` time and space. Reuse of that
   identifier with different command content is a collision and has no effect.
6. Preparation is move-only and semantically nonmutating. Its mutable engine
   borrow permits the owned book to rebuild pre-reserved analytical scratch but
   cannot alter active orders, phase, identities, or sequences. It owns the
   complete fallibly reserved report vector and, for uncross, the complete
   prepared trade/cancellation storage. Commit rejects a token from another
   engine or any token stale against engine/book state before mutation.
7. Limits require a protected terminal lane of at least `O_max + 2` reports and
   one report capacity of at least `2 O_max + 1` events. After the ordinary
   prefix is full, only a currently valid owner cancellation, freeze/close
   transition, or executable uncross may consume the lane. Invalid, stale, and
   otherwise rejected commands cannot erode it; exact retries remain available.
   Closed-phase cancellation plus the `O_max + 2` bound prevents retained
   interest from becoming unreachable after history saturation.
8. Every new report has one contiguous command sequence and a non-empty event
   trace with contiguous global event sequences. Event grammar is command- and
   outcome-specific: phase, admission, cancellation, rejection, and uncross
   trade/remainder/completion traces are not interchangeable. Offline
   validation audits cache key/content/report identity, sequence continuity,
   event grammar and counts, phase/cycle coherence, cache reservation, and the
   complete underlying collection book.
9. Phase commands, business rejections, and monotonic cache lookup are expected
   `O(1)`. Admission/cancellation inherit the collection-book AVL bounds.
   Uncross preparation adds `O(T + C)` event construction to the book bound;
   commit adds `O(T + C)` emission into already reserved immutable trace storage
   and performs no vector growth. The independent audit is
   `O(H log H + I + O + P)` time with `O(H + O)` scratch. With retained reports
   `H`, engine state is `O(H_max + I_max + O_max + P_max)`.
10. The engine remains process-local authority. Stable command/report codecs,
    semantic checkpoint capture/direct restoration, deterministic full-WAL
    recovery, and checkpoint-anchored prefix cutover are supplied by the engine,
    snapshot, and durable layers below. Risk or ledger effects, market-data
    publication, settlement, calendar/session scheduling, controller
    authentication, reference/dynamic-band sources, and venue conformance are
    not inferred.

## Coupled call-auction risk invariants

1. `CallAuctionRiskManagedEngine` is the sole supported mutation path when a
   call-auction shard is risk controlled. It owns one `CallAuctionEngine`, one
   immutable account-profile registry, and exactly one conservative reservation
   for every active collection-book order. Profile registration is bounded,
   constructor-reserved, duplicate checked, and closes after the first command
   is sequenced.
2. Core route, instrument, phase, cycle, revision, identity, ownership, and
   capacity results precede risk. Only a core-admissible submit reaches risk;
   missing, blocked, reduce-only, quantity, notional, aggregate-open, position,
   and arithmetic failures become ordinary sequenced auction rejections. Exact
   retry returns the cached report and never reapplies risk state.
3. Conservative per-lot valuation is the maximum reachable absolute raw-price
   magnitude. A market order uses `max(abs(collar minimum), abs(collar
   maximum))`; a buy limit uses `max(abs(collar minimum), abs(limit))`; a sell
   limit uses `max(abs(limit), abs(collar maximum))`. Reservation notional is
   that `u64` magnitude multiplied by positive leaves lots in checked `u128`
   arithmetic. Partial fills retain the identical per-lot valuation rather
   than introducing a post-acceptance aggregate-limit discontinuity.
4. Admission evaluates per-order quantity/notional, aggregate active order
   count/quantity/notional, reduce-only state, and independent full-execution
   long/short position bounds. Market and limit auction interest can remain
   active under a remainder-retaining policy, so every accepted submit is
   evaluated as potentially reserving.
5. Accepted order, owner cancellation, trade, and remainder-cancellation events
   are the only reservation transitions. Each trade reduces both source-order
   reservations by the paired quantity. After the complete uncross trace,
   executed buys and sells are accumulated in `u128` and netted once per
   account before checked conversion to the signed `i128` position. A permitted
   same-account buy/sell pair therefore has zero position effect without a
   transient directional update.
6. Structural validation recomputes every reservation valuation/notional,
   every account aggregate, all position bounds, constructor-owned hash
   headroom, quiescent uncross scratch, and one-to-one active-order parity.
   Submit/cancel reservation work is expected `O(1)` plus underlying book work;
   risk application for `T` trades and `C` remainder cancellations is
   `O(T + C)` expected time and uses a constructor-reserved account-netting map.
7. `CallAuctionRiskCheckpoint` binds the physical definition/profile prefix,
   embeds canonical auction direct state and complete command/report lineage,
   and stores account-ID-sorted profiles with redundant current exposures.
   Restore rebuilds reservations from active orders and compares every
   aggregate. Validation independently replays all commands through the
   coupled core-first risk gate; validation capacity includes historically
   risk-rejected submits because they still require core preparation.
8. The coupled checkpoint has a stable complete-value little-endian codec and
   direct restore under default or explicit limits. Snapshot version 4 assigns
   it `QSNP` kind `5`. `DurableCallAuctionRiskEngine` binds a canonical
   definition/profile prefix, persists command/report pairs, completes at most
   one dangling non-retry command, verifies risk-aware replay, and supports
   single-file or segmented A/B cutover. The plain
   `DurableCallAuctionEngine` below remains a distinct non-risk grammar.

## Durable call-auction invariants

1. WAL version 4 assigns stable record-kind tags `9` and `10` to one
   call-auction command and its complete execution report. All multibyte fields
   are little-endian, enum tags are explicit, and decoding reconstructs and
   validates domain values, contiguous event sequencing, report outcome/event
   grammar, clearing totals, trade identity, and uncross body counts.
2. An uncut auction journal contains one immutable instrument definition
   followed by strict command/report pairs. A compacted journal instead begins
   with one kind-`8` anchor bound to snapshot-version-4 call-auction kind `4`,
   followed by the same suffix grammar. A report without a command, consecutive
   commands, a second definition, an interior anchor, a
   continuous-matching/risk/ledger record, or any other kind fails recovery. At
   most one final command may lack its report after termination.
3. Runtime submission prepares the complete bounded engine transition before
   command append, appends the exact prepared command, consumes the same
   move-only token, and appends its report. Failure after command
   acknowledgement poisons that instance; deterministic reopen is the only
   supported continuation. The configured buffered/data-sync/full-sync policy
   defines acknowledgement strength.
4. Full-WAL recovery starts a fresh engine, submits every persisted command,
   and requires exact equality with its following report. Checkpoint recovery
   independently validates direct state against complete semantic history,
   rebuilds indexed phase/book/counter/retry state, and executes only the WAL
   suffix. Definition drift, deterministic or prefix divergence,
   ahead-of-WAL state, capacity/invariant failure, and a reproduced or persisted
   `replayed = true` report fail closed. A final dangling non-retry command is
   completed once and receives the reproduced report.
5. Exact retries are resolved from retained immutable history before storage
   mutation and append zero frames, including after reopen. A persisted retry,
   whether paired or dangling, is noncanonical and rejected. This makes the WAL
   a unique state-transition history rather than a request-attempt log.
6. A call-auction checkpoint retains the immutable definition and WAL origin,
   completed report boundary, phase/cycle, book revision, next priority/trade
   counters, canonical accepted IDs and active orders, and complete
   command/report history. Capture independently replays that history and
   requires exact checkpoint equality. Decode projects every event, including
   per-cycle pre-uncross remainder quantities, before direct reconstruction.
7. Uncut checkpoint recovery proves every physical frame through the checkpoint
   generation. Cutover synchronizes the inactive A/B slot, then publishes one
   exact kind/checksum/generation/slot anchor; recovery never guesses the other
   slot. Both single-file and version-2 segmented layouts preserve one global
   sequence and can complete one dangling suffix command after the anchor.
8. For `C` commands with `E` events and `O` active-order scale, checkpoint event
   projection has a conservative `O((C + E) log O)` bound before engine/book
   audit; complete idempotency history is intentionally retained. Cutover bounds
   WAL bytes scanned and avoids re-executing checkpointed commands, but does not
   bound snapshot size, capture pause, semantic-history lifetime, or
   allocator-failure exposure.

## Instrument invariants

1. Asset codes and instrument symbols are non-empty canonical uppercase ASCII
   in fixed-capacity representations; asset and instrument identifiers are
   non-zero.
2. Asset and price decimal scales are bounded to 18 digits.
3. A definition has distinct base and quote assets, positive settlement
   multipliers, a positive `i64`-representable tick, aligned collar endpoints,
   and positive aligned quantity bounds.
4. New order quantity must satisfy the inclusive entry minimum and maximum plus
   lot grid. Positive residual leaves after execution need not satisfy the entry
   minimum, but must remain no greater than the maximum and grid aligned.
5. Catalog asset identifiers and codes are unique.
6. Instrument versions and effective timestamps increase strictly; symbol,
   kind, base asset, and quote asset cannot change under one instrument ID.
7. Effective-time lookup is `O(log V)` over `V` versions and exact-version
   lookup rejects an absent version.
8. A matching WAL's first frame contains the complete definition; reopening
   requires structural equality with the requested definition.
9. The definition-correlated settlement path rejects a trade whose instrument
   ID or version differs before constructing or persisting ledger postings.

## Ledger invariants

1. Every financial entry has an effective date and at least two non-zero legs;
   an administrative period control has no effective date and zero legs.
2. A financial entry contains at most one leg per `(account, asset)` pair.
3. Financial posting amounts balance independently for every asset by comparing
   exact positive and negative magnitudes. Values through `u128::MAX` remain
   inline; larger totals spill into canonical little-endian `u64` limbs.
   Canonical posting order cannot cause a false intermediate overflow.
4. Validation and all checked balance calculations complete before commit.
5. Journal sequence, balances, idempotency index, and journal order change
   together after successful validation.
6. Posting order is canonicalized by `(asset, account)`.
7. Exact transaction retries do not change balances; differing content under
   the same transaction identifier is rejected.
8. Settlement transaction identifiers are globally supplied because book trade
   identifiers are only local to one instrument shard.
9. Preparation calculates every next balance without mutation and binds it to
   the current ledger generation.
10. Durable posting writes the canonical entry, correction, or batch before
    committing its prepared state; stale preparations cannot commit.
11. Recovery accepts only ledger-entry, ledger-correction, and ledger-batch
    records and reconstructs every balance, reversal link, period boundary, and
    last booking timestamp from the canonical WAL sequence.
12. A complete invariant audit cross-checks journal order and sequence,
    transaction index identity, deterministic entry replay, canonical balances,
    and independently accumulated exact positive/negative magnitudes per asset.
13. A checkpoint contains all ledger records plus a redundant, strictly
    `(asset, account)`-ordered image of non-zero balances. Its generation equals
    its record count. Decoding rejects exact duplicate records, partial
    corrections/batches, or transaction collisions while replaying every
    record before accepting that balance image.
14. Durable checkpoint publication follows a successful WAL `sync_all` barrier
    and a successful live-ledger invariant audit.
15. Checkpoint-assisted recovery accepts the checkpoint only when its complete
    record history equals the exact prefix of an uncut WAL, or when a compacted
    WAL anchor in either physical layout exactly binds its A/B slot, kind,
    semantic generation, physical sequence, payload length, and checksum. It
    then applies only the suffix and reruns the complete live-ledger audit.
16. Every entry carries an immutable lifecycle kind. A reversal names one
    preceding transaction and its postings must be the exact signed inverse of
    that target in the same canonical key order.
17. One entry can have at most one committed reversal. A reversal entry may
    itself be reversed once, creating an explicit append-only reinstatement
    chain rather than mutating or deleting prior history.
18. The reversal index changes atomically with balances, journal order, and the
    transaction index. Deterministic replay and checkpoint restoration
    reconstruct and cross-audit that index.
19. A correction contains exactly one target reversal followed by one standard
    replacement. Both transactions share one event sequence and one
    CRC-protected WAL frame. Admission calculates exact final balances from both
    posting sets without exposing or requiring a representable intermediate
    state.
20. A reconciliation statement is a complete non-zero balance image at one
    exact ledger generation. It has unique `(asset, account)` keys and
    independently equal arbitrary-magnitude positive/negative totals for every
    represented asset.
21. Reconciliation rejects a stale/future generation and an observation time
    preceding that generation's last journal event before comparison; it emits
    only non-zero `external - internal` differences in canonical order.
22. Every financial effective date must be later than the current inclusive
    `closed_through` boundary. Reversals carry their own effective date and do
    not bypass the fence.
23. `recorded_at` timestamps are nondecreasing in journal order; equality is
    valid. Exact transaction retries are resolved before timestamp and period-
    transition checks and cannot create a second effect.
24. A period close is a zero-posting journal event that strictly advances the
    inclusive boundary. A reopen is a zero-posting event that moves an existing
    boundary backward or removes it. Administrative controls cannot be reversed
    as financial postings.
25. Checkpoint replay and WAL-suffix replay apply the same timestamp, dated-
    posting, close, and reopen rules, then cross-audit the reconstructed fence.
26. A `LedgerBatch` contains at least two entries with distinct transaction
    identifiers and nondecreasing booking timestamps. Its vector order is
    authoritative; it is not sorted or inferred from identifiers.
27. Batch validation uses an overlay over the committed ledger. An earlier
    member's period transition, transaction, or reversal link is visible to
    later members; a later member is not visible to an earlier one. Any failed
    member leaves balances, indexes, lifecycle state, and event sequence
    unchanged.
28. For every affected `(account, asset)` key, batch admission computes the
    single final value `b' = b + Σδᵢ`. While both signs remain, the accumulator
    consumes a term opposite to its current sign; that addition cannot
    overflow. Once one sign remains, the accumulator moves monotonically toward
    `b'`. Therefore a checked overflow represents an unrepresentable final
    `i128`, not an artifact of member order.
29. Every batch member shares one ledger-event sequence. Exact replay requires
    equal entry content, the same sequence for every transaction, and the same
    ordered batch record. A subset committed separately, or the whole set
    committed under another grouping, is a nonmutating partial-commit error.
30. The complete batch occupies one bounded, CRC-protected WAL frame and one
    checkpoint record. Torn-tail repair and segmented rotation therefore retain
    every member or no member; replay never exposes a committed prefix of the
    batch.
31. `LedgerMagnitude` has no fixed numerical ceiling. Its inline `u128` state
    is allocation-free; overflow spills once into an exact limb vector and
    subsequent addition propagates carries without truncation. Trial balance,
    entry validation, reconciliation, replay audit, and unbalanced diagnostics
    use the same representation. Decimal rendering divides a diagnostic copy
    into base-10¹⁹ chunks and never changes authoritative state.

Signed balances are intentional accounting state. Credit limits, collateral,
and margin are not inferred by the ledger. The implemented order risk layer
consumes seeded positions and matching traces; it does not derive available
collateral from ledger balances.

## Pre-trade risk invariants

1. Each account has at most one immutable profile per risk-managed shard, with
   `Active`, `ReduceOnly`, or `Blocked` entry state. Cancellation bypasses entry
   limits after matching ownership and identity validation.
2. Core instrument and matching business checks precede risk checks; a core
   rejection cannot be replaced by a risk rejection.
3. Per-order quantity/notional, resting-order count, aggregate resting
   quantity/notional, and worst-case long/short position are independently
   bounded with checked integer arithmetic.
4. Incoming notional covers every reachable execution price: buy limits use
   the maximum absolute magnitude over `[collar minimum, limit]`, sell limits
   over `[limit, collar maximum]`, and market orders over the full collar.
   Units are raw price quanta multiplied by lots (`u128`).
5. IOC, FOK, and market commands cannot retain a reservation and therefore do
   not consume resting-order count/quantity/notional capacity. Their complete
   quantity still consumes worst-case position and per-order capacity.
6. Once an order rests, its reservation retains the conservative per-lot
   reachable-price magnitude used at admission. This can exceed
   `abs(resting price)` for a maker-only future fill, but it prevents aggregate
   open-notional utilization from dropping merely because the command crossed
   the acceptance boundary. Partial fills multiply the retained magnitude by
   current total leaves.
7. Worst-case long exposure is executed position plus all resting buy lots plus
   the incoming buy quantity; short exposure is the executed position minus
   equivalent sell exposure.
8. `ReduceOnly` permits only the side opposing a non-zero position and requires
   all reducing reservations plus the new quantity not to cross zero.
9. Matching traces release maker reservations on fills, all reservations on
   cancellation, old exposure before replacement, and prevented resting lots
   under decrement-and-cancel STP. Reserve risk and notional are based on total
   leaves; replenishing a displayed slice has no independent risk effect.
   Trades update buyer/seller positions once.
10. Single-order, mass, and accepted block-and-cancel account controls bypass
    numerical entry limits after instrument identity validation. Instrument
    trading-state control is account-independent and also bypasses numerical
    risk admission. Each cancelled order releases its complete total-
    leaves reservation exactly once before the completion summary is ignored by
    risk state.
11. Exact command retries do not apply risk state twice. Risk rejections are
    normal sequenced and durable rejection events.
12. Cross-audit recomputes account aggregates from reservations and verifies a
    one-to-one structural match with every active book order.
13. A durable risk shard binds the complete instrument definition followed by
    account-ID-sorted immutable profiles. Recovery completes only an exact
    metadata prefix before the first command. The supplied profile count must fit
    `RiskManagedLimits` before a WAL path is created or opened.
14. Construction and restoration fallibly reserve the complete
    `max_active_orders` risk-reservation index. Preparation is read-only even if
    profile registration occurs between split preparation and commit.
    Replacement and partial-decrement paths remove before reinsertion. At a full
    active-order bound, an admitted residual has already removed at least one
    maker reservation, so the constructor-owned capacity is reused. Validation
    rejects reservation cardinality or allocation capacity inconsistent with
    the configured maximum.
15. Construction fallibly reserves the complete registered-account profile
    index. Duplicate identity is checked before capacity, insertion cannot
    rehash, and registration becomes unavailable after the first sequenced
    command freezes the metadata set. Direct split preparation may register a
    missing profile before the first commit because no command has yet been
    sequenced; the constructor-owned reservation index already covers either
    authorization result. Checkpoint restoration rejects a selected profile
    bound below the canonical account count. Structural validation audits both
    configured cardinality and retained allocation capacity. `RiskHashIndex`
    telemetry exposes configured, allocated, and occupied counts for profiles
    and reservations.
16. `RiskManagedLimits` requires matching account-control capacity to be at
    least registered-profile capacity, so every profiled account can retain one
    never-evicted fence revision. Coupled risk rejects a control for an
    unprofiled account before matching mutation. An accepted control releases
    reservations from its ordinary cancellation trace; the final control event
    has no second exposure effect.

## Market-data publication invariants

1. Every non-replayed matching event maps to exactly one public update carrying
   the identical event sequence and timestamp; no private event creates a
   sequence hole.
2. Public updates contain instrument ID and immutable definition version. They
   contain no account, order, or command identifiers.
3. Events without a public depth, trade, or instrument-state effect emit `NoBookChange`; market-data version 2
   performs no conflation or sequence renumbering.
4. Level updates contain absolute post-event aggregate quantity and order count,
   not relative deltas. Both fields are zero only for level deletion.
5. Trade updates contain monotonic trade ID, signed execution price, positive
   quantity, aggressor side, and the absolute maker level after execution. A
   replica proves that aggregate maker quantity falls by exactly the printed
   quantity and that maker order count is unchanged or decreases by one.
6. The publisher tracks active order side, price, total leaves, displayed
   leaves, and display policy solely to translate private traces. It publishes
   only displayed leaves, removes a depleted slice from visible order count,
   restores that count on a separately sequenced reserve refresh, handles every
   STP policy, and removes old exposure before non-priority-retaining replacement.
7. Each mass-cancelled order produces the same absolute visible-level update as
   an individual cancel; the aggregate completion produces `NoBookChange`.
   Publisher validation proves account/scope membership, ascending order-ID
   trace order, and exact count/total agreement without exposing those private
   fields publicly.
8. Account-control cancellations use the same level transition as ordinary
   cancellation. The publisher privately mirrors prior/current fence state and
   revision, validates ordered cancellation aggregates and the final control
   event, emits `NoBookChange` for the completion, and cross-audits its control
   mirror against the authoritative book.
9. Instrument transition-and-cancel emits an absolute level update for each
   canonical cancellation, then a public trading-state update with prior state,
   current state, and revision. Publisher and replica require exact prior state
   and next revision; snapshots carry the same effective state boundary.
10. Exact command retries produce no second public update.
11. Publisher bootstrap from a live or WAL-recovered book captures all active
   orders, depth, final event sequence, and final trade ID, then cross-audits
   the private and public aggregates.
12. A full-depth snapshot contains occupied bids in descending price order and
   asks in ascending price order at one source sequence. Locked or crossed
   snapshots are invalid.
13. A replica rejects a missing, duplicated, or reordered sequence before
    mutating depth. A non-stale full-depth snapshot resets the recovery boundary.
14. Trace or structural failures after incremental mutation poison publisher or
    replica state; a fresh authoritative bootstrap/snapshot is required.
15. The stable complete-value schema is
    [Market-data payload format version 2](market-data-v2.md). Network framing,
    fanout, entitlement, and retransmission sessions are outside this boundary.

## Call-auction order-book and publication invariants

1. `CallAuctionBook::limit_depth` exposes anonymized occupied limit aggregates
   in best-to-worst order; market-constrained quantity and order count are
   separate side-specific values. Locked and crossed opposing limits are valid
   collection state.
2. Every non-replayed auction event maps to one public update at the identical
   source sequence and timestamp. Rejections preserve continuity through
   `NoPublicChange`; exact command retries publish no second update.
3. Public payloads exclude account, order, and command identity. An auction
   trade print contains cycle ID, monotonic trade ID, common price, and positive
   lot quantity.
4. Accepted, owner-cancelled, and uncross-remainder changes carry a positive
   anonymized quantity and one absolute market/limit aggregate. Replica state
   must reconcile quantity exactly and order count by one before mutation.
5. Each paired trade carries absolute buy and sell aggregate state. Both sides
   fall by exactly the print quantity; each affected order count is unchanged
   or decreases by one. Trade identity is contiguous across cycles.
6. The final uncross update must reconcile all preceding prints and remainder
   removals to one common clearing price, executable quantity, event counts,
   next book revision, and next phase revision before the replica closes the
   cycle.
7. Publisher bootstrap captures active private orders, aggregate depth,
   phase/cycle state, book revision, and command/event/trade boundaries from a
   live or recovered engine, then cross-audits them against the authoritative
   book.
8. Snapshots carry complete public collection state at one event/command
   boundary. A replica rejects stale images, invalid phase/cycle topology,
   wrong-side/noncanonical/empty levels, and definition-off-grid prices.
9. Missing or reordered sequences fail before mutation. A later structural
   error poisons publisher or replica state until authoritative reconstruction
   or a non-stale valid snapshot.
10. The stable schema is [call-auction market-data payload version 1](auction-market-data-v1.md).
    Indicative state is intentionally absent because reference, band, and price
    policy remain explicit external inputs. Network distribution remains
    outside this boundary.

## Journal and recovery invariants

1. Every frame carries `QWAL` magic, format version, typed record kind, bounded
   payload length, CRC-32C, and a contiguous global sequence.
2. CRC-32C covers the complete header with its checksum field zeroed plus the
   payload.
3. Payload allocation occurs only after the declared length is checked against
   the configured maximum and physical file length.
4. Repair mode truncates only a physically incomplete final frame.
5. Invalid magic, unsupported version, unknown kind, checksum mismatch, and
   sequence discontinuity are non-repairable corruption.
6. An ambiguous write or durability-barrier failure poisons the writer until
   reopen and recovery.
7. A `JournalBatch` uses one write and barrier across multiple frames but is not
   one transactional frame; recovery may retain its verified frame prefix. A
   ledger correction or `LedgerBatch` instead uses one typed frame, so recovery
   retains every contained entry or none.
8. Typed codecs reject invalid identifiers, quantities, enum tags, booleans,
   lengths, trailing bytes, noncanonical postings, and contradictory reports.
9. A plain matching journal begins with one complete instrument definition. A
   risk-managed journal then contains a canonical account-profile set before
   the first command. Metadata after command processing is invalid grammar.
10. Durable matching writes a command before state mutation and writes the
    reproduced report afterward.
11. Recovery accepts only bound metadata followed by alternating
    command/report records, byte-equivalent deterministic matching/risk
    outcomes, and at most one final command without a report.
12. A writer canonicalizes the WAL path and atomically creates a versioned
    sidecar lease before scanning or mutation. A second canonical-path writer
    fails closed; read-only snapshot readers remain available.
13. Lease and newly created WAL directory entries are synchronized through the
    parent directory. Clean `close` synchronizes WAL data/metadata before
    releasing and synchronizing the lease removal.
14. Abandoned lease removal requires the exact owner identity previously
    observed from `WriterLeaseHeld`; process liveness and a quiesced recovery
    window are external preconditions. Malformed leases require a separate
    explicit recovery operation and are never reclaimed during normal open.
15. The default append acknowledgement is `SyncAll`. Partial writes, failed
    barriers, and failed explicit synchronization poison the writer; injected
    failures verify complete-prefix recovery and ambiguous complete-frame replay.
16. A segmented directory has one CRC-32C-protected version-2 `QSEG` marker. It
    binds immutable capacity, lineage origin, and payload limit and selects one
    active physical generation plus its first retained sequence. One canonical
    manager lease excludes other managers and raw member-file writers.
17. Rotation completes encoding, capacity, length, and sequence-space preflight
    before closing the active file. A frame or batch is placed wholly in one
    segment; every canonical filename includes its marker generation and first
    global sequence.
18. Every closed segment is nonempty and scanned strictly. Only the final
    segment can be empty or can repair a physically incomplete tail; no closed
    corruption is repaired or skipped.
19. Interruption between next-segment creation and append can leave one empty
    final file. Reopen validates its expected start sequence and reuses it.
20. Matching, risk, and ledger segmented recovery streams one segment at a time
    while applying the same logical record grammars as single-file recovery.
21. Explicit incomplete-initialization recovery removes an invalid `QSEG`
    marker only under manager ownership and only when no segment or unknown
    persistent entry exists. Normal cutover changes only its checksummed active
    generation and first-retained-sequence selector after the new generation is
    synchronized.
22. Matching, coupled risk, ledger, and call-auction cutover publishes an inactive A/B
    checkpoint slot before publishing one synchronized version-4 anchor frame.
    Single-file storage atomically renames that frame over the WAL. Segmented
    storage first synchronizes a next-generation anchor segment, then atomically
    replaces and directory-synchronizes the `QSEG` selector. The active slot is
    never overwritten before the physical layout selects its successor.
23. A compacted WAL cannot open without its checkpoint base. Recovery derives
    the anchor-selected slot and never guesses the alternate slot. Abandoned
    single-file pre-rename staging is discarded only through an explicit newly
    leased recovery operation after prior-writer liveness is disproved.
    Segmented readers ignore non-selected generations and deterministic staging;
    a manager validates the complete selected generation before removing them.

## Semantic snapshot invariants

1. A version-4 `QSNP` file carries a fixed 28 B header with magic, typed payload
   kind (`1` ledger, `2` matching, `3` coupled risk/matching, `4` call auction,
   `5` coupled call-auction risk),
   bounded `u64` length, CRC-32C, and semantic generation.
2. CRC-32C covers the zero-checksum header and complete payload. Physical and
   declared lengths, typed kind, codec invariants, and header/payload generation
   must all agree before a value is returned.
3. Snapshot writers use canonical-path `QLCK` ownership. Targets inside a
   marker-bound segmented-WAL directory are rejected before mutation.
4. Replacement exclusively creates and synchronizes `<target>.pending`, renames
   it over the target, synchronizes the parent directory, and releases the
   lease. An existing pending file always requires explicit recovery.
5. A normal write cannot regress generation, replace equal-generation state
   with different content, or advance to a history that does not extend the
   current exact ledger-record, matching-command/report, immutable-profile-
   bound coupled-risk, or call-auction command/report lineage.
6. Pending recovery promotes only an absent-current or newer same-lineage
   value. It discards a stale value only when the current history extends it,
   and preserves both sides on equal-generation or cross-lineage divergence.
7. Truncated or provably corrupt pending content is removable explicitly.
   Unsupported versions/kinds and values exceeding the caller's configured
   bound are preserved for a compatible recovery process.
8. CRC-32C is an accidental-corruption detector, not an authenticity proof.
9. Ledger, matching, coupled risk, and call-auction checkpoints retain complete
   semantic history. Uncut recovery scans the complete WAL to prove the prefix.
   Version-4 cutover in either physical layout replaces that prefix with an
   anchor bound to one version-4 A/B snapshot slot, so reopen scans only the
   anchor and suffix. This does not bound checkpoint memory, capture pause,
   retained idempotency/audit history, or semantic shard-generation lifetime.
10. Matching capture independently replays complete command/report history and
    requires exact live-state equality before publication. Recovery reconstructs
    FIFO/reserve/STP state and exact-retry caches directly, then applies only the
    suffix after a completed-report WAL boundary.
11. Coupled risk capture binds the WAL origin, final profile-metadata sequence,
    definition, and canonical immutable profile set. It reconstructs one total-
    leaves reservation per active private order, compares redundant account
    exposures, and independently replays all commands through the risk/matching
    state machine before publication. Recovery applies state transitions only
    after the checkpoint generation.
12. Call-auction capture binds the WAL origin and definition and retains phase,
    cycle, collection revision, accepted IDs, active orders, priority/trade
    counters, and exact retry history. Independent replay must reproduce the
    direct image. Restore rebuilds AVL/FIFO state and cached reports directly,
    then applies only suffix commands.
13. Coupled call-auction/risk capture additionally binds the canonical immutable
    profile prefix and redundant account positions/exposures. Restore rebuilds
    reservations from active auction orders; independent replay must reproduce
    every accepted and risk-rejected report. Kind-`5` uncut recovery proves the
    definition, all profile frames, and every command/report prefix frame;
    anchored recovery binds the exact A/B slot before applying only the suffix.

The authoritative persisted framing and payload schemas are
[WAL format version 4](wal-v4.md) and
[Semantic snapshot format version 4](snapshot-v4.md). Filesystem and device
assumptions are bounded by the [Local storage contract](storage.md).

## Failure model

Business rejections are sequenced trace events. Identifier exhaustion and
idempotency collisions are operational errors. Arithmetic uses checked
operations. Matching state, risk reservations/positions, and ledger balances
can be reconstructed from verified local WALs. Public depth can bootstrap from
that recovered matching state; consumers repair an incremental gap with a
newer full-depth snapshot. Forced-process-termination, concurrent-writer,
abandoned/malformed-lease, injected-write/barrier, exact-boundary/batch rotation,
closed-segment corruption, active-tail repair, cross-segment replay, torn-report,
metadata-prefix, replay-divergence, entry-reconstruction, feed-gap, and publisher
cross-audit tests exercise these paths. Reserve tests additionally cover
admission bounds, FIFO-tail refresh, repeated slices in one match, hidden-aware
FOK, STP, total-leaves risk, displayed-only publication, and WAL recovery.
Mass-cancel tests cover empty and side-scoped selection, canonical audit order,
large-book sparse-account selection, intrusive-link continuity across reserve refresh,
replacement and execution, hidden-total risk release, displayed-depth
publication, malformed summaries, exact replay, and WAL reconstruction.
Account-control tests cover stale/exhausted revisions, exact retry, atomic
canonical cancellation, admission fencing, re-enable, protected-history use,
constructor capacity stability/exhaustion, unprofiled risk rejection,
reservation release, market-data validation, direct and durable checkpoint
restoration, interrupted-report completion, and version-1 artifact rejection.
Invariant tests additionally inject an account-link break and require the
independent ownership/side/head/tail/count/cycle/bidirectional audit to reject it.
Best-level index tests cover bid/ask extrema, better/worse insertion, non-best
and repeated best deletion, deliberate cached-price and cached-aggregate
corruption, repricing, full execution, STP removal, empty-side transitions, and
direct checkpoint reconstruction.
Indexed-AVL tests cover all four rotation shapes, forward/reverse traversal,
leaf/one-child/two-child deletion, slot reuse without capacity growth,
topology-independent equality, tree/free-list corruption, unrepresentable arena
reservation, and 20,000 generated operations differentially checked against
`BTreeMap` after every mutation.
FOK tests cover hidden total-leaves eligibility, same-price self barriers,
cancel-resting across self orders, better-price reserve exhaustion before a
worse self barrier, all supported FOK STP policies, and allocation-free/model
equivalence against literal FIFO-tail reserve requeue across 20,000 generated
books.
Auction tests cover full and bounded tick-grid interval discovery, market-only
and mixed market/limit interest, outside-band levels, reference clamping,
unquoted clearing prices, negative and signed-extreme prices,
pressure/reference/final tie policies, malformed grids/bands and aggregate
overflow, and two independent 20,000-case generated suites against exhaustive
enumeration. Order-level allocation tests cover market, price,
class, time and ID priority; ineligible tails; partial fills; exact aggregate
reconciliation; operational limits; totals above `u64::MAX`; and 20,000
generated plans against a literal priority walk.
Capacity tests cover invalid policies, every active/account/control/side-level/identity
boundary, ordinary-history exhaustion, invalid-control reserve protection,
valid individual and mass cancellation, exact retry at exhaustion, released
level accounting during replace, insufficient/sufficient checkpoint limits,
pre-WAL rejection, durable recovery, and post-recovery retry.
Matching checkpoint tests cover capture-time replay audit, FIFO-tail reserve
state, resting STP, exact retry, stable kind/codec, semantic corruption,
non-default WAL origins, lineage forks, WAL-prefix divergence, ahead-of-WAL
rejection, path aliasing, and single/segmented suffix replay. Ledger snapshot
framing, generation and lineage divergence, interrupted-pending recovery,
independent trial balance,
checkpoint/WAL prefix proof, segmented suffix replay, reversal lineage and
reinstatement, indivisible correction replay and torn-tail repair, correction
arithmetic boundaries, generalized multi-entry netting, partial-group and
collision rejection, ordered in-batch period/reversal transitions, stale
preparation, single/segmented WAL grouping, batch torn-tail repair, batch
checkpoint-prefix/suffix recovery, invalid reversal recovery, exact-generation
external balance reconciliation, exact side totals crossing `u128`, wide
unbalanced diagnostics, large-total checkpoint replay, dated-entry fences,
temporal regression, period close/reopen, and checkpoint-plus-WAL period
reconstruction are also tested.
Coupled risk-checkpoint tests cover sequenced risk rejection, executed position,
hidden total-leaves reservation, exact retry, malformed owner/exposure state,
profile and same-generation lineage drift, non-default WAL origins, exact WAL-
prefix proof, ahead-of-WAL rejection, path aliasing, and single/segmented suffix
replay. Single-file cutover tests cover A/B alternation, anchor binding,
non-default physical origins, suffix continuation, corrupt/wrong slots,
abandoned staging, and a failed post-rename directory barrier. There is no
claim of replicated durability, remote consensus, segmented retention,
checkpoint-memory-bounded restart, durable external-statement anchoring, or
qualified storage-device power-loss behavior. Call-auction checkpoint tests
cover stable kind/codec framing, direct restore, multi-cycle retained remainder
projection, exact uncut prefix proof, ahead-of-WAL rejection, suffix-only
replay, retry suppression, path aliases, single/segmented cutover, A/B
alternation, corrupt/wrong slots, and dangling suffix completion. There is no
additional claim that semantic checkpoint history is size bounded.

## Standards and primary-source provenance

- CRC-32C uses the Castagnoli generator and complemented input/output procedure
  specified for CRC32C in [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
  Quotick applies that checksum to its own WAL framing; it does not claim iSCSI
  protocol compatibility.
- Writer leases use Rust's atomic, fail-if-present
  [`OpenOptions::create_new`](https://doc.rust-lang.org/stable/std/fs/struct.OpenOptions.html#method.create_new).
  WAL and directory barriers use [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all);
  the underlying transfer remains conditional on the implementation as specified
  by [POSIX `fsync`](https://pubs.opengroup.org/onlinepubs/009695399/functions/fsync.html).
- Snapshot replacement uses Rust
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html) and
  depends on the atomic same-filesystem namespace replacement specified by
  [POSIX.1-2024 `rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).
- The signed `Price` domain covers real exchange cases in which negative prices
  are supported. [CME Clearing Advisory 20-152](https://www.cmegroup.com/notices/clearing/2020/04/Chadv20-152.pdf)
  is the primary-source basis for retaining negative futures-price support.
- Native reserve behavior is venue-specific. CME documents `DisplayQty` as the
  maximum visible portion, repeated replenishment of hidden quantity, stable
  native-iceberg `OrderID`, and potentially changed priority on refresh in the
  [CME Globex Reference Guide](https://www.cmegroup.com/content/dam/cmegroup/globex/files/GlobexRefGd.pdf)
  and [CME Market by Order FAQ](https://www.cmegroup.com/articles/faqs/market-by-order-mbo.html).
  Nasdaq Fixed Income likewise specifies peak replenishment and a new timestamp
  on refresh in its
  [Fusion Fixed Income Market Model](https://www.nasdaq.com/docs/2026/03/04/Market-Model-Fusion-Fixed%20Income-1.1-March-2-2026.pdf).
  Quotick's stable private order ID plus FIFO-tail refresh is its explicit
  instrument-shard contract, not a claim of universal venue equivalence.
- FIX defines `OrderMassCancelRequest(35=q)` as a separately identified request
  to cancel remaining quantity for an order group and permits an optional side
  qualifier in the
  [FIX Trading Community trade specification](https://www.fixtrading.org/online-specification/business-area-trade/).
  CME certifies instrument-scoped mass action and requires an audit-trail line
  per confirmed cancelled order in its
  [iLink mass-cancel audit test](https://www.cmegroup.com/tools-information/webhelp/autocert-audit-trail-ilink3/Content/moc.html).
  Quotick maps that pattern to one account in one instrument-version shard;
  cross-shard and delegated scopes are not inferred.
- FIX `TradingSessionStatus(35=h)` separately identifies market/session scope
  and carries `TradSesStatus(340)` as the state of a trading session in the
  [FIX 5.0 SP2 message definition](https://fiximate.fixtrading.org/legacy/en/FIX.5.0SP2/body_5249104.html).
  Quotick's control is instead one instrument-shard state plus revision: it has
  no session identifier, calendar, trading method/mode, event reason, or FIX
  compatibility claim. Those omissions bound A59 and prevent state labels from
  being interpreted as auction/session scheduling.
- Nasdaq Equity 4 Rule 4754(b)(2) specifies a Closing Cross price hierarchy
  beginning with maximum executable shares, then minimum imbalance, followed by
  venue-specific remaining-interest, midpoint-distance, benchmark, and
  post-only adjustments, including benchmark thresholds in paragraph (E).
  Rule 4754(b)(3) then defines separate MOC, displayed,
  and non-displayed allocation categories with category-specific price/time
  priority in the
  [official Nasdaq rulebook](https://listingcenter.nasdaq.com/rulebook/nasdaq/rules/nasdaq-equity-4).
  Quotick's venue-neutral analytical kernel makes maximum quantity, minimum
  absolute imbalance, optional pressure, reference distance, an explicit static
  candidate band, aggregate market interest, final price direction, and one
  market/price/class/time/ID allocation rule explicit. It does not claim Nasdaq
  conformance: order eligibility, entered-price constraints, exact priority
  categories, benchmark calculation and updates, collars, post-only
  adjustments, counterparty pairing, and uncross execution remain outside this
  kernel. `CallAuctionBook` adds an internal process-local priority pairing and
  remainder-policy transition; it does not make those semantics Nasdaq
  conformant.
- Gregorian calendar strings used at service boundaries follow
  [ISO 8601-1:2019](https://www.iso.org/standard/70907.html). The internal
  `AccountingDate` wire scalar is signed days relative to 1970-01-01 and does
  not itself implement calendar, time-zone, or business-day policy.
- All other field order, admission, accounting, sequencing, and recovery rules
  in this repository are Quotick's explicitly specified internal contracts,
  verified by the referenced test suites rather than attributed to an external
  standard.

## Required production increments

| Impact | Capability | Evidence required for completion |
|---|---|---|
| High | Durable storage completion | externally coordinated retired-generation archival/handoff; kernel inode locking or qualified alias exclusion; forced-power-loss filesystem/device evidence |
| High | Ledger lifecycle completion | controller authorization; versioned calendar ingestion; durable external-statement evidence; externally anchored cutoff proofs; allocation/fee/settlement workflow adapters over atomic batches |
| High | Snapshots and compaction | single-file and segmented matching/risk/ledger/call-auction WAL cutover are implemented; remaining evidence is bounded checkpoint memory/capture pause, semantic generation rollover, and externally retained audit/idempotency proofs |
| High | Replication and failover | deterministic leader change; duplicate/lost-command fault injection; recovery-point objective evidence |
| High | Portfolio/collateral risk expansion | cross-instrument netting, currency conversion, margin models, ledger-backed availability, scenario stress, and replicated reservation ownership |
| High | Matching lifecycle expansion | basic revisioned instrument state changes, bounded crossed call-auction collection, aggregate depth queries, banded discovery with market interest, pure order-level price-time allocation, deterministic pairing/atomic uncross, sequenced auction phase/idempotency, live and durable risk reservations/positions, stable auction/private-public wire schemas, gap-repair snapshots, semantic engine checkpoints, and plain/coupled-risk full-WAL plus single/segmented cutover recovery are implemented; remaining work is reference and dynamic-band derivation, reserve/display and venue priority/allocation policies, preventive self-trade policies, ledger integration, calendar/session scheduling, volatility triggers and interruption auctions, stop, pegged, discretionary, day/GTD, venue-specific amendment/uncross/publication semantics, and authenticated market-data transport; cross-instrument/multi-leg execution with atomic ownership and replay proofs |
| High | Instrument lifecycle expansion | trading calendars, session transitions, corporate actions, derivative expiry/exercise, and external symbology mappings |
| High | Venue reserve-order conformance | per-venue refresh priority, modification rules, public feed mapping, session persistence, mass-cancel behavior, and certified protocol fixtures |
| High | Coordinated multi-shard kill controls | local revisioned account fence and atomic cancellation are implemented; remaining evidence is authenticated firm/session/account ownership, cross-shard fanout, completion aggregation, and cancel-on-behalf audit export |
| High | Clearing lifecycle | novation/allocation, fees, settlement dates, fails, corrections, busts, and reconciliation |
| High | Security boundary | authenticated principals, authorization policy, secret management, audit export, and abuse controls |
| Medium | Gateways and schemas | versioned binary protocol, FIX adapter, backpressure, session recovery, and conformance fixtures |
| Medium | Market-data distribution | authenticated transport framing, entitlement, fanout, retransmission sessions, bandwidth control, and conformance fixtures |
| Medium | Operations | metrics, traces, structured logs, health, capacity limits, alert rules, and runbooks |
| Medium | Performance evidence | pinned-hardware benchmarks, allocation counts, tail latency, saturation, and regression thresholds |
| Medium | Verification expansion | model-based/property tests, fuzzing, crash simulation, concurrency model checking, and long soak tests |
| Low | User interfaces | administrative, trader, surveillance, reporting, and visualization surfaces after authoritative APIs stabilize |
