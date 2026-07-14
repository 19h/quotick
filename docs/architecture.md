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
22. Every active order appears exactly once in the ordered account/side index.
    Reserve FIFO-tail refresh preserves that membership without index churn.
    Mass-cancel preparation determines `K` and fallibly reserves capacity for
    `K` canonical IDs and `K + 1` events before durable command append or book
    mutation. Commit then detaches the selected side or account index atomically.
    Neither vector grows during cancellation; execution traverses only selected
    order IDs in ascending order and never scans unrelated active orders.
23. Each side caches its complete best `PriceLevel`. The cached price, FIFO
    head/tail, displayed-lot sum, and visible order count equal the corresponding
    extremal entry in the authoritative ordered price map. Every level aggregate
    mutation refreshes the cache when it targets the current best; deletion of
    the best recomputes the new extremum before control returns to matching.
24. FOK inspection never mutates state or materializes reserve slices. At a
    crossed price without self liquidity, every external total leaf is eligible.
    Cancel-resting excludes self orders and retains all external total leaves.
    For cancel-aggressor and cancel-both, the first self order is a FIFO barrier:
    only current displayed slices of earlier orders precede it because refreshed
    slices rejoin at the tail. The scan visits each inspected active order at
    most once and uses constant auxiliary space.
25. Every book has finite validated maxima for active orders, active accounts,
    occupied prices per side, accepted order IDs, retained commands, and events
    per execution report. Active accounts and per-side levels cannot exceed
    active-order capacity; accepted identity and ordinary history can establish
    every maximum active order. Report capacity is at least
    `max_active_orders + 1`, so one maximum-size mass cancellation always fits.
26. The tail of retained-command capacity reserves at least one slot per maximum
    active order. Once ordinary history fills, new and replace commands stop.
    Only a cancel or mass-cancel that passes current core business validation may
    enter the reserve; malformed, unknown, wrong-owner, or wrong-instrument
    controls cannot consume it. Exact cached retries remain available even at
    total exhaustion.
27. Capacity checks, required matching hash-table `try_reserve` calls, and
    fallible event/mass-cancel vector reservations precede the first matching
    mutation. `PreparedCommand` owns the unique empty vectors; mutable
    preparation may increase hash capacity but changes no semantic state.
    Durable wrappers run the same preflight before appending a command frame, so
    these reservation failures are unsequenced operational results and cannot
    leave a dangling WAL command. Ordered price/account tree nodes remain outside
    this recoverable boundary.
28. Checkpoint restoration rejects current cardinalities or retained report
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
31. Every completed report owns an immutable `EventTrace`. The first response,
    retained idempotency entry, exact retries, and in-memory checkpoint copies
    share one `Arc<Vec<Event>>` owner and its event buffer; cloning is `O(1)`.
    Preparation allocates the Arc control block and fallibly reserves the
    complete safe vector buffer before durable append or the first transition.
    Builder finalization moves the owner in `O(1)` without allocation or event
    copy. Explicit diagnostic mutation is
    copy-on-write and cannot modify cached history. Equality, validation, and
    encoding depend only on ordered event values, so pointer identity and spare
    vector capacity never enter replay or wire semantics.
32. Every resting order contributes mutation-maintained future event work. A
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

The book wraps each `BTreeMap<Price, PriceLevel>` in a side-aware index with a
mutation-maintained complete best-level cache. This provides `O(1)` best-price,
best-FIFO-head, and best-snapshot discovery while preserving deterministic
ordered traversal. It uses `HashMap<OrderId, RestingOrder>` for direct lookup,
`HashMap<AccountId, AccountOrderIndex>` containing side-specific
`BTreeSet<OrderId>` values for canonical account selection, and doubly linked
order identifiers for FIFO removal without scanning a level or book.
`OrderBookLimits` bounds all monotonic and active matching indexes. Optional
construction-time hash reservation covers active orders, active accounts,
accepted IDs, and retained reports; ordered price/account trees remain bounded
but allocate nodes on demand. Fallible construction and durable recovery use
`try_reserve` and report the first failed hash resource before state exists.
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

## Instrument invariants

1. Asset codes and instrument symbols are non-empty canonical uppercase ASCII
   in fixed-capacity representations; asset and instrument identifiers are
   non-zero.
2. Asset and price decimal scales are bounded to 18 digits.
3. A definition has distinct base and quote assets, positive settlement
   multipliers, a positive `i64`-representable tick, aligned collar endpoints,
   and positive aligned quantity bounds.
4. Catalog asset identifiers and codes are unique.
5. Instrument versions and effective timestamps increase strictly; symbol,
   kind, base asset, and quote asset cannot change under one instrument ID.
6. Effective-time lookup is `O(log V)` over `V` versions and exact-version
   lookup rejects an absent version.
7. A matching WAL's first frame contains the complete definition; reopening
   requires structural equality with the requested definition.
8. The definition-correlated settlement path rejects a trade whose instrument
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
    record history equals the exact WAL prefix. It then applies only the suffix
    and reruns the complete live-ledger audit.
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
6. Once an order rests, its maker execution price is exact, so retained
   notional is `abs(resting price) × leaves lots`.
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
10. Single-order and mass cancellation bypass entry limits after instrument
    identity validation. Each mass-cancelled order releases its complete total-
    leaves reservation exactly once before the completion summary is ignored by
    risk state.
11. Exact command retries do not apply risk state twice. Risk rejections are
    normal sequenced and durable rejection events.
12. Cross-audit recomputes account aggregates from reservations and verifies a
    one-to-one structural match with every active book order.
13. A durable risk shard binds the complete instrument definition followed by
    account-ID-sorted immutable profiles. Recovery completes only an exact
    metadata prefix before the first command.

## Market-data publication invariants

1. Every non-replayed matching event maps to exactly one public update carrying
   the identical event sequence and timestamp; no private event creates a
   sequence hole.
2. Public updates contain instrument ID and immutable definition version. They
   contain no account, order, or command identifiers.
3. Events without a public depth or trade effect emit `NoBookChange`; version 1
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
8. Exact command retries produce no second public update.
9. Publisher bootstrap from a live or WAL-recovered book captures all active
   orders, depth, final event sequence, and final trade ID, then cross-audits
   the private and public aggregates.
10. A full-depth snapshot contains occupied bids in descending price order and
   asks in ascending price order at one source sequence. Locked or crossed
   snapshots are invalid.
11. A replica rejects a missing, duplicated, or reordered sequence before
    mutating depth. A non-stale full-depth snapshot resets the recovery boundary.
12. Trace or structural failures after incremental mutation poison publisher or
    replica state; a fresh authoritative bootstrap/snapshot is required.
13. The stable complete-value schema is
    [Market-data payload format version 1](market-data-v1.md). Network framing,
    fanout, entitlement, and retransmission sessions are outside this boundary.

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
16. A segmented directory has one versioned `QSEG` marker binding capacity,
    initial sequence, and payload limit; one canonical manager lease excludes
    other managers and raw member-file writers.
17. Rotation completes encoding, capacity, length, and sequence-space preflight
    before closing the active file. A frame or batch is placed wholly in one
    segment and names the next segment by its first global sequence.
18. Every closed segment is nonempty and scanned strictly. Only the final
    segment can be empty or can repair a physically incomplete tail; no closed
    corruption is repaired or skipped.
19. Interruption between next-segment creation and append can leave one empty
    final file. Reopen validates its expected start sequence and reuses it.
20. Matching, risk, and ledger segmented recovery streams one segment at a time
    while applying the same logical record grammars as single-file recovery.
21. Explicit incomplete-initialization recovery removes an invalid `QSEG`
    marker only under manager ownership and only when no segment or unknown
    persistent entry exists; a valid marker is immutable.

## Semantic snapshot invariants

1. A `QSNP` file carries a fixed 28 B header with magic, version, typed payload
   kind (`1` ledger, `2` matching, `3` coupled risk/matching), bounded `u64`
   length, CRC-32C, and semantic generation.
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
   current exact ledger-record, matching-command/report, or immutable-profile-
   bound coupled risk lineage.
6. Pending recovery promotes only an absent-current or newer same-lineage
   value. It discards a stale value only when the current history extends it,
   and preserves both sides on equal-generation or cross-lineage divergence.
7. Truncated or provably corrupt pending content is removable explicitly.
   Unsupported versions/kinds and values exceeding the caller's configured
   bound are preserved for a compatible recovery process.
8. CRC-32C is an accidental-corruption detector, not an authenticity proof.
9. Ledger, matching, and coupled risk checkpoints retain complete history, and
   durable recovery still scans the complete WAL to prove the prefix. Version 1
   performs no WAL cutover, compaction, retention, bounded-memory, or bounded-
   restart protocol.
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

The authoritative version-1 framing and payload schema is
[WAL format version 1](wal-v1.md) and
[Semantic snapshot format version 1](snapshot-v1.md). Filesystem and device
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
large-book sparse-account selection, index continuity across reserve refresh,
replacement and execution, hidden-total risk release, displayed-depth
publication, malformed summaries, exact replay, and WAL reconstruction.
Best-level index tests cover bid/ask extrema, better/worse insertion, non-best
and repeated best deletion, deliberate cached-price and cached-aggregate
corruption, repricing, full execution, STP removal, empty-side transitions, and
direct checkpoint reconstruction.
FOK tests cover hidden total-leaves eligibility, same-price self barriers,
cancel-resting across self orders, better-price reserve exhaustion before a
worse self barrier, all supported FOK STP policies, and allocation-free/model
equivalence against literal FIFO-tail reserve requeue across 20,000 generated
books.
Capacity tests cover invalid policies, every active/account/side-level/identity
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
replay. There is no claim of replicated durability, remote consensus, WAL
cutover or retention, bounded restart, durable external-statement anchoring, or
qualified storage-device power-loss behavior.

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
| High | Durable storage completion | segment retention/archival with deletion fencing; kernel inode locking or qualified alias exclusion; forced-power-loss filesystem/device evidence |
| High | Ledger lifecycle completion | controller authorization; versioned calendar ingestion; durable external-statement evidence; externally anchored cutoff proofs; allocation/fee/settlement workflow adapters over atomic batches |
| High | Snapshots and compaction | fenced matching/risk/ledger WAL cutover, bounded restart time/memory, segment retention, and externally retained audit/idempotency proofs |
| High | Replication and failover | deterministic leader change; duplicate/lost-command fault injection; recovery-point objective evidence |
| High | Portfolio/collateral risk expansion | cross-instrument netting, currency conversion, margin models, ledger-backed availability, scenario stress, and replicated reservation ownership |
| High | Instrument lifecycle expansion | trading calendars, session transitions, corporate actions, derivative expiry/exercise, and external symbology mappings |
| High | Venue reserve-order conformance | per-venue refresh priority, modification rules, public feed mapping, session persistence, mass-cancel behavior, and certified protocol fixtures |
| High | Coordinated kill controls | authenticated firm/session/account ownership; atomic admission fence; cross-shard fanout; completion aggregation; cancel-on-behalf audit evidence |
| High | Clearing lifecycle | novation/allocation, fees, settlement dates, fails, corrections, busts, and reconciliation |
| High | Security boundary | authenticated principals, authorization policy, secret management, audit export, and abuse controls |
| Medium | Gateways and schemas | versioned binary protocol, FIX adapter, backpressure, session recovery, and conformance fixtures |
| Medium | Market-data distribution | authenticated transport framing, entitlement, fanout, retransmission sessions, bandwidth control, and conformance fixtures |
| Medium | Operations | metrics, traces, structured logs, health, capacity limits, alert rules, and runbooks |
| Medium | Performance evidence | pinned-hardware benchmarks, allocation counts, tail latency, saturation, and regression thresholds |
| Medium | Verification expansion | model-based/property tests, fuzzing, crash simulation, concurrency model checking, and long soak tests |
| Low | User interfaces | administrative, trader, surveillance, reporting, and visualization surfaces after authoritative APIs stabilize |
