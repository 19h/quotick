# Assumption Register

Each dependent result is valid only while its tagged assumptions survive the
listed falsification probe.

The register holds one section per assumption. Each section states what is
assumed (**Assumption**), which results depend on it (**Dependent results**),
and the stress test that would refute it (**Falsification probe**). The
identifiers A1-A152 are stable and are referenced from code comments and other
documents.

## A1 — instrument definition authority

**Assumption.** The supplied immutable instrument definition is the
authoritative and externally correct interpretation of each raw price quantum,
tick, collar, lot increment, size bound, and trading state. Internal
consistency is validated; correctness against an exchange or legal
specification is external.

**Dependent results.** Admission, price priority, crossing, and replay.

**Falsification probe.** Import independently sourced conformance fixtures for
every rule regime and boundary. Any accepted off-grid/out-of-collar order or
disagreement with the source specification falsifies A1.

## A2 — lot quantities and unit conversions

**Assumption.** `Quantity` is lots; the exact execution version's
`base_units_per_lot` and `quote_units_per_price_unit` are authoritative
economic conversions.

**Dependent results.** Level depth, base delivery, and quote notional.

**Falsification probe.** Replay minimum, maximum, negative-price, and
overflow-boundary trades across multiplier changes; independently calculate all
`i128` postings. Any accepted version mismatch or numerical disagreement
falsifies A2.

## A3 — single-writer shards

**Assumption.** Exactly one thread mutates an `OrderBook`; parallelism is
across instrument shards.

**Dependent results.** Deterministic FIFO, event order, absence of locks on the
hot path.

**Falsification probe.** Attempt concurrent mutation behind the final
dispatcher and run concurrency model checking. More than one accepted writer
for a shard falsifies A3.

## A4 — identifier uniqueness scopes

**Assumption.** `CommandId` is unique within a book, `OrderId` is never reused
after acceptance whether the order is resting or dormant, `TradeId` is book-
local, and `TransactionId` is globally unique.

**Dependent results.** Idempotency, trace correlation, exactly-once ledger
effect.

**Falsification probe.** Inject exact retries and differing-content collisions
across shards and restarts. Any second state transition for the same scoped
identifier falsifies A4.

## A5 — receive timestamps and arrival priority

**Assumption.** Receive timestamps are trace data, not matching-priority
inputs; command arrival at the single writer defines priority. For continuous
GTD controls, the supplied timestamp is also the authoritative temporal fence:
an admitted deadline is later than intake, and an expiry-sweep horizon is no
later than its command timestamp. The matching engine reads no wall clock.
Stop-reference sweeps carry a separately authoritative price value; neither
receive time nor a locally produced trade implicitly changes it.

**Dependent results.** FIFO determinism, replay, deterministic GTD intake,
controller-driven expiry-watermark advancement, and explicit stop-reference
sequencing.

**Falsification probe.** Feed decreasing/equal timestamps and confirm identical
arrival-order matching; submit deadlines at/before intake, future sweep
horizons, equal/regressing watermarks, and replay after clock rollback. Priority
reordering, accepted invalid temporal fences, or an engine clock read falsifies
A5. Any implicit stop-reference change from time or local matching also
falsifies A5.

## A6 — negative balances and risk/ledger separation

**Assumption.** Negative account balances are valid general-ledger state; the
order-risk layer is separate from ledger posting and does not make ledger
acceptance imply credit authorization.

**Dependent results.** Atomic posting, settlement, and risk/ledger separation.

**Falsification probe.** Add a blocking balance policy in the posting path. If
ledger acceptance begins to imply credit authorization, A6 is no longer valid.

## A7 — negative and zero prices

**Assumption.** Negative and zero prices are valid for some instruments.

**Dependent results.** Price type range and settlement cash-flow direction.

**Falsification probe.** Execute zero and negative boundary prices and
reconcile all per-asset sums to zero. A hard positive-price assertion falsifies
A7.

## A8 — durable WAL scope and acknowledgement policy

**Assumption.** Durable continuous matching, call auction, risk, and ledger
guarantees are scoped to one local single-file or CRC-valid marker-selected
segmented WAL and the selected acknowledgement policy; `SyncAll` is the
default. Physical-prefix cutover additionally depends on an anchor-selected
local A/B checkpoint pair under A24/A28/A58 and the subsystem lineage
assumption, including A66 for call auctions.

**Dependent results.** Command/report/profile recovery after process
termination and across segment boundaries; physical-prefix retirement for
checkpoint-enabled continuous matching, risk, ledger, and call-auction layouts.

**Falsification probe.** Inject process termination, torn frames, delayed
writes, rotation and cutover at every boundary, marker-selector corruption,
power loss, filesystem remount, and device-cache reordering. Loss, duplication,
generation mixing, cross-lineage restoration, or acceptance of an anchor
without its exact selected checkpoint falsifies the storage assumptions behind
A8.

## A9 — finite shard limits and non-eviction

**Assumption.** Every shard has a validated finite `OrderBookLimits` policy.
Retained command reports and their events, accepted order identifiers,
account-control revisions, and the inline instrument-state revision do not
evict within one shard generation. Ordinary admission stops independently
before command and event cancellation reserves; valid expiry sweeps share
those protected lanes. Stop-trigger sweeps use the ordinary lane because one
trigger can expand into matching work. Total exhaustion requires a fenced
generation rollover.

**Dependent results.** Exact replay response, bounded
history/event/identity/control cardinality, cancellation headroom, and memory
complexity `O(C_max + E_max)`.

**Falsification probe.** Run beyond ordinary and total command/event
boundaries; flood new/replace, unknown/wrong-owner cancel, valid cancel, empty
mass-cancel, empty/full/regressing expiry sweeps, empty/partial/backlogged stop-
trigger sweeps, stale/valid account and instrument-state controls, and exact
retries; restart from WAL/checkpoint under equal, larger, and insufficient
limits. Growth past a bound, a business-invalid control consuming either
reserve, a stop sweep consuming protected history, mutation by an exact retry,
or silent identity/control/event eviction falsifies A9.

## A10 — unobservable hash iteration order

**Assumption.** Hash bucket/probe order and process-key hash order are never
externally observable. Exposed ordered data comes from price trees, FIFO links,
journal vectors, or the insertion-ordered dense command-history component of
bounded retry caches under A75/A84/A120; no output is derived from bucket
order.

**Dependent results.** Deterministic public outputs across process seeds.

**Falsification probe.** Replay identical command streams under varied hash
seeds and byte-compare reports, depth, journal order, and retained-history
iteration. Any difference falsifies A10.

## A11 — once-only trade settlement

**Assumption.** A continuous trade is durably settled once using a caller-
supplied globally unique transaction ID and the definition-correlated path. A
complete accepted call-auction uncross instead supplies exactly one such ID per
trade in report order and, under A118, one per explicit fee transfer. It settles
as one entry or atomic batch under A116/A118. A119 can later reverse that exact
complete settlement and optionally append one replacement settlement as one
event. The lower-level convention API is not an authorization boundary.

**Dependent results.** Delivery-versus-payment balances, WAL reconstruction,
and retry behavior.

**Falsification probe.** Submit exact retries, transaction collisions,
partially precommitted auction groups, mismatched instrument versions, and
terminate between WAL append and balance commit. Any mismatched-version
posting, duplicate economic effect, split auction group, or lost acknowledged
entry falsifies A11.

## A12 — allocation-failure boundaries

**Assumption.** Allocator failure and process abort remain outside the general
recoverable error model except at explicit construction, preparation,
checkpoint, codec, journal-frame, and fallible-output boundaries. Authoritative
instrument-catalog, continuous matching/risk, continuous and call-auction
market-data order/control/scratch, call-auction risk/history, and ledger
balance/transaction/reversal hashes use constructor-owned dense entry vectors
and fixed open-addressed bucket arrays. Instrument definitions, continuous
matching price and GTD-expiry arenas, continuous and call-auction retained-event
arenas,
continuous market-data, call-auction price/identity/account, and both public-projection
price arenas, continuous and call-auction replica active/standby depth, the
ledger journal vector, prepared auction/ledger buffers, the call-auction mass-
cancel scratch vector, the continuous
order-selection pool, and the call-auction uncross pool reserve their
applicable complete or command-exact bounds before mutation.
The immutable trading calendar owns its caller-supplied row vector and exactly
reserves its derived session-ID index before publication.

Successful continuous-book/risk, call-auction-book/engine/risk, and indexed-AVL
structural audits allocate no scratch; continuous matching/risk,
call-auction/risk, and ledger checkpoint capture/semantic/capacity resources,
call-auction settlement/correction-entry and per-fee/reversal posting
construction, codec collections/output, and WAL frame/batch/read buffers
reserve fallibly with typed resource identity. Arc
control blocks, caller-owned command/entry
objects, decoded/caller-built event traces and checkpoints, snapshot-file
ownership, path/string construction, ledger diagnostic/reconciliation
collections, failure-detail formatting, caller-owned or cloned generic auction
allocation plans, and wide ledger magnitudes can still allocate or abort.

Continuous order-book depth and private active-order output, call-auction limit
depth, continuous and call-auction account-order-ID output, plus continuous and
call-auction market-data batch/snapshot/depth outputs have explicit fallible
APIs. Continuous and call-auction authoritative books also expose
allocation-free market-priority depth iterators, as do continuous and
call-auction public replicas. Continuous private immediate-execution quotes,
authoritative and replica displayed-liquidity quotes, and resting-order queue-
position queries return fixed-size values without successful-path allocation.
Borrowed ledger-record lookup, history iteration,
per-record transaction iteration, and point-in-time balance reconstruction
allocate no output and return typed journal/index or reconstruction
contradictions. Convenience wrappers can still panic on allocation failure;
the account-order-ID wrappers can also panic if private topology is corrupt.

**Dependent results.** Typed failure exists at the enumerated boundaries; no
end-to-end allocation-failure continuation claim follows. Authoritative bounded
hashes, fixed arenas including both retained-event stores, fixed ledger journal
order, and every accepted catalog/matching/risk/auction/ledger/publication
state transition reuse already-owned storage. Hash lookup/insertion/deletion
are expected `O(1)` under `RandomState`; an adversarial full collision cluster
is `O(N)` but remains bounded and allocation-free. Matching, auction, ledger,
and both publication paths own complete commit/output inputs before mutation.
Exact retry shares existing state and cannot consume capacity; creating an
arena, calendar, or selection/uncross-pool `Arc` during construction,
constructing a snapshot-file image, or an external clone remains outside the
guarantee.

**Falsification probe.** Force all keys into one collision cluster and remove
head/middle/tail entries; differentially execute at least 100,000 mixed hash
operations; assert dense/bucket/vector/AVL/event-arena capacities never move;
exercise interleaved catalog version registration, immutable calendar
construction/lookup/codec/clone behavior, allocation-free
continuous/call-auction book/engine/risk and AVL audits, typed
continuous/call-auction/ledger checkpoint capture/validation, codec, and
journal-frame reservation failure, sustained matching/risk deletion, both
market-data order/price churn paths, and balance-identity reuse; exhaust every
catalog, ledger, matching-event, auction-event, prepared-selection,
prepared-uncross, checkpoint-capture, and market-data resource independently;
inject layout/allocation failure at every typed boundary.

Any lookup/model divergence, post-construction authoritative-index/arena
growth, missing typed failure, state mutation before capacity success, commit
allocation, or output/mutation before all required reservations succeed
falsifies A12. Broader allocation-failure continuation requires pools/fallible
ownership for the remaining enumerated allocators.

## A13 — canonical WAL paths and leases

**Assumption.** Every raw writer uses the canonical WAL pathname and sidecar
lease; every segmented writer uses the canonical directory and manager lease.
No participant introduces a hard-link alias, replaces path components, or
deletes a live lease.

**Dependent results.** Exclusive canonical-path ownership, contiguous global
sequence assignment, rotation, and append offsets.

**Falsification probe.** Start concurrent raw writers/managers through
identical, relative, symlinked, and hard-linked paths while replacing
lease/path entries. More than one successful writer falsifies A13 and requires
a kernel inode lock or consensus ownership.

## A14 — CRC-32C scope

**Assumption.** CRC-32C protects WAL frames and semantic snapshots against
accidental corruption, not deliberate forgery.

**Dependent results.** Corruption detection, torn-tail classification, and
snapshot validation.

**Falsification probe.** Modify a payload without updating CRC, then modify it
while recomputing CRC. Acceptance of the first falsifies implementation
integrity; concern about the second requires authenticated records.

## A15 — format version immutability

**Assumption.** WAL format version 20, snapshot format version 20, continuous
market-data payload version 3, call-auction market-data payload version 5, and
trading-calendar payload version 1 are immutable. WAL and snapshot versions
`1` through `19` are expired and rejected explicitly rather than inferred or
migrated. WAL v20 preserves v19 values and adds continuous order-type tag `3`,
`MarketToLimit`; event-kind tag `15`, `MarketToLimitPriced`; and rejection
tags `56` and `57` for empty opposite liquidity and a non-resting lifetime.
Snapshot v20 preserves v19 direct rows and embeds WAL-v20 values in
chronological histories; matching and coupled-risk kinds `2` and `3` therefore
retain the submitted unpriced command and explicit captured price. WAL v19
preserved v18 values except that each private call-auction
trade grows from 56 B to 72 B by inserting its immutable instrument ID and
definition version before order/account/price/quantity fields. Snapshot v19
preserves v18 direct rows and embeds WAL-v19 values in chronological histories;
call-auction kinds `4` and `5` therefore contain the instrument-bound trade
value. WAL v18 preserved v17 values and changed minimum-quantity IOC under
decrement-and-cancel from unsupported to the A115 exact two-counter execution
interpretation. Snapshot v18 preserves v17 direct rows and embeds WAL-v18
values in chronological histories. WAL v17 preserves v16 values and changes
continuous FOK decrement-
and-cancel from unsupported to the A114 external-fill-before-self-barrier
interpretation. Snapshot v17 preserves v16 direct rows and embeds WAL-v17
values in chronological histories. WAL v16 preserved v15 values and added
call-auction self-trade policy tag `1`, `Abort`, and rejection tag `23`,
`SelfTradeWouldOccur`. Snapshot v16 preserved v15 direct rows and embedded
WAL-v16 values in chronological histories.
WAL v15 added call-auction indicative command tag `7`, action tag `7`,
event-kind tag `9`, and a nullable fixed-layout state. Snapshot v15 preserved
v14 direct rows and embedded WAL-v15 values in
chronological histories, from which the current optional indication is
derived. WAL v14 appended one little-endian `u16` priority-class scalar to
call-auction order commands, event snapshots, and direct checkpoint rows. WAL
v13 added one explicit
allocation-policy byte to call-auction uncross commands and completion events.
WAL v12 added call-auction command/action tag `6` for retained-priority
amendment, rejection tag `22` for a non-reduction, and event-kind tag `8` for
`OrderAmended`; those values remain unchanged in v14.
Continuous market-data v3 preserves v2 bytes but adds the absent-public-maker
trade interpretation required for fully hidden execution. Call-auction market-
data v5 preserves v4 layouts and adds update-kind tag `6`, `Indicative`, plus
one optional indication in snapshots. Version 4 added book-reason tag `5`,
`Amended`.
Trading-calendar v1 is a complete value with no self-describing schema field;
an enclosing protocol selects it explicitly, while its encoded
`CalendarVersion` identifies schedule content rather than wire
interpretation. No runtime
interprets an expired envelope as current. Any future incompatible evolution
uses a new explicit version and provenance-preserving migration when
authoritative predecessors exist.

**Dependent results.** Deterministic decoding, historical replay, checkpoint
recovery, anchor interpretation, stable auction records/images, stable calendar
images, and fail-closed format boundaries.

**Falsification probe.** Byte-compare golden WAL-v20, snapshot-v20,
market-data-v3, auction-market-data-v5, and trading-calendar-v1 fixtures through
every supported release; mutate valid WAL frames and images to versions `1`
through `19`; verify definition booleans, every display, TIF, rejection, and
cancellation tag, continuous expiry/stop/source tags, raw auction record tags
`9`/`10`, replacement, mass-cancel, amendment, and allocation-policy tags,
priority-class scalars, auction-trade instrument ID/version fields, uncross
self-trade tags, rejection tag `23`, indicative command/action/event/update
tags and nullable state, market-to-limit command/event/rejection tags,
snapshot kinds `1` through `5`, hidden-maker trade
application, auction replacement and mass-cancel projection, and every calendar
scalar/row offset.
Any changed supported bytes/interpretation or acceptance across an expired
envelope boundary falsifies A15.

## A16 — at most one dangling command

**Assumption.** At most one final command can lack a report because submission
is serial and command/report pairs are not interleaved.

**Dependent results.** Automatic interrupted-report completion.

**Falsification probe.** Inject termination at every protocol boundary and
introduce command pipelining. More than one outstanding command falsifies A16
and requires transaction/correlation identifiers in the WAL grammar.

## A17 — instrument version binding

**Assumption.** The gateway or sequencer selects the definition effective at
the authoritative event time, and one `OrderBook` remains bound to that version
for its lifetime. In-place rule changes are not performed.

**Dependent results.** Effective-time rule selection, FIFO continuity, and
deterministic replay across rule regimes.

**Falsification probe.** Exercise events at `effective_from - 1 ns`,
`effective_from`, and `effective_from + 1 ns`, including delayed and reordered
arrivals. Any command admitted under a version other than the selected
authoritative-time version falsifies A17.

## A18 — seeded positions and immutable risk profiles

**Assumption.** Seeded account positions and immutable numerical risk profiles
are authoritative at shard start; no external fill, bust, transfer, or
numerical profile change occurs outside the command trace. Direct profile
bootstrap closes after the first sequenced command, while durable profile
metadata is fixed before the command grammar begins. A separate revisioned
matching-level admission fence may block/re-enable a registered account without
mutating its numerical profile.

**Dependent results.** Position limits, reduce-only behavior, deterministic
risk replay, immutable post-start numerical profiles, and sequenced local kill
control.

**Falsification probe.** Register before first preparation, between first
preparation and commit, and after the first accepted or rejected command;
mutate persisted profile metadata after a command; sequence block/re-enable
controls; reconcile seeded and replayed positions against an independent
clearing source; inject an out-of-band position/profile change. Any late
accepted registration, unexplained position difference, profile drift, or
unsequenced fence change falsifies A18 and requires additional durable
administrative/clearing events.

## A19 — raw-quantum notional limits

**Assumption.** Risk notional limits are denominated in absolute raw price
quanta multiplied by lots, not a normalized reporting currency. Instrument
collars bound every reachable matching price.

**Dependent results.** Per-order and aggregate notional rejections, including
negative prices.

**Falsification probe.** Calculate side-specific reachable-price extrema
independently for positive, zero-crossing, and negative collars. Any execution
outside the collar or normalized-currency interpretation of these thresholds
falsifies A19.

## A20 — sole risk-managed mutation path

**Assumption.** `RiskManagedOrderBook` is the sole mutation path for a
risk-controlled shard; its accepted matching trace, including expiry
cancellations and stop arm/trigger transitions, is sufficient to update every
reservation and position without a second fallible business decision. A
dormant stop owns exactly one reservation valued from its activation
constraint; triggering changes reservation state without duplicating exposure.

**Dependent results.** In-process matching/risk atomicity and durable
reconstruction.

**Falsification probe.** Apply model-generated combinations of fills,
individual/mass/expiry/account-control cancels, stop arm/trigger/failure,
replacements, and all STP modes; cross-audit after every command and compare
recovery byte-for-byte. Any resting or dormant active order without exactly one
matching reservation, or any duplicated activation/position/control effect,
falsifies A20.

## A21 — single-publisher report stream

**Assumption.** After bootstrap, one publisher receives every non-replayed
report from its authoritative book in matching-event order; no second publisher
state is merged into that sequence. The publisher privately mirrors active GTD
deadlines and the expiry watermark, account-control state/revision, and
instrument trading state/revision to validate traces. It also mirrors dormant
stop identities, canonical buy/sell trigger indices, and the committed stop
reference; public payloads remain anonymous except for instrument state.

**Dependent results.** One-to-one incremental publication, exact-retry
suppression, expiry/control/stop-cancellation validation, canonical trigger
validation, state-transition publication, and publisher/book cross-audit.

**Falsification probe.** Drop, duplicate, reorder, corrupt expiry
order/watermark/aggregates, stop arm/trigger/reference/backlog order, or control
prior/current/revision/aggregates, and splice reports from another instrument/
version. Any undetected sequence, identity, expiry, stop, control, state, or
depth discontinuity, or any aggregate/control mismatch that passes cross-
audit, falsifies A21.

## A22 — native reserve policy

**Assumption.** The configured native reserve policy is the authoritative
policy for this shard: one stable private order ID, a fixed maximum displayed
peak, total hidden leaves eligible for matching/FOK/risk, and displayed-class-
tail requeue after full slice exhaustion. The displayed/reserve class remains
ahead of the fully hidden class at one price. Public depth contains only active
displayed slices. Discretionary, pegged, synthetic/implied, randomized-peak,
and other venue-managed hidden priority are outside this policy.

**Dependent results.** Reserve admission, maker priority, replenishment events,
public depth, FOK, risk, and replay.

**Falsification probe.** Replay certified venue fixtures spanning partial/full
peak fills, competing same-price orders, repeated refresh, STP, cancel/replace,
FOK, session boundaries, and public-feed messages. Any venue rule requiring a
new private order ID, retained/pro-rata hidden priority, randomized display,
different modification behavior, or different feed aggregation falsifies A22
for that adapter and requires a distinct versioned policy.

## A23 — snapshot-plus-suffix replica recovery

**Assumption.** A recovery snapshot and buffered incrementals originate from
the same immutable instrument-version shard, and the consumer applies only the
contiguous suffix beginning at `snapshot sequence + 1`.

**Dependent results.** Race-free public replica recovery after sequence loss.

**Falsification probe.** Delay snapshot capture while dropping/reordering
buffered updates and inject cross-version images. Acceptance of a noncontiguous
suffix or convergence to depth unequal to the publisher falsifies A23.

## A24 — directory and file synchronization semantics

**Assumption.** The host supports opening and synchronizing WAL/snapshot parent
directories, and successful file/directory `sync_all` calls have the
persistence semantics established by the qualified filesystem, mount,
controller, and device stack.

**Dependent results.** Durable creation/repair, segment/marker and snapshot
replacement, lease creation/removal, and `SyncAll` acknowledgements.

**Falsification probe.** Force power loss after every write, file barrier,
rename, and directory barrier, including each rotation and snapshot-publication
phase; remount and compare verified files and directory entries. Any lost
acknowledged frame or resurrected/deleted synchronized entry falsifies A24 for
that stack.

## A25 — abandoned lease recovery

**Assumption.** Abandoned valid/invalid raw, manager, or snapshot lease
recovery occurs only after independent proof that the observed owner cannot
write and while all new writer starts are externally quiesced.

**Dependent results.** Safe progress after process termination leaves a durable
or partially emitted lease.

**Falsification probe.** Invoke either recovery API while the owner remains
live, can resume, or while another writer races between comparison and
deletion. Any such topology falsifies A25 and requires kernel-released locking
or a fenced lease service.

## A26 — segmented WAL directory ownership

**Assumption.** A segmented WAL directory is dedicated to one
`SegmentedJournal`; no external participant mutates its marker, member names,
closed files, or inventory. Capacity, lineage origin, and payload limit remain
immutable. The CRC-32C-protected marker may advance only its active generation
and first-retained sequence under the manager-held A58 cutover protocol.
Incomplete-marker recovery is invoked only for pre-segment initialization.

**Dependent results.** Filename-derived generation selection, strict
selected-generation recovery, configuration binding, bounded invalid-marker
recovery, deterministic inactive-generation cleanup after validation, and one
global sequence without generation mixing.

**Falsification probe.** Concurrently add, rename, truncate, hard-link, or
rewrite entries; mutate the selector with and without recomputing its checksum;
inject invalid selected and valid non-selected generations; interrupt each
staging/publication/cleanup boundary; invoke marker recovery before/after
segment evidence; and reopen under drifted settings. Acceptance without a
deterministic error, selected-generation mixing, cleanup before
selected-generation validation, or removal of a valid/post-segment marker
falsifies implementation integrity. External archival continuity requires a
separately coordinated handoff.

## A27 — replay under writer exclusion

**Assumption.** Authoritative replay runs while the raw writer or segmented
manager lease excludes concurrent mutation. Standalone readers used beside a
live writer consume a verified prefix, not an atomic point-in-time snapshot.

**Dependent results.** Deterministic matching/risk/ledger reconstruction and
fixed-inventory segmented iteration.

**Falsification probe.** Append and rotate while repeatedly opening readers. A
corrupt accepted frame falsifies reader integrity; a requirement that every
reader observe one atomic cross-file instant falsifies A27 and requires
snapshot coordination or immutable generation manifests.

## A28 — snapshot path ownership and rename semantics

**Assumption.** Each semantic snapshot target, `.pending` file, and writer
lease are dedicated to one canonical-path snapshot owner, remain on one
qualified local filesystem, and do not alias another storage object. POSIX-like
same-filesystem rename semantics apply.

**Dependent results.** Exclusive snapshot publication, atomic namespace
replacement, and explicit pending recovery.

**Falsification probe.** Start writers through canonical, relative, symlinked,
and hard-linked names; place targets across mount points; replace path
components; and terminate around every publication phase. Multiple successful
owners, a cross-mount replacement, or an observable partial target falsifies
A28.

## A29 — ledger checkpoint lineage

**Assumption.** A ledger checkpoint and its recovery WAL represent one
immutable ledger-record history; numeric generation is never treated as lineage
without exact prefix equality or an exact version-12 checkpoint anchor. An
anchored WAL separately binds semantic record count and physical retired-prefix
sequence. Capture/audit resource or replay-constructor failure under A89 occurs
before snapshot/cutover mutation and leaves the durable ledger unpoisoned; live
semantic contradiction poisons it.

**Dependent results.** Checkpoint restoration, correction/batch grouping,
suffix-only balance application, idempotency preservation, physical-prefix
retirement, divergence detection, and retryable operational capture failure.

**Falsification probe.** Inject every A89 resource/construction failure,
same-generation forks, higher-generation forks, stale pending files, a
checkpoint ahead of an uncut WAL, record-group mutation, WAL-prefix mutation,
wrong/corrupt A/B slots, semantic/physical sequence substitution, and
corruption with/without a recomputed CRC. Any resource-induced poison,
snapshot/cutover mutation after capture failure, accepted
non-prefix/non-anchored history, split correction/batch, or second economic
effect falsifies A29.

## A30 — retained ledger history horizons

**Assumption.** Retaining and decoding the complete ledger transaction history
inside each checkpoint is acceptable only for bounded volume horizons and
configured payload limits. Single-file cutover retires physical WAL prefix
bytes but does not reduce this retained semantic history.

**Dependent results.** Checkpoint memory/storage complexity and current
recovery latency.

**Falsification probe.** Run a production-volume ledger soak while measuring
checkpoint size, peak memory, capture latency, uncut-WAL reopen latency, and
anchored checkpoint-plus-suffix reopen latency. Exceeding declared limits
falsifies A30 and requires bounded idempotency/audit-history retention in
addition to physical cutover.

## A31 — immutable entries and reversal lifecycle

**Assumption.** Posted entries are immutable; reversal is the sole implemented
negation lifecycle. Each entry can be reversed once, and reversal of a reversal
is intentionally represented as another once-only append-only reinstatement
rather than deletion. Corrections and batches preserve that lineage rather than
rewriting prior entries.

**Dependent results.** Exact reversal/correction/batch economics, lineage
audit, idempotency, checkpoint restoration, and durable recovery.

**Falsification probe.** Attempt missing/later target, same/different-content
duplicate ID, altered/reordered leg, second reversal, reversal-of-reversal,
partial grouped event, `i128::MIN`, restart, and checkpoint-suffix cases. Any
accepted non-inverse, second reversal of one target, mutated prior entry, split
grouped event, or inconsistent recovered index falsifies A31.

## A32 — reconciliation statement completeness

**Assumption.** A `ReconciliationStatement` is an authoritative complete-ledger
balance image for its declared generation, observation time, source reference,
and asset universe; partial account/custodian extracts are not interpreted as
complete statements. Its observation cannot precede the last recorded event in
that generation.

**Dependent results.** Zero-break reconciliation and deterministic
external-minus-internal differences.

**Falsification probe.** Omit an account or asset, duplicate a key, inject
zero/unbalanced balances, use an observation timestamp before the generation's
final event, compare before/after a concurrent posting, and independently
reproduce every difference. An incomplete/temporally impossible image accepted
as reconciled or a noncanonical/nonreproducible break falsifies A32.

## A33 — caller-supplied accounting dates

**Assumption.** The caller maps each financial event to an authoritative
Gregorian accounting/value date before ledger admission. `AccountingDate` is
signed days relative to 1970-01-01; it does not select a business calendar,
time zone, holiday rule, settlement convention, or legal close date.

**Dependent results.** Closed-period rejection, dated settlement/reversal,
deterministic WAL/checkpoint replay.

**Falsification probe.** Exercise epoch, negative, maximum/minimum,
close-boundary, calendar-transition, holiday, and delayed-booking cases against
an independently versioned calendar source. Any different upstream date mapping
for the same event falsifies A33.

## A34 — authoritative event time and period controls

**Assumption.** `recorded_at` is an authoritative UTC nanosecond event time
supplied in nondecreasing journal order, and only an externally authorized
controller invokes period close/reopen. The ledger enforces ordering and
transition shape, not clock authenticity or principal authorization.

**Dependent results.** Temporal replay fence, period controls, and
reconciliation observation ordering.

**Falsification probe.** Inject decreasing/equal timestamps, clock rollback,
stale exact retries, unauthorized controls, repeated/non-advancing closes,
forward reopens, restarts, and checkpoint/WAL suffix transitions. Acceptance of
a regression or invalid transition falsifies implementation integrity; forged
time or unauthorized but structurally valid control falsifies A34's
environment.

## A35 — retained dated ledger formats

**Assumption.** The dated 47 B fixed ledger-entry payload, ledger-correction
WAL kind `6`, ledger-batch WAL kind `7`, and tagged record-based checkpoint
schema first defined under version 3 are retained byte-for-byte by the
deployable version-12 WAL/version-12 snapshot accounting formats; no earlier
undated or flat-entry expired `QWAL`/`QSNP` ledger artifact requires
compatibility. Older development envelopes fail before payload interpretation
rather than receiving inferred dates, times, or grouping.

**Dependent results.** A version-12 WAL/version-12 snapshot dated atomic
grouped-event accounting schema with an explicit rejection boundary for expired
artifacts.

**Falsification probe.** Inventory every persisted ledger WAL/snapshot and
decode golden supported-version fixtures before deployment. Discovery of an
authoritative predecessor that must remain readable falsifies A35 and requires
a provenance-preserving migration tool; silently changing its envelope is
prohibited.

## A36 — single-frame atomic corrections

**Assumption.** One correction is exactly one existing-entry reversal plus one
standard replacement, and one CRC-protected WAL frame is the authoritative
atomic recovery unit. Split external messages and controller authorization are
outside this specialized correction contract; arbitrary groups use
`LedgerBatch`.

**Dependent results.** One-sequence correction idempotency, all-or-neither tail
recovery, direct final-balance arithmetic, and checkpoint grouping.

**Falsification probe.** Tear the correction frame at every byte; corrupt each
nested length/tag/entry; retry either transaction separately and together;
inject partial prior state, closed dates, timestamp regression, duplicate IDs,
and three-term `i128` boundary cases. Any recovered single member, second
effect, accepted partial state, or incorrect final balance falsifies A36.

## A37 — retained matching payload formats

**Assumption.** The reserve-, mass-cancel-, account-control-, GTD-, stop-, fully
hidden, and minimum-quantity command/report variants defined through version 8
remain explicit variants inside deployable WAL version 20. Version 9 added
durable stop-reference source identity, source version, source sequence, and
typed discontinuity/collision outcomes; it does not infer them from WAL version
8. No earlier matching WAL requires runtime compatibility. Expired envelopes
fail before payload interpretation rather than receiving inferred display,
mass-cancel, control, expiry, stop, reference-source, trigger-priority, hidden,
minimum-quantity, or anchor semantics.

**Dependent results.** Explicit fully displayed/reserve/fully hidden/GTD/stop
state, refresh, expiry, and trigger events, canonical mass cancellation,
revisioned account fencing, and anchored cutover under WAL envelope version
`20`.

**Falsification probe.** Inventory every persisted matching WAL and decode
golden version-20 fixtures before deployment. Reject version-19 artifacts with
and without re-labelled headers, and mutate every hidden/minimum-quantity/
expiry/stop/source tag, definition flag, threshold, deadline, watermark,
reference, trigger, activation, aggregate, and ordering relation.
Discovery of an authoritative
predecessor that must remain readable falsifies A37 and requires a
provenance-preserving migration; silently inferring absent fields is not
permitted.

## A38 — mass-cancel ownership scope

**Assumption.** `MassCancel.account_id` and
`CallAuctionMassCancel.account_id` are authoritative owner selections already
authenticated and authorized by the upstream gateway or control plane. One
continuous or call-auction book contains only one instrument version; `All`
therefore means all active orders for that account in that shard, not across a
portfolio, firm, session, or market segment.

**Dependent results.** Continuous and call-auction account/side selection,
cancellation authorization boundary, audit totals, risk release, and durable
replay.

**Falsification probe.** Attempt a principal/account substitution, delegated
cancel-on-behalf, cross-session/cross-instrument kill, concurrent new order,
and replay during every trading state. Any requirement for
firm/session/portfolio scope, in-engine principal authorization, or an atomic
cross-shard fence falsifies A38 and requires a versioned coordinator plus
authenticated ownership metadata.

## A39 — matching checkpoint publication

**Assumption.** A matching checkpoint is published only by `DurableOrderBook`
after a WAL barrier and a complete independent replay audit. Complete semantic
lineage remains in the immutable selected checkpoint; an uncut WAL retains the
corresponding physical prefix, while cutover in either layout replaces it only
with an exact A/B anchor under A58. CRC-32C detects accidental image corruption
but does not authenticate a state image rewritten together with its checksum.
Complete command/report history remains acceptable only for bounded horizons
under A9. Capture and validation reservation or temporary replay-shard
construction failure occurs before snapshot/cutover mutation and leaves the
durable shard unpoisoned; semantic contradiction poisons it.

**Dependent results.** Direct matching-state restoration,
FIFO/reserve/STP/GTD/dormant-stop continuity, expiry-watermark and stop-
reference reconstruction, exact retry after checkpoint, suffix-only matching
replay, physical-prefix retirement in both layouts, checkpoint lineage
ordering, and retryable pre-semantic resource failure under A88.

**Falsification probe.** Mutate boundary, definition, command/report order,
event/trade sequence, active FIFO/display/expiration state, dormant rows,
trigger priority, expiry watermark, stop reference/backlog, WAL prefix,
generation selector, or anchor/slot identity; force each A88
resource and replay
constructor failure; test non-default WAL origins, same/higher-generation
forks, ahead-of-WAL images, recomputed CRCs, and production-volume
memory/restart limits. Any accepted accidental mutation, unproved lineage,
mismatched deterministic capture, generation mixing, second command effect,
resource-induced poison, or cutover mutation after capture failure falsifies
implementation integrity. A deliberate-rewrite threat or bounded-memory
requirement requires authenticated state commitments plus fenced
retention/idempotency watermarks.

## A40 — coupled risk checkpoint publication

**Assumption.** A coupled risk checkpoint is published only by
`DurableRiskOrderBook` after a WAL barrier and a complete replay through the
same immutable definition and canonical profile set. Executed position is
derived solely from retained command/report history; every live matching order
has exactly one total-leaves reservation. An uncut WAL retains the complete
prefix; a compacted WAL in either layout replaces it only with an exact A/B
anchor under A58.

**Dependent results.** Direct restoration of risk rejections, positions,
hidden/displayed reserve state, reservations, exposure aggregates, exact
retries, physical-prefix retirement, and suffix-only coupled state transitions.

**Falsification probe.** Mutate WAL origin, profile count/order/content,
metadata boundary, risk rejection, executed position, active
owner/side/price/total leaves, redundant exposure, WAL prefix, generation
selector, or anchor/slot identity; inject missing profiles, same-generation
forks, ahead-of-WAL images, recomputed CRCs, out-of-band fills/transfers, and
production-volume capture/reopen loads. Any accepted mismatch, unexplained
position, unmatched reservation, generation mixing, second command effect, or
unproved prefix/anchor falsifies implementation integrity. Out-of-band state,
bounded retained history, or deliberate rewrite requires sequenced
administrative events, retention watermarks, and authenticated commitments.

## A41 — per-account intrusive order index

**Assumption.** Each continuous and call-auction mutation-maintained per-
account/per-side intrusive index is redundant derived state: every active order
occurs exactly once under its owner and side, and no inactive order occurs.
Head/tail, forward/backward links, count, quantity, and applicable event-work
aggregates are mutually consistent. `OrderId` ascending order is the canonical
mass-cancel audit order.

**Dependent results.** `O(K)` link traversal plus in-place `O(K log K)`
canonicalization independent of total active orders `O`; `O(1)` ordinary
membership insertion/removal without a separate membership-node allocation;
checkpoint/WAL reconstruction of both derived topologies.

**Falsification probe.** Exercise rest, partial/full fill, cancel,
retained/lost-priority replace, every STP policy, reserve refresh, call-auction
partial/full uncross, side/all mass cancel, sparse accounts in large books,
deterministic replay, and direct checkpoint restoration; cross-audit after
every transition. Inject broken
links, cycles, owner/side errors, count/head/tail divergence, and missing
orders. Any undetected topology error, missing/duplicate/stale membership,
noncanonical output, selection runtime proportional to unrelated `O`, or
account membership allocation falsifies A41.

## A42 — ordered ledger batches

**Assumption.** A `LedgerBatch` is an already-authorized ordered accounting
instruction containing at least two canonical entries. Declared order is
authoritative for time, period controls, and reversal lineage; balances expose
only `b + Σδᵢ`. Every member is stored in one CRC-protected WAL frame and one
ledger record.

**Dependent results.** Ordered in-batch lifecycle visibility, direct
final-balance arithmetic, shared event sequence, exact grouped replay,
all-or-neither crash recovery, and checkpoint lineage.

**Falsification probe.** Permute members; target a later transaction; combine
close/reopen with dated entries; repeat/reuse transaction IDs; separately
commit none/some/all members; force stale preparation; exercise cancelling
deltas at `i128` boundaries; tear/corrupt every frame byte; rotate segments;
and restore checkpoint plus suffix. Any visible prefix, order-insensitive
lifecycle result, artificial intermediate overflow, accepted partial/wrong
grouping, second effect, or replay divergence falsifies A42.

## A43 — wide aggregate totals

**Assumption.** Individual posting and account-balance amounts remain signed
`i128`, but no fixed-width ceiling is imposed on the exact positive or negative
aggregate across accounts. `LedgerMagnitude` may allocate only after a side
exceeds `u128::MAX`; allocator failure remains under A12.

**Dependent results.** Acceptance of mathematically balanced large-leg entries,
trial-balance completeness, reconciliation validation, invariant audits, exact
unbalanced diagnostics, and checkpoint replay beyond `u128` aggregate totals.

**Falsification probe.** Cross `u128::MAX` with one entry and with separately
committed entries; vary canonical leg order; compare independently calculated
decimal totals; add after spill; exercise balanced/unbalanced reconciliation,
checkpoint encode/decode, replay, and maximum configured account/leg volumes.
Any false arithmetic rejection, truncation, unequal reconstructed total,
fixed-width wrap, or audit failure on valid committed state falsifies A43.

## A44 — cached best-level state

**Assumption.** Each side's bounded execution-price AVL map is authoritative
for matching and its cached best `(price, key-checked slot handle, head, tail,
public quantity, public order count)` is redundant derived state. A second
bounded AVL contains exactly prices with non-zero public quantity and supplies
an independently cached public best. Both indexes and caches update through the
sole level insert, handle-update, and remove boundary. No caller obtains
unrestricted mutable level access.

**Dependent results.** Allocation-free `O(1)` execution-best price/order and
public-best level discovery plus A83 direct maker-level mutation; no
ordered-tree traversal per maker-slice selection or non-empty aggregate update;
deterministic checkpoint reconstruction derives fresh process-local handles.

**Falsification probe.** Exercise empty/non-empty transitions,
better/worse/equal-price insertion, best/non-best deletion, partial/full fill,
retained/lost-priority replace, all STP modes, sole/multi-order reserve
refresh, mass cancel, and checkpoint/WAL reconstruction in both side
directions. Deliberately corrupt the cached price, handle, or any cached level
field or public-price membership and run the independent AVL/cache audit. Any
undetected divergence, stale handle, or best lookup inconsistent with its AVL
map falsifies A44.

## A45 — FOK eligibility under reserve replenishment

**Assumption.** For FOK eligibility, reserve replenishment changes execution
order but not total fillable external leaves at a price unless a cancel-
aggressor, cancel-both, or decrement-and-cancel self order is encountered. A
self order in the displayed class is a barrier after only the current slices
preceding it because refresh rejoins at that class's tail. A self order in the
hidden class is a barrier after all total leaves in the preceding displayed
class, plus earlier hidden leaves, because refresh never crosses into the
hidden class. Cancel-resting removes self orders and leaves every external
total leaf eligible.
For decrement-and-cancel, the self order is a FOK eligibility barrier because
prevented self quantity is not an external trade. A successful FOK completes
before reaching that barrier and therefore emits no STP event.

**Dependent results.** Allocation-free FOK preflight with `O(1)` auxiliary
space and one visit per active order in crossed levels; exact hidden-liquidity
and STP behavior without materialized slice queues.

**Falsification probe.** Differentially compare against an independent literal
slice/requeue simulation across generated levels, FIFO ownership patterns,
partial reserve slices, fully hidden FIFO, quantities, prices, and all four
FOK STP modes; retain explicit displayed- and hidden-class same-price
barriers, better-price hidden exhaustion, cancel-resting, insufficient-
liquidity, and execution-trace fixtures. Any
eligibility or trace disagreement falsifies A45.

## A46 — operational limits policy

**Assumption.** Caller-selected finite `OrderBookLimits` and
`RiskManagedLimits` are authoritative operational policy for the current shard
process. Capacity failures are unsequenced operational errors: exact retries
bypass gates, and only a currently business-valid cancel, mass-cancel, expiry
sweep, block-and-cancel account control, or instrument transition-and-cancel
into an entry-closed state may enter either reserved command/event lane.
`cancellation_reserve >= max_active_orders`, `max_report_events >=
max_active_orders + 1`, and the derived event reserve is `max_active_orders +
1`; ordinary retained-event capacity covers both `max_report_events` and `2 ×
max_active_orders`.

Stop-trigger sweeps use ordinary history/event capacity and independently
prove their full activation/matching report bound. All four price arenas, the
GTD-expiry arena, both stop-trigger arenas, the retained-event arena, and all
matching/profile/reservation dense hashes are fallibly constructed through
configured maxima before the shard exists. Limits are not financial
WAL/snapshot payload semantics and may change at restart only when recovered
state and replayed historical peaks fit.

**Dependent results.** Mutation-free and pre-WAL semantic exhaustion; bounded
active/account/level/identity/control/history/report/total-event/profile
cardinalities; deterministic recovery; allocation-free ordered-price,
live-event, and authoritative hash mutation after construction. Fallible
construction/durable recovery identify an unrepresentable/unavailable
arena/hash layout or exhausted process-local book identity before state exists.

**Falsification probe.** Exercise every bound at `N-1`, `N`, and `N+1`; fill
ordinary command and event histories independently; perform individual, mass,
expiry, account block-and-cancel, and instrument-wide cancellation through
each reserve; require stop-trigger rejection from the protected lane; exact-
retry at exhaustion; recover raw/segmented WAL and
checkpoints with equal/larger/smaller matching and profile limits; request
`usize::MAX` layouts; assert dense, bucket, AVL, and event-arena capacities
through sustained identity churn; inject historical peaks above a lowered
policy. Any
mutation/WAL creation on semantic capacity failure, invalid command consuming
reserve, cardinality excess, arena/index growth, accepted undersized
restoration, unidentified construction failure, or replay divergence falsifies
A46.

## A47 — resting-residual prediction

**Assumption.** At one crossed price, cancel-resting excludes self leaves while
every external total leaf is reachable; cancel-aggressor/cancel-both terminate
the aggressor at the first self-order FIFO barrier, with only preceding
displayed slices reachable before that barrier; decrement-and-cancel can
consume every self and external total leaf through reserve refresh. The same
relations hold across successively worse crossed prices. Consequently, a
non-zero resting residual proves that every crossed opposite level was
completely removed. Replacement preview observes the pre-removal book, but the
amended order is on the incoming side and therefore cannot alter inspected
opposite-side liquidity.

**Dependent results.** Exact allocation-free prediction of whether a valid
GTC/GTD new order or non-priority-retaining replacement rests, the exact maker-order
cardinality removed before append, and, for a new account, whether complete
maker-account memberships disappear. Normal capacity admission remains expected
`O(1)`. A full resting bound invokes an `O(O_c + P_c log P)` liquidity scan;
proving new-account release can additionally visit all `O` active memberships.
Both use `O(1)` auxiliary space. Final active-order, active-account, and
same-side price-level admission is exact; a full same-side replacement level is
charged only for a proved residual.

**Falsification probe.** Differentially compare preview against actual GTC/GTD
new-order execution for at least 20,000 generated books spanning both sides,
positive and negative prices, multiple levels, FIFO ownership patterns,
full/reserve display, partial reserve slices, quantities, and all four STP
modes; compare exact removed-order counts and complete-account release.
Independently compare at least 20,000 generated full-level replacement
decisions against actual execution across the same dimensions. Retain explicit
full-fill, aggressor-cancel, true-residual, single/multi-order account release,
cancel-resting, durable reopen/replay, coupled risk-checkpoint, and
uncrossed-account fixtures. Any rest/no-rest, removed-count, account-release,
final-capacity, recovery, or replay disagreement; preview mutation; or scan
away from a full boundary falsifies A47.

## A48 — prepared command tokens

**Assumption.** A `PreparedCommand` is a process-local, single-use proof bound
to one non-reused `OrderBook` instance identifier and one monotonic
retained-command cardinality under A9. The allocator returns an explicit
construction error instead of wrapping its `u64` identity. Preparation fixes
operational checks and the core business result, carries a safe event bound
into the constructor-owned arena, and may own one isolated constructor-owned
order-selection lease under A87. Fixed authoritative index storage already
exists, so matching and risk-managed preparation borrow shard state immutably
and cannot change semantic state; only operational lease occupancy changes.
Commit first rejects a foreign instance or unrelated generation, while an
intervening exact command resolves through idempotent replay.

**Dependent results.** One matching preparation across direct, risk-managed,
durable, and durable-risk submission; no repeated capacity/FOK/business scan,
prepared-vector growth, or authoritative-index allocation; semantically
mutation-free stale/foreign rejection with lease return; WAL-before-state
ordering. Prepared tokens are operational memory objects and are not
persistence semantics.

**Falsification probe.** Prepare through an immutable reference, capture
pool/vector and index capacities/pointers, and prove commit/finalization retain
them through identity churn; insert an unrelated command; commit the exact
command through another path; reuse the same command ID with different content;
pass a token to another structurally identical book; exercise
accepted/core-rejected/risk-rejected commands through durable recovery and
checkpoint replay. Any preparation-time semantic mutation, foreign/stale
semantic mutation, lost lease, second effect, repeated preflight,
prepared-vector/index growth, state-before-WAL transition, or token
interpretation after restart falsifies A48.

## A49 — shared event-trace storage

**Assumption.** An `ExecutionReport` event sequence is immutable shared
content. A live `EventTrace` stores an `Arc<EventArena>` plus exact
start/length; report/cache/checkpoint/replay clones share that arena. Decoded
and caller-built traces use an owned `Arc<Vec<Event>>` fallback. Explicit
diagnostic mutation detaches either backing into `Arc<Vec<Event>>`
copy-on-write storage and cannot change another owner. Binary encoding is
defined solely by ordered event values, never backing kind, range, vector
capacity, or reference count.

**Dependent results.** Live builder finalization and event-trace cloning are
`O(1)` and allocation-free; no event-buffer allocation/copy occurs during
matching, finalization, idempotency insertion, exact retry, or in-memory
checkpoint cloning. `E` insertions/iteration/encoding remain `O(E)`, indexing
is `O(1)`, and the trace remains `Send + Sync`. Arena-backed traces are
intentionally not a contiguous `&[Event]`; decoded fallback storage is
contiguous.

**Falsification probe.** Capture the next uninitialized arena slot and prove
finalization publishes that exact event address; assert owner/range sharing
between first response, cache, and retries; mutate one response through
copy-on-write and byte-compare the next replay; round-trip every event variant
and stable fixture; exercise malformed reports, market-data corruption,
cancellations, raw/segmented replay, matching/risk checkpoints, and concurrent
send/share checks. Any rewrite of a published slot, cache mutation through
another handle, non-`O(1)` clone, wire-byte change, order divergence, reference
cycle, or non-thread-safe trace falsifies A49.

## A50 — per-command event and trade bounds

**Assumption.** For a resting fully displayed or fully hidden order, one future
maker/STP interaction event remains. For a reserve order with total leaves
`L`, displayed leaves `D`, and peak `p`, `s = 1 + ceil((L - D) / p)` future
displayed slices remain and `e = 2s - 1` interaction/refresh event units are
sufficient.
Per-level, per-side, and per-account/per-side sums of `e` are redundant
mutation-maintained state. All admitted quantities and display peaks are
positive multiples of the instrument lot increment.

**Dependent results.** Preparation computes safe incoming-matching event and
trade bounds in `O(1)` without a book scan. An expiry sweep counts only its
ordered `K`-order prefix in `O(K + 1)`. A stop-trigger sweep visits its bounded
eligible prefix and sums one conservative incoming bound per activation before
mutation. For incoming quantity `Q` and increment `q`, at most `Q/q` consuming
interactions occur; cancel-resting
additionally emits two events per opposite self order, cancel-aggressor at most
two at its first self encounter, cancel-both at most three, and
decrement-and-cancel consumes the total side work bound.
Rejection/cancel/retained-priority replacement are one event; post-only
acceptance is two; mass cancel, expiry sweep, and block-and-cancel are `K + 1`.
A stop activation adds one trigger event, its ordinary execution bound, and one
sweep completion shared across the batch.
Sequence/trade/per-report/total-event exhaustion is checked against these
bounds; an event push beyond the prepared maximum is an invariant failure.
Conservative aggregates can reject early at a boundary, but only actual
committed events advance the arena cursor.

**Falsification probe.** Differentially execute at least 20,000 generated books
across both sides, multiple prices, full/reserve/fully hidden orders, partial
displayed slices, all five TIF modes, and all four STP modes; assert actual
event/trade counts never exceed preparation, retained-event count equals exact
cached coverage, and complete invariant validation passes. Independently
generate at least 20,000 non-priority replacements. Exercise final
sequence/event slots,
reserve refresh, partial/full fill, replacement, cancellation, STP, stop-
market/limit activation, multi-stop batches, WAL/checkpoint recovery, and
deliberate aggregate/range corruption. Any bound
overrun, arena overwrite/growth, retained-count drift, false acceptance past
identifier exhaustion, stale aggregate not detected by audit, or
quantity/display state outside the instrument grid falsifies A50.

## A51 — retained-event capacity lanes

**Assumption.** `max_report_events` and `max_retained_events` are authoritative
operational bounds. `max_report_events >= max_active_orders + 1`. The protected
total-event tail is `max_active_orders + 1`; ordinary capacity is
`max_retained_events - (max_active_orders + 1)` and covers both
`max_report_events` and `2 × max_active_orders`. Every durable submission uses
the `PreparedCommand` created before append. Constructor
[`Vec::try_reserve_exact`](https://doc.rust-lang.org/1.85.0/std/vec/struct.Vec.html#method.try_reserve_exact)
failure is `CapacityReservationFailed(RetainedEvents)`; arena slots use
[`OnceLock`](https://doc.rust-lang.org/1.85.0/std/sync/struct.OnceLock.html).
The arena `Arc` control block and checkpoint/snapshot ownership remain under
A12; codec/WAL buffers are A80/A81/A82.

**Dependent results.** A command whose safe A50 bound exceeds its per-report or
permitted total lane fails without sequence, matching, risk, or WAL mutation.
Preparation is storage-neutral; stale/foreign/exact-race tokens consume no
slot. Commit initializes only actual events. Checkpoint restoration rejects
either an overlarge report or total and rebinds accepted values into its new
arena; raw replay re-applies the same gates. Constructor memory is
`max_retained_events × size_of::<OnceLock<Event>>()` plus vector/Arc overhead
before allocator rounding.

**Falsification probe.** Validate zero, overflow, reserve, per-report,
active-population, ordinary, total, default, and `usize::MAX` cases; force
deterministic arena layout failure; fill the ordinary event lane, admit only
valid protected cancellations and expiry sweeps, reject stop-trigger work from
the protected tail, exhaust total capacity, and retry exact content;
prepare competing tokens and prove stale commit consumes nothing; compare
state, sequence, arena telemetry, and WAL length on failure; restore
direct/risk checkpoints and raw/segmented WAL under equal/larger/lower totals.
Any accepted over-bound trace, invalid reserve use, changed
state/sequence/WAL/cursor on failure, post-WAL arena reservation, slot rewrite,
or undersized recovery acceptance falsifies A51.

## A52 — bounded dense hash indexes

**Assumption.** Every authoritative matching/risk/auction hash index stores
values densely up to one immutable semantic maximum and initializes a
power-of-two linear-probing bucket array of at least `2 × maximum` before state
exists. `RandomState` hashes are process-local and nonsemantic under A10.
Removal backward-shifts the affected probe cluster, then dense `swap_remove`
repairs the moved entry's bucket index. At least half the buckets remain empty,
so unsuccessful lookup terminates. A continuous mass-cancel or block-and-
cancel selection and a call-auction mass cancel each contain exactly the
mutation-maintained account/scope count `K`.

**Dependent results.** Constructor failure is typed to the exact index.
Successful construction owns all dense and bucket storage; insertion,
replacement, lookup, clear, and removal cannot grow or rehash. Dense iteration
is `O(N)`; ordinary hash work is expected `O(1)`, while an adversarial single
collision cluster is `O(N)`. Continuous preparation uses `&self`; event storage
is already resident, and a non-empty selection acquires an A87 constructor-
owned lease. The auction engine instead owns one constructor-reserved
`max_active_orders` snapshot vector. Continuous mass cancellation and block-
and-cancel remain `O(K(log K + log P))`; call-auction mass cancellation is
`O(K(log K + log O + log P))`. Both use `O(K)` reserved scratch.

**Falsification probe.** Force every key to one hash value; fill, replace, and
remove cluster head/middle/tail across wraparound; differentially compare at
least 100,000 generated operations with `HashMap`; assert capacities after
every operation. Exercise all five matching indexes through sustained
commit/cancel/different-key reuse, full bounds, maker release, replacement,
rejection, checkpoint/WAL restoration, and non-empty continuous/auction mass
cancel or block. Force
`usize::MAX` layout failure. Any lookup/model divergence, nonterminating probe,
entry/bucket mismatch, capacity movement, constructor success below the limit,
incorrect `K`, preparation semantic mutation, untyped lease exhaustion, or
untyped failure falsifies A52.

## A53 — one-to-one risk reservations

**Assumption.** Coupled risk reservations are one-to-one with active book
orders under A20 and bounded by `max_active_orders`. The fixed dense
reservation map owns that complete entry/bucket layout during construction and
checkpoint restoration under A52. New-order trace application removes maker
reservations before inserting any residual; replacement and partial decrement
remove their existing reservation before reinsertion. At a full active bound,
A47 proves that a resting residual follows at least one maker removal. The
engine stores and audits the configured reservation maximum independently of
current cardinality.

**Dependent results.** Failure is typed as
`CapacityReservationFailed(RiskReservations)` before a shard exists.
Risk-managed preparation uses `&self` and cannot mutate the reservation map.
Authorization and trace application remain expected `O(1)` per relevant event
and allocate no reservation-index storage under arbitrary bounded identity
churn.

**Falsification probe.** Construct at a finite maximum and prove dense capacity
at least the limit; prepare through an immutable reference with a missing
profile, register it, commit, and verify semantic state. Force `usize::MAX`
layout failure. Run sustained maker removal/residual insertion, full/partial
fills, every TIF/STP mode, replacement, recovery, and checkpoint restoration
while asserting unchanged capacity. Constructor success below the limit,
missing typed failure, profile-registration race, reservation cardinality
divergence, capacity movement, or insertion before required removal falsifies
A53.

## A54 — derived account-list topology

**Assumption.** Account-list topology is derived process state, not
matching/WAL semantics. `RestingOrder` and `AccountSideIndex` equality
therefore exclude account link endpoints while retaining economic state, price
FIFO links, counts, and event-work aggregates. Validation independently proves
ownership, side, uniqueness/acyclicity, bidirectional links, head/tail, counts,
and aggregates. Mass cancellation gathers exactly `K` unique members and uses
allocation-free in-place [Rust 1.85
`sort_unstable`](https://doc.rust-lang.org/1.85.0/std/primitive.slice.html#method.sort_unstable);
unique `OrderId` values make instability semantically irrelevant.

**Dependent results.** Direct checkpoint restoration and WAL replay may rebuild
a different valid account-list order while remaining semantically equal.
Ordinary membership mutation is `O(1)`; canonical mass-cancel selection is `O(K
log K)` time with no buffer growth after construction under A87.

**Falsification probe.** Restore the same semantic state from differently
ordered checkpoint rows and replay paths, validate both topologies, and compare
state; corrupt each link/end/count/aggregate dimension; exercise nonmonotonic
IDs on both sides and assert ascending output. Equality that depends on
reconstructed account-list order, undetected topology corruption,
post-construction buffer growth, or noncanonical output falsifies A54.

## A55 — indexed AVL matching arenas

**Assumption.** The execution-price, public-price, GTD-expiry, and buy/sell
stop-trigger implementations follow the
height-balanced search-tree invariant introduced by Adel'son-Vel'skii and
Landis ([primary paper,
1962](https://www.mathnet.ru/php/archive.phtml?jrnid=dan&option_lang=eng&paperid=26964&wshow=paper)):
every occupied node has strict binary-search ordering and left/right height
difference at most one. Nodes live in a stable-index `Vec` arena whose complete
per-side limit is established by [Rust 1.85
`Vec::try_reserve_exact`](https://doc.rust-lang.org/1.85.0/std/vec/struct.Vec.html#method.try_reserve_exact)
during construction; removed slots form an intrusive free list. Rotations and
two-child deletion relink nodes without moving a surviving key/value. Arena
topology, slot identity, and free-list order are derived state and excluded
from semantic equality/WAL/checkpoints.

**Dependent results.** Worst-case `O(log(P + 1))` lookup, insert, delete,
predecessor, and successor; `O(P)` ordered traversal; A83 expected-key handles
provide `O(1)` direct value access until their key is removed; no heap
allocation after book construction for price-level mutation; deterministic
rebuild under different insertion orders. An inclusive range initializes in
`O(log(P + 1))`, visits `K` in-range occupied keys in `O(K)` additional time,
supports double-ended traversal, and uses only the existing fixed stacks. The
structural auditor validates
every child reference once, requires exactly `P - 1` tree edges, resolves every
occupied key to its exact stable slot, and then validates reachability,
ordering, balance, cached height, and free-list coverage in `O(P log(P + 1))`
time and `O(1)` auxiliary space without allocating. Price-arena memory
reservation is `2 P_max S_level + 2 P_max S_public` bytes plus four vector
headers, where `S_level` is one execution-price/`PriceLevel` slot and
`S_public` one public-price/unit slot. The expiry arena adds
`O_max × S_expiry`, where `S_expiry` is the target ABI size of one
`(deadline, OrderId)` index slot. The two stop
arenas add at most `2 × O_max × S_stop`, where `S_stop` is the target ABI slot
size; all byte bounds remain subject to allocator rounding.
Double-ended traversal uses a fixed `256 ×
size_of::<usize>()`-byte stack (2,048 B on a 64-bit target).

**Falsification probe.** Exercise LL/RR/LR/RL rotations; leaf, one-child, and
physical two-child deletion while retaining every surviving handle; differently
keyed slot reuse; zero-comparison handle access; deletion/reinsertion at full
initialized length; forward/reverse iteration; inclusive, outside, and
inverted ranges; mixed front/back consumption; fused exhaustion; a narrow-
range comparison bound; replacement;
topology-independent equality; unrepresentable constructor reservation; direct
height/order/root-reachable and disconnected
cycle/shared-child/reachability/free-list corruption; and at least 20,000
deterministic insert/remove operations and inclusive ranges differentially
against `BTreeMap`, validating after every mutation. Any moved surviving
key/value, model
divergence, balance/order violation, unreachable/duplicate slot,
post-construction arena growth, noncanonical or duplicate range traversal,
topology-dependent
semantic equality, validation scratch allocation, or untyped reservation
failure falsifies A55.

## A56 — bounded profile registry

**Assumption.** `RiskManagedLimits.max_registered_accounts` is a non-zero
finite operational bound independent of matching active-account capacity but no
greater than matching account-control capacity. The fixed dense profile index
owns that complete entry/bucket layout before the shard exists under A52.
Profiles are immutable values; the registry admits unique accounts only before
the first command/report pair is sequenced. Durable open validates the supplied
count and fallibly reserves its canonical sorting vector before touching the
WAL path. Checkpoints omit operational limits and must fit the policy selected
at restoration.

**Dependent results.** Registration is expected `O(1)`, cannot allocate or
rehash, checks duplicate identity before capacity, and returns typed
full/locked errors without state change. Every profiled account can retain one
never-evicted control revision. A coupled shard's dense entry maxima total `H =
2O_max + A_max + I_max + T_max + C_max + R_max`; at defaults, `H = 2(4,096) +
4,096 + 4(65,536) = 274,432`. The corresponding initialized lookup layouts
total 548,864 bucket slots because each listed power-of-two maximum receives
twice as many buckets. ABI byte size and allocator-reserved capacity remain
target-specific. Direct and durable post-start profile-set drift is impossible
through the supported API/WAL grammar.

**Falsification probe.** Reject zero and `usize::MAX` bounds and matching
control capacity below `R_max`; inspect configured/allocated/occupied
telemetry; fill to `R_max`, retry a duplicate, exceed with a new account,
submit the first accepted or rejected command, then attempt late registration.
Prepare the first command with a missing profile, register within the
unsequenced split window, and commit. Deliberately shrink profile capacity and
require invariant failure. Restore a two-profile checkpoint under one and two
slots. Open a two-profile durable shard under one slot and prove no WAL path
was created.

Exercise canonical sorting, duplicate metadata, prefix completion,
raw/segmented/checkpoint recovery, forced collision, and allocator fault
injection. Any allocation/capacity change during registration, wrong error
precedence, late mutation, undersized restoration, WAL creation on
profile/control-capacity failure, or profile drift falsifies A56.

## A57 — account admission fences

**Assumption.** Each account admission fence is local to one serialized
instrument shard under A3. An absent entry means `Enabled` at revision `0`;
every accepted `BlockAndCancel` or `Enable` command must present the exact
current revision, increments it once without wrap, and retains the resulting
`(state, revision)` forever within the shard generation. `BlockAndCancel`
selects all active account orders canonically, cancels them and applies
`Blocked` as one execution report; `Enable` changes only the fence. Numerical
risk profiles remain immutable under A18. Authentication and authorization of
the controller are external inputs; coupled risk requires the target profile to
exist.

**Dependent results.** Deterministic, idempotent, version-12
WAL/version-12-checkpoint-recoverable local admission control; no active order
survives an accepted block; blocked new/replacement admission fails while
cancellation remains available. For `K` selected orders, block is `O(K(log K +
log P))` time and `O(K)` constructor-leased scratch under A87; enable is
expected `O(1)`. Market data privately validates state/revision/cancellation
aggregates while publishing no account identity.

**Falsification probe.** Fill and empty account scopes; exercise exact retry,
command-ID collision, stale and `u64::MAX` revisions, prepared-token staleness,
ordinary-history exhaustion/protected-lane admission, control-map capacity,
unknown profile, blocked new/replace and allowed cancel,
direct/durable/checkpoint/cutover/dangling-command recovery, expired-version
rejection, and market-data prior-state/revision/aggregate corruption. Any
partial cancellation/fence transition, active blocked-account order, second
retry effect, revision gap/wrap, post-construction selection growth, lost
lease, unaudited recovery divergence, or public account disclosure falsifies
A57. Cross-instrument atomicity or authenticated remote administration would
falsify the local-input boundary and require a separately sequenced
coordinator/protocol.

## A58 — checkpoint cutover protocol

**Assumption.** Checkpoint cutover runs under one retained canonical
WAL/manager lease on a qualified local namespace. It synchronizes the inactive
A/B snapshot slot first. Single-file storage then exclusively creates and
synchronizes `<wal>.cutover.pending` containing the anchor plus any retained
suffix, atomically renames it over the WAL, and synchronizes the parent.
Segmented storage exclusively creates and synchronizes one or more bounded
next-generation anchor-plus-suffix segments, then exclusively writes and
synchronizes `format.qseg.pending`, atomically renames that CRC-32C-protected
selector over `format.qseg`, and synchronizes the directory before removing
inactive generations. All layout and checkpoint paths are dedicated and
externally immutable under A13/A24/A26/A28. CRC-32C is non-authenticating under
A14.

**Dependent results.** A crash exposes either the complete prior selected WAL
with any slot it selected, or the complete anchor-plus-suffix/new-slot pair;
segmented readers select one marker generation and never combine inventories.
Reopen scans only anchor plus suffix; repeated cutover never overwrites the
currently selected slot; semantic checkpoint generation, physical WAL sequence,
and segmented inventory generation remain distinct.

**Falsification probe.** Cut over matching, coupled risk, ledger, plain
auction, and coupled auction/risk repeatedly through A/B in both layouts; use
non-default ledger sequence origins; terminate before/after snapshot
publication, anchor/suffix write/barriers/name publication, selector rename,
directory barrier, and cleanup; inject missing/corrupt/wrong same-generation
slots, occupied staging, marker checksum faults, invalid selected generations,
inactive artifacts, generation/sequence exhaustion, missing checkpoint context,
path aliases, external path mutation, recomputed CRCs, remount, and power loss.
Any guessed alternate slot, accepted anchor mismatch, lost acknowledged suffix,
current-slot overwrite before selection, concurrent writer, cross-generation
frame mixing, or cleanup before selected-generation validation falsifies A58.
Qualified power-loss evidence remains required under A24.

## A59 — trading-state transitions

**Assumption.** One serialized instrument shard has one effective
`TradingState` initialized from its immutable definition at revision `0`. An
accepted `TradingStateControl` presents the exact current revision, increments
once without wrap, and targets a different state. `Transition` retains orders;
`TransitionAndCancel` is prohibited when targeting `Open` and otherwise cancels
every active order in ascending `OrderId` order before the final state event.
Any state may transition directly to any different state because calendars,
session identifiers, controller authentication, and venue transition graphs are
external and currently unrepresented.

**Dependent results.** Deterministic entry gating with cancel/control
availability in every state; exact retry; version-12 WAL and version-12
checkpoint reconstruction; account-independent risk bypass with
cancellation-derived reservation release; public market-data state/revision
recovery. For `O` active orders, transition-and-cancel is `O(O log O + O log
P)` time and `O(O)` constructor-leased scratch under A87; transition-only is
`O(1)`.

**Falsification probe.** Exercise all source/target pairs,
stale/unchanged/`u64::MAX` revisions, exact retry and collision, prepared-token
staleness, zero/maximum active orders, canonical cancellation order and totals,
protected-history admission, risk reservations across accounts, WAL reopen,
checkpoint codec/restore/cutover, malformed event grammar, public
update/snapshot revision gaps, and non-open new/replace versus allowed
cancel/control. Any partial cancellation, noncanonical trace, surviving order
immediately after cancel-all, double revision advance, state not derived on
recovery, lost lease, risk/public divergence, or accepted cancel-all into
`Open` falsifies A59. A calendar-, session-, auction-, or volatility-triggered
interpretation would falsify the external-input boundary and requires explicit
sequenced fields and transition rules.

## A60 — auction clearing-price discovery

**Assumption.** The call-auction caller supplies complete eligible aggregate
limit interest as positive lot quantities at canonical, strictly ordered prices
on an authoritative zero-anchored tick grid; zero-or-positive aggregate market
interest; an authoritative aligned inclusive candidate-price band; an aligned
reference price that may lie outside that band; and an explicit ranking policy.
Limits may lie outside the band and contribute only where executable. The
kernel does not infer eligibility, market quantities, displayed/hidden
treatment, reference, static/dynamic band, session phase, or venue rules from
the continuous book.

**Dependent results.** [A60] Clearing-price discovery alone is deterministic
and allocation-free: for `B` bid and `A` ask levels it initializes
market-plus-limit state at the band minimum and evaluates every in-band
constant demand/supply interval in `O(B + A)` time and `O(1)` auxiliary space
using checked `u128` lot aggregates. The zero-market convenience entry point is
the same algorithm over the grid's full representable band. [A60] It returns
the selected aggregate state or no price; it does not derive eligible interest
or a band, admit crossed interest, allocate or execute fills, mutate risk,
publish events, or provide durability.

**Falsification probe.** Differentially compare every policy with exhaustive
bounded tick-grid enumeration, including market-only and mixed interest, zero
and `u128::MAX` market totals, unquoted optimum prices,
single-tick/full/signed-extreme bands, outside-band levels and references,
band/grid mismatch, duplicate/unordered/off-grid levels, aggregate exhaustion,
and both imbalance directions. Count allocations and import venue-certified
eligibility, reference, threshold/collar, tie-break, allocation, and execution
fixtures. Any missed in-band optimum, out-of-band result, arithmetic wrap,
input-dependent allocation, disagreement between bounded and exhaustive
discovery, or attempt to interpret a caller-supplied band as a derived venue
control falsifies A60 or its integration boundary.

## A61 — price-time allocation plans

**Assumption.** The order-level allocation caller supplies the complete order
population corresponding to an A60 clearing result under A4 identity uniqueness
and A1 lot/grid authority. Market constraints lead each side; limit constraints
are best-to-worst; equal constraints are strictly
class/priority-sequence/`OrderId` ordered. Priority class semantics,
aggregation of market and limit orders into A60, displayed/hidden treatment,
and order eligibility remain authoritative caller inputs.

**Dependent results.** [A61] The price-time allocator independently
reconstructs eligible `u128` totals at the selected price, including every
supplied market order, requires exact equality with discovery, and emits
positive source-indexed fills on each side summing to the same non-zero
executable quantity. With `B` buys, `A` sells, and `F_b + F_a` fills, it is
`O(B + A)` time and `O(F_b + F_a)` result space; fill-vector capacity requests
use the exact derived cardinalities and occur before output construction, while
the allocator may grant more capacity. [A61] The plan has no counterparty,
self-trade, trade-ID, mutation, event, risk, market-data, or durability
semantics.

**Falsification probe.** Reorder market/price/class/time/ID dimensions
independently; inject off-grid limits, mixed market/limit aggregate mismatch,
zero execution, finite-limit exhaustion, allocation failure, duplicate
identities at the owning book, ineligible tails, partial final orders, and
totals above `u64::MAX`. Reconcile mixed market/limit discovery directly to
allocation; differentially compare at least 20,000 generated plans with a
literal priority walk and sum every side in `u128`. Any noncanonical
acceptance, zero/over-target fill, side-total divergence, allocation after a
failed reservation, or interpretation as an executed uncross falsifies A61.
A selected `ProRataTime` policy is governed separately by A110. Venue size-
ranking, hybrid, displayed/non-displayed, imbalance, and self-trade fixtures
falsify price-time universality and require explicit versioned policies.

## A62 — call-auction collection book

**Assumption.** One `CallAuctionBook` is single-writer state for one immutable
instrument version and one controller-owned collection phase. It admits only
fully active market or limit quantity, assigns one internal strict arrival
sequence, retains one authoritative A111 priority class, permits locked/crossed
limit interest,
validates route/version/tick/collar/lot rules, and deliberately does not
interpret the definition's continuous `TradingState` as auction-session
authority. Canceled `OrderId` values remain consumed. A64 supplies the
sequenced controller and A65 its local durable wrapper; the reference, optional
dynamic candidate band, and external controller authority remain caller inputs.
One explicit cancel/replace operation removes an owned active target and admits
a distinct never-used identity atomically. Both IDs remain consumed, the
replacement receives a fresh arrival sequence with complete priority loss, and
the book revision advances once. This is not an in-place amendment.
Every active order also belongs to one bounded owner/side intrusive lane with
exact count and `u128` quantity. Account/side mass cancellation selects only
that lane, returns snapshots in ascending `OrderId` order, and advances the book
revision once exactly for a non-empty selection.
Read-only account/side order-ID extraction reserves the selected lane count,
validates the same ownership, side, link, count, and quantity state, and returns
ascending `OrderId` values without mutation. Allocation and topology failures
remain typed; an unknown account returns an empty vector.
Aggregate limit-depth inspection excludes market-constrained interest and
returns bids descending or asks ascending. `limit_depth_iter` borrows the book
and exposes a double-ended exact-size iterator without allocating output;
`limit_level` and `best_limit_level` return one copied aggregate without output
allocation. `try_limit_depth` reserves at most the lesser of occupied side
prices and the requested limit before copying, returns typed reservation
failure without partial output, and changes no book state.
`limit_depth_range_iter` applies the same market order to exact inclusive
endpoints using the shared AVL band traversal; an inverted range is empty.
`try_limit_depth_range` counts selected rows without allocation, reserves that
exact semantic cardinality, and copies through a second identical traversal.
One retained-priority amendment accepts only a strictly smaller positive,
lot-aligned active quantity. It preserves identity, owner, side, constraint,
price, queue links, priority class, and priority sequence; changes queue and
owner aggregates by the exact delta; changes no count; and advances the book
revision once.

**Dependent results.** [A41, A52, A62] Constructor-reserved stable-slot AVL
arenas bound active orders, accepted identities, and per-side prices; one
bounded account hash owns the complete owner index. Admission, amendment,
replacement, cancellation, and mass cancellation allocate no heap storage
under arbitrary bounded identity churn. Replacement preflight includes the
target's released active slot and singleton price-level slot, while
accepted-ID headroom remains monotonic.
Market then price then class then FIFO input is reconstructed deterministically for
A60/A61/A110. Every indicative result is bound to a process-local book identity
and exact mutation revision; foreign/stale allocation is rejected before order
reconstruction. Admission is `O(log I + log O + log P)`, cancellation is `O(log
O + log P)`, indicative discovery is `O(B + A)`, and allocation-input
construction is `O(O log O + P)` because intrusive links name orders by ID.
Replacement is `O(log I + log O + log P)` with `O(1)` auxiliary space.
Amendment is `O(log O + log P)` with `O(1)` auxiliary space. Mass-
cancel preflight is expected `O(1)`; applying `K` selected orders is
`O(K(log K + log O + log P))` using caller-owned reserved output and is
independent of unrelated active orders. Read-only extraction of those `K`
identifiers is expected `O(1) + O(K log K)` time and `O(K)` caller-owned
output, also independent of unrelated active orders. No collection-book state
is mutated. Aggregate direct/best lookup is `O(log(P + 1))` and allocation-
free depth iteration has `O(log(P + 1))` setup, `O(P)` complete traversal, and
`O(1)` auxiliary space. For depth limit `L`, fallible materialization costs
`O(log(P + 1) + min(P, L))` time and `O(min(P, L))` output space; it reserves
before traversal and does not mutate the book.
For `K` selected occupied limit prices inside an inclusive band, range
traversal costs `O(log(P + 1) + K)` time and `O(1)` auxiliary space. Fallible
range materialization makes two traversals with the same asymptotic bound and
owns `O(K)` output after reserving exactly `K` rows.

**Falsification probe.** Exercise routing/version and every instrument
boundary; locked/crossed/market-only interest; middle/head/tail cancellation
and owner mismatch; per-side level, active, and accepted-ID exhaustion; ID
reuse; saturated same-level and singleton-level replacement; account mismatch;
invalid replacement route/quantity; priority loss; atomic rejection; strict
amendment reduction, immutable fields, retained priority, and aggregate delta;
empty/all/side mass cancellation, sparse owners in a full book, output-capacity
and revision exhaustion, account-link corruption, canonical output, one-
revision commit, and fixed allocation telemetry;
empty/all/side read-only account queries, unknown owners, typed allocation
failure, account-link corruption, canonical IDs, and nonmutation;
empty/one/two-sided aggregate depth, direct and missing-level lookup, best
lookup, market-only interest, both market-priority directions, limits `0`, `1`,
occupied cardinality, and `usize::MAX`, iterator/materialized equality, typed
depth allocation failure, inclusive and inverted ranges, forward/reverse range
traversal, exact selected-row materialization, and nonmutation;
foreign/stale indicative results; exact capacity telemetry; 20,000 mixed
insert/remove/amend/replace/mass-cancel operations against independent
aggregate and priority models;
allocator observation under churn; audit every AVL, queue, link, aggregate, and
sequence. Any accepted invalid command, reused ID, lost FIFO relation, model
divergence, accepted foreign/stale plan, heap growth in collection
mutation/scratch, partial replacement, unintended replacement priority
retention, amendment priority loss, or retained-order
mutation by analysis falsifies A62. A venue with additional priority
categories, reserve/display semantics, price/side amendments, quantity
increases, size ranking, or a different pro-rata/hybrid calculation requires a
new versioned policy/state machine.

## A63 — process-local uncross

**Assumption.** One process-local uncross explicitly selects A61 `PriceTime` or
A110 `ProRataTime` allocation, remainder treatment (`RetainAll`,
`CancelMarket`, or `CancelAll`), and self-trade policy (`Permit` or `Abort`).
The selected fills are authoritative. Pairing walks both fill vectors in
canonical order. `Permit` retains a same-account pair as a trade. `Abort`
rejects the complete preparation at the first same-account pair under A113,
without re-pairing or changing the selected fills. Book-local trade identity
and one
collection revision advance only at commit. Positive residual leaves remain lot
aligned and at or below the entry maximum but may be below the new-order
minimum.

**Dependent results.** [A63, A86] Preparation is nonmutating and move-only: it
acquires one isolated constructor-owned buffer set, clears it without changing
capacity, and writes both fills, deterministic pairs, and remainder
cancellations in place. `Abort` failure reports the first canonical account,
buy order, sell order, and positive prevented quantity, then returns the lease.
Foreign/stale commit fails before mutation and dropping
either the preparation or result returns its lease. Same-revision commit
reduces/removes every fill, applies remainder policy, consumes contiguous
`TradeId` values, and advances revision as one allocation-free transition. For
`T` pairs, `C` cancellations, and `M` affected orders, preparation is `O(O log
O + P + F_b + F_a + T)` time with `O(1)` hot-path auxiliary allocation and
commit is `O(M(log O + log P))`; the bounded result storage is charged once to
A86 construction.

The book primitive is not itself a sequenced command, durable event,
risk/ledger transition, or market-data trace. A64 maps A113 abort to one
business rejection, and A65 persists the resulting command/report trace.

**Falsification probe.** Exercise full/partial fills, all three remainder
policies, retained FIFO across auctions, permitted and aborted same-account
pairs, external pairs under `Abort`, head/middle/tail
removal, foreign/stale preparations, exact trade/revision progression,
quantities below entry minimum, constructor reservation failure, and lease
exhaustion/release. Differentially compare at least 10,000 generated pairings
and post-state images with literal two-pointer and remainder models, validating
the book after every commit. Any volume mismatch, noncontiguous trade ID,
priority inversion, wrong self-trade treatment, wrong remainder, partial
mutation on a rejected preparation, commit allocation, capacity movement,
cross-lease contamination, lost lease, or post-state/audit divergence falsifies
A63.

Venue-specific cancel/decrement STP, authenticated beneficial-owner mapping,
alternative pairing, allocation adjustments, or rewinding this matching state
after a bust/correction require separate versioned behavior. A119 supplies only
the downstream ledger correction.

## A64 — sequenced auction phase graph

**Assumption.** One process-local `CallAuctionEngine` and one serialized writer
own one A62/A63 book, one never-evicted command cache, and the exact phase
graph `Closed -> Collecting <-> Frozen -> Closed`, including explicit
`Collecting -> Closed`. `AuctionId` starts at `1` and advances contiguously
when a cycle starts. Every control, new entry, amendment, and replacement
presents the exact phase revision; entry, amendment, and replacement also
present the active `AuctionId`. Submit, amendment, and replacement are valid
only while collecting. An
accepted replacement emits exactly target `OrderCancelled(Replaced)` then
replacement `OrderAccepted` with contiguous event sequences and one book
revision. An accepted amendment emits exactly one `OrderAmended` event with
post-state, previous quantity, and successor book revision, and it uses the
ordinary history lane. Owner cancellation is cycle/revision-independent and
valid in every phase. Account-scoped mass cancellation is also cycle/revision-
independent and valid in every phase; it emits `K` ascending-`OrderId`
`OrderCancelled(MassCancel)` events and one exact aggregate completion,
including the completion for `K = 0`. Uncross requires the exact frozen cycle/
revision; successful uncross closes, an unsuccessful uncross remains frozen,
and explicit close retains interest. A113 `Abort` is one sequenced business
rejection; it leaves phase, phase revision, book, and trade counter unchanged.
Indicative publication requires the exact
instrument version, active cycle, and phase revision and is valid while
collecting or frozen. It observes the current book revision under the explicit
A60 band, reference, and policy and emits one nullable A112 state without
changing the book.

**Dependent results.** [A41, A52, A64, A85, A86, A113] Accepted commands and
business rejections
receive contiguous command/event sequences; operational capacity, allocation,
stale/foreign preparation, and counter failures are unsequenced. Exact retries
precede capacity gates and share immutable report-event storage; different
content under one `CommandId` is rejected. Accepted indication replaces the
retained value; any accepted non-indicative command invalidates it, including
an empty mass cancel. Rejection and exact retry preserve it. A protected lane of at least
`O_max + 2` reports is consumable only by a currently valid individual cancel,
non-empty mass cancel, freeze/close, or executable uncross, and one report
admits at least `2 O_max + 1` events. Empty mass cancellation remains an
ordinary-lane command. Closed-phase cancellation ensures retained interest
remains removable after ordinary-history exhaustion.

Preparation is move-only and semantically nonmutating, although pre-reserved
analytical scratch may be rebuilt; constructor-owned event storage, one
`O_max` mass-cancel snapshot vector, and `P` isolated uncross buffer sets exist
before commit. Phase/rejection/cache work is expected `O(1)`; admission/
amendment/replacement/cancellation/uncross inherit A62/A63 plus `O(T + C)`
trace work.
Accepted replacement adds exactly two-event construction; mass cancellation
adds `K + 1` event construction. Indicative discovery is `O(B + A)` time and
`O(1)` auxiliary space and adds one fixed-size event and one optional retained
state. State is
`O(H_max + E_max + I_max + P O_max + P_max)`. Stable
wire/full-WAL recovery are supplied by A65 and semantic checkpoint/cutover by
A66; coupled risk, market-data projection, and the A116 atomic settlement
adapter consume the report as separate boundaries. Publication transport,
clearing lifecycle policy, calendar, controller authentication, reference/
dynamic-band derivation, and venue conformance do not follow.

**Falsification probe.** Exercise every phase edge and invalid edge,
exact/stale revisions, delayed prior-cycle/reopen submissions, amendments, and
replacements,
empty and crossed indicative publication in collecting and frozen phases,
book/phase invalidation, rejection preservation, and exact retry,
skipped/reused/exhausted `AuctionId`, exact retry/content collision,
foreign/stale prepared tokens, ordinary/terminal history boundaries, malformed
terminal attempts, report-capacity and sequence exhaustion, prepared-uncross
pool exhaustion, empty/non-empty all/side mass cancellation in every phase,
ordinary/terminal-lane selection, empty and non-executable uncross at
`u64::MAX`, A113 abort and exact retry with a retained indication, close with
retained interest followed by closed-phase cancellation,
and event/cache audit corruption. Differentially compare at least 10,000 generated controller
commands with a literal phase model.

Any mutation on rejection, cross-cycle entry, amendment, or replacement,
sequence gap/wrap, second retry effect, consumed reserve from an invalid
command, sequencing or WAL append on pool exhaustion, stranded retained
interest, accepted foreign/stale token, phase/book partial commit, trace grammar
mismatch, stale or misbound indication, or audit divergence falsifies A64.
Multi-process ownership or venue
session semantics require an external fenced/versioned authority; local restart
continuity is bounded by A65/A66.

## A65 — durable auction WAL grammar

**Assumption.** One `DurableCallAuctionEngine` owns the only writer lease for
one WAL-version-20 single-file or marker-selected segmented auction shard. Its
uncut grammar is one immutable definition followed by command/report pairs and
at most one final dangling command. Submission prepares before command append,
commits the same token, then appends the exact report; acknowledgement strength
is selected by journal policy. Exact retries append no frames. Full-WAL open
reconstructs a fresh A64 engine and requires exact report equality; it
completes one final dangling non-retry command but rejects paired or dangling
persisted retries, definition drift, unexpected kinds, report-without-command,
consecutive commands, divergence, capacity failure, and invariant failure.

**Dependent results.** [A15, A65] Stable little-endian auction record kinds
`9`/`10`; replacement command/action tag `4` and `Replaced` cancellation tag
`2`; mass-cancel command/action tag `5`, cancellation tag `3`, and completion
event tag `7`; amendment command/action tag `6`, rejection tag `22`, and event
tag `8`; indicative command/action tag `7` and nullable event tag `9`; uncross
self-trade policy tag `1` and rejection tag `23` under A113;
deterministic restart continuity and exact-retry cache reconstruction in both
physical layouts; no silent second effect or request-attempt logging.
For `C` commands, `E` report events, `B` bytes, and `S` segments, full-WAL open
costs `O(B + S)` framing plus the sum of `C` A64 command costs and `O(E)`
report comparison. A66 may replace prefix replay with verified checkpoint
reconstruction.

**Falsification probe.** Byte-compare every command/event shape and raw kind
tags; reject invalid tags, lengths, booleans, identities, inverted bands, zero
clearing execution, contradictory report grammar, trade self-pairing, and
overflowed cancellation source quantity. Corrupt a trade instrument ID or
definition version independently of its owning definition. Terminate after
definition, command
append, engine commit, partial/full report append, rotation, and sync; repair
only a torn active tail; replay exact state/trade IDs/cache identity; retry
before/after reopen and prove zero frame growth. Inject definition drift,
unexpected records, consecutive/dangling duplicates, persisted `replayed =
true`, report mutation, replacement trace reordering/identity reuse/priority
retention, mass-cancel account/scope/order/count/quantity/revision corruption,
amendment owner/quantity/immutable-field/priority/revision corruption,
indicative auction/phase/book/band/reference/policy/presence corruption,
abort policy/rejection mismatch, segment corruption, insufficient limits, and
frame versions `1` through `18`. Any accepted
divergent/noncanonical history,
duplicate transition, retry frame, cross-layout semantic difference, or
unaudited dangling completion falsifies A65.

## A66 — auction checkpoint lineage

**Assumption.** A snapshot-version-20 call-auction checkpoint and its recovery
WAL represent one immutable A65 command/report lineage. The image retains the
definition/WAL origin, completed report boundary, phase/cycle, book revision,
next priority/trade counters, canonical accepted identities and active orders,
and complete exact-retry history. The current optional indicative state is
derived from accepted history under A112 and is not duplicated in a direct
row. A completed checkpoint is released only after
independent replay requires exact direct-state equality; A97/A98 separate
non-replaying capture from that proof. Numeric generation is never accepted
without exact uncut prefix equality or a kind/checksum/generation/slot-bound
version-20 anchor. Capture/validation resource or temporary-constructor failure
under A78 occurs before snapshot/cutover mutation and leaves the durable shard
unpoisoned; semantic contradiction poisons it.

**Dependent results.** [A41, A66, A97, A98] Direct reconstruction of bounded
AVL/FIFO/account/book/cache state; suffix-only command execution; exact retry after
restore; single-file and segmented A/B prefix retirement; retryable operational
capture failure. Per-cycle projection snapshots quantity before each uncross,
so a later cancellation is reconciled against that cycle rather than original
admission. Cutover bounds WAL bytes scanned and checkpointed command
re-execution, but semantic decode/capture remain proportional to retained
history/events and do not establish bounded checkpoint memory or generation
rollover.

**Falsification probe.** Mutate every direct field, history
command/report/event, replacement grammar and priority, mass-cancel account/
scope/order/totals/revision grammar, phase/cycle, counter,
amendment target/owner/previous-current quantity/priority/revision grammar,
abort policy/rejection grammar and unchanged frozen state, indicative state
grammar, binding, invalidation, and suffix continuation,
accepted/active identity, uncut
prefix frame, metadata origin, definition, and A/B anchor identity. Force every
A78 direct/coupled resource and constructor failure; use a checkpoint ahead of
WAL, same-generation fork, corrupt/wrong slot, path alias, insufficient limits,
partial retained remainder across two auctions, suffix retry, and dangling
suffix command; terminate at every snapshot/barrier/anchor/selector boundary.
Any accepted contradiction, resource-induced poison, snapshot/cutover mutation
after capture failure, cross-lineage restore, guessed slot,
priority/trade/event discontinuity, duplicate effect, remainder misprojection,
or partial cutover falsifies A66.

## A67 — auction market-data projection

**Assumption.** One A64 instrument-version engine is the authoritative source
for one process-local call-auction public projection constructed under an
immutable validated `CallAuctionMarketDataLimits` envelope covering the
engine's configured active-order, per-side price-level, and per-report event
maxima rather than current occupancy. A replica independently selects a finite
envelope and consumes only same-instrument/version snapshots and contiguous
updates under A23. Every non-replayed private event produces one same-sequence
public event; exact retries produce none. Public values omit
account/order/command identity but expose per-event changed quantity, absolute
market/limit aggregates, cycle phase, anonymized pair prints, and final
clearing. Crossed/locked opposing limits are valid. Indicative values are
published under A112 as one complete update containing the exact auction,
phase revision, book revision, explicit reference/band/policy, and optional
clearing. It is valid in collecting or frozen state and changes no book
revision. Any accepted non-indicative command invalidates the retained value;
`NoPublicChange` rejection and an empty exact-retry batch preserve it. One
original A113 abort rejection produces one `NoPublicChange`; its exact retry
produces an empty batch. One
accepted replacement projects one complete command batch of exactly two
updates: anonymized target removal with reason `Replaced`, then replacement
addition with reason `Accepted`. The source and replica book revision advances
once, on the second update.
One accepted mass cancel projects `K` anonymized `MassCancelled` removals and
one `MassCancelCompleted` update in a complete command batch. The completion
exposes exact `u64` count, `u128` quantity, and resulting book revision but no
account or scope. All batch timestamps agree. Replica book revision advances
at completion exactly when `K > 0`; `K = 0` emits only a completion and leaves
the revision unchanged.
One accepted amendment projects exactly one anonymous `Amended` removal with
the positive leaves delta and absolute post-state aggregate. The aggregate
order count is retained, and the replica advances book revision once. The
single-update API accepts this complete one-event command boundary.

**Dependent results.** [A67, A113] Publisher bid/ask stable-slot AVL arenas,
active-order dense hash, and uncross-source dense-hash scratch own complete
storage before bootstrap. Bootstrap and cross-audit use no transient
order/depth collections; successful structural AVL diagnostics are
allocation-free `O(P log P)`. Replica active/standby bid/ask arenas and
batch-level scratch are also constructor-owned. Defaults are 4,096 active
orders, 4,096 limit prices per side, and 8,193 updates per batch; ABI bytes and
page residency remain target-specific. Publication fallibly reserves `O(E)`
output before the first event, then performs expected `O(E + U log P)`
mutation/audit work.

Replica batch cardinality simulation is expected `O(E + U)` before `O(E log P)`
application. Snapshot cardinality failure is nonmutating; accepted snapshot
application fills preallocated standby trees in `O(P log P)` and swaps both
sides atomically. Direct book and replica depth output is `O(min(P,L))`.
Replica full-depth iteration has `O(log(P + 1))` setup, `O(P)` complete
traversal, and `O(1)` auxiliary space. For `K` occupied limit prices in an
inclusive band and `S` selected by the requested limit, replica range iteration
is `O(log(P + 1) + K)` time and `O(1)` auxiliary space; fallible
materialization counts and copies in two passes and owns `O(S)` output after
exact reservation. Market interest remains separate;
publisher bootstrap/cross-audit is expected `O(O + P)`; an uncross report is
expected `O(T + C)` over prints and cancellations, while replacement has
`E = 2`, amendment and indicative publication have `E = 1`, and mass
cancellation has `E = K + 1`. Indicative retention and invalidation are `O(1)`.
Structural failure after incremental mutation poisons state; capacity preflight
failure does not. Stable payload version 5 contains no process-local limit
metadata.

Indicative updates are 84 B without clearing and 124 B with clearing. No
transport, entitlement, cadence, conflation, venue filtering, or
information-hiding guarantee follows.

**Falsification probe.** Exercise market/limit acceptance, user removal,
replacement across sides/constraints/prices at saturated capacity,
retained-priority amendment across market/limit aggregates,
empty/all/side mass cancellation in every phase, anonymized removals, exact
completion totals, timestamp equality, conditional revision, split and
malformed complete batches,
crossed depth, multi-pair uncross, all affected aggregate combinations, exact
retry/rejection, A113 abort no-change plus empty retry, two-cycle retained
remainder, monotonic trade/cycle/revision
state, empty/crossed collecting/frozen indication, invalidation and
preservation, 84 B/124 B update and snapshot codec round trips, gaps, stale
repair, wrong
identity/grid, changed-quantity/absolute-state corruption, and source
divergence. Reject zero, contradictory, undersized-source, and unrepresentable
envelopes with exact resource identity; reject a new full-replica price and
oversized batch without mutation, poison, or scratch residue; reject an
oversized snapshot then apply a valid image while retaining both arena
allocations; run at least 1,000 order/price identities with periodic source
audit, snapshot swaps, and fixed allocation telemetry; deliberately discard
arena/scratch reservations and require invariant rejection.

Any public private identity, sequence hole, duplicate retry effect, accepted
aggregate mismatch, same-cycle multi-price print,
completion-volume/count/revision mismatch, split/reordered replacement batch,
replacement revision mismatch, split/reordered/miscounted mass-cancel batch,
private account/scope disclosure, crossed-depth rejection, accepted
amendment identity/priority disclosure, amended-count drift,
undersized publisher, post-construction state growth, capacity-masked mutation,
scratch residue, stale/misbound indication, non-atomic snapshot replacement,
changed wire bytes, or
recovery divergence falsifies A67. A venue feed requiring indicative imbalance,
order-level publication, a different disclosure schedule, conflation, delay,
filtering, or auction-status codes requires a separately versioned projection.

## A68 — auction risk reservations and netting

**Assumption.** One `CallAuctionRiskManagedEngine` is the sole mutation path
for a risk-controlled A64 shard. Its immutable profile set is bounded and
closes after the first sequenced command. Core auction business/capacity
results precede risk. For replacement, core target/admission preflight precedes
risk; authorization subtracts the owned target reservation before evaluating
the complete replacement under the same immutable account profile, and a risk
rejection preserves the target. Every accepted market or limit order may survive an
uncross and therefore reserves its full active quantity at the maximum
reachable absolute collar magnitude: market `max(|min|,|max|)`, buy limit
`max(|min|,|limit|)`, sell limit `max(|limit|,|max|)`. The same per-lot
valuation remains on partial leaves. Trade events reduce both reservations; an
uncross accumulates buys/sells in `u128` and applies only the net signed
position delta per account, including zero for permitted same-account pairs.
An A113 abort rejection applies no reservation, exposure, position, or netting-
scratch transition.
An accepted replacement trace removes the target reservation before inserting
the replacement reservation. An accepted mass cancel releases each selected
reservation exactly once from its canonical removal events; its completion has
no second risk-state effect and undergoes no numerical entry authorization.
An accepted amendment undergoes no new numerical entry authorization and
reduces its reservation quantity, conservative notional, and account exposure
by the exact positive leaves delta without changing reservation count.
Indicative publication requires no account profile and changes no reservation,
exposure, or position state.

**Dependent results.** [A68, A99, A100, A113]
Missing/blocked/reduce-only/numerical
failures are sequenced stable rejection tags `12`--`21`; exact retries have no
second exposure effect. Profile, reservation, uncross-netting, and
auction-history indexes own complete fixed dense/bucket storage before state
exists. Expected risk work is `O(1)` per submit/amend/replace/cancel event,
`O(K)` for a mass cancel with `K` selected reservations, and `O(T + C)` for
an uncross with `T` pairs and `C` remainder cancellations. Indicative
authorization, indicative trace application, and abort rejection application
are `O(1)` no-ops.
`CallAuctionRiskCheckpoint` canonically retains profiles/exposures plus the
A66-style auction image, reconstructs reservations from active orders, and is
released only after full history replay through the coupled gate; A99/A100
stage that proof.

Snapshot-v18 kind `5` and `DurableCallAuctionRiskEngine` bind a canonical
definition/profile prefix, risk-aware command/report replay, one
dangling-command completion, exact uncut prefix proof, and
single-file/segmented A/B cutover. Dynamic profile/control mutation,
portfolio/collateral/margin models, fees, currency normalization, and
authenticated controller authority do not follow.

**Falsification probe.** Exercise signed zero-crossing collars, market and both
limit sides, every risk rejection, core-versus-risk precedence, net
replacement at saturated one-order capacity, rejected larger replacement,
empty and populated all/side mass cancellation with exact once-only release,
strict amendment with exact reservation/notional/exposure release,
indicative publication without a registered profile and with unchanged risk,
partial/full
fills, all remainder policies, close/cancel, retained multi-cycle interest,
same-account pairs near signed position bounds, reduce-only aggregate sides,
abort before risk mutation with preserved reservations and zero position,
exact retry, stale/foreign preparation, forced hash collisions, index-capacity
stability, and insufficient restore limits. Encode/decode, truncate, corrupt
the metadata origin, rebuild active reservations, continue a suffix, require
independent coupled replay equality, complete only an exact profile prefix,
reject a valid report from another profile set, complete a dangling risk
rejection, distinguish plain/risk grammars, and cut over both physical layouts.

Any active order/reservation mismatch, partial risk replacement, index growth, post-acceptance valuation
decrease, duplicate retry effect, transient self-trade position effect,
noncanonical checkpoint/profile acceptance, replay under-sizing, cross-profile
report acceptance, or risk outcome not reproducible from retained profiles
falsifies A68. Qualified power-loss evidence remains bounded by A8/A24.

## A69 — ledger limits envelope

**Assumption.** One ledger generation is constructed under one immutable
validated `LedgerLimits` envelope. Its non-zero balance keys, retained
transactions, reversal lineages, sequenced records, posting legs per
transaction, transactions per record, and total retained posting legs are
finite independent resources. `Ledger::new` selects the documented defaults;
production construction and all four durable physical/checkpoint layouts may
select explicit limits. Exact retry and content collision resolve before
capacity. Zero balances have no authoritative identity and release their fixed
slot within the same atomic final-state preflight.

**Dependent results.** [A69, A90] Balance, transaction, and reversal maps own
complete dense/bucket layouts; journal order owns the complete record vector.
Default dense maxima total 163,840 and initialized lookup buckets total
327,680, with 65,536 record slots and 262,144 retained posting legs; ABI bytes
and page residency remain target-specific. Single-entry, correction, and batch
preparation reserve all balance-update, overlay, term, and reversal buffers
before mutation. The journal retains the immutable shared `LedgerBatch` value
itself instead of allocating a second transaction-identity vector.

Commit is allocation-free and expected `O(L)` for one entry/correction or
`O(N + U)` for a batch after preparation, with expected `O(1)` hash access.
Durable capacity failure precedes WAL append; restore under lower limits fails
at the first irreconcilable record. History exhaustion requires a separately
fenced generation rollover and external audit/idempotency retention proof.

**Falsification probe.** Reject zero, contradictory, and unrepresentable
limits; independently fill every resource; retry exact content and collide
differing content at exhaustion; atomically replace zeroed balance identities
at a full key bound; exercise single entries, reversals, corrections, and
batches; compare generated batches with an independent balance model; assert
hash/journal allocation telemetry never changes; prove shared batch/posting
identity through commit/checkpoint/restore; restore checkpoints and
full/compacted WALs under equal, larger, and lower limits; byte-check WAL
length after failed durable admission.

Any redundant batch record-identity allocation, post-construction authoritative
growth, zero retained balance, wrong precedence, partial mutation, post-WAL
capacity failure, accepted undersized recovery, retained-posting mismatch, or
replay divergence falsifies A69. `LedgerMagnitude` spill, initial
immutable-value `Arc` construction, input/checkpoint/codec ownership, audit
construction, generation rollover, and external audit retention remain under
A12/A43/A90 and the storage assumptions.

## A70 — continuous market-data envelopes

**Assumption.** One continuous-market-data publisher is constructed under one
immutable validated `MarketDataLimits` envelope that covers the authoritative
matching shard's configured active-order (including dormant-stop), per-side price-level,
account-control, and per-report event maxima—not merely its current
cardinality. One replica independently selects a finite envelope and consumes
only same-instrument/version snapshots and contiguous updates under A21/A23.
The publisher retains fully hidden orders in private state but excludes them
from public quantity/count. A trade from an absent public maker price is valid
only with the canonical zero/zero maker level defined by payload version 3.

**Dependent results.** [A70] Publisher bid/ask and buy/sell stop-trigger AVL
arenas, resting-order/dormant-stop/control dense hashes, and affected-level
scratch own complete storage before bootstrap.
Replica active/standby bid/ask arenas and batch-level scratch are also
constructor-owned. Default maxima are 4,096 active orders, 4,096 prices per
side, 65,536 account controls, and 65,536 updates per batch; ABI bytes and page
residency remain target-specific. Publication fallibly reserves `O(E)` owned
output before the first private-event transition, then performs expected `O(E +
U log P)` validation for `U <= E` unique affected prices.

Replica batch cardinality simulation is expected `O(E)` and mutation is `O(E
log P)`; an insertion may reuse a deletion earlier in the same batch. Snapshot
cardinality failure is nonmutating, while accepted snapshot application fills
preallocated standby trees in `O(P log P)` and swaps both sides atomically.
Structural failure after an incremental mutation still poisons state by design;
capacity preflight failure does not.

Replica full-depth iteration has `O(log(P + 1))` setup, `O(P)` complete
traversal, and `O(1)` auxiliary space. For `K` occupied public prices in an
inclusive band and `S` selected by the requested limit, range iteration is
`O(log(P + 1) + K)` time and `O(1)` auxiliary space; fallible materialization
counts and copies in two passes and owns `O(S)` output after exact reservation.
The inverted range is empty.

**Falsification probe.** Reject zero, contradictory, undersized-source, and
unrepresentable layouts; assert exact resource identity. Fill a replica side,
reject a new price without depth/sequence/poison change, and admit
delete-then-insert replacement at the same bound. Reject a batch above its
update maximum before transition. Reject an oversized snapshot, then apply a
valid image while proving active/standby allocation stability. Run at least
1,000 distinct order/price cycles with periodic publisher cross-audit, snapshot
swaps, and replica structural validation; assert every AVL, dense, bucket, and
scratch allocation remains fixed and scratch is empty between calls.
Exercise fully hidden rest/replace/cancel/trade traces, hidden-only execution
prices, and stop arm/replace/cancel/expiry/trigger traces at full identity
capacity, corrupt canonical trigger order/reference/backlog, and require
publisher rejection without public-depth divergence.

Deliberately discard active-arena and scratch reservations and require
invariant rejection. Any accepted undersized publisher, post-construction state
growth, capacity-masked partial mutation, scratch residue, non-atomic snapshot
replacement, source/replica divergence, accepted lost reservation, or changed
wire bytes falsifies A70. Publisher output ownership, snapshot/depth vectors,
codecs, transport buffering, and adversarial hash complexity remain bounded by
A12/A23 and explicit external interfaces; the equivalent call-auction contract
is A67.

## A71 — instrument catalog generation

**Assumption.** One `InstrumentCatalog` generation is constructed under one
immutable validated envelope bounding registered assets, distinct instruments,
and immutable definitions across all version histories. Asset identifiers and
canonical codes remain bijective. Instrument identity is stable, and version
plus effective time increase strictly within each history.
`InstrumentCatalog::new` uses defaults of 4,096 assets, 16,384 instruments, and
65,536 definitions; production construction reports invalid or unrepresentable
layouts before state exists through
[`Vec::try_reserve_exact`](https://doc.rust-lang.org/1.85.0/std/vec/struct.Vec.html#method.try_reserve_exact)
and bounded-index construction.

**Dependent results.** [A71] The asset, code, and instrument indexes own
complete dense/bucket layouts and all definitions occupy one
constructor-reserved flat arena. Exact duplicate/semantic errors precede
capacity; a rejected registration is nonmutating. Asset and new-instrument
access is expected `O(1)` outside adversarial hash clusters. Version and
effective-time lookup is expected `O(1) + O(log V)` using
[`slice::partition_point`](https://doc.rust-lang.org/1.85.0/std/primitive.slice.html#method.partition_point)
over one contiguous history of `V` versions. An interleaved version append may
shift `O(D)` definitions and rebase `O(I)` instrument ranges; this is bounded
control-plane work and performs no allocation.

The allocation-free structural audit is expected `O(A + D + I²)`. Fixed memory
is `O(A_max + I_max + D_max)`; ABI bytes and page residency remain
target-specific. Generation rollover, authoritative source ingestion, and
signed distribution remain external.

**Falsification probe.** Reject zero, contradictory, and unrepresentable
layouts with exact resource identity. Fill asset, instrument, and global
version resources independently; at exhaustion, retry duplicate and invalid
histories to verify semantic precedence and compare complete pre/post state.
Interleave at least 1,024 definitions across 16 instruments; validate
exact/effective lookups and structural range coverage periodically while
proving all dense, bucket, and arena capacities remain fixed. Corrupt a
reservation and an overflowing private range and require invariant errors
without unchecked arithmetic. Any post-construction growth, partial dual-index
update, range overlap/gap, identity drift, nonmonotonic history, lookup drift
after rebasing, capacity-masked semantic error, or mutation on capacity failure
falsifies A71.

## A72 — allocation-free continuous-book audit

**Assumption.** A successful continuous `OrderBook::validate` audit may use
only immutable book state and fixed stack/scalar traversal; it does not
construct temporary order-identity sets. Every traversed price FIFO entry must
resolve to an order with the same side and price, and every account-list entry
to the same owner and side. Because each price and account/side list is unique,
any repeated identity or cycle within a list necessarily attempts more than the
authoritative active-order cardinality. Each dormant identity instead resolves
to exactly one side-derived stop key. A55 independently proves each execution-
price, public-price, expiry, and stop arena's tree and free-list topology
without scratch.

**Dependent results.** [A72] Successful complete live-book audit is allocation-
free `O(O + P log(P + 1) + X log(X + 1) + S log(S + 1))` time and `O(1)`
auxiliary space for `O` active orders, at most `P` initialized slots per
execution/public price arena, `X <= O` initialized expiry slots, and `S <= O`
initialized stop slots. Exact price/account/expiry/trigger coverage,
forward/backward links, aggregates, accepted identity, account controls,
watermark exclusion, spread, hash layouts, arena reservations, order-selection
pool configuration/capacity, cached extrema/handles, and AVL topology remain
checked. Human-readable formatting after an invariant failure can allocate
under A12.

**Falsification probe.** Build empty, single-level, multi-level, two-sided,
multi-account, reserve, fully hidden, GTD, dormant-stop, triggered-stop,
control, and churned
books and audit after every transition. Inject price-FIFO and account-list self/
multi-node cycles,
duplicate membership, wrong side/price/owner, missing order, broken
prior/next/head/tail, count/quantity/event-work drift, missing/duplicate/wrong
expiry keys, active deadlines at/before the watermark, missing/wrong/duplicate
stop keys, ineligible trigger/reference state, stale best handle, lost
hash/arena/selection-pool reservation, shared AVL children, disconnected
occupied cycles, and unlinked vacant slots. Any successful-audit allocation,
missed corruption, unchecked traversal, panic before typed invariant rejection,
or complexity dependent on configured rather than initialized AVL slots
falsifies A72.

## A73 — matching-checkpoint validation scratch

**Assumption.** Matching-checkpoint semantic validation and selected-limit
admission require duplicate/lookup/cardinality scratch proportional to retained
history `C` and active direct rows `O`; quadratic scans are not substituted for
indexes. Each validation phase constructs only bounded dense/open-addressed
indexes whose semantic maximum equals the corresponding immutable input
cardinality. Capture and validation allocation/layout failure is represented
directly by `OrderBookCheckpointError::ResourceReservationFailed { resource,
maximum }` without constructing an owned diagnostic string; temporary
book-construction failure preserves its `MatchingError`.

**Dependent results.** [A73, A88] History validation reserves command-ID,
accepted-order-ID, account-control, and dormant-stop-lineage indexes through
`C`, plus active-order IDs through `O`, for peak semantic maxima `4C + O`.
Capacity validation
reserves controlled accounts through `C` and active accounts/bid prices/ask
prices through `O`, for `C + 3O`. For `E` retained events, expected validation
is `O(C + E + O)` outside collision clusters; adversarial hash work is bounded
`O(C^2 + E + O^2)`. Reservation failure precedes use of the failed resource and
every restored-book mutation. Capture vectors are A88; codec decoding is A80;
propagated invalid-detail strings remain under A12/A39.

**Falsification probe.** Force `usize::MAX` construction for every
set/map/vector resource and require exact resource/maximum equality. Exercise
empty and maximum semantic histories, duplicate IDs, account-control revision
chains, canonical resting/dormant rows, corrupted trigger priority/reference,
insufficient/equal/larger restoration limits,
full and compacted WALs, A/B cutover, segmented recovery, and coupled-risk
restoration. Any standard growing map/set in matching checkpoint validation,
insertion before that resource's complete reservation, untyped
resource/construction failure, accepted duplicate/capacity contradiction,
state/WAL mutation before failure, recovery divergence, or quadratic
normal-case replacement falsifies A73.

## A74 — allocation-free auction-book audit

**Assumption.** A successful `CallAuctionBook::validate` audit may use only
immutable book state plus fixed stack/scalar traversal; it does not construct a
temporary order-identity set. Every FIFO entry must resolve to the exact side
and constraint, including the exact limit price. Each market side and
side/price limit queue is unique, so cross-queue duplicate membership
contradicts that identity; repetition within one queue is a cycle or count/link
contradiction. A global traversed-count equality proves exact coverage of the
active-order index. Every account/side entry must also resolve to the exact
owner and side; repetition within one owner lane is a cycle or count/link
contradiction, and a second global count proves exact account-index coverage.
A55 independently proves all four indexed-AVL tree/free-list topologies without
scratch.

**Dependent results.** [A74] Successful complete collection-book audit is
allocation-free and uses `O(1)` auxiliary space. For `R` active orders, `I`
accepted identifiers, and `S` initialized slots across the active-order,
accepted-identifier, and two price arenas, queue plus accepted-identity lookup
is `O(R(log R + log I))` and complete arena auditing is `O(S log S)`. Price and
account links, queue/ownership identity, FIFO priority, counts, quantities,
accepted identity,
instrument rules, sequence bounds, finite limits, and constructor reservations
remain checked. Human-readable failure construction can allocate under A12.

**Falsification probe.** Build empty, market-only, one/multiple limit-level,
two-sided, and churned books and audit after every transition. Inject market
and limit FIFO self/multi-node cycles, repeated membership, wrong
side/constraint/price, absent and unqueued orders, broken prior/next/head/tail,
account self/multi-node cycles, wrong owner/side, missing account membership,
account head/tail/count/quantity drift, count/quantity/priority drift, lost
arena/hash reservation, shared AVL children,
disconnected occupied cycles, and unlinked vacant slots. Any successful-audit
allocation, missed corruption, unchecked traversal, panic before typed
invariant rejection, or complexity dependent on configured rather than
initialized slots falsifies A74.

## A75 — chronological auction report history

**Assumption.** `CallAuctionEngine` report history is append-only and never
evicted within one engine generation. The bounded report hash stores dense
entries in insertion order; every new report is inserted only after its exact
next command sequence is assigned, while exact retries do not insert.
Checkpoint semantic validation proves chronological history before restoration
inserts it in that same order. Dense order is therefore an audited engine
invariant rather than an unordered projection.

**Dependent results.** [A75, A120] A successful engine audit checks cache
layout/identity, command and event continuity, report grammar, phase replay,
capacity, and the complete A74 book directly in `O(H + E)` history work and
`O(1)` auxiliary space for `H` retained commands containing `E` events, with no
successful-path heap allocation. Checkpoint capture emits the already-canonical
order without `O(H log H)` sorting; owned checkpoint payload allocation remains
under A12/A66. Failure-detail construction can allocate.

**Falsification probe.** Audit empty history and every
phase/rejection/submit/amend/replace/cancel/mass-cancel/indicative/uncross
grammar; derive the last indication and its invalidation from history; run at
least 10,000 model commands; restore full-WAL and checkpoint histories; mutate
cache keys, command/event sequences, timestamps, grammar, and phase state.
Deliberately remove and reinsert an early bounded-cache entry and require
chronology rejection. Any supported report removal/eviction, insertion outside
exact command order, successful audit of reordered dense history,
successful-path audit allocation, checkpoint order drift, or dependence on
hash bucket order falsifies A75.

## A76 — intrusive auction reservation lists

**Assumption.** Each active call-auction risk reservation belongs to exactly
one registered account and is linked in one intrusive per-account list. Account
head/tail and reservation previous/next identities are private redundant
topology maintained atomically with the existing exposure aggregates; public
`CallAuctionReservationSnapshot` equality remains purely economic. Because
account identity is immutable in a reservation, cross-account duplicate
membership contradicts the owner, while repetition within one account list is a
cycle or link/count contradiction.

**Dependent results.** [A76] Reservation append, partial-fill or amendment
replacement, and removal remain expected `O(1)` and allocation-free in
constructor-owned bounded hashes. A successful risk audit validates hash
resources, valuation/notional, intrusive membership, exact account aggregates,
position limits, quiescent netting scratch, and active-book parity with
expected `O(A + O)` risk work and `O(1)` auxiliary space for `A` accounts and
`O` reservations. The complete coupled audit adds A74/A75; a full adversarial
collision cluster can produce bounded quadratic hash work. Failure-detail and
checkpoint/replay diagnostics remain under A12/A68.

**Falsification probe.** Insert multiple same/different-account market and
limit reservations; partially fill and remove head/middle/tail/final members;
cancel, mass cancel, and retain remainders across cycles; restore checkpoints and durable
suffixes; audit after every transition. Inject self/multi-node cycles, unlinked
reservations, wrong owner, broken previous/next/head/tail,
exposure/count/quantity/notional drift, missing profiles/orders/reservations,
lost hash headroom, and nonquiescent netting scratch. Any membership-node
allocation, missed corruption, post-construction index growth, partial
link/exposure mutation, successful-path audit allocation, or superlinear
expected work outside collision clusters falsifies A76.

## A77 — intrusive continuous reservation lists

**Assumption.** Each active continuous-risk reservation belongs to exactly one
registered account and is linked in one intrusive per-account list. Account
head/tail and reservation previous/next identities are private redundant
topology maintained atomically with exposure aggregates; `ReservationSnapshot`,
checkpoint/WAL bytes, and coupled-state equality remain economic and exclude
link order. Immutable reservation ownership makes cross-account duplicate
membership contradict the owner, while repetition within one account list is a
cycle or link/count contradiction.

**Dependent results.** [A77] Reservation append, partial-fill replacement, and
removal remain expected `O(1)` and allocation-free in constructor-owned bounded
hashes. A successful risk-only audit validates hash resources,
valuation/notional, intrusive membership, exact aggregates, position limits,
and complete coverage with expected `O(A + O)` work and `O(1)` auxiliary space
for `A` accounts and `O` reservations. Complete
`RiskManagedOrderBook::validate` adds the A72 continuous-book bound and an
expected `O(O)` dense-book/risk parity pass. A full adversarial collision
cluster can produce bounded quadratic risk work. Failure-detail and
checkpoint/replay ownership remain under A12/A40.

**Falsification probe.** Insert multiple same/different-account reservations;
partially fill and remove head/middle/tail/final members; exercise reserve
refresh, replacement, every STP mode, individual/mass/control cancellation,
checkpoint restoration, and durable suffix replay; audit after every
transition. Rebuild equivalent reservations in different valid private link
order and require semantic equality. Inject self/multi-node cycles, unlinked
reservations, wrong owner, broken previous/next/head/tail,
aggregate/count/quantity/notional drift, missing profiles/orders/reservations,
and lost hash headroom. Any membership-node allocation, semantic dependence on
topology, missed corruption, post-construction index growth, partial
link/exposure mutation, successful-path audit allocation, or superlinear
expected work outside collision clusters falsifies A77.

## A78 — auction checkpoint capture resources

**Assumption.** Call-auction checkpoint history is immutable input containing
at most one newly accepted order and one unique command identity per retained
command. Acceptance may be a submission or the new identity in a replacement.
Every active projected order and every distinct source order within one uncross
must therefore be drawn from at most `C` accepted identities for `C` retained
commands. Direct accepted IDs and active orders are canonical
`OrderId`-ascending projections; their order is not inferred from hash
iteration. Capture owns exactly `C` history, `O` active-order, and `I`
accepted-identifier rows; coupled risk adds exactly `A` canonical
account/profile/exposure rows.
The call-auction account index is derived from active rows and is not another
checkpoint image.

**Dependent results.** [A78] Each capture vector completes `try_reserve_exact`
for its immutable source cardinality before its first push. Direct capture is
`O(C + O + I)` row-copy work; coupled capture adds `O(A log A)` account sorting
because the bounded account hash has deliberately nonsemantic iteration order.
Semantic validation fallibly reserves four dense/open-addressed scratch indexes
through `C` for projected orders, accepted IDs, command IDs, and reusable
per-uncross source quantities. Selected-limit validation rejects scalar
cardinality excess before reserving two price sets through `O`.

Expected validation is `O(C + E + O)` for `E` events with `O(max(C, O))` peak
auxiliary storage; a full adversarial collision cluster is bounded by `O(C(C +
E) + O²)`. `CallAuctionCheckpointError::ResourceReservationFailed { resource,
maximum }` and the coupled nested error preserve exact capture/validation
identity; temporary constructor failures retain their source. Direct and
coupled restoration borrow immutable checkpoints and do not clone their
embedded vectors. Durable publication has synchronized the WAL but performs no
snapshot/cutover mutation and remains unpoisoned on these operational failures;
semantic `Invalid` contradictions poison.

**Falsification probe.** Force `usize::MAX` construction for all direct/coupled
capture vectors and every scratch-map/set class and require exact
resource/maximum equality. Exercise empty history, nonmonotonic accepted
identities, multiple uncross cycles with retained partials, duplicate
command/order IDs, stale sources, corrupt accepted/active projections, mass-
cancel order/scope/totals/revision contradictions,
lower/equal/larger selected limits, repeated borrowed restoration, full WAL,
and A/B suffix recovery. Any infallible capture `collect`/`with_capacity`,
growing standard validation map/set, dependence on hash iteration order,
insertion beyond a proved bound, full embedded-checkpoint clone, untyped or
flattened reservation/construction failure, resource-induced durable poison,
accepted projection contradiction, or quadratic normal-case replacement
falsifies A78. Snapshot ownership, full-history pause, and authentication
remain under A12/A66.

## A79 — ledger batch overlays and trial balance

**Assumption.** A `LedgerBatch` contains exactly `N` private entries whose
unique transaction IDs were proved at construction; its pending transaction
overlay can contain at most `N` entries and its pending original-to-reversal
overlay at most `N` entries. Reconciliation input is canonical `(asset,
account)` sorted before validation. A live ledger contains no zero balances, so
trial-balance terms are bounded exactly by non-zero balance count `A`, and
distinct asset output count `D` is at most `A`.

**Dependent results.** [A79] Batch construction and preparation fallibly
construct one `N`-bounded identity set and two `N`-bounded dense/open-addressed
maps. Expected identity/overlay work is `O(N)` and adversarial full-collision
work is bounded `O(N²)`; commit remains allocation-free. `try_trial_balance`
fallibly reserves one flat `A`-term vector, sorts it in `O(A log A)`, and emits
an exactly reserved `D`-asset vector using `O(A + D + W)` memory for `W`
spilled magnitude limbs. Reconciliation balance validation streams its sorted
slice in `O(S + W)` without collection scratch. The convenience `trial_balance`
wrapper and magnitude spill remain under A12/A43.

**Falsification probe.** Force `usize::MAX` construction for the identity set,
both overlays, trial terms, and trial output and require exact
`LedgerPreparationResource`/`LedgerQueryResource` failures. Exercise duplicate
IDs, earlier-member reversal/control visibility, partial prior commitment,
generated batches, checkpoint/full-WAL/A/B replay, multiple assets,
zero/duplicate/unbalanced statements, totals above `u128::MAX`, and collision
clusters. Any standard growing map/set in ledger production code, overlay
growth beyond `N`, mutation before all preparation storage exists, noncanonical
output, missed ordered lifecycle dependency, reconciliation collection scratch,
untyped flat-query failure, or commit allocation falsifies A79.

## A80 — decoder cardinality proofs

**Assumption.** Every collection cardinality decoded by the stable payload
codecs is an unsigned `u32` and has a format-specific positive lower bound `m`
bytes per element. For declared count `N` and remaining payload `B`,
`Decoder::count` rejects `N > floor(B / m)` before allocator access. Nested
variable-sized values are first bounded by their enclosing payload slice.

**Dependent results.** [A80] After the byte/cardinality proof,
`reserve_decoded_vec` makes one exact fallible reservation through `N` elements
and maps layout or allocator failure to `CodecError::CapacityReservationFailed
{ field, maximum: N }`. Decoding then takes `O(B)` time and owns `O(N)` result
storage; the input payload remains borrowed. Existing encoded bytes are
unchanged. Encoder growth is covered by A81 and WAL frame payload reads by A82;
continuous matching/risk checkpoint reconstruction is A73/A88, while other
checkpoint semantic-object construction remains under A12.

**Falsification probe.** Replace a valid count by `u32::MAX` in a physically
short payload and require `InvalidLength` before reservation. Force
`usize::MAX` through the shared vector constructor and require exact
field/maximum equality. Round-trip every command, report, market-data image,
journal object, and plain/coupled checkpoint; byte-compare stable fixtures. Any
decoder-side `Vec::with_capacity`, ad hoc reserve without typed maximum,
reservation before the remaining-byte proof, accepted impossible count, changed
supported wire bytes, or noncanonical result falsifies A80.

## A81 — encoder growth contract

**Assumption.** Every stable `BinaryCodec::encode` implementation emits scalars
and byte slices exclusively through the private `Encoder`; no implementation
mutates its output vector directly. For current encoded length `L` and next
write length `A`, the encoder proves `L + A` with checked arithmetic before
access.

**Dependent results.** [A81] When `L + A` exceeds retained capacity, one
`Vec::try_reserve(A)` supplies amortized geometric growth. Address-space
overflow is `EncodingLengthOverflow { current_length: L, additional: A }`;
layout/allocation failure is `EncodingCapacityReservationFailed {
minimum_length: L + A }`. The first failure is retained, later writes are
no-ops, and `finish` returns the error instead of exposing partial bytes.
Successful encoding is amortized `O(B)` time and `O(B)` owned output for `B`
bytes, with stable wire bytes. Nested codec outputs independently preserve the
same contract.

**Falsification probe.** Force checked `1 + usize::MAX` and require exact
operand identity; force an empty encoder to reserve `usize::MAX` and require
the exact minimum length. Run every golden-layout, variant, nested batch,
market-data, and plain/coupled checkpoint round trip in debug and release.
Statically require the only output-vector `try_reserve`/`extend_from_slice`
pair to remain inside `Encoder`, and every codec finish path to return its
fallible result. Any direct encoder-vector write, infallible reserve/growth,
overwritten first failure, successful partial output, superlinear successful
copying, or changed supported wire bytes falsifies A81.

## A82 — WAL frame assembly reservations

**Assumption.** A physical WAL frame has fixed header length `H = 24 B`,
bounded payload length `P`, and total length `F = H + P` proved with checked
`usize`/`u64` arithmetic. A `JournalBatch` contains `R` already-owned payloads
whose total `F_total = sum(H + P_i)` and contiguous sequence/offset successor
are validated before output storage or I/O.

**Dependent results.** [A82] Single-frame append and checkpoint-anchor staging
exactly reserve `F`; batch append exactly reserves `F_total` bytes plus `R`
receipts. Each frame header is built on the stack, checksummed with its
payload, and appended directly to the batch buffer, eliminating `R`
intermediate frame vectors and copies. A segmented batch additionally reserves
`R` wrapped receipts and an inventory slot before any required rotation. Frame
reading validates configured/physical length before exactly reserving `P`.
Every layout/allocation failure is `JournalError::CapacityReservationFailed {
resource, maximum }` before file bytes, logical sequence, offsets, poison
state, or rotation change. Assembly and verification are `O(F_total)` time with
`O(F_total + R)` owned append output; one frame is `O(F)`.

**Falsification probe.** Force `usize::MAX` for frame, batch-frame,
plain/segmented receipt, read-payload, segment-inventory, and batch-record
resources and require exact identity/cardinality. Call reserved-frame assembly
with zero capacity and require `FrameBytes/H`. Exercise golden frame bytes,
heterogeneous batches, partial-write recovery, sequence/offset exhaustion,
payload limits, single/segmented rotation, checkpoint cutover, process
termination, and every durability mode in debug/release. Any
`Vec::with_capacity`, `vec![0; P]`, per-record frame vector, receipt allocation
after write, inventory growth after closing the active segment, allocation
failure that mutates storage/sequence/offset/poison state, checksum drift, or
recovery divergence falsifies A82. Path/string ownership, initial segment
discovery, snapshot-file framing, and OS I/O buffers remain under A12.

## A83 — indexed AVL handles

**Assumption.** An `IndexedAvlHandle` is process-local, internal, and always
dereferenced together with its expected key. Rotations and removal of another
key preserve every surviving slot; two-child deletion physically relinks the
successor node instead of copying its key/value. Removing the addressed key
invalidates the handle, and `PriceLevels` replaces a removed best-level handle
synchronously before returning to matching under the single-writer assumption
A1. Handles are neither stored in resting orders nor persisted in reports,
WALs, or checkpoints.

**Dependent results.** [A83] Array indexing plus one `Price` equality check
makes best-level lookup and mutation by handle `O(1)` without an ordered key
search. Partial maker fills, removal from a level that remains occupied, and
reserve replenishment are `O(1)` price-index work. Replenishment splices the
order to the displayed-class tail in place and updates level/account work
aggregates without deleting/reinserting the price. Empty-level AVL removal and
residual level insertion remain `O(log(P + 1))`. A command consuming `E`
displayed maker slices and exhausting `L <= E` price levels is
`O(E + (L + 1) log(P + 1))`
expected time outside bounded hash-collision costs, with `O(1)` matching
auxiliary space after preparation.

**Falsification probe.** Retain handles through all four rotation shapes and
unrelated leaf/one-child/two-child deletion; require exact key/value access.
Remove and differently reinsert into the same slot and require the old
key/handle pair to fail. Use an `Ord` comparison counter and require zero
ordered comparisons for direct handle reads/writes. Exercise partial/full
makers, sole/multi-order reserve refresh, every STP mode, best/non-best
deletion, checkpoints, recovery, and allocation telemetry; audit after
mutation. Any moved surviving key/value, accepted differently keyed reuse,
stale cached best handle, ordered price search for a non-empty maker mutation,
refresh-level removal/reinsertion, FIFO/aggregate drift, semantic byte change,
or weaker complexity falsifies A83. Same-key handle reuse after explicit
removal is outside the handle lifetime and must never cross a `PriceLevels`
mutation boundary.

## A84 — continuous event arena

**Assumption.** Under the single-writer contract A3, every successful
continuous-book command inserts one report into append-only dense history and
publishes one adjacent event-arena range starting at the prior retained-event
cursor. A safe
[`OnceLock<Event>`](https://doc.rust-lang.org/1.85.0/std/sync/struct.OnceLock.html)
initializes each slot at most once; exact retries, rejected preparations, and
stale/foreign commits insert no report and initialize no slot. History
removal/eviction is unsupported.

**Dependent results.** [A84] Cached live ranges form an exact nonoverlapping
partition `[0, retained_event_count)`. Successful structural audit streams
dense reports in `O(C + E)` time and `O(1)` auxiliary space to verify arena
identity, adjacency, per-report maxima, total coverage, and configured
allocation. Matching report construction performs zero heap allocations after
construction; optional selection scratch is constructor-leased under A87.
Checkpoint restoration copies `E` validated decoded events once into a newly
reserved arena; steady-state replay thereafter shares ranges.

**Falsification probe.** Inspect the next slot before/after commit; prepare
multiple same-generation tokens, commit one, and reject the stale token without
cursor movement; fill ordinary/protected/total event boundaries; exact-retry at
total exhaustion; deep-clone test books and mutate both independently; restore
matching/risk checkpoints and raw/segmented WAL; corrupt backing identity,
start, length, dense order, retained count, and arena reservation and require
audit rejection. Any second initialization, overlapping/gapped live range,
cursor movement without cache insertion, post-construction arena/selection
growth, stale-token consumption, missed range corruption, or semantic replay
divergence falsifies A84. An externally retained live trace keeps the complete
arena `Arc` alive after book drop; bounded retention therefore depends on the
configured arena and consumer lifetime.

## A85 — call-auction event arena

**Assumption.** Under A3/A64, the call-auction engine reserves
`max_retained_events` safe `OnceLock<CallAuctionEvent>` slots at construction.
Each committed report publishes the next adjacent range; preparation is
event-storage-neutral, and foreign/stale/exact-race tokens initialize no slot.
For `O_max` active orders, maximum accepted uncross trace size is
conservatively `2 O_max + 1`; the protected terminal event tail is `2 O_max +
2`, covering one freeze event plus that uncross, and ordinary event capacity is
at least `O_max + 1` to collect a full book after opening its cycle. An accepted
replacement consumes exactly two ordinary event slots; replacement is not a
terminal-lane action. An accepted indicative publication consumes exactly one
ordinary event slot and is not a terminal-lane action. A mass cancel consumes
exactly `K + 1` slots. It may use
the terminal lane only for `K > 0`; `K = 0` remains ordinary. After a non-empty
mass cancel, the remaining tail still covers individual cancellation of every
survivor or one freeze plus the survivors' maximum uncross trace.

**Dependent results.** [A85] Per-report and total bounds are independent. An
ordinary action, including replacement, indicative publication, or empty mass
cancel, whose conservative report bound crosses the ordinary event
watermark fails unsequenced with `AdmissionEventHistory`; a currently valid
cancel, non-empty mass cancel, freeze/close, or executable uncross may use the tail. Total exhaustion
is `RetainedEvents`. Commit charges only the actual trace length. Live report
creation, cloning, retry, cache insertion, and structural audit allocate no
event storage; audit verifies arena identity, exact range adjacency, total
coverage, and configured reservation in `O(C + E)` time and `O(1)` auxiliary
space. Direct checkpoint restoration first rejects an excessive aggregate event
count, then copies each validated event once into the selected arena.

**Falsification probe.** Validate zero/overflow/minimum total and per-report
limits; force constructor arena reservation failure; inspect a slot before
preparation and after commit; prepare foreign/stale/exact-race tokens and
verify cursor stability; fill the ordinary watermark, reject invalid reserve
use, ordinary replacement/indicative publication, and empty mass cancellation;
apply side/all non-
empty mass cancellation, then freeze and uncross survivors; retry without
consumption; restore direct/risk
checkpoints and full/segmented WAL; corrupt arena identity, range order,
retained cursor, and reservation. Any post-construction arena growth, second
slot initialization, range gap/overlap, invalid terminal admission, stale-token
consumption, accepted oversized checkpoint, or replay divergence falsifies A85.

The current default is 73,730 slots; on measured `aarch64-apple-darwin`,
`size_of::<OnceLock<CallAuctionEvent>>() = 192 B`, so slots occupy `14,156,160
B` (`14.156160 MB`) before vector/Arc/allocator overhead. The event itself
remains 176 B; the aligned `OnceLock` slot is the retained-arena capacity term.
Externally retained live traces keep the complete arena alive.

## A86 — uncross buffer pool

**Assumption.** A call-auction book constructs `P = max_prepared_uncrosses`
isolated uncross buffer sets before publishing the book. Each set requests
capacity for `O = max_active_orders` elements independently for buy fills, sell
fills, counterparty trades, and remainder cancellations; granted capacity may
be larger. Each fill side and cancellation count is at most `O`; with positive
fills on both sides, the deterministic two-pointer pair count is at most `O -
1`, so the requested trade bound `O` is conservative. A preparation and its
committed result are move-only views of one lease; consumer retention pins that
lease until `Drop`. The default is `P = 2`.

**Dependent results.** [A86] Pool acquisition/release is expected `O(1)` and
uses one mutex that is uncontended under the A3/A64 serialized-writer contract.
Clearing and refilling leased vectors never changes capacity. Exhausting all
`P` leases returns `PreparationCapacityExhausted { maximum: P }` before command
sequencing, WAL append, event initialization, or authoritative book mutation.
The constructor's minimum element-storage request is `P O (2 S_fill + S_trade +
S_cancel)` bytes before four vector headers per set, the pool vector,
Arc/mutex, allocator over-reservation/rounding, and resident-page effects. On
the measured `aarch64-apple-darwin` build, `S_fill = 24 B`, `S_trade = 72 B`,
and `S_cancel = 56 B`; defaults therefore request at least `2 × 4,096 × (2 × 24
B + 72 B + 56 B) = 1,441,792 B = 1.441792 MB` of element storage. The `Arc`
control-block allocation remains under A12.

**Falsification probe.** Reject `P = 0`; force unrepresentable reservation
independently for every vector class and the pool; assert all reported granted
capacities cover `O`; hold `P` preparations/results concurrently, require typed
exhaustion with unchanged engine sequence/event/WAL/book state, and prove
release on preparation drop, successful commit, stale commit, and result drop.
Prepare different policies simultaneously and require disjoint immutable
outputs. Churn acquire/commit/drop while asserting fixed capacities. Any
post-construction vector growth, cross-lease contamination, reused live buffer,
lost/double lease, allocation during preparation/commit, pool exhaustion after
sequencing or mutation, accepted `P + 1` live leases, or cardinality above `O`
falsifies A86.

Mutex contention/latency outside the serialized-writer assumption, allocator
size classes, page residency, and cache locality require target-specific
measurement.

## A87 — order-selection lease pool

**Assumption.** A continuous `OrderBook` constructs `L =
max_prepared_order_selections` isolated `OrderId` vectors before publishing the
book; each requests `O = max_active_orders` elements and may receive more
capacity. A core-accepted mass cancel, expiry sweep, stop-trigger sweep, block-
and-cancel, or transition-and-cancel with selected cardinality `K > 0` acquires one move-only
lease during immutable preparation. `K <= O` follows from the authoritative
active-order/account/expiry/trigger indexes. A selection with `K = 0` uses an
allocation-free zero-capacity vector and consumes no lease, preserving empty
cancellation/control availability even when all non-empty leases are retained.
The default is `L = 2`.

**Dependent results.** [A48, A52, A87] Pool acquisition/release is expected
`O(1)` through one mutex that is uncontended under A3. Commit fills and sorts
the leased vector in place, drains it, and returns the cleared storage on
successful, exact-race, stale, foreign, gate-rejected, or dropped preparation
paths. Exhausting all `L` non-empty leases returns
`PreparationCapacityExhausted { maximum: L }` before command WAL append, event
initialization, or matching/risk mutation. The constructor's minimum
element-storage request is `L O S_id` bytes before vector headers, the pool
vector, Arc/mutex, allocator over-reservation/rounding, and resident pages. On
measured `aarch64-apple-darwin`, `S_id = size_of::<OrderId>() = 8 B`; defaults
request `2 × 4,096 × 8 B = 65,536 B = 0.065536 MB`. The pool `Arc` control
block remains under A12.

**Falsification probe.** Reject `L = 0`; force independent unrepresentable
buffer and outer-pool reservations with exact resource identity; assert granted
per-lease capacity covers `O`; acquire `L` distinct buffers, write different
identities, and prove no cross-lease contamination; require typed `L + 1`
exhaustion without state/sequence/event/WAL change; prepare empty control,
expiry, and stop-trigger selections while exhausted; prove release on drop,
normal commit, exact race, stale
commit, foreign commit, deterministic gate rejection, and durable retry;
corrupt retained capacity and require audit rejection.

Any post-construction growth, allocation during selection preparation/commit,
empty-selection blockage, live-buffer reuse, lost/double lease, pool error
after WAL/state mutation, cardinality above `O`, or capacity audit omission
falsifies A87. Mutex latency outside A3, allocator size classes, page
residency, and cache locality require target-specific measurement.

## A88 — matching/risk capture vectors

**Assumption.** Continuous matching checkpoint capture owns exactly `C`
chronological `CommandReportCheckpoint` rows, `R` canonical
`RestingOrderCheckpoint` rows, and `S` canonical `DormantStopCheckpoint` rows
for the live retained-command and active-order cardinalities, where
`R + S = O`. Resting rows retain executable working quantity and canonical
displayed-class-before-hidden FIFO order. Each resting or dormant row retains
its optional GTD deadline, while the inclusive expiry watermark, stop
reference, and trigger lineage are reconstructed from chronological history.
Coupled risk capture additionally owns exactly `A`
account/profile/exposure rows. Each vector is empty and completes
`try_reserve_exact` for its immutable source cardinality before its first push;
live arena-backed event traces clone in `O(1)` without allocating or copying
event values under A49/A84. Matching validation/restoration temporary books and
coupled-risk temporary shards use fallible constructors and preserve returned
`MatchingError` values. Coupled restoration borrows the embedded matching
checkpoint and does not clone its vectors.

**Dependent results.** [A39, A73, A88, A93, A95, A96] Capture performs `O(C +
O + A)` row-copy work and owns `O(C + O + A)` output memory before
codec/snapshot framing. Minimum element storage is
`C S_history + R S_order + S S_stop` bytes for direct matching and adds
`A S_account` bytes for coupled risk, before vector headers, allocator rounding,
shared-owner control blocks, and snapshot encoding. Exact ABI sizes and
resident memory are target-dependent. `CaptureHistory`, `CaptureActiveOrders`,
`CaptureDormantStops`, and `CaptureAccounts` failures report the exact requested
cardinality. Durable matching/risk checkpoint orchestration
has already synchronized the WAL but performs no snapshot or cutover mutation
on these failures and leaves the shard unpoisoned; `Invalid` semantic
contradictions remain poison-worthy.

**Falsification probe.** Force `usize::MAX` for all four capture vector
classes and every A73 validation resource; force temporary matching/risk
constructor failures and preserve the nested resource identity; capture empty
and maximum configured live states; verify canonical row order, trace backing
reuse, absence of a full embedded-checkpoint clone, unchanged
book/risk/WAL/snapshot namespaces on failure, retry success, and poison
classification. Any infallible `collect`/`with_capacity` in continuous
matching/risk capture, full matching-checkpoint clone during coupled
restoration, push before complete reservation, event copy, lost returned
construction cause, resource-induced poison, snapshot/cutover mutation after
capture failure, or semantic error left usable falsifies A88.

Temporary arena/pool `Arc` control blocks, snapshot payload ownership,
invalid-detail formatting, history-dependent writer reconstruction, staged
verification, and authentication remain separate boundaries under
A12/A39/A93/A95. Call-auction and ledger capture are A78/A89.

## A89 — ledger checkpoint capture sharing

**Assumption.** One ledger checkpoint retains exactly `R` chronological
`LedgerRecord` values and `A` canonical non-zero `LedgerBalance` rows. The
independently materialized replay-audit record vector is the returned
checkpoint record vector; capture does not materialize it twice. Trial-balance
audit storage and both checkpoint vectors reserve fallibly before their first
push. Under A90, record materialization and replay clones share every immutable
posting/batch vector and allocate no nested storage. Restoration borrows the
immutable checkpoint and clones one allocation-free record handle at a time
rather than cloning both complete vectors.

**Dependent results.** [A69, A89, A90] Capture performs record
materialization/replay linear in retained record and posting content, plus `O(A
log A)` balance and trial sorting, and owns `O(R + A)` new checkpoint rows
before codec/snapshot framing; nested entry/posting graphs remain shared with
live immutable state. `LedgerCheckpointCaptureError` preserves exact record,
balance, trial-term, or trial-output resource/cardinality and temporary
replay-ledger construction failures. Durable publication has synchronized the
WAL but performs no snapshot/cutover mutation and remains unpoisoned for
operational capture failures; live structural/replay contradictions are
`Invalid` and poison. Minimum new element storage is `R S_record + A S_balance`
bytes before vector headers, allocator rounding, snapshot encoding, and
resident pages; retained shared graphs still contribute to total RSS.

**Falsification probe.** Force `usize::MAX` for all four capture/audit vector
classes and every replay constructor resource. Exercise empty and maximum
configured ledgers, entries/corrections/batches, zero-balance identity release,
period state, wide magnitudes, pointer-identity sharing across live
records/capture/restore, repeated borrowed restoration under lower/equal/larger
limits, uncut and anchored single/segmented recovery, retry after operational
failure, and semantic corruption poison. Any nested vector allocation during
in-memory record clone/capture/restore, second record capture vector, full
checkpoint clone during restoration, infallible checkpoint
`collect`/`with_capacity`, push before complete reservation, flattened
resource/constructor failure, resource-induced poison, snapshot/cutover
mutation after capture failure, noncanonical balance order, record/balance
replay divergence, or accepted semantic corruption falsifies A89.

Snapshot encoding ownership, reconciliation/diagnostic output, full-history
pause, authenticated retention, and initial/decoded immutable-value `Arc`
construction remain under A12/A43/A90.

## A90 — immutable ledger value objects

**Assumption.** `JournalEntry` and `LedgerBatch` are immutable value objects
after successful semantic construction. One entry owns canonical postings
through `Arc<Vec<Posting>>`; one batch owns authoritative entry order through
`Arc<Vec<JournalEntry>>`. No API exposes mutable access. Equality and stable
codecs depend only on ordered values, never pointer identity, vector capacity,
or reference count.

**Dependent results.** [A42, A69, A89, A90] Entry, correction, batch,
journal-record, exact-retry, checkpoint, and restoration clones are `O(1)` per
outer value and allocate no nested vectors. Batch commit clones `N` small entry
handles in `O(N)` without allocation and retains the same shared batch in
journal order, eliminating the former `N`-identifier record buffer. Dropping
any owner preserves values while another owner exists. Initial construction and
decoding create one `Arc` control block per entry and batch after the
caller/decoder vector exists; stable Rust exposes no stable fallible
`Arc::new`, so that boundary remains A12. Atomic reference-count increments add
target-dependent contention/cache cost.

**Falsification probe.** Clone entries/batches before and after commit, compare
`Arc::ptr_eq` through public sharing inspectors, drop owners in every order,
capture and restore checkpoints, retry exact entries/batches, send/share values
across threads, and byte-compare all existing codec fixtures. Mutate source
input vectors after construction and prove no effect. Any mutable alias, nested
clone allocation, lost value after owner drop, pointer-dependent
equality/bytes, changed wire fixture, non-`Send`/`Sync` value, redundant
batch-ID vector, or commit allocation falsifies A90. Allocator continuation at
initial/decoded `Arc` construction requires a fallible shared-owner
implementation or stable allocator API.

## A91 — immutable matching checkpoints

**Assumption.** `OrderBookCheckpoint` is an immutable value object. Its
canonical resting-order image, dormant-stop image, and chronological command/
report history are private `Arc<Vec<_>>` values exposed only as immutable
slices; nested live
report events already reference immutable A84 arena ranges. Equality,
lineage-prefix comparison, validation, and stable codecs depend only on ordered
values, never allocation capacity, pointer identity, or reference count.

**Dependent results.** [A39, A73, A84, A88, A91, A92] A direct
matching-checkpoint clone is `O(1)` time/space per outer value and allocates no
row or event storage. Under A92, cloning a `RiskManagedCheckpoint` also shares
its canonical account image. Capture and decode first construct exactly
reserved row vectors and then create three `Arc` control blocks; stable Rust
exposes no stable fallible `Arc::new`, so those three allocations remain under
A12. Direct restoration borrows the checkpoint and deliberately copies `E`
event values once into the newly constructed book's independent fixed arena.
Atomic reference-count increments and long-lived checkpoint handles can retain
the complete matching history/event arena and add target-dependent cache
contention.

**Falsification probe.** Clone checkpoints captured from empty, active, and
maximum states; require public pointer-sharing inspectors for all three images
and `EventTrace::shares_storage_with` for nested live traces; drop source
book/report/checkpoint owners in every order; restore and retry after the
source owners are gone; send/share checkpoints across threads; byte-compare
existing codec/snapshot fixtures and decode into independent storage. Any
mutable alias, row/event allocation during checkpoint clone, lost value after
owner drop, pointer-dependent equality/lineage/bytes, changed wire fixture,
non-`Send`/`Sync` checkpoint, or restoration that aliases mutable book state
falsifies A91. Fallible control-block construction requires a custom
shared-owner implementation or stable allocator API.

## A92 — immutable checkpoint value graphs

**Assumption.** Every completed semantic checkpoint is an immutable value
graph. `RiskManagedCheckpoint` and `CallAuctionRiskCheckpoint` retain canonical
account rows in private `Arc<Vec<RiskAccountCheckpoint>>`;
`CallAuctionCheckpoint` retains accepted identities, active orders, and history
in three private shared vectors; `LedgerCheckpoint` retains balances and
records in two private shared vectors. Public APIs expose only immutable slices
and explicit pointer-sharing diagnostics. Nested matching event traces follow
A84/A91, call-auction event traces follow A85, and ledger posting/batch graphs
follow A90. The optional current auction indication is derived from the shared
history under A112 and adds no fourth checkpoint image. Equality, same-lineage
prefix checks, validation, and stable codecs inspect ordered values only.

**Dependent results.** [A39, A66, A78, A88, A89, A90, A91, A92] Cloning a
direct or coupled continuous-matching, call-auction, or ledger checkpoint is
`O(1)` and allocates/copies no semantic rows, event values, postings, or batch
entries. Decoding creates independent row graphs. Restoration borrows the
immutable image and builds independent bounded mutable state; continuous and
call-auction restoration copy validated event values once into their new fixed
arenas, while ledger restoration clones allocation-free immutable record
handles. Capture/decode first own exactly reserved vectors, then create one
`Arc` control block per top-level image: one additional block for either risk
wrapper, three for a direct call-auction image, and two for a ledger image.

Stable Rust exposes no stable fallible `Arc::new`, so these control blocks
remain under A12. Snapshot/codec byte output remains separately allocated under
A80/A81 and may use nested temporary payload buffers.

**Falsification probe.** Clone every direct/coupled checkpoint at empty and
populated boundaries; require all public `Arc::ptr_eq` inspectors, nested
trace/posting/batch sharing, owner-drop survival, `Send + Sync`, identical
equality/lineage/bytes, and independent decode ownership. Restore every clone
under lower/equal/larger limits, mutate only the restored engine/ledger, and
prove the checkpoint and sibling restoration remain unchanged. Exercise uncut
and anchored single/segmented recovery and every typed capture failure. Any
mutable alias, deep clone allocation, pointer-dependent semantic result,
changed wire bytes, lost value after owner drop, shared mutable restoration
state, non-thread-safe checkpoint, or untyped pre-`Arc` capture reservation
failure falsifies A92. Allocator continuation at the top-level control-block
boundary requires a fallible shared-owner implementation or stable allocator
API.

## A93 — staged matching capture and verification

**Assumption.** Under the serialized-writer contract A3,
`OrderBook::capture_checkpoint_candidate` observes one immutable completed
report boundary. It audits the live bounded topology and event arena, captures
canonical resting and dormant rows plus chronological command/report handles, derives
accepted-order identities, account controls, effective trading state, the
inclusive expiry watermark, stop reference/trigger lineage, retained-event
count, and next event/trade identifiers from that history, and requires exact equality with the
corresponding live semantic state. The returned `OrderBookCheckpointCapture`
exposes only metadata/cardinalities and has no `BinaryCodec` or snapshot-payload
implementation. Its consuming `verify`
transition constructs an isolated book under the captured limits, reproduces
every report, and requires a fresh canonical replay projection to equal the
candidate before releasing `OrderBookCheckpoint`.

**Dependent results.** [A3, A39, A73, A84, A88, A91, A93, A94] For `C` retained
commands containing `E` events, `O` active orders, `P` initialized price slots,
`X` initialized expiry slots, and `S` initialized stop slots, writer-side
capture is expected
`O(C + E + O + P log(P + 1) + X log(X + 1) + S log(S + 1))` audit/copy work
with the A73 bounded lineage scratch, but executes no matching transitions from
history.
Verification is independent of the source book and may run on another thread
while that writer advances; it incurs complete deterministic replay plus a
second canonical projection with the same audit bound.
Candidate clones are `O(1)` and share all three checkpoint images and nested
event ranges. `OrderBook::checkpoint` delegates to capture plus verify. Durable
staged publication is separately fenced by A94.

**Falsification probe.** Capture before a suffix, advance the source book
through GTD admission/expiry and stop arm/trigger, verify the capture on another thread, and
require its exact old boundary, value
equality, byte equality, retry behavior, `Send + Sync`, and shared clone
storage. Corrupt each live lineage scalar/index and candidate boundary/resting/dormant row
class in white-box tests; force all A73/A88 reservation and temporary-book
constructor failures; require that only `OrderBookCheckpoint` satisfies the
codec/snapshot APIs. Any public candidate-row escape, stable candidate codec,
nonconsuming verification, accepted report/projection divergence, loss of exact
capture boundary after source advancement, semantic resource failure classified
as operational, or replay work inside `capture_checkpoint_candidate` falsifies
A93.

Writer-side row copying, structural/lineage audit, shared event-arena lifetime,
snapshot allocation, and authenticated publication remain explicit high-impact
boundaries.

## A94 — durable matching capture fence

**Assumption.** `DurableOrderBook::capture_checkpoint_candidate` first
completes a full WAL data/metadata barrier, then captures the exact
completed-report head and its private physical suffix cursor into
`DurableOrderBookCheckpointCapture`. The capture owns a shared atomic
poison/origin token identifying one open shard incarnation and a process-local
monotonic physical-cutover epoch. Its consuming off-thread verification
releases only `VerifiedDurableOrderBookCheckpoint`; semantic replay/projection
failure stores the shared poison latch with release ordering, while typed
reservation or temporary-book construction failure does not.

Standalone and prefix-retiring publication accept only a verified value whose
token is pointer-identical to the live shard and whose cutover epoch is
unchanged. Ordinary append-only command/report suffix growth is permitted;
reopen and any successful WAL-prefix cutover invalidate the fence.

**Dependent results.** [A3, A39, A58, A73, A88, A93, A94, A101] Writer
exclusion is needed for the initial WAL barrier and A93 structural/lineage
capture; full replay runs independently. `write_verified_checkpoint` publishes
the older exact prefix standalone. `compact_verified_checkpoint` uses A101 to
synchronize and migrate only the post-capture physical suffix, publishes
`anchor(G) + suffix(G+1..H)`, and advances the epoch after success.
`write_checkpoint` and `compact_to_checkpoint` synchronously compose the same
typestates. Publication rechecks poison, origin, epoch, path isolation,
metadata origin, cursor generation, and `G <= H`; normal snapshot same-lineage
monotonicity remains enforced by A40. An asynchronously detected semantic
failure prevents subsequent observed operations and close, although one
operation already in flight may complete before its next poison observation.

**Falsification probe.** Capture after two reports, append a fill-producing
suffix, verify on another thread, cut over the older generation, inspect the
anchor/command/report sequence, reopen, and require suffix-only replay plus
exact retry. Require `Send + Sync`, shared clone rows, wrong-shard/reopen
rejection, post-cutover epoch rejection including repeat cutover, no output on
fence failure, and shared-latch poison after semantic failure. Exercise
single/segmented suffix cursors and snapshot monotonicity. Any unbarriered
capture, forged cursor/origin, publication after cutover/reopen, lost/reordered
suffix, prefix rescan substituted for the cursor, stale checkpoint ahead of
WAL, resource-induced poison, or semantic failure without shared poison
falsifies A94/A101. Peak replay/projection memory, writer-side capture
audit/copy, atomic detection latency, and authenticated export remain
bounded-scope risks.

## A95 — staged coupled-risk capture and verification

**Assumption.** Under A3, `RiskManagedOrderBook::capture_checkpoint_candidate`
captures one completed coupled report boundary without re-executing history. It
audits live matching topology, command-derived lineage, profile capacity,
reservations, and account aggregates; captures canonical matching and account
rows; directly reconstructs positions and total-leaves reservations under the
live finite policy; and requires exact equality with the live coupled shard.
The opaque `RiskManagedCheckpointCapture` exposes only limits, metadata,
cardinalities, and storage-sharing evidence and has no codec/snapshot
implementation.

Its consuming `verify` transition registers the immutable profile set in an
isolated shard, reproduces every matching/risk report, and requires exact
equality with a fresh direct reconstruction before releasing
`RiskManagedCheckpoint`. Decode and `RiskManagedCheckpoint::from_parts` retain
full coupled replay validation; `RiskManagedOrderBook::checkpoint` delegates to
capture plus verify.

**Dependent results.** [A3, A39, A40, A56, A73, A88, A91, A92, A93, A95, A96]
For `C` retained commands containing `E` events, `O` active
orders/reservations, `P` initialized price slots, and `A` accounts, writer-side
capture retains the A93 matching audit/copy cost, exactly reserves and sorts
`A` rows in `O(A log A)`, and directly reconstructs coupled state in `O(C + E +
O log(P + 1) + A)` expected time without executing matching transitions.
Verification independently performs direct reconstruction plus complete coupled
replay under the captured limits. Candidate clones are `O(1)` and share
matching/history/account images. The protocol removes complete deterministic
replay from writer exclusion but does not remove direct reconstruction or
canonical capture work.

**Falsification probe.** Capture reserve-display orders, fills, risk
rejections, signed positions, reservations, and profiles; clone and verify
off-thread after source suffix growth; require old-boundary equality, `Send +
Sync`, shared rows, exact restore, and synchronous-path equivalence. Mutate
WAL/profile/exposure/reservation/history classes; force account, matching, and
constructor resource failures; decode semantic corruptions; require only the
verified checkpoint to be encodable. Any replay inside capture, missing
live/direct proof, accepted report/profile/exposure/reservation divergence,
candidate row escape/codec, changed source boundary, or flattened failure
falsifies A95. Writer-side direct reconstruction, retained complete history,
peak worker memory, and shared-owner allocation remain high-impact boundaries.

## A96 — durable coupled-risk capture fence

**Assumption.** `DurableRiskOrderBook::capture_checkpoint_candidate` completes
a full WAL barrier before A95 capture and binds the candidate, physical suffix
cursor, shared poison/origin token, and cutover epoch to one open shard
incarnation. Consuming verification releases only
`VerifiedDurableRiskManagedCheckpoint`; semantic failure stores poison with
release ordering, while typed reservation/construction failure does not.
Standalone or A101 prefix-retiring publication requires pointer-identical
origin, unchanged epoch, exact `F`, profile boundary `M`, cursor generation,
and `generation <= current WAL head`. Ordinary suffix growth is valid; reopen
and successful cutover invalidate the fence.

**Dependent results.** [A3, A40, A56, A58, A94, A95, A96, A101] Complete
coupled replay runs independently. `compact_verified_checkpoint` migrates only
suffix bytes after the captured cursor, then advances the epoch; synchronous
checkpoint/cutover APIs compose the same stages. Acquire observations in
`is_poisoned`, submit, checkpoint, sync, and close retain the A94 in-flight
bound.

**Falsification probe.** Verify while appending a fill-producing suffix;
prefix-retire through segmented storage; inspect exact anchor/command/report
continuity and require restored profile, position, leaves reservation, and
retry state. Reject other shard, reopen, repeated/post-cutover publication,
wrong metadata/head/cursor, and output creation on fence failure. Any prefix
rescan, suffix loss/reordering, cross-origin publication, semantic failure
without poison, operational poison, or accepted metadata/head drift falsifies
A96/A101. Profile evolution, generation rollover, authenticated export, and
bounded retained history remain outside this protocol.

## A97 — staged auction capture and verification

**Assumption.** Under A3/A64, `CallAuctionEngine::capture_checkpoint_candidate`
observes one completed report boundary, runs the allocation-free live
engine/book/event-arena audit, exactly captures canonical accepted identities,
active orders, and chronological command/report handles, and projects
phase/cycle, collection revision, priorities, trades, identities, orders, and
counters plus the current optional indication from retained traces without
executing commands. The opaque
`CallAuctionCheckpointCapture` exposes only limits, metadata, phase,
cardinalities, and storage-sharing evidence and has no codec/snapshot
implementation. Consuming `verify` constructs one isolated engine under the
captured policy, reproduces every plain or externally gated report exactly, and
requires a fresh canonical projection to equal the candidate before releasing
`CallAuctionCheckpoint`. `CallAuctionEngine::checkpoint` composes these phases.

**Dependent results.** [A3, A64, A66, A75, A78, A85, A92, A97, A98] For `C`
commands containing `E` events, `O` active orders, and `I` accepted identities,
writer-side capture is expected `O(C + E + O + I)` work plus bounded book/arena
audit and uses the A78 scratch bounds, but executes no state transitions.
Verification owns one temporary engine, performs full auction work, and
projects once more. Candidate clones are `O(1)` and share all three row images
and nested event ranges.

**Falsification probe.** Capture collecting/frozen/closed cycles, accepted and
rejected submissions, amendments, and replacements, individual and mass
cancellations, empty/crossed indications and their invalidation,
executable/non-executable uncrosses,
retained remainders, risk-gated reports, and counter boundaries; clone, advance
the source, verify off-thread, and require exact
old-generation/synchronous-path equality, `Send + Sync`, and shared rows.
Corrupt live and candidate phase, boundary, history/event, accepted/active,
priority/trade/revision classes and force every A78/constructor failure. Any
replay during capture, candidate codec/row escape, accepted trace/projection
divergence, changed boundary after suffix growth, or flattened failure
falsifies A97. Complete retained history, writer audit/projection, worker
replay memory, and shared-owner construction remain explicit boundaries.

## A98 — durable auction capture fence

**Assumption.** `DurableCallAuctionEngine::capture_checkpoint_candidate`
completes a full WAL barrier before A97 capture and binds the candidate plus
physical suffix cursor to one open shard incarnation, shared poison/origin
token, and monotonic cutover epoch. Verification releases only
`VerifiedDurableCallAuctionCheckpoint`; semantic failure stores poison with
release ordering and operational resource/construction failure does not.
Standalone or A101 prefix-retiring publication requires pointer-identical
origin, unchanged epoch, exact metadata origin/cursor generation, and
`generation <= current WAL head`. Append-only suffix growth is valid; reopen
and successful cutover invalidate publication.

**Dependent results.** [A3, A58, A65, A66, A94, A97, A98, A101] Complete
auction replay runs independently. `compact_verified_checkpoint` migrates only
suffix frames after the captured cursor and advances the epoch after
publication; synchronous checkpoint/cutover APIs compose the same stages.

**Falsification probe.** Verify while appending sell, mass-cancel, freeze, and
uncross suffix commands; retire the older prefix through rotated segmented storage;
inspect exact frame continuity and require restored closed phase, empty book,
and trade counter. Reject origin/reopen/epoch/metadata/head/cursor drift
without output or operational poison. Any prefix rescan, suffix loss, replay
under writer exclusion, unverified publication, or semantic failure without
poison falsifies A98/A101.

## A99 — staged auction-risk capture and verification

**Assumption.** Under A3/A68,
`CallAuctionRiskManagedEngine::capture_checkpoint_candidate` performs A97
non-replaying auction capture, exactly reserves and sorts canonical
account/profile/exposure rows, audits live coupled structure, reconstructs the
auction engine and every position/reservation/exposure directly under captured
limits, and requires canonical direct/live equality without executing retained
commands. The opaque `CallAuctionRiskCheckpointCapture` has no codec/snapshot
implementation. Consuming `verify` performs one direct reconstruction and one
isolated coupled replay, then compares canonical auction and account
projections before releasing `CallAuctionRiskCheckpoint`. Decode retains the
same full coupled replay proof; prior nested plain-auction replay calls are
eliminated.

**Dependent results.** [A3, A68, A76, A78, A92, A97, A99, A100] Writer-side
work includes A97 capture, `O(A log A)` account sorting, and history-dependent
direct reconstruction but no command execution. Verification performs coupled
replay exactly once plus direct/canonical projections. Candidate clones share
auction/account images in `O(1)`.

**Falsification probe.** Capture core and every risk rejection, market/limit
reservations, signed collar valuation, frozen phase, full/partial uncross
positions, retained remainders, individual/mass cancellation release,
quantity-amendment reservation release,
same-account netting, and multiple cycles;
advance through an uncross suffix during worker verification and require exact
old positions/reservations plus current suffix state. Corrupt profile,
position, exposure, reservation, auction, and lineage classes; force nested and
account/construction resources; decode corruption. Any nested replay
amplification, replay inside capture, missing direct/live proof, accepted risk
divergence, candidate codec, or flattened failure falsifies A99.

## A100 — durable auction-risk capture fence

**Assumption.** `DurableCallAuctionRiskEngine::capture_checkpoint_candidate`
completes a WAL barrier before A99 capture and binds the candidate plus
physical suffix cursor to one open shard incarnation, cutover epoch, and shared
poison/origin token. Verification releases only
`VerifiedDurableCallAuctionRiskCheckpoint`; semantic failure poisons with
release ordering while operational failure remains retryable. Standalone or
A101 prefix-retiring publication requires pointer-identical origin, unchanged
epoch, exact first sequence `F`, profile boundary `M`, cursor generation, and
`generation <= current WAL head`. Suffix growth is valid; another shard,
reopen, and cutover are rejected before output creation.

**Dependent results.** [A3, A58, A68, A78, A96, A98, A99, A100, A101] Full
coupled replay runs off-thread while later command/report pairs append.
`compact_verified_checkpoint` migrates only the captured suffix and advances
the epoch. Recovery restores phase/book/profile/position/reservation state from
the older checkpoint and executes only that suffix.

**Falsification probe.** Retire an older kind-`5` checkpoint across rotated
segments after sell, mass-cancel, freeze, and uncross suffix commands; inspect frame
continuity and require exact position and reservation release. Reject
origin/reopen/epoch/profile/head/cursor drift and classify nested operational
failures. Any unverified publication, prefix rescan, suffix loss/reordering,
accepted cross-origin/profile drift, semantic failure without poison, or
operational poison falsifies A100/A101. Profile evolution, replicated
ownership, bounded history/generation rollover, and authenticated export remain
outside this protocol.

## A101 — physical suffix cutover cursor

**Assumption.** One verified durable capture retains a private physical cutover
cursor recorded immediately after its full WAL barrier: single-file `(initial
sequence, G, byte offset)` or segmented `(physical generation, first sequence,
segment start, G, byte offset)`. While its origin/epoch fence remains valid,
only append-only frames may follow. `Journal` and `SegmentedJournal` cutover
synchronize the current head `H`, stream-verify frames starting exactly at the
cursor, write a new image containing `anchor(G)` followed by unchanged logical
`(kind, sequence, payload)` frames `G+1..H`, synchronize that image, and only
then publish the same-filesystem rename or checksummed generation marker.
Segmented migration may repack frames but never split one frame; the previous
selected generation remains authoritative until marker publication.

**Dependent results.** [A3, A24, A40, A58, A94, A96, A98, A100, A101]
Single-file cutover uses `O(P_max)` temporary payload/frame memory. Segmented
cutover additionally retains `O(S_suffix + 1)` verified segment descriptors and
creates at most `S_suffix + 1` new bounded segments for `S_suffix` source
segments intersecting the suffix. Writer-held scan/copy work is `O(B_suffix +
S_suffix)`, not proportional to the retired prefix; checksum verification and
re-encoding are linear in suffix bytes. Snapshot writing remains outside engine
mutation and the process epoch advances only after physical publication
succeeds. Pre-publication staging failure leaves the old selector
authoritative; post-publication barrier/installation ambiguity poisons and
requires reopen.

**Falsification probe.** Capture at the end and middle of a segment, append
suffixes that remain in place and rotate repeatedly, then cut over and
byte/value-compare every retained frame. Exercise zero-length first-segment
suffix, multisegment repacking, exact-head suffix length zero, occupied
staging, generation/sequence exhaustion, marker and parent-directory barrier
faults, stale/wrong-layout cursors, repeat publication, and recovery at every
publication boundary. Any scan before the cursor, unverified or missing suffix
frame, changed kind/sequence/payload, frame split, selector publication before
all new bytes are synchronized, deletion of the selected old generation before
marker publication, accepted cursor from another layout/generation, or epoch
advance on failed publication falsifies A101.

Hard-link alias exclusion, external archival, filesystem/device power-loss
qualification, and bounded suffix size remain outside this local protocol.

## A102 — explicit continuous stop-reference authority

**Assumption.** One upstream authority supplies every continuous stop reference
as a serialized `StopTriggerSweep` command for the exact instrument version.
The `StopReference` binds the intended last-trade price at that command boundary
to the A106 source identity, source version, and source sequence. The matching
engine does not infer reference movement from its own trades, timestamps, wall
time, market data, or restart context. A sweep activates only the configured
canonical bounded prefix; if eligible stops remain, the authority repeats the
exact reference until the backlog is zero before advancing its cursor. Ordering
among equal thresholds is the priority sequence carried by the arm event—
initially the accepted-order sequence, or the replacement event sequence after
reprioritization—followed by `OrderId`.

**Dependent results.** [A3, A5, A9, A15, A20, A21, A37, A39, A50, A70, A102,
A106]
Stop intake, activation, risk reservation, public invisibility, checkpoint
lineage, WAL recovery, and exact retry are deterministic without an engine
clock or inferred trade-to-trigger coupling. A missed, duplicated, conflicting,
or unannounced-source upstream reference is a typed admission or idempotency
failure rather than an unrecorded local state change.

**Falsification probe.** Replay identical new-order and sweep streams while
varying wall time, receive timestamps, local matching trades, process restarts,
and publication timing; require byte-identical reports and checkpoints. Submit
buy/sell thresholds on both sides of the reference, equal trigger priorities,
partial batches, a different reference with backlog, and exact/differing-content
retries. Inject source-sequence gaps/regressions, cursor/content collisions,
version reset, and source identity change. Compare the upstream authoritative
trade-reference series with the committed sweep series. Any implicit reference
change, activation without a persisted sweep, noncanonical equal-threshold
order, accepted cursor advance over backlog, second retry effect, or inability
to identify a missing/conflicting upstream reference falsifies A102 and
requires a different versioned trigger authority.

## A103 — fully hidden continuous queue policy

**Assumption.** One immutable instrument-definition flag is authoritative for
fully hidden continuous-order admission. A fully hidden qualifier is valid only
for a limit order whose time in force may rest. At one price, fully displayed
and native-reserve orders form the first execution class and fully hidden
orders the second; FIFO applies within each class, and reserve refresh rejoins
the first class's tail. Every hidden leaf is executable for matching, FOK,
minimum-quantity IOC, STP, risk, checkpoint, and replay, while public quantity
and public order count are zero. Venue-specific alternative hidden classes,
midpoint/non-displayed types, minimum-quantity priority, and discretionary
interaction are not inferred.

**Dependent results.** [A1, A15, A20, A21, A22, A44, A45, A47, A50, A55,
A70, A72, A83, A88, A103] Deterministic displayed-before-hidden execution,
hidden FIFO, reserve refresh priority, hidden-aware FOK/STP barriers, total-
leaves risk, version-12 WAL/snapshot recovery, and version-3 public projection.
A hidden-only price can be the private execution best while being absent from
public best/depth. A hidden-maker trade prints at its execution price with a
canonical absent public maker level when no visible same-price level exists.

**Falsification probe.** Import venue-certified fixtures and execute an older
hidden order, later fully displayed and reserve orders at the same price,
multiple reserve refreshes, multiple hidden orders, every FOK/STP policy,
same-price self barriers in both classes, retained/lost-priority replacement,
GTD/stop-limit activation, checkpoint/WAL recovery, risk reconstruction, and
publisher/replica gap repair. Independently compare private queue order, public
depth/count, trade sequence, and restored state. Any hidden order executing
ahead of displayed-class liquidity, reserve refresh crossing behind hidden,
hidden FIFO inversion, hidden public depth, lost executable leaves, replica
sequence gap, replay divergence, or required venue rule outside the represented
class model falsifies A103 for that integration and requires a new versioned
policy.

## A104 — versioned UTC trading-calendar authority

**Assumption.** One upstream publisher assigns every immutable
`TradingCalendar` generation a non-zero `(CalendarId, CalendarVersion)` pair
whose content never changes. It supplies complete sessions in canonical entry-
time order with unique non-zero `TradingSessionId` values, authoritative UTC
nanosecond entry/session/day boundaries, and authoritative `AccountingDate`
values. The same ID/version pair is not reused for different bytes. Quotick
validates structural chronology but does not derive or authenticate venue
hours, time zones, daylight-saving transitions, holidays, early closes, or
business dates.

The gateway resolves `Day` or `GoodForSession` at its authoritative
`received_at` against the half-open active entry window and submits the
resulting absolute GTD deadline to matching. It retains the returned calendar,
session, and date provenance when that evidence is required; the current
matching WAL/checkpoint grammar retains only the normalized deadline. A
controller submits the corresponding explicit inclusive `ExpirySweep` at or
after the calendar boundary. Native TIF values do not require an active
session and pass through unchanged.

**Dependent results.** [A5, A15, A17, A20, A21, A37, A39, A80, A81, A104]
Deterministic active-session lookup, day/good-for-session normalization,
boundary-checked sweep construction, stable calendar bytes, and reuse of the
existing GTD matching/risk/publication/recovery semantics. Calendar queries
read no wall clock, and a cloned generation shares immutable rows/indexes.
No claim follows that the core matching WAL can reconstruct the original
calendar-relative request or prove which publisher authorized a generation.

**Falsification probe.** Import independently sourced venue schedules across
overnight sessions, multiple sessions assigned to one trading date, weekends,
holidays, daylight-saving changes, early closes, and version cutovers. Resolve
at `open - 1 ns`, `open`, `close - 1 ns`, `close`, session expiry, and day
expiry; compare calendar ID/version/session/date/deadline evidence and replay
the normalized command/sweep stream. Mutate row order, duplicate IDs,
same-date day boundaries, cross-date overlap, identifiers, counts, and trailing
bytes. Reuse one ID/version for different content, omit or delay a boundary
sweep, or discard ingress provenance before an audit reconstruction.

Any accepted structural contradiction, time-zone or holiday inference inside
Quotick, resolution outside the active entry window, noncanonical boundary,
changed supported bytes, matching replay divergence, or publisher schedule
disagreement falsifies A104 or its environment. Authenticated distribution,
atomic activation, original-request durability, multi-shard synchronization,
and sequenced session-state transitions require additional protocols.

## A105 — atomic minimum-quantity IOC

**Assumption.** An explicit `ImmediateOrCancelWithMinimum` order combines an
IOC lifetime with one non-zero execution threshold. The threshold is lot-grid
aligned and no greater than original quantity; it may be below the instrument's
new-order size minimum because it constrains execution rather than entry. Only
external traded quantity counts. Cancel-resting excludes self orders, while
cancel-aggressor and cancel-both stop eligibility at the same reserve/hidden
self barriers used by FOK. Decrement-and-cancel instead follows A115: prevented
self quantity does not satisfy the threshold, but it consumes both incoming
leaves and the maker's current executable slice.

The nonmutating preflight precedes matching and STP effects. If eligible
quantity is below the threshold, the order is accepted and its complete
quantity is cancelled with `MinimumQuantityUnavailable`; no maker, STP, risk,
reservation, position, or public state changes. If the threshold is met,
ordinary IOC matching can execute beyond it and cancels only the final
remainder. A dormant stop retains the constraint, evaluates it against
activation-time liquidity, and cannot be replaced below the threshold. This
pairing, cancellation reason, and STP/reserve policy are Quotick internal
contracts; FIX `MinQty(110)` and IOC terminology do not supply those
venue-specific rules.

**Dependent results.** [A1, A5, A9, A15, A20, A21, A22, A37, A39, A45, A50,
A70, A83, A88, A102, A103, A105, A115] Allocation-free `O(1)`-space threshold
inspection, atomic failure, reserve/hidden-aware eligibility, stop activation,
no-change public projection, risk release, stable WAL-v20/snapshot-v20 bytes,
checkpoint/WAL recovery, and exact retry are deterministic. The specialized
A115 scan has the time bound stated there; other policies retain the one-pass
FOK eligibility bound.

**Falsification probe.** Exercise thresholds below, equal to, and above
available external quantity across multiple prices; thresholds off grid, above
original quantity, and below the entry minimum; every STP policy; displayed,
reserve, and hidden makers; displayed- and hidden-class self barriers; ordinary
and stop activation; replacement; public projection; risk; checkpoint/WAL
recovery; and exact/different-content retries. Compare accepted executions with
an independent literal reserve-refresh queue. Any partial maker/STP mutation
on threshold failure, prevented self quantity counted as execution, execution
below threshold, artificial cap at the threshold, incorrect decrement-and-
cancel admission or execution, replay divergence, or tag drift falsifies A105.

## A106 — sequenced stop-reference source cursor

**Assumption.** One instrument-version shard receives one logical
stop-reference stream. Its non-zero `StopReferenceSourceId` is fixed for the
shard. The first accepted `StopReference` binds any non-zero source version and
sequence as the shard baseline. Thereafter, with no eligible backlog, the same
source version must advance by exactly one sequence; the immediate next source
version starts at sequence `1`. Skipped/regressed source versions or sequences,
source identity changes, and reuse of one cursor for a different price are
typed nonmutating rejections. An exact current cursor/content pair can repeat
only to drain an eligible bounded backlog.

The sequence is the contiguous per-shard stop-reference stream, not an
unfiltered transport sequence containing unrelated instruments or message
types. An ingress adapter must project and persist that coordinate before
submission. Source identity/version authentication, raw-feed normalization,
retransmission, gap repair, and failover authority remain external. Source-ID
change or exhaustion of the version/sequence transition space requires a new
instrument shard lineage.

**Dependent results.** [A3, A5, A14, A15, A16, A17, A20, A21, A37, A50, A70,
A73, A88, A102, A106] Stop-reference gaps, regressions, reset discontinuities,
and cursor/content conflicts are detected before matching mutation. Commands,
trigger events, completion events, checkpoints, continuous market-data private
mirrors, plain/coupled risk, WAL replay, and exact retries retain one identical
source coordinate. Validation and transition work use `O(1)` time and space;
each encoded reference is 32 B.

**Falsification probe.** Start from non-one baseline sequences; advance same-
version references contiguously; inject duplicate, lower, and skipped
sequences; roll to the immediate next version at sequence `1`; inject skipped,
regressed, zero, and non-one reset coordinates; change source ID; reuse one
cursor for another price; and exhaust `u64` sequence/version boundaries. Create
a partial eligible backlog and require only the exact current reference to
continue. Corrupt each cursor field in commands, trigger/completion events,
matching and coupled-risk checkpoints, plain/segmented WAL recovery, and
publisher private state. Any accepted discontinuity/conflict, changed state on
rejection, lost coordinate, replay divergence, or noncanonical optional bytes
falsifies A106.

## A107 — bounded continuous market-data replay

**Assumption.** One `MarketDataReplayBuffer` retains an exact finite suffix of
the public A70 update stream for one instrument/version. Construction binds an
already-published sequence boundary and a non-zero retained-update maximum
`N`; no update at or before an otherwise unretained boundary is inferred. A
non-replayed command batch must contain `E <= N` contiguous updates. Exact
overlap is accepted only while every duplicated sequence and value remains
available; conflicting content, gaps, identity/version drift, and unprovable
evicted overlap fail before mutation.

`replay_after(s, L)` uses an exclusive source cursor and positive page limit
`L`. It returns at most `L` exact updates in ascending sequence, including
across physical ring wrap. A cursor above the latest boundary or a required
first sequence older than the retained suffix is explicit. The ring is
process-local volatile recovery state, not a transport, authenticated channel,
durable archive, entitlement service, or remote retransmission session.

**Dependent results.** [A12, A21, A23, A70, A107] Constructor work is `O(N)`
time and typed slot space. Admission is `O(E)` time and performs no allocation;
each retained lookup and new slot write is `O(1)`. Replay setup is `O(1)` and
iteration is `O(R)` for `R <= min(L, N)` returned updates with `O(1)` iterator
state. The existing `MarketDataReplica` remains the sole public-depth
transition grammar; retained replay therefore introduces no second level/trade
application implementation. Snapshot fallback remains authoritative when a
cursor is unavailable or an incremental structural failure poisons a replica.

**Falsification probe.** Reject zero and unrepresentable capacities; initialize
at zero and non-zero recovered boundaries; append single updates and complete
command batches; inject internal sequence gaps, foreign identity/version,
content collision, exact duplicates, partial retained overlap, and overlap
older than the ring. Wrap repeatedly at `N`, prove allocation capacity is
unchanged, page every boundary, reject zero limits and future cursors, and
exercise `u64::MAX`. Drop a retained suffix and require exact replica repair;
drop an evicted prefix and require typed unavailability followed by atomic
snapshot recovery. Any partial mutation on failure, stale value returned,
sequence reordering, allocation growth, duplicated depth grammar, changed
version-3 payload byte, or recovery divergence falsifies A107.

## A108 — bounded call-auction batch replay

**Assumption.** One `CallAuctionMarketDataReplayBuffer` retains an exact finite
suffix of the public A67 update stream for one instrument/version. Construction
binds an already-published event sequence and a non-zero retained-update
maximum `N`. Each admitted non-replayed command batch contains `E <= N`
contiguous updates; its exact first and final positions are retained with every
value. Exact overlap requires both value and boundary equality. Conflicting
content or boundaries, gaps, identity/version drift, oversized batches, and
unprovable evicted overlap fail before mutation.

`replay_batches_after(s, L)` requires a positive update limit `L` and an
exclusive cursor at a complete command-batch boundary. It returns only complete
batches in source order and never splits a multi-update uncross, two-update
replacement trace, or mass-cancel removal/completion trace. A cursor inside a
batch and a next batch larger than `L` are reported distinctly. Update-wise
eviction can leave an incomplete oldest batch; that partial batch is
unavailable and the first later complete boundary is reported. The ring is
process-local volatile state, not durable history, transport framing,
authentication, entitlement, fanout, or a remote retransmission session.
The unframed single-update replica API rejects a `Replaced` removal because it
cannot prove the required complete command boundary.
It likewise rejects `MassCancelled` removals and `MassCancelCompleted` because
their count/quantity/revision grammar requires the complete batch.
An indicative publication is a complete one-update batch and replays through
the same revision-binding and invalidation rules as live application.

**Dependent results.** [A12, A23, A67, A108] Construction is `O(N)` time and
typed slot space. Admission is `O(E)` time and allocation-free. Successful page
selection and complete iteration are each `O(R)` for `R` returned updates and
use `O(1)` iterator state; diagnosing a partial oldest batch can scan `O(N)`
slots. `CallAuctionMarketDataReplica::apply_replay_batch` reuses the live batch
identity, sequence, capacity, transition, poisoning, and command-counter path,
so recovery advances event and command boundaries together. Snapshot fallback
remains authoritative when a complete required batch is unavailable or the
replica is poisoned. Version-5 payload bytes do not change.

**Falsification probe.** Reject zero and unrepresentable capacities; admit
single-update phase/order/amendment/indicative batches, a two-update replacement,
empty/non-empty mass-cancel batches, and a multi-update
uncross. Inject identity, version,
sequence, content, boundary, capacity, and evicted-overlap faults;
repeat exact batches; wrap the ring without allocation growth; paginate without
splitting a batch; reject zero limits, future cursors, inside-batch cursors, and
limits below the next batch. Evict only the first update of a replacement,
mass cancel, or uncross and require typed unavailability with the next complete
boundary. Repair a skipped replica and compare source event sequence, command
sequence, phase, revision,
and both depth sides. Any partial mutation on failure, split batch, stale value,
lost command boundary, allocation growth, duplicate transition grammar,
changed version-5 payload byte, or recovery divergence falsifies A108.

## A109 — retained-priority call-auction quantity reduction

**Assumption.** A call-auction amendment is a strict reduction of one active
order's positive lot-aligned leaves during the exact `Collecting` cycle and
phase revision. The authenticated account owns the target. Order identity,
account, side, market/limit constraint, limit price, queue position, and
priority class and sequence are immutable. Equal quantity, increased quantity,
price or side change, and zero leaves are not amendment semantics. Price/side
changes continue to require A62 new-identity replacement with priority loss.

**Dependent results.** [A15, A41, A52, A62, A64, A65, A66, A67, A68, A74,
A75, A76, A78, A97, A99, A108, A109] One accepted amendment performs
allocation-free `O(log O + log P)` book mutation with `O(1)` auxiliary space,
one book revision, no accepted-ID/count/level change, and exact queue/account
aggregate reduction. It emits one replayable `OrderAmended` event in the
ordinary history lane. Coupled risk removes the exact quantity and conservative
notional delta without a new entry gate. Public payload v5 emits one anonymous
`Amended` aggregate delta with unchanged order count and invalidates any prior
indication. WAL/snapshot v20 preserve
exact retry and checkpoint-plus-suffix recovery.

**Falsification probe.** Exercise market and limit orders at head, middle, and
tail priority; owner, route, cycle, phase-revision, unknown-order, equal,
increased, off-grid, below-entry-minimum but valid-leaves, and revision-
exhaustion cases. Compare pre/post identity, priority, links, counts, accepted
IDs, queue/account quantities, indicative allocation order, risk reservation,
notional/exposure, public aggregate/count, codec bytes, checkpoint projection,
WAL suffix recovery, and exact retry. Inject event changes to owner, side,
constraint, price, priority class, priority sequence, previous quantity, new
quantity, or book revision.
Any accepted non-reduction, priority/identity change, new risk rejection,
private public-field disclosure, count drift, duplicate retry effect,
allocation growth, or recovery divergence falsifies A109.

## A110 — price-class pro-rata auction allocation

**Assumption.** `ProRataTime` receives the same complete canonical order
population and A60 clearing result as A61. Allocation tiers are one market or
exact limit-price constraint plus one priority class. Market, economically
better price, and lower priority-class tiers precede worse tiers. Each order
quantity and the clearing executable quantity are aligned to the instrument's
positive quantity increment, which is the allocation quantum.

Every tier before the marginal tier fills completely. If `R` quanta remain in
a marginal tier containing `Q` quanta, order `i` with `q_i` quanta receives
`floor(q_i × R / Q)` quanta. Residual quanta are assigned at most once per
order in ascending priority-sequence/`OrderId` order. Worse tiers receive zero.
Both sides apply the rule independently and must reconcile to the same
executable quantity. No size sorting, time weighting, top-order preference,
minimum allocation, displayed/hidden distinction, or hybrid FIFO percentage is
inferred.

**Dependent results.** [A1, A4, A12, A15, A60, A62, A63, A64, A65, A66,
A67, A68, A74, A75, A76, A78, A97, A99, A108, A110, A111] The kernel
computes exact
shares without constructing an overflowing product, using at most four fixed
64-step multiply/divides per marginal order across validation and
construction. Validation and fill construction are `O(B + A)` time with
`O(F_b + F_a)` result space. The collection book uses the
instrument quantity increment, the engine emits the resulting deterministic
trade/remainder trace, risk consumes that trace without an independent
allocation inference, and auction market-data payload v5 projects the same
events while adding A112. WAL v20 and snapshot v20 persist the explicit policy
for
full replay, checkpoint recovery, and exact retry. A61 price-time behavior
remains separately selectable and byte-tagged.

**Falsification probe.** Exercise market, better-price, marginal-price, and
priority-class boundaries; non-unit quantity increments; base shares of zero;
multiple residual quanta; equal priority sequences with `OrderId` tie breaks;
balanced and one-sided excess; and totals whose direct `u64 × u128` product
exceeds `u128`. Reject zero quantum, off-quantum order quantities, malformed
canonical order, aggregate disagreement, zero execution, and bounded-capacity
failure before output or mutation. Differentially compare at least 20,000
generated small-integer plans with direct arithmetic. Compare price-time and
pro-rata divergence through book preparation, pairing, engine events, risk,
codec tags, full-WAL recovery, snapshot recovery, and exact retry. Any
truncation approximation, non-FIFO residual, tier leakage, off-grid fill,
side-total disagreement, policy loss, replay divergence, or capacity growth
falsifies A110.

## A111 — authoritative call-auction priority classes

**Assumption.** Every `CallAuctionOrder` carries one authoritative
`AuctionPriorityClass(u16)` supplied by the ingress/controller. A lower scalar
precedes a higher scalar only after market/limit category and economic price
have compared equal; priority sequence and then `OrderId` break ties. The
scalar is an internal ordering coordinate and does not imply a venue order
category without an independently qualified adapter mapping.

Priority class is immutable through retained-priority amendment, partial fill,
cancellation, checkpoint, and recovery. A62 replacement may supply another
class, but it assigns a new identity and fresh priority sequence. Public
call-auction market data validates the class in private command/event state but
does not disclose order-level class or identity.

**Dependent results.** [A1, A4, A15, A60, A61, A62, A63, A64, A65, A66,
A67, A68, A74, A75, A76, A78, A97, A99, A109, A110, A111] One shared
comparator governs analytical validation and live allocation scratch. Arrival
queues remain mutation-time FIFO structures; rebuilding caller-owned order
scratch resolves their identities and performs an allocation-free unstable
sort in `O(O log O + P)` time. Both `PriceTime` and `ProRataTime` therefore
honor market/price/class/time/ID order. Risk consumes the resulting execution
trace without independently inferring class. WAL v20 and snapshot v20 preserve
the class through full replay, direct checkpoint restore, suffix recovery, and
exact retry.

**Falsification probe.** Exercise classes `0` and `u16::MAX`; invert class and
arrival order; compare market against limit, better price against better class,
and equal-class sequence/`OrderId` ties; place the class boundary at a
price-time or pro-rata marginal tier; amend, partially fill, replace, cancel,
and mass cancel; corrupt class in command/event/checkpoint state; and compare
full-WAL, snapshot-plus-suffix, coupled-risk, and public-market-data recovery.
Generate at least 20,000 collection mutations across four classes and compare
scratch order with an independent literal comparator. Any class-before-price
ordering, mutable class, tier leakage, public disclosure, validation drift,
replay divergence, or allocation growth falsifies A111.

## A112 — sequenced indicative call-auction publication

**Assumption.** The controller supplies one authoritative aligned candidate
band, reference price, and A60 ranking policy for an exact instrument version,
active `AuctionId`, and non-zero phase revision. `CallAuctionIndicativeCommand`
is valid only in `Collecting` or `Frozen`. It observes the exact current
collection-book revision and reuses the A60/A62 aggregate discovery kernel; it
does not independently derive interest, a reference, a band, eligibility, or a
venue disclosure schedule.

Acceptance emits exactly one `IndicativePublished` event. Its state binds the
auction ID, phase revision, book revision, band, reference, policy, and either
one non-zero aggregate clearing result inside the band or `None` when current
interest cannot execute. Absence is a successful publication, not a rejection.
The command changes no order, aggregate, book revision, reservation, exposure,
or position.

The engine retains at most one current indication. An accepted indicative
command replaces it. Every accepted non-indicative command clears it, including
an empty mass cancel and a phase transition. A business rejection preserves
it. An exact retry returns the original report with `replayed = true`, emits no
second event or public update, and preserves whatever indication is current at
retry time rather than restoring the original observation.

**Dependent results.** [A15, A41, A52, A60, A62, A64, A65, A66, A67, A68,
A74, A75, A78, A85, A92, A97, A99, A108, A112] Discovery is `O(B + A)` time
for `B` bid and `A` ask aggregates and `O(1)` auxiliary space because it uses
constructor-owned scratch and the existing allocation-free kernel. One command
adds one fixed-size event and one optional fixed-size live state. Risk
authorization and application are `O(1)` no-ops.

WAL v20 retains command/action tag `7` and event-kind tag `9`; nullable reports
are 98 B or 138 B. Snapshot v20 derives the current value from accepted
history and checkpoint-plus-suffix invalidation. Auction market-data v5 uses
kind tag `6`, 84 B or 124 B updates, complete one-update replay batches, and an
optional snapshot value. Publisher, replica, direct restore, full-WAL recovery,
coupled risk recovery, exact retry, and source cross-audit must reproduce the
same optional state.

The public value reveals its explicit discovery inputs, revision coordinates,
indicative price, paired quantity, and aggregate imbalance quantities. No
claim follows about transport, authentication, entitlement, cadence,
conflation, thresholds, obfuscation, reference/band derivation, eligibility,
or compatibility with a venue feed.

**Falsification probe.** Publish on empty, one-sided, locked, crossed,
market-only, and signed-price books in collecting and frozen phases. Exercise
both pressure rules and both final ties; references below, inside, and above
the selected interval; exact collar endpoints; and executable quantities above
`u64::MAX`. Reject closed phase, route/version/cycle/revision drift, inverted or
off-grid bands, out-of-collar values, zero phase revision, malformed policy
tags, noncanonical booleans, zero-execution present clearing, and clearing
outside the band.

After publication, apply every accepted command kind, including empty mass
cancel, and require invalidation; apply every rejection and exact retry and
require preservation. Retry an older indicative command after later
invalidation and require no restoration. Corrupt history order, command/event
binding, event cardinality, optional state, book/phase revision, WAL/snapshot
version, market-data update, replay boundary, and snapshot binding. Compare
direct engine state, publisher, live replica, replayed replica, full-WAL open,
snapshot restore, suffix recovery, and coupled risk state. Count allocations
and verify `O(B + A)` discovery with fixed scratch.

Any private identity disclosure, book or risk mutation, second retry effect,
stale/misbound retained value, incorrect nullable clearing, failure to
invalidate/preserve as specified, wire-size/tag drift, recovery divergence,
or post-construction scratch growth falsifies A112. A venue-specific reference,
band, imbalance calculation, dissemination schedule, suppression rule, or
protocol mapping requires an explicit versioned adapter or state machine.

## A113 — fail-closed call-auction self-trade abort

**Assumption.** The routed `AccountId` is the authoritative beneficial-owner
scope for local self-trade comparison. Equal values therefore identify a
prohibited pair when an uncross explicitly selects `Abort`; unequal values are
treated as external counterparties. Authentication, omnibus/subaccount
mapping, and cross-shard ownership equivalence are upstream responsibilities.

Allocation under A61 or A110 remains authoritative. The A63 two-pointer walk
tests each next canonical pair before emitting it. At the first equal account,
the complete preparation fails with the exact account, buy order, sell order,
and positive prevented quantity. It does not search for another counterparty,
alter either fill vector, cancel/decrement interest, infer aggressor/resting
roles, consume a trade ID, or change book revision. A possible alternative
pairing does not change this result.

**Dependent results.** [A1, A4, A15, A60, A61, A62, A63, A64, A65, A66,
A67, A68, A74, A75, A78, A86, A97, A99, A108, A110, A111, A112, A113]
Detection adds constant work per pair and retains the `O(T)` pairing bound and
constructor-owned storage. The sequenced engine maps the direct diagnostic to
one `SelfTradeWouldOccur` business rejection, remains `Frozen`, and preserves
the current indication. Exact retry reuses the report. Coupled risk changes no
reservation, exposure, position, or netting scratch. Public payload version 5
emits `NoPublicChange` for the original rejection and no retry update.

WAL v20 encodes self-trade policy tag `1` and rejection tag `23`; its generic
one-event rejected report is 49 B. Snapshot v20 retains the command/report row
without changing direct order, counter, phase, indication, or risk rows. An
accepted `Abort` uncross is valid only when all canonical pairs have unequal
account IDs. `Permit` retains the prior same-account trade and coupled net-zero
position behavior.

**Falsification probe.** Exercise both allocation methods, all remainder
policies, a first-pair conflict, a conflict after one or more external pairs,
multiple orders per account, partial fills, and a book where an alternative
pairing could avoid the conflict. Require the exact first canonical diagnostic,
unchanged active orders/aggregates/revision/trade ID, returned buffer capacity,
and successful later `Permit` continuation. Exercise an all-external `Abort`
uncross and require ordinary commit.

At engine, risk, publisher, WAL, and snapshot boundaries, require one sequenced
rejection, frozen phase, preserved indication and risk state, 49 B stable
report tags, exact retry without event/update/frame growth, full-WAL recovery,
direct checkpoint recovery, and successful structural audits. Corrupt policy,
rejection, outcome/event agreement, or an accepted trade account pair and
require decode/replay failure. Any alternate re-pairing, partial trade/order/
risk mutation, counter advance, buffer leak, public identity disclosure,
second retry effect, recovery divergence, or acceptance under an incorrect
account-ownership mapping falsifies A113.

## A114 — atomic FOK decrement-and-cancel

**Assumption.** A continuous FOK order is filled only by external trades for
its complete original quantity. Decrementing prevented self quantity is not a
fill. Its allocation-free preflight therefore treats the first priority-
reachable self order under `DecrementAndCancel` as the same eligibility barrier
used by cancel-aggressor and cancel-both. External liquidity after that barrier
is ineligible.

For a displayed-class self order, the barrier follows only current slices of
earlier displayed orders because reserve refreshes rejoin the class tail. For a
hidden-class self order, it follows all total leaves in the displayed class and
the leaves of earlier hidden orders. If the original quantity is available
before the barrier, ordinary matching completes without reaching the self
order and emits no STP event. Otherwise the command returns
`InsufficientLiquidity` before any maker, STP, sequence, risk, reservation, or
public-depth mutation. Dormant FOK stops apply the identical rule at activation
and use `TriggeredFokUnfilled` on failure. A105 remains separate because a
minimum-quantity IOC can continue after meeting its threshold. A115 supplies
that TIF's distinct exact two-counter virtual reserve-queue simulation; it must
not be reused for FOK's first-self-barrier rule.

**Dependent results.** [A1, A9, A15, A20, A21, A22, A37, A39, A45, A49,
A50, A70, A83, A88, A102, A103, A105, A114, A115] Direct and dormant activation,
coupled risk, market-data publication, checkpoint restoration, WAL recovery,
and exact retry reproduce one atomic result. Preflight remains
`O(O_c + P_c log(P + 1))` time for `O_c` inspected orders across `P_c` crossed
levels, uses `O(1)` auxiliary space, and allocates nothing.

**Falsification probe.** Place displayed, reserve, and fully hidden self orders
before, within, and after sufficient external liquidity at the same and worse
prices. Exercise partial current reserve slices, multiple refreshes, direct and
dormant entry, both sides, risk reservations, public no-change/trade
projection, stable WAL-v20/snapshot-v20 recovery, exact retry, and later
successful external continuation. Differentially compare at least 20,000
generated books with a literal slice/requeue reference queue. Any accepted
partial FOK, counted self decrement, reachable self STP event, mutation on
failure, ignored earlier barrier, rejection despite a complete external fill
before self, allocation, replay divergence, or reuse of A115 continuation
semantics falsifies A114.

## A115 — exact minimum-quantity decrement-and-cancel

**Assumption.** For `ImmediateOrCancelWithMinimum` under
`DecrementAndCancel`, the minimum is satisfied only by external traded
quantity. A self maker instead consumes the lesser of incoming leaves and that
maker's current executable slice from both sides, emits no trade, and supplies
no threshold credit. The immutable preflight follows the exact execution
priority walk: initial displayed and reserve slices in FIFO order, refreshed
reserve slices requeued at the displayed-class tail, fully hidden FIFO only
after every displayed reserve is exhausted, and then the next crossed price.

The scanner carries separate incoming-leaves and remaining-external-threshold
counters. It aggregates complete reserve-refresh rounds, evaluates at most one
partial round in FIFO order, and never materializes a virtual queue. If
incoming leaves reach zero before the external threshold, the accepted command
cancels its complete original quantity with
`MinimumQuantityUnavailable`; maker, STP, risk, reservation, position,
and public state remain unchanged. If the threshold reaches zero
first, ordinary IOC execution proceeds from the original state, may execute
beyond the threshold, emits the ordinary decrement/refresh/trade trace, and
cancels only remaining incoming leaves. Dormant minimum-quantity stops apply
the identical rule against activation-time liquidity.

**Dependent results.** [A1, A5, A9, A15, A20, A21, A22, A37, A39, A45, A50,
A70, A83, A88, A102, A103, A105, A114, A115] For `O_c` initially inspected
orders over `P_c` crossed prices, `D_p` displayed orders at price `p`, and at
most `R_p` remaining reserve rounds there, the scan costs
`O(O_c + P_c log(P + 1) + sum_p D_p log(R_p + 1))` time, `O(1)` auxiliary
space, and zero allocations. Instrument admission bounds each replenishment
count by `u32`, so the binary search performs at most 32 aggregate passes per
price. Direct and dormant entry, risk, private/public projection, checkpoint,
WAL-v20, snapshot-v20, exact retry, and subsequent execution reproduce the
same result. Legacy rejection tag `51` remains decodable but is not emitted for
a well-formed A115 command.

**Falsification probe.** Place external and self displayed, reserve, and fully
hidden orders before and after the threshold at one or multiple crossed prices
on both sides. Exercise threshold-minus-one, exact-threshold, and
threshold-plus-one external quantities; partial initial slices; zero, one, and
maximum replenishments; a partial final reserve round; hidden-class entry;
incoming exhaustion on self decrement; direct and dormant activation; risk
reservations and positions; public no-change/trade projection; checkpoint and
WAL recovery; exact retry; and later accepted continuation. Differentially
compare at least 20,000 generated cases with a literal slice/requeue reference
queue. Any self threshold credit, priority inversion, skipped or duplicated
refresh, hidden-before-displayed execution, maker/STP/risk/public mutation on
failure, threshold-capped success, allocation, nontermination, replay
divergence, or current emission of rejection tag `51` falsifies A115.

## A116 — instrument-bound atomic call-auction settlement

**Assumption.** One settlement input is a complete accepted A64 uncross report,
not a selected trade subset or an indicative result. Its command sequence and
all event sequences are positive; event sequences are contiguous; every event
has the report command ID; and the final event is `UncrossCompleted`. Its
positive trade count and cancellation count exactly partition the preceding
trade prefix and remainder-cancellation suffix. Every trade uses the declared
clearing price, their checked quantity sum equals the declared executable
quantity, and every trade carries the supplied immutable A1 instrument ID and
definition version.

The caller supplies exactly one globally unique `TransactionId` per trade in
report order plus one authoritative A33 effective date and A34 nonregressing
booking timestamp. For raw signed price `p`, lot quantity `q`, base units per
lot `b`, and quote units per price unit `c`, the shared DVP constructor computes
`base = q × b` and `quote = p × q × c` in checked signed `i128`. Buyer and
seller receive exact opposite base and quote postings; a zero quote value omits
both zero legs. A same-account pair is rejected even when A113's separate
auction policy is `Permit`.

All entries are constructed before ledger mutation. One trade maps to one
ordinary entry; multiple trades map to one ordered A42 batch. The in-memory and
durable APIs reuse A11 transaction idempotency, A29/A89 checkpoints, A42/A79
batch atomicity, A65/A66 auction recovery, and the existing entry/batch WAL
append-before-commit paths. No clearing lifecycle authorization, novation,
allocation account, fee, settlement-date derivation, custody, money-settlement,
or legal-finality rule is inferred. The linked-obligation boundary is
consistent with [CPSS-IOSCO PFMI Principle 12](https://www.bis.org/cpmi/publ/d101.htm)
and the [CPMI DvP report](https://www.bis.org/cpmi/publ/d06.htm); the exact
mapping and arithmetic are Quotick internal contracts.

**Dependent results.** [A1, A6, A7, A11, A12, A15, A17, A29, A33, A34,
A42, A43, A64, A65, A66, A69, A79, A89, A90, A113, A116] For `T` trades,
`C` cancellations, `L <= 4T` non-zero posting legs, and `U` affected
`(AccountId, AssetId)` keys, report validation is `O(T + C)` time and `O(1)`
auxiliary space. Settlement construction fallibly reserves exactly `T` entry
handles and owns `O(T + L)` result storage. The single-trade path uses ordinary
entry preparation. The multi-trade path adds expected `O(T)` construction time
and `O(T)` identity storage, followed by A79's `O(L log L)` preparation,
`O(T + L + U)` auxiliary storage, and expected `O(T + U)` commit. A full
adversarial transaction-hash collision cluster can make batch identity and
overlay work `O(T²)` without storage growth.

WAL/snapshot version 20 preserves trade identity before settlement. One
multi-trade settlement occupies one kind-`7` frame and one batch checkpoint
record. Exact retry appends no frame and applies no second effect. A separately
committed subset or different grouping is a typed nonmutating partial-commit or
collision failure. Construction, arithmetic, period, timestamp, balance,
capacity, append, and recovery failure cannot expose a committed trade prefix.

**Falsification probe.** Exercise single- and multi-trade uncross reports,
multiple pairs at one price, signed and zero prices, maximum factors, identical
accounts under `Permit`, duplicate transaction IDs, instrument/version drift,
too few/many transaction IDs, rejected and non-uncross reports, missing or
reordered events, corrupted command/event sequences, wrong body counts,
non-trade prefix values, wrong clearing price, and aggregate-quantity mismatch.
Precommit no, one, and all member transactions both separately and under a
different batch. Force settlement-entry reservation failure, final `i128`
overflow, every ledger-capacity boundary, a closed date, and timestamp
regression; compare all balances, indexes, records, sequence, and WAL length
before and after failure.

For durable settlement, terminate at every frame-write/barrier/commit boundary;
repair a torn final frame; recover from uncut WAL and both A/B checkpoint slots;
continue through a suffix; and retry before and after reopen. Any accepted
report contradiction, mismatched definition, self-settlement, partial economic
effect, split WAL/checkpoint grouping, retry frame growth, recovery divergence,
or venue/clearing policy inference falsifies A116.

## A117 — fallible continuous order-book query output

**Assumption.** A read-only query observes one immutable borrow of one A3
continuous-book shard. `try_depth` exposes only non-zero public displayed
aggregates, with bids descending and asks ascending, and reserves at most the
lesser of the requested limit and occupied execution-price count before
traversal. `depth_iter` exposes the same market-priority sequence without
caller-owned output allocation. It is double-ended and retains occupied-price
cardinality as an upper size hint because hidden-only prices remain absent.
`depth_range_iter` restricts that projection to exact inclusive price
endpoints through the shared AVL band traversal; an inverted range is empty.
`try_depth_range` counts the selected visible rows without allocation,
reserves that exact semantic cardinality, and copies through a second
identical traversal.
`try_active_orders` reserves the exact indexed non-dormant count, rejects any
traversal/cardinality contradiction before vector growth, validates every
snapshot, and sorts the complete private result by `OrderId`.

`try_account_active_order_ids` performs one expected constant-time A41 account
lookup, derives and fallibly reserves the selected side/all count, traverses
only those intrusive links, validates owner, side, declared list length, and
unique identity, and sorts by `OrderId`. An unknown account returns an empty
vector. Reservation follows the standard library's
[`Vec::try_reserve_exact`](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.try_reserve_exact)
contract: the requested semantic maximum is exact for the query, while the
allocator may provide additional physical capacity. No query mutates the book,
and no partial vector is returned on resource or invariant failure.

**Dependent results.** [A3, A10, A12, A41, A52, A72, A83, A117] For `P`
occupied execution prices, `V <= P` public prices, and depth limit `L`, depth
costs `O(P)` time in the hidden-only worst case, returns
`O(min(V, L))` rows, and requests at most `min(P, L)` capacity.
Depth-iterator setup is `O(log(P + 1))`; complete traversal is `O(P)` time and
`O(1)` auxiliary space.
For an inclusive band containing `K` occupied execution prices, range setup
and inspection cost `O(log(P + 1) + K)` time and `O(1)` auxiliary space;
hidden-only prices may be inspected but are not returned. Fallible range
materialization makes two such passes, owns `O(V_b)` output for the selected
visible count `V_b <= K`, and reserves exactly `V_b` rows.
For `T` active
identities, `S` dormant stops, and `R = T - S` resting orders, complete private
output costs `O(T + R log R)` time and `O(R)` result space. For `K` selected
account orders, expected work is `O(1) + O(K log K)` and result space is
`O(K)`, independent of unrelated active orders. Existing convenience methods
delegate to these paths and retain A12's panic boundary. Read-only query output
changes no WAL or snapshot state and therefore requires no wire-version change.

**Falsification probe.** Compare fallible and convenience results for empty,
one-sided, crossed, reserve, and fully hidden books; both sides; limits `0`, `1`,
occupied cardinality, and `usize::MAX`; unknown accounts; side/all account
scope; inclusive, outside, and inverted price bands; forward/reverse range
consumption; and noncanonical dense/hash iteration order. Require market-priority
public depth, hidden-only exclusion, canonical ascending private identities,
exact selected-row range reservation, exact reported reservation resources and
maxima, and byte-identical book state
before and after every success and failure. Corrupt dormant/resting counts,
account heads, tails, counts, links, owners, sides, and duplicate membership in
white-box fixtures. Inject every output-reservation failure and exercise at
least 250,000 active identities under allocation counting. Any implicit vector
growth, leaked partial output, unrelated-order scan in an account query,
private identity in public depth, nondeterministic ordering, mutation, or
untyped fallible-path failure falsifies A117.

## A118 — atomic trade-bound call-auction fees

**Assumption.** A fee input extends one complete A116 settlement; it is not an
independent trade assertion. Each `CallAuctionFee` carries one globally unique
`TransactionId`, one book-local `TradeId`, distinct debit and credit
`AccountId` values, one explicit `AssetId`, and one positive signed-`i128`
amount in that asset's smallest ledger unit. The debit account supplies the
amount and the credit account receives it. A rebate reverses those accounts
rather than using a zero or negative amount.

The fee vector is grouped in the report's canonical strictly increasing trade-
ID order. For each trade, `from_report_with_fees` constructs the A116 DVP entry
and then every contiguous fee entry bound to that trade. Trades may have zero,
one, or multiple fees. An unknown or reordered binding and any global
transaction-ID duplication across DVP or fee entries fail before ledger
mutation. Fee calculation, account-role mapping, asset selection, and tax or
disclosure parameters are authoritative external inputs. Authorization remains
an external lifecycle responsibility; the ledger does not infer or attest it.

**Dependent results.** [A1, A7, A11, A12, A17, A29, A33, A34, A42, A43,
A64, A65, A66, A69, A79, A89, A90, A116, A118] For `T` trades, `C`
cancellations, `F` fees, `N = T + F` entries, `L <= 4T + 2F` non-zero posting
legs, and `U` affected balance keys, report and fee-binding validation is
`O(T + C + F)` time and `O(1)` auxiliary space. Construction fallibly reserves
exactly `N` entry handles and two postings per fee and owns `O(N + L)` result
storage. Except for `T = 1, F = 0`, A42 batch construction adds expected
`O(N)` time and `O(N)` identity storage; A79 preparation is `O(L log L)` with
`O(N + L + U)` auxiliary storage, and commit is expected `O(N + U)`. A full
adversarial transaction-hash collision cluster can make identity and overlay
work `O(N²)` without storage growth.

One fee-enriched settlement occupies one ordinary kind-`7` ledger-batch frame
and one batch checkpoint record. Standard entry/batch encoding already carries
the postings, references, effective date, and booking timestamp, so WAL and
their values remain unchanged inside WAL/snapshot version 20. DVP and fees
share one event sequence, final
balance image, capacity decision, exact-retry identity, append-before-commit
transition, checkpoint, and recovery result. No prefix is observable after
construction, capacity, period, timestamp, balance, WAL, or recovery failure.

**Falsification probe.** Exercise zero, one, and multiple fees per trade;
fee-free trades between fee-bearing trades; buyer, seller, and third-account
debits; third-asset fees; reversed-account rebates; maximum positive amounts;
and single- and multi-trade reports. Reject zero/negative amounts, identical
fee accounts, unknown trade IDs, reordered fee groups, duplicate DVP/fee and
fee/fee transaction IDs, nonmonotonic report trade IDs, closed dates, timestamp
regression, final-balance overflow, every entry/posting/record capacity, and
every fee-posting/settlement-entry reservation failure before mutation.

Precommit one DVP or fee transaction separately and under another batch. For
durable settlement, terminate at every frame-write/barrier/commit boundary,
repair a torn tail, recover from uncut WAL and both A/B checkpoint slots, apply
a suffix, and retry before and after reopen. Compare every balance, transaction
reference, posting, record, sequence, checkpoint image, and WAL length. Any fee
bound to another trade, sign/direction ambiguity, partial DVP or fee effect,
second retry effect, frame split, recovery divergence, or inferred external fee
policy falsifies A118.

## A119 — atomic full-settlement call-auction correction

**Assumption.** One correction is an authoritative, already-authorized
instruction over one exact A116/A118 settlement previously committed through
`settle_call_auction`. The caller supplies exactly one new globally unique
reversal `TransactionId` per original DVP and fee entry in canonical settlement
order, one explicit A33 effective date, one A34 booking timestamp, and one
reference. A bust contains those exact inverses. A replacement correction
appends one separately validated complete settlement after every inverse; it
does not select a trade subset, amend an entry in place, or infer replacement
fees.

Application requires identical original entry content and the exact original
entry/batch grouping. It does not rewind the A64 engine, auction-risk state,
private/public market data, or an external position system. Correction reason,
authorization, external lifecycle coordination, and statement evidence are
authoritative upstream inputs.

**Dependent results.** [A1, A7, A11, A12, A17, A29, A33, A34, A42, A43,
A64, A65, A66, A69, A79, A89, A90, A116, A118, A119] For `N` original
entries, `M` replacement entries, `K = N + M` correction entries,
`L_o` original posting legs, `L_r` replacement posting legs,
`L = L_o + L_r`, and `U` affected balance keys, construction is
`O(K + L_o)` time and owns `O(K + L_o)` new entry-handle and reversal-posting
storage. It fallibly reserves exactly `K` correction handles and exactly each
original entry's posting count for its inverse. Except when `N = 1, M = 0`,
A42 batch construction adds expected `O(K)` identity work and `O(K)` storage;
A79 preparation is `O(L log L)` with `O(K + L + U)` auxiliary storage, and
commit is expected `O(K + U)`.

The original event's content and grouping are proved before mutation. One-entry
busts use an ordinary entry; every other correction is one ordered batch. The
standard entry/kind-`7` batch encoding therefore yields one WAL frame, one
checkpoint record, one final balance image, exact retry without frame growth,
and unchanged value semantics inside WAL/snapshot version 20.

**Falsification probe.** Exercise fee-free and fee-enriched one/multi-trade
busts; replacement settlements with different trades and fees; zero, duplicate,
colliding, too few, and too many correction identities; absent, content-
colliding, separately committed, differently ordered, and differently grouped
originals; already-reversed and non-reversible targets; closed effective dates;
timestamp regression; final-balance overflow; and every reversal, transaction,
posting, record, and per-record capacity boundary. Require every inverse before
every replacement and compare the final balances with an independent literal
inverse-plus-replacement model.

Terminate durable correction at every frame-write/barrier/commit boundary,
repair a torn tail, recover from uncut WAL and both A/B checkpoint slots, apply
a suffix, and retry before and after reopen. Any accepted original-group
contradiction, partial inverse/replacement effect, matching/risk state mutation,
second retry effect, split frame/checkpoint record, recovery divergence, or
inferred authorization/reason/external synchronization falsifies A119.

## A120 — zero-copy live command/report history

**Assumption.** The continuous `OrderBook` and sequenced
`CallAuctionEngine` retain one bounded, append-only command/report cache per
generation. A completed accepted command or business rejection is inserted
only at its exact next command sequence; exact retries do not insert. A84 and
A75 audit the continuous and auction dense orders, respectively. Full-WAL
replay and checkpoint restoration rebuild those rows in the same chronological
order. The cached canonical report remains `replayed = false`; replay marking
is applied only to the response clone returned by an exact retry.

`retained_command_report` and `retained_history` borrow this existing cache
under one immutable engine borrow. They create neither a second authoritative
store nor an owned output collection. The local interfaces provide no
principal authentication, account filtering, entitlement, remote pagination,
transport framing, eviction, or generation rollover.

**Dependent results.** [A3, A4, A9, A10, A12, A49, A65, A66, A75, A84,
A85, A120] Lookup by `CommandId` is expected `O(1)` outside adversarial hash
clusters. Complete iteration is `O(C)` for `C` retained commands with `O(1)`
iterator state. Both operations allocate no output storage, clone no command
or report, copy no event, construct no checkpoint, and perform no matching,
risk, auction, WAL, snapshot, sequence, or capacity mutation. The borrowed row
contains the exact cached command and report, includes accepted and rejected
outcomes, and remains available after direct, full-WAL, or checkpoint-plus-
suffix recovery. No wire version changes because no persisted value changes.

**Falsification probe.** Insert accepted and business-rejected continuous and
auction commands, perform exact retries and differing-content collisions, and
query missing and present identities at ordinary and total history capacity.
Compare lookup and iteration addresses, commands, reports, replay flags,
command/event sequences, exact iterator length, cache/event telemetry, and
complete state bytes before and after every query. Repeat after full-WAL,
direct-checkpoint, and checkpoint-plus-suffix restoration, including a source
suffix after capture. Deliberately reorder dense history or corrupt its event-
arena ranges and require the existing structural audits to fail. Any query
allocation, copy, reordered/duplicated/omitted row, `replayed = true` cached
report, retry insertion, state or capacity mutation, recovery difference, or
borrowed view surviving a mutable engine transition falsifies A120.

## A121 — zero-copy fail-closed ledger history inspection

**Assumption.** A borrowed ledger-history query observes one immutable borrow
of one A29 ledger generation. `try_record_view` uses stable one-based event
sequences: zero and positions beyond the retained journal return `Ok(None)`.
For a retained position it resolves the journal record against the
authoritative transaction index. Absence, sequence/identity disagreement, or
batch-content disagreement is a typed `LedgerHistoryError`, never an absent
record result.

`LedgerRecordView` preserves the exact entry, correction, or ordered-batch
grouping. `LedgerRecordTransactions` follows event-declared transaction order.
`retained_history` is chronological, exact-size, and double-ended by record;
the per-record transaction iterator is also exact-size and double-ended. These
interfaces create no second history store, own no output collection, clone no
entry or batch, and perform no balance, index, journal, capacity, WAL, snapshot,
or checkpoint mutation. Error-detail formatting after a contradiction remains
an A12 allocation boundary.

**Dependent results.** [A11, A12, A29, A30, A42, A69, A79, A89, A90, A121]
One entry or correction resolves in expected `O(1)` time. A batch with `N`
transactions resolves in expected `O(N)` time. Complete traversal of `R`
records containing `T` transactions therefore performs expected `O(T)` index
work with `O(1)` iterator state and no output allocation. A full adversarial
transaction-index collision cluster can require `O(T^2)` complete-traversal
work without storage growth. The cloned `record` compatibility query and A89
checkpoint materialization compose the same typed resolver and clone only the
A90 immutable outer handles after consistency succeeds. Direct checkpoint,
full-WAL, and checkpoint-plus-suffix recovery expose the same grouping,
sequence, content, and ordering without a wire-version change.

**Falsification probe.** Exercise empty, single-entry, correction, and batch
history; zero, first, last, and out-of-range sequences; alternating front/back
record traversal; alternating front/back transaction traversal; and exact
iterator lengths. Compare pointer identity with transaction lookup and value
parity with cloned `record`. Remove an indexed transaction; alter its sequence,
identity, or batch content; require the exact query contradiction from direct
lookup and iteration and a typed invalid checkpoint capture from the shared
resolver. Repeat after direct-checkpoint, full-WAL, and checkpoint-prefix/WAL-
suffix recovery while checking balances, generation, capacities, journal/index
contents, and WAL length before and after every query. Count allocations on the
successful paths.

Any output allocation, nested clone, grouping/order/sequence drift, silent
absence for a retained contradiction, partial history collection, mutation,
recovery difference, divergent checkpoint resolution, or borrowed view
surviving a mutable ledger transition falsifies A121.

## A122 — exact point-in-time ledger balances

**Assumption.** A point-in-time balance generation denotes one exact completed
ledger-record boundary. Generation zero is the empty ledger; a generation
beyond the current journal head is a typed nonmutating failure. The query
observes one immutable ledger borrow and composes A121's fail-closed journal/
transaction-index resolution. Entry, correction, and ordered-batch records are
indivisible; no correction or batch member is a queryable boundary.

For the selected `(AccountId, AssetId)` key, reconstruction scans each
record's transaction entries in event-declared order and uses the canonical
`(asset, account)` posting order for binary-search lookup. While both term
signs remain, it consumes a term opposite to the current accumulated sign.
Once one sign remains, the accumulated value moves monotonically toward the
record's atomic final value. A current-generation query additionally requires
the reconstructed value to equal the authoritative balance index. The query
owns no output, allocates no auxiliary storage, and changes no balance, index,
journal, capacity, WAL, snapshot, or checkpoint state.

**Dependent results.** [A6, A11, A12, A29, A31, A36, A42, A69, A79, A89,
A90, A121, A122] Generation zero and absent keys return zero. Corrections and
batches expose only their atomic final effects, including cancelling signed
extremes whose member order would overflow. For `E` inspected transaction
entries whose posting counts are `L_i`, the query performs expected
`O(E + sum(log(L_i + 1)))` time and `O(1)` auxiliary space; the two sign passes
are a constant factor. A full adversarial transaction-index collision cluster
can increase index resolution to `O(E^2)` without storage growth. Direct
checkpoint, full-WAL, and checkpoint-prefix/WAL-suffix recovery reproduce the
same generation and balance without a wire-version change.

**Falsification probe.** Query empty, first, intermediate, current, and future
generations; known and absent keys; positive, negative, and zero crossings;
corrections; batches; and cancelling `i128` extremes. Differentially compare
at least 1,024 generated record boundaries with balances captured immediately
after commit. Remove or alter indexed history, force an unrepresentable atomic
result in the reconstruction kernel, corrupt the current balance index, and
require exact typed failures. Repeat after direct checkpoint, full-WAL, and
checkpoint-prefix/WAL-suffix restoration while checking journal/index
capacity, generation, balance, and WAL length before and after each query.
Any observable member boundary, false transaction-order overflow, silent
history contradiction, current-index divergence, allocation, mutation, or
recovery difference falsifies A122.

## A123 — allocation-free inclusive price-band depth

**Assumption.** The caller's `RangeInclusive<Price>` endpoints are exact and
inclusive. A lower endpoint greater than its upper endpoint denotes an empty
band. One immutable continuous or call-auction book or public-replica borrow
remains stable for the complete query. Both books and both replica types
compose one A55 stable-slot AVL range primitive, which initializes independent
forward and reverse fixed stacks and does not linearly traverse occupied keys
outside the band.

Bid results remain descending and ask results ascending. Continuous output
contains only non-zero public displayed aggregates, so an in-band hidden-only
execution price is inspected but omitted by the authoritative book and remains
absent from its public replica. Call-auction output contains only
limit-constrained aggregates; market-constrained interest remains separate in
the authoritative book and replica. Iterator construction and traversal
allocate no output or auxiliary heap storage. Each fallible materializer first
counts selected rows without allocation, reserves exactly that semantic
cardinality, and copies through a second equivalent traversal; no partial
vector is returned.

**Dependent results.** [A1, A3, A12, A55, A62, A67, A70, A72, A74, A117,
A123] For `P` occupied side prices, `K` in-band occupied prices inspected, and
`V <= K` selected output rows, iterator work is `O(log(P + 1) + K)` time and
`O(1)` auxiliary space. Fallible materialization makes two traversals with the
same asymptotic bound, requests exactly `V` output slots, and owns `O(V)`
result space. The two fixed 128-index stacks retain A55's
`256 × size_of::<usize>()` traversal bound. The query changes no matching,
auction, replica, WAL, snapshot, or wire state.

**Falsification probe.** Exercise empty, singleton, full, outside, absent-
endpoint, and inverted bands after every rotation and deletion shape. Consume
forward, reverse, and mixed ends through exhaustion and require fused empty
behavior without duplicates. Bound ordered comparisons for a narrow band in a
1,023-key tree and differentially compare at least 20,000 generated mutation/
range steps with `BTreeMap`. At all four public book/replica APIs, test both
sides, limits `0`, `1`, and `usize::MAX`, hidden-only continuous levels,
separate auction market interest, exact fallible/convenience parity, replica/
authoritative parity, typed reservation failure, unchanged resource telemetry,
and complete post-query validation. Any
out-of-band row, missed inclusive endpoint, duplicate, non-market ordering,
outside-band linear scan, output/traversal allocation, partial failure output,
state mutation, or model divergence falsifies A123.

## A124 — exact immediate-execution economics

**Assumption.** One `ImmediateExecutionRequest` binds a hypothetical
aggressor's `AccountId`, side, positive lot quantity, shared market-or-limit
`StopActivation` constraint, and one of the four continuous self-trade
prevention policies. One immutable `OrderBook` borrow remains stable for the
complete query. The scan follows the exact private execution topology:
displayed and reserve orders precede fully hidden orders at one price, FIFO
holds within each class, and each reserve refresh rejoins the displayed-class
tail before hidden liquidity. Cancel-resting logically removes self orders
from the hypothetical path; cancel-aggressor and cancel-both stop at the first
reachable self order; decrement-and-cancel consumes the lesser of incoming
leaves and each self maker's current executable slice without trade.

The result binds the instrument ID, immutable definition version, last visible
book event sequence, and complete request. Requested lots equal external
executed lots plus decrement-and-cancel self-trade-consumed lots plus unfilled
lots. Raw-price notional is the exact signed `i128` sum of
`price.raw() × executed lots`; the final external execution price is the worst
price, and the distinct contributing-price count is exact. Termination
distinguishes a complete external fill, self-trade prevention, the supplied
price limit, and book exhaustion. The quote is an
immutable observation, not admission, an account-control or risk decision, a
fee calculation, a liquidity reservation, a command commitment, or a report
of the resting-order cancellations that live cancel-resting or cancel-both
would emit.

**Dependent results.** [A1, A2, A3, A4, A7, A12, A19, A22, A44, A45, A47,
A55, A72, A83, A103, A115, A124] Ordinary policies inspect `O_c` orders over
`P_c` crossed prices in `O(O_c + P_c log(P + 1))` time, `O(1)` auxiliary
space, and zero allocations. For decrement-and-cancel, with `D_p` displayed
orders and at most `R_p` remaining reserve rounds at price `p`, exact work is
`O(O_c + P_c log(P + 1) + sum_p D_p log(R_p + 1))`; A115's binary search
performs at most 32 aggregate passes because admission bounds `R_p` by `u32`.
Output is one fixed-size value. Since total executed quantity is at most
`u64::MAX` lots and `|price.raw()| <= 2^63`, the absolute accumulated notional
is less than `2^127` and is representable by `i128`. The query changes no
matching, risk, reservation, sequence, WAL, snapshot, or public state and
requires no wire-version change.

**Falsification probe.** For both sides, market and limit constraints, signed
prices, every termination, and all four self-trade policies, compare the quote
with a committed immediate-order trace from the identical state. Exercise
displayed, reserve, and hidden self/external orders, partial initial slices,
multiple refresh rounds, price-limit boundaries, exhausted books, and
`i64::MIN`, `i64::MAX`, and `u64::MAX` arithmetic. Differentially compare at
least 20,000 generated multi-price books with an independent literal two-class
slice/requeue model. Require exact quantity partitioning, signed notional,
worst price, request/provenance binding, unchanged private/public/history state,
and zero query allocations. Any priority or STP divergence, self quantity
credited as execution, incorrect termination, count, or notional, overflow,
mutation, allocation, stale provenance at the observed boundary, or implied
execution commitment falsifies A124.

## A125 — exact current-slice queue position

**Assumption.** One `try_order_queue_position` query binds one `OrderId` to one
immutable continuous `OrderBook` borrow. An unknown identity or accepted
dormant stop has no resting queue position and returns absence. A resting
target returns its complete private snapshot and displayed-priority or hidden
queue class. For a displayed-class target, executable quantity ahead is the
sum of current positive working slices of earlier displayed-class orders;
reserve quantity behind those slices is excluded because each refresh rejoins
the class tail behind the target. For a hidden target, quantity ahead is the
total leaves of every displayed-class order plus every earlier hidden order;
all displayed reserve refreshes remain ahead of the hidden class.

The result counts distinct predecessor orders and exact lots before the
target's current executable slice. It binds instrument ID, immutable definition
version, and last visible book event sequence. It does not predict the target's
position after its current reserve slice refreshes, execution probability,
elapsed wait, future arrivals/cancellations, STP behavior of an unknown
aggressor, or remote quote validity. The query walks the existing predecessor
links and relevant price-level metadata; it creates no second queue or owned
output. Relevant identity, side, price, display-class, working/leaves, level-
head/count, and reciprocal-next contradictions are typed failures. Formatting
failure detail after detection remains an A12 allocation boundary.

**Dependent results.** [A1, A3, A10, A12, A22, A44, A45, A47, A72, A83,
A103, A125] For `K` distinct predecessors at one price among `O` active orders
and `P` occupied prices, target lookup is expected `O(1)`, level lookup is
`O(log(P + 1))`, and predecessor traversal is expected `O(K)`. Total expected
time is `O(log(P + 1) + K)`, auxiliary space is `O(1)`, and successful output
allocation is zero. A full adversarial active-order hash collision cluster can
increase predecessor resolution to `O(K O)` without storage growth. Checked
`u128` quantity accumulation and checked `u64` predecessor counting fail closed
on an unrepresentable result. No matching, risk, reservation, sequence, WAL,
snapshot, or public state changes, so no wire-version change follows.

**Falsification probe.** Place fully displayed, reserve, and fully hidden
targets at head, middle, and tail on both sides and multiple prices. Exercise
partial current slices, repeated reserve refresh, hidden-only levels, unknown
identities, dormant stops, and quantities above `u64::MAX` in aggregate.
Compare every resting order in at least 20,000 generated books with an
independent forward FIFO model. Corrupt index identity, predecessor/reciprocal
links, cycles, side, price, working/leaves relationships, class order, level
head, and level count and require a typed failure before output. Require exact
order/class/count/quantity/provenance, unchanged book telemetry/state, and zero
successful-path allocations. Any refresh counted ahead of a displayed target,
reserve leaves omitted ahead of a hidden target, priority divergence, silent
relevant corruption, mutation, allocation, or implied fill prediction
falsifies A125.

## A126 — zero-copy account-and-asset ledger statements

**Assumption.** `JournalEntry::posting` binary-searches the canonical
`(asset, account)` posting order established by entry validation. Because one
entry cannot repeat an `(AccountId, AssetId)` key, a present lookup identifies
exactly one immutable posting.

`Ledger::account_statement` observes one immutable ledger generation and
composes the A121 retained-history resolver. It yields one
`LedgerStatementLine` for every transaction containing the selected account
and asset, in ledger-record and event-declared transaction order. Each line
borrows the canonical `JournalEntry` and `Posting` and retains the enclosing
one-based sequence, complete `LedgerRecordView`, and zero-based transaction
position, so filtering does not erase entry, correction, or batch boundaries.
Reverse traversal yields the exact reverse order. A journal/index
contradiction is emitted as its typed `LedgerHistoryError` at that record
position rather than being filtered as absence.

The interface creates no second account-history index or owned output,
allocates no successful-path storage, and changes no balance, index, journal,
capacity, WAL, snapshot, or checkpoint state. It is process-local filtering,
not principal authentication, account authorization, entitlement, audit
export, remote pagination, transport, or generation rollover.

**Dependent results.** [A11, A12, A29, A30, A42, A69, A79, A89, A90, A121,
A126] For `R` records containing `T` transactions whose posting counts are
`L_i`, complete traversal performs expected
`O(T + sum(log(L_i + 1)))` work with `O(1)` iterator state and no output
allocation. A full adversarial transaction-index collision cluster can
increase history resolution to `O(T^2)` without storage growth. Empty and
absent-key statements are empty. Direct checkpoint, full-WAL, and checkpoint-
prefix/WAL-suffix recovery retain identical line order, values, grouping, and
pointer-borrowing semantics without a wire-version change.

**Falsification probe.** Exercise present and absent keys across ordinary
entries, both correction members, multi-entry batches, unrelated accounts and
assets, empty history, and interleaved forward/reverse traversal. Compare at
least 1,024 generated records with an independent literal transaction/posting
filter in both directions. Require entry/posting pointer identity, exact
sequence and transaction position, unchanged generation/balance/capacity
state, and no successful-path allocation. Remove or mismatch a transaction-
index row and require the exact A121 error rather than omission. Repeat after
direct-checkpoint, full-WAL, and checkpoint-prefix/WAL-suffix restoration. Any
duplicate, omitted, reordered, cloned, or wrongly grouped posting; linear leg
scan; silent contradiction; mutation; allocation; recovery difference; or
implied authorization falsifies A126.

## A127 — prevalidated private price-level order traversal

**Assumption.** One `try_price_level_orders` query observes one immutable
continuous `OrderBook` borrow at an exact `(Side, Price)` key. An unoccupied
key returns an empty iterator. An occupied key is fully validated before the
iterator is returned: every member must resolve through the active-order
index, match the selected side and price, be non-dormant, have valid positive
working/total leaves and display policy, follow reciprocal FIFO links in
displayed/reserve-before-hidden class order, and reproduce the level tail,
displayed tail, order counts, public quantity, and future-event aggregate.

After validation, `PriceLevelOrders` yields complete `OrderSnapshot` values in
executable displayed-class FIFO then hidden-class FIFO order. It is exact-size,
double-ended, and fused; mixed front/back consumption cannot duplicate or omit
a row. It retains only the immutable book borrow, selected key, front/back
identities, and remaining count. Raw FIFO links and internal level aggregates
are not public output. Safe Rust cannot mutate the borrowed book between
validation and exhaustion.

The query creates no second queue or owned output, allocates no successful-
path storage, and changes no order, level, account, sequence, history, risk,
WAL, snapshot, or market-data state. Human-readable failure detail after
pre-output corruption detection remains an A12 allocation boundary. The view
is process-local private state and supplies no authentication, authorization,
entitlement, remote pagination, or transport.

**Dependent results.** [A1, A3, A10, A12, A22, A44, A45, A47, A72, A83,
A103, A127] For `K` orders at the selected key among `O` active orders and `P`
occupied prices, lookup is `O(log(P + 1))`; prevalidation and complete
iteration each perform `K` expected `O(1)` active-order resolutions. Total
expected time is `O(log(P + 1) + K)` with `O(1)` iterator state and zero output
allocation. A full adversarial active-order collision cluster can increase
each pass to `O(K O)` without storage growth. Checkpoint and WAL restoration
reproduce the same queue because they rebuild the validated private FIFO; no
wire-version change follows.

**Falsification probe.** Exercise both sides, multiple and signed prices,
unoccupied and hidden-only levels, fully displayed/reserve/hidden members,
partial current slices, reserve refresh to the displayed-class tail, and mixed
front/back traversal while checking exact size after each step. Compare every
price level in at least 20,000 generated books with an independent forward-
link model in both directions. Corrupt missing identity, side, price, dormant
state, previous/next topology, cycle, working/leaves relationship, display
class order, head/tail, count, public aggregate, displayed tail, and event work;
require a typed constructor failure before any row exists. Repeat after direct
checkpoint and full-WAL recovery while checking complete state and telemetry
before and after the query. Any partial output before validation, duplicate,
omission, order/class divergence, leaked raw link, mutation, allocation,
recovery difference, or implied authorization falsifies A127.

## A128 — coherent provenance-bound best bid and offer

**Assumption.** `try_best_bid_offer` observes one immutable continuous
`OrderBook` state and composes its existing cached public best bid and offer.
Each present side is a `LevelSnapshot` containing only positive displayed
aggregate quantity and displayed-order count; a fully hidden-only price is not
a quote. Empty, bid-only, offer-only, and two-sided states remain distinct
through the two optional sides.

The fixed-size `BestBidOffer` binds both sides to the instrument identifier,
immutable instrument-definition version, and last committed book-event
sequence observed by the query. A present two-sided quote must be strictly
uncrossed. A zero cached aggregate or order count, or a locked/crossed pair,
returns a typed `InvariantViolation` before a value exists. This local check
does not replace the complete A45 price-index/extremum audit.

For a two-sided quote, `spread_raw` is exact offer raw price minus bid raw
price in raw-price units. The widest valid signed-domain spread is
`i64::MAX - i64::MIN = 18,446,744,073,709,551,615 = u64::MAX` raw-price units.
`midpoint_raw_numerator` is the exact bid-plus-offer raw-price sum in `i128`
with denominator two, so a half-unit midpoint is retained without a rounding
rule. Either arithmetic accessor returns absence unless both sides exist.

The successful path allocates nothing and changes no order, level, cache,
sequence, history, risk, WAL, snapshot, or market-data state. Human-readable
failure detail remains an A12 corruption-path allocation boundary. The value
is one shard-local source observation; it is not a consolidated cross-venue
quote, clock-synchronization proof, executable-liquidity reservation, or
remote transport message.

**Dependent results.** [A1, A3, A10, A12, A22, A44, A45, A72, A83, A103,
A128] Both cached-side reads, provenance capture, selected-aggregate checks,
cross check, and exact `i128` arithmetic are `O(1)` time and space with zero
output allocation. Direct checkpoint restoration reproduces the value because
the visible extrema are reconstructed and validated from semantic price-level
state; no wire-version change follows.

**Falsification probe.** Exercise empty, bid-only, offer-only, hidden-only,
and two-sided books; positive, negative, zero-adjacent, and signed-extreme
prices; multiple visible orders per level; and a better hidden-only price.
Require exact instrument/version/event-sequence provenance, unchanged state,
the full-domain `u64::MAX` spread, an exact `-1/2` raw midpoint at the signed
extremes, and direct-checkpoint equivalence. Corrupt each cached side to a zero
quantity/count and to a locked/crossed pair; require typed failure. Any mixed-
state provenance, hidden-liquidity disclosure, rounded midpoint, overflow,
panic, mutation, allocation, or consolidated/executable interpretation
falsifies A128.

## A129 — checked provenance-bound cumulative public depth

**Assumption.** `try_depth_range_summary` observes one immutable continuous
`OrderBook` state and uses the same stable-slot AVL inclusive range descent and
market-direction projection as the A123 public-depth iterator. It retains the
exact caller range start and end. An inverted range is empty.
`try_depth_summary` composes the same fold with the immutable instrument
definition's inclusive minimum and maximum prices.

Only displayed aggregate rows enter `DepthSummary`; a fully hidden-only price
is absent. Bids are inspected from high to low and offers from low to high, so
the first and last selected visible rows are the exact market-priority best and
worst prices. Each selected row must have positive aggregate quantity and
displayed-order count. Level count uses checked `usize`; displayed-order count
and displayed quantity use checked `u128` addition. Any contradiction or
overflow returns a typed `InvariantViolation` and discards the local partial
summary before output ownership changes hands. This aggregate fold does not
traverse private level membership or replace the complete A45 invariant audit.

The fixed-size result binds side, exact selection endpoints, best/worst prices,
level count, displayed-order count, and displayed lots to instrument identity,
immutable instrument-definition version, and last committed book-event
sequence. It contains no per-level rows, hidden quantity/count, price notional,
VWAP, currency conversion, fee, queue-priority, or executable-liquidity claim.

The successful path allocates nothing and changes no order, level, cache,
sequence, history, risk, WAL, snapshot, or market-data state. Human-readable
failure detail remains an A12 corruption-path allocation boundary. The result
is a shard-local state observation and supplies no remote pagination,
conflation, entitlement, clock-alignment, or transport semantics.

**Dependent results.** [A1, A3, A10, A12, A22, A44, A45, A72, A83, A103,
A123, A129] For `K` occupied execution prices in the selected band among `P`
occupied prices, setup plus traversal is `O(log(P + 1) + K)` time with `O(1)`
fixed output/state and no output allocation. Hidden-only in-band prices
contribute to `K` but not the totals. A full-side summary has `K = P`. Direct
checkpoint restoration reproduces the same result because semantic price
levels and public extrema are reconstructed and validated; no wire-version
change follows.

**Falsification probe.** Exercise both sides; empty, one-sided, hidden-only,
and mixed visible/hidden books; full-definition and narrow inclusive bands;
outside, single-price, and inverted ranges; multiple rows and orders; signed
prices; and direct checkpoint restoration. Compare totals and best/worst
prices with a literal checked fold over `depth_range_iter`. Require exact
instrument/version/event-sequence/side/range provenance and unchanged state.
Corrupt a selected row to zero quantity and zero count, then construct a
multi-row `u128` quantity overflow; require typed failure without partial
output. Any hidden disclosure, direction error, endpoint loss, unchecked wrap,
partial result, mutation, allocation, recovery difference, or implied notional,
VWAP, consolidated, or executable interpretation falsifies A129.

## A130 — poison-aware public-replica BBO and band summaries

**Assumption.** A healthy `MarketDataReplica` has applied one contiguous
instrument/version source sequence or one non-stale validated repair snapshot.
`try_best_bid_offer` and `try_depth_range_summary` observe one immutable replica
state and bind output to its instrument identity, immutable definition version,
and final applied source sequence. A poisoned replica returns
`MarketDataError::Poisoned` before inspecting or returning derived state.

Replica `try_best_bid_offer` reuses the exact A128 `BestBidOffer` constructor
and validation. Empty and one-sided states retain absent sides; present extrema
must have positive public quantity/count and remain strictly uncrossed. Replica
`try_depth_range_summary` reuses the exact A129 `DepthSummary` accumulator over
the existing inclusive market-direction range iterator. It retains caller
endpoints, treats inversion as empty, and uses checked level, public-order, and
public-quantity arithmetic.

Every active replica level is public; private order membership and hidden
liquidity do not exist in this consumer state. Invalid selected aggregates,
locked/crossed extrema, or cumulative overflow return a typed
`MarketDataError::SourceDivergence` without poisoning or exposing a partial
value. These local checks do not replace `MarketDataReplica::validate` or
source-sequence/snapshot grammar validation.

The replica exposes only explicit-band summaries. Its constructor binds
instrument identity/version but intentionally owns no `PriceRules`, so it
cannot label a definition-wide minimum-to-maximum selection without a separate
versioned definition input. Inventing raw-domain or observed-domain endpoints
would make otherwise equal source and replica summaries semantically unequal.

Both successful queries allocate nothing and change no active/standby arena,
scratch index, sequence, trade, trading-state, poison, replay, or snapshot
state. They do not reserve liquidity, establish clock freshness, consolidate
venues, or add wire fields. When a healthy replica is caught up to its source,
its BBO and same-band summary equal the authoritative A128/A129 values exactly.
Shared human-readable validation detail may allocate only after corruption and
is discarded when the replica returns its static source-divergence category.

**Dependent results.** [A1, A3, A10, A12, A22, A44, A45, A72, A83, A103,
A123, A128, A129, A130] Replica BBO is `O(log(P + 1))` time and `O(1)` space
because each ordered-map extremum descends one bounded AVL path. For `K`
selected levels among `P` occupied replica prices, a band summary is
`O(log(P + 1) + K)` time with `O(1)` fixed output/state. Neither allocates
output. The shared value constructors and accumulator prevent a second
arithmetic/provenance model; no payload or snapshot wire-version change
follows.

**Falsification probe.** Compare source and replica BBO plus same-band summaries
after genesis, every accepted or rejected command class, absolute incremental
batches, exact retry, snapshot repair, and durable publisher bootstrap. Cover
both directions, empty/one-sided/two-sided state, signed prices, narrow/outside/
inverted bands, multiple levels, and sequence-only no-book changes. Require
exact value equality and nonmutation. Poison a replica; inject zero quantity/
count, locked/crossed extrema, and multi-level `u128` overflow; require the
specified typed error without partial output or poison change. Any stale-state
output while poisoned, provenance mismatch, parity divergence, unchecked wrap,
allocation, mutation, invented definition bounds, or consolidated/executable
interpretation falsifies A130.

## A131 — exact displayed-liquidity sweep quotes

**Assumption.** One `DisplayedLiquidityRequest` binds a hypothetical aggressor
side, positive lot quantity, and shared market-or-limit `StopActivation`
constraint. One immutable continuous `OrderBook` or healthy
`MarketDataReplica` borrow remains stable for the complete query. The quote
walks opposite-side public aggregate depth in market priority: offers ascend
for a buy request and bids descend for a sell request. A limit includes only
prices at or better than its exact endpoint.

Only current displayed quantity enters the calculation. A reserve order
contributes its current working slice; its undisplayed leaves and every fully
hidden order are absent. Each inspected candidate must have positive public
quantity and displayed-order count. The authoritative book returns a typed
`InvariantViolation` for a contradiction. A replica rejects poison before
traversal and maps the same fold failure to
`MarketDataError::SourceDivergence` without returning a partial value.

`DisplayedLiquidityQuote` binds the complete request to instrument identity,
immutable definition version, and final observed source event sequence.
Requested lots equal quoted plus unquoted lots. Raw-price notional is the exact
signed `i128` sum of `price.raw() × quoted lots`; the last contributing price
is the worst quoted price, and the distinct contributing price count is exact.
Termination distinguishes complete displayed fill, the next public price
outside the limit, and exhaustion of public depth.

The authoritative and replica queries use one shared checked depth fold. Its
notional arithmetic composes the same `ExecutionQuoteTotals` accumulator used
by the private A124 quote, and market/limit crossing is one shared predicate
used by matching preview. The successful query path allocates nothing, reserves
no liquidity, and changes no book, replica, sequence, poison, risk, WAL,
snapshot, or replay state. Human-readable authoritative invariant detail may
allocate only after corruption is detected and is discarded by replica error
mapping. It is neither an A124 private execution prediction nor a commitment:
hidden execution prices, reserve refresh, STP, later commands, admission,
account controls, risk, and fees can make a committed result differ.

**Dependent results.** [A1, A2, A3, A7, A10, A12, A22, A44, A55, A70, A72,
A83, A103, A123, A124, A128, A130, A131] For `K` occupied execution prices
inspected through termination among `P` authoritative prices, work is
`O(log(P + 1) + K)` time with `O(1)` fixed output/state. Hidden-only prices can
contribute to `K` but not the quote. Replica work has the same bound over
public prices. Since quoted quantity is at most `u64::MAX` lots and
`|price.raw()| <= 2^63`, the absolute accumulated notional is less than
`2^127` and fits `i128`. No output allocation or wire-version change follows.

**Falsification probe.** Exercise both aggressor directions, market and exact
limit boundaries, empty and one-/multi-level books, filled, price-limit, and
book-exhausted outcomes, partial terminal levels, signed and zero-adjacent
prices, current reserve slices, fully hidden prices, and `i64::MIN`,
`i64::MAX`, and `u64::MAX` arithmetic. Compare with a literal checked fold over
public depth and require exact quantity partition, signed notional, worst
price, contributing count, request/provenance, nonmutation, and zero allocation.
Compare caught-up source and replica results after incremental commands, exact
retry, snapshot repair, and durable bootstrap. Poison a replica and corrupt
source/replica levels to zero quantity/count; require the specified typed
failure without partial output. Any hidden or reserve-hidden disclosure,
wrong crossing direction, arithmetic wrap, provenance/parity divergence,
mutation, allocation, stale poisoned output, or implied executable commitment
falsifies A131.

## A132 — fail-closed continuous-replica observation boundary

**Assumption.** One public observation holds one immutable
`MarketDataReplica` borrow. Every economic fallible query first composes one
shared coherent-state gate. The gate rejects `poisoned = true` before depth
inspection, then reads both public AVL extrema, requires every present extremum
to have positive quantity and order count, and requires a present bid to be
strictly below a present offer. Failure is `MarketDataError::Poisoned` or the
static `MarketDataError::SourceDivergence` category before an output or
iterator is exposed.

The boundary includes BBO, separate best bid/ask, exact-price public levels,
trading state, explicit-band summary, top-N public-depth imbalance, displayed-
liquidity quote, full/range depth materialization, and fallible full/range depth
iterators. Each iterator item independently requires positive quantity and
order count at the exact row encountered. A stream may therefore yield a valid
selected row before a later corrupt non-extremum row returns
`SourceDivergence`; it never skips that row. `try_depth` validates only the
requested market-priority prefix.
`try_depth_range` first validates and counts the exact selected range/limit,
reserves that complete cardinality, and copies through a second identical pass
while the same immutable borrow prevents drift. Neither materializer returns a
partial owned vector.

The convenience level, depth, range, iterator, best-side, and trading-state
methods delegate to the fallible boundary and panic on typed failure rather
than return partially advanced state. `last_sequence`, `is_poisoned`, resource
limits, and arena/index telemetry remain readable because they diagnose and
size repair; they are not economic observations. Applying one valid non-stale
authoritative snapshot atomically replaces the depth/state boundary and clears
poison under A23/A70. No query mutates state, allocates successful iterator
output, or changes payload, snapshot, WAL, or checkpoint bytes.

**Dependent results.** [A1, A3, A10, A12, A23, A44, A55, A70, A72, A83,
A103, A123, A128, A129, A130, A131, A132] Poison rejection is `O(1)` before
tree access. The complete coherent-state gate is `O(log(P + 1))` time and
`O(1)` space for `P` occupied public prices because it descends at most two AVL
extremum paths. Full and range fallible iterators retain their existing
`O(log(P + 1))` setup, `O(P)` or `O(K)` traversal, `O(1)` iterator-state, and
zero output-allocation bounds; each item adds `O(1)` validation. A full depth
prefix of `S = min(P, L)` rows is `O(log(P + 1) + S)` time and `O(S)` owned
output. For `S = min(K, L)` rows selected by one band containing `K` prices and
limit `L`, range materialization makes two `O(log(P + 1) + S)` prefix passes
and owns `O(S)` output after exact reservation. Best-side and trading-state
queries share the gate's `O(log(P + 1))` bound. Healthy caught-up replicas
retain exact authoritative parity.

**Falsification probe.** Cause an actual incremental trade-reconciliation
failure after at least one replica mutation and require poison from every
fallible economic observation. Apply the current valid publisher snapshot and
require exact depth, iterator, exact-price level, best-side, BBO, trading-state,
summary, quote, and imbalance parity. In white-box state, inject zero
quantity/count at an extremum and at a deeper row, locked/crossed extrema, and
invalid rows reached
first from either iterator end and inside an inclusive range. Require outer-
gate failure for poison/extremum corruption, exact per-item failure for deeper
corruption, success when the invalid deeper row is outside the selected limit/
range, and a typed bulk failure without partial ownership when it is selected.
Exercise empty, one-sided, both-sided, inverted-range, incremental, exact-
retry, snapshot-repair, and durable-bootstrap states. Any economic output while
poisoned, iterator exposed through incoherent extrema, invalid selected row
omitted, partial bulk result, diagnostic read treated as economic state,
mutation, allocation on successful streaming, recovery/parity difference, or
wire-byte change falsifies A132.

## A133 — exact provenance-bound top-N public-depth imbalance

**Assumption.** One `try_public_depth_imbalance(N)` call observes one immutable
continuous `OrderBook` or healthy `MarketDataReplica` borrow. The same
caller-supplied `usize` visible-level limit `N` is applied independently in
market priority to bids and asks. `N = 0` selects neither side. A limit larger
than current public depth selects every visible level without changing the
reported limit.

Only displayed aggregate rows enter the result. A fully hidden-only price is
absent, and a reserve order contributes only its current displayed slice, not
reserve-hidden leaves. Per side, the result retains exact selected level count,
displayed-order count, displayed quantity in lots, and market-priority best and
worst selected prices. Both selections bind to one instrument identifier,
immutable definition version, and final observed book-event/source sequence.
The output contains no per-order identity, private quantity, executable-
liquidity reservation, notional, price-distance weighting, consolidation, or
clock-freshness claim.

Both side folds use the exact private `DepthAggregateTotals` accumulator shared
with A129 `DepthSummary`. Every selected candidate has positive displayed
quantity and order count; level count uses checked `usize`, while displayed-
order and displayed-quantity totals use checked `u128`. Combined displayed
quantity is the checked sum of both per-side quantities. The normalized
imbalance is represented without rounding as an optional greater-quantity
`Side`, the absolute displayed-quantity difference numerator, and combined
displayed-quantity denominator. Equal positive sides and empty depth both have
no imbalance side and numerator zero; only empty depth has denominator zero.
No normalized ratio is defined for that empty state. The value is an exact
state statistic, not a price-direction forecast.

The authoritative query first applies A128 coherent-extrema validation and
then folds public-depth candidates, including contradictory quantity/count rows
that a convenience public iterator could otherwise omit. Fully hidden rows
remain absent. The replica query first applies the A132 poison/coherent-extrema
gate, folds its public-only AVL state, and maps any accumulator or combined-
quantity failure to static `MarketDataError::SourceDivergence`. Any error
discards both local side totals and exposes no partial value. A healthy caught-
up replica equals its authoritative source exactly. Neither successful path
allocates or mutates state, and the fixed-size process-local query adds no WAL,
checkpoint, snapshot, or market-data payload field.

**Dependent results.** [A1, A2, A3, A10, A12, A22, A44, A45, A55, A70, A72,
A83, A103, A123, A128, A129, A130, A132, A133] For `K_b` and `K_a` occupied
authoritative execution prices traversed to select at most `N` visible levels
per side, work is `O(log(P + 1) + K_b + K_a)` time with `O(1)` fixed
output/state; hidden-only prices can contribute to traversal but not output.
For `B` public replica bids and `A` public replica asks, work is
`O(log(P + 1) + min(B, N) + min(A, N))` time and `O(1)` space, including the
shared coherent-state gate. Direct checkpoint restoration and caught-up
incremental, retry, repaired-snapshot, and durable-bootstrap replicas reproduce
the exact value. No wire-version change follows.

**Falsification probe.** Exercise `N` equal to `0`, `1`, exact side depth, and
`usize::MAX`; empty, bid-only, ask-only, equal-positive, buy-dominant, and sell-
dominant states; multiple levels/orders; signed prices; fully hidden-only
prices; current reserve slices with reserve-hidden leaves; and unequal side
cardinalities. Require exact provenance, market-priority endpoints, side
counts/totals, checked combined denominator, side plus absolute numerator,
unchanged state, and source/replica/checkpoint equality. Corrupt the best and a
selected non-best row to zero quantity or count; place the deeper corrupt row
just inside and outside `N`; construct per-side and bid-plus-ask `u128`
overflow; poison a replica and then repair it by valid snapshot. Require gate
failure for corrupt extrema irrespective of `N`; require typed failure without
partial output when deeper corruption or overflow is selected, and success
when it lies outside the selected prefix. Any hidden or reserve-hidden
disclosure, asymmetric limit interpretation, rounded or empty-state ratio,
unchecked wrap, stale poisoned output, provenance/parity/recovery divergence,
mutation, successful-path allocation, predictive interpretation, or wire-byte
change falsifies A133.

## A134 — exact-price provenance-bound public-level observation

**Assumption.** One `try_public_level(side, price)` call observes one immutable
continuous `OrderBook` or healthy `MarketDataReplica` borrow. The supplied
`(Side, Price)` is an exact lookup key. It need not be on the instrument tick
grid or inside its admission collar: an off-grid, outside-collar, unoccupied,
or opposite-side key ordinarily returns absent displayed state rather than an
admission error.

`PublicLevelObservation` binds the exact queried side and price plus an optional
`LevelSnapshot` to instrument identity, immutable definition version, and the
final observed book-event/source sequence. A present snapshot repeats the exact
queried price and carries positive displayed quantity and displayed-order
count. A fully hidden-only execution price is absent. A reserve order
contributes only its current displayed slice; reserve-hidden leaves and private
order identities are absent. A state-bound absent result makes no claim that
another side, price, source, version, or later sequence is absent.

The authoritative query first applies A128 coherent-extrema validation. At the
requested key it then requires execution-level visibility and redundant public-
price AVL membership to agree. A candidate with one zero aggregate dimension,
a key/snapshot mismatch, or a membership contradiction returns typed
`InvariantViolation`. Both-zero execution aggregates denote no displayed
liquidity at this local observation boundary; A45 remains the complete private
topology and redundant-index audit. The legacy `level` convenience method now
composes this fallible path and panics on detected corruption rather than
returning an ambiguous absence.

The replica query first applies the A132 poison/coherent-extrema gate, performs
one exact lookup in public-only depth, and reuses the same key/aggregate/value
constructor. Target corruption maps to static
`MarketDataError::SourceDivergence`; poison or incoherent extrema fail before
target lookup. Its new `level` convenience method also composes the typed path.
A deeper corrupt non-extremum row outside the exact key is not inspected.
Publisher affected-level reconciliation uses the typed authoritative query and
clears its fixed scratch before returning source divergence, so corruption
cannot convert that internal comparison into a panic or leave scratch residue.

Neither successful query allocates or mutates order, level, replica, sequence,
history, risk, WAL, checkpoint, snapshot, replay, or publisher state. The value
is a process-local state observation, not an authenticated entitlement,
consolidated quote, liquidity reservation, remote freshness proof, or transport
message.

**Dependent results.** [A1, A3, A10, A12, A22, A44, A45, A55, A70, A72, A83,
A103, A117, A123, A128, A130, A132, A134] The authoritative query performs an
`O(1)` coherent-extrema check plus two exact AVL lookups and is therefore
`O(log(P + 1))` time with `O(1)` fixed output/state for `P` occupied execution
prices. The replica gate plus exact public-AVL lookup has the same asymptotic
bound. Present and absent observations allocate no output. Direct checkpoint
restoration and caught-up incremental, exact-retry, repaired-snapshot, and
durable-bootstrap replicas reproduce the same value. No wire-version change
follows.

**Falsification probe.** Exercise both sides; empty, one-sided, and two-sided
books; best and non-best displayed levels; unoccupied, opposite-side,
hidden-only, reserve, signed, zero, off-grid, and outside-collar prices; multiple
orders at one key; and exact source-sequence advances with and without a target
change. Require exact key/provenance, current displayed aggregate, state-bound
absence, convenience/fallible parity, unchanged state, and checkpoint/source/
replica equality. Corrupt target quantity, count, embedded price, and redundant
public membership; corrupt an extremum and a deeper unselected row; lock/cross
the extrema; poison a replica and repair it with a valid snapshot. Require gate
failure for poison/extremum corruption, target failure only when the exact key
is selected, success through unrelated deeper corruption, typed publisher
divergence with empty scratch, and no partial value. Any hidden or reserve-
hidden disclosure, off-grid admission inference, key/provenance drift, stale
poisoned output, unchecked contradiction, panic in the typed path, mutation,
successful-path allocation, recovery/parity difference, or wire-byte change
falsifies A134.

## A135 — fail-closed authoritative public-depth boundary

**Assumption.** One public query holds one immutable continuous `OrderBook`
borrow. `try_best_bid`, `try_best_ask`, `try_depth_iter`,
`try_depth_range_iter`, `try_depth`, `try_depth_range`,
`try_depth_range_summary`, and `try_depth_summary` compose the A128 coherent-
extrema gate before returning a side, iterator, vector, or summary. The A131
displayed-liquidity quote now applies the same gate before its bounded sweep;
A133 imbalance and A134 exact-level observations retain their existing gate.

The outer iterator result rejects a zero aggregate/count at either cached
public extremum and rejects locked/crossed extrema before a traversal is
exposed, including for a zero limit, inverted range, empty selected band, or
opposite-side request. Each streamed execution-price candidate with either a
non-zero displayed quantity or displayed-order count must have both values
positive. A both-zero row is hidden-only at this local public boundary and is
omitted. Bids remain descending, asks ascending, and full/range iterators
remain double-ended. A valid prefix may therefore be consumed before a later
non-extremum contradiction returns typed `InvariantViolation`; a contradiction
outside the consumed prefix or inclusive range is not inspected.

`try_depth` and `try_depth_range` first validate and count the exact selected
visible prefix without allocation, reserve that complete cardinality, and copy
through a second identical traversal under the same immutable borrow. An
invariant or reservation failure returns no caller-owned partial vector.
`try_depth_range_summary` folds the same typed range traversal, and any row or
cumulative-arithmetic failure discards its fixed-size local partial value.

The `best_bid`, `best_ask`, `depth`, `depth_range`, `depth_iter`, and
`depth_range_iter` convenience methods delegate to the fallible boundary and
panic on typed failure. Diagnostic resource telemetry, private observations,
and the crate-private publisher projection remain separate interfaces. Per-row
stream validation does not prove complete A45 private FIFO or redundant
public-index topology; A134 proves membership at one exact key and
`OrderBook::validate` remains the complete structural audit.

No successful iterator, best-side, summary, quote, or imbalance query allocates
or mutates state. Successful materialization owns only its exactly reserved
output vector. No query adds a command, event, risk, WAL, checkpoint, snapshot,
market-data payload, or wire-version field.

**Dependent results.** [A1, A3, A10, A12, A22, A44, A45, A55, A70, A72, A83,
A103, A123, A128, A129, A130, A131, A132, A133, A134, A135] Best-side and
outer-gate work is `O(1)`. Full iterator setup is `O(log(P + 1))`; consuming
`K` occupied execution prices is `O(K)` with `O(1)` iterator state. Range setup
and traversal are `O(log(P + 1) + K)`. Each candidate adds `O(1)` validation.
Materializers make two bounded traversals and own `O(S)` output for `S`
selected visible rows; no error returns partial output. Direct checkpoint
restoration and caught-up incremental, retry, repaired-snapshot, and durable-
bootstrap replicas preserve healthy source/replica parity. No wire change
follows.

**Falsification probe.** Exercise both sides; empty, one-sided, two-sided,
hidden-only, reserve, signed-price, full, narrow, outside, singleton, and
inverted bands; limits `0`, `1`, exact visible depth, and `usize::MAX`; and
forward, reverse, and mixed-end traversal. Corrupt each extremum and a deeper
row independently to zero quantity or count; lock/cross the sides; place the
deeper row immediately inside and outside the selected prefix/range. Require
outer failure before exposure for extremum corruption irrespective of the
selection, exact per-item failure for a reached deeper row, success when that
row is not reached, exact reservation, no partial vector, unchanged telemetry
and state, and convenience/fallible parity. Compare checkpoint source and
healthy incremental, exact-retry, snapshot-repaired, and durable-bootstrap
replicas after every command class. Any invalid selected row omitted, economic
state exposed through incoherent extrema, direction/range drift, hidden
disclosure, partial ownership, successful-path streaming allocation, mutation,
recovery/parity difference, panic in a typed path, or wire-byte change
falsifies A135.

## A136 — provenance-bound revisioned trading-state observation

**Assumption.** One trading-state query holds one immutable continuous
`OrderBook` or healthy `MarketDataReplica` borrow. The fixed-size
`TradingStateObservation` binds `TradingStateSnapshot` to instrument,
immutable definition version, and the final matching/source event sequence.
Revision zero is valid genesis state; every other accepted revision must be
less than or equal to that sequence. One shared constructor enforces this
bound before a value is exposed.

The authoritative query first applies the A135 coherent-extrema gate in
`O(1)` time. The replica query first applies the A132 poison/coherent-extrema
gate in `O(log(P + 1))` time for `P` occupied public prices. Revision-ahead
corruption returns typed `InvariantViolation` at the source and static
`MarketDataError::SourceDivergence` at the replica. Existing snapshot-returning
fallible methods compose the observation path, and convenience methods panic
on typed corruption. Publisher bootstrap and source parity checks reuse one
fallible adapter, mapping source corruption to typed source divergence rather
than entering a convenience panic boundary.

Successful queries allocate and mutate nothing and add no command, event,
risk, WAL, checkpoint, snapshot, replay, market-data payload, or wire-version
field. The local revision bound and coherent public extrema do not prove that
the state/revision pair is derivable from complete retained command history;
checkpoint capture's live-lineage comparison and checkpoint reconstruction
remain the complete local history-definition checks. `OrderBook::validate`
remains the complete live structural-index audit.

**Dependent results.** [A1, A3, A10, A12, A23, A44, A55, A59, A70, A72, A83,
A103, A128, A130, A132, A135, A136] Source observation is `O(1)` time and
`O(1)` fixed space. Replica observation is `O(log(P + 1))` time and `O(1)`
fixed space. Genesis, direct checkpoint restoration, WAL recovery, publisher
bootstrap, caught-up incremental application, exact retry, and repaired-
snapshot replicas reproduce the same provenance and revision. No wire change
follows.

**Falsification probe.** Observe genesis and every accepted state transition;
exercise transition-only and transition-and-cancel, rejection, exact retry,
checkpoint restoration, WAL recovery, durable publisher bootstrap, incremental
replication, and snapshot repair. Require exact instrument/version/sequence,
state/revision, source/replica equality, no revision advance on rejection or
retry, and unchanged state after every query. Inject a revision greater than
the event/source sequence; corrupt, lock, and cross public extrema; poison and
repair a replica. Require typed failure before any value for revision,
extrema, or poison corruption and exact parity after repair. Any unprovenanced
state, revision-ahead success, stale poisoned value, mutation, successful-path
allocation, publisher panic, recovery/parity difference, or wire-byte change
falsifies A136.

## A137 — provenance-bound account admission-fence observation

**Assumption.** One account-fence query holds one immutable continuous
`OrderBook` borrow. The fixed-size `AccountControlObservation` binds one
`AccountControlSnapshot` to account, instrument, immutable definition version,
and final matching event sequence. An account absent from retained control
state is enabled at revision zero. A retained control must use a non-zero
revision no greater than the event sequence, and a blocked account must have no
entry in the active-account order index.

The shared A136 revision validator enforces the sequence bound. The single-
account query performs one expected bounded-hash control lookup and, for a
retained blocked state, one expected bounded-hash active-account lookup. The
snapshot-returning fallible method composes the observation path; the
convenience method panics on typed corruption. Matching preparation,
application, and structural validation retain a private raw accessor so a
diagnostic corruption check cannot recurse through that panic boundary.

Market-data publisher bootstrap and complete source cross-audit validate every
retained control through the same account-local helper before copying or
comparing the private mirror. Corrupt source state maps to static typed source
divergence. Account identity remains absent from public market-data payloads;
no command, event, checkpoint, snapshot, replay, codec, or wire field changes.
The local observation does not prove complete retained-history derivation;
checkpoint capture's live-lineage comparison and checkpoint reconstruction
remain that proof.

**Dependent results.** [A1, A3, A10, A12, A18, A57, A70, A72, A136, A137]
One account observation is expected `O(1)` time and `O(1)` fixed space. A full
publisher control-source validation is expected `O(T)` for `T` retained
controls. Successful paths allocate and mutate nothing. Genesis, accepted
block/enable, business rejection, exact retry, direct checkpoint restoration,
plain-WAL recovery, coupled-risk recovery, and publisher bootstrap retain exact
account/instrument/version/sequence/state/revision semantics. No wire change
follows.

**Falsification probe.** Query controlled and absent accounts at genesis and
after accepted block-and-cancel and enable commands. Exercise empty and
non-empty cancellation scopes, business rejection, exact retry, direct
checkpoint restoration, plain and coupled-risk WAL recovery, publisher
bootstrap, and complete source cross-audit. Require exact provenance, revision
stability with sequence advance on rejection, complete observation stability
on retry, no blocked-account index membership, and no query mutation. Inject a
retained zero revision, a revision ahead of the event sequence, and a blocked
state with active account membership. Require typed failure before any value
and typed publisher construction failure. Any identity drift, impossible
genesis retention, revision-ahead success, blocked-membership success,
mutation, successful-path allocation, recovery difference, public account
disclosure, publisher panic, or wire-byte change falsifies A137.

## A138 — provenance-bound exact active-order observation

**Assumption.** One exact active-order query holds one immutable continuous
`OrderBook` borrow. The fixed-size `ActiveOrderObservation` binds the queried
`OrderId` to instrument, immutable definition version, final matching event
sequence, and an optional `ActiveOrderSnapshot`. The enum distinguishes a
complete `OrderSnapshot` from a complete `DormantStopSnapshot`; absence is
bound to the same source state rather than returned as an unversioned `None`.

The query performs one expected bounded-hash identity lookup. A present hash
key must equal the embedded order identifier. Resting state must have positive
total and working quantities, working quantity no greater than total leaves,
and a working/display relationship valid for fully displayed, reserve, or
fully hidden state. Dormant state must have positive leaves, canonical working
quantity for its display policy, the activation risk price, no price-level FIFO
links, non-zero trigger priority, and expiration identical to its retained
lifetime. `try_order` and `try_dormant_stop` project the observation through
the same validation; the convenience methods delegate to those typed paths and
panic on selected-row corruption.

Complete market-data publisher bootstrap/source parity and coupled-risk audit
consume fallible dormant snapshots, so local dormant corruption becomes typed
source divergence or `RiskInvariantViolation` rather than a snapshot panic.
The observation proves only the selected row. It does not prove complete
price-level FIFO, account membership, GTD-expiry membership, stop-trigger
membership, or command-history derivation; `try_order_queue_position`,
`try_price_level_orders`, `OrderBook::validate`, and checkpoint-lineage
validation retain those stronger scopes. No command, event, checkpoint,
snapshot, replay, codec, or wire field changes.

**Dependent results.** [A1, A2, A3, A4, A9, A10, A12, A17, A22, A57,
A72, A103, A125, A138] One observation or typed projection is expected `O(1)`
time and `O(1)` fixed space; an adversarial complete hash-collision cluster is
`O(O)` for `O` active identities. Successful paths allocate and mutate nothing.
Direct checkpoint restoration and full-WAL recovery retain exact provenance
and absent/resting/dormant state. No wire change follows.

**Falsification probe.** Query absent identities, fully displayed, reserve,
fully hidden, dormant stop-market, dormant stop-limit, and dormant GTD orders.
Exercise partial fill, reserve refresh, replacement, cancellation, activation,
complete fill, business rejection, exact retry, direct checkpoint restoration,
publisher bootstrap, coupled-risk audit, and full-WAL recovery. Require exact
identifier/instrument/version/sequence/state, no mutation, and typed-projection
parity. Inject selected key/identity drift; resting zero leaves, zero working,
working-above-leaves, and display inconsistency; dormant zero leaves,
noncanonical working quantity, activation-risk-price drift, price-level links,
zero priority, and lifetime/expiry drift. Require typed failure before any
value and typed publisher/risk failure. Any unprovenanced absence, state-class
ambiguity, local-corruption success, mutation, successful-path allocation,
recovery difference, publisher/risk panic, or wire-byte change falsifies A138.

## A139 — frozen-best continuous market-to-limit

**Assumption.** `OrderType::MarketToLimit` is a direct continuous new-order
instruction with no submitted price. It is admitted only with GTC or GTD. After
instrument, quantity, display, identity, account-fence, expiry, and lifetime
validation, but before matching or STP, the engine captures the current best
executable opposite price from the private price index. The captured price
includes a hidden-only best level and is independent of the public BBO.

Acceptance emits ordinary `OrderAccepted` followed immediately by exactly one
`MarketToLimitPriced { order_id, limit_price }`. The matching kernel then uses
one ordinary limit constraint at that exact price. It cannot execute a worse
level; any residual rests at the captured price under the submitted fully
displayed, reserve, or fully hidden policy. Cancel-resting STP can remove the
captured maker but cannot reprice the aggressor to newly exposed liquidity.
The pricing event and converted residual do not change direct stop activation,
which remains market-or-limit only.

IOC, minimum-quantity IOC, FOK, and post-only are rejected with
`MarketToLimitRequiresRestingLifetime` before opposite-book availability is
evaluated. A valid lifetime with no opposite executable price is rejected with
`MarketToLimitBookEmpty`. Both are sequenced business outcomes and do not
consume the submitted order identity. Accepted capacity preflight uses the
captured price as its effective limit and must reject an unavailable residual
price level before acceptance or any maker mutation.

Risk authorization values the unpriced command over the complete signed market
collar. If execution leaves a residual, trace application creates a reservation
from the actual captured limit and remaining total leaves. The publisher
derives the same executable best by scanning its complete private order mirror,
including hidden orders, before any trace mutation; it requires the pricing
event before every market-to-limit trade, STP interaction, or residual and maps
that event to `NoBookChange`. The high-level best-opposite/residual concept is
also defined by
[ASX 24 Operating Rules, Procedure 4020](https://www.asx.com.au/content/dam/asx/rules-guidance-notes-waivers/asx-24-operating-rules/rules/ASX-24-Operating-Rules-Section-04.pdf).
Quotick's hidden-liquidity, lifetime, STP, risk, and wire rules are internal and
do not assert venue protocol conformance.

**Dependent results.** [A1, A2, A3, A4, A5, A9, A10, A12, A15, A17, A18,
A19, A22, A37, A39, A42, A44, A57, A67, A72, A73, A80, A81, A82, A85,
A91, A103, A139] Capture and successful-path conversion are `O(1)` before
ordinary effective-limit matching. The publisher pricing check is `O(O)` time
over `O` tracked active orders and `O(1)` auxiliary space. WAL-v20,
snapshot-v20, direct checkpoint restore, coupled-risk recovery, publisher
replay, and exact retry retain the submitted type, captured price, report, and
residual exactly.

**Falsification probe.** Submit buy and sell market-to-limit orders against
empty books; visible, reserve, and hidden-only best levels; multiple worse
levels; all six TIF variants; all three display policies; every STP policy;
positive, zero, and negative prices; partial and full fills; saturated active,
account, and price-level capacity; and signed-collar risk limits. Require one
pricing event immediately after acceptance, no worse-price execution, exact
residual price/display, no accepted-ID consumption on rejection, market-collar
authorization, limit-priced residual reservation, and exact retry without WAL
growth. Corrupt, omit, duplicate, reorder, or reprice the pricing event and
require publisher poison or recovery divergence before public output. Restore
through direct checkpoints, raw and segmented WAL, coupled risk, and snapshot
cutover, then require identical observation and report bytes. Any public-BBO
capture, post-STP repricing, worse-level trade, late capacity failure, identity
consumption on rejection, noncanonical risk reservation, accepted unsupported
TIF, stop activation as market-to-limit, recovery drift, or version-19
acceptance falsifies A139.

## A140 — atomic conditional immediate execution

**Assumption.** `ImmediateExecutionSubmission` maps its command and order
identities, instrument coordinates, timestamp, account, side, positive
quantity, market-or-limit constraint, and self-trade policy to exactly one
ordinary fully displayed `ImmediateOrCancel` `NewOrder`. One exclusive mutable
shard borrow spans core command preparation, the coupled-risk authorization
precheck when present, the A124 private quote, a caller-supplied acceptance
predicate, and commit of the same prepared command.

The predicate runs only when core preparation and coupled-risk authorization
permit execution. It observes the exact A124 quote from the same book
generation that will be committed. Predicate decline or unwind drops the
preparation before consuming an order identity or changing sequence, events,
trades, matching state, risk state, or WAL bytes. Acceptance commits without
an intervening shard mutation. Core or risk business rejection and exact retry
bypass the predicate and return a reported outcome without a current quote;
risk rejection remains an ordinary sequenced report and retry remains the
existing nonmutating replay.

Durable variants invoke the predicate before command append. Acceptance and
core or risk rejection retain the existing command-before-state-before-report
protocol, poisoning and recovery rules; decline, unwind, and replay append
nothing. The callback decision is process-local and is neither persisted nor
authenticated. A140 does not reserve a standalone A124 quote, validate a quote
across borrows, represent fees, report projected cancel-resting or cancel-both
side effects inside the quote, or generalize the canonical IOC display,
lifetime, and order-type subset. It reuses the existing command and report wire
encodings and requires no wire-version change.

**Dependent results.** [A1, A2, A3, A4, A5, A7, A9, A10, A12, A15, A18,
A19, A22, A37, A39, A44, A45, A47, A55, A57, A67, A72, A80, A81, A82,
A83, A85, A103, A115, A124, A140] With quote cost `Q`, ordinary IOC commit
cost `M`, and caller predicate cost `F`, acceptance costs `O(Q + M + F)` and
decline costs `O(Q + F)`, with A124's exact ordinary-policy and decrement-and-
cancel bounds for `Q`. Coupled risk adds expected `O(1)` authorization before
the predicate and repeats that unchanged expected `O(1)` check at commit.
Fixed auxiliary state is `O(1)` and the API introduces no allocation. Durable
acceptance and core or risk rejection append the existing two frames; decline,
unwind, and exact retry append zero.

**Falsification probe.** For both sides, market and limit constraints, signed
prices, displayed, reserve, and hidden makers, every A124 termination, and all
four self-trade policies, run predicates that accept, decline, and unwind.
Exercise core rejection, every applicable coupled-risk rejection, exact retry,
and command-ID collision; count predicate calls and compare every accepted
quote with its committed trace. On decline or unwind, require identical order-
identity availability, event and trade sequences, private/public book, risk
reservations, exposure, positions, and WAL length. On acceptance and business
rejection, require canonical command/report frame order, poison-on-failure, and
exact plain and coupled-risk recovery; on retry require no WAL growth. Any
noncanonical IOC mapping, predicate call on rejection or replay, quote/trace
divergence, mutation before acceptance, interposed shard state, partial durable
decision, recovery drift, allocation, or new wire value falsifies A140.

## A141 — exact private immediate-execution curve

**Assumption.** One `try_immediate_execution_curve` call holds one immutable
`OrderBook` borrow for its complete operation and accepts the same
`ImmediateExecutionRequest` as A124. Its first shared scanner pass produces the
complete A124 quote, including the exact count `C` of distinct prices with
positive external execution. The query fallibly requests capacity for exactly
`C` `ImmediateExecutionLevel` rows before an identical second shared scanner
pass populates caller-owned output. The allocator may grant more capacity.

Rows follow aggressor market priority and contain one distinct execution price
and its positive aggregate external quantity. Each row's exact signed raw-
price notional is `price.raw() × executed lots`. Row quantities and notionals
sum to the embedded quote, row count equals its contributing-price count, and
the final row price equals its worst execution price. Decrement-and-cancel self
consumption and unfilled quantity remain in the embedded quote and never form
rows. Cancel-resting and cancel-both projected maker cancellations remain
outside both outputs.

Reservation failure identifies `ImmediateExecutionLevels` and returns no
partial curve. The operation does not mutate matching, risk, reservation,
identity, sequence, history, WAL, snapshot, or public state; it does not
reserve liquidity or commit a command. The curve and its callback-free local
query are not encoded and require no wire-version change.

**Dependent results.** [A1, A2, A3, A4, A7, A12, A19, A22, A44, A45, A47,
A55, A72, A83, A103, A115, A117, A124, A141] For applicable A124 scan cost
`Q`, requested quantity `q`, and `C <= min(P_c, q)` contributing prices, the
two scans plus row construction cost `O(2Q + C) = O(Q)` time, `O(1)` scanner
state, and `O(C)` caller-owned output. Capacity for exactly `C` rows is
requested before copying; the allocator may grant more, and successful
population does not grow the vector. Each row stores one `Price` and one
`Quantity`; exact row notional is derived in `O(1)`.

**Falsification probe.** For both sides, market and limit constraints, signed
prices, every termination, and all four self-trade policies, exercise
displayed, reserve, and fully hidden self/external orders, partial reserve
slices, repeated refresh, empty execution, price limits, and book exhaustion.
Compare quote and every curve row with committed IOC trades from identical
state and with at least 20,000 generated books under an independent literal
two-class slice/requeue model. Require strict market order, distinct positive
rows, exact count/quantity/notional/worst-price reconciliation, nonmutation,
an exact capacity request, typed resource attribution, no partial failure
output, and no wire change. Any self quantity represented as execution,
duplicate/zero or misordered row, quote/curve/model divergence, hidden/reserve
priority drift, post-request growth, mutation, or implied commitment falsifies
A141.

## A142 — atomic curve-aware conditional immediate execution

**Assumption.** One `try_submit_immediate_execution_curve_if` call on an
`OrderBook`, `RiskManagedOrderBook`, `DurableOrderBook`, or
`DurableRiskOrderBook` holds the corresponding exclusive mutable shard borrow
through A140 preparation, A141 curve construction, a caller predicate, and
commit of the same canonical fully displayed market-or-limit IOC preparation.
It reuses the exact A124 quote computed during A140 preflight, fallibly requests
capacity for that quote's `C` contributing prices, and performs one identical
private scanner pass to populate the curve. The repeated scan must reproduce
the supplied quote exactly; contradiction is a typed order-book query invariant
failure in release and returns no curve.

Exact replay plus core or coupled-risk rejection bypass curve allocation and
the predicate and return a reported outcome without a current curve. On an
otherwise admissible command, the predicate borrows the complete curve while
the preparation and curve remain owned by the call. Curve reservation failure,
predicate decline, or unwind drops both before consuming order identity or
changing sequence, event, trade, matching, risk, history, public, or WAL state.
Acceptance commits without an intervening shard mutation and returns that exact
curve with the report.

Durable variants construct the curve and invoke the predicate before appending
the command. Acceptance and core or risk rejection retain the existing
command-before-state-before-report protocol; allocation failure, decline,
unwind, and replay append zero frames. Query allocation failure is typed by the
plain combined matching/query error or the durable wrapper's order-book-query
variant and does not poison the wrapper. A142 persists neither the curve nor
the callback decision and reuses the existing command/report encodings without
a wire-version change.

**Dependent results.** [A1, A2, A3, A4, A5, A7, A9, A10, A12, A15, A18,
A19, A22, A37, A39, A44, A45, A47, A55, A57, A67, A72, A80, A81, A82,
A83, A85, A103, A115, A117, A124, A140, A141, A142] For A124 scan cost
`Q`, `C` contributing prices, ordinary IOC commit cost `M`, and predicate cost
`F`, acceptance costs `O(2Q + C + F + M) = O(Q + F + M)` and decline costs
`O(2Q + C + F) = O(Q + F)`. The first `Q` is A140 preflight; the second is
A141 population from that prepared quote. Core/risk rejection and replay skip
the second scan, allocation, and `F`. Coupled risk adds the existing expected
`O(1)` authorization precheck and commit recheck. Scanner auxiliary state is
`O(1)` and the returned/retained curve owns `O(C)` caller output. Durable
acceptance and business rejection append two existing frames; allocation
failure, decline, unwind, and replay append zero.

**Falsification probe.** Across all four surfaces, both sides, market and limit
constraints, signed prices, all four self-trade policies, every termination,
and displayed, reserve, and fully hidden self/external liquidity, run predicates
that accept, decline, and unwind. Compare every predicate curve row and quote
with the committed trade trace and the A141 literal two-class model. Inject
unrepresentable curve output, a supplied-quote/second-scan contradiction, core
and every applicable risk rejection, exact retry, command-ID collision, and
durable write/report failures. Count predicate calls, allocation attempts,
identity/sequence availability, coupled positions/reservations, book state,
poison, and WAL frames; reopen plain and coupled-risk logs. Any allocation or
predicate call on rejection/replay, partial curve, quote/curve/trace mismatch,
mutation or WAL growth before acceptance, missing typed query provenance,
interposed shard mutation, durable protocol drift, recovery divergence, or new
wire value falsifies A142.

## A143 — atomic conditional new-order execution observation

**Assumption.** One `submit_new_order_if` or
`try_submit_new_order_curve_if` call on an `OrderBook`,
`RiskManagedOrderBook`, `DurableOrderBook`, or `DurableRiskOrderBook` holds the
corresponding exclusive mutable shard borrow across ordinary `NewOrder`
preparation, coupled-risk authorization precheck when present, observation,
predicate, and commit of that same preparation. It accepts the complete
submitted market, market-to-limit, limit, dormant-stop, display, TIF, and STP
fields; it neither canonicalizes them to A140 IOC nor introduces another
matching path.

`OrderExecution<T>` distinguishes `Active(T)` from `DormantStop`. An active
order maps account, side, quantity, STP, and its effective market-or-limit
constraint to the exact A124 quote. Curve submission maps that quote through
A141 before invoking the predicate. Market-to-limit uses the private opposite
best that the unchanged commit captures in `MarketToLimitPriced`; a valid
noncrossing post-only order carries an empty active observation. A valid stop
instead carries `DormantStop`, allocates no curve, and makes no claim about
activation-time liquidity. An already-triggered or reference-less stop is a
core rejection and bypasses the predicate.

The A124/A141 value describes current private execution economics; the
submitted TIF remains authoritative at commit. A valid FOK reaches the
predicate only after its complete-fill core preflight. A crossing post-only
order and an insufficient FOK bypass the predicate as core rejections. A
minimum-quantity IOC can expose positive external liquidity below its
threshold, then atomically emit `MinimumQuantityUnavailable` without trading
if the predicate accepts it. GTC/GTD residuals retain their submitted display
and effective limit, including the frozen market-to-limit price.

Exact replay plus core or coupled-risk rejection returns a reported outcome
without observation, curve allocation, or predicate execution. Curve
reservation failure, predicate decline, or unwind drops the preparation before
order identity, sequence, event, trade, matching, risk, history, public, or WAL
mutation. Acceptance commits without an intervening shard transition and
returns the exact observation with the report. Durable acceptance and business
rejection retain command-before-state-before-report; allocation failure,
decline, unwind, and replay append zero frames. The observation and callback
decision are process-local, unencoded, unauthenticated, and valid only within
the call. Existing command, report, checkpoint, market-data, and wire values
are unchanged.

**Dependent results.** [A1, A2, A3, A4, A5, A7, A9, A10, A12, A15, A18,
A19, A22, A37, A39, A44, A45, A47, A55, A57, A67, A72, A80, A81, A82,
A83, A85, A103, A105, A115, A124, A139, A140, A141, A142, A143] Let `A`
be ordinary new-order preparation cost, `Q` the applicable A124 scan cost, `C`
the contributing-price count, `F` predicate cost, and `M` commit cost. Active
quote acceptance costs `O(A + Q + F + M)` and decline costs `O(A + Q + F)`.
Active curve acceptance costs
`O(A + 2Q + C + F + M) = O(A + Q + F + M)` and decline costs
`O(A + 2Q + C + F) = O(A + Q + F)`, with `O(C)` caller-owned output and
`O(1)` scanner state. Dormant-stop acceptance costs `O(A + F + M)`, decline
costs `O(A + F)`, and uses `O(1)` observation space with no curve allocation.
Core/risk rejection and replay pay only their existing preparation/gate path
and skip `Q`, `C`, and `F`. Coupled risk retains its expected `O(1)` precheck
and commit recheck. Durable acceptance and business rejection append two
existing frames; allocation failure, decline, unwind, and replay append zero.

**Falsification probe.** Across all four surfaces, exercise market,
market-to-limit, limit, GTC, GTD, IOC, minimum-quantity IOC, FOK, post-only,
fully displayed, reserve, fully hidden, and dormant stop-market/stop-limit
orders on both sides, signed prices, all four STP policies, and every A124
termination. Run accepting, declining, and unwinding predicates. Require the
same private best in market-to-limit observation, pricing event, trades, and
residual; explicit dormant state without a scan; empty valid post-only state;
and submitted TIF/display behavior after acceptance. Exercise sub-threshold
minimum IOC, insufficient FOK, crossing post-only, unavailable/already-
triggered stop, every applicable risk rejection, exact retry, command-ID
collision, curve reservation failure, and durable write/report failure.

Count predicate and allocation calls; compare identity/sequence availability,
private/public book, risk positions/reservations/exposures, WAL frames, and
plain/coupled-risk reopen state. Any observation or predicate on
rejection/replay, active/dormant misclassification, market-to-limit price
drift, activation forecast, TIF/display replacement, partial curve, mutation or
WAL growth before acceptance, interposed shard transition, durable protocol or
recovery drift, or new wire value falsifies A143.

## A144 — atomic conditional replacement execution observation

**Assumption.** One `submit_replace_order_if` or
`try_submit_replace_order_curve_if` call on an `OrderBook`,
`RiskManagedOrderBook`, `DurableOrderBook`, or `DurableRiskOrderBook` holds the
corresponding exclusive mutable shard borrow across ordinary `ReplaceOrder`
preparation, coupled-risk authorization precheck when present, observation,
predicate, and commit of that same preparation. It reuses the ordinary
replacement path and introduces no second ownership, admission, matching,
risk, report, persistence, or recovery implementation.

Exact replay plus core or coupled-risk rejection returns a reported outcome
without observation, curve allocation, or predicate execution. Core business
preflight includes route/version, target existence and ownership, account/
trading state, quantity/price/display constraints, and the dormant stop-market
prohibition. Ordinary preparation also fixes capacity and sequence/event
resources as typed operational errors before observation. Coupled risk applies
the existing authorization that nets out the target reservation; rejection
preserves that reservation and clears the observation before the predicate.

For an active target, `OrderExecution<T>::Active` maps its immutable account,
side, and STP policy plus the replacement quantity and limit price to the exact
A124 quote or A141 curve. The target is same-side state, so leaving it in place
during observation cannot alter scanned opposite liquidity. A same-price,
unchanged-display quantity reduction therefore produces an empty active
observation and retains priority if accepted. A price or priority-changing
replacement observes the external executions that its unchanged commit can
produce. A dormant stop-limit carries `DormantStop`, allocates no curve, and
makes no claim about activation-time liquidity; dormant stop-market replacement
remains a core rejection.

Curve reservation failure, predicate decline, or unwind drops the preparation
before sequence, event, trade, matching, risk, history, public, or WAL mutation
and leaves the original active order and reservation byte-for-byte unchanged.
Acceptance commits without an intervening shard transition and returns the
exact observation with the report. Durable acceptance and business rejection
retain command-before-state-before-report; allocation failure, decline,
unwind, and replay append zero frames. The observation and callback decision
are process-local, unencoded, unauthenticated, and valid only within the call.
Existing command, report, checkpoint, market-data, and wire values are
unchanged.

**Dependent results.** [A1, A2, A3, A4, A5, A7, A9, A10, A12, A15, A18,
A19, A22, A37, A39, A44, A45, A47, A55, A57, A67, A72, A80, A81, A82,
A83, A85, A103, A124, A139, A141, A142, A143, A144] Let `R` be ordinary
replacement preparation cost, `Q` the applicable A124 scan cost, `C` the
contributing-price count, `F` predicate cost, and `M` commit cost. Active quote
acceptance costs `O(R + Q + F + M)` and decline costs `O(R + Q + F)`. Active
curve acceptance costs
`O(R + 2Q + C + F + M) = O(R + Q + F + M)` and decline costs
`O(R + 2Q + C + F) = O(R + Q + F)`, with `O(C)` caller-owned output and
`O(1)` scanner state. Dormant-stop acceptance costs `O(R + F + M)`, decline
costs `O(R + F)`, and uses `O(1)` observation space with no curve allocation.
Core/risk rejection and replay pay only their existing preparation/gate path
and skip `Q`, `C`, and `F`. Coupled risk retains its expected `O(1)` net-
replacement precheck and commit recheck. Durable acceptance and business
rejection append two existing frames; allocation failure, decline, unwind, and
replay append zero. All arithmetic is exact integer arithmetic; approximation
error is zero.

**Falsification probe.** Across all four surfaces, exercise active same-price
priority-retaining reduction, quantity increase, price changes, signed prices,
fully displayed/reserve/hidden modes, both sides, every STP policy, partial and
full execution, every A124 termination, and dormant stop-limit replacement.
Run accepting, declining, and unwinding predicates. Require exact quote/curve/
committed-trace parity, unchanged target priority and state after noncommit,
explicit dormant state without a scan, and ordinary retained/lost-priority
events after acceptance. Exercise unknown/wrong-owner targets, wrong route or
version, invalid quantity/price/display, blocked/non-open admission, dormant
stop-market replacement, every applicable risk rejection, exact retry,
command-ID collision, curve reservation failure, sequence/event/capacity
exhaustion, and durable write/report failure.

Count predicate and allocation calls; compare target snapshots and queue
priority, private/public book, risk positions/reservations/exposures, WAL
frames, and plain/coupled-risk reopen state. Any observation or predicate on
rejection/replay, target-dependent opposite-liquidity drift, active/dormant
misclassification, activation forecast, partial curve, mutation or WAL growth
before acceptance, reservation loss on risk rejection, interposed shard
transition, durable protocol or recovery drift, or new wire value falsifies
A144.

## A145 — atomic conditional cancellation observation

**Assumption.** One `submit_cancel_order_if` call on an `OrderBook`,
`RiskManagedOrderBook`, `DurableOrderBook`, or `DurableRiskOrderBook` holds the
corresponding exclusive mutable shard borrow across ordinary `CancelOrder`
preparation, construction of one `ActiveOrderObservation`, a borrowed local
predicate, and commit of that same preparation. It reuses the ordinary
cancellation, coupled-risk trace, report, persistence, and recovery paths.

Exact replay plus unknown-order, wrong-owner, and other core business
rejections return a reported `ConditionalCommandOutcome` without an
observation or predicate execution. Otherwise the observation binds instrument
ID, immutable definition version, last visible book event sequence, queried
order ID, and validated resting or dormant-stop state. A contradictory private
order snapshot returns `ConditionalOrderError::Query` before the predicate.
Predicate decline or unwind drops immutable preparation before consuming
sequence, event, matching, risk, history, public, or WAL state. Acceptance
commits without an intervening shard transition and returns the exact
observation with the ordinary cancellation report. Coupled risk releases the
target reservation exactly once on commit and retains it on every noncommit
path.

Durable acceptance and business rejection retain command-before-state-before-
report and append two existing frames. Observation failure, decline, unwind,
and replay append zero frames. The observation and callback decision are
process-local, unencoded, unauthenticated, and valid only within the call.
Existing command, report, checkpoint, market-data, and wire values are
unchanged.

**Dependent results.** [A1, A2, A3, A4, A5, A7, A9, A10, A12, A15, A18,
A19, A22, A37, A39, A44, A45, A47, A55, A57, A67, A72, A80, A81, A82,
A83, A85, A103, A139, A142, A143, A144, A145] Let `A` be ordinary
cancellation preparation cost, `V` the validated active-order lookup cost,
`F` predicate cost, and `M` cancellation commit cost. Acceptance costs
`O(A + V + F + M)` and decline costs `O(A + V + F)`, with `O(1)` observation
and evaluator auxiliary space. `V` is one expected `O(1)` order-index lookup
plus fixed-size resting or dormant-stop validation. Core rejection and replay
pay only their existing preparation path and skip `V` and `F`. Coupled risk
retains expected `O(1)` cancellation authorization and reservation release.
Durable acceptance and business rejection append two existing frames;
observation failure, decline, unwind, and replay append zero. All arithmetic is
exact integer arithmetic; approximation error is zero.

**Falsification probe.** Across all four surfaces, exercise fully displayed,
reserve, fully hidden, and dormant stop-market/stop-limit targets on both sides
and signed prices. Run accepting, declining, and unwinding predicates. Require
the same instrument/version/sequence/order provenance and complete resting or
dormant state observed immediately before accepted cancellation. Exercise
unknown and wrong-owner targets, wrong route or version, blocked/non-open
admission where applicable, exact retry, command-ID collision, sequence/event/
capacity exhaustion, private-order corruption, and durable write/report
failure.

Count predicate calls; compare target snapshots and queue priority, private/
public book, risk positions/reservations/exposures, command history, WAL frames,
and plain/coupled-risk reopen state. Any observation or predicate on rejection
or replay, active/dormant misclassification, accepted observation/report
provenance drift, mutation or WAL growth before acceptance, reservation loss
on noncommit, missing reservation release on acceptance, interposed shard
transition, durable protocol or recovery drift, or new wire value falsifies
A145.

## A146 — atomic conditional mass-cancellation observation

**Assumption.** One `try_submit_mass_cancel_if` call on an `OrderBook`,
`RiskManagedOrderBook`, `DurableOrderBook`, or `DurableRiskOrderBook` holds the
corresponding exclusive mutable shard borrow across ordinary `MassCancel`
preparation, canonical selected-ID construction in that preparation's move-only
A87 lease, complete caller-owned selected-state construction, a borrowed local
predicate, and commit of that same preparation. It reuses the ordinary mass-
cancellation, coupled-risk trace, report, persistence, and recovery paths.

For a core-admissible all-order or one-side scope containing `K` active orders,
selection performs one expected A41 account lookup, traverses exactly those
intrusive members, validates owner/side/link/count state, and sorts unique IDs
in ascending `OrderId` order inside the already leased vector. One exact
fallible request for `K` `ActiveOrderSnapshot` rows then materializes every
selected fully displayed, reserve, fully hidden, or dormant-stop state through
the A138 exact-order validator. `MassCancelObservation` binds that ordered
state to instrument ID, immutable definition version, last visible book event
sequence, account, scope, selected count, and the checked exact `u128` sum of
total leaves. Caller output never aliases the constructor-owned lease.

Exact replay plus wrong-route, wrong-version, and other core business
rejections return a reported `ConditionalCommandOutcome` without selection,
caller output, or predicate execution. An empty valid selection instead
constructs an empty observation and invokes the predicate. Output reservation
or selected-list corruption returns `ConditionalOrderError::Query` before the
predicate or semantic mutation. Predicate decline or unwind drops the
preparation, clears and returns its lease, and changes no sequence, event,
matching, risk, history, public, or WAL state. Acceptance validates and consumes
the identical prepared IDs without another account-list traversal or sort.
Coupled risk releases exactly those reservations once from the ordinary trace.

Durable acceptance and business rejection retain command-before-state-before-
report and append two existing frames. Query failure, decline, unwind, and
replay append zero frames. The observation and callback decision are process-
local, unencoded, unauthenticated, and valid only within the call. Existing
command, report, checkpoint, market-data, and wire values are unchanged.

**Dependent results.** [A1, A2, A3, A4, A5, A7, A9, A10, A12, A15, A18,
A19, A20, A22, A37, A38, A39, A41, A48, A52, A54, A57, A72, A77, A80,
A81, A82, A84, A87, A103, A117, A138, A145, A146] Let `A` be ordinary
mass-cancel preparation cost, `K` selected orders, `F` predicate cost, and `M`
ordinary mass-cancel commit cost. Selection, ascending canonicalization, and
complete snapshot validation cost expected `O(K log K)` after the expected
`O(1)` account lookup. Acceptance costs `O(A + K log K + F + M)` and decline
costs `O(A + K log K + F)`. Accepted commit does not repeat the selection or
sort. The fixed prepared lease owns `O(K)` selected-ID scratch and the returned
or retained caller observation owns `O(K)` rows; evaluator auxiliary state is
`O(1)`. Core rejection and replay retain their existing preparation path and
skip selection, caller allocation, and `F`. Coupled risk adds expected `O(K)`
reservation release on acceptance. Durable acceptance and business rejection
append two existing frames; query failure, decline, unwind, and replay append
zero. Count and quantity arithmetic is exact integer arithmetic; approximation
error is zero.

**Falsification probe.** Across all four surfaces, exercise empty, all-order,
buy-only, and sell-only scopes over mixed fully displayed, reserve, fully
hidden, dormant stop-market, and dormant stop-limit state with nonmonotonic
identities. Run accepting, declining, and unwinding predicates. Require exact
instrument/version/sequence/account/scope provenance, ascending IDs, complete
snapshots, selected count, `u128` total leaves, unchanged unrelated orders, and
one predicate call for a valid empty selection. Exercise wrong route/version,
every core rejection, exact retry, command-ID collision, selected-output
reservation failure, account-list corruption, selection-pool exhaustion, and
durable write/report failure.

Count predicate and allocation calls; compare selection-pool availability,
private/public book, risk positions/reservations/exposures, command history,
WAL frames, and plain/coupled-risk reopen state. Any selection, output, or
predicate on rejection or replay; unrelated-order scan; noncanonical,
incomplete, aliased, or provenance-drifted observation; second selection at
commit; mutation or WAL growth before acceptance; lease loss; reservation
release on noncommit or missing release on acceptance; interposed shard
transition; durable protocol or recovery drift; or new wire value falsifies
A146.

## A147 — atomic conditional account-control observation

**Assumption.** One `try_submit_account_control_if` call on an `OrderBook`,
`RiskManagedOrderBook`, `DurableOrderBook`, or `DurableRiskOrderBook` holds the
corresponding exclusive mutable shard borrow across ordinary `AccountControl`
preparation, construction of one `AccountControlSubmissionObservation`, a
borrowed local predicate, and commit of that same preparation. It reuses the
ordinary revisioned fence, account-order cancellation, coupled-risk trace,
report, persistence, and recovery paths.

The observation contains the exact current A137 `AccountControlObservation`,
requested action, and resulting fence state. For a core-admissible
`BlockAndCancel` selecting `K` active orders, preparation performs the same
expected A41 account lookup, selected-link validation, and ascending
`OrderId` canonicalization as A146 inside its move-only A87 lease. It makes one
exact fallible request for `K` caller-owned `ActiveOrderSnapshot` rows,
validates every selected fully displayed, reserve, fully hidden, or dormant-
stop state through A138, and reports the exact checked `u128` total leaves.
`Enable` has no selected orders, requests no caller-owned selected output, and
acquires no selection lease.

Exact replay plus wrong-route, wrong-version, stale-revision, exhausted-
revision, and other core business rejections return a reported
`ConditionalCommandOutcome` without observation or predicate execution. On a
coupled-risk shard, missing-profile authorization occurs before selected-state
construction and predicate execution. A query failure, predicate decline, or
unwind drops preparation and returns any lease before sequence, event,
matching, risk, history, public, or WAL mutation. Acceptance validates and
consumes the identical prepared IDs without another account-list traversal or
sort, advances the account fence once, and releases exactly the selected
reservations through the ordinary cancellation trace.

Durable acceptance and business rejection retain command-before-state-before-
report and append the existing two frames. Query failure, decline, unwind, and
replay append zero frames. The observation and callback decision are process-
local, unencoded, unauthenticated, and valid only within the call. Existing
command, report, checkpoint, market-data, and wire values are unchanged.

**Dependent results.** [A1, A2, A3, A4, A5, A7, A9, A10, A12, A15, A18,
A19, A20, A22, A37, A38, A39, A41, A48, A52, A54, A57, A72, A77, A80,
A81, A82, A84, A87, A103, A117, A137, A138, A146, A147] Let `A` be ordinary
account-control preparation cost, `K` selected orders, `F` predicate cost,
and `M` ordinary account-control commit cost. Block-and-cancel selection,
canonicalization, and complete selected-state validation cost expected
`O(K log K)` after the expected `O(1)` account lookup. Acceptance costs
`O(A + K log K + F + M)` and decline costs `O(A + K log K + F)`. Accepted
commit reuses the prepared IDs and does not repeat account-list selection or
sorting. The fixed prepared lease owns `O(K)` selected-ID scratch, the returned
or retained observation owns `O(K)` rows, and evaluator auxiliary state is
`O(1)`. Enable acceptance is `O(A + F + M)`, decline is `O(A + F)`, and its
observation has `O(1)` state with no selected-output allocation or lease.
Core/risk rejection and replay retain their existing preparation/gate path and
skip selected output and `F`. Coupled acceptance releases `K` reservations in
expected `O(K)` time. Durable acceptance and business rejection append two
existing frames; query failure, decline, unwind, and replay append zero. Count,
revision, and quantity arithmetic is exact integer arithmetic; approximation
error is zero.

**Falsification probe.** Across all four surfaces, exercise block-and-cancel
over mixed fully displayed, reserve, fully hidden, dormant stop-market, and
dormant stop-limit state with nonmonotonic identities, plus enable from a
current blocked fence. Run accepting, declining, and unwinding predicates.
Require exact instrument/version/sequence/account/current-fence/action/
resulting-fence provenance, ascending IDs, complete snapshots, selected count,
`u128` total leaves, unchanged unrelated orders, and empty allocation-free
enable selection. Exercise wrong route/version, stale and exhausted revisions,
every core rejection, unprofiled coupled risk, exact retry, command-ID
collision, selected-output reservation failure, account-list corruption,
selection-pool exhaustion, and durable write/report failure.

Count predicate and allocation calls; compare selection-pool availability,
private/public book, fence revision, risk positions/reservations/exposures,
command history, WAL frames, and plain/coupled-risk reopen state. Any selected
output or predicate on rejection or replay; selected work before risk
authorization; enable lease/output allocation; unrelated-order scan;
noncanonical, incomplete, aliased, or provenance-drifted observation; second
selection at commit; partial fence/cancellation mutation; WAL growth before
acceptance; lease loss; reservation release on noncommit or missing release on
acceptance; interposed shard transition; durable protocol or recovery drift;
or new wire value falsifies A147.

## A148 — atomic conditional trading-state-control observation

**Assumption.** One `try_submit_trading_state_control_if` call on an
`OrderBook`, `RiskManagedOrderBook`, `DurableOrderBook`, or
`DurableRiskOrderBook` holds the corresponding exclusive mutable shard borrow
across ordinary `TradingStateControl` preparation, construction of one
`TradingStateControlSubmissionObservation`, a borrowed local predicate, and
commit of that same preparation. It reuses the ordinary revisioned instrument
state, all-order cancellation, coupled-risk trace, report, persistence, and
recovery paths.

The observation contains the exact current A136 `TradingStateObservation`,
requested target state, action, and resulting state. For a core-admissible
`TransitionAndCancel` selecting `O` active orders, preparation fills the same
move-only A87 all-order lease acquired by ordinary preparation, canonicalizes
its IDs in ascending `OrderId` order, makes one exact fallible request for `O`
caller-owned `ActiveOrderSnapshot` rows, validates every selected fully
displayed, reserve, fully hidden, or dormant-stop state through A138, and
reports the exact checked `u128` total leaves. Accepted commit validates and
consumes those identical prepared IDs without another all-order scan or sort.
`Transition` has no selected orders, requests no caller-owned selected output,
and acquires no selection lease.

Exact replay plus wrong-route, wrong-version, stale-revision, exhausted-
revision, unchanged-state, transition-and-cancel-to-open, and other core
business rejections return a reported `ConditionalCommandOutcome` without
observation or predicate execution. Coupled risk performs its existing
account-independent state-control authorization before observation. A query
failure, predicate decline, or unwind drops preparation and returns any lease
before sequence, event, matching, risk, history, public, or WAL mutation.
Acceptance advances the instrument state once and releases exactly the
selected reservations through the ordinary cancellation trace.

Durable acceptance and business rejection retain command-before-state-before-
report and append the existing two frames. Query failure, decline, unwind, and
replay append zero frames. The observation and callback decision are process-
local, unencoded, unauthenticated, and valid only within the call. Existing
command, report, checkpoint, market-data, and wire values are unchanged.

**Dependent results.** [A1, A2, A3, A4, A5, A7, A9, A10, A12, A15, A19,
A20, A22, A37, A39, A48, A52, A59, A72, A77, A80, A81, A82, A84, A87,
A103, A117, A136, A138, A146, A147, A148] Let `A` be ordinary trading-state-
control preparation cost, `O` active selected orders, `F` predicate cost, and
`M` ordinary control commit cost. Transition-and-cancel selection,
canonicalization, and complete selected-state validation cost expected
`O(O log O)`. Acceptance costs `O(A + O log O + F + M)` and decline costs
`O(A + O log O + F)`. Accepted commit reuses the prepared IDs and does not
repeat all-order selection or sorting. The fixed prepared lease owns `O(O)`
selected-ID scratch, the returned or retained observation owns `O(O)` rows,
and evaluator auxiliary state is `O(1)`. Transition acceptance is
`O(A + F + M)`, decline is `O(A + F)`, and its observation has `O(1)` state
with no selected-output allocation or lease. Core rejection and replay retain
their existing preparation path and skip selected output and `F`. Coupled
acceptance releases `O` reservations in expected `O(O)` time. Durable
acceptance and business rejection append two existing frames; query failure,
decline, unwind, and replay append zero. Count, revision, and quantity
arithmetic is exact integer arithmetic; approximation error is zero.

**Falsification probe.** Across all four surfaces, exercise empty and mixed
fully displayed, reserve, fully hidden, dormant stop-market, and dormant stop-
limit books with nonmonotonic identities. Run transition-and-cancel and
transition-only controls with accepting, declining, and unwinding predicates.
Require exact instrument/version/sequence/current-state/revision/target/action/
resulting-state provenance, ascending IDs, complete snapshots, selected count,
`u128` total leaves, one predicate call for a valid empty cancellation set, and
allocation-free transition-only observation. Exercise wrong route/version,
stale and exhausted revisions, unchanged state, transition-and-cancel-to-open,
every core rejection, exact retry, command-ID collision, selected-output
reservation failure, selected-state corruption, selection-pool exhaustion,
and durable write/report failure.

Count predicate and allocation calls; compare selection-pool availability,
private/public book, instrument state/revision, risk positions/reservations/
exposures, command history, WAL frames, and plain/coupled-risk reopen state.
Any selected output or predicate on rejection or replay; transition-only lease
or output allocation; noncanonical, incomplete, aliased, or provenance-drifted
observation; second all-order selection or sort at commit; partial state or
cancellation mutation; WAL growth before acceptance; lease loss; reservation
release on noncommit or missing release on acceptance; interposed shard
transition; durable protocol or recovery drift; or new wire value falsifies
A148.

## A149 — atomic conditional expiry-sweep observation

**Assumption.** One `try_submit_expiry_sweep_if` call on an `OrderBook`,
`RiskManagedOrderBook`, `DurableOrderBook`, or `DurableRiskOrderBook` holds the
corresponding exclusive mutable shard borrow across ordinary `ExpirySweep`
preparation, construction of one `ExpirySweepObservation`, a borrowed local
predicate, and commit of that same preparation. It reuses the ordinary
inclusive watermark, expiry cancellation, coupled-risk trace, report,
persistence, and recovery paths.

For a core-admissible horizon selecting `K` active GTD orders, observation
fills the same move-only A87 lease acquired by ordinary preparation exactly
once in canonical `(expires_at, OrderId)` order. Each selected expiry-index
entry is checked against its exact active order and deadline before one exact
fallible request for `K` caller-owned `ActiveOrderSnapshot` rows. The result
binds instrument ID, immutable definition version, last visible book event
sequence, previous optional watermark, requested horizon and resulting current
watermark, complete selected resting or dormant-stop states, selected count,
and the checked exact `u128` total leaves. Accepted commit validates and drains
those identical prepared IDs without another expiry-prefix traversal or sort.
An empty valid prefix acquires no lease, allocates no selected-order output,
and still invokes the predicate with the watermark transition.

Exact replay plus wrong-route, wrong-version, a horizon after command time,
watermark regression, and other core business rejections return a reported
`ConditionalCommandOutcome` without selection, observation, or predicate
execution. Coupled risk performs its existing account-independent expiry
authorization before observation. A query failure, predicate decline, or
unwind drops preparation and returns any lease before sequence, event,
matching, risk, history, public, or WAL mutation. Acceptance advances the
watermark and releases exactly the selected reservations through the ordinary
canonical cancellation trace.

Durable acceptance and business rejection retain command-before-state-before-
report and append the existing two frames. Query failure, decline, unwind, and
replay append zero frames. The observation and callback decision are process-
local, unencoded, unauthenticated, and valid only within the call. Existing
command, report, checkpoint, market-data, and wire values are unchanged.

**Dependent results.** [A1, A2, A3, A4, A5, A9, A10, A12, A15, A20, A21,
A22, A37, A39, A48, A50, A52, A55, A72, A77, A80, A81, A82, A84, A87,
A103, A117, A138, A145, A146, A147, A148, A149] Let `A` be ordinary expiry-
sweep preparation cost, `K` selected orders, `X` active GTD orders, `F`
predicate cost, and `M` ordinary expiry commit cost. Canonical selection and
exact expiry-index validation cost
`O(K log(X + 1))` after ordinary preparation; complete selected-state
validation adds expected `O(K)` active-order work. Acceptance costs
`O(A + K log(X + 1) + F + M)` and decline costs
`O(A + K log(X + 1) + F)`. Accepted commit reuses the prepared IDs and does
not repeat the ordered-prefix traversal or sorting. The constructor-owned lease
retains `O(K)` ID scratch, caller output owns `O(K)`
`ActiveOrderSnapshot` rows, and evaluator auxiliary state is `O(1)`. Core
rejection and replay retain their existing preparation path and skip selection,
output, and `F`; a valid empty prefix invokes `F` without a lease or selected
output. Coupled acceptance releases `K` reservations in expected `O(K)` time.
Durable acceptance and business rejection append two existing frames; query
failure, decline, unwind, and replay append zero. Count, quantity, and
nanosecond-watermark arithmetic is exact integer arithmetic; approximation
error is zero.

**Falsification probe.** Across all four surfaces, exercise empty and mixed
fully displayed, reserve, fully hidden, dormant stop-market, and dormant stop-
limit GTD state with equal deadlines and nonmonotonic identities. Run
accepting, declining, and unwinding predicates. Require exact instrument/
version/sequence/previous-watermark/horizon/current-watermark provenance,
canonical `(expires_at, OrderId)` rows, complete snapshots, selected count,
`u128` total leaves, unchanged later-GTD and GTC orders, and one predicate call
for a valid empty prefix. Exercise wrong route/version, a future horizon,
watermark regression, every core rejection, exact retry, command-ID collision,
selected-output reservation failure, expiry-index corruption, selection-pool
exhaustion, and durable write/report failure.

Count predicate and allocation calls; compare selection-pool availability,
private/public book, expiry watermark, risk positions/reservations/exposures,
command history, WAL frames, and plain/coupled-risk reopen state. Any selection,
output, or predicate on rejection or replay; noncanonical, incomplete, aliased,
or provenance-drifted observation; second expiry-prefix selection or sort at
commit; partial watermark/cancellation mutation; WAL growth before acceptance;
lease loss; reservation release on noncommit or missing release on acceptance;
interposed shard transition; durable protocol or recovery drift; or new wire
value falsifies A149.

## A150 — atomic conditional stop-trigger-sweep observation

**Assumption.** One `try_submit_stop_trigger_sweep_if` call on an `OrderBook`,
`RiskManagedOrderBook`, `DurableOrderBook`, or `DurableRiskOrderBook` holds the
corresponding exclusive mutable shard borrow across ordinary
`StopTriggerSweep` preparation, construction of one
`StopTriggerSweepObservation`, a borrowed local predicate, and commit of that
same preparation. It reuses the ordinary sourced-reference transition,
bounded activation, coupled-risk trace, report, persistence, and recovery
paths.

For a core-admissible reference selecting `K` dormant stops, observation fills
the same move-only A87 lease acquired by ordinary preparation exactly once
with the canonical eligible side prefix. Buy priority is `(trigger ascending,
priority sequence, OrderId)`; sell priority is `(trigger descending, priority
sequence, OrderId)`. Each selected trigger-index entry is checked against its
exact dormant stop before one exact fallible request for `K` caller-owned
`DormantStopSnapshot` rows. The result binds instrument ID, immutable
definition version, last visible book event sequence, previous optional
reference, requested sourced reference, positive maximum batch size, complete
selected dormant states, selected count, checked exact `u128` total leaves,
and remaining eligible count. Accepted commit validates and drains those
identical prepared IDs without another trigger-prefix selection or sort. An
empty valid prefix acquires no lease, allocates no selected-order output, and
still invokes the predicate with the reference transition.

Exact replay plus wrong-route, wrong-version, zero maximum, invalid source
continuity, an advance while an eligible backlog remains, and other core
business rejections return a reported `ConditionalCommandOutcome` without
selection, observation, or predicate execution. Coupled risk performs its
existing account-independent trigger-sweep authorization before observation.
A query failure, predicate decline, or unwind drops preparation and returns
any lease before sequence, event, reference, matching, risk, history, public,
or WAL mutation. Acceptance advances the reference and transitions exactly
the selected reservations through the ordinary canonical trigger, trade,
cancellation, and residual trace.

Durable acceptance and business rejection retain command-before-state-before-
report and append the existing two frames. Query failure, decline, unwind, and
replay append zero frames. The observation and callback decision are process-
local, unencoded, unauthenticated, and valid only within the call. Existing
command, report, checkpoint, market-data, and wire values are unchanged.

**Dependent results.** [A1, A2, A3, A4, A5, A9, A10, A12, A15, A20, A21,
A22, A37, A39, A48, A50, A52, A55, A72, A77, A80, A81, A82, A84, A87, A102,
A103, A106, A117, A138, A145, A146, A147, A148, A149, A150] Let `A` be
ordinary trigger-sweep preparation cost, `K` selected stops, `S` dormant stops
in the selected side index, `F` predicate cost, and `M` ordinary trigger-sweep
commit cost. Canonical selection and exact trigger-index validation cost
`O(K log(S + 1))` after ordinary preparation; complete selected-state
validation adds expected `O(K)` dormant-order work. Acceptance costs
`O(A + K log(S + 1) + F + M)` and decline costs
`O(A + K log(S + 1) + F)`. Accepted commit reuses the prepared IDs and does
not repeat trigger-prefix selection or sorting. The constructor-owned lease
retains `O(K)` ID scratch, caller output owns `O(K)`
`DormantStopSnapshot` rows, and evaluator auxiliary state is `O(1)`. Core
rejection and replay retain their existing preparation path and skip selection,
output, and `F`; a valid empty prefix invokes `F` without a lease or selected
output. Coupled acceptance applies the ordinary risk trace for exactly the
selected activations and their resulting lifecycle. Durable acceptance and
business rejection append two existing frames; query failure, decline, unwind,
and replay append zero. Count, quantity, price, and source-coordinate
arithmetic is exact integer arithmetic; approximation error is zero.

**Falsification probe.** Across all four surfaces, exercise empty, truncated,
and remaining-backlog selections containing fully displayed, reserve, fully
hidden, stop-market, stop-limit, GTC, and GTD state. Cover buy and sell sides,
equal triggers with nonmonotonic identities, and multiple priority sequences.
Run accepting, declining, and unwinding predicates. Require exact instrument/
version/sequence/prior-reference/requested-reference/maximum provenance,
canonical side-specific rows, complete snapshots, selected count, `u128` total
leaves, remaining eligible count, unchanged ineligible stops, and one predicate
call for a valid empty prefix. Exercise wrong route/version, zero maximum,
source discontinuity, source regression, advance with backlog, every core
rejection, exact retry, command-ID collision, selected-output reservation
failure, trigger-index corruption, selection-pool exhaustion, and durable
write/report failure.

Count predicate and allocation calls; compare selection-pool availability,
private/public book, stop reference and backlog, risk positions/reservations/
exposures, command history, WAL frames, and plain/coupled-risk reopen state.
Any selection, output, or predicate on rejection or replay; mixed-side,
noncanonical, incomplete, aliased, or provenance-drifted observation; second
trigger-prefix selection or sort at commit; partial reference/activation
mutation; WAL growth before acceptance; lease loss; reservation change on
noncommit or trace divergence on acceptance; interposed shard transition;
durable protocol or recovery drift; or new wire value falsifies A150.

## A151 — atomic conditional call-auction uncross

**Assumption.** One `try_submit_uncross_if` call on a `CallAuctionEngine`,
`CallAuctionRiskManagedEngine`, `DurableCallAuctionEngine`, or
`DurableCallAuctionRiskEngine` holds the corresponding exclusive mutable shard
borrow across ordinary A63/A64 uncross preparation, complete preparation
validation, one borrowed local predicate, and commit of that same move-only
preparation. The command remains an ordinary `CallAuctionUncrossCommand` and
the accepted/rejected result remains an ordinary `CallAuctionExecutionReport`.

For a core-admissible executable uncross, `CallAuctionUncrossObservation`
borrows the A86 allocation plan, deterministic counterparty trades, and
remainder cancellations directly from the preparation. Its fixed-size value
also binds exact command identity and receive time, instrument ID/version,
prospective command and first-event sequences, current and resulting phase and
book revisions, active auction, price band, reference, price and uncross
policies, and the previous still-current A112 indication. Before predicate
execution, the engine proves same-instance/same-generation engine state and
revalidates every fill, trade, cancellation, source aggregate, account binding,
trade-ID range, phase, revision, auction, and policy relation. The observation
cannot outlive or mutate the preparation.

Exact replay and every core business rejection, including A113 self-trade
abort, bypass the predicate and return a reported outcome. Predicate decline
or unwind drops the preparation and returns its A86 lease before consuming a
sequence or changing event, indication, phase, book, trade identity, history,
risk, public, or WAL state. Acceptance commits without an intervening engine
mutation. Coupled acceptance applies the existing complete uncross risk trace
once; its account-independent authorization adds no new rejection class.

Durable acceptance and business rejection retain command-before-state-before-
report and append the existing two frames. Decline, unwind, and replay append
zero frames. The observation and callback decision are process-local,
unencoded, unauthenticated, unauthorized, and retained only for the predicate
duration. A151 introduces no command, report, market-data, checkpoint, snapshot,
or WAL value and requires no wire-version change.

**Dependent results.** [A1, A2, A3, A4, A5, A9, A10, A12, A15, A22, A37,
A39, A52, A60, A61, A62, A63, A64, A65, A67, A72, A80, A81, A82, A85, A86,
A110, A112, A113, A151] Let `A` be ordinary uncross preparation cost, `O` the
canonical source-order count, `F_b + F_a` positive fills, `T` trade pairs, `C`
remainder cancellations, `V` pre-predicate validation, `F` predicate cost, and
`M` ordinary commit cost. Stable-AVL identity resolution and linear source
scans give `V = O(O + (F_b + F_a + T + C) log(O + 1))`. Acceptance costs
`O(A + V + F + M)` and decline or unwind costs `O(A + V + F)`. Ordinary `M`
retains its own preparation validation before mutation, so acceptance performs
the deliberate pre-predicate validation plus the ordinary commit validation.
The observation and evaluator add `O(1)` auxiliary state and no allocation;
the existing A86 lease retains `O(F_b + F_a + T + C)` live elements. Coupled
acceptance retains expected `O(T + C)` risk application. Durable acceptance and
business rejection append two existing frames; decline, unwind, and replay
append zero. Count, quantity, price, revision, and sequence arithmetic is exact
integer arithmetic; approximation error is zero.

**Falsification probe.** Across all four surfaces, exercise empty/nonexecutable,
one-pair, multi-pair, partial, complete, retained-remainder, market-only,
limit-only, mixed, signed-price, price-time, pro-rata-time, `Permit`, and A113
`Abort` uncrosses. Run accepting, declining, and unwinding predicates. Require
exact command/instrument/sequence/time/phase/book/auction/band/reference/policy/
previous-indication provenance, pointer identity with the prepared plan/trade/
cancellation storage, and exact equality between accepted observation and the
committed trace. Exercise wrong route/version/cycle/revision/phase, empty
execution, every business rejection, exact retry, command-ID collision,
sequence/history/report exhaustion, A86 pool exhaustion, and internally stale
or foreign preparations before predicate execution.

Count predicate calls and compare lease availability, private/public book,
phase, indication, sequence/trade counters, risk reservations/exposures/
positions, command history, WAL frames, and plain/coupled reopen state. Any
predicate on rejection or replay; copied, incomplete, mutable, retained, or
provenance-drifted observation; callback before exact preparation validation;
different accepted allocation, pair, or cancellation; sequencing or state
change on noncommit; WAL growth before acceptance; lost lease; risk trace
duplication or omission; interposed engine mutation; durable grammar/recovery
drift; allocation; or new wire value falsifies A151.

## A152 — provenance-bound call-auction order observation and conditional cancellation

**Assumption.** `CallAuctionBook::try_order_observation` returns one owned,
fixed-size `CallAuctionOrderObservation` bound to the immutable instrument ID,
instrument-definition version, current collection-book revision, and requested
`OrderId`. Indexed absence produces `state = None`. Present state is exposed
only after the selected active row is consistent with accepted identity,
instrument quantity/price rules, assigned priority prefix, its exact
market/limit queue aggregates and endpoints, reciprocal immediate queue links,
and its owner's exact side-lane aggregates, endpoints, and reciprocal immediate
links. This is a selected-order local proof, not a complete book/topology audit;
unrelated corruption remains the responsibility of the A74 offline audit.

One `try_submit_cancel_order_if` call on `CallAuctionEngine`,
`CallAuctionRiskManagedEngine`, `DurableCallAuctionEngine`, or
`DurableCallAuctionRiskEngine` holds the corresponding exclusive shard borrow
across ordinary owner-cancel preparation, fail-closed selected-order
observation, one synchronous predicate, and commit of that same move-only
preparation. `CallAuctionCancelObservation` owns the exact target snapshot and
binds command identity/time, instrument ID/version, prospective command and
first-event sequences, phase/cycle snapshot, source/resulting book revisions,
and the previous still-current A112 indication. Its compact internal indication
form is lossless: current auction/phase/book coordinates are independently
validated and the clearing result is reconstructed exactly from price and
eligible buy/sell `u128` quantities. The public value is `Copy`, has no
destructor, and is independent of the preparation after return.

Exact replay and every core business rejection bypass observation and the
predicate and return `observation = None` with the ordinary report. Decline
returns the owned observation without consuming sequence, invalidating the
indication, mutating book/history/risk, or appending WAL; unwind has the same
state semantics. Acceptance revalidates the stored source revision and exact
selected state at commit, removes that same order, invalidates the current
indication, and returns `observation = Some` with the ordinary report. Coupled
authorization is account-independent for cancellation; accepted application
releases exactly the target reservation once. The coupled wrapper validates
that account-independent authorization before the predicate and repeats it at
ordinary commit. Durable acceptance and business rejection retain the existing
command/report two-frame grammar; decline,
unwind, and replay append zero frames. Neither observation nor decision is
encoded, authenticated, authorized, remotely transported, or coordinated
across shards. A152 adds no wire value or version.

**Dependent results.** [A1, A2, A3, A4, A5, A9, A10, A12, A15, A22, A37,
A39, A52, A60, A62, A64, A65, A67, A72, A74, A80, A81, A82, A85, A112,
A151, A152] For `O` active orders, `P` occupied limit prices, and `A` active
accounts, one present direct observation performs a constant number of stable-
AVL identity/neighbor lookups and at most one price lookup plus one expected
constant-time account lookup. Expected time is `O(log(O + 1) + log(P + 1))`,
auxiliary space is `O(1)`, and successful allocation is zero. A fully
colliding account hash can add `O(A)` lookup time without storage growth.
Absent lookup is `O(log(O + 1))`.

Ordinary conditional preparation and pre-predicate observation each retain the
same AVL bound. Decline or unwind costs
`O(log(O + 1) + log(P + 1) + F)` for predicate cost `F`; acceptance adds one
commit-time selected observation plus ordinary cancellation and therefore
retains `O(log(O + 1) + log(P + 1) + F)` asymptotic time with larger constant
factors. The observation and generic conditional evaluator add `O(1)` storage.
Coupled application is expected `O(1)` beyond book work. Durable acceptance
and business rejection append two existing frames; decline, unwind, and replay
append zero. Count, quantity, price, revision, and sequence arithmetic is exact
integer arithmetic; approximation error is zero.

**Falsification probe.** Exercise missing, singleton, head, middle, and tail
market/limit targets on both sides, multiple accounts, classes, signed prices,
and present/absent previous indications with and without clearing. Corrupt the
selected identity, accepted-ID membership, quantity, price, priority sequence,
queue aggregate/endpoints, immediate queue links, owner membership, owner-lane
aggregate/endpoints, and immediate owner links; require a typed query failure
before state output or predicate execution. Mutate an unrelated order through
an internal white-box path after cancel preparation and require stale-
generation rejection before the predicate.

Across all four conditional surfaces, run accepting, declining, and unwinding
predicates; exercise wrong route/version, wrong owner, missing target, exact
retry, command-ID collision, counter/history exhaustion, selected-state
corruption, journal failure, and reopen. Require exact observation/report event
equality, one predicate call only for a core-admissible target, unchanged
sequence/book/indication/history/risk/WAL on noncommit, exact target-only
removal and reservation release on acceptance, zero replay frames, two accepted
frames, and identical plain/coupled recovered state. Any partial or stale
observation, predicate on rejection/replay, hidden whole-book-validity claim,
interposed mutation, noncommit effect, different removed target, reservation
drift, WAL-before-acceptance, recovery divergence, successful-path allocation,
or new wire value falsifies A152.

## Bounded scope expansion

Each entry below is tagged with an impact level and records an implemented
capability, a remaining risk, or an opportunity.

- **High impact:** A152 closes the local gap between observing one active
  call-auction target and committing its owner cancellation. The exact target,
  provenance, phase, revision, sequence, and prior indication are owned by the
  outcome, while the exclusive engine borrow prevents an interposed safe
  transition and accepted commit revalidates the same target.

- **Medium impact opportunity:** deterministic OMS, reconciliation,
  surveillance, and exposure controls can condition cancellation on exact
  private target state without copying a full book or inferring state from a
  later event. The reusable owned conditional evaluator can support future
  amendment/replacement controls after their operation-specific observations
  and gates are defined.

- **Medium impact risk:** A152 validates the selected order, immediate price-
  queue neighbors, immediate owner-lane neighbors, and redundant local
  aggregates. It deliberately does not traverse unrelated topology, and the
  synchronous predicate extends the exclusive shard borrow. Whole-book
  integrity still requires A74; maximum-capacity predicate and cache-latency
  effects remain unknown pending pinned-hardware measurement.

- **High impact boundary:** A152 supplies no principal authentication,
  cancel-on-behalf authorization, durable decision evidence before command
  append, remote protocol, callback deadline, or multi-shard atomicity. A
  process failure after local acceptance but before WAL command append retains
  no recoverable policy decision.

- **High impact:** A151 closes the process-local gap between call-auction
  allocation/pairing observation and uncross commit. Policy code can inspect
  the exact prepared fills, counterparty pairs, remainder cancellations,
  revisions, and prior indication while the exclusive engine borrow prevents
  an interposed local transition; acceptance consumes that same preparation.

- **Medium impact opportunity:** exact pre-uncross allocation, pairing, and
  cancellation views can support deterministic local capacity, concentration,
  exposure, surveillance, and execution-quality gates without reconstructing
  private economics from the later event trace. Cross-shard or external
  decisions still require synchronized and authenticated inputs.

- **Medium impact risk:** the A151 predicate extends the exclusive local shard
  borrow while it runs and retains one A86 lease. Slow or blocked callback work
  therefore increases shard latency and can delay lease return. The operation
  intentionally performs full fail-closed preparation validation before the
  predicate and repeats ordinary validation at accepted commit; pinned-hardware
  latency at configured maximum order count remains unknown.

- **High impact boundary:** A151 is not a remote conditional-order protocol,
  authorization system, durable decision record, multi-shard transaction, or
  authenticated reference/band source. A process failure after a predicate
  accepts but before command persistence leaves no durable evidence of that
  decision; recovery can reproduce only appended ordinary commands.

- **High impact:** continuous matching now supports a frozen-best
  market-to-limit instruction with GTC/GTD residuals, hidden-best capture,
  reserve/hidden display, all STP policies, conservative pre-trade risk,
  explicit private pricing provenance, public projection, and WAL/snapshot
  recovery.

- **Medium impact risk:** Quotick captures the private executable best,
  including hidden-only liquidity. Venue protocols can define market-to-limit
  against a displayed, protected, routed, or otherwise filtered best and can
  permit different lifetime or display combinations; those adapters require
  separately versioned conformance rules.

- **Medium impact opportunity:** the explicit pricing event permits exact OMS,
  risk, surveillance, and replay attribution of the conversion price without
  inferring it from public BBO or the first trade.

- **High impact:** authoritative continuous public-depth observations now
  share the same coherent-extrema and selected-row validation semantics as
  healthy replicas. Full/range streaming remains allocation-free and double-
  ended; materializers reserve exact output and return no partial vector.

- **Medium impact:** authoritative books and healthy replicas now expose one
  fixed-size trading-state observation bound to instrument, immutable
  definition version, final source sequence, and accepted control revision.
  The shared constructor rejects a revision ahead of that sequence.

- **High impact:** authoritative books now expose one fixed-size account-fence
  observation bound to account, instrument, immutable definition version,
  final event sequence, effective admission state, and accepted revision.
  Retained genesis revisions, sequence-ahead revisions, and blocked accounts
  with active membership fail before a value is returned.

- **High impact:** authoritative books now expose one fixed-size exact private
  order observation bound to queried identity, instrument, immutable definition
  version, and final event sequence. Resting, dormant-stop, and state-bound
  absent results share one typed query and fallible projection path.

- **Medium impact risk:** selected-row validation does not prove complete
  price-level, account, expiry, trigger, or retained-history topology. Queue-
  position, price-level, structural, and checkpoint-lineage validation remain
  independent stronger boundaries.

- **Medium impact opportunity:** exact active-order state and state-bound
  absence can support deterministic OMS/gateway reconciliation, recovery
  comparison, private lifecycle analytics, and operational visualization
  without correlating separate order, dormant-stop, and sequence reads.

- **Medium impact risk:** the observation proves one local shard fence and its
  local active-membership consequence. Controller authentication,
  authorization, cross-instrument kill coordination, and complete history
  derivation remain external or checkpoint-lineage boundaries.

- **Medium impact opportunity:** exact account/state/revision/source-sequence
  fences support deterministic gateway admission, control propagation,
  recovery comparison, and account-scoped operational visualization without
  correlating separate state and sequence reads.

- **Medium impact risk:** provenance and the local revision bound do not prove
  controller/session authorization, remote freshness, venue-specific state-
  transition legality, or complete history derivation. The authoritative
  structural audit, checkpoint-lineage audit, and external control-plane
  evidence remain separate boundaries.

- **Medium impact opportunity:** exact state/revision/source-sequence fences
  can drive deterministic gateway admission, state-duration series, recovery
  comparison, and visualization without correlating an unversioned state read
  to a separate sequence read.

- **Medium impact risk:** a fallible stream may yield a valid prefix before a
  later non-extremum contradiction is reached. Consumers requiring all-or-
  nothing ownership must use the vector materializers or a fixed-size summary.
  Convenience methods panic on typed corruption and are not recovery APIs.

- **Medium impact boundary:** selected-row validation proves displayed
  quantity/count consistency but not the complete private FIFO or redundant
  public-index topology. Exact-key membership is available through A134;
  complete topology proof remains the explicit A45 structural audit.

- **Medium impact opportunity:** authoritative and replica full/range readers
  now have structurally parallel fallible contracts, allowing one consumer
  adapter to process live source state, repaired replicas, and deterministic
  replay without an infallible source-only branch.

- **Medium impact:** authoritative books and healthy continuous public replicas
  now expose fixed-size exact-price observations whose present or absent state
  is bound to side, price, instrument, definition version, and source sequence.
  One shared constructor validates the selected key and aggregate; the source
  additionally proves redundant public-index membership locally.

- **Medium impact risk:** repeated exact-price lookups over a dense numeric
  range cost `O(R log(P + 1))` for `R` requested prices, including absent keys.
  Existing inclusive depth-range iterators provide `O(log(P + 1) + K)` sparse
  traversal for `K` occupied in-band prices and remain the bounded interface for
  complete heatmap or ladder capture.

- **Medium impact opportunity:** state-bound absence and exact source sequence
  support deterministic ladder cells, alert conditions, visualization, and
  source/replica reconciliation without materializing complete depth. Remote
  subscriptions still require authenticated transport and freshness contracts.

- **Medium impact:** authoritative continuous books and healthy public replicas
  now expose one exact fixed-size top-N displayed-depth imbalance with common
  provenance, independently bounded bid/ask market-priority prefixes, shared
  checked per-side accumulation, checked combined quantity, and source/replica/
  checkpoint parity. Hidden-only and reserve-hidden liquidity remain excluded.

- **Medium impact risk:** the statistic is level-count bounded and quantity-
  weighted. It does not normalize by price distance, elapsed time, order age,
  venue, or executable hidden liquidity, and it makes no price-direction or
  subsequent-fill claim. A consumer requiring any such semantics needs a
  separately specified feature and data provenance.

- **Medium impact opportunity:** the exact side, numerator, denominator, level
  endpoints, and source sequence can form deterministic replayable imbalance
  series without floating-point rounding. Cross-venue aggregation or event-time
  alignment requires authenticated source identity, transport, and clock
  contracts outside this query.

- **High impact:** every continuous public-replica economic observation now
  shares the A132 poison/coherent-extrema gate. Typed full/range iterators add
  per-row validation, bulk depth returns no partial vector, and a valid current
  snapshot restores the complete observation boundary after an actual
  partially applied incremental failure.

- **Medium impact risk:** legacy infallible depth, iterator, best-side, and
  trading-state convenience methods now panic when the replica is poisoned or
  internally incoherent. Consumers handling untrusted delivery or recovery
  state must use the typed fallible methods. Streaming fallible iterators can
  emit valid earlier rows before a later non-extremum corruption is detected.

- **Low impact boundary:** source sequence, poison status, resource limits, and
  allocation telemetry remain available while economic observation is fenced
  because repair orchestration requires them. Transport freshness, source
  authentication, entitlement, and cross-venue time alignment remain external.

- **High impact:** full call-auction settlement busts now reverse every DVP and
  fee entry in canonical order, and replacement corrections append one complete
  validated settlement in the same atomic event under A119. Exact original
  grouping, one-frame durability, checkpoint recovery, capacity failure, and
  exact retry reuse the bounded ledger paths. Authorization, correction reason,
  partial allocation/trade amendments, and coordinated matching, risk,
  market-data, and external-position correction remain separate lifecycle
  inputs or state machines.

- **High impact:** explicit positive fee transfers now bind to call-auction
  trades and commit atomically with their DVP entries under A118. Multiple fees,
  third-party collectors, reverse-direction rebates, capacity failure,
  one-frame durability, recovery, and exact retry reuse the bounded batch path.
  Fee schedules, calculation, authorization, account-role mapping, tax, and
  settlement-date policy remain external lifecycle inputs.

- **Medium impact:** continuous public-depth and complete private resting-order
  extraction, call-auction aggregate limit depth, plus continuous and
  call-auction account-scoped identifier extraction now have typed fallible
  output under A117 and A62. Both authoritative books and both public replicas
  also expose allocation-free full and inclusive price-band market-priority
  aggregate iterators under A123; the call-auction book exposes direct and best
  aggregate-level lookup. A localized heatmap or surveillance window can now
  replace repeated point queries or a full-depth traversal with one logarithmic
  band descent plus linear in-band work. Bounds and canonical ordering are
  covered through hidden/market interest, account-list corruption,
  replica/authoritative parity, typed allocation failure, and large caller-
  owned results. Target-hardware narrow-band latency, allocator latency for
  materialized results, resident memory, and sustained 250,000-order snapshot
  cadence remain unknown until measured.

- **High impact:** continuous matching now exposes exact fixed-size private
  immediate-execution economics under all four STP policies. The result binds
  its request and instrument/version/event-sequence provenance, partitions
  execution, self-trade decrement, and unfilled lots, and reports signed raw-
  price notional, worst price, and exact termination under A124. Reserve and
  hidden priority share the execution preflight scanners and are covered by a
  20,000-case literal two-class queue differential.

- **High impact:** A140 now binds one exact private quote, a local acceptance
  predicate, coupled-risk authorization, and the same canonical fully
  displayed IOC commit under one exclusive shard borrow. Predicate decline or
  unwind changes no matching, risk, identity, sequence, or WAL state; exact
  replay and business rejection bypass the predicate.

- **High impact:** A141 now exposes that same exact private path as one
  market-ordered aggregate per externally executed price. It requests capacity
  for the exact row count before copying, reconciles every row to the embedded
  A124 quote, and shares the 20,000-case literal reserve/hidden/STP
  differential.

- **High impact:** A142 now binds the complete A141 curve, a borrowed local
  acceptance predicate, coupled-risk authorization, and the same canonical IOC
  commit under one exclusive shard borrow. Replay and business rejection avoid
  curve allocation; allocation failure, decline, and unwind precede all
  matching, risk, identity, sequence, and WAL mutation.

- **High impact:** A143 now applies the same atomic process-local quote/curve
  decision to every continuous `NewOrder` shape across plain, coupled-risk,
  durable, and durable-risk books. Active orders preserve their submitted TIF
  and display semantics; dormant stops are explicit and contain no activation
  forecast; market-to-limit observation and commit share one frozen private
  best.

- **High impact:** A144 now applies that observation-bound commit to continuous
  replacement across plain, coupled-risk, durable, and durable-risk books.
  Active replacement uses the target's account/side/STP with the requested
  quantity/price; dormant stop-limit replacement remains explicit; decline,
  unwind, allocation failure, and replay preserve the target and append no WAL
  frames.

- **High impact:** A145 now applies the same observation-bound commit to
  cancellation across plain, coupled-risk, durable, and durable-risk books.
  The predicate receives exact validated resting or dormant-stop state;
  decline, unwind, query failure, and replay preserve the target, its
  reservation, and WAL length.

- **Medium impact opportunity:** one cancellation predicate can gate on exact
  leaves, working quantity, price, display mode, dormant trigger state, expiry,
  and book event sequence without a separate observation borrow.

- **Medium impact risk:** a cancellation predicate executes synchronously
  while the shard is exclusively borrowed. Its latency and external blocking
  extend local cancellation latency; no callback deadline or scheduling
  isolation is provided.

- **High impact boundary:** A145 closes observation-to-cancellation validity
  only within one synchronous process-local call. It provides no remote or
  asynchronous validity, callback authentication or durability, cross-shard
  cancellation, or external cancel-on-behalf authorization.

- **High impact:** A146 closes the account-query-to-mass-cancel race inside one
  local call. The predicate receives the exact canonical selected resting and
  dormant-stop states, count, total leaves, scope, and book provenance that the
  accepted preparation removes across plain, coupled-risk, durable, and
  durable-risk books.

- **Medium impact opportunity:** one mass-cancellation predicate can gate on
  exact per-order price, leaves, working quantity, display mode, expiry,
  dormant trigger state, aggregate selected quantity, and source sequence
  without correlating a separate account-order query.

- **Medium impact risk:** conditional mass cancellation materializes `O(K)`
  private caller output and executes its predicate synchronously while the
  shard is exclusively borrowed. Allocation, validation, callback latency, and
  external blocking extend local cancellation latency; no callback deadline or
  scheduling isolation is provided.

- **High impact boundary:** A146 is one process-local synchronous account/scope
  decision. It adds no remote or asynchronous validity, callback
  authentication or durability, cross-shard firm/session kill coordination,
  delegated cancel-on-behalf authorization, or completion aggregation.

- **High impact:** A147 closes the account-fence/order-query-to-control race
  inside one local call. A block-and-cancel predicate receives the exact
  current fence, requested action, resulting blocked state, and canonical
  selected resting/dormant order set that acceptance removes. Enable binds the
  current and resulting fence without selected-order output or a lease.

- **Medium impact opportunity:** one account-control predicate can gate on the
  current revision/state and exact cancellation set, including per-order
  leaves, display, expiry, dormant trigger state, aggregate quantity, and book
  sequence, without correlating separate fence and account-order queries.

- **Medium impact risk:** conditional block-and-cancel materializes `O(K)`
  private caller output and executes its predicate synchronously while the
  shard is exclusively borrowed. Allocation, validation, callback latency, and
  external blocking extend local control latency; no callback deadline or
  scheduling isolation is provided. Conditional enable avoids that output.

- **High impact boundary:** A147 is one process-local synchronous account
  control. It adds no remote or asynchronous validity, callback authentication
  or durability, controller authorization, cross-shard firm/session kill
  coordination, delegated cancel-on-behalf authority, or completion
  aggregation.

- **High impact:** A148 closes the instrument-state/order-query-to-control race
  inside one local call. A transition-and-cancel predicate receives the exact
  current instrument state, requested target and action, resulting state, and
  canonical complete active-order set that acceptance removes. Transition-only
  binds current and resulting state without selected-order output or a lease.

- **Medium impact opportunity:** one trading-state-control predicate can gate
  on current revision/state and the exact global cancellation set, including
  per-order leaves, display, expiry, dormant trigger state, aggregate quantity,
  and book sequence, without correlating separate state and order queries.

- **Medium impact risk:** conditional transition-and-cancel materializes
  `O(O)` private caller output and executes its predicate synchronously while
  the shard is exclusively borrowed. Allocation, validation, callback latency,
  and external blocking extend local control latency; no callback deadline or
  scheduling isolation is provided. Conditional transition avoids that output.

- **High impact boundary:** A148 is one process-local synchronous instrument
  control. It adds no remote or asynchronous validity, callback authentication
  or durability, controller authorization, session/calendar scheduling,
  cross-shard coordination, or completion aggregation.

- **High impact:** A149 closes the expiry-query-to-sweep race inside one local
  call. The predicate receives the exact prior and resulting watermark plus the
  canonical complete GTD state that acceptance removes across plain, coupled-
  risk, durable, and durable-risk books.

- **Medium impact opportunity:** one expiry predicate can gate on exact
  deadlines, resting or dormant state, leaves, display, aggregate expiring
  quantity, and source sequence without correlating a separate active-order or
  expiry-index query.

- **Medium impact risk:** conditional expiry materializes `O(K)` private caller
  output and executes its predicate synchronously while the shard is
  exclusively borrowed. Allocation, expiry-index validation, callback latency,
  and external blocking extend local sweep latency; no callback deadline or
  scheduling isolation is provided.

- **High impact boundary:** A149 is one process-local synchronous expiry
  decision. It adds no remote or asynchronous validity, callback
  authentication or durability, clock or calendar scheduling, controller
  authorization, cross-shard coordination, or completion aggregation.

- **High impact:** A150 closes the stop-reference-query-to-trigger race inside
  one local call. The predicate receives the exact prior/requested sourced
  reference and canonical bounded dormant-stop prefix that acceptance activates
  across plain, coupled-risk, durable, and durable-risk books.

- **Medium impact opportunity:** one trigger predicate can gate on exact source
  coordinates, trigger priority, dormant state, leaves, activation shape,
  aggregate quantity, and remaining eligible backlog without correlating a
  separate stop or reference query.

- **Medium impact risk:** conditional stop triggering materializes `O(K)`
  private caller output and executes its predicate synchronously while the
  shard is exclusively borrowed. Allocation, trigger-index validation,
  callback latency, and external blocking extend local sweep latency; no
  callback deadline or scheduling isolation is provided.

- **High impact boundary:** A150 is one process-local synchronous reference
  decision. It adds no remote or asynchronous validity, callback or source
  authentication or durability, raw-feed normalization or gap recovery,
  controller authorization, cross-shard coordination, or completion
  aggregation.

- **Medium impact opportunity:** one predicate can gate immediate slippage,
  per-price concentration, passive resting admission, reserve/hidden residual
  policy, minimum-quantity availability, a market-to-limit captured price, or
  replacement execution without a second book borrow or a duplicated
  execution implementation.

- **Medium impact boundary:** A124/A141 report current execution economics,
  while submitted TIF remains authoritative. In particular, accepted sub-
  threshold minimum-quantity IOC cancels without trading, and dormant-stop
  observation contains no activation-time curve. Consumers must interpret the
  observation together with the original `NewOrder`.

- **Medium impact opportunity:** exact per-price predicate input permits local
  deterministic maximum-level, stepwise slippage, and liquidity-concentration
  admission without reconstructing private depth or risking a second-borrow
  state change. Fee, routing, and cross-venue inputs remain external.

- **Medium impact risk:** quote and curve predicates execute synchronously
  while the shard is exclusively borrowed. Their latency and any external
  blocking directly extend local command latency; no callback deadline,
  scheduling isolation, or persisted decision evidence is provided.

- **High impact risk:** the curve exposes executable fully hidden and reserve-
  hidden quantity by price to its process-local caller. Authentication,
  entitlement, disclosure/conflation policy, remote pagination/transport, and
  query-rate controls remain outside the order-book API.

- **High impact risk:** a standalone A124 quote or A141 curve still neither
  reserves liquidity nor fences a later borrow. A140/A142 close canonical IOC,
  A143 closes arbitrary new-order submission, A144 closes continuous
  replacement, and A145 closes continuous cancellation only within one
  synchronous process-local call. Remote or
  asynchronous validity, callback authentication/durability, fees, cross-shard
  execution, and projected cancel-resting or cancel-both maker effects require
  separately specified protocols.

- **Medium impact opportunity:** the exact quantity partition, signed
  notional, worst price, and termination can drive deterministic slippage,
  collar, minimum-quantity, and venue-routing analysis without materializing
  private depth. Cross-venue comparison still requires synchronized provenance,
  normalized units, fee/risk inputs, and independently specified queue rules.

- **High impact:** exact current-slice queue position now exposes one resting
  order's displayed/hidden class, distinct predecessors, executable lots ahead,
  complete order snapshot, and instrument/version/event-sequence provenance
  without output allocation under A125. Reserve refresh and hidden-class
  semantics are checked against every order in 20,000 generated books.

- **Medium impact:** prevalidated private price-level traversal now exposes
  complete displayed/reserve/hidden executable FIFO order in both directions
  under A127 without an output allocation or raw-link disclosure. Every level
  in the same 20,000 generated books is compared with an independent link
  model; target-hardware validation and traversal latency remain unknown.

- **High impact risk:** the price-level iterator exposes private account,
  order, total-leaves, working-slice, display, and expiry state to its local
  in-process caller. Authentication, entitlement, venue disclosure policy,
  conflation, remote pagination, and transport remain separate interfaces.

- **Medium impact:** one fixed-size continuous-book BBO now binds both public
  visible extrema to instrument/version/event-sequence provenance under A128.
  Exact raw spread covers the complete signed-price domain; the midpoint keeps
  denominator two without rounding. Empty, one-sided, hidden-only, corrupted,
  direct-checkpoint, and signed-extreme cases are covered.

- **Medium impact risk:** the BBO is one shard-local source observation, not a
  consolidated best quote. Cross-venue source identity, clock alignment,
  staleness policy, fees, sizes after queue consumption, entitlements, and
  executable reservation require separately sequenced interfaces.

- **Medium impact:** checked cumulative public-depth summaries now expose
  provenance-bound level, displayed-order, and displayed-lot totals plus
  market-priority best/worst prices for full sides and inclusive bands under
  A129. The fixed-size query allocates no output and fails on zero-valued rows
  or cumulative overflow; direct checkpoint and hidden-only cases are covered.

- **Medium impact opportunity:** cumulative summaries can drive bounded local
  liquidity, concentration, imbalance, and surveillance features without
  materializing depth. Price notional, VWAP, cross-venue consolidation, fees,
  clock alignment, and executable-liquidity modelling remain separate checked
  calculations and sequenced inputs.

- **Medium impact:** healthy continuous public replicas now expose the exact
  shared provenance-bound BBO and explicit-band cumulative summary values under
  A130. Repeated incremental, snapshot-repair, and durable-bootstrap tests
  require equality with the caught-up authoritative book; poisoned or corrupt
  replica state fails before output.

- **Low impact boundary:** a replica intentionally has no definition-wide
  summary method because its constructor owns instrument identity/version but
  not versioned price-rule endpoints. Callers requiring full-definition labels
  must supply the corresponding definition and request that explicit band.

- **Medium impact:** authoritative books and healthy replicas now expose one
  exact fixed-size displayed-liquidity sweep quote under A131. Market and limit
  requests return provenance, quoted/unquoted lots, signed raw notional, worst
  price, contributing price count, and exact termination without materializing
  depth. Incremental, snapshot-repair, and durable-bootstrap tests require
  source/replica parity.

- **Medium impact risk:** displayed-liquidity quotes intentionally omit fully
  hidden orders and future reserve refreshes and reserve no public quantity.
  They therefore cannot substitute for A124 private execution economics or an
  atomic quote-to-command protocol; later commands, STP, risk, and fees remain
  outside the result.

- **Medium impact opportunity:** the exact public quantity/notional fraction
  and worst price can drive local slippage, market-impact, routing, and depth-
  exhaustion features without caller-owned vectors. Cross-venue analysis still
  requires synchronized source identity, normalized units, fees, entitlements,
  and explicit staleness policy.

- **Medium impact risk:** queue position is a state observation, not a fill-
  probability or latency estimate. Subsequent execution, cancellation,
  replacement, reserve refresh, and other committed commands can change it;
  unknown aggressor identity also prevents STP forecasting. Remote validity,
  event-driven position updates, and venue-calibrated fill models require
  separately sequenced inputs and explicit model assumptions.

- **Medium impact opportunity:** provenance-bound position changes can support
  deterministic queue-depletion, maker-performance, cancellation-response, and
  fill-model feature series without exporting private FIFO links. Statistical
  inference still requires synchronized market events and independently
  validated venue rules.

- **Medium impact:** continuous and call-auction live command/report history
  now supports expected constant-time exact lookup and zero-copy chronological
  iteration over the bounded idempotency cache under A120. Accepted and
  rejected outcomes remain queryable after WAL/checkpoint recovery without a
  checkpoint allocation. Target-hardware cache behavior for full-history scans
  remains unknown until measured.

- **Medium impact:** ledger event history now supports zero-copy, fail-closed
  one-based lookup and chronological/reverse iteration under A121. Exact entry,
  correction, and batch grouping and event-declared transaction order survive
  direct checkpoint and checkpoint-plus-suffix recovery. Target-hardware cache
  behavior for maximum-history scans remains unknown until measured.

- **Medium impact:** exact point-in-time ledger balances now reconstruct one
  account/asset value at any completed record boundary under A122 without
  output or auxiliary allocation. Atomic correction/batch grouping and
  fail-closed history validation survive direct, full-WAL, and checkpoint-
  prefix/WAL-suffix recovery. Target-hardware latency and cache behavior for
  maximum-history scans remain unknown until measured.

- **Medium impact:** account-and-asset statement filtering now streams borrowed
  canonical postings in both directions under A126 without a secondary index
  or output allocation. Stable record sequence, transaction position, and
  complete atomic grouping remain attached to every line. Maximum-history
  scan latency and sparse-key selectivity on target hardware remain unknown
  until measured.

- **High impact risk:** the local history views expose complete private
  command/report and ledger-event content to their in-process caller. A126
  provides local account-and-asset filtering but no authentication or
  authorization. Entitlement, remote pagination and transport, audit export,
  eviction, and fenced generation rollover remain separate interfaces or
  lifecycle protocols.

- **High impact:** continuous FOK and minimum-quantity IOC now have atomic
  behavior under all four STP policies. FOK decrement-and-cancel requires the
  full original quantity before the first priority-reachable self barrier;
  minimum-quantity decrement-and-cancel uses the separate exact two-counter
  reserve-queue simulation in A115. Both are covered through reserve/hidden
  priority, dormant activation, coupled risk, market data, WAL/snapshot
  recovery, and exact retry. Authenticated venue beneficial-owner mapping and
  protocol conformance remain external.

- **High impact:** source identity/version/sequence now accompanies every
  continuous stop reference through matching, risk, private publication state,
  checkpoints, and WAL recovery. This detects shard-local gaps, regressions,
  reset discontinuities, source changes, and cursor/content conflicts.
  Authenticated raw-feed acquisition, per-shard sequence projection,
  retransmission/gap repair, failover fencing, and conformance to a selected
  venue trade-reference policy remain external production increments.
- **High impact:** call-auction command/report codecs, deterministic full-WAL
  recovery, semantic kind-`4` checkpoints, exact uncut prefix proof, and
  single/segmented A/B anchor cutover are implemented, including exact retry
  suppression and one dangling-command completion. Cutover bounds physical WAL
  scan and checkpointed command re-execution. Complete semantic history remains
  inside the checkpoint, so snapshot bytes, capture pause, validation work,
  idempotency lifetime, and supervisory shard-generation rollover remain
  unbounded by the cutover protocol.
- **High impact:** every private call-auction trade now carries immutable
  instrument identity/version, and one complete accepted uncross report maps to
  one DVP ledger entry or atomic batch with one global transaction ID per trade.
  Explicit positive trade-bound fee transfers can join that same atomic event
  with independent transaction identity. Entry construction, exact retry,
  collision/partial-commit detection, durable one-frame recovery, and ledger
  checkpoint cutover reuse the existing bounded ledger paths. Clearing
  authorization, novation/allocation accounts, fee calculation/authorization,
  settlement dates, custody, external money settlement, and legal finality
  remain separate lifecycle boundaries.
- **High impact:** call-auction collection now supports atomic new-identity
  replacement with full priority loss, saturated active/price-level capacity
  reuse subject to fresh accepted-ID headroom, risk reservation substitution,
  WAL/snapshot recovery, exact retry, and one two-update public batch.
  It also supports strict retained-priority quantity reduction with exact
  aggregate/risk release and one anonymous public delta. Venue-specific price/
  side amendment, quantity increase, and protocol-adapter semantics remain
  outside this state machine and require an explicit versioned policy.
- **High impact:** call-auction uncross now selects explicit price-time or
  price/class-tier pro-rata-time allocation. Pro-rata shares use exact
  instrument-increment floors, FIFO residual quanta, deterministic trade
  pairing, and WAL-v20/snapshot-v20 recovery. Every live order carries one
  authoritative typed priority-class scalar used by both policies after
  market/price ordering and before time. An authenticated venue-category-to-
  scalar mapping remains adapter conformance work. Venue algorithms that rank by
  size, weight by time, reserve top-order or minimum shares, split FIFO and
  pro-rata percentages, or distinguish display categories remain separate
  conformance work rather than aliases of `ProRataTime`.
- **High impact:** call-auction self-trade policy now supports fail-closed
  `Abort` on the first canonical equal-`AccountId` pair. The complete uncross
  is rejected before book, trade-ID, phase, risk, or public-depth mutation and
  recovers exactly through WAL/snapshot version 20. Authenticated beneficial-
  owner mapping, alternative-counterparty rearrangement, and venue-specific
  cancel/decrement or aggressor/resting instructions remain external.
- **High impact:** call-auction collection and sequencing now support bounded
  account/side mass cancellation through an intrusive owner index. One accepted
  command selects `K` orders independently of unrelated active interest, emits
  canonical ascending removals plus one aggregate completion, increments the
  book revision exactly once when `K > 0`, releases each risk reservation, and
  recovers under WAL/snapshot version 20. Public payload version 5 projects the
  same complete batch without account or scope identity. Authenticated firm,
  session, or cross-shard scope and completion aggregation remain external.
- **High impact risk:** the auction writer lease is local ownership, not a
  replicated fencing token. Active/passive failover or multi-process session
  authority can admit split-brain writers unless an external sequencer provides
  monotonic epochs and proves command ownership across restart.
- **Medium impact:** stable auction command/report traces now project to
  versioned anonymized phase, aggregate-depth, trade, final-clearing, and
  snapshot payloads without account/order/command identity. A bounded local
  replay ring now preserves complete command batches for short-gap repair.
  Sequenced nullable indicative publication now carries an explicit reference,
  band, ranking policy, auction, phase revision, and book revision through live
  publication, replay, snapshots, WAL, and checkpoint recovery. Authoritative
  input derivation and venue disclosure cadence/filtering remain external.
- **High impact risk:** the auction payload layer has no authenticated framing,
  entitlement, multicast/fanout, remote retransmission session, bandwidth
  control, or externally qualified conformance fixtures. Its local replay ring
  and replica prove semantic continuity, not remote sender identity or delivery
  availability.
- **High impact:** local command/event recovery, canonical-path writer exclusion,
  directory-entry synchronization, explicit abandoned-owner recovery, and
  deterministic storage-fault injection, automatic segment rotation, and
  cross-segment durable replay are implemented. Marker-selected generation
  cutover now fences local deletion of retired segment prefixes; external
  archival/handoff, hard-link/inode fencing, storage power-loss qualification,
  and replication remain prerequisites for broader production recovery claims.
  Fenced A/B matching/risk/ledger cutover bounds the physical WAL prefix and
  subsequent WAL scan in both layouts; the selected checkpoint still retains
  and validates complete semantic history.
- **High impact:** direct and WAL-synchronized continuous matching, continuous
  coupled-risk, call-auction, and coupled call-auction/risk capture can
  hand an immutable, nonencodable candidate to another thread and defer
  deterministic history replay until its consuming verification transition.
  Writer-side canonical row copying, structural/lineage projection, and both
  coupled direct reconstructions remain history-dependent and require measured
  pause bounds. Durable publication is fenced to the exact shard incarnation
  and pre-cutover epoch. A101 now permits verified older checkpoints to retire
  their prefix while streaming only the post-capture suffix from its private
  physical cursor; full semantic replay is not repeated under writer exclusion.
  Suffix copy time remains proportional to bytes appended during verification,
  and checkpoint capture/projection pauses remain history-dependent.
- **High impact:** entry-before-balance recovery, independent trial/replay audit,
  canonical checksummed checkpoints, exact WAL-prefix proof, suffix application,
  once-only exact reversal/reinstatement lineage, and generation-bound complete
  external balance comparison, dated financial entries, monotonic booking time,
  durable close/reopen fences, and single-frame atomic reversal-plus-replacement
  corrections plus generalized ordered multi-entry batches are implemented.
  A119 additionally composes those primitives into complete call-auction
  DVP/fee busts and replacement corrections with exact original-group proof.
  Batch final-balance netting, in-batch period/reversal sequencing, exact grouped
  replay, torn-tail repair, segmented rotation, and checkpoint suffix recovery
  are covered, as is anchored prefix retirement in both physical layouts. Controller
  authorization, versioned calendar ingestion, durable external evidence,
  clearing workflow adapters, external retired-generation archival, and
  checkpoint-memory-bounded restart remain.
- **High impact:** immutable versioned tick, lot, asset, trading-state,
  settlement, bounded native reserve, and fully hidden admission rules are
  implemented. Immutable versioned UTC trading-calendar images, canonical
  session lookup, day/session-to-GTD normalization, expiry-sweep construction,
  and stable calendar payload bytes are also implemented; authoritative source
  ingestion, signed distribution, atomic activation, venue-certified
  display/queue adapters, corporate actions, and derivative lifecycle rules
  remain outside the boundary.
- **High impact:** the dated ledger codec is deployment-safe only under A35.
  Any authoritative undated predecessor requires an explicit version boundary
  and migration; the implementation intentionally does not synthesize missing
  effective dates or booking timestamps.
- **High impact:** deterministic single-instrument order risk, reservations,
  profile-bound durability, coupled semantic checkpoints, exact WAL-prefix
  proof for uncut storage, anchor-bound cutover in both layouts, suffix-only state
  transitions, and cross-audit are implemented;
  ledger-backed available funds, cross-instrument portfolio netting,
  margin/scenario models, busts, transfers, and replicated ownership remain
  outside the boundary.
- **High impact:** finite matching cardinalities and a protected
  cancellation/expiry history lane are implemented, but no fenced generation
  rollover, durable idempotency watermark, or history eviction exists.
  Ordinary admission stops before the reserve; once total history or
  accepted-identity capacity is exhausted, supervisory cutover is required.
  Checkpoints preserve complete exact-retry lineage and do not conceal this
  lifecycle boundary.
- **High impact:** matching includes one continuous single-instrument
  price-time-priority book with market/limit,
  GTC/GTD/IOC/minimum-quantity-IOC/FOK/post-only, native reserve, fully hidden
  displayed-priority queue classes,
  stop-market/stop-limit, replace/cancel, canonical explicit expiry and
  stop-reference sweeps, mass cancellation, four STP policies, and revisioned
  open/cancel-only/halted/closed controls. A
  separate bounded call-auction book
  now collects crossed market/limit interest, replaces owned orders atomically
  under a fresh identity and priority, mass-cancels account/side interest in
  canonical order, feeds statically banded aggregate discovery, produces an
  explicitly selected price-time or price/class-tier pro-rata-time order-level
  allocation plan, and consumes it through deterministic process-local pairing
  and one atomic book commit. A bounded sequenced
  controller now adds explicit collection/freeze/close phases, exact revision/
  auction identity, sequenced business outcomes, idempotency, and protected
  terminal history. Stable auction wire schemas,
  semantic checkpoints, exact prefix proof, full-WAL plus cutover
  single/segmented recovery, full and inclusive-band aggregate-depth queries,
  and anonymized
  phase/trade/final-clearing publication with retained complete-batch replay
  and gap-repair snapshots, including sequenced nullable indication, are
  implemented. Reference/dynamic-band construction and venue-specific
  indication cadence/filtering,
  auction display and venue-specific size-ranked, time-weighted, minimum-share,
  or hybrid allocation policies, venue-specific self-trade cancellation,
  decrement, and alternative-pairing policies, clearing-lifecycle authorization,
  fee calculation/authorization, allocation, and settlement-date derivation,
  authenticated public/private
  transport, market-on-auction and imbalance-only order types,
  authoritative external continuous stop-reference ingestion, pegged triggers,
  discretionary ranges,
  authoritative calendar distribution/activation, ingress-provenance
  durability, sequenced session-state transitions,
  volatility-trigger logic and interruption auctions,
  venue-specific in-place amendment priority, and atomic multi-leg/cross-instrument
  execution require explicit sequenced state machines, new wire versions,
  differential venue fixtures, and crash/replay proofs; no existing enum
  silently approximates those semantics.
- **High impact risk:** dormant-stop determinism proves only the behavior after
  a reference command is sequenced. Feed selection, matching response to trade
  corrections/busts, reference-source authentication, missed-reference
  recovery, and cross-shard ordering are external. A stale but structurally valid reference
  can deterministically produce a state different from the intended venue
  state without violating the local matching grammar.
- **Medium impact opportunity:** the explicit reference, trigger-priority, and
  remaining-backlog trace permits exact activation-latency and counterfactual
  trigger analysis without exposing dormant identity in public depth. Joining
  it to an authoritative reference feed requires a versioned provenance key.
- **High impact:** the instrument catalog, continuous matching, continuous risk, continuous and
  call-auction market-data publisher/replica state and scratch, call-auction risk, uncross-netting
  scratch, auction retry/event history, and ledger balance/transaction/reversal state
  now use constructor-owned fixed-capacity dense/open-addressed indexes and/or
  stable-slot AVL arenas. Continuous replicas reserve active and standby depth;
  ledger journal order is reserved to its complete bound; zero-balance removal
  supports bounded balance-identity churn. Backward-shift deletion and AVL free
  slots support arbitrary bounded different-identity churn without table/tree
  growth. The catalog additionally reserves one flat immutable-definition arena;
  interleaved appends shift definitions and rebase bounded ranges without growth.
  Successful continuous-book/risk, call-auction-book/engine/risk, and shared
  indexed-AVL audits additionally use no transient collections. Continuous,
  call-auction/risk, and ledger checkpoint capture is fallible under
  A78/A88/A89. Remaining allocation boundaries include prepared `Arc` control
  blocks, caller-owned input/output and decoded checkpoint objects,
  snapshot-file payloads, ledger diagnostic/reconciliation collections,
  failure-detail formatting, and wide ledger magnitudes under
  A12/A43/A67/A70/A71/A72/A73/A74/A75/A76/A77/A78/A79/A80/A81/A82/A83/A89.
- **Medium impact:** atomic ledger-batch preparation is independent of unrelated
  ledger balances but retains one flat signed delta term per posting, plus
  per-entry lifecycle/idempotency overlays, until allocation-free commit. All
  buffers reserve fallibly before mutation. Default ledger and WAL bounds cap
  one durable batch; allocator counts and tail latency for maximum
  fee/allocation/settlement bundles remain to be measured on target hardware.
- **Medium impact:** exact ledger side totals remain allocation-free through
  `u128::MAX` and then retain canonical `u64` limbs. Addition is amortized
  constant time with carry-propagation worst case proportional to limb count;
  decimal diagnostic formatting operates on a copy and is superlinear in limb
  count. Production-volume wide-total allocation and formatting benchmarks are
  unknown.
- **Medium impact:** continuous account mass cancellation and block-and-cancel,
  plus call-auction account mass cancellation, each traverse exactly `K`
  intrusive per-account/per-side members and sort unique IDs in place in
  `O(K log K)`, independently of `O` unrelated active orders. Continuous
  unlinking is `O(K log P)`; call-auction unlinking is
  `O(K(log O + log P))` for `P` price levels. Each index adds two links per
  active order and fixed per-account side state within `O(O)` memory. Ordinary
  account membership rest/removal is `O(1)` and performs no separate tree-node
  allocation. Pinned-hardware tail-latency and cache-footprint measurements
  remain required.
- **Medium impact:** instrument transition-and-cancel visits all `O` active
  orders, leases constructor-owned capacity for `O` identifiers before persistence, sorts them in
  `O(O log O)`, and unlinks them in `O(O log P)`. Unlike account controls, it
  does not require a risk profile. Controller identity, authorization,
  calendar/session identity, reason codes, and cross-shard completion evidence
  are unknown external inputs; adding any of them changes command and audit
  semantics.
- **High impact risk:** continuous GTD is driven only by an explicit sequenced
  inclusive watermark. Clock selection, controller authentication, sweep
  cadence, delayed-command policy, calendar generation activation, original-
  request audit persistence, and multi-shard synchronization are external.
  A104 supplies deterministic mapping once one immutable UTC generation is
  selected; an unsequenced local timer or inferred session close would still
  invalidate deterministic replay.
- **Medium impact:** expiring `K` orders traverses the ordered expiry prefix and
  then performs `K` price/expiry AVL removals, emitting `K + 1` events. The
  asymptotic bound is deterministic; pinned-hardware p50/p99.9 latency, burst
  cadence, and cache-residency effects at `K = O_max` remain unknown.
- **Medium impact:** execution and public best-level caches plus A83 key-checked
  stable-slot handles make execution-price/FIFO discovery, public-best lookup,
  partial maker mutation, non-empty-level removal, and reserve displayed-class-
  tail refresh allocation-free `O(1)` without an ordered price search. They add
  two execution-price handles/copies and two public-best copies per book; all
  authoritative-map mutations synchronize public membership and both cache
  classes. Empty-
  level deletion, residual insertion, and next-worse traversal remain
  `O(log(P + 1))`. Pinned-hardware branch/cache-miss and p50/p99.9 latency
  deltas are unknown.
- **Medium impact:** future-event work adds one `u128` aggregate to every
  occupied level and side plus two `u128` aggregates to every active-account
  index. Rest, removal, refresh, and replacement update them in `O(1)` around
  existing tree/hash operations; full audit recomputes them in `O(O + P)`.
  Their cache-footprint and latency effect at production `O`, `P`, and account
  cardinalities are unknown. The report-capacity bound uses all opposite-side
  prices and can reject a narrow nonmarketable limit order early near sequence,
  per-report, or total-event exhaustion; it no longer retains that conservative
  slack as per-report vector capacity.
- **Medium impact:** FOK and minimum-quantity IOC preflight allocate no queue.
  FOK and non-decrement minimum scans visit each crossed order at most once in
  `O(O_c + P_c log(P + 1))` time. A115 decrement-and-cancel minimum scanning
  adds `sum_p D_p log(R_p + 1)` aggregate work for exact reserve rounds while
  retaining `O(1)` auxiliary space; `R_p` is `u32`-bounded. Independent
  20,000-case deterministic differential tests compare both FOK and minimum-
  quantity behavior with literal slice/requeue models. Maximum-book scan
  latency and interaction with CPU cache residency are unknown until measured
  on declared production capacities and hardware.
- **Medium impact:** all four execution/public price AVL arenas, the GTD-expiry
  AVL, and both stop-trigger AVL arenas are always fallibly reserved during
  book construction. Price reservation is
  `2 P_max S_level + 2 P_max S_public` and expiry reservation is
  `O_max × S_expiry` bytes; stop reservation is
  `2 × O_max × S_stop` bytes before allocator rounding, where `S_price`,
  `S_level`, `S_public`, `S_expiry`, and `S_stop` are the target ABI slot sizes;
  exact ABI-dependent slot sizes and resident-page behavior require target
  measurement. All five matching fixed-capacity hashes and the coupled-risk
  profile/reservation maps also reserve their
  complete dense maxima and initialize lookup arrays at load at most 0.5 during
  construction. Exact ABI byte footprint, seed-dependent probe distribution,
  and page residency remain target-dependent and require measurement. Matching and risk
  preparation now use immutable shard borrows and cannot change hash capacity.
  Continuous matching and the sequenced call-auction engine now reserve safe
  `OnceLock` event arenas through their independent `max_retained_events`
  limits at construction. Live reports retain exact adjacent
  ranges, so conservative command bounds can reject near a boundary but do not
  retain per-report spare capacity. Non-empty mass cancellation,
  expiry sweep, stop-trigger sweep, block-and-cancel, and instrument transition-and-cancel
  preparations lease one isolated constructor-owned identifier-selection vector
  under A87; empty selections consume no lease. Execution, finalization, cache
  insertion, retry, and checkpoint clone
  do not allocate or copy live events. The fixed arena footprint is
  `max_retained_events × size_of::<OnceLock<Event>>()` before allocator rounding;
  the current `aarch64-apple-darwin` layouts are
  `262,144 × 144 B = 37,748,736 B` (`37.748736 MB`) for continuous matching and
  `73,730 × 192 B = 14,156,160 B` (`14.156160 MB`) for call auction, excluding
  vector/Arc/allocator overhead. Other target ABI sizes, resident-page behavior, cache locality
  relative to contiguous per-report vectors, and constructor first-touch cost
  require measurement. One
  externally retained live trace keeps the complete arena `Arc` alive after the
  book is dropped. Call-auction uncross preparations/results instead lease one
  of `P` constructor-owned buffer sets under A86; consumer retention can
  exhaust that pool but does not allocate result vectors. Decoded/caller-built
  traces, diagnostic copy-on-write, decoded/caller-owned checkpoint buffers, and generic
  caller-owned auction allocation plans remain allocated under A12. End-to-end
  allocation-failure continuation remains incomplete.
- **High impact:** price-arena, complete matching/profile/reservation/ledger
  hash construction, fixed ledger-journal reservation, durable profile
  canonicalization, continuous/call-auction/risk/ledger checkpoint capture, and
  matching/ledger preparation-buffer exhaustion are typed
  failures before a shard, state transition, or WAL command exists. Allocation
  failure for `Arc` control blocks, decoded checkpoint/snapshot ownership, diagnostic
  audits, wide magnitudes, and other process memory remains non-recoverable under
  A12/A43. Codec and WAL framing reservations are typed under A80/A81/A82, but
  end-to-end allocation-failure continuation is not established.
- **High impact:** the numerical profile set is immutable after first command
  sequencing. A revisioned per-instrument admission fence with atomic local
  block-and-cancel and re-enable is implemented. Intraday numerical-limit and
  position amendments, risk-state clearing corrections, account onboarding, controller
  authentication/authorization, and cross-instrument atomic kill coordination
  require additional sequenced, versioned administrative events or protocols;
  they are not represented by registration or the local fence.
- **Medium impact:** the indexed AVL replaces allocator-owned tree nodes with
  contiguous stable slots and guarantees `O(log(P + 1))` ordered structural
  mutation without heap work. Rotations and two-child deletion preserve every
  surviving slot; an internal expected-key handle provides `O(1)` direct value
  access until its own key is removed. Handles are not persistent identities.
  A double-ended iterator carries two fixed 128-index stacks: `256 × 8 B =
  2,048 B` on 64-bit targets. Target-specific node size, cache residency,
  rotation cost, page-fault behavior after virtual reservation, and p50/p99.9
  latency relative to representative price distributions remain unknown.
- **Medium impact:** final active-order, active-account, and same-side price-level
  capacity is exact for GTC/GTD/post-only new-order admission and
  price-changing GTC/GTD replacement. A replacement at a full unchanged old
  level scans crossed
  opposite liquidity only when the absent target level would otherwise exceed
  the bound; full fill or STP termination consumes no target level. At a full
  new-account bound, a residual requires an allocation-free proof over as many
  as all `O` active
  account memberships after the crossed-level scan. This is boundary-only but
  its production tail latency remains unmeasured. IOC, FOK, and market orders
  bypass resting-capacity gates.
- **Low impact:** prepared matching tokens are deliberately process-local and
  cannot cross book instances or restart. Durable recovery reads the persisted
  command and constructs a new preparation against recovered state; serializing
  or transporting the token itself is outside the contract.
- **Medium impact:** formal state-machine/model-based tests are required for the
  combinatorial interaction of TIF, replacement, and self-trade policies.
- **Medium impact:** local continuous and call-auction level-2 incrementals,
  full-depth images, stable payloads, gap detection, constructor-reserved
  per-instrument suffix replay, typed eviction/collision/batch-boundary
  handling, and snapshot fallback are implemented. Authenticated network
  framing, entitlements, fanout, remote retransmission sessions, and bandwidth
  controls remain outside the boundary.
- **Medium impact risk:** public refresh updates necessarily reveal that hidden
  leaves survived the preceding displayed-slice depletion; the final partial
  slice can also bound prior hidden quantity. Quantifying this information
  leakage and any venue-specific feed obfuscation is outside the current local
  publisher contract.
- **Medium impact risk:** a fully hidden-maker trade reveals executable
  liquidity at its price even though the preceding public depth was empty.
  Quantifying information leakage or applying venue-specific delayed,
  conflated, or suppressed publication requires a different versioned feed
  policy.
- **High impact risk:** call-auction risk profiles are immutable within one
  durable lineage. Profile revision, external position synchronization,
  clearing transfers, and matching/risk application of busts or corrections
  require a separately sequenced durable event model; A119 changes only ledger
  state. Mutating risk inputs out of band would invalidate deterministic risk
  replay. The plain durable auction runtime intentionally rejects profile-
  prefixed risk journals.
- **High impact risk:** auction and continuous risk are per-account,
  per-instrument raw-price-times-lots controls. Cross-instrument portfolio
  offsets, collateral, margin, currency conversion, fees, option Greeks,
  clearing transfers, matching/risk bust or correction application, and
  external position synchronization remain unrepresented.
- **Medium impact opportunity:** conservative auction reservations and netted
  uncross deltas form deterministic utilization/position time series that can
  be joined to anonymized clearing/depth projections without making public data
  an authoritative risk source.
- **Medium impact:** current asymptotic complexity is established by data
  structures; throughput and tail-latency claims remain unknown until measured
  on declared hardware and workloads.
- **Medium impact opportunity:** definition records make historical re-simulation
  self-describing at the shard level; deterministic cross-version market-quality
  and rule-change analyses can be built without inferring tick or multiplier
  regimes from timestamps alone.
- **Medium impact opportunity:** sequenced risk rejections and reconstructed
  reservations provide deterministic inputs for limit-utilization analytics,
  surveillance, and counterfactual replay under alternative profiles.
- **Medium impact opportunity:** one-to-one event/public sequences permit exact
  latency attribution and deterministic public/private book reconciliation
  without heuristic correlation identifiers.
- **Medium impact opportunity:** continuous matching and call-auction sequencing
  now convert prepared report bounds into deterministic total-event
  backpressure without retaining per-report slack. Their conservative command
  bounds can still reject near a history boundary. Crossed-range aggregates or
  bounded pre-execution scans could reduce this rejection, but their latency,
  cache, and proof trade-offs are unmeasured.
- **Medium impact opportunity:** strictly verified immutable closed segments are
  natural audit-export and replication units. Exploiting this requires a fenced
  handoff protocol because current directory ownership rejects external
  archival or deletion mutations.
- **Medium impact opportunity:** redundant checkpoint balances plus retained
  journal history provide deterministic internal reconciliation evidence and
  generation-addressed audit images. External statement/custodian anchors and
  persistent signed evidence are still required for cross-system reconciliation.
- **Medium impact opportunity:** the generalized ledger-batch primitive now
  carries explicit trade-bound call-auction fees without exposing partial
  economic state. Allocation, multi-leg settlement, and clearing bundles can
  reuse the same primitive, but product-specific construction, authorization,
  and external lifecycle evidence remain separate adapters.
- **Low impact:** UI and visualization layers should consume immutable versioned
  traces and snapshots; they are not authoritative state owners.
