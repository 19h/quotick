# Assumption Register

Each dependent result is valid only while its tagged assumptions survive the
listed falsification probe.

The register holds one section per assumption. Each section states what is
assumed (**Assumption**), which results depend on it (**Dependent results**),
and the stress test that would refute it (**Falsification probe**). The
identifiers A1-A111 are stable and are referenced from code comments and other
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

**Assumption.** Hash iteration order is never externally observable; all
exposed ordered data comes from price trees, FIFO links, or journal vectors.

**Dependent results.** Deterministic public outputs across process seeds.

**Falsification probe.** Replay identical command streams under varied hash
seeds and byte-compare reports, depth, and journal order. Any difference
falsifies A10.

## A11 — once-only trade settlement

**Assumption.** A trade is durably settled once using a caller-supplied
globally unique transaction ID and the definition-correlated settlement path;
the lower-level convention API is not an authorization boundary.

**Dependent results.** Delivery-versus-payment balances, WAL reconstruction,
and retry behavior.

**Falsification probe.** Submit exact retries, transaction collisions,
mismatched instrument versions, and terminate between WAL append and balance
commit. Any mismatched-version posting, duplicate economic effect, or lost
acknowledged entry falsifies A11.

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
codec collections/output, and WAL frame/batch/read buffers reserve fallibly
with typed resource identity. Arc control blocks, caller-owned command/entry
objects, decoded/caller-built event traces and checkpoints, snapshot-file
ownership, path/string construction, ledger diagnostic/reconciliation
collections, failure-detail formatting, caller-owned or cloned generic auction
allocation plans, and wide ledger magnitudes can still allocate or abort.

Continuous and call-auction market-data batch/snapshot/depth output has
explicit fallible APIs, while convenience wrappers can still panic on
allocation failure.

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

**Assumption.** WAL format version 14, snapshot format version 14, continuous
market-data payload version 3, call-auction market-data payload version 4, and
trading-calendar payload version 1 are immutable. WAL and snapshot versions
`1` through `13` are expired and rejected explicitly rather than inferred or
migrated. WAL v14 preserves v13 values and appends one little-endian `u16`
priority-class scalar to call-auction order commands and event snapshots.
Snapshot v14 appends the same scalar to direct active-order rows and embeds
WAL-v14 values in chronological histories. WAL v13 added one explicit
allocation-policy byte to call-auction uncross commands and completion events.
WAL v12 added call-auction command/action tag `6` for retained-priority
amendment, rejection tag `22` for a non-reduction, and event-kind tag `8` for
`OrderAmended`; those values remain unchanged in v14.
Continuous market-data v3 preserves v2 bytes but adds the absent-public-maker
trade interpretation required for fully hidden execution. Call-auction market-
data v4 preserves v3 layouts and adds book-reason tag `5`, `Amended`.
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

**Falsification probe.** Byte-compare golden WAL-v14, snapshot-v14,
market-data-v3, auction-market-data-v4, and trading-calendar-v1 fixtures through
every supported release; mutate valid WAL frames and images to versions `1`
through `13`; verify definition booleans, every display, TIF, rejection, and
cancellation tag, continuous expiry/stop/source tags, raw auction record tags
`9`/`10`, replacement, mass-cancel, amendment, and allocation-policy tags,
priority-class scalars, snapshot kinds `1` through `5`, hidden-maker trade
application, auction
replacement and mass-cancel projection, and every calendar scalar/row offset.
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
remain explicit variants inside deployable WAL version 14. Version 9 added
durable stop-reference source identity, source version, source sequence, and
typed discontinuity/collision outcomes; it does not infer them from WAL version
8. No earlier matching WAL requires runtime compatibility. Expired envelopes
fail before payload interpretation rather than receiving inferred display,
mass-cancel, control, expiry, stop, reference-source, trigger-priority, hidden,
minimum-quantity, or anchor semantics.

**Dependent results.** Explicit fully displayed/reserve/fully hidden/GTD/stop
state, refresh, expiry, and trigger events, canonical mass cancellation,
revisioned account fencing, and anchored cutover under WAL envelope version
`14`.

**Falsification probe.** Inventory every persisted matching WAL and decode
golden version-12 fixtures before deployment. Reject version-11 artifacts with
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
order but not total fillable external leaves at a price unless a
cancel-aggressor/cancel-both self order is encountered. A self order in the
displayed class is a barrier after only the current slices preceding it because
refresh rejoins at that class's tail. A self order in the hidden class is a
barrier after all total leaves in the preceding displayed class, plus earlier
hidden leaves, because refresh never crosses into the hidden class. Cancel-
resting removes self orders and leaves every external total leaf eligible.
Decrement-and-cancel remains inadmissible for FOK.

**Dependent results.** Allocation-free FOK preflight with `O(1)` auxiliary
space and one visit per active order in crossed levels; exact hidden-liquidity
and STP behavior without materialized slice queues.

**Falsification probe.** Differentially compare against an independent literal
slice/requeue simulation across generated levels, FIFO ownership patterns,
partial reserve slices, fully hidden FIFO, quantities, prices, and every
supported FOK STP mode; retain explicit displayed- and hidden-class same-price
barriers, better-price hidden exhaustion,
cancel-resting, insufficient-liquidity, and execution-trace fixtures. Any
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
rebuild under different insertion orders. The structural auditor validates
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
initialized length; forward/reverse iteration; replacement;
topology-independent equality; unrepresentable constructor reservation; direct
height/order/root-reachable and disconnected
cycle/shared-child/reachability/free-list corruption; and at least 20,000
deterministic insert/remove operations differentially against `BTreeMap`,
validating after every mutation. Any moved surviving key/value, model
divergence, balance/order violation, unreachable/duplicate slot,
post-construction arena growth, noncanonical traversal, topology-dependent
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
independent of unrelated active orders. No continuous-book state is mutated.

**Falsification probe.** Exercise routing/version and every instrument
boundary; locked/crossed/market-only interest; middle/head/tail cancellation
and owner mismatch; per-side level, active, and accepted-ID exhaustion; ID
reuse; saturated same-level and singleton-level replacement; account mismatch;
invalid replacement route/quantity; priority loss; atomic rejection; strict
amendment reduction, immutable fields, retained priority, and aggregate delta;
empty/all/side mass cancellation, sparse owners in a full book, output-capacity
and revision exhaustion, account-link corruption, canonical output, one-
revision commit, and fixed allocation telemetry;
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
`CancelMarket`, or `CancelAll`), and the only represented self-trade policy
(`Permit`). The selected fills are authoritative. Pairing walks both fill
vectors in canonical order; a same-account pair is a trade, not prevention.
Book-local trade identity and one
collection revision advance only at commit. Positive residual leaves remain lot
aligned and at or below the entry maximum but may be below the new-order
minimum.

**Dependent results.** [A63, A86] Preparation is nonmutating and move-only: it
acquires one isolated constructor-owned buffer set, clears it without changing
capacity, and writes both fills, deterministic pairs, and remainder
cancellations in place. Foreign/stale commit fails before mutation and dropping
either the preparation or result returns its lease. Same-revision commit
reduces/removes every fill, applies remainder policy, consumes contiguous
`TradeId` values, and advances revision as one allocation-free transition. For
`T` pairs, `C` cancellations, and `M` affected orders, preparation is `O(O log
O + P + F_b + F_a + T)` time with `O(1)` hot-path auxiliary allocation and
commit is `O(M(log O + log P))`; the bounded result storage is charged once to
A86 construction.

The book primitive is not itself a sequenced command, durable event,
risk/ledger transition, market-data trace, or preventive STP implementation;
A64 sequences it and A65 persists the resulting command/report trace.

**Falsification probe.** Exercise full/partial fills, all three remainder
policies, retained FIFO across auctions, same-account pairs, head/middle/tail
removal, foreign/stale preparations, exact trade/revision progression,
quantities below entry minimum, constructor reservation failure, and lease
exhaustion/release. Differentially compare at least 10,000 generated pairings
and post-state images with literal two-pointer and remainder models, validating
the book after every commit. Any volume mismatch, noncontiguous trade ID,
priority inversion, unintended self-trade prevention, wrong remainder, partial
mutation on a rejected preparation, commit allocation, capacity movement,
cross-lease contamination, lost lease, or post-state/audit divergence falsifies
A63.

Venue preventive STP, alternative pairing, allocation adjustments,
bust/correction, sequencing, or durability semantics beyond A65 require
separate versioned semantics.

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
and explicit close retains interest.

**Dependent results.** [A41, A52, A64, A85, A86] Accepted commands and business rejections
receive contiguous command/event sequences; operational capacity, allocation,
stale/foreign preparation, and counter failures are unsequenced. Exact retries
precede capacity gates and share immutable report-event storage; different
content under one `CommandId` is rejected. A protected lane of at least
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
adds `K + 1` event construction. State is
`O(H_max + E_max + I_max + P O_max + P_max)`. Stable
wire/full-WAL recovery are supplied by A65 and semantic checkpoint/cutover by
A66; risk/ledger effects, publication transport, settlement, calendar,
controller authentication, reference/dynamic-band derivation, and venue
conformance do not follow.

**Falsification probe.** Exercise every phase edge and invalid edge,
exact/stale revisions, delayed prior-cycle/reopen submissions, amendments, and
replacements,
skipped/reused/exhausted `AuctionId`, exact retry/content collision,
foreign/stale prepared tokens, ordinary/terminal history boundaries, malformed
terminal attempts, report-capacity and sequence exhaustion, prepared-uncross
pool exhaustion, empty/non-empty all/side mass cancellation in every phase,
ordinary/terminal-lane selection, empty and non-executable uncross at
`u64::MAX`, close with retained interest followed by closed-phase cancellation,
and event/cache audit corruption. Differentially compare at least 10,000 generated controller
commands with a literal phase model.

Any mutation on rejection, cross-cycle entry, amendment, or replacement,
sequence gap/wrap, second retry effect, consumed reserve from an invalid
command, sequencing or WAL append on pool exhaustion, stranded retained
interest, accepted foreign/stale token, phase/book partial commit, trace grammar
mismatch, or audit divergence falsifies A64. Multi-process ownership or venue
session semantics require an external fenced/versioned authority; local restart
continuity is bounded by A65/A66.

## A65 — durable auction WAL grammar

**Assumption.** One `DurableCallAuctionEngine` owns the only writer lease for
one WAL-version-12 single-file or marker-selected segmented auction shard. Its
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
tag `8`; deterministic restart continuity and exact-retry cache reconstruction
in both physical layouts; no silent second effect or request-attempt logging.
For `C` commands, `E` report events, `B` bytes, and `S` segments, full-WAL open
costs `O(B + S)` framing plus the sum of `C` A64 command costs and `O(E)`
report comparison. A66 may replace prefix replay with verified checkpoint
reconstruction.

**Falsification probe.** Byte-compare every command/event shape and raw kind
tags; reject invalid tags, lengths, booleans, identities, inverted bands, zero
clearing execution, contradictory report grammar, trade self-pairing, and
overflowed cancellation source quantity. Terminate after definition, command
append, engine commit, partial/full report append, rotation, and sync; repair
only a torn active tail; replay exact state/trade IDs/cache identity; retry
before/after reopen and prove zero frame growth. Inject definition drift,
unexpected records, consecutive/dangling duplicates, persisted `replayed =
true`, report mutation, replacement trace reordering/identity reuse/priority
retention, mass-cancel account/scope/order/count/quantity/revision corruption,
amendment owner/quantity/immutable-field/priority/revision corruption,
segment corruption, insufficient limits, and frame versions
`1`/`2`/`3`/`4`/`5`/`6`/`7`/`8`/`9`/`10`/`11`. Any accepted
divergent/noncanonical history,
duplicate transition, retry frame, cross-layout semantic difference, or
unaudited dangling completion falsifies A65.

## A66 — auction checkpoint lineage

**Assumption.** A snapshot-version-12 call-auction checkpoint and its recovery
WAL represent one immutable A65 command/report lineage. The image retains the
definition/WAL origin, completed report boundary, phase/cycle, book revision,
next priority/trade counters, canonical accepted identities and active orders,
and complete exact-retry history. A completed checkpoint is released only after
independent replay requires exact direct-state equality; A97/A98 separate
non-replaying capture from that proof. Numeric generation is never accepted
without exact uncut prefix equality or a kind/checksum/generation/slot-bound
version-12 anchor. Capture/validation resource or temporary-constructor failure
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
absent because reference, candidate band, and ranking policy are external under
A60/A64. One accepted replacement projects one complete command batch of
exactly two updates: anonymized target removal with reason `Replaced`, then
replacement addition with reason `Accepted`. The source and replica book
revision advances once, on the second update.
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

**Dependent results.** [A67] Publisher bid/ask stable-slot AVL arenas,
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
sides atomically. Direct book and replica depth output is `O(min(P,L))`;
publisher bootstrap/cross-audit is expected `O(O + P)`; an uncross report is
expected `O(T + C)` over prints and cancellations, while replacement has
`E = 2`, amendment has `E = 1`, and mass cancellation has `E = K + 1`.
Structural failure after incremental mutation poisons state; capacity preflight
failure does not. Stable payload version 4 contains no process-local limit
metadata.

No transport, entitlement, conflation, indicative publication, or
information-hiding guarantee follows.

**Falsification probe.** Exercise market/limit acceptance, user removal,
replacement across sides/constraints/prices at saturated capacity,
retained-priority amendment across market/limit aggregates,
empty/all/side mass cancellation in every phase, anonymized removals, exact
completion totals, timestamp equality, conditional revision, split and
malformed complete batches,
crossed depth, multi-pair uncross, all affected aggregate combinations, exact
retry/rejection, two-cycle retained remainder, monotonic trade/cycle/revision
state, update/snapshot codec round trips, gaps, stale repair, wrong
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
scratch residue, non-atomic snapshot replacement, changed wire bytes, or
recovery divergence falsifies A67. A venue feed requiring indicative imbalance,
order-level publication, conflation, delay, or auction-status codes requires a
separately versioned projection.

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
An accepted replacement trace removes the target reservation before inserting
the replacement reservation. An accepted mass cancel releases each selected
reservation exactly once from its canonical removal events; its completion has
no second risk-state effect and undergoes no numerical entry authorization.
An accepted amendment undergoes no new numerical entry authorization and
reduces its reservation quantity, conservative notional, and account exposure
by the exact positive leaves delta without changing reservation count.

**Dependent results.** [A68, A99, A100] Missing/blocked/reduce-only/numerical
failures are sequenced stable rejection tags `12`--`21`; exact retries have no
second exposure effect. Profile, reservation, uncross-netting, and
auction-history indexes own complete fixed dense/bucket storage before state
exists. Expected risk work is `O(1)` per submit/amend/replace/cancel event,
`O(K)` for a mass cancel with `K` selected reservations, and `O(T + C)` for
an uncross with `T` pairs and `C` remainder cancellations.
`CallAuctionRiskCheckpoint` canonically retains profiles/exposures plus the
A66-style auction image, reconstructs reservations from active orders, and is
released only after full history replay through the coupled gate; A99/A100
stage that proof.

Snapshot-v14 kind `5` and `DurableCallAuctionRiskEngine` bind a canonical
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
partial/full
fills, all remainder policies, close/cancel, retained multi-cycle interest,
same-account pairs near signed position bounds, reduce-only aggregate sides,
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

**Dependent results.** [A75] A successful engine audit checks cache
layout/identity, command and event continuity, report grammar, phase replay,
capacity, and the complete A74 book directly in `O(H + E)` history work and
`O(1)` auxiliary space for `H` retained commands containing `E` events, with no
successful-path heap allocation. Checkpoint capture emits the already-canonical
order without `O(H log H)` sorting; owned checkpoint payload allocation remains
under A12/A66. Failure-detail construction can allocate.

**Falsification probe.** Audit empty history and every
phase/rejection/submit/amend/replace/cancel/mass-cancel/uncross grammar; run at
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
terminal-lane action. A mass cancel consumes exactly `K + 1` slots. It may use
the terminal lane only for `K > 0`; `K = 0` remains ordinary. After a non-empty
mass cancel, the remaining tail still covers individual cancellation of every
survivor or one freeze plus the survivors' maximum uncross trace.

**Dependent results.** [A85] Per-report and total bounds are independent. An
ordinary action, including replacement or empty mass cancel, whose conservative report bound crosses the ordinary event
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
use, ordinary replacement, and empty mass cancellation; apply side/all non-
empty mass cancellation, then freeze and uncross survivors; retry without
consumption; restore direct/risk
checkpoints and full/segmented WAL; corrupt arena identity, range order,
retained cursor, and reservation. Any post-construction arena growth, second
slot initialization, range gap/overlap, invalid terminal admission, stale-token
consumption, accepted oversized checkpoint, or replay divergence falsifies A85.

The current default is 73,730 slots; on measured `aarch64-apple-darwin`,
`size_of::<OnceLock<CallAuctionEvent>>() = 176 B`, so slots occupy `12,976,480
B` (`12.976480 MB`) before vector/Arc/allocator overhead. Externally retained
live traces keep the complete arena alive.

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
the measured `aarch64-apple-darwin` build, `S_fill = 24 B`, `S_trade = 56 B`,
and `S_cancel = 56 B`; defaults therefore request at least `2 × 4,096 × (2 × 24
B + 56 B + 56 B) = 1,310,720 B = 1.310720 MB` of element storage. The `Arc`
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
follow A90. Equality, same-lineage prefix checks, validation, and stable codecs
inspect ordered values only.

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
counters from retained traces without executing commands. The opaque
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
cancellations, executable/non-executable uncrosses,
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
self barriers used by FOK. Decrement-and-cancel is inadmissible because
prevented self quantity is not executed quantity.

The nonmutating preflight precedes matching and STP effects. If eligible
quantity is below the threshold, the order is accepted and its complete
quantity is cancelled with `MinimumQuantityUnavailable`; no maker changes. If
the threshold is met, ordinary IOC matching can execute beyond it and cancels
only the final remainder. A dormant stop retains the constraint, evaluates it
against activation-time liquidity, and cannot be replaced below the threshold.
This pairing, cancellation reason, and STP/reserve policy are Quotick internal
contracts; FIX `MinQty(110)` and IOC terminology do not supply those venue-
specific rules.

**Dependent results.** [A1, A5, A9, A15, A20, A21, A22, A37, A39, A45, A50,
A70, A83, A88, A102, A103, A105] Allocation-free `O(1)`-space threshold
inspection, atomic failure, reserve/hidden-aware eligibility, stop activation,
no-change public projection, risk release, stable WAL-v14/snapshot-v14 bytes,
checkpoint/WAL recovery, and exact retry are deterministic.

**Falsification probe.** Exercise thresholds below, equal to, and above
available external quantity across multiple prices; thresholds off grid, above
original quantity, and below the entry minimum; every STP policy; displayed,
reserve, and hidden makers; displayed- and hidden-class self barriers; ordinary
and stop activation; replacement; public projection; risk; checkpoint/WAL
recovery; and exact/different-content retries. Compare accepted executions with
an independent literal reserve-refresh queue. Any partial maker/STP mutation
on threshold failure, prevented self quantity counted as execution, execution
below threshold, artificial cap at the threshold, unsupported decrement-and-
cancel admission, replay divergence, or tag drift falsifies A105.

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

**Dependent results.** [A12, A23, A67, A108] Construction is `O(N)` time and
typed slot space. Admission is `O(E)` time and allocation-free. Successful page
selection and complete iteration are each `O(R)` for `R` returned updates and
use `O(1)` iterator state; diagnosing a partial oldest batch can scan `O(N)`
slots. `CallAuctionMarketDataReplica::apply_replay_batch` reuses the live batch
identity, sequence, capacity, transition, poisoning, and command-counter path,
so recovery advances event and command boundaries together. Snapshot fallback
remains authoritative when a complete required batch is unavailable or the
replica is poisoned. Version-4 payload bytes do not change.

**Falsification probe.** Reject zero and unrepresentable capacities; admit
single-update phase/order/amendment batches, a two-update replacement,
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
changed version-4 payload byte, or recovery divergence falsifies A108.

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
notional delta without a new entry gate. Public payload v4 emits one anonymous
`Amended` aggregate delta with unchanged order count. WAL/snapshot v14 preserve
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
allocation inference, and auction market-data payload v4 remains a projection
of the same events. WAL v14 and snapshot v14 persist the explicit policy for
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
trace without independently inferring class. WAL v14 and snapshot v14 preserve
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

## Bounded scope expansion

Each entry below is tagged with an impact level and records an implemented
capability, a remaining risk, or an opportunity.

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
  pairing, and WAL-v14/snapshot-v14 recovery. Every live order carries one
  authoritative typed priority-class scalar used by both policies after
  market/price ordering and before time. An authenticated venue-category-to-
  scalar mapping remains adapter conformance work. Venue algorithms that rank by
  size, weight by time, reserve top-order or minimum shares, split FIFO and
  pro-rata percentages, or distinguish display categories remain separate
  conformance work rather than aliases of `ProRataTime`.
- **High impact:** call-auction collection and sequencing now support bounded
  account/side mass cancellation through an intrusive owner index. One accepted
  command selects `K` orders independently of unrelated active interest, emits
  canonical ascending removals plus one aggregate completion, increments the
  book revision exactly once when `K > 0`, releases each risk reservation, and
  recovers under WAL/snapshot version 14. Public payload version 4 projects the
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
  Indicative publication remains absent until authoritative reference, band,
  ranking, and venue disclosure policy are carried explicitly.
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
  single/segmented recovery, aggregate-depth queries, and anonymized
  phase/trade/final-clearing publication with retained complete-batch replay
  and gap-repair snapshots are implemented. Reference/dynamic-band
  construction, indicative publication,
  auction display and venue-specific size-ranked, time-weighted, minimum-share,
  or hybrid allocation policies, preventive
  self-trade policies, auction-ledger integration, settlement, authenticated
  public/private transport, market-on-auction and imbalance-only order types,
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
  a reference command is sequenced. Feed selection, trade correction/bust
  handling, reference-source authentication, missed-reference recovery, and
  cross-shard ordering are external. A stale but structurally valid reference
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
- **Medium impact:** FOK and minimum-quantity IOC preflight allocate no queue
  and scan each active order in crossed levels at most once. For `O_c`
  inspected orders and `P_c` crossed levels, time is
  `O(O_c + P_c log P)` and auxiliary space is `O(1)`, independent of reserve
  replenishment count. A 20,000-case deterministic FOK differential test
  matches the prior literal slice/requeue model; targeted minimum-quantity
  tests cover atomic failure and reserve self barriers. Maximum-book scan
  latency and interaction with CPU cache residency remain unknown until
  measured on declared production capacities and hardware.
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
  `73,730 × 176 B = 12,976,480 B` (`12.976480 MB`) for call auction, excluding
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
  position amendments, clearing corrections, account onboarding, controller
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
  clearing transfers, busts/corrections, and administrative controls require a
  separately sequenced durable event model; mutating those inputs out of band
  would invalidate deterministic risk replay. The plain durable auction runtime
  intentionally rejects profile-prefixed risk journals.
- **High impact risk:** auction and continuous risk are per-account,
  per-instrument raw-price-times-lots controls. Cross-instrument portfolio
  offsets, collateral, margin, currency conversion, fees, option Greeks,
  clearing transfers, busts/corrections, and external position synchronization
  remain unrepresented.
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
- **Medium impact opportunity:** the generalized ledger-batch primitive can
  carry fees, allocations, multi-leg settlement, and clearing bundles without
  exposing partial economic state. Product-specific construction,
  authorization, and external lifecycle evidence remain separate adapters.
- **Low impact:** UI and visualization layers should consume immutable versioned
  traces and snapshots; they are not authoritative state owners.
