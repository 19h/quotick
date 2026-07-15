# Expired WAL Format Version 3

**Expired historical format.** This document preserves the historical
byte-level schema for expired Quotick WAL version 3; it is retained as
provenance for payloads first carried forward by version 4. The current runtime
rejects this envelope and writes [WAL version 5](wal-v5.md). Version 4
historically incorporated record payloads `1` through `8` from this document
byte-for-byte while changing the envelope and adding call-auction record kinds;
version 5 preserves its unchanged non-continuous payload descendants.

All multibyte integers are little-endian. No Rust enum layout, padding,
pointer, or platform ABI is persisted.

## Contents

- [Frame](#frame)
- [Segmented-directory marker version 2](#segmented-directory-marker-version-2)
- [Scalar notation](#scalar-notation)
- [Instrument-definition payload](#instrument-definition-payload)
- [Account-risk-definition payload](#account-risk-definition-payload)
- [Command payload](#command-payload)
- [Matching capacity and WAL admission](#matching-capacity-and-wal-admission)
- [Execution-report payload](#execution-report-payload)
- [Ledger-entry payload](#ledger-entry-payload)
- [Ledger-correction payload](#ledger-correction-payload)
- [Ledger-batch payload](#ledger-batch-payload)
- [Recovery](#recovery)
  - [Checkpoint-anchor payload and WAL
    cutover](#checkpoint-anchor-payload-and-wal-cutover)
- [Writer ownership and acknowledgement](#writer-ownership-and-acknowledgement)
- [Primary-source provenance](#primary-source-provenance)

## Frame

| Offset (bytes) | Width (bytes) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `3` |
| 6 | 2 | record kind: command `1`, execution report `2`, ledger entry `3`, instrument definition `4`, account risk definition `5`, ledger correction `6`, ledger batch `7`, checkpoint anchor `8` |
| 8 | 4 | payload length |
| 12 | 4 | CRC-32C |
| 16 | 8 | contiguous journal sequence |
| 24 | payload length | typed payload |

CRC-32C uses the reflected Castagnoli polynomial `0x82F63B78`, initial state
`0xFFFFFFFF`, and final XOR `0xFFFFFFFF`. The checksum covers the complete
header with bytes 12–15 set to zero, followed by the payload.

## Segmented-directory marker version 2

Segmentation does not alter `QWAL` frames or their global sequence. A segmented
WAL is a dedicated directory containing `format.qseg` and canonical files named
`segment-GGGGGGGGGGGGGGGGGGGG-SSSSSSSSSSSSSSSSSSSS.qwal`. `G` is the
zero-padded 20-digit physical inventory generation and `S` is the zero-padded
20-digit first frame sequence assigned to that file. Both are non-zero `u64`
values.

The 46-byte `format.qseg` marker is:

| Offset (bytes) | Width (bytes) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSEG` |
| 4 | 2 | marker version `2` |
| 6 | 8 | maximum segment bytes `u64` |
| 14 | 8 | immutable lineage-origin sequence `u64` |
| 22 | 4 | maximum payload bytes `u32` |
| 26 | 8 | marker-selected active physical generation `u64` |
| 34 | 8 | first sequence retained by that generation `u64` |
| 42 | 4 | CRC-32C over the complete marker with this field zeroed |

All marker integers are little-endian. Capacity, lineage origin, and payload
maximum are immutable for the directory. The active generation and its first
sequence advance only through checkpoint cutover. The initial generation is
`1`. Readers select exactly the named generation and ignore recognized staged
or non-selected generation files. A writer validates the complete selected
generation before synchronously removing those inactive artifacts. The active
acknowledgement policy and recovery mode are not persisted because they do not
change frame or segment interpretation.

An invalid partial/checksum-failing marker is recoverable only while no segment
or unknown directory entry exists. A valid marker is never removed by
incomplete-initialization recovery. CRC-32C detects accidental selector or
configuration corruption; it is not an authentication mechanism.

## Scalar notation

- `u8`, `u32`, `u64`, `u128`, `i32`, `i64`, `i128`: fixed-width integers.
- `bool`: `u8`, where `0` is false and `1` is true.
- Every identifier: validated non-zero `u64`.
- `Price`: `i64` instrument quantum.
- `Quantity`: validated non-zero `u64` lots.
- Collection: `u32` element count followed by elements.

## Instrument-definition payload

Fields occur in this exact order:

1. instrument ID `u64`;
2. immutable version `u64`;
3. effective-from Unix timestamp in nanoseconds `u64`;
4. symbol length `u8`, then that many canonical ASCII bytes (1–32 bytes);
5. instrument-kind tag `u8`;
6. base asset ID `u64`, quote asset ID `u64`;
7. price decimal scale `u8`, tick size `u64`, minimum price `i64`, maximum
   price `i64`;
8. quantity increment `u64`, minimum quantity `u64`, maximum quantity `u64`;
9. maximum reserve replenishments `u32`; zero disables reserve orders;
10. base units per lot `u64`, quote units per raw price unit `u64`;
11. trading-state tag `u8`.

Instrument-kind tags are equity `0`, spot `1`, future `2`, option `3`, bond
`4`, swap `5`, index `6`, and synthetic `7`. Trading-state tags are open `0`,
cancel-only `1`, halted `2`, and closed `3`. Decoding reconstructs the validated
domain types and rejects any inconsistent scale, grid, boundary, identity, or
conversion multiplier.

## Account-risk-definition payload

Fields occur in this exact order (121 bytes total):

1. account ID `u64`;
2. account-risk-state tag `u8`: active `0`, reduce-only `1`, blocked `2`;
3. signed initial position lots `i128`;
4. maximum order quantity lots `u64`;
5. maximum order notional `u128`;
6. maximum open orders `u64`;
7. maximum open quantity lots `u128`;
8. maximum open notional `u128`;
9. maximum long position lots `u128`;
10. maximum absolute short position lots `u128`.

All limits are positive. Per-order quantity/notional cannot exceed their
aggregate open counterparts; position limits cannot exceed `i128::MAX`; the
initial position must be inside its signed bound. Risk-managed journals store
these records in strictly increasing account-ID order.

## Command payload

The first `u8` selects new `0`, cancel `1`, replace `2`, mass cancel `3`,
account control `4`, or instrument trading-state control `5`.

- New: command ID, order ID, account ID, instrument ID, instrument version,
  side, quantity, display policy, order type, time in force, self-trade policy,
  receive timestamp.
- Cancel: command ID, order ID, account ID, instrument ID, instrument version,
  receive timestamp.
- Replace: command ID, order ID, account ID, instrument ID, instrument version,
  new leaves quantity, new price, new display policy, receive timestamp.
- Mass cancel: command ID, account ID, instrument ID, instrument version,
  selection scope, receive timestamp.
- Account control: command ID, account ID, instrument ID, instrument version,
  expected control revision `u64`, action, receive timestamp.
- Instrument trading-state control: command ID, instrument ID, instrument
  version, expected state revision `u64`, target trading-state tag, action,
  receive timestamp.

Side tags are buy `0`, sell `1`. Order type tags are market `0`, limit `1`
followed by price. Time-in-force tags are GTC `0`, IOC `1`, FOK `2`, post-only
`3`. Self-trade tags are cancel aggressor `0`, cancel resting `1`, cancel both
`2`, decrement-and-cancel `3`.

Display-policy tags are fully displayed `0` and reserve `1` followed by peak
quantity `u64`. A reserve peak is lot-grid aligned and strictly smaller than
total quantity. The implied replenishment count
`floor((total quantity - 1) / peak)` cannot exceed the definition's configured
maximum. Reserve is admitted only on resting-capable limit orders.

Mass-cancel scope tags are all owned orders `0`, or one side `1` followed by a
side tag. The command applies only within its instrument-version shard and is
admitted in every trading state after identity validation.

Account-control action tags are block-and-cancel `0` and enable `1`. An account
without retained control state is enabled at revision zero. Acceptance requires
the command's expected revision to equal current state and increments it by
exactly one. Block-and-cancel closes the admission fence and cancels every
resting order for the account in the same command transition. Enable reopens
entry and cancels nothing. New orders and replacements for a blocked account
are rejected; owner cancellations remain admitted.

Trading-state-control action tags are transition `0` and
transition-and-cancel `1`. Effective state starts from the definition's state at
revision zero. Acceptance requires an exact expected revision, increments it
once, and requires a different target. Transition-and-cancel cannot target open;
for another target it cancels every active order in ascending order ID before
the final state event. Transition retains resting orders. New and replace use
effective state; cancellation and administrative controls remain available in
every state after identity validation.

## Matching capacity and WAL admission

`OrderBookLimits` is finite operational process policy and is not encoded in
version-3 financial metadata. It does not change the interpretation of an
existing frame. Durable matching and risk wrappers prepare matching state
before appending a command frame. A capacity failure therefore has no event
sequence, report, or WAL representation and the command identifier remains
unused.

Successful preparation produces an opaque process-local token bound to the
exact book instance and retained-command generation. Durable matching and risk
append `PreparedCommand.command()` and then commit the same token, so capacity,
identifier-space, FOK, and core business checks are not repeated after WAL
append. Commit rejects foreign or stale tokens before mutation; recovery never
serializes a token and instead prepares the persisted command against recovered
state.

Both stable-slot price-level AVL arenas, all five matching hash indexes,
and the complete coupled-risk profile and reservation indexes are fallibly
reserved to their configured maxima when the shard is constructed. Preparation
therefore borrows matching/risk state immutably, proves the safe report bound
against the constructor-owned retained-event arena, and fallibly reserves only
the exact identifier selection for mass-cancel, block-and-cancel, or instrument
transition-and-cancel. These structures cannot grow during commit;
level insertion/deletion and account membership allocate no node.
Active-order/account, identity, control, history, profile, and risk-reservation
indexes use fixed-capacity dense values with open-addressed lookup buckets and
backward-shift deletion, so different-key churn cannot rehash or allocate after
construction.

The one arena `Arc` control block and unrelated codec/checkpoint buffers are
outside this operational guarantee. AVL topology, vacant-slot links,
and account/side membership links are derived in-memory state: they are absent
from WAL/checkpoint bytes and are rebuilt and independently audited during
recovery.

`EventTrace` is an in-memory immutable shared representation only. Live reports
reference exact ranges in one constructor-owned append-only arena; decoded
reports use an owned vector fallback. WAL report encoding still serializes the
same event count and ordered event values; backing kind, range, `Arc` identity,
ownership count, and copy-on-write state are absent from version-3 payload
bytes. Decoding constructs a new vector event buffer plus shared-owner control
block, retains that vector without a final event copy, and applies the same
semantic report validation. Checkpoint restoration copies validated values into
the new live arena before exposing the restored book.

When a resting bound is full, the same pre-WAL preparation performs an
allocation-free GTC/post-only residual preview. A command proved to be fully
executed or aggressor-cancelled proceeds without a resting slot; a residual that
would rest proves complete removal of every crossed opposite level. Cached level
counts and account membership then provide exact final active-order and
active-account cardinalities; same-side price-level capacity is unchanged by
opposite-side matching. All three limits are decided before WAL append.

Retained command history has an ordinary prefix and a protected cancellation
tail. Once the ordinary prefix fills, new and replace commands fail preflight.
Only a cancel, mass cancel, block-and-cancel account control, or instrument
transition-and-cancel into an entry-closed state that currently passes core
instrument, identity, ownership/revision, and active-state checks may enter the
tail. Reopening controls remain ordinary admission commands. Exact cached retries do
not append frames and remain available at total exhaustion. The configured tail
is at least the maximum active-order count, permitting one valid individual
cancel for every maximally populated order when the reserve has not already
been consumed by valid controls.

Recovery APIs select limits explicitly or use finite defaults. Raw WAL replay
fails if a retained historical transition exceeds the selected policy. For an
uncut or segmented WAL, checkpoint-assisted recovery first verifies the
complete WAL prefix. For an anchored WAL in either physical layout, it verifies
the selected checkpoint against the anchor and reads only the physical suffix. Both paths
require retained/current checkpoint cardinalities and every replayed suffix
transition to fit. Operational limit changes require no wire-format migration
but may require larger recovery limits or a fenced generation rollover.

Rejection-reason tags are wrong instrument `0`, duplicate order `1`, unknown
order `2`, not owner `3`, market cannot rest `4`, market cannot post `5`,
unsupported FOK/STP combination `6`, insufficient liquidity `7`, post-only
would cross `8`, wrong instrument version `9`, instrument not open `10`, price
off tick grid `11`, price outside collar `12`, quantity off lot grid `13`,
quantity outside limits `14`, missing risk profile `15`, blocked risk account
`16`, reduce-only violation `17`, risk order-quantity limit `18`, risk
order-notional limit `19`, risk open-order-count limit `20`, risk open-quantity
limit `21`, risk open-notional limit `22`, risk position limit `23`, and risk
arithmetic overflow `24`, reserve unsupported `25`, display quantity off grid
`26`, display quantity not smaller than total `27`, reserve replenishment limit
`28`, reserve cannot be immediate `29`, display-mode conversion forbidden
`30`, account admission blocked `31`, account-control revision mismatch `32`,
account-control revision exhausted `33`, trading-state revision mismatch `34`,
trading-state revision exhausted `35`, trading state unchanged `36`, and
transition-and-cancel into open `37`.

Cancellation-reason tags are user request `0`, unfilled remainder `1`, STP
aggressor `2`, STP resting `3`, mass cancel `4`, account control `5`, and
trading-state control `6`.

## Execution-report payload

Fields are command ID, outcome, replay boolean, event count, then events.
Outcome is accepted `0` or rejected `1` followed by a rejection-reason tag.
Each event contains event sequence, command ID, occurrence timestamp, and one
event-kind union:

| Tag | Event |
|---:|---|
| 0 | order accepted: order ID, total quantity, display policy |
| 1 | order rested: order ID, price, total leaves quantity, displayed quantity |
| 2 | trade: trade ID, instrument ID, instrument version, price, quantity, buy/sell orders, buyer/seller accounts, maker/taker orders |
| 3 | order cancelled: order ID, quantity, reason |
| 4 | order replaced: order ID, old/new prices, old/new total quantities, old/new display policies, retained-priority boolean |
| 5 | self-trade prevented: aggressor/resting orders, quantity, policy |
| 6 | command rejected: reason |
| 7 | reserve refreshed: order ID, price, displayed quantity, total leaves quantity |
| 8 | mass cancel completed: account ID, scope, cancelled order count `u64`, total cancelled leaves `u128` |
| 9 | account control applied: account ID, previous/current admission-state tags, revision `u64`, cancelled order count `u64`, total cancelled leaves `u128` |
| 10 | trading-state control applied: previous/current trading-state tags, revision `u64`, cancelled order count `u64`, total cancelled leaves `u128` |

Decoding requires non-empty, contiguous events correlated to the report command.
Accepted reports cannot contain rejection events; rejected reports contain
exactly one matching rejection event.

Reserve refresh keeps the same private order ID but records a new displayed
slice after the prior slice has reached zero and the order has moved to the
price-level FIFO tail. Total leaves are used for cancel, replacement, FOK, and
risk state. Displayed leaves are used for aggregate public depth.

A mass cancel emits one tag-`3` order-cancelled event per selected order in
strictly ascending order ID, each carrying that order's total leaves, followed
by exactly one tag-`8` completion. The completion count and `u128` quantity sum
must equal those preceding cancellation events. An empty selection emits only
the zero-valued completion.

A block-and-cancel account control emits one tag-`3` cancellation with reason
`5` per selected order in strictly ascending order ID, then one tag-`9`
completion. The completion aggregates equal the preceding cancellations. An
enable control emits only tag `9` with zero cancellation aggregates. Admission
state tags are enabled `0` and blocked `1`; the completion revision equals
`expected revision + 1` and carries the exact prior and resulting state.

An instrument transition-and-cancel emits one tag-`3` cancellation with reason
`6` per active order in strictly ascending order ID, then one tag-`10`
completion whose aggregates equal those cancellations. Transition-only emits
only tag `10` with zero aggregates. Trading-state tags are those defined for the
instrument payload; completion revision equals `expected revision + 1` and
carries the exact prior and target state.

Under assumptions A15, A57, and A59, account-control command tag `4`, event tag
`9`, rejection tags `31`–`33`, and cancellation tag `5` remain unchanged;
instrument-state control uses command tag `5`, event tag `10`, rejection tags
`34`–`37`, and cancellation tag `6` in version 3. Version-1 and version-2 WAL
artifacts are rejected as unsupported; absent control or cutover fields are
never assigned inferred semantics.

## Ledger-entry payload

Fields occur in this exact order:

| Order | Width | Field |
|---:|---:|---|
| 1 | 8 B | transaction ID `u64` |
| 2 | 8 B | source reference `u64` |
| 3 | 1 B | effective-date-present `bool` |
| 4 | 4 B | signed days from 1970-01-01 `i32`; zero when absent |
| 5 | 8 B | recorded-at Unix timestamp in nanoseconds `u64` |
| 6 | 4 B | posting count `u32` |
| 7 | 32 B each | account ID `u64`, asset ID `u64`, signed amount `i128` |
| 8 | 1 B | entry-kind tag `u8` |
| 9 | 8 B | related transaction ID `u64`; zero when absent |
| 10 | 1 B | period-boundary-present `bool` |
| 11 | 4 B | signed boundary days from 1970-01-01 `i32`; zero when absent |

The fixed payload portion is 47 B and each posting is 32 B. Financial entries
require an effective date. Their postings are strictly sorted by `(asset ID,
account ID)`, contain no duplicate pair or zero amount, contain at least two
legs, and balance independently for every asset. Balance proof compares exact
positive and negative magnitudes; it does not add signed legs in wire order and
therefore has no `i128`/`u128` aggregate ceiling. Administrative period controls
have no effective date and exactly zero postings. `recorded_at` is
nondecreasing over accepted journal sequence; equal timestamps are permitted.

| Tag | Meaning | Related transaction | Period boundary |
|---:|---|---|---|
| 0 | standard financial entry | zero | absent |
| 1 | reversal financial entry | non-zero target transaction | absent |
| 2 | period close | zero | present inclusive `closed_through` date |
| 3 | period reopen | zero | replacement boundary or absent to reopen all dates |

Any other tag or contradictory shape is invalid. A close must strictly advance
the current inclusive boundary. A reopen requires an existing boundary and
must replace it with an earlier value or remove it. A financial effective date
at or before the current boundary is rejected. Exact transaction retries are
resolved before time or transition validation and return their original
sequence without another effect.

Framing validation alone cannot establish reversal semantics. Ledger replay
requires the target to precede the reversal, requires that target not already
have a committed reversal, and compares every reversal posting with the exact
signed inverse of the target posting. Reversing a reversal is permitted once
and is an explicit reinstatement; the lineage remains an append-only chain.
Period controls have no financial posting effect and cannot be reversed.

`AccountingDate` is a compact internal key. Calendar/service boundaries map it
to an authoritative Gregorian date representation; ISO 8601 string parsing,
business-day calendars, time zones, and close authorization are outside this
payload codec.

## Ledger-correction payload

A ledger correction is one record-kind `6` payload containing, in order:

1. reversal payload length `u32`, then one complete `JournalEntry` payload;
2. replacement payload length `u32`, then one complete `JournalEntry` payload.

Its encoded length is `102 B + 32 B × (Lᵣ + Lₚ)`, where `Lᵣ` and `Lₚ`
are the reversal and replacement posting counts. The minimum is 230 B because
both financial entries require at least two legs. The first entry must be an
exact reversal and the second must be a standard entry. Their transaction IDs
must be distinct and neither may equal the corrected target; the replacement
timestamp cannot precede the reversal timestamp.

Ledger admission additionally proves that the target precedes the correction,
has no prior reversal, both effective dates are open, neither correction
transaction was previously committed, and the exact final balances are
representable. The two entries share one ledger-event sequence. Exact retries
replay that event without a second effect. Because the complete pair occupies
one CRC-protected frame, final-tail repair retains both entries or neither; it
cannot retain only one correction member.

## Ledger-batch payload

A ledger batch is one record-kind `7` payload containing:

1. entry count `u32`, which must be at least `2`;
2. for every entry in authoritative order: payload length `u32`, then one
   complete `JournalEntry` payload.

For `N` entries and `L` total posting legs, its encoded length is
`4 B + 51 B × N + 32 B × L`. The minimum is `106 B` for two zero-posting
period controls; a two-member all-financial batch is at least `234 B`.
Transaction identifiers must be distinct and `recorded_at` values must be
nondecreasing in declared order.

Admission evaluates period controls, transaction visibility, and reversal
lineage sequentially over an overlay: effects introduced by an earlier member
are visible to later members only. Balance effects are not applied
sequentially. For each `(account, asset)` key, admission computes the directly
representable final value `b + Σδᵢ`; failure of any member or final value leaves
the ledger unchanged. Every member shares the one ledger-event sequence.

An exact retry requires equal entry content and the identical ordered grouping.
If only some members exist, or all exist under another event grouping, replay
fails as partial prior commitment. The complete payload occupies one bounded,
CRC-protected frame, so torn-tail repair and segment rotation retain every
member or none.

## Recovery

A frame is authoritative only after its header, declared payload, CRC-32C, and
expected sequence verify. Repair mode may truncate bytes beginning at an
incomplete final frame. It never repairs or skips a complete corrupt frame.

Across a segmented directory, canonical filenames are sorted by their encoded
start sequence and frames remain one contiguous global sequence. Every
non-final file is scanned in strict mode and must contain at least one frame.
Only the final file may be empty or may repair a physically incomplete tail.
The final empty-file case represents interruption between segment creation and
the first append and is reused on reopen. A frame or grouped append larger than
the configured segment capacity is rejected before rotation; grouped appends
are placed wholly within one segment.

A matching journal begins with exactly one instrument-definition record. The
remaining grammar is alternating command and execution-report records, with at
most one final command lacking a report. An empty journal is initialized by
appending the requested definition; a nonempty journal without definition as
its first frame is rejected. Recovery compares the complete persisted
definition with the requested definition before replay. A ledger journal
accepts ledger-entry, ledger-correction, and ledger-batch records only.

Semantic checkpoint payloads are not embedded in WAL frames. Period controls
use ledger-entry record kind `3`; indivisible reversal-plus-replacement events
use ledger-correction kind `6`; generalized multi-entry events use ledger-batch
kind `7`. Matching, coupled risk/matching, and ledger checkpoints are separate
`QSNP` files described by
[Semantic snapshot format version 2](snapshot-v2.md).

An uncut WAL uses prefix-proving recovery: open scans every frame, requires the
checkpoint's complete command/report or ledger-record sequence to equal the
exact WAL prefix, then applies the remaining suffix. A matching checkpoint
boundary is always a completed execution-report frame.

`DurableOrderBook::capture_checkpoint_candidate` completes a full WAL barrier
at that boundary before returning a nonencodable capture. Deterministic replay
may proceed off-thread while later command/report pairs append. Standalone
publication accepts only the verified typestate through the same open shard and
unchanged process-local cutover epoch. This permits an older exact prefix but
rejects reopen or physical-prefix retirement. The staged handle cannot drive
record-kind-`8` cutover; prefix retirement remains a synchronous current-head
operation so no suffix can be discarded.

`DurableRiskOrderBook::capture_checkpoint_candidate` applies the same staged
barrier and origin/epoch fence to the coupled grammar. Its capture also binds
the original first sequence `F`, final immutable profile boundary `M`, and
canonical profile/account image. Direct reconstruction proves live positions,
exposures, and total-leaves reservations before handoff; complete coupled
command/report replay occurs only in the consuming verifier. A verified older
generation may be written as a standalone kind-`3` snapshot after ordinary
suffix growth. Another shard, reopen, metadata drift, an ahead-of-head value,
or successful prefix cutover rejects publication before output creation.

### Checkpoint-anchor payload and WAL cutover

Record kind `8` is exactly 30 bytes:

| Order | Width | Field |
|---:|---:|---|
| 1 | 1 | snapshot kind: ledger `1`, matching `2`, coupled risk/matching `3` |
| 2 | 1 | checkpoint slot: A `0`, B `1` |
| 3 | 8 | semantic snapshot generation |
| 4 | 8 | physical WAL sequence of this anchor frame |
| 5 | 8 | encoded semantic snapshot payload length |
| 6 | 4 | exact complete `QSNP` envelope CRC-32C from the synchronized snapshot receipt |

Both sequence values are non-zero. Matching and coupled-risk checkpoint
generation equals the last completed report sequence. Ledger generation counts
semantic ledger records and is independent of a non-default physical WAL
origin; the separate anchor sequence prevents those domains from being
conflated.

A compacted WAL contains the anchor as the first physical frame in its selected
file or segment generation. The frame sequence equals the anchor's physical
sequence, which is also the last sequence of the retired WAL prefix; the next
appended business frame uses `anchor sequence + 1`. Open without a checkpoint
base fails explicitly. Checkpoint-assisted open derives the selected file by
appending `.cutover-a` or `.cutover-b` to that base, verifies the normal `QSNP`
envelope and semantic payload, then requires kind, slot, semantic generation,
payload length, and checksum to equal the anchor before replaying the WAL
suffix.

Every cutover first alternates two immutable checkpoint slots:

1. synchronize every prior WAL append;
2. construct and independently audit the current semantic checkpoint;
3. write and directory-synchronize the inactive A/B snapshot slot using the
   normal pending-file protocol;
4. publish the layout-specific physical anchor described below.

For a single-file WAL, physical publication then:

1. creates `<wal>.cutover.pending` exclusively, writes one anchor frame, and
   synchronizes the complete file;
2. atomically renames that file over the WAL and synchronizes the parent
   directory while retaining the same writer lease;
3. appends subsequent records to the replacement file.

Before the WAL rename, the prior WAL and its selected checkpoint remain authoritative;
the newly written inactive slot is harmless. After the synchronized rename,
the new WAL selects the new slot and the old slot remains a fallback artifact.
A crash before rename can leave `<wal>.cutover.pending`; it is discarded only
by `recover_abandoned_cutover_pending` after prior-writer liveness has been
independently disproved, its lease recovered, and a new exclusive writer lease
acquired. A crash after rename may expose the old or new name binding until the
qualified directory barrier is known durable; either pair is complete. CRC-32C
detects accidental corruption but is not an authenticity proof under A14.

For a segmented WAL, physical publication instead:

1. increments the non-zero marker generation without wrap;
2. exclusively creates `.quotick-segments.cutover.pending`, writes and
   synchronizes one anchor frame, then renames it to
   `segment-<next-generation>-<anchor-sequence>.qwal` and synchronizes the
   directory;
3. exclusively creates and synchronizes `format.qseg.pending` containing the
   next generation and anchor sequence, atomically renames it over
   `format.qseg`, and synchronizes the directory;
4. opens the marker-selected anchor segment, removes all recognized
   non-selected generations/staging files, synchronizes those removals, and
   appends subsequent records in the selected generation.

Before the marker rename, the prior generation is authoritative and the new
generation is ignored. After the marker rename, the new generation is
authoritative and prior generation files are ignored even if cleanup was
interrupted. A reader never combines generations. A new manager validates the
complete selected generation before removing inactive files. A marker rename
whose directory barrier fails is ambiguous; reopening accepts whichever
CRC-valid marker generation survived, and both corresponding physical
generations were synchronized before the selector change.

Matching, coupled risk/matching, and ledger runtimes implement both physical
cutover layouts.

A risk-managed matching journal inserts zero or more account-risk-definition
records between the instrument definition and the first command. The complete
canonical profile set must equal the requested set. If the file ends during
metadata initialization, recovery may append only the missing suffix of an
exact profile prefix; metadata drift or metadata after a command is rejected.
The requested count must fit `RiskManagedLimits`; count rejection and fallible
canonical-vector reservation occur before the WAL path is created or opened.
The in-memory profile index has complete constructor-owned headroom and becomes
locked when the first command is sequenced.

Commands and reports then follow the same alternating grammar, but replay uses
the coupled matching/risk state machine so risk rejections, positions, and
reservations are reconstructed deterministically.

The coupled risk checkpoint stores the true first sequence `F`, embeds matching
state whose metadata boundary is `M = F + A` for `A` canonical profiles, and
ends at `G = M + 2C` for `C` complete command/report pairs. Assisted recovery
proves definition/profile metadata and every retained pair against the exact
WAL before restoring positions and total-leaves reservations and applying the
suffix.

The staged durable-risk path synchronizes through `G` before capture, may verify
that exact prefix off-thread while later pairs append, and publishes only a
same-origin, same-cutover-epoch verified typestate. Record-kind-`8` cutover
remains a synchronous current-head operation.

## Writer ownership and acknowledgement

The `QWAL` frame format does not encode writer ownership. Before opening a raw
WAL, the runtime atomically creates the canonical-path sidecar lease specified
in the [Local storage contract](storage.md). A segmented directory instead has
one manager lease and rejects raw writers for its member files. Lease framing
has its own `QLCK` magic and version and is not a journal record.

`SyncAll` is the default append policy. A receipt states which configured
barrier returned successfully; it does not alter frame bytes. Any partial write
or failed acknowledgement barrier poisons the in-process writer. Reopening
verifies the physical log: an ambiguous complete frame is retained and replayed,
while repair mode may truncate only an incomplete final frame.

## Primary-source provenance

- CRC-32C uses the Castagnoli procedure in
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Gregorian date strings at system boundaries are governed by
  [ISO 8601-1:2019](https://www.iso.org/standard/70907.html); the ledger wire
  value is its own signed epoch-day scalar rather than an ISO character string.
- `DisplayQty`/maximum-show semantics and native-iceberg order identity are
  grounded in the
  [CME Globex Reference Guide](https://www.cmegroup.com/content/dam/cmegroup/globex/files/GlobexRefGd.pdf)
  and [CME Market by Order FAQ](https://www.cmegroup.com/articles/faqs/market-by-order-mbo.html).
  The precise FIFO-tail refresh rule remains Quotick's versioned internal
  contract because reserve priority and feed behavior vary by venue.
