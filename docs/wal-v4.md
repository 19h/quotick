# WAL Format Version 4

This document is the authoritative byte-level schema for Quotick WAL version
4. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 4 retains the version-3 payload bytes for record kinds `1` through `8`
and adds call-auction record kinds `9` and `10`. The runtime accepts only
version 4. Versions `1`, `2`, and `3` are expired envelopes and are rejected;
the [version-3 schema](wal-v3.md) is retained only as historical provenance for
the unchanged payloads incorporated here.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `4` |
| 6 | 2 | record kind |
| 8 | 4 | payload length `u32` |
| 12 | 4 | CRC-32C `u32` |
| 16 | 8 | contiguous journal sequence `u64` |
| 24 | payload length | typed payload |

Record-kind tags are continuous matching command `1`, continuous matching
execution report `2`, ledger entry `3`, instrument definition `4`, account risk
definition `5`, ledger correction `6`, ledger batch `7`, checkpoint anchor `8`,
call-auction command `9`, and call-auction execution report `10`.

CRC-32C uses the reflected Castagnoli polynomial `0x82F63B78`, initial state
`0xFFFFFFFF`, and final XOR `0xFFFFFFFF`. It covers the complete header with
bytes 12--15 zeroed, followed by the payload. CRC-32C detects accidental
corruption; it is not an authentication mechanism.

The version-2 segmented-directory marker, canonical segment naming, global
sequence rules, writer lease, tail-repair boundary, and checkpoint-cutover
selector are unchanged from the version-3 schema. Every frame in the selected
generation nevertheless carries envelope version `4`.

## Common scalar notation

- `u8`, `u32`, `u64`, `u128`, `i32`, `i64`, and `i128` are fixed-width.
- `bool` is `u8`: false `0`, true `1`; all other values are invalid.
- Identifiers are validated non-zero `u64` values.
- `Price` is signed raw `i64` instrument quanta.
- `Quantity` is a validated non-zero `u64` lot count.
- `TimestampNs` is unsigned Unix time in nanoseconds as `u64`.
- A variable collection is a `u32` count followed by exactly that many values.

## Record kinds 1 through 8

The instrument-definition, account-risk-definition, continuous matching
command/report, ledger entry/correction/batch, and checkpoint-anchor payloads
are byte-for-byte those specified in the [version-3 schema](wal-v3.md). Their
tags and semantic validation are unchanged. Version 4 changes only the outer
envelope version and introduces kinds `9` and `10`; it does not reinterpret an
expired version-3 frame.

## Call-auction command payload, kind 9

The first `u8` selects phase control `0`, submit `1`, cancel `2`, or uncross
`3`. Fields follow in the exact order below.

| Tag | Ordered fields after the tag |
|---:|---|
| 0 | command ID, instrument ID, instrument version, auction ID, expected phase revision `u64`, target phase `u8`, receive timestamp |
| 1 | command ID, auction ID, expected phase revision `u64`, auction order, receive timestamp |
| 2 | command ID, instrument ID, instrument version, account ID, order ID, receive timestamp |
| 3 | command ID, instrument ID, instrument version, auction ID, expected phase revision `u64`, minimum price `i64`, maximum price `i64`, reference price `i64`, pressure rule `u8`, final tie break `u8`, remainder policy `u8`, self-trade policy `u8`, receive timestamp |

Phase tags are closed `0`, collecting `1`, and frozen `2`. An auction order is
order ID, account ID, instrument ID, instrument version, side, constraint, and
quantity. Side tags are buy `0` and sell `1`. Constraint tags are market `0`
and limit `1` followed by raw price `i64`.

Pressure-rule tags are ignore `0` and favor imbalance `1`. Final-price tie
break tags are lower `0` and higher `1`. Remainder-policy tags are retain all
`0`, cancel market `1`, and cancel all `2`. The only represented self-trade
policy is permit `0`. An uncross band must have `minimum <= maximum`; tick,
collar, reference, route, cycle, and phase validation occur against recovered
instrument/engine state.

## Call-auction execution-report payload, kind 10

Fields are command ID `u64`, non-zero command sequence `u64`, outcome, event
count `u32`, ordered events, and replay `bool`. Outcome is accepted `0`, or
rejected `1` followed by a rejection reason. A canonical durable report always
has replay false: exact retries are served from reconstructed history without
new WAL frames.

Every event begins with non-zero event sequence `u64`, command ID `u64`,
occurrence timestamp `u64`, and event-kind tag:

| Tag | Event and ordered fields |
|---:|---|
| 0 | phase changed: auction ID, previous phase, current phase, revision `u64` |
| 1 | order accepted: order snapshot |
| 2 | order cancelled: order snapshot, cancellation reason `u8` |
| 3 | trade: trade ID, buy order ID, buy account ID, sell order ID, sell account ID, raw price `i64`, quantity |
| 4 | remainder cancelled: order ID, account ID, side, constraint, remaining quantity, already-executed lots `u64` |
| 5 | uncross completed: auction ID, clearing state, remainder policy, self-trade policy, trade count `u64`, cancellation count `u64`, book revision `u64`, phase revision `u64` |
| 6 | command rejected: rejection reason |

An order snapshot is order ID, account ID, side, constraint, quantity, and
non-zero priority sequence `u64`. Cancellation-reason tags are user requested
`0` and uncross remainder `1`. A tag-`2` accepted cancellation uses only user
requested; an uncross remainder uses tag `4`.

A clearing state is raw clearing price `i64`, aggregate buy quantity `u128`, and
aggregate sell quantity `u128`. Executable quantity and absolute imbalance are
derived by checked comparison/subtraction; executable quantity must be
positive. A trade cannot name one order on both sides. For a remainder
cancellation, `remaining quantity + already-executed lots` must fit `u64`.

Rejection-reason tags are:

| Tag | Meaning and optional fields |
|---:|---|
| 0 | wrong instrument |
| 1 | wrong instrument version |
| 2 | phase revision mismatch: observed `u64`, current `u64` |
| 3 | invalid phase transition: source phase, target phase |
| 4 | action not allowed: action, phase |
| 5 | auction ID mismatch: observed ID, current-present `bool`, optional current ID |
| 6 | auction ID is not next: expected ID, observed ID |
| 7 | instrument admission error tag |
| 8 | duplicate order |
| 9 | unknown order |
| 10 | not order owner |
| 11 | no executable interest |
| 12 | risk profile missing |
| 13 | risk account blocked |
| 14 | risk reduce-only violation |
| 15 | risk per-order quantity limit |
| 16 | risk per-order conservative notional limit |
| 17 | risk aggregate open-order count limit |
| 18 | risk aggregate open-quantity limit |
| 19 | risk aggregate conservative open-notional limit |
| 20 | risk worst-case position limit |
| 21 | risk arithmetic overflow |

Action tags are phase control `0`, submit `1`, cancel `2`, and uncross `3`.
Instrument-admission-error tags are wrong instrument `0`, wrong version `1`,
continuous trading state disallows entry `2`, off-tick price `3`, outside-collar
price `4`, off-grid quantity `5`, outside-limits quantity `6`, reserve unsupported
`7`, display quantity off grid `8`, display quantity not below order `9`, and
reserve replenishment limit `10`. The continuous-state error is retained as a
stable domain union member; the auction engine's route/phase validation governs
call-auction admission.

Tags `12`--`21` are emitted only by the coupled call-auction/risk state
machine. They remain part of the stable report union and checkpoint history.
The plain durable call-auction runtime has no profile metadata and therefore
cannot reproduce a WAL containing those outcomes; such a history fails its
deterministic replay check rather than being interpreted as non-risk state.

Decoded report validation requires a non-empty, contiguous, command-bound event
trace. A rejected report has exactly one matching tag-`6` event. An accepted
report is exactly one valid phase change, one order acceptance, one
user-requested cancellation, or one uncross trace. An uncross trace contains
one or more contiguous trade IDs first, then exactly the declared number of
remainder cancellations, then one completion. Every trade price equals the
clearing price, trade quantities sum exactly to executable quantity, declared
counts equal the body, and both final revisions are non-zero.

## Durable Call-Auction Journal Grammar

An uncut call-auction shard contains exactly one instrument definition at
physical sequence `M` followed by zero or more kind-`9`/kind-`10`
command/report pairs. No risk metadata, continuous-matching record, ledger
record, checkpoint anchor after the first frame, or second definition is
admitted. At most one final kind-`9` command may lack its report after
termination.

Uncut checkpoint-assisted open requires a snapshot-version-4 kind-`4`
checkpoint whose definition sequence is `M`. Every physical command/report
frame through checkpoint generation `G` must byte-decode to the corresponding
semantic history value; a mismatch or checkpoint newer than the verified WAL
fails recovery. The indexed engine is restored from the checkpoint, and only
frames after `G` execute.

A compacted call-auction shard instead begins at sequence `G` with one kind-`8`
checkpoint anchor naming `CallAuctionCheckpoint` kind `4`. The anchor physical
sequence and stored WAL sequence both equal `G`. Its A/B slot, semantic
generation, payload length, and checksum must identify the exact selected
snapshot, whose immutable definition must equal the requested definition.
Recovery never probes or guesses the alternate slot. Kind-`9`/kind-`10` suffix
pairs and at most one dangling kind-`9` command may follow the anchor.

Without a checkpoint, open validates the immutable definition, submits every
command to a fresh bounded engine, and requires exact equality with its
following persisted report. With either verified checkpoint form, open restores
the checkpointed engine directly and applies only the suffix. A reproduced or
persisted replay report is noncanonical and fails recovery. A final dangling
non-retry command is submitted once and its deterministic report is appended. A
dangling exact retry fails recovery instead of adding a report. Report
divergence, consecutive commands, a report without a command, definition drift,
unexpected kinds, anchor/checkpoint mismatch, and capacity/invariant failure all
fail closed.

Runtime submission performs complete engine preparation before appending kind
`9`, commits the same move-only preparation, then appends kind `10`. Failure
after command acknowledgement poisons that process instance; reopen resolves
the durable prefix. Exact retries are resolved before append and add zero
frames. Acknowledgement strength is the configured buffered, data-sync, or
full-sync journal policy.

The same logical grammar applies to a single file and to the selected global
sequence across version-2 segmented storage. Cutover first synchronizes the
inactive A/B snapshot slot, then atomically replaces the single WAL or publishes
a new segmented generation containing the exact anchor. Subsequent cutover
alternates slots. The [snapshot-version-4 schema](snapshot-v4.md) defines the
kind-`4` payload, lineage, direct reconstruction, and remaining semantic-history
cost.

## Durable Coupled Call-Auction/Risk Journal Grammar

An uncut coupled shard begins with one instrument definition at physical
sequence `F`, followed by `N` kind-`5` account-risk definitions in strictly
increasing `AccountId` order. The final metadata sequence is:

```text
M = F + N.
```

Zero or more kind-`9`/kind-`10` auction command/report pairs follow `M`.
Metadata after the first command, duplicate/noncanonical profiles, a command
before the complete requested profile set, and every continuous-matching or
ledger kind are invalid. If termination leaves only an exact prefix of the
requested canonical profile set and no command has begun, recovery appends the
missing profile frames before accepting commands. At most one final kind-`9`
command may lack its report.

Full replay constructs `CallAuctionRiskManagedEngine` from the requested
definition and exact persisted profiles. Every command is submitted through
core-first risk admission and must reproduce its complete report, including
risk rejection tags `12`--`21`. A final dangling non-retry command is completed
once and its report appended. Exact retries add no frames and a persisted paired
or dangling retry is noncanonical.

Uncut checkpoint-assisted open accepts only snapshot-version-4 kind `5`. The
checkpoint must bind `F`, `M`, the immutable definition, and exactly the
requested canonical profile set. Every command/report frame through generation
`G` must equal the nested checkpoint history before direct restoration; only
the suffix after `G` executes.

Plain and coupled durable auction runtimes may synchronize through a completed
report boundary `G`, capture a nonencodable canonical candidate without command
execution, and verify that exact prefix off-thread while later command/report
pairs append. Standalone publication accepts only the verified typestate through
the same open shard and unchanged process-local cutover epoch; coupled
publication also rechecks `F` and `M`. Another shard, reopen, metadata drift, a
checkpoint ahead of the WAL head, or successful prefix cutover is rejected
before snapshot creation. A staged handle cannot drive kind-`8` cutover.

A compacted coupled shard begins at `G` with a kind-`8` anchor naming
`CallAuctionRiskCheckpoint` kind `5`. The selected A/B snapshot must match the
anchor kind, slot, generation, payload length, checksum, definition, WAL
origin, metadata boundary, and profile set. Kind-`9`/kind-`10` suffix pairs and
at most one dangling command may follow. The same cutover protocol applies to
single-file and marker-selected segmented storage.

Plain and coupled auction grammars are deliberately disjoint. Plain recovery
rejects profile metadata; coupled recovery requires the complete profile
prefix. This prevents a risk-controlled report history from being interpreted
under a non-risk state machine.

## Compatibility boundary

There is no implicit upgrade or field inference across envelope versions.
Versions `1`, `2`, and `3` fail before payload interpretation. If an
authoritative version-3 deployment is discovered, migration requires an
explicit provenance-preserving converter that emits independently verifiable
version-4 frames; changing the version bytes in place is invalid because the
CRC covers the envelope.
