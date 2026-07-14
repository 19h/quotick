# Semantic snapshot format version 2

Version 2 is retained as historical provenance for payload kinds `1` through
`3`. Snapshot version 3 subsequently preserved those payload bytes and added
call-auction kind `4`. The current runtime rejects both expired envelopes and
uses [snapshot version 4](snapshot-v4.md), which also adds coupled
call-auction/risk kind `5`.

`SnapshotFile` stores a complete typed semantic value in a bounded, versioned,
CRC-32C envelope. Version 2 assigns payload kind `1` to `LedgerCheckpoint`,
kind `2` to `OrderBookCheckpoint`, and kind `3` to
`RiskManagedCheckpoint`. The payload trait is sealed, so downstream codecs
cannot claim a reserved kind. All integers are little-endian.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `2` |
| 6 | 2 | typed payload kind: ledger checkpoint `1`, matching checkpoint `2`, coupled risk checkpoint `3` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16–19 set to zero, followed by
the exact payload. The physical file length must equal `28 B + payload length`.
The default payload limit is 1 GiB (1,073,741,824 B); a caller can select a
smaller or larger `u64` limit. The limit is checked before allocation on read
and before any filesystem mutation on write.

The checksum detects accidental corruption. It is not a message-authentication
code and does not protect against an actor that can rewrite both payload and
checksum.

## Ledger-checkpoint payload

The ledger payload is:

| Order | Width | Field |
|---:|---:|---|
| 1 | 8 B | checkpoint generation `u64` |
| 2 | 4 B | non-zero balance count `u32` |
| 3 | 32 B each | account ID `u64`, asset ID `u64`, signed amount `i128` |
| 4 | 4 B | ledger-record count `u32` |
| 5 | variable | for each record: encoded length `u32`, record tag `u8`, then the tagged payload |

A `JournalEntry` payload is `47 B + 32 B × posting count`: transaction ID,
source reference, optional signed epoch-day effective date, recorded-at Unix
nanoseconds, posting count and postings, followed by lifecycle kind, optional
related transaction, and optional signed epoch-day period boundary. Financial
entries contain at least two postings; period close/reopen controls contain
zero. The complete byte schema and lifecycle tags are defined in
[WAL format version 4](wal-v4.md).

Record tag `0` contains one `JournalEntry`. Record tag `1` contains one
`LedgerCorrection`: reversal length `u32` and entry payload, followed by
replacement length `u32` and entry payload. Record tag `2` contains one
`LedgerBatch`: entry count `u32`, then one length-prefixed entry payload per
member in authoritative order. Unknown tags are invalid. For `B` non-zero
balances, `S` single-entry records, `C` correction records, `G` batch records,
`E_G` entries inside batch records, and `L` total posting legs across all
contained entries, the ledger payload length is

```text
P = 16 B + 32 B × B + 52 B × S + 107 B × C
    + 9 B × G + 51 B × E_G + 32 B × L.
```

The total snapshot length is `28 B + P`. Declared collection counts are checked
against the remaining payload using their minimum valid encoded sizes before
collection allocation; integer size conversions and framing additions are
checked. Allocator failure remains outside the recoverable model (A12).

## Matching-checkpoint payload

The matching payload is:

| Order | Width | Field |
|---:|---:|---|
| 1 | 8 B | final immutable-metadata sequence `M`; for standalone matching this is the first global sequence `F`, occupied by the definition |
| 2 | 8 B | completed execution-report WAL boundary `G` |
| 3 | variable | definition length `u32`, then the complete instrument-definition payload |
| 4 | 4 B | active-order count `u32` |
| 5 | 43 B or 51 B each | order ID `u64`, account ID `u64`, side `u8`, signed raw price `i64`, total leaves `u64`, displayed leaves `u64`, display policy, and STP policy `u8` |
| 6 | 4 B | completed command/report count `C` as `u32` |
| 7 | variable | for each chronological pair: command length `u32` and command payload, then report length `u32` and report payload |

Display policy is fully displayed tag `0` with no value or reserve tag `1`
followed by peak lots `u64`. The order sizes above therefore differ by 8 B.
Command, report, event, display, side, and STP fields use the exact tags in
[WAL format version 4](wal-v4.md).

Active orders are canonicalized as all buys then all sells, with ascending raw
price within each side and FIFO order within a price. This storage order is not
market-priority order. Intrusive previous/next links and level aggregates are
not persisted: restoration reconstructs them and audits the resulting book.
Every private field required by future matching is retained, including hidden
total leaves, the current displayed slice, reserve peak, owner, and the resting
STP policy used by replacement.

Revisioned account admission controls are derived from accepted command/report
history and are not duplicated as a separate checkpoint collection. Validation
requires each accepted control's expected revision to equal its account's prior
revision, the final completion event to carry the exact prior/resulting states
and incremented revision, and block-and-cancel aggregates to equal its ordered
cancellation events. A reconstructed blocked account cannot retain an order.

Effective instrument trading state is likewise derived from the definition's
genesis state plus accepted command/report history and is not duplicated as a
direct field. Validation requires an exact revision chain, a different target,
no transition-and-cancel into open, ascending state-control cancellation order,
exact cancellation aggregates, and one final event carrying the prior/target
states and incremented revision. Restoration materializes that derived
`(TradingState, revision)` before structural cross-audit.

The metadata boundary is followed by exactly one command frame and one report
frame per retained history entry, so a valid stable boundary satisfies

```text
G = M + 2 × C.
```

For standalone matching kind `2`, `M = F`; its encoded bytes and physical
equation are unchanged. All additions and conversions are checked. `G` cannot
point to a command frame or into a pair.

## Coupled risk-checkpoint payload

The risk payload is:

| Order | Width | Field |
|---:|---:|---|
| 1 | 8 B | first global WAL sequence `F`; the definition occupies this sequence |
| 2 | variable | embedded matching-checkpoint length `u32`, then the complete matching-checkpoint payload whose metadata sequence is `M` |
| 3 | 4 B | canonical account/profile count `A` as `u32` |
| 4 | 197 B each | account-risk-definition length `u32`, fixed 121 B account-risk-definition payload, current signed position `i128`, open-buy lots `u128`, open-sell lots `u128`, open notional `u128`, and open-order count `u64` |

Accounts are strictly ascending by account ID. The embedded matching image
retains each active order's owner, side, raw price, total leaves (including
hidden reserve leaves), displayed slice, display policy, and STP policy.
Reservations are not duplicated as another collection. Restore derives one
reservation from each embedded active order and requires the resulting
per-account buy lots, sell lots, absolute raw-price-times-lots notional, and
order count to equal the redundant account aggregates exactly.

The risk WAL contains the definition at `F`, then exactly `A` canonical
account-risk-definition frames, then `C` command/report pairs. Consequently:

```text
M = F + A
G = M + 2 × C = F + A + 2 × C.
```

Every addition and count conversion is checked. `G` is the embedded matching
generation and the `QSNP` envelope generation.

## Semantic validation

A ledger checkpoint is accepted only if all of the following hold:

1. The envelope generation equals the payload generation.
2. The generation equals the number of complete ledger records. A correction
   is one record containing two transaction entries; a batch is one record
   containing at least two.
3. Balances are non-zero and strictly ordered by `(asset ID, account ID)`;
   duplicate keys are therefore impossible.
4. Every financial entry passes canonical double-entry validation using exact
   positive/negative per-asset magnitudes with no fixed-width aggregate ceiling;
   every period control has the required zero-posting/absent-effective-date
   shape.
5. Replaying the complete record sequence succeeds without exact duplicate
   records, transaction-ID collisions, a partial correction/batch, timestamp
   regression, invalid reversal lineage, invalid close/reopen progression,
   closed-date posting, or arithmetic overflow.
6. Balances independently reconstructed by replay equal the redundant balance
   image exactly.

`Ledger::validate` additionally checks the live journal/index correspondence,
record and transaction sequences, deterministic replay, and independently
accumulated positive and negative totals for every asset, plus reconstructed
reversal, accounting-period, and last-recorded-time state.
`Ledger::checkpoint` runs that audit before capture. Zero balances are omitted
from the image, while the complete record sequence is retained to preserve
correction grouping, exact transaction idempotency, and reconstruct the period
fence. Ordered batch grouping and all member transaction identifiers are
retained by the same record history.

For ledger snapshots, generation lineage is the exact ledger-record prefix
relation. A generation `g₂` is a successor of `g₁` only when `g₂ ≥ g₁` and its
first `g₁` records equal the complete prior history. Numeric generation order
alone is insufficient.

A matching checkpoint is accepted structurally only when:

1. `M > 0` and `G = M + 2 × C` without overflow;
2. command identifiers are unique, every report is non-replayed and bound to
   its command, and events form one global sequence beginning at `1`;
3. every event timestamp equals its source command timestamp;
4. trade identifiers form one sequence beginning at `1` and every trade is
   bound to the checkpoint definition;
5. accepted order identifiers are unique and every active order was accepted;
6. active orders have unique identifiers, canonical side/price/FIFO order,
   definition-valid prices and leaves, and coherent displayed/reserve state;
7. account-control and instrument-state-control histories have contiguous
   revisions and exact cancellation/completion grammar;
8. reconstruction produces valid FIFO links, price-level aggregates, accepted-
   order membership, and an uncrossed book.
9. restoration under caller-selected `OrderBookLimits` proves retained command,
   accepted-ID, account-control, active-order, active-account, and per-side occupied-level
   cardinalities fit before allocating live indexes.

`OrderBook::checkpoint` first audits the live structure, captures the image,
then independently replays every retained command and requires every report and
the complete resulting `OrderBook` to equal live state. This capture-time replay
prevents publication of a structurally valid image that contradicts its own
history. Read-time decoding performs the structural checks above; it does not
repeat prefix matching. The envelope checksum detects accidental image changes.
An actor able to rewrite both image and checksum remains outside the authenticity
model under A14, A39, and A40.

Matching and registered-risk-profile limits are operational process policy, not
financial snapshot payload, and are therefore not encoded in snapshot kind `2`
or `3`. A kind-3 restore selects `RiskManagedLimits`, which embeds matching
limits and independently bounds canonical profiles. Equal or larger limits
preserve current state; insufficient limits fail restoration explicitly. A checkpoint can bypass
historical matching transitions, so only retained/current checkpoint
cardinalities are tested; raw WAL replay additionally exercises every retained
historical peak under the selected policy.

Matching snapshot lineage requires identical `M` (and therefore `F` for kind
`2`) and definition plus exact chronological command/report-prefix equality. A
numerically newer checkpoint on another command lineage is rejected.

A coupled risk checkpoint additionally requires:

1. `F > 0`, strictly canonical unique accounts, and `M = F + A` without
   overflow;
2. the embedded definition and every active order to bind to the same coupled
   shard, with every active owner present in the account set;
3. every profile to pass numerical and initial-position validation;
4. reservations reconstructed from total active leaves to equal all redundant
   open-exposure aggregates;
5. current and worst-case positions to remain within immutable limits;
6. replay of every retained command through `RiskManagedOrderBook` to reproduce
   every report, including risk-generated rejections; and
7. replayed matching state, positions, and reservations to equal the direct
   image exactly.

Risk checkpoint lineage requires identical `F`, definition, canonical account
IDs/profiles, embedded metadata boundary, and chronological command/report
prefix. Current exposures may differ between generations because they are
derived state proved by the extended history.

## Replacement protocol

Snapshot mutation uses the canonical target path and the same 34 B `QLCK`
sidecar lease as a raw WAL. A normal replacement performs:

1. Encode and bound the semantic payload.
2. Canonicalize the target and reject a parent carrying the segmented-WAL
   `format.qseg` marker.
3. Acquire `<target>.writer.lock` by exclusive creation.
4. Refuse normal publication if `<target>.pending` already exists.
5. Validate any current snapshot. Reject generation regression,
   same-generation divergence, or a newer value that does not extend the
   current lineage. A byte-identical same-generation write is a no-op.
6. Exclusively create `<target>.pending`, write the complete header and payload,
   and call `sync_all` on that file.
7. Rename the pending file over the current target, call `sync_all` on the
   parent directory, and release the lease.

On POSIX.1-2024 systems, same-filesystem `rename` atomically replaces the
directory entry, so concurrent namespace observers see the prior or replacement
file rather than an intermediate name. Rust documents that `std::fs::rename`
replaces an existing target and fails across mount points. Persistence after
power loss remains conditional on the qualified filesystem, mount, controller,
and device behavior despite successful file and directory barriers.

A direct `SnapshotFile` caller must dedicate the target and its `.pending` and
`.writer.lock` sidecars to snapshot use. `DurableOrderBook::write_checkpoint`,
`DurableRiskOrderBook::write_checkpoint`, and
`DurableLedger::write_checkpoint` additionally reject aliases of their single
WAL, lease, and `.cutover.pending` namespace and reject every path inside their managed segmented-WAL
directory.

## Two-slot WAL-cutover checkpoints

`compact_to_checkpoint` derives `<base>.cutover-a` and
`<base>.cutover-b`. It always writes the slot not referenced by the current
WAL anchor. Each slot is an ordinary independently framed version-2
`QSNP` file with its own `.pending` and writer-lease sidecars; no alternate
snapshot envelope is introduced. The synchronized `SnapshotReceipt`
generation, payload length, and complete envelope checksum are copied into the
version-4 WAL anchor together with the selected slot and the independent
physical WAL boundary.

The inactive slot is published before the physical WAL selector changes. A
single-file selector change renames the anchor file over the WAL; a segmented
selector change renames a CRC-32C-protected next-generation marker after its
anchor segment is synchronized. Therefore a crash before selection leaves the
prior WAL/checkpoint pair authoritative, and a crash after a directory-
synchronized selector change leaves the new pair authoritative. Repeated
cutovers alternate slots, so the currently selected snapshot is never
overwritten before the WAL selects its successor.
Open reads only the slot named by the anchor and fails on missing, malformed,
wrong-kind, wrong-generation, wrong-length, or wrong-checksum content. An
unselected valid or invalid slot cannot influence recovery.

## Explicit pending recovery

Normal writes never overwrite an abandoned pending file. After independently
proving writer termination and quiescing new starts, the caller first resolves
an abandoned valid or malformed lease through the explicit lease-recovery API,
then invokes `SnapshotFile::recover_pending`.

| Pending/current state | Result | Filesystem effect |
|---|---|---|
| no pending file | `NoPendingSnapshot` | none |
| truncated, bad-magic, length-inconsistent, checksum-invalid, generation-inconsistent, or codec-invalid pending | `DiscardedInvalid` | remove pending; synchronize parent |
| unsupported-version, unexpected-kind, or configured-size-disallowed pending | original error | preserve pending and current |
| valid pending; no current | `Promoted` | rename pending to current; synchronize parent |
| valid newer pending extending current | `Promoted` | replace current; synchronize parent |
| valid newer pending on another lineage | `LineageDivergence` | preserve both |
| valid older pending that is a prefix of current | `DiscardedStale` | remove pending; synchronize parent |
| valid older pending on another lineage | `LineageDivergence` | preserve both |
| byte-identical current and pending | `DiscardedRedundant` | remove pending; synchronize parent |
| same generation with different content | `SameGenerationDivergence` | preserve both |
| invalid current | `CurrentSnapshotInvalid` or the original policy/resource error | preserve both |

Lease recovery has the same external owner-termination and quiescence
preconditions as WAL lease recovery. The owner comparison and deletion are not
an atomic compare-and-delete operation.

## Durable-matching checkpoint recovery

`DurableOrderBook::write_checkpoint` rejects poisoned state and path conflicts,
synchronizes the WAL, captures and independently replay-audits matching state,
then publishes the snapshot. For an uncut WAL, `open_with_checkpoint` and
`open_segmented_with_checkpoint`:

1. acquire WAL ownership and complete physical recovery;
2. require the WAL and checkpoint to carry the identical first sequence and
   immutable definition;
3. read and structurally validate the checkpoint;
4. stream the complete verified WAL and compare each retained command/report
   with the exact frame at that global sequence;
5. reject any kind/content divergence or a checkpoint ahead of the WAL;
6. prove retained/current cardinalities fit the selected matching limits, then
   reconstruct active indices, FIFO links, counters, accepted-order membership,
   and the exact retry cache directly from the checkpoint;
7. deterministically replay only complete command/report pairs after `G`;
8. complete at most one final command lacking its report; and
9. run the complete live-book invariant audit.

For an anchored WAL in either physical layout, checkpoint-assisted open
validates the first anchor against its selected A/B slot, restores the same
fully audited checkpoint state, and replays only frames after the retired
prefix boundary. A segmented open selects exactly the generation named by the
CRC-valid directory marker. Opening an anchored WAL without a checkpoint base
fails explicitly.

For `W` verified WAL bytes across `S` segments, `C` retained checkpoint
commands, `O` active orders over `P` price levels, and `N` suffix commands, open
uses `O(W + C + O log P + suffix matching work)` time. It does not perform the
matching transitions for the first `C` commands. Memory remains
`O(min(C,C_max) + O + P + S)` because exact retries and never-reusable accepted
order IDs retain complete history only through the finite generation bounds
under A9/A39/A46. Capture performs one independent complete-history replay
plus `O(O + P)` structural audit synchronously under exclusive shard ownership.
These are cold-path costs, but they create an `O(C)` capture pause until a
generation-fenced immutable/COW handoff is implemented and verified.

## Durable-risk checkpoint recovery

`DurableRiskOrderBook::write_checkpoint` rejects poisoned state and path
conflicts, synchronizes the WAL, captures canonical matching/account state,
derives and cross-checks reservations, independently replays the complete
history through the coupled risk/matching state machine, and publishes only an
exact live-state image. For an uncut WAL, `open_with_checkpoint` and
`open_segmented_with_checkpoint`:

1. acquire WAL ownership and complete physical recovery;
2. prove the checkpoint's `F`, `M`, definition, and canonical immutable profile
   set against the WAL metadata grammar;
3. decode and semantically validate the complete checkpoint, including coupled
   replay of risk rejections and position effects;
4. stream the verified WAL and compare every checkpoint command/report with
   the exact frame at its global sequence;
5. reject kind/content divergence or a checkpoint ahead of the WAL;
6. prove the canonical profile count and matching cardinalities fit the selected
   coupled resource policy, then reconstruct matching indices, FIFO/reserve/STP state, profiles, executed
   positions, total-leaves reservations, exposure aggregates, and retry caches;
7. replay only complete coupled command/report pairs after `G`;
8. complete at most one final command lacking its report; and
9. run the complete matching/risk cross-audit.

For an anchored risk WAL in either physical layout, the checkpoint itself supplies and is
validated against the original `F`, metadata boundary `M`, immutable
definition, and canonical profile set. Recovery then reads only the anchor and
suffix frames. The A/B protocol never rewrites the slot selected by the current
WAL.

For `W` verified WAL bytes across `S` segments, `C` retained checkpoint
commands, `O` active orders over `P` price levels, `A` accounts, and `N` suffix
commands, open uses `O(W + C + O log P + A + suffix matching/risk work)` time
and `O(C + O + P + A + S)` memory. Capture performs one complete coupled replay
plus structural and exposure audits synchronously under exclusive shard
ownership. It therefore has an `O(C)` admission pause and retains `O(C)`
history. Uncut recovery scans `O(W)` bytes; anchored recovery in either physical
layout replaces that term with the compacted suffix bytes. The finite
matching limits bound current in-process history but no automatic shard
generation rollover is established.

## Durable-ledger checkpoint recovery

`DurableLedger::write_checkpoint` first rejects poisoned state and path
conflicts, synchronizes the WAL with `sync_all`, audits the live ledger, and
then publishes the snapshot. For an uncut WAL, `open_with_checkpoint` and
`open_segmented_with_checkpoint`:

1. acquire WAL writer ownership and complete ordinary WAL recovery;
2. read and semantically validate the checkpoint;
3. reconstruct the in-memory ledger from the validated checkpoint;
4. stream the complete verified WAL and compare every checkpoint record with
   the corresponding exact WAL prefix record;
5. reject a checkpoint ahead of the WAL or any prefix divergence;
6. apply only WAL records after the checkpoint generation; and
7. run the complete live-ledger invariant audit.

For an anchored ledger WAL in either physical layout, the anchor independently stores
semantic record generation and physical WAL sequence. Recovery restores the
selected A/B checkpoint, initializes the verified record count from its
semantic generation, then applies only suffix frames. This remains correct for
a WAL whose configured first sequence is not `1`.

Uncut recovery retains and scans the complete WAL. If `W` is
verified WAL bytes, `S` physical segments, `R` checkpoint records, and `N` WAL
records after the checkpoint, open remains `O(W + R + N)` time. The segmented
reader uses `O(S)` descriptors and one bounded frame payload; the restored
ledger and checkpoint payload require state proportional to retained balances
and complete checkpoint history. Snapshot construction and validation are
linear in retained balances and record/entry/posting history, apart from
logarithmic ordered-map/set factors inside accounting validation.

Version 2 checkpoint payloads still retain complete semantic history. Version
3 WAL cutover in either physical layout removes prefix bytes from subsequent
WAL scans and disk occupancy while preserving that history in the selected checkpoint.
Consequently this establishes bounded physical WAL suffix recovery only under
the selected snapshot payload limit; it does not establish bounded ledger
checkpoint memory, bounded retained audit/idempotency history, semantic
generation rollover, or external archival continuity. Those require separately
fenced lifecycle and external audit/idempotency proofs.

## Primary-source provenance

- CRC-32C uses the Castagnoli procedure in
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Exclusive lease and pending creation use Rust
  [`OpenOptions::create_new`](https://doc.rust-lang.org/stable/std/fs/struct.OpenOptions.html#method.create_new).
- File and directory barriers use Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all),
  with persistence bounded by [POSIX `fsync`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html).
- Replacement uses Rust
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html) and
  relies on the atomic same-filesystem namespace semantics specified by
  [POSIX.1-2024 `rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).

The `QSNP` framing, payload-kind registry, matching and ledger payload layouts,
lineage rules, and recovery matrix are Quotick internal contracts rather than
external standards.
