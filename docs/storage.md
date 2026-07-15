# Local storage contract

This document bounds the guarantees of `Journal`, `SegmentedJournal`,
`SnapshotFile`, `DurableOrderBook`, `DurableRiskOrderBook`, and `DurableLedger`.
The durable runtimes expose both single-file `open` and directory-backed
`open_segmented` constructors. It distinguishes properties proved by the state
machines from properties conditional on the operating system, filesystem, and
storage device.

## Contents

- [Canonical-path writer ownership](#canonical-path-writer-ownership)
- [Segmented directory ownership and format](#segmented-directory-ownership-and-format)
- [Abandoned writer recovery](#abandoned-writer-recovery)
- [Semantic snapshot ownership and replacement](#semantic-snapshot-ownership-and-replacement)
- [Checkpoint WAL cutover](#checkpoint-wal-cutover)
- [File and directory durability](#file-and-directory-durability)
- [Failure and recovery matrix](#failure-and-recovery-matrix)
- [Unimplemented storage properties](#unimplemented-storage-properties)

## Canonical-path writer ownership

This section defines how a single-file WAL writer acquires, holds, and
releases exclusive ownership through a lease sidecar.

`Journal::open` first canonicalizes an existing WAL path. For a new WAL it
canonicalizes the existing parent directory and joins the requested file name.
It then atomically creates `<canonical WAL path>.writer.lock` with
`OpenOptions::create_new(true)`. Rust specifies `create_new` as atomic and as
failing when the target already exists ([Rust `OpenOptions::create_new`](https://doc.rust-lang.org/stable/std/fs/struct.OpenOptions.html#method.create_new)).

The 34-byte lease record is:

| Offset | Width | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QLCK` |
| 4 | 2 | lease version `1`, little-endian |
| 6 | 4 | process ID `u32` |
| 10 | 16 | acquisition Unix time in nanoseconds `u128` |
| 26 | 8 | process-local acquisition nonce `u64` |

Lease creation writes the complete identity, calls `sync_all` on the lease, and
calls `sync_all` on its parent directory before WAL access. A second writer for
the same canonical path receives `WriterLeaseHeld`; read-only
`JournalReader`s do not acquire the lease. Malformed or truncated leases fail
closed.

Normal `Drop` removes a lease only when its on-disk owner still equals the
instance owner, then attempts to synchronize the parent directory. Because
`Drop` cannot return I/O failures, `Journal::close` is the authoritative clean
shutdown path: it synchronizes WAL data and metadata, removes the lease, and
synchronizes the directory removal. Durable matching, risk, and ledger wrappers
expose equivalent `close` operations.

This is canonical-path exclusion, not an inode or distributed lock. A writable
directory participant that introduces a hard-link alias, replaces path
components, or deletes a live lease can violate it. Network filesystems are
unsupported until their exclusive-create and cache-coherence behavior is
qualified.

## Segmented directory ownership and format

This section defines the on-disk contents of a segmented WAL directory, its
single manager lease, its marker format, and its rotation behavior.

### Manager lease

`SegmentedJournal` manages one dedicated canonical directory under a single
manager lease. The manager lease uses the same 34-byte `QLCK` record and
exclusive-create protocol as a raw journal; its path is
`.quotick-segments.writer.lock`. Raw `Journal` writers reject files whose parent
contains the segmented marker, so per-file writers cannot bypass the manager.
Readers do not acquire the manager lease.

### Directory inventory

The directory inventory is deliberately closed:

| Entry | Meaning |
|---|---|
| `format.qseg` | authoritative 46-byte CRC-32C generation selector and immutable configuration |
| `format.qseg.pending` | synchronized next-generation marker staged during cutover |
| `.quotick-segments.writer.lock` | live manager ownership, present only while owned or abandoned |
| `.quotick-segments.cutover.pending` | first staged next-generation segment |
| `segment-GGGGGGGGGGGGGGGGGGGG-SSSSSSSSSSSSSSSSSSSS.qwal` | WAL segment; `G` is its physical generation and `S` its first global sequence |

### Marker format

The marker uses little-endian integers:

| Offset (bytes) | Width (bytes) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSEG` |
| 4 | 2 | marker version `2` |
| 6 | 8 | maximum physical segment bytes `u64` |
| 14 | 8 | immutable lineage-origin sequence `u64` |
| 22 | 4 | maximum frame payload bytes `u32` |
| 26 | 8 | active physical generation `u64` |
| 34 | 8 | first retained global sequence in that generation `u64` |
| 42 | 4 | CRC-32C over the complete marker with this field zeroed |

The segment capacity includes `QWAL` headers and payloads, but not the marker or
lease. Capacity, lineage origin, and maximum payload are immutable and must
exactly equal the marker on reopen. The active generation starts at `1`; its
first retained sequence begins at the lineage origin and advances only through
checkpoint cutover. Acknowledgement and tail-recovery policies are runtime
policies and are not marker fields.

Each of the following conditions fails closed:

- unknown entries;
- noncanonical names;
- an absent marker in a nonempty directory;
- marker checksum failure;
- immutable configuration drift.

If termination interrupts the initial marker write before any segment exists,
`recover_incomplete_initialization` acquires the manager lease and removes only
an invalid marker in an otherwise empty persistent inventory. It refuses a
valid marker and refuses to act when any segment or unknown entry exists.

### Rotation

Rotation is size-triggered and sequence-preserving:

1. Encoding, payload, total-length, capacity, and sequence-space checks finish
   before filesystem mutation.
2. If the complete frame or acknowledgement batch does not fit a nonempty
   active segment, that segment is closed with `sync_all`.
3. The manager creates and synchronizes a segment named by the active generation
   and next global sequence and synchronizes the directory entry.
4. The complete frame or batch is appended to the new active segment. A batch
   is never intentionally split by rotation.

Within the marker-selected generation, sorted canonical names plus strict frame
scans derive one contiguous global sequence. A crash before new-file creation
leaves the prior file active. A crash after creation can leave one empty final
segment, which is valid and reused. Only the final selected segment can use
`RepairTornTail`; every earlier selected segment is opened strictly, and an
empty non-final segment is invalid. Corruption, truncation, oversize files, or a
sequence gap in the selected generation are never skipped or repaired.

### Generation selection and readers

Recognized non-selected generations and the two deterministic cutover staging
files are not part of the logical WAL. Read-only open ignores them. Writer open
first validates the complete marker-selected generation under the manager
lease, then removes inactive artifacts and synchronizes the directory. If the
selected generation is invalid, cleanup does not run and all other generation
files remain available for diagnosis.

`SegmentedJournalReader` captures the active generation selected by one valid
marker, streams its fixed inventory one file at a time, and verifies one global
contiguous sequence. Its retained memory overhead is `O(S)` for `S` active
segment descriptors plus one bounded frame payload; directory discovery is
`O(S + I)` for `I` inactive artifacts. It does not materialize the complete
WAL. Durable matching, risk, and ledger recovery use this streaming path while
holding manager ownership. A standalone reader does not provide an atomic
point-in-time snapshot of a concurrently appending active segment or cutover;
it either verifies its captured generation or returns an I/O/inventory error.

## Abandoned writer recovery

Process termination can leave a durable lease. Recovery is deliberately not
automatic:

1. Attempt `Journal::open` and retain the exact `WriterLeaseOwner` returned by
   `WriterLeaseHeld`.
2. Establish outside the library that the recorded process cannot write the WAL
   and that no alias names the same file.
3. Prevent any new writer from starting until recovery finishes, then call
   `Journal::recover_abandoned_writer` with the observed owner.
4. The operation rereads the lease and removes it only if the complete identity
   still matches, then synchronizes the parent directory.
5. Reopen and scan the WAL. Repair mode may discard only a physically incomplete
   final frame.

An owner mismatch is a compare failure and never removes the current lease.
Comparison and deletion are separate filesystem operations, so the quiesced
maintenance window is required; this is not an atomic compare-and-delete.

If termination interrupted initial lease emission, the malformed lease remains
fail-closed and `recover_abandoned_invalid_writer` removes it only after proving
that it does not decode as a supported owner. This operation has the same
liveness and quiescence preconditions. Process ID and nonce are diagnostic
correlation values, not credentials.

For a segmented directory, the equivalent operations are
`SegmentedJournal::recover_abandoned_writer` and
`SegmentedJournal::recover_abandoned_invalid_writer`. They act only on the
manager lease; individual managed segments do not carry writer leases.

## Semantic snapshot ownership and replacement

This section defines snapshot-target ownership, the staged pending-file
replacement protocol, and recovery of pending files left by a terminated
writer.

`SnapshotFile` canonicalizes its target and uses the raw-journal 34 B `QLCK`
sidecar protocol at `<target>.writer.lock`. It rejects every mutating operation
whose target parent carries `format.qseg`; the segmented directory inventory
therefore remains closed. Read-only snapshot access does not acquire a lease.

The replacement staging path is `<target>.pending` in the same directory. A
normal write refuses to proceed while that path exists. After validating
generation and exact semantic lineage against any current snapshot, the
writer:

1. exclusively creates the pending path,
2. writes the complete bounded `QSNP` file,
3. synchronizes it,
4. renames it over the target,
5. synchronizes the parent directory, and
6. releases the lease.

Rust specifies that `std::fs::rename` replaces an existing target and rejects a
cross-mount operation ([Rust `rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html)).
For a qualified POSIX-like filesystem, replacement of the same directory entry
is atomic: an observer sees the prior or new name binding
([POSIX.1-2024 `rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html)).
This namespace atomicity is distinct from persistence through power loss.

An incomplete or complete pending file can survive a terminated writer. After
resolving any abandoned lease under the same external liveness/quiescence
preconditions as WAL recovery, `recover_pending` validates both files and uses
semantic generation plus exact history lineage. It decides as follows:

- it promotes a newer successor;
- it removes a proven stale prefix or byte-identical duplicate;
- it removes provably malformed pending content;
- it preserves both files on divergence or invalid current state;
- a pending file rejected only because its version/kind is unsupported or its
  size exceeds the caller's configured limit is preserved.

The complete wire and decision contract is
[Semantic snapshot format version 7](snapshot-v7.md).

Direct users must dedicate the target and its two sidecars to snapshots.
`DurableOrderBook`, `DurableRiskOrderBook`, `DurableLedger`, and
`DurableCallAuctionEngine` additionally check that a checkpoint target, pending
path, and lease cannot alias their single-file WAL/lease or reside anywhere
inside their segmented directory. `write_checkpoint` synchronizes the WAL
before publishing an independently audited matching, coupled risk/matching,
ledger, or call-auction image.

## Checkpoint WAL cutover

This section defines how a checkpoint retires a WAL prefix: the anchor and
A/B slot protocol shared by both layouts, then the single-file and segmented
replacement sequences.

### Anchor and slot protocol

`compact_to_checkpoint` is implemented for both physical layouts of durable
matching, coupled risk/matching, ledger, and call-auction runtimes. Matching,
coupled continuous risk, plain call-auction, and coupled call-auction/risk also
accept an off-thread verified older checkpoint through
`compact_verified_checkpoint`. Each verified value carries a private physical
cursor recorded at its WAL barrier.

Cutover alternates deterministic `<checkpoint base>.cutover-a` and
`.cutover-b` snapshot slots. The inactive slot is published with the ordinary
`QSNP` pending-file, file-barrier, rename, and directory-barrier protocol
before the WAL selector changes. The anchor binds:

- snapshot kind,
- A/B slot,
- semantic generation,
- retired physical WAL sequence,
- encoded snapshot payload length, and
- the complete `QSNP` envelope CRC-32C.

For an older generation `G` with current head `H`, the replacement WAL is the
anchor at `G` followed by the verified original frames `G+1..H`; subsequent
appends continue at `H+1`. Ordinary reopen
without the checkpoint base fails explicitly; checkpoint-assisted reopen
validates every anchor field before applying suffix frames.

### Single-file layout

For a single-file WAL, the retained writer lease:

1. exclusively creates `<canonical WAL>.cutover.pending`,
2. writes the anchor,
3. streams and verifies only bytes after the captured offset,
4. re-encodes the unchanged suffix frames,
5. synchronizes the complete anchor-plus-suffix file, and
6. then renames that file over the WAL and synchronizes the parent
   directory.

Before that rename, the prior WAL and any slot it selects remain
authoritative. After its directory barrier, the anchor-selected WAL/slot pair
is authoritative and the prior slot remains intact.

An interrupted pre-rename attempt can leave the staging file. After
prior-writer termination is established and its lease is explicitly
recovered, `Journal::recover_abandoned_cutover_pending` removes that file
under a new exclusive lease and synchronizes the directory; it never promotes
staged content.

### Segmented layout

For a segmented WAL, the retained manager lease:

1. resumes at the captured segment and byte offset,
2. verifies only the later global frames,
3. repacks the anchor plus unchanged suffix into one or more bounded
   segments in the next non-zero physical generation,
4. synchronizes every new segment and its directory entry,
5. next writes and synchronizes `format.qseg.pending`,
6. atomically renames that CRC-32C-protected marker over `format.qseg`, and
7. synchronizes the directory.

The marker is the sole generation selector: before its rename the prior
generation is authoritative; afterward the next generation is authoritative.
Old-generation files and recognized staging artifacts are removed only after
the complete selected generation validates, and their removal is
directory-synchronized. An interrupted cleanup cannot resurrect or mix the
retired prefix. Reader open ignores inactive generations; writer open
validates the selected generation before cleaning them.

### Path dedication

Checkpoint path-conflict checks cover every layout-owned path and each A/B
target's snapshot pending path and writer lease. Snapshot targets must remain
outside a segmented WAL directory. All participating paths must remain
dedicated and on the same qualified local filesystem.

## File and directory durability

This section lists the synchronization barriers each storage operation
performs and what a successful barrier does and does not prove.

- Creating a WAL uses exclusive creation, `File::sync_all`, then
  parent-directory `sync_all`.
- Repairing a torn tail performs `set_len` followed by file `sync_all`.
- Lease creation and removal also synchronize their directory entry.
- Snapshot publication synchronizes the complete pending file before rename
  and synchronizes the parent after rename; pending removal and promotion
  also synchronize the parent.
- WAL cutover likewise synchronizes the complete anchor staging file and its
  canonical entry before changing the layout selector, synchronizes the
  selector's parent after publication, and synchronizes retired inventory
  removal.

Cursor-based cutover memory is bounded by one configured payload/frame buffer
plus the segmented suffix inventory; writer-held copy work is linear in
suffix bytes and suffix segments, not the retired prefix.

Append acknowledgement policies are:

| Policy | Barrier before receipt |
|---|---|
| `Buffered` | complete `write_all` only |
| `Flush` | `write_all`, then Rust `File::flush` |
| `SyncData` | `write_all`, then `File::sync_data` |
| `SyncAll` | `write_all`, then `File::sync_all` |

`SyncAll` is the default because WAL length and metadata are part of recoverable
file state. Rust documents `sync_all` as synchronizing content and metadata and
notes that close-time errors are otherwise ignored
([Rust `File`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all)).
POSIX specifies that `fsync` requests transfer to the associated storage device,
waits for completion or an error, and leaves transfer nature
implementation-defined ([POSIX `fsync`](https://pubs.opengroup.org/onlinepubs/009695399/functions/fsync.html)).

Consequently, a successful API barrier proves that the operating-system call
returned success. It does not by itself prove a storage device's power-loss
behavior. Filesystem, mount mode, controller cache, drive cache, virtualization,
and network-storage semantics require separate qualification with forced-power
loss and remount tests.

## Failure and recovery matrix

This section tabulates each failure point, the in-process state it leaves,
and how reopen interprets the resulting bytes.

| Failure point | In-process state | Reopen interpretation |
|---|---|---|
| validation/encoding before write | unchanged, healthy | no new bytes |
| partial `write_all` | poisoned | strict error; repair truncates only incomplete final frame |
| complete write, failed barrier | poisoned; acknowledgement failed | complete frame may be present and is replayed exactly once |
| failed explicit `sync_data`/`sync_all` | poisoned | scan verified prefix; caller treats last operation as ambiguous |
| partial grouped append | poisoned | every complete verified frame remains; incomplete suffix is repairable |
| complete frame with bad CRC/header/sequence | reopen fails | never repaired or skipped |
| rotation after prior close but before next-file creation | current instance fails/terminates | prior final segment is reopened as active |
| rotation after next-file creation but before append | current instance fails/terminates | one empty final segment is reused |
| termination during initial marker write | open reports invalid marker | explicit recovery removes it only when no segment/unknown entry exists |
| torn append in active segment | poisoned | strict error; repair can truncate only that final tail |
| any defect in a closed segment | reopen fails | never repaired, skipped, or converted into a new boundary |
| snapshot failure before pending creation | unchanged target | no pending file; lease cleanup is attempted |
| partial snapshot pending write or failed pending barrier | current target unchanged | pending requires explicit recovery and is discarded only after validation proves it malformed |
| complete pending file before rename | current target unchanged | explicit recovery promotes only a valid same-lineage successor |
| rename completed before parent barrier | namespace contains replacement if operation returned | power-loss persistence is filesystem/device conditional; reopen validates complete content |
| stale, redundant, or divergent pending generation | current target remains authoritative until recovery | stale/redundant is synchronized away; divergence preserves both and fails closed |
| checkpoint disagrees with WAL prefix or extends beyond WAL | durable open fails | no suffix is applied and no storage is mutated |
| cutover snapshot published before WAL replacement | prior WAL and any slot it selected remain authoritative | inactive slot is ignored and can be reused by a later cutover |
| single-file anchor staging write/barrier fails before rename | prior WAL remains authoritative; staging may remain | explicit staging recovery discards it under a newly acquired writer lease |
| single-file WAL rename completes but directory barrier fails | writer is poisoned and result is ambiguous | reopen accepts only a complete old or anchor-selected new WAL/checkpoint pair; storage qualification determines power-loss persistence |
| segmented anchor generation is staged before marker rename | prior marker generation remains authoritative | reader ignores the staged generation; validated writer open removes inactive artifacts |
| segmented marker rename completes but its directory barrier fails | writer is poisoned and selector persistence is ambiguous | reopen accepts whichever CRC-valid marker survived; both possible selected generations were fully synchronized before publication |
| verified cutover cursor ends inside a segment or exactly at a segment boundary | new generation contains the anchor plus every later verified frame | an empty remainder in the boundary segment is permitted; sequence verification resumes in the same or next segment without scanning the retired prefix |
| suffix migration fails before selector publication | prior WAL/generation remains authoritative | staged output is never promoted; the writer is poisoned when segmented staging state requires reopen cleanup |
| segmented cleanup is interrupted after marker publication | new marker generation remains authoritative | reader ignores old generations; validated writer open completes cleanup |
| segmented marker checksum fails | open fails closed before selecting a generation | no inactive file is promoted or removed |
| compacted WAL is opened without its checkpoint base, or its selected slot is missing/corrupt/mismatched | durable open fails | no suffix is applied; another slot is never guessed |
| reversal target is absent, administrative, already reversed, or not the exact posting inverse | durable ledger post/open fails | balances and reversal index remain unchanged; no invalid suffix is accepted |
| correction frame is torn | poisoned | strict open fails; repair removes the incomplete frame, so neither reversal nor replacement is recovered |
| correction is complete but either member collides, is invalid, or was committed separately | durable ledger correction/open fails | balances, indexes, and event sequence remain unchanged; no one-member state is accepted |
| ledger-batch frame is torn | poisoned | strict open fails; repair removes the incomplete frame, so no batch member is recovered |
| ledger batch is complete but a member/order/lifecycle transition is invalid or members were committed under another grouping | durable ledger batch/open fails | balances, indexes, period/reversal state, and event sequence remain unchanged; no member prefix is accepted |
| financial effective date is closed, booking time regresses, or close/reopen progression is invalid | durable ledger post/open fails before commit | balances, period boundary, booking timestamp, and WAL remain unchanged for live validation failures; an invalid persisted suffix is rejected during recovery |

### Test coverage

- **Deterministic injected-fault tests** exercise partial frame writes,
  complete writes with failed acknowledgement barriers, partial grouped
  writes, explicit sync failures, poison behavior, strict reopening, and
  verified-prefix repair.
- **Segment tests** exercise exact-boundary and whole-batch rotation,
  configuration drift, closed-file corruption, active-tail repair, manager
  exclusion, interrupted empty-file creation, and pre-rotation sequence
  exhaustion.
- **A forced-process-termination test** additionally proves recovery after an
  abandoned writer lease and a possible torn tail.
- **Snapshot/checkpoint tests** exercise stable framing, payload bounds,
  corrupt and incomplete pending files, current/pending generation forks,
  exact matching-command/report and ledger-record lineage, matching
  displayed/hidden FIFO, reserve/STP/GTD-expiry/dormant-stop restoration,
  correction and generalized
  ledger-batch grouping, batch torn-tail repair and whole-frame segment rotation,
  managed-directory rejection, WAL-path alias rejection, single/segmented
  WAL-prefix proof, suffix replay, coupled risk
  rejection/position/reservation restoration, immutable-profile binding, and
  reversal-index recovery.
- **Call-auction coverage** additionally includes canonical
  phase/book/counter/cache restoration, multi-cycle retained remainders,
  exact retry suppression, uncut prefix forks/ahead state, corrupt/wrong A/B
  slots, and dangling suffix completion after an anchor.
- **Cutover tests** additionally cover repeated A/B replacement, non-genesis
  physical sequences, anchor/snapshot divergence, missing checkpoint context,
  suffix continuation, occupied and abandoned staging paths, path aliasing,
  generation selection and cleanup, invalid selected-generation preservation,
  marker checksum corruption, generation exhaustion, captured cursors at
  mid-segment and exact segment-end boundaries, multisegment suffix
  repacking, off-thread verified matching/risk/auction prefix retirement, and
  injected post-publication directory-barrier failures in both layouts.
- **Ledger-period tests** additionally exercise inclusive boundary dates,
  non-advancing closes, backward/full reopen, timestamp regression,
  administrative-reversal rejection, and checkpoint-plus-WAL suffix
  reconstruction.

## Unimplemented storage properties

This section lists storage properties that are not implemented.

- externally coordinated archival/handoff of retired generations;
- bounded idempotency/audit history, semantic generation rollover, and
  checkpoint-memory-bounded restart;
- authenticated records against deliberate forgery;
- kernel inode locks covering hard-link aliases;
- remote replication, quorum acknowledgement, and failover;
- declared filesystem/device power-loss qualification.

No claim outside the implemented and conditional boundary above is made.
