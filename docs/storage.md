# Local storage contract

This document bounds the guarantees of `Journal`, `SegmentedJournal`,
`SnapshotFile`, `DurableOrderBook`, `DurableRiskOrderBook`, and `DurableLedger`.
The durable runtimes expose both single-file `open` and directory-backed
`open_segmented` constructors. It distinguishes properties proved by the state
machines from properties conditional on the operating system, filesystem, and
storage device.

## Canonical-path writer ownership

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

`SegmentedJournal` manages one dedicated canonical directory under a single
manager lease. The manager lease uses the same 34-byte `QLCK` record and
exclusive-create protocol as a raw journal; its path is
`.quotick-segments.writer.lock`. Raw `Journal` writers reject files whose parent
contains the segmented marker, so per-file writers cannot bypass the manager.
Readers do not acquire the manager lease.

The directory inventory is deliberately closed:

| Entry | Meaning |
|---|---|
| `format.qseg` | immutable 26-byte segmented-format marker |
| `.quotick-segments.writer.lock` | live manager ownership, present only while owned or abandoned |
| `segment-SSSSSSSSSSSSSSSSSSSS.qwal` | WAL segment; `S` is the zero-padded 20-digit first global sequence |

The marker uses little-endian integers:

| Offset (bytes) | Width (bytes) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSEG` |
| 4 | 2 | marker version `1` |
| 6 | 8 | maximum physical segment bytes `u64` |
| 14 | 8 | first global sequence `u64` |
| 22 | 4 | maximum frame payload bytes `u32` |

The segment capacity includes `QWAL` headers and payloads, but not the marker or
lease. Capacity, first sequence, and maximum payload are immutable and must
exactly equal the marker on reopen. Acknowledgement and tail-recovery policies
are runtime policies and are not marker fields. Unknown entries, noncanonical
names, an absent marker in a nonempty directory, or marker drift fail closed.
If termination interrupts the initial marker write before any segment exists,
`recover_incomplete_initialization` acquires the manager lease and removes only
an invalid marker in an otherwise empty persistent inventory. It refuses a
valid marker and refuses to act when any segment or unknown entry exists.

Rotation is size-triggered and sequence-preserving:

1. Encoding, payload, total-length, capacity, and sequence-space checks finish
   before filesystem mutation.
2. If the complete frame or acknowledgement batch does not fit a nonempty
   active segment, that segment is closed with `sync_all`.
3. The manager creates and synchronizes a segment named by the next global
   sequence and synchronizes the directory entry.
4. The complete frame or batch is appended to the new active segment. A batch
   is never intentionally split by rotation.

No mutable manifest is required: sorted canonical names plus strict frame scans
derive the inventory and global sequence. A crash before new-file creation
leaves the prior file active. A crash after creation can leave one empty final
segment, which is valid and reused. Only the final segment can use
`RepairTornTail`; every earlier segment is always opened strictly, and an empty
non-final segment is invalid. Corruption, truncation, oversize files, or a
sequence gap in a closed segment are never skipped or repaired.

`SegmentedJournalReader` streams a fixed inventory one file at a time and
verifies one global contiguous sequence. Its memory overhead is `O(S)` for `S`
segment descriptors plus one bounded frame payload; it does not materialize the
complete WAL. Durable matching, risk, and ledger recovery use this streaming
path while holding manager ownership. A standalone reader does not provide an
atomic point-in-time snapshot of a concurrently appending active segment; it is
a verified prefix reader, and callers requiring authoritative recovery must
exclude concurrent mutation.

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

`SnapshotFile` canonicalizes its target and uses the raw-journal 34 B `QLCK`
sidecar protocol at `<target>.writer.lock`. It rejects every mutating operation
whose target parent carries `format.qseg`; the segmented directory inventory
therefore remains closed. Read-only snapshot access does not acquire a lease.

The replacement staging path is `<target>.pending` in the same directory. A
normal write refuses to proceed while that path exists. After validating
generation and exact semantic lineage against any current snapshot, the writer
exclusively creates the pending path, writes the complete bounded `QSNP` file,
synchronizes it, renames it over the target, synchronizes the parent directory,
and releases the lease.

Rust specifies that `std::fs::rename` replaces an existing target and rejects a
cross-mount operation ([Rust `rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html)).
For a qualified POSIX-like filesystem, replacement of the same directory entry
is atomic: an observer sees the prior or new name binding
([POSIX.1-2024 `rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html)).
This namespace atomicity is distinct from persistence through power loss.

An incomplete or complete pending file can survive a terminated writer. After
resolving any abandoned lease under the same external liveness/quiescence
preconditions as WAL recovery, `recover_pending` validates both files and uses
semantic generation plus exact history lineage. It promotes a newer successor,
removes a proven stale prefix or byte-identical duplicate, removes provably
malformed pending content, and preserves both files on divergence or invalid
current state. A pending file rejected only because its version/kind is
unsupported or its size exceeds the caller's configured limit is preserved.
The complete wire and decision contract is
[Semantic snapshot format version 1](snapshot-v1.md).

Direct users must dedicate the target and its two sidecars to snapshots.
`DurableOrderBook`, `DurableRiskOrderBook`, and `DurableLedger` additionally
check that a checkpoint target, pending path, and lease cannot alias their
single-file WAL/lease or reside anywhere inside their segmented directory.
`write_checkpoint` synchronizes the WAL before publishing an independently
audited matching, coupled risk/matching, or ledger image.

## File and directory durability

Creating a WAL uses exclusive creation, `File::sync_all`, then parent-directory
`sync_all`. Repairing a torn tail performs `set_len` followed by file
`sync_all`. Lease creation and removal also synchronize their directory entry.
Snapshot publication synchronizes the complete pending file before rename and
synchronizes the parent after rename; pending removal and promotion also
synchronize the parent.

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
| reversal target is absent, administrative, already reversed, or not the exact posting inverse | durable ledger post/open fails | balances and reversal index remain unchanged; no invalid suffix is accepted |
| correction frame is torn | poisoned | strict open fails; repair removes the incomplete frame, so neither reversal nor replacement is recovered |
| correction is complete but either member collides, is invalid, or was committed separately | durable ledger correction/open fails | balances, indexes, and event sequence remain unchanged; no one-member state is accepted |
| financial effective date is closed, booking time regresses, or close/reopen progression is invalid | durable ledger post/open fails before commit | balances, period boundary, booking timestamp, and WAL remain unchanged for live validation failures; an invalid persisted suffix is rejected during recovery |

Deterministic injected-fault tests exercise partial frame writes, complete writes
with failed acknowledgement barriers, partial grouped writes, explicit sync
failures, poison behavior, strict reopening, and verified-prefix repair.
Segment tests exercise exact-boundary and whole-batch rotation, configuration
drift, closed-file corruption, active-tail repair, manager exclusion,
interrupted empty-file creation, and pre-rotation sequence exhaustion. A
forced-process-termination test additionally proves recovery after an abandoned
writer lease and a possible torn tail. Snapshot/checkpoint tests exercise stable
framing, payload bounds, corrupt and incomplete pending files, current/pending
generation forks, exact matching-command/report and ledger-record lineage,
matching FIFO/reserve/STP restoration, correction grouping, managed-directory
rejection, WAL-path alias rejection, single/segmented WAL-prefix proof, suffix
replay, coupled risk rejection/position/reservation restoration, immutable-
profile binding, and reversal-index recovery.
Ledger-period tests additionally exercise inclusive boundary dates, non-
advancing closes, backward/full reopen, timestamp regression, administrative-
reversal rejection, and checkpoint-plus-WAL suffix reconstruction.

## Unimplemented storage properties

- automatic segment retention, archival, and deletion fencing;
- matching/risk/ledger checkpoint WAL cutover, compaction, bounded idempotency
  history, and bounded restart;
- authenticated records against deliberate forgery;
- kernel inode locks covering hard-link aliases;
- remote replication, quorum acknowledgement, and failover;
- declared filesystem/device power-loss qualification.

No claim outside the implemented and conditional boundary above is made.
