# Local storage contract

This document bounds the guarantees of `Journal`, `DurableOrderBook`,
`DurableRiskOrderBook`, and `DurableLedger`. It distinguishes properties proved
by the state machines from properties conditional on the operating system,
filesystem, and storage device.

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

## File and directory durability

Creating a WAL uses exclusive creation, `File::sync_all`, then parent-directory
`sync_all`. Repairing a torn tail performs `set_len` followed by file
`sync_all`. Lease creation and removal also synchronize their directory entry.

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

Deterministic injected-fault tests exercise partial frame writes, complete writes
with failed acknowledgement barriers, partial grouped writes, explicit sync
failures, poison behavior, strict reopening, and verified-prefix repair. A
forced-process-termination test additionally proves recovery after an abandoned
writer lease and a possible torn tail.

## Unimplemented storage properties

- automatic segment rotation and retention;
- checksummed semantic state snapshots and WAL cutover;
- authenticated records against deliberate forgery;
- kernel inode locks covering hard-link aliases;
- remote replication, quorum acknowledgement, and failover;
- declared filesystem/device power-loss qualification.

No claim outside the implemented and conditional boundary above is made.
