# Semantic snapshot format version 1

`SnapshotFile` stores a complete typed semantic value in a bounded, versioned,
CRC-32C envelope. Version 1 currently assigns payload kind `1` to
`LedgerCheckpoint`. The payload trait is sealed, so downstream codecs cannot
claim a reserved kind. All integers are little-endian.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `1` |
| 6 | 2 | typed payload kind; ledger checkpoint is `1` |
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
| 4 | 4 B | journal-entry count `u32` |
| 5 | variable | for each entry: encoded length `u32`, then one complete `JournalEntry` payload |

A `JournalEntry` payload is `20 B + 32 B × posting count`: transaction ID
`u64`, source reference `u64`, posting count `u32`, then account ID `u64`,
asset ID `u64`, and amount `i128` for each posting. For `B` non-zero balances,
`E` entries, and `L` total posting legs, the ledger payload length is

```text
P = 16 B + 32 B × B + 24 B × E + 32 B × L.
```

The total snapshot length is `28 B + P`. Declared collection counts are checked
against the remaining payload using their minimum valid encoded sizes before
collection allocation; integer size conversions and framing additions are
checked. Allocator failure remains outside the recoverable model (A12).

## Semantic validation

A ledger checkpoint is accepted only if all of the following hold:

1. The envelope generation equals the payload generation.
2. The generation equals the number of complete journal entries.
3. Balances are non-zero and strictly ordered by `(asset ID, account ID)`;
   duplicate keys are therefore impossible.
4. Every entry passes the ordinary canonical double-entry validation.
5. Replaying the complete entry sequence succeeds without exact duplicate
   records, transaction-ID collisions, or arithmetic overflow.
6. Balances independently reconstructed by replay equal the redundant balance
   image exactly.

`Ledger::validate` additionally checks the live journal/index correspondence,
entry sequences, deterministic replay, and independently accumulated positive
and negative totals for every asset. `Ledger::checkpoint` runs that audit
before capture. Zero balances are omitted from the image, while the complete
entry sequence is retained to preserve exact transaction idempotency.

For ledger snapshots, generation lineage is the exact journal-entry prefix
relation. A generation `g₂` is a successor of `g₁` only when `g₂ ≥ g₁` and its
first `g₁` entries equal the complete prior history. Numeric generation order
alone is insufficient.

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
`.writer.lock` sidecars to snapshot use. `DurableLedger::write_checkpoint`
additionally rejects aliases of its single WAL and lease, and rejects every
path inside its managed segmented-WAL directory.

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

## Durable-ledger checkpoint recovery

`DurableLedger::write_checkpoint` first rejects poisoned state and path
conflicts, synchronizes the WAL with `sync_all`, audits the live ledger, and
then publishes the snapshot. `open_with_checkpoint` and
`open_segmented_with_checkpoint`:

1. acquire WAL writer ownership and complete ordinary WAL recovery;
2. read and semantically validate the checkpoint;
3. reconstruct the in-memory ledger from the validated checkpoint;
4. stream the complete verified WAL and compare every checkpoint entry with
   the corresponding exact WAL prefix entry;
5. reject a checkpoint ahead of the WAL or any prefix divergence;
6. apply only WAL entries after the checkpoint generation; and
7. run the complete live-ledger invariant audit.

The current proof intentionally retains and scans the complete WAL. If `W` is
verified WAL bytes, `S` physical segments, `C` checkpoint entries, and `N` WAL
entries after the checkpoint, open remains `O(W + C + N)` time. The segmented
reader uses `O(S)` descriptors and one bounded frame payload; the restored
ledger and checkpoint payload require state proportional to retained balances
and complete checkpoint history. Snapshot construction and validation are
linear in retained balances and entry/posting history, apart from logarithmic
ordered-map/set factors inside accounting validation.

No WAL cutover, truncation, segment retention, or bounded-restart claim follows
from version 1. Those properties require a fenced cutover protocol that proves
the checkpoint generation, preserves required audit history, and prevents a
retired WAL prefix from reappearing.

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

The `QSNP` framing, payload-kind registry, ledger payload layout, lineage rules,
and recovery matrix are Quotick internal contracts rather than external
standards.
