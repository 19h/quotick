# Expired Semantic Snapshot Format Version 3

Version 3 introduced call-auction checkpoint kind `4` while preserving the
version-2 payload bytes for kinds `1` through `3`. The current runtime rejects
this envelope and writes [snapshot version 4](snapshot-v4.md), which preserves
all four payload byte layouts and adds coupled call-auction/risk kind `5`.

The [version-2 schema](snapshot-v2.md) remains the byte-level definition of
kinds `1` through `3`. Version 3 changes their envelope version only; it does
not reinterpret those payloads.

## `QSNP` Envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `3` |
| 6 | 2 | typed payload kind: ledger `1`, matching `2`, coupled risk `3`, call auction `4` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 set to zero, followed by
the exact payload. Physical file length must equal `28 B + payload length`.
The default payload limit is 1 GiB (1,073,741,824 B), and the selected `u64`
limit is checked before allocation or filesystem mutation.

CRC-32C detects accidental corruption. It is not a message-authentication code
and does not protect against an actor able to rewrite both payload and checksum.

## Call-Auction Checkpoint Payload, Kind 4

The payload fields occur in this exact order:

| Order | Width | Field |
|---:|---:|---|
| 1 | 8 B | immutable-definition WAL sequence `M` |
| 2 | 8 B | completed execution-report WAL boundary `G` |
| 3 | variable | instrument-definition length `u32`, then definition payload |
| 4 | 1 B | phase: closed `0`, collecting `1`, frozen `2` |
| 5 | 8 B | phase revision `u64` |
| 6 | 1 B or 9 B | active-auction-present `bool`, then optional `AuctionId` |
| 7 | 1 B or 9 B | last-auction-present `bool`, then optional `AuctionId` |
| 8 | 8 B | collection-book revision `u64` |
| 9 | 8 B | next priority sequence `u64` |
| 10 | 8 B | next book-local trade ID `u64` |
| 11 | variable | accepted-order count `u32`, then ascending `OrderId` values (`8 B` each) |
| 12 | variable | active-order count `u32`, then ascending-ID order snapshots |
| 13 | variable | history count `C` as `u32`, then chronological command/report pairs |

An active-order snapshot is order ID `u64`, account ID `u64`, side `u8`,
constraint, positive remaining quantity `u64`, and non-zero priority sequence
`u64`. A market constraint is tag `0`; a limit constraint is tag `1` followed
by raw price `i64`. Snapshot size is therefore 34 B for market interest and
42 B for limit interest.

Each history entry is command length `u32` plus one WAL-v4 kind-`9` payload,
then report length `u32` plus one WAL-v4 kind-`10` payload. The exact command,
report, event, rejection, policy, and domain tags are specified in
[WAL format version 4](wal-v4.md).

The metadata boundary is followed logically by exactly one command and one
report frame for each retained history entry:

```text
G = M + 2 × C.
```

All additions, conversions, declared lengths, and collection reservations are
checked. `G` cannot name a command frame or a partial pair.

## Call-Auction Semantic Validation

A kind-`4` image is accepted only when all of the following hold:

1. `M > 0`, `G = M + 2 × C`, command IDs are unique, command sequences begin
   at `1`, reports are non-replayed, and event sequences form one contiguous
   global series beginning at `1`.
2. Every report is bound to its exact command and timestamp and satisfies the
   phase, submission, cancellation, rejection, or uncross event grammar.
3. Accepted submission events reconstruct strictly increasing priority
   sequences; accepted order IDs are unique, sorted, never reusable, and equal
   the redundant accepted-ID image.
4. Chronological trades name active eligible opposite-side orders, use the
   clearing price, carry positive quantities, and consume contiguous trade IDs.
5. Every remainder cancellation equals the positive order remainder after all
   trades in that uncross, and `remainder + executed-in-that-uncross` equals the
   quantity present immediately before that uncross. This is evaluated per
   cycle, including an order partially retained across multiple cycles.
6. The projected active orders equal the canonical ascending-ID direct image.
   The projected phase/cycle state, book revision, next priority sequence, and
   next trade ID equal the redundant direct fields exactly.
7. Reconstruction under caller-selected limits proves history, report-event,
   accepted-ID, active-order, and per-side occupied-price cardinalities fit
   before allocating live indexes. Rebuilt AVL arenas, FIFO links, aggregates,
   phase state, counters, and exact-retry cache then pass the full engine audit.

Capture audits the live engine, constructs the direct image, independently
replays the complete retained command history, and requires exact report and
checkpoint equality before publication. Restore rebuilds indexed book state
and retry history directly rather than rerunning auction discovery or uncross
algorithms.

## Lineage and WAL Cutover

Auction snapshot lineage requires equal `M`, equal immutable definition, a
nondecreasing `G`, and exact command/report prefix equality. Numeric generation
alone is insufficient.

With an uncut WAL, checkpoint-assisted open verifies every physical frame
through `G` against the snapshot before applying only the suffix. With a
compacted WAL, publication first synchronizes the inactive A/B kind-`4` slot,
then replaces the physical prefix with a version-4 checkpoint anchor containing
the exact slot, semantic generation, payload length, checksum, and physical
sequence. Recovery never guesses the alternate slot. The anchor and selected
snapshot must agree exactly before suffix replay or dangling-command completion.

Cutover bounds WAL bytes scanned at reopen, but the checkpoint deliberately
retains complete exact-retry and audit history. Payload size remains
`O(H + E + I + O)` for `H` commands, `E` events, `I` accepted identities, and
`O` active orders. Decode projection performs `O((H + E) log O)` ordered-map
work in the conservative bound, followed by the engine/book audit. Capture also
performs one complete deterministic history replay. None of these semantic
costs is removed by physical WAL cutover.

## Compatibility Boundary

Envelope version 3 is expired and rejected before payload interpretation. If an authoritative earlier deployment
exists, migration requires an explicit provenance-preserving converter;
changing version bytes in place is invalid because the CRC covers the envelope.

## Primary-Source Provenance

- CRC-32C uses the Castagnoli procedure in
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication uses Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all)
  and [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  with filesystem persistence and same-filesystem rename semantics bounded by
  [POSIX `fsync`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html)
  and [POSIX.1-2024 `rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).

The `QSNP` envelope, payload-kind registry, call-auction payload, lineage, and
recovery rules are Quotick internal contracts verified by the repository test
suites rather than attributed to an external standard.
