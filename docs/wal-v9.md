# WAL Format Version 9

The current runtime writes and accepts only [WAL version 10](wal-v10.md).
This document remains the authoritative historical schema for version 9;
version-9 frames are rejected before payload interpretation.

This document is the authoritative byte-level schema for Quotick WAL version
9. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 9 preserved the version-8 frame, record-kind registry, instrument
definition, and existing value bytes. Continuous stop-reference commands and
events add durable upstream source identity, source version, and source
sequence. The version-9 runtime accepted only version 9; versions `1` through
`8` were expired envelopes and rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `9` |
| 6 | 2 | record kind |
| 8 | 4 | payload length `u32` |
| 12 | 4 | CRC-32C `u32` |
| 16 | 8 | contiguous journal sequence `u64` |
| 24 | payload length | typed payload |

Record-kind tags remain: continuous command `1`, continuous report `2`,
ledger entry `3`, instrument definition `4`, account risk definition `5`,
ledger correction `6`, ledger batch `7`, checkpoint anchor `8`, call-auction
command `9`, and call-auction report `10`.

CRC-32C uses the reflected Castagnoli polynomial `0x82F63B78`, initial state
`0xFFFFFFFF`, and final XOR `0xFFFFFFFF`. It covers the complete header with
bytes 12--15 zeroed, followed by the payload. Segmented directories, leases,
repair, sequence, rotation, and cutover retain the version-8 rules.

## Stop-reference value

Every `StopReference` has this fixed 32 B representation:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | non-zero stop-reference source ID `u64` |
| 2 | 8 | non-zero stop-reference source version `u64` |
| 3 | 8 | non-zero source sequence `u64` |
| 4 | 8 | raw signed reference price `i64` |

The first accepted reference in an instrument shard may carry any non-zero
source sequence and binds the source ID. With no eligible backlog, another
reference in the same source version requires the exact next source sequence.
The immediate next source version requires source sequence `1`; skipped or
regressed versions and sequences are rejected. The source ID cannot change
within the shard. Reusing one cursor for a different price is a typed collision.
An exact current reference may repeat only while draining its eligible bounded
backlog; no different cursor may advance over that backlog.

## Continuous command payload, kind 1

Command tags, field order, display, order type, self-trade prevention, and
time-in-force representation remain those of
[WAL version 8](wal-v8.md), except command tag `7`. A stop-trigger sweep is:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | command ID |
| 2 | 8 | instrument ID |
| 3 | 8 | instrument version |
| 4 | 32 | current `StopReference` |
| 5 | 4 | maximum activations as `u32` |
| 6 | 8 | receive timestamp as `TimestampNs` |

## Continuous execution-report payload, kind 2

The report/event prefixes and event tags remain those of version 8. Event tag
`13`, `StopOrderTriggered`, contains order ID `u64`, raw signed trigger price
`i64`, the 32 B satisfying `StopReference`, and retained priority sequence
`u64`.

Event tag `14`, `StopTriggerSweepCompleted`, contains:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | previous-reference-present canonical `bool` |
| 2 | 32 | previous reference, or 32 zero bytes when absent |
| 3 | 32 | current `StopReference` |
| 4 | 8 | triggered-order count `u64` |
| 5 | 8 | remaining eligible-order count `u64` |

The rejection-reason registry adds tags `52` through `55`:

| Tag | Meaning |
|---:|---|
| `52` | `StopReferenceCursorCollision` |
| `53` | `StopReferenceSourceMismatch` |
| `54` | `StopReferenceVersionDiscontinuity` |
| `55` | `StopReferenceSequenceDiscontinuity` |

Existing rejection tags `0` through `51`, cancellation tags `0` through `11`,
and all other event values are unchanged.

## Payloads with unchanged value schemas

Instrument definition kind `4`, ledger entry kind `3`, account-risk definition
kind `5`, ledger correction kind `6`, ledger batch kind `7`, checkpoint anchor
kind `8`, call-auction command kind `9`, and call-auction report kind `10`
retain their version-8 value schemas.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; zero domain
identifiers, source versions, source sequences, or quantities; noncanonical
booleans and absent references; declared length/count overflow; and every
reconstructed domain or report-grammar violation. Matching admission
separately rejects cursor collisions, source changes, version/sequence
discontinuities, and reference advancement over eligible backlog before state
mutation.

## Compatibility boundary

Only envelope version 9 is accepted. Version-8 commands and events contain
only a reference price and cannot prove source identity, reset generation, or
source sequence. Re-labelling a frame is invalid because CRC-32C covers its
header and the missing provenance cannot be inferred. Migration of
authoritative version-8 artifacts requires an explicit provenance-preserving
converter.

## Primary-source provenance

- CRC-32C follows [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- FIX `ApplicationSequenceControl` identifies an upstream application and its
  application sequence for sequencing and recovery in the
  [FIX Latest specification introduction](https://www.fixtrading.org/wp-content/uploads/download-manager-files/FIX-Latest-Specification-Introduction.pdf).
- FIX session sequencing starts a new sequence space at `1` after a session
  reset in the
  [FIX Session Layer technical standard](https://www.fixtrading.org/standards/fix-session-layer-online/).
- Quotick's per-instrument source binding, version transition, exact-cursor
  collision, backlog, tags, and replay grammar are internal deterministic
  contracts verified by codec, matching, market-data, checkpoint, risk, and
  durable-recovery tests; they do not claim FIX wire compatibility.
