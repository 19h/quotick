# WAL Format Version 10

This document is the authoritative byte-level schema for Quotick WAL version
10. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 10 preserves the version-9 frame, record-kind registry, instrument
definition, continuous commands and reports, and all existing value bytes. It
adds call-auction new-identity cancel/replace values. The runtime accepts only
version 10; versions `1` through `9` are expired envelopes and are rejected
before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `10` |
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
repair, sequence, rotation, and cutover retain the version-9 rules.

## Call-auction command payload, kind 9

Existing command tags and layouts remain those of
[WAL version 9](wal-v9.md). The registry adds tag `4`, `Replace`, with fields
in this exact order:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | command tag `4` |
| 2 | 8 | non-zero command ID |
| 3 | 8 | non-zero auction ID |
| 4 | 8 | expected phase revision `u64` |
| 5 | 8 | non-zero account ID |
| 6 | 8 | non-zero target order ID |
| 7 | variable | complete replacement order |
| 8 | 8 | receive timestamp as `TimestampNs` |

The replacement order is non-zero order ID `u64`, account ID `u64`,
instrument ID `u64`, instrument version `u64`, side tag `u8` (buy `0`, sell
`1`), constraint (market tag `0`, or limit tag `1` plus raw price `i64`), and
non-zero lot quantity `u64`. The outer and replacement account IDs must match.

Replacement is accepted only in the collecting phase for the named active
auction and exact phase revision. It atomically removes the owned active
target and admits the fresh replacement identity. The target ID remains
consumed, the replacement ID becomes consumed, and the replacement receives
a fresh priority sequence. Released target capacity is included in preflight,
so an otherwise valid replacement can reuse saturated active-order and
singleton price-level capacity without transient overflow. A fresh accepted-ID
slot is still required because accepted identities are never released. Any
failure leaves all state unchanged.

## Call-auction report payload, kind 10

The call-auction action registry used by rejection values adds tag `4`,
`Replace`. The cancellation-reason registry adds tag `2`, `Replaced`;
existing reasons remain owner-requested `0` and uncross remainder `1`.

An accepted replacement report contains exactly two chronological events for
one command:

1. event tag `2`, `OrderCancelled`, carrying the target snapshot and reason
   `Replaced` (`2`);
2. event tag `1`, `OrderAccepted`, carrying the replacement snapshot with a
   distinct order ID, the same account ID, and a fresh priority sequence.

The two events have contiguous engine event sequences, the command's receive
timestamp, and the same command ID. The source book revision advances once for
the atomic command. A risk rejection instead emits the ordinary single
`CommandRejected` event for action `Replace`; it does not cancel the target.
Exact command retries return the cached report and append no WAL records.

## Payloads with unchanged value schemas

Continuous command kind `1`, continuous report kind `2`, ledger entry kind
`3`, instrument definition kind `4`, account-risk definition kind `5`, ledger
correction kind `6`, ledger batch kind `7`, and checkpoint anchor kind `8`
retain their version-9 value schemas. Call-auction command/report values other
than the additions above also remain unchanged.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; zero domain
identifiers or quantities; noncanonical booleans; declared length/count
overflow; and every reconstructed domain or report-grammar violation. Matching
separately rejects stale phase/cycle identity, unknown or unowned targets,
mismatched accounts, reused replacement identity, invalid instrument admission,
risk failure, and exhausted counters before mutation.

## Compatibility boundary

Only envelope version 10 is accepted. Version-9 decoders do not define
call-auction command/action tag `4` or cancellation-reason tag `2`.
Re-labelling a frame is invalid because CRC-32C covers its header and because
the new values cannot be inferred. Migration of authoritative version-9
artifacts requires an explicit provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- FIX defines `OrderCancelReplaceRequest(35=G)` as a request to change an
  existing order and identifies the prior accepted order through
  `OrigClOrdID(41)` in the
  [FIX Latest message registry](https://fiximate.fixtrading.org/en/FIX.Latest/msg17.html).
- Quotick's atomic capacity reuse, full priority loss, two-event trace, tags,
  revision semantics, risk netting, and replay grammar are internal
  deterministic contracts verified by book, codec, matching, market-data,
  checkpoint, risk, and durable-recovery tests; they do not claim FIX wire
  compatibility or venue-specific amendment semantics.
