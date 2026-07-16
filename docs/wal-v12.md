# WAL Format Version 12

This document is the authoritative byte-level schema for Quotick WAL version
12. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 12 preserves the version-11 frame, record-kind registry, instrument
definition, continuous commands and reports, and every existing value byte. It
adds retained-priority call-auction quantity-reduction values. The runtime
accepts only version 12; versions `1` through `11` are expired envelopes and
are rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `12` |
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
repair, sequence, rotation, and cutover retain the version-11 rules.

## Call-auction command payload, kind 9

Existing command tags and layouts remain those of
[WAL version 11](wal-v11.md). The registry adds tag `6`, `Amend`, with fields
in this exact order:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | command tag `6` |
| 2 | 8 | non-zero command ID |
| 3 | 8 | non-zero instrument ID |
| 4 | 8 | non-zero instrument version |
| 5 | 8 | non-zero auction ID |
| 6 | 8 | expected phase revision `u64` |
| 7 | 8 | non-zero account ID |
| 8 | 8 | non-zero active order ID |
| 9 | 8 | non-zero new leaves quantity |
| 10 | 8 | receive timestamp as `TimestampNs` |

The complete command is 73 B. It is route-checked and accepted only in the
`Collecting` phase for the exact active auction ID and phase revision. The
account must own the active order. New leaves must satisfy the instrument's
post-execution lot rules and be strictly smaller than current active leaves.

An accepted amendment preserves order ID, account, side, market/limit
constraint, price, queue links, and priority sequence. It consumes no new
order identity, changes no order or level count, and advances the collection-
book revision exactly once. Equal or increased quantity is business rejection
`AmendQuantityNotReduced`.

## Call-auction report payload, kind 10

The call-auction action registry used by rejection values adds tag `6`,
`Amend`. The rejection-reason registry adds tag `22`,
`AmendQuantityNotReduced`. All existing action and rejection tags are
unchanged.

The event-kind registry adds tag `8`, `OrderAmended`, with this exact payload:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | event-kind tag `8` |
| 2 | 34 market / 42 limit | post-amendment order snapshot |
| 3 | 8 | previous active quantity |
| 4 | 8 | resulting collection-book revision |

The event-kind payload is 51 B for a market order and 59 B for a limit order.
The common event prefix remains sequence `u64`, command ID `u64`, and
timestamp `u64`, so the complete event is 75 B or 83 B.

An accepted amendment report contains exactly one `OrderAmended` event. Its
post-state quantity equals the command quantity; its previous quantity is
strictly larger; all immutable order fields and priority match projected
pre-state; and its book revision is the exact successor. Exact command retries
return the cached report and append no WAL records.

## Payloads with unchanged value schemas

Continuous command kind `1`, continuous report kind `2`, ledger entry kind
`3`, instrument definition kind `4`, account-risk definition kind `5`, ledger
correction kind `6`, ledger batch kind `7`, and checkpoint anchor kind `8`
retain their version-11 value schemas. Call-auction values other than the
additions above also remain unchanged.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; zero domain
identifiers or quantities; noncanonical booleans; declared length/count
overflow; and every reconstructed domain or report-grammar violation. Auction
history validation additionally rejects absent targets, owner disagreement,
nondecreasing quantity, immutable-field or priority changes, invalid leaves,
and book-revision contradictions.

## Compatibility boundary

Only envelope version 12 is accepted. Version-11 decoders do not define
call-auction command/action tag `6`, rejection tag `22`, or event-kind tag `8`.
Re-labelling a frame is invalid because CRC-32C covers its header and because
the new values cannot be inferred. Migration of authoritative version-11
artifacts requires an explicit provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- FIX identifies `OrderCancelReplaceRequest(35=G)` as the message for reducing
  an order rather than cancelling all remaining quantity in the
  [FIX Trading Community trade specification](https://www.fixtrading.org/online-specification/business-area-trade/).
- Quotick's retained identity, priority, revision, risk, history-lane, and
  replay rules are internal deterministic contracts verified by book, engine,
  codec, market-data, checkpoint, risk, and durable-recovery tests; they do not
  claim FIX wire compatibility.
