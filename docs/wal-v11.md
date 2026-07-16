# Expired WAL Format Version 11

Version 11 is expired. The runtime accepts only
[WAL format version 12](wal-v12.md).

This document is the authoritative byte-level schema for Quotick WAL version
11. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 11 preserves the version-10 frame, record-kind registry, instrument
definition, continuous commands and reports, and all existing value bytes. It
adds account-scoped call-auction mass-cancel values. The runtime accepts only
version 11; versions `1` through `10` are expired envelopes and are rejected
before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `11` |
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
repair, sequence, rotation, and cutover retain the version-10 rules.

## Call-auction command payload, kind 9

Existing command tags and layouts remain those of
[WAL version 10](wal-v10.md). The registry adds tag `5`, `MassCancel`, with
fields in this exact order:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | command tag `5` |
| 2 | 8 | non-zero command ID |
| 3 | 8 | non-zero instrument ID |
| 4 | 8 | non-zero instrument version |
| 5 | 8 | non-zero account ID |
| 6 | 1 or 2 | mass-cancel scope |
| 7 | 8 | receive timestamp as `TimestampNs` |

The complete command is 42 B for scope `All` and 43 B for a side scope. Scope
tag `0` means `All` and has no following byte. Scope tag `1` means `Side` and
is followed by side tag buy `0` or sell `1`.

The command is route-checked and valid in `Closed`, `Collecting`, and `Frozen`
without a phase revision or auction ID. It selects only active orders owned by
the account and, for `Side`, only that side. A non-empty selection is cancelled
in strictly ascending `OrderId` order and advances the collection-book revision
once. An empty selection is accepted and leaves that revision unchanged. Any
preflight or capacity failure leaves all state unchanged.

## Call-auction report payload, kind 10

The call-auction action registry used by rejection values adds tag `5`,
`MassCancel`. The cancellation-reason registry adds tag `3`, `MassCancel`;
existing reasons remain owner-requested `0`, uncross remainder `1`, and
replaced `2`.

The event-kind registry adds tag `7`, `MassCancelCompleted`, with this exact
payload:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | event-kind tag `7` |
| 2 | 8 | non-zero account ID |
| 3 | 1 or 2 | mass-cancel scope |
| 4 | 8 | cancelled-order count `u64` |
| 5 | 16 | cancelled quantity in lots `u128` |
| 6 | 8 | collection-book revision `u64` |

The event-kind payload is 42 B for `All` and 43 B for a side scope. The common
event prefix remains sequence `u64`, command ID `u64`, and timestamp `u64`, so
the complete event is 66 B or 67 B.

An accepted mass-cancel report contains exactly `K + 1` chronological events:

1. `K` event-tag-`2` `OrderCancelled` values, each carrying one selected order
   snapshot and cancellation reason `MassCancel` (`3`), in strictly ascending
   `OrderId` order;
2. one event-tag-`7` `MassCancelCompleted` value with the source account and
   scope, exact `K` count, exact `u128` cancelled-lot sum, and resulting book
   revision.

All events have contiguous engine event sequences, the command's receive
timestamp, and the same command ID. Count and quantity are both zero exactly
for an empty selection. A non-empty selection increments the source book
revision once; an empty selection preserves it. Exact command retries return
the cached report and append no WAL records.

## Payloads with unchanged value schemas

Continuous command kind `1`, continuous report kind `2`, ledger entry kind
`3`, instrument definition kind `4`, account-risk definition kind `5`, ledger
correction kind `6`, ledger batch kind `7`, and checkpoint anchor kind `8`
retain their version-10 value schemas. Call-auction command/report values other
than the additions above also remain unchanged.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; zero domain
identifiers or quantities; noncanonical booleans; declared length/count
overflow; and every reconstructed domain or report-grammar violation. Auction
history validation additionally rejects wrong account or side membership,
nonascending cancellation identities, missing or repeated completion, count or
quantity disagreement, inconsistent empty totals, and book-revision
contradictions.

## Compatibility boundary

Only envelope version 11 is accepted. Version-10 decoders do not define
call-auction command/action tag `5`, cancellation-reason tag `3`, or event-kind
tag `7`. Re-labelling a frame is invalid because CRC-32C covers its header and
because the new values cannot be inferred. Migration of authoritative
version-10 artifacts requires an explicit provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- FIX defines `OrderMassCancelRequest(35=q)` as a separately identified request
  to cancel remaining quantity for an order group and permits an optional side
  qualifier in the
  [FIX Trading Community trade appendix](https://www.fixtrading.org/online-specification/trade-appendix/).
- Quotick's account scope, canonical order, aggregate completion, revision,
  terminal-lane, risk, and replay rules are internal deterministic contracts
  verified by book, engine, codec, market-data, checkpoint, risk, and durable-
  recovery tests; they do not claim FIX wire compatibility.
