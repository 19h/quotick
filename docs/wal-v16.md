# Expired WAL Format Version 16

This is a historical byte-level record. The runtime rejects version 16. The
authoritative current schema is [WAL format version 17](wal-v17.md).

This document is the authoritative byte-level schema for Quotick WAL version
16. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 16 preserves the version-15 frame, record-kind registry, and existing
values. It adds one call-auction self-trade policy and one sequenced rejection.
The runtime accepts only version 16; versions `1` through `15` are expired and
rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `16` |
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
repair, rotation, and cutover retain the version-15 rules.

## Call-auction uncross policy

Call-auction command tag `3`, `Uncross`, remains 78 B. Its policy suffix is:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 65 | 1 | pressure rule: ignore `0`, favor imbalance `1` |
| 66 | 1 | final tie: lower `0`, higher `1` |
| 67 | 1 | allocation: price-time `0`, pro-rata-time `1` |
| 68 | 1 | remainder: retain all `0`, cancel market `1`, cancel all `2` |
| 69 | 1 | self-trade: permit `0`, abort `1` |
| 70 | 8 | receive timestamp, Unix nanoseconds `u64` |

The same three-byte allocation/remainder/self-trade policy remains embedded in
event-kind tag `5`, `UncrossCompleted`. Accepted completion sizes are
unchanged. An `Abort` completion is valid only when every pair produced by the
canonical two-pointer pairing walk has distinct buyer and seller account IDs.

## Self-trade rejection

Call-auction rejection tag `23` is `SelfTradeWouldOccur`. It carries no value
bytes. It is valid only for an applicable tag-`3` uncross command whose
explicit self-trade policy is `Abort`.

A one-event rejected report is 49 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 8 | non-zero command ID |
| 8 | 8 | non-zero command sequence |
| 16 | 1 | rejected outcome tag `1` |
| 17 | 1 | rejection tag `23` |
| 18 | 4 | event count `1` |
| 22 | 8 | non-zero event sequence |
| 30 | 8 | matching command ID |
| 38 | 8 | event timestamp, Unix nanoseconds `u64` |
| 46 | 1 | event-kind tag `6`, `CommandRejected` |
| 47 | 1 | rejection tag `23` |
| 48 | 1 | canonical replay boolean, false `0` |

The direct collection-book preparation error identifies the account, buy
order, sell order, and prevented quantity. Those diagnostics are process-local
and are not persisted in the sequenced rejection.

## Deterministic semantics

Allocation remains authoritative and unchanged. Pairing walks the buy and sell
fill vectors in canonical allocation order and repeatedly transfers the
smaller remaining fill. `Permit` retains version-15 behavior. `Abort` stops at
the first pair whose buyer and seller `AccountId` values are equal. It does not
search for another counterparty, modify allocation, cancel or decrement an
order, or assign aggressor/resting roles.

The abort occurs before trade-ID, book-revision, phase, risk, or order-state
mutation. The sequenced engine emits the rejection, remains `Frozen`, and
preserves the latest valid indication. Exact retry returns the cached report
and appends no WAL frame. Coupled risk applies no reservation, exposure, or
position transition. Auction market-data version 5 emits one
`NoPublicChange` update for the original rejection and no update for retry.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; invalid domain
identifiers; invalid uncross policy tags; rejection/report-grammar mismatch;
accepted abort completions containing a same-account pair; declared
length/count overflow; and all version-15 rejection rules. Accepted uncross
reports must reproduce the command policy exactly.

## Compatibility boundary

Only envelope version 16 is accepted. A version-15 decoder has no self-trade
tag `1` or rejection tag `23`. Relabelling is invalid because CRC-32C covers
the header and because the new values require version-16 semantic validation.
Authoritative predecessor migration requires an explicit
provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- CME describes self-match prevention using a common identifier and
  instruction-dependent cancellation behavior in its
  [Globex Self-Match Prevention FAQ](https://www.cmegroup.com/solutions/market-access/globex/trade-on-globex/faq-self-match.html).
- Quotick uses already-routed `AccountId` equality and a fail-closed complete-
  uncross rejection. This is an internal deterministic contract and does not
  claim CME or other venue-protocol compatibility.
