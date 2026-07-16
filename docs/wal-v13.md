# Expired WAL Format Version 13

Version 13 is expired. The runtime accepts only
[WAL format version 14](wal-v14.md).

This document is the authoritative byte-level schema for Quotick WAL version
13. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 13 preserves the version-12 frame, record-kind registry, instrument
definition, continuous commands and reports, and every existing value byte. It
adds an explicit call-auction allocation-policy byte to uncross commands and
completion events. The runtime accepts only version 13; versions `1` through
`12` are expired envelopes and are rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `13` |
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
repair, sequence, rotation, and cutover retain the version-12 rules.

## Call-auction command payload, kind 9

Existing command tags and layouts remain those of
[WAL version 12](wal-v12.md), except tag `3`, `Uncross`. Its fields occur in
this exact order:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | command tag `3` |
| 2 | 8 | non-zero command ID |
| 3 | 8 | non-zero instrument ID |
| 4 | 8 | non-zero instrument version |
| 5 | 8 | non-zero auction ID |
| 6 | 8 | expected phase revision `u64` |
| 7 | 8 | minimum raw price `i64` |
| 8 | 8 | maximum raw price `i64` |
| 9 | 8 | reference raw price `i64` |
| 10 | 1 | pressure rule |
| 11 | 1 | final-price tie break |
| 12 | 1 | allocation policy |
| 13 | 1 | remainder policy |
| 14 | 1 | self-trade policy |
| 15 | 8 | receive timestamp as `TimestampNs` |

The complete command is 78 B. Pressure-rule tags remain ignore `0` and favor
imbalance `1`. Final-price tie-break tags remain lower `0` and higher `1`.
Remainder-policy tags remain retain all `0`, cancel market `1`, and cancel all
`2`. The only represented self-trade policy remains permit `0`.

The allocation-policy registry is:

| Tag | Meaning |
|---:|---|
| `0` | `PriceTime` |
| `1` | `ProRataTime` |

`PriceTime` retains strict market/price/priority-class/time/order-ID
allocation. `ProRataTime` retains strict market, economically better price,
and lower priority-class tiers. Every tier preceding the marginal tier fills
completely. Within the marginal tier, each order receives
`floor(order quanta × remaining quanta / tier quanta)` allocation quanta.
Residual quanta are assigned once in ascending time/order-ID priority. Worse
tiers receive zero. The allocation quantum is the instrument quantity
increment, and arithmetic is exact without an overflowing product.

Both sides are allocated independently to the same clearing executable
quantity. Pairing then consumes both canonical fill sequences under the
existing self-trade, trade-ID, remainder, and atomic commit rules.

## Call-auction report payload, kind 10

Existing action, rejection, cancellation, and event-kind tags remain those of
version 12. Event-kind tag `5`, `UncrossCompleted`, now has this exact payload:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | event-kind tag `5` |
| 2 | 8 | non-zero auction ID |
| 3 | 40 | clearing state |
| 4 | 1 | allocation policy |
| 5 | 1 | remainder policy |
| 6 | 1 | self-trade policy |
| 7 | 8 | trade count `u64` |
| 8 | 8 | cancellation count `u64` |
| 9 | 8 | collection-book revision `u64` |
| 10 | 8 | phase revision `u64` |

The clearing state remains raw clearing price `i64`, aggregate buy quantity
`u128`, and aggregate sell quantity `u128`. The event-kind payload is 84 B.
The common event prefix remains sequence `u64`, command ID `u64`, and
timestamp `u64`, so the complete event is 108 B.

The completion allocation policy must equal its source uncross command. Trade
and remainder events must reconcile to the fills produced by that policy.
Exact command retries return the cached report and append no WAL records.

## Payloads with unchanged value schemas

Continuous command kind `1`, continuous report kind `2`, ledger entry kind
`3`, instrument definition kind `4`, account-risk definition kind `5`, ledger
correction kind `6`, ledger batch kind `7`, and checkpoint anchor kind `8`
retain their version-12 value schemas. Call-auction values other than the
uncross-policy additions above also remain unchanged.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; zero domain
identifiers or quantities; noncanonical booleans; declared length/count
overflow; and every reconstructed domain or report-grammar violation. Auction
history validation additionally rejects command/completion policy
disagreement, fills or trades inconsistent with the selected policy,
off-increment quantities, and direct-state or revision contradictions.

## Compatibility boundary

Only envelope version 13 is accepted. Version-12 decoders do not define the
allocation-policy byte in uncross commands or completion events. Re-labelling
a frame is invalid because CRC-32C covers its header and because inserting the
new policy changes payload length and every following byte. Migration of
authoritative version-12 artifacts requires an explicit provenance-preserving
converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Eurex distinguishes price-time, pro-rata, and time-pro-rata allocation and
  identifies price priority as common to its allocation methods in its
  [T7 matching principles](https://www.eurex.com/ex-en/trade/order-book-trading/matching-principles).
- CME documents timestamp priority for residual pro-rata quantity in its
  [Globex matching-algorithm change notice](https://www.cmegroup.com/tools-information/lookups/advisories/electronic-trading/20080609.html).
- Quotick's priority-class tiers, quantity quantum, exact floor arithmetic,
  FIFO residual, per-side reconciliation, pairing, and replay rules are
  internal deterministic contracts verified by kernel, book, engine, codec,
  checkpoint, and durable-recovery tests; they do not claim venue wire or
  matching compatibility.
