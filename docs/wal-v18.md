# WAL Format Version 18

> **Expired.** Runtime version 19 rejects this envelope. The current schema is
> [WAL format version 19](wal-v19.md). Historical bytes below are unchanged.

This document is the authoritative byte-level schema for Quotick WAL version
18. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 18 preserves the version-17 frame, record-kind registry, and payload
values. It changes the deterministic interpretation of continuous
minimum-quantity IOC orders using decrement-and-cancel self-trade prevention.
The runtime accepts only version 18; versions `1` through `17` are expired and
rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `18` |
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
repair, rotation, and cutover retain the version-17 rules.

## Minimum-quantity decrement-and-cancel semantics

The continuous new-order payload is byte-identical to version 17. In its
existing fields, `TimeInForce::ImmediateOrCancelWithMinimum` retains tag `5`
followed by the non-zero `u64` minimum quantity, and
`SelfTradePrevention::DecrementAndCancel` retains tag `3`. Historical
`UnsupportedMinimumQuantitySelfTradePolicy` rejection tag `51` remains valid
for stable decoding, but a version-18 engine does not emit it for a well-formed
minimum-quantity order.

Only external traded quantity satisfies the minimum. A prevented self
interaction consumes the lesser of incoming leaves and the resting maker's
current executable slice from both orders, but supplies no threshold credit.
The nonmutating preflight follows the exact price, priority-class, and FIFO
execution walk. It consumes initial displayed slices, requeues refreshed
reserve slices at the displayed-class tail, exhausts all displayed reserve
liquidity before entering fully hidden FIFO, and then advances to the next
crossed price.

If incoming leaves reach zero before the minimum is satisfied, the report is
accepted and cancels the complete original quantity with existing cancellation
tag `11`, `MinimumQuantityUnavailable`. No maker, STP, risk reservation,
position, or public-book state changes. If the minimum is satisfied first,
ordinary IOC execution starts from the original state, may trade beyond the
minimum, emits existing trade, reserve-refresh, and self-prevention events, and
cancels only the final incoming remainder. Dormant minimum-quantity stops apply
the same rule against activation-time liquidity.

Version-17 FOK decrement-and-cancel semantics remain unchanged. FOK requires
the complete original quantity before the first priority-reachable self
barrier; it does not use the minimum-quantity continuation scan.

## Other payloads

All continuous, ledger, checkpoint-anchor, call-auction, and coupled-risk
payload fields and tags not described above are byte-identical to version 17.
Call-auction allocation, authoritative priority classes, indicative
publication, fail-closed abort, risk, exact-retry, and market-data version-5
semantics are unchanged.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; invalid domain
identifiers; invalid policy tags; rejection/report-grammar mismatch;
minimum-quantity command/report divergence; accepted threshold-failure reports
that mutate makers, STP, risk, or public state; declared length/count overflow;
and all version-17 rejection rules. Accepted reports must reproduce their
command semantics exactly.

## Compatibility boundary

Only envelope version 18 is accepted. Relabelling a version-17 artifact is
invalid because CRC-32C covers the header and because continuous
minimum-quantity decrement-and-cancel reports require version-18 semantic
validation. Authoritative predecessor migration requires an explicit
provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- FIX defines `MinQty(110)` in its
  [field definition](https://fiximate.fixtrading.org/en/FIX.Latest/tag110.html)
  and IOC `TimeInForce(59=3)` in the
  [time-in-force definition](https://fiximate.fixtrading.org/en/FIX.Latest/tag59.html).
- CME describes self-match prevention using a common identifier and
  instruction-dependent cancellation behavior in its
  [Globex Self-Match Prevention FAQ](https://www.cmegroup.com/solutions/market-access/globex/trade-on-globex/faq-self-match.html).
- Quotick's external-threshold and exact virtual reserve-queue rules are
  internal deterministic contracts. They do not claim FIX, CME, or other
  venue-protocol compatibility.
