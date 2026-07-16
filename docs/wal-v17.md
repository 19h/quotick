# Expired WAL Format Version 17

This is a historical byte-level record. The runtime rejects version 17. The
authoritative current schema is [WAL format version 18](wal-v18.md).

This document is the authoritative byte-level schema for Quotick WAL version
17. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 17 preserves the version-16 frame, record-kind registry, and payload
values. It changes the deterministic interpretation of continuous FOK orders
using decrement-and-cancel self-trade prevention. The runtime accepts only
version 17; versions `1` through `16` are expired and rejected before payload
interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `17` |
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
repair, rotation, and cutover retain the version-16 rules.

## Continuous FOK decrement-and-cancel semantics

The continuous new-order payload is byte-identical to version 16. In its
existing fields, `TimeInForce::FillOrKill` retains tag `2` and
`SelfTradePrevention::DecrementAndCancel` retains tag `3`. The historical
`UnsupportedFokSelfTradePolicy` rejection retains tag `6` for stable decoding,
but a version-17 engine does not emit it for a well-formed FOK order.

FOK requires the original quantity to execute as external trades. Prevented
self quantity is not execution. The nonmutating preflight therefore treats the
first priority-reachable self order as a barrier under decrement-and-cancel,
using the same reserve and hidden-class traversal as cancel-aggressor and
cancel-both. External quantity after that barrier is ineligible. If external
quantity before the barrier is insufficient, rejection tag `7`,
`InsufficientLiquidity`, is emitted before maker or STP mutation. If it is
sufficient, ordinary matching completes before reaching the self order and
emits no self-trade-prevention event.

A dormant FOK stop retains the same fields and applies the same preflight when
activated. Failure cancels the dormant order with existing cancellation tag
`8`, `TriggeredFokUnfilled`, without changing makers. Minimum-quantity IOC with
decrement-and-cancel remains separately inadmissible.

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

Call-auction rejection tag `23`, `SelfTradeWouldOccur`, carries no value bytes.
It is valid only for an applicable tag-`3` uncross command whose explicit
self-trade policy is `Abort`. Its generic one-event rejected report remains
49 B. Allocation, pairing, fail-closed abort, risk neutrality, exact retry, and
auction market-data version-5 semantics remain unchanged from version 16.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; invalid domain
identifiers; invalid policy tags; rejection/report-grammar mismatch;
continuous FOK command/report divergence; accepted abort completions containing
a same-account pair; declared length/count overflow; and all version-16
rejection rules. Accepted reports must reproduce their command semantics
exactly.

## Compatibility boundary

Only envelope version 17 is accepted. Relabelling a version-16 artifact is
invalid because CRC-32C covers the header and because continuous FOK
decrement-and-cancel reports require version-17 semantic validation.
Authoritative predecessor migration requires an explicit
provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- FIX 5.0 SP2 `TimeInForce(59)` assigns `4` to Fill or Kill in the
  [FIX field definition](https://fiximate.fixtrading.org/legacy/en/FIX.5.0SP2/tag59.html).
- CME describes self-match prevention using a common identifier and
  instruction-dependent cancellation behavior in its
  [Globex Self-Match Prevention FAQ](https://www.cmegroup.com/solutions/market-access/globex/trade-on-globex/faq-self-match.html).
- Quotick's all-external-fill-before-self-barrier rule is an internal
  deterministic contract. It does not claim FIX, CME, or other venue-protocol
  compatibility.
