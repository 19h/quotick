# Expired WAL Format Version 14

Version 14 is expired. The runtime accepts only
[WAL format version 15](wal-v15.md).

This document is the authoritative byte-level schema for Quotick WAL version
14. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 14 preserves the version-13 frame, record-kind registry, continuous
matching, ledger, instrument, risk, and call-auction value registries. It adds
one authoritative `u16` priority-class scalar to every call-auction order and
active-order snapshot. The runtime accepts only version 14; versions `1`
through `13` are expired and rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `14` |
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
repair, rotation, and cutover retain the version-13 rules.

## Call-auction order value

Every call-auction order in a tag-`1` `Submit` command or nested tag-`4`
`Replace` command has this exact field order:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | non-zero order ID |
| 2 | 8 | non-zero account ID |
| 3 | 8 | non-zero instrument ID |
| 4 | 8 | non-zero instrument version |
| 5 | 1 | side: buy `0`, sell `1` |
| 6 | 1 or 9 | constraint: market `0`, or limit `1` plus raw price `i64` |
| 7 | 8 | non-zero active quantity in lots |
| 8 | 2 | priority class `u16` |

The value is 44 B for a market order and 52 B for a limit order. Lower
priority-class values execute first only after market/limit and economic price
priority have selected an identical constraint. Within one class, the
book-assigned priority sequence and then `OrderId` determine order.

The complete `Submit` command is 77 B for market or 85 B for limit. The
complete `Replace` command is 93 B for market or 101 B for limit. A replacement
may carry a different class but receives a fresh priority sequence. A strict
quantity-reduction amendment carries no class field because it preserves the
active order's existing class.

## Call-auction active-order snapshot

Event kinds `1` `OrderAccepted`, `2` `OrderCancelled`, and `8`
`OrderAmended` embed this snapshot:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | non-zero order ID |
| 2 | 8 | non-zero account ID |
| 3 | 1 | side |
| 4 | 1 or 9 | market or limit constraint |
| 5 | 8 | non-zero active quantity in lots |
| 6 | 2 | priority class `u16` |
| 7 | 8 | non-zero book-assigned priority sequence |

The snapshot is 36 B for market or 44 B for limit. Including the event-kind
byte, the respective kind payload sizes are:

| Event kind | Market (B) | Limit (B) |
|---|---:|---:|
| `OrderAccepted` | 37 | 45 |
| `OrderCancelled` plus reason `u8` | 38 | 46 |
| `OrderAmended` plus prior quantity and book revision | 53 | 61 |

The common event prefix is 24 B, so complete `OrderAmended` events are 77 B
or 85 B. An accepted submit or replacement snapshot must repeat the command
class exactly. Amendment, cancellation, mass cancellation, partial uncross,
checkpoint projection, and exact retry preserve it.

## Allocation and uncross interaction

Tag-`3` `Uncross` and event-kind-`5` `UncrossCompleted` retain their version-13
allocation, remainder, and self-trade policy bytes. Canonical order priority is
market, economic price, ascending class, ascending priority sequence, then
ascending order ID. `PriceTime` walks that order. `ProRataTime` treats one
constraint and one class as an allocation tier, so a worse class receives no
quantity until every better class at that constraint is fully allocated.

The priority class does not change aggregate clearing-price discovery, public
aggregate depth, or conservative per-order risk reservation. It changes which
private orders receive fills and therefore the authoritative trade, position,
remainder, and replay trace.

## Payloads with unchanged value schemas

Kinds `1` through `8` retain their version-13 value schemas. Call-auction
command tags, action tags, rejection tags, cancellation reasons, trade values,
uncross policies, and completion layouts also remain version 13. Only values
that contain a call-auction order or active-order snapshot gain the `u16`
field above.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; zero domain
identifiers or quantities; noncanonical booleans; declared length/count
overflow; and reconstructed domain or report-grammar violations. Auction
history validation additionally rejects a submitted/replacement class that
differs from its accepted snapshot, any amendment that changes class, class-
ignorant priority ordering, direct-row/history disagreement, or a resulting
allocation/trade/risk contradiction.

## Compatibility boundary

Only envelope version 14 is accepted. A version-13 decoder has no priority-
class bytes in call-auction orders or snapshots. Relabelling is invalid because
CRC-32C covers the header and because inserting the new scalar changes payload
length and every following byte. Authoritative version-13 migration requires
an explicit provenance-preserving converter; the runtime never infers class
`0` for historical bytes.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Eurex states that central-book orders are sorted by type, price, and then the
  configured allocation criteria, with market orders receiving highest
  priority, in its
  [T7 matching principles](https://www.eurex.com/ex-en/trade/order-book-trading/matching-principles).
- Nasdaq documents distinct on-open, on-close, and imbalance-only order
  categories and their auction eligibility in its
  [Opening and Closing Cross guide](https://www.nasdaqtrader.com/content/ProductsServices/Trading/Crosses/openclose_faqs.pdf).
- The mapping from an authenticated venue/order category to Quotick's `u16`
  class is an internal versioned ingress contract. No numeric class is claimed
  to be compatible with a venue protocol without a conformance adapter.
