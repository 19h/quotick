# WAL Format Version 19

This document is the authoritative byte-level schema for Quotick WAL version
19. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 19 preserves the version-18 frame, record-kind registry, and all
payload values except the private call-auction trade value. That value now
carries the immutable instrument identifier and definition version required by
the settlement boundary. The runtime accepts only version 19; versions `1`
through `18` are expired and rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `19` |
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
repair, rotation, and cutover retain the version-18 rules.

## Call-auction trade value

The private trade payload grows from 56 B to 72 B. Its field layout relative
to the start of the trade value is:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 8 | trade ID `u64` |
| 8 | 8 | instrument ID `u64` |
| 16 | 8 | instrument definition version `u64` |
| 24 | 8 | buy order ID `u64` |
| 32 | 8 | buy account ID `u64` |
| 40 | 8 | sell order ID `u64` |
| 48 | 8 | sell account ID `u64` |
| 56 | 8 | clearing price `i64` |
| 64 | 8 | positive quantity in lots `u64` |

A call-auction `Trade` event contains its existing 24 B event prefix, 1 B
event-kind tag, and this 72 B value, for 97 B in total. The decoder rejects
zero domain identifiers, equal buy/sell order IDs, invalid quantity, and any
truncation or trailing bytes. Report, checkpoint, risk, and market-data replay
also validate the trade against the owning instrument shard.

## Atomic auction settlement mapping

Settlement is constructed only from one accepted report whose final event is
`UncrossCompleted`, whose event sequence is contiguous, and whose declared
trade/cancellation counts, clearing price, and aggregate quantity equal its
body. Every trade must match the supplied immutable instrument definition.
The caller supplies exactly one globally unique ledger transaction ID per
trade in report order.

For price `p` in raw price quanta, quantity `q` in lots, base units per lot
`b`, and quote units per price unit `c`, checked signed `i128` arithmetic is:

```text
base  = q × b
quote = p × q × c
buyer  : +base asset, -quote asset
seller : -base asset, +quote asset
```

Zero quote value omits both zero quote legs. One trade maps to one kind-`3`
ledger-entry frame; two or more trades map to one kind-`7` ledger-batch frame.
All entries are constructed before ledger mutation. Existing exact transaction
retry, collision, partial-prior-commit, batch, WAL, checkpoint, and recovery
rules then apply unchanged. A same-account buyer/seller pair is rejected by
the DvP entry constructor even when the auction's separate self-trade policy
permitted the pair.

The linked delivery/payment atomicity follows the exchange-of-value boundary
described by [CPSS-IOSCO PFMI Principle 12](https://www.bis.org/cpmi/publ/d101.htm)
and the [CPMI DvP report](https://www.bis.org/cpmi/publ/d06.htm). Quotick's
per-trade transaction mapping, integer formulas, and same-account rule are
internal deterministic contracts; they do not assert venue, clearing-house,
custody, money-settlement, or legal-finality conformance.

## Other payloads

Continuous matching, ledger, checkpoint-anchor, call-auction command, coupled-
risk, and all non-trade call-auction values are byte-identical to version 18.
Version-18 minimum-quantity IOC decrement-and-cancel semantics remain current.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; invalid domain
identifiers; invalid policies; contradictory command/report grammar; invalid
call-auction identity, counts, totals, or sequence; declared length/count
overflow; and all version-18 rejection conditions. Accepted reports must
reproduce their command semantics exactly.

## Compatibility boundary

Only envelope version 19 is accepted. Relabelling a version-18 artifact is
invalid because CRC-32C covers the header and because call-auction trade values
have a different width and identity contract. Authoritative predecessor
migration requires an explicit provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- The exchange-of-value settlement boundary is described by
  [CPSS-IOSCO PFMI Principle 12](https://www.bis.org/cpmi/publ/d101.htm) and
  [CPMI, *Delivery versus payment in securities settlement systems*](https://www.bis.org/cpmi/publ/d06.htm).
- All Quotick field layouts, report-to-transaction mapping, arithmetic, retry,
  and rejection rules are internal contracts verified by stable-codec,
  ledger, WAL, recovery, and checkpoint tests.
