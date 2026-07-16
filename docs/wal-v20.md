# WAL Format Version 20

This document is the authoritative byte-level schema for Quotick WAL version
20. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 20 preserves the version-19 frame, record-kind registry, and all
existing payload values. It adds continuous market-to-limit command, pricing-
event, and rejection tags. The runtime accepts only version 20; versions `1`
through `19` are expired and rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `20` |
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
repair, rotation, and cutover retain the version-19 rules.

## Continuous market-to-limit values

Continuous `NewOrder` retains command tag `0` and its existing field order.
The `OrderType` registry adds tag `3`, `MarketToLimit`, with no price payload.
The next byte is therefore the ordinary `TimeInForce` tag. Tags `0` `Market`,
`1` `Limit(i64)`, and `2` `Stop` retain their version-19 bytes.

An accepted market-to-limit report begins with the ordinary `OrderAccepted`
event and then contains exactly one `MarketToLimitPriced` event before any
trade, self-trade prevention, cancellation, refresh, or residual-rest event.
Event-kind tag `15` has this value layout:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 1 | event-kind tag `15` |
| 1 | 8 | accepted order ID `u64` |
| 9 | 8 | captured limit price `i64` |

The complete event prepends the existing 24 B sequence, command-ID, and
timestamp prefix, so it is 41 B. The captured price is the best executable
opposite price at acceptance, including a hidden-only best level. The matching
kernel converts the incoming constraint to an ordinary limit at exactly this
price. It cannot sweep a worse level. Any residual rests at the captured price
under its submitted display and GTC or GTD lifetime.

The rejection registry appends:

| Tag | Variant | Meaning |
|---:|---|---|
| 56 | `MarketToLimitBookEmpty` | No executable opposite price existed |
| 57 | `MarketToLimitRequiresRestingLifetime` | TIF was IOC, minimum IOC, FOK, or post-only |

These are ordinary sequenced business rejections. They do not accept or
consume the submitted order ID. Lifetime validation precedes opposite-book
capture. Direct stop activation does not accept `MarketToLimit`; the stop
activation registry remains market-or-limit only.

Risk authorization values the unpriced command over the full signed collar.
After matching, any residual reservation uses the captured limit constraint.
The report remains the replay authority: exact retry, raw WAL recovery,
checkpoint replay, and market-data publication require the same pricing event
and residual state.

## Preserved version-19 values

The 72 B private call-auction trade retains its version-19 instrument ID and
definition version. All call-auction, ledger, checkpoint-anchor, instrument,
risk-definition, continuous non-market-to-limit, and market-data values are
otherwise byte-identical to version 19.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; invalid domain
identifiers; invalid policies; contradictory command/report grammar; declared
length/count overflow; and all version-19 rejection conditions. Matching,
recovery, risk, and publication additionally reject a missing, duplicate,
misidentified, repriced, late, or execution-inconsistent market-to-limit
pricing event.

## Compatibility boundary

Only envelope version 20 is accepted. Relabelling a version-19 artifact is
invalid because CRC-32C covers the header and version 19 does not define the
new tags. Authoritative predecessor migration requires an explicit
provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- ASX defines market-to-limit at the venue boundary as execution at the best
  opposing price with an unfilled remainder resting there as a limit order in
  [ASX 24 Operating Rules, Procedure 4020](https://www.asx.com.au/content/dam/asx/rules-guidance-notes-waivers/asx-24-operating-rules/rules/ASX-24-Operating-Rules-Section-04.pdf).
- Quotick's hidden-liquidity capture, accepted lifetimes, STP ordering, risk
  valuation, exact event grammar, and wire tags are internal deterministic
  contracts verified by matching, codec, risk, market-data, WAL, and
  checkpoint tests; they do not assert ASX protocol conformance.
