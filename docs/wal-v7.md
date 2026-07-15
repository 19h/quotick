# Expired WAL Format Version 7

This document is the authoritative byte-level schema for Quotick WAL version
7. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 7 preserves the version-6 frame and record-kind registry. It adds an
instrument-level fully hidden-order capability, display tag `2`, and the
matching outcomes required to replay hidden liquidity. The runtime accepts
only version 7; versions `1` through `6` are expired envelopes and are rejected
before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `7` |
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
repair, sequence, and cutover retain the version-6 rules.

## Instrument-definition payload, kind 4

The payload field order is:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | instrument ID |
| 2 | 8 | definition version |
| 3 | 8 | effective-from UTC nanoseconds |
| 4 | 1 + `N` | symbol length `u8`, then `N` UTF-8 bytes |
| 5 | 1 | instrument-kind tag |
| 6 | 8 | base-asset ID |
| 7 | 8 | quote-asset ID |
| 8 | 1 | price decimal scale |
| 9 | 8 | tick size in raw price quanta |
| 10 | 8 | minimum raw signed price |
| 11 | 8 | maximum raw signed price |
| 12 | 8 | quantity increment in lots |
| 13 | 8 | minimum quantity in lots |
| 14 | 8 | maximum quantity in lots |
| 15 | 4 | maximum reserve replenishments |
| 16 | 1 | fully hidden orders supported, canonical `bool` |
| 17 | 8 | base units per lot |
| 18 | 8 | quote units per raw price unit |
| 19 | 1 | initial trading-state tag |

The new field at order 16 is `0` or `1`; every other byte retains the
version-6 representation. The definition payload is also used by the initial
metadata record of continuous and call-auction journals and by snapshot
payloads that embed an instrument definition.

## Order-display representation

Every display field uses one of these tags:

| Tag | Meaning | Additional bytes |
|---:|---|---:|
| `0` | fully displayed | 0 |
| `1` | native reserve | peak quantity `u64` |
| `2` | fully hidden | 0 |

A fully hidden order is valid only when the definition enables it. It is
accepted only for a limit order with a resting-capable time in force. Hidden
orders have all remaining leaves executable at their limit price, expose no
public quantity or order count, and follow the deterministic equal-price queue
class specified in the architecture.

Conversion among fully displayed, reserve, and fully hidden modes is rejected
by replacement. A same-price hidden quantity reduction retains priority;
quantity increase or price change loses priority within the hidden class.

## Continuous command payload, kind 1

Command tags and field order remain those of
[WAL version 6](wal-v6.md). Display fields in new and replace commands use the
version-7 table above. Stop-limit orders may carry fully hidden display when
the activation constraint and time in force can rest; stop-market and
immediate-only orders cannot.

## Continuous execution-report payload, kind 2

The report and event layouts remain those of version 6, with these explicit
version-7 additions and semantics:

- `OrderAccepted` and `OrderReplaced` display fields accept tag `2`.
- `OrderRested` event tag `1` retains the byte order `order ID`, raw price,
  total leaves, and one non-zero quantity. The final quantity is the executable
  working quantity: it is total leaves for fully displayed and fully hidden
  orders, and the current peak-bounded slice for reserve orders. Public
  visibility is derived from the display carried by the accepted order.
- rejection tag `48` is `HiddenOrderNotSupported`;
- rejection tag `49` is `HiddenOrderCannotBeImmediate`.

Existing rejection tags `0` through `47`, cancellation tags, event tags, stop
grammar, and all bytes representing existing values are unchanged.

## Call-auction report extension, kind 10

The admission-error registry used inside call-auction reports retains tags
`0` through `10` and assigns tag `11` to `HiddenOrderNotSupported`. The
current call-auction order model has no display qualifier, so ordinary auction
commands do not produce this value. The tag is nevertheless explicit so the
shared instrument admission enum remains total and stable.

## Payloads with unchanged value schemas

Ledger entry kind `3`, account-risk definition kind `5`, ledger correction
kind `6`, ledger batch kind `7`, checkpoint anchor kind `8`, and call-auction
command kind `9` retain their version-6 value schemas. Kind `10` retains all
existing bytes and only extends the admission-error tag registry as stated
above.

## Compatibility boundary

Only envelope version 7 is accepted. A version-6 definition lacks the hidden-
support boolean, and version-6 decoders do not define display tag `2` or
rejection tags `48` and `49`. Re-labeling a frame is invalid because CRC-32C
covers its header and because absent fields cannot be inferred. Migration of
authoritative version-6 artifacts requires an explicit provenance-preserving
converter.

## Primary-source provenance

- CRC-32C follows [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Fully hidden admission, displayed-before-hidden continuous priority, tags,
  and replay grammar are Quotick internal deterministic contracts verified by
  codec, matching, market-data, checkpoint, risk, and durable-recovery tests;
  they are not attributed to an external venue.
