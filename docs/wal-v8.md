# WAL Format Version 8

This document is the authoritative byte-level schema for Quotick WAL version
8. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 8 preserves the version-7 frame, record-kind registry, instrument
definition, and all existing value bytes. Continuous matching adds an explicit
minimum-quantity IOC lifetime and its typed admission/cancellation outcomes.
The runtime accepts only version 8; versions `1` through `7` are expired
envelopes and are rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `8` |
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
repair, sequence, rotation, and cutover retain the version-7 rules.

## Continuous command payload, kind 1

Command tags, field order, display representation, order-type representation,
self-trade-prevention tags, and all existing lifetime bytes remain those of
[WAL version 7](wal-v7.md). Every `TimeInForce` field uses this registry:

| Tag | Meaning | Additional bytes |
|---:|---|---:|
| `0` | good until cancelled | 0 |
| `1` | immediate or cancel | 0 |
| `2` | fill or kill | 0 |
| `3` | post only | 0 |
| `4` | good until timestamp | absolute UTC nanoseconds `u64` |
| `5` | immediate or cancel with minimum quantity | minimum quantity in lots `u64` |

Tag `5` requires a non-zero minimum no greater than original order quantity
and aligned to the instrument lot increment. It is immediate-only, so reserve
and fully hidden incoming display qualifiers are invalid. Decrement-and-cancel
self-trade prevention is unsupported for this lifetime. A dormant stop retains
the same tag and quantity for activation-time evaluation; a stop-limit
replacement cannot reduce total leaves below the minimum.

## Continuous execution-report payload, kind 2

The report and event layouts remain those of version 7. The rejection-reason
registry adds:

| Tag | Meaning |
|---:|---|
| `50` | `InvalidMinimumQuantity` |
| `51` | `UnsupportedMinimumQuantitySelfTradePolicy` |

Existing rejection tags `0` through `49` are unchanged. The cancellation-
reason registry adds tag `11`, `MinimumQuantityUnavailable`; existing
cancellation tags `0` through `10` are unchanged.

The minimum is a precondition, not an execution cap. Matching first performs a
nonmutating scan for the required external trade quantity using price priority,
reserve/hidden queue priority, and the selected supported self-trade policy. If
eligible quantity is below the minimum, `OrderAccepted` is followed by an
`OrderCancelled` for the complete incoming quantity with cancellation tag
`11`; no maker or STP state changes. If the threshold is met, ordinary IOC
matching can execute beyond it and any remaining incoming quantity uses
`UnfilledRemainder` tag `1`.

For a triggered stop, `StopOrderTriggered` precedes the same activation-time
test. Threshold failure cancels the complete triggered quantity with tag `11`.

## Payloads with unchanged value schemas

Instrument definition kind `4` and its fully hidden-support boolean retain the
version-7 schema. Ledger entry kind `3`, account-risk definition kind `5`,
ledger correction kind `6`, ledger batch kind `7`, checkpoint anchor kind `8`,
call-auction command kind `9`, and call-auction report kind `10` retain their
version-7 value schemas.

## Decoder rejection rules

The decoder rejects unknown record, command, TIF, rejection, cancellation,
event, display, order-type, STP, and nested payload tags; truncation; trailing
bytes; zero domain identifiers or quantities; noncanonical booleans; declared
length/count overflow; and every reconstructed domain or report-grammar
violation. Matching business validation separately rejects the invalid
minimum/STP combinations above before state mutation.

## Compatibility boundary

Only envelope version 8 is accepted. Version-7 decoders do not define TIF tag
`5`, rejection tags `50`/`51`, or cancellation tag `11`. Re-labelling a frame
is invalid because CRC-32C covers its header and because new values cannot be
inferred. Migration of authoritative version-7 artifacts requires an explicit
provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- FIX `MinQty(110)` defines the minimum quantity of an order to be executed,
  and FIX `TimeInForce(59)` defines IOC (`3`) in the
  [FIX Latest field registry](https://fiximate.fixtrading.org/en/FIX.Latest/fields_sorted_by_tagnum.html).
- The combined TIF, atomic preflight, STP/reserve/hidden behavior, stop
  activation, tags, and replay grammar are Quotick internal deterministic
  contracts verified by codec, matching, market-data, checkpoint, risk, and
  durable-recovery tests; they are not attributed to an external venue.
