# WAL Format Version 15

This document is the authoritative byte-level schema for Quotick WAL version
15. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 15 preserves the version-14 frame, record-kind registry, and all
existing values. It adds a sequenced call-auction indicative command, event,
and action. The runtime accepts only version 15; versions `1` through `14` are
expired and rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `15` |
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
repair, rotation, and cutover retain the version-14 rules.

## Call-auction indicative command

Call-auction command tag `7` is `Indicative`. The complete command is 75 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 1 | command tag `7` |
| 1 | 8 | non-zero command ID |
| 9 | 8 | non-zero instrument ID |
| 17 | 8 | non-zero instrument version |
| 25 | 8 | non-zero auction ID |
| 33 | 8 | non-zero expected phase revision |
| 41 | 8 | minimum price `i64` |
| 49 | 8 | maximum price `i64` |
| 57 | 8 | reference price `i64` |
| 65 | 1 | pressure rule: ignore `0`, favor imbalance `1` |
| 66 | 1 | final tie: lower `0`, higher `1` |
| 67 | 8 | receive timestamp, Unix nanoseconds `u64` |

The price band is inclusive and ordered. Instrument validation additionally
requires both endpoints to align to the instrument tick grid and remain inside
its collar. Command tags `0` through `6` retain their version-14 layouts.

## Indicative state and event

Call-auction event-kind tag `9` is `IndicativePublished`. Its state begins
immediately after the tag:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | non-zero auction ID |
| 2 | 8 | non-zero phase revision |
| 3 | 8 | collection-book revision |
| 4 | 8 | inclusive minimum price `i64` |
| 5 | 8 | inclusive maximum price `i64` |
| 6 | 8 | reference price `i64` |
| 7 | 1 | pressure-rule tag |
| 8 | 1 | final-tie tag |
| 9 | 1 | clearing-present canonical boolean |
| 10 | 0 or 40 | optional clearing value |

The fixed state is 51 B. When clearing is present, its 40 B value is clearing
price `i64`, aggregate buy quantity `u128`, and aggregate sell quantity
`u128`, for a 91 B state. Present clearing must have non-zero executable
quantity and a price inside the encoded band. Absence is canonical when the
observed book has no executable interest; it is still a successful
publication.

Every event retains the 24 B sequence, command-ID, and timestamp prefix.
Therefore an indicative event is 76 B without clearing and 116 B with
clearing. An accepted one-event report is respectively 98 B or 138 B,
including the report command ID, command sequence, accepted outcome tag,
`u32` event count, and final replay boolean.

## Deterministic semantics

The command is accepted only in `Collecting` or `Frozen` for the exact active
auction, instrument version, and phase revision. Discovery observes the
current collection-book revision and reuses the ordinary banded clearing
kernel. Acceptance emits exactly one event and changes neither book revision
nor risk state.

The engine retains at most one current indication. Any accepted
non-indicative command invalidates it, including an empty mass cancellation or
a phase transition. A rejection and an exact idempotent retry preserve it;
the retry emits no new event. The indication is reconstructed from accepted
history during checkpoint restore rather than duplicated in a direct row.

`ActionNotAllowed` action tag `7` identifies `Indicative`. Existing action tags
`0` through `6`, event tags `0` through `8`, rejection tags, order values,
priority classes, and all non-auction payloads retain version-14 layouts.

## Decoder rejection rules

The decoder rejects unknown tags; truncation; trailing bytes; zero domain
identifiers or phase revisions; inverted price bands; noncanonical booleans;
unknown price-policy tags; present clearing with zero executable quantity or
price outside the band; declared length/count overflow; and reconstructed
domain or report-grammar violations. Accepted indicative reports must contain
exactly one matching event. Rejected reports retain the ordinary single
`CommandRejected` grammar.

## Compatibility boundary

Only envelope version 15 is accepted. A version-14 decoder has no indicative
command, action, or event tags. Relabelling is invalid because CRC-32C covers
the header and because the new values require version-15 semantic validation.
Authoritative predecessor migration requires an explicit
provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Nasdaq documents dissemination of paired shares, imbalance quantity and
  side, reference prices, and indicative or likely clearing prices in its
  [Opening and Closing Cross fact sheet](https://www.nasdaqtrader.com/content/productsservices/trading/crosses/fact_sheet.pdf).
- Nasdaq specifies field-level NOI values and dissemination behavior in its
  [NOIView specification](https://nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NOIViewSpecification.pdf).
- Quotick's tags, state binding, invalidation rule, and nullable representation
  are internal deterministic contracts. They do not claim venue-protocol
  compatibility.
