# WAL Format Version 6

This document is the authoritative byte-level schema for Quotick WAL version
6. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 6 preserves the version-5 frame layout, record-kind registry, and
payload bytes except for continuous matching commands and execution reports
(kinds `1` and `2`). Those payloads add deterministic dormant stop orders and
explicit stop-reference sweeps. The runtime accepts only version 6; versions
`1` through `5` are expired envelopes and are rejected before payload
interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `6` |
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
bytes 12--15 zeroed, followed by the payload. The segmented directory, writer
lease, repair, sequencing, and cutover rules are unchanged from
[version 5](wal-v5.md).

## Unchanged payloads

Kinds `3` through `10` are byte-for-byte the payloads specified by
[WAL version 5](wal-v5.md). Their semantic validation is unchanged. A kind-`8`
checkpoint anchor identifies a version-6 snapshot even though its anchor
payload bytes are unchanged.

## Continuous command payload, kind 1

Existing command tags `0` through `6` and their field order are unchanged.
Stop-trigger sweep is command tag `7`, followed by:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | command ID |
| 2 | 8 | instrument ID |
| 3 | 8 | instrument version |
| 4 | 8 | new reference as raw signed `Price` |
| 5 | 4 | maximum activations as `u32` |
| 6 | 8 | receive timestamp as `TimestampNs` |

For a tag-`0` new order, order-type tags are market `0`, limit `1` followed by
one raw signed `Price`, and stop `2`. Stop is followed by the raw signed trigger
price and one activation constraint: market tag `0`, or limit tag `1` followed
by one raw signed `Price`. Time-in-force, display, self-trade-prevention, and
all other tag-`0` fields retain their version-5 representation.

A stop is admitted only after a reference has been initialized. A buy trigger
must be strictly above the committed reference; a sell trigger must be strictly
below it. Stop-market post-only and stop-market replacement are rejected.
Stop-limit replacement does not amend the trigger. Quantity reduction at the
same activation limit and display retains trigger priority; other accepted
stop-limit amendments receive the replacement event sequence as priority.

`maximum activations` is positive and no greater than the configured active-
order bound. If stops eligible at the current reference remain, another sweep
at a different reference is rejected until a same-reference continuation
drains that backlog. Exact command retries remain inert.

## Continuous execution-report payload, kind 2

The report prefix and event prefix remain: command ID `u64`, outcome, replay
`bool`, event count `u32`; then event sequence `u64`, command ID `u64`,
occurrence timestamp `u64`, and event-kind tag. Existing event tags `0` through
`11` retain their version-5 meanings and bytes.

Stop-order armed is event tag `12`, followed by:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | order ID |
| 2 | 8 | raw signed trigger price |
| 3 | 1 or 9 | activation: market `0`, or limit `1` plus raw price |
| 4 | 8 | trigger-priority event sequence |

Stop-order triggered is event tag `13`, followed by order ID `u64`, raw signed
trigger price `i64`, raw signed satisfying reference `i64`, and retained
priority sequence `u64`.

Stop-trigger-sweep completion is event tag `14`, followed by:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | previous-reference-present `bool` |
| 2 | 8 | previous raw price, or canonical zero when absent |
| 3 | 8 | current raw reference price |
| 4 | 8 | triggered-order count `u64` |
| 5 | 8 | remaining eligible-order count `u64` |

Rejection tags `41` through `47` respectively mean stop reference unavailable,
stop already triggered, empty trigger batch, batch exceeds active-order
capacity, trigger backlog, stop-market cannot post, and stop-market cannot be
replaced. Existing rejection tags `0` through `40` are unchanged.

Cancellation-reason tags `8`, `9`, and `10` respectively mean triggered FOK
unfilled, triggered post-only would cross, and triggered residual capacity
unavailable. Existing cancellation tags `0` through `7` are unchanged.

An accepted sweep emits zero or more tag-`13` events, all execution effects for
each activated order, then exactly one tag-`14` completion. Eligible buy stops
are selected by ascending `(trigger price, priority sequence, order ID)`;
eligible sell stops use descending trigger price, then ascending priority
sequence and order ID. Because accepted buy triggers begin strictly above the
committed reference and accepted sell triggers strictly below it, one reference
move can make at most one side eligible. The event trace, completion counts,
remaining backlog, and committed reference must be mutually consistent.

Triggered orders use ordinary matching, self-trade prevention, reservation,
GTD, and residual-resting rules. An activation-time FOK failure, post-only
cross, or unavailable residual price-level capacity cancels the complete
triggered quantity with the typed reason above; it does not partially mutate
the book.

## Compatibility boundary

Only envelope version 6 is accepted. Re-labeling a version-5 frame is invalid:
CRC-32C covers the header, and version-5 readers do not define the new payload
tags. Migration of authoritative version-5 artifacts requires an explicit
provenance-preserving converter; the runtime does not infer reference prices,
triggers, or activation order.

## Primary-source provenance

- CRC-32C follows [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Stop-reference authority, canonical activation order, tags, and replay
  grammar are Quotick internal deterministic contracts verified by repository
  golden-byte, checkpoint, recovery, market-data, and risk tests; they are not
  attributed to an external venue.
