# WAL Format Version 5

This document is the authoritative byte-level schema for Quotick WAL version
5. All multibyte integers are little-endian. Rust enum layout, padding,
pointer identity, collection capacity, and platform ABI are never persisted.

Version 5 preserves the version-4 frame layout, record-kind registry, and
payload bytes except for continuous matching commands and execution reports
(kinds `1` and `2`). Those two payloads add deterministic good-til-timestamp
(GTD) order expiry. The runtime accepts only version 5; versions `1` through
`4` are expired envelopes and are rejected before payload interpretation.

## Frame

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `5` |
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
[version 4](wal-v4.md).

## Unchanged payloads

Kinds `3` through `10` are byte-for-byte the payloads specified by
[WAL version 4](wal-v4.md). Their semantic validation is unchanged. A kind-`8`
checkpoint anchor identifies a version-5 snapshot even though its anchor
payload bytes are unchanged.

## Continuous command payload, kind 1

Existing command tags and field order are unchanged: new `0`, cancel `1`,
replace `2`, mass cancel `3`, account control `4`, and instrument trading-state
control `5`. Expiry sweep is tag `6`, followed by:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | command ID |
| 2 | 8 | instrument ID |
| 3 | 8 | instrument version |
| 4 | 8 | inclusive expiry horizon as `TimestampNs` |
| 5 | 8 | receive timestamp as `TimestampNs` |

For a tag-`0` new order, time-in-force tags remain GTC `0`, IOC `1`, FOK `2`,
and post-only `3`. GTD is tag `4` followed by its absolute expiration as one
`u64` Unix-nanosecond timestamp. All other tag-`0` fields remain those defined
by [WAL version 3](wal-v3.md#command-payload).

An admitted GTD deadline is strictly later than the new command's receive
timestamp and strictly later than the shard's committed inclusive expiry
watermark, if one exists. A sweep horizon cannot exceed its own receive
timestamp and cannot be less than the committed watermark. Equal-watermark
sweeps are valid monotonic acknowledgements; exact command retries are inert.

## Continuous execution-report payload, kind 2

The report prefix and event prefix are unchanged: command ID `u64`, outcome,
replay `bool`, event count `u32`; then event sequence `u64`, command ID `u64`,
occurrence timestamp `u64`, and event-kind tag.

Existing event tags `0` through `10` retain their version-4 meanings and bytes.
Expiry-sweep completion is event tag `11`, followed by:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | previous-watermark-present `bool` |
| 2 | 8 | previous watermark, or canonical zero when absent |
| 3 | 8 | current inclusive watermark |
| 4 | 8 | expired-order count `u64` |
| 5 | 16 | expired total-leaves quantity `u128` |

Cancellation-reason tag `7` means expired. Rejection tags `38`, `39`, and `40`
mean order already expired, expiry-watermark regression, and expiry horizon
after command time, respectively. Existing cancellation tags `0` through `6`
and rejection tags `0` through `37` are unchanged.

An accepted sweep emits one tag-`3` cancellation with reason `7` per selected
resting GTD order, strictly ordered by `(expiration timestamp, OrderId)`, then
exactly one tag-`11` completion. Each selected expiration is at or before the
command horizon. The completion previous/current watermarks must equal the
pre-command watermark and command horizon; its count and `u128` quantity must
equal the preceding cancellations. An empty sweep emits only the zero-count
completion.

Continuous risk treats tag-`6` commands as account-independent controls.
Ordinary cancellation-event processing releases each expired order's complete
remaining reservation; tag `11` has no additional risk effect.

## Compatibility boundary

Only envelope version 5 is accepted. Re-labeling a version-4 frame is invalid:
CRC-32C covers the header, and version-4 readers do not define the new payload
tags. Migration of authoritative version-4 artifacts requires an explicit
provenance-preserving converter; the runtime does not infer deadlines or
watermarks.

## Primary-source provenance

- CRC-32C follows [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- The expiry command, ordering, tags, and replay grammar are Quotick internal
  deterministic contracts verified by repository golden-byte, recovery, and
  differential state tests; they are not attributed to an external venue.
