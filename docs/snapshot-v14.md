# Expired Semantic Snapshot Format Version 14

Version 14 is expired. The runtime accepts only
[semantic snapshot format version 15](snapshot-v15.md).

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 14 preserves the version-13 kind registry
and adds the WAL-v14 call-auction priority class to direct active rows and
chronological command/report history.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `14` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-13 payload bytes.
- Matching kind `2` retains its version-13 payload bytes.
- Continuous coupled-risk kind `3` retains its version-13 payload bytes.
- Call-auction kind `4` adds priority class to every direct active-order row
  and uses WAL-v14 values in chronological history.
- Coupled call-auction/risk kind `5` embeds the changed kind-`4` checkpoint and
  retains its version-1 risk-account rows.

The instrument definition remains byte-identical to version 13 and is defined
by [WAL version 14](wal-v14.md).

## Call-auction payload, kind 4

Top-level metadata, phase state, accepted-identity rows, and history framing
retain version 13. Each direct active-order row uses the WAL-v14 active-order
snapshot schema: order ID, account ID, side, constraint, quantity, priority
class `u16`, and priority sequence. Rows remain ordered by `OrderId` in the
canonical checkpoint image.

Each chronological command/report pair uses [WAL version 14](wal-v14.md).
Independent history projection must reproduce every direct row including its
class. Accepted submit and replacement events repeat their command class;
amendment preserves it; cancellations remove it; and partial uncross retains
it with the residual order.

Restoration rebuilds arrival queues in priority-sequence order and rebuilds
allocation scratch under market/price/class/time/ID priority. Queue topology,
scratch order, and process-local account indexes are not persisted. Direct
rows, replayed state, allocation traces, trade IDs, book revision, and phase
revision must agree exactly.

The physical boundary remains:

```text
G = M + 2 × C.
```

## Coupled call-auction/risk payload, kind 5

Kind `5` retains the version-1 outer field order and risk rows documented in
[the coupled risk payload](auction-risk-checkpoint-v1.md). Its embedded auction
checkpoint is the version-14 kind-`4` value above. Risk reservations do not
duplicate priority class because it does not change conservative reservation
magnitude; cross-audit binds each reservation to the authoritative active order,
and replay derives positions and remainders from the class-sensitive trade
trace.

## Decoder rejection rules

The envelope decoder rejects wrong magic, every version other than `14`,
unknown kind, physical/declared length disagreement, payloads over the caller
limit, CRC mismatch, header/payload generation mismatch, and trailing bytes.
Typed payload decoders additionally reject invalid WAL-v14 values, row order,
direct/history class disagreement, amendment class changes, priority-order or
allocation divergence, stop-source lineage errors, and risk contradictions.

## Compatibility boundary

Only envelope version 14 is accepted. Snapshot versions `1` through `13` are
expired and rejected before payload interpretation. A version-13 image lacks
the call-auction priority class in both direct rows and history. Authoritative
predecessor migration requires an explicit provenance-preserving converter;
the runtime does not synthesize a default class.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication is bounded by Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all),
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  POSIX.1-2024
  [`fsync`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html),
  and
  [`rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).
- Priority-class row reconstruction and coupled-risk validation are Quotick
  internal contracts verified by stable-codec, corruption, direct-restore,
  full-WAL, snapshot-suffix, market-data, and risk tests.
