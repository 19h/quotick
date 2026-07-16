# Expired Semantic Snapshot Format Version 15

Version 15 is expired. The runtime accepts only
[semantic snapshot format version 16](snapshot-v16.md).

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 15 preserves the version-14 kind registry
and direct row schemas. Call-auction history uses WAL-v15 values and can
therefore reconstruct the latest valid sequenced indicative state.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `15` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-14 payload bytes.
- Matching kind `2` retains its version-14 payload bytes.
- Continuous coupled-risk kind `3` retains its version-14 payload bytes.
- Call-auction kind `4` retains version-14 direct rows and uses WAL-v15 values
  in chronological command/report history.
- Coupled call-auction/risk kind `5` embeds the changed kind-`4` checkpoint and
  retains its version-1 risk-account rows.

The instrument definition and call-auction priority-class rows remain defined
by [WAL version 15](wal-v15.md).

## Call-auction payload, kind 4

Top-level metadata, phase state, accepted-identity rows, active-order rows,
and history framing retain version 14. Each chronological command/report pair
uses [WAL version 15](wal-v15.md), including command tag `7` and event-kind tag
`9` for indicative publication.

The direct checkpoint image does not add a second indicative field.
Restoration derives the latest indication by scanning accepted history:

- accepted `Indicative` replaces the retained indication;
- any accepted non-indicative command clears it;
- a rejected command preserves it; and
- an exact retry is already represented by its original history row and adds
  no row or event.

The resulting state must bind the current active auction ID, phase revision,
and book revision and must exist only in `Collecting` or `Frozen`. Direct
restore, complete replay, and a checkpoint-plus-WAL suffix must reproduce the
same optional value. An accepted suffix command applies the same invalidation
rule.

Canonical active-order rows remain ordered by `OrderId` and retain the
WAL-v14 priority class. Restoration rebuilds arrival queues and allocation
scratch under market/price/class/time/ID priority. Queue topology, scratch
order, process-local account indexes, and market-data state are not persisted.

The physical boundary remains:

```text
G = M + 2 × C.
```

## Coupled call-auction/risk payload, kind 5

Kind `5` retains the version-1 outer field order and risk rows documented in
[the coupled risk payload](auction-risk-checkpoint-v1.md). Its embedded auction
checkpoint is the version-15 kind-`4` value above. Indicative events have no
risk effect and create no reservation, exposure, or position field. Coupled
restore nevertheless validates the same engine history before accepting the
unchanged risk image.

## Decoder rejection rules

The envelope decoder rejects wrong magic, every version other than `15`,
unknown kind, physical/declared length disagreement, payloads over the caller
limit, CRC mismatch, header/payload generation mismatch, and trailing bytes.
Typed payload decoders additionally reject invalid WAL-v15 values, row order,
direct/history disagreement, invalid indication grammar or binding,
priority-order or allocation divergence, stop-source lineage errors, and risk
contradictions.

## Compatibility boundary

Only envelope version 15 is accepted. Snapshot versions `1` through `14` are
expired and rejected before payload interpretation. A version-14 image cannot
contain the new call-auction command and event values. Authoritative
predecessor migration requires an explicit provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication is bounded by Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all),
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  POSIX.1-2024
  [`fsync`](https://pubs.opengroup.org/onlinepubs/009695399/functions/fsync.html),
  and
  [`rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).
- Indicative reconstruction and coupled-risk neutrality are Quotick internal
  contracts verified by stable-codec, corruption, direct-restore, full-WAL,
  snapshot-suffix, market-data, and risk tests.
