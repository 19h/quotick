# Expired Semantic Snapshot Format Version 16

This is a historical byte-level record. The runtime rejects version 16. The
authoritative current schema is
[semantic snapshot format version 17](snapshot-v17.md).

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 16 preserves the version-15 kind registry
and direct row schemas. Call-auction history uses WAL-v16 values and therefore
retains the explicit abort-on-self-trade policy and its sequenced rejection.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `16` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-15 payload bytes.
- Matching kind `2` retains its version-15 payload bytes.
- Continuous coupled-risk kind `3` retains its version-15 payload bytes.
- Call-auction kind `4` retains version-15 direct rows and uses WAL-v16 values
  in chronological command/report history.
- Coupled call-auction/risk kind `5` embeds the changed kind-`4` checkpoint and
  retains its version-1 risk-account rows.

The instrument definition and call-auction order rows remain defined by
[WAL version 16](wal-v16.md).

## Call-auction payload, kind 4

Top-level metadata, phase state, accepted-identity rows, active-order rows, and
history framing retain version 15. Each chronological command/report pair uses
[WAL version 16](wal-v16.md), including self-trade policy tag `1` and rejection
tag `23`.

A rejected abort row preserves phase, phase revision, active auction, book
revision, next trade ID, active orders, and the latest valid indication. Direct
restore and independent full-history replay must reproduce that unchanged
state and the exact cached rejection. A later exact retry adds no history row.

An accepted `Abort` uncross is restored only when its canonical trade trace has
no equal buyer/seller account pair. It otherwise follows the unchanged
allocation, remainder, counter, direct-row, and per-cycle projection rules.
No alternative-counterparty state is stored or inferred.

The optional current indication remains derived from accepted history rather
than duplicated in a direct row. Canonical active-order rows remain ordered by
`OrderId`; restoration rebuilds arrival queues and allocation scratch under
market/price/class/time/ID priority. Queue topology, scratch order,
process-local account indexes, and market-data state are not persisted.

The physical boundary remains:

```text
G = M + 2 × C.
```

## Coupled call-auction/risk payload, kind 5

Kind `5` retains the version-1 outer field order and risk rows documented in
[the coupled risk payload](auction-risk-checkpoint-v1.md). Its embedded auction
checkpoint is the version-16 kind-`4` value above. A self-trade rejection has
no risk effect; replay must preserve every reservation, exposure, and position.

## Decoder rejection rules

The envelope decoder rejects wrong magic, every version other than `16`,
unknown kind, physical/declared length disagreement, payloads over the caller
limit, CRC mismatch, header/payload generation mismatch, and trailing bytes.
Typed payload decoders additionally reject invalid WAL-v16 values, row order,
direct/history disagreement, invalid abort rejection/completion grammar,
priority-order or allocation divergence, and risk contradictions.

## Compatibility boundary

Only envelope version 16 is accepted. Snapshot versions `1` through `15` are
expired and rejected before payload interpretation. A version-15 image cannot
contain the new call-auction policy or rejection values. Authoritative
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
- Abort reconstruction and coupled-risk neutrality are Quotick internal
  contracts verified by stable-codec, direct-restore, full-WAL, risk, and
  retry tests.
