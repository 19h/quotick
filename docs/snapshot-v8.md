# Semantic Snapshot Format Version 8

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 8 preserves the version-7 kind registry,
instrument definition, and direct row schemas. Continuous matching and coupled
risk histories can contain the WAL-v8 minimum-quantity IOC command/report
values.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `8` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-7 payload bytes.
- Matching kind `2` retains direct rows and changes history values as specified
  below.
- Continuous coupled-risk kind `3` embeds the changed kind-`2` history.
- Call-auction kind `4` retains its version-7 value schema.
- Coupled call-auction/risk kind `5` retains its version-7 value schema.

The instrument definition is the schema in [WAL version 8](wal-v8.md) and is
byte-identical to the version-7 definition.

## Matching checkpoint payload, kind 2

The top-level field order remains:

1. immutable-definition WAL sequence `M` as `u64`;
2. completed report WAL boundary `G` as `u64`;
3. definition length `u32` and version-8 definition payload;
4. active resting-order count `u32` and canonical private order rows;
5. dormant-stop count `u32` and canonical dormant-stop rows;
6. completed command/report count `C` as `u32` and chronological pairs, each
   containing command length/payload then report length/payload.

Active resting-order rows remain the version-7 52 B fully displayed/hidden or
60 B reserve schemas. Minimum-quantity IOC is immediate-only and therefore
cannot appear as an active resting row.

Dormant-stop rows retain the version-7 field order. Their TIF field accepts
WAL-v8 tag `5` followed by the 8 B minimum quantity. The minimum must be
lot-grid aligned and no greater than dormant leaves. It is checked against
eligible external liquidity only when the stop activates.

History command/report bytes use [WAL version 8](wal-v8.md), including TIF tag
`5`, rejection tags `50`/`51`, and cancellation tag `11`. Direct rows must
reconcile exactly with chronological acceptance, replacement, match, refresh,
cancel, expiry, control, and stop events. Restoration rebuilds execution-price,
public-price, FIFO, account, expiry, and trigger indexes before cross-audit.

The physical boundary remains:

```text
G = M + 2 × C.
```

## Continuous coupled-risk payload, kind 3

Kind `3` retains the version-7 outer field order and risk-account rows. Its
embedded matching checkpoint is the version-8 kind-`2` payload above. An
immediate minimum-quantity order retains no reservation; a dormant stop retains
its complete conservative reservation until activation or cancellation.

## Call-auction payloads, kinds 4 and 5

Kinds `4` and `5` retain their version-7 payload values. Their current auction
order model has no minimum-quantity qualifier. The version-8 envelope is still
required so all snapshot kinds share one explicit accepted format boundary.

## Decoder rejection rules

The envelope decoder rejects wrong magic, every version other than `8`, unknown
kind, physical/declared length disagreement, payloads over the caller limit,
CRC mismatch, header/payload generation mismatch, and trailing bytes. Typed
payload decoders additionally reject all invalid WAL-v8 tags, lengths, counts,
domain values, row order, direct/history disagreement, and lineage or risk
contradictions.

## Compatibility boundary

Only envelope version 8 is accepted. Snapshot versions `1` through `7` are
expired and rejected before payload interpretation. A version-7 image cannot
represent the new TIF and outcome tags in matching history or dormant-stop
state. Authoritative predecessor migration requires an explicit provenance-
preserving converter.

## Primary-source provenance

- CRC-32C follows [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication is bounded by Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all),
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  POSIX.1-2024 [`fsync`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html),
  and [`rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).
- Minimum-quantity checkpoint fields and validation are Quotick internal
  contracts verified by codec, corruption, direct-restore, durable-reopen,
  market-data, and coupled-risk tests.
