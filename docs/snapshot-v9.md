# Semantic Snapshot Format Version 9

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 9 preserves the version-8 kind registry,
instrument definition, and direct row schemas. Continuous matching and coupled
risk histories contain the WAL-v9 sourced stop-reference values.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `9` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-8 payload bytes.
- Matching kind `2` retains direct rows and changes history values as specified
  below.
- Continuous coupled-risk kind `3` embeds the changed kind-`2` history.
- Call-auction kind `4` retains its version-8 value schema.
- Coupled call-auction/risk kind `5` retains its version-8 value schema.

The instrument definition is the schema in [WAL version 9](wal-v9.md) and is
byte-identical to the version-8 definition.

## Matching checkpoint payload, kind 2

The top-level field order remains:

1. immutable-definition WAL sequence `M` as `u64`;
2. completed report WAL boundary `G` as `u64`;
3. definition length `u32` and version-9 definition payload;
4. active resting-order count `u32` and canonical private order rows;
5. dormant-stop count `u32` and canonical dormant-stop rows;
6. completed command/report count `C` as `u32` and chronological pairs, each
   containing command length/payload then report length/payload.

Active resting-order and dormant-stop direct rows retain their version-8
schemas. The committed stop reference is not duplicated in a direct row. It is
reconstructed from accepted tag-`7` command/tag-`14` completion pairs and must
equal live or restored state.

History command/report bytes use [WAL version 9](wal-v9.md). Every accepted
stop-reference transition proves source binding, contiguous same-version
sequence, contiguous version rollover with sequence `1`, exact backlog
continuation, canonical trigger order, and identical command/event references.
Direct rows must reconcile exactly with chronological acceptance, replacement,
match, refresh, cancel, expiry, control, and stop events. Restoration rebuilds
execution-price, public-price, FIFO, account, expiry, and trigger indexes before
cross-audit.

The physical boundary remains:

```text
G = M + 2 × C.
```

## Continuous coupled-risk payload, kind 3

Kind `3` retains the version-8 outer field order and risk-account rows. Its
embedded matching checkpoint is the version-9 kind-`2` payload above. Dormant
stops retain their complete conservative reservations until activation or
cancellation.

## Call-auction payloads, kinds 4 and 5

Kinds `4` and `5` retain their version-8 payload values. The version-9 envelope
is required so all snapshot kinds share one explicit accepted format boundary.

## Decoder rejection rules

The envelope decoder rejects wrong magic, every version other than `9`, unknown
kind, physical/declared length disagreement, payloads over the caller limit,
CRC mismatch, header/payload generation mismatch, and trailing bytes. Typed
payload decoders additionally reject invalid WAL-v9 tags, identifiers,
noncanonical optional references, lengths, counts, row order, direct/history
disagreement, stop-source lineage, and risk contradictions.

## Compatibility boundary

Only envelope version 9 is accepted. Snapshot versions `1` through `8` are
expired and rejected before payload interpretation. A version-8 image cannot
represent source identity, version, or sequence in matching history.
Authoritative predecessor migration requires an explicit provenance-preserving
converter.

## Primary-source provenance

- CRC-32C follows [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication is bounded by Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all),
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  POSIX.1-2024 [`fsync`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html),
  and [`rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).
- Sourced stop-reference checkpoint fields and lineage validation are Quotick
  internal contracts verified by codec, corruption, direct-restore,
  durable-reopen, market-data, and coupled-risk tests.
