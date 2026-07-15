# Expired Semantic Snapshot Format Version 7

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 7 preserves the version-6 kind registry and
adds fully hidden continuous-order state plus the version-7 instrument
definition to every payload that embeds one.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `7` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-6 payload bytes.
- Matching kind `2` changes as specified below.
- Continuous coupled-risk kind `3` embeds the changed kind-`2` payload.
- Call-auction kind `4` embeds the version-7 instrument definition.
- Coupled call-auction/risk kind `5` embeds the changed kind-`4` payload.

The instrument definition is the schema in [WAL version 7](wal-v7.md) and
includes the canonical fully hidden-support boolean after the maximum reserve-
replenishment count.

## Matching checkpoint payload, kind 2

The top-level field order remains:

1. immutable-definition WAL sequence `M` as `u64`;
2. completed report WAL boundary `G` as `u64`;
3. definition length `u32` and version-7 definition payload;
4. active resting-order count `u32` and canonical private order rows;
5. dormant-stop count `u32` and canonical dormant-stop rows;
6. completed command/report count `C` as `u32` and chronological pairs, each
   containing command length/payload then report length/payload.

### Active resting-order row

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | order ID |
| 2 | 8 | account ID |
| 3 | 1 | side |
| 4 | 8 | raw signed limit price |
| 5 | 8 | total leaves |
| 6 | 8 | executable working quantity |
| 7 | 1 or 9 | display: full `0`, reserve `1` plus peak, hidden `2` |
| 8 | 1 | self-trade-prevention policy |
| 9 | 9 | expiration-present `bool` plus timestamp or canonical zero |

Fully displayed and fully hidden rows are 52 B; reserve rows are 60 B. The
working quantity equals total leaves for full and hidden rows. For reserve it
is positive, no greater than the peak, and no greater than total leaves. A
hidden row is valid only when the embedded definition enables hidden orders.

Rows are canonicalized as buys then sells, ascending raw price within each
side. At one price, every fully displayed or reserve row precedes every hidden
row; FIFO is retained within each class. Reserve refresh requeues at the tail
of the displayed-priority class, still ahead of hidden liquidity.

### Dormant-stop row

Dormant-stop fields and ordering retain the version-6 schema, except display
tag `2` is valid for a resting-capable stop-limit order when the definition
enables it. A dormant row stores total leaves but no active working slice;
activation derives the working quantity from the display policy.

History command/report bytes use [WAL version 7](wal-v7.md). Direct rows must
reconcile exactly with chronological acceptance, replacement, match, refresh,
cancel, expiry, control, and stop events. Restoration rebuilds execution-price,
public-price, FIFO, account, expiry, and trigger indexes before cross-audit.

The physical boundary remains:

```text
G = M + 2 × C.
```

## Continuous coupled-risk payload, kind 3

Kind `3` retains the version-6 outer field order and risk-account rows. Its
embedded matching checkpoint is the version-7 kind-`2` payload above. Every
active fully hidden order produces one reservation for its complete remaining
leaves, exactly as a fully displayed order; public visibility does not change
risk valuation.

## Call-auction payloads, kinds 4 and 5

Kind `4` retains its version-6 call-auction fields and row ordering but embeds
the version-7 definition. Kind `5` retains its outer coupled-risk fields and
embeds that changed kind-`4` value. The current auction order model remains
fully active and has no display qualifier; the definition flag is preserved
for exact lineage rather than interpreted as auction priority policy.

## Compatibility boundary

Only envelope version 7 is accepted. Snapshot versions `1` through `6` are
expired and rejected before payload interpretation. A version-6 image lacks
the definition flag and cannot represent a fully hidden row or its queue
class. Authoritative predecessor migration requires an explicit provenance-
preserving converter.

## Primary-source provenance

- CRC-32C follows [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication is bounded by Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all),
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  POSIX.1-2024 [`fsync`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html),
  and [`rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).
- Hidden checkpoint fields and validation are Quotick internal contracts
  verified by codec, corruption, direct-restore, durable-reopen, market-data,
  and coupled-risk tests.
