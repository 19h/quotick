# Expired Semantic Snapshot Format Version 5

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 5 preserves the version-4 kind registry.
Ledger kind `1`, call-auction kind `4`, and coupled call-auction/risk kind `5`
retain their payload bytes. Continuous matching kind `2` and continuous
coupled-risk kind `3` add deterministic GTD expiry state through their embedded
matching checkpoint.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `5` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Unchanged payloads

Kinds `1`, `4`, and `5` are byte-for-byte those specified by
[snapshot version 4](snapshot-v4.md) and its referenced schemas. Their
lineage, validation, capture, restoration, and cutover rules are unchanged.

## Matching checkpoint payload, kind 2

The top-level field order remains:

1. immutable-definition WAL sequence `M` as `u64`;
2. completed report WAL boundary `G` as `u64`;
3. definition length `u32` and definition payload;
4. active-order count `u32` and canonical private order rows;
5. completed command/report count `C` as `u32` and chronological pairs, each
   containing command length/payload then report length/payload.

Every private order row has the following exact fields:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | order ID |
| 2 | 8 | account ID |
| 3 | 1 | side |
| 4 | 8 | raw signed price |
| 5 | 8 | total leaves |
| 6 | 8 | displayed leaves |
| 7 | 1 or 9 | display policy: fully displayed `0`, or reserve `1` plus peak `u64` |
| 8 | 1 | self-trade-prevention policy |
| 9 | 1 | expiration-present `bool` |
| 10 | 8 | GTD expiration, or canonical zero when absent |

Rows are therefore 52 B when fully displayed and 60 B when reserve. The
expiration is present exactly for a resting GTD order. Active rows remain
canonicalized as buys then sells, ascending raw price within each side, FIFO
within a price.

History command/report bytes use [WAL version 5](wal-v5.md). Accepted expiry
sweeps in retained history derive one optional inclusive watermark. Its chain
cannot regress, every completion must reproduce the exact previous/current
watermark and cancellation aggregates, and no active row may expire at or
before the derived watermark. Restoration rebuilds the fixed-capacity ordered
expiry index from the canonical rows and audits it against active orders.

The physical boundary remains:

```text
G = M + 2 × C.
```

## Continuous coupled-risk payload, kind 3

Kind `3` retains the version-4 outer field order and risk-account rows, but its
embedded matching checkpoint is the version-5 kind-`2` payload above. Risk
reservation reconstruction uses total active leaves, including GTD orders.
Orders removed by accepted expiry cancellations have no retained reservation;
the final sweep summary has no separate exposure effect.

## Compatibility boundary

Only envelope version 5 is accepted. Snapshot versions `1` through `4` are
expired and rejected before payload interpretation. A version-4 matching or
continuous-risk image lacks explicit active-row expirations and cannot be
re-labelled. Authoritative predecessor migration requires an explicit
provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication is bounded by Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all),
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  POSIX.1-2024 [`fsync`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html),
  and [`rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).
- GTD checkpoint fields and validation are Quotick internal contracts verified
  by repository codec, direct-restore, durable-reopen, and cutover tests.
