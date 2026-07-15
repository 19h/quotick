# Semantic Snapshot Format Version 6

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 6 preserves the version-5 kind registry.
Ledger kind `1`, call-auction kind `4`, and coupled call-auction/risk kind `5`
retain their payload bytes. Continuous matching kind `2` and continuous
coupled-risk kind `3` add dormant stop state through their embedded matching
checkpoint.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `6` |
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
[snapshot version 5](snapshot-v5.md) and its referenced schemas. Their lineage,
validation, capture, restoration, and cutover rules are unchanged.

## Matching checkpoint payload, kind 2

The top-level field order is:

1. immutable-definition WAL sequence `M` as `u64`;
2. completed report WAL boundary `G` as `u64`;
3. definition length `u32` and definition payload;
4. active resting-order count `u32` and canonical private order rows;
5. dormant-stop count `u32` and canonical dormant-stop rows;
6. completed command/report count `C` as `u32` and chronological pairs, each
   containing command length/payload then report length/payload.

Active resting-order rows retain their version-5 fields and 52 B/60 B sizes.
They are canonicalized as buys then sells, ascending raw price within each
side, FIFO within a price.

Every dormant-stop row has the following exact fields:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | order ID |
| 2 | 8 | account ID |
| 3 | 1 | side |
| 4 | 8 | total dormant leaves |
| 5 | 1 or 9 | display: fully displayed `0`, or reserve `1` plus peak `u64` |
| 6 | 1 | self-trade-prevention policy |
| 7 | 9 | expiration-present `bool` plus expiration or canonical zero |
| 8 | 8 | raw signed trigger price |
| 9 | 1 or 9 | activation: market `0`, or limit `1` plus raw price |
| 10 | 1 or 9 | time in force; GTD tag `4` carries its timestamp |
| 11 | 8 | trigger-priority event sequence |

A fully displayed, market-activation, non-GTD row is 54 B. Limit activation,
reserve display, and GTD each add 8 B. The separately encoded optional
expiration must equal the GTD timestamp and must be absent for other
time-in-force values.

Dormant rows are canonicalized as buy triggers in ascending trigger-price
order, then sell triggers in descending trigger-price order. Equal trigger
prices use ascending `(priority sequence, order ID)`. History must reconstruct
the exact same dormant set from accepted new orders, arm/replace/cancel/expiry/
control events, and canonical trigger events. Direct rows are not trusted as an
independent source of lineage.

History command/report bytes use [WAL version 6](wal-v6.md). Accepted stop-
trigger sweeps derive one optional committed reference, including empty sweeps.
The reference/backlog chain and every arm, trigger, completion, and dormant row
must reproduce the live indices exactly. Restoration rebuilds the buy/sell
trigger AVL arenas and expiry/account/identity membership before cross-audit.

The physical boundary remains:

```text
G = M + 2 × C.
```

## Continuous coupled-risk payload, kind 3

Kind `3` retains the version-5 outer field order and risk-account rows, but its
embedded matching checkpoint is the version-6 kind-`2` payload above. Risk
reservation reconstruction includes dormant stops. A stop-limit reservation
uses its activation limit; a stop-market reservation uses the configured
market collar. Triggering changes the reservation from dormant to active
without duplicating exposure. Subsequent trades, cancellation, or residual rest
use ordinary coupled-risk transitions.

## Compatibility boundary

Only envelope version 6 is accepted. Snapshot versions `1` through `5` are
expired and rejected before payload interpretation. A version-5 matching or
continuous-risk image has no dormant-stop row section and cannot be relabelled.
Authoritative predecessor migration requires an explicit provenance-preserving
converter.

## Primary-source provenance

- CRC-32C follows [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication is bounded by Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all),
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  POSIX.1-2024 [`fsync`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html),
  and [`rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).
- Dormant-stop checkpoint fields and validation are Quotick internal contracts
  verified by repository codec, corruption, direct-restore, durable-reopen,
  market-data, and coupled-risk tests.
