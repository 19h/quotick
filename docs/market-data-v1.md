# Market-data payload format version 1

This document defines the complete-value binary payloads implemented by
`BinaryCodec` for `MarketDataUpdate` and `MarketDataSnapshot`. It does not
define a network transport, session protocol, compression envelope, or message
authentication scheme. All multibyte integers are little-endian. No padding or
native Rust representation is serialized.

## Semantic contract

- One non-replayed matching event produces exactly one public update carrying
  the identical event sequence and timestamp.
- Matching-event sequences are strictly contiguous. There is no sequence
  renumbering or conflation in version 1.
- `NoBookChange` preserves continuity for private lifecycle events that do not
  modify public depth or print a trade.
- A level update is absolute state after its source event. Quantity and order
  count are either both zero, meaning deletion, or both non-zero.
- A trade update contains an anonymized print and the absolute maker-level state
  after execution. Account, order, and command identifiers are excluded.
- Full-depth snapshots contain only occupied levels in strict market-priority
  order: descending bids and ascending asks.
- Instrument identifier and immutable definition version are present in every
  update and snapshot.
- A publisher reconstructed from a WAL-recovered `OrderBook` starts at the
  book's last event sequence and trade identifier; historical increments are
  not emitted again.

## Scalar notation

| Notation | Width | Interpretation |
|---|---:|---|
| `u8` | 1 byte | unsigned integer or explicit tag |
| `u32` | 4 bytes | unsigned collection count |
| `u64` | 8 bytes | unsigned integer |
| `i64` | 8 bytes | signed integer price quanta |
| `u128` | 16 bytes | unsigned aggregate lot quantity |

Every identifier uses its non-zero `u64` domain representation. A quantity is a
non-zero `u64` number of lots. A timestamp is `u64` nanoseconds since the Unix
epoch, UTC.

## Aggregate level

An encoded level is exactly 33 bytes:

| Offset | Type | Field |
|---:|---:|---|
| 0 | `u8` | side: buy `0`, sell `1` |
| 1 | `i64` | price quanta |
| 9 | `u128` | aggregate visible leaves quantity |
| 25 | `u64` | visible order count |

## Incremental update

The common header is 32 bytes:

| Offset | Type | Field |
|---:|---:|---|
| 0 | `u64` | instrument ID |
| 8 | `u64` | instrument version |
| 16 | `u64` | matching-event sequence; non-zero |
| 24 | `u64` | event timestamp in UTC nanoseconds |

The payload begins at offset 32 with a `u8` kind tag:

| Tag | Payload after tag | Total bytes |
|---:|---|---:|
| 0 | none (`NoBookChange`) | 33 |
| 1 | one 33-byte aggregate level | 66 |
| 2 | trade ID `u64`, price `i64`, quantity `u64`, aggressor side `u8`, one 33-byte maker level | 91 |

For tag 2, trade price equals maker-level price, aggressor side opposes maker
side, and trade ID cannot exceed the event sequence. A replica additionally
requires the prior maker-level quantity minus printed quantity to equal the new
absolute quantity; maker order count must remain constant or decrease by one.

## Full-depth snapshot

The snapshot begins with:

| Offset | Type | Field |
|---:|---:|---|
| 0 | `u64` | instrument ID |
| 8 | `u64` | instrument version |
| 16 | `u64` | last reflected matching-event sequence; zero is genesis |
| 24 | `u8` | last-trade presence tag: none `0`, some `1` |
| 25 | conditional `u64` | last trade ID when the presence tag is `1` |

The optional trade identifier is followed by a bid count `u32`, that many
33-byte aggregate levels, an ask count `u32`, and that many aggregate levels.
The last trade ID, when present, cannot exceed the snapshot event sequence.

The decoder rejects invalid tags, zero identifiers, truncated collections,
trailing bytes, empty occupied levels, wrong-side levels, duplicate or
non-priority-ordered prices, and crossed or locked snapshots.

## Gap recovery protocol

For a transport that acquires snapshots independently of incrementals, the
race-free consumer procedure is:

1. Begin buffering version-matched incrementals.
2. Obtain and validate a full-depth snapshot from the same authoritative shard.
3. Replace replica state with the snapshot and discard buffered updates whose
   sequence is less than or equal to the snapshot boundary.
4. Require the first retained update to equal `snapshot sequence + 1` and every
   following update to be contiguous.
5. If this condition fails, discard the buffer and repeat from step 1.

`MarketDataReplica` implements snapshot replacement, contiguous batch
application, nonmutating gap detection, and fail-closed structural poisoning.
Transport buffering and retry orchestration remain outside this payload layer.
