# Market-data payload format version 2

This document defines the complete-value binary payloads implemented by
`BinaryCodec` for `MarketDataUpdate` and `MarketDataSnapshot`. It does not
define a network transport, session protocol, compression envelope, or message
authentication scheme. All multibyte integers are little-endian. No padding or
native Rust representation is serialized.

## Contents

- [Semantic contract](#semantic-contract)
- [Scalar notation](#scalar-notation)
- [Aggregate level](#aggregate-level)
- [Incremental update](#incremental-update)
- [Full-depth snapshot](#full-depth-snapshot)
- [Gap recovery protocol](#gap-recovery-protocol)
- [Information boundary](#information-boundary)

## Semantic contract

This section lists the guarantees that relate private matching events to the
public updates and snapshots defined in the sections below.

- One non-replayed matching event produces exactly one public update carrying
  the identical event sequence and timestamp.
- Matching-event sequences are strictly contiguous. There is no sequence
  renumbering or conflation in version 2.
- `NoBookChange` preserves continuity for private lifecycle events that do not
  modify public depth, print a trade, or change instrument state.
- Reserve hidden leaves are never included in public quantity or order count.
  A depleted visible slice can delete an order/level in its trade update; a
  following source-sequenced level update publishes the replenished slice after
  the private order has requeued at the FIFO tail.
- Each order selected by a private mass cancel produces its normal absolute
  level update. The private aggregate completion produces `NoBookChange`, so
  account, scope, order identifiers, and hidden total leaves remain absent from
  the public payload while sequence continuity is preserved.
- A level update is absolute state after its source event. Quantity and order
  count are either both zero, meaning deletion, or both non-zero.
- A trade update contains an anonymized print and the absolute maker-level state
  after execution. Account, order, and command identifiers are excluded.
- An accepted trading-state control produces a public transition carrying the
  prior state, effective state, and compare-and-increment revision. A
  transition-and-cancel command emits ordinary absolute level updates for each
  cancelled order before its final state update.
- Full-depth snapshots contain only occupied levels in strict market-priority
  order: descending bids and ascending asks. They also carry the effective
  trading state and revision at the same source boundary.
- Instrument identifier and immutable definition version are present in every
  update and snapshot.
- A publisher reconstructed from a WAL-recovered `OrderBook` starts at the
  book's last event sequence and trade identifier; historical increments are
  not emitted again.
- `MarketDataLimits` is process-local operational policy and is never serialized
  into an update or snapshot. A publisher envelope must cover the matching
  shard's configured maxima. A replica rejects a batch or snapshot above its
  selected envelope before changing its public sequence/depth boundary.

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

This section defines the common header shared by every update kind and the
payload encoded after each kind tag.

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
| 3 | previous trading state `u8`, current trading state `u8`, revision `u64` | 43 |

### Tag 2: trade constraints

For tag 2:

- Trade price equals maker-level price.
- Aggressor side opposes maker side.
- Trade ID cannot exceed the event sequence.
- A replica additionally requires the prior maker-level quantity minus printed
  quantity to equal the new absolute quantity; maker order count must remain
  constant or decrease by one.
- For a fully depleted reserve slice, the count decreases even though the
  private order retains hidden leaves. Its subsequent refresh is an ordinary
  absolute level update at the next matching-event sequence and increases
  visible order count again.

### Tag 3: trading-state constraints

For tag 3, the trading-state tags are:

| Tag | Trading state |
|---:|---|
| `0` | open |
| `1` | cancel-only |
| `2` | halted |
| `3` | closed |

- Prior and current state must differ.
- Revision is non-zero, cannot exceed the event sequence, and must equal the
  replica's current revision plus one.
- Prior state must equal the replica's current state.

## Full-depth snapshot

This section defines the snapshot layout and the validation a decoder applies
before accepting one.

The snapshot begins with:

| Offset | Type | Field |
|---:|---:|---|
| 0 | `u64` | instrument ID |
| 8 | `u64` | instrument version |
| 16 | `u64` | last reflected matching-event sequence; zero is genesis |
| 24 | `u8` | last-trade presence tag: none `0`, some `1` |
| 25 | conditional `u64` | last trade ID when the presence tag is `1` |

The optional trade identifier is followed, in order, by:

- effective trading state `u8`
- trading-state revision `u64`
- a bid count `u32`, then that many 33-byte aggregate levels
- an ask count `u32`, then that many aggregate levels

Constraints:

- The last trade ID, when present, cannot exceed the snapshot event sequence.
- The trading-state revision cannot exceed that sequence.
- Revision zero denotes the immutable definition's initial state; a replica
  rejects another state at revision zero.

The decoder rejects:

- invalid tags
- zero identifiers
- truncated collections
- trailing bytes
- empty occupied levels
- wrong-side levels
- duplicate or non-priority-ordered prices
- infeasible state revisions
- crossed or locked snapshots

Snapshots contain displayed aggregate quantity only. Total hidden reserve
leaves and private reserve order identifiers are intentionally absent.

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

### Replica behavior

`MarketDataReplica` is initialized with the definition's trading state and
implements:

- snapshot replacement
- contiguous batch application
- nonmutating gap detection
- state-revision comparison
- finite batch/side capacity preflight
- fail-closed structural poisoning

It owns active and standby price trees for both sides: after all snapshot
checks pass, the image is constructed in standby storage and both sides are
swapped atomically. Snapshot cardinality failure therefore retains the prior
depth and does not poison the replica. Incremental structural failure after
mutation still requires a new snapshot.
Transport buffering and retry orchestration remain outside this payload layer.

### Version compatibility

Version 2 preserves the byte representation of incremental tags `0`–`2` and
adds tag `3`. It changes every full-depth snapshot by inserting the 9-byte
state/revision pair. Payloads contain no self-describing schema-version field;
transport/session negotiation must select version 2 before decoding. A
version-1 snapshot must not be passed to the version-2 decoder.

## Information boundary

Although hidden total leaves are excluded, a refresh proves that additional
quantity survived the preceding slice depletion, and a final partial slice can
bound that surviving quantity. Version 2 performs no delay, conflation,
randomized peak, or venue-specific obfuscation. Consumers must therefore treat
the feed as displayed-depth data, not as proof that reserve size is
non-inferable.

The distinction between displayed peak and hidden total, and the possibility
of changed priority on native-iceberg refresh, are documented by the
[CME Market by Order FAQ](https://www.cmegroup.com/articles/faqs/market-by-order-mbo.html).
This payload remains Quotick-specific and does not claim byte or behavioral
compatibility with a CME market-data channel.
