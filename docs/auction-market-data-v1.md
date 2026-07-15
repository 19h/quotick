# Call-auction market-data payload format version 1

This document defines the complete-value little-endian binary payloads
implemented by `BinaryCodec` for `CallAuctionMarketDataUpdate` and
`CallAuctionMarketDataSnapshot`. It does not define network framing,
authentication, entitlement, compression, retransmission, or session recovery.
No padding or native Rust representation is serialized.

## Contents

- [Semantic contract](#semantic-contract)
- [Scalar notation](#scalar-notation)
- [Aggregate level](#aggregate-level)
- [Incremental update](#incremental-update)
- [Full-depth snapshot](#full-depth-snapshot)
- [Gap recovery protocol](#gap-recovery-protocol)
- [Information boundary](#information-boundary)

## Semantic contract

This section defines the meaning of each payload kind and the reconciliation
obligations attached to it; byte layout follows in the later sections.

- Every non-replayed `CallAuctionEvent` produces exactly one public update at
  the identical event sequence and timestamp. Rejections use
  `NoPublicChange`; exact retries produce no updates.
- Account, order, and command identifiers are absent. Trade prints contain
  only auction identity, monotonic trade identity, clearing price, and lot
  quantity.
- Market-constrained interest is represented independently on each side.
  Limit depth is descending on the buy side and ascending on the sell side.
  Opposing prices may lock or cross because this is collection state, not a
  continuous executable book.
- Each book transition carries both its positive anonymized changed quantity
  and its absolute post-event aggregate. A replica proves exact quantity and
  order-count reconciliation before mutation.
- Each trade carries the absolute affected buy and sell aggregate. A replica
  proves that each falls by exactly the printed quantity and that each order
  count is unchanged or decreases by one.
- `UncrossCompleted` reconciles the preceding prints and remainder removals to
  the final clearing quantity, common price, count totals, book revision, and
  phase revision before closing the cycle.
- A snapshot contains effective phase/cycle state, command and event
  boundaries, book revision, last trade identity, both market aggregates, and
  full limit depth. It contains no indicative price because reference price,
  candidate band, and ranking policy are authoritative external query inputs.
- Publisher bootstrap from a live or WAL/checkpoint-recovered engine starts at
  its final command/event/trade boundaries and emits no historical increments.
- `CallAuctionMarketDataLimits` is process-local operational policy, not wire
  semantics. Publisher construction must cover the source engine's configured
  active-order, per-side limit-level, and per-report event maxima. Version-1
  payload bytes contain no capacity or allocation metadata.

## Scalar notation

| Notation | Width | Interpretation |
|---|---:|---|
| `u8` | 1 byte | unsigned integer or explicit tag |
| `u32` | 4 bytes | unsigned collection count |
| `u64` | 8 bytes | unsigned integer |
| `i64` | 8 bytes | signed price quanta |
| `u128` | 16 bytes | unsigned aggregate lot quantity |

Identifiers use non-zero `u64` domain values. An order/trade quantity is a
non-zero `u64` number of lots. A timestamp is `u64` nanoseconds since the Unix
epoch, UTC.

## Aggregate level

Every aggregate starts with side (`u8`: buy `0`, sell `1`) and constraint:

- market constraint: tag `0`;
- limit constraint: tag `1`, then price `i64`.

Aggregate quantity `u128` and order count `u64` follow. A market aggregate is
26 bytes; a limit aggregate is 34 bytes.

Quantity and order count are either both zero (deletion/empty market interest)
or both non-zero.

## Incremental update

This section defines the wire layout of one incremental update and its
decode-time and replica-side validation.

The common header is 32 bytes, in order:

1. instrument ID `u64`;
2. instrument version `u64`;
3. non-zero engine-event sequence `u64`;
4. event timestamp `u64`.

A `u8` kind tag follows:

| Tag | Payload after tag | Total bytes |
|---:|---|---:|
| 0 | none (`NoPublicChange`) | 33 |
| 1 | reason `u8`, changed quantity `u64`, one aggregate | 68 market / 76 limit |
| 2 | auction ID `u64`, trade ID `u64`, price `i64`, quantity `u64`, buy aggregate, sell aggregate | 117–133 |
| 3 | auction ID `u64`, previous phase `u8`, current phase `u8`, revision `u64` | 51 |
| 4 | auction ID `u64`, clearing, trade/cancellation counts, book/phase revisions | 113 |

Enumeration tags and the clearing encoding:

- **Book reasons** are accepted `0`, owner-cancelled `1`, and uncross
  remainder `2`.
- **Phases** are closed `0`, collecting `1`, and frozen `2`.
- A **clearing** is price `i64`, eligible buy quantity `u128`, and eligible
  sell quantity `u128`; its executable and imbalance fields are
  deterministically derived rather than serialized.

The decoder rejects:

- invalid tags,
- zero domain quantities/identifiers,
- inconsistent empty aggregates,
- infeasible revisions,
- zero-execution uncrosses,
- truncation, and
- trailing bytes.

Stateful publisher/replica validation adds sequence, phase graph, cycle
succession, aggregate delta, trade identity, clearing-total, definition
grid/collar, and source-engine proofs.

## Full-depth snapshot

This section defines the wire layout of one snapshot and its structural
constraints.

The fixed prefix is instrument ID `u64`, instrument version `u64`, reflected
event sequence `u64`, and reflected command sequence `u64`. It is followed by:

1. phase `u8`, phase revision `u64`;
2. optional active auction ID and optional last auction ID, each encoded as a
   boolean `u8` followed by conditional `u64`;
3. book revision `u64`;
4. optional last trade ID in the same presence/value form;
5. market-buy and market-sell aggregates;
6. bid count `u32` and that many limit aggregates;
7. ask count `u32` and that many limit aggregates.

Structural constraints:

- The event boundary is not earlier than the command boundary; book and phase
  revisions cannot exceed the command boundary.
- Closed phase has no active cycle; collecting/frozen phase has an active
  cycle equal to the last started cycle.
- Revision zero is the empty closed genesis.
- Occupied snapshot levels are positive, side-correct, strictly price ordered,
  and may cross or lock.

## Gap recovery protocol

This section defines the consumer procedure for recovering from a sequence
gap and the replica behavior that supports it.

1. Begin buffering version-matched incrementals.
2. Obtain a snapshot from the same authoritative instrument-version shard.
3. Validate and atomically replace replica state; discard buffered updates at
   or before the snapshot event sequence.
4. Require the first retained sequence to equal snapshot sequence plus one and
   require strict continuity thereafter.
5. On any gap or structural failure, discard the incremental buffer and repeat.

`CallAuctionMarketDataReplica` performs non-mutating identity/gap preflight and
simulates batch limit-level cardinality in constructor-owned scratch before
transition. Batch-size, price-level, and snapshot-cardinality failures leave
depth, sequences, poison state, and scratch unchanged. Structural failure after
incremental mutation poisons state.

The replica owns active and standby bid/ask arenas; a non-stale valid snapshot
fills the standby image, swaps both sides atomically, retains the prior active
image for reuse, and clears poisoning.

Payloads contain no schema-version field; a transport/session must negotiate
version 1 before decoding.

## Information boundary

Order and account identity are absent, but event-by-event aggregate changes
reveal individual accepted/cancelled quantities and pairing quantities.
Version 1 performs no delay, conflation, minimum-quantity filter, or
venue-specific imbalance obfuscation. Indicative-price publication requires a
separately versioned policy carrying its authoritative reference, band, and
ranking provenance.
