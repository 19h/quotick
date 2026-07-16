# Call-auction market-data payload format version 2

This document defines the complete-value little-endian binary payloads
implemented by `BinaryCodec` for `CallAuctionMarketDataUpdate` and
`CallAuctionMarketDataSnapshot`. It does not define network framing,
authentication, entitlement, compression, retransmission, or session recovery.
No padding or native Rust representation is serialized.

Version 2 preserves every version-1 layout and existing enumeration value. It
adds one book-change reason for accepted new-identity replacement.

## Semantic contract

- Every non-replayed `CallAuctionEvent` produces exactly one public update at
  the identical event sequence and timestamp. Rejections use
  `NoPublicChange`; exact retries produce no updates.
- An accepted replacement is one command batch containing exactly two public
  updates: target removal with reason `Replaced`, followed by replacement
  addition with reason `Accepted`.
- Account, order, and command identifiers are absent. The batch boundary, not
  either payload alone, proves that the two anonymized changes are one command.
- Market-constrained interest is represented independently on each side.
  Limit depth is descending on buys and ascending on sells; collection depth
  may lock or cross.
- Each book transition carries its positive changed quantity and absolute
  post-event aggregate. The replacement removal does not advance the replica
  book revision; the following acceptance advances it once, matching the
  source engine's single atomic book revision.
- Trades, phase transitions, uncross completion, snapshots, and capacity
  contracts retain their version-1 semantics.

## Scalar and aggregate notation

Scalar widths and aggregate layouts are unchanged from
[version 1](auction-market-data-v1.md): tags are `u8`, counts `u32`, domain
integers `u64`, prices `i64`, and aggregate lot quantities `u128`. Identifiers
and changed quantities are non-zero. Aggregate quantity and count are either
both zero or both non-zero.

## Incremental update

The common header remains 32 B: instrument ID `u64`, instrument version
`u64`, non-zero engine-event sequence `u64`, and event timestamp `u64`.
The update kind registry and payload layouts remain:

| Tag | Payload after tag | Total bytes |
|---:|---|---:|
| 0 | none (`NoPublicChange`) | 33 |
| 1 | reason `u8`, changed quantity `u64`, one aggregate | 68 market / 76 limit |
| 2 | auction ID, trade ID, price, quantity, buy aggregate, sell aggregate | 117–133 |
| 3 | auction ID, previous phase, current phase, revision | 51 |
| 4 | auction ID, clearing, counts, book/phase revisions | 113 |

Book-reason tags are:

| Tag | Meaning |
|---:|---|
| `0` | `Accepted` |
| `1` | `UserCancelled` |
| `2` | `UncrossRemainder` |
| `3` | `Replaced` target removal |

Phases remain closed `0`, collecting `1`, and frozen `2`. Clearing encoding
and all decoder rejection rules remain version 1, with unknown book-reason
tags rejected.

## Full-depth snapshot

The snapshot byte layout and structural constraints are unchanged from
version 1. A snapshot reflects only the final atomic replacement state and its
single resulting book revision; it contains neither target/replacement order
identity nor an intermediate replacement state.

## Gap recovery and batch atomicity

Recovery retains the version-1 procedure. The replay cursor is the final event
sequence of the last completely applied command batch.
`replay_batches_after` never splits a command batch, including a replacement's
two updates. A positive page limit that cannot contain the next complete batch
returns the existing bounded error rather than a prefix. A cursor between the
two replacement updates is not a command boundary and is rejected.
The unframed single-update replica API rejects a `Replaced` removal because it
cannot prove or atomically retain the required following acceptance.

Replica batch preflight proves identity, sequence continuity, price-level
capacity, and exact two-update replacement shape before mutation. Aggregate
deltas and the final single-revision transition are validated while applying
the complete batch. Structural failure leaves state unchanged during
non-mutating preflight or poisons state if detected after incremental mutation,
as in version 1.

Payloads contain no schema-version or command-ID field. A transport/session
must negotiate version 2 and preserve complete command batches before
decoding or applying payloads.

## Information boundary

Order and account identity remain absent. The adjacent changed quantities and
one command boundary reveal that one anonymized order was replaced by another;
they do not reveal either identity. Version 2 performs no delay, conflation,
minimum-quantity filter, or venue-specific imbalance obfuscation. Indicative
price publication remains a separately versioned policy boundary.

## Primary-source provenance

- FIX defines `OrderCancelReplaceRequest(35=G)` and its previous accepted
  identity relation in the
  [FIX Latest message registry](https://fiximate.fixtrading.org/en/FIX.Latest/msg17.html).
- Quotick's two-update batch, anonymization, single book revision, payload tag,
  and replay-boundary rules are internal deterministic contracts verified by
  codec, publisher, replica, replay, snapshot, and durable-recovery tests; they
  do not claim FIX wire compatibility.
