# Expired Call-auction Market-data Payload Format Version 4

Version 4 is expired. New producers and consumers use
[call-auction market-data payload format version 5](auction-market-data-v5.md).

This document defines the complete-value little-endian binary payloads
implemented by `BinaryCodec` for `CallAuctionMarketDataUpdate` and
`CallAuctionMarketDataSnapshot`. It does not define network framing,
authentication, entitlement, compression, retransmission, or session recovery.
No padding or native Rust representation is serialized.

Version 4 preserves every version-3 layout and existing enumeration value. It
adds one book-change reason for retained-priority quantity reduction while
keeping account, order, command, and priority identity private.

## Semantic contract

- Every non-replayed `CallAuctionEvent` produces exactly one public update at
  the identical event sequence and timestamp. Rejections use
  `NoPublicChange`; exact retries produce no updates.
- An accepted amendment produces one `Book` update with reason `Amended`.
  Changed quantity is the positive difference between previous and current
  active leaves. The absolute post-event aggregate has that quantity removed
  and retains its previous order count.
- The update advances the replica collection-book revision exactly once and is
  valid only while the replicated phase is `Collecting`.
- Account, order, command, and priority identifiers are absent. Side,
  market/limit constraint, changed quantity, and absolute aggregate remain
  public.
- Market-constrained interest is represented independently on each side.
  Limit depth is descending on buys and ascending on sells; collection depth
  may lock or cross.
- Mass cancellation, replacement, trades, phase transitions, uncross
  completion, snapshots, and capacity contracts retain their version-3
  semantics.

## Scalar and aggregate notation

Scalar widths and aggregate layouts are unchanged from
[version 3](auction-market-data-v3.md): tags are `u8`, counts in encoded
vectors are `u32`, domain integers are `u64`, prices are `i64`, and aggregate
lot quantities are `u128`. Identifiers and changed quantities are non-zero.
Aggregate quantity and count are either both zero or both non-zero.

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
| 5 | cancelled count `u64`, cancelled lots `u128`, book revision `u64` | 65 |

Book-reason tags are:

| Tag | Meaning |
|---:|---|
| `0` | `Accepted` |
| `1` | `UserCancelled` |
| `2` | `UncrossRemainder` |
| `3` | `Replaced` target removal |
| `4` | `MassCancelled` removal |
| `5` | `Amended` retained-count quantity removal |

Phases remain closed `0`, collecting `1`, and frozen `2`. Clearing encoding
and all prior decoder rejection rules remain version 3. Unknown kind and book-
reason tags are rejected.

For `Amended`, let previous aggregate quantity and count be `Q` and `N`, and
let the encoded positive changed quantity be `D`. The next aggregate must be
exactly `(Q - D, N)`. Underflow, a count change, or application outside
collection is rejected. The payload does not carry book revision; sequence-
ordered application advances the replica's revision by one.

## Full-depth snapshot

The snapshot byte layout and structural constraints are unchanged from
version 3. A snapshot reflects only post-amendment aggregate state and the
resulting book revision. It contains no account, order, command, priority, or
previous-quantity field.

## Gap recovery and batch atomicity

Recovery retains the version-3 procedure. An amendment is one complete
one-update command batch and may also be applied through the unframed single-
update API. Identity, sequence continuity, price-level capacity, and complete
batch grammar are proved before mutation. Aggregate delta and the single-
revision transition are validated during application.

Structural failure leaves state unchanged during nonmutating preflight or
poisons state if detected after incremental mutation, as in version 3.
Payloads contain no schema-version or command-ID field. A transport/session
must negotiate version 4 and preserve complete command batches before decoding
or applying payloads.

## Information boundary

Order, account, command, and priority identity remain absent. The changed
quantity, side, constraint, aggregate transition, event timing, and command
boundary reveal an anonymous reduction. Version 4 performs no delay,
conflation, minimum-quantity filter, or venue-specific imbalance obfuscation.
Indicative price publication remains a separately versioned policy boundary.

## Primary-source provenance

- FIX identifies `OrderCancelReplaceRequest(35=G)` as the message for reducing
  an order rather than cancelling all remaining quantity in the
  [FIX Trading Community trade specification](https://www.fixtrading.org/online-specification/business-area-trade/).
- Quotick's anonymous delta, retained aggregate count, revision, payload tag,
  and replay rules are internal deterministic contracts verified by codec,
  publisher, replica, replay, snapshot, and durable-recovery tests; they do not
  claim FIX wire compatibility.
