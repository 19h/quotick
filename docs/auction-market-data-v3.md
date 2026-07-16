# Expired call-auction market-data payload format version 3

Version 3 is expired. New producers and consumers use
[call-auction market-data payload format version 5](auction-market-data-v5.md).

This document defines the complete-value little-endian binary payloads
implemented by `BinaryCodec` for `CallAuctionMarketDataUpdate` and
`CallAuctionMarketDataSnapshot`. It does not define network framing,
authentication, entitlement, compression, retransmission, or session recovery.
No padding or native Rust representation is serialized.

Version 3 preserves every version-2 layout and existing enumeration value. It
adds one book-change reason and one completion kind for account-scoped mass
cancellation while keeping account, scope, order, and command identity private.

## Semantic contract

- Every non-replayed `CallAuctionEvent` produces exactly one public update at
  the identical event sequence and timestamp. Rejections use
  `NoPublicChange`; exact retries produce no updates.
- An accepted mass cancel is one command batch containing `K` anonymized
  removals with reason `MassCancelled`, followed by exactly one
  `MassCancelCompleted` update. `K = 0` is a one-update batch.
- The completion carries the exact `u64` removal count, exact `u128` cancelled-
  lot sum, and resulting book revision. It omits the private account and scope.
- Account, order, and command identifiers are absent. The batch boundary, not
  any individual payload, proves that the changes are one command.
- Market-constrained interest is represented independently on each side.
  Limit depth is descending on buys and ascending on sells; collection depth
  may lock or cross.
- Each removal carries its positive changed quantity and absolute post-event
  aggregate. Removal updates do not advance replica book revision; the final
  completion advances it once exactly when `K > 0`. An empty selection leaves
  it unchanged.
- Replacement, trades, phase transitions, uncross completion, snapshots, and
  capacity contracts retain their version-2 semantics.

## Scalar and aggregate notation

Scalar widths and aggregate layouts are unchanged from
[version 2](auction-market-data-v2.md): tags are `u8`, counts in encoded
vectors are `u32`, domain integers are `u64`, prices are `i64`, and aggregate
lot quantities are `u128`. Identifiers and changed quantities are non-zero.
Aggregate quantity and count are either both zero or both non-zero.

## Incremental update

The common header remains 32 B: instrument ID `u64`, instrument version
`u64`, non-zero engine-event sequence `u64`, and event timestamp `u64`.
The update kind registry and payload layouts are:

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

Phases remain closed `0`, collecting `1`, and frozen `2`. Clearing encoding
and all prior decoder rejection rules remain version 2. Unknown kind and book-
reason tags are rejected. A mass-cancel completion is valid only when count and
quantity are both zero or both non-zero, and its book revision cannot exceed
its event sequence.

## Full-depth snapshot

The snapshot byte layout and structural constraints are unchanged from
version 2. A snapshot reflects only the final atomic mass-cancel state and its
resulting book revision. It contains no account, scope, cancelled-order
identity, or intermediate command state.

## Gap recovery and batch atomicity

Recovery retains the version-2 procedure. The replay cursor is the final event
sequence of the last completely applied command batch.
`replay_batches_after` never splits a command batch, including a mass cancel's
removals and completion. A positive page limit that cannot contain the next
complete batch returns the existing bounded error rather than a prefix. A
cursor inside the batch is not a command boundary and is rejected.

Replica batch preflight requires a mass-cancel batch to contain only `K`
`MassCancelled` removals followed by one completion. Every update has the same
timestamp; observed count and positive changed-quantity sum equal the declared
values. The unframed single-update API rejects both a `MassCancelled` removal
and a completion because it cannot prove or atomically retain the full shape.

Identity, sequence continuity, price-level capacity, and complete batch grammar
are proved before mutation. Aggregate deltas and the conditional single-
revision transition are validated during application. Structural failure
leaves state unchanged during nonmutating preflight or poisons state if detected
after incremental mutation, as in version 2.

Payloads contain no schema-version or command-ID field. A transport/session
must negotiate version 3 and preserve complete command batches before decoding
or applying payloads.

## Information boundary

Order, account, scope, and command identity remain absent. Adjacent changed
quantities, sides, constraints, aggregate transitions, completion totals, and
one command boundary reveal the anonymized shape of the cancellation; they do
not reveal the selected owner. Version 3 performs no delay, conflation,
minimum-quantity filter, or venue-specific imbalance obfuscation. Indicative
price publication remains a separately versioned policy boundary.

## Primary-source provenance

- FIX defines `OrderMassCancelRequest(35=q)` and its optional side qualifier in
  the
  [FIX Trading Community trade appendix](https://www.fixtrading.org/online-specification/trade-appendix/).
- Quotick's anonymized removal/completion batch, conditional single book
  revision, payload tags, and replay-boundary rules are internal deterministic
  contracts verified by codec, publisher, replica, replay, snapshot, and
  durable-recovery tests; they do not claim FIX wire compatibility.
