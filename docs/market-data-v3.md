# Market-data Payload Format Version 3

This document defines the complete-value binary payloads implemented by
`BinaryCodec` for `MarketDataUpdate` and `MarketDataSnapshot`. Version 3 adds
the public/private projection rules for fully hidden continuous liquidity. It
does not define a transport, session protocol, compression envelope, or
authentication scheme. All multibyte integers are little-endian.

## Semantic contract

- One non-replayed matching event produces one update with the identical
  sequence and timestamp. `NoBookChange` preserves every private-only event;
  there is no conflation or renumbering.
- Public quantity and order count include fully displayed leaves and current
  reserve slices only. Fully hidden resting orders, dormant stops, and reserve
  hidden leaves contribute zero.
- A fully hidden order's acceptance, rest, same-price reduction, replacement,
  and cancellation emit the ordinary private matching events but project to
  `NoBookChange` unless execution prints a trade.
- At one price, displayed and reserve liquidity executes before fully hidden
  liquidity. Consequently, a fully hidden maker can execute while a public
  level at another price exists, but cannot execute while a public level at
  its own price remains.
- A trade against a fully hidden maker still publishes its anonymized price,
  quantity, trade ID, and aggressor side. When no public maker level existed,
  its absolute maker-level field is the canonical zero quantity/zero count at
  the trade price. The replica advances both trade and event sequences without
  creating or subtracting a public level.
- Reserve refresh, mass cancellation, GTD, stop activation, account control,
  and trading-state semantics retain the version-2 contract.
- Snapshots contain only public occupied levels in descending bid and ascending
  ask order. A price occupied exclusively by fully hidden orders is absent.

## Scalar notation

| Notation | Width | Interpretation |
|---|---:|---|
| `u8` | 1 B | unsigned integer or explicit tag |
| `u32` | 4 B | unsigned collection count |
| `u64` | 8 B | unsigned integer |
| `i64` | 8 B | signed integer price quanta |
| `u128` | 16 B | unsigned aggregate lot quantity |

Identifiers use non-zero `u64` domain values. Quantity is a non-zero `u64`
number of lots. Timestamp is UTC nanoseconds since the Unix epoch.

## Aggregate level

An aggregate level is 33 B:

| Offset | Type | Field |
|---:|---:|---|
| 0 | `u8` | side: buy `0`, sell `1` |
| 1 | `i64` | raw price |
| 9 | `u128` | aggregate public quantity |
| 25 | `u64` | public order count |

Quantity and count are both zero only for a deletion or the canonical absent
maker level carried by a fully hidden trade.

## Incremental update

Version 3 retains the version-2 bytes. The 32 B common header contains
instrument ID, instrument version, non-zero matching-event sequence, and event
timestamp, each as `u64`. One tag follows:

| Tag | Payload after tag | Total bytes |
|---:|---|---:|
| `0` | none (`NoBookChange`) | 33 |
| `1` | one 33 B aggregate level | 66 |
| `2` | trade ID `u64`, price `i64`, quantity `u64`, aggressor side `u8`, one 33 B maker level | 91 |
| `3` | prior state `u8`, current state `u8`, revision `u64` | 43 |

### Trade reconciliation

For tag `2`, price equals maker-level price and aggressor side opposes maker
side. Trade IDs are contiguous.

If the replica has the maker price, prior public quantity minus print quantity
must equal the new absolute quantity, and count is unchanged or decreases by
one. If the replica has no maker price, it accepts the trade only when the
maker-level quantity and count are exactly zero. This second form represents
execution against fully hidden liquidity and leaves public depth unchanged.

## Full-depth snapshot

The snapshot layout is byte-for-byte version 2:

1. instrument ID `u64`;
2. instrument version `u64`;
3. last reflected event sequence `u64`;
4. last-trade presence `u8`, then conditional trade ID `u64`;
5. effective trading-state tag `u8` and revision `u64`;
6. bid count `u32` and 33 B bid levels;
7. ask count `u32` and 33 B ask levels.

The decoder rejects invalid tags, identifiers, lengths, ordering, empty
occupied levels, inconsistent state revisions, and locked/crossed images.
Fully hidden-only prices, order identities, accounts, dormant stops, reserve
hidden leaves, trigger state, and stop reference are absent.

## Gap recovery and version boundary

`MarketDataReplayBuffer` can retain an exact constructor-bounded suffix of
these unchanged update values for one instrument/version. It accepts complete
publisher batches only after proving identity, internal contiguity, exact
retained overlap, and the next sequence without mutation. A zero-copy query
returns a bounded sequence page after an exclusive cursor, including across
physical ring wrap. A conflicting duplicate, future cursor, or first required
sequence older than retained evidence is explicit.

Consumers can request a retained short gap and apply the returned updates
through the ordinary replica grammar. If the first missing sequence has been
evicted, they apply a same-shard snapshot, discard buffered updates at or
before its sequence, then require the exact contiguous suffix. A structural
incremental failure still requires another authoritative snapshot. Replay
capacity, cursors, and ring metadata are process-local and add no payload bytes.

Although version 3 retains version-2 bytes, its tag-`2` validation accepts the
canonical absent maker level for fully hidden execution. Payloads have no
self-describing version field; transport/session negotiation must select
version 3 before decoding. Version 2 is expired because interpreting the same
trade under its stricter prior-level rule would reject a valid hidden-maker
print.

## Information boundary

Hidden order identity and resting size are not published, but an execution
print reveals that executable liquidity existed at its price. Reserve refresh
continues to reveal that quantity survived a depleted slice. Version 3 applies
no delay, conflation, randomized peak, or venue-specific obfuscation.

The displayed/hidden distinction for native reserve orders is documented by
the [CME Market by Order FAQ](https://www.cmegroup.com/articles/faqs/market-by-order-mbo.html).
Quotick's fully hidden queue class and payload bytes are internal contracts and
do not claim compatibility with a CME market-data channel.

CME MDP 3.0 specifies packet-sequence range recovery and a 2,000-packet request
maximum for its authenticated TCP historical replay component in the
[CME TCP recovery specification](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457574209).
Quotick's buffer is an internal per-instrument event-update ring; it defines no
CME packet/channel mapping, FIX request, authentication, or network session.
