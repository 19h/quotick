# Coupled Call-Auction Risk Checkpoint Payload Version 1

`CallAuctionRiskCheckpoint` is a complete-value, little-endian semantic codec
and the payload retained by snapshot-version-11 `QSNP` kind `5`. It has no WAL
record kind of its own. Version 1 names the current immutable payload contract;
any incompatible change requires a new explicit format or enclosing version.

## Layout

Fields occur in this exact order:

| Order | Width | Field |
|---:|---:|---|
| 1 | 8 B | physical WAL first sequence `F` occupied by the instrument definition |
| 2 | variable | embedded call-auction checkpoint length `u32`, then the complete [snapshot-v3 kind-4 payload](snapshot-v3.md#call-auction-checkpoint-payload-kind-4) with its definition evolved by snapshot v7, without a `QSNP` header |
| 3 | 4 B | canonical account count `N` as `u32` |
| 4 | variable | `N` account rows in strictly increasing `AccountId` order |

Each account row is:

| Order | Width | Field |
|---:|---:|---|
| 1 | variable | account-risk-definition length `u32`, then its stable WAL-v11 kind-5 payload without a WAL header |
| 2 | 16 B | current signed executed position lots `i128` |
| 3 | 16 B | aggregate active buy lots `u128` |
| 4 | 16 B | aggregate active sell lots `u128` |
| 5 | 16 B | aggregate conservative active notional `u128` |
| 6 | 8 B | active reservation count `u64` |

An account-risk definition is 121 B:

| Field | Type |
|---|---:|
| account ID | `u64` |
| state | `u8` |
| initial position | `i128` |
| maximum order quantity | `u64` |
| maximum order notional | `u128` |
| maximum active-order count | `u64` |
| maximum active quantity | `u128` |
| maximum active notional | `u128` |
| maximum long position | `u128` |
| maximum short position | `u128` |

State tags are active `0`, reduce-only `1`, and blocked `2`.

If the embedded auction metadata boundary is `M`, canonical metadata requires:

```text
M = F + N.
```

The embedded auction checkpoint separately requires its completed report
boundary `G = M + 2C` for `C` command/report pairs.

## Semantic validation

This section defines the rejection conditions, the direct-restoration steps,
and the independent replay validation.

Decode and construction reject:

- zero `F`
- arithmetic or length overflow
- noncanonical/duplicate account order
- invalid profiles
- an inconsistent metadata boundary
- malformed embedded auction state
- trailing bytes

Direct restoration performs all of the following:

1. Restores the embedded phase, book, counters, accepted identities, and exact
   command/report cache under selected finite limits.
2. Registers every immutable profile and seeds its stored current position.
3. Reconstructs one conservative reservation from every active auction order.
4. Requires each reconstructed account exposure to equal the redundant row.
5. Cross-audits one-to-one book/reservation parity and every numerical bound.

Checkpoint construction and decode also independently replay every retained
command through `CallAuctionRiskManagedEngine`. Risk-rejected submit and
replace commands must reproduce the exact rejection from the retained
profiles. Replacement authorization first subtracts the owned target's
reservation, then evaluates the replacement under the same immutable profile.
An accepted two-event trace removes the target reservation before inserting
the replacement reservation. Validation capacity includes all historical
submits and replacements, not only accepted identities, because core
preparation occurs before the external risk gate.

Each accepted mass-cancel removal releases the corresponding reservation.
The aggregate `MassCancelCompleted` event changes no risk state. Independent
replay requires the private account, scope, canonical order sequence, count,
and quantity to reconcile with the core command/report trace.

## Complexity and boundary

This section states the payload's complexity bounds and the responsibilities
of the enclosing durable components.

For `C` commands, `E` events, `A` accounts, and `O` active orders, payload size
is `O(C + E + A + O)`. Direct reconstruction is `O(C + E + A + O)` plus the
embedded indexed-book audit; independent validation re-executes the complete
command history. Constructor-owned live profile, reservation, and account-net
maps remain bounded by the selected `CallAuctionRiskLimits`.

- `SnapshotFile` supplies version-11 framing, CRC protection, synchronized
  atomic replacement, and A/B slot publication.
- `DurableCallAuctionRiskEngine` supplies profile-prefixed WAL
  acknowledgement, full replay, one dangling-command completion, exact prefix
  proof, and single-file or segmented anchor cutover.
