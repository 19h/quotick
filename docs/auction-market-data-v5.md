# Call-auction Market-data Payload Format Version 5

This document defines the complete-value little-endian binary payloads
implemented by `BinaryCodec` for `CallAuctionMarketDataUpdate` and
`CallAuctionMarketDataSnapshot`. It does not define network framing,
authentication, entitlement, compression, retransmission, or session recovery.
No padding or native Rust representation is serialized.

Version 5 preserves every version-4 enumeration value and layout and adds one
nullable, revision-bound indicative state. Account, order, command, and
priority identity remain private.

## Semantic contract

- Every non-replayed `CallAuctionEvent` produces exactly one public update at
  the identical event sequence and timestamp. Rejections use
  `NoPublicChange`; exact retries produce no updates.
- An accepted indicative command produces one `Indicative` update in
  `Collecting` or `Frozen`, including when no interest can execute.
- The state binds the active auction ID, phase revision, collection-book
  revision, inclusive price band, reference price, explicit price policy, and
  optional clearing result.
- Indicative publication changes neither book revision nor risk state. Any
  accepted non-indicative command invalidates the retained state; rejection
  and exact retry preserve it.
- Account, order, command, and priority identifiers are absent. The explicit
  discovery inputs and aggregate clearing quantities are public.
- Market interest, limit depth, amendments, replacements, mass cancellation,
  trades, phase transitions, uncross completion, and capacity contracts retain
  their version-4 semantics.

## Scalar and aggregate notation

Scalar widths and aggregate layouts are unchanged from
[version 4](auction-market-data-v4.md): tags are `u8`, encoded vector counts
are `u32`, domain integers are `u64`, prices are `i64`, and aggregate lot
quantities are `u128`. Identifiers are non-zero. Aggregate quantity and count
are either both zero or both non-zero.

Price-policy tags are pressure rule `0` ignore or `1` favor imbalance,
followed by final tie `0` lower or `1` higher. A clearing value is price `i64`,
buy quantity `u128`, and sell quantity `u128`.

## Incremental update

The common header remains 32 B: instrument ID `u64`, instrument version
`u64`, non-zero engine-event sequence `u64`, and event timestamp `u64`.
Existing tags `0` through `5` retain their version-4 payloads. Tag `6` is:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 1 | update-kind tag `6` |
| 2 | 8 | non-zero auction ID |
| 3 | 8 | non-zero phase revision |
| 4 | 8 | collection-book revision |
| 5 | 8 | inclusive minimum price `i64` |
| 6 | 8 | inclusive maximum price `i64` |
| 7 | 8 | reference price `i64` |
| 8 | 1 | pressure-rule tag |
| 9 | 1 | final-tie tag |
| 10 | 1 | clearing-present canonical boolean |
| 11 | 0 or 40 | optional clearing value |

The indicative payload after its kind tag is 51 B without clearing and 91 B
with clearing. Complete updates are therefore 84 B and 124 B. Present
clearing must have non-zero executable quantity and a price inside the band.
Absence represents a successful observation with no executable interest.

## Full-depth snapshot

The snapshot has this exact field sequence:

| Order | Width (B) | Field |
|---:|---:|---|
| 1 | 8 | instrument ID |
| 2 | 8 | instrument version |
| 3 | 8 | as-of event sequence |
| 4 | 8 | command sequence |
| 5 | 1 | phase tag |
| 6 | 8 | phase revision |
| 7 | 1 or 9 | optional active auction ID |
| 8 | 1 or 9 | optional last auction ID |
| 9 | 8 | collection-book revision |
| 10 | 1 or 52/92 | optional indicative state |
| 11 | 1 or 9 | optional last trade ID |
| 12 | 26 | market-buy aggregate |
| 13 | 26 | market-sell aggregate |
| 14 | 4 | bid count `u32` |
| 15 | 26 or 34 each | bid aggregates |
| 16 | 4 | ask count `u32` |
| 17 | 26 or 34 each | ask aggregates |

Every option begins with a canonical boolean. An indicative option occupies 1
B when absent, 52 B when present without clearing, and 92 B when present with
clearing. The empty closed snapshot is consequently 113 B, one byte larger
than version 4. Market aggregates encode side, market constraint, quantity,
and count in 26 B; limit aggregates additionally carry an 8 B price.

A present indication must bind the snapshot's active auction, phase revision,
and book revision, and the phase must be `Collecting` or `Frozen`. Snapshot
application validates the complete image before atomically replacing replica
state.

## Gap recovery and batch atomicity

The process-local replay ring retains version-4 complete-batch rules. An
indicative command is one complete one-update batch and may also be applied
through the unframed single-update API. Identity, sequence continuity,
capacity, and complete batch grammar are proved before mutation.

Each non-indicative public transition except `NoPublicChange` clears the
retained indication before applying its ordinary effect. `NoPublicChange`
preserves it because it represents a rejection. Exact retries have empty
replayed batches and preserve it without replica application.

Structural failure leaves state unchanged during nonmutating preflight or
poisons state if detected after incremental mutation, as in version 4.
Payloads contain no schema-version or command-ID field. A transport/session
must negotiate version 5 and preserve complete command batches before decoding
or applying payloads.

## Information boundary

Order, account, command, and priority identity remain absent. The auction ID,
phase/book revisions, price band, reference, price policy, indicative price,
paired quantity, imbalance quantities, event timing, and command boundary can
be inferred from the state. Version 5 performs no delay, cadence control,
conflation, minimum-quantity filter, or venue-specific imbalance obfuscation.

## Primary-source provenance

- Nasdaq documents dissemination of paired shares, imbalance quantity and
  side, reference prices, and indicative or likely clearing prices in its
  [Opening and Closing Cross fact sheet](https://www.nasdaqtrader.com/content/productsservices/trading/crosses/fact_sheet.pdf).
- Nasdaq specifies field-level NOI values and dissemination behavior in its
  [NOIView specification](https://nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NOIViewSpecification.pdf).
- Quotick's nullable value, tags, explicit policy, invalidation rule, and replay
  semantics are internal deterministic contracts. They do not claim Nasdaq or
  FIX wire compatibility.
