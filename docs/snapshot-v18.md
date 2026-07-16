# Semantic Snapshot Format Version 18

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 18 preserves the version-17 kind registry
and direct row schemas. Continuous matching history uses WAL-v18 semantics for
minimum-quantity IOC under decrement-and-cancel; field tags and row layouts
remain unchanged.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `18` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-17 payload bytes.
- Matching kind `2` retains direct rows and embeds WAL-v18 values in complete
  chronological command/report history.
- Continuous coupled-risk kind `3` retains its direct rows and embeds the
  changed kind-`2` matching checkpoint.
- Call-auction kind `4` retains version-17 direct rows and uses unchanged
  call-auction values from WAL v18 in chronological command/report history.
- Coupled call-auction/risk kind `5` embeds the kind-`4` checkpoint and retains
  its version-1 risk-account rows.

The instrument definition, order rows, and chronological command/report values
are defined by [WAL version 18](wal-v18.md).

## Continuous matching payloads, kinds 2 and 3

Canonical active-order, dormant-stop, counter, control, and history framing are
byte-identical to version 17. Version 18 changes validation and replay of a
minimum-quantity IOC order with decrement-and-cancel. Full-history replay must
reproduce the exact external-trade threshold decision while prevented self
quantity consumes incoming leaves and the current maker slice. Threshold
failure must reproduce the accepted full-quantity
`MinimumQuantityUnavailable` cancellation without maker, STP, risk,
reservation, position, or public-state mutation. Success reproduces the
ordinary IOC trace and can execute beyond the threshold. Dormant stop
activation applies the same rule against restored activation-time liquidity.

Direct restore and independent history replay must agree on active makers,
orders, reservations, positions, sequences, and cached reports. Contradictory
threshold credit, reserve/hidden priority, partial execution, STP mutation, or
report grammar is rejected. Version-17 FOK decrement-and-cancel semantics
remain distinct and unchanged.

## Call-auction payloads, kinds 4 and 5

Top-level metadata, phase state, accepted-identity rows, active-order rows,
history framing, explicit abort-on-self-trade policy, sequenced rejection, and
optional indication derivation retain version-17 semantics and bytes. The
physical boundary remains:

```text
G = M + 2 × C.
```

Kind `5` retains the version-1 outer field order and risk rows documented in
[the coupled risk payload](auction-risk-checkpoint-v1.md). Its embedded auction
checkpoint is the version-18 kind-`4` value above.

## Decoder rejection rules

The envelope decoder rejects wrong magic, every version other than `18`,
unknown kind, physical/declared length disagreement, payloads over the caller
limit, CRC mismatch, header/payload generation mismatch, and trailing bytes.
Typed payload decoders additionally reject invalid WAL-v18 values, row order,
direct/history disagreement, minimum-quantity decrement-and-cancel divergence,
invalid auction abort grammar, priority/allocation divergence, and risk
contradictions.

## Compatibility boundary

Only envelope version 18 is accepted. Snapshot versions `1` through `17` are
expired and rejected before payload interpretation. Relabelling is invalid
because CRC-32C covers the header and matching history requires version-18
semantic validation. Authoritative predecessor migration requires an explicit
provenance-preserving converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication is bounded by Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all),
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  POSIX.1-2024
  [`fsync`](https://pubs.opengroup.org/onlinepubs/009695399/functions/fsync.html),
  and
  [`rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).
- Minimum-quantity reserve-queue reconstruction, FOK barrier separation, and
  coupled-risk neutrality are Quotick internal contracts verified by model,
  stable-codec, direct-restore, full-WAL, risk, and exact-retry tests.
