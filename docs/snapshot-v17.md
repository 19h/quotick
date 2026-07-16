# Semantic Snapshot Format Version 17

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 17 preserves the version-16 kind registry
and direct row schemas. Continuous matching history uses WAL-v17 semantics for
FOK decrement-and-cancel; all field tags and row layouts remain unchanged.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `17` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-16 payload bytes.
- Matching kind `2` retains direct rows and embeds WAL-v17 values in complete
  chronological command/report history.
- Continuous coupled-risk kind `3` retains its direct rows and embeds the
  changed kind-`2` matching checkpoint.
- Call-auction kind `4` retains version-16 direct rows and uses unchanged
  call-auction values from WAL v17 in chronological command/report history.
- Coupled call-auction/risk kind `5` embeds the kind-`4` checkpoint and retains
  its version-1 risk-account rows.

The instrument definition, order rows, and chronological command/report values
are defined by [WAL version 17](wal-v17.md).

## Continuous matching payloads, kinds 2 and 3

Canonical active-order, dormant-stop, counter, control, and history framing are
byte-identical to version 16. Version 17 changes only validation and replay of
a continuous FOK order with decrement-and-cancel. Full-history replay must
either reproduce a complete external fill before the first priority-reachable
self barrier or reproduce a nonmutating `InsufficientLiquidity` result. A
dormant FOK stop applies the same rule at activation.

Direct restore and independent history replay must agree on active makers,
orders, reservations, positions, sequences, and cached reports. A contradictory
accepted partial execution, STP mutation, or report is rejected. Minimum-
quantity IOC with decrement-and-cancel remains inadmissible.

## Call-auction payloads, kinds 4 and 5

Top-level metadata, phase state, accepted-identity rows, active-order rows,
history framing, explicit abort-on-self-trade policy, sequenced rejection, and
optional indication derivation retain version-16 semantics and bytes. The
physical boundary remains:

```text
G = M + 2 × C.
```

Kind `5` retains the version-1 outer field order and risk rows documented in
[the coupled risk payload](auction-risk-checkpoint-v1.md). Its embedded auction
checkpoint is the version-17 kind-`4` value above.

## Decoder rejection rules

The envelope decoder rejects wrong magic, every version other than `17`,
unknown kind, physical/declared length disagreement, payloads over the caller
limit, CRC mismatch, header/payload generation mismatch, and trailing bytes.
Typed payload decoders additionally reject invalid WAL-v17 values, row order,
direct/history disagreement, continuous FOK decrement-and-cancel divergence,
invalid auction abort grammar, priority/allocation divergence, and risk
contradictions.

## Compatibility boundary

Only envelope version 17 is accepted. Snapshot versions `1` through `16` are
expired and rejected before payload interpretation. Relabelling is invalid
because CRC-32C covers the header and because matching history requires
version-17 semantic validation. Authoritative predecessor migration requires
an explicit provenance-preserving converter.

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
- FOK barrier reconstruction and coupled-risk neutrality are Quotick internal
  contracts verified by model, stable-codec, direct-restore, full-WAL, risk,
  and exact-retry tests.
