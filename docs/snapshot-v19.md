# Semantic Snapshot Format Version 19

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 19 preserves the version-18 kind registry
and direct row schemas. Chronological call-auction history embeds the version-
19 instrument-bound trade value.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `19` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-18 payload bytes.
- Matching kind `2` retains direct rows and embeds WAL-v19 values in complete
  chronological command/report history; continuous values are unchanged.
- Continuous coupled-risk kind `3` retains its direct rows and embeds the
  unchanged kind-`2` matching checkpoint.
- Call-auction kind `4` retains its version-18 direct rows. Its chronological
  reports encode every private trade using the 72 B WAL-v19 value, including
  immutable instrument ID and definition version.
- Coupled call-auction/risk kind `5` embeds the changed kind-`4` checkpoint and
  retains its version-1 risk-account rows.

The instrument definition, order rows, and chronological command/report values
are defined by [WAL format version 19](wal-v19.md).

## Continuous matching payloads, kinds 2 and 3

All direct rows, command/report values, and replay semantics are byte-identical
to version 18. Version-18 minimum-quantity IOC decrement-and-cancel validation
remains current. Direct restore and independent chronological replay must
produce the same active makers, dormant stops, controls, reservations,
positions, sequences, and cached reports.

## Call-auction payloads, kinds 4 and 5

Top-level metadata, phase state, accepted-identity rows, active-order rows,
history framing, priority/allocation policies, explicit abort-on-self-trade,
sequenced rejection, and optional-indication derivation retain version-18
semantics. Only each chronological trade event changes: its private trade value
is the 72 B WAL-v19 layout and must match the checkpoint's immutable definition.
The physical boundary remains:

```text
G = M + 2 × C.
```

Kind `5` retains the version-1 outer field order and risk rows documented in
[the coupled risk payload](auction-risk-checkpoint-v1.md). Its embedded auction
checkpoint is the version-19 kind-`4` value above.

Ledger snapshots reached after auction settlement retain the ordinary kind-`1`
entry/batch history. Settlement does not add a snapshot kind or alter direct
ledger rows.

## Decoder rejection rules

The envelope decoder rejects wrong magic, every version other than `19`,
unknown kind, physical/declared length disagreement, payloads over the caller
limit, CRC mismatch, header/payload generation mismatch, and trailing bytes.
Typed payload decoders additionally reject invalid WAL-v19 values, row order,
direct/history disagreement, auction trade-definition mismatch, allocation or
report divergence, and coupled-risk contradictions.

## Compatibility boundary

Only envelope version 19 is accepted. Snapshot versions `1` through `18` are
expired and rejected before payload interpretation. Relabelling is invalid
because CRC-32C covers the header and call-auction history requires the version-
19 trade width and identity contract. Authoritative predecessor migration
requires an explicit provenance-preserving converter.

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
- Auction trade identity, direct/history agreement, and ledger settlement are
  Quotick internal contracts verified by stable-codec, full-WAL, direct-
  restore, coupled-risk, ledger, recovery, and checkpoint tests.
