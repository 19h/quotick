# Semantic Snapshot Format Version 20

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 20 preserves the version-19 kind registry
and direct row schemas. Continuous chronological history embeds the WAL-v20
market-to-limit command and pricing-event values.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `20` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-19 payload bytes.
- Matching kind `2` retains direct rows and embeds WAL-v20 values in complete
  chronological command/report history.
- Continuous coupled-risk kind `3` retains its direct rows and embeds the
  changed kind-`2` matching checkpoint.
- Call-auction kind `4` retains its version-19 direct rows and chronological
  values, including the 72 B instrument-bound private trade.
- Coupled call-auction/risk kind `5` embeds the unchanged kind-`4` checkpoint
  and retains its version-1 risk-account rows.

The instrument definition, order rows, and chronological command/report values
are defined by [WAL format version 20](wal-v20.md).

## Continuous matching payloads, kinds 2 and 3

Direct resting rows remain ordinary limit rows. A market-to-limit command is
retained as `OrderType` tag `3` in chronological history; its accepted report
retains event-kind tag `15` and the captured price. Independent history replay
must reproduce the direct residual row, complete report, risk position, and
limit-priced reservation exactly. Rejected empty-book or invalid-lifetime
commands retain rejection tag `56` or `57` and create no direct order row.

All other direct rows and replay semantics are byte-identical to version 19.
Direct restore and independent chronological replay must produce the same
active makers, dormant stops, controls, reservations, positions, sequences,
and cached reports.

## Other payload kinds

Call-auction kinds `4` and `5`, ledger kind `1`, and all non-market-to-limit
values retain version-19 semantics. The matching physical boundary remains:

```text
G = M + 2 × C.
```

Kind `5` retains the version-1 outer field order and risk rows documented in
[the coupled risk payload](auction-risk-checkpoint-v1.md).

## Decoder rejection rules

The envelope decoder rejects wrong magic, every version other than `20`,
unknown kind, physical/declared length disagreement, payloads over the caller
limit, CRC mismatch, header/payload generation mismatch, and trailing bytes.
Typed payload decoders additionally reject invalid WAL-v20 values, row order,
direct/history disagreement, market-to-limit pricing or residual divergence,
auction trade-definition mismatch, allocation or report divergence, and
coupled-risk contradictions.

## Compatibility boundary

Only envelope version 20 is accepted. Snapshot versions `1` through `19` are
expired and rejected before payload interpretation. Relabelling is invalid
because CRC-32C covers the header and version-19 chronological history does not
define market-to-limit tags. Authoritative predecessor migration requires an
explicit provenance-preserving converter.

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
- Market-to-limit direct/history agreement, risk reconstruction, and exact
  retry are Quotick internal contracts verified by stable-codec, full-WAL,
  direct-restore, coupled-risk, publisher, and checkpoint tests.
