# Semantic Snapshot Format Version 11

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 11 preserves the version-10 kind registry,
instrument definition, and direct row schemas. Call-auction matching and
coupled-risk histories use WAL-v11 account-scoped mass-cancel values.

## `QSNP` envelope

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `11` |
| 6 | 2 | payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 zeroed plus the exact
payload. Physical length is `28 B + payload length`. The default payload limit
is 1 GiB (1,073,741,824 B) and is checked before allocation or filesystem
mutation.

## Payload-kind compatibility

- Ledger kind `1` retains its version-10 payload bytes.
- Matching kind `2` retains its version-10 payload bytes.
- Continuous coupled-risk kind `3` retains its version-10 payload bytes.
- Call-auction kind `4` retains direct rows and uses WAL-v11 values in its
  chronological command/report history.
- Coupled call-auction/risk kind `5` embeds the changed kind-`4` history and
  retains its version-1 risk-account rows.

The instrument definition is the schema in [WAL version 11](wal-v11.md) and is
byte-identical to the version-10 definition.

## Matching and continuous coupled-risk payloads, kinds 2 and 3

Kinds `2` and `3` retain their version-10 field order, direct rows, sourced
stop-reference lineage, and replay validation. Their chronological command and
report bytes use WAL version 11, whose continuous value bytes are unchanged.

## Call-auction payload, kind 4

The top-level field order and direct rows retain the version-10 schema. Each
chronological command/report pair uses [WAL version 11](wal-v11.md), including
the call-auction `MassCancel` command and action, `MassCancel` cancellation
reason, and `MassCancelCompleted` event.

Direct rows must reconcile exactly with replay. Each accepted mass cancel must
replay as zero or more selected order removals in ascending `OrderId` order,
followed by one completion with the exact account, scope, count, quantity, and
resulting book revision. A non-empty selection advances the book revision once;
an empty selection leaves direct state unchanged except for retained command
and event history. The process-local per-account index is reconstructed from
active rows and is not persisted.

The physical boundary remains:

```text
G = M + 2 × C.
```

## Coupled call-auction/risk payload, kind 5

Kind `5` retains the version-1 outer field order and account rows documented in
[the coupled risk payload](auction-risk-checkpoint-v1.md). Its embedded auction
checkpoint is the version-11 kind-`4` payload above. Independent replay removes
each selected reservation exactly once from the cancellation trace and treats
the completion as an audit-only event for risk state.

## Decoder rejection rules

The envelope decoder rejects wrong magic, every version other than `11`,
unknown kind, physical/declared length disagreement, payloads over the caller
limit, CRC mismatch, header/payload generation mismatch, and trailing bytes.
Typed payload decoders additionally reject invalid WAL-v11 tags, identifiers,
lengths, counts, row order, direct/history disagreement, mass-cancel grammar,
stop-source lineage, and risk contradictions.

## Compatibility boundary

Only envelope version 11 is accepted. Snapshot versions `1` through `10` are
expired and rejected before payload interpretation. A version-10 image cannot
represent new call-auction mass-cancel values in chronological history.
Authoritative predecessor migration requires an explicit provenance-preserving
converter.

## Primary-source provenance

- CRC-32C follows
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication is bounded by Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all),
  [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  POSIX.1-2024
  [`fsync`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html),
  and
  [`rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).
- Mass-cancel history, direct-state, account-index reconstruction, and risk
  validation are Quotick internal contracts verified by codec, corruption,
  direct-restore, durable-reopen, market-data, and coupled-risk tests.
