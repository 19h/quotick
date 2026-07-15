# Semantic Snapshot Format Version 4

`SnapshotFile` stores one complete typed semantic value in a bounded,
versioned CRC-32C envelope. Version 4 preserves the version-3 payload bytes and
kind tags for ledger `1`, continuous matching `2`, continuous coupled risk `3`,
and plain call auction `4`. It assigns previously invalid kind `5` to the
coupled call-auction/risk checkpoint. All multibyte integers are little-endian.
The payload trait is sealed, so downstream codecs cannot claim a reserved kind.

The [version-3 schema](snapshot-v3.md) remains the byte-level definition of
kinds `1` through `4`. Version 4 changes their envelope version only and does
not reinterpret those payloads.

## `QSNP` Envelope

This section defines the fixed header layout and its integrity rules.

The fixed header is 28 B:

| Offset (B) | Width (B) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSNP` |
| 4 | 2 | envelope version `4` |
| 6 | 2 | typed payload kind: ledger `1`, matching `2`, continuous coupled risk `3`, plain call auction `4`, coupled call-auction risk `5` |
| 8 | 8 | payload length `u64` |
| 16 | 4 | CRC-32C `u32` |
| 20 | 8 | semantic generation `u64` |

CRC-32C covers the complete header with bytes 16--19 set to zero, followed by
the exact payload. Physical file length must equal `28 B + payload length`.
The default payload limit is 1 GiB (1,073,741,824 B), and the selected `u64`
limit is checked before allocation or filesystem mutation.

CRC-32C detects accidental corruption. It is not a message-authentication code
and does not protect against an actor able to rewrite both payload and checksum.

## Coupled Call-Auction/Risk Payload, Kind 5

This section defines the kind-`5` payload, its decode-time verification, and
the staged capture path that publishes it.

Kind `5` contains the exact
[coupled call-auction risk checkpoint payload version 1](auction-risk-checkpoint-v1.md).
Its semantic generation is the completed call-auction execution-report WAL
boundary embedded in the nested auction checkpoint.

The payload binds:

1. the physical WAL origin occupied by one immutable instrument definition;
2. the exact metadata boundary following canonical account-ID-sorted immutable
   risk profiles;
3. phase, cycle, book revision, priority/trade counters, accepted identities,
   active orders, and complete command/report history;
4. canonical immutable profiles plus redundant positions and aggregate active
   reservation exposure.

Decode reconstructs the active reservations from private active orders,
compares every redundant account exposure, audits the coupled live structure,
and independently replays complete retained history through the core-first risk
gate. A historically risk-rejected submit must reproduce from the retained
profile set.

Live plain and coupled auction capture are staged independently of these stable
payloads. `CallAuctionCheckpointCapture` and
`CallAuctionRiskCheckpointCapture` expose no codec or snapshot implementation.
They perform structural/lineage projection (and coupled direct reconstruction)
without command execution. Consuming verification releases the respective
stable checkpoint only after exact replay and canonical projection equality.

Durable capture first synchronizes the represented WAL prefix and binds a
verified standalone publication to the same open shard incarnation and
pre-cutover epoch. Append-only suffix growth is permitted; reopen and physical
prefix retirement reject the handle.

A verified handle may also drive A/B prefix retirement through its private
physical cursor: the WAL replacement is `anchor(G)` plus the verified
post-capture suffix through the current head, and the epoch advances only
after publication.

## Lineage and WAL Cutover

This section defines lineage acceptance for kind-`5` checkpoints and their
role in checkpoint-assisted WAL recovery.

Kind-`5` lineage requires equal WAL origin, immutable definition, canonical
account identity/profile set, and exact command/report prefix; generation must
not regress. Position and exposure values may advance only as consequences of
that retained command lineage.

With an **uncut WAL**, checkpoint-assisted open verifies every definition,
profile, command, and report frame through the checkpoint generation before
applying only the suffix. With a **compacted WAL**, publication synchronizes
the inactive A/B kind-`5` slot before replacing the physical prefix with a
version-4 WAL checkpoint anchor containing exact slot, kind, generation,
payload length, checksum, and physical sequence. Recovery never guesses the
alternate slot.

Cutover bounds WAL bytes scanned at reopen, but the checkpoint deliberately
retains complete exact-retry and audit history. It does not bound snapshot size,
capture pause, semantic-history lifetime, or allocator-failure exposure.

## Compatibility Boundary

Only envelope version 4 is accepted. Snapshot versions 1, 2, and 3 are expired
and rejected before payload interpretation. Version 4 preserves the complete
payload bytes of every version-3 kind; changing an expired envelope version in
place remains invalid because CRC-32C covers the header.

If authoritative earlier artifacts must remain readable, migration requires an
explicit provenance-preserving converter. The runtime does not infer missing
fields or silently reinterpret an expired envelope.

## Primary-Source Provenance

- CRC-32C uses the Castagnoli procedure in
  [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
- Snapshot publication uses Rust
  [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all)
  and [`std::fs::rename`](https://doc.rust-lang.org/stable/std/fs/fn.rename.html),
  with filesystem persistence and same-filesystem rename semantics bounded by
  [POSIX `fsync`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/fsync.html)
  and [POSIX.1-2024 `rename`](https://pubs.opengroup.org/onlinepubs/9799919799/functions/rename.html).

The `QSNP` kind registry, financial payloads, lineage rules, and recovery
grammars are Quotick internal contracts verified by repository tests rather
than attributed to an external financial standard.
