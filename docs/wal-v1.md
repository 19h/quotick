# WAL Format Version 1

This document is the authoritative byte-level schema for Quotick WAL version 1.
All multibyte integers are little-endian. No Rust enum layout, padding, pointer,
or platform ABI is persisted.

## Frame

| Offset (bytes) | Width (bytes) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QWAL` |
| 4 | 2 | format version `1` |
| 6 | 2 | record kind: command `1`, execution report `2`, ledger entry `3`, instrument definition `4`, account risk definition `5` |
| 8 | 4 | payload length |
| 12 | 4 | CRC-32C |
| 16 | 8 | contiguous journal sequence |
| 24 | payload length | typed payload |

CRC-32C uses the reflected Castagnoli polynomial `0x82F63B78`, initial state
`0xFFFFFFFF`, and final XOR `0xFFFFFFFF`. The checksum covers the complete
header with bytes 12–15 set to zero, followed by the payload.

## Segmented-directory marker version 1

Segmentation does not alter `QWAL` frames or their global sequence. A segmented
WAL is a dedicated directory containing `format.qseg` and canonical files named
`segment-SSSSSSSSSSSSSSSSSSSS.qwal`, where the 20 decimal digits encode the
first frame sequence assigned to that file.

The 26-byte `format.qseg` marker is:

| Offset (bytes) | Width (bytes) | Field |
|---:|---:|---|
| 0 | 4 | ASCII magic `QSEG` |
| 4 | 2 | marker version `1` |
| 6 | 8 | maximum segment bytes `u64` |
| 14 | 8 | initial global sequence `u64` |
| 22 | 4 | maximum payload bytes `u32` |

All marker integers are little-endian. These three configuration values are
immutable for the directory. The active acknowledgement policy and recovery
mode are intentionally not persisted because they do not change frame or
segment interpretation.

An invalid partial marker is recoverable only while no segment or unknown
directory entry exists. A valid marker is never removed by incomplete-
initialization recovery.

## Scalar notation

- `u8`, `u32`, `u64`, `u128`, `i64`, `i128`: fixed-width integers.
- `bool`: `u8`, where `0` is false and `1` is true.
- Every identifier: validated non-zero `u64`.
- `Price`: `i64` instrument quantum.
- `Quantity`: validated non-zero `u64` lots.
- Collection: `u32` element count followed by elements.

## Instrument-definition payload

Fields occur in this exact order:

1. instrument ID `u64`;
2. immutable version `u64`;
3. effective-from Unix timestamp in nanoseconds `u64`;
4. symbol length `u8`, then that many canonical ASCII bytes (1–32 bytes);
5. instrument-kind tag `u8`;
6. base asset ID `u64`, quote asset ID `u64`;
7. price decimal scale `u8`, tick size `u64`, minimum price `i64`, maximum
   price `i64`;
8. quantity increment `u64`, minimum quantity `u64`, maximum quantity `u64`;
9. base units per lot `u64`, quote units per raw price unit `u64`;
10. trading-state tag `u8`.

Instrument-kind tags are equity `0`, spot `1`, future `2`, option `3`, bond
`4`, swap `5`, index `6`, and synthetic `7`. Trading-state tags are open `0`,
cancel-only `1`, halted `2`, and closed `3`. Decoding reconstructs the validated
domain types and rejects any inconsistent scale, grid, boundary, identity, or
conversion multiplier.

## Account-risk-definition payload

Fields occur in this exact order (121 bytes total):

1. account ID `u64`;
2. account-risk-state tag `u8`: active `0`, reduce-only `1`, blocked `2`;
3. signed initial position lots `i128`;
4. maximum order quantity lots `u64`;
5. maximum order notional `u128`;
6. maximum open orders `u64`;
7. maximum open quantity lots `u128`;
8. maximum open notional `u128`;
9. maximum long position lots `u128`;
10. maximum absolute short position lots `u128`.

All limits are positive. Per-order quantity/notional cannot exceed their
aggregate open counterparts; position limits cannot exceed `i128::MAX`; the
initial position must be inside its signed bound. Risk-managed journals store
these records in strictly increasing account-ID order.

## Command payload

The first `u8` selects new `0`, cancel `1`, or replace `2`.

- New: command ID, order ID, account ID, instrument ID, instrument version,
  side, quantity, order type, time in force, self-trade policy, receive timestamp.
- Cancel: command ID, order ID, account ID, instrument ID, instrument version,
  receive timestamp.
- Replace: command ID, order ID, account ID, instrument ID, instrument version,
  new leaves quantity, new price, receive timestamp.

Side tags are buy `0`, sell `1`. Order type tags are market `0`, limit `1`
followed by price. Time-in-force tags are GTC `0`, IOC `1`, FOK `2`, post-only
`3`. Self-trade tags are cancel aggressor `0`, cancel resting `1`, cancel both
`2`, decrement-and-cancel `3`.

Rejection-reason tags are wrong instrument `0`, duplicate order `1`, unknown
order `2`, not owner `3`, market cannot rest `4`, market cannot post `5`,
unsupported FOK/STP combination `6`, insufficient liquidity `7`, post-only
would cross `8`, wrong instrument version `9`, instrument not open `10`, price
off tick grid `11`, price outside collar `12`, quantity off lot grid `13`,
quantity outside limits `14`, missing risk profile `15`, blocked risk account
`16`, reduce-only violation `17`, risk order-quantity limit `18`, risk
order-notional limit `19`, risk open-order-count limit `20`, risk open-quantity
limit `21`, risk open-notional limit `22`, risk position limit `23`, and risk
arithmetic overflow `24`. Cancellation-reason tags are user request `0`,
unfilled remainder `1`, STP aggressor `2`, and STP resting `3`.

## Execution-report payload

Fields are command ID, outcome, replay boolean, event count, then events.
Outcome is accepted `0` or rejected `1` followed by a rejection-reason tag.
Each event contains event sequence, command ID, occurrence timestamp, and one
event-kind union:

| Tag | Event |
|---:|---|
| 0 | order accepted: order ID, quantity |
| 1 | order rested: order ID, price, leaves quantity |
| 2 | trade: trade ID, instrument ID, instrument version, price, quantity, buy/sell orders, buyer/seller accounts, maker/taker orders |
| 3 | order cancelled: order ID, quantity, reason |
| 4 | order replaced: order ID, old/new prices, old/new quantities, retained-priority boolean |
| 5 | self-trade prevented: aggressor/resting orders, quantity, policy |
| 6 | command rejected: reason |

Decoding requires non-empty, contiguous events correlated to the report command.
Accepted reports cannot contain rejection events; rejected reports contain
exactly one matching rejection event.

## Ledger-entry payload

Fields are transaction ID, source reference `u64`, posting count, then postings.
Each posting is account ID, asset ID, and signed `i128` amount. Postings are
strictly sorted by `(asset ID, account ID)`, contain no duplicate pair or zero
amount, and balance independently to zero for every asset.

## Recovery

A frame is authoritative only after its header, declared payload, CRC-32C, and
expected sequence verify. Repair mode may truncate bytes beginning at an
incomplete final frame. It never repairs or skips a complete corrupt frame.

Across a segmented directory, canonical filenames are sorted by their encoded
start sequence and frames remain one contiguous global sequence. Every
non-final file is scanned in strict mode and must contain at least one frame.
Only the final file may be empty or may repair a physically incomplete tail.
The final empty-file case represents interruption between segment creation and
the first append and is reused on reopen. A frame or grouped append larger than
the configured segment capacity is rejected before rotation; grouped appends
are placed wholly within one segment.

A matching journal begins with exactly one instrument-definition record. The
remaining grammar is alternating command and execution-report records, with at
most one final command lacking a report. An empty journal is initialized by
appending the requested definition; a nonempty journal without definition as
its first frame is rejected. Recovery compares the complete persisted
definition with the requested definition before replay. A ledger journal
accepts only ledger-entry records.

A ledger semantic checkpoint is not a WAL frame and introduces no version-1
record kind. It is a separate `QSNP` file described by
[Semantic snapshot format version 1](snapshot-v1.md). Checkpoint-assisted open
still scans every ledger-entry frame and requires the checkpoint's complete
entry sequence to equal the exact WAL prefix before applying the remaining
suffix. Version 1 does not truncate or retire that prefix.

A risk-managed matching journal inserts zero or more account-risk-definition
records between the instrument definition and the first command. The complete
canonical profile set must equal the requested set. If the file ends during
metadata initialization, recovery may append only the missing suffix of an
exact profile prefix; metadata drift or metadata after a command is rejected.
Commands and reports then follow the same alternating grammar, but replay uses
the coupled matching/risk state machine so risk rejections, positions, and
reservations are reconstructed deterministically.

## Writer ownership and acknowledgement

The `QWAL` frame format does not encode writer ownership. Before opening a raw
WAL, the runtime atomically creates the canonical-path sidecar lease specified
in the [Local storage contract](storage.md). A segmented directory instead has
one manager lease and rejects raw writers for its member files. Lease framing
has its own `QLCK` magic and version and is not a journal record.

`SyncAll` is the default append policy. A receipt states which configured
barrier returned successfully; it does not alter frame bytes. Any partial write
or failed acknowledgement barrier poisons the in-process writer. Reopening
verifies the physical log: an ambiguous complete frame is retained and replayed,
while repair mode may truncate only an incomplete final frame.
