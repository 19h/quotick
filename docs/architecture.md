# Architecture

## System boundary

The implemented system is a deterministic state machine with local durable
matching and ledger runtimes. One `OrderBook` owns one instrument and accepts
commands from exactly one mutating thread. `DurableOrderBook` records each
command before matching and records its trace afterward. `DurableLedger`
records each prepared entry before committing calculated balances.

```text
implemented

immutable instrument definition -> definition-bound CRC-32C WAL
                                             |
validated command -> account risk -> command WAL -> instrument book
                         ^              ^                 |
                         |              +-- report WAL <--+-- sequenced trace
                         |                                   |
              positions/reservations <---------------- trade/order events
                                                             |
                                       absolute L2 updates / trade prints
                                                             |
                                            gap-detecting public replica
                                                             |
                                                           trade
                                                                       |
                                                   version-bound settlement rules
                                                                       |
                                                            balanced journal entry
                                                                       |
                                                               ledger-entry WAL
                                                                       |
                                                                 account balances

required platform layers, not implemented

gateway -> authentication -> portfolio/collateral risk -> replicated sequencer and shards
   -> clearing lifecycle -> reporting
```

## Matching invariants

1. Every command matches the book's instrument identifier and immutable
   definition version before business-state access.
2. New and replacement quantities satisfy the configured lot increment and
   inclusive size bounds; limit prices satisfy the signed tick grid and collar.
3. New orders and replacements require `Open`; cancellation remains available
   in `Open`, `CancelOnly`, `Halted`, and `Closed` states after identity checks.
4. An active order appears in exactly one hash-index entry and one FIFO level.
5. A level head has no previous order; a level tail has no next order.
6. Level quantity equals the sum of leaves quantities and is represented as
   `u128`; each individual leaves quantity is a non-zero `u64`.
7. Bids execute from highest price to lowest; asks execute from lowest price to
   highest; equal-price orders execute in insertion order.
8. Trade price is the resting order price and every trade carries the book's
   immutable instrument version.
9. FOK validation precedes every matching-state mutation.
10. Exact command replays reproduce the original event sequence and cannot
   reapply state.
11. A command identifier reused for different content cannot mutate state.
12. Event sequences are strictly increasing within a book.
13. Order identifiers cannot be reused after an accepted new order.

The book uses `BTreeMap<Price, PriceLevel>` for deterministic ordered price
discovery, `HashMap<OrderId, RestingOrder>` for direct lookup, and doubly linked
order identifiers for FIFO removal without scanning a level.

## Instrument invariants

1. Asset codes and instrument symbols are non-empty canonical uppercase ASCII
   in fixed-capacity representations; asset and instrument identifiers are
   non-zero.
2. Asset and price decimal scales are bounded to 18 digits.
3. A definition has distinct base and quote assets, positive settlement
   multipliers, a positive `i64`-representable tick, aligned collar endpoints,
   and positive aligned quantity bounds.
4. Catalog asset identifiers and codes are unique.
5. Instrument versions and effective timestamps increase strictly; symbol,
   kind, base asset, and quote asset cannot change under one instrument ID.
6. Effective-time lookup is `O(log V)` over `V` versions and exact-version
   lookup rejects an absent version.
7. A matching WAL's first frame contains the complete definition; reopening
   requires structural equality with the requested definition.
8. The definition-correlated settlement path rejects a trade whose instrument
   ID or version differs before constructing or persisting ledger postings.

## Ledger invariants

1. Every entry has at least two non-zero legs.
2. An entry contains at most one leg per `(account, asset)` pair.
3. Signed posting amounts sum to zero independently for every asset.
4. Validation and all checked balance calculations complete before commit.
5. Journal sequence, balances, idempotency index, and journal order change
   together after successful validation.
6. Posting order is canonicalized by `(asset, account)`.
7. Exact transaction retries do not change balances; differing content under
   the same transaction identifier is rejected.
8. Settlement transaction identifiers are globally supplied because book trade
   identifiers are only local to one instrument shard.
9. Preparation calculates every next balance without mutation and binds it to
   the current ledger generation.
10. Durable posting writes the canonical entry before committing its prepared
    balances; stale preparations cannot commit.
11. Recovery accepts only ledger-entry records and reconstructs every balance
    from the canonical WAL sequence.

Signed balances are intentional accounting state. Credit limits, collateral,
and margin are not inferred by the ledger. The implemented order risk layer
consumes seeded positions and matching traces; it does not derive available
collateral from ledger balances.

## Pre-trade risk invariants

1. Each account has at most one immutable profile per risk-managed shard, with
   `Active`, `ReduceOnly`, or `Blocked` entry state. Cancellation bypasses entry
   limits after matching ownership and identity validation.
2. Core instrument and matching business checks precede risk checks; a core
   rejection cannot be replaced by a risk rejection.
3. Per-order quantity/notional, resting-order count, aggregate resting
   quantity/notional, and worst-case long/short position are independently
   bounded with checked integer arithmetic.
4. Incoming notional covers every reachable execution price: buy limits use
   the maximum absolute magnitude over `[collar minimum, limit]`, sell limits
   over `[limit, collar maximum]`, and market orders over the full collar.
   Units are raw price quanta multiplied by lots (`u128`).
5. IOC, FOK, and market commands cannot retain a reservation and therefore do
   not consume resting-order count/quantity/notional capacity. Their complete
   quantity still consumes worst-case position and per-order capacity.
6. Once an order rests, its maker execution price is exact, so retained
   notional is `abs(resting price) × leaves lots`.
7. Worst-case long exposure is executed position plus all resting buy lots plus
   the incoming buy quantity; short exposure is the executed position minus
   equivalent sell exposure.
8. `ReduceOnly` permits only the side opposing a non-zero position and requires
   all reducing reservations plus the new quantity not to cross zero.
9. Matching traces release maker reservations on fills, all reservations on
   cancellation, old exposure before replacement, and prevented resting lots
   under decrement-and-cancel STP. Trades update buyer/seller positions once.
10. Exact command retries do not apply risk state twice. Risk rejections are
    normal sequenced and durable rejection events.
11. Cross-audit recomputes account aggregates from reservations and verifies a
    one-to-one structural match with every active book order.
12. A durable risk shard binds the complete instrument definition followed by
    account-ID-sorted immutable profiles. Recovery completes only an exact
    metadata prefix before the first command.

## Market-data publication invariants

1. Every non-replayed matching event maps to exactly one public update carrying
   the identical event sequence and timestamp; no private event creates a
   sequence hole.
2. Public updates contain instrument ID and immutable definition version. They
   contain no account, order, or command identifiers.
3. Events without a public depth or trade effect emit `NoBookChange`; version 1
   performs no conflation or sequence renumbering.
4. Level updates contain absolute post-event aggregate quantity and order count,
   not relative deltas. Both fields are zero only for level deletion.
5. Trade updates contain monotonic trade ID, signed execution price, positive
   quantity, aggressor side, and the absolute maker level after execution. A
   replica proves that aggregate maker quantity falls by exactly the printed
   quantity and that maker order count is unchanged or decreases by one.
6. The publisher tracks active order side, price, and leaves solely to translate
   private traces. It removes or decrements makers on fills, handles every STP
   policy, and removes old exposure before non-priority-retaining replacement.
7. Exact command retries produce no second public update.
8. Publisher bootstrap from a live or WAL-recovered book captures all active
   orders, depth, final event sequence, and final trade ID, then cross-audits
   the private and public aggregates.
9. A full-depth snapshot contains occupied bids in descending price order and
   asks in ascending price order at one source sequence. Locked or crossed
   snapshots are invalid.
10. A replica rejects a missing, duplicated, or reordered sequence before
    mutating depth. A non-stale full-depth snapshot resets the recovery boundary.
11. Trace or structural failures after incremental mutation poison publisher or
    replica state; a fresh authoritative bootstrap/snapshot is required.
12. The stable complete-value schema is
    [Market-data payload format version 1](market-data-v1.md). Network framing,
    fanout, entitlement, and retransmission sessions are outside this boundary.

## Journal and recovery invariants

1. Every frame carries `QWAL` magic, format version, typed record kind, bounded
   payload length, CRC-32C, and contiguous segment sequence.
2. CRC-32C covers the complete header with its checksum field zeroed plus the
   payload.
3. Payload allocation occurs only after the declared length is checked against
   the configured maximum and physical file length.
4. Repair mode truncates only a physically incomplete final frame.
5. Invalid magic, unsupported version, unknown kind, checksum mismatch, and
   sequence discontinuity are non-repairable corruption.
6. An ambiguous write or durability-barrier failure poisons the writer until
   reopen and recovery.
7. A batch uses one write and barrier but is not a transactional frame; recovery
   may retain its verified prefix.
8. Typed codecs reject invalid identifiers, quantities, enum tags, booleans,
   lengths, trailing bytes, noncanonical postings, and contradictory reports.
9. A plain matching journal begins with one complete instrument definition. A
   risk-managed journal then contains a canonical account-profile set before
   the first command. Metadata after command processing is invalid grammar.
10. Durable matching writes a command before state mutation and writes the
    reproduced report afterward.
11. Recovery accepts only bound metadata followed by alternating
    command/report records, byte-equivalent deterministic matching/risk
    outcomes, and at most one final command without a report.
12. A writer canonicalizes the WAL path and atomically creates a versioned
    sidecar lease before scanning or mutation. A second canonical-path writer
    fails closed; read-only snapshot readers remain available.
13. Lease and newly created WAL directory entries are synchronized through the
    parent directory. Clean `close` synchronizes WAL data/metadata before
    releasing and synchronizing the lease removal.
14. Abandoned lease removal requires the exact owner identity previously
    observed from `WriterLeaseHeld`; process liveness and a quiesced recovery
    window are external preconditions. Malformed leases require a separate
    explicit recovery operation and are never reclaimed during normal open.
15. The default append acknowledgement is `SyncAll`. Partial writes, failed
    barriers, and failed explicit synchronization poison the writer; injected
    failures verify complete-prefix recovery and ambiguous complete-frame replay.

The authoritative version-1 framing and payload schema is
[WAL format version 1](wal-v1.md). Filesystem and device assumptions are bounded
by the [Local storage contract](storage.md).

## Failure model

Business rejections are sequenced trace events. Identifier exhaustion and
idempotency collisions are operational errors. Arithmetic uses checked
operations. Matching state, risk reservations/positions, and ledger balances
can be reconstructed from verified local WALs. Public depth can bootstrap from
that recovered matching state; consumers repair an incremental gap with a
newer full-depth snapshot. Forced-process-termination, concurrent-writer,
abandoned/malformed-lease, injected-write/barrier, torn-report, metadata-prefix,
replay-divergence, entry-reconstruction, feed-gap, and publisher cross-audit
tests exercise these paths. There is no claim of
replicated durability, remote consensus, snapshot recovery, or storage-device
power-loss behavior.

## Standards and primary-source provenance

- CRC-32C uses the Castagnoli generator and complemented input/output procedure
  specified for CRC32C in [IETF RFC 3720, section 12.1](https://www.rfc-editor.org/rfc/rfc3720#section-12.1).
  Quotick applies that checksum to its own WAL framing; it does not claim iSCSI
  protocol compatibility.
- Writer leases use Rust's atomic, fail-if-present
  [`OpenOptions::create_new`](https://doc.rust-lang.org/stable/std/fs/struct.OpenOptions.html#method.create_new).
  WAL and directory barriers use [`File::sync_all`](https://doc.rust-lang.org/stable/std/fs/struct.File.html#method.sync_all);
  the underlying transfer remains conditional on the implementation as specified
  by [POSIX `fsync`](https://pubs.opengroup.org/onlinepubs/009695399/functions/fsync.html).
- The signed `Price` domain covers real exchange cases in which negative prices
  are supported. [CME Clearing Advisory 20-152](https://www.cmegroup.com/notices/clearing/2020/04/Chadv20-152.pdf)
  is the primary-source basis for retaining negative futures-price support.
- All other field order, admission, accounting, sequencing, and recovery rules
  in this repository are Quotick's explicitly specified internal contracts,
  verified by the referenced test suites rather than attributed to an external
  standard.

## Required production increments

| Impact | Capability | Evidence required for completion |
|---|---|---|
| High | Durable storage completion | automatic segment rotation/retention; kernel inode locking or qualified alias exclusion; forced-power-loss filesystem/device evidence |
| High | Ledger reconciliation and checkpoints | independent trial balances, external statement reconciliation, checkpoint proofs, corrections, and bounded restart time |
| High | Snapshots and compaction | checksummed state snapshots, WAL cutover proof, bounded restart time, and retained audit history |
| High | Replication and failover | deterministic leader change; duplicate/lost-command fault injection; recovery-point objective evidence |
| High | Portfolio/collateral risk expansion | cross-instrument netting, currency conversion, margin models, ledger-backed availability, scenario stress, and replicated reservation ownership |
| High | Instrument lifecycle expansion | trading calendars, session transitions, corporate actions, derivative expiry/exercise, and external symbology mappings |
| High | Clearing lifecycle | novation/allocation, fees, settlement dates, fails, corrections, busts, and reconciliation |
| High | Security boundary | authenticated principals, authorization policy, secret management, audit export, and abuse controls |
| Medium | Gateways and schemas | versioned binary protocol, FIX adapter, backpressure, session recovery, and conformance fixtures |
| Medium | Market-data distribution | authenticated transport framing, entitlement, fanout, retransmission sessions, bandwidth control, and conformance fixtures |
| Medium | Operations | metrics, traces, structured logs, health, capacity limits, alert rules, and runbooks |
| Medium | Performance evidence | pinned-hardware benchmarks, allocation counts, tail latency, saturation, and regression thresholds |
| Medium | Verification expansion | model-based/property tests, fuzzing, crash simulation, concurrency model checking, and long soak tests |
| Low | User interfaces | administrative, trader, surveillance, reporting, and visualization surfaces after authoritative APIs stabilize |
