# Trading-calendar Payload Format Version 1

This document defines the complete-value binary payload implemented by
`BinaryCodec` for `TradingCalendar`. It contains no transport, WAL, snapshot,
compression, signature, or authentication envelope. All multibyte integers are
little-endian.

Payload format version 1 is distinct from the encoded `CalendarVersion`: the
former selects this byte schema, while the latter identifies immutable
publisher content. The payload has no self-describing format-version field, so
an enclosing protocol must select version 1 before decoding.

## Scalar notation

| Notation | Width | Interpretation |
|---|---:|---|
| `u32` | 4 B | unsigned collection count |
| `u64` | 8 B | unsigned integer or UTC nanoseconds since the Unix epoch |
| `i32` | 4 B | signed days since 1970-01-01 |

`CalendarId`, `CalendarVersion`, and `TradingSessionId` are non-zero `u64`
domain values. `AccountingDate` is an `i32` scalar; no Gregorian, time-zone,
holiday, or venue-hours calculation occurs in the codec.

## Complete payload

For `S` sessions, total payload length is exactly `28 B + 44 S B`:

| Offset | Width | Field |
|---:|---:|---|
| 0 | 8 B | calendar ID |
| 8 | 8 B | immutable calendar version |
| 16 | 8 B | generation effective-from UTC timestamp |
| 24 | 4 B | session count `S` as `u32` |
| 28 | `44 S` B | session rows in canonical entry-time order |

Session row `i` begins at `28 + 44i`:

| Row offset | Width | Field |
|---:|---:|---|
| 0 | 8 B | trading-session ID |
| 8 | 4 B | accounting date as signed days since 1970-01-01 |
| 12 | 8 B | inclusive order-entry opening timestamp |
| 20 | 8 B | exclusive order-entry closing timestamp |
| 28 | 8 B | inclusive session-order expiry timestamp |
| 36 | 8 B | inclusive day-order expiry timestamp |

## Canonical validation

Decode and construction reject:

- a zero calendar ID, calendar version, or trading-session ID;
- `S = 0`;
- an entry window not satisfying `open < close`;
- session boundaries not satisfying
  `close <= session expiry <= day expiry`;
- an effective-from timestamp later than the first entry open;
- a session-order expiry later than the next session's entry open;
- decreasing accounting dates;
- unequal day-order expiries among sessions assigned to one accounting date;
- a prior date's day-order expiry later than the next date's entry open;
- duplicate trading-session IDs; and
- truncation or trailing bytes.

The `u32` session count is proved against the remaining payload using the
44 B minimum row size before allocator access. The decoder then makes one exact
fallible reservation for `S` rows, constructs each validated session, and
re-applies complete schedule validation. It separately reserves and sorts an
`S`-entry ID index; either reservation failure is typed.

## Resolution boundary

The payload stores schedule facts, not orders. `Day` and `GoodForSession`
requests resolve only while `open <= received_at < close`, producing the
session's absolute day-order or session-order expiry as the existing matching
GTD lifetime. Native matching TIF values pass through unchanged.

The matching WAL and checkpoint schemas retain that normalized absolute
deadline, not the calendar ID/version, session ID, accounting date, or original
calendar-relative qualifier. An enclosing gateway/audit protocol must retain
those values when reconstruction of the original request is required.

## Version compatibility

Version 1 bytes and validation rules are immutable. Any incompatible field,
ordering, width, or interpretation change requires a new
`trading-calendar-v<N>.md` contract and explicit enclosing-version selection.
Changing `CalendarVersion` alone identifies new schedule content and does not
change this payload schema.
