//! Constructor-reserved short-gap retransmission for sequenced market data.

use std::fmt;
use std::iter::FusedIterator;

use crate::auction_market_data::{CallAuctionMarketDataBatch, CallAuctionMarketDataUpdate};
use crate::domain::{InstrumentId, InstrumentVersion};

use super::{MarketDataBatch, MarketDataUpdate};

mod private {
    pub trait Sealed {}
}

/// A Quotick incremental type accepted by the shared bounded replay core.
///
/// This trait is sealed. Continuous and call-auction updates are the supported
/// implementations; downstream crates cannot add implementations whose
/// identity or sequence semantics the ring cannot validate.
pub(crate) trait SequencedReplayUpdate: private::Sealed + Copy + Eq {
    const PRESERVE_BATCH_BOUNDARIES: bool;

    /// Returns the immutable instrument identifier used by replay admission.
    #[doc(hidden)]
    fn replay_instrument_id(self) -> InstrumentId;

    /// Returns the immutable instrument version used by replay admission.
    #[doc(hidden)]
    fn replay_instrument_version(self) -> InstrumentVersion;

    /// Returns the non-zero source sequence used by replay admission.
    #[doc(hidden)]
    fn replay_sequence(self) -> u64;
}

impl private::Sealed for MarketDataUpdate {}

impl SequencedReplayUpdate for MarketDataUpdate {
    const PRESERVE_BATCH_BOUNDARIES: bool = false;

    fn replay_instrument_id(self) -> InstrumentId {
        self.instrument_id()
    }

    fn replay_instrument_version(self) -> InstrumentVersion {
        self.instrument_version()
    }

    fn replay_sequence(self) -> u64 {
        self.sequence()
    }
}

impl private::Sealed for CallAuctionMarketDataUpdate {}

impl SequencedReplayUpdate for CallAuctionMarketDataUpdate {
    const PRESERVE_BATCH_BOUNDARIES: bool = true;

    fn replay_instrument_id(self) -> InstrumentId {
        self.instrument_id()
    }

    fn replay_instrument_version(self) -> InstrumentVersion {
        self.instrument_version()
    }

    fn replay_sequence(self) -> u64 {
        self.sequence()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReplaySlot<U> {
    update: U,
    starts_batch: bool,
    ends_batch: bool,
}

/// Failure before a market-data replay buffer owns usable storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataReplayConstructionError {
    /// A retransmission window cannot retain zero updates.
    ZeroCapacity,
    /// Complete ring storage could not be represented or allocated.
    ReservationFailed {
        /// Requested retained-update maximum.
        maximum: usize,
    },
}

impl fmt::Display for MarketDataReplayConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroCapacity => {
                formatter.write_str("market-data replay capacity must be non-zero")
            }
            Self::ReservationFailed { maximum } => write!(
                formatter,
                "failed to reserve market-data replay storage through {maximum} updates"
            ),
        }
    }
}

impl std::error::Error for MarketDataReplayConstructionError {}

/// Deterministic replay admission or range-query failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataReplayError {
    /// An update belongs to another instrument.
    WrongInstrument,
    /// An update belongs to another immutable instrument version.
    WrongInstrumentVersion,
    /// A non-replayed publication batch was empty, or a replay marker had data.
    InvalidBatch,
    /// One command batch cannot fit atomically in the retained window.
    BatchExceedsCapacity {
        /// Configured retained-update maximum.
        maximum: usize,
        /// Updates in the submitted batch.
        attempted: usize,
    },
    /// The submitted update stream skipped or reordered a sequence.
    SequenceGap {
        /// Required next sequence.
        expected: u64,
        /// Received sequence.
        actual: u64,
    },
    /// One retained sequence was reused for different update content.
    SequenceCollision {
        /// Conflicting source sequence.
        sequence: u64,
    },
    /// The requested or retried update is older than retained evidence.
    Unavailable {
        /// First sequence still retained, absent before the first append.
        earliest_available: Option<u64>,
        /// First sequence required by the caller.
        requested_sequence: u64,
    },
    /// A range cursor is later than the latest published boundary.
    CursorAhead {
        /// Latest sequence observed by the buffer.
        latest_sequence: u64,
        /// Caller-supplied exclusive cursor.
        requested_after: u64,
    },
    /// An auction replay cursor falls inside, rather than after, one batch.
    CursorInsideBatch {
        /// Caller-supplied exclusive cursor.
        requested_after: u64,
    },
    /// The first complete retained batch exceeds the requested page limit.
    BatchExceedsRequestLimit {
        /// Caller-supplied page maximum in updates.
        maximum: usize,
        /// Updates in the next complete batch.
        required: usize,
    },
    /// A replay page must request at least one update.
    ZeroRequestLimit,
    /// Checked source-sequence arithmetic overflowed.
    ArithmeticOverflow,
    /// Internal ring topology or retained content contradicted its metadata.
    CorruptState(&'static str),
}

impl fmt::Display for MarketDataReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongInstrument => {
                formatter.write_str("market-data replay update belongs to another instrument")
            }
            Self::WrongInstrumentVersion => formatter
                .write_str("market-data replay update belongs to another instrument version"),
            Self::InvalidBatch => formatter.write_str(
                "market-data replay batch must be non-empty unless it is an empty retry marker",
            ),
            Self::BatchExceedsCapacity { maximum, attempted } => write!(
                formatter,
                "market-data replay capacity {maximum} is below batch cardinality {attempted}"
            ),
            Self::SequenceGap { expected, actual } => write!(
                formatter,
                "market-data replay sequence gap: expected {expected}, received {actual}"
            ),
            Self::SequenceCollision { sequence } => write!(
                formatter,
                "market-data replay sequence {sequence} has conflicting content"
            ),
            Self::Unavailable {
                earliest_available,
                requested_sequence,
            } => write!(
                formatter,
                "market-data replay sequence {requested_sequence} is unavailable; earliest retained is {earliest_available:?}"
            ),
            Self::CursorAhead {
                latest_sequence,
                requested_after,
            } => write!(
                formatter,
                "market-data replay cursor {requested_after} exceeds latest sequence {latest_sequence}"
            ),
            Self::CursorInsideBatch { requested_after } => write!(
                formatter,
                "market-data replay cursor {requested_after} falls inside a publication batch"
            ),
            Self::BatchExceedsRequestLimit { maximum, required } => write!(
                formatter,
                "market-data replay page limit {maximum} is below next batch cardinality {required}"
            ),
            Self::ZeroRequestLimit => {
                formatter.write_str("market-data replay request limit must be non-zero")
            }
            Self::ArithmeticOverflow => {
                formatter.write_str("market-data replay sequence arithmetic overflowed")
            }
            Self::CorruptState(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for MarketDataReplayError {}

/// Allocation and retained-range telemetry for one replay ring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketDataReplayStatus {
    /// Semantic retained-update maximum.
    pub maximum: usize,
    /// Allocated vector capacity; never changes after construction.
    pub allocation_capacity: usize,
    /// Updates currently retained.
    pub retained: usize,
    /// First retained source sequence.
    pub earliest_sequence: Option<u64>,
    /// Latest published source boundary, including an empty initial prefix.
    pub latest_sequence: u64,
}

/// Fixed-capacity retransmission storage for one continuous instrument feed.
#[derive(Debug)]
pub struct MarketDataReplayBuffer {
    inner: SequencedMarketDataReplayBuffer<MarketDataUpdate>,
}

impl MarketDataReplayBuffer {
    /// Constructs an empty replay window after an already-published boundary.
    ///
    /// `initial_sequence` permits construction from a publisher snapshot or
    /// recovered book. No update at or before that boundary is initially
    /// available for retransmission.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayConstructionError`] before state exists when
    /// `maximum_updates` is zero or complete ring storage cannot be reserved.
    pub fn try_new(
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        initial_sequence: u64,
        maximum_updates: usize,
    ) -> Result<Self, MarketDataReplayConstructionError> {
        Ok(Self {
            inner: SequencedMarketDataReplayBuffer::<MarketDataUpdate>::try_new(
                instrument_id,
                instrument_version,
                initial_sequence,
                maximum_updates,
            )?,
        })
    }

    /// Returns this ring's immutable identity and allocation telemetry.
    #[must_use]
    pub fn status(&self) -> MarketDataReplayStatus {
        self.inner.status()
    }

    /// Returns the first retained sequence, if any update has been retained.
    #[must_use]
    pub fn earliest_sequence(&self) -> Option<u64> {
        self.inner.earliest_sequence()
    }

    /// Returns the latest published sequence observed by the ring.
    #[must_use]
    pub const fn latest_sequence(&self) -> u64 {
        self.inner.latest_sequence()
    }

    /// Returns the number of retained updates.
    #[must_use]
    pub const fn retained_len(&self) -> usize {
        self.inner.retained_len()
    }

    /// Returns the fixed semantic retained-update maximum.
    #[must_use]
    pub fn maximum_updates(&self) -> usize {
        self.inner.maximum_updates()
    }

    /// Admits one decoded incremental update.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayError`] for identity drift, a gap, conflicting
    /// duplicate content, or a retry older than retained evidence.
    pub fn push(&mut self, update: MarketDataUpdate) -> Result<usize, MarketDataReplayError> {
        self.inner.push(update)
    }

    /// Atomically admits one publisher command batch.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayError`] before mutation for invalid batch
    /// shape, identity drift, capacity, gaps, collisions, or evicted overlap.
    pub fn push_batch(&mut self, batch: &MarketDataBatch) -> Result<usize, MarketDataReplayError> {
        self.inner.push_batch(batch)
    }

    /// Returns at most `maximum_updates` values after `after_sequence`.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayError`] for a zero limit, future cursor, or an
    /// evicted required sequence.
    pub fn replay_after(
        &self,
        after_sequence: u64,
        maximum_updates: usize,
    ) -> Result<MarketDataReplay<'_>, MarketDataReplayError> {
        self.inner.replay_after(after_sequence, maximum_updates)
    }

    /// Audits allocation, occupancy, identity, sequences, and batch boundaries.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayError::CorruptState`] on the first internal
    /// contradiction.
    pub fn validate(&self) -> Result<(), MarketDataReplayError> {
        self.inner.validate()
    }
}

/// Fixed-capacity, source-sequenced retransmission storage for one instrument.
#[derive(Debug)]
pub(crate) struct SequencedMarketDataReplayBuffer<U: SequencedReplayUpdate> {
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    slots: Vec<Option<ReplaySlot<U>>>,
    next_slot: usize,
    retained: usize,
    latest_sequence: u64,
}

impl<U: SequencedReplayUpdate> SequencedMarketDataReplayBuffer<U> {
    /// Constructs an empty replay window after an already-published boundary.
    ///
    /// `initial_sequence` permits construction from a publisher snapshot or
    /// recovered book. No update at or before that boundary is initially
    /// available for retransmission.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayConstructionError`] before state exists when
    /// `maximum_updates` is zero or complete ring storage cannot be reserved.
    pub fn try_new(
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        initial_sequence: u64,
        maximum_updates: usize,
    ) -> Result<Self, MarketDataReplayConstructionError> {
        if maximum_updates == 0 {
            return Err(MarketDataReplayConstructionError::ZeroCapacity);
        }
        let mut slots = Vec::new();
        slots.try_reserve_exact(maximum_updates).map_err(|_| {
            MarketDataReplayConstructionError::ReservationFailed {
                maximum: maximum_updates,
            }
        })?;
        slots.resize(maximum_updates, None);
        Ok(Self {
            instrument_id,
            instrument_version,
            slots,
            next_slot: 0,
            retained: 0,
            latest_sequence: initial_sequence,
        })
    }

    /// Returns this ring's immutable identity and allocation telemetry.
    #[must_use]
    pub fn status(&self) -> MarketDataReplayStatus {
        MarketDataReplayStatus {
            maximum: self.slots.len(),
            allocation_capacity: self.slots.capacity(),
            retained: self.retained,
            earliest_sequence: self.earliest_sequence(),
            latest_sequence: self.latest_sequence,
        }
    }

    /// Returns the first retained sequence, if any update has been retained.
    #[must_use]
    pub fn earliest_sequence(&self) -> Option<u64> {
        if self.retained == 0 {
            return None;
        }
        self.slots[self.earliest_slot()].map(|slot| slot.update.replay_sequence())
    }

    /// Returns the latest published sequence observed by the ring.
    #[must_use]
    pub const fn latest_sequence(&self) -> u64 {
        self.latest_sequence
    }

    /// Returns the number of retained updates.
    #[must_use]
    pub const fn retained_len(&self) -> usize {
        self.retained
    }

    /// Returns the fixed semantic retained-update maximum.
    #[must_use]
    pub fn maximum_updates(&self) -> usize {
        self.slots.len()
    }
}

impl SequencedMarketDataReplayBuffer<MarketDataUpdate> {
    /// Admits one decoded incremental update.
    ///
    /// Exact retained duplicates are no-ops. Identity, collision, eviction,
    /// and sequence checks complete before the ring changes.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayError`] for identity drift, a gap, conflicting
    /// duplicate content, or a retry older than retained evidence.
    pub fn push(&mut self, update: MarketDataUpdate) -> Result<usize, MarketDataReplayError> {
        self.push_updates(std::slice::from_ref(&update))
    }

    /// Atomically admits one publisher command batch.
    ///
    /// Empty exact-retry markers are accepted as no-ops. A non-replayed batch
    /// must fit the complete ring so its update interval is never partially
    /// retained by one admission call.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayError`] before mutation for invalid batch
    /// shape, identity drift, capacity, gaps, collisions, or evicted overlap.
    pub fn push_batch(&mut self, batch: &MarketDataBatch) -> Result<usize, MarketDataReplayError> {
        if batch.replayed() {
            return if batch.updates().is_empty() {
                Ok(0)
            } else {
                Err(MarketDataReplayError::InvalidBatch)
            };
        }
        if batch.updates().is_empty() {
            return Err(MarketDataReplayError::InvalidBatch);
        }
        self.push_updates(batch.updates())
    }

    /// Returns at most `maximum_updates` values strictly after `after_sequence`.
    ///
    /// The result borrows the ring and iterates in source order without copying
    /// or allocation, including when the retained range wraps physically.
    /// Callers can paginate using the returned final sequence.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayError`] for a zero page limit, a cursor beyond
    /// the published boundary, or a required sequence that has been evicted.
    pub fn replay_after(
        &self,
        after_sequence: u64,
        maximum_updates: usize,
    ) -> Result<MarketDataReplay<'_>, MarketDataReplayError> {
        if maximum_updates == 0 {
            return Err(MarketDataReplayError::ZeroRequestLimit);
        }
        if after_sequence > self.latest_sequence {
            return Err(MarketDataReplayError::CursorAhead {
                latest_sequence: self.latest_sequence,
                requested_after: after_sequence,
            });
        }
        if after_sequence == self.latest_sequence {
            return Ok(MarketDataReplay::empty(&self.slots, self.next_slot));
        }
        let requested_sequence = after_sequence
            .checked_add(1)
            .ok_or(MarketDataReplayError::ArithmeticOverflow)?;
        let earliest = self
            .earliest_sequence()
            .ok_or(MarketDataReplayError::Unavailable {
                earliest_available: None,
                requested_sequence,
            })?;
        if requested_sequence < earliest {
            return Err(MarketDataReplayError::Unavailable {
                earliest_available: Some(earliest),
                requested_sequence,
            });
        }
        let offset = usize::try_from(requested_sequence - earliest)
            .map_err(|_| MarketDataReplayError::CorruptState("replay offset exceeds usize"))?;
        if offset >= self.retained {
            return Err(MarketDataReplayError::CorruptState(
                "replay cursor is absent from the retained range",
            ));
        }
        let available = self.retained - offset;
        let returned = available.min(maximum_updates);
        let next_slot = ring_advance(self.earliest_slot(), offset, self.slots.len());
        let returned_minus_one =
            u64::try_from(returned - 1).map_err(|_| MarketDataReplayError::ArithmeticOverflow)?;
        let last_sequence = requested_sequence
            .checked_add(returned_minus_one)
            .ok_or(MarketDataReplayError::ArithmeticOverflow)?;
        Ok(MarketDataReplay {
            slots: &self.slots,
            next_slot,
            remaining: returned,
            first_sequence: Some(requested_sequence),
            last_sequence: Some(last_sequence),
            has_more: returned < available,
        })
    }
}

impl SequencedMarketDataReplayBuffer<CallAuctionMarketDataUpdate> {
    /// Atomically admits one call-auction publisher command batch.
    ///
    /// Batch starts and ends are retained with the updates so retransmission
    /// preserves the replica's command-sequence boundary. Empty exact-retry
    /// markers are accepted as no-ops.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayError`] before mutation for invalid batch
    /// shape, identity drift, capacity, gaps, collisions, or evicted overlap.
    pub(crate) fn push_auction_batch(
        &mut self,
        batch: &CallAuctionMarketDataBatch,
    ) -> Result<usize, MarketDataReplayError> {
        if batch.replayed() {
            return if batch.updates().is_empty() {
                Ok(0)
            } else {
                Err(MarketDataReplayError::InvalidBatch)
            };
        }
        if batch.updates().is_empty() {
            return Err(MarketDataReplayError::InvalidBatch);
        }
        self.push_updates(batch.updates())
    }

    /// Returns complete call-auction batches strictly after `after_sequence`.
    ///
    /// `maximum_updates` bounds the total updates in the page. The result never
    /// splits a batch, borrows ring storage without copying or allocation, and
    /// retains exact command boundaries across physical ring wrap.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayError`] for a zero page limit, an unavailable
    /// or non-boundary cursor, a cursor beyond the published boundary, or when
    /// the next complete batch alone exceeds `maximum_updates`.
    pub(crate) fn replay_auction_batches_after(
        &self,
        after_sequence: u64,
        maximum_updates: usize,
    ) -> Result<CallAuctionMarketDataReplay<'_>, MarketDataReplayError> {
        if maximum_updates == 0 {
            return Err(MarketDataReplayError::ZeroRequestLimit);
        }
        if after_sequence > self.latest_sequence {
            return Err(MarketDataReplayError::CursorAhead {
                latest_sequence: self.latest_sequence,
                requested_after: after_sequence,
            });
        }
        if after_sequence == self.latest_sequence {
            return Ok(CallAuctionMarketDataReplay::empty(
                &self.slots,
                self.next_slot,
            ));
        }
        let requested_sequence = after_sequence
            .checked_add(1)
            .ok_or(MarketDataReplayError::ArithmeticOverflow)?;
        let earliest = self
            .earliest_sequence()
            .ok_or(MarketDataReplayError::Unavailable {
                earliest_available: None,
                requested_sequence,
            })?;
        if requested_sequence < earliest {
            return Err(MarketDataReplayError::Unavailable {
                earliest_available: self.earliest_complete_batch_sequence(),
                requested_sequence,
            });
        }
        if after_sequence >= earliest {
            let cursor =
                self.retained_slot(after_sequence)
                    .ok_or(MarketDataReplayError::CorruptState(
                        "auction replay cursor is absent from the retained range",
                    ))?;
            if !cursor.ends_batch {
                return Err(MarketDataReplayError::CursorInsideBatch {
                    requested_after: after_sequence,
                });
            }
        }
        let offset = usize::try_from(requested_sequence - earliest)
            .map_err(|_| MarketDataReplayError::CorruptState("replay offset exceeds usize"))?;
        if offset >= self.retained {
            return Err(MarketDataReplayError::CorruptState(
                "auction replay cursor is absent from the retained range",
            ));
        }
        let first_slot = ring_advance(self.earliest_slot(), offset, self.slots.len());
        if !self.slots[first_slot]
            .ok_or(MarketDataReplayError::CorruptState(
                "auction replay range begins at an empty slot",
            ))?
            .starts_batch
        {
            return Err(MarketDataReplayError::Unavailable {
                earliest_available: self.earliest_complete_batch_sequence(),
                requested_sequence,
            });
        }

        let available = self.retained - offset;
        let (returned_updates, returned_batches) =
            self.auction_page_shape(first_slot, available, maximum_updates)?;
        let returned_minus_one = u64::try_from(returned_updates - 1)
            .map_err(|_| MarketDataReplayError::ArithmeticOverflow)?;
        let last_sequence = requested_sequence
            .checked_add(returned_minus_one)
            .ok_or(MarketDataReplayError::ArithmeticOverflow)?;
        Ok(CallAuctionMarketDataReplay {
            slots: &self.slots,
            next_slot: first_slot,
            remaining_updates: returned_updates,
            remaining_batches: returned_batches,
            first_sequence: Some(requested_sequence),
            last_sequence: Some(last_sequence),
            has_more: returned_updates < available,
        })
    }

    fn auction_page_shape(
        &self,
        first_slot: usize,
        available: usize,
        maximum_updates: usize,
    ) -> Result<(usize, usize), MarketDataReplayError> {
        let mut scan_slot = first_slot;
        let mut returned_updates = 0;
        let mut returned_batches = 0;
        while returned_updates < available {
            let first = self.slots[scan_slot].ok_or(MarketDataReplayError::CorruptState(
                "auction replay batch begins at an empty slot",
            ))?;
            if !first.starts_batch {
                return Err(MarketDataReplayError::CorruptState(
                    "auction replay page encounters a missing batch start",
                ));
            }
            let mut batch_updates = 0;
            loop {
                let entry = self.slots[scan_slot].ok_or(MarketDataReplayError::CorruptState(
                    "auction replay batch contains an empty slot",
                ))?;
                batch_updates += 1;
                scan_slot = ring_advance(scan_slot, 1, self.slots.len());
                if entry.ends_batch {
                    break;
                }
                if returned_updates + batch_updates >= available {
                    return Err(MarketDataReplayError::CorruptState(
                        "auction replay retained suffix ends inside a batch",
                    ));
                }
            }
            if batch_updates > maximum_updates - returned_updates {
                if returned_updates == 0 {
                    return Err(MarketDataReplayError::BatchExceedsRequestLimit {
                        maximum: maximum_updates,
                        required: batch_updates,
                    });
                }
                break;
            }
            returned_updates += batch_updates;
            returned_batches += 1;
        }
        Ok((returned_updates, returned_batches))
    }
}

impl<U: SequencedReplayUpdate> SequencedMarketDataReplayBuffer<U> {
    /// Audits ring allocation, occupancy, identity, and contiguous sequences.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataReplayError::CorruptState`] on the first internal
    /// contradiction.
    pub fn validate(&self) -> Result<(), MarketDataReplayError> {
        let maximum = self.slots.len();
        if maximum == 0
            || self.slots.capacity() < maximum
            || self.next_slot >= maximum
            || self.retained > maximum
        {
            return Err(MarketDataReplayError::CorruptState(
                "market-data replay ring contradicts its allocation metadata",
            ));
        }
        if self.slots.iter().filter(|slot| slot.is_some()).count() != self.retained {
            return Err(MarketDataReplayError::CorruptState(
                "market-data replay ring occupancy is inconsistent",
            ));
        }
        let Some(mut expected) = self.earliest_sequence() else {
            return if self.retained == 0 && self.next_slot == 0 {
                Ok(())
            } else {
                Err(MarketDataReplayError::CorruptState(
                    "empty market-data replay ring has noncanonical topology",
                ))
            };
        };
        let mut slot = self.earliest_slot();
        let mut previous_ends_batch = None;
        for _ in 0..self.retained {
            let update = self.slots[slot].ok_or(MarketDataReplayError::CorruptState(
                "market-data replay ring contains an occupied-range hole",
            ))?;
            if update.update.replay_instrument_id() != self.instrument_id
                || update.update.replay_instrument_version() != self.instrument_version
                || update.update.replay_sequence() != expected
            {
                return Err(MarketDataReplayError::CorruptState(
                    "market-data replay ring identity or sequence is inconsistent",
                ));
            }
            if previous_ends_batch.is_some_and(|ended| ended != update.starts_batch) {
                return Err(MarketDataReplayError::CorruptState(
                    "market-data replay batch boundaries are inconsistent",
                ));
            }
            previous_ends_batch = Some(update.ends_batch);
            slot = ring_advance(slot, 1, maximum);
            if expected != self.latest_sequence {
                expected = expected
                    .checked_add(1)
                    .ok_or(MarketDataReplayError::CorruptState(
                        "market-data replay retained sequence overflowed",
                    ))?;
            }
        }
        if expected != self.latest_sequence
            || slot != self.next_slot
            || previous_ends_batch != Some(true)
        {
            return Err(MarketDataReplayError::CorruptState(
                "market-data replay boundary differs from retained content",
            ));
        }
        Ok(())
    }

    fn push_updates(&mut self, updates: &[U]) -> Result<usize, MarketDataReplayError> {
        let mut previous_sequence: Option<u64> = None;
        for &update in updates {
            if update.replay_instrument_id() != self.instrument_id {
                return Err(MarketDataReplayError::WrongInstrument);
            }
            if update.replay_instrument_version() != self.instrument_version {
                return Err(MarketDataReplayError::WrongInstrumentVersion);
            }
            if let Some(previous) = previous_sequence {
                let expected = previous
                    .checked_add(1)
                    .ok_or(MarketDataReplayError::ArithmeticOverflow)?;
                if update.replay_sequence() != expected {
                    return Err(MarketDataReplayError::SequenceGap {
                        expected,
                        actual: update.replay_sequence(),
                    });
                }
            }
            previous_sequence = Some(update.replay_sequence());
        }
        if updates.len() > self.slots.len() {
            return Err(MarketDataReplayError::BatchExceedsCapacity {
                maximum: self.slots.len(),
                attempted: updates.len(),
            });
        }

        let mut new_start = updates.len();
        for (index, &update) in updates.iter().enumerate() {
            let expected_slot = ReplaySlot {
                update,
                starts_batch: !U::PRESERVE_BATCH_BOUNDARIES || index == 0,
                ends_batch: !U::PRESERVE_BATCH_BOUNDARIES || index + 1 == updates.len(),
            };
            if update.replay_sequence() <= self.latest_sequence {
                let existing = self.retained_slot(update.replay_sequence()).ok_or(
                    MarketDataReplayError::Unavailable {
                        earliest_available: self.earliest_sequence(),
                        requested_sequence: update.replay_sequence(),
                    },
                )?;
                if existing != expected_slot {
                    return Err(MarketDataReplayError::SequenceCollision {
                        sequence: update.replay_sequence(),
                    });
                }
                continue;
            }
            let expected = self
                .latest_sequence
                .checked_add(1)
                .ok_or(MarketDataReplayError::ArithmeticOverflow)?;
            if update.replay_sequence() != expected {
                return Err(MarketDataReplayError::SequenceGap {
                    expected,
                    actual: update.replay_sequence(),
                });
            }
            new_start = index;
            break;
        }

        for (index, &update) in updates.iter().enumerate().skip(new_start) {
            self.slots[self.next_slot] = Some(ReplaySlot {
                update,
                starts_batch: !U::PRESERVE_BATCH_BOUNDARIES || index == 0,
                ends_batch: !U::PRESERVE_BATCH_BOUNDARIES || index + 1 == updates.len(),
            });
            self.next_slot = ring_advance(self.next_slot, 1, self.slots.len());
            if self.retained < self.slots.len() {
                self.retained += 1;
            }
            self.latest_sequence = update.replay_sequence();
        }
        Ok(updates.len() - new_start)
    }

    fn retained_slot(&self, sequence: u64) -> Option<ReplaySlot<U>> {
        let earliest = self.earliest_sequence()?;
        if sequence < earliest || sequence > self.latest_sequence {
            return None;
        }
        let offset = usize::try_from(sequence - earliest).ok()?;
        if offset >= self.retained {
            return None;
        }
        self.slots[ring_advance(self.earliest_slot(), offset, self.slots.len())]
    }

    fn earliest_complete_batch_sequence(&self) -> Option<u64> {
        let mut slot = self.earliest_slot();
        for _ in 0..self.retained {
            let entry = self.slots[slot]?;
            if entry.starts_batch {
                return Some(entry.update.replay_sequence());
            }
            slot = ring_advance(slot, 1, self.slots.len());
        }
        None
    }

    fn earliest_slot(&self) -> usize {
        if self.next_slot >= self.retained {
            self.next_slot - self.retained
        } else {
            self.slots.len() - (self.retained - self.next_slot)
        }
    }
}

/// Zero-copy, source-ordered page of complete call-auction publication batches.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionMarketDataReplay<'a> {
    slots: &'a [Option<ReplaySlot<CallAuctionMarketDataUpdate>>],
    next_slot: usize,
    remaining_updates: usize,
    remaining_batches: usize,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    has_more: bool,
}

impl CallAuctionMarketDataReplay<'_> {
    /// Returns the original page's first sequence, if it is non-empty.
    #[must_use]
    pub const fn first_sequence(&self) -> Option<u64> {
        self.first_sequence
    }

    /// Returns the original page's final sequence, if it is non-empty.
    #[must_use]
    pub const fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    /// Returns whether a subsequent complete batch remains at query time.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    const fn empty(
        slots: &[Option<ReplaySlot<CallAuctionMarketDataUpdate>>],
        next_slot: usize,
    ) -> CallAuctionMarketDataReplay<'_> {
        CallAuctionMarketDataReplay {
            slots,
            next_slot,
            remaining_updates: 0,
            remaining_batches: 0,
            first_sequence: None,
            last_sequence: None,
            has_more: false,
        }
    }
}

impl<'a> Iterator for CallAuctionMarketDataReplay<'a> {
    type Item = CallAuctionMarketDataReplayBatch<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_batches == 0 {
            return None;
        }
        let first_slot = self.next_slot;
        let first = self.slots[first_slot]
            .expect("validated auction replay batch must begin at an occupied slot");
        debug_assert!(first.starts_batch);
        let mut updates = 0;
        let last_sequence = loop {
            let entry = self.slots[self.next_slot]
                .expect("validated auction replay batch must contain every update");
            updates += 1;
            self.next_slot = ring_advance(self.next_slot, 1, self.slots.len());
            if entry.ends_batch {
                break entry.update.sequence();
            }
        };
        self.remaining_updates -= updates;
        self.remaining_batches -= 1;
        Some(CallAuctionMarketDataReplayBatch {
            slots: self.slots,
            first_slot,
            next_slot: first_slot,
            total_updates: updates,
            remaining: updates,
            first_sequence: first.update.sequence(),
            last_sequence,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining_batches, Some(self.remaining_batches))
    }
}

impl ExactSizeIterator for CallAuctionMarketDataReplay<'_> {
    fn len(&self) -> usize {
        self.remaining_batches
    }
}

impl FusedIterator for CallAuctionMarketDataReplay<'_> {}

/// Zero-copy view over one complete retained call-auction publication batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionMarketDataReplayBatch<'a> {
    slots: &'a [Option<ReplaySlot<CallAuctionMarketDataUpdate>>],
    first_slot: usize,
    next_slot: usize,
    total_updates: usize,
    remaining: usize,
    first_sequence: u64,
    last_sequence: u64,
}

impl CallAuctionMarketDataReplayBatch<'_> {
    /// Returns the batch's first source sequence.
    #[must_use]
    pub const fn first_sequence(&self) -> u64 {
        self.first_sequence
    }

    /// Returns the batch's final source sequence.
    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    pub(crate) const fn complete_iter(&self) -> Self {
        Self {
            slots: self.slots,
            first_slot: self.first_slot,
            next_slot: self.first_slot,
            total_updates: self.total_updates,
            remaining: self.total_updates,
            first_sequence: self.first_sequence,
            last_sequence: self.last_sequence,
        }
    }
}

impl Iterator for CallAuctionMarketDataReplayBatch<'_> {
    type Item = CallAuctionMarketDataUpdate;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let entry = self.slots[self.next_slot]
            .expect("validated auction replay batch must contain every update");
        self.next_slot = ring_advance(self.next_slot, 1, self.slots.len());
        self.remaining -= 1;
        Some(entry.update)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for CallAuctionMarketDataReplayBatch<'_> {
    fn len(&self) -> usize {
        self.remaining
    }
}

impl FusedIterator for CallAuctionMarketDataReplayBatch<'_> {}

/// Zero-copy source-ordered view over one bounded replay page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataReplay<'a> {
    slots: &'a [Option<ReplaySlot<MarketDataUpdate>>],
    next_slot: usize,
    remaining: usize,
    first_sequence: Option<u64>,
    last_sequence: Option<u64>,
    has_more: bool,
}

impl MarketDataReplay<'_> {
    /// Returns the original page's first sequence, if it is non-empty.
    #[must_use]
    pub const fn first_sequence(&self) -> Option<u64> {
        self.first_sequence
    }

    /// Returns the original page's final sequence, if it is non-empty.
    #[must_use]
    pub const fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }

    /// Returns whether a subsequent page remains at query time.
    #[must_use]
    pub const fn has_more(&self) -> bool {
        self.has_more
    }

    const fn empty(
        slots: &[Option<ReplaySlot<MarketDataUpdate>>],
        next_slot: usize,
    ) -> MarketDataReplay<'_> {
        MarketDataReplay {
            slots,
            next_slot,
            remaining: 0,
            first_sequence: None,
            last_sequence: None,
            has_more: false,
        }
    }
}

impl Iterator for MarketDataReplay<'_> {
    type Item = MarketDataUpdate;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let update = self.slots[self.next_slot]
            .expect("validated market-data replay range must contain every update");
        self.next_slot = ring_advance(self.next_slot, 1, self.slots.len());
        self.remaining -= 1;
        Some(update.update)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for MarketDataReplay<'_> {
    fn len(&self) -> usize {
        self.remaining
    }
}

impl FusedIterator for MarketDataReplay<'_> {}

fn ring_advance(index: usize, distance: usize, capacity: usize) -> usize {
    debug_assert!(index < capacity);
    debug_assert!(distance <= capacity);
    let until_end = capacity - index;
    if distance < until_end {
        index + distance
    } else {
        distance - until_end
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::TimestampNs;

    use super::*;
    use crate::market_data::MarketDataKind;

    fn update(sequence: u64) -> MarketDataUpdate {
        MarketDataUpdate::from_parts(
            InstrumentId::new(1).unwrap(),
            InstrumentVersion::new(1).unwrap(),
            sequence,
            TimestampNs::from_unix_nanos(sequence),
            MarketDataKind::NoBookChange,
        )
        .unwrap()
    }

    #[test]
    fn repeated_wrap_keeps_the_exact_allocation_and_final_suffix() {
        let mut replay = MarketDataReplayBuffer::try_new(
            InstrumentId::new(1).unwrap(),
            InstrumentVersion::new(1).unwrap(),
            0,
            3,
        )
        .unwrap();
        let allocation = replay.status().allocation_capacity;
        for sequence in 1..=10_000 {
            assert_eq!(replay.push(update(sequence)).unwrap(), 1);
            assert_eq!(replay.status().allocation_capacity, allocation);
        }
        assert_eq!(replay.earliest_sequence(), Some(9_998));
        assert_eq!(replay.latest_sequence(), 10_000);
        assert_eq!(
            replay
                .replay_after(9_997, 3)
                .unwrap()
                .map(MarketDataUpdate::sequence)
                .collect::<Vec<_>>(),
            vec![9_998, 9_999, 10_000]
        );
        replay.validate().unwrap();
    }

    #[test]
    fn final_u64_sequence_is_retainable_replayable_and_idempotent() {
        let mut replay = MarketDataReplayBuffer::try_new(
            InstrumentId::new(1).unwrap(),
            InstrumentVersion::new(1).unwrap(),
            u64::MAX - 1,
            1,
        )
        .unwrap();
        assert_eq!(replay.push(update(u64::MAX)).unwrap(), 1);
        assert_eq!(replay.push(update(u64::MAX)).unwrap(), 0);
        assert_eq!(
            replay
                .replay_after(u64::MAX - 1, 1)
                .unwrap()
                .next()
                .map(MarketDataUpdate::sequence),
            Some(u64::MAX)
        );
        assert_eq!(replay.replay_after(u64::MAX, 1).unwrap().len(), 0);
        replay.validate().unwrap();
    }

    #[test]
    fn empty_ring_audit_rejects_a_noncanonical_write_index() {
        let mut replay = MarketDataReplayBuffer::try_new(
            InstrumentId::new(1).unwrap(),
            InstrumentVersion::new(1).unwrap(),
            7,
            2,
        )
        .unwrap();
        replay.inner.next_slot = 1;
        assert_eq!(
            replay.validate(),
            Err(MarketDataReplayError::CorruptState(
                "empty market-data replay ring has noncanonical topology"
            ))
        );
    }
}
