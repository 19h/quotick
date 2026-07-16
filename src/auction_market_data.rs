//! Deterministic, anonymized call-auction depth, phase, and execution publication.
//!
//! Each private auction-engine event maps to one public update at the identical
//! event sequence. Absolute market/limit aggregates permit deterministic gap
//! recovery without exposing order or account identity. Full-depth snapshots
//! establish an atomic replica repair boundary.
//!
//! Publisher order/uncross mirrors, ordered limit depth, replica active/standby
//! depth, and replica batch-capacity scratch are finitely bounded and completely
//! reserved before usable state exists. Incremental state mutation never grows,
//! shrinks, or rehashes authoritative storage.

use std::fmt;
use std::hash::Hash;

use crate::auction::{AuctionClearing, AuctionOrderConstraint, AuctionPriorityClass};
use crate::auction_book::{
    CallAuctionBookLimits, CallAuctionCancellation, CallAuctionLevelSnapshot,
    CallAuctionOrderSnapshot, CallAuctionTrade,
};
use crate::auction_engine::{
    CallAuctionCommand, CallAuctionCommandOutcome, CallAuctionEngine, CallAuctionEngineLimits,
    CallAuctionEvent, CallAuctionEventKind, CallAuctionExecutionReport, CallAuctionPhase,
    CallAuctionPhaseSnapshot,
};
use crate::bounded_hash::{BoundedHashError, BoundedHashMap};
use crate::domain::{
    AccountId, AuctionId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs, TradeId,
};
use crate::indexed_avl::IndexedAvlMap;
use crate::instrument::InstrumentDefinition;
pub use crate::market_data::{CallAuctionMarketDataReplay, CallAuctionMarketDataReplayBatch};
use crate::matching::MassCancelScope;

/// Failure before a call-auction replay buffer owns usable storage.
pub type CallAuctionMarketDataReplayConstructionError =
    crate::market_data::MarketDataReplayConstructionError;

/// Deterministic call-auction replay admission or range-query failure.
pub type CallAuctionMarketDataReplayError = crate::market_data::MarketDataReplayError;

/// Allocation and retained-range telemetry for one call-auction replay ring.
pub type CallAuctionMarketDataReplayStatus = crate::market_data::MarketDataReplayStatus;

type AuctionDepthMap = IndexedAvlMap<Price, Aggregate>;

/// One finite call-auction market-data state or preparation resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionMarketDataResource {
    /// Active order identities mirrored by a publisher.
    TrackedOrders,
    /// Occupied bid limit prices.
    BidPriceLevels,
    /// Occupied ask limit prices.
    AskPriceLevels,
    /// Updates emitted or consumed by one command batch.
    BatchUpdates,
    /// Source-order quantities retained while validating one uncross.
    UncrossSourceOrders,
    /// Unique limit-level identities used for nonmutating replica preflight.
    BatchLevelIdentities,
}

impl fmt::Display for CallAuctionMarketDataResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TrackedOrders => "tracked orders",
            Self::BidPriceLevels => "bid price levels",
            Self::AskPriceLevels => "ask price levels",
            Self::BatchUpdates => "batch updates",
            Self::UncrossSourceOrders => "uncross source orders",
            Self::BatchLevelIdentities => "batch level identities",
        })
    }
}

/// Raw construction values for bounded call-auction public state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionMarketDataLimitsSpec {
    /// Maximum active orders mirrored by a publisher.
    pub max_active_orders: usize,
    /// Maximum occupied limit prices independently on each side.
    pub max_price_levels_per_side: usize,
    /// Maximum updates in one auction-command publication batch.
    pub max_batch_updates: usize,
}

impl Default for CallAuctionMarketDataLimitsSpec {
    fn default() -> Self {
        Self {
            max_active_orders: CallAuctionBookLimits::DEFAULT_MAX_ACTIVE_ORDERS,
            max_price_levels_per_side: CallAuctionBookLimits::DEFAULT_MAX_PRICE_LEVELS_PER_SIDE,
            max_batch_updates: CallAuctionBookLimits::DEFAULT_MAX_ACTIVE_ORDERS * 2 + 1,
        }
    }
}

/// Invalid finite call-auction market-data policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionMarketDataLimitsError {
    /// Active-order maximum is zero.
    ZeroActiveOrders,
    /// Per-side limit-price maximum is zero.
    ZeroPriceLevels,
    /// Per-batch update maximum is zero.
    ZeroBatchUpdates,
    /// More prices per side than total active orders were configured.
    PriceLevelsExceedActiveOrders,
    /// One maximum-size uncross report cannot fit in a publication batch.
    BatchUpdatesBelowUncrossMaximum,
    /// A derived maximum cannot be represented by `usize`.
    DerivedBoundOverflow,
}

impl fmt::Display for CallAuctionMarketDataLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroActiveOrders => "call-auction market-data active-order limit is zero",
            Self::ZeroPriceLevels => "call-auction market-data per-side price-level limit is zero",
            Self::ZeroBatchUpdates => "call-auction market-data batch-update limit is zero",
            Self::PriceLevelsExceedActiveOrders => {
                "call-auction market-data price-level limit exceeds its active-order limit"
            }
            Self::BatchUpdatesBelowUncrossMaximum => {
                "call-auction market-data batch limit cannot hold a maximum-size uncross"
            }
            Self::DerivedBoundOverflow => {
                "call-auction market-data derived resource bound overflows usize"
            }
        })
    }
}

impl std::error::Error for CallAuctionMarketDataLimitsError {}

/// Validated immutable finite resource policy for call-auction public state.
#[allow(
    clippy::struct_field_names,
    reason = "the max prefix distinguishes configured limits from occupancy"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionMarketDataLimits {
    max_active_orders: usize,
    max_price_levels_per_side: usize,
    max_batch_updates: usize,
}

impl CallAuctionMarketDataLimits {
    /// Validates one call-auction public-state resource policy.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataLimitsError`] for a zero, contradictory,
    /// or arithmetically unrepresentable envelope.
    pub const fn new(
        spec: CallAuctionMarketDataLimitsSpec,
    ) -> Result<Self, CallAuctionMarketDataLimitsError> {
        if spec.max_active_orders == 0 {
            return Err(CallAuctionMarketDataLimitsError::ZeroActiveOrders);
        }
        if spec.max_price_levels_per_side == 0 {
            return Err(CallAuctionMarketDataLimitsError::ZeroPriceLevels);
        }
        if spec.max_batch_updates == 0 {
            return Err(CallAuctionMarketDataLimitsError::ZeroBatchUpdates);
        }
        if spec.max_price_levels_per_side > spec.max_active_orders {
            return Err(CallAuctionMarketDataLimitsError::PriceLevelsExceedActiveOrders);
        }
        let Some(maximum_uncross_updates) = spec.max_active_orders.checked_mul(2) else {
            return Err(CallAuctionMarketDataLimitsError::DerivedBoundOverflow);
        };
        let Some(maximum_uncross_updates) = maximum_uncross_updates.checked_add(1) else {
            return Err(CallAuctionMarketDataLimitsError::DerivedBoundOverflow);
        };
        if spec.max_batch_updates < maximum_uncross_updates {
            return Err(CallAuctionMarketDataLimitsError::BatchUpdatesBelowUncrossMaximum);
        }
        Ok(Self {
            max_active_orders: spec.max_active_orders,
            max_price_levels_per_side: spec.max_price_levels_per_side,
            max_batch_updates: spec.max_batch_updates,
        })
    }

    /// Derives the exact public-state envelope of one sequenced auction shard.
    #[must_use]
    pub const fn from_engine(limits: CallAuctionEngineLimits) -> Self {
        Self {
            max_active_orders: limits.book().max_active_orders(),
            max_price_levels_per_side: limits.book().max_price_levels_per_side(),
            max_batch_updates: limits.max_report_events(),
        }
    }

    /// Maximum publisher-mirrored active orders.
    #[must_use]
    pub const fn max_active_orders(self) -> usize {
        self.max_active_orders
    }

    /// Maximum occupied limit prices independently on each side.
    #[must_use]
    pub const fn max_price_levels_per_side(self) -> usize {
        self.max_price_levels_per_side
    }

    /// Maximum updates in one command batch.
    #[must_use]
    pub const fn max_batch_updates(self) -> usize {
        self.max_batch_updates
    }

    /// Returns the corresponding raw specification.
    #[must_use]
    pub const fn spec(self) -> CallAuctionMarketDataLimitsSpec {
        CallAuctionMarketDataLimitsSpec {
            max_active_orders: self.max_active_orders,
            max_price_levels_per_side: self.max_price_levels_per_side,
            max_batch_updates: self.max_batch_updates,
        }
    }
}

impl Default for CallAuctionMarketDataLimits {
    fn default() -> Self {
        Self::new(CallAuctionMarketDataLimitsSpec::default())
            .expect("default call-auction market-data limits must be valid")
    }
}

impl TryFrom<CallAuctionMarketDataLimitsSpec> for CallAuctionMarketDataLimits {
    type Error = CallAuctionMarketDataLimitsError;

    fn try_from(spec: CallAuctionMarketDataLimitsSpec) -> Result<Self, Self::Error> {
        Self::new(spec)
    }
}

/// Failure before a call-auction publisher or replica owns usable state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionMarketDataConstructionError {
    /// The selected finite policy is invalid.
    InvalidLimits(CallAuctionMarketDataLimitsError),
    /// A publisher envelope is smaller than its authoritative engine.
    LimitsBelowSource {
        /// Insufficient resource.
        resource: CallAuctionMarketDataResource,
        /// Selected maximum.
        selected: usize,
        /// Required source maximum.
        required: usize,
    },
    /// Complete constructor-owned storage could not be reserved.
    ReservationFailed {
        /// Resource whose layout could not be represented or allocated.
        resource: CallAuctionMarketDataResource,
        /// Requested semantic maximum.
        maximum: usize,
    },
    /// The source engine failed bootstrap validation.
    Source(CallAuctionMarketDataError),
}

impl fmt::Display for CallAuctionMarketDataConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits(error) => error.fmt(formatter),
            Self::LimitsBelowSource {
                resource,
                selected,
                required,
            } => write!(
                formatter,
                "call-auction market-data {resource} limit {selected} is below source requirement {required}"
            ),
            Self::ReservationFailed { resource, maximum } => write!(
                formatter,
                "failed to reserve call-auction market-data {resource} through {maximum} entries"
            ),
            Self::Source(error) => write!(
                formatter,
                "call-auction market-data source is invalid: {error}"
            ),
        }
    }
}

impl std::error::Error for CallAuctionMarketDataConstructionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidLimits(error) => Some(error),
            Self::Source(error) => Some(error),
            Self::LimitsBelowSource { .. } | Self::ReservationFailed { .. } => None,
        }
    }
}

impl From<CallAuctionMarketDataLimitsError> for CallAuctionMarketDataConstructionError {
    fn from(error: CallAuctionMarketDataLimitsError) -> Self {
        Self::InvalidLimits(error)
    }
}

/// Publisher/replica bounded hash layout identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionMarketDataHashIndex {
    /// Publisher active-order mirror.
    TrackedOrders,
    /// Publisher uncross-source scratch.
    PublisherUncrossSources,
    /// Replica batch level-capacity scratch.
    ReplicaBatchLevels,
}

/// Allocation and occupancy telemetry for one bounded hash layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionMarketDataHashIndexStatus {
    /// Configured semantic maximum.
    pub configured_entries: usize,
    /// Dense-entry allocation capacity.
    pub allocated_entries: usize,
    /// Initialized open-addressed bucket count.
    pub initialized_buckets: usize,
    /// Current occupied entries.
    pub occupied_entries: usize,
}

/// Allocation, high-water, occupancy, and reuse telemetry for one limit-depth arena.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionMarketDataArenaStatus {
    /// Configured semantic slot maximum.
    pub configured_slots: usize,
    /// Allocated slot capacity.
    pub allocated_slots: usize,
    /// Slots initialized at least once.
    pub initialized_slots: usize,
    /// Currently occupied slots.
    pub occupied_slots: usize,
    /// Initialized vacant slots available for immediate reuse.
    pub reusable_slots: usize,
}

/// Absolute public state for one market or limit constraint on one side.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionMarketDataLevel {
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: u128,
    order_count: u64,
}

impl CallAuctionMarketDataLevel {
    pub(crate) const fn from_parts(
        side: Side,
        constraint: AuctionOrderConstraint,
        quantity: u128,
        order_count: u64,
    ) -> Result<Self, CallAuctionMarketDataError> {
        if (quantity == 0) != (order_count == 0) {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction level quantity and order count must be zero or non-zero together",
            ));
        }
        Ok(Self {
            side,
            constraint,
            quantity,
            order_count,
        })
    }

    /// Returns the book side.
    #[must_use]
    pub const fn side(self) -> Side {
        self.side
    }

    /// Returns the market or limit constraint represented by this aggregate.
    #[must_use]
    pub const fn constraint(self) -> AuctionOrderConstraint {
        self.constraint
    }

    /// Returns aggregate active quantity in instrument-defined lots.
    #[must_use]
    pub const fn quantity(self) -> u128 {
        self.quantity
    }

    /// Returns the number of active orders represented by the aggregate.
    #[must_use]
    pub const fn order_count(self) -> u64 {
        self.order_count
    }
}

/// Public reason for one aggregate collection-book transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionBookChangeReason {
    /// New interest was accepted during collection.
    Accepted,
    /// Active interest was removed by its owner.
    UserCancelled,
    /// Active interest was removed before accepting its new identity.
    Replaced,
    /// Active interest was removed by an account-scoped mass cancel.
    MassCancelled,
    /// Active quantity was reduced without changing its order count.
    Amended,
    /// Unexecuted interest was removed by the final uncross policy.
    UncrossRemainder,
}

/// An anonymized execution at the common auction clearing price.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionTradePrint {
    auction_id: AuctionId,
    trade_id: TradeId,
    price: Price,
    quantity: Quantity,
}

impl CallAuctionTradePrint {
    pub(crate) const fn from_parts(
        auction_id: AuctionId,
        trade_id: TradeId,
        price: Price,
        quantity: Quantity,
    ) -> Self {
        Self {
            auction_id,
            trade_id,
            price,
            quantity,
        }
    }

    /// Returns the auction cycle that produced the execution.
    #[must_use]
    pub const fn auction_id(self) -> AuctionId {
        self.auction_id
    }

    /// Returns the book-local monotonic trade identifier.
    #[must_use]
    pub const fn trade_id(self) -> TradeId {
        self.trade_id
    }

    /// Returns the common clearing price in instrument-defined quanta.
    #[must_use]
    pub const fn price(self) -> Price {
        self.price
    }

    /// Returns executed quantity in instrument-defined lots.
    #[must_use]
    pub const fn quantity(self) -> Quantity {
        self.quantity
    }
}

/// One public update corresponding to exactly one auction-engine event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionMarketDataKind {
    /// The source sequence advanced without a public state change.
    NoPublicChange,
    /// One market or limit aggregate changed absolutely.
    Book {
        /// Why collection state changed.
        reason: CallAuctionBookChangeReason,
        /// Positive quantity added or removed by this source event.
        quantity: Quantity,
        /// Absolute state after the transition; zero means delete.
        level: CallAuctionMarketDataLevel,
    },
    /// One pair executed and both affected side aggregates changed atomically.
    Trade {
        /// Anonymized execution.
        print: CallAuctionTradePrint,
        /// Absolute buy-side aggregate after this pair.
        buy: CallAuctionMarketDataLevel,
        /// Absolute sell-side aggregate after this pair.
        sell: CallAuctionMarketDataLevel,
    },
    /// Effective phase authority changed.
    PhaseChanged {
        /// Controlled cycle.
        auction_id: AuctionId,
        /// Previous phase.
        previous: CallAuctionPhase,
        /// Current phase.
        current: CallAuctionPhase,
        /// Newly committed phase revision.
        revision: u64,
    },
    /// One final uncross closed its cycle.
    UncrossCompleted {
        /// Closed cycle.
        auction_id: AuctionId,
        /// Final aggregate clearing state.
        clearing: AuctionClearing,
        /// Number of anonymized pair prints.
        trade_count: u64,
        /// Number of policy-driven remainder removals.
        cancellation_count: u64,
        /// Authoritative collection-book revision after consumption.
        book_revision: u64,
        /// Authoritative phase revision after closure.
        phase_revision: u64,
    },
    /// One account-scoped mass cancellation completed.
    MassCancelCompleted {
        /// Number of anonymized order removals in this command batch.
        cancelled_order_count: u64,
        /// Sum of removed quantity in instrument-defined lots.
        cancelled_quantity_lots: u128,
        /// Authoritative collection-book revision after the command.
        book_revision: u64,
    },
}

/// One source-sequenced public call-auction update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionMarketDataUpdate {
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    sequence: u64,
    occurred_at: TimestampNs,
    kind: CallAuctionMarketDataKind,
}

impl CallAuctionMarketDataUpdate {
    pub(crate) fn from_parts(
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        sequence: u64,
        occurred_at: TimestampNs,
        kind: CallAuctionMarketDataKind,
    ) -> Result<Self, CallAuctionMarketDataError> {
        if sequence == 0 {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction market-data sequence must be non-zero",
            ));
        }
        validate_update_kind(sequence, kind)?;
        Ok(Self {
            instrument_id,
            instrument_version,
            sequence,
            occurred_at,
            kind,
        })
    }

    /// Returns the instrument identifier.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the immutable instrument version.
    #[must_use]
    pub const fn instrument_version(self) -> InstrumentVersion {
        self.instrument_version
    }

    /// Returns the auction-engine event sequence.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    /// Returns the source event timestamp.
    #[must_use]
    pub const fn occurred_at(self) -> TimestampNs {
        self.occurred_at
    }

    /// Returns the public payload.
    #[must_use]
    pub const fn kind(self) -> CallAuctionMarketDataKind {
        self.kind
    }
}

/// A contiguous public result for one auction command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionMarketDataBatch {
    updates: Vec<CallAuctionMarketDataUpdate>,
    replayed: bool,
}

impl CallAuctionMarketDataBatch {
    /// Returns updates in ascending contiguous source-event order.
    #[must_use]
    pub fn updates(&self) -> &[CallAuctionMarketDataUpdate] {
        &self.updates
    }

    /// Returns whether an exact source-command retry was suppressed.
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }

    /// Returns the first source sequence, if present.
    #[must_use]
    pub fn first_sequence(&self) -> Option<u64> {
        self.updates.first().map(|update| update.sequence)
    }

    /// Returns the final source sequence, if present.
    #[must_use]
    pub fn last_sequence(&self) -> Option<u64> {
        self.updates.last().map(|update| update.sequence)
    }
}

/// Fixed-capacity, batch-preserving retransmission storage for one auction.
#[derive(Debug)]
pub struct CallAuctionMarketDataReplayBuffer {
    inner: crate::market_data::SequencedMarketDataReplayBuffer<CallAuctionMarketDataUpdate>,
}

impl CallAuctionMarketDataReplayBuffer {
    /// Constructs an empty replay window after an already-published boundary.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataReplayConstructionError`] before state
    /// exists when `maximum_updates` is zero or ring storage cannot be reserved.
    pub fn try_new(
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        initial_sequence: u64,
        maximum_updates: usize,
    ) -> Result<Self, CallAuctionMarketDataReplayConstructionError> {
        Ok(Self {
            inner: crate::market_data::SequencedMarketDataReplayBuffer::try_new(
                instrument_id,
                instrument_version,
                initial_sequence,
                maximum_updates,
            )?,
        })
    }

    /// Returns this ring's immutable identity and allocation telemetry.
    #[must_use]
    pub fn status(&self) -> CallAuctionMarketDataReplayStatus {
        self.inner.status()
    }

    /// Returns the first retained sequence, including a partial oldest batch.
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

    /// Atomically admits one auction publisher command batch.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataReplayError`] before mutation for invalid
    /// shape, identity drift, capacity, gaps, collisions, or evicted overlap.
    pub fn push_batch(
        &mut self,
        batch: &CallAuctionMarketDataBatch,
    ) -> Result<usize, CallAuctionMarketDataReplayError> {
        self.inner.push_auction_batch(batch)
    }

    /// Returns complete batches strictly after `after_sequence` without copy.
    ///
    /// `maximum_updates` bounds total updates and never splits a batch.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataReplayError`] for an invalid limit,
    /// unavailable or non-boundary cursor, future cursor, or oversized next
    /// batch.
    pub fn replay_batches_after(
        &self,
        after_sequence: u64,
        maximum_updates: usize,
    ) -> Result<CallAuctionMarketDataReplay<'_>, CallAuctionMarketDataReplayError> {
        self.inner
            .replay_auction_batches_after(after_sequence, maximum_updates)
    }

    /// Audits allocation, occupancy, identity, sequences, and batch boundaries.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataReplayError::CorruptState`] on the first
    /// internal contradiction.
    pub fn validate(&self) -> Result<(), CallAuctionMarketDataReplayError> {
        self.inner.validate()
    }
}

/// A complete public call-auction state image at one event boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionMarketDataSnapshot {
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    as_of_sequence: u64,
    command_sequence: u64,
    phase: CallAuctionPhaseSnapshot,
    book_revision: u64,
    last_trade_id: Option<TradeId>,
    market_buy: CallAuctionMarketDataLevel,
    market_sell: CallAuctionMarketDataLevel,
    bids: Vec<CallAuctionMarketDataLevel>,
    asks: Vec<CallAuctionMarketDataLevel>,
}

impl CallAuctionMarketDataSnapshot {
    #[allow(
        clippy::too_many_arguments,
        reason = "the stable recovery image exposes each independently validated component"
    )]
    pub(crate) fn from_parts(
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        as_of_sequence: u64,
        command_sequence: u64,
        phase: CallAuctionPhaseSnapshot,
        book_revision: u64,
        last_trade_id: Option<TradeId>,
        market_buy: CallAuctionMarketDataLevel,
        market_sell: CallAuctionMarketDataLevel,
        bids: Vec<CallAuctionMarketDataLevel>,
        asks: Vec<CallAuctionMarketDataLevel>,
    ) -> Result<Self, CallAuctionMarketDataError> {
        let snapshot = Self {
            instrument_id,
            instrument_version,
            as_of_sequence,
            command_sequence,
            phase,
            book_revision,
            last_trade_id,
            market_buy,
            market_sell,
            bids,
            asks,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Returns the instrument identifier.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the immutable instrument version.
    #[must_use]
    pub const fn instrument_version(&self) -> InstrumentVersion {
        self.instrument_version
    }

    /// Returns the last engine-event sequence reflected by this image.
    #[must_use]
    pub const fn as_of_sequence(&self) -> u64 {
        self.as_of_sequence
    }

    /// Returns the last committed-command sequence reflected by this image.
    #[must_use]
    pub const fn command_sequence(&self) -> u64 {
        self.command_sequence
    }

    /// Returns effective phase and cycle identity.
    #[must_use]
    pub const fn phase(&self) -> CallAuctionPhaseSnapshot {
        self.phase
    }

    /// Returns the authoritative collection-book mutation revision.
    #[must_use]
    pub const fn book_revision(&self) -> u64 {
        self.book_revision
    }

    /// Returns the final trade identifier reflected by this image.
    #[must_use]
    pub const fn last_trade_id(&self) -> Option<TradeId> {
        self.last_trade_id
    }

    /// Returns absolute market-constrained buy interest.
    #[must_use]
    pub const fn market_buy(&self) -> CallAuctionMarketDataLevel {
        self.market_buy
    }

    /// Returns absolute market-constrained sell interest.
    #[must_use]
    pub const fn market_sell(&self) -> CallAuctionMarketDataLevel {
        self.market_sell
    }

    /// Returns limit bids in best-to-worst order.
    #[must_use]
    pub fn bids(&self) -> &[CallAuctionMarketDataLevel] {
        &self.bids
    }

    /// Returns limit asks in best-to-worst order.
    #[must_use]
    pub fn asks(&self) -> &[CallAuctionMarketDataLevel] {
        &self.asks
    }

    fn validate(&self) -> Result<(), CallAuctionMarketDataError> {
        validate_phase_snapshot(self.phase, self.command_sequence)?;
        if self.command_sequence > self.as_of_sequence || self.book_revision > self.command_sequence
        {
            return Err(CallAuctionMarketDataError::InvalidSnapshot(
                "auction snapshot event, command, and book revisions are inconsistent",
            ));
        }
        if self
            .last_trade_id
            .is_some_and(|trade_id| trade_id.get() > self.as_of_sequence)
        {
            return Err(CallAuctionMarketDataError::InvalidSnapshot(
                "auction trade identifier exceeds the event sequence",
            ));
        }
        validate_snapshot_market(Side::Buy, self.market_buy)?;
        validate_snapshot_market(Side::Sell, self.market_sell)?;
        validate_snapshot_side(Side::Buy, &self.bids)?;
        validate_snapshot_side(Side::Sell, &self.asks)
    }
}

/// Publication, validation, or replica-recovery failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionMarketDataError {
    /// A value belongs to another instrument.
    WrongInstrument,
    /// A value belongs to another immutable instrument version.
    WrongInstrumentVersion,
    /// A source sequence was missing, duplicated, or reordered.
    SequenceGap {
        /// Required next sequence.
        expected: u64,
        /// Received sequence.
        actual: u64,
    },
    /// A committed-command sequence was missing, duplicated, or reordered.
    CommandSequenceGap {
        /// Required next sequence.
        expected: u64,
        /// Received sequence.
        actual: u64,
    },
    /// A snapshot predates the replica state it would replace.
    StaleSnapshot {
        /// Current event sequence.
        current: u64,
        /// Snapshot event boundary.
        snapshot: u64,
    },
    /// A full-depth image violated a structural invariant.
    InvalidSnapshot(&'static str),
    /// An incremental update violated a structural invariant.
    InvalidUpdate(&'static str),
    /// A private report contradicted its command or event grammar.
    TraceMismatch(&'static str),
    /// Published aggregate state differed from the authoritative engine.
    SourceDivergence(&'static str),
    /// Checked aggregate or sequence arithmetic overflowed.
    ArithmeticOverflow,
    /// A finite state or batch cardinality would exceed its selected maximum.
    CapacityExceeded {
        /// Exhausted resource.
        resource: CallAuctionMarketDataResource,
        /// Selected maximum.
        maximum: usize,
        /// Cardinality the operation attempted to establish.
        attempted: usize,
    },
    /// Caller-owned batch, snapshot, or depth output could not be reserved.
    PreparationAllocationFailed(CallAuctionMarketDataResource),
    /// A previous partial publication or replica failure requires a snapshot.
    Poisoned,
}

impl fmt::Display for CallAuctionMarketDataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongInstrument => {
                formatter.write_str("auction market data belongs to another instrument")
            }
            Self::WrongInstrumentVersion => {
                formatter.write_str("auction market data belongs to another instrument version")
            }
            Self::SequenceGap { expected, actual } => write!(
                formatter,
                "auction market-data event gap: expected {expected}, received {actual}"
            ),
            Self::CommandSequenceGap { expected, actual } => write!(
                formatter,
                "auction market-data command gap: expected {expected}, received {actual}"
            ),
            Self::StaleSnapshot { current, snapshot } => write!(
                formatter,
                "auction snapshot sequence {snapshot} predates replica sequence {current}"
            ),
            Self::InvalidSnapshot(detail)
            | Self::InvalidUpdate(detail)
            | Self::TraceMismatch(detail)
            | Self::SourceDivergence(detail) => formatter.write_str(detail),
            Self::ArithmeticOverflow => {
                formatter.write_str("auction market-data arithmetic overflow")
            }
            Self::CapacityExceeded {
                resource,
                maximum,
                attempted,
            } => write!(
                formatter,
                "call-auction market-data {resource} capacity {maximum} cannot hold {attempted} entries"
            ),
            Self::PreparationAllocationFailed(resource) => write!(
                formatter,
                "could not reserve call-auction market-data {resource} output"
            ),
            Self::Poisoned => formatter.write_str("auction market-data state is poisoned"),
        }
    }
}

impl std::error::Error for CallAuctionMarketDataError {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Aggregate {
    quantity: u128,
    order_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TrackedOrder {
    account_id: AccountId,
    side: Side,
    constraint: AuctionOrderConstraint,
    leaves: u64,
    priority_class: AuctionPriorityClass,
    priority_sequence: u64,
}

impl From<CallAuctionOrderSnapshot> for TrackedOrder {
    fn from(order: CallAuctionOrderSnapshot) -> Self {
        Self {
            account_id: order.account_id,
            side: order.side,
            constraint: order.constraint,
            leaves: order.quantity.lots(),
            priority_class: order.priority_class,
            priority_sequence: order.priority_sequence,
        }
    }
}

#[derive(Debug, Default)]
struct UncrossProgress {
    trade_count: u64,
    cancellation_count: u64,
    executed_quantity: u128,
    price: Option<Price>,
    completed: bool,
}

#[derive(Debug, Default)]
struct MassCancelProgress {
    order_count: u64,
    quantity_lots: u128,
    previous_order_id: Option<OrderId>,
    completed: bool,
}

/// Stateful adapter from private auction traces to anonymized public state.
#[derive(Debug)]
pub struct CallAuctionMarketDataPublisher {
    limits: CallAuctionMarketDataLimits,
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    phase: CallAuctionPhaseSnapshot,
    book_revision: u64,
    market_buy: Aggregate,
    market_sell: Aggregate,
    bids: AuctionDepthMap,
    asks: AuctionDepthMap,
    orders: BoundedHashMap<OrderId, TrackedOrder>,
    uncross_sources: BoundedHashMap<OrderId, u64>,
    last_command_sequence: u64,
    last_event_sequence: u64,
    last_trade_id: Option<TradeId>,
    poisoned: bool,
}

impl CallAuctionMarketDataPublisher {
    /// Bootstraps publication from an empty, live, or recovered auction engine.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataConstructionError`] if complete fixed
    /// storage cannot be reserved or source bootstrap validation fails.
    pub fn from_engine(
        engine: &CallAuctionEngine,
    ) -> Result<Self, CallAuctionMarketDataConstructionError> {
        Self::construct(
            engine,
            CallAuctionMarketDataLimits::from_engine(engine.limits()),
        )
    }

    /// Bootstraps under an explicit finite envelope covering the source engine.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataConstructionError`] for an invalid or
    /// undersized policy, reservation failure, or source divergence.
    pub fn try_from_engine_with_limits(
        engine: &CallAuctionEngine,
        spec: CallAuctionMarketDataLimitsSpec,
    ) -> Result<Self, CallAuctionMarketDataConstructionError> {
        let limits = CallAuctionMarketDataLimits::new(spec)?;
        Self::construct(engine, limits)
    }

    fn construct(
        engine: &CallAuctionEngine,
        limits: CallAuctionMarketDataLimits,
    ) -> Result<Self, CallAuctionMarketDataConstructionError> {
        ensure_source_limits(limits, engine.limits())?;
        let definition = engine.book().definition();
        let maximum_levels = limits.max_price_levels_per_side();
        let mut publisher = Self {
            limits,
            instrument_id: definition.instrument_id(),
            instrument_version: definition.version(),
            phase: engine.phase_snapshot(),
            book_revision: engine.book().state_revision(),
            market_buy: aggregate_market(engine, Side::Buy),
            market_sell: aggregate_market(engine, Side::Sell),
            bids: try_depth_map(
                maximum_levels,
                CallAuctionMarketDataResource::BidPriceLevels,
            )?,
            asks: try_depth_map(
                maximum_levels,
                CallAuctionMarketDataResource::AskPriceLevels,
            )?,
            orders: try_hash_map(
                limits.max_active_orders(),
                CallAuctionMarketDataResource::TrackedOrders,
            )?,
            uncross_sources: try_hash_map(
                limits.max_active_orders(),
                CallAuctionMarketDataResource::UncrossSourceOrders,
            )?,
            last_command_sequence: previous_sequence(
                engine.next_command_sequence(),
                "auction next command sequence is zero",
            )
            .map_err(CallAuctionMarketDataConstructionError::Source)?,
            last_event_sequence: previous_sequence(
                engine.next_event_sequence(),
                "auction next event sequence is zero",
            )
            .map_err(CallAuctionMarketDataConstructionError::Source)?,
            last_trade_id: previous_trade_id(engine.book().next_trade_id())
                .map_err(CallAuctionMarketDataConstructionError::Source)?,
            poisoned: false,
        };

        for level in engine.book().limit_depth_levels(Side::Buy) {
            publisher.bids.insert(
                level.price(),
                Aggregate {
                    quantity: level.quantity(),
                    order_count: level.order_count(),
                },
            );
        }
        for level in engine.book().limit_depth_levels(Side::Sell) {
            publisher.asks.insert(
                level.price(),
                Aggregate {
                    quantity: level.quantity(),
                    order_count: level.order_count(),
                },
            );
        }
        for order in engine.book().active_order_states() {
            publisher.orders.insert(order.order_id, order.into());
        }
        publisher
            .validate_against(engine)
            .map_err(CallAuctionMarketDataConstructionError::Source)?;
        Ok(publisher)
    }

    /// Returns this publisher's immutable finite resource envelope.
    #[must_use]
    pub const fn limits(&self) -> CallAuctionMarketDataLimits {
        self.limits
    }

    /// Returns fixed limit-price arena telemetry for one side.
    #[must_use]
    pub fn price_level_arena_status(&self, side: Side) -> CallAuctionMarketDataArenaStatus {
        arena_status(self.levels(side))
    }

    /// Returns publisher hash telemetry, or `None` for replica-only scratch.
    #[must_use]
    pub fn hash_index_status(
        &self,
        index: CallAuctionMarketDataHashIndex,
    ) -> Option<CallAuctionMarketDataHashIndexStatus> {
        match index {
            CallAuctionMarketDataHashIndex::TrackedOrders => Some(hash_status(&self.orders)),
            CallAuctionMarketDataHashIndex::PublisherUncrossSources => {
                Some(hash_status(&self.uncross_sources))
            }
            CallAuctionMarketDataHashIndex::ReplicaBatchLevels => None,
        }
    }

    /// Returns the final published source-event sequence.
    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_event_sequence
    }

    /// Returns the final published committed-command sequence.
    #[must_use]
    pub const fn last_command_sequence(&self) -> u64 {
        self.last_command_sequence
    }

    /// Returns whether a trace failure invalidated incremental state.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Fallibly materializes a complete full-depth recovery image in `O(P)` time.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataError::PreparationAllocationFailed`]
    /// before returning a partial image if either output vector cannot be reserved.
    pub fn try_snapshot(
        &self,
    ) -> Result<CallAuctionMarketDataSnapshot, CallAuctionMarketDataError> {
        let mut bids = Vec::new();
        bids.try_reserve_exact(self.bids.len()).map_err(|_| {
            CallAuctionMarketDataError::PreparationAllocationFailed(
                CallAuctionMarketDataResource::BidPriceLevels,
            )
        })?;
        for (&price, &aggregate) in self.bids.iter().rev() {
            bids.push(level_from_aggregate(
                Side::Buy,
                AuctionOrderConstraint::Limit(price),
                aggregate,
            )?);
        }
        let mut asks = Vec::new();
        asks.try_reserve_exact(self.asks.len()).map_err(|_| {
            CallAuctionMarketDataError::PreparationAllocationFailed(
                CallAuctionMarketDataResource::AskPriceLevels,
            )
        })?;
        for (&price, &aggregate) in self.asks.iter() {
            asks.push(level_from_aggregate(
                Side::Sell,
                AuctionOrderConstraint::Limit(price),
                aggregate,
            )?);
        }
        Ok(CallAuctionMarketDataSnapshot {
            instrument_id: self.instrument_id,
            instrument_version: self.instrument_version,
            as_of_sequence: self.last_event_sequence,
            command_sequence: self.last_command_sequence,
            phase: self.phase,
            book_revision: self.book_revision,
            last_trade_id: self.last_trade_id,
            market_buy: self.market_level(Side::Buy),
            market_sell: self.market_level(Side::Sell),
            bids,
            asks,
        })
    }

    /// Materializes a complete full-depth image using the fallible production path.
    ///
    /// # Panics
    ///
    /// Panics only if output-vector allocation fails. Use [`Self::try_snapshot`]
    /// when allocation failure must remain typed.
    #[must_use]
    pub fn snapshot(&self) -> CallAuctionMarketDataSnapshot {
        self.try_snapshot()
            .expect("call-auction market-data snapshot output allocation failed")
    }

    /// Publishes one complete auction command/report trace.
    ///
    /// Exact retries produce an empty replay batch. A partial trace failure
    /// poisons the publisher; reconstruct it from the authoritative engine.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataError`] for identity, sequence, grammar,
    /// arithmetic, or final source-state divergence.
    #[allow(
        clippy::too_many_lines,
        reason = "one transaction boundary keeps preflight, event application, and source audit contiguous"
    )]
    pub fn publish(
        &mut self,
        command: CallAuctionCommand,
        report: &CallAuctionExecutionReport,
        engine: &CallAuctionEngine,
    ) -> Result<CallAuctionMarketDataBatch, CallAuctionMarketDataError> {
        if self.poisoned {
            return Err(CallAuctionMarketDataError::Poisoned);
        }
        if command_instrument(command) != self.instrument_id {
            return self.fail(CallAuctionMarketDataError::WrongInstrument);
        }
        if command_version(command) != self.instrument_version {
            return self.fail(CallAuctionMarketDataError::WrongInstrumentVersion);
        }
        if report.command_id != command.command_id() || report.events.is_empty() {
            return self.fail(CallAuctionMarketDataError::TraceMismatch(
                "auction report identity is wrong or its event trace is empty",
            ));
        }
        if report.events.iter().any(|event| {
            event.command_id != report.command_id || event.occurred_at != command.received_at()
        }) {
            return self.fail(CallAuctionMarketDataError::TraceMismatch(
                "auction event context differs from its source command",
            ));
        }
        if let Err(error) = validate_report_outcome(report) {
            return self.fail(error);
        }
        if report.replayed {
            if report.command_sequence > self.last_command_sequence
                || report
                    .events
                    .last()
                    .is_some_and(|event| event.sequence > self.last_event_sequence)
            {
                return self.fail(CallAuctionMarketDataError::TraceMismatch(
                    "replayed auction report contains unpublished future state",
                ));
            }
            return Ok(CallAuctionMarketDataBatch {
                updates: Vec::new(),
                replayed: true,
            });
        }
        let expected_command = self
            .last_command_sequence
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        if report.command_sequence != expected_command {
            return self.fail(CallAuctionMarketDataError::CommandSequenceGap {
                expected: expected_command,
                actual: report.command_sequence,
            });
        }
        if report.events.len() > self.limits.max_batch_updates() {
            return Err(CallAuctionMarketDataError::CapacityExceeded {
                resource: CallAuctionMarketDataResource::BatchUpdates,
                maximum: self.limits.max_batch_updates(),
                attempted: report.events.len(),
            });
        }

        let mut updates = Vec::new();
        if updates.try_reserve_exact(report.events.len()).is_err() {
            return Err(CallAuctionMarketDataError::PreparationAllocationFailed(
                CallAuctionMarketDataResource::BatchUpdates,
            ));
        }
        let mut uncross = UncrossProgress::default();
        let mut mass_cancel = MassCancelProgress::default();
        self.uncross_sources.clear();
        for event in report.events.iter().copied() {
            let expected = self
                .last_event_sequence
                .checked_add(1)
                .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
            if event.sequence != expected {
                return self.fail(CallAuctionMarketDataError::SequenceGap {
                    expected,
                    actual: event.sequence,
                });
            }
            let kind = match self.apply_event(command, event, &mut uncross, &mut mass_cancel) {
                Ok(kind) => kind,
                Err(error) => return self.fail(error),
            };
            updates.push(CallAuctionMarketDataUpdate {
                instrument_id: self.instrument_id,
                instrument_version: self.instrument_version,
                sequence: event.sequence,
                occurred_at: event.occurred_at,
                kind,
            });
            self.last_event_sequence = event.sequence;
        }
        let accepted_uncross = matches!(command, CallAuctionCommand::Uncross(_))
            && report.outcome == CallAuctionCommandOutcome::Accepted;
        if uncross.completed != accepted_uncross {
            return self.fail(CallAuctionMarketDataError::TraceMismatch(
                "auction uncross command and completion event disagree",
            ));
        }
        let accepted_mass_cancel = matches!(command, CallAuctionCommand::MassCancel(_))
            && report.outcome == CallAuctionCommandOutcome::Accepted;
        if mass_cancel.completed != accepted_mass_cancel {
            return self.fail(CallAuctionMarketDataError::TraceMismatch(
                "auction mass-cancel command and completion event disagree",
            ));
        }
        self.last_command_sequence = report.command_sequence;
        self.uncross_sources.clear();
        if let Err(error) = self.validate_against(engine) {
            return self.fail(error);
        }
        Ok(CallAuctionMarketDataBatch {
            updates,
            replayed: false,
        })
    }

    /// Audits the complete private mirror against the authoritative engine.
    ///
    /// This diagnostic is `O(O + P)` for `O` active orders and `P` prices.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataError::SourceDivergence`] at the first
    /// mismatch.
    pub fn validate_against(
        &self,
        engine: &CallAuctionEngine,
    ) -> Result<(), CallAuctionMarketDataError> {
        let definition = engine.book().definition();
        if definition.instrument_id() != self.instrument_id {
            return Err(CallAuctionMarketDataError::WrongInstrument);
        }
        if definition.version() != self.instrument_version {
            return Err(CallAuctionMarketDataError::WrongInstrumentVersion);
        }
        let engine_command_sequence = previous_sequence(
            engine.next_command_sequence(),
            "auction next command sequence is zero",
        )?;
        let engine_event_sequence = previous_sequence(
            engine.next_event_sequence(),
            "auction next event sequence is zero",
        )?;
        if self.phase != engine.phase_snapshot()
            || self.book_revision != engine.book().state_revision()
            || self.last_command_sequence != engine_command_sequence
            || self.last_event_sequence != engine_event_sequence
            || self.last_trade_id != previous_trade_id(engine.book().next_trade_id())?
        {
            return Err(CallAuctionMarketDataError::SourceDivergence(
                "publisher sequence, phase, book revision, or trade state differs from the engine",
            ));
        }
        let active = engine.book().active_order_states();
        if active.len() != self.orders.len()
            || active
                .into_iter()
                .any(|order| self.orders.get(&order.order_id).copied() != Some(order.into()))
        {
            return Err(CallAuctionMarketDataError::SourceDivergence(
                "publisher active-order mirror differs from the auction book",
            ));
        }
        if self.market_buy != aggregate_market(engine, Side::Buy)
            || self.market_sell != aggregate_market(engine, Side::Sell)
            || !auction_depth_matches(&self.bids, engine.book().limit_depth_levels(Side::Buy))
            || !auction_depth_matches(&self.asks, engine.book().limit_depth_levels(Side::Sell))
        {
            return Err(CallAuctionMarketDataError::SourceDivergence(
                "publisher aggregate state differs from the auction book",
            ));
        }
        self.validate_layouts()?;
        Ok(())
    }

    fn validate_layouts(&self) -> Result<(), CallAuctionMarketDataError> {
        let maximum_levels = self.limits.max_price_levels_per_side();
        if self.bids.maximum() != maximum_levels
            || self.asks.maximum() != maximum_levels
            || self.orders.maximum() != self.limits.max_active_orders()
            || self.uncross_sources.maximum() != self.limits.max_active_orders()
            || !self.uncross_sources.is_empty()
            || self.bids.allocation_capacity() < maximum_levels
            || self.asks.allocation_capacity() < maximum_levels
        {
            return Err(CallAuctionMarketDataError::SourceDivergence(
                "auction publisher fixed storage contradicts its configured limits",
            ));
        }
        self.bids.validate().map_err(|_| {
            CallAuctionMarketDataError::SourceDivergence(
                "auction publisher bid arena is structurally invalid",
            )
        })?;
        self.asks.validate().map_err(|_| {
            CallAuctionMarketDataError::SourceDivergence(
                "auction publisher ask arena is structurally invalid",
            )
        })?;
        self.orders.validate_layout().map_err(|_| {
            CallAuctionMarketDataError::SourceDivergence(
                "auction publisher order hash layout is invalid",
            )
        })?;
        self.uncross_sources.validate_layout().map_err(|_| {
            CallAuctionMarketDataError::SourceDivergence(
                "auction publisher uncross scratch layout is invalid",
            )
        })?;
        if self
            .bids
            .iter()
            .chain(self.asks.iter())
            .any(|(_, aggregate)| aggregate.quantity == 0 || aggregate.order_count == 0)
        {
            return Err(CallAuctionMarketDataError::SourceDivergence(
                "auction publisher retains an empty limit aggregate",
            ));
        }
        Ok(())
    }

    fn apply_event(
        &mut self,
        command: CallAuctionCommand,
        event: CallAuctionEvent,
        uncross: &mut UncrossProgress,
        mass_cancel: &mut MassCancelProgress,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        match event.kind {
            CallAuctionEventKind::PhaseChanged {
                auction_id,
                previous,
                current,
                revision,
            } => self.apply_phase(command, auction_id, previous, current, revision),
            CallAuctionEventKind::OrderAccepted(order) => self.apply_accept(command, order),
            CallAuctionEventKind::OrderCancelled { order, reason } => match reason {
                crate::auction_engine::CallAuctionCancellationReason::UserRequested => {
                    self.apply_user_cancel(command, order)
                }
                crate::auction_engine::CallAuctionCancellationReason::Replaced => {
                    self.apply_replace_cancel(command, order)
                }
                crate::auction_engine::CallAuctionCancellationReason::MassCancel => {
                    self.apply_mass_cancel(command, order, mass_cancel)
                }
                crate::auction_engine::CallAuctionCancellationReason::UncrossRemainder => {
                    Err(CallAuctionMarketDataError::TraceMismatch(
                        "standalone auction cancellation has an uncross-remainder reason",
                    ))
                }
            },
            CallAuctionEventKind::Trade(trade) => self.apply_trade(command, trade, uncross),
            CallAuctionEventKind::RemainderCancelled(cancellation) => {
                self.apply_remainder(command, cancellation, uncross)
            }
            CallAuctionEventKind::UncrossCompleted {
                auction_id,
                clearing,
                policy,
                trade_count,
                cancellation_count,
                book_revision,
                phase_revision,
            } => {
                let CallAuctionCommand::Uncross(source) = command else {
                    return Err(CallAuctionMarketDataError::TraceMismatch(
                        "non-uncross command emitted completion",
                    ));
                };
                if source.auction_id != auction_id || source.uncross_policy != policy {
                    return Err(CallAuctionMarketDataError::TraceMismatch(
                        "uncross completion differs from its source command",
                    ));
                }
                self.complete_uncross(
                    auction_id,
                    clearing,
                    trade_count,
                    cancellation_count,
                    book_revision,
                    phase_revision,
                    uncross,
                )
            }
            CallAuctionEventKind::MassCancelCompleted {
                account_id,
                scope,
                cancelled_order_count,
                cancelled_quantity_lots,
                book_revision,
            } => self.complete_mass_cancel(
                command,
                account_id,
                scope,
                cancelled_order_count,
                cancelled_quantity_lots,
                book_revision,
                mass_cancel,
            ),
            CallAuctionEventKind::OrderAmended {
                order,
                previous_quantity,
                book_revision,
            } => self.apply_amend(command, order, previous_quantity, book_revision),
            CallAuctionEventKind::CommandRejected(_) => {
                Ok(CallAuctionMarketDataKind::NoPublicChange)
            }
        }
    }

    fn apply_phase(
        &mut self,
        command: CallAuctionCommand,
        auction_id: AuctionId,
        previous: CallAuctionPhase,
        current: CallAuctionPhase,
        revision: u64,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        let CallAuctionCommand::PhaseControl(source) = command else {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "non-phase command emitted a phase transition",
            ));
        };
        let expected_revision = self
            .phase
            .revision()
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        if source.auction_id != auction_id
            || source.target_phase != current
            || self.phase.phase() != previous
            || source.expected_phase_revision != self.phase.revision()
            || revision != expected_revision
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "phase update contradicts the command or publisher state",
            ));
        }
        let (active, last) = transition_cycle(self.phase, auction_id, previous, current)?;
        self.phase = CallAuctionPhaseSnapshot::from_parts(current, revision, active, last);
        Ok(CallAuctionMarketDataKind::PhaseChanged {
            auction_id,
            previous,
            current,
            revision,
        })
    }

    fn apply_accept(
        &mut self,
        command: CallAuctionCommand,
        order: CallAuctionOrderSnapshot,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        let (auction_id, expected_phase_revision, submitted) = match command {
            CallAuctionCommand::Submit(source) => (
                source.auction_id,
                source.expected_phase_revision,
                source.order,
            ),
            CallAuctionCommand::Replace(source) => {
                if source.account_id != source.replacement.account_id()
                    || self.orders.contains_key(&source.target_order_id)
                {
                    return Err(CallAuctionMarketDataError::TraceMismatch(
                        "replacement acceptance precedes its source cancellation",
                    ));
                }
                (
                    source.auction_id,
                    source.expected_phase_revision,
                    source.replacement,
                )
            }
            _ => {
                return Err(CallAuctionMarketDataError::TraceMismatch(
                    "non-submit command emitted an accepted order",
                ));
            }
        };
        if self.phase.phase() != CallAuctionPhase::Collecting
            || self.phase.active_auction_id() != Some(auction_id)
            || expected_phase_revision != self.phase.revision()
            || order.order_id != submitted.order_id()
            || order.account_id != submitted.account_id()
            || order.side != submitted.side()
            || order.constraint != submitted.constraint()
            || order.quantity != submitted.quantity()
            || order.priority_class != submitted.priority_class()
            || order.priority_sequence == 0
            || self.orders.contains_key(&order.order_id)
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "accepted order contradicts its command or collection state",
            ));
        }
        if self.orders.len() >= self.limits.max_active_orders() {
            return Err(CallAuctionMarketDataError::CapacityExceeded {
                resource: CallAuctionMarketDataResource::TrackedOrders,
                maximum: self.limits.max_active_orders(),
                attempted: self.orders.len().saturating_add(1),
            });
        }
        self.preflight_new_limit_level(order.side, order.constraint)?;
        self.orders.insert(order.order_id, order.into());
        let level = self.add_level(order.side, order.constraint, order.quantity.lots())?;
        self.book_revision = self
            .book_revision
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        Ok(CallAuctionMarketDataKind::Book {
            reason: CallAuctionBookChangeReason::Accepted,
            quantity: order.quantity,
            level,
        })
    }

    fn apply_user_cancel(
        &mut self,
        command: CallAuctionCommand,
        order: CallAuctionOrderSnapshot,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        let CallAuctionCommand::Cancel(source) = command else {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "non-cancel command emitted a user cancellation",
            ));
        };
        let tracked = self.orders.get(&order.order_id).copied().ok_or(
            CallAuctionMarketDataError::TraceMismatch("cancelled auction order is absent"),
        )?;
        if source.order_id != order.order_id
            || source.account_id != order.account_id
            || TrackedOrder::from(order) != tracked
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "cancelled order contradicts its command or tracked state",
            ));
        }
        let level = self.remove_order(order.order_id, order.quantity.lots())?;
        self.book_revision = self
            .book_revision
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        Ok(CallAuctionMarketDataKind::Book {
            reason: CallAuctionBookChangeReason::UserCancelled,
            quantity: order.quantity,
            level,
        })
    }

    fn apply_amend(
        &mut self,
        command: CallAuctionCommand,
        order: CallAuctionOrderSnapshot,
        previous_quantity: Quantity,
        book_revision: u64,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        let CallAuctionCommand::Amend(source) = command else {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "non-amend command emitted an amended order",
            ));
        };
        let tracked = self.orders.get(&order.order_id).copied().ok_or(
            CallAuctionMarketDataError::TraceMismatch("amended auction order is absent"),
        )?;
        let expected_revision = self
            .book_revision
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        if self.phase.phase() != CallAuctionPhase::Collecting
            || self.phase.active_auction_id() != Some(source.auction_id)
            || self.phase.revision() != source.expected_phase_revision
            || source.account_id != order.account_id
            || source.order_id != order.order_id
            || source.new_quantity != order.quantity
            || tracked.account_id != order.account_id
            || tracked.side != order.side
            || tracked.constraint != order.constraint
            || tracked.priority_class != order.priority_class
            || tracked.priority_sequence != order.priority_sequence
            || tracked.leaves != previous_quantity.lots()
            || order.quantity.lots() >= previous_quantity.lots()
            || book_revision != expected_revision
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "amended order contradicts its command, event, or tracked state",
            ));
        }
        let delta_lots = previous_quantity
            .lots()
            .checked_sub(order.quantity.lots())
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        let level = self.decrement_order(order.order_id, delta_lots)?;
        self.book_revision = book_revision;
        Ok(CallAuctionMarketDataKind::Book {
            reason: CallAuctionBookChangeReason::Amended,
            quantity: Quantity::new(delta_lots)
                .expect("strict amendment reduction produces a positive delta"),
            level,
        })
    }

    fn apply_replace_cancel(
        &mut self,
        command: CallAuctionCommand,
        order: CallAuctionOrderSnapshot,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        let CallAuctionCommand::Replace(source) = command else {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "non-replace command emitted a replacement cancellation",
            ));
        };
        let tracked = self.orders.get(&order.order_id).copied().ok_or(
            CallAuctionMarketDataError::TraceMismatch("replaced auction order is absent"),
        )?;
        if source.target_order_id != order.order_id
            || source.account_id != order.account_id
            || source.replacement.account_id() != source.account_id
            || TrackedOrder::from(order) != tracked
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "replaced order contradicts its command or tracked state",
            ));
        }
        let level = self.remove_order(order.order_id, order.quantity.lots())?;
        Ok(CallAuctionMarketDataKind::Book {
            reason: CallAuctionBookChangeReason::Replaced,
            quantity: order.quantity,
            level,
        })
    }

    fn apply_mass_cancel(
        &mut self,
        command: CallAuctionCommand,
        order: CallAuctionOrderSnapshot,
        progress: &mut MassCancelProgress,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        let CallAuctionCommand::MassCancel(source) = command else {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "non-mass-cancel command emitted a mass cancellation",
            ));
        };
        let tracked = self.orders.get(&order.order_id).copied().ok_or(
            CallAuctionMarketDataError::TraceMismatch("mass-cancelled auction order is absent"),
        )?;
        if progress.completed
            || source.account_id != order.account_id
            || !source.scope.includes(order.side)
            || progress
                .previous_order_id
                .is_some_and(|previous| previous >= order.order_id)
            || TrackedOrder::from(order) != tracked
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "mass-cancelled order contradicts its command, canonical order, or tracked state",
            ));
        }
        let level = self.remove_order(order.order_id, order.quantity.lots())?;
        progress.order_count = progress
            .order_count
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        progress.quantity_lots = progress
            .quantity_lots
            .checked_add(u128::from(order.quantity.lots()))
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        progress.previous_order_id = Some(order.order_id);
        Ok(CallAuctionMarketDataKind::Book {
            reason: CallAuctionBookChangeReason::MassCancelled,
            quantity: order.quantity,
            level,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the mass-cancel completion event is a fixed private audit record"
    )]
    fn complete_mass_cancel(
        &mut self,
        command: CallAuctionCommand,
        account_id: AccountId,
        scope: MassCancelScope,
        cancelled_order_count: u64,
        cancelled_quantity_lots: u128,
        book_revision: u64,
        progress: &mut MassCancelProgress,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        let CallAuctionCommand::MassCancel(source) = command else {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "non-mass-cancel command emitted mass-cancel completion",
            ));
        };
        let expected_book_revision = if cancelled_order_count == 0 {
            self.book_revision
        } else {
            self.book_revision
                .checked_add(1)
                .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?
        };
        if progress.completed
            || source.account_id != account_id
            || source.scope != scope
            || progress.order_count != cancelled_order_count
            || progress.quantity_lots != cancelled_quantity_lots
            || book_revision != expected_book_revision
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "mass-cancel completion contradicts its command or removal trace",
            ));
        }
        self.book_revision = book_revision;
        progress.completed = true;
        Ok(CallAuctionMarketDataKind::MassCancelCompleted {
            cancelled_order_count,
            cancelled_quantity_lots,
            book_revision,
        })
    }

    fn apply_trade(
        &mut self,
        command: CallAuctionCommand,
        trade: CallAuctionTrade,
        uncross: &mut UncrossProgress,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        let CallAuctionCommand::Uncross(source) = command else {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "non-uncross command emitted an auction trade",
            ));
        };
        if self.phase.phase() != CallAuctionPhase::Frozen
            || self.phase.active_auction_id() != Some(source.auction_id)
            || trade.price().raw() < source.price_band.minimum().raw()
            || trade.price().raw() > source.price_band.maximum().raw()
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "auction trade contradicts frozen-cycle context",
            ));
        }
        self.advance_trade_id(trade.trade_id())?;
        let buy_order = self.orders.get(&trade.buy_order_id()).copied().ok_or(
            CallAuctionMarketDataError::TraceMismatch("auction trade buy order is absent"),
        )?;
        let sell_order = self.orders.get(&trade.sell_order_id()).copied().ok_or(
            CallAuctionMarketDataError::TraceMismatch("auction trade sell order is absent"),
        )?;
        if buy_order.side != Side::Buy
            || sell_order.side != Side::Sell
            || buy_order.account_id != trade.buy_account_id()
            || sell_order.account_id != trade.sell_account_id()
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "auction trade identities or owners contradict tracked state",
            ));
        }
        self.remember_source(trade.buy_order_id(), buy_order.leaves)?;
        self.remember_source(trade.sell_order_id(), sell_order.leaves)?;
        let buy_change = self.decrement_order(trade.buy_order_id(), trade.quantity().lots())?;
        let sell_change = self.decrement_order(trade.sell_order_id(), trade.quantity().lots())?;
        uncross.trade_count = uncross
            .trade_count
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        uncross.executed_quantity = uncross
            .executed_quantity
            .checked_add(u128::from(trade.quantity().lots()))
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        match uncross.price {
            None => uncross.price = Some(trade.price()),
            Some(price) if price == trade.price() => {}
            Some(_) => {
                return Err(CallAuctionMarketDataError::TraceMismatch(
                    "one uncross printed multiple clearing prices",
                ));
            }
        }
        Ok(CallAuctionMarketDataKind::Trade {
            print: CallAuctionTradePrint {
                auction_id: source.auction_id,
                trade_id: trade.trade_id(),
                price: trade.price(),
                quantity: trade.quantity(),
            },
            buy: buy_change,
            sell: sell_change,
        })
    }

    fn apply_remainder(
        &mut self,
        command: CallAuctionCommand,
        cancellation: CallAuctionCancellation,
        uncross: &mut UncrossProgress,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        if !matches!(command, CallAuctionCommand::Uncross(_)) {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "non-uncross command emitted a remainder cancellation",
            ));
        }
        let tracked = self.orders.get(&cancellation.order_id()).copied().ok_or(
            CallAuctionMarketDataError::TraceMismatch("auction remainder order is absent"),
        )?;
        if tracked.account_id != cancellation.account_id()
            || tracked.side != cancellation.side()
            || tracked.constraint != cancellation.constraint()
            || tracked.leaves != cancellation.quantity().lots()
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "auction remainder contradicts tracked state",
            ));
        }
        self.remember_source(cancellation.order_id(), tracked.leaves)?;
        let source_quantity = *self
            .uncross_sources
            .get(&cancellation.order_id())
            .expect("remembered uncross source must remain present");
        if source_quantity.checked_sub(cancellation.quantity().lots())
            != Some(cancellation.executed_quantity_lots())
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "auction remainder does not reconcile to this uncross source quantity",
            ));
        }
        let level = self.remove_order(cancellation.order_id(), cancellation.quantity().lots())?;
        uncross.cancellation_count = uncross
            .cancellation_count
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        Ok(CallAuctionMarketDataKind::Book {
            reason: CallAuctionBookChangeReason::UncrossRemainder,
            quantity: cancellation.quantity(),
            level,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the uncross completion event is a fixed public audit record"
    )]
    fn complete_uncross(
        &mut self,
        auction_id: AuctionId,
        clearing: AuctionClearing,
        trade_count: u64,
        cancellation_count: u64,
        book_revision: u64,
        phase_revision: u64,
        uncross: &mut UncrossProgress,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        let expected_book_revision = self
            .book_revision
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        let expected_phase_revision = self
            .phase
            .revision()
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        if uncross.completed
            || self.phase.phase() != CallAuctionPhase::Frozen
            || self.phase.active_auction_id() != Some(auction_id)
            || uncross.trade_count != trade_count
            || uncross.cancellation_count != cancellation_count
            || uncross.executed_quantity != clearing.executable_quantity()
            || uncross.price != Some(clearing.price())
            || book_revision != expected_book_revision
            || phase_revision != expected_phase_revision
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "uncross completion contradicts its public trace or phase state",
            ));
        }
        self.book_revision = book_revision;
        self.phase = CallAuctionPhaseSnapshot::from_parts(
            CallAuctionPhase::Closed,
            phase_revision,
            None,
            Some(auction_id),
        );
        uncross.completed = true;
        Ok(CallAuctionMarketDataKind::UncrossCompleted {
            auction_id,
            clearing,
            trade_count,
            cancellation_count,
            book_revision,
            phase_revision,
        })
    }

    fn add_level(
        &mut self,
        side: Side,
        constraint: AuctionOrderConstraint,
        quantity: u64,
    ) -> Result<CallAuctionMarketDataLevel, CallAuctionMarketDataError> {
        let mut aggregate = self.aggregate_value(side, constraint);
        aggregate.quantity = aggregate
            .quantity
            .checked_add(u128::from(quantity))
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        aggregate.order_count = aggregate
            .order_count
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        self.set_aggregate(side, constraint, aggregate)?;
        level_from_aggregate(side, constraint, aggregate)
    }

    fn decrement_order(
        &mut self,
        order_id: OrderId,
        quantity: u64,
    ) -> Result<CallAuctionMarketDataLevel, CallAuctionMarketDataError> {
        let order = self.orders.get(&order_id).copied().ok_or(
            CallAuctionMarketDataError::TraceMismatch("auction execution target is absent"),
        )?;
        if quantity == 0 || quantity > order.leaves {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "auction execution quantity exceeds tracked leaves",
            ));
        }
        let removes_order = quantity == order.leaves;
        let mut result = self.aggregate_value(order.side, order.constraint);
        result.quantity = result.quantity.checked_sub(u128::from(quantity)).ok_or(
            CallAuctionMarketDataError::TraceMismatch(
                "auction aggregate underflowed during execution",
            ),
        )?;
        if removes_order {
            result.order_count = result.order_count.checked_sub(1).ok_or(
                CallAuctionMarketDataError::TraceMismatch(
                    "auction aggregate order count underflowed",
                ),
            )?;
        }
        if removes_order {
            self.orders.remove(&order_id);
        } else {
            self.orders
                .get_mut(&order_id)
                .expect("checked auction order exists")
                .leaves -= quantity;
        }
        self.set_aggregate(order.side, order.constraint, result)?;
        level_from_aggregate(order.side, order.constraint, result)
    }

    fn remove_order(
        &mut self,
        order_id: OrderId,
        quantity: u64,
    ) -> Result<CallAuctionMarketDataLevel, CallAuctionMarketDataError> {
        let order = self.orders.get(&order_id).copied().ok_or(
            CallAuctionMarketDataError::TraceMismatch("auction removal target is absent"),
        )?;
        if order.leaves != quantity {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "auction removal quantity differs from tracked leaves",
            ));
        }
        self.decrement_order(order_id, quantity)
    }

    fn advance_trade_id(&mut self, actual: TradeId) -> Result<(), CallAuctionMarketDataError> {
        let expected = match self.last_trade_id {
            Some(previous) => previous
                .get()
                .checked_add(1)
                .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?,
            None => 1,
        };
        if actual.get() != expected {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "auction trade identifier is not contiguous",
            ));
        }
        self.last_trade_id = Some(actual);
        Ok(())
    }

    fn aggregate_value(&self, side: Side, constraint: AuctionOrderConstraint) -> Aggregate {
        match (side, constraint) {
            (Side::Buy, AuctionOrderConstraint::Market) => self.market_buy,
            (Side::Sell, AuctionOrderConstraint::Market) => self.market_sell,
            (Side::Buy, AuctionOrderConstraint::Limit(price)) => {
                self.bids.get(&price).copied().unwrap_or_default()
            }
            (Side::Sell, AuctionOrderConstraint::Limit(price)) => {
                self.asks.get(&price).copied().unwrap_or_default()
            }
        }
    }

    fn set_aggregate(
        &mut self,
        side: Side,
        constraint: AuctionOrderConstraint,
        aggregate: Aggregate,
    ) -> Result<(), CallAuctionMarketDataError> {
        match (side, constraint) {
            (Side::Buy, AuctionOrderConstraint::Market) => self.market_buy = aggregate,
            (Side::Sell, AuctionOrderConstraint::Market) => self.market_sell = aggregate,
            (Side::Buy, AuctionOrderConstraint::Limit(price)) => {
                set_bounded_limit_level(
                    &mut self.bids,
                    Side::Buy,
                    price,
                    aggregate,
                    self.limits.max_price_levels_per_side(),
                )?;
            }
            (Side::Sell, AuctionOrderConstraint::Limit(price)) => {
                set_bounded_limit_level(
                    &mut self.asks,
                    Side::Sell,
                    price,
                    aggregate,
                    self.limits.max_price_levels_per_side(),
                )?;
            }
        }
        Ok(())
    }

    fn preflight_new_limit_level(
        &self,
        side: Side,
        constraint: AuctionOrderConstraint,
    ) -> Result<(), CallAuctionMarketDataError> {
        let AuctionOrderConstraint::Limit(price) = constraint else {
            return Ok(());
        };
        let levels = self.levels(side);
        if levels.get(&price).is_none() && levels.len() >= self.limits.max_price_levels_per_side() {
            return Err(CallAuctionMarketDataError::CapacityExceeded {
                resource: side_resource(side),
                maximum: self.limits.max_price_levels_per_side(),
                attempted: levels.len().saturating_add(1),
            });
        }
        Ok(())
    }

    fn remember_source(
        &mut self,
        order_id: OrderId,
        leaves: u64,
    ) -> Result<(), CallAuctionMarketDataError> {
        if let Some(source) = self.uncross_sources.get(&order_id) {
            if *source < leaves {
                return Err(CallAuctionMarketDataError::TraceMismatch(
                    "auction order leaves increased during one uncross",
                ));
            }
            return Ok(());
        }
        self.uncross_sources
            .try_insert(order_id, leaves)
            .map_err(|_| CallAuctionMarketDataError::CapacityExceeded {
                resource: CallAuctionMarketDataResource::UncrossSourceOrders,
                maximum: self.limits.max_active_orders(),
                attempted: self.uncross_sources.len().saturating_add(1),
            })?;
        Ok(())
    }

    fn market_level(&self, side: Side) -> CallAuctionMarketDataLevel {
        let aggregate = match side {
            Side::Buy => self.market_buy,
            Side::Sell => self.market_sell,
        };
        level_from_aggregate(side, AuctionOrderConstraint::Market, aggregate)
            .expect("publisher maintains valid market aggregate")
    }

    fn levels(&self, side: Side) -> &AuctionDepthMap {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn fail<T>(
        &mut self,
        error: CallAuctionMarketDataError,
    ) -> Result<T, CallAuctionMarketDataError> {
        self.uncross_sources.clear();
        self.poisoned = true;
        Err(error)
    }
}

/// Gap-detecting consumer state for one immutable call-auction instrument version.
#[derive(Debug)]
pub struct CallAuctionMarketDataReplica {
    limits: CallAuctionMarketDataLimits,
    definition: InstrumentDefinition,
    phase: CallAuctionPhaseSnapshot,
    book_revision: u64,
    market_buy: Aggregate,
    market_sell: Aggregate,
    bids: AuctionDepthMap,
    asks: AuctionDepthMap,
    standby_bids: AuctionDepthMap,
    standby_asks: AuctionDepthMap,
    batch_levels: BoundedHashMap<(Side, Price), bool>,
    last_command_sequence: u64,
    last_event_sequence: u64,
    last_trade_id: Option<TradeId>,
    cycle_trade_count: u64,
    cycle_cancellation_count: u64,
    cycle_executed_quantity: u128,
    cycle_trade_price: Option<Price>,
    poisoned: bool,
}

impl CallAuctionMarketDataReplica {
    /// Creates an empty consumer bound to one immutable instrument definition.
    ///
    /// # Panics
    ///
    /// Panics only if the documented default fixed storage cannot be allocated.
    /// Use [`Self::try_with_limits`] when construction failure must remain typed.
    #[must_use]
    pub fn new(definition: InstrumentDefinition) -> Self {
        Self::try_with_limits(definition, CallAuctionMarketDataLimitsSpec::default())
            .expect("default call-auction market-data replica storage must be constructible")
    }

    /// Fallibly constructs a finite, double-buffered replica state machine.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataConstructionError`] before state exists if
    /// the policy is invalid or any active/standby/scratch layout cannot be reserved.
    pub fn try_with_limits(
        definition: InstrumentDefinition,
        spec: CallAuctionMarketDataLimitsSpec,
    ) -> Result<Self, CallAuctionMarketDataConstructionError> {
        let limits = CallAuctionMarketDataLimits::new(spec)?;
        let maximum_levels = limits.max_price_levels_per_side();
        Ok(Self {
            limits,
            definition,
            phase: CallAuctionPhaseSnapshot::default(),
            book_revision: 0,
            market_buy: Aggregate::default(),
            market_sell: Aggregate::default(),
            bids: try_depth_map(
                maximum_levels,
                CallAuctionMarketDataResource::BidPriceLevels,
            )?,
            asks: try_depth_map(
                maximum_levels,
                CallAuctionMarketDataResource::AskPriceLevels,
            )?,
            standby_bids: try_depth_map(
                maximum_levels,
                CallAuctionMarketDataResource::BidPriceLevels,
            )?,
            standby_asks: try_depth_map(
                maximum_levels,
                CallAuctionMarketDataResource::AskPriceLevels,
            )?,
            batch_levels: try_hash_map(
                limits.max_batch_updates(),
                CallAuctionMarketDataResource::BatchLevelIdentities,
            )?,
            last_command_sequence: 0,
            last_event_sequence: 0,
            last_trade_id: None,
            cycle_trade_count: 0,
            cycle_cancellation_count: 0,
            cycle_executed_quantity: 0,
            cycle_trade_price: None,
            poisoned: false,
        })
    }

    /// Returns this replica's immutable finite resource envelope.
    #[must_use]
    pub const fn limits(&self) -> CallAuctionMarketDataLimits {
        self.limits
    }

    /// Returns active limit-price arena telemetry for one side.
    #[must_use]
    pub fn price_level_arena_status(&self, side: Side) -> CallAuctionMarketDataArenaStatus {
        arena_status(self.levels(side))
    }

    /// Returns standby snapshot-arena telemetry for one side.
    #[must_use]
    pub fn standby_price_level_arena_status(&self, side: Side) -> CallAuctionMarketDataArenaStatus {
        arena_status(match side {
            Side::Buy => &self.standby_bids,
            Side::Sell => &self.standby_asks,
        })
    }

    /// Returns the fixed batch-capacity scratch hash telemetry.
    #[must_use]
    pub fn batch_level_hash_status(&self) -> CallAuctionMarketDataHashIndexStatus {
        hash_status(&self.batch_levels)
    }

    /// Atomically replaces local state with a non-stale verified snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataError`] for identity, staleness,
    /// definition, or snapshot structural failure.
    pub fn apply_snapshot(
        &mut self,
        snapshot: &CallAuctionMarketDataSnapshot,
    ) -> Result<(), CallAuctionMarketDataError> {
        if snapshot.instrument_id != self.definition.instrument_id() {
            return Err(CallAuctionMarketDataError::WrongInstrument);
        }
        if snapshot.instrument_version != self.definition.version() {
            return Err(CallAuctionMarketDataError::WrongInstrumentVersion);
        }
        if snapshot.as_of_sequence < self.last_event_sequence {
            return Err(CallAuctionMarketDataError::StaleSnapshot {
                current: self.last_event_sequence,
                snapshot: snapshot.as_of_sequence,
            });
        }
        snapshot.validate()?;
        self.validate_snapshot_prices(snapshot)?;
        let maximum = self.limits.max_price_levels_per_side();
        if snapshot.bids.len() > maximum {
            return Err(CallAuctionMarketDataError::CapacityExceeded {
                resource: CallAuctionMarketDataResource::BidPriceLevels,
                maximum,
                attempted: snapshot.bids.len(),
            });
        }
        if snapshot.asks.len() > maximum {
            return Err(CallAuctionMarketDataError::CapacityExceeded {
                resource: CallAuctionMarketDataResource::AskPriceLevels,
                maximum,
                attempted: snapshot.asks.len(),
            });
        }
        self.standby_bids.clear();
        self.standby_asks.clear();
        for level in &snapshot.bids {
            self.standby_bids
                .insert(limit_price(*level), aggregate_from_level(*level));
        }
        for level in &snapshot.asks {
            self.standby_asks
                .insert(limit_price(*level), aggregate_from_level(*level));
        }
        std::mem::swap(&mut self.bids, &mut self.standby_bids);
        std::mem::swap(&mut self.asks, &mut self.standby_asks);
        self.phase = snapshot.phase;
        self.book_revision = snapshot.book_revision;
        self.market_buy = aggregate_from_level(snapshot.market_buy);
        self.market_sell = aggregate_from_level(snapshot.market_sell);
        self.last_command_sequence = snapshot.command_sequence;
        self.last_event_sequence = snapshot.as_of_sequence;
        self.last_trade_id = snapshot.last_trade_id;
        self.reset_cycle_trace();
        self.poisoned = false;
        Ok(())
    }

    /// Applies one contiguous command batch.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataError`] for gaps, identity mismatch, or
    /// an invalid transition. A structural failure poisons the replica.
    pub fn apply_batch(
        &mut self,
        batch: &CallAuctionMarketDataBatch,
    ) -> Result<(), CallAuctionMarketDataError> {
        if self.poisoned {
            return Err(CallAuctionMarketDataError::Poisoned);
        }
        if batch.replayed {
            return if batch.updates.is_empty() {
                Ok(())
            } else {
                Err(CallAuctionMarketDataError::InvalidUpdate(
                    "replayed auction batch contains updates",
                ))
            };
        }
        if batch.updates.is_empty() {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "non-replayed auction batch is empty",
            ));
        }
        self.apply_complete_batch(batch.updates.iter().copied())
    }

    /// Applies one complete zero-copy batch returned by retained replay.
    ///
    /// The same identity, continuity, capacity, transition, poisoning, and
    /// command-boundary rules as [`Self::apply_batch`] apply. The replay view is
    /// reusable because this method iterates an independent clone.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataError`] for gaps, identity mismatch,
    /// capacity exhaustion, or an invalid transition. A structural failure
    /// after mutation poisons the replica.
    pub fn apply_replay_batch(
        &mut self,
        batch: &CallAuctionMarketDataReplayBatch<'_>,
    ) -> Result<(), CallAuctionMarketDataError> {
        if self.poisoned {
            return Err(CallAuctionMarketDataError::Poisoned);
        }
        self.apply_complete_batch(batch.complete_iter())
    }

    /// Applies one decoded update without a command-boundary counter.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataError`] for a gap, identity mismatch, or
    /// invalid transition. Prefer [`Self::apply_batch`] when batch boundaries
    /// are available.
    pub fn apply(
        &mut self,
        update: CallAuctionMarketDataUpdate,
    ) -> Result<(), CallAuctionMarketDataError> {
        if self.poisoned {
            return Err(CallAuctionMarketDataError::Poisoned);
        }
        if matches!(
            update.kind,
            CallAuctionMarketDataKind::Book {
                reason: CallAuctionBookChangeReason::Replaced,
                ..
            }
        ) {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction replacement removal requires a complete command batch",
            ));
        }
        if matches!(
            update.kind,
            CallAuctionMarketDataKind::Book {
                reason: CallAuctionBookChangeReason::MassCancelled,
                ..
            } | CallAuctionMarketDataKind::MassCancelCompleted { .. }
        ) {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction mass cancellation requires a complete command batch",
            ));
        }
        self.preflight_identity(update)?;
        let expected = self
            .last_event_sequence
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        if update.sequence != expected {
            return Err(CallAuctionMarketDataError::SequenceGap {
                expected,
                actual: update.sequence,
            });
        }
        self.preflight_batch_level_capacity(std::iter::once(update))?;
        if let Err(error) = self.apply_verified(update) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    /// Fallibly returns limit depth in best-to-worst order.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataError::PreparationAllocationFailed`]
    /// before partial output if the result vector cannot be reserved.
    pub fn try_limit_depth(
        &self,
        side: Side,
        limit: usize,
    ) -> Result<Vec<CallAuctionLevelSnapshot>, CallAuctionMarketDataError> {
        let output_len = self.levels(side).len().min(limit);
        let mut output = Vec::new();
        output.try_reserve_exact(output_len).map_err(|_| {
            CallAuctionMarketDataError::PreparationAllocationFailed(side_resource(side))
        })?;
        match side {
            Side::Buy => {
                for (&price, aggregate) in self.bids.iter().rev().take(limit) {
                    output.push(
                        CallAuctionLevelSnapshot::from_parts(
                            side,
                            price,
                            aggregate.quantity,
                            aggregate.order_count,
                        )
                        .ok_or(
                            CallAuctionMarketDataError::SourceDivergence(
                                "auction replica bid output contains an empty level",
                            ),
                        )?,
                    );
                }
            }
            Side::Sell => {
                for (&price, aggregate) in self.asks.iter().take(limit) {
                    output.push(
                        CallAuctionLevelSnapshot::from_parts(
                            side,
                            price,
                            aggregate.quantity,
                            aggregate.order_count,
                        )
                        .ok_or(
                            CallAuctionMarketDataError::SourceDivergence(
                                "auction replica ask output contains an empty level",
                            ),
                        )?,
                    );
                }
            }
        }
        Ok(output)
    }

    /// Returns limit depth using the fallible production path.
    ///
    /// # Panics
    ///
    /// Panics only if result-vector allocation fails. Use
    /// [`Self::try_limit_depth`] when allocation failure must remain typed.
    #[must_use]
    pub fn limit_depth(&self, side: Side, limit: usize) -> Vec<CallAuctionLevelSnapshot> {
        self.try_limit_depth(side, limit)
            .expect("call-auction market-data depth output allocation failed")
    }

    /// Returns active market-constrained quantity on one side in lots.
    #[must_use]
    pub const fn market_quantity(&self, side: Side) -> u128 {
        match side {
            Side::Buy => self.market_buy.quantity,
            Side::Sell => self.market_sell.quantity,
        }
    }

    /// Returns active market-constrained order count on one side.
    #[must_use]
    pub const fn market_order_count(&self, side: Side) -> u64 {
        match side {
            Side::Buy => self.market_buy.order_count,
            Side::Sell => self.market_sell.order_count,
        }
    }

    /// Returns effective phase and cycle state.
    #[must_use]
    pub const fn phase(&self) -> CallAuctionPhaseSnapshot {
        self.phase
    }

    /// Returns the final applied event sequence.
    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_event_sequence
    }

    /// Returns the number of complete batches applied or snapshot-restored.
    #[must_use]
    pub const fn last_command_sequence(&self) -> u64 {
        self.last_command_sequence
    }

    /// Returns the authoritative collection-book revision.
    #[must_use]
    pub const fn book_revision(&self) -> u64 {
        self.book_revision
    }

    /// Returns whether a new snapshot is required after invalid incremental state.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Audits every fixed layout and active/standby public-state invariant.
    ///
    /// This diagnostic path is `O(P + U)` and can allocate inside the AVL
    /// structural auditor; it is not part of incremental application.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMarketDataError::SourceDivergence`] on the first
    /// internal layout or state contradiction.
    pub fn validate(&self) -> Result<(), CallAuctionMarketDataError> {
        let maximum = self.limits.max_price_levels_per_side();
        for (detail, side, levels) in [
            (
                "auction replica active bid arena is structurally invalid",
                Side::Buy,
                &self.bids,
            ),
            (
                "auction replica active ask arena is structurally invalid",
                Side::Sell,
                &self.asks,
            ),
            (
                "auction replica standby bid arena is structurally invalid",
                Side::Buy,
                &self.standby_bids,
            ),
            (
                "auction replica standby ask arena is structurally invalid",
                Side::Sell,
                &self.standby_asks,
            ),
        ] {
            if levels.maximum() != maximum || levels.allocation_capacity() < maximum {
                return Err(CallAuctionMarketDataError::SourceDivergence(
                    "auction replica price arena contradicts its configured limit",
                ));
            }
            levels
                .validate()
                .map_err(|_| CallAuctionMarketDataError::SourceDivergence(detail))?;
            for (price, aggregate) in levels.iter() {
                if aggregate.quantity == 0
                    || aggregate.order_count == 0
                    || self.definition.price_rules().validate(*price).is_err()
                {
                    return Err(CallAuctionMarketDataError::SourceDivergence(match side {
                        Side::Buy => "auction replica bid arena retains an invalid level",
                        Side::Sell => "auction replica ask arena retains an invalid level",
                    }));
                }
            }
        }
        if self.batch_levels.maximum() != self.limits.max_batch_updates()
            || !self.batch_levels.is_empty()
            || self.batch_levels.validate_layout().is_err()
        {
            return Err(CallAuctionMarketDataError::SourceDivergence(
                "auction replica batch scratch contradicts its fixed empty layout",
            ));
        }
        if (self.market_buy.quantity == 0) != (self.market_buy.order_count == 0)
            || (self.market_sell.quantity == 0) != (self.market_sell.order_count == 0)
            || self.last_command_sequence > self.last_event_sequence
            || self.book_revision > self.last_event_sequence
            || self
                .last_trade_id
                .is_some_and(|trade_id| trade_id.get() > self.last_event_sequence)
            || validate_phase_snapshot(self.phase, self.last_event_sequence).is_err()
        {
            return Err(CallAuctionMarketDataError::SourceDivergence(
                "auction replica scalar state is internally inconsistent",
            ));
        }
        Ok(())
    }

    fn preflight_identity(
        &self,
        update: CallAuctionMarketDataUpdate,
    ) -> Result<(), CallAuctionMarketDataError> {
        if update.instrument_id != self.definition.instrument_id() {
            return Err(CallAuctionMarketDataError::WrongInstrument);
        }
        if update.instrument_version != self.definition.version() {
            return Err(CallAuctionMarketDataError::WrongInstrumentVersion);
        }
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive public-event grammar keeps every replica transition auditable"
    )]
    fn apply_verified(
        &mut self,
        update: CallAuctionMarketDataUpdate,
    ) -> Result<(), CallAuctionMarketDataError> {
        validate_update_kind(update.sequence, update.kind)?;
        match update.kind {
            CallAuctionMarketDataKind::NoPublicChange => {}
            CallAuctionMarketDataKind::Book {
                reason,
                quantity,
                level,
            } => {
                self.validate_level_price(level)?;
                self.validate_book_transition(reason, quantity, level)?;
                match reason {
                    CallAuctionBookChangeReason::Accepted => {
                        if self.phase.phase() != CallAuctionPhase::Collecting {
                            return Err(CallAuctionMarketDataError::InvalidUpdate(
                                "accepted public interest outside collection",
                            ));
                        }
                        self.book_revision = self
                            .book_revision
                            .checked_add(1)
                            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                    }
                    CallAuctionBookChangeReason::UserCancelled => {
                        self.book_revision = self
                            .book_revision
                            .checked_add(1)
                            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                    }
                    CallAuctionBookChangeReason::Replaced => {
                        if self.phase.phase() != CallAuctionPhase::Collecting {
                            return Err(CallAuctionMarketDataError::InvalidUpdate(
                                "replacement cancellation occurred outside collection",
                            ));
                        }
                    }
                    CallAuctionBookChangeReason::MassCancelled => {}
                    CallAuctionBookChangeReason::Amended => {
                        if self.phase.phase() != CallAuctionPhase::Collecting {
                            return Err(CallAuctionMarketDataError::InvalidUpdate(
                                "auction amendment occurred outside collection",
                            ));
                        }
                        self.book_revision = self
                            .book_revision
                            .checked_add(1)
                            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                    }
                    CallAuctionBookChangeReason::UncrossRemainder => {
                        if self.phase.phase() != CallAuctionPhase::Frozen {
                            return Err(CallAuctionMarketDataError::InvalidUpdate(
                                "uncross remainder occurred outside the frozen phase",
                            ));
                        }
                        self.cycle_cancellation_count = self
                            .cycle_cancellation_count
                            .checked_add(1)
                            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                    }
                }
                self.set_level(level)?;
            }
            CallAuctionMarketDataKind::Trade { print, buy, sell } => {
                if self.phase.phase() != CallAuctionPhase::Frozen
                    || self.phase.active_auction_id() != Some(print.auction_id)
                    || buy.side != Side::Buy
                    || sell.side != Side::Sell
                {
                    return Err(CallAuctionMarketDataError::InvalidUpdate(
                        "auction trade contradicts phase or side context",
                    ));
                }
                self.validate_level_price(buy)?;
                self.validate_level_price(sell)?;
                self.validate_trade_transition(print, buy)?;
                self.validate_trade_transition(print, sell)?;
                self.advance_replica_trade_id(print.trade_id)?;
                self.set_level(buy)?;
                self.set_level(sell)?;
                self.cycle_trade_count = self
                    .cycle_trade_count
                    .checked_add(1)
                    .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                self.cycle_executed_quantity = self
                    .cycle_executed_quantity
                    .checked_add(u128::from(print.quantity.lots()))
                    .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                match self.cycle_trade_price {
                    None => self.cycle_trade_price = Some(print.price),
                    Some(price) if price == print.price => {}
                    Some(_) => {
                        return Err(CallAuctionMarketDataError::InvalidUpdate(
                            "one auction cycle printed multiple prices",
                        ));
                    }
                }
            }
            CallAuctionMarketDataKind::PhaseChanged {
                auction_id,
                previous,
                current,
                revision,
            } => {
                let expected_revision = self
                    .phase
                    .revision()
                    .checked_add(1)
                    .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                if self.phase.phase() != previous || revision != expected_revision {
                    return Err(CallAuctionMarketDataError::InvalidUpdate(
                        "auction phase update is not the next transition",
                    ));
                }
                let (active, last) = transition_cycle(self.phase, auction_id, previous, current)?;
                self.phase = CallAuctionPhaseSnapshot::from_parts(current, revision, active, last);
                if current == CallAuctionPhase::Collecting {
                    self.reset_cycle_trace();
                }
            }
            CallAuctionMarketDataKind::UncrossCompleted {
                auction_id,
                clearing,
                trade_count,
                cancellation_count,
                book_revision,
                phase_revision,
            } => {
                let expected_book = self
                    .book_revision
                    .checked_add(1)
                    .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                let expected_phase = self
                    .phase
                    .revision()
                    .checked_add(1)
                    .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                if self.phase.phase() != CallAuctionPhase::Frozen
                    || self.phase.active_auction_id() != Some(auction_id)
                    || self.cycle_trade_count != trade_count
                    || self.cycle_cancellation_count != cancellation_count
                    || self.cycle_executed_quantity != clearing.executable_quantity()
                    || self.cycle_trade_price != Some(clearing.price())
                    || book_revision != expected_book
                    || phase_revision != expected_phase
                {
                    return Err(CallAuctionMarketDataError::InvalidUpdate(
                        "auction completion does not reconcile to its incremental trace",
                    ));
                }
                self.book_revision = book_revision;
                self.phase = CallAuctionPhaseSnapshot::from_parts(
                    CallAuctionPhase::Closed,
                    phase_revision,
                    None,
                    Some(auction_id),
                );
                self.reset_cycle_trace();
            }
            CallAuctionMarketDataKind::MassCancelCompleted {
                cancelled_order_count,
                cancelled_quantity_lots: _,
                book_revision,
            } => {
                let expected_book = if cancelled_order_count == 0 {
                    self.book_revision
                } else {
                    self.book_revision
                        .checked_add(1)
                        .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?
                };
                if book_revision != expected_book {
                    return Err(CallAuctionMarketDataError::InvalidUpdate(
                        "auction mass-cancel completion has an invalid book revision",
                    ));
                }
                self.book_revision = book_revision;
            }
        }
        self.last_event_sequence = update.sequence;
        Ok(())
    }

    fn validate_trade_transition(
        &self,
        print: CallAuctionTradePrint,
        next: CallAuctionMarketDataLevel,
    ) -> Result<(), CallAuctionMarketDataError> {
        let previous = self.aggregate(next.side, next.constraint).ok_or(
            CallAuctionMarketDataError::InvalidUpdate(
                "auction trade references an absent public aggregate",
            ),
        )?;
        let expected_quantity = previous
            .quantity
            .checked_sub(u128::from(print.quantity.lots()))
            .ok_or(CallAuctionMarketDataError::InvalidUpdate(
                "auction trade exceeds a public aggregate",
            ))?;
        let retained = next.order_count == previous.order_count;
        let removed = previous
            .order_count
            .checked_sub(1)
            .is_some_and(|count| next.order_count == count);
        if next.quantity != expected_quantity || (!retained && !removed) {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction trade does not reconcile to its aggregate transition",
            ));
        }
        Ok(())
    }

    fn validate_book_transition(
        &self,
        reason: CallAuctionBookChangeReason,
        quantity: Quantity,
        next: CallAuctionMarketDataLevel,
    ) -> Result<(), CallAuctionMarketDataError> {
        let previous = self
            .aggregate(next.side, next.constraint)
            .unwrap_or_default();
        let changed = u128::from(quantity.lots());
        let (expected_quantity, expected_count) = match reason {
            CallAuctionBookChangeReason::Accepted => (
                previous
                    .quantity
                    .checked_add(changed)
                    .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?,
                previous
                    .order_count
                    .checked_add(1)
                    .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?,
            ),
            CallAuctionBookChangeReason::UserCancelled
            | CallAuctionBookChangeReason::Replaced
            | CallAuctionBookChangeReason::MassCancelled
            | CallAuctionBookChangeReason::UncrossRemainder => (
                previous.quantity.checked_sub(changed).ok_or(
                    CallAuctionMarketDataError::InvalidUpdate(
                        "auction book removal exceeds its public aggregate",
                    ),
                )?,
                previous.order_count.checked_sub(1).ok_or(
                    CallAuctionMarketDataError::InvalidUpdate(
                        "auction book removal references an empty public aggregate",
                    ),
                )?,
            ),
            CallAuctionBookChangeReason::Amended => (
                previous.quantity.checked_sub(changed).ok_or(
                    CallAuctionMarketDataError::InvalidUpdate(
                        "auction amendment exceeds its public aggregate",
                    ),
                )?,
                previous.order_count,
            ),
        };
        if next.quantity != expected_quantity || next.order_count != expected_count {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction book change does not reconcile to its quantity and absolute aggregate",
            ));
        }
        Ok(())
    }

    fn aggregate(&self, side: Side, constraint: AuctionOrderConstraint) -> Option<Aggregate> {
        match (side, constraint) {
            (Side::Buy, AuctionOrderConstraint::Market) => Some(self.market_buy),
            (Side::Sell, AuctionOrderConstraint::Market) => Some(self.market_sell),
            (Side::Buy, AuctionOrderConstraint::Limit(price)) => self.bids.get(&price).copied(),
            (Side::Sell, AuctionOrderConstraint::Limit(price)) => self.asks.get(&price).copied(),
        }
        .filter(|aggregate| *aggregate != Aggregate::default())
    }

    fn set_level(
        &mut self,
        level: CallAuctionMarketDataLevel,
    ) -> Result<(), CallAuctionMarketDataError> {
        let aggregate = aggregate_from_level(level);
        match (level.side, level.constraint) {
            (Side::Buy, AuctionOrderConstraint::Market) => self.market_buy = aggregate,
            (Side::Sell, AuctionOrderConstraint::Market) => self.market_sell = aggregate,
            (Side::Buy, AuctionOrderConstraint::Limit(price)) => {
                set_bounded_limit_level(
                    &mut self.bids,
                    Side::Buy,
                    price,
                    aggregate,
                    self.limits.max_price_levels_per_side(),
                )?;
            }
            (Side::Sell, AuctionOrderConstraint::Limit(price)) => {
                set_bounded_limit_level(
                    &mut self.asks,
                    Side::Sell,
                    price,
                    aggregate,
                    self.limits.max_price_levels_per_side(),
                )?;
            }
        }
        Ok(())
    }

    fn apply_complete_batch<I>(&mut self, updates: I) -> Result<(), CallAuctionMarketDataError>
    where
        I: Clone + ExactSizeIterator<Item = CallAuctionMarketDataUpdate>,
    {
        let update_count = updates.len();
        if update_count == 0 {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "complete auction batch is empty",
            ));
        }
        if update_count > self.limits.max_batch_updates() {
            return Err(CallAuctionMarketDataError::CapacityExceeded {
                resource: CallAuctionMarketDataResource::BatchUpdates,
                maximum: self.limits.max_batch_updates(),
                attempted: update_count,
            });
        }
        let next_command_sequence = self
            .last_command_sequence
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        let mut expected = self
            .last_event_sequence
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        for (index, update) in updates.clone().enumerate() {
            self.preflight_identity(update)?;
            if update.sequence != expected {
                return Err(CallAuctionMarketDataError::SequenceGap {
                    expected,
                    actual: update.sequence,
                });
            }
            if index + 1 < update_count {
                expected = expected
                    .checked_add(1)
                    .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
            }
        }
        preflight_complete_batch_grammar(updates.clone())?;
        self.preflight_batch_level_capacity(updates.clone())?;
        for update in updates {
            if let Err(error) = self.apply_verified(update) {
                self.poisoned = true;
                return Err(error);
            }
        }
        self.last_command_sequence = next_command_sequence;
        Ok(())
    }

    fn preflight_batch_level_capacity<I>(
        &mut self,
        updates: I,
    ) -> Result<(), CallAuctionMarketDataError>
    where
        I: IntoIterator<Item = CallAuctionMarketDataUpdate>,
    {
        let result = preflight_replica_level_capacity(
            &mut self.batch_levels,
            &self.bids,
            &self.asks,
            self.limits.max_price_levels_per_side(),
            updates,
        );
        self.batch_levels.clear();
        result
    }

    fn levels(&self, side: Side) -> &AuctionDepthMap {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn validate_level_price(
        &self,
        level: CallAuctionMarketDataLevel,
    ) -> Result<(), CallAuctionMarketDataError> {
        if let AuctionOrderConstraint::Limit(price) = level.constraint {
            self.definition.price_rules().validate(price).map_err(|_| {
                CallAuctionMarketDataError::InvalidUpdate(
                    "auction market-data limit is off grid or outside the collar",
                )
            })?;
        }
        Ok(())
    }

    fn validate_snapshot_prices(
        &self,
        snapshot: &CallAuctionMarketDataSnapshot,
    ) -> Result<(), CallAuctionMarketDataError> {
        for level in snapshot.bids.iter().chain(&snapshot.asks) {
            if let AuctionOrderConstraint::Limit(price) = level.constraint {
                self.definition.price_rules().validate(price).map_err(|_| {
                    CallAuctionMarketDataError::InvalidSnapshot(
                        "auction snapshot limit is off grid or outside the collar",
                    )
                })?;
            }
        }
        Ok(())
    }

    fn advance_replica_trade_id(
        &mut self,
        actual: TradeId,
    ) -> Result<(), CallAuctionMarketDataError> {
        let expected = match self.last_trade_id {
            Some(previous) => previous
                .get()
                .checked_add(1)
                .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?,
            None => 1,
        };
        if actual.get() != expected {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction trade identifier is not contiguous",
            ));
        }
        self.last_trade_id = Some(actual);
        Ok(())
    }

    fn reset_cycle_trace(&mut self) {
        self.cycle_trade_count = 0;
        self.cycle_cancellation_count = 0;
        self.cycle_executed_quantity = 0;
        self.cycle_trade_price = None;
    }
}

fn preflight_complete_batch_grammar<I>(updates: I) -> Result<(), CallAuctionMarketDataError>
where
    I: ExactSizeIterator<Item = CallAuctionMarketDataUpdate>,
{
    let update_count = updates.len();
    let mut replacement_index = None;
    let mut first_occurred_at = None;
    let mut accepted_index = None;
    let mut timestamps_match = true;
    let mut mass_cancel_count = 0_usize;
    let mut mass_cancel_quantity = 0_u128;
    let mut mass_cancel_completion = None;
    for (index, update) in updates.enumerate() {
        if index == 0 {
            first_occurred_at = Some(update.occurred_at);
        } else if first_occurred_at != Some(update.occurred_at) {
            timestamps_match = false;
        }
        if let CallAuctionMarketDataKind::Book {
            reason, quantity, ..
        } = update.kind
        {
            match reason {
                CallAuctionBookChangeReason::Accepted => accepted_index = Some(index),
                CallAuctionBookChangeReason::Replaced => {
                    if replacement_index.replace(index).is_some() {
                        return Err(CallAuctionMarketDataError::InvalidUpdate(
                            "auction command batch repeats a replacement removal",
                        ));
                    }
                }
                CallAuctionBookChangeReason::MassCancelled => {
                    mass_cancel_count = mass_cancel_count
                        .checked_add(1)
                        .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                    mass_cancel_quantity = mass_cancel_quantity
                        .checked_add(u128::from(quantity.lots()))
                        .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                }
                CallAuctionBookChangeReason::UserCancelled
                | CallAuctionBookChangeReason::Amended
                | CallAuctionBookChangeReason::UncrossRemainder => {}
            }
        }
        if let CallAuctionMarketDataKind::MassCancelCompleted {
            cancelled_order_count,
            cancelled_quantity_lots,
            ..
        } = update.kind
        {
            if mass_cancel_completion
                .replace((index, cancelled_order_count, cancelled_quantity_lots))
                .is_some()
            {
                return Err(CallAuctionMarketDataError::InvalidUpdate(
                    "auction command batch repeats a mass-cancel completion",
                ));
            }
        }
        if replacement_index.is_some() && update.occurred_at != first_occurred_at.unwrap() {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction replacement batch timestamps differ",
            ));
        }
    }
    if replacement_index.is_some()
        && (update_count != 2 || replacement_index != Some(0) || accepted_index != Some(1))
    {
        return Err(CallAuctionMarketDataError::InvalidUpdate(
            "auction replacement must be Replaced then Accepted in one two-update batch",
        ));
    }
    if mass_cancel_count != 0 || mass_cancel_completion.is_some() {
        let Some((completion_index, declared_count, declared_quantity)) = mass_cancel_completion
        else {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction mass-cancel removals lack a final completion",
            ));
        };
        let observed_count = u64::try_from(mass_cancel_count)
            .map_err(|_| CallAuctionMarketDataError::ArithmeticOverflow)?;
        if completion_index.checked_add(1) != Some(update_count)
            || mass_cancel_count.checked_add(1) != Some(update_count)
            || observed_count != declared_count
            || mass_cancel_quantity != declared_quantity
            || !timestamps_match
        {
            return Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction mass cancel must contain only reconciled removals followed by one completion",
            ));
        }
    }
    Ok(())
}

fn validate_update_kind(
    sequence: u64,
    kind: CallAuctionMarketDataKind,
) -> Result<(), CallAuctionMarketDataError> {
    match kind {
        CallAuctionMarketDataKind::NoPublicChange => Ok(()),
        CallAuctionMarketDataKind::Book { level, .. } => validate_level_shape(level),
        CallAuctionMarketDataKind::Trade { print, buy, sell } => {
            validate_level_shape(buy)?;
            validate_level_shape(sell)?;
            if print.trade_id.get() > sequence || buy.side != Side::Buy || sell.side != Side::Sell {
                return Err(CallAuctionMarketDataError::InvalidUpdate(
                    "auction trade print has an infeasible identifier or side update",
                ));
            }
            Ok(())
        }
        CallAuctionMarketDataKind::PhaseChanged {
            previous,
            current,
            revision,
            ..
        } => {
            if previous == current || revision == 0 || revision > sequence {
                return Err(CallAuctionMarketDataError::InvalidUpdate(
                    "auction phase update is unchanged or has an infeasible revision",
                ));
            }
            Ok(())
        }
        CallAuctionMarketDataKind::UncrossCompleted {
            clearing,
            trade_count,
            book_revision,
            phase_revision,
            ..
        } => {
            if clearing.executable_quantity() == 0
                || trade_count == 0
                || book_revision == 0
                || phase_revision == 0
                || phase_revision > sequence
            {
                return Err(CallAuctionMarketDataError::InvalidUpdate(
                    "auction completion contains an infeasible clearing or revision",
                ));
            }
            Ok(())
        }
        CallAuctionMarketDataKind::MassCancelCompleted {
            cancelled_order_count,
            cancelled_quantity_lots,
            book_revision,
        } => {
            if (cancelled_order_count == 0) != (cancelled_quantity_lots == 0)
                || book_revision > sequence
            {
                return Err(CallAuctionMarketDataError::InvalidUpdate(
                    "auction mass-cancel completion contains inconsistent totals or revision",
                ));
            }
            Ok(())
        }
    }
}

fn validate_level_shape(
    level: CallAuctionMarketDataLevel,
) -> Result<(), CallAuctionMarketDataError> {
    if (level.quantity == 0) != (level.order_count == 0) {
        return Err(CallAuctionMarketDataError::InvalidUpdate(
            "auction level quantity and order count disagree",
        ));
    }
    Ok(())
}

fn validate_snapshot_market(
    expected_side: Side,
    level: CallAuctionMarketDataLevel,
) -> Result<(), CallAuctionMarketDataError> {
    if level.side != expected_side
        || level.constraint != AuctionOrderConstraint::Market
        || (level.quantity == 0) != (level.order_count == 0)
    {
        return Err(CallAuctionMarketDataError::InvalidSnapshot(
            "auction snapshot market aggregate is structurally invalid",
        ));
    }
    Ok(())
}

fn validate_snapshot_side(
    expected_side: Side,
    levels: &[CallAuctionMarketDataLevel],
) -> Result<(), CallAuctionMarketDataError> {
    let mut previous: Option<Price> = None;
    for level in levels {
        let AuctionOrderConstraint::Limit(price) = level.constraint else {
            return Err(CallAuctionMarketDataError::InvalidSnapshot(
                "auction limit-depth image contains market interest",
            ));
        };
        if level.side != expected_side || level.quantity == 0 || level.order_count == 0 {
            return Err(CallAuctionMarketDataError::InvalidSnapshot(
                "auction limit-depth level is empty or on the wrong side",
            ));
        }
        if let Some(previous_price) = previous {
            let ordered = match expected_side {
                Side::Buy => previous_price > price,
                Side::Sell => previous_price < price,
            };
            if !ordered {
                return Err(CallAuctionMarketDataError::InvalidSnapshot(
                    "auction limit-depth levels are not strictly market ordered",
                ));
            }
        }
        previous = Some(price);
    }
    Ok(())
}

fn validate_phase_snapshot(
    phase: CallAuctionPhaseSnapshot,
    command_sequence: u64,
) -> Result<(), CallAuctionMarketDataError> {
    if phase.revision() > command_sequence
        || (phase.phase() == CallAuctionPhase::Closed) != phase.active_auction_id().is_none()
        || phase
            .active_auction_id()
            .is_some_and(|active| Some(active) != phase.last_auction_id())
        || (phase.revision() == 0
            && (phase.phase() != CallAuctionPhase::Closed
                || phase.active_auction_id().is_some()
                || phase.last_auction_id().is_some()))
    {
        return Err(CallAuctionMarketDataError::InvalidSnapshot(
            "auction snapshot phase, cycle, revision, and command boundary disagree",
        ));
    }
    Ok(())
}

fn transition_cycle(
    phase: CallAuctionPhaseSnapshot,
    auction_id: AuctionId,
    previous: CallAuctionPhase,
    current: CallAuctionPhase,
) -> Result<(Option<AuctionId>, Option<AuctionId>), CallAuctionMarketDataError> {
    if phase.phase() != previous || previous == current {
        return Err(CallAuctionMarketDataError::InvalidUpdate(
            "auction phase transition source is invalid",
        ));
    }
    match (previous, current) {
        (CallAuctionPhase::Closed, CallAuctionPhase::Collecting) => {
            let expected = match phase.last_auction_id() {
                Some(last) => last.get().checked_add(1),
                None => Some(1),
            };
            if expected != Some(auction_id.get()) {
                return Err(CallAuctionMarketDataError::InvalidUpdate(
                    "auction cycle identity is not the exact successor",
                ));
            }
            Ok((Some(auction_id), Some(auction_id)))
        }
        (CallAuctionPhase::Collecting, CallAuctionPhase::Frozen)
        | (CallAuctionPhase::Frozen, CallAuctionPhase::Collecting) => {
            if phase.active_auction_id() != Some(auction_id) {
                return Err(CallAuctionMarketDataError::InvalidUpdate(
                    "auction transition references another active cycle",
                ));
            }
            Ok((Some(auction_id), phase.last_auction_id()))
        }
        (CallAuctionPhase::Collecting | CallAuctionPhase::Frozen, CallAuctionPhase::Closed) => {
            if phase.active_auction_id() != Some(auction_id) {
                return Err(CallAuctionMarketDataError::InvalidUpdate(
                    "auction closure references another active cycle",
                ));
            }
            Ok((None, phase.last_auction_id()))
        }
        _ => Err(CallAuctionMarketDataError::InvalidUpdate(
            "auction phase transition is outside the explicit graph",
        )),
    }
}

fn validate_report_outcome(
    report: &CallAuctionExecutionReport,
) -> Result<(), CallAuctionMarketDataError> {
    if report
        .events
        .iter()
        .zip(report.events.iter().skip(1))
        .any(|(left, right)| {
            left.sequence
                .checked_add(1)
                .is_none_or(|expected| right.sequence != expected)
        })
    {
        return Err(CallAuctionMarketDataError::TraceMismatch(
            "auction report event sequences are not contiguous",
        ));
    }
    match report.outcome {
        CallAuctionCommandOutcome::Accepted => {
            if report
                .events
                .iter()
                .any(|event| matches!(event.kind, CallAuctionEventKind::CommandRejected(_)))
            {
                return Err(CallAuctionMarketDataError::TraceMismatch(
                    "accepted auction report contains a rejection",
                ));
            }
        }
        CallAuctionCommandOutcome::Rejected(reason) => {
            if report.events.len() != 1
                || report.events[0].kind != CallAuctionEventKind::CommandRejected(reason)
            {
                return Err(CallAuctionMarketDataError::TraceMismatch(
                    "rejected auction report does not contain exactly its rejection",
                ));
            }
        }
    }
    Ok(())
}

fn command_instrument(command: CallAuctionCommand) -> InstrumentId {
    match command {
        CallAuctionCommand::PhaseControl(value) => value.instrument_id,
        CallAuctionCommand::Submit(value) => value.order.instrument_id(),
        CallAuctionCommand::Cancel(value) => value.instrument_id,
        CallAuctionCommand::MassCancel(value) => value.instrument_id,
        CallAuctionCommand::Amend(value) => value.instrument_id,
        CallAuctionCommand::Replace(value) => value.replacement.instrument_id(),
        CallAuctionCommand::Uncross(value) => value.instrument_id,
    }
}

fn command_version(command: CallAuctionCommand) -> InstrumentVersion {
    match command {
        CallAuctionCommand::PhaseControl(value) => value.instrument_version,
        CallAuctionCommand::Submit(value) => value.order.instrument_version(),
        CallAuctionCommand::Cancel(value) => value.instrument_version,
        CallAuctionCommand::MassCancel(value) => value.instrument_version,
        CallAuctionCommand::Amend(value) => value.instrument_version,
        CallAuctionCommand::Replace(value) => value.replacement.instrument_version(),
        CallAuctionCommand::Uncross(value) => value.instrument_version,
    }
}

fn level_from_aggregate(
    side: Side,
    constraint: AuctionOrderConstraint,
    aggregate: Aggregate,
) -> Result<CallAuctionMarketDataLevel, CallAuctionMarketDataError> {
    CallAuctionMarketDataLevel::from_parts(
        side,
        constraint,
        aggregate.quantity,
        aggregate.order_count,
    )
}

const fn aggregate_from_level(level: CallAuctionMarketDataLevel) -> Aggregate {
    Aggregate {
        quantity: level.quantity,
        order_count: level.order_count,
    }
}

fn aggregate_market(engine: &CallAuctionEngine, side: Side) -> Aggregate {
    Aggregate {
        quantity: engine.book().market_quantity(side),
        order_count: engine.book().market_order_count(side),
    }
}

fn previous_trade_id(next_trade_id: u64) -> Result<Option<TradeId>, CallAuctionMarketDataError> {
    match next_trade_id.checked_sub(1) {
        Some(0) => Ok(None),
        Some(value) => TradeId::new(value).map(Some).map_err(|_| {
            CallAuctionMarketDataError::SourceDivergence("auction next trade identifier is invalid")
        }),
        None => Err(CallAuctionMarketDataError::SourceDivergence(
            "auction next trade identifier is zero",
        )),
    }
}

fn previous_sequence(next: u64, detail: &'static str) -> Result<u64, CallAuctionMarketDataError> {
    next.checked_sub(1)
        .ok_or(CallAuctionMarketDataError::SourceDivergence(detail))
}

fn try_depth_map(
    maximum: usize,
    resource: CallAuctionMarketDataResource,
) -> Result<AuctionDepthMap, CallAuctionMarketDataConstructionError> {
    IndexedAvlMap::try_with_capacity(maximum).map_err(|_| {
        CallAuctionMarketDataConstructionError::ReservationFailed { resource, maximum }
    })
}

fn try_hash_map<K, V>(
    maximum: usize,
    resource: CallAuctionMarketDataResource,
) -> Result<BoundedHashMap<K, V>, CallAuctionMarketDataConstructionError>
where
    K: Eq + Hash,
{
    BoundedHashMap::try_new(maximum).map_err(|_: BoundedHashError| {
        CallAuctionMarketDataConstructionError::ReservationFailed { resource, maximum }
    })
}

fn ensure_source_limits(
    selected: CallAuctionMarketDataLimits,
    source: CallAuctionEngineLimits,
) -> Result<(), CallAuctionMarketDataConstructionError> {
    let requirements = [
        (
            CallAuctionMarketDataResource::TrackedOrders,
            selected.max_active_orders(),
            source.book().max_active_orders(),
        ),
        (
            CallAuctionMarketDataResource::BidPriceLevels,
            selected.max_price_levels_per_side(),
            source.book().max_price_levels_per_side(),
        ),
        (
            CallAuctionMarketDataResource::BatchUpdates,
            selected.max_batch_updates(),
            source.max_report_events(),
        ),
    ];
    for (resource, selected, required) in requirements {
        if selected < required {
            return Err(CallAuctionMarketDataConstructionError::LimitsBelowSource {
                resource,
                selected,
                required,
            });
        }
    }
    Ok(())
}

const fn side_resource(side: Side) -> CallAuctionMarketDataResource {
    match side {
        Side::Buy => CallAuctionMarketDataResource::BidPriceLevels,
        Side::Sell => CallAuctionMarketDataResource::AskPriceLevels,
    }
}

fn set_bounded_limit_level(
    levels: &mut AuctionDepthMap,
    side: Side,
    price: Price,
    aggregate: Aggregate,
    maximum: usize,
) -> Result<(), CallAuctionMarketDataError> {
    if aggregate == Aggregate::default() {
        levels.remove(&price);
    } else {
        if levels.get(&price).is_none() && levels.len() >= maximum {
            return Err(CallAuctionMarketDataError::CapacityExceeded {
                resource: side_resource(side),
                maximum,
                attempted: levels.len().saturating_add(1),
            });
        }
        levels.insert(price, aggregate);
    }
    Ok(())
}

const fn update_levels(
    update: CallAuctionMarketDataUpdate,
) -> [Option<CallAuctionMarketDataLevel>; 2] {
    match update.kind {
        CallAuctionMarketDataKind::Book { level, .. } => [Some(level), None],
        CallAuctionMarketDataKind::Trade { buy, sell, .. } => [Some(buy), Some(sell)],
        CallAuctionMarketDataKind::NoPublicChange
        | CallAuctionMarketDataKind::PhaseChanged { .. }
        | CallAuctionMarketDataKind::UncrossCompleted { .. }
        | CallAuctionMarketDataKind::MassCancelCompleted { .. } => [None, None],
    }
}

fn preflight_replica_level_capacity(
    scratch: &mut BoundedHashMap<(Side, Price), bool>,
    bids: &AuctionDepthMap,
    asks: &AuctionDepthMap,
    maximum: usize,
    updates: impl IntoIterator<Item = CallAuctionMarketDataUpdate>,
) -> Result<(), CallAuctionMarketDataError> {
    let mut bid_count = bids.len();
    let mut ask_count = asks.len();
    for update in updates {
        for level in update_levels(update).into_iter().flatten() {
            let AuctionOrderConstraint::Limit(price) = level.constraint else {
                continue;
            };
            let key = (level.side, price);
            let previous = scratch.get(&key).copied().unwrap_or_else(|| {
                let levels = match level.side {
                    Side::Buy => bids,
                    Side::Sell => asks,
                };
                levels.get(&price).is_some()
            });
            let next = level.quantity != 0;
            let count = match level.side {
                Side::Buy => &mut bid_count,
                Side::Sell => &mut ask_count,
            };
            match (previous, next) {
                (false, true) => {
                    *count = count
                        .checked_add(1)
                        .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
                }
                (true, false) => {
                    *count =
                        count
                            .checked_sub(1)
                            .ok_or(CallAuctionMarketDataError::InvalidUpdate(
                                "auction replica level-capacity preflight underflowed",
                            ))?;
                }
                (false, false) | (true, true) => {}
            }
            if *count > maximum {
                return Err(CallAuctionMarketDataError::CapacityExceeded {
                    resource: side_resource(level.side),
                    maximum,
                    attempted: *count,
                });
            }
            scratch.try_insert(key, next).map_err(|_| {
                CallAuctionMarketDataError::CapacityExceeded {
                    resource: CallAuctionMarketDataResource::BatchLevelIdentities,
                    maximum: scratch.maximum(),
                    attempted: scratch.len().saturating_add(1),
                }
            })?;
        }
    }
    Ok(())
}

fn auction_depth_matches(
    mirror: &AuctionDepthMap,
    source: impl ExactSizeIterator<Item = CallAuctionLevelSnapshot>,
) -> bool {
    mirror.len() == source.len()
        && mirror
            .iter()
            .zip(source)
            .all(|((&price, aggregate), level)| {
                price == level.price()
                    && aggregate.quantity == level.quantity()
                    && aggregate.order_count == level.order_count()
            })
}

fn arena_status(levels: &AuctionDepthMap) -> CallAuctionMarketDataArenaStatus {
    CallAuctionMarketDataArenaStatus {
        configured_slots: levels.maximum(),
        allocated_slots: levels.allocation_capacity(),
        initialized_slots: levels.storage_len(),
        occupied_slots: levels.len(),
        reusable_slots: levels.storage_len().saturating_sub(levels.len()),
    }
}

fn hash_status<K, V>(index: &BoundedHashMap<K, V>) -> CallAuctionMarketDataHashIndexStatus
where
    K: Eq + Hash,
{
    CallAuctionMarketDataHashIndexStatus {
        configured_entries: index.maximum(),
        allocated_entries: index.capacity(),
        initialized_buckets: index.bucket_count(),
        occupied_entries: index.len(),
    }
}

fn limit_price(level: CallAuctionMarketDataLevel) -> Price {
    match level.constraint {
        AuctionOrderConstraint::Limit(price) => price,
        AuctionOrderConstraint::Market => unreachable!("validated snapshot limit"),
    }
}

#[cfg(test)]
mod tests {
    use crate::domain::{AssetId, InstrumentId, InstrumentVersion};
    use crate::instrument::{
        InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules, QuantityRules,
        ReserveOrderRules, TradingState,
    };

    use super::*;

    fn definition() -> InstrumentDefinition {
        InstrumentDefinition::new(InstrumentSpec {
            instrument_id: InstrumentId::new(1).unwrap(),
            version: InstrumentVersion::new(1).unwrap(),
            effective_from: TimestampNs::from_unix_nanos(0),
            symbol: InstrumentSymbol::new("AUCT-MD-UNIT").unwrap(),
            kind: InstrumentKind::Spot,
            base_asset_id: AssetId::new(1).unwrap(),
            quote_asset_id: AssetId::new(2).unwrap(),
            price: PriceRules::new(0, 1, Price::from_raw(0), Price::from_raw(1_000)).unwrap(),
            quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
            reserve: ReserveOrderRules::disabled(),
            hidden_orders_supported: false,
            base_units_per_lot: 1,
            quote_units_per_price_unit: 1,
            trading_state: TradingState::Halted,
        })
        .unwrap()
    }

    #[test]
    fn invariant_validation_rejects_lost_arena_and_scratch_reservations() {
        let spec = CallAuctionMarketDataLimitsSpec {
            max_active_orders: 1,
            max_price_levels_per_side: 1,
            max_batch_updates: 3,
        };
        let mut lost_arena =
            CallAuctionMarketDataReplica::try_with_limits(definition(), spec).unwrap();
        lost_arena.bids.shrink_to_fit();
        assert!(matches!(
            lost_arena.validate(),
            Err(CallAuctionMarketDataError::SourceDivergence(
                "auction replica price arena contradicts its configured limit"
            ))
        ));

        let mut lost_scratch =
            CallAuctionMarketDataReplica::try_with_limits(definition(), spec).unwrap();
        lost_scratch.batch_levels.shrink_to_fit();
        assert!(matches!(
            lost_scratch.validate(),
            Err(CallAuctionMarketDataError::SourceDivergence(
                "auction replica batch scratch contradicts its fixed empty layout"
            ))
        ));
    }

    #[test]
    fn final_representable_event_sequence_applies_without_requiring_a_successor() {
        let definition = definition();
        let mut replica = CallAuctionMarketDataReplica::try_with_limits(
            definition,
            CallAuctionMarketDataLimitsSpec {
                max_active_orders: 1,
                max_price_levels_per_side: 1,
                max_batch_updates: 3,
            },
        )
        .unwrap();
        replica.last_event_sequence = u64::MAX - 1;
        let batch = CallAuctionMarketDataBatch {
            updates: vec![CallAuctionMarketDataUpdate {
                instrument_id: definition.instrument_id(),
                instrument_version: definition.version(),
                sequence: u64::MAX,
                occurred_at: TimestampNs::from_unix_nanos(u64::MAX),
                kind: CallAuctionMarketDataKind::NoPublicChange,
            }],
            replayed: false,
        };

        replica.apply_batch(&batch).unwrap();
        assert_eq!(replica.last_sequence(), u64::MAX);
        assert_eq!(replica.last_command_sequence(), 1);
        replica.validate().unwrap();
    }

    #[test]
    fn mass_cancel_batch_preflight_rejects_incomplete_or_inconsistent_aggregates() {
        let definition = definition();
        let removal = CallAuctionMarketDataUpdate {
            instrument_id: definition.instrument_id(),
            instrument_version: definition.version(),
            sequence: 1,
            occurred_at: TimestampNs::from_unix_nanos(7),
            kind: CallAuctionMarketDataKind::Book {
                reason: CallAuctionBookChangeReason::MassCancelled,
                quantity: Quantity::new(2).unwrap(),
                level: CallAuctionMarketDataLevel {
                    side: Side::Buy,
                    constraint: AuctionOrderConstraint::Market,
                    quantity: 0,
                    order_count: 0,
                },
            },
        };
        let completion = CallAuctionMarketDataUpdate {
            instrument_id: definition.instrument_id(),
            instrument_version: definition.version(),
            sequence: 2,
            occurred_at: TimestampNs::from_unix_nanos(7),
            kind: CallAuctionMarketDataKind::MassCancelCompleted {
                cancelled_order_count: 1,
                cancelled_quantity_lots: 2,
                book_revision: 1,
            },
        };

        preflight_complete_batch_grammar([removal, completion].into_iter()).unwrap();
        assert!(matches!(
            preflight_complete_batch_grammar([removal].into_iter()),
            Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction mass-cancel removals lack a final completion"
            ))
        ));

        let mut wrong_count = completion;
        wrong_count.kind = CallAuctionMarketDataKind::MassCancelCompleted {
            cancelled_order_count: 2,
            cancelled_quantity_lots: 2,
            book_revision: 1,
        };
        assert!(matches!(
            preflight_complete_batch_grammar([removal, wrong_count].into_iter()),
            Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction mass cancel must contain only reconciled removals followed by one completion"
            ))
        ));

        let mut wrong_time = completion;
        wrong_time.occurred_at = TimestampNs::from_unix_nanos(8);
        assert!(matches!(
            preflight_complete_batch_grammar([removal, wrong_time].into_iter()),
            Err(CallAuctionMarketDataError::InvalidUpdate(
                "auction mass cancel must contain only reconciled removals followed by one completion"
            ))
        ));
    }
}
