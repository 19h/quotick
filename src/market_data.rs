//! Deterministic, anonymized level-2 market-data publication and recovery.
//!
//! Every matching event produces exactly one market-data update with the same
//! sequence. Book-changing updates carry absolute aggregate level state, so a
//! consumer never needs to infer an order-level delta. Full-depth snapshots
//! establish a recovery boundary after a detected sequence gap.
//!
//! Publisher order/control mirrors, both ordered depth sides, affected-level
//! validation scratch, replica active/standby depth, and replica batch scratch
//! are finitely bounded and completely reserved before usable state exists.
//! Incremental state mutation never grows or rehashes authoritative storage.

use std::fmt;
use std::hash::Hash;

use crate::bounded_hash::{BoundedHashError, BoundedHashMap};
use crate::domain::{
    AccountId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs, TradeId,
};
use crate::indexed_avl::IndexedAvlMap;
use crate::instrument::TradingState;
use crate::matching::{
    AccountAdmissionState, AccountControlAction, AccountControlSnapshot, CancelReason, Command,
    CommandOutcome, Event, EventKind, ExecutionReport, LevelSnapshot, MassCancelScope, OrderBook,
    OrderBookLimits, OrderDisplay, OrderType, SelfTradePrevention, Trade,
    TradingStateControlAction, TradingStateSnapshot,
};

/// One finite market-data state or preparation resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataResource {
    /// Resting-order identities mirrored by a publisher.
    TrackedOrders,
    /// Occupied bid prices.
    BidPriceLevels,
    /// Occupied ask prices.
    AskPriceLevels,
    /// Revisioned account-control state mirrored by a publisher.
    AccountControls,
    /// Updates emitted or consumed by one command batch.
    BatchUpdates,
    /// Unique level identities used for nonmutating replica capacity preflight.
    BatchLevelIdentities,
}

impl fmt::Display for MarketDataResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TrackedOrders => "tracked orders",
            Self::BidPriceLevels => "bid price levels",
            Self::AskPriceLevels => "ask price levels",
            Self::AccountControls => "account controls",
            Self::BatchUpdates => "batch updates",
            Self::BatchLevelIdentities => "batch level identities",
        })
    }
}

/// Raw construction values for bounded market-data publisher and replica state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketDataLimitsSpec {
    /// Maximum resting orders mirrored by one publisher.
    pub max_active_orders: usize,
    /// Maximum occupied prices independently on each side.
    pub max_price_levels_per_side: usize,
    /// Maximum revisioned account controls mirrored by one publisher.
    pub max_account_controls: usize,
    /// Maximum updates in one matching-command publication batch.
    pub max_batch_updates: usize,
}

impl Default for MarketDataLimitsSpec {
    fn default() -> Self {
        Self {
            max_active_orders: OrderBookLimits::DEFAULT_MAX_ACTIVE_ORDERS,
            max_price_levels_per_side: OrderBookLimits::DEFAULT_MAX_PRICE_LEVELS_PER_SIDE,
            max_account_controls: OrderBookLimits::DEFAULT_MAX_ACCOUNT_CONTROLS,
            max_batch_updates: OrderBookLimits::DEFAULT_MAX_REPORT_EVENTS,
        }
    }
}

/// Invalid finite market-data resource policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataLimitsError {
    /// Resting-order maximum is zero.
    ZeroActiveOrders,
    /// Per-side price-level maximum is zero.
    ZeroPriceLevels,
    /// Account-control maximum is zero.
    ZeroAccountControls,
    /// Per-batch update maximum is zero.
    ZeroBatchUpdates,
    /// More prices per side than total active orders were configured.
    PriceLevelsExceedActiveOrders,
    /// One maximum-size aggregate cancellation cannot fit in a publication batch.
    BatchUpdatesBelowMassCancelMaximum,
}

impl fmt::Display for MarketDataLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroActiveOrders => "market-data active-order limit is zero",
            Self::ZeroPriceLevels => "market-data per-side price-level limit is zero",
            Self::ZeroAccountControls => "market-data account-control limit is zero",
            Self::ZeroBatchUpdates => "market-data batch-update limit is zero",
            Self::PriceLevelsExceedActiveOrders => {
                "market-data per-side price-level limit exceeds its active-order limit"
            }
            Self::BatchUpdatesBelowMassCancelMaximum => {
                "market-data batch-update limit cannot hold a maximum-size mass cancellation"
            }
        })
    }
}

impl std::error::Error for MarketDataLimitsError {}

/// Validated immutable finite resource policy for market-data state.
#[allow(
    clippy::struct_field_names,
    reason = "the max prefix distinguishes immutable limits from observed cardinalities"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketDataLimits {
    max_active_orders: usize,
    max_price_levels_per_side: usize,
    max_account_controls: usize,
    max_batch_updates: usize,
}

impl MarketDataLimits {
    /// Validates one market-data resource policy.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataLimitsError`] for a zero or contradictory bound.
    pub const fn new(spec: MarketDataLimitsSpec) -> Result<Self, MarketDataLimitsError> {
        if spec.max_active_orders == 0 {
            return Err(MarketDataLimitsError::ZeroActiveOrders);
        }
        if spec.max_price_levels_per_side == 0 {
            return Err(MarketDataLimitsError::ZeroPriceLevels);
        }
        if spec.max_account_controls == 0 {
            return Err(MarketDataLimitsError::ZeroAccountControls);
        }
        if spec.max_batch_updates == 0 {
            return Err(MarketDataLimitsError::ZeroBatchUpdates);
        }
        if spec.max_price_levels_per_side > spec.max_active_orders {
            return Err(MarketDataLimitsError::PriceLevelsExceedActiveOrders);
        }
        let Some(maximum_mass_cancel_updates) = spec.max_active_orders.checked_add(1) else {
            return Err(MarketDataLimitsError::BatchUpdatesBelowMassCancelMaximum);
        };
        if spec.max_batch_updates < maximum_mass_cancel_updates {
            return Err(MarketDataLimitsError::BatchUpdatesBelowMassCancelMaximum);
        }
        Ok(Self {
            max_active_orders: spec.max_active_orders,
            max_price_levels_per_side: spec.max_price_levels_per_side,
            max_account_controls: spec.max_account_controls,
            max_batch_updates: spec.max_batch_updates,
        })
    }

    /// Derives the exact mirror envelope of one matching shard.
    #[must_use]
    pub const fn from_order_book(limits: OrderBookLimits) -> Self {
        Self {
            max_active_orders: limits.max_active_orders(),
            max_price_levels_per_side: limits.max_price_levels_per_side(),
            max_account_controls: limits.max_account_controls(),
            max_batch_updates: limits.max_report_events(),
        }
    }

    /// Maximum publisher-mirrored active orders.
    #[must_use]
    pub const fn max_active_orders(self) -> usize {
        self.max_active_orders
    }

    /// Maximum occupied prices independently on each side.
    #[must_use]
    pub const fn max_price_levels_per_side(self) -> usize {
        self.max_price_levels_per_side
    }

    /// Maximum publisher-mirrored account controls.
    #[must_use]
    pub const fn max_account_controls(self) -> usize {
        self.max_account_controls
    }

    /// Maximum updates in one command batch.
    #[must_use]
    pub const fn max_batch_updates(self) -> usize {
        self.max_batch_updates
    }

    /// Returns the corresponding raw specification.
    #[must_use]
    pub const fn spec(self) -> MarketDataLimitsSpec {
        MarketDataLimitsSpec {
            max_active_orders: self.max_active_orders,
            max_price_levels_per_side: self.max_price_levels_per_side,
            max_account_controls: self.max_account_controls,
            max_batch_updates: self.max_batch_updates,
        }
    }
}

impl Default for MarketDataLimits {
    fn default() -> Self {
        Self::new(MarketDataLimitsSpec::default())
            .expect("default market-data limits must be valid")
    }
}

impl TryFrom<MarketDataLimitsSpec> for MarketDataLimits {
    type Error = MarketDataLimitsError;

    fn try_from(spec: MarketDataLimitsSpec) -> Result<Self, Self::Error> {
        Self::new(spec)
    }
}

/// Failure before a publisher or replica owns usable state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataConstructionError {
    /// The selected finite resource policy is invalid.
    InvalidLimits(MarketDataLimitsError),
    /// A publisher policy is smaller than its authoritative matching shard.
    LimitsBelowSource {
        /// Insufficient resource.
        resource: MarketDataResource,
        /// Selected maximum.
        selected: usize,
        /// Required source maximum.
        required: usize,
    },
    /// Complete constructor-owned storage could not be reserved.
    ReservationFailed {
        /// Resource whose storage could not be represented or allocated.
        resource: MarketDataResource,
        /// Requested semantic maximum.
        maximum: usize,
    },
    /// The source book failed publisher bootstrap validation.
    Source(MarketDataError),
}

impl fmt::Display for MarketDataConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits(error) => error.fmt(formatter),
            Self::LimitsBelowSource {
                resource,
                selected,
                required,
            } => write!(
                formatter,
                "market-data {resource} limit {selected} is below source requirement {required}"
            ),
            Self::ReservationFailed { resource, maximum } => write!(
                formatter,
                "failed to reserve market-data {resource} storage through {maximum} entries"
            ),
            Self::Source(error) => write!(formatter, "market-data source is invalid: {error}"),
        }
    }
}

impl std::error::Error for MarketDataConstructionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidLimits(error) => Some(error),
            Self::Source(error) => Some(error),
            Self::LimitsBelowSource { .. } | Self::ReservationFailed { .. } => None,
        }
    }
}

impl From<MarketDataLimitsError> for MarketDataConstructionError {
    fn from(error: MarketDataLimitsError) -> Self {
        Self::InvalidLimits(error)
    }
}

/// An absolute aggregate state for one publicly visible price level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketDataLevel {
    /// Book side.
    pub side: Side,
    /// Price in instrument-defined quanta.
    pub price: Price,
    /// Aggregate visible leaves quantity. Zero means delete the level.
    pub quantity: u128,
    /// Number of visible orders. Zero means delete the level.
    pub order_count: u64,
}

impl MarketDataLevel {
    pub(crate) fn new(
        side: Side,
        price: Price,
        quantity: u128,
        order_count: u64,
    ) -> Result<Self, MarketDataError> {
        if (quantity == 0) != (order_count == 0) {
            return Err(MarketDataError::InvalidUpdate(
                "level quantity and order count must be zero or non-zero together",
            ));
        }
        Ok(Self {
            side,
            price,
            quantity,
            order_count,
        })
    }
}

/// An anonymized public execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TradePrint {
    /// Monotonic trade identifier within the instrument shard.
    pub trade_id: TradeId,
    /// Resting-order execution price.
    pub price: Price,
    /// Executed quantity.
    pub quantity: Quantity,
    /// Side that removed liquidity.
    pub aggressor_side: Side,
}

/// One public update corresponding to exactly one matching event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataKind {
    /// The source sequence advanced without changing public depth or printing a trade.
    NoBookChange,
    /// One aggregate level changed.
    Level(MarketDataLevel),
    /// A trade printed and its maker level changed atomically.
    Trade {
        /// Anonymized execution.
        print: TradePrint,
        /// Absolute maker-level state after the execution.
        maker_level: MarketDataLevel,
    },
    /// The effective instrument-wide trading state changed.
    TradingState {
        /// Effective state before the transition.
        previous_state: TradingState,
        /// Effective state after the transition.
        current_state: TradingState,
        /// Monotonically increasing state-control revision.
        revision: u64,
    },
}

/// A source-sequenced public market-data update.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketDataUpdate {
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    sequence: u64,
    occurred_at: TimestampNs,
    kind: MarketDataKind,
}

impl MarketDataUpdate {
    pub(crate) fn from_parts(
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        sequence: u64,
        occurred_at: TimestampNs,
        kind: MarketDataKind,
    ) -> Result<Self, MarketDataError> {
        if sequence == 0 {
            return Err(MarketDataError::InvalidUpdate(
                "market-data sequence must be non-zero",
            ));
        }
        if let MarketDataKind::Trade { print, maker_level } = kind {
            if print.aggressor_side == maker_level.side
                || print.price != maker_level.price
                || print.trade_id.get() > sequence
            {
                return Err(MarketDataError::InvalidUpdate(
                    "trade print contradicts its maker-level update",
                ));
            }
        }
        if let MarketDataKind::TradingState {
            previous_state,
            current_state,
            revision,
        } = kind
        {
            if previous_state == current_state || revision == 0 || revision > sequence {
                return Err(MarketDataError::InvalidUpdate(
                    "trading-state update must change state at a feasible non-zero revision",
                ));
            }
        }
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

    /// Returns the matching-event sequence.
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
    pub const fn kind(self) -> MarketDataKind {
        self.kind
    }
}

/// A contiguous publication result for one matching command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataBatch {
    updates: Vec<MarketDataUpdate>,
    replayed: bool,
}

impl MarketDataBatch {
    /// Returns the source updates in ascending contiguous sequence order.
    #[must_use]
    pub fn updates(&self) -> &[MarketDataUpdate] {
        &self.updates
    }

    /// Returns whether an exact matching retry was suppressed.
    #[must_use]
    pub const fn replayed(&self) -> bool {
        self.replayed
    }

    /// Returns the first source sequence, if this batch is not empty.
    #[must_use]
    pub fn first_sequence(&self) -> Option<u64> {
        self.updates.first().map(|update| update.sequence)
    }

    /// Returns the final source sequence, if this batch is not empty.
    #[must_use]
    pub fn last_sequence(&self) -> Option<u64> {
        self.updates.last().map(|update| update.sequence)
    }
}

/// A complete public depth image at one source sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarketDataSnapshot {
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    as_of_sequence: u64,
    last_trade_id: Option<TradeId>,
    trading_state: TradingStateSnapshot,
    bids: Vec<MarketDataLevel>,
    asks: Vec<MarketDataLevel>,
}

impl MarketDataSnapshot {
    pub(crate) fn from_parts(
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        as_of_sequence: u64,
        last_trade_id: Option<TradeId>,
        trading_state: TradingStateSnapshot,
        bids: Vec<MarketDataLevel>,
        asks: Vec<MarketDataLevel>,
    ) -> Result<Self, MarketDataError> {
        let snapshot = Self {
            instrument_id,
            instrument_version,
            as_of_sequence,
            last_trade_id,
            trading_state,
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

    /// Returns the last matching-event sequence reflected in this image.
    #[must_use]
    pub const fn as_of_sequence(&self) -> u64 {
        self.as_of_sequence
    }

    /// Returns the last execution reflected in this image.
    #[must_use]
    pub const fn last_trade_id(&self) -> Option<TradeId> {
        self.last_trade_id
    }

    /// Returns the effective trading state at the snapshot boundary.
    #[must_use]
    pub const fn trading_state(&self) -> TradingStateSnapshot {
        self.trading_state
    }

    /// Returns bids in best-to-worst order.
    #[must_use]
    pub fn bids(&self) -> &[MarketDataLevel] {
        &self.bids
    }

    /// Returns asks in best-to-worst order.
    #[must_use]
    pub fn asks(&self) -> &[MarketDataLevel] {
        &self.asks
    }

    fn validate(&self) -> Result<(), MarketDataError> {
        if self
            .last_trade_id
            .is_some_and(|trade_id| trade_id.get() > self.as_of_sequence)
        {
            return Err(MarketDataError::InvalidSnapshot(
                "snapshot trade identifier exceeds its event sequence",
            ));
        }
        if self.trading_state.revision() > self.as_of_sequence {
            return Err(MarketDataError::InvalidSnapshot(
                "snapshot trading-state revision exceeds its event sequence",
            ));
        }
        validate_snapshot_side(Side::Buy, &self.bids)?;
        validate_snapshot_side(Side::Sell, &self.asks)?;
        if let (Some(bid), Some(ask)) = (self.bids.first(), self.asks.first()) {
            if bid.price >= ask.price {
                return Err(MarketDataError::InvalidSnapshot(
                    "snapshot contains a crossed or locked book",
                ));
            }
        }
        Ok(())
    }
}

fn validate_snapshot_side(
    expected_side: Side,
    levels: &[MarketDataLevel],
) -> Result<(), MarketDataError> {
    let mut previous = None;
    for level in levels {
        if level.side != expected_side || level.quantity == 0 || level.order_count == 0 {
            return Err(MarketDataError::InvalidSnapshot(
                "snapshot level has an invalid side or empty aggregate",
            ));
        }
        if let Some(previous_price) = previous {
            let ordered = match expected_side {
                Side::Buy => previous_price > level.price,
                Side::Sell => previous_price < level.price,
            };
            if !ordered {
                return Err(MarketDataError::InvalidSnapshot(
                    "snapshot levels are not strictly ordered in market priority",
                ));
            }
        }
        previous = Some(level.price);
    }
    Ok(())
}

/// Publication, validation, or replica-recovery failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataError {
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
    /// A snapshot predates the replica state it would replace.
    StaleSnapshot {
        /// Current replica sequence.
        current: u64,
        /// Snapshot boundary.
        snapshot: u64,
    },
    /// A full-depth image violated a structural invariant.
    InvalidSnapshot(&'static str),
    /// An incremental update violated a structural invariant.
    InvalidUpdate(&'static str),
    /// A matching report contradicted its command or event grammar.
    TraceMismatch(&'static str),
    /// Published aggregate state differed from the authoritative book.
    SourceDivergence(&'static str),
    /// Checked aggregate or sequence arithmetic overflowed.
    ArithmeticOverflow,
    /// One finite semantic state or batch bound would be exceeded.
    CapacityExceeded {
        /// Exhausted resource.
        resource: MarketDataResource,
        /// Configured maximum.
        maximum: usize,
        /// Cardinality required by the operation.
        attempted: usize,
    },
    /// A complete publication output buffer could not be reserved before mutation.
    PreparationAllocationFailed(MarketDataResource),
    /// A previous partial publication failure requires bootstrap from a fresh book.
    Poisoned,
}

impl fmt::Display for MarketDataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongInstrument => {
                formatter.write_str("market data belongs to another instrument")
            }
            Self::WrongInstrumentVersion => {
                formatter.write_str("market data belongs to another instrument version")
            }
            Self::SequenceGap { expected, actual } => {
                write!(
                    formatter,
                    "market-data sequence gap: expected {expected}, received {actual}"
                )
            }
            Self::StaleSnapshot { current, snapshot } => write!(
                formatter,
                "snapshot sequence {snapshot} predates replica sequence {current}"
            ),
            Self::InvalidSnapshot(detail)
            | Self::InvalidUpdate(detail)
            | Self::TraceMismatch(detail)
            | Self::SourceDivergence(detail) => formatter.write_str(detail),
            Self::ArithmeticOverflow => formatter.write_str("market-data arithmetic overflow"),
            Self::CapacityExceeded {
                resource,
                maximum,
                attempted,
            } => write!(
                formatter,
                "market-data {resource} capacity {maximum} is below attempted cardinality {attempted}"
            ),
            Self::PreparationAllocationFailed(resource) => write!(
                formatter,
                "failed to reserve market-data {resource} preparation storage"
            ),
            Self::Poisoned => formatter.write_str("market-data state is poisoned"),
        }
    }
}

impl std::error::Error for MarketDataError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Aggregate {
    quantity: u128,
    order_count: u64,
}

type DepthMap = IndexedAvlMap<Price, Aggregate>;

/// One publisher or replica bounded hash index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MarketDataHashIndex {
    /// Publisher active-order mirror.
    TrackedOrders,
    /// Publisher account-control mirror.
    AccountControls,
    /// Publisher affected-level validation scratch identities.
    PublisherBatchLevels,
    /// Replica batch-capacity scratch identities.
    ReplicaBatchLevels,
}

/// Process-local fixed hash-index allocation telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketDataHashIndexStatus {
    /// Configured semantic entries.
    pub configured_entries: usize,
    /// Constructor-allocated dense entries.
    pub allocated_entries: usize,
    /// Initialized open-addressed lookup buckets.
    pub initialized_buckets: usize,
    /// Currently occupied entries.
    pub occupied_entries: usize,
}

/// Process-local fixed price-level arena allocation telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarketDataArenaStatus {
    /// Configured semantic slots.
    pub configured_slots: usize,
    /// Constructor-allocated slots.
    pub allocated_slots: usize,
    /// Slots initialized at least once since the arena was created.
    pub initialized_slots: usize,
    /// Currently occupied slots.
    pub occupied_slots: usize,
    /// Initialized vacant slots immediately reusable without allocation.
    pub reusable_slots: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TrackedOrder {
    account_id: AccountId,
    side: Side,
    price: Price,
    leaves: u64,
    displayed: u64,
    display: OrderDisplay,
    expires_at: Option<TimestampNs>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct MassCancelProgress {
    order_count: u64,
    quantity_lots: u128,
    last_order_id: Option<OrderId>,
    last_expiry: Option<(TimestampNs, OrderId)>,
    completed: bool,
}

/// Stateful adapter from private matching traces to anonymized public depth.
#[derive(Debug)]
pub struct MarketDataPublisher {
    limits: MarketDataLimits,
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    bids: DepthMap,
    asks: DepthMap,
    orders: BoundedHashMap<OrderId, TrackedOrder>,
    account_controls: BoundedHashMap<AccountId, AccountControlSnapshot>,
    affected_levels: BoundedHashMap<(Side, Price), ()>,
    trading_state: TradingStateSnapshot,
    expiry_watermark: Option<TimestampNs>,
    last_sequence: u64,
    last_trade_id: Option<TradeId>,
    poisoned: bool,
}

impl MarketDataPublisher {
    /// Bootstraps publication state from an empty, live, or WAL-recovered book.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataConstructionError`] if complete fixed storage cannot
    /// be reserved or the source book fails mirror validation.
    pub fn from_book(book: &OrderBook) -> Result<Self, MarketDataConstructionError> {
        Self::construct(book, MarketDataLimits::from_order_book(book.limits()))
    }

    /// Bootstraps under an explicit finite envelope that must cover the source shard.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataConstructionError`] for an invalid/undersized policy,
    /// reservation failure, or source-state divergence.
    pub fn try_from_book_with_limits(
        book: &OrderBook,
        spec: MarketDataLimitsSpec,
    ) -> Result<Self, MarketDataConstructionError> {
        let limits = MarketDataLimits::new(spec)?;
        Self::construct(book, limits)
    }

    fn construct(
        book: &OrderBook,
        limits: MarketDataLimits,
    ) -> Result<Self, MarketDataConstructionError> {
        ensure_source_limits(limits, book.limits())?;
        let maximum_levels = limits.max_price_levels_per_side();
        let mut publisher = Self {
            limits,
            instrument_id: book.instrument_id(),
            instrument_version: book.instrument_version(),
            bids: try_depth_map(maximum_levels, MarketDataResource::BidPriceLevels)?,
            asks: try_depth_map(maximum_levels, MarketDataResource::AskPriceLevels)?,
            orders: try_hash_map(
                limits.max_active_orders(),
                MarketDataResource::TrackedOrders,
            )?,
            account_controls: try_hash_map(
                limits.max_account_controls(),
                MarketDataResource::AccountControls,
            )?,
            affected_levels: try_hash_map(
                limits.max_batch_updates(),
                MarketDataResource::BatchLevelIdentities,
            )?,
            trading_state: book.trading_state(),
            expiry_watermark: book.expiry_watermark(),
            last_sequence: book.last_event_sequence(),
            last_trade_id: book.last_trade_id(),
            poisoned: false,
        };

        for level in book.depth_levels(Side::Buy) {
            publisher.bids.insert(
                level.price,
                Aggregate {
                    quantity: level.quantity,
                    order_count: level.order_count,
                },
            );
        }
        for level in book.depth_levels(Side::Sell) {
            publisher.asks.insert(
                level.price,
                Aggregate {
                    quantity: level.quantity,
                    order_count: level.order_count,
                },
            );
        }
        for order in book.active_order_states() {
            let order = order.map_err(|_| {
                MarketDataConstructionError::Source(MarketDataError::SourceDivergence(
                    "matching book failed active-order snapshot validation",
                ))
            })?;
            publisher.orders.insert(
                order.order_id,
                TrackedOrder {
                    account_id: order.account_id,
                    side: order.side,
                    price: order.price,
                    leaves: order.leaves_quantity.lots(),
                    displayed: order.displayed_quantity.lots(),
                    display: order.display,
                    expires_at: order.expires_at,
                },
            );
        }
        for (account_id, snapshot) in book.account_controls() {
            publisher.account_controls.insert(account_id, snapshot);
        }
        publisher
            .validate_against(book)
            .map_err(MarketDataConstructionError::Source)?;
        Ok(publisher)
    }

    /// Returns this publisher's immutable finite resource envelope.
    #[must_use]
    pub const fn limits(&self) -> MarketDataLimits {
        self.limits
    }

    /// Returns fixed price-level arena telemetry for one side.
    #[must_use]
    pub fn price_level_arena_status(&self, side: Side) -> MarketDataArenaStatus {
        arena_status(self.levels(side))
    }

    /// Returns publisher hash-index telemetry, or `None` for replica-only scratch.
    #[must_use]
    pub fn hash_index_status(
        &self,
        index: MarketDataHashIndex,
    ) -> Option<MarketDataHashIndexStatus> {
        match index {
            MarketDataHashIndex::TrackedOrders => Some(hash_status(&self.orders)),
            MarketDataHashIndex::AccountControls => Some(hash_status(&self.account_controls)),
            MarketDataHashIndex::PublisherBatchLevels => Some(hash_status(&self.affected_levels)),
            MarketDataHashIndex::ReplicaBatchLevels => None,
        }
    }

    /// Returns the final published source sequence.
    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// Returns whether a trace failure invalidated incremental publication state.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Fallibly materializes a complete full-depth recovery image in `O(P)` time.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::PreparationAllocationFailed`] before emitting
    /// a partial image if either output vector cannot be reserved.
    pub fn try_snapshot(&self) -> Result<MarketDataSnapshot, MarketDataError> {
        let mut bids = Vec::new();
        bids.try_reserve_exact(self.bids.len()).map_err(|_| {
            MarketDataError::PreparationAllocationFailed(MarketDataResource::BidPriceLevels)
        })?;
        bids.extend(
            self.bids
                .iter()
                .rev()
                .map(|(&price, level)| MarketDataLevel {
                    side: Side::Buy,
                    price,
                    quantity: level.quantity,
                    order_count: level.order_count,
                }),
        );
        let mut asks = Vec::new();
        asks.try_reserve_exact(self.asks.len()).map_err(|_| {
            MarketDataError::PreparationAllocationFailed(MarketDataResource::AskPriceLevels)
        })?;
        asks.extend(self.asks.iter().map(|(&price, level)| MarketDataLevel {
            side: Side::Sell,
            price,
            quantity: level.quantity,
            order_count: level.order_count,
        }));
        Ok(MarketDataSnapshot {
            instrument_id: self.instrument_id,
            instrument_version: self.instrument_version,
            as_of_sequence: self.last_sequence,
            last_trade_id: self.last_trade_id,
            trading_state: self.trading_state,
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
    pub fn snapshot(&self) -> MarketDataSnapshot {
        self.try_snapshot()
            .expect("market-data snapshot output allocation failed")
    }

    /// Publishes one complete matching trace.
    ///
    /// Exact matching retries return an empty replay batch. Every non-replayed
    /// matching event yields one update at the identical sequence. A trace
    /// failure poisons the publisher because preceding events in the same
    /// report may already have been applied; reconstruct with [`Self::from_book`].
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for identity, sequence, trace, arithmetic,
    /// or source-state divergence.
    #[allow(
        clippy::too_many_lines,
        reason = "publication keeps complete preflight and event-transition ordering auditable"
    )]
    pub fn publish(
        &mut self,
        command: Command,
        report: &ExecutionReport,
        book: &OrderBook,
    ) -> Result<MarketDataBatch, MarketDataError> {
        if self.poisoned {
            return Err(MarketDataError::Poisoned);
        }
        if command_instrument(command) != self.instrument_id {
            return self.fail(MarketDataError::WrongInstrument);
        }
        if command_version(command) != self.instrument_version {
            return self.fail(MarketDataError::WrongInstrumentVersion);
        }
        if report.command_id != command_id(command) {
            return self.fail(MarketDataError::TraceMismatch(
                "report command identifier differs from its source command",
            ));
        }
        if report.events.is_empty() {
            return self.fail(MarketDataError::TraceMismatch(
                "matching report contains no events",
            ));
        }
        if let Err(error) = validate_report_outcome(report) {
            return self.fail(error);
        }
        if report.events.iter().any(|event| {
            event.command_id != report.command_id || event.occurred_at != command_time(command)
        }) {
            return self.fail(MarketDataError::TraceMismatch(
                "event context differs from its command",
            ));
        }
        if report.replayed {
            if report
                .events
                .last()
                .is_some_and(|event| event.sequence > self.last_sequence)
            {
                return self.fail(MarketDataError::TraceMismatch(
                    "replayed report contains an unpublished future sequence",
                ));
            }
            return Ok(MarketDataBatch {
                updates: Vec::new(),
                replayed: true,
            });
        }

        if report.events.len() > self.limits.max_batch_updates() {
            return Err(MarketDataError::CapacityExceeded {
                resource: MarketDataResource::BatchUpdates,
                maximum: self.limits.max_batch_updates(),
                attempted: report.events.len(),
            });
        }
        let mut updates = Vec::new();
        updates
            .try_reserve_exact(report.events.len())
            .map_err(|_| {
                MarketDataError::PreparationAllocationFailed(MarketDataResource::BatchUpdates)
            })?;
        let mut replacement_state = None;
        let mut mass_cancel_progress = MassCancelProgress::default();
        for event in &report.events {
            let expected = self
                .last_sequence
                .checked_add(1)
                .ok_or(MarketDataError::ArithmeticOverflow);
            let expected = match expected {
                Ok(value) => value,
                Err(error) => return self.fail(error),
            };
            if event.sequence != expected {
                return self.fail(MarketDataError::SequenceGap {
                    expected,
                    actual: event.sequence,
                });
            }
            let kind = match self.apply_event(
                command,
                *event,
                &mut replacement_state,
                &mut mass_cancel_progress,
            ) {
                Ok(kind) => kind,
                Err(error) => return self.fail(error),
            };
            updates.push(MarketDataUpdate {
                instrument_id: self.instrument_id,
                instrument_version: self.instrument_version,
                sequence: event.sequence,
                occurred_at: event.occurred_at,
                kind,
            });
            self.last_sequence = event.sequence;
        }
        let requires_completion = report.outcome == CommandOutcome::Accepted
            && matches!(
                command,
                Command::MassCancel(_)
                    | Command::AccountControl(_)
                    | Command::TradingStateControl(_)
                    | Command::ExpirySweep(_)
            );
        if requires_completion != mass_cancel_progress.completed {
            return self.fail(MarketDataError::TraceMismatch(
                "aggregate cancellation/control command and completion event disagree",
            ));
        }
        if let Err(error) = self.validate_publication(&updates, book) {
            return self.fail(error);
        }
        Ok(MarketDataBatch {
            updates,
            replayed: false,
        })
    }

    /// Audits the complete publisher mirror against an authoritative book.
    ///
    /// This is `O(O + P)` and is intended for recovery, tests, diagnostics,
    /// and periodic integrity checks rather than every hot-path publication.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::SourceDivergence`] on the first mismatch.
    pub fn validate_against(&self, book: &OrderBook) -> Result<(), MarketDataError> {
        if self.instrument_id != book.instrument_id() {
            return Err(MarketDataError::WrongInstrument);
        }
        if self.instrument_version != book.instrument_version() {
            return Err(MarketDataError::WrongInstrumentVersion);
        }
        if self.last_sequence != book.last_event_sequence()
            || self.last_trade_id != book.last_trade_id()
            || self.trading_state != book.trading_state()
            || self.expiry_watermark != book.expiry_watermark()
        {
            return Err(MarketDataError::SourceDivergence(
                "publisher source sequences differ from the matching book",
            ));
        }
        if self.orders.len() != book.active_order_count() {
            return Err(MarketDataError::SourceDivergence(
                "publisher active-order count differs from the matching book",
            ));
        }
        if self.account_controls.len() != book.account_controls().len()
            || book.account_controls().any(|(account_id, snapshot)| {
                self.account_controls.get(&account_id) != Some(&snapshot)
            })
        {
            return Err(MarketDataError::SourceDivergence(
                "publisher account-control mirror differs from the matching book",
            ));
        }
        for order in book.active_order_states() {
            let order = order.map_err(|_| {
                MarketDataError::SourceDivergence(
                    "matching book failed active-order snapshot validation",
                )
            })?;
            let expected = TrackedOrder {
                account_id: order.account_id,
                side: order.side,
                price: order.price,
                leaves: order.leaves_quantity.lots(),
                displayed: order.displayed_quantity.lots(),
                display: order.display,
                expires_at: order.expires_at,
            };
            if self.orders.get(&order.order_id) != Some(&expected) {
                return Err(MarketDataError::SourceDivergence(
                    "publisher order mirror differs from the matching book",
                ));
            }
        }
        if !depth_matches(&self.bids, book.depth_levels(Side::Buy))
            || !depth_matches(&self.asks, book.depth_levels(Side::Sell))
        {
            return Err(MarketDataError::SourceDivergence(
                "publisher aggregate depth differs from the matching book",
            ));
        }
        self.validate_layouts()?;
        Ok(())
    }

    fn validate_layouts(&self) -> Result<(), MarketDataError> {
        if self.bids.maximum() != self.limits.max_price_levels_per_side()
            || self.asks.maximum() != self.limits.max_price_levels_per_side()
            || self.orders.maximum() != self.limits.max_active_orders()
            || self.account_controls.maximum() != self.limits.max_account_controls()
            || self.affected_levels.maximum() != self.limits.max_batch_updates()
            || !self.affected_levels.is_empty()
            || self.bids.allocation_capacity() < self.limits.max_price_levels_per_side()
            || self.asks.allocation_capacity() < self.limits.max_price_levels_per_side()
        {
            return Err(MarketDataError::SourceDivergence(
                "publisher fixed storage contradicts its configured limits",
            ));
        }
        self.bids.validate().map_err(|_| {
            MarketDataError::SourceDivergence("publisher bid arena is structurally invalid")
        })?;
        self.asks.validate().map_err(|_| {
            MarketDataError::SourceDivergence("publisher ask arena is structurally invalid")
        })?;
        self.orders.validate_layout().map_err(|_| {
            MarketDataError::SourceDivergence("publisher order hash layout is invalid")
        })?;
        self.account_controls.validate_layout().map_err(|_| {
            MarketDataError::SourceDivergence("publisher account-control hash layout is invalid")
        })?;
        self.affected_levels.validate_layout().map_err(|_| {
            MarketDataError::SourceDivergence("publisher batch scratch hash layout is invalid")
        })?;
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive private-event grammar keeps every source transition auditable"
    )]
    fn apply_event(
        &mut self,
        command: Command,
        event: Event,
        replacement_state: &mut Option<(Side, Option<TimestampNs>)>,
        mass_cancel_progress: &mut MassCancelProgress,
    ) -> Result<MarketDataKind, MarketDataError> {
        match event.kind {
            EventKind::OrderAccepted {
                order_id,
                quantity,
                display,
            } => Self::handle_order_accepted(command, order_id, quantity, display),
            EventKind::OrderRefreshed {
                order_id,
                price,
                displayed_quantity,
                leaves_quantity,
            } => self.handle_order_refreshed(order_id, price, displayed_quantity, leaves_quantity),
            EventKind::OrderRested {
                order_id,
                price,
                leaves_quantity,
                displayed_quantity,
            } => self.handle_order_rested(
                command,
                order_id,
                price,
                leaves_quantity,
                displayed_quantity,
                replacement_state,
            ),
            EventKind::Trade(trade) => self.handle_trade(command, trade),
            EventKind::OrderCancelled {
                order_id,
                quantity,
                reason,
            } => self.handle_cancel(command, order_id, quantity, reason, mass_cancel_progress),
            EventKind::OrderReplaced {
                order_id,
                old_price,
                new_price,
                old_quantity,
                new_quantity,
                old_display,
                new_display,
                priority_retained,
            } => self.handle_replace(
                command,
                order_id,
                old_price,
                new_price,
                old_quantity,
                new_quantity,
                old_display,
                new_display,
                priority_retained,
                replacement_state,
            ),
            EventKind::SelfTradePrevented {
                aggressor_order_id,
                resting_order_id,
                quantity,
                policy,
            } => self.handle_self_trade(
                command,
                aggressor_order_id,
                resting_order_id,
                quantity,
                policy,
            ),
            EventKind::MassCancelCompleted {
                account_id,
                scope,
                cancelled_order_count,
                cancelled_quantity_lots,
            } => Self::handle_mass_cancel_completed(
                command,
                account_id,
                scope,
                cancelled_order_count,
                cancelled_quantity_lots,
                mass_cancel_progress,
            ),
            EventKind::AccountControlApplied {
                account_id,
                previous_state,
                current_state,
                revision,
                cancelled_order_count,
                cancelled_quantity_lots,
            } => self.handle_account_control_applied(
                command,
                account_id,
                previous_state,
                current_state,
                revision,
                cancelled_order_count,
                cancelled_quantity_lots,
                mass_cancel_progress,
            ),
            EventKind::TradingStateControlApplied {
                previous_state,
                current_state,
                revision,
                cancelled_order_count,
                cancelled_quantity_lots,
            } => self.handle_trading_state_control_applied(
                command,
                previous_state,
                current_state,
                revision,
                cancelled_order_count,
                cancelled_quantity_lots,
                mass_cancel_progress,
            ),
            EventKind::ExpirySweepCompleted {
                previous_watermark,
                current_watermark,
                expired_order_count,
                expired_quantity_lots,
            } => self.handle_expiry_sweep_completed(
                command,
                previous_watermark,
                current_watermark,
                expired_order_count,
                expired_quantity_lots,
                mass_cancel_progress,
            ),
            EventKind::CommandRejected(_) => Ok(MarketDataKind::NoBookChange),
        }
    }

    fn handle_order_accepted(
        command: Command,
        order_id: OrderId,
        quantity: Quantity,
        display: OrderDisplay,
    ) -> Result<MarketDataKind, MarketDataError> {
        let Command::New(new_order) = command else {
            return Err(MarketDataError::TraceMismatch(
                "non-new command emitted OrderAccepted",
            ));
        };
        if order_id != new_order.order_id
            || quantity != new_order.quantity
            || display != new_order.display
        {
            return Err(MarketDataError::TraceMismatch(
                "OrderAccepted differs from the new command",
            ));
        }
        Ok(MarketDataKind::NoBookChange)
    }

    fn handle_order_rested(
        &mut self,
        command: Command,
        order_id: OrderId,
        price: Price,
        leaves_quantity: Quantity,
        displayed_quantity: Quantity,
        replacement_state: &mut Option<(Side, Option<TimestampNs>)>,
    ) -> Result<MarketDataKind, MarketDataError> {
        let (account_id, side, display, expires_at) = match command {
            Command::New(new_order) if new_order.order_id == order_id => {
                if new_order.order_type != OrderType::Limit(price) {
                    return Err(MarketDataError::TraceMismatch(
                        "resting price differs from the new limit",
                    ));
                }
                (
                    new_order.account_id,
                    new_order.side,
                    new_order.display,
                    new_order.time_in_force.expires_at(),
                )
            }
            Command::Replace(replace) if replace.order_id == order_id => {
                if replace.new_price != price {
                    return Err(MarketDataError::TraceMismatch(
                        "resting price differs from the replacement limit",
                    ));
                }
                let (side, expires_at) =
                    replacement_state
                        .take()
                        .ok_or(MarketDataError::TraceMismatch(
                            "replacement rested without losing its previous priority",
                        ))?;
                (replace.account_id, side, replace.new_display, expires_at)
            }
            _ => {
                return Err(MarketDataError::TraceMismatch(
                    "OrderRested differs from the source command",
                ));
            }
        };
        if self.orders.contains_key(&order_id) {
            return Err(MarketDataError::TraceMismatch(
                "OrderRested duplicated an active order",
            ));
        }
        if displayed_quantity.lots() != displayed_lots(display, leaves_quantity.lots()) {
            return Err(MarketDataError::TraceMismatch(
                "rested display differs from the order display policy",
            ));
        }
        self.orders
            .try_insert(
                order_id,
                TrackedOrder {
                    account_id,
                    side,
                    price,
                    leaves: leaves_quantity.lots(),
                    displayed: displayed_quantity.lots(),
                    display,
                    expires_at,
                },
            )
            .map_err(|_| MarketDataError::CapacityExceeded {
                resource: MarketDataResource::TrackedOrders,
                maximum: self.limits.max_active_orders(),
                attempted: self.orders.len().saturating_add(1),
            })?;
        self.add_level(side, price, displayed_quantity.lots())
            .map(MarketDataKind::Level)
    }

    fn handle_order_refreshed(
        &mut self,
        order_id: OrderId,
        price: Price,
        displayed_quantity: Quantity,
        leaves_quantity: Quantity,
    ) -> Result<MarketDataKind, MarketDataError> {
        let order = self
            .orders
            .get(&order_id)
            .copied()
            .ok_or(MarketDataError::TraceMismatch(
                "reserve refresh target is absent",
            ))?;
        if order.price != price
            || order.displayed != 0
            || order.leaves != leaves_quantity.lots()
            || !order.display.is_reserve()
            || displayed_quantity.lots() != displayed_lots(order.display, order.leaves)
        {
            return Err(MarketDataError::TraceMismatch(
                "reserve refresh contradicts tracked hidden state",
            ));
        }
        self.orders
            .get_mut(&order_id)
            .expect("checked refresh target exists")
            .displayed = displayed_quantity.lots();
        self.add_level(order.side, order.price, displayed_quantity.lots())
            .map(MarketDataKind::Level)
    }

    fn handle_trade(
        &mut self,
        command: Command,
        trade: Trade,
    ) -> Result<MarketDataKind, MarketDataError> {
        if trade.instrument_id != self.instrument_id {
            return Err(MarketDataError::WrongInstrument);
        }
        if trade.instrument_version != self.instrument_version {
            return Err(MarketDataError::WrongInstrumentVersion);
        }
        if command_order_id(command) != Some(trade.taker_order_id) {
            return Err(MarketDataError::TraceMismatch(
                "trade taker differs from the source command order",
            ));
        }
        let maker = self.orders.get(&trade.maker_order_id).copied().ok_or(
            MarketDataError::TraceMismatch("trade maker is absent from public state"),
        )?;
        let expected_maker = match maker.side {
            Side::Buy => trade.buy_order_id,
            Side::Sell => trade.sell_order_id,
        };
        let (maker_account, taker_account) = match maker.side {
            Side::Buy => (trade.buyer_account_id, trade.seller_account_id),
            Side::Sell => (trade.seller_account_id, trade.buyer_account_id),
        };
        if expected_maker != trade.maker_order_id
            || maker.price != trade.price
            || maker.account_id != maker_account
            || command_account_id(command) != Some(taker_account)
        {
            return Err(MarketDataError::TraceMismatch(
                "trade identity, owner, side, or price contradicts tracked state",
            ));
        }
        self.advance_trade_id(trade.trade_id)?;
        let level = self.decrement_order(trade.maker_order_id, trade.quantity.lots())?;
        Ok(MarketDataKind::Trade {
            print: TradePrint {
                trade_id: trade.trade_id,
                price: trade.price,
                quantity: trade.quantity,
                aggressor_side: maker.side.opposite(),
            },
            maker_level: level,
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive cancellation grammar validates every reason against its command"
    )]
    fn handle_cancel(
        &mut self,
        command: Command,
        order_id: OrderId,
        quantity: Quantity,
        reason: CancelReason,
        mass_cancel_progress: &mut MassCancelProgress,
    ) -> Result<MarketDataKind, MarketDataError> {
        if let Some(order) = self.orders.get(&order_id).copied() {
            match reason {
                CancelReason::UserRequested => {
                    let Command::Cancel(cancel) = command else {
                        return Err(MarketDataError::TraceMismatch(
                            "non-cancel command emitted a user cancellation",
                        ));
                    };
                    if cancel.order_id != order_id || cancel.account_id != order.account_id {
                        return Err(MarketDataError::TraceMismatch(
                            "user cancellation differs from its source command",
                        ));
                    }
                }
                CancelReason::SelfTradeResting => {
                    if command_order_id(command).is_none()
                        || command_account_id(command) != Some(order.account_id)
                    {
                        return Err(MarketDataError::TraceMismatch(
                            "self-trade cancellation targeted another account",
                        ));
                    }
                }
                CancelReason::MassCancel => {
                    let Command::MassCancel(mass_cancel) = command else {
                        return Err(MarketDataError::TraceMismatch(
                            "non-mass-cancel command emitted a mass cancellation",
                        ));
                    };
                    if mass_cancel_progress.completed
                        || mass_cancel.account_id != order.account_id
                        || !mass_cancel.scope.includes(order.side)
                        || mass_cancel_progress
                            .last_order_id
                            .is_some_and(|previous| order_id <= previous)
                    {
                        return Err(MarketDataError::TraceMismatch(
                            "mass cancellation targeted an order outside its scope",
                        ));
                    }
                    mass_cancel_progress.order_count = mass_cancel_progress
                        .order_count
                        .checked_add(1)
                        .ok_or(MarketDataError::ArithmeticOverflow)?;
                    mass_cancel_progress.quantity_lots = mass_cancel_progress
                        .quantity_lots
                        .checked_add(u128::from(quantity.lots()))
                        .ok_or(MarketDataError::ArithmeticOverflow)?;
                    mass_cancel_progress.last_order_id = Some(order_id);
                }
                CancelReason::AccountControl => {
                    let Command::AccountControl(control) = command else {
                        return Err(MarketDataError::TraceMismatch(
                            "non-control command emitted an account-control cancellation",
                        ));
                    };
                    if control.action != AccountControlAction::BlockAndCancel
                        || mass_cancel_progress.completed
                        || control.account_id != order.account_id
                        || mass_cancel_progress
                            .last_order_id
                            .is_some_and(|previous| order_id <= previous)
                    {
                        return Err(MarketDataError::TraceMismatch(
                            "account control targeted an order outside its account",
                        ));
                    }
                    mass_cancel_progress.order_count = mass_cancel_progress
                        .order_count
                        .checked_add(1)
                        .ok_or(MarketDataError::ArithmeticOverflow)?;
                    mass_cancel_progress.quantity_lots = mass_cancel_progress
                        .quantity_lots
                        .checked_add(u128::from(quantity.lots()))
                        .ok_or(MarketDataError::ArithmeticOverflow)?;
                    mass_cancel_progress.last_order_id = Some(order_id);
                }
                CancelReason::TradingStateControl => {
                    let Command::TradingStateControl(control) = command else {
                        return Err(MarketDataError::TraceMismatch(
                            "non-state-control command emitted a state-control cancellation",
                        ));
                    };
                    if control.action != TradingStateControlAction::TransitionAndCancel
                        || control.target_state == TradingState::Open
                        || mass_cancel_progress.completed
                        || mass_cancel_progress
                            .last_order_id
                            .is_some_and(|previous| order_id <= previous)
                    {
                        return Err(MarketDataError::TraceMismatch(
                            "trading-state control cancellation is not canonical",
                        ));
                    }
                    mass_cancel_progress.order_count = mass_cancel_progress
                        .order_count
                        .checked_add(1)
                        .ok_or(MarketDataError::ArithmeticOverflow)?;
                    mass_cancel_progress.quantity_lots = mass_cancel_progress
                        .quantity_lots
                        .checked_add(u128::from(quantity.lots()))
                        .ok_or(MarketDataError::ArithmeticOverflow)?;
                    mass_cancel_progress.last_order_id = Some(order_id);
                }
                CancelReason::Expired => {
                    let Command::ExpirySweep(sweep) = command else {
                        return Err(MarketDataError::TraceMismatch(
                            "non-expiry command emitted an expiry cancellation",
                        ));
                    };
                    let expires_at = order.expires_at.ok_or(MarketDataError::TraceMismatch(
                        "expiry cancellation targeted a non-GTD order",
                    ))?;
                    let key = (expires_at, order_id);
                    if mass_cancel_progress.completed
                        || expires_at > sweep.through
                        || mass_cancel_progress
                            .last_expiry
                            .is_some_and(|previous| key <= previous)
                    {
                        return Err(MarketDataError::TraceMismatch(
                            "expiry cancellation is outside its sweep or not canonical",
                        ));
                    }
                    mass_cancel_progress.order_count = mass_cancel_progress
                        .order_count
                        .checked_add(1)
                        .ok_or(MarketDataError::ArithmeticOverflow)?;
                    mass_cancel_progress.quantity_lots = mass_cancel_progress
                        .quantity_lots
                        .checked_add(u128::from(quantity.lots()))
                        .ok_or(MarketDataError::ArithmeticOverflow)?;
                    mass_cancel_progress.last_expiry = Some(key);
                }
                CancelReason::UnfilledRemainder | CancelReason::SelfTradeAggressor => {
                    return Err(MarketDataError::TraceMismatch(
                        "an incoming-only cancellation targeted a resting order",
                    ));
                }
            }
            self.remove_tracked_order(order_id, quantity.lots())
                .map(MarketDataKind::Level)
        } else {
            if !matches!(
                reason,
                CancelReason::UnfilledRemainder | CancelReason::SelfTradeAggressor
            ) || command_order_id(command) != Some(order_id)
            {
                return Err(MarketDataError::TraceMismatch(
                    "resting-order cancellation target is absent",
                ));
            }
            Ok(MarketDataKind::NoBookChange)
        }
    }

    fn handle_mass_cancel_completed(
        command: Command,
        account_id: AccountId,
        scope: MassCancelScope,
        cancelled_order_count: u64,
        cancelled_quantity_lots: u128,
        progress: &mut MassCancelProgress,
    ) -> Result<MarketDataKind, MarketDataError> {
        let Command::MassCancel(mass_cancel) = command else {
            return Err(MarketDataError::TraceMismatch(
                "non-mass-cancel command emitted MassCancelCompleted",
            ));
        };
        if progress.completed
            || mass_cancel.account_id != account_id
            || mass_cancel.scope != scope
            || progress.order_count != cancelled_order_count
            || progress.quantity_lots != cancelled_quantity_lots
        {
            return Err(MarketDataError::TraceMismatch(
                "MassCancelCompleted contradicts its command or cancellations",
            ));
        }
        progress.completed = true;
        Ok(MarketDataKind::NoBookChange)
    }

    fn handle_expiry_sweep_completed(
        &mut self,
        command: Command,
        previous_watermark: Option<TimestampNs>,
        current_watermark: TimestampNs,
        expired_order_count: u64,
        expired_quantity_lots: u128,
        progress: &mut MassCancelProgress,
    ) -> Result<MarketDataKind, MarketDataError> {
        let Command::ExpirySweep(sweep) = command else {
            return Err(MarketDataError::TraceMismatch(
                "non-expiry command emitted ExpirySweepCompleted",
            ));
        };
        if progress.completed
            || previous_watermark != self.expiry_watermark
            || current_watermark != sweep.through
            || progress.order_count != expired_order_count
            || progress.quantity_lots != expired_quantity_lots
        {
            return Err(MarketDataError::TraceMismatch(
                "ExpirySweepCompleted contradicts its command or cancellations",
            ));
        }
        self.expiry_watermark = Some(current_watermark);
        progress.completed = true;
        Ok(MarketDataKind::NoBookChange)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the account-control completion event is a fixed audit record"
    )]
    fn handle_account_control_applied(
        &mut self,
        command: Command,
        account_id: AccountId,
        previous_state: AccountAdmissionState,
        current_state: AccountAdmissionState,
        revision: u64,
        cancelled_order_count: u64,
        cancelled_quantity_lots: u128,
        progress: &mut MassCancelProgress,
    ) -> Result<MarketDataKind, MarketDataError> {
        let Command::AccountControl(control) = command else {
            return Err(MarketDataError::TraceMismatch(
                "non-control command emitted AccountControlApplied",
            ));
        };
        let expected_revision = control
            .expected_revision
            .checked_add(1)
            .ok_or(MarketDataError::ArithmeticOverflow)?;
        let expected_state = match control.action {
            AccountControlAction::BlockAndCancel => AccountAdmissionState::Blocked,
            AccountControlAction::Enable => AccountAdmissionState::Enabled,
        };
        let previous = self
            .account_controls
            .get(&account_id)
            .copied()
            .unwrap_or_default();
        if progress.completed
            || control.account_id != account_id
            || previous.state() != previous_state
            || previous.revision() != control.expected_revision
            || current_state != expected_state
            || revision != expected_revision
            || progress.order_count != cancelled_order_count
            || progress.quantity_lots != cancelled_quantity_lots
            || (control.action == AccountControlAction::Enable
                && (cancelled_order_count != 0 || cancelled_quantity_lots != 0))
        {
            return Err(MarketDataError::TraceMismatch(
                "AccountControlApplied contradicts its command or cancellations",
            ));
        }
        self.account_controls
            .try_insert(
                account_id,
                AccountControlSnapshot::from_parts(current_state, revision),
            )
            .map_err(|_| MarketDataError::CapacityExceeded {
                resource: MarketDataResource::AccountControls,
                maximum: self.limits.max_account_controls(),
                attempted: self.account_controls.len().saturating_add(1),
            })?;
        progress.completed = true;
        Ok(MarketDataKind::NoBookChange)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the trading-state completion event is a fixed audit record"
    )]
    fn handle_trading_state_control_applied(
        &mut self,
        command: Command,
        previous_state: TradingState,
        current_state: TradingState,
        revision: u64,
        cancelled_order_count: u64,
        cancelled_quantity_lots: u128,
        progress: &mut MassCancelProgress,
    ) -> Result<MarketDataKind, MarketDataError> {
        let Command::TradingStateControl(control) = command else {
            return Err(MarketDataError::TraceMismatch(
                "non-state-control command emitted TradingStateControlApplied",
            ));
        };
        let expected_revision = control
            .expected_revision
            .checked_add(1)
            .ok_or(MarketDataError::ArithmeticOverflow)?;
        if progress.completed
            || self.trading_state.state() != previous_state
            || self.trading_state.revision() != control.expected_revision
            || control.target_state != current_state
            || previous_state == current_state
            || revision != expected_revision
            || progress.order_count != cancelled_order_count
            || progress.quantity_lots != cancelled_quantity_lots
            || (control.action == TradingStateControlAction::Transition
                && (cancelled_order_count != 0 || cancelled_quantity_lots != 0))
            || (control.action == TradingStateControlAction::TransitionAndCancel
                && (control.target_state == TradingState::Open || !self.orders.is_empty()))
        {
            return Err(MarketDataError::TraceMismatch(
                "TradingStateControlApplied contradicts its command or cancellations",
            ));
        }
        self.trading_state = TradingStateSnapshot::from_parts(current_state, revision);
        progress.completed = true;
        Ok(MarketDataKind::TradingState {
            previous_state,
            current_state,
            revision,
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the replacement event is a fixed state-transition record"
    )]
    fn handle_replace(
        &mut self,
        command: Command,
        order_id: OrderId,
        old_price: Price,
        new_price: Price,
        old_quantity: Quantity,
        new_quantity: Quantity,
        old_display: OrderDisplay,
        new_display: OrderDisplay,
        priority_retained: bool,
        replacement_state: &mut Option<(Side, Option<TimestampNs>)>,
    ) -> Result<MarketDataKind, MarketDataError> {
        let Command::Replace(replace) = command else {
            return Err(MarketDataError::TraceMismatch(
                "non-replace command emitted OrderReplaced",
            ));
        };
        if replace.order_id != order_id
            || replace.new_price != new_price
            || replace.new_quantity != new_quantity
            || replace.new_display != new_display
        {
            return Err(MarketDataError::TraceMismatch(
                "OrderReplaced differs from its source command",
            ));
        }
        let old = self
            .orders
            .get(&order_id)
            .copied()
            .ok_or(MarketDataError::TraceMismatch(
                "replacement target is absent",
            ))?;
        if old.price != old_price
            || old.leaves != old_quantity.lots()
            || old.display != old_display
            || old.account_id != replace.account_id
        {
            return Err(MarketDataError::TraceMismatch(
                "replacement old state differs from public state",
            ));
        }
        let level = if priority_retained {
            if new_price != old.price
                || new_quantity.lots() > old.leaves
                || new_display != old.display
            {
                return Err(MarketDataError::TraceMismatch(
                    "priority-retaining replacement increased exposure",
                ));
            }
            let new_displayed = old.displayed.min(new_quantity.lots());
            let displayed_reduction = old.displayed - new_displayed;
            let tracked = self
                .orders
                .get_mut(&order_id)
                .ok_or(MarketDataError::TraceMismatch(
                    "replacement target disappeared",
                ))?;
            tracked.leaves = new_quantity.lots();
            tracked.displayed = new_displayed;
            self.reduce_level_quantity(old.side, old.price, displayed_reduction)?
        } else {
            *replacement_state = Some((old.side, old.expires_at));
            self.remove_tracked_order(order_id, old.leaves)?
        };
        Ok(MarketDataKind::Level(level))
    }

    fn handle_self_trade(
        &mut self,
        command: Command,
        aggressor_order_id: OrderId,
        resting_order_id: OrderId,
        quantity: Quantity,
        policy: SelfTradePrevention,
    ) -> Result<MarketDataKind, MarketDataError> {
        if command_order_id(command) != Some(aggressor_order_id) {
            return Err(MarketDataError::TraceMismatch(
                "self-trade aggressor differs from the source command",
            ));
        }
        if self
            .orders
            .get(&resting_order_id)
            .is_none_or(|resting| Some(resting.account_id) != command_account_id(command))
        {
            return Err(MarketDataError::TraceMismatch(
                "self-trade resting order is absent or owned by another account",
            ));
        }
        if policy == SelfTradePrevention::DecrementAndCancel {
            self.decrement_order(resting_order_id, quantity.lots())
                .map(MarketDataKind::Level)
        } else {
            Ok(MarketDataKind::NoBookChange)
        }
    }

    fn add_level(
        &mut self,
        side: Side,
        price: Price,
        quantity: u64,
    ) -> Result<MarketDataLevel, MarketDataError> {
        let previous = self.levels(side).get(&price).copied().unwrap_or(Aggregate {
            quantity: 0,
            order_count: 0,
        });
        let quantity = previous
            .quantity
            .checked_add(u128::from(quantity))
            .ok_or(MarketDataError::ArithmeticOverflow)?;
        let order_count = previous
            .order_count
            .checked_add(1)
            .ok_or(MarketDataError::ArithmeticOverflow)?;
        if previous.order_count == 0 && !self.levels(side).has_insertion_capacity() {
            let resource = match side {
                Side::Buy => MarketDataResource::BidPriceLevels,
                Side::Sell => MarketDataResource::AskPriceLevels,
            };
            return Err(MarketDataError::CapacityExceeded {
                resource,
                maximum: self.limits.max_price_levels_per_side(),
                attempted: self.levels(side).len().saturating_add(1),
            });
        }
        self.levels_mut(side).insert(
            price,
            Aggregate {
                quantity,
                order_count,
            },
        );
        MarketDataLevel::new(side, price, quantity, order_count)
    }

    fn decrement_order(
        &mut self,
        order_id: OrderId,
        quantity: u64,
    ) -> Result<MarketDataLevel, MarketDataError> {
        let order = self
            .orders
            .get(&order_id)
            .copied()
            .ok_or(MarketDataError::TraceMismatch(
                "decremented order is absent from public state",
            ))?;
        if quantity == 0 || quantity > order.displayed {
            return Err(MarketDataError::TraceMismatch(
                "decremented quantity exceeds the displayed resting slice",
            ));
        }
        let removes_order = quantity == order.leaves;
        let removes_display = quantity == order.displayed;
        if removes_order {
            self.orders.remove(&order_id);
        } else {
            let tracked = self
                .orders
                .get_mut(&order_id)
                .expect("checked resting order exists");
            tracked.leaves -= quantity;
            tracked.displayed -= quantity;
        }
        let level = self.levels_mut(order.side).get_mut(&order.price).ok_or(
            MarketDataError::TraceMismatch("resting order references an absent public level"),
        )?;
        level.quantity = level.quantity.checked_sub(u128::from(quantity)).ok_or(
            MarketDataError::TraceMismatch(
                "public level quantity is smaller than its order decrement",
            ),
        )?;
        if removes_display {
            level.order_count =
                level
                    .order_count
                    .checked_sub(1)
                    .ok_or(MarketDataError::TraceMismatch(
                        "public level order count underflowed",
                    ))?;
        }
        let result =
            MarketDataLevel::new(order.side, order.price, level.quantity, level.order_count)?;
        if result.quantity == 0 {
            self.levels_mut(order.side).remove(&order.price);
        }
        Ok(result)
    }

    fn remove_tracked_order(
        &mut self,
        order_id: OrderId,
        total_quantity: u64,
    ) -> Result<MarketDataLevel, MarketDataError> {
        let order = self
            .orders
            .remove(&order_id)
            .ok_or(MarketDataError::TraceMismatch(
                "removed order is absent from public state",
            ))?;
        if total_quantity != order.leaves || order.displayed == 0 {
            self.orders.insert(order_id, order);
            return Err(MarketDataError::TraceMismatch(
                "removed quantity differs from total resting leaves",
            ));
        }
        let level = self.levels_mut(order.side).get_mut(&order.price).ok_or(
            MarketDataError::TraceMismatch("resting order references an absent public level"),
        )?;
        level.quantity = level
            .quantity
            .checked_sub(u128::from(order.displayed))
            .ok_or(MarketDataError::TraceMismatch(
                "public level quantity is smaller than the removed display",
            ))?;
        level.order_count =
            level
                .order_count
                .checked_sub(1)
                .ok_or(MarketDataError::TraceMismatch(
                    "public level order count underflowed",
                ))?;
        let result =
            MarketDataLevel::new(order.side, order.price, level.quantity, level.order_count)?;
        if result.quantity == 0 {
            self.levels_mut(order.side).remove(&order.price);
        }
        Ok(result)
    }

    fn reduce_level_quantity(
        &mut self,
        side: Side,
        price: Price,
        reduction: u64,
    ) -> Result<MarketDataLevel, MarketDataError> {
        let level = self
            .levels_mut(side)
            .get_mut(&price)
            .ok_or(MarketDataError::TraceMismatch(
                "replacement references an absent public level",
            ))?;
        level.quantity = level.quantity.checked_sub(u128::from(reduction)).ok_or(
            MarketDataError::TraceMismatch("replacement reduction exceeds public level quantity"),
        )?;
        MarketDataLevel::new(side, price, level.quantity, level.order_count)
    }

    fn advance_trade_id(&mut self, actual: TradeId) -> Result<(), MarketDataError> {
        let expected = match self.last_trade_id {
            Some(previous) => previous
                .get()
                .checked_add(1)
                .ok_or(MarketDataError::ArithmeticOverflow)?,
            None => 1,
        };
        if actual.get() != expected {
            return Err(MarketDataError::TraceMismatch(
                "trade identifiers are not contiguous",
            ));
        }
        self.last_trade_id = Some(actual);
        Ok(())
    }

    fn validate_publication(
        &mut self,
        updates: &[MarketDataUpdate],
        book: &OrderBook,
    ) -> Result<(), MarketDataError> {
        if book.instrument_id() != self.instrument_id {
            return Err(MarketDataError::WrongInstrument);
        }
        if book.instrument_version() != self.instrument_version {
            return Err(MarketDataError::WrongInstrumentVersion);
        }
        if book.last_event_sequence() != self.last_sequence
            || book.last_trade_id() != self.last_trade_id
            || book.active_order_count() != self.orders.len()
            || book.trading_state() != self.trading_state
        {
            return Err(MarketDataError::SourceDivergence(
                "published sequence, trade, or order count differs from the matching book",
            ));
        }
        self.affected_levels.clear();
        for update in updates {
            let affected = match update.kind {
                MarketDataKind::Level(level) => Some((level.side, level.price)),
                MarketDataKind::Trade { maker_level, .. } => {
                    Some((maker_level.side, maker_level.price))
                }
                MarketDataKind::NoBookChange | MarketDataKind::TradingState { .. } => None,
            };
            let Some((side, price)) = affected else {
                continue;
            };
            self.affected_levels.insert((side, price), ());
        }
        let mut diverged = false;
        for &(side, price) in self.affected_levels.keys() {
            let published = self.level(side, price);
            if published != book.level(side, price) {
                diverged = true;
                break;
            }
        }
        self.affected_levels.clear();
        if diverged {
            return Err(MarketDataError::SourceDivergence(
                "published aggregate level differs from the matching book",
            ));
        }
        debug_assert!(self.validate_against(book).is_ok());
        Ok(())
    }

    fn level(&self, side: Side, price: Price) -> Option<LevelSnapshot> {
        self.levels(side).get(&price).map(|level| LevelSnapshot {
            price,
            quantity: level.quantity,
            order_count: level.order_count,
        })
    }

    fn levels(&self, side: Side) -> &DepthMap {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: Side) -> &mut DepthMap {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn fail<T>(&mut self, error: MarketDataError) -> Result<T, MarketDataError> {
        self.poisoned = true;
        Err(error)
    }
}

/// A gap-detecting full-depth consumer state machine.
#[derive(Debug)]
pub struct MarketDataReplica {
    limits: MarketDataLimits,
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    bids: DepthMap,
    asks: DepthMap,
    standby_bids: DepthMap,
    standby_asks: DepthMap,
    batch_levels: BoundedHashMap<(Side, Price), bool>,
    last_sequence: u64,
    last_trade_id: Option<TradeId>,
    initial_trading_state: TradingState,
    trading_state: TradingStateSnapshot,
    poisoned: bool,
}

impl MarketDataReplica {
    /// Creates an empty consumer for one immutable instrument version.
    ///
    /// # Panics
    ///
    /// Panics only if the documented default fixed storage cannot be allocated.
    /// Use [`Self::try_with_limits`] when construction failure must remain typed.
    #[must_use]
    pub fn new(
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        initial_trading_state: TradingState,
    ) -> Self {
        Self::try_with_limits(
            instrument_id,
            instrument_version,
            initial_trading_state,
            MarketDataLimitsSpec::default(),
        )
        .expect("default market-data replica storage must be constructible")
    }

    /// Fallibly constructs a finite, double-buffered consumer state machine.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataConstructionError`] before state exists if the policy
    /// is invalid or any complete active/standby/scratch layout cannot be reserved.
    pub fn try_with_limits(
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        initial_trading_state: TradingState,
        spec: MarketDataLimitsSpec,
    ) -> Result<Self, MarketDataConstructionError> {
        let limits = MarketDataLimits::new(spec)?;
        let maximum_levels = limits.max_price_levels_per_side();
        Ok(Self {
            limits,
            instrument_id,
            instrument_version,
            bids: try_depth_map(maximum_levels, MarketDataResource::BidPriceLevels)?,
            asks: try_depth_map(maximum_levels, MarketDataResource::AskPriceLevels)?,
            standby_bids: try_depth_map(maximum_levels, MarketDataResource::BidPriceLevels)?,
            standby_asks: try_depth_map(maximum_levels, MarketDataResource::AskPriceLevels)?,
            batch_levels: try_hash_map(
                limits.max_batch_updates(),
                MarketDataResource::BatchLevelIdentities,
            )?,
            last_sequence: 0,
            last_trade_id: None,
            initial_trading_state,
            trading_state: TradingStateSnapshot::from_parts(initial_trading_state, 0),
            poisoned: false,
        })
    }

    /// Returns this replica's immutable finite resource envelope.
    #[must_use]
    pub const fn limits(&self) -> MarketDataLimits {
        self.limits
    }

    /// Returns active price-level arena telemetry for one side.
    #[must_use]
    pub fn price_level_arena_status(&self, side: Side) -> MarketDataArenaStatus {
        arena_status(self.levels(side))
    }

    /// Returns standby snapshot arena telemetry for one side.
    #[must_use]
    pub fn standby_price_level_arena_status(&self, side: Side) -> MarketDataArenaStatus {
        let levels = match side {
            Side::Buy => &self.standby_bids,
            Side::Sell => &self.standby_asks,
        };
        arena_status(levels)
    }

    /// Returns the fixed batch-capacity scratch hash telemetry.
    #[must_use]
    pub fn batch_level_hash_status(&self) -> MarketDataHashIndexStatus {
        hash_status(&self.batch_levels)
    }

    /// Atomically replaces local depth with a non-stale verified snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for identity, staleness, or snapshot
    /// structural failure. A valid snapshot also clears a poisoned state.
    pub fn apply_snapshot(&mut self, snapshot: &MarketDataSnapshot) -> Result<(), MarketDataError> {
        if snapshot.instrument_id != self.instrument_id {
            return Err(MarketDataError::WrongInstrument);
        }
        if snapshot.instrument_version != self.instrument_version {
            return Err(MarketDataError::WrongInstrumentVersion);
        }
        if snapshot.as_of_sequence < self.last_sequence {
            return Err(MarketDataError::StaleSnapshot {
                current: self.last_sequence,
                snapshot: snapshot.as_of_sequence,
            });
        }
        snapshot.validate()?;
        if snapshot.trading_state.revision() == 0
            && snapshot.trading_state.state() != self.initial_trading_state
        {
            return Err(MarketDataError::InvalidSnapshot(
                "genesis trading state differs from the configured instrument",
            ));
        }
        let maximum = self.limits.max_price_levels_per_side();
        if snapshot.bids.len() > maximum {
            return Err(MarketDataError::CapacityExceeded {
                resource: MarketDataResource::BidPriceLevels,
                maximum,
                attempted: snapshot.bids.len(),
            });
        }
        if snapshot.asks.len() > maximum {
            return Err(MarketDataError::CapacityExceeded {
                resource: MarketDataResource::AskPriceLevels,
                maximum,
                attempted: snapshot.asks.len(),
            });
        }
        self.standby_bids.clear();
        self.standby_asks.clear();
        for level in &snapshot.bids {
            self.standby_bids.insert(
                level.price,
                Aggregate {
                    quantity: level.quantity,
                    order_count: level.order_count,
                },
            );
        }
        for level in &snapshot.asks {
            self.standby_asks.insert(
                level.price,
                Aggregate {
                    quantity: level.quantity,
                    order_count: level.order_count,
                },
            );
        }
        std::mem::swap(&mut self.bids, &mut self.standby_bids);
        std::mem::swap(&mut self.asks, &mut self.standby_asks);
        self.last_sequence = snapshot.as_of_sequence;
        self.last_trade_id = snapshot.last_trade_id;
        self.trading_state = snapshot.trading_state;
        self.poisoned = false;
        Ok(())
    }

    /// Applies one contiguous command batch.
    ///
    /// Identity and sequence preflight is nonmutating. A later structural
    /// failure poisons the replica; apply a newer snapshot before continuing.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for gaps, identity mismatch, invalid trade
    /// order, or crossed/invalid level state.
    pub fn apply_batch(&mut self, batch: &MarketDataBatch) -> Result<(), MarketDataError> {
        if self.poisoned {
            return Err(MarketDataError::Poisoned);
        }
        if batch.replayed {
            return if batch.updates.is_empty() {
                Ok(())
            } else {
                Err(MarketDataError::InvalidUpdate(
                    "replayed batch must not contain updates",
                ))
            };
        }
        if batch.updates.is_empty() {
            return Err(MarketDataError::InvalidUpdate(
                "non-replayed batch must contain updates",
            ));
        }
        if batch.updates.len() > self.limits.max_batch_updates() {
            return Err(MarketDataError::CapacityExceeded {
                resource: MarketDataResource::BatchUpdates,
                maximum: self.limits.max_batch_updates(),
                attempted: batch.updates.len(),
            });
        }
        let mut expected = self
            .last_sequence
            .checked_add(1)
            .ok_or(MarketDataError::ArithmeticOverflow)?;
        for update in &batch.updates {
            if update.instrument_id != self.instrument_id {
                return Err(MarketDataError::WrongInstrument);
            }
            if update.instrument_version != self.instrument_version {
                return Err(MarketDataError::WrongInstrumentVersion);
            }
            if update.sequence != expected {
                return Err(MarketDataError::SequenceGap {
                    expected,
                    actual: update.sequence,
                });
            }
            expected = expected
                .checked_add(1)
                .ok_or(MarketDataError::ArithmeticOverflow)?;
        }
        self.preflight_batch_level_capacity(&batch.updates)?;
        for update in &batch.updates {
            if let Err(error) = self.apply_verified_update(*update) {
                self.poisoned = true;
                return Err(error);
            }
        }
        Ok(())
    }

    /// Fallibly returns up to `limit` levels in market-priority order.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::PreparationAllocationFailed`] without partial
    /// output if the caller-owned result vector cannot be reserved.
    pub fn try_depth(
        &self,
        side: Side,
        limit: usize,
    ) -> Result<Vec<LevelSnapshot>, MarketDataError> {
        let output_len = self.levels(side).len().min(limit);
        let mut output = Vec::new();
        output
            .try_reserve_exact(output_len)
            .map_err(|_| MarketDataError::PreparationAllocationFailed(side_resource(side)))?;
        match side {
            Side::Buy => {
                output.extend(self.bids.iter().rev().take(limit).map(|(&price, level)| {
                    LevelSnapshot {
                        price,
                        quantity: level.quantity,
                        order_count: level.order_count,
                    }
                }));
            }
            Side::Sell => {
                output.extend(
                    self.asks
                        .iter()
                        .take(limit)
                        .map(|(&price, level)| LevelSnapshot {
                            price,
                            quantity: level.quantity,
                            order_count: level.order_count,
                        }),
                );
            }
        }
        Ok(output)
    }

    /// Returns up to `limit` levels using the fallible production path.
    ///
    /// # Panics
    ///
    /// Panics only if result-vector allocation fails. Use [`Self::try_depth`]
    /// when allocation failure must remain typed.
    #[must_use]
    pub fn depth(&self, side: Side, limit: usize) -> Vec<LevelSnapshot> {
        self.try_depth(side, limit)
            .expect("market-data depth output allocation failed")
    }

    /// Applies one decoded incremental update.
    ///
    /// Identity and sequence checks occur before mutation. A structural
    /// payload failure poisons the replica until a valid snapshot is applied.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError`] for a gap, identity mismatch, invalid trade
    /// order, or crossed/invalid depth.
    pub fn apply(&mut self, update: MarketDataUpdate) -> Result<(), MarketDataError> {
        if self.poisoned {
            return Err(MarketDataError::Poisoned);
        }
        if update.instrument_id != self.instrument_id {
            return Err(MarketDataError::WrongInstrument);
        }
        if update.instrument_version != self.instrument_version {
            return Err(MarketDataError::WrongInstrumentVersion);
        }
        let expected = self
            .last_sequence
            .checked_add(1)
            .ok_or(MarketDataError::ArithmeticOverflow)?;
        if update.sequence != expected {
            return Err(MarketDataError::SequenceGap {
                expected,
                actual: update.sequence,
            });
        }
        self.preflight_one_level_capacity(update)?;
        if let Err(error) = self.apply_verified_update(update) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    /// Returns the current best bid.
    #[must_use]
    pub fn best_bid(&self) -> Option<LevelSnapshot> {
        self.bids
            .last_key_value()
            .map(|(&price, level)| LevelSnapshot {
                price,
                quantity: level.quantity,
                order_count: level.order_count,
            })
    }

    /// Returns the current best ask.
    #[must_use]
    pub fn best_ask(&self) -> Option<LevelSnapshot> {
        self.asks
            .first_key_value()
            .map(|(&price, level)| LevelSnapshot {
                price,
                quantity: level.quantity,
                order_count: level.order_count,
            })
    }

    /// Returns the final applied source sequence.
    #[must_use]
    pub const fn last_sequence(&self) -> u64 {
        self.last_sequence
    }

    /// Returns the current effective trading state and accepted revision.
    #[must_use]
    pub const fn trading_state(&self) -> TradingStateSnapshot {
        self.trading_state
    }

    /// Returns whether invalid incremental state requires a new snapshot.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Audits every fixed layout and active/standby aggregate invariant.
    ///
    /// This diagnostic path is `O(P + U)` and can allocate inside the AVL
    /// structural auditor; it is not part of incremental application.
    ///
    /// # Errors
    ///
    /// Returns [`MarketDataError::SourceDivergence`] on the first internal
    /// layout or aggregate contradiction.
    pub fn validate(&self) -> Result<(), MarketDataError> {
        let maximum = self.limits.max_price_levels_per_side();
        for (detail, levels) in [
            (
                "replica active bid arena is structurally invalid",
                &self.bids,
            ),
            (
                "replica active ask arena is structurally invalid",
                &self.asks,
            ),
            (
                "replica standby bid arena is structurally invalid",
                &self.standby_bids,
            ),
            (
                "replica standby ask arena is structurally invalid",
                &self.standby_asks,
            ),
        ] {
            if levels.maximum() != maximum || levels.allocation_capacity() < maximum {
                return Err(MarketDataError::SourceDivergence(
                    "replica price arena contradicts its configured limit",
                ));
            }
            levels
                .validate()
                .map_err(|_| MarketDataError::SourceDivergence(detail))?;
            if levels
                .iter()
                .any(|(_, aggregate)| aggregate.quantity == 0 || aggregate.order_count == 0)
            {
                return Err(MarketDataError::SourceDivergence(
                    "replica retains an empty aggregate level",
                ));
            }
        }
        if self.batch_levels.maximum() != self.limits.max_batch_updates()
            || !self.batch_levels.is_empty()
            || self.batch_levels.validate_layout().is_err()
        {
            return Err(MarketDataError::SourceDivergence(
                "replica batch scratch contradicts its fixed empty layout",
            ));
        }
        for (bids, asks, detail) in [
            (
                &self.bids,
                &self.asks,
                "replica active depth is crossed or locked",
            ),
            (
                &self.standby_bids,
                &self.standby_asks,
                "replica standby depth is crossed or locked",
            ),
        ] {
            if bids
                .last_key_value()
                .zip(asks.first_key_value())
                .is_some_and(|((bid, _), (ask, _))| bid >= ask)
            {
                return Err(MarketDataError::SourceDivergence(detail));
            }
        }
        Ok(())
    }

    fn apply_verified_update(&mut self, update: MarketDataUpdate) -> Result<(), MarketDataError> {
        match update.kind {
            MarketDataKind::NoBookChange => {}
            MarketDataKind::Level(level) => self.set_level(level)?,
            MarketDataKind::Trade { print, maker_level } => {
                let expected = match self.last_trade_id {
                    Some(previous) => previous
                        .get()
                        .checked_add(1)
                        .ok_or(MarketDataError::ArithmeticOverflow)?,
                    None => 1,
                };
                if print.trade_id.get() != expected
                    || print.aggressor_side == maker_level.side
                    || print.price != maker_level.price
                {
                    return Err(MarketDataError::InvalidUpdate(
                        "trade print contradicts its identifier or maker-level update",
                    ));
                }
                let previous = self
                    .levels(maker_level.side)
                    .get(&maker_level.price)
                    .copied()
                    .ok_or(MarketDataError::InvalidUpdate(
                        "trade update references an absent maker level",
                    ))?;
                let expected_quantity = previous
                    .quantity
                    .checked_sub(u128::from(print.quantity.lots()))
                    .ok_or(MarketDataError::InvalidUpdate(
                        "trade quantity exceeds the maker level",
                    ))?;
                let retained_count = maker_level.order_count == previous.order_count;
                let removed_one = previous
                    .order_count
                    .checked_sub(1)
                    .is_some_and(|count| maker_level.order_count == count);
                if maker_level.quantity != expected_quantity
                    || (!retained_count && !removed_one)
                    || (maker_level.quantity == 0 && maker_level.order_count != 0)
                {
                    return Err(MarketDataError::InvalidUpdate(
                        "trade print does not reconcile to its maker-level transition",
                    ));
                }
                self.last_trade_id = Some(print.trade_id);
                self.set_level(maker_level)?;
            }
            MarketDataKind::TradingState {
                previous_state,
                current_state,
                revision,
            } => {
                let expected_revision = self
                    .trading_state
                    .revision()
                    .checked_add(1)
                    .ok_or(MarketDataError::ArithmeticOverflow)?;
                if self.trading_state.state() != previous_state
                    || previous_state == current_state
                    || revision != expected_revision
                {
                    return Err(MarketDataError::InvalidUpdate(
                        "trading-state update is not the next valid transition",
                    ));
                }
                self.trading_state = TradingStateSnapshot::from_parts(current_state, revision);
            }
        }
        if let (Some((&bid, _)), Some((&ask, _))) =
            (self.bids.last_key_value(), self.asks.first_key_value())
        {
            if bid >= ask {
                return Err(MarketDataError::InvalidUpdate(
                    "incremental update produced a crossed or locked book",
                ));
            }
        }
        self.last_sequence = update.sequence;
        Ok(())
    }

    fn preflight_batch_level_capacity(
        &mut self,
        updates: &[MarketDataUpdate],
    ) -> Result<(), MarketDataError> {
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

    fn preflight_one_level_capacity(
        &self,
        update: MarketDataUpdate,
    ) -> Result<(), MarketDataError> {
        let Some(level) = update_level(update) else {
            return Ok(());
        };
        let levels = self.levels(level.side);
        if level.quantity != 0
            && levels.get(&level.price).is_none()
            && levels.len() >= self.limits.max_price_levels_per_side()
        {
            return Err(MarketDataError::CapacityExceeded {
                resource: side_resource(level.side),
                maximum: self.limits.max_price_levels_per_side(),
                attempted: levels.len().saturating_add(1),
            });
        }
        Ok(())
    }

    fn set_level(&mut self, level: MarketDataLevel) -> Result<(), MarketDataError> {
        let maximum = self.limits.max_price_levels_per_side();
        let levels = match level.side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        if level.quantity == 0 {
            levels.remove(&level.price);
        } else {
            if levels.get(&level.price).is_none() && levels.len() >= maximum {
                return Err(MarketDataError::CapacityExceeded {
                    resource: side_resource(level.side),
                    maximum,
                    attempted: levels.len().saturating_add(1),
                });
            }
            levels.insert(
                level.price,
                Aggregate {
                    quantity: level.quantity,
                    order_count: level.order_count,
                },
            );
        }
        Ok(())
    }

    fn levels(&self, side: Side) -> &DepthMap {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }
}

fn try_depth_map(
    maximum: usize,
    resource: MarketDataResource,
) -> Result<DepthMap, MarketDataConstructionError> {
    IndexedAvlMap::try_with_capacity(maximum)
        .map_err(|_| MarketDataConstructionError::ReservationFailed { resource, maximum })
}

fn try_hash_map<K, V>(
    maximum: usize,
    resource: MarketDataResource,
) -> Result<BoundedHashMap<K, V>, MarketDataConstructionError>
where
    K: Eq + Hash,
{
    BoundedHashMap::try_new(maximum).map_err(|_: BoundedHashError| {
        MarketDataConstructionError::ReservationFailed { resource, maximum }
    })
}

fn ensure_source_limits(
    selected: MarketDataLimits,
    source: OrderBookLimits,
) -> Result<(), MarketDataConstructionError> {
    let requirements = [
        (
            MarketDataResource::TrackedOrders,
            selected.max_active_orders(),
            source.max_active_orders(),
        ),
        (
            MarketDataResource::BidPriceLevels,
            selected.max_price_levels_per_side(),
            source.max_price_levels_per_side(),
        ),
        (
            MarketDataResource::AccountControls,
            selected.max_account_controls(),
            source.max_account_controls(),
        ),
        (
            MarketDataResource::BatchUpdates,
            selected.max_batch_updates(),
            source.max_report_events(),
        ),
    ];
    for (resource, selected, required) in requirements {
        if selected < required {
            return Err(MarketDataConstructionError::LimitsBelowSource {
                resource,
                selected,
                required,
            });
        }
    }
    Ok(())
}

const fn side_resource(side: Side) -> MarketDataResource {
    match side {
        Side::Buy => MarketDataResource::BidPriceLevels,
        Side::Sell => MarketDataResource::AskPriceLevels,
    }
}

const fn update_level(update: MarketDataUpdate) -> Option<MarketDataLevel> {
    match update.kind {
        MarketDataKind::Level(level) => Some(level),
        MarketDataKind::Trade { maker_level, .. } => Some(maker_level),
        MarketDataKind::NoBookChange | MarketDataKind::TradingState { .. } => None,
    }
}

fn preflight_replica_level_capacity(
    scratch: &mut BoundedHashMap<(Side, Price), bool>,
    bids: &DepthMap,
    asks: &DepthMap,
    maximum: usize,
    updates: &[MarketDataUpdate],
) -> Result<(), MarketDataError> {
    let mut bid_count = bids.len();
    let mut ask_count = asks.len();
    for &update in updates {
        let Some(level) = update_level(update) else {
            continue;
        };
        let key = (level.side, level.price);
        let previous = scratch.get(&key).copied().unwrap_or_else(|| {
            let levels = match level.side {
                Side::Buy => bids,
                Side::Sell => asks,
            };
            levels.get(&level.price).is_some()
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
                    .ok_or(MarketDataError::ArithmeticOverflow)?;
            }
            (true, false) => {
                *count = count.checked_sub(1).ok_or(MarketDataError::InvalidUpdate(
                    "replica level-capacity preflight underflowed",
                ))?;
            }
            (false, false) | (true, true) => {}
        }
        if *count > maximum {
            return Err(MarketDataError::CapacityExceeded {
                resource: side_resource(level.side),
                maximum,
                attempted: *count,
            });
        }
        scratch
            .try_insert(key, next)
            .map_err(|_| MarketDataError::CapacityExceeded {
                resource: MarketDataResource::BatchLevelIdentities,
                maximum: scratch.maximum(),
                attempted: scratch.len().saturating_add(1),
            })?;
    }
    Ok(())
}

fn depth_matches(mirror: &DepthMap, source: impl ExactSizeIterator<Item = LevelSnapshot>) -> bool {
    mirror.len() == source.len()
        && mirror
            .iter()
            .zip(source)
            .all(|((&price, aggregate), level)| {
                price == level.price
                    && aggregate.quantity == level.quantity
                    && aggregate.order_count == level.order_count
            })
}

fn arena_status(levels: &DepthMap) -> MarketDataArenaStatus {
    MarketDataArenaStatus {
        configured_slots: levels.maximum(),
        allocated_slots: levels.allocation_capacity(),
        initialized_slots: levels.storage_len(),
        occupied_slots: levels.len(),
        reusable_slots: levels.storage_len().saturating_sub(levels.len()),
    }
}

fn hash_status<K, V>(index: &BoundedHashMap<K, V>) -> MarketDataHashIndexStatus
where
    K: Eq + Hash,
{
    MarketDataHashIndexStatus {
        configured_entries: index.maximum(),
        allocated_entries: index.capacity(),
        initialized_buckets: index.bucket_count(),
        occupied_entries: index.len(),
    }
}

const fn displayed_lots(display: OrderDisplay, leaves: u64) -> u64 {
    match display {
        OrderDisplay::FullyDisplayed => leaves,
        OrderDisplay::Reserve { peak } => {
            if peak.lots() < leaves {
                peak.lots()
            } else {
                leaves
            }
        }
    }
}

const fn command_id(command: Command) -> CommandId {
    match command {
        Command::New(value) => value.command_id,
        Command::Cancel(value) => value.command_id,
        Command::Replace(value) => value.command_id,
        Command::MassCancel(value) => value.command_id,
        Command::AccountControl(value) => value.command_id,
        Command::TradingStateControl(value) => value.command_id,
        Command::ExpirySweep(value) => value.command_id,
    }
}

const fn command_order_id(command: Command) -> Option<OrderId> {
    match command {
        Command::New(value) => Some(value.order_id),
        Command::Cancel(value) => Some(value.order_id),
        Command::Replace(value) => Some(value.order_id),
        Command::MassCancel(_)
        | Command::AccountControl(_)
        | Command::TradingStateControl(_)
        | Command::ExpirySweep(_) => None,
    }
}

const fn command_account_id(command: Command) -> Option<AccountId> {
    match command {
        Command::New(value) => Some(value.account_id),
        Command::Cancel(value) => Some(value.account_id),
        Command::Replace(value) => Some(value.account_id),
        Command::MassCancel(value) => Some(value.account_id),
        Command::AccountControl(value) => Some(value.account_id),
        Command::TradingStateControl(_) | Command::ExpirySweep(_) => None,
    }
}

const fn command_instrument(command: Command) -> InstrumentId {
    match command {
        Command::New(value) => value.instrument_id,
        Command::Cancel(value) => value.instrument_id,
        Command::Replace(value) => value.instrument_id,
        Command::MassCancel(value) => value.instrument_id,
        Command::AccountControl(value) => value.instrument_id,
        Command::TradingStateControl(value) => value.instrument_id,
        Command::ExpirySweep(value) => value.instrument_id,
    }
}

const fn command_version(command: Command) -> InstrumentVersion {
    match command {
        Command::New(value) => value.instrument_version,
        Command::Cancel(value) => value.instrument_version,
        Command::Replace(value) => value.instrument_version,
        Command::MassCancel(value) => value.instrument_version,
        Command::AccountControl(value) => value.instrument_version,
        Command::TradingStateControl(value) => value.instrument_version,
        Command::ExpirySweep(value) => value.instrument_version,
    }
}

const fn command_time(command: Command) -> TimestampNs {
    match command {
        Command::New(value) => value.received_at,
        Command::Cancel(value) => value.received_at,
        Command::Replace(value) => value.received_at,
        Command::MassCancel(value) => value.received_at,
        Command::AccountControl(value) => value.received_at,
        Command::TradingStateControl(value) => value.received_at,
        Command::ExpirySweep(value) => value.received_at,
    }
}

fn validate_report_outcome(report: &ExecutionReport) -> Result<(), MarketDataError> {
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
        return Err(MarketDataError::TraceMismatch(
            "matching report event sequences are not contiguous",
        ));
    }
    match report.outcome {
        CommandOutcome::Accepted => {
            if report
                .events
                .iter()
                .any(|event| matches!(event.kind, EventKind::CommandRejected(_)))
            {
                return Err(MarketDataError::TraceMismatch(
                    "accepted report contains a rejection event",
                ));
            }
        }
        CommandOutcome::Rejected(reason) => {
            if report.events.len() != 1
                || !matches!(
                    report.events[0].kind,
                    EventKind::CommandRejected(actual) if actual == reason
                )
            {
                return Err(MarketDataError::TraceMismatch(
                    "rejected report does not contain exactly its rejection event",
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod resource_limit_tests {
    use super::*;

    fn replica() -> MarketDataReplica {
        MarketDataReplica::try_with_limits(
            InstrumentId::new(1).unwrap(),
            InstrumentVersion::new(1).unwrap(),
            TradingState::Open,
            MarketDataLimitsSpec {
                max_active_orders: 2,
                max_price_levels_per_side: 2,
                max_account_controls: 1,
                max_batch_updates: 3,
            },
        )
        .unwrap()
    }

    #[test]
    fn invariant_validation_rejects_lost_arena_and_scratch_reservations() {
        let mut lost_arena = replica();
        lost_arena.bids.shrink_to_fit();
        assert_eq!(
            lost_arena.validate(),
            Err(MarketDataError::SourceDivergence(
                "replica price arena contradicts its configured limit"
            ))
        );

        let mut lost_scratch = replica();
        lost_scratch.batch_levels.shrink_to_fit();
        assert_eq!(
            lost_scratch.validate(),
            Err(MarketDataError::SourceDivergence(
                "replica batch scratch contradicts its fixed empty layout"
            ))
        );
    }
}
