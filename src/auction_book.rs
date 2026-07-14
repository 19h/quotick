//! Bounded crossed-interest collection for one call-auction instrument shard.
//!
//! This module owns active market and limit orders while an auction is
//! collecting interest. Unlike the continuous matcher, opposing limit prices
//! may lock or cross. Admission and cancellation mutate constructor-reserved
//! identity and price indexes without heap allocation.
//! Indicative discovery and immutable allocation-plan construction delegate to
//! [`crate::auction`]. A separate prepared transition pairs and consumes one
//! allocation process-locally; sequencing, phase authority, durable events,
//! recovery, risk, and settlement remain higher-layer responsibilities.

use std::collections::HashSet;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::auction::{
    AuctionAllocationError, AuctionAllocationLimits, AuctionAllocationPlan, AuctionClearing,
    AuctionError, AuctionLevel, AuctionMarketInterest, AuctionOrder, AuctionOrderConstraint,
    AuctionOrderFill, AuctionPriceBand, AuctionPriceGrid, AuctionPricePolicy,
    allocate_clearing_price_time, discover_bounded_clearing_price,
};
use crate::domain::{
    AccountId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side, TradeId,
};
use crate::indexed_avl::IndexedAvlMap;
use crate::instrument::{AdmissionError, InstrumentDefinition};

static NEXT_CALL_AUCTION_BOOK_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

fn next_call_auction_book_instance_id() -> Result<u64, CallAuctionConstructionError> {
    let mut current = NEXT_CALL_AUCTION_BOOK_INSTANCE_ID.load(Ordering::Relaxed);
    loop {
        if current == u64::MAX {
            return Err(CallAuctionConstructionError::BookInstanceIdExhausted);
        }
        match NEXT_CALL_AUCTION_BOOK_INSTANCE_ID.compare_exchange_weak(
            current,
            current + 1,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(current),
            Err(observed) => current = observed,
        }
    }
}

/// Raw construction values for a bounded call-auction collection book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionBookLimitsSpec {
    /// Maximum simultaneous active orders across both sides and constraint types.
    pub max_active_orders: usize,
    /// Maximum occupied limit prices independently on each side.
    pub max_price_levels_per_side: usize,
    /// Maximum accepted, never-reusable order identifiers.
    pub max_accepted_order_ids: usize,
}

/// Invalid bounded call-auction resource policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionBookLimitsError {
    /// Active-order maximum is zero.
    ZeroActiveOrders,
    /// Per-side limit-price maximum is zero.
    ZeroPriceLevels,
    /// Accepted-order-identifier maximum is zero.
    ZeroAcceptedOrderIds,
    /// More price levels per side than total active orders were configured.
    PriceLevelsExceedActiveOrders,
    /// Active-order capacity exceeds accepted-identifier capacity.
    ActiveOrdersExceedAcceptedOrderIds,
}

impl fmt::Display for CallAuctionBookLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroActiveOrders => "call-auction active-order limit is zero",
            Self::ZeroPriceLevels => "call-auction per-side price-level limit is zero",
            Self::ZeroAcceptedOrderIds => "call-auction accepted-order-identifier limit is zero",
            Self::PriceLevelsExceedActiveOrders => {
                "call-auction per-side price-level limit exceeds active-order limit"
            }
            Self::ActiveOrdersExceedAcceptedOrderIds => {
                "call-auction active-order limit exceeds accepted-order-identifier limit"
            }
        })
    }
}

impl std::error::Error for CallAuctionBookLimitsError {}

/// Validated finite resource policy for one call-auction collection book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionBookLimits {
    active_orders: usize,
    price_levels_per_side: usize,
    accepted_order_ids: usize,
}

impl CallAuctionBookLimits {
    /// Default maximum simultaneous active orders.
    pub const DEFAULT_MAX_ACTIVE_ORDERS: usize = 4_096;
    /// Default maximum occupied limit prices independently on each side.
    pub const DEFAULT_MAX_PRICE_LEVELS_PER_SIDE: usize = 4_096;
    /// Default maximum accepted, never-reusable order identifiers.
    pub const DEFAULT_MAX_ACCEPTED_ORDER_IDS: usize = 65_536;

    /// Validates one finite resource policy.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionBookLimitsError`] for zero or incoherent bounds.
    pub const fn new(spec: CallAuctionBookLimitsSpec) -> Result<Self, CallAuctionBookLimitsError> {
        if spec.max_active_orders == 0 {
            return Err(CallAuctionBookLimitsError::ZeroActiveOrders);
        }
        if spec.max_price_levels_per_side == 0 {
            return Err(CallAuctionBookLimitsError::ZeroPriceLevels);
        }
        if spec.max_accepted_order_ids == 0 {
            return Err(CallAuctionBookLimitsError::ZeroAcceptedOrderIds);
        }
        if spec.max_price_levels_per_side > spec.max_active_orders {
            return Err(CallAuctionBookLimitsError::PriceLevelsExceedActiveOrders);
        }
        if spec.max_active_orders > spec.max_accepted_order_ids {
            return Err(CallAuctionBookLimitsError::ActiveOrdersExceedAcceptedOrderIds);
        }
        Ok(Self {
            active_orders: spec.max_active_orders,
            price_levels_per_side: spec.max_price_levels_per_side,
            accepted_order_ids: spec.max_accepted_order_ids,
        })
    }

    /// Maximum simultaneous active orders.
    #[must_use]
    pub const fn max_active_orders(self) -> usize {
        self.active_orders
    }

    /// Maximum occupied limit prices independently on each side.
    #[must_use]
    pub const fn max_price_levels_per_side(self) -> usize {
        self.price_levels_per_side
    }

    /// Maximum accepted, never-reusable order identifiers.
    #[must_use]
    pub const fn max_accepted_order_ids(self) -> usize {
        self.accepted_order_ids
    }
}

impl Default for CallAuctionBookLimits {
    fn default() -> Self {
        Self::new(CallAuctionBookLimitsSpec {
            max_active_orders: Self::DEFAULT_MAX_ACTIVE_ORDERS,
            max_price_levels_per_side: Self::DEFAULT_MAX_PRICE_LEVELS_PER_SIDE,
            max_accepted_order_ids: Self::DEFAULT_MAX_ACCEPTED_ORDER_IDS,
        })
        .expect("built-in call-auction limits are valid")
    }
}

/// A constructor-reserved call-auction resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionCapacity {
    /// Stable active order-identity arena.
    ActiveOrders,
    /// Stable never-reusable accepted-order-identity arena.
    AcceptedOrderIds,
    /// Stable bid limit-price arena.
    BidPriceLevels,
    /// Stable ask limit-price arena.
    AskPriceLevels,
    /// Canonical aggregate bid scratch.
    BidLevelScratch,
    /// Canonical aggregate ask scratch.
    AskLevelScratch,
    /// Canonical buy order scratch.
    BuyOrderScratch,
    /// Canonical sell order scratch.
    SellOrderScratch,
    /// Exact deterministic counterparty-pair vector for one prepared uncross.
    UncrossTrades,
    /// Exact unexecuted-remainder cancellation vector for one prepared uncross.
    UncrossCancellations,
}

impl fmt::Display for CallAuctionCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ActiveOrders => "active orders",
            Self::AcceptedOrderIds => "accepted order identifiers",
            Self::BidPriceLevels => "bid price levels",
            Self::AskPriceLevels => "ask price levels",
            Self::BidLevelScratch => "aggregate bid scratch",
            Self::AskLevelScratch => "aggregate ask scratch",
            Self::BuyOrderScratch => "canonical buy-order scratch",
            Self::SellOrderScratch => "canonical sell-order scratch",
            Self::UncrossTrades => "prepared uncross trades",
            Self::UncrossCancellations => "prepared uncross cancellations",
        })
    }
}

/// Failure while constructing a call-auction collection book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionConstructionError {
    /// One complete bounded allocation could not be reserved.
    CapacityReservationFailed(CallAuctionCapacity),
    /// Instrument price metadata could not form an auction grid and band.
    InvalidInstrumentPriceConfiguration(AuctionError),
    /// Process-local collection-book identity was exhausted.
    BookInstanceIdExhausted,
}

impl fmt::Display for CallAuctionConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityReservationFailed(capacity) => {
                write!(formatter, "call-auction {capacity} reservation failed")
            }
            Self::InvalidInstrumentPriceConfiguration(error) => {
                write!(
                    formatter,
                    "invalid call-auction instrument price configuration: {error}"
                )
            }
            Self::BookInstanceIdExhausted => {
                formatter.write_str("call-auction book instance identity exhausted")
            }
        }
    }
}

impl std::error::Error for CallAuctionConstructionError {}

/// One order submitted to a single-instrument call-auction collection shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionOrder {
    order_id: OrderId,
    account_id: AccountId,
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: Quantity,
}

impl CallAuctionOrder {
    /// Constructs one already domain-validated auction order request.
    #[must_use]
    pub const fn new(
        order_id: OrderId,
        account_id: AccountId,
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        side: Side,
        constraint: AuctionOrderConstraint,
        quantity: Quantity,
    ) -> Self {
        Self {
            order_id,
            account_id,
            instrument_id,
            instrument_version,
            side,
            constraint,
            quantity,
        }
    }

    /// Returns the order identifier.
    #[must_use]
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    /// Returns the owner account.
    #[must_use]
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }

    /// Returns the routed instrument.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the immutable instrument version.
    #[must_use]
    pub const fn instrument_version(self) -> InstrumentVersion {
        self.instrument_version
    }

    /// Returns the economic side.
    #[must_use]
    pub const fn side(self) -> Side {
        self.side
    }

    /// Returns the market or limit constraint.
    #[must_use]
    pub const fn constraint(self) -> AuctionOrderConstraint {
        self.constraint
    }

    /// Returns the positive quantity in instrument-defined lots.
    #[must_use]
    pub const fn quantity(self) -> Quantity {
        self.quantity
    }
}

/// Visible immutable state of one active call-auction order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionOrderSnapshot {
    /// Order identifier.
    pub order_id: OrderId,
    /// Owner account.
    pub account_id: AccountId,
    /// Side.
    pub side: Side,
    /// Market or limit constraint.
    pub constraint: AuctionOrderConstraint,
    /// Active quantity.
    pub quantity: Quantity,
    /// Shard-assigned strict time-priority sequence.
    pub priority_sequence: u64,
}

/// One anonymized aggregate limit-price level in a call-auction collection book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionLevelSnapshot {
    side: Side,
    price: Price,
    quantity: u128,
    order_count: u64,
}

impl CallAuctionLevelSnapshot {
    pub(crate) const fn from_parts(
        side: Side,
        price: Price,
        quantity: u128,
        order_count: u64,
    ) -> Option<Self> {
        if quantity == 0 || order_count == 0 {
            None
        } else {
            Some(Self {
                side,
                price,
                quantity,
                order_count,
            })
        }
    }

    /// Returns the book side.
    #[must_use]
    pub const fn side(self) -> Side {
        self.side
    }

    /// Returns the limit price in instrument-defined quanta.
    #[must_use]
    pub const fn price(self) -> Price {
        self.price
    }

    /// Returns aggregate active quantity in instrument-defined lots.
    #[must_use]
    pub const fn quantity(self) -> u128 {
        self.quantity
    }

    /// Returns the number of active orders at this price.
    #[must_use]
    pub const fn order_count(self) -> u64 {
        self.order_count
    }
}

/// Admission failure for one call-auction order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionAdmissionError {
    /// Routing, version, price, or quantity violated immutable instrument rules.
    Instrument(AdmissionError),
    /// The identifier was previously accepted and is never reusable.
    DuplicateOrderId,
    /// One configured finite resource had no remaining semantic capacity.
    CapacityExceeded(CallAuctionCapacity),
    /// The internal strict time-priority sequence was exhausted.
    PrioritySequenceExhausted,
    /// The mutation revision was exhausted.
    StateRevisionExhausted,
    /// A maintained aggregate exceeded `u128`.
    AggregateQuantityOverflow(Side),
}

impl fmt::Display for CallAuctionAdmissionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Instrument(error) => {
                write!(formatter, "call-auction instrument admission: {error:?}")
            }
            Self::DuplicateOrderId => {
                formatter.write_str("duplicate call-auction order identifier")
            }
            Self::CapacityExceeded(capacity) => {
                write!(formatter, "call-auction {capacity} capacity exceeded")
            }
            Self::PrioritySequenceExhausted => {
                formatter.write_str("call-auction priority sequence exhausted")
            }
            Self::StateRevisionExhausted => {
                formatter.write_str("call-auction state revision exhausted")
            }
            Self::AggregateQuantityOverflow(side) => {
                write!(
                    formatter,
                    "call-auction {side:?} aggregate quantity overflow"
                )
            }
        }
    }
}

impl std::error::Error for CallAuctionAdmissionError {}

/// Cancellation failure for one active call-auction order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionCancelError {
    /// No active order had the supplied identifier.
    UnknownOrder,
    /// The supplied account did not own the active order.
    AccountMismatch,
    /// The mutation revision was exhausted.
    StateRevisionExhausted,
}

impl fmt::Display for CallAuctionCancelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownOrder => "unknown active call-auction order",
            Self::AccountMismatch => "call-auction cancellation account mismatch",
            Self::StateRevisionExhausted => "call-auction state revision exhausted",
        })
    }
}

impl std::error::Error for CallAuctionCancelError {}

/// One indicative clearing result bound to an exact collection-book revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionIndicative {
    book_instance_id: u64,
    state_revision: u64,
    clearing: AuctionClearing,
}

impl CallAuctionIndicative {
    /// Returns the analytical aggregate clearing state.
    #[must_use]
    pub const fn clearing(self) -> AuctionClearing {
        self.clearing
    }

    /// Returns the exact book revision observed by discovery.
    #[must_use]
    pub const fn state_revision(self) -> u64 {
        self.state_revision
    }
}

/// Failure while converting an indicative result into an allocation plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionPlanError {
    /// The result originated from another process-local book instance.
    ForeignBook,
    /// Active interest changed after indicative discovery.
    StaleRevision {
        /// Revision carried by the result.
        observed: u64,
        /// Current collection-book revision.
        current: u64,
    },
    /// The analytical allocator rejected current canonical interest or failed reservation.
    Allocation(AuctionAllocationError),
}

impl fmt::Display for CallAuctionPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignBook => formatter.write_str("foreign call-auction indicative result"),
            Self::StaleRevision { observed, current } => write!(
                formatter,
                "stale call-auction indicative revision {observed}; current revision is {current}"
            ),
            Self::Allocation(error) => write!(formatter, "call-auction allocation: {error}"),
        }
    }
}

impl std::error::Error for CallAuctionPlanError {}

/// Treatment of positive active quantity remaining after one uncross.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionRemainderPolicy {
    /// Retain all market and limit remainder for a later externally controlled phase.
    RetainAll,
    /// Cancel market remainder while retaining limit remainder at existing priority.
    CancelMarket,
    /// Cancel every positive unexecuted remainder.
    CancelAll,
}

impl CallAuctionRemainderPolicy {
    const fn cancels(self, constraint: AuctionOrderConstraint) -> bool {
        match self {
            Self::RetainAll => false,
            Self::CancelMarket => matches!(constraint, AuctionOrderConstraint::Market),
            Self::CancelAll => true,
        }
    }
}

/// Counterparty self-trade treatment during deterministic auction pairing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionSelfTradePolicy {
    /// Permit a pair whose buy and sell accounts are identical.
    Permit,
}

/// Explicit policy applied by one atomic call-auction uncross.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionUncrossPolicy {
    remainder: CallAuctionRemainderPolicy,
    self_trade: CallAuctionSelfTradePolicy,
}

impl CallAuctionUncrossPolicy {
    /// Creates an explicit uncross policy.
    #[must_use]
    pub const fn new(
        remainder: CallAuctionRemainderPolicy,
        self_trade: CallAuctionSelfTradePolicy,
    ) -> Self {
        Self {
            remainder,
            self_trade,
        }
    }

    /// Returns the unexecuted-remainder treatment.
    #[must_use]
    pub const fn remainder(self) -> CallAuctionRemainderPolicy {
        self.remainder
    }

    /// Returns the counterparty self-trade treatment.
    #[must_use]
    pub const fn self_trade(self) -> CallAuctionSelfTradePolicy {
        self.self_trade
    }
}

/// One deterministic buyer/seller pair at the common auction clearing price.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionTrade {
    trade_id: TradeId,
    buy_order_id: OrderId,
    buy_account_id: AccountId,
    sell_order_id: OrderId,
    sell_account_id: AccountId,
    price: Price,
    quantity: Quantity,
}

impl CallAuctionTrade {
    pub(crate) const fn from_decoded(
        trade_id: TradeId,
        buy_order_id: OrderId,
        buy_account_id: AccountId,
        sell_order_id: OrderId,
        sell_account_id: AccountId,
        price: Price,
        quantity: Quantity,
    ) -> Option<Self> {
        if buy_order_id.get() == sell_order_id.get() {
            return None;
        }
        Some(Self {
            trade_id,
            buy_order_id,
            buy_account_id,
            sell_order_id,
            sell_account_id,
            price,
            quantity,
        })
    }

    /// Returns the book-local monotonic trade identifier.
    #[must_use]
    pub const fn trade_id(self) -> TradeId {
        self.trade_id
    }

    /// Returns the allocated buy order.
    #[must_use]
    pub const fn buy_order_id(self) -> OrderId {
        self.buy_order_id
    }

    /// Returns the buyer account.
    #[must_use]
    pub const fn buy_account_id(self) -> AccountId {
        self.buy_account_id
    }

    /// Returns the allocated sell order.
    #[must_use]
    pub const fn sell_order_id(self) -> OrderId {
        self.sell_order_id
    }

    /// Returns the seller account.
    #[must_use]
    pub const fn sell_account_id(self) -> AccountId {
        self.sell_account_id
    }

    /// Returns the common clearing price in instrument-defined quanta.
    #[must_use]
    pub const fn price(self) -> Price {
        self.price
    }

    /// Returns the positive paired quantity in instrument-defined lots.
    #[must_use]
    pub const fn quantity(self) -> Quantity {
        self.quantity
    }
}

/// One positive post-allocation remainder canceled by uncross policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionCancellation {
    order_id: OrderId,
    account_id: AccountId,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: Quantity,
    executed_quantity_lots: u64,
}

impl CallAuctionCancellation {
    pub(crate) const fn from_decoded(
        order_id: OrderId,
        account_id: AccountId,
        side: Side,
        constraint: AuctionOrderConstraint,
        quantity: Quantity,
        executed_quantity_lots: u64,
    ) -> Self {
        Self {
            order_id,
            account_id,
            side,
            constraint,
            quantity,
            executed_quantity_lots,
        }
    }

    /// Returns the canceled order identifier.
    #[must_use]
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    /// Returns the owner account.
    #[must_use]
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }

    /// Returns the order side.
    #[must_use]
    pub const fn side(self) -> Side {
        self.side
    }

    /// Returns the order's market or limit constraint.
    #[must_use]
    pub const fn constraint(self) -> AuctionOrderConstraint {
        self.constraint
    }

    /// Returns positive unexecuted quantity canceled after allocation.
    #[must_use]
    pub const fn quantity(self) -> Quantity {
        self.quantity
    }

    /// Returns quantity executed from the source order before cancellation.
    #[must_use]
    pub const fn executed_quantity_lots(self) -> u64 {
        self.executed_quantity_lots
    }
}

/// Failure while fully preparing an allocation, pairing, and remainder plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionPrepareError {
    /// Indicative result or analytical allocation failed validation.
    Plan(CallAuctionPlanError),
    /// Book-local trade identifier space cannot represent the complete pairing.
    TradeIdentifierExhausted,
    /// Collection mutation revision cannot represent the atomic commit.
    StateRevisionExhausted,
    /// Exact trade or cancellation result reservation failed.
    CapacityReservationFailed(CallAuctionCapacity),
    /// Private collection state contradicted its validated analytical plan.
    InternalInvariantViolation,
}

impl fmt::Display for CallAuctionPrepareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plan(error) => write!(formatter, "call-auction uncross plan: {error}"),
            Self::TradeIdentifierExhausted => {
                formatter.write_str("call-auction trade identifier exhausted")
            }
            Self::StateRevisionExhausted => {
                formatter.write_str("call-auction state revision exhausted")
            }
            Self::CapacityReservationFailed(capacity) => {
                write!(formatter, "call-auction {capacity} reservation failed")
            }
            Self::InternalInvariantViolation => {
                formatter.write_str("call-auction private state contradicts prepared uncross")
            }
        }
    }
}

impl std::error::Error for CallAuctionPrepareError {}

/// An immutable, process-local, revision-bound auction uncross preparation.
#[derive(Debug)]
pub struct PreparedCallAuctionUncross {
    book_instance_id: u64,
    state_revision: u64,
    next_state_revision: u64,
    next_trade_id: u64,
    policy: CallAuctionUncrossPolicy,
    plan: AuctionAllocationPlan,
    trades: Vec<CallAuctionTrade>,
    cancellations: Vec<CallAuctionCancellation>,
}

impl PreparedCallAuctionUncross {
    /// Returns the exact source collection revision.
    #[must_use]
    pub const fn state_revision(&self) -> u64 {
        self.state_revision
    }

    /// Returns the explicit uncross policy.
    #[must_use]
    pub const fn policy(&self) -> CallAuctionUncrossPolicy {
        self.policy
    }

    /// Returns the immutable order-level allocation plan.
    #[must_use]
    pub const fn allocation_plan(&self) -> &AuctionAllocationPlan {
        &self.plan
    }

    /// Returns proposed counterparty pairs in deterministic priority-walk order.
    #[must_use]
    pub fn trades(&self) -> &[CallAuctionTrade] {
        &self.trades
    }

    /// Returns proposed policy-driven remainder cancellations.
    #[must_use]
    pub fn cancellations(&self) -> &[CallAuctionCancellation] {
        &self.cancellations
    }
}

/// Failure while committing a previously complete uncross preparation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionCommitError {
    /// Preparation belongs to another process-local collection book.
    ForeignPreparation,
    /// Active collection state changed after preparation.
    StalePreparation {
        /// Revision carried by the preparation.
        observed: u64,
        /// Current book revision.
        current: u64,
    },
    /// Private state contradicted a same-instance, same-revision preparation.
    InternalInvariantViolation,
}

impl fmt::Display for CallAuctionCommitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ForeignPreparation => {
                formatter.write_str("foreign call-auction uncross preparation")
            }
            Self::StalePreparation { observed, current } => write!(
                formatter,
                "stale call-auction uncross revision {observed}; current revision is {current}"
            ),
            Self::InternalInvariantViolation => {
                formatter.write_str("call-auction state contradicts uncross preparation")
            }
        }
    }
}

impl std::error::Error for CallAuctionCommitError {}

/// One atomically committed call-auction uncross result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionUncross {
    state_revision: u64,
    policy: CallAuctionUncrossPolicy,
    plan: AuctionAllocationPlan,
    trades: Vec<CallAuctionTrade>,
    cancellations: Vec<CallAuctionCancellation>,
}

impl CallAuctionUncross {
    /// Returns the collection revision after atomic commit.
    #[must_use]
    pub const fn state_revision(&self) -> u64 {
        self.state_revision
    }

    /// Returns the applied policy.
    #[must_use]
    pub const fn policy(&self) -> CallAuctionUncrossPolicy {
        self.policy
    }

    /// Returns the allocation consumed by this commit.
    #[must_use]
    pub const fn allocation_plan(&self) -> &AuctionAllocationPlan {
        &self.plan
    }

    /// Returns deterministic counterparty pairs.
    #[must_use]
    pub fn trades(&self) -> &[CallAuctionTrade] {
        &self.trades
    }

    /// Returns remainders canceled by policy.
    #[must_use]
    pub fn cancellations(&self) -> &[CallAuctionCancellation] {
        &self.cancellations
    }
}

/// Process-local allocation telemetry for one call-auction collection book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionResourceStatus {
    /// Configured active-order maximum.
    pub configured_active_orders: usize,
    /// Constructor-reserved active-order slots.
    pub allocated_active_orders: usize,
    /// Currently active orders.
    pub active_orders: usize,
    /// Configured accepted-identifier maximum.
    pub configured_accepted_order_ids: usize,
    /// Constructor-reserved accepted-identifier slots.
    pub allocated_accepted_order_ids: usize,
    /// Accepted identifiers retained, including canceled orders.
    pub accepted_order_ids: usize,
    /// Constructor-reserved bid price slots.
    pub allocated_bid_price_levels: usize,
    /// Constructor-reserved ask price slots.
    pub allocated_ask_price_levels: usize,
    /// Occupied bid prices.
    pub bid_price_levels: usize,
    /// Occupied ask prices.
    pub ask_price_levels: usize,
    /// Aggregate-bid scratch capacity.
    pub bid_level_scratch: usize,
    /// Aggregate-ask scratch capacity.
    pub ask_level_scratch: usize,
    /// Canonical buy-order scratch capacity.
    pub buy_order_scratch: usize,
    /// Canonical sell-order scratch capacity.
    pub sell_order_scratch: usize,
}

/// Structural corruption detected by an offline call-auction audit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionInvariantViolation {
    detail: String,
}

impl CallAuctionInvariantViolation {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    /// Returns a stable diagnostic description.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for CallAuctionInvariantViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(formatter)
    }
}

impl std::error::Error for CallAuctionInvariantViolation {}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AuctionQueue {
    head: Option<OrderId>,
    tail: Option<OrderId>,
    total_quantity: u128,
    order_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RestingAuctionOrder {
    order_id: OrderId,
    account_id: AccountId,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: Quantity,
    priority_sequence: u64,
    previous: Option<OrderId>,
    next: Option<OrderId>,
}

impl From<RestingAuctionOrder> for CallAuctionOrderSnapshot {
    fn from(order: RestingAuctionOrder) -> Self {
        Self {
            order_id: order.order_id,
            account_id: order.account_id,
            side: order.side,
            constraint: order.constraint,
            quantity: order.quantity,
            priority_sequence: order.priority_sequence,
        }
    }
}

#[derive(Debug)]
struct AuctionPriceLevels {
    side: Side,
    by_price: IndexedAvlMap<Price, AuctionQueue>,
}

impl AuctionPriceLevels {
    fn try_new(
        side: Side,
        maximum_levels: usize,
    ) -> Result<Self, std::collections::TryReserveError> {
        Ok(Self {
            side,
            by_price: IndexedAvlMap::try_with_capacity(maximum_levels)?,
        })
    }

    fn len(&self) -> usize {
        self.by_price.len()
    }

    fn get(&self, price: Price) -> Option<&AuctionQueue> {
        self.by_price.get(&price)
    }

    fn insert(&mut self, price: Price, queue: AuctionQueue) -> Option<AuctionQueue> {
        self.by_price.insert(price, queue)
    }

    fn remove(&mut self, price: Price) -> Option<AuctionQueue> {
        self.by_price.remove(&price)
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&Price, &AuctionQueue)> {
        self.by_price.iter()
    }

    fn best_price(&self) -> Option<Price> {
        match self.side {
            Side::Buy => self.by_price.last_key_value().map(|(price, _)| *price),
            Side::Sell => self.by_price.first_key_value().map(|(price, _)| *price),
        }
    }
}

/// Bounded active order state for one call-auction instrument version.
#[derive(Debug)]
pub struct CallAuctionBook {
    instance_id: u64,
    definition: InstrumentDefinition,
    limits: CallAuctionBookLimits,
    grid: AuctionPriceGrid,
    instrument_band: AuctionPriceBand,
    orders: IndexedAvlMap<OrderId, RestingAuctionOrder>,
    seen_order_ids: IndexedAvlMap<OrderId, ()>,
    bids: AuctionPriceLevels,
    asks: AuctionPriceLevels,
    market_buys: AuctionQueue,
    market_sells: AuctionQueue,
    next_priority_sequence: u64,
    next_trade_id: u64,
    state_revision: u64,
    bid_level_scratch: Vec<AuctionLevel>,
    ask_level_scratch: Vec<AuctionLevel>,
    buy_order_scratch: Vec<AuctionOrder>,
    sell_order_scratch: Vec<AuctionOrder>,
}

impl CallAuctionBook {
    /// Creates an empty book under default finite limits.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionConstructionError`] if a complete bounded resource
    /// allocation cannot be reserved or instrument price metadata is invalid.
    pub fn try_new(definition: InstrumentDefinition) -> Result<Self, CallAuctionConstructionError> {
        Self::try_with_limits(definition, CallAuctionBookLimits::default())
    }

    /// Creates an empty book and reserves every mutation and analysis index.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionConstructionError`] naming the failed reservation
    /// or an instrument price configuration that cannot define an auction grid.
    pub fn try_with_limits(
        definition: InstrumentDefinition,
        limits: CallAuctionBookLimits,
    ) -> Result<Self, CallAuctionConstructionError> {
        let price_rules = definition.price_rules();
        let grid = AuctionPriceGrid::new(price_rules.tick_size())
            .map_err(CallAuctionConstructionError::InvalidInstrumentPriceConfiguration)?;
        let instrument_band =
            AuctionPriceBand::new(grid, price_rules.minimum(), price_rules.maximum())
                .map_err(CallAuctionConstructionError::InvalidInstrumentPriceConfiguration)?;
        let bids = AuctionPriceLevels::try_new(Side::Buy, limits.max_price_levels_per_side())
            .map_err(|_| {
                CallAuctionConstructionError::CapacityReservationFailed(
                    CallAuctionCapacity::BidPriceLevels,
                )
            })?;
        let asks = AuctionPriceLevels::try_new(Side::Sell, limits.max_price_levels_per_side())
            .map_err(|_| {
                CallAuctionConstructionError::CapacityReservationFailed(
                    CallAuctionCapacity::AskPriceLevels,
                )
            })?;
        let orders =
            IndexedAvlMap::try_with_capacity(limits.max_active_orders()).map_err(|_| {
                CallAuctionConstructionError::CapacityReservationFailed(
                    CallAuctionCapacity::ActiveOrders,
                )
            })?;
        let seen_order_ids = IndexedAvlMap::try_with_capacity(limits.max_accepted_order_ids())
            .map_err(|_| {
                CallAuctionConstructionError::CapacityReservationFailed(
                    CallAuctionCapacity::AcceptedOrderIds,
                )
            })?;
        let mut bid_level_scratch = Vec::new();
        bid_level_scratch
            .try_reserve_exact(limits.max_price_levels_per_side())
            .map_err(|_| {
                CallAuctionConstructionError::CapacityReservationFailed(
                    CallAuctionCapacity::BidLevelScratch,
                )
            })?;
        let mut ask_level_scratch = Vec::new();
        ask_level_scratch
            .try_reserve_exact(limits.max_price_levels_per_side())
            .map_err(|_| {
                CallAuctionConstructionError::CapacityReservationFailed(
                    CallAuctionCapacity::AskLevelScratch,
                )
            })?;
        let mut buy_order_scratch = Vec::new();
        buy_order_scratch
            .try_reserve_exact(limits.max_active_orders())
            .map_err(|_| {
                CallAuctionConstructionError::CapacityReservationFailed(
                    CallAuctionCapacity::BuyOrderScratch,
                )
            })?;
        let mut sell_order_scratch = Vec::new();
        sell_order_scratch
            .try_reserve_exact(limits.max_active_orders())
            .map_err(|_| {
                CallAuctionConstructionError::CapacityReservationFailed(
                    CallAuctionCapacity::SellOrderScratch,
                )
            })?;
        let instance_id = next_call_auction_book_instance_id()?;
        Ok(Self {
            instance_id,
            definition,
            limits,
            grid,
            instrument_band,
            orders,
            seen_order_ids,
            bids,
            asks,
            market_buys: AuctionQueue::default(),
            market_sells: AuctionQueue::default(),
            next_priority_sequence: 1,
            next_trade_id: 1,
            state_revision: 0,
            bid_level_scratch,
            ask_level_scratch,
            buy_order_scratch,
            sell_order_scratch,
        })
    }

    /// Returns the immutable instrument definition owned by this shard.
    #[must_use]
    pub const fn definition(&self) -> InstrumentDefinition {
        self.definition
    }

    /// Returns the finite resource policy.
    #[must_use]
    pub const fn limits(&self) -> CallAuctionBookLimits {
        self.limits
    }

    /// Returns the instrument tick grid.
    #[must_use]
    pub const fn price_grid(&self) -> AuctionPriceGrid {
        self.grid
    }

    /// Returns the immutable instrument collar as an auction candidate band.
    #[must_use]
    pub const fn instrument_price_band(&self) -> AuctionPriceBand {
        self.instrument_band
    }

    /// Returns the number of active orders.
    #[must_use]
    pub fn active_order_count(&self) -> usize {
        self.orders.len()
    }

    /// Returns the number of accepted identifiers, including canceled orders.
    #[must_use]
    pub fn accepted_order_id_count(&self) -> usize {
        self.seen_order_ids.len()
    }

    /// Returns the mutation revision of active collection state.
    #[must_use]
    pub const fn state_revision(&self) -> u64 {
        self.state_revision
    }

    /// Returns the next book-local trade identifier assigned by an uncross.
    #[must_use]
    pub const fn next_trade_id(&self) -> u64 {
        self.next_trade_id
    }

    pub(crate) const fn next_priority_sequence(&self) -> u64 {
        self.next_priority_sequence
    }

    pub(crate) fn checkpoint_active_orders(&self) -> Vec<CallAuctionOrderSnapshot> {
        self.orders
            .iter()
            .map(|(_, order)| (*order).into())
            .collect()
    }

    pub(crate) fn checkpoint_accepted_order_ids(&self) -> Vec<OrderId> {
        self.seen_order_ids
            .iter()
            .map(|(order_id, ())| *order_id)
            .collect()
    }

    pub(crate) fn from_checkpoint_state(
        definition: InstrumentDefinition,
        limits: CallAuctionBookLimits,
        active_orders: &[CallAuctionOrderSnapshot],
        accepted_order_ids: &[OrderId],
        next_priority_sequence: u64,
        next_trade_id: u64,
        state_revision: u64,
    ) -> Result<Self, String> {
        let mut book =
            Self::try_with_limits(definition, limits).map_err(|error| error.to_string())?;
        for order_id in accepted_order_ids.iter().copied() {
            if book.seen_order_ids.insert(order_id, ()).is_some() {
                return Err("auction checkpoint repeats an accepted order identifier".into());
            }
        }

        let mut ordered = active_orders.to_vec();
        ordered.sort_unstable_by_key(|order| order.priority_sequence);
        for snapshot in ordered {
            if book.seen_order_ids.get(&snapshot.order_id).is_none() {
                return Err("auction checkpoint active order lacks accepted identity".into());
            }
            let queue = book.queue_snapshot(snapshot.side, snapshot.constraint);
            let total_quantity = queue
                .total_quantity
                .checked_add(u128::from(snapshot.quantity.lots()))
                .ok_or_else(|| "auction checkpoint queue quantity overflows".to_owned())?;
            let order_count = queue
                .order_count
                .checked_add(1)
                .ok_or_else(|| "auction checkpoint queue count overflows".to_owned())?;
            let resting = RestingAuctionOrder {
                order_id: snapshot.order_id,
                account_id: snapshot.account_id,
                side: snapshot.side,
                constraint: snapshot.constraint,
                quantity: snapshot.quantity,
                priority_sequence: snapshot.priority_sequence,
                previous: queue.tail,
                next: None,
            };
            if let Some(tail) = queue.tail {
                let previous = book.orders.get_mut(&tail).ok_or_else(|| {
                    "auction checkpoint queue tail references an absent order".to_owned()
                })?;
                previous.next = Some(snapshot.order_id);
            }
            book.replace_queue(
                snapshot.side,
                snapshot.constraint,
                AuctionQueue {
                    head: queue.head.or(Some(snapshot.order_id)),
                    tail: Some(snapshot.order_id),
                    total_quantity,
                    order_count,
                },
            );
            if book.orders.insert(snapshot.order_id, resting).is_some() {
                return Err("auction checkpoint repeats an active order identifier".into());
            }
        }
        book.next_priority_sequence = next_priority_sequence;
        book.next_trade_id = next_trade_id;
        book.state_revision = state_revision;
        book.validate().map_err(|error| error.to_string())?;
        Ok(book)
    }

    /// Returns the number of occupied limit prices on one side.
    #[must_use]
    pub fn price_level_count(&self, side: Side) -> usize {
        self.levels(side).len()
    }

    /// Returns the most aggressive occupied limit price on one side.
    #[must_use]
    pub fn best_limit_price(&self, side: Side) -> Option<Price> {
        self.levels(side).best_price()
    }

    /// Returns active market quantity on one side in instrument-defined lots.
    #[must_use]
    pub fn market_quantity(&self, side: Side) -> u128 {
        self.market_queue(side).total_quantity
    }

    /// Returns the number of active market-constrained orders on one side.
    #[must_use]
    pub fn market_order_count(&self, side: Side) -> u64 {
        self.market_queue(side).order_count
    }

    /// Returns anonymized limit depth in best-to-worst price order.
    ///
    /// Market-constrained interest is reported separately by
    /// [`Self::market_quantity`] and [`Self::market_order_count`]. This query is
    /// `O(min(P, limit))` time and result space for `P` occupied prices.
    #[must_use]
    pub fn limit_depth(&self, side: Side, limit: usize) -> Vec<CallAuctionLevelSnapshot> {
        let level = |(&price, queue): (&Price, &AuctionQueue)| CallAuctionLevelSnapshot {
            side,
            price,
            quantity: queue.total_quantity,
            order_count: queue.order_count,
        };
        match side {
            Side::Buy => self
                .levels(side)
                .iter()
                .rev()
                .take(limit)
                .map(level)
                .collect(),
            Side::Sell => self.levels(side).iter().take(limit).map(level).collect(),
        }
    }

    /// Returns immutable state for one active order.
    #[must_use]
    pub fn order(&self, order_id: OrderId) -> Option<CallAuctionOrderSnapshot> {
        self.orders.get(&order_id).copied().map(Into::into)
    }

    /// Returns constructor allocation and current occupancy telemetry.
    #[must_use]
    pub fn resource_status(&self) -> CallAuctionResourceStatus {
        CallAuctionResourceStatus {
            configured_active_orders: self.limits.max_active_orders(),
            allocated_active_orders: self.orders.allocation_capacity(),
            active_orders: self.orders.len(),
            configured_accepted_order_ids: self.limits.max_accepted_order_ids(),
            allocated_accepted_order_ids: self.seen_order_ids.allocation_capacity(),
            accepted_order_ids: self.seen_order_ids.len(),
            allocated_bid_price_levels: self.bids.by_price.allocation_capacity(),
            allocated_ask_price_levels: self.asks.by_price.allocation_capacity(),
            bid_price_levels: self.bids.len(),
            ask_price_levels: self.asks.len(),
            bid_level_scratch: self.bid_level_scratch.capacity(),
            ask_level_scratch: self.ask_level_scratch.capacity(),
            buy_order_scratch: self.buy_order_scratch.capacity(),
            sell_order_scratch: self.sell_order_scratch.capacity(),
        }
    }

    /// Admits one market or limit order at strict shard-assigned time priority.
    ///
    /// The current implementation has one priority class (`0`): market orders
    /// precede limit orders, limit prices are ordered by aggressiveness, and
    /// admission sequence orders interest within an equal constraint.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionAdmissionError`] for routing/version or instrument
    /// rule violations, duplicate identity, finite-capacity exhaustion,
    /// sequence exhaustion, or checked aggregate overflow.
    pub fn admit(
        &mut self,
        command: CallAuctionOrder,
    ) -> Result<CallAuctionOrderSnapshot, CallAuctionAdmissionError> {
        self.preflight_admission(command)?;
        let next_priority_sequence = self
            .next_priority_sequence
            .checked_add(1)
            .ok_or(CallAuctionAdmissionError::PrioritySequenceExhausted)?;
        let next_state_revision = self
            .state_revision
            .checked_add(1)
            .ok_or(CallAuctionAdmissionError::StateRevisionExhausted)?;
        let queue = self.queue_snapshot(command.side, command.constraint);
        let total_quantity = queue
            .total_quantity
            .checked_add(u128::from(command.quantity.lots()))
            .ok_or(CallAuctionAdmissionError::AggregateQuantityOverflow(
                command.side,
            ))?;
        let order_count = queue.order_count.checked_add(1).ok_or(
            CallAuctionAdmissionError::AggregateQuantityOverflow(command.side),
        )?;
        let resting = RestingAuctionOrder {
            order_id: command.order_id,
            account_id: command.account_id,
            side: command.side,
            constraint: command.constraint,
            quantity: command.quantity,
            priority_sequence: self.next_priority_sequence,
            previous: queue.tail,
            next: None,
        };

        if let Some(tail) = queue.tail {
            if let Some(previous) = self.orders.get_mut(&tail) {
                previous.next = Some(command.order_id);
            }
        }
        let updated_queue = AuctionQueue {
            head: queue.head.or(Some(command.order_id)),
            tail: Some(command.order_id),
            total_quantity,
            order_count,
        };
        self.replace_queue(command.side, command.constraint, updated_queue);
        let replaced_order = self.orders.insert(command.order_id, resting);
        let replaced_identity = self.seen_order_ids.insert(command.order_id, ());
        debug_assert!(replaced_order.is_none());
        debug_assert!(replaced_identity.is_none());
        self.next_priority_sequence = next_priority_sequence;
        self.state_revision = next_state_revision;
        Ok(resting.into())
    }

    pub(crate) fn preflight_admission(
        &self,
        command: CallAuctionOrder,
    ) -> Result<(), CallAuctionAdmissionError> {
        self.validate_command(command)?;
        if self.seen_order_ids.get(&command.order_id).is_some() {
            return Err(CallAuctionAdmissionError::DuplicateOrderId);
        }
        if self.orders.len() >= self.limits.max_active_orders() {
            return Err(CallAuctionAdmissionError::CapacityExceeded(
                CallAuctionCapacity::ActiveOrders,
            ));
        }
        if self.seen_order_ids.len() >= self.limits.max_accepted_order_ids() {
            return Err(CallAuctionAdmissionError::CapacityExceeded(
                CallAuctionCapacity::AcceptedOrderIds,
            ));
        }
        if let AuctionOrderConstraint::Limit(price) = command.constraint {
            let levels = self.levels(command.side);
            if levels.get(price).is_none()
                && levels.len() >= self.limits.max_price_levels_per_side()
            {
                return Err(CallAuctionAdmissionError::CapacityExceeded(
                    match command.side {
                        Side::Buy => CallAuctionCapacity::BidPriceLevels,
                        Side::Sell => CallAuctionCapacity::AskPriceLevels,
                    },
                ));
            }
        }
        self.next_priority_sequence
            .checked_add(1)
            .ok_or(CallAuctionAdmissionError::PrioritySequenceExhausted)?;
        self.state_revision
            .checked_add(1)
            .ok_or(CallAuctionAdmissionError::StateRevisionExhausted)?;
        let queue = self.queue_snapshot(command.side, command.constraint);
        queue
            .total_quantity
            .checked_add(u128::from(command.quantity.lots()))
            .ok_or(CallAuctionAdmissionError::AggregateQuantityOverflow(
                command.side,
            ))?;
        queue.order_count.checked_add(1).ok_or(
            CallAuctionAdmissionError::AggregateQuantityOverflow(command.side),
        )?;
        Ok(())
    }

    /// Cancels one active order after exact owner verification.
    ///
    /// Accepted order identity remains retained and cannot be reused.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionCancelError`] when the order is absent or the
    /// supplied account does not own it.
    pub fn cancel(
        &mut self,
        account_id: AccountId,
        order_id: OrderId,
    ) -> Result<CallAuctionOrderSnapshot, CallAuctionCancelError> {
        let snapshot = self.preflight_cancel(account_id, order_id)?;
        let order = self
            .orders
            .get(&order_id)
            .copied()
            .ok_or(CallAuctionCancelError::UnknownOrder)?;
        let next_state_revision = self
            .state_revision
            .checked_add(1)
            .ok_or(CallAuctionCancelError::StateRevisionExhausted)?;
        self.remove_active_order(order);
        self.state_revision = next_state_revision;
        Ok(snapshot)
    }

    pub(crate) fn preflight_cancel(
        &self,
        account_id: AccountId,
        order_id: OrderId,
    ) -> Result<CallAuctionOrderSnapshot, CallAuctionCancelError> {
        let order = self
            .orders
            .get(&order_id)
            .copied()
            .ok_or(CallAuctionCancelError::UnknownOrder)?;
        if order.account_id != account_id {
            return Err(CallAuctionCancelError::AccountMismatch);
        }
        self.state_revision
            .checked_add(1)
            .ok_or(CallAuctionCancelError::StateRevisionExhausted)?;
        Ok(order.into())
    }

    /// Computes one indicative clearing result from current active interest.
    ///
    /// Aggregate scratch is reused without allocation. Limit prices outside a
    /// caller-selected dynamic band remain in the book and receive the bounded
    /// eligibility semantics of [`discover_bounded_clearing_price`].
    ///
    /// # Errors
    ///
    /// Returns [`AuctionError`] for an invalid dynamic band/reference or any
    /// aggregate-kernel validation/arithmetic failure.
    pub fn indicative_clearing(
        &mut self,
        price_band: AuctionPriceBand,
        reference_price: Price,
        policy: AuctionPricePolicy,
    ) -> Result<Option<CallAuctionIndicative>, AuctionError> {
        self.rebuild_level_scratch()?;
        discover_bounded_clearing_price(
            &self.bid_level_scratch,
            &self.ask_level_scratch,
            AuctionMarketInterest::new(
                self.market_buys.total_quantity,
                self.market_sells.total_quantity,
            ),
            self.grid,
            price_band,
            reference_price,
            policy,
        )
        .map(|clearing| {
            clearing.map(|clearing| CallAuctionIndicative {
                book_instance_id: self.instance_id,
                state_revision: self.state_revision,
                clearing,
            })
        })
    }

    /// Constructs an immutable price-time allocation plan for current interest.
    ///
    /// Canonical input scratch is reused without allocation. The returned fill
    /// vector capacity requests use the exact derived fill cardinalities and
    /// are fallible; the allocator may grant additional capacity.
    /// No active order state is mutated.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionPlanError`] when the indicative result is foreign or
    /// stale, current interest does not reconcile, or result reservation fails.
    pub fn allocation_plan(
        &mut self,
        indicative: CallAuctionIndicative,
    ) -> Result<AuctionAllocationPlan, CallAuctionPlanError> {
        if indicative.book_instance_id != self.instance_id {
            return Err(CallAuctionPlanError::ForeignBook);
        }
        if indicative.state_revision != self.state_revision {
            return Err(CallAuctionPlanError::StaleRevision {
                observed: indicative.state_revision,
                current: self.state_revision,
            });
        }
        self.rebuild_order_scratch();
        let limits = AuctionAllocationLimits::new(self.limits.max_active_orders())
            .map_err(CallAuctionPlanError::Allocation)?;
        allocate_clearing_price_time(
            &self.buy_order_scratch,
            &self.sell_order_scratch,
            self.grid,
            indicative.clearing,
            limits,
        )
        .map_err(CallAuctionPlanError::Allocation)
    }

    /// Fully prepares deterministic pairing and remainder treatment without
    /// mutating active orders, trade identity, or collection revision.
    ///
    /// The supplied policy explicitly permits self-trade; no preventive policy
    /// is inferred. Both side-fill vectors, the exact counterparty-pair vector,
    /// and the exact cancellation vector exist before a preparation is returned.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionPrepareError`] for foreign/stale indicative state,
    /// allocation mismatch, identifier/revision exhaustion, capacity reservation
    /// failure, or a private invariant contradiction.
    pub fn prepare_uncross(
        &mut self,
        indicative: CallAuctionIndicative,
        policy: CallAuctionUncrossPolicy,
    ) -> Result<PreparedCallAuctionUncross, CallAuctionPrepareError> {
        let next_state_revision = self
            .state_revision
            .checked_add(1)
            .ok_or(CallAuctionPrepareError::StateRevisionExhausted)?;
        let plan = self
            .allocation_plan(indicative)
            .map_err(CallAuctionPrepareError::Plan)?;
        let (trades, next_trade_id) = self.prepare_trade_pairs(&plan)?;
        let cancellations = self.prepare_remainder_cancellations(&plan, policy.remainder)?;
        Ok(PreparedCallAuctionUncross {
            book_instance_id: self.instance_id,
            state_revision: self.state_revision,
            next_state_revision,
            next_trade_id,
            policy,
            plan,
            trades,
            cancellations,
        })
    }

    /// Atomically consumes one same-instance, same-revision uncross preparation.
    ///
    /// Commit allocates no storage and performs no remaining business decision:
    /// positive fills reduce or remove orders, selected remainders are removed,
    /// and trade/revision counters advance together.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionCommitError`] before mutation for a foreign, stale,
    /// or internally contradictory preparation.
    pub fn commit_uncross(
        &mut self,
        prepared: PreparedCallAuctionUncross,
    ) -> Result<CallAuctionUncross, CallAuctionCommitError> {
        if prepared.book_instance_id != self.instance_id {
            return Err(CallAuctionCommitError::ForeignPreparation);
        }
        if prepared.state_revision != self.state_revision {
            return Err(CallAuctionCommitError::StalePreparation {
                observed: prepared.state_revision,
                current: self.state_revision,
            });
        }
        self.validate_uncross_preparation(&prepared)?;
        for fill in prepared.plan.buy_fills() {
            self.apply_fill(*fill);
        }
        for fill in prepared.plan.sell_fills() {
            self.apply_fill(*fill);
        }
        for cancellation in &prepared.cancellations {
            if let Some(order) = self.orders.get(&cancellation.order_id).copied() {
                self.remove_active_order(order);
            } else {
                debug_assert!(false, "validated remainder cancellation must remain active");
            }
        }
        self.next_trade_id = prepared.next_trade_id;
        self.state_revision = prepared.next_state_revision;
        Ok(CallAuctionUncross {
            state_revision: prepared.next_state_revision,
            policy: prepared.policy,
            plan: prepared.plan,
            trades: prepared.trades,
            cancellations: prepared.cancellations,
        })
    }

    /// Performs an allocation-using offline audit of every redundant index,
    /// intrusive queue, aggregate, priority relation, and finite bound.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionInvariantViolation`] at the first contradiction.
    pub fn validate(&self) -> Result<(), CallAuctionInvariantViolation> {
        self.validate_storage()?;
        let mut visited = HashSet::new();
        visited
            .try_reserve(self.orders.len())
            .map_err(|_| CallAuctionInvariantViolation::new("offline audit allocation failed"))?;
        self.validate_queue(
            self.market_buys,
            Side::Buy,
            AuctionOrderConstraint::Market,
            &mut visited,
        )?;
        self.validate_queue(
            self.market_sells,
            Side::Sell,
            AuctionOrderConstraint::Market,
            &mut visited,
        )?;
        for (price, queue) in self.bids.iter() {
            self.validate_queue(
                *queue,
                Side::Buy,
                AuctionOrderConstraint::Limit(*price),
                &mut visited,
            )?;
        }
        for (price, queue) in self.asks.iter() {
            self.validate_queue(
                *queue,
                Side::Sell,
                AuctionOrderConstraint::Limit(*price),
                &mut visited,
            )?;
        }
        if visited.len() != self.orders.len() {
            return Err(CallAuctionInvariantViolation::new(
                "active order index contains an unqueued order",
            ));
        }
        self.validate_active_orders()
    }

    fn validate_storage(&self) -> Result<(), CallAuctionInvariantViolation> {
        self.orders.validate().map_err(|detail| {
            CallAuctionInvariantViolation::new(format!("call-auction active-order AVL: {detail}"))
        })?;
        self.seen_order_ids.validate().map_err(|detail| {
            CallAuctionInvariantViolation::new(format!(
                "call-auction accepted-identifier AVL: {detail}"
            ))
        })?;
        self.bids.by_price.validate().map_err(|detail| {
            CallAuctionInvariantViolation::new(format!("call-auction bid AVL: {detail}"))
        })?;
        self.asks.by_price.validate().map_err(|detail| {
            CallAuctionInvariantViolation::new(format!("call-auction ask AVL: {detail}"))
        })?;
        if self.orders.len() > self.limits.max_active_orders() {
            return Err(CallAuctionInvariantViolation::new(
                "active orders exceed configured maximum",
            ));
        }
        if self.seen_order_ids.len() > self.limits.max_accepted_order_ids() {
            return Err(CallAuctionInvariantViolation::new(
                "accepted identifiers exceed configured maximum",
            ));
        }
        if self.bids.len() > self.limits.max_price_levels_per_side()
            || self.asks.len() > self.limits.max_price_levels_per_side()
        {
            return Err(CallAuctionInvariantViolation::new(
                "occupied limit prices exceed configured maximum",
            ));
        }
        let status = self.resource_status();
        if status.allocated_active_orders < status.configured_active_orders
            || status.allocated_accepted_order_ids < status.configured_accepted_order_ids
            || status.allocated_bid_price_levels < self.limits.max_price_levels_per_side()
            || status.allocated_ask_price_levels < self.limits.max_price_levels_per_side()
            || status.bid_level_scratch < self.limits.max_price_levels_per_side()
            || status.ask_level_scratch < self.limits.max_price_levels_per_side()
            || status.buy_order_scratch < self.limits.max_active_orders()
            || status.sell_order_scratch < self.limits.max_active_orders()
        {
            return Err(CallAuctionInvariantViolation::new(
                "constructor reservation is below a configured maximum",
            ));
        }
        Ok(())
    }

    fn validate_active_orders(&self) -> Result<(), CallAuctionInvariantViolation> {
        for (order_id, order) in self.orders.iter() {
            if *order_id != order.order_id || self.seen_order_ids.get(order_id).is_none() {
                return Err(CallAuctionInvariantViolation::new(
                    "active order identity is inconsistent or absent from accepted identities",
                ));
            }
            self.definition
                .quantity_rules()
                .validate_leaves(order.quantity)
                .map_err(|_| {
                    CallAuctionInvariantViolation::new(
                        "active order quantity violates instrument rules",
                    )
                })?;
            if let AuctionOrderConstraint::Limit(price) = order.constraint {
                self.definition.price_rules().validate(price).map_err(|_| {
                    CallAuctionInvariantViolation::new(
                        "active limit price violates instrument rules",
                    )
                })?;
            }
            if order.priority_sequence == 0
                || order.priority_sequence >= self.next_priority_sequence
            {
                return Err(CallAuctionInvariantViolation::new(
                    "active order priority sequence is outside the assigned prefix",
                ));
            }
        }
        Ok(())
    }

    fn prepare_trade_pairs(
        &self,
        plan: &AuctionAllocationPlan,
    ) -> Result<(Vec<CallAuctionTrade>, u64), CallAuctionPrepareError> {
        let pair_count = required_pair_count(plan.buy_fills(), plan.sell_fills())
            .ok_or(CallAuctionPrepareError::InternalInvariantViolation)?;
        let pair_count_u64 = u64::try_from(pair_count)
            .map_err(|_| CallAuctionPrepareError::TradeIdentifierExhausted)?;
        let next_trade_id = self
            .next_trade_id
            .checked_add(pair_count_u64)
            .ok_or(CallAuctionPrepareError::TradeIdentifierExhausted)?;
        let mut trades = Vec::new();
        trades.try_reserve_exact(pair_count).map_err(|_| {
            CallAuctionPrepareError::CapacityReservationFailed(CallAuctionCapacity::UncrossTrades)
        })?;
        let buys = plan.buy_fills();
        let sells = plan.sell_fills();
        let mut buy_index = 0_usize;
        let mut sell_index = 0_usize;
        let mut buy_remaining = buys.first().map_or(0, |fill| fill.quantity_lots());
        let mut sell_remaining = sells.first().map_or(0, |fill| fill.quantity_lots());
        while buy_index < buys.len() && sell_index < sells.len() {
            let quantity_lots = buy_remaining.min(sell_remaining);
            let buy_fill = buys[buy_index];
            let sell_fill = sells[sell_index];
            let buy_order = self
                .orders
                .get(&buy_fill.order_id())
                .copied()
                .ok_or(CallAuctionPrepareError::InternalInvariantViolation)?;
            let sell_order = self
                .orders
                .get(&sell_fill.order_id())
                .copied()
                .ok_or(CallAuctionPrepareError::InternalInvariantViolation)?;
            if buy_order.side != Side::Buy || sell_order.side != Side::Sell || quantity_lots == 0 {
                return Err(CallAuctionPrepareError::InternalInvariantViolation);
            }
            let offset = u64::try_from(trades.len())
                .map_err(|_| CallAuctionPrepareError::TradeIdentifierExhausted)?;
            let raw_trade_id = self
                .next_trade_id
                .checked_add(offset)
                .ok_or(CallAuctionPrepareError::TradeIdentifierExhausted)?;
            let trade_id = TradeId::new(raw_trade_id)
                .map_err(|_| CallAuctionPrepareError::InternalInvariantViolation)?;
            let quantity = Quantity::new(quantity_lots)
                .map_err(|_| CallAuctionPrepareError::InternalInvariantViolation)?;
            trades.push(CallAuctionTrade {
                trade_id,
                buy_order_id: buy_fill.order_id(),
                buy_account_id: buy_order.account_id,
                sell_order_id: sell_fill.order_id(),
                sell_account_id: sell_order.account_id,
                price: plan.clearing().price(),
                quantity,
            });
            buy_remaining -= quantity_lots;
            sell_remaining -= quantity_lots;
            if buy_remaining == 0 {
                buy_index += 1;
                buy_remaining = buys.get(buy_index).map_or(0, |fill| fill.quantity_lots());
            }
            if sell_remaining == 0 {
                sell_index += 1;
                sell_remaining = sells.get(sell_index).map_or(0, |fill| fill.quantity_lots());
            }
        }
        if trades.len() != pair_count
            || buy_index != buys.len()
            || sell_index != sells.len()
            || buy_remaining != 0
            || sell_remaining != 0
        {
            return Err(CallAuctionPrepareError::InternalInvariantViolation);
        }
        Ok((trades, next_trade_id))
    }

    fn prepare_remainder_cancellations(
        &self,
        plan: &AuctionAllocationPlan,
        policy: CallAuctionRemainderPolicy,
    ) -> Result<Vec<CallAuctionCancellation>, CallAuctionPrepareError> {
        let buy_count =
            required_cancellation_count(&self.buy_order_scratch, plan.buy_fills(), policy);
        let sell_count =
            required_cancellation_count(&self.sell_order_scratch, plan.sell_fills(), policy);
        let count = buy_count
            .checked_add(sell_count)
            .ok_or(CallAuctionPrepareError::InternalInvariantViolation)?;
        let mut cancellations = Vec::new();
        cancellations.try_reserve_exact(count).map_err(|_| {
            CallAuctionPrepareError::CapacityReservationFailed(
                CallAuctionCapacity::UncrossCancellations,
            )
        })?;
        append_remainder_cancellations(
            &self.buy_order_scratch,
            plan.buy_fills(),
            policy,
            &self.orders,
            &mut cancellations,
        )?;
        append_remainder_cancellations(
            &self.sell_order_scratch,
            plan.sell_fills(),
            policy,
            &self.orders,
            &mut cancellations,
        )?;
        if cancellations.len() != count {
            return Err(CallAuctionPrepareError::InternalInvariantViolation);
        }
        Ok(cancellations)
    }

    fn validate_uncross_preparation(
        &self,
        prepared: &PreparedCallAuctionUncross,
    ) -> Result<(), CallAuctionCommitError> {
        if self.state_revision.checked_add(1) != Some(prepared.next_state_revision) {
            return Err(CallAuctionCommitError::InternalInvariantViolation);
        }
        self.validate_prepared_fills(
            prepared.plan.buy_fills(),
            Side::Buy,
            &self.buy_order_scratch,
        )?;
        self.validate_prepared_fills(
            prepared.plan.sell_fills(),
            Side::Sell,
            &self.sell_order_scratch,
        )?;
        self.validate_prepared_trades(prepared)?;
        self.validate_prepared_cancellations(prepared)?;
        Ok(())
    }

    fn validate_prepared_trades(
        &self,
        prepared: &PreparedCallAuctionUncross,
    ) -> Result<(), CallAuctionCommitError> {
        let buys = prepared.plan.buy_fills();
        let sells = prepared.plan.sell_fills();
        let expected_count = required_pair_count(buys, sells)
            .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
        if prepared.trades.len() != expected_count {
            return Err(CallAuctionCommitError::InternalInvariantViolation);
        }
        let mut buy_index = 0_usize;
        let mut sell_index = 0_usize;
        let mut buy_remaining = buys.first().map_or(0_u64, |fill| fill.quantity_lots());
        let mut sell_remaining = sells.first().map_or(0_u64, |fill| fill.quantity_lots());
        let mut paired_quantity = 0_u128;
        for (index, trade) in prepared.trades.iter().copied().enumerate() {
            let buy_fill = buys
                .get(buy_index)
                .copied()
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            let sell_fill = sells
                .get(sell_index)
                .copied()
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            let expected_quantity_lots = buy_remaining.min(sell_remaining);
            if expected_quantity_lots == 0 {
                return Err(CallAuctionCommitError::InternalInvariantViolation);
            }
            let offset = u64::try_from(index)
                .map_err(|_| CallAuctionCommitError::InternalInvariantViolation)?;
            let raw_trade_id = self
                .next_trade_id
                .checked_add(offset)
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            let buy_order = self
                .orders
                .get(&trade.buy_order_id)
                .copied()
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            let sell_order = self
                .orders
                .get(&trade.sell_order_id)
                .copied()
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            if trade.trade_id.get() != raw_trade_id
                || trade.price != prepared.plan.clearing().price()
                || trade.buy_order_id != buy_fill.order_id()
                || trade.sell_order_id != sell_fill.order_id()
                || trade.quantity.lots() != expected_quantity_lots
                || buy_order.side != Side::Buy
                || sell_order.side != Side::Sell
                || buy_order.account_id != trade.buy_account_id
                || sell_order.account_id != trade.sell_account_id
            {
                return Err(CallAuctionCommitError::InternalInvariantViolation);
            }
            paired_quantity = paired_quantity
                .checked_add(u128::from(trade.quantity.lots()))
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            buy_remaining -= expected_quantity_lots;
            sell_remaining -= expected_quantity_lots;
            if buy_remaining == 0 {
                buy_index += 1;
                buy_remaining = buys
                    .get(buy_index)
                    .map_or(0_u64, |fill| fill.quantity_lots());
            }
            if sell_remaining == 0 {
                sell_index += 1;
                sell_remaining = sells
                    .get(sell_index)
                    .map_or(0_u64, |fill| fill.quantity_lots());
            }
        }
        let trade_count = u64::try_from(prepared.trades.len())
            .map_err(|_| CallAuctionCommitError::InternalInvariantViolation)?;
        if self.next_trade_id.checked_add(trade_count) != Some(prepared.next_trade_id)
            || paired_quantity != prepared.plan.executable_quantity()
            || buy_index != buys.len()
            || sell_index != sells.len()
            || buy_remaining != 0
            || sell_remaining != 0
        {
            return Err(CallAuctionCommitError::InternalInvariantViolation);
        }
        Ok(())
    }

    fn validate_prepared_cancellations(
        &self,
        prepared: &PreparedCallAuctionUncross,
    ) -> Result<(), CallAuctionCommitError> {
        let mut cancellation_index = 0_usize;
        self.validate_prepared_side_cancellations(
            &self.buy_order_scratch,
            prepared.plan.buy_fills(),
            prepared.policy.remainder,
            &prepared.cancellations,
            &mut cancellation_index,
        )?;
        self.validate_prepared_side_cancellations(
            &self.sell_order_scratch,
            prepared.plan.sell_fills(),
            prepared.policy.remainder,
            &prepared.cancellations,
            &mut cancellation_index,
        )?;
        if cancellation_index != prepared.cancellations.len() {
            return Err(CallAuctionCommitError::InternalInvariantViolation);
        }
        Ok(())
    }

    fn validate_prepared_side_cancellations(
        &self,
        auction_orders: &[AuctionOrder],
        fills: &[AuctionOrderFill],
        policy: CallAuctionRemainderPolicy,
        cancellations: &[CallAuctionCancellation],
        cancellation_index: &mut usize,
    ) -> Result<(), CallAuctionCommitError> {
        let mut fill_index = 0_usize;
        for (source_index, auction_order) in auction_orders.iter().copied().enumerate() {
            let executed_quantity_lots =
                fill_quantity_for_source(source_index, fills, &mut fill_index);
            let remaining_lots = auction_order
                .quantity()
                .lots()
                .checked_sub(executed_quantity_lots)
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            if remaining_lots == 0 || !policy.cancels(auction_order.constraint()) {
                continue;
            }
            let cancellation = cancellations
                .get(*cancellation_index)
                .copied()
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            let order = self
                .orders
                .get(&auction_order.order_id())
                .copied()
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            if cancellation.order_id != auction_order.order_id()
                || cancellation.quantity.lots() != remaining_lots
                || cancellation.executed_quantity_lots != executed_quantity_lots
                || order.account_id != cancellation.account_id
                || order.side != cancellation.side
                || order.constraint != cancellation.constraint
                || order.constraint != auction_order.constraint()
                || order.quantity != auction_order.quantity()
            {
                return Err(CallAuctionCommitError::InternalInvariantViolation);
            }
            *cancellation_index = cancellation_index
                .checked_add(1)
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
        }
        Ok(())
    }

    fn validate_prepared_fills(
        &self,
        fills: &[AuctionOrderFill],
        side: Side,
        auction_orders: &[AuctionOrder],
    ) -> Result<(), CallAuctionCommitError> {
        let mut previous_source_index = None;
        for fill in fills.iter().copied() {
            if previous_source_index.is_some_and(|previous| previous >= fill.source_index()) {
                return Err(CallAuctionCommitError::InternalInvariantViolation);
            }
            let auction_order = auction_orders
                .get(fill.source_index())
                .copied()
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            let order = self
                .orders
                .get(&fill.order_id())
                .copied()
                .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
            if auction_order.order_id() != fill.order_id()
                || auction_order.constraint() != order.constraint
                || auction_order.quantity() != order.quantity
                || order.side != side
                || fill.quantity_lots() == 0
                || fill.quantity_lots() > order.quantity.lots()
            {
                return Err(CallAuctionCommitError::InternalInvariantViolation);
            }
            previous_source_index = Some(fill.source_index());
        }
        Ok(())
    }

    fn apply_fill(&mut self, fill: AuctionOrderFill) {
        let Some(order) = self.orders.get(&fill.order_id()).copied() else {
            debug_assert!(false, "validated auction fill order must remain active");
            return;
        };
        if fill.quantity_lots() == order.quantity.lots() {
            self.remove_active_order(order);
            return;
        }
        let Some(remaining_lots) = order.quantity.lots().checked_sub(fill.quantity_lots()) else {
            debug_assert!(false, "validated auction fill cannot exceed leaves");
            return;
        };
        let Ok(remaining) = Quantity::new(remaining_lots) else {
            debug_assert!(false, "partial auction fill must leave positive quantity");
            return;
        };
        let mut queue = self.queue_snapshot(order.side, order.constraint);
        queue.total_quantity -= u128::from(fill.quantity_lots());
        self.replace_queue(order.side, order.constraint, queue);
        if let Some(active) = self.orders.get_mut(&order.order_id) {
            active.quantity = remaining;
        } else {
            debug_assert!(false, "validated partial fill order must remain indexed");
        }
    }

    fn remove_active_order(&mut self, order: RestingAuctionOrder) {
        if let Some(previous) = order.previous {
            if let Some(previous_order) = self.orders.get_mut(&previous) {
                previous_order.next = order.next;
            }
        }
        if let Some(next) = order.next {
            if let Some(next_order) = self.orders.get_mut(&next) {
                next_order.previous = order.previous;
            }
        }
        let mut queue = self.queue_snapshot(order.side, order.constraint);
        if queue.head == Some(order.order_id) {
            queue.head = order.next;
        }
        if queue.tail == Some(order.order_id) {
            queue.tail = order.previous;
        }
        queue.total_quantity -= u128::from(order.quantity.lots());
        queue.order_count -= 1;
        if queue.order_count == 0 {
            debug_assert_eq!(queue.total_quantity, 0);
            debug_assert!(queue.head.is_none() && queue.tail.is_none());
        }
        self.replace_queue(order.side, order.constraint, queue);
        let removed = self.orders.remove(&order.order_id);
        debug_assert_eq!(removed, Some(order));
    }

    fn validate_command(&self, command: CallAuctionOrder) -> Result<(), CallAuctionAdmissionError> {
        if command.instrument_id != self.definition.instrument_id() {
            return Err(CallAuctionAdmissionError::Instrument(
                AdmissionError::WrongInstrument,
            ));
        }
        if command.instrument_version != self.definition.version() {
            return Err(CallAuctionAdmissionError::Instrument(
                AdmissionError::WrongVersion,
            ));
        }
        self.definition
            .quantity_rules()
            .validate(command.quantity)
            .map_err(CallAuctionAdmissionError::Instrument)?;
        if let AuctionOrderConstraint::Limit(price) = command.constraint {
            self.definition
                .price_rules()
                .validate(price)
                .map_err(CallAuctionAdmissionError::Instrument)?;
        }
        Ok(())
    }

    fn levels(&self, side: Side) -> &AuctionPriceLevels {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn levels_mut(&mut self, side: Side) -> &mut AuctionPriceLevels {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn market_queue(&self, side: Side) -> &AuctionQueue {
        match side {
            Side::Buy => &self.market_buys,
            Side::Sell => &self.market_sells,
        }
    }

    fn queue_snapshot(&self, side: Side, constraint: AuctionOrderConstraint) -> AuctionQueue {
        match constraint {
            AuctionOrderConstraint::Market => *self.market_queue(side),
            AuctionOrderConstraint::Limit(price) => {
                self.levels(side).get(price).copied().unwrap_or_default()
            }
        }
    }

    fn replace_queue(
        &mut self,
        side: Side,
        constraint: AuctionOrderConstraint,
        queue: AuctionQueue,
    ) {
        match constraint {
            AuctionOrderConstraint::Market => match side {
                Side::Buy => self.market_buys = queue,
                Side::Sell => self.market_sells = queue,
            },
            AuctionOrderConstraint::Limit(price) if queue.order_count == 0 => {
                let removed = self.levels_mut(side).remove(price);
                debug_assert!(removed.is_some());
            }
            AuctionOrderConstraint::Limit(price) => {
                self.levels_mut(side).insert(price, queue);
            }
        }
    }

    fn rebuild_level_scratch(&mut self) -> Result<(), AuctionError> {
        self.bid_level_scratch.clear();
        for (price, queue) in self.bids.iter().rev() {
            self.bid_level_scratch
                .push(AuctionLevel::new(*price, queue.total_quantity)?);
        }
        self.ask_level_scratch.clear();
        for (price, queue) in self.asks.iter() {
            self.ask_level_scratch
                .push(AuctionLevel::new(*price, queue.total_quantity)?);
        }
        Ok(())
    }

    fn rebuild_order_scratch(&mut self) {
        self.buy_order_scratch.clear();
        append_queue_orders(self.market_buys, &self.orders, &mut self.buy_order_scratch);
        for (_, queue) in self.bids.iter().rev() {
            append_queue_orders(*queue, &self.orders, &mut self.buy_order_scratch);
        }
        self.sell_order_scratch.clear();
        append_queue_orders(
            self.market_sells,
            &self.orders,
            &mut self.sell_order_scratch,
        );
        for (_, queue) in self.asks.iter() {
            append_queue_orders(*queue, &self.orders, &mut self.sell_order_scratch);
        }
    }

    fn validate_queue(
        &self,
        queue: AuctionQueue,
        side: Side,
        constraint: AuctionOrderConstraint,
        visited: &mut HashSet<OrderId>,
    ) -> Result<(), CallAuctionInvariantViolation> {
        if queue.order_count == 0 {
            if queue.head.is_some() || queue.tail.is_some() || queue.total_quantity != 0 {
                return Err(CallAuctionInvariantViolation::new(
                    "empty auction queue has non-empty redundant state",
                ));
            }
            return Ok(());
        }
        if queue.head.is_none() || queue.tail.is_none() || queue.total_quantity == 0 {
            return Err(CallAuctionInvariantViolation::new(
                "non-empty auction queue lacks head, tail, or aggregate",
            ));
        }
        let mut current = queue.head;
        let mut previous = None;
        let mut previous_sequence = None;
        let mut total = 0_u128;
        let mut count = 0_u64;
        while let Some(order_id) = current {
            let order = self.orders.get(&order_id).copied().ok_or_else(|| {
                CallAuctionInvariantViolation::new("auction queue references an absent order")
            })?;
            if order.side != side
                || order.constraint != constraint
                || order.previous != previous
                || previous_sequence.is_some_and(|sequence| sequence >= order.priority_sequence)
            {
                return Err(CallAuctionInvariantViolation::new(
                    "auction queue membership, link, or priority is inconsistent",
                ));
            }
            if !visited.insert(order_id) {
                return Err(CallAuctionInvariantViolation::new(
                    "active order occurs in multiple auction queue positions",
                ));
            }
            total = total
                .checked_add(u128::from(order.quantity.lots()))
                .ok_or_else(|| {
                    CallAuctionInvariantViolation::new("auction queue quantity overflow")
                })?;
            count = count.checked_add(1).ok_or_else(|| {
                CallAuctionInvariantViolation::new("auction queue count overflow")
            })?;
            if count > queue.order_count {
                return Err(CallAuctionInvariantViolation::new(
                    "auction queue traversal exceeds recorded count",
                ));
            }
            previous = Some(order_id);
            previous_sequence = Some(order.priority_sequence);
            current = order.next;
        }
        if previous != queue.tail || count != queue.order_count || total != queue.total_quantity {
            return Err(CallAuctionInvariantViolation::new(
                "auction queue tail, count, or quantity aggregate is inconsistent",
            ));
        }
        Ok(())
    }
}

fn required_pair_count(buys: &[AuctionOrderFill], sells: &[AuctionOrderFill]) -> Option<usize> {
    let mut buy_index = 0_usize;
    let mut sell_index = 0_usize;
    let mut buy_remaining = buys.first()?.quantity_lots();
    let mut sell_remaining = sells.first()?.quantity_lots();
    let mut count = 0_usize;
    while buy_index < buys.len() && sell_index < sells.len() {
        let quantity = buy_remaining.min(sell_remaining);
        if quantity == 0 {
            return None;
        }
        count = count.checked_add(1)?;
        buy_remaining -= quantity;
        sell_remaining -= quantity;
        if buy_remaining == 0 {
            buy_index += 1;
            buy_remaining = buys.get(buy_index).map_or(0, |fill| fill.quantity_lots());
        }
        if sell_remaining == 0 {
            sell_index += 1;
            sell_remaining = sells.get(sell_index).map_or(0, |fill| fill.quantity_lots());
        }
    }
    (buy_index == buys.len()
        && sell_index == sells.len()
        && buy_remaining == 0
        && sell_remaining == 0)
        .then_some(count)
}

fn required_cancellation_count(
    orders: &[AuctionOrder],
    fills: &[AuctionOrderFill],
    policy: CallAuctionRemainderPolicy,
) -> usize {
    let mut fill_index = 0_usize;
    orders
        .iter()
        .enumerate()
        .filter(|(order_index, order)| {
            let executed = fill_quantity_for_source(*order_index, fills, &mut fill_index);
            executed < order.quantity().lots() && policy.cancels(order.constraint())
        })
        .count()
}

fn append_remainder_cancellations(
    auction_orders: &[AuctionOrder],
    fills: &[AuctionOrderFill],
    policy: CallAuctionRemainderPolicy,
    active_orders: &IndexedAvlMap<OrderId, RestingAuctionOrder>,
    output: &mut Vec<CallAuctionCancellation>,
) -> Result<(), CallAuctionPrepareError> {
    let mut fill_index = 0_usize;
    for (order_index, auction_order) in auction_orders.iter().copied().enumerate() {
        let executed_quantity_lots = fill_quantity_for_source(order_index, fills, &mut fill_index);
        let remaining_lots = auction_order
            .quantity()
            .lots()
            .checked_sub(executed_quantity_lots)
            .ok_or(CallAuctionPrepareError::InternalInvariantViolation)?;
        if remaining_lots == 0 || !policy.cancels(auction_order.constraint()) {
            continue;
        }
        let active = active_orders
            .get(&auction_order.order_id())
            .copied()
            .ok_or(CallAuctionPrepareError::InternalInvariantViolation)?;
        if active.constraint != auction_order.constraint()
            || active.quantity != auction_order.quantity()
        {
            return Err(CallAuctionPrepareError::InternalInvariantViolation);
        }
        let quantity = Quantity::new(remaining_lots)
            .map_err(|_| CallAuctionPrepareError::InternalInvariantViolation)?;
        output.push(CallAuctionCancellation {
            order_id: active.order_id,
            account_id: active.account_id,
            side: active.side,
            constraint: active.constraint,
            quantity,
            executed_quantity_lots,
        });
    }
    Ok(())
}

fn fill_quantity_for_source(
    source_index: usize,
    fills: &[AuctionOrderFill],
    fill_index: &mut usize,
) -> u64 {
    while fills
        .get(*fill_index)
        .is_some_and(|fill| fill.source_index() < source_index)
    {
        *fill_index += 1;
    }
    fills.get(*fill_index).map_or(0, |fill| {
        if fill.source_index() == source_index {
            *fill_index += 1;
            fill.quantity_lots()
        } else {
            0
        }
    })
}

fn append_queue_orders(
    queue: AuctionQueue,
    orders: &IndexedAvlMap<OrderId, RestingAuctionOrder>,
    output: &mut Vec<AuctionOrder>,
) {
    let mut current = queue.head;
    let mut appended = 0_u64;
    while let Some(order_id) = current {
        if appended >= queue.order_count {
            break;
        }
        let Some(order) = orders.get(&order_id).copied() else {
            break;
        };
        output.push(AuctionOrder::new(
            order.order_id,
            order.constraint,
            order.quantity,
            0,
            order.priority_sequence,
        ));
        appended += 1;
        current = order.next;
    }
}
