//! Bounded crossed-interest collection for one call-auction instrument shard.
//!
//! This module owns active market and limit orders while an auction is
//! collecting interest. Unlike the continuous matcher, opposing limit prices
//! may lock or cross. Admission and cancellation mutate constructor-reserved
//! identity and price indexes without heap allocation.
//! Indicative discovery and immutable allocation-plan construction delegate to
//! [`crate::auction`]. Constructor-owned leased buffers prepare, pair, and
//! consume uncrosses without hot-path allocation; sequencing, phase authority,
//! durable events, recovery, risk, and settlement remain higher-layer
//! responsibilities.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::auction::{
    AuctionAllocationError, AuctionAllocationLimits, AuctionAllocationMethod,
    AuctionAllocationPlan, AuctionAllocationPolicy, AuctionClearing, AuctionError, AuctionLevel,
    AuctionMarketInterest, AuctionOrder, AuctionOrderConstraint, AuctionOrderFill,
    AuctionPriceBand, AuctionPriceGrid, AuctionPricePolicy, AuctionPriorityClass,
    allocate_clearing_reusing, allocate_clearing_with_policy, compare_order_priority,
    discover_bounded_clearing_price,
};
use crate::bounded_hash::BoundedHashMap;
use crate::domain::{
    AccountId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side, TradeId,
};
use crate::indexed_avl::IndexedAvlMap;
use crate::instrument::{AdmissionError, InstrumentDefinition};
use crate::matching::MassCancelScope;

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
    /// Maximum simultaneously retained uncross preparations/results.
    pub max_prepared_uncrosses: usize,
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
    /// Prepared-uncross lease maximum is zero.
    ZeroPreparedUncrosses,
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
            Self::ZeroPreparedUncrosses => "call-auction prepared-uncross limit is zero",
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
    prepared_uncrosses: usize,
}

impl CallAuctionBookLimits {
    /// Default maximum simultaneous active orders.
    pub const DEFAULT_MAX_ACTIVE_ORDERS: usize = 4_096;
    /// Default maximum occupied limit prices independently on each side.
    pub const DEFAULT_MAX_PRICE_LEVELS_PER_SIDE: usize = 4_096;
    /// Default maximum accepted, never-reusable order identifiers.
    pub const DEFAULT_MAX_ACCEPTED_ORDER_IDS: usize = 65_536;
    /// Default maximum simultaneous prepared or externally retained uncrosses.
    pub const DEFAULT_MAX_PREPARED_UNCROSSES: usize = 2;

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
        if spec.max_prepared_uncrosses == 0 {
            return Err(CallAuctionBookLimitsError::ZeroPreparedUncrosses);
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
            prepared_uncrosses: spec.max_prepared_uncrosses,
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

    /// Maximum simultaneous prepared or externally retained uncrosses.
    #[must_use]
    pub const fn max_prepared_uncrosses(self) -> usize {
        self.prepared_uncrosses
    }
}

impl Default for CallAuctionBookLimits {
    fn default() -> Self {
        Self::new(CallAuctionBookLimitsSpec {
            max_active_orders: Self::DEFAULT_MAX_ACTIVE_ORDERS,
            max_price_levels_per_side: Self::DEFAULT_MAX_PRICE_LEVELS_PER_SIDE,
            max_accepted_order_ids: Self::DEFAULT_MAX_ACCEPTED_ORDER_IDS,
            max_prepared_uncrosses: Self::DEFAULT_MAX_PREPARED_UNCROSSES,
        })
        .expect("built-in call-auction limits are valid")
    }
}

/// A constructor-reserved call-auction resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionCapacity {
    /// Stable active order-identity arena.
    ActiveOrders,
    /// Fixed account-to-active-order index.
    ActiveAccounts,
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
    /// Constructor-owned buy-fill vectors for prepared uncrosses.
    UncrossBuyFills,
    /// Constructor-owned sell-fill vectors for prepared uncrosses.
    UncrossSellFills,
    /// Constructor-owned deterministic counterparty-pair vectors.
    UncrossTrades,
    /// Constructor-owned unexecuted-remainder cancellation vectors.
    UncrossCancellations,
    /// Fixed pool of simultaneously leased uncross buffer sets.
    PreparedUncrosses,
}

impl fmt::Display for CallAuctionCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ActiveOrders => "active orders",
            Self::ActiveAccounts => "active accounts",
            Self::AcceptedOrderIds => "accepted order identifiers",
            Self::BidPriceLevels => "bid price levels",
            Self::AskPriceLevels => "ask price levels",
            Self::BidLevelScratch => "aggregate bid scratch",
            Self::AskLevelScratch => "aggregate ask scratch",
            Self::BuyOrderScratch => "canonical buy-order scratch",
            Self::SellOrderScratch => "canonical sell-order scratch",
            Self::UncrossBuyFills => "prepared uncross buy fills",
            Self::UncrossSellFills => "prepared uncross sell fills",
            Self::UncrossTrades => "prepared uncross trades",
            Self::UncrossCancellations => "prepared uncross cancellations",
            Self::PreparedUncrosses => "prepared uncross pool",
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
    priority_class: AuctionPriorityClass,
}

impl CallAuctionOrder {
    /// Constructs one already domain-validated auction order request.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "constructor keeps every immutable auction-order dimension explicit"
    )]
    pub const fn new(
        order_id: OrderId,
        account_id: AccountId,
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        side: Side,
        constraint: AuctionOrderConstraint,
        quantity: Quantity,
        priority_class: AuctionPriorityClass,
    ) -> Self {
        Self {
            order_id,
            account_id,
            instrument_id,
            instrument_version,
            side,
            constraint,
            quantity,
            priority_class,
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

    /// Returns the authoritative within-price execution-priority tier.
    #[must_use]
    pub const fn priority_class(self) -> AuctionPriorityClass {
        self.priority_class
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
    /// Authoritative within-price execution-priority tier.
    pub priority_class: AuctionPriorityClass,
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

/// Retained-priority quantity-reduction failure for one active order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionAmendError {
    /// No active order had the supplied identifier.
    UnknownOrder,
    /// The supplied account did not own the active order.
    AccountMismatch,
    /// The proposed leaves quantity violated immutable instrument rules.
    Instrument(AdmissionError),
    /// The proposed quantity was equal to or greater than active leaves.
    QuantityNotReduced,
    /// The mutation revision was exhausted.
    StateRevisionExhausted,
}

impl fmt::Display for CallAuctionAmendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownOrder => formatter.write_str("unknown active call-auction order"),
            Self::AccountMismatch => formatter.write_str("call-auction amendment account mismatch"),
            Self::Instrument(error) => {
                write!(
                    formatter,
                    "call-auction amendment instrument rule: {error:?}"
                )
            }
            Self::QuantityNotReduced => {
                formatter.write_str("call-auction amendment quantity was not reduced")
            }
            Self::StateRevisionExhausted => {
                formatter.write_str("call-auction state revision exhausted")
            }
        }
    }
}

impl std::error::Error for CallAuctionAmendError {}

/// One retained-priority active-quantity reduction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionAmendment {
    previous: CallAuctionOrderSnapshot,
    current: CallAuctionOrderSnapshot,
    state_revision: u64,
}

impl CallAuctionAmendment {
    /// Returns the complete active state before the reduction.
    #[must_use]
    pub const fn previous(self) -> CallAuctionOrderSnapshot {
        self.previous
    }

    /// Returns the complete active state after the reduction.
    #[must_use]
    pub const fn current(self) -> CallAuctionOrderSnapshot {
        self.current
    }

    /// Returns the collection-book revision after the reduction.
    #[must_use]
    pub const fn state_revision(self) -> u64 {
        self.state_revision
    }
}

/// Failure while selecting or applying one account-scoped mass cancel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionMassCancelError {
    /// The caller-owned output must be empty on entry.
    OutputNotEmpty,
    /// The caller-owned output cannot hold the complete atomic selection.
    OutputCapacityExceeded {
        /// Required snapshot slots.
        required: usize,
        /// Currently allocated snapshot slots.
        available: usize,
    },
    /// The mutation revision was exhausted for a non-empty selection.
    StateRevisionExhausted,
    /// A redundant account index contradicted active order state.
    InternalInvariantViolation,
}

impl fmt::Display for CallAuctionMassCancelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutputNotEmpty => {
                formatter.write_str("call-auction mass-cancel output is not empty")
            }
            Self::OutputCapacityExceeded {
                required,
                available,
            } => write!(
                formatter,
                "call-auction mass-cancel output capacity {available} is below required {required}"
            ),
            Self::StateRevisionExhausted => {
                formatter.write_str("call-auction state revision exhausted")
            }
            Self::InternalInvariantViolation => {
                formatter.write_str("call-auction mass-cancel index invariant violation")
            }
        }
    }
}

impl std::error::Error for CallAuctionMassCancelError {}

/// Preflighted aggregate effect of one account-scoped call-auction mass cancel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionMassCancelResult {
    cancelled_order_count: u64,
    cancelled_quantity_lots: u128,
    state_revision: u64,
}

impl CallAuctionMassCancelResult {
    /// Number of selected active orders.
    #[must_use]
    pub const fn cancelled_order_count(self) -> u64 {
        self.cancelled_order_count
    }

    /// Sum of selected active quantity in instrument-defined lots.
    #[must_use]
    pub const fn cancelled_quantity_lots(self) -> u128 {
        self.cancelled_quantity_lots
    }

    /// Collection-book revision after the command is applied.
    #[must_use]
    pub const fn state_revision(self) -> u64 {
        self.state_revision
    }
}

/// Atomic cancel/replace failure for one active call-auction order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionReplaceError {
    /// The active target or its authorizing account failed cancellation checks.
    Target(CallAuctionCancelError),
    /// The replacement order names a different account than the authorizing owner.
    ReplacementAccountMismatch,
    /// The new order failed immutable admission or finite-capacity checks.
    Replacement(CallAuctionAdmissionError),
}

impl fmt::Display for CallAuctionReplaceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Target(error) => write!(formatter, "call-auction replacement target: {error}"),
            Self::ReplacementAccountMismatch => {
                formatter.write_str("call-auction replacement account differs from target owner")
            }
            Self::Replacement(error) => {
                write!(formatter, "call-auction replacement order: {error}")
            }
        }
    }
}

impl std::error::Error for CallAuctionReplaceError {}

/// One atomically removed target and newly admitted replacement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionReplacement {
    cancelled: CallAuctionOrderSnapshot,
    accepted: CallAuctionOrderSnapshot,
}

impl CallAuctionReplacement {
    /// Returns the complete removed target state.
    #[must_use]
    pub const fn cancelled(self) -> CallAuctionOrderSnapshot {
        self.cancelled
    }

    /// Returns the complete newly admitted state with fresh time priority.
    #[must_use]
    pub const fn accepted(self) -> CallAuctionOrderSnapshot {
        self.accepted
    }
}

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
    /// Abort the complete uncross at the first canonical same-account pair.
    Abort,
}

/// Explicit policy applied by one atomic call-auction uncross.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionUncrossPolicy {
    allocation: AuctionAllocationPolicy,
    remainder: CallAuctionRemainderPolicy,
    self_trade: CallAuctionSelfTradePolicy,
}

impl CallAuctionUncrossPolicy {
    /// Creates an explicit uncross policy.
    #[must_use]
    pub const fn new(
        allocation: AuctionAllocationPolicy,
        remainder: CallAuctionRemainderPolicy,
        self_trade: CallAuctionSelfTradePolicy,
    ) -> Self {
        Self {
            allocation,
            remainder,
            self_trade,
        }
    }

    /// Returns the order-level allocation treatment.
    #[must_use]
    pub const fn allocation(self) -> AuctionAllocationPolicy {
        self.allocation
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
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    buy_order_id: OrderId,
    buy_account_id: AccountId,
    sell_order_id: OrderId,
    sell_account_id: AccountId,
    price: Price,
    quantity: Quantity,
}

#[derive(Clone, Copy)]
pub(crate) struct CallAuctionTradeParticipants {
    pub(crate) buy_order: OrderId,
    pub(crate) buy_account: AccountId,
    pub(crate) sell_order: OrderId,
    pub(crate) sell_account: AccountId,
}

impl CallAuctionTrade {
    pub(crate) const fn from_decoded(
        trade_id: TradeId,
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        participants: CallAuctionTradeParticipants,
        price: Price,
        quantity: Quantity,
    ) -> Option<Self> {
        if participants.buy_order.get() == participants.sell_order.get() {
            return None;
        }
        Some(Self {
            trade_id,
            instrument_id,
            instrument_version,
            buy_order_id: participants.buy_order,
            buy_account_id: participants.buy_account,
            sell_order_id: participants.sell_order,
            sell_account_id: participants.sell_account,
            price,
            quantity,
        })
    }

    /// Returns the book-local monotonic trade identifier.
    #[must_use]
    pub const fn trade_id(self) -> TradeId {
        self.trade_id
    }

    /// Returns the immutable instrument identifier settled by this trade.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the immutable instrument-definition version used by the uncross.
    #[must_use]
    pub const fn instrument_version(self) -> InstrumentVersion {
        self.instrument_version
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
    /// Every constructor-owned uncross buffer set is currently leased.
    PreparationCapacityExhausted {
        /// Configured simultaneous lease maximum.
        maximum: usize,
    },
    /// Canonical pairing reached equal buyer and seller account identities.
    SelfTradeWouldOccur {
        /// Account present on both sides of the pair.
        account_id: AccountId,
        /// Canonically selected buy order.
        buy_order_id: OrderId,
        /// Canonically selected sell order.
        sell_order_id: OrderId,
        /// Positive quantity the pair would execute.
        quantity: Quantity,
    },
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
            Self::PreparationCapacityExhausted { maximum } => write!(
                formatter,
                "call-auction prepared-uncross capacity {maximum} is exhausted"
            ),
            Self::SelfTradeWouldOccur {
                account_id,
                buy_order_id,
                sell_order_id,
                quantity,
            } => write!(
                formatter,
                "call-auction canonical pair buy {buy_order_id} and sell {sell_order_id} would self-trade account {account_id} for {} lots",
                quantity.lots()
            ),
            Self::InternalInvariantViolation => {
                formatter.write_str("call-auction private state contradicts prepared uncross")
            }
        }
    }
}

impl std::error::Error for CallAuctionPrepareError {}

#[derive(Debug)]
struct CallAuctionUncrossBuffers {
    plan: AuctionAllocationPlan,
    trades: Vec<CallAuctionTrade>,
    cancellations: Vec<CallAuctionCancellation>,
}

impl CallAuctionUncrossBuffers {
    fn try_new(maximum_orders: usize) -> Result<Self, CallAuctionConstructionError> {
        let buy_fills = try_uncross_vector(maximum_orders, CallAuctionCapacity::UncrossBuyFills)?;
        let sell_fills = try_uncross_vector(maximum_orders, CallAuctionCapacity::UncrossSellFills)?;
        Ok(Self {
            plan: AuctionAllocationPlan::with_fill_storage(buy_fills, sell_fills),
            trades: try_uncross_vector(maximum_orders, CallAuctionCapacity::UncrossTrades)?,
            cancellations: try_uncross_vector(
                maximum_orders,
                CallAuctionCapacity::UncrossCancellations,
            )?,
        })
    }

    fn reset(&mut self) {
        self.plan.clear_reusable();
        self.trades.clear();
        self.cancellations.clear();
    }

    fn plan(&self) -> &AuctionAllocationPlan {
        &self.plan
    }
}

fn try_uncross_vector<T>(
    maximum: usize,
    capacity: CallAuctionCapacity,
) -> Result<Vec<T>, CallAuctionConstructionError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(maximum)
        .map_err(|_| CallAuctionConstructionError::CapacityReservationFailed(capacity))?;
    Ok(values)
}

#[derive(Debug)]
struct CallAuctionUncrossPool {
    available: Mutex<Vec<CallAuctionUncrossBuffers>>,
    capacity: usize,
    fill_capacity_per_side: usize,
    trade_capacity: usize,
    cancellation_capacity: usize,
}

impl CallAuctionUncrossPool {
    fn try_new(
        capacity: usize,
        maximum_orders: usize,
    ) -> Result<Arc<Self>, CallAuctionConstructionError> {
        let mut available = Vec::new();
        available.try_reserve_exact(capacity).map_err(|_| {
            CallAuctionConstructionError::CapacityReservationFailed(
                CallAuctionCapacity::PreparedUncrosses,
            )
        })?;
        for _ in 0..capacity {
            available.push(CallAuctionUncrossBuffers::try_new(maximum_orders)?);
        }
        let fill_capacity_per_side = available
            .iter()
            .map(|buffers| buffers.plan.minimum_fill_capacity())
            .min()
            .unwrap_or(0);
        let trade_capacity = available
            .iter()
            .map(|buffers| buffers.trades.capacity())
            .min()
            .unwrap_or(0);
        let cancellation_capacity = available
            .iter()
            .map(|buffers| buffers.cancellations.capacity())
            .min()
            .unwrap_or(0);
        Ok(Arc::new(Self {
            available: Mutex::new(available),
            capacity,
            fill_capacity_per_side,
            trade_capacity,
            cancellation_capacity,
        }))
    }

    fn lock_available(&self) -> MutexGuard<'_, Vec<CallAuctionUncrossBuffers>> {
        self.available
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn acquire(self: &Arc<Self>) -> Option<PooledCallAuctionUncross> {
        let buffers = self.lock_available().pop()?;
        Some(PooledCallAuctionUncross {
            pool: Arc::clone(self),
            buffers: Some(buffers),
        })
    }

    fn release(&self, mut buffers: CallAuctionUncrossBuffers) {
        buffers.reset();
        let mut available = self.lock_available();
        assert!(
            available.len() < self.capacity,
            "uncross buffer must be returned exactly once"
        );
        available.push(buffers);
    }

    fn available(&self) -> usize {
        self.lock_available().len()
    }
}

struct PooledCallAuctionUncross {
    pool: Arc<CallAuctionUncrossPool>,
    buffers: Option<CallAuctionUncrossBuffers>,
}

impl PooledCallAuctionUncross {
    fn buffers(&self) -> &CallAuctionUncrossBuffers {
        self.buffers
            .as_ref()
            .expect("live uncross lease must contain its buffers")
    }

    fn buffers_mut(&mut self) -> &mut CallAuctionUncrossBuffers {
        self.buffers
            .as_mut()
            .expect("live uncross lease must contain its buffers")
    }
}

impl fmt::Debug for PooledCallAuctionUncross {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let buffers = self.buffers();
        formatter
            .debug_struct("PooledCallAuctionUncross")
            .field("buy_fills", &buffers.plan.buy_fills().len())
            .field("sell_fills", &buffers.plan.sell_fills().len())
            .field("trades", &buffers.trades.len())
            .field("cancellations", &buffers.cancellations.len())
            .finish()
    }
}

impl Drop for PooledCallAuctionUncross {
    fn drop(&mut self) {
        if let Some(buffers) = self.buffers.take() {
            self.pool.release(buffers);
        }
    }
}

/// An immutable, process-local, revision-bound auction uncross preparation.
#[derive(Debug)]
pub struct PreparedCallAuctionUncross {
    book_instance_id: u64,
    state_revision: u64,
    next_state_revision: u64,
    next_trade_id: u64,
    policy: CallAuctionUncrossPolicy,
    pooled: PooledCallAuctionUncross,
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
    pub fn allocation_plan(&self) -> &AuctionAllocationPlan {
        self.pooled.buffers().plan()
    }

    /// Returns proposed counterparty pairs in deterministic priority-walk order.
    #[must_use]
    pub fn trades(&self) -> &[CallAuctionTrade] {
        &self.pooled.buffers().trades
    }

    /// Returns proposed policy-driven remainder cancellations.
    #[must_use]
    pub fn cancellations(&self) -> &[CallAuctionCancellation] {
        &self.pooled.buffers().cancellations
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
#[derive(Debug)]
pub struct CallAuctionUncross {
    state_revision: u64,
    policy: CallAuctionUncrossPolicy,
    pooled: PooledCallAuctionUncross,
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
    pub fn allocation_plan(&self) -> &AuctionAllocationPlan {
        self.pooled.buffers().plan()
    }

    /// Returns deterministic counterparty pairs.
    #[must_use]
    pub fn trades(&self) -> &[CallAuctionTrade] {
        &self.pooled.buffers().trades
    }

    /// Returns remainders canceled by policy.
    #[must_use]
    pub fn cancellations(&self) -> &[CallAuctionCancellation] {
        &self.pooled.buffers().cancellations
    }
}

impl PartialEq for CallAuctionUncross {
    fn eq(&self, other: &Self) -> bool {
        self.state_revision == other.state_revision
            && self.policy == other.policy
            && self.allocation_plan() == other.allocation_plan()
            && self.trades() == other.trades()
            && self.cancellations() == other.cancellations()
    }
}

impl Eq for CallAuctionUncross {}

/// Process-local allocation telemetry for one call-auction collection book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionResourceStatus {
    /// Configured active-order maximum.
    pub configured_active_orders: usize,
    /// Constructor-reserved active-order slots.
    pub allocated_active_orders: usize,
    /// Currently active orders.
    pub active_orders: usize,
    /// Maximum active accounts implied by the active-order bound.
    pub configured_active_accounts: usize,
    /// Constructor-reserved active-account entries.
    pub allocated_active_accounts: usize,
    /// Initialized lookup buckets in the fixed account index.
    pub account_index_buckets: usize,
    /// Accounts currently owning at least one active order.
    pub active_accounts: usize,
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
    /// Configured simultaneous uncross preparation/result leases.
    pub configured_prepared_uncrosses: usize,
    /// Unleased constructor-owned uncross buffer sets.
    pub available_prepared_uncrosses: usize,
    /// Fill capacity per side in each leased buffer set.
    pub uncross_fill_capacity_per_side: usize,
    /// Trade capacity in each leased buffer set.
    pub uncross_trade_capacity: usize,
    /// Cancellation capacity in each leased buffer set.
    pub uncross_cancellation_capacity: usize,
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

/// Caller-owned output constructed by one read-only call-auction book query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionBookQueryResource {
    /// Canonically ordered active-order identifiers for one account.
    AccountOrderIds,
}

impl fmt::Display for CallAuctionBookQueryResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccountOrderIds => formatter.write_str("account-order-identifier output"),
        }
    }
}

/// Failure while constructing caller-owned read-only call-auction book output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallAuctionBookQueryError {
    /// The complete bounded output vector could not be reserved.
    ReservationFailed {
        /// Output whose reservation failed.
        resource: CallAuctionBookQueryResource,
        /// Maximum number of output entries required by the query.
        maximum: usize,
    },
    /// Private account or active-order state contradicted its invariants.
    InvariantViolation(CallAuctionInvariantViolation),
}

impl fmt::Display for CallAuctionBookQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservationFailed { resource, maximum } => write!(
                formatter,
                "call-auction book {resource} could not reserve {maximum} entries"
            ),
            Self::InvariantViolation(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CallAuctionBookQueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ReservationFailed { .. } => None,
            Self::InvariantViolation(error) => Some(error),
        }
    }
}

impl From<CallAuctionInvariantViolation> for CallAuctionBookQueryError {
    fn from(error: CallAuctionInvariantViolation) -> Self {
        Self::InvariantViolation(error)
    }
}

fn reserve_call_auction_book_query_vec<T>(
    maximum: usize,
    resource: CallAuctionBookQueryResource,
) -> Result<Vec<T>, CallAuctionBookQueryError> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(maximum)
        .map_err(|_| CallAuctionBookQueryError::ReservationFailed { resource, maximum })?;
    Ok(output)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AuctionQueue {
    head: Option<OrderId>,
    tail: Option<OrderId>,
    total_quantity: u128,
    order_count: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AuctionAccountSideIndex {
    head: Option<OrderId>,
    tail: Option<OrderId>,
    order_count: usize,
    total_quantity: u128,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AuctionAccountIndex {
    buys: AuctionAccountSideIndex,
    sells: AuctionAccountSideIndex,
}

impl AuctionAccountIndex {
    fn side(self, side: Side) -> AuctionAccountSideIndex {
        match side {
            Side::Buy => self.buys,
            Side::Sell => self.sells,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut AuctionAccountSideIndex {
        match side {
            Side::Buy => &mut self.buys,
            Side::Sell => &mut self.sells,
        }
    }

    fn selected(self, scope: MassCancelScope) -> Option<(usize, u128)> {
        match scope {
            MassCancelScope::All => Some((
                self.buys.order_count.checked_add(self.sells.order_count)?,
                self.buys
                    .total_quantity
                    .checked_add(self.sells.total_quantity)?,
            )),
            MassCancelScope::Side(side) => {
                let selected = self.side(side);
                Some((selected.order_count, selected.total_quantity))
            }
        }
    }

    fn is_empty(self) -> bool {
        self.buys.order_count == 0 && self.sells.order_count == 0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RestingAuctionOrder {
    order_id: OrderId,
    account_id: AccountId,
    side: Side,
    constraint: AuctionOrderConstraint,
    quantity: Quantity,
    priority_class: AuctionPriorityClass,
    priority_sequence: u64,
    previous: Option<OrderId>,
    next: Option<OrderId>,
    account_previous: Option<OrderId>,
    account_next: Option<OrderId>,
}

impl From<RestingAuctionOrder> for CallAuctionOrderSnapshot {
    fn from(order: RestingAuctionOrder) -> Self {
        Self {
            order_id: order.order_id,
            account_id: order.account_id,
            side: order.side,
            constraint: order.constraint,
            quantity: order.quantity,
            priority_class: order.priority_class,
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

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&Price, &AuctionQueue)> + ExactSizeIterator {
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
    account_orders: BoundedHashMap<AccountId, AuctionAccountIndex>,
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
    uncross_pool: Arc<CallAuctionUncrossPool>,
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
        let account_orders = BoundedHashMap::try_new(limits.max_active_orders()).map_err(|_| {
            CallAuctionConstructionError::CapacityReservationFailed(
                CallAuctionCapacity::ActiveAccounts,
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
        let uncross_pool = CallAuctionUncrossPool::try_new(
            limits.max_prepared_uncrosses(),
            limits.max_active_orders(),
        )?;
        let instance_id = next_call_auction_book_instance_id()?;
        Ok(Self {
            instance_id,
            definition,
            limits,
            grid,
            instrument_band,
            orders,
            account_orders,
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
            uncross_pool,
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

    /// Iterates active order state in ascending identifier order without allocating.
    pub(crate) fn active_order_states(
        &self,
    ) -> impl ExactSizeIterator<Item = CallAuctionOrderSnapshot> + '_ {
        self.orders.iter().map(|(_, order)| (*order).into())
    }

    /// Iterates every accepted identifier in ascending order without allocating.
    pub(crate) fn accepted_order_ids(&self) -> impl ExactSizeIterator<Item = OrderId> + '_ {
        self.seen_order_ids.iter().map(|(order_id, ())| *order_id)
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
            let mut resting = RestingAuctionOrder {
                order_id: snapshot.order_id,
                account_id: snapshot.account_id,
                side: snapshot.side,
                constraint: snapshot.constraint,
                quantity: snapshot.quantity,
                priority_class: snapshot.priority_class,
                priority_sequence: snapshot.priority_sequence,
                previous: queue.tail,
                next: None,
                account_previous: None,
                account_next: None,
            };
            book.link_account_membership(&mut resting);
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
        match side {
            Side::Buy => self.limit_depth_levels(side).rev().take(limit).collect(),
            Side::Sell => self.limit_depth_levels(side).take(limit).collect(),
        }
    }

    /// Iterates aggregate limit depth in ascending price order without allocating.
    pub(crate) fn limit_depth_levels(
        &self,
        side: Side,
    ) -> impl DoubleEndedIterator<Item = CallAuctionLevelSnapshot> + ExactSizeIterator + '_ {
        self.levels(side)
            .iter()
            .map(move |(&price, queue)| CallAuctionLevelSnapshot {
                side,
                price,
                quantity: queue.total_quantity,
                order_count: queue.order_count,
            })
    }

    /// Returns immutable state for one active order.
    #[must_use]
    pub fn order(&self, order_id: OrderId) -> Option<CallAuctionOrderSnapshot> {
        self.orders.get(&order_id).copied().map(Into::into)
    }

    /// Returns one account's active-order count for all orders or one side.
    ///
    /// This performs one expected `O(1)` account lookup and no book scan.
    #[must_use]
    pub fn account_active_order_count(
        &self,
        account_id: AccountId,
        scope: MassCancelScope,
    ) -> usize {
        let Some(index) = self.account_orders.get(&account_id).copied() else {
            return 0;
        };
        match scope {
            MassCancelScope::All => index
                .buys
                .order_count
                .saturating_add(index.sells.order_count),
            MassCancelScope::Side(side) => index.side(side).order_count,
        }
    }

    /// Fallibly returns one account's active order IDs in canonical ascending order.
    ///
    /// This allocates `O(K)` output, traverses only the `K` selected orders, and
    /// sorts them in `O(K log K)` time without auxiliary allocation.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionBookQueryError::ReservationFailed`] before
    /// traversal if the complete bounded vector cannot be reserved, or
    /// [`CallAuctionBookQueryError::InvariantViolation`] if the private account
    /// list contradicts its declared identity, side, quantity, or cardinality.
    pub fn try_account_active_order_ids(
        &self,
        account_id: AccountId,
        scope: MassCancelScope,
    ) -> Result<Vec<OrderId>, CallAuctionBookQueryError> {
        let Some(index) = self.account_orders.get(&account_id).copied() else {
            return Ok(Vec::new());
        };
        let selected_count = match scope {
            MassCancelScope::All => index
                .buys
                .order_count
                .checked_add(index.sells.order_count)
                .ok_or_else(|| {
                    CallAuctionInvariantViolation::new(format!(
                        "account {account_id} active-order count is exhausted"
                    ))
                })?,
            MassCancelScope::Side(side) => index.side(side).order_count,
        };
        if selected_count > self.orders.len() {
            return Err(CallAuctionInvariantViolation::new(format!(
                "account {account_id} active-order count exceeds total active orders"
            ))
            .into());
        }
        let mut selected = reserve_call_auction_book_query_vec(
            selected_count,
            CallAuctionBookQueryResource::AccountOrderIds,
        )?;
        match scope {
            MassCancelScope::All => {
                self.try_append_account_side_values(
                    index,
                    account_id,
                    Side::Buy,
                    &mut selected,
                    |order| order.order_id,
                )?;
                self.try_append_account_side_values(
                    index,
                    account_id,
                    Side::Sell,
                    &mut selected,
                    |order| order.order_id,
                )?;
            }
            MassCancelScope::Side(side) => self.try_append_account_side_values(
                index,
                account_id,
                side,
                &mut selected,
                |order| order.order_id,
            )?,
        }
        if selected.len() != selected_count {
            return Err(CallAuctionInvariantViolation::new(format!(
                "account {account_id} query output differs from its declared count"
            ))
            .into());
        }
        selected.sort_unstable();
        if selected.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(CallAuctionInvariantViolation::new(format!(
                "account {account_id} query traversed one active order more than once"
            ))
            .into());
        }
        Ok(selected)
    }

    /// Returns account order IDs using the fallible production path.
    ///
    /// # Panics
    ///
    /// Panics if output allocation fails or private account-list state is
    /// corrupt. Use [`Self::try_account_active_order_ids`] when either failure
    /// must remain typed.
    #[must_use]
    pub fn account_active_order_ids(
        &self,
        account_id: AccountId,
        scope: MassCancelScope,
    ) -> Vec<OrderId> {
        self.try_account_active_order_ids(account_id, scope)
            .expect("call-auction book account-order query failed")
    }

    /// Preflights the aggregate effect of one account-scoped mass cancel.
    ///
    /// The operation performs one expected `O(1)` account lookup. An empty
    /// selection is valid and preserves the current collection revision.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMassCancelError`] if a non-empty selection cannot
    /// advance the revision or redundant aggregate state is unrepresentable.
    pub fn preflight_mass_cancel(
        &self,
        account_id: AccountId,
        scope: MassCancelScope,
    ) -> Result<CallAuctionMassCancelResult, CallAuctionMassCancelError> {
        let (selected_count, cancelled_quantity_lots) = self
            .account_orders
            .get(&account_id)
            .copied()
            .map_or(Some((0, 0)), |index| index.selected(scope))
            .ok_or(CallAuctionMassCancelError::InternalInvariantViolation)?;
        let cancelled_order_count = u64::try_from(selected_count)
            .map_err(|_| CallAuctionMassCancelError::InternalInvariantViolation)?;
        let state_revision = if selected_count == 0 {
            self.state_revision
        } else {
            self.state_revision
                .checked_add(1)
                .ok_or(CallAuctionMassCancelError::StateRevisionExhausted)?
        };
        Ok(CallAuctionMassCancelResult {
            cancelled_order_count,
            cancelled_quantity_lots,
            state_revision,
        })
    }

    /// Atomically removes one account's selected active orders.
    ///
    /// `output` must be empty and retain capacity for the complete selection.
    /// Selected snapshots are returned in ascending [`OrderId`] order. The
    /// account index bounds selection work to `K` selected orders; sorting is
    /// `O(K log K)` and each removal is `O(log O)` for `O` active orders. No
    /// storage allocation occurs after construction when caller capacity is
    /// sufficient. A non-empty selection advances the book revision exactly
    /// once; an empty selection leaves it unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionMassCancelError`] before book mutation for invalid
    /// output state, insufficient output capacity, revision exhaustion, or an
    /// account-index contradiction.
    pub fn mass_cancel_into(
        &mut self,
        account_id: AccountId,
        scope: MassCancelScope,
        output: &mut Vec<CallAuctionOrderSnapshot>,
    ) -> Result<CallAuctionMassCancelResult, CallAuctionMassCancelError> {
        if !output.is_empty() {
            return Err(CallAuctionMassCancelError::OutputNotEmpty);
        }
        let result = self.preflight_mass_cancel(account_id, scope)?;
        let selected_count = usize::try_from(result.cancelled_order_count)
            .map_err(|_| CallAuctionMassCancelError::InternalInvariantViolation)?;
        if output.capacity() < selected_count {
            return Err(CallAuctionMassCancelError::OutputCapacityExceeded {
                required: selected_count,
                available: output.capacity(),
            });
        }
        if selected_count == 0 {
            return Ok(result);
        }
        let Some(index) = self.account_orders.get(&account_id).copied() else {
            return Err(CallAuctionMassCancelError::InternalInvariantViolation);
        };
        let append_result = match scope {
            MassCancelScope::All => self
                .try_append_account_side_values(
                    index,
                    account_id,
                    Side::Buy,
                    output,
                    CallAuctionOrderSnapshot::from,
                )
                .and_then(|()| {
                    self.try_append_account_side_values(
                        index,
                        account_id,
                        Side::Sell,
                        output,
                        CallAuctionOrderSnapshot::from,
                    )
                }),
            MassCancelScope::Side(side) => self.try_append_account_side_values(
                index,
                account_id,
                side,
                output,
                CallAuctionOrderSnapshot::from,
            ),
        };
        if append_result.is_err() || output.len() != selected_count {
            output.clear();
            return Err(CallAuctionMassCancelError::InternalInvariantViolation);
        }
        output.sort_unstable_by_key(|order| order.order_id);
        let invalid_selection = output
            .windows(2)
            .any(|pair| pair[0].order_id == pair[1].order_id)
            || output.iter().copied().any(|snapshot| {
                self.orders
                    .get(&snapshot.order_id)
                    .copied()
                    .map(CallAuctionOrderSnapshot::from)
                    != Some(snapshot)
            });
        if invalid_selection {
            output.clear();
            return Err(CallAuctionMassCancelError::InternalInvariantViolation);
        }
        for snapshot in output.iter().copied() {
            let Some(order) = self.orders.get(&snapshot.order_id).copied() else {
                return Err(CallAuctionMassCancelError::InternalInvariantViolation);
            };
            self.remove_active_order(order);
        }
        self.state_revision = result.state_revision;
        Ok(result)
    }

    /// Returns constructor allocation and current occupancy telemetry.
    #[must_use]
    pub fn resource_status(&self) -> CallAuctionResourceStatus {
        CallAuctionResourceStatus {
            configured_active_orders: self.limits.max_active_orders(),
            allocated_active_orders: self.orders.allocation_capacity(),
            active_orders: self.orders.len(),
            configured_active_accounts: self.limits.max_active_orders(),
            allocated_active_accounts: self.account_orders.capacity(),
            account_index_buckets: self.account_orders.bucket_count(),
            active_accounts: self.account_orders.len(),
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
            configured_prepared_uncrosses: self.limits.max_prepared_uncrosses(),
            available_prepared_uncrosses: self.uncross_pool.available(),
            uncross_fill_capacity_per_side: self.uncross_pool.fill_capacity_per_side,
            uncross_trade_capacity: self.uncross_pool.trade_capacity,
            uncross_cancellation_capacity: self.uncross_pool.cancellation_capacity,
        }
    }

    /// Admits one market or limit order at authoritative class/time priority.
    ///
    /// Market orders precede limit orders, limit prices are ordered by
    /// aggressiveness, lower priority classes precede higher classes within an
    /// equal constraint, and admission sequence orders interest within one
    /// class. Class meaning is supplied by the versioned ingress/controller.
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
        Ok(self.insert_preflighted(command, next_priority_sequence, next_state_revision))
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
        let account_side = self
            .account_orders
            .get(&command.account_id)
            .copied()
            .unwrap_or_default()
            .side(command.side);
        account_side
            .total_quantity
            .checked_add(u128::from(command.quantity.lots()))
            .ok_or(CallAuctionAdmissionError::AggregateQuantityOverflow(
                command.side,
            ))?;
        account_side.order_count.checked_add(1).ok_or(
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

    /// Reduces one owned active order's quantity without changing priority.
    ///
    /// Identity, account, side, constraint, queue links, and priority sequence
    /// remain unchanged. The queue and account aggregates decrease by the exact
    /// quantity delta. The operation is `O(log O + log P)` for `O` active
    /// orders and `P` occupied prices, uses `O(1)` auxiliary space, and does not
    /// allocate.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionAmendError`] before mutation when the order is
    /// absent, ownership differs, the proposed leaves violate instrument rules,
    /// quantity is not strictly reduced, or the book revision is exhausted.
    ///
    /// # Panics
    ///
    /// Panics only if internal queue, account, or active-order indexes already
    /// contradict a target that passed preflight. Safe public operations
    /// preserve those invariants.
    pub fn amend(
        &mut self,
        account_id: AccountId,
        order_id: OrderId,
        new_quantity: Quantity,
    ) -> Result<CallAuctionAmendment, CallAuctionAmendError> {
        let previous = self.preflight_amend(account_id, order_id, new_quantity)?;
        let state_revision = self
            .state_revision
            .checked_add(1)
            .ok_or(CallAuctionAmendError::StateRevisionExhausted)?;
        let delta_lots = previous
            .quantity
            .lots()
            .checked_sub(new_quantity.lots())
            .expect("preflighted amendment must strictly reduce quantity");

        let mut queue = self.queue_snapshot(previous.side, previous.constraint);
        queue.total_quantity = queue
            .total_quantity
            .checked_sub(u128::from(delta_lots))
            .expect("active order quantity must be represented in its queue");
        self.replace_queue(previous.side, previous.constraint, queue);

        let account_side = self
            .account_orders
            .get_mut(&previous.account_id)
            .expect("active order account index must exist")
            .side_mut(previous.side);
        account_side.total_quantity = account_side
            .total_quantity
            .checked_sub(u128::from(delta_lots))
            .expect("active order quantity must be represented in its account index");

        let active = self
            .orders
            .get_mut(&order_id)
            .expect("preflighted amendment target must remain active");
        active.quantity = new_quantity;
        let current = (*active).into();
        self.state_revision = state_revision;
        Ok(CallAuctionAmendment {
            previous,
            current,
            state_revision,
        })
    }

    pub(crate) fn preflight_amend(
        &self,
        account_id: AccountId,
        order_id: OrderId,
        new_quantity: Quantity,
    ) -> Result<CallAuctionOrderSnapshot, CallAuctionAmendError> {
        let order = self
            .orders
            .get(&order_id)
            .copied()
            .ok_or(CallAuctionAmendError::UnknownOrder)?;
        if order.account_id != account_id {
            return Err(CallAuctionAmendError::AccountMismatch);
        }
        self.definition
            .quantity_rules()
            .validate_leaves(new_quantity)
            .map_err(CallAuctionAmendError::Instrument)?;
        if new_quantity.lots() >= order.quantity.lots() {
            return Err(CallAuctionAmendError::QuantityNotReduced);
        }
        self.state_revision
            .checked_add(1)
            .ok_or(CallAuctionAmendError::StateRevisionExhausted)?;
        Ok(order.into())
    }

    /// Atomically cancels one owned target and admits a new-identity replacement.
    ///
    /// The replacement always receives a fresh strict priority sequence. The
    /// target identity remains consumed, and the replacement identity becomes
    /// permanently consumed. Active-order and price-level slots released by
    /// the target may be reused by the replacement without storage growth.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionReplaceError`] before mutation for target ownership,
    /// replacement identity/domain, capacity, sequence, revision, or aggregate
    /// failure.
    pub fn replace(
        &mut self,
        account_id: AccountId,
        target_order_id: OrderId,
        replacement: CallAuctionOrder,
    ) -> Result<CallAuctionReplacement, CallAuctionReplaceError> {
        let cancelled = self.preflight_replace(account_id, target_order_id, replacement)?;
        let target =
            self.orders
                .get(&target_order_id)
                .copied()
                .ok_or(CallAuctionReplaceError::Target(
                    CallAuctionCancelError::UnknownOrder,
                ))?;
        let next_priority_sequence = self.next_priority_sequence.checked_add(1).ok_or(
            CallAuctionReplaceError::Replacement(
                CallAuctionAdmissionError::PrioritySequenceExhausted,
            ),
        )?;
        let next_state_revision =
            self.state_revision
                .checked_add(1)
                .ok_or(CallAuctionReplaceError::Replacement(
                    CallAuctionAdmissionError::StateRevisionExhausted,
                ))?;
        self.remove_active_order(target);
        let accepted =
            self.insert_preflighted(replacement, next_priority_sequence, next_state_revision);
        Ok(CallAuctionReplacement {
            cancelled,
            accepted,
        })
    }

    pub(crate) fn preflight_replace(
        &self,
        account_id: AccountId,
        target_order_id: OrderId,
        replacement: CallAuctionOrder,
    ) -> Result<CallAuctionOrderSnapshot, CallAuctionReplaceError> {
        let target =
            self.orders
                .get(&target_order_id)
                .copied()
                .ok_or(CallAuctionReplaceError::Target(
                    CallAuctionCancelError::UnknownOrder,
                ))?;
        if target.account_id != account_id {
            return Err(CallAuctionReplaceError::Target(
                CallAuctionCancelError::AccountMismatch,
            ));
        }
        if replacement.account_id != account_id {
            return Err(CallAuctionReplaceError::ReplacementAccountMismatch);
        }
        self.validate_command(replacement)
            .map_err(CallAuctionReplaceError::Replacement)?;
        if self.seen_order_ids.get(&replacement.order_id).is_some() {
            return Err(CallAuctionReplaceError::Replacement(
                CallAuctionAdmissionError::DuplicateOrderId,
            ));
        }
        if self.seen_order_ids.len() >= self.limits.max_accepted_order_ids() {
            return Err(CallAuctionReplaceError::Replacement(
                CallAuctionAdmissionError::CapacityExceeded(CallAuctionCapacity::AcceptedOrderIds),
            ));
        }
        self.preflight_replacement_level(target, replacement)?;
        self.next_priority_sequence
            .checked_add(1)
            .ok_or(CallAuctionReplaceError::Replacement(
                CallAuctionAdmissionError::PrioritySequenceExhausted,
            ))?;
        self.state_revision
            .checked_add(1)
            .ok_or(CallAuctionReplaceError::Replacement(
                CallAuctionAdmissionError::StateRevisionExhausted,
            ))?;
        let mut queue = self.queue_snapshot(replacement.side, replacement.constraint);
        if target.side == replacement.side && target.constraint == replacement.constraint {
            queue.total_quantity = queue
                .total_quantity
                .checked_sub(u128::from(target.quantity.lots()))
                .ok_or(CallAuctionReplaceError::Replacement(
                    CallAuctionAdmissionError::AggregateQuantityOverflow(replacement.side),
                ))?;
            queue.order_count =
                queue
                    .order_count
                    .checked_sub(1)
                    .ok_or(CallAuctionReplaceError::Replacement(
                        CallAuctionAdmissionError::AggregateQuantityOverflow(replacement.side),
                    ))?;
        }
        queue
            .total_quantity
            .checked_add(u128::from(replacement.quantity.lots()))
            .ok_or(CallAuctionReplaceError::Replacement(
                CallAuctionAdmissionError::AggregateQuantityOverflow(replacement.side),
            ))?;
        queue
            .order_count
            .checked_add(1)
            .ok_or(CallAuctionReplaceError::Replacement(
                CallAuctionAdmissionError::AggregateQuantityOverflow(replacement.side),
            ))?;
        let mut account_side = self
            .account_orders
            .get(&account_id)
            .copied()
            .ok_or(CallAuctionReplaceError::Target(
                CallAuctionCancelError::UnknownOrder,
            ))?
            .side(replacement.side);
        if target.side == replacement.side {
            account_side.total_quantity = account_side
                .total_quantity
                .checked_sub(u128::from(target.quantity.lots()))
                .ok_or(CallAuctionReplaceError::Replacement(
                    CallAuctionAdmissionError::AggregateQuantityOverflow(replacement.side),
                ))?;
            account_side.order_count = account_side.order_count.checked_sub(1).ok_or(
                CallAuctionReplaceError::Replacement(
                    CallAuctionAdmissionError::AggregateQuantityOverflow(replacement.side),
                ),
            )?;
        }
        account_side
            .total_quantity
            .checked_add(u128::from(replacement.quantity.lots()))
            .ok_or(CallAuctionReplaceError::Replacement(
                CallAuctionAdmissionError::AggregateQuantityOverflow(replacement.side),
            ))?;
        account_side
            .order_count
            .checked_add(1)
            .ok_or(CallAuctionReplaceError::Replacement(
                CallAuctionAdmissionError::AggregateQuantityOverflow(replacement.side),
            ))?;
        Ok(target.into())
    }

    fn preflight_replacement_level(
        &self,
        target: RestingAuctionOrder,
        replacement: CallAuctionOrder,
    ) -> Result<(), CallAuctionReplaceError> {
        let AuctionOrderConstraint::Limit(price) = replacement.constraint else {
            return Ok(());
        };
        let levels = self.levels(replacement.side);
        let target_level = match target.constraint {
            AuctionOrderConstraint::Limit(target_price) if target.side == replacement.side => {
                Some((
                    target_price,
                    self.queue_snapshot(target.side, target.constraint),
                ))
            }
            AuctionOrderConstraint::Market | AuctionOrderConstraint::Limit(_) => None,
        };
        let target_removes_level = target_level.is_some_and(|(_, queue)| queue.order_count == 1);
        let target_is_requested_level =
            target_level.is_some_and(|(target_price, _)| target_price == price);
        let requested_exists_after_removal =
            levels.get(price).is_some() && !(target_is_requested_level && target_removes_level);
        let levels_after_removal = levels
            .len()
            .saturating_sub(usize::from(target_removes_level));
        if !requested_exists_after_removal
            && levels_after_removal >= self.limits.max_price_levels_per_side()
        {
            return Err(CallAuctionReplaceError::Replacement(
                CallAuctionAdmissionError::CapacityExceeded(match replacement.side {
                    Side::Buy => CallAuctionCapacity::BidPriceLevels,
                    Side::Sell => CallAuctionCapacity::AskPriceLevels,
                }),
            ));
        }
        Ok(())
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

    /// Constructs an immutable allocation plan for current interest.
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
        policy: AuctionAllocationPolicy,
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
        allocate_clearing_with_policy(
            &self.buy_order_scratch,
            &self.sell_order_scratch,
            self.grid,
            indicative.clearing,
            policy,
            self.definition.quantity_rules().increment_lots(),
            limits,
        )
        .map_err(CallAuctionPlanError::Allocation)
    }

    /// Fully prepares deterministic pairing and remainder treatment without
    /// mutating active orders, trade identity, or collection revision.
    ///
    /// The supplied policy explicitly selects allocation and either permits or
    /// aborts at the first canonical self-trade pair; neither behavior is
    /// inferred and abort does not search for an alternative pairing. One
    /// constructor-owned lease supplies both
    /// side-fill vectors, the counterparty-pair vector, and the cancellation
    /// vector. Dropping the preparation or committed result returns that isolated
    /// storage to the pool.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionPrepareError`] for foreign/stale indicative state,
    /// allocation mismatch, identifier/revision exhaustion, exhaustion of all
    /// prepared-uncross leases, or a private invariant contradiction.
    pub fn prepare_uncross(
        &mut self,
        indicative: CallAuctionIndicative,
        policy: CallAuctionUncrossPolicy,
    ) -> Result<PreparedCallAuctionUncross, CallAuctionPrepareError> {
        let next_state_revision = self
            .state_revision
            .checked_add(1)
            .ok_or(CallAuctionPrepareError::StateRevisionExhausted)?;
        if indicative.book_instance_id != self.instance_id {
            return Err(CallAuctionPrepareError::Plan(
                CallAuctionPlanError::ForeignBook,
            ));
        }
        if indicative.state_revision != self.state_revision {
            return Err(CallAuctionPrepareError::Plan(
                CallAuctionPlanError::StaleRevision {
                    observed: indicative.state_revision,
                    current: self.state_revision,
                },
            ));
        }
        let mut pooled = self.uncross_pool.acquire().ok_or(
            CallAuctionPrepareError::PreparationCapacityExhausted {
                maximum: self.limits.max_prepared_uncrosses(),
            },
        )?;
        self.rebuild_order_scratch();
        let limits = AuctionAllocationLimits::new(self.limits.max_active_orders())
            .map_err(CallAuctionPlanError::Allocation)
            .map_err(CallAuctionPrepareError::Plan)?;
        let buffers = pooled.buffers_mut();
        allocate_clearing_reusing(
            &self.buy_order_scratch,
            &self.sell_order_scratch,
            self.grid,
            indicative.clearing,
            AuctionAllocationMethod::new(
                policy.allocation,
                self.definition.quantity_rules().increment_lots(),
            ),
            limits,
            &mut buffers.plan,
        )
        .map_err(CallAuctionPlanError::Allocation)
        .map_err(CallAuctionPrepareError::Plan)?;
        let CallAuctionUncrossBuffers {
            plan,
            trades,
            cancellations,
            ..
        } = buffers;
        let next_trade_id = self.prepare_trade_pairs(plan, policy.self_trade, trades)?;
        self.prepare_remainder_cancellations(plan, policy.remainder, cancellations)?;
        Ok(PreparedCallAuctionUncross {
            book_instance_id: self.instance_id,
            state_revision: self.state_revision,
            next_state_revision,
            next_trade_id,
            policy,
            pooled,
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
        for fill in prepared.allocation_plan().buy_fills() {
            self.apply_fill(*fill);
        }
        for fill in prepared.allocation_plan().sell_fills() {
            self.apply_fill(*fill);
        }
        for cancellation in prepared.cancellations() {
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
            pooled: prepared.pooled,
        })
    }

    /// Audits every redundant index, intrusive queue, aggregate, priority
    /// relation, and finite bound.
    ///
    /// Successful validation uses `O(1)` auxiliary space and performs no heap
    /// allocation. Queue traversal is `O(R log R)` for `R` active orders because
    /// intrusive links resolve through the active-order AVL; accepted-identity
    /// membership adds `O(R log I)` for `I` accepted identifiers. The four
    /// allocation-free indexed-AVL audits take `O(S log S)` in aggregate for
    /// `S` initialized active-order, accepted-identifier, and price-arena slots.
    /// Failure-detail construction can allocate, so this remains an offline
    /// audit rather than a collection-hot-path operation.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionInvariantViolation`] at the first contradiction.
    pub fn validate(&self) -> Result<(), CallAuctionInvariantViolation> {
        self.validate_storage()?;
        let mut indexed_orders = 0_usize;
        self.validate_queue(
            self.market_buys,
            Side::Buy,
            AuctionOrderConstraint::Market,
            &mut indexed_orders,
        )?;
        self.validate_queue(
            self.market_sells,
            Side::Sell,
            AuctionOrderConstraint::Market,
            &mut indexed_orders,
        )?;
        for (price, queue) in self.bids.iter() {
            self.validate_queue(
                *queue,
                Side::Buy,
                AuctionOrderConstraint::Limit(*price),
                &mut indexed_orders,
            )?;
        }
        for (price, queue) in self.asks.iter() {
            self.validate_queue(
                *queue,
                Side::Sell,
                AuctionOrderConstraint::Limit(*price),
                &mut indexed_orders,
            )?;
        }
        if indexed_orders != self.orders.len() {
            return Err(CallAuctionInvariantViolation::new(
                "active order index contains an unqueued order",
            ));
        }
        self.validate_account_order_index()?;
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
        self.account_orders.validate_layout().map_err(|detail| {
            CallAuctionInvariantViolation::new(format!(
                "call-auction active-account hash: {detail}"
            ))
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
            || status.allocated_active_accounts < status.configured_active_accounts
            || status.account_index_buckets == 0
            || status.allocated_bid_price_levels < self.limits.max_price_levels_per_side()
            || status.allocated_ask_price_levels < self.limits.max_price_levels_per_side()
            || status.bid_level_scratch < self.limits.max_price_levels_per_side()
            || status.ask_level_scratch < self.limits.max_price_levels_per_side()
            || status.buy_order_scratch < self.limits.max_active_orders()
            || status.sell_order_scratch < self.limits.max_active_orders()
            || status.configured_prepared_uncrosses != self.limits.max_prepared_uncrosses()
            || status.available_prepared_uncrosses > status.configured_prepared_uncrosses
            || status.uncross_fill_capacity_per_side < self.limits.max_active_orders()
            || status.uncross_trade_capacity < self.limits.max_active_orders()
            || status.uncross_cancellation_capacity < self.limits.max_active_orders()
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
            if !self.account_orders.contains_key(&order.account_id) {
                return Err(CallAuctionInvariantViolation::new(
                    "active order owner is absent from the account index",
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

    fn validate_account_order_index(&self) -> Result<(), CallAuctionInvariantViolation> {
        let mut indexed_orders = 0_usize;
        for (&account_id, index) in self.account_orders.iter() {
            if index.is_empty() {
                return Err(CallAuctionInvariantViolation::new(
                    "empty account remains in the active-order account index",
                ));
            }
            for side in [Side::Buy, Side::Sell] {
                let side_index = index.side(side);
                if side_index.order_count == 0 {
                    if side_index.head.is_some()
                        || side_index.tail.is_some()
                        || side_index.total_quantity != 0
                    {
                        return Err(CallAuctionInvariantViolation::new(
                            "empty account side has non-empty redundant state",
                        ));
                    }
                    continue;
                }
                if side_index.head.is_none()
                    || side_index.tail.is_none()
                    || side_index.total_quantity == 0
                {
                    return Err(CallAuctionInvariantViolation::new(
                        "non-empty account side lacks a head, tail, or quantity aggregate",
                    ));
                }
                let mut current = side_index.head;
                let mut previous = None;
                let mut count = 0_usize;
                let mut total_quantity = 0_u128;
                while let Some(order_id) = current {
                    if count >= side_index.order_count || indexed_orders >= self.orders.len() {
                        return Err(CallAuctionInvariantViolation::new(
                            "active order occurs more than once or participates in an account-index cycle",
                        ));
                    }
                    let order = self.orders.get(&order_id).copied().ok_or_else(|| {
                        CallAuctionInvariantViolation::new(
                            "account index references an absent active order",
                        )
                    })?;
                    if order.account_id != account_id
                        || order.side != side
                        || order.account_previous != previous
                    {
                        return Err(CallAuctionInvariantViolation::new(
                            "account-index membership or previous link is inconsistent",
                        ));
                    }
                    total_quantity = total_quantity
                        .checked_add(u128::from(order.quantity.lots()))
                        .ok_or_else(|| {
                            CallAuctionInvariantViolation::new(
                                "account-index quantity aggregate overflows",
                            )
                        })?;
                    count = count.checked_add(1).ok_or_else(|| {
                        CallAuctionInvariantViolation::new("account-index count overflows")
                    })?;
                    indexed_orders = indexed_orders.checked_add(1).ok_or_else(|| {
                        CallAuctionInvariantViolation::new("account-index total count overflows")
                    })?;
                    previous = Some(order_id);
                    current = order.account_next;
                }
                if previous != side_index.tail
                    || count != side_index.order_count
                    || total_quantity != side_index.total_quantity
                {
                    return Err(CallAuctionInvariantViolation::new(
                        "account-index tail, count, or quantity aggregate is inconsistent",
                    ));
                }
            }
        }
        if indexed_orders != self.orders.len() {
            return Err(CallAuctionInvariantViolation::new(
                "active order index contains an order absent from account indexes",
            ));
        }
        Ok(())
    }

    fn prepare_trade_pairs(
        &self,
        plan: &AuctionAllocationPlan,
        self_trade: CallAuctionSelfTradePolicy,
        trades: &mut Vec<CallAuctionTrade>,
    ) -> Result<u64, CallAuctionPrepareError> {
        let pair_count = required_pair_count(plan.buy_fills(), plan.sell_fills())
            .ok_or(CallAuctionPrepareError::InternalInvariantViolation)?;
        let pair_count_u64 = u64::try_from(pair_count)
            .map_err(|_| CallAuctionPrepareError::TradeIdentifierExhausted)?;
        let next_trade_id = self
            .next_trade_id
            .checked_add(pair_count_u64)
            .ok_or(CallAuctionPrepareError::TradeIdentifierExhausted)?;
        if trades.capacity() < pair_count {
            return Err(CallAuctionPrepareError::InternalInvariantViolation);
        }
        trades.clear();
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
            let quantity = Quantity::new(quantity_lots)
                .map_err(|_| CallAuctionPrepareError::InternalInvariantViolation)?;
            if self_trade == CallAuctionSelfTradePolicy::Abort
                && buy_order.account_id == sell_order.account_id
            {
                return Err(CallAuctionPrepareError::SelfTradeWouldOccur {
                    account_id: buy_order.account_id,
                    buy_order_id: buy_fill.order_id(),
                    sell_order_id: sell_fill.order_id(),
                    quantity,
                });
            }
            let offset = u64::try_from(trades.len())
                .map_err(|_| CallAuctionPrepareError::TradeIdentifierExhausted)?;
            let raw_trade_id = self
                .next_trade_id
                .checked_add(offset)
                .ok_or(CallAuctionPrepareError::TradeIdentifierExhausted)?;
            let trade_id = TradeId::new(raw_trade_id)
                .map_err(|_| CallAuctionPrepareError::InternalInvariantViolation)?;
            trades.push(CallAuctionTrade {
                trade_id,
                instrument_id: self.definition.instrument_id(),
                instrument_version: self.definition.version(),
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
        Ok(next_trade_id)
    }

    fn prepare_remainder_cancellations(
        &self,
        plan: &AuctionAllocationPlan,
        policy: CallAuctionRemainderPolicy,
        cancellations: &mut Vec<CallAuctionCancellation>,
    ) -> Result<(), CallAuctionPrepareError> {
        let buy_count =
            required_cancellation_count(&self.buy_order_scratch, plan.buy_fills(), policy);
        let sell_count =
            required_cancellation_count(&self.sell_order_scratch, plan.sell_fills(), policy);
        let count = buy_count
            .checked_add(sell_count)
            .ok_or(CallAuctionPrepareError::InternalInvariantViolation)?;
        if cancellations.capacity() < count {
            return Err(CallAuctionPrepareError::InternalInvariantViolation);
        }
        cancellations.clear();
        append_remainder_cancellations(
            &self.buy_order_scratch,
            plan.buy_fills(),
            policy,
            &self.orders,
            cancellations,
        )?;
        append_remainder_cancellations(
            &self.sell_order_scratch,
            plan.sell_fills(),
            policy,
            &self.orders,
            cancellations,
        )?;
        if cancellations.len() != count {
            return Err(CallAuctionPrepareError::InternalInvariantViolation);
        }
        Ok(())
    }

    fn validate_uncross_preparation(
        &self,
        prepared: &PreparedCallAuctionUncross,
    ) -> Result<(), CallAuctionCommitError> {
        if self.state_revision.checked_add(1) != Some(prepared.next_state_revision) {
            return Err(CallAuctionCommitError::InternalInvariantViolation);
        }
        self.validate_prepared_fills(
            prepared.allocation_plan().buy_fills(),
            Side::Buy,
            &self.buy_order_scratch,
        )?;
        self.validate_prepared_fills(
            prepared.allocation_plan().sell_fills(),
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
        let buys = prepared.allocation_plan().buy_fills();
        let sells = prepared.allocation_plan().sell_fills();
        let expected_count = required_pair_count(buys, sells)
            .ok_or(CallAuctionCommitError::InternalInvariantViolation)?;
        if prepared.trades().len() != expected_count {
            return Err(CallAuctionCommitError::InternalInvariantViolation);
        }
        let mut buy_index = 0_usize;
        let mut sell_index = 0_usize;
        let mut buy_remaining = buys.first().map_or(0_u64, |fill| fill.quantity_lots());
        let mut sell_remaining = sells.first().map_or(0_u64, |fill| fill.quantity_lots());
        let mut paired_quantity = 0_u128;
        for (index, trade) in prepared.trades().iter().copied().enumerate() {
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
                || trade.price != prepared.allocation_plan().clearing().price()
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
        let trade_count = u64::try_from(prepared.trades().len())
            .map_err(|_| CallAuctionCommitError::InternalInvariantViolation)?;
        if self.next_trade_id.checked_add(trade_count) != Some(prepared.next_trade_id)
            || paired_quantity != prepared.allocation_plan().executable_quantity()
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
            prepared.allocation_plan().buy_fills(),
            prepared.policy.remainder,
            prepared.cancellations(),
            &mut cancellation_index,
        )?;
        self.validate_prepared_side_cancellations(
            &self.sell_order_scratch,
            prepared.allocation_plan().sell_fills(),
            prepared.policy.remainder,
            prepared.cancellations(),
            &mut cancellation_index,
        )?;
        if cancellation_index != prepared.cancellations().len() {
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
        let account_side = self
            .account_orders
            .get_mut(&order.account_id)
            .expect("active order account index must exist")
            .side_mut(order.side);
        account_side.total_quantity -= u128::from(fill.quantity_lots());
        if let Some(active) = self.orders.get_mut(&order.order_id) {
            active.quantity = remaining;
        } else {
            debug_assert!(false, "validated partial fill order must remain indexed");
        }
    }

    fn insert_preflighted(
        &mut self,
        command: CallAuctionOrder,
        next_priority_sequence: u64,
        next_state_revision: u64,
    ) -> CallAuctionOrderSnapshot {
        let queue = self.queue_snapshot(command.side, command.constraint);
        let total_quantity = queue
            .total_quantity
            .checked_add(u128::from(command.quantity.lots()))
            .expect("preflighted auction aggregate must remain representable");
        let order_count = queue
            .order_count
            .checked_add(1)
            .expect("preflighted auction order count must remain representable");
        let mut resting = RestingAuctionOrder {
            order_id: command.order_id,
            account_id: command.account_id,
            side: command.side,
            constraint: command.constraint,
            quantity: command.quantity,
            priority_class: command.priority_class,
            priority_sequence: self.next_priority_sequence,
            previous: queue.tail,
            next: None,
            account_previous: None,
            account_next: None,
        };
        self.link_account_membership(&mut resting);
        if let Some(tail) = queue.tail {
            self.orders
                .get_mut(&tail)
                .expect("preflighted auction queue tail must remain active")
                .next = Some(command.order_id);
        }
        self.replace_queue(
            command.side,
            command.constraint,
            AuctionQueue {
                head: queue.head.or(Some(command.order_id)),
                tail: Some(command.order_id),
                total_quantity,
                order_count,
            },
        );
        let replaced_order = self.orders.insert(command.order_id, resting);
        let replaced_identity = self.seen_order_ids.insert(command.order_id, ());
        debug_assert!(replaced_order.is_none());
        debug_assert!(replaced_identity.is_none());
        self.next_priority_sequence = next_priority_sequence;
        self.state_revision = next_state_revision;
        resting.into()
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
        self.unlink_account_membership(order);
        let removed = self.orders.remove(&order.order_id);
        debug_assert_eq!(removed, Some(order));
    }

    fn link_account_membership(&mut self, order: &mut RestingAuctionOrder) {
        debug_assert!(order.account_previous.is_none() && order.account_next.is_none());
        if !self.account_orders.contains_key(&order.account_id) {
            let replaced = self
                .account_orders
                .insert(order.account_id, AuctionAccountIndex::default());
            debug_assert!(replaced.is_none());
        }
        let previous = self
            .account_orders
            .get(&order.account_id)
            .expect("active account index must exist")
            .side(order.side)
            .tail;
        order.account_previous = previous;
        if let Some(previous) = previous {
            let previous_order = self
                .orders
                .get_mut(&previous)
                .expect("account-index tail must remain active");
            debug_assert_eq!(previous_order.account_id, order.account_id);
            debug_assert_eq!(previous_order.side, order.side);
            debug_assert!(previous_order.account_next.is_none());
            previous_order.account_next = Some(order.order_id);
        }
        let side_index = self
            .account_orders
            .get_mut(&order.account_id)
            .expect("active account index must exist")
            .side_mut(order.side);
        side_index.head = side_index.head.or(Some(order.order_id));
        side_index.tail = Some(order.order_id);
        side_index.order_count = side_index
            .order_count
            .checked_add(1)
            .expect("active orders cannot exceed addressable memory");
        side_index.total_quantity = side_index
            .total_quantity
            .checked_add(u128::from(order.quantity.lots()))
            .expect("u64-sized active-order set cannot overflow u128 quantity");
    }

    fn unlink_account_membership(&mut self, order: RestingAuctionOrder) {
        if let Some(previous) = order.account_previous {
            self.orders
                .get_mut(&previous)
                .expect("previous account-index order must remain active")
                .account_next = order.account_next;
        }
        if let Some(next) = order.account_next {
            self.orders
                .get_mut(&next)
                .expect("next account-index order must remain active")
                .account_previous = order.account_previous;
        }
        let remove_account = {
            let index = self
                .account_orders
                .get_mut(&order.account_id)
                .expect("active order account index must exist");
            let side_index = index.side_mut(order.side);
            if side_index.head == Some(order.order_id) {
                side_index.head = order.account_next;
            }
            if side_index.tail == Some(order.order_id) {
                side_index.tail = order.account_previous;
            }
            side_index.order_count -= 1;
            side_index.total_quantity -= u128::from(order.quantity.lots());
            if side_index.order_count == 0 {
                debug_assert!(side_index.head.is_none());
                debug_assert!(side_index.tail.is_none());
                debug_assert_eq!(side_index.total_quantity, 0);
                *side_index = AuctionAccountSideIndex::default();
            }
            index.is_empty()
        };
        if remove_account {
            let removed = self.account_orders.remove(&order.account_id);
            debug_assert!(removed.is_some_and(AuctionAccountIndex::is_empty));
        }
    }

    fn try_append_account_side_values<T>(
        &self,
        index: AuctionAccountIndex,
        account_id: AccountId,
        side: Side,
        output: &mut Vec<T>,
        map: impl Fn(RestingAuctionOrder) -> T,
    ) -> Result<(), CallAuctionInvariantViolation> {
        let side_index = index.side(side);
        let mut current = side_index.head;
        let mut previous = None;
        let mut total_quantity = 0_u128;
        for _ in 0..side_index.order_count {
            let order_id = current.ok_or_else(|| {
                CallAuctionInvariantViolation::new(format!(
                    "account {account_id} {side:?} list ended before its declared count"
                ))
            })?;
            let order = self.orders.get(&order_id).copied().ok_or_else(|| {
                CallAuctionInvariantViolation::new(format!(
                    "account {account_id} list references absent order {order_id}"
                ))
            })?;
            if order.account_id != account_id
                || order.side != side
                || order.account_previous != previous
            {
                return Err(CallAuctionInvariantViolation::new(format!(
                    "account {account_id} {side:?} list references order {order_id} with inconsistent ownership, side, or previous link"
                )));
            }
            total_quantity = total_quantity
                .checked_add(u128::from(order.quantity.lots()))
                .ok_or_else(|| {
                    CallAuctionInvariantViolation::new(format!(
                        "account {account_id} {side:?} quantity aggregate overflows"
                    ))
                })?;
            output.push(map(order));
            previous = Some(order_id);
            current = order.account_next;
        }
        if current.is_some()
            || previous != side_index.tail
            || total_quantity != side_index.total_quantity
        {
            return Err(CallAuctionInvariantViolation::new(format!(
                "account {account_id} {side:?} list contradicts its tail, count, or quantity aggregate"
            )));
        }
        Ok(())
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
        self.buy_order_scratch
            .sort_unstable_by(|left, right| compare_order_priority(*left, *right, Side::Buy));
        self.sell_order_scratch.clear();
        append_queue_orders(
            self.market_sells,
            &self.orders,
            &mut self.sell_order_scratch,
        );
        for (_, queue) in self.asks.iter() {
            append_queue_orders(*queue, &self.orders, &mut self.sell_order_scratch);
        }
        self.sell_order_scratch
            .sort_unstable_by(|left, right| compare_order_priority(*left, *right, Side::Sell));
    }

    fn validate_queue(
        &self,
        queue: AuctionQueue,
        side: Side,
        constraint: AuctionOrderConstraint,
        indexed_orders: &mut usize,
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
        let mut count = 0_usize;
        let expected_count = usize::try_from(queue.order_count).map_err(|_| {
            CallAuctionInvariantViolation::new(
                "auction queue count cannot be represented on this target",
            )
        })?;
        while let Some(order_id) = current {
            if count >= expected_count {
                return Err(CallAuctionInvariantViolation::new(
                    "auction queue traversal exceeds recorded count",
                ));
            }
            if *indexed_orders >= self.orders.len() {
                return Err(CallAuctionInvariantViolation::new(
                    "active order occurs more than once or participates in an auction queue cycle",
                ));
            }
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
            total = total
                .checked_add(u128::from(order.quantity.lots()))
                .ok_or_else(|| {
                    CallAuctionInvariantViolation::new("auction queue quantity overflow")
                })?;
            count = count.checked_add(1).ok_or_else(|| {
                CallAuctionInvariantViolation::new("auction queue count overflow")
            })?;
            *indexed_orders = indexed_orders.checked_add(1).ok_or_else(|| {
                CallAuctionInvariantViolation::new("auction queue total count overflow")
            })?;
            previous = Some(order_id);
            previous_sequence = Some(order.priority_sequence);
            current = order.next;
        }
        if previous != queue.tail || count != expected_count || total != queue.total_quantity {
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
            order.priority_class,
            order.priority_sequence,
        ));
        appended += 1;
        current = order.next;
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuctionOrderConstraint, AuctionQueue, CallAuctionBook, CallAuctionBookLimits,
        CallAuctionBookLimitsSpec, CallAuctionBookQueryError, CallAuctionBookQueryResource,
        CallAuctionCancellation, CallAuctionCapacity, CallAuctionConstructionError,
        CallAuctionOrder, CallAuctionTrade, CallAuctionUncrossPool,
        reserve_call_auction_book_query_vec, try_uncross_vector,
    };
    use crate::auction::AuctionOrderFill;
    use crate::domain::{
        AccountId, AssetId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
        TimestampNs,
    };
    use crate::instrument::{
        InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
        QuantityRules, ReserveOrderRules, TradingState,
    };
    use crate::matching::MassCancelScope;

    fn definition() -> InstrumentDefinition {
        InstrumentDefinition::new(InstrumentSpec {
            instrument_id: InstrumentId::new(7).unwrap(),
            version: InstrumentVersion::new(3).unwrap(),
            effective_from: TimestampNs::from_unix_nanos(0),
            symbol: InstrumentSymbol::new("AUCTION-AUDIT").unwrap(),
            kind: InstrumentKind::Spot,
            base_asset_id: AssetId::new(1).unwrap(),
            quote_asset_id: AssetId::new(2).unwrap(),
            price: PriceRules::new(0, 5, Price::from_raw(-100), Price::from_raw(200)).unwrap(),
            quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
            reserve: ReserveOrderRules::disabled(),
            hidden_orders_supported: false,
            base_units_per_lot: 1,
            quote_units_per_price_unit: 1,
            trading_state: TradingState::Halted,
        })
        .unwrap()
    }

    fn two_order_book(constraint: AuctionOrderConstraint) -> CallAuctionBook {
        let limits = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
            max_active_orders: 2,
            max_price_levels_per_side: 2,
            max_accepted_order_ids: 2,
            max_prepared_uncrosses: 2,
        })
        .unwrap();
        let mut book = CallAuctionBook::try_with_limits(definition(), limits).unwrap();
        for order_id in [10_u64, 20] {
            book.admit(CallAuctionOrder::new(
                OrderId::new(order_id).unwrap(),
                AccountId::new(1).unwrap(),
                InstrumentId::new(7).unwrap(),
                InstrumentVersion::new(3).unwrap(),
                Side::Buy,
                constraint,
                Quantity::new(1).unwrap(),
                crate::auction::AuctionPriorityClass::HIGHEST,
            ))
            .unwrap();
        }
        book.validate().unwrap();
        book
    }

    #[test]
    fn unrepresentable_book_query_is_typed_and_nonpoisoning() {
        let resource = CallAuctionBookQueryResource::AccountOrderIds;
        let error =
            reserve_call_auction_book_query_vec::<OrderId>(usize::MAX, resource).unwrap_err();
        assert_eq!(
            error,
            CallAuctionBookQueryError::ReservationFailed {
                resource,
                maximum: usize::MAX,
            }
        );
    }

    #[test]
    fn every_uncross_buffer_class_reports_typed_unrepresentable_reservation() {
        for (result, capacity) in [
            (
                try_uncross_vector::<AuctionOrderFill>(
                    usize::MAX,
                    CallAuctionCapacity::UncrossBuyFills,
                ),
                CallAuctionCapacity::UncrossBuyFills,
            ),
            (
                try_uncross_vector::<AuctionOrderFill>(
                    usize::MAX,
                    CallAuctionCapacity::UncrossSellFills,
                ),
                CallAuctionCapacity::UncrossSellFills,
            ),
        ] {
            assert_eq!(
                result.unwrap_err(),
                CallAuctionConstructionError::CapacityReservationFailed(capacity)
            );
        }
        assert!(matches!(
            try_uncross_vector::<CallAuctionTrade>(usize::MAX, CallAuctionCapacity::UncrossTrades,),
            Err(CallAuctionConstructionError::CapacityReservationFailed(
                CallAuctionCapacity::UncrossTrades
            ))
        ));
        assert!(matches!(
            try_uncross_vector::<CallAuctionCancellation>(
                usize::MAX,
                CallAuctionCapacity::UncrossCancellations,
            ),
            Err(CallAuctionConstructionError::CapacityReservationFailed(
                CallAuctionCapacity::UncrossCancellations
            ))
        ));
        assert!(matches!(
            CallAuctionUncrossPool::try_new(usize::MAX, 1),
            Err(CallAuctionConstructionError::CapacityReservationFailed(
                CallAuctionCapacity::PreparedUncrosses
            ))
        ));
    }

    #[test]
    fn allocation_free_validation_rejects_market_and_limit_queue_cycles() {
        for constraint in [
            AuctionOrderConstraint::Market,
            AuctionOrderConstraint::Limit(Price::from_raw(100)),
        ] {
            let mut book = two_order_book(constraint);
            let head = OrderId::new(10).unwrap();
            let tail = OrderId::new(20).unwrap();
            book.orders.get_mut(&tail).unwrap().next = Some(head);
            let detail = book.validate().unwrap_err().detail().to_owned();
            assert!(
                detail.contains("cycle") || detail.contains("exceeds recorded count"),
                "unexpected invariant detail: {detail}"
            );
        }
    }

    #[test]
    fn allocation_free_validation_rejects_unqueued_active_orders() {
        let mut book = two_order_book(AuctionOrderConstraint::Market);
        book.market_buys = AuctionQueue::default();
        assert!(
            book.validate()
                .unwrap_err()
                .detail()
                .contains("unqueued order")
        );
    }

    #[test]
    fn allocation_free_validation_rejects_account_cycles_and_missing_membership() {
        let mut cyclic = two_order_book(AuctionOrderConstraint::Market);
        let head = OrderId::new(10).unwrap();
        let tail = OrderId::new(20).unwrap();
        cyclic.orders.get_mut(&tail).unwrap().account_next = Some(head);
        assert!(
            cyclic
                .validate()
                .unwrap_err()
                .detail()
                .contains("account-index cycle")
        );

        let mut missing = two_order_book(AuctionOrderConstraint::Market);
        missing.account_orders.remove(&AccountId::new(1).unwrap());
        assert!(
            missing
                .validate()
                .unwrap_err()
                .detail()
                .contains("absent from account indexes")
        );
    }

    #[test]
    fn account_order_query_reports_private_index_corruption_without_mutation() {
        let mut book = two_order_book(AuctionOrderConstraint::Market);
        let head = OrderId::new(10).unwrap();
        let tail = OrderId::new(20).unwrap();
        book.orders.get_mut(&tail).unwrap().account_next = Some(head);
        let before = book.resource_status();

        let error = book
            .try_account_active_order_ids(AccountId::new(1).unwrap(), MassCancelScope::All)
            .unwrap_err();
        assert!(matches!(
            error,
            CallAuctionBookQueryError::InvariantViolation(_)
        ));
        assert_eq!(book.resource_status(), before);
    }
}
