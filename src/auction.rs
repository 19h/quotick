//! Deterministic call-auction price discovery and order-level allocation.
//!
//! The kernel is independent of order storage and execution. It consumes
//! canonical aggregate bid and ask levels plus optional market quantities,
//! sweeps every constant demand/supply interval within an explicit aligned
//! price band, and selects one clearing price under an explicit tie-break
//! policy. Discovery is `O(B + A)` time and `O(1)` auxiliary space for `B` bid
//! levels and `A` ask levels. It performs no heap allocation and uses checked
//! `u128` aggregate quantity arithmetic.
//!
//! Separate price-time and price/class-tier pro-rata allocators reconcile
//! canonical order-level interest to one clearing result, fallibly reserve both
//! exact fill vectors, and emit independent per-side plans in `O(O_b + O_a)`
//! time and `O(F_b + F_a)` result space. Neither kernel mutates an order book or
//! constructs counterparty pairs.

use std::cmp::Ordering;
use std::fmt;

use crate::{OrderId, Price, Quantity, Side};

/// Positive raw-price increment defining the valid discrete auction grid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuctionPriceGrid {
    tick_size: u64,
    tick_i64: i64,
}

impl AuctionPriceGrid {
    /// Creates a grid anchored at raw price zero.
    ///
    /// # Errors
    ///
    /// Returns [`AuctionError::InvalidTickSize`] for zero or a value exceeding
    /// `i64::MAX`.
    pub fn new(tick_size: u64) -> Result<Self, AuctionError> {
        let Ok(tick_i64) = i64::try_from(tick_size) else {
            return Err(AuctionError::InvalidTickSize(tick_size));
        };
        if tick_i64 == 0 {
            return Err(AuctionError::InvalidTickSize(tick_size));
        }
        Ok(Self {
            tick_size,
            tick_i64,
        })
    }

    /// Returns the positive raw-price increment.
    #[must_use]
    pub const fn tick_size(self) -> u64 {
        self.tick_size
    }

    /// Returns whether a price lies on the zero-anchored grid.
    #[must_use]
    pub fn contains(self, price: Price) -> bool {
        price.raw().rem_euclid(self.tick_i64) == 0
    }

    fn successor(self, price: Price) -> Option<Price> {
        price.raw().checked_add(self.tick_i64).map(Price::from_raw)
    }

    fn predecessor(self, price: Price) -> Option<Price> {
        price.raw().checked_sub(self.tick_i64).map(Price::from_raw)
    }

    fn maximum(self) -> Price {
        Price::from_raw(i64::MAX - i64::MAX.rem_euclid(self.tick_i64))
    }

    fn minimum(self) -> Price {
        let remainder = i64::MIN.rem_euclid(self.tick_i64);
        if remainder == 0 {
            Price::from_raw(i64::MIN)
        } else {
            Price::from_raw(i64::MIN + (self.tick_i64 - remainder))
        }
    }
}

/// Inclusive finite candidate-price domain for market-aware auction discovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuctionPriceBand {
    minimum: Price,
    maximum: Price,
}

impl AuctionPriceBand {
    /// Creates an ordered band whose endpoints lie on `grid`.
    ///
    /// # Errors
    ///
    /// Returns [`AuctionError::PriceBandInverted`] when `minimum > maximum`,
    /// or a boundary-specific off-grid error when either endpoint is unaligned.
    pub fn new(
        grid: AuctionPriceGrid,
        minimum: Price,
        maximum: Price,
    ) -> Result<Self, AuctionError> {
        validate_price_band(grid, minimum, maximum)?;
        Ok(Self { minimum, maximum })
    }

    /// Returns the complete representable domain of one grid.
    #[must_use]
    pub fn full(grid: AuctionPriceGrid) -> Self {
        Self {
            minimum: grid.minimum(),
            maximum: grid.maximum(),
        }
    }

    /// Returns the inclusive lower candidate price.
    #[must_use]
    pub const fn minimum(self) -> Price {
        self.minimum
    }

    /// Returns the inclusive upper candidate price.
    #[must_use]
    pub const fn maximum(self) -> Price {
        self.maximum
    }

    pub(crate) const fn from_ordered_prices(minimum: Price, maximum: Price) -> Option<Self> {
        if minimum.raw() <= maximum.raw() {
            Some(Self { minimum, maximum })
        } else {
            None
        }
    }
}

/// Market-constrained quantities that remain eligible across an entire price band.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AuctionMarketInterest {
    buy_quantity: u128,
    sell_quantity: u128,
}

impl AuctionMarketInterest {
    /// No market-constrained interest on either side.
    pub const NONE: Self = Self::new(0, 0);

    /// Creates zero-or-positive market quantities in instrument-defined lots.
    #[must_use]
    pub const fn new(buy_quantity: u128, sell_quantity: u128) -> Self {
        Self {
            buy_quantity,
            sell_quantity,
        }
    }

    /// Returns market-constrained buy quantity in lots.
    #[must_use]
    pub const fn buy_quantity(self) -> u128 {
        self.buy_quantity
    }

    /// Returns market-constrained sell quantity in lots.
    #[must_use]
    pub const fn sell_quantity(self) -> u128 {
        self.sell_quantity
    }
}

/// One positive aggregate auction quantity at one price.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuctionLevel {
    price: Price,
    quantity: u128,
}

impl AuctionLevel {
    /// Creates a positive aggregate auction level.
    ///
    /// # Errors
    ///
    /// Returns [`AuctionError::ZeroLevelQuantity`] when `quantity` is zero.
    pub const fn new(price: Price, quantity: u128) -> Result<Self, AuctionError> {
        if quantity == 0 {
            Err(AuctionError::ZeroLevelQuantity)
        } else {
            Ok(Self { price, quantity })
        }
    }

    /// Returns the level price in instrument-defined quanta.
    #[must_use]
    pub const fn price(self) -> Price {
        self.price
    }

    /// Returns aggregate quantity in instrument-defined lots.
    #[must_use]
    pub const fn quantity(self) -> u128 {
        self.quantity
    }
}

/// Price constraint for one order participating in an auction allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuctionOrderConstraint {
    /// Eligible at every externally selected clearing price.
    Market,
    /// Eligible only when the limit price permits execution at the clearing price.
    Limit(Price),
}

/// Authoritative execution-priority tier within one auction price/constraint.
///
/// Lower numeric values execute before higher values. The type deliberately
/// carries no venue meaning: a gateway or controller must map its versioned
/// order categories to this scalar before admission.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct AuctionPriorityClass(u16);

impl AuctionPriorityClass {
    /// Highest representable priority class.
    pub const HIGHEST: Self = Self(0);

    /// Constructs a priority class from its stable wire scalar.
    #[must_use]
    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    /// Returns the stable wire scalar; lower values execute first.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0
    }
}

/// One positive order-level quantity supplied to auction allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuctionOrder {
    order_id: OrderId,
    constraint: AuctionOrderConstraint,
    quantity: Quantity,
    priority_class: AuctionPriorityClass,
    priority_sequence: u64,
}

impl AuctionOrder {
    /// Constructs one auction order from already validated domain values.
    ///
    /// Lower `priority_class` values precede higher values at an equal price.
    /// `priority_sequence` is the caller's monotonic order-priority sequence;
    /// [`OrderId`] is the final deterministic tie break.
    #[must_use]
    pub const fn new(
        order_id: OrderId,
        constraint: AuctionOrderConstraint,
        quantity: Quantity,
        priority_class: AuctionPriorityClass,
        priority_sequence: u64,
    ) -> Self {
        Self {
            order_id,
            constraint,
            quantity,
            priority_class,
            priority_sequence,
        }
    }

    /// Returns the caller-owned order identity.
    #[must_use]
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    /// Returns the market or limit constraint.
    #[must_use]
    pub const fn constraint(self) -> AuctionOrderConstraint {
        self.constraint
    }

    /// Returns the positive leaves quantity in instrument-defined lots.
    #[must_use]
    pub const fn quantity(self) -> Quantity {
        self.quantity
    }

    /// Returns the caller-defined priority class; lower values execute first.
    #[must_use]
    pub const fn priority_class(self) -> AuctionPriorityClass {
        self.priority_class
    }

    /// Returns the caller-defined priority sequence; lower values execute first.
    #[must_use]
    pub const fn priority_sequence(self) -> u64 {
        self.priority_sequence
    }
}

/// Finite input bound for one order-level auction allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuctionAllocationLimits {
    max_orders_per_side: usize,
}

/// Deterministic order-level allocation within one aggregate clearing result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuctionAllocationPolicy {
    /// Strict market/price/class/time/identity priority.
    PriceTime,
    /// Strict market/price/class tiers with pro-rata marginal allocation and
    /// time/identity priority for residual allocation quanta.
    ProRataTime,
}

#[derive(Clone, Copy)]
pub(crate) struct AuctionAllocationMethod {
    policy: AuctionAllocationPolicy,
    allocation_quantum_lots: u64,
}

impl AuctionAllocationMethod {
    pub(crate) const fn new(policy: AuctionAllocationPolicy, allocation_quantum_lots: u64) -> Self {
        Self {
            policy,
            allocation_quantum_lots,
        }
    }
}

impl AuctionAllocationLimits {
    /// Operational default for each side of one allocation request.
    pub const DEFAULT_MAX_ORDERS_PER_SIDE: usize = 65_536;

    /// Creates a positive per-side order bound.
    ///
    /// # Errors
    ///
    /// Returns [`AuctionAllocationError::InvalidOrderLimit`] for zero.
    pub const fn new(max_orders_per_side: usize) -> Result<Self, AuctionAllocationError> {
        if max_orders_per_side == 0 {
            Err(AuctionAllocationError::InvalidOrderLimit(
                max_orders_per_side,
            ))
        } else {
            Ok(Self {
                max_orders_per_side,
            })
        }
    }

    /// Returns the maximum supplied orders on either side.
    #[must_use]
    pub const fn max_orders_per_side(self) -> usize {
        self.max_orders_per_side
    }
}

impl Default for AuctionAllocationLimits {
    fn default() -> Self {
        Self {
            max_orders_per_side: Self::DEFAULT_MAX_ORDERS_PER_SIDE,
        }
    }
}

/// One positive order fill in a deterministic auction allocation plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuctionOrderFill {
    source_index: usize,
    order_id: OrderId,
    quantity_lots: u64,
}

impl AuctionOrderFill {
    /// Returns the order's zero-based index in its source-side slice.
    #[must_use]
    pub const fn source_index(self) -> usize {
        self.source_index
    }

    /// Returns the caller-owned order identity.
    #[must_use]
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    /// Returns the strictly positive allocated quantity in lots.
    #[must_use]
    pub const fn quantity_lots(self) -> u64 {
        self.quantity_lots
    }
}

/// Fully reconciled per-side fills at one aggregate clearing result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuctionAllocationPlan {
    clearing: AuctionClearing,
    buy_fills: Vec<AuctionOrderFill>,
    sell_fills: Vec<AuctionOrderFill>,
}

impl AuctionAllocationPlan {
    /// Returns the aggregate clearing state to which both sides reconcile.
    #[must_use]
    pub const fn clearing(&self) -> AuctionClearing {
        self.clearing
    }

    /// Returns the equal aggregate quantity allocated on each side.
    #[must_use]
    pub const fn executable_quantity(&self) -> u128 {
        self.clearing.executable_quantity()
    }

    /// Returns fills in canonical buy execution priority.
    #[must_use]
    pub fn buy_fills(&self) -> &[AuctionOrderFill] {
        &self.buy_fills
    }

    /// Returns fills in canonical sell execution priority.
    #[must_use]
    pub fn sell_fills(&self) -> &[AuctionOrderFill] {
        &self.sell_fills
    }

    pub(crate) fn with_fill_storage(
        buy_fills: Vec<AuctionOrderFill>,
        sell_fills: Vec<AuctionOrderFill>,
    ) -> Self {
        Self {
            clearing: AuctionClearing::from_quantities(Price::from_raw(0), 0, 0),
            buy_fills,
            sell_fills,
        }
    }

    pub(crate) fn clear_reusable(&mut self) {
        self.clearing = AuctionClearing::from_quantities(Price::from_raw(0), 0, 0);
        self.buy_fills.clear();
        self.sell_fills.clear();
    }

    pub(crate) fn minimum_fill_capacity(&self) -> usize {
        self.buy_fills.capacity().min(self.sell_fills.capacity())
    }
}

/// Invalid input or resource exhaustion during order-level auction allocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuctionAllocationError {
    /// The configured per-side order maximum was zero.
    InvalidOrderLimit(usize),
    /// The configured pro-rata allocation quantum was zero.
    InvalidAllocationQuantum(u64),
    /// A supplied side exceeded its explicit operational bound.
    OrderLimitExceeded {
        /// Side exceeding the bound.
        side: Side,
        /// Supplied order count.
        count: usize,
        /// Configured maximum.
        maximum: usize,
    },
    /// The selected clearing price was not aligned to the allocation grid.
    ClearingPriceOffGrid,
    /// A limit order was not aligned to the allocation grid.
    LimitPriceOffGrid {
        /// Side containing the invalid order.
        side: Side,
        /// Zero-based source index.
        index: usize,
    },
    /// An order quantity was not aligned to the pro-rata allocation quantum.
    OrderQuantityOffAllocationGrid {
        /// Side containing the invalid order.
        side: Side,
        /// Zero-based source index.
        index: usize,
        /// Supplied order quantity in instrument-defined lots.
        quantity_lots: u64,
        /// Required allocation quantum in instrument-defined lots.
        allocation_quantum_lots: u64,
    },
    /// Market/price/class/time/ID priority was not strictly canonical.
    NonCanonicalOrders {
        /// Side whose priority order failed validation.
        side: Side,
        /// Zero-based index of the first invalid order.
        index: usize,
    },
    /// Eligible order quantity exceeded `u128` on one side.
    QuantityOverflow(Side),
    /// Eligible order totals did not equal the supplied discovery result.
    ClearingQuantityMismatch {
        /// Buy quantity recorded by discovery.
        expected_buy: u128,
        /// Buy quantity reconstructed from eligible orders.
        observed_buy: u128,
        /// Sell quantity recorded by discovery.
        expected_sell: u128,
        /// Sell quantity reconstructed from eligible orders.
        observed_sell: u128,
    },
    /// The supplied aggregate state had no executable quantity.
    ZeroExecutableQuantity,
    /// Exact fill-vector reservation failed before plan construction.
    CapacityReservationFailed(Side),
    /// An internal fill scalar could not fit its order-level `u64` representation.
    FillQuantityOverflow(Side),
    /// Validated eligible quantity was insufficient while constructing fills.
    InsufficientEligibleQuantity(Side),
}

impl fmt::Display for AuctionAllocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOrderLimit(limit) => {
                write!(formatter, "invalid auction allocation order limit {limit}")
            }
            Self::InvalidAllocationQuantum(quantum) => {
                write!(formatter, "invalid auction allocation quantum {quantum}")
            }
            Self::OrderLimitExceeded {
                side,
                count,
                maximum,
            } => write!(
                formatter,
                "auction {side:?} order count {count} exceeds maximum {maximum}"
            ),
            Self::ClearingPriceOffGrid => {
                formatter.write_str("auction clearing price is off allocation grid")
            }
            Self::LimitPriceOffGrid { side, index } => write!(
                formatter,
                "auction {side:?} limit order at index {index} is off grid"
            ),
            Self::OrderQuantityOffAllocationGrid {
                side,
                index,
                quantity_lots,
                allocation_quantum_lots,
            } => write!(
                formatter,
                "auction {side:?} order quantity {quantity_lots} at index {index} is not aligned to allocation quantum {allocation_quantum_lots}"
            ),
            Self::NonCanonicalOrders { side, index } => write!(
                formatter,
                "auction {side:?} order priority is noncanonical at index {index}"
            ),
            Self::QuantityOverflow(side) => {
                write!(formatter, "auction {side:?} eligible quantity overflow")
            }
            Self::ClearingQuantityMismatch {
                expected_buy,
                observed_buy,
                expected_sell,
                observed_sell,
            } => write!(
                formatter,
                "auction clearing quantities buy={expected_buy}/{observed_buy} sell={expected_sell}/{observed_sell} do not reconcile"
            ),
            Self::ZeroExecutableQuantity => {
                formatter.write_str("auction clearing has zero executable quantity")
            }
            Self::CapacityReservationFailed(side) => {
                write!(formatter, "auction {side:?} fill reservation failed")
            }
            Self::FillQuantityOverflow(side) => {
                write!(formatter, "auction {side:?} fill quantity exceeds u64")
            }
            Self::InsufficientEligibleQuantity(side) => {
                write!(
                    formatter,
                    "auction {side:?} eligible quantity was insufficient"
                )
            }
        }
    }
}

impl std::error::Error for AuctionAllocationError {}

/// Whether equal-volume/equal-imbalance candidates follow market pressure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuctionPressureRule {
    /// Proceed directly to reference-price proximity.
    Ignore,
    /// Buy imbalance selects the higher price and sell imbalance the lower
    /// price when both candidates have the same imbalance side.
    FavorImbalance,
}

/// Deterministic final rule after every preceding auction criterion ties.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuctionFinalTieBreak {
    /// Select the lower raw price.
    LowerPrice,
    /// Select the higher raw price.
    HigherPrice,
}

/// Clearing-price ranking policy after volume and absolute imbalance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuctionPricePolicy {
    pressure_rule: AuctionPressureRule,
    final_tie_break: AuctionFinalTieBreak,
}

impl AuctionPricePolicy {
    /// Ignore pressure, minimize reference distance, then select lower price.
    pub const REFERENCE_THEN_LOWER: Self = Self::new(
        AuctionPressureRule::Ignore,
        AuctionFinalTieBreak::LowerPrice,
    );

    /// Ignore pressure, minimize reference distance, then select higher price.
    pub const REFERENCE_THEN_HIGHER: Self = Self::new(
        AuctionPressureRule::Ignore,
        AuctionFinalTieBreak::HigherPrice,
    );

    /// Follow same-side imbalance pressure, then reference distance and lower price.
    pub const PRESSURE_THEN_REFERENCE_LOWER: Self = Self::new(
        AuctionPressureRule::FavorImbalance,
        AuctionFinalTieBreak::LowerPrice,
    );

    /// Follow same-side imbalance pressure, then reference distance and higher price.
    pub const PRESSURE_THEN_REFERENCE_HIGHER: Self = Self::new(
        AuctionPressureRule::FavorImbalance,
        AuctionFinalTieBreak::HigherPrice,
    );

    /// Constructs a policy from explicit pressure and final-tie behavior.
    #[must_use]
    pub const fn new(
        pressure_rule: AuctionPressureRule,
        final_tie_break: AuctionFinalTieBreak,
    ) -> Self {
        Self {
            pressure_rule,
            final_tie_break,
        }
    }

    /// Returns the optional imbalance-pressure rule.
    #[must_use]
    pub const fn pressure_rule(self) -> AuctionPressureRule {
        self.pressure_rule
    }

    /// Returns the final deterministic price rule.
    #[must_use]
    pub const fn final_tie_break(self) -> AuctionFinalTieBreak {
        self.final_tie_break
    }
}

impl Default for AuctionPricePolicy {
    fn default() -> Self {
        Self::REFERENCE_THEN_LOWER
    }
}

/// Aggregate state evaluated at one selected auction clearing price.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AuctionClearing {
    price: Price,
    executable_quantity: u128,
    buy_quantity: u128,
    sell_quantity: u128,
    imbalance_side: Option<Side>,
    imbalance_quantity: u128,
}

impl AuctionClearing {
    /// Evaluates aggregate executable volume and imbalance at one price.
    ///
    /// `buy_quantity` is cumulative demand priced at or above `price`;
    /// `sell_quantity` is cumulative supply priced at or below it.
    #[must_use]
    pub const fn from_quantities(price: Price, buy_quantity: u128, sell_quantity: u128) -> Self {
        let executable_quantity = if buy_quantity < sell_quantity {
            buy_quantity
        } else {
            sell_quantity
        };
        let (imbalance_side, imbalance_quantity) = if buy_quantity > sell_quantity {
            (Some(Side::Buy), buy_quantity - sell_quantity)
        } else if sell_quantity > buy_quantity {
            (Some(Side::Sell), sell_quantity - buy_quantity)
        } else {
            (None, 0)
        };
        Self {
            price,
            executable_quantity,
            buy_quantity,
            sell_quantity,
            imbalance_side,
            imbalance_quantity,
        }
    }

    /// Returns the selected clearing price.
    #[must_use]
    pub const fn price(self) -> Price {
        self.price
    }

    /// Returns executable aggregate quantity in lots.
    #[must_use]
    pub const fn executable_quantity(self) -> u128 {
        self.executable_quantity
    }

    /// Returns cumulative eligible buy quantity at the selected price.
    #[must_use]
    pub const fn buy_quantity(self) -> u128 {
        self.buy_quantity
    }

    /// Returns cumulative eligible sell quantity at the selected price.
    #[must_use]
    pub const fn sell_quantity(self) -> u128 {
        self.sell_quantity
    }

    /// Returns the side with excess eligible quantity, or `None` when balanced.
    #[must_use]
    pub const fn imbalance_side(self) -> Option<Side> {
        self.imbalance_side
    }

    /// Returns absolute unexecuted eligible quantity at the selected price.
    #[must_use]
    pub const fn imbalance_quantity(self) -> u128 {
        self.imbalance_quantity
    }

    /// Returns whether this candidate outranks another under the supplied policy.
    #[must_use]
    pub fn is_better_than(
        self,
        other: Self,
        reference_price: Price,
        policy: AuctionPricePolicy,
    ) -> bool {
        if self.executable_quantity != other.executable_quantity {
            return self.executable_quantity > other.executable_quantity;
        }
        if self.imbalance_quantity != other.imbalance_quantity {
            return self.imbalance_quantity < other.imbalance_quantity;
        }
        if policy.pressure_rule == AuctionPressureRule::FavorImbalance
            && self.imbalance_side == other.imbalance_side
            && self.price != other.price
        {
            match self.imbalance_side {
                Some(Side::Buy) => return self.price > other.price,
                Some(Side::Sell) => return self.price < other.price,
                None => {}
            }
        }
        let self_distance = self.price.raw().abs_diff(reference_price.raw());
        let other_distance = other.price.raw().abs_diff(reference_price.raw());
        if self_distance != other_distance {
            return self_distance < other_distance;
        }
        match policy.final_tie_break {
            AuctionFinalTieBreak::LowerPrice => self.price < other.price,
            AuctionFinalTieBreak::HigherPrice => self.price > other.price,
        }
    }
}

/// Invalid aggregate input or arithmetic exhaustion during auction discovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuctionError {
    /// Tick size was zero or exceeded the signed raw-price representation.
    InvalidTickSize(u64),
    /// The inclusive candidate-price minimum exceeded its maximum.
    PriceBandInverted,
    /// The inclusive candidate-price minimum was not grid aligned.
    PriceBandMinimumOffGrid,
    /// The inclusive candidate-price maximum was not grid aligned.
    PriceBandMaximumOffGrid,
    /// An aggregate level had zero quantity.
    ZeroLevelQuantity,
    /// One aggregate level was not aligned to the selected tick grid.
    PriceOffGrid {
        /// Side containing the invalid level.
        side: Side,
        /// Zero-based index of the invalid level.
        index: usize,
    },
    /// The reference price was not aligned to the selected tick grid.
    ReferencePriceOffGrid,
    /// Levels were duplicated or not strictly ordered for their side.
    NonCanonicalLevels {
        /// Side whose level order failed validation.
        side: Side,
        /// Zero-based index of the first invalid level.
        index: usize,
    },
    /// Aggregate quantity exceeded `u128` on one side.
    QuantityOverflow(Side),
    /// A cumulative aggregate contradicted the validated level total.
    QuantityUnderflow(Side),
}

impl fmt::Display for AuctionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTickSize(tick_size) => {
                write!(formatter, "invalid auction tick size {tick_size}")
            }
            Self::PriceBandInverted => formatter.write_str("auction price band is inverted"),
            Self::PriceBandMinimumOffGrid => {
                formatter.write_str("auction price-band minimum is off grid")
            }
            Self::PriceBandMaximumOffGrid => {
                formatter.write_str("auction price-band maximum is off grid")
            }
            Self::ZeroLevelQuantity => formatter.write_str("auction level quantity is zero"),
            Self::PriceOffGrid { side, index } => write!(
                formatter,
                "auction {side:?} level at index {index} is off grid"
            ),
            Self::ReferencePriceOffGrid => {
                formatter.write_str("auction reference price is off grid")
            }
            Self::NonCanonicalLevels { side, index } => write!(
                formatter,
                "auction {side:?} levels are noncanonical at index {index}"
            ),
            Self::QuantityOverflow(side) => {
                write!(formatter, "auction {side:?} aggregate quantity overflow")
            }
            Self::QuantityUnderflow(side) => {
                write!(formatter, "auction {side:?} aggregate quantity underflow")
            }
        }
    }
}

impl std::error::Error for AuctionError {}

/// Discovers one deterministic clearing price from canonical aggregate depth.
///
/// This is the zero-market, full-representable-grid convenience form of
/// [`discover_bounded_clearing_price`]. Bids must be strictly descending and
/// asks strictly ascending by price. Every level and the reference must lie on
/// `grid`. Demand changes one tick above a bid price; supply changes at an ask
/// price. The algorithm merges those transition points and evaluates the
/// complete closed grid interval before the next transition, including prices
/// with no order at that exact value. A candidate interval must have non-zero
/// executable quantity. Ranking is:
///
/// 1. maximum executable quantity;
/// 2. minimum absolute imbalance;
/// 3. optional same-side imbalance pressure;
/// 4. minimum unsigned distance to `reference_price`;
/// 5. the policy's explicit final lower/higher rule.
///
/// # Errors
///
/// Returns [`AuctionError::NonCanonicalLevels`] for unordered or duplicate
/// prices, [`AuctionError::PriceOffGrid`] or
/// [`AuctionError::ReferencePriceOffGrid`] for grid violations, and
/// [`AuctionError::QuantityOverflow`] when a side total exceeds `u128`.
/// [`AuctionError::QuantityUnderflow`] fails closed if a running cumulative
/// quantity contradicts its validated total.
pub fn discover_clearing_price(
    bids: &[AuctionLevel],
    asks: &[AuctionLevel],
    grid: AuctionPriceGrid,
    reference_price: Price,
    policy: AuctionPricePolicy,
) -> Result<Option<AuctionClearing>, AuctionError> {
    discover_bounded_clearing_price(
        bids,
        asks,
        AuctionMarketInterest::NONE,
        grid,
        AuctionPriceBand::full(grid),
        reference_price,
        policy,
    )
}

/// Discovers a clearing price within an inclusive band with market interest.
///
/// Limit levels remain canonical aggregate depth on `grid`, but may lie outside
/// `price_band`: a buy below the minimum or sell above the maximum is never
/// eligible, while a buy above the maximum or sell below the minimum remains
/// eligible throughout the band. Market quantities are eligible at every
/// candidate. The aligned reference may also lie outside the band and is then
/// clamped to a tied candidate interval unless imbalance pressure selects an
/// interval edge first.
///
/// The sweep initializes demand and supply at the inclusive band minimum, then
/// merges only bid-successor and ask transitions within the band. It evaluates
/// every complete constant-state grid interval in `O(B + A)` time and `O(1)`
/// auxiliary space without enumerating the numeric price range or allocating.
/// Candidate ranking is identical to [`discover_clearing_price`].
///
/// # Errors
///
/// Returns [`AuctionError`] for a band/grid mismatch, off-grid reference or
/// level, noncanonical levels, aggregate overflow after adding market interest,
/// or an internal cumulative underflow.
pub fn discover_bounded_clearing_price(
    bids: &[AuctionLevel],
    asks: &[AuctionLevel],
    market: AuctionMarketInterest,
    grid: AuctionPriceGrid,
    price_band: AuctionPriceBand,
    reference_price: Price,
    policy: AuctionPricePolicy,
) -> Result<Option<AuctionClearing>, AuctionError> {
    validate_price_band(grid, price_band.minimum, price_band.maximum)?;
    if !grid.contains(reference_price) {
        return Err(AuctionError::ReferencePriceOffGrid);
    }
    let total_buy = validate_levels(bids, Side::Buy, grid)?;
    let total_sell = validate_levels(asks, Side::Sell, grid)?;
    total_buy
        .checked_add(market.buy_quantity)
        .ok_or(AuctionError::QuantityOverflow(Side::Buy))?;
    total_sell
        .checked_add(market.sell_quantity)
        .ok_or(AuctionError::QuantityOverflow(Side::Sell))?;

    let mut demand = market.buy_quantity;
    let mut remaining_bids = 0_usize;
    for level in bids.iter().copied() {
        if level.price < price_band.minimum {
            break;
        }
        demand = demand
            .checked_add(level.quantity)
            .ok_or(AuctionError::QuantityOverflow(Side::Buy))?;
        remaining_bids += 1;
    }

    let mut supply = market.sell_quantity;
    let mut next_ask = 0_usize;
    for level in asks.iter().copied() {
        if level.price > price_band.minimum {
            break;
        }
        supply = supply
            .checked_add(level.quantity)
            .ok_or(AuctionError::QuantityOverflow(Side::Sell))?;
        next_ask += 1;
    }

    let mut interval_lower = price_band.minimum;
    let mut best = None;

    loop {
        let next_bid_transition = remaining_bids
            .checked_sub(1)
            .and_then(|index| grid.successor(bids[index].price))
            .filter(|price| *price <= price_band.maximum);
        let next_ask_price = asks
            .get(next_ask)
            .map(|level| level.price)
            .filter(|price| *price <= price_band.maximum);
        let next_transition = minimum_price(next_bid_transition, next_ask_price);
        let interval_upper = next_transition
            .and_then(|next| grid.predecessor(next))
            .unwrap_or(price_band.maximum);
        if interval_lower <= interval_upper {
            let candidate_price = interval_candidate_price(
                interval_lower,
                interval_upper,
                reference_price,
                demand,
                supply,
                policy,
            );
            let candidate = AuctionClearing::from_quantities(candidate_price, demand, supply);
            if candidate.executable_quantity != 0
                && best.is_none_or(|current| {
                    candidate.is_better_than(current, reference_price, policy)
                })
            {
                best = Some(candidate);
            }
        }

        let Some(transition_price) = next_transition else {
            break;
        };
        while asks
            .get(next_ask)
            .is_some_and(|level| level.price == transition_price)
        {
            supply = supply
                .checked_add(asks[next_ask].quantity)
                .ok_or(AuctionError::QuantityOverflow(Side::Sell))?;
            next_ask += 1;
        }
        while remaining_bids != 0
            && grid.successor(bids[remaining_bids - 1].price) == Some(transition_price)
        {
            demand = demand
                .checked_sub(bids[remaining_bids - 1].quantity)
                .ok_or(AuctionError::QuantityUnderflow(Side::Buy))?;
            remaining_bids -= 1;
        }
        interval_lower = transition_price;
    }
    Ok(best)
}

/// Allocates a non-zero clearing result by strict market/price/class/time/ID priority.
///
/// Each side must be canonical: market constraints first; limit constraints in
/// economically best-to-worst order (descending buys, ascending sells); and at
/// an equal constraint, ascending priority class, priority sequence, then order
/// ID. Only orders eligible at `clearing.price()` contribute to reconciliation.
/// Their totals must exactly equal `clearing.buy_quantity()` and
/// `clearing.sell_quantity()`. Order identities are assumed unique under the
/// owning book's identity invariant; this linear allocator does not allocate a
/// separate identity set, while source indexes keep every emitted fill tied to
/// its exact input row.
///
/// This function allocates each side independently and deliberately does not
/// pair counterparties, apply self-trade policy, assign trade identifiers,
/// mutate leaves, or emit events. It performs `O(B + A)` validation and
/// allocation work, reserves exactly the required positive fill counts before
/// constructing either side, and uses `O(F_b + F_a)` result memory.
///
/// # Errors
///
/// Returns [`AuctionAllocationError`] for an invalid operational limit,
/// over-limit input, grid or canonical-priority violation, quantity mismatch,
/// zero executable quantity, arithmetic exhaustion, or fill-vector reservation
/// failure.
pub fn allocate_clearing_price_time(
    bids: &[AuctionOrder],
    asks: &[AuctionOrder],
    grid: AuctionPriceGrid,
    clearing: AuctionClearing,
    limits: AuctionAllocationLimits,
) -> Result<AuctionAllocationPlan, AuctionAllocationError> {
    allocate_clearing_with_policy(
        bids,
        asks,
        grid,
        clearing,
        AuctionAllocationPolicy::PriceTime,
        1,
        limits,
    )
}

/// Allocates a non-zero clearing result by price/class-tier pro-rata priority.
///
/// Market, economically better price, and lower priority-class tiers precede
/// worse tiers. Every tier before the marginal tier fills completely. Orders
/// in the marginal tier receive their floored proportional share in multiples
/// of `allocation_quantum_lots`; remaining quanta are assigned once in
/// canonical time/identity order. The implementation uses exact fixed-width
/// multiply/divide without constructing an overflowing intermediate product.
///
/// # Errors
///
/// Returns [`AuctionAllocationError`] for the price-time validation failures,
/// a zero allocation quantum, an order quantity off the allocation grid,
/// arithmetic exhaustion, or fill-vector reservation failure.
pub fn allocate_clearing_pro_rata_time(
    bids: &[AuctionOrder],
    asks: &[AuctionOrder],
    grid: AuctionPriceGrid,
    clearing: AuctionClearing,
    allocation_quantum_lots: u64,
    limits: AuctionAllocationLimits,
) -> Result<AuctionAllocationPlan, AuctionAllocationError> {
    allocate_clearing_with_policy(
        bids,
        asks,
        grid,
        clearing,
        AuctionAllocationPolicy::ProRataTime,
        allocation_quantum_lots,
        limits,
    )
}

pub(crate) fn allocate_clearing_with_policy(
    bids: &[AuctionOrder],
    asks: &[AuctionOrder],
    grid: AuctionPriceGrid,
    clearing: AuctionClearing,
    policy: AuctionAllocationPolicy,
    allocation_quantum_lots: u64,
    limits: AuctionAllocationLimits,
) -> Result<AuctionAllocationPlan, AuctionAllocationError> {
    let (buy_fill_count, sell_fill_count) = validate_policy_allocation_request(
        bids,
        asks,
        grid,
        clearing,
        policy,
        allocation_quantum_lots,
        limits,
    )?;
    let mut buy_fills = Vec::new();
    buy_fills
        .try_reserve_exact(buy_fill_count)
        .map_err(|_| AuctionAllocationError::CapacityReservationFailed(Side::Buy))?;
    let mut sell_fills = Vec::new();
    sell_fills
        .try_reserve_exact(sell_fill_count)
        .map_err(|_| AuctionAllocationError::CapacityReservationFailed(Side::Sell))?;
    build_allocation_plan(
        bids,
        asks,
        clearing,
        policy,
        allocation_quantum_lots,
        &mut buy_fills,
        &mut sell_fills,
    )
}

/// Uses caller-owned fill vectors whose capacities cover the validated result.
pub(crate) fn allocate_clearing_reusing(
    bids: &[AuctionOrder],
    asks: &[AuctionOrder],
    grid: AuctionPriceGrid,
    clearing: AuctionClearing,
    method: AuctionAllocationMethod,
    limits: AuctionAllocationLimits,
    plan: &mut AuctionAllocationPlan,
) -> Result<(), AuctionAllocationError> {
    let (buy_fill_count, sell_fill_count) = validate_policy_allocation_request(
        bids,
        asks,
        grid,
        clearing,
        method.policy,
        method.allocation_quantum_lots,
        limits,
    )?;
    if plan.buy_fills.capacity() < buy_fill_count {
        return Err(AuctionAllocationError::CapacityReservationFailed(Side::Buy));
    }
    if plan.sell_fills.capacity() < sell_fill_count {
        return Err(AuctionAllocationError::CapacityReservationFailed(
            Side::Sell,
        ));
    }
    plan.buy_fills.clear();
    plan.sell_fills.clear();
    let executable_quantity = clearing.executable_quantity();
    append_policy_fills(
        bids,
        Side::Buy,
        clearing.price(),
        executable_quantity,
        method.policy,
        method.allocation_quantum_lots,
        &mut plan.buy_fills,
    )?;
    append_policy_fills(
        asks,
        Side::Sell,
        clearing.price(),
        executable_quantity,
        method.policy,
        method.allocation_quantum_lots,
        &mut plan.sell_fills,
    )?;
    plan.clearing = clearing;
    Ok(())
}

fn validate_policy_allocation_request(
    bids: &[AuctionOrder],
    asks: &[AuctionOrder],
    grid: AuctionPriceGrid,
    clearing: AuctionClearing,
    policy: AuctionAllocationPolicy,
    allocation_quantum_lots: u64,
    limits: AuctionAllocationLimits,
) -> Result<(usize, usize), AuctionAllocationError> {
    let price_time_counts = validate_allocation_request(bids, asks, grid, clearing, limits)?;
    if policy == AuctionAllocationPolicy::PriceTime {
        return Ok(price_time_counts);
    }
    validate_allocation_quantum(bids, Side::Buy, allocation_quantum_lots)?;
    validate_allocation_quantum(asks, Side::Sell, allocation_quantum_lots)?;
    let executable_quantity = clearing.executable_quantity();
    Ok((
        walk_pro_rata_fills(
            bids,
            Side::Buy,
            clearing.price(),
            executable_quantity,
            allocation_quantum_lots,
            |_, _, _| Ok(()),
        )?,
        walk_pro_rata_fills(
            asks,
            Side::Sell,
            clearing.price(),
            executable_quantity,
            allocation_quantum_lots,
            |_, _, _| Ok(()),
        )?,
    ))
}

fn validate_allocation_request(
    bids: &[AuctionOrder],
    asks: &[AuctionOrder],
    grid: AuctionPriceGrid,
    clearing: AuctionClearing,
    limits: AuctionAllocationLimits,
) -> Result<(usize, usize), AuctionAllocationError> {
    if limits.max_orders_per_side == 0 {
        return Err(AuctionAllocationError::InvalidOrderLimit(0));
    }
    validate_order_count(bids, Side::Buy, limits)?;
    validate_order_count(asks, Side::Sell, limits)?;
    if !grid.contains(clearing.price()) {
        return Err(AuctionAllocationError::ClearingPriceOffGrid);
    }

    let observed_buy = validate_allocation_orders(bids, Side::Buy, grid, clearing.price())?;
    let observed_sell = validate_allocation_orders(asks, Side::Sell, grid, clearing.price())?;
    if observed_buy != clearing.buy_quantity() || observed_sell != clearing.sell_quantity() {
        return Err(AuctionAllocationError::ClearingQuantityMismatch {
            expected_buy: clearing.buy_quantity(),
            observed_buy,
            expected_sell: clearing.sell_quantity(),
            observed_sell,
        });
    }
    let executable_quantity = clearing.executable_quantity();
    if executable_quantity == 0 {
        return Err(AuctionAllocationError::ZeroExecutableQuantity);
    }

    Ok((
        required_fill_count(bids, Side::Buy, clearing.price(), executable_quantity),
        required_fill_count(asks, Side::Sell, clearing.price(), executable_quantity),
    ))
}

fn build_allocation_plan(
    bids: &[AuctionOrder],
    asks: &[AuctionOrder],
    clearing: AuctionClearing,
    policy: AuctionAllocationPolicy,
    allocation_quantum_lots: u64,
    buy_fills: &mut Vec<AuctionOrderFill>,
    sell_fills: &mut Vec<AuctionOrderFill>,
) -> Result<AuctionAllocationPlan, AuctionAllocationError> {
    buy_fills.clear();
    sell_fills.clear();
    let executable_quantity = clearing.executable_quantity();
    append_policy_fills(
        bids,
        Side::Buy,
        clearing.price(),
        executable_quantity,
        policy,
        allocation_quantum_lots,
        buy_fills,
    )?;
    append_policy_fills(
        asks,
        Side::Sell,
        clearing.price(),
        executable_quantity,
        policy,
        allocation_quantum_lots,
        sell_fills,
    )?;
    Ok(AuctionAllocationPlan {
        clearing,
        buy_fills: std::mem::take(buy_fills),
        sell_fills: std::mem::take(sell_fills),
    })
}

fn validate_order_count(
    orders: &[AuctionOrder],
    side: Side,
    limits: AuctionAllocationLimits,
) -> Result<(), AuctionAllocationError> {
    if orders.len() > limits.max_orders_per_side {
        return Err(AuctionAllocationError::OrderLimitExceeded {
            side,
            count: orders.len(),
            maximum: limits.max_orders_per_side,
        });
    }
    Ok(())
}

fn validate_allocation_orders(
    orders: &[AuctionOrder],
    side: Side,
    grid: AuctionPriceGrid,
    clearing_price: Price,
) -> Result<u128, AuctionAllocationError> {
    let mut previous = None;
    let mut eligible_quantity = 0_u128;
    for (index, order) in orders.iter().copied().enumerate() {
        if let AuctionOrderConstraint::Limit(price) = order.constraint {
            if !grid.contains(price) {
                return Err(AuctionAllocationError::LimitPriceOffGrid { side, index });
            }
        }
        if previous.is_some_and(|previous| !precedes(previous, order, side)) {
            return Err(AuctionAllocationError::NonCanonicalOrders { side, index });
        }
        if is_eligible(order, side, clearing_price) {
            eligible_quantity = eligible_quantity
                .checked_add(u128::from(order.quantity.lots()))
                .ok_or(AuctionAllocationError::QuantityOverflow(side))?;
        }
        previous = Some(order);
    }
    Ok(eligible_quantity)
}

fn precedes(previous: AuctionOrder, current: AuctionOrder, side: Side) -> bool {
    compare_order_priority(previous, current, side) == Ordering::Less
}

pub(crate) fn compare_order_priority(
    previous: AuctionOrder,
    current: AuctionOrder,
    side: Side,
) -> Ordering {
    match (previous.constraint, current.constraint) {
        (AuctionOrderConstraint::Market, AuctionOrderConstraint::Limit(_)) => Ordering::Less,
        (AuctionOrderConstraint::Limit(_), AuctionOrderConstraint::Market) => Ordering::Greater,
        (AuctionOrderConstraint::Market, AuctionOrderConstraint::Market) => {
            priority_key(previous).cmp(&priority_key(current))
        }
        (AuctionOrderConstraint::Limit(previous_price), AuctionOrderConstraint::Limit(price)) => {
            if previous_price == price {
                priority_key(previous).cmp(&priority_key(current))
            } else {
                match side {
                    Side::Buy => price.cmp(&previous_price),
                    Side::Sell => previous_price.cmp(&price),
                }
            }
        }
    }
}

fn priority_key(order: AuctionOrder) -> (AuctionPriorityClass, u64, OrderId) {
    (
        order.priority_class,
        order.priority_sequence,
        order.order_id,
    )
}

fn is_eligible(order: AuctionOrder, side: Side, clearing_price: Price) -> bool {
    match order.constraint {
        AuctionOrderConstraint::Market => true,
        AuctionOrderConstraint::Limit(price) => match side {
            Side::Buy => price >= clearing_price,
            Side::Sell => price <= clearing_price,
        },
    }
}

fn required_fill_count(
    orders: &[AuctionOrder],
    side: Side,
    clearing_price: Price,
    mut remaining: u128,
) -> usize {
    let mut count = 0_usize;
    for order in orders.iter().copied() {
        if remaining == 0 || !is_eligible(order, side, clearing_price) {
            break;
        }
        remaining = remaining.saturating_sub(u128::from(order.quantity.lots()));
        count += 1;
    }
    count
}

fn append_fills(
    orders: &[AuctionOrder],
    side: Side,
    clearing_price: Price,
    mut remaining: u128,
    fills: &mut Vec<AuctionOrderFill>,
) -> Result<(), AuctionAllocationError> {
    for (source_index, order) in orders.iter().copied().enumerate() {
        if remaining == 0 || !is_eligible(order, side, clearing_price) {
            break;
        }
        let fill_quantity = remaining.min(u128::from(order.quantity.lots()));
        let quantity_lots = u64::try_from(fill_quantity)
            .map_err(|_| AuctionAllocationError::FillQuantityOverflow(side))?;
        fills.push(AuctionOrderFill {
            source_index,
            order_id: order.order_id,
            quantity_lots,
        });
        remaining -= fill_quantity;
    }
    if remaining != 0 {
        return Err(AuctionAllocationError::InsufficientEligibleQuantity(side));
    }
    Ok(())
}

fn validate_allocation_quantum(
    orders: &[AuctionOrder],
    side: Side,
    allocation_quantum_lots: u64,
) -> Result<(), AuctionAllocationError> {
    if allocation_quantum_lots == 0 {
        return Err(AuctionAllocationError::InvalidAllocationQuantum(0));
    }
    for (index, order) in orders.iter().copied().enumerate() {
        if order.quantity.lots() % allocation_quantum_lots != 0 {
            return Err(AuctionAllocationError::OrderQuantityOffAllocationGrid {
                side,
                index,
                quantity_lots: order.quantity.lots(),
                allocation_quantum_lots,
            });
        }
    }
    Ok(())
}

fn append_policy_fills(
    orders: &[AuctionOrder],
    side: Side,
    clearing_price: Price,
    executable_quantity: u128,
    policy: AuctionAllocationPolicy,
    allocation_quantum_lots: u64,
    fills: &mut Vec<AuctionOrderFill>,
) -> Result<(), AuctionAllocationError> {
    match policy {
        AuctionAllocationPolicy::PriceTime => {
            append_fills(orders, side, clearing_price, executable_quantity, fills)
        }
        AuctionAllocationPolicy::ProRataTime => {
            walk_pro_rata_fills(
                orders,
                side,
                clearing_price,
                executable_quantity,
                allocation_quantum_lots,
                |source_index, order, quantity_lots| {
                    fills.push(AuctionOrderFill {
                        source_index,
                        order_id: order.order_id,
                        quantity_lots,
                    });
                    Ok(())
                },
            )?;
            Ok(())
        }
    }
}

fn walk_pro_rata_fills(
    orders: &[AuctionOrder],
    side: Side,
    clearing_price: Price,
    mut remaining: u128,
    allocation_quantum_lots: u64,
    mut emit: impl FnMut(usize, AuctionOrder, u64) -> Result<(), AuctionAllocationError>,
) -> Result<usize, AuctionAllocationError> {
    if allocation_quantum_lots == 0 {
        return Err(AuctionAllocationError::InvalidAllocationQuantum(0));
    }
    let allocation_quantum = u128::from(allocation_quantum_lots);
    let mut source_index = 0_usize;
    let mut fill_count = 0_usize;
    while remaining != 0 {
        let Some(first) = orders.get(source_index).copied() else {
            return Err(AuctionAllocationError::InsufficientEligibleQuantity(side));
        };
        if !is_eligible(first, side, clearing_price) {
            return Err(AuctionAllocationError::InsufficientEligibleQuantity(side));
        }
        let mut tier_end = source_index + 1;
        while orders.get(tier_end).is_some_and(|order| {
            is_eligible(*order, side, clearing_price) && same_allocation_tier(first, *order)
        }) {
            tier_end += 1;
        }
        let mut tier_quantity = 0_u128;
        for order in orders[source_index..tier_end].iter().copied() {
            tier_quantity = tier_quantity
                .checked_add(u128::from(order.quantity.lots()))
                .ok_or(AuctionAllocationError::QuantityOverflow(side))?;
        }
        if remaining >= tier_quantity {
            for (offset, order) in orders[source_index..tier_end].iter().copied().enumerate() {
                emit(source_index + offset, order, order.quantity.lots())?;
                fill_count = fill_count
                    .checked_add(1)
                    .ok_or(AuctionAllocationError::FillQuantityOverflow(side))?;
            }
            remaining -= tier_quantity;
            source_index = tier_end;
            continue;
        }

        if remaining % allocation_quantum != 0 || tier_quantity % allocation_quantum != 0 {
            return Err(AuctionAllocationError::InsufficientEligibleQuantity(side));
        }
        let remaining_quanta = remaining / allocation_quantum;
        let tier_quanta = tier_quantity / allocation_quantum;
        let mut allocated_quanta = 0_u128;
        for order in orders[source_index..tier_end].iter().copied() {
            let order_quanta = order.quantity.lots() / allocation_quantum_lots;
            let base = exact_mul_div_floor(order_quanta, remaining_quanta, tier_quanta)
                .ok_or(AuctionAllocationError::FillQuantityOverflow(side))?;
            allocated_quanta = allocated_quanta
                .checked_add(u128::from(base))
                .ok_or(AuctionAllocationError::QuantityOverflow(side))?;
        }
        let mut residual_quanta = remaining_quanta
            .checked_sub(allocated_quanta)
            .ok_or(AuctionAllocationError::QuantityOverflow(side))?;
        for (offset, order) in orders[source_index..tier_end].iter().copied().enumerate() {
            let order_quanta = order.quantity.lots() / allocation_quantum_lots;
            let mut fill_quanta = exact_mul_div_floor(order_quanta, remaining_quanta, tier_quanta)
                .ok_or(AuctionAllocationError::FillQuantityOverflow(side))?;
            if residual_quanta != 0 && fill_quanta < order_quanta {
                fill_quanta = fill_quanta
                    .checked_add(1)
                    .ok_or(AuctionAllocationError::FillQuantityOverflow(side))?;
                residual_quanta -= 1;
            }
            if fill_quanta == 0 {
                continue;
            }
            let quantity_lots = fill_quanta
                .checked_mul(allocation_quantum_lots)
                .ok_or(AuctionAllocationError::FillQuantityOverflow(side))?;
            emit(source_index + offset, order, quantity_lots)?;
            fill_count = fill_count
                .checked_add(1)
                .ok_or(AuctionAllocationError::FillQuantityOverflow(side))?;
        }
        if residual_quanta != 0 {
            return Err(AuctionAllocationError::InsufficientEligibleQuantity(side));
        }
        remaining = 0;
    }
    Ok(fill_count)
}

fn same_allocation_tier(first: AuctionOrder, other: AuctionOrder) -> bool {
    first.constraint == other.constraint && first.priority_class == other.priority_class
}

fn exact_mul_div_floor(factor: u64, multiplier: u128, divisor: u128) -> Option<u64> {
    if divisor == 0 || multiplier >= divisor {
        return None;
    }
    let mut quotient = 0_u64;
    let mut remainder = 0_u128;
    for bit in (0..u64::BITS).rev() {
        let (doubled, double_carry) = modular_double(remainder, divisor);
        remainder = doubled;
        let mut carry = double_carry;
        if factor & (1_u64 << bit) != 0 {
            let (added, add_carry) = modular_add(remainder, multiplier, divisor);
            remainder = added;
            carry += add_carry;
        }
        quotient = quotient.checked_mul(2)?.checked_add(carry)?;
    }
    Some(quotient)
}

fn modular_double(value: u128, modulus: u128) -> (u128, u64) {
    let complement = modulus - value;
    if value >= complement {
        (value - complement, 1)
    } else {
        (value + value, 0)
    }
}

fn modular_add(left: u128, right: u128, modulus: u128) -> (u128, u64) {
    let complement = modulus - right;
    if left >= complement {
        (left - complement, 1)
    } else {
        (left + right, 0)
    }
}

fn validate_price_band(
    grid: AuctionPriceGrid,
    minimum: Price,
    maximum: Price,
) -> Result<(), AuctionError> {
    if minimum > maximum {
        return Err(AuctionError::PriceBandInverted);
    }
    if !grid.contains(minimum) {
        return Err(AuctionError::PriceBandMinimumOffGrid);
    }
    if !grid.contains(maximum) {
        return Err(AuctionError::PriceBandMaximumOffGrid);
    }
    Ok(())
}

fn validate_levels(
    levels: &[AuctionLevel],
    side: Side,
    grid: AuctionPriceGrid,
) -> Result<u128, AuctionError> {
    let mut total = 0_u128;
    let mut previous = None;
    for (index, level) in levels.iter().copied().enumerate() {
        if !grid.contains(level.price) {
            return Err(AuctionError::PriceOffGrid { side, index });
        }
        if previous.is_some_and(|previous_price| match side {
            Side::Buy => previous_price <= level.price,
            Side::Sell => previous_price >= level.price,
        }) {
            return Err(AuctionError::NonCanonicalLevels { side, index });
        }
        total = total
            .checked_add(level.quantity)
            .ok_or(AuctionError::QuantityOverflow(side))?;
        previous = Some(level.price);
    }
    Ok(total)
}

fn minimum_price(first: Option<Price>, second: Option<Price>) -> Option<Price> {
    match (first, second) {
        (Some(first), Some(second)) => Some(first.min(second)),
        (Some(first), None) => Some(first),
        (None, Some(second)) => Some(second),
        (None, None) => None,
    }
}

fn interval_candidate_price(
    lower: Price,
    upper: Price,
    reference: Price,
    demand: u128,
    supply: u128,
    policy: AuctionPricePolicy,
) -> Price {
    if policy.pressure_rule == AuctionPressureRule::FavorImbalance {
        if demand > supply {
            return upper;
        }
        if supply > demand {
            return lower;
        }
    }
    reference.max(lower).min(upper)
}
