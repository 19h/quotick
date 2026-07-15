//! A deterministic, single-instrument, price-time-priority matching engine.
//!
//! One [`OrderBook`] is intended to be owned by one execution shard.  It uses
//! fixed-capacity dense hash indexes, intrusive per-account/per-side indexes,
//! and intrusive FIFO links inside ordered price levels. New resting orders and cancellations
//! avoid scans of an entire price level or book. A redundant complete best-level
//! cache makes market-extremum discovery `O(1)`. Its key-checked stable-slot
//! handle makes non-empty maker-level aggregate mutation and reserve FIFO-tail
//! refresh `O(1)`; ordered level insertion, empty-level deletion, and next-worse
//! traversal remain `O(log(P + 1))`, where `P` is the number of occupied price
//! levels. A command consuming `E` displayed maker slices and exhausting `L`
//! price levels is `O(E + (L + 1) log(P + 1))`. A reserve maker can contribute
//! multiple slices. Account
//! mass-cancel and block-and-cancel selection visit `K` selected orders;
//! instrument transition-and-cancel visits all `O` active orders. Both
//! canonically sort in `O(K log K)` or `O(O log O)`. FOK inspection uses `O(1)` auxiliary space and visits each
//! active order in crossed levels at most once; it never materializes reserve
//! replenishment slices.

use std::cmp::Reverse;
use std::fmt;
use std::hash::Hash;
use std::ops::Index;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::bounded_hash::{BoundedHashMap, BoundedHashSet};
use crate::domain::{
    AccountId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs, TradeId,
};
use crate::indexed_avl::{IndexedAvlHandle, IndexedAvlMap};
use crate::instrument::{AdmissionError, InstrumentDefinition, TradingState};
use crate::trace_arena::AppendOnlyTraceArena;

static NEXT_ORDER_BOOK_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

fn next_order_book_instance_id() -> Result<u64, MatchingError> {
    let mut current = NEXT_ORDER_BOOK_INSTANCE_ID.load(Ordering::Relaxed);
    loop {
        let next = current
            .checked_add(1)
            .ok_or(MatchingError::BookInstanceIdExhausted)?;
        match NEXT_ORDER_BOOK_INSTANCE_ID.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(current),
            Err(observed) => current = observed,
        }
    }
}

/// Execution constraint applied when an order is immediately active or a stop
/// order is triggered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopActivation {
    /// Execute at available prices and cancel any unfilled remainder.
    Market,
    /// Execute at the specified price or better and apply the order lifetime
    /// to any residual.
    Limit(Price),
}

impl StopActivation {
    const fn order_type(self) -> OrderType {
        match self {
            Self::Market => OrderType::Market,
            Self::Limit(price) => OrderType::Limit(price),
        }
    }

    const fn risk_price(self) -> Option<Price> {
        match self {
            Self::Market => None,
            Self::Limit(price) => Some(price),
        }
    }
}

/// The price and trigger constraint attached to a new order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderType {
    /// Execute at available prices and never rest.
    Market,
    /// Execute at the specified price or better.
    Limit(Price),
    /// Remain dormant until the explicit last-trade reference reaches the
    /// side-derived trigger condition, then apply the supplied constraint.
    Stop {
        /// Buy stops trigger at or above this price; sell stops trigger at or
        /// below it.
        trigger_price: Price,
        /// Constraint applied after deterministic activation.
        activation: StopActivation,
    },
}

impl OrderType {
    /// Returns the dormant stop instruction, if this is a stop order.
    #[must_use]
    pub const fn stop(self) -> Option<(Price, StopActivation)> {
        match self {
            Self::Stop {
                trigger_price,
                activation,
            } => Some((trigger_price, activation)),
            Self::Market | Self::Limit(_) => None,
        }
    }

    const fn active(self) -> Self {
        match self {
            Self::Market | Self::Limit(_) => self,
            Self::Stop { activation, .. } => activation.order_type(),
        }
    }
}

/// Quantity exposure policy for a resting limit order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderDisplay {
    /// Expose the complete resting leaves quantity.
    FullyDisplayed,
    /// Expose at most one fixed peak while retaining additional hidden leaves.
    Reserve {
        /// Maximum displayed lots in each replenished slice.
        peak: Quantity,
    },
    /// Expose no resting quantity while remaining executable at the limit
    /// price behind every displayed-priority order at that price.
    Hidden,
}

impl OrderDisplay {
    /// Returns true for a native reserve order.
    #[must_use]
    pub const fn is_reserve(self) -> bool {
        matches!(self, Self::Reserve { .. })
    }

    /// Returns true for a fully hidden resting order.
    #[must_use]
    pub const fn is_hidden(self) -> bool {
        matches!(self, Self::Hidden)
    }

    /// Returns the configured reserve peak, if any.
    #[must_use]
    pub const fn peak(self) -> Option<Quantity> {
        match self {
            Self::FullyDisplayed | Self::Hidden => None,
            Self::Reserve { peak } => Some(peak),
        }
    }

    const fn working_lots(self, total_leaves: u64) -> u64 {
        match self {
            Self::FullyDisplayed | Self::Hidden => total_leaves,
            Self::Reserve { peak } => {
                if peak.lots() < total_leaves {
                    peak.lots()
                } else {
                    total_leaves
                }
            }
        }
    }

    const fn visible_lots(self, working_lots: u64) -> u64 {
        match self {
            Self::FullyDisplayed | Self::Reserve { .. } => working_lots,
            Self::Hidden => 0,
        }
    }

    const fn same_mode(self, other: Self) -> bool {
        matches!(
            (self, other),
            (Self::FullyDisplayed, Self::FullyDisplayed)
                | (Self::Reserve { .. }, Self::Reserve { .. })
                | (Self::Hidden, Self::Hidden)
        )
    }
}

/// Lifetime and immediate-execution constraint for an order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeInForce {
    /// Rest any unfilled limit quantity until explicitly cancelled.
    GoodTilCancelled,
    /// Rest unfilled limit quantity until an explicit inclusive expiry sweep.
    GoodTilTimestamp {
        /// Absolute UTC expiration in nanoseconds since the Unix epoch.
        expires_at: TimestampNs,
    },
    /// Execute immediately and cancel any unfilled remainder.
    ImmediateOrCancel,
    /// Execute the complete quantity immediately or reject without mutation.
    FillOrKill,
    /// Reject if any quantity would execute immediately; otherwise rest.
    PostOnly,
}

impl TimeInForce {
    /// Returns whether this lifetime permits a limit order to remain active.
    #[must_use]
    pub const fn may_rest(self) -> bool {
        matches!(
            self,
            Self::GoodTilCancelled | Self::GoodTilTimestamp { .. } | Self::PostOnly
        )
    }

    /// Returns the absolute expiration timestamp for GTD orders.
    #[must_use]
    pub const fn expires_at(self) -> Option<TimestampNs> {
        match self {
            Self::GoodTilTimestamp { expires_at } => Some(expires_at),
            Self::GoodTilCancelled
            | Self::ImmediateOrCancel
            | Self::FillOrKill
            | Self::PostOnly => None,
        }
    }
}

/// Action taken when an incoming order would trade with the same account.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SelfTradePrevention {
    /// Cancel the unfilled incoming quantity.
    CancelAggressor,
    /// Cancel the resting order and continue matching the incoming order.
    CancelResting,
    /// Cancel the resting order and the unfilled incoming quantity.
    CancelBoth,
    /// Decrement both orders by the prevented quantity and cancel the exhausted order.
    DecrementAndCancel,
}

/// A validated request to enter a new order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NewOrder {
    /// Idempotency key for the command.
    pub command_id: CommandId,
    /// Client-supplied order identifier, never reusable within this book.
    pub order_id: OrderId,
    /// Beneficial owner used for authorization and self-trade prevention.
    pub account_id: AccountId,
    /// Instrument routed to this book.
    pub instrument_id: InstrumentId,
    /// Immutable instrument-rule version used for admission and replay.
    pub instrument_version: InstrumentVersion,
    /// Buy or sell.
    pub side: Side,
    /// Original quantity.
    pub quantity: Quantity,
    /// Fully displayed or fixed-peak reserve exposure while resting.
    pub display: OrderDisplay,
    /// Immediate market/limit or dormant stop constraint.
    pub order_type: OrderType,
    /// Lifetime constraint.
    pub time_in_force: TimeInForce,
    /// Self-trade prevention action.
    pub self_trade_prevention: SelfTradePrevention,
    /// Gateway receive time.
    pub received_at: TimestampNs,
}

/// A request to cancel a resting or dormant order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CancelOrder {
    /// Idempotency key for the command.
    pub command_id: CommandId,
    /// Order to cancel.
    pub order_id: OrderId,
    /// Account authorizing cancellation.
    pub account_id: AccountId,
    /// Instrument routed to this book.
    pub instrument_id: InstrumentId,
    /// Immutable instrument-rule version used for routing and replay.
    pub instrument_version: InstrumentVersion,
    /// Gateway receive time.
    pub received_at: TimestampNs,
}

/// Selection applied by one account-scoped mass-cancel command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MassCancelScope {
    /// Cancel every active order owned by the account in this instrument shard.
    All,
    /// Cancel only active orders on one side.
    Side(Side),
}

impl MassCancelScope {
    /// Returns whether the scope selects the supplied side.
    #[must_use]
    pub fn includes(self, side: Side) -> bool {
        match self {
            Self::All => true,
            Self::Side(selected) => selected == side,
        }
    }
}

/// A request to cancel a canonical account-scoped set of active orders.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MassCancel {
    /// Idempotency key for the mass-cancel control.
    pub command_id: CommandId,
    /// Beneficial owner whose orders are selected.
    pub account_id: AccountId,
    /// Instrument routed to this book.
    pub instrument_id: InstrumentId,
    /// Immutable instrument-rule version used for routing and replay.
    pub instrument_version: InstrumentVersion,
    /// All owned orders or only one owned side.
    pub scope: MassCancelScope,
    /// Gateway receive time.
    pub received_at: TimestampNs,
}

/// A request to replace a resting limit or dormant stop-limit order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplaceOrder {
    /// Idempotency key for the command.
    pub command_id: CommandId,
    /// Order to amend.
    pub order_id: OrderId,
    /// Account authorizing replacement.
    pub account_id: AccountId,
    /// Instrument routed to this book.
    pub instrument_id: InstrumentId,
    /// Immutable instrument-rule version used for admission and replay.
    pub instrument_version: InstrumentVersion,
    /// New leaves quantity, not new original quantity.
    pub new_quantity: Quantity,
    /// New limit price.
    pub new_price: Price,
    /// New display policy. Changing among fully displayed, reserve, and fully
    /// hidden modes is rejected.
    pub new_display: OrderDisplay,
    /// Gateway receive time.
    pub received_at: TimestampNs,
}

/// Runtime account admission state owned by one execution shard.
///
/// This fence is independent of immutable numerical risk limits. A blocked
/// account may still cancel existing orders but cannot enter or replace orders.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AccountAdmissionState {
    /// New orders and replacements remain eligible for ordinary admission.
    #[default]
    Enabled,
    /// New orders and replacements are rejected at the matching boundary.
    Blocked,
}

/// Revisioned administrative transition for one account admission fence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountControlAction {
    /// Close entry atomically and cancel every active order for the account.
    BlockAndCancel,
    /// Reopen entry without creating or modifying any order.
    Enable,
}

impl AccountControlAction {
    const fn resulting_state(self) -> AccountAdmissionState {
        match self {
            Self::BlockAndCancel => AccountAdmissionState::Blocked,
            Self::Enable => AccountAdmissionState::Enabled,
        }
    }
}

/// Revision-checked administrative command for one account in one instrument shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccountControl {
    /// Globally unique idempotency key within the shard generation.
    pub command_id: CommandId,
    /// Account whose entry fence is changed.
    pub account_id: AccountId,
    /// Routed instrument.
    pub instrument_id: InstrumentId,
    /// Immutable instrument-definition version expected by the controller.
    pub instrument_version: InstrumentVersion,
    /// Current revision observed by the controller; genesis is revision zero.
    pub expected_revision: u64,
    /// Requested fence transition.
    pub action: AccountControlAction,
    /// Gateway/controller receive time.
    pub received_at: TimestampNs,
}

/// Mutation applied by one instrument-wide trading-state control.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TradingStateControlAction {
    /// Change state without cancelling active orders.
    Transition,
    /// Change to an entry-closed state and atomically cancel every active order.
    TransitionAndCancel,
}

/// Revision-checked instrument-wide trading-state command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TradingStateControl {
    /// Globally unique idempotency key within the shard generation.
    pub command_id: CommandId,
    /// Routed instrument.
    pub instrument_id: InstrumentId,
    /// Immutable instrument-definition version expected by the controller.
    pub instrument_version: InstrumentVersion,
    /// Current revision observed by the controller; genesis is revision zero.
    pub expected_revision: u64,
    /// Requested effective trading state.
    pub target_state: TradingState,
    /// Transition-only or transition-plus-cancel-all behavior.
    pub action: TradingStateControlAction,
    /// Gateway/controller receive time.
    pub received_at: TimestampNs,
}

/// Advances the deterministic inclusive GTD-expiry watermark for one shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpirySweep {
    /// Globally unique idempotency key within the shard generation.
    pub command_id: CommandId,
    /// Routed instrument.
    pub instrument_id: InstrumentId,
    /// Immutable instrument-definition version expected by the controller.
    pub instrument_version: InstrumentVersion,
    /// Inclusive absolute expiration horizon.
    pub through: TimestampNs,
    /// Gateway/controller receive time; the horizon cannot exceed this value.
    pub received_at: TimestampNs,
}

/// Advances the deterministic last-trade stop reference and activates a
/// bounded canonical prefix of newly eligible dormant stops.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StopTriggerSweep {
    /// Globally unique idempotency key within the shard generation.
    pub command_id: CommandId,
    /// Routed instrument.
    pub instrument_id: InstrumentId,
    /// Immutable instrument-definition version expected by the controller.
    pub instrument_version: InstrumentVersion,
    /// New authoritative last-trade reference price.
    pub reference_price: Price,
    /// Maximum dormant orders this command may activate.
    pub maximum_orders: u32,
    /// Gateway/controller receive time.
    pub received_at: TimestampNs,
}

/// Read-only effective instrument trading state and control revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TradingStateSnapshot {
    state: TradingState,
    revision: u64,
}

impl TradingStateSnapshot {
    pub(crate) const fn from_parts(state: TradingState, revision: u64) -> Self {
        Self { state, revision }
    }

    /// Returns the effective trading state.
    #[must_use]
    pub const fn state(self) -> TradingState {
        self.state
    }

    /// Returns the last accepted state-control revision; zero denotes genesis.
    #[must_use]
    pub const fn revision(self) -> u64 {
        self.revision
    }
}

/// Read-only effective account-fence state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AccountControlSnapshot {
    state: AccountAdmissionState,
    revision: u64,
}

impl AccountControlSnapshot {
    pub(crate) const fn from_parts(state: AccountAdmissionState, revision: u64) -> Self {
        Self { state, revision }
    }

    /// Returns the effective admission state.
    #[must_use]
    pub const fn state(self) -> AccountAdmissionState {
        self.state
    }

    /// Returns the last accepted control revision; zero denotes no control yet.
    #[must_use]
    pub const fn revision(self) -> u64 {
        self.revision
    }
}

/// A state-changing order-book command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Command {
    /// Submit a new order.
    New(NewOrder),
    /// Cancel a resting or dormant order.
    Cancel(CancelOrder),
    /// Replace a resting limit or dormant stop-limit order.
    Replace(ReplaceOrder),
    /// Cancel an account-scoped set of active orders.
    MassCancel(MassCancel),
    /// Change an account admission fence, optionally cancelling all active orders atomically.
    AccountControl(AccountControl),
    /// Change the instrument-wide effective trading state.
    TradingStateControl(TradingStateControl),
    /// Expire every GTD order at or before an inclusive absolute horizon.
    ExpirySweep(ExpirySweep),
    /// Advance the last-trade stop reference and activate a bounded prefix.
    StopTriggerSweep(StopTriggerSweep),
}

impl Command {
    const fn command_id(self) -> CommandId {
        match self {
            Self::New(command) => command.command_id,
            Self::Cancel(command) => command.command_id,
            Self::Replace(command) => command.command_id,
            Self::MassCancel(command) => command.command_id,
            Self::AccountControl(command) => command.command_id,
            Self::TradingStateControl(command) => command.command_id,
            Self::ExpirySweep(command) => command.command_id,
            Self::StopTriggerSweep(command) => command.command_id,
        }
    }

    const fn received_at(self) -> TimestampNs {
        match self {
            Self::New(command) => command.received_at,
            Self::Cancel(command) => command.received_at,
            Self::Replace(command) => command.received_at,
            Self::MassCancel(command) => command.received_at,
            Self::AccountControl(command) => command.received_at,
            Self::TradingStateControl(command) => command.received_at,
            Self::ExpirySweep(command) => command.received_at,
            Self::StopTriggerSweep(command) => command.received_at,
        }
    }
}

/// Why an otherwise well-formed command was rejected.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectReason {
    /// The command was routed to a different instrument.
    WrongInstrument,
    /// The order identifier has already been used.
    DuplicateOrder,
    /// The order does not exist in the active book.
    UnknownOrder,
    /// The authorizing account does not own the order.
    NotOrderOwner,
    /// A market order used a resting-only lifetime.
    MarketOrderCannotRest,
    /// A market order used post-only behavior.
    MarketOrderCannotPost,
    /// Fill-or-kill with decrement-and-cancel has ambiguous full-fill semantics.
    UnsupportedFokSelfTradePolicy,
    /// Available eligible liquidity cannot completely fill a fill-or-kill order.
    InsufficientLiquidity,
    /// A post-only order would execute on entry.
    PostOnlyWouldCross,
    /// The command referenced another immutable instrument version.
    WrongInstrumentVersion,
    /// Instrument trading state disabled entry or amendment.
    InstrumentNotOpen,
    /// Limit price was not aligned to the tick grid.
    PriceOffTickGrid,
    /// Limit price was outside the inclusive collar.
    PriceOutsideCollar,
    /// Quantity was not aligned to the lot increment.
    QuantityOffGrid,
    /// Quantity was outside inclusive order-size limits.
    QuantityOutsideLimits,
    /// No pre-trade risk profile existed for the account.
    RiskProfileMissing,
    /// Account-level risk state blocked order entry.
    RiskAccountBlocked,
    /// Order would not strictly reduce a reduce-only account's exposure.
    RiskReduceOnly,
    /// Order quantity exceeded the per-order risk limit.
    RiskOrderQuantityLimit,
    /// Worst-case order notional exceeded the per-order risk limit.
    RiskOrderNotionalLimit,
    /// Resting-order count would exceed the account limit.
    RiskOpenOrderCountLimit,
    /// Aggregate resting quantity would exceed the account limit.
    RiskOpenQuantityLimit,
    /// Aggregate worst-case resting notional would exceed the account limit.
    RiskOpenNotionalLimit,
    /// Worst-case executed position would exceed the long or short limit.
    RiskPositionLimit,
    /// Conservative risk arithmetic exceeded its integer representation.
    RiskArithmeticOverflow,
    /// The immutable instrument version disables reserve orders.
    ReserveOrderNotSupported,
    /// The immutable instrument version disables fully hidden orders.
    HiddenOrderNotSupported,
    /// A reserve display peak was not aligned to the lot increment.
    DisplayQuantityOffGrid,
    /// A reserve display peak was not smaller than total leaves.
    DisplayQuantityNotLessThanOrder,
    /// The order would require more peak refreshes than its admitted state permits.
    ReserveReplenishmentLimit,
    /// A reserve qualifier was attached to a non-resting order.
    ReserveOrderCannotBeImmediate,
    /// A hidden qualifier was attached to a non-resting order.
    HiddenOrderCannotBeImmediate,
    /// Replacement attempted to convert between displayed and reserve modes.
    OrderDisplayModeChangeNotAllowed,
    /// A shard-local administrative fence blocks new orders and replacements.
    AccountAdmissionBlocked,
    /// The account-control compare revision did not equal current state.
    AccountControlRevisionMismatch,
    /// The account-control revision cannot advance beyond `u64::MAX`.
    AccountControlRevisionExhausted,
    /// The trading-state compare revision did not equal current state.
    TradingStateControlRevisionMismatch,
    /// The trading-state control revision cannot advance beyond `u64::MAX`.
    TradingStateControlRevisionExhausted,
    /// The requested trading state is already effective.
    TradingStateUnchanged,
    /// Cancel-all cannot transition into the entry-open state.
    TradingStateControlCannotCancelIntoOpen,
    /// A GTD deadline was not later than intake or the committed expiry watermark.
    OrderAlreadyExpired,
    /// An expiry sweep attempted to move the committed watermark backward.
    ExpiryWatermarkRegression,
    /// An expiry sweep horizon was later than the command's own receive time.
    ExpiryHorizonAfterCommandTime,
    /// A stop order was submitted before any authoritative reference price.
    StopReferenceUnavailable,
    /// A stop condition was already satisfied by the committed reference.
    StopAlreadyTriggered,
    /// A stop-trigger command requested no activations.
    StopTriggerBatchEmpty,
    /// A stop-trigger command requested more orders than the active-order bound.
    StopTriggerBatchExceedsActiveOrderCapacity,
    /// Eligible stops at the current reference must be drained before it moves.
    StopTriggerBacklog,
    /// A dormant stop-market order cannot use post-only behavior.
    StopMarketCannotPost,
    /// The replacement schema cannot amend a dormant stop-market constraint.
    StopMarketCannotBeReplaced,
}

impl From<AdmissionError> for RejectReason {
    fn from(error: AdmissionError) -> Self {
        match error {
            AdmissionError::WrongInstrument => Self::WrongInstrument,
            AdmissionError::WrongVersion => Self::WrongInstrumentVersion,
            AdmissionError::TradingStateDisallowsEntry => Self::InstrumentNotOpen,
            AdmissionError::PriceOffTickGrid => Self::PriceOffTickGrid,
            AdmissionError::PriceOutsideCollar => Self::PriceOutsideCollar,
            AdmissionError::QuantityOffGrid => Self::QuantityOffGrid,
            AdmissionError::QuantityOutsideLimits => Self::QuantityOutsideLimits,
            AdmissionError::ReserveOrderNotSupported => Self::ReserveOrderNotSupported,
            AdmissionError::HiddenOrderNotSupported => Self::HiddenOrderNotSupported,
            AdmissionError::DisplayQuantityOffGrid => Self::DisplayQuantityOffGrid,
            AdmissionError::DisplayQuantityNotLessThanOrder => {
                Self::DisplayQuantityNotLessThanOrder
            }
            AdmissionError::ReserveReplenishmentLimit => Self::ReserveReplenishmentLimit,
        }
    }
}

/// Why remaining order quantity was cancelled.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelReason {
    /// Explicit cancellation by the owner.
    UserRequested,
    /// Immediate-or-cancel or market remainder.
    UnfilledRemainder,
    /// Incoming quantity cancelled by self-trade prevention.
    SelfTradeAggressor,
    /// Resting quantity cancelled by self-trade prevention.
    SelfTradeResting,
    /// Resting quantity selected by an account-scoped mass cancel.
    MassCancel,
    /// Resting quantity removed by an atomic account admission control.
    AccountControl,
    /// Resting quantity removed by an instrument-wide state transition.
    TradingStateControl,
    /// Resting GTD quantity reached an explicit inclusive expiry watermark.
    Expired,
    /// A triggered FOK stop could not fill completely at activation.
    TriggeredFokUnfilled,
    /// A triggered post-only stop-limit would remove liquidity.
    TriggeredPostOnlyWouldCross,
    /// A triggered stop-limit residual could not enter its bounded price arena.
    TriggeredCapacityUnavailable,
}

/// Whether a filled order supplied or removed liquidity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LiquidityRole {
    /// The order was already resting.
    Maker,
    /// The order initiated the match.
    Taker,
}

/// A complete trade suitable for downstream clearing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Trade {
    /// Monotonic identifier within the book.
    pub trade_id: TradeId,
    /// Traded instrument.
    pub instrument_id: InstrumentId,
    /// Immutable instrument definition version used for execution.
    pub instrument_version: InstrumentVersion,
    /// Resting order's price.
    pub price: Price,
    /// Executed quantity.
    pub quantity: Quantity,
    /// Buy order identifier.
    pub buy_order_id: OrderId,
    /// Sell order identifier.
    pub sell_order_id: OrderId,
    /// Buy-side account.
    pub buyer_account_id: AccountId,
    /// Sell-side account.
    pub seller_account_id: AccountId,
    /// Resting order identifier.
    pub maker_order_id: OrderId,
    /// Incoming order identifier.
    pub taker_order_id: OrderId,
}

/// An externally observable matching-engine state transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EventKind {
    /// A new order passed entry validation.
    OrderAccepted {
        /// Accepted order.
        order_id: OrderId,
        /// Original quantity.
        quantity: Quantity,
        /// Accepted display policy.
        display: OrderDisplay,
    },
    /// An order or remainder entered the FIFO queue.
    OrderRested {
        /// Resting order.
        order_id: OrderId,
        /// Resting price.
        price: Price,
        /// Leaves quantity.
        leaves_quantity: Quantity,
        /// Quantity currently executable before a reserve refresh is required.
        working_quantity: Quantity,
    },
    /// A depleted reserve peak replenished at the displayed-class tail.
    OrderRefreshed {
        /// Persistent private order identifier.
        order_id: OrderId,
        /// Unchanged limit price.
        price: Price,
        /// New displayed peak, capped by total leaves.
        displayed_quantity: Quantity,
        /// Total leaves after the execution or prevention that caused refresh.
        leaves_quantity: Quantity,
    },
    /// An account-scoped mass cancel completed, including an empty selection.
    MassCancelCompleted {
        /// Account whose orders were selected.
        account_id: AccountId,
        /// Selection applied within the instrument shard.
        scope: MassCancelScope,
        /// Number of cancelled active orders.
        cancelled_order_count: u64,
        /// Sum of cancelled total leaves in lots, including hidden reserve leaves.
        cancelled_quantity_lots: u128,
    },
    /// A revision-checked account admission transition completed atomically.
    AccountControlApplied {
        /// Controlled account.
        account_id: AccountId,
        /// Effective state before this transition.
        previous_state: AccountAdmissionState,
        /// Effective state after this transition.
        current_state: AccountAdmissionState,
        /// Newly committed monotonically increasing account-control revision.
        revision: u64,
        /// Number of active orders atomically removed by the transition.
        cancelled_order_count: u64,
        /// Sum of removed total leaves in lots, including hidden reserve leaves.
        cancelled_quantity_lots: u128,
    },
    /// A revision-checked instrument-wide trading-state transition completed atomically.
    TradingStateControlApplied {
        /// Effective state before this transition.
        previous_state: TradingState,
        /// Effective state after this transition.
        current_state: TradingState,
        /// Newly committed monotonically increasing control revision.
        revision: u64,
        /// Number of active orders atomically removed by the transition.
        cancelled_order_count: u64,
        /// Sum of removed total leaves in lots, including hidden reserve leaves.
        cancelled_quantity_lots: u128,
    },
    /// An inclusive GTD-expiry watermark advance completed atomically.
    ExpirySweepCompleted {
        /// Previously committed inclusive watermark, absent before the first sweep.
        previous_watermark: Option<TimestampNs>,
        /// Newly committed inclusive watermark.
        current_watermark: TimestampNs,
        /// Number of resting or dormant GTD orders removed by this sweep.
        expired_order_count: u64,
        /// Sum of removed total leaves in lots, including hidden reserve leaves.
        expired_quantity_lots: u128,
    },
    /// A validated stop order entered the dormant trigger index.
    StopOrderArmed {
        /// Dormant order identifier.
        order_id: OrderId,
        /// Side-derived trigger threshold.
        trigger_price: Price,
        /// Constraint applied after triggering.
        activation: StopActivation,
        /// Monotonic matching-event sequence used as same-price trigger priority.
        priority_sequence: u64,
    },
    /// One dormant stop left the trigger index and became an incoming order.
    StopOrderTriggered {
        /// Activated order identifier.
        order_id: OrderId,
        /// Original trigger threshold.
        trigger_price: Price,
        /// Reference price that satisfied the trigger.
        reference_price: Price,
        /// Retained same-price trigger priority.
        priority_sequence: u64,
    },
    /// A bounded stop-reference advance completed atomically.
    StopTriggerSweepCompleted {
        /// Previously committed reference, absent before initialization.
        previous_reference_price: Option<Price>,
        /// Newly committed authoritative reference.
        current_reference_price: Price,
        /// Number of stops activated by this command.
        triggered_order_count: u64,
        /// Eligible stops retained for a same-reference continuation.
        remaining_eligible_order_count: u64,
    },
    /// A trade occurred.
    Trade(Trade),
    /// Quantity was cancelled.
    OrderCancelled {
        /// Affected order.
        order_id: OrderId,
        /// Cancelled quantity.
        quantity: Quantity,
        /// Cancellation cause.
        reason: CancelReason,
    },
    /// A resting limit or dormant stop-limit order was amended.
    OrderReplaced {
        /// Amended order.
        order_id: OrderId,
        /// Previous price.
        old_price: Price,
        /// New price.
        new_price: Price,
        /// Previous leaves quantity.
        old_quantity: Quantity,
        /// New requested leaves quantity.
        new_quantity: Quantity,
        /// Previous display policy.
        old_display: OrderDisplay,
        /// New display policy.
        new_display: OrderDisplay,
        /// Whether FIFO priority was retained.
        priority_retained: bool,
    },
    /// A potential self-trade was suppressed without an execution.
    SelfTradePrevented {
        /// Incoming order.
        aggressor_order_id: OrderId,
        /// Resting order.
        resting_order_id: OrderId,
        /// Quantity that did not trade.
        quantity: Quantity,
        /// Applied policy.
        policy: SelfTradePrevention,
    },
    /// The command failed validation and did not mutate book state.
    CommandRejected(RejectReason),
}

/// A sequenced event carrying deterministic trace context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Event {
    /// Strictly increasing sequence within the book.
    pub sequence: u64,
    /// Command responsible for the event.
    pub command_id: CommandId,
    /// Receive timestamp copied from the command.
    pub occurred_at: TimestampNs,
    /// State transition.
    pub kind: EventKind,
}

/// Constructor-owned append-only storage for committed matching events.
type EventArena = AppendOnlyTraceArena<Event>;

fn try_event_arena(maximum_events: usize) -> Result<Arc<EventArena>, MatchingError> {
    EventArena::try_with_capacity(maximum_events)
        .map_err(|_| MatchingError::CapacityReservationFailed(MatchingCapacity::RetainedEvents))
}

#[derive(Clone)]
enum EventTraceBacking {
    Arena(Arc<EventArena>),
    Owned(Arc<Vec<Event>>),
}

/// Immutable shared event sequence for one execution report.
///
/// Live matching reports reference a range of one constructor-owned append-only
/// arena, so trace creation and cloning allocate no event storage or control
/// block. Decoded and caller-constructed traces retain an owned fallback.
/// [`Self::make_mut`] detaches into owned copy-on-write storage for diagnostic
/// mutation without corrupting the idempotency cache.
#[derive(Clone)]
pub struct EventTrace {
    backing: EventTraceBacking,
    start: usize,
    length: usize,
}

impl EventTrace {
    /// Returns the number of events in this trace.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.length
    }

    /// Returns whether this trace contains no events.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Iterates over events in deterministic sequence order.
    #[must_use]
    pub fn iter(&self) -> EventTraceIter<'_> {
        EventTraceIter {
            trace: self,
            front: 0,
            back: self.length,
        }
    }

    /// Returns the event at `index`, if present.
    #[must_use]
    pub fn get(&self, index: usize) -> Option<&Event> {
        (index < self.length).then(|| self.event_at(index))
    }

    /// Returns the first event, if present.
    #[must_use]
    pub fn first(&self) -> Option<&Event> {
        self.get(0)
    }

    /// Returns the last event, if present.
    #[must_use]
    pub fn last(&self) -> Option<&Event> {
        self.length.checked_sub(1).and_then(|index| self.get(index))
    }

    /// Copies this trace into independent contiguous storage.
    #[must_use]
    pub fn to_vec(&self) -> Vec<Event> {
        self.iter().copied().collect()
    }

    fn event_at(&self, index: usize) -> &Event {
        assert!(index < self.length, "event-trace index out of bounds");
        let absolute = self
            .start
            .checked_add(index)
            .expect("published event index must be representable");
        match &self.backing {
            EventTraceBacking::Arena(arena) => arena.get(absolute),
            EventTraceBacking::Owned(events) => &events[absolute],
        }
    }

    /// Returns a mutable event slice, detaching from arena or shared storage.
    pub fn make_mut(&mut self) -> &mut [Event] {
        let detach = match &self.backing {
            EventTraceBacking::Arena(_) => true,
            EventTraceBacking::Owned(events) => self.start != 0 || self.length != events.len(),
        };
        if detach {
            let owned = self.to_vec();
            *self = owned.into();
        }
        let EventTraceBacking::Owned(events) = &mut self.backing else {
            unreachable!("detached trace must own its storage")
        };
        Arc::make_mut(events).as_mut_slice()
    }

    /// Returns whether two traces reference the identical immutable owner and buffer.
    #[must_use]
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        if self.start != other.start || self.length != other.length {
            return false;
        }
        match (&self.backing, &other.backing) {
            (EventTraceBacking::Arena(left), EventTraceBacking::Arena(right)) => {
                Arc::ptr_eq(left, right)
            }
            (EventTraceBacking::Owned(left), EventTraceBacking::Owned(right)) => {
                Arc::ptr_eq(left, right)
            }
            (EventTraceBacking::Arena(_), EventTraceBacking::Owned(_))
            | (EventTraceBacking::Owned(_), EventTraceBacking::Arena(_)) => false,
        }
    }

    fn from_arena(arena: Arc<EventArena>, start: usize, length: usize) -> Self {
        debug_assert!(
            start
                .checked_add(length)
                .is_some_and(|end| end <= arena.capacity())
        );
        Self {
            backing: EventTraceBacking::Arena(arena),
            start,
            length,
        }
    }

    fn arena_range_for(&self, arena: &Arc<EventArena>) -> Option<(usize, usize)> {
        match &self.backing {
            EventTraceBacking::Arena(current) if Arc::ptr_eq(current, arena) => {
                Some((self.start, self.length))
            }
            EventTraceBacking::Arena(_) | EventTraceBacking::Owned(_) => None,
        }
    }

    #[cfg(test)]
    fn rebind_arena(&self, source: &Arc<EventArena>, target: Arc<EventArena>) -> Self {
        let EventTraceBacking::Arena(current) = &self.backing else {
            panic!("live order-book reports must use arena storage")
        };
        assert!(Arc::ptr_eq(current, source));
        Self::from_arena(target, self.start, self.length)
    }
}

impl From<Vec<Event>> for EventTrace {
    fn from(events: Vec<Event>) -> Self {
        let length = events.len();
        Self {
            backing: EventTraceBacking::Owned(Arc::new(events)),
            start: 0,
            length,
        }
    }
}

impl fmt::Debug for EventTrace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for EventTrace {
    fn eq(&self, other: &Self) -> bool {
        self.length == other.length && self.iter().eq(other.iter())
    }
}

impl Eq for EventTrace {}

/// Immutable iterator over one possibly arena-backed event trace.
#[derive(Clone, Debug)]
pub struct EventTraceIter<'a> {
    trace: &'a EventTrace,
    front: usize,
    back: usize,
}

impl<'a> Iterator for EventTraceIter<'a> {
    type Item = &'a Event;

    fn next(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            return None;
        }
        let index = self.front;
        self.front += 1;
        Some(self.trace.event_at(index))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.back - self.front;
        (remaining, Some(remaining))
    }
}

impl DoubleEndedIterator for EventTraceIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            return None;
        }
        self.back -= 1;
        Some(self.trace.event_at(self.back))
    }
}

impl ExactSizeIterator for EventTraceIter<'_> {}
impl std::iter::FusedIterator for EventTraceIter<'_> {}

impl Index<usize> for EventTrace {
    type Output = Event;

    fn index(&self, index: usize) -> &Self::Output {
        self.event_at(index)
    }
}

#[derive(Debug)]
struct EventTraceBuilder {
    arena: Arc<EventArena>,
    start: usize,
    length: usize,
    maximum_events: usize,
}

impl EventTraceBuilder {
    fn new(arena: Arc<EventArena>, start: usize, maximum_events: usize) -> Self {
        assert!(
            start
                .checked_add(maximum_events)
                .is_some_and(|end| end <= arena.capacity()),
            "prepared report bound must fit retained-event arena"
        );
        Self {
            arena,
            start,
            length: 0,
            maximum_events,
        }
    }

    fn push(&mut self, event: Event) {
        assert!(
            self.length < self.maximum_events,
            "prepared event bound must cover the complete execution report"
        );
        let index = self
            .start
            .checked_add(self.length)
            .expect("prepared event index must be representable");
        self.arena.write_once(index, event);
        self.length += 1;
    }

    fn finish(self) -> EventTrace {
        EventTrace::from_arena(self.arena, self.start, self.length)
    }
}

#[cfg(test)]
mod event_trace_builder_tests {
    use std::sync::Arc;

    use super::{
        Event, EventKind, EventTraceBuilder, MatchingCapacity, MatchingError, RejectReason,
        try_event_arena,
    };
    use crate::{CommandId, TimestampNs};

    #[test]
    fn finalization_publishes_the_exact_preallocated_arena_range() {
        let command_id = CommandId::new(1).expect("non-zero command");
        let arena = try_event_arena(8).unwrap();
        let mut builder = EventTraceBuilder::new(Arc::clone(&arena), 3, 2);
        builder.push(Event {
            sequence: 1,
            command_id,
            occurred_at: TimestampNs::from_unix_nanos(1),
            kind: EventKind::CommandRejected(RejectReason::UnknownOrder),
        });
        let trace = builder.finish();
        assert_eq!(std::ptr::from_ref(&trace[0]), arena.slot_pointer(3));
        assert_eq!(trace.len(), 1);
        assert_eq!(trace[0].command_id, command_id);
    }

    #[test]
    fn unrepresentable_event_arena_is_a_typed_constructor_failure() {
        assert!(matches!(
            try_event_arena(usize::MAX),
            Err(MatchingError::CapacityReservationFailed(
                MatchingCapacity::RetainedEvents
            ))
        ));
    }
}

impl<'a> IntoIterator for &'a EventTrace {
    type Item = &'a Event;
    type IntoIter = EventTraceIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Overall disposition of a command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandOutcome {
    /// The command was accepted and may have produced one or more transitions.
    Accepted,
    /// The command was rejected without mutating matching state.
    Rejected(RejectReason),
}

/// All events and trace metadata produced by one command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionReport {
    /// Idempotency key of the command.
    pub command_id: CommandId,
    /// Accepted or rejected disposition.
    pub outcome: CommandOutcome,
    /// Ordered events caused by this command.
    pub events: EventTrace,
    /// True when this report came from the exact-command idempotency cache.
    pub replayed: bool,
}

/// Result of preparing one command against an unchanged semantic matching generation.
#[derive(Debug)]
pub enum CommandPreparation {
    /// The exact command is already committed and its cached trace is returned.
    Replay(ExecutionReport),
    /// Operational and core business preflight completed for one generation.
    Ready(PreparedCommand),
}

/// Opaque single-use command validated against one retained-history generation.
///
/// A prepared command contains no borrowed book state and can be written to a
/// WAL before commit. For mass cancellation, block-and-cancel, or
/// transition-and-cancel it holds one isolated constructor-owned order-selection
/// lease. Event storage belongs to the book and is not consumed until a
/// non-stale token commits. All matching hash-table and selection headroom was
/// reserved when the book was constructed, so preparation borrows the book
/// immutably and cannot allocate or change semantic book state; acquiring a
/// non-empty selection changes only process-local pool occupancy.
/// [`OrderBook::commit`] rejects it if an unrelated command changes the book
/// generation after preparation.
#[derive(Debug)]
pub struct PreparedCommand {
    command: Command,
    book_instance_id: u64,
    expected_retained_commands: usize,
    core_rejection: Option<RejectReason>,
    maximum_events: usize,
    selected_order_ids: Option<PooledOrderSelection>,
}

impl PreparedCommand {
    /// Returns the immutable command represented by this preparation.
    #[must_use]
    pub const fn command(&self) -> Command {
        self.command
    }

    /// Returns the core matching rejection already established by preparation.
    #[must_use]
    pub const fn core_rejection(&self) -> Option<RejectReason> {
        self.core_rejection
    }
}

#[derive(Debug)]
struct OrderSelectionPool {
    available: Mutex<Vec<Vec<OrderId>>>,
    capacity: usize,
    order_capacity: usize,
}

impl OrderSelectionPool {
    fn try_new(capacity: usize, maximum_orders: usize) -> Result<Arc<Self>, MatchingError> {
        let mut available = Vec::new();
        available.try_reserve_exact(capacity).map_err(|_| {
            MatchingError::CapacityReservationFailed(MatchingCapacity::PreparedOrderSelections)
        })?;
        for _ in 0..capacity {
            let mut selected = Vec::new();
            selected.try_reserve_exact(maximum_orders).map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::OrderSelectionBuffers)
            })?;
            available.push(selected);
        }
        let order_capacity = available.iter().map(Vec::capacity).min().unwrap_or(0);
        Ok(Arc::new(Self {
            available: Mutex::new(available),
            capacity,
            order_capacity,
        }))
    }

    fn lock_available(&self) -> MutexGuard<'_, Vec<Vec<OrderId>>> {
        self.available
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn acquire(self: &Arc<Self>) -> Option<PooledOrderSelection> {
        let selected = self.lock_available().pop()?;
        Some(PooledOrderSelection {
            pool: Some(Arc::clone(self)),
            selected: Some(selected),
        })
    }

    fn release(&self, mut selected: Vec<OrderId>) {
        selected.clear();
        let mut available = self.lock_available();
        assert!(
            available.len() < self.capacity,
            "matching order-selection buffer must be returned exactly once"
        );
        available.push(selected);
    }

    fn available(&self) -> usize {
        self.lock_available().len()
    }
}

struct PooledOrderSelection {
    pool: Option<Arc<OrderSelectionPool>>,
    selected: Option<Vec<OrderId>>,
}

impl PooledOrderSelection {
    fn empty() -> Self {
        Self {
            pool: None,
            selected: Some(Vec::new()),
        }
    }

    fn values(&self) -> &[OrderId] {
        self.selected
            .as_deref()
            .expect("live matching order-selection lease must contain its buffer")
    }

    fn values_mut(&mut self) -> &mut Vec<OrderId> {
        self.selected
            .as_mut()
            .expect("live matching order-selection lease must contain its buffer")
    }

    fn capacity(&self) -> usize {
        self.selected
            .as_ref()
            .expect("live matching order-selection lease must contain its buffer")
            .capacity()
    }
}

impl fmt::Debug for PooledOrderSelection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PooledOrderSelection")
            .field("length", &self.values().len())
            .field("capacity", &self.capacity())
            .finish()
    }
}

impl Drop for PooledOrderSelection {
    fn drop(&mut self) {
        if let Some(selected) = self.selected.take() {
            if let Some(pool) = &self.pool {
                pool.release(selected);
            } else {
                debug_assert!(selected.is_empty());
            }
        }
    }
}

/// A visible aggregate price level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LevelSnapshot {
    /// Level price.
    pub price: Price,
    /// Aggregate leaves quantity.
    pub quantity: u128,
    /// Number of resting orders.
    pub order_count: u64,
}

/// Process-local allocation state of one bounded price-level arena.
///
/// These counters are operational telemetry. They do not affect matching
/// semantics, equality, checkpoints, or WAL encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriceLevelArenaStatus {
    /// Configured semantic maximum occupied prices on this side.
    pub configured_slots: usize,
    /// Element capacity returned by the allocator during construction.
    pub allocated_slots: usize,
    /// Stable slots initialized at least once (occupied plus reusable).
    pub initialized_slots: usize,
    /// Currently occupied price slots.
    pub occupied_slots: usize,
    /// Initialized vacant slots linked into the reuse list.
    pub reusable_slots: usize,
}

/// Process-local allocation state of the append-only retained-event arena.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventArenaStatus {
    /// Configured total event-slot bound.
    pub configured_events: usize,
    /// Event-slot capacity returned by the allocator during construction.
    pub allocated_events: usize,
    /// Events published by committed reports.
    pub retained_events: usize,
}

/// Process-local allocation state of constructor-owned order-selection storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderSelectionPoolStatus {
    /// Configured simultaneous prepared selection leases.
    pub configured_leases: usize,
    /// Buffer sets not currently held by a preparation or commit.
    pub available_leases: usize,
    /// Minimum order-identifier capacity granted to every buffer set.
    pub allocated_order_ids_per_lease: usize,
}

/// One constructor-reserved matching hash index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchingHashIndex {
    /// Active order identifier to intrusive resting-order state.
    ActiveOrders,
    /// Account identifier to intrusive per-side membership state.
    ActiveAccounts,
    /// Never-reusable accepted order identifiers.
    AcceptedOrderIds,
    /// Command identifier to immutable idempotency history.
    CommandHistory,
    /// Account identifier to never-evicted revisioned admission-control state.
    AccountControls,
}

/// Process-local allocation state of one matching hash index.
///
/// These counters are operational telemetry. They do not affect matching
/// semantics, equality, checkpoints, or WAL encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HashIndexStatus {
    /// Configured maximum number of simultaneously retained entries.
    pub configured_entries: usize,
    /// Entry capacity currently available without growing or rehashing.
    pub allocated_entries: usize,
    /// Entries currently present in the index.
    pub occupied_entries: usize,
}

/// Private state of one resting order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderSnapshot {
    /// Order identifier.
    pub order_id: OrderId,
    /// Owner account.
    pub account_id: AccountId,
    /// Side.
    pub side: Side,
    /// Limit price.
    pub price: Price,
    /// Leaves quantity.
    pub leaves_quantity: Quantity,
    /// Quantity currently executable before a reserve refresh is required.
    pub working_quantity: Quantity,
    /// Persistent display policy.
    pub display: OrderDisplay,
    /// Absolute expiry for GTD orders; absent for GTC and post-only orders.
    pub expires_at: Option<TimestampNs>,
}

impl OrderSnapshot {
    /// Returns the quantity currently represented in public depth.
    ///
    /// Fully hidden orders return `None`; displayed and reserve orders return
    /// their current positive working quantity.
    #[must_use]
    pub const fn visible_quantity(self) -> Option<Quantity> {
        match self.display {
            OrderDisplay::FullyDisplayed | OrderDisplay::Reserve { .. } => {
                Some(self.working_quantity)
            }
            OrderDisplay::Hidden => None,
        }
    }
}

/// Private state of one accepted stop order that has not yet triggered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DormantStopSnapshot {
    /// Order identifier.
    pub order_id: OrderId,
    /// Owner account.
    pub account_id: AccountId,
    /// Buy or sell trigger direction.
    pub side: Side,
    /// Total dormant leaves quantity.
    pub leaves_quantity: Quantity,
    /// Persistent display policy applied if a stop-limit residual rests.
    pub display: OrderDisplay,
    /// Side-derived trigger threshold.
    pub trigger_price: Price,
    /// Constraint applied after activation.
    pub activation: StopActivation,
    /// Activation-time lifetime policy.
    pub time_in_force: TimeInForce,
    /// Self-trade prevention applied after activation.
    pub self_trade_prevention: SelfTradePrevention,
    /// Monotonic same-price trigger priority.
    pub priority_sequence: u64,
    /// Absolute GTD expiration, if present.
    pub expires_at: Option<TimestampNs>,
}

/// Complete private state of one resting order in a semantic checkpoint.
///
/// Entries are stored in canonical side/price order and FIFO order within a
/// price level. Intrusive links and level aggregates are reconstructed rather
/// than persisted redundantly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestingOrderCheckpoint {
    pub(crate) order_id: OrderId,
    pub(crate) account_id: AccountId,
    pub(crate) side: Side,
    pub(crate) price: Price,
    pub(crate) leaves: Quantity,
    pub(crate) working: Quantity,
    pub(crate) display: OrderDisplay,
    pub(crate) self_trade_prevention: SelfTradePrevention,
    pub(crate) expires_at: Option<TimestampNs>,
}

impl RestingOrderCheckpoint {
    /// Returns the private order identifier.
    #[must_use]
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    /// Returns the owning account.
    #[must_use]
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }

    /// Returns the resting side.
    #[must_use]
    pub const fn side(self) -> Side {
        self.side
    }

    /// Returns the resting price.
    #[must_use]
    pub const fn price(self) -> Price {
        self.price
    }

    /// Returns total leaves, including any non-public quantity.
    #[must_use]
    pub const fn leaves(self) -> Quantity {
        self.leaves
    }

    /// Returns the currently executable working slice.
    #[must_use]
    pub const fn working(self) -> Quantity {
        self.working
    }

    /// Returns the current public quantity, absent for fully hidden orders.
    #[must_use]
    pub const fn visible_quantity(self) -> Option<Quantity> {
        match self.display {
            OrderDisplay::FullyDisplayed | OrderDisplay::Reserve { .. } => Some(self.working),
            OrderDisplay::Hidden => None,
        }
    }

    /// Returns the persistent display policy.
    #[must_use]
    pub const fn display(self) -> OrderDisplay {
        self.display
    }

    /// Returns the resting order's self-trade prevention policy.
    #[must_use]
    pub const fn self_trade_prevention(self) -> SelfTradePrevention {
        self.self_trade_prevention
    }

    /// Returns the absolute GTD expiration, if present.
    #[must_use]
    pub const fn expires_at(self) -> Option<TimestampNs> {
        self.expires_at
    }
}

/// Complete private state of one dormant stop in a semantic checkpoint.
///
/// Rows are canonical: buy stops precede sell stops; buy triggers ascend,
/// sell triggers descend, and each equal trigger follows priority sequence
/// then order identifier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DormantStopCheckpoint {
    pub(crate) order_id: OrderId,
    pub(crate) account_id: AccountId,
    pub(crate) side: Side,
    pub(crate) leaves: Quantity,
    pub(crate) display: OrderDisplay,
    pub(crate) self_trade_prevention: SelfTradePrevention,
    pub(crate) expires_at: Option<TimestampNs>,
    pub(crate) trigger_price: Price,
    pub(crate) activation: StopActivation,
    pub(crate) time_in_force: TimeInForce,
    pub(crate) priority_sequence: u64,
}

impl DormantStopCheckpoint {
    /// Returns the private order identifier.
    #[must_use]
    pub const fn order_id(self) -> OrderId {
        self.order_id
    }

    /// Returns the owning account.
    #[must_use]
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }

    /// Returns the stop side.
    #[must_use]
    pub const fn side(self) -> Side {
        self.side
    }

    /// Returns total dormant leaves.
    #[must_use]
    pub const fn leaves(self) -> Quantity {
        self.leaves
    }

    /// Returns the activation-time display policy.
    #[must_use]
    pub const fn display(self) -> OrderDisplay {
        self.display
    }

    /// Returns the activation-time self-trade policy.
    #[must_use]
    pub const fn self_trade_prevention(self) -> SelfTradePrevention {
        self.self_trade_prevention
    }

    /// Returns the absolute GTD expiration, if present.
    #[must_use]
    pub const fn expires_at(self) -> Option<TimestampNs> {
        self.expires_at
    }

    /// Returns the side-derived trigger threshold.
    #[must_use]
    pub const fn trigger_price(self) -> Price {
        self.trigger_price
    }

    /// Returns the constraint applied after triggering.
    #[must_use]
    pub const fn activation(self) -> StopActivation {
        self.activation
    }

    /// Returns the activation-time lifetime policy.
    #[must_use]
    pub const fn time_in_force(self) -> TimeInForce {
        self.time_in_force
    }

    /// Returns same-trigger activation priority.
    #[must_use]
    pub const fn priority_sequence(self) -> u64 {
        self.priority_sequence
    }
}

/// One chronologically ordered idempotency entry in a matching checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandReportCheckpoint {
    pub(crate) command: Command,
    pub(crate) report: ExecutionReport,
}

impl CommandReportCheckpoint {
    /// Returns the original command.
    #[must_use]
    pub const fn command(&self) -> Command {
        self.command
    }

    /// Returns the original non-replayed execution report.
    #[must_use]
    pub fn report(&self) -> &ExecutionReport {
        &self.report
    }
}

/// Canonical direct state plus complete idempotency lineage for one order book.
///
/// Active-order and history vectors are immutable shared values, so cloning a
/// checkpoint is `O(1)` and allocates no row storage. Construction and decoding
/// still create one shared-owner control block per vector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBookCheckpoint {
    wal_metadata_sequence: u64,
    wal_sequence: u64,
    definition: InstrumentDefinition,
    orders: Arc<Vec<RestingOrderCheckpoint>>,
    dormant_stops: Arc<Vec<DormantStopCheckpoint>>,
    history: Arc<Vec<CommandReportCheckpoint>>,
}

#[derive(Debug)]
struct OrderBookCheckpointLineage {
    seen_order_ids: BoundedHashSet<OrderId>,
    account_controls: BoundedHashMap<AccountId, AccountControlSnapshot>,
    trading_state: TradingStateSnapshot,
    expiry_watermark: Option<TimestampNs>,
    stop_reference_price: Option<Price>,
    retained_event_count: usize,
    next_sequence: u64,
    next_trade_id: u64,
}

impl OrderBookCheckpoint {
    /// Returns the final immutable-metadata sequence immediately before command history.
    #[must_use]
    pub const fn wal_metadata_sequence(&self) -> u64 {
        self.wal_metadata_sequence
    }

    /// Returns the definition sequence for a standalone matching checkpoint.
    ///
    /// For a checkpoint embedded in [`crate::risk::RiskManagedCheckpoint`],
    /// use [`Self::wal_metadata_sequence`] because risk profiles follow the definition.
    #[must_use]
    pub const fn wal_first_sequence(&self) -> u64 {
        self.wal_metadata_sequence
    }

    /// Returns the completed execution-report WAL boundary represented here.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.wal_sequence
    }

    /// Returns the immutable instrument definition captured by the checkpoint.
    #[must_use]
    pub const fn definition(&self) -> InstrumentDefinition {
        self.definition
    }

    /// Returns the number of active private orders.
    #[must_use]
    pub fn active_order_count(&self) -> usize {
        self.orders.len().saturating_add(self.dormant_stops.len())
    }

    /// Returns the number of completed commands retained for exact retry.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.history.len()
    }

    /// Returns canonical private resting-order state.
    #[must_use]
    pub fn orders(&self) -> &[RestingOrderCheckpoint] {
        self.orders.as_slice()
    }

    /// Returns canonical private dormant-stop state.
    #[must_use]
    pub fn dormant_stops(&self) -> &[DormantStopCheckpoint] {
        self.dormant_stops.as_slice()
    }

    /// Returns chronological command/report idempotency state.
    #[must_use]
    pub fn history(&self) -> &[CommandReportCheckpoint] {
        self.history.as_slice()
    }

    /// Returns whether two checkpoints share both immutable order-state images.
    #[must_use]
    pub fn shares_order_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.orders, &other.orders)
            && Arc::ptr_eq(&self.dormant_stops, &other.dormant_stops)
    }

    /// Returns whether two checkpoints share the identical immutable history image.
    #[must_use]
    pub fn shares_history_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.history, &other.history)
    }

    pub(crate) fn from_parts(
        wal_metadata_sequence: u64,
        wal_sequence: u64,
        definition: InstrumentDefinition,
        orders: Vec<RestingOrderCheckpoint>,
        dormant_stops: Vec<DormantStopCheckpoint>,
        history: Vec<CommandReportCheckpoint>,
    ) -> Result<Self, OrderBookCheckpointError> {
        Self::from_parts_with_lineage(
            wal_metadata_sequence,
            wal_sequence,
            definition,
            orders,
            dormant_stops,
            history,
        )
        .map(|(checkpoint, _)| checkpoint)
    }

    fn from_parts_with_lineage(
        wal_metadata_sequence: u64,
        wal_sequence: u64,
        definition: InstrumentDefinition,
        orders: Vec<RestingOrderCheckpoint>,
        dormant_stops: Vec<DormantStopCheckpoint>,
        history: Vec<CommandReportCheckpoint>,
    ) -> Result<(Self, OrderBookCheckpointLineage), OrderBookCheckpointError> {
        let checkpoint = Self {
            wal_metadata_sequence,
            wal_sequence,
            definition,
            orders: Arc::new(orders),
            dormant_stops: Arc::new(dormant_stops),
            history: Arc::new(history),
        };
        let lineage = checkpoint.validate_lineage()?;
        Ok((checkpoint, lineage))
    }

    fn validate(&self) -> Result<(), OrderBookCheckpointError> {
        self.validate_lineage().map(drop)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one chronological audit keeps command, event, trade, identity, and control lineage coupled"
    )]
    fn validate_lineage(&self) -> Result<OrderBookCheckpointLineage, OrderBookCheckpointError> {
        if self.wal_metadata_sequence == 0 {
            return Err(OrderBookCheckpointError::new(
                "checkpoint WAL metadata sequence is zero",
            ));
        }
        let command_frames = u64::try_from(self.history.len())
            .map_err(|_| OrderBookCheckpointError::new("checkpoint command count exceeds u64"))?
            .checked_mul(2)
            .ok_or_else(|| OrderBookCheckpointError::new("checkpoint WAL boundary overflow"))?;
        let expected_boundary = self
            .wal_metadata_sequence
            .checked_add(command_frames)
            .ok_or_else(|| OrderBookCheckpointError::new("checkpoint WAL boundary overflow"))?;
        if self.wal_sequence != expected_boundary {
            return Err(OrderBookCheckpointError::new(
                "checkpoint WAL boundary does not terminate its command/report history",
            ));
        }

        let mut command_ids = reserve_checkpoint_set(
            self.history.len(),
            OrderBookCheckpointResource::HistoryCommandIdentifiers,
        )?;
        let mut seen_order_ids = reserve_checkpoint_set(
            self.history.len(),
            OrderBookCheckpointResource::HistoryAcceptedOrderIdentifiers,
        )?;
        let mut account_controls: BoundedHashMap<AccountId, AccountControlSnapshot> =
            reserve_checkpoint_map(
                self.history.len(),
                OrderBookCheckpointResource::HistoryAccountControls,
            )?;
        let mut dormant_lineage: BoundedHashMap<OrderId, DormantStopCheckpoint> =
            reserve_checkpoint_map(
                self.history.len(),
                OrderBookCheckpointResource::HistoryDormantStops,
            )?;
        let mut trading_state =
            TradingStateSnapshot::from_parts(self.definition.trading_state(), 0);
        let mut expiry_watermark = None;
        let mut stop_reference_price = None;
        let mut retained_event_count = 0_usize;
        let mut expected_event_sequence = 1_u64;
        let mut expected_trade_id = 1_u64;
        for entry in self.history.iter() {
            let command_id = entry.command.command_id();
            if !command_ids.insert(command_id) {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint contains a duplicate command identifier",
                ));
            }
            if entry.report.command_id != command_id || entry.report.replayed {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint command/report identity or replay state is invalid",
                ));
            }
            if entry.report.events.is_empty() {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint execution report has no event",
                ));
            }
            retained_event_count = retained_event_count
                .checked_add(entry.report.events.len())
                .ok_or_else(|| {
                    OrderBookCheckpointError::new(
                        "checkpoint retained event count is not representable",
                    )
                })?;
            for event in &entry.report.events {
                if event.sequence != expected_event_sequence
                    || event.command_id != command_id
                    || event.occurred_at != entry.command.received_at()
                {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint event trace is not globally contiguous and command-bound",
                    ));
                }
                expected_event_sequence =
                    expected_event_sequence.checked_add(1).ok_or_else(|| {
                        OrderBookCheckpointError::new("checkpoint event sequence is exhausted")
                    })?;
                if let EventKind::Trade(trade) = event.kind {
                    if trade.trade_id.get() != expected_trade_id
                        || trade.instrument_id != self.definition.instrument_id()
                        || trade.instrument_version != self.definition.version()
                    {
                        return Err(OrderBookCheckpointError::new(
                            "checkpoint trade lineage or instrument binding is invalid",
                        ));
                    }
                    expected_trade_id = expected_trade_id.checked_add(1).ok_or_else(|| {
                        OrderBookCheckpointError::new("checkpoint trade identifier is exhausted")
                    })?;
                }
            }
            if let (Command::New(order), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
            {
                if !seen_order_ids.insert(order.order_id) {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint accepts an order identifier more than once",
                    ));
                }
                if let Some((trigger_price, activation)) = order.order_type.stop() {
                    let reference = stop_reference_price.ok_or_else(|| {
                        OrderBookCheckpointError::new(
                            "checkpoint accepts a stop before reference initialization",
                        )
                    })?;
                    let trigger_is_satisfied = match order.side {
                        Side::Buy => reference >= trigger_price,
                        Side::Sell => reference <= trigger_price,
                    };
                    let (Some(accepted), Some(armed)) =
                        (entry.report.events.get(0), entry.report.events.get(1))
                    else {
                        return Err(OrderBookCheckpointError::new(
                            "checkpoint accepted stop trace is not arm-only",
                        ));
                    };
                    if entry.report.events.len() != 2
                        || trigger_is_satisfied
                        || !matches!(
                            accepted.kind,
                            EventKind::OrderAccepted {
                                order_id,
                                quantity,
                                display,
                            } if order_id == order.order_id
                                && quantity == order.quantity
                                && display == order.display
                        )
                        || !matches!(
                            armed.kind,
                            EventKind::StopOrderArmed {
                                order_id,
                                trigger_price: observed_trigger,
                                activation: observed_activation,
                                priority_sequence,
                            } if order_id == order.order_id
                                && observed_trigger == trigger_price
                                && observed_activation == activation
                                && priority_sequence == accepted.sequence
                        )
                    {
                        return Err(OrderBookCheckpointError::new(
                            "checkpoint accepted stop trace contradicts its command",
                        ));
                    }
                    let checkpoint = DormantStopCheckpoint {
                        order_id: order.order_id,
                        account_id: order.account_id,
                        side: order.side,
                        leaves: order.quantity,
                        display: order.display,
                        trigger_price,
                        activation,
                        time_in_force: order.time_in_force,
                        self_trade_prevention: order.self_trade_prevention,
                        priority_sequence: accepted.sequence,
                        expires_at: order.time_in_force.expires_at(),
                    };
                    validate_checkpoint_stop(self.definition, checkpoint)?;
                    dormant_lineage.insert(order.order_id, checkpoint);
                }
            }
            if let (Command::Replace(replace), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
                && let Some(previous) = dormant_lineage.get(&replace.order_id).copied()
            {
                let Some(event) = entry.report.events.first() else {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint dormant replacement trace has extra transitions",
                    ));
                };
                let StopActivation::Limit(old_price) = previous.activation else {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint replaces a dormant stop-market order",
                    ));
                };
                let priority_retained = old_price == replace.new_price
                    && replace.new_quantity.lots() <= previous.leaves.lots()
                    && replace.new_display == previous.display;
                if entry.report.events.len() != 1
                    || replace.account_id != previous.account_id
                    || !matches!(
                        event.kind,
                        EventKind::OrderReplaced {
                            order_id,
                            old_price: observed_old_price,
                            new_price,
                            old_quantity,
                            new_quantity,
                            old_display,
                            new_display,
                            priority_retained: observed_priority,
                        } if order_id == replace.order_id
                            && observed_old_price == old_price
                            && new_price == replace.new_price
                            && old_quantity == previous.leaves
                            && new_quantity == replace.new_quantity
                            && old_display == previous.display
                            && new_display == replace.new_display
                            && observed_priority == priority_retained
                    )
                {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint dormant replacement trace contradicts its command",
                    ));
                }
                dormant_lineage.insert(
                    replace.order_id,
                    DormantStopCheckpoint {
                        leaves: replace.new_quantity,
                        display: replace.new_display,
                        activation: StopActivation::Limit(replace.new_price),
                        priority_sequence: if priority_retained {
                            previous.priority_sequence
                        } else {
                            event.sequence
                        },
                        ..previous
                    },
                );
            }
            if let (Command::AccountControl(control), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
            {
                let previous = account_controls
                    .get(&control.account_id)
                    .copied()
                    .unwrap_or_default();
                if previous.revision != control.expected_revision {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint account-control revision chain is invalid",
                    ));
                }
                let revision = control.expected_revision.checked_add(1).ok_or_else(|| {
                    OrderBookCheckpointError::new(
                        "checkpoint account-control revision is exhausted",
                    )
                })?;
                let current_state = control.action.resulting_state();
                let mut cancelled_order_count = 0_u64;
                let mut cancelled_quantity_lots = 0_u128;
                for event in entry
                    .report
                    .events
                    .iter()
                    .take(entry.report.events.len() - 1)
                {
                    let EventKind::OrderCancelled {
                        quantity,
                        reason: CancelReason::AccountControl,
                        ..
                    } = event.kind
                    else {
                        return Err(OrderBookCheckpointError::new(
                            "checkpoint account-control trace contains an invalid transition",
                        ));
                    };
                    cancelled_order_count =
                        cancelled_order_count.checked_add(1).ok_or_else(|| {
                            OrderBookCheckpointError::new(
                                "checkpoint account-control cancellation count is exhausted",
                            )
                        })?;
                    cancelled_quantity_lots = cancelled_quantity_lots
                        .checked_add(u128::from(quantity.lots()))
                        .ok_or_else(|| {
                            OrderBookCheckpointError::new(
                                "checkpoint account-control cancellation quantity overflow",
                            )
                        })?;
                }
                let expected_cancel_count = if control.action == AccountControlAction::Enable {
                    0
                } else {
                    cancelled_order_count
                };
                if cancelled_order_count != expected_cancel_count
                    || !matches!(
                        entry.report.events.last().map(|event| event.kind),
                        Some(EventKind::AccountControlApplied {
                            account_id,
                            previous_state,
                            current_state: observed_state,
                            revision: observed_revision,
                            cancelled_order_count: observed_count,
                            cancelled_quantity_lots: observed_quantity,
                        }) if account_id == control.account_id
                            && previous_state == previous.state
                            && observed_state == current_state
                            && observed_revision == revision
                            && observed_count == cancelled_order_count
                            && observed_quantity == cancelled_quantity_lots
                    )
                {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint account-control completion event is invalid",
                    ));
                }
                account_controls.insert(
                    control.account_id,
                    AccountControlSnapshot {
                        state: current_state,
                        revision,
                    },
                );
            }
            if let (Command::TradingStateControl(control), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
            {
                if trading_state.revision != control.expected_revision
                    || trading_state.state == control.target_state
                    || (control.action == TradingStateControlAction::TransitionAndCancel
                        && control.target_state == TradingState::Open)
                {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint trading-state control chain is invalid",
                    ));
                }
                let revision = control.expected_revision.checked_add(1).ok_or_else(|| {
                    OrderBookCheckpointError::new(
                        "checkpoint trading-state control revision is exhausted",
                    )
                })?;
                let mut cancelled_order_count = 0_u64;
                let mut cancelled_quantity_lots = 0_u128;
                let mut previous_order_id = None;
                for event in entry
                    .report
                    .events
                    .iter()
                    .take(entry.report.events.len() - 1)
                {
                    let EventKind::OrderCancelled {
                        order_id,
                        quantity,
                        reason: CancelReason::TradingStateControl,
                    } = event.kind
                    else {
                        return Err(OrderBookCheckpointError::new(
                            "checkpoint trading-state trace contains an invalid transition",
                        ));
                    };
                    if previous_order_id.is_some_and(|previous| order_id <= previous) {
                        return Err(OrderBookCheckpointError::new(
                            "checkpoint trading-state cancellations are not canonical",
                        ));
                    }
                    previous_order_id = Some(order_id);
                    cancelled_order_count =
                        cancelled_order_count.checked_add(1).ok_or_else(|| {
                            OrderBookCheckpointError::new(
                                "checkpoint trading-state cancellation count is exhausted",
                            )
                        })?;
                    cancelled_quantity_lots = cancelled_quantity_lots
                        .checked_add(u128::from(quantity.lots()))
                        .ok_or_else(|| {
                            OrderBookCheckpointError::new(
                                "checkpoint trading-state cancellation quantity overflow",
                            )
                        })?;
                }
                if (control.action == TradingStateControlAction::Transition
                    && cancelled_order_count != 0)
                    || !matches!(
                        entry.report.events.last().map(|event| event.kind),
                        Some(EventKind::TradingStateControlApplied {
                            previous_state,
                            current_state,
                            revision: observed_revision,
                            cancelled_order_count: observed_count,
                            cancelled_quantity_lots: observed_quantity,
                        }) if previous_state == trading_state.state
                            && current_state == control.target_state
                            && observed_revision == revision
                            && observed_count == cancelled_order_count
                            && observed_quantity == cancelled_quantity_lots
                    )
                {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint trading-state completion event is invalid",
                    ));
                }
                trading_state = TradingStateSnapshot::from_parts(control.target_state, revision);
            }
            if let (Command::ExpirySweep(sweep), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
            {
                if sweep.through > sweep.received_at
                    || expiry_watermark.is_some_and(|watermark| sweep.through < watermark)
                {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint expiry-watermark chain is invalid",
                    ));
                }
                let mut expired_order_count = 0_u64;
                let mut expired_quantity_lots = 0_u128;
                for event in entry
                    .report
                    .events
                    .iter()
                    .take(entry.report.events.len() - 1)
                {
                    let EventKind::OrderCancelled {
                        quantity,
                        reason: CancelReason::Expired,
                        ..
                    } = event.kind
                    else {
                        return Err(OrderBookCheckpointError::new(
                            "checkpoint expiry trace contains an invalid transition",
                        ));
                    };
                    expired_order_count = expired_order_count.checked_add(1).ok_or_else(|| {
                        OrderBookCheckpointError::new(
                            "checkpoint expiry cancellation count is exhausted",
                        )
                    })?;
                    expired_quantity_lots = expired_quantity_lots
                        .checked_add(u128::from(quantity.lots()))
                        .ok_or_else(|| {
                            OrderBookCheckpointError::new(
                                "checkpoint expiry cancellation quantity overflow",
                            )
                        })?;
                }
                if !matches!(
                    entry.report.events.last().map(|event| event.kind),
                    Some(EventKind::ExpirySweepCompleted {
                        previous_watermark,
                        current_watermark,
                        expired_order_count: observed_count,
                        expired_quantity_lots: observed_quantity,
                    }) if previous_watermark == expiry_watermark
                        && current_watermark == sweep.through
                        && observed_count == expired_order_count
                        && observed_quantity == expired_quantity_lots
                ) {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint expiry completion event is invalid",
                    ));
                }
                expiry_watermark = Some(sweep.through);
            }
            if let (Command::StopTriggerSweep(sweep), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
            {
                if sweep.maximum_orders == 0 {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint accepts an empty stop-trigger batch",
                    ));
                }
                if let Some(current) = stop_reference_price
                    && current != sweep.reference_price
                    && eligible_checkpoint_stop_count(&dormant_lineage, current)? != 0
                {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint advances a stop reference with an eligible backlog",
                    ));
                }
                let initial_eligible =
                    eligible_checkpoint_stop_count(&dormant_lineage, sweep.reference_price)?;
                let mut triggered = 0_u64;
                for event in entry
                    .report
                    .events
                    .iter()
                    .take(entry.report.events.len() - 1)
                {
                    let EventKind::StopOrderTriggered {
                        order_id,
                        trigger_price,
                        reference_price,
                        priority_sequence,
                    } = event.kind
                    else {
                        continue;
                    };
                    let expected =
                        first_eligible_checkpoint_stop(&dormant_lineage, sweep.reference_price)?
                            .ok_or_else(|| {
                                OrderBookCheckpointError::new(
                                    "checkpoint triggers an absent or ineligible stop",
                                )
                            })?;
                    if expected.order_id != order_id
                        || expected.trigger_price != trigger_price
                        || expected.priority_sequence != priority_sequence
                        || reference_price != sweep.reference_price
                    {
                        return Err(OrderBookCheckpointError::new(
                            "checkpoint stop activation order is not canonical",
                        ));
                    }
                    dormant_lineage.remove(&order_id);
                    triggered = triggered.checked_add(1).ok_or_else(|| {
                        OrderBookCheckpointError::new("checkpoint stop-trigger count is exhausted")
                    })?;
                }
                let remaining =
                    eligible_checkpoint_stop_count(&dormant_lineage, sweep.reference_price)?;
                if triggered != initial_eligible.min(u64::from(sweep.maximum_orders))
                    || !matches!(
                        entry.report.events.last().map(|event| event.kind),
                        Some(EventKind::StopTriggerSweepCompleted {
                            previous_reference_price,
                            current_reference_price,
                            triggered_order_count,
                            remaining_eligible_order_count,
                        }) if previous_reference_price == stop_reference_price
                            && current_reference_price == sweep.reference_price
                            && triggered_order_count == triggered
                            && remaining_eligible_order_count == remaining
                    )
                {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint stop-reference lineage is invalid",
                    ));
                }
                stop_reference_price = Some(sweep.reference_price);
            }
            for event in &entry.report.events {
                let EventKind::OrderCancelled {
                    order_id,
                    quantity,
                    reason,
                } = event.kind
                else {
                    continue;
                };
                let Some(stop) = dormant_lineage.get(&order_id).copied() else {
                    continue;
                };
                if quantity != stop.leaves
                    || !matches!(
                        reason,
                        CancelReason::UserRequested
                            | CancelReason::MassCancel
                            | CancelReason::AccountControl
                            | CancelReason::TradingStateControl
                            | CancelReason::Expired
                    )
                {
                    return Err(OrderBookCheckpointError::new(
                        "checkpoint dormant-stop cancellation is invalid",
                    ));
                }
                dormant_lineage.remove(&order_id);
            }
        }

        let mut active_ids = reserve_checkpoint_set(
            self.active_order_count(),
            OrderBookCheckpointResource::ActiveOrderIdentifiers,
        )?;
        let mut previous_key = None;
        for order in self.orders.iter() {
            if !active_ids.insert(order.order_id) || !seen_order_ids.contains(&order.order_id) {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint active-order identity is duplicated or absent from accepted history",
                ));
            }
            let key = (
                side_wire_order(order.side),
                order.price,
                order.display.is_hidden(),
            );
            if previous_key.is_some_and(|previous| previous > key) {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint active orders are not in canonical side/price/FIFO order",
                ));
            }
            previous_key = Some(key);
            validate_checkpoint_order(self.definition, *order)?;
            if order
                .expires_at
                .is_some_and(|expires_at| expiry_watermark.is_some_and(|value| expires_at <= value))
            {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint retains an order at or before its expiry watermark",
                ));
            }
            if account_controls
                .get(&order.account_id)
                .is_some_and(|control| control.state == AccountAdmissionState::Blocked)
            {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint retains an order for a blocked account",
                ));
            }
        }
        let mut previous_stop_key = None;
        for order in self.dormant_stops.iter() {
            if !active_ids.insert(order.order_id) || !seen_order_ids.contains(&order.order_id) {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint dormant-stop identity is duplicated or absent from accepted history",
                ));
            }
            let key = dormant_checkpoint_sort_key(*order);
            if previous_stop_key.is_some_and(|previous| previous > key) {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint dormant stops are not in canonical trigger order",
                ));
            }
            previous_stop_key = Some(key);
            validate_checkpoint_stop(self.definition, *order)?;
            if order
                .expires_at
                .is_some_and(|expires_at| expiry_watermark.is_some_and(|value| expires_at <= value))
            {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint retains a dormant stop at or before its expiry watermark",
                ));
            }
            if account_controls
                .get(&order.account_id)
                .is_some_and(|control| control.state == AccountAdmissionState::Blocked)
            {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint retains a dormant stop for a blocked account",
                ));
            }
            if dormant_lineage.get(&order.order_id) != Some(order) {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint dormant-stop state diverges from command history",
                ));
            }
        }
        if dormant_lineage.len() != self.dormant_stops.len() {
            return Err(OrderBookCheckpointError::new(
                "checkpoint omits dormant-stop state reconstructed from history",
            ));
        }
        if !self.dormant_stops.is_empty() && stop_reference_price.is_none() {
            return Err(OrderBookCheckpointError::new(
                "checkpoint retains dormant stops without a reference price",
            ));
        }
        Ok(OrderBookCheckpointLineage {
            seen_order_ids,
            account_controls,
            trading_state,
            expiry_watermark,
            stop_reference_price,
            retained_event_count,
            next_sequence: expected_event_sequence,
            next_trade_id: expected_trade_id,
        })
    }

    pub(crate) fn is_successor_of(&self, previous: &Self) -> bool {
        self.wal_metadata_sequence == previous.wal_metadata_sequence
            && self.definition == previous.definition
            && self.wal_sequence >= previous.wal_sequence
            && self
                .history
                .as_slice()
                .starts_with(previous.history.as_slice())
    }
}

/// An immutable but not yet replay-verified continuous-order-book capture.
///
/// Capture performs live structural and lineage audits but deliberately defers
/// deterministic command replay. This type has no stable codec or snapshot
/// payload implementation; call [`Self::verify`] to obtain the only persistable
/// [`OrderBookCheckpoint`] form. Clones share all candidate row and event
/// storage and are therefore `O(1)`.
#[derive(Clone)]
pub struct OrderBookCheckpointCapture {
    checkpoint: OrderBookCheckpoint,
    limits: OrderBookLimits,
}

impl OrderBookCheckpointCapture {
    /// Returns the completed execution-report WAL boundary represented here.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.checkpoint.generation()
    }

    /// Returns the final immutable-metadata sequence before command history.
    #[must_use]
    pub const fn wal_metadata_sequence(&self) -> u64 {
        self.checkpoint.wal_metadata_sequence()
    }

    /// Returns the immutable captured instrument definition.
    #[must_use]
    pub const fn definition(&self) -> InstrumentDefinition {
        self.checkpoint.definition()
    }

    /// Returns the finite policy under which replay verification will run.
    #[must_use]
    pub const fn limits(&self) -> OrderBookLimits {
        self.limits
    }

    /// Returns the captured active-order cardinality without exposing rows.
    #[must_use]
    pub fn active_order_count(&self) -> usize {
        self.checkpoint.active_order_count()
    }

    /// Returns the captured command/report cardinality without exposing rows.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.checkpoint.command_count()
    }

    /// Returns whether two captures share every immutable candidate row image.
    #[must_use]
    pub fn shares_checkpoint_storage_with(&self, other: &Self) -> bool {
        self.checkpoint.shares_order_storage_with(&other.checkpoint)
            && self
                .checkpoint
                .shares_history_storage_with(&other.checkpoint)
    }

    /// Consumes the unverified capture and proves deterministic history replay.
    ///
    /// Verification constructs an isolated book under the captured finite
    /// policy, requires exact command/report reproduction, and compares a fresh
    /// canonical projection of the replayed state with the captured candidate.
    /// It may therefore run on another thread while the source writer advances.
    ///
    /// # Errors
    ///
    /// Returns [`OrderBookCheckpointError::BookConstructionFailed`] if the
    /// replay book cannot be reserved, a typed checkpoint resource error if the
    /// replay projection cannot be reserved, or `Invalid` for any semantic
    /// replay or direct-state divergence.
    pub fn verify(self) -> Result<OrderBookCheckpoint, OrderBookCheckpointError> {
        let Self { checkpoint, limits } = self;
        let mut replay = OrderBook::try_with_limits(checkpoint.definition, limits)
            .map_err(OrderBookCheckpointError::BookConstructionFailed)?;
        for entry in checkpoint.history.iter() {
            let reproduced = replay.submit(entry.command).map_err(|error| {
                OrderBookCheckpointError::new(format!(
                    "checkpoint history cannot be replayed: {error}"
                ))
            })?;
            if reproduced != entry.report {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint live state history diverges under deterministic replay",
                ));
            }
        }
        let replayed =
            replay.checkpoint_state(checkpoint.wal_metadata_sequence, checkpoint.wal_sequence)?;
        if replayed != checkpoint {
            return Err(OrderBookCheckpointError::new(
                "checkpoint direct state differs from deterministic history replay",
            ));
        }
        Ok(checkpoint)
    }
}

#[cfg(test)]
impl OrderBookCheckpointCapture {
    pub(crate) fn corrupt_generation_for_test(&mut self) {
        self.checkpoint.wal_sequence = self.checkpoint.wal_sequence.saturating_add(1);
    }
}

/// One fallibly reserved matching-checkpoint capture or validation resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderBookCheckpointResource {
    /// Chronological command/report rows owned by a newly captured checkpoint.
    CaptureHistory,
    /// Canonical resting-order rows owned by a newly captured checkpoint.
    CaptureActiveOrders,
    /// Canonical dormant-stop rows owned by a newly captured checkpoint.
    CaptureDormantStops,
    /// Unique command identifiers in retained history.
    HistoryCommandIdentifiers,
    /// Never-reused accepted order identifiers reconstructed from history.
    HistoryAcceptedOrderIdentifiers,
    /// Latest accepted account-control state reconstructed from history.
    HistoryAccountControls,
    /// Dormant-stop state reconstructed from chronological history.
    HistoryDormantStops,
    /// Unique active-order identities in the direct checkpoint image.
    ActiveOrderIdentifiers,
    /// Unique controlled accounts counted against restoration limits.
    CapacityControlledAccounts,
    /// Unique active accounts counted against restoration limits.
    CapacityActiveAccounts,
    /// Unique active bid prices counted against restoration limits.
    CapacityBidPrices,
    /// Unique active ask prices counted against restoration limits.
    CapacityAskPrices,
}

impl OrderBookCheckpointResource {
    const fn failure_detail(self) -> &'static str {
        match self {
            Self::CaptureHistory => "checkpoint history capture reservation failed",
            Self::CaptureActiveOrders => "checkpoint active-order capture reservation failed",
            Self::CaptureDormantStops => "checkpoint dormant-stop capture reservation failed",
            Self::HistoryCommandIdentifiers => {
                "checkpoint command-identifier scratch reservation failed"
            }
            Self::HistoryAcceptedOrderIdentifiers => {
                "checkpoint accepted-order-identifier scratch reservation failed"
            }
            Self::HistoryAccountControls => "checkpoint account-control scratch reservation failed",
            Self::HistoryDormantStops => "checkpoint dormant-stop lineage reservation failed",
            Self::ActiveOrderIdentifiers => {
                "checkpoint active-order-identifier scratch reservation failed"
            }
            Self::CapacityControlledAccounts => {
                "checkpoint controlled-account capacity scratch reservation failed"
            }
            Self::CapacityActiveAccounts => {
                "checkpoint active-account capacity scratch reservation failed"
            }
            Self::CapacityBidPrices => "checkpoint bid-price capacity scratch reservation failed",
            Self::CapacityAskPrices => "checkpoint ask-price capacity scratch reservation failed",
        }
    }
}

impl fmt::Display for OrderBookCheckpointResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CaptureHistory => "captured history rows",
            Self::CaptureActiveOrders => "captured active-order rows",
            Self::CaptureDormantStops => "captured dormant-stop rows",
            Self::HistoryCommandIdentifiers => "history command identifiers",
            Self::HistoryAcceptedOrderIdentifiers => "history accepted order identifiers",
            Self::HistoryAccountControls => "history account controls",
            Self::HistoryDormantStops => "history dormant stops",
            Self::ActiveOrderIdentifiers => "active order identifiers",
            Self::CapacityControlledAccounts => "capacity controlled accounts",
            Self::CapacityActiveAccounts => "capacity active accounts",
            Self::CapacityBidPrices => "capacity bid prices",
            Self::CapacityAskPrices => "capacity ask prices",
        })
    }
}

/// Semantic matching-checkpoint construction or restoration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OrderBookCheckpointError {
    /// Checkpoint content or selected-limit contradiction.
    Invalid(String),
    /// A temporary book required for validation or restoration could not be constructed.
    BookConstructionFailed(MatchingError),
    /// A complete capture or validation resource could not be represented or reserved.
    ResourceReservationFailed {
        /// Capture vector or validation index whose construction failed.
        resource: OrderBookCheckpointResource,
        /// Requested semantic maximum entries.
        maximum: usize,
    },
}

impl OrderBookCheckpointError {
    fn new(detail: impl Into<String>) -> Self {
        Self::Invalid(detail.into())
    }

    /// Returns a stable diagnostic description.
    #[must_use]
    pub fn detail(&self) -> &str {
        match self {
            Self::Invalid(detail) => detail,
            Self::BookConstructionFailed(_) => "matching checkpoint book construction failed",
            Self::ResourceReservationFailed { resource, .. } => resource.failure_detail(),
        }
    }

    /// Returns the failed capture or validation resource, if reservation failed.
    #[must_use]
    pub const fn resource(&self) -> Option<OrderBookCheckpointResource> {
        match self {
            Self::ResourceReservationFailed { resource, .. } => Some(*resource),
            Self::Invalid(_) | Self::BookConstructionFailed(_) => None,
        }
    }

    /// Returns the underlying temporary-book construction failure, if present.
    #[must_use]
    pub const fn construction_error(&self) -> Option<MatchingError> {
        match self {
            Self::BookConstructionFailed(error) => Some(*error),
            Self::Invalid(_) | Self::ResourceReservationFailed { .. } => None,
        }
    }

    /// Returns whether this failure is an explicitly typed reservation result.
    #[must_use]
    pub const fn is_resource_exhaustion(&self) -> bool {
        matches!(self, Self::ResourceReservationFailed { .. })
    }

    /// Returns whether failure occurred before semantic validation could mutate state.
    #[must_use]
    pub const fn is_operational_failure(&self) -> bool {
        matches!(
            self,
            Self::BookConstructionFailed(_) | Self::ResourceReservationFailed { .. }
        )
    }
}

impl fmt::Display for OrderBookCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(detail) => detail.fmt(formatter),
            Self::BookConstructionFailed(error) => {
                write!(
                    formatter,
                    "matching checkpoint book construction failed: {error}"
                )
            }
            Self::ResourceReservationFailed { resource, maximum } => write!(
                formatter,
                "failed to reserve matching-checkpoint {resource} through {maximum} entries"
            ),
        }
    }
}

impl std::error::Error for OrderBookCheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BookConstructionFailed(error) => Some(error),
            Self::Invalid(_) | Self::ResourceReservationFailed { .. } => None,
        }
    }
}

fn reserve_checkpoint_set<K>(
    maximum: usize,
    resource: OrderBookCheckpointResource,
) -> Result<BoundedHashSet<K>, OrderBookCheckpointError>
where
    K: Eq + Hash,
{
    BoundedHashSet::try_new(maximum)
        .map_err(|_| OrderBookCheckpointError::ResourceReservationFailed { resource, maximum })
}

fn reserve_checkpoint_map<K, V>(
    maximum: usize,
    resource: OrderBookCheckpointResource,
) -> Result<BoundedHashMap<K, V>, OrderBookCheckpointError>
where
    K: Eq + Hash,
{
    BoundedHashMap::try_new(maximum)
        .map_err(|_| OrderBookCheckpointError::ResourceReservationFailed { resource, maximum })
}

fn reserve_checkpoint_vec<T>(
    maximum: usize,
    resource: OrderBookCheckpointResource,
) -> Result<Vec<T>, OrderBookCheckpointError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(maximum)
        .map_err(|_| OrderBookCheckpointError::ResourceReservationFailed { resource, maximum })?;
    Ok(values)
}

#[cfg(test)]
mod checkpoint_resource_tests {
    use super::{
        AccountControlSnapshot, AccountId, CommandId, CommandReportCheckpoint,
        DormantStopCheckpoint, OrderBookCheckpointError, OrderBookCheckpointResource, OrderId,
        RestingOrderCheckpoint, reserve_checkpoint_map, reserve_checkpoint_set,
        reserve_checkpoint_vec,
    };

    #[test]
    fn unrepresentable_checkpoint_storage_is_typed_by_exact_resource() {
        for resource in [
            OrderBookCheckpointResource::HistoryCommandIdentifiers,
            OrderBookCheckpointResource::HistoryAcceptedOrderIdentifiers,
            OrderBookCheckpointResource::HistoryDormantStops,
            OrderBookCheckpointResource::ActiveOrderIdentifiers,
            OrderBookCheckpointResource::CapacityControlledAccounts,
            OrderBookCheckpointResource::CapacityActiveAccounts,
            OrderBookCheckpointResource::CapacityBidPrices,
            OrderBookCheckpointResource::CapacityAskPrices,
        ] {
            assert_eq!(
                reserve_checkpoint_set::<CommandId>(usize::MAX, resource).unwrap_err(),
                OrderBookCheckpointError::ResourceReservationFailed {
                    resource,
                    maximum: usize::MAX,
                }
            );
        }
        assert_eq!(
            reserve_checkpoint_map::<AccountId, AccountControlSnapshot>(
                usize::MAX,
                OrderBookCheckpointResource::HistoryAccountControls,
            )
            .unwrap_err(),
            OrderBookCheckpointError::ResourceReservationFailed {
                resource: OrderBookCheckpointResource::HistoryAccountControls,
                maximum: usize::MAX,
            }
        );
        assert_eq!(
            reserve_checkpoint_map::<OrderId, DormantStopCheckpoint>(
                usize::MAX,
                OrderBookCheckpointResource::HistoryDormantStops,
            )
            .unwrap_err(),
            OrderBookCheckpointError::ResourceReservationFailed {
                resource: OrderBookCheckpointResource::HistoryDormantStops,
                maximum: usize::MAX,
            }
        );
        for resource in [
            OrderBookCheckpointResource::CaptureHistory,
            OrderBookCheckpointResource::CaptureActiveOrders,
            OrderBookCheckpointResource::CaptureDormantStops,
        ] {
            let error = match resource {
                OrderBookCheckpointResource::CaptureHistory => {
                    reserve_checkpoint_vec::<CommandReportCheckpoint>(usize::MAX, resource)
                        .unwrap_err()
                }
                OrderBookCheckpointResource::CaptureActiveOrders => {
                    reserve_checkpoint_vec::<RestingOrderCheckpoint>(usize::MAX, resource)
                        .unwrap_err()
                }
                OrderBookCheckpointResource::CaptureDormantStops => {
                    reserve_checkpoint_vec::<DormantStopCheckpoint>(usize::MAX, resource)
                        .unwrap_err()
                }
                _ => unreachable!("capture resource set is exhaustive"),
            };
            assert_eq!(
                error,
                OrderBookCheckpointError::ResourceReservationFailed {
                    resource,
                    maximum: usize::MAX,
                }
            );
            assert!(error.is_resource_exhaustion());
            assert_eq!(error.resource(), Some(resource));
        }
    }
}

#[cfg(test)]
mod staged_checkpoint_capture_tests {
    use std::sync::Arc;

    use crate::domain::AssetId;
    use crate::instrument::{
        InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules, QuantityRules,
        ReserveOrderRules,
    };

    use super::{
        AccountId, InstrumentDefinition, InstrumentId, InstrumentVersion, OrderBook,
        OrderBookCheckpointError, OrderDisplay, OrderId, Price, Quantity, RestingOrderCheckpoint,
        SelfTradePrevention, Side, TimestampNs, TradingState,
    };

    fn definition() -> InstrumentDefinition {
        InstrumentDefinition::new(InstrumentSpec {
            instrument_id: InstrumentId::new(1).unwrap(),
            version: InstrumentVersion::new(1).unwrap(),
            effective_from: TimestampNs::from_unix_nanos(0),
            symbol: InstrumentSymbol::new("STAGED-CHECKPOINT").unwrap(),
            kind: InstrumentKind::Future,
            base_asset_id: AssetId::new(1).unwrap(),
            quote_asset_id: AssetId::new(2).unwrap(),
            price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
            quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
            reserve: ReserveOrderRules::new(100).unwrap(),
            hidden_orders_supported: false,
            base_units_per_lot: 1,
            quote_units_per_price_unit: 1,
            trading_state: TradingState::Open,
        })
        .unwrap()
    }

    #[test]
    fn staged_capture_rejects_live_lineage_and_post_capture_corruption() {
        let mut corrupted_live = OrderBook::new(definition());
        corrupted_live.next_sequence = 2;
        let Err(error) = corrupted_live.capture_checkpoint_candidate(1, 1) else {
            panic!("corrupt live lineage must not produce a capture");
        };
        assert_eq!(
            error,
            OrderBookCheckpointError::Invalid(
                "live next event sequence differs from command history".into()
            )
        );

        let book = OrderBook::new(definition());
        let mut capture = book.capture_checkpoint_candidate(1, 1).unwrap();
        capture.checkpoint.wal_sequence = 2;
        let error = capture.verify().unwrap_err();
        assert_eq!(
            error,
            OrderBookCheckpointError::Invalid(
                "checkpoint WAL boundary does not terminate its command/report history".into()
            )
        );

        let book = OrderBook::new(definition());
        let mut capture = book.capture_checkpoint_candidate(1, 1).unwrap();
        Arc::make_mut(&mut capture.checkpoint.orders).push(RestingOrderCheckpoint {
            order_id: OrderId::new(1).unwrap(),
            account_id: AccountId::new(1).unwrap(),
            side: Side::Buy,
            price: Price::from_raw(100),
            leaves: Quantity::new(1).unwrap(),
            working: Quantity::new(1).unwrap(),
            display: OrderDisplay::FullyDisplayed,
            self_trade_prevention: SelfTradePrevention::CancelAggressor,
            expires_at: None,
        });
        let error = capture.verify().unwrap_err();
        assert_eq!(
            error,
            OrderBookCheckpointError::Invalid(
                "checkpoint direct state differs from deterministic history replay".into()
            )
        );
    }
}

const fn side_wire_order(side: Side) -> u8 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn validate_checkpoint_order(
    definition: InstrumentDefinition,
    order: RestingOrderCheckpoint,
) -> Result<(), OrderBookCheckpointError> {
    definition
        .price_rules()
        .validate(order.price)
        .map_err(|_| OrderBookCheckpointError::new("checkpoint order price violates definition"))?;
    definition
        .quantity_rules()
        .validate_leaves(order.leaves)
        .map_err(|_| OrderBookCheckpointError::new("checkpoint order leaves violate definition"))?;
    if order.working.lots() > order.leaves.lots() {
        return Err(OrderBookCheckpointError::new(
            "checkpoint working quantity exceeds total leaves",
        ));
    }
    match order.display {
        OrderDisplay::FullyDisplayed if order.working != order.leaves => Err(
            OrderBookCheckpointError::new("checkpoint fully displayed order hides quantity"),
        ),
        OrderDisplay::Reserve { peak }
            if !definition.reserve_order_rules().enabled()
                || peak.lots() % definition.quantity_rules().increment_lots() != 0
                || order.working.lots() > peak.lots() =>
        {
            Err(OrderBookCheckpointError::new(
                "checkpoint reserve display violates definition or peak",
            ))
        }
        OrderDisplay::Hidden
            if !definition.hidden_orders_supported() || order.working != order.leaves =>
        {
            Err(OrderBookCheckpointError::new(
                "checkpoint hidden order violates definition or has partial working quantity",
            ))
        }
        _ => Ok(()),
    }
}

fn dormant_checkpoint_sort_key(order: DormantStopCheckpoint) -> (u8, i128, u64, OrderId) {
    let trigger = match order.side {
        Side::Buy => i128::from(order.trigger_price.raw()),
        Side::Sell => -i128::from(order.trigger_price.raw()),
    };
    (
        side_wire_order(order.side),
        trigger,
        order.priority_sequence,
        order.order_id,
    )
}

fn first_eligible_checkpoint_stop(
    stops: &BoundedHashMap<OrderId, DormantStopCheckpoint>,
    reference_price: Price,
) -> Result<Option<DormantStopCheckpoint>, OrderBookCheckpointError> {
    let mut buy = None;
    let mut sell = None;
    for stop in stops.values().copied() {
        let eligible = match stop.side {
            Side::Buy => stop.trigger_price <= reference_price,
            Side::Sell => reference_price <= stop.trigger_price,
        };
        if !eligible {
            continue;
        }
        let selected = match stop.side {
            Side::Buy => &mut buy,
            Side::Sell => &mut sell,
        };
        if selected.is_none_or(|current| {
            dormant_checkpoint_sort_key(stop) < dormant_checkpoint_sort_key(current)
        }) {
            *selected = Some(stop);
        }
    }
    match (buy, sell) {
        (Some(_), Some(_)) => Err(OrderBookCheckpointError::new(
            "checkpoint makes buy and sell stops simultaneously eligible",
        )),
        (Some(stop), None) | (None, Some(stop)) => Ok(Some(stop)),
        (None, None) => Ok(None),
    }
}

fn eligible_checkpoint_stop_count(
    stops: &BoundedHashMap<OrderId, DormantStopCheckpoint>,
    reference_price: Price,
) -> Result<u64, OrderBookCheckpointError> {
    let mut buys = 0_u64;
    let mut sells = 0_u64;
    for stop in stops.values() {
        let count = match stop.side {
            Side::Buy if stop.trigger_price <= reference_price => &mut buys,
            Side::Sell if reference_price <= stop.trigger_price => &mut sells,
            Side::Buy | Side::Sell => continue,
        };
        *count = count.checked_add(1).ok_or_else(|| {
            OrderBookCheckpointError::new("checkpoint eligible-stop count is exhausted")
        })?;
    }
    if buys != 0 && sells != 0 {
        return Err(OrderBookCheckpointError::new(
            "checkpoint makes buy and sell stops simultaneously eligible",
        ));
    }
    buys.checked_add(sells)
        .ok_or_else(|| OrderBookCheckpointError::new("checkpoint eligible-stop count is exhausted"))
}

fn validate_checkpoint_stop(
    definition: InstrumentDefinition,
    order: DormantStopCheckpoint,
) -> Result<(), OrderBookCheckpointError> {
    if order.priority_sequence == 0 {
        return Err(OrderBookCheckpointError::new(
            "checkpoint dormant stop has zero trigger priority",
        ));
    }
    let command = Command::New(NewOrder {
        command_id: CommandId::new(1).expect("one is non-zero"),
        order_id: order.order_id,
        account_id: order.account_id,
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        side: order.side,
        quantity: order.leaves,
        display: order.display,
        order_type: OrderType::Stop {
            trigger_price: order.trigger_price,
            activation: order.activation,
        },
        time_in_force: order.time_in_force,
        self_trade_prevention: order.self_trade_prevention,
        received_at: TimestampNs::from_unix_nanos(0),
    });
    definition.admit(command).map_err(|_| {
        OrderBookCheckpointError::new("checkpoint dormant stop violates instrument definition")
    })?;
    if order.activation == StopActivation::Market && order.time_in_force == TimeInForce::PostOnly {
        return Err(OrderBookCheckpointError::new(
            "checkpoint dormant stop-market is post-only",
        ));
    }
    if order.display.is_reserve()
        && !(matches!(order.activation, StopActivation::Limit(_)) && order.time_in_force.may_rest())
    {
        return Err(OrderBookCheckpointError::new(
            "checkpoint dormant stop has an invalid reserve qualifier",
        ));
    }
    if order.display.is_hidden()
        && !(matches!(order.activation, StopActivation::Limit(_)) && order.time_in_force.may_rest())
    {
        return Err(OrderBookCheckpointError::new(
            "checkpoint dormant stop has an invalid hidden qualifier",
        ));
    }
    Ok(())
}

fn validate_checkpoint_capacity(
    checkpoint: &OrderBookCheckpoint,
    limits: OrderBookLimits,
) -> Result<(), OrderBookCheckpointError> {
    if checkpoint.history.len() > limits.max_retained_commands() {
        return Err(OrderBookCheckpointError::new(
            "checkpoint command history exceeds selected capacity",
        ));
    }
    if checkpoint
        .history
        .iter()
        .any(|entry| entry.report.events.len() > limits.max_report_events())
    {
        return Err(OrderBookCheckpointError::new(
            "checkpoint report exceeds selected event capacity",
        ));
    }
    let retained_events = checkpoint.history.iter().try_fold(0_usize, |total, entry| {
        total.checked_add(entry.report.events.len())
    });
    if retained_events.is_none_or(|count| count > limits.max_retained_events()) {
        return Err(OrderBookCheckpointError::new(
            "checkpoint retained events exceed selected capacity",
        ));
    }
    let accepted_order_ids = checkpoint
        .history
        .iter()
        .filter(|entry| {
            matches!(
                (entry.command, entry.report.outcome),
                (Command::New(_), CommandOutcome::Accepted)
            )
        })
        .count();
    if accepted_order_ids > limits.max_accepted_order_ids() {
        return Err(OrderBookCheckpointError::new(
            "checkpoint accepted-order identifiers exceed selected capacity",
        ));
    }
    let mut controlled_accounts = reserve_checkpoint_set(
        checkpoint.history.len(),
        OrderBookCheckpointResource::CapacityControlledAccounts,
    )?;
    for entry in checkpoint.history.iter() {
        if let (Command::AccountControl(control), CommandOutcome::Accepted) =
            (entry.command, entry.report.outcome)
        {
            controlled_accounts.insert(control.account_id);
        }
    }
    if controlled_accounts.len() > limits.max_account_controls() {
        return Err(OrderBookCheckpointError::new(
            "checkpoint account-control capacity is insufficient",
        ));
    }
    if checkpoint.active_order_count() > limits.max_active_orders() {
        return Err(OrderBookCheckpointError::new(
            "checkpoint active-order capacity is insufficient",
        ));
    }
    let mut accounts = reserve_checkpoint_set(
        checkpoint.active_order_count(),
        OrderBookCheckpointResource::CapacityActiveAccounts,
    )?;
    let mut bids = reserve_checkpoint_set(
        checkpoint.orders.len(),
        OrderBookCheckpointResource::CapacityBidPrices,
    )?;
    let mut asks = reserve_checkpoint_set(
        checkpoint.orders.len(),
        OrderBookCheckpointResource::CapacityAskPrices,
    )?;
    for order in checkpoint.orders.iter() {
        accounts.insert(order.account_id);
        match order.side {
            Side::Buy => bids.insert(order.price),
            Side::Sell => asks.insert(order.price),
        };
    }
    for order in checkpoint.dormant_stops.iter() {
        accounts.insert(order.account_id);
    }
    if accounts.len() > limits.max_active_accounts() {
        return Err(OrderBookCheckpointError::new(
            "checkpoint active-account capacity is insufficient",
        ));
    }
    if bids.len() > limits.max_price_levels_per_side()
        || asks.len() > limits.max_price_levels_per_side()
    {
        return Err(OrderBookCheckpointError::new(
            "checkpoint price-level capacity is insufficient",
        ));
    }
    Ok(())
}

/// A bounded matching or coupled-risk resource that prevented command admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchingCapacity {
    /// Ordinary new/replace history reached the cancellation-reserve boundary.
    AdmissionCommandHistory,
    /// Ordinary command events reached the protected cancellation-event boundary.
    AdmissionEventHistory,
    /// Total retained command history, including the cancellation reserve.
    CommandHistory,
    /// Total retained events, including the protected cancellation reserve.
    RetainedEvents,
    /// Never-reusable accepted order identifiers.
    AcceptedOrderIds,
    /// Never-evicted account admission-control revisions.
    AccountControls,
    /// Simultaneously active resting or dormant orders.
    ActiveOrders,
    /// Ordered GTD-expiration index bounded by active orders.
    ExpiryIndex,
    /// Dormant buy-stop trigger index bounded by active orders.
    BuyStopIndex,
    /// Dormant sell-stop trigger index bounded by active orders.
    SellStopIndex,
    /// Accounts with at least one active resting or dormant order.
    ActiveAccounts,
    /// Occupied bid price levels.
    BidPriceLevels,
    /// Occupied ask price levels.
    AskPriceLevels,
    /// Events retained in one execution report.
    ReportEvents,
    /// Constructor-owned active-order identifier buffers used by selection commands.
    OrderSelectionBuffers,
    /// Fixed pool of simultaneously leased order-selection buffers.
    PreparedOrderSelections,
    /// Coupled pre-trade risk reservations for active orders.
    RiskReservations,
    /// Immutable account profiles registered in a coupled risk shard.
    RiskAccounts,
}

impl fmt::Display for MatchingCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AdmissionCommandHistory => "ordinary command history",
            Self::AdmissionEventHistory => "ordinary event history",
            Self::CommandHistory => "total command history",
            Self::RetainedEvents => "total retained events",
            Self::AcceptedOrderIds => "accepted order identifiers",
            Self::AccountControls => "account admission controls",
            Self::ActiveOrders => "active orders",
            Self::ExpiryIndex => "GTD expiry index",
            Self::BuyStopIndex => "buy-stop trigger index",
            Self::SellStopIndex => "sell-stop trigger index",
            Self::ActiveAccounts => "active accounts",
            Self::BidPriceLevels => "bid price levels",
            Self::AskPriceLevels => "ask price levels",
            Self::ReportEvents => "execution-report events",
            Self::OrderSelectionBuffers => "prepared order-selection buffers",
            Self::PreparedOrderSelections => "prepared order-selection pool",
            Self::RiskReservations => "risk reservations",
            Self::RiskAccounts => "registered risk accounts",
        })
    }
}

/// Non-business failure that prevents deterministic command processing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchingError {
    /// A command identifier was reused with different command content.
    CommandIdCollision(CommandId),
    /// A monotonic sequence exhausted its `u64` representation.
    SequenceExhausted,
    /// A monotonic trade identifier exhausted its `u64` representation.
    TradeIdExhausted,
    /// A configured bounded matching resource has no admissible capacity.
    CapacityExhausted(MatchingCapacity),
    /// A fallible configured-resource memory reservation failed.
    CapacityReservationFailed(MatchingCapacity),
    /// Every constructor-owned order-selection buffer is currently leased.
    PreparationCapacityExhausted {
        /// Configured simultaneous selection-lease maximum.
        maximum: usize,
    },
    /// The book generation changed after a command was prepared.
    StalePreparation,
    /// A prepared command belongs to another order-book instance.
    ForeignPreparation,
    /// Process-local order-book identity exhausted its `u64` representation.
    BookInstanceIdExhausted,
    /// Private matching state contradicted its constructor reservation or preflight.
    InternalInvariantViolation,
}

impl fmt::Display for MatchingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandIdCollision(id) => {
                write!(
                    formatter,
                    "command identifier {id} was reused with different content"
                )
            }
            Self::SequenceExhausted => formatter.write_str("event sequence exhausted"),
            Self::TradeIdExhausted => formatter.write_str("trade identifier exhausted"),
            Self::CapacityExhausted(resource) => {
                write!(formatter, "matching capacity exhausted: {resource}")
            }
            Self::CapacityReservationFailed(resource) => {
                write!(
                    formatter,
                    "matching capacity reservation failed: {resource}"
                )
            }
            Self::PreparationCapacityExhausted { maximum } => write!(
                formatter,
                "matching prepared order-selection capacity {maximum} is exhausted"
            ),
            Self::StalePreparation => formatter.write_str("prepared matching command is stale"),
            Self::ForeignPreparation => {
                formatter.write_str("prepared matching command belongs to another order book")
            }
            Self::BookInstanceIdExhausted => {
                formatter.write_str("order-book instance identifier exhausted")
            }
            Self::InternalInvariantViolation => {
                formatter.write_str("private matching state contradicts its preparation")
            }
        }
    }
}

impl std::error::Error for MatchingError {}

/// Raw construction values for a bounded [`OrderBook`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderBookLimitsSpec {
    /// Maximum simultaneous resting or dormant orders.
    pub max_active_orders: usize,
    /// Maximum accounts represented in the active-order index.
    pub max_active_accounts: usize,
    /// Maximum occupied prices independently on each side.
    pub max_price_levels_per_side: usize,
    /// Maximum never-reusable accepted order identifiers.
    pub max_accepted_order_ids: usize,
    /// Maximum accounts retaining revisioned administrative admission controls.
    pub max_account_controls: usize,
    /// Maximum retained command/report pairs.
    pub max_retained_commands: usize,
    /// Tail history slots reserved for business-valid cancellation-capable commands.
    pub cancellation_reserve: usize,
    /// Maximum events retained by one execution report.
    pub max_report_events: usize,
    /// Maximum events retained across all execution reports.
    pub max_retained_events: usize,
    /// Maximum simultaneous prepared commands retaining order-selection storage.
    pub max_prepared_order_selections: usize,
}

/// Invalid bounded order-book resource policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderBookLimitsError {
    /// Active-order maximum is zero.
    ZeroActiveOrders,
    /// Active-account maximum is zero.
    ZeroActiveAccounts,
    /// Per-side price-level maximum is zero.
    ZeroPriceLevels,
    /// Accepted-order-identifier maximum is zero.
    ZeroAcceptedOrderIds,
    /// Account-control maximum is zero.
    ZeroAccountControls,
    /// Retained-command maximum is zero.
    ZeroRetainedCommands,
    /// Per-report event maximum is zero.
    ZeroReportEvents,
    /// Total retained-event maximum is zero.
    ZeroRetainedEvents,
    /// Prepared order-selection lease maximum is zero.
    ZeroPreparedOrderSelections,
    /// More active accounts than active orders were configured.
    ActiveAccountsExceedActiveOrders,
    /// More price levels per side than total active orders were configured.
    PriceLevelsExceedActiveOrders,
    /// Active-order capacity exceeds accepted-identifier capacity.
    ActiveOrdersExceedAcceptedOrderIds,
    /// The reserve cannot individually cancel every maximally active order.
    CancellationReserveBelowActiveOrders,
    /// One maximum-size mass cancellation cannot fit in one report.
    ReportEventsBelowMassCancelMaximum,
    /// Cancellation reservation consumes the complete history capacity.
    NoOrdinaryCommandCapacity,
    /// Ordinary history cannot admit the configured maximum active population.
    ActiveOrdersExceedOrdinaryCommandCapacity,
    /// The protected cancellation-event reserve is not representable.
    RetainedEventReserveOverflow,
    /// The protected cancellation-event reserve consumes the complete arena.
    NoOrdinaryEventCapacity,
    /// One maximum-size report cannot fit outside the protected event reserve.
    ReportEventsExceedOrdinaryEventCapacity,
    /// Ordinary event capacity cannot admit the maximum active population.
    ActiveOrdersExceedOrdinaryEventCapacity,
}

impl fmt::Display for OrderBookLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ZeroActiveOrders => "active-order limit is zero",
            Self::ZeroActiveAccounts => "active-account limit is zero",
            Self::ZeroPriceLevels => "per-side price-level limit is zero",
            Self::ZeroAcceptedOrderIds => "accepted-order-identifier limit is zero",
            Self::ZeroAccountControls => "account-control limit is zero",
            Self::ZeroRetainedCommands => "retained-command limit is zero",
            Self::ZeroReportEvents => "execution-report event limit is zero",
            Self::ZeroRetainedEvents => "retained-event limit is zero",
            Self::ZeroPreparedOrderSelections => "prepared order-selection lease limit is zero",
            Self::ActiveAccountsExceedActiveOrders => {
                "active-account limit exceeds active-order limit"
            }
            Self::PriceLevelsExceedActiveOrders => {
                "per-side price-level limit exceeds active-order limit"
            }
            Self::ActiveOrdersExceedAcceptedOrderIds => {
                "active-order limit exceeds accepted-order-identifier limit"
            }
            Self::CancellationReserveBelowActiveOrders => {
                "cancellation reserve is smaller than active-order limit"
            }
            Self::ReportEventsBelowMassCancelMaximum => {
                "execution-report event limit cannot hold a maximum-size mass cancellation"
            }
            Self::NoOrdinaryCommandCapacity => {
                "cancellation reserve leaves no ordinary command capacity"
            }
            Self::ActiveOrdersExceedOrdinaryCommandCapacity => {
                "active-order limit exceeds ordinary command capacity"
            }
            Self::RetainedEventReserveOverflow => {
                "protected cancellation-event reserve is not representable"
            }
            Self::NoOrdinaryEventCapacity => {
                "protected cancellation-event reserve leaves no ordinary event capacity"
            }
            Self::ReportEventsExceedOrdinaryEventCapacity => {
                "per-report event limit exceeds ordinary event capacity"
            }
            Self::ActiveOrdersExceedOrdinaryEventCapacity => {
                "maximum active population cannot fit in ordinary event capacity"
            }
        })
    }
}

impl std::error::Error for OrderBookLimitsError {}

/// Validated finite resource policy for one matching shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderBookLimits {
    max_active_orders: usize,
    max_active_accounts: usize,
    max_price_levels_per_side: usize,
    max_accepted_order_ids: usize,
    max_account_controls: usize,
    max_retained_commands: usize,
    cancellation_reserve: usize,
    max_report_events: usize,
    max_retained_events: usize,
    max_prepared_order_selections: usize,
}

impl OrderBookLimits {
    /// Default maximum simultaneous resting or dormant orders.
    pub const DEFAULT_MAX_ACTIVE_ORDERS: usize = 4_096;
    /// Default maximum accounts with active resting or dormant orders.
    pub const DEFAULT_MAX_ACTIVE_ACCOUNTS: usize = 4_096;
    /// Default maximum occupied prices independently on each side.
    pub const DEFAULT_MAX_PRICE_LEVELS_PER_SIDE: usize = 4_096;
    /// Default maximum accepted, never-reusable order identifiers.
    pub const DEFAULT_MAX_ACCEPTED_ORDER_IDS: usize = 65_536;
    /// Default maximum accounts retaining administrative control revisions.
    pub const DEFAULT_MAX_ACCOUNT_CONTROLS: usize = 65_536;
    /// Default maximum retained command/report pairs.
    pub const DEFAULT_MAX_RETAINED_COMMANDS: usize = 65_536;
    /// Default tail slots reserved for cancellation controls.
    pub const DEFAULT_CANCELLATION_RESERVE: usize = 4_096;
    /// Default maximum events retained by one execution report.
    pub const DEFAULT_MAX_REPORT_EVENTS: usize = 65_536;
    /// Default maximum events retained across all reports.
    pub const DEFAULT_MAX_RETAINED_EVENTS: usize = 262_144;
    /// Default maximum simultaneous prepared order-selection commands.
    pub const DEFAULT_MAX_PREPARED_ORDER_SELECTIONS: usize = 2;

    /// Validates a finite matching-resource policy.
    ///
    /// # Errors
    ///
    /// Returns [`OrderBookLimitsError`] for a zero or internally incoherent bound.
    pub const fn new(spec: OrderBookLimitsSpec) -> Result<Self, OrderBookLimitsError> {
        if spec.max_active_orders == 0 {
            return Err(OrderBookLimitsError::ZeroActiveOrders);
        }
        if spec.max_active_accounts == 0 {
            return Err(OrderBookLimitsError::ZeroActiveAccounts);
        }
        if spec.max_price_levels_per_side == 0 {
            return Err(OrderBookLimitsError::ZeroPriceLevels);
        }
        if spec.max_accepted_order_ids == 0 {
            return Err(OrderBookLimitsError::ZeroAcceptedOrderIds);
        }
        if spec.max_account_controls == 0 {
            return Err(OrderBookLimitsError::ZeroAccountControls);
        }
        if spec.max_retained_commands == 0 {
            return Err(OrderBookLimitsError::ZeroRetainedCommands);
        }
        if spec.max_report_events == 0 {
            return Err(OrderBookLimitsError::ZeroReportEvents);
        }
        if spec.max_retained_events == 0 {
            return Err(OrderBookLimitsError::ZeroRetainedEvents);
        }
        if spec.max_prepared_order_selections == 0 {
            return Err(OrderBookLimitsError::ZeroPreparedOrderSelections);
        }
        if spec.max_active_accounts > spec.max_active_orders {
            return Err(OrderBookLimitsError::ActiveAccountsExceedActiveOrders);
        }
        if spec.max_price_levels_per_side > spec.max_active_orders {
            return Err(OrderBookLimitsError::PriceLevelsExceedActiveOrders);
        }
        if spec.max_active_orders > spec.max_accepted_order_ids {
            return Err(OrderBookLimitsError::ActiveOrdersExceedAcceptedOrderIds);
        }
        if spec.cancellation_reserve < spec.max_active_orders {
            return Err(OrderBookLimitsError::CancellationReserveBelowActiveOrders);
        }
        let Some(maximum_mass_cancel_events) = spec.max_active_orders.checked_add(1) else {
            return Err(OrderBookLimitsError::RetainedEventReserveOverflow);
        };
        if spec.max_report_events < maximum_mass_cancel_events {
            return Err(OrderBookLimitsError::ReportEventsBelowMassCancelMaximum);
        }
        if spec.cancellation_reserve >= spec.max_retained_commands {
            return Err(OrderBookLimitsError::NoOrdinaryCommandCapacity);
        }
        if spec.max_active_orders > spec.max_retained_commands - spec.cancellation_reserve {
            return Err(OrderBookLimitsError::ActiveOrdersExceedOrdinaryCommandCapacity);
        }
        let cancellation_event_reserve = maximum_mass_cancel_events;
        if cancellation_event_reserve >= spec.max_retained_events {
            return Err(OrderBookLimitsError::NoOrdinaryEventCapacity);
        }
        let ordinary_event_capacity = spec.max_retained_events - cancellation_event_reserve;
        if spec.max_report_events > ordinary_event_capacity {
            return Err(OrderBookLimitsError::ReportEventsExceedOrdinaryEventCapacity);
        }
        let Some(active_population_events) = spec.max_active_orders.checked_mul(2) else {
            return Err(OrderBookLimitsError::ActiveOrdersExceedOrdinaryEventCapacity);
        };
        if active_population_events > ordinary_event_capacity {
            return Err(OrderBookLimitsError::ActiveOrdersExceedOrdinaryEventCapacity);
        }
        Ok(Self {
            max_active_orders: spec.max_active_orders,
            max_active_accounts: spec.max_active_accounts,
            max_price_levels_per_side: spec.max_price_levels_per_side,
            max_accepted_order_ids: spec.max_accepted_order_ids,
            max_account_controls: spec.max_account_controls,
            max_retained_commands: spec.max_retained_commands,
            cancellation_reserve: spec.cancellation_reserve,
            max_report_events: spec.max_report_events,
            max_retained_events: spec.max_retained_events,
            max_prepared_order_selections: spec.max_prepared_order_selections,
        })
    }

    /// Maximum simultaneous resting or dormant orders.
    #[must_use]
    pub const fn max_active_orders(self) -> usize {
        self.max_active_orders
    }

    /// Maximum accounts with active resting or dormant orders.
    #[must_use]
    pub const fn max_active_accounts(self) -> usize {
        self.max_active_accounts
    }

    /// Maximum occupied prices independently on each side.
    #[must_use]
    pub const fn max_price_levels_per_side(self) -> usize {
        self.max_price_levels_per_side
    }

    /// Maximum accepted, never-reusable order identifiers.
    #[must_use]
    pub const fn max_accepted_order_ids(self) -> usize {
        self.max_accepted_order_ids
    }

    /// Maximum accounts retaining revisioned administrative controls.
    #[must_use]
    pub const fn max_account_controls(self) -> usize {
        self.max_account_controls
    }

    /// Maximum retained command/report pairs including cancellation reserve.
    #[must_use]
    pub const fn max_retained_commands(self) -> usize {
        self.max_retained_commands
    }

    /// Slots reserved for cancel and mass-cancel commands.
    #[must_use]
    pub const fn cancellation_reserve(self) -> usize {
        self.cancellation_reserve
    }

    /// Maximum events retained by one execution report.
    #[must_use]
    pub const fn max_report_events(self) -> usize {
        self.max_report_events
    }

    /// Maximum events retained across all execution reports.
    #[must_use]
    pub const fn max_retained_events(self) -> usize {
        self.max_retained_events
    }

    /// Maximum simultaneous prepared commands retaining order-selection storage.
    #[must_use]
    pub const fn max_prepared_order_selections(self) -> usize {
        self.max_prepared_order_selections
    }

    /// Event slots protected for business-valid cancellation-capable commands.
    #[must_use]
    pub const fn cancellation_event_reserve(self) -> usize {
        self.max_active_orders + 1
    }

    /// Maximum ordinary retained events before the cancellation lane begins.
    #[must_use]
    pub const fn ordinary_event_capacity(self) -> usize {
        self.max_retained_events - self.cancellation_event_reserve()
    }

    /// Maximum new/replace command history before the cancellation lane begins.
    #[must_use]
    pub const fn ordinary_command_capacity(self) -> usize {
        self.max_retained_commands - self.cancellation_reserve
    }
}

impl Default for OrderBookLimits {
    fn default() -> Self {
        Self::new(OrderBookLimitsSpec {
            max_active_orders: Self::DEFAULT_MAX_ACTIVE_ORDERS,
            max_active_accounts: Self::DEFAULT_MAX_ACTIVE_ACCOUNTS,
            max_price_levels_per_side: Self::DEFAULT_MAX_PRICE_LEVELS_PER_SIDE,
            max_accepted_order_ids: Self::DEFAULT_MAX_ACCEPTED_ORDER_IDS,
            max_account_controls: Self::DEFAULT_MAX_ACCOUNT_CONTROLS,
            max_retained_commands: Self::DEFAULT_MAX_RETAINED_COMMANDS,
            cancellation_reserve: Self::DEFAULT_CANCELLATION_RESERVE,
            max_report_events: Self::DEFAULT_MAX_REPORT_EVENTS,
            max_retained_events: Self::DEFAULT_MAX_RETAINED_EVENTS,
            max_prepared_order_selections: Self::DEFAULT_MAX_PREPARED_ORDER_SELECTIONS,
        })
        .expect("built-in order-book limits are valid")
    }
}

/// Structural corruption detected by an offline order-book audit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvariantViolation {
    detail: String,
}

impl InvariantViolation {
    fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }

    /// Returns a stable human-readable description of the failed invariant.
    #[must_use]
    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(formatter)
    }
}

impl std::error::Error for InvariantViolation {}

#[derive(Clone, Copy, Debug)]
struct RestingOrder {
    order_id: OrderId,
    account_id: AccountId,
    side: Side,
    price: Price,
    leaves: u64,
    displayed: u64,
    display: OrderDisplay,
    self_trade_prevention: SelfTradePrevention,
    expires_at: Option<TimestampNs>,
    stop: Option<DormantStopState>,
    previous: Option<OrderId>,
    next: Option<OrderId>,
    account_previous: Option<OrderId>,
    account_next: Option<OrderId>,
}

impl PartialEq for RestingOrder {
    fn eq(&self, other: &Self) -> bool {
        self.order_id == other.order_id
            && self.account_id == other.account_id
            && self.side == other.side
            && self.price == other.price
            && self.leaves == other.leaves
            && self.displayed == other.displayed
            && self.display == other.display
            && self.self_trade_prevention == other.self_trade_prevention
            && self.expires_at == other.expires_at
            && self.stop == other.stop
            && self.previous == other.previous
            && self.next == other.next
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ExpiryKey {
    expires_at: TimestampNs,
    order_id: OrderId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DormantStopState {
    trigger_price: Price,
    activation: StopActivation,
    time_in_force: TimeInForce,
    priority_sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct BuyStopKey {
    trigger_price: Price,
    priority_sequence: u64,
    order_id: OrderId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SellStopKey {
    trigger_price: Reverse<Price>,
    priority_sequence: u64,
    order_id: OrderId,
}

impl Eq for RestingOrder {}

impl RestingOrder {
    fn remaining_event_units(self) -> u128 {
        if self.stop.is_some() {
            return 0;
        }
        let slices = match self.display {
            OrderDisplay::FullyDisplayed | OrderDisplay::Hidden => 1,
            OrderDisplay::Reserve { peak } => {
                let hidden = u128::from(self.leaves - self.displayed);
                let peak = u128::from(peak.lots());
                1 + hidden.div_ceil(peak)
            }
        };
        slices
            .checked_mul(2)
            .and_then(|value| value.checked_sub(1))
            .expect("u64 order quantity has representable event work")
    }

    fn is_dormant_stop(self) -> bool {
        self.stop.is_some()
    }

    fn stop_snapshot(self) -> Option<DormantStopSnapshot> {
        let stop = self.stop?;
        Some(DormantStopSnapshot {
            order_id: self.order_id,
            account_id: self.account_id,
            side: self.side,
            leaves_quantity: Quantity::new(self.leaves)
                .expect("dormant stop leaves must be non-zero"),
            display: self.display,
            trigger_price: stop.trigger_price,
            activation: stop.activation,
            time_in_force: stop.time_in_force,
            self_trade_prevention: self.self_trade_prevention,
            priority_sequence: stop.priority_sequence,
            expires_at: self.expires_at,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PriceLevel {
    head: OrderId,
    tail: OrderId,
    displayed_tail: Option<OrderId>,
    total_quantity: u128,
    order_count: u64,
    visible_order_count: u64,
    event_units: u128,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PriceLevelHandle {
    price: Price,
    slot: IndexedAvlHandle,
}

/// Bounded execution and public prices for one side with mutation-maintained
/// market extrema and a key-checked execution-level stable-slot handle.
///
/// The execution-price AVL is the authoritative level enumeration and the
/// public-price AVL indexes its non-zero visible subset. Both cached extrema
/// are redundant derived state, updated by every applicable mutation and
/// independently checked by `validate_extremum`. Read-only best-price
/// discovery and handle-addressed maker-level mutation therefore do not
/// traverse either tree, and level mutation performs no allocation.
#[cfg_attr(test, derive(Clone))]
#[derive(Debug)]
struct PriceLevels {
    side: Side,
    by_price: IndexedAvlMap<Price, PriceLevel>,
    visible_by_price: IndexedAvlMap<Price, ()>,
    best: Option<(Price, PriceLevel)>,
    best_handle: Option<PriceLevelHandle>,
    best_visible: Option<(Price, PriceLevel)>,
    event_units: u128,
}

impl PartialEq for PriceLevels {
    fn eq(&self, other: &Self) -> bool {
        self.side == other.side
            && self.by_price == other.by_price
            && self.visible_by_price == other.visible_by_price
            && self.best == other.best
            && self.best_visible == other.best_visible
            && self.event_units == other.event_units
    }
}

impl Eq for PriceLevels {}

impl PriceLevels {
    fn try_new(
        side: Side,
        maximum_levels: usize,
    ) -> Result<Self, std::collections::TryReserveError> {
        Ok(Self {
            side,
            by_price: IndexedAvlMap::try_with_capacity(maximum_levels)?,
            visible_by_price: IndexedAvlMap::try_with_capacity(maximum_levels)?,
            best: None,
            best_handle: None,
            best_visible: None,
            event_units: 0,
        })
    }

    fn get(&self, price: Price) -> Option<&PriceLevel> {
        self.by_price.get(&price)
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&Price, &PriceLevel)> + ExactSizeIterator {
        self.by_price.iter()
    }

    fn values(&self) -> impl Iterator<Item = &PriceLevel> {
        self.by_price.iter().map(|(_, level)| level)
    }

    fn len(&self) -> usize {
        self.by_price.len()
    }

    fn allocation_capacity(&self) -> usize {
        self.by_price.allocation_capacity()
    }

    fn storage_len(&self) -> usize {
        self.by_price.storage_len()
    }

    fn event_units(&self) -> u128 {
        self.event_units
    }

    fn best_price(&self) -> Option<Price> {
        self.best.map(|(price, _)| price)
    }

    #[cfg(test)]
    fn best_level(&self) -> Option<(Price, PriceLevel)> {
        self.best
    }

    fn best_visible_level(&self) -> Option<(Price, PriceLevel)> {
        self.best_visible
    }

    fn best_level_with_handle(&self) -> Option<(PriceLevelHandle, PriceLevel)> {
        let handle = self.best_handle?;
        let (price, level) = self.best?;
        debug_assert_eq!(handle.price, price);
        Some((handle, level))
    }

    fn handle(&self, price: Price) -> Option<PriceLevelHandle> {
        self.by_price
            .get_handle(&price)
            .map(|slot| PriceLevelHandle { price, slot })
    }

    fn get_by_handle(&self, handle: PriceLevelHandle) -> Option<&PriceLevel> {
        self.by_price.get_by_handle(handle.slot, &handle.price)
    }

    fn insert(&mut self, price: Price, level: PriceLevel) -> Option<PriceLevel> {
        let (slot, replaced) = self.by_price.insert_with_handle(price, level);
        self.event_units = self
            .event_units
            .checked_sub(replaced.map_or(0, |value| value.event_units))
            .and_then(|value| value.checked_add(level.event_units))
            .expect("active-order event work must fit u128");
        if self
            .best
            .is_none_or(|(current, _)| current == price || self.is_better(price, current))
        {
            self.best = Some((price, level));
            self.best_handle = Some(PriceLevelHandle { price, slot });
        }
        self.sync_visible_level(price, replaced, Some(level));
        replaced
    }

    fn update<R>(&mut self, price: Price, update: impl FnOnce(&mut PriceLevel) -> R) -> Option<R> {
        let handle = self.handle(price)?;
        self.update_by_handle(handle, update)
    }

    fn update_by_handle<R>(
        &mut self,
        handle: PriceLevelHandle,
        update: impl FnOnce(&mut PriceLevel) -> R,
    ) -> Option<R> {
        let (result, snapshot, previous) = {
            let level = self
                .by_price
                .get_mut_by_handle(handle.slot, &handle.price)?;
            let previous = *level;
            let result = update(level);
            (result, *level, previous)
        };
        self.event_units = self
            .event_units
            .checked_sub(previous.event_units)
            .and_then(|value| value.checked_add(snapshot.event_units))
            .expect("active-order event work must fit u128");
        if self.best_handle == Some(handle) {
            self.best = Some((handle.price, snapshot));
        }
        self.sync_visible_level(handle.price, Some(previous), Some(snapshot));
        Some(result)
    }

    #[cfg(test)]
    fn remove(&mut self, price: Price) -> Option<PriceLevel> {
        let handle = self.handle(price)?;
        self.remove_by_handle(handle)
    }

    fn remove_by_handle(&mut self, handle: PriceLevelHandle) -> Option<PriceLevel> {
        let removed = self.by_price.remove_by_handle(handle.slot, &handle.price);
        if let Some(level) = removed {
            self.event_units = self
                .event_units
                .checked_sub(level.event_units)
                .expect("removed level event work must be indexed");
        }
        if self.best_handle == Some(handle) {
            let extremum = self.map_extremum();
            self.best = extremum.map(|(_, price, level)| (price, level));
            self.best_handle = extremum.map(|(handle, _, _)| handle);
        }
        if let Some(level) = removed {
            self.sync_visible_level(handle.price, Some(level), None);
        }
        removed
    }

    fn next_worse(&self, current: Price) -> Option<Price> {
        match self.side {
            Side::Buy => self.by_price.predecessor_key(&current).copied(),
            Side::Sell => self.by_price.successor_key(&current).copied(),
        }
    }

    fn validate_extremum(&self) -> Result<(), InvariantViolation> {
        if self.visible_by_price.maximum() != self.by_price.maximum()
            || self.visible_by_price.allocation_capacity() < self.by_price.maximum()
        {
            return Err(InvariantViolation::new(format!(
                "{:?} public-price index reservation differs from its price-level arena",
                self.side
            )));
        }
        self.by_price.validate().map_err(|detail| {
            InvariantViolation::new(format!("{:?} price index: {detail}", self.side))
        })?;
        self.visible_by_price.validate().map_err(|detail| {
            InvariantViolation::new(format!("{:?} public-price index: {detail}", self.side))
        })?;
        let actual = self.map_extremum();
        let actual_best = actual.map(|(_, price, level)| (price, level));
        let actual_handle = actual.map(|(handle, _, _)| handle);
        if self.best != actual_best || self.best_handle != actual_handle {
            return Err(InvariantViolation::new(format!(
                "{:?} cached best level {:?}/{:?} differs from indexed-AVL extremum {:?}/{:?}",
                self.side, self.best, self.best_handle, actual_best, actual_handle
            )));
        }
        let actual_visible_best = self.visible_extremum().and_then(|price| {
            self.by_price
                .get(&price)
                .copied()
                .map(|level| (price, level))
        });
        if self.best_visible != actual_visible_best {
            return Err(InvariantViolation::new(format!(
                "{:?} cached public best level {:?} differs from public-price extremum {:?}",
                self.side, self.best_visible, actual_visible_best
            )));
        }
        if self.visible_by_price.len()
            != self
                .by_price
                .iter()
                .filter(|(_, level)| level.total_quantity != 0)
                .count()
            || self.visible_by_price.iter().any(|(price, ())| {
                self.by_price
                    .get(price)
                    .is_none_or(|level| level.total_quantity == 0)
            })
        {
            return Err(InvariantViolation::new(format!(
                "{:?} public-price membership differs from visible aggregates",
                self.side
            )));
        }
        let actual_event_units = self
            .by_price
            .iter()
            .map(|(_, level)| level)
            .try_fold(0_u128, |total, level| total.checked_add(level.event_units));
        if actual_event_units != Some(self.event_units) {
            return Err(InvariantViolation::new(format!(
                "{:?} cached event work {} differs from indexed-AVL aggregate {:?}",
                self.side, self.event_units, actual_event_units
            )));
        }
        Ok(())
    }

    fn is_better(&self, candidate: Price, current: Price) -> bool {
        match self.side {
            Side::Buy => candidate > current,
            Side::Sell => candidate < current,
        }
    }

    fn map_extremum(&self) -> Option<(PriceLevelHandle, Price, PriceLevel)> {
        let (slot, &price, &level) = match self.side {
            Side::Buy => self.by_price.last_key_value_with_handle(),
            Side::Sell => self.by_price.first_key_value_with_handle(),
        }?;
        Some((PriceLevelHandle { price, slot }, price, level))
    }

    fn sync_visible_level(
        &mut self,
        price: Price,
        previous: Option<PriceLevel>,
        current: Option<PriceLevel>,
    ) {
        let was_visible = previous.is_some_and(|level| level.total_quantity != 0);
        let is_visible = current.is_some_and(|level| level.total_quantity != 0);
        match (was_visible, is_visible) {
            (false, true) => {
                assert!(self.visible_by_price.insert(price, ()).is_none());
            }
            (true, false) => {
                assert_eq!(self.visible_by_price.remove(&price), Some(()));
            }
            (false, false) | (true, true) => {}
        }
        if is_visible
            && self
                .best_visible
                .is_none_or(|(best, _)| best == price || self.is_better(price, best))
        {
            self.best_visible = current.map(|level| (price, level));
        } else if self
            .best_visible
            .is_some_and(|(best, _)| best == price && !is_visible)
        {
            self.best_visible = self
                .visible_extremum()
                .and_then(|best| self.by_price.get(&best).copied().map(|level| (best, level)));
        }
    }

    fn visible_extremum(&self) -> Option<Price> {
        match self.side {
            Side::Buy => self
                .visible_by_price
                .last_key_value()
                .map(|(&price, ())| price),
            Side::Sell => self
                .visible_by_price
                .first_key_value()
                .map(|(&price, ())| price),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AccountSideIndex {
    head: Option<OrderId>,
    tail: Option<OrderId>,
    order_count: usize,
    event_units: u128,
}

impl PartialEq for AccountSideIndex {
    fn eq(&self, other: &Self) -> bool {
        self.order_count == other.order_count && self.event_units == other.event_units
    }
}

impl Eq for AccountSideIndex {}

#[cfg_attr(test, derive(Clone))]
#[derive(Debug, Default, Eq, PartialEq)]
struct AccountOrderIndex {
    buys: AccountSideIndex,
    sells: AccountSideIndex,
}

impl AccountOrderIndex {
    fn side(&self, side: Side) -> &AccountSideIndex {
        match side {
            Side::Buy => &self.buys,
            Side::Sell => &self.sells,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut AccountSideIndex {
        match side {
            Side::Buy => &mut self.buys,
            Side::Sell => &mut self.sells,
        }
    }

    fn head(&self, side: Side) -> Option<OrderId> {
        self.side(side).head
    }

    fn tail(&self, side: Side) -> Option<OrderId> {
        self.side(side).tail
    }

    fn order_count(&self, side: Side) -> usize {
        self.side(side).order_count
    }

    fn event_units(&self, side: Side) -> u128 {
        self.side(side).event_units
    }

    fn event_units_mut(&mut self, side: Side) -> &mut u128 {
        &mut self.side_mut(side).event_units
    }

    fn replace_event_units(&mut self, side: Side, old: u128, new: u128) {
        let updated = self
            .event_units(side)
            .checked_sub(old)
            .and_then(|value| value.checked_add(new))
            .expect("replacement event work must fit account aggregate");
        *self.event_units_mut(side) = updated;
    }

    fn append(&mut self, side: Side, order_id: OrderId, event_units: u128) -> Option<OrderId> {
        let index = self.side_mut(side);
        let previous = index.tail;
        if index.order_count == 0 {
            assert!(index.head.is_none() && index.tail.is_none());
            index.head = Some(order_id);
        } else {
            assert!(index.head.is_some() && index.tail.is_some());
        }
        index.tail = Some(order_id);
        index.order_count = index
            .order_count
            .checked_add(1)
            .expect("account order count must fit usize");
        index.event_units = index
            .event_units
            .checked_add(event_units)
            .expect("account event work must fit u128");
        previous
    }

    fn remove(
        &mut self,
        side: Side,
        order_id: OrderId,
        previous: Option<OrderId>,
        next: Option<OrderId>,
        event_units: u128,
    ) {
        let index = self.side_mut(side);
        assert_ne!(
            index.order_count, 0,
            "removed account side must be non-empty"
        );
        if previous.is_none() {
            assert_eq!(index.head, Some(order_id));
            index.head = next;
        }
        if next.is_none() {
            assert_eq!(index.tail, Some(order_id));
            index.tail = previous;
        }
        index.order_count -= 1;
        index.event_units = index
            .event_units
            .checked_sub(event_units)
            .expect("removed order event work must be indexed by account");
        if index.order_count == 0 {
            assert!(index.head.is_none() && index.tail.is_none());
        }
    }

    fn take_side(&mut self, side: Side) -> AccountSideIndex {
        std::mem::take(self.side_mut(side))
    }

    fn is_empty(&self) -> bool {
        self.buys.order_count == 0 && self.sells.order_count == 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CachedReport {
    command: Command,
    report: ExecutionReport,
}

#[derive(Clone, Copy, Debug)]
struct IncomingOrder {
    order_id: OrderId,
    account_id: AccountId,
    side: Side,
    order_type: OrderType,
    leaves: u64,
    display: OrderDisplay,
    self_trade_prevention: SelfTradePrevention,
}

#[derive(Clone, Copy, Debug)]
struct IncomingPreview {
    account_id: AccountId,
    side: Side,
    order_type: OrderType,
    leaves: u64,
    self_trade_prevention: SelfTradePrevention,
}

impl From<NewOrder> for IncomingPreview {
    fn from(order: NewOrder) -> Self {
        Self {
            account_id: order.account_id,
            side: order.side,
            order_type: order.order_type,
            leaves: order.quantity.lots(),
            self_trade_prevention: order.self_trade_prevention,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ReserveRefresh {
    order_id: OrderId,
    price: Price,
    displayed: u64,
    leaves: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LevelLiquidity {
    Filled,
    Remaining(u64),
    BlockedBySelfTrade,
}

/// A deterministic single-instrument central limit order book.
#[derive(Debug)]
pub struct OrderBook {
    instance_id: u64,
    definition: InstrumentDefinition,
    trading_state: TradingStateSnapshot,
    limits: OrderBookLimits,
    bids: PriceLevels,
    asks: PriceLevels,
    orders: BoundedHashMap<OrderId, RestingOrder>,
    expiries: IndexedAvlMap<ExpiryKey, OrderId>,
    expiry_watermark: Option<TimestampNs>,
    buy_stops: IndexedAvlMap<BuyStopKey, OrderId>,
    sell_stops: IndexedAvlMap<SellStopKey, OrderId>,
    stop_reference_price: Option<Price>,
    account_orders: BoundedHashMap<AccountId, AccountOrderIndex>,
    seen_order_ids: BoundedHashSet<OrderId>,
    account_controls: BoundedHashMap<AccountId, AccountControlSnapshot>,
    reports: BoundedHashMap<CommandId, CachedReport>,
    event_arena: Arc<EventArena>,
    order_selection_pool: Arc<OrderSelectionPool>,
    retained_event_count: usize,
    next_sequence: u64,
    next_trade_id: u64,
}

impl PartialEq for OrderBook {
    fn eq(&self, other: &Self) -> bool {
        self.definition == other.definition
            && self.trading_state == other.trading_state
            && self.limits == other.limits
            && self.bids == other.bids
            && self.asks == other.asks
            && self.orders == other.orders
            && self.expiries == other.expiries
            && self.expiry_watermark == other.expiry_watermark
            && self.buy_stops == other.buy_stops
            && self.sell_stops == other.sell_stops
            && self.stop_reference_price == other.stop_reference_price
            && self.account_orders == other.account_orders
            && self.seen_order_ids == other.seen_order_ids
            && self.account_controls == other.account_controls
            && self.reports == other.reports
            && self.retained_event_count == other.retained_event_count
            && self.next_sequence == other.next_sequence
            && self.next_trade_id == other.next_trade_id
    }
}

impl Eq for OrderBook {}

#[cfg(test)]
impl Clone for OrderBook {
    fn clone(&self) -> Self {
        let event_arena = try_event_arena(self.event_arena.capacity())
            .expect("test clone must reserve the validated event-arena capacity");
        let order_selection_pool = OrderSelectionPool::try_new(
            self.limits.max_prepared_order_selections(),
            self.limits.max_active_orders(),
        )
        .expect("test clone must reserve the validated order-selection pool");
        self.event_arena
            .copy_prefix_to(&event_arena, self.retained_event_count);
        let mut reports = self.reports.clone();
        for cached in reports.values_mut() {
            cached.report.events = cached
                .report
                .events
                .rebind_arena(&self.event_arena, Arc::clone(&event_arena));
        }
        Self {
            instance_id: self.instance_id,
            definition: self.definition,
            trading_state: self.trading_state,
            limits: self.limits,
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            orders: self.orders.clone(),
            expiries: self.expiries.clone(),
            expiry_watermark: self.expiry_watermark,
            buy_stops: self.buy_stops.clone(),
            sell_stops: self.sell_stops.clone(),
            stop_reference_price: self.stop_reference_price,
            account_orders: self.account_orders.clone(),
            seen_order_ids: self.seen_order_ids.clone(),
            account_controls: self.account_controls.clone(),
            reports,
            event_arena,
            order_selection_pool,
            retained_event_count: self.retained_event_count,
            next_sequence: self.next_sequence,
            next_trade_id: self.next_trade_id,
        }
    }
}

impl OrderBook {
    /// Creates an empty finitely bounded order book using [`OrderBookLimits::default`].
    #[must_use]
    pub fn new(definition: InstrumentDefinition) -> Self {
        Self::with_limits(definition, OrderBookLimits::default())
    }

    /// Creates an empty order book under an explicit finite resource policy.
    ///
    /// This convenience constructor follows A12 and panics if requested upfront
    /// reservation cannot be satisfied. Use [`Self::try_with_limits`] when
    /// configuration/allocation failure must be reported.
    ///
    /// # Panics
    ///
    /// Panics when constructor-time price/event/selection arena or hash
    /// reservation or process-local instance identity allocation fails.
    #[must_use]
    pub fn with_limits(definition: InstrumentDefinition, limits: OrderBookLimits) -> Self {
        Self::try_with_limits(definition, limits)
            .expect("order-book capacity reservation must succeed under A12")
    }

    /// Creates an empty bounded order book and fallibly reserves the complete
    /// price-level, retained-event, and order-selection arenas plus all
    /// configured hash maxima.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError::CapacityReservationFailed`] naming the first
    /// price/event/selection arena or hash index whose requested reservation
    /// cannot be represented or allocated, or
    /// [`MatchingError::BookInstanceIdExhausted`] if process-local book identity
    /// is exhausted.
    pub fn try_with_limits(
        definition: InstrumentDefinition,
        limits: OrderBookLimits,
    ) -> Result<Self, MatchingError> {
        let bids =
            PriceLevels::try_new(Side::Buy, limits.max_price_levels_per_side()).map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::BidPriceLevels)
            })?;
        let asks =
            PriceLevels::try_new(Side::Sell, limits.max_price_levels_per_side()).map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::AskPriceLevels)
            })?;
        let orders = BoundedHashMap::try_new(limits.max_active_orders()).map_err(|_| {
            MatchingError::CapacityReservationFailed(MatchingCapacity::ActiveOrders)
        })?;
        let expiries = IndexedAvlMap::try_with_capacity(limits.max_active_orders())
            .map_err(|_| MatchingError::CapacityReservationFailed(MatchingCapacity::ExpiryIndex))?;
        let buy_stops =
            IndexedAvlMap::try_with_capacity(limits.max_active_orders()).map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::BuyStopIndex)
            })?;
        let sell_stops =
            IndexedAvlMap::try_with_capacity(limits.max_active_orders()).map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::SellStopIndex)
            })?;
        let account_orders =
            BoundedHashMap::try_new(limits.max_active_accounts()).map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::ActiveAccounts)
            })?;
        let seen_order_ids =
            BoundedHashSet::try_new(limits.max_accepted_order_ids()).map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::AcceptedOrderIds)
            })?;
        let account_controls =
            BoundedHashMap::try_new(limits.max_account_controls()).map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::AccountControls)
            })?;
        let reports = BoundedHashMap::try_new(limits.max_retained_commands()).map_err(|_| {
            MatchingError::CapacityReservationFailed(MatchingCapacity::CommandHistory)
        })?;
        let event_arena = try_event_arena(limits.max_retained_events())?;
        let order_selection_pool = OrderSelectionPool::try_new(
            limits.max_prepared_order_selections(),
            limits.max_active_orders(),
        )?;
        let instance_id = next_order_book_instance_id()?;
        let trading_state = TradingStateSnapshot::from_parts(definition.trading_state(), 0);
        Ok(Self {
            instance_id,
            definition,
            trading_state,
            limits,
            bids,
            asks,
            orders,
            expiries,
            expiry_watermark: None,
            buy_stops,
            sell_stops,
            stop_reference_price: None,
            account_orders,
            seen_order_ids,
            account_controls,
            reports,
            event_arena,
            order_selection_pool,
            retained_event_count: 0,
            next_sequence: 1,
            next_trade_id: 1,
        })
    }

    /// Captures and synchronously verifies canonical state at one completed WAL
    /// report boundary.
    ///
    /// The checkpoint retains complete command/report history because exact
    /// retries and never-reusable order identifiers are part of matching state.
    /// This convenience path uses the same replay transition as
    /// [`Self::capture_checkpoint_candidate`] followed by
    /// [`OrderBookCheckpointCapture::verify`].
    ///
    /// # Errors
    ///
    /// Returns [`OrderBookCheckpointError::ResourceReservationFailed`] when the
    /// complete history or active-order capture cannot be reserved, preserves a
    /// typed temporary-book construction failure, or reports an invalid live
    /// structure, WAL boundary, or canonical checkpoint.
    pub fn checkpoint(
        &self,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<OrderBookCheckpoint, OrderBookCheckpointError> {
        self.capture_checkpoint_candidate(wal_metadata_sequence, wal_sequence)?
            .verify()
    }

    /// Captures an immutable candidate without deterministic full-history replay.
    ///
    /// The writer-side phase audits live topology, event-arena layout, complete
    /// command-derived lineage, and the WAL boundary, then copies `C`
    /// command/report handles plus canonical resting and dormant rows. Resting
    /// orders are emitted in buy-then-sell, ascending-price, FIFO order;
    /// dormant stops use canonical trigger priority. The returned value is not
    /// encodable or persistable until consumed by
    /// [`OrderBookCheckpointCapture::verify`].
    ///
    /// # Errors
    ///
    /// Returns a typed capture/validation reservation failure or `Invalid` for
    /// a live structural, lineage, WAL-boundary, or canonical-state contradiction.
    pub fn capture_checkpoint_candidate(
        &self,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<OrderBookCheckpointCapture, OrderBookCheckpointError> {
        Ok(OrderBookCheckpointCapture {
            checkpoint: self.checkpoint_state(wal_metadata_sequence, wal_sequence)?,
            limits: self.limits,
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "checkpoint capture audits and copies each canonical state class in one boundary"
    )]
    pub(crate) fn checkpoint_state(
        &self,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<OrderBookCheckpoint, OrderBookCheckpointError> {
        self.validate()
            .map_err(|error| OrderBookCheckpointError::new(error.detail()))?;
        let mut history = reserve_checkpoint_vec(
            self.reports.len(),
            OrderBookCheckpointResource::CaptureHistory,
        )?;
        let mut orders = reserve_checkpoint_vec(
            self.resting_order_count(),
            OrderBookCheckpointResource::CaptureActiveOrders,
        )?;
        let mut dormant_stops = reserve_checkpoint_vec(
            self.dormant_stop_count(),
            OrderBookCheckpointResource::CaptureDormantStops,
        )?;
        for (command_id, cached) in self.reports.iter() {
            if *command_id != cached.command.command_id() {
                return Err(OrderBookCheckpointError::new(
                    "live command-history key differs from its cached command",
                ));
            }
            history.push(CommandReportCheckpoint {
                command: cached.command,
                report: cached.report.clone(),
            });
        }

        for side in [Side::Buy, Side::Sell] {
            for level in self.levels(side).values() {
                let mut current = Some(level.head);
                while let Some(order_id) = current {
                    let order = self.orders.get(&order_id).ok_or_else(|| {
                        OrderBookCheckpointError::new(
                            "live price level references an absent order during checkpoint",
                        )
                    })?;
                    orders.push(RestingOrderCheckpoint {
                        order_id,
                        account_id: order.account_id,
                        side: order.side,
                        price: order.price,
                        leaves: Quantity::new(order.leaves).map_err(|_| {
                            OrderBookCheckpointError::new(
                                "live order has zero leaves during checkpoint",
                            )
                        })?,
                        working: Quantity::new(order.displayed).map_err(|_| {
                            OrderBookCheckpointError::new(
                                "live order has zero working quantity during checkpoint",
                            )
                        })?,
                        display: order.display,
                        self_trade_prevention: order.self_trade_prevention,
                        expires_at: order.expires_at,
                    });
                    current = order.next;
                }
            }
        }
        self.for_each_dormant_stop_in_canonical_order(|order| {
            let stop = order.stop.expect("canonical dormant order has stop state");
            dormant_stops.push(DormantStopCheckpoint {
                order_id: order.order_id,
                account_id: order.account_id,
                side: order.side,
                leaves: Quantity::new(order.leaves).expect("live dormant stop has non-zero leaves"),
                display: order.display,
                self_trade_prevention: order.self_trade_prevention,
                expires_at: order.expires_at,
                trigger_price: stop.trigger_price,
                activation: stop.activation,
                time_in_force: stop.time_in_force,
                priority_sequence: stop.priority_sequence,
            });
        })?;
        let (checkpoint, lineage) = OrderBookCheckpoint::from_parts_with_lineage(
            wal_metadata_sequence,
            wal_sequence,
            self.definition,
            orders,
            dormant_stops,
            history,
        )?;
        if lineage.seen_order_ids != self.seen_order_ids {
            return Err(OrderBookCheckpointError::new(
                "live accepted-order identity state differs from command history",
            ));
        }
        if lineage.account_controls != self.account_controls {
            return Err(OrderBookCheckpointError::new(
                "live account-control state differs from command history",
            ));
        }
        if lineage.trading_state != self.trading_state {
            return Err(OrderBookCheckpointError::new(
                "live trading-state control state differs from command history",
            ));
        }
        if lineage.expiry_watermark != self.expiry_watermark {
            return Err(OrderBookCheckpointError::new(
                "live expiry watermark differs from command history",
            ));
        }
        if lineage.stop_reference_price != self.stop_reference_price {
            return Err(OrderBookCheckpointError::new(
                "live stop reference differs from command history",
            ));
        }
        if lineage.retained_event_count != self.retained_event_count {
            return Err(OrderBookCheckpointError::new(
                "live retained-event count differs from command history",
            ));
        }
        if lineage.next_sequence != self.next_sequence {
            return Err(OrderBookCheckpointError::new(
                "live next event sequence differs from command history",
            ));
        }
        if lineage.next_trade_id != self.next_trade_id {
            return Err(OrderBookCheckpointError::new(
                "live next trade identifier differs from command history",
            ));
        }
        Ok(checkpoint)
    }

    /// Restores a directly indexed order book from a validated semantic checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OrderBookCheckpointError`] when semantic or reconstructed
    /// structural invariants fail.
    pub fn from_checkpoint(
        checkpoint: &OrderBookCheckpoint,
    ) -> Result<Self, OrderBookCheckpointError> {
        Self::from_checkpoint_with_limits(checkpoint, OrderBookLimits::default())
    }

    /// Restores a checkpoint under an explicit current finite resource policy.
    ///
    /// The selected policy may differ from the capture-time operational policy,
    /// but every retained and active cardinality must fit before state is built.
    ///
    /// # Errors
    ///
    /// Returns [`OrderBookCheckpointError`] when the checkpoint is invalid, its
    /// current state exceeds a selected limit, a validation resource cannot be
    /// reserved, or temporary book construction fails.
    #[allow(
        clippy::too_many_lines,
        reason = "direct restoration rebuilds and cross-audits every derived bounded index"
    )]
    pub fn from_checkpoint_with_limits(
        checkpoint: &OrderBookCheckpoint,
        limits: OrderBookLimits,
    ) -> Result<Self, OrderBookCheckpointError> {
        checkpoint.validate()?;
        validate_checkpoint_capacity(checkpoint, limits)?;
        let mut book = Self::try_with_limits(checkpoint.definition, limits)
            .map_err(OrderBookCheckpointError::BookConstructionFailed)?;
        for source in checkpoint.history.iter() {
            let mut entry = source.clone();
            if let (Command::New(order), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
            {
                book.seen_order_ids.insert(order.order_id);
            }
            if let (Command::AccountControl(control), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
            {
                let revision = control.expected_revision.checked_add(1).ok_or_else(|| {
                    OrderBookCheckpointError::new(
                        "checkpoint account-control revision is exhausted",
                    )
                })?;
                book.account_controls.insert(
                    control.account_id,
                    AccountControlSnapshot {
                        state: control.action.resulting_state(),
                        revision,
                    },
                );
            }
            if let (Command::TradingStateControl(control), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
            {
                let revision = control.expected_revision.checked_add(1).ok_or_else(|| {
                    OrderBookCheckpointError::new(
                        "checkpoint trading-state control revision is exhausted",
                    )
                })?;
                book.trading_state =
                    TradingStateSnapshot::from_parts(control.target_state, revision);
            }
            if let (Command::ExpirySweep(sweep), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
            {
                book.expiry_watermark = Some(sweep.through);
            }
            if let (Command::StopTriggerSweep(sweep), CommandOutcome::Accepted) =
                (entry.command, entry.report.outcome)
            {
                book.stop_reference_price = Some(sweep.reference_price);
            }
            for event in &entry.report.events {
                book.next_sequence = event.sequence.checked_add(1).ok_or_else(|| {
                    OrderBookCheckpointError::new("checkpoint event sequence is exhausted")
                })?;
                if let EventKind::Trade(trade) = event.kind {
                    book.next_trade_id = trade.trade_id.get().checked_add(1).ok_or_else(|| {
                        OrderBookCheckpointError::new("checkpoint trade identifier is exhausted")
                    })?;
                }
            }
            let event_count = entry.report.events.len();
            let mut events = EventTraceBuilder::new(
                Arc::clone(&book.event_arena),
                book.retained_event_count,
                event_count,
            );
            for event in entry.report.events.iter().copied() {
                events.push(event);
            }
            entry.report.events = events.finish();
            book.retained_event_count = book
                .retained_event_count
                .checked_add(event_count)
                .ok_or_else(|| {
                    OrderBookCheckpointError::new(
                        "checkpoint retained event count is not representable",
                    )
                })?;
            book.reports.insert(
                entry.command.command_id(),
                CachedReport {
                    command: entry.command,
                    report: entry.report,
                },
            );
        }
        for &state in checkpoint.orders.iter() {
            book.append_order(RestingOrder {
                order_id: state.order_id,
                account_id: state.account_id,
                side: state.side,
                price: state.price,
                leaves: state.leaves.lots(),
                displayed: state.working.lots(),
                display: state.display,
                self_trade_prevention: state.self_trade_prevention,
                expires_at: state.expires_at,
                stop: None,
                previous: None,
                next: None,
                account_previous: None,
                account_next: None,
            });
        }
        for &state in checkpoint.dormant_stops.iter() {
            let displayed = state.display.working_lots(state.leaves.lots());
            book.append_dormant_stop(RestingOrder {
                order_id: state.order_id,
                account_id: state.account_id,
                side: state.side,
                price: state.activation.risk_price().unwrap_or(state.trigger_price),
                leaves: state.leaves.lots(),
                displayed,
                display: state.display,
                self_trade_prevention: state.self_trade_prevention,
                expires_at: state.expires_at,
                stop: Some(DormantStopState {
                    trigger_price: state.trigger_price,
                    activation: state.activation,
                    time_in_force: state.time_in_force,
                    priority_sequence: state.priority_sequence,
                }),
                previous: None,
                next: None,
                account_previous: None,
                account_next: None,
            });
        }
        book.validate()
            .map_err(|error| OrderBookCheckpointError::new(error.detail()))?;
        Ok(book)
    }

    /// Returns this shard's finite resource policy.
    #[must_use]
    pub const fn limits(&self) -> OrderBookLimits {
        self.limits
    }

    /// Returns this shard's instrument.
    #[must_use]
    pub const fn instrument_id(&self) -> InstrumentId {
        self.definition.instrument_id()
    }

    /// Returns this shard's immutable instrument-rule version.
    #[must_use]
    pub const fn instrument_version(&self) -> InstrumentVersion {
        self.definition.version()
    }

    /// Returns the effective instrument trading state and accepted revision.
    #[must_use]
    pub const fn trading_state(&self) -> TradingStateSnapshot {
        self.trading_state
    }

    /// Returns the committed inclusive GTD-expiry watermark, if any.
    #[must_use]
    pub const fn expiry_watermark(&self) -> Option<TimestampNs> {
        self.expiry_watermark
    }

    /// Returns the committed authoritative last-trade stop reference, if initialized.
    #[must_use]
    pub const fn stop_reference_price(&self) -> Option<Price> {
        self.stop_reference_price
    }

    /// Applies a command exactly once and returns its complete trace.
    ///
    /// An exact duplicate returns the original event sequence with `replayed`
    /// set. Reusing a command identifier for different content is an error and
    /// cannot mutate the book.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError`] for an idempotency-key collision, configured
    /// capacity exhaustion, or sequence/trade-identifier exhaustion.
    pub fn submit(&mut self, command: Command) -> Result<ExecutionReport, MatchingError> {
        match self.prepare(command)? {
            CommandPreparation::Replay(report) => Ok(report),
            CommandPreparation::Ready(prepared) => self.commit(prepared),
        }
    }

    /// Evaluates all matching and instrument business rules without mutation.
    ///
    /// External deterministic gates can call this before their own checks so
    /// core matching rejection precedence remains authoritative.
    ///
    /// # Errors
    ///
    /// Returns the business [`RejectReason`] that [`OrderBook::submit`] would
    /// emit for a command not already present in the idempotency cache.
    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive command match preserves documented business-rejection precedence"
    )]
    pub fn check_business_rules(&self, command: Command) -> Result<(), RejectReason> {
        match command {
            Command::New(command) => {
                self.definition
                    .admit_in_state(Command::New(command), self.trading_state.state)
                    .map_err(RejectReason::from)?;
                if self.seen_order_ids.contains(&command.order_id) {
                    return Err(RejectReason::DuplicateOrder);
                }
                if self.account_control(command.account_id).state == AccountAdmissionState::Blocked
                {
                    return Err(RejectReason::AccountAdmissionBlocked);
                }
                if command
                    .time_in_force
                    .expires_at()
                    .is_some_and(|expires_at| {
                        expires_at <= command.received_at
                            || self
                                .expiry_watermark
                                .is_some_and(|watermark| expires_at <= watermark)
                    })
                {
                    return Err(RejectReason::OrderAlreadyExpired);
                }
                if let Some((trigger_price, activation)) = command.order_type.stop() {
                    let reference = self
                        .stop_reference_price
                        .ok_or(RejectReason::StopReferenceUnavailable)?;
                    let satisfied = match command.side {
                        Side::Buy => reference >= trigger_price,
                        Side::Sell => reference <= trigger_price,
                    };
                    if satisfied {
                        return Err(RejectReason::StopAlreadyTriggered);
                    }
                    if activation == StopActivation::Market
                        && command.time_in_force == TimeInForce::PostOnly
                    {
                        return Err(RejectReason::StopMarketCannotPost);
                    }
                }
                let active_order_type = command.order_type.active();
                if command.display.is_reserve()
                    && !(matches!(active_order_type, OrderType::Limit(_))
                        && command.time_in_force.may_rest())
                {
                    return Err(RejectReason::ReserveOrderCannotBeImmediate);
                }
                if command.display.is_hidden()
                    && !(matches!(active_order_type, OrderType::Limit(_))
                        && command.time_in_force.may_rest())
                {
                    return Err(RejectReason::HiddenOrderCannotBeImmediate);
                }
                if command.order_type.stop().is_some() {
                    if command.time_in_force == TimeInForce::FillOrKill
                        && command.self_trade_prevention == SelfTradePrevention::DecrementAndCancel
                    {
                        return Err(RejectReason::UnsupportedFokSelfTradePolicy);
                    }
                    return Ok(());
                }
                match (active_order_type, command.time_in_force) {
                    (
                        OrderType::Market,
                        TimeInForce::GoodTilCancelled | TimeInForce::GoodTilTimestamp { .. },
                    ) => {
                        return Err(RejectReason::MarketOrderCannotRest);
                    }
                    (OrderType::Market, TimeInForce::PostOnly) => {
                        return Err(RejectReason::MarketOrderCannotPost);
                    }
                    _ => {}
                }
                if command.time_in_force == TimeInForce::FillOrKill
                    && command.self_trade_prevention == SelfTradePrevention::DecrementAndCancel
                {
                    return Err(RejectReason::UnsupportedFokSelfTradePolicy);
                }
                if command.time_in_force == TimeInForce::PostOnly
                    && self.would_cross(command.side, active_order_type)
                {
                    return Err(RejectReason::PostOnlyWouldCross);
                }
                if command.time_in_force == TimeInForce::FillOrKill && !self.can_fill(&command) {
                    return Err(RejectReason::InsufficientLiquidity);
                }
            }
            Command::Cancel(command) => {
                self.definition
                    .admit(Command::Cancel(command))
                    .map_err(RejectReason::from)?;
                let order = self
                    .orders
                    .get(&command.order_id)
                    .ok_or(RejectReason::UnknownOrder)?;
                if order.account_id != command.account_id {
                    return Err(RejectReason::NotOrderOwner);
                }
            }
            Command::Replace(command) => {
                self.definition
                    .admit_in_state(Command::Replace(command), self.trading_state.state)
                    .map_err(RejectReason::from)?;
                let order = self
                    .orders
                    .get(&command.order_id)
                    .ok_or(RejectReason::UnknownOrder)?;
                if order.account_id != command.account_id {
                    return Err(RejectReason::NotOrderOwner);
                }
                if self.account_control(command.account_id).state == AccountAdmissionState::Blocked
                {
                    return Err(RejectReason::AccountAdmissionBlocked);
                }
                if !order.display.same_mode(command.new_display) {
                    return Err(RejectReason::OrderDisplayModeChangeNotAllowed);
                }
                if order
                    .stop
                    .is_some_and(|stop| stop.activation == StopActivation::Market)
                {
                    return Err(RejectReason::StopMarketCannotBeReplaced);
                }
            }
            Command::MassCancel(command) => {
                self.definition
                    .admit(Command::MassCancel(command))
                    .map_err(RejectReason::from)?;
            }
            Command::AccountControl(command) => {
                self.definition
                    .admit(Command::AccountControl(command))
                    .map_err(RejectReason::from)?;
                let current = self.account_control(command.account_id);
                if current.revision != command.expected_revision {
                    return Err(RejectReason::AccountControlRevisionMismatch);
                }
                if command.expected_revision.checked_add(1).is_none() {
                    return Err(RejectReason::AccountControlRevisionExhausted);
                }
            }
            Command::TradingStateControl(command) => {
                self.definition
                    .admit(Command::TradingStateControl(command))
                    .map_err(RejectReason::from)?;
                if self.trading_state.revision != command.expected_revision {
                    return Err(RejectReason::TradingStateControlRevisionMismatch);
                }
                if command.expected_revision.checked_add(1).is_none() {
                    return Err(RejectReason::TradingStateControlRevisionExhausted);
                }
                if self.trading_state.state == command.target_state {
                    return Err(RejectReason::TradingStateUnchanged);
                }
                if command.action == TradingStateControlAction::TransitionAndCancel
                    && command.target_state == TradingState::Open
                {
                    return Err(RejectReason::TradingStateControlCannotCancelIntoOpen);
                }
            }
            Command::ExpirySweep(command) => {
                self.definition
                    .admit(Command::ExpirySweep(command))
                    .map_err(RejectReason::from)?;
                if command.through > command.received_at {
                    return Err(RejectReason::ExpiryHorizonAfterCommandTime);
                }
                if self
                    .expiry_watermark
                    .is_some_and(|watermark| command.through < watermark)
                {
                    return Err(RejectReason::ExpiryWatermarkRegression);
                }
            }
            Command::StopTriggerSweep(command) => {
                self.definition
                    .admit(Command::StopTriggerSweep(command))
                    .map_err(RejectReason::from)?;
                if command.maximum_orders == 0 {
                    return Err(RejectReason::StopTriggerBatchEmpty);
                }
                if usize::try_from(command.maximum_orders)
                    .map_or(true, |maximum| maximum > self.limits.max_active_orders())
                {
                    return Err(RejectReason::StopTriggerBatchExceedsActiveOrderCapacity);
                }
                if self.stop_reference_price.is_some_and(|current| {
                    current != command.reference_price && self.eligible_stop_count(current) != 0
                }) {
                    return Err(RejectReason::StopTriggerBacklog);
                }
                if self.trading_state.state != TradingState::Open
                    && self.eligible_stop_count(command.reference_price) != 0
                {
                    return Err(RejectReason::InstrumentNotOpen);
                }
            }
        }
        Ok(())
    }

    /// Sequences and caches a rejection produced by an external deterministic gate.
    ///
    /// Core business rules are evaluated first. Exact retries return the
    /// original cached report, and identifier collisions remain operational
    /// errors.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError`] for idempotency collision, configured capacity,
    /// or sequence exhaustion.
    pub fn reject_by_gate(
        &mut self,
        command: Command,
        gate_reason: RejectReason,
    ) -> Result<ExecutionReport, MatchingError> {
        match self.prepare(command)? {
            CommandPreparation::Replay(report) => Ok(report),
            CommandPreparation::Ready(prepared) => {
                self.commit_with_gate(prepared, Some(gate_reason))
            }
        }
    }

    /// Prepares a command against the current matching generation.
    ///
    /// Preparation checks idempotency, all operational capacity and identifier
    /// bounds, and core matching business rules exactly once. It also proves the
    /// complete report event bound against constructor-owned arena headroom and
    /// leases any non-empty mass-cancel, expiry-sweep, block-and-cancel, or
    /// instrument transition-and-cancel selection buffer.
    /// Every matching hash index already owns its complete configured headroom,
    /// so preparation changes no semantic book state. A ready token can be durably written
    /// through [`PreparedCommand::command`] and then applied with
    /// [`OrderBook::commit`] without event/scratch growth or hash-table rehashing.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError`] for command-identifier collision, configured
    /// capacity, or sequence/trade-identifier exhaustion.
    pub fn prepare(&self, command: Command) -> Result<CommandPreparation, MatchingError> {
        let command_id = command.command_id();
        if let Some(cached) = self.reports.get(&command_id) {
            if cached.command != command {
                return Err(MatchingError::CommandIdCollision(command_id));
            }
            let mut report = cached.report.clone();
            report.replayed = true;
            return Ok(CommandPreparation::Replay(report));
        }

        let core_rejection = self.check_history_capacity_and_business(command)?;
        self.check_command_capacity(command, core_rejection)?;
        let (maximum_events, maximum_trades) = self.maximum_report_work(command, core_rejection)?;
        if maximum_events > self.limits.max_report_events() {
            return Err(MatchingError::CapacityExhausted(
                MatchingCapacity::ReportEvents,
            ));
        }
        self.check_event_capacity(command, core_rejection, maximum_events)?;
        let maximum_events_u64 =
            u64::try_from(maximum_events).map_err(|_| MatchingError::SequenceExhausted)?;
        self.next_sequence
            .checked_add(maximum_events_u64)
            .ok_or(MatchingError::SequenceExhausted)?;
        self.next_trade_id
            .checked_add(maximum_trades)
            .ok_or(MatchingError::TradeIdExhausted)?;
        let selected_order_ids = match (core_rejection, command) {
            (None, Command::MassCancel(command)) => {
                let selected_count =
                    self.account_active_order_count(command.account_id, command.scope);
                Some(self.acquire_order_selection(selected_count)?)
            }
            (
                None,
                Command::AccountControl(AccountControl {
                    action: AccountControlAction::BlockAndCancel,
                    account_id,
                    ..
                }),
            ) => {
                let selected_count =
                    self.account_active_order_count(account_id, MassCancelScope::All);
                Some(self.acquire_order_selection(selected_count)?)
            }
            (
                None,
                Command::TradingStateControl(TradingStateControl {
                    action: TradingStateControlAction::TransitionAndCancel,
                    ..
                }),
            ) => Some(self.acquire_order_selection(self.orders.len())?),
            (None, Command::ExpirySweep(command)) => {
                Some(self.acquire_order_selection(self.expiring_order_count(command.through))?)
            }
            (None, Command::StopTriggerSweep(command)) => Some(
                self.acquire_order_selection(
                    self.eligible_stop_count(command.reference_price).min(
                        usize::try_from(command.maximum_orders)
                            .map_err(|_| MatchingError::InternalInvariantViolation)?,
                    ),
                )?,
            ),
            _ => None,
        };
        Ok(CommandPreparation::Ready(PreparedCommand {
            command,
            book_instance_id: self.instance_id,
            expected_retained_commands: self.reports.len(),
            core_rejection,
            maximum_events,
            selected_order_ids,
        }))
    }

    fn acquire_order_selection(
        &self,
        required: usize,
    ) -> Result<PooledOrderSelection, MatchingError> {
        if required == 0 {
            return Ok(PooledOrderSelection::empty());
        }
        let selected = self.order_selection_pool.acquire().ok_or(
            MatchingError::PreparationCapacityExhausted {
                maximum: self.limits.max_prepared_order_selections(),
            },
        )?;
        if selected.capacity() < required {
            return Err(MatchingError::InternalInvariantViolation);
        }
        Ok(selected)
    }

    /// Computes a safe command-specific report/trade bound.
    ///
    /// Incoming matching commands use `O(1)` mutation-maintained aggregates;
    /// an expiry sweep counts only its ordered qualifying prefix.
    fn maximum_report_work(
        &self,
        command: Command,
        core_rejection: Option<RejectReason>,
    ) -> Result<(usize, u64), MatchingError> {
        if core_rejection.is_some() {
            return Ok((1, 0));
        }

        let (events, trades) = match command {
            Command::New(order) if order.order_type.stop().is_some() => (2_u128, 0),
            Command::New(order) if order.time_in_force == TimeInForce::PostOnly => (2_u128, 0),
            Command::New(order) => {
                let terminal_events = u128::from(order.time_in_force != TimeInForce::FillOrKill);
                self.maximum_matching_work(IncomingPreview::from(order), terminal_events)?
            }
            Command::Cancel(_) => (1, 0),
            Command::MassCancel(command) => {
                let selected = self.account_active_order_count(command.account_id, command.scope);
                let events = selected
                    .checked_add(1)
                    .ok_or(MatchingError::SequenceExhausted)?;
                return Ok((events, 0));
            }
            Command::AccountControl(command) => {
                let selected = match command.action {
                    AccountControlAction::BlockAndCancel => {
                        self.account_active_order_count(command.account_id, MassCancelScope::All)
                    }
                    AccountControlAction::Enable => 0,
                };
                let events = selected
                    .checked_add(1)
                    .ok_or(MatchingError::SequenceExhausted)?;
                return Ok((events, 0));
            }
            Command::TradingStateControl(command) => {
                let selected = match command.action {
                    TradingStateControlAction::Transition => 0,
                    TradingStateControlAction::TransitionAndCancel => self.orders.len(),
                };
                let events = selected
                    .checked_add(1)
                    .ok_or(MatchingError::SequenceExhausted)?;
                return Ok((events, 0));
            }
            Command::ExpirySweep(command) => {
                let selected = self.expiring_order_count(command.through);
                let events = selected
                    .checked_add(1)
                    .ok_or(MatchingError::SequenceExhausted)?;
                return Ok((events, 0));
            }
            Command::StopTriggerSweep(command) => {
                return self.maximum_stop_trigger_work(command);
            }
            Command::Replace(command) => {
                let old = self
                    .orders
                    .get(&command.order_id)
                    .expect("accepted replacement must reference an active order");
                if old.is_dormant_stop() {
                    (1, 0)
                } else {
                    let priority_retained = old.price == command.new_price
                        && command.new_quantity.lots() <= old.leaves
                        && old.display == command.new_display;
                    if priority_retained {
                        (1, 0)
                    } else {
                        self.maximum_matching_work(
                            IncomingPreview {
                                account_id: old.account_id,
                                side: old.side,
                                order_type: OrderType::Limit(command.new_price),
                                leaves: command.new_quantity.lots(),
                                self_trade_prevention: old.self_trade_prevention,
                            },
                            1,
                        )?
                    }
                }
            }
        };
        let events = usize::try_from(events).map_err(|_| MatchingError::SequenceExhausted)?;
        Ok((events, trades))
    }

    fn maximum_matching_work(
        &self,
        incoming: IncomingPreview,
        terminal_events: u128,
    ) -> Result<(u128, u64), MatchingError> {
        let quantity_steps = incoming.leaves / self.definition.quantity_rules().increment_lots();
        let quantity_steps = u128::from(quantity_steps);
        let opposite = incoming.side.opposite();
        let total_event_units = self.levels(opposite).event_units();
        let (self_order_count, self_event_units) = self
            .account_orders
            .get(&incoming.account_id)
            .map_or((0_u128, 0_u128), |index| {
                (
                    u128::try_from(index.order_count(opposite))
                        .expect("active-order count must fit u128"),
                    index.event_units(opposite),
                )
            });
        let external_event_units = total_event_units
            .checked_sub(self_event_units)
            .expect("account event work must be a subset of side event work");
        let consuming_event_limit = quantity_steps
            .checked_mul(2)
            .ok_or(MatchingError::SequenceExhausted)?;
        let matching_and_terminal = match incoming.self_trade_prevention {
            SelfTradePrevention::CancelResting => consuming_event_limit
                .min(external_event_units)
                .checked_add(
                    self_order_count
                        .checked_mul(2)
                        .ok_or(MatchingError::SequenceExhausted)?,
                )
                .and_then(|value| value.checked_add(terminal_events))
                .ok_or(MatchingError::SequenceExhausted)?,
            SelfTradePrevention::CancelAggressor => consuming_event_limit
                .min(external_event_units)
                .checked_add(terminal_events.max(if self_order_count == 0 { 0 } else { 2 }))
                .ok_or(MatchingError::SequenceExhausted)?,
            SelfTradePrevention::CancelBoth => consuming_event_limit
                .min(external_event_units)
                .checked_add(terminal_events.max(if self_order_count == 0 { 0 } else { 3 }))
                .ok_or(MatchingError::SequenceExhausted)?,
            SelfTradePrevention::DecrementAndCancel => consuming_event_limit
                .min(total_event_units)
                .checked_add(terminal_events)
                .ok_or(MatchingError::SequenceExhausted)?,
        };
        let events = matching_and_terminal
            .checked_add(1)
            .ok_or(MatchingError::SequenceExhausted)?;
        let maximum_trades = quantity_steps.min(external_event_units);
        let maximum_trades =
            u64::try_from(maximum_trades).map_err(|_| MatchingError::TradeIdExhausted)?;
        Ok((events, maximum_trades))
    }

    fn maximum_stop_trigger_work(
        &self,
        command: StopTriggerSweep,
    ) -> Result<(usize, u64), MatchingError> {
        let limit = self.eligible_stop_count(command.reference_price).min(
            usize::try_from(command.maximum_orders)
                .map_err(|_| MatchingError::InternalInvariantViolation)?,
        );
        let mut events = 1_u128;
        let mut trades = 0_u64;
        self.for_each_eligible_stop(command.reference_price, limit, |order_id| {
            let order = self
                .orders
                .get(&order_id)
                .copied()
                .ok_or(MatchingError::InternalInvariantViolation)?;
            let stop = order
                .stop
                .ok_or(MatchingError::InternalInvariantViolation)?;
            let (order_events, order_trades) = if stop.time_in_force == TimeInForce::PostOnly {
                (2_u128, 0)
            } else {
                let terminal = u128::from(stop.time_in_force != TimeInForce::FillOrKill);
                let (candidate_events, candidate_trades) = self.maximum_matching_work(
                    IncomingPreview {
                        account_id: order.account_id,
                        side: order.side,
                        order_type: stop.activation.order_type(),
                        leaves: order.leaves,
                        self_trade_prevention: order.self_trade_prevention,
                    },
                    terminal,
                )?;
                if stop.time_in_force == TimeInForce::FillOrKill {
                    (candidate_events.max(2), candidate_trades)
                } else {
                    (candidate_events, candidate_trades)
                }
            };
            events = events
                .checked_add(order_events)
                .ok_or(MatchingError::SequenceExhausted)?;
            trades = trades
                .checked_add(order_trades)
                .ok_or(MatchingError::TradeIdExhausted)?;
            Ok(())
        })?;
        Ok((
            usize::try_from(events).map_err(|_| MatchingError::SequenceExhausted)?,
            trades,
        ))
    }

    fn eligible_stop_count(&self, reference_price: Price) -> usize {
        let buys = self
            .buy_stops
            .iter()
            .take_while(|(key, _)| key.trigger_price <= reference_price)
            .count();
        let sells = self
            .sell_stops
            .iter()
            .take_while(|(key, _)| key.trigger_price.0 >= reference_price)
            .count();
        debug_assert!(
            buys == 0 || sells == 0,
            "valid stop state cannot have eligible stops on both sides"
        );
        buys.saturating_add(sells)
    }

    fn for_each_eligible_stop<E>(
        &self,
        reference_price: Price,
        limit: usize,
        mut visit: impl FnMut(OrderId) -> Result<(), E>,
    ) -> Result<(), E> {
        let buy_count = self
            .buy_stops
            .iter()
            .take_while(|(key, _)| key.trigger_price <= reference_price)
            .count();
        if buy_count != 0 {
            for &order_id in self
                .buy_stops
                .iter()
                .take(buy_count.min(limit))
                .map(|(_, order_id)| order_id)
            {
                visit(order_id)?;
            }
            return Ok(());
        }
        for &order_id in self
            .sell_stops
            .iter()
            .take_while(|(key, _)| key.trigger_price.0 >= reference_price)
            .take(limit)
            .map(|(_, order_id)| order_id)
        {
            visit(order_id)?;
        }
        Ok(())
    }

    fn for_each_dormant_stop_in_canonical_order(
        &self,
        mut visit: impl FnMut(RestingOrder),
    ) -> Result<(), OrderBookCheckpointError> {
        for &order_id in self.buy_stops.iter().map(|(_, order_id)| order_id) {
            let order = self.orders.get(&order_id).copied().ok_or_else(|| {
                OrderBookCheckpointError::new(
                    "buy-stop index references an absent order during checkpoint",
                )
            })?;
            visit(order);
        }
        for &order_id in self.sell_stops.iter().map(|(_, order_id)| order_id) {
            let order = self.orders.get(&order_id).copied().ok_or_else(|| {
                OrderBookCheckpointError::new(
                    "sell-stop index references an absent order during checkpoint",
                )
            })?;
            visit(order);
        }
        Ok(())
    }

    /// Commits one prepared command without repeating preflight.
    ///
    /// If the exact command was committed after preparation, this resolves to
    /// its cached replay. Any unrelated intervening command makes the token
    /// stale. The retained-history generation check precedes every mutation.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError::CommandIdCollision`] if another command reused
    /// the identifier, [`MatchingError::StalePreparation`] if an unrelated
    /// command committed after preparation, or a sequence error only if an
    /// internal invariant contradicts successful preparation.
    pub fn commit(&mut self, prepared: PreparedCommand) -> Result<ExecutionReport, MatchingError> {
        self.commit_with_gate(prepared, None)
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "opaque prepared commands are intentionally single-use capabilities"
    )]
    pub(crate) fn commit_with_gate(
        &mut self,
        prepared: PreparedCommand,
        gate_reason: Option<RejectReason>,
    ) -> Result<ExecutionReport, MatchingError> {
        let PreparedCommand {
            command,
            book_instance_id,
            expected_retained_commands,
            core_rejection,
            maximum_events,
            selected_order_ids,
        } = prepared;
        if self.instance_id != book_instance_id {
            return Err(MatchingError::ForeignPreparation);
        }
        let command_id = command.command_id();
        if let Some(cached) = self.reports.get(&command_id) {
            if cached.command != command {
                return Err(MatchingError::CommandIdCollision(command_id));
            }
            let mut report = cached.report.clone();
            report.replayed = true;
            return Ok(report);
        }
        if self.reports.len() != expected_retained_commands {
            return Err(MatchingError::StalePreparation);
        }
        let events = EventTraceBuilder::new(
            Arc::clone(&self.event_arena),
            self.retained_event_count,
            maximum_events,
        );

        let report = if let Some(reason) = core_rejection.or(gate_reason) {
            self.rejected(command_id, command.received_at(), reason, events)?
        } else {
            match command {
                Command::New(new_order) => self.apply_new(new_order, events)?,
                Command::Cancel(cancel) => self.apply_cancel(cancel, events)?,
                Command::Replace(replace) => self.apply_replace(replace, events)?,
                Command::MassCancel(mass_cancel) => self.apply_mass_cancel(
                    mass_cancel,
                    events,
                    selected_order_ids
                        .expect("accepted mass cancellation must own selection storage"),
                )?,
                Command::AccountControl(control) => {
                    self.apply_account_control(control, events, selected_order_ids)?
                }
                Command::TradingStateControl(control) => {
                    self.apply_trading_state_control(control, events, selected_order_ids)?
                }
                Command::ExpirySweep(sweep) => self.apply_expiry_sweep(
                    sweep,
                    events,
                    selected_order_ids.expect("accepted expiry sweep must own selection storage"),
                )?,
                Command::StopTriggerSweep(sweep) => self.apply_stop_trigger_sweep(
                    sweep,
                    events,
                    selected_order_ids
                        .expect("accepted stop trigger sweep must own selection storage"),
                )?,
            }
        };
        self.cache_report(command, &report);
        Ok(report)
    }

    fn check_history_capacity_and_business(
        &self,
        command: Command,
    ) -> Result<Option<RejectReason>, MatchingError> {
        if self.reports.len() >= self.limits.max_retained_commands() {
            return Err(MatchingError::CapacityExhausted(
                MatchingCapacity::CommandHistory,
            ));
        }
        if self.reports.len() >= self.limits.ordinary_command_capacity() {
            if !Self::is_protected_cancellation_command(command) {
                return Err(MatchingError::CapacityExhausted(
                    MatchingCapacity::AdmissionCommandHistory,
                ));
            }
            let core_rejection = self.check_business_rules(command).err();
            if core_rejection.is_some() {
                return Err(MatchingError::CapacityExhausted(
                    MatchingCapacity::AdmissionCommandHistory,
                ));
            }
            return Ok(None);
        }
        Ok(self.check_business_rules(command).err())
    }

    fn is_protected_cancellation_command(command: Command) -> bool {
        matches!(
            command,
            Command::Cancel(_)
                | Command::MassCancel(_)
                | Command::AccountControl(AccountControl {
                    action: AccountControlAction::BlockAndCancel,
                    ..
                })
                | Command::TradingStateControl(TradingStateControl {
                    action: TradingStateControlAction::TransitionAndCancel,
                    target_state: TradingState::CancelOnly
                        | TradingState::Halted
                        | TradingState::Closed,
                    ..
                })
                | Command::ExpirySweep(_)
        )
    }

    fn check_event_capacity(
        &self,
        command: Command,
        core_rejection: Option<RejectReason>,
        maximum_events: usize,
    ) -> Result<(), MatchingError> {
        let final_count = self
            .retained_event_count
            .checked_add(maximum_events)
            .ok_or(MatchingError::CapacityExhausted(
                MatchingCapacity::RetainedEvents,
            ))?;
        if final_count > self.limits.max_retained_events() {
            return Err(MatchingError::CapacityExhausted(
                MatchingCapacity::RetainedEvents,
            ));
        }
        if final_count > self.limits.ordinary_event_capacity()
            && (core_rejection.is_some() || !Self::is_protected_cancellation_command(command))
        {
            return Err(MatchingError::CapacityExhausted(
                MatchingCapacity::AdmissionEventHistory,
            ));
        }
        Ok(())
    }

    fn check_command_capacity(
        &self,
        command: Command,
        core_rejection: Option<RejectReason>,
    ) -> Result<(), MatchingError> {
        match command {
            Command::New(command) => self.check_new_capacity(command),
            Command::Replace(command) => self.check_replace_capacity(command),
            Command::Cancel(_)
            | Command::MassCancel(_)
            | Command::TradingStateControl(_)
            | Command::ExpirySweep(_)
            | Command::StopTriggerSweep(_) => Ok(()),
            Command::AccountControl(command) => {
                if core_rejection.is_some()
                    || self.account_controls.contains_key(&command.account_id)
                    || self.account_controls.len() < self.limits.max_account_controls()
                {
                    Ok(())
                } else {
                    Err(MatchingError::CapacityExhausted(
                        MatchingCapacity::AccountControls,
                    ))
                }
            }
        }
    }

    fn check_new_capacity(&self, command: NewOrder) -> Result<(), MatchingError> {
        if self.seen_order_ids.contains(&command.order_id) {
            return Ok(());
        }
        if self.seen_order_ids.len() >= self.limits.max_accepted_order_ids() {
            return Err(MatchingError::CapacityExhausted(
                MatchingCapacity::AcceptedOrderIds,
            ));
        }
        if command.order_type.stop().is_some() {
            if self.orders.len() >= self.limits.max_active_orders() {
                return Err(MatchingError::CapacityExhausted(
                    MatchingCapacity::ActiveOrders,
                ));
            }
            if !self.account_orders.contains_key(&command.account_id)
                && self.account_orders.len() >= self.limits.max_active_accounts()
            {
                return Err(MatchingError::CapacityExhausted(
                    MatchingCapacity::ActiveAccounts,
                ));
            }
            return Ok(());
        }
        let OrderType::Limit(price) = command.order_type else {
            return Ok(());
        };
        if !command.time_in_force.may_rest() {
            return Ok(());
        }
        let active_orders_full = self.orders.len() >= self.limits.max_active_orders();
        let active_accounts_full = !self.account_orders.contains_key(&command.account_id)
            && self.account_orders.len() >= self.limits.max_active_accounts();
        let levels = self.levels(command.side);
        let price_levels_full =
            levels.get(price).is_none() && levels.len() >= self.limits.max_price_levels_per_side();
        if !active_orders_full && !active_accounts_full && !price_levels_full {
            return Ok(());
        }
        let preview = IncomingPreview::from(command);
        let Some(removed_orders) = self.resting_residual_removed_orders(preview) else {
            return Ok(());
        };
        if active_orders_full {
            let final_active_orders = self
                .orders
                .len()
                .checked_sub(removed_orders)
                .and_then(|remaining| remaining.checked_add(1))
                .expect("residual preview removal count must describe active makers");
            if final_active_orders > self.limits.max_active_orders() {
                return Err(MatchingError::CapacityExhausted(
                    MatchingCapacity::ActiveOrders,
                ));
            }
        }
        if active_accounts_full {
            let released_accounts = self.resting_residual_released_accounts(preview);
            let final_active_accounts = self
                .account_orders
                .len()
                .checked_sub(released_accounts)
                .and_then(|remaining| remaining.checked_add(1))
                .expect("residual preview release count must describe active accounts");
            if final_active_accounts > self.limits.max_active_accounts() {
                return Err(MatchingError::CapacityExhausted(
                    MatchingCapacity::ActiveAccounts,
                ));
            }
        }
        if price_levels_full {
            return Err(MatchingError::CapacityExhausted(
                Self::price_level_capacity(command.side),
            ));
        }
        Ok(())
    }

    fn check_replace_capacity(&self, command: ReplaceOrder) -> Result<(), MatchingError> {
        let Some(order) = self.orders.get(&command.order_id) else {
            return Ok(());
        };
        if order.is_dormant_stop() {
            return Ok(());
        }
        let levels = self.levels(order.side);
        if levels.get(command.new_price).is_some() {
            return Ok(());
        }
        let old_level_is_released = order.price != command.new_price
            && levels
                .get(order.price)
                .is_some_and(|level| level.order_count == 1);
        let levels_after_removal = levels.len() - usize::from(old_level_is_released);
        if levels_after_removal < self.limits.max_price_levels_per_side() {
            return Ok(());
        }
        // Replacement removes the old same-side order before executing. It
        // therefore changes no opposite-side input to the residual theorem: at
        // a full level bound, only a proved non-zero residual needs the absent
        // target level.
        let preview = IncomingPreview {
            account_id: order.account_id,
            side: order.side,
            order_type: OrderType::Limit(command.new_price),
            leaves: command.new_quantity.lots(),
            self_trade_prevention: order.self_trade_prevention,
        };
        if self.resting_residual_removed_orders(preview).is_none() {
            return Ok(());
        }
        Err(MatchingError::CapacityExhausted(
            Self::price_level_capacity(order.side),
        ))
    }

    const fn price_level_capacity(side: Side) -> MatchingCapacity {
        match side {
            Side::Buy => MatchingCapacity::BidPriceLevels,
            Side::Sell => MatchingCapacity::AskPriceLevels,
        }
    }

    /// Returns the current best bid.
    #[must_use]
    pub fn best_bid(&self) -> Option<LevelSnapshot> {
        self.bids
            .best_visible_level()
            .map(|(price, level)| Self::level_snapshot(price, &level))
    }

    /// Returns the current best ask.
    #[must_use]
    pub fn best_ask(&self) -> Option<LevelSnapshot> {
        self.asks
            .best_visible_level()
            .map(|(price, level)| Self::level_snapshot(price, &level))
    }

    /// Returns process-local allocation telemetry for one execution-price
    /// arena.
    ///
    /// The allocated capacity is at least the configured maximum but can be
    /// larger because allocator rounding is permitted. Initialized slots are a
    /// high-water mark; removal moves a slot to the reusable count instead of
    /// deallocating it. The paired public-price arena has the same configured
    /// reservation and is independently checked by [`Self::validate`].
    #[must_use]
    pub fn price_level_arena_status(&self, side: Side) -> PriceLevelArenaStatus {
        let levels = self.levels(side);
        PriceLevelArenaStatus {
            configured_slots: self.limits.max_price_levels_per_side(),
            allocated_slots: levels.allocation_capacity(),
            initialized_slots: levels.storage_len(),
            occupied_slots: levels.len(),
            reusable_slots: levels.storage_len().saturating_sub(levels.len()),
        }
    }

    /// Returns process-local allocation telemetry for retained event storage.
    #[must_use]
    pub fn event_arena_status(&self) -> EventArenaStatus {
        EventArenaStatus {
            configured_events: self.limits.max_retained_events(),
            allocated_events: self.event_arena.allocated_capacity(),
            retained_events: self.retained_event_count,
        }
    }

    /// Returns constructor allocation and current availability for order-selection storage.
    #[must_use]
    pub fn order_selection_pool_status(&self) -> OrderSelectionPoolStatus {
        OrderSelectionPoolStatus {
            configured_leases: self.limits.max_prepared_order_selections(),
            available_leases: self.order_selection_pool.available(),
            allocated_order_ids_per_lease: self.order_selection_pool.order_capacity,
        }
    }

    /// Returns process-local allocation telemetry for one bounded hash index.
    ///
    /// Construction reserves at least the complete configured entry bound.
    /// Allocator rounding can make `allocated_entries` larger. The capacity is
    /// invariant for the lifetime of the book because no matching path shrinks,
    /// grows, or explicitly rehashes these indexes after construction.
    #[must_use]
    pub fn hash_index_status(&self, index: MatchingHashIndex) -> HashIndexStatus {
        match index {
            MatchingHashIndex::ActiveOrders => HashIndexStatus {
                configured_entries: self.limits.max_active_orders(),
                allocated_entries: self.orders.capacity(),
                occupied_entries: self.orders.len(),
            },
            MatchingHashIndex::ActiveAccounts => HashIndexStatus {
                configured_entries: self.limits.max_active_accounts(),
                allocated_entries: self.account_orders.capacity(),
                occupied_entries: self.account_orders.len(),
            },
            MatchingHashIndex::AcceptedOrderIds => HashIndexStatus {
                configured_entries: self.limits.max_accepted_order_ids(),
                allocated_entries: self.seen_order_ids.capacity(),
                occupied_entries: self.seen_order_ids.len(),
            },
            MatchingHashIndex::AccountControls => HashIndexStatus {
                configured_entries: self.limits.max_account_controls(),
                allocated_entries: self.account_controls.capacity(),
                occupied_entries: self.account_controls.len(),
            },
            MatchingHashIndex::CommandHistory => HashIndexStatus {
                configured_entries: self.limits.max_retained_commands(),
                allocated_entries: self.reports.capacity(),
                occupied_entries: self.reports.len(),
            },
        }
    }

    /// Returns up to `limit` levels in market-priority order.
    #[must_use]
    pub fn depth(&self, side: Side, limit: usize) -> Vec<LevelSnapshot> {
        match side {
            Side::Buy => self.depth_levels(side).rev().take(limit).collect(),
            Side::Sell => self.depth_levels(side).take(limit).collect(),
        }
    }

    /// Iterates aggregate levels in ascending price order without allocating.
    pub(crate) fn depth_levels(
        &self,
        side: Side,
    ) -> impl DoubleEndedIterator<Item = LevelSnapshot> + '_ {
        self.levels(side)
            .iter()
            .filter(|(_, level)| level.total_quantity != 0)
            .map(|(&price, level)| Self::level_snapshot(price, level))
    }

    /// Returns one public aggregate level, or `None` when the price has no
    /// displayed liquidity.
    #[must_use]
    pub fn level(&self, side: Side, price: Price) -> Option<LevelSnapshot> {
        self.levels(side)
            .get(price)
            .filter(|level| level.total_quantity != 0)
            .map(|level| Self::level_snapshot(price, level))
    }

    /// Returns a resting order by identifier.
    #[must_use]
    pub fn order(&self, order_id: OrderId) -> Option<OrderSnapshot> {
        self.orders.get(&order_id).and_then(|order| {
            if order.is_dormant_stop() {
                return None;
            }
            Quantity::new(order.leaves)
                .ok()
                .and_then(|leaves_quantity| {
                    Quantity::new(order.displayed)
                        .ok()
                        .map(|working_quantity| OrderSnapshot {
                            order_id,
                            account_id: order.account_id,
                            side: order.side,
                            price: order.price,
                            leaves_quantity,
                            working_quantity,
                            display: order.display,
                            expires_at: order.expires_at,
                        })
                })
        })
    }

    /// Returns every active resting order in ascending identifier order.
    ///
    /// This allocates and sorts in `O(O log O)` time. It is intended for
    /// snapshots, recovered-state bootstrap, and diagnostics rather than the
    /// matching hot path.
    ///
    /// # Errors
    ///
    /// Returns [`InvariantViolation`] if a resting order contains an impossible
    /// zero total or displayed leaves quantity.
    pub fn active_orders(&self) -> Result<Vec<OrderSnapshot>, InvariantViolation> {
        let mut orders = Vec::with_capacity(self.resting_order_count());
        for order in self.active_order_states() {
            orders.push(order?);
        }
        orders.sort_unstable_by_key(|order| order.order_id);
        Ok(orders)
    }

    /// Iterates resting-order state in dense process-local order without allocating.
    pub(crate) fn active_order_states(
        &self,
    ) -> impl Iterator<Item = Result<OrderSnapshot, InvariantViolation>> + '_ {
        self.orders
            .values()
            .filter(|order| !order.is_dormant_stop())
            .map(|order| {
                let leaves_quantity = Quantity::new(order.leaves).map_err(|_| {
                    InvariantViolation::new(format!(
                        "active order {} has zero leaves quantity",
                        order.order_id
                    ))
                })?;
                let working_quantity = Quantity::new(order.displayed).map_err(|_| {
                    InvariantViolation::new(format!(
                        "active order {} has zero working quantity",
                        order.order_id
                    ))
                })?;
                Ok(OrderSnapshot {
                    order_id: order.order_id,
                    account_id: order.account_id,
                    side: order.side,
                    price: order.price,
                    leaves_quantity,
                    working_quantity,
                    display: order.display,
                    expires_at: order.expires_at,
                })
            })
    }

    /// Returns one dormant stop by identifier.
    #[must_use]
    pub fn dormant_stop(&self, order_id: OrderId) -> Option<DormantStopSnapshot> {
        self.orders
            .get(&order_id)
            .copied()
            .and_then(RestingOrder::stop_snapshot)
    }

    /// Iterates dormant stop state in dense process-local order without allocating.
    pub(crate) fn dormant_stop_states(&self) -> impl Iterator<Item = DormantStopSnapshot> + '_ {
        self.orders
            .values()
            .filter_map(|order| order.stop_snapshot())
    }

    /// Returns the number of dormant stops excluded from displayed depth.
    #[must_use]
    pub fn dormant_stop_count(&self) -> usize {
        self.buy_stops.len().saturating_add(self.sell_stops.len())
    }

    /// Returns the number of orders currently represented in price levels.
    #[must_use]
    pub fn resting_order_count(&self) -> usize {
        self.orders.len().saturating_sub(self.dormant_stop_count())
    }

    /// Returns the most recently allocated event sequence, or zero at genesis.
    #[must_use]
    pub const fn last_event_sequence(&self) -> u64 {
        self.next_sequence - 1
    }

    /// Returns the most recently allocated trade identifier, or `None` before
    /// the first execution.
    #[must_use]
    pub fn last_trade_id(&self) -> Option<TradeId> {
        let value = self.next_trade_id - 1;
        TradeId::new(value).ok()
    }

    /// Returns the number of currently resting or dormant stop orders.
    #[must_use]
    pub fn active_order_count(&self) -> usize {
        self.orders.len()
    }

    /// Returns the number of sequenced command/report pairs retained for exact replay.
    #[must_use]
    pub fn retained_command_count(&self) -> usize {
        self.reports.len()
    }

    /// Returns one account's effective admission fence and revision.
    ///
    /// Accounts without an accepted control command are enabled at revision zero.
    #[must_use]
    pub fn account_control(&self, account_id: AccountId) -> AccountControlSnapshot {
        self.account_controls
            .get(&account_id)
            .copied()
            .unwrap_or_default()
    }

    /// Iterates retained account-control state without allocation.
    #[must_use]
    pub fn account_controls(
        &self,
    ) -> impl ExactSizeIterator<Item = (AccountId, AccountControlSnapshot)> + '_ {
        self.account_controls
            .iter()
            .map(|(&account_id, &snapshot)| (account_id, snapshot))
    }

    /// Returns the active-order count for one account and optional side.
    ///
    /// This performs one expected `O(1)` account lookup and no book scan.
    #[must_use]
    pub fn account_active_order_count(
        &self,
        account_id: AccountId,
        scope: MassCancelScope,
    ) -> usize {
        let Some(index) = self.account_orders.get(&account_id) else {
            return 0;
        };
        match scope {
            MassCancelScope::All => index
                .order_count(Side::Buy)
                .saturating_add(index.order_count(Side::Sell)),
            MassCancelScope::Side(side) => index.order_count(side),
        }
    }

    /// Returns one account's selected active order IDs in canonical ascending order.
    ///
    /// This allocates `O(K)` output, traverses only the `K` selected orders, and
    /// sorts them in `O(K log K)` time without auxiliary allocation.
    #[must_use]
    pub fn account_active_order_ids(
        &self,
        account_id: AccountId,
        scope: MassCancelScope,
    ) -> Vec<OrderId> {
        let Some(index) = self.account_orders.get(&account_id) else {
            return Vec::new();
        };
        let selected_count = match scope {
            MassCancelScope::All => index
                .order_count(Side::Buy)
                .saturating_add(index.order_count(Side::Sell)),
            MassCancelScope::Side(side) => index.order_count(side),
        };
        let mut selected = Vec::with_capacity(selected_count);
        match scope {
            MassCancelScope::All => {
                self.append_account_side_ids(index, Side::Buy, &mut selected);
                self.append_account_side_ids(index, Side::Sell, &mut selected);
            }
            MassCancelScope::Side(side) => {
                self.append_account_side_ids(index, side, &mut selected);
            }
        }
        selected.sort_unstable();
        selected
    }

    fn append_account_side_ids(
        &self,
        index: &AccountOrderIndex,
        side: Side,
        selected: &mut Vec<OrderId>,
    ) {
        let mut current = index.head(side);
        for _ in 0..index.order_count(side) {
            let order_id = current.expect("account index ended before its declared count");
            selected.push(order_id);
            current = self
                .orders
                .get(&order_id)
                .expect("account index order must exist")
                .account_next;
        }
        debug_assert!(
            current.is_none(),
            "account index exceeds its declared count"
        );
    }

    /// Audits all index, FIFO-link, aggregate, and spread invariants.
    ///
    /// Successful validation uses `O(1)` auxiliary space and performs no heap
    /// allocation. FIFO/account traversal is `O(O)` for `O` active orders; the
    /// two price indexed-AVL audits are `O(P log(P + 1))` for at most `P`
    /// initialized slots per side, and the expiry-AVL audit is
    /// `O(X log(X + 1))` for `X <= O` initialized slots. Failure-detail
    /// formatting can allocate, so this remains an offline audit rather than a
    /// matching-hot-path operation.
    ///
    /// # Errors
    ///
    /// Returns [`InvariantViolation`] at the first detected corruption.
    pub fn validate(&self) -> Result<(), InvariantViolation> {
        self.validate_capacity()?;
        self.bids.validate_extremum()?;
        self.asks.validate_extremum()?;
        let indexed_by_price = self
            .validate_side(Side::Buy)?
            .checked_add(self.validate_side(Side::Sell)?)
            .ok_or_else(|| InvariantViolation::new("price-index order count is exhausted"))?;
        if indexed_by_price > self.orders.len() {
            return Err(InvariantViolation::new(
                "price levels reference an active order more than once",
            ));
        }
        let expected_resting = self.resting_order_count();
        if indexed_by_price != expected_resting {
            return Err(InvariantViolation::new(format!(
                "{} active orders are absent from price levels",
                expected_resting.abs_diff(indexed_by_price)
            )));
        }
        if self
            .orders
            .keys()
            .any(|order_id| !self.seen_order_ids.contains(order_id))
        {
            return Err(InvariantViolation::new(
                "an active order is absent from the accepted-order index",
            ));
        }
        self.validate_account_order_index()?;
        self.validate_expiry_index()?;
        self.validate_stop_indexes()?;
        if self.orders.values().any(|order| {
            self.account_control(order.account_id).state == AccountAdmissionState::Blocked
        }) {
            return Err(InvariantViolation::new(
                "a blocked account retains an active order",
            ));
        }
        if let (Some(bid), Some(ask)) = (self.best_price(Side::Buy), self.best_price(Side::Sell)) {
            if bid >= ask {
                return Err(InvariantViolation::new(format!(
                    "crossed resting book: best bid {} >= best ask {}",
                    bid.raw(),
                    ask.raw()
                )));
            }
        }
        Ok(())
    }

    fn validate_capacity(&self) -> Result<(), InvariantViolation> {
        self.validate_expiry_capacity()?;
        let reservations = [
            (
                self.orders.capacity(),
                self.limits.max_active_orders(),
                "active-order",
            ),
            (
                self.account_orders.capacity(),
                self.limits.max_active_accounts(),
                "active-account",
            ),
            (
                self.seen_order_ids.capacity(),
                self.limits.max_accepted_order_ids(),
                "accepted-order-identifier",
            ),
            (
                self.account_controls.capacity(),
                self.limits.max_account_controls(),
                "account-control",
            ),
            (
                self.reports.capacity(),
                self.limits.max_retained_commands(),
                "command-history",
            ),
        ];
        for (allocated, configured, name) in reservations {
            if allocated < configured {
                return Err(InvariantViolation::new(format!(
                    "{name} hash capacity {allocated} is below its constructor reservation {configured}"
                )));
            }
        }
        let selection_status = self.order_selection_pool_status();
        if selection_status.configured_leases != self.limits.max_prepared_order_selections()
            || selection_status.available_leases > selection_status.configured_leases
            || selection_status.allocated_order_ids_per_lease < self.limits.max_active_orders()
        {
            return Err(InvariantViolation::new(
                "order-selection pool reservation is below its configured maximum",
            ));
        }
        self.validate_hash_layouts()?;
        let checks = [
            (
                self.orders.len(),
                self.limits.max_active_orders(),
                "active-order",
            ),
            (
                self.account_orders.len(),
                self.limits.max_active_accounts(),
                "active-account",
            ),
            (
                self.seen_order_ids.len(),
                self.limits.max_accepted_order_ids(),
                "accepted-order-identifier",
            ),
            (
                self.account_controls.len(),
                self.limits.max_account_controls(),
                "account-control",
            ),
            (
                self.reports.len(),
                self.limits.max_retained_commands(),
                "command-history",
            ),
            (
                self.bids.len(),
                self.limits.max_price_levels_per_side(),
                "bid-price-level",
            ),
            (
                self.asks.len(),
                self.limits.max_price_levels_per_side(),
                "ask-price-level",
            ),
        ];
        for (observed, limit, name) in checks {
            if observed > limit {
                return Err(InvariantViolation::new(format!(
                    "{name} cardinality {observed} exceeds configured capacity {limit}"
                )));
            }
        }
        self.validate_event_capacity()?;
        if self
            .account_controls
            .values()
            .any(|control| control.revision == 0)
        {
            return Err(InvariantViolation::new(
                "retained account control has the reserved genesis revision",
            ));
        }
        Ok(())
    }

    fn validate_expiry_capacity(&self) -> Result<(), InvariantViolation> {
        if self.expiries.maximum() != self.limits.max_active_orders()
            || self.expiries.allocation_capacity() < self.limits.max_active_orders()
        {
            return Err(InvariantViolation::new(
                "GTD expiry-index reservation is below the active-order maximum",
            ));
        }
        if self.buy_stops.maximum() != self.limits.max_active_orders()
            || self.buy_stops.allocation_capacity() < self.limits.max_active_orders()
        {
            return Err(InvariantViolation::new(
                "buy-stop trigger-index reservation is below the active-order maximum",
            ));
        }
        if self.sell_stops.maximum() != self.limits.max_active_orders()
            || self.sell_stops.allocation_capacity() < self.limits.max_active_orders()
        {
            return Err(InvariantViolation::new(
                "sell-stop trigger-index reservation is below the active-order maximum",
            ));
        }
        Ok(())
    }

    fn validate_stop_indexes(&self) -> Result<(), InvariantViolation> {
        self.buy_stops.validate().map_err(|detail| {
            InvariantViolation::new(format!("buy-stop trigger index is invalid: {detail}"))
        })?;
        self.sell_stops.validate().map_err(|detail| {
            InvariantViolation::new(format!("sell-stop trigger index is invalid: {detail}"))
        })?;
        let mut indexed = 0_usize;
        for (key, &order_id) in self.buy_stops.iter() {
            let order = self.validate_stop_index_entry(order_id, Side::Buy)?;
            let stop = order.stop.expect("validated dormant stop state exists");
            if key.trigger_price != stop.trigger_price
                || key.priority_sequence != stop.priority_sequence
                || key.order_id != order_id
            {
                return Err(InvariantViolation::new(format!(
                    "buy-stop index disagrees with order {order_id}"
                )));
            }
            indexed = indexed
                .checked_add(1)
                .ok_or_else(|| InvariantViolation::new("stop-index count is exhausted"))?;
        }
        for (key, &order_id) in self.sell_stops.iter() {
            let order = self.validate_stop_index_entry(order_id, Side::Sell)?;
            let stop = order.stop.expect("validated dormant stop state exists");
            if key.trigger_price.0 != stop.trigger_price
                || key.priority_sequence != stop.priority_sequence
                || key.order_id != order_id
            {
                return Err(InvariantViolation::new(format!(
                    "sell-stop index disagrees with order {order_id}"
                )));
            }
            indexed = indexed
                .checked_add(1)
                .ok_or_else(|| InvariantViolation::new("stop-index count is exhausted"))?;
        }
        if indexed != self.dormant_stop_count()
            || indexed
                != self
                    .orders
                    .values()
                    .filter(|order| order.is_dormant_stop())
                    .count()
        {
            return Err(InvariantViolation::new(
                "dormant stop cardinality differs from trigger-index coverage",
            ));
        }
        if indexed != 0 && self.stop_reference_price.is_none() {
            return Err(InvariantViolation::new(
                "dormant stops exist before stop-reference initialization",
            ));
        }
        let eligible_buys = self.stop_reference_price.map_or(0, |reference| {
            self.buy_stops
                .iter()
                .take_while(|(key, _)| key.trigger_price <= reference)
                .count()
        });
        let eligible_sells = self.stop_reference_price.map_or(0, |reference| {
            self.sell_stops
                .iter()
                .take_while(|(key, _)| key.trigger_price.0 >= reference)
                .count()
        });
        if eligible_buys != 0 && eligible_sells != 0 {
            return Err(InvariantViolation::new(
                "eligible stop backlog exists on both sides",
            ));
        }
        Ok(())
    }

    fn validate_stop_index_entry(
        &self,
        order_id: OrderId,
        side: Side,
    ) -> Result<RestingOrder, InvariantViolation> {
        let order = self.orders.get(&order_id).copied().ok_or_else(|| {
            InvariantViolation::new(format!("stop index references missing order {order_id}"))
        })?;
        if order.side != side
            || order.stop.is_none()
            || order.previous.is_some()
            || order.next.is_some()
            || order.displayed == 0
            || order.displayed > order.leaves
        {
            return Err(InvariantViolation::new(format!(
                "stop index references invalid dormant order {order_id}"
            )));
        }
        Ok(order)
    }

    fn validate_expiry_index(&self) -> Result<(), InvariantViolation> {
        self.expiries.validate().map_err(|detail| {
            InvariantViolation::new(format!("GTD expiry index is invalid: {detail}"))
        })?;
        let mut indexed = 0_usize;
        for (key, &order_id) in self.expiries.iter() {
            if key.order_id != order_id {
                return Err(InvariantViolation::new(
                    "GTD expiry key and indexed order identity differ",
                ));
            }
            let order = self.orders.get(&order_id).ok_or_else(|| {
                InvariantViolation::new(format!(
                    "GTD expiry index references missing order {order_id}"
                ))
            })?;
            if order.expires_at != Some(key.expires_at) {
                return Err(InvariantViolation::new(format!(
                    "GTD expiry index disagrees with order {order_id}"
                )));
            }
            if self
                .expiry_watermark
                .is_some_and(|watermark| key.expires_at <= watermark)
            {
                return Err(InvariantViolation::new(format!(
                    "order {order_id} remains active at or before the expiry watermark"
                )));
            }
            indexed = indexed
                .checked_add(1)
                .ok_or_else(|| InvariantViolation::new("GTD expiry count is exhausted"))?;
        }
        let expected = self
            .orders
            .values()
            .filter(|order| order.expires_at.is_some())
            .count();
        if indexed != expected {
            return Err(InvariantViolation::new(format!(
                "GTD expiry index covers {indexed} orders, expected {expected}"
            )));
        }
        Ok(())
    }

    fn validate_event_capacity(&self) -> Result<(), InvariantViolation> {
        if self.event_arena.capacity() != self.limits.max_retained_events()
            || self.event_arena.allocated_capacity() < self.limits.max_retained_events()
        {
            return Err(InvariantViolation::new(format!(
                "retained-event arena capacity {}/{} differs from configured capacity {}",
                self.event_arena.capacity(),
                self.event_arena.allocated_capacity(),
                self.limits.max_retained_events()
            )));
        }
        if self
            .reports
            .values()
            .any(|cached| cached.report.events.len() > self.limits.max_report_events())
        {
            return Err(InvariantViolation::new(
                "retained report exceeds configured event capacity",
            ));
        }
        let mut expected_event_start = 0_usize;
        for cached in self.reports.values() {
            let Some((start, length)) = cached.report.events.arena_range_for(&self.event_arena)
            else {
                return Err(InvariantViolation::new(
                    "retained report does not reference this book's event arena",
                ));
            };
            if start != expected_event_start {
                return Err(InvariantViolation::new(format!(
                    "retained report event range starts at {start}, expected {expected_event_start}"
                )));
            }
            expected_event_start = expected_event_start.checked_add(length).ok_or_else(|| {
                InvariantViolation::new("retained report event count is not representable")
            })?;
        }
        if expected_event_start != self.retained_event_count
            || self.retained_event_count > self.limits.max_retained_events()
        {
            return Err(InvariantViolation::new(format!(
                "retained event count {} differs from canonical report coverage {} or exceeds {}",
                self.retained_event_count,
                expected_event_start,
                self.limits.max_retained_events()
            )));
        }
        Ok(())
    }

    fn validate_hash_layouts(&self) -> Result<(), InvariantViolation> {
        for (name, layout) in [
            ("active-order", self.orders.validate_layout()),
            ("active-account", self.account_orders.validate_layout()),
            (
                "accepted-order-identifier",
                self.seen_order_ids.validate_layout(),
            ),
            ("account-control", self.account_controls.validate_layout()),
            ("command-history", self.reports.validate_layout()),
        ] {
            if let Err(detail) = layout {
                return Err(InvariantViolation::new(format!(
                    "{name} hash layout is invalid: {detail}"
                )));
            }
        }
        Ok(())
    }

    fn validate_account_order_index(&self) -> Result<(), InvariantViolation> {
        let mut indexed = 0_usize;
        for (&account_id, index) in self.account_orders.iter() {
            if index.is_empty() {
                return Err(InvariantViolation::new(format!(
                    "account {account_id} has an empty active-order index"
                )));
            }
            for side in [Side::Buy, Side::Sell] {
                let side_index = index.side(side);
                if side_index.order_count == 0 {
                    if side_index.head.is_some() || side_index.tail.is_some() {
                        return Err(InvariantViolation::new(format!(
                            "account {account_id} {side:?} has links but no indexed orders"
                        )));
                    }
                } else if side_index.head.is_none() || side_index.tail.is_none() {
                    return Err(InvariantViolation::new(format!(
                        "account {account_id} {side:?} is missing a head or tail link"
                    )));
                }

                let mut event_units = 0_u128;
                let mut current = side_index.head;
                let mut previous = None;
                let mut order_count = 0_usize;
                while let Some(order_id) = current {
                    if indexed >= self.orders.len() {
                        return Err(InvariantViolation::new(format!(
                            "order {order_id} occurs more than once or participates in an account-index cycle"
                        )));
                    }
                    let order = self.orders.get(&order_id).ok_or_else(|| {
                        InvariantViolation::new(format!(
                            "account index references missing order {order_id}"
                        ))
                    })?;
                    if order.account_id != account_id || order.side != side {
                        return Err(InvariantViolation::new(format!(
                            "order {order_id} is indexed under the wrong account or side"
                        )));
                    }
                    if order.account_previous != previous {
                        return Err(InvariantViolation::new(format!(
                            "order {order_id} has a broken previous account link"
                        )));
                    }
                    order_count = order_count.checked_add(1).ok_or_else(|| {
                        InvariantViolation::new("account-index order count is exhausted")
                    })?;
                    indexed = indexed.checked_add(1).ok_or_else(|| {
                        InvariantViolation::new("account-index total order count is exhausted")
                    })?;
                    event_units = event_units
                        .checked_add(order.remaining_event_units())
                        .expect("active account event work must fit u128");
                    previous = Some(order_id);
                    current = order.account_next;
                }
                if previous != side_index.tail {
                    return Err(InvariantViolation::new(format!(
                        "account {account_id} {side:?} has a broken account tail link"
                    )));
                }
                if order_count != side_index.order_count {
                    return Err(InvariantViolation::new(format!(
                        "account {account_id} {side:?} order count {order_count} differs from indexed count {}",
                        side_index.order_count
                    )));
                }
                if event_units != index.event_units(side) {
                    return Err(InvariantViolation::new(format!(
                        "account {account_id} {side:?} event work {} differs from indexed aggregate {}",
                        event_units,
                        index.event_units(side)
                    )));
                }
            }
        }
        if indexed != self.orders.len() {
            return Err(InvariantViolation::new(format!(
                "{} active orders are absent from account indexes",
                self.orders.len() - indexed
            )));
        }
        Ok(())
    }

    fn level_snapshot(price: Price, level: &PriceLevel) -> LevelSnapshot {
        LevelSnapshot {
            price,
            quantity: level.total_quantity,
            order_count: level.visible_order_count,
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one allocation-free pass audits links, display classes, and aggregates"
    )]
    fn validate_side(&self, side: Side) -> Result<usize, InvariantViolation> {
        let mut indexed_orders = 0_usize;
        for (&price, level) in self.levels(side).iter() {
            if level.order_count == 0 {
                return Err(InvariantViolation::new(format!(
                    "empty {side:?} level at price {}",
                    price.raw()
                )));
            }
            let mut current = Some(level.head);
            let mut previous = None;
            let mut count = 0_u64;
            let mut visible_count = 0_u64;
            let mut total = 0_u128;
            let mut event_units = 0_u128;
            let mut displayed_tail = None;
            let mut hidden_class_started = false;
            while let Some(order_id) = current {
                if indexed_orders >= self.orders.len() {
                    return Err(InvariantViolation::new(format!(
                        "order {order_id} is duplicated or participates in a FIFO cycle"
                    )));
                }
                let order = self.orders.get(&order_id).ok_or_else(|| {
                    InvariantViolation::new(format!("level references missing order {order_id}"))
                })?;
                if order.side != side || order.price != price {
                    return Err(InvariantViolation::new(format!(
                        "order {order_id} is indexed under the wrong side or price"
                    )));
                }
                if order.is_dormant_stop() {
                    return Err(InvariantViolation::new(format!(
                        "dormant stop {order_id} appears in a price level"
                    )));
                }
                if order.previous != previous {
                    return Err(InvariantViolation::new(format!(
                        "order {order_id} has a broken previous FIFO link"
                    )));
                }
                if order.displayed == 0 || order.displayed > order.leaves {
                    return Err(InvariantViolation::new(format!(
                        "order {order_id} has invalid working/total leaves"
                    )));
                }
                match order.display {
                    OrderDisplay::FullyDisplayed if order.displayed != order.leaves => {
                        return Err(InvariantViolation::new(format!(
                            "fully displayed order {order_id} hides leaves"
                        )));
                    }
                    OrderDisplay::Reserve { peak } if order.displayed > peak.lots() => {
                        return Err(InvariantViolation::new(format!(
                            "reserve order {order_id} exceeds its display peak"
                        )));
                    }
                    OrderDisplay::Hidden if order.displayed != order.leaves => {
                        return Err(InvariantViolation::new(format!(
                            "hidden order {order_id} has a partial working quantity"
                        )));
                    }
                    _ => {}
                }
                if order.display.is_hidden() {
                    hidden_class_started = true;
                } else {
                    if hidden_class_started {
                        return Err(InvariantViolation::new(format!(
                            "displayed-priority order {order_id} follows hidden liquidity"
                        )));
                    }
                    displayed_tail = Some(order_id);
                    visible_count += 1;
                    total += u128::from(order.displayed);
                }
                count += 1;
                indexed_orders = indexed_orders.checked_add(1).ok_or_else(|| {
                    InvariantViolation::new("price-index total order count is exhausted")
                })?;
                event_units = event_units
                    .checked_add(order.remaining_event_units())
                    .expect("active level event work must fit u128");
                previous = Some(order_id);
                current = order.next;
            }
            if previous != Some(level.tail) {
                return Err(InvariantViolation::new(format!(
                    "{side:?} level at {} has an incorrect tail",
                    price.raw()
                )));
            }
            if displayed_tail != level.displayed_tail {
                return Err(InvariantViolation::new(format!(
                    "{side:?} level at {} has an incorrect displayed-class tail",
                    price.raw()
                )));
            }
            if count != level.order_count
                || visible_count != level.visible_order_count
                || total != level.total_quantity
                || event_units != level.event_units
            {
                return Err(InvariantViolation::new(format!(
                    "{side:?} level at {} has inconsistent aggregates",
                    price.raw()
                )));
            }
        }
        Ok(indexed_orders)
    }

    fn apply_new(
        &mut self,
        command: NewOrder,
        mut events: EventTraceBuilder,
    ) -> Result<ExecutionReport, MatchingError> {
        assert!(
            self.seen_order_ids.len() < self.limits.max_accepted_order_ids(),
            "new-order preflight must reserve accepted-order identity capacity"
        );
        assert!(
            self.seen_order_ids.insert(command.order_id),
            "business preflight must reject reused order identifiers"
        );
        let priority_sequence = self.next_sequence;
        self.push_event(
            &mut events,
            command.command_id,
            command.received_at,
            EventKind::OrderAccepted {
                order_id: command.order_id,
                quantity: command.quantity,
                display: command.display,
            },
        )?;

        if let OrderType::Stop {
            trigger_price,
            activation,
        } = command.order_type
        {
            let displayed = command.display.working_lots(command.quantity.lots());
            self.append_dormant_stop(RestingOrder {
                order_id: command.order_id,
                account_id: command.account_id,
                side: command.side,
                price: activation.risk_price().unwrap_or(trigger_price),
                leaves: command.quantity.lots(),
                displayed,
                display: command.display,
                self_trade_prevention: command.self_trade_prevention,
                expires_at: command.time_in_force.expires_at(),
                stop: Some(DormantStopState {
                    trigger_price,
                    activation,
                    time_in_force: command.time_in_force,
                    priority_sequence,
                }),
                previous: None,
                next: None,
                account_previous: None,
                account_next: None,
            });
            self.push_event(
                &mut events,
                command.command_id,
                command.received_at,
                EventKind::StopOrderArmed {
                    order_id: command.order_id,
                    trigger_price,
                    activation,
                    priority_sequence,
                },
            )?;
            return Ok(ExecutionReport {
                command_id: command.command_id,
                outcome: CommandOutcome::Accepted,
                events: events.finish(),
                replayed: false,
            });
        }

        let incoming = IncomingOrder {
            order_id: command.order_id,
            account_id: command.account_id,
            side: command.side,
            order_type: command.order_type.active(),
            leaves: command.quantity.lots(),
            display: command.display,
            self_trade_prevention: command.self_trade_prevention,
        };
        self.execute_incoming(
            incoming,
            command.time_in_force,
            command.command_id,
            command.received_at,
            &mut events,
        )?;

        Ok(ExecutionReport {
            command_id: command.command_id,
            outcome: CommandOutcome::Accepted,
            events: events.finish(),
            replayed: false,
        })
    }

    fn apply_cancel(
        &mut self,
        command: CancelOrder,
        mut events: EventTraceBuilder,
    ) -> Result<ExecutionReport, MatchingError> {
        let removed = self.remove_order(command.order_id);
        self.push_event(
            &mut events,
            command.command_id,
            command.received_at,
            EventKind::OrderCancelled {
                order_id: command.order_id,
                quantity: Quantity::new(removed.leaves)
                    .expect("resting-order invariant requires non-zero leaves"),
                reason: CancelReason::UserRequested,
            },
        )?;
        Ok(ExecutionReport {
            command_id: command.command_id,
            outcome: CommandOutcome::Accepted,
            events: events.finish(),
            replayed: false,
        })
    }

    fn apply_mass_cancel(
        &mut self,
        command: MassCancel,
        mut events: EventTraceBuilder,
        mut selected: PooledOrderSelection,
    ) -> Result<ExecutionReport, MatchingError> {
        let selected_count = self.account_active_order_count(command.account_id, command.scope);
        debug_assert_eq!(events.maximum_events, selected_count + 1);
        if selected.capacity() < selected_count {
            return Err(MatchingError::InternalInvariantViolation);
        }
        let cancelled_order_count =
            u64::try_from(selected_count).map_err(|_| MatchingError::SequenceExhausted)?;
        let selected_values = selected.values_mut();
        let selected_buffer = selected_values.as_ptr();
        self.take_account_orders_into(command.account_id, command.scope, selected_values);
        debug_assert_eq!(selected_values.len(), selected_count);
        debug_assert_eq!(selected_values.as_ptr(), selected_buffer);

        let mut cancelled_quantity_lots = 0_u128;
        for order_id in selected_values.drain(..) {
            let removed = self.remove_order_preserving_account_index(order_id);
            cancelled_quantity_lots = cancelled_quantity_lots
                .checked_add(u128::from(removed.leaves))
                .expect("u64-sized active-order set cannot overflow u128 total quantity");
            self.push_cancelled(
                &mut events,
                command.command_id,
                command.received_at,
                order_id,
                removed.leaves,
                CancelReason::MassCancel,
            )?;
        }
        self.push_event(
            &mut events,
            command.command_id,
            command.received_at,
            EventKind::MassCancelCompleted {
                account_id: command.account_id,
                scope: command.scope,
                cancelled_order_count,
                cancelled_quantity_lots,
            },
        )?;
        Ok(ExecutionReport {
            command_id: command.command_id,
            outcome: CommandOutcome::Accepted,
            events: events.finish(),
            replayed: false,
        })
    }

    fn apply_account_control(
        &mut self,
        command: AccountControl,
        mut events: EventTraceBuilder,
        selected: Option<PooledOrderSelection>,
    ) -> Result<ExecutionReport, MatchingError> {
        let previous = self.account_control(command.account_id);
        assert_eq!(
            previous.revision, command.expected_revision,
            "account-control revision was checked during preparation"
        );
        let revision = command
            .expected_revision
            .checked_add(1)
            .expect("account-control revision exhaustion was preflighted");
        let current_state = command.action.resulting_state();
        let (cancelled_order_count, cancelled_quantity_lots) = match command.action {
            AccountControlAction::BlockAndCancel => {
                let mut selected =
                    selected.expect("block-and-cancel preparation owns selection storage");
                let selected_count =
                    self.account_active_order_count(command.account_id, MassCancelScope::All);
                debug_assert_eq!(events.maximum_events, selected_count + 1);
                if selected.capacity() < selected_count {
                    return Err(MatchingError::InternalInvariantViolation);
                }
                let cancelled_order_count =
                    u64::try_from(selected_count).map_err(|_| MatchingError::SequenceExhausted)?;
                let selected_values = selected.values_mut();
                let selected_buffer = selected_values.as_ptr();
                self.take_account_orders_into(
                    command.account_id,
                    MassCancelScope::All,
                    selected_values,
                );
                debug_assert_eq!(selected_values.len(), selected_count);
                debug_assert_eq!(selected_values.as_ptr(), selected_buffer);

                let mut cancelled_quantity_lots = 0_u128;
                for order_id in selected_values.drain(..) {
                    let removed = self.remove_order_preserving_account_index(order_id);
                    cancelled_quantity_lots = cancelled_quantity_lots
                        .checked_add(u128::from(removed.leaves))
                        .expect("u64-sized active-order set cannot overflow u128 total quantity");
                    self.push_cancelled(
                        &mut events,
                        command.command_id,
                        command.received_at,
                        order_id,
                        removed.leaves,
                        CancelReason::AccountControl,
                    )?;
                }
                (cancelled_order_count, cancelled_quantity_lots)
            }
            AccountControlAction::Enable => {
                assert!(
                    selected.is_none(),
                    "enable needs no order-selection storage"
                );
                (0, 0)
            }
        };

        let allocated = self.account_controls.capacity();
        let existed = self.account_controls.contains_key(&command.account_id);
        if !existed {
            assert!(
                self.account_controls.len() < self.limits.max_account_controls(),
                "account-control capacity was preflighted"
            );
            assert!(
                self.account_controls.len() < allocated,
                "constructor must reserve account-control insertion headroom"
            );
        }
        let replaced = self.account_controls.insert(
            command.account_id,
            AccountControlSnapshot {
                state: current_state,
                revision,
            },
        );
        assert_eq!(
            replaced,
            existed.then_some(previous),
            "account-control revision preflight must identify existing state"
        );
        debug_assert_eq!(self.account_controls.capacity(), allocated);
        self.push_event(
            &mut events,
            command.command_id,
            command.received_at,
            EventKind::AccountControlApplied {
                account_id: command.account_id,
                previous_state: previous.state,
                current_state,
                revision,
                cancelled_order_count,
                cancelled_quantity_lots,
            },
        )?;
        Ok(ExecutionReport {
            command_id: command.command_id,
            outcome: CommandOutcome::Accepted,
            events: events.finish(),
            replayed: false,
        })
    }

    fn apply_trading_state_control(
        &mut self,
        command: TradingStateControl,
        mut events: EventTraceBuilder,
        selected: Option<PooledOrderSelection>,
    ) -> Result<ExecutionReport, MatchingError> {
        let previous = self.trading_state;
        assert_eq!(
            previous.revision, command.expected_revision,
            "trading-state revision was checked during preparation"
        );
        let revision = command
            .expected_revision
            .checked_add(1)
            .expect("trading-state revision exhaustion was preflighted");

        let (cancelled_order_count, cancelled_quantity_lots) = match command.action {
            TradingStateControlAction::Transition => {
                assert!(
                    selected.is_none(),
                    "transition-only control needs no order-selection storage"
                );
                (0, 0)
            }
            TradingStateControlAction::TransitionAndCancel => {
                let mut selected =
                    selected.expect("transition-and-cancel preparation owns selection storage");
                let selected_count = self.orders.len();
                debug_assert_eq!(events.maximum_events, selected_count + 1);
                if selected.capacity() < selected_count {
                    return Err(MatchingError::InternalInvariantViolation);
                }
                let selected_values = selected.values_mut();
                let selected_buffer = selected_values.as_ptr();
                selected_values.extend(self.orders.keys().copied());
                selected_values.sort_unstable();
                debug_assert_eq!(selected_values.len(), selected_count);
                debug_assert_eq!(selected_values.as_ptr(), selected_buffer);

                let cancelled_order_count =
                    u64::try_from(selected_count).map_err(|_| MatchingError::SequenceExhausted)?;
                let mut cancelled_quantity_lots = 0_u128;
                for order_id in selected_values.drain(..) {
                    let removed = self.remove_order(order_id);
                    cancelled_quantity_lots = cancelled_quantity_lots
                        .checked_add(u128::from(removed.leaves))
                        .expect("u64-sized active-order set cannot overflow u128 total quantity");
                    self.push_cancelled(
                        &mut events,
                        command.command_id,
                        command.received_at,
                        order_id,
                        removed.leaves,
                        CancelReason::TradingStateControl,
                    )?;
                }
                (cancelled_order_count, cancelled_quantity_lots)
            }
        };

        self.trading_state = TradingStateSnapshot::from_parts(command.target_state, revision);
        self.push_event(
            &mut events,
            command.command_id,
            command.received_at,
            EventKind::TradingStateControlApplied {
                previous_state: previous.state,
                current_state: command.target_state,
                revision,
                cancelled_order_count,
                cancelled_quantity_lots,
            },
        )?;
        Ok(ExecutionReport {
            command_id: command.command_id,
            outcome: CommandOutcome::Accepted,
            events: events.finish(),
            replayed: false,
        })
    }

    fn apply_expiry_sweep(
        &mut self,
        command: ExpirySweep,
        mut events: EventTraceBuilder,
        mut selected: PooledOrderSelection,
    ) -> Result<ExecutionReport, MatchingError> {
        let previous_watermark = self.expiry_watermark;
        debug_assert!(previous_watermark.is_none_or(|value| command.through >= value));
        let selected_count = self.expiring_order_count(command.through);
        debug_assert_eq!(events.maximum_events, selected_count + 1);
        if selected.capacity() < selected_count {
            return Err(MatchingError::InternalInvariantViolation);
        }
        let selected_values = selected.values_mut();
        let selected_buffer = selected_values.as_ptr();
        selected_values.extend(
            self.expiries
                .iter()
                .take_while(|(key, _)| key.expires_at <= command.through)
                .map(|(_, &order_id)| order_id),
        );
        debug_assert_eq!(selected_values.len(), selected_count);
        debug_assert_eq!(selected_values.as_ptr(), selected_buffer);

        let expired_order_count =
            u64::try_from(selected_count).map_err(|_| MatchingError::SequenceExhausted)?;
        let mut expired_quantity_lots = 0_u128;
        for order_id in selected_values.drain(..) {
            let removed = self.remove_order(order_id);
            expired_quantity_lots = expired_quantity_lots
                .checked_add(u128::from(removed.leaves))
                .expect("u64-sized active-order set cannot overflow u128 total quantity");
            self.push_cancelled(
                &mut events,
                command.command_id,
                command.received_at,
                order_id,
                removed.leaves,
                CancelReason::Expired,
            )?;
        }
        self.expiry_watermark = Some(command.through);
        self.push_event(
            &mut events,
            command.command_id,
            command.received_at,
            EventKind::ExpirySweepCompleted {
                previous_watermark,
                current_watermark: command.through,
                expired_order_count,
                expired_quantity_lots,
            },
        )?;
        Ok(ExecutionReport {
            command_id: command.command_id,
            outcome: CommandOutcome::Accepted,
            events: events.finish(),
            replayed: false,
        })
    }

    fn expiring_order_count(&self, through: TimestampNs) -> usize {
        self.expiries
            .iter()
            .take_while(|(key, _)| key.expires_at <= through)
            .count()
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one atomic trigger command applies the complete activation-time execution grammar"
    )]
    fn apply_stop_trigger_sweep(
        &mut self,
        command: StopTriggerSweep,
        mut events: EventTraceBuilder,
        mut selected: PooledOrderSelection,
    ) -> Result<ExecutionReport, MatchingError> {
        let previous_reference_price = self.stop_reference_price;
        let selected_count = self.eligible_stop_count(command.reference_price).min(
            usize::try_from(command.maximum_orders)
                .map_err(|_| MatchingError::InternalInvariantViolation)?,
        );
        if selected.capacity() < selected_count {
            return Err(MatchingError::InternalInvariantViolation);
        }
        let selected_values = selected.values_mut();
        let selected_buffer = selected_values.as_ptr();
        self.for_each_eligible_stop(command.reference_price, selected_count, |order_id| {
            selected_values.push(order_id);
            Ok::<(), MatchingError>(())
        })?;
        debug_assert_eq!(selected_values.len(), selected_count);
        debug_assert_eq!(selected_values.as_ptr(), selected_buffer);

        for order_id in selected_values.drain(..) {
            let removed = self.remove_order(order_id);
            let stop = removed
                .stop
                .expect("selected trigger target must be a dormant stop");
            self.push_event(
                &mut events,
                command.command_id,
                command.received_at,
                EventKind::StopOrderTriggered {
                    order_id,
                    trigger_price: stop.trigger_price,
                    reference_price: command.reference_price,
                    priority_sequence: stop.priority_sequence,
                },
            )?;

            let active_order_type = stop.activation.order_type();
            let incoming = IncomingOrder {
                order_id,
                account_id: removed.account_id,
                side: removed.side,
                order_type: active_order_type,
                leaves: removed.leaves,
                display: removed.display,
                self_trade_prevention: removed.self_trade_prevention,
            };
            if stop.time_in_force == TimeInForce::FillOrKill {
                let candidate = NewOrder {
                    command_id: command.command_id,
                    order_id,
                    account_id: removed.account_id,
                    instrument_id: command.instrument_id,
                    instrument_version: command.instrument_version,
                    side: removed.side,
                    quantity: Quantity::new(removed.leaves)
                        .expect("dormant stop leaves must be non-zero"),
                    display: removed.display,
                    order_type: active_order_type,
                    time_in_force: TimeInForce::FillOrKill,
                    self_trade_prevention: removed.self_trade_prevention,
                    received_at: command.received_at,
                };
                if !self.can_fill(&candidate) {
                    self.push_cancelled(
                        &mut events,
                        command.command_id,
                        command.received_at,
                        order_id,
                        removed.leaves,
                        CancelReason::TriggeredFokUnfilled,
                    )?;
                    continue;
                }
            }
            if stop.time_in_force == TimeInForce::PostOnly
                && self.would_cross(removed.side, active_order_type)
            {
                self.push_cancelled(
                    &mut events,
                    command.command_id,
                    command.received_at,
                    order_id,
                    removed.leaves,
                    CancelReason::TriggeredPostOnlyWouldCross,
                )?;
                continue;
            }
            if !self.triggered_residual_has_capacity(incoming, stop.time_in_force) {
                self.push_cancelled(
                    &mut events,
                    command.command_id,
                    command.received_at,
                    order_id,
                    removed.leaves,
                    CancelReason::TriggeredCapacityUnavailable,
                )?;
                continue;
            }
            self.execute_incoming(
                incoming,
                stop.time_in_force,
                command.command_id,
                command.received_at,
                &mut events,
            )?;
        }

        self.stop_reference_price = Some(command.reference_price);
        let remaining = self.eligible_stop_count(command.reference_price);
        self.push_event(
            &mut events,
            command.command_id,
            command.received_at,
            EventKind::StopTriggerSweepCompleted {
                previous_reference_price,
                current_reference_price: command.reference_price,
                triggered_order_count: u64::try_from(selected_count)
                    .map_err(|_| MatchingError::SequenceExhausted)?,
                remaining_eligible_order_count: u64::try_from(remaining)
                    .map_err(|_| MatchingError::SequenceExhausted)?,
            },
        )?;
        Ok(ExecutionReport {
            command_id: command.command_id,
            outcome: CommandOutcome::Accepted,
            events: events.finish(),
            replayed: false,
        })
    }

    fn triggered_residual_has_capacity(
        &self,
        incoming: IncomingOrder,
        time_in_force: TimeInForce,
    ) -> bool {
        let OrderType::Limit(price) = incoming.order_type else {
            return true;
        };
        if !time_in_force.may_rest() || self.levels(incoming.side).get(price).is_some() {
            return true;
        }
        if self.levels(incoming.side).len() < self.limits.max_price_levels_per_side() {
            return true;
        }
        self.resting_residual_removed_orders(IncomingPreview {
            account_id: incoming.account_id,
            side: incoming.side,
            order_type: incoming.order_type,
            leaves: incoming.leaves,
            self_trade_prevention: incoming.self_trade_prevention,
        })
        .is_none()
    }

    fn apply_replace(
        &mut self,
        command: ReplaceOrder,
        mut events: EventTraceBuilder,
    ) -> Result<ExecutionReport, MatchingError> {
        let old = self
            .orders
            .get(&command.order_id)
            .copied()
            .expect("prechecked replacement order must exist");
        if old.is_dormant_stop() {
            return self.apply_replace_dormant_stop(command, old, events);
        }
        let new_quantity = command.new_quantity.lots();
        let priority_retained = old.price == command.new_price
            && new_quantity <= old.leaves
            && old.display == command.new_display;
        self.push_event(
            &mut events,
            command.command_id,
            command.received_at,
            EventKind::OrderReplaced {
                order_id: command.order_id,
                old_price: old.price,
                new_price: command.new_price,
                old_quantity: Quantity::new(old.leaves)
                    .expect("resting-order invariant requires non-zero leaves"),
                new_quantity: command.new_quantity,
                old_display: old.display,
                new_display: command.new_display,
                priority_retained,
            },
        )?;

        if priority_retained {
            let new_displayed = old.displayed.min(new_quantity);
            let displayed_reduction = old.displayed - new_displayed;
            let visible_reduction = old.display.visible_lots(displayed_reduction);
            let new = RestingOrder {
                leaves: new_quantity,
                displayed: new_displayed,
                ..old
            };
            let old_event_units = old.remaining_event_units();
            let new_event_units = new.remaining_event_units();
            self.levels_mut(old.side)
                .update(old.price, |level| {
                    level.total_quantity -= u128::from(visible_reduction);
                    level.event_units = level
                        .event_units
                        .checked_sub(old_event_units)
                        .and_then(|value| value.checked_add(new_event_units))
                        .expect("replacement event work must fit level aggregate");
                })
                .expect("resting order must reference an existing level");
            self.account_orders
                .get_mut(&old.account_id)
                .expect("replacement account index must exist")
                .replace_event_units(old.side, old_event_units, new_event_units);
            let order = self
                .orders
                .get_mut(&command.order_id)
                .expect("validated order must exist");
            *order = new;
        } else {
            self.remove_order(command.order_id);
            let incoming = IncomingOrder {
                order_id: command.order_id,
                account_id: old.account_id,
                side: old.side,
                order_type: OrderType::Limit(command.new_price),
                leaves: new_quantity,
                display: command.new_display,
                self_trade_prevention: old.self_trade_prevention,
            };
            self.execute_incoming(
                incoming,
                old.expires_at
                    .map_or(TimeInForce::GoodTilCancelled, |expires_at| {
                        TimeInForce::GoodTilTimestamp { expires_at }
                    }),
                command.command_id,
                command.received_at,
                &mut events,
            )?;
        }

        Ok(ExecutionReport {
            command_id: command.command_id,
            outcome: CommandOutcome::Accepted,
            events: events.finish(),
            replayed: false,
        })
    }

    fn apply_replace_dormant_stop(
        &mut self,
        command: ReplaceOrder,
        old: RestingOrder,
        mut events: EventTraceBuilder,
    ) -> Result<ExecutionReport, MatchingError> {
        let old_stop = old
            .stop
            .expect("dormant replacement must retain stop state");
        debug_assert!(matches!(old_stop.activation, StopActivation::Limit(_)));
        let new_quantity = command.new_quantity.lots();
        let priority_retained = old.price == command.new_price
            && new_quantity <= old.leaves
            && old.display == command.new_display;
        let replacement_sequence = self.next_sequence;
        self.push_event(
            &mut events,
            command.command_id,
            command.received_at,
            EventKind::OrderReplaced {
                order_id: command.order_id,
                old_price: old.price,
                new_price: command.new_price,
                old_quantity: Quantity::new(old.leaves)
                    .expect("dormant stop leaves must be non-zero"),
                new_quantity: command.new_quantity,
                old_display: old.display,
                new_display: command.new_display,
                priority_retained,
            },
        )?;

        let mut updated = old;
        updated.price = command.new_price;
        updated.leaves = new_quantity;
        updated.displayed = command.new_display.working_lots(new_quantity);
        updated.display = command.new_display;
        let updated_priority = if priority_retained {
            old_stop.priority_sequence
        } else {
            replacement_sequence
        };
        updated.stop = Some(DormantStopState {
            activation: StopActivation::Limit(command.new_price),
            priority_sequence: updated_priority,
            ..old_stop
        });

        if !priority_retained {
            let removed = match old.side {
                Side::Buy => self.buy_stops.remove(&BuyStopKey {
                    trigger_price: old_stop.trigger_price,
                    priority_sequence: old_stop.priority_sequence,
                    order_id: old.order_id,
                }),
                Side::Sell => self.sell_stops.remove(&SellStopKey {
                    trigger_price: Reverse(old_stop.trigger_price),
                    priority_sequence: old_stop.priority_sequence,
                    order_id: old.order_id,
                }),
            };
            assert_eq!(removed, Some(old.order_id));
            match old.side {
                Side::Buy => {
                    assert!(
                        self.buy_stops
                            .insert(
                                BuyStopKey {
                                    trigger_price: old_stop.trigger_price,
                                    priority_sequence: updated_priority,
                                    order_id: old.order_id,
                                },
                                old.order_id,
                            )
                            .is_none()
                    );
                }
                Side::Sell => {
                    assert!(
                        self.sell_stops
                            .insert(
                                SellStopKey {
                                    trigger_price: Reverse(old_stop.trigger_price),
                                    priority_sequence: updated_priority,
                                    order_id: old.order_id,
                                },
                                old.order_id,
                            )
                            .is_none()
                    );
                }
            }
        }
        *self
            .orders
            .get_mut(&command.order_id)
            .expect("dormant replacement target must remain active") = updated;
        Ok(ExecutionReport {
            command_id: command.command_id,
            outcome: CommandOutcome::Accepted,
            events: events.finish(),
            replayed: false,
        })
    }

    fn rejected(
        &mut self,
        command_id: CommandId,
        occurred_at: TimestampNs,
        reason: RejectReason,
        mut events: EventTraceBuilder,
    ) -> Result<ExecutionReport, MatchingError> {
        self.push_event(
            &mut events,
            command_id,
            occurred_at,
            EventKind::CommandRejected(reason),
        )?;
        Ok(ExecutionReport {
            command_id,
            outcome: CommandOutcome::Rejected(reason),
            events: events.finish(),
            replayed: false,
        })
    }

    fn cache_report(&mut self, command: Command, report: &ExecutionReport) {
        assert!(
            self.reports.len() < self.limits.max_retained_commands(),
            "command preflight must reserve retained-history capacity"
        );
        assert!(
            report.events.arena_range_for(&self.event_arena)
                == Some((self.retained_event_count, report.events.len())),
            "committed report must publish the next canonical event-arena range"
        );
        let retained_event_count = self
            .retained_event_count
            .checked_add(report.events.len())
            .expect("prepared retained-event count must be representable");
        assert!(
            retained_event_count <= self.limits.max_retained_events(),
            "event preflight must reserve retained-event capacity"
        );
        assert!(
            self.reports
                .insert(
                    command.command_id(),
                    CachedReport {
                        command,
                        report: report.clone(),
                    },
                )
                .is_none(),
            "command preflight must reject cached identifiers"
        );
        self.retained_event_count = retained_event_count;
    }

    fn execute_incoming(
        &mut self,
        mut incoming: IncomingOrder,
        time_in_force: TimeInForce,
        command_id: CommandId,
        occurred_at: TimestampNs,
        events: &mut EventTraceBuilder,
    ) -> Result<(), MatchingError> {
        while incoming.leaves > 0 {
            let Some((resting_id, level_handle)) =
                self.best_opposite_order(incoming.side, incoming.order_type)
            else {
                break;
            };
            let resting = *self
                .orders
                .get(&resting_id)
                .expect("price-level head must reference an active order");

            if resting.account_id == incoming.account_id {
                self.prevent_self_trade(
                    &mut incoming,
                    resting,
                    level_handle,
                    command_id,
                    occurred_at,
                    events,
                )?;
                continue;
            }

            let executed = incoming.leaves.min(resting.displayed);
            let trade = self.make_trade(&incoming, &resting, executed)?;
            self.push_event(events, command_id, occurred_at, EventKind::Trade(trade))?;
            incoming.leaves -= executed;
            if let Some(refresh) = self.decrement_resting(resting_id, executed, level_handle) {
                self.push_refresh(events, command_id, occurred_at, refresh)?;
            }
        }

        if incoming.leaves == 0 {
            return Ok(());
        }
        match (incoming.order_type, time_in_force) {
            (
                OrderType::Limit(price),
                TimeInForce::GoodTilCancelled
                | TimeInForce::GoodTilTimestamp { .. }
                | TimeInForce::PostOnly,
            ) => {
                let displayed = incoming.display.working_lots(incoming.leaves);
                self.append_order(RestingOrder {
                    order_id: incoming.order_id,
                    account_id: incoming.account_id,
                    side: incoming.side,
                    price,
                    leaves: incoming.leaves,
                    displayed,
                    display: incoming.display,
                    self_trade_prevention: incoming.self_trade_prevention,
                    expires_at: time_in_force.expires_at(),
                    stop: None,
                    previous: None,
                    next: None,
                    account_previous: None,
                    account_next: None,
                });
                self.push_event(
                    events,
                    command_id,
                    occurred_at,
                    EventKind::OrderRested {
                        order_id: incoming.order_id,
                        price,
                        leaves_quantity: Quantity::new(incoming.leaves)
                            .expect("resting quantity must be non-zero"),
                        working_quantity: Quantity::new(displayed)
                            .expect("working resting quantity must be non-zero"),
                    },
                )?;
            }
            _ => {
                self.push_cancelled(
                    events,
                    command_id,
                    occurred_at,
                    incoming.order_id,
                    incoming.leaves,
                    CancelReason::UnfilledRemainder,
                )?;
            }
        }
        Ok(())
    }

    fn prevent_self_trade(
        &mut self,
        incoming: &mut IncomingOrder,
        resting: RestingOrder,
        level_handle: PriceLevelHandle,
        command_id: CommandId,
        occurred_at: TimestampNs,
        events: &mut EventTraceBuilder,
    ) -> Result<(), MatchingError> {
        let prevented = incoming.leaves.min(resting.displayed);
        self.push_event(
            events,
            command_id,
            occurred_at,
            EventKind::SelfTradePrevented {
                aggressor_order_id: incoming.order_id,
                resting_order_id: resting.order_id,
                quantity: Quantity::new(prevented).expect("prevented quantity must be non-zero"),
                policy: incoming.self_trade_prevention,
            },
        )?;
        match incoming.self_trade_prevention {
            SelfTradePrevention::CancelAggressor => self.cancel_incoming(
                incoming,
                CancelReason::SelfTradeAggressor,
                command_id,
                occurred_at,
                events,
            )?,
            SelfTradePrevention::CancelResting => {
                let removed = self.remove_order_at_level(resting.order_id, level_handle);
                self.push_cancelled(
                    events,
                    command_id,
                    occurred_at,
                    removed.order_id,
                    removed.leaves,
                    CancelReason::SelfTradeResting,
                )?;
            }
            SelfTradePrevention::CancelBoth => {
                let removed = self.remove_order_at_level(resting.order_id, level_handle);
                self.push_cancelled(
                    events,
                    command_id,
                    occurred_at,
                    removed.order_id,
                    removed.leaves,
                    CancelReason::SelfTradeResting,
                )?;
                self.cancel_incoming(
                    incoming,
                    CancelReason::SelfTradeAggressor,
                    command_id,
                    occurred_at,
                    events,
                )?;
            }
            SelfTradePrevention::DecrementAndCancel => {
                incoming.leaves -= prevented;
                if let Some(refresh) =
                    self.decrement_resting(resting.order_id, prevented, level_handle)
                {
                    self.push_refresh(events, command_id, occurred_at, refresh)?;
                }
            }
        }
        Ok(())
    }

    fn cancel_incoming(
        &mut self,
        incoming: &mut IncomingOrder,
        reason: CancelReason,
        command_id: CommandId,
        occurred_at: TimestampNs,
        events: &mut EventTraceBuilder,
    ) -> Result<(), MatchingError> {
        let quantity = incoming.leaves;
        incoming.leaves = 0;
        self.push_cancelled(
            events,
            command_id,
            occurred_at,
            incoming.order_id,
            quantity,
            reason,
        )
    }

    fn push_cancelled(
        &mut self,
        events: &mut EventTraceBuilder,
        command_id: CommandId,
        occurred_at: TimestampNs,
        order_id: OrderId,
        quantity: u64,
        reason: CancelReason,
    ) -> Result<(), MatchingError> {
        self.push_event(
            events,
            command_id,
            occurred_at,
            EventKind::OrderCancelled {
                order_id,
                quantity: Quantity::new(quantity).expect("cancelled quantity must be non-zero"),
                reason,
            },
        )
    }

    fn push_refresh(
        &mut self,
        events: &mut EventTraceBuilder,
        command_id: CommandId,
        occurred_at: TimestampNs,
        refresh: ReserveRefresh,
    ) -> Result<(), MatchingError> {
        self.push_event(
            events,
            command_id,
            occurred_at,
            EventKind::OrderRefreshed {
                order_id: refresh.order_id,
                price: refresh.price,
                displayed_quantity: Quantity::new(refresh.displayed)
                    .expect("refreshed display must be non-zero"),
                leaves_quantity: Quantity::new(refresh.leaves)
                    .expect("refreshed total leaves must be non-zero"),
            },
        )
    }

    fn make_trade(
        &mut self,
        incoming: &IncomingOrder,
        resting: &RestingOrder,
        quantity: u64,
    ) -> Result<Trade, MatchingError> {
        let trade_id =
            TradeId::new(self.next_trade_id).map_err(|_| MatchingError::TradeIdExhausted)?;
        self.next_trade_id = self
            .next_trade_id
            .checked_add(1)
            .ok_or(MatchingError::TradeIdExhausted)?;
        let (buy_order_id, sell_order_id, buyer_account_id, seller_account_id) =
            if incoming.side == Side::Buy {
                (
                    incoming.order_id,
                    resting.order_id,
                    incoming.account_id,
                    resting.account_id,
                )
            } else {
                (
                    resting.order_id,
                    incoming.order_id,
                    resting.account_id,
                    incoming.account_id,
                )
            };
        Ok(Trade {
            trade_id,
            instrument_id: self.definition.instrument_id(),
            instrument_version: self.definition.version(),
            price: resting.price,
            quantity: Quantity::new(quantity).expect("executed quantity must be non-zero"),
            buy_order_id,
            sell_order_id,
            buyer_account_id,
            seller_account_id,
            maker_order_id: resting.order_id,
            taker_order_id: incoming.order_id,
        })
    }

    fn push_event(
        &mut self,
        events: &mut EventTraceBuilder,
        command_id: CommandId,
        occurred_at: TimestampNs,
        kind: EventKind,
    ) -> Result<(), MatchingError> {
        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(MatchingError::SequenceExhausted)?;
        events.push(Event {
            sequence,
            command_id,
            occurred_at,
            kind,
        });
        Ok(())
    }

    fn would_cross(&self, side: Side, order_type: OrderType) -> bool {
        match (side, order_type) {
            (_, OrderType::Market) => self.best_price(side.opposite()).is_some(),
            (Side::Buy, OrderType::Limit(limit)) => {
                self.best_price(Side::Sell).is_some_and(|ask| limit >= ask)
            }
            (Side::Sell, OrderType::Limit(limit)) => {
                self.best_price(Side::Buy).is_some_and(|bid| limit <= bid)
            }
            (_, OrderType::Stop { .. }) => {
                unreachable!("dormant stop constraint cannot enter active crossing")
            }
        }
    }

    fn best_opposite_order(
        &self,
        side: Side,
        order_type: OrderType,
    ) -> Option<(OrderId, PriceLevelHandle)> {
        let opposite = side.opposite();
        let (handle, level) = self.levels(opposite).best_level_with_handle()?;
        let price = handle.price;
        let crosses = match (side, order_type) {
            (_, OrderType::Market) => true,
            (Side::Buy, OrderType::Limit(limit)) => limit >= price,
            (Side::Sell, OrderType::Limit(limit)) => limit <= price,
            (_, OrderType::Stop { .. }) => {
                unreachable!("dormant stop constraint cannot enter active matching")
            }
        };
        crosses.then_some((level.head, handle))
    }

    /// Returns the number of makers removed before a non-zero residual rests.
    ///
    /// This follows the same reserve/STP liquidity theorem as FOK inspection and
    /// does not mutate or materialize replenishment slices. A blocking self
    /// encounter cancels the aggressor, cancel-resting excludes self leaves, and
    /// decrement-and-cancel consumes both external and self leaves. Therefore,
    /// if a residual remains, every crossed level inspected by this function was
    /// completely removed and its cached order count is the exact cardinality
    /// released before the residual is appended.
    fn resting_residual_removed_orders(&self, incoming: IncomingPreview) -> Option<usize> {
        let mut remaining = incoming.leaves;
        let mut removed_orders = 0_usize;
        let mut price = self.best_price(incoming.side.opposite());
        while let Some(current_price) = price {
            let crosses = match (incoming.side, incoming.order_type) {
                (_, OrderType::Market) => true,
                (Side::Buy, OrderType::Limit(limit)) => limit >= current_price,
                (Side::Sell, OrderType::Limit(limit)) => limit <= current_price,
                (_, OrderType::Stop { .. }) => {
                    unreachable!("dormant stop constraint cannot enter residual preview")
                }
            };
            if !crosses {
                break;
            }
            let level = self
                .levels(incoming.side.opposite())
                .get(current_price)
                .expect("enumerated price must have a level");
            let liquidity = match incoming.self_trade_prevention {
                SelfTradePrevention::CancelResting => {
                    self.cancel_resting_liquidity(incoming.account_id, level.head, remaining)
                }
                SelfTradePrevention::CancelAggressor | SelfTradePrevention::CancelBoth => {
                    self.blocking_liquidity(incoming.account_id, level.head, remaining)
                }
                SelfTradePrevention::DecrementAndCancel => {
                    self.total_level_liquidity(level.head, remaining)
                }
            };
            match liquidity {
                LevelLiquidity::Filled | LevelLiquidity::BlockedBySelfTrade => return None,
                LevelLiquidity::Remaining(next) => {
                    remaining = next;
                    removed_orders = removed_orders
                        .checked_add(usize::try_from(level.order_count).expect(
                            "one price level cannot contain more orders than addressable memory",
                        ))
                        .expect("crossed order count cannot exceed active order count");
                }
            }
            price = self.next_worse_price(incoming.side.opposite(), current_price);
        }
        debug_assert_ne!(remaining, 0);
        Some(removed_orders)
    }

    /// Counts current accounts that lose every active order before the predicted
    /// residual is appended.
    ///
    /// This is called only after [`Self::resting_residual_removed_orders`]
    /// proves that every crossed opposite level is completely removed. Account
    /// memberships are disjoint, so the scan visits at most every active order
    /// once and requires no auxiliary storage.
    fn resting_residual_released_accounts(&self, incoming: IncomingPreview) -> usize {
        debug_assert!(!self.account_orders.contains_key(&incoming.account_id));
        self.account_orders
            .values()
            .filter(|index| !index.is_empty() && self.account_orders_are_crossed(index, incoming))
            .count()
    }

    fn account_orders_are_crossed(
        &self,
        index: &AccountOrderIndex,
        incoming: IncomingPreview,
    ) -> bool {
        for side in [Side::Buy, Side::Sell] {
            let mut current = index.head(side);
            for _ in 0..index.order_count(side) {
                let order_id = current.expect("account index ended before its declared count");
                let order = self
                    .orders
                    .get(&order_id)
                    .expect("account index order must exist");
                if order.side != incoming.side.opposite()
                    || !match (incoming.side, incoming.order_type) {
                        (_, OrderType::Market) => true,
                        (Side::Buy, OrderType::Limit(limit)) => limit >= order.price,
                        (Side::Sell, OrderType::Limit(limit)) => limit <= order.price,
                        (_, OrderType::Stop { .. }) => {
                            unreachable!("dormant stop constraint cannot enter account preview")
                        }
                    }
                {
                    return false;
                }
                current = order.account_next;
            }
            debug_assert!(
                current.is_none(),
                "account index exceeds its declared count"
            );
        }
        true
    }

    fn can_fill(&self, incoming: &NewOrder) -> bool {
        let mut remaining = incoming.quantity.lots();
        let mut price = self.best_price(incoming.side.opposite());
        while let Some(current_price) = price {
            let crosses = match (incoming.side, incoming.order_type) {
                (_, OrderType::Market) => true,
                (Side::Buy, OrderType::Limit(limit)) => limit >= current_price,
                (Side::Sell, OrderType::Limit(limit)) => limit <= current_price,
                (_, OrderType::Stop { .. }) => {
                    unreachable!("dormant stop constraint cannot enter FOK inspection")
                }
            };
            if !crosses {
                return false;
            }
            let level = self
                .levels(incoming.side.opposite())
                .get(current_price)
                .expect("enumerated price must have a level");
            match self.fok_level_liquidity(incoming, level.head, remaining) {
                LevelLiquidity::Filled => return true,
                LevelLiquidity::Remaining(next) => remaining = next,
                LevelLiquidity::BlockedBySelfTrade => return false,
            }
            price = self.next_worse_price(incoming.side.opposite(), current_price);
        }
        false
    }

    /// Determines FOK liquidity at one price without materializing reserve
    /// slices.
    ///
    /// With no self order, every external order's total leaves are reachable
    /// before matching advances to a worse price. Cancel-resting removes self
    /// orders, so the same total-leaves rule applies to the remaining queue.
    /// Under an aggressor-blocking policy, a self order in the displayed class
    /// blocks reserve slices that requeue behind it. If the first self order is
    /// hidden, every displayed order's total leaves remain reachable because
    /// reserve refreshes stay ahead of the hidden class.
    fn fok_level_liquidity(
        &self,
        incoming: &NewOrder,
        head: OrderId,
        remaining: u64,
    ) -> LevelLiquidity {
        match incoming.self_trade_prevention {
            SelfTradePrevention::CancelResting => {
                self.cancel_resting_liquidity(incoming.account_id, head, remaining)
            }
            SelfTradePrevention::CancelAggressor | SelfTradePrevention::CancelBoth => {
                self.blocking_liquidity(incoming.account_id, head, remaining)
            }
            SelfTradePrevention::DecrementAndCancel => LevelLiquidity::BlockedBySelfTrade,
        }
    }

    fn cancel_resting_liquidity(
        &self,
        account_id: AccountId,
        head: OrderId,
        mut remaining: u64,
    ) -> LevelLiquidity {
        let mut current = Some(head);
        while let Some(order_id) = current {
            let order = self
                .orders
                .get(&order_id)
                .expect("price-level order must exist");
            current = order.next;
            if order.account_id == account_id {
                continue;
            }
            remaining = remaining.saturating_sub(order.leaves);
            if remaining == 0 {
                return LevelLiquidity::Filled;
            }
        }
        LevelLiquidity::Remaining(remaining)
    }

    fn blocking_liquidity(
        &self,
        account_id: AccountId,
        head: OrderId,
        remaining: u64,
    ) -> LevelLiquidity {
        let mut total_path_remaining = remaining;
        let mut pre_barrier_remaining = remaining;
        let mut current = Some(head);
        while let Some(order_id) = current {
            let order = self
                .orders
                .get(&order_id)
                .expect("price-level order must exist");
            current = order.next;
            if order.account_id == account_id {
                if order.display.is_hidden() && total_path_remaining == 0 {
                    return LevelLiquidity::Filled;
                }
                return LevelLiquidity::BlockedBySelfTrade;
            }
            total_path_remaining = total_path_remaining.saturating_sub(order.leaves);
            pre_barrier_remaining = pre_barrier_remaining.saturating_sub(order.displayed);
            if pre_barrier_remaining == 0 {
                return LevelLiquidity::Filled;
            }
        }
        if total_path_remaining == 0 {
            LevelLiquidity::Filled
        } else {
            LevelLiquidity::Remaining(total_path_remaining)
        }
    }

    fn total_level_liquidity(&self, head: OrderId, mut remaining: u64) -> LevelLiquidity {
        let mut current = Some(head);
        while let Some(order_id) = current {
            let order = self
                .orders
                .get(&order_id)
                .expect("price-level order must exist");
            current = order.next;
            remaining = remaining.saturating_sub(order.leaves);
            if remaining == 0 {
                return LevelLiquidity::Filled;
            }
        }
        LevelLiquidity::Remaining(remaining)
    }

    fn next_worse_price(&self, side: Side, current: Price) -> Option<Price> {
        self.levels(side).next_worse(current)
    }

    fn best_price(&self, side: Side) -> Option<Price> {
        self.levels(side).best_price()
    }

    fn levels(&self, side: Side) -> &PriceLevels {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    fn take_account_orders_into(
        &mut self,
        account_id: AccountId,
        scope: MassCancelScope,
        selected: &mut Vec<OrderId>,
    ) {
        debug_assert!(selected.is_empty());
        let Some(index) = self.account_orders.get(&account_id) else {
            return;
        };
        let selected_count = match scope {
            MassCancelScope::All => index
                .order_count(Side::Buy)
                .checked_add(index.order_count(Side::Sell))
                .expect("account orders cannot exceed addressable memory"),
            MassCancelScope::Side(side) => index.order_count(side),
        };
        debug_assert!(selected.capacity() >= selected_count);
        match scope {
            MassCancelScope::All => {
                self.append_account_side_ids(index, Side::Buy, selected);
                self.append_account_side_ids(index, Side::Sell, selected);
            }
            MassCancelScope::Side(side) => {
                self.append_account_side_ids(index, side, selected);
            }
        }
        debug_assert_eq!(selected.len(), selected_count);
        selected.sort_unstable();

        match scope {
            MassCancelScope::All => {
                let removed = self
                    .account_orders
                    .remove(&account_id)
                    .expect("selected account index must exist");
                debug_assert_eq!(
                    removed
                        .order_count(Side::Buy)
                        .checked_add(removed.order_count(Side::Sell)),
                    Some(selected_count)
                );
            }
            MassCancelScope::Side(side) => {
                let index = self
                    .account_orders
                    .get_mut(&account_id)
                    .expect("selected account index must exist");
                let removed = index.take_side(side);
                debug_assert_eq!(removed.order_count, selected_count);
                let remove_account = index.is_empty();
                if remove_account {
                    self.account_orders.remove(&account_id);
                }
            }
        }
    }

    fn levels_mut(&mut self, side: Side) -> &mut PriceLevels {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn append_order(&mut self, mut order: RestingOrder) {
        assert!(
            order.stop.is_none(),
            "resting order cannot retain a stop instruction"
        );
        assert!(
            self.orders.len() < self.limits.max_active_orders(),
            "resting-order preflight must reserve active-order capacity"
        );
        assert!(
            self.account_orders.contains_key(&order.account_id)
                || self.account_orders.len() < self.limits.max_active_accounts(),
            "resting-order preflight must reserve active-account capacity"
        );
        assert!(
            self.levels(order.side).get(order.price).is_some()
                || self.levels(order.side).len() < self.limits.max_price_levels_per_side(),
            "resting-order preflight must reserve price-level capacity"
        );
        self.append_account_membership(&mut order);
        self.append_order_preserving_account_index(order);
    }

    fn append_dormant_stop(&mut self, mut order: RestingOrder) {
        assert!(
            order.stop.is_some(),
            "dormant stop must retain its trigger state"
        );
        assert!(
            self.orders.len() < self.limits.max_active_orders(),
            "stop-order preflight must reserve active-order capacity"
        );
        assert!(
            self.account_orders.contains_key(&order.account_id)
                || self.account_orders.len() < self.limits.max_active_accounts(),
            "stop-order preflight must reserve active-account capacity"
        );
        self.append_account_membership(&mut order);
        let stop = order.stop.expect("dormant stop state exists");
        assert!(
            self.orders.insert(order.order_id, order).is_none(),
            "active order identifier must be unique"
        );
        self.insert_expiry(order);
        match order.side {
            Side::Buy => {
                assert!(
                    self.buy_stops
                        .insert(
                            BuyStopKey {
                                trigger_price: stop.trigger_price,
                                priority_sequence: stop.priority_sequence,
                                order_id: order.order_id,
                            },
                            order.order_id,
                        )
                        .is_none(),
                    "buy-stop trigger identity must be unique"
                );
            }
            Side::Sell => {
                assert!(
                    self.sell_stops
                        .insert(
                            SellStopKey {
                                trigger_price: Reverse(stop.trigger_price),
                                priority_sequence: stop.priority_sequence,
                                order_id: order.order_id,
                            },
                            order.order_id,
                        )
                        .is_none(),
                    "sell-stop trigger identity must be unique"
                );
            }
        }
    }

    fn append_account_membership(&mut self, order: &mut RestingOrder) {
        let event_units = order.remaining_event_units();
        assert!(
            order.account_previous.is_none() && order.account_next.is_none(),
            "new account-index member must have no links"
        );
        let previous = self
            .account_orders
            .get(&order.account_id)
            .and_then(|index| index.tail(order.side));
        order.account_previous = previous;
        if let Some(previous) = previous {
            let previous_order = self
                .orders
                .get_mut(&previous)
                .expect("account-index tail must exist");
            assert_eq!(previous_order.account_id, order.account_id);
            assert_eq!(previous_order.side, order.side);
            assert!(previous_order.account_next.is_none());
            previous_order.account_next = Some(order.order_id);
        }
        if !self.account_orders.contains_key(&order.account_id) {
            assert!(
                self.account_orders
                    .insert(order.account_id, AccountOrderIndex::default())
                    .is_none(),
                "new active account index must be absent"
            );
        }
        let indexed_previous = self
            .account_orders
            .get_mut(&order.account_id)
            .expect("active account index must exist after insertion")
            .append(order.side, order.order_id, event_units);
        assert_eq!(indexed_previous, previous);
    }

    fn append_order_preserving_account_index(&mut self, mut order: RestingOrder) {
        assert!(order.stop.is_none(), "price-level member cannot be dormant");
        let event_units = order.remaining_event_units();
        let visible = order.display.visible_lots(order.displayed);
        let existing_level = self.levels(order.side).handle(order.price).map(|handle| {
            let level = *self
                .levels(order.side)
                .get_by_handle(handle)
                .expect("fresh price-level handle must resolve");
            (handle, level)
        });

        if let Some((handle, level)) = existing_level {
            if order.display.is_hidden() {
                order.previous = Some(level.tail);
                self.orders
                    .get_mut(&level.tail)
                    .expect("price-level tail must exist")
                    .next = Some(order.order_id);
            } else {
                order.previous = level.displayed_tail;
                order.next = level
                    .displayed_tail
                    .and_then(|tail| {
                        self.orders
                            .get(&tail)
                            .expect("displayed-class tail must exist")
                            .next
                    })
                    .or_else(|| level.displayed_tail.is_none().then_some(level.head));
                if let Some(previous) = order.previous {
                    self.orders
                        .get_mut(&previous)
                        .expect("displayed-class tail must exist")
                        .next = Some(order.order_id);
                }
                if let Some(next) = order.next {
                    self.orders
                        .get_mut(&next)
                        .expect("hidden-class head must exist")
                        .previous = Some(order.order_id);
                }
            }
            self.levels_mut(order.side)
                .update_by_handle(handle, |level| {
                    if order.display.is_hidden() {
                        level.tail = order.order_id;
                    } else {
                        if order.previous.is_none() {
                            level.head = order.order_id;
                        }
                        if order.next.is_none() {
                            level.tail = order.order_id;
                        }
                        level.displayed_tail = Some(order.order_id);
                    }
                    level.total_quantity += u128::from(visible);
                    level.order_count += 1;
                    level.visible_order_count += u64::from(!order.display.is_hidden());
                    level.event_units += event_units;
                })
                .expect("existing order level must remain addressable");
        } else {
            self.levels_mut(order.side).insert(
                order.price,
                PriceLevel {
                    head: order.order_id,
                    tail: order.order_id,
                    displayed_tail: (!order.display.is_hidden()).then_some(order.order_id),
                    total_quantity: u128::from(visible),
                    order_count: 1,
                    visible_order_count: u64::from(!order.display.is_hidden()),
                    event_units,
                },
            );
        }
        assert!(
            self.orders.insert(order.order_id, order).is_none(),
            "active order identifier must be unique"
        );
        self.insert_expiry(order);
    }

    fn insert_expiry(&mut self, order: RestingOrder) {
        if let Some(expires_at) = order.expires_at {
            assert!(
                self.expiries
                    .insert(
                        ExpiryKey {
                            expires_at,
                            order_id: order.order_id,
                        },
                        order.order_id,
                    )
                    .is_none(),
                "GTD expiry identity must be unique"
            );
        }
    }

    fn decrement_resting(
        &mut self,
        order_id: OrderId,
        quantity: u64,
        level_handle: PriceLevelHandle,
    ) -> Option<ReserveRefresh> {
        let order = *self
            .orders
            .get(&order_id)
            .expect("decremented order must exist");
        debug_assert!(quantity <= order.displayed);
        debug_assert_eq!(level_handle.price, order.price);
        if quantity == order.leaves {
            self.remove_order_at_level(order_id, level_handle);
            None
        } else if quantity < order.displayed {
            let updated = self
                .orders
                .get_mut(&order_id)
                .expect("decremented order must exist");
            updated.leaves -= quantity;
            updated.displayed -= quantity;
            let visible_reduction = order.display.visible_lots(quantity);
            self.levels_mut(order.side)
                .update_by_handle(level_handle, |level| {
                    level.total_quantity -= u128::from(visible_reduction);
                })
                .expect("resting order must reference a level");
            None
        } else {
            Some(self.refresh_resting(order, quantity, level_handle))
        }
    }

    fn refresh_resting(
        &mut self,
        order: RestingOrder,
        quantity: u64,
        level_handle: PriceLevelHandle,
    ) -> ReserveRefresh {
        debug_assert_eq!(quantity, order.displayed);
        debug_assert!(quantity < order.leaves);
        debug_assert_eq!(level_handle.price, order.price);
        let old_event_units = order.remaining_event_units();
        let mut refreshed = RestingOrder {
            leaves: order.leaves - quantity,
            ..order
        };
        refreshed.displayed = refreshed.display.working_lots(refreshed.leaves);
        let new_event_units = refreshed.remaining_event_units();

        let level = *self
            .levels(order.side)
            .get_by_handle(level_handle)
            .expect("resting order handle must reference its level");
        let move_to_displayed_tail = level.displayed_tail != Some(order.order_id);
        if move_to_displayed_tail {
            let next = order
                .next
                .expect("non-tail displayed order must have a FIFO successor");
            if let Some(previous) = order.previous {
                self.orders
                    .get_mut(&previous)
                    .expect("previous FIFO link must exist")
                    .next = Some(next);
            }
            self.orders
                .get_mut(&next)
                .expect("next FIFO link must exist")
                .previous = order.previous;
            let displayed_tail = level
                .displayed_tail
                .expect("reserve order implies a displayed-class tail");
            debug_assert_ne!(displayed_tail, order.order_id);
            let insertion_next = self
                .orders
                .get(&displayed_tail)
                .expect("displayed-class tail must exist")
                .next;
            self.orders
                .get_mut(&displayed_tail)
                .expect("displayed-class tail must exist")
                .next = Some(order.order_id);
            if let Some(insertion_next) = insertion_next {
                self.orders
                    .get_mut(&insertion_next)
                    .expect("hidden-class head must exist")
                    .previous = Some(order.order_id);
            }
            refreshed.previous = Some(displayed_tail);
            refreshed.next = insertion_next;
        }

        self.levels_mut(order.side)
            .update_by_handle(level_handle, |level| {
                level.total_quantity = level
                    .total_quantity
                    .checked_sub(u128::from(order.display.visible_lots(order.displayed)))
                    .and_then(|total| {
                        total.checked_add(u128::from(
                            refreshed.display.visible_lots(refreshed.displayed),
                        ))
                    })
                    .expect("reserve refresh quantity must fit level aggregate");
                level.event_units = level
                    .event_units
                    .checked_sub(old_event_units)
                    .and_then(|total| total.checked_add(new_event_units))
                    .expect("reserve refresh work must fit level aggregate");
                if move_to_displayed_tail {
                    let next = order
                        .next
                        .expect("moved displayed order must have had a successor");
                    if order.previous.is_none() {
                        level.head = next;
                    }
                    if refreshed.next.is_none() {
                        level.tail = order.order_id;
                    }
                    level.displayed_tail = Some(order.order_id);
                }
            })
            .expect("resting order handle must reference its level");
        self.account_orders
            .get_mut(&order.account_id)
            .expect("refreshed order account index must exist")
            .replace_event_units(order.side, old_event_units, new_event_units);
        *self
            .orders
            .get_mut(&order.order_id)
            .expect("refreshed order must remain active") = refreshed;

        ReserveRefresh {
            order_id: refreshed.order_id,
            price: refreshed.price,
            displayed: refreshed.displayed,
            leaves: refreshed.leaves,
        }
    }

    fn remove_order(&mut self, order_id: OrderId) -> RestingOrder {
        let order = *self
            .orders
            .get(&order_id)
            .expect("validated removal target must exist");
        if order.is_dormant_stop() {
            let removed = self.remove_dormant_stop_preserving_account_index(order_id);
            self.remove_account_membership(removed);
            return removed;
        }
        let level_handle = self
            .levels(order.side)
            .handle(order.price)
            .expect("resting order must reference a level");
        self.remove_order_at_level(order_id, level_handle)
    }

    fn remove_order_at_level(
        &mut self,
        order_id: OrderId,
        level_handle: PriceLevelHandle,
    ) -> RestingOrder {
        let order = self.remove_order_preserving_account_index_at(order_id, level_handle);
        self.remove_account_membership(order);
        order
    }

    fn remove_account_membership(&mut self, order: RestingOrder) {
        if let Some(previous) = order.account_previous {
            self.orders
                .get_mut(&previous)
                .expect("previous account link must exist")
                .account_next = order.account_next;
        }
        if let Some(next) = order.account_next {
            self.orders
                .get_mut(&next)
                .expect("next account link must exist")
                .account_previous = order.account_previous;
        }
        let index = self
            .account_orders
            .get_mut(&order.account_id)
            .expect("active order account index must exist");
        index.remove(
            order.side,
            order.order_id,
            order.account_previous,
            order.account_next,
            order.remaining_event_units(),
        );
        if index.is_empty() {
            self.account_orders.remove(&order.account_id);
        }
    }

    fn remove_order_preserving_account_index(&mut self, order_id: OrderId) -> RestingOrder {
        let order = *self
            .orders
            .get(&order_id)
            .expect("validated removal target must exist");
        if order.is_dormant_stop() {
            return self.remove_dormant_stop_preserving_account_index(order_id);
        }
        let level_handle = self
            .levels(order.side)
            .handle(order.price)
            .expect("resting order must reference a level");
        self.remove_order_preserving_account_index_at(order_id, level_handle)
    }

    fn remove_order_preserving_account_index_at(
        &mut self,
        order_id: OrderId,
        level_handle: PriceLevelHandle,
    ) -> RestingOrder {
        let order = self
            .orders
            .remove(&order_id)
            .expect("validated removal target must exist");
        if let Some(expires_at) = order.expires_at {
            assert_eq!(
                self.expiries.remove(&ExpiryKey {
                    expires_at,
                    order_id,
                }),
                Some(order_id),
                "active GTD order must have one expiry-index entry"
            );
        }
        debug_assert_eq!(level_handle.price, order.price);
        if let Some(previous) = order.previous {
            self.orders
                .get_mut(&previous)
                .expect("previous FIFO link must exist")
                .next = order.next;
        }
        if let Some(next) = order.next {
            self.orders
                .get_mut(&next)
                .expect("next FIFO link must exist")
                .previous = order.previous;
        }

        let level_is_empty = self
            .levels_mut(order.side)
            .update_by_handle(level_handle, |level| {
                level.total_quantity -= u128::from(order.display.visible_lots(order.displayed));
                level.order_count -= 1;
                level.visible_order_count -= u64::from(!order.display.is_hidden());
                level.event_units -= order.remaining_event_units();
                if order.previous.is_none() {
                    if let Some(next) = order.next {
                        level.head = next;
                    }
                }
                if order.next.is_none() {
                    if let Some(previous) = order.previous {
                        level.tail = previous;
                    }
                }
                if level.displayed_tail == Some(order.order_id) {
                    level.displayed_tail = order.previous;
                }
                level.order_count == 0
            })
            .expect("resting order must reference a level");
        if level_is_empty {
            self.levels_mut(order.side)
                .remove_by_handle(level_handle)
                .expect("empty resting level handle must remain valid");
        }
        order
    }

    fn remove_dormant_stop_preserving_account_index(&mut self, order_id: OrderId) -> RestingOrder {
        let order = self
            .orders
            .remove(&order_id)
            .expect("validated dormant-stop removal target must exist");
        let stop = order.stop.expect("dormant stop must retain trigger state");
        if let Some(expires_at) = order.expires_at {
            assert_eq!(
                self.expiries.remove(&ExpiryKey {
                    expires_at,
                    order_id,
                }),
                Some(order_id),
                "active GTD stop must have one expiry-index entry"
            );
        }
        let removed = match order.side {
            Side::Buy => self.buy_stops.remove(&BuyStopKey {
                trigger_price: stop.trigger_price,
                priority_sequence: stop.priority_sequence,
                order_id,
            }),
            Side::Sell => self.sell_stops.remove(&SellStopKey {
                trigger_price: Reverse(stop.trigger_price),
                priority_sequence: stop.priority_sequence,
                order_id,
            }),
        };
        assert_eq!(
            removed,
            Some(order_id),
            "dormant stop must have one trigger-index entry"
        );
        order
    }
}

#[cfg(test)]
mod price_level_index_tests {
    use std::sync::Arc;

    use super::{
        AccountAdmissionState, AccountControl, AccountControlAction, AccountControlSnapshot,
        Command, CommandOutcome, CommandPreparation, EventTraceBuilder, IncomingPreview,
        InvariantViolation, MassCancel, MassCancelScope, MatchingCapacity, MatchingError, NewOrder,
        OrderBook, OrderBookLimits, OrderBookLimitsSpec, OrderDisplay, OrderSelectionPool,
        OrderType, PriceLevel, PriceLevels, RejectReason, ReplaceOrder, RestingOrder,
        SelfTradePrevention, TimeInForce, try_event_arena,
    };
    use crate::instrument::{
        InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
        QuantityRules, ReserveOrderRules, TradingState,
    };
    use crate::{
        AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity,
        Side, TimestampNs,
    };

    struct Generator(u64);

    impl Generator {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0
        }

        fn bounded(&mut self, exclusive_upper: u64) -> u64 {
            self.next() % exclusive_upper
        }
    }

    fn reserve_definition() -> InstrumentDefinition {
        InstrumentDefinition::new(InstrumentSpec {
            instrument_id: InstrumentId::new(1).unwrap(),
            version: InstrumentVersion::new(1).unwrap(),
            effective_from: TimestampNs::from_unix_nanos(0),
            symbol: InstrumentSymbol::new("FOKMODEL").unwrap(),
            kind: InstrumentKind::Future,
            base_asset_id: AssetId::new(1).unwrap(),
            quote_asset_id: AssetId::new(2).unwrap(),
            price: PriceRules::new(0, 1, Price::from_raw(-1_000), Price::from_raw(1_000)).unwrap(),
            quantity: QuantityRules::new(1, 1, u64::MAX).unwrap(),
            reserve: ReserveOrderRules::new(64).unwrap(),
            hidden_orders_supported: true,
            base_units_per_lot: 1,
            quote_units_per_price_unit: 1,
            trading_state: TradingState::Open,
        })
        .unwrap()
    }

    #[test]
    fn unrepresentable_order_selection_storage_is_typed_by_exact_resource() {
        assert!(matches!(
            OrderSelectionPool::try_new(1, usize::MAX),
            Err(MatchingError::CapacityReservationFailed(
                MatchingCapacity::OrderSelectionBuffers
            ))
        ));
        assert!(matches!(
            OrderSelectionPool::try_new(usize::MAX, 1),
            Err(MatchingError::CapacityReservationFailed(
                MatchingCapacity::PreparedOrderSelections
            ))
        ));
    }

    #[test]
    fn order_selection_leases_are_isolated_and_returned_after_drop() {
        let pool = OrderSelectionPool::try_new(2, 4).unwrap();
        let mut first = pool.acquire().unwrap();
        let mut second = pool.acquire().unwrap();
        assert_eq!(pool.available(), 0);
        assert_ne!(first.values().as_ptr(), second.values().as_ptr());
        first.values_mut().push(OrderId::new(1).unwrap());
        second.values_mut().push(OrderId::new(2).unwrap());
        assert_eq!(first.values(), [OrderId::new(1).unwrap()]);
        assert_eq!(second.values(), [OrderId::new(2).unwrap()]);
        drop(first);
        assert_eq!(pool.available(), 1);
        assert_eq!(second.values(), [OrderId::new(2).unwrap()]);
        drop(second);
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn audit_rejects_reordered_dense_history_before_checkpoint_capture() {
        let mut book = OrderBook::new(reserve_definition());
        for (command_id, account_id) in [(1_u64, 11_u64), (2, 12)] {
            book.submit(Command::AccountControl(AccountControl {
                command_id: CommandId::new(command_id).unwrap(),
                account_id: AccountId::new(account_id).unwrap(),
                instrument_id: InstrumentId::new(1).unwrap(),
                instrument_version: InstrumentVersion::new(1).unwrap(),
                expected_revision: 0,
                action: AccountControlAction::Enable,
                received_at: TimestampNs::from_unix_nanos(command_id),
            }))
            .unwrap();
        }
        let first_id = CommandId::new(1).unwrap();
        let first = book.reports.remove(&first_id).unwrap();
        assert!(book.reports.insert(first_id, first).is_none());
        let error = book.validate().unwrap_err();
        assert!(
            error
                .detail()
                .contains("retained report event range starts")
        );
        assert!(book.checkpoint_state(1, 5).is_err());
    }

    fn reference_can_fill(book: &OrderBook, incoming: &NewOrder) -> bool {
        let mut remaining = incoming.quantity.lots();
        let mut price = book.best_price(incoming.side.opposite());
        while let Some(current_price) = price {
            let crosses = match (incoming.side, incoming.order_type) {
                (_, OrderType::Market) => true,
                (Side::Buy, OrderType::Limit(limit)) => limit >= current_price,
                (Side::Sell, OrderType::Limit(limit)) => limit <= current_price,
                (_, OrderType::Stop { .. }) => {
                    unreachable!("dormant stop constraint cannot enter test FOK model")
                }
            };
            if !crosses {
                return false;
            }
            let level = book
                .levels(incoming.side.opposite())
                .get(current_price)
                .unwrap();
            let mut queue = std::collections::VecDeque::new();
            let mut order_id = Some(level.head);
            while let Some(current_id) = order_id {
                let order = *book.orders.get(&current_id).unwrap();
                order_id = order.next;
                queue.push_back(order);
            }
            while let Some(mut order) = queue.pop_front() {
                if order.account_id == incoming.account_id {
                    match incoming.self_trade_prevention {
                        SelfTradePrevention::CancelResting => continue,
                        SelfTradePrevention::CancelAggressor
                        | SelfTradePrevention::CancelBoth
                        | SelfTradePrevention::DecrementAndCancel => return false,
                    }
                }
                let executed = remaining.min(order.displayed);
                remaining -= executed;
                if remaining == 0 {
                    return true;
                }
                order.leaves -= executed;
                if order.leaves > 0 {
                    order.displayed = order.display.working_lots(order.leaves);
                    order.previous = None;
                    order.next = None;
                    queue.push_back(order);
                }
            }
            price = book.next_worse_price(incoming.side.opposite(), current_price);
        }
        false
    }

    fn generated_book_and_fok(generator: &mut Generator) -> (OrderBook, NewOrder) {
        let limits = OrderBookLimits::new(OrderBookLimitsSpec {
            max_active_orders: 32,
            max_active_accounts: 32,
            max_price_levels_per_side: 4,
            max_accepted_order_ids: 64,
            max_account_controls: 64,
            max_retained_commands: 64,
            cancellation_reserve: 32,
            max_report_events: 256,
            max_retained_events: 1_024,
            max_prepared_order_selections: 2,
        })
        .unwrap();
        let mut book = OrderBook::with_limits(reserve_definition(), limits);
        let incoming_side = if generator.bounded(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let base_price = if generator.bounded(2) == 0 { -100 } else { 100 };
        let level_count = generator.bounded(3) + 1;
        let mut next_order_id = 1_u64;
        for level_index in 0..level_count {
            let order_count = generator.bounded(5) + 1;
            for _ in 0..order_count {
                let leaves = generator.bounded(20) + 1;
                let (display, displayed) = if generator.bounded(2) == 0 {
                    (OrderDisplay::FullyDisplayed, leaves)
                } else {
                    let peak = generator.bounded(5) + 1;
                    let displayed = generator.bounded(leaves.min(peak)) + 1;
                    (
                        OrderDisplay::Reserve {
                            peak: Quantity::new(peak).unwrap(),
                        },
                        displayed,
                    )
                };
                let account_id = if generator.bounded(4) == 0 {
                    AccountId::new(1).unwrap()
                } else {
                    AccountId::new(generator.bounded(8) + 2).unwrap()
                };
                let level_offset = i64::try_from(level_index).unwrap();
                let price = match incoming_side {
                    Side::Buy => base_price + level_offset,
                    Side::Sell => base_price - level_offset,
                };
                let order_id = OrderId::new(next_order_id).unwrap();
                book.append_order(RestingOrder {
                    order_id,
                    account_id,
                    side: incoming_side.opposite(),
                    price: Price::from_raw(price),
                    leaves,
                    displayed,
                    display,
                    self_trade_prevention: SelfTradePrevention::CancelAggressor,
                    expires_at: None,
                    stop: None,
                    previous: None,
                    next: None,
                    account_previous: None,
                    account_next: None,
                });
                book.seen_order_ids.insert(order_id);
                next_order_id += 1;
            }
        }
        let policy = match generator.bounded(3) {
            0 => SelfTradePrevention::CancelAggressor,
            1 => SelfTradePrevention::CancelResting,
            _ => SelfTradePrevention::CancelBoth,
        };
        let incoming = NewOrder {
            command_id: CommandId::new(1).unwrap(),
            order_id: OrderId::new(next_order_id).unwrap(),
            account_id: AccountId::new(1).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            side: incoming_side,
            quantity: Quantity::new(generator.bounded(80) + 1).unwrap(),
            display: OrderDisplay::FullyDisplayed,
            order_type: OrderType::Limit(Price::from_raw(match incoming_side {
                Side::Buy => base_price + 2,
                Side::Sell => base_price - 2,
            })),
            time_in_force: TimeInForce::FillOrKill,
            self_trade_prevention: policy,
            received_at: TimestampNs::from_unix_nanos(1),
        };
        (book, incoming)
    }

    fn append_full_replacement_level(
        book: &mut OrderBook,
        incoming: NewOrder,
        generator: &mut Generator,
    ) -> RestingOrder {
        let old_price = match incoming.side {
            Side::Buy => Price::from_raw(-500),
            Side::Sell => Price::from_raw(500),
        };
        let old_leaves = generator.bounded(20) + 1;
        let old = RestingOrder {
            order_id: incoming.order_id,
            account_id: incoming.account_id,
            side: incoming.side,
            price: old_price,
            leaves: old_leaves,
            displayed: old_leaves,
            display: OrderDisplay::FullyDisplayed,
            self_trade_prevention: incoming.self_trade_prevention,
            expires_at: incoming.time_in_force.expires_at(),
            stop: None,
            previous: None,
            next: None,
            account_previous: None,
            account_next: None,
        };
        book.append_order(old);
        book.seen_order_ids.insert(old.order_id);

        let peer_id = OrderId::new(old.order_id.get() + 1).unwrap();
        book.append_order(RestingOrder {
            order_id: peer_id,
            account_id: AccountId::new(99).unwrap(),
            side: incoming.side,
            price: old_price,
            leaves: 1,
            displayed: 1,
            display: OrderDisplay::FullyDisplayed,
            self_trade_prevention: SelfTradePrevention::CancelAggressor,
            expires_at: None,
            stop: None,
            previous: None,
            next: None,
            account_previous: None,
            account_next: None,
        });
        book.seen_order_ids.insert(peer_id);

        for offset in 0..2 {
            let order_id = OrderId::new(old.order_id.get() + 2 + offset).unwrap();
            let price = match incoming.side {
                Side::Buy => Price::from_raw(-600 - i64::try_from(offset).unwrap() * 100),
                Side::Sell => Price::from_raw(600 + i64::try_from(offset).unwrap() * 100),
            };
            book.append_order(RestingOrder {
                order_id,
                account_id: AccountId::new(98).unwrap(),
                side: incoming.side,
                price,
                leaves: 1,
                displayed: 1,
                display: OrderDisplay::FullyDisplayed,
                self_trade_prevention: SelfTradePrevention::CancelAggressor,
                expires_at: None,
                stop: None,
                previous: None,
                next: None,
                account_previous: None,
                account_next: None,
            });
            book.seen_order_ids.insert(order_id);
        }
        book.limits = OrderBookLimits::new(OrderBookLimitsSpec {
            max_active_orders: 32,
            max_active_accounts: 32,
            max_price_levels_per_side: 3,
            max_accepted_order_ids: 64,
            max_account_controls: 64,
            max_retained_commands: 64,
            cancellation_reserve: 32,
            max_report_events: 256,
            max_retained_events: 1_024,
            max_prepared_order_selections: 2,
        })
        .unwrap();
        old
    }

    fn level(order_id: u64) -> PriceLevel {
        let order_id = OrderId::new(order_id).expect("non-zero test order identifier");
        PriceLevel {
            head: order_id,
            tail: order_id,
            displayed_tail: Some(order_id),
            total_quantity: u128::from(order_id.get()),
            order_count: 1,
            visible_order_count: 1,
            event_units: u128::from(order_id.get()),
        }
    }

    #[test]
    fn cached_extremum_tracks_both_market_directions_and_non_best_removal() {
        for (side, expected) in [(Side::Buy, 110), (Side::Sell, 90)] {
            let mut levels = PriceLevels::try_new(side, 3).unwrap();
            assert_eq!(levels.best_price(), None);

            assert!(levels.insert(Price::from_raw(100), level(1)).is_none());
            assert!(levels.insert(Price::from_raw(90), level(2)).is_none());
            assert!(levels.insert(Price::from_raw(110), level(3)).is_none());
            assert_eq!(levels.best_price(), Some(Price::from_raw(expected)));
            levels.validate_extremum().unwrap();

            assert!(levels.remove(Price::from_raw(100)).is_some());
            assert_eq!(levels.best_price(), Some(Price::from_raw(expected)));
            levels.validate_extremum().unwrap();
        }
    }

    #[test]
    fn best_level_handle_updates_survive_unrelated_two_child_deletion() {
        let mut levels = PriceLevels::try_new(Side::Sell, 8).unwrap();
        for (order_id, price) in [40_i64, 20, 60, 10, 30, 50, 70, 25].into_iter().enumerate() {
            levels.insert(
                Price::from_raw(price),
                level(u64::try_from(order_id).unwrap() + 1),
            );
        }
        let (best_handle, best) = levels.best_level_with_handle().unwrap();
        assert_eq!(best_handle.price, Price::from_raw(10));
        let original_quantity = best.total_quantity;

        levels
            .update_by_handle(best_handle, |level| level.total_quantity += 7)
            .unwrap();
        assert_eq!(
            levels.best_level().unwrap().1.total_quantity,
            original_quantity + 7
        );
        assert!(levels.remove(Price::from_raw(40)).is_some());
        levels
            .update_by_handle(best_handle, |level| level.total_quantity += 11)
            .unwrap();
        assert_eq!(
            levels.best_level().unwrap().1.total_quantity,
            original_quantity + 18
        );
        levels.validate_extremum().unwrap();
    }

    #[test]
    fn maker_handle_covers_partial_fill_refresh_and_level_removal() {
        let mut book = OrderBook::new(reserve_definition());
        let price = Price::from_raw(100);
        for order in [
            RestingOrder {
                order_id: OrderId::new(1).unwrap(),
                account_id: AccountId::new(11).unwrap(),
                side: Side::Sell,
                price,
                leaves: 5,
                displayed: 2,
                display: OrderDisplay::Reserve {
                    peak: Quantity::new(2).unwrap(),
                },
                self_trade_prevention: SelfTradePrevention::CancelAggressor,
                expires_at: None,
                stop: None,
                previous: None,
                next: None,
                account_previous: None,
                account_next: None,
            },
            RestingOrder {
                order_id: OrderId::new(2).unwrap(),
                account_id: AccountId::new(12).unwrap(),
                side: Side::Sell,
                price,
                leaves: 3,
                displayed: 3,
                display: OrderDisplay::FullyDisplayed,
                self_trade_prevention: SelfTradePrevention::CancelAggressor,
                expires_at: None,
                stop: None,
                previous: None,
                next: None,
                account_previous: None,
                account_next: None,
            },
            RestingOrder {
                order_id: OrderId::new(3).unwrap(),
                account_id: AccountId::new(13).unwrap(),
                side: Side::Sell,
                price: Price::from_raw(200),
                leaves: 1,
                displayed: 1,
                display: OrderDisplay::FullyDisplayed,
                self_trade_prevention: SelfTradePrevention::CancelAggressor,
                expires_at: None,
                stop: None,
                previous: None,
                next: None,
                account_previous: None,
                account_next: None,
            },
        ] {
            assert!(book.seen_order_ids.insert(order.order_id));
            book.append_order(order);
        }
        let (handle, level) = book.asks.best_level_with_handle().unwrap();
        assert_eq!(
            (handle.price, level.head, level.tail),
            (price, OrderId::new(1).unwrap(), OrderId::new(2).unwrap())
        );

        assert!(
            book.decrement_resting(OrderId::new(1).unwrap(), 2, handle)
                .is_some()
        );
        let level = book.asks.best_level().unwrap().1;
        assert_eq!(
            (level.head, level.tail, level.total_quantity),
            (OrderId::new(2).unwrap(), OrderId::new(1).unwrap(), 5)
        );
        assert!(
            book.decrement_resting(OrderId::new(2).unwrap(), 1, handle)
                .is_none()
        );
        assert!(
            book.decrement_resting(OrderId::new(2).unwrap(), 2, handle)
                .is_none()
        );
        assert!(
            book.decrement_resting(OrderId::new(1).unwrap(), 2, handle)
                .is_some()
        );
        let level = book.asks.best_level().unwrap().1;
        assert_eq!(
            (level.head, level.tail, level.total_quantity),
            (OrderId::new(1).unwrap(), OrderId::new(1).unwrap(), 1)
        );
        assert!(
            book.decrement_resting(OrderId::new(1).unwrap(), 1, handle)
                .is_none()
        );
        assert_eq!(book.best_ask().unwrap().price, Price::from_raw(200));
        book.validate().unwrap();
    }

    #[test]
    fn deleting_best_recomputes_until_the_side_is_empty() {
        for (side, ordered_prices) in [(Side::Buy, [110, 100, 90]), (Side::Sell, [90, 100, 110])] {
            let mut levels = PriceLevels::try_new(side, 3).unwrap();
            for (index, price) in [100, 90, 110].into_iter().enumerate() {
                assert!(
                    levels
                        .insert(Price::from_raw(price), level(index as u64 + 1))
                        .is_none()
                );
            }
            for price in ordered_prices {
                assert_eq!(levels.best_price(), Some(Price::from_raw(price)));
                assert!(levels.remove(Price::from_raw(price)).is_some());
                levels.validate_extremum().unwrap();
            }
            assert_eq!(levels.best_price(), None);
        }
    }

    #[test]
    fn invariant_validation_rejects_missing_stale_and_non_extreme_caches() {
        let mut levels = PriceLevels::try_new(Side::Buy, 3).unwrap();
        levels.insert(Price::from_raw(100), level(1));
        levels.insert(Price::from_raw(110), level(2));
        let inferior_handle = levels.handle(Price::from_raw(100)).unwrap();

        for corrupted in [
            None,
            Some((Price::from_raw(100), level(1))),
            Some((Price::from_raw(120), level(3))),
            Some((Price::from_raw(110), level(4))),
        ] {
            levels.best = corrupted;
            let error: InvariantViolation = levels.validate_extremum().unwrap_err();
            assert!(error.detail().contains("cached best"));
        }

        levels.best = Some((Price::from_raw(110), level(2)));
        levels.best_handle = Some(inferior_handle);
        let error: InvariantViolation = levels.validate_extremum().unwrap_err();
        assert!(error.detail().contains("cached best"));
    }

    #[test]
    fn invariant_validation_recomputes_level_and_account_event_work() {
        let mut generator = Generator(0x72d4_0a9e_61bc_35f8);
        let (book, incoming) = generated_book_and_fok(&mut generator);
        book.validate().expect("generated book must be valid");

        let side = incoming.side.opposite();
        let price = book.best_price(side).expect("generated side is non-empty");
        let level = *book.levels(side).get(price).unwrap();
        let account_id = book.orders.get(&level.head).unwrap().account_id;

        let mut corrupted_level = book.clone();
        corrupted_level
            .levels_mut(side)
            .update(price, |level| level.event_units += 1)
            .unwrap();
        let error = corrupted_level.validate().unwrap_err();
        assert!(error.detail().contains("inconsistent aggregates"));

        let mut corrupted_account = book;
        *corrupted_account
            .account_orders
            .get_mut(&account_id)
            .unwrap()
            .event_units_mut(side) += 1;
        let error = corrupted_account.validate().unwrap_err();
        assert!(error.detail().contains("event work"));
    }

    #[test]
    fn invariant_validation_rejects_broken_intrusive_account_links() {
        let mut book = OrderBook::new(reserve_definition());
        for order_id in [30_u64, 10] {
            let order_id = OrderId::new(order_id).unwrap();
            book.append_order(RestingOrder {
                order_id,
                account_id: AccountId::new(1).unwrap(),
                side: Side::Buy,
                price: Price::from_raw(100),
                leaves: 1,
                displayed: 1,
                display: OrderDisplay::FullyDisplayed,
                self_trade_prevention: SelfTradePrevention::CancelAggressor,
                expires_at: None,
                stop: None,
                previous: None,
                next: None,
                account_previous: None,
                account_next: None,
            });
            book.seen_order_ids.insert(order_id);
        }
        book.validate().unwrap();
        let semantic_clone = book.clone();

        book.orders
            .get_mut(&OrderId::new(10).unwrap())
            .unwrap()
            .account_previous = None;
        assert_eq!(book, semantic_clone);
        let error = book.validate().unwrap_err();
        assert!(error.detail().contains("broken previous account link"));
    }

    #[test]
    fn allocation_free_validation_rejects_price_and_account_cycles() {
        let mut book = OrderBook::new(reserve_definition());
        for order_id in [10_u64, 20] {
            let order_id = OrderId::new(order_id).unwrap();
            book.append_order(RestingOrder {
                order_id,
                account_id: AccountId::new(1).unwrap(),
                side: Side::Buy,
                price: Price::from_raw(100),
                leaves: 1,
                displayed: 1,
                display: OrderDisplay::FullyDisplayed,
                self_trade_prevention: SelfTradePrevention::CancelAggressor,
                expires_at: None,
                stop: None,
                previous: None,
                next: None,
                account_previous: None,
                account_next: None,
            });
            book.seen_order_ids.insert(order_id);
        }
        book.validate().unwrap();

        let head = OrderId::new(10).unwrap();
        let tail = OrderId::new(20).unwrap();
        let mut price_cycle = book.clone();
        price_cycle.orders.get_mut(&tail).unwrap().next = Some(head);
        assert!(
            price_cycle
                .validate()
                .unwrap_err()
                .detail()
                .contains("FIFO cycle")
        );

        book.orders.get_mut(&tail).unwrap().account_next = Some(head);
        assert!(
            book.validate()
                .unwrap_err()
                .detail()
                .contains("account-index cycle")
        );
    }

    #[test]
    fn intrusive_account_links_survive_head_middle_tail_removal_and_side_isolation() {
        let mut book = OrderBook::new(reserve_definition());
        let account_id = AccountId::new(1).unwrap();
        for (order_id, side, price) in [
            (30_u64, Side::Buy, 100_i64),
            (10, Side::Buy, 99),
            (20, Side::Buy, 98),
            (5, Side::Sell, 200),
        ] {
            let order_id = OrderId::new(order_id).unwrap();
            book.append_order(RestingOrder {
                order_id,
                account_id,
                side,
                price: Price::from_raw(price),
                leaves: 1,
                displayed: 1,
                display: OrderDisplay::FullyDisplayed,
                self_trade_prevention: SelfTradePrevention::CancelAggressor,
                expires_at: None,
                stop: None,
                previous: None,
                next: None,
                account_previous: None,
                account_next: None,
            });
            book.seen_order_ids.insert(order_id);
        }

        for (removed, expected_buys) in
            [(10_u64, vec![20_u64, 30]), (30, vec![20]), (20, Vec::new())]
        {
            book.remove_order(OrderId::new(removed).unwrap());
            assert_eq!(
                book.account_active_order_ids(account_id, MassCancelScope::Side(Side::Buy)),
                expected_buys
                    .into_iter()
                    .map(|order_id| OrderId::new(order_id).unwrap())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                book.account_active_order_ids(account_id, MassCancelScope::Side(Side::Sell)),
                vec![OrderId::new(5).unwrap()]
            );
            book.validate().unwrap();
        }
    }

    #[test]
    fn checkpoint_rebuilds_nonsemantic_account_topology_in_canonical_row_order() {
        let mut book = OrderBook::new(reserve_definition());
        let account_id = AccountId::new(1).unwrap();
        for (command_id, order_id, price) in [(1_u64, 30_u64, 80_i64), (2, 10, 100), (3, 20, 90)] {
            book.submit(Command::New(NewOrder {
                command_id: CommandId::new(command_id).unwrap(),
                order_id: OrderId::new(order_id).unwrap(),
                account_id,
                instrument_id: InstrumentId::new(1).unwrap(),
                instrument_version: InstrumentVersion::new(1).unwrap(),
                side: Side::Buy,
                quantity: Quantity::new(1).unwrap(),
                display: OrderDisplay::FullyDisplayed,
                order_type: OrderType::Limit(Price::from_raw(price)),
                time_in_force: TimeInForce::GoodTilCancelled,
                self_trade_prevention: SelfTradePrevention::CancelAggressor,
                received_at: TimestampNs::from_unix_nanos(command_id),
            }))
            .unwrap();
        }

        let mut live_topology = Vec::new();
        book.append_account_side_ids(
            book.account_orders.get(&account_id).unwrap(),
            Side::Buy,
            &mut live_topology,
        );
        assert_eq!(
            live_topology,
            [30_u64, 10, 20].map(|order_id| OrderId::new(order_id).unwrap())
        );

        let restored = OrderBook::from_checkpoint(&book.checkpoint(1, 7).unwrap()).unwrap();
        let mut restored_topology = Vec::new();
        restored.append_account_side_ids(
            restored.account_orders.get(&account_id).unwrap(),
            Side::Buy,
            &mut restored_topology,
        );
        assert_eq!(
            restored_topology,
            [30_u64, 20, 10].map(|order_id| OrderId::new(order_id).unwrap())
        );
        assert_eq!(restored, book);
        restored.validate().unwrap();
    }

    #[test]
    fn allocation_free_fok_scan_matches_slice_queue_reference() {
        let mut generator = Generator(0x91c7_5a2b_d4e8_603f);
        for case in 0..20_000 {
            let (book, incoming) = generated_book_and_fok(&mut generator);
            assert_eq!(
                book.can_fill(&incoming),
                reference_can_fill(&book, &incoming),
                "FOK model divergence in generated case {case}"
            );
        }
    }

    #[test]
    fn command_specific_event_bound_admits_the_last_complete_report() {
        let mut book = OrderBook::new(reserve_definition());
        book.next_sequence = u64::MAX - 2;
        let command = NewOrder {
            command_id: CommandId::new(1).unwrap(),
            order_id: OrderId::new(1).unwrap(),
            account_id: AccountId::new(1).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            side: Side::Buy,
            quantity: Quantity::new(1).unwrap(),
            display: OrderDisplay::FullyDisplayed,
            order_type: OrderType::Limit(Price::from_raw(1)),
            time_in_force: TimeInForce::GoodTilCancelled,
            self_trade_prevention: SelfTradePrevention::CancelAggressor,
            received_at: TimestampNs::from_unix_nanos(1),
        };
        let CommandPreparation::Ready(prepared) = book.prepare(Command::New(command)).unwrap()
        else {
            panic!("new command cannot be a replay")
        };
        let event_slot = book.retained_event_count;
        assert!(book.event_arena.slot_pointer(event_slot).is_null());
        let report = book
            .commit(prepared)
            .expect("two-event report fits before sequence exhaustion");
        assert_eq!(report.events.len(), 2);
        assert_eq!(
            std::ptr::from_ref(&report.events[0]),
            book.event_arena.slot_pointer(event_slot)
        );
        assert_eq!(report.events[0].sequence, u64::MAX - 2);
        assert_eq!(report.events[1].sequence, u64::MAX - 1);
        assert_eq!(book.next_sequence, u64::MAX);
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one allocator invariant fixture compares every constructor-owned index, arena, and pool"
    )]
    fn construction_owns_hash_and_selection_headroom() {
        let mut book = OrderBook::new(reserve_definition());
        let new = NewOrder {
            command_id: CommandId::new(1).unwrap(),
            order_id: OrderId::new(1).unwrap(),
            account_id: AccountId::new(1).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            side: Side::Buy,
            quantity: Quantity::new(1).unwrap(),
            display: OrderDisplay::FullyDisplayed,
            order_type: OrderType::Limit(Price::from_raw(1)),
            time_in_force: TimeInForce::GoodTilTimestamp {
                expires_at: TimestampNs::from_unix_nanos(100),
            },
            self_trade_prevention: SelfTradePrevention::CancelAggressor,
            received_at: TimestampNs::from_unix_nanos(1),
        };
        assert!(book.orders.capacity() >= book.limits.max_active_orders());
        assert!(book.account_orders.capacity() >= book.limits.max_active_accounts());
        assert!(book.seen_order_ids.capacity() >= book.limits.max_accepted_order_ids());
        assert!(book.account_controls.capacity() >= book.limits.max_account_controls());
        assert!(book.reports.capacity() >= book.limits.max_retained_commands());
        let initial_selection = book.order_selection_pool_status();
        assert_eq!(
            initial_selection.configured_leases,
            book.limits.max_prepared_order_selections()
        );
        assert_eq!(
            initial_selection.available_leases,
            initial_selection.configured_leases
        );
        assert!(initial_selection.allocated_order_ids_per_lease >= book.limits.max_active_orders());
        assert_eq!(book.bids.storage_len(), 0);
        assert!(book.bids.allocation_capacity() >= book.limits.max_price_levels_per_side());
        assert_eq!(book.expiries.storage_len(), 0);
        assert_eq!(book.expiries.maximum(), book.limits.max_active_orders());
        assert!(book.expiries.allocation_capacity() >= book.limits.max_active_orders());
        assert_eq!(
            book.price_level_arena_status(Side::Buy),
            super::PriceLevelArenaStatus {
                configured_slots: book.limits.max_price_levels_per_side(),
                allocated_slots: book.bids.allocation_capacity(),
                initialized_slots: 0,
                occupied_slots: 0,
                reusable_slots: 0,
            }
        );
        let CommandPreparation::Ready(prepared) = book.prepare(Command::New(new)).unwrap() else {
            panic!("new command cannot be a replay")
        };
        let capacities = (
            book.orders.capacity(),
            book.account_orders.capacity(),
            book.seen_order_ids.capacity(),
            book.account_controls.capacity(),
            book.reports.capacity(),
            book.bids.allocation_capacity(),
            book.expiries.allocation_capacity(),
        );
        book.commit(prepared).unwrap();
        assert_eq!(book.bids.storage_len(), 1);
        assert_eq!(book.expiries.storage_len(), 1);
        assert_eq!(book.expiries.len(), 1);
        assert_eq!(book.price_level_arena_status(Side::Buy).occupied_slots, 1);
        assert_eq!(book.price_level_arena_status(Side::Buy).reusable_slots, 0);
        assert_eq!(
            capacities,
            (
                book.orders.capacity(),
                book.account_orders.capacity(),
                book.seen_order_ids.capacity(),
                book.account_controls.capacity(),
                book.reports.capacity(),
                book.bids.allocation_capacity(),
                book.expiries.allocation_capacity(),
            )
        );

        let mass_cancel = Command::MassCancel(MassCancel {
            command_id: CommandId::new(2).unwrap(),
            account_id: AccountId::new(1).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            scope: MassCancelScope::All,
            received_at: TimestampNs::from_unix_nanos(2),
        });
        let CommandPreparation::Ready(prepared) = book.prepare(mass_cancel).unwrap() else {
            panic!("mass cancel cannot be a replay")
        };
        let selected = prepared
            .selected_order_ids
            .as_ref()
            .expect("mass cancellation owns selection storage");
        assert!(selected.values().is_empty());
        assert!(selected.capacity() >= book.limits.max_active_orders());
        assert_eq!(
            book.order_selection_pool_status().available_leases,
            initial_selection.available_leases - 1
        );
        book.commit(prepared).unwrap();
        assert_eq!(book.order_selection_pool_status(), initial_selection);
        assert_eq!(book.active_order_count(), 0);
        assert_eq!(book.bids.storage_len(), 1);
        assert_eq!(book.expiries.storage_len(), 1);
        assert_eq!(book.expiries.len(), 0);
        assert_eq!(book.price_level_arena_status(Side::Buy).occupied_slots, 0);
        assert_eq!(book.price_level_arena_status(Side::Buy).reusable_slots, 1);

        book.submit(Command::New(NewOrder {
            command_id: CommandId::new(3).unwrap(),
            order_id: OrderId::new(2).unwrap(),
            order_type: OrderType::Limit(Price::from_raw(2)),
            received_at: TimestampNs::from_unix_nanos(3),
            ..new
        }))
        .unwrap();
        assert_eq!(book.bids.storage_len(), 1);
        assert_eq!(book.bids.allocation_capacity(), capacities.5);
        assert_eq!(book.expiries.allocation_capacity(), capacities.6);
        assert_eq!(book.expiries.len(), 1);
        assert_eq!(book.price_level_arena_status(Side::Buy).occupied_slots, 1);
        assert_eq!(book.price_level_arena_status(Side::Buy).reusable_slots, 0);

        let control = Command::AccountControl(AccountControl {
            command_id: CommandId::new(4).unwrap(),
            account_id: new.account_id,
            instrument_id: new.instrument_id,
            instrument_version: new.instrument_version,
            expected_revision: 0,
            action: AccountControlAction::BlockAndCancel,
            received_at: TimestampNs::from_unix_nanos(4),
        });
        let control_capacity = book.account_controls.capacity();
        let CommandPreparation::Ready(prepared) = book.prepare(control).unwrap() else {
            panic!("account control cannot be a replay")
        };
        let retained_events = book.retained_event_count;
        assert!(book.event_arena.slot_pointer(retained_events).is_null());
        let selected = prepared
            .selected_order_ids
            .as_ref()
            .expect("block-and-cancel owns selection storage");
        assert!(selected.values().is_empty());
        assert!(selected.capacity() >= book.limits.max_active_orders());
        assert_eq!(book.active_order_count(), 1);
        assert_eq!(book.account_control(new.account_id).revision(), 0);
        let report = book.commit(prepared).unwrap();
        assert_eq!(book.order_selection_pool_status(), initial_selection);
        assert_eq!(
            std::ptr::from_ref(&report.events[0]),
            book.event_arena.slot_pointer(retained_events)
        );
        assert_eq!(book.active_order_count(), 0);
        assert_eq!(book.expiries.len(), 0);
        assert_eq!(book.account_control(new.account_id).revision(), 1);
        assert_eq!(book.account_controls.capacity(), control_capacity);
    }

    #[test]
    fn invariant_validation_rejects_lost_constructor_headroom() {
        let mut book = OrderBook::new(reserve_definition());
        book.orders.shrink_to_fit();
        let error = book
            .validate()
            .expect_err("an index below its constructor reservation is invalid");
        assert!(error.detail().contains("active-order hash capacity"));

        let mut book = OrderBook::new(reserve_definition());
        book.account_controls.shrink_to_fit();
        let error = book
            .validate()
            .expect_err("lost control headroom is invalid");
        assert!(error.detail().contains("account-control hash capacity"));

        let mut book = OrderBook::new(reserve_definition());
        book.expiries.shrink_to_fit();
        let error = book
            .validate()
            .expect_err("lost GTD-expiry headroom is invalid");
        assert!(error.detail().contains("GTD expiry-index reservation"));

        let mut book = OrderBook::new(reserve_definition());
        book.bids.visible_by_price.shrink_to_fit();
        let error = book
            .validate()
            .expect_err("lost public-price headroom is invalid");
        assert!(error.detail().contains("public-price index reservation"));

        let mut book = OrderBook::new(reserve_definition());
        Arc::get_mut(&mut book.order_selection_pool)
            .expect("unleased pool has one owner")
            .order_capacity = 0;
        let error = book
            .validate()
            .expect_err("lost order-selection headroom is invalid");
        assert!(error.detail().contains("order-selection pool reservation"));
    }

    #[test]
    fn checkpoint_rejects_hidden_before_displayed_at_one_price() {
        let mut book = OrderBook::new(reserve_definition());
        for (command_id, order_id, account_id, display) in [
            (1, 1, 1, OrderDisplay::Hidden),
            (2, 2, 2, OrderDisplay::FullyDisplayed),
        ] {
            book.submit(Command::New(NewOrder {
                command_id: CommandId::new(command_id).unwrap(),
                order_id: OrderId::new(order_id).unwrap(),
                account_id: AccountId::new(account_id).unwrap(),
                instrument_id: InstrumentId::new(1).unwrap(),
                instrument_version: InstrumentVersion::new(1).unwrap(),
                side: Side::Buy,
                quantity: Quantity::new(1).unwrap(),
                display,
                order_type: OrderType::Limit(Price::from_raw(100)),
                time_in_force: TimeInForce::GoodTilCancelled,
                self_trade_prevention: SelfTradePrevention::CancelAggressor,
                received_at: TimestampNs::from_unix_nanos(command_id),
            }))
            .unwrap();
        }
        let mut checkpoint = book.checkpoint_state(1, 5).unwrap();
        Arc::make_mut(&mut checkpoint.orders).swap(0, 1);

        let error = checkpoint
            .validate()
            .expect_err("hidden-before-displayed checkpoint rows are noncanonical");
        assert!(
            error
                .to_string()
                .contains("canonical side/price/FIFO order")
        );
    }

    #[test]
    fn exhausted_account_control_revision_is_a_nonmutating_business_rejection() {
        let mut book = OrderBook::new(reserve_definition());
        let account_id = AccountId::new(1).unwrap();
        book.account_controls.insert(
            account_id,
            AccountControlSnapshot {
                state: AccountAdmissionState::Blocked,
                revision: u64::MAX,
            },
        );
        let command = Command::AccountControl(AccountControl {
            command_id: CommandId::new(1).unwrap(),
            account_id,
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            expected_revision: u64::MAX,
            action: AccountControlAction::Enable,
            received_at: TimestampNs::from_unix_nanos(1),
        });
        assert_eq!(
            book.check_business_rules(command),
            Err(RejectReason::AccountControlRevisionExhausted)
        );
        let report = book.submit(command).unwrap();
        assert_eq!(
            report.outcome,
            CommandOutcome::Rejected(RejectReason::AccountControlRevisionExhausted)
        );
        assert_eq!(book.account_control(account_id).revision(), u64::MAX);
    }

    #[test]
    fn command_specific_work_bound_covers_generated_reports() {
        let mut generator = Generator(0x3e19_85ab_c742_d601);
        for case in 0..20_000 {
            let (mut book, mut incoming) = generated_book_and_fok(&mut generator);
            incoming.time_in_force = match generator.bounded(4) {
                0 => TimeInForce::GoodTilCancelled,
                1 => TimeInForce::ImmediateOrCancel,
                2 => TimeInForce::FillOrKill,
                _ => TimeInForce::PostOnly,
            };
            incoming.self_trade_prevention = match generator.bounded(4) {
                0 => SelfTradePrevention::CancelAggressor,
                1 => SelfTradePrevention::CancelResting,
                2 => SelfTradePrevention::CancelBoth,
                _ => SelfTradePrevention::DecrementAndCancel,
            };
            let command = Command::New(incoming);
            let core_rejection = book.check_business_rules(command).err();
            let (maximum_events, maximum_trades) = book
                .maximum_report_work(command, core_rejection)
                .expect("generated command work must be representable");
            let report = book
                .submit(command)
                .expect("generated command cannot exhaust operational capacity");
            let actual_trades = report
                .events
                .iter()
                .filter(|event| matches!(event.kind, super::EventKind::Trade(_)))
                .count();
            assert!(
                report.events.len() <= maximum_events,
                "event bound was exceeded in generated case {case}: {} > {maximum_events}",
                report.events.len()
            );
            assert!(
                book.event_arena_status().allocated_events >= book.limits().max_retained_events(),
                "constructor event arena was smaller than its limit in generated case {case}"
            );
            assert!(
                u64::try_from(actual_trades).unwrap() <= maximum_trades,
                "trade bound was exceeded in generated case {case}: {actual_trades} > {maximum_trades}"
            );
            book.validate()
                .expect("generated execution must preserve all maintained aggregates");
        }
    }

    #[test]
    fn residual_preview_matches_real_gtc_execution() {
        let mut generator = Generator(0x4d82_107f_a639_ce55);
        for case in 0..20_000 {
            let (book, mut incoming) = generated_book_and_fok(&mut generator);
            incoming.time_in_force = TimeInForce::GoodTilCancelled;
            incoming.self_trade_prevention = match generator.bounded(4) {
                0 => SelfTradePrevention::CancelAggressor,
                1 => SelfTradePrevention::CancelResting,
                2 => SelfTradePrevention::CancelBoth,
                _ => SelfTradePrevention::DecrementAndCancel,
            };
            let predicted = book.resting_residual_removed_orders(IncomingPreview::from(incoming));
            let mut executed = book.clone();
            executed
                .submit(Command::New(incoming))
                .expect("generated GTC command is valid");
            let actual = executed.order(incoming.order_id).is_some();
            assert_eq!(
                predicted.is_some(),
                actual,
                "residual preview diverged in generated case {case}"
            );
            if let Some(predicted_removed) = predicted {
                let actual_removed = book
                    .active_order_count()
                    .checked_add(1)
                    .and_then(|with_incoming| {
                        with_incoming.checked_sub(executed.active_order_count())
                    })
                    .expect("a resting residual adds exactly one active order");
                assert_eq!(
                    predicted_removed, actual_removed,
                    "removed-order preview diverged in generated case {case}"
                );
            }

            let mut external = incoming;
            external.account_id = AccountId::new(100).unwrap();
            let external_preview =
                book.resting_residual_removed_orders(IncomingPreview::from(external));
            if external_preview.is_some() {
                let predicted_released_accounts =
                    book.resting_residual_released_accounts(IncomingPreview::from(external));
                let initial_accounts = book.account_orders.len();
                let mut external_execution = book.clone();
                external_execution
                    .submit(Command::New(external))
                    .expect("generated external-account GTC command is valid");
                assert!(external_execution.order(external.order_id).is_some());
                let released_accounts = initial_accounts
                    .checked_add(1)
                    .and_then(|with_incoming| {
                        with_incoming.checked_sub(external_execution.account_orders.len())
                    })
                    .expect("a new residual account changes final account cardinality exactly");
                assert_eq!(
                    predicted_released_accounts, released_accounts,
                    "account-release preview diverged in generated case {case}"
                );
            }
        }
    }

    #[test]
    fn replacement_residual_preview_matches_real_execution() {
        let mut generator = Generator(0xa763_84f1_09de_2bc5);
        for case in 0..20_000 {
            let (mut book, mut incoming) = generated_book_and_fok(&mut generator);
            incoming.time_in_force = TimeInForce::GoodTilCancelled;
            incoming.self_trade_prevention = match generator.bounded(4) {
                0 => SelfTradePrevention::CancelAggressor,
                1 => SelfTradePrevention::CancelResting,
                2 => SelfTradePrevention::CancelBoth,
                _ => SelfTradePrevention::DecrementAndCancel,
            };
            let old = append_full_replacement_level(&mut book, incoming, &mut generator);

            let preview = IncomingPreview {
                account_id: old.account_id,
                side: old.side,
                order_type: incoming.order_type,
                leaves: incoming.quantity.lots(),
                self_trade_prevention: old.self_trade_prevention,
            };
            let predicted = book.resting_residual_removed_orders(preview);
            let initial_active_orders = book.active_order_count();
            let OrderType::Limit(new_price) = incoming.order_type else {
                unreachable!("generated replacement preview is limit-priced")
            };
            let replacement = ReplaceOrder {
                command_id: incoming.command_id,
                order_id: old.order_id,
                account_id: old.account_id,
                instrument_id: incoming.instrument_id,
                instrument_version: incoming.instrument_version,
                new_quantity: incoming.quantity,
                new_price,
                new_display: OrderDisplay::FullyDisplayed,
                received_at: incoming.received_at,
            };
            assert_eq!(
                book.check_replace_capacity(replacement).is_ok(),
                predicted.is_none(),
                "replacement capacity decision diverged in generated case {case}"
            );
            book.limits = OrderBookLimits::default();
            let (maximum_events, maximum_trades) = book
                .maximum_report_work(Command::Replace(replacement), None)
                .expect("generated replacement work is representable");
            let events = EventTraceBuilder::new(
                try_event_arena(maximum_events)
                    .expect("generated replacement trace allocation must succeed"),
                0,
                maximum_events,
            );
            let report = book
                .apply_replace(replacement, events)
                .expect("generated replacement execution cannot exhaust identifiers");
            assert!(report.events.len() <= maximum_events);
            assert!(
                u64::try_from(
                    report
                        .events
                        .iter()
                        .filter(|event| matches!(event.kind, super::EventKind::Trade(_)))
                        .count()
                )
                .unwrap()
                    <= maximum_trades,
                "replacement trade bound was exceeded in generated case {case}"
            );

            assert_eq!(
                predicted.is_some(),
                book.order(old.order_id).is_some(),
                "replacement residual preview diverged in generated case {case}"
            );
            if let Some(predicted_removed) = predicted {
                assert_eq!(
                    predicted_removed,
                    initial_active_orders - book.active_order_count(),
                    "replacement maker-removal preview diverged in generated case {case}"
                );
            }
        }
    }
}
