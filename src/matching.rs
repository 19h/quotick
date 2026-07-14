//! A deterministic, single-instrument, price-time-priority matching engine.
//!
//! One [`OrderBook`] is intended to be owned by one execution shard.  It uses
//! an order hash index, intrusive per-account/per-side indexes, and intrusive
//! FIFO links inside ordered price levels. New resting orders and cancellations
//! avoid scans of an entire price level or book. A redundant complete best-level
//! cache makes market-extremum discovery `O(1)`; ordered level insertion,
//! deletion, and next-worse traversal remain `O(log P)`, where `P` is the number
//! of occupied price levels. A command consuming `E` displayed maker slices is
//! `O((E + 1) log P)`. A reserve maker can contribute multiple slices. Account
//! mass-cancel selection visits `K` selected orders and canonically sorts them in
//! `O(K log K)`. FOK inspection uses `O(1)` auxiliary space and visits each
//! active order in crossed levels at most once; it never materializes reserve
//! replenishment slices.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Deref;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::domain::{
    AccountId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs, TradeId,
};
use crate::indexed_avl::IndexedAvlMap;
use crate::instrument::{AdmissionError, InstrumentDefinition};

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

/// The price constraint attached to a new order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OrderType {
    /// Execute at available prices and never rest.
    Market,
    /// Execute at the specified price or better.
    Limit(Price),
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
}

impl OrderDisplay {
    /// Returns true for a native reserve order.
    #[must_use]
    pub const fn is_reserve(self) -> bool {
        matches!(self, Self::Reserve { .. })
    }

    /// Returns the configured reserve peak, if any.
    #[must_use]
    pub const fn peak(self) -> Option<Quantity> {
        match self {
            Self::FullyDisplayed => None,
            Self::Reserve { peak } => Some(peak),
        }
    }

    const fn displayed_lots(self, total_leaves: u64) -> u64 {
        match self {
            Self::FullyDisplayed => total_leaves,
            Self::Reserve { peak } => {
                if peak.lots() < total_leaves {
                    peak.lots()
                } else {
                    total_leaves
                }
            }
        }
    }
}

/// Lifetime and immediate-execution constraint for an order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeInForce {
    /// Rest any unfilled limit quantity until explicitly cancelled.
    GoodTilCancelled,
    /// Execute immediately and cancel any unfilled remainder.
    ImmediateOrCancel,
    /// Execute the complete quantity immediately or reject without mutation.
    FillOrKill,
    /// Reject if any quantity would execute immediately; otherwise rest.
    PostOnly,
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
    /// Market or limit constraint.
    pub order_type: OrderType,
    /// Lifetime constraint.
    pub time_in_force: TimeInForce,
    /// Self-trade prevention action.
    pub self_trade_prevention: SelfTradePrevention,
    /// Gateway receive time.
    pub received_at: TimestampNs,
}

/// A request to cancel a resting order.
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
    /// Cancel every resting order owned by the account in this instrument shard.
    All,
    /// Cancel only resting orders on one side.
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

/// A request to cancel a canonical account-scoped set of resting orders.
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

/// A request to replace the leaves quantity and limit price of a resting order.
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
    /// New display policy. Changing between displayed and reserve is rejected.
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
    /// Close entry atomically and cancel every resting order for the account.
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
    /// Cancel a resting order.
    Cancel(CancelOrder),
    /// Replace a resting order.
    Replace(ReplaceOrder),
    /// Cancel an account-scoped set of resting orders.
    MassCancel(MassCancel),
    /// Change an account admission fence, optionally cancelling all resting orders atomically.
    AccountControl(AccountControl),
}

impl Command {
    const fn command_id(self) -> CommandId {
        match self {
            Self::New(command) => command.command_id,
            Self::Cancel(command) => command.command_id,
            Self::Replace(command) => command.command_id,
            Self::MassCancel(command) => command.command_id,
            Self::AccountControl(command) => command.command_id,
        }
    }

    const fn received_at(self) -> TimestampNs {
        match self {
            Self::New(command) => command.received_at,
            Self::Cancel(command) => command.received_at,
            Self::Replace(command) => command.received_at,
            Self::MassCancel(command) => command.received_at,
            Self::AccountControl(command) => command.received_at,
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
    /// A reserve display peak was not aligned to the lot increment.
    DisplayQuantityOffGrid,
    /// A reserve display peak was not smaller than total leaves.
    DisplayQuantityNotLessThanOrder,
    /// The order would require more peak refreshes than its admitted state permits.
    ReserveReplenishmentLimit,
    /// A reserve qualifier was attached to a non-resting order.
    ReserveOrderCannotBeImmediate,
    /// Replacement attempted to convert between displayed and reserve modes.
    OrderDisplayModeChangeNotAllowed,
    /// A shard-local administrative fence blocks new orders and replacements.
    AccountAdmissionBlocked,
    /// The account-control compare revision did not equal current state.
    AccountControlRevisionMismatch,
    /// The account-control revision cannot advance beyond `u64::MAX`.
    AccountControlRevisionExhausted,
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
        /// Quantity currently visible at the level.
        displayed_quantity: Quantity,
    },
    /// A depleted reserve peak replenished and moved to the FIFO tail.
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
        /// Number of cancelled resting orders.
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
        /// Number of resting orders atomically removed by the transition.
        cancelled_order_count: u64,
        /// Sum of removed total leaves in lots, including hidden reserve leaves.
        cancelled_quantity_lots: u128,
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
    /// A resting order was amended.
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

/// Immutable shared event sequence for one execution report.
///
/// Cloning a trace is `O(1)` and shares the same control block and event buffer.
/// Ordinary consumers receive slice semantics through [`Deref`].
/// [`Self::make_mut`] performs explicit copy-on-write for diagnostic mutation
/// without corrupting the idempotency cache. Conversion from `Vec<Event>` keeps
/// that vector's event buffer and allocates only the shared-owner control block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventTrace(Arc<Vec<Event>>);

impl EventTrace {
    /// Returns the complete immutable event slice.
    #[must_use]
    pub fn as_slice(&self) -> &[Event] {
        self.0.as_slice()
    }

    /// Returns a mutable event slice, copying first when storage is shared.
    pub fn make_mut(&mut self) -> &mut [Event] {
        Arc::make_mut(&mut self.0).as_mut_slice()
    }

    /// Returns whether two traces reference the identical immutable owner and buffer.
    #[must_use]
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl From<Vec<Event>> for EventTrace {
    fn from(events: Vec<Event>) -> Self {
        Self(Arc::new(events))
    }
}

#[derive(Debug)]
struct EventTraceBuilder {
    events: Arc<Vec<Event>>,
    maximum_events: usize,
}

impl EventTraceBuilder {
    fn try_with_capacity(maximum_events: usize) -> Result<Self, MatchingError> {
        let mut events = Vec::new();
        events.try_reserve_exact(maximum_events).map_err(|_| {
            MatchingError::CapacityReservationFailed(MatchingCapacity::ReportEvents)
        })?;
        Ok(Self {
            events: Arc::new(events),
            maximum_events,
        })
    }

    fn push(&mut self, event: Event) {
        assert!(
            self.events.len() < self.maximum_events,
            "prepared event bound must cover the complete execution report"
        );
        Arc::get_mut(&mut self.events)
            .expect("event builder must have unique storage")
            .push(event);
    }

    fn finish(self) -> EventTrace {
        EventTrace(self.events)
    }
}

#[cfg(test)]
mod event_trace_builder_tests {
    use super::{
        Event, EventKind, EventTraceBuilder, MatchingCapacity, MatchingError, RejectReason,
    };
    use crate::{CommandId, TimestampNs};

    #[test]
    fn finalization_retains_the_builder_event_buffer() {
        let command_id = CommandId::new(1).expect("non-zero command");
        let mut builder = EventTraceBuilder::try_with_capacity(8).unwrap();
        builder.push(Event {
            sequence: 1,
            command_id,
            occurred_at: TimestampNs::from_unix_nanos(1),
            kind: EventKind::CommandRejected(RejectReason::UnknownOrder),
        });
        let original_buffer = builder.events.as_ptr();
        let trace = builder.finish();
        assert_eq!(trace.as_ptr(), original_buffer);
    }

    #[test]
    fn unrepresentable_event_capacity_is_a_typed_preparation_failure() {
        assert!(matches!(
            EventTraceBuilder::try_with_capacity(usize::MAX),
            Err(MatchingError::CapacityReservationFailed(
                MatchingCapacity::ReportEvents
            ))
        ));
    }
}

impl Deref for EventTrace {
    type Target = [Event];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[Event]> for EventTrace {
    fn as_ref(&self) -> &[Event] {
        self.as_slice()
    }
}

impl<'a> IntoIterator for &'a EventTrace {
    type Item = &'a Event;
    type IntoIter = slice::Iter<'a, Event>;

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
    /// Contiguous events caused by this command.
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
/// WAL before commit. It owns the empty Arc-backed event vector and, for mass
/// cancellation, the empty order-selection vector whose complete command-specific
/// capacities were fallibly reserved during preparation. All matching hash-table
/// headroom was reserved when the book was constructed, so preparation borrows the
/// book immutably and cannot change either allocator or semantic book state.
/// [`OrderBook::commit`] rejects it if an unrelated command changes the book
/// generation after preparation.
#[derive(Debug)]
pub struct PreparedCommand {
    command: Command,
    book_instance_id: u64,
    expected_retained_commands: usize,
    core_rejection: Option<RejectReason>,
    events: EventTraceBuilder,
    selected_order_ids: Option<Vec<OrderId>>,
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

/// A visible resting order.
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
    /// Quantity currently represented in public depth.
    pub displayed_quantity: Quantity,
    /// Persistent display policy.
    pub display: OrderDisplay,
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
    pub(crate) displayed: Quantity,
    pub(crate) display: OrderDisplay,
    pub(crate) self_trade_prevention: SelfTradePrevention,
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

    /// Returns total leaves, including hidden reserve quantity.
    #[must_use]
    pub const fn leaves(self) -> Quantity {
        self.leaves
    }

    /// Returns the currently displayed slice.
    #[must_use]
    pub const fn displayed(self) -> Quantity {
        self.displayed
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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBookCheckpoint {
    wal_metadata_sequence: u64,
    wal_sequence: u64,
    definition: InstrumentDefinition,
    orders: Vec<RestingOrderCheckpoint>,
    history: Vec<CommandReportCheckpoint>,
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
        self.orders.len()
    }

    /// Returns the number of completed commands retained for exact retry.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.history.len()
    }

    /// Returns canonical private active-order state.
    #[must_use]
    pub fn orders(&self) -> &[RestingOrderCheckpoint] {
        &self.orders
    }

    /// Returns chronological command/report idempotency state.
    #[must_use]
    pub fn history(&self) -> &[CommandReportCheckpoint] {
        &self.history
    }

    pub(crate) fn from_parts(
        wal_metadata_sequence: u64,
        wal_sequence: u64,
        definition: InstrumentDefinition,
        orders: Vec<RestingOrderCheckpoint>,
        history: Vec<CommandReportCheckpoint>,
    ) -> Result<Self, OrderBookCheckpointError> {
        let checkpoint = Self {
            wal_metadata_sequence,
            wal_sequence,
            definition,
            orders,
            history,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    fn validate(&self) -> Result<(), OrderBookCheckpointError> {
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

        let mut command_ids = HashSet::with_capacity(self.history.len());
        let mut seen_order_ids = HashSet::new();
        let mut account_controls: HashMap<AccountId, AccountControlSnapshot> = HashMap::new();
        let mut expected_event_sequence = 1_u64;
        let mut expected_trade_id = 1_u64;
        for entry in &self.history {
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
                for event in &entry.report.events[..entry.report.events.len() - 1] {
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
        }

        let mut active_ids = HashSet::with_capacity(self.orders.len());
        let mut previous_key = None;
        for order in &self.orders {
            if !active_ids.insert(order.order_id) || !seen_order_ids.contains(&order.order_id) {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint active-order identity is duplicated or absent from accepted history",
                ));
            }
            let key = (side_wire_order(order.side), order.price);
            if previous_key.is_some_and(|previous| previous > key) {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint active orders are not in canonical side/price/FIFO order",
                ));
            }
            previous_key = Some(key);
            validate_checkpoint_order(self.definition, *order)?;
            if account_controls
                .get(&order.account_id)
                .is_some_and(|control| control.state == AccountAdmissionState::Blocked)
            {
                return Err(OrderBookCheckpointError::new(
                    "checkpoint retains an order for a blocked account",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn is_successor_of(&self, previous: &Self) -> bool {
        self.wal_metadata_sequence == previous.wal_metadata_sequence
            && self.definition == previous.definition
            && self.wal_sequence >= previous.wal_sequence
            && self.history.starts_with(&previous.history)
    }
}

/// Semantic matching-checkpoint construction or restoration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBookCheckpointError {
    detail: String,
}

impl OrderBookCheckpointError {
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

impl fmt::Display for OrderBookCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(formatter)
    }
}

impl std::error::Error for OrderBookCheckpointError {}

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
        .validate(order.leaves)
        .map_err(|_| OrderBookCheckpointError::new("checkpoint order leaves violate definition"))?;
    if order.displayed.lots() > order.leaves.lots() {
        return Err(OrderBookCheckpointError::new(
            "checkpoint displayed quantity exceeds total leaves",
        ));
    }
    match order.display {
        OrderDisplay::FullyDisplayed if order.displayed != order.leaves => Err(
            OrderBookCheckpointError::new("checkpoint fully displayed order hides quantity"),
        ),
        OrderDisplay::Reserve { peak }
            if !definition.reserve_order_rules().enabled()
                || peak.lots() % definition.quantity_rules().increment_lots() != 0
                || order.displayed.lots() > peak.lots() =>
        {
            Err(OrderBookCheckpointError::new(
                "checkpoint reserve display violates definition or peak",
            ))
        }
        _ => Ok(()),
    }
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
    let controlled_accounts = checkpoint
        .history
        .iter()
        .filter_map(|entry| match (entry.command, entry.report.outcome) {
            (Command::AccountControl(control), CommandOutcome::Accepted) => {
                Some(control.account_id)
            }
            _ => None,
        })
        .collect::<HashSet<_>>()
        .len();
    if controlled_accounts > limits.max_account_controls() {
        return Err(OrderBookCheckpointError::new(
            "checkpoint account-control capacity is insufficient",
        ));
    }
    if checkpoint.orders.len() > limits.max_active_orders() {
        return Err(OrderBookCheckpointError::new(
            "checkpoint active-order capacity is insufficient",
        ));
    }
    let mut accounts = HashSet::with_capacity(checkpoint.orders.len());
    let mut bids = HashSet::new();
    let mut asks = HashSet::new();
    for order in &checkpoint.orders {
        accounts.insert(order.account_id);
        match order.side {
            Side::Buy => bids.insert(order.price),
            Side::Sell => asks.insert(order.price),
        };
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
    /// Total retained command history, including the cancellation reserve.
    CommandHistory,
    /// Never-reusable accepted order identifiers.
    AcceptedOrderIds,
    /// Never-evicted account admission-control revisions.
    AccountControls,
    /// Simultaneously active resting orders.
    ActiveOrders,
    /// Accounts with at least one active resting order.
    ActiveAccounts,
    /// Occupied bid price levels.
    BidPriceLevels,
    /// Occupied ask price levels.
    AskPriceLevels,
    /// Events retained in one execution report.
    ReportEvents,
    /// Active-order identifiers selected by one mass cancellation.
    MassCancelSelection,
    /// Coupled pre-trade risk reservations for active orders.
    RiskReservations,
    /// Immutable account profiles registered in a coupled risk shard.
    RiskAccounts,
}

impl fmt::Display for MatchingCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AdmissionCommandHistory => "ordinary command history",
            Self::CommandHistory => "total command history",
            Self::AcceptedOrderIds => "accepted order identifiers",
            Self::AccountControls => "account admission controls",
            Self::ActiveOrders => "active orders",
            Self::ActiveAccounts => "active accounts",
            Self::BidPriceLevels => "bid price levels",
            Self::AskPriceLevels => "ask price levels",
            Self::ReportEvents => "execution-report events",
            Self::MassCancelSelection => "mass-cancel order selection",
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
    /// The book generation changed after a command was prepared.
    StalePreparation,
    /// A prepared command belongs to another order-book instance.
    ForeignPreparation,
    /// Process-local order-book identity exhausted its `u64` representation.
    BookInstanceIdExhausted,
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
            Self::StalePreparation => formatter.write_str("prepared matching command is stale"),
            Self::ForeignPreparation => {
                formatter.write_str("prepared matching command belongs to another order book")
            }
            Self::BookInstanceIdExhausted => {
                formatter.write_str("order-book instance identifier exhausted")
            }
        }
    }
}

impl std::error::Error for MatchingError {}

/// Raw construction values for a bounded [`OrderBook`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderBookLimitsSpec {
    /// Maximum simultaneous resting orders.
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
    /// Tail history slots reserved exclusively for cancel and mass-cancel commands.
    pub cancellation_reserve: usize,
    /// Maximum events retained by one execution report.
    pub max_report_events: usize,
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
}

impl OrderBookLimits {
    /// Default maximum simultaneous resting orders.
    pub const DEFAULT_MAX_ACTIVE_ORDERS: usize = 4_096;
    /// Default maximum accounts with resting orders.
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
            return Err(OrderBookLimitsError::ReportEventsBelowMassCancelMaximum);
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
        Ok(Self {
            max_active_orders: spec.max_active_orders,
            max_active_accounts: spec.max_active_accounts,
            max_price_levels_per_side: spec.max_price_levels_per_side,
            max_accepted_order_ids: spec.max_accepted_order_ids,
            max_account_controls: spec.max_account_controls,
            max_retained_commands: spec.max_retained_commands,
            cancellation_reserve: spec.cancellation_reserve,
            max_report_events: spec.max_report_events,
        })
    }

    /// Maximum simultaneous resting orders.
    #[must_use]
    pub const fn max_active_orders(self) -> usize {
        self.max_active_orders
    }

    /// Maximum accounts with resting orders.
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
            && self.previous == other.previous
            && self.next == other.next
    }
}

impl Eq for RestingOrder {}

impl RestingOrder {
    fn remaining_event_units(self) -> u128 {
        let slices = match self.display {
            OrderDisplay::FullyDisplayed => 1,
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PriceLevel {
    head: OrderId,
    tail: OrderId,
    total_quantity: u128,
    order_count: u64,
    event_units: u128,
}

/// Bounded occupied prices for one side with a mutation-maintained market
/// extremum.
///
/// The preallocated indexed AVL map remains the authoritative ordered
/// enumeration/index structure. The cached best price is redundant derived
/// state, updated only by `insert` and `remove` and independently checked by
/// `validate_extremum`. Read-only best-price discovery therefore does not
/// traverse the tree, and level mutation performs no allocation.
#[cfg_attr(test, derive(Clone))]
#[derive(Debug, Eq, PartialEq)]
struct PriceLevels {
    side: Side,
    by_price: IndexedAvlMap<Price, PriceLevel>,
    best: Option<(Price, PriceLevel)>,
    event_units: u128,
}

impl PriceLevels {
    fn try_new(
        side: Side,
        maximum_levels: usize,
    ) -> Result<Self, std::collections::TryReserveError> {
        Ok(Self {
            side,
            by_price: IndexedAvlMap::try_with_capacity(maximum_levels)?,
            best: None,
            event_units: 0,
        })
    }

    fn get(&self, price: Price) -> Option<&PriceLevel> {
        self.by_price.get(&price)
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&Price, &PriceLevel)> {
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

    fn best_level(&self) -> Option<(Price, PriceLevel)> {
        self.best
    }

    fn insert(&mut self, price: Price, level: PriceLevel) -> Option<PriceLevel> {
        let replaced = self.by_price.insert(price, level);
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
        }
        replaced
    }

    fn update<R>(&mut self, price: Price, update: impl FnOnce(&mut PriceLevel) -> R) -> Option<R> {
        let (result, snapshot, previous_event_units) = {
            let level = self.by_price.get_mut(&price)?;
            let previous_event_units = level.event_units;
            let result = update(level);
            (result, *level, previous_event_units)
        };
        self.event_units = self
            .event_units
            .checked_sub(previous_event_units)
            .and_then(|value| value.checked_add(snapshot.event_units))
            .expect("active-order event work must fit u128");
        if self.best.is_some_and(|(best_price, _)| best_price == price) {
            self.best = Some((price, snapshot));
        }
        Some(result)
    }

    fn remove(&mut self, price: Price) -> Option<PriceLevel> {
        let removed = self.by_price.remove(&price);
        if let Some(level) = removed {
            self.event_units = self
                .event_units
                .checked_sub(level.event_units)
                .expect("removed level event work must be indexed");
        }
        if removed.is_some() && self.best.is_some_and(|(best_price, _)| best_price == price) {
            self.best = self.map_extremum();
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
        self.by_price.validate().map_err(|detail| {
            InvariantViolation::new(format!("{:?} price index: {detail}", self.side))
        })?;
        let actual = self.map_extremum();
        if self.best != actual {
            return Err(InvariantViolation::new(format!(
                "{:?} cached best level {:?} differs from indexed-AVL extremum {:?}",
                self.side, self.best, actual
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

    fn map_extremum(&self) -> Option<(Price, PriceLevel)> {
        match self.side {
            Side::Buy => self
                .by_price
                .last_key_value()
                .map(|(&price, &level)| (price, level)),
            Side::Sell => self
                .by_price
                .first_key_value()
                .map(|(&price, &level)| (price, level)),
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
#[cfg_attr(test, derive(Clone))]
#[derive(Debug)]
pub struct OrderBook {
    instance_id: u64,
    definition: InstrumentDefinition,
    limits: OrderBookLimits,
    bids: PriceLevels,
    asks: PriceLevels,
    orders: HashMap<OrderId, RestingOrder>,
    account_orders: HashMap<AccountId, AccountOrderIndex>,
    seen_order_ids: HashSet<OrderId>,
    account_controls: HashMap<AccountId, AccountControlSnapshot>,
    reports: HashMap<CommandId, CachedReport>,
    next_sequence: u64,
    next_trade_id: u64,
}

impl PartialEq for OrderBook {
    fn eq(&self, other: &Self) -> bool {
        self.definition == other.definition
            && self.limits == other.limits
            && self.bids == other.bids
            && self.asks == other.asks
            && self.orders == other.orders
            && self.account_orders == other.account_orders
            && self.seen_order_ids == other.seen_order_ids
            && self.account_controls == other.account_controls
            && self.reports == other.reports
            && self.next_sequence == other.next_sequence
            && self.next_trade_id == other.next_trade_id
    }
}

impl Eq for OrderBook {}

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
    /// Panics when constructor-time price-arena/hash reservation or process-local
    /// instance identity allocation fails.
    #[must_use]
    pub fn with_limits(definition: InstrumentDefinition, limits: OrderBookLimits) -> Self {
        Self::try_with_limits(definition, limits)
            .expect("order-book capacity reservation must succeed under A12")
    }

    /// Creates an empty bounded order book and fallibly reserves both complete
    /// price-level arenas plus any configured hash maxima.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError::CapacityReservationFailed`] naming the first
    /// price arena or hash index whose requested reservation cannot be represented
    /// or allocated, or [`MatchingError::BookInstanceIdExhausted`] if
    /// process-local book identity is exhausted.
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
        let mut orders = HashMap::new();
        let mut account_orders = HashMap::new();
        let mut seen_order_ids = HashSet::new();
        let mut account_controls = HashMap::new();
        let mut reports = HashMap::new();
        orders
            .try_reserve(limits.max_active_orders())
            .map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::ActiveOrders)
            })?;
        account_orders
            .try_reserve(limits.max_active_accounts())
            .map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::ActiveAccounts)
            })?;
        seen_order_ids
            .try_reserve(limits.max_accepted_order_ids())
            .map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::AcceptedOrderIds)
            })?;
        account_controls
            .try_reserve(limits.max_account_controls())
            .map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::AccountControls)
            })?;
        reports
            .try_reserve(limits.max_retained_commands())
            .map_err(|_| {
                MatchingError::CapacityReservationFailed(MatchingCapacity::CommandHistory)
            })?;
        let instance_id = next_order_book_instance_id()?;
        Ok(Self {
            instance_id,
            definition,
            limits,
            bids,
            asks,
            orders,
            account_orders,
            seen_order_ids,
            account_controls,
            reports,
            next_sequence: 1,
            next_trade_id: 1,
        })
    }

    /// Captures canonical direct state at one completed WAL report boundary.
    ///
    /// The checkpoint retains complete command/report history because exact
    /// retries and never-reusable order identifiers are part of matching state.
    /// Active orders are emitted in buy-then-sell, ascending-price, FIFO order.
    ///
    /// # Errors
    ///
    /// Returns [`OrderBookCheckpointError`] when the live structure is invalid,
    /// the WAL boundary is not consistent with the retained command count, or
    /// canonical checkpoint validation fails.
    pub fn checkpoint(
        &self,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<OrderBookCheckpoint, OrderBookCheckpointError> {
        let checkpoint = self.checkpoint_state(wal_metadata_sequence, wal_sequence)?;
        let mut replay = Self::try_with_limits(self.definition, self.limits)
            .map_err(|error| OrderBookCheckpointError::new(error.to_string()))?;
        for entry in checkpoint.history() {
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
        if replay != *self {
            return Err(OrderBookCheckpointError::new(
                "checkpoint direct state differs from deterministic history replay",
            ));
        }
        Ok(checkpoint)
    }

    pub(crate) fn checkpoint_state(
        &self,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<OrderBookCheckpoint, OrderBookCheckpointError> {
        self.validate()
            .map_err(|error| OrderBookCheckpointError::new(error.detail()))?;
        let mut history: Vec<_> = self
            .reports
            .values()
            .map(|cached| CommandReportCheckpoint {
                command: cached.command,
                report: cached.report.clone(),
            })
            .collect();
        history.sort_unstable_by_key(|entry| {
            entry
                .report
                .events
                .first()
                .map_or(u64::MAX, |event| event.sequence)
        });

        let mut orders = Vec::with_capacity(self.orders.len());
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
                        displayed: Quantity::new(order.displayed).map_err(|_| {
                            OrderBookCheckpointError::new(
                                "live order has zero displayed quantity during checkpoint",
                            )
                        })?,
                        display: order.display,
                        self_trade_prevention: order.self_trade_prevention,
                    });
                    current = order.next;
                }
            }
        }
        OrderBookCheckpoint::from_parts(
            wal_metadata_sequence,
            wal_sequence,
            self.definition,
            orders,
            history,
        )
    }

    /// Restores a directly indexed order book from a validated semantic checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`OrderBookCheckpointError`] when semantic or reconstructed
    /// structural invariants fail.
    pub fn from_checkpoint(
        checkpoint: OrderBookCheckpoint,
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
    /// Returns [`OrderBookCheckpointError`] when the checkpoint is invalid or
    /// its current state exceeds any selected limit.
    pub fn from_checkpoint_with_limits(
        checkpoint: OrderBookCheckpoint,
        limits: OrderBookLimits,
    ) -> Result<Self, OrderBookCheckpointError> {
        checkpoint.validate()?;
        validate_checkpoint_capacity(&checkpoint, limits)?;
        let mut book = Self::try_with_limits(checkpoint.definition, limits)
            .map_err(|error| OrderBookCheckpointError::new(error.to_string()))?;
        for entry in checkpoint.history {
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
            book.reports.insert(
                entry.command.command_id(),
                CachedReport {
                    command: entry.command,
                    report: entry.report,
                },
            );
        }
        for state in checkpoint.orders {
            book.append_order(RestingOrder {
                order_id: state.order_id,
                account_id: state.account_id,
                side: state.side,
                price: state.price,
                leaves: state.leaves.lots(),
                displayed: state.displayed.lots(),
                display: state.display,
                self_trade_prevention: state.self_trade_prevention,
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
    pub fn check_business_rules(&self, command: Command) -> Result<(), RejectReason> {
        match command {
            Command::New(command) => {
                self.definition
                    .admit(Command::New(command))
                    .map_err(RejectReason::from)?;
                if self.seen_order_ids.contains(&command.order_id) {
                    return Err(RejectReason::DuplicateOrder);
                }
                if self.account_control(command.account_id).state == AccountAdmissionState::Blocked
                {
                    return Err(RejectReason::AccountAdmissionBlocked);
                }
                if command.display.is_reserve()
                    && !matches!(
                        (command.order_type, command.time_in_force),
                        (
                            OrderType::Limit(_),
                            TimeInForce::GoodTilCancelled | TimeInForce::PostOnly
                        )
                    )
                {
                    return Err(RejectReason::ReserveOrderCannotBeImmediate);
                }
                match (command.order_type, command.time_in_force) {
                    (OrderType::Market, TimeInForce::GoodTilCancelled) => {
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
                    && self.would_cross(command.side, command.order_type)
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
                    .admit(Command::Replace(command))
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
                if order.display.is_reserve() != command.new_display.is_reserve() {
                    return Err(RejectReason::OrderDisplayModeChangeNotAllowed);
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
    /// bounds, and core matching business rules exactly once. It also fallibly
    /// reserves the complete report event buffer and mass-cancel selection buffer.
    /// Every matching hash index already owns its complete configured headroom,
    /// so preparation is read-only with respect to the book. A ready token can be durably written
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
        let maximum_events_u64 =
            u64::try_from(maximum_events).map_err(|_| MatchingError::SequenceExhausted)?;
        self.next_sequence
            .checked_add(maximum_events_u64)
            .ok_or(MatchingError::SequenceExhausted)?;
        self.next_trade_id
            .checked_add(maximum_trades)
            .ok_or(MatchingError::TradeIdExhausted)?;
        let events = EventTraceBuilder::try_with_capacity(maximum_events)?;
        let selected_order_ids = match (core_rejection, command) {
            (None, Command::MassCancel(command)) => {
                let selected_count =
                    self.account_active_order_count(command.account_id, command.scope);
                let mut selected = Vec::new();
                selected.try_reserve_exact(selected_count).map_err(|_| {
                    MatchingError::CapacityReservationFailed(MatchingCapacity::MassCancelSelection)
                })?;
                Some(selected)
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
                let mut selected = Vec::new();
                selected.try_reserve_exact(selected_count).map_err(|_| {
                    MatchingError::CapacityReservationFailed(MatchingCapacity::MassCancelSelection)
                })?;
                Some(selected)
            }
            _ => None,
        };
        Ok(CommandPreparation::Ready(PreparedCommand {
            command,
            book_instance_id: self.instance_id,
            expected_retained_commands: self.reports.len(),
            core_rejection,
            events,
            selected_order_ids,
        }))
    }

    /// Computes a safe command-specific report/trade bound in `O(1)` from
    /// mutation-maintained side and account aggregates.
    fn maximum_report_work(
        &self,
        command: Command,
        core_rejection: Option<RejectReason>,
    ) -> Result<(usize, u64), MatchingError> {
        if core_rejection.is_some() {
            return Ok((1, 0));
        }

        let (events, trades) = match command {
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
            Command::Replace(command) => {
                let old = self
                    .orders
                    .get(&command.order_id)
                    .expect("accepted replacement must reference an active order");
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
            events,
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
            if !matches!(
                command,
                Command::Cancel(_)
                    | Command::MassCancel(_)
                    | Command::AccountControl(AccountControl {
                        action: AccountControlAction::BlockAndCancel,
                        ..
                    })
            ) {
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

    fn check_command_capacity(
        &self,
        command: Command,
        core_rejection: Option<RejectReason>,
    ) -> Result<(), MatchingError> {
        match command {
            Command::New(command) => self.check_new_capacity(command),
            Command::Replace(command) => self.check_replace_capacity(command),
            Command::Cancel(_) | Command::MassCancel(_) => Ok(()),
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
        let OrderType::Limit(price) = command.order_type else {
            return Ok(());
        };
        if !matches!(
            command.time_in_force,
            TimeInForce::GoodTilCancelled | TimeInForce::PostOnly
        ) {
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
            .best_level()
            .map(|(price, level)| Self::level_snapshot(price, &level))
    }

    /// Returns the current best ask.
    #[must_use]
    pub fn best_ask(&self) -> Option<LevelSnapshot> {
        self.asks
            .best_level()
            .map(|(price, level)| Self::level_snapshot(price, &level))
    }

    /// Returns process-local allocation telemetry for one price-level arena.
    ///
    /// The allocated capacity is at least the configured maximum but can be
    /// larger because allocator rounding is permitted. Initialized slots are a
    /// high-water mark; removal moves a slot to the reusable count instead of
    /// deallocating it.
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
            Side::Buy => self
                .bids
                .iter()
                .rev()
                .take(limit)
                .map(|(&price, level)| Self::level_snapshot(price, level))
                .collect(),
            Side::Sell => self
                .asks
                .iter()
                .take(limit)
                .map(|(&price, level)| Self::level_snapshot(price, level))
                .collect(),
        }
    }

    /// Returns one aggregate level, or `None` when the price is unoccupied.
    #[must_use]
    pub fn level(&self, side: Side, price: Price) -> Option<LevelSnapshot> {
        self.levels(side)
            .get(price)
            .map(|level| Self::level_snapshot(price, level))
    }

    /// Returns a resting order by identifier.
    #[must_use]
    pub fn order(&self, order_id: OrderId) -> Option<OrderSnapshot> {
        self.orders.get(&order_id).and_then(|order| {
            Quantity::new(order.leaves)
                .ok()
                .and_then(|leaves_quantity| {
                    Quantity::new(order.displayed)
                        .ok()
                        .map(|displayed_quantity| OrderSnapshot {
                            order_id,
                            account_id: order.account_id,
                            side: order.side,
                            price: order.price,
                            leaves_quantity,
                            displayed_quantity,
                            display: order.display,
                        })
                })
        })
    }

    /// Returns every active order in ascending identifier order.
    ///
    /// This allocates and sorts in `O(O log O)` time. It is intended for
    /// snapshots, recovered-state bootstrap, and diagnostics rather than the
    /// matching hot path.
    ///
    /// # Errors
    ///
    /// Returns [`InvariantViolation`] if an active order contains an impossible
    /// zero total or displayed leaves quantity.
    pub fn active_orders(&self) -> Result<Vec<OrderSnapshot>, InvariantViolation> {
        let mut orders = Vec::with_capacity(self.orders.len());
        for order in self.orders.values() {
            let leaves_quantity = Quantity::new(order.leaves).map_err(|_| {
                InvariantViolation::new(format!(
                    "active order {} has zero leaves quantity",
                    order.order_id
                ))
            })?;
            let displayed_quantity = Quantity::new(order.displayed).map_err(|_| {
                InvariantViolation::new(format!(
                    "active order {} has zero displayed quantity",
                    order.order_id
                ))
            })?;
            orders.push(OrderSnapshot {
                order_id: order.order_id,
                account_id: order.account_id,
                side: order.side,
                price: order.price,
                leaves_quantity,
                displayed_quantity,
                display: order.display,
            });
        }
        orders.sort_unstable_by_key(|order| order.order_id);
        Ok(orders)
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

    /// Returns the number of currently resting orders.
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
    /// This is `O(O + P)` and allocates `O(O)` temporary memory, so it is
    /// intended for tests, snapshots, diagnostics, and replay verification—not
    /// the matching hot path.
    ///
    /// # Errors
    ///
    /// Returns [`InvariantViolation`] at the first detected corruption.
    pub fn validate(&self) -> Result<(), InvariantViolation> {
        self.validate_capacity()?;
        let mut visited = HashSet::with_capacity(self.orders.len());
        self.bids.validate_extremum()?;
        self.asks.validate_extremum()?;
        self.validate_side(Side::Buy, &mut visited)?;
        self.validate_side(Side::Sell, &mut visited)?;
        if visited.len() != self.orders.len() {
            return Err(InvariantViolation::new(format!(
                "{} active orders are absent from price levels",
                self.orders.len() - visited.len()
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
        if self
            .reports
            .values()
            .any(|cached| cached.report.events.len() > self.limits.max_report_events())
        {
            return Err(InvariantViolation::new(
                "retained report exceeds configured event capacity",
            ));
        }
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

    fn validate_account_order_index(&self) -> Result<(), InvariantViolation> {
        let mut indexed = HashSet::with_capacity(self.orders.len());
        for (&account_id, index) in &self.account_orders {
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
                    if !indexed.insert(order_id) {
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
        if indexed.len() != self.orders.len() {
            return Err(InvariantViolation::new(format!(
                "{} active orders are absent from account indexes",
                self.orders.len() - indexed.len()
            )));
        }
        Ok(())
    }

    fn level_snapshot(price: Price, level: &PriceLevel) -> LevelSnapshot {
        LevelSnapshot {
            price,
            quantity: level.total_quantity,
            order_count: level.order_count,
        }
    }

    fn validate_side(
        &self,
        side: Side,
        visited: &mut HashSet<OrderId>,
    ) -> Result<(), InvariantViolation> {
        for (&price, level) in self.levels(side).iter() {
            if level.order_count == 0 || level.total_quantity == 0 {
                return Err(InvariantViolation::new(format!(
                    "empty {side:?} level at price {}",
                    price.raw()
                )));
            }
            let mut current = Some(level.head);
            let mut previous = None;
            let mut count = 0_u64;
            let mut total = 0_u128;
            let mut event_units = 0_u128;
            while let Some(order_id) = current {
                if !visited.insert(order_id) {
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
                if order.previous != previous {
                    return Err(InvariantViolation::new(format!(
                        "order {order_id} has a broken previous FIFO link"
                    )));
                }
                if order.displayed == 0 || order.displayed > order.leaves {
                    return Err(InvariantViolation::new(format!(
                        "order {order_id} has invalid displayed/total leaves"
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
                    _ => {}
                }
                count += 1;
                total += u128::from(order.displayed);
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
            if count != level.order_count
                || total != level.total_quantity
                || event_units != level.event_units
            {
                return Err(InvariantViolation::new(format!(
                    "{side:?} level at {} has inconsistent aggregates",
                    price.raw()
                )));
            }
        }
        Ok(())
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

        let incoming = IncomingOrder {
            order_id: command.order_id,
            account_id: command.account_id,
            side: command.side,
            order_type: command.order_type,
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
        mut selected: Vec<OrderId>,
    ) -> Result<ExecutionReport, MatchingError> {
        let selected_count = self.account_active_order_count(command.account_id, command.scope);
        debug_assert_eq!(events.maximum_events, selected_count + 1);
        debug_assert!(selected.capacity() >= selected_count);
        let cancelled_order_count =
            u64::try_from(selected_count).map_err(|_| MatchingError::SequenceExhausted)?;
        let selected_buffer = selected.as_ptr();
        self.take_account_orders_into(command.account_id, command.scope, &mut selected);
        debug_assert_eq!(selected.len(), selected_count);
        debug_assert_eq!(selected.as_ptr(), selected_buffer);

        let mut cancelled_quantity_lots = 0_u128;
        for order_id in selected {
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
        selected: Option<Vec<OrderId>>,
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
                debug_assert!(selected.capacity() >= selected_count);
                let cancelled_order_count =
                    u64::try_from(selected_count).map_err(|_| MatchingError::SequenceExhausted)?;
                let selected_buffer = selected.as_ptr();
                self.take_account_orders_into(
                    command.account_id,
                    MassCancelScope::All,
                    &mut selected,
                );
                debug_assert_eq!(selected.len(), selected_count);
                debug_assert_eq!(selected.as_ptr(), selected_buffer);

                let mut cancelled_quantity_lots = 0_u128;
                for order_id in selected {
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
            let new = RestingOrder {
                leaves: new_quantity,
                displayed: new_displayed,
                ..old
            };
            let old_event_units = old.remaining_event_units();
            let new_event_units = new.remaining_event_units();
            self.levels_mut(old.side)
                .update(old.price, |level| {
                    level.total_quantity -= u128::from(displayed_reduction);
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
                TimeInForce::GoodTilCancelled,
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
            let Some(resting_id) = self.best_opposite_order(incoming.side, incoming.order_type)
            else {
                break;
            };
            let resting = *self
                .orders
                .get(&resting_id)
                .expect("price-level head must reference an active order");

            if resting.account_id == incoming.account_id {
                self.prevent_self_trade(&mut incoming, resting, command_id, occurred_at, events)?;
                continue;
            }

            let executed = incoming.leaves.min(resting.displayed);
            let trade = self.make_trade(&incoming, &resting, executed)?;
            self.push_event(events, command_id, occurred_at, EventKind::Trade(trade))?;
            incoming.leaves -= executed;
            if let Some(refresh) = self.decrement_resting(resting_id, executed) {
                self.push_refresh(events, command_id, occurred_at, refresh)?;
            }
        }

        if incoming.leaves == 0 {
            return Ok(());
        }
        match (incoming.order_type, time_in_force) {
            (OrderType::Limit(price), TimeInForce::GoodTilCancelled | TimeInForce::PostOnly) => {
                let displayed = incoming.display.displayed_lots(incoming.leaves);
                self.append_order(RestingOrder {
                    order_id: incoming.order_id,
                    account_id: incoming.account_id,
                    side: incoming.side,
                    price,
                    leaves: incoming.leaves,
                    displayed,
                    display: incoming.display,
                    self_trade_prevention: incoming.self_trade_prevention,
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
                        displayed_quantity: Quantity::new(displayed)
                            .expect("displayed resting quantity must be non-zero"),
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
                let removed = self.remove_order(resting.order_id);
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
                let removed = self.remove_order(resting.order_id);
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
                if let Some(refresh) = self.decrement_resting(resting.order_id, prevented) {
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
        }
    }

    fn best_opposite_order(&self, side: Side, order_type: OrderType) -> Option<OrderId> {
        let opposite = side.opposite();
        let (price, level) = self.levels(opposite).best_level()?;
        let crosses = match (side, order_type) {
            (_, OrderType::Market) => true,
            (Side::Buy, OrderType::Limit(limit)) => limit >= price,
            (Side::Sell, OrderType::Limit(limit)) => limit <= price,
        };
        crosses.then_some(level.head)
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
    /// Under an aggressor-blocking policy, the first self order is a FIFO
    /// barrier: only current displayed slices ahead of it are reachable, because
    /// a replenished reserve slice rejoins behind that barrier.
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
        let indexed_previous = self
            .account_orders
            .entry(order.account_id)
            .or_default()
            .append(order.side, order.order_id, event_units);
        assert_eq!(indexed_previous, previous);
        self.append_order_preserving_account_index(order);
    }

    fn append_order_preserving_account_index(&mut self, mut order: RestingOrder) {
        let event_units = order.remaining_event_units();
        let existing_tail = self
            .levels(order.side)
            .get(order.price)
            .map(|level| level.tail);
        order.previous = existing_tail;

        if let Some(tail) = existing_tail {
            self.orders
                .get_mut(&tail)
                .expect("price-level tail must exist")
                .next = Some(order.order_id);
            self.levels_mut(order.side)
                .update(order.price, |level| {
                    level.tail = order.order_id;
                    level.total_quantity += u128::from(order.displayed);
                    level.order_count += 1;
                    level.event_units += event_units;
                })
                .expect("tail implies existing price level");
        } else {
            self.levels_mut(order.side).insert(
                order.price,
                PriceLevel {
                    head: order.order_id,
                    tail: order.order_id,
                    total_quantity: u128::from(order.displayed),
                    order_count: 1,
                    event_units,
                },
            );
        }
        assert!(
            self.orders.insert(order.order_id, order).is_none(),
            "active order identifier must be unique"
        );
    }

    fn decrement_resting(&mut self, order_id: OrderId, quantity: u64) -> Option<ReserveRefresh> {
        let order = *self
            .orders
            .get(&order_id)
            .expect("decremented order must exist");
        debug_assert!(quantity <= order.displayed);
        if quantity == order.leaves {
            self.remove_order(order_id);
            None
        } else if quantity < order.displayed {
            let updated = self
                .orders
                .get_mut(&order_id)
                .expect("decremented order must exist");
            updated.leaves -= quantity;
            updated.displayed -= quantity;
            self.levels_mut(order.side)
                .update(order.price, |level| {
                    level.total_quantity -= u128::from(quantity);
                })
                .expect("resting order must reference a level");
            None
        } else {
            let mut refreshed = self.remove_order_preserving_account_index(order_id);
            let old_event_units = refreshed.remaining_event_units();
            refreshed.leaves -= quantity;
            refreshed.displayed = refreshed.display.displayed_lots(refreshed.leaves);
            refreshed.previous = None;
            refreshed.next = None;
            let result = ReserveRefresh {
                order_id,
                price: refreshed.price,
                displayed: refreshed.displayed,
                leaves: refreshed.leaves,
            };
            let new_event_units = refreshed.remaining_event_units();
            self.account_orders
                .get_mut(&refreshed.account_id)
                .expect("refreshed order account index must exist")
                .replace_event_units(refreshed.side, old_event_units, new_event_units);
            self.append_order_preserving_account_index(refreshed);
            Some(result)
        }
    }

    fn remove_order(&mut self, order_id: OrderId) -> RestingOrder {
        let order = self.remove_order_preserving_account_index(order_id);
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
            order_id,
            order.account_previous,
            order.account_next,
            order.remaining_event_units(),
        );
        if index.is_empty() {
            self.account_orders.remove(&order.account_id);
        }
        order
    }

    fn remove_order_preserving_account_index(&mut self, order_id: OrderId) -> RestingOrder {
        let order = self
            .orders
            .remove(&order_id)
            .expect("validated removal target must exist");
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
            .update(order.price, |level| {
                level.total_quantity -= u128::from(order.displayed);
                level.order_count -= 1;
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
                level.order_count == 0
            })
            .expect("resting order must reference a level");
        if level_is_empty {
            self.levels_mut(order.side).remove(order.price);
        }
        order
    }
}

#[cfg(test)]
mod price_level_index_tests {
    use super::{
        AccountAdmissionState, AccountControl, AccountControlAction, AccountControlSnapshot,
        Command, CommandOutcome, CommandPreparation, EventTraceBuilder, IncomingPreview,
        InvariantViolation, MassCancel, MassCancelScope, NewOrder, OrderBook, OrderBookLimits,
        OrderBookLimitsSpec, OrderDisplay, OrderType, PriceLevel, PriceLevels, RejectReason,
        ReplaceOrder, RestingOrder, SelfTradePrevention, TimeInForce,
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
            base_units_per_lot: 1,
            quote_units_per_price_unit: 1,
            trading_state: TradingState::Open,
        })
        .unwrap()
    }

    fn reference_can_fill(book: &OrderBook, incoming: &NewOrder) -> bool {
        let mut remaining = incoming.quantity.lots();
        let mut price = book.best_price(incoming.side.opposite());
        while let Some(current_price) = price {
            let crosses = match (incoming.side, incoming.order_type) {
                (_, OrderType::Market) => true,
                (Side::Buy, OrderType::Limit(limit)) => limit >= current_price,
                (Side::Sell, OrderType::Limit(limit)) => limit <= current_price,
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
                    order.displayed = order.display.displayed_lots(order.leaves);
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
        let mut book = OrderBook::new(reserve_definition());
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
        })
        .unwrap();
        old
    }

    fn level(order_id: u64) -> PriceLevel {
        let order_id = OrderId::new(order_id).expect("non-zero test order identifier");
        PriceLevel {
            head: order_id,
            tail: order_id,
            total_quantity: u128::from(order_id.get()),
            order_count: 1,
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

        let restored = OrderBook::from_checkpoint(book.checkpoint(1, 7).unwrap()).unwrap();
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
        let prepared_buffer = prepared.events.events.as_ptr();
        let report = book
            .commit(prepared)
            .expect("two-event report fits before sequence exhaustion");
        assert_eq!(report.events.len(), 2);
        assert_eq!(report.events.as_ptr(), prepared_buffer);
        assert_eq!(report.events[0].sequence, u64::MAX - 2);
        assert_eq!(report.events[1].sequence, u64::MAX - 1);
        assert_eq!(book.next_sequence, u64::MAX);
    }

    #[test]
    fn construction_owns_hash_headroom_and_preparation_owns_selection_storage() {
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
            time_in_force: TimeInForce::GoodTilCancelled,
            self_trade_prevention: SelfTradePrevention::CancelAggressor,
            received_at: TimestampNs::from_unix_nanos(1),
        };
        assert!(book.orders.capacity() >= book.limits.max_active_orders());
        assert!(book.account_orders.capacity() >= book.limits.max_active_accounts());
        assert!(book.seen_order_ids.capacity() >= book.limits.max_accepted_order_ids());
        assert!(book.account_controls.capacity() >= book.limits.max_account_controls());
        assert!(book.reports.capacity() >= book.limits.max_retained_commands());
        assert_eq!(book.bids.storage_len(), 0);
        assert!(book.bids.allocation_capacity() >= book.limits.max_price_levels_per_side());
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
        );
        book.commit(prepared).unwrap();
        assert_eq!(book.bids.storage_len(), 1);
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
        assert!(selected.is_empty());
        assert!(selected.capacity() >= 1);
        book.commit(prepared).unwrap();
        assert_eq!(book.active_order_count(), 0);
        assert_eq!(book.bids.storage_len(), 1);
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
        let event_buffer = prepared.events.events.as_ptr();
        let selected = prepared
            .selected_order_ids
            .as_ref()
            .expect("block-and-cancel owns selection storage");
        assert!(selected.is_empty());
        assert!(selected.capacity() >= 1);
        assert_eq!(book.active_order_count(), 1);
        assert_eq!(book.account_control(new.account_id).revision(), 0);
        let report = book.commit(prepared).unwrap();
        assert_eq!(report.events.as_ptr(), event_buffer);
        assert_eq!(book.active_order_count(), 0);
        assert_eq!(book.account_control(new.account_id).revision(), 1);
        assert_eq!(book.account_controls.capacity(), control_capacity);
    }

    #[test]
    fn invariant_validation_rejects_lost_constructor_hash_headroom() {
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
                report.events.0.capacity() >= maximum_events,
                "prepared allocation was smaller than its bound in generated case {case}"
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
            let events = EventTraceBuilder::try_with_capacity(maximum_events)
                .expect("generated replacement trace allocation must succeed");
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
