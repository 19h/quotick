//! A deterministic, single-instrument, price-time-priority matching engine.
//!
//! One [`OrderBook`] is intended to be owned by one execution shard.  It uses
//! an order hash index, ordered per-account/per-side indexes, and intrusive FIFO
//! links inside ordered price levels. New resting orders and cancellations avoid
//! scans of an entire price level or book. A redundant complete best-level cache
//! makes market-extremum discovery `O(1)`; ordered level insertion, deletion,
//! and next-worse traversal remain `O(log P)`, where `P` is the number of
//! occupied price levels. A command consuming `E` displayed maker slices is
//! `O((E + 1) log P)`. A reserve maker can contribute multiple slices. Account
//! mass-cancel selection is `O(K)` for `K` selected orders. FOK inspection uses
//! `O(1)` auxiliary space and visits each active order in crossed levels at most
//! once; it never materializes reserve replenishment slices.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;

use crate::domain::{
    AccountId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs, TradeId,
};
use crate::instrument::{AdmissionError, InstrumentDefinition};

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
}

impl Command {
    const fn command_id(self) -> CommandId {
        match self {
            Self::New(command) => command.command_id,
            Self::Cancel(command) => command.command_id,
            Self::Replace(command) => command.command_id,
            Self::MassCancel(command) => command.command_id,
        }
    }

    const fn received_at(self) -> TimestampNs {
        match self {
            Self::New(command) => command.received_at,
            Self::Cancel(command) => command.received_at,
            Self::Replace(command) => command.received_at,
            Self::MassCancel(command) => command.received_at,
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
    pub events: Vec<Event>,
    /// True when this report came from the exact-command idempotency cache.
    pub replayed: bool,
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

/// A bounded matching resource that prevented command admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MatchingCapacity {
    /// Ordinary new/replace history reached the cancellation-reserve boundary.
    AdmissionCommandHistory,
    /// Total retained command history, including the cancellation reserve.
    CommandHistory,
    /// Never-reusable accepted order identifiers.
    AcceptedOrderIds,
    /// Simultaneously active resting orders.
    ActiveOrders,
    /// Accounts with at least one active resting order.
    ActiveAccounts,
    /// Occupied bid price levels.
    BidPriceLevels,
    /// Occupied ask price levels.
    AskPriceLevels,
}

impl fmt::Display for MatchingCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AdmissionCommandHistory => "ordinary command history",
            Self::CommandHistory => "total command history",
            Self::AcceptedOrderIds => "accepted order identifiers",
            Self::ActiveOrders => "active orders",
            Self::ActiveAccounts => "active accounts",
            Self::BidPriceLevels => "bid price levels",
            Self::AskPriceLevels => "ask price levels",
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
    /// Constructor-time hash reservation failed for the configured resource.
    CapacityReservationFailed(MatchingCapacity),
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
    /// Maximum retained command/report pairs.
    pub max_retained_commands: usize,
    /// Tail history slots reserved exclusively for cancel and mass-cancel commands.
    pub cancellation_reserve: usize,
    /// Whether hash indexes reserve their declared maxima at construction.
    pub preallocate: bool,
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
    /// Retained-command maximum is zero.
    ZeroRetainedCommands,
    /// More active accounts than active orders were configured.
    ActiveAccountsExceedActiveOrders,
    /// More price levels per side than total active orders were configured.
    PriceLevelsExceedActiveOrders,
    /// Active-order capacity exceeds accepted-identifier capacity.
    ActiveOrdersExceedAcceptedOrderIds,
    /// The reserve cannot individually cancel every maximally active order.
    CancellationReserveBelowActiveOrders,
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
            Self::ZeroRetainedCommands => "retained-command limit is zero",
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
    max_retained_commands: usize,
    cancellation_reserve: usize,
    preallocate: bool,
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
    /// Default maximum retained command/report pairs.
    pub const DEFAULT_MAX_RETAINED_COMMANDS: usize = 65_536;
    /// Default tail slots reserved for cancellation controls.
    pub const DEFAULT_CANCELLATION_RESERVE: usize = 4_096;

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
        if spec.max_retained_commands == 0 {
            return Err(OrderBookLimitsError::ZeroRetainedCommands);
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
            max_retained_commands: spec.max_retained_commands,
            cancellation_reserve: spec.cancellation_reserve,
            preallocate: spec.preallocate,
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

    /// Maximum new/replace command history before the cancellation lane begins.
    #[must_use]
    pub const fn ordinary_command_capacity(self) -> usize {
        self.max_retained_commands - self.cancellation_reserve
    }

    /// Whether hash indexes reserve their declared maxima on construction.
    #[must_use]
    pub const fn preallocates(self) -> bool {
        self.preallocate
    }
}

impl Default for OrderBookLimits {
    fn default() -> Self {
        Self::new(OrderBookLimitsSpec {
            max_active_orders: Self::DEFAULT_MAX_ACTIVE_ORDERS,
            max_active_accounts: Self::DEFAULT_MAX_ACTIVE_ACCOUNTS,
            max_price_levels_per_side: Self::DEFAULT_MAX_PRICE_LEVELS_PER_SIDE,
            max_accepted_order_ids: Self::DEFAULT_MAX_ACCEPTED_ORDER_IDS,
            max_retained_commands: Self::DEFAULT_MAX_RETAINED_COMMANDS,
            cancellation_reserve: Self::DEFAULT_CANCELLATION_RESERVE,
            preallocate: false,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PriceLevel {
    head: OrderId,
    tail: OrderId,
    total_quantity: u128,
    order_count: u64,
}

/// Ordered occupied prices for one side with a mutation-maintained market
/// extremum.
///
/// The ordered map remains the authoritative enumeration/index structure. The
/// cached best price is redundant derived state, updated only by `insert` and
/// `remove` and independently checked by `validate_extremum`. Read-only best
/// price discovery therefore does not traverse the tree.
#[derive(Debug, Eq, PartialEq)]
struct PriceLevels {
    side: Side,
    by_price: BTreeMap<Price, PriceLevel>,
    best: Option<(Price, PriceLevel)>,
}

impl PriceLevels {
    fn new(side: Side) -> Self {
        Self {
            side,
            by_price: BTreeMap::new(),
            best: None,
        }
    }

    fn get(&self, price: Price) -> Option<&PriceLevel> {
        self.by_price.get(&price)
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = (&Price, &PriceLevel)> {
        self.by_price.iter()
    }

    fn values(&self) -> impl Iterator<Item = &PriceLevel> {
        self.by_price.values()
    }

    fn len(&self) -> usize {
        self.by_price.len()
    }

    fn best_price(&self) -> Option<Price> {
        self.best.map(|(price, _)| price)
    }

    fn best_level(&self) -> Option<(Price, PriceLevel)> {
        self.best
    }

    fn insert(&mut self, price: Price, level: PriceLevel) -> Option<PriceLevel> {
        let replaced = self.by_price.insert(price, level);
        if self
            .best
            .is_none_or(|(current, _)| current == price || self.is_better(price, current))
        {
            self.best = Some((price, level));
        }
        replaced
    }

    fn update<R>(&mut self, price: Price, update: impl FnOnce(&mut PriceLevel) -> R) -> Option<R> {
        let (result, snapshot) = {
            let level = self.by_price.get_mut(&price)?;
            let result = update(level);
            (result, *level)
        };
        if self.best.is_some_and(|(best_price, _)| best_price == price) {
            self.best = Some((price, snapshot));
        }
        Some(result)
    }

    fn remove(&mut self, price: Price) -> Option<PriceLevel> {
        let removed = self.by_price.remove(&price);
        if removed.is_some() && self.best.is_some_and(|(best_price, _)| best_price == price) {
            self.best = self.map_extremum();
        }
        removed
    }

    fn next_worse(&self, current: Price) -> Option<Price> {
        match self.side {
            Side::Buy => self
                .by_price
                .range(..current)
                .next_back()
                .map(|(&price, _)| price),
            Side::Sell => self
                .by_price
                .range((
                    std::ops::Bound::Excluded(current),
                    std::ops::Bound::Unbounded,
                ))
                .next()
                .map(|(&price, _)| price),
        }
    }

    fn validate_extremum(&self) -> Result<(), InvariantViolation> {
        let actual = self.map_extremum();
        if self.best != actual {
            return Err(InvariantViolation::new(format!(
                "{:?} cached best level {:?} differs from ordered-map extremum {:?}",
                self.side, self.best, actual
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

#[derive(Debug, Default, Eq, PartialEq)]
struct AccountOrderIndex {
    buys: BTreeSet<OrderId>,
    sells: BTreeSet<OrderId>,
}

impl AccountOrderIndex {
    fn orders(&self, side: Side) -> &BTreeSet<OrderId> {
        match side {
            Side::Buy => &self.buys,
            Side::Sell => &self.sells,
        }
    }

    fn orders_mut(&mut self, side: Side) -> &mut BTreeSet<OrderId> {
        match side {
            Side::Buy => &mut self.buys,
            Side::Sell => &mut self.sells,
        }
    }

    fn is_empty(&self) -> bool {
        self.buys.is_empty() && self.sells.is_empty()
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
struct ReserveRefresh {
    order_id: OrderId,
    price: Price,
    displayed: u64,
    leaves: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FokLevelLiquidity {
    Filled,
    Remaining(u64),
    BlockedBySelfTrade,
}

/// A deterministic single-instrument central limit order book.
#[derive(Debug, Eq, PartialEq)]
pub struct OrderBook {
    definition: InstrumentDefinition,
    limits: OrderBookLimits,
    bids: PriceLevels,
    asks: PriceLevels,
    orders: HashMap<OrderId, RestingOrder>,
    account_orders: HashMap<AccountId, AccountOrderIndex>,
    seen_order_ids: HashSet<OrderId>,
    reports: HashMap<CommandId, CachedReport>,
    next_sequence: u64,
    next_trade_id: u64,
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
    /// Panics when constructor-time hash reservation fails.
    #[must_use]
    pub fn with_limits(definition: InstrumentDefinition, limits: OrderBookLimits) -> Self {
        Self::try_with_limits(definition, limits)
            .expect("order-book capacity reservation must succeed under A12")
    }

    /// Creates an empty bounded order book and fallibly reserves configured hash maxima.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError::CapacityReservationFailed`] naming the first
    /// hash index whose requested reservation cannot be represented or allocated.
    pub fn try_with_limits(
        definition: InstrumentDefinition,
        limits: OrderBookLimits,
    ) -> Result<Self, MatchingError> {
        let mut orders = HashMap::new();
        let mut account_orders = HashMap::new();
        let mut seen_order_ids = HashSet::new();
        let mut reports = HashMap::new();
        if limits.preallocates() {
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
            reports
                .try_reserve(limits.max_retained_commands())
                .map_err(|_| {
                    MatchingError::CapacityReservationFailed(MatchingCapacity::CommandHistory)
                })?;
        }
        Ok(Self {
            definition,
            limits,
            bids: PriceLevels::new(Side::Buy),
            asks: PriceLevels::new(Side::Sell),
            orders,
            account_orders,
            seen_order_ids,
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
        if let Some(report) = self.preflight(command)? {
            return Ok(report);
        }
        let report = if let Err(reason) = self.check_business_rules(command) {
            self.rejected(command.command_id(), command.received_at(), reason)?
        } else {
            match command {
                Command::New(new_order) => self.apply_new(new_order)?,
                Command::Cancel(cancel) => self.apply_cancel(cancel)?,
                Command::Replace(replace) => self.apply_replace(replace)?,
                Command::MassCancel(mass_cancel) => self.apply_mass_cancel(mass_cancel)?,
            }
        };
        self.cache_report(command, &report);
        Ok(report)
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
                if order.display.is_reserve() != command.new_display.is_reserve() {
                    return Err(RejectReason::OrderDisplayModeChangeNotAllowed);
                }
            }
            Command::MassCancel(command) => {
                self.definition
                    .admit(Command::MassCancel(command))
                    .map_err(RejectReason::from)?;
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
        if let Some(report) = self.preflight(command)? {
            return Ok(report);
        }
        let reason = self
            .check_business_rules(command)
            .err()
            .unwrap_or(gate_reason);
        let report = self.rejected(command.command_id(), command.received_at(), reason)?;
        self.cache_report(command, &report);
        Ok(report)
    }

    /// Checks idempotency and operational capacity without mutating book state.
    ///
    /// `Ok(Some(report))` is an exact-command replay. `Ok(None)` guarantees that
    /// [`OrderBook::submit`] cannot subsequently fail with an operational error
    /// while this exclusively owned book remains unchanged. Business rejection
    /// remains a normal execution report.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError`] for command-identifier collision, configured
    /// capacity, or sequence/trade-identifier exhaustion.
    pub fn preflight(&self, command: Command) -> Result<Option<ExecutionReport>, MatchingError> {
        let command_id = command.command_id();
        if let Some(cached) = self.reports.get(&command_id) {
            if cached.command != command {
                return Err(MatchingError::CommandIdCollision(command_id));
            }
            let mut report = cached.report.clone();
            report.replayed = true;
            return Ok(Some(report));
        }

        self.check_capacity(command)?;

        // Reserve enough identifier space for the worst case before the first
        // mutation. Each displayed slice can produce at most three events under
        // cancel-both STP. The instrument's per-state replenishment cap provides
        // an O(1) bound without scanning active orders.
        let active_orders =
            u64::try_from(self.orders.len()).map_err(|_| MatchingError::SequenceExhausted)?;
        let maximum_slices_per_order = u64::from(
            self.definition
                .reserve_order_rules()
                .maximum_replenishments(),
        ) + 1;
        let maximum_slices = active_orders
            .checked_mul(maximum_slices_per_order)
            .ok_or(MatchingError::SequenceExhausted)?;
        let maximum_events = maximum_slices
            .checked_mul(3)
            .and_then(|value| value.checked_add(3))
            .ok_or(MatchingError::SequenceExhausted)?;
        self.next_sequence
            .checked_add(maximum_events)
            .ok_or(MatchingError::SequenceExhausted)?;
        self.next_trade_id
            .checked_add(maximum_slices)
            .ok_or(MatchingError::TradeIdExhausted)?;
        Ok(None)
    }

    fn check_capacity(&self, command: Command) -> Result<(), MatchingError> {
        if self.reports.len() >= self.limits.max_retained_commands() {
            return Err(MatchingError::CapacityExhausted(
                MatchingCapacity::CommandHistory,
            ));
        }
        if self.reports.len() >= self.limits.ordinary_command_capacity()
            && !self.is_reserved_control(command)
        {
            return Err(MatchingError::CapacityExhausted(
                MatchingCapacity::AdmissionCommandHistory,
            ));
        }

        match command {
            Command::New(command) => self.check_new_capacity(command),
            Command::Replace(command) => self.check_replace_capacity(command),
            Command::Cancel(_) | Command::MassCancel(_) => Ok(()),
        }
    }

    fn is_reserved_control(&self, command: Command) -> bool {
        matches!(command, Command::Cancel(_) | Command::MassCancel(_))
            && self.check_business_rules(command).is_ok()
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
        let levels = self.levels(command.side);
        if levels.get(price).is_none() && levels.len() >= self.limits.max_price_levels_per_side() {
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
        if levels_after_removal >= self.limits.max_price_levels_per_side() {
            return Err(MatchingError::CapacityExhausted(
                Self::price_level_capacity(order.side),
            ));
        }
        Ok(())
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
            MassCancelScope::All => index.buys.len().saturating_add(index.sells.len()),
            MassCancelScope::Side(side) => index.orders(side).len(),
        }
    }

    /// Returns one account's selected active order IDs in canonical ascending order.
    ///
    /// This allocates `O(K)` output and traverses only the `K` selected orders.
    #[must_use]
    pub fn account_active_order_ids(
        &self,
        account_id: AccountId,
        scope: MassCancelScope,
    ) -> Vec<OrderId> {
        let Some(index) = self.account_orders.get(&account_id) else {
            return Vec::new();
        };
        match scope {
            MassCancelScope::All => index.buys.union(&index.sells).copied().collect(),
            MassCancelScope::Side(side) => index.orders(side).iter().copied().collect(),
        }
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
                for &order_id in index.orders(side) {
                    if !indexed.insert(order_id) {
                        return Err(InvariantViolation::new(format!(
                            "order {order_id} occurs more than once in account indexes"
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
                previous = Some(order_id);
                current = order.next;
            }
            if previous != Some(level.tail) {
                return Err(InvariantViolation::new(format!(
                    "{side:?} level at {} has an incorrect tail",
                    price.raw()
                )));
            }
            if count != level.order_count || total != level.total_quantity {
                return Err(InvariantViolation::new(format!(
                    "{side:?} level at {} has inconsistent aggregates",
                    price.raw()
                )));
            }
        }
        Ok(())
    }

    fn apply_new(&mut self, command: NewOrder) -> Result<ExecutionReport, MatchingError> {
        assert!(
            self.seen_order_ids.len() < self.limits.max_accepted_order_ids(),
            "new-order preflight must reserve accepted-order identity capacity"
        );
        assert!(
            self.seen_order_ids.insert(command.order_id),
            "business preflight must reject reused order identifiers"
        );
        let mut events = Vec::with_capacity(4);
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
            events,
            replayed: false,
        })
    }

    fn apply_cancel(&mut self, command: CancelOrder) -> Result<ExecutionReport, MatchingError> {
        let removed = self.remove_order(command.order_id);
        let mut events = Vec::with_capacity(1);
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
            events,
            replayed: false,
        })
    }

    fn apply_mass_cancel(&mut self, command: MassCancel) -> Result<ExecutionReport, MatchingError> {
        let selected = self.take_account_orders(command.account_id, command.scope);

        let cancelled_order_count =
            u64::try_from(selected.len()).map_err(|_| MatchingError::SequenceExhausted)?;
        let mut cancelled_quantity_lots = 0_u128;
        let mut events = Vec::with_capacity(selected.len().saturating_add(1));
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
            events,
            replayed: false,
        })
    }

    fn apply_replace(&mut self, command: ReplaceOrder) -> Result<ExecutionReport, MatchingError> {
        let old = self
            .orders
            .get(&command.order_id)
            .copied()
            .expect("prechecked replacement order must exist");
        let new_quantity = command.new_quantity.lots();
        let priority_retained = old.price == command.new_price
            && new_quantity <= old.leaves
            && old.display == command.new_display;
        let mut events = Vec::with_capacity(4);
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
            self.levels_mut(old.side)
                .update(old.price, |level| {
                    level.total_quantity -= u128::from(displayed_reduction);
                })
                .expect("resting order must reference an existing level");
            let order = self
                .orders
                .get_mut(&command.order_id)
                .expect("validated order must exist");
            order.leaves = new_quantity;
            order.displayed = new_displayed;
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
            events,
            replayed: false,
        })
    }

    fn rejected(
        &mut self,
        command_id: CommandId,
        occurred_at: TimestampNs,
        reason: RejectReason,
    ) -> Result<ExecutionReport, MatchingError> {
        let mut events = Vec::with_capacity(1);
        self.push_event(
            &mut events,
            command_id,
            occurred_at,
            EventKind::CommandRejected(reason),
        )?;
        Ok(ExecutionReport {
            command_id,
            outcome: CommandOutcome::Rejected(reason),
            events,
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
        events: &mut Vec<Event>,
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
        events: &mut Vec<Event>,
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
        events: &mut Vec<Event>,
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
        events: &mut Vec<Event>,
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
        events: &mut Vec<Event>,
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
        events: &mut Vec<Event>,
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
                FokLevelLiquidity::Filled => return true,
                FokLevelLiquidity::Remaining(next) => remaining = next,
                FokLevelLiquidity::BlockedBySelfTrade => return false,
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
    ) -> FokLevelLiquidity {
        match incoming.self_trade_prevention {
            SelfTradePrevention::CancelResting => {
                self.fok_cancel_resting_liquidity(incoming.account_id, head, remaining)
            }
            SelfTradePrevention::CancelAggressor | SelfTradePrevention::CancelBoth => {
                self.fok_blocking_liquidity(incoming.account_id, head, remaining)
            }
            SelfTradePrevention::DecrementAndCancel => FokLevelLiquidity::BlockedBySelfTrade,
        }
    }

    fn fok_cancel_resting_liquidity(
        &self,
        account_id: AccountId,
        head: OrderId,
        mut remaining: u64,
    ) -> FokLevelLiquidity {
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
                return FokLevelLiquidity::Filled;
            }
        }
        FokLevelLiquidity::Remaining(remaining)
    }

    fn fok_blocking_liquidity(
        &self,
        account_id: AccountId,
        head: OrderId,
        remaining: u64,
    ) -> FokLevelLiquidity {
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
                return FokLevelLiquidity::BlockedBySelfTrade;
            }
            total_path_remaining = total_path_remaining.saturating_sub(order.leaves);
            pre_barrier_remaining = pre_barrier_remaining.saturating_sub(order.displayed);
            if pre_barrier_remaining == 0 {
                return FokLevelLiquidity::Filled;
            }
        }
        if total_path_remaining == 0 {
            FokLevelLiquidity::Filled
        } else {
            FokLevelLiquidity::Remaining(total_path_remaining)
        }
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

    fn take_account_orders(
        &mut self,
        account_id: AccountId,
        scope: MassCancelScope,
    ) -> Vec<OrderId> {
        match scope {
            MassCancelScope::All => self
                .account_orders
                .remove(&account_id)
                .map_or_else(Vec::new, |index| merge_order_ids(index.buys, index.sells)),
            MassCancelScope::Side(side) => {
                let Some(index) = self.account_orders.get_mut(&account_id) else {
                    return Vec::new();
                };
                let selected = std::mem::take(index.orders_mut(side));
                let remove_account = index.is_empty();
                if remove_account {
                    self.account_orders.remove(&account_id);
                }
                selected.into_iter().collect()
            }
        }
    }

    fn levels_mut(&mut self, side: Side) -> &mut PriceLevels {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn append_order(&mut self, order: RestingOrder) {
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
        let inserted = self
            .account_orders
            .entry(order.account_id)
            .or_default()
            .orders_mut(order.side)
            .insert(order.order_id);
        assert!(inserted, "active order must be absent from account index");
        self.append_order_preserving_account_index(order);
    }

    fn append_order_preserving_account_index(&mut self, mut order: RestingOrder) {
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
            self.append_order_preserving_account_index(refreshed);
            Some(result)
        }
    }

    fn remove_order(&mut self, order_id: OrderId) -> RestingOrder {
        let order = self.remove_order_preserving_account_index(order_id);
        let index = self
            .account_orders
            .get_mut(&order.account_id)
            .expect("active order account index must exist");
        assert!(
            index.orders_mut(order.side).remove(&order_id),
            "active order must exist in its account index"
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

fn merge_order_ids(buys: BTreeSet<OrderId>, sells: BTreeSet<OrderId>) -> Vec<OrderId> {
    let capacity = buys
        .len()
        .checked_add(sells.len())
        .expect("disjoint active-order subsets cannot exceed total address space");
    let mut merged = Vec::with_capacity(capacity);
    let mut buys = buys.into_iter().peekable();
    let mut sells = sells.into_iter().peekable();
    while let (Some(buy), Some(sell)) = (buys.peek(), sells.peek()) {
        if buy < sell {
            merged.push(buys.next().expect("peeked buy exists"));
        } else {
            assert_ne!(buy, sell, "active order cannot be indexed on both sides");
            merged.push(sells.next().expect("peeked sell exists"));
        }
    }
    merged.extend(buys);
    merged.extend(sells);
    merged
}

#[cfg(test)]
mod price_level_index_tests {
    use super::{
        InvariantViolation, NewOrder, OrderBook, OrderDisplay, OrderType, PriceLevel, PriceLevels,
        RestingOrder, SelfTradePrevention, TimeInForce,
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
            price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
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
                book.append_order(RestingOrder {
                    order_id: OrderId::new(next_order_id).unwrap(),
                    account_id,
                    side: Side::Sell,
                    price: Price::from_raw(100 + i64::try_from(level_index).unwrap()),
                    leaves,
                    displayed,
                    display,
                    self_trade_prevention: SelfTradePrevention::CancelAggressor,
                    previous: None,
                    next: None,
                });
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
            side: Side::Buy,
            quantity: Quantity::new(generator.bounded(80) + 1).unwrap(),
            display: OrderDisplay::FullyDisplayed,
            order_type: OrderType::Limit(Price::from_raw(102)),
            time_in_force: TimeInForce::FillOrKill,
            self_trade_prevention: policy,
            received_at: TimestampNs::from_unix_nanos(1),
        };
        (book, incoming)
    }

    fn level(order_id: u64) -> PriceLevel {
        let order_id = OrderId::new(order_id).expect("non-zero test order identifier");
        PriceLevel {
            head: order_id,
            tail: order_id,
            total_quantity: u128::from(order_id.get()),
            order_count: 1,
        }
    }

    #[test]
    fn cached_extremum_tracks_both_market_directions_and_non_best_removal() {
        for (side, expected) in [(Side::Buy, 110), (Side::Sell, 90)] {
            let mut levels = PriceLevels::new(side);
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
            let mut levels = PriceLevels::new(side);
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
        let mut levels = PriceLevels::new(Side::Buy);
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
}
