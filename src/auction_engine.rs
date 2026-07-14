//! Sequenced, bounded phase authority for one call-auction collection book.
//!
//! The engine owns one [`CallAuctionBook`], an explicit phase/cycle state, a
//! never-evicted bounded idempotency cache, and contiguous command/event
//! sequences. Business rejections are sequenced; operational capacity,
//! allocator, and counter failures are returned before command commitment.
//! The engine itself is process-local. Stable wire encoding, semantic
//! checkpoints, full-WAL recovery, and checkpoint-anchored prefix cutover are
//! provided by the codec, snapshot, and durable-auction modules. Risk,
//! public/private publication, and settlement remain separate integrations.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::ops::Deref;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::auction::{AuctionClearing, AuctionError, AuctionPriceBand, AuctionPricePolicy};
use crate::auction_book::{
    CallAuctionAdmissionError, CallAuctionBook, CallAuctionBookLimits, CallAuctionCancelError,
    CallAuctionCancellation, CallAuctionCommitError, CallAuctionConstructionError,
    CallAuctionIndicative, CallAuctionInvariantViolation, CallAuctionOrder,
    CallAuctionOrderSnapshot, CallAuctionPrepareError, CallAuctionTrade, CallAuctionUncrossPolicy,
    PreparedCallAuctionUncross,
};
use crate::instrument::{AdmissionError, InstrumentDefinition};
use crate::{
    AccountId, AuctionId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity,
    TimestampNs,
};

static NEXT_CALL_AUCTION_ENGINE_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

fn next_call_auction_engine_instance_id() -> Result<u64, CallAuctionEngineConstructionError> {
    let mut current = NEXT_CALL_AUCTION_ENGINE_INSTANCE_ID.load(Ordering::Relaxed);
    loop {
        if current == u64::MAX {
            return Err(CallAuctionEngineConstructionError::EngineInstanceIdExhausted);
        }
        match NEXT_CALL_AUCTION_ENGINE_INSTANCE_ID.compare_exchange_weak(
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

/// Raw finite resource policy for one sequenced call-auction engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionEngineLimitsSpec {
    /// Collection-book cardinality policy.
    pub book: CallAuctionBookLimits,
    /// Never-evicted exact-command reports retained in this engine generation.
    pub max_retained_commands: usize,
    /// Tail reserved for valid cancel, freeze/close, and uncross commands.
    pub terminal_command_reserve: usize,
    /// Maximum events in one command report.
    pub max_report_events: usize,
}

/// Invalid sequenced call-auction resource policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionEngineLimitsError {
    /// Retained-command maximum is zero.
    ZeroRetainedCommands,
    /// Terminal reserve leaves no ordinary command slot.
    TerminalReserveExhaustsHistory,
    /// Terminal reserve cannot cancel a maximum book and close/freeze it.
    TerminalReserveBelowRequired {
        /// Configured reserve.
        configured: usize,
        /// Minimum derived from the active-order bound.
        required: usize,
    },
    /// Report-event maximum cannot represent a maximum-cardinality uncross.
    ReportEventsBelowRequired {
        /// Configured report bound.
        configured: usize,
        /// Conservative minimum derived from the active-order bound.
        required: usize,
    },
    /// A derived finite bound cannot be represented by `usize`.
    DerivedBoundOverflow,
}

impl fmt::Display for CallAuctionEngineLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRetainedCommands => {
                formatter.write_str("call-auction retained-command limit is zero")
            }
            Self::TerminalReserveExhaustsHistory => formatter
                .write_str("call-auction terminal reserve leaves no ordinary command capacity"),
            Self::TerminalReserveBelowRequired {
                configured,
                required,
            } => write!(
                formatter,
                "call-auction terminal reserve {configured} is below required {required}"
            ),
            Self::ReportEventsBelowRequired {
                configured,
                required,
            } => write!(
                formatter,
                "call-auction report-event limit {configured} is below required {required}"
            ),
            Self::DerivedBoundOverflow => {
                formatter.write_str("call-auction derived resource bound overflows usize")
            }
        }
    }
}

impl std::error::Error for CallAuctionEngineLimitsError {}

/// Validated finite resource policy for one sequenced call-auction engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionEngineLimits {
    book: CallAuctionBookLimits,
    retained_commands: usize,
    terminal_command_reserve: usize,
    report_events: usize,
}

impl CallAuctionEngineLimits {
    /// Default retained exact-command history.
    pub const DEFAULT_MAX_RETAINED_COMMANDS: usize = 65_536;

    /// Validates one complete engine policy.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineLimitsError`] for zero, overflow, or an
    /// incoherent history/report bound.
    pub const fn new(
        spec: CallAuctionEngineLimitsSpec,
    ) -> Result<Self, CallAuctionEngineLimitsError> {
        if spec.max_retained_commands == 0 {
            return Err(CallAuctionEngineLimitsError::ZeroRetainedCommands);
        }
        let Some(required_terminal) = spec.book.max_active_orders().checked_add(2) else {
            return Err(CallAuctionEngineLimitsError::DerivedBoundOverflow);
        };
        if spec.terminal_command_reserve < required_terminal {
            return Err(CallAuctionEngineLimitsError::TerminalReserveBelowRequired {
                configured: spec.terminal_command_reserve,
                required: required_terminal,
            });
        }
        if spec.terminal_command_reserve >= spec.max_retained_commands {
            return Err(CallAuctionEngineLimitsError::TerminalReserveExhaustsHistory);
        }
        let Some(required_events) = spec.book.max_active_orders().checked_mul(2) else {
            return Err(CallAuctionEngineLimitsError::DerivedBoundOverflow);
        };
        let Some(required_events) = required_events.checked_add(1) else {
            return Err(CallAuctionEngineLimitsError::DerivedBoundOverflow);
        };
        if spec.max_report_events < required_events {
            return Err(CallAuctionEngineLimitsError::ReportEventsBelowRequired {
                configured: spec.max_report_events,
                required: required_events,
            });
        }
        Ok(Self {
            book: spec.book,
            retained_commands: spec.max_retained_commands,
            terminal_command_reserve: spec.terminal_command_reserve,
            report_events: spec.max_report_events,
        })
    }

    /// Collection-book cardinality policy.
    #[must_use]
    pub const fn book(self) -> CallAuctionBookLimits {
        self.book
    }

    /// Total never-evicted command/report capacity.
    #[must_use]
    pub const fn max_retained_commands(self) -> usize {
        self.retained_commands
    }

    /// Tail reserved for currently valid terminal controls.
    #[must_use]
    pub const fn terminal_command_reserve(self) -> usize {
        self.terminal_command_reserve
    }

    /// Ordinary command capacity before the terminal reserve.
    #[must_use]
    pub const fn ordinary_command_capacity(self) -> usize {
        self.retained_commands - self.terminal_command_reserve
    }

    /// Maximum events in one report.
    #[must_use]
    pub const fn max_report_events(self) -> usize {
        self.report_events
    }
}

impl Default for CallAuctionEngineLimits {
    fn default() -> Self {
        let book = CallAuctionBookLimits::default();
        Self::new(CallAuctionEngineLimitsSpec {
            book,
            max_retained_commands: Self::DEFAULT_MAX_RETAINED_COMMANDS,
            terminal_command_reserve: book.max_active_orders() + 2,
            max_report_events: book.max_active_orders() * 2 + 1,
        })
        .expect("built-in call-auction engine limits are valid")
    }
}

/// A constructor or preparation resource owned by the sequenced engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionEngineCapacity {
    /// Never-evicted exact-command report index.
    CommandHistory,
    /// Ordinary history prefix, excluding the protected terminal lane.
    AdmissionCommandHistory,
    /// One command's immutable shared event trace.
    ReportEvents,
}

impl fmt::Display for CallAuctionEngineCapacity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CommandHistory => "command history",
            Self::AdmissionCommandHistory => "ordinary command history",
            Self::ReportEvents => "report events",
        })
    }
}

/// Failure while constructing one sequenced call-auction engine.
#[derive(Debug)]
pub enum CallAuctionEngineConstructionError {
    /// Collection-book construction failed.
    Book(CallAuctionConstructionError),
    /// Retained exact-command cache reservation failed.
    CommandHistoryReservationFailed,
    /// Process-local engine identity was exhausted.
    EngineInstanceIdExhausted,
}

impl fmt::Display for CallAuctionEngineConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Book(error) => write!(formatter, "call-auction engine book: {error}"),
            Self::CommandHistoryReservationFailed => {
                formatter.write_str("call-auction command-history reservation failed")
            }
            Self::EngineInstanceIdExhausted => {
                formatter.write_str("call-auction engine instance identity exhausted")
            }
        }
    }
}

impl std::error::Error for CallAuctionEngineConstructionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Book(error) => Some(error),
            Self::CommandHistoryReservationFailed | Self::EngineInstanceIdExhausted => None,
        }
    }
}

impl From<CallAuctionConstructionError> for CallAuctionEngineConstructionError {
    fn from(error: CallAuctionConstructionError) -> Self {
        Self::Book(error)
    }
}

/// Authoritative process-local phase of one call-auction cycle.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CallAuctionPhase {
    /// No cycle accepts new interest; retained interest remains cancelable.
    #[default]
    Closed,
    /// New interest and owner cancellations are admitted.
    Collecting,
    /// Entry is closed; owner cancellation and final uncross remain available.
    Frozen,
}

/// Read-only phase/cycle state.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CallAuctionPhaseSnapshot {
    phase: CallAuctionPhase,
    revision: u64,
    active_auction_id: Option<AuctionId>,
    last_auction_id: Option<AuctionId>,
}

impl CallAuctionPhaseSnapshot {
    pub(crate) const fn from_parts(
        phase: CallAuctionPhase,
        revision: u64,
        active_auction_id: Option<AuctionId>,
        last_auction_id: Option<AuctionId>,
    ) -> Self {
        Self {
            phase,
            revision,
            active_auction_id,
            last_auction_id,
        }
    }

    /// Effective phase.
    #[must_use]
    pub const fn phase(self) -> CallAuctionPhase {
        self.phase
    }

    /// Last accepted phase-control revision; genesis is zero.
    #[must_use]
    pub const fn revision(self) -> u64 {
        self.revision
    }

    /// Current cycle identity outside `Closed`.
    #[must_use]
    pub const fn active_auction_id(self) -> Option<AuctionId> {
        self.active_auction_id
    }

    /// Most recently started cycle identity.
    #[must_use]
    pub const fn last_auction_id(self) -> Option<AuctionId> {
        self.last_auction_id
    }
}

/// Revision-checked phase transition for one instrument-version auction shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionPhaseControl {
    /// Idempotency key.
    pub command_id: CommandId,
    /// Routed instrument.
    pub instrument_id: InstrumentId,
    /// Immutable instrument version.
    pub instrument_version: InstrumentVersion,
    /// Cycle being started or controlled.
    pub auction_id: AuctionId,
    /// Current phase revision observed by the controller.
    pub expected_phase_revision: u64,
    /// Requested phase.
    pub target_phase: CallAuctionPhase,
    /// Gateway/controller receive time.
    pub received_at: TimestampNs,
}

/// Sequenced submission of one collection-book order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionSubmitOrder {
    /// Idempotency key.
    pub command_id: CommandId,
    /// Collection cycle to which this entry belongs.
    pub auction_id: AuctionId,
    /// Current phase revision observed by the submitter.
    pub expected_phase_revision: u64,
    /// Order admitted if collection is active.
    pub order: CallAuctionOrder,
    /// Gateway receive time.
    pub received_at: TimestampNs,
}

/// Sequenced owner cancellation of one auction order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionCancelOrder {
    /// Idempotency key.
    pub command_id: CommandId,
    /// Routed instrument.
    pub instrument_id: InstrumentId,
    /// Immutable instrument version.
    pub instrument_version: InstrumentVersion,
    /// Authorizing owner account.
    pub account_id: AccountId,
    /// Active order to cancel.
    pub order_id: OrderId,
    /// Gateway receive time.
    pub received_at: TimestampNs,
}

/// Revision-checked final uncross command for one frozen auction cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionUncrossCommand {
    /// Idempotency key.
    pub command_id: CommandId,
    /// Routed instrument.
    pub instrument_id: InstrumentId,
    /// Immutable instrument version.
    pub instrument_version: InstrumentVersion,
    /// Frozen cycle being uncrossed.
    pub auction_id: AuctionId,
    /// Current phase revision observed by the controller.
    pub expected_phase_revision: u64,
    /// Authoritative aligned candidate-price band.
    pub price_band: AuctionPriceBand,
    /// Authoritative aligned reference price.
    pub reference_price: Price,
    /// Explicit price-ranking policy.
    pub price_policy: AuctionPricePolicy,
    /// Explicit pairing/remainder policy.
    pub uncross_policy: CallAuctionUncrossPolicy,
    /// Gateway/controller receive time.
    pub received_at: TimestampNs,
}

/// One state-changing sequenced call-auction command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionCommand {
    /// Start, freeze, reopen, or close a cycle.
    PhaseControl(CallAuctionPhaseControl),
    /// Submit new auction interest.
    Submit(CallAuctionSubmitOrder),
    /// Cancel active owned interest.
    Cancel(CallAuctionCancelOrder),
    /// Discover, allocate, pair, and consume one frozen auction.
    Uncross(CallAuctionUncrossCommand),
}

impl CallAuctionCommand {
    /// Returns the idempotency key.
    #[must_use]
    pub const fn command_id(self) -> CommandId {
        match self {
            Self::PhaseControl(command) => command.command_id,
            Self::Submit(command) => command.command_id,
            Self::Cancel(command) => command.command_id,
            Self::Uncross(command) => command.command_id,
        }
    }

    /// Returns the authoritative receive time.
    #[must_use]
    pub const fn received_at(self) -> TimestampNs {
        match self {
            Self::PhaseControl(command) => command.received_at,
            Self::Submit(command) => command.received_at,
            Self::Cancel(command) => command.received_at,
            Self::Uncross(command) => command.received_at,
        }
    }
}

/// Command category used in phase-rejection diagnostics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionAction {
    /// Revisioned phase transition.
    PhaseControl,
    /// New order admission.
    Submit,
    /// Owner cancellation.
    Cancel,
    /// Final uncross.
    Uncross,
}

/// Deterministic business rejection for a sequenced auction command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionRejectReason {
    /// Command was routed to another instrument.
    WrongInstrument,
    /// Command referenced another immutable instrument version.
    WrongInstrumentVersion,
    /// Controller's phase revision was stale or ahead.
    PhaseRevisionMismatch {
        /// Revision supplied by the command.
        observed: u64,
        /// Current revision.
        current: u64,
    },
    /// Transition is absent from the explicit phase graph.
    InvalidPhaseTransition {
        /// Effective source phase.
        from: CallAuctionPhase,
        /// Requested target phase.
        to: CallAuctionPhase,
    },
    /// Command is unavailable in the effective phase.
    ActionNotAllowed {
        /// Attempted action.
        action: CallAuctionAction,
        /// Effective phase.
        phase: CallAuctionPhase,
    },
    /// Command referenced a cycle other than the active one.
    AuctionIdMismatch {
        /// Cycle supplied by the command.
        observed: AuctionId,
        /// Currently active cycle, if any.
        current: Option<AuctionId>,
    },
    /// A newly started cycle did not use the exact next identity.
    AuctionIdNotNext {
        /// Exact next cycle identity.
        expected: AuctionId,
        /// Supplied cycle identity.
        observed: AuctionId,
    },
    /// Immutable instrument admission rules rejected submitted interest.
    Instrument(AdmissionError),
    /// Order identity was already consumed.
    DuplicateOrder,
    /// Cancellation referenced no active order.
    UnknownOrder,
    /// Cancellation account did not own the order.
    NotOrderOwner,
    /// Frozen interest had no executable clearing state.
    NoExecutableInterest,
    /// Submitted account has no immutable risk profile.
    RiskProfileMissing,
    /// Submitted account is blocked by its risk profile.
    RiskAccountBlocked,
    /// Submitted interest is not a strict reduce-only exposure reduction.
    RiskReduceOnly,
    /// Submitted quantity exceeds the per-order risk limit.
    RiskOrderQuantityLimit,
    /// Submitted conservative notional exceeds the per-order risk limit.
    RiskOrderNotionalLimit,
    /// Resting order count would exceed the aggregate risk limit.
    RiskOpenOrderCountLimit,
    /// Resting quantity would exceed the aggregate risk limit.
    RiskOpenQuantityLimit,
    /// Resting conservative notional would exceed the aggregate risk limit.
    RiskOpenNotionalLimit,
    /// Worst-case long or short position would exceed its risk limit.
    RiskPositionLimit,
    /// Risk arithmetic could not represent the requested exposure.
    RiskArithmeticOverflow,
}

/// Why active auction quantity was removed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionCancellationReason {
    /// Explicit owner cancellation command.
    UserRequested,
    /// Unexecuted remainder selected by the uncross policy.
    UncrossRemainder,
}

/// One externally observable sequenced auction transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionEventKind {
    /// Effective phase changed.
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
    /// New interest entered the collection book.
    OrderAccepted(CallAuctionOrderSnapshot),
    /// Active interest was removed.
    OrderCancelled {
        /// Removed order state.
        order: CallAuctionOrderSnapshot,
        /// Removal cause.
        reason: CallAuctionCancellationReason,
    },
    /// One deterministic auction counterparty pair executed.
    Trade(CallAuctionTrade),
    /// One policy-selected unexecuted remainder was removed.
    RemainderCancelled(CallAuctionCancellation),
    /// Final uncross completed and closed the cycle.
    UncrossCompleted {
        /// Closed cycle.
        auction_id: AuctionId,
        /// Aggregate clearing state.
        clearing: AuctionClearing,
        /// Applied uncross policy.
        policy: CallAuctionUncrossPolicy,
        /// Number of emitted counterparty pairs.
        trade_count: u64,
        /// Number of policy-driven remainder cancellations.
        cancellation_count: u64,
        /// Collection-book revision after consumption.
        book_revision: u64,
        /// Newly committed phase revision.
        phase_revision: u64,
    },
    /// Command failed deterministic business validation.
    CommandRejected(CallAuctionRejectReason),
}

/// One event with contiguous engine trace context.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionEvent {
    /// Strictly increasing engine-local event sequence.
    pub sequence: u64,
    /// Command responsible for the event.
    pub command_id: CommandId,
    /// Receive time copied from the command.
    pub occurred_at: TimestampNs,
    /// State transition.
    pub kind: CallAuctionEventKind,
}

/// Shared immutable events for one sequenced command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionEventTrace(Arc<Vec<CallAuctionEvent>>);

impl CallAuctionEventTrace {
    pub(crate) fn from_decoded(events: Vec<CallAuctionEvent>) -> Self {
        Self(Arc::new(events))
    }

    /// Returns the complete event slice.
    #[must_use]
    pub fn as_slice(&self) -> &[CallAuctionEvent] {
        self.0.as_slice()
    }

    /// Returns whether two traces share one immutable owner and vector buffer.
    #[must_use]
    pub fn shares_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Deref for CallAuctionEventTrace {
    type Target = [CallAuctionEvent];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl AsRef<[CallAuctionEvent]> for CallAuctionEventTrace {
    fn as_ref(&self) -> &[CallAuctionEvent] {
        self.as_slice()
    }
}

impl<'a> IntoIterator for &'a CallAuctionEventTrace {
    type Item = &'a CallAuctionEvent;
    type IntoIter = slice::Iter<'a, CallAuctionEvent>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[derive(Debug)]
struct CallAuctionEventTraceBuilder {
    events: Arc<Vec<CallAuctionEvent>>,
    maximum_events: usize,
}

impl CallAuctionEventTraceBuilder {
    fn try_with_capacity(maximum_events: usize) -> Result<Self, CallAuctionEngineError> {
        let mut events = Vec::new();
        events.try_reserve_exact(maximum_events).map_err(|_| {
            CallAuctionEngineError::CapacityReservationFailed(
                CallAuctionEngineCapacity::ReportEvents,
            )
        })?;
        Ok(Self {
            events: Arc::new(events),
            maximum_events,
        })
    }

    fn push(&mut self, event: CallAuctionEvent) {
        assert!(
            self.events.len() < self.maximum_events,
            "prepared auction event bound must cover the complete report"
        );
        Arc::get_mut(&mut self.events)
            .expect("auction event builder must have unique storage")
            .push(event);
    }

    fn finish(self) -> CallAuctionEventTrace {
        CallAuctionEventTrace(self.events)
    }
}

/// Overall command disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionCommandOutcome {
    /// Command committed its requested state transition.
    Accepted,
    /// Command was sequenced but made no state change.
    Rejected(CallAuctionRejectReason),
}

/// Exact immutable report for one sequenced command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionExecutionReport {
    /// Idempotency key.
    pub command_id: CommandId,
    /// Strictly increasing committed-command sequence.
    pub command_sequence: u64,
    /// Accepted/rejected disposition.
    pub outcome: CallAuctionCommandOutcome,
    /// Contiguous immutable event trace.
    pub events: CallAuctionEventTrace,
    /// True only for an exact-command cache replay.
    pub replayed: bool,
}

impl CallAuctionExecutionReport {
    pub(crate) fn from_decoded(
        command_id: CommandId,
        command_sequence: u64,
        outcome: CallAuctionCommandOutcome,
        events: Vec<CallAuctionEvent>,
        replayed: bool,
    ) -> Result<Self, &'static str> {
        if command_sequence == 0 || events.is_empty() {
            return Err("call-auction report has a zero sequence or empty event trace");
        }
        let mut expected_event_sequence = events[0].sequence;
        if expected_event_sequence == 0 {
            return Err("call-auction event sequence is zero");
        }
        for event in &events {
            if event.sequence != expected_event_sequence || event.command_id != command_id {
                return Err("call-auction event trace is noncontiguous or misbound");
            }
            expected_event_sequence = expected_event_sequence
                .checked_add(1)
                .ok_or("call-auction event sequence overflows")?;
        }
        match outcome {
            CallAuctionCommandOutcome::Rejected(reason) => {
                if events.len() != 1
                    || events[0].kind != CallAuctionEventKind::CommandRejected(reason)
                {
                    return Err("call-auction rejected report grammar is inconsistent");
                }
            }
            CallAuctionCommandOutcome::Accepted => {
                if !decoded_accepted_event_grammar_is_valid(&events) {
                    return Err("call-auction accepted report grammar is inconsistent");
                }
            }
        }
        Ok(Self {
            command_id,
            command_sequence,
            outcome,
            events: CallAuctionEventTrace::from_decoded(events),
            replayed,
        })
    }
}

/// One immutable command/report pair retained by an auction checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionCommandReportCheckpoint {
    command: CallAuctionCommand,
    report: CallAuctionExecutionReport,
}

impl CallAuctionCommandReportCheckpoint {
    pub(crate) const fn from_parts(
        command: CallAuctionCommand,
        report: CallAuctionExecutionReport,
    ) -> Self {
        Self { command, report }
    }

    /// Returns the sequenced command.
    #[must_use]
    pub const fn command(&self) -> CallAuctionCommand {
        self.command
    }

    /// Returns the exact non-replayed execution report.
    #[must_use]
    pub const fn report(&self) -> &CallAuctionExecutionReport {
        &self.report
    }
}

/// Canonical direct auction state plus complete idempotency lineage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionCheckpoint {
    wal_metadata_sequence: u64,
    wal_sequence: u64,
    definition: InstrumentDefinition,
    phase: CallAuctionPhaseSnapshot,
    book_revision: u64,
    next_priority_sequence: u64,
    next_trade_id: u64,
    accepted_order_ids: Vec<OrderId>,
    active_orders: Vec<CallAuctionOrderSnapshot>,
    history: Vec<CallAuctionCommandReportCheckpoint>,
}

impl CallAuctionCheckpoint {
    /// Returns the immutable-definition WAL sequence before auction history.
    #[must_use]
    pub const fn wal_metadata_sequence(&self) -> u64 {
        self.wal_metadata_sequence
    }

    /// Returns the completed report boundary represented by this checkpoint.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.wal_sequence
    }

    /// Returns the immutable instrument definition.
    #[must_use]
    pub const fn definition(&self) -> InstrumentDefinition {
        self.definition
    }

    /// Returns effective phase and cycle state.
    #[must_use]
    pub const fn phase(&self) -> CallAuctionPhaseSnapshot {
        self.phase
    }

    /// Returns the direct collection-book mutation revision.
    #[must_use]
    pub const fn book_revision(&self) -> u64 {
        self.book_revision
    }

    /// Returns the next internal collection priority sequence.
    #[must_use]
    pub const fn next_priority_sequence(&self) -> u64 {
        self.next_priority_sequence
    }

    /// Returns the next book-local auction trade identifier.
    #[must_use]
    pub const fn next_trade_id(&self) -> u64 {
        self.next_trade_id
    }

    /// Returns every accepted, never-reusable order identifier canonically.
    #[must_use]
    pub fn accepted_order_ids(&self) -> &[OrderId] {
        &self.accepted_order_ids
    }

    /// Returns canonical active private auction-order state.
    #[must_use]
    pub fn active_orders(&self) -> &[CallAuctionOrderSnapshot] {
        &self.active_orders
    }

    /// Returns chronological command/report idempotency state.
    #[must_use]
    pub fn history(&self) -> &[CallAuctionCommandReportCheckpoint] {
        &self.history
    }

    /// Returns the number of checkpointed commands.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.history.len()
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the stable checkpoint codec exposes each independently validated state component"
    )]
    pub(crate) fn from_parts(
        wal_metadata_sequence: u64,
        wal_sequence: u64,
        definition: InstrumentDefinition,
        phase: CallAuctionPhaseSnapshot,
        book_revision: u64,
        next_priority_sequence: u64,
        next_trade_id: u64,
        accepted_order_ids: Vec<OrderId>,
        active_orders: Vec<CallAuctionOrderSnapshot>,
        history: Vec<CallAuctionCommandReportCheckpoint>,
    ) -> Result<Self, CallAuctionCheckpointError> {
        let checkpoint = Self {
            wal_metadata_sequence,
            wal_sequence,
            definition,
            phase,
            book_revision,
            next_priority_sequence,
            next_trade_id,
            accepted_order_ids,
            active_orders,
            history,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    pub(crate) fn is_successor_of(&self, previous: &Self) -> bool {
        self.wal_metadata_sequence == previous.wal_metadata_sequence
            && self.definition == previous.definition
            && self.wal_sequence >= previous.wal_sequence
            && self.history.starts_with(&previous.history)
    }
}

/// Semantic auction-checkpoint construction or restoration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionCheckpointError {
    detail: String,
}

impl CallAuctionCheckpointError {
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

impl fmt::Display for CallAuctionCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(formatter)
    }
}

impl std::error::Error for CallAuctionCheckpointError {}

impl CallAuctionCheckpoint {
    #[allow(
        clippy::too_many_lines,
        reason = "one chronological projection cross-validates phase, orders, identities, counters, and immutable traces"
    )]
    fn validate(&self) -> Result<(), CallAuctionCheckpointError> {
        if self.wal_metadata_sequence == 0 {
            return Err(CallAuctionCheckpointError::new(
                "auction checkpoint WAL metadata sequence is zero",
            ));
        }
        let history_frames = u64::try_from(self.history.len())
            .map_err(|_| CallAuctionCheckpointError::new("auction history exceeds u64"))?
            .checked_mul(2)
            .ok_or_else(|| {
                CallAuctionCheckpointError::new("auction checkpoint WAL boundary overflows")
            })?;
        if self.wal_metadata_sequence.checked_add(history_frames) != Some(self.wal_sequence) {
            return Err(CallAuctionCheckpointError::new(
                "auction checkpoint WAL boundary does not terminate its history",
            ));
        }

        let mut projected = BTreeMap::<OrderId, CallAuctionOrderSnapshot>::new();
        let mut accepted = Vec::new();
        let mut command_ids = BTreeSet::<CommandId>::new();
        let mut phase_audit = CallAuctionPhaseAudit::default();
        let mut expected_command_sequence = 1_u64;
        let mut expected_event_sequence = 1_u64;
        let mut expected_priority_sequence = 1_u64;
        let mut expected_trade_id = 1_u64;
        let mut expected_book_revision = 0_u64;

        for entry in &self.history {
            let command_id = entry.command.command_id();
            if !command_ids.insert(command_id)
                || entry.report.command_id != command_id
                || entry.report.command_sequence != expected_command_sequence
                || entry.report.replayed
                || entry.report.events.is_empty()
            {
                return Err(CallAuctionCheckpointError::new(
                    "auction checkpoint command/report chronology is invalid",
                ));
            }
            let cached = CachedCallAuctionReport {
                command: entry.command,
                report: entry.report.clone(),
            };
            if !cached_report_grammar_is_valid(&cached)
                || !phase_audit.observe(
                    &cached,
                    self.definition.instrument_id(),
                    self.definition.version(),
                )
            {
                return Err(CallAuctionCheckpointError::new(
                    "auction checkpoint command/report grammar is invalid",
                ));
            }
            for event in entry.report.events.iter().copied() {
                if event.sequence != expected_event_sequence
                    || event.command_id != command_id
                    || event.occurred_at != entry.command.received_at()
                {
                    return Err(CallAuctionCheckpointError::new(
                        "auction checkpoint events are noncontiguous or misbound",
                    ));
                }
                expected_event_sequence =
                    expected_event_sequence.checked_add(1).ok_or_else(|| {
                        CallAuctionCheckpointError::new(
                            "auction checkpoint event sequence is exhausted",
                        )
                    })?;
                if let CallAuctionEventKind::Trade(trade) = event.kind {
                    if trade.trade_id().get() != expected_trade_id {
                        return Err(CallAuctionCheckpointError::new(
                            "auction checkpoint trade identifiers are noncontiguous",
                        ));
                    }
                    expected_trade_id = expected_trade_id.checked_add(1).ok_or_else(|| {
                        CallAuctionCheckpointError::new(
                            "auction checkpoint trade identifier is exhausted",
                        )
                    })?;
                }
            }

            if entry.report.outcome == CallAuctionCommandOutcome::Accepted {
                match entry.command {
                    CallAuctionCommand::PhaseControl(_) => {}
                    CallAuctionCommand::Submit(submit) => {
                        let [
                            CallAuctionEvent {
                                kind: CallAuctionEventKind::OrderAccepted(snapshot),
                                ..
                            },
                        ] = entry.report.events.as_slice()
                        else {
                            return Err(CallAuctionCheckpointError::new(
                                "auction checkpoint accepted submission trace is invalid",
                            ));
                        };
                        if snapshot.priority_sequence != expected_priority_sequence
                            || projected.insert(snapshot.order_id, *snapshot).is_some()
                        {
                            return Err(CallAuctionCheckpointError::new(
                                "auction checkpoint accepted-order priority or identity is invalid",
                            ));
                        }
                        validate_checkpoint_auction_order(
                            self.definition,
                            submit.order,
                            *snapshot,
                        )?;
                        accepted.push(snapshot.order_id);
                        expected_priority_sequence =
                            expected_priority_sequence.checked_add(1).ok_or_else(|| {
                                CallAuctionCheckpointError::new(
                                    "auction checkpoint priority sequence is exhausted",
                                )
                            })?;
                        expected_book_revision =
                            expected_book_revision.checked_add(1).ok_or_else(|| {
                                CallAuctionCheckpointError::new(
                                    "auction checkpoint book revision is exhausted",
                                )
                            })?;
                    }
                    CallAuctionCommand::Cancel(_) => {
                        let [
                            CallAuctionEvent {
                                kind: CallAuctionEventKind::OrderCancelled { order, .. },
                                ..
                            },
                        ] = entry.report.events.as_slice()
                        else {
                            return Err(CallAuctionCheckpointError::new(
                                "auction checkpoint accepted cancellation trace is invalid",
                            ));
                        };
                        if projected.remove(&order.order_id) != Some(*order) {
                            return Err(CallAuctionCheckpointError::new(
                                "auction checkpoint cancellation contradicts projected state",
                            ));
                        }
                        expected_book_revision =
                            expected_book_revision.checked_add(1).ok_or_else(|| {
                                CallAuctionCheckpointError::new(
                                    "auction checkpoint book revision is exhausted",
                                )
                            })?;
                    }
                    CallAuctionCommand::Uncross(_) => {
                        project_checkpoint_uncross(&entry.report, &mut projected)?;
                        expected_book_revision =
                            expected_book_revision.checked_add(1).ok_or_else(|| {
                                CallAuctionCheckpointError::new(
                                    "auction checkpoint book revision is exhausted",
                                )
                            })?;
                        let Some(CallAuctionEvent {
                            kind: CallAuctionEventKind::UncrossCompleted { book_revision, .. },
                            ..
                        }) = entry.report.events.last()
                        else {
                            return Err(CallAuctionCheckpointError::new(
                                "auction checkpoint uncross completion is absent",
                            ));
                        };
                        if *book_revision != expected_book_revision {
                            return Err(CallAuctionCheckpointError::new(
                                "auction checkpoint uncross book revision is invalid",
                            ));
                        }
                    }
                }
            }
            expected_command_sequence =
                expected_command_sequence.checked_add(1).ok_or_else(|| {
                    CallAuctionCheckpointError::new(
                        "auction checkpoint command sequence is exhausted",
                    )
                })?;
        }

        accepted.sort_unstable();
        if accepted != self.accepted_order_ids
            || !strictly_increasing_order_ids(&self.accepted_order_ids)
        {
            return Err(CallAuctionCheckpointError::new(
                "auction checkpoint accepted identities contradict history",
            ));
        }
        if !strictly_increasing_active_orders(&self.active_orders)
            || projected.len() != self.active_orders.len()
            || !projected
                .values()
                .zip(&self.active_orders)
                .all(|(projected, stored)| projected == stored)
        {
            return Err(CallAuctionCheckpointError::new(
                "auction checkpoint active orders contradict event projection",
            ));
        }
        if self.phase
            != CallAuctionPhaseSnapshot::from_parts(
                phase_audit.phase,
                phase_audit.revision,
                phase_audit.active_auction_id,
                phase_audit.last_auction_id,
            )
            || self.book_revision != expected_book_revision
            || self.next_priority_sequence != expected_priority_sequence
            || self.next_trade_id != expected_trade_id
        {
            return Err(CallAuctionCheckpointError::new(
                "auction checkpoint direct counters or phase contradict history",
            ));
        }
        Ok(())
    }
}

fn validate_checkpoint_auction_order(
    definition: InstrumentDefinition,
    order: CallAuctionOrder,
    snapshot: CallAuctionOrderSnapshot,
) -> Result<(), CallAuctionCheckpointError> {
    if snapshot.order_id != order.order_id()
        || snapshot.account_id != order.account_id()
        || snapshot.side != order.side()
        || snapshot.constraint != order.constraint()
        || snapshot.quantity != order.quantity()
    {
        return Err(CallAuctionCheckpointError::new(
            "auction checkpoint accepted snapshot differs from its command",
        ));
    }
    definition
        .quantity_rules()
        .validate(order.quantity())
        .map_err(|_| {
            CallAuctionCheckpointError::new(
                "auction checkpoint accepted quantity violates its definition",
            )
        })?;
    if let crate::auction::AuctionOrderConstraint::Limit(price) = order.constraint() {
        definition.price_rules().validate(price).map_err(|_| {
            CallAuctionCheckpointError::new(
                "auction checkpoint accepted price violates its definition",
            )
        })?;
    }
    Ok(())
}

fn project_checkpoint_uncross(
    report: &CallAuctionExecutionReport,
    projected: &mut BTreeMap<OrderId, CallAuctionOrderSnapshot>,
) -> Result<(), CallAuctionCheckpointError> {
    let mut source_quantities = BTreeMap::new();
    for event in &report.events[..report.events.len() - 1] {
        match event.kind {
            CallAuctionEventKind::Trade(trade) => {
                remember_checkpoint_uncross_source(
                    projected,
                    &mut source_quantities,
                    trade.buy_order_id(),
                )?;
                remember_checkpoint_uncross_source(
                    projected,
                    &mut source_quantities,
                    trade.sell_order_id(),
                )?;
                reduce_projected_order(
                    projected,
                    trade.buy_order_id(),
                    trade.buy_account_id(),
                    crate::Side::Buy,
                    trade.price(),
                    trade.quantity().lots(),
                )?;
                reduce_projected_order(
                    projected,
                    trade.sell_order_id(),
                    trade.sell_account_id(),
                    crate::Side::Sell,
                    trade.price(),
                    trade.quantity().lots(),
                )?;
            }
            CallAuctionEventKind::RemainderCancelled(cancellation) => {
                let Some(order) = projected.remove(&cancellation.order_id()) else {
                    return Err(CallAuctionCheckpointError::new(
                        "auction checkpoint remainder cancellation names an inactive order",
                    ));
                };
                let source_quantity = source_quantities
                    .get(&cancellation.order_id())
                    .copied()
                    .unwrap_or_else(|| order.quantity.lots());
                if order.account_id != cancellation.account_id()
                    || order.side != cancellation.side()
                    || order.constraint != cancellation.constraint()
                    || order.quantity != cancellation.quantity()
                    || cancellation
                        .quantity()
                        .lots()
                        .checked_add(cancellation.executed_quantity_lots())
                        != Some(source_quantity)
                {
                    return Err(CallAuctionCheckpointError::new(
                        "auction checkpoint remainder cancellation contradicts projected state",
                    ));
                }
            }
            _ => {
                return Err(CallAuctionCheckpointError::new(
                    "auction checkpoint uncross body contains an invalid event",
                ));
            }
        }
    }
    Ok(())
}

fn remember_checkpoint_uncross_source(
    projected: &BTreeMap<OrderId, CallAuctionOrderSnapshot>,
    source_quantities: &mut BTreeMap<OrderId, u64>,
    order_id: OrderId,
) -> Result<(), CallAuctionCheckpointError> {
    if source_quantities.contains_key(&order_id) {
        return Ok(());
    }
    let quantity = projected
        .get(&order_id)
        .map(|order| order.quantity.lots())
        .ok_or_else(|| {
            CallAuctionCheckpointError::new(
                "auction checkpoint trade lacks an active source quantity",
            )
        })?;
    source_quantities.insert(order_id, quantity);
    Ok(())
}

fn reduce_projected_order(
    projected: &mut BTreeMap<OrderId, CallAuctionOrderSnapshot>,
    order_id: OrderId,
    account_id: AccountId,
    side: crate::Side,
    price: Price,
    executed_lots: u64,
) -> Result<(), CallAuctionCheckpointError> {
    let order = projected.get(&order_id).copied().ok_or_else(|| {
        CallAuctionCheckpointError::new("auction checkpoint trade names an inactive order")
    })?;
    let price_is_eligible = match (side, order.constraint) {
        (_, crate::auction::AuctionOrderConstraint::Market) => true,
        (crate::Side::Buy, crate::auction::AuctionOrderConstraint::Limit(limit)) => limit >= price,
        (crate::Side::Sell, crate::auction::AuctionOrderConstraint::Limit(limit)) => limit <= price,
    };
    if order.account_id != account_id
        || order.side != side
        || !price_is_eligible
        || executed_lots == 0
        || executed_lots > order.quantity.lots()
    {
        return Err(CallAuctionCheckpointError::new(
            "auction checkpoint trade contradicts projected order state",
        ));
    }
    if executed_lots == order.quantity.lots() {
        projected.remove(&order_id);
    } else {
        let remaining = Quantity::new(order.quantity.lots() - executed_lots).map_err(|_| {
            CallAuctionCheckpointError::new("auction checkpoint trade leaves zero quantity")
        })?;
        projected
            .get_mut(&order_id)
            .expect("projected auction order was found above")
            .quantity = remaining;
    }
    Ok(())
}

fn strictly_increasing_order_ids(values: &[OrderId]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn strictly_increasing_active_orders(values: &[CallAuctionOrderSnapshot]) -> bool {
    values
        .windows(2)
        .all(|pair| pair[0].order_id < pair[1].order_id)
}

fn validate_call_auction_checkpoint_capacity(
    checkpoint: &CallAuctionCheckpoint,
    limits: CallAuctionEngineLimits,
) -> Result<(), CallAuctionCheckpointError> {
    if checkpoint.history.len() > limits.max_retained_commands() {
        return Err(CallAuctionCheckpointError::new(
            "auction checkpoint history exceeds selected capacity",
        ));
    }
    if checkpoint
        .history
        .iter()
        .any(|entry| entry.report.events.len() > limits.max_report_events())
    {
        return Err(CallAuctionCheckpointError::new(
            "auction checkpoint report exceeds selected event capacity",
        ));
    }
    let book_limits = limits.book();
    if checkpoint.accepted_order_ids.len() > book_limits.max_accepted_order_ids() {
        return Err(CallAuctionCheckpointError::new(
            "auction checkpoint accepted identities exceed selected capacity",
        ));
    }
    if checkpoint.active_orders.len() > book_limits.max_active_orders() {
        return Err(CallAuctionCheckpointError::new(
            "auction checkpoint active orders exceed selected capacity",
        ));
    }
    let mut bid_prices = BTreeSet::new();
    let mut ask_prices = BTreeSet::new();
    for order in &checkpoint.active_orders {
        let crate::auction::AuctionOrderConstraint::Limit(price) = order.constraint else {
            continue;
        };
        match order.side {
            crate::Side::Buy => {
                bid_prices.insert(price);
            }
            crate::Side::Sell => {
                ask_prices.insert(price);
            }
        }
    }
    if bid_prices.len() > book_limits.max_price_levels_per_side()
        || ask_prices.len() > book_limits.max_price_levels_per_side()
    {
        return Err(CallAuctionCheckpointError::new(
            "auction checkpoint price levels exceed selected capacity",
        ));
    }
    Ok(())
}

fn decoded_accepted_event_grammar_is_valid(events: &[CallAuctionEvent]) -> bool {
    match events {
        [
            CallAuctionEvent {
                kind:
                    CallAuctionEventKind::PhaseChanged {
                        previous,
                        current,
                        revision,
                        ..
                    },
                ..
            },
        ] => decoded_phase_transition_is_valid(*previous, *current) && *revision != 0,
        [
            CallAuctionEvent {
                kind:
                    CallAuctionEventKind::OrderAccepted(_)
                    | CallAuctionEventKind::OrderCancelled {
                        reason: CallAuctionCancellationReason::UserRequested,
                        ..
                    },
                ..
            },
        ] => true,
        _ => decoded_uncross_event_grammar_is_valid(events),
    }
}

const fn decoded_phase_transition_is_valid(
    previous: CallAuctionPhase,
    current: CallAuctionPhase,
) -> bool {
    matches!(
        (previous, current),
        (
            CallAuctionPhase::Closed | CallAuctionPhase::Frozen,
            CallAuctionPhase::Collecting
        ) | (CallAuctionPhase::Collecting, CallAuctionPhase::Frozen)
            | (
                CallAuctionPhase::Collecting | CallAuctionPhase::Frozen,
                CallAuctionPhase::Closed
            )
    )
}

fn decoded_uncross_event_grammar_is_valid(events: &[CallAuctionEvent]) -> bool {
    let Some(CallAuctionEvent {
        kind:
            CallAuctionEventKind::UncrossCompleted {
                clearing,
                trade_count,
                cancellation_count,
                book_revision,
                phase_revision,
                ..
            },
        ..
    }) = events.last()
    else {
        return false;
    };
    let (Ok(trade_count), Ok(cancellation_count)) = (
        usize::try_from(*trade_count),
        usize::try_from(*cancellation_count),
    ) else {
        return false;
    };
    let Some(body_count) = trade_count.checked_add(cancellation_count) else {
        return false;
    };
    let body = &events[..events.len() - 1];
    if trade_count == 0 || body.len() != body_count || *book_revision == 0 || *phase_revision == 0 {
        return false;
    }
    let (trades, cancellations) = body.split_at(trade_count);
    let mut executed = 0_u128;
    let mut previous_trade_id: Option<u64> = None;
    for event in trades {
        let CallAuctionEventKind::Trade(trade) = event.kind else {
            return false;
        };
        if trade.price() != clearing.price()
            || previous_trade_id
                .is_some_and(|previous| previous.checked_add(1) != Some(trade.trade_id().get()))
        {
            return false;
        }
        previous_trade_id = Some(trade.trade_id().get());
        let Some(next) = executed.checked_add(u128::from(trade.quantity().lots())) else {
            return false;
        };
        executed = next;
    }
    cancellations
        .iter()
        .all(|event| matches!(event.kind, CallAuctionEventKind::RemainderCancelled(_)))
        && executed == clearing.executable_quantity()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CachedCallAuctionReport {
    command: CallAuctionCommand,
    report: CallAuctionExecutionReport,
}

/// Operational failure outside deterministic business rejection semantics.
#[derive(Debug)]
pub enum CallAuctionEngineError {
    /// A command identifier was reused for different command bytes/fields.
    CommandIdCollision(CommandId),
    /// Configured semantic capacity was exhausted before sequencing.
    CapacityExhausted(CallAuctionEngineCapacity),
    /// Fallible report reservation failed.
    CapacityReservationFailed(CallAuctionEngineCapacity),
    /// Committed-command or event sequence cannot advance.
    SequenceExhausted,
    /// Phase revision cannot advance.
    PhaseRevisionExhausted,
    /// Exact next auction identity cannot be represented.
    AuctionIdExhausted,
    /// Collection admission encountered an operational failure.
    BookAdmission(CallAuctionAdmissionError),
    /// Collection cancellation encountered an operational failure.
    BookCancellation(CallAuctionCancelError),
    /// Indicative discovery encountered invalid/arithmetic input.
    Discovery(AuctionError),
    /// Allocation/pairing preparation failed before sequencing.
    UncrossPreparation(CallAuctionPrepareError),
    /// A validated uncross preparation contradicted commit state.
    UncrossCommit(CallAuctionCommitError),
    /// Prepared command belongs to another engine.
    ForeignPreparation,
    /// An unrelated command committed after preparation.
    StalePreparation,
    /// Private state contradicted successful preparation.
    InternalInvariantViolation,
}

impl fmt::Display for CallAuctionEngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CommandIdCollision(command_id) => {
                write!(
                    formatter,
                    "call-auction command identifier collision {command_id}"
                )
            }
            Self::CapacityExhausted(capacity) => {
                write!(formatter, "call-auction {capacity} capacity exhausted")
            }
            Self::CapacityReservationFailed(capacity) => {
                write!(formatter, "call-auction {capacity} reservation failed")
            }
            Self::SequenceExhausted => formatter.write_str("call-auction sequence exhausted"),
            Self::PhaseRevisionExhausted => {
                formatter.write_str("call-auction phase revision exhausted")
            }
            Self::AuctionIdExhausted => {
                formatter.write_str("call-auction cycle identifier exhausted")
            }
            Self::BookAdmission(error) => write!(formatter, "call-auction admission: {error}"),
            Self::BookCancellation(error) => {
                write!(formatter, "call-auction cancellation: {error}")
            }
            Self::Discovery(error) => write!(formatter, "call-auction discovery: {error}"),
            Self::UncrossPreparation(error) => {
                write!(formatter, "call-auction uncross preparation: {error}")
            }
            Self::UncrossCommit(error) => write!(formatter, "call-auction uncross commit: {error}"),
            Self::ForeignPreparation => {
                formatter.write_str("foreign sequenced call-auction preparation")
            }
            Self::StalePreparation => {
                formatter.write_str("stale sequenced call-auction preparation")
            }
            Self::InternalInvariantViolation => {
                formatter.write_str("sequenced call-auction private invariant violation")
            }
        }
    }
}

impl std::error::Error for CallAuctionEngineError {}

#[derive(Debug)]
enum PreparedCallAuctionAction {
    Rejected(CallAuctionRejectReason),
    PhaseTransition {
        auction_id: AuctionId,
        previous: CallAuctionPhase,
        current: CallAuctionPhase,
        revision: u64,
        active_after: Option<AuctionId>,
        last_after: Option<AuctionId>,
    },
    Submit(CallAuctionOrder),
    Cancel {
        account_id: AccountId,
        order_id: OrderId,
    },
    Uncross {
        auction_id: AuctionId,
        phase_revision: u64,
        prepared: PreparedCallAuctionUncross,
    },
}

impl PreparedCallAuctionAction {
    fn maximum_events(&self) -> Result<usize, CallAuctionEngineError> {
        match self {
            Self::Uncross { prepared, .. } => prepared
                .trades()
                .len()
                .checked_add(prepared.cancellations().len())
                .and_then(|value| value.checked_add(1))
                .ok_or(CallAuctionEngineError::SequenceExhausted),
            Self::Rejected(_)
            | Self::PhaseTransition { .. }
            | Self::Submit(_)
            | Self::Cancel { .. } => Ok(1),
        }
    }

    const fn is_terminal_lane_eligible(&self, command: CallAuctionCommand) -> bool {
        matches!(
            (self, command),
            (Self::Cancel { .. }, CallAuctionCommand::Cancel(_))
                | (Self::Uncross { .. }, CallAuctionCommand::Uncross(_))
                | (
                    Self::PhaseTransition { .. },
                    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
                        target_phase: CallAuctionPhase::Frozen | CallAuctionPhase::Closed,
                        ..
                    }),
                )
        )
    }
}

/// Result of preparing one sequenced auction command.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "the move-only ready token stays inline to avoid an untyped Box allocation"
)]
pub enum CallAuctionCommandPreparation {
    /// Exact command is already retained.
    Replay(CallAuctionExecutionReport),
    /// Complete operational and business preflight succeeded.
    Ready(PreparedCallAuctionCommand),
}

/// Opaque single-use command preparation suitable for a future durable wrapper.
#[derive(Debug)]
pub struct PreparedCallAuctionCommand {
    command: CallAuctionCommand,
    engine_instance_id: u64,
    expected_retained_commands: usize,
    action: PreparedCallAuctionAction,
    events: CallAuctionEventTraceBuilder,
}

impl PreparedCallAuctionCommand {
    /// Returns the exact command represented by this preparation.
    #[must_use]
    pub const fn command(&self) -> CallAuctionCommand {
        self.command
    }

    pub(crate) const fn core_rejection(&self) -> Option<CallAuctionRejectReason> {
        match self.action {
            PreparedCallAuctionAction::Rejected(reason) => Some(reason),
            _ => None,
        }
    }
}

/// Process-local resource telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionEngineResourceStatus {
    /// Configured retained-command maximum.
    pub configured_retained_commands: usize,
    /// Hash capacity currently owned by the exact-command cache.
    pub allocated_retained_commands: usize,
    /// Committed command/report count.
    pub retained_commands: usize,
    /// Ordinary command capacity before the terminal lane.
    pub ordinary_command_capacity: usize,
    /// Protected terminal-command slots.
    pub terminal_command_reserve: usize,
    /// Maximum events per report.
    pub max_report_events: usize,
}

/// Unsequenced indicative-query failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionQueryError {
    /// Query referenced no active cycle or the wrong cycle.
    AuctionIdMismatch {
        /// Supplied identity.
        observed: AuctionId,
        /// Active identity, if any.
        current: Option<AuctionId>,
    },
    /// Indicative discovery failed input/arithmetic validation.
    Discovery(AuctionError),
}

impl fmt::Display for CallAuctionQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuctionIdMismatch { observed, current } => write!(
                formatter,
                "call-auction query cycle {observed} differs from active {current:?}"
            ),
            Self::Discovery(error) => write!(formatter, "call-auction query discovery: {error}"),
        }
    }
}

impl std::error::Error for CallAuctionQueryError {}

/// Sequenced phase authority and idempotency owner for one call-auction book.
#[derive(Debug)]
pub struct CallAuctionEngine {
    instance_id: u64,
    limits: CallAuctionEngineLimits,
    book: CallAuctionBook,
    phase: CallAuctionPhase,
    phase_revision: u64,
    active_auction_id: Option<AuctionId>,
    last_auction_id: Option<AuctionId>,
    next_command_sequence: u64,
    next_event_sequence: u64,
    reports: HashMap<CommandId, CachedCallAuctionReport>,
}

impl CallAuctionEngine {
    /// Constructs an empty engine under default bounded limits.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineConstructionError`] for any complete resource
    /// reservation, instrument configuration, or instance-identity failure.
    pub fn try_new(
        definition: crate::instrument::InstrumentDefinition,
    ) -> Result<Self, CallAuctionEngineConstructionError> {
        Self::try_with_limits(definition, CallAuctionEngineLimits::default())
    }

    /// Constructs an empty engine under explicit validated limits.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineConstructionError`] before state exists.
    pub fn try_with_limits(
        definition: crate::instrument::InstrumentDefinition,
        limits: CallAuctionEngineLimits,
    ) -> Result<Self, CallAuctionEngineConstructionError> {
        let book = CallAuctionBook::try_with_limits(definition, limits.book())?;
        let mut reports = HashMap::new();
        reports
            .try_reserve(limits.max_retained_commands())
            .map_err(|_| CallAuctionEngineConstructionError::CommandHistoryReservationFailed)?;
        let instance_id = next_call_auction_engine_instance_id()?;
        Ok(Self {
            instance_id,
            limits,
            book,
            phase: CallAuctionPhase::Closed,
            phase_revision: 0,
            active_auction_id: None,
            last_auction_id: None,
            next_command_sequence: 1,
            next_event_sequence: 1,
            reports,
        })
    }

    /// Returns the immutable collection book.
    #[must_use]
    pub const fn book(&self) -> &CallAuctionBook {
        &self.book
    }

    /// Returns the validated resource policy.
    #[must_use]
    pub const fn limits(&self) -> CallAuctionEngineLimits {
        self.limits
    }

    /// Returns effective phase and cycle identity.
    #[must_use]
    pub const fn phase_snapshot(&self) -> CallAuctionPhaseSnapshot {
        CallAuctionPhaseSnapshot {
            phase: self.phase,
            revision: self.phase_revision,
            active_auction_id: self.active_auction_id,
            last_auction_id: self.last_auction_id,
        }
    }

    /// Returns the next committed-command sequence.
    #[must_use]
    pub const fn next_command_sequence(&self) -> u64 {
        self.next_command_sequence
    }

    /// Returns the next event sequence.
    #[must_use]
    pub const fn next_event_sequence(&self) -> u64 {
        self.next_event_sequence
    }

    /// Returns command-cache occupancy and capacity.
    #[must_use]
    pub fn resource_status(&self) -> CallAuctionEngineResourceStatus {
        CallAuctionEngineResourceStatus {
            configured_retained_commands: self.limits.max_retained_commands(),
            allocated_retained_commands: self.reports.capacity(),
            retained_commands: self.reports.len(),
            ordinary_command_capacity: self.limits.ordinary_command_capacity(),
            terminal_command_reserve: self.limits.terminal_command_reserve(),
            max_report_events: self.limits.max_report_events(),
        }
    }

    /// Captures canonical direct state at one completed WAL report boundary.
    ///
    /// Capture independently replays complete retained history and requires the
    /// reproduced direct state to equal the live engine before returning.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionCheckpointError`] for an invalid live structure,
    /// inconsistent WAL boundary, replay divergence, or reconstruction failure.
    pub fn checkpoint(
        &self,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<CallAuctionCheckpoint, CallAuctionCheckpointError> {
        let checkpoint = self.checkpoint_state(wal_metadata_sequence, wal_sequence)?;
        let mut replay = Self::try_with_limits(self.book.definition(), self.limits)
            .map_err(|error| CallAuctionCheckpointError::new(error.to_string()))?;
        for entry in checkpoint.history() {
            let preparation = replay.prepare(entry.command()).map_err(|error| {
                CallAuctionCheckpointError::new(format!(
                    "auction checkpoint history cannot be prepared: {error}"
                ))
            })?;
            let reproduced = match preparation {
                CallAuctionCommandPreparation::Replay(_) => {
                    return Err(CallAuctionCheckpointError::new(
                        "auction checkpoint history unexpectedly replays a command",
                    ));
                }
                CallAuctionCommandPreparation::Ready(prepared) => {
                    let external_rejection = match entry.report().outcome {
                        CallAuctionCommandOutcome::Rejected(reason)
                            if is_external_risk_rejection(reason) =>
                        {
                            Some(reason)
                        }
                        _ => None,
                    };
                    replay
                        .commit_with_gate(prepared, external_rejection)
                        .map_err(|error| {
                            CallAuctionCheckpointError::new(format!(
                                "auction checkpoint history cannot be committed: {error}"
                            ))
                        })?
                }
            };
            if reproduced != *entry.report() {
                return Err(CallAuctionCheckpointError::new(
                    "auction checkpoint history diverges under deterministic replay",
                ));
            }
        }
        let replayed = replay.checkpoint_state(wal_metadata_sequence, wal_sequence)?;
        if replayed != checkpoint {
            return Err(CallAuctionCheckpointError::new(
                "auction checkpoint direct state differs from deterministic history replay",
            ));
        }
        Ok(checkpoint)
    }

    fn checkpoint_state(
        &self,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<CallAuctionCheckpoint, CallAuctionCheckpointError> {
        self.validate()
            .map_err(|error| CallAuctionCheckpointError::new(error.detail()))?;
        let mut history: Vec<_> = self
            .reports
            .values()
            .map(|cached| CallAuctionCommandReportCheckpoint {
                command: cached.command,
                report: cached.report.clone(),
            })
            .collect();
        history.sort_unstable_by_key(|entry| entry.report.command_sequence);
        CallAuctionCheckpoint::from_parts(
            wal_metadata_sequence,
            wal_sequence,
            self.book.definition(),
            self.phase_snapshot(),
            self.book.state_revision(),
            self.book.next_priority_sequence(),
            self.book.next_trade_id(),
            self.book.checkpoint_accepted_order_ids(),
            self.book.checkpoint_active_orders(),
            history,
        )
    }

    /// Restores a directly indexed auction engine under default finite limits.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionCheckpointError`] for semantic, capacity,
    /// construction, or reconstructed invariant failure.
    pub fn from_checkpoint(
        checkpoint: CallAuctionCheckpoint,
    ) -> Result<Self, CallAuctionCheckpointError> {
        Self::from_checkpoint_with_limits(checkpoint, CallAuctionEngineLimits::default())
    }

    /// Restores a checkpoint under an explicit current finite resource policy.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionCheckpointError`] when retained state exceeds the
    /// policy or any semantic/structural invariant fails.
    pub fn from_checkpoint_with_limits(
        checkpoint: CallAuctionCheckpoint,
        limits: CallAuctionEngineLimits,
    ) -> Result<Self, CallAuctionCheckpointError> {
        checkpoint.validate()?;
        validate_call_auction_checkpoint_capacity(&checkpoint, limits)?;
        let mut engine = Self::try_with_limits(checkpoint.definition, limits)
            .map_err(|error| CallAuctionCheckpointError::new(error.to_string()))?;
        engine.book = CallAuctionBook::from_checkpoint_state(
            checkpoint.definition,
            limits.book(),
            &checkpoint.active_orders,
            &checkpoint.accepted_order_ids,
            checkpoint.next_priority_sequence,
            checkpoint.next_trade_id,
            checkpoint.book_revision,
        )
        .map_err(CallAuctionCheckpointError::new)?;
        engine.phase = checkpoint.phase.phase;
        engine.phase_revision = checkpoint.phase.revision;
        engine.active_auction_id = checkpoint.phase.active_auction_id;
        engine.last_auction_id = checkpoint.phase.last_auction_id;
        engine.next_command_sequence = u64::try_from(checkpoint.history.len())
            .map_err(|_| CallAuctionCheckpointError::new("auction history exceeds u64"))?
            .checked_add(1)
            .ok_or_else(|| {
                CallAuctionCheckpointError::new("auction command sequence is exhausted")
            })?;
        engine.next_event_sequence = checkpoint
            .history
            .last()
            .and_then(|entry| entry.report.events.last())
            .map_or(Ok(1), |event| {
                event.sequence.checked_add(1).ok_or_else(|| {
                    CallAuctionCheckpointError::new("auction event sequence is exhausted")
                })
            })?;
        for entry in checkpoint.history {
            engine.reports.insert(
                entry.command.command_id(),
                CachedCallAuctionReport {
                    command: entry.command,
                    report: entry.report,
                },
            );
        }
        engine
            .validate()
            .map_err(|error| CallAuctionCheckpointError::new(error.detail()))?;
        Ok(engine)
    }

    /// Computes an unsequenced indicative state for the active cycle.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionQueryError`] for a wrong cycle or discovery input.
    pub fn indicative(
        &mut self,
        auction_id: AuctionId,
        price_band: AuctionPriceBand,
        reference_price: Price,
        policy: AuctionPricePolicy,
    ) -> Result<Option<CallAuctionIndicative>, CallAuctionQueryError> {
        if self.active_auction_id != Some(auction_id) {
            return Err(CallAuctionQueryError::AuctionIdMismatch {
                observed: auction_id,
                current: self.active_auction_id,
            });
        }
        self.book
            .indicative_clearing(price_band, reference_price, policy)
            .map_err(CallAuctionQueryError::Discovery)
    }

    /// Prepares and commits one command.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] only for unsequenced operational
    /// failure. Deterministic business failures are returned as rejected reports.
    pub fn submit(
        &mut self,
        command: CallAuctionCommand,
    ) -> Result<CallAuctionExecutionReport, CallAuctionEngineError> {
        match self.prepare(command)? {
            CallAuctionCommandPreparation::Replay(report) => Ok(report),
            CallAuctionCommandPreparation::Ready(prepared) => self.commit(prepared),
        }
    }

    /// Fully preflights one command without semantic state mutation.
    ///
    /// Exact retries precede every capacity gate. The returned move-only token
    /// owns complete report storage and, for an uncross, all allocation/pairing
    /// storage before a future durable append point.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] for collision, capacity, counter, or
    /// underlying analytical preparation failure.
    pub fn prepare(
        &mut self,
        command: CallAuctionCommand,
    ) -> Result<CallAuctionCommandPreparation, CallAuctionEngineError> {
        let command_id = command.command_id();
        if let Some(cached) = self.reports.get(&command_id) {
            if cached.command != command {
                return Err(CallAuctionEngineError::CommandIdCollision(command_id));
            }
            let mut report = cached.report.clone();
            report.replayed = true;
            return Ok(CallAuctionCommandPreparation::Replay(report));
        }

        let action = self.prepare_action(command)?;
        self.check_history_capacity(&action, command)?;
        let maximum_events = action.maximum_events()?;
        if maximum_events > self.limits.max_report_events() {
            return Err(CallAuctionEngineError::CapacityExhausted(
                CallAuctionEngineCapacity::ReportEvents,
            ));
        }
        let maximum_events_u64 =
            u64::try_from(maximum_events).map_err(|_| CallAuctionEngineError::SequenceExhausted)?;
        self.next_command_sequence
            .checked_add(1)
            .ok_or(CallAuctionEngineError::SequenceExhausted)?;
        self.next_event_sequence
            .checked_add(maximum_events_u64)
            .ok_or(CallAuctionEngineError::SequenceExhausted)?;
        let events = CallAuctionEventTraceBuilder::try_with_capacity(maximum_events)?;
        Ok(CallAuctionCommandPreparation::Ready(
            PreparedCallAuctionCommand {
                command,
                engine_instance_id: self.instance_id,
                expected_retained_commands: self.reports.len(),
                action,
                events,
            },
        ))
    }

    /// Commits one same-instance, same-generation preparation.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] before mutation for a foreign/stale
    /// token or a private invariant contradiction.
    pub fn commit(
        &mut self,
        prepared: PreparedCallAuctionCommand,
    ) -> Result<CallAuctionExecutionReport, CallAuctionEngineError> {
        self.commit_with_gate(prepared, None)
    }

    pub(crate) fn commit_with_gate(
        &mut self,
        prepared: PreparedCallAuctionCommand,
        external_rejection: Option<CallAuctionRejectReason>,
    ) -> Result<CallAuctionExecutionReport, CallAuctionEngineError> {
        let PreparedCallAuctionCommand {
            command,
            engine_instance_id,
            expected_retained_commands,
            mut action,
            mut events,
        } = prepared;
        if engine_instance_id != self.instance_id {
            return Err(CallAuctionEngineError::ForeignPreparation);
        }
        let command_id = command.command_id();
        if let Some(cached) = self.reports.get(&command_id) {
            if cached.command != command {
                return Err(CallAuctionEngineError::CommandIdCollision(command_id));
            }
            let mut report = cached.report.clone();
            report.replayed = true;
            return Ok(report);
        }
        if expected_retained_commands != self.reports.len() {
            return Err(CallAuctionEngineError::StalePreparation);
        }
        if self.reports.len() >= self.limits.max_retained_commands() {
            return Err(CallAuctionEngineError::InternalInvariantViolation);
        }
        if let Some(reason) = external_rejection {
            if !matches!(command, CallAuctionCommand::Submit(_))
                || !is_external_risk_rejection(reason)
            {
                return Err(CallAuctionEngineError::InternalInvariantViolation);
            }
            if !matches!(action, PreparedCallAuctionAction::Rejected(_)) {
                action = PreparedCallAuctionAction::Rejected(reason);
            }
        }
        let next_command_sequence = self
            .next_command_sequence
            .checked_add(1)
            .ok_or(CallAuctionEngineError::InternalInvariantViolation)?;
        let outcome = self.apply_prepared_action(command, action, &mut events)?;
        let command_sequence = self.next_command_sequence;
        self.next_command_sequence = next_command_sequence;
        let report = CallAuctionExecutionReport {
            command_id,
            command_sequence,
            outcome,
            events: events.finish(),
            replayed: false,
        };
        let replaced = self.reports.insert(
            command_id,
            CachedCallAuctionReport {
                command,
                report: report.clone(),
            },
        );
        debug_assert!(replaced.is_none());
        Ok(report)
    }

    fn apply_prepared_action(
        &mut self,
        command: CallAuctionCommand,
        action: PreparedCallAuctionAction,
        events: &mut CallAuctionEventTraceBuilder,
    ) -> Result<CallAuctionCommandOutcome, CallAuctionEngineError> {
        let outcome = match action {
            PreparedCallAuctionAction::Rejected(reason) => {
                self.push_event(
                    command,
                    CallAuctionEventKind::CommandRejected(reason),
                    events,
                );
                CallAuctionCommandOutcome::Rejected(reason)
            }
            PreparedCallAuctionAction::PhaseTransition {
                auction_id,
                previous,
                current,
                revision,
                active_after,
                last_after,
            } => {
                self.phase = current;
                self.phase_revision = revision;
                self.active_auction_id = active_after;
                self.last_auction_id = last_after;
                self.push_event(
                    command,
                    CallAuctionEventKind::PhaseChanged {
                        auction_id,
                        previous,
                        current,
                        revision,
                    },
                    events,
                );
                CallAuctionCommandOutcome::Accepted
            }
            PreparedCallAuctionAction::Submit(order) => {
                let snapshot = self
                    .book
                    .admit(order)
                    .map_err(|_| CallAuctionEngineError::InternalInvariantViolation)?;
                self.push_event(
                    command,
                    CallAuctionEventKind::OrderAccepted(snapshot),
                    events,
                );
                CallAuctionCommandOutcome::Accepted
            }
            PreparedCallAuctionAction::Cancel {
                account_id,
                order_id,
            } => {
                let snapshot = self
                    .book
                    .cancel(account_id, order_id)
                    .map_err(|_| CallAuctionEngineError::InternalInvariantViolation)?;
                self.push_event(
                    command,
                    CallAuctionEventKind::OrderCancelled {
                        order: snapshot,
                        reason: CallAuctionCancellationReason::UserRequested,
                    },
                    events,
                );
                CallAuctionCommandOutcome::Accepted
            }
            PreparedCallAuctionAction::Uncross {
                auction_id,
                phase_revision,
                prepared,
            } => {
                self.apply_prepared_uncross(command, auction_id, phase_revision, prepared, events)?
            }
        };
        Ok(outcome)
    }

    fn apply_prepared_uncross(
        &mut self,
        command: CallAuctionCommand,
        auction_id: AuctionId,
        phase_revision: u64,
        prepared: PreparedCallAuctionUncross,
        events: &mut CallAuctionEventTraceBuilder,
    ) -> Result<CallAuctionCommandOutcome, CallAuctionEngineError> {
        let trade_count = u64::try_from(prepared.trades().len())
            .map_err(|_| CallAuctionEngineError::InternalInvariantViolation)?;
        let cancellation_count = u64::try_from(prepared.cancellations().len())
            .map_err(|_| CallAuctionEngineError::InternalInvariantViolation)?;
        let uncross = self
            .book
            .commit_uncross(prepared)
            .map_err(CallAuctionEngineError::UncrossCommit)?;
        for trade in uncross.trades().iter().copied() {
            self.push_event(command, CallAuctionEventKind::Trade(trade), events);
        }
        for cancellation in uncross.cancellations().iter().copied() {
            self.push_event(
                command,
                CallAuctionEventKind::RemainderCancelled(cancellation),
                events,
            );
        }
        self.phase = CallAuctionPhase::Closed;
        self.phase_revision = phase_revision;
        self.active_auction_id = None;
        self.push_event(
            command,
            CallAuctionEventKind::UncrossCompleted {
                auction_id,
                clearing: uncross.allocation_plan().clearing(),
                policy: uncross.policy(),
                trade_count,
                cancellation_count,
                book_revision: uncross.state_revision(),
                phase_revision,
            },
            events,
        );
        Ok(CallAuctionCommandOutcome::Accepted)
    }

    fn prepare_action(
        &mut self,
        command: CallAuctionCommand,
    ) -> Result<PreparedCallAuctionAction, CallAuctionEngineError> {
        match command {
            CallAuctionCommand::PhaseControl(control) => self.prepare_phase_control(control),
            CallAuctionCommand::Submit(submit) => {
                if let Some(reason) = self.route_rejection(
                    submit.order.instrument_id(),
                    submit.order.instrument_version(),
                ) {
                    return Ok(PreparedCallAuctionAction::Rejected(reason));
                }
                if submit.expected_phase_revision != self.phase_revision {
                    return Ok(PreparedCallAuctionAction::Rejected(
                        CallAuctionRejectReason::PhaseRevisionMismatch {
                            observed: submit.expected_phase_revision,
                            current: self.phase_revision,
                        },
                    ));
                }
                if self.phase != CallAuctionPhase::Collecting {
                    return Ok(PreparedCallAuctionAction::Rejected(
                        CallAuctionRejectReason::ActionNotAllowed {
                            action: CallAuctionAction::Submit,
                            phase: self.phase,
                        },
                    ));
                }
                if self.active_auction_id != Some(submit.auction_id) {
                    return Ok(PreparedCallAuctionAction::Rejected(
                        CallAuctionRejectReason::AuctionIdMismatch {
                            observed: submit.auction_id,
                            current: self.active_auction_id,
                        },
                    ));
                }
                match self.book.preflight_admission(submit.order) {
                    Ok(()) => Ok(PreparedCallAuctionAction::Submit(submit.order)),
                    Err(CallAuctionAdmissionError::Instrument(error)) => {
                        Ok(PreparedCallAuctionAction::Rejected(
                            CallAuctionRejectReason::Instrument(error),
                        ))
                    }
                    Err(CallAuctionAdmissionError::DuplicateOrderId) => {
                        Ok(PreparedCallAuctionAction::Rejected(
                            CallAuctionRejectReason::DuplicateOrder,
                        ))
                    }
                    Err(error) => Err(CallAuctionEngineError::BookAdmission(error)),
                }
            }
            CallAuctionCommand::Cancel(cancel) => {
                if let Some(reason) =
                    self.route_rejection(cancel.instrument_id, cancel.instrument_version)
                {
                    return Ok(PreparedCallAuctionAction::Rejected(reason));
                }
                match self
                    .book
                    .preflight_cancel(cancel.account_id, cancel.order_id)
                {
                    Ok(_) => Ok(PreparedCallAuctionAction::Cancel {
                        account_id: cancel.account_id,
                        order_id: cancel.order_id,
                    }),
                    Err(CallAuctionCancelError::UnknownOrder) => Ok(
                        PreparedCallAuctionAction::Rejected(CallAuctionRejectReason::UnknownOrder),
                    ),
                    Err(CallAuctionCancelError::AccountMismatch) => Ok(
                        PreparedCallAuctionAction::Rejected(CallAuctionRejectReason::NotOrderOwner),
                    ),
                    Err(error) => Err(CallAuctionEngineError::BookCancellation(error)),
                }
            }
            CallAuctionCommand::Uncross(uncross) => self.prepare_uncross_command(uncross),
        }
    }

    fn prepare_phase_control(
        &self,
        control: CallAuctionPhaseControl,
    ) -> Result<PreparedCallAuctionAction, CallAuctionEngineError> {
        if let Some(reason) =
            self.route_rejection(control.instrument_id, control.instrument_version)
        {
            return Ok(PreparedCallAuctionAction::Rejected(reason));
        }
        if control.expected_phase_revision != self.phase_revision {
            return Ok(PreparedCallAuctionAction::Rejected(
                CallAuctionRejectReason::PhaseRevisionMismatch {
                    observed: control.expected_phase_revision,
                    current: self.phase_revision,
                },
            ));
        }
        let (active_after, last_after) = match (self.phase, control.target_phase) {
            (CallAuctionPhase::Closed, CallAuctionPhase::Collecting) => {
                let next_raw = self
                    .last_auction_id
                    .map_or(Some(1), |auction_id| auction_id.get().checked_add(1))
                    .ok_or(CallAuctionEngineError::AuctionIdExhausted)?;
                let next = AuctionId::new(next_raw)
                    .map_err(|_| CallAuctionEngineError::InternalInvariantViolation)?;
                if control.auction_id != next {
                    return Ok(PreparedCallAuctionAction::Rejected(
                        CallAuctionRejectReason::AuctionIdNotNext {
                            expected: next,
                            observed: control.auction_id,
                        },
                    ));
                }
                (Some(next), Some(next))
            }
            (CallAuctionPhase::Collecting, CallAuctionPhase::Frozen)
            | (CallAuctionPhase::Frozen, CallAuctionPhase::Collecting) => {
                if self.active_auction_id != Some(control.auction_id) {
                    return Ok(PreparedCallAuctionAction::Rejected(
                        CallAuctionRejectReason::AuctionIdMismatch {
                            observed: control.auction_id,
                            current: self.active_auction_id,
                        },
                    ));
                }
                (self.active_auction_id, self.last_auction_id)
            }
            (CallAuctionPhase::Collecting | CallAuctionPhase::Frozen, CallAuctionPhase::Closed) => {
                if self.active_auction_id != Some(control.auction_id) {
                    return Ok(PreparedCallAuctionAction::Rejected(
                        CallAuctionRejectReason::AuctionIdMismatch {
                            observed: control.auction_id,
                            current: self.active_auction_id,
                        },
                    ));
                }
                (None, self.last_auction_id)
            }
            (from, to) => {
                return Ok(PreparedCallAuctionAction::Rejected(
                    CallAuctionRejectReason::InvalidPhaseTransition { from, to },
                ));
            }
        };
        let revision = self
            .phase_revision
            .checked_add(1)
            .ok_or(CallAuctionEngineError::PhaseRevisionExhausted)?;
        Ok(PreparedCallAuctionAction::PhaseTransition {
            auction_id: control.auction_id,
            previous: self.phase,
            current: control.target_phase,
            revision,
            active_after,
            last_after,
        })
    }

    fn prepare_uncross_command(
        &mut self,
        uncross: CallAuctionUncrossCommand,
    ) -> Result<PreparedCallAuctionAction, CallAuctionEngineError> {
        if let Some(reason) =
            self.route_rejection(uncross.instrument_id, uncross.instrument_version)
        {
            return Ok(PreparedCallAuctionAction::Rejected(reason));
        }
        if uncross.expected_phase_revision != self.phase_revision {
            return Ok(PreparedCallAuctionAction::Rejected(
                CallAuctionRejectReason::PhaseRevisionMismatch {
                    observed: uncross.expected_phase_revision,
                    current: self.phase_revision,
                },
            ));
        }
        if self.phase != CallAuctionPhase::Frozen {
            return Ok(PreparedCallAuctionAction::Rejected(
                CallAuctionRejectReason::ActionNotAllowed {
                    action: CallAuctionAction::Uncross,
                    phase: self.phase,
                },
            ));
        }
        if self.active_auction_id != Some(uncross.auction_id) {
            return Ok(PreparedCallAuctionAction::Rejected(
                CallAuctionRejectReason::AuctionIdMismatch {
                    observed: uncross.auction_id,
                    current: self.active_auction_id,
                },
            ));
        }
        let indicative = self
            .book
            .indicative_clearing(
                uncross.price_band,
                uncross.reference_price,
                uncross.price_policy,
            )
            .map_err(CallAuctionEngineError::Discovery)?;
        let Some(indicative) = indicative else {
            return Ok(PreparedCallAuctionAction::Rejected(
                CallAuctionRejectReason::NoExecutableInterest,
            ));
        };
        let phase_revision = self
            .phase_revision
            .checked_add(1)
            .ok_or(CallAuctionEngineError::PhaseRevisionExhausted)?;
        let prepared = self
            .book
            .prepare_uncross(indicative, uncross.uncross_policy)
            .map_err(CallAuctionEngineError::UncrossPreparation)?;
        Ok(PreparedCallAuctionAction::Uncross {
            auction_id: uncross.auction_id,
            phase_revision,
            prepared,
        })
    }

    fn route_rejection(
        &self,
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
    ) -> Option<CallAuctionRejectReason> {
        if instrument_id != self.book.definition().instrument_id() {
            Some(CallAuctionRejectReason::WrongInstrument)
        } else if instrument_version != self.book.definition().version() {
            Some(CallAuctionRejectReason::WrongInstrumentVersion)
        } else {
            None
        }
    }

    fn check_history_capacity(
        &self,
        action: &PreparedCallAuctionAction,
        command: CallAuctionCommand,
    ) -> Result<(), CallAuctionEngineError> {
        if self.reports.len() >= self.limits.max_retained_commands() {
            return Err(CallAuctionEngineError::CapacityExhausted(
                CallAuctionEngineCapacity::CommandHistory,
            ));
        }
        if self.reports.len() >= self.limits.ordinary_command_capacity()
            && !action.is_terminal_lane_eligible(command)
        {
            return Err(CallAuctionEngineError::CapacityExhausted(
                CallAuctionEngineCapacity::AdmissionCommandHistory,
            ));
        }
        Ok(())
    }

    fn push_event(
        &mut self,
        command: CallAuctionCommand,
        kind: CallAuctionEventKind,
        events: &mut CallAuctionEventTraceBuilder,
    ) {
        let sequence = self.next_event_sequence;
        self.next_event_sequence = self
            .next_event_sequence
            .checked_add(1)
            .expect("successful event preflight must preserve sequence space");
        events.push(CallAuctionEvent {
            sequence,
            command_id: command.command_id(),
            occurred_at: command.received_at(),
            kind,
        });
    }

    /// Performs an allocation-using independent audit of book, phase, cache,
    /// command sequences, and event continuity.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineInvariantViolation`] at the first contradiction.
    pub fn validate(&self) -> Result<(), CallAuctionEngineInvariantViolation> {
        self.book
            .validate()
            .map_err(CallAuctionEngineInvariantViolation::Book)?;
        if self.reports.len() > self.limits.max_retained_commands()
            || self.reports.capacity() < self.limits.max_retained_commands()
        {
            return Err(CallAuctionEngineInvariantViolation::new(
                "auction report cache contradicts configured capacity",
            ));
        }
        if (self.phase == CallAuctionPhase::Closed) != self.active_auction_id.is_none() {
            return Err(CallAuctionEngineInvariantViolation::new(
                "auction phase and active cycle identity disagree",
            ));
        }
        if self.active_auction_id.is_some() && self.active_auction_id != self.last_auction_id {
            return Err(CallAuctionEngineInvariantViolation::new(
                "active auction identity is not the last started cycle",
            ));
        }
        self.validate_report_history()
    }

    fn validate_report_history(&self) -> Result<(), CallAuctionEngineInvariantViolation> {
        let retained_u64 = u64::try_from(self.reports.len()).map_err(|_| {
            CallAuctionEngineInvariantViolation::new("auction command count exceeds u64")
        })?;
        let expected_next_command = retained_u64.checked_add(1).ok_or_else(|| {
            CallAuctionEngineInvariantViolation::new("auction command count exhausts sequence")
        })?;
        if self.next_command_sequence != expected_next_command {
            return Err(CallAuctionEngineInvariantViolation::new(
                "auction command sequence disagrees with retained history",
            ));
        }
        let mut reports = Vec::new();
        reports.try_reserve_exact(self.reports.len()).map_err(|_| {
            CallAuctionEngineInvariantViolation::new("auction audit allocation failed")
        })?;
        for (key, cached) in &self.reports {
            if *key != cached.command.command_id() || *key != cached.report.command_id {
                return Err(CallAuctionEngineInvariantViolation::new(
                    "auction cache key, command, and report identity disagree",
                ));
            }
            if !cached_report_grammar_is_valid(cached) {
                return Err(CallAuctionEngineInvariantViolation::new(
                    "auction cached report violates command/event grammar",
                ));
            }
            reports.push(&cached.report);
        }
        reports.sort_unstable_by_key(|report| report.command_sequence);
        let mut expected_command = 1_u64;
        let mut expected_event = 1_u64;
        let mut phase_audit = CallAuctionPhaseAudit::default();
        for report in reports {
            if report.command_sequence != expected_command
                || report.command_id
                    != report
                        .events
                        .first()
                        .map_or(report.command_id, |event| event.command_id)
                || report.events.is_empty()
            {
                return Err(CallAuctionEngineInvariantViolation::new(
                    "auction cached report command metadata is inconsistent",
                ));
            }
            let cached = self.reports.get(&report.command_id).ok_or_else(|| {
                CallAuctionEngineInvariantViolation::new(
                    "auction report disappeared from its identity index",
                )
            })?;
            let received_at = cached.command.received_at();
            if !phase_audit.observe(
                cached,
                self.book.definition().instrument_id(),
                self.book.definition().version(),
            ) {
                return Err(CallAuctionEngineInvariantViolation::new(
                    "auction history contradicts controller phase semantics",
                ));
            }
            for event in report.events.iter().copied() {
                if event.sequence != expected_event
                    || event.command_id != report.command_id
                    || event.occurred_at != received_at
                {
                    return Err(CallAuctionEngineInvariantViolation::new(
                        "auction event trace is noncontiguous or misbound",
                    ));
                }
                expected_event = expected_event.checked_add(1).ok_or_else(|| {
                    CallAuctionEngineInvariantViolation::new("auction event audit overflow")
                })?;
            }
            expected_command = expected_command.checked_add(1).ok_or_else(|| {
                CallAuctionEngineInvariantViolation::new("auction command audit overflow")
            })?;
        }
        if expected_event != self.next_event_sequence {
            return Err(CallAuctionEngineInvariantViolation::new(
                "auction next event sequence disagrees with retained traces",
            ));
        }
        if phase_audit.phase != self.phase
            || phase_audit.revision != self.phase_revision
            || phase_audit.active_auction_id != self.active_auction_id
            || phase_audit.last_auction_id != self.last_auction_id
        {
            return Err(CallAuctionEngineInvariantViolation::new(
                "auction replayed phase state disagrees with live state",
            ));
        }
        Ok(())
    }
}

fn cached_report_grammar_is_valid(cached: &CachedCallAuctionReport) -> bool {
    let report = &cached.report;
    if report.replayed || report.events.is_empty() {
        return false;
    }
    match report.outcome {
        CallAuctionCommandOutcome::Rejected(reason) => {
            report.events.len() == 1
                && report.events[0].kind == CallAuctionEventKind::CommandRejected(reason)
        }
        CallAuctionCommandOutcome::Accepted => match cached.command {
            CallAuctionCommand::PhaseControl(control) => {
                let Some(expected_revision) = control.expected_phase_revision.checked_add(1) else {
                    return false;
                };
                matches!(
                    report.events.as_slice(),
                    [CallAuctionEvent {
                        kind: CallAuctionEventKind::PhaseChanged {
                            auction_id,
                            current,
                            revision,
                            ..
                        },
                        ..
                    }] if *auction_id == control.auction_id
                        && *current == control.target_phase
                        && *revision == expected_revision
                )
            }
            CallAuctionCommand::Submit(submit) => {
                matches!(
                    report.events.as_slice(),
                    [CallAuctionEvent {
                        kind: CallAuctionEventKind::OrderAccepted(snapshot),
                        ..
                    }] if snapshot.order_id == submit.order.order_id()
                        && snapshot.account_id == submit.order.account_id()
                        && snapshot.side == submit.order.side()
                        && snapshot.constraint == submit.order.constraint()
                        && snapshot.quantity == submit.order.quantity()
                )
            }
            CallAuctionCommand::Cancel(cancel) => {
                matches!(
                    report.events.as_slice(),
                    [CallAuctionEvent {
                        kind: CallAuctionEventKind::OrderCancelled {
                            order,
                            reason: CallAuctionCancellationReason::UserRequested,
                        },
                        ..
                    }] if order.order_id == cancel.order_id && order.account_id == cancel.account_id
                )
            }
            CallAuctionCommand::Uncross(uncross) => {
                cached_uncross_grammar_is_valid(report, uncross)
            }
        },
    }
}

const fn is_external_risk_rejection(reason: CallAuctionRejectReason) -> bool {
    matches!(
        reason,
        CallAuctionRejectReason::RiskProfileMissing
            | CallAuctionRejectReason::RiskAccountBlocked
            | CallAuctionRejectReason::RiskReduceOnly
            | CallAuctionRejectReason::RiskOrderQuantityLimit
            | CallAuctionRejectReason::RiskOrderNotionalLimit
            | CallAuctionRejectReason::RiskOpenOrderCountLimit
            | CallAuctionRejectReason::RiskOpenQuantityLimit
            | CallAuctionRejectReason::RiskOpenNotionalLimit
            | CallAuctionRejectReason::RiskPositionLimit
            | CallAuctionRejectReason::RiskArithmeticOverflow
    )
}

fn cached_uncross_grammar_is_valid(
    report: &CallAuctionExecutionReport,
    uncross: CallAuctionUncrossCommand,
) -> bool {
    let Some(expected_phase_revision) = uncross.expected_phase_revision.checked_add(1) else {
        return false;
    };
    let Some(CallAuctionEvent {
        kind:
            CallAuctionEventKind::UncrossCompleted {
                auction_id,
                clearing,
                policy,
                trade_count,
                cancellation_count,
                phase_revision,
                ..
            },
        ..
    }) = report.events.last()
    else {
        return false;
    };
    let body = &report.events[..report.events.len() - 1];
    let Ok(observed_trades) = usize::try_from(*trade_count) else {
        return false;
    };
    let Ok(observed_cancellations) = usize::try_from(*cancellation_count) else {
        return false;
    };
    let Some(observed_body) = observed_trades.checked_add(observed_cancellations) else {
        return false;
    };
    if body.len() != observed_body {
        return false;
    }
    let (trade_events, cancellation_events) = body.split_at(observed_trades);
    let mut trade_quantity = 0_u128;
    let mut previous_trade_id: Option<u64> = None;
    for event in trade_events {
        let CallAuctionEventKind::Trade(trade) = event.kind else {
            return false;
        };
        if trade.price() != clearing.price()
            || previous_trade_id
                .is_some_and(|previous| previous.checked_add(1) != Some(trade.trade_id().get()))
        {
            return false;
        }
        previous_trade_id = Some(trade.trade_id().get());
        let Some(next_quantity) = trade_quantity.checked_add(u128::from(trade.quantity().lots()))
        else {
            return false;
        };
        trade_quantity = next_quantity;
    }
    cancellation_events
        .iter()
        .all(|event| matches!(event.kind, CallAuctionEventKind::RemainderCancelled(_)))
        && trade_quantity == clearing.executable_quantity()
        && *auction_id == uncross.auction_id
        && *policy == uncross.uncross_policy
        && *phase_revision == expected_phase_revision
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CallAuctionControllerPreflight {
    Applicable,
    Rejected(CallAuctionRejectReason),
    OperationalFailure,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CallAuctionPhaseAudit {
    phase: CallAuctionPhase,
    revision: u64,
    active_auction_id: Option<AuctionId>,
    last_auction_id: Option<AuctionId>,
}

impl CallAuctionPhaseAudit {
    fn observe(
        &mut self,
        cached: &CachedCallAuctionReport,
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
    ) -> bool {
        let preflight = self.preflight(cached.command, instrument_id, instrument_version);
        match cached.report.outcome {
            CallAuctionCommandOutcome::Rejected(reason) => match preflight {
                CallAuctionControllerPreflight::Rejected(expected) => reason == expected,
                CallAuctionControllerPreflight::OperationalFailure => false,
                CallAuctionControllerPreflight::Applicable => {
                    matches!(
                        (cached.command, reason),
                        (
                            CallAuctionCommand::Submit(_),
                            CallAuctionRejectReason::Instrument(_)
                                | CallAuctionRejectReason::DuplicateOrder
                        ) | (
                            CallAuctionCommand::Cancel(_),
                            CallAuctionRejectReason::UnknownOrder
                                | CallAuctionRejectReason::NotOrderOwner
                        ) | (
                            CallAuctionCommand::Uncross(_),
                            CallAuctionRejectReason::NoExecutableInterest
                        )
                    ) || matches!(cached.command, CallAuctionCommand::Submit(_))
                        && is_external_risk_rejection(reason)
                }
            },
            CallAuctionCommandOutcome::Accepted => {
                if preflight != CallAuctionControllerPreflight::Applicable {
                    return false;
                }
                self.apply_accepted(cached)
            }
        }
    }

    fn preflight(
        self,
        command: CallAuctionCommand,
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
    ) -> CallAuctionControllerPreflight {
        let (observed_instrument, observed_version) = match command {
            CallAuctionCommand::PhaseControl(control) => {
                (control.instrument_id, control.instrument_version)
            }
            CallAuctionCommand::Submit(submit) => (
                submit.order.instrument_id(),
                submit.order.instrument_version(),
            ),
            CallAuctionCommand::Cancel(cancel) => (cancel.instrument_id, cancel.instrument_version),
            CallAuctionCommand::Uncross(uncross) => {
                (uncross.instrument_id, uncross.instrument_version)
            }
        };
        if observed_instrument != instrument_id {
            return CallAuctionControllerPreflight::Rejected(
                CallAuctionRejectReason::WrongInstrument,
            );
        }
        if observed_version != instrument_version {
            return CallAuctionControllerPreflight::Rejected(
                CallAuctionRejectReason::WrongInstrumentVersion,
            );
        }
        match command {
            CallAuctionCommand::PhaseControl(control) => self.preflight_control(control),
            CallAuctionCommand::Submit(submit) => {
                if submit.expected_phase_revision != self.revision {
                    CallAuctionControllerPreflight::Rejected(
                        CallAuctionRejectReason::PhaseRevisionMismatch {
                            observed: submit.expected_phase_revision,
                            current: self.revision,
                        },
                    )
                } else if self.phase != CallAuctionPhase::Collecting {
                    CallAuctionControllerPreflight::Rejected(
                        CallAuctionRejectReason::ActionNotAllowed {
                            action: CallAuctionAction::Submit,
                            phase: self.phase,
                        },
                    )
                } else if self.active_auction_id != Some(submit.auction_id) {
                    CallAuctionControllerPreflight::Rejected(
                        CallAuctionRejectReason::AuctionIdMismatch {
                            observed: submit.auction_id,
                            current: self.active_auction_id,
                        },
                    )
                } else {
                    CallAuctionControllerPreflight::Applicable
                }
            }
            CallAuctionCommand::Uncross(uncross) => {
                if uncross.expected_phase_revision != self.revision {
                    CallAuctionControllerPreflight::Rejected(
                        CallAuctionRejectReason::PhaseRevisionMismatch {
                            observed: uncross.expected_phase_revision,
                            current: self.revision,
                        },
                    )
                } else if self.phase != CallAuctionPhase::Frozen {
                    CallAuctionControllerPreflight::Rejected(
                        CallAuctionRejectReason::ActionNotAllowed {
                            action: CallAuctionAction::Uncross,
                            phase: self.phase,
                        },
                    )
                } else if self.active_auction_id != Some(uncross.auction_id) {
                    CallAuctionControllerPreflight::Rejected(
                        CallAuctionRejectReason::AuctionIdMismatch {
                            observed: uncross.auction_id,
                            current: self.active_auction_id,
                        },
                    )
                } else {
                    CallAuctionControllerPreflight::Applicable
                }
            }
            CallAuctionCommand::Cancel(_) => CallAuctionControllerPreflight::Applicable,
        }
    }

    fn preflight_control(self, control: CallAuctionPhaseControl) -> CallAuctionControllerPreflight {
        if control.expected_phase_revision != self.revision {
            return CallAuctionControllerPreflight::Rejected(
                CallAuctionRejectReason::PhaseRevisionMismatch {
                    observed: control.expected_phase_revision,
                    current: self.revision,
                },
            );
        }
        let identity_matches = match (self.phase, control.target_phase) {
            (CallAuctionPhase::Closed, CallAuctionPhase::Collecting) => {
                let Some(next) = self
                    .last_auction_id
                    .map_or(Some(1), |auction_id| auction_id.get().checked_add(1))
                else {
                    return CallAuctionControllerPreflight::OperationalFailure;
                };
                if control.auction_id.get() != next {
                    return CallAuctionControllerPreflight::Rejected(
                        CallAuctionRejectReason::AuctionIdNotNext {
                            expected: AuctionId::new(next)
                                .expect("successor of a non-zero auction ID remains non-zero"),
                            observed: control.auction_id,
                        },
                    );
                }
                true
            }
            (CallAuctionPhase::Collecting, CallAuctionPhase::Frozen)
            | (CallAuctionPhase::Frozen, CallAuctionPhase::Collecting)
            | (CallAuctionPhase::Collecting | CallAuctionPhase::Frozen, CallAuctionPhase::Closed) => {
                self.active_auction_id == Some(control.auction_id)
            }
            (from, to) => {
                return CallAuctionControllerPreflight::Rejected(
                    CallAuctionRejectReason::InvalidPhaseTransition { from, to },
                );
            }
        };
        if !identity_matches {
            return CallAuctionControllerPreflight::Rejected(
                CallAuctionRejectReason::AuctionIdMismatch {
                    observed: control.auction_id,
                    current: self.active_auction_id,
                },
            );
        }
        if self.revision == u64::MAX {
            CallAuctionControllerPreflight::OperationalFailure
        } else {
            CallAuctionControllerPreflight::Applicable
        }
    }

    fn apply_accepted(&mut self, cached: &CachedCallAuctionReport) -> bool {
        match cached.command {
            CallAuctionCommand::PhaseControl(control) => {
                let [
                    CallAuctionEvent {
                        kind:
                            CallAuctionEventKind::PhaseChanged {
                                auction_id,
                                previous,
                                current,
                                revision,
                            },
                        ..
                    },
                ] = cached.report.events.as_slice()
                else {
                    return false;
                };
                let Some(next_revision) = self.revision.checked_add(1) else {
                    return false;
                };
                if *auction_id != control.auction_id
                    || *previous != self.phase
                    || *current != control.target_phase
                    || *revision != next_revision
                {
                    return false;
                }
                if self.phase == CallAuctionPhase::Closed {
                    self.active_auction_id = Some(control.auction_id);
                    self.last_auction_id = Some(control.auction_id);
                } else if control.target_phase == CallAuctionPhase::Closed {
                    self.active_auction_id = None;
                }
                self.phase = control.target_phase;
                self.revision = next_revision;
                true
            }
            CallAuctionCommand::Uncross(uncross) => {
                let Some(CallAuctionEvent {
                    kind:
                        CallAuctionEventKind::UncrossCompleted {
                            auction_id,
                            phase_revision,
                            ..
                        },
                    ..
                }) = cached.report.events.last()
                else {
                    return false;
                };
                let Some(next_revision) = self.revision.checked_add(1) else {
                    return false;
                };
                if *auction_id != uncross.auction_id || *phase_revision != next_revision {
                    return false;
                }
                self.phase = CallAuctionPhase::Closed;
                self.revision = next_revision;
                self.active_auction_id = None;
                true
            }
            CallAuctionCommand::Submit(_) | CallAuctionCommand::Cancel(_) => true,
        }
    }
}

/// Structural contradiction detected by an offline engine audit.
#[derive(Debug)]
pub enum CallAuctionEngineInvariantViolation {
    /// Collection-book audit failed.
    Book(CallAuctionInvariantViolation),
    /// Engine-owned state contradicted its invariants.
    Engine(String),
}

impl CallAuctionEngineInvariantViolation {
    fn new(detail: impl Into<String>) -> Self {
        Self::Engine(detail.into())
    }

    /// Returns a stable diagnostic description.
    #[must_use]
    pub fn detail(&self) -> &str {
        match self {
            Self::Book(error) => error.detail(),
            Self::Engine(detail) => detail,
        }
    }
}

impl fmt::Display for CallAuctionEngineInvariantViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail().fmt(formatter)
    }
}

impl std::error::Error for CallAuctionEngineInvariantViolation {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Book(error) => Some(error),
            Self::Engine(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        CallAuctionCommand, CallAuctionCommandOutcome, CallAuctionEngine, CallAuctionEventKind,
        CallAuctionPhase, CallAuctionPhaseControl, CallAuctionRejectReason,
        CallAuctionUncrossCommand,
    };
    use crate::auction::AuctionPricePolicy;
    use crate::auction_book::{
        CallAuctionRemainderPolicy, CallAuctionSelfTradePolicy, CallAuctionUncrossPolicy,
    };
    use crate::instrument::{
        InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
        QuantityRules, ReserveOrderRules, TradingState,
    };
    use crate::{
        AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion, Price, TimestampNs,
    };

    fn definition() -> InstrumentDefinition {
        InstrumentDefinition::new(InstrumentSpec {
            instrument_id: InstrumentId::new(991).unwrap(),
            version: InstrumentVersion::new(1).unwrap(),
            effective_from: TimestampNs::from_unix_nanos(0),
            symbol: InstrumentSymbol::new("EDGE").unwrap(),
            kind: InstrumentKind::Spot,
            base_asset_id: AssetId::new(1).unwrap(),
            quote_asset_id: AssetId::new(2).unwrap(),
            price: PriceRules::new(0, 1, Price::from_raw(0), Price::from_raw(100)).unwrap(),
            quantity: QuantityRules::new(1, 1, 100).unwrap(),
            reserve: ReserveOrderRules::disabled(),
            base_units_per_lot: 1,
            quote_units_per_price_unit: 1,
            trading_state: TradingState::Halted,
        })
        .unwrap()
    }

    #[test]
    fn invalid_transition_precedes_unused_exhausted_phase_revision() {
        let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
        engine.phase_revision = u64::MAX;
        let report = engine
            .submit(CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
                command_id: CommandId::new(1).unwrap(),
                instrument_id: engine.book.definition().instrument_id(),
                instrument_version: engine.book.definition().version(),
                auction_id: AuctionId::new(1).unwrap(),
                expected_phase_revision: u64::MAX,
                target_phase: CallAuctionPhase::Closed,
                received_at: TimestampNs::from_unix_nanos(1),
            }))
            .unwrap();
        assert_eq!(
            report.outcome,
            CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::InvalidPhaseTransition {
                from: CallAuctionPhase::Closed,
                to: CallAuctionPhase::Closed,
            })
        );
    }

    #[test]
    fn no_execution_precedes_unused_exhausted_phase_revision() {
        let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
        let auction_id = AuctionId::new(1).unwrap();
        engine.phase = CallAuctionPhase::Frozen;
        engine.phase_revision = u64::MAX;
        engine.active_auction_id = Some(auction_id);
        engine.last_auction_id = Some(auction_id);
        let report = engine
            .submit(CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
                command_id: CommandId::new(1).unwrap(),
                instrument_id: engine.book.definition().instrument_id(),
                instrument_version: engine.book.definition().version(),
                auction_id,
                expected_phase_revision: u64::MAX,
                price_band: engine.book.instrument_price_band(),
                reference_price: Price::from_raw(50),
                price_policy: AuctionPricePolicy::REFERENCE_THEN_LOWER,
                uncross_policy: CallAuctionUncrossPolicy::new(
                    CallAuctionRemainderPolicy::CancelAll,
                    CallAuctionSelfTradePolicy::Permit,
                ),
                received_at: TimestampNs::from_unix_nanos(1),
            }))
            .unwrap();
        assert_eq!(
            report.outcome,
            CallAuctionCommandOutcome::Rejected(CallAuctionRejectReason::NoExecutableInterest)
        );
    }

    #[test]
    fn offline_audit_rejects_corrupt_cached_phase_grammar() {
        let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
        let command_id = CommandId::new(1).unwrap();
        engine
            .submit(CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
                command_id,
                instrument_id: engine.book.definition().instrument_id(),
                instrument_version: engine.book.definition().version(),
                auction_id: AuctionId::new(1).unwrap(),
                expected_phase_revision: 0,
                target_phase: CallAuctionPhase::Collecting,
                received_at: TimestampNs::from_unix_nanos(1),
            }))
            .unwrap();
        engine.validate().unwrap();

        let cached = engine.reports.get_mut(&command_id).unwrap();
        let events = Arc::make_mut(&mut cached.report.events.0);
        let CallAuctionEventKind::PhaseChanged { revision, .. } = &mut events[0].kind else {
            unreachable!();
        };
        *revision = 2;
        assert_eq!(
            engine.validate().unwrap_err().detail(),
            "auction cached report violates command/event grammar"
        );
    }
}
