//! Deterministic, anonymized call-auction depth, phase, and execution publication.
//!
//! Each private auction-engine event maps to one public update at the identical
//! event sequence. Absolute market/limit aggregates permit deterministic gap
//! recovery without exposing order or account identity. Full-depth snapshots
//! establish an atomic replica repair boundary.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use crate::auction::{AuctionClearing, AuctionOrderConstraint};
use crate::auction_book::{
    CallAuctionCancellation, CallAuctionLevelSnapshot, CallAuctionOrderSnapshot, CallAuctionTrade,
};
use crate::auction_engine::{
    CallAuctionCommand, CallAuctionCommandOutcome, CallAuctionEngine, CallAuctionEvent,
    CallAuctionEventKind, CallAuctionExecutionReport, CallAuctionPhase, CallAuctionPhaseSnapshot,
};
use crate::domain::{
    AccountId, AuctionId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs, TradeId,
};
use crate::instrument::InstrumentDefinition;

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
    /// Publication scratch or mirror storage could not be reserved.
    CapacityReservationFailed(&'static str),
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
            Self::CapacityReservationFailed(resource) => {
                write!(
                    formatter,
                    "could not reserve auction market-data {resource}"
                )
            }
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
    priority_sequence: u64,
}

impl From<CallAuctionOrderSnapshot> for TrackedOrder {
    fn from(order: CallAuctionOrderSnapshot) -> Self {
        Self {
            account_id: order.account_id,
            side: order.side,
            constraint: order.constraint,
            leaves: order.quantity.lots(),
            priority_sequence: order.priority_sequence,
        }
    }
}

#[derive(Debug, Default)]
struct UncrossProgress {
    sources: HashMap<OrderId, u64>,
    trade_count: u64,
    cancellation_count: u64,
    executed_quantity: u128,
    price: Option<Price>,
    completed: bool,
}

/// Stateful adapter from private auction traces to anonymized public state.
#[derive(Debug)]
pub struct CallAuctionMarketDataPublisher {
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    phase: CallAuctionPhaseSnapshot,
    book_revision: u64,
    market_buy: Aggregate,
    market_sell: Aggregate,
    bids: BTreeMap<Price, Aggregate>,
    asks: BTreeMap<Price, Aggregate>,
    orders: HashMap<OrderId, TrackedOrder>,
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
    /// Returns [`CallAuctionMarketDataError::SourceDivergence`] if the engine's
    /// private orders and maintained aggregate book disagree.
    pub fn from_engine(engine: &CallAuctionEngine) -> Result<Self, CallAuctionMarketDataError> {
        let definition = engine.book().definition();
        let mut orders = HashMap::new();
        orders
            .try_reserve(engine.limits().book().max_active_orders())
            .map_err(|_| CallAuctionMarketDataError::CapacityReservationFailed("order mirror"))?;
        for order in engine.book().checkpoint_active_orders() {
            orders.insert(order.order_id, order.into());
        }
        let publisher = Self {
            instrument_id: definition.instrument_id(),
            instrument_version: definition.version(),
            phase: engine.phase_snapshot(),
            book_revision: engine.book().state_revision(),
            market_buy: aggregate_market(engine, Side::Buy),
            market_sell: aggregate_market(engine, Side::Sell),
            bids: aggregate_depth(engine, Side::Buy),
            asks: aggregate_depth(engine, Side::Sell),
            orders,
            last_command_sequence: previous_sequence(
                engine.next_command_sequence(),
                "auction next command sequence is zero",
            )?,
            last_event_sequence: previous_sequence(
                engine.next_event_sequence(),
                "auction next event sequence is zero",
            )?,
            last_trade_id: previous_trade_id(engine.book().next_trade_id())?,
            poisoned: false,
        };
        publisher.validate_against(engine)?;
        Ok(publisher)
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

    /// Materializes a complete full-depth recovery image in `O(P)` time.
    #[must_use]
    pub fn snapshot(&self) -> CallAuctionMarketDataSnapshot {
        CallAuctionMarketDataSnapshot {
            instrument_id: self.instrument_id,
            instrument_version: self.instrument_version,
            as_of_sequence: self.last_event_sequence,
            command_sequence: self.last_command_sequence,
            phase: self.phase,
            book_revision: self.book_revision,
            last_trade_id: self.last_trade_id,
            market_buy: self.market_level(Side::Buy),
            market_sell: self.market_level(Side::Sell),
            bids: self.limit_levels(Side::Buy),
            asks: self.limit_levels(Side::Sell),
        }
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

        let mut updates = Vec::new();
        if updates.try_reserve_exact(report.events.len()).is_err() {
            return self.fail(CallAuctionMarketDataError::CapacityReservationFailed(
                "update batch",
            ));
        }
        let mut uncross = UncrossProgress::default();
        if uncross.sources.try_reserve(self.orders.len()).is_err() {
            return self.fail(CallAuctionMarketDataError::CapacityReservationFailed(
                "uncross source mirror",
            ));
        }
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
            let kind = match self.apply_event(command, event, &mut uncross) {
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
        self.last_command_sequence = report.command_sequence;
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
        let active = engine.book().checkpoint_active_orders();
        if active.len() != self.orders.len()
            || active
                .iter()
                .any(|order| self.orders.get(&order.order_id).copied() != Some((*order).into()))
        {
            return Err(CallAuctionMarketDataError::SourceDivergence(
                "publisher active-order mirror differs from the auction book",
            ));
        }
        if self.market_buy != aggregate_market(engine, Side::Buy)
            || self.market_sell != aggregate_market(engine, Side::Sell)
            || self.bids != aggregate_depth(engine, Side::Buy)
            || self.asks != aggregate_depth(engine, Side::Sell)
        {
            return Err(CallAuctionMarketDataError::SourceDivergence(
                "publisher aggregate state differs from the auction book",
            ));
        }
        Ok(())
    }

    fn apply_event(
        &mut self,
        command: CallAuctionCommand,
        event: CallAuctionEvent,
        uncross: &mut UncrossProgress,
    ) -> Result<CallAuctionMarketDataKind, CallAuctionMarketDataError> {
        match event.kind {
            CallAuctionEventKind::PhaseChanged {
                auction_id,
                previous,
                current,
                revision,
            } => self.apply_phase(command, auction_id, previous, current, revision),
            CallAuctionEventKind::OrderAccepted(order) => self.apply_accept(command, order),
            CallAuctionEventKind::OrderCancelled { order, reason } => {
                if reason != crate::auction_engine::CallAuctionCancellationReason::UserRequested {
                    return Err(CallAuctionMarketDataError::TraceMismatch(
                        "standalone auction cancellation has a non-user reason",
                    ));
                }
                self.apply_user_cancel(command, order)
            }
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
        let CallAuctionCommand::Submit(source) = command else {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "non-submit command emitted an accepted order",
            ));
        };
        let submitted = source.order;
        if self.phase.phase() != CallAuctionPhase::Collecting
            || self.phase.active_auction_id() != Some(source.auction_id)
            || source.expected_phase_revision != self.phase.revision()
            || order.order_id != submitted.order_id()
            || order.account_id != submitted.account_id()
            || order.side != submitted.side()
            || order.constraint != submitted.constraint()
            || order.quantity != submitted.quantity()
            || order.priority_sequence == 0
            || self.orders.contains_key(&order.order_id)
        {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "accepted order contradicts its command or collection state",
            ));
        }
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
        remember_source(uncross, trade.buy_order_id(), buy_order.leaves)?;
        remember_source(uncross, trade.sell_order_id(), sell_order.leaves)?;
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
        remember_source(uncross, cancellation.order_id(), tracked.leaves)?;
        let source_quantity = uncross.sources[&cancellation.order_id()];
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
        let aggregate = self.aggregate_mut(side, constraint);
        aggregate.quantity = aggregate
            .quantity
            .checked_add(u128::from(quantity))
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        aggregate.order_count = aggregate
            .order_count
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        level_from_aggregate(side, constraint, *aggregate)
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
        let result = {
            let aggregate = self.aggregate_mut(order.side, order.constraint);
            aggregate.quantity = aggregate.quantity.checked_sub(u128::from(quantity)).ok_or(
                CallAuctionMarketDataError::TraceMismatch(
                    "auction aggregate underflowed during execution",
                ),
            )?;
            if removes_order {
                aggregate.order_count = aggregate.order_count.checked_sub(1).ok_or(
                    CallAuctionMarketDataError::TraceMismatch(
                        "auction aggregate order count underflowed",
                    ),
                )?;
            }
            *aggregate
        };
        if removes_order {
            self.orders.remove(&order_id);
        } else {
            self.orders
                .get_mut(&order_id)
                .expect("checked auction order exists")
                .leaves -= quantity;
        }
        self.remove_empty_level(order.side, order.constraint, result);
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

    fn aggregate_mut(&mut self, side: Side, constraint: AuctionOrderConstraint) -> &mut Aggregate {
        match (side, constraint) {
            (Side::Buy, AuctionOrderConstraint::Market) => &mut self.market_buy,
            (Side::Sell, AuctionOrderConstraint::Market) => &mut self.market_sell,
            (Side::Buy, AuctionOrderConstraint::Limit(price)) => {
                self.bids.entry(price).or_default()
            }
            (Side::Sell, AuctionOrderConstraint::Limit(price)) => {
                self.asks.entry(price).or_default()
            }
        }
    }

    fn remove_empty_level(
        &mut self,
        side: Side,
        constraint: AuctionOrderConstraint,
        aggregate: Aggregate,
    ) {
        if aggregate != Aggregate::default() {
            return;
        }
        match (side, constraint) {
            (Side::Buy, AuctionOrderConstraint::Limit(price)) => {
                self.bids.remove(&price);
            }
            (Side::Sell, AuctionOrderConstraint::Limit(price)) => {
                self.asks.remove(&price);
            }
            (_, AuctionOrderConstraint::Market) => {}
        }
    }

    fn market_level(&self, side: Side) -> CallAuctionMarketDataLevel {
        let aggregate = match side {
            Side::Buy => self.market_buy,
            Side::Sell => self.market_sell,
        };
        level_from_aggregate(side, AuctionOrderConstraint::Market, aggregate)
            .expect("publisher maintains valid market aggregate")
    }

    fn limit_levels(&self, side: Side) -> Vec<CallAuctionMarketDataLevel> {
        let map = |(&price, &aggregate): (&Price, &Aggregate)| {
            level_from_aggregate(side, AuctionOrderConstraint::Limit(price), aggregate)
                .expect("publisher maintains valid limit aggregate")
        };
        match side {
            Side::Buy => self.bids.iter().rev().map(map).collect(),
            Side::Sell => self.asks.iter().map(map).collect(),
        }
    }

    fn fail<T>(
        &mut self,
        error: CallAuctionMarketDataError,
    ) -> Result<T, CallAuctionMarketDataError> {
        self.poisoned = true;
        Err(error)
    }
}

/// Gap-detecting consumer state for one immutable call-auction instrument version.
#[derive(Debug)]
pub struct CallAuctionMarketDataReplica {
    definition: InstrumentDefinition,
    phase: CallAuctionPhaseSnapshot,
    book_revision: u64,
    market_buy: Aggregate,
    market_sell: Aggregate,
    bids: BTreeMap<Price, Aggregate>,
    asks: BTreeMap<Price, Aggregate>,
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
    #[must_use]
    pub fn new(definition: InstrumentDefinition) -> Self {
        Self {
            definition,
            phase: CallAuctionPhaseSnapshot::default(),
            book_revision: 0,
            market_buy: Aggregate::default(),
            market_sell: Aggregate::default(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_command_sequence: 0,
            last_event_sequence: 0,
            last_trade_id: None,
            cycle_trade_count: 0,
            cycle_cancellation_count: 0,
            cycle_executed_quantity: 0,
            cycle_trade_price: None,
            poisoned: false,
        }
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
        self.phase = snapshot.phase;
        self.book_revision = snapshot.book_revision;
        self.market_buy = aggregate_from_level(snapshot.market_buy);
        self.market_sell = aggregate_from_level(snapshot.market_sell);
        self.bids = snapshot
            .bids
            .iter()
            .map(|level| (limit_price(*level), aggregate_from_level(*level)))
            .collect();
        self.asks = snapshot
            .asks
            .iter()
            .map(|level| (limit_price(*level), aggregate_from_level(*level)))
            .collect();
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
        let next_command_sequence = self
            .last_command_sequence
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        let mut expected = self
            .last_event_sequence
            .checked_add(1)
            .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        for update in &batch.updates {
            self.preflight_identity(*update)?;
            if update.sequence != expected {
                return Err(CallAuctionMarketDataError::SequenceGap {
                    expected,
                    actual: update.sequence,
                });
            }
            expected = expected
                .checked_add(1)
                .ok_or(CallAuctionMarketDataError::ArithmeticOverflow)?;
        }
        for update in &batch.updates {
            if let Err(error) = self.apply_verified(*update) {
                self.poisoned = true;
                return Err(error);
            }
        }
        self.last_command_sequence = next_command_sequence;
        Ok(())
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
        if let Err(error) = self.apply_verified(update) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    /// Returns limit depth in best-to-worst order.
    ///
    /// # Panics
    ///
    /// Panics only if previously validated replica state contains an empty
    /// limit aggregate, which indicates an internal invariant violation.
    #[must_use]
    pub fn limit_depth(&self, side: Side, limit: usize) -> Vec<CallAuctionLevelSnapshot> {
        let map = |(&price, aggregate): (&Price, &Aggregate)| {
            CallAuctionLevelSnapshot::from_parts(
                side,
                price,
                aggregate.quantity,
                aggregate.order_count,
            )
            .expect("replica stores only non-empty limit levels")
        };
        match side {
            Side::Buy => self.bids.iter().rev().take(limit).map(map).collect(),
            Side::Sell => self.asks.iter().take(limit).map(map).collect(),
        }
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
                        if self.phase.phase() == CallAuctionPhase::Closed {
                            return Err(CallAuctionMarketDataError::InvalidUpdate(
                                "user cancellation has no active auction cycle",
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
                self.set_level(level);
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
                self.set_level(buy);
                self.set_level(sell);
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

    fn set_level(&mut self, level: CallAuctionMarketDataLevel) {
        let aggregate = aggregate_from_level(level);
        match (level.side, level.constraint) {
            (Side::Buy, AuctionOrderConstraint::Market) => self.market_buy = aggregate,
            (Side::Sell, AuctionOrderConstraint::Market) => self.market_sell = aggregate,
            (Side::Buy, AuctionOrderConstraint::Limit(price)) => {
                set_limit_level(&mut self.bids, price, aggregate);
            }
            (Side::Sell, AuctionOrderConstraint::Limit(price)) => {
                set_limit_level(&mut self.asks, price, aggregate);
            }
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
    if report.events.windows(2).any(|window| {
        window[0]
            .sequence
            .checked_add(1)
            .is_none_or(|expected| window[1].sequence != expected)
    }) {
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
        CallAuctionCommand::Uncross(value) => value.instrument_id,
    }
}

fn command_version(command: CallAuctionCommand) -> InstrumentVersion {
    match command {
        CallAuctionCommand::PhaseControl(value) => value.instrument_version,
        CallAuctionCommand::Submit(value) => value.order.instrument_version(),
        CallAuctionCommand::Cancel(value) => value.instrument_version,
        CallAuctionCommand::Uncross(value) => value.instrument_version,
    }
}

fn remember_source(
    uncross: &mut UncrossProgress,
    order_id: OrderId,
    leaves: u64,
) -> Result<(), CallAuctionMarketDataError> {
    if let Some(source) = uncross.sources.get(&order_id) {
        if *source < leaves {
            return Err(CallAuctionMarketDataError::TraceMismatch(
                "auction order leaves increased during one uncross",
            ));
        }
    } else {
        uncross.sources.insert(order_id, leaves);
    }
    Ok(())
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

fn aggregate_depth(engine: &CallAuctionEngine, side: Side) -> BTreeMap<Price, Aggregate> {
    engine
        .book()
        .limit_depth(side, usize::MAX)
        .into_iter()
        .map(|level| {
            (
                level.price(),
                Aggregate {
                    quantity: level.quantity(),
                    order_count: level.order_count(),
                },
            )
        })
        .collect()
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

fn set_limit_level(levels: &mut BTreeMap<Price, Aggregate>, price: Price, aggregate: Aggregate) {
    if aggregate == Aggregate::default() {
        levels.remove(&price);
    } else {
        levels.insert(price, aggregate);
    }
}

fn limit_price(level: CallAuctionMarketDataLevel) -> Price {
    match level.constraint {
        AuctionOrderConstraint::Limit(price) => price,
        AuctionOrderConstraint::Market => unreachable!("validated snapshot limit"),
    }
}
