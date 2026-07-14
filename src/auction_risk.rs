//! Deterministic pre-trade risk and conservative reservations for call auctions.
//!
//! The coupled engine sequences core business failures before risk, converts
//! risk failures into ordinary idempotent auction reports, reserves every
//! accepted market or limit order at its maximum reachable absolute collar
//! price, and applies all uncross position deltas only after netting per account.

use std::collections::HashMap;
use std::fmt;

use crate::auction::AuctionOrderConstraint;
use crate::auction_book::{
    CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrderSnapshot,
};
use crate::auction_engine::{
    CallAuctionCheckpoint, CallAuctionCheckpointError, CallAuctionCommand,
    CallAuctionCommandOutcome, CallAuctionCommandPreparation, CallAuctionEngine,
    CallAuctionEngineConstructionError, CallAuctionEngineError, CallAuctionEngineLimits,
    CallAuctionEngineLimitsSpec, CallAuctionEventKind, CallAuctionExecutionReport,
    CallAuctionRejectReason, PreparedCallAuctionCommand,
};
use crate::domain::{AccountId, OrderId, Side};
use crate::instrument::InstrumentDefinition;
use crate::risk::{
    RiskAccountCheckpoint, RiskError, RiskHashIndex, RiskHashIndexStatus, RiskPriceConstraint,
    RiskProfile, RiskRejectReason, RiskSnapshot, conservative_order_notional,
    conservative_price_magnitude, evaluate_pretrade_order, position_within_limits,
    worst_case_position_within_limits,
};

/// Raw finite resources for one coupled call-auction/risk shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionRiskLimitsSpec {
    /// Independently validated call-auction engine resources.
    pub auction: CallAuctionEngineLimits,
    /// Maximum immutable account profiles registered in this shard.
    pub max_registered_accounts: usize,
}

/// Invalid coupled call-auction/risk resource policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionRiskLimitsError {
    /// The shard could not register any risk profile.
    ZeroRegisteredAccounts,
}

impl fmt::Display for CallAuctionRiskLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRegisteredAccounts => {
                formatter.write_str("registered auction risk-account limit is zero")
            }
        }
    }
}

impl std::error::Error for CallAuctionRiskLimitsError {}

/// Validated resources for one coupled call-auction/risk shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionRiskLimits {
    auction: CallAuctionEngineLimits,
    max_registered_accounts: usize,
}

impl CallAuctionRiskLimits {
    /// Default maximum immutable risk profiles in one auction shard.
    pub const DEFAULT_MAX_REGISTERED_ACCOUNTS: usize = 65_536;

    /// Validates one finite coupled resource policy.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionRiskLimitsError::ZeroRegisteredAccounts`] for zero
    /// profile capacity.
    pub const fn new(spec: CallAuctionRiskLimitsSpec) -> Result<Self, CallAuctionRiskLimitsError> {
        if spec.max_registered_accounts == 0 {
            return Err(CallAuctionRiskLimitsError::ZeroRegisteredAccounts);
        }
        Ok(Self {
            auction: spec.auction,
            max_registered_accounts: spec.max_registered_accounts,
        })
    }

    /// Returns the embedded auction-engine resource policy.
    #[must_use]
    pub const fn auction(self) -> CallAuctionEngineLimits {
        self.auction
    }

    /// Returns maximum registered account profiles.
    #[must_use]
    pub const fn max_registered_accounts(self) -> usize {
        self.max_registered_accounts
    }
}

impl Default for CallAuctionRiskLimits {
    fn default() -> Self {
        Self {
            auction: CallAuctionEngineLimits::default(),
            max_registered_accounts: Self::DEFAULT_MAX_REGISTERED_ACCOUNTS,
        }
    }
}

/// Failure while constructing one coupled call-auction/risk shard.
#[derive(Debug)]
pub enum CallAuctionRiskConstructionError {
    /// The underlying auction engine could not be constructed.
    Auction(CallAuctionEngineConstructionError),
    /// Complete immutable account-registry reservation failed.
    AccountReservationFailed,
    /// Complete active-order reservation-index reservation failed.
    OrderReservationFailed,
    /// Complete uncross position-netting scratch reservation failed.
    PositionScratchReservationFailed,
}

impl fmt::Display for CallAuctionRiskConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auction(error) => write!(formatter, "call-auction risk engine: {error}"),
            Self::AccountReservationFailed => {
                formatter.write_str("call-auction risk account reservation failed")
            }
            Self::OrderReservationFailed => {
                formatter.write_str("call-auction risk order reservation failed")
            }
            Self::PositionScratchReservationFailed => {
                formatter.write_str("call-auction risk position scratch reservation failed")
            }
        }
    }
}

impl std::error::Error for CallAuctionRiskConstructionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Auction(error) => Some(error),
            Self::AccountReservationFailed
            | Self::OrderReservationFailed
            | Self::PositionScratchReservationFailed => None,
        }
    }
}

impl From<CallAuctionEngineConstructionError> for CallAuctionRiskConstructionError {
    fn from(error: CallAuctionEngineConstructionError) -> Self {
        Self::Auction(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CallAuctionRiskAccount {
    profile: RiskProfile,
    exposure: RiskSnapshot,
}

/// Conservative reservation retained for one active call-auction order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionReservationSnapshot {
    account_id: AccountId,
    side: Side,
    constraint: AuctionOrderConstraint,
    valuation_per_lot: u64,
    quantity_lots: u64,
    notional: u128,
}

impl CallAuctionReservationSnapshot {
    /// Returns the owning account.
    #[must_use]
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }

    /// Returns the reserved side.
    #[must_use]
    pub const fn side(self) -> Side {
        self.side
    }

    /// Returns the order's market or limit constraint.
    #[must_use]
    pub const fn constraint(self) -> AuctionOrderConstraint {
        self.constraint
    }

    /// Returns maximum reachable absolute price magnitude reserved per lot.
    #[must_use]
    pub const fn valuation_per_lot(self) -> u64 {
        self.valuation_per_lot
    }

    /// Returns active reserved quantity in lots.
    #[must_use]
    pub const fn quantity_lots(self) -> u64 {
        self.quantity_lots
    }

    /// Returns conservative raw-price-times-lots notional.
    #[must_use]
    pub const fn notional(self) -> u128 {
        self.notional
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ExecutedLots {
    buys: u128,
    sells: u128,
}

/// Immutable profiles, positions, and active call-auction reservations.
#[derive(Debug)]
pub struct CallAuctionRiskEngine {
    definition: InstrumentDefinition,
    accounts: HashMap<AccountId, CallAuctionRiskAccount>,
    reservations: HashMap<OrderId, CallAuctionReservationSnapshot>,
    position_scratch: HashMap<AccountId, ExecutedLots>,
    maximum_accounts: usize,
    maximum_reservations: usize,
}

impl CallAuctionRiskEngine {
    fn try_with_limits(
        definition: InstrumentDefinition,
        limits: CallAuctionRiskLimits,
    ) -> Result<Self, CallAuctionRiskConstructionError> {
        let mut accounts = HashMap::new();
        accounts
            .try_reserve(limits.max_registered_accounts())
            .map_err(|_| CallAuctionRiskConstructionError::AccountReservationFailed)?;
        let maximum_reservations = limits.auction().book().max_active_orders();
        let mut reservations = HashMap::new();
        reservations
            .try_reserve(maximum_reservations)
            .map_err(|_| CallAuctionRiskConstructionError::OrderReservationFailed)?;
        let mut position_scratch = HashMap::new();
        position_scratch
            .try_reserve(limits.max_registered_accounts())
            .map_err(|_| CallAuctionRiskConstructionError::PositionScratchReservationFailed)?;
        Ok(Self {
            definition,
            accounts,
            reservations,
            position_scratch,
            maximum_accounts: limits.max_registered_accounts(),
            maximum_reservations,
        })
    }

    /// Registers one immutable account profile.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError::DuplicateProfile`] or
    /// [`RiskError::ProfileCapacityExhausted`] before mutation.
    ///
    /// # Panics
    ///
    /// Panics only if private corruption removed constructor-owned hash
    /// headroom without changing its configured bound.
    pub fn register_account(
        &mut self,
        account_id: AccountId,
        profile: RiskProfile,
    ) -> Result<(), RiskError> {
        if self.accounts.contains_key(&account_id) {
            return Err(RiskError::DuplicateProfile(account_id));
        }
        if self.accounts.len() >= self.maximum_accounts {
            return Err(RiskError::ProfileCapacityExhausted {
                maximum: self.maximum_accounts,
            });
        }
        let capacity = self.accounts.capacity();
        assert!(self.accounts.len() < capacity);
        assert!(
            self.accounts
                .insert(
                    account_id,
                    CallAuctionRiskAccount {
                        profile,
                        exposure: RiskSnapshot::from_parts(
                            profile.initial_position_lots(),
                            0,
                            0,
                            0,
                            0,
                        ),
                    },
                )
                .is_none()
        );
        debug_assert_eq!(self.accounts.capacity(), capacity);
        Ok(())
    }

    /// Returns one account's current position and open exposure.
    #[must_use]
    pub fn snapshot(&self, account_id: AccountId) -> Option<RiskSnapshot> {
        self.accounts
            .get(&account_id)
            .map(|account| account.exposure)
    }

    /// Returns one active auction-order reservation.
    #[must_use]
    pub fn reservation(&self, order_id: OrderId) -> Option<CallAuctionReservationSnapshot> {
        self.reservations.get(&order_id).copied()
    }

    /// Returns active reservation count.
    #[must_use]
    pub fn reservation_count(&self) -> usize {
        self.reservations.len()
    }

    /// Returns allocation telemetry for one constructor-reserved risk index.
    #[must_use]
    pub fn hash_index_status(&self, index: RiskHashIndex) -> RiskHashIndexStatus {
        match index {
            RiskHashIndex::AccountProfiles => RiskHashIndexStatus {
                configured_entries: self.maximum_accounts,
                allocated_entries: self.accounts.capacity(),
                occupied_entries: self.accounts.len(),
            },
            RiskHashIndex::ActiveReservations => RiskHashIndexStatus {
                configured_entries: self.maximum_reservations,
                allocated_entries: self.reservations.capacity(),
                occupied_entries: self.reservations.len(),
            },
        }
    }

    fn authorize(&self, command: CallAuctionCommand) -> Result<(), CallAuctionRejectReason> {
        let CallAuctionCommand::Submit(submit) = command else {
            return Ok(());
        };
        let order = submit.order;
        let account = self
            .accounts
            .get(&order.account_id())
            .ok_or(CallAuctionRejectReason::RiskProfileMissing)?;
        let constraint = risk_constraint(order.constraint());
        let notional = conservative_order_notional(
            self.definition,
            order.side(),
            constraint,
            order.quantity().lots(),
        )
        .map_err(call_auction_risk_rejection)?;
        evaluate_pretrade_order(
            account.profile,
            account.exposure,
            order.side(),
            order.quantity().lots(),
            notional,
            true,
        )
        .map_err(call_auction_risk_rejection)
    }

    fn apply(&mut self, command: CallAuctionCommand, report: &CallAuctionExecutionReport) {
        if report.replayed || report.outcome != CallAuctionCommandOutcome::Accepted {
            return;
        }
        self.position_scratch.clear();
        for event in report.events.iter().copied() {
            match event.kind {
                CallAuctionEventKind::OrderAccepted(order) => self.insert_reservation(order),
                CallAuctionEventKind::OrderCancelled { order, .. } => {
                    self.remove_reservation(order.order_id);
                }
                CallAuctionEventKind::Trade(trade) => {
                    self.decrement_reservation(trade.buy_order_id(), trade.quantity().lots());
                    self.decrement_reservation(trade.sell_order_id(), trade.quantity().lots());
                    self.add_execution(trade.buy_account_id(), Side::Buy, trade.quantity().lots());
                    self.add_execution(
                        trade.sell_account_id(),
                        Side::Sell,
                        trade.quantity().lots(),
                    );
                }
                CallAuctionEventKind::RemainderCancelled(cancellation) => {
                    self.remove_reservation(cancellation.order_id());
                }
                CallAuctionEventKind::PhaseChanged { .. }
                | CallAuctionEventKind::UncrossCompleted { .. }
                | CallAuctionEventKind::CommandRejected(_) => {}
            }
        }
        if matches!(command, CallAuctionCommand::Uncross(_)) {
            self.apply_netted_positions();
        } else {
            debug_assert!(self.position_scratch.is_empty());
        }
    }

    fn insert_reservation(&mut self, order: CallAuctionOrderSnapshot) {
        let capacity = self.reservations.capacity();
        assert!(self.reservations.len() < capacity);
        let constraint = risk_constraint(order.constraint);
        let valuation_per_lot =
            conservative_price_magnitude(self.definition, order.side, constraint);
        let notional = u128::from(valuation_per_lot)
            .checked_mul(u128::from(order.quantity.lots()))
            .expect("authorized auction notional must remain representable");
        let reservation = CallAuctionReservationSnapshot {
            account_id: order.account_id,
            side: order.side,
            constraint: order.constraint,
            valuation_per_lot,
            quantity_lots: order.quantity.lots(),
            notional,
        };
        assert!(
            self.reservations
                .insert(order.order_id, reservation)
                .is_none()
        );
        debug_assert_eq!(self.reservations.capacity(), capacity);
        self.add_open_exposure(reservation);
    }

    fn decrement_reservation(&mut self, order_id: OrderId, quantity_lots: u64) {
        let current = self
            .reservations
            .get(&order_id)
            .copied()
            .expect("auction trade order must have a risk reservation");
        assert!(quantity_lots <= current.quantity_lots);
        self.remove_reservation(order_id);
        let remaining = current.quantity_lots - quantity_lots;
        if remaining == 0 {
            return;
        }
        let capacity = self.reservations.capacity();
        let notional = u128::from(current.valuation_per_lot)
            .checked_mul(u128::from(remaining))
            .expect("partial auction reservation notional remains representable");
        let replacement = CallAuctionReservationSnapshot {
            quantity_lots: remaining,
            notional,
            ..current
        };
        assert!(self.reservations.insert(order_id, replacement).is_none());
        debug_assert_eq!(self.reservations.capacity(), capacity);
        self.add_open_exposure(replacement);
    }

    fn remove_reservation(&mut self, order_id: OrderId) -> CallAuctionReservationSnapshot {
        let reservation = self
            .reservations
            .remove(&order_id)
            .expect("active auction order must have a risk reservation");
        let account = self
            .accounts
            .get_mut(&reservation.account_id)
            .expect("reserved auction account must have a risk profile");
        let exposure = account.exposure;
        let quantity = u128::from(reservation.quantity_lots);
        let (open_buy_lots, open_sell_lots) = match reservation.side {
            Side::Buy => (
                exposure
                    .open_buy_lots()
                    .checked_sub(quantity)
                    .expect("buy reservation aggregate cannot underflow"),
                exposure.open_sell_lots(),
            ),
            Side::Sell => (
                exposure.open_buy_lots(),
                exposure
                    .open_sell_lots()
                    .checked_sub(quantity)
                    .expect("sell reservation aggregate cannot underflow"),
            ),
        };
        account.exposure = RiskSnapshot::from_parts(
            exposure.position_lots(),
            open_buy_lots,
            open_sell_lots,
            exposure
                .open_notional()
                .checked_sub(reservation.notional)
                .expect("reservation notional aggregate cannot underflow"),
            exposure
                .open_orders()
                .checked_sub(1)
                .expect("reservation count aggregate cannot underflow"),
        );
        reservation
    }

    fn add_open_exposure(&mut self, reservation: CallAuctionReservationSnapshot) {
        let account = self
            .accounts
            .get_mut(&reservation.account_id)
            .expect("authorized auction account must have a risk profile");
        let exposure = account.exposure;
        let quantity = u128::from(reservation.quantity_lots);
        let (open_buy_lots, open_sell_lots) = match reservation.side {
            Side::Buy => (
                exposure
                    .open_buy_lots()
                    .checked_add(quantity)
                    .expect("authorized buy aggregate remains representable"),
                exposure.open_sell_lots(),
            ),
            Side::Sell => (
                exposure.open_buy_lots(),
                exposure
                    .open_sell_lots()
                    .checked_add(quantity)
                    .expect("authorized sell aggregate remains representable"),
            ),
        };
        account.exposure = RiskSnapshot::from_parts(
            exposure.position_lots(),
            open_buy_lots,
            open_sell_lots,
            exposure
                .open_notional()
                .checked_add(reservation.notional)
                .expect("authorized notional aggregate remains representable"),
            exposure
                .open_orders()
                .checked_add(1)
                .expect("authorized order aggregate remains representable"),
        );
    }

    fn add_execution(&mut self, account_id: AccountId, side: Side, quantity_lots: u64) {
        let capacity = self.position_scratch.capacity();
        let delta = self.position_scratch.entry(account_id).or_default();
        match side {
            Side::Buy => {
                delta.buys = delta
                    .buys
                    .checked_add(u128::from(quantity_lots))
                    .expect("authorized auction buy execution remains representable");
            }
            Side::Sell => {
                delta.sells = delta
                    .sells
                    .checked_add(u128::from(quantity_lots))
                    .expect("authorized auction sell execution remains representable");
            }
        }
        debug_assert_eq!(self.position_scratch.capacity(), capacity);
    }

    fn apply_netted_positions(&mut self) {
        for (&account_id, delta) in &self.position_scratch {
            let account = self
                .accounts
                .get_mut(&account_id)
                .expect("executing auction account must have a risk profile");
            let position = account.exposure.position_lots();
            let next = if delta.buys >= delta.sells {
                let net = i128::try_from(delta.buys - delta.sells)
                    .expect("authorized net auction buy fits signed position");
                position
                    .checked_add(net)
                    .expect("authorized auction long position remains representable")
            } else {
                let net = i128::try_from(delta.sells - delta.buys)
                    .expect("authorized net auction sell fits signed position");
                position
                    .checked_sub(net)
                    .expect("authorized auction short position remains representable")
            };
            let exposure = account.exposure;
            account.exposure = RiskSnapshot::from_parts(
                next,
                exposure.open_buy_lots(),
                exposure.open_sell_lots(),
                exposure.open_notional(),
                exposure.open_orders(),
            );
        }
        self.position_scratch.clear();
    }

    fn validate(&self) -> Result<(), CallAuctionRiskInvariantViolation> {
        self.validate_resource_bounds()?;
        let mut aggregates: HashMap<AccountId, (u128, u128, u128, u64)> = HashMap::new();
        aggregates.try_reserve(self.accounts.len()).map_err(|_| {
            CallAuctionRiskInvariantViolation::new("auction risk audit allocation failed")
        })?;
        for (&order_id, reservation) in &self.reservations {
            let constraint = risk_constraint(reservation.constraint);
            let expected_valuation =
                conservative_price_magnitude(self.definition, reservation.side, constraint);
            let expected_notional = u128::from(expected_valuation)
                .checked_mul(u128::from(reservation.quantity_lots))
                .ok_or_else(|| {
                    CallAuctionRiskInvariantViolation::new(format!(
                        "auction reservation {order_id} notional overflows"
                    ))
                })?;
            if reservation.quantity_lots == 0
                || reservation.valuation_per_lot != expected_valuation
                || reservation.notional != expected_notional
                || !self.accounts.contains_key(&reservation.account_id)
            {
                return Err(CallAuctionRiskInvariantViolation::new(format!(
                    "auction reservation {order_id} is invalid"
                )));
            }
            let aggregate = aggregates.entry(reservation.account_id).or_default();
            match reservation.side {
                Side::Buy => {
                    aggregate.0 = aggregate
                        .0
                        .checked_add(u128::from(reservation.quantity_lots))
                        .ok_or_else(|| {
                            CallAuctionRiskInvariantViolation::new(
                                "auction buy reservation aggregate overflows",
                            )
                        })?;
                }
                Side::Sell => {
                    aggregate.1 = aggregate
                        .1
                        .checked_add(u128::from(reservation.quantity_lots))
                        .ok_or_else(|| {
                            CallAuctionRiskInvariantViolation::new(
                                "auction sell reservation aggregate overflows",
                            )
                        })?;
                }
            }
            aggregate.2 = aggregate
                .2
                .checked_add(reservation.notional)
                .ok_or_else(|| {
                    CallAuctionRiskInvariantViolation::new(
                        "auction reservation notional aggregate overflows",
                    )
                })?;
            aggregate.3 = aggregate.3.checked_add(1).ok_or_else(|| {
                CallAuctionRiskInvariantViolation::new(
                    "auction reservation count aggregate overflows",
                )
            })?;
        }
        for (&account_id, account) in &self.accounts {
            let expected = aggregates.get(&account_id).copied().unwrap_or_default();
            let exposure = account.exposure;
            if (
                exposure.open_buy_lots(),
                exposure.open_sell_lots(),
                exposure.open_notional(),
                exposure.open_orders(),
            ) != expected
                || !position_within_limits(exposure.position_lots(), account.profile.limits())
                || !worst_case_position_within_limits(
                    exposure.position_lots(),
                    exposure.open_buy_lots(),
                    exposure.open_sell_lots(),
                    account.profile.limits(),
                )
            {
                return Err(CallAuctionRiskInvariantViolation::new(format!(
                    "auction risk account {account_id} exposure is invalid"
                )));
            }
        }
        Ok(())
    }

    fn validate_resource_bounds(&self) -> Result<(), CallAuctionRiskInvariantViolation> {
        if self.accounts.capacity() < self.maximum_accounts
            || self.accounts.len() > self.maximum_accounts
        {
            return Err(CallAuctionRiskInvariantViolation::new(
                "auction risk account index contradicts configured capacity",
            ));
        }
        if self.reservations.capacity() < self.maximum_reservations
            || self.reservations.len() > self.maximum_reservations
        {
            return Err(CallAuctionRiskInvariantViolation::new(
                "auction risk reservation index contradicts configured capacity",
            ));
        }
        if self.position_scratch.capacity() < self.maximum_accounts
            || !self.position_scratch.is_empty()
        {
            return Err(CallAuctionRiskInvariantViolation::new(
                "auction risk position scratch is undersized or not quiescent",
            ));
        }
        Ok(())
    }
}

/// Structural inconsistency between call-auction and risk state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionRiskInvariantViolation {
    detail: String,
}

impl CallAuctionRiskInvariantViolation {
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

impl fmt::Display for CallAuctionRiskInvariantViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(formatter)
    }
}

impl std::error::Error for CallAuctionRiskInvariantViolation {}

/// Canonical coupled call-auction, risk-profile, position, and exposure state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionRiskCheckpoint {
    wal_first_sequence: u64,
    auction: CallAuctionCheckpoint,
    accounts: Vec<RiskAccountCheckpoint>,
}

impl CallAuctionRiskCheckpoint {
    /// Returns the first physical WAL sequence occupied by the definition.
    #[must_use]
    pub const fn wal_first_sequence(&self) -> u64 {
        self.wal_first_sequence
    }

    /// Returns the completed report boundary represented by this checkpoint.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.auction.generation()
    }

    /// Returns canonical direct auction state and complete command lineage.
    #[must_use]
    pub const fn auction(&self) -> &CallAuctionCheckpoint {
        &self.auction
    }

    /// Returns account-sorted immutable profiles and current exposures.
    #[must_use]
    pub fn accounts(&self) -> &[RiskAccountCheckpoint] {
        &self.accounts
    }

    pub(crate) fn from_parts(
        wal_first_sequence: u64,
        auction: CallAuctionCheckpoint,
        accounts: Vec<RiskAccountCheckpoint>,
    ) -> Result<Self, CallAuctionRiskCheckpointError> {
        let checkpoint = Self {
            wal_first_sequence,
            auction,
            accounts,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    fn validate(&self) -> Result<(), CallAuctionRiskCheckpointError> {
        if self.wal_first_sequence == 0 {
            return Err(CallAuctionRiskCheckpointError::new(
                "auction risk checkpoint WAL first sequence is zero",
            ));
        }
        let profile_count = u64::try_from(self.accounts.len()).map_err(|_| {
            CallAuctionRiskCheckpointError::new("auction risk checkpoint account count exceeds u64")
        })?;
        let expected_metadata_sequence = self
            .wal_first_sequence
            .checked_add(profile_count)
            .ok_or_else(|| {
                CallAuctionRiskCheckpointError::new(
                    "auction risk checkpoint metadata boundary overflows",
                )
            })?;
        if self.auction.wal_metadata_sequence() != expected_metadata_sequence {
            return Err(CallAuctionRiskCheckpointError::new(
                "auction risk checkpoint auction boundary does not follow profile metadata",
            ));
        }
        if self
            .accounts
            .windows(2)
            .any(|pair| pair[0].account_id() >= pair[1].account_id())
        {
            return Err(CallAuctionRiskCheckpointError::new(
                "auction risk checkpoint accounts are not strictly canonical",
            ));
        }

        let limits = checkpoint_validation_limits(self)?;
        let direct = self.restore_direct_with_limits(limits)?;
        let mut replay =
            CallAuctionRiskManagedEngine::try_with_limits(self.auction.definition(), limits)
                .map_err(|error| {
                    CallAuctionRiskCheckpointError::new(format!(
                        "auction risk checkpoint replay construction failed: {error}"
                    ))
                })?;
        for account in &self.accounts {
            replay.register_account(account.account_id(), account.profile())?;
        }
        for entry in self.auction.history() {
            let reproduced = replay.submit(entry.command()).map_err(|error| {
                CallAuctionRiskCheckpointError::new(format!(
                    "auction risk checkpoint history cannot be replayed: {error}"
                ))
            })?;
            if reproduced != *entry.report() {
                return Err(CallAuctionRiskCheckpointError::new(
                    "auction risk checkpoint history diverges under coupled replay",
                ));
            }
        }
        let replayed_auction = replay
            .engine
            .checkpoint(
                self.auction.wal_metadata_sequence(),
                self.auction.generation(),
            )
            .map_err(CallAuctionRiskCheckpointError::from)?;
        if replayed_auction != self.auction
            || checkpoint_accounts(&replay.risk) != self.accounts
            || checkpoint_accounts(&direct.risk) != self.accounts
        {
            return Err(CallAuctionRiskCheckpointError::new(
                "auction risk checkpoint direct state differs from coupled history replay",
            ));
        }
        Ok(())
    }

    fn restore_direct(
        &self,
    ) -> Result<CallAuctionRiskManagedEngine, CallAuctionRiskCheckpointError> {
        self.restore_direct_with_limits(CallAuctionRiskLimits::default())
    }

    fn restore_direct_with_limits(
        &self,
        limits: CallAuctionRiskLimits,
    ) -> Result<CallAuctionRiskManagedEngine, CallAuctionRiskCheckpointError> {
        if self.accounts.len() > limits.max_registered_accounts() {
            return Err(CallAuctionRiskCheckpointError::new(format!(
                "auction risk checkpoint account count {} exceeds selected capacity {}",
                self.accounts.len(),
                limits.max_registered_accounts()
            )));
        }
        let engine =
            CallAuctionEngine::from_checkpoint_with_limits(self.auction.clone(), limits.auction())?;
        let mut risk = CallAuctionRiskEngine::try_with_limits(self.auction.definition(), limits)
            .map_err(|error| {
                CallAuctionRiskCheckpointError::new(format!(
                    "auction risk checkpoint capacity reservation failed: {error}"
                ))
            })?;
        for account in &self.accounts {
            risk.register_account(account.account_id(), account.profile())?;
            let registered = risk
                .accounts
                .get_mut(&account.account_id())
                .expect("registered checkpoint account exists");
            registered.exposure =
                RiskSnapshot::from_parts(account.exposure().position_lots(), 0, 0, 0, 0);
        }
        for order in self.auction.active_orders().iter().copied() {
            if !risk.accounts.contains_key(&order.account_id) {
                return Err(CallAuctionRiskCheckpointError::new(format!(
                    "auction risk checkpoint active order {} has no account profile",
                    order.order_id
                )));
            }
            risk.insert_reservation(order);
        }
        for account in &self.accounts {
            let restored = risk
                .snapshot(account.account_id())
                .expect("registered checkpoint account exists");
            if restored != account.exposure() {
                return Err(CallAuctionRiskCheckpointError::new(format!(
                    "auction risk checkpoint account {} exposure differs from active reservations",
                    account.account_id()
                )));
            }
        }
        let managed = CallAuctionRiskManagedEngine {
            engine,
            risk,
            limits,
        };
        managed
            .validate()
            .map_err(|error| CallAuctionRiskCheckpointError::new(error.detail()))?;
        Ok(managed)
    }

    /// Returns whether this checkpoint extends the same immutable profile and
    /// auction-command lineage as `previous`.
    #[must_use]
    pub fn is_successor_of(&self, previous: &Self) -> bool {
        self.wal_first_sequence == previous.wal_first_sequence
            && self.accounts.len() == previous.accounts.len()
            && self
                .accounts
                .iter()
                .zip(&previous.accounts)
                .all(|(current, old)| {
                    current.account_id() == old.account_id() && current.profile() == old.profile()
                })
            && self.auction.is_successor_of(&previous.auction)
    }
}

/// Semantic coupled auction/risk checkpoint construction or restoration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionRiskCheckpointError {
    detail: String,
}

impl CallAuctionRiskCheckpointError {
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

impl fmt::Display for CallAuctionRiskCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(formatter)
    }
}

impl std::error::Error for CallAuctionRiskCheckpointError {}

impl From<CallAuctionCheckpointError> for CallAuctionRiskCheckpointError {
    fn from(error: CallAuctionCheckpointError) -> Self {
        Self::new(error.detail())
    }
}

impl From<RiskError> for CallAuctionRiskCheckpointError {
    fn from(error: RiskError) -> Self {
        Self::new(error.to_string())
    }
}

/// One call-auction engine atomically coupled to deterministic pre-trade risk.
#[derive(Debug)]
pub struct CallAuctionRiskManagedEngine {
    engine: CallAuctionEngine,
    risk: CallAuctionRiskEngine,
    limits: CallAuctionRiskLimits,
}

impl CallAuctionRiskManagedEngine {
    /// Creates an empty coupled shard under default finite limits.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionRiskConstructionError`] when any complete
    /// constructor reservation or auction-engine construction fails.
    pub fn try_new(
        definition: InstrumentDefinition,
    ) -> Result<Self, CallAuctionRiskConstructionError> {
        Self::try_with_limits(definition, CallAuctionRiskLimits::default())
    }

    /// Creates an empty coupled shard under explicit finite limits.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionRiskConstructionError`] before state exists.
    pub fn try_with_limits(
        definition: InstrumentDefinition,
        limits: CallAuctionRiskLimits,
    ) -> Result<Self, CallAuctionRiskConstructionError> {
        let risk = CallAuctionRiskEngine::try_with_limits(definition, limits)?;
        let engine = CallAuctionEngine::try_with_limits(definition, limits.auction())?;
        Ok(Self {
            engine,
            risk,
            limits,
        })
    }

    /// Registers one account before the first auction command is sequenced.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError::ProfileRegistryLocked`] after sequencing, or the
    /// underlying duplicate/capacity error before mutation.
    pub fn register_account(
        &mut self,
        account_id: AccountId,
        profile: RiskProfile,
    ) -> Result<(), RiskError> {
        if self.engine.next_command_sequence() != 1 {
            return Err(RiskError::ProfileRegistryLocked);
        }
        self.risk.register_account(account_id, profile)
    }

    /// Prepares one core auction command without semantic state mutation.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] for core operational failure.
    pub fn prepare(
        &mut self,
        command: CallAuctionCommand,
    ) -> Result<CallAuctionCommandPreparation, CallAuctionEngineError> {
        let preparation = self.engine.prepare(command)?;
        if matches!(preparation, CallAuctionCommandPreparation::Ready(_)) {
            debug_assert_eq!(
                self.engine.book().active_order_count(),
                self.risk.reservation_count()
            );
        }
        Ok(preparation)
    }

    /// Commits one prepared command after core-first risk authorization.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] for stale/foreign/colliding prepared
    /// state or a core operational invariant failure.
    pub fn commit(
        &mut self,
        prepared: PreparedCallAuctionCommand,
    ) -> Result<CallAuctionExecutionReport, CallAuctionEngineError> {
        let command = prepared.command();
        let core_rejection = prepared.core_rejection();
        let risk_rejection = if core_rejection.is_none() {
            self.risk.authorize(command).err()
        } else {
            None
        };
        let apply_risk = core_rejection.is_none() && risk_rejection.is_none();
        let report = self.engine.commit_with_gate(prepared, risk_rejection)?;
        if apply_risk && !report.replayed {
            self.risk.apply(command, &report);
        }
        debug_assert!(self.validate().is_ok());
        Ok(report)
    }

    /// Applies one command with exact retry suppression and sequenced risk rejection.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] for unsequenced operational failure.
    pub fn submit(
        &mut self,
        command: CallAuctionCommand,
    ) -> Result<CallAuctionExecutionReport, CallAuctionEngineError> {
        match self.prepare(command)? {
            CallAuctionCommandPreparation::Replay(report) => Ok(report),
            CallAuctionCommandPreparation::Ready(prepared) => self.commit(prepared),
        }
    }

    /// Captures and independently audits coupled auction and risk state.
    ///
    /// The WAL grammar represented by the boundaries is one instrument
    /// definition at `wal_first_sequence`, one immutable profile per account,
    /// then strict auction command/report pairs.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionRiskCheckpointError`] for invalid live state,
    /// inconsistent physical boundaries, direct reconstruction divergence, or
    /// coupled deterministic replay divergence.
    pub fn checkpoint(
        &self,
        wal_first_sequence: u64,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<CallAuctionRiskCheckpoint, CallAuctionRiskCheckpointError> {
        self.validate()
            .map_err(|error| CallAuctionRiskCheckpointError::new(error.detail()))?;
        let auction = self
            .engine
            .checkpoint(wal_metadata_sequence, wal_sequence)?;
        let accounts = checkpoint_accounts(&self.risk);
        let checkpoint =
            CallAuctionRiskCheckpoint::from_parts(wal_first_sequence, auction, accounts)?;
        let restored = checkpoint.restore_direct_with_limits(self.limits)?;
        if checkpoint_accounts(&restored.risk) != checkpoint.accounts
            || restored
                .engine
                .checkpoint(wal_metadata_sequence, wal_sequence)?
                != checkpoint.auction
        {
            return Err(CallAuctionRiskCheckpointError::new(
                "auction risk checkpoint direct state differs from live coupled state",
            ));
        }
        Ok(checkpoint)
    }

    /// Restores directly indexed coupled state under default finite limits.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionRiskCheckpointError`] for invalid semantic state,
    /// capacity exhaustion, or replay divergence.
    pub fn from_checkpoint(
        checkpoint: &CallAuctionRiskCheckpoint,
    ) -> Result<Self, CallAuctionRiskCheckpointError> {
        checkpoint.restore_direct()
    }

    /// Restores directly indexed coupled state under explicit current limits.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionRiskCheckpointError`] when recovered state exceeds
    /// the selected capacity or fails semantic reconstruction and replay.
    pub fn from_checkpoint_with_limits(
        checkpoint: &CallAuctionRiskCheckpoint,
        limits: CallAuctionRiskLimits,
    ) -> Result<Self, CallAuctionRiskCheckpointError> {
        checkpoint.restore_direct_with_limits(limits)
    }

    /// Returns the read-only authoritative auction engine.
    #[must_use]
    pub const fn engine(&self) -> &CallAuctionEngine {
        &self.engine
    }

    /// Returns read-only risk state.
    #[must_use]
    pub const fn risk(&self) -> &CallAuctionRiskEngine {
        &self.risk
    }

    /// Returns complete coupled resource limits.
    #[must_use]
    pub const fn limits(&self) -> CallAuctionRiskLimits {
        self.limits
    }

    /// Cross-audits engine structure, reservations, positions, and aggregates.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionRiskInvariantViolation`] at the first contradiction.
    pub fn validate(&self) -> Result<(), CallAuctionRiskInvariantViolation> {
        self.engine
            .validate()
            .map_err(|error| CallAuctionRiskInvariantViolation::new(error.detail()))?;
        self.risk.validate()?;
        if self.engine.book().active_order_count() != self.risk.reservations.len() {
            return Err(CallAuctionRiskInvariantViolation::new(
                "auction active-order count differs from risk reservation count",
            ));
        }
        for (&order_id, reservation) in &self.risk.reservations {
            let order = self.engine.book().order(order_id).ok_or_else(|| {
                CallAuctionRiskInvariantViolation::new(format!(
                    "auction reservation {order_id} has no active order"
                ))
            })?;
            if order.account_id != reservation.account_id
                || order.side != reservation.side
                || order.constraint != reservation.constraint
                || order.quantity.lots() != reservation.quantity_lots
            {
                return Err(CallAuctionRiskInvariantViolation::new(format!(
                    "auction reservation {order_id} differs from active order"
                )));
            }
        }
        Ok(())
    }
}

fn checkpoint_accounts(risk: &CallAuctionRiskEngine) -> Vec<RiskAccountCheckpoint> {
    let mut accounts: Vec<_> = risk
        .accounts
        .iter()
        .map(|(&account_id, account)| {
            RiskAccountCheckpoint::from_parts(account_id, account.profile, account.exposure)
        })
        .collect();
    accounts.sort_unstable_by_key(|account| account.account_id());
    accounts
}

fn checkpoint_validation_limits(
    checkpoint: &CallAuctionRiskCheckpoint,
) -> Result<CallAuctionRiskLimits, CallAuctionRiskCheckpointError> {
    let accepted = checkpoint.auction.accepted_order_ids().len();
    let active = checkpoint.auction.active_orders().len();
    let submitted = checkpoint
        .auction
        .history()
        .iter()
        .filter(|entry| matches!(entry.command(), CallAuctionCommand::Submit(_)))
        .count();
    let order_bound = accepted.max(active).max(submitted).max(1);
    let book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: order_bound,
        max_price_levels_per_side: order_bound,
        max_accepted_order_ids: order_bound,
    })
    .map_err(|error| {
        CallAuctionRiskCheckpointError::new(format!(
            "auction risk checkpoint validation book limits are invalid: {error}"
        ))
    })?;
    let terminal_command_reserve = order_bound.checked_add(2).ok_or_else(|| {
        CallAuctionRiskCheckpointError::new(
            "auction risk checkpoint terminal command bound overflows",
        )
    })?;
    let max_retained_commands = checkpoint
        .auction
        .command_count()
        .checked_add(terminal_command_reserve)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| {
            CallAuctionRiskCheckpointError::new(
                "auction risk checkpoint history capacity overflows",
            )
        })?;
    let max_report_events = order_bound
        .checked_mul(2)
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| {
            CallAuctionRiskCheckpointError::new("auction risk checkpoint event capacity overflows")
        })?;
    let auction = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands,
        terminal_command_reserve,
        max_report_events,
    })
    .map_err(|error| {
        CallAuctionRiskCheckpointError::new(format!(
            "auction risk checkpoint validation engine limits are invalid: {error}"
        ))
    })?;
    CallAuctionRiskLimits::new(CallAuctionRiskLimitsSpec {
        auction,
        max_registered_accounts: checkpoint.accounts.len().max(1),
    })
    .map_err(|error| {
        CallAuctionRiskCheckpointError::new(format!(
            "auction risk checkpoint validation risk limits are invalid: {error}"
        ))
    })
}

const fn risk_constraint(constraint: AuctionOrderConstraint) -> RiskPriceConstraint {
    match constraint {
        AuctionOrderConstraint::Market => RiskPriceConstraint::Market,
        AuctionOrderConstraint::Limit(price) => RiskPriceConstraint::Limit(price),
    }
}

const fn call_auction_risk_rejection(reason: RiskRejectReason) -> CallAuctionRejectReason {
    match reason {
        RiskRejectReason::AccountBlocked => CallAuctionRejectReason::RiskAccountBlocked,
        RiskRejectReason::ReduceOnly => CallAuctionRejectReason::RiskReduceOnly,
        RiskRejectReason::OrderQuantityLimit => CallAuctionRejectReason::RiskOrderQuantityLimit,
        RiskRejectReason::OrderNotionalLimit => CallAuctionRejectReason::RiskOrderNotionalLimit,
        RiskRejectReason::OpenOrderCountLimit => CallAuctionRejectReason::RiskOpenOrderCountLimit,
        RiskRejectReason::OpenQuantityLimit => CallAuctionRejectReason::RiskOpenQuantityLimit,
        RiskRejectReason::OpenNotionalLimit => CallAuctionRejectReason::RiskOpenNotionalLimit,
        RiskRejectReason::PositionLimit => CallAuctionRejectReason::RiskPositionLimit,
        RiskRejectReason::ArithmeticOverflow => CallAuctionRejectReason::RiskArithmeticOverflow,
    }
}
