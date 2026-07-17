//! Deterministic pre-trade risk and conservative reservations for call auctions.
//!
//! The coupled engine sequences core business failures before risk, converts
//! risk failures into ordinary idempotent auction reports, reserves every
//! accepted market or limit order at its maximum reachable absolute collar
//! price, and applies all uncross position deltas only after netting per account.
//! Profile, reservation, and uncross-netting indexes own fixed-capacity dense
//! storage before the coupled engine exists. Reservations additionally maintain
//! allocation-free intrusive per-account membership for exact aggregate audit.

use std::fmt;
use std::sync::Arc;

use crate::auction::AuctionOrderConstraint;
use crate::auction_book::{
    CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrder, CallAuctionOrderSnapshot,
};
use crate::auction_engine::{
    CallAuctionAmendObservation, CallAuctionAmendOrder, CallAuctionCancelObservation,
    CallAuctionCancelOrder, CallAuctionCheckpoint, CallAuctionCheckpointError, CallAuctionCommand,
    CallAuctionCommandOutcome, CallAuctionCommandPreparation, CallAuctionEngine,
    CallAuctionEngineConstructionError, CallAuctionEngineError, CallAuctionEngineLimits,
    CallAuctionEngineLimitsSpec, CallAuctionEventKind, CallAuctionExecutionReport,
    CallAuctionMassCancel, CallAuctionMassCancelObservation, CallAuctionRejectReason,
    CallAuctionReplaceObservation, CallAuctionReplaceOrder, CallAuctionUncrossCommand,
    CallAuctionUncrossObservation, ConditionalCallAuctionCommandOutcome,
    ConditionalCallAuctionCommandPreparation, ConditionalCallAuctionOutcome,
    ConditionalCallAuctionPreparation, PreparedCallAuctionCommand, evaluate_conditional_amend,
    evaluate_conditional_cancel, evaluate_conditional_mass_cancel, evaluate_conditional_replace,
    evaluate_conditional_uncross,
};
use crate::bounded_hash::BoundedHashMap;
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
    reservation_head: Option<OrderId>,
    reservation_tail: Option<OrderId>,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ActiveCallAuctionReservation {
    snapshot: CallAuctionReservationSnapshot,
    previous: Option<OrderId>,
    next: Option<OrderId>,
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
    accounts: BoundedHashMap<AccountId, CallAuctionRiskAccount>,
    reservations: BoundedHashMap<OrderId, ActiveCallAuctionReservation>,
    position_scratch: BoundedHashMap<AccountId, ExecutedLots>,
    maximum_accounts: usize,
    maximum_reservations: usize,
}

impl CallAuctionRiskEngine {
    fn try_with_limits(
        definition: InstrumentDefinition,
        limits: CallAuctionRiskLimits,
    ) -> Result<Self, CallAuctionRiskConstructionError> {
        let accounts = BoundedHashMap::try_new(limits.max_registered_accounts())
            .map_err(|_| CallAuctionRiskConstructionError::AccountReservationFailed)?;
        let maximum_reservations = limits.auction().book().max_active_orders();
        let reservations = BoundedHashMap::try_new(maximum_reservations)
            .map_err(|_| CallAuctionRiskConstructionError::OrderReservationFailed)?;
        let position_scratch = BoundedHashMap::try_new(limits.max_registered_accounts())
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
                        reservation_head: None,
                        reservation_tail: None,
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
        self.reservations
            .get(&order_id)
            .map(|reservation| reservation.snapshot)
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
        match command {
            CallAuctionCommand::Submit(submit) => {
                let account = self
                    .accounts
                    .get(&submit.order.account_id())
                    .ok_or(CallAuctionRejectReason::RiskProfileMissing)?;
                self.authorize_order(submit.order, *account)
            }
            CallAuctionCommand::Replace(replace) => {
                let account = self
                    .accounts
                    .get(&replace.account_id)
                    .ok_or(CallAuctionRejectReason::RiskProfileMissing)?;
                let target = self
                    .reservations
                    .get(&replace.target_order_id)
                    .expect("core-approved replacement target must have a risk reservation")
                    .snapshot;
                assert_eq!(target.account_id, replace.account_id);
                let baseline = exposure_without_reservation(account.exposure, target)
                    .expect("risk exposure must contain its replacement target");
                self.authorize_order_against(replace.replacement, account.profile, baseline)
            }
            CallAuctionCommand::PhaseControl(_)
            | CallAuctionCommand::Cancel(_)
            | CallAuctionCommand::MassCancel(_)
            | CallAuctionCommand::Amend(_)
            | CallAuctionCommand::Indicative(_)
            | CallAuctionCommand::Uncross(_) => Ok(()),
        }
    }

    fn authorize_order(
        &self,
        order: CallAuctionOrder,
        account: CallAuctionRiskAccount,
    ) -> Result<(), CallAuctionRejectReason> {
        self.authorize_order_against(order, account.profile, account.exposure)
    }

    fn authorize_order_against(
        &self,
        order: CallAuctionOrder,
        profile: RiskProfile,
        baseline: RiskSnapshot,
    ) -> Result<(), CallAuctionRejectReason> {
        let account = self
            .accounts
            .get(&order.account_id())
            .ok_or(CallAuctionRejectReason::RiskProfileMissing)?;
        debug_assert_eq!(account.profile, profile);
        let constraint = risk_constraint(order.constraint());
        let notional = conservative_order_notional(
            self.definition,
            order.side(),
            constraint,
            order.quantity().lots(),
        )
        .map_err(call_auction_risk_rejection)?;
        evaluate_pretrade_order(
            profile,
            baseline,
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
                CallAuctionEventKind::OrderAmended {
                    order,
                    previous_quantity,
                    ..
                } => {
                    let delta_lots = previous_quantity
                        .lots()
                        .checked_sub(order.quantity.lots())
                        .expect("accepted amendment must strictly reduce quantity");
                    self.decrement_reservation(order.order_id, delta_lots);
                }
                CallAuctionEventKind::Trade(trade) => {
                    debug_assert_eq!(trade.instrument_id(), self.definition.instrument_id());
                    debug_assert_eq!(trade.instrument_version(), self.definition.version());
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
                | CallAuctionEventKind::MassCancelCompleted { .. }
                | CallAuctionEventKind::UncrossCompleted { .. }
                | CallAuctionEventKind::IndicativePublished(_)
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
        self.append_reservation(order.order_id, reservation);
    }

    fn append_reservation(
        &mut self,
        order_id: OrderId,
        reservation: CallAuctionReservationSnapshot,
    ) {
        let capacity = self.reservations.capacity();
        assert!(self.reservations.len() < capacity);
        assert!(!self.reservations.contains_key(&order_id));
        let previous = self
            .accounts
            .get(&reservation.account_id)
            .expect("authorized auction account must have a risk profile")
            .reservation_tail;
        if let Some(previous_id) = previous {
            let tail = self
                .reservations
                .get_mut(&previous_id)
                .expect("auction risk account tail must reference a reservation");
            assert!(tail.next.is_none());
            tail.next = Some(order_id);
        }
        assert!(
            self.reservations
                .insert(
                    order_id,
                    ActiveCallAuctionReservation {
                        snapshot: reservation,
                        previous,
                        next: None,
                    },
                )
                .is_none()
        );
        let account = self
            .accounts
            .get_mut(&reservation.account_id)
            .expect("authorized auction account must have a risk profile");
        if account.reservation_head.is_none() {
            assert!(previous.is_none());
            account.reservation_head = Some(order_id);
        }
        account.reservation_tail = Some(order_id);
        debug_assert_eq!(self.reservations.capacity(), capacity);
        self.add_open_exposure(reservation);
    }

    fn decrement_reservation(&mut self, order_id: OrderId, quantity_lots: u64) {
        let current = self
            .reservations
            .get(&order_id)
            .map(|reservation| reservation.snapshot)
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
        self.append_reservation(order_id, replacement);
        debug_assert_eq!(self.reservations.capacity(), capacity);
    }

    fn remove_reservation(&mut self, order_id: OrderId) -> CallAuctionReservationSnapshot {
        let active = self
            .reservations
            .remove(&order_id)
            .expect("active auction order must have a risk reservation");
        if let Some(previous_id) = active.previous {
            let previous = self
                .reservations
                .get_mut(&previous_id)
                .expect("auction risk reservation previous link must resolve");
            assert_eq!(previous.next, Some(order_id));
            previous.next = active.next;
        }
        if let Some(next_id) = active.next {
            let next = self
                .reservations
                .get_mut(&next_id)
                .expect("auction risk reservation next link must resolve");
            assert_eq!(next.previous, Some(order_id));
            next.previous = active.previous;
        }
        let reservation = active.snapshot;
        let account = self
            .accounts
            .get_mut(&reservation.account_id)
            .expect("reserved auction account must have a risk profile");
        if active.previous.is_none() {
            assert_eq!(account.reservation_head, Some(order_id));
            account.reservation_head = active.next;
        }
        if active.next.is_none() {
            assert_eq!(account.reservation_tail, Some(order_id));
            account.reservation_tail = active.previous;
        }
        assert_eq!(
            account.reservation_head.is_none(),
            account.reservation_tail.is_none()
        );
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
        if !self.position_scratch.contains_key(&account_id) {
            assert!(
                self.position_scratch
                    .insert(account_id, ExecutedLots::default())
                    .is_none(),
                "new auction position accumulator must be absent"
            );
        }
        let delta = self
            .position_scratch
            .get_mut(&account_id)
            .expect("auction position accumulator must exist after insertion");
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
        for (&account_id, delta) in self.position_scratch.iter() {
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
        self.validate_reservation_economics()?;
        self.validate_account_reservation_index()
    }

    fn validate_reservation_economics(&self) -> Result<(), CallAuctionRiskInvariantViolation> {
        for (&order_id, active) in self.reservations.iter() {
            let reservation = active.snapshot;
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
        }
        Ok(())
    }

    fn validate_account_reservation_index(&self) -> Result<(), CallAuctionRiskInvariantViolation> {
        let mut indexed_reservations = 0_usize;
        for (&account_id, account) in self.accounts.iter() {
            self.validate_account_reservations(account_id, account, &mut indexed_reservations)?;
        }
        if indexed_reservations != self.reservations.len() {
            return Err(CallAuctionRiskInvariantViolation::new(
                "active auction reservation is absent from its account index",
            ));
        }
        Ok(())
    }

    fn validate_account_reservations(
        &self,
        account_id: AccountId,
        account: &CallAuctionRiskAccount,
        indexed_reservations: &mut usize,
    ) -> Result<(), CallAuctionRiskInvariantViolation> {
        let mut current = account.reservation_head;
        let mut previous = None;
        let mut open_buy_lots = 0_u128;
        let mut open_sell_lots = 0_u128;
        let mut open_notional = 0_u128;
        let mut open_orders = 0_u64;
        while let Some(order_id) = current {
            if *indexed_reservations >= self.reservations.len() {
                return Err(CallAuctionRiskInvariantViolation::new(
                    "auction reservation occurs more than once or participates in an account-index cycle",
                ));
            }
            let active = self.reservations.get(&order_id).ok_or_else(|| {
                CallAuctionRiskInvariantViolation::new(
                    "auction account index references an absent reservation",
                )
            })?;
            let reservation = active.snapshot;
            if reservation.account_id != account_id || active.previous != previous {
                return Err(CallAuctionRiskInvariantViolation::new(
                    "auction reservation account membership or previous link is inconsistent",
                ));
            }
            match reservation.side {
                Side::Buy => {
                    open_buy_lots = open_buy_lots
                        .checked_add(u128::from(reservation.quantity_lots))
                        .ok_or_else(|| {
                            CallAuctionRiskInvariantViolation::new(
                                "auction buy reservation aggregate overflows",
                            )
                        })?;
                }
                Side::Sell => {
                    open_sell_lots = open_sell_lots
                        .checked_add(u128::from(reservation.quantity_lots))
                        .ok_or_else(|| {
                            CallAuctionRiskInvariantViolation::new(
                                "auction sell reservation aggregate overflows",
                            )
                        })?;
                }
            }
            open_notional = open_notional
                .checked_add(reservation.notional)
                .ok_or_else(|| {
                    CallAuctionRiskInvariantViolation::new(
                        "auction reservation notional aggregate overflows",
                    )
                })?;
            open_orders = open_orders.checked_add(1).ok_or_else(|| {
                CallAuctionRiskInvariantViolation::new(
                    "auction reservation count aggregate overflows",
                )
            })?;
            *indexed_reservations = indexed_reservations.checked_add(1).ok_or_else(|| {
                CallAuctionRiskInvariantViolation::new(
                    "auction indexed-reservation count overflows",
                )
            })?;
            previous = Some(order_id);
            current = active.next;
        }
        if previous != account.reservation_tail {
            return Err(CallAuctionRiskInvariantViolation::new(
                "auction risk account reservation tail is inconsistent",
            ));
        }
        let exposure = account.exposure;
        if (
            exposure.open_buy_lots(),
            exposure.open_sell_lots(),
            exposure.open_notional(),
            exposure.open_orders(),
        ) != (open_buy_lots, open_sell_lots, open_notional, open_orders)
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
        Ok(())
    }

    fn validate_resource_bounds(&self) -> Result<(), CallAuctionRiskInvariantViolation> {
        for (name, layout) in [
            ("account", self.accounts.validate_layout()),
            ("reservation", self.reservations.validate_layout()),
            ("position-scratch", self.position_scratch.validate_layout()),
        ] {
            if let Err(detail) = layout {
                return Err(CallAuctionRiskInvariantViolation::new(format!(
                    "auction risk {name} hash layout is invalid: {detail}"
                )));
            }
        }
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

fn exposure_without_reservation(
    exposure: RiskSnapshot,
    reservation: CallAuctionReservationSnapshot,
) -> Option<RiskSnapshot> {
    let quantity = u128::from(reservation.quantity_lots);
    let (open_buy_lots, open_sell_lots) = match reservation.side {
        Side::Buy => (
            exposure.open_buy_lots().checked_sub(quantity)?,
            exposure.open_sell_lots(),
        ),
        Side::Sell => (
            exposure.open_buy_lots(),
            exposure.open_sell_lots().checked_sub(quantity)?,
        ),
    };
    Some(RiskSnapshot::from_parts(
        exposure.position_lots(),
        open_buy_lots,
        open_sell_lots,
        exposure.open_notional().checked_sub(reservation.notional)?,
        exposure.open_orders().checked_sub(1)?,
    ))
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
///
/// The canonical account image is immutable shared storage. The embedded
/// auction image is shared independently, so cloning the complete coupled
/// checkpoint is `O(1)` and copies no semantic rows or event values.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionRiskCheckpoint {
    wal_first_sequence: u64,
    auction: CallAuctionCheckpoint,
    accounts: Arc<Vec<RiskAccountCheckpoint>>,
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
        self.accounts.as_slice()
    }

    /// Returns whether two checkpoints share the identical immutable account image.
    #[must_use]
    pub fn shares_account_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.accounts, &other.accounts)
    }

    pub(crate) fn from_parts(
        wal_first_sequence: u64,
        auction: CallAuctionCheckpoint,
        accounts: Vec<RiskAccountCheckpoint>,
    ) -> Result<Self, CallAuctionRiskCheckpointError> {
        let checkpoint = Self::from_captured_parts(wal_first_sequence, auction, accounts)?;
        let limits = checkpoint_validation_limits(&checkpoint)?;
        checkpoint.verify_replay_with_limits(limits)?;
        Ok(checkpoint)
    }

    fn from_captured_parts(
        wal_first_sequence: u64,
        auction: CallAuctionCheckpoint,
        accounts: Vec<RiskAccountCheckpoint>,
    ) -> Result<Self, CallAuctionRiskCheckpointError> {
        let checkpoint = Self {
            wal_first_sequence,
            auction,
            accounts: Arc::new(accounts),
        };
        checkpoint.validate_structure()?;
        Ok(checkpoint)
    }

    fn validate_structure(&self) -> Result<(), CallAuctionRiskCheckpointError> {
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
        Ok(())
    }

    fn verify_replay_with_limits(
        &self,
        limits: CallAuctionRiskLimits,
    ) -> Result<(), CallAuctionRiskCheckpointError> {
        self.validate_structure()?;
        let direct = self.restore_direct_with_limits(limits)?;
        let mut replay =
            CallAuctionRiskManagedEngine::try_with_limits(self.auction.definition(), limits)
                .map_err(CallAuctionRiskCheckpointError::Construction)?;
        for account in self.accounts.iter() {
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
            .checkpoint_state(
                self.auction.wal_metadata_sequence(),
                self.auction.generation(),
            )
            .map_err(CallAuctionRiskCheckpointError::from)?;
        let direct_auction = direct.engine.checkpoint_state(
            self.auction.wal_metadata_sequence(),
            self.auction.generation(),
        )?;
        if replayed_auction != self.auction
            || direct_auction != self.auction
            || checkpoint_accounts(&replay.risk)?.as_slice() != self.accounts.as_slice()
            || checkpoint_accounts(&direct.risk)?.as_slice() != self.accounts.as_slice()
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
            CallAuctionEngine::from_checkpoint_with_limits(&self.auction, limits.auction())?;
        let mut risk = CallAuctionRiskEngine::try_with_limits(self.auction.definition(), limits)
            .map_err(CallAuctionRiskCheckpointError::Construction)?;
        for account in self.accounts.iter() {
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
        for account in self.accounts.iter() {
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
                .zip(previous.accounts.iter())
                .all(|(current, old)| {
                    current.account_id() == old.account_id() && current.profile() == old.profile()
                })
            && self.auction.is_successor_of(&previous.auction)
    }
}

/// An immutable but not yet coupled-replay-verified auction-risk capture.
///
/// Capture audits live auction/risk structure, profiles, positions, exposures,
/// and reservations and proves direct reconstruction equality, but deliberately
/// defers command execution. This type has no codec or snapshot implementation;
/// clones share every immutable auction/account row image in `O(1)`.
#[derive(Clone)]
pub struct CallAuctionRiskCheckpointCapture {
    checkpoint: CallAuctionRiskCheckpoint,
    limits: CallAuctionRiskLimits,
}

impl CallAuctionRiskCheckpointCapture {
    /// Returns the first physical WAL sequence occupied by the definition.
    #[must_use]
    pub const fn wal_first_sequence(&self) -> u64 {
        self.checkpoint.wal_first_sequence()
    }

    /// Returns the final immutable definition/profile metadata sequence.
    #[must_use]
    pub const fn wal_metadata_sequence(&self) -> u64 {
        self.checkpoint.auction().wal_metadata_sequence()
    }

    /// Returns the completed report boundary represented here.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.checkpoint.generation()
    }

    /// Returns the finite coupled resource policy used during verification.
    #[must_use]
    pub const fn limits(&self) -> CallAuctionRiskLimits {
        self.limits
    }

    /// Returns the retained command/report cardinality.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.checkpoint.auction().command_count()
    }

    /// Returns the active order/reservation cardinality.
    #[must_use]
    pub fn active_order_count(&self) -> usize {
        self.checkpoint.auction().active_orders().len()
    }

    /// Returns the canonical immutable account/profile cardinality.
    #[must_use]
    pub fn account_count(&self) -> usize {
        self.checkpoint.accounts().len()
    }

    /// Returns whether two captures share every immutable checkpoint row image.
    #[must_use]
    pub fn shares_checkpoint_storage_with(&self, other: &Self) -> bool {
        self.checkpoint
            .shares_account_storage_with(&other.checkpoint)
            && self
                .checkpoint
                .auction()
                .shares_accepted_order_storage_with(other.checkpoint.auction())
            && self
                .checkpoint
                .auction()
                .shares_active_order_storage_with(other.checkpoint.auction())
            && self
                .checkpoint
                .auction()
                .shares_history_storage_with(other.checkpoint.auction())
    }

    /// Consumes this capture and proves deterministic coupled auction/risk replay.
    ///
    /// Verification reconstructs direct state, registers the captured immutable
    /// profiles in an isolated shard, requires exact command/report reproduction,
    /// and compares canonical auction and account projections. It may run while
    /// the source writer advances through later command/report pairs.
    ///
    /// # Errors
    ///
    /// Returns a typed nested auction/resource/construction/profile failure or
    /// `Invalid` for any replay, position, exposure, reservation, or projection
    /// divergence.
    pub fn verify(self) -> Result<CallAuctionRiskCheckpoint, CallAuctionRiskCheckpointError> {
        let Self { checkpoint, limits } = self;
        checkpoint.verify_replay_with_limits(limits)?;
        Ok(checkpoint)
    }
}

#[cfg(test)]
impl CallAuctionRiskCheckpointCapture {
    pub(crate) fn corrupt_wal_lineage_for_test(&mut self) {
        self.checkpoint.wal_first_sequence = 0;
    }
}

/// One fallibly reserved coupled call-auction/risk checkpoint resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionRiskCheckpointResource {
    /// Canonical account/profile/exposure rows copied from live risk state.
    CaptureAccounts,
}

impl fmt::Display for CallAuctionRiskCheckpointResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CaptureAccounts => formatter.write_str("capture accounts"),
        }
    }
}

/// Semantic coupled auction/risk checkpoint construction or restoration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CallAuctionRiskCheckpointError {
    /// Checkpoint content or coupled-state contradiction.
    Invalid(String),
    /// Direct call-auction checkpoint capture or restoration failed.
    Auction(CallAuctionCheckpointError),
    /// A temporary coupled risk engine could not be constructed.
    Construction(CallAuctionRiskConstructionError),
    /// Immutable profile registration or reconstruction failed.
    Risk(RiskError),
    /// A coupled checkpoint capture resource could not be reserved.
    ResourceReservationFailed {
        /// Resource whose construction failed.
        resource: CallAuctionRiskCheckpointResource,
        /// Requested semantic maximum entries.
        maximum: usize,
    },
}

impl CallAuctionRiskCheckpointError {
    fn new(detail: impl Into<String>) -> Self {
        Self::Invalid(detail.into())
    }

    /// Returns a stable diagnostic description.
    #[must_use]
    pub fn detail(&self) -> &str {
        match self {
            Self::Invalid(detail) => detail,
            Self::Auction(error) => error.detail(),
            Self::Construction(_) => "auction risk checkpoint construction failed",
            Self::Risk(_) => "auction risk checkpoint profile reconstruction failed",
            Self::ResourceReservationFailed { .. } => {
                "auction risk checkpoint account capture reservation failed"
            }
        }
    }

    /// Returns the failed coupled capture resource.
    #[must_use]
    pub const fn resource(&self) -> Option<CallAuctionRiskCheckpointResource> {
        match self {
            Self::ResourceReservationFailed { resource, .. } => Some(*resource),
            Self::Invalid(_) | Self::Auction(_) | Self::Construction(_) | Self::Risk(_) => None,
        }
    }

    /// Returns the preserved direct auction-checkpoint failure.
    #[must_use]
    pub const fn auction_error(&self) -> Option<&CallAuctionCheckpointError> {
        match self {
            Self::Auction(error) => Some(error),
            Self::Invalid(_)
            | Self::Construction(_)
            | Self::Risk(_)
            | Self::ResourceReservationFailed { .. } => None,
        }
    }

    /// Returns the preserved coupled-engine construction failure.
    #[must_use]
    pub const fn construction_error(&self) -> Option<&CallAuctionRiskConstructionError> {
        match self {
            Self::Construction(error) => Some(error),
            Self::Invalid(_)
            | Self::Auction(_)
            | Self::Risk(_)
            | Self::ResourceReservationFailed { .. } => None,
        }
    }

    /// Returns the preserved profile registration or reconstruction failure.
    #[must_use]
    pub const fn risk_error(&self) -> Option<&RiskError> {
        match self {
            Self::Risk(error) => Some(error),
            Self::Invalid(_)
            | Self::Auction(_)
            | Self::Construction(_)
            | Self::ResourceReservationFailed { .. } => None,
        }
    }

    /// Returns whether this error or its direct-auction cause is resource exhaustion.
    #[must_use]
    pub const fn is_resource_exhaustion(&self) -> bool {
        match self {
            Self::Auction(error) => error.is_resource_exhaustion(),
            Self::ResourceReservationFailed { .. } => true,
            Self::Invalid(_) | Self::Construction(_) | Self::Risk(_) => false,
        }
    }

    /// Returns whether retry under different resource availability can succeed.
    #[must_use]
    pub const fn is_operational_failure(&self) -> bool {
        match self {
            Self::Auction(error) => error.is_operational_failure(),
            Self::Construction(_) | Self::ResourceReservationFailed { .. } => true,
            Self::Invalid(_) | Self::Risk(_) => false,
        }
    }
}

impl fmt::Display for CallAuctionRiskCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(detail) => detail.fmt(formatter),
            Self::Auction(error) => error.fmt(formatter),
            Self::Construction(error) => {
                write!(
                    formatter,
                    "failed to construct auction risk checkpoint shard: {error}"
                )
            }
            Self::Risk(error) => error.fmt(formatter),
            Self::ResourceReservationFailed { resource, maximum } => write!(
                formatter,
                "failed to reserve auction risk checkpoint {resource} through {maximum} entries"
            ),
        }
    }
}

impl std::error::Error for CallAuctionRiskCheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Auction(error) => Some(error),
            Self::Construction(error) => Some(error),
            Self::Risk(error) => Some(error),
            Self::Invalid(_) | Self::ResourceReservationFailed { .. } => None,
        }
    }
}

impl From<CallAuctionCheckpointError> for CallAuctionRiskCheckpointError {
    fn from(error: CallAuctionCheckpointError) -> Self {
        Self::Auction(error)
    }
}

impl From<RiskError> for CallAuctionRiskCheckpointError {
    fn from(error: RiskError) -> Self {
        Self::Risk(error)
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

    pub(crate) fn validate_conditional_cancel_authorization(
        &self,
        preparation: &CallAuctionCommandPreparation,
    ) -> Result<(), CallAuctionEngineError> {
        self.validate_conditional_authorization(preparation, |command| {
            matches!(command, CallAuctionCommand::Cancel(_))
        })
    }

    pub(crate) fn validate_conditional_mass_cancel_authorization(
        &self,
        preparation: &CallAuctionCommandPreparation,
    ) -> Result<(), CallAuctionEngineError> {
        self.validate_conditional_authorization(preparation, |command| {
            matches!(command, CallAuctionCommand::MassCancel(_))
        })
    }

    pub(crate) fn validate_conditional_amend_authorization(
        &self,
        preparation: &CallAuctionCommandPreparation,
    ) -> Result<(), CallAuctionEngineError> {
        self.validate_conditional_authorization(preparation, |command| {
            matches!(command, CallAuctionCommand::Amend(_))
        })
    }

    pub(crate) fn conditional_replace_observation_is_authorized(
        &self,
        preparation: &CallAuctionCommandPreparation,
    ) -> Result<bool, CallAuctionEngineError> {
        let CallAuctionCommandPreparation::Ready(prepared) = preparation else {
            return Ok(false);
        };
        if !matches!(prepared.command(), CallAuctionCommand::Replace(_)) {
            return Err(CallAuctionEngineError::InternalInvariantViolation);
        }
        Ok(prepared.core_rejection().is_none() && self.risk.authorize(prepared.command()).is_ok())
    }

    fn validate_conditional_authorization(
        &self,
        preparation: &CallAuctionCommandPreparation,
        is_expected_command: fn(CallAuctionCommand) -> bool,
    ) -> Result<(), CallAuctionEngineError> {
        let CallAuctionCommandPreparation::Ready(prepared) = preparation else {
            return Ok(());
        };
        if !is_expected_command(prepared.command()) {
            return Err(CallAuctionEngineError::InternalInvariantViolation);
        }
        if prepared.core_rejection().is_none() && self.risk.authorize(prepared.command()).is_err() {
            return Err(CallAuctionEngineError::InternalInvariantViolation);
        }
        Ok(())
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

    /// Atomically risk-gates, observes, and conditionally commits one owner cancel.
    ///
    /// Replay plus core rejection bypass `accept`. Decline or unwind changes
    /// neither auction nor risk state. Acceptance commits the exact observed
    /// preparation and releases its coupled reservation once.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] for preparation, fail-closed query,
    /// generation validation, or coupled commit failure.
    pub fn try_submit_cancel_order_if(
        &mut self,
        command: CallAuctionCancelOrder,
        accept: impl FnOnce(&CallAuctionCancelObservation) -> bool,
    ) -> Result<
        ConditionalCallAuctionCommandOutcome<CallAuctionCancelObservation>,
        CallAuctionEngineError,
    > {
        let preparation = self.prepare(CallAuctionCommand::Cancel(command))?;
        self.validate_conditional_cancel_authorization(&preparation)?;
        match evaluate_conditional_cancel(&self.engine, preparation, accept)? {
            ConditionalCallAuctionCommandPreparation::Complete(outcome) => Ok(outcome),
            ConditionalCallAuctionCommandPreparation::Commit {
                prepared,
                observation,
            } => {
                self.commit(prepared)
                    .map(|report| ConditionalCallAuctionCommandOutcome::Reported {
                        observation: if report.replayed { None } else { observation },
                        report,
                    })
            }
        }
    }

    /// Atomically risk-gates, observes, and conditionally commits one account
    /// mass cancellation.
    ///
    /// Replay plus core rejection bypass `accept`. The predicate borrows the
    /// exact canonical selection from constructor-owned auction scratch.
    /// Decline or unwind changes neither auction nor risk state; acceptance
    /// commits the same-generation preparation and releases every selected
    /// coupled reservation once.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] for preparation, fail-closed
    /// selection, generation validation, or coupled commit failure.
    pub fn try_submit_mass_cancel_if(
        &mut self,
        command: CallAuctionMassCancel,
        accept: impl FnOnce(&CallAuctionMassCancelObservation<'_>) -> bool,
    ) -> Result<ConditionalCallAuctionOutcome, CallAuctionEngineError> {
        let preparation = self.prepare(CallAuctionCommand::MassCancel(command))?;
        self.validate_conditional_mass_cancel_authorization(&preparation)?;
        match evaluate_conditional_mass_cancel(&mut self.engine, preparation, accept)? {
            ConditionalCallAuctionPreparation::Complete(outcome) => Ok(outcome),
            ConditionalCallAuctionPreparation::Commit(prepared) => self
                .commit(prepared)
                .map(ConditionalCallAuctionOutcome::Reported),
        }
    }

    /// Atomically risk-gates, observes, and conditionally commits one amendment.
    ///
    /// Replay plus core rejection bypass `accept`. Decline or unwind changes
    /// neither auction nor risk state. Acceptance commits the exact observed
    /// quantity reduction and decreases its coupled reservation once while
    /// retaining order identity and priority.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] for preparation, fail-closed query,
    /// generation validation, or coupled commit failure.
    pub fn try_submit_amend_order_if(
        &mut self,
        command: CallAuctionAmendOrder,
        accept: impl FnOnce(&CallAuctionAmendObservation) -> bool,
    ) -> Result<
        ConditionalCallAuctionCommandOutcome<CallAuctionAmendObservation>,
        CallAuctionEngineError,
    > {
        let preparation = self.prepare(CallAuctionCommand::Amend(command))?;
        self.validate_conditional_amend_authorization(&preparation)?;
        match evaluate_conditional_amend(&self.engine, preparation, accept)? {
            ConditionalCallAuctionCommandPreparation::Complete(outcome) => Ok(outcome),
            ConditionalCallAuctionCommandPreparation::Commit {
                prepared,
                observation,
            } => {
                self.commit(prepared)
                    .map(|report| ConditionalCallAuctionCommandOutcome::Reported {
                        observation: if report.replayed { None } else { observation },
                        report,
                    })
            }
        }
    }

    /// Atomically risk-gates, observes, and conditionally commits one replacement.
    ///
    /// Replay plus core or risk rejection bypass `accept`. Decline or unwind
    /// changes neither auction nor risk state. Acceptance commits the exact
    /// observed target/new-identity transition and applies its net coupled
    /// reservation effect once.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] for preparation, fail-closed query,
    /// generation validation, or coupled commit failure.
    pub fn try_submit_replace_order_if(
        &mut self,
        command: CallAuctionReplaceOrder,
        accept: impl FnOnce(&CallAuctionReplaceObservation) -> bool,
    ) -> Result<
        ConditionalCallAuctionCommandOutcome<CallAuctionReplaceObservation>,
        CallAuctionEngineError,
    > {
        let preparation = self.prepare(CallAuctionCommand::Replace(command))?;
        let observe_authorized =
            self.conditional_replace_observation_is_authorized(&preparation)?;
        match evaluate_conditional_replace(&self.engine, preparation, observe_authorized, accept)? {
            ConditionalCallAuctionCommandPreparation::Complete(outcome) => Ok(outcome),
            ConditionalCallAuctionCommandPreparation::Commit {
                prepared,
                observation,
            } => {
                self.commit(prepared)
                    .map(|report| ConditionalCallAuctionCommandOutcome::Reported {
                        observation: if report.replayed { None } else { observation },
                        report,
                    })
            }
        }
    }

    /// Atomically risk-gates, observes, and conditionally commits one uncross.
    ///
    /// Replay plus core rejection bypass `accept`. A core-admissible uncross
    /// borrows the exact zero-copy allocation, trade pairs, and remainder
    /// cancellations from the preparation. Decline or unwind changes neither
    /// auction nor risk state; acceptance commits that token and applies its
    /// coupled risk trace once.
    ///
    /// # Errors
    ///
    /// Returns [`CallAuctionEngineError`] for preparation, observation
    /// validation, or coupled commit failure.
    pub fn try_submit_uncross_if(
        &mut self,
        command: CallAuctionUncrossCommand,
        accept: impl FnOnce(&CallAuctionUncrossObservation<'_>) -> bool,
    ) -> Result<ConditionalCallAuctionOutcome, CallAuctionEngineError> {
        let preparation = self.prepare(CallAuctionCommand::Uncross(command))?;
        match evaluate_conditional_uncross(&self.engine, preparation, accept)? {
            ConditionalCallAuctionPreparation::Complete(outcome) => Ok(outcome),
            ConditionalCallAuctionPreparation::Commit(prepared) => self
                .commit(prepared)
                .map(ConditionalCallAuctionOutcome::Reported),
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
        self.capture_checkpoint_candidate(wal_first_sequence, wal_metadata_sequence, wal_sequence)?
            .verify()
    }

    /// Captures immutable coupled state without executing retained history.
    ///
    /// The writer-side phase audits live auction/risk invariants, materializes
    /// canonical auction and account images, reconstructs positions/exposures/
    /// reservations directly under the current limits, and requires exact live
    /// equality. The returned value is not encodable or persistable until its
    /// consuming [`CallAuctionRiskCheckpointCapture::verify`] transition.
    ///
    /// # Errors
    ///
    /// Returns a typed nested capture/resource/construction failure or `Invalid`
    /// for live, WAL, profile, position, exposure, reservation, or direct-state
    /// contradiction.
    pub fn capture_checkpoint_candidate(
        &self,
        wal_first_sequence: u64,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<CallAuctionRiskCheckpointCapture, CallAuctionRiskCheckpointError> {
        self.validate()
            .map_err(|error| CallAuctionRiskCheckpointError::new(error.detail()))?;
        let auction = self
            .engine
            .checkpoint_state(wal_metadata_sequence, wal_sequence)?;
        let accounts = checkpoint_accounts(&self.risk)?;
        let checkpoint =
            CallAuctionRiskCheckpoint::from_captured_parts(wal_first_sequence, auction, accounts)?;
        let restored = checkpoint.restore_direct_with_limits(self.limits)?;
        if checkpoint_accounts(&restored.risk)?.as_slice() != checkpoint.accounts.as_slice()
            || restored
                .engine
                .checkpoint_state(wal_metadata_sequence, wal_sequence)?
                != checkpoint.auction
        {
            return Err(CallAuctionRiskCheckpointError::new(
                "auction risk checkpoint direct state differs from live coupled state",
            ));
        }
        Ok(CallAuctionRiskCheckpointCapture {
            checkpoint,
            limits: self.limits,
        })
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

    pub(crate) const fn engine_mut(&mut self) -> &mut CallAuctionEngine {
        &mut self.engine
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
    /// Successful validation performs no heap allocation and uses `O(1)`
    /// auxiliary space. The private risk audit traverses `A` accounts and `O`
    /// reservations in expected `O(A + O)` time through constructor-owned hash
    /// indexes and intrusive account lists. Active-order parity is another
    /// expected `O(O)` pass; the embedded engine/book audits retain their own
    /// A74/A75 bounds. Adversarial full hash collisions remain finite but can
    /// make the risk passes quadratic. Failure-detail construction can allocate.
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
        for order in self.engine.book().active_order_states() {
            let reservation = self
                .risk
                .reservations
                .get(&order.order_id)
                .map(|active| active.snapshot)
                .ok_or_else(|| {
                    CallAuctionRiskInvariantViolation::new(format!(
                        "auction active order {} has no risk reservation",
                        order.order_id
                    ))
                })?;
            if order.account_id != reservation.account_id
                || order.side != reservation.side
                || order.constraint != reservation.constraint
                || order.quantity.lots() != reservation.quantity_lots
            {
                return Err(CallAuctionRiskInvariantViolation::new(format!(
                    "auction reservation {} differs from active order",
                    order.order_id
                )));
            }
        }
        Ok(())
    }
}

fn reserve_call_auction_risk_checkpoint_vec<T>(
    maximum: usize,
    resource: CallAuctionRiskCheckpointResource,
) -> Result<Vec<T>, CallAuctionRiskCheckpointError> {
    let mut values = Vec::new();
    values.try_reserve_exact(maximum).map_err(|_| {
        CallAuctionRiskCheckpointError::ResourceReservationFailed { resource, maximum }
    })?;
    Ok(values)
}

fn checkpoint_accounts(
    risk: &CallAuctionRiskEngine,
) -> Result<Vec<RiskAccountCheckpoint>, CallAuctionRiskCheckpointError> {
    let maximum = risk.accounts.len();
    let mut accounts = reserve_call_auction_risk_checkpoint_vec(
        maximum,
        CallAuctionRiskCheckpointResource::CaptureAccounts,
    )?;
    for (&account_id, account) in risk.accounts.iter() {
        accounts.push(RiskAccountCheckpoint::from_parts(
            account_id,
            account.profile,
            account.exposure,
        ));
    }
    accounts.sort_unstable_by_key(|account| account.account_id());
    Ok(accounts)
}

fn checkpoint_validation_event_capacity(
    checkpoint: &CallAuctionRiskCheckpoint,
    order_bound: usize,
    max_report_events: usize,
) -> Result<usize, CallAuctionRiskCheckpointError> {
    let terminal_event_reserve = max_report_events.checked_add(1).ok_or_else(|| {
        CallAuctionRiskCheckpointError::new(
            "auction risk checkpoint terminal event capacity overflows",
        )
    })?;
    let checkpoint_events =
        checkpoint
            .auction
            .history()
            .iter()
            .try_fold(0_usize, |total, entry| {
                total
                    .checked_add(entry.report().events.len())
                    .ok_or_else(|| {
                        CallAuctionRiskCheckpointError::new(
                            "auction risk checkpoint retained event count overflows",
                        )
                    })
            })?;
    let required_ordinary_events = order_bound.checked_add(1).ok_or_else(|| {
        CallAuctionRiskCheckpointError::new(
            "auction risk checkpoint ordinary event capacity overflows",
        )
    })?;
    let minimum_retained_events = terminal_event_reserve
        .checked_add(required_ordinary_events)
        .ok_or_else(|| {
            CallAuctionRiskCheckpointError::new(
                "auction risk checkpoint minimum retained event capacity overflows",
            )
        })?;
    checkpoint_events
        .checked_add(terminal_event_reserve)
        .and_then(|value| value.checked_add(1))
        .map(|value| value.max(minimum_retained_events))
        .ok_or_else(|| {
            CallAuctionRiskCheckpointError::new(
                "auction risk checkpoint event history capacity overflows",
            )
        })
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
        .filter(|entry| {
            matches!(
                entry.command(),
                CallAuctionCommand::Submit(_) | CallAuctionCommand::Replace(_)
            )
        })
        .count();
    let order_bound = accepted.max(active).max(submitted).max(1);
    let book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: order_bound,
        max_price_levels_per_side: order_bound,
        max_accepted_order_ids: order_bound,
        max_prepared_uncrosses: CallAuctionBookLimits::DEFAULT_MAX_PREPARED_UNCROSSES,
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
    let max_retained_events =
        checkpoint_validation_event_capacity(checkpoint, order_bound, max_report_events)?;
    let auction = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands,
        terminal_command_reserve,
        max_retained_events,
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

#[cfg(test)]
mod tests {
    use super::{
        CallAuctionRiskCheckpointError, CallAuctionRiskCheckpointResource, CallAuctionRiskEngine,
        CallAuctionRiskLimits, CallAuctionRiskLimitsSpec, reserve_call_auction_risk_checkpoint_vec,
    };
    use crate::auction::AuctionOrderConstraint;
    use crate::auction_book::{
        CallAuctionBookLimits, CallAuctionBookLimitsSpec, CallAuctionOrderSnapshot,
    };
    use crate::auction_engine::{CallAuctionEngineLimits, CallAuctionEngineLimitsSpec};
    use crate::domain::{
        AccountId, AssetId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
        TimestampNs,
    };
    use crate::instrument::{
        InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
        QuantityRules, ReserveOrderRules, TradingState,
    };
    use crate::risk::{AccountRiskState, RiskLimitSpec, RiskLimits, RiskProfile, RiskSnapshot};

    fn definition() -> InstrumentDefinition {
        InstrumentDefinition::new(InstrumentSpec {
            instrument_id: InstrumentId::new(41).unwrap(),
            version: InstrumentVersion::new(7).unwrap(),
            effective_from: TimestampNs::from_unix_nanos(0),
            symbol: InstrumentSymbol::new("AUCTION-RISK-AUDIT").unwrap(),
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

    fn risk_engine() -> CallAuctionRiskEngine {
        let book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
            max_active_orders: 4,
            max_price_levels_per_side: 4,
            max_accepted_order_ids: 8,
            max_prepared_uncrosses: 2,
        })
        .unwrap();
        let auction = CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
            book,
            max_retained_commands: 16,
            terminal_command_reserve: 6,
            max_retained_events: 26,
            max_report_events: 9,
        })
        .unwrap();
        let limits = CallAuctionRiskLimits::new(CallAuctionRiskLimitsSpec {
            auction,
            max_registered_accounts: 2,
        })
        .unwrap();
        let mut risk = CallAuctionRiskEngine::try_with_limits(definition(), limits).unwrap();
        let profile = RiskProfile::new(
            AccountRiskState::Active,
            0,
            RiskLimits::new(RiskLimitSpec {
                max_order_quantity_lots: 1_000,
                max_order_notional: 200_000,
                max_open_orders: 4,
                max_open_quantity_lots: 4_000,
                max_open_notional: 800_000,
                max_long_position_lots: i128::MAX.unsigned_abs(),
                max_short_position_lots: i128::MAX.unsigned_abs(),
            })
            .unwrap(),
        )
        .unwrap();
        risk.register_account(AccountId::new(1).unwrap(), profile)
            .unwrap();
        for (priority_sequence, order_id) in [10_u64, 20].into_iter().enumerate() {
            risk.insert_reservation(CallAuctionOrderSnapshot {
                order_id: OrderId::new(order_id).unwrap(),
                account_id: AccountId::new(1).unwrap(),
                side: Side::Buy,
                constraint: AuctionOrderConstraint::Market,
                quantity: Quantity::new(1).unwrap(),
                priority_class: crate::auction::AuctionPriorityClass::HIGHEST,
                priority_sequence: u64::try_from(priority_sequence).unwrap() + 1,
            });
        }
        risk.validate().unwrap();
        risk
    }

    #[test]
    fn allocation_free_risk_audit_rejects_cycles_and_unlinked_reservations() {
        let head = OrderId::new(10).unwrap();
        let tail = OrderId::new(20).unwrap();

        let mut cycle = risk_engine();
        cycle.reservations.get_mut(&tail).unwrap().next = Some(head);
        assert!(
            cycle
                .validate()
                .unwrap_err()
                .detail()
                .contains("account-index cycle")
        );

        let mut unlinked = risk_engine();
        unlinked.reservations.get_mut(&head).unwrap().next = None;
        let account = unlinked
            .accounts
            .get_mut(&AccountId::new(1).unwrap())
            .unwrap();
        account.reservation_tail = Some(head);
        account.exposure = RiskSnapshot::from_parts(0, 1, 0, 200, 1);
        assert!(
            unlinked
                .validate()
                .unwrap_err()
                .detail()
                .contains("absent from its account index")
        );
    }

    #[test]
    fn unrepresentable_checkpoint_capture_is_typed_by_exact_resource() {
        let error = reserve_call_auction_risk_checkpoint_vec::<RiskSnapshot>(
            usize::MAX,
            CallAuctionRiskCheckpointResource::CaptureAccounts,
        )
        .unwrap_err();
        assert_eq!(
            error,
            CallAuctionRiskCheckpointError::ResourceReservationFailed {
                resource: CallAuctionRiskCheckpointResource::CaptureAccounts,
                maximum: usize::MAX,
            }
        );
        assert_eq!(
            error.resource(),
            Some(CallAuctionRiskCheckpointResource::CaptureAccounts)
        );
        assert!(error.is_resource_exhaustion());
        assert!(error.is_operational_failure());
    }

    #[test]
    fn reservation_links_survive_middle_partial_head_and_last_removal() {
        let mut risk = risk_engine();
        let third = OrderId::new(30).unwrap();
        risk.insert_reservation(CallAuctionOrderSnapshot {
            order_id: third,
            account_id: AccountId::new(1).unwrap(),
            side: Side::Buy,
            constraint: AuctionOrderConstraint::Market,
            quantity: Quantity::new(2).unwrap(),
            priority_class: crate::auction::AuctionPriorityClass::HIGHEST,
            priority_sequence: 3,
        });
        risk.validate().unwrap();

        risk.remove_reservation(OrderId::new(20).unwrap());
        risk.validate().unwrap();
        risk.decrement_reservation(third, 1);
        risk.validate().unwrap();
        risk.remove_reservation(OrderId::new(10).unwrap());
        risk.validate().unwrap();
        risk.remove_reservation(third);
        risk.validate().unwrap();
        assert_eq!(risk.reservation_count(), 0);
    }
}
