//! Deterministic pre-trade limits and conservative open-order reservations.
//!
//! The risk layer owns no matching priority. It authorizes a complete incoming
//! quantity against worst-case position and absolute raw-price notional, then
//! derives the retained reservation from the matching engine's sequenced trace.

use std::collections::HashMap;
use std::fmt;

use crate::domain::{AccountId, OrderId, Price, Side};
use crate::instrument::InstrumentDefinition;
use crate::matching::{
    Command, CommandOutcome, EventKind, ExecutionReport, MatchingError, NewOrder, OrderBook,
    OrderBookCheckpoint, OrderBookCheckpointError, OrderBookLimits, OrderType, RejectReason,
    ReplaceOrder, SelfTradePrevention, TimeInForce, Trade,
};

/// Account-level order-entry state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountRiskState {
    /// Orders are evaluated against all numerical limits.
    Active,
    /// Only aggregate orders that cannot cross or increase the current position are permitted.
    ReduceOnly,
    /// New orders and replacements are rejected; cancellation remains available.
    Blocked,
}

/// Constructor fields for immutable account risk limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskLimitSpec {
    /// Maximum lots in one command.
    pub max_order_quantity_lots: u64,
    /// Maximum absolute raw-price-times-lots notional in one command.
    pub max_order_notional: u128,
    /// Maximum simultaneously resting orders.
    pub max_open_orders: u64,
    /// Maximum aggregate resting lots across both sides.
    pub max_open_quantity_lots: u128,
    /// Maximum aggregate absolute raw-price-times-lots resting notional.
    pub max_open_notional: u128,
    /// Maximum worst-case long position in lots.
    pub max_long_position_lots: u128,
    /// Maximum worst-case absolute short position in lots.
    pub max_short_position_lots: u128,
}

/// Validated immutable numerical limits for one account and instrument shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskLimits {
    spec: RiskLimitSpec,
}

impl RiskLimits {
    /// Validates an account limit set.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError::InvalidLimits`] for zero limits, a per-order limit
    /// exceeding its aggregate counterpart, or a position bound exceeding
    /// signed `i128` position capacity.
    pub fn new(spec: RiskLimitSpec) -> Result<Self, RiskError> {
        if spec.max_order_quantity_lots == 0
            || spec.max_order_notional == 0
            || spec.max_open_orders == 0
            || spec.max_open_quantity_lots == 0
            || spec.max_open_notional == 0
            || spec.max_long_position_lots == 0
            || spec.max_short_position_lots == 0
            || u128::from(spec.max_order_quantity_lots) > spec.max_open_quantity_lots
            || spec.max_order_notional > spec.max_open_notional
            || spec.max_long_position_lots > i128::MAX.unsigned_abs()
            || spec.max_short_position_lots > i128::MAX.unsigned_abs()
        {
            return Err(RiskError::InvalidLimits);
        }
        Ok(Self { spec })
    }

    /// Returns the per-order quantity limit.
    #[must_use]
    pub const fn max_order_quantity_lots(self) -> u64 {
        self.spec.max_order_quantity_lots
    }

    /// Returns the per-order absolute notional limit.
    #[must_use]
    pub const fn max_order_notional(self) -> u128 {
        self.spec.max_order_notional
    }

    /// Returns the resting-order count limit.
    #[must_use]
    pub const fn max_open_orders(self) -> u64 {
        self.spec.max_open_orders
    }

    /// Returns the aggregate resting quantity limit.
    #[must_use]
    pub const fn max_open_quantity_lots(self) -> u128 {
        self.spec.max_open_quantity_lots
    }

    /// Returns the aggregate resting absolute notional limit.
    #[must_use]
    pub const fn max_open_notional(self) -> u128 {
        self.spec.max_open_notional
    }

    /// Returns the maximum worst-case long position.
    #[must_use]
    pub const fn max_long_position_lots(self) -> u128 {
        self.spec.max_long_position_lots
    }

    /// Returns the maximum worst-case absolute short position.
    #[must_use]
    pub const fn max_short_position_lots(self) -> u128 {
        self.spec.max_short_position_lots
    }
}

/// Immutable account configuration for one risk-managed book.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskProfile {
    state: AccountRiskState,
    initial_position_lots: i128,
    limits: RiskLimits,
}

/// Canonical account identifier and immutable profile persisted by durable risk shards.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccountRiskDefinition {
    account_id: AccountId,
    profile: RiskProfile,
}

impl AccountRiskDefinition {
    /// Associates one account with one immutable risk profile.
    #[must_use]
    pub const fn new(account_id: AccountId, profile: RiskProfile) -> Self {
        Self {
            account_id,
            profile,
        }
    }

    /// Returns the account identifier.
    #[must_use]
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }

    /// Returns the immutable risk profile.
    #[must_use]
    pub const fn profile(self) -> RiskProfile {
        self.profile
    }
}

impl RiskProfile {
    /// Constructs a profile and checks its opening position.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError::InitialPositionOutsideLimits`] when the signed
    /// initial position exceeds the configured long or short bound.
    pub fn new(
        state: AccountRiskState,
        initial_position_lots: i128,
        limits: RiskLimits,
    ) -> Result<Self, RiskError> {
        if !position_within_limits(initial_position_lots, limits) {
            return Err(RiskError::InitialPositionOutsideLimits);
        }
        Ok(Self {
            state,
            initial_position_lots,
            limits,
        })
    }

    /// Returns the account order-entry state.
    #[must_use]
    pub const fn state(self) -> AccountRiskState {
        self.state
    }

    /// Returns the signed opening position.
    #[must_use]
    pub const fn initial_position_lots(self) -> i128 {
        self.initial_position_lots
    }

    /// Returns the numerical limits.
    #[must_use]
    pub const fn limits(self) -> RiskLimits {
        self.limits
    }
}

/// Risk configuration failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskError {
    /// Limit relationships were zero, inverted, or not representable.
    InvalidLimits,
    /// Initial signed position exceeded a position limit.
    InitialPositionOutsideLimits,
    /// An account profile was registered more than once.
    DuplicateProfile(AccountId),
}

impl fmt::Display for RiskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits => formatter.write_str("risk limits are invalid"),
            Self::InitialPositionOutsideLimits => {
                formatter.write_str("initial position exceeds risk limits")
            }
            Self::DuplicateProfile(account_id) => {
                write!(
                    formatter,
                    "risk profile for account {account_id} already exists"
                )
            }
        }
    }
}

impl std::error::Error for RiskError {}

/// Read-only account exposure state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskSnapshot {
    position_lots: i128,
    open_buy_lots: u128,
    open_sell_lots: u128,
    open_notional: u128,
    open_orders: u64,
}

impl RiskSnapshot {
    pub(crate) const fn from_parts(
        position_lots: i128,
        open_buy_lots: u128,
        open_sell_lots: u128,
        open_notional: u128,
        open_orders: u64,
    ) -> Self {
        Self {
            position_lots,
            open_buy_lots,
            open_sell_lots,
            open_notional,
            open_orders,
        }
    }

    /// Returns the signed executed position.
    #[must_use]
    pub const fn position_lots(self) -> i128 {
        self.position_lots
    }

    /// Returns aggregate resting buy quantity.
    #[must_use]
    pub const fn open_buy_lots(self) -> u128 {
        self.open_buy_lots
    }

    /// Returns aggregate resting sell quantity.
    #[must_use]
    pub const fn open_sell_lots(self) -> u128 {
        self.open_sell_lots
    }

    /// Returns aggregate absolute resting notional.
    #[must_use]
    pub const fn open_notional(self) -> u128 {
        self.open_notional
    }

    /// Returns the number of resting reservations.
    #[must_use]
    pub const fn open_orders(self) -> u64 {
        self.open_orders
    }
}

/// Canonical profile and current exposure for one checkpointed account.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskAccountCheckpoint {
    account_id: AccountId,
    profile: RiskProfile,
    exposure: RiskSnapshot,
}

impl RiskAccountCheckpoint {
    /// Returns the account identifier.
    #[must_use]
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }

    /// Returns the immutable account profile.
    #[must_use]
    pub const fn profile(self) -> RiskProfile {
        self.profile
    }

    /// Returns current signed position and aggregate open-order exposure.
    #[must_use]
    pub const fn exposure(self) -> RiskSnapshot {
        self.exposure
    }

    pub(crate) const fn from_parts(
        account_id: AccountId,
        profile: RiskProfile,
        exposure: RiskSnapshot,
    ) -> Self {
        Self {
            account_id,
            profile,
            exposure,
        }
    }
}

/// Coupled matching/risk direct state at a completed risk-WAL report boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskManagedCheckpoint {
    wal_first_sequence: u64,
    matching: OrderBookCheckpoint,
    accounts: Vec<RiskAccountCheckpoint>,
}

impl RiskManagedCheckpoint {
    /// Returns the first WAL sequence, occupied by the instrument definition.
    #[must_use]
    pub const fn wal_first_sequence(&self) -> u64 {
        self.wal_first_sequence
    }

    /// Returns the completed execution-report WAL boundary represented here.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.matching.generation()
    }

    /// Returns the embedded canonical matching state and complete report history.
    #[must_use]
    pub const fn matching(&self) -> &OrderBookCheckpoint {
        &self.matching
    }

    /// Returns canonical account-sorted profiles and current exposures.
    #[must_use]
    pub fn accounts(&self) -> &[RiskAccountCheckpoint] {
        &self.accounts
    }

    pub(crate) fn from_parts(
        wal_first_sequence: u64,
        matching: OrderBookCheckpoint,
        accounts: Vec<RiskAccountCheckpoint>,
    ) -> Result<Self, RiskManagedCheckpointError> {
        let checkpoint = Self {
            wal_first_sequence,
            matching,
            accounts,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    fn validate(&self) -> Result<(), RiskManagedCheckpointError> {
        if self.wal_first_sequence == 0 {
            return Err(RiskManagedCheckpointError::new(
                "risk checkpoint WAL first sequence is zero",
            ));
        }
        let profile_count = u64::try_from(self.accounts.len()).map_err(|_| {
            RiskManagedCheckpointError::new("risk checkpoint account count exceeds u64")
        })?;
        let expected_metadata_sequence = self
            .wal_first_sequence
            .checked_add(profile_count)
            .ok_or_else(|| {
                RiskManagedCheckpointError::new("risk checkpoint metadata boundary overflow")
            })?;
        if self.matching.wal_metadata_sequence() != expected_metadata_sequence {
            return Err(RiskManagedCheckpointError::new(
                "risk checkpoint matching boundary does not follow its profile metadata",
            ));
        }
        for pair in self.accounts.windows(2) {
            if pair[0].account_id >= pair[1].account_id {
                return Err(RiskManagedCheckpointError::new(
                    "risk checkpoint accounts are not strictly canonical",
                ));
            }
        }

        let direct = self.restore_direct()?;
        let mut replay = RiskManagedOrderBook::new(self.matching.definition());
        for account in &self.accounts {
            replay.register_account(account.account_id, account.profile)?;
        }
        for entry in self.matching.history() {
            let reproduced = replay.submit(entry.command()).map_err(|error| {
                RiskManagedCheckpointError::new(format!(
                    "risk checkpoint history cannot be replayed: {error}"
                ))
            })?;
            if reproduced != *entry.report() {
                return Err(RiskManagedCheckpointError::new(
                    "risk checkpoint history diverges under coupled deterministic replay",
                ));
            }
        }
        if replay != direct {
            return Err(RiskManagedCheckpointError::new(
                "risk checkpoint direct state differs from coupled history replay",
            ));
        }
        Ok(())
    }

    fn restore_direct(&self) -> Result<RiskManagedOrderBook, RiskManagedCheckpointError> {
        self.restore_direct_with_limits(OrderBookLimits::default())
    }

    fn restore_direct_with_limits(
        &self,
        limits: OrderBookLimits,
    ) -> Result<RiskManagedOrderBook, RiskManagedCheckpointError> {
        let book = OrderBook::from_checkpoint_with_limits(self.matching.clone(), limits)?;
        let mut risk = RiskEngine::new(self.matching.definition());
        for account in &self.accounts {
            risk.register_account(account.account_id, account.profile)?;
            risk.accounts
                .get_mut(&account.account_id)
                .expect("registered checkpoint account exists")
                .exposure
                .position_lots = account.exposure.position_lots;
        }
        for order in self.matching.orders() {
            if !risk.accounts.contains_key(&order.account_id()) {
                return Err(RiskManagedCheckpointError::new(format!(
                    "risk checkpoint active order {} has no account profile",
                    order.order_id()
                )));
            }
            risk.insert_reservation(
                order.order_id(),
                order.account_id(),
                order.side(),
                order.price(),
                order.leaves().lots(),
            );
        }
        for account in &self.accounts {
            let restored = risk
                .snapshot(account.account_id)
                .expect("registered checkpoint account exists");
            if restored != account.exposure {
                return Err(RiskManagedCheckpointError::new(format!(
                    "risk checkpoint account {} exposure differs from active reservations",
                    account.account_id
                )));
            }
        }
        let managed = RiskManagedOrderBook { book, risk };
        managed
            .validate()
            .map_err(|error| RiskManagedCheckpointError::new(error.detail()))?;
        Ok(managed)
    }

    pub(crate) fn is_successor_of(&self, previous: &Self) -> bool {
        self.wal_first_sequence == previous.wal_first_sequence
            && self.accounts.len() == previous.accounts.len()
            && self
                .accounts
                .iter()
                .zip(&previous.accounts)
                .all(|(current, old)| {
                    current.account_id == old.account_id && current.profile == old.profile
                })
            && self.matching.is_successor_of(&previous.matching)
    }
}

/// Semantic coupled risk/matching checkpoint construction or restoration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskManagedCheckpointError {
    detail: String,
}

impl RiskManagedCheckpointError {
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

impl fmt::Display for RiskManagedCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(formatter)
    }
}

impl std::error::Error for RiskManagedCheckpointError {}

impl From<OrderBookCheckpointError> for RiskManagedCheckpointError {
    fn from(error: OrderBookCheckpointError) -> Self {
        Self::new(error.detail())
    }
}

impl From<RiskError> for RiskManagedCheckpointError {
    fn from(error: RiskError) -> Self {
        Self::new(error.to_string())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RiskAccount {
    profile: RiskProfile,
    exposure: RiskSnapshot,
}

/// Read-only reservation state for one resting order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReservationSnapshot {
    account_id: AccountId,
    side: Side,
    price: Price,
    quantity_lots: u64,
    notional: u128,
}

impl ReservationSnapshot {
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

    /// Returns the resting limit price.
    #[must_use]
    pub const fn price(self) -> Price {
        self.price
    }

    /// Returns the reserved leaves quantity.
    #[must_use]
    pub const fn quantity_lots(self) -> u64 {
        self.quantity_lots
    }

    /// Returns absolute raw-price-times-lots notional.
    #[must_use]
    pub const fn notional(self) -> u128 {
        self.notional
    }
}

/// Deterministic account profiles, positions, and resting-order reservations.
#[derive(Debug, Eq, PartialEq)]
pub struct RiskEngine {
    definition: InstrumentDefinition,
    accounts: HashMap<AccountId, RiskAccount>,
    reservations: HashMap<OrderId, ReservationSnapshot>,
}

impl RiskEngine {
    fn new(definition: InstrumentDefinition) -> Self {
        Self {
            definition,
            accounts: HashMap::new(),
            reservations: HashMap::new(),
        }
    }

    /// Registers one immutable account profile.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError::DuplicateProfile`] when the account already exists.
    pub fn register_account(
        &mut self,
        account_id: AccountId,
        profile: RiskProfile,
    ) -> Result<(), RiskError> {
        if self.accounts.contains_key(&account_id) {
            return Err(RiskError::DuplicateProfile(account_id));
        }
        self.accounts.insert(
            account_id,
            RiskAccount {
                profile,
                exposure: RiskSnapshot {
                    position_lots: profile.initial_position_lots,
                    open_buy_lots: 0,
                    open_sell_lots: 0,
                    open_notional: 0,
                    open_orders: 0,
                },
            },
        );
        Ok(())
    }

    /// Returns one account's current position and reservations.
    #[must_use]
    pub fn snapshot(&self, account_id: AccountId) -> Option<RiskSnapshot> {
        self.accounts.get(&account_id).map(|value| value.exposure)
    }

    /// Returns one active order reservation.
    #[must_use]
    pub fn reservation(&self, order_id: OrderId) -> Option<ReservationSnapshot> {
        self.reservations.get(&order_id).copied()
    }

    /// Returns the active reservation count.
    #[must_use]
    pub fn reservation_count(&self) -> usize {
        self.reservations.len()
    }

    fn authorize(&self, command: Command) -> Result<(), RejectReason> {
        match command {
            Command::New(order) => self.authorize_new(order),
            Command::Cancel(_) | Command::MassCancel(_) => Ok(()),
            Command::Replace(order) => self.authorize_replace(order),
        }
    }

    fn authorize_new(&self, order: NewOrder) -> Result<(), RejectReason> {
        let account = self
            .accounts
            .get(&order.account_id)
            .ok_or(RejectReason::RiskProfileMissing)?;
        let notional = self.order_notional(order.side, order.order_type, order.quantity.lots())?;
        Self::check_order(
            *account,
            account.exposure,
            order.side,
            order.quantity.lots(),
            notional,
            matches!(
                (order.order_type, order.time_in_force),
                (
                    OrderType::Limit(_),
                    TimeInForce::GoodTilCancelled | TimeInForce::PostOnly
                )
            ),
        )
    }

    fn authorize_replace(&self, order: ReplaceOrder) -> Result<(), RejectReason> {
        let account = self
            .accounts
            .get(&order.account_id)
            .ok_or(RejectReason::RiskProfileMissing)?;
        let old = self
            .reservations
            .get(&order.order_id)
            .copied()
            .ok_or(RejectReason::RiskArithmeticOverflow)?;
        if old.account_id != order.account_id {
            return Err(RejectReason::RiskArithmeticOverflow);
        }
        let baseline = subtract_reservation(account.exposure, old)
            .ok_or(RejectReason::RiskArithmeticOverflow)?;
        let notional = self.order_notional(
            old.side,
            OrderType::Limit(order.new_price),
            order.new_quantity.lots(),
        )?;
        Self::check_order(
            *account,
            baseline,
            old.side,
            order.new_quantity.lots(),
            notional,
            true,
        )
    }

    fn check_order(
        account: RiskAccount,
        baseline: RiskSnapshot,
        side: Side,
        quantity: u64,
        notional: u128,
        may_rest: bool,
    ) -> Result<(), RejectReason> {
        let limits = account.profile.limits;
        match account.profile.state {
            AccountRiskState::Active => {}
            AccountRiskState::Blocked => return Err(RejectReason::RiskAccountBlocked),
            AccountRiskState::ReduceOnly => {
                if !strictly_reduces(baseline, side, quantity) {
                    return Err(RejectReason::RiskReduceOnly);
                }
            }
        }
        if quantity > limits.max_order_quantity_lots() {
            return Err(RejectReason::RiskOrderQuantityLimit);
        }
        if notional > limits.max_order_notional() {
            return Err(RejectReason::RiskOrderNotionalLimit);
        }
        let added_open_orders = u64::from(may_rest);
        let added_open_quantity = if may_rest { u128::from(quantity) } else { 0 };
        let added_open_notional = if may_rest { notional } else { 0 };
        let open_orders = baseline
            .open_orders
            .checked_add(added_open_orders)
            .ok_or(RejectReason::RiskArithmeticOverflow)?;
        if open_orders > limits.max_open_orders() {
            return Err(RejectReason::RiskOpenOrderCountLimit);
        }
        let total_open = baseline
            .open_buy_lots
            .checked_add(baseline.open_sell_lots)
            .and_then(|value| value.checked_add(added_open_quantity))
            .ok_or(RejectReason::RiskArithmeticOverflow)?;
        if total_open > limits.max_open_quantity_lots() {
            return Err(RejectReason::RiskOpenQuantityLimit);
        }
        let open_notional = baseline
            .open_notional
            .checked_add(added_open_notional)
            .ok_or(RejectReason::RiskArithmeticOverflow)?;
        if open_notional > limits.max_open_notional() {
            return Err(RejectReason::RiskOpenNotionalLimit);
        }
        let (worst_buy, worst_sell) = match side {
            Side::Buy => (
                baseline
                    .open_buy_lots
                    .checked_add(u128::from(quantity))
                    .ok_or(RejectReason::RiskArithmeticOverflow)?,
                baseline.open_sell_lots,
            ),
            Side::Sell => (
                baseline.open_buy_lots,
                baseline
                    .open_sell_lots
                    .checked_add(u128::from(quantity))
                    .ok_or(RejectReason::RiskArithmeticOverflow)?,
            ),
        };
        if !worst_case_position_within_limits(baseline.position_lots, worst_buy, worst_sell, limits)
        {
            return Err(RejectReason::RiskPositionLimit);
        }
        Ok(())
    }

    fn order_notional(
        &self,
        side: Side,
        order_type: OrderType,
        quantity: u64,
    ) -> Result<u128, RejectReason> {
        let rules = self.definition.price_rules();
        let magnitude = match (side, order_type) {
            (Side::Buy, OrderType::Limit(limit)) => limit
                .raw()
                .unsigned_abs()
                .max(rules.minimum().raw().unsigned_abs()),
            (Side::Sell, OrderType::Limit(limit)) => limit
                .raw()
                .unsigned_abs()
                .max(rules.maximum().raw().unsigned_abs()),
            (_, OrderType::Market) => rules
                .minimum()
                .raw()
                .unsigned_abs()
                .max(rules.maximum().raw().unsigned_abs()),
        };
        u128::from(magnitude)
            .checked_mul(u128::from(quantity))
            .ok_or(RejectReason::RiskArithmeticOverflow)
    }

    fn apply(&mut self, command: Command, report: &ExecutionReport) {
        if !matches!(report.outcome, CommandOutcome::Accepted) {
            return;
        }
        let replacement_side = if let Command::Replace(order) = command {
            Some(
                self.reservations
                    .get(&order.order_id)
                    .expect("accepted replacement must have an existing reservation")
                    .side,
            )
        } else {
            None
        };
        if let Command::Replace(order) = command {
            self.remove_reservation(order.order_id);
        }

        for event in &report.events {
            match event.kind {
                EventKind::Trade(trade) => self.apply_trade(trade),
                EventKind::OrderCancelled { order_id, .. } => {
                    if self.reservations.contains_key(&order_id) {
                        self.remove_reservation(order_id);
                    }
                }
                EventKind::SelfTradePrevented {
                    resting_order_id,
                    quantity,
                    policy: SelfTradePrevention::DecrementAndCancel,
                    ..
                } => self.decrement_reservation(resting_order_id, quantity.lots()),
                _ => {}
            }
        }

        match command {
            Command::New(order) => {
                if let Some((price, quantity)) = rested_order(report, order.order_id) {
                    self.insert_reservation(
                        order.order_id,
                        order.account_id,
                        order.side,
                        price,
                        quantity,
                    );
                }
            }
            Command::Cancel(_) | Command::MassCancel(_) => {}
            Command::Replace(order) => {
                if replacement_retained_priority(report, order.order_id) {
                    self.insert_reservation(
                        order.order_id,
                        order.account_id,
                        replacement_side.expect("replacement side was captured before removal"),
                        order.new_price,
                        order.new_quantity.lots(),
                    );
                } else if let Some((price, quantity)) = rested_order(report, order.order_id) {
                    self.insert_reservation(
                        order.order_id,
                        order.account_id,
                        replacement_side.expect("replacement side was captured before removal"),
                        price,
                        quantity,
                    );
                }
            }
        }
    }

    fn apply_trade(&mut self, trade: Trade) {
        self.decrement_reservation(trade.maker_order_id, trade.quantity.lots());
        let quantity = i128::from(trade.quantity.lots());
        let buyer = self
            .accounts
            .get_mut(&trade.buyer_account_id)
            .expect("authorized buyer must have a risk profile");
        buyer.exposure.position_lots = buyer
            .exposure
            .position_lots
            .checked_add(quantity)
            .expect("pre-trade long position capacity must cover execution");
        let seller = self
            .accounts
            .get_mut(&trade.seller_account_id)
            .expect("authorized seller must have a risk profile");
        seller.exposure.position_lots = seller
            .exposure
            .position_lots
            .checked_sub(quantity)
            .expect("pre-trade short position capacity must cover execution");
    }

    fn insert_reservation(
        &mut self,
        order_id: OrderId,
        account_id: AccountId,
        side: Side,
        price: Price,
        quantity_lots: u64,
    ) {
        let notional = absolute_notional(price, quantity_lots)
            .expect("pre-trade notional capacity must cover resting leaves");
        let reservation = ReservationSnapshot {
            account_id,
            side,
            price,
            quantity_lots,
            notional,
        };
        assert!(self.reservations.insert(order_id, reservation).is_none());
        let exposure = &mut self
            .accounts
            .get_mut(&account_id)
            .expect("authorized account must have a risk profile")
            .exposure;
        match side {
            Side::Buy => {
                exposure.open_buy_lots = exposure
                    .open_buy_lots
                    .checked_add(u128::from(quantity_lots))
                    .expect("authorization reserved aggregate buy capacity");
            }
            Side::Sell => {
                exposure.open_sell_lots = exposure
                    .open_sell_lots
                    .checked_add(u128::from(quantity_lots))
                    .expect("authorization reserved aggregate sell capacity");
            }
        }
        exposure.open_notional = exposure
            .open_notional
            .checked_add(notional)
            .expect("authorization reserved aggregate notional capacity");
        exposure.open_orders = exposure
            .open_orders
            .checked_add(1)
            .expect("authorization reserved order-count capacity");
    }

    fn decrement_reservation(&mut self, order_id: OrderId, quantity_lots: u64) {
        let Some(current) = self.reservations.get(&order_id).copied() else {
            return;
        };
        assert!(quantity_lots <= current.quantity_lots);
        if quantity_lots == current.quantity_lots {
            self.remove_reservation(order_id);
            return;
        }
        self.remove_reservation(order_id);
        self.insert_reservation(
            order_id,
            current.account_id,
            current.side,
            current.price,
            current.quantity_lots - quantity_lots,
        );
    }

    fn remove_reservation(&mut self, order_id: OrderId) -> ReservationSnapshot {
        let reservation = self
            .reservations
            .remove(&order_id)
            .expect("active matching order must have a risk reservation");
        let exposure = &mut self
            .accounts
            .get_mut(&reservation.account_id)
            .expect("reserved account must have a risk profile")
            .exposure;
        match reservation.side {
            Side::Buy => exposure.open_buy_lots -= u128::from(reservation.quantity_lots),
            Side::Sell => exposure.open_sell_lots -= u128::from(reservation.quantity_lots),
        }
        exposure.open_notional -= reservation.notional;
        exposure.open_orders -= 1;
        reservation
    }

    fn validate(&self) -> Result<(), RiskInvariantViolation> {
        let mut calculated: HashMap<AccountId, (u128, u128, u128, u64)> = HashMap::new();
        for (&order_id, reservation) in &self.reservations {
            let expected_notional = absolute_notional(reservation.price, reservation.quantity_lots)
                .ok_or_else(|| {
                    RiskInvariantViolation::new(format!(
                        "reservation {order_id} notional cannot be represented"
                    ))
                })?;
            if reservation.notional != expected_notional {
                return Err(RiskInvariantViolation::new(format!(
                    "reservation {order_id} has inconsistent notional"
                )));
            }
            let entry = calculated.entry(reservation.account_id).or_default();
            match reservation.side {
                Side::Buy => entry.0 = checked_audit_add(entry.0, reservation.quantity_lots)?,
                Side::Sell => entry.1 = checked_audit_add(entry.1, reservation.quantity_lots)?,
            }
            entry.2 = entry.2.checked_add(reservation.notional).ok_or_else(|| {
                RiskInvariantViolation::new("aggregate reservation notional overflow")
            })?;
            entry.3 = entry.3.checked_add(1).ok_or_else(|| {
                RiskInvariantViolation::new("aggregate reservation count overflow")
            })?;
        }
        for (&account_id, account) in &self.accounts {
            let expected = calculated.get(&account_id).copied().unwrap_or_default();
            let actual = account.exposure;
            if (
                actual.open_buy_lots,
                actual.open_sell_lots,
                actual.open_notional,
                actual.open_orders,
            ) != expected
            {
                return Err(RiskInvariantViolation::new(format!(
                    "account {account_id} exposure aggregates differ from reservations"
                )));
            }
            if !position_within_limits(actual.position_lots, account.profile.limits)
                || !worst_case_position_within_limits(
                    actual.position_lots,
                    actual.open_buy_lots,
                    actual.open_sell_lots,
                    account.profile.limits,
                )
            {
                return Err(RiskInvariantViolation::new(format!(
                    "account {account_id} exceeds position limits"
                )));
            }
        }
        Ok(())
    }
}

/// Structural inconsistency between risk exposure and matching state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskInvariantViolation {
    detail: String,
}

impl RiskInvariantViolation {
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

impl fmt::Display for RiskInvariantViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(formatter)
    }
}

impl std::error::Error for RiskInvariantViolation {}

/// One order book coupled to deterministic pre-trade risk and reservations.
#[derive(Debug, Eq, PartialEq)]
pub struct RiskManagedOrderBook {
    book: OrderBook,
    risk: RiskEngine,
}

impl RiskManagedOrderBook {
    /// Creates an empty risk-managed book for one immutable definition.
    #[must_use]
    pub fn new(definition: InstrumentDefinition) -> Self {
        Self::with_limits(definition, OrderBookLimits::default())
    }

    /// Creates an empty risk-managed book under explicit matching limits.
    ///
    /// # Panics
    ///
    /// Panics when requested constructor-time matching hash reservation fails.
    #[must_use]
    pub fn with_limits(definition: InstrumentDefinition, limits: OrderBookLimits) -> Self {
        Self::try_with_limits(definition, limits)
            .expect("matching capacity reservation must succeed under A12")
    }

    /// Creates an empty risk-managed book with fallible matching preallocation.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError::CapacityReservationFailed`] when a configured
    /// matching hash reservation cannot be represented or allocated.
    pub fn try_with_limits(
        definition: InstrumentDefinition,
        limits: OrderBookLimits,
    ) -> Result<Self, MatchingError> {
        let book = OrderBook::try_with_limits(definition, limits)?;
        Ok(Self {
            book,
            risk: RiskEngine::new(definition),
        })
    }

    /// Registers an account before it enters orders.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError::DuplicateProfile`] for a repeated account.
    pub fn register_account(
        &mut self,
        account_id: AccountId,
        profile: RiskProfile,
    ) -> Result<(), RiskError> {
        self.risk.register_account(account_id, profile)
    }

    /// Applies one command after core matching checks and conservative risk authorization.
    ///
    /// Exact retries return the original report without applying risk state twice.
    /// Risk rejections are normal sequenced matching reports.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError`] for command collision, matching capacity, or
    /// sequence/identifier exhaustion.
    pub fn submit(&mut self, command: Command) -> Result<ExecutionReport, MatchingError> {
        if let Some(report) = self.book.preflight(command)? {
            return Ok(report);
        }
        if self.book.check_business_rules(command).is_err() {
            return self.book.submit(command);
        }
        if let Err(reason) = self.risk.authorize(command) {
            return self.book.reject_by_gate(command, reason);
        }
        let report = self.book.submit(command)?;
        self.risk.apply(command, &report);
        debug_assert!(self.validate().is_ok());
        Ok(report)
    }

    /// Performs operational idempotency/capacity checks without mutation.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError`] for command collision, configured matching
    /// capacity, or exhausted sequence capacity.
    pub fn preflight(&self, command: Command) -> Result<Option<ExecutionReport>, MatchingError> {
        self.book.preflight(command)
    }

    /// Captures and independently audits coupled matching, positions, and reservations.
    ///
    /// # Errors
    ///
    /// Returns [`RiskManagedCheckpointError`] when live state, physical WAL
    /// boundaries, canonical account state, or coupled deterministic replay diverge.
    pub fn checkpoint(
        &self,
        wal_first_sequence: u64,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<RiskManagedCheckpoint, RiskManagedCheckpointError> {
        self.validate()
            .map_err(|error| RiskManagedCheckpointError::new(error.detail()))?;
        let matching = self
            .book
            .checkpoint_state(wal_metadata_sequence, wal_sequence)?;
        let mut accounts: Vec<_> = self
            .risk
            .accounts
            .iter()
            .map(|(&account_id, account)| RiskAccountCheckpoint {
                account_id,
                profile: account.profile,
                exposure: account.exposure,
            })
            .collect();
        accounts.sort_unstable_by_key(|value| value.account_id);
        let checkpoint = RiskManagedCheckpoint::from_parts(wal_first_sequence, matching, accounts)?;
        let restored = checkpoint.restore_direct_with_limits(self.book.limits())?;
        if restored != *self {
            return Err(RiskManagedCheckpointError::new(
                "risk checkpoint direct state differs from live coupled state",
            ));
        }
        Ok(checkpoint)
    }

    /// Restores directly indexed matching/risk state from an audited checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`RiskManagedCheckpointError`] for invalid semantic state or
    /// coupled deterministic replay divergence.
    pub fn from_checkpoint(
        checkpoint: &RiskManagedCheckpoint,
    ) -> Result<Self, RiskManagedCheckpointError> {
        checkpoint.restore_direct()
    }

    /// Restores coupled matching/risk state under explicit current matching limits.
    ///
    /// # Errors
    ///
    /// Returns [`RiskManagedCheckpointError`] when semantic state is invalid or
    /// any recovered matching cardinality exceeds the selected limits.
    pub fn from_checkpoint_with_limits(
        checkpoint: &RiskManagedCheckpoint,
        limits: OrderBookLimits,
    ) -> Result<Self, RiskManagedCheckpointError> {
        checkpoint.restore_direct_with_limits(limits)
    }

    /// Returns the underlying read-only order book.
    #[must_use]
    pub const fn book(&self) -> &OrderBook {
        &self.book
    }

    /// Returns the read-only risk engine.
    #[must_use]
    pub const fn risk(&self) -> &RiskEngine {
        &self.risk
    }

    /// Cross-checks matching structure, every reservation, and account aggregates.
    ///
    /// # Errors
    ///
    /// Returns [`RiskInvariantViolation`] at the first inconsistency.
    pub fn validate(&self) -> Result<(), RiskInvariantViolation> {
        self.book
            .validate()
            .map_err(|error| RiskInvariantViolation::new(error.detail()))?;
        self.risk.validate()?;
        if self.book.active_order_count() != self.risk.reservations.len() {
            return Err(RiskInvariantViolation::new(
                "active order count differs from reservation count",
            ));
        }
        for (&order_id, reservation) in &self.risk.reservations {
            let order = self.book.order(order_id).ok_or_else(|| {
                RiskInvariantViolation::new(format!(
                    "reservation {order_id} has no active matching order"
                ))
            })?;
            if order.account_id != reservation.account_id
                || order.side != reservation.side
                || order.price != reservation.price
                || order.leaves_quantity.lots() != reservation.quantity_lots
            {
                return Err(RiskInvariantViolation::new(format!(
                    "reservation {order_id} differs from matching order"
                )));
            }
        }
        Ok(())
    }
}

fn absolute_notional(price: Price, quantity_lots: u64) -> Option<u128> {
    u128::from(price.raw().unsigned_abs()).checked_mul(u128::from(quantity_lots))
}

fn position_within_limits(position: i128, limits: RiskLimits) -> bool {
    if position >= 0 {
        position.unsigned_abs() <= limits.max_long_position_lots()
    } else {
        position.unsigned_abs() <= limits.max_short_position_lots()
    }
}

fn worst_case_position_within_limits(
    position: i128,
    open_buy_lots: u128,
    open_sell_lots: u128,
    limits: RiskLimits,
) -> bool {
    let Ok(buys) = i128::try_from(open_buy_lots) else {
        return false;
    };
    let Ok(sells) = i128::try_from(open_sell_lots) else {
        return false;
    };
    let Some(long) = position.checked_add(buys) else {
        return false;
    };
    let Some(short) = position.checked_sub(sells) else {
        return false;
    };
    long <= i128::try_from(limits.max_long_position_lots()).expect("validated long limit fits i128")
        && short
            >= -i128::try_from(limits.max_short_position_lots())
                .expect("validated short limit fits i128")
}

fn strictly_reduces(exposure: RiskSnapshot, side: Side, quantity: u64) -> bool {
    match (exposure.position_lots.cmp(&0), side) {
        (std::cmp::Ordering::Greater, Side::Sell) => exposure
            .open_sell_lots
            .checked_add(u128::from(quantity))
            .is_some_and(|total| total <= exposure.position_lots.unsigned_abs()),
        (std::cmp::Ordering::Less, Side::Buy) => exposure
            .open_buy_lots
            .checked_add(u128::from(quantity))
            .is_some_and(|total| total <= exposure.position_lots.unsigned_abs()),
        _ => false,
    }
}

fn subtract_reservation(
    exposure: RiskSnapshot,
    reservation: ReservationSnapshot,
) -> Option<RiskSnapshot> {
    let quantity = u128::from(reservation.quantity_lots);
    let (open_buy_lots, open_sell_lots) = match reservation.side {
        Side::Buy => (
            exposure.open_buy_lots.checked_sub(quantity)?,
            exposure.open_sell_lots,
        ),
        Side::Sell => (
            exposure.open_buy_lots,
            exposure.open_sell_lots.checked_sub(quantity)?,
        ),
    };
    Some(RiskSnapshot {
        position_lots: exposure.position_lots,
        open_buy_lots,
        open_sell_lots,
        open_notional: exposure.open_notional.checked_sub(reservation.notional)?,
        open_orders: exposure.open_orders.checked_sub(1)?,
    })
}

fn rested_order(report: &ExecutionReport, order_id: OrderId) -> Option<(Price, u64)> {
    report.events.iter().find_map(|event| match event.kind {
        EventKind::OrderRested {
            order_id: value,
            price,
            leaves_quantity,
            ..
        } if value == order_id => Some((price, leaves_quantity.lots())),
        _ => None,
    })
}

fn replacement_retained_priority(report: &ExecutionReport, order_id: OrderId) -> bool {
    report.events.iter().any(|event| {
        matches!(
            event.kind,
            EventKind::OrderReplaced {
                order_id: value,
                priority_retained: true,
                ..
            } if value == order_id
        )
    })
}

fn checked_audit_add(current: u128, quantity: u64) -> Result<u128, RiskInvariantViolation> {
    current
        .checked_add(u128::from(quantity))
        .ok_or_else(|| RiskInvariantViolation::new("aggregate reservation quantity overflow"))
}
