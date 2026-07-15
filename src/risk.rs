//! Deterministic pre-trade limits and conservative open-order reservations.
//!
//! The risk layer owns no matching priority. It authorizes a complete incoming
//! quantity against worst-case position and absolute raw-price notional, then
//! derives the retained reservation from the matching engine's sequenced trace.
//! Profile and active-reservation indexes use fixed-capacity dense hash storage,
//! so accepted hot-path mutations cannot trigger table growth or rehashing.
//! Private intrusive per-account reservation links permit complete aggregate and
//! membership auditing without a temporary collection; the links are derived
//! process topology and are excluded from public economic state and equality.

use std::fmt;
use std::sync::Arc;

use crate::bounded_hash::BoundedHashMap;
use crate::domain::{AccountId, OrderId, Price, Side};
use crate::instrument::InstrumentDefinition;
use crate::matching::{
    Command, CommandOutcome, CommandPreparation, EventKind, ExecutionReport, MatchingCapacity,
    MatchingError, NewOrder, OrderBook, OrderBookCheckpoint, OrderBookCheckpointError,
    OrderBookLimits, OrderBookLimitsSpec, OrderType, PreparedCommand, RejectReason, ReplaceOrder,
    SelfTradePrevention, Trade,
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

/// Raw finite resource policy for one coupled matching/risk shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskManagedLimitsSpec {
    /// Independently validated matching-engine resource policy.
    pub matching: OrderBookLimits,
    /// Maximum immutable account profiles registered in this shard.
    pub max_registered_accounts: usize,
}

/// Invalid coupled matching/risk resource policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskManagedLimitsError {
    /// The shard could not register any risk profile.
    ZeroRegisteredAccounts,
    /// Matching cannot retain one control revision for every registered profile.
    AccountControlsBelowRegisteredAccounts {
        /// Matching account-control maximum.
        account_controls: usize,
        /// Coupled registered-profile maximum.
        registered_accounts: usize,
    },
}

impl fmt::Display for RiskManagedLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroRegisteredAccounts => {
                formatter.write_str("registered risk-account limit is zero")
            }
            Self::AccountControlsBelowRegisteredAccounts {
                account_controls,
                registered_accounts,
            } => write!(
                formatter,
                "account-control capacity {account_controls} is below registered risk-account capacity {registered_accounts}"
            ),
        }
    }
}

impl std::error::Error for RiskManagedLimitsError {}

/// Validated finite resources for one coupled matching/risk shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskManagedLimits {
    matching: OrderBookLimits,
    max_registered_accounts: usize,
}

impl RiskManagedLimits {
    /// Default maximum immutable account profiles in one shard.
    pub const DEFAULT_MAX_REGISTERED_ACCOUNTS: usize = 65_536;

    /// Validates a coupled resource policy.
    ///
    /// # Errors
    ///
    /// Returns [`RiskManagedLimitsError::ZeroRegisteredAccounts`] for a zero
    /// profile-registry bound.
    pub const fn new(spec: RiskManagedLimitsSpec) -> Result<Self, RiskManagedLimitsError> {
        if spec.max_registered_accounts == 0 {
            return Err(RiskManagedLimitsError::ZeroRegisteredAccounts);
        }
        if spec.matching.max_account_controls() < spec.max_registered_accounts {
            return Err(
                RiskManagedLimitsError::AccountControlsBelowRegisteredAccounts {
                    account_controls: spec.matching.max_account_controls(),
                    registered_accounts: spec.max_registered_accounts,
                },
            );
        }
        Ok(Self {
            matching: spec.matching,
            max_registered_accounts: spec.max_registered_accounts,
        })
    }

    /// Returns the embedded matching-engine resource policy.
    #[must_use]
    pub const fn matching(self) -> OrderBookLimits {
        self.matching
    }

    /// Returns the maximum registered account profiles.
    #[must_use]
    pub const fn max_registered_accounts(self) -> usize {
        self.max_registered_accounts
    }
}

impl Default for RiskManagedLimits {
    fn default() -> Self {
        Self::new(RiskManagedLimitsSpec {
            matching: OrderBookLimits::default(),
            max_registered_accounts: Self::DEFAULT_MAX_REGISTERED_ACCOUNTS,
        })
        .expect("built-in coupled risk limits are valid")
    }
}

fn checkpoint_validation_matching_limits(max_account_controls: usize) -> OrderBookLimits {
    let base = OrderBookLimits::default();
    OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: base.max_active_orders(),
        max_active_accounts: base.max_active_accounts(),
        max_price_levels_per_side: base.max_price_levels_per_side(),
        max_accepted_order_ids: base.max_accepted_order_ids(),
        max_account_controls,
        max_retained_commands: base.max_retained_commands(),
        cancellation_reserve: base.cancellation_reserve(),
        max_report_events: base.max_report_events(),
        max_retained_events: base.max_retained_events(),
        max_prepared_order_selections: base.max_prepared_order_selections(),
    })
    .expect("checkpoint validation matching limits are coherent")
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
    /// The finite immutable profile registry is full.
    ProfileCapacityExhausted {
        /// Configured maximum registered profiles.
        maximum: usize,
    },
    /// Profile metadata was already frozen by the first sequenced command.
    ProfileRegistryLocked,
}

/// Market or limit execution-price domain used by conservative risk valuation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskPriceConstraint {
    /// Execution may occur anywhere inside the immutable instrument collar.
    Market,
    /// Execution is bounded by this validated side-specific limit.
    Limit(Price),
}

/// Domain-neutral deterministic pre-trade rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskRejectReason {
    /// Account entry is blocked.
    AccountBlocked,
    /// The order is not a strict aggregate position reduction.
    ReduceOnly,
    /// Per-order quantity exceeds its limit.
    OrderQuantityLimit,
    /// Per-order conservative notional exceeds its limit.
    OrderNotionalLimit,
    /// Resting order count would exceed its limit.
    OpenOrderCountLimit,
    /// Aggregate resting quantity would exceed its limit.
    OpenQuantityLimit,
    /// Aggregate conservative resting notional would exceed its limit.
    OpenNotionalLimit,
    /// Worst-case long or short position would exceed its limit.
    PositionLimit,
    /// Checked arithmetic could not represent the decision.
    ArithmeticOverflow,
}

impl fmt::Display for RiskRejectReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AccountBlocked => "risk account is blocked",
            Self::ReduceOnly => "order does not strictly reduce aggregate position exposure",
            Self::OrderQuantityLimit => "order quantity exceeds the risk limit",
            Self::OrderNotionalLimit => "order notional exceeds the risk limit",
            Self::OpenOrderCountLimit => "open-order count exceeds the risk limit",
            Self::OpenQuantityLimit => "open quantity exceeds the risk limit",
            Self::OpenNotionalLimit => "open notional exceeds the risk limit",
            Self::PositionLimit => "worst-case position exceeds the risk limit",
            Self::ArithmeticOverflow => "risk arithmetic overflow",
        })
    }
}

impl std::error::Error for RiskRejectReason {}

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
            Self::ProfileCapacityExhausted { maximum } => {
                write!(
                    formatter,
                    "registered risk-account capacity {maximum} is exhausted"
                )
            }
            Self::ProfileRegistryLocked => {
                formatter.write_str("risk-profile registry is locked after command sequencing")
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
///
/// The canonical account image is immutable shared storage. Together with the
/// embedded shared matching checkpoint, this makes checkpoint cloning `O(1)`
/// without allocating or copying semantic rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RiskManagedCheckpoint {
    wal_first_sequence: u64,
    matching: OrderBookCheckpoint,
    accounts: Arc<Vec<RiskAccountCheckpoint>>,
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
        self.accounts.as_slice()
    }

    /// Returns whether two checkpoints share the identical immutable account image.
    #[must_use]
    pub fn shares_account_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.accounts, &other.accounts)
    }

    pub(crate) fn from_parts(
        wal_first_sequence: u64,
        matching: OrderBookCheckpoint,
        accounts: Vec<RiskAccountCheckpoint>,
    ) -> Result<Self, RiskManagedCheckpointError> {
        let checkpoint = Self::from_captured_parts(wal_first_sequence, matching, accounts)?;
        checkpoint.verify_replay_with_limits(checkpoint.validation_limits())?;
        Ok(checkpoint)
    }

    fn from_captured_parts(
        wal_first_sequence: u64,
        matching: OrderBookCheckpoint,
        accounts: Vec<RiskAccountCheckpoint>,
    ) -> Result<Self, RiskManagedCheckpointError> {
        let checkpoint = Self {
            wal_first_sequence,
            matching,
            accounts: Arc::new(accounts),
        };
        checkpoint.validate_structure()?;
        Ok(checkpoint)
    }

    fn validate_structure(&self) -> Result<(), RiskManagedCheckpointError> {
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
        Ok(())
    }

    fn validation_limits(&self) -> RiskManagedLimits {
        let maximum_accounts = self
            .accounts
            .len()
            .max(RiskManagedLimits::DEFAULT_MAX_REGISTERED_ACCOUNTS);
        RiskManagedLimits::new(RiskManagedLimitsSpec {
            matching: checkpoint_validation_matching_limits(maximum_accounts),
            max_registered_accounts: maximum_accounts,
        })
        .expect("checkpoint validation profile capacity is non-zero")
    }

    fn verify_replay_with_limits(
        &self,
        limits: RiskManagedLimits,
    ) -> Result<(), RiskManagedCheckpointError> {
        self.validate_structure()?;
        let direct = self.restore_direct_with_limits(limits)?;
        let mut replay = RiskManagedOrderBook::try_with_limits(self.matching.definition(), limits)
            .map_err(RiskManagedCheckpointError::ConstructionFailed)?;
        for account in self.accounts.iter() {
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
        self.restore_direct_with_limits(RiskManagedLimits::default())
    }

    fn restore_direct_with_limits(
        &self,
        limits: RiskManagedLimits,
    ) -> Result<RiskManagedOrderBook, RiskManagedCheckpointError> {
        if self.accounts.len() > limits.max_registered_accounts() {
            return Err(RiskManagedCheckpointError::new(format!(
                "risk checkpoint account count {} exceeds selected capacity {}",
                self.accounts.len(),
                limits.max_registered_accounts()
            )));
        }
        let book = OrderBook::from_checkpoint_with_limits(&self.matching, limits.matching())?;
        let mut risk = RiskEngine::try_with_limits(self.matching.definition(), limits)
            .map_err(RiskManagedCheckpointError::ConstructionFailed)?;
        for account in self.accounts.iter() {
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
                RiskPriceConstraint::Limit(order.price()),
                order.leaves().lots(),
                false,
            );
        }
        for order in self.matching.dormant_stops() {
            if !risk.accounts.contains_key(&order.account_id()) {
                return Err(RiskManagedCheckpointError::new(format!(
                    "risk checkpoint dormant stop {} has no account profile",
                    order.order_id()
                )));
            }
            let constraint = match order.activation() {
                crate::matching::StopActivation::Market => RiskPriceConstraint::Market,
                crate::matching::StopActivation::Limit(price) => RiskPriceConstraint::Limit(price),
            };
            risk.insert_reservation(
                order.order_id(),
                order.account_id(),
                order.side(),
                constraint,
                order.leaves().lots(),
                true,
            );
        }
        for account in self.accounts.iter() {
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
                .zip(previous.accounts.iter())
                .all(|(current, old)| {
                    current.account_id == old.account_id && current.profile == old.profile
                })
            && self.matching.is_successor_of(&previous.matching)
    }
}

/// An immutable but not yet coupled-replay-verified risk checkpoint capture.
///
/// Capture audits live matching topology, command-derived lineage, profiles,
/// positions, and reservations, but defers deterministic coupled replay. This
/// type has no codec or snapshot payload implementation. Clones share every
/// immutable checkpoint row image and are therefore `O(1)`.
#[derive(Clone)]
pub struct RiskManagedCheckpointCapture {
    checkpoint: RiskManagedCheckpoint,
    limits: RiskManagedLimits,
}

impl RiskManagedCheckpointCapture {
    /// Returns the first physical WAL sequence occupied by the definition.
    #[must_use]
    pub const fn wal_first_sequence(&self) -> u64 {
        self.checkpoint.wal_first_sequence()
    }

    /// Returns the final immutable definition/profile metadata sequence.
    #[must_use]
    pub const fn wal_metadata_sequence(&self) -> u64 {
        self.checkpoint.matching().wal_metadata_sequence()
    }

    /// Returns the completed execution-report WAL boundary represented here.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.checkpoint.generation()
    }

    /// Returns the finite policy under which coupled replay will run.
    #[must_use]
    pub const fn limits(&self) -> RiskManagedLimits {
        self.limits
    }

    /// Returns the captured account/profile cardinality without exposing rows.
    #[must_use]
    pub fn account_count(&self) -> usize {
        self.checkpoint.accounts().len()
    }

    /// Returns the captured active-order/reservation cardinality.
    #[must_use]
    pub fn active_order_count(&self) -> usize {
        self.checkpoint.matching().active_order_count()
    }

    /// Returns the captured command/report cardinality.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.checkpoint.matching().command_count()
    }

    /// Returns whether two captures share every immutable checkpoint row image.
    #[must_use]
    pub fn shares_checkpoint_storage_with(&self, other: &Self) -> bool {
        self.checkpoint
            .shares_account_storage_with(&other.checkpoint)
            && self
                .checkpoint
                .matching()
                .shares_order_storage_with(other.checkpoint.matching())
            && self
                .checkpoint
                .matching()
                .shares_history_storage_with(other.checkpoint.matching())
    }

    /// Consumes the capture and proves deterministic coupled matching/risk replay.
    ///
    /// Verification reconstructs direct positions and reservations, registers
    /// the captured immutable profiles in an isolated shard, requires exact
    /// command/report reproduction, and compares the replayed coupled state with
    /// the direct image. It may run on another thread while the source advances.
    ///
    /// # Errors
    ///
    /// Returns a typed matching/resource/construction failure or `Invalid` for
    /// any structural, exposure, reservation, or deterministic replay divergence.
    pub fn verify(self) -> Result<RiskManagedCheckpoint, RiskManagedCheckpointError> {
        let Self { checkpoint, limits } = self;
        checkpoint.verify_replay_with_limits(limits)?;
        Ok(checkpoint)
    }
}

#[cfg(test)]
impl RiskManagedCheckpointCapture {
    pub(crate) fn corrupt_wal_lineage_for_test(&mut self) {
        self.checkpoint.wal_first_sequence = 0;
    }
}

/// One fallibly reserved coupled-risk checkpoint capture resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskManagedCheckpointResource {
    /// Canonical account/profile/exposure rows owned by a captured checkpoint.
    CaptureAccounts,
}

impl RiskManagedCheckpointResource {
    const fn failure_detail(self) -> &'static str {
        match self {
            Self::CaptureAccounts => "risk checkpoint account capture reservation failed",
        }
    }
}

impl fmt::Display for RiskManagedCheckpointResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CaptureAccounts => "captured account rows",
        })
    }
}

/// Semantic coupled risk/matching checkpoint construction or restoration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RiskManagedCheckpointError {
    /// Coupled checkpoint content or selected-limit contradiction.
    Invalid(String),
    /// The embedded matching checkpoint failed with preserved typed context.
    Matching(OrderBookCheckpointError),
    /// Temporary coupled state required for validation could not be constructed.
    ConstructionFailed(MatchingError),
    /// A complete risk-checkpoint capture resource could not be reserved.
    ResourceReservationFailed {
        /// Capture vector whose construction failed.
        resource: RiskManagedCheckpointResource,
        /// Requested semantic maximum rows.
        maximum: usize,
    },
}

impl RiskManagedCheckpointError {
    fn new(detail: impl Into<String>) -> Self {
        Self::Invalid(detail.into())
    }

    /// Returns a stable diagnostic description.
    #[must_use]
    pub fn detail(&self) -> &str {
        match self {
            Self::Invalid(detail) => detail,
            Self::Matching(error) => error.detail(),
            Self::ConstructionFailed(_) => "risk checkpoint shard construction failed",
            Self::ResourceReservationFailed { resource, .. } => resource.failure_detail(),
        }
    }

    /// Returns the failed risk capture resource, if reservation failed there.
    #[must_use]
    pub const fn resource(&self) -> Option<RiskManagedCheckpointResource> {
        match self {
            Self::ResourceReservationFailed { resource, .. } => Some(*resource),
            Self::Invalid(_) | Self::Matching(_) | Self::ConstructionFailed(_) => None,
        }
    }

    /// Returns the typed embedded matching-checkpoint failure, if present.
    #[must_use]
    pub const fn matching_error(&self) -> Option<&OrderBookCheckpointError> {
        match self {
            Self::Matching(error) => Some(error),
            Self::Invalid(_)
            | Self::ConstructionFailed(_)
            | Self::ResourceReservationFailed { .. } => None,
        }
    }

    /// Returns the underlying temporary coupled-shard construction failure, if present.
    #[must_use]
    pub const fn construction_error(&self) -> Option<MatchingError> {
        match self {
            Self::ConstructionFailed(error) => Some(*error),
            Self::Invalid(_) | Self::Matching(_) | Self::ResourceReservationFailed { .. } => None,
        }
    }

    /// Returns whether this failure is an explicitly typed reservation result.
    #[must_use]
    pub const fn is_resource_exhaustion(&self) -> bool {
        match self {
            Self::ResourceReservationFailed { .. } => true,
            Self::Matching(error) => error.is_resource_exhaustion(),
            Self::Invalid(_) | Self::ConstructionFailed(_) => false,
        }
    }

    /// Returns whether failure occurred before semantic validation could mutate state.
    #[must_use]
    pub const fn is_operational_failure(&self) -> bool {
        match self {
            Self::Matching(error) => error.is_operational_failure(),
            Self::ConstructionFailed(_) | Self::ResourceReservationFailed { .. } => true,
            Self::Invalid(_) => false,
        }
    }
}

impl fmt::Display for RiskManagedCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(detail) => detail.fmt(formatter),
            Self::Matching(error) => error.fmt(formatter),
            Self::ConstructionFailed(error) => {
                write!(
                    formatter,
                    "risk checkpoint shard construction failed: {error}"
                )
            }
            Self::ResourceReservationFailed { resource, maximum } => write!(
                formatter,
                "failed to reserve risk checkpoint {resource} through {maximum} rows"
            ),
        }
    }
}

impl std::error::Error for RiskManagedCheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Matching(error) => Some(error),
            Self::ConstructionFailed(error) => Some(error),
            Self::Invalid(_) | Self::ResourceReservationFailed { .. } => None,
        }
    }
}

impl From<OrderBookCheckpointError> for RiskManagedCheckpointError {
    fn from(error: OrderBookCheckpointError) -> Self {
        Self::Matching(error)
    }
}

impl From<RiskError> for RiskManagedCheckpointError {
    fn from(error: RiskError) -> Self {
        Self::new(error.to_string())
    }
}

fn reserve_risk_checkpoint_vec<T>(
    maximum: usize,
    resource: RiskManagedCheckpointResource,
) -> Result<Vec<T>, RiskManagedCheckpointError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(maximum)
        .map_err(|_| RiskManagedCheckpointError::ResourceReservationFailed { resource, maximum })?;
    Ok(values)
}

#[cfg(test)]
mod checkpoint_resource_tests {
    use super::{
        RiskAccountCheckpoint, RiskManagedCheckpointError, RiskManagedCheckpointResource,
        reserve_risk_checkpoint_vec,
    };

    #[test]
    fn unrepresentable_risk_checkpoint_capture_is_typed_and_nonpoisoning() {
        let resource = RiskManagedCheckpointResource::CaptureAccounts;
        let error =
            reserve_risk_checkpoint_vec::<RiskAccountCheckpoint>(usize::MAX, resource).unwrap_err();
        assert_eq!(
            error,
            RiskManagedCheckpointError::ResourceReservationFailed {
                resource,
                maximum: usize::MAX,
            }
        );
        assert_eq!(error.resource(), Some(resource));
        assert!(error.is_resource_exhaustion());
    }
}

#[derive(Clone, Copy, Debug)]
struct RiskAccount {
    profile: RiskProfile,
    exposure: RiskSnapshot,
    reservation_head: Option<OrderId>,
    reservation_tail: Option<OrderId>,
}

impl PartialEq for RiskAccount {
    fn eq(&self, other: &Self) -> bool {
        self.profile == other.profile && self.exposure == other.exposure
    }
}

impl Eq for RiskAccount {}

/// One constructor-reserved coupled-risk hash index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiskHashIndex {
    /// Immutable account profile and exposure registry.
    AccountProfiles,
    /// Active resting-order reservations.
    ActiveReservations,
}

/// Process-local allocation state of one coupled-risk hash index.
///
/// These counters are operational telemetry and are excluded from financial
/// semantics, equality, checkpoints, and WAL encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RiskHashIndexStatus {
    /// Configured maximum simultaneously retained entries.
    pub configured_entries: usize,
    /// Entry capacity available without growing or rehashing.
    pub allocated_entries: usize,
    /// Entries currently present in the index.
    pub occupied_entries: usize,
}

/// Read-only reservation state for one resting order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReservationSnapshot {
    account_id: AccountId,
    side: Side,
    constraint: RiskPriceConstraint,
    dormant_stop: bool,
    valuation_per_lot: u64,
    quantity_lots: u64,
    notional: u128,
}

#[derive(Clone, Copy, Debug)]
struct ActiveReservation {
    snapshot: ReservationSnapshot,
    previous: Option<OrderId>,
    next: Option<OrderId>,
}

impl PartialEq for ActiveReservation {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot == other.snapshot
    }
}

impl Eq for ActiveReservation {}

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

    /// Returns the conservative market or limit execution constraint.
    #[must_use]
    pub const fn constraint(self) -> RiskPriceConstraint {
        self.constraint
    }

    /// Returns the limit price, or `None` for market-constrained dormant stops.
    #[must_use]
    pub const fn price(self) -> Option<Price> {
        match self.constraint {
            RiskPriceConstraint::Market => None,
            RiskPriceConstraint::Limit(price) => Some(price),
        }
    }

    /// Returns whether this reservation belongs to a dormant stop.
    #[must_use]
    pub const fn is_dormant_stop(self) -> bool {
        self.dormant_stop
    }

    /// Returns the maximum reachable absolute price magnitude reserved per lot.
    #[must_use]
    pub const fn valuation_per_lot(self) -> u64 {
        self.valuation_per_lot
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
    accounts: BoundedHashMap<AccountId, RiskAccount>,
    reservations: BoundedHashMap<OrderId, ActiveReservation>,
    maximum_accounts: usize,
    maximum_reservations: usize,
}

fn try_new_risk_accounts(
    maximum: usize,
) -> Result<BoundedHashMap<AccountId, RiskAccount>, MatchingError> {
    BoundedHashMap::try_new(maximum)
        .map_err(|_| MatchingError::CapacityReservationFailed(MatchingCapacity::RiskAccounts))
}

fn try_new_risk_reservations(
    maximum: usize,
) -> Result<BoundedHashMap<OrderId, ActiveReservation>, MatchingError> {
    BoundedHashMap::try_new(maximum)
        .map_err(|_| MatchingError::CapacityReservationFailed(MatchingCapacity::RiskReservations))
}

impl RiskEngine {
    fn try_with_limits(
        definition: InstrumentDefinition,
        limits: RiskManagedLimits,
    ) -> Result<Self, MatchingError> {
        let maximum_accounts = limits.max_registered_accounts();
        let maximum_reservations = limits.matching().max_active_orders();
        Ok(Self {
            definition,
            accounts: try_new_risk_accounts(maximum_accounts)?,
            reservations: try_new_risk_reservations(maximum_reservations)?,
            maximum_accounts,
            maximum_reservations,
        })
    }

    /// Registers one immutable account profile.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError::DuplicateProfile`] when the account already exists,
    /// or [`RiskError::ProfileCapacityExhausted`] when the constructor-owned
    /// registry is full.
    ///
    /// # Panics
    ///
    /// Panics only if private structural corruption removed constructor-owned
    /// hash headroom without changing the configured bound.
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
        let allocated = self.accounts.capacity();
        assert!(
            self.accounts.len() < allocated,
            "risk construction must reserve profile insertion headroom"
        );
        assert!(
            self.accounts
                .insert(
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
                        reservation_head: None,
                        reservation_tail: None,
                    },
                )
                .is_none(),
            "duplicate profile was rejected before insertion"
        );
        debug_assert_eq!(self.accounts.capacity(), allocated);
        Ok(())
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

    /// Returns one account's current position and reservations.
    #[must_use]
    pub fn snapshot(&self, account_id: AccountId) -> Option<RiskSnapshot> {
        self.accounts.get(&account_id).map(|value| value.exposure)
    }

    /// Returns one active order reservation.
    #[must_use]
    pub fn reservation(&self, order_id: OrderId) -> Option<ReservationSnapshot> {
        self.reservations
            .get(&order_id)
            .map(|reservation| reservation.snapshot)
    }

    /// Returns the active reservation count.
    #[must_use]
    pub fn reservation_count(&self) -> usize {
        self.reservations.len()
    }

    /// Returns the configured maximum active-order reservations.
    #[must_use]
    pub const fn reservation_limit(&self) -> usize {
        self.maximum_reservations
    }

    /// Returns allocation capacity of the active-order reservation index.
    ///
    /// This operational metric can exceed [`Self::reservation_count`] and has
    /// no effect on deterministic risk semantics or persistence.
    #[must_use]
    pub fn reservation_capacity(&self) -> usize {
        self.reservations.capacity()
    }

    fn authorize(&self, command: Command) -> Result<(), RejectReason> {
        match command {
            Command::New(order) => self.authorize_new(order),
            Command::Cancel(_)
            | Command::MassCancel(_)
            | Command::TradingStateControl(_)
            | Command::ExpirySweep(_)
            | Command::StopTriggerSweep(_) => Ok(()),
            Command::Replace(order) => self.authorize_replace(order),
            Command::AccountControl(control) => self
                .accounts
                .contains_key(&control.account_id)
                .then_some(())
                .ok_or(RejectReason::RiskProfileMissing),
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
            order.order_type.stop().is_some()
                || (matches!(order.order_type, OrderType::Limit(_))
                    && order.time_in_force.may_rest()),
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
            .map(|reservation| reservation.snapshot)
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
        evaluate_pretrade_order(
            account.profile,
            baseline,
            side,
            quantity,
            notional,
            may_rest,
        )
        .map_err(matching_risk_rejection)
    }

    fn order_notional(
        &self,
        side: Side,
        order_type: OrderType,
        quantity: u64,
    ) -> Result<u128, RejectReason> {
        let constraint = match order_type {
            OrderType::Market => RiskPriceConstraint::Market,
            OrderType::Limit(price) => RiskPriceConstraint::Limit(price),
            OrderType::Stop { activation, .. } => match activation {
                crate::matching::StopActivation::Market => RiskPriceConstraint::Market,
                crate::matching::StopActivation::Limit(price) => RiskPriceConstraint::Limit(price),
            },
        };
        conservative_order_notional(self.definition, side, constraint, quantity)
            .map_err(matching_risk_rejection)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one trace reducer keeps every matching event's reservation transition exhaustive"
    )]
    fn apply(&mut self, command: Command, report: &ExecutionReport) {
        if !matches!(report.outcome, CommandOutcome::Accepted) {
            return;
        }
        let old_reservation = if let Command::Replace(order) = command {
            Some(
                self.reservations
                    .get(&order.order_id)
                    .expect("accepted replacement must have an existing reservation")
                    .snapshot,
            )
        } else {
            None
        };
        let replacement_side = old_reservation.map(|reservation| reservation.side);
        if let Command::Replace(order) = command {
            self.remove_reservation(order.order_id);
        }

        for event in &report.events {
            match event.kind {
                EventKind::Trade(trade) => self.apply_trade(trade),
                EventKind::StopOrderTriggered { order_id, .. } => {
                    self.activate_reservation(order_id);
                }
                EventKind::OrderCancelled { order_id, .. } => {
                    if self.reservations.contains_key(&order_id) {
                        self.remove_reservation(order_id);
                    }
                }
                EventKind::SelfTradePrevented {
                    aggressor_order_id,
                    resting_order_id,
                    quantity,
                    policy: SelfTradePrevention::DecrementAndCancel,
                    ..
                } => {
                    self.decrement_reservation(resting_order_id, quantity.lots());
                    self.decrement_reservation(aggressor_order_id, quantity.lots());
                }
                _ => {}
            }
        }

        match command {
            Command::New(order) => {
                if report.events.iter().any(|event| {
                    matches!(
                        event.kind,
                        EventKind::StopOrderArmed { order_id, .. } if order_id == order.order_id
                    )
                }) {
                    let constraint = match order.order_type {
                        OrderType::Stop { activation, .. } => match activation {
                            crate::matching::StopActivation::Market => RiskPriceConstraint::Market,
                            crate::matching::StopActivation::Limit(price) => {
                                RiskPriceConstraint::Limit(price)
                            }
                        },
                        OrderType::Market | OrderType::Limit(_) => {
                            unreachable!("StopOrderArmed requires a stop command")
                        }
                    };
                    self.insert_reservation(
                        order.order_id,
                        order.account_id,
                        order.side,
                        constraint,
                        order.quantity.lots(),
                        true,
                    );
                } else if let Some((price, quantity)) = rested_order(report, order.order_id) {
                    self.insert_reservation(
                        order.order_id,
                        order.account_id,
                        order.side,
                        RiskPriceConstraint::Limit(price),
                        quantity,
                        false,
                    );
                }
            }
            Command::Cancel(_)
            | Command::MassCancel(_)
            | Command::AccountControl(_)
            | Command::TradingStateControl(_)
            | Command::ExpirySweep(_)
            | Command::StopTriggerSweep(_) => {}
            Command::Replace(order) => {
                let dormant = old_reservation.is_some_and(|reservation| reservation.dormant_stop);
                if dormant || replacement_retained_priority(report, order.order_id) {
                    self.insert_reservation(
                        order.order_id,
                        order.account_id,
                        replacement_side.expect("replacement side was captured before removal"),
                        RiskPriceConstraint::Limit(order.new_price),
                        order.new_quantity.lots(),
                        dormant,
                    );
                } else if let Some((price, quantity)) = rested_order(report, order.order_id) {
                    self.insert_reservation(
                        order.order_id,
                        order.account_id,
                        replacement_side.expect("replacement side was captured before removal"),
                        RiskPriceConstraint::Limit(price),
                        quantity,
                        false,
                    );
                }
            }
        }
    }

    fn apply_trade(&mut self, trade: Trade) {
        self.decrement_reservation(trade.maker_order_id, trade.quantity.lots());
        self.decrement_reservation(trade.taker_order_id, trade.quantity.lots());
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
        constraint: RiskPriceConstraint,
        quantity_lots: u64,
        dormant_stop: bool,
    ) {
        let valuation_per_lot = conservative_price_magnitude(self.definition, side, constraint);
        let notional = u128::from(valuation_per_lot)
            .checked_mul(u128::from(quantity_lots))
            .expect("pre-trade notional capacity must cover resting leaves");
        let reservation = ReservationSnapshot {
            account_id,
            side,
            constraint,
            dormant_stop,
            valuation_per_lot,
            quantity_lots,
            notional,
        };
        self.append_reservation(order_id, reservation);
    }

    fn append_reservation(&mut self, order_id: OrderId, reservation: ReservationSnapshot) {
        let prepared_capacity = self.reservations.capacity();
        assert!(
            self.reservations.len() < prepared_capacity,
            "risk construction/restoration must reserve insertion headroom"
        );
        assert!(!self.reservations.contains_key(&order_id));
        let previous = self
            .accounts
            .get(&reservation.account_id)
            .expect("authorized account must have a risk profile")
            .reservation_tail;
        if let Some(previous_id) = previous {
            let tail = self
                .reservations
                .get_mut(&previous_id)
                .expect("risk account tail must reference a reservation");
            assert!(tail.next.is_none());
            tail.next = Some(order_id);
        }
        assert!(
            self.reservations
                .insert(
                    order_id,
                    ActiveReservation {
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
            .expect("authorized account must have a risk profile");
        if account.reservation_head.is_none() {
            assert!(previous.is_none());
            account.reservation_head = Some(order_id);
        }
        account.reservation_tail = Some(order_id);
        debug_assert_eq!(self.reservations.capacity(), prepared_capacity);
        let exposure = &mut account.exposure;
        match reservation.side {
            Side::Buy => {
                exposure.open_buy_lots = exposure
                    .open_buy_lots
                    .checked_add(u128::from(reservation.quantity_lots))
                    .expect("authorization reserved aggregate buy capacity");
            }
            Side::Sell => {
                exposure.open_sell_lots = exposure
                    .open_sell_lots
                    .checked_add(u128::from(reservation.quantity_lots))
                    .expect("authorization reserved aggregate sell capacity");
            }
        }
        exposure.open_notional = exposure
            .open_notional
            .checked_add(reservation.notional)
            .expect("authorization reserved aggregate notional capacity");
        exposure.open_orders = exposure
            .open_orders
            .checked_add(1)
            .expect("authorization reserved order-count capacity");
    }

    fn decrement_reservation(&mut self, order_id: OrderId, quantity_lots: u64) {
        let Some(current) = self
            .reservations
            .get(&order_id)
            .map(|reservation| reservation.snapshot)
        else {
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
            current.constraint,
            current.quantity_lots - quantity_lots,
            current.dormant_stop,
        );
    }

    fn activate_reservation(&mut self, order_id: OrderId) {
        let reservation = self
            .reservations
            .get_mut(&order_id)
            .expect("triggered stop must retain its risk reservation");
        assert!(reservation.snapshot.dormant_stop);
        reservation.snapshot.dormant_stop = false;
    }

    fn remove_reservation(&mut self, order_id: OrderId) -> ReservationSnapshot {
        let active = self
            .reservations
            .remove(&order_id)
            .expect("active matching order must have a risk reservation");
        if let Some(previous_id) = active.previous {
            let previous = self
                .reservations
                .get_mut(&previous_id)
                .expect("risk reservation previous link must resolve");
            assert_eq!(previous.next, Some(order_id));
            previous.next = active.next;
        }
        if let Some(next_id) = active.next {
            let next = self
                .reservations
                .get_mut(&next_id)
                .expect("risk reservation next link must resolve");
            assert_eq!(next.previous, Some(order_id));
            next.previous = active.previous;
        }
        let reservation = active.snapshot;
        let account = self
            .accounts
            .get_mut(&reservation.account_id)
            .expect("reserved account must have a risk profile");
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
        let exposure = &mut account.exposure;
        match reservation.side {
            Side::Buy => exposure.open_buy_lots -= u128::from(reservation.quantity_lots),
            Side::Sell => exposure.open_sell_lots -= u128::from(reservation.quantity_lots),
        }
        exposure.open_notional -= reservation.notional;
        exposure.open_orders -= 1;
        reservation
    }

    fn validate(&self) -> Result<(), RiskInvariantViolation> {
        self.validate_resource_bounds()?;
        self.validate_reservation_economics()?;
        self.validate_account_reservation_index()
    }

    fn validate_resource_bounds(&self) -> Result<(), RiskInvariantViolation> {
        if self.accounts.capacity() < self.maximum_accounts {
            return Err(RiskInvariantViolation::new(format!(
                "risk-account hash capacity {} is below its constructor reservation {}",
                self.accounts.capacity(),
                self.maximum_accounts
            )));
        }
        if self.accounts.len() > self.maximum_accounts {
            return Err(RiskInvariantViolation::new(format!(
                "risk-account cardinality {} exceeds configured capacity {}",
                self.accounts.len(),
                self.maximum_accounts
            )));
        }
        if self.reservations.capacity() < self.maximum_reservations {
            return Err(RiskInvariantViolation::new(format!(
                "risk-reservation hash capacity {} is below its constructor reservation {}",
                self.reservations.capacity(),
                self.maximum_reservations
            )));
        }
        if self.reservations.len() > self.maximum_reservations {
            return Err(RiskInvariantViolation::new(format!(
                "risk-reservation cardinality {} exceeds configured capacity {}",
                self.reservations.len(),
                self.maximum_reservations
            )));
        }
        self.accounts.validate_layout().map_err(|detail| {
            RiskInvariantViolation::new(format!("risk-account hash layout is invalid: {detail}"))
        })?;
        self.reservations.validate_layout().map_err(|detail| {
            RiskInvariantViolation::new(format!(
                "risk-reservation hash layout is invalid: {detail}"
            ))
        })?;
        Ok(())
    }

    fn validate_reservation_economics(&self) -> Result<(), RiskInvariantViolation> {
        for (&order_id, active) in self.reservations.iter() {
            let reservation = active.snapshot;
            let expected_valuation = conservative_price_magnitude(
                self.definition,
                reservation.side,
                reservation.constraint,
            );
            let expected_notional = u128::from(expected_valuation)
                .checked_mul(u128::from(reservation.quantity_lots))
                .ok_or_else(|| {
                    RiskInvariantViolation::new(format!(
                        "reservation {order_id} notional cannot be represented"
                    ))
                })?;
            if reservation.quantity_lots == 0
                || reservation.valuation_per_lot != expected_valuation
                || reservation.notional != expected_notional
                || !self.accounts.contains_key(&reservation.account_id)
            {
                return Err(RiskInvariantViolation::new(format!(
                    "reservation {order_id} has invalid ownership, quantity, or notional"
                )));
            }
        }
        Ok(())
    }

    fn validate_account_reservation_index(&self) -> Result<(), RiskInvariantViolation> {
        let mut indexed_reservations = 0_usize;
        for (&account_id, account) in self.accounts.iter() {
            self.validate_account_reservations(account_id, account, &mut indexed_reservations)?;
        }
        if indexed_reservations != self.reservations.len() {
            return Err(RiskInvariantViolation::new(
                "active reservation is absent from its risk-account index",
            ));
        }
        Ok(())
    }

    fn validate_account_reservations(
        &self,
        account_id: AccountId,
        account: &RiskAccount,
        indexed_reservations: &mut usize,
    ) -> Result<(), RiskInvariantViolation> {
        let mut current = account.reservation_head;
        let mut previous = None;
        let mut open_buy_lots = 0_u128;
        let mut open_sell_lots = 0_u128;
        let mut open_notional = 0_u128;
        let mut open_orders = 0_u64;
        while let Some(order_id) = current {
            if *indexed_reservations >= self.reservations.len() {
                return Err(RiskInvariantViolation::new(
                    "reservation occurs more than once or participates in a risk-account cycle",
                ));
            }
            let active = self.reservations.get(&order_id).ok_or_else(|| {
                RiskInvariantViolation::new("risk-account index references an absent reservation")
            })?;
            let reservation = active.snapshot;
            if reservation.account_id != account_id || active.previous != previous {
                return Err(RiskInvariantViolation::new(
                    "reservation account membership or previous link is inconsistent",
                ));
            }
            match reservation.side {
                Side::Buy => {
                    open_buy_lots = checked_audit_add(open_buy_lots, reservation.quantity_lots)?;
                }
                Side::Sell => {
                    open_sell_lots = checked_audit_add(open_sell_lots, reservation.quantity_lots)?;
                }
            }
            open_notional = open_notional
                .checked_add(reservation.notional)
                .ok_or_else(|| {
                    RiskInvariantViolation::new("aggregate reservation notional overflow")
                })?;
            open_orders = open_orders.checked_add(1).ok_or_else(|| {
                RiskInvariantViolation::new("aggregate reservation count overflow")
            })?;
            *indexed_reservations = indexed_reservations
                .checked_add(1)
                .ok_or_else(|| RiskInvariantViolation::new("indexed reservation count overflow"))?;
            previous = Some(order_id);
            current = active.next;
        }
        if previous != account.reservation_tail {
            return Err(RiskInvariantViolation::new(
                "risk-account reservation tail is inconsistent",
            ));
        }
        let actual = account.exposure;
        if (
            actual.open_buy_lots,
            actual.open_sell_lots,
            actual.open_notional,
            actual.open_orders,
        ) != (open_buy_lots, open_sell_lots, open_notional, open_orders)
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
        Self::with_limits(definition, RiskManagedLimits::default())
    }

    /// Creates an empty risk-managed book under explicit coupled resource limits.
    ///
    /// # Panics
    ///
    /// Panics when requested constructor-time matching/risk hash reservation or
    /// process-local book identity allocation fails.
    #[must_use]
    pub fn with_limits(definition: InstrumentDefinition, limits: RiskManagedLimits) -> Self {
        Self::try_with_limits(definition, limits)
            .expect("matching/risk capacity reservation must succeed under A12")
    }

    /// Creates an empty risk-managed book with fallible complete matching/risk reservation.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError::CapacityReservationFailed`] when a configured
    /// matching, profile, or risk-reservation hash capacity cannot be represented or allocated, or
    /// [`MatchingError::BookInstanceIdExhausted`] when process-local book
    /// identity is exhausted.
    pub fn try_with_limits(
        definition: InstrumentDefinition,
        limits: RiskManagedLimits,
    ) -> Result<Self, MatchingError> {
        let risk = RiskEngine::try_with_limits(definition, limits)?;
        let book = OrderBook::try_with_limits(definition, limits.matching())?;
        Ok(Self { book, risk })
    }

    /// Registers an account before it enters orders.
    ///
    /// # Errors
    ///
    /// Returns [`RiskError::ProfileRegistryLocked`] after the first sequenced
    /// command, [`RiskError::DuplicateProfile`] for a repeated account, or
    /// [`RiskError::ProfileCapacityExhausted`] when the registry is full.
    ///
    /// # Panics
    ///
    /// Panics only if private risk-index corruption contradicts the successful
    /// constructor reservation.
    pub fn register_account(
        &mut self,
        account_id: AccountId,
        profile: RiskProfile,
    ) -> Result<(), RiskError> {
        if self.book.retained_command_count() != 0 {
            return Err(RiskError::ProfileRegistryLocked);
        }
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
        match self.prepare(command)? {
            CommandPreparation::Replay(report) => Ok(report),
            CommandPreparation::Ready(prepared) => self.commit(prepared),
        }
    }

    /// Prepares matching operational/core checks against immutable coupled state.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError`] for command collision, configured matching
    /// capacity, or exhausted sequence capacity.
    pub fn prepare(&self, command: Command) -> Result<CommandPreparation, MatchingError> {
        let preparation = self.book.prepare(command)?;
        if matches!(&preparation, CommandPreparation::Ready(_)) {
            debug_assert_eq!(
                self.risk.reservation_count(),
                self.book.active_order_count(),
                "coupled preparation requires matching/risk cardinality parity"
            );
        }
        Ok(preparation)
    }

    /// Applies risk authorization and commits one generation-bound command.
    ///
    /// Core business rejection stored in the preparation precedes risk. An
    /// accepted non-replay trace updates matching and risk state exactly once.
    ///
    /// # Errors
    ///
    /// Returns [`MatchingError`] if the preparation is stale, collides, or
    /// contradicts a preflighted sequence invariant.
    pub fn commit(&mut self, prepared: PreparedCommand) -> Result<ExecutionReport, MatchingError> {
        let command = prepared.command();
        let core_rejection = prepared.core_rejection();
        let risk_rejection = if core_rejection.is_none() {
            self.risk.authorize(command).err()
        } else {
            None
        };
        let apply_risk = core_rejection.is_none() && risk_rejection.is_none();
        let report = self.book.commit_with_gate(prepared, risk_rejection)?;
        if apply_risk && !report.replayed {
            self.risk.apply(command, &report);
        }
        debug_assert!(self.validate().is_ok());
        Ok(report)
    }

    /// Captures and independently audits coupled matching, positions, and reservations.
    ///
    /// # Errors
    ///
    /// Returns [`RiskManagedCheckpointError::ResourceReservationFailed`] when
    /// canonical account rows cannot be reserved, preserves embedded matching
    /// and temporary-shard construction failures, or reports divergent live
    /// state, physical WAL boundaries, account state, or deterministic replay.
    pub fn checkpoint(
        &self,
        wal_first_sequence: u64,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<RiskManagedCheckpoint, RiskManagedCheckpointError> {
        self.capture_checkpoint_candidate(wal_first_sequence, wal_metadata_sequence, wal_sequence)?
            .verify()
    }

    /// Captures immutable coupled state without deterministic history replay.
    ///
    /// The writer-side phase audits live matching/risk structure and complete
    /// command-derived lineage, captures canonical account and matching rows,
    /// and proves that direct reconstruction equals the live coupled state. The
    /// returned value cannot be encoded or persisted until consumed by
    /// [`RiskManagedCheckpointCapture::verify`].
    ///
    /// # Errors
    ///
    /// Returns a typed capture/validation reservation failure or `Invalid` for
    /// a live structural, lineage, WAL-boundary, exposure, or reservation
    /// contradiction.
    pub fn capture_checkpoint_candidate(
        &self,
        wal_first_sequence: u64,
        wal_metadata_sequence: u64,
        wal_sequence: u64,
    ) -> Result<RiskManagedCheckpointCapture, RiskManagedCheckpointError> {
        self.validate()
            .map_err(|error| RiskManagedCheckpointError::new(error.detail()))?;
        let matching = self
            .book
            .checkpoint_state(wal_metadata_sequence, wal_sequence)?;
        let mut accounts = reserve_risk_checkpoint_vec(
            self.risk.accounts.len(),
            RiskManagedCheckpointResource::CaptureAccounts,
        )?;
        for (&account_id, account) in self.risk.accounts.iter() {
            accounts.push(RiskAccountCheckpoint {
                account_id,
                profile: account.profile,
                exposure: account.exposure,
            });
        }
        accounts.sort_unstable_by_key(|value| value.account_id);
        let checkpoint =
            RiskManagedCheckpoint::from_captured_parts(wal_first_sequence, matching, accounts)?;
        let restored = checkpoint.restore_direct_with_limits(self.limits())?;
        if restored != *self {
            return Err(RiskManagedCheckpointError::new(
                "risk checkpoint direct state differs from live coupled state",
            ));
        }
        Ok(RiskManagedCheckpointCapture {
            checkpoint,
            limits: self.limits(),
        })
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

    /// Restores coupled matching/risk state under explicit current resource limits.
    ///
    /// # Errors
    ///
    /// Returns [`RiskManagedCheckpointError`] when semantic state is invalid or
    /// any recovered matching or profile cardinality exceeds the selected limits.
    pub fn from_checkpoint_with_limits(
        checkpoint: &RiskManagedCheckpoint,
        limits: RiskManagedLimits,
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

    /// Returns the complete current coupled resource policy.
    #[must_use]
    pub const fn limits(&self) -> RiskManagedLimits {
        RiskManagedLimits {
            matching: self.book.limits(),
            max_registered_accounts: self.risk.maximum_accounts,
        }
    }

    /// Cross-checks matching structure, every reservation, and account aggregates.
    ///
    /// A successful audit allocates no heap storage and uses `O(1)` auxiliary
    /// space. For `A` registered accounts, `O` active orders/reservations, and
    /// `P` initialized price-arena slots, risk membership and book/risk parity
    /// take expected `O(A + O)` time, while the embedded matching-book audit has
    /// the `O(O + P log P)` bound documented by [`OrderBook::validate`]. A full
    /// adversarial hash-collision cluster can make the hash-backed risk work
    /// quadratic. Constructing a human-readable failure detail can allocate.
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
        for order in self.book.active_order_states() {
            let order = order.map_err(|error| RiskInvariantViolation::new(error.detail()))?;
            let reservation = self
                .risk
                .reservations
                .get(&order.order_id)
                .map(|active| active.snapshot)
                .ok_or_else(|| {
                    RiskInvariantViolation::new(format!(
                        "active matching order {} has no risk reservation",
                        order.order_id
                    ))
                })?;
            if order.account_id != reservation.account_id
                || order.side != reservation.side
                || RiskPriceConstraint::Limit(order.price) != reservation.constraint
                || reservation.dormant_stop
                || order.leaves_quantity.lots() != reservation.quantity_lots
            {
                return Err(RiskInvariantViolation::new(format!(
                    "reservation {} differs from matching order",
                    order.order_id
                )));
            }
        }
        for order in self.book.dormant_stop_states() {
            let reservation = self
                .risk
                .reservations
                .get(&order.order_id)
                .map(|active| active.snapshot)
                .ok_or_else(|| {
                    RiskInvariantViolation::new(format!(
                        "dormant stop {} has no risk reservation",
                        order.order_id
                    ))
                })?;
            let constraint = match order.activation {
                crate::matching::StopActivation::Market => RiskPriceConstraint::Market,
                crate::matching::StopActivation::Limit(price) => RiskPriceConstraint::Limit(price),
            };
            if order.account_id != reservation.account_id
                || order.side != reservation.side
                || constraint != reservation.constraint
                || !reservation.dormant_stop
                || order.leaves_quantity.lots() != reservation.quantity_lots
            {
                return Err(RiskInvariantViolation::new(format!(
                    "reservation {} differs from dormant stop",
                    order.order_id
                )));
            }
        }
        Ok(())
    }
}

/// Returns the maximum reachable absolute execution-price magnitude per lot.
///
/// A buy limit spans the instrument minimum through its limit; a sell limit
/// spans its limit through the instrument maximum. Market interest spans the
/// complete immutable collar. The supplied limit must already satisfy the
/// instrument definition.
#[must_use]
pub fn conservative_price_magnitude(
    definition: InstrumentDefinition,
    side: Side,
    constraint: RiskPriceConstraint,
) -> u64 {
    let rules = definition.price_rules();
    match (side, constraint) {
        (Side::Buy, RiskPriceConstraint::Limit(limit)) => limit
            .raw()
            .unsigned_abs()
            .max(rules.minimum().raw().unsigned_abs()),
        (Side::Sell, RiskPriceConstraint::Limit(limit)) => limit
            .raw()
            .unsigned_abs()
            .max(rules.maximum().raw().unsigned_abs()),
        (_, RiskPriceConstraint::Market) => rules
            .minimum()
            .raw()
            .unsigned_abs()
            .max(rules.maximum().raw().unsigned_abs()),
    }
}

/// Computes maximum reachable absolute raw-price-times-lots notional.
///
/// # Errors
///
/// Returns [`RiskRejectReason::ArithmeticOverflow`] when the product cannot fit
/// in `u128`.
pub fn conservative_order_notional(
    definition: InstrumentDefinition,
    side: Side,
    constraint: RiskPriceConstraint,
    quantity_lots: u64,
) -> Result<u128, RiskRejectReason> {
    u128::from(conservative_price_magnitude(definition, side, constraint))
        .checked_mul(u128::from(quantity_lots))
        .ok_or(RiskRejectReason::ArithmeticOverflow)
}

/// Evaluates one order against immutable account limits and current exposure.
///
/// `notional` must be the complete conservative notional for `quantity_lots`.
/// When `may_rest` is false, resting count/quantity/notional are not added, but
/// worst-case position still includes the complete incoming quantity because
/// it may execute immediately.
///
/// # Errors
///
/// Returns the first deterministic limit or arithmetic rejection.
pub fn evaluate_pretrade_order(
    profile: RiskProfile,
    baseline: RiskSnapshot,
    side: Side,
    quantity_lots: u64,
    notional: u128,
    may_rest: bool,
) -> Result<(), RiskRejectReason> {
    let limits = profile.limits;
    match profile.state {
        AccountRiskState::Active => {}
        AccountRiskState::Blocked => return Err(RiskRejectReason::AccountBlocked),
        AccountRiskState::ReduceOnly => {
            if !strictly_reduces(baseline, side, quantity_lots) {
                return Err(RiskRejectReason::ReduceOnly);
            }
        }
    }
    if quantity_lots > limits.max_order_quantity_lots() {
        return Err(RiskRejectReason::OrderQuantityLimit);
    }
    if notional > limits.max_order_notional() {
        return Err(RiskRejectReason::OrderNotionalLimit);
    }
    let added_open_orders = u64::from(may_rest);
    let added_open_quantity = if may_rest {
        u128::from(quantity_lots)
    } else {
        0
    };
    let added_open_notional = if may_rest { notional } else { 0 };
    let open_orders = baseline
        .open_orders
        .checked_add(added_open_orders)
        .ok_or(RiskRejectReason::ArithmeticOverflow)?;
    if open_orders > limits.max_open_orders() {
        return Err(RiskRejectReason::OpenOrderCountLimit);
    }
    let total_open = baseline
        .open_buy_lots
        .checked_add(baseline.open_sell_lots)
        .and_then(|value| value.checked_add(added_open_quantity))
        .ok_or(RiskRejectReason::ArithmeticOverflow)?;
    if total_open > limits.max_open_quantity_lots() {
        return Err(RiskRejectReason::OpenQuantityLimit);
    }
    let open_notional = baseline
        .open_notional
        .checked_add(added_open_notional)
        .ok_or(RiskRejectReason::ArithmeticOverflow)?;
    if open_notional > limits.max_open_notional() {
        return Err(RiskRejectReason::OpenNotionalLimit);
    }
    let (worst_buy, worst_sell) = match side {
        Side::Buy => (
            baseline
                .open_buy_lots
                .checked_add(u128::from(quantity_lots))
                .ok_or(RiskRejectReason::ArithmeticOverflow)?,
            baseline.open_sell_lots,
        ),
        Side::Sell => (
            baseline.open_buy_lots,
            baseline
                .open_sell_lots
                .checked_add(u128::from(quantity_lots))
                .ok_or(RiskRejectReason::ArithmeticOverflow)?,
        ),
    };
    if !worst_case_position_within_limits(baseline.position_lots, worst_buy, worst_sell, limits) {
        return Err(RiskRejectReason::PositionLimit);
    }
    Ok(())
}

const fn matching_risk_rejection(reason: RiskRejectReason) -> RejectReason {
    match reason {
        RiskRejectReason::AccountBlocked => RejectReason::RiskAccountBlocked,
        RiskRejectReason::ReduceOnly => RejectReason::RiskReduceOnly,
        RiskRejectReason::OrderQuantityLimit => RejectReason::RiskOrderQuantityLimit,
        RiskRejectReason::OrderNotionalLimit => RejectReason::RiskOrderNotionalLimit,
        RiskRejectReason::OpenOrderCountLimit => RejectReason::RiskOpenOrderCountLimit,
        RiskRejectReason::OpenQuantityLimit => RejectReason::RiskOpenQuantityLimit,
        RiskRejectReason::OpenNotionalLimit => RejectReason::RiskOpenNotionalLimit,
        RiskRejectReason::PositionLimit => RejectReason::RiskPositionLimit,
        RiskRejectReason::ArithmeticOverflow => RejectReason::RiskArithmeticOverflow,
    }
}

/// Returns whether an executed signed position is inside immutable limits.
#[must_use]
pub fn position_within_limits(position: i128, limits: RiskLimits) -> bool {
    if position >= 0 {
        position.unsigned_abs() <= limits.max_long_position_lots()
    } else {
        position.unsigned_abs() <= limits.max_short_position_lots()
    }
}

/// Returns whether independent full execution of all open buys or all open
/// sells remains inside the long and short limits.
#[must_use]
pub fn worst_case_position_within_limits(
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
    let Ok(long_limit) = i128::try_from(limits.max_long_position_lots()) else {
        return false;
    };
    let Ok(short_limit) = i128::try_from(limits.max_short_position_lots()) else {
        return false;
    };
    long <= long_limit && short >= -short_limit
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

#[cfg(test)]
mod reservation_capacity_tests {
    use super::{
        AccountRiskState, MatchingCapacity, MatchingError, RiskEngine, RiskLimitSpec, RiskLimits,
        RiskManagedLimits, RiskManagedLimitsSpec, RiskPriceConstraint, RiskProfile, RiskSnapshot,
        try_new_risk_accounts, try_new_risk_reservations,
    };
    use crate::instrument::{
        InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
        QuantityRules, ReserveOrderRules, TradingState,
    };
    use crate::matching::OrderBookLimits;
    use crate::{
        AccountId, AssetId, InstrumentId, InstrumentVersion, OrderId, Price, Side, TimestampNs,
    };

    fn definition() -> InstrumentDefinition {
        InstrumentDefinition::new(InstrumentSpec {
            instrument_id: InstrumentId::new(1).unwrap(),
            version: InstrumentVersion::new(1).unwrap(),
            effective_from: TimestampNs::from_unix_nanos(0),
            symbol: InstrumentSymbol::new("RISK-CAPACITY").unwrap(),
            kind: InstrumentKind::Spot,
            base_asset_id: AssetId::new(1).unwrap(),
            quote_asset_id: AssetId::new(2).unwrap(),
            price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
            quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
            reserve: ReserveOrderRules::disabled(),
            hidden_orders_supported: false,
            base_units_per_lot: 1,
            quote_units_per_price_unit: 1,
            trading_state: TradingState::Open,
        })
        .unwrap()
    }

    fn risk_with_reservations() -> RiskEngine {
        let coupled = RiskManagedLimits::new(RiskManagedLimitsSpec {
            matching: OrderBookLimits::default(),
            max_registered_accounts: 1,
        })
        .unwrap();
        let mut risk = RiskEngine::try_with_limits(definition(), coupled).unwrap();
        let profile = RiskProfile::new(
            AccountRiskState::Active,
            0,
            RiskLimits::new(RiskLimitSpec {
                max_order_quantity_lots: 1_000,
                max_order_notional: 1_000_000,
                max_open_orders: 8,
                max_open_quantity_lots: 8_000,
                max_open_notional: 8_000_000,
                max_long_position_lots: i128::MAX.unsigned_abs(),
                max_short_position_lots: i128::MAX.unsigned_abs(),
            })
            .unwrap(),
        )
        .unwrap();
        let account_id = AccountId::new(1).unwrap();
        risk.register_account(account_id, profile).unwrap();
        for order_id in [10_u64, 20] {
            risk.insert_reservation(
                OrderId::new(order_id).unwrap(),
                account_id,
                Side::Buy,
                RiskPriceConstraint::Limit(Price::from_raw(100)),
                2,
                false,
            );
        }
        risk.validate().unwrap();
        risk
    }

    #[test]
    fn unrepresentable_reservation_capacity_is_a_typed_failure() {
        assert!(matches!(
            try_new_risk_reservations(usize::MAX),
            Err(MatchingError::CapacityReservationFailed(
                MatchingCapacity::RiskReservations
            ))
        ));
    }

    #[test]
    fn unrepresentable_account_capacity_is_a_typed_failure() {
        assert!(matches!(
            try_new_risk_accounts(usize::MAX),
            Err(MatchingError::CapacityReservationFailed(
                MatchingCapacity::RiskAccounts
            ))
        ));
    }

    #[test]
    fn invariant_validation_rejects_lost_reservation_headroom() {
        let limits = OrderBookLimits::default();
        let coupled = RiskManagedLimits::new(RiskManagedLimitsSpec {
            matching: limits,
            max_registered_accounts: 1,
        })
        .unwrap();
        let mut risk = RiskEngine::try_with_limits(definition(), coupled).unwrap();
        assert!(risk.reservations.capacity() >= limits.max_active_orders());
        risk.reservations.shrink_to_fit();
        let error = risk
            .validate()
            .expect_err("reservation capacity below its constructor bound is invalid");
        assert!(error.detail().contains("risk-reservation hash capacity"));
    }

    #[test]
    fn invariant_validation_rejects_lost_account_headroom() {
        let coupled = RiskManagedLimits::new(RiskManagedLimitsSpec {
            matching: OrderBookLimits::default(),
            max_registered_accounts: 2,
        })
        .unwrap();
        let mut risk = RiskEngine::try_with_limits(definition(), coupled).unwrap();
        risk.accounts.shrink_to_fit();
        let error = risk
            .validate()
            .expect_err("account capacity below its constructor bound is invalid");
        assert!(error.detail().contains("risk-account hash capacity"));
    }

    #[test]
    fn allocation_free_audit_rejects_cycles_and_unlinked_reservations() {
        let head = OrderId::new(10).unwrap();
        let tail = OrderId::new(20).unwrap();

        let mut cycle = risk_with_reservations();
        cycle.reservations.get_mut(&tail).unwrap().next = Some(head);
        assert!(
            cycle
                .validate()
                .unwrap_err()
                .detail()
                .contains("risk-account cycle")
        );

        let mut unlinked = risk_with_reservations();
        unlinked.reservations.get_mut(&head).unwrap().next = None;
        let account = unlinked
            .accounts
            .get_mut(&AccountId::new(1).unwrap())
            .unwrap();
        account.reservation_tail = Some(head);
        account.exposure = RiskSnapshot {
            position_lots: 0,
            open_buy_lots: 2,
            open_sell_lots: 0,
            open_notional: 200,
            open_orders: 1,
        };
        assert!(
            unlinked
                .validate()
                .unwrap_err()
                .detail()
                .contains("absent from its risk-account index")
        );
    }

    #[test]
    fn topology_is_nonsemantic_and_survives_every_removal_shape() {
        let expected = risk_with_reservations();
        let mut reordered = risk_with_reservations();
        let first_id = OrderId::new(10).unwrap();
        let first = reordered.remove_reservation(first_id);
        reordered.append_reservation(first_id, first);
        reordered.validate().unwrap();
        assert_eq!(reordered, expected);

        let third = OrderId::new(30).unwrap();
        reordered.insert_reservation(
            third,
            AccountId::new(1).unwrap(),
            Side::Buy,
            RiskPriceConstraint::Limit(Price::from_raw(100)),
            4,
            false,
        );
        reordered.remove_reservation(OrderId::new(20).unwrap());
        reordered.validate().unwrap();
        reordered.decrement_reservation(third, 1);
        reordered.validate().unwrap();
        reordered.remove_reservation(first_id);
        reordered.validate().unwrap();
        reordered.remove_reservation(third);
        reordered.validate().unwrap();
        assert_eq!(reordered.reservation_count(), 0);
    }
}
