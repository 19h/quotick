//! Atomic, finitely bounded multi-asset double-entry accounting and trade settlement.
//!
//! Authoritative balance, transaction, and reversal indexes plus journal order
//! reserve their complete configured generation layouts before mutation.
//! Command-exact preparation owns all commit inputs in flat vectors and bounded
//! dense/open-addressed overlays; accepted commits allocate no storage. Ledger
//! queries expose fallible flat-buffer construction where output ownership is
//! unavoidable.

use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use crate::auction_book::CallAuctionTrade;
use crate::auction_engine::{
    CallAuctionCommandOutcome, CallAuctionEventKind, CallAuctionExecutionReport,
};
use crate::bounded_hash::{BoundedHashError, BoundedHashMap, BoundedHashSet};
use crate::domain::{
    AccountId, AccountingDate, AssetId, InstrumentId, InstrumentVersion, Price, Quantity,
    ReconciliationId, Side, TimestampNs, TradeId, TransactionId,
};
use crate::instrument::InstrumentDefinition;
use crate::matching::Trade;

pub use crate::ledger_magnitude::LedgerMagnitude;

const DEFAULT_MAX_LEDGER_BALANCE_KEYS: usize = 65_536;
const DEFAULT_MAX_LEDGER_TRANSACTIONS: usize = 65_536;
const DEFAULT_MAX_LEDGER_REVERSALS: usize = 32_768;
const DEFAULT_MAX_LEDGER_RECORDS: usize = 65_536;
const DEFAULT_MAX_LEDGER_POSTINGS_PER_TRANSACTION: usize = 256;
const DEFAULT_MAX_LEDGER_TRANSACTIONS_PER_RECORD: usize = 1_024;
const DEFAULT_MAX_LEDGER_RETAINED_POSTINGS: usize = 262_144;

/// One finite authoritative ledger resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerResource {
    /// Non-zero `(account, asset)` balance identities.
    BalanceKeys,
    /// Retained transaction entries and their idempotency identities.
    Transactions,
    /// Original-to-reversal lineage identities.
    Reversals,
    /// Indivisible sequenced journal events.
    Records,
    /// Posting legs retained across all transaction entries.
    RetainedPostings,
    /// Posting legs accepted in one transaction entry.
    PostingsPerTransaction,
    /// Transaction entries accepted in one journal event.
    TransactionsPerRecord,
}

/// Fallible temporary storage reserved before authoritative ledger mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerPreparationResource {
    /// Posting vector for a generated exact reversal.
    ReversalPostings,
    /// Two-leg posting vector for one explicit trade fee.
    CallAuctionFeePostings,
    /// Canonical fee assessments for one complete call-auction report.
    CallAuctionFeeAssessments,
    /// Explicit fees calculated for one complete call-auction settlement.
    CallAuctionCalculatedFees,
    /// DVP entries constructed for one complete call-auction report.
    CallAuctionSettlementEntries,
    /// Reversal and replacement entries for one call-auction correction.
    CallAuctionCorrectionEntries,
    /// Identity set used to validate one batch.
    BatchIdentitySet,
    /// Pending transaction lookup for ordered batch semantics.
    PendingTransactions,
    /// Pending reversal lookup for ordered batch semantics.
    PendingReversals,
    /// Reversal lineage additions carried by a prepared batch.
    NewReversals,
    /// Flat signed balance terms for a prepared event.
    BalanceTerms,
    /// Exact final balance image carried by a prepared event.
    BalanceUpdates,
}

/// Fallibly allocated storage used by read-only ledger queries and audits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerQueryResource {
    /// Flat `(asset, amount)` terms sorted for trial-balance aggregation.
    TrialBalanceTerms,
    /// Canonical per-asset trial-balance result vector.
    TrialBalanceOutput,
}

impl fmt::Display for LedgerQueryResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::TrialBalanceTerms => "trial-balance terms",
            Self::TrialBalanceOutput => "trial-balance output",
        })
    }
}

/// Allocation/layout failure while constructing a read-only ledger query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerQueryError {
    /// One exact query buffer could not be represented or reserved.
    ReservationFailed {
        /// Query resource whose construction failed.
        resource: LedgerQueryResource,
        /// Requested exact maximum entries.
        maximum: usize,
    },
}

impl fmt::Display for LedgerQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReservationFailed { resource, maximum } => write!(
                formatter,
                "ledger {resource} could not reserve {maximum} entries"
            ),
        }
    }
}

impl std::error::Error for LedgerQueryError {}

/// Journal/index inconsistency detected by a borrowed ledger-history query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerHistoryError {
    /// A zero-based journal position cannot be represented as a one-based sequence.
    SequenceOverflow {
        /// Zero-based journal position.
        index: usize,
    },
    /// One journal member is absent from the authoritative transaction index.
    MissingTransaction {
        /// One-based ledger-event sequence.
        sequence: u64,
        /// Missing transaction identity.
        transaction_id: TransactionId,
    },
    /// One indexed transaction contradicts its journal sequence, identity, or content.
    TransactionMismatch {
        /// One-based ledger-event sequence.
        sequence: u64,
        /// Contradictory transaction identity.
        transaction_id: TransactionId,
    },
}

impl fmt::Display for LedgerHistoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SequenceOverflow { index } => {
                write!(
                    formatter,
                    "ledger journal index {index} has no representable sequence"
                )
            }
            Self::MissingTransaction {
                sequence,
                transaction_id,
            } => write!(
                formatter,
                "ledger record {sequence} transaction {transaction_id} is absent from the index"
            ),
            Self::TransactionMismatch {
                sequence,
                transaction_id,
            } => write!(
                formatter,
                "ledger record {sequence} transaction {transaction_id} contradicts the index"
            ),
        }
    }
}

impl std::error::Error for LedgerHistoryError {}

/// Failure while reconstructing one balance at a committed ledger generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerAsOfError {
    /// The requested record boundary has not been committed.
    GenerationOutOfRange {
        /// Requested ledger generation.
        requested: u64,
        /// Current committed ledger generation.
        current: u64,
    },
    /// Retained journal and transaction-index state contradict one another.
    History(LedgerHistoryError),
    /// One record's atomic balance effect cannot be represented as `i128`.
    BalanceOverflow {
        /// One-based ledger-event sequence.
        sequence: u64,
        /// Account whose reconstructed balance overflowed.
        account_id: AccountId,
        /// Asset whose reconstructed balance overflowed.
        asset_id: AssetId,
    },
    /// Full-history reconstruction disagrees with the current balance index.
    CurrentBalanceMismatch {
        /// Account whose balance differs.
        account_id: AccountId,
        /// Asset whose balance differs.
        asset_id: AssetId,
        /// Balance reconstructed from complete retained history.
        reconstructed: i128,
        /// Balance stored in the current balance index.
        indexed: i128,
    },
}

impl fmt::Display for LedgerAsOfError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationOutOfRange { requested, current } => write!(
                formatter,
                "ledger generation {requested} is beyond current generation {current}"
            ),
            Self::History(error) => error.fmt(formatter),
            Self::BalanceOverflow {
                sequence,
                account_id,
                asset_id,
            } => write!(
                formatter,
                "ledger record {sequence} overflows balance for account {account_id} asset {asset_id}"
            ),
            Self::CurrentBalanceMismatch {
                account_id,
                asset_id,
                reconstructed,
                indexed,
            } => write!(
                formatter,
                "ledger reconstructed balance {reconstructed} for account {account_id} asset {asset_id} differs from indexed balance {indexed}"
            ),
        }
    }
}

impl std::error::Error for LedgerAsOfError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::History(error) => Some(error),
            Self::GenerationOutOfRange { .. }
            | Self::BalanceOverflow { .. }
            | Self::CurrentBalanceMismatch { .. } => None,
        }
    }
}

impl From<LedgerHistoryError> for LedgerAsOfError {
    fn from(error: LedgerHistoryError) -> Self {
        Self::History(error)
    }
}

impl fmt::Display for LedgerPreparationResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ReversalPostings => "reversal posting preparation",
            Self::CallAuctionFeePostings => "call-auction fee-posting preparation",
            Self::CallAuctionFeeAssessments => "call-auction fee-assessment preparation",
            Self::CallAuctionCalculatedFees => "call-auction calculated-fee preparation",
            Self::CallAuctionSettlementEntries => "call-auction settlement-entry preparation",
            Self::CallAuctionCorrectionEntries => "call-auction correction-entry preparation",
            Self::BatchIdentitySet => "batch identity validation",
            Self::PendingTransactions => "batch pending-transaction preparation",
            Self::PendingReversals => "batch pending-reversal preparation",
            Self::NewReversals => "batch reversal-lineage preparation",
            Self::BalanceTerms => "balance-term preparation",
            Self::BalanceUpdates => "balance-update preparation",
        })
    }
}

impl fmt::Display for LedgerResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BalanceKeys => "non-zero balance keys",
            Self::Transactions => "retained transactions",
            Self::Reversals => "reversal lineages",
            Self::Records => "journal records",
            Self::RetainedPostings => "retained posting legs",
            Self::PostingsPerTransaction => "posting legs per transaction",
            Self::TransactionsPerRecord => "transactions per record",
        })
    }
}

/// Requested finite resource envelope for one ledger generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerLimitsSpec {
    /// Maximum concurrently non-zero `(account, asset)` balances.
    pub max_balance_keys: usize,
    /// Maximum retained transaction entries.
    pub max_transactions: usize,
    /// Maximum retained original-to-reversal mappings.
    pub max_reversals: usize,
    /// Maximum indivisible sequenced journal events.
    pub max_records: usize,
    /// Maximum posting legs in one transaction entry.
    pub max_postings_per_transaction: usize,
    /// Maximum transaction entries grouped in one journal event.
    pub max_transactions_per_record: usize,
    /// Maximum posting legs retained by all transaction entries.
    pub max_retained_postings: usize,
}

impl Default for LedgerLimitsSpec {
    fn default() -> Self {
        Self {
            max_balance_keys: DEFAULT_MAX_LEDGER_BALANCE_KEYS,
            max_transactions: DEFAULT_MAX_LEDGER_TRANSACTIONS,
            max_reversals: DEFAULT_MAX_LEDGER_REVERSALS,
            max_records: DEFAULT_MAX_LEDGER_RECORDS,
            max_postings_per_transaction: DEFAULT_MAX_LEDGER_POSTINGS_PER_TRANSACTION,
            max_transactions_per_record: DEFAULT_MAX_LEDGER_TRANSACTIONS_PER_RECORD,
            max_retained_postings: DEFAULT_MAX_LEDGER_RETAINED_POSTINGS,
        }
    }
}

/// Contradiction in a requested ledger resource envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerLimitError {
    /// Every finite maximum must be positive.
    ZeroMaximum(LedgerResource),
    /// A record cannot admit more transactions than the generation retains.
    TransactionsPerRecordExceedTransactions {
        /// Per-record maximum.
        per_record: usize,
        /// Generation transaction maximum.
        transactions: usize,
    },
    /// One transaction cannot admit more postings than the generation retains.
    PostingsPerTransactionExceedRetainedPostings {
        /// Per-transaction maximum.
        per_transaction: usize,
        /// Generation posting maximum.
        retained_postings: usize,
    },
    /// The record maximum cannot exceed the transaction maximum because each
    /// non-empty record introduces at least one transaction.
    RecordsExceedTransactions {
        /// Record maximum.
        records: usize,
        /// Transaction maximum.
        transactions: usize,
    },
    /// Reversal lineage cannot outnumber retained transactions.
    ReversalsExceedTransactions {
        /// Reversal maximum.
        reversals: usize,
        /// Transaction maximum.
        transactions: usize,
    },
}

impl fmt::Display for LedgerLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroMaximum(resource) => write!(formatter, "ledger {resource} maximum is zero"),
            Self::TransactionsPerRecordExceedTransactions {
                per_record,
                transactions,
            } => write!(
                formatter,
                "ledger transactions-per-record maximum {per_record} exceeds transaction maximum {transactions}"
            ),
            Self::PostingsPerTransactionExceedRetainedPostings {
                per_transaction,
                retained_postings,
            } => write!(
                formatter,
                "ledger postings-per-transaction maximum {per_transaction} exceeds retained-posting maximum {retained_postings}"
            ),
            Self::RecordsExceedTransactions {
                records,
                transactions,
            } => write!(
                formatter,
                "ledger record maximum {records} exceeds transaction maximum {transactions}"
            ),
            Self::ReversalsExceedTransactions {
                reversals,
                transactions,
            } => write!(
                formatter,
                "ledger reversal maximum {reversals} exceeds transaction maximum {transactions}"
            ),
        }
    }
}

impl std::error::Error for LedgerLimitError {}

/// Validated finite ledger limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerLimits(LedgerLimitsSpec);

impl LedgerLimits {
    /// Returns the validated specification.
    #[must_use]
    pub const fn spec(self) -> LedgerLimitsSpec {
        self.0
    }
}

impl TryFrom<LedgerLimitsSpec> for LedgerLimits {
    type Error = LedgerLimitError;

    fn try_from(spec: LedgerLimitsSpec) -> Result<Self, Self::Error> {
        for (resource, maximum) in [
            (LedgerResource::BalanceKeys, spec.max_balance_keys),
            (LedgerResource::Transactions, spec.max_transactions),
            (LedgerResource::Reversals, spec.max_reversals),
            (LedgerResource::Records, spec.max_records),
            (
                LedgerResource::PostingsPerTransaction,
                spec.max_postings_per_transaction,
            ),
            (
                LedgerResource::TransactionsPerRecord,
                spec.max_transactions_per_record,
            ),
            (LedgerResource::RetainedPostings, spec.max_retained_postings),
        ] {
            if maximum == 0 {
                return Err(LedgerLimitError::ZeroMaximum(resource));
            }
        }
        if spec.max_transactions_per_record > spec.max_transactions {
            return Err(LedgerLimitError::TransactionsPerRecordExceedTransactions {
                per_record: spec.max_transactions_per_record,
                transactions: spec.max_transactions,
            });
        }
        if spec.max_postings_per_transaction > spec.max_retained_postings {
            return Err(
                LedgerLimitError::PostingsPerTransactionExceedRetainedPostings {
                    per_transaction: spec.max_postings_per_transaction,
                    retained_postings: spec.max_retained_postings,
                },
            );
        }
        if spec.max_records > spec.max_transactions {
            return Err(LedgerLimitError::RecordsExceedTransactions {
                records: spec.max_records,
                transactions: spec.max_transactions,
            });
        }
        if spec.max_reversals > spec.max_transactions {
            return Err(LedgerLimitError::ReversalsExceedTransactions {
                reversals: spec.max_reversals,
                transactions: spec.max_transactions,
            });
        }
        Ok(Self(spec))
    }
}

/// Failure while validating or reserving a complete ledger state envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerConstructionError {
    /// Requested maxima contradict one another.
    InvalidLimits(LedgerLimitError),
    /// One authoritative resource could not reserve its complete layout.
    ReservationFailed {
        /// Resource whose reservation failed.
        resource: LedgerResource,
        /// Requested semantic maximum.
        maximum: usize,
    },
}

impl fmt::Display for LedgerConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits(error) => error.fmt(formatter),
            Self::ReservationFailed { resource, maximum } => write!(
                formatter,
                "ledger {resource} could not reserve its complete maximum {maximum}"
            ),
        }
    }
}

impl std::error::Error for LedgerConstructionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidLimits(error) => Some(error),
            Self::ReservationFailed { .. } => None,
        }
    }
}

/// Authoritative fixed hash-index allocation telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerHashIndexStatus {
    /// Occupied semantic entries.
    pub occupied_entries: usize,
    /// Configured semantic maximum.
    pub maximum_entries: usize,
    /// Constructor-allocated dense-entry capacity.
    pub allocated_entries: usize,
    /// Initialized open-addressed lookup buckets.
    pub initialized_buckets: usize,
}

/// Selects one authoritative ledger hash index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerHashIndex {
    /// Non-zero balance index.
    BalanceKeys,
    /// Transaction/idempotency index.
    Transactions,
    /// Reversal-lineage index.
    Reversals,
}

/// Constructor-owned journal-vector allocation telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerJournalStatus {
    /// Occupied indivisible records.
    pub occupied_records: usize,
    /// Configured semantic maximum.
    pub maximum_records: usize,
    /// Constructor-allocated element capacity.
    pub allocated_records: usize,
}

fn reserve_ledger_map<K, V>(
    maximum: usize,
    resource: LedgerResource,
) -> Result<BoundedHashMap<K, V>, LedgerConstructionError>
where
    K: Eq + Hash,
{
    BoundedHashMap::try_new(maximum).map_err(|_: BoundedHashError| {
        LedgerConstructionError::ReservationFailed { resource, maximum }
    })
}

fn reserve_ledger_preparation_map<K, V>(
    maximum: usize,
    resource: LedgerPreparationResource,
) -> Result<BoundedHashMap<K, V>, LedgerError>
where
    K: Eq + Hash,
{
    BoundedHashMap::try_new(maximum).map_err(|_| LedgerError::PreparationAllocationFailed(resource))
}

fn reserve_ledger_preparation_set<K>(
    maximum: usize,
    resource: LedgerPreparationResource,
) -> Result<BoundedHashSet<K>, LedgerError>
where
    K: Eq + Hash,
{
    BoundedHashSet::try_new(maximum).map_err(|_| LedgerError::PreparationAllocationFailed(resource))
}

fn reserve_ledger_preparation_vec<T>(
    maximum: usize,
    resource: LedgerPreparationResource,
) -> Result<Vec<T>, LedgerError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(maximum)
        .map_err(|_| LedgerError::PreparationAllocationFailed(resource))?;
    Ok(values)
}

fn reserve_ledger_query_vec<T>(
    maximum: usize,
    resource: LedgerQueryResource,
) -> Result<Vec<T>, LedgerQueryError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(maximum)
        .map_err(|_| LedgerQueryError::ReservationFailed { resource, maximum })?;
    Ok(values)
}

/// One signed change to an account's balance in one asset.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Posting {
    /// Account whose balance changes.
    pub account_id: AccountId,
    /// Asset denomination of the amount.
    pub asset_id: AssetId,
    /// Signed integer amount in the asset's smallest ledger unit.
    pub amount: i128,
}

/// A balanced, immutable group of postings applied atomically.
///
/// Cloning an entry is `O(1)` and allocation-free because the canonical
/// posting vector is shared. Construction creates the shared-owner control
/// block after semantic validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalEntry {
    /// Idempotency key for the entry.
    transaction_id: TransactionId,
    /// Human- and machine-correlatable source reference.
    reference: u64,
    /// Financial value date; absent for administrative control entries.
    effective_date: Option<AccountingDate>,
    /// Monotonic UTC booking timestamp.
    recorded_at: TimestampNs,
    /// All entry legs.
    postings: Arc<Vec<Posting>>,
    /// Accounting lifecycle semantics carried by this entry.
    kind: LedgerEntryKind,
}

/// Stable accounting lifecycle classification for a journal entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerEntryKind {
    /// An ordinary independently authorized accounting entry.
    Standard,
    /// The exact economic inverse of one previously posted entry.
    Reversal {
        /// Transaction whose postings are reversed.
        reversed_transaction_id: TransactionId,
    },
    /// Advances the inclusive closed accounting-date boundary.
    PeriodClose {
        /// Last date on which new financial entries are prohibited.
        closed_through: AccountingDate,
    },
    /// Moves or removes the inclusive closed accounting-date boundary.
    PeriodReopen {
        /// Replacement boundary; `None` reopens all dates.
        new_closed_through: Option<AccountingDate>,
    },
}

/// One indivisible accounting correction containing an exact reversal and its replacement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerCorrection {
    reversal: JournalEntry,
    replacement: JournalEntry,
}

/// An ordered group of journal entries committed as one indivisible ledger event.
///
/// Entry order is authoritative for booking timestamps, accounting-period
/// controls, and reversal lineage. Balance effects are observed only as the
/// final aggregate image of the complete batch. Cloning a batch is `O(1)` and
/// allocation-free; its immutable entry vector and every nested posting vector
/// are shared.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerBatch {
    entries: Arc<Vec<JournalEntry>>,
}

/// Economic quantity to which one call-auction fee rate is applied.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionFeeBasis {
    /// Executed quantity in instrument-defined lots.
    TradedLots,
    /// Delivered base asset in its smallest ledger unit.
    BaseAssetUnits,
    /// Absolute quote notional in its smallest ledger unit.
    QuoteNotionalMagnitude,
}

/// Deterministic rounding applied to a non-negative fee-rate result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionFeeRounding {
    /// Round toward zero.
    Down,
    /// Round away from zero when a fractional remainder exists.
    Up,
    /// Round to the nearest integer, resolving an exact tie to an even integer.
    NearestTiesToEven,
}

/// Transfer direction derived from the sign of a call-auction fee rate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CallAuctionFeeDirection {
    /// The participant supplies an amount to the fee account.
    ParticipantPays,
    /// The fee account supplies the participant rebate.
    ParticipantReceives,
}

/// One exact rational fee or rebate rule for one call-auction side.
///
/// A positive numerator charges the participant and a negative numerator pays
/// the participant a rebate. The calculated magnitude is rounded and then
/// clamped to the positive minimum and optional maximum.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionFeeRule {
    asset_id: AssetId,
    basis: CallAuctionFeeBasis,
    rate_numerator: i64,
    rate_denominator: u64,
    rounding: CallAuctionFeeRounding,
    minimum_amount: i128,
    maximum_amount: Option<i128>,
}

/// Immutable call-auction fee policy bound to one instrument definition.
///
/// The schedule revision is assessment provenance. Calculated transfers retain
/// their trade references in ordinary ledger entries; distribution,
/// authentication, authorization, and durable schedule-registry storage remain
/// external lifecycle responsibilities.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionFeeSchedule {
    revision: u64,
    instrument_id: InstrumentId,
    instrument_version: InstrumentVersion,
    fee_account_id: AccountId,
    buy_rule: Option<CallAuctionFeeRule>,
    sell_rule: Option<CallAuctionFeeRule>,
}

/// One calculated fee or rebate before assignment of a transaction identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionFeeAssessment {
    schedule_revision: u64,
    trade_id: TradeId,
    side: Side,
    participant_account_id: AccountId,
    fee_account_id: AccountId,
    asset_id: AssetId,
    basis: CallAuctionFeeBasis,
    basis_amount: u128,
    direction: CallAuctionFeeDirection,
    amount: i128,
}

/// One explicit positive fee transfer bound to one call-auction trade.
///
/// The amount is denominated in the asset's smallest ledger unit. The debit
/// account supplies the amount and the credit account receives it. Rebate
/// semantics use the opposite account direction rather than a negative amount.
/// Fee calculation, account selection, and asset selection are authoritative
/// caller inputs. Authorization remains an external lifecycle responsibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionFee {
    transaction_id: TransactionId,
    trade_id: TradeId,
    debit_account_id: AccountId,
    credit_account_id: AccountId,
    asset_id: AssetId,
    amount: i128,
}

/// One complete accepted call-auction uncross mapped to one ledger event.
///
/// A single counterparty pair without fees is represented by one journal
/// entry. Multiple DVP entries or any explicit fee transfer are represented by
/// one atomic ordered batch, so ledger readers never observe a partially
/// settled uncross or its fees.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionSettlement {
    record: CallAuctionSettlementRecord,
}

/// One exact full-settlement bust or atomic reversal-plus-replacement event.
///
/// Every original DVP and fee transaction is reversed in its canonical
/// settlement order. A replacement correction appends one independently
/// validated [`CallAuctionSettlement`] after all reversals. The original
/// settlement value is retained so application can prove that its transactions
/// were committed together as that exact ledger event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallAuctionSettlementCorrection {
    original: CallAuctionSettlement,
    record: CallAuctionSettlementRecord,
    reversal_transaction_count: usize,
    replacement_transaction_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum CallAuctionSettlementRecord {
    Entry(JournalEntry),
    Batch(LedgerBatch),
}

impl CallAuctionSettlementRecord {
    fn from_entries(mut entries: Vec<JournalEntry>) -> Result<Self, LedgerError> {
        if entries.len() == 1 {
            let Some(entry) = entries.pop() else {
                unreachable!("one call-auction entry disappeared")
            };
            Ok(Self::Entry(entry))
        } else if entries.is_empty() {
            Err(LedgerError::CallAuctionSettlementReportInvalid)
        } else {
            LedgerBatch::new(entries).map(Self::Batch)
        }
    }

    fn entries(&self) -> &[JournalEntry] {
        match self {
            Self::Entry(entry) => std::slice::from_ref(entry),
            Self::Batch(batch) => batch.entries(),
        }
    }

    fn transaction_count(&self) -> usize {
        self.entries().len()
    }

    fn primary_transaction_id(&self) -> TransactionId {
        self.entries()[0].transaction_id
    }
}

/// One sequenced ledger event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerRecord {
    /// One independently committed financial or administrative entry.
    Entry(JournalEntry),
    /// One atomic reversal-plus-replacement correction.
    Correction(LedgerCorrection),
    /// One atomic ordered group of two or more entries.
    Batch(LedgerBatch),
}

impl LedgerRecord {
    /// Returns the number of transaction identifiers introduced by this event.
    #[must_use]
    pub fn transaction_count(&self) -> usize {
        match self {
            Self::Entry(_) => 1,
            Self::Correction(_) => 2,
            Self::Batch(batch) => batch.entries.len(),
        }
    }

    /// Returns the event's first transaction identifier.
    #[must_use]
    pub fn primary_transaction_id(&self) -> TransactionId {
        match self {
            Self::Entry(entry) => entry.transaction_id,
            Self::Correction(correction) => correction.reversal.transaction_id,
            Self::Batch(batch) => batch.entries[0].transaction_id,
        }
    }
}

/// Borrowed canonical content of one sequenced ledger event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerRecordView<'a> {
    /// One independently committed financial or administrative entry.
    Entry(&'a JournalEntry),
    /// One atomic reversal-plus-replacement correction.
    Correction {
        /// Exact reversal entry.
        reversal: &'a JournalEntry,
        /// Exact replacement entry.
        replacement: &'a JournalEntry,
    },
    /// One atomic ordered group of two or more entries.
    Batch(&'a LedgerBatch),
}

impl<'a> LedgerRecordView<'a> {
    /// Returns the number of transaction entries introduced by this event.
    #[must_use]
    pub fn transaction_count(self) -> usize {
        match self {
            Self::Entry(_) => 1,
            Self::Correction { .. } => 2,
            Self::Batch(batch) => batch.entries().len(),
        }
    }

    /// Returns the event's first transaction identifier.
    #[must_use]
    pub fn primary_transaction_id(self) -> TransactionId {
        match self {
            Self::Entry(entry) => entry.transaction_id(),
            Self::Correction { reversal, .. } => reversal.transaction_id(),
            Self::Batch(batch) => batch.primary_transaction_id(),
        }
    }

    /// Returns one transaction entry in event-declared order.
    #[must_use]
    pub fn transaction(self, index: usize) -> Option<&'a JournalEntry> {
        match (self, index) {
            (Self::Entry(entry), 0) => Some(entry),
            (Self::Correction { reversal, .. }, 0) => Some(reversal),
            (Self::Correction { replacement, .. }, 1) => Some(replacement),
            (Self::Batch(batch), index) => batch.entries().get(index),
            (Self::Entry(_) | Self::Correction { .. }, _) => None,
        }
    }

    /// Iterates transaction entries in event-declared order without allocation.
    #[must_use]
    pub fn transactions(self) -> LedgerRecordTransactions<'a> {
        LedgerRecordTransactions {
            record: self,
            front: 0,
            back: self.transaction_count(),
        }
    }
}

impl From<LedgerRecordView<'_>> for LedgerRecord {
    fn from(record: LedgerRecordView<'_>) -> Self {
        match record {
            LedgerRecordView::Entry(entry) => Self::Entry(entry.clone()),
            LedgerRecordView::Correction {
                reversal,
                replacement,
            } => Self::Correction(LedgerCorrection {
                reversal: reversal.clone(),
                replacement: replacement.clone(),
            }),
            LedgerRecordView::Batch(batch) => Self::Batch(batch.clone()),
        }
    }
}

/// Double-ended exact-size transaction iterator for one borrowed ledger event.
#[derive(Clone, Debug)]
pub struct LedgerRecordTransactions<'a> {
    record: LedgerRecordView<'a>,
    front: usize,
    back: usize,
}

impl<'a> Iterator for LedgerRecordTransactions<'a> {
    type Item = &'a JournalEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            return None;
        }
        let index = self.front;
        self.front += 1;
        self.record.transaction(index)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.back - self.front;
        (remaining, Some(remaining))
    }
}

impl DoubleEndedIterator for LedgerRecordTransactions<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front == self.back {
            return None;
        }
        self.back -= 1;
        self.record.transaction(self.back)
    }
}

impl ExactSizeIterator for LedgerRecordTransactions<'_> {}
impl std::iter::FusedIterator for LedgerRecordTransactions<'_> {}

/// One borrowed ledger event paired with its stable one-based sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetainedLedgerRecord<'a> {
    sequence: u64,
    record: LedgerRecordView<'a>,
}

impl<'a> RetainedLedgerRecord<'a> {
    /// Returns the stable one-based ledger-event sequence.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    /// Returns the borrowed canonical event content.
    #[must_use]
    pub const fn record(self) -> LedgerRecordView<'a> {
        self.record
    }
}

/// One borrowed posting selected for an account-and-asset statement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerStatementLine<'a> {
    retained: RetainedLedgerRecord<'a>,
    transaction_index: usize,
    entry: &'a JournalEntry,
    posting: &'a Posting,
}

impl<'a> LedgerStatementLine<'a> {
    /// Returns the stable one-based sequence of the enclosing ledger event.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.retained.sequence()
    }

    /// Returns the complete borrowed event containing this posting.
    #[must_use]
    pub const fn record(self) -> LedgerRecordView<'a> {
        self.retained.record()
    }

    /// Returns the zero-based transaction position inside the enclosing event.
    #[must_use]
    pub const fn transaction_index(self) -> usize {
        self.transaction_index
    }

    /// Returns the canonical transaction entry containing this posting.
    #[must_use]
    pub const fn entry(self) -> &'a JournalEntry {
        self.entry
    }

    /// Returns the exact signed posting selected by the statement key.
    #[must_use]
    pub const fn posting(self) -> &'a Posting {
        self.posting
    }
}

#[derive(Clone, Debug)]
struct LedgerStatementRecord<'a> {
    retained: Option<RetainedLedgerRecord<'a>>,
    error: Option<LedgerHistoryError>,
    account_id: AccountId,
    asset_id: AssetId,
    front: usize,
    back: usize,
}

impl<'a> LedgerStatementRecord<'a> {
    fn new(
        retained: Result<RetainedLedgerRecord<'a>, LedgerHistoryError>,
        account_id: AccountId,
        asset_id: AssetId,
    ) -> Self {
        match retained {
            Ok(retained) => Self {
                retained: Some(retained),
                error: None,
                account_id,
                asset_id,
                front: 0,
                back: retained.record().transaction_count(),
            },
            Err(error) => Self {
                retained: None,
                error: Some(error),
                account_id,
                asset_id,
                front: 0,
                back: 0,
            },
        }
    }

    fn line(&self, transaction_index: usize) -> Option<LedgerStatementLine<'a>> {
        let retained = self.retained?;
        let entry = retained.record().transaction(transaction_index)?;
        let posting = entry.posting(self.account_id, self.asset_id)?;
        Some(LedgerStatementLine {
            retained,
            transaction_index,
            entry,
            posting,
        })
    }
}

impl<'a> Iterator for LedgerStatementRecord<'a> {
    type Item = Result<LedgerStatementLine<'a>, LedgerHistoryError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = self.error.take() {
            return Some(Err(error));
        }
        while self.front < self.back {
            let transaction_index = self.front;
            self.front += 1;
            if let Some(line) = self.line(transaction_index) {
                return Some(Ok(line));
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.error.is_some() {
            return (1, Some(1));
        }
        (0, Some(self.back - self.front))
    }
}

impl DoubleEndedIterator for LedgerStatementRecord<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(error) = self.error.take() {
            return Some(Err(error));
        }
        while self.front < self.back {
            self.back -= 1;
            if let Some(line) = self.line(self.back) {
                return Some(Ok(line));
            }
        }
        None
    }
}

impl std::iter::FusedIterator for LedgerStatementRecord<'_> {}

/// Ledger validation or arithmetic failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerError {
    /// Fewer than two non-zero posting legs were supplied.
    TooFewPostings,
    /// A zero posting would add no accounting information.
    ZeroPosting,
    /// The same account and asset appeared more than once in an entry.
    DuplicateAccountAsset,
    /// Positive and negative posting totals for this asset differed.
    Unbalanced {
        /// Unbalanced asset.
        asset_id: AssetId,
        /// Exact sum of positive posting amounts.
        positive_total: Box<LedgerMagnitude>,
        /// Exact absolute sum of negative posting amounts.
        negative_total: Box<LedgerMagnitude>,
    },
    /// An account balance or fixed-width settlement calculation overflowed.
    ArithmeticOverflow,
    /// A valid new event would exceed one finite generation resource.
    CapacityExceeded {
        /// Exhausted resource.
        resource: LedgerResource,
        /// Configured semantic maximum.
        maximum: usize,
        /// Exact cardinality the event would produce.
        attempted: usize,
    },
    /// Temporary storage could not be reserved before mutation.
    PreparationAllocationFailed(LedgerPreparationResource),
    /// A transaction identifier was reused for different content.
    TransactionIdCollision(TransactionId),
    /// Buyer and seller were the same account.
    SelfSettlement,
    /// Base and quote assets were identical.
    IdenticalSettlementAssets,
    /// A settlement conversion factor was zero.
    ZeroSettlementMultiplier,
    /// Trade and settlement definition instrument identifiers differed.
    SettlementInstrumentMismatch,
    /// Trade and settlement definition versions differed.
    SettlementVersionMismatch,
    /// The report was not a complete accepted uncross event trace.
    CallAuctionSettlementReportInvalid,
    /// Global transaction identifiers did not cover every emitted trade.
    CallAuctionSettlementTransactionCountMismatch {
        /// Supplied global transaction identifiers.
        transaction_count: usize,
        /// Counterparty pairs declared by the uncross completion event.
        trade_count: usize,
    },
    /// A fee transfer amount was zero or negative.
    FeeAmountNotPositive(i128),
    /// A fee transfer used one account as both debit and credit.
    FeeAccountsIdentical(AccountId),
    /// A fee rule used a zero signed rate numerator.
    FeeRateNumeratorZero,
    /// A fee rule used a zero rational denominator.
    FeeRateDenominatorZero,
    /// A fee rule used a zero or negative minimum amount.
    FeeMinimumNotPositive(i128),
    /// A fee rule maximum was below its positive minimum.
    FeeMaximumBelowMinimum {
        /// Positive minimum amount.
        minimum: i128,
        /// Invalid maximum amount.
        maximum: i128,
    },
    /// A fee schedule used revision zero.
    FeeScheduleRevisionZero,
    /// A fee schedule configured neither a buy rule nor a sell rule.
    FeeScheduleEmpty,
    /// Fee schedule and settlement definition instrument identifiers differed.
    FeeScheduleInstrumentMismatch,
    /// Fee schedule and settlement definition versions differed.
    FeeScheduleVersionMismatch,
    /// Fee transaction identifiers did not cover every calculated assessment.
    CallAuctionFeeTransactionCountMismatch {
        /// Supplied global fee transaction identifiers.
        transaction_count: usize,
        /// Canonical fee assessments calculated from the report.
        assessment_count: usize,
    },
    /// A fee did not bind to the next trade in canonical report order.
    CallAuctionFeeTradeMismatch(TradeId),
    /// Reversal transaction identifiers did not cover the complete settlement.
    CallAuctionCorrectionTransactionCountMismatch {
        /// Supplied reversal transaction identifiers.
        reversal_transaction_count: usize,
        /// DVP and fee transactions in the original settlement.
        settlement_transaction_count: usize,
    },
    /// The original single-entry settlement was committed in another grouping.
    CallAuctionCorrectionOriginalGroupingMismatch(TransactionId),
    /// Ledger state changed after an entry was prepared and before commit.
    StalePreparation,
    /// The transaction targeted by a reversal was not present.
    ReversalTargetMissing(TransactionId),
    /// The target already had a committed reversal.
    TransactionAlreadyReversed {
        /// Original transaction.
        original_transaction_id: TransactionId,
        /// Existing reversal transaction.
        reversal_transaction_id: TransactionId,
    },
    /// Reversal postings were not the exact signed inverse of the target.
    InvalidReversalPostings(TransactionId),
    /// A target leg was `i128::MIN` and therefore had no representable inverse.
    NonReversibleAmount {
        /// Original transaction.
        original_transaction_id: TransactionId,
        /// Account on the non-reversible leg.
        account_id: AccountId,
        /// Asset on the non-reversible leg.
        asset_id: AssetId,
    },
    /// Financial postings were attempted without a value date.
    FinancialEntryMissingEffectiveDate,
    /// An administrative period-control entry carried a financial value date.
    ControlEntryHasEffectiveDate,
    /// An administrative period-control entry carried posting legs.
    ControlEntryHasPostings,
    /// A financial posting targeted an inclusively closed date.
    AccountingPeriodClosed {
        /// Entry value date.
        effective_date: AccountingDate,
        /// Inclusive close boundary.
        closed_through: AccountingDate,
    },
    /// A close did not advance beyond the current boundary.
    PeriodCloseNotAdvancing {
        /// Existing boundary, if any.
        current_closed_through: Option<AccountingDate>,
        /// Requested boundary.
        proposed_closed_through: AccountingDate,
    },
    /// Reopen was requested while no date was closed.
    AccountingPeriodAlreadyOpen,
    /// A reopen did not move the boundary backward.
    InvalidPeriodReopen {
        /// Existing boundary.
        current_closed_through: AccountingDate,
        /// Requested replacement boundary.
        proposed_closed_through: Option<AccountingDate>,
    },
    /// Booking timestamps regressed within the authoritative journal.
    RecordedTimestampRegression {
        /// Last committed timestamp.
        previous: TimestampNs,
        /// Proposed timestamp.
        proposed: TimestampNs,
    },
    /// Administrative controls have no financial posting effect to reverse.
    NonFinancialReversalTarget(TransactionId),
    /// A correction replacement was not an ordinary financial entry.
    CorrectionReplacementNotStandard(TransactionId),
    /// The first correction member was not a reversal entry.
    CorrectionFirstEntryNotReversal(TransactionId),
    /// Reversal and replacement reused one transaction identifier.
    CorrectionTransactionIdsNotDistinct {
        /// Correction reversal transaction.
        reversal_transaction_id: TransactionId,
        /// Corrected replacement transaction.
        replacement_transaction_id: TransactionId,
    },
    /// One member of a correction was already committed outside that exact event.
    CorrectionAlreadyPartiallyCommitted(TransactionId),
    /// An atomic batch contained fewer than two entries.
    BatchTooFewEntries,
    /// An atomic batch repeated one transaction identifier.
    BatchDuplicateTransaction(TransactionId),
    /// One member of a batch was already committed outside that exact event.
    BatchAlreadyPartiallyCommitted(TransactionId),
}

impl fmt::Display for LedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooFewPostings => {
                formatter.write_str("journal entry requires at least two postings")
            }
            Self::ZeroPosting => formatter.write_str("zero-value postings are not permitted"),
            Self::DuplicateAccountAsset => {
                formatter.write_str("an entry may contain only one posting per account and asset")
            }
            Self::Unbalanced { .. } => format_unbalanced_ledger_error(self, formatter),
            Self::ArithmeticOverflow => formatter.write_str("ledger arithmetic overflow"),
            resource_error @ (Self::CapacityExceeded { .. }
            | Self::PreparationAllocationFailed(_)) => {
                format_ledger_resource_error(resource_error, formatter)
            }
            Self::TransactionIdCollision(id) => {
                write!(
                    formatter,
                    "transaction identifier {id} was reused with different content"
                )
            }
            settlement_error @ (Self::SelfSettlement
            | Self::IdenticalSettlementAssets
            | Self::ZeroSettlementMultiplier
            | Self::SettlementInstrumentMismatch
            | Self::SettlementVersionMismatch
            | Self::CallAuctionSettlementReportInvalid
            | Self::CallAuctionSettlementTransactionCountMismatch { .. }
            | Self::FeeAmountNotPositive(_)
            | Self::FeeAccountsIdentical(_)
            | Self::FeeRateNumeratorZero
            | Self::FeeRateDenominatorZero
            | Self::FeeMinimumNotPositive(_)
            | Self::FeeMaximumBelowMinimum { .. }
            | Self::FeeScheduleRevisionZero
            | Self::FeeScheduleEmpty
            | Self::FeeScheduleInstrumentMismatch
            | Self::FeeScheduleVersionMismatch
            | Self::CallAuctionFeeTransactionCountMismatch { .. }
            | Self::CallAuctionFeeTradeMismatch(_)
            | Self::CallAuctionCorrectionTransactionCountMismatch { .. }
            | Self::CallAuctionCorrectionOriginalGroupingMismatch(_)) => {
                format_settlement_error(settlement_error, formatter)
            }
            lifecycle_error @ (Self::StalePreparation
            | Self::ReversalTargetMissing(_)
            | Self::TransactionAlreadyReversed { .. }
            | Self::InvalidReversalPostings(_)
            | Self::NonReversibleAmount { .. }
            | Self::CorrectionReplacementNotStandard(_)
            | Self::CorrectionFirstEntryNotReversal(_)
            | Self::CorrectionTransactionIdsNotDistinct { .. }
            | Self::CorrectionAlreadyPartiallyCommitted(_)
            | Self::BatchTooFewEntries
            | Self::BatchDuplicateTransaction(_)
            | Self::BatchAlreadyPartiallyCommitted(_)) => {
                format_ledger_lifecycle_error(lifecycle_error, formatter)
            }
            period_error @ (Self::FinancialEntryMissingEffectiveDate
            | Self::ControlEntryHasEffectiveDate
            | Self::ControlEntryHasPostings
            | Self::AccountingPeriodClosed { .. }
            | Self::PeriodCloseNotAdvancing { .. }
            | Self::AccountingPeriodAlreadyOpen
            | Self::InvalidPeriodReopen { .. }
            | Self::RecordedTimestampRegression { .. }
            | Self::NonFinancialReversalTarget(_)) => {
                format_accounting_period_error(period_error, formatter)
            }
        }
    }
}

fn format_ledger_lifecycle_error(
    error: &LedgerError,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    match error {
        LedgerError::StalePreparation => formatter.write_str("prepared journal entry is stale"),
        LedgerError::ReversalTargetMissing(transaction_id) => write!(
            formatter,
            "reversal target transaction {transaction_id} is not committed"
        ),
        LedgerError::TransactionAlreadyReversed {
            original_transaction_id,
            reversal_transaction_id,
        } => write!(
            formatter,
            "transaction {original_transaction_id} was already reversed by {reversal_transaction_id}"
        ),
        LedgerError::InvalidReversalPostings(transaction_id) => write!(
            formatter,
            "reversal postings are not the exact inverse of transaction {transaction_id}"
        ),
        LedgerError::NonReversibleAmount {
            original_transaction_id,
            account_id,
            asset_id,
        } => write!(
            formatter,
            "transaction {original_transaction_id} has a non-reversible i128::MIN leg for account {account_id}, asset {asset_id}"
        ),
        LedgerError::CorrectionReplacementNotStandard(transaction_id) => write!(
            formatter,
            "correction replacement transaction {transaction_id} is not a standard financial entry"
        ),
        LedgerError::CorrectionFirstEntryNotReversal(transaction_id) => write!(
            formatter,
            "correction transaction {transaction_id} is not a reversal entry"
        ),
        LedgerError::CorrectionTransactionIdsNotDistinct {
            reversal_transaction_id,
            replacement_transaction_id,
        } => write!(
            formatter,
            "correction reversal {reversal_transaction_id} and replacement {replacement_transaction_id} must be distinct"
        ),
        LedgerError::CorrectionAlreadyPartiallyCommitted(transaction_id) => write!(
            formatter,
            "correction transaction {transaction_id} is already committed outside the exact correction event"
        ),
        LedgerError::BatchTooFewEntries => {
            formatter.write_str("ledger batch requires at least two entries")
        }
        LedgerError::BatchDuplicateTransaction(transaction_id) => write!(
            formatter,
            "ledger batch repeats transaction identifier {transaction_id}"
        ),
        LedgerError::BatchAlreadyPartiallyCommitted(transaction_id) => write!(
            formatter,
            "batch transaction {transaction_id} is already committed outside the exact batch event"
        ),
        _ => unreachable!("lifecycle formatter received a non-lifecycle ledger error"),
    }
}

fn format_ledger_resource_error(
    error: &LedgerError,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    match error {
        LedgerError::CapacityExceeded {
            resource,
            maximum,
            attempted,
        } => write!(
            formatter,
            "ledger {resource} capacity {maximum} cannot admit exact cardinality {attempted}"
        ),
        LedgerError::PreparationAllocationFailed(resource) => {
            write!(formatter, "ledger {resource} allocation failed")
        }
        _ => unreachable!("resource formatter received a non-resource ledger error"),
    }
}

fn format_settlement_error(error: &LedgerError, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    match error {
        LedgerError::SelfSettlement => formatter.write_str("buyer and seller accounts must differ"),
        LedgerError::IdenticalSettlementAssets => {
            formatter.write_str("base and quote settlement assets must differ")
        }
        LedgerError::ZeroSettlementMultiplier => {
            formatter.write_str("settlement conversion multipliers must be non-zero")
        }
        LedgerError::SettlementInstrumentMismatch => {
            formatter.write_str("trade and settlement definition instruments differ")
        }
        LedgerError::SettlementVersionMismatch => {
            formatter.write_str("trade and settlement definition versions differ")
        }
        LedgerError::CallAuctionSettlementReportInvalid => formatter
            .write_str("call-auction settlement requires one complete accepted uncross report"),
        LedgerError::CallAuctionSettlementTransactionCountMismatch {
            transaction_count,
            trade_count,
        } => write!(
            formatter,
            "call-auction settlement supplied {transaction_count} transaction identifiers for {trade_count} trades"
        ),
        LedgerError::FeeAmountNotPositive(amount) => {
            write!(formatter, "fee transfer amount {amount} is not positive")
        }
        LedgerError::FeeAccountsIdentical(account_id) => write!(
            formatter,
            "fee transfer debit and credit account {account_id} are identical"
        ),
        LedgerError::FeeRateNumeratorZero => {
            formatter.write_str("call-auction fee rate numerator must be non-zero")
        }
        LedgerError::FeeRateDenominatorZero => {
            formatter.write_str("call-auction fee rate denominator must be non-zero")
        }
        LedgerError::FeeMinimumNotPositive(amount) => write!(
            formatter,
            "call-auction fee minimum amount {amount} is not positive"
        ),
        LedgerError::FeeMaximumBelowMinimum { minimum, maximum } => write!(
            formatter,
            "call-auction fee maximum amount {maximum} is below minimum {minimum}"
        ),
        LedgerError::FeeScheduleRevisionZero => {
            formatter.write_str("call-auction fee schedule revision must be non-zero")
        }
        LedgerError::FeeScheduleEmpty => {
            formatter.write_str("call-auction fee schedule requires a buy or sell rule")
        }
        LedgerError::FeeScheduleInstrumentMismatch => {
            formatter.write_str("call-auction fee schedule and settlement instruments differ")
        }
        LedgerError::FeeScheduleVersionMismatch => formatter
            .write_str("call-auction fee schedule and settlement definition versions differ"),
        LedgerError::CallAuctionFeeTransactionCountMismatch {
            transaction_count,
            assessment_count,
        } => write!(
            formatter,
            "call-auction settlement supplied {transaction_count} fee transaction identifiers for {assessment_count} assessments"
        ),
        LedgerError::CallAuctionFeeTradeMismatch(trade_id) => write!(
            formatter,
            "call-auction fee for trade {trade_id} is absent or outside canonical report order"
        ),
        LedgerError::CallAuctionCorrectionTransactionCountMismatch {
            reversal_transaction_count,
            settlement_transaction_count,
        } => write!(
            formatter,
            "call-auction correction supplied {reversal_transaction_count} reversal transaction identifiers for {settlement_transaction_count} settlement transactions"
        ),
        LedgerError::CallAuctionCorrectionOriginalGroupingMismatch(transaction_id) => write!(
            formatter,
            "call-auction settlement transaction {transaction_id} was committed outside its exact original event"
        ),
        _ => unreachable!("settlement formatter received a non-settlement error"),
    }
}

fn format_accounting_period_error(
    error: &LedgerError,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    match error {
        LedgerError::FinancialEntryMissingEffectiveDate => {
            formatter.write_str("financial ledger entry requires an effective date")
        }
        LedgerError::ControlEntryHasEffectiveDate => {
            formatter.write_str("period-control entry cannot carry an effective date")
        }
        LedgerError::ControlEntryHasPostings => {
            formatter.write_str("period-control entry cannot carry posting legs")
        }
        LedgerError::AccountingPeriodClosed {
            effective_date,
            closed_through,
        } => write!(
            formatter,
            "effective date {} is closed through {}",
            effective_date.days_since_unix_epoch(),
            closed_through.days_since_unix_epoch()
        ),
        LedgerError::PeriodCloseNotAdvancing {
            current_closed_through,
            proposed_closed_through,
        } => write!(
            formatter,
            "period close through {} does not advance current boundary {:?}",
            proposed_closed_through.days_since_unix_epoch(),
            current_closed_through.map(AccountingDate::days_since_unix_epoch)
        ),
        LedgerError::AccountingPeriodAlreadyOpen => {
            formatter.write_str("all accounting dates are already open")
        }
        LedgerError::InvalidPeriodReopen {
            current_closed_through,
            proposed_closed_through,
        } => write!(
            formatter,
            "period reopen boundary {:?} does not precede current boundary {}",
            proposed_closed_through.map(AccountingDate::days_since_unix_epoch),
            current_closed_through.days_since_unix_epoch()
        ),
        LedgerError::RecordedTimestampRegression { previous, proposed } => write!(
            formatter,
            "ledger recorded timestamp {} precedes committed timestamp {}",
            proposed.as_unix_nanos(),
            previous.as_unix_nanos()
        ),
        LedgerError::NonFinancialReversalTarget(transaction_id) => write!(
            formatter,
            "transaction {transaction_id} is administrative and has no financial effect to reverse"
        ),
        _ => unreachable!("accounting-period formatter received a non-period error"),
    }
}

impl std::error::Error for LedgerError {}

fn format_unbalanced_ledger_error(
    error: &LedgerError,
    formatter: &mut fmt::Formatter<'_>,
) -> fmt::Result {
    let LedgerError::Unbalanced {
        asset_id,
        positive_total,
        negative_total,
    } = error
    else {
        unreachable!("unbalanced formatter received another ledger error");
    };
    write!(
        formatter,
        "asset {asset_id} has positive total {positive_total} and negative total {negative_total}"
    )
}

#[derive(Clone, Copy)]
struct SettlementExecution {
    reference: u64,
    buyer_account_id: AccountId,
    seller_account_id: AccountId,
    price: Price,
    quantity: Quantity,
}

impl JournalEntry {
    /// Validates and constructs a balanced journal entry.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] when the entry is empty, contains zero or
    /// duplicate legs, or is not balanced by asset.
    pub fn new(
        transaction_id: TransactionId,
        reference: u64,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        postings: Vec<Posting>,
    ) -> Result<Self, LedgerError> {
        Self::with_kind(
            transaction_id,
            reference,
            Some(effective_date),
            recorded_at,
            postings,
            LedgerEntryKind::Standard,
        )
    }

    pub(crate) fn with_kind(
        transaction_id: TransactionId,
        reference: u64,
        effective_date: Option<AccountingDate>,
        recorded_at: TimestampNs,
        mut postings: Vec<Posting>,
        kind: LedgerEntryKind,
    ) -> Result<Self, LedgerError> {
        postings.sort_unstable_by_key(|posting| (posting.asset_id, posting.account_id));
        match kind {
            LedgerEntryKind::Standard | LedgerEntryKind::Reversal { .. } => {
                if effective_date.is_none() {
                    return Err(LedgerError::FinancialEntryMissingEffectiveDate);
                }
                validate_postings(&postings)?;
            }
            LedgerEntryKind::PeriodClose { .. } | LedgerEntryKind::PeriodReopen { .. } => {
                if effective_date.is_some() {
                    return Err(LedgerError::ControlEntryHasEffectiveDate);
                }
                if !postings.is_empty() {
                    return Err(LedgerError::ControlEntryHasPostings);
                }
            }
        }
        Ok(Self {
            transaction_id,
            reference,
            effective_date,
            recorded_at,
            postings: Arc::new(postings),
            kind,
        })
    }

    /// Constructs the exact signed inverse of an immutable prior entry.
    ///
    /// Posting the result additionally requires that the target exists and has
    /// not already been reversed.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::NonReversibleAmount`] when any target amount is
    /// `i128::MIN`, whose positive inverse is not representable.
    pub fn reversal(
        transaction_id: TransactionId,
        reference: u64,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        original: &Self,
    ) -> Result<Self, LedgerError> {
        if matches!(
            original.kind,
            LedgerEntryKind::PeriodClose { .. } | LedgerEntryKind::PeriodReopen { .. }
        ) {
            return Err(LedgerError::NonFinancialReversalTarget(
                original.transaction_id,
            ));
        }
        let mut postings = Vec::new();
        postings
            .try_reserve_exact(original.postings.len())
            .map_err(|_| {
                LedgerError::PreparationAllocationFailed(
                    LedgerPreparationResource::ReversalPostings,
                )
            })?;
        for posting in original.postings.iter() {
            let amount = posting
                .amount
                .checked_neg()
                .ok_or(LedgerError::NonReversibleAmount {
                    original_transaction_id: original.transaction_id,
                    account_id: posting.account_id,
                    asset_id: posting.asset_id,
                })?;
            postings.push(Posting { amount, ..*posting });
        }
        Self::with_kind(
            transaction_id,
            reference,
            Some(effective_date),
            recorded_at,
            postings,
            LedgerEntryKind::Reversal {
                reversed_transaction_id: original.transaction_id,
            },
        )
    }

    /// Constructs an administrative period-close entry.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if the control-entry shape cannot be established.
    pub fn period_close(
        transaction_id: TransactionId,
        reference: u64,
        recorded_at: TimestampNs,
        closed_through: AccountingDate,
    ) -> Result<Self, LedgerError> {
        Self::with_kind(
            transaction_id,
            reference,
            None,
            recorded_at,
            Vec::new(),
            LedgerEntryKind::PeriodClose { closed_through },
        )
    }

    /// Constructs an administrative period-reopen entry.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if the control-entry shape cannot be established.
    pub fn period_reopen(
        transaction_id: TransactionId,
        reference: u64,
        recorded_at: TimestampNs,
        new_closed_through: Option<AccountingDate>,
    ) -> Result<Self, LedgerError> {
        Self::with_kind(
            transaction_id,
            reference,
            None,
            recorded_at,
            Vec::new(),
            LedgerEntryKind::PeriodReopen { new_closed_through },
        )
    }

    /// Returns this entry's idempotency key.
    #[must_use]
    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    /// Returns the source-system reference.
    #[must_use]
    pub const fn reference(&self) -> u64 {
        self.reference
    }

    /// Returns the financial value date, or `None` for period controls.
    #[must_use]
    pub const fn effective_date(&self) -> Option<AccountingDate> {
        self.effective_date
    }

    /// Returns the monotonic UTC booking timestamp.
    #[must_use]
    pub const fn recorded_at(&self) -> TimestampNs {
        self.recorded_at
    }

    /// Returns the canonical account-and-asset-sorted posting legs.
    #[must_use]
    pub fn postings(&self) -> &[Posting] {
        self.postings.as_slice()
    }

    /// Returns this entry's posting for one account and asset, if present.
    ///
    /// The canonical posting order makes lookup `O(log(L + 1))` for `L`
    /// posting legs. The returned value borrows the immutable posting vector.
    #[must_use]
    pub fn posting(&self, account_id: AccountId, asset_id: AssetId) -> Option<&Posting> {
        self.postings
            .binary_search_by_key(&(asset_id, account_id), |posting| {
                (posting.asset_id, posting.account_id)
            })
            .ok()
            .map(|index| &self.postings[index])
    }

    /// Returns whether two immutable entries share the identical posting vector.
    #[must_use]
    pub fn shares_posting_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.postings, &other.postings)
    }

    /// Returns the accounting lifecycle classification.
    #[must_use]
    pub const fn kind(&self) -> LedgerEntryKind {
        self.kind
    }

    /// Returns the reversed transaction for a reversal entry.
    #[must_use]
    pub const fn reversed_transaction(&self) -> Option<TransactionId> {
        match self.kind {
            LedgerEntryKind::Standard
            | LedgerEntryKind::PeriodClose { .. }
            | LedgerEntryKind::PeriodReopen { .. } => None,
            LedgerEntryKind::Reversal {
                reversed_transaction_id,
            } => Some(reversed_transaction_id),
        }
    }

    /// Constructs a balanced delivery-versus-payment entry from a trade.
    ///
    /// `transaction_id` must be globally unique across execution shards;
    /// matching trade identifiers are only book-local.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for self settlement, invalid conversion factors,
    /// identical assets, arithmetic overflow, or invalid posting structure.
    pub fn from_trade(
        transaction_id: TransactionId,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        trade: &Trade,
        convention: SettlementConvention,
    ) -> Result<Self, LedgerError> {
        Self::from_settlement_execution(
            transaction_id,
            effective_date,
            recorded_at,
            SettlementExecution {
                reference: trade.trade_id.get(),
                buyer_account_id: trade.buyer_account_id,
                seller_account_id: trade.seller_account_id,
                price: trade.price,
                quantity: trade.quantity,
            },
            convention,
        )
    }

    fn from_settlement_execution(
        transaction_id: TransactionId,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        execution: SettlementExecution,
        convention: SettlementConvention,
    ) -> Result<Self, LedgerError> {
        if execution.buyer_account_id == execution.seller_account_id {
            return Err(LedgerError::SelfSettlement);
        }
        let (base_amount, notional, opposite_notional) =
            settlement_amounts(execution.price, execution.quantity, convention)?;
        let mut postings = vec![
            Posting {
                account_id: execution.buyer_account_id,
                asset_id: convention.base_asset_id,
                amount: base_amount,
            },
            Posting {
                account_id: execution.seller_account_id,
                asset_id: convention.base_asset_id,
                amount: -base_amount,
            },
        ];
        if notional != 0 {
            postings.extend([
                Posting {
                    account_id: execution.buyer_account_id,
                    asset_id: convention.quote_asset_id,
                    amount: opposite_notional,
                },
                Posting {
                    account_id: execution.seller_account_id,
                    asset_id: convention.quote_asset_id,
                    amount: notional,
                },
            ]);
        }
        Self::new(
            transaction_id,
            execution.reference,
            effective_date,
            recorded_at,
            postings,
        )
    }

    fn from_call_auction_trade(
        transaction_id: TransactionId,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        trade: CallAuctionTrade,
        definition: InstrumentDefinition,
    ) -> Result<Self, LedgerError> {
        if trade.instrument_id() != definition.instrument_id() {
            return Err(LedgerError::SettlementInstrumentMismatch);
        }
        if trade.instrument_version() != definition.version() {
            return Err(LedgerError::SettlementVersionMismatch);
        }
        Self::from_settlement_execution(
            transaction_id,
            effective_date,
            recorded_at,
            SettlementExecution {
                reference: trade.trade_id().get(),
                buyer_account_id: trade.buy_account_id(),
                seller_account_id: trade.sell_account_id(),
                price: trade.price(),
                quantity: trade.quantity(),
            },
            definition.settlement_convention(),
        )
    }

    /// Constructs settlement using an exact immutable instrument definition.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] when the trade identity/version does not match
    /// the definition or when settlement construction fails.
    pub fn from_instrument(
        transaction_id: TransactionId,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        trade: &Trade,
        definition: InstrumentDefinition,
    ) -> Result<Self, LedgerError> {
        if trade.instrument_id != definition.instrument_id() {
            return Err(LedgerError::SettlementInstrumentMismatch);
        }
        if trade.instrument_version != definition.version() {
            return Err(LedgerError::SettlementVersionMismatch);
        }
        Self::from_trade(
            transaction_id,
            effective_date,
            recorded_at,
            trade,
            definition.settlement_convention(),
        )
    }
}

impl LedgerCorrection {
    /// Constructs an exact reversal paired with one ordinary replacement entry.
    ///
    /// The pair is only locally canonicalized here. Posting additionally proves
    /// that the target exists, is unreversed, both value dates are open, and
    /// neither correction transaction was previously committed.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for a non-reversible target, non-standard
    /// replacement, transaction reuse, or timestamp regression within the pair.
    pub fn new(
        reversal_transaction_id: TransactionId,
        reversal_reference: u64,
        reversal_effective_date: AccountingDate,
        reversal_recorded_at: TimestampNs,
        replacement: JournalEntry,
        original: &JournalEntry,
    ) -> Result<Self, LedgerError> {
        let reversal = JournalEntry::reversal(
            reversal_transaction_id,
            reversal_reference,
            reversal_effective_date,
            reversal_recorded_at,
            original,
        )?;
        Self::from_parts(reversal, replacement)
    }

    pub(crate) fn from_parts(
        reversal: JournalEntry,
        replacement: JournalEntry,
    ) -> Result<Self, LedgerError> {
        let LedgerEntryKind::Reversal {
            reversed_transaction_id,
        } = reversal.kind
        else {
            return Err(LedgerError::CorrectionFirstEntryNotReversal(
                reversal.transaction_id,
            ));
        };
        if replacement.kind != LedgerEntryKind::Standard {
            return Err(LedgerError::CorrectionReplacementNotStandard(
                replacement.transaction_id,
            ));
        }
        if reversal.transaction_id == replacement.transaction_id {
            return Err(LedgerError::CorrectionTransactionIdsNotDistinct {
                reversal_transaction_id: reversal.transaction_id,
                replacement_transaction_id: replacement.transaction_id,
            });
        }
        if reversal.transaction_id == reversed_transaction_id {
            return Err(LedgerError::TransactionIdCollision(reversal.transaction_id));
        }
        if replacement.transaction_id == reversed_transaction_id {
            return Err(LedgerError::TransactionIdCollision(
                replacement.transaction_id,
            ));
        }
        if replacement.recorded_at < reversal.recorded_at {
            return Err(LedgerError::RecordedTimestampRegression {
                previous: reversal.recorded_at,
                proposed: replacement.recorded_at,
            });
        }
        Ok(Self {
            reversal,
            replacement,
        })
    }

    /// Returns the exact target reversal.
    #[must_use]
    pub const fn reversal(&self) -> &JournalEntry {
        &self.reversal
    }

    /// Returns the corrected ordinary entry.
    #[must_use]
    pub const fn replacement(&self) -> &JournalEntry {
        &self.replacement
    }

    /// Returns the transaction being corrected.
    #[must_use]
    pub const fn corrected_transaction_id(&self) -> TransactionId {
        match self.reversal.kind {
            LedgerEntryKind::Reversal {
                reversed_transaction_id,
            } => reversed_transaction_id,
            _ => unreachable!(),
        }
    }
}

fn validate_settlement_convention(convention: SettlementConvention) -> Result<(), LedgerError> {
    if convention.base_asset_id == convention.quote_asset_id {
        return Err(LedgerError::IdenticalSettlementAssets);
    }
    if convention.base_units_per_lot == 0 || convention.quote_units_per_price_unit == 0 {
        return Err(LedgerError::ZeroSettlementMultiplier);
    }
    Ok(())
}

fn settlement_amounts(
    price: Price,
    quantity: Quantity,
    convention: SettlementConvention,
) -> Result<(i128, i128, i128), LedgerError> {
    validate_settlement_convention(convention)?;
    let base_amount = i128::from(quantity.lots())
        .checked_mul(i128::from(convention.base_units_per_lot))
        .ok_or(LedgerError::ArithmeticOverflow)?;
    let notional = i128::from(price.raw())
        .checked_mul(i128::from(quantity.lots()))
        .and_then(|value| value.checked_mul(i128::from(convention.quote_units_per_price_unit)))
        .ok_or(LedgerError::ArithmeticOverflow)?;
    let opposite_notional = notional
        .checked_neg()
        .ok_or(LedgerError::ArithmeticOverflow)?;
    Ok((base_amount, notional, opposite_notional))
}

impl LedgerBatch {
    /// Constructs an ordered atomic group of two or more distinct entries.
    ///
    /// The declared order is retained exactly. Each member is already
    /// canonical by construction; this boundary additionally proves unique
    /// transaction identifiers in one exact bounded hash set and nondecreasing
    /// booking timestamps. Expected construction is `O(N)` for `N` entries;
    /// adversarial full hash collisions are bounded by `O(N²)`.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for insufficient cardinality, a repeated
    /// transaction identifier, or a timestamp regression within the batch.
    pub fn new(entries: Vec<JournalEntry>) -> Result<Self, LedgerError> {
        if entries.len() < 2 {
            return Err(LedgerError::BatchTooFewEntries);
        }
        let mut transaction_ids = reserve_ledger_preparation_set(
            entries.len(),
            LedgerPreparationResource::BatchIdentitySet,
        )?;
        for entry in &entries {
            if !transaction_ids.insert(entry.transaction_id) {
                return Err(LedgerError::BatchDuplicateTransaction(entry.transaction_id));
            }
        }
        if let Some(pair) = entries
            .windows(2)
            .find(|pair| pair[1].recorded_at < pair[0].recorded_at)
        {
            return Err(LedgerError::RecordedTimestampRegression {
                previous: pair[0].recorded_at,
                proposed: pair[1].recorded_at,
            });
        }
        Ok(Self {
            entries: Arc::new(entries),
        })
    }

    /// Returns entries in their authoritative application order.
    #[must_use]
    pub fn entries(&self) -> &[JournalEntry] {
        self.entries.as_slice()
    }

    /// Returns whether two immutable batches share the identical entry vector.
    #[must_use]
    pub fn shares_entry_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.entries, &other.entries)
    }

    /// Returns the first transaction identifier used as the event correlation key.
    #[must_use]
    pub fn primary_transaction_id(&self) -> TransactionId {
        self.entries[0].transaction_id
    }
}

impl CallAuctionFeeRule {
    /// Constructs one exact rational fee or rebate rule.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] when the rate is zero or has a zero denominator,
    /// the minimum is not positive, or the optional maximum is below the
    /// minimum.
    pub fn new(
        asset_id: AssetId,
        basis: CallAuctionFeeBasis,
        rate_numerator: i64,
        rate_denominator: u64,
        rounding: CallAuctionFeeRounding,
        minimum_amount: i128,
        maximum_amount: Option<i128>,
    ) -> Result<Self, LedgerError> {
        if rate_numerator == 0 {
            return Err(LedgerError::FeeRateNumeratorZero);
        }
        if rate_denominator == 0 {
            return Err(LedgerError::FeeRateDenominatorZero);
        }
        if minimum_amount <= 0 {
            return Err(LedgerError::FeeMinimumNotPositive(minimum_amount));
        }
        if let Some(maximum) = maximum_amount {
            if maximum < minimum_amount {
                return Err(LedgerError::FeeMaximumBelowMinimum {
                    minimum: minimum_amount,
                    maximum,
                });
            }
        }
        Ok(Self {
            asset_id,
            basis,
            rate_numerator,
            rate_denominator,
            rounding,
            minimum_amount,
            maximum_amount,
        })
    }

    /// Returns the fee or rebate asset denomination.
    #[must_use]
    pub const fn asset_id(self) -> AssetId {
        self.asset_id
    }

    /// Returns the economic quantity to which the rational rate is applied.
    #[must_use]
    pub const fn basis(self) -> CallAuctionFeeBasis {
        self.basis
    }

    /// Returns the signed rational rate numerator.
    #[must_use]
    pub const fn rate_numerator(self) -> i64 {
        self.rate_numerator
    }

    /// Returns the positive rational rate denominator.
    #[must_use]
    pub const fn rate_denominator(self) -> u64 {
        self.rate_denominator
    }

    /// Returns the deterministic rounding rule.
    #[must_use]
    pub const fn rounding(self) -> CallAuctionFeeRounding {
        self.rounding
    }

    /// Returns the positive post-rounding minimum amount.
    #[must_use]
    pub const fn minimum_amount(self) -> i128 {
        self.minimum_amount
    }

    /// Returns the optional post-rounding maximum amount.
    #[must_use]
    pub const fn maximum_amount(self) -> Option<i128> {
        self.maximum_amount
    }

    /// Returns the transfer direction encoded by the signed numerator.
    #[must_use]
    pub const fn direction(self) -> CallAuctionFeeDirection {
        if self.rate_numerator > 0 {
            CallAuctionFeeDirection::ParticipantPays
        } else {
            CallAuctionFeeDirection::ParticipantReceives
        }
    }

    /// Calculates the positive transfer magnitude for one execution.
    ///
    /// The result uses exact integer arithmetic, applies the configured rounding
    /// once, and then applies the minimum and optional maximum.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for an invalid settlement convention or when an
    /// uncapped intermediate or result is not representable.
    pub fn calculate_amount(
        self,
        price: Price,
        quantity: Quantity,
        convention: SettlementConvention,
    ) -> Result<i128, LedgerError> {
        let basis_amount = self.calculate_basis_amount(price, quantity, convention)?;
        self.calculate_amount_from_basis(basis_amount)
    }

    fn calculate_basis_amount(
        self,
        price: Price,
        quantity: Quantity,
        convention: SettlementConvention,
    ) -> Result<u128, LedgerError> {
        let (base_amount, notional, _) = settlement_amounts(price, quantity, convention)?;
        match self.basis {
            CallAuctionFeeBasis::TradedLots => Ok(u128::from(quantity.lots())),
            CallAuctionFeeBasis::BaseAssetUnits => {
                u128::try_from(base_amount).map_err(|_| LedgerError::ArithmeticOverflow)
            }
            CallAuctionFeeBasis::QuoteNotionalMagnitude => {
                let magnitude = notional
                    .checked_abs()
                    .ok_or(LedgerError::ArithmeticOverflow)?;
                u128::try_from(magnitude).map_err(|_| LedgerError::ArithmeticOverflow)
            }
        }
    }

    fn calculate_amount_from_basis(self, basis_amount: u128) -> Result<i128, LedgerError> {
        let numerator = u128::from(self.rate_numerator.unsigned_abs());
        let denominator = u128::from(self.rate_denominator);
        let whole = basis_amount / denominator;
        let basis_remainder = basis_amount % denominator;
        let maximum = self
            .maximum_amount
            .map(u128::try_from)
            .transpose()
            .map_err(|_| LedgerError::ArithmeticOverflow)?;
        let minimum =
            u128::try_from(self.minimum_amount).map_err(|_| LedgerError::ArithmeticOverflow)?;

        let Some(whole_product) = whole.checked_mul(numerator) else {
            return capped_fee_overflow(maximum);
        };
        let fractional_product = basis_remainder
            .checked_mul(numerator)
            .ok_or(LedgerError::ArithmeticOverflow)?;
        let Some(quotient) = whole_product.checked_add(fractional_product / denominator) else {
            return capped_fee_overflow(maximum);
        };
        let remainder = fractional_product % denominator;
        let increment = match self.rounding {
            CallAuctionFeeRounding::Down => false,
            CallAuctionFeeRounding::Up => remainder != 0,
            CallAuctionFeeRounding::NearestTiesToEven => {
                let complement = denominator - remainder;
                remainder > complement || (remainder == complement && quotient % 2 != 0)
            }
        };
        let rounded = if increment {
            let Some(rounded) = quotient.checked_add(1) else {
                return capped_fee_overflow(maximum);
            };
            rounded
        } else {
            quotient
        };
        let clamped = rounded.max(minimum).min(maximum.unwrap_or(u128::MAX));
        i128::try_from(clamped).map_err(|_| LedgerError::ArithmeticOverflow)
    }
}

fn capped_fee_overflow(maximum: Option<u128>) -> Result<i128, LedgerError> {
    maximum.map_or(Err(LedgerError::ArithmeticOverflow), |value| {
        i128::try_from(value).map_err(|_| LedgerError::ArithmeticOverflow)
    })
}

impl CallAuctionFeeSchedule {
    /// Constructs one immutable instrument-version-bound fee schedule.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::FeeScheduleRevisionZero`] for revision zero and
    /// [`LedgerError::FeeScheduleEmpty`] when neither side has a rule.
    pub fn new(
        revision: u64,
        instrument_id: InstrumentId,
        instrument_version: InstrumentVersion,
        fee_account_id: AccountId,
        buy_rule: Option<CallAuctionFeeRule>,
        sell_rule: Option<CallAuctionFeeRule>,
    ) -> Result<Self, LedgerError> {
        if revision == 0 {
            return Err(LedgerError::FeeScheduleRevisionZero);
        }
        if buy_rule.is_none() && sell_rule.is_none() {
            return Err(LedgerError::FeeScheduleEmpty);
        }
        Ok(Self {
            revision,
            instrument_id,
            instrument_version,
            fee_account_id,
            buy_rule,
            sell_rule,
        })
    }

    /// Returns the non-zero schedule revision.
    #[must_use]
    pub const fn revision(self) -> u64 {
        self.revision
    }

    /// Returns the bound instrument identity.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.instrument_id
    }

    /// Returns the bound immutable instrument-definition version.
    #[must_use]
    pub const fn instrument_version(self) -> InstrumentVersion {
        self.instrument_version
    }

    /// Returns the account that receives fees and supplies rebates.
    #[must_use]
    pub const fn fee_account_id(self) -> AccountId {
        self.fee_account_id
    }

    /// Returns the optional buyer rule.
    #[must_use]
    pub const fn buy_rule(self) -> Option<CallAuctionFeeRule> {
        self.buy_rule
    }

    /// Returns the optional seller rule.
    #[must_use]
    pub const fn sell_rule(self) -> Option<CallAuctionFeeRule> {
        self.sell_rule
    }

    /// Calculates all fees and rebates in report order, Buy before Sell.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for schedule-definition mismatch, an invalid
    /// report, participant/fee account identity, arithmetic overflow, or exact
    /// output reservation failure.
    pub fn assess_report(
        self,
        report: &CallAuctionExecutionReport,
        definition: InstrumentDefinition,
    ) -> Result<Vec<CallAuctionFeeAssessment>, LedgerError> {
        self.validate_definition(definition)?;
        let trade_count = validate_call_auction_settlement_report(report, definition)?;
        let assessment_count = self.assessment_count(trade_count)?;
        let mut assessments = reserve_ledger_preparation_vec(
            assessment_count,
            LedgerPreparationResource::CallAuctionFeeAssessments,
        )?;
        self.for_each_assessment(report, definition, trade_count, |assessment| {
            assessments.push(assessment);
            Ok(())
        })?;
        Ok(assessments)
    }

    fn validate_definition(self, definition: InstrumentDefinition) -> Result<(), LedgerError> {
        if self.instrument_id != definition.instrument_id() {
            return Err(LedgerError::FeeScheduleInstrumentMismatch);
        }
        if self.instrument_version != definition.version() {
            return Err(LedgerError::FeeScheduleVersionMismatch);
        }
        Ok(())
    }

    fn assessment_count(self, trade_count: usize) -> Result<usize, LedgerError> {
        let rules_per_trade = usize::from(self.buy_rule.is_some())
            .checked_add(usize::from(self.sell_rule.is_some()))
            .ok_or(LedgerError::ArithmeticOverflow)?;
        trade_count
            .checked_mul(rules_per_trade)
            .ok_or(LedgerError::ArithmeticOverflow)
    }

    fn for_each_assessment(
        self,
        report: &CallAuctionExecutionReport,
        definition: InstrumentDefinition,
        trade_count: usize,
        mut accept: impl FnMut(CallAuctionFeeAssessment) -> Result<(), LedgerError>,
    ) -> Result<(), LedgerError> {
        let convention = definition.settlement_convention();
        for event in report.events.iter().take(trade_count) {
            let CallAuctionEventKind::Trade(trade) = event.kind else {
                unreachable!("validated call-auction trade prefix changed")
            };
            if let Some(rule) = self.buy_rule {
                accept(self.assess_trade(trade, Side::Buy, rule, convention)?)?;
            }
            if let Some(rule) = self.sell_rule {
                accept(self.assess_trade(trade, Side::Sell, rule, convention)?)?;
            }
        }
        Ok(())
    }

    fn assess_trade(
        self,
        trade: CallAuctionTrade,
        side: Side,
        rule: CallAuctionFeeRule,
        convention: SettlementConvention,
    ) -> Result<CallAuctionFeeAssessment, LedgerError> {
        let participant_account_id = match side {
            Side::Buy => trade.buy_account_id(),
            Side::Sell => trade.sell_account_id(),
        };
        if participant_account_id == self.fee_account_id {
            return Err(LedgerError::FeeAccountsIdentical(participant_account_id));
        }
        let basis_amount =
            rule.calculate_basis_amount(trade.price(), trade.quantity(), convention)?;
        let amount = rule.calculate_amount_from_basis(basis_amount)?;
        Ok(CallAuctionFeeAssessment {
            schedule_revision: self.revision,
            trade_id: trade.trade_id(),
            side,
            participant_account_id,
            fee_account_id: self.fee_account_id,
            asset_id: rule.asset_id,
            basis: rule.basis,
            basis_amount,
            direction: rule.direction(),
            amount,
        })
    }
}

impl CallAuctionFeeAssessment {
    /// Returns the schedule revision that produced this assessment.
    #[must_use]
    pub const fn schedule_revision(self) -> u64 {
        self.schedule_revision
    }

    /// Returns the book-local trade identity.
    #[must_use]
    pub const fn trade_id(self) -> TradeId {
        self.trade_id
    }

    /// Returns the assessed participant side.
    #[must_use]
    pub const fn side(self) -> Side {
        self.side
    }

    /// Returns the assessed participant account.
    #[must_use]
    pub const fn participant_account_id(self) -> AccountId {
        self.participant_account_id
    }

    /// Returns the account that receives the fee or supplies the rebate.
    #[must_use]
    pub const fn fee_account_id(self) -> AccountId {
        self.fee_account_id
    }

    /// Returns the transfer asset denomination.
    #[must_use]
    pub const fn asset_id(self) -> AssetId {
        self.asset_id
    }

    /// Returns the economic basis kind.
    #[must_use]
    pub const fn basis(self) -> CallAuctionFeeBasis {
        self.basis
    }

    /// Returns the non-negative basis magnitude in basis-defined units.
    #[must_use]
    pub const fn basis_amount(self) -> u128 {
        self.basis_amount
    }

    /// Returns whether the participant pays a fee or receives a rebate.
    #[must_use]
    pub const fn direction(self) -> CallAuctionFeeDirection {
        self.direction
    }

    /// Returns the positive transfer magnitude in the asset's smallest unit.
    #[must_use]
    pub const fn amount(self) -> i128 {
        self.amount
    }

    /// Binds this assessment to one global transaction identity.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if the assessed accounts or amount cannot form
    /// an explicit fee transfer.
    pub fn into_fee(self, transaction_id: TransactionId) -> Result<CallAuctionFee, LedgerError> {
        let (debit_account_id, credit_account_id) = match self.direction {
            CallAuctionFeeDirection::ParticipantPays => {
                (self.participant_account_id, self.fee_account_id)
            }
            CallAuctionFeeDirection::ParticipantReceives => {
                (self.fee_account_id, self.participant_account_id)
            }
        };
        CallAuctionFee::new(
            transaction_id,
            self.trade_id,
            debit_account_id,
            credit_account_id,
            self.asset_id,
            self.amount,
        )
    }
}

impl CallAuctionFee {
    /// Constructs one positive, single-asset fee transfer.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::FeeAmountNotPositive`] for a zero or negative
    /// amount and [`LedgerError::FeeAccountsIdentical`] when the transfer would
    /// debit and credit the same account.
    pub fn new(
        transaction_id: TransactionId,
        trade_id: TradeId,
        debit_account_id: AccountId,
        credit_account_id: AccountId,
        asset_id: AssetId,
        amount: i128,
    ) -> Result<Self, LedgerError> {
        if amount <= 0 {
            return Err(LedgerError::FeeAmountNotPositive(amount));
        }
        if debit_account_id == credit_account_id {
            return Err(LedgerError::FeeAccountsIdentical(debit_account_id));
        }
        Ok(Self {
            transaction_id,
            trade_id,
            debit_account_id,
            credit_account_id,
            asset_id,
            amount,
        })
    }

    /// Returns the globally unique transaction identity for this fee entry.
    #[must_use]
    pub const fn transaction_id(self) -> TransactionId {
        self.transaction_id
    }

    /// Returns the book-local trade identity to which this fee is bound.
    #[must_use]
    pub const fn trade_id(self) -> TradeId {
        self.trade_id
    }

    /// Returns the account debited by the positive fee amount.
    #[must_use]
    pub const fn debit_account_id(self) -> AccountId {
        self.debit_account_id
    }

    /// Returns the account credited by the positive fee amount.
    #[must_use]
    pub const fn credit_account_id(self) -> AccountId {
        self.credit_account_id
    }

    /// Returns the asset denomination of the fee amount.
    #[must_use]
    pub const fn asset_id(self) -> AssetId {
        self.asset_id
    }

    /// Returns the positive amount in the asset's smallest ledger unit.
    #[must_use]
    pub const fn amount(self) -> i128 {
        self.amount
    }

    fn into_entry(
        self,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
    ) -> Result<JournalEntry, LedgerError> {
        let mut postings =
            reserve_ledger_preparation_vec(2, LedgerPreparationResource::CallAuctionFeePostings)?;
        postings.push(Posting {
            account_id: self.debit_account_id,
            asset_id: self.asset_id,
            amount: -self.amount,
        });
        postings.push(Posting {
            account_id: self.credit_account_id,
            asset_id: self.asset_id,
            amount: self.amount,
        });
        JournalEntry::new(
            self.transaction_id,
            self.trade_id.get(),
            effective_date,
            recorded_at,
            postings,
        )
    }
}

impl CallAuctionSettlement {
    /// Constructs one DVP ledger event from a complete accepted uncross report.
    ///
    /// Global transaction identifiers bind to trade events in report order.
    /// The immutable instrument definition must match every trade. Report
    /// structure, aggregate executable quantity, and all entries are validated
    /// before a settlement value is returned.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for a non-uncross or incomplete report, a
    /// transaction-count or definition mismatch, self settlement, invalid
    /// settlement factors, arithmetic overflow, or batch construction failure.
    pub fn from_report(
        transaction_ids: Vec<TransactionId>,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        report: &CallAuctionExecutionReport,
        definition: InstrumentDefinition,
    ) -> Result<Self, LedgerError> {
        Self::from_report_with_fees(
            transaction_ids,
            Vec::new(),
            effective_date,
            recorded_at,
            report,
            definition,
        )
    }

    /// Constructs one atomic DVP-and-fee event from a complete accepted report.
    ///
    /// `transaction_ids` binds one DVP entry to every trade event in report
    /// order. `fees` must be grouped in the same report order; every contiguous
    /// fee group follows its referenced DVP entry in the resulting batch.
    /// Multiple fees and fee-free trades are valid. Each fee is a separately
    /// idempotent standard entry, while the complete DVP-and-fee set commits as
    /// one indivisible ledger event.
    ///
    /// Fee calculation, payer/recipient selection, and asset denomination are
    /// not inferred. The caller supplies those authoritative values explicitly
    /// through [`CallAuctionFee`]; authorization remains external.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for every ordinary report, DVP, and batch
    /// failure; a fee outside canonical report order returns
    /// [`LedgerError::CallAuctionFeeTradeMismatch`]. No ledger state is mutated
    /// during construction.
    pub fn from_report_with_fees(
        transaction_ids: Vec<TransactionId>,
        fees: Vec<CallAuctionFee>,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        report: &CallAuctionExecutionReport,
        definition: InstrumentDefinition,
    ) -> Result<Self, LedgerError> {
        let trade_count = validate_call_auction_settlement_report(report, definition)?;
        Self::from_validated_report_with_fees(
            transaction_ids,
            fees,
            effective_date,
            recorded_at,
            report,
            definition,
            trade_count,
        )
    }

    /// Calculates side-specific fees and constructs one atomic DVP-and-fee event.
    ///
    /// The schedule must match the immutable settlement definition. One DVP
    /// transaction identity binds to each report trade. One fee transaction
    /// identity binds to each configured side assessment in canonical report
    /// order, Buy before Sell. Calculated transfers use [`CallAuctionFee`] and
    /// the same indivisible settlement batch as explicitly supplied fees.
    ///
    /// The schedule revision is not stored as separate ledger policy metadata;
    /// the resulting entries durably retain the exact postings and trade
    /// references. Schedule distribution, authorization, and registry durability
    /// remain external.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for schedule-definition mismatch, report or DVP
    /// failure, fee-transaction count mismatch, arithmetic overflow, or exact
    /// preparation reservation failure. No ledger state is mutated during
    /// construction.
    pub fn from_report_with_fee_schedule(
        transaction_ids: Vec<TransactionId>,
        fee_transaction_ids: Vec<TransactionId>,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        report: &CallAuctionExecutionReport,
        definition: InstrumentDefinition,
        schedule: &CallAuctionFeeSchedule,
    ) -> Result<Self, LedgerError> {
        let schedule = *schedule;
        schedule.validate_definition(definition)?;
        let trade_count = validate_call_auction_settlement_report(report, definition)?;
        validate_call_auction_settlement_transaction_count(transaction_ids.len(), trade_count)?;
        let assessment_count = schedule.assessment_count(trade_count)?;
        if fee_transaction_ids.len() != assessment_count {
            return Err(LedgerError::CallAuctionFeeTransactionCountMismatch {
                transaction_count: fee_transaction_ids.len(),
                assessment_count,
            });
        }

        let mut fees = reserve_ledger_preparation_vec(
            assessment_count,
            LedgerPreparationResource::CallAuctionCalculatedFees,
        )?;
        let mut fee_transaction_ids = fee_transaction_ids.into_iter();
        schedule.for_each_assessment(report, definition, trade_count, |assessment| {
            let Some(transaction_id) = fee_transaction_ids.next() else {
                unreachable!("validated fee transaction count changed")
            };
            fees.push(assessment.into_fee(transaction_id)?);
            Ok(())
        })?;
        debug_assert!(fee_transaction_ids.next().is_none());
        Self::from_validated_report_with_fees(
            transaction_ids,
            fees,
            effective_date,
            recorded_at,
            report,
            definition,
            trade_count,
        )
    }

    fn from_validated_report_with_fees(
        transaction_ids: Vec<TransactionId>,
        fees: Vec<CallAuctionFee>,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        report: &CallAuctionExecutionReport,
        definition: InstrumentDefinition,
        trade_count: usize,
    ) -> Result<Self, LedgerError> {
        validate_call_auction_settlement_transaction_count(transaction_ids.len(), trade_count)?;

        let entry_count = trade_count
            .checked_add(fees.len())
            .ok_or(LedgerError::ArithmeticOverflow)?;
        let mut entries = reserve_ledger_preparation_vec(
            entry_count,
            LedgerPreparationResource::CallAuctionSettlementEntries,
        )?;
        let mut fees = fees.into_iter().peekable();
        for (transaction_id, event) in transaction_ids
            .into_iter()
            .zip(report.events.iter().take(trade_count))
        {
            let CallAuctionEventKind::Trade(trade) = event.kind else {
                unreachable!("validated call-auction trade prefix changed")
            };
            entries.push(JournalEntry::from_call_auction_trade(
                transaction_id,
                effective_date,
                recorded_at,
                trade,
                definition,
            )?);
            while fees
                .peek()
                .is_some_and(|fee| fee.trade_id == trade.trade_id())
            {
                let Some(fee) = fees.next() else {
                    unreachable!("peeked call-auction fee disappeared")
                };
                entries.push(fee.into_entry(effective_date, recorded_at)?);
            }
        }
        if let Some(fee) = fees.next() {
            return Err(LedgerError::CallAuctionFeeTradeMismatch(fee.trade_id));
        }
        let record = CallAuctionSettlementRecord::from_entries(entries)?;
        Ok(Self { record })
    }

    /// Returns the number of global transactions in the atomic ledger event.
    #[must_use]
    pub fn transaction_count(&self) -> usize {
        self.record.transaction_count()
    }

    pub(crate) fn into_record(self) -> CallAuctionSettlementRecord {
        self.record
    }
}

impl CallAuctionSettlementCorrection {
    /// Constructs one exact full-settlement bust.
    ///
    /// One new reversal transaction identifier must be supplied for every DVP
    /// and fee entry in canonical original-settlement order. A one-entry bust
    /// retains the ordinary entry path; larger busts use one [`LedgerBatch`].
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for identifier-count mismatch, an
    /// unrepresentable inverse, duplicate transaction identity, timestamp
    /// regression, or preparation allocation failure.
    pub fn bust(
        reversal_transaction_ids: Vec<TransactionId>,
        reversal_reference: u64,
        reversal_effective_date: AccountingDate,
        reversal_recorded_at: TimestampNs,
        original: &CallAuctionSettlement,
    ) -> Result<Self, LedgerError> {
        Self::from_parts(
            reversal_transaction_ids,
            reversal_reference,
            reversal_effective_date,
            reversal_recorded_at,
            None,
            original,
        )
    }

    /// Constructs one full reversal followed by one replacement settlement.
    ///
    /// All original DVP and fee entries are reversed first. Every replacement
    /// DVP and fee entry follows in its own canonical settlement order. The
    /// complete correction is one ordered batch and exposes only its aggregate
    /// final balance image.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for any bust-construction failure, duplicate
    /// identity across reversals and replacements, or booking-time regression
    /// from the reversal group into the replacement.
    pub fn replace(
        reversal_transaction_ids: Vec<TransactionId>,
        reversal_reference: u64,
        reversal_effective_date: AccountingDate,
        reversal_recorded_at: TimestampNs,
        replacement: CallAuctionSettlement,
        original: &CallAuctionSettlement,
    ) -> Result<Self, LedgerError> {
        Self::from_parts(
            reversal_transaction_ids,
            reversal_reference,
            reversal_effective_date,
            reversal_recorded_at,
            Some(replacement),
            original,
        )
    }

    fn from_parts(
        reversal_transaction_ids: Vec<TransactionId>,
        reversal_reference: u64,
        reversal_effective_date: AccountingDate,
        reversal_recorded_at: TimestampNs,
        replacement: Option<CallAuctionSettlement>,
        original: &CallAuctionSettlement,
    ) -> Result<Self, LedgerError> {
        let reversal_transaction_count = reversal_transaction_ids.len();
        let settlement_transaction_count = original.transaction_count();
        if reversal_transaction_count != settlement_transaction_count {
            return Err(LedgerError::CallAuctionCorrectionTransactionCountMismatch {
                reversal_transaction_count,
                settlement_transaction_count,
            });
        }
        let replacement_transaction_count = replacement
            .as_ref()
            .map_or(0, CallAuctionSettlement::transaction_count);
        let transaction_count = reversal_transaction_count
            .checked_add(replacement_transaction_count)
            .ok_or(LedgerError::ArithmeticOverflow)?;
        let mut entries = reserve_ledger_preparation_vec(
            transaction_count,
            LedgerPreparationResource::CallAuctionCorrectionEntries,
        )?;
        for (transaction_id, original_entry) in reversal_transaction_ids
            .into_iter()
            .zip(original.record.entries())
        {
            entries.push(JournalEntry::reversal(
                transaction_id,
                reversal_reference,
                reversal_effective_date,
                reversal_recorded_at,
                original_entry,
            )?);
        }
        if let Some(replacement) = replacement {
            entries.extend(replacement.record.entries().iter().cloned());
        }
        let record = CallAuctionSettlementRecord::from_entries(entries)?;
        Ok(Self {
            original: original.clone(),
            record,
            reversal_transaction_count,
            replacement_transaction_count,
        })
    }

    /// Returns the exact original settlement whose event grouping must exist.
    #[must_use]
    pub const fn original_settlement(&self) -> &CallAuctionSettlement {
        &self.original
    }

    /// Returns the number of exact reversal entries in this correction.
    #[must_use]
    pub const fn reversal_transaction_count(&self) -> usize {
        self.reversal_transaction_count
    }

    /// Returns the number of replacement DVP and fee entries.
    #[must_use]
    pub const fn replacement_transaction_count(&self) -> usize {
        self.replacement_transaction_count
    }

    /// Returns the total reversal-plus-replacement transaction count.
    #[must_use]
    pub const fn transaction_count(&self) -> usize {
        self.reversal_transaction_count + self.replacement_transaction_count
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        CallAuctionSettlement,
        CallAuctionSettlementRecord,
        usize,
        usize,
    ) {
        (
            self.original,
            self.record,
            self.reversal_transaction_count,
            self.replacement_transaction_count,
        )
    }
}

fn validate_call_auction_settlement_transaction_count(
    transaction_count: usize,
    trade_count: usize,
) -> Result<(), LedgerError> {
    if transaction_count != trade_count {
        return Err(LedgerError::CallAuctionSettlementTransactionCountMismatch {
            transaction_count,
            trade_count,
        });
    }
    Ok(())
}

fn validate_call_auction_settlement_report(
    report: &CallAuctionExecutionReport,
    definition: InstrumentDefinition,
) -> Result<usize, LedgerError> {
    if report.command_sequence == 0 || report.outcome != CallAuctionCommandOutcome::Accepted {
        return Err(LedgerError::CallAuctionSettlementReportInvalid);
    }
    let Some(completion) = report.events.last() else {
        return Err(LedgerError::CallAuctionSettlementReportInvalid);
    };
    let CallAuctionEventKind::UncrossCompleted {
        clearing,
        trade_count,
        cancellation_count,
        ..
    } = completion.kind
    else {
        return Err(LedgerError::CallAuctionSettlementReportInvalid);
    };
    let (Ok(trade_count), Ok(cancellation_count)) = (
        usize::try_from(trade_count),
        usize::try_from(cancellation_count),
    ) else {
        return Err(LedgerError::CallAuctionSettlementReportInvalid);
    };
    let Some(expected_event_count) = trade_count
        .checked_add(cancellation_count)
        .and_then(|count| count.checked_add(1))
    else {
        return Err(LedgerError::CallAuctionSettlementReportInvalid);
    };
    if trade_count == 0 || report.events.len() != expected_event_count {
        return Err(LedgerError::CallAuctionSettlementReportInvalid);
    }
    let Some(first_event) = report.events.first() else {
        return Err(LedgerError::CallAuctionSettlementReportInvalid);
    };
    let mut expected_sequence = first_event.sequence;
    if expected_sequence == 0 {
        return Err(LedgerError::CallAuctionSettlementReportInvalid);
    }
    for event in &report.events {
        if event.sequence != expected_sequence || event.command_id != report.command_id {
            return Err(LedgerError::CallAuctionSettlementReportInvalid);
        }
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(LedgerError::CallAuctionSettlementReportInvalid)?;
    }

    let mut executed_quantity = 0_u128;
    let mut previous_trade_id = None;
    for event in report.events.iter().take(trade_count) {
        let CallAuctionEventKind::Trade(trade) = event.kind else {
            return Err(LedgerError::CallAuctionSettlementReportInvalid);
        };
        if previous_trade_id.is_some_and(|previous| trade.trade_id() <= previous) {
            return Err(LedgerError::CallAuctionSettlementReportInvalid);
        }
        previous_trade_id = Some(trade.trade_id());
        if trade.instrument_id() != definition.instrument_id() {
            return Err(LedgerError::SettlementInstrumentMismatch);
        }
        if trade.instrument_version() != definition.version() {
            return Err(LedgerError::SettlementVersionMismatch);
        }
        if trade.price() != clearing.price() {
            return Err(LedgerError::CallAuctionSettlementReportInvalid);
        }
        executed_quantity = executed_quantity
            .checked_add(u128::from(trade.quantity().lots()))
            .ok_or(LedgerError::ArithmeticOverflow)?;
    }
    if report
        .events
        .iter()
        .skip(trade_count)
        .take(cancellation_count)
        .any(|event| !matches!(event.kind, CallAuctionEventKind::RemainderCancelled(_)))
        || executed_quantity != clearing.executable_quantity()
    {
        return Err(LedgerError::CallAuctionSettlementReportInvalid);
    }
    Ok(trade_count)
}

/// Defines how an execution maps into base and quote ledger units.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SettlementConvention {
    /// Asset delivered from seller to buyer.
    pub base_asset_id: AssetId,
    /// Asset delivered from buyer to seller.
    pub quote_asset_id: AssetId,
    /// Base ledger units represented by one traded lot.
    pub base_units_per_lot: u64,
    /// Quote ledger units represented by one price quantum times one lot.
    pub quote_units_per_price_unit: u64,
}

/// Result of posting a journal entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PostReceipt {
    /// Strictly increasing journal sequence.
    pub sequence: u64,
    /// True when the exact entry had already been posted.
    pub replayed: bool,
}

/// Result of committing one indivisible correction event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CorrectionReceipt {
    /// Strictly increasing ledger-event sequence shared by both transactions.
    pub sequence: u64,
    /// True when this exact correction event was already committed.
    pub replayed: bool,
    /// Exact reversal transaction.
    pub reversal_transaction_id: TransactionId,
    /// Corrected replacement transaction.
    pub replacement_transaction_id: TransactionId,
}

/// Result of committing one indivisible multi-entry event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BatchReceipt {
    /// Strictly increasing ledger-event sequence shared by every member.
    pub sequence: u64,
    /// True when this exact grouped event was already committed.
    pub replayed: bool,
    /// Number of transaction entries committed by the event.
    pub transaction_count: usize,
}

/// Result of committing one complete call-auction settlement event.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionSettlementReceipt {
    sequence: u64,
    replayed: bool,
    transaction_count: usize,
}

/// Result of committing one full-settlement bust or replacement correction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CallAuctionSettlementCorrectionReceipt {
    sequence: u64,
    replayed: bool,
    reversal_transaction_count: usize,
    replacement_transaction_count: usize,
}

impl CallAuctionSettlementReceipt {
    /// Returns the strictly increasing ledger-event sequence.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    /// Returns whether this exact settlement event was already committed.
    #[must_use]
    pub const fn replayed(self) -> bool {
        self.replayed
    }

    /// Returns the number of DVP and fee transaction entries in the event.
    #[must_use]
    pub const fn transaction_count(self) -> usize {
        self.transaction_count
    }
}

impl From<PostReceipt> for CallAuctionSettlementReceipt {
    fn from(receipt: PostReceipt) -> Self {
        Self {
            sequence: receipt.sequence,
            replayed: receipt.replayed,
            transaction_count: 1,
        }
    }
}

impl From<BatchReceipt> for CallAuctionSettlementReceipt {
    fn from(receipt: BatchReceipt) -> Self {
        Self {
            sequence: receipt.sequence,
            replayed: receipt.replayed,
            transaction_count: receipt.transaction_count,
        }
    }
}

impl CallAuctionSettlementCorrectionReceipt {
    pub(crate) fn from_settlement_receipt(
        receipt: CallAuctionSettlementReceipt,
        reversal_transaction_count: usize,
        replacement_transaction_count: usize,
    ) -> Self {
        debug_assert_eq!(
            receipt.transaction_count,
            reversal_transaction_count + replacement_transaction_count
        );
        Self {
            sequence: receipt.sequence,
            replayed: receipt.replayed,
            reversal_transaction_count,
            replacement_transaction_count,
        }
    }

    /// Returns the strictly increasing ledger-event sequence.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    /// Returns whether this exact correction event was already committed.
    #[must_use]
    pub const fn replayed(self) -> bool {
        self.replayed
    }

    /// Returns the number of exact reversal entries.
    #[must_use]
    pub const fn reversal_transaction_count(self) -> usize {
        self.reversal_transaction_count
    }

    /// Returns the number of replacement DVP and fee entries.
    #[must_use]
    pub const fn replacement_transaction_count(self) -> usize {
        self.replacement_transaction_count
    }

    /// Returns the total reversal-plus-replacement transaction count.
    #[must_use]
    pub const fn transaction_count(self) -> usize {
        self.reversal_transaction_count + self.replacement_transaction_count
    }
}

/// Result of validating an entry against a specific ledger generation.
#[derive(Debug)]
pub enum PostingPreparation {
    /// The exact transaction is already committed.
    Replay(PostReceipt),
    /// The entry is valid and ready for a single commit.
    Ready(PreparedPosting),
}

/// Result of validating a correction against a specific ledger generation.
#[derive(Debug)]
pub enum CorrectionPreparation {
    /// The exact correction event is already committed.
    Replay(CorrectionReceipt),
    /// The correction is valid and ready for one atomic commit.
    Ready(PreparedCorrection),
}

/// Result of validating a batch against a specific ledger generation.
#[derive(Debug)]
pub enum BatchPreparation {
    /// The exact grouped event is already committed.
    Replay(BatchReceipt),
    /// The complete batch is valid and ready for one atomic commit.
    Ready(PreparedBatch),
}

/// Validated balance changes for one ledger generation.
#[derive(Debug)]
pub struct PreparedPosting {
    entry: JournalEntry,
    next_balances: Vec<BalanceUpdate>,
    period_update: PeriodUpdate,
    retained_postings_after: usize,
    expected_record_count: usize,
    sequence: u64,
}

/// Validated final balance image for one atomic correction event.
#[derive(Debug)]
pub struct PreparedCorrection {
    correction: LedgerCorrection,
    next_balances: Vec<BalanceUpdate>,
    retained_postings_after: usize,
    expected_record_count: usize,
    sequence: u64,
}

/// Validated final state for one atomic multi-entry event.
#[derive(Debug)]
pub struct PreparedBatch {
    batch: LedgerBatch,
    next_balances: Vec<BalanceUpdate>,
    final_closed_through: Option<AccountingDate>,
    new_reversals: Vec<(TransactionId, TransactionId)>,
    retained_postings_after: usize,
    expected_record_count: usize,
    sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PeriodUpdate {
    Unchanged,
    Set(AccountingDate),
    Clear,
}

impl PreparedPosting {
    /// Returns the immutable entry that must be durably recorded before commit.
    #[must_use]
    pub const fn entry(&self) -> &JournalEntry {
        &self.entry
    }
}

impl PreparedCorrection {
    /// Returns the immutable correction that must be durably recorded before commit.
    #[must_use]
    pub const fn correction(&self) -> &LedgerCorrection {
        &self.correction
    }
}

impl PreparedBatch {
    /// Returns the immutable batch that must be durably recorded before commit.
    #[must_use]
    pub const fn batch(&self) -> &LedgerBatch {
        &self.batch
    }
}

#[derive(Clone, Debug)]
struct PostedEntry {
    entry: JournalEntry,
    sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LedgerRecordKey {
    Entry(TransactionId),
    Correction {
        reversal_transaction_id: TransactionId,
        replacement_transaction_id: TransactionId,
    },
    Batch(LedgerBatch),
}

impl LedgerRecordKey {
    fn transaction_count(&self) -> usize {
        match self {
            Self::Entry(_) => 1,
            Self::Correction { .. } => 2,
            Self::Batch(batch) => batch.entries.len(),
        }
    }

    fn transaction_id_at(&self, index: usize) -> Option<TransactionId> {
        match (self, index) {
            (Self::Entry(transaction_id), 0) => Some(*transaction_id),
            (
                Self::Correction {
                    reversal_transaction_id,
                    ..
                },
                0,
            ) => Some(*reversal_transaction_id),
            (
                Self::Correction {
                    replacement_transaction_id,
                    ..
                },
                1,
            ) => Some(*replacement_transaction_id),
            (Self::Batch(batch), index) => {
                batch.entries.get(index).map(|entry| entry.transaction_id)
            }
            (Self::Entry(_) | Self::Correction { .. }, _) => None,
        }
    }
}

type BalanceKey = (AccountId, AssetId);
type BalanceUpdate = (BalanceKey, i128);

fn ensure_ledger_capacity(
    current: usize,
    added: usize,
    maximum: usize,
    resource: LedgerResource,
) -> Result<usize, LedgerError> {
    let attempted = current
        .checked_add(added)
        .ok_or(LedgerError::ArithmeticOverflow)?;
    if attempted > maximum {
        return Err(LedgerError::CapacityExceeded {
            resource,
            maximum,
            attempted,
        });
    }
    Ok(attempted)
}

fn apply_balance_updates(
    balances: &mut BoundedHashMap<BalanceKey, i128>,
    updates: Vec<BalanceUpdate>,
) {
    for (key, value) in &updates {
        if *value == 0 {
            balances.remove(key);
        }
    }
    for (key, value) in updates {
        if value != 0 {
            balances.insert(key, value);
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct AssetSideTotals {
    positive: LedgerMagnitude,
    negative: LedgerMagnitude,
}

impl AssetSideTotals {
    fn add(&mut self, amount: i128) {
        match amount.cmp(&0) {
            std::cmp::Ordering::Greater => self.positive.add_u128(amount.unsigned_abs()),
            std::cmp::Ordering::Less => self.negative.add_u128(amount.unsigned_abs()),
            std::cmp::Ordering::Equal => {}
        }
    }

    fn is_balanced(&self) -> bool {
        self.positive == self.negative
    }
}

/// One canonical non-zero account balance captured in a ledger checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerBalance {
    account_id: AccountId,
    asset_id: AssetId,
    amount: i128,
}

impl LedgerBalance {
    pub(crate) const fn from_parts(account_id: AccountId, asset_id: AssetId, amount: i128) -> Self {
        Self {
            account_id,
            asset_id,
            amount,
        }
    }

    /// Returns the account identifier.
    #[must_use]
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }

    /// Returns the asset denomination.
    #[must_use]
    pub const fn asset_id(self) -> AssetId {
        self.asset_id
    }

    /// Returns the signed balance in the asset's smallest ledger unit.
    #[must_use]
    pub const fn amount(self) -> i128 {
        self.amount
    }
}

/// Independently accumulated positive and negative balances for one asset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AssetTrialBalance {
    asset_id: AssetId,
    positive_total: LedgerMagnitude,
    negative_total: LedgerMagnitude,
}

impl AssetTrialBalance {
    /// Returns the asset denomination.
    #[must_use]
    pub const fn asset_id(&self) -> AssetId {
        self.asset_id
    }

    /// Returns the sum of strictly positive account balances.
    #[must_use]
    pub const fn positive_total(&self) -> &LedgerMagnitude {
        &self.positive_total
    }

    /// Returns the absolute sum of strictly negative account balances.
    #[must_use]
    pub const fn negative_total(&self) -> &LedgerMagnitude {
        &self.negative_total
    }

    /// Returns whether independently accumulated sides are equal.
    #[must_use]
    pub fn is_balanced(&self) -> bool {
        self.positive_total == self.negative_total
    }
}

/// One externally observed account balance used for complete-ledger reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconciliationBalance {
    /// Account observed by the external source.
    pub account_id: AccountId,
    /// Asset denomination.
    pub asset_id: AssetId,
    /// Signed balance in the asset's smallest ledger unit.
    pub amount: i128,
}

/// Immutable complete external balance statement at one ledger generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationStatement {
    reconciliation_id: ReconciliationId,
    generation: u64,
    observed_at: TimestampNs,
    source_reference: u64,
    balances: Vec<ReconciliationBalance>,
}

impl ReconciliationStatement {
    /// Validates and canonicalizes a complete double-entry balance statement.
    ///
    /// Empty statements are valid for an empty ledger. Non-empty statements
    /// must contain unique, non-zero `(account, asset)` balances whose positive
    /// and negative totals agree independently for every asset.
    ///
    /// # Errors
    ///
    /// Returns [`ReconciliationError`] for zero/duplicate balances or an
    /// unbalanced asset.
    pub fn new(
        reconciliation_id: ReconciliationId,
        generation: u64,
        observed_at: TimestampNs,
        source_reference: u64,
        mut balances: Vec<ReconciliationBalance>,
    ) -> Result<Self, ReconciliationError> {
        balances.sort_unstable_by_key(|balance| (balance.asset_id, balance.account_id));
        validate_reconciliation_balances(&balances)?;
        Ok(Self {
            reconciliation_id,
            generation,
            observed_at,
            source_reference,
            balances,
        })
    }

    /// Returns the external reconciliation identity.
    #[must_use]
    pub const fn reconciliation_id(&self) -> ReconciliationId {
        self.reconciliation_id
    }

    /// Returns the exact ledger generation represented by the statement.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns when the external state was observed, in UTC nanoseconds.
    #[must_use]
    pub const fn observed_at(&self) -> TimestampNs {
        self.observed_at
    }

    /// Returns the source-system correlation value.
    #[must_use]
    pub const fn source_reference(&self) -> u64 {
        self.source_reference
    }

    /// Returns balances in strict `(asset, account)` order.
    #[must_use]
    pub fn balances(&self) -> &[ReconciliationBalance] {
        &self.balances
    }
}

/// One canonical external-minus-ledger balance discrepancy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReconciliationDifference {
    account_id: AccountId,
    asset_id: AssetId,
    ledger_amount: i128,
    statement_amount: i128,
    difference: i128,
}

impl ReconciliationDifference {
    /// Returns the divergent account.
    #[must_use]
    pub const fn account_id(self) -> AccountId {
        self.account_id
    }

    /// Returns the divergent asset.
    #[must_use]
    pub const fn asset_id(self) -> AssetId {
        self.asset_id
    }

    /// Returns the authoritative internal balance.
    #[must_use]
    pub const fn ledger_amount(self) -> i128 {
        self.ledger_amount
    }

    /// Returns the externally observed balance.
    #[must_use]
    pub const fn statement_amount(self) -> i128 {
        self.statement_amount
    }

    /// Returns `statement_amount - ledger_amount`.
    #[must_use]
    pub const fn difference(self) -> i128 {
        self.difference
    }
}

/// Immutable result of comparing one exact-generation external statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationReport {
    reconciliation_id: ReconciliationId,
    generation: u64,
    observed_at: TimestampNs,
    source_reference: u64,
    compared_balances: usize,
    differences: Vec<ReconciliationDifference>,
}

impl ReconciliationReport {
    /// Returns the external reconciliation identity.
    #[must_use]
    pub const fn reconciliation_id(&self) -> ReconciliationId {
        self.reconciliation_id
    }

    /// Returns the compared ledger generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns when the external state was observed.
    #[must_use]
    pub const fn observed_at(&self) -> TimestampNs {
        self.observed_at
    }

    /// Returns the external source correlation value.
    #[must_use]
    pub const fn source_reference(&self) -> u64 {
        self.source_reference
    }

    /// Returns the size of the union of internal and external balance keys.
    #[must_use]
    pub const fn compared_balances(&self) -> usize {
        self.compared_balances
    }

    /// Returns only non-zero differences in strict `(asset, account)` order.
    #[must_use]
    pub fn differences(&self) -> &[ReconciliationDifference] {
        &self.differences
    }

    /// Returns whether every external balance exactly equaled internal state.
    #[must_use]
    pub fn is_reconciled(&self) -> bool {
        self.differences.is_empty()
    }
}

/// External statement validation or exact-generation comparison failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconciliationError {
    /// A zero balance was encoded instead of being omitted.
    ZeroBalance {
        /// Account carrying the zero.
        account_id: AccountId,
        /// Asset carrying the zero.
        asset_id: AssetId,
    },
    /// The same account and asset appeared more than once.
    DuplicateAccountAsset {
        /// Duplicated account.
        account_id: AccountId,
        /// Duplicated asset.
        asset_id: AssetId,
    },
    /// External positive and negative totals differed for an asset.
    Unbalanced {
        /// Unbalanced asset.
        asset_id: AssetId,
        /// Exact sum of positive balances.
        positive_total: Box<LedgerMagnitude>,
        /// Exact absolute sum of negative balances.
        negative_total: Box<LedgerMagnitude>,
    },
    /// Reconciliation cardinality exceeded its representation.
    CardinalityOverflow,
    /// The external statement did not represent the current ledger generation.
    GenerationMismatch {
        /// Current ledger generation.
        ledger_generation: u64,
        /// External statement generation.
        statement_generation: u64,
    },
    /// The statement observation preceded the last event in its claimed generation.
    ObservationPrecedesLedger {
        /// Last committed journal-event timestamp.
        last_recorded_at: TimestampNs,
        /// External observation timestamp.
        observed_at: TimestampNs,
    },
    /// One external-minus-internal signed difference exceeded `i128`.
    DifferenceOverflow {
        /// Divergent account.
        account_id: AccountId,
        /// Divergent asset.
        asset_id: AssetId,
        /// Internal value.
        ledger_amount: i128,
        /// External value.
        statement_amount: i128,
    },
}

impl fmt::Display for ReconciliationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroBalance {
                account_id,
                asset_id,
            } => write!(
                formatter,
                "reconciliation balance for account {account_id}, asset {asset_id} is zero"
            ),
            Self::DuplicateAccountAsset {
                account_id,
                asset_id,
            } => write!(
                formatter,
                "reconciliation repeats account {account_id}, asset {asset_id}"
            ),
            Self::Unbalanced {
                asset_id,
                positive_total,
                negative_total,
            } => write!(
                formatter,
                "reconciliation asset {asset_id} has positive total {positive_total} and negative total {negative_total}"
            ),
            Self::CardinalityOverflow => formatter.write_str("reconciliation cardinality overflow"),
            Self::GenerationMismatch {
                ledger_generation,
                statement_generation,
            } => write!(
                formatter,
                "reconciliation generation {statement_generation} differs from ledger generation {ledger_generation}"
            ),
            Self::ObservationPrecedesLedger {
                last_recorded_at,
                observed_at,
            } => write!(
                formatter,
                "reconciliation observation {} precedes ledger timestamp {}",
                observed_at.as_unix_nanos(),
                last_recorded_at.as_unix_nanos()
            ),
            Self::DifferenceOverflow {
                account_id,
                asset_id,
                ledger_amount,
                statement_amount,
            } => write!(
                formatter,
                "reconciliation difference overflows for account {account_id}, asset {asset_id}: external {statement_amount} minus ledger {ledger_amount}"
            ),
        }
    }
}

impl std::error::Error for ReconciliationError {}

/// Immutable canonical ledger state plus complete idempotency history.
///
/// Balance and record vectors are immutable shared values. Cloning the complete
/// checkpoint is `O(1)` and allocates no row or nested posting/batch storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerCheckpoint {
    generation: u64,
    balances: Arc<Vec<LedgerBalance>>,
    records: Arc<Vec<LedgerRecord>>,
}

impl LedgerCheckpoint {
    pub(crate) fn from_parts(
        generation: u64,
        balances: Vec<LedgerBalance>,
        records: Vec<LedgerRecord>,
    ) -> Result<Self, LedgerCheckpointError> {
        validate_checkpoint(generation, &balances, &records)?;
        Ok(Self {
            generation,
            balances: Arc::new(balances),
            records: Arc::new(records),
        })
    }

    /// Returns the number of indivisible ledger events covered by the checkpoint.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns non-zero balances in strict `(asset, account)` order.
    #[must_use]
    pub fn balances(&self) -> &[LedgerBalance] {
        self.balances.as_slice()
    }

    /// Returns canonical ledger events in sequence order.
    #[must_use]
    pub fn records(&self) -> &[LedgerRecord] {
        self.records.as_slice()
    }

    /// Returns whether two checkpoints share the identical immutable balance image.
    #[must_use]
    pub fn shares_balance_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.balances, &other.balances)
    }

    /// Returns whether two checkpoints share the identical immutable record image.
    #[must_use]
    pub fn shares_record_storage_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.records, &other.records)
    }
}

/// One fallibly reserved ledger-checkpoint capture or audit resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerCheckpointCaptureResource {
    /// Chronological materialized records retained by the checkpoint.
    CaptureRecords,
    /// Canonical non-zero balance rows retained by the checkpoint.
    CaptureBalances,
    /// Flat trial-balance terms used by the independent audit.
    AuditTrialBalanceTerms,
    /// Per-asset trial-balance rows used by the independent audit.
    AuditTrialBalanceOutput,
}

impl fmt::Display for LedgerCheckpointCaptureResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::CaptureRecords => "capture records",
            Self::CaptureBalances => "capture balances",
            Self::AuditTrialBalanceTerms => "audit trial-balance terms",
            Self::AuditTrialBalanceOutput => "audit trial-balance output",
        })
    }
}

/// Ledger checkpoint capture failure before snapshot or WAL-cutover mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerCheckpointCaptureError {
    /// Live authoritative state contradicted its structural or replay invariants.
    Invalid(LedgerInvariantViolation),
    /// A temporary replay ledger could not reserve its complete layout.
    Construction(LedgerConstructionError),
    /// A capture or audit vector could not be represented or reserved.
    ResourceReservationFailed {
        /// Resource whose construction failed.
        resource: LedgerCheckpointCaptureResource,
        /// Requested exact maximum entries.
        maximum: usize,
    },
}

impl LedgerCheckpointCaptureError {
    /// Returns a stable diagnostic description.
    #[must_use]
    pub fn detail(&self) -> &str {
        match self {
            Self::Invalid(error) => error.detail(),
            Self::Construction(_) => "ledger checkpoint replay construction failed",
            Self::ResourceReservationFailed { resource, .. } => match resource {
                LedgerCheckpointCaptureResource::CaptureRecords => {
                    "ledger checkpoint record capture reservation failed"
                }
                LedgerCheckpointCaptureResource::CaptureBalances => {
                    "ledger checkpoint balance capture reservation failed"
                }
                LedgerCheckpointCaptureResource::AuditTrialBalanceTerms => {
                    "ledger checkpoint trial-balance term reservation failed"
                }
                LedgerCheckpointCaptureResource::AuditTrialBalanceOutput => {
                    "ledger checkpoint trial-balance output reservation failed"
                }
            },
        }
    }

    /// Returns the failed capture or audit resource.
    #[must_use]
    pub const fn resource(&self) -> Option<LedgerCheckpointCaptureResource> {
        match self {
            Self::ResourceReservationFailed { resource, .. } => Some(*resource),
            Self::Invalid(_) | Self::Construction(_) => None,
        }
    }

    /// Returns the preserved temporary replay-ledger construction failure.
    #[must_use]
    pub const fn construction_error(&self) -> Option<&LedgerConstructionError> {
        match self {
            Self::Construction(error) => Some(error),
            Self::Invalid(_) | Self::ResourceReservationFailed { .. } => None,
        }
    }

    /// Returns whether one capture or audit reservation failed.
    #[must_use]
    pub const fn is_resource_exhaustion(&self) -> bool {
        matches!(self, Self::ResourceReservationFailed { .. })
    }

    /// Returns whether retry under different resource availability can succeed.
    #[must_use]
    pub const fn is_operational_failure(&self) -> bool {
        matches!(
            self,
            Self::Construction(_) | Self::ResourceReservationFailed { .. }
        )
    }
}

impl fmt::Display for LedgerCheckpointCaptureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(error) => error.fmt(formatter),
            Self::Construction(error) => {
                write!(
                    formatter,
                    "failed to construct ledger checkpoint replay: {error}"
                )
            }
            Self::ResourceReservationFailed { resource, maximum } => write!(
                formatter,
                "failed to reserve ledger checkpoint {resource} through {maximum} entries"
            ),
        }
    }
}

impl std::error::Error for LedgerCheckpointCaptureError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Invalid(error) => Some(error),
            Self::Construction(error) => Some(error),
            Self::ResourceReservationFailed { .. } => None,
        }
    }
}

impl From<LedgerInvariantViolation> for LedgerCheckpointCaptureError {
    fn from(error: LedgerInvariantViolation) -> Self {
        Self::Invalid(error)
    }
}

/// Semantic checkpoint validation or restoration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerCheckpointError {
    /// Generation did not equal the number of persisted ledger events.
    GenerationMismatch {
        /// Encoded generation.
        generation: u64,
        /// Number of events represented by the checkpoint.
        record_count: usize,
    },
    /// Balances were zero, duplicated, or not strictly `(asset, account)` sorted.
    NonCanonicalBalances,
    /// Replaying a checkpoint event failed.
    RecordReplay {
        /// Zero-based event position.
        index: usize,
        /// Ledger failure raised by deterministic replay.
        error: LedgerError,
    },
    /// An exact transaction retry appeared in another checkpoint event.
    DuplicateTransaction {
        /// Zero-based duplicate entry position.
        index: usize,
        /// Repeated transaction identifier.
        transaction_id: TransactionId,
    },
    /// Replayed balances differed from the redundant checkpoint balance image.
    BalanceMismatch,
    /// Checkpoint cardinalities could not be represented by platform `usize`.
    CardinalityOverflow,
    /// A complete replay layout could not be constructed.
    Construction(LedgerConstructionError),
}

impl fmt::Display for LedgerCheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationMismatch {
                generation,
                record_count,
            } => write!(
                formatter,
                "ledger checkpoint generation {generation} differs from {record_count} records"
            ),
            Self::NonCanonicalBalances => formatter.write_str(
                "ledger checkpoint balances must be non-zero and strictly sorted by asset/account",
            ),
            Self::RecordReplay { index, error } => {
                write!(
                    formatter,
                    "ledger checkpoint record {index} failed replay: {error}"
                )
            }
            Self::DuplicateTransaction {
                index,
                transaction_id,
            } => write!(
                formatter,
                "ledger checkpoint record {index} duplicates transaction {transaction_id}"
            ),
            Self::BalanceMismatch => formatter
                .write_str("ledger checkpoint balances differ from deterministic record replay"),
            Self::CardinalityOverflow => {
                formatter.write_str("ledger checkpoint cardinality overflow")
            }
            Self::Construction(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for LedgerCheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RecordReplay { error, .. } => Some(error),
            Self::Construction(error) => Some(error),
            _ => None,
        }
    }
}

/// Structural inconsistency between ledger indexes, entries, and balances.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerInvariantViolation {
    detail: String,
}

impl LedgerInvariantViolation {
    pub(crate) fn new(detail: impl Into<String>) -> Self {
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

impl fmt::Display for LedgerInvariantViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.detail.fmt(formatter)
    }
}

impl std::error::Error for LedgerInvariantViolation {}

/// In-memory atomic double-entry ledger.
///
/// The ledger permits signed balances. Credit limits, reservations, margin,
/// and account-state policies belong to a pre-posting risk layer.
#[derive(Debug)]
pub struct Ledger {
    limits: LedgerLimits,
    balances: BoundedHashMap<(AccountId, AssetId), i128>,
    entries: BoundedHashMap<TransactionId, PostedEntry>,
    reversals: BoundedHashMap<TransactionId, TransactionId>,
    journal: Vec<LedgerRecordKey>,
    retained_postings: usize,
    closed_through: Option<AccountingDate>,
    last_recorded_at: Option<TimestampNs>,
}

impl Default for Ledger {
    fn default() -> Self {
        Self::new()
    }
}

impl Ledger {
    /// Creates an empty ledger under the finite default resource envelope.
    ///
    /// Production initialization that must report reservation failure uses
    /// [`Ledger::try_with_limits`].
    ///
    /// # Panics
    ///
    /// Panics if the process cannot reserve the documented default envelope.
    #[must_use]
    pub fn new() -> Self {
        Self::try_with_limits(LedgerLimitsSpec::default())
            .expect("default ledger resource envelope must be representable")
    }

    /// Creates an empty ledger after reserving every authoritative index and
    /// journal slot to its complete configured maximum.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerConstructionError`] before state exists if limits are
    /// contradictory or one complete fixed layout cannot be allocated.
    pub fn try_with_limits(spec: LedgerLimitsSpec) -> Result<Self, LedgerConstructionError> {
        let limits =
            LedgerLimits::try_from(spec).map_err(LedgerConstructionError::InvalidLimits)?;
        let balances = reserve_ledger_map(spec.max_balance_keys, LedgerResource::BalanceKeys)?;
        let entries = reserve_ledger_map(spec.max_transactions, LedgerResource::Transactions)?;
        let reversals = reserve_ledger_map(spec.max_reversals, LedgerResource::Reversals)?;
        let mut journal = Vec::new();
        journal.try_reserve_exact(spec.max_records).map_err(|_| {
            LedgerConstructionError::ReservationFailed {
                resource: LedgerResource::Records,
                maximum: spec.max_records,
            }
        })?;
        Ok(Self {
            limits,
            balances,
            entries,
            reversals,
            journal,
            retained_postings: 0,
            closed_through: None,
            last_recorded_at: None,
        })
    }

    /// Returns the immutable validated generation limits.
    #[must_use]
    pub const fn limits(&self) -> LedgerLimits {
        self.limits
    }

    /// Returns fixed allocation and occupancy telemetry for one authoritative
    /// ledger hash index.
    #[must_use]
    pub fn hash_index_status(&self, index: LedgerHashIndex) -> LedgerHashIndexStatus {
        fn status<K, V>(map: &BoundedHashMap<K, V>) -> LedgerHashIndexStatus
        where
            K: Eq + Hash,
        {
            LedgerHashIndexStatus {
                occupied_entries: map.len(),
                maximum_entries: map.maximum(),
                allocated_entries: map.capacity(),
                initialized_buckets: map.bucket_count(),
            }
        }

        match index {
            LedgerHashIndex::BalanceKeys => status(&self.balances),
            LedgerHashIndex::Transactions => status(&self.entries),
            LedgerHashIndex::Reversals => status(&self.reversals),
        }
    }

    /// Returns fixed allocation and occupancy telemetry for journal order.
    #[must_use]
    pub fn journal_status(&self) -> LedgerJournalStatus {
        LedgerJournalStatus {
            occupied_records: self.journal.len(),
            maximum_records: self.limits.0.max_records,
            allocated_records: self.journal.capacity(),
        }
    }

    /// Returns the number of stored non-zero `(account, asset)` balances.
    #[must_use]
    pub fn nonzero_balance_count(&self) -> usize {
        self.balances.len()
    }

    /// Returns the number of posting legs retained in transaction history.
    #[must_use]
    pub const fn retained_posting_count(&self) -> usize {
        self.retained_postings
    }

    fn preflight_event_capacity(
        &self,
        transaction_count: usize,
        reversal_count: usize,
        posting_count: usize,
        next_balances: &[BalanceUpdate],
    ) -> Result<usize, LedgerError> {
        let limits = self.limits.0;
        ensure_ledger_capacity(
            self.entries.len(),
            transaction_count,
            limits.max_transactions,
            LedgerResource::Transactions,
        )?;
        ensure_ledger_capacity(
            self.journal.len(),
            1,
            limits.max_records,
            LedgerResource::Records,
        )?;
        ensure_ledger_capacity(
            self.reversals.len(),
            reversal_count,
            limits.max_reversals,
            LedgerResource::Reversals,
        )?;
        let retained_postings_after = ensure_ledger_capacity(
            self.retained_postings,
            posting_count,
            limits.max_retained_postings,
            LedgerResource::RetainedPostings,
        )?;
        let mut balances_after = self.balances.len();
        for (key, value) in next_balances {
            match (self.balances.contains_key(key), *value == 0) {
                (true, true) => {
                    balances_after = balances_after
                        .checked_sub(1)
                        .ok_or(LedgerError::ArithmeticOverflow)?;
                }
                (false, false) => {
                    balances_after = balances_after
                        .checked_add(1)
                        .ok_or(LedgerError::ArithmeticOverflow)?;
                }
                (true, false) | (false, true) => {}
            }
        }
        if balances_after > limits.max_balance_keys {
            return Err(LedgerError::CapacityExceeded {
                resource: LedgerResource::BalanceKeys,
                maximum: limits.max_balance_keys,
                attempted: balances_after,
            });
        }
        Ok(retained_postings_after)
    }

    fn validate_entry_cardinality(&self, entry: &JournalEntry) -> Result<(), LedgerError> {
        let maximum = self.limits.0.max_postings_per_transaction;
        if entry.postings.len() > maximum {
            return Err(LedgerError::CapacityExceeded {
                resource: LedgerResource::PostingsPerTransaction,
                maximum,
                attempted: entry.postings.len(),
            });
        }
        Ok(())
    }

    fn validate_record_cardinality(&self, transaction_count: usize) -> Result<(), LedgerError> {
        let maximum = self.limits.0.max_transactions_per_record;
        if transaction_count > maximum {
            return Err(LedgerError::CapacityExceeded {
                resource: LedgerResource::TransactionsPerRecord,
                maximum,
                attempted: transaction_count,
            });
        }
        Ok(())
    }

    fn validate_entry_lifecycle(&self, entry: &JournalEntry) -> Result<PeriodUpdate, LedgerError> {
        if let Some(previous) = self.last_recorded_at {
            if entry.recorded_at < previous {
                return Err(LedgerError::RecordedTimestampRegression {
                    previous,
                    proposed: entry.recorded_at,
                });
            }
        }
        match entry.kind {
            LedgerEntryKind::Standard => {
                validate_financial_entry(entry, self.closed_through)?;
                Ok(PeriodUpdate::Unchanged)
            }
            LedgerEntryKind::Reversal {
                reversed_transaction_id,
            } => {
                validate_financial_entry(entry, self.closed_through)?;
                let original = self
                    .entries
                    .get(&reversed_transaction_id)
                    .ok_or(LedgerError::ReversalTargetMissing(reversed_transaction_id))?;
                if matches!(
                    original.entry.kind,
                    LedgerEntryKind::PeriodClose { .. } | LedgerEntryKind::PeriodReopen { .. }
                ) {
                    return Err(LedgerError::NonFinancialReversalTarget(
                        reversed_transaction_id,
                    ));
                }
                if let Some(&reversal_transaction_id) = self.reversals.get(&reversed_transaction_id)
                {
                    return Err(LedgerError::TransactionAlreadyReversed {
                        original_transaction_id: reversed_transaction_id,
                        reversal_transaction_id,
                    });
                }
                if !postings_are_exact_inverse(&entry.postings, &original.entry.postings) {
                    return Err(LedgerError::InvalidReversalPostings(
                        reversed_transaction_id,
                    ));
                }
                Ok(PeriodUpdate::Unchanged)
            }
            LedgerEntryKind::PeriodClose { closed_through } => {
                validate_control_entry(entry)?;
                if self
                    .closed_through
                    .is_some_and(|current| closed_through <= current)
                {
                    return Err(LedgerError::PeriodCloseNotAdvancing {
                        current_closed_through: self.closed_through,
                        proposed_closed_through: closed_through,
                    });
                }
                Ok(PeriodUpdate::Set(closed_through))
            }
            LedgerEntryKind::PeriodReopen { new_closed_through } => {
                validate_control_entry(entry)?;
                let current_closed_through = self
                    .closed_through
                    .ok_or(LedgerError::AccountingPeriodAlreadyOpen)?;
                if new_closed_through.is_some_and(|proposed| proposed >= current_closed_through) {
                    return Err(LedgerError::InvalidPeriodReopen {
                        current_closed_through,
                        proposed_closed_through: new_closed_through,
                    });
                }
                Ok(new_closed_through.map_or(PeriodUpdate::Clear, PeriodUpdate::Set))
            }
        }
    }

    /// Applies a balanced entry atomically.
    ///
    /// Exact retries return the original sequence. A collision, validation
    /// error, or arithmetic overflow leaves every balance unchanged.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] on invalid entry content, identifier collision,
    /// sequence exhaustion, or balance overflow.
    pub fn post(&mut self, entry: JournalEntry) -> Result<PostReceipt, LedgerError> {
        match self.prepare(entry)? {
            PostingPreparation::Replay(receipt) => Ok(receipt),
            PostingPreparation::Ready(prepared) => self.commit(prepared),
        }
    }

    /// Applies an exact reversal and its ordinary replacement as one event.
    ///
    /// No state exposing only one member of the correction is observable. The
    /// final balance delta is calculated directly from both canonical posting
    /// sets, so an unrepresentable intermediate balance is not materialized.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for collision, partial prior commitment,
    /// reversal-lineage, period, timestamp, sequence, or final-balance failure.
    pub fn correct(
        &mut self,
        correction: LedgerCorrection,
    ) -> Result<CorrectionReceipt, LedgerError> {
        match self.prepare_correction(correction)? {
            CorrectionPreparation::Replay(receipt) => Ok(receipt),
            CorrectionPreparation::Ready(prepared) => self.commit_correction(prepared),
        }
    }

    /// Applies two or more ordered entries as one indivisible ledger event.
    ///
    /// Lifecycle and lineage rules are evaluated in declared entry order.
    /// Balance effects are aggregated directly into a final image, so neither
    /// readers nor arithmetic observe a partial batch.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for collision, partial prior commitment,
    /// reversal-lineage, period, timestamp, sequence, or final-balance failure.
    pub fn post_batch(&mut self, batch: LedgerBatch) -> Result<BatchReceipt, LedgerError> {
        match self.prepare_batch(batch)? {
            BatchPreparation::Replay(receipt) => Ok(receipt),
            BatchPreparation::Ready(prepared) => self.commit_batch(prepared),
        }
    }

    /// Atomically commits every DVP and explicit fee entry from one uncross.
    ///
    /// A one-trade fee-free uncross is one ordinary ledger entry. Multiple
    /// trades or any explicit fee use one batch event, with exact-retry and
    /// collision semantics delegated to the same canonical posting paths as
    /// direct ledger use.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for collision, partial prior commitment,
    /// accounting-period, timestamp, capacity, or final-balance failure.
    pub fn settle_call_auction(
        &mut self,
        settlement: CallAuctionSettlement,
    ) -> Result<CallAuctionSettlementReceipt, LedgerError> {
        match settlement.record {
            CallAuctionSettlementRecord::Entry(entry) => self.post(entry).map(Into::into),
            CallAuctionSettlementRecord::Batch(batch) => self.post_batch(batch).map(Into::into),
        }
    }

    /// Atomically busts or replaces one exact committed call-auction settlement.
    ///
    /// The original DVP and fee transactions must still identify the exact
    /// entry or batch event created by [`Ledger::settle_call_auction`]. Every
    /// original entry is reversed before any optional replacement entry, while
    /// balances expose only the complete final image.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for missing or differently grouped original
    /// settlement state, reversal lineage, collision, partial prior commitment,
    /// period, timestamp, capacity, or final-balance failure.
    pub fn correct_call_auction(
        &mut self,
        correction: CallAuctionSettlementCorrection,
    ) -> Result<CallAuctionSettlementCorrectionReceipt, LedgerError> {
        let (original, record, reversal_count, replacement_count) = correction.into_parts();
        self.validate_committed_call_auction_settlement(&original)?;
        let receipt = match record {
            CallAuctionSettlementRecord::Entry(entry) => self.post(entry).map(Into::into),
            CallAuctionSettlementRecord::Batch(batch) => self.post_batch(batch).map(Into::into),
        }?;
        Ok(
            CallAuctionSettlementCorrectionReceipt::from_settlement_receipt(
                receipt,
                reversal_count,
                replacement_count,
            ),
        )
    }

    pub(crate) fn validate_committed_call_auction_settlement(
        &self,
        settlement: &CallAuctionSettlement,
    ) -> Result<(), LedgerError> {
        match &settlement.record {
            CallAuctionSettlementRecord::Entry(entry) => {
                let transaction_id = entry.transaction_id;
                let posted = self
                    .entries
                    .get(&transaction_id)
                    .ok_or(LedgerError::ReversalTargetMissing(transaction_id))?;
                if posted.entry != *entry {
                    return Err(LedgerError::TransactionIdCollision(transaction_id));
                }
                let record_index = posted
                    .sequence
                    .checked_sub(1)
                    .and_then(|value| usize::try_from(value).ok());
                if record_index.and_then(|index| self.journal.get(index))
                    != Some(&LedgerRecordKey::Entry(transaction_id))
                {
                    return Err(LedgerError::CallAuctionCorrectionOriginalGroupingMismatch(
                        transaction_id,
                    ));
                }
                Ok(())
            }
            CallAuctionSettlementRecord::Batch(batch) => {
                if self.existing_batch_receipt(batch)?.is_some() {
                    Ok(())
                } else {
                    Err(LedgerError::ReversalTargetMissing(
                        settlement.record.primary_transaction_id(),
                    ))
                }
            }
        }
    }

    /// Validates a complete batch against the current ledger generation.
    ///
    /// Reversal targets and period controls introduced earlier in the same
    /// batch are visible to later members. Later members are never visible to
    /// earlier ones. Pending transaction and reversal overlays are exact
    /// `N`-bounded dense/open-addressed hashes. No ledger state is mutated
    /// during preparation, and commit consumes only already-owned storage.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] when exact replay cannot be established or any
    /// ordered state transition or aggregate final balance is invalid.
    pub fn prepare_batch(&self, batch: LedgerBatch) -> Result<BatchPreparation, LedgerError> {
        if let Some(receipt) = self.existing_batch_receipt(&batch)? {
            return Ok(BatchPreparation::Replay(receipt));
        }
        self.validate_record_cardinality(batch.entries.len())?;
        let posting_count = batch.entries.iter().try_fold(0_usize, |count, entry| {
            self.validate_entry_cardinality(entry)?;
            count
                .checked_add(entry.postings.len())
                .ok_or(LedgerError::ArithmeticOverflow)
        })?;
        let mut previous_recorded_at = self.last_recorded_at;
        let mut closed_through = self.closed_through;
        let mut pending_entries = reserve_ledger_preparation_map(
            batch.entries.len(),
            LedgerPreparationResource::PendingTransactions,
        )?;
        let mut pending_reversals = reserve_ledger_preparation_map(
            batch.entries.len(),
            LedgerPreparationResource::PendingReversals,
        )?;
        let mut new_reversals = Vec::new();
        new_reversals
            .try_reserve_exact(batch.entries.len())
            .map_err(|_| {
                LedgerError::PreparationAllocationFailed(LedgerPreparationResource::NewReversals)
            })?;

        for entry in batch.entries.iter() {
            if let Some(previous) = previous_recorded_at {
                if entry.recorded_at < previous {
                    return Err(LedgerError::RecordedTimestampRegression {
                        previous,
                        proposed: entry.recorded_at,
                    });
                }
            }
            validate_batch_lifecycle_entry(
                entry,
                &mut closed_through,
                &self.entries,
                &pending_entries,
                &self.reversals,
                &mut pending_reversals,
                &mut new_reversals,
            )?;
            if pending_entries
                .insert(entry.transaction_id, entry)
                .is_some()
            {
                return Err(LedgerError::BatchDuplicateTransaction(entry.transaction_id));
            }
            previous_recorded_at = Some(entry.recorded_at);
        }

        let next_balances = calculate_batch_balances(&self.balances, batch.entries.as_slice())?;
        let retained_postings_after = self.preflight_event_capacity(
            batch.entries.len(),
            new_reversals.len(),
            posting_count,
            &next_balances,
        )?;
        let sequence = u64::try_from(self.journal.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(LedgerError::ArithmeticOverflow)?;
        Ok(BatchPreparation::Ready(PreparedBatch {
            batch,
            next_balances,
            final_closed_through: closed_through,
            new_reversals,
            retained_postings_after,
            expected_record_count: self.journal.len(),
            sequence,
        }))
    }

    fn existing_batch_receipt(
        &self,
        batch: &LedgerBatch,
    ) -> Result<Option<BatchReceipt>, LedgerError> {
        let mut first_present = None;
        let mut present_count = 0_usize;
        let mut sequence = None;
        for entry in batch.entries.iter() {
            let Some(existing) = self.entries.get(&entry.transaction_id) else {
                continue;
            };
            if existing.entry != *entry {
                return Err(LedgerError::TransactionIdCollision(entry.transaction_id));
            }
            first_present.get_or_insert(entry.transaction_id);
            present_count = present_count
                .checked_add(1)
                .ok_or(LedgerError::ArithmeticOverflow)?;
            sequence.get_or_insert(existing.sequence);
        }
        let Some(first_present) = first_present else {
            return Ok(None);
        };
        if present_count != batch.entries.len() {
            return Err(LedgerError::BatchAlreadyPartiallyCommitted(first_present));
        }
        let sequence =
            sequence.ok_or(LedgerError::BatchAlreadyPartiallyCommitted(first_present))?;
        if batch.entries.iter().any(|entry| {
            self.entries
                .get(&entry.transaction_id)
                .is_none_or(|posted| posted.sequence != sequence)
        }) {
            return Err(LedgerError::BatchAlreadyPartiallyCommitted(first_present));
        }
        let record_index = sequence
            .checked_sub(1)
            .and_then(|value| usize::try_from(value).ok());
        let exact_group = record_index
            .and_then(|index| self.journal.get(index))
            .is_some_and(|record| match record {
                LedgerRecordKey::Batch(committed) => committed == batch,
                LedgerRecordKey::Entry(_) | LedgerRecordKey::Correction { .. } => false,
            });
        if !exact_group {
            return Err(LedgerError::BatchAlreadyPartiallyCommitted(first_present));
        }
        Ok(Some(BatchReceipt {
            sequence,
            replayed: true,
            transaction_count: batch.entries.len(),
        }))
    }

    /// Commits a prepared batch without another fallible business calculation.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::StalePreparation`] if another event committed
    /// after preparation or any batch transaction is now present.
    pub fn commit_batch(&mut self, prepared: PreparedBatch) -> Result<BatchReceipt, LedgerError> {
        if self.journal.len() != prepared.expected_record_count
            || prepared
                .batch
                .entries
                .iter()
                .any(|entry| self.entries.contains_key(&entry.transaction_id))
        {
            return Err(LedgerError::StalePreparation);
        }
        apply_balance_updates(&mut self.balances, prepared.next_balances);
        self.closed_through = prepared.final_closed_through;
        for (original_transaction_id, reversal_transaction_id) in prepared.new_reversals {
            self.reversals
                .insert(original_transaction_id, reversal_transaction_id);
        }
        let transaction_count = prepared.batch.entries.len();
        let last_recorded_at = prepared.batch.entries[transaction_count - 1].recorded_at;
        for entry in prepared.batch.entries.iter().cloned() {
            let transaction_id = entry.transaction_id;
            self.entries.insert(
                transaction_id,
                PostedEntry {
                    entry,
                    sequence: prepared.sequence,
                },
            );
        }
        self.journal
            .push(LedgerRecordKey::Batch(prepared.batch.clone()));
        self.retained_postings = prepared.retained_postings_after;
        self.last_recorded_at = Some(last_recorded_at);
        Ok(BatchReceipt {
            sequence: prepared.sequence,
            replayed: false,
            transaction_count,
        })
    }

    /// Validates an indivisible correction against the current ledger generation.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] without mutation when either member, lineage,
    /// effective date, timestamp, sequence, or final balance is invalid.
    pub fn prepare_correction(
        &self,
        correction: LedgerCorrection,
    ) -> Result<CorrectionPreparation, LedgerError> {
        if let Some(receipt) = self.existing_correction_receipt(&correction)? {
            return Ok(CorrectionPreparation::Replay(receipt));
        }
        self.validate_record_cardinality(2)?;
        self.validate_entry_cardinality(&correction.reversal)?;
        self.validate_entry_cardinality(&correction.replacement)?;
        if let Some(previous) = self.last_recorded_at {
            if correction.reversal.recorded_at < previous {
                return Err(LedgerError::RecordedTimestampRegression {
                    previous,
                    proposed: correction.reversal.recorded_at,
                });
            }
        }
        validate_financial_entry(&correction.reversal, self.closed_through)?;
        validate_financial_entry(&correction.replacement, self.closed_through)?;
        let reversed_transaction_id = correction.corrected_transaction_id();
        let original = self
            .entries
            .get(&reversed_transaction_id)
            .ok_or(LedgerError::ReversalTargetMissing(reversed_transaction_id))?;
        if matches!(
            original.entry.kind,
            LedgerEntryKind::PeriodClose { .. } | LedgerEntryKind::PeriodReopen { .. }
        ) {
            return Err(LedgerError::NonFinancialReversalTarget(
                reversed_transaction_id,
            ));
        }
        if let Some(&existing_reversal_transaction_id) =
            self.reversals.get(&reversed_transaction_id)
        {
            return Err(LedgerError::TransactionAlreadyReversed {
                original_transaction_id: reversed_transaction_id,
                reversal_transaction_id: existing_reversal_transaction_id,
            });
        }
        if !postings_are_exact_inverse(&correction.reversal.postings, &original.entry.postings) {
            return Err(LedgerError::InvalidReversalPostings(
                reversed_transaction_id,
            ));
        }
        let next_balances = calculate_correction_balances(
            &self.balances,
            &correction.reversal.postings,
            &correction.replacement.postings,
        )?;
        let posting_count = correction
            .reversal
            .postings
            .len()
            .checked_add(correction.replacement.postings.len())
            .ok_or(LedgerError::ArithmeticOverflow)?;
        let retained_postings_after =
            self.preflight_event_capacity(2, 1, posting_count, &next_balances)?;
        let sequence = u64::try_from(self.journal.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(LedgerError::ArithmeticOverflow)?;
        Ok(CorrectionPreparation::Ready(PreparedCorrection {
            correction,
            next_balances,
            retained_postings_after,
            expected_record_count: self.journal.len(),
            sequence,
        }))
    }

    fn existing_correction_receipt(
        &self,
        correction: &LedgerCorrection,
    ) -> Result<Option<CorrectionReceipt>, LedgerError> {
        let reversal_transaction_id = correction.reversal.transaction_id;
        let replacement_transaction_id = correction.replacement.transaction_id;
        let reversal = self.entries.get(&reversal_transaction_id);
        let replacement = self.entries.get(&replacement_transaction_id);
        match (reversal, replacement) {
            (None, None) => Ok(None),
            (Some(reversal), Some(replacement)) => {
                if reversal.entry != correction.reversal {
                    return Err(LedgerError::TransactionIdCollision(reversal_transaction_id));
                }
                if replacement.entry != correction.replacement {
                    return Err(LedgerError::TransactionIdCollision(
                        replacement_transaction_id,
                    ));
                }
                let record_index = reversal
                    .sequence
                    .checked_sub(1)
                    .and_then(|value| usize::try_from(value).ok());
                if reversal.sequence != replacement.sequence
                    || record_index.and_then(|index| self.journal.get(index))
                        != Some(&LedgerRecordKey::Correction {
                            reversal_transaction_id,
                            replacement_transaction_id,
                        })
                {
                    return Err(LedgerError::CorrectionAlreadyPartiallyCommitted(
                        reversal_transaction_id,
                    ));
                }
                Ok(Some(CorrectionReceipt {
                    sequence: reversal.sequence,
                    replayed: true,
                    reversal_transaction_id,
                    replacement_transaction_id,
                }))
            }
            (Some(existing), None) => {
                if existing.entry != correction.reversal {
                    return Err(LedgerError::TransactionIdCollision(reversal_transaction_id));
                }
                Err(LedgerError::CorrectionAlreadyPartiallyCommitted(
                    reversal_transaction_id,
                ))
            }
            (None, Some(existing)) => {
                if existing.entry != correction.replacement {
                    return Err(LedgerError::TransactionIdCollision(
                        replacement_transaction_id,
                    ));
                }
                Err(LedgerError::CorrectionAlreadyPartiallyCommitted(
                    replacement_transaction_id,
                ))
            }
        }
    }

    /// Commits a prepared correction without another fallible calculation.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::StalePreparation`] if any event committed after
    /// preparation or either correction transaction is now present.
    pub fn commit_correction(
        &mut self,
        prepared: PreparedCorrection,
    ) -> Result<CorrectionReceipt, LedgerError> {
        let reversal_transaction_id = prepared.correction.reversal.transaction_id;
        let replacement_transaction_id = prepared.correction.replacement.transaction_id;
        if self.journal.len() != prepared.expected_record_count
            || self.entries.contains_key(&reversal_transaction_id)
            || self.entries.contains_key(&replacement_transaction_id)
        {
            return Err(LedgerError::StalePreparation);
        }
        apply_balance_updates(&mut self.balances, prepared.next_balances);
        let reversed_transaction_id = prepared.correction.corrected_transaction_id();
        self.reversals
            .insert(reversed_transaction_id, reversal_transaction_id);
        let recorded_at = prepared.correction.replacement.recorded_at;
        self.entries.insert(
            reversal_transaction_id,
            PostedEntry {
                entry: prepared.correction.reversal,
                sequence: prepared.sequence,
            },
        );
        self.entries.insert(
            replacement_transaction_id,
            PostedEntry {
                entry: prepared.correction.replacement,
                sequence: prepared.sequence,
            },
        );
        self.journal.push(LedgerRecordKey::Correction {
            reversal_transaction_id,
            replacement_transaction_id,
        });
        self.retained_postings = prepared.retained_postings_after;
        self.last_recorded_at = Some(recorded_at);
        Ok(CorrectionReceipt {
            sequence: prepared.sequence,
            replayed: false,
            reversal_transaction_id,
            replacement_transaction_id,
        })
    }

    /// Constructs and posts the exact reversal of a committed transaction.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if the target is absent/already reversed, an
    /// amount has no representable inverse, or ordinary posting fails.
    pub fn reverse(
        &mut self,
        transaction_id: TransactionId,
        reference: u64,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        reversed_transaction_id: TransactionId,
    ) -> Result<PostReceipt, LedgerError> {
        let original = self
            .entries
            .get(&reversed_transaction_id)
            .ok_or(LedgerError::ReversalTargetMissing(reversed_transaction_id))?;
        let reversal = JournalEntry::reversal(
            transaction_id,
            reference,
            effective_date,
            recorded_at,
            &original.entry,
        )?;
        self.post(reversal)
    }

    /// Advances the inclusive closed accounting-date boundary durably.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if the transaction collides, booking time
    /// regresses, or the proposed boundary does not advance.
    pub fn close_period(
        &mut self,
        transaction_id: TransactionId,
        reference: u64,
        recorded_at: TimestampNs,
        closed_through: AccountingDate,
    ) -> Result<PostReceipt, LedgerError> {
        self.post(JournalEntry::period_close(
            transaction_id,
            reference,
            recorded_at,
            closed_through,
        )?)
    }

    /// Moves the close boundary backward or removes it durably.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] if all dates are already open, the transaction
    /// collides, booking time regresses, or the proposed boundary does not
    /// precede the current boundary.
    pub fn reopen_period(
        &mut self,
        transaction_id: TransactionId,
        reference: u64,
        recorded_at: TimestampNs,
        new_closed_through: Option<AccountingDate>,
    ) -> Result<PostReceipt, LedgerError> {
        self.post(JournalEntry::period_reopen(
            transaction_id,
            reference,
            recorded_at,
            new_closed_through,
        )?)
    }

    /// Validates an entry and calculates every resulting balance without mutation.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for invalid entry content, transaction collision,
    /// sequence exhaustion, or balance overflow.
    pub fn prepare(&self, entry: JournalEntry) -> Result<PostingPreparation, LedgerError> {
        if let Some(existing) = self.entries.get(&entry.transaction_id) {
            if existing.entry == entry {
                return Ok(PostingPreparation::Replay(PostReceipt {
                    sequence: existing.sequence,
                    replayed: true,
                }));
            }
            return Err(LedgerError::TransactionIdCollision(entry.transaction_id));
        }
        self.validate_record_cardinality(1)?;
        self.validate_entry_cardinality(&entry)?;
        let period_update = self.validate_entry_lifecycle(&entry)?;

        let mut next_balances = Vec::new();
        next_balances
            .try_reserve_exact(entry.postings.len())
            .map_err(|_| {
                LedgerError::PreparationAllocationFailed(LedgerPreparationResource::BalanceUpdates)
            })?;
        for posting in entry.postings.iter() {
            let key = (posting.account_id, posting.asset_id);
            let current = self.balances.get(&key).copied().unwrap_or(0);
            let next = current
                .checked_add(posting.amount)
                .ok_or(LedgerError::ArithmeticOverflow)?;
            next_balances.push((key, next));
        }
        let reversal_count = usize::from(entry.reversed_transaction().is_some());
        let retained_postings_after =
            self.preflight_event_capacity(1, reversal_count, entry.postings.len(), &next_balances)?;
        let sequence = u64::try_from(self.journal.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(LedgerError::ArithmeticOverflow)?;

        Ok(PostingPreparation::Ready(PreparedPosting {
            entry,
            next_balances,
            period_update,
            retained_postings_after,
            expected_record_count: self.journal.len(),
            sequence,
        }))
    }

    /// Commits a prepared entry without repeating arithmetic validation.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::StalePreparation`] when another entry was
    /// committed after preparation.
    pub fn commit(&mut self, prepared: PreparedPosting) -> Result<PostReceipt, LedgerError> {
        if self.journal.len() != prepared.expected_record_count
            || self.entries.contains_key(&prepared.entry.transaction_id)
        {
            return Err(LedgerError::StalePreparation);
        }
        apply_balance_updates(&mut self.balances, prepared.next_balances);
        match prepared.period_update {
            PeriodUpdate::Unchanged => {}
            PeriodUpdate::Set(closed_through) => self.closed_through = Some(closed_through),
            PeriodUpdate::Clear => self.closed_through = None,
        }
        let transaction_id = prepared.entry.transaction_id;
        let recorded_at = prepared.entry.recorded_at;
        if let Some(reversed_transaction_id) = prepared.entry.reversed_transaction() {
            self.reversals
                .insert(reversed_transaction_id, transaction_id);
        }
        self.entries.insert(
            transaction_id,
            PostedEntry {
                entry: prepared.entry,
                sequence: prepared.sequence,
            },
        );
        self.journal.push(LedgerRecordKey::Entry(transaction_id));
        self.retained_postings = prepared.retained_postings_after;
        self.last_recorded_at = Some(recorded_at);
        Ok(PostReceipt {
            sequence: prepared.sequence,
            replayed: false,
        })
    }

    /// Converts a trade into a delivery-versus-payment entry and posts it.
    ///
    /// `transaction_id` must be globally unique across all execution shards;
    /// trade identifiers are only guaranteed unique within one order book.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for invalid settlement conventions, self
    /// settlement, arithmetic overflow, or any error from [`Ledger::post`].
    pub fn settle_trade(
        &mut self,
        transaction_id: TransactionId,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        trade: &Trade,
        convention: SettlementConvention,
    ) -> Result<PostReceipt, LedgerError> {
        let entry = JournalEntry::from_trade(
            transaction_id,
            effective_date,
            recorded_at,
            trade,
            convention,
        )?;
        self.post(entry)
    }

    /// Atomically settles a trade under its exact immutable instrument version.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for definition mismatch, invalid settlement,
    /// arithmetic overflow, or posting failure.
    pub fn settle_instrument_trade(
        &mut self,
        transaction_id: TransactionId,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        trade: &Trade,
        definition: InstrumentDefinition,
    ) -> Result<PostReceipt, LedgerError> {
        let entry = JournalEntry::from_instrument(
            transaction_id,
            effective_date,
            recorded_at,
            trade,
            definition,
        )?;
        self.post(entry)
    }

    /// Returns one account's signed balance in an asset.
    #[must_use]
    pub fn balance(&self, account_id: AccountId, asset_id: AssetId) -> i128 {
        self.balances
            .get(&(account_id, asset_id))
            .copied()
            .unwrap_or(0)
    }

    /// Returns the number of committed transaction entries.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Returns the number of indivisible sequenced ledger events.
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.journal.len()
    }

    /// Returns the inclusive closed accounting-date boundary.
    #[must_use]
    pub const fn closed_through(&self) -> Option<AccountingDate> {
        self.closed_through
    }

    /// Returns the last committed UTC booking timestamp.
    #[must_use]
    pub const fn last_recorded_at(&self) -> Option<TimestampNs> {
        self.last_recorded_at
    }

    /// Returns transaction identifiers in journal sequence order.
    pub fn transaction_ids(&self) -> impl Iterator<Item = TransactionId> + '_ {
        self.journal.iter().flat_map(|record| {
            (0..record.transaction_count()).filter_map(move |index| record.transaction_id_at(index))
        })
    }

    /// Returns one borrowed canonical event at a one-based ledger sequence.
    ///
    /// A valid result borrows the authoritative journal and transaction index,
    /// clones no entry, and allocates no output. Sequence zero and positions
    /// beyond the retained journal return `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerHistoryError`] if the journal and transaction index
    /// contradict one another.
    pub fn try_record_view(
        &self,
        sequence: u64,
    ) -> Result<Option<LedgerRecordView<'_>>, LedgerHistoryError> {
        let Some(index) = sequence
            .checked_sub(1)
            .and_then(|value| usize::try_from(value).ok())
        else {
            return Ok(None);
        };
        let Some(record) = self.journal.get(index) else {
            return Ok(None);
        };
        self.resolve_retained_record(index, record)
            .map(|record| Some(record.record))
    }

    /// Iterates retained ledger events in sequence order without allocation.
    ///
    /// The iterator is double-ended and exact-sized by event count. Resolving
    /// all `R` records containing `T` transactions performs expected `O(T)`
    /// index work with `O(1)` iterator state.
    #[must_use]
    pub fn retained_history(
        &self,
    ) -> impl DoubleEndedIterator<Item = Result<RetainedLedgerRecord<'_>, LedgerHistoryError>>
    + ExactSizeIterator
    + '_ {
        self.journal
            .iter()
            .enumerate()
            .map(move |(index, record)| self.resolve_retained_record(index, record))
    }

    /// Iterates retained postings for one account and asset in journal order.
    ///
    /// Each line borrows its canonical entry and posting, retains the enclosing
    /// entry/correction/batch view and stable sequence, and identifies the
    /// transaction's zero-based position inside that event. The iterator is
    /// double-ended and allocates no output or auxiliary storage.
    ///
    /// Resolving `R` records containing `T` transactions and posting counts
    /// `L_i` performs expected `O(T + sum(log(L_i + 1)))` work with `O(1)`
    /// iterator state. This is local filtering only; authorization and remote
    /// pagination are external lifecycle responsibilities.
    #[must_use]
    pub fn account_statement(
        &self,
        account_id: AccountId,
        asset_id: AssetId,
    ) -> impl DoubleEndedIterator<Item = Result<LedgerStatementLine<'_>, LedgerHistoryError>> + '_
    {
        self.retained_history()
            .flat_map(move |retained| LedgerStatementRecord::new(retained, account_id, asset_id))
    }

    /// Reconstructs one signed balance after an exact committed record boundary.
    ///
    /// Generation zero denotes the empty ledger. Corrections and batches are
    /// applied as indivisible records, so no intermediate member balance is
    /// observable. The query allocates no output or auxiliary storage.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerAsOfError`] for a future generation, retained-history
    /// contradiction, unrepresentable atomic result, or disagreement between
    /// complete reconstruction and the current balance index.
    pub fn try_balance_at(
        &self,
        generation: u64,
        account_id: AccountId,
        asset_id: AssetId,
    ) -> Result<i128, LedgerAsOfError> {
        let current_generation = u64::try_from(self.journal.len()).map_err(|_| {
            LedgerHistoryError::SequenceOverflow {
                index: self.journal.len().saturating_sub(1),
            }
        })?;
        if generation > current_generation {
            return Err(LedgerAsOfError::GenerationOutOfRange {
                requested: generation,
                current: current_generation,
            });
        }
        let record_limit =
            usize::try_from(generation).map_err(|_| LedgerHistoryError::SequenceOverflow {
                index: self.journal.len(),
            })?;
        let mut reconstructed = 0_i128;
        for retained in self.retained_history().take(record_limit) {
            let retained = retained?;
            reconstructed = Self::apply_record_balance(
                retained.record,
                retained.sequence,
                account_id,
                asset_id,
                reconstructed,
            )?;
        }
        if generation == current_generation {
            let indexed = self.balance(account_id, asset_id);
            if reconstructed != indexed {
                return Err(LedgerAsOfError::CurrentBalanceMismatch {
                    account_id,
                    asset_id,
                    reconstructed,
                    indexed,
                });
            }
        }
        Ok(reconstructed)
    }

    fn apply_record_balance(
        record: LedgerRecordView<'_>,
        sequence: u64,
        account_id: AccountId,
        asset_id: AssetId,
        mut balance: i128,
    ) -> Result<i128, LedgerAsOfError> {
        let mut negative_index = 0_usize;
        let mut positive_index = 0_usize;
        loop {
            let amount = if balance >= 0 {
                Self::next_record_amount(record, &mut negative_index, account_id, asset_id, false)
                    .or_else(|| {
                        Self::next_record_amount(
                            record,
                            &mut positive_index,
                            account_id,
                            asset_id,
                            true,
                        )
                    })
            } else {
                Self::next_record_amount(record, &mut positive_index, account_id, asset_id, true)
                    .or_else(|| {
                        Self::next_record_amount(
                            record,
                            &mut negative_index,
                            account_id,
                            asset_id,
                            false,
                        )
                    })
            };
            let Some(amount) = amount else {
                return Ok(balance);
            };
            balance = balance
                .checked_add(amount)
                .ok_or(LedgerAsOfError::BalanceOverflow {
                    sequence,
                    account_id,
                    asset_id,
                })?;
        }
    }

    fn next_record_amount(
        record: LedgerRecordView<'_>,
        transaction_index: &mut usize,
        account_id: AccountId,
        asset_id: AssetId,
        positive: bool,
    ) -> Option<i128> {
        while *transaction_index < record.transaction_count() {
            let index = *transaction_index;
            *transaction_index += 1;
            let entry = record.transaction(index)?;
            if let Some(posting) = entry.posting(account_id, asset_id)
                && posting.amount.is_positive() == positive
            {
                return Some(posting.amount);
            }
        }
        None
    }

    /// Returns a cloned canonical event at a one-based ledger sequence.
    ///
    /// The clone shares immutable batch/entry/posting storage and allocates no
    /// nested vectors.
    #[must_use]
    pub fn record(&self, sequence: u64) -> Option<LedgerRecord> {
        self.try_record_view(sequence)
            .ok()
            .flatten()
            .map(LedgerRecord::from)
    }

    fn resolve_retained_record<'a>(
        &'a self,
        index: usize,
        record: &'a LedgerRecordKey,
    ) -> Result<RetainedLedgerRecord<'a>, LedgerHistoryError> {
        let sequence = u64::try_from(index)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(LedgerHistoryError::SequenceOverflow { index })?;
        let record = match record {
            LedgerRecordKey::Entry(transaction_id) => LedgerRecordView::Entry(
                self.resolve_history_transaction(sequence, *transaction_id, None)?,
            ),
            LedgerRecordKey::Correction {
                reversal_transaction_id,
                replacement_transaction_id,
            } => LedgerRecordView::Correction {
                reversal: self.resolve_history_transaction(
                    sequence,
                    *reversal_transaction_id,
                    None,
                )?,
                replacement: self.resolve_history_transaction(
                    sequence,
                    *replacement_transaction_id,
                    None,
                )?,
            },
            LedgerRecordKey::Batch(batch) => {
                for entry in batch.entries() {
                    self.resolve_history_transaction(
                        sequence,
                        entry.transaction_id(),
                        Some(entry),
                    )?;
                }
                LedgerRecordView::Batch(batch)
            }
        };
        Ok(RetainedLedgerRecord { sequence, record })
    }

    fn resolve_history_transaction(
        &self,
        sequence: u64,
        transaction_id: TransactionId,
        expected: Option<&JournalEntry>,
    ) -> Result<&JournalEntry, LedgerHistoryError> {
        let posted =
            self.entries
                .get(&transaction_id)
                .ok_or(LedgerHistoryError::MissingTransaction {
                    sequence,
                    transaction_id,
                })?;
        if posted.sequence != sequence
            || posted.entry.transaction_id() != transaction_id
            || expected.is_some_and(|entry| posted.entry != *entry)
        {
            return Err(LedgerHistoryError::TransactionMismatch {
                sequence,
                transaction_id,
            });
        }
        Ok(&posted.entry)
    }

    /// Returns a committed entry by transaction identifier.
    #[must_use]
    pub fn transaction(&self, transaction_id: TransactionId) -> Option<&JournalEntry> {
        self.entries
            .get(&transaction_id)
            .map(|posted| &posted.entry)
    }

    /// Returns the reversal transaction committed for an original transaction.
    #[must_use]
    pub fn reversal_for(&self, transaction_id: TransactionId) -> Option<TransactionId> {
        self.reversals.get(&transaction_id).copied()
    }

    /// Returns the original transaction targeted by a reversal transaction.
    #[must_use]
    pub fn reversed_transaction(&self, transaction_id: TransactionId) -> Option<TransactionId> {
        self.entries
            .get(&transaction_id)
            .and_then(|posted| posted.entry.reversed_transaction())
    }

    /// Compares a complete external balance statement with this exact generation.
    ///
    /// The output contains only non-zero `external - internal` differences in
    /// canonical `(asset, account)` order. Complexity is `O(A log A + S)` time
    /// and `O(A + D)` memory for `A` internal non-zero balances, `S` statement
    /// balances, and `D` differences.
    ///
    /// # Errors
    ///
    /// Returns [`ReconciliationError::GenerationMismatch`] if state advanced,
    /// [`ReconciliationError::ObservationPrecedesLedger`] if the statement
    /// predates its claimed state, or [`ReconciliationError::DifferenceOverflow`]
    /// if a signed delta is not representable.
    pub fn reconcile(
        &self,
        statement: &ReconciliationStatement,
    ) -> Result<ReconciliationReport, ReconciliationError> {
        let ledger_generation = u64::try_from(self.journal.len())
            .map_err(|_| ReconciliationError::CardinalityOverflow)?;
        if ledger_generation != statement.generation {
            return Err(ReconciliationError::GenerationMismatch {
                ledger_generation,
                statement_generation: statement.generation,
            });
        }
        if let Some(last_recorded_at) = self.last_recorded_at {
            if statement.observed_at < last_recorded_at {
                return Err(ReconciliationError::ObservationPrecedesLedger {
                    last_recorded_at,
                    observed_at: statement.observed_at,
                });
            }
        }
        let internal = canonical_balances(&self.balances);
        let mut internal_index = 0;
        let mut statement_index = 0;
        let mut compared_balances = 0_usize;
        let mut differences = Vec::new();
        while internal_index < internal.len() || statement_index < statement.balances.len() {
            let internal_value = internal.get(internal_index);
            let statement_value = statement.balances.get(statement_index);
            let internal_key = internal_value.map(|value| (value.asset_id, value.account_id));
            let statement_key = statement_value.map(|value| (value.asset_id, value.account_id));
            let key = match (internal_key, statement_key) {
                (Some(left), Some(right)) => left.min(right),
                (Some(left), None) => left,
                (None, Some(right)) => right,
                (None, None) => break,
            };
            let ledger_amount = match internal_value {
                Some(value) if internal_key == Some(key) => {
                    internal_index += 1;
                    value.amount
                }
                _ => 0,
            };
            let statement_amount = match statement_value {
                Some(value) if statement_key == Some(key) => {
                    statement_index += 1;
                    value.amount
                }
                _ => 0,
            };
            compared_balances = compared_balances
                .checked_add(1)
                .ok_or(ReconciliationError::CardinalityOverflow)?;
            let difference = statement_amount.checked_sub(ledger_amount).ok_or(
                ReconciliationError::DifferenceOverflow {
                    account_id: key.1,
                    asset_id: key.0,
                    ledger_amount,
                    statement_amount,
                },
            )?;
            if difference != 0 {
                differences.push(ReconciliationDifference {
                    account_id: key.1,
                    asset_id: key.0,
                    ledger_amount,
                    statement_amount,
                    difference,
                });
            }
        }
        Ok(ReconciliationReport {
            reconciliation_id: statement.reconciliation_id,
            generation: statement.generation,
            observed_at: statement.observed_at,
            source_reference: statement.source_reference,
            compared_balances,
            differences,
        })
    }

    /// Fallibly accumulates positive and negative balances for every asset.
    ///
    /// The result is in ascending asset-ID order. Totals have arbitrary exact
    /// magnitude, with an allocation-free `u128` common case. One flat term
    /// arena is reserved through exactly `A` non-zero balances, sorted in
    /// `O(A log A)`, and grouped into an exactly reserved `D`-asset output.
    /// Auxiliary memory is `O(A + D + W)` for `W` spilled magnitude limbs.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerQueryError`] before partial output escapes when either
    /// complete flat vector cannot be represented or reserved.
    pub fn try_trial_balance(&self) -> Result<Vec<AssetTrialBalance>, LedgerQueryError> {
        let mut terms =
            reserve_ledger_query_vec(self.balances.len(), LedgerQueryResource::TrialBalanceTerms)?;
        terms.extend(
            self.balances
                .iter()
                .filter_map(|(&(_, asset_id), &amount)| {
                    (amount != 0).then_some((asset_id, amount))
                }),
        );
        terms.sort_unstable_by_key(|(asset_id, _)| *asset_id);
        let asset_count = terms
            .windows(2)
            .filter(|pair| pair[0].0 != pair[1].0)
            .count()
            .checked_add(usize::from(!terms.is_empty()))
            .ok_or(LedgerQueryError::ReservationFailed {
                resource: LedgerQueryResource::TrialBalanceOutput,
                maximum: usize::MAX,
            })?;
        let mut output =
            reserve_ledger_query_vec(asset_count, LedgerQueryResource::TrialBalanceOutput)?;
        let mut index = 0_usize;
        while let Some(&(asset_id, _)) = terms.get(index) {
            let mut totals = AssetSideTotals::default();
            while let Some(&(term_asset_id, amount)) = terms.get(index) {
                if term_asset_id != asset_id {
                    break;
                }
                totals.add(amount);
                index += 1;
            }
            output.push(AssetTrialBalance {
                asset_id,
                positive_total: totals.positive,
                negative_total: totals.negative,
            });
        }
        Ok(output)
    }

    /// Infallible convenience wrapper for [`Self::try_trial_balance`].
    ///
    /// # Panics
    ///
    /// Panics when complete query output cannot be represented or allocated.
    #[must_use]
    pub fn trial_balance(&self) -> Vec<AssetTrialBalance> {
        self.try_trial_balance()
            .expect("ledger trial-balance query allocation failed")
    }

    fn validate_resource_invariants(&self) -> Result<(), LedgerInvariantViolation> {
        let limits = self.limits.0;
        if self.balances.maximum() != limits.max_balance_keys
            || self.entries.maximum() != limits.max_transactions
            || self.reversals.maximum() != limits.max_reversals
        {
            return Err(LedgerInvariantViolation::new(
                "ledger hash maxima contradict the selected resource limits",
            ));
        }
        if self.journal.capacity() < limits.max_records || self.journal.len() > limits.max_records {
            return Err(LedgerInvariantViolation::new(
                "ledger journal allocation or cardinality contradicts its limit",
            ));
        }
        for (label, layout) in [
            ("balance", self.balances.validate_layout()),
            ("transaction", self.entries.validate_layout()),
            ("reversal", self.reversals.validate_layout()),
        ] {
            if let Err(detail) = layout {
                return Err(LedgerInvariantViolation::new(format!(
                    "ledger {label} hash layout is invalid: {detail}"
                )));
            }
        }
        if self.balances.values().any(|amount| *amount == 0) {
            return Err(LedgerInvariantViolation::new(
                "ledger balance index retains a zero balance",
            ));
        }
        let calculated_postings = self.entries.values().try_fold(0_usize, |count, posted| {
            if posted.entry.postings.len() > limits.max_postings_per_transaction {
                return None;
            }
            count.checked_add(posted.entry.postings.len())
        });
        if calculated_postings != Some(self.retained_postings)
            || self.retained_postings > limits.max_retained_postings
        {
            return Err(LedgerInvariantViolation::new(
                "ledger retained posting cardinality contradicts transaction history or limits",
            ));
        }
        if self
            .journal
            .iter()
            .any(|record| record.transaction_count() > limits.max_transactions_per_record)
        {
            return Err(LedgerInvariantViolation::new(
                "ledger record transaction cardinality exceeds its limit",
            ));
        }
        let expected_entry_count = self
            .journal
            .iter()
            .try_fold(0_usize, |count, record| {
                count.checked_add(record.transaction_count())
            })
            .ok_or_else(|| LedgerInvariantViolation::new("ledger entry cardinality overflow"))?;
        if self.entries.len() != expected_entry_count {
            return Err(LedgerInvariantViolation::new(
                "ledger entry index length differs from journal transaction cardinality",
            ));
        }
        Ok(())
    }

    fn materialize_audit_records(&self) -> Result<Vec<LedgerRecord>, LedgerCheckpointCaptureError> {
        let mut records = reserve_ledger_checkpoint_vec(
            self.journal.len(),
            LedgerCheckpointCaptureResource::CaptureRecords,
        )?;
        for (index, record_key) in self.journal.iter().enumerate() {
            let record = self
                .resolve_retained_record(index, record_key)
                .map_err(|error| LedgerInvariantViolation::new(error.to_string()))?;
            records.push(LedgerRecord::from(record.record));
        }
        Ok(records)
    }

    fn replay_audit_records(
        &self,
        records: &[LedgerRecord],
    ) -> Result<Self, LedgerCheckpointCaptureError> {
        let mut replayed = Self::try_with_limits(self.limits.spec())
            .map_err(LedgerCheckpointCaptureError::Construction)?;
        for record in records {
            match record {
                LedgerRecord::Entry(entry) => replayed.post(entry.clone()).map(|_| ()),
                LedgerRecord::Correction(correction) => {
                    replayed.correct(correction.clone()).map(|_| ())
                }
                LedgerRecord::Batch(batch) => replayed.post_batch(batch.clone()).map(|_| ()),
            }
            .map_err(|error| {
                LedgerCheckpointCaptureError::Invalid(LedgerInvariantViolation::new(format!(
                    "ledger replay failed: {error}"
                )))
            })?;
        }
        Ok(replayed)
    }

    fn validate_replayed_state(&self, replayed: &Self) -> Result<(), LedgerInvariantViolation> {
        let contradiction = if self.journal != replayed.journal {
            Some("stored ledger-event grouping differs from deterministic replay")
        } else if self.balances != replayed.balances {
            Some("stored balances differ from deterministic record replay")
        } else if self.reversals != replayed.reversals {
            Some("stored reversal index differs from deterministic record replay")
        } else if self.retained_postings != replayed.retained_postings {
            Some("stored retained-posting count differs from deterministic replay")
        } else if self.closed_through != replayed.closed_through {
            Some("stored accounting-period boundary differs from deterministic replay")
        } else if self.last_recorded_at != replayed.last_recorded_at {
            Some("stored booking timestamp differs from deterministic record replay")
        } else {
            None
        };
        contradiction.map_or(Ok(()), |detail| Err(LedgerInvariantViolation::new(detail)))
    }

    fn validate_for_checkpoint(&self) -> Result<Vec<LedgerRecord>, LedgerCheckpointCaptureError> {
        self.validate_resource_invariants()?;
        let records = self.materialize_audit_records()?;
        let replayed = self.replay_audit_records(&records)?;
        self.validate_replayed_state(&replayed)?;
        let trial = self.try_trial_balance().map_err(|error| match error {
            LedgerQueryError::ReservationFailed { resource, maximum } => {
                LedgerCheckpointCaptureError::ResourceReservationFailed {
                    resource: match resource {
                        LedgerQueryResource::TrialBalanceTerms => {
                            LedgerCheckpointCaptureResource::AuditTrialBalanceTerms
                        }
                        LedgerQueryResource::TrialBalanceOutput => {
                            LedgerCheckpointCaptureResource::AuditTrialBalanceOutput
                        }
                    },
                    maximum,
                }
            }
        })?;
        if let Some(unbalanced) = trial.into_iter().find(|value| !value.is_balanced()) {
            return Err(LedgerCheckpointCaptureError::Invalid(
                LedgerInvariantViolation::new(format!(
                    "asset {} trial balance is not zero",
                    unbalanced.asset_id()
                )),
            ));
        }
        Ok(records)
    }

    /// Cross-audits journal order, idempotency entries, deterministic replay,
    /// canonical balances, and per-asset trial balances.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerInvariantViolation`] at the first structural divergence
    /// or when an audit resource cannot be constructed.
    pub fn validate(&self) -> Result<(), LedgerInvariantViolation> {
        self.validate_for_checkpoint()
            .map(|_| ())
            .map_err(|error| LedgerInvariantViolation::new(error.to_string()))
    }

    /// Captures canonical balances and complete transaction-idempotency history.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerCheckpointCaptureError`] if current state does not pass
    /// an independent replay/trial-balance audit or if a complete capture
    /// resource cannot be represented or reserved.
    pub fn checkpoint(&self) -> Result<LedgerCheckpoint, LedgerCheckpointCaptureError> {
        let records = self.validate_for_checkpoint()?;
        let generation = u64::try_from(self.journal.len())
            .map_err(|_| LedgerInvariantViolation::new("ledger generation overflow"))?;
        let mut balances = reserve_ledger_checkpoint_vec(
            self.balances.len(),
            LedgerCheckpointCaptureResource::CaptureBalances,
        )?;
        for (&(account_id, asset_id), &amount) in self.balances.iter() {
            if amount != 0 {
                balances.push(LedgerBalance {
                    account_id,
                    asset_id,
                    amount,
                });
            }
        }
        balances.sort_unstable_by_key(|value| (value.asset_id, value.account_id));
        Ok(LedgerCheckpoint {
            generation,
            balances: Arc::new(balances),
            records: Arc::new(records),
        })
    }

    /// Reconstructs a ledger from a checkpoint whose private type invariant was
    /// established during audited capture or binary decoding.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerCheckpointError`] if deterministic event replay fails.
    pub fn from_checkpoint(checkpoint: &LedgerCheckpoint) -> Result<Self, LedgerCheckpointError> {
        Self::from_checkpoint_with_limits(checkpoint, LedgerLimitsSpec::default())
    }

    /// Reconstructs a checkpoint under an explicit finite resource envelope.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerCheckpointError`] when the layout cannot be reserved or
    /// any checkpoint event exceeds the selected limits during replay.
    pub fn from_checkpoint_with_limits(
        checkpoint: &LedgerCheckpoint,
        limits: LedgerLimitsSpec,
    ) -> Result<Self, LedgerCheckpointError> {
        let ledger = Self::try_with_limits(limits).map_err(LedgerCheckpointError::Construction)?;
        ledger.restore_checkpoint(checkpoint)
    }

    pub(crate) fn restore_checkpoint(
        mut self,
        checkpoint: &LedgerCheckpoint,
    ) -> Result<Self, LedgerCheckpointError> {
        debug_assert!(self.journal.is_empty());
        debug_assert!(self.entries.is_empty());
        for (index, record) in checkpoint.records.iter().cloned().enumerate() {
            match record {
                LedgerRecord::Entry(entry) => self.post(entry).map(|_| ()),
                LedgerRecord::Correction(correction) => self.correct(correction).map(|_| ()),
                LedgerRecord::Batch(batch) => self.post_batch(batch).map(|_| ()),
            }
            .map_err(|error| LedgerCheckpointError::RecordReplay { index, error })?;
        }
        Ok(self)
    }
}

fn reserve_ledger_checkpoint_vec<T>(
    maximum: usize,
    resource: LedgerCheckpointCaptureResource,
) -> Result<Vec<T>, LedgerCheckpointCaptureError> {
    let mut values = Vec::new();
    values.try_reserve_exact(maximum).map_err(|_| {
        LedgerCheckpointCaptureError::ResourceReservationFailed { resource, maximum }
    })?;
    Ok(values)
}

fn canonical_balances(balances: &BoundedHashMap<(AccountId, AssetId), i128>) -> Vec<LedgerBalance> {
    let mut values: Vec<_> = balances
        .iter()
        .filter_map(|(&(account_id, asset_id), &amount)| {
            (amount != 0).then_some(LedgerBalance {
                account_id,
                asset_id,
                amount,
            })
        })
        .collect();
    values.sort_unstable_by_key(|value| (value.asset_id, value.account_id));
    values
}

fn checkpoint_replay_limits(
    records: &[LedgerRecord],
    encoded_balance_count: usize,
) -> Result<LedgerLimitsSpec, LedgerCheckpointError> {
    let mut transactions = 0_usize;
    let mut retained_postings = 0_usize;
    let mut max_postings_per_transaction = 1_usize;
    let mut max_transactions_per_record = 1_usize;
    for record in records {
        let record_transactions = record.transaction_count();
        transactions = transactions
            .checked_add(record_transactions)
            .ok_or(LedgerCheckpointError::CardinalityOverflow)?;
        max_transactions_per_record = max_transactions_per_record.max(record_transactions);
        let mut accumulate = |entry: &JournalEntry| -> Result<(), LedgerCheckpointError> {
            retained_postings = retained_postings
                .checked_add(entry.postings.len())
                .ok_or(LedgerCheckpointError::CardinalityOverflow)?;
            max_postings_per_transaction = max_postings_per_transaction.max(entry.postings.len());
            Ok(())
        };
        match record {
            LedgerRecord::Entry(entry) => accumulate(entry)?,
            LedgerRecord::Correction(correction) => {
                accumulate(&correction.reversal)?;
                accumulate(&correction.replacement)?;
            }
            LedgerRecord::Batch(batch) => {
                for entry in batch.entries.iter() {
                    accumulate(entry)?;
                }
            }
        }
    }
    let transactions = transactions.max(1);
    let retained_postings = retained_postings.max(1);
    Ok(LedgerLimitsSpec {
        max_balance_keys: retained_postings.max(encoded_balance_count).max(1),
        max_transactions: transactions,
        max_reversals: transactions,
        max_records: records.len().max(1),
        max_postings_per_transaction,
        max_transactions_per_record,
        max_retained_postings: retained_postings,
    })
}

fn validate_checkpoint(
    generation: u64,
    balances: &[LedgerBalance],
    records: &[LedgerRecord],
) -> Result<(), LedgerCheckpointError> {
    if usize::try_from(generation).ok() != Some(records.len()) {
        return Err(LedgerCheckpointError::GenerationMismatch {
            generation,
            record_count: records.len(),
        });
    }
    if balances.iter().any(|balance| balance.amount == 0)
        || balances.windows(2).any(|pair| {
            (pair[0].asset_id, pair[0].account_id) >= (pair[1].asset_id, pair[1].account_id)
        })
    {
        return Err(LedgerCheckpointError::NonCanonicalBalances);
    }
    let replay_limits = checkpoint_replay_limits(records, balances.len())?;
    let mut replayed =
        Ledger::try_with_limits(replay_limits).map_err(LedgerCheckpointError::Construction)?;
    for (index, record) in records.iter().enumerate() {
        let replayed_record = match record {
            LedgerRecord::Entry(entry) => replayed.post(entry.clone()).map(|value| value.replayed),
            LedgerRecord::Correction(correction) => replayed
                .correct(correction.clone())
                .map(|value| value.replayed),
            LedgerRecord::Batch(batch) => replayed
                .post_batch(batch.clone())
                .map(|value| value.replayed),
        }
        .map_err(|error| LedgerCheckpointError::RecordReplay { index, error })?;
        if replayed_record {
            return Err(LedgerCheckpointError::DuplicateTransaction {
                index,
                transaction_id: record.primary_transaction_id(),
            });
        }
    }
    if replayed.balances.len() != balances.len()
        || balances.iter().any(|balance| {
            replayed
                .balances
                .get(&(balance.account_id, balance.asset_id))
                != Some(&balance.amount)
        })
    {
        return Err(LedgerCheckpointError::BalanceMismatch);
    }
    Ok(())
}

fn postings_are_exact_inverse(candidate: &[Posting], original: &[Posting]) -> bool {
    candidate.len() == original.len()
        && candidate.iter().zip(original).all(|(candidate, original)| {
            candidate.account_id == original.account_id
                && candidate.asset_id == original.asset_id
                && original.amount.checked_neg() == Some(candidate.amount)
        })
}

fn calculate_batch_balances(
    balances: &BoundedHashMap<BalanceKey, i128>,
    entries: &[JournalEntry],
) -> Result<Vec<BalanceUpdate>, LedgerError> {
    let posting_count = entries
        .iter()
        .try_fold(0_usize, |count, entry| {
            count.checked_add(entry.postings.len())
        })
        .ok_or(LedgerError::ArithmeticOverflow)?;
    let mut terms = Vec::<(BalanceKey, i128)>::new();
    terms.try_reserve_exact(posting_count).map_err(|_| {
        LedgerError::PreparationAllocationFailed(LedgerPreparationResource::BalanceTerms)
    })?;
    for posting in entries.iter().flat_map(|entry| entry.postings.iter()) {
        terms.push(((posting.account_id, posting.asset_id), posting.amount));
    }
    terms.sort_unstable_by(|(left_key, left_amount), (right_key, right_amount)| {
        left_key
            .cmp(right_key)
            .then_with(|| left_amount.is_positive().cmp(&right_amount.is_positive()))
    });

    let mut next_balances = Vec::new();
    next_balances.try_reserve_exact(terms.len()).map_err(|_| {
        LedgerError::PreparationAllocationFailed(LedgerPreparationResource::BalanceUpdates)
    })?;
    let mut group_start = 0_usize;
    while group_start < terms.len() {
        let key = terms[group_start].0;
        let mut group_end = group_start + 1;
        while group_end < terms.len() && terms[group_end].0 == key {
            group_end += 1;
        }
        let positive_start = terms[group_start..group_end]
            .partition_point(|(_, amount)| amount.is_negative())
            + group_start;
        let current = balances.get(&key).copied().unwrap_or(0);
        let mut next = current;
        let mut negative_index = group_start;
        let mut positive_index = positive_start;
        while negative_index < positive_start || positive_index < group_end {
            let amount = if next >= 0 && negative_index < positive_start {
                let amount = terms[negative_index].1;
                negative_index += 1;
                amount
            } else if positive_index < group_end {
                let amount = terms[positive_index].1;
                positive_index += 1;
                amount
            } else {
                let amount = terms[negative_index].1;
                negative_index += 1;
                amount
            };
            next = next
                .checked_add(amount)
                .ok_or(LedgerError::ArithmeticOverflow)?;
        }
        if next != current {
            next_balances.push((key, next));
        }
        group_start = group_end;
    }
    Ok(next_balances)
}

fn calculate_correction_balances(
    balances: &BoundedHashMap<(AccountId, AssetId), i128>,
    left: &[Posting],
    right: &[Posting],
) -> Result<Vec<BalanceUpdate>, LedgerError> {
    let capacity = left
        .len()
        .checked_add(right.len())
        .ok_or(LedgerError::ArithmeticOverflow)?;
    let mut next_balances = Vec::new();
    next_balances.try_reserve_exact(capacity).map_err(|_| {
        LedgerError::PreparationAllocationFailed(LedgerPreparationResource::BalanceUpdates)
    })?;
    let mut left_index = 0;
    let mut right_index = 0;
    while left_index < left.len() || right_index < right.len() {
        let left_posting = left.get(left_index);
        let right_posting = right.get(right_index);
        let left_key = left_posting.map(|value| (value.asset_id, value.account_id));
        let right_key = right_posting.map(|value| (value.asset_id, value.account_id));
        let key = match (left_key, right_key) {
            (Some(left_key), Some(right_key)) => left_key.min(right_key),
            (Some(left_key), None) => left_key,
            (None, Some(right_key)) => right_key,
            (None, None) => break,
        };
        let left_amount = if left_key == Some(key) {
            let amount = left_posting.map_or(0, |posting| posting.amount);
            left_index += 1;
            amount
        } else {
            0
        };
        let right_amount = if right_key == Some(key) {
            let amount = right_posting.map_or(0, |posting| posting.amount);
            right_index += 1;
            amount
        } else {
            0
        };
        let balance_key = (key.1, key.0);
        let current = balances.get(&balance_key).copied().unwrap_or(0);
        let next = checked_add_three(current, left_amount, right_amount)
            .ok_or(LedgerError::ArithmeticOverflow)?;
        if next != current {
            next_balances.push((balance_key, next));
        }
    }
    Ok(next_balances)
}

fn checked_add_three(first: i128, second: i128, third: i128) -> Option<i128> {
    first
        .checked_add(second)
        .and_then(|partial| partial.checked_add(third))
        .or_else(|| {
            first
                .checked_add(third)
                .and_then(|partial| partial.checked_add(second))
        })
        .or_else(|| {
            second
                .checked_add(third)
                .and_then(|partial| partial.checked_add(first))
        })
}

fn validate_batch_lifecycle_entry(
    entry: &JournalEntry,
    closed_through: &mut Option<AccountingDate>,
    base_entries: &BoundedHashMap<TransactionId, PostedEntry>,
    pending_entries: &BoundedHashMap<TransactionId, &JournalEntry>,
    base_reversals: &BoundedHashMap<TransactionId, TransactionId>,
    pending_reversals: &mut BoundedHashMap<TransactionId, TransactionId>,
    new_reversals: &mut Vec<(TransactionId, TransactionId)>,
) -> Result<(), LedgerError> {
    match entry.kind {
        LedgerEntryKind::Standard => validate_financial_entry(entry, *closed_through),
        LedgerEntryKind::Reversal {
            reversed_transaction_id,
        } => {
            validate_financial_entry(entry, *closed_through)?;
            let original = pending_entries
                .get(&reversed_transaction_id)
                .copied()
                .or_else(|| {
                    base_entries
                        .get(&reversed_transaction_id)
                        .map(|posted| &posted.entry)
                })
                .ok_or(LedgerError::ReversalTargetMissing(reversed_transaction_id))?;
            if matches!(
                original.kind,
                LedgerEntryKind::PeriodClose { .. } | LedgerEntryKind::PeriodReopen { .. }
            ) {
                return Err(LedgerError::NonFinancialReversalTarget(
                    reversed_transaction_id,
                ));
            }
            if let Some(&reversal_transaction_id) = pending_reversals
                .get(&reversed_transaction_id)
                .or_else(|| base_reversals.get(&reversed_transaction_id))
            {
                return Err(LedgerError::TransactionAlreadyReversed {
                    original_transaction_id: reversed_transaction_id,
                    reversal_transaction_id,
                });
            }
            if !postings_are_exact_inverse(&entry.postings, &original.postings) {
                return Err(LedgerError::InvalidReversalPostings(
                    reversed_transaction_id,
                ));
            }
            if let Some(reversal_transaction_id) =
                pending_reversals.insert(reversed_transaction_id, entry.transaction_id)
            {
                return Err(LedgerError::TransactionAlreadyReversed {
                    original_transaction_id: reversed_transaction_id,
                    reversal_transaction_id,
                });
            }
            new_reversals.push((reversed_transaction_id, entry.transaction_id));
            Ok(())
        }
        LedgerEntryKind::PeriodClose {
            closed_through: proposed,
        } => {
            validate_control_entry(entry)?;
            if closed_through.is_some_and(|current| proposed <= current) {
                return Err(LedgerError::PeriodCloseNotAdvancing {
                    current_closed_through: *closed_through,
                    proposed_closed_through: proposed,
                });
            }
            *closed_through = Some(proposed);
            Ok(())
        }
        LedgerEntryKind::PeriodReopen { new_closed_through } => {
            validate_control_entry(entry)?;
            let current = closed_through.ok_or(LedgerError::AccountingPeriodAlreadyOpen)?;
            if new_closed_through.is_some_and(|proposed| proposed >= current) {
                return Err(LedgerError::InvalidPeriodReopen {
                    current_closed_through: current,
                    proposed_closed_through: new_closed_through,
                });
            }
            *closed_through = new_closed_through;
            Ok(())
        }
    }
}

fn validate_financial_entry(
    entry: &JournalEntry,
    closed_through: Option<AccountingDate>,
) -> Result<(), LedgerError> {
    let effective_date = entry
        .effective_date
        .ok_or(LedgerError::FinancialEntryMissingEffectiveDate)?;
    validate_postings(&entry.postings)?;
    if let Some(closed_through) = closed_through {
        if effective_date <= closed_through {
            return Err(LedgerError::AccountingPeriodClosed {
                effective_date,
                closed_through,
            });
        }
    }
    Ok(())
}

fn validate_control_entry(entry: &JournalEntry) -> Result<(), LedgerError> {
    if entry.effective_date.is_some() {
        return Err(LedgerError::ControlEntryHasEffectiveDate);
    }
    if !entry.postings.is_empty() {
        return Err(LedgerError::ControlEntryHasPostings);
    }
    Ok(())
}

fn validate_reconciliation_balances(
    balances: &[ReconciliationBalance],
) -> Result<(), ReconciliationError> {
    for balance in balances {
        if balance.amount == 0 {
            return Err(ReconciliationError::ZeroBalance {
                account_id: balance.account_id,
                asset_id: balance.asset_id,
            });
        }
    }
    if let Some(pair) = balances.windows(2).find(|pair| {
        (pair[0].asset_id, pair[0].account_id) == (pair[1].asset_id, pair[1].account_id)
    }) {
        return Err(ReconciliationError::DuplicateAccountAsset {
            account_id: pair[0].account_id,
            asset_id: pair[0].asset_id,
        });
    }
    let mut index = 0_usize;
    while let Some(balance) = balances.get(index) {
        let asset_id = balance.asset_id;
        let mut totals = AssetSideTotals::default();
        while let Some(balance) = balances.get(index) {
            if balance.asset_id != asset_id {
                break;
            }
            totals.add(balance.amount);
            index += 1;
        }
        if !totals.is_balanced() {
            return Err(ReconciliationError::Unbalanced {
                asset_id,
                positive_total: Box::new(totals.positive),
                negative_total: Box::new(totals.negative),
            });
        }
    }
    Ok(())
}

fn validate_postings(postings: &[Posting]) -> Result<(), LedgerError> {
    if postings.len() < 2 {
        return Err(LedgerError::TooFewPostings);
    }
    if postings.iter().any(|posting| posting.amount == 0) {
        return Err(LedgerError::ZeroPosting);
    }
    if postings.windows(2).any(|pair| {
        (pair[0].account_id, pair[0].asset_id) == (pair[1].account_id, pair[1].asset_id)
    }) {
        return Err(LedgerError::DuplicateAccountAsset);
    }
    let mut asset_id = postings[0].asset_id;
    let mut totals = AssetSideTotals::default();
    for posting in postings {
        if posting.asset_id != asset_id {
            if !totals.is_balanced() {
                return Err(LedgerError::Unbalanced {
                    asset_id,
                    positive_total: Box::new(totals.positive),
                    negative_total: Box::new(totals.negative),
                });
            }
            asset_id = posting.asset_id;
            totals = AssetSideTotals::default();
        }
        totals.add(posting.amount);
    }
    if !totals.is_balanced() {
        return Err(LedgerError::Unbalanced {
            asset_id,
            positive_total: Box::new(totals.positive),
            negative_total: Box::new(totals.negative),
        });
    }
    Ok(())
}

#[cfg(test)]
mod resource_limit_tests {
    use super::*;

    fn limits() -> LedgerLimitsSpec {
        LedgerLimitsSpec {
            max_balance_keys: 2,
            max_transactions: 2,
            max_reversals: 1,
            max_records: 2,
            max_postings_per_transaction: 2,
            max_transactions_per_record: 2,
            max_retained_postings: 4,
        }
    }

    fn entry() -> JournalEntry {
        JournalEntry::new(
            TransactionId::new(1).unwrap(),
            1,
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(1),
            vec![
                Posting {
                    account_id: AccountId::new(1).unwrap(),
                    asset_id: AssetId::new(1).unwrap(),
                    amount: 1,
                },
                Posting {
                    account_id: AccountId::new(2).unwrap(),
                    asset_id: AssetId::new(1).unwrap(),
                    amount: -1,
                },
            ],
        )
        .unwrap()
    }

    #[test]
    fn unrepresentable_batch_hash_scratch_is_typed_by_exact_resource() {
        assert_eq!(
            reserve_ledger_preparation_set::<TransactionId>(
                usize::MAX,
                LedgerPreparationResource::BatchIdentitySet,
            )
            .unwrap_err(),
            LedgerError::PreparationAllocationFailed(LedgerPreparationResource::BatchIdentitySet,)
        );
        for resource in [
            LedgerPreparationResource::PendingTransactions,
            LedgerPreparationResource::PendingReversals,
        ] {
            assert_eq!(
                reserve_ledger_preparation_map::<TransactionId, TransactionId>(
                    usize::MAX,
                    resource,
                )
                .unwrap_err(),
                LedgerError::PreparationAllocationFailed(resource)
            );
        }
    }

    #[test]
    fn unrepresentable_fee_scratch_is_typed_by_exact_resource() {
        for resource in [
            LedgerPreparationResource::CallAuctionFeeAssessments,
            LedgerPreparationResource::CallAuctionCalculatedFees,
        ] {
            assert_eq!(
                reserve_ledger_preparation_vec::<u8>(usize::MAX, resource).unwrap_err(),
                LedgerError::PreparationAllocationFailed(resource)
            );
        }
    }

    #[test]
    fn unrepresentable_checkpoint_capture_is_typed_by_exact_resource() {
        for resource in [
            LedgerCheckpointCaptureResource::CaptureRecords,
            LedgerCheckpointCaptureResource::CaptureBalances,
            LedgerCheckpointCaptureResource::AuditTrialBalanceTerms,
            LedgerCheckpointCaptureResource::AuditTrialBalanceOutput,
        ] {
            let error =
                reserve_ledger_checkpoint_vec::<LedgerBalance>(usize::MAX, resource).unwrap_err();
            assert_eq!(
                error,
                LedgerCheckpointCaptureError::ResourceReservationFailed {
                    resource,
                    maximum: usize::MAX,
                }
            );
            assert_eq!(error.resource(), Some(resource));
            assert!(error.is_resource_exhaustion());
            assert!(error.is_operational_failure());
        }
    }

    #[test]
    fn unrepresentable_trial_balance_buffers_are_typed_by_exact_resource() {
        for resource in [
            LedgerQueryResource::TrialBalanceTerms,
            LedgerQueryResource::TrialBalanceOutput,
        ] {
            assert_eq!(
                reserve_ledger_query_vec::<AssetTrialBalance>(usize::MAX, resource).unwrap_err(),
                LedgerQueryError::ReservationFailed {
                    resource,
                    maximum: usize::MAX,
                }
            );
        }
    }

    #[test]
    fn borrowed_history_reports_missing_and_mismatched_index_entries() {
        let transaction_id = TransactionId::new(1).unwrap();
        let mut missing = Ledger::try_with_limits(limits()).unwrap();
        missing.post(entry()).unwrap();
        missing.entries.remove(&transaction_id);
        let error = missing.try_record_view(1).unwrap_err();
        assert_eq!(
            error,
            LedgerHistoryError::MissingTransaction {
                sequence: 1,
                transaction_id,
            }
        );
        assert_eq!(
            error.to_string(),
            "ledger record 1 transaction 1 is absent from the index"
        );
        assert!(std::error::Error::source(&error).is_none());
        assert_eq!(missing.retained_history().next(), Some(Err(error)));
        assert_eq!(
            missing
                .account_statement(AccountId::new(1).unwrap(), AssetId::new(1).unwrap())
                .next(),
            Some(Err(error))
        );
        let as_of_error = missing
            .try_balance_at(1, AccountId::new(1).unwrap(), AssetId::new(1).unwrap())
            .unwrap_err();
        assert_eq!(as_of_error, LedgerAsOfError::History(error));
        assert_eq!(
            std::error::Error::source(&as_of_error).unwrap().to_string(),
            error.to_string()
        );

        let mut mismatched = Ledger::try_with_limits(limits()).unwrap();
        mismatched.post(entry()).unwrap();
        mismatched
            .entries
            .get_mut(&transaction_id)
            .unwrap()
            .sequence = 2;
        assert_eq!(
            mismatched.try_record_view(1),
            Err(LedgerHistoryError::TransactionMismatch {
                sequence: 1,
                transaction_id,
            })
        );
        assert_eq!(
            mismatched
                .account_statement(AccountId::new(1).unwrap(), AssetId::new(1).unwrap())
                .next(),
            Some(Err(LedgerHistoryError::TransactionMismatch {
                sequence: 1,
                transaction_id,
            }))
        );
    }

    #[test]
    fn point_in_time_balance_reports_overflow_and_current_index_divergence() {
        let account_id = AccountId::new(1).unwrap();
        let asset_id = AssetId::new(1).unwrap();
        let value = entry();
        assert_eq!(
            Ledger::apply_record_balance(
                LedgerRecordView::Entry(&value),
                1,
                account_id,
                asset_id,
                i128::MAX,
            ),
            Err(LedgerAsOfError::BalanceOverflow {
                sequence: 1,
                account_id,
                asset_id,
            })
        );

        let mut ledger = Ledger::try_with_limits(limits()).unwrap();
        ledger.post(value).unwrap();
        *ledger.balances.get_mut(&(account_id, asset_id)).unwrap() = 2;
        let error = ledger.try_balance_at(1, account_id, asset_id).unwrap_err();
        assert_eq!(
            error,
            LedgerAsOfError::CurrentBalanceMismatch {
                account_id,
                asset_id,
                reconstructed: 1,
                indexed: 2,
            }
        );
        assert_eq!(
            error.to_string(),
            "ledger reconstructed balance 1 for account 1 asset 1 differs from indexed balance 2"
        );
        assert!(std::error::Error::source(&error).is_none());
    }

    #[test]
    fn invariant_validation_rejects_lost_layout_and_posting_cardinality() {
        let mut hash_corrupt = Ledger::try_with_limits(limits()).unwrap();
        hash_corrupt.balances.shrink_to_fit();
        assert!(
            hash_corrupt
                .validate()
                .unwrap_err()
                .detail()
                .contains("balance hash layout")
        );

        let mut journal_corrupt = Ledger::try_with_limits(limits()).unwrap();
        journal_corrupt.journal.shrink_to_fit();
        assert!(
            journal_corrupt
                .validate()
                .unwrap_err()
                .detail()
                .contains("journal allocation")
        );

        let mut count_corrupt = Ledger::try_with_limits(limits()).unwrap();
        count_corrupt.post(entry()).unwrap();
        count_corrupt.retained_postings -= 1;
        assert!(
            count_corrupt
                .validate()
                .unwrap_err()
                .detail()
                .contains("retained posting cardinality")
        );
    }
}
