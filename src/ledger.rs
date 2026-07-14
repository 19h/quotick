//! Atomic multi-asset double-entry accounting and trade settlement.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use crate::domain::{
    AccountId, AccountingDate, AssetId, ReconciliationId, TimestampNs, TransactionId,
};
use crate::instrument::InstrumentDefinition;
use crate::matching::Trade;

pub use crate::ledger_magnitude::LedgerMagnitude;

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
    postings: Vec<Posting>,
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
/// final aggregate image of the complete batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerBatch {
    entries: Vec<JournalEntry>,
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
            Self::TransactionIdCollision(id) => {
                write!(
                    formatter,
                    "transaction identifier {id} was reused with different content"
                )
            }
            Self::SelfSettlement => formatter.write_str("buyer and seller accounts must differ"),
            Self::IdenticalSettlementAssets => {
                formatter.write_str("base and quote settlement assets must differ")
            }
            Self::ZeroSettlementMultiplier => {
                formatter.write_str("settlement conversion multipliers must be non-zero")
            }
            Self::SettlementInstrumentMismatch => {
                formatter.write_str("trade and settlement definition instruments differ")
            }
            Self::SettlementVersionMismatch => {
                formatter.write_str("trade and settlement definition versions differ")
            }
            Self::StalePreparation => formatter.write_str("prepared journal entry is stale"),
            Self::ReversalTargetMissing(transaction_id) => write!(
                formatter,
                "reversal target transaction {transaction_id} is not committed"
            ),
            Self::TransactionAlreadyReversed {
                original_transaction_id,
                reversal_transaction_id,
            } => write!(
                formatter,
                "transaction {original_transaction_id} was already reversed by {reversal_transaction_id}"
            ),
            Self::InvalidReversalPostings(transaction_id) => write!(
                formatter,
                "reversal postings are not the exact inverse of transaction {transaction_id}"
            ),
            Self::NonReversibleAmount {
                original_transaction_id,
                account_id,
                asset_id,
            } => write!(
                formatter,
                "transaction {original_transaction_id} has a non-reversible i128::MIN leg for account {account_id}, asset {asset_id}"
            ),
            Self::CorrectionReplacementNotStandard(transaction_id) => write!(
                formatter,
                "correction replacement transaction {transaction_id} is not a standard financial entry"
            ),
            Self::CorrectionFirstEntryNotReversal(transaction_id) => write!(
                formatter,
                "correction transaction {transaction_id} is not a reversal entry"
            ),
            Self::CorrectionTransactionIdsNotDistinct {
                reversal_transaction_id,
                replacement_transaction_id,
            } => write!(
                formatter,
                "correction reversal {reversal_transaction_id} and replacement {replacement_transaction_id} must be distinct"
            ),
            Self::CorrectionAlreadyPartiallyCommitted(transaction_id) => write!(
                formatter,
                "correction transaction {transaction_id} is already committed outside the exact correction event"
            ),
            Self::BatchTooFewEntries => {
                formatter.write_str("ledger batch requires at least two entries")
            }
            Self::BatchDuplicateTransaction(transaction_id) => write!(
                formatter,
                "ledger batch repeats transaction identifier {transaction_id}"
            ),
            Self::BatchAlreadyPartiallyCommitted(transaction_id) => write!(
                formatter,
                "batch transaction {transaction_id} is already committed outside the exact batch event"
            ),
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
            postings,
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
        let mut postings = Vec::with_capacity(original.postings.len());
        for posting in &original.postings {
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
        &self.postings
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
        if trade.buyer_account_id == trade.seller_account_id {
            return Err(LedgerError::SelfSettlement);
        }
        if convention.base_asset_id == convention.quote_asset_id {
            return Err(LedgerError::IdenticalSettlementAssets);
        }
        if convention.base_units_per_lot == 0 || convention.quote_units_per_price_unit == 0 {
            return Err(LedgerError::ZeroSettlementMultiplier);
        }

        let base_amount = i128::from(trade.quantity.lots())
            .checked_mul(i128::from(convention.base_units_per_lot))
            .ok_or(LedgerError::ArithmeticOverflow)?;
        let notional = i128::from(trade.price.raw())
            .checked_mul(i128::from(trade.quantity.lots()))
            .and_then(|value| value.checked_mul(i128::from(convention.quote_units_per_price_unit)))
            .ok_or(LedgerError::ArithmeticOverflow)?;
        let opposite_notional = notional
            .checked_neg()
            .ok_or(LedgerError::ArithmeticOverflow)?;
        let mut postings = vec![
            Posting {
                account_id: trade.buyer_account_id,
                asset_id: convention.base_asset_id,
                amount: base_amount,
            },
            Posting {
                account_id: trade.seller_account_id,
                asset_id: convention.base_asset_id,
                amount: -base_amount,
            },
        ];
        if notional != 0 {
            postings.extend([
                Posting {
                    account_id: trade.buyer_account_id,
                    asset_id: convention.quote_asset_id,
                    amount: opposite_notional,
                },
                Posting {
                    account_id: trade.seller_account_id,
                    asset_id: convention.quote_asset_id,
                    amount: notional,
                },
            ]);
        }
        Self::new(
            transaction_id,
            trade.trade_id.get(),
            effective_date,
            recorded_at,
            postings,
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

impl LedgerBatch {
    /// Constructs an ordered atomic group of two or more distinct entries.
    ///
    /// The declared order is retained exactly. Each member is already
    /// canonical by construction; this boundary additionally proves unique
    /// transaction identifiers and nondecreasing booking timestamps.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] for insufficient cardinality, a repeated
    /// transaction identifier, or a timestamp regression within the batch.
    pub fn new(entries: Vec<JournalEntry>) -> Result<Self, LedgerError> {
        if entries.len() < 2 {
            return Err(LedgerError::BatchTooFewEntries);
        }
        let mut transaction_ids = BTreeSet::new();
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
        Ok(Self { entries })
    }

    /// Returns entries in their authoritative application order.
    #[must_use]
    pub fn entries(&self) -> &[JournalEntry] {
        &self.entries
    }

    /// Returns the first transaction identifier used as the event correlation key.
    #[must_use]
    pub fn primary_transaction_id(&self) -> TransactionId {
        self.entries[0].transaction_id
    }
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
    expected_record_count: usize,
    sequence: u64,
}

/// Validated final balance image for one atomic correction event.
#[derive(Debug)]
pub struct PreparedCorrection {
    correction: LedgerCorrection,
    next_balances: Vec<BalanceUpdate>,
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
    Batch(Vec<TransactionId>),
}

impl LedgerRecordKey {
    fn transaction_count(&self) -> usize {
        match self {
            Self::Entry(_) => 1,
            Self::Correction { .. } => 2,
            Self::Batch(transaction_ids) => transaction_ids.len(),
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
            (Self::Batch(transaction_ids), index) => transaction_ids.get(index).copied(),
            (Self::Entry(_) | Self::Correction { .. }, _) => None,
        }
    }
}

type BalanceKey = (AccountId, AssetId);
type BalanceUpdate = (BalanceKey, i128);

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerCheckpoint {
    generation: u64,
    balances: Vec<LedgerBalance>,
    records: Vec<LedgerRecord>,
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
            balances,
            records,
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
        &self.balances
    }

    /// Returns canonical ledger events in sequence order.
    #[must_use]
    pub fn records(&self) -> &[LedgerRecord] {
        &self.records
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
        }
    }
}

impl std::error::Error for LedgerCheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::RecordReplay { error, .. } => Some(error),
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
#[derive(Debug, Default)]
pub struct Ledger {
    balances: HashMap<(AccountId, AssetId), i128>,
    entries: HashMap<TransactionId, PostedEntry>,
    reversals: HashMap<TransactionId, TransactionId>,
    journal: Vec<LedgerRecordKey>,
    closed_through: Option<AccountingDate>,
    last_recorded_at: Option<TimestampNs>,
}

impl Ledger {
    /// Creates an empty ledger.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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

    /// Validates a complete batch against the current ledger generation.
    ///
    /// Reversal targets and period controls introduced earlier in the same
    /// batch are visible to later members. Later members are never visible to
    /// earlier ones. No ledger state is mutated during preparation.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] when exact replay cannot be established or any
    /// ordered state transition or aggregate final balance is invalid.
    pub fn prepare_batch(&self, batch: LedgerBatch) -> Result<BatchPreparation, LedgerError> {
        if let Some(receipt) = self.existing_batch_receipt(&batch)? {
            return Ok(BatchPreparation::Replay(receipt));
        }
        let mut previous_recorded_at = self.last_recorded_at;
        let mut closed_through = self.closed_through;
        let mut pending_entries: HashMap<TransactionId, &JournalEntry> =
            HashMap::with_capacity(batch.entries.len());
        let mut pending_reversals = HashMap::new();
        let mut new_reversals = Vec::new();

        for entry in &batch.entries {
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
            pending_entries.insert(entry.transaction_id, entry);
            previous_recorded_at = Some(entry.recorded_at);
        }

        let next_balances = calculate_batch_balances(&self.balances, &batch.entries)?;
        let sequence = u64::try_from(self.journal.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(LedgerError::ArithmeticOverflow)?;
        Ok(BatchPreparation::Ready(PreparedBatch {
            batch,
            next_balances,
            final_closed_through: closed_through,
            new_reversals,
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
        for entry in &batch.entries {
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
                LedgerRecordKey::Batch(transaction_ids) => {
                    transaction_ids.len() == batch.entries.len()
                        && transaction_ids
                            .iter()
                            .zip(&batch.entries)
                            .all(|(transaction_id, entry)| *transaction_id == entry.transaction_id)
                }
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
        for (key, value) in prepared.next_balances {
            self.balances.insert(key, value);
        }
        self.closed_through = prepared.final_closed_through;
        for (original_transaction_id, reversal_transaction_id) in prepared.new_reversals {
            self.reversals
                .insert(original_transaction_id, reversal_transaction_id);
        }
        let transaction_count = prepared.batch.entries.len();
        let last_recorded_at = prepared.batch.entries[transaction_count - 1].recorded_at;
        let mut transaction_ids = Vec::with_capacity(transaction_count);
        for entry in prepared.batch.entries {
            let transaction_id = entry.transaction_id;
            transaction_ids.push(transaction_id);
            self.entries.insert(
                transaction_id,
                PostedEntry {
                    entry,
                    sequence: prepared.sequence,
                },
            );
        }
        self.journal.push(LedgerRecordKey::Batch(transaction_ids));
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
        let sequence = u64::try_from(self.journal.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(LedgerError::ArithmeticOverflow)?;
        Ok(CorrectionPreparation::Ready(PreparedCorrection {
            correction,
            next_balances,
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
        for (key, value) in prepared.next_balances {
            self.balances.insert(key, value);
        }
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
        if let Some(previous) = self.last_recorded_at {
            if entry.recorded_at < previous {
                return Err(LedgerError::RecordedTimestampRegression {
                    previous,
                    proposed: entry.recorded_at,
                });
            }
        }
        let period_update = match entry.kind {
            LedgerEntryKind::Standard => {
                validate_financial_entry(&entry, self.closed_through)?;
                PeriodUpdate::Unchanged
            }
            LedgerEntryKind::Reversal {
                reversed_transaction_id,
            } => {
                validate_financial_entry(&entry, self.closed_through)?;
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
                PeriodUpdate::Unchanged
            }
            LedgerEntryKind::PeriodClose { closed_through } => {
                validate_control_entry(&entry)?;
                if self
                    .closed_through
                    .is_some_and(|current| closed_through <= current)
                {
                    return Err(LedgerError::PeriodCloseNotAdvancing {
                        current_closed_through: self.closed_through,
                        proposed_closed_through: closed_through,
                    });
                }
                PeriodUpdate::Set(closed_through)
            }
            LedgerEntryKind::PeriodReopen { new_closed_through } => {
                validate_control_entry(&entry)?;
                let current_closed_through = self
                    .closed_through
                    .ok_or(LedgerError::AccountingPeriodAlreadyOpen)?;
                if new_closed_through.is_some_and(|proposed| proposed >= current_closed_through) {
                    return Err(LedgerError::InvalidPeriodReopen {
                        current_closed_through,
                        proposed_closed_through: new_closed_through,
                    });
                }
                new_closed_through.map_or(PeriodUpdate::Clear, PeriodUpdate::Set)
            }
        };

        let mut next_balances = Vec::with_capacity(entry.postings.len());
        for posting in &entry.postings {
            let key = (posting.account_id, posting.asset_id);
            let current = self.balances.get(&key).copied().unwrap_or(0);
            let next = current
                .checked_add(posting.amount)
                .ok_or(LedgerError::ArithmeticOverflow)?;
            next_balances.push((key, next));
        }
        let sequence = u64::try_from(self.journal.len())
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(LedgerError::ArithmeticOverflow)?;

        Ok(PostingPreparation::Ready(PreparedPosting {
            entry,
            next_balances,
            period_update,
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
        for (key, value) in prepared.next_balances {
            self.balances.insert(key, value);
        }
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

    /// Returns a cloned canonical event at a one-based ledger sequence.
    #[must_use]
    pub fn record(&self, sequence: u64) -> Option<LedgerRecord> {
        let index = sequence
            .checked_sub(1)
            .and_then(|value| usize::try_from(value).ok())?;
        self.journal
            .get(index)
            .and_then(|record| self.materialize_record(record))
    }

    fn materialize_record(&self, record: &LedgerRecordKey) -> Option<LedgerRecord> {
        match record {
            LedgerRecordKey::Entry(transaction_id) => self
                .entries
                .get(transaction_id)
                .map(|posted| LedgerRecord::Entry(posted.entry.clone())),
            LedgerRecordKey::Correction {
                reversal_transaction_id,
                replacement_transaction_id,
            } => {
                let reversal = self.entries.get(reversal_transaction_id)?.entry.clone();
                let replacement = self.entries.get(replacement_transaction_id)?.entry.clone();
                Some(LedgerRecord::Correction(LedgerCorrection {
                    reversal,
                    replacement,
                }))
            }
            LedgerRecordKey::Batch(transaction_ids) => {
                let entries = transaction_ids
                    .iter()
                    .map(|transaction_id| {
                        self.entries
                            .get(transaction_id)
                            .map(|posted| posted.entry.clone())
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(LedgerRecord::Batch(LedgerBatch { entries }))
            }
        }
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

    /// Independently accumulates positive and negative balances for every asset.
    ///
    /// The result is in ascending asset-ID order. Totals have arbitrary exact
    /// magnitude, with an allocation-free `u128` common case. It is amortized
    /// `O(A log D)` time and `O(D + W)` memory for `A` non-zero balances, `D`
    /// asset denominations, and `W` spilled magnitude limbs.
    #[must_use]
    pub fn trial_balance(&self) -> Vec<AssetTrialBalance> {
        let mut totals: BTreeMap<AssetId, AssetSideTotals> = BTreeMap::new();
        for (&(_, asset_id), &amount) in &self.balances {
            if amount != 0 {
                totals.entry(asset_id).or_default().add(amount);
            }
        }
        totals
            .into_iter()
            .map(|(asset_id, totals)| AssetTrialBalance {
                asset_id,
                positive_total: totals.positive,
                negative_total: totals.negative,
            })
            .collect()
    }

    /// Cross-audits journal order, idempotency entries, deterministic replay,
    /// canonical balances, and per-asset trial balances.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerInvariantViolation`] at the first structural divergence.
    pub fn validate(&self) -> Result<(), LedgerInvariantViolation> {
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
        let mut records = Vec::with_capacity(self.journal.len());
        for (index, record_key) in self.journal.iter().enumerate() {
            let expected_sequence = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| LedgerInvariantViolation::new("ledger sequence overflow"))?;
            for transaction_id in (0..record_key.transaction_count())
                .filter_map(|position| record_key.transaction_id_at(position))
            {
                let posted = self.entries.get(&transaction_id).ok_or_else(|| {
                    LedgerInvariantViolation::new(format!(
                        "journal transaction {transaction_id} is absent from the entry index"
                    ))
                })?;
                if posted.sequence != expected_sequence
                    || posted.entry.transaction_id() != transaction_id
                {
                    return Err(LedgerInvariantViolation::new(format!(
                        "ledger transaction {transaction_id} has an invalid sequence or identity"
                    )));
                }
            }
            records.push(self.materialize_record(record_key).ok_or_else(|| {
                LedgerInvariantViolation::new(format!(
                    "ledger record {expected_sequence} cannot be materialized"
                ))
            })?);
        }
        let replayed = replay_records(&records).map_err(|error| {
            LedgerInvariantViolation::new(format!("ledger replay failed: {error}"))
        })?;
        if self.journal != replayed.journal {
            return Err(LedgerInvariantViolation::new(
                "stored ledger-event grouping differs from deterministic replay",
            ));
        }
        if canonical_balances(&self.balances) != canonical_balances(&replayed.balances) {
            return Err(LedgerInvariantViolation::new(
                "stored balances differ from deterministic record replay",
            ));
        }
        if self.reversals != replayed.reversals {
            return Err(LedgerInvariantViolation::new(
                "stored reversal index differs from deterministic record replay",
            ));
        }
        if self.closed_through != replayed.closed_through {
            return Err(LedgerInvariantViolation::new(
                "stored accounting-period boundary differs from deterministic record replay",
            ));
        }
        if self.last_recorded_at != replayed.last_recorded_at {
            return Err(LedgerInvariantViolation::new(
                "stored booking timestamp differs from deterministic record replay",
            ));
        }
        let trial = self.trial_balance();
        if let Some(unbalanced) = trial.into_iter().find(|value| !value.is_balanced()) {
            return Err(LedgerInvariantViolation::new(format!(
                "asset {} trial balance is not zero",
                unbalanced.asset_id()
            )));
        }
        Ok(())
    }

    /// Captures canonical balances and complete transaction-idempotency history.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerInvariantViolation`] if current state does not pass an
    /// independent replay and trial-balance audit.
    pub fn checkpoint(&self) -> Result<LedgerCheckpoint, LedgerInvariantViolation> {
        self.validate()?;
        let generation = u64::try_from(self.journal.len())
            .map_err(|_| LedgerInvariantViolation::new("ledger generation overflow"))?;
        let records = self
            .journal
            .iter()
            .enumerate()
            .map(|(index, record)| {
                self.materialize_record(record).ok_or_else(|| {
                    LedgerInvariantViolation::new(format!(
                        "validated ledger record {} disappeared from the entry index",
                        index + 1
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LedgerCheckpoint {
            generation,
            balances: canonical_balances(&self.balances),
            records,
        })
    }

    /// Reconstructs a ledger from a checkpoint whose private type invariant was
    /// established during audited capture or binary decoding.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerCheckpointError`] if deterministic event replay fails.
    pub fn from_checkpoint(checkpoint: LedgerCheckpoint) -> Result<Self, LedgerCheckpointError> {
        let mut ledger = Self::new();
        for (index, record) in checkpoint.records.into_iter().enumerate() {
            match record {
                LedgerRecord::Entry(entry) => ledger.post(entry).map(|_| ()),
                LedgerRecord::Correction(correction) => ledger.correct(correction).map(|_| ()),
                LedgerRecord::Batch(batch) => ledger.post_batch(batch).map(|_| ()),
            }
            .map_err(|error| LedgerCheckpointError::RecordReplay { index, error })?;
        }
        Ok(ledger)
    }
}

fn canonical_balances(balances: &HashMap<(AccountId, AssetId), i128>) -> Vec<LedgerBalance> {
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

fn replay_records(records: &[LedgerRecord]) -> Result<Ledger, LedgerError> {
    let mut ledger = Ledger::new();
    for record in records {
        match record {
            LedgerRecord::Entry(entry) => {
                ledger.post(entry.clone())?;
            }
            LedgerRecord::Correction(correction) => {
                ledger.correct(correction.clone())?;
            }
            LedgerRecord::Batch(batch) => {
                ledger.post_batch(batch.clone())?;
            }
        }
    }
    Ok(ledger)
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
    let mut replayed = Ledger::new();
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
    if canonical_balances(&replayed.balances) != balances {
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

#[derive(Default)]
struct BalanceDeltaTerms {
    positive: Vec<i128>,
    negative: Vec<i128>,
}

fn calculate_batch_balances(
    balances: &HashMap<BalanceKey, i128>,
    entries: &[JournalEntry],
) -> Result<Vec<BalanceUpdate>, LedgerError> {
    let posting_count = entries.iter().try_fold(0_usize, |count, entry| {
        count.checked_add(entry.postings.len())
    });
    let mut terms = HashMap::<BalanceKey, BalanceDeltaTerms>::with_capacity(
        posting_count.ok_or(LedgerError::ArithmeticOverflow)?,
    );
    for posting in entries.iter().flat_map(|entry| &entry.postings) {
        let values = terms
            .entry((posting.account_id, posting.asset_id))
            .or_default();
        if posting.amount > 0 {
            values.positive.push(posting.amount);
        } else {
            values.negative.push(posting.amount);
        }
    }

    let mut next_balances = Vec::with_capacity(terms.len());
    for (key, values) in terms {
        let current = balances.get(&key).copied().unwrap_or(0);
        let next = checked_add_signed_terms(current, &values.positive, &values.negative)
            .ok_or(LedgerError::ArithmeticOverflow)?;
        if next != current {
            next_balances.push((key, next));
        }
    }
    Ok(next_balances)
}

fn checked_add_signed_terms(mut value: i128, positive: &[i128], negative: &[i128]) -> Option<i128> {
    let mut positive_index = 0;
    let mut negative_index = 0;
    while positive_index < positive.len() || negative_index < negative.len() {
        let amount = if value >= 0 && negative_index < negative.len() {
            let amount = negative[negative_index];
            negative_index += 1;
            amount
        } else if positive_index < positive.len() {
            let amount = positive[positive_index];
            positive_index += 1;
            amount
        } else {
            let amount = negative[negative_index];
            negative_index += 1;
            amount
        };
        value = value.checked_add(amount)?;
    }
    Some(value)
}

fn calculate_correction_balances(
    balances: &HashMap<(AccountId, AssetId), i128>,
    left: &[Posting],
    right: &[Posting],
) -> Result<Vec<BalanceUpdate>, LedgerError> {
    let capacity = left
        .len()
        .checked_add(right.len())
        .ok_or(LedgerError::ArithmeticOverflow)?;
    let mut next_balances = Vec::with_capacity(capacity);
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
    base_entries: &HashMap<TransactionId, PostedEntry>,
    pending_entries: &HashMap<TransactionId, &JournalEntry>,
    base_reversals: &HashMap<TransactionId, TransactionId>,
    pending_reversals: &mut HashMap<TransactionId, TransactionId>,
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
            pending_reversals.insert(reversed_transaction_id, entry.transaction_id);
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
    let mut totals: BTreeMap<AssetId, AssetSideTotals> = BTreeMap::new();
    for balance in balances {
        totals
            .entry(balance.asset_id)
            .or_default()
            .add(balance.amount);
    }
    if let Some((asset_id, totals)) = totals.into_iter().find(|(_, totals)| !totals.is_balanced()) {
        return Err(ReconciliationError::Unbalanced {
            asset_id,
            positive_total: Box::new(totals.positive),
            negative_total: Box::new(totals.negative),
        });
    }
    Ok(())
}

fn validate_postings(postings: &[Posting]) -> Result<(), LedgerError> {
    if postings.len() < 2 {
        return Err(LedgerError::TooFewPostings);
    }
    let mut totals: BTreeMap<AssetId, AssetSideTotals> = BTreeMap::new();
    let mut keys = BTreeSet::new();
    for posting in postings {
        if posting.amount == 0 {
            return Err(LedgerError::ZeroPosting);
        }
        if !keys.insert((posting.account_id, posting.asset_id)) {
            return Err(LedgerError::DuplicateAccountAsset);
        }
        totals
            .entry(posting.asset_id)
            .or_default()
            .add(posting.amount);
    }
    for (asset_id, totals) in totals {
        if !totals.is_balanced() {
            return Err(LedgerError::Unbalanced {
                asset_id,
                positive_total: Box::new(totals.positive),
                negative_total: Box::new(totals.negative),
            });
        }
    }
    Ok(())
}
