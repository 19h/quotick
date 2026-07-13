//! Atomic multi-asset double-entry accounting and trade settlement.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use crate::domain::{AccountId, AssetId, TransactionId};
use crate::instrument::InstrumentDefinition;
use crate::matching::Trade;

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
    /// All entry legs.
    postings: Vec<Posting>,
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
    /// Posting sums for this asset were not zero.
    Unbalanced {
        /// Unbalanced asset.
        asset_id: AssetId,
        /// Signed residual that must have been zero.
        residual: i128,
    },
    /// A balance or validation sum overflowed `i128`.
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
            Self::Unbalanced { asset_id, residual } => {
                write!(
                    formatter,
                    "asset {asset_id} has unbalanced residual {residual}"
                )
            }
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
        }
    }
}

impl std::error::Error for LedgerError {}

impl JournalEntry {
    /// Validates and constructs a balanced journal entry.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] when the entry is empty, contains zero or
    /// duplicate legs, overflows during validation, or is not balanced by asset.
    pub fn new(
        transaction_id: TransactionId,
        reference: u64,
        mut postings: Vec<Posting>,
    ) -> Result<Self, LedgerError> {
        postings.sort_unstable_by_key(|posting| (posting.asset_id, posting.account_id));
        validate_postings(&postings)?;
        Ok(Self {
            transaction_id,
            reference,
            postings,
        })
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

    /// Returns the canonical account-and-asset-sorted posting legs.
    #[must_use]
    pub fn postings(&self) -> &[Posting] {
        &self.postings
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
        Self::new(transaction_id, trade.trade_id.get(), postings)
    }

    /// Constructs settlement using an exact immutable instrument definition.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError`] when the trade identity/version does not match
    /// the definition or when settlement construction fails.
    pub fn from_instrument(
        transaction_id: TransactionId,
        trade: &Trade,
        definition: InstrumentDefinition,
    ) -> Result<Self, LedgerError> {
        if trade.instrument_id != definition.instrument_id() {
            return Err(LedgerError::SettlementInstrumentMismatch);
        }
        if trade.instrument_version != definition.version() {
            return Err(LedgerError::SettlementVersionMismatch);
        }
        Self::from_trade(transaction_id, trade, definition.settlement_convention())
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

/// Result of validating an entry against a specific ledger generation.
#[derive(Debug)]
pub enum PostingPreparation {
    /// The exact transaction is already committed.
    Replay(PostReceipt),
    /// The entry is valid and ready for a single commit.
    Ready(PreparedPosting),
}

/// Validated balance changes for one ledger generation.
#[derive(Debug)]
pub struct PreparedPosting {
    entry: JournalEntry,
    next_balances: Vec<((AccountId, AssetId), i128)>,
    expected_entry_count: usize,
    sequence: u64,
}

impl PreparedPosting {
    /// Returns the immutable entry that must be durably recorded before commit.
    #[must_use]
    pub const fn entry(&self) -> &JournalEntry {
        &self.entry
    }
}

#[derive(Clone, Debug)]
struct PostedEntry {
    entry: JournalEntry,
    sequence: u64,
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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssetTrialBalance {
    asset_id: AssetId,
    positive_total: u128,
    negative_total: u128,
}

impl AssetTrialBalance {
    /// Returns the asset denomination.
    #[must_use]
    pub const fn asset_id(self) -> AssetId {
        self.asset_id
    }

    /// Returns the sum of strictly positive account balances.
    #[must_use]
    pub const fn positive_total(self) -> u128 {
        self.positive_total
    }

    /// Returns the absolute sum of strictly negative account balances.
    #[must_use]
    pub const fn negative_total(self) -> u128 {
        self.negative_total
    }

    /// Returns whether independently accumulated sides are equal.
    #[must_use]
    pub const fn is_balanced(self) -> bool {
        self.positive_total == self.negative_total
    }
}

/// Immutable canonical ledger state plus complete idempotency history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerCheckpoint {
    generation: u64,
    balances: Vec<LedgerBalance>,
    entries: Vec<JournalEntry>,
}

impl LedgerCheckpoint {
    pub(crate) fn from_parts(
        generation: u64,
        balances: Vec<LedgerBalance>,
        entries: Vec<JournalEntry>,
    ) -> Result<Self, LedgerCheckpointError> {
        validate_checkpoint(generation, &balances, &entries)?;
        Ok(Self {
            generation,
            balances,
            entries,
        })
    }

    /// Returns the number of entries covered by the checkpoint.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns non-zero balances in strict `(asset, account)` order.
    #[must_use]
    pub fn balances(&self) -> &[LedgerBalance] {
        &self.balances
    }

    /// Returns canonical entries in ledger sequence order.
    #[must_use]
    pub fn entries(&self) -> &[JournalEntry] {
        &self.entries
    }
}

/// Semantic checkpoint validation or restoration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerCheckpointError {
    /// Generation did not equal the number of persisted entries.
    GenerationMismatch {
        /// Encoded generation.
        generation: u64,
        /// Number of entries represented by the checkpoint.
        entry_count: usize,
    },
    /// Balances were zero, duplicated, or not strictly `(asset, account)` sorted.
    NonCanonicalBalances,
    /// Replaying checkpoint entries failed.
    EntryReplay {
        /// Zero-based entry position.
        index: usize,
        /// Ledger failure raised by deterministic replay.
        error: LedgerError,
    },
    /// An exact transaction retry appeared as another checkpoint journal record.
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
                entry_count,
            } => write!(
                formatter,
                "ledger checkpoint generation {generation} differs from {entry_count} entries"
            ),
            Self::NonCanonicalBalances => formatter.write_str(
                "ledger checkpoint balances must be non-zero and strictly sorted by asset/account",
            ),
            Self::EntryReplay { index, error } => {
                write!(
                    formatter,
                    "ledger checkpoint entry {index} failed replay: {error}"
                )
            }
            Self::DuplicateTransaction {
                index,
                transaction_id,
            } => write!(
                formatter,
                "ledger checkpoint entry {index} duplicates transaction {transaction_id}"
            ),
            Self::BalanceMismatch => formatter
                .write_str("ledger checkpoint balances differ from deterministic entry replay"),
        }
    }
}

impl std::error::Error for LedgerCheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::EntryReplay { error, .. } => Some(error),
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
    journal: Vec<TransactionId>,
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
        validate_postings(&entry.postings)?;

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
            expected_entry_count: self.journal.len(),
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
        if self.journal.len() != prepared.expected_entry_count
            || self.entries.contains_key(&prepared.entry.transaction_id)
        {
            return Err(LedgerError::StalePreparation);
        }
        for (key, value) in prepared.next_balances {
            self.balances.insert(key, value);
        }
        let transaction_id = prepared.entry.transaction_id;
        self.entries.insert(
            transaction_id,
            PostedEntry {
                entry: prepared.entry,
                sequence: prepared.sequence,
            },
        );
        self.journal.push(transaction_id);
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
        trade: &Trade,
        convention: SettlementConvention,
    ) -> Result<PostReceipt, LedgerError> {
        let entry = JournalEntry::from_trade(transaction_id, trade, convention)?;
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
        trade: &Trade,
        definition: InstrumentDefinition,
    ) -> Result<PostReceipt, LedgerError> {
        let entry = JournalEntry::from_instrument(transaction_id, trade, definition)?;
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

    /// Returns the number of committed entries.
    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.journal.len()
    }

    /// Returns transaction identifiers in journal sequence order.
    #[must_use]
    pub fn transaction_ids(&self) -> impl ExactSizeIterator<Item = TransactionId> + '_ {
        self.journal.iter().copied()
    }

    /// Returns the canonical entry at a one-based ledger sequence.
    #[must_use]
    pub fn entry(&self, sequence: u64) -> Option<&JournalEntry> {
        let index = sequence
            .checked_sub(1)
            .and_then(|value| usize::try_from(value).ok())?;
        let transaction_id = self.journal.get(index)?;
        self.entries.get(transaction_id).map(|posted| &posted.entry)
    }

    /// Independently accumulates positive and negative balances for every asset.
    ///
    /// The result is in ascending asset-ID order. It is `O(A)` time and `O(D)`
    /// memory for `A` non-zero account balances and `D` asset denominations.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerError::ArithmeticOverflow`] if either unsigned side
    /// cannot be accumulated in `u128`.
    pub fn trial_balance(&self) -> Result<Vec<AssetTrialBalance>, LedgerError> {
        let mut totals: BTreeMap<AssetId, (u128, u128)> = BTreeMap::new();
        for (&(_, asset_id), &amount) in &self.balances {
            if amount == 0 {
                continue;
            }
            let total = totals.entry(asset_id).or_default();
            if amount > 0 {
                total.0 = total
                    .0
                    .checked_add(amount.unsigned_abs())
                    .ok_or(LedgerError::ArithmeticOverflow)?;
            } else {
                total.1 = total
                    .1
                    .checked_add(amount.unsigned_abs())
                    .ok_or(LedgerError::ArithmeticOverflow)?;
            }
        }
        Ok(totals
            .into_iter()
            .map(
                |(asset_id, (positive_total, negative_total))| AssetTrialBalance {
                    asset_id,
                    positive_total,
                    negative_total,
                },
            )
            .collect())
    }

    /// Cross-audits journal order, idempotency entries, deterministic replay,
    /// canonical balances, and per-asset trial balances.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerInvariantViolation`] at the first structural divergence.
    pub fn validate(&self) -> Result<(), LedgerInvariantViolation> {
        if self.entries.len() != self.journal.len() {
            return Err(LedgerInvariantViolation::new(
                "ledger entry index length differs from journal length",
            ));
        }
        let mut ordered = Vec::with_capacity(self.journal.len());
        for (index, transaction_id) in self.journal.iter().copied().enumerate() {
            let posted = self.entries.get(&transaction_id).ok_or_else(|| {
                LedgerInvariantViolation::new(format!(
                    "journal transaction {transaction_id} is absent from the entry index"
                ))
            })?;
            let expected_sequence = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or_else(|| LedgerInvariantViolation::new("ledger sequence overflow"))?;
            if posted.sequence != expected_sequence
                || posted.entry.transaction_id() != transaction_id
            {
                return Err(LedgerInvariantViolation::new(format!(
                    "ledger transaction {transaction_id} has an invalid sequence or identity"
                )));
            }
            ordered.push(posted.entry.clone());
        }
        let replayed = replay_entries(&ordered).map_err(|error| {
            LedgerInvariantViolation::new(format!("ledger replay failed: {error}"))
        })?;
        if canonical_balances(&self.balances) != canonical_balances(&replayed.balances) {
            return Err(LedgerInvariantViolation::new(
                "stored balances differ from deterministic entry replay",
            ));
        }
        let trial = self
            .trial_balance()
            .map_err(|error| LedgerInvariantViolation::new(error.to_string()))?;
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
        let entries = self
            .journal
            .iter()
            .map(|transaction_id| {
                self.entries
                    .get(transaction_id)
                    .map(|posted| posted.entry.clone())
                    .ok_or_else(|| {
                        LedgerInvariantViolation::new(format!(
                            "validated transaction {transaction_id} disappeared from the entry index"
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LedgerCheckpoint {
            generation,
            balances: canonical_balances(&self.balances),
            entries,
        })
    }

    /// Reconstructs a ledger from a checkpoint whose private type invariant was
    /// established during audited capture or binary decoding.
    ///
    /// # Errors
    ///
    /// Returns [`LedgerCheckpointError`] if the validated generation cannot be
    /// represented while rebuilding one-based entry sequences.
    pub fn from_checkpoint(checkpoint: LedgerCheckpoint) -> Result<Self, LedgerCheckpointError> {
        let mut balances = HashMap::with_capacity(checkpoint.balances.len());
        for balance in checkpoint.balances {
            balances.insert((balance.account_id, balance.asset_id), balance.amount);
        }
        let mut entries = HashMap::with_capacity(checkpoint.entries.len());
        let mut journal = Vec::with_capacity(checkpoint.entries.len());
        for (index, entry) in checkpoint.entries.into_iter().enumerate() {
            let sequence = u64::try_from(index)
                .ok()
                .and_then(|value| value.checked_add(1))
                .ok_or(LedgerCheckpointError::GenerationMismatch {
                    generation: checkpoint.generation,
                    entry_count: index,
                })?;
            let transaction_id = entry.transaction_id();
            entries.insert(transaction_id, PostedEntry { entry, sequence });
            journal.push(transaction_id);
        }
        Ok(Self {
            balances,
            entries,
            journal,
        })
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

fn replay_entries(entries: &[JournalEntry]) -> Result<Ledger, LedgerError> {
    let mut ledger = Ledger::new();
    for entry in entries {
        ledger.post(entry.clone())?;
    }
    Ok(ledger)
}

fn validate_checkpoint(
    generation: u64,
    balances: &[LedgerBalance],
    entries: &[JournalEntry],
) -> Result<(), LedgerCheckpointError> {
    if usize::try_from(generation).ok() != Some(entries.len()) {
        return Err(LedgerCheckpointError::GenerationMismatch {
            generation,
            entry_count: entries.len(),
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
    for (index, entry) in entries.iter().enumerate() {
        let receipt = replayed
            .post(entry.clone())
            .map_err(|error| LedgerCheckpointError::EntryReplay { index, error })?;
        if receipt.replayed {
            return Err(LedgerCheckpointError::DuplicateTransaction {
                index,
                transaction_id: entry.transaction_id(),
            });
        }
    }
    if canonical_balances(&replayed.balances) != balances {
        return Err(LedgerCheckpointError::BalanceMismatch);
    }
    Ok(())
}

fn validate_postings(postings: &[Posting]) -> Result<(), LedgerError> {
    if postings.len() < 2 {
        return Err(LedgerError::TooFewPostings);
    }
    let mut sums: BTreeMap<AssetId, i128> = BTreeMap::new();
    let mut keys = BTreeSet::new();
    for posting in postings {
        if posting.amount == 0 {
            return Err(LedgerError::ZeroPosting);
        }
        if !keys.insert((posting.account_id, posting.asset_id)) {
            return Err(LedgerError::DuplicateAccountAsset);
        }
        let current = sums.get(&posting.asset_id).copied().unwrap_or(0);
        let sum = current
            .checked_add(posting.amount)
            .ok_or(LedgerError::ArithmeticOverflow)?;
        sums.insert(posting.asset_id, sum);
    }
    for (asset_id, residual) in sums {
        if residual != 0 {
            return Err(LedgerError::Unbalanced { asset_id, residual });
        }
    }
    Ok(())
}
