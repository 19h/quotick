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
