//! Entry-before-balance durable orchestration for the double-entry ledger.

use std::fmt;
use std::path::Path;

use crate::domain::TransactionId;
use crate::instrument::InstrumentDefinition;
use crate::journal::{
    Journal, JournalError, JournalOptions, JournalReader, RecordKind, RecoveryReport,
};
use crate::ledger::{
    JournalEntry, Ledger, LedgerError, PostReceipt, PostingPreparation, SettlementConvention,
};
use crate::matching::Trade;

/// Result of reconstructing a durable ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableLedgerRecoveryReport {
    /// Physical journal recovery performed at open.
    pub journal: RecoveryReport,
    /// Balanced entries replayed into account balances.
    pub replayed_entries: u64,
}

/// Durable ledger orchestration or replay failure.
#[derive(Debug)]
pub enum DurableLedgerError {
    /// Journal framing, codec, recovery, or I/O failure.
    Journal(JournalError),
    /// Entry validation, collision, arithmetic, or preparation failure.
    Ledger(LedgerError),
    /// A matching record appeared in a ledger-only journal.
    UnexpectedRecord {
        /// Journal frame sequence.
        sequence: u64,
        /// Unexpected record kind.
        kind: RecordKind,
    },
    /// A transaction was present more than once in the WAL.
    DuplicateTransactionRecord {
        /// Journal frame sequence containing the duplicate.
        sequence: u64,
        /// Repeated transaction identifier.
        transaction_id: TransactionId,
    },
    /// The runtime is unusable until reopened after an ambiguous transition.
    Poisoned,
}

impl fmt::Display for DurableLedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(error) => error.fmt(formatter),
            Self::Ledger(error) => error.fmt(formatter),
            Self::UnexpectedRecord { sequence, kind } => {
                write!(
                    formatter,
                    "unexpected {kind:?} record at ledger WAL sequence {sequence}"
                )
            }
            Self::DuplicateTransactionRecord {
                sequence,
                transaction_id,
            } => write!(
                formatter,
                "duplicate transaction {transaction_id} at ledger WAL sequence {sequence}"
            ),
            Self::Poisoned => {
                formatter.write_str("durable ledger is poisoned and must be reopened for recovery")
            }
        }
    }
}

impl std::error::Error for DurableLedgerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(error) => Some(error),
            Self::Ledger(error) => Some(error),
            _ => None,
        }
    }
}

impl From<JournalError> for DurableLedgerError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

impl From<LedgerError> for DurableLedgerError {
    fn from(error: LedgerError) -> Self {
        Self::Ledger(error)
    }
}

/// Crash-recoverable single-writer double-entry ledger.
#[derive(Debug)]
pub struct DurableLedger {
    ledger: Ledger,
    journal: Journal,
    recovery: DurableLedgerRecoveryReport,
    poisoned: bool,
}

impl DurableLedger {
    /// Opens and verifies a ledger WAL, then reconstructs every balance from its
    /// canonical entries.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for journal failure, unexpected record
    /// kinds, duplicate transaction records, or ledger validation failure.
    pub fn open(
        path: impl AsRef<Path>,
        options: JournalOptions,
    ) -> Result<Self, DurableLedgerError> {
        let path = path.as_ref();
        let journal = Journal::open(path, options)?;
        let journal_recovery = journal.recovery();
        let reader = JournalReader::open(path, options)?;
        let mut ledger = Ledger::new();
        let mut replayed_entries = 0_u64;
        for frame_result in reader {
            let frame = frame_result?;
            if frame.kind() != RecordKind::LedgerEntry {
                return Err(DurableLedgerError::UnexpectedRecord {
                    sequence: frame.sequence(),
                    kind: frame.kind(),
                });
            }
            let entry: JournalEntry = frame.decode()?;
            let transaction_id = entry.transaction_id();
            let receipt = ledger.post(entry)?;
            if receipt.replayed {
                return Err(DurableLedgerError::DuplicateTransactionRecord {
                    sequence: frame.sequence(),
                    transaction_id,
                });
            }
            replayed_entries = replayed_entries
                .checked_add(1)
                .ok_or(LedgerError::ArithmeticOverflow)?;
        }
        Ok(Self {
            ledger,
            journal,
            recovery: DurableLedgerRecoveryReport {
                journal: journal_recovery,
                replayed_entries,
            },
            poisoned: false,
        })
    }

    /// Returns recovery statistics captured at open.
    #[must_use]
    pub const fn recovery(&self) -> DurableLedgerRecoveryReport {
        self.recovery
    }

    /// Returns the reconstructed read-only ledger.
    #[must_use]
    pub const fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    /// Validates an entry, durably records it, then commits its prepared balances.
    ///
    /// Exact retries return the original receipt without adding a WAL frame.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for ledger preparation, WAL, or commit
    /// failure. A failure after WAL acknowledgement poisons this instance.
    pub fn post(&mut self, entry: JournalEntry) -> Result<PostReceipt, DurableLedgerError> {
        if self.poisoned {
            return Err(DurableLedgerError::Poisoned);
        }
        let prepared = match self.ledger.prepare(entry)? {
            PostingPreparation::Replay(receipt) => return Ok(receipt),
            PostingPreparation::Ready(prepared) => prepared,
        };
        if let Err(error) = self.journal.append(prepared.entry()) {
            self.poisoned = self.journal.is_poisoned();
            return Err(DurableLedgerError::Journal(error));
        }
        match self.ledger.commit(prepared) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.poisoned = true;
                Err(DurableLedgerError::Ledger(error))
            }
        }
    }

    /// Creates and durably posts a delivery-versus-payment entry for a trade.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for invalid settlement, arithmetic,
    /// journal, or commit failure.
    pub fn settle_trade(
        &mut self,
        transaction_id: TransactionId,
        trade: &Trade,
        convention: SettlementConvention,
    ) -> Result<PostReceipt, DurableLedgerError> {
        let entry = JournalEntry::from_trade(transaction_id, trade, convention)?;
        self.post(entry)
    }

    /// Durably settles a trade under its exact immutable instrument version.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for definition mismatch, invalid
    /// settlement, arithmetic, journal, or commit failure.
    pub fn settle_instrument_trade(
        &mut self,
        transaction_id: TransactionId,
        trade: &Trade,
        definition: InstrumentDefinition,
    ) -> Result<PostReceipt, DurableLedgerError> {
        let entry = JournalEntry::from_instrument(transaction_id, trade, definition)?;
        self.post(entry)
    }

    /// Synchronizes ledger WAL data and metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] and poisons the runtime when synchronization fails.
    pub fn sync_all(&mut self) -> Result<(), DurableLedgerError> {
        if self.poisoned {
            return Err(DurableLedgerError::Poisoned);
        }
        if let Err(error) = self.journal.sync_all() {
            self.poisoned = true;
            return Err(DurableLedgerError::Journal(error));
        }
        Ok(())
    }

    /// Synchronizes all accounting data and metadata, then releases exclusive
    /// writer ownership.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] if the runtime is poisoned or final
    /// journal synchronization/lease release fails.
    pub fn close(self) -> Result<(), DurableLedgerError> {
        if self.poisoned {
            return Err(DurableLedgerError::Poisoned);
        }
        self.journal.close().map_err(DurableLedgerError::Journal)
    }
}
