//! Entry-before-balance durable orchestration for the double-entry ledger.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::domain::TransactionId;
use crate::durable_storage::{StorageError, StorageOptions, StorageReader, StorageWriter};
use crate::instrument::InstrumentDefinition;
use crate::journal::{
    Journal, JournalError, JournalLayout, JournalOptions, RecordKind, SegmentedJournalError,
    SegmentedJournalOptions, StorageRecoveryReport, normalize_journal_path,
};
use crate::ledger::{
    JournalEntry, Ledger, LedgerCheckpoint, LedgerCheckpointError, LedgerError,
    LedgerInvariantViolation, PostReceipt, PostingPreparation, SettlementConvention,
};
use crate::matching::Trade;
use crate::snapshot::{SnapshotError, SnapshotFile, SnapshotOptions, SnapshotReceipt};

/// Result of reconstructing a durable ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableLedgerRecoveryReport {
    /// Physical journal recovery performed at open.
    pub journal: StorageRecoveryReport,
    /// Entries restored from a semantically verified checkpoint.
    pub checkpointed_entries: u64,
    /// Balanced entries replayed into account balances.
    pub replayed_entries: u64,
}

/// Durable ledger orchestration or replay failure.
#[derive(Debug)]
pub enum DurableLedgerError {
    /// Journal framing, codec, recovery, or I/O failure.
    Journal(JournalError),
    /// Segmented-journal inventory, rotation, framing, recovery, or I/O failure.
    SegmentedJournal(SegmentedJournalError),
    /// Entry validation, collision, arithmetic, or preparation failure.
    Ledger(LedgerError),
    /// Semantic checkpoint construction or restoration failed.
    Checkpoint(LedgerCheckpointError),
    /// Ledger state failed an independent structural/replay audit.
    Invariant(LedgerInvariantViolation),
    /// Snapshot framing, ownership, checksum, generation, or I/O failed.
    Snapshot(SnapshotError),
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
    /// A checkpoint entry differed from the same WAL prefix position.
    CheckpointPrefixDivergence {
        /// Physical WAL frame sequence.
        wal_sequence: u64,
        /// Zero-based checkpoint entry position.
        checkpoint_index: u64,
    },
    /// A checkpoint claimed more entries than the complete verified WAL.
    CheckpointAheadOfWal {
        /// Entries represented by the checkpoint.
        checkpoint_entries: u64,
        /// Complete ledger-entry frames present in the WAL.
        wal_entries: u64,
    },
    /// Snapshot output aliases a WAL file, lease, pending path, or managed directory.
    CheckpointPathConflictsWithWal {
        /// Conflicting canonical path.
        path: PathBuf,
    },
    /// The runtime is unusable until reopened after an ambiguous transition.
    Poisoned,
}

impl fmt::Display for DurableLedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(error) => error.fmt(formatter),
            Self::SegmentedJournal(error) => error.fmt(formatter),
            Self::Ledger(error) => error.fmt(formatter),
            Self::Checkpoint(error) => error.fmt(formatter),
            Self::Invariant(error) => error.fmt(formatter),
            Self::Snapshot(error) => error.fmt(formatter),
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
            Self::CheckpointPrefixDivergence {
                wal_sequence,
                checkpoint_index,
            } => write!(
                formatter,
                "ledger checkpoint entry {checkpoint_index} differs from WAL frame {wal_sequence}"
            ),
            Self::CheckpointAheadOfWal {
                checkpoint_entries,
                wal_entries,
            } => write!(
                formatter,
                "ledger checkpoint covers {checkpoint_entries} entries but WAL has {wal_entries}"
            ),
            Self::CheckpointPathConflictsWithWal { path } => write!(
                formatter,
                "checkpoint path {} conflicts with WAL storage",
                path.display()
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
            Self::SegmentedJournal(error) => Some(error),
            Self::Ledger(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            Self::Invariant(error) => Some(error),
            Self::Snapshot(error) => Some(error),
            _ => None,
        }
    }
}

impl From<JournalError> for DurableLedgerError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

impl From<SegmentedJournalError> for DurableLedgerError {
    fn from(error: SegmentedJournalError) -> Self {
        Self::SegmentedJournal(error)
    }
}

impl From<StorageError> for DurableLedgerError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Journal(error) => Self::Journal(error),
            StorageError::Segmented(error) => Self::SegmentedJournal(error),
        }
    }
}

impl From<LedgerError> for DurableLedgerError {
    fn from(error: LedgerError) -> Self {
        Self::Ledger(error)
    }
}

impl From<LedgerCheckpointError> for DurableLedgerError {
    fn from(error: LedgerCheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

impl From<LedgerInvariantViolation> for DurableLedgerError {
    fn from(error: LedgerInvariantViolation) -> Self {
        Self::Invariant(error)
    }
}

impl From<SnapshotError> for DurableLedgerError {
    fn from(error: SnapshotError) -> Self {
        Self::Snapshot(error)
    }
}

/// Crash-recoverable single-writer double-entry ledger.
#[derive(Debug)]
pub struct DurableLedger {
    ledger: Ledger,
    journal: StorageWriter,
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
        Self::open_storage(path.as_ref(), StorageOptions::Single(options), None)
    }

    /// Opens a single-file ledger WAL using a checksum-verified semantic
    /// checkpoint and proves that the checkpoint equals the exact WAL prefix.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for snapshot, path-alias, WAL-prefix,
    /// journal, or ledger validation failure.
    pub fn open_with_checkpoint(
        path: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        options: JournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableLedgerError> {
        Self::open_storage(
            path.as_ref(),
            StorageOptions::Single(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens and reconstructs a ledger over an automatically rotating WAL directory.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for segmented storage, grammar,
    /// duplicate-transaction, or ledger validation failure.
    pub fn open_segmented(
        directory: impl AsRef<Path>,
        options: SegmentedJournalOptions,
    ) -> Result<Self, DurableLedgerError> {
        Self::open_storage(directory.as_ref(), StorageOptions::Segmented(options), None)
    }

    /// Opens a segmented ledger WAL using a checksum-verified semantic
    /// checkpoint and proves its complete entry history against the WAL prefix.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for snapshot, managed-directory,
    /// WAL-prefix, segmented journal, or ledger validation failure.
    pub fn open_segmented_with_checkpoint(
        directory: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        options: SegmentedJournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableLedgerError> {
        Self::open_storage(
            directory.as_ref(),
            StorageOptions::Segmented(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    fn open_storage(
        path: &Path,
        options: StorageOptions,
        checkpoint_source: Option<(&Path, SnapshotOptions)>,
    ) -> Result<Self, DurableLedgerError> {
        if let Some((checkpoint_path, _)) = checkpoint_source {
            validate_checkpoint_path(path, storage_layout(options), checkpoint_path)?;
        }
        let journal = StorageWriter::open(path, options)?;
        let journal_recovery = journal.recovery();
        let checkpoint = checkpoint_source
            .map(|(checkpoint_path, snapshot_options)| {
                SnapshotFile::read::<LedgerCheckpoint>(checkpoint_path, snapshot_options)
            })
            .transpose()?;
        let reader = StorageReader::open(path, options)?;
        let checkpointed_entries = checkpoint.as_ref().map_or(0, LedgerCheckpoint::generation);
        let mut ledger = checkpoint
            .map(Ledger::from_checkpoint)
            .transpose()?
            .unwrap_or_default();
        let mut replayed_entries = 0_u64;
        let mut wal_entries = 0_u64;
        for frame_result in reader {
            let frame = frame_result?;
            if frame.kind() != RecordKind::LedgerEntry {
                return Err(DurableLedgerError::UnexpectedRecord {
                    sequence: frame.sequence(),
                    kind: frame.kind(),
                });
            }
            let entry: JournalEntry = frame.decode()?;
            if wal_entries < checkpointed_entries {
                let checkpoint_sequence = wal_entries
                    .checked_add(1)
                    .ok_or(LedgerError::ArithmeticOverflow)?;
                if ledger.entry(checkpoint_sequence) != Some(&entry) {
                    return Err(DurableLedgerError::CheckpointPrefixDivergence {
                        wal_sequence: frame.sequence(),
                        checkpoint_index: wal_entries,
                    });
                }
                wal_entries = checkpoint_sequence;
                continue;
            }
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
            wal_entries = wal_entries
                .checked_add(1)
                .ok_or(LedgerError::ArithmeticOverflow)?;
        }
        if wal_entries < checkpointed_entries {
            return Err(DurableLedgerError::CheckpointAheadOfWal {
                checkpoint_entries: checkpointed_entries,
                wal_entries,
            });
        }
        ledger.validate()?;
        Ok(Self {
            ledger,
            journal,
            recovery: DurableLedgerRecoveryReport {
                journal: journal_recovery,
                checkpointed_entries,
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
            return Err(error.into());
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

    /// Synchronizes the WAL, independently audits ledger state, and atomically
    /// replaces a checksum-protected semantic checkpoint.
    ///
    /// Checkpoint output must not alias the WAL, either writer lease, a pending
    /// snapshot path, or any path inside a managed segment directory.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for poison, path conflict, WAL barrier,
    /// ledger invariant, snapshot ownership, codec, checksum, or I/O failure.
    pub fn write_checkpoint(
        &mut self,
        path: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<SnapshotReceipt, DurableLedgerError> {
        if self.poisoned {
            return Err(DurableLedgerError::Poisoned);
        }
        validate_checkpoint_path(self.journal.path(), self.journal.layout(), path.as_ref())?;
        self.sync_all()?;
        let checkpoint = self.ledger.checkpoint()?;
        SnapshotFile::write(path, &checkpoint, options).map_err(Into::into)
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
            return Err(error.into());
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
        self.journal.close().map_err(Into::into)
    }
}

const fn storage_layout(options: StorageOptions) -> JournalLayout {
    match options {
        StorageOptions::Single(_) => JournalLayout::SingleFile,
        StorageOptions::Segmented(_) => JournalLayout::Segmented,
    }
}

fn validate_checkpoint_path(
    storage_path: &Path,
    layout: JournalLayout,
    checkpoint_path: &Path,
) -> Result<(), DurableLedgerError> {
    let storage_path = normalize_journal_path(storage_path).map_err(JournalError::Io)?;
    let checkpoint_path = normalize_journal_path(checkpoint_path).map_err(JournalError::Io)?;
    let pending_path = normalize_journal_path(&SnapshotFile::pending_path(&checkpoint_path))
        .map_err(JournalError::Io)?;
    let checkpoint_lease = SnapshotFile::writer_lease_path(&checkpoint_path)?;
    let snapshot_mutations = [&checkpoint_path, &pending_path, &checkpoint_lease];
    match layout {
        JournalLayout::Segmented => {
            if let Some(conflict) = snapshot_mutations
                .into_iter()
                .find(|path| path.starts_with(&storage_path))
            {
                return Err(DurableLedgerError::CheckpointPathConflictsWithWal {
                    path: conflict.clone(),
                });
            }
        }
        JournalLayout::SingleFile => {
            let storage_lease = Journal::writer_lease_path(&storage_path)?;
            let storage_paths = [&storage_path, &storage_lease];
            if let Some(conflict) = snapshot_mutations
                .into_iter()
                .find(|snapshot| storage_paths.contains(snapshot))
            {
                return Err(DurableLedgerError::CheckpointPathConflictsWithWal {
                    path: conflict.clone(),
                });
            }
        }
    }
    Ok(())
}
