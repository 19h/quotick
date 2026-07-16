//! Entry-before-balance durable orchestration for the double-entry ledger.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::domain::{AccountingDate, TimestampNs, TransactionId};
use crate::durable_storage::{StorageError, StorageOptions, StorageReader, StorageWriter};
use crate::instrument::InstrumentDefinition;
use crate::journal::{
    Journal, JournalError, JournalFrame, JournalLayout, JournalOptions, RecordKind,
    SegmentedJournalError, SegmentedJournalOptions, StorageRecoveryReport, normalize_journal_path,
};
use crate::ledger::{
    BatchPreparation, BatchReceipt, CallAuctionSettlement, CallAuctionSettlementReceipt,
    CallAuctionSettlementRecord, CorrectionPreparation, CorrectionReceipt, JournalEntry, Ledger,
    LedgerBatch, LedgerCheckpoint, LedgerCheckpointCaptureError, LedgerCheckpointError,
    LedgerConstructionError, LedgerCorrection, LedgerError, LedgerInvariantViolation,
    LedgerLimitsSpec, LedgerRecord, PostReceipt, PostingPreparation, SettlementConvention,
};
use crate::matching::Trade;
use crate::snapshot::{
    CheckpointAnchor, CheckpointCutoverReceipt, CheckpointSlot, SnapshotError, SnapshotFile,
    SnapshotKind, SnapshotOptions, SnapshotReceipt,
};

/// Result of reconstructing a durable ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableLedgerRecoveryReport {
    /// Physical journal recovery performed at open.
    pub journal: StorageRecoveryReport,
    /// Ledger events restored from a semantically verified checkpoint.
    pub checkpointed_records: u64,
    /// Ledger events replayed into account balances.
    pub replayed_records: u64,
}

/// Durable ledger orchestration or replay failure.
#[derive(Debug)]
pub enum DurableLedgerError {
    /// Finite authoritative ledger storage could not be constructed.
    Construction(LedgerConstructionError),
    /// Journal framing, codec, recovery, or I/O failure.
    Journal(JournalError),
    /// Segmented-journal inventory, rotation, framing, recovery, or I/O failure.
    SegmentedJournal(SegmentedJournalError),
    /// Entry validation, collision, arithmetic, or preparation failure.
    Ledger(LedgerError),
    /// Semantic checkpoint construction or restoration failed.
    Checkpoint(LedgerCheckpointError),
    /// Audited checkpoint capture failed before snapshot or cutover mutation.
    CheckpointCapture(LedgerCheckpointCaptureError),
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
    /// A checkpoint record differed from the same WAL prefix position.
    CheckpointPrefixDivergence {
        /// Physical WAL frame sequence.
        wal_sequence: u64,
        /// Zero-based checkpoint record position.
        checkpoint_record_index: u64,
    },
    /// A checkpoint claimed more records than the complete verified WAL.
    CheckpointAheadOfWal {
        /// Events represented by the checkpoint.
        checkpoint_records: u64,
        /// Complete ledger-event frames present in the WAL.
        wal_records: u64,
    },
    /// A compacted ledger WAL requires its anchor-selected checkpoint base path.
    CheckpointRequiredForCompactedWal {
        /// Physical sequence of the checkpoint anchor.
        sequence: u64,
    },
    /// A compacted ledger WAL anchor names another checkpoint kind.
    CheckpointAnchorKindMismatch {
        /// Kind stored in the anchor.
        actual: SnapshotKind,
    },
    /// The selected ledger checkpoint does not equal the anchor identity.
    CheckpointAnchorMismatch {
        /// Anchored semantic ledger generation.
        generation: u64,
        /// Selected alternating checkpoint slot.
        slot: CheckpointSlot,
    },
    /// An empty ledger has no durable frame boundary to replace with an anchor.
    CompactionRequiresNonEmptyLedger,
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
            Self::Construction(error) => error.fmt(formatter),
            Self::Journal(error) => error.fmt(formatter),
            Self::SegmentedJournal(error) => error.fmt(formatter),
            Self::Ledger(error) => error.fmt(formatter),
            Self::Checkpoint(error) => error.fmt(formatter),
            Self::CheckpointCapture(error) => error.fmt(formatter),
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
                checkpoint_record_index,
            } => write!(
                formatter,
                "ledger checkpoint record {checkpoint_record_index} differs from WAL frame {wal_sequence}"
            ),
            Self::CheckpointAheadOfWal {
                checkpoint_records,
                wal_records,
            } => write!(
                formatter,
                "ledger checkpoint covers {checkpoint_records} records but WAL has {wal_records}"
            ),
            Self::CheckpointRequiredForCompactedWal { sequence } => write!(
                formatter,
                "ledger WAL begins with checkpoint anchor {sequence}; a checkpoint base path is required"
            ),
            Self::CheckpointAnchorKindMismatch { actual } => write!(
                formatter,
                "ledger WAL anchor references incompatible snapshot kind {actual:?}"
            ),
            Self::CheckpointAnchorMismatch { generation, slot } => write!(
                formatter,
                "ledger WAL anchor for semantic generation {generation} does not match checkpoint slot {slot:?}"
            ),
            Self::CompactionRequiresNonEmptyLedger => {
                formatter.write_str("an empty ledger has no WAL prefix to compact")
            }
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
            Self::Construction(error) => Some(error),
            Self::Journal(error) => Some(error),
            Self::SegmentedJournal(error) => Some(error),
            Self::Ledger(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            Self::CheckpointCapture(error) => Some(error),
            Self::Invariant(error) => Some(error),
            Self::Snapshot(error) => Some(error),
            _ => None,
        }
    }
}

impl From<LedgerConstructionError> for DurableLedgerError {
    fn from(error: LedgerConstructionError) -> Self {
        Self::Construction(error)
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

impl From<LedgerCheckpointCaptureError> for DurableLedgerError {
    fn from(error: LedgerCheckpointCaptureError) -> Self {
        Self::CheckpointCapture(error)
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
    checkpoint_slot: Option<CheckpointSlot>,
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
        Self::open_with_limits(path, options, LedgerLimitsSpec::default())
    }

    /// Opens and reconstructs a single-file ledger under explicit finite
    /// authoritative resource limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for construction, storage, replay, or
    /// invariant failure.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        options: JournalOptions,
        limits: LedgerLimitsSpec,
    ) -> Result<Self, DurableLedgerError> {
        Self::open_storage(path.as_ref(), StorageOptions::Single(options), None, limits)
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
        Self::open_with_checkpoint_and_limits(
            path,
            checkpoint_path,
            options,
            snapshot_options,
            LedgerLimitsSpec::default(),
        )
    }

    /// Opens a single-file ledger from a verified checkpoint under explicit
    /// finite authoritative resource limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for construction, snapshot, path, WAL,
    /// replay, or invariant failure.
    pub fn open_with_checkpoint_and_limits(
        path: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        options: JournalOptions,
        snapshot_options: SnapshotOptions,
        limits: LedgerLimitsSpec,
    ) -> Result<Self, DurableLedgerError> {
        Self::open_storage(
            path.as_ref(),
            StorageOptions::Single(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
            limits,
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
        Self::open_segmented_with_limits(directory, options, LedgerLimitsSpec::default())
    }

    /// Opens a segmented ledger under explicit finite authoritative limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for construction, storage, replay, or
    /// invariant failure.
    pub fn open_segmented_with_limits(
        directory: impl AsRef<Path>,
        options: SegmentedJournalOptions,
        limits: LedgerLimitsSpec,
    ) -> Result<Self, DurableLedgerError> {
        Self::open_storage(
            directory.as_ref(),
            StorageOptions::Segmented(options),
            None,
            limits,
        )
    }

    /// Opens a segmented ledger WAL using a checksum-verified semantic
    /// checkpoint and proves its complete record history against the WAL prefix.
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
        Self::open_segmented_with_checkpoint_and_limits(
            directory,
            checkpoint_path,
            options,
            snapshot_options,
            LedgerLimitsSpec::default(),
        )
    }

    /// Opens a segmented ledger from a verified checkpoint under explicit
    /// finite authoritative resource limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for construction, snapshot, storage,
    /// replay, or invariant failure.
    pub fn open_segmented_with_checkpoint_and_limits(
        directory: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        options: SegmentedJournalOptions,
        snapshot_options: SnapshotOptions,
        limits: LedgerLimitsSpec,
    ) -> Result<Self, DurableLedgerError> {
        Self::open_storage(
            directory.as_ref(),
            StorageOptions::Segmented(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
            limits,
        )
    }

    #[allow(
        clippy::too_many_lines,
        reason = "one recovery audit keeps anchor selection, prefix proof, suffix application, and final invariant validation contiguous"
    )]
    fn open_storage(
        path: &Path,
        options: StorageOptions,
        checkpoint_source: Option<(&Path, SnapshotOptions)>,
        limits: LedgerLimitsSpec,
    ) -> Result<Self, DurableLedgerError> {
        let empty_ledger = Ledger::try_with_limits(limits)?;
        if let Some((checkpoint_path, _)) = checkpoint_source {
            validate_checkpoint_path(path, storage_layout(options), checkpoint_path)?;
        }
        let journal = StorageWriter::open(path, options)?;
        let journal_recovery = journal.recovery();
        let mut reader = StorageReader::open(path, options)?;
        let first_frame = reader.next().transpose()?;
        let (checkpoint, checkpoint_slot, mut next_frame, compacted) = if let Some(frame) =
            first_frame
        {
            if frame.kind() == RecordKind::CheckpointAnchor {
                let anchor: CheckpointAnchor = frame.decode()?;
                if anchor.kind() != SnapshotKind::LedgerCheckpoint {
                    return Err(DurableLedgerError::CheckpointAnchorKindMismatch {
                        actual: anchor.kind(),
                    });
                }
                let Some((checkpoint_base, snapshot_options)) = checkpoint_source else {
                    return Err(DurableLedgerError::CheckpointRequiredForCompactedWal {
                        sequence: frame.sequence(),
                    });
                };
                if frame.sequence() != anchor.wal_sequence() {
                    return Err(DurableLedgerError::CheckpointAnchorMismatch {
                        generation: anchor.generation(),
                        slot: anchor.slot(),
                    });
                }
                let slot_path = SnapshotFile::slot_path(checkpoint_base, anchor.slot());
                validate_checkpoint_path(path, storage_layout(options), &slot_path)?;
                let (checkpoint, receipt) = SnapshotFile::read_with_receipt::<LedgerCheckpoint>(
                    &slot_path,
                    snapshot_options,
                )?;
                if !anchor.matches_receipt(receipt)
                    || checkpoint.generation() != anchor.generation()
                {
                    return Err(DurableLedgerError::CheckpointAnchorMismatch {
                        generation: anchor.generation(),
                        slot: anchor.slot(),
                    });
                }
                (Some(checkpoint), Some(anchor.slot()), None, true)
            } else {
                let checkpoint = checkpoint_source
                    .map(|(checkpoint_path, snapshot_options)| {
                        SnapshotFile::read::<LedgerCheckpoint>(checkpoint_path, snapshot_options)
                    })
                    .transpose()?;
                (checkpoint, None, Some(frame), false)
            }
        } else {
            let checkpoint = checkpoint_source
                .map(|(checkpoint_path, snapshot_options)| {
                    SnapshotFile::read::<LedgerCheckpoint>(checkpoint_path, snapshot_options)
                })
                .transpose()?;
            (checkpoint, None, None, false)
        };
        let checkpointed_records = checkpoint.as_ref().map_or(0, LedgerCheckpoint::generation);
        let mut ledger = match checkpoint {
            Some(checkpoint) => empty_ledger.restore_checkpoint(&checkpoint)?,
            None => empty_ledger,
        };
        let mut replayed_records = 0_u64;
        let mut wal_records = if compacted { checkpointed_records } else { 0 };
        loop {
            let frame = if let Some(frame) = next_frame.take() {
                frame
            } else {
                let Some(frame) = reader.next().transpose()? else {
                    break;
                };
                frame
            };
            let record = decode_ledger_record(&frame)?;
            if wal_records < checkpointed_records {
                let checkpoint_sequence = wal_records
                    .checked_add(1)
                    .ok_or(LedgerError::ArithmeticOverflow)?;
                if ledger.record(checkpoint_sequence).as_ref() != Some(&record) {
                    return Err(DurableLedgerError::CheckpointPrefixDivergence {
                        wal_sequence: frame.sequence(),
                        checkpoint_record_index: wal_records,
                    });
                }
                wal_records = checkpoint_sequence;
                continue;
            }
            let transaction_id = record.primary_transaction_id();
            let replayed = match record {
                LedgerRecord::Entry(entry) => ledger.post(entry)?.replayed,
                LedgerRecord::Correction(correction) => ledger.correct(correction)?.replayed,
                LedgerRecord::Batch(batch) => ledger.post_batch(batch)?.replayed,
            };
            if replayed {
                return Err(DurableLedgerError::DuplicateTransactionRecord {
                    sequence: frame.sequence(),
                    transaction_id,
                });
            }
            replayed_records = replayed_records
                .checked_add(1)
                .ok_or(LedgerError::ArithmeticOverflow)?;
            wal_records = wal_records
                .checked_add(1)
                .ok_or(LedgerError::ArithmeticOverflow)?;
        }
        if wal_records < checkpointed_records {
            return Err(DurableLedgerError::CheckpointAheadOfWal {
                checkpoint_records: checkpointed_records,
                wal_records,
            });
        }
        ledger.validate()?;
        Ok(Self {
            ledger,
            journal,
            recovery: DurableLedgerRecoveryReport {
                journal: journal_recovery,
                checkpointed_records,
                replayed_records,
            },
            checkpoint_slot,
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

    /// Durably records and commits one indivisible reversal-plus-replacement event.
    ///
    /// The complete correction is encoded in one checksummed WAL frame. A torn
    /// final frame therefore contributes neither transaction during recovery;
    /// a complete verified frame contributes both.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for correction preparation, WAL, or commit
    /// failure. Ambiguous I/O or a post-append commit failure poisons the runtime.
    pub fn correct(
        &mut self,
        correction: LedgerCorrection,
    ) -> Result<CorrectionReceipt, DurableLedgerError> {
        if self.poisoned {
            return Err(DurableLedgerError::Poisoned);
        }
        let prepared = match self.ledger.prepare_correction(correction)? {
            CorrectionPreparation::Replay(receipt) => return Ok(receipt),
            CorrectionPreparation::Ready(prepared) => prepared,
        };
        if let Err(error) = self.journal.append(prepared.correction()) {
            self.poisoned = self.journal.is_poisoned();
            return Err(error.into());
        }
        match self.ledger.commit_correction(prepared) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.poisoned = true;
                Err(DurableLedgerError::Ledger(error))
            }
        }
    }

    /// Durably records and commits an ordered multi-entry event in one WAL frame.
    ///
    /// Exact retries return the original receipt without appending a frame. A
    /// torn final frame contributes no batch member during repair recovery.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for batch preparation, WAL, or commit
    /// failure. Ambiguous I/O or a post-append commit failure poisons the runtime.
    pub fn post_batch(&mut self, batch: LedgerBatch) -> Result<BatchReceipt, DurableLedgerError> {
        if self.poisoned {
            return Err(DurableLedgerError::Poisoned);
        }
        let prepared = match self.ledger.prepare_batch(batch)? {
            BatchPreparation::Replay(receipt) => return Ok(receipt),
            BatchPreparation::Ready(prepared) => prepared,
        };
        if let Err(error) = self.journal.append(prepared.batch()) {
            self.poisoned = self.journal.is_poisoned();
            return Err(error.into());
        }
        match self.ledger.commit_batch(prepared) {
            Ok(receipt) => Ok(receipt),
            Err(error) => {
                self.poisoned = true;
                Err(DurableLedgerError::Ledger(error))
            }
        }
    }

    /// Durably commits every DVP entry from one accepted call-auction uncross.
    ///
    /// Multi-trade settlements use one batch WAL frame and one indivisible
    /// ledger event. Exact retries return the original receipt without WAL
    /// growth.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for ledger preparation, WAL, or commit
    /// failure. Ambiguous I/O or post-append failure poisons the runtime.
    pub fn settle_call_auction(
        &mut self,
        settlement: CallAuctionSettlement,
    ) -> Result<CallAuctionSettlementReceipt, DurableLedgerError> {
        match settlement.into_record() {
            CallAuctionSettlementRecord::Entry(entry) => self.post(entry).map(Into::into),
            CallAuctionSettlementRecord::Batch(batch) => self.post_batch(batch).map(Into::into),
        }
    }

    /// Constructs, durably records, and commits the exact reversal of a prior entry.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] if the target is absent/already reversed,
    /// an inverse amount is unrepresentable, or durable posting fails.
    pub fn reverse(
        &mut self,
        transaction_id: TransactionId,
        reference: u64,
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        reversed_transaction_id: TransactionId,
    ) -> Result<PostReceipt, DurableLedgerError> {
        let original = self
            .ledger
            .transaction(reversed_transaction_id)
            .ok_or(LedgerError::ReversalTargetMissing(reversed_transaction_id))?;
        let reversal = JournalEntry::reversal(
            transaction_id,
            reference,
            effective_date,
            recorded_at,
            original,
        )?;
        self.post(reversal)
    }

    /// Durably advances the inclusive closed accounting-date boundary.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for invalid period progression, timestamp,
    /// WAL, or commit failure.
    pub fn close_period(
        &mut self,
        transaction_id: TransactionId,
        reference: u64,
        recorded_at: TimestampNs,
        closed_through: AccountingDate,
    ) -> Result<PostReceipt, DurableLedgerError> {
        let entry =
            JournalEntry::period_close(transaction_id, reference, recorded_at, closed_through)?;
        self.post(entry)
    }

    /// Durably moves or removes the inclusive closed accounting-date boundary.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for invalid period regression, timestamp,
    /// WAL, or commit failure.
    pub fn reopen_period(
        &mut self,
        transaction_id: TransactionId,
        reference: u64,
        recorded_at: TimestampNs,
        new_closed_through: Option<AccountingDate>,
    ) -> Result<PostReceipt, DurableLedgerError> {
        let entry = JournalEntry::period_reopen(
            transaction_id,
            reference,
            recorded_at,
            new_closed_through,
        )?;
        self.post(entry)
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
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        trade: &Trade,
        convention: SettlementConvention,
    ) -> Result<PostReceipt, DurableLedgerError> {
        let entry = JournalEntry::from_trade(
            transaction_id,
            effective_date,
            recorded_at,
            trade,
            convention,
        )?;
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
        effective_date: AccountingDate,
        recorded_at: TimestampNs,
        trade: &Trade,
        definition: InstrumentDefinition,
    ) -> Result<PostReceipt, DurableLedgerError> {
        let entry = JournalEntry::from_instrument(
            transaction_id,
            effective_date,
            recorded_at,
            trade,
            definition,
        )?;
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
        let checkpoint = match self.ledger.checkpoint() {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = checkpoint_capture_failure_requires_poison(&error);
                return Err(DurableLedgerError::CheckpointCapture(error));
            }
        };
        SnapshotFile::write(path, &checkpoint, options).map_err(Into::into)
    }

    /// Writes the inactive ledger checkpoint slot and atomically retires the
    /// selected WAL prefix behind an anchor that separately binds semantic
    /// record generation and physical WAL sequence.
    ///
    /// # Errors
    ///
    /// Returns [`DurableLedgerError`] for empty storage, poison, path conflict,
    /// ledger audit, snapshot persistence, or physical cutover failure.
    pub fn compact_to_checkpoint(
        &mut self,
        checkpoint_base: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<CheckpointCutoverReceipt, DurableLedgerError> {
        if self.poisoned {
            return Err(DurableLedgerError::Poisoned);
        }
        if self.ledger.record_count() == 0 {
            return Err(DurableLedgerError::CompactionRequiresNonEmptyLedger);
        }
        validate_checkpoint_path(
            self.journal.path(),
            self.journal.layout(),
            checkpoint_base.as_ref(),
        )?;
        self.sync_all()?;
        let wal_sequence = self
            .journal
            .next_sequence()
            .checked_sub(1)
            .ok_or(LedgerError::ArithmeticOverflow)?;
        let checkpoint = match self.ledger.checkpoint() {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = checkpoint_capture_failure_requires_poison(&error);
                return Err(DurableLedgerError::CheckpointCapture(error));
            }
        };
        let slot = self
            .checkpoint_slot
            .map_or(CheckpointSlot::A, CheckpointSlot::alternate);
        let slot_path = SnapshotFile::slot_path(checkpoint_base, slot);
        validate_checkpoint_path(self.journal.path(), self.journal.layout(), &slot_path)?;
        let snapshot = SnapshotFile::write(&slot_path, &checkpoint, options)?;
        let anchor =
            CheckpointAnchor::new(SnapshotKind::LedgerCheckpoint, slot, snapshot, wal_sequence);
        if let Err(error) = self.journal.replace_with_checkpoint_anchor(anchor) {
            self.poisoned = self.journal.is_poisoned();
            return Err(error.into());
        }
        self.checkpoint_slot = Some(slot);
        Ok(CheckpointCutoverReceipt::new(snapshot, slot, wal_sequence))
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

fn decode_ledger_record(frame: &JournalFrame) -> Result<LedgerRecord, DurableLedgerError> {
    match frame.kind() {
        RecordKind::LedgerEntry => Ok(LedgerRecord::Entry(frame.decode()?)),
        RecordKind::LedgerCorrection => Ok(LedgerRecord::Correction(frame.decode()?)),
        RecordKind::LedgerBatch => Ok(LedgerRecord::Batch(frame.decode()?)),
        kind => Err(DurableLedgerError::UnexpectedRecord {
            sequence: frame.sequence(),
            kind,
        }),
    }
}

const fn storage_layout(options: StorageOptions) -> JournalLayout {
    match options {
        StorageOptions::Single(_) => JournalLayout::SingleFile,
        StorageOptions::Segmented(_) => JournalLayout::Segmented,
    }
}

const fn checkpoint_capture_failure_requires_poison(error: &LedgerCheckpointCaptureError) -> bool {
    !error.is_operational_failure()
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
            let storage_cutover = Journal::cutover_pending_path(&storage_path)?;
            let storage_paths = [&storage_path, &storage_lease, &storage_cutover];
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

#[cfg(test)]
mod checkpoint_capture_failure_tests {
    use super::checkpoint_capture_failure_requires_poison;
    use crate::ledger::{
        LedgerCheckpointCaptureError, LedgerCheckpointCaptureResource, LedgerConstructionError,
        LedgerInvariantViolation, LedgerResource,
    };

    #[test]
    fn resource_pressure_is_retryable_but_semantic_failure_requires_poison() {
        let resource = LedgerCheckpointCaptureError::ResourceReservationFailed {
            resource: LedgerCheckpointCaptureResource::CaptureRecords,
            maximum: usize::MAX,
        };
        let construction = LedgerCheckpointCaptureError::Construction(
            LedgerConstructionError::ReservationFailed {
                resource: LedgerResource::Records,
                maximum: usize::MAX,
            },
        );
        assert!(!checkpoint_capture_failure_requires_poison(&resource));
        assert!(!checkpoint_capture_failure_requires_poison(&construction));
        assert!(checkpoint_capture_failure_requires_poison(
            &LedgerCheckpointCaptureError::Invalid(LedgerInvariantViolation::new(
                "semantic contradiction"
            ))
        ));
    }
}
