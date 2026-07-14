//! Crash-recoverable orchestration of one sequenced call-auction engine.
//!
//! The WAL grammar is one immutable instrument definition followed by strict
//! command/report pairs. A command is acknowledged before in-memory commit. If
//! termination occurs before its report is stored, recovery deterministically
//! completes that one final command and appends the reproduced report.

use std::fmt;
use std::path::{Path, PathBuf};

use crate::auction_engine::{
    CallAuctionCheckpoint, CallAuctionCheckpointError, CallAuctionCommand,
    CallAuctionCommandPreparation, CallAuctionEngine, CallAuctionEngineConstructionError,
    CallAuctionEngineError, CallAuctionEngineInvariantViolation, CallAuctionEngineLimits,
    CallAuctionExecutionReport,
};
use crate::auction_risk::{
    CallAuctionRiskCheckpoint, CallAuctionRiskCheckpointError, CallAuctionRiskConstructionError,
    CallAuctionRiskInvariantViolation, CallAuctionRiskLimits, CallAuctionRiskManagedEngine,
};
use crate::domain::{InstrumentId, InstrumentVersion};
use crate::durable_storage::{StorageError, StorageOptions, StorageReader, StorageWriter};
use crate::instrument::InstrumentDefinition;
use crate::journal::{
    Journal, JournalError, JournalFrame, JournalLayout, JournalOptions, RecordKind,
    SegmentedJournalError, SegmentedJournalOptions, StorageRecoveryReport, normalize_journal_path,
};
use crate::risk::{AccountRiskDefinition, RiskError};
use crate::snapshot::{
    CheckpointAnchor, CheckpointCutoverReceipt, CheckpointSlot, SnapshotError, SnapshotFile,
    SnapshotKind, SnapshotOptions, SnapshotReceipt,
};

/// Result of reconstructing one durable call-auction shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableCallAuctionRecoveryReport {
    /// Physical frame recovery performed by the selected journal layout.
    pub journal: StorageRecoveryReport,
    /// Completed commands restored directly from a verified semantic checkpoint.
    pub checkpointed_commands: u64,
    /// Complete command/report pairs reproduced and verified.
    pub replayed_commands: u64,
    /// Whether recovery completed one final command lacking its report.
    pub completed_dangling_command: bool,
}

/// Result of reconstructing one durable coupled call-auction/risk shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableCallAuctionRiskRecoveryReport {
    /// Physical frame recovery performed by the selected journal layout.
    pub journal: StorageRecoveryReport,
    /// Completed commands restored directly from a verified semantic checkpoint.
    pub checkpointed_commands: u64,
    /// Complete command/report pairs reproduced and verified.
    pub replayed_commands: u64,
    /// Missing immutable profile frames completed before command processing.
    pub completed_profile_records: u64,
    /// Whether recovery completed one final command lacking its report.
    pub completed_dangling_command: bool,
}

/// Durable call-auction orchestration or replay failure.
#[derive(Debug)]
pub enum DurableCallAuctionError {
    /// Single-file journal framing, codec, recovery, or I/O failure.
    Journal(JournalError),
    /// Segmented-journal inventory, framing, recovery, or I/O failure.
    SegmentedJournal(SegmentedJournalError),
    /// Engine construction or complete resource reservation failed.
    Construction(CallAuctionEngineConstructionError),
    /// Command preparation, deterministic replay, or engine commit failed.
    Engine(CallAuctionEngineError),
    /// Reconstructed controller or collection state violated an invariant.
    Invariant(CallAuctionEngineInvariantViolation),
    /// Semantic checkpoint construction or restoration failed.
    Checkpoint(CallAuctionCheckpointError),
    /// Coupled engine construction or complete resource reservation failed.
    RiskConstruction(CallAuctionRiskConstructionError),
    /// Immutable profile registration or capacity validation failed.
    Risk(RiskError),
    /// Reconstructed coupled auction/risk state violated an invariant.
    RiskInvariant(CallAuctionRiskInvariantViolation),
    /// Coupled auction/risk checkpoint construction or restoration failed.
    RiskCheckpoint(CallAuctionRiskCheckpointError),
    /// Snapshot framing, ownership, checksum, generation, or I/O failed.
    Snapshot(SnapshotError),
    /// The first frame was not an immutable instrument definition.
    DefinitionRecordRequired {
        /// Journal frame sequence.
        sequence: u64,
        /// Payload kind found instead.
        actual: RecordKind,
    },
    /// The journal remained empty after initialization.
    DefinitionRecordMissing,
    /// The journal is bound to another immutable definition.
    DefinitionMismatch {
        /// Requested instrument.
        requested_instrument_id: InstrumentId,
        /// Requested version.
        requested_version: InstrumentVersion,
        /// Persisted instrument.
        persisted_instrument_id: InstrumentId,
        /// Persisted version.
        persisted_version: InstrumentVersion,
    },
    /// A checkpoint's metadata origin differs from the journal lineage.
    CheckpointWalLineageMismatch {
        /// Definition/metadata origin retained by the checkpoint.
        checkpoint_first_sequence: u64,
        /// First physical sequence in the uncut WAL.
        wal_first_sequence: u64,
    },
    /// A checkpoint definition differs from the requested/WAL definition.
    CheckpointDefinitionMismatch,
    /// A checkpoint's profile-metadata boundary differs from the WAL prefix.
    CheckpointMetadataBoundaryMismatch {
        /// Boundary retained by the checkpoint.
        checkpoint_metadata_sequence: u64,
        /// Boundary derived from definition plus requested profiles.
        wal_metadata_sequence: u64,
    },
    /// A checkpoint's immutable profile set differs from the requested set.
    CheckpointRiskProfileSetMismatch,
    /// Persisted immutable profile metadata differs from the requested set.
    RiskProfileSetMismatch {
        /// Canonical requested profile count.
        requested_count: usize,
        /// Persisted profile-prefix count.
        persisted_count: usize,
    },
    /// Complete canonical profile-vector reservation failed before WAL access.
    RiskProfileCanonicalizationFailed,
    /// A checkpoint command or report differs from its uncut WAL frame.
    CheckpointPrefixDivergence {
        /// Physical WAL frame sequence where values differed.
        wal_sequence: u64,
    },
    /// A checkpoint boundary is newer than the complete verified WAL.
    CheckpointAheadOfWal {
        /// Completed report boundary claimed by the checkpoint.
        checkpoint_sequence: u64,
        /// Last complete physical frame in the WAL.
        wal_last_sequence: Option<u64>,
    },
    /// A compacted WAL requires its anchor-selected checkpoint base path.
    CheckpointRequiredForCompactedWal {
        /// Physical sequence of the checkpoint anchor.
        sequence: u64,
    },
    /// A compacted WAL anchor names another semantic checkpoint type.
    CheckpointAnchorKindMismatch {
        /// Kind stored in the anchor.
        actual: SnapshotKind,
    },
    /// The selected checkpoint bytes differ from the WAL anchor identity.
    CheckpointAnchorMismatch {
        /// Anchored semantic generation.
        generation: u64,
        /// Anchored alternating slot.
        slot: CheckpointSlot,
    },
    /// Snapshot output aliases WAL storage or its ownership namespace.
    CheckpointPathConflictsWithWal {
        /// Conflicting canonical path.
        path: PathBuf,
    },
    /// A frame violated the auction-journal grammar.
    UnexpectedRecord {
        /// Journal frame sequence.
        sequence: u64,
        /// Unexpected payload kind.
        kind: RecordKind,
    },
    /// A second command appeared before the prior command's report.
    ConsecutiveCommands {
        /// Sequence of the uncompleted command.
        pending_sequence: u64,
        /// Sequence of the next command.
        next_sequence: u64,
    },
    /// An auction report had no preceding command.
    ReportWithoutCommand {
        /// Journal frame sequence.
        sequence: u64,
    },
    /// Deterministic replay did not reproduce the persisted report.
    ReplayDivergence {
        /// Sequence of the replayed command frame.
        command_sequence: u64,
        /// Sequence of the persisted report frame.
        report_sequence: u64,
    },
    /// A retry was persisted despite the canonical no-new-frame contract.
    PersistedRetry {
        /// Duplicate command frame sequence.
        command_sequence: u64,
        /// Replay report frame sequence.
        report_sequence: u64,
    },
    /// A final dangling command was an exact retry of completed history.
    PersistedDanglingRetry {
        /// Duplicate command frame sequence.
        command_sequence: u64,
    },
    /// The runtime is unusable until reopened after an ambiguous transition.
    Poisoned,
}

impl fmt::Display for DurableCallAuctionError {
    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive match keeps every durable grammar and recovery diagnostic explicit"
    )]
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(error) => error.fmt(formatter),
            Self::SegmentedJournal(error) => error.fmt(formatter),
            Self::Construction(error) => error.fmt(formatter),
            Self::Engine(error) => error.fmt(formatter),
            Self::Invariant(error) => error.fmt(formatter),
            Self::Checkpoint(error) => error.fmt(formatter),
            Self::RiskConstruction(error) => error.fmt(formatter),
            Self::Risk(error) => error.fmt(formatter),
            Self::RiskInvariant(error) => error.fmt(formatter),
            Self::RiskCheckpoint(error) => error.fmt(formatter),
            Self::Snapshot(error) => error.fmt(formatter),
            Self::DefinitionRecordRequired { sequence, actual } => write!(
                formatter,
                "first call-auction journal frame at sequence {sequence} must be an instrument definition, found {actual:?}"
            ),
            Self::DefinitionRecordMissing => {
                formatter.write_str("call-auction journal has no instrument-definition frame")
            }
            Self::DefinitionMismatch {
                requested_instrument_id,
                requested_version,
                persisted_instrument_id,
                persisted_version,
            } => write!(
                formatter,
                "requested call-auction definition {requested_instrument_id}@{requested_version} does not match persisted definition {persisted_instrument_id}@{persisted_version}"
            ),
            Self::CheckpointWalLineageMismatch {
                checkpoint_first_sequence,
                wal_first_sequence,
            } => write!(
                formatter,
                "auction checkpoint WAL starts at {checkpoint_first_sequence}, but journal starts at {wal_first_sequence}"
            ),
            Self::CheckpointDefinitionMismatch => formatter
                .write_str("auction checkpoint instrument definition differs from the WAL binding"),
            Self::CheckpointMetadataBoundaryMismatch {
                checkpoint_metadata_sequence,
                wal_metadata_sequence,
            } => write!(
                formatter,
                "auction risk checkpoint metadata boundary {checkpoint_metadata_sequence} differs from WAL boundary {wal_metadata_sequence}"
            ),
            Self::CheckpointRiskProfileSetMismatch => formatter
                .write_str("auction risk checkpoint profile set differs from requested metadata"),
            Self::RiskProfileSetMismatch {
                requested_count,
                persisted_count,
            } => write!(
                formatter,
                "requested {requested_count} auction risk profiles but persisted metadata has {persisted_count}"
            ),
            Self::RiskProfileCanonicalizationFailed => {
                formatter.write_str("could not reserve canonical auction risk profile metadata")
            }
            Self::CheckpointPrefixDivergence { wal_sequence } => write!(
                formatter,
                "auction checkpoint differs from WAL frame {wal_sequence}"
            ),
            Self::CheckpointAheadOfWal {
                checkpoint_sequence,
                wal_last_sequence,
            } => write!(
                formatter,
                "auction checkpoint covers WAL sequence {checkpoint_sequence}, but verified WAL ends at {wal_last_sequence:?}"
            ),
            Self::CheckpointRequiredForCompactedWal { sequence } => write!(
                formatter,
                "auction WAL begins with checkpoint anchor {sequence}; a checkpoint base path is required"
            ),
            Self::CheckpointAnchorKindMismatch { actual } => write!(
                formatter,
                "auction WAL anchor references incompatible snapshot kind {actual:?}"
            ),
            Self::CheckpointAnchorMismatch { generation, slot } => write!(
                formatter,
                "auction WAL anchor at generation {generation} does not match checkpoint slot {slot:?}"
            ),
            Self::CheckpointPathConflictsWithWal { path } => write!(
                formatter,
                "auction checkpoint path {} conflicts with WAL storage",
                path.display()
            ),
            Self::UnexpectedRecord { sequence, kind } => write!(
                formatter,
                "unexpected {kind:?} record at call-auction journal sequence {sequence}"
            ),
            Self::ConsecutiveCommands {
                pending_sequence,
                next_sequence,
            } => write!(
                formatter,
                "call-auction command at sequence {pending_sequence} has no report before command at {next_sequence}"
            ),
            Self::ReportWithoutCommand { sequence } => write!(
                formatter,
                "call-auction report at sequence {sequence} has no preceding command"
            ),
            Self::ReplayDivergence {
                command_sequence,
                report_sequence,
            } => write!(
                formatter,
                "replay of call-auction command at {command_sequence} diverged from report at {report_sequence}"
            ),
            Self::PersistedRetry {
                command_sequence,
                report_sequence,
            } => write!(
                formatter,
                "call-auction retry at {command_sequence}/{report_sequence} was persisted noncanonically"
            ),
            Self::PersistedDanglingRetry { command_sequence } => write!(
                formatter,
                "dangling call-auction retry at {command_sequence} was persisted noncanonically"
            ),
            Self::Poisoned => formatter.write_str(
                "durable call-auction shard is poisoned and must be reopened for recovery",
            ),
        }
    }
}

impl std::error::Error for DurableCallAuctionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(error) => Some(error),
            Self::SegmentedJournal(error) => Some(error),
            Self::Construction(error) => Some(error),
            Self::Engine(error) => Some(error),
            Self::Invariant(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            Self::RiskConstruction(error) => Some(error),
            Self::Risk(error) => Some(error),
            Self::RiskInvariant(error) => Some(error),
            Self::RiskCheckpoint(error) => Some(error),
            Self::Snapshot(error) => Some(error),
            _ => None,
        }
    }
}

impl From<JournalError> for DurableCallAuctionError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

impl From<SegmentedJournalError> for DurableCallAuctionError {
    fn from(error: SegmentedJournalError) -> Self {
        Self::SegmentedJournal(error)
    }
}

impl From<StorageError> for DurableCallAuctionError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Journal(error) => Self::Journal(error),
            StorageError::Segmented(error) => Self::SegmentedJournal(error),
        }
    }
}

impl From<CallAuctionEngineConstructionError> for DurableCallAuctionError {
    fn from(error: CallAuctionEngineConstructionError) -> Self {
        Self::Construction(error)
    }
}

impl From<CallAuctionEngineError> for DurableCallAuctionError {
    fn from(error: CallAuctionEngineError) -> Self {
        Self::Engine(error)
    }
}

impl From<CallAuctionCheckpointError> for DurableCallAuctionError {
    fn from(error: CallAuctionCheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

impl From<CallAuctionRiskConstructionError> for DurableCallAuctionError {
    fn from(error: CallAuctionRiskConstructionError) -> Self {
        Self::RiskConstruction(error)
    }
}

impl From<RiskError> for DurableCallAuctionError {
    fn from(error: RiskError) -> Self {
        Self::Risk(error)
    }
}

impl From<CallAuctionRiskCheckpointError> for DurableCallAuctionError {
    fn from(error: CallAuctionRiskCheckpointError) -> Self {
        Self::RiskCheckpoint(error)
    }
}

impl From<CallAuctionRiskInvariantViolation> for DurableCallAuctionError {
    fn from(error: CallAuctionRiskInvariantViolation) -> Self {
        Self::RiskInvariant(error)
    }
}

impl From<SnapshotError> for DurableCallAuctionError {
    fn from(error: SnapshotError) -> Self {
        Self::Snapshot(error)
    }
}

/// One crash-recoverable, single-writer call-auction shard.
#[derive(Debug)]
pub struct DurableCallAuctionEngine {
    engine: CallAuctionEngine,
    journal: StorageWriter,
    wal_metadata_sequence: u64,
    checkpoint_slot: Option<CheckpointSlot>,
    recovery: DurableCallAuctionRecoveryReport,
    poisoned: bool,
}

impl DurableCallAuctionEngine {
    /// Opens or creates a single-file auction WAL under default engine limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for storage, definition, replay,
    /// deterministic-divergence, capacity, or invariant failure.
    pub fn open(
        path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        options: JournalOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            CallAuctionEngineLimits::default(),
            StorageOptions::Single(options),
            None,
        )
    }

    /// Opens a single-file auction WAL under explicit finite engine limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] when recovered state exceeds the
    /// selected limits or any storage/replay invariant fails.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        limits: CallAuctionEngineLimits,
        options: JournalOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            limits,
            StorageOptions::Single(options),
            None,
        )
    }

    /// Opens a single-file auction WAL from a verified semantic checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for snapshot, path, definition,
    /// prefix, storage, replay, or reconstructed-state failure.
    pub fn open_with_checkpoint(
        path: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        options: JournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            CallAuctionEngineLimits::default(),
            StorageOptions::Single(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens a single-file auction WAL and checkpoint under explicit limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] when the checkpoint or suffix does
    /// not fit or any storage/reconstruction invariant fails.
    pub fn open_with_checkpoint_and_limits(
        path: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        limits: CallAuctionEngineLimits,
        options: JournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            limits,
            StorageOptions::Single(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens or creates an automatically rotating segmented auction WAL.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for inventory, framing, definition,
    /// replay, capacity, or invariant failure.
    pub fn open_segmented(
        directory: impl AsRef<Path>,
        definition: InstrumentDefinition,
        options: SegmentedJournalOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            CallAuctionEngineLimits::default(),
            StorageOptions::Segmented(options),
            None,
        )
    }

    /// Opens a segmented auction WAL under explicit finite engine limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] when storage/replay is invalid or
    /// recovered state exceeds the selected limits.
    pub fn open_segmented_with_limits(
        directory: impl AsRef<Path>,
        definition: InstrumentDefinition,
        limits: CallAuctionEngineLimits,
        options: SegmentedJournalOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            limits,
            StorageOptions::Segmented(options),
            None,
        )
    }

    /// Opens segmented auction storage from an exact semantic checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for snapshot, path, anchor/prefix,
    /// segmented storage, replay, or reconstructed-state failure.
    pub fn open_segmented_with_checkpoint(
        directory: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        options: SegmentedJournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            CallAuctionEngineLimits::default(),
            StorageOptions::Segmented(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens segmented auction storage and a checkpoint under explicit limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for capacity, snapshot, storage,
    /// replay, or reconstructed invariant failure.
    pub fn open_segmented_with_checkpoint_and_limits(
        directory: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        limits: CallAuctionEngineLimits,
        options: SegmentedJournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            limits,
            StorageOptions::Segmented(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    fn open_storage(
        path: &Path,
        definition: InstrumentDefinition,
        limits: CallAuctionEngineLimits,
        options: StorageOptions,
        checkpoint_source: Option<(&Path, SnapshotOptions)>,
    ) -> Result<Self, DurableCallAuctionError> {
        let opened = open_auction_journal(path, definition, options, checkpoint_source)?;
        let mut journal = opened.journal;
        let replay = replay_auction_suffix(
            opened.reader,
            opened.checkpoint,
            definition,
            limits,
            opened.wal_first_sequence,
        )?;
        let AuctionReplay {
            mut engine,
            pending,
            checkpointed_commands,
            replayed_commands,
        } = replay;
        let completed_dangling_command = if let Some((command_sequence, command)) = pending {
            let report = engine.submit(command)?;
            if report.replayed {
                return Err(DurableCallAuctionError::PersistedDanglingRetry { command_sequence });
            }
            journal.append(&report)?;
            true
        } else {
            false
        };
        engine
            .validate()
            .map_err(DurableCallAuctionError::Invariant)?;
        Ok(Self {
            engine,
            journal,
            wal_metadata_sequence: opened.wal_metadata_sequence,
            checkpoint_slot: opened.checkpoint_slot,
            recovery: DurableCallAuctionRecoveryReport {
                journal: opened.recovery,
                checkpointed_commands,
                replayed_commands,
                completed_dangling_command,
            },
            poisoned: false,
        })
    }

    /// Returns recovery statistics captured at open.
    #[must_use]
    pub const fn recovery(&self) -> DurableCallAuctionRecoveryReport {
        self.recovery
    }

    /// Returns the reconstructed process-local engine.
    #[must_use]
    pub const fn engine(&self) -> &CallAuctionEngine {
        &self.engine
    }

    /// Durably sequences one command and records its deterministic report.
    ///
    /// Exact retries return shared cached history without adding WAL frames.
    /// Operational failures and command collisions precede command persistence.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for preparation, persistence, or
    /// commit failure. Any failure after command acknowledgement poisons this
    /// instance until deterministic reopen.
    pub fn submit(
        &mut self,
        command: CallAuctionCommand,
    ) -> Result<CallAuctionExecutionReport, DurableCallAuctionError> {
        if self.poisoned {
            return Err(DurableCallAuctionError::Poisoned);
        }
        let prepared = match self.engine.prepare(command)? {
            CallAuctionCommandPreparation::Replay(report) => return Ok(report),
            CallAuctionCommandPreparation::Ready(prepared) => prepared,
        };
        if let Err(error) = self.journal.append(&prepared.command()) {
            self.poisoned = self.journal.is_poisoned();
            return Err(error.into());
        }
        let report = match self.engine.commit(prepared) {
            Ok(report) => report,
            Err(error) => {
                self.poisoned = true;
                return Err(DurableCallAuctionError::Engine(error));
            }
        };
        if let Err(error) = self.journal.append(&report) {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(report)
    }

    /// Writes a synchronized semantic checkpoint without changing WAL layout.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for poison, path conflict, capture,
    /// encoding, persistence, or synchronization failure.
    pub fn write_checkpoint(
        &mut self,
        path: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<SnapshotReceipt, DurableCallAuctionError> {
        if self.poisoned {
            return Err(DurableCallAuctionError::Poisoned);
        }
        validate_auction_checkpoint_path(
            self.journal.path(),
            self.journal.layout(),
            path.as_ref(),
        )?;
        self.sync_all()?;
        let wal_sequence = self
            .journal
            .next_sequence()
            .checked_sub(1)
            .ok_or(CallAuctionEngineError::SequenceExhausted)?;
        let checkpoint = match self
            .engine
            .checkpoint(self.wal_metadata_sequence, wal_sequence)
        {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = true;
                return Err(DurableCallAuctionError::Checkpoint(error));
            }
        };
        SnapshotFile::write(path, &checkpoint, options).map_err(Into::into)
    }

    /// Publishes an inactive A/B auction checkpoint and atomically retires the
    /// selected physical WAL prefix behind an exact checkpoint anchor.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for poison, path conflict, capture,
    /// snapshot persistence, or layout-specific cutover failure.
    pub fn compact_to_checkpoint(
        &mut self,
        checkpoint_base: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<CheckpointCutoverReceipt, DurableCallAuctionError> {
        if self.poisoned {
            return Err(DurableCallAuctionError::Poisoned);
        }
        validate_auction_checkpoint_path(
            self.journal.path(),
            self.journal.layout(),
            checkpoint_base.as_ref(),
        )?;
        self.sync_all()?;
        let wal_sequence = self
            .journal
            .next_sequence()
            .checked_sub(1)
            .ok_or(CallAuctionEngineError::SequenceExhausted)?;
        let checkpoint = match self
            .engine
            .checkpoint(self.wal_metadata_sequence, wal_sequence)
        {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = true;
                return Err(DurableCallAuctionError::Checkpoint(error));
            }
        };
        let slot = self
            .checkpoint_slot
            .map_or(CheckpointSlot::A, CheckpointSlot::alternate);
        let slot_path = SnapshotFile::slot_path(checkpoint_base, slot);
        validate_auction_checkpoint_path(self.journal.path(), self.journal.layout(), &slot_path)?;
        let snapshot = SnapshotFile::write(&slot_path, &checkpoint, options)?;
        let anchor = CheckpointAnchor::new(
            SnapshotKind::CallAuctionCheckpoint,
            slot,
            snapshot,
            wal_sequence,
        );
        if let Err(error) = self.journal.replace_with_checkpoint_anchor(anchor) {
            self.poisoned = self.journal.is_poisoned();
            return Err(error.into());
        }
        self.checkpoint_slot = Some(slot);
        Ok(CheckpointCutoverReceipt::new(snapshot, slot, wal_sequence))
    }

    /// Synchronizes journal data and metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] and poisons the shard when the
    /// synchronization outcome is ambiguous.
    pub fn sync_all(&mut self) -> Result<(), DurableCallAuctionError> {
        if self.poisoned {
            return Err(DurableCallAuctionError::Poisoned);
        }
        if let Err(error) = self.journal.sync_all() {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(())
    }

    /// Synchronizes storage and releases exclusive writer ownership.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for poison, synchronization, or
    /// lease-release failure.
    pub fn close(self) -> Result<(), DurableCallAuctionError> {
        if self.poisoned {
            return Err(DurableCallAuctionError::Poisoned);
        }
        self.journal.close().map_err(Into::into)
    }
}

/// One crash-recoverable, single-writer call-auction shard coupled to risk.
#[derive(Debug)]
pub struct DurableCallAuctionRiskEngine {
    managed: CallAuctionRiskManagedEngine,
    journal: StorageWriter,
    wal_first_sequence: u64,
    wal_metadata_sequence: u64,
    checkpoint_slot: Option<CheckpointSlot>,
    recovery: DurableCallAuctionRiskRecoveryReport,
    poisoned: bool,
}

impl DurableCallAuctionRiskEngine {
    /// Opens or creates a single-file profile-prefixed auction-risk WAL.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for profile, storage, replay,
    /// deterministic-divergence, capacity, or invariant failure.
    pub fn open(
        path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        options: JournalOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            profiles,
            CallAuctionRiskLimits::default(),
            StorageOptions::Single(options),
            None,
        )
    }

    /// Opens a single-file auction-risk WAL under explicit finite limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] when metadata or recovered state
    /// exceeds selected capacity or any storage/replay invariant fails.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        limits: CallAuctionRiskLimits,
        options: JournalOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            profiles,
            limits,
            StorageOptions::Single(options),
            None,
        )
    }

    /// Opens a single-file auction-risk WAL from a verified checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for snapshot, metadata, prefix,
    /// storage, replay, or reconstructed-state failure.
    pub fn open_with_checkpoint(
        path: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        options: JournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            profiles,
            CallAuctionRiskLimits::default(),
            StorageOptions::Single(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens a single-file auction-risk WAL and checkpoint under explicit limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] when recovered state exceeds limits
    /// or metadata, snapshot, storage, and replay validation fails.
    pub fn open_with_checkpoint_and_limits(
        path: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        limits: CallAuctionRiskLimits,
        options: JournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            profiles,
            limits,
            StorageOptions::Single(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens or creates a segmented profile-prefixed auction-risk WAL.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for inventory, metadata, framing,
    /// replay, capacity, or invariant failure.
    pub fn open_segmented(
        directory: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        options: SegmentedJournalOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            profiles,
            CallAuctionRiskLimits::default(),
            StorageOptions::Segmented(options),
            None,
        )
    }

    /// Opens a segmented auction-risk WAL under explicit finite limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] when recovered state exceeds limits
    /// or storage, metadata, and replay validation fails.
    pub fn open_segmented_with_limits(
        directory: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        limits: CallAuctionRiskLimits,
        options: SegmentedJournalOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            profiles,
            limits,
            StorageOptions::Segmented(options),
            None,
        )
    }

    /// Opens segmented auction-risk storage from a verified checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for snapshot, metadata, prefix,
    /// segmented storage, replay, or reconstructed-state failure.
    pub fn open_segmented_with_checkpoint(
        directory: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        options: SegmentedJournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            profiles,
            CallAuctionRiskLimits::default(),
            StorageOptions::Segmented(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens segmented auction-risk storage and checkpoint under explicit limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for capacity, snapshot, metadata,
    /// storage, replay, or reconstructed invariant failure.
    pub fn open_segmented_with_checkpoint_and_limits(
        directory: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        limits: CallAuctionRiskLimits,
        options: SegmentedJournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableCallAuctionError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            profiles,
            limits,
            StorageOptions::Segmented(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    fn open_storage(
        path: &Path,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        limits: CallAuctionRiskLimits,
        options: StorageOptions,
        checkpoint_source: Option<(&Path, SnapshotOptions)>,
    ) -> Result<Self, DurableCallAuctionError> {
        if profiles.len() > limits.max_registered_accounts() {
            return Err(DurableCallAuctionError::Risk(
                RiskError::ProfileCapacityExhausted {
                    maximum: limits.max_registered_accounts(),
                },
            ));
        }
        let profiles = canonical_auction_risk_profiles(profiles)?;
        if let Some((checkpoint_path, _)) = checkpoint_source {
            validate_auction_checkpoint_path(path, storage_layout(options), checkpoint_path)?;
        }
        let opened =
            open_auction_risk_journal(path, definition, &profiles, options, checkpoint_source)?;
        let mut journal = opened.journal;
        let replay = replay_auction_risk_suffix(
            opened.reader,
            opened.first_data_frame,
            opened.reader_exhausted,
            definition,
            &profiles,
            opened.checkpoint,
            limits,
            opened.replay_start_sequence,
        )?;
        let mut managed = replay.managed;
        let completed_dangling_command = if let Some((command_sequence, command)) = replay.pending {
            let report = managed.submit(command)?;
            if report.replayed {
                return Err(DurableCallAuctionError::PersistedDanglingRetry { command_sequence });
            }
            journal.append(&report)?;
            true
        } else {
            false
        };
        managed
            .validate()
            .map_err(DurableCallAuctionError::RiskInvariant)?;
        Ok(Self {
            managed,
            journal,
            wal_first_sequence: opened.wal_first_sequence,
            wal_metadata_sequence: opened.wal_metadata_sequence,
            checkpoint_slot: opened.checkpoint_slot,
            recovery: DurableCallAuctionRiskRecoveryReport {
                journal: opened.recovery,
                checkpointed_commands: replay.checkpointed_commands,
                replayed_commands: replay.replayed_commands,
                completed_profile_records: opened.completed_profile_records,
                completed_dangling_command,
            },
            poisoned: false,
        })
    }

    /// Returns recovery statistics captured at open.
    #[must_use]
    pub const fn recovery(&self) -> DurableCallAuctionRiskRecoveryReport {
        self.recovery
    }

    /// Returns reconstructed coupled auction and risk state.
    #[must_use]
    pub const fn managed(&self) -> &CallAuctionRiskManagedEngine {
        &self.managed
    }

    /// Persists a command before applying its coupled auction/risk transition.
    ///
    /// Exact retries return retained history without adding WAL frames.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for preparation, storage, or commit
    /// failure. Failure after command acknowledgement poisons this instance.
    pub fn submit(
        &mut self,
        command: CallAuctionCommand,
    ) -> Result<CallAuctionExecutionReport, DurableCallAuctionError> {
        if self.poisoned {
            return Err(DurableCallAuctionError::Poisoned);
        }
        let prepared = match self.managed.prepare(command)? {
            CallAuctionCommandPreparation::Replay(report) => return Ok(report),
            CallAuctionCommandPreparation::Ready(prepared) => prepared,
        };
        if let Err(error) = self.journal.append(&prepared.command()) {
            self.poisoned = self.journal.is_poisoned();
            return Err(error.into());
        }
        let report = match self.managed.commit(prepared) {
            Ok(report) => report,
            Err(error) => {
                self.poisoned = true;
                return Err(DurableCallAuctionError::Engine(error));
            }
        };
        if let Err(error) = self.journal.append(&report) {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(report)
    }

    /// Synchronizes the WAL, audits coupled state, and writes a checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for poison, path conflict, barrier,
    /// checkpoint audit/encoding, ownership, or I/O failure.
    pub fn write_checkpoint(
        &mut self,
        path: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<SnapshotReceipt, DurableCallAuctionError> {
        if self.poisoned {
            return Err(DurableCallAuctionError::Poisoned);
        }
        validate_auction_checkpoint_path(
            self.journal.path(),
            self.journal.layout(),
            path.as_ref(),
        )?;
        self.sync_all()?;
        let wal_sequence = self
            .journal
            .next_sequence()
            .checked_sub(1)
            .ok_or(CallAuctionEngineError::SequenceExhausted)?;
        let checkpoint = match self.managed.checkpoint(
            self.wal_first_sequence,
            self.wal_metadata_sequence,
            wal_sequence,
        ) {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = true;
                return Err(DurableCallAuctionError::RiskCheckpoint(error));
            }
        };
        SnapshotFile::write(path, &checkpoint, options).map_err(Into::into)
    }

    /// Publishes an inactive A/B checkpoint and retires the selected WAL prefix.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for poison, path conflict, capture,
    /// snapshot persistence, or layout-specific cutover failure.
    pub fn compact_to_checkpoint(
        &mut self,
        checkpoint_base: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<CheckpointCutoverReceipt, DurableCallAuctionError> {
        if self.poisoned {
            return Err(DurableCallAuctionError::Poisoned);
        }
        validate_auction_checkpoint_path(
            self.journal.path(),
            self.journal.layout(),
            checkpoint_base.as_ref(),
        )?;
        self.sync_all()?;
        let wal_sequence = self
            .journal
            .next_sequence()
            .checked_sub(1)
            .ok_or(CallAuctionEngineError::SequenceExhausted)?;
        let checkpoint = match self.managed.checkpoint(
            self.wal_first_sequence,
            self.wal_metadata_sequence,
            wal_sequence,
        ) {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = true;
                return Err(DurableCallAuctionError::RiskCheckpoint(error));
            }
        };
        let slot = self
            .checkpoint_slot
            .map_or(CheckpointSlot::A, CheckpointSlot::alternate);
        let slot_path = SnapshotFile::slot_path(checkpoint_base.as_ref(), slot);
        validate_auction_checkpoint_path(self.journal.path(), self.journal.layout(), &slot_path)?;
        let snapshot = SnapshotFile::write(&slot_path, &checkpoint, options)?;
        let anchor = CheckpointAnchor::new(
            SnapshotKind::CallAuctionRiskCheckpoint,
            slot,
            snapshot,
            wal_sequence,
        );
        if let Err(error) = self.journal.replace_with_checkpoint_anchor(anchor) {
            self.poisoned = self.journal.is_poisoned();
            return Err(error.into());
        }
        self.checkpoint_slot = Some(slot);
        Ok(CheckpointCutoverReceipt::new(snapshot, slot, wal_sequence))
    }

    /// Synchronizes journal data and metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] and poisons the shard when the
    /// synchronization outcome is ambiguous.
    pub fn sync_all(&mut self) -> Result<(), DurableCallAuctionError> {
        if self.poisoned {
            return Err(DurableCallAuctionError::Poisoned);
        }
        if let Err(error) = self.journal.sync_all() {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(())
    }

    /// Synchronizes storage and releases exclusive writer ownership.
    ///
    /// # Errors
    ///
    /// Returns [`DurableCallAuctionError`] for poison, synchronization, or
    /// lease-release failure.
    pub fn close(self) -> Result<(), DurableCallAuctionError> {
        if self.poisoned {
            return Err(DurableCallAuctionError::Poisoned);
        }
        self.journal.close().map_err(Into::into)
    }
}

struct OpenedAuctionJournal {
    journal: StorageWriter,
    reader: StorageReader,
    recovery: StorageRecoveryReport,
    checkpoint: Option<CallAuctionCheckpoint>,
    wal_first_sequence: u64,
    wal_metadata_sequence: u64,
    checkpoint_slot: Option<CheckpointSlot>,
}

fn open_auction_journal(
    path: &Path,
    definition: InstrumentDefinition,
    options: StorageOptions,
    checkpoint_source: Option<(&Path, SnapshotOptions)>,
) -> Result<OpenedAuctionJournal, DurableCallAuctionError> {
    if let Some((checkpoint_path, _)) = checkpoint_source {
        validate_auction_checkpoint_path(path, storage_layout(options), checkpoint_path)?;
    }
    let mut journal = StorageWriter::open(path, options)?;
    let recovery = journal.recovery();
    if recovery.last_sequence.is_none() {
        journal.append(&definition)?;
    }
    let mut reader = StorageReader::open(path, options)?;
    let Some(first_frame) = reader.next().transpose()? else {
        return Err(DurableCallAuctionError::DefinitionRecordMissing);
    };
    let (checkpoint, wal_metadata_sequence, checkpoint_slot) = match first_frame.kind() {
        RecordKind::InstrumentDefinition => {
            validate_auction_definition_frame(&first_frame, definition)?;
            let checkpoint = checkpoint_source
                .map(|(checkpoint_path, snapshot_options)| {
                    SnapshotFile::read::<CallAuctionCheckpoint>(checkpoint_path, snapshot_options)
                })
                .transpose()?;
            if let Some(value) = &checkpoint {
                if value.wal_metadata_sequence() != first_frame.sequence() {
                    return Err(DurableCallAuctionError::CheckpointWalLineageMismatch {
                        checkpoint_first_sequence: value.wal_metadata_sequence(),
                        wal_first_sequence: first_frame.sequence(),
                    });
                }
                if value.definition() != definition {
                    return Err(DurableCallAuctionError::CheckpointDefinitionMismatch);
                }
            }
            (checkpoint, first_frame.sequence(), None)
        }
        RecordKind::CheckpointAnchor => {
            let anchor: CheckpointAnchor = first_frame.decode()?;
            if anchor.kind() != SnapshotKind::CallAuctionCheckpoint {
                return Err(DurableCallAuctionError::CheckpointAnchorKindMismatch {
                    actual: anchor.kind(),
                });
            }
            let Some((checkpoint_base, snapshot_options)) = checkpoint_source else {
                return Err(DurableCallAuctionError::CheckpointRequiredForCompactedWal {
                    sequence: first_frame.sequence(),
                });
            };
            if first_frame.sequence() != anchor.wal_sequence() {
                return Err(DurableCallAuctionError::CheckpointAnchorMismatch {
                    generation: anchor.generation(),
                    slot: anchor.slot(),
                });
            }
            let slot_path = SnapshotFile::slot_path(checkpoint_base, anchor.slot());
            validate_auction_checkpoint_path(path, storage_layout(options), &slot_path)?;
            let (checkpoint, receipt) = SnapshotFile::read_with_receipt::<CallAuctionCheckpoint>(
                &slot_path,
                snapshot_options,
            )?;
            if !anchor.matches_receipt(receipt)
                || checkpoint.generation() != anchor.generation()
                || checkpoint.definition() != definition
            {
                return Err(DurableCallAuctionError::CheckpointAnchorMismatch {
                    generation: anchor.generation(),
                    slot: anchor.slot(),
                });
            }
            let wal_metadata_sequence = checkpoint.wal_metadata_sequence();
            (Some(checkpoint), wal_metadata_sequence, Some(anchor.slot()))
        }
        actual => {
            return Err(DurableCallAuctionError::DefinitionRecordRequired {
                sequence: first_frame.sequence(),
                actual,
            });
        }
    };
    Ok(OpenedAuctionJournal {
        journal,
        reader,
        recovery,
        checkpoint,
        wal_first_sequence: first_frame.sequence(),
        wal_metadata_sequence,
        checkpoint_slot,
    })
}

fn validate_auction_definition_frame(
    frame: &JournalFrame,
    definition: InstrumentDefinition,
) -> Result<(), DurableCallAuctionError> {
    if frame.kind() != RecordKind::InstrumentDefinition {
        return Err(DurableCallAuctionError::DefinitionRecordRequired {
            sequence: frame.sequence(),
            actual: frame.kind(),
        });
    }
    let persisted: InstrumentDefinition = frame.decode()?;
    if persisted != definition {
        return Err(DurableCallAuctionError::DefinitionMismatch {
            requested_instrument_id: definition.instrument_id(),
            requested_version: definition.version(),
            persisted_instrument_id: persisted.instrument_id(),
            persisted_version: persisted.version(),
        });
    }
    Ok(())
}

struct AuctionReplay {
    engine: CallAuctionEngine,
    pending: Option<(u64, CallAuctionCommand)>,
    checkpointed_commands: u64,
    replayed_commands: u64,
}

fn replay_auction_suffix(
    reader: StorageReader,
    mut checkpoint: Option<CallAuctionCheckpoint>,
    definition: InstrumentDefinition,
    limits: CallAuctionEngineLimits,
    wal_first_sequence: u64,
) -> Result<AuctionReplay, DurableCallAuctionError> {
    let checkpointed_commands = checkpoint
        .as_ref()
        .map(|value| u64::try_from(value.command_count()))
        .transpose()
        .map_err(|_| CallAuctionEngineError::SequenceExhausted)?
        .unwrap_or(0);
    let mut engine = if checkpoint.is_none() {
        Some(CallAuctionEngine::try_with_limits(definition, limits)?)
    } else {
        None
    };
    let mut pending = None;
    let mut replayed_commands = 0_u64;
    let mut last_wal_sequence = wal_first_sequence;
    for frame_result in reader {
        let frame = frame_result?;
        last_wal_sequence = frame.sequence();
        if checkpoint
            .as_ref()
            .is_some_and(|value| frame.sequence() <= value.generation())
        {
            validate_auction_checkpoint_prefix_frame(
                checkpoint.as_ref().expect("checkpoint presence was tested"),
                &frame,
            )?;
            continue;
        }
        if engine.is_none() {
            engine = Some(CallAuctionEngine::from_checkpoint_with_limits(
                checkpoint
                    .take()
                    .expect("first suffix frame follows a checkpoint"),
                limits,
            )?);
        }
        if replay_frame(
            engine
                .as_mut()
                .expect("engine is initialized before suffix replay"),
            &mut pending,
            &frame,
        )? {
            replayed_commands = replayed_commands
                .checked_add(1)
                .ok_or(CallAuctionEngineError::SequenceExhausted)?;
        }
    }
    if let Some(value) = checkpoint {
        if last_wal_sequence < value.generation() {
            return Err(DurableCallAuctionError::CheckpointAheadOfWal {
                checkpoint_sequence: value.generation(),
                wal_last_sequence: Some(last_wal_sequence),
            });
        }
        engine = Some(CallAuctionEngine::from_checkpoint_with_limits(
            value, limits,
        )?);
    }
    Ok(AuctionReplay {
        engine: engine.expect("engine exists after WAL/checkpoint recovery"),
        pending,
        checkpointed_commands,
        replayed_commands,
    })
}

fn validate_auction_checkpoint_prefix_frame(
    checkpoint: &CallAuctionCheckpoint,
    frame: &JournalFrame,
) -> Result<(), DurableCallAuctionError> {
    let relative = frame
        .sequence()
        .checked_sub(checkpoint.wal_metadata_sequence())
        .ok_or(DurableCallAuctionError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        })?;
    if relative == 0 {
        return Err(DurableCallAuctionError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        });
    }
    let history_index = usize::try_from((relative - 1) / 2).map_err(|_| {
        DurableCallAuctionError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        }
    })?;
    let Some(expected) = checkpoint.history().get(history_index) else {
        return Err(DurableCallAuctionError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        });
    };
    let equal = if relative % 2 == 1 {
        frame.kind() == RecordKind::CallAuctionCommand
            && frame
                .decode::<CallAuctionCommand>()
                .is_ok_and(|actual| actual == expected.command())
    } else {
        frame.kind() == RecordKind::CallAuctionExecutionReport
            && frame
                .decode::<CallAuctionExecutionReport>()
                .is_ok_and(|actual| actual == *expected.report())
    };
    if !equal {
        return Err(DurableCallAuctionError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        });
    }
    Ok(())
}

const fn storage_layout(options: StorageOptions) -> JournalLayout {
    match options {
        StorageOptions::Single(_) => JournalLayout::SingleFile,
        StorageOptions::Segmented(_) => JournalLayout::Segmented,
    }
}

fn validate_auction_checkpoint_path(
    storage_path: &Path,
    layout: JournalLayout,
    checkpoint_path: &Path,
) -> Result<(), DurableCallAuctionError> {
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
                return Err(DurableCallAuctionError::CheckpointPathConflictsWithWal {
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
                return Err(DurableCallAuctionError::CheckpointPathConflictsWithWal {
                    path: conflict.clone(),
                });
            }
        }
    }
    Ok(())
}

fn replay_frame(
    engine: &mut CallAuctionEngine,
    pending: &mut Option<(u64, CallAuctionCommand)>,
    frame: &crate::journal::JournalFrame,
) -> Result<bool, DurableCallAuctionError> {
    match frame.kind() {
        RecordKind::CallAuctionCommand => {
            if let Some((pending_sequence, _)) = pending {
                return Err(DurableCallAuctionError::ConsecutiveCommands {
                    pending_sequence: *pending_sequence,
                    next_sequence: frame.sequence(),
                });
            }
            *pending = Some((frame.sequence(), frame.decode()?));
            Ok(false)
        }
        RecordKind::CallAuctionExecutionReport => {
            let Some((command_sequence, command)) = pending.take() else {
                return Err(DurableCallAuctionError::ReportWithoutCommand {
                    sequence: frame.sequence(),
                });
            };
            let persisted: CallAuctionExecutionReport = frame.decode()?;
            let reproduced = engine.submit(command)?;
            if reproduced.replayed || persisted.replayed {
                return Err(DurableCallAuctionError::PersistedRetry {
                    command_sequence,
                    report_sequence: frame.sequence(),
                });
            }
            if reproduced != persisted {
                return Err(DurableCallAuctionError::ReplayDivergence {
                    command_sequence,
                    report_sequence: frame.sequence(),
                });
            }
            Ok(true)
        }
        kind => Err(DurableCallAuctionError::UnexpectedRecord {
            sequence: frame.sequence(),
            kind,
        }),
    }
}

struct OpenedAuctionRiskJournal {
    journal: StorageWriter,
    reader: StorageReader,
    first_data_frame: Option<JournalFrame>,
    reader_exhausted: bool,
    recovery: StorageRecoveryReport,
    completed_profile_records: u64,
    wal_first_sequence: u64,
    wal_metadata_sequence: u64,
    replay_start_sequence: u64,
    checkpoint_slot: Option<CheckpointSlot>,
    checkpoint: Option<CallAuctionRiskCheckpoint>,
}

fn canonical_auction_risk_profiles(
    profiles: &[AccountRiskDefinition],
) -> Result<Vec<AccountRiskDefinition>, DurableCallAuctionError> {
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(profiles.len())
        .map_err(|_| DurableCallAuctionError::RiskProfileCanonicalizationFailed)?;
    canonical.extend_from_slice(profiles);
    canonical.sort_unstable_by_key(|profile| profile.account_id());
    if let Some(duplicate) = canonical
        .windows(2)
        .find(|pair| pair[0].account_id() == pair[1].account_id())
    {
        return Err(DurableCallAuctionError::Risk(RiskError::DuplicateProfile(
            duplicate[0].account_id(),
        )));
    }
    Ok(canonical)
}

fn open_auction_risk_journal(
    path: &Path,
    definition: InstrumentDefinition,
    profiles: &[AccountRiskDefinition],
    options: StorageOptions,
    checkpoint_source: Option<(&Path, SnapshotOptions)>,
) -> Result<OpenedAuctionRiskJournal, DurableCallAuctionError> {
    let mut journal = StorageWriter::open(path, options)?;
    let recovery = journal.recovery();
    if recovery.last_sequence.is_none() {
        journal.append(&definition)?;
    }
    let mut reader = StorageReader::open(path, options)?;
    let Some(first_frame) = reader.next().transpose()? else {
        return Err(DurableCallAuctionError::DefinitionRecordMissing);
    };
    if first_frame.kind() == RecordKind::CheckpointAnchor {
        return open_anchored_auction_risk_journal(
            path,
            definition,
            profiles,
            options,
            checkpoint_source,
            journal,
            reader,
            recovery,
            &first_frame,
        );
    }
    validate_auction_definition_frame(&first_frame, definition)?;

    let mut persisted_profiles: Vec<AccountRiskDefinition> = Vec::new();
    persisted_profiles
        .try_reserve_exact(profiles.len())
        .map_err(|_| DurableCallAuctionError::RiskProfileCanonicalizationFailed)?;
    let mut first_data_frame = None;
    while let Some(frame) = reader.next().transpose()? {
        if frame.kind() == RecordKind::AccountRiskDefinition {
            if persisted_profiles.len() >= profiles.len() {
                return Err(DurableCallAuctionError::RiskProfileSetMismatch {
                    requested_count: profiles.len(),
                    persisted_count: persisted_profiles.len() + 1,
                });
            }
            persisted_profiles.push(frame.decode()?);
        } else {
            first_data_frame = Some(frame);
            break;
        }
    }
    let prefix_matches = persisted_profiles.len() <= profiles.len()
        && persisted_profiles
            .iter()
            .zip(profiles)
            .all(|(persisted, requested)| persisted == requested);
    if !prefix_matches || (first_data_frame.is_some() && persisted_profiles.len() != profiles.len())
    {
        return Err(DurableCallAuctionError::RiskProfileSetMismatch {
            requested_count: profiles.len(),
            persisted_count: persisted_profiles.len(),
        });
    }

    let reader_exhausted = first_data_frame.is_none();
    let mut completed_profile_records = 0_u64;
    if reader_exhausted {
        for profile in &profiles[persisted_profiles.len()..] {
            journal.append(profile)?;
            completed_profile_records = completed_profile_records
                .checked_add(1)
                .ok_or(CallAuctionEngineError::SequenceExhausted)?;
        }
    }
    let profile_count =
        u64::try_from(profiles.len()).map_err(|_| CallAuctionEngineError::SequenceExhausted)?;
    let wal_metadata_sequence = first_frame
        .sequence()
        .checked_add(profile_count)
        .ok_or(CallAuctionEngineError::SequenceExhausted)?;
    let checkpoint = checkpoint_source
        .map(|(checkpoint_path, snapshot_options)| {
            SnapshotFile::read::<CallAuctionRiskCheckpoint>(checkpoint_path, snapshot_options)
        })
        .transpose()?;
    validate_auction_risk_checkpoint_binding(
        checkpoint.as_ref(),
        first_frame.sequence(),
        wal_metadata_sequence,
        definition,
        profiles,
    )?;
    Ok(OpenedAuctionRiskJournal {
        journal,
        reader,
        first_data_frame,
        reader_exhausted,
        recovery,
        completed_profile_records,
        wal_first_sequence: first_frame.sequence(),
        wal_metadata_sequence,
        replay_start_sequence: wal_metadata_sequence,
        checkpoint_slot: None,
        checkpoint,
    })
}

#[allow(
    clippy::too_many_arguments,
    reason = "anchored recovery keeps storage ownership, immutable metadata, and selected snapshot explicit"
)]
fn open_anchored_auction_risk_journal(
    path: &Path,
    definition: InstrumentDefinition,
    profiles: &[AccountRiskDefinition],
    options: StorageOptions,
    checkpoint_source: Option<(&Path, SnapshotOptions)>,
    journal: StorageWriter,
    mut reader: StorageReader,
    recovery: StorageRecoveryReport,
    first_frame: &JournalFrame,
) -> Result<OpenedAuctionRiskJournal, DurableCallAuctionError> {
    let anchor: CheckpointAnchor = first_frame.decode()?;
    if anchor.kind() != SnapshotKind::CallAuctionRiskCheckpoint {
        return Err(DurableCallAuctionError::CheckpointAnchorKindMismatch {
            actual: anchor.kind(),
        });
    }
    let Some((checkpoint_base, snapshot_options)) = checkpoint_source else {
        return Err(DurableCallAuctionError::CheckpointRequiredForCompactedWal {
            sequence: first_frame.sequence(),
        });
    };
    if first_frame.sequence() != anchor.wal_sequence() {
        return Err(DurableCallAuctionError::CheckpointAnchorMismatch {
            generation: anchor.generation(),
            slot: anchor.slot(),
        });
    }
    let slot_path = SnapshotFile::slot_path(checkpoint_base, anchor.slot());
    validate_auction_checkpoint_path(path, storage_layout(options), &slot_path)?;
    let (checkpoint, receipt) =
        SnapshotFile::read_with_receipt::<CallAuctionRiskCheckpoint>(&slot_path, snapshot_options)?;
    if !anchor.matches_receipt(receipt) || checkpoint.generation() != anchor.generation() {
        return Err(DurableCallAuctionError::CheckpointAnchorMismatch {
            generation: anchor.generation(),
            slot: anchor.slot(),
        });
    }
    let wal_first_sequence = checkpoint.wal_first_sequence();
    let wal_metadata_sequence = checkpoint.auction().wal_metadata_sequence();
    validate_auction_risk_checkpoint_binding(
        Some(&checkpoint),
        wal_first_sequence,
        wal_metadata_sequence,
        definition,
        profiles,
    )?;
    let first_data_frame = reader.next().transpose()?;
    let reader_exhausted = first_data_frame.is_none();
    Ok(OpenedAuctionRiskJournal {
        journal,
        reader,
        first_data_frame,
        reader_exhausted,
        recovery,
        completed_profile_records: 0,
        wal_first_sequence,
        wal_metadata_sequence,
        replay_start_sequence: anchor.wal_sequence(),
        checkpoint_slot: Some(anchor.slot()),
        checkpoint: Some(checkpoint),
    })
}

fn validate_auction_risk_checkpoint_binding(
    checkpoint: Option<&CallAuctionRiskCheckpoint>,
    wal_first_sequence: u64,
    wal_metadata_sequence: u64,
    definition: InstrumentDefinition,
    profiles: &[AccountRiskDefinition],
) -> Result<(), DurableCallAuctionError> {
    let Some(value) = checkpoint else {
        return Ok(());
    };
    if value.wal_first_sequence() != wal_first_sequence {
        return Err(DurableCallAuctionError::CheckpointWalLineageMismatch {
            checkpoint_first_sequence: value.wal_first_sequence(),
            wal_first_sequence,
        });
    }
    if value.auction().wal_metadata_sequence() != wal_metadata_sequence {
        return Err(
            DurableCallAuctionError::CheckpointMetadataBoundaryMismatch {
                checkpoint_metadata_sequence: value.auction().wal_metadata_sequence(),
                wal_metadata_sequence,
            },
        );
    }
    if value.auction().definition() != definition {
        return Err(DurableCallAuctionError::CheckpointDefinitionMismatch);
    }
    let profiles_match = value.accounts().len() == profiles.len()
        && value
            .accounts()
            .iter()
            .zip(profiles)
            .all(|(checkpoint_account, requested)| {
                checkpoint_account.account_id() == requested.account_id()
                    && checkpoint_account.profile() == requested.profile()
            });
    if !profiles_match {
        return Err(DurableCallAuctionError::CheckpointRiskProfileSetMismatch);
    }
    Ok(())
}

struct AuctionRiskReplay {
    managed: CallAuctionRiskManagedEngine,
    pending: Option<(u64, CallAuctionCommand)>,
    checkpointed_commands: u64,
    replayed_commands: u64,
}

#[allow(
    clippy::too_many_arguments,
    reason = "recovery keeps physical reader state, immutable metadata, limits, and checkpoint explicit"
)]
fn replay_auction_risk_suffix(
    mut reader: StorageReader,
    first_data_frame: Option<JournalFrame>,
    reader_exhausted: bool,
    definition: InstrumentDefinition,
    profiles: &[AccountRiskDefinition],
    mut checkpoint: Option<CallAuctionRiskCheckpoint>,
    limits: CallAuctionRiskLimits,
    replay_start_sequence: u64,
) -> Result<AuctionRiskReplay, DurableCallAuctionError> {
    let checkpointed_commands = checkpoint
        .as_ref()
        .map(|value| u64::try_from(value.auction().command_count()))
        .transpose()
        .map_err(|_| CallAuctionEngineError::SequenceExhausted)?
        .unwrap_or(0);
    let mut managed = if checkpoint.is_none() {
        let mut value = CallAuctionRiskManagedEngine::try_with_limits(definition, limits)?;
        for profile in profiles {
            value.register_account(profile.account_id(), profile.profile())?;
        }
        Some(value)
    } else {
        None
    };
    let mut pending = None;
    let mut replayed_commands = 0_u64;
    let mut last_wal_sequence = replay_start_sequence;
    let mut next_frame = first_data_frame;
    loop {
        let frame = if let Some(frame) = next_frame.take() {
            frame
        } else {
            if reader_exhausted {
                break;
            }
            let Some(frame) = reader.next().transpose()? else {
                break;
            };
            frame
        };
        last_wal_sequence = frame.sequence();
        if checkpoint
            .as_ref()
            .is_some_and(|value| frame.sequence() <= value.generation())
        {
            validate_auction_risk_checkpoint_prefix_frame(
                checkpoint.as_ref().expect("checkpoint presence was tested"),
                &frame,
            )?;
            continue;
        }
        if managed.is_none() {
            let value = checkpoint
                .take()
                .expect("first suffix frame follows checkpoint");
            managed = Some(CallAuctionRiskManagedEngine::from_checkpoint_with_limits(
                &value, limits,
            )?);
        }
        if replay_auction_risk_frame(
            managed
                .as_mut()
                .expect("managed engine is initialized before suffix replay"),
            &mut pending,
            &frame,
        )? {
            replayed_commands = replayed_commands
                .checked_add(1)
                .ok_or(CallAuctionEngineError::SequenceExhausted)?;
        }
    }
    if let Some(value) = checkpoint {
        if last_wal_sequence < value.generation() {
            return Err(DurableCallAuctionError::CheckpointAheadOfWal {
                checkpoint_sequence: value.generation(),
                wal_last_sequence: Some(last_wal_sequence),
            });
        }
        managed = Some(CallAuctionRiskManagedEngine::from_checkpoint_with_limits(
            &value, limits,
        )?);
    }
    Ok(AuctionRiskReplay {
        managed: managed.expect("managed engine exists after WAL/checkpoint recovery"),
        pending,
        checkpointed_commands,
        replayed_commands,
    })
}

fn replay_auction_risk_frame(
    managed: &mut CallAuctionRiskManagedEngine,
    pending: &mut Option<(u64, CallAuctionCommand)>,
    frame: &JournalFrame,
) -> Result<bool, DurableCallAuctionError> {
    match frame.kind() {
        RecordKind::CallAuctionCommand => {
            if let Some((pending_sequence, _)) = pending {
                return Err(DurableCallAuctionError::ConsecutiveCommands {
                    pending_sequence: *pending_sequence,
                    next_sequence: frame.sequence(),
                });
            }
            *pending = Some((frame.sequence(), frame.decode()?));
            Ok(false)
        }
        RecordKind::CallAuctionExecutionReport => {
            let Some((command_sequence, command)) = pending.take() else {
                return Err(DurableCallAuctionError::ReportWithoutCommand {
                    sequence: frame.sequence(),
                });
            };
            let persisted: CallAuctionExecutionReport = frame.decode()?;
            let reproduced = managed.submit(command)?;
            if reproduced.replayed || persisted.replayed {
                return Err(DurableCallAuctionError::PersistedRetry {
                    command_sequence,
                    report_sequence: frame.sequence(),
                });
            }
            if reproduced != persisted {
                return Err(DurableCallAuctionError::ReplayDivergence {
                    command_sequence,
                    report_sequence: frame.sequence(),
                });
            }
            Ok(true)
        }
        kind => Err(DurableCallAuctionError::UnexpectedRecord {
            sequence: frame.sequence(),
            kind,
        }),
    }
}

fn validate_auction_risk_checkpoint_prefix_frame(
    checkpoint: &CallAuctionRiskCheckpoint,
    frame: &JournalFrame,
) -> Result<(), DurableCallAuctionError> {
    let relative = frame
        .sequence()
        .checked_sub(checkpoint.auction().wal_metadata_sequence())
        .ok_or(DurableCallAuctionError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        })?;
    if relative == 0 {
        return Err(DurableCallAuctionError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        });
    }
    let history_index = usize::try_from((relative - 1) / 2).map_err(|_| {
        DurableCallAuctionError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        }
    })?;
    let Some(expected) = checkpoint.auction().history().get(history_index) else {
        return Err(DurableCallAuctionError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        });
    };
    let equal = if relative % 2 == 1 {
        frame.kind() == RecordKind::CallAuctionCommand
            && frame
                .decode::<CallAuctionCommand>()
                .is_ok_and(|actual| actual == expected.command())
    } else {
        frame.kind() == RecordKind::CallAuctionExecutionReport
            && frame
                .decode::<CallAuctionExecutionReport>()
                .is_ok_and(|actual| actual == *expected.report())
    };
    if !equal {
        return Err(DurableCallAuctionError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        });
    }
    Ok(())
}
