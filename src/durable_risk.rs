//! Crash-recoverable orchestration for matching coupled to pre-trade risk.
//!
//! The WAL grammar is one instrument definition, a canonical account-sorted
//! risk-profile set, and then alternating command/report records. Recovery
//! replays both matching and reservation state and rejects any profile drift.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::domain::{InstrumentId, InstrumentVersion};
use crate::durable_storage::{
    StorageCutoverBoundary, StorageError, StorageOptions, StorageReader, StorageWriter,
};
use crate::instrument::InstrumentDefinition;
use crate::journal::{
    Journal, JournalError, JournalFrame, JournalLayout, JournalOptions, RecordKind,
    SegmentedJournalError, SegmentedJournalOptions, StorageRecoveryReport, normalize_journal_path,
};
use crate::matching::{
    Command, CommandPreparation, ConditionalImmediateExecutionDecision,
    ConditionalImmediateExecutionPreparation, ExecutionReport, ImmediateExecutionCurve,
    ImmediateExecutionCurveOutcome, ImmediateExecutionOutcome, ImmediateExecutionQuote,
    ImmediateExecutionSubmission, MatchingError, OrderBook, OrderBookQueryError, PreparedCommand,
    evaluate_conditional_immediate_execution,
};
use crate::risk::{
    AccountRiskDefinition, RiskError, RiskInvariantViolation, RiskManagedCheckpoint,
    RiskManagedCheckpointCapture, RiskManagedCheckpointError, RiskManagedLimits,
    RiskManagedOrderBook,
};
use crate::snapshot::{
    CheckpointAnchor, CheckpointCutoverReceipt, CheckpointSlot, SnapshotError, SnapshotFile,
    SnapshotKind, SnapshotOptions, SnapshotReceipt,
};

/// Result of reconstructing a durable risk-managed matching shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableRiskRecoveryReport {
    /// Physical frame recovery performed by the journal.
    pub journal: StorageRecoveryReport,
    /// Completed commands restored directly from a verified semantic checkpoint.
    pub checkpointed_commands: u64,
    /// Complete command/report pairs replayed.
    pub replayed_commands: u64,
    /// Profiles appended to complete interrupted metadata initialization.
    pub completed_profile_records: u64,
    /// Whether one final command was completed with a reconstructed report.
    pub completed_dangling_command: bool,
}

/// A WAL-synchronized but not yet coupled-replay-verified durable risk capture.
///
/// This value is bound to one open [`DurableRiskOrderBook`] incarnation and
/// its current physical WAL-prefix epoch. It has no codec or snapshot payload
/// implementation. Cloning it shares all immutable rows and the same
/// asynchronous poison/origin token in `O(1)`.
#[derive(Clone)]
pub struct DurableRiskManagedCheckpointCapture {
    capture: RiskManagedCheckpointCapture,
    origin: Arc<AtomicBool>,
    cutover_epoch: u64,
    cutover_boundary: StorageCutoverBoundary,
}

impl DurableRiskManagedCheckpointCapture {
    /// Returns the first physical WAL sequence occupied by the definition.
    #[must_use]
    pub const fn wal_first_sequence(&self) -> u64 {
        self.capture.wal_first_sequence()
    }

    /// Returns the final immutable definition/profile metadata sequence.
    #[must_use]
    pub const fn wal_metadata_sequence(&self) -> u64 {
        self.capture.wal_metadata_sequence()
    }

    /// Returns the completed durable report boundary represented here.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.capture.generation()
    }

    /// Returns the captured account/profile cardinality.
    #[must_use]
    pub fn account_count(&self) -> usize {
        self.capture.account_count()
    }

    /// Returns the captured command/report cardinality.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.capture.command_count()
    }

    /// Returns the captured active-order/reservation cardinality.
    #[must_use]
    pub fn active_order_count(&self) -> usize {
        self.capture.active_order_count()
    }

    /// Returns whether two captures share every immutable checkpoint row image.
    #[must_use]
    pub fn shares_checkpoint_storage_with(&self, other: &Self) -> bool {
        self.capture.shares_checkpoint_storage_with(&other.capture)
    }

    /// Consumes the capture and verifies deterministic coupled replay.
    ///
    /// Semantic failure atomically poisons the originating durable shard. A
    /// typed reservation or temporary replay-shard construction failure leaves
    /// that shard usable, and a cloned capture may be retried.
    ///
    /// # Errors
    ///
    /// Returns the underlying coupled-risk checkpoint verification failure.
    pub fn verify(
        self,
    ) -> Result<VerifiedDurableRiskManagedCheckpoint, RiskManagedCheckpointError> {
        let Self {
            capture,
            origin,
            cutover_epoch,
            cutover_boundary,
        } = self;
        match capture.verify() {
            Ok(checkpoint) => Ok(VerifiedDurableRiskManagedCheckpoint {
                checkpoint,
                origin,
                cutover_epoch,
                cutover_boundary,
            }),
            Err(error) => {
                record_checkpoint_verification_failure(&origin, &error);
                Err(error)
            }
        }
    }
}

/// A coupled-replay-verified checkpoint bound to one durable shard incarnation.
///
/// Only this type is accepted by standalone or prefix-retiring verified
/// publication. Its private origin and cutover epoch prevent publication
/// through another shard, a reopened shard, or the source shard after physical
/// WAL-prefix retirement.
#[derive(Clone)]
pub struct VerifiedDurableRiskManagedCheckpoint {
    checkpoint: RiskManagedCheckpoint,
    origin: Arc<AtomicBool>,
    cutover_epoch: u64,
    cutover_boundary: StorageCutoverBoundary,
}

impl VerifiedDurableRiskManagedCheckpoint {
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

    /// Returns the completed report boundary represented here.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.checkpoint.generation()
    }

    /// Returns the verified account/profile cardinality.
    #[must_use]
    pub fn account_count(&self) -> usize {
        self.checkpoint.accounts().len()
    }

    /// Returns the verified command/report cardinality.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.checkpoint.matching().command_count()
    }

    /// Returns the verified active-order/reservation cardinality.
    #[must_use]
    pub fn active_order_count(&self) -> usize {
        self.checkpoint.matching().active_order_count()
    }

    /// Returns whether two values share every immutable verified row image.
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
}

/// Durable risk-managed matching failure.
#[derive(Debug)]
pub enum DurableRiskError {
    /// Journal framing, codec, I/O, or recovery failure.
    Journal(JournalError),
    /// Segmented-journal inventory, rotation, framing, recovery, or I/O failure.
    SegmentedJournal(SegmentedJournalError),
    /// Matching operational failure.
    Matching(MatchingError),
    /// Exact caller-owned order-book query output could not be constructed.
    OrderBookQuery(OrderBookQueryError),
    /// Risk-profile validation failure.
    Risk(RiskError),
    /// Reconstructed matching/risk state violated an invariant.
    Invariant(RiskInvariantViolation),
    /// Semantic coupled risk/matching checkpoint failed validation.
    Checkpoint(RiskManagedCheckpointError),
    /// Snapshot framing, checksum, ownership, generation, or I/O failed.
    Snapshot(SnapshotError),
    /// The initialized journal unexpectedly had no first frame.
    DefinitionRecordMissing,
    /// First frame was not an instrument definition.
    DefinitionRecordRequired {
        /// First frame sequence.
        sequence: u64,
        /// Kind found instead.
        actual: RecordKind,
    },
    /// Persisted and requested definitions were not structurally identical.
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
    /// Persisted canonical profile set differed from the requested set.
    RiskProfileSetMismatch {
        /// Requested profile count.
        requested_count: usize,
        /// Persisted profile count.
        persisted_count: usize,
    },
    /// A checkpoint belongs to a different physical WAL origin.
    CheckpointWalLineageMismatch {
        /// First WAL sequence claimed by the checkpoint.
        checkpoint_first_sequence: u64,
        /// First sequence of the opened WAL.
        wal_first_sequence: u64,
    },
    /// A checkpoint's last immutable-metadata frame differs from this WAL.
    CheckpointMetadataBoundaryMismatch {
        /// Metadata boundary claimed by the checkpoint.
        checkpoint_metadata_sequence: u64,
        /// Metadata boundary derived from the WAL definition/profile grammar.
        wal_metadata_sequence: u64,
    },
    /// A checkpoint's instrument definition differs from the WAL binding.
    CheckpointDefinitionMismatch,
    /// A checkpoint's immutable profile set differs from the WAL binding.
    CheckpointRiskProfileSetMismatch,
    /// A checkpoint command or report differs from the same WAL sequence.
    CheckpointPrefixDivergence {
        /// Physical WAL sequence where values differed.
        wal_sequence: u64,
    },
    /// A checkpoint boundary is newer than the complete verified WAL.
    CheckpointAheadOfWal {
        /// Completed report boundary claimed by the checkpoint.
        checkpoint_sequence: u64,
        /// Last verified WAL frame sequence.
        wal_last_sequence: Option<u64>,
    },
    /// A compacted WAL requires its anchor-selected coupled checkpoint.
    CheckpointRequiredForCompactedWal {
        /// Physical sequence of the checkpoint anchor.
        sequence: u64,
    },
    /// A compacted risk WAL anchor names another semantic checkpoint kind.
    CheckpointAnchorKindMismatch {
        /// Kind stored in the anchor.
        actual: SnapshotKind,
    },
    /// The selected coupled checkpoint does not equal the WAL anchor identity.
    CheckpointAnchorMismatch {
        /// Anchored checkpoint generation.
        generation: u64,
        /// Selected alternating checkpoint slot.
        slot: CheckpointSlot,
    },
    /// Snapshot output aliases WAL storage or its ownership namespace.
    CheckpointPathConflictsWithWal {
        /// Conflicting normalized path.
        path: PathBuf,
    },
    /// A verified checkpoint belongs to another or previously reopened shard.
    CheckpointCaptureOriginMismatch,
    /// Physical WAL-prefix retirement occurred after the checkpoint was captured.
    CheckpointCaptureStale {
        /// Process-local cutover epoch retained by the capture.
        captured_epoch: u64,
        /// Current process-local cutover epoch of the source shard.
        current_epoch: u64,
    },
    /// Process-local durable checkpoint cutover fencing cannot advance.
    CheckpointCutoverEpochExhausted,
    /// A payload kind was not permitted in a risk-managed matching journal.
    UnexpectedRecord {
        /// Frame sequence.
        sequence: u64,
        /// Unexpected kind.
        kind: RecordKind,
    },
    /// Another command appeared before the pending command's report.
    ConsecutiveCommands {
        /// Pending command sequence.
        pending_sequence: u64,
        /// Next command sequence.
        next_sequence: u64,
    },
    /// An execution report had no preceding command.
    ReportWithoutCommand {
        /// Report frame sequence.
        sequence: u64,
    },
    /// Deterministic replay did not reproduce the stored report.
    ReplayDivergence {
        /// Command frame sequence.
        command_sequence: u64,
        /// Report frame sequence.
        report_sequence: u64,
    },
    /// Runtime cannot continue after an ambiguous durable transition.
    Poisoned,
}

impl fmt::Display for DurableRiskError {
    #[allow(
        clippy::too_many_lines,
        reason = "the exhaustive durable-risk error map keeps every operator-visible failure distinct"
    )]
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(error) => error.fmt(formatter),
            Self::SegmentedJournal(error) => error.fmt(formatter),
            Self::Matching(error) => error.fmt(formatter),
            Self::OrderBookQuery(error) => error.fmt(formatter),
            Self::Risk(error) => error.fmt(formatter),
            Self::Invariant(error) => error.fmt(formatter),
            Self::Checkpoint(error) => error.fmt(formatter),
            Self::Snapshot(error) => error.fmt(formatter),
            Self::DefinitionRecordMissing => {
                formatter.write_str("risk matching journal has no instrument definition")
            }
            Self::DefinitionRecordRequired { sequence, actual } => write!(
                formatter,
                "first risk matching frame at {sequence} must be an instrument definition, found {actual:?}"
            ),
            Self::DefinitionMismatch {
                requested_instrument_id,
                requested_version,
                persisted_instrument_id,
                persisted_version,
            } => write!(
                formatter,
                "requested definition {requested_instrument_id}@{requested_version} differs from persisted definition {persisted_instrument_id}@{persisted_version}"
            ),
            Self::RiskProfileSetMismatch {
                requested_count,
                persisted_count,
            } => write!(
                formatter,
                "requested {requested_count} risk profiles differ from {persisted_count} persisted profiles"
            ),
            Self::CheckpointWalLineageMismatch {
                checkpoint_first_sequence,
                wal_first_sequence,
            } => write!(
                formatter,
                "risk checkpoint WAL starts at {checkpoint_first_sequence}, but journal starts at {wal_first_sequence}"
            ),
            Self::CheckpointMetadataBoundaryMismatch {
                checkpoint_metadata_sequence,
                wal_metadata_sequence,
            } => write!(
                formatter,
                "risk checkpoint metadata ends at {checkpoint_metadata_sequence}, but WAL metadata ends at {wal_metadata_sequence}"
            ),
            Self::CheckpointDefinitionMismatch => formatter
                .write_str("risk checkpoint instrument definition differs from the WAL binding"),
            Self::CheckpointRiskProfileSetMismatch => {
                formatter.write_str("risk checkpoint profile set differs from the WAL binding")
            }
            Self::CheckpointPrefixDivergence { wal_sequence } => write!(
                formatter,
                "risk checkpoint differs from WAL frame {wal_sequence}"
            ),
            Self::CheckpointAheadOfWal {
                checkpoint_sequence,
                wal_last_sequence,
            } => write!(
                formatter,
                "risk checkpoint covers WAL sequence {checkpoint_sequence}, but verified WAL ends at {wal_last_sequence:?}"
            ),
            Self::CheckpointRequiredForCompactedWal { sequence } => write!(
                formatter,
                "risk WAL begins with checkpoint anchor {sequence}; a checkpoint base path is required"
            ),
            Self::CheckpointAnchorKindMismatch { actual } => write!(
                formatter,
                "risk WAL anchor references incompatible snapshot kind {actual:?}"
            ),
            Self::CheckpointAnchorMismatch { generation, slot } => write!(
                formatter,
                "risk WAL anchor at generation {generation} does not match checkpoint slot {slot:?}"
            ),
            Self::CheckpointPathConflictsWithWal { path } => write!(
                formatter,
                "risk checkpoint path {} conflicts with WAL storage",
                path.display()
            ),
            Self::CheckpointCaptureOriginMismatch => formatter
                .write_str("verified risk checkpoint belongs to another durable shard incarnation"),
            Self::CheckpointCaptureStale {
                captured_epoch,
                current_epoch,
            } => write!(
                formatter,
                "verified risk checkpoint was captured at WAL cutover epoch {captured_epoch}, but the shard is at epoch {current_epoch}"
            ),
            Self::CheckpointCutoverEpochExhausted => {
                formatter.write_str("durable risk checkpoint cutover epoch is exhausted")
            }
            Self::UnexpectedRecord { sequence, kind } => {
                write!(
                    formatter,
                    "unexpected {kind:?} record at sequence {sequence}"
                )
            }
            Self::ConsecutiveCommands {
                pending_sequence,
                next_sequence,
            } => write!(
                formatter,
                "command at {pending_sequence} has no report before command at {next_sequence}"
            ),
            Self::ReportWithoutCommand { sequence } => {
                write!(formatter, "report at {sequence} has no preceding command")
            }
            Self::ReplayDivergence {
                command_sequence,
                report_sequence,
            } => write!(
                formatter,
                "risk replay of command at {command_sequence} diverged from report at {report_sequence}"
            ),
            Self::Poisoned => {
                formatter.write_str("durable risk matching shard is poisoned and requires recovery")
            }
        }
    }
}

impl std::error::Error for DurableRiskError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(error) => Some(error),
            Self::SegmentedJournal(error) => Some(error),
            Self::Matching(error) => Some(error),
            Self::OrderBookQuery(error) => Some(error),
            Self::Risk(error) => Some(error),
            Self::Invariant(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            Self::Snapshot(error) => Some(error),
            _ => None,
        }
    }
}

impl From<JournalError> for DurableRiskError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

impl From<SegmentedJournalError> for DurableRiskError {
    fn from(error: SegmentedJournalError) -> Self {
        Self::SegmentedJournal(error)
    }
}

impl From<StorageError> for DurableRiskError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Journal(error) => Self::Journal(error),
            StorageError::Segmented(error) => Self::SegmentedJournal(error),
        }
    }
}

impl From<MatchingError> for DurableRiskError {
    fn from(error: MatchingError) -> Self {
        Self::Matching(error)
    }
}

impl From<OrderBookQueryError> for DurableRiskError {
    fn from(error: OrderBookQueryError) -> Self {
        Self::OrderBookQuery(error)
    }
}

impl From<RiskError> for DurableRiskError {
    fn from(error: RiskError) -> Self {
        Self::Risk(error)
    }
}

impl From<RiskManagedCheckpointError> for DurableRiskError {
    fn from(error: RiskManagedCheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

impl From<SnapshotError> for DurableRiskError {
    fn from(error: SnapshotError) -> Self {
        Self::Snapshot(error)
    }
}

/// One single-writer, definition/profile-bound durable risk matching shard.
#[derive(Debug)]
pub struct DurableRiskOrderBook {
    managed: RiskManagedOrderBook,
    journal: StorageWriter,
    wal_first_sequence: u64,
    wal_metadata_sequence: u64,
    checkpoint_slot: Option<CheckpointSlot>,
    checkpoint_verification_origin: Arc<AtomicBool>,
    cutover_epoch: u64,
    recovery: DurableRiskRecoveryReport,
    poisoned: bool,
}

struct OpenedRiskJournal {
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
    checkpoint: Option<RiskManagedCheckpoint>,
}

struct RiskReplay {
    managed: RiskManagedOrderBook,
    pending: Option<(u64, Command)>,
    checkpointed_commands: u64,
    replayed_commands: u64,
}

impl DurableRiskOrderBook {
    /// Opens and replays a risk-managed matching WAL.
    ///
    /// Profile definitions are canonicalized by account identifier. A journal
    /// that ended during initial profile emission is completed only when its
    /// persisted definitions are an exact prefix and no command follows.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for invalid metadata, profile drift,
    /// journal grammar failure, replay divergence, or invariant failure.
    pub fn open(
        path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        options: JournalOptions,
    ) -> Result<Self, DurableRiskError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            profiles,
            RiskManagedLimits::default(),
            StorageOptions::Single(options),
            None,
        )
    }

    /// Opens and replays a risk-managed WAL under explicit coupled resource limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for metadata/storage/replay failure or
    /// matching/profile limits insufficient for recovered state.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        limits: RiskManagedLimits,
        options: JournalOptions,
    ) -> Result<Self, DurableRiskError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            profiles,
            limits,
            StorageOptions::Single(options),
            None,
        )
    }

    /// Opens a risk-managed WAL from a checksum-verified coupled checkpoint,
    /// proves its complete metadata and command/report lineage, and replays only its suffix.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for snapshot, path, definition/profile,
    /// prefix, journal, matching, risk, or reconstructed-state failure.
    pub fn open_with_checkpoint(
        path: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        options: JournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableRiskError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            profiles,
            RiskManagedLimits::default(),
            StorageOptions::Single(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens a risk-managed WAL and checkpoint under explicit coupled resource limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for snapshot/metadata/storage/replay failure
    /// or matching/profile limits insufficient for recovered state.
    pub fn open_with_checkpoint_and_limits(
        path: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        limits: RiskManagedLimits,
        options: JournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableRiskError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            profiles,
            limits,
            StorageOptions::Single(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens a risk-managed matching shard over an automatically rotating WAL directory.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for segmented storage, metadata, risk,
    /// matching, grammar, or deterministic replay failure.
    pub fn open_segmented(
        directory: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        options: SegmentedJournalOptions,
    ) -> Result<Self, DurableRiskError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            profiles,
            RiskManagedLimits::default(),
            StorageOptions::Segmented(options),
            None,
        )
    }

    /// Opens segmented risk-managed storage under explicit coupled resource limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for storage/replay failure or insufficient limits.
    pub fn open_segmented_with_limits(
        directory: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        limits: RiskManagedLimits,
        options: SegmentedJournalOptions,
    ) -> Result<Self, DurableRiskError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            profiles,
            limits,
            StorageOptions::Segmented(options),
            None,
        )
    }

    /// Opens a segmented risk-managed WAL from a verified coupled checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for snapshot, managed-path, metadata,
    /// prefix, segmented storage, matching, risk, or invariant failure.
    pub fn open_segmented_with_checkpoint(
        directory: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        options: SegmentedJournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableRiskError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            profiles,
            RiskManagedLimits::default(),
            StorageOptions::Segmented(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens segmented risk storage and a checkpoint under explicit coupled resource limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for snapshot/storage/replay failure or
    /// matching/profile limits insufficient for recovered state.
    pub fn open_segmented_with_checkpoint_and_limits(
        directory: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        profiles: &[AccountRiskDefinition],
        limits: RiskManagedLimits,
        options: SegmentedJournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableRiskError> {
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
        limits: RiskManagedLimits,
        options: StorageOptions,
        checkpoint_source: Option<(&Path, SnapshotOptions)>,
    ) -> Result<Self, DurableRiskError> {
        if profiles.len() > limits.max_registered_accounts() {
            return Err(DurableRiskError::Risk(
                RiskError::ProfileCapacityExhausted {
                    maximum: limits.max_registered_accounts(),
                },
            ));
        }
        let profiles = canonical_profiles(profiles)?;
        if let Some((checkpoint_path, _)) = checkpoint_source {
            validate_checkpoint_path(path, storage_layout(options), checkpoint_path)?;
        }
        let OpenedRiskJournal {
            mut journal,
            reader,
            first_data_frame,
            reader_exhausted: replay_reader_exhausted,
            recovery: journal_recovery,
            completed_profile_records,
            wal_first_sequence,
            wal_metadata_sequence,
            replay_start_sequence,
            checkpoint_slot,
            checkpoint,
        } = open_risk_metadata(path, definition, &profiles, options, checkpoint_source)?;
        let replay = replay_risk_suffix(
            reader,
            first_data_frame,
            replay_reader_exhausted,
            definition,
            &profiles,
            checkpoint,
            limits,
            replay_start_sequence,
        )?;
        let mut managed = replay.managed;
        let completed_dangling_command = if let Some((_, command)) = replay.pending {
            let report = managed.submit(command)?;
            journal.append(&report)?;
            true
        } else {
            false
        };
        managed.validate().map_err(DurableRiskError::Invariant)?;
        Ok(Self {
            managed,
            journal,
            wal_first_sequence,
            wal_metadata_sequence,
            checkpoint_slot,
            checkpoint_verification_origin: Arc::new(AtomicBool::new(false)),
            cutover_epoch: 0,
            recovery: DurableRiskRecoveryReport {
                journal: journal_recovery,
                checkpointed_commands: replay.checkpointed_commands,
                replayed_commands: replay.replayed_commands,
                completed_profile_records,
                completed_dangling_command,
            },
            poisoned: false,
        })
    }

    /// Returns recovery statistics captured at open.
    #[must_use]
    pub const fn recovery(&self) -> DurableRiskRecoveryReport {
        self.recovery
    }

    /// Returns reconstructed matching and risk state.
    #[must_use]
    pub const fn managed(&self) -> &RiskManagedOrderBook {
        &self.managed
    }

    /// Returns whether local failure or asynchronous checkpoint verification
    /// has made this shard unusable until reopen/recovery.
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned || self.checkpoint_verification_origin.load(Ordering::Acquire)
    }

    /// Writes a command before applying matching/risk state and then persists its trace.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for preparation, journal, or matching failure.
    /// A failure after command acknowledgement poisons this instance.
    pub fn submit(&mut self, command: Command) -> Result<ExecutionReport, DurableRiskError> {
        if self.is_poisoned() {
            return Err(DurableRiskError::Poisoned);
        }
        let prepared = match self.managed.prepare(command)? {
            CommandPreparation::Replay(report) => {
                if self.is_poisoned() {
                    return Err(DurableRiskError::Poisoned);
                }
                return Ok(report);
            }
            CommandPreparation::Ready(prepared) => prepared,
        };
        self.commit_prepared(prepared)
    }

    /// Durably risk-gates, quotes, and conditionally submits one canonical IOC order.
    ///
    /// Replay plus core or risk rejection bypass `accept`. Decline occurs
    /// before WAL or coupled-state mutation. Acceptance writes the command,
    /// applies the same matching preparation and risk gate, then writes the
    /// report under the ordinary poison/recovery protocol.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for poison, preparation, journal, or
    /// coupled commit failure. A failure after command acknowledgement poisons
    /// the instance for deterministic reopen recovery.
    pub fn submit_immediate_execution_if(
        &mut self,
        submission: ImmediateExecutionSubmission,
        accept: impl FnOnce(ImmediateExecutionQuote) -> bool,
    ) -> Result<ImmediateExecutionOutcome, DurableRiskError> {
        self.submit_conditional_immediate_execution(
            submission,
            |_, quote| Ok(quote),
            |quote| accept(*quote),
        )
        .map(Into::into)
    }

    /// Durably risk-gates, constructs an exact private execution curve, and
    /// conditionally submits one canonical IOC order.
    ///
    /// Replay plus core or risk rejection bypass curve allocation and
    /// `accept`. Allocation failure, decline, or predicate unwind occurs
    /// before WAL or coupled-state mutation. Acceptance persists the command,
    /// commits the curve-bound coupled preparation, and persists its report.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for poison, preparation, exact curve
    /// construction, journal, or coupled commit failure. A failure after
    /// command acknowledgement poisons the instance for deterministic reopen
    /// recovery.
    pub fn try_submit_immediate_execution_curve_if(
        &mut self,
        submission: ImmediateExecutionSubmission,
        accept: impl FnOnce(&ImmediateExecutionCurve) -> bool,
    ) -> Result<ImmediateExecutionCurveOutcome, DurableRiskError> {
        self.submit_conditional_immediate_execution(
            submission,
            |book, quote| Ok(book.try_immediate_execution_curve_for_quote(quote)?),
            accept,
        )
        .map(Into::into)
    }

    fn submit_conditional_immediate_execution<T>(
        &mut self,
        submission: ImmediateExecutionSubmission,
        observe: impl FnOnce(&OrderBook, ImmediateExecutionQuote) -> Result<T, DurableRiskError>,
        accept: impl FnOnce(&T) -> bool,
    ) -> Result<ConditionalImmediateExecutionDecision<T>, DurableRiskError> {
        if self.is_poisoned() {
            return Err(DurableRiskError::Poisoned);
        }
        let preflight = self
            .managed
            .prepare_conditional_immediate_execution(submission)?;
        match evaluate_conditional_immediate_execution(
            preflight,
            |quote| observe(self.managed.book(), quote),
            accept,
        )? {
            ConditionalImmediateExecutionPreparation::Complete(decision) => {
                if self.is_poisoned() {
                    return Err(DurableRiskError::Poisoned);
                }
                Ok(decision)
            }
            ConditionalImmediateExecutionPreparation::Commit {
                prepared,
                observation,
            } => {
                let report = self.commit_prepared(prepared)?;
                let retained_observation = if report.replayed { None } else { observation };
                Ok(ConditionalImmediateExecutionDecision::Reported {
                    observation: retained_observation,
                    report,
                })
            }
        }
    }

    fn commit_prepared(
        &mut self,
        prepared: PreparedCommand,
    ) -> Result<ExecutionReport, DurableRiskError> {
        if self.is_poisoned() {
            return Err(DurableRiskError::Poisoned);
        }
        if let Err(error) = self.journal.append(&prepared.command()) {
            self.poisoned = self.journal.is_poisoned();
            return Err(error.into());
        }
        let report = match self.managed.commit(prepared) {
            Ok(report) => report,
            Err(error) => {
                self.poisoned = true;
                return Err(DurableRiskError::Matching(error));
            }
        };
        if let Err(error) = self.journal.append(&report) {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(report)
    }

    /// Synchronizes the risk WAL and captures one immutable coupled checkpoint
    /// candidate without deterministic history replay.
    ///
    /// The returned value is bound to this exact open shard incarnation and
    /// current physical-prefix epoch. Ordinary command/report suffix growth is
    /// permitted while verification runs; WAL-prefix cutover is not.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for poison, WAL synchronization failure, or
    /// a typed live coupled-checkpoint capture/lineage failure. Operational
    /// allocation/construction failures leave the shard usable; semantic
    /// contradictions poison it.
    pub fn capture_checkpoint_candidate(
        &mut self,
    ) -> Result<DurableRiskManagedCheckpointCapture, DurableRiskError> {
        if self.is_poisoned() {
            return Err(DurableRiskError::Poisoned);
        }
        self.sync_all()?;
        if self.is_poisoned() {
            return Err(DurableRiskError::Poisoned);
        }
        let cutover_boundary = self.journal.cutover_boundary()?;
        let wal_sequence = cutover_boundary.wal_sequence();
        let capture = match self.managed.capture_checkpoint_candidate(
            self.wal_first_sequence,
            self.wal_metadata_sequence,
            wal_sequence,
        ) {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = checkpoint_failure_requires_poison(&error);
                return Err(DurableRiskError::Checkpoint(error));
            }
        };
        Ok(DurableRiskManagedCheckpointCapture {
            capture,
            origin: Arc::clone(&self.checkpoint_verification_origin),
            cutover_epoch: self.cutover_epoch,
            cutover_boundary,
        })
    }

    /// Synchronizes the risk WAL, audits coupled state, and atomically replaces
    /// a checksum-protected semantic checkpoint.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for poison, path conflict, WAL barrier,
    /// checkpoint audit/codec, snapshot ownership, or I/O failure.
    /// Typed matching/risk checkpoint resource and temporary-construction
    /// failures leave the shard unpoisoned; semantic contradictions poison it.
    pub fn write_checkpoint(
        &mut self,
        path: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<SnapshotReceipt, DurableRiskError> {
        if self.is_poisoned() {
            return Err(DurableRiskError::Poisoned);
        }
        validate_checkpoint_path(self.journal.path(), self.journal.layout(), path.as_ref())?;
        let capture = self.capture_checkpoint_candidate()?;
        let verified = match capture.verify() {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = checkpoint_failure_requires_poison(&error);
                return Err(DurableRiskError::Checkpoint(error));
            }
        };
        self.write_verified_checkpoint(path, &verified, options)
    }

    /// Publishes a coupled-replay-verified capture through its originating shard.
    ///
    /// Ordinary suffix growth after capture is allowed because the checkpoint
    /// remains an exact older WAL prefix. Publication rejects another/reopened
    /// shard and any source shard whose physical prefix was compacted after
    /// capture. This method writes a standalone checkpoint and does not retire
    /// a WAL prefix.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for poison, origin/epoch or metadata
    /// mismatch, path conflict, checkpoint-ahead contradiction, snapshot
    /// ownership, codec, or I/O failure.
    pub fn write_verified_checkpoint(
        &mut self,
        path: impl AsRef<Path>,
        verified: &VerifiedDurableRiskManagedCheckpoint,
        options: SnapshotOptions,
    ) -> Result<SnapshotReceipt, DurableRiskError> {
        validate_checkpoint_path(self.journal.path(), self.journal.layout(), path.as_ref())?;
        self.validate_verified_checkpoint(verified)?;
        SnapshotFile::write(path, &verified.checkpoint, options).map_err(Into::into)
    }

    /// Publishes a coupled-replay-verified checkpoint and retires its WAL prefix.
    ///
    /// Any command/report suffix appended after capture is synchronized and
    /// migrated unchanged behind the new checkpoint anchor. The coupled replay
    /// proof is not repeated while the writer is excluded.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for poison, origin/epoch, lineage, metadata,
    /// or WAL-boundary drift, path conflict, snapshot persistence, suffix
    /// migration, or physical publication failure.
    pub fn compact_verified_checkpoint(
        &mut self,
        checkpoint_base: impl AsRef<Path>,
        verified: &VerifiedDurableRiskManagedCheckpoint,
        options: SnapshotOptions,
    ) -> Result<CheckpointCutoverReceipt, DurableRiskError> {
        validate_checkpoint_path(
            self.journal.path(),
            self.journal.layout(),
            checkpoint_base.as_ref(),
        )?;
        self.compact_verified_checkpoint_validated(checkpoint_base.as_ref(), verified, options)
    }

    fn validate_verified_checkpoint(
        &self,
        verified: &VerifiedDurableRiskManagedCheckpoint,
    ) -> Result<u64, DurableRiskError> {
        if self.is_poisoned() {
            return Err(DurableRiskError::Poisoned);
        }
        if !Arc::ptr_eq(&self.checkpoint_verification_origin, &verified.origin) {
            return Err(DurableRiskError::CheckpointCaptureOriginMismatch);
        }
        if verified.cutover_epoch != self.cutover_epoch {
            return Err(DurableRiskError::CheckpointCaptureStale {
                captured_epoch: verified.cutover_epoch,
                current_epoch: self.cutover_epoch,
            });
        }
        if verified.checkpoint.wal_first_sequence() != self.wal_first_sequence {
            return Err(DurableRiskError::CheckpointWalLineageMismatch {
                checkpoint_first_sequence: verified.checkpoint.wal_first_sequence(),
                wal_first_sequence: self.wal_first_sequence,
            });
        }
        if verified.checkpoint.matching().wal_metadata_sequence() != self.wal_metadata_sequence {
            return Err(DurableRiskError::CheckpointMetadataBoundaryMismatch {
                checkpoint_metadata_sequence: verified
                    .checkpoint
                    .matching()
                    .wal_metadata_sequence(),
                wal_metadata_sequence: self.wal_metadata_sequence,
            });
        }
        let wal_sequence = self
            .journal
            .next_sequence()
            .checked_sub(1)
            .ok_or(MatchingError::SequenceExhausted)?;
        if verified.checkpoint.generation() != verified.cutover_boundary.wal_sequence()
            || verified.checkpoint.generation() > wal_sequence
        {
            return Err(DurableRiskError::CheckpointAheadOfWal {
                checkpoint_sequence: verified.checkpoint.generation(),
                wal_last_sequence: Some(wal_sequence),
            });
        }
        Ok(wal_sequence)
    }

    /// Writes the inactive coupled checkpoint slot and atomically retires the
    /// selected risk WAL prefix behind its exact checkpoint anchor.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for poison, path conflicts, audit/encoding
    /// failure, snapshot persistence, or physical cutover failure.
    /// Pre-semantic checkpoint resource/construction failure performs no cutover
    /// mutation and leaves the shard unpoisoned.
    pub fn compact_to_checkpoint(
        &mut self,
        checkpoint_base: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<CheckpointCutoverReceipt, DurableRiskError> {
        if self.is_poisoned() {
            return Err(DurableRiskError::Poisoned);
        }
        validate_checkpoint_path(
            self.journal.path(),
            self.journal.layout(),
            checkpoint_base.as_ref(),
        )?;
        let capture = self.capture_checkpoint_candidate()?;
        let verified = match capture.verify() {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = checkpoint_failure_requires_poison(&error);
                return Err(DurableRiskError::Checkpoint(error));
            }
        };
        self.compact_verified_checkpoint_validated(checkpoint_base.as_ref(), &verified, options)
    }

    fn compact_verified_checkpoint_validated(
        &mut self,
        checkpoint_base: &Path,
        verified: &VerifiedDurableRiskManagedCheckpoint,
        options: SnapshotOptions,
    ) -> Result<CheckpointCutoverReceipt, DurableRiskError> {
        self.validate_verified_checkpoint(verified)?;
        let next_cutover_epoch = self
            .cutover_epoch
            .checked_add(1)
            .ok_or(DurableRiskError::CheckpointCutoverEpochExhausted)?;
        let slot = self
            .checkpoint_slot
            .map_or(CheckpointSlot::A, CheckpointSlot::alternate);
        let slot_path = SnapshotFile::slot_path(checkpoint_base, slot);
        validate_checkpoint_path(self.journal.path(), self.journal.layout(), &slot_path)?;
        let snapshot = SnapshotFile::write(&slot_path, &verified.checkpoint, options)?;
        let anchor = CheckpointAnchor::new(
            SnapshotKind::RiskManagedCheckpoint,
            slot,
            snapshot,
            verified.checkpoint.generation(),
        );
        if let Err(error) = self
            .journal
            .replace_with_checkpoint_anchor_at(anchor, verified.cutover_boundary)
        {
            self.poisoned = self.journal.is_poisoned();
            return Err(error.into());
        }
        self.checkpoint_slot = Some(slot);
        self.cutover_epoch = next_cutover_epoch;
        Ok(CheckpointCutoverReceipt::new(
            snapshot,
            slot,
            verified.checkpoint.generation(),
        ))
    }

    /// Synchronizes journal data and metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] and poisons the shard on synchronization failure.
    pub fn sync_all(&mut self) -> Result<(), DurableRiskError> {
        if self.is_poisoned() {
            return Err(DurableRiskError::Poisoned);
        }
        if let Err(error) = self.journal.sync_all() {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(())
    }

    /// Synchronizes all matching/risk data and metadata, then releases
    /// exclusive writer ownership.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] if the shard is poisoned or final journal
    /// synchronization/lease release fails.
    pub fn close(self) -> Result<(), DurableRiskError> {
        if self.is_poisoned() {
            return Err(DurableRiskError::Poisoned);
        }
        self.journal.close().map_err(Into::into)
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "one metadata boundary handles full-prefix initialization and anchor-selected compacted recovery without ambiguous shared state"
)]
fn open_risk_metadata(
    path: &Path,
    definition: InstrumentDefinition,
    profiles: &[AccountRiskDefinition],
    options: StorageOptions,
    checkpoint_source: Option<(&Path, SnapshotOptions)>,
) -> Result<OpenedRiskJournal, DurableRiskError> {
    let mut journal = StorageWriter::open(path, options)?;
    let recovery = journal.recovery();
    if recovery.last_sequence.is_none() {
        journal.append(&definition)?;
    }

    let mut reader = StorageReader::open(path, options)?;
    let Some(first_frame) = reader.next().transpose()? else {
        return Err(DurableRiskError::DefinitionRecordMissing);
    };
    if first_frame.kind() == RecordKind::CheckpointAnchor {
        let anchor: CheckpointAnchor = first_frame.decode()?;
        if anchor.kind() != SnapshotKind::RiskManagedCheckpoint {
            return Err(DurableRiskError::CheckpointAnchorKindMismatch {
                actual: anchor.kind(),
            });
        }
        let Some((checkpoint_base, snapshot_options)) = checkpoint_source else {
            return Err(DurableRiskError::CheckpointRequiredForCompactedWal {
                sequence: first_frame.sequence(),
            });
        };
        if first_frame.sequence() != anchor.wal_sequence() {
            return Err(DurableRiskError::CheckpointAnchorMismatch {
                generation: anchor.generation(),
                slot: anchor.slot(),
            });
        }
        let slot_path = SnapshotFile::slot_path(checkpoint_base, anchor.slot());
        validate_checkpoint_path(path, storage_layout(options), &slot_path)?;
        let (checkpoint, receipt) =
            SnapshotFile::read_with_receipt::<RiskManagedCheckpoint>(&slot_path, snapshot_options)?;
        if !anchor.matches_receipt(receipt) || checkpoint.generation() != anchor.generation() {
            return Err(DurableRiskError::CheckpointAnchorMismatch {
                generation: anchor.generation(),
                slot: anchor.slot(),
            });
        }
        let wal_first_sequence = checkpoint.wal_first_sequence();
        let wal_metadata_sequence = checkpoint.matching().wal_metadata_sequence();
        validate_checkpoint_binding(
            Some(&checkpoint),
            wal_first_sequence,
            wal_metadata_sequence,
            definition,
            profiles,
        )?;
        let first_data_frame = reader.next().transpose()?;
        let reader_exhausted = first_data_frame.is_none();
        return Ok(OpenedRiskJournal {
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
        });
    }
    if first_frame.kind() != RecordKind::InstrumentDefinition {
        return Err(DurableRiskError::DefinitionRecordRequired {
            sequence: first_frame.sequence(),
            actual: first_frame.kind(),
        });
    }
    let persisted_definition: InstrumentDefinition = first_frame.decode()?;
    if persisted_definition != definition {
        return Err(DurableRiskError::DefinitionMismatch {
            requested_instrument_id: definition.instrument_id(),
            requested_version: definition.version(),
            persisted_instrument_id: persisted_definition.instrument_id(),
            persisted_version: persisted_definition.version(),
        });
    }

    let mut persisted_profiles: Vec<AccountRiskDefinition> = Vec::new();
    let mut first_data_frame = None;
    while let Some(frame) = reader.next().transpose()? {
        if frame.kind() == RecordKind::AccountRiskDefinition {
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
        return Err(DurableRiskError::RiskProfileSetMismatch {
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
                .ok_or(MatchingError::SequenceExhausted)?;
        }
    }
    let profile_count =
        u64::try_from(profiles.len()).map_err(|_| MatchingError::SequenceExhausted)?;
    let wal_metadata_sequence = first_frame
        .sequence()
        .checked_add(profile_count)
        .ok_or(MatchingError::SequenceExhausted)?;
    let checkpoint = checkpoint_source
        .map(|(checkpoint_path, snapshot_options)| {
            SnapshotFile::read::<RiskManagedCheckpoint>(checkpoint_path, snapshot_options)
        })
        .transpose()?;
    validate_checkpoint_binding(
        checkpoint.as_ref(),
        first_frame.sequence(),
        wal_metadata_sequence,
        definition,
        profiles,
    )?;
    Ok(OpenedRiskJournal {
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

fn validate_checkpoint_binding(
    checkpoint: Option<&RiskManagedCheckpoint>,
    wal_first_sequence: u64,
    wal_metadata_sequence: u64,
    definition: InstrumentDefinition,
    profiles: &[AccountRiskDefinition],
) -> Result<(), DurableRiskError> {
    let Some(value) = checkpoint else {
        return Ok(());
    };
    if value.wal_first_sequence() != wal_first_sequence {
        return Err(DurableRiskError::CheckpointWalLineageMismatch {
            checkpoint_first_sequence: value.wal_first_sequence(),
            wal_first_sequence,
        });
    }
    if value.matching().wal_metadata_sequence() != wal_metadata_sequence {
        return Err(DurableRiskError::CheckpointMetadataBoundaryMismatch {
            checkpoint_metadata_sequence: value.matching().wal_metadata_sequence(),
            wal_metadata_sequence,
        });
    }
    if value.matching().definition() != definition {
        return Err(DurableRiskError::CheckpointDefinitionMismatch);
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
        return Err(DurableRiskError::CheckpointRiskProfileSetMismatch);
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "recovery inputs keep physical reader state, immutable metadata, limits, and checkpoint explicit"
)]
fn replay_risk_suffix(
    mut reader: StorageReader,
    first_data_frame: Option<JournalFrame>,
    reader_exhausted: bool,
    definition: InstrumentDefinition,
    profiles: &[AccountRiskDefinition],
    mut checkpoint: Option<RiskManagedCheckpoint>,
    limits: RiskManagedLimits,
    replay_start_sequence: u64,
) -> Result<RiskReplay, DurableRiskError> {
    let checkpointed_commands = checkpoint
        .as_ref()
        .map(|value| u64::try_from(value.matching().command_count()))
        .transpose()
        .map_err(|_| MatchingError::SequenceExhausted)?
        .unwrap_or(0);
    let mut managed = if checkpoint.is_none() {
        let mut value = RiskManagedOrderBook::try_with_limits(definition, limits)?;
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
            validate_checkpoint_prefix_frame(
                checkpoint.as_ref().expect("checkpoint presence was tested"),
                &frame,
            )?;
            continue;
        }
        if managed.is_none() {
            let value = checkpoint
                .take()
                .expect("first suffix frame follows checkpoint");
            managed = Some(RiskManagedOrderBook::from_checkpoint_with_limits(
                &value, limits,
            )?);
        }
        if replay_risk_frame(
            managed
                .as_mut()
                .expect("managed book is initialized before suffix replay"),
            &mut pending,
            &frame,
        )? {
            replayed_commands = replayed_commands
                .checked_add(1)
                .ok_or(MatchingError::SequenceExhausted)?;
        }
    }
    if let Some(value) = checkpoint {
        if last_wal_sequence < value.generation() {
            return Err(DurableRiskError::CheckpointAheadOfWal {
                checkpoint_sequence: value.generation(),
                wal_last_sequence: Some(last_wal_sequence),
            });
        }
        managed = Some(RiskManagedOrderBook::from_checkpoint_with_limits(
            &value, limits,
        )?);
    }
    Ok(RiskReplay {
        managed: managed.expect("managed book exists after WAL/checkpoint recovery"),
        pending,
        checkpointed_commands,
        replayed_commands,
    })
}

fn replay_risk_frame(
    managed: &mut RiskManagedOrderBook,
    pending: &mut Option<(u64, Command)>,
    frame: &JournalFrame,
) -> Result<bool, DurableRiskError> {
    match frame.kind() {
        RecordKind::Command => {
            if let Some((pending_sequence, _)) = pending {
                return Err(DurableRiskError::ConsecutiveCommands {
                    pending_sequence: *pending_sequence,
                    next_sequence: frame.sequence(),
                });
            }
            *pending = Some((frame.sequence(), frame.decode()?));
            Ok(false)
        }
        RecordKind::ExecutionReport => {
            let Some((command_sequence, command)) = pending.take() else {
                return Err(DurableRiskError::ReportWithoutCommand {
                    sequence: frame.sequence(),
                });
            };
            let persisted: ExecutionReport = frame.decode()?;
            let reproduced = managed.submit(command)?;
            if reproduced != persisted {
                return Err(DurableRiskError::ReplayDivergence {
                    command_sequence,
                    report_sequence: frame.sequence(),
                });
            }
            Ok(true)
        }
        kind => Err(DurableRiskError::UnexpectedRecord {
            sequence: frame.sequence(),
            kind,
        }),
    }
}

fn validate_checkpoint_prefix_frame(
    checkpoint: &RiskManagedCheckpoint,
    frame: &JournalFrame,
) -> Result<(), DurableRiskError> {
    let relative = frame
        .sequence()
        .checked_sub(checkpoint.matching().wal_metadata_sequence())
        .ok_or(DurableRiskError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        })?;
    if relative == 0 {
        return Err(DurableRiskError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        });
    }
    let history_index = usize::try_from((relative - 1) / 2).map_err(|_| {
        DurableRiskError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        }
    })?;
    let Some(expected) = checkpoint.matching().history().get(history_index) else {
        return Err(DurableRiskError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        });
    };
    let equal = if relative % 2 == 1 {
        frame.kind() == RecordKind::Command
            && frame
                .decode::<Command>()
                .is_ok_and(|actual| actual == expected.command())
    } else {
        frame.kind() == RecordKind::ExecutionReport
            && frame
                .decode::<ExecutionReport>()
                .is_ok_and(|actual| actual == *expected.report())
    };
    if !equal {
        return Err(DurableRiskError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        });
    }
    Ok(())
}

fn canonical_profiles(
    profiles: &[AccountRiskDefinition],
) -> Result<Vec<AccountRiskDefinition>, DurableRiskError> {
    let mut canonical = Vec::new();
    canonical.try_reserve_exact(profiles.len()).map_err(|_| {
        DurableRiskError::Matching(MatchingError::CapacityReservationFailed(
            crate::matching::MatchingCapacity::RiskAccounts,
        ))
    })?;
    canonical.extend_from_slice(profiles);
    canonical.sort_unstable_by_key(|value| value.account_id());
    for pair in canonical.windows(2) {
        if pair[0].account_id() == pair[1].account_id() {
            return Err(DurableRiskError::Risk(RiskError::DuplicateProfile(
                pair[0].account_id(),
            )));
        }
    }
    Ok(canonical)
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
) -> Result<(), DurableRiskError> {
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
                return Err(DurableRiskError::CheckpointPathConflictsWithWal {
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
                return Err(DurableRiskError::CheckpointPathConflictsWithWal {
                    path: conflict.clone(),
                });
            }
        }
    }
    Ok(())
}

const fn checkpoint_failure_requires_poison(error: &RiskManagedCheckpointError) -> bool {
    !error.is_operational_failure()
}

fn record_checkpoint_verification_failure(poison: &AtomicBool, error: &RiskManagedCheckpointError) {
    if checkpoint_failure_requires_poison(error) {
        poison.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod checkpoint_failure_tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crate::domain::{AssetId, InstrumentId, InstrumentVersion, Price, TimestampNs};
    use crate::instrument::{
        InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
        QuantityRules, ReserveOrderRules, TradingState,
    };
    use crate::matching::{OrderBookCheckpointError, OrderBookCheckpointResource};
    use crate::risk::{
        RiskManagedCheckpointError, RiskManagedCheckpointResource, RiskManagedOrderBook,
    };

    use super::{
        DurableRiskManagedCheckpointCapture, checkpoint_failure_requires_poison,
        record_checkpoint_verification_failure,
    };

    fn definition() -> InstrumentDefinition {
        InstrumentDefinition::new(InstrumentSpec {
            instrument_id: InstrumentId::new(1).unwrap(),
            version: InstrumentVersion::new(1).unwrap(),
            effective_from: TimestampNs::from_unix_nanos(0),
            symbol: InstrumentSymbol::new("DURABLE-RISK-STAGED-CHECKPOINT").unwrap(),
            kind: InstrumentKind::Future,
            base_asset_id: AssetId::new(1).unwrap(),
            quote_asset_id: AssetId::new(2).unwrap(),
            price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
            quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
            reserve: ReserveOrderRules::new(100).unwrap(),
            hidden_orders_supported: false,
            base_units_per_lot: 1,
            quote_units_per_price_unit: 1,
            trading_state: TradingState::Open,
        })
        .unwrap()
    }

    #[test]
    fn nested_and_direct_resource_pressure_are_retryable() {
        let matching = RiskManagedCheckpointError::Matching(
            OrderBookCheckpointError::ResourceReservationFailed {
                resource: OrderBookCheckpointResource::CaptureActiveOrders,
                maximum: 1,
            },
        );
        let accounts = RiskManagedCheckpointError::ResourceReservationFailed {
            resource: RiskManagedCheckpointResource::CaptureAccounts,
            maximum: 1,
        };
        let construction = RiskManagedCheckpointError::ConstructionFailed(
            crate::matching::MatchingError::CapacityReservationFailed(
                crate::matching::MatchingCapacity::RiskAccounts,
            ),
        );
        assert!(!checkpoint_failure_requires_poison(&matching));
        assert!(!checkpoint_failure_requires_poison(&accounts));
        assert!(!checkpoint_failure_requires_poison(&construction));
        assert!(checkpoint_failure_requires_poison(
            &RiskManagedCheckpointError::Invalid("corrupt coupled state".to_owned())
        ));
    }

    #[test]
    fn asynchronous_verification_latch_records_only_semantic_failure() {
        let poison = AtomicBool::new(false);
        let operational = RiskManagedCheckpointError::ResourceReservationFailed {
            resource: RiskManagedCheckpointResource::CaptureAccounts,
            maximum: 1,
        };
        record_checkpoint_verification_failure(&poison, &operational);
        assert!(!poison.load(Ordering::Acquire));

        let semantic = RiskManagedCheckpointError::Invalid("replay divergence".to_owned());
        record_checkpoint_verification_failure(&poison, &semantic);
        assert!(poison.load(Ordering::Acquire));
    }

    #[test]
    fn durable_capture_semantic_verification_failure_trips_its_origin_latch() {
        let managed = RiskManagedOrderBook::new(definition());
        let mut capture = managed.capture_checkpoint_candidate(1, 1, 1).unwrap();
        capture.corrupt_wal_lineage_for_test();
        let origin = Arc::new(AtomicBool::new(false));
        let durable = DurableRiskManagedCheckpointCapture {
            capture,
            origin: Arc::clone(&origin),
            cutover_epoch: 0,
            cutover_boundary: crate::durable_storage::StorageCutoverBoundary::for_test(1),
        };

        assert!(matches!(
            durable.verify(),
            Err(RiskManagedCheckpointError::Invalid(_))
        ));
        assert!(origin.load(Ordering::Acquire));
    }
}
