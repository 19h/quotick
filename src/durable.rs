//! Crash-recoverable orchestration of one matching shard and its WAL.
//!
//! The protocol appends and acknowledges a command before applying it to the
//! in-memory book, then appends its deterministic execution report. Recovery
//! either replays every verified command/report pair or proves a direct-state
//! checkpoint against the complete WAL prefix and replays only its suffix. One
//! possible final command interrupted before report persistence is completed.

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
    AccountControl, AccountControlSubmissionObservation, ActiveOrderObservation, CancelOrder,
    Command, CommandPreparation, ConditionalCommandOutcome, ConditionalExecutionPreflight,
    ConditionalExecutionPreparation, ConditionalOrderOutcome, ExecutionReport,
    ImmediateExecutionCurve, ImmediateExecutionCurveOutcome, ImmediateExecutionOutcome,
    ImmediateExecutionQuote, ImmediateExecutionSubmission, InvariantViolation, MassCancel,
    MassCancelObservation, MatchingError, NewOrder, OrderBook, OrderBookCheckpoint,
    OrderBookCheckpointCapture, OrderBookCheckpointError, OrderBookLimits, OrderBookQueryError,
    OrderExecution, PreparedCommand, ReplaceOrder, TradingStateControl,
    TradingStateControlSubmissionObservation, evaluate_conditional_execution,
};
use crate::snapshot::{
    CheckpointAnchor, CheckpointCutoverReceipt, CheckpointSlot, SnapshotError, SnapshotFile,
    SnapshotKind, SnapshotOptions, SnapshotReceipt,
};

/// Result of reconstructing a durable matching shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableRecoveryReport {
    /// Physical frame recovery performed by the journal.
    pub journal: StorageRecoveryReport,
    /// Completed commands restored directly from a verified semantic checkpoint.
    pub checkpointed_commands: u64,
    /// Commands reconstructed from complete command/report pairs.
    pub replayed_commands: u64,
    /// Whether a final durable command lacked a report and was completed.
    pub completed_dangling_command: bool,
}

/// A WAL-synchronized but not yet replay-verified durable matching capture.
///
/// This value is bound to one open [`DurableOrderBook`] incarnation and its
/// current physical WAL-prefix epoch. It has no codec or snapshot payload
/// implementation. Cloning it shares the immutable matching candidate and the
/// same asynchronous poison/origin token in `O(1)`.
#[derive(Clone)]
pub struct DurableOrderBookCheckpointCapture {
    capture: OrderBookCheckpointCapture,
    origin: Arc<AtomicBool>,
    cutover_epoch: u64,
    cutover_boundary: StorageCutoverBoundary,
}

impl DurableOrderBookCheckpointCapture {
    /// Returns the completed durable report boundary represented here.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.capture.generation()
    }

    /// Returns the final immutable-metadata sequence before command history.
    #[must_use]
    pub const fn wal_metadata_sequence(&self) -> u64 {
        self.capture.wal_metadata_sequence()
    }

    /// Returns the captured command/report cardinality.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.capture.command_count()
    }

    /// Returns the captured active-order cardinality.
    #[must_use]
    pub fn active_order_count(&self) -> usize {
        self.capture.active_order_count()
    }

    /// Returns whether two captures share the identical immutable candidate rows.
    #[must_use]
    pub fn shares_checkpoint_storage_with(&self, other: &Self) -> bool {
        self.capture.shares_checkpoint_storage_with(&other.capture)
    }

    /// Consumes the capture and verifies deterministic matching replay.
    ///
    /// Semantic failure atomically poisons the originating durable shard. A
    /// typed reservation or temporary replay-book construction failure leaves
    /// that shard usable, and a cloned capture may be retried.
    ///
    /// # Errors
    ///
    /// Returns the underlying matching-checkpoint verification failure.
    pub fn verify(self) -> Result<VerifiedDurableOrderBookCheckpoint, OrderBookCheckpointError> {
        let Self {
            capture,
            origin,
            cutover_epoch,
            cutover_boundary,
        } = self;
        match capture.verify() {
            Ok(checkpoint) => Ok(VerifiedDurableOrderBookCheckpoint {
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

/// A replay-verified checkpoint bound to one durable shard incarnation.
///
/// Only this type is accepted by standalone or prefix-retiring verified
/// publication. Its private origin and cutover epoch prevent publication
/// through another shard, a reopened shard, or the source shard after physical
/// WAL-prefix retirement.
#[derive(Clone)]
pub struct VerifiedDurableOrderBookCheckpoint {
    checkpoint: OrderBookCheckpoint,
    origin: Arc<AtomicBool>,
    cutover_epoch: u64,
    cutover_boundary: StorageCutoverBoundary,
}

impl VerifiedDurableOrderBookCheckpoint {
    /// Returns the completed report boundary represented here.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.checkpoint.generation()
    }

    /// Returns the final immutable-metadata sequence before command history.
    #[must_use]
    pub const fn wal_metadata_sequence(&self) -> u64 {
        self.checkpoint.wal_metadata_sequence()
    }

    /// Returns the verified command/report cardinality.
    #[must_use]
    pub fn command_count(&self) -> usize {
        self.checkpoint.command_count()
    }

    /// Returns the verified active-order cardinality.
    #[must_use]
    pub fn active_order_count(&self) -> usize {
        self.checkpoint.active_order_count()
    }

    /// Returns whether two values share the identical verified checkpoint rows.
    #[must_use]
    pub fn shares_checkpoint_storage_with(&self, other: &Self) -> bool {
        self.checkpoint.shares_order_storage_with(&other.checkpoint)
            && self
                .checkpoint
                .shares_history_storage_with(&other.checkpoint)
    }
}

/// Durable matching orchestration or replay failure.
#[derive(Debug)]
pub enum DurableError {
    /// Journal framing, recovery, codec, or I/O failure.
    Journal(JournalError),
    /// Segmented-journal inventory, rotation, framing, recovery, or I/O failure.
    SegmentedJournal(SegmentedJournalError),
    /// Matching operational error.
    Matching(MatchingError),
    /// Exact caller-owned order-book query output could not be constructed.
    OrderBookQuery(OrderBookQueryError),
    /// Reconstructed book structure violated an internal invariant.
    Invariant(InvariantViolation),
    /// Semantic checkpoint construction or restoration failed.
    Checkpoint(OrderBookCheckpointError),
    /// Snapshot framing, ownership, checksum, generation, or I/O failed.
    Snapshot(SnapshotError),
    /// The first journal frame was not an instrument definition.
    DefinitionRecordRequired {
        /// Journal frame sequence.
        sequence: u64,
        /// Payload kind found instead.
        actual: RecordKind,
    },
    /// The journal was empty after initialization, indicating external mutation.
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
    /// A record violated the matching-journal grammar.
    UnexpectedRecord {
        /// Journal frame sequence.
        sequence: u64,
        /// Unexpected payload kind.
        kind: RecordKind,
    },
    /// Another command appeared before the preceding command's report.
    ConsecutiveCommands {
        /// Sequence of the uncompleted command.
        pending_sequence: u64,
        /// Sequence of the next command.
        next_sequence: u64,
    },
    /// An execution report had no preceding command.
    ReportWithoutCommand {
        /// Journal frame sequence.
        sequence: u64,
    },
    /// Deterministic replay did not reproduce the persisted execution report.
    ReplayDivergence {
        /// Sequence of the replayed command frame.
        command_sequence: u64,
        /// Sequence of the persisted report frame.
        report_sequence: u64,
    },
    /// A checkpoint's first WAL sequence differs from the journal lineage.
    CheckpointWalLineageMismatch {
        /// First sequence retained by the checkpoint.
        checkpoint_first_sequence: u64,
        /// First sequence configured for the WAL.
        wal_first_sequence: u64,
    },
    /// A checkpoint definition differs from the requested and persisted definition.
    CheckpointDefinitionMismatch,
    /// A checkpoint command or report differs from the same WAL sequence.
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
    /// A compacted WAL cannot be opened without its anchor-selected checkpoint base path.
    CheckpointRequiredForCompactedWal {
        /// Physical sequence of the checkpoint anchor.
        sequence: u64,
    },
    /// A compacted WAL anchor names another semantic checkpoint type.
    CheckpointAnchorKindMismatch {
        /// Kind stored in the anchor.
        actual: SnapshotKind,
    },
    /// The selected checkpoint bytes do not equal the identity embedded in the WAL anchor.
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
    /// The runtime is unusable until reopened and recovered after an ambiguous transition.
    Poisoned,
}

impl fmt::Display for DurableError {
    #[allow(
        clippy::too_many_lines,
        reason = "one exhaustive typed durable-error formatter keeps every variant explicit"
    )]
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(error) => error.fmt(formatter),
            Self::SegmentedJournal(error) => error.fmt(formatter),
            Self::Matching(error) => error.fmt(formatter),
            Self::OrderBookQuery(error) => error.fmt(formatter),
            Self::Invariant(error) => error.fmt(formatter),
            Self::Checkpoint(error) => error.fmt(formatter),
            Self::Snapshot(error) => error.fmt(formatter),
            Self::DefinitionRecordRequired { sequence, actual } => write!(
                formatter,
                "first matching-journal frame at sequence {sequence} must be an instrument definition, found {actual:?}"
            ),
            Self::DefinitionRecordMissing => {
                formatter.write_str("matching journal has no instrument-definition frame")
            }
            Self::DefinitionMismatch {
                requested_instrument_id,
                requested_version,
                persisted_instrument_id,
                persisted_version,
            } => write!(
                formatter,
                "requested definition {requested_instrument_id}@{requested_version} does not exactly match persisted definition {persisted_instrument_id}@{persisted_version}"
            ),
            Self::UnexpectedRecord { sequence, kind } => {
                write!(
                    formatter,
                    "unexpected {kind:?} record at journal sequence {sequence}"
                )
            }
            Self::ConsecutiveCommands {
                pending_sequence,
                next_sequence,
            } => write!(
                formatter,
                "command at sequence {pending_sequence} has no report before command at {next_sequence}"
            ),
            Self::ReportWithoutCommand { sequence } => {
                write!(
                    formatter,
                    "execution report at sequence {sequence} has no command"
                )
            }
            Self::ReplayDivergence {
                command_sequence,
                report_sequence,
            } => write!(
                formatter,
                "replay of command at {command_sequence} diverged from report at {report_sequence}"
            ),
            Self::CheckpointWalLineageMismatch {
                checkpoint_first_sequence,
                wal_first_sequence,
            } => write!(
                formatter,
                "matching checkpoint WAL starts at {checkpoint_first_sequence}, but journal starts at {wal_first_sequence}"
            ),
            Self::CheckpointDefinitionMismatch => formatter.write_str(
                "matching checkpoint instrument definition differs from the WAL binding",
            ),
            Self::CheckpointPrefixDivergence { wal_sequence } => write!(
                formatter,
                "matching checkpoint differs from WAL frame {wal_sequence}"
            ),
            Self::CheckpointAheadOfWal {
                checkpoint_sequence,
                wal_last_sequence,
            } => write!(
                formatter,
                "matching checkpoint covers WAL sequence {checkpoint_sequence}, but verified WAL ends at {wal_last_sequence:?}"
            ),
            Self::CheckpointRequiredForCompactedWal { sequence } => write!(
                formatter,
                "matching WAL begins with checkpoint anchor {sequence}; a checkpoint base path is required"
            ),
            Self::CheckpointAnchorKindMismatch { actual } => write!(
                formatter,
                "matching WAL anchor references incompatible snapshot kind {actual:?}"
            ),
            Self::CheckpointAnchorMismatch { generation, slot } => write!(
                formatter,
                "matching WAL anchor at generation {generation} does not match checkpoint slot {slot:?}"
            ),
            Self::CheckpointPathConflictsWithWal { path } => write!(
                formatter,
                "matching checkpoint path {} conflicts with WAL storage",
                path.display()
            ),
            Self::CheckpointCaptureOriginMismatch => formatter.write_str(
                "verified matching checkpoint belongs to another durable shard incarnation",
            ),
            Self::CheckpointCaptureStale {
                captured_epoch,
                current_epoch,
            } => write!(
                formatter,
                "verified matching checkpoint was captured at WAL cutover epoch {captured_epoch}, but the shard is at epoch {current_epoch}"
            ),
            Self::CheckpointCutoverEpochExhausted => {
                formatter.write_str("durable matching checkpoint cutover epoch is exhausted")
            }
            Self::Poisoned => formatter
                .write_str("durable matching shard is poisoned and must be reopened for recovery"),
        }
    }
}

impl std::error::Error for DurableError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(error) => Some(error),
            Self::SegmentedJournal(error) => Some(error),
            Self::Matching(error) => Some(error),
            Self::OrderBookQuery(error) => Some(error),
            Self::Invariant(error) => Some(error),
            Self::Checkpoint(error) => Some(error),
            Self::Snapshot(error) => Some(error),
            _ => None,
        }
    }
}

impl From<JournalError> for DurableError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

impl From<SegmentedJournalError> for DurableError {
    fn from(error: SegmentedJournalError) -> Self {
        Self::SegmentedJournal(error)
    }
}

impl From<StorageError> for DurableError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Journal(error) => Self::Journal(error),
            StorageError::Segmented(error) => Self::SegmentedJournal(error),
        }
    }
}

impl From<MatchingError> for DurableError {
    fn from(error: MatchingError) -> Self {
        Self::Matching(error)
    }
}

impl From<OrderBookQueryError> for DurableError {
    fn from(error: OrderBookQueryError) -> Self {
        Self::OrderBookQuery(error)
    }
}

impl From<OrderBookCheckpointError> for DurableError {
    fn from(error: OrderBookCheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

impl From<SnapshotError> for DurableError {
    fn from(error: SnapshotError) -> Self {
        Self::Snapshot(error)
    }
}

/// One crash-recoverable, single-writer instrument matching shard.
#[derive(Debug)]
pub struct DurableOrderBook {
    book: OrderBook,
    journal: StorageWriter,
    wal_metadata_sequence: u64,
    checkpoint_slot: Option<CheckpointSlot>,
    checkpoint_verification_origin: Arc<AtomicBool>,
    cutover_epoch: u64,
    recovery: DurableRecoveryReport,
    poisoned: bool,
}

impl DurableOrderBook {
    /// Opens a matching journal, reconstructs the book, validates every stored
    /// report against deterministic replay, and completes a possible final
    /// command that had been durably written without its report.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for journal failure, invalid record grammar,
    /// matching failure, or replay divergence.
    pub fn open(
        path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        options: JournalOptions,
    ) -> Result<Self, DurableError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            OrderBookLimits::default(),
            StorageOptions::Single(options),
            None,
        )
    }

    /// Opens and replays a matching WAL under explicit finite matching limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] when storage/replay is invalid or recovered
    /// state exceeds the selected limits.
    pub fn open_with_limits(
        path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        limits: OrderBookLimits,
        options: JournalOptions,
    ) -> Result<Self, DurableError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            limits,
            StorageOptions::Single(options),
            None,
        )
    }

    /// Opens a matching WAL from a checksum-verified direct-state checkpoint,
    /// proves its complete command/report lineage against the WAL prefix, and
    /// deterministically replays only the suffix.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for snapshot, path-alias, definition, prefix,
    /// journal, matching, or reconstructed-state failure.
    pub fn open_with_checkpoint(
        path: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        options: JournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            OrderBookLimits::default(),
            StorageOptions::Single(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens a matching WAL and checkpoint under explicit finite matching limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for snapshot/storage/replay failure or selected
    /// limits insufficient for recovered state.
    pub fn open_with_checkpoint_and_limits(
        path: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        limits: OrderBookLimits,
        options: JournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableError> {
        Self::open_storage(
            path.as_ref(),
            definition,
            limits,
            StorageOptions::Single(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens a matching shard over an automatically rotating WAL directory.
    ///
    /// Recovery streams all strictly verified closed segments and the active
    /// segment as one globally sequenced journal.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for segmented storage, grammar, matching, or
    /// deterministic replay failure.
    pub fn open_segmented(
        directory: impl AsRef<Path>,
        definition: InstrumentDefinition,
        options: SegmentedJournalOptions,
    ) -> Result<Self, DurableError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            OrderBookLimits::default(),
            StorageOptions::Segmented(options),
            None,
        )
    }

    /// Opens a segmented matching WAL under explicit finite matching limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for storage/replay failure or insufficient limits.
    pub fn open_segmented_with_limits(
        directory: impl AsRef<Path>,
        definition: InstrumentDefinition,
        limits: OrderBookLimits,
        options: SegmentedJournalOptions,
    ) -> Result<Self, DurableError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            limits,
            StorageOptions::Segmented(options),
            None,
        )
    }

    /// Opens a segmented matching WAL from a verified semantic checkpoint and
    /// replays only command/report frames after its exact global sequence boundary.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for snapshot, managed-path, prefix, segmented
    /// storage, matching, or reconstructed-state failure.
    pub fn open_segmented_with_checkpoint(
        directory: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        options: SegmentedJournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableError> {
        Self::open_storage(
            directory.as_ref(),
            definition,
            OrderBookLimits::default(),
            StorageOptions::Segmented(options),
            Some((checkpoint_path.as_ref(), snapshot_options)),
        )
    }

    /// Opens segmented matching storage and a checkpoint under explicit limits.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for snapshot/storage/replay failure or selected
    /// limits insufficient for recovered state.
    pub fn open_segmented_with_checkpoint_and_limits(
        directory: impl AsRef<Path>,
        checkpoint_path: impl AsRef<Path>,
        definition: InstrumentDefinition,
        limits: OrderBookLimits,
        options: SegmentedJournalOptions,
        snapshot_options: SnapshotOptions,
    ) -> Result<Self, DurableError> {
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
        limits: OrderBookLimits,
        options: StorageOptions,
        checkpoint_source: Option<(&Path, SnapshotOptions)>,
    ) -> Result<Self, DurableError> {
        let opened = open_matching_journal(path, definition, options, checkpoint_source)?;
        let mut journal = opened.journal;
        let replay = replay_matching_suffix(
            opened.reader,
            opened.checkpoint,
            definition,
            limits,
            opened.wal_first_sequence,
        )?;
        let mut book = replay.book;
        let completed_dangling_command = if let Some((_, command)) = replay.pending {
            let report = book.submit(command)?;
            journal.append(&report)?;
            true
        } else {
            false
        };
        book.validate().map_err(DurableError::Invariant)?;
        Ok(Self {
            book,
            journal,
            wal_metadata_sequence: opened.wal_metadata_sequence,
            checkpoint_slot: opened.checkpoint_slot,
            checkpoint_verification_origin: Arc::new(AtomicBool::new(false)),
            cutover_epoch: 0,
            recovery: DurableRecoveryReport {
                journal: opened.recovery,
                checkpointed_commands: replay.checkpointed_commands,
                replayed_commands: replay.replayed_commands,
                completed_dangling_command,
            },
            poisoned: false,
        })
    }

    /// Returns recovery and replay statistics captured at open.
    #[must_use]
    pub const fn recovery(&self) -> DurableRecoveryReport {
        self.recovery
    }

    /// Returns the reconstructed read-only order book.
    #[must_use]
    pub const fn book(&self) -> &OrderBook {
        &self.book
    }

    /// Returns whether local failure or asynchronous checkpoint verification
    /// has made this shard unusable until reopen/recovery.
    #[must_use]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned || self.checkpoint_verification_origin.load(Ordering::Acquire)
    }

    /// Durably sequences a command, applies it, and durably records its trace.
    ///
    /// Exact retries return the cached replay report without adding WAL frames.
    /// Identifier collisions and capacity exhaustion are detected before the
    /// command frame is written.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for matching preparation or journal failure. A
    /// failure after the command frame is acknowledged poisons this instance;
    /// reopening completes recovery deterministically.
    pub fn submit(&mut self, command: Command) -> Result<ExecutionReport, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        let prepared = match self.book.prepare(command)? {
            CommandPreparation::Replay(report) => {
                if self.is_poisoned() {
                    return Err(DurableError::Poisoned);
                }
                return Ok(report);
            }
            CommandPreparation::Ready(prepared) => prepared,
        };
        self.commit_prepared(prepared)
    }

    /// Durably observes and conditionally submits one complete new order.
    ///
    /// Replay and core rejection bypass `accept`. Otherwise the predicate
    /// borrows an active private quote or explicit dormant-stop state before
    /// any WAL append. Decline or unwind changes neither WAL nor semantic
    /// state. Acceptance appends the command, commits the same preparation,
    /// and appends its report under the ordinary poison/recovery protocol.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, matching preparation, journal, or
    /// commit failure. A failure after command acknowledgement poisons the
    /// instance for deterministic reopen recovery.
    pub fn submit_new_order_if(
        &mut self,
        order: NewOrder,
        accept: impl FnOnce(&OrderExecution<ImmediateExecutionQuote>) -> bool,
    ) -> Result<ConditionalOrderOutcome<ImmediateExecutionQuote>, DurableError> {
        self.submit_conditional_new_order(order, |_, observation| Ok(observation), accept)
    }

    /// Durably constructs an active private execution curve and conditionally
    /// submits one complete new order.
    ///
    /// Replay and core rejection bypass allocation and `accept`; a valid stop
    /// exposes explicit dormant state without allocation. Allocation failure,
    /// decline, or unwind precedes WAL and semantic mutation. Acceptance writes
    /// the command, commits the observation-bound preparation, and writes the
    /// report under the ordinary poison/recovery protocol.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, matching preparation, exact curve
    /// construction, journal, or commit failure.
    pub fn try_submit_new_order_curve_if(
        &mut self,
        order: NewOrder,
        accept: impl FnOnce(&OrderExecution<ImmediateExecutionCurve>) -> bool,
    ) -> Result<ConditionalOrderOutcome<ImmediateExecutionCurve>, DurableError> {
        self.submit_conditional_new_order(
            order,
            |book, observation| Ok(book.try_order_execution_curve(observation)?),
            accept,
        )
    }

    /// Durably observes and conditionally replaces one active order.
    ///
    /// Replay and core rejection bypass `accept`. Otherwise the predicate
    /// borrows an active private quote or explicit dormant-stop state before
    /// any WAL append. Decline or unwind changes neither WAL nor semantic
    /// state. Acceptance persists and commits the same preparation.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, matching preparation, journal, or
    /// commit failure. A failure after command acknowledgement poisons the
    /// instance for deterministic reopen recovery.
    pub fn submit_replace_order_if(
        &mut self,
        replacement: ReplaceOrder,
        accept: impl FnOnce(&OrderExecution<ImmediateExecutionQuote>) -> bool,
    ) -> Result<ConditionalOrderOutcome<ImmediateExecutionQuote>, DurableError> {
        self.submit_conditional_replace_order(replacement, |_, observation| Ok(observation), accept)
    }

    /// Durably constructs an active private execution curve and conditionally
    /// replaces one order.
    ///
    /// Replay and core rejection bypass allocation and `accept`; a dormant
    /// stop-limit exposes explicit dormant state without allocation.
    /// Allocation failure, decline, or unwind precedes WAL and semantic
    /// mutation. Acceptance persists the observation-bound preparation.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, matching preparation, exact curve
    /// construction, journal, or commit failure.
    pub fn try_submit_replace_order_curve_if(
        &mut self,
        replacement: ReplaceOrder,
        accept: impl FnOnce(&OrderExecution<ImmediateExecutionCurve>) -> bool,
    ) -> Result<ConditionalOrderOutcome<ImmediateExecutionCurve>, DurableError> {
        self.submit_conditional_replace_order(
            replacement,
            |book, observation| Ok(book.try_order_execution_curve(observation)?),
            accept,
        )
    }

    /// Durably observes and conditionally cancels one active order.
    ///
    /// Replay and core rejection bypass `accept`. Otherwise the predicate
    /// borrows validated, provenance-bound resting or dormant state before any
    /// WAL append. Decline or unwind changes neither WAL nor semantic state.
    /// Acceptance persists and commits the same preparation.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, matching preparation, private
    /// observation, journal, or commit failure. A failure after command
    /// acknowledgement poisons the instance for deterministic reopen recovery.
    pub fn submit_cancel_order_if(
        &mut self,
        cancellation: CancelOrder,
        accept: impl FnOnce(&ActiveOrderObservation) -> bool,
    ) -> Result<ConditionalCommandOutcome<ActiveOrderObservation>, DurableError> {
        self.submit_conditional_cancel_order(cancellation, |_, observation| Ok(observation), accept)
    }

    /// Durably materializes and conditionally applies one account mass cancel.
    ///
    /// Replay and core rejection bypass selection and `accept`. Otherwise the
    /// predicate borrows complete selected resting and dormant-stop states
    /// before WAL append. Output failure, decline, or unwind changes neither
    /// WAL nor semantic state. Acceptance persists and commits the preparation
    /// containing the same canonical selected identifiers.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, preparation, selected-output,
    /// journal, or commit failure. A post-acknowledgement failure poisons the
    /// instance for deterministic reopen recovery.
    pub fn try_submit_mass_cancel_if(
        &mut self,
        command: MassCancel,
        accept: impl FnOnce(&MassCancelObservation) -> bool,
    ) -> Result<ConditionalCommandOutcome<MassCancelObservation>, DurableError> {
        self.submit_conditional_mass_cancel(command, |_, observation| Ok(observation), accept)
    }

    /// Durably observes and conditionally applies one account control.
    ///
    /// Replay and core rejection bypass observation and `accept`. Otherwise
    /// the predicate borrows the frozen current fence and complete canonical
    /// block-and-cancel selection before WAL append. Enable observations are
    /// selection-free. Observation failure, decline, or unwind changes neither
    /// WAL nor semantic state.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, preparation, observation, journal,
    /// or commit failure. A post-acknowledgement failure poisons the instance
    /// for deterministic reopen recovery.
    pub fn try_submit_account_control_if(
        &mut self,
        command: AccountControl,
        accept: impl FnOnce(&AccountControlSubmissionObservation) -> bool,
    ) -> Result<ConditionalCommandOutcome<AccountControlSubmissionObservation>, DurableError> {
        self.submit_conditional_account_control(command, |_, observation| Ok(observation), accept)
    }

    /// Durably observes and conditionally applies one trading-state control.
    ///
    /// Replay and core rejection bypass observation and `accept`. Otherwise
    /// the predicate borrows the frozen current state and complete canonical
    /// transition-and-cancel selection before WAL append. Transition-only
    /// observations are selection-free. Observation failure, decline, or
    /// unwind changes neither WAL nor semantic state.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, preparation, observation, journal,
    /// or commit failure. A post-acknowledgement failure poisons the instance
    /// for deterministic reopen recovery.
    pub fn try_submit_trading_state_control_if(
        &mut self,
        command: TradingStateControl,
        accept: impl FnOnce(&TradingStateControlSubmissionObservation) -> bool,
    ) -> Result<ConditionalCommandOutcome<TradingStateControlSubmissionObservation>, DurableError>
    {
        self.submit_conditional_trading_state_control(
            command,
            |_, observation| Ok(observation),
            accept,
        )
    }

    /// Durably and atomically quotes and conditionally submits one canonical IOC order.
    ///
    /// Replay and core rejection bypass `accept`. A declined quote is returned
    /// before any WAL append or semantic mutation. Acceptance appends the
    /// canonical command, commits the exact preparation evaluated by the
    /// predicate, then appends its report under the ordinary poison protocol.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, matching preparation, journal, or
    /// commit failure. A failure after command acknowledgement poisons the
    /// instance for deterministic reopen recovery.
    pub fn submit_immediate_execution_if(
        &mut self,
        submission: ImmediateExecutionSubmission,
        accept: impl FnOnce(ImmediateExecutionQuote) -> bool,
    ) -> Result<ImmediateExecutionOutcome, DurableError> {
        self.submit_conditional_immediate_execution(
            submission,
            |_, quote| Ok(quote),
            |quote| accept(*quote),
        )
        .map(Into::into)
    }

    /// Durably constructs an exact private execution curve and conditionally
    /// submits one canonical IOC order.
    ///
    /// Replay and core rejection bypass curve allocation and `accept`.
    /// Allocation failure, decline, or predicate unwind occurs before WAL or
    /// semantic mutation. Acceptance writes the canonical command, commits the
    /// preparation bound to the curve, and writes its report under the ordinary
    /// poison/recovery protocol.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, matching preparation, exact curve
    /// construction, journal, or commit failure. A failure after command
    /// acknowledgement poisons the instance for deterministic reopen recovery.
    pub fn try_submit_immediate_execution_curve_if(
        &mut self,
        submission: ImmediateExecutionSubmission,
        accept: impl FnOnce(&ImmediateExecutionCurve) -> bool,
    ) -> Result<ImmediateExecutionCurveOutcome, DurableError> {
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
        observe: impl FnOnce(&OrderBook, ImmediateExecutionQuote) -> Result<T, DurableError>,
        accept: impl FnOnce(&T) -> bool,
    ) -> Result<ConditionalCommandOutcome<T>, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        let preflight = self
            .book
            .prepare_conditional_immediate_execution(submission)?;
        self.submit_conditional_execution(preflight, observe, accept)
    }

    fn submit_conditional_new_order<T>(
        &mut self,
        order: NewOrder,
        observe: impl FnOnce(
            &OrderBook,
            OrderExecution<ImmediateExecutionQuote>,
        ) -> Result<T, DurableError>,
        accept: impl FnOnce(&T) -> bool,
    ) -> Result<ConditionalCommandOutcome<T>, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        let preflight = self.book.prepare_conditional_new_order(order)?;
        self.submit_conditional_execution(preflight, observe, accept)
    }

    fn submit_conditional_replace_order<T>(
        &mut self,
        replacement: ReplaceOrder,
        observe: impl FnOnce(
            &OrderBook,
            OrderExecution<ImmediateExecutionQuote>,
        ) -> Result<T, DurableError>,
        accept: impl FnOnce(&T) -> bool,
    ) -> Result<ConditionalCommandOutcome<T>, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        let preflight = self.book.prepare_conditional_replace_order(replacement)?;
        self.submit_conditional_execution(preflight, observe, accept)
    }

    fn submit_conditional_cancel_order<T>(
        &mut self,
        cancellation: CancelOrder,
        observe: impl FnOnce(&OrderBook, ActiveOrderObservation) -> Result<T, DurableError>,
        accept: impl FnOnce(&T) -> bool,
    ) -> Result<ConditionalCommandOutcome<T>, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        let preflight = self
            .book
            .prepare_conditional_cancel_order::<DurableError>(cancellation)?;
        self.submit_conditional_execution(preflight, observe, accept)
    }

    fn submit_conditional_mass_cancel<T>(
        &mut self,
        command: MassCancel,
        observe: impl FnOnce(&OrderBook, MassCancelObservation) -> Result<T, DurableError>,
        accept: impl FnOnce(&T) -> bool,
    ) -> Result<ConditionalCommandOutcome<T>, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        let preflight = self
            .book
            .prepare_conditional_mass_cancel::<DurableError>(command)?;
        self.submit_conditional_execution(preflight, observe, accept)
    }

    fn submit_conditional_account_control<T>(
        &mut self,
        command: AccountControl,
        observe: impl FnOnce(&OrderBook, AccountControlSubmissionObservation) -> Result<T, DurableError>,
        accept: impl FnOnce(&T) -> bool,
    ) -> Result<ConditionalCommandOutcome<T>, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        let preflight = self
            .book
            .prepare_conditional_account_control::<DurableError>(command)?;
        self.submit_conditional_execution(preflight, observe, accept)
    }

    fn submit_conditional_trading_state_control<T>(
        &mut self,
        command: TradingStateControl,
        observe: impl FnOnce(
            &OrderBook,
            TradingStateControlSubmissionObservation,
        ) -> Result<T, DurableError>,
        accept: impl FnOnce(&T) -> bool,
    ) -> Result<ConditionalCommandOutcome<T>, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        let preflight = self
            .book
            .prepare_conditional_trading_state_control::<DurableError>(command)?;
        self.submit_conditional_execution(preflight, observe, accept)
    }

    fn submit_conditional_execution<T, U>(
        &mut self,
        preflight: ConditionalExecutionPreflight<T>,
        observe: impl FnOnce(&OrderBook, T) -> Result<U, DurableError>,
        accept: impl FnOnce(&U) -> bool,
    ) -> Result<ConditionalCommandOutcome<U>, DurableError> {
        match evaluate_conditional_execution(preflight, |value| observe(&self.book, value), accept)?
        {
            ConditionalExecutionPreparation::Complete(decision) => {
                if self.is_poisoned() {
                    return Err(DurableError::Poisoned);
                }
                Ok(decision)
            }
            ConditionalExecutionPreparation::Commit {
                prepared,
                observation,
            } => {
                let report = self.commit_prepared(prepared)?;
                let retained_observation = if report.replayed { None } else { observation };
                Ok(ConditionalCommandOutcome::Reported {
                    observation: retained_observation,
                    report,
                })
            }
        }
    }

    fn commit_prepared(
        &mut self,
        prepared: PreparedCommand,
    ) -> Result<ExecutionReport, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        if let Err(error) = self.journal.append(&prepared.command()) {
            self.poisoned = self.journal.is_poisoned();
            return Err(error.into());
        }
        let report = match self.book.commit(prepared) {
            Ok(report) => report,
            Err(error) => {
                self.poisoned = true;
                return Err(DurableError::Matching(error));
            }
        };
        if let Err(error) = self.journal.append(&report) {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(report)
    }

    /// Synchronizes the matching WAL and captures one immutable checkpoint
    /// candidate without deterministic history replay.
    ///
    /// The returned value is bound to this exact open shard incarnation and
    /// current physical-prefix epoch. Ordinary command/report suffix growth is
    /// permitted while verification runs; WAL-prefix cutover is not.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, WAL synchronization failure, or a
    /// typed live checkpoint capture/lineage failure. Operational checkpoint
    /// allocation/construction failures leave the shard usable; semantic
    /// contradictions poison it.
    pub fn capture_checkpoint_candidate(
        &mut self,
    ) -> Result<DurableOrderBookCheckpointCapture, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        self.sync_all()?;
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        let cutover_boundary = self.journal.cutover_boundary()?;
        let wal_sequence = cutover_boundary.wal_sequence();
        let capture = match self
            .book
            .capture_checkpoint_candidate(self.wal_metadata_sequence, wal_sequence)
        {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = checkpoint_failure_requires_poison(&error);
                return Err(DurableError::Checkpoint(error));
            }
        };
        Ok(DurableOrderBookCheckpointCapture {
            capture,
            origin: Arc::clone(&self.checkpoint_verification_origin),
            cutover_epoch: self.cutover_epoch,
            cutover_boundary,
        })
    }

    /// Synchronizes the matching WAL, audits direct state, and atomically
    /// replaces a checksum-protected semantic checkpoint.
    ///
    /// Checkpoint output cannot alias the WAL, its lease/pending namespace, or
    /// any path inside a managed segmented-journal directory.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, path conflict, WAL barrier,
    /// matching invariant, checkpoint codec, snapshot ownership, or I/O failure.
    /// Typed checkpoint resource and temporary-book construction failures leave
    /// the shard unpoisoned for retry; semantic checkpoint contradictions poison it.
    pub fn write_checkpoint(
        &mut self,
        path: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<SnapshotReceipt, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        validate_checkpoint_path(self.journal.path(), self.journal.layout(), path.as_ref())?;
        let capture = self.capture_checkpoint_candidate()?;
        let verified = match capture.verify() {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = checkpoint_failure_requires_poison(&error);
                return Err(DurableError::Checkpoint(error));
            }
        };
        self.write_verified_checkpoint(path, &verified, options)
    }

    /// Publishes a replay-verified capture through its originating durable shard.
    ///
    /// Ordinary suffix growth after capture is allowed because the checkpoint
    /// remains an exact older WAL prefix. Publication rejects another/reopened
    /// shard and any source shard whose physical prefix was compacted after
    /// capture. This method writes a standalone checkpoint and does not retire
    /// a WAL prefix.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, origin/epoch mismatch, path conflict,
    /// checkpoint-ahead contradiction, snapshot ownership, codec, or I/O failure.
    pub fn write_verified_checkpoint(
        &mut self,
        path: impl AsRef<Path>,
        verified: &VerifiedDurableOrderBookCheckpoint,
        options: SnapshotOptions,
    ) -> Result<SnapshotReceipt, DurableError> {
        validate_checkpoint_path(self.journal.path(), self.journal.layout(), path.as_ref())?;
        self.validate_verified_checkpoint(verified)?;
        SnapshotFile::write(path, &verified.checkpoint, options).map_err(Into::into)
    }

    /// Publishes a replay-verified checkpoint and atomically retires its WAL prefix.
    ///
    /// Any command/report suffix appended after capture is synchronized and
    /// migrated unchanged behind the new checkpoint anchor. Verification is not
    /// repeated under writer exclusion. Successful publication advances the
    /// process-local cutover epoch and invalidates every older capture.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, origin/epoch or WAL-boundary drift,
    /// path conflict, snapshot persistence, suffix migration, or physical
    /// publication failure.
    pub fn compact_verified_checkpoint(
        &mut self,
        checkpoint_base: impl AsRef<Path>,
        verified: &VerifiedDurableOrderBookCheckpoint,
        options: SnapshotOptions,
    ) -> Result<CheckpointCutoverReceipt, DurableError> {
        validate_checkpoint_path(
            self.journal.path(),
            self.journal.layout(),
            checkpoint_base.as_ref(),
        )?;
        self.compact_verified_checkpoint_validated(checkpoint_base.as_ref(), verified, options)
    }

    /// Writes the inactive checkpoint slot, synchronizes it, and atomically
    /// retires the selected WAL prefix behind an exact checkpoint anchor.
    ///
    /// Slot A/B alternation keeps the checkpoint referenced by the current WAL
    /// intact until the layout-specific WAL selector and its directory barrier
    /// complete. The resulting WAL retains the retired physical boundary as its
    /// anchor sequence and appends the next command at `anchor sequence + 1`.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for poison, path conflict, WAL/checkpoint
    /// validation, snapshot persistence, or physical cutover failure.
    /// Pre-semantic checkpoint resource/construction failure performs no cutover
    /// mutation and leaves the shard unpoisoned.
    pub fn compact_to_checkpoint(
        &mut self,
        checkpoint_base: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<CheckpointCutoverReceipt, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
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
                return Err(DurableError::Checkpoint(error));
            }
        };
        self.compact_verified_checkpoint_validated(checkpoint_base.as_ref(), &verified, options)
    }

    fn validate_verified_checkpoint(
        &self,
        verified: &VerifiedDurableOrderBookCheckpoint,
    ) -> Result<u64, DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        if !Arc::ptr_eq(&self.checkpoint_verification_origin, &verified.origin) {
            return Err(DurableError::CheckpointCaptureOriginMismatch);
        }
        if verified.cutover_epoch != self.cutover_epoch {
            return Err(DurableError::CheckpointCaptureStale {
                captured_epoch: verified.cutover_epoch,
                current_epoch: self.cutover_epoch,
            });
        }
        let wal_sequence = self
            .journal
            .next_sequence()
            .checked_sub(1)
            .ok_or(MatchingError::SequenceExhausted)?;
        if verified.checkpoint.wal_metadata_sequence() != self.wal_metadata_sequence
            || verified.checkpoint.generation() != verified.cutover_boundary.wal_sequence()
            || verified.checkpoint.generation() > wal_sequence
        {
            return Err(DurableError::CheckpointAheadOfWal {
                checkpoint_sequence: verified.checkpoint.generation(),
                wal_last_sequence: Some(wal_sequence),
            });
        }
        Ok(wal_sequence)
    }

    fn compact_verified_checkpoint_validated(
        &mut self,
        checkpoint_base: &Path,
        verified: &VerifiedDurableOrderBookCheckpoint,
        options: SnapshotOptions,
    ) -> Result<CheckpointCutoverReceipt, DurableError> {
        self.validate_verified_checkpoint(verified)?;
        let next_cutover_epoch = self
            .cutover_epoch
            .checked_add(1)
            .ok_or(DurableError::CheckpointCutoverEpochExhausted)?;
        let slot = self
            .checkpoint_slot
            .map_or(CheckpointSlot::A, CheckpointSlot::alternate);
        let slot_path = SnapshotFile::slot_path(checkpoint_base, slot);
        validate_checkpoint_path(self.journal.path(), self.journal.layout(), &slot_path)?;
        let snapshot = SnapshotFile::write(&slot_path, &verified.checkpoint, options)?;
        let anchor = CheckpointAnchor::new(
            SnapshotKind::MatchingCheckpoint,
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
    /// Returns [`DurableError`] and poisons the shard when synchronization fails.
    pub fn sync_all(&mut self) -> Result<(), DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        if let Err(error) = self.journal.sync_all() {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(())
    }

    /// Synchronizes all journal data and metadata, then releases exclusive
    /// writer ownership.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] if the shard is poisoned or final journal
    /// synchronization/lease release fails.
    pub fn close(self) -> Result<(), DurableError> {
        if self.is_poisoned() {
            return Err(DurableError::Poisoned);
        }
        self.journal.close().map_err(Into::into)
    }
}

struct OpenedMatchingJournal {
    journal: StorageWriter,
    reader: StorageReader,
    recovery: StorageRecoveryReport,
    checkpoint: Option<OrderBookCheckpoint>,
    wal_first_sequence: u64,
    wal_metadata_sequence: u64,
    checkpoint_slot: Option<CheckpointSlot>,
}

fn open_matching_journal(
    path: &Path,
    definition: InstrumentDefinition,
    options: StorageOptions,
    checkpoint_source: Option<(&Path, SnapshotOptions)>,
) -> Result<OpenedMatchingJournal, DurableError> {
    if let Some((checkpoint_path, _)) = checkpoint_source {
        validate_checkpoint_path(path, storage_layout(options), checkpoint_path)?;
    }
    let mut journal = StorageWriter::open(path, options)?;
    let recovery = journal.recovery();
    if recovery.last_sequence.is_none() {
        journal.append(&definition)?;
    }
    let mut reader = StorageReader::open(path, options)?;
    let Some(first_frame) = reader.next().transpose()? else {
        return Err(DurableError::DefinitionRecordMissing);
    };
    let (checkpoint, wal_metadata_sequence, checkpoint_slot) = match first_frame.kind() {
        RecordKind::InstrumentDefinition => {
            validate_definition_frame(&first_frame, definition)?;
            let checkpoint = checkpoint_source
                .map(|(checkpoint_path, snapshot_options)| {
                    SnapshotFile::read::<OrderBookCheckpoint>(checkpoint_path, snapshot_options)
                })
                .transpose()?;
            if let Some(value) = &checkpoint {
                if value.wal_metadata_sequence() != first_frame.sequence() {
                    return Err(DurableError::CheckpointWalLineageMismatch {
                        checkpoint_first_sequence: value.wal_metadata_sequence(),
                        wal_first_sequence: first_frame.sequence(),
                    });
                }
                if value.definition() != definition {
                    return Err(DurableError::CheckpointDefinitionMismatch);
                }
            }
            (checkpoint, first_frame.sequence(), None)
        }
        RecordKind::CheckpointAnchor => {
            let anchor: CheckpointAnchor = first_frame.decode()?;
            if anchor.kind() != SnapshotKind::MatchingCheckpoint {
                return Err(DurableError::CheckpointAnchorKindMismatch {
                    actual: anchor.kind(),
                });
            }
            let Some((checkpoint_base, snapshot_options)) = checkpoint_source else {
                return Err(DurableError::CheckpointRequiredForCompactedWal {
                    sequence: first_frame.sequence(),
                });
            };
            if first_frame.sequence() != anchor.wal_sequence() {
                return Err(DurableError::CheckpointAnchorMismatch {
                    generation: anchor.generation(),
                    slot: anchor.slot(),
                });
            }
            let slot_path = SnapshotFile::slot_path(checkpoint_base, anchor.slot());
            validate_checkpoint_path(path, storage_layout(options), &slot_path)?;
            let (checkpoint, receipt) = SnapshotFile::read_with_receipt::<OrderBookCheckpoint>(
                &slot_path,
                snapshot_options,
            )?;
            if !anchor.matches_receipt(receipt)
                || checkpoint.generation() != anchor.generation()
                || checkpoint.definition() != definition
            {
                return Err(DurableError::CheckpointAnchorMismatch {
                    generation: anchor.generation(),
                    slot: anchor.slot(),
                });
            }
            let wal_metadata_sequence = checkpoint.wal_metadata_sequence();
            (Some(checkpoint), wal_metadata_sequence, Some(anchor.slot()))
        }
        actual => {
            return Err(DurableError::DefinitionRecordRequired {
                sequence: first_frame.sequence(),
                actual,
            });
        }
    };
    Ok(OpenedMatchingJournal {
        journal,
        reader,
        recovery,
        checkpoint,
        wal_first_sequence: first_frame.sequence(),
        wal_metadata_sequence,
        checkpoint_slot,
    })
}

fn validate_definition_frame(
    frame: &JournalFrame,
    definition: InstrumentDefinition,
) -> Result<(), DurableError> {
    if frame.kind() != RecordKind::InstrumentDefinition {
        return Err(DurableError::DefinitionRecordRequired {
            sequence: frame.sequence(),
            actual: frame.kind(),
        });
    }
    let persisted: InstrumentDefinition = frame.decode()?;
    if persisted != definition {
        return Err(DurableError::DefinitionMismatch {
            requested_instrument_id: definition.instrument_id(),
            requested_version: definition.version(),
            persisted_instrument_id: persisted.instrument_id(),
            persisted_version: persisted.version(),
        });
    }
    Ok(())
}

struct MatchingReplay {
    book: OrderBook,
    pending: Option<(u64, Command)>,
    checkpointed_commands: u64,
    replayed_commands: u64,
}

fn replay_matching_suffix(
    reader: StorageReader,
    mut checkpoint: Option<OrderBookCheckpoint>,
    definition: InstrumentDefinition,
    limits: OrderBookLimits,
    wal_first_sequence: u64,
) -> Result<MatchingReplay, DurableError> {
    let checkpointed_commands = checkpoint
        .as_ref()
        .map(|value| u64::try_from(value.command_count()))
        .transpose()
        .map_err(|_| MatchingError::SequenceExhausted)?
        .unwrap_or(0);
    let mut book = if checkpoint.is_none() {
        Some(OrderBook::try_with_limits(definition, limits)?)
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
            validate_checkpoint_prefix_frame(
                checkpoint.as_ref().expect("checkpoint presence was tested"),
                &frame,
            )?;
            continue;
        }
        if book.is_none() {
            let restored = OrderBook::from_checkpoint_with_limits(
                checkpoint
                    .as_ref()
                    .expect("first suffix frame follows a checkpoint"),
                limits,
            )?;
            book = Some(restored);
            checkpoint = None;
        }
        if replay_matching_frame(
            book.as_mut()
                .expect("book is initialized before suffix replay"),
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
            return Err(DurableError::CheckpointAheadOfWal {
                checkpoint_sequence: value.generation(),
                wal_last_sequence: Some(last_wal_sequence),
            });
        }
        book = Some(OrderBook::from_checkpoint_with_limits(&value, limits)?);
    }
    Ok(MatchingReplay {
        book: book.expect("book exists after WAL/checkpoint recovery"),
        pending,
        checkpointed_commands,
        replayed_commands,
    })
}

fn replay_matching_frame(
    book: &mut OrderBook,
    pending: &mut Option<(u64, Command)>,
    frame: &JournalFrame,
) -> Result<bool, DurableError> {
    match frame.kind() {
        RecordKind::Command => {
            if let Some((pending_sequence, _)) = pending {
                return Err(DurableError::ConsecutiveCommands {
                    pending_sequence: *pending_sequence,
                    next_sequence: frame.sequence(),
                });
            }
            *pending = Some((frame.sequence(), frame.decode()?));
            Ok(false)
        }
        RecordKind::ExecutionReport => {
            let Some((command_sequence, command)) = pending.take() else {
                return Err(DurableError::ReportWithoutCommand {
                    sequence: frame.sequence(),
                });
            };
            let persisted: ExecutionReport = frame.decode()?;
            let reproduced = book.submit(command)?;
            if reproduced != persisted {
                return Err(DurableError::ReplayDivergence {
                    command_sequence,
                    report_sequence: frame.sequence(),
                });
            }
            Ok(true)
        }
        RecordKind::LedgerEntry
        | RecordKind::InstrumentDefinition
        | RecordKind::AccountRiskDefinition
        | RecordKind::LedgerCorrection
        | RecordKind::LedgerBatch
        | RecordKind::CheckpointAnchor
        | RecordKind::CallAuctionCommand
        | RecordKind::CallAuctionExecutionReport => Err(DurableError::UnexpectedRecord {
            sequence: frame.sequence(),
            kind: frame.kind(),
        }),
    }
}

fn validate_checkpoint_prefix_frame(
    checkpoint: &OrderBookCheckpoint,
    frame: &JournalFrame,
) -> Result<(), DurableError> {
    let relative = frame
        .sequence()
        .checked_sub(checkpoint.wal_metadata_sequence())
        .ok_or(DurableError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        })?;
    if relative == 0 {
        return Err(DurableError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        });
    }
    let history_index = usize::try_from((relative - 1) / 2).map_err(|_| {
        DurableError::CheckpointPrefixDivergence {
            wal_sequence: frame.sequence(),
        }
    })?;
    let Some(expected) = checkpoint.history().get(history_index) else {
        return Err(DurableError::CheckpointPrefixDivergence {
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
        return Err(DurableError::CheckpointPrefixDivergence {
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

fn validate_checkpoint_path(
    storage_path: &Path,
    layout: JournalLayout,
    checkpoint_path: &Path,
) -> Result<(), DurableError> {
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
                return Err(DurableError::CheckpointPathConflictsWithWal {
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
                return Err(DurableError::CheckpointPathConflictsWithWal {
                    path: conflict.clone(),
                });
            }
        }
    }
    Ok(())
}

const fn checkpoint_failure_requires_poison(error: &OrderBookCheckpointError) -> bool {
    !error.is_operational_failure()
}

fn record_checkpoint_verification_failure(poison: &AtomicBool, error: &OrderBookCheckpointError) {
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
    use crate::matching::{OrderBook, OrderBookCheckpointError, OrderBookCheckpointResource};

    use super::{
        DurableOrderBookCheckpointCapture, checkpoint_failure_requires_poison,
        record_checkpoint_verification_failure,
    };

    fn definition() -> InstrumentDefinition {
        InstrumentDefinition::new(InstrumentSpec {
            instrument_id: InstrumentId::new(1).unwrap(),
            version: InstrumentVersion::new(1).unwrap(),
            effective_from: TimestampNs::from_unix_nanos(0),
            symbol: InstrumentSymbol::new("DURABLE-STAGED-CHECKPOINT").unwrap(),
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
    fn resource_pressure_is_retryable_but_semantic_failure_requires_poison() {
        let resource = OrderBookCheckpointError::ResourceReservationFailed {
            resource: OrderBookCheckpointResource::CaptureHistory,
            maximum: 1,
        };
        let construction = OrderBookCheckpointError::BookConstructionFailed(
            crate::matching::MatchingError::CapacityReservationFailed(
                crate::matching::MatchingCapacity::ActiveOrders,
            ),
        );
        assert!(!checkpoint_failure_requires_poison(&resource));
        assert!(!checkpoint_failure_requires_poison(&construction));
        assert!(checkpoint_failure_requires_poison(
            &OrderBookCheckpointError::Invalid("corrupt live state".to_owned())
        ));
    }

    #[test]
    fn asynchronous_verification_latch_records_only_semantic_failure() {
        let poison = AtomicBool::new(false);
        let operational = OrderBookCheckpointError::ResourceReservationFailed {
            resource: OrderBookCheckpointResource::CaptureActiveOrders,
            maximum: 1,
        };
        record_checkpoint_verification_failure(&poison, &operational);
        assert!(!poison.load(Ordering::Acquire));

        let semantic = OrderBookCheckpointError::Invalid("replay divergence".to_owned());
        record_checkpoint_verification_failure(&poison, &semantic);
        assert!(poison.load(Ordering::Acquire));
    }

    #[test]
    fn durable_capture_semantic_verification_failure_trips_its_origin_latch() {
        let book = OrderBook::new(definition());
        let mut capture = book.capture_checkpoint_candidate(1, 1).unwrap();
        capture.corrupt_generation_for_test();
        let origin = Arc::new(AtomicBool::new(false));
        let durable = DurableOrderBookCheckpointCapture {
            capture,
            origin: Arc::clone(&origin),
            cutover_epoch: 0,
            cutover_boundary: crate::durable_storage::StorageCutoverBoundary::for_test(1),
        };

        assert!(matches!(
            durable.verify(),
            Err(OrderBookCheckpointError::Invalid(_))
        ));
        assert!(origin.load(Ordering::Acquire));
    }
}
