//! Crash-recoverable orchestration of one matching shard and its WAL.
//!
//! The protocol appends and acknowledges a command before applying it to the
//! in-memory book, then appends its deterministic execution report. Recovery
//! replays every verified command/report pair and completes one possible final
//! command whose report was interrupted by process termination.

use std::fmt;
use std::path::Path;

use crate::domain::{InstrumentId, InstrumentVersion};
use crate::instrument::InstrumentDefinition;
use crate::journal::{
    Journal, JournalError, JournalOptions, JournalReader, RecordKind, RecoveryReport,
};
use crate::matching::{Command, ExecutionReport, InvariantViolation, MatchingError, OrderBook};

/// Result of reconstructing a durable matching shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableRecoveryReport {
    /// Physical frame recovery performed by the journal.
    pub journal: RecoveryReport,
    /// Commands reconstructed from complete command/report pairs.
    pub replayed_commands: u64,
    /// Whether a final durable command lacked a report and was completed.
    pub completed_dangling_command: bool,
}

/// Durable matching orchestration or replay failure.
#[derive(Debug)]
pub enum DurableError {
    /// Journal framing, recovery, codec, or I/O failure.
    Journal(JournalError),
    /// Matching operational error.
    Matching(MatchingError),
    /// Reconstructed book structure violated an internal invariant.
    Invariant(InvariantViolation),
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
    /// The runtime is unusable until reopened and recovered after an ambiguous transition.
    Poisoned,
}

impl fmt::Display for DurableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(error) => error.fmt(formatter),
            Self::Matching(error) => error.fmt(formatter),
            Self::Invariant(error) => error.fmt(formatter),
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
            Self::Poisoned => formatter
                .write_str("durable matching shard is poisoned and must be reopened for recovery"),
        }
    }
}

impl std::error::Error for DurableError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Journal(error) => Some(error),
            Self::Matching(error) => Some(error),
            Self::Invariant(error) => Some(error),
            _ => None,
        }
    }
}

impl From<JournalError> for DurableError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

impl From<MatchingError> for DurableError {
    fn from(error: MatchingError) -> Self {
        Self::Matching(error)
    }
}

/// One crash-recoverable, single-writer instrument matching shard.
#[derive(Debug)]
pub struct DurableOrderBook {
    book: OrderBook,
    journal: Journal,
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
        let path = path.as_ref();
        let mut journal = Journal::open(path, options)?;
        let journal_recovery = journal.recovery();
        if journal_recovery.last_sequence.is_none() {
            journal.append(&definition)?;
        }
        let mut reader = JournalReader::open(path, options)?;
        let Some(definition_frame) = reader.next().transpose()? else {
            return Err(DurableError::DefinitionRecordMissing);
        };
        if definition_frame.kind() != RecordKind::InstrumentDefinition {
            return Err(DurableError::DefinitionRecordRequired {
                sequence: definition_frame.sequence(),
                actual: definition_frame.kind(),
            });
        }
        let persisted_definition: InstrumentDefinition = definition_frame.decode()?;
        if persisted_definition != definition {
            return Err(DurableError::DefinitionMismatch {
                requested_instrument_id: definition.instrument_id(),
                requested_version: definition.version(),
                persisted_instrument_id: persisted_definition.instrument_id(),
                persisted_version: persisted_definition.version(),
            });
        }
        let mut book = OrderBook::new(definition);
        let mut pending: Option<(u64, Command)> = None;
        let mut replayed_commands = 0_u64;

        for frame_result in reader {
            let frame = frame_result?;
            match frame.kind() {
                RecordKind::Command => {
                    if let Some((pending_sequence, _)) = pending {
                        return Err(DurableError::ConsecutiveCommands {
                            pending_sequence,
                            next_sequence: frame.sequence(),
                        });
                    }
                    pending = Some((frame.sequence(), frame.decode()?));
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
                    replayed_commands = replayed_commands
                        .checked_add(1)
                        .ok_or(MatchingError::SequenceExhausted)?;
                }
                RecordKind::LedgerEntry
                | RecordKind::InstrumentDefinition
                | RecordKind::AccountRiskDefinition => {
                    return Err(DurableError::UnexpectedRecord {
                        sequence: frame.sequence(),
                        kind: frame.kind(),
                    });
                }
            }
        }

        let completed_dangling_command = if let Some((_, command)) = pending {
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
            recovery: DurableRecoveryReport {
                journal: journal_recovery,
                replayed_commands,
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

    /// Durably sequences a command, applies it, and durably records its trace.
    ///
    /// Exact retries return the cached replay report without adding WAL frames.
    /// Identifier collisions and capacity exhaustion are detected before the
    /// command frame is written.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] for matching preflight or journal failure. A
    /// failure after the command frame is acknowledged poisons this instance;
    /// reopening completes recovery deterministically.
    pub fn submit(&mut self, command: Command) -> Result<ExecutionReport, DurableError> {
        if self.poisoned {
            return Err(DurableError::Poisoned);
        }
        if let Some(report) = self.book.preflight(command)? {
            return Ok(report);
        }
        if let Err(error) = self.journal.append(&command) {
            self.poisoned = self.journal.is_poisoned();
            return Err(DurableError::Journal(error));
        }
        let report = match self.book.submit(command) {
            Ok(report) => report,
            Err(error) => {
                self.poisoned = true;
                return Err(DurableError::Matching(error));
            }
        };
        if let Err(error) = self.journal.append(&report) {
            self.poisoned = true;
            return Err(DurableError::Journal(error));
        }
        Ok(report)
    }

    /// Synchronizes journal data and metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DurableError`] and poisons the shard when synchronization fails.
    pub fn sync_all(&mut self) -> Result<(), DurableError> {
        if self.poisoned {
            return Err(DurableError::Poisoned);
        }
        if let Err(error) = self.journal.sync_all() {
            self.poisoned = true;
            return Err(DurableError::Journal(error));
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
        if self.poisoned {
            return Err(DurableError::Poisoned);
        }
        self.journal.close().map_err(DurableError::Journal)
    }
}
