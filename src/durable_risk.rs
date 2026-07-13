//! Crash-recoverable orchestration for matching coupled to pre-trade risk.
//!
//! The WAL grammar is one instrument definition, a canonical account-sorted
//! risk-profile set, and then alternating command/report records. Recovery
//! replays both matching and reservation state and rejects any profile drift.

use std::fmt;
use std::path::Path;

use crate::domain::{InstrumentId, InstrumentVersion};
use crate::instrument::InstrumentDefinition;
use crate::journal::{
    Journal, JournalError, JournalFrame, JournalOptions, JournalReader, RecordKind, RecoveryReport,
};
use crate::matching::{Command, ExecutionReport, MatchingError};
use crate::risk::{AccountRiskDefinition, RiskError, RiskInvariantViolation, RiskManagedOrderBook};

/// Result of reconstructing a durable risk-managed matching shard.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableRiskRecoveryReport {
    /// Physical frame recovery performed by the journal.
    pub journal: RecoveryReport,
    /// Complete command/report pairs replayed.
    pub replayed_commands: u64,
    /// Profiles appended to complete interrupted metadata initialization.
    pub completed_profile_records: u64,
    /// Whether one final command was completed with a reconstructed report.
    pub completed_dangling_command: bool,
}

/// Durable risk-managed matching failure.
#[derive(Debug)]
pub enum DurableRiskError {
    /// Journal framing, codec, I/O, or recovery failure.
    Journal(JournalError),
    /// Matching operational failure.
    Matching(MatchingError),
    /// Risk-profile validation failure.
    Risk(RiskError),
    /// Reconstructed matching/risk state violated an invariant.
    Invariant(RiskInvariantViolation),
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Journal(error) => error.fmt(formatter),
            Self::Matching(error) => error.fmt(formatter),
            Self::Risk(error) => error.fmt(formatter),
            Self::Invariant(error) => error.fmt(formatter),
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
            Self::Matching(error) => Some(error),
            Self::Risk(error) => Some(error),
            Self::Invariant(error) => Some(error),
            _ => None,
        }
    }
}

impl From<JournalError> for DurableRiskError {
    fn from(error: JournalError) -> Self {
        Self::Journal(error)
    }
}

impl From<MatchingError> for DurableRiskError {
    fn from(error: MatchingError) -> Self {
        Self::Matching(error)
    }
}

impl From<RiskError> for DurableRiskError {
    fn from(error: RiskError) -> Self {
        Self::Risk(error)
    }
}

/// One single-writer, definition/profile-bound durable risk matching shard.
#[derive(Debug)]
pub struct DurableRiskOrderBook {
    managed: RiskManagedOrderBook,
    journal: Journal,
    recovery: DurableRiskRecoveryReport,
    poisoned: bool,
}

struct OpenedRiskJournal {
    journal: Journal,
    reader: JournalReader,
    first_data_frame: Option<JournalFrame>,
    reader_exhausted: bool,
    recovery: RecoveryReport,
    completed_profile_records: u64,
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
        let profiles = canonical_profiles(profiles)?;
        let path = path.as_ref();
        let OpenedRiskJournal {
            mut journal,
            mut reader,
            first_data_frame,
            reader_exhausted: replay_reader_exhausted,
            recovery: journal_recovery,
            completed_profile_records,
        } = open_risk_metadata(path, definition, &profiles, options)?;

        let mut managed = RiskManagedOrderBook::new(definition);
        for profile in &profiles {
            managed.register_account(profile.account_id(), profile.profile())?;
        }
        let mut pending: Option<(u64, Command)> = None;
        let mut replayed_commands = 0_u64;
        let mut next_frame = first_data_frame;
        loop {
            let frame = if let Some(frame) = next_frame.take() {
                frame
            } else {
                if replay_reader_exhausted {
                    break;
                }
                let Some(frame) = reader.next().transpose()? else {
                    break;
                };
                frame
            };
            match frame.kind() {
                RecordKind::Command => {
                    if let Some((pending_sequence, _)) = pending {
                        return Err(DurableRiskError::ConsecutiveCommands {
                            pending_sequence,
                            next_sequence: frame.sequence(),
                        });
                    }
                    pending = Some((frame.sequence(), frame.decode()?));
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
                    replayed_commands = replayed_commands
                        .checked_add(1)
                        .ok_or(MatchingError::SequenceExhausted)?;
                }
                kind => {
                    return Err(DurableRiskError::UnexpectedRecord {
                        sequence: frame.sequence(),
                        kind,
                    });
                }
            }
        }

        let completed_dangling_command = if let Some((_, command)) = pending {
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
            recovery: DurableRiskRecoveryReport {
                journal: journal_recovery,
                replayed_commands,
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

    /// Writes a command before applying matching/risk state and then persists its trace.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] for preflight, journal, or matching failure.
    /// A failure after command acknowledgement poisons this instance.
    pub fn submit(&mut self, command: Command) -> Result<ExecutionReport, DurableRiskError> {
        if self.poisoned {
            return Err(DurableRiskError::Poisoned);
        }
        if let Some(report) = self.managed.preflight(command)? {
            return Ok(report);
        }
        if let Err(error) = self.journal.append(&command) {
            self.poisoned = self.journal.is_poisoned();
            return Err(DurableRiskError::Journal(error));
        }
        let report = match self.managed.submit(command) {
            Ok(report) => report,
            Err(error) => {
                self.poisoned = true;
                return Err(DurableRiskError::Matching(error));
            }
        };
        if let Err(error) = self.journal.append(&report) {
            self.poisoned = true;
            return Err(DurableRiskError::Journal(error));
        }
        Ok(report)
    }

    /// Synchronizes journal data and metadata.
    ///
    /// # Errors
    ///
    /// Returns [`DurableRiskError`] and poisons the shard on synchronization failure.
    pub fn sync_all(&mut self) -> Result<(), DurableRiskError> {
        if self.poisoned {
            return Err(DurableRiskError::Poisoned);
        }
        if let Err(error) = self.journal.sync_all() {
            self.poisoned = true;
            return Err(DurableRiskError::Journal(error));
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
        if self.poisoned {
            return Err(DurableRiskError::Poisoned);
        }
        self.journal.close().map_err(DurableRiskError::Journal)
    }
}

fn open_risk_metadata(
    path: &Path,
    definition: InstrumentDefinition,
    profiles: &[AccountRiskDefinition],
    options: JournalOptions,
) -> Result<OpenedRiskJournal, DurableRiskError> {
    let mut journal = Journal::open(path, options)?;
    let recovery = journal.recovery();
    if recovery.last_sequence.is_none() {
        journal.append(&definition)?;
    }

    let mut reader = JournalReader::open(path, options)?;
    let Some(definition_frame) = reader.next().transpose()? else {
        return Err(DurableRiskError::DefinitionRecordMissing);
    };
    if definition_frame.kind() != RecordKind::InstrumentDefinition {
        return Err(DurableRiskError::DefinitionRecordRequired {
            sequence: definition_frame.sequence(),
            actual: definition_frame.kind(),
        });
    }
    let persisted_definition: InstrumentDefinition = definition_frame.decode()?;
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
    Ok(OpenedRiskJournal {
        journal,
        reader,
        first_data_frame,
        reader_exhausted,
        recovery,
        completed_profile_records,
    })
}

fn canonical_profiles(
    profiles: &[AccountRiskDefinition],
) -> Result<Vec<AccountRiskDefinition>, RiskError> {
    let mut canonical = profiles.to_vec();
    canonical.sort_unstable_by_key(|value| value.account_id());
    for pair in canonical.windows(2) {
        if pair[0].account_id() == pair[1].account_id() {
            return Err(RiskError::DuplicateProfile(pair[0].account_id()));
        }
    }
    Ok(canonical)
}
