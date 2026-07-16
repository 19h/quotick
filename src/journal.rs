//! A versioned, checksummed, append-only write-ahead journal.
//!
//! Each frame contains a 24-byte header followed by a bounded payload:
//!
//! ```text
//! 0..4   magic "QWAL"
//! 4..6   format version (`u16`, little-endian)
//! 6..8   record kind (`u16`, little-endian)
//! 8..12  payload length (`u32`, little-endian)
//! 12..16 CRC-32C of header-with-zero-checksum and payload
//! 16..24 contiguous journal sequence (`u64`, little-endian)
//! ```
//!
//! Recovery can repair only a physically incomplete final frame. A checksum
//! mismatch, sequence discontinuity, invalid magic, unsupported version, or
//! unknown record kind is always corruption and is never silently discarded.
//! Complete frame, batch-byte, receipt, read-payload, and segment-inventory
//! storage is fallibly reserved before the corresponding write, read, or
//! rotation. A batch writes stack-built headers directly into one reserved byte
//! buffer and never allocates an intermediate vector per frame.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::auction_engine::{CallAuctionCommand, CallAuctionExecutionReport};
use crate::codec::{BinaryCodec, CodecError};
use crate::instrument::InstrumentDefinition;
use crate::ledger::{JournalEntry, LedgerBatch, LedgerCorrection};
use crate::matching::{Command, ExecutionReport};
use crate::risk::AccountRiskDefinition;
use crate::snapshot::CheckpointAnchor;

pub use crate::segmented_journal::{
    SegmentDescriptor, SegmentedAppendReceipt, SegmentedJournal, SegmentedJournalError,
    SegmentedJournalOptions, SegmentedJournalReader, SegmentedRecoveryReport,
};

const MAGIC: [u8; 4] = *b"QWAL";
const FORMAT_VERSION: u16 = 16;
pub(crate) const HEADER_LENGTH: usize = 24;
const CHECKSUM_START: usize = 12;
const CHECKSUM_END: usize = 16;
const DEFAULT_MAX_PAYLOAD_LENGTH: u32 = 16 * 1024 * 1024;
const CRC32C_POLYNOMIAL: u32 = 0x82F6_3B78;
const CRC32C_TABLE: [u32; 256] = make_crc32c_table();
const LEASE_MAGIC: [u8; 4] = *b"QLCK";
const LEASE_VERSION: u16 = 1;
const LEASE_LENGTH: usize = 34;
pub(crate) const SEGMENT_DIRECTORY_MARKER: &str = "format.qseg";
static NEXT_LEASE_NONCE: AtomicU64 = AtomicU64::new(1);

/// Stable type identifier for a journal payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordKind {
    /// A matching-engine command written before application.
    Command,
    /// The complete trace resulting from one matching command.
    ExecutionReport,
    /// A balanced ledger journal entry.
    LedgerEntry,
    /// Exact immutable definition bound to one matching journal.
    InstrumentDefinition,
    /// Immutable account risk profile bound to one risk-managed matching journal.
    AccountRiskDefinition,
    /// One atomic ledger reversal-plus-replacement correction.
    LedgerCorrection,
    /// One atomic ordered group of ledger entries.
    LedgerBatch,
    /// Identity of a synchronized semantic checkpoint replacing an older WAL prefix.
    CheckpointAnchor,
    /// A call-auction command written before application.
    CallAuctionCommand,
    /// The complete trace resulting from one call-auction command.
    CallAuctionExecutionReport,
}

impl RecordKind {
    const fn wire(self) -> u16 {
        match self {
            Self::Command => 1,
            Self::ExecutionReport => 2,
            Self::LedgerEntry => 3,
            Self::InstrumentDefinition => 4,
            Self::AccountRiskDefinition => 5,
            Self::LedgerCorrection => 6,
            Self::LedgerBatch => 7,
            Self::CheckpointAnchor => 8,
            Self::CallAuctionCommand => 9,
            Self::CallAuctionExecutionReport => 10,
        }
    }

    fn from_wire(value: u16, offset: u64) -> Result<Self, JournalError> {
        match value {
            1 => Ok(Self::Command),
            2 => Ok(Self::ExecutionReport),
            3 => Ok(Self::LedgerEntry),
            4 => Ok(Self::InstrumentDefinition),
            5 => Ok(Self::AccountRiskDefinition),
            6 => Ok(Self::LedgerCorrection),
            7 => Ok(Self::LedgerBatch),
            8 => Ok(Self::CheckpointAnchor),
            9 => Ok(Self::CallAuctionCommand),
            10 => Ok(Self::CallAuctionExecutionReport),
            _ => Err(JournalError::UnknownRecordKind { offset, value }),
        }
    }
}

/// Typed payload accepted by [`Journal::append`].
pub trait DurableRecord: BinaryCodec {
    /// Stable frame record kind.
    const KIND: RecordKind;
}

impl DurableRecord for Command {
    const KIND: RecordKind = RecordKind::Command;
}

impl DurableRecord for ExecutionReport {
    const KIND: RecordKind = RecordKind::ExecutionReport;
}

impl DurableRecord for JournalEntry {
    const KIND: RecordKind = RecordKind::LedgerEntry;
}

impl DurableRecord for InstrumentDefinition {
    const KIND: RecordKind = RecordKind::InstrumentDefinition;
}

impl DurableRecord for AccountRiskDefinition {
    const KIND: RecordKind = RecordKind::AccountRiskDefinition;
}

impl DurableRecord for LedgerCorrection {
    const KIND: RecordKind = RecordKind::LedgerCorrection;
}

impl DurableRecord for LedgerBatch {
    const KIND: RecordKind = RecordKind::LedgerBatch;
}

impl DurableRecord for CheckpointAnchor {
    const KIND: RecordKind = RecordKind::CheckpointAnchor;
}

impl DurableRecord for CallAuctionCommand {
    const KIND: RecordKind = RecordKind::CallAuctionCommand;
}

impl DurableRecord for CallAuctionExecutionReport {
    const KIND: RecordKind = RecordKind::CallAuctionExecutionReport;
}

/// A decoded known payload from a journal frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KnownRecord {
    /// Matching command.
    Command(Command),
    /// Matching execution trace.
    ExecutionReport(ExecutionReport),
    /// Balanced accounting entry.
    LedgerEntry(JournalEntry),
    /// Immutable instrument definition.
    InstrumentDefinition(InstrumentDefinition),
    /// Immutable account risk profile.
    AccountRiskDefinition(AccountRiskDefinition),
    /// Atomic accounting correction.
    LedgerCorrection(LedgerCorrection),
    /// Atomic ordered accounting batch.
    LedgerBatch(LedgerBatch),
    /// Synchronized checkpoint identity at a compacted physical WAL origin.
    CheckpointAnchor(CheckpointAnchor),
    /// Sequenced call-auction command.
    CallAuctionCommand(CallAuctionCommand),
    /// Complete call-auction execution trace.
    CallAuctionExecutionReport(CallAuctionExecutionReport),
}

/// Acknowledgement policy for successful appends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Durability {
    /// Return after `write_all`; data may remain only in the operating-system cache.
    Buffered,
    /// Flush Rust and operating-system file buffers without a device durability guarantee.
    Flush,
    /// Call `File::sync_data` before acknowledging the append.
    SyncData,
    /// Call `File::sync_all` before acknowledging, covering data and file metadata.
    SyncAll,
}

/// Action allowed when opening a journal with an incomplete final frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryMode {
    /// Treat every incomplete frame as an error and do not modify the file.
    Strict,
    /// Truncate an incomplete final frame to the last verified boundary.
    RepairTornTail,
}

/// Journal framing, recovery, and acknowledgement configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JournalOptions {
    /// Maximum accepted payload bytes per frame.
    pub max_payload_length: u32,
    /// Append acknowledgement policy.
    pub durability: Durability,
    /// Incomplete-tail recovery policy.
    pub recovery: RecoveryMode,
    /// Expected sequence of the first frame, enabling independently managed segments.
    pub initial_sequence: u64,
}

impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            max_payload_length: DEFAULT_MAX_PAYLOAD_LENGTH,
            durability: Durability::SyncAll,
            recovery: RecoveryMode::Strict,
            initial_sequence: 1,
        }
    }
}

/// Diagnostic identity stored in an exclusive journal-writer lease.
///
/// This identity is a compare token, not an authentication credential. An
/// operator may use it to recover an abandoned lease only after independently
/// proving that the owning process has terminated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterLeaseOwner {
    process_id: u32,
    acquired_at_unix_nanos: u128,
    nonce: u64,
}

impl WriterLeaseOwner {
    /// Returns the operating-system process identifier recorded at acquisition.
    #[must_use]
    pub const fn process_id(self) -> u32 {
        self.process_id
    }

    /// Returns lease acquisition time in nanoseconds since the Unix epoch.
    #[must_use]
    pub const fn acquired_at_unix_nanos(self) -> u128 {
        self.acquired_at_unix_nanos
    }

    /// Returns the process-local acquisition nonce.
    #[must_use]
    pub const fn nonce(self) -> u64 {
        self.nonce
    }
}

/// Result of scanning and optionally repairing an existing journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryReport {
    /// Last verified frame sequence, or `None` for an empty journal.
    pub last_sequence: Option<u64>,
    /// Bytes covered by verified complete frames.
    pub valid_bytes: u64,
    /// Bytes removed from an incomplete final frame.
    pub truncated_bytes: u64,
}

/// Physical layout represented by a durable-runtime recovery report.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalLayout {
    /// One append-only file and one writer lease.
    SingleFile,
    /// A marker-bound directory of size-limited files and one manager lease.
    Segmented,
}

/// Layout-independent physical recovery summary used by durable runtimes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorageRecoveryReport {
    /// Physical storage layout opened by the runtime.
    pub layout: JournalLayout,
    /// First retained global frame sequence in the selected physical layout.
    pub first_sequence: u64,
    /// Last verified global frame sequence, or `None` for empty storage.
    pub last_sequence: Option<u64>,
    /// Sum of bytes covered by complete verified frames.
    pub valid_bytes: u128,
    /// Bytes removed from the only tail eligible for repair.
    pub truncated_bytes: u64,
    /// Number of physical files, including an empty active file.
    pub segment_count: u64,
    /// First global sequence assigned to the active physical file.
    pub active_segment_start: u64,
}

/// Successful append metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendReceipt {
    /// Assigned contiguous journal sequence.
    pub sequence: u64,
    /// Starting byte offset of the frame.
    pub offset: u64,
    /// Complete header-plus-payload length.
    pub frame_length: usize,
    /// Durability policy satisfied before this receipt was returned.
    pub durability: Durability,
}

#[derive(Clone, Debug)]
struct EncodedRecord {
    kind: RecordKind,
    payload: Vec<u8>,
}

/// A heterogeneous group of pre-encoded records committed with one write and
/// one durability barrier.
///
/// A batch reduces syscall and synchronization overhead. It is an
/// acknowledgement group, not a transactional frame: recovery after a torn
/// write can retain a verified prefix. Consumers use command and transaction
/// idempotency to replay the unacknowledged suffix.
#[derive(Clone, Debug, Default)]
pub struct JournalBatch {
    records: Vec<EncodedRecord>,
}

impl JournalBatch {
    /// Creates an empty batch.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            records: Vec::new(),
        }
    }

    /// Creates an empty batch with fallibly reserved record headroom.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::CapacityReservationFailed`] when the requested
    /// record layout is unrepresentable or unavailable.
    pub fn try_with_capacity(maximum_records: usize) -> Result<Self, JournalError> {
        Ok(Self {
            records: reserve_journal_vec(JournalResource::BatchRecords, maximum_records)?,
        })
    }

    /// Encodes and adds one typed record.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Codec`] if the record cannot be represented by
    /// its stable payload format, or [`JournalError::CapacityReservationFailed`]
    /// if the next batch-record slot cannot be reserved.
    pub fn push<T: DurableRecord>(&mut self, record: &T) -> Result<(), JournalError> {
        reserve_journal_additional(&mut self.records, JournalResource::BatchRecords, 1)?;
        self.records.push(EncodedRecord {
            kind: T::KIND,
            payload: record.encode().map_err(JournalError::Codec)?,
        });
        Ok(())
    }

    /// Adds already encoded payload bytes.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::CapacityReservationFailed`] if the next
    /// batch-record slot cannot be reserved.
    pub fn push_raw(&mut self, kind: RecordKind, payload: Vec<u8>) -> Result<(), JournalError> {
        reserve_journal_additional(&mut self.records, JournalResource::BatchRecords, 1)?;
        self.records.push(EncodedRecord { kind, payload });
        Ok(())
    }

    /// Returns the number of records in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns whether the batch contains no records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Removes all records while retaining allocated batch capacity.
    pub fn clear(&mut self) {
        self.records.clear();
    }

    pub(crate) fn encoded_frame_length(
        &self,
        maximum_payload_length: u32,
    ) -> Result<u64, JournalError> {
        let mut total = 0_u64;
        for record in &self.records {
            let payload_length =
                u32::try_from(record.payload.len()).map_err(|_| JournalError::PayloadTooLarge {
                    length: record.payload.len(),
                    maximum: maximum_payload_length,
                })?;
            if payload_length > maximum_payload_length {
                return Err(JournalError::PayloadTooLarge {
                    length: record.payload.len(),
                    maximum: maximum_payload_length,
                });
            }
            let frame_length = u64::try_from(HEADER_LENGTH)
                .map_err(|_| JournalError::FrameLengthOverflow)?
                .checked_add(u64::from(payload_length))
                .ok_or(JournalError::FrameLengthOverflow)?;
            total = total
                .checked_add(frame_length)
                .ok_or(JournalError::FrameLengthOverflow)?;
        }
        Ok(total)
    }
}

/// One fallibly reserved journal-owned buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalResource {
    /// Encoded records retained by a caller-owned [`JournalBatch`].
    BatchRecords,
    /// One complete header-plus-payload frame.
    FrameBytes,
    /// Concatenated header-plus-payload bytes for one append batch.
    BatchFrameBytes,
    /// Plain append receipts returned for one batch.
    BatchReceipts,
    /// Payload bytes read from one verified physical frame.
    ReadPayloadBytes,
    /// Segmented append receipts returned for one batch.
    SegmentedBatchReceipts,
    /// Physical segment descriptors retained by a segmented writer.
    SegmentInventory,
}

impl fmt::Display for JournalResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BatchRecords => "batch-record entries",
            Self::FrameBytes => "frame bytes",
            Self::BatchFrameBytes => "batch-frame bytes",
            Self::BatchReceipts => "batch-receipt entries",
            Self::ReadPayloadBytes => "read-payload bytes",
            Self::SegmentedBatchReceipts => "segmented batch-receipt entries",
            Self::SegmentInventory => "segment-inventory entries",
        })
    }
}

pub(crate) fn reserve_journal_vec<T>(
    resource: JournalResource,
    maximum: usize,
) -> Result<Vec<T>, JournalError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(maximum)
        .map_err(|_| JournalError::CapacityReservationFailed { resource, maximum })?;
    Ok(values)
}

pub(crate) fn reserve_journal_additional<T>(
    values: &mut Vec<T>,
    resource: JournalResource,
    additional: usize,
) -> Result<(), JournalError> {
    let maximum = values
        .len()
        .checked_add(additional)
        .ok_or(JournalError::FrameLengthOverflow)?;
    values
        .try_reserve(additional)
        .map_err(|_| JournalError::CapacityReservationFailed { resource, maximum })
}

/// A verified frame returned by [`JournalReader`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JournalFrame {
    offset: u64,
    sequence: u64,
    kind: RecordKind,
    payload: Vec<u8>,
}

impl JournalFrame {
    /// Returns the frame's starting byte offset.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the contiguous journal sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Returns the payload kind.
    #[must_use]
    pub const fn kind(&self) -> RecordKind {
        self.kind
    }

    /// Returns verified raw payload bytes.
    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns the complete encoded frame length.
    #[must_use]
    pub fn frame_length(&self) -> usize {
        HEADER_LENGTH + self.payload.len()
    }

    /// Decodes the payload as a specific durable type and verifies its kind.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::RecordKindMismatch`] for a type mismatch or
    /// [`JournalError::Codec`] for an invalid payload.
    pub fn decode<T: DurableRecord>(&self) -> Result<T, JournalError> {
        if self.kind != T::KIND {
            return Err(JournalError::RecordKindMismatch {
                expected: T::KIND,
                actual: self.kind,
            });
        }
        T::decode(&self.payload).map_err(JournalError::Codec)
    }

    /// Decodes the payload according to the frame's record kind.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Codec`] when the typed payload is invalid.
    pub fn decode_known(&self) -> Result<KnownRecord, JournalError> {
        match self.kind {
            RecordKind::Command => self.decode().map(KnownRecord::Command),
            RecordKind::ExecutionReport => self.decode().map(KnownRecord::ExecutionReport),
            RecordKind::LedgerEntry => self.decode().map(KnownRecord::LedgerEntry),
            RecordKind::InstrumentDefinition => {
                self.decode().map(KnownRecord::InstrumentDefinition)
            }
            RecordKind::AccountRiskDefinition => {
                self.decode().map(KnownRecord::AccountRiskDefinition)
            }
            RecordKind::LedgerCorrection => self.decode().map(KnownRecord::LedgerCorrection),
            RecordKind::LedgerBatch => self.decode().map(KnownRecord::LedgerBatch),
            RecordKind::CheckpointAnchor => self.decode().map(KnownRecord::CheckpointAnchor),
            RecordKind::CallAuctionCommand => self.decode().map(KnownRecord::CallAuctionCommand),
            RecordKind::CallAuctionExecutionReport => {
                self.decode().map(KnownRecord::CallAuctionExecutionReport)
            }
        }
    }
}

/// Journal I/O, framing, sequencing, corruption, or payload error.
#[derive(Debug)]
pub enum JournalError {
    /// Filesystem operation failed.
    Io(io::Error),
    /// Typed payload codec failed before writing or after reading.
    Codec(CodecError),
    /// Journal operations were attempted after an ambiguous write or sync failure.
    Poisoned,
    /// Another writer owns the canonical journal path.
    WriterLeaseHeld {
        /// Canonical sidecar lease path.
        lease_path: PathBuf,
        /// Observed writer identity.
        owner: WriterLeaseOwner,
    },
    /// An abandoned-writer recovery token no longer matches the lease on disk.
    WriterLeaseOwnerMismatch {
        /// Owner identity supplied by the recovery caller.
        expected: WriterLeaseOwner,
        /// Owner identity currently stored on disk.
        actual: WriterLeaseOwner,
    },
    /// A writer lease was truncated or used unsupported framing.
    InvalidWriterLease {
        /// Canonical sidecar lease path.
        lease_path: PathBuf,
    },
    /// No unique process-local writer-lease nonce remains representable.
    WriterLeaseIdentityExhausted,
    /// A raw writer attempted to open a file owned by a segmented-journal manager.
    ManagedSegment {
        /// Marker proving that the parent is a dedicated segmented directory.
        marker_path: PathBuf,
    },
    /// Segment sequence zero is invalid.
    InvalidInitialSequence,
    /// No subsequent `u64` sequence can be represented.
    SequenceExhausted,
    /// Header-plus-payload or file offset arithmetic exceeded its representation.
    FrameLengthOverflow,
    /// A complete journal-owned buffer could not be represented or reserved.
    CapacityReservationFailed {
        /// Exact logical buffer that failed.
        resource: JournalResource,
        /// Exact required byte or entry count, according to `resource`.
        maximum: usize,
    },
    /// A checkpoint anchor is not retained within the current WAL range.
    CheckpointAnchorSequenceMismatch {
        /// Last sequence in the current WAL.
        current: Option<u64>,
        /// Generation claimed by the replacement anchor.
        proposed: u64,
    },
    /// A prior interrupted cutover left the deterministic replacement path occupied.
    CutoverPendingExists {
        /// Path requiring explicit recovery or removal after provenance checks.
        path: PathBuf,
    },
    /// No abandoned checkpoint-cutover pending file exists at the derived path.
    CutoverPendingMissing {
        /// Derived path that was absent.
        path: PathBuf,
    },
    /// Payload exceeded the configured frame limit.
    PayloadTooLarge {
        /// Requested payload bytes.
        length: usize,
        /// Configured maximum bytes.
        maximum: u32,
    },
    /// A physically incomplete frame was encountered.
    TruncatedFrame {
        /// Frame starting byte offset.
        offset: u64,
        /// Complete frame bytes required.
        expected_length: u64,
        /// Bytes available from `offset` to end of file.
        available_length: u64,
    },
    /// Frame magic did not match `QWAL`.
    InvalidMagic {
        /// Frame starting byte offset.
        offset: u64,
        /// Bytes found in the magic field.
        found: [u8; 4],
    },
    /// Frame version is not supported by this reader.
    UnsupportedVersion {
        /// Frame starting byte offset.
        offset: u64,
        /// Version found in the frame.
        version: u16,
    },
    /// Frame record kind is not defined by this version.
    UnknownRecordKind {
        /// Frame starting byte offset.
        offset: u64,
        /// Raw record-kind value.
        value: u16,
    },
    /// Payload checksum did not match the frame.
    ChecksumMismatch {
        /// Frame starting byte offset.
        offset: u64,
        /// Checksum stored in the frame.
        expected: u32,
        /// Checksum calculated from the frame.
        actual: u32,
    },
    /// A frame sequence was not the next expected value.
    SequenceDiscontinuity {
        /// Frame starting byte offset.
        offset: u64,
        /// Required sequence.
        expected: u64,
        /// Sequence found in the frame.
        actual: u64,
    },
    /// A frame was decoded using a type with another record kind.
    RecordKindMismatch {
        /// Kind required by the requested type.
        expected: RecordKind,
        /// Kind stored in the frame.
        actual: RecordKind,
    },
}

impl fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "journal I/O error: {error}"),
            Self::Codec(error) => write!(formatter, "journal payload error: {error}"),
            Self::Poisoned => {
                formatter.write_str("journal is poisoned after an ambiguous I/O failure")
            }
            Self::WriterLeaseHeld { lease_path, owner } => write!(
                formatter,
                "journal writer lease {} is held by process {} (nonce {})",
                lease_path.display(),
                owner.process_id,
                owner.nonce
            ),
            Self::WriterLeaseOwnerMismatch { expected, actual } => write!(
                formatter,
                "writer lease owner changed: expected process {} nonce {}, found process {} nonce {}",
                expected.process_id, expected.nonce, actual.process_id, actual.nonce
            ),
            Self::InvalidWriterLease { lease_path } => write!(
                formatter,
                "writer lease {} is truncated or has unsupported framing",
                lease_path.display()
            ),
            Self::WriterLeaseIdentityExhausted => {
                formatter.write_str("writer lease identity space exhausted")
            }
            Self::ManagedSegment { marker_path } => write!(
                formatter,
                "raw journal writer is forbidden inside segmented directory {}",
                marker_path.display()
            ),
            Self::InvalidInitialSequence => {
                formatter.write_str("initial journal sequence must be non-zero")
            }
            Self::SequenceExhausted => formatter.write_str("journal sequence exhausted"),
            Self::FrameLengthOverflow => formatter.write_str("journal frame length overflow"),
            Self::CapacityReservationFailed { resource, maximum } => {
                format_reservation_failure(formatter, *resource, *maximum)
            }
            Self::CheckpointAnchorSequenceMismatch { current, proposed } => write!(
                formatter,
                "checkpoint anchor sequence {proposed} is not retained within the current WAL ending at {current:?}"
            ),
            Self::CutoverPendingExists { path } => write!(
                formatter,
                "checkpoint cutover pending file {} already exists",
                path.display()
            ),
            Self::CutoverPendingMissing { path } => write!(
                formatter,
                "checkpoint cutover pending file {} does not exist",
                path.display()
            ),
            Self::PayloadTooLarge { length, maximum } => {
                format_payload_too_large(formatter, *length, *maximum)
            }
            Self::TruncatedFrame {
                offset,
                expected_length,
                available_length,
            } => write!(
                formatter,
                "truncated frame at {offset}: requires {expected_length} bytes, has {available_length}"
            ),
            Self::InvalidMagic { offset, found } => {
                write!(formatter, "invalid journal magic at {offset}: {found:02X?}")
            }
            Self::UnsupportedVersion { offset, version } => {
                write!(
                    formatter,
                    "unsupported journal version {version} at {offset}"
                )
            }
            Self::UnknownRecordKind { offset, value } => {
                write!(formatter, "unknown journal record kind {value} at {offset}")
            }
            Self::ChecksumMismatch {
                offset,
                expected,
                actual,
            } => write!(
                formatter,
                "journal checksum mismatch at {offset}: stored {expected:#010x}, calculated {actual:#010x}"
            ),
            Self::SequenceDiscontinuity {
                offset,
                expected,
                actual,
            } => write!(
                formatter,
                "journal sequence discontinuity at {offset}: expected {expected}, found {actual}"
            ),
            Self::RecordKindMismatch { expected, actual } => {
                write!(
                    formatter,
                    "record kind mismatch: expected {expected:?}, found {actual:?}"
                )
            }
        }
    }
}

fn format_reservation_failure(
    formatter: &mut fmt::Formatter<'_>,
    resource: JournalResource,
    maximum: usize,
) -> fmt::Result {
    write!(
        formatter,
        "failed to reserve journal {resource} through {maximum}"
    )
}

fn format_payload_too_large(
    formatter: &mut fmt::Formatter<'_>,
    length: usize,
    maximum: u32,
) -> fmt::Result {
    write!(
        formatter,
        "payload length {length} exceeds configured maximum {maximum}"
    )
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Codec(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for JournalError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug)]
pub(crate) struct WriterLease {
    path: PathBuf,
    owner: WriterLeaseOwner,
    file: Option<File>,
}

impl WriterLease {
    pub(crate) const fn owner(&self) -> WriterLeaseOwner {
        self.owner
    }

    pub(crate) fn acquire(journal_path: &Path) -> Result<Self, JournalError> {
        let path = writer_lease_path(journal_path);
        let nonce = next_lease_nonce()?;
        let owner = WriterLeaseOwner {
            process_id: std::process::id(),
            acquired_at_unix_nanos: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            nonce,
        };
        let mut file = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                let actual = read_writer_lease(&path)?;
                return Err(JournalError::WriterLeaseHeld {
                    lease_path: path,
                    owner: actual,
                });
            }
            Err(error) => return Err(JournalError::Io(error)),
        };
        let bytes = encode_writer_lease(owner);
        if let Err(error) = file
            .write_all(&bytes)
            .and_then(|()| file.sync_all())
            .and_then(|()| sync_parent(&path))
        {
            drop(file);
            let _ = fs::remove_file(&path);
            let _ = sync_parent(&path);
            return Err(JournalError::Io(error));
        }
        Ok(Self {
            path,
            owner,
            file: Some(file),
        })
    }

    pub(crate) fn release(&mut self) -> Result<(), JournalError> {
        self.file.take();
        let current = match read_writer_lease(&self.path) {
            Ok(owner) => owner,
            Err(JournalError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if current != self.owner {
            return Err(JournalError::WriterLeaseOwnerMismatch {
                expected: self.owner,
                actual: current,
            });
        }
        fs::remove_file(&self.path)?;
        sync_parent(&self.path)?;
        Ok(())
    }
}

impl Drop for WriterLease {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

pub(crate) fn normalize_journal_path(path: &Path) -> io::Result<PathBuf> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let file_name = path.file_name().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "journal path has no file name")
            })?;
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            let parent = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            Ok(fs::canonicalize(parent)?.join(file_name))
        }
        Err(error) => Err(error),
    }
}

fn next_lease_nonce() -> Result<u64, JournalError> {
    let mut current = NEXT_LEASE_NONCE.load(Ordering::Relaxed);
    loop {
        let next = current
            .checked_add(1)
            .ok_or(JournalError::WriterLeaseIdentityExhausted)?;
        match NEXT_LEASE_NONCE.compare_exchange_weak(
            current,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return Ok(current),
            Err(actual) => current = actual,
        }
    }
}

fn writer_lease_path(journal_path: &Path) -> PathBuf {
    let mut value = journal_path.as_os_str().to_os_string();
    value.push(".writer.lock");
    PathBuf::from(value)
}

pub(crate) fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()
}

fn encode_writer_lease(owner: WriterLeaseOwner) -> [u8; LEASE_LENGTH] {
    let mut bytes = [0_u8; LEASE_LENGTH];
    bytes[0..4].copy_from_slice(&LEASE_MAGIC);
    bytes[4..6].copy_from_slice(&LEASE_VERSION.to_le_bytes());
    bytes[6..10].copy_from_slice(&owner.process_id.to_le_bytes());
    bytes[10..26].copy_from_slice(&owner.acquired_at_unix_nanos.to_le_bytes());
    bytes[26..34].copy_from_slice(&owner.nonce.to_le_bytes());
    bytes
}

fn read_writer_lease(path: &Path) -> Result<WriterLeaseOwner, JournalError> {
    let bytes = fs::read(path)?;
    if bytes.len() != LEASE_LENGTH
        || bytes[0..4] != LEASE_MAGIC
        || u16::from_le_bytes(
            bytes[4..6]
                .try_into()
                .expect("lease version has a fixed width"),
        ) != LEASE_VERSION
    {
        return Err(JournalError::InvalidWriterLease {
            lease_path: path.to_path_buf(),
        });
    }
    Ok(WriterLeaseOwner {
        process_id: u32::from_le_bytes(
            bytes[6..10]
                .try_into()
                .expect("lease process ID has a fixed width"),
        ),
        acquired_at_unix_nanos: u128::from_le_bytes(
            bytes[10..26]
                .try_into()
                .expect("lease acquisition time has a fixed width"),
        ),
        nonce: u64::from_le_bytes(
            bytes[26..34]
                .try_into()
                .expect("lease nonce has a fixed width"),
        ),
    })
}

/// Single-writer append-only journal.
#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
    file: File,
    writer_lease: Option<WriterLease>,
    options: JournalOptions,
    recovery: RecoveryReport,
    next_sequence: u64,
    offset: u64,
    poisoned: bool,
    #[cfg(test)]
    injected_fault: Option<InjectedFault>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JournalCutoverBoundary {
    initial_sequence: u64,
    wal_sequence: u64,
    suffix_offset: u64,
}

struct JournalCutoverPlan {
    current_sequence: Option<u64>,
    anchor_frame: Vec<u8>,
    anchor_frame_length: u64,
    pending_path: PathBuf,
}

struct StagedJournalCutover {
    replacement: File,
    valid_bytes: u64,
}

impl JournalCutoverBoundary {
    pub(crate) const fn wal_sequence(self) -> u64 {
        self.wal_sequence
    }

    #[cfg(test)]
    pub(crate) const fn for_test(wal_sequence: u64) -> Self {
        Self {
            initial_sequence: 1,
            wal_sequence,
            suffix_offset: 0,
        }
    }
}

fn copy_checkpoint_suffix(
    replacement: &mut File,
    anchor_frame: &[u8],
    anchor_frame_length: u64,
    reader: JournalReader,
    current_sequence: Option<u64>,
    anchor_sequence: u64,
    maximum_payload_length: u32,
) -> Result<u64, JournalError> {
    replacement.write_all(anchor_frame)?;
    let mut valid_bytes = anchor_frame_length;
    let mut copied_last_sequence = anchor_sequence;
    for result in reader {
        let suffix = result?;
        let payload_length =
            u32::try_from(suffix.payload().len()).map_err(|_| JournalError::PayloadTooLarge {
                length: suffix.payload().len(),
                maximum: maximum_payload_length,
            })?;
        let encoded = encode_frame(
            suffix.kind(),
            suffix.sequence(),
            payload_length,
            suffix.payload(),
        )?;
        replacement.write_all(&encoded)?;
        valid_bytes = valid_bytes
            .checked_add(
                u64::try_from(encoded.len()).map_err(|_| JournalError::FrameLengthOverflow)?,
            )
            .ok_or(JournalError::FrameLengthOverflow)?;
        copied_last_sequence = suffix.sequence();
    }
    if Some(copied_last_sequence) != current_sequence {
        return Err(JournalError::CheckpointAnchorSequenceMismatch {
            current: current_sequence,
            proposed: anchor_sequence,
        });
    }
    replacement.sync_all()?;
    Ok(valid_bytes)
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InjectedFault {
    PartialWrite(usize),
    DurabilityBarrier,
    CutoverDirectoryBarrier,
    SyncAll,
    SyncData,
}

impl Journal {
    /// Opens or creates a journal, verifies every existing frame, and optionally
    /// repairs an incomplete final frame.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] for I/O failure, corruption, sequence gaps,
    /// unsupported framing, oversized payloads, an existing writer lease, or a
    /// strict-mode torn tail.
    pub fn open(path: impl AsRef<Path>, options: JournalOptions) -> Result<Self, JournalError> {
        Self::open_internal(path.as_ref(), options, false)
    }

    pub(crate) fn open_managed(path: &Path, options: JournalOptions) -> Result<Self, JournalError> {
        Self::open_internal(path, options, true)
    }

    fn open_internal(
        path: &Path,
        options: JournalOptions,
        managed: bool,
    ) -> Result<Self, JournalError> {
        if options.initial_sequence == 0 {
            return Err(JournalError::InvalidInitialSequence);
        }
        let path = normalize_journal_path(path)?;
        let marker_path = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(SEGMENT_DIRECTORY_MARKER);
        if !managed && marker_path.exists() {
            return Err(JournalError::ManagedSegment { marker_path });
        }
        let writer_lease = if managed {
            None
        } else {
            Some(WriterLease::acquire(&path)?)
        };
        let (mut file, created) = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => (file, true),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (
                OpenOptions::new().read(true).write(true).open(&path)?,
                false,
            ),
            Err(error) => return Err(JournalError::Io(error)),
        };
        if created {
            file.sync_all()?;
            sync_parent(&path)?;
        }
        let mut options = options;
        let file_length = file.metadata()?.len();
        options.initial_sequence =
            effective_initial_sequence(&mut file, file_length, options.initial_sequence)?;
        let recovery = scan_and_recover(&mut file, options)?;
        let next_sequence = match recovery.last_sequence {
            Some(last) => last.checked_add(1).ok_or(JournalError::SequenceExhausted)?,
            None => options.initial_sequence,
        };
        file.seek(SeekFrom::Start(recovery.valid_bytes))?;
        Ok(Self {
            path,
            file,
            writer_lease,
            options,
            recovery,
            next_sequence,
            offset: recovery.valid_bytes,
            poisoned: false,
            #[cfg(test)]
            injected_fault: None,
        })
    }

    /// Returns the canonical journal path owned by this writer.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the diagnostic identity of this journal's exclusive writer lease.
    ///
    /// # Panics
    ///
    /// This cannot panic for a [`Journal`] returned by the public [`Journal::open`]
    /// constructor. Internally managed segment writers do not have per-file
    /// leases and are never exposed as raw [`Journal`] values.
    #[must_use]
    pub const fn writer_lease_owner(&self) -> WriterLeaseOwner {
        match &self.writer_lease {
            Some(writer_lease) => writer_lease.owner(),
            None => panic!("managed journals are not exposed as raw Journal values"),
        }
    }

    /// Resolves the canonical writer-lease sidecar path for a journal.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Io`] when the journal path or its parent cannot
    /// be canonicalized.
    pub fn writer_lease_path(path: impl AsRef<Path>) -> Result<PathBuf, JournalError> {
        let journal_path = normalize_journal_path(path.as_ref())?;
        Ok(writer_lease_path(&journal_path))
    }

    /// Resolves the deterministic staging path used for WAL prefix replacement.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Io`] when the journal path cannot be normalized.
    pub fn cutover_pending_path(path: impl AsRef<Path>) -> Result<PathBuf, JournalError> {
        let journal_path = normalize_journal_path(path.as_ref())?;
        Ok(cutover_pending_path(&journal_path))
    }

    /// Removes an abandoned writer lease only when its on-disk identity still
    /// equals the caller's previously observed identity.
    ///
    /// The caller must independently establish that the recorded process is no
    /// longer capable of writing the journal and must prevent new writers from
    /// starting until recovery returns. Owner comparison and deletion are
    /// separate filesystem operations, not an atomic compare-and-delete.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::WriterLeaseOwnerMismatch`] if another owner has
    /// replaced the observed lease, [`JournalError::InvalidWriterLease`] for a
    /// malformed lease, or [`JournalError::Io`] for filesystem failure.
    pub fn recover_abandoned_writer(
        path: impl AsRef<Path>,
        expected: WriterLeaseOwner,
    ) -> Result<(), JournalError> {
        let journal_path = normalize_journal_path(path.as_ref())?;
        let lease_path = writer_lease_path(&journal_path);
        let actual = read_writer_lease(&lease_path)?;
        if actual != expected {
            return Err(JournalError::WriterLeaseOwnerMismatch { expected, actual });
        }
        fs::remove_file(&lease_path)?;
        sync_parent(&lease_path)?;
        Ok(())
    }

    /// Removes a malformed abandoned lease after verifying that it cannot be
    /// decoded as any supported owner identity.
    ///
    /// The caller must independently prove that no writer remains live and
    /// must prevent new writers from starting until recovery returns. A valid
    /// lease is never removed by this operation.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::WriterLeaseHeld`] if the lease becomes valid, or
    /// [`JournalError::Io`] for filesystem failure.
    pub fn recover_abandoned_invalid_writer(path: impl AsRef<Path>) -> Result<(), JournalError> {
        let journal_path = normalize_journal_path(path.as_ref())?;
        let lease_path = writer_lease_path(&journal_path);
        match read_writer_lease(&lease_path) {
            Ok(owner) => {
                return Err(JournalError::WriterLeaseHeld { lease_path, owner });
            }
            Err(JournalError::InvalidWriterLease { .. }) => {}
            Err(error) => return Err(error),
        }
        fs::remove_file(&lease_path)?;
        sync_parent(&lease_path)?;
        Ok(())
    }

    /// Discards a staging file left before an interrupted WAL cutover rename.
    ///
    /// The caller must first prove the prior writer cannot resume and recover
    /// its abandoned lease. This operation acquires a new writer lease, requires
    /// the authoritative WAL path to remain present, removes only the derived
    /// staging path, synchronizes the parent directory, and releases ownership.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::WriterLeaseHeld`] while any writer lease remains,
    /// [`JournalError::CutoverPendingMissing`] when no staging file exists, or
    /// [`JournalError::Io`] for filesystem failure.
    pub fn recover_abandoned_cutover_pending(path: impl AsRef<Path>) -> Result<(), JournalError> {
        let journal_path = normalize_journal_path(path.as_ref())?;
        fs::metadata(&journal_path)?;
        let mut writer_lease = WriterLease::acquire(&journal_path)?;
        let pending_path = cutover_pending_path(&journal_path);
        match fs::remove_file(&pending_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                writer_lease.release()?;
                return Err(JournalError::CutoverPendingMissing { path: pending_path });
            }
            Err(error) => return Err(JournalError::Io(error)),
        }
        sync_parent(&pending_path)?;
        writer_lease.release()?;
        Ok(())
    }

    /// Returns the verified recovery outcome from open.
    #[must_use]
    pub const fn recovery(&self) -> RecoveryReport {
        self.recovery
    }

    /// Returns the sequence that will be assigned to the next successful append.
    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub(crate) const fn initial_sequence(&self) -> u64 {
        self.options.initial_sequence
    }

    /// Returns whether an ambiguous write or synchronization failure has made
    /// this writer unusable until reopen and recovery.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Encodes and appends one typed durable record.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] for encoding, payload limits, sequence
    /// exhaustion, I/O failure, or use after an ambiguous I/O failure.
    pub fn append<T: DurableRecord>(&mut self, record: &T) -> Result<AppendReceipt, JournalError> {
        let payload = record.encode().map_err(JournalError::Codec)?;
        self.append_raw(T::KIND, &payload)
    }

    /// Appends already encoded payload bytes under an explicit record kind.
    ///
    /// This method validates framing but intentionally does not validate the
    /// payload codec; [`JournalFrame::decode`] performs typed validation.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] for payload limits, sequence exhaustion, I/O
    /// failure, or use after an ambiguous I/O failure.
    pub fn append_raw(
        &mut self,
        kind: RecordKind,
        payload: &[u8],
    ) -> Result<AppendReceipt, JournalError> {
        if self.poisoned {
            return Err(JournalError::Poisoned);
        }
        let payload_length =
            u32::try_from(payload.len()).map_err(|_| JournalError::PayloadTooLarge {
                length: payload.len(),
                maximum: self.options.max_payload_length,
            })?;
        if payload_length > self.options.max_payload_length {
            return Err(JournalError::PayloadTooLarge {
                length: payload.len(),
                maximum: self.options.max_payload_length,
            });
        }
        let sequence = self.next_sequence;
        let next_sequence = sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        let frame_length = HEADER_LENGTH
            .checked_add(payload.len())
            .ok_or(JournalError::FrameLengthOverflow)?;
        let next_offset = self
            .offset
            .checked_add(
                u64::try_from(frame_length).map_err(|_| JournalError::FrameLengthOverflow)?,
            )
            .ok_or(JournalError::FrameLengthOverflow)?;
        let frame = encode_frame(kind, sequence, payload_length, payload)?;
        let receipt = AppendReceipt {
            sequence,
            offset: self.offset,
            frame_length: frame.len(),
            durability: self.options.durability,
        };

        if let Err(error) = self.write_bytes(&frame) {
            self.poisoned = true;
            return Err(JournalError::Io(error));
        }
        if let Err(error) = self.apply_configured_durability() {
            self.poisoned = true;
            return Err(JournalError::Io(error));
        }
        self.next_sequence = next_sequence;
        self.offset = next_offset;
        Ok(receipt)
    }

    /// Appends a heterogeneous record group using one write and durability barrier.
    ///
    /// An empty batch is a successful no-op. A failed write or durability
    /// barrier poisons the writer because the committed prefix is ambiguous
    /// until reopen and recovery.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] for payload limits, length, sequence, or buffer
    /// exhaustion, I/O failure, or use after an ambiguous I/O failure.
    pub fn append_batch(
        &mut self,
        batch: &JournalBatch,
    ) -> Result<Vec<AppendReceipt>, JournalError> {
        self.ensure_healthy()?;
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        let record_count =
            u64::try_from(batch.len()).map_err(|_| JournalError::SequenceExhausted)?;
        let next_sequence = self
            .next_sequence
            .checked_add(record_count)
            .ok_or(JournalError::SequenceExhausted)?;
        let mut total_length = 0_usize;
        for record in &batch.records {
            self.validate_payload(&record.payload)?;
            total_length = total_length
                .checked_add(HEADER_LENGTH)
                .and_then(|value| value.checked_add(record.payload.len()))
                .ok_or(JournalError::FrameLengthOverflow)?;
        }
        let next_offset = self
            .offset
            .checked_add(
                u64::try_from(total_length).map_err(|_| JournalError::FrameLengthOverflow)?,
            )
            .ok_or(JournalError::FrameLengthOverflow)?;

        let mut bytes = reserve_journal_vec(JournalResource::BatchFrameBytes, total_length)?;
        let mut receipts = reserve_journal_vec(JournalResource::BatchReceipts, batch.len())?;
        let mut offset = self.offset;
        for (sequence, record) in (self.next_sequence..).zip(&batch.records) {
            let payload_length = u32::try_from(record.payload.len())
                .map_err(|_| JournalError::FrameLengthOverflow)?;
            let frame_length = append_frame_reserved(
                &mut bytes,
                JournalResource::BatchFrameBytes,
                record.kind,
                sequence,
                payload_length,
                &record.payload,
            )?;
            receipts.push(AppendReceipt {
                sequence,
                offset,
                frame_length,
                durability: self.options.durability,
            });
            offset = offset
                .checked_add(
                    u64::try_from(frame_length).map_err(|_| JournalError::FrameLengthOverflow)?,
                )
                .ok_or(JournalError::FrameLengthOverflow)?;
        }

        if let Err(error) = self.write_bytes(&bytes) {
            self.poisoned = true;
            return Err(JournalError::Io(error));
        }
        if let Err(error) = self.apply_configured_durability() {
            self.poisoned = true;
            return Err(JournalError::Io(error));
        }
        self.next_sequence = next_sequence;
        self.offset = next_offset;
        Ok(receipts)
    }

    #[cfg(test)]
    pub(crate) fn replace_with_checkpoint_anchor(
        &mut self,
        anchor: CheckpointAnchor,
    ) -> Result<(), JournalError> {
        let boundary = self.cutover_boundary()?;
        self.replace_with_checkpoint_anchor_at(anchor, boundary)
    }

    pub(crate) fn replace_with_checkpoint_anchor_at(
        &mut self,
        anchor: CheckpointAnchor,
        boundary: JournalCutoverBoundary,
    ) -> Result<(), JournalError> {
        self.ensure_healthy()?;
        let plan = self.prepare_checkpoint_cutover(anchor, boundary)?;
        self.sync_all()?;
        let staged = self.stage_checkpoint_cutover(anchor, boundary, &plan)?;
        self.publish_checkpoint_cutover(anchor, &plan, staged)
    }

    fn prepare_checkpoint_cutover(
        &self,
        anchor: CheckpointAnchor,
        boundary: JournalCutoverBoundary,
    ) -> Result<JournalCutoverPlan, JournalError> {
        let current = if self.offset == 0 {
            None
        } else {
            Some(
                self.next_sequence
                    .checked_sub(1)
                    .ok_or(JournalError::SequenceExhausted)?,
            )
        };
        if anchor.wal_sequence() != boundary.wal_sequence
            || boundary.initial_sequence != self.options.initial_sequence
            || boundary.suffix_offset > self.offset
            || current.is_none_or(|value| anchor.wal_sequence() > value)
        {
            return Err(JournalError::CheckpointAnchorSequenceMismatch {
                current,
                proposed: anchor.wal_sequence(),
            });
        }
        let payload = anchor.encode().map_err(JournalError::Codec)?;
        self.validate_payload(&payload)?;
        let payload_length =
            u32::try_from(payload.len()).map_err(|_| JournalError::PayloadTooLarge {
                length: payload.len(),
                maximum: self.options.max_payload_length,
            })?;
        let frame = encode_frame(
            RecordKind::CheckpointAnchor,
            anchor.wal_sequence(),
            payload_length,
            &payload,
        )?;
        let anchor_frame_length =
            u64::try_from(frame.len()).map_err(|_| JournalError::FrameLengthOverflow)?;
        Ok(JournalCutoverPlan {
            current_sequence: current,
            anchor_frame: frame,
            anchor_frame_length,
            pending_path: cutover_pending_path(&self.path),
        })
    }

    fn stage_checkpoint_cutover(
        &mut self,
        anchor: CheckpointAnchor,
        boundary: JournalCutoverBoundary,
        plan: &JournalCutoverPlan,
    ) -> Result<StagedJournalCutover, JournalError> {
        let expected_suffix_sequence = boundary
            .wal_sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        let reader = JournalReader::open_suffix(
            &self.path,
            boundary.suffix_offset,
            expected_suffix_sequence,
            self.options.max_payload_length,
        )?;
        let mut replacement = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&plan.pending_path)
        {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                return Err(JournalError::CutoverPendingExists {
                    path: plan.pending_path.clone(),
                });
            }
            Err(error) => return Err(JournalError::Io(error)),
        };
        let staged = copy_checkpoint_suffix(
            &mut replacement,
            &plan.anchor_frame,
            plan.anchor_frame_length,
            reader,
            plan.current_sequence,
            anchor.wal_sequence(),
            self.options.max_payload_length,
        );
        let valid_bytes = match staged {
            Ok(value) => value,
            Err(error) => {
                drop(replacement);
                if let Err(cleanup) = fs::remove_file(&plan.pending_path)
                    .and_then(|()| sync_parent(&plan.pending_path))
                {
                    self.poisoned = true;
                    return Err(JournalError::Io(cleanup));
                }
                return Err(error);
            }
        };
        Ok(StagedJournalCutover {
            replacement,
            valid_bytes,
        })
    }

    fn publish_checkpoint_cutover(
        &mut self,
        anchor: CheckpointAnchor,
        plan: &JournalCutoverPlan,
        staged: StagedJournalCutover,
    ) -> Result<(), JournalError> {
        fs::rename(&plan.pending_path, &self.path)?;
        #[cfg(test)]
        if self.injected_fault == Some(InjectedFault::CutoverDirectoryBarrier) {
            self.injected_fault = None;
            self.poisoned = true;
            return Err(JournalError::Io(injected_io_error(
                "checkpoint-cutover directory barrier",
            )));
        }
        if let Err(error) = sync_parent(&self.path) {
            self.poisoned = true;
            return Err(JournalError::Io(error));
        }
        self.file = staged.replacement;
        self.options.initial_sequence = anchor.wal_sequence();
        self.recovery = RecoveryReport {
            last_sequence: plan.current_sequence,
            valid_bytes: staged.valid_bytes,
            truncated_bytes: 0,
        };
        self.offset = self.recovery.valid_bytes;
        Ok(())
    }

    /// Establishes a data-and-metadata durability barrier for all prior appends.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Io`] and poisons this writer if `sync_all` fails.
    pub fn sync_all(&mut self) -> Result<(), JournalError> {
        self.ensure_healthy()?;
        if let Err(error) = self.sync_all_file() {
            self.poisoned = true;
            return Err(JournalError::Io(error));
        }
        Ok(())
    }

    /// Establishes a data durability barrier for all prior appends.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Io`] and poisons this writer if `sync_data` fails.
    pub fn sync_data(&mut self) -> Result<(), JournalError> {
        self.ensure_healthy()?;
        if let Err(error) = self.sync_data_file() {
            self.poisoned = true;
            return Err(JournalError::Io(error));
        }
        Ok(())
    }

    /// Synchronizes journal data and metadata, then releases the writer lease
    /// and synchronizes its parent-directory removal.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError`] if the journal is poisoned or either
    /// synchronization/release operation fails.
    pub fn close(mut self) -> Result<(), JournalError> {
        self.sync_all()?;
        if let Some(writer_lease) = &mut self.writer_lease {
            writer_lease.release()?;
        }
        Ok(())
    }

    pub(crate) const fn current_offset(&self) -> u64 {
        self.offset
    }

    pub(crate) fn cutover_boundary(&self) -> Result<JournalCutoverBoundary, JournalError> {
        self.ensure_healthy()?;
        let wal_sequence = if self.offset == 0 {
            None
        } else {
            self.next_sequence.checked_sub(1)
        }
        .ok_or(JournalError::CheckpointAnchorSequenceMismatch {
            current: None,
            proposed: self.options.initial_sequence,
        })?;
        Ok(JournalCutoverBoundary {
            initial_sequence: self.options.initial_sequence,
            wal_sequence,
            suffix_offset: self.offset,
        })
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> io::Result<()> {
        #[cfg(test)]
        if let Some(InjectedFault::PartialWrite(limit)) = self.injected_fault {
            self.injected_fault = None;
            let length = limit.min(bytes.len());
            self.file.write_all(&bytes[..length])?;
            return Err(injected_io_error("partial journal write"));
        }
        self.file.write_all(bytes)
    }

    fn apply_configured_durability(&mut self) -> io::Result<()> {
        #[cfg(test)]
        if self.injected_fault == Some(InjectedFault::DurabilityBarrier) {
            self.injected_fault = None;
            return Err(injected_io_error("journal durability barrier"));
        }
        apply_durability(&mut self.file, self.options.durability)
    }

    fn sync_all_file(&mut self) -> io::Result<()> {
        #[cfg(test)]
        if self.injected_fault == Some(InjectedFault::SyncAll) {
            self.injected_fault = None;
            return Err(injected_io_error("journal sync_all"));
        }
        self.file.sync_all()
    }

    fn sync_data_file(&mut self) -> io::Result<()> {
        #[cfg(test)]
        if self.injected_fault == Some(InjectedFault::SyncData) {
            self.injected_fault = None;
            return Err(injected_io_error("journal sync_data"));
        }
        self.file.sync_data()
    }

    fn ensure_healthy(&self) -> Result<(), JournalError> {
        if self.poisoned {
            Err(JournalError::Poisoned)
        } else {
            Ok(())
        }
    }

    fn validate_payload(&self, payload: &[u8]) -> Result<(), JournalError> {
        let payload_length =
            u32::try_from(payload.len()).map_err(|_| JournalError::PayloadTooLarge {
                length: payload.len(),
                maximum: self.options.max_payload_length,
            })?;
        if payload_length > self.options.max_payload_length {
            Err(JournalError::PayloadTooLarge {
                length: payload.len(),
                maximum: self.options.max_payload_length,
            })
        } else {
            Ok(())
        }
    }
}

/// Streaming verifier for an immutable journal length captured at open.
#[derive(Debug)]
pub struct JournalReader {
    file: File,
    file_length: u64,
    offset: u64,
    expected_sequence: u64,
    maximum_payload_length: u32,
    finished: bool,
}

impl JournalReader {
    /// Opens a journal for streaming verification and typed decoding.
    ///
    /// The reader observes the file length at open and does not tail subsequent
    /// appends.
    ///
    /// # Errors
    ///
    /// Returns [`JournalError::Io`] when the file cannot be opened or inspected,
    /// or [`JournalError::InvalidInitialSequence`] for a zero segment sequence.
    pub fn open(path: impl AsRef<Path>, options: JournalOptions) -> Result<Self, JournalError> {
        if options.initial_sequence == 0 {
            return Err(JournalError::InvalidInitialSequence);
        }
        let mut file = File::open(path)?;
        let file_length = file.metadata()?.len();
        let initial_sequence =
            effective_initial_sequence(&mut file, file_length, options.initial_sequence)?;
        Ok(Self {
            file,
            file_length,
            offset: 0,
            expected_sequence: initial_sequence,
            maximum_payload_length: options.max_payload_length,
            finished: false,
        })
    }

    pub(crate) fn open_suffix(
        path: impl AsRef<Path>,
        offset: u64,
        expected_sequence: u64,
        maximum_payload_length: u32,
    ) -> Result<Self, JournalError> {
        if expected_sequence == 0 {
            return Err(JournalError::InvalidInitialSequence);
        }
        let file = File::open(path)?;
        let file_length = file.metadata()?.len();
        if offset > file_length {
            return Err(JournalError::TruncatedFrame {
                offset: file_length,
                expected_length: offset,
                available_length: file_length,
            });
        }
        Ok(Self {
            file,
            file_length,
            offset,
            expected_sequence,
            maximum_payload_length,
            finished: false,
        })
    }
}

fn effective_initial_sequence(
    file: &mut File,
    file_length: u64,
    configured: u64,
) -> Result<u64, JournalError> {
    if file_length < u64::try_from(HEADER_LENGTH).expect("header length fits u64") {
        return Ok(configured);
    }
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0_u8; HEADER_LENGTH];
    file.read_exact(&mut header)?;
    if header[0..4] == MAGIC
        && u16::from_le_bytes(header[4..6].try_into().expect("version width")) == FORMAT_VERSION
        && u16::from_le_bytes(header[6..8].try_into().expect("kind width"))
            == RecordKind::CheckpointAnchor.wire()
    {
        let sequence = u64::from_le_bytes(header[16..24].try_into().expect("sequence width"));
        if sequence == 0 {
            return Err(JournalError::InvalidInitialSequence);
        }
        Ok(sequence)
    } else {
        Ok(configured)
    }
}

fn cutover_pending_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(".cutover.pending");
    PathBuf::from(value)
}

impl Iterator for JournalReader {
    type Item = Result<JournalFrame, JournalError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished || self.offset == self.file_length {
            self.finished = true;
            return None;
        }
        match read_frame_at(
            &mut self.file,
            self.file_length,
            self.offset,
            self.expected_sequence,
            self.maximum_payload_length,
        ) {
            Ok(frame) => {
                self.offset += u64::try_from(frame.frame_length())
                    .expect("bounded frame length always fits in u64");
                self.expected_sequence = if let Some(next) = self.expected_sequence.checked_add(1) {
                    next
                } else {
                    self.finished = true;
                    return Some(Err(JournalError::SequenceExhausted));
                };
                Some(Ok(frame))
            }
            Err(error) => {
                self.finished = true;
                Some(Err(error))
            }
        }
    }
}

fn scan_and_recover(
    file: &mut File,
    options: JournalOptions,
) -> Result<RecoveryReport, JournalError> {
    let file_length = file.metadata()?.len();
    let mut offset = 0_u64;
    let mut expected_sequence = options.initial_sequence;
    let mut last_sequence = None;
    while offset < file_length {
        match read_frame_at(
            file,
            file_length,
            offset,
            expected_sequence,
            options.max_payload_length,
        ) {
            Ok(frame) => {
                let frame_length = u64::try_from(frame.frame_length())
                    .map_err(|_| JournalError::SequenceExhausted)?;
                offset = offset
                    .checked_add(frame_length)
                    .ok_or(JournalError::SequenceExhausted)?;
                last_sequence = Some(frame.sequence);
                expected_sequence = expected_sequence
                    .checked_add(1)
                    .ok_or(JournalError::SequenceExhausted)?;
            }
            Err(JournalError::TruncatedFrame { .. })
                if options.recovery == RecoveryMode::RepairTornTail =>
            {
                let truncated_bytes = file_length - offset;
                file.set_len(offset)?;
                file.sync_all()?;
                return Ok(RecoveryReport {
                    last_sequence,
                    valid_bytes: offset,
                    truncated_bytes,
                });
            }
            Err(error) => return Err(error),
        }
    }
    Ok(RecoveryReport {
        last_sequence,
        valid_bytes: offset,
        truncated_bytes: 0,
    })
}

fn read_frame_at(
    file: &mut File,
    file_length: u64,
    offset: u64,
    expected_sequence: u64,
    maximum_payload_length: u32,
) -> Result<JournalFrame, JournalError> {
    let available = file_length - offset;
    if available < u64::try_from(HEADER_LENGTH).expect("header length fits u64") {
        return Err(JournalError::TruncatedFrame {
            offset,
            expected_length: u64::try_from(HEADER_LENGTH).expect("header length fits u64"),
            available_length: available,
        });
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut header = [0_u8; HEADER_LENGTH];
    file.read_exact(&mut header)?;
    let found_magic = header[0..4]
        .try_into()
        .expect("four-byte slice converts to four-byte array");
    if found_magic != MAGIC {
        return Err(JournalError::InvalidMagic {
            offset,
            found: found_magic,
        });
    }
    let version = u16::from_le_bytes(header[4..6].try_into().expect("two-byte version field"));
    if version != FORMAT_VERSION {
        return Err(JournalError::UnsupportedVersion { offset, version });
    }
    let kind = RecordKind::from_wire(
        u16::from_le_bytes(header[6..8].try_into().expect("two-byte kind field")),
        offset,
    )?;
    let payload_length = u32::from_le_bytes(
        header[8..12]
            .try_into()
            .expect("four-byte payload length field"),
    );
    if payload_length > maximum_payload_length {
        return Err(JournalError::PayloadTooLarge {
            length: usize::try_from(payload_length).expect("u32 fits usize"),
            maximum: maximum_payload_length,
        });
    }
    let frame_length = u64::try_from(HEADER_LENGTH)
        .expect("header length fits u64")
        .checked_add(u64::from(payload_length))
        .ok_or(JournalError::SequenceExhausted)?;
    if available < frame_length {
        return Err(JournalError::TruncatedFrame {
            offset,
            expected_length: frame_length,
            available_length: available,
        });
    }
    let stored_checksum = u32::from_le_bytes(
        header[CHECKSUM_START..CHECKSUM_END]
            .try_into()
            .expect("four-byte checksum field"),
    );
    let sequence = u64::from_le_bytes(
        header[16..24]
            .try_into()
            .expect("eight-byte sequence field"),
    );
    let payload_length = usize::try_from(payload_length).expect("u32 fits usize");
    let mut payload = reserve_journal_vec(JournalResource::ReadPayloadBytes, payload_length)?;
    payload.resize(payload_length, 0);
    file.read_exact(&mut payload)?;
    let calculated_checksum = frame_checksum(&header, &payload);
    if stored_checksum != calculated_checksum {
        return Err(JournalError::ChecksumMismatch {
            offset,
            expected: stored_checksum,
            actual: calculated_checksum,
        });
    }
    if sequence != expected_sequence {
        return Err(JournalError::SequenceDiscontinuity {
            offset,
            expected: expected_sequence,
            actual: sequence,
        });
    }
    Ok(JournalFrame {
        offset,
        sequence,
        kind,
        payload,
    })
}

fn encode_frame(
    kind: RecordKind,
    sequence: u64,
    payload_length: u32,
    payload: &[u8],
) -> Result<Vec<u8>, JournalError> {
    let frame_length = HEADER_LENGTH
        .checked_add(payload.len())
        .ok_or(JournalError::FrameLengthOverflow)?;
    let mut frame = reserve_journal_vec(JournalResource::FrameBytes, frame_length)?;
    append_frame_reserved(
        &mut frame,
        JournalResource::FrameBytes,
        kind,
        sequence,
        payload_length,
        payload,
    )?;
    Ok(frame)
}

fn append_frame_reserved(
    output: &mut Vec<u8>,
    resource: JournalResource,
    kind: RecordKind,
    sequence: u64,
    payload_length: u32,
    payload: &[u8],
) -> Result<usize, JournalError> {
    let frame_length = HEADER_LENGTH
        .checked_add(payload.len())
        .ok_or(JournalError::FrameLengthOverflow)?;
    let final_length = output
        .len()
        .checked_add(frame_length)
        .ok_or(JournalError::FrameLengthOverflow)?;
    if final_length > output.capacity() {
        return Err(JournalError::CapacityReservationFailed {
            resource,
            maximum: final_length,
        });
    }

    let mut header = [0_u8; HEADER_LENGTH];
    header[0..4].copy_from_slice(&MAGIC);
    header[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&kind.wire().to_le_bytes());
    header[8..12].copy_from_slice(&payload_length.to_le_bytes());
    header[16..24].copy_from_slice(&sequence.to_le_bytes());
    let checksum = frame_checksum(&header, payload);
    header[CHECKSUM_START..CHECKSUM_END].copy_from_slice(&checksum.to_le_bytes());
    output.extend_from_slice(&header);
    output.extend_from_slice(payload);
    Ok(frame_length)
}

fn apply_durability(file: &mut File, durability: Durability) -> io::Result<()> {
    match durability {
        Durability::Buffered => Ok(()),
        Durability::Flush => file.flush(),
        Durability::SyncData => file.sync_data(),
        Durability::SyncAll => file.sync_all(),
    }
}

#[cfg(test)]
fn injected_io_error(operation: &'static str) -> io::Error {
    io::Error::other(format!("injected failure during {operation}"))
}

fn frame_checksum(header: &[u8; HEADER_LENGTH], payload: &[u8]) -> u32 {
    let mut state = u32::MAX;
    state = update_crc32c(state, &header[..CHECKSUM_START]);
    state = update_crc32c(state, &[0_u8; CHECKSUM_END - CHECKSUM_START]);
    state = update_crc32c(state, &header[CHECKSUM_END..]);
    state = update_crc32c(state, payload);
    !state
}

fn update_crc32c(mut state: u32, bytes: &[u8]) -> u32 {
    for &byte in bytes {
        let index = usize::try_from((state ^ u32::from(byte)) & 0xFF)
            .expect("CRC table index is at most 255");
        state = CRC32C_TABLE[index] ^ (state >> 8);
    }
    state
}

pub(crate) fn crc32c_parts(parts: &[&[u8]]) -> u32 {
    let mut state = u32::MAX;
    for part in parts {
        state = update_crc32c(state, part);
    }
    !state
}

const fn make_crc32c_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0_usize;
    let mut initial_value = 0_u32;
    while index < table.len() {
        let mut value = initial_value;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 == 0 {
                value >> 1
            } else {
                (value >> 1) ^ CRC32C_POLYNOMIAL
            };
            bit += 1;
        }
        table[index] = value;
        index += 1;
        initial_value += 1;
    }
    table
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{
        CRC32C_TABLE, Durability, HEADER_LENGTH, InjectedFault, Journal, JournalBatch,
        JournalError, JournalOptions, JournalReader, JournalResource, RecordKind, RecoveryMode,
        append_frame_reserved, cutover_pending_path, encode_frame, reserve_journal_vec,
        update_crc32c,
    };
    use crate::codec::BinaryCodec;
    use crate::snapshot::{CheckpointAnchor, CheckpointSlot, SnapshotKind};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    fn test_path(label: &str) -> PathBuf {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "quotick-unit-{label}-{}-{nonce}.wal",
            std::process::id()
        ))
    }

    fn buffered_options() -> JournalOptions {
        JournalOptions {
            durability: Durability::Buffered,
            ..JournalOptions::default()
        }
    }

    #[test]
    fn unrepresentable_journal_buffers_are_typed_by_exact_resource() {
        for resource in [
            JournalResource::FrameBytes,
            JournalResource::BatchFrameBytes,
            JournalResource::BatchReceipts,
            JournalResource::ReadPayloadBytes,
            JournalResource::SegmentedBatchReceipts,
            JournalResource::SegmentInventory,
        ] {
            assert!(matches!(
                reserve_journal_vec::<u128>(resource, usize::MAX),
                Err(JournalError::CapacityReservationFailed {
                    resource: actual,
                    maximum: usize::MAX,
                }) if actual == resource
            ));
        }
        assert!(matches!(
            JournalBatch::try_with_capacity(usize::MAX),
            Err(JournalError::CapacityReservationFailed {
                resource: JournalResource::BatchRecords,
                maximum: usize::MAX,
            })
        ));

        let mut unreserved = Vec::new();
        assert!(matches!(
            append_frame_reserved(
                &mut unreserved,
                JournalResource::FrameBytes,
                RecordKind::Command,
                1,
                0,
                &[],
            ),
            Err(JournalError::CapacityReservationFailed {
                resource: JournalResource::FrameBytes,
                maximum: HEADER_LENGTH,
            })
        ));
    }

    #[test]
    fn frame_encoding_has_stable_bytes_without_intermediate_allocation() {
        assert_eq!(
            encode_frame(RecordKind::Command, 1, 2, &[0xAA, 0xBB]).unwrap(),
            [
                0x51, 0x57, 0x41, 0x4C, 0x10, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x61, 0x26,
                0xF6, 0x0E, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xAA, 0xBB,
            ]
        );
    }

    fn remove_test_files(path: &PathBuf) {
        let _ = fs::remove_file(path);
        if let Ok(lease_path) = Journal::writer_lease_path(path) {
            let _ = fs::remove_file(lease_path);
        }
    }

    #[test]
    fn crc32c_matches_the_standard_check_value() {
        let checksum = !update_crc32c(u32::MAX, b"123456789");
        assert_eq!(checksum, 0xE306_9283);
        assert_ne!(CRC32C_TABLE[1], 0);
    }

    #[test]
    fn checkpoint_cutover_preserves_a_verified_suffix_and_writer_ownership() {
        let path = test_path("checkpoint-cutover");
        remove_test_files(&path);
        let mut journal = Journal::open(&path, buffered_options()).unwrap();
        journal.append_raw(RecordKind::Command, &[1]).unwrap();
        let boundary = journal.cutover_boundary().unwrap();
        journal
            .append_raw(RecordKind::ExecutionReport, &[2])
            .unwrap();
        let owner = journal.writer_lease_owner();
        let anchor = CheckpointAnchor::from_parts(
            SnapshotKind::MatchingCheckpoint,
            CheckpointSlot::A,
            1,
            1,
            99,
            0x1234_5678,
        );
        let mut expected_anchor = vec![2, 0];
        expected_anchor.extend_from_slice(&1_u64.to_le_bytes());
        expected_anchor.extend_from_slice(&1_u64.to_le_bytes());
        expected_anchor.extend_from_slice(&99_u64.to_le_bytes());
        expected_anchor.extend_from_slice(&0x1234_5678_u32.to_le_bytes());
        assert_eq!(anchor.encode().unwrap(), expected_anchor);
        journal
            .replace_with_checkpoint_anchor_at(anchor, boundary)
            .unwrap();
        assert_eq!(journal.writer_lease_owner(), owner);
        assert_eq!(journal.initial_sequence(), 1);
        assert_eq!(journal.next_sequence(), 3);
        journal.append_raw(RecordKind::Command, &[3]).unwrap();
        journal.close().unwrap();

        let frames = JournalReader::open(&path, buffered_options())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].sequence(), 1);
        assert_eq!(frames[0].decode::<CheckpointAnchor>().unwrap(), anchor);
        assert_eq!(frames[1].sequence(), 2);
        assert_eq!(frames[1].kind(), RecordKind::ExecutionReport);
        assert_eq!(frames[1].payload(), &[2]);
        assert_eq!(frames[2].sequence(), 3);
        assert_eq!(frames[2].kind(), RecordKind::Command);
        assert_eq!(frames[2].payload(), &[3]);
        remove_test_files(&path);
    }

    #[test]
    fn checkpoint_anchor_rejects_zero_semantic_or_physical_sequences() {
        let zero_generation = CheckpointAnchor::from_parts(
            SnapshotKind::MatchingCheckpoint,
            CheckpointSlot::A,
            0,
            1,
            99,
            0x1234_5678,
        );
        assert!(matches!(
            zero_generation.encode(),
            Err(crate::codec::CodecError::InvalidValue(
                "checkpoint anchor generation is zero"
            ))
        ));
        let zero_wal_sequence = CheckpointAnchor::from_parts(
            SnapshotKind::MatchingCheckpoint,
            CheckpointSlot::A,
            1,
            0,
            99,
            0x1234_5678,
        );
        assert!(matches!(
            zero_wal_sequence.encode(),
            Err(crate::codec::CodecError::InvalidValue(
                "checkpoint anchor WAL sequence is zero"
            ))
        ));
    }

    #[test]
    fn occupied_cutover_pending_path_is_nonmutating_and_typed() {
        let path = test_path("checkpoint-cutover-pending");
        remove_test_files(&path);
        let mut journal = Journal::open(&path, buffered_options()).unwrap();
        journal.append_raw(RecordKind::Command, &[1]).unwrap();
        let pending = cutover_pending_path(journal.path());
        fs::write(&pending, b"abandoned").unwrap();
        let anchor = CheckpointAnchor::from_parts(
            SnapshotKind::MatchingCheckpoint,
            CheckpointSlot::A,
            1,
            1,
            99,
            0x1234_5678,
        );
        assert!(matches!(
            journal.replace_with_checkpoint_anchor(anchor),
            Err(JournalError::CutoverPendingExists { path: actual }) if actual == pending
        ));
        assert_eq!(journal.next_sequence(), 2);
        assert!(!journal.is_poisoned());
        fs::remove_file(pending).unwrap();
        journal.close().unwrap();
        remove_test_files(&path);
    }

    #[test]
    fn cutover_directory_barrier_failure_reopens_the_complete_anchor() {
        let path = test_path("checkpoint-cutover-directory-barrier");
        remove_test_files(&path);
        let mut journal = Journal::open(&path, buffered_options()).unwrap();
        journal.append_raw(RecordKind::Command, &[1]).unwrap();
        let anchor = CheckpointAnchor::from_parts(
            SnapshotKind::MatchingCheckpoint,
            CheckpointSlot::A,
            1,
            1,
            99,
            0x1234_5678,
        );
        journal.injected_fault = Some(InjectedFault::CutoverDirectoryBarrier);
        assert!(matches!(
            journal.replace_with_checkpoint_anchor(anchor),
            Err(JournalError::Io(_))
        ));
        assert!(journal.is_poisoned());
        drop(journal);

        let recovered = Journal::open(&path, buffered_options()).unwrap();
        assert_eq!(recovered.initial_sequence(), 1);
        assert_eq!(recovered.next_sequence(), 2);
        let frame = JournalReader::open(&path, buffered_options())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        assert_eq!(frame.decode::<CheckpointAnchor>().unwrap(), anchor);
        recovered.close().unwrap();
        remove_test_files(&path);
    }

    #[test]
    fn injected_partial_write_poisoning_repairs_only_the_torn_frame() {
        let path = test_path("partial-write");
        remove_test_files(&path);
        let mut journal = Journal::open(&path, buffered_options()).expect("journal opens");
        journal.injected_fault = Some(InjectedFault::PartialWrite(10));
        assert!(matches!(
            journal.append_raw(RecordKind::Command, &[1, 2, 3]),
            Err(JournalError::Io(_))
        ));
        assert!(journal.is_poisoned());
        assert!(matches!(
            journal.append_raw(RecordKind::Command, &[4]),
            Err(JournalError::Poisoned)
        ));
        drop(journal);
        assert!(matches!(
            Journal::open(&path, buffered_options()),
            Err(JournalError::TruncatedFrame { .. })
        ));
        let repair = JournalOptions {
            recovery: RecoveryMode::RepairTornTail,
            ..buffered_options()
        };
        let repaired = Journal::open(&path, repair).expect("torn frame repairs");
        assert_eq!(repaired.recovery().valid_bytes, 0);
        assert_eq!(repaired.recovery().truncated_bytes, 10);
        repaired.close().expect("journal closes");
        remove_test_files(&path);
    }

    #[test]
    fn injected_barrier_failure_recovers_the_ambiguous_complete_frame() {
        let path = test_path("barrier-failure");
        remove_test_files(&path);
        let mut journal = Journal::open(&path, JournalOptions::default()).expect("journal opens");
        journal.injected_fault = Some(InjectedFault::DurabilityBarrier);
        assert!(matches!(
            journal.append_raw(RecordKind::Command, &[1, 2, 3]),
            Err(JournalError::Io(_))
        ));
        assert!(journal.is_poisoned());
        drop(journal);

        let recovered = Journal::open(&path, buffered_options()).expect("journal recovers");
        assert_eq!(recovered.recovery().last_sequence, Some(1));
        assert_eq!(recovered.next_sequence(), 2);
        recovered.close().expect("journal closes");
        remove_test_files(&path);
    }

    #[test]
    fn injected_batch_failure_retains_only_its_complete_verified_prefix() {
        let path = test_path("batch-partial");
        remove_test_files(&path);
        let mut batch = JournalBatch::new();
        batch
            .push_raw(RecordKind::Command, vec![1; 8])
            .expect("first raw record reserves");
        batch
            .push_raw(RecordKind::Command, vec![2; 8])
            .expect("second raw record reserves");
        let mut journal = Journal::open(&path, buffered_options()).expect("journal opens");
        journal.injected_fault = Some(InjectedFault::PartialWrite(HEADER_LENGTH + 8 + 5));
        assert!(matches!(
            journal.append_batch(&batch),
            Err(JournalError::Io(_))
        ));
        drop(journal);

        let repair = JournalOptions {
            recovery: RecoveryMode::RepairTornTail,
            ..buffered_options()
        };
        let recovered = Journal::open(&path, repair).expect("batch prefix recovers");
        assert_eq!(recovered.recovery().last_sequence, Some(1));
        assert_eq!(
            recovered.recovery().valid_bytes,
            u64::try_from(HEADER_LENGTH + 8).expect("frame length fits u64")
        );
        assert_eq!(recovered.recovery().truncated_bytes, 5);
        recovered.close().expect("journal closes");
        remove_test_files(&path);
    }

    #[test]
    fn injected_explicit_sync_failures_poison_without_changing_logical_offsets() {
        let path = test_path("explicit-sync");
        remove_test_files(&path);
        let mut journal = Journal::open(&path, buffered_options()).expect("journal opens");
        journal
            .append_raw(RecordKind::Command, &[1])
            .expect("buffered append succeeds");
        journal.injected_fault = Some(InjectedFault::SyncData);
        assert!(matches!(journal.sync_data(), Err(JournalError::Io(_))));
        assert!(journal.is_poisoned());
        drop(journal);

        let mut recovered = Journal::open(&path, buffered_options()).expect("journal recovers");
        assert_eq!(recovered.next_sequence(), 2);
        recovered.injected_fault = Some(InjectedFault::SyncAll);
        assert!(matches!(recovered.sync_all(), Err(JournalError::Io(_))));
        assert!(recovered.is_poisoned());
        drop(recovered);
        remove_test_files(&path);
    }
}
