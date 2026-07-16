//! Versioned, checksummed semantic snapshot files with explicit crash recovery.

use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use crate::auction_engine::CallAuctionCheckpoint;
use crate::auction_risk::CallAuctionRiskCheckpoint;
use crate::codec::{BinaryCodec, CodecError};
use crate::journal::{
    Journal, JournalError, SEGMENT_DIRECTORY_MARKER, WriterLease, WriterLeaseOwner, crc32c_parts,
    normalize_journal_path, sync_parent,
};
use crate::ledger::LedgerCheckpoint;
use crate::matching::OrderBookCheckpoint;
use crate::risk::RiskManagedCheckpoint;

const MAGIC: [u8; 4] = *b"QSNP";
const VERSION: u16 = 16;
const HEADER_LENGTH: usize = 28;
const CHECKSUM_START: usize = 16;
const CHECKSUM_END: usize = 20;
const DEFAULT_MAXIMUM_PAYLOAD_BYTES: u64 = 1_073_741_824;

/// Stable semantic payload identifier in a `QSNP` header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum SnapshotKind {
    /// Canonical ledger balances and complete transaction history.
    LedgerCheckpoint = 1,
    /// Canonical matching state and complete command idempotency history.
    MatchingCheckpoint = 2,
    /// Coupled matching, risk-profile, position, and reservation state.
    RiskManagedCheckpoint = 3,
    /// Canonical call-auction phase, book, counters, and command history.
    CallAuctionCheckpoint = 4,
    /// Coupled call-auction profiles, positions, reservations, and history.
    CallAuctionRiskCheckpoint = 5,
}

impl SnapshotKind {
    const fn wire(self) -> u16 {
        self as u16
    }

    pub(crate) const fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::LedgerCheckpoint),
            2 => Some(Self::MatchingCheckpoint),
            3 => Some(Self::RiskManagedCheckpoint),
            4 => Some(Self::CallAuctionCheckpoint),
            5 => Some(Self::CallAuctionRiskCheckpoint),
            _ => None,
        }
    }
}

/// One of two deterministic checkpoint files used by crash-safe WAL cutover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointSlot {
    /// First alternating checkpoint file.
    A,
    /// Second alternating checkpoint file.
    B,
}

impl CheckpointSlot {
    pub(crate) const fn wire(self) -> u8 {
        match self {
            Self::A => 0,
            Self::B => 1,
        }
    }

    pub(crate) const fn from_wire(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::A),
            1 => Some(Self::B),
            _ => None,
        }
    }

    pub(crate) const fn alternate(self) -> Self {
        match self {
            Self::A => Self::B,
            Self::B => Self::A,
        }
    }
}

mod sealed {
    pub trait SnapshotPayload {}

    impl SnapshotPayload for crate::ledger::LedgerCheckpoint {}
    impl SnapshotPayload for crate::matching::OrderBookCheckpoint {}
    impl SnapshotPayload for crate::risk::RiskManagedCheckpoint {}
    impl SnapshotPayload for crate::auction_engine::CallAuctionCheckpoint {}
    impl SnapshotPayload for crate::auction_risk::CallAuctionRiskCheckpoint {}
}

/// Typed semantic value with a crate-controlled stable snapshot-kind assignment.
///
/// This trait is sealed so downstream implementations cannot claim an existing
/// version-3 kind tag with a different codec.
pub trait SnapshotPayload: BinaryCodec + sealed::SnapshotPayload {
    /// Stable envelope payload kind.
    const KIND: SnapshotKind;

    /// Monotonic semantic generation used to order pending recovery.
    fn generation(&self) -> u64;

    /// Returns whether this value is a valid same-lineage successor of `previous`.
    fn is_successor_of(&self, previous: &Self) -> bool;
}

impl SnapshotPayload for LedgerCheckpoint {
    const KIND: SnapshotKind = SnapshotKind::LedgerCheckpoint;

    fn generation(&self) -> u64 {
        self.generation()
    }

    fn is_successor_of(&self, previous: &Self) -> bool {
        self.generation() >= previous.generation() && self.records().starts_with(previous.records())
    }
}

impl SnapshotPayload for OrderBookCheckpoint {
    const KIND: SnapshotKind = SnapshotKind::MatchingCheckpoint;

    fn generation(&self) -> u64 {
        self.generation()
    }

    fn is_successor_of(&self, previous: &Self) -> bool {
        self.is_successor_of(previous)
    }
}

impl SnapshotPayload for RiskManagedCheckpoint {
    const KIND: SnapshotKind = SnapshotKind::RiskManagedCheckpoint;

    fn generation(&self) -> u64 {
        self.generation()
    }

    fn is_successor_of(&self, previous: &Self) -> bool {
        self.is_successor_of(previous)
    }
}

impl SnapshotPayload for CallAuctionCheckpoint {
    const KIND: SnapshotKind = SnapshotKind::CallAuctionCheckpoint;

    fn generation(&self) -> u64 {
        self.generation()
    }

    fn is_successor_of(&self, previous: &Self) -> bool {
        self.is_successor_of(previous)
    }
}

impl SnapshotPayload for CallAuctionRiskCheckpoint {
    const KIND: SnapshotKind = SnapshotKind::CallAuctionRiskCheckpoint;

    fn generation(&self) -> u64 {
        self.generation()
    }

    fn is_successor_of(&self, previous: &Self) -> bool {
        self.is_successor_of(previous)
    }
}

/// Bounded snapshot decoding configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotOptions {
    /// Maximum encoded semantic payload bytes accepted or emitted.
    pub maximum_payload_bytes: u64,
}

impl Default for SnapshotOptions {
    fn default() -> Self {
        Self {
            maximum_payload_bytes: DEFAULT_MAXIMUM_PAYLOAD_BYTES,
        }
    }
}

/// Successful snapshot replacement metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotReceipt {
    generation: u64,
    payload_length: u64,
    checksum: u32,
}

impl SnapshotReceipt {
    /// Returns the semantic generation persisted in the envelope.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns encoded semantic payload bytes, excluding the 28-byte header.
    #[must_use]
    pub const fn payload_length(self) -> u64 {
        self.payload_length
    }

    /// Returns the stored CRC-32C over the zeroed header and payload.
    #[must_use]
    pub const fn checksum(self) -> u32 {
        self.checksum
    }
}

/// Identity of one synchronized semantic checkpoint embedded in a compacted WAL.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointAnchor {
    kind: SnapshotKind,
    slot: CheckpointSlot,
    generation: u64,
    wal_sequence: u64,
    payload_length: u64,
    checksum: u32,
}

impl CheckpointAnchor {
    /// Constructs an anchor from the exact receipt returned after snapshot synchronization.
    #[must_use]
    pub const fn new(
        kind: SnapshotKind,
        slot: CheckpointSlot,
        receipt: SnapshotReceipt,
        wal_sequence: u64,
    ) -> Self {
        Self {
            kind,
            slot,
            generation: receipt.generation,
            wal_sequence,
            payload_length: receipt.payload_length,
            checksum: receipt.checksum,
        }
    }

    /// Returns the anchored semantic payload kind.
    #[must_use]
    pub const fn kind(self) -> SnapshotKind {
        self.kind
    }

    /// Returns the alternating checkpoint slot selected by this WAL generation.
    #[must_use]
    pub const fn slot(self) -> CheckpointSlot {
        self.slot
    }

    /// Returns the semantic checkpoint generation.
    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns the physical sequence assigned to the anchor frame.
    #[must_use]
    pub const fn wal_sequence(self) -> u64 {
        self.wal_sequence
    }

    /// Returns the anchored encoded semantic payload length.
    #[must_use]
    pub const fn payload_length(self) -> u64 {
        self.payload_length
    }

    /// Returns the anchored complete snapshot-envelope CRC-32C.
    #[must_use]
    pub const fn checksum(self) -> u32 {
        self.checksum
    }

    /// Returns whether a read snapshot receipt exactly matches this anchor.
    #[must_use]
    pub const fn matches_receipt(self, receipt: SnapshotReceipt) -> bool {
        self.generation == receipt.generation
            && self.payload_length == receipt.payload_length
            && self.checksum == receipt.checksum
    }

    pub(crate) const fn from_parts(
        kind: SnapshotKind,
        slot: CheckpointSlot,
        generation: u64,
        wal_sequence: u64,
        payload_length: u64,
        checksum: u32,
    ) -> Self {
        Self {
            kind,
            slot,
            generation,
            wal_sequence,
            payload_length,
            checksum,
        }
    }
}

/// Successful two-slot checkpoint and physical WAL cutover.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointCutoverReceipt {
    snapshot: SnapshotReceipt,
    slot: CheckpointSlot,
    wal_first_sequence: u64,
}

impl CheckpointCutoverReceipt {
    pub(crate) const fn new(
        snapshot: SnapshotReceipt,
        slot: CheckpointSlot,
        wal_first_sequence: u64,
    ) -> Self {
        Self {
            snapshot,
            slot,
            wal_first_sequence,
        }
    }

    /// Returns the synchronized checkpoint receipt.
    #[must_use]
    pub const fn snapshot(self) -> SnapshotReceipt {
        self.snapshot
    }

    /// Returns the checkpoint slot selected by the new WAL anchor.
    #[must_use]
    pub const fn slot(self) -> CheckpointSlot {
        self.slot
    }

    /// Returns the first sequence retained by the replacement WAL.
    #[must_use]
    pub const fn wal_first_sequence(self) -> u64 {
        self.wal_first_sequence
    }
}

/// Resolution applied to an abandoned `.pending` snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingSnapshotRecovery {
    /// A complete pending generation replaced an absent or older current file.
    Promoted {
        /// Promoted semantic generation.
        generation: u64,
    },
    /// An incomplete, corrupt, or semantically invalid pending file was removed.
    DiscardedInvalid,
    /// A pending generation older than the current generation was removed.
    DiscardedStale {
        /// Removed pending generation.
        pending_generation: u64,
        /// Retained current generation.
        current_generation: u64,
    },
    /// Byte-identical current and pending files at one generation were deduplicated.
    DiscardedRedundant {
        /// Retained semantic generation.
        generation: u64,
    },
}

/// Snapshot framing, ownership, codec, or filesystem failure.
#[derive(Debug)]
pub enum SnapshotError {
    /// Snapshot or directory filesystem operation failed.
    Io(io::Error),
    /// Typed semantic payload encoding or validation failed.
    Codec(CodecError),
    /// Snapshot writer-lease acquisition or release failed.
    WriterLease(JournalError),
    /// Encoded or declared payload exceeded the configured bound.
    PayloadTooLarge {
        /// Encoded or declared bytes.
        actual: u64,
        /// Configured maximum bytes.
        maximum: u64,
    },
    /// Physical file was shorter than the fixed header.
    TruncatedHeader {
        /// Physical bytes present.
        actual: u64,
    },
    /// Header magic was not `QSNP`.
    InvalidMagic([u8; 4]),
    /// Header version is unsupported.
    UnsupportedVersion(u16),
    /// Header payload kind differs from the requested Rust type.
    UnexpectedKind {
        /// Requested stable kind.
        expected: SnapshotKind,
        /// Stored raw kind.
        actual: u16,
    },
    /// Declared payload length did not equal physical payload bytes.
    LengthMismatch {
        /// Header-declared payload bytes.
        declared: u64,
        /// Physical payload bytes.
        actual: u64,
    },
    /// CRC-32C did not match the zeroed header and payload.
    ChecksumMismatch {
        /// Header checksum.
        expected: u32,
        /// Recalculated checksum.
        actual: u32,
    },
    /// Header generation differed from the decoded semantic generation.
    GenerationMismatch {
        /// Header generation.
        header: u64,
        /// Payload generation.
        payload: u64,
    },
    /// An abandoned `.pending` file requires explicit recovery.
    PendingSnapshotExists(PathBuf),
    /// No pending file exists to recover.
    NoPendingSnapshot(PathBuf),
    /// Current snapshot is invalid, so pending ordering cannot be proven.
    CurrentSnapshotInvalid(PathBuf),
    /// The target is inside a closed segmented-journal directory.
    ManagedSegmentDirectory {
        /// Marker proving that the parent directory belongs to the segment manager.
        marker_path: PathBuf,
    },
    /// Current and pending payloads differ under the same generation.
    SameGenerationDivergence {
        /// Conflicting semantic generation.
        generation: u64,
    },
    /// A normal write proposed a generation older than the current snapshot.
    GenerationRegression {
        /// Current semantic generation.
        current: u64,
        /// Proposed semantic generation.
        proposed: u64,
    },
    /// Generations were ordered but their semantic histories were not prefix-related.
    LineageDivergence {
        /// Current semantic generation.
        current: u64,
        /// Proposed or pending semantic generation.
        proposed: u64,
    },
    /// Header-plus-payload length exceeded addressable arithmetic.
    LengthOverflow,
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "snapshot I/O error: {error}"),
            Self::Codec(error) => write!(formatter, "snapshot payload error: {error}"),
            Self::WriterLease(error) => error.fmt(formatter),
            Self::PayloadTooLarge { actual, maximum } => write!(
                formatter,
                "snapshot payload has {actual} bytes, exceeding maximum {maximum}"
            ),
            Self::TruncatedHeader { actual } => write!(
                formatter,
                "snapshot has {actual} bytes, fewer than the {HEADER_LENGTH}-byte header"
            ),
            Self::InvalidMagic(found) => {
                write!(formatter, "invalid snapshot magic: {found:02X?}")
            }
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported snapshot version {version}")
            }
            Self::UnexpectedKind { expected, actual } => write!(
                formatter,
                "snapshot kind {actual} differs from requested {expected:?}"
            ),
            Self::LengthMismatch { declared, actual } => write!(
                formatter,
                "snapshot declares {declared} payload bytes but has {actual}"
            ),
            Self::ChecksumMismatch { expected, actual } => write!(
                formatter,
                "snapshot checksum mismatch: stored {expected:#010x}, calculated {actual:#010x}"
            ),
            Self::GenerationMismatch { header, payload } => write!(
                formatter,
                "snapshot header generation {header} differs from payload generation {payload}"
            ),
            Self::PendingSnapshotExists(path) => {
                write!(
                    formatter,
                    "pending snapshot {} already exists",
                    path.display()
                )
            }
            Self::NoPendingSnapshot(path) => {
                write!(
                    formatter,
                    "pending snapshot {} does not exist",
                    path.display()
                )
            }
            Self::CurrentSnapshotInvalid(path) => write!(
                formatter,
                "current snapshot {} is invalid; pending ordering is unknown",
                path.display()
            ),
            Self::ManagedSegmentDirectory { marker_path } => write!(
                formatter,
                "snapshot target is inside segmented-journal directory marked by {}",
                marker_path.display()
            ),
            Self::SameGenerationDivergence { generation } => write!(
                formatter,
                "current and pending snapshots diverge at generation {generation}"
            ),
            Self::GenerationRegression { current, proposed } => write!(
                formatter,
                "snapshot generation regression from {current} to {proposed}"
            ),
            Self::LineageDivergence { current, proposed } => write!(
                formatter,
                "snapshot generation {proposed} does not extend generation {current}"
            ),
            Self::LengthOverflow => formatter.write_str("snapshot length overflow"),
        }
    }
}

impl std::error::Error for SnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::WriterLease(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for SnapshotError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<CodecError> for SnapshotError {
    fn from(error: CodecError) -> Self {
        Self::Codec(error)
    }
}

#[derive(Debug)]
struct LoadedSnapshot<T> {
    value: T,
    payload: Vec<u8>,
    receipt: SnapshotReceipt,
}

/// Namespace for strict snapshot read, replacement, and pending recovery.
#[derive(Clone, Copy, Debug, Default)]
pub struct SnapshotFile;

impl SnapshotFile {
    /// Derives one of the two checkpoint-slot paths from an operator-selected base path.
    #[must_use]
    pub fn slot_path(path: impl AsRef<Path>, slot: CheckpointSlot) -> PathBuf {
        let mut value: OsString = path.as_ref().as_os_str().to_os_string();
        value.push(match slot {
            CheckpointSlot::A => ".cutover-a",
            CheckpointSlot::B => ".cutover-b",
        });
        PathBuf::from(value)
    }

    /// Fully writes and synchronizes `<path>.pending`, atomically renames it to
    /// `path`, synchronizes the parent directory, and releases writer ownership.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] for encoding, size, pending-file, ownership,
    /// write, synchronization, rename, or directory-barrier failure.
    pub fn write<T: SnapshotPayload>(
        path: impl AsRef<Path>,
        value: &T,
        options: SnapshotOptions,
    ) -> Result<SnapshotReceipt, SnapshotError> {
        let payload = value.encode()?;
        let payload_length =
            u64::try_from(payload.len()).map_err(|_| SnapshotError::LengthOverflow)?;
        validate_payload_length(payload_length, options)?;
        let path = normalize_journal_path(path.as_ref())?;
        ensure_not_segmented_member(&path)?;
        let pending_path = Self::pending_path(&path);
        let mut writer_lease = WriterLease::acquire(&path).map_err(SnapshotError::WriterLease)?;
        if pending_path.exists() {
            writer_lease.release().map_err(SnapshotError::WriterLease)?;
            return Err(SnapshotError::PendingSnapshotExists(pending_path));
        }
        let (header, receipt) = encode_header(T::KIND, value.generation(), &payload)?;
        match fs::metadata(&path) {
            Ok(_) => match read_loaded::<T>(&path, options) {
                Ok(current) if receipt.generation < current.receipt.generation => {
                    writer_lease.release().map_err(SnapshotError::WriterLease)?;
                    return Err(SnapshotError::GenerationRegression {
                        current: current.receipt.generation,
                        proposed: receipt.generation,
                    });
                }
                Ok(current)
                    if receipt.generation == current.receipt.generation
                        && payload == current.payload =>
                {
                    writer_lease.release().map_err(SnapshotError::WriterLease)?;
                    return Ok(current.receipt);
                }
                Ok(current) if receipt.generation == current.receipt.generation => {
                    writer_lease.release().map_err(SnapshotError::WriterLease)?;
                    return Err(SnapshotError::SameGenerationDivergence {
                        generation: receipt.generation,
                    });
                }
                Ok(current) if !value.is_successor_of(&current.value) => {
                    writer_lease.release().map_err(SnapshotError::WriterLease)?;
                    return Err(SnapshotError::LineageDivergence {
                        current: current.receipt.generation,
                        proposed: receipt.generation,
                    });
                }
                Ok(_) => {}
                Err(error) if is_content_failure(&error) => {
                    writer_lease.release().map_err(SnapshotError::WriterLease)?;
                    return Err(SnapshotError::CurrentSnapshotInvalid(path));
                }
                Err(error) => return Err(error),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut pending = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&pending_path)?;
        pending.write_all(&header)?;
        pending.write_all(&payload)?;
        pending.sync_all()?;
        fs::rename(&pending_path, &path)?;
        sync_parent(&path)?;
        writer_lease.release().map_err(SnapshotError::WriterLease)?;
        Ok(receipt)
    }

    /// Reads, bounds, verifies, decodes, and semantically validates one snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] for framing, length, checksum, kind,
    /// generation, codec, validation, or filesystem failure.
    pub fn read<T: SnapshotPayload>(
        path: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<T, SnapshotError> {
        Ok(read_loaded::<T>(path.as_ref(), options)?.value)
    }

    pub(crate) fn read_with_receipt<T: SnapshotPayload>(
        path: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<(T, SnapshotReceipt), SnapshotError> {
        let loaded = read_loaded::<T>(path.as_ref(), options)?;
        Ok((loaded.value, loaded.receipt))
    }

    /// Resolves an abandoned `.pending` file under exclusive snapshot ownership.
    ///
    /// Complete newer files are promoted, byte-identical or stale files are
    /// removed, invalid files are discarded, and same-generation divergence or
    /// invalid current state fails closed without changing either file.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] for absent pending state, ownership,
    /// filesystem failure, invalid current state, or same-generation divergence.
    pub fn recover_pending<T: SnapshotPayload>(
        path: impl AsRef<Path>,
        options: SnapshotOptions,
    ) -> Result<PendingSnapshotRecovery, SnapshotError> {
        let path = normalize_journal_path(path.as_ref())?;
        ensure_not_segmented_member(&path)?;
        let pending_path = Self::pending_path(&path);
        let mut writer_lease = WriterLease::acquire(&path).map_err(SnapshotError::WriterLease)?;
        if !pending_path.exists() {
            writer_lease.release().map_err(SnapshotError::WriterLease)?;
            return Err(SnapshotError::NoPendingSnapshot(pending_path));
        }
        let pending = match read_loaded::<T>(&pending_path, options) {
            Ok(value) => value,
            Err(error) if is_discardable_pending_failure(&error) => {
                remove_and_sync(&pending_path)?;
                writer_lease.release().map_err(SnapshotError::WriterLease)?;
                return Ok(PendingSnapshotRecovery::DiscardedInvalid);
            }
            Err(error) => return Err(error),
        };
        let current = match fs::metadata(&path) {
            Ok(_) => match read_loaded::<T>(&path, options) {
                Ok(value) => Some(value),
                Err(error) if is_content_failure(&error) => {
                    writer_lease.release().map_err(SnapshotError::WriterLease)?;
                    return Err(SnapshotError::CurrentSnapshotInvalid(path));
                }
                Err(error) => return Err(error),
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let pending_generation = pending.receipt.generation;
        let result = match current {
            None => {
                promote_and_sync(&pending_path, &path)?;
                PendingSnapshotRecovery::Promoted {
                    generation: pending_generation,
                }
            }
            Some(current)
                if pending_generation > current.receipt.generation
                    && !pending.value.is_successor_of(&current.value) =>
            {
                writer_lease.release().map_err(SnapshotError::WriterLease)?;
                return Err(SnapshotError::LineageDivergence {
                    current: current.receipt.generation,
                    proposed: pending_generation,
                });
            }
            Some(current) if pending_generation > current.receipt.generation => {
                promote_and_sync(&pending_path, &path)?;
                PendingSnapshotRecovery::Promoted {
                    generation: pending_generation,
                }
            }
            Some(current)
                if pending_generation < current.receipt.generation
                    && !current.value.is_successor_of(&pending.value) =>
            {
                writer_lease.release().map_err(SnapshotError::WriterLease)?;
                return Err(SnapshotError::LineageDivergence {
                    current: current.receipt.generation,
                    proposed: pending_generation,
                });
            }
            Some(current) if pending_generation < current.receipt.generation => {
                remove_and_sync(&pending_path)?;
                PendingSnapshotRecovery::DiscardedStale {
                    pending_generation,
                    current_generation: current.receipt.generation,
                }
            }
            Some(current) if pending.payload == current.payload => {
                remove_and_sync(&pending_path)?;
                PendingSnapshotRecovery::DiscardedRedundant {
                    generation: pending_generation,
                }
            }
            Some(_) => {
                writer_lease.release().map_err(SnapshotError::WriterLease)?;
                return Err(SnapshotError::SameGenerationDivergence {
                    generation: pending_generation,
                });
            }
        };
        writer_lease.release().map_err(SnapshotError::WriterLease)?;
        Ok(result)
    }

    /// Returns the deterministic pending-file name for a snapshot path.
    #[must_use]
    pub fn pending_path(path: impl AsRef<Path>) -> PathBuf {
        let mut value: OsString = path.as_ref().as_os_str().to_os_string();
        value.push(".pending");
        PathBuf::from(value)
    }

    /// Resolves the canonical snapshot writer-lease sidecar path.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::WriterLease`] if the snapshot path cannot be normalized.
    pub fn writer_lease_path(path: impl AsRef<Path>) -> Result<PathBuf, SnapshotError> {
        Journal::writer_lease_path(path).map_err(SnapshotError::WriterLease)
    }

    /// Removes an abandoned snapshot lease only when its identity still equals
    /// the previously observed owner.
    ///
    /// The caller must independently prove owner termination and externally
    /// quiesce new writer starts until recovery returns.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::WriterLease`] for identity or filesystem failure.
    pub fn recover_abandoned_writer(
        path: impl AsRef<Path>,
        owner: WriterLeaseOwner,
    ) -> Result<(), SnapshotError> {
        let path = normalize_journal_path(path.as_ref())?;
        ensure_not_segmented_member(&path)?;
        Journal::recover_abandoned_writer(path, owner).map_err(SnapshotError::WriterLease)
    }

    /// Removes a malformed abandoned snapshot lease after verifying that it
    /// cannot be decoded as a valid owner identity.
    ///
    /// The caller must independently establish writer quiescence.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError::WriterLease`] if the lease becomes valid or a
    /// filesystem operation fails.
    pub fn recover_abandoned_invalid_writer(path: impl AsRef<Path>) -> Result<(), SnapshotError> {
        let path = normalize_journal_path(path.as_ref())?;
        ensure_not_segmented_member(&path)?;
        Journal::recover_abandoned_invalid_writer(path).map_err(SnapshotError::WriterLease)
    }
}

fn ensure_not_segmented_member(path: &Path) -> Result<(), SnapshotError> {
    let marker_path = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(SEGMENT_DIRECTORY_MARKER);
    if marker_path.exists() {
        Err(SnapshotError::ManagedSegmentDirectory { marker_path })
    } else {
        Ok(())
    }
}

fn encode_header(
    kind: SnapshotKind,
    generation: u64,
    payload: &[u8],
) -> Result<([u8; HEADER_LENGTH], SnapshotReceipt), SnapshotError> {
    let payload_length = u64::try_from(payload.len()).map_err(|_| SnapshotError::LengthOverflow)?;
    let mut header = [0_u8; HEADER_LENGTH];
    header[0..4].copy_from_slice(&MAGIC);
    header[4..6].copy_from_slice(&VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&kind.wire().to_le_bytes());
    header[8..16].copy_from_slice(&payload_length.to_le_bytes());
    header[20..28].copy_from_slice(&generation.to_le_bytes());
    let checksum = crc32c_parts(&[&header, payload]);
    header[CHECKSUM_START..CHECKSUM_END].copy_from_slice(&checksum.to_le_bytes());
    Ok((
        header,
        SnapshotReceipt {
            generation,
            payload_length,
            checksum,
        },
    ))
}

fn read_loaded<T: SnapshotPayload>(
    path: &Path,
    options: SnapshotOptions,
) -> Result<LoadedSnapshot<T>, SnapshotError> {
    let mut file = File::open(path)?;
    let physical_length = file.metadata()?.len();
    let header_length = u64::try_from(HEADER_LENGTH).map_err(|_| SnapshotError::LengthOverflow)?;
    if physical_length < header_length {
        return Err(SnapshotError::TruncatedHeader {
            actual: physical_length,
        });
    }
    let mut header = [0_u8; HEADER_LENGTH];
    file.read_exact(&mut header)?;
    let found_magic: [u8; 4] = header[0..4]
        .try_into()
        .expect("snapshot magic has fixed width");
    if found_magic != MAGIC {
        return Err(SnapshotError::InvalidMagic(found_magic));
    }
    let version = u16::from_le_bytes(
        header[4..6]
            .try_into()
            .expect("snapshot version has fixed width"),
    );
    if version != VERSION {
        return Err(SnapshotError::UnsupportedVersion(version));
    }
    let actual_kind = u16::from_le_bytes(
        header[6..8]
            .try_into()
            .expect("snapshot kind has fixed width"),
    );
    if actual_kind != T::KIND.wire() {
        return Err(SnapshotError::UnexpectedKind {
            expected: T::KIND,
            actual: actual_kind,
        });
    }
    let payload_length = u64::from_le_bytes(
        header[8..16]
            .try_into()
            .expect("snapshot payload length has fixed width"),
    );
    validate_payload_length(payload_length, options)?;
    let actual_payload = physical_length
        .checked_sub(header_length)
        .ok_or(SnapshotError::LengthOverflow)?;
    if payload_length != actual_payload {
        return Err(SnapshotError::LengthMismatch {
            declared: payload_length,
            actual: actual_payload,
        });
    }
    let payload_size =
        usize::try_from(payload_length).map_err(|_| SnapshotError::LengthOverflow)?;
    let mut payload = vec![0_u8; payload_size];
    file.read_exact(&mut payload)?;
    let expected_checksum = u32::from_le_bytes(
        header[CHECKSUM_START..CHECKSUM_END]
            .try_into()
            .expect("snapshot checksum has fixed width"),
    );
    header[CHECKSUM_START..CHECKSUM_END].fill(0);
    let actual_checksum = crc32c_parts(&[&header, &payload]);
    if expected_checksum != actual_checksum {
        return Err(SnapshotError::ChecksumMismatch {
            expected: expected_checksum,
            actual: actual_checksum,
        });
    }
    let header_generation = u64::from_le_bytes(
        header[20..28]
            .try_into()
            .expect("snapshot generation has fixed width"),
    );
    let value = T::decode(&payload)?;
    let payload_generation = value.generation();
    if header_generation != payload_generation {
        return Err(SnapshotError::GenerationMismatch {
            header: header_generation,
            payload: payload_generation,
        });
    }
    Ok(LoadedSnapshot {
        value,
        payload,
        receipt: SnapshotReceipt {
            generation: header_generation,
            payload_length,
            checksum: expected_checksum,
        },
    })
}

fn validate_payload_length(
    payload_length: u64,
    options: SnapshotOptions,
) -> Result<(), SnapshotError> {
    if payload_length > options.maximum_payload_bytes {
        Err(SnapshotError::PayloadTooLarge {
            actual: payload_length,
            maximum: options.maximum_payload_bytes,
        })
    } else {
        Ok(())
    }
}

fn is_content_failure(error: &SnapshotError) -> bool {
    matches!(
        error,
        SnapshotError::TruncatedHeader { .. }
            | SnapshotError::InvalidMagic(_)
            | SnapshotError::UnsupportedVersion(_)
            | SnapshotError::UnexpectedKind { .. }
            | SnapshotError::LengthMismatch { .. }
            | SnapshotError::ChecksumMismatch { .. }
            | SnapshotError::GenerationMismatch { .. }
            | SnapshotError::Codec(_)
    )
}

fn is_discardable_pending_failure(error: &SnapshotError) -> bool {
    matches!(
        error,
        SnapshotError::TruncatedHeader { .. }
            | SnapshotError::InvalidMagic(_)
            | SnapshotError::LengthMismatch { .. }
            | SnapshotError::ChecksumMismatch { .. }
            | SnapshotError::GenerationMismatch { .. }
            | SnapshotError::Codec(_)
    )
}

fn remove_and_sync(path: &Path) -> Result<(), SnapshotError> {
    fs::remove_file(path)?;
    sync_parent(path)?;
    Ok(())
}

fn promote_and_sync(pending_path: &Path, current_path: &Path) -> Result<(), SnapshotError> {
    fs::rename(pending_path, current_path)?;
    sync_parent(current_path)?;
    Ok(())
}
