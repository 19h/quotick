//! Size-bounded physical WAL segments with one global logical sequence.

use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::codec::BinaryCodec;
use crate::journal::{
    AppendReceipt, DurableRecord, HEADER_LENGTH, Journal, JournalBatch, JournalError, JournalFrame,
    JournalOptions, JournalReader, JournalResource, RecordKind, SEGMENT_DIRECTORY_MARKER,
    WriterLease, WriterLeaseOwner, crc32c_parts, normalize_journal_path,
    reserve_journal_additional, reserve_journal_vec, sync_parent,
};
use crate::snapshot::CheckpointAnchor;

const SEGMENT_MARKER_MAGIC: [u8; 4] = *b"QSEG";
const SEGMENT_MARKER_VERSION: u16 = 2;
const SEGMENT_MARKER_LENGTH: usize = 46;
const MARKER_CHECKSUM_START: usize = 42;
const MARKER_CHECKSUM_END: usize = 46;
const SEGMENT_PREFIX: &str = "segment-";
const SEGMENT_SUFFIX: &str = ".qwal";
const SEGMENT_DIGITS: usize = 20;
const INITIAL_GENERATION: u64 = 1;
const MANAGER_LEASE_BASE: &str = ".quotick-segments";
const MANAGER_LEASE_FILE: &str = ".quotick-segments.writer.lock";
const MARKER_PENDING_FILE: &str = "format.qseg.pending";
const CUTOVER_PENDING_FILE: &str = ".quotick-segments.cutover.pending";
const DEFAULT_MAXIMUM_SEGMENT_BYTES: u64 = 1_073_741_824;

/// Configuration for an automatically rotating journal directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentedJournalOptions {
    /// Maximum complete frame bytes in one physical segment.
    pub maximum_segment_bytes: u64,
    /// Per-frame payload, durability, recovery, and initial-sequence options.
    pub journal: JournalOptions,
}

impl Default for SegmentedJournalOptions {
    fn default() -> Self {
        Self {
            maximum_segment_bytes: DEFAULT_MAXIMUM_SEGMENT_BYTES,
            journal: JournalOptions::default(),
        }
    }
}

/// One physical segment in a logical journal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SegmentDescriptor {
    generation: u64,
    start_sequence: u64,
    path: PathBuf,
    valid_bytes: u64,
}

impl SegmentDescriptor {
    /// Returns the marker-selected physical inventory generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the global sequence assigned to this segment's first frame.
    #[must_use]
    pub const fn start_sequence(&self) -> u64 {
        self.start_sequence
    }

    /// Returns the canonical physical segment path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns bytes occupied by complete verified frames.
    #[must_use]
    pub const fn valid_bytes(&self) -> u64 {
        self.valid_bytes
    }
}

/// Recovery summary across every physical segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentedRecoveryReport {
    /// Marker-selected physical inventory generation.
    pub active_generation: u64,
    /// Number of physical segment files, including a possible empty active segment.
    pub segment_count: u64,
    /// First retained global sequence in the marker-selected generation.
    pub first_sequence: u64,
    /// Last verified frame sequence, or `None` for a new empty journal.
    pub last_sequence: Option<u64>,
    /// Sum of verified frame bytes across all segments.
    pub valid_bytes: u128,
    /// Bytes removed from an incomplete active-segment tail.
    pub truncated_bytes: u64,
    /// Start sequence encoded by the active segment name.
    pub active_segment_start: u64,
}

/// Append metadata correlated to a physical segment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentedAppendReceipt {
    segment_start_sequence: u64,
    inner: AppendReceipt,
}

impl SegmentedAppendReceipt {
    /// Returns the assigned global frame sequence.
    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.inner.sequence
    }

    /// Returns the first sequence of the containing physical segment.
    #[must_use]
    pub const fn segment_start_sequence(self) -> u64 {
        self.segment_start_sequence
    }

    /// Returns the byte offset within the containing segment.
    #[must_use]
    pub const fn segment_offset(self) -> u64 {
        self.inner.offset
    }

    /// Returns the complete frame length.
    #[must_use]
    pub const fn frame_length(self) -> usize {
        self.inner.frame_length
    }
}

/// Segmented directory, continuity, capacity, or underlying journal failure.
#[derive(Debug)]
pub enum SegmentedJournalError {
    /// Directory or manager-lease storage failure.
    Storage(JournalError),
    /// Failure tied to one named physical segment.
    Segment {
        /// Segment path.
        path: PathBuf,
        /// Underlying framing or I/O error.
        error: JournalError,
    },
    /// Segment capacity cannot contain even one frame header.
    InvalidSegmentCapacity {
        /// Requested maximum bytes.
        actual: u64,
        /// Minimum permitted bytes.
        minimum: u64,
    },
    /// One frame or atomic append batch exceeds segment capacity.
    SegmentCapacityExceeded {
        /// Complete requested bytes.
        required: u64,
        /// Configured segment maximum.
        maximum: u64,
    },
    /// An existing marker does not equal requested immutable format settings.
    ConfigurationMismatch {
        /// Persisted segment capacity.
        persisted_maximum_segment_bytes: u64,
        /// Requested segment capacity.
        requested_maximum_segment_bytes: u64,
        /// Persisted first sequence.
        persisted_initial_sequence: u64,
        /// Requested first sequence.
        requested_initial_sequence: u64,
        /// Persisted payload maximum.
        persisted_maximum_payload_length: u32,
        /// Requested payload maximum.
        requested_maximum_payload_length: u32,
    },
    /// A nonempty directory did not contain its versioned format marker.
    MarkerMissing,
    /// The format marker was truncated, malformed, or unsupported.
    InvalidMarker,
    /// The format marker CRC-32C does not cover its stored fields.
    MarkerChecksumMismatch {
        /// Checksum stored in the marker.
        expected: u32,
        /// Checksum calculated after zeroing the checksum field.
        actual: u32,
    },
    /// Incomplete-initialization recovery found a valid marker and refused removal.
    MarkerRecoveryNotRequired,
    /// Incomplete-initialization recovery found an entry proving initialization advanced.
    MarkerRecoveryUnsafe(PathBuf),
    /// The dedicated directory contained an unrecognized entry.
    UnexpectedDirectoryEntry(PathBuf),
    /// A segment file name did not use the canonical fixed-width grammar.
    InvalidSegmentName(PathBuf),
    /// The first segment did not begin at the configured initial sequence.
    FirstSegmentMismatch {
        /// Configured first sequence.
        expected: u64,
        /// Segment name sequence.
        actual: u64,
    },
    /// Adjacent physical segments did not form one contiguous global sequence.
    SegmentDiscontinuity {
        /// Required next segment start.
        expected: u64,
        /// Observed segment start.
        actual: u64,
    },
    /// A closed segment contained no frames.
    EmptyNonFinalSegment(PathBuf),
    /// A recovered segment exceeds the immutable capacity.
    ExistingSegmentTooLarge {
        /// Segment path.
        path: PathBuf,
        /// Physical bytes.
        actual: u64,
        /// Configured maximum.
        maximum: u64,
    },
    /// No segment exists in an otherwise valid reader directory.
    NoSegments,
    /// A rotation failure left this manager unusable until reopen.
    Poisoned,
    /// No later physical inventory generation can be represented.
    GenerationExhausted,
    /// A deterministic segmented-cutover staging path is already occupied.
    CutoverPendingExists(PathBuf),
}

impl fmt::Display for SegmentedJournalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => error.fmt(formatter),
            Self::Segment { path, error } => {
                write!(formatter, "segment {}: {error}", path.display())
            }
            Self::InvalidSegmentCapacity { actual, minimum } => write!(
                formatter,
                "segment capacity {actual} bytes is below minimum {minimum}"
            ),
            Self::SegmentCapacityExceeded { required, maximum } => write!(
                formatter,
                "append requires {required} segment bytes but maximum is {maximum}"
            ),
            Self::ConfigurationMismatch { .. } => {
                formatter.write_str("segmented journal configuration differs from its marker")
            }
            Self::MarkerMissing => {
                formatter.write_str("nonempty segmented directory has no format marker")
            }
            Self::InvalidMarker => formatter.write_str("invalid segmented journal marker"),
            Self::MarkerChecksumMismatch { expected, actual } => write!(
                formatter,
                "segmented journal marker checksum mismatch: expected {expected:#010X}, calculated {actual:#010X}"
            ),
            Self::MarkerRecoveryNotRequired => {
                formatter.write_str("segmented journal marker is valid and cannot be recovered")
            }
            Self::MarkerRecoveryUnsafe(path) => write!(
                formatter,
                "segmented marker recovery is unsafe while {} exists",
                path.display()
            ),
            Self::UnexpectedDirectoryEntry(path) => {
                write!(
                    formatter,
                    "unexpected segmented-directory entry {}",
                    path.display()
                )
            }
            Self::InvalidSegmentName(path) => {
                write!(formatter, "invalid segment file name {}", path.display())
            }
            Self::FirstSegmentMismatch { expected, actual } => write!(
                formatter,
                "first segment starts at {actual}, expected {expected}"
            ),
            Self::SegmentDiscontinuity { expected, actual } => write!(
                formatter,
                "segment sequence discontinuity: expected {expected}, found {actual}"
            ),
            Self::EmptyNonFinalSegment(path) => {
                write!(formatter, "closed segment {} is empty", path.display())
            }
            Self::ExistingSegmentTooLarge {
                path,
                actual,
                maximum,
            } => write!(
                formatter,
                "segment {} has {actual} bytes, exceeding maximum {maximum}",
                path.display()
            ),
            Self::NoSegments => formatter.write_str("segmented journal contains no segments"),
            Self::Poisoned => formatter.write_str("segmented journal is poisoned"),
            Self::GenerationExhausted => {
                formatter.write_str("segmented journal generation exhausted")
            }
            Self::CutoverPendingExists(path) => write!(
                formatter,
                "segmented checkpoint cutover staging path {} already exists",
                path.display()
            ),
        }
    }
}

impl std::error::Error for SegmentedJournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) | Self::Segment { error, .. } => Some(error),
            _ => None,
        }
    }
}

impl From<JournalError> for SegmentedJournalError {
    fn from(error: JournalError) -> Self {
        Self::Storage(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Marker {
    maximum_segment_bytes: u64,
    initial_sequence: u64,
    maximum_payload_length: u32,
    active_generation: u64,
    active_first_sequence: u64,
}

#[derive(Debug)]
struct CutoverPlan {
    next_generation: u64,
    staging_path: PathBuf,
    next_path: PathBuf,
    next_marker: Marker,
}

#[derive(Debug)]
struct StagedCutoverGeneration {
    segments: Vec<SegmentDescriptor>,
    last_sequence: u64,
    valid_bytes: u128,
}

struct CutoverSegmentState {
    segments: Vec<SegmentDescriptor>,
    current: Option<Journal>,
    current_start: u64,
    current_path: PathBuf,
    current_is_staging: bool,
    last_sequence: u64,
    valid_bytes: u128,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SegmentedJournalCutoverBoundary {
    active_generation: u64,
    active_first_sequence: u64,
    segment_start_sequence: u64,
    suffix_offset: u64,
    wal_sequence: u64,
}

impl SegmentedJournalCutoverBoundary {
    pub(crate) const fn wal_sequence(self) -> u64 {
        self.wal_sequence
    }
}

/// One exclusive writer over an automatically rotating segment directory.
#[derive(Debug)]
pub struct SegmentedJournal {
    directory: PathBuf,
    options: SegmentedJournalOptions,
    marker: Marker,
    manager_lease: WriterLease,
    current: Option<Journal>,
    segments: Vec<SegmentDescriptor>,
    recovery: SegmentedRecoveryReport,
    poisoned: bool,
    #[cfg(test)]
    injected_fault: Option<InjectedFault>,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InjectedFault {
    CutoverMarkerDirectoryBarrier,
}

impl SegmentedJournal {
    /// Opens or creates a dedicated segmented directory and verifies global continuity.
    ///
    /// Closed segments are always strict. The configured recovery mode applies
    /// only to the final active segment.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError`] for ownership, marker, directory,
    /// capacity, framing, corruption, or sequence-continuity failure.
    pub fn open(
        directory: impl AsRef<Path>,
        options: SegmentedJournalOptions,
    ) -> Result<Self, SegmentedJournalError> {
        validate_options(options)?;
        let directory = prepare_directory(directory.as_ref())?;
        let manager_lease = WriterLease::acquire(&manager_lease_base(&directory))?;
        let marker = ensure_marker(&directory, options)?;
        let mut inventory = discover_segments(&directory, marker.active_generation)?;
        let mut segments = std::mem::take(&mut inventory.active);
        if segments.is_empty() {
            if marker.active_generation != INITIAL_GENERATION
                || marker.active_first_sequence != options.journal.initial_sequence
            {
                return Err(SegmentedJournalError::NoSegments);
            }
            let start = marker.active_first_sequence;
            let path = segment_path(&directory, marker.active_generation, start);
            let current = Journal::open_managed(&path, segment_options(options, start))
                .map_err(|error| segment_error(path.clone(), error))?;
            segments.push(SegmentDescriptor {
                generation: marker.active_generation,
                start_sequence: start,
                path,
                valid_bytes: 0,
            });
            cleanup_inactive_inventory(&directory, &inventory.inactive)?;
            return Ok(Self {
                directory,
                options,
                marker,
                manager_lease,
                current: Some(current),
                segments,
                recovery: SegmentedRecoveryReport {
                    active_generation: marker.active_generation,
                    segment_count: 1,
                    first_sequence: start,
                    last_sequence: None,
                    valid_bytes: 0,
                    truncated_bytes: 0,
                    active_segment_start: start,
                },
                poisoned: false,
                #[cfg(test)]
                injected_fault: None,
            });
        }

        verify_first_segment(&segments, marker.active_first_sequence)?;
        let (closed_last, closed_bytes) = verify_closed_segments(&mut segments, options)?;
        let active_index = segments.len() - 1;
        let active_start_sequence = segments[active_index].start_sequence;
        let active_path = segments[active_index].path.clone();
        let expected_active = closed_last
            .map(next_sequence)
            .transpose()?
            .unwrap_or(marker.active_first_sequence);
        if active_start_sequence != expected_active {
            return Err(SegmentedJournalError::SegmentDiscontinuity {
                expected: expected_active,
                actual: active_start_sequence,
            });
        }
        reject_oversized_existing(&segments[active_index], options.maximum_segment_bytes)?;
        let current = Journal::open_managed(
            &active_path,
            segment_options(options, active_start_sequence),
        )
        .map_err(|error| segment_error(active_path.clone(), error))?;
        let active_recovery = current.recovery();
        let active_bytes = current.current_offset();
        segments[active_index].valid_bytes = active_bytes;
        let last_sequence = active_recovery.last_sequence.or(closed_last);
        let valid_bytes = closed_bytes
            .checked_add(u128::from(active_bytes))
            .ok_or(JournalError::FrameLengthOverflow)?;
        let segment_count =
            u64::try_from(segments.len()).map_err(|_| JournalError::FrameLengthOverflow)?;
        cleanup_inactive_inventory(&directory, &inventory.inactive)?;
        Ok(Self {
            directory,
            options,
            marker,
            manager_lease,
            current: Some(current),
            recovery: SegmentedRecoveryReport {
                active_generation: marker.active_generation,
                segment_count,
                first_sequence: marker.active_first_sequence,
                last_sequence,
                valid_bytes,
                truncated_bytes: active_recovery.truncated_bytes,
                active_segment_start: active_start_sequence,
            },
            segments,
            poisoned: false,
            #[cfg(test)]
            injected_fault: None,
        })
    }

    /// Returns recovery state captured at open.
    #[must_use]
    pub const fn recovery(&self) -> SegmentedRecoveryReport {
        self.recovery
    }

    /// Returns the canonical directory owned by this manager.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Returns physical segments in global sequence order.
    #[must_use]
    pub fn segments(&self) -> &[SegmentDescriptor] {
        &self.segments
    }

    /// Returns the global sequence assigned to the next append.
    #[must_use]
    pub fn next_sequence(&self) -> u64 {
        self.current
            .as_ref()
            .map_or(self.marker.active_first_sequence, Journal::next_sequence)
    }

    /// Returns the directory manager's diagnostic lease identity.
    #[must_use]
    pub const fn writer_lease_owner(&self) -> WriterLeaseOwner {
        self.manager_lease.owner()
    }

    /// Returns whether rotation or journal I/O left the writer unusable.
    #[must_use]
    pub const fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Encodes and appends one typed record, rotating before it when necessary.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError`] for encoding, capacity, rotation,
    /// journal, or poisoned-state failure.
    pub fn append<T: DurableRecord>(
        &mut self,
        record: &T,
    ) -> Result<SegmentedAppendReceipt, SegmentedJournalError> {
        let payload = record
            .encode()
            .map_err(JournalError::Codec)
            .map_err(SegmentedJournalError::Storage)?;
        self.append_raw(T::KIND, &payload)
    }

    /// Appends encoded payload bytes under an explicit record kind.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError`] for payload, capacity, rotation,
    /// journal, or poisoned-state failure.
    pub fn append_raw(
        &mut self,
        kind: RecordKind,
        payload: &[u8],
    ) -> Result<SegmentedAppendReceipt, SegmentedJournalError> {
        self.ensure_healthy()?;
        let payload_length =
            u32::try_from(payload.len()).map_err(|_| JournalError::PayloadTooLarge {
                length: payload.len(),
                maximum: self.options.journal.max_payload_length,
            })?;
        if payload_length > self.options.journal.max_payload_length {
            return Err(JournalError::PayloadTooLarge {
                length: payload.len(),
                maximum: self.options.journal.max_payload_length,
            }
            .into());
        }
        self.next_sequence()
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        let frame_length = u64::try_from(HEADER_LENGTH)
            .map_err(|_| JournalError::FrameLengthOverflow)?
            .checked_add(u64::from(payload_length))
            .ok_or(JournalError::FrameLengthOverflow)?;
        self.rotate_if_needed(frame_length)?;
        let segment_start = self.active_segment_start();
        let active_path = self.active_segment_path().to_path_buf();
        let result = self
            .current
            .as_mut()
            .ok_or(SegmentedJournalError::Poisoned)?
            .append_raw(kind, payload);
        let receipt = match result {
            Ok(receipt) => receipt,
            Err(error) => {
                self.poisoned = self.current.as_ref().is_none_or(Journal::is_poisoned);
                return Err(segment_error(active_path, error));
            }
        };
        self.update_active_bytes(
            u64::try_from(receipt.frame_length).map_err(|_| JournalError::FrameLengthOverflow)?,
        )?;
        Ok(SegmentedAppendReceipt {
            segment_start_sequence: segment_start,
            inner: receipt,
        })
    }

    /// Appends one acknowledgement group without splitting it across segments.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError`] for capacity, rotation, journal, or
    /// poisoned-state failure.
    pub fn append_batch(
        &mut self,
        batch: &JournalBatch,
    ) -> Result<Vec<SegmentedAppendReceipt>, SegmentedJournalError> {
        self.ensure_healthy()?;
        if batch.is_empty() {
            return Ok(Vec::new());
        }
        let length = batch
            .encoded_frame_length(self.options.journal.max_payload_length)
            .map_err(SegmentedJournalError::Storage)?;
        let record_count =
            u64::try_from(batch.len()).map_err(|_| JournalError::SequenceExhausted)?;
        self.next_sequence()
            .checked_add(record_count)
            .ok_or(JournalError::SequenceExhausted)?;
        let mut segmented_receipts =
            reserve_journal_vec(JournalResource::SegmentedBatchReceipts, batch.len())
                .map_err(SegmentedJournalError::Storage)?;
        self.rotate_if_needed(length)?;
        let segment_start = self.active_segment_start();
        let active_path = self.active_segment_path().to_path_buf();
        let result = self
            .current
            .as_mut()
            .ok_or(SegmentedJournalError::Poisoned)?
            .append_batch(batch);
        let receipts = match result {
            Ok(receipts) => receipts,
            Err(error) => {
                self.poisoned = self.current.as_ref().is_none_or(Journal::is_poisoned);
                return Err(segment_error(active_path, error));
            }
        };
        self.update_active_bytes(length)?;
        for inner in receipts {
            segmented_receipts.push(SegmentedAppendReceipt {
                segment_start_sequence: segment_start,
                inner,
            });
        }
        Ok(segmented_receipts)
    }

    /// Replaces the selected prefix with a checkpoint anchor, preserves any
    /// later suffix frames, and advances the marker-selected generation.
    ///
    /// The current generation remains authoritative until every replacement
    /// segment is synchronized and the replacement marker is atomically
    /// published. Files from non-selected generations are removed only after
    /// that marker barrier and are ignored by readers during interrupted cleanup.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError`] for poison, boundary mismatch,
    /// generation/capacity exhaustion, occupied staging, or storage failure.
    pub fn replace_with_checkpoint_anchor(
        &mut self,
        anchor: CheckpointAnchor,
    ) -> Result<(), SegmentedJournalError> {
        let boundary = self.cutover_boundary()?;
        self.replace_with_checkpoint_anchor_at(anchor, boundary)
    }

    pub(crate) fn replace_with_checkpoint_anchor_at(
        &mut self,
        anchor: CheckpointAnchor,
        boundary: SegmentedJournalCutoverBoundary,
    ) -> Result<(), SegmentedJournalError> {
        self.ensure_healthy()?;
        let plan = self.prepare_cutover(anchor, boundary)?;
        let staged = self.stage_cutover_generation(anchor, boundary, &plan)?;
        self.publish_cutover_marker(plan.next_marker)?;
        self.install_cutover_generation(&plan, staged)
    }

    fn prepare_cutover(
        &self,
        anchor: CheckpointAnchor,
        boundary: SegmentedJournalCutoverBoundary,
    ) -> Result<CutoverPlan, SegmentedJournalError> {
        let current_boundary = self.next_sequence().checked_sub(1);
        let next_generation = self
            .marker
            .active_generation
            .checked_add(1)
            .ok_or(SegmentedJournalError::GenerationExhausted)?;
        let retained_segment = self.segments.iter().find(|segment| {
            segment.generation == boundary.active_generation
                && segment.start_sequence == boundary.segment_start_sequence
        });
        if anchor.wal_sequence() != boundary.wal_sequence
            || boundary.active_generation != self.marker.active_generation
            || boundary.active_first_sequence != self.marker.active_first_sequence
            || retained_segment.is_none_or(|segment| boundary.suffix_offset > segment.valid_bytes)
            || current_boundary.is_none_or(|value| anchor.wal_sequence() > value)
        {
            return Err(JournalError::CheckpointAnchorSequenceMismatch {
                current: current_boundary,
                proposed: anchor.wal_sequence(),
            }
            .into());
        }
        let payload = anchor.encode().map_err(JournalError::Codec)?;
        let payload_length =
            u32::try_from(payload.len()).map_err(|_| JournalError::PayloadTooLarge {
                length: payload.len(),
                maximum: self.options.journal.max_payload_length,
            })?;
        if payload_length > self.options.journal.max_payload_length {
            return Err(JournalError::PayloadTooLarge {
                length: payload.len(),
                maximum: self.options.journal.max_payload_length,
            }
            .into());
        }
        let frame_length = u64::try_from(HEADER_LENGTH)
            .map_err(|_| JournalError::FrameLengthOverflow)?
            .checked_add(u64::from(payload_length))
            .ok_or(JournalError::FrameLengthOverflow)?;
        if frame_length > self.options.maximum_segment_bytes {
            return Err(SegmentedJournalError::SegmentCapacityExceeded {
                required: frame_length,
                maximum: self.options.maximum_segment_bytes,
            });
        }
        anchor
            .wal_sequence()
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;

        let staging_path = self.directory.join(CUTOVER_PENDING_FILE);
        let marker_pending_path = self.directory.join(MARKER_PENDING_FILE);
        let next_path = segment_path(&self.directory, next_generation, anchor.wal_sequence());
        for path in [&staging_path, &marker_pending_path, &next_path] {
            if path.exists() {
                return Err(SegmentedJournalError::CutoverPendingExists(path.clone()));
            }
        }
        Ok(CutoverPlan {
            next_generation,
            staging_path,
            next_path,
            next_marker: Marker {
                active_generation: next_generation,
                active_first_sequence: anchor.wal_sequence(),
                ..self.marker
            },
        })
    }

    fn stage_cutover_generation(
        &mut self,
        anchor: CheckpointAnchor,
        boundary: SegmentedJournalCutoverBoundary,
        plan: &CutoverPlan,
    ) -> Result<StagedCutoverGeneration, SegmentedJournalError> {
        let (reader, mut state) = self.prepare_staged_generation(anchor, boundary, plan)?;
        for result in reader {
            let frame = match result {
                Ok(value) => value,
                Err(error) => {
                    self.poisoned = true;
                    return Err(error);
                }
            };
            self.append_staged_suffix_frame(&mut state, &frame, plan)?;
        }
        if self.next_sequence().checked_sub(1) != Some(state.last_sequence) {
            self.poisoned = true;
            return Err(JournalError::CheckpointAnchorSequenceMismatch {
                current: self.next_sequence().checked_sub(1),
                proposed: anchor.wal_sequence(),
            }
            .into());
        }
        self.finish_staged_generation(state, plan)
    }

    fn prepare_staged_generation(
        &mut self,
        anchor: CheckpointAnchor,
        boundary: SegmentedJournalCutoverBoundary,
        plan: &CutoverPlan,
    ) -> Result<(SegmentedJournalReader, CutoverSegmentState), SegmentedJournalError> {
        self.sync_all()?;
        let Some(suffix_start_index) = self
            .segments
            .iter()
            .position(|segment| segment.start_sequence == boundary.segment_start_sequence)
        else {
            self.poisoned = true;
            return Err(JournalError::CheckpointAnchorSequenceMismatch {
                current: self.next_sequence().checked_sub(1),
                proposed: boundary.wal_sequence,
            }
            .into());
        };
        let maximum_segments = self
            .segments
            .len()
            .checked_sub(suffix_start_index)
            .ok_or(JournalError::FrameLengthOverflow)?
            .checked_add(1)
            .ok_or(JournalError::FrameLengthOverflow)?;
        let segments = reserve_journal_vec(JournalResource::SegmentInventory, maximum_segments)
            .map_err(SegmentedJournalError::Storage)?;
        let source_segments = std::mem::take(&mut self.segments);
        let reader =
            match SegmentedJournalReader::open_suffix(source_segments, self.options, boundary) {
                Ok(value) => value,
                Err(error) => {
                    self.poisoned = true;
                    return Err(error);
                }
            };
        let staged_options = buffered_segment_options(self.options, anchor.wal_sequence());
        let mut current = match Journal::open_managed(&plan.staging_path, staged_options) {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = true;
                return Err(segment_error(plan.staging_path.clone(), error));
            }
        };
        if let Err(error) = current.append(&anchor) {
            self.poisoned = true;
            return Err(segment_error(plan.staging_path.clone(), error));
        }
        Ok((
            reader,
            CutoverSegmentState {
                segments,
                current: Some(current),
                current_start: anchor.wal_sequence(),
                current_path: plan.staging_path.clone(),
                current_is_staging: true,
                last_sequence: anchor.wal_sequence(),
                valid_bytes: 0,
            },
        ))
    }

    fn append_staged_suffix_frame(
        &mut self,
        state: &mut CutoverSegmentState,
        frame: &JournalFrame,
        plan: &CutoverPlan,
    ) -> Result<(), SegmentedJournalError> {
        let frame_length =
            u64::try_from(frame.frame_length()).map_err(|_| JournalError::FrameLengthOverflow)?;
        if frame_length > self.options.maximum_segment_bytes {
            self.poisoned = true;
            return Err(SegmentedJournalError::SegmentCapacityExceeded {
                required: frame_length,
                maximum: self.options.maximum_segment_bytes,
            });
        }
        let fits = state
            .current
            .as_ref()
            .ok_or(SegmentedJournalError::Poisoned)?
            .current_offset()
            .checked_add(frame_length)
            .is_some_and(|length| length <= self.options.maximum_segment_bytes);
        if !fits {
            self.rotate_staged_segment(state, frame.sequence(), plan)?;
        }
        if let Err(error) = state
            .current
            .as_mut()
            .ok_or(SegmentedJournalError::Poisoned)?
            .append_raw(frame.kind(), frame.payload())
        {
            self.poisoned = true;
            return Err(segment_error(state.current_path.clone(), error));
        }
        state.last_sequence = frame.sequence();
        Ok(())
    }

    fn rotate_staged_segment(
        &mut self,
        state: &mut CutoverSegmentState,
        next_start: u64,
        plan: &CutoverPlan,
    ) -> Result<(), SegmentedJournalError> {
        let current = state
            .current
            .take()
            .ok_or(SegmentedJournalError::Poisoned)?;
        let completed_bytes = current.current_offset();
        if let Err(error) = current.close() {
            self.poisoned = true;
            return Err(segment_error(state.current_path.clone(), error));
        }
        let completed_path = if state.current_is_staging {
            if let Err(error) = fs::rename(&plan.staging_path, &plan.next_path)
                .and_then(|()| sync_parent(&plan.next_path))
            {
                self.poisoned = true;
                return Err(JournalError::Io(error).into());
            }
            state.current_is_staging = false;
            plan.next_path.clone()
        } else {
            std::mem::take(&mut state.current_path)
        };
        state.valid_bytes = state
            .valid_bytes
            .checked_add(u128::from(completed_bytes))
            .ok_or(JournalError::FrameLengthOverflow)?;
        let completed = SegmentDescriptor {
            generation: plan.next_generation,
            start_sequence: state.current_start,
            path: completed_path,
            valid_bytes: completed_bytes,
        };
        if let Err(error) = push_reserved_segment(&mut state.segments, completed) {
            self.poisoned = true;
            return Err(error.into());
        }

        state.current_start = next_start;
        state.current_path = segment_path(&self.directory, plan.next_generation, next_start);
        if state.current_path.exists() {
            self.poisoned = true;
            return Err(SegmentedJournalError::CutoverPendingExists(
                state.current_path.clone(),
            ));
        }
        state.current = match Journal::open_managed(
            &state.current_path,
            buffered_segment_options(self.options, next_start),
        ) {
            Ok(value) => Some(value),
            Err(error) => {
                self.poisoned = true;
                return Err(segment_error(state.current_path.clone(), error));
            }
        };
        Ok(())
    }

    fn finish_staged_generation(
        &mut self,
        mut state: CutoverSegmentState,
        plan: &CutoverPlan,
    ) -> Result<StagedCutoverGeneration, SegmentedJournalError> {
        let current = state
            .current
            .take()
            .ok_or(SegmentedJournalError::Poisoned)?;
        let completed_bytes = current.current_offset();
        if let Err(error) = current.close() {
            self.poisoned = true;
            return Err(segment_error(state.current_path, error));
        }
        let completed_path = if state.current_is_staging {
            if let Err(error) = fs::rename(&plan.staging_path, &plan.next_path)
                .and_then(|()| sync_parent(&plan.next_path))
            {
                self.poisoned = true;
                return Err(JournalError::Io(error).into());
            }
            plan.next_path.clone()
        } else {
            state.current_path
        };
        state.valid_bytes = state
            .valid_bytes
            .checked_add(u128::from(completed_bytes))
            .ok_or(JournalError::FrameLengthOverflow)?;
        let completed = SegmentDescriptor {
            generation: plan.next_generation,
            start_sequence: state.current_start,
            path: completed_path,
            valid_bytes: completed_bytes,
        };
        if let Err(error) = push_reserved_segment(&mut state.segments, completed) {
            self.poisoned = true;
            return Err(error.into());
        }
        Ok(StagedCutoverGeneration {
            segments: state.segments,
            last_sequence: state.last_sequence,
            valid_bytes: state.valid_bytes,
        })
    }

    fn publish_cutover_marker(&mut self, next_marker: Marker) -> Result<(), SegmentedJournalError> {
        let marker_path = match replace_marker(&self.directory, next_marker) {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = true;
                return Err(error);
            }
        };
        #[cfg(test)]
        if self.injected_fault == Some(InjectedFault::CutoverMarkerDirectoryBarrier) {
            self.injected_fault = None;
            self.poisoned = true;
            return Err(JournalError::Io(io::Error::other(
                "injected segmented-cutover marker directory barrier failure",
            ))
            .into());
        }
        if let Err(error) = sync_parent(&marker_path) {
            self.poisoned = true;
            return Err(JournalError::Io(error).into());
        }
        Ok(())
    }

    fn install_cutover_generation(
        &mut self,
        plan: &CutoverPlan,
        staged: StagedCutoverGeneration,
    ) -> Result<(), SegmentedJournalError> {
        self.marker = plan.next_marker;
        drop(self.current.take());
        let active = staged
            .segments
            .last()
            .ok_or(SegmentedJournalError::NoSegments)?;
        let next = match Journal::open_managed(
            &active.path,
            segment_options(self.options, active.start_sequence),
        ) {
            Ok(value) => value,
            Err(error) => {
                self.poisoned = true;
                return Err(segment_error(active.path.clone(), error));
            }
        };
        self.current = Some(next);
        let segment_count =
            u64::try_from(staged.segments.len()).map_err(|_| JournalError::FrameLengthOverflow)?;
        let active_segment_start = active.start_sequence;
        self.segments = staged.segments;
        self.recovery = SegmentedRecoveryReport {
            active_generation: plan.next_generation,
            segment_count,
            first_sequence: plan.next_marker.active_first_sequence,
            last_sequence: Some(staged.last_sequence),
            valid_bytes: staged.valid_bytes,
            truncated_bytes: 0,
            active_segment_start,
        };
        let inventory = discover_segments(&self.directory, plan.next_generation)?;
        if let Err(error) = cleanup_inactive_inventory(&self.directory, &inventory.inactive) {
            self.poisoned = true;
            return Err(error);
        }
        Ok(())
    }

    /// Synchronizes active-segment data and metadata.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError`] for poison or synchronization failure.
    pub fn sync_all(&mut self) -> Result<(), SegmentedJournalError> {
        self.ensure_healthy()?;
        let path = self.active_segment_path().to_path_buf();
        self.current
            .as_mut()
            .ok_or(SegmentedJournalError::Poisoned)?
            .sync_all()
            .map_err(|error| {
                self.poisoned = true;
                segment_error(path, error)
            })
    }

    pub(crate) fn cutover_boundary(
        &self,
    ) -> Result<SegmentedJournalCutoverBoundary, SegmentedJournalError> {
        self.ensure_healthy()?;
        let wal_sequence = self.next_sequence().checked_sub(1).ok_or(
            JournalError::CheckpointAnchorSequenceMismatch {
                current: None,
                proposed: self.marker.active_first_sequence,
            },
        )?;
        let segment = self
            .segments
            .iter()
            .rev()
            .find(|segment| segment.valid_bytes != 0)
            .ok_or(JournalError::CheckpointAnchorSequenceMismatch {
                current: None,
                proposed: self.marker.active_first_sequence,
            })?;
        Ok(SegmentedJournalCutoverBoundary {
            active_generation: self.marker.active_generation,
            active_first_sequence: self.marker.active_first_sequence,
            segment_start_sequence: segment.start_sequence,
            suffix_offset: segment.valid_bytes,
            wal_sequence,
        })
    }

    /// Synchronizes the active segment and durably releases manager ownership.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError`] for poison, segment synchronization,
    /// or manager-lease release failure.
    pub fn close(mut self) -> Result<(), SegmentedJournalError> {
        self.ensure_healthy()?;
        if let Some(current) = self.current.take() {
            let path = self.active_segment_path().to_path_buf();
            current
                .close()
                .map_err(|error| segment_error(path, error))?;
        }
        self.manager_lease.release()?;
        Ok(())
    }

    /// Resolves the manager lease sidecar for operational diagnostics.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError::Storage`] if the directory cannot be canonicalized.
    pub fn writer_lease_path(
        directory: impl AsRef<Path>,
    ) -> Result<PathBuf, SegmentedJournalError> {
        let directory = fs::canonicalize(directory).map_err(JournalError::Io)?;
        Journal::writer_lease_path(manager_lease_base(&directory)).map_err(Into::into)
    }

    /// Removes an abandoned directory-manager lease under the ordinary writer
    /// recovery preconditions.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError::Storage`] for identity or filesystem failure.
    pub fn recover_abandoned_writer(
        directory: impl AsRef<Path>,
        owner: WriterLeaseOwner,
    ) -> Result<(), SegmentedJournalError> {
        let directory = fs::canonicalize(directory).map_err(JournalError::Io)?;
        Journal::recover_abandoned_writer(manager_lease_base(&directory), owner).map_err(Into::into)
    }

    /// Removes a malformed abandoned manager lease after verifying that it is
    /// not a decodable owner identity.
    ///
    /// The caller must independently establish manager quiescence and prevent
    /// new managers from starting until recovery returns.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError::Storage`] if the lease becomes valid or
    /// a filesystem operation fails.
    pub fn recover_abandoned_invalid_writer(
        directory: impl AsRef<Path>,
    ) -> Result<(), SegmentedJournalError> {
        let directory = fs::canonicalize(directory).map_err(JournalError::Io)?;
        Journal::recover_abandoned_invalid_writer(manager_lease_base(&directory))
            .map_err(Into::into)
    }

    /// Removes a malformed marker left before initial segment creation.
    ///
    /// The operation acquires the manager lease and refuses removal if the
    /// marker is valid or if any segment or unknown directory entry exists.
    /// The caller must externally prevent competing recovery operations until
    /// this operation returns.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError::MarkerRecoveryNotRequired`] for a valid
    /// marker, [`SegmentedJournalError::MarkerRecoveryUnsafe`] when any other
    /// persistent entry exists, or a storage error for lease/filesystem failure.
    pub fn recover_incomplete_initialization(
        directory: impl AsRef<Path>,
    ) -> Result<(), SegmentedJournalError> {
        let directory = fs::canonicalize(directory).map_err(JournalError::Io)?;
        let mut manager_lease = WriterLease::acquire(&manager_lease_base(&directory))?;
        let marker_path = directory.join(SEGMENT_DIRECTORY_MARKER);
        let bytes = fs::read(&marker_path).map_err(JournalError::Io)?;
        if decode_marker(&bytes).is_ok() {
            return Err(SegmentedJournalError::MarkerRecoveryNotRequired);
        }
        for result in fs::read_dir(&directory).map_err(JournalError::Io)? {
            let entry = result.map_err(JournalError::Io)?;
            let name = entry.file_name();
            if name != SEGMENT_DIRECTORY_MARKER && name != MANAGER_LEASE_FILE {
                return Err(SegmentedJournalError::MarkerRecoveryUnsafe(entry.path()));
            }
        }
        fs::remove_file(&marker_path).map_err(JournalError::Io)?;
        sync_parent(&marker_path).map_err(JournalError::Io)?;
        manager_lease.release()?;
        Ok(())
    }

    fn rotate_if_needed(&mut self, required: u64) -> Result<(), SegmentedJournalError> {
        if required > self.options.maximum_segment_bytes {
            return Err(SegmentedJournalError::SegmentCapacityExceeded {
                required,
                maximum: self.options.maximum_segment_bytes,
            });
        }
        let current_bytes = self
            .current
            .as_ref()
            .ok_or(SegmentedJournalError::Poisoned)?
            .current_offset();
        let fits = current_bytes
            .checked_add(required)
            .is_some_and(|total| total <= self.options.maximum_segment_bytes);
        if current_bytes == 0 || fits {
            return Ok(());
        }
        reserve_journal_additional(&mut self.segments, JournalResource::SegmentInventory, 1)
            .map_err(SegmentedJournalError::Storage)?;
        let start = self.next_sequence();
        let current_path = self.active_segment_path().to_path_buf();
        let path = segment_path(&self.directory, self.marker.active_generation, start);
        let current = self.current.take().ok_or(SegmentedJournalError::Poisoned)?;
        if let Err(error) = current.close() {
            self.poisoned = true;
            return Err(segment_error(current_path, error));
        }
        let next = match Journal::open_managed(&path, segment_options(self.options, start)) {
            Ok(journal) => journal,
            Err(error) => {
                self.poisoned = true;
                return Err(segment_error(path, error));
            }
        };
        self.segments.push(SegmentDescriptor {
            generation: self.marker.active_generation,
            start_sequence: start,
            path,
            valid_bytes: 0,
        });
        self.current = Some(next);
        Ok(())
    }

    fn active_segment_start(&self) -> u64 {
        self.segments
            .last()
            .map_or(self.marker.active_first_sequence, |segment| {
                segment.start_sequence
            })
    }

    fn active_segment_path(&self) -> &Path {
        self.segments
            .last()
            .map_or(self.directory.as_path(), |segment| segment.path())
    }

    fn update_active_bytes(&mut self, appended: u64) -> Result<(), SegmentedJournalError> {
        let active = self
            .segments
            .last_mut()
            .ok_or(SegmentedJournalError::Poisoned)?;
        active.valid_bytes = active
            .valid_bytes
            .checked_add(appended)
            .ok_or(JournalError::FrameLengthOverflow)?;
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<(), SegmentedJournalError> {
        if self.poisoned {
            Err(SegmentedJournalError::Poisoned)
        } else {
            Ok(())
        }
    }
}

/// Streaming verifier across a fixed segment inventory captured at open.
#[derive(Debug)]
pub struct SegmentedJournalReader {
    segments: Vec<SegmentDescriptor>,
    options: SegmentedJournalOptions,
    index: usize,
    current: Option<JournalReader>,
    current_had_frame: bool,
    current_may_be_empty: bool,
    first_suffix_offset: Option<u64>,
    expected_sequence: u64,
    finished: bool,
}

impl SegmentedJournalReader {
    /// Opens an immutable view of the current segment inventory.
    ///
    /// # Errors
    ///
    /// Returns [`SegmentedJournalError`] for marker, directory, configuration,
    /// capacity, or inventory failure.
    pub fn open(
        directory: impl AsRef<Path>,
        options: SegmentedJournalOptions,
    ) -> Result<Self, SegmentedJournalError> {
        validate_options(options)?;
        let directory = fs::canonicalize(directory).map_err(JournalError::Io)?;
        let marker = read_and_validate_marker(&directory, options)?;
        let segments = discover_segments(&directory, marker.active_generation)?.active;
        if segments.is_empty() {
            return Err(SegmentedJournalError::NoSegments);
        }
        verify_first_segment(&segments, marker.active_first_sequence)?;
        for segment in &segments {
            reject_oversized_existing(segment, options.maximum_segment_bytes)?;
        }
        Ok(Self {
            segments,
            options,
            index: 0,
            current: None,
            current_had_frame: false,
            current_may_be_empty: false,
            first_suffix_offset: None,
            expected_sequence: marker.active_first_sequence,
            finished: false,
        })
    }

    fn open_suffix(
        segments: Vec<SegmentDescriptor>,
        options: SegmentedJournalOptions,
        boundary: SegmentedJournalCutoverBoundary,
    ) -> Result<Self, SegmentedJournalError> {
        validate_options(options)?;
        if segments.is_empty() {
            return Err(SegmentedJournalError::NoSegments);
        }
        if segments[0].start_sequence != boundary.active_first_sequence
            || segments
                .iter()
                .any(|segment| segment.generation != boundary.active_generation)
        {
            return Err(JournalError::CheckpointAnchorSequenceMismatch {
                current: None,
                proposed: boundary.wal_sequence,
            }
            .into());
        }
        let start_index = segments
            .iter()
            .position(|segment| segment.start_sequence == boundary.segment_start_sequence)
            .ok_or(JournalError::CheckpointAnchorSequenceMismatch {
                current: None,
                proposed: boundary.wal_sequence,
            })?;
        if boundary.suffix_offset > segments[start_index].valid_bytes {
            return Err(JournalError::CheckpointAnchorSequenceMismatch {
                current: None,
                proposed: boundary.wal_sequence,
            }
            .into());
        }
        let expected_sequence = boundary
            .wal_sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        Ok(Self {
            segments,
            options,
            index: start_index,
            current: None,
            current_had_frame: false,
            current_may_be_empty: true,
            first_suffix_offset: Some(boundary.suffix_offset),
            expected_sequence,
            finished: false,
        })
    }
}

impl Iterator for SegmentedJournalReader {
    type Item = Result<JournalFrame, SegmentedJournalError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.finished {
                return None;
            }
            if self.current.is_none() {
                let Some(segment) = self.segments.get(self.index) else {
                    self.finished = true;
                    return None;
                };
                let suffix_offset = self.first_suffix_offset.take();
                if suffix_offset.is_none() && segment.start_sequence != self.expected_sequence {
                    self.finished = true;
                    return Some(Err(SegmentedJournalError::SegmentDiscontinuity {
                        expected: self.expected_sequence,
                        actual: segment.start_sequence,
                    }));
                }
                let reader = if let Some(offset) = suffix_offset {
                    JournalReader::open_suffix(
                        &segment.path,
                        offset,
                        self.expected_sequence,
                        self.options.journal.max_payload_length,
                    )
                } else {
                    JournalReader::open(
                        &segment.path,
                        segment_options(self.options, segment.start_sequence),
                    )
                };
                match reader {
                    Ok(reader) => self.current = Some(reader),
                    Err(error) => {
                        self.finished = true;
                        return Some(Err(segment_error(segment.path.clone(), error)));
                    }
                }
                self.current_had_frame = false;
            }
            let segment = &self.segments[self.index];
            match self
                .current
                .as_mut()
                .expect("reader was initialized")
                .next()
            {
                Some(Ok(frame)) => {
                    self.current_had_frame = true;
                    self.expected_sequence = if let Some(next) = frame.sequence().checked_add(1) {
                        next
                    } else {
                        self.finished = true;
                        return Some(Err(SegmentedJournalError::Storage(
                            JournalError::SequenceExhausted,
                        )));
                    };
                    return Some(Ok(frame));
                }
                Some(Err(error)) => {
                    self.finished = true;
                    return Some(Err(segment_error(segment.path.clone(), error)));
                }
                None => {
                    let is_final = self.index + 1 == self.segments.len();
                    if !self.current_had_frame && !is_final && !self.current_may_be_empty {
                        self.finished = true;
                        return Some(Err(SegmentedJournalError::EmptyNonFinalSegment(
                            segment.path.clone(),
                        )));
                    }
                    self.current_may_be_empty = false;
                    self.current = None;
                    self.index += 1;
                }
            }
        }
    }
}

fn validate_options(options: SegmentedJournalOptions) -> Result<(), SegmentedJournalError> {
    let minimum = u64::try_from(HEADER_LENGTH).map_err(|_| JournalError::FrameLengthOverflow)?;
    if options.maximum_segment_bytes < minimum {
        return Err(SegmentedJournalError::InvalidSegmentCapacity {
            actual: options.maximum_segment_bytes,
            minimum,
        });
    }
    if options.journal.initial_sequence == 0 {
        return Err(JournalError::InvalidInitialSequence.into());
    }
    Ok(())
}

fn prepare_directory(path: &Path) -> Result<PathBuf, SegmentedJournalError> {
    match fs::create_dir(path) {
        Ok(()) => sync_parent(path).map_err(JournalError::Io)?,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if !fs::metadata(path).map_err(JournalError::Io)?.is_dir() {
                return Err(JournalError::Io(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    "segmented journal path is not a directory",
                ))
                .into());
            }
        }
        Err(error) => return Err(JournalError::Io(error).into()),
    }
    fs::canonicalize(path)
        .map_err(JournalError::Io)
        .map_err(Into::into)
}

fn ensure_marker(
    directory: &Path,
    options: SegmentedJournalOptions,
) -> Result<Marker, SegmentedJournalError> {
    let path = directory.join(SEGMENT_DIRECTORY_MARKER);
    if path.exists() {
        return read_and_validate_marker(directory, options);
    }
    let mut entries = fs::read_dir(directory).map_err(JournalError::Io)?;
    if entries.any(|entry| {
        entry.is_ok_and(|entry| entry.file_name().to_string_lossy() != MANAGER_LEASE_FILE)
    }) {
        return Err(SegmentedJournalError::MarkerMissing);
    }
    let marker = Marker {
        maximum_segment_bytes: options.maximum_segment_bytes,
        initial_sequence: options.journal.initial_sequence,
        maximum_payload_length: options.journal.max_payload_length,
        active_generation: INITIAL_GENERATION,
        active_first_sequence: options.journal.initial_sequence,
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(JournalError::Io)?;
    if let Err(error) = file
        .write_all(&encode_marker(marker))
        .and_then(|()| file.sync_all())
        .and_then(|()| sync_parent(&path))
    {
        drop(file);
        let _ = fs::remove_file(&path);
        let _ = sync_parent(&path);
        return Err(JournalError::Io(error).into());
    }
    Ok(marker)
}

fn read_and_validate_marker(
    directory: &Path,
    options: SegmentedJournalOptions,
) -> Result<Marker, SegmentedJournalError> {
    let bytes = fs::read(directory.join(SEGMENT_DIRECTORY_MARKER)).map_err(JournalError::Io)?;
    let marker = decode_marker(&bytes)?;
    if marker.maximum_segment_bytes != options.maximum_segment_bytes
        || marker.initial_sequence != options.journal.initial_sequence
        || marker.maximum_payload_length != options.journal.max_payload_length
    {
        return Err(SegmentedJournalError::ConfigurationMismatch {
            persisted_maximum_segment_bytes: marker.maximum_segment_bytes,
            requested_maximum_segment_bytes: options.maximum_segment_bytes,
            persisted_initial_sequence: marker.initial_sequence,
            requested_initial_sequence: options.journal.initial_sequence,
            persisted_maximum_payload_length: marker.maximum_payload_length,
            requested_maximum_payload_length: options.journal.max_payload_length,
        });
    }
    Ok(marker)
}

fn replace_marker(directory: &Path, marker: Marker) -> Result<PathBuf, SegmentedJournalError> {
    let marker_path = directory.join(SEGMENT_DIRECTORY_MARKER);
    let pending_path = directory.join(MARKER_PENDING_FILE);
    let mut pending = match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&pending_path)
    {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(SegmentedJournalError::CutoverPendingExists(pending_path));
        }
        Err(error) => return Err(JournalError::Io(error).into()),
    };
    pending
        .write_all(&encode_marker(marker))
        .and_then(|()| pending.sync_all())
        .map_err(JournalError::Io)?;
    fs::rename(&pending_path, &marker_path).map_err(JournalError::Io)?;
    Ok(marker_path)
}

fn encode_marker(marker: Marker) -> [u8; SEGMENT_MARKER_LENGTH] {
    let mut bytes = [0_u8; SEGMENT_MARKER_LENGTH];
    bytes[0..4].copy_from_slice(&SEGMENT_MARKER_MAGIC);
    bytes[4..6].copy_from_slice(&SEGMENT_MARKER_VERSION.to_le_bytes());
    bytes[6..14].copy_from_slice(&marker.maximum_segment_bytes.to_le_bytes());
    bytes[14..22].copy_from_slice(&marker.initial_sequence.to_le_bytes());
    bytes[22..26].copy_from_slice(&marker.maximum_payload_length.to_le_bytes());
    bytes[26..34].copy_from_slice(&marker.active_generation.to_le_bytes());
    bytes[34..42].copy_from_slice(&marker.active_first_sequence.to_le_bytes());
    let checksum = crc32c_parts(&[&bytes]);
    bytes[MARKER_CHECKSUM_START..MARKER_CHECKSUM_END].copy_from_slice(&checksum.to_le_bytes());
    bytes
}

fn decode_marker(bytes: &[u8]) -> Result<Marker, SegmentedJournalError> {
    if bytes.len() != SEGMENT_MARKER_LENGTH
        || bytes[0..4] != SEGMENT_MARKER_MAGIC
        || u16::from_le_bytes(
            bytes[4..6]
                .try_into()
                .expect("marker version has fixed width"),
        ) != SEGMENT_MARKER_VERSION
    {
        return Err(SegmentedJournalError::InvalidMarker);
    }
    let expected = u32::from_le_bytes(
        bytes[MARKER_CHECKSUM_START..MARKER_CHECKSUM_END]
            .try_into()
            .expect("marker checksum has fixed width"),
    );
    let mut checked = [0_u8; SEGMENT_MARKER_LENGTH];
    checked.copy_from_slice(bytes);
    checked[MARKER_CHECKSUM_START..MARKER_CHECKSUM_END].fill(0);
    let actual = crc32c_parts(&[&checked]);
    if expected != actual {
        return Err(SegmentedJournalError::MarkerChecksumMismatch { expected, actual });
    }
    let marker = Marker {
        maximum_segment_bytes: u64::from_le_bytes(
            bytes[6..14]
                .try_into()
                .expect("marker segment capacity has fixed width"),
        ),
        initial_sequence: u64::from_le_bytes(
            bytes[14..22]
                .try_into()
                .expect("marker initial sequence has fixed width"),
        ),
        maximum_payload_length: u32::from_le_bytes(
            bytes[22..26]
                .try_into()
                .expect("marker payload capacity has fixed width"),
        ),
        active_generation: u64::from_le_bytes(
            bytes[26..34]
                .try_into()
                .expect("marker active generation has fixed width"),
        ),
        active_first_sequence: u64::from_le_bytes(
            bytes[34..42]
                .try_into()
                .expect("marker active first sequence has fixed width"),
        ),
    };
    if marker.active_generation == 0 || marker.active_first_sequence == 0 {
        return Err(SegmentedJournalError::InvalidMarker);
    }
    Ok(marker)
}

#[derive(Debug, Default)]
struct SegmentInventory {
    active: Vec<SegmentDescriptor>,
    inactive: Vec<PathBuf>,
}

fn discover_segments(
    directory: &Path,
    active_generation: u64,
) -> Result<SegmentInventory, SegmentedJournalError> {
    let mut inventory = SegmentInventory::default();
    for result in fs::read_dir(directory).map_err(JournalError::Io)? {
        let entry = result.map_err(JournalError::Io)?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name == SEGMENT_DIRECTORY_MARKER || name == MANAGER_LEASE_FILE {
            continue;
        }
        let path = entry.path();
        if name == MARKER_PENDING_FILE || name == CUTOVER_PENDING_FILE {
            if !entry.file_type().map_err(JournalError::Io)?.is_file() {
                return Err(SegmentedJournalError::UnexpectedDirectoryEntry(path));
            }
            inventory.inactive.push(path);
            continue;
        }
        if !entry.file_type().map_err(JournalError::Io)?.is_file() {
            return Err(SegmentedJournalError::UnexpectedDirectoryEntry(path));
        }
        let Some((generation, start_sequence)) = parse_segment_name(&name) else {
            return Err(SegmentedJournalError::InvalidSegmentName(path));
        };
        if generation == active_generation {
            inventory.active.push(SegmentDescriptor {
                generation,
                start_sequence,
                valid_bytes: fs::metadata(&path).map_err(JournalError::Io)?.len(),
                path: normalize_journal_path(&path).map_err(JournalError::Io)?,
            });
        } else {
            inventory.inactive.push(path);
        }
    }
    inventory
        .active
        .sort_unstable_by_key(|segment| segment.start_sequence);
    Ok(inventory)
}

fn cleanup_inactive_inventory(
    directory: &Path,
    inactive: &[PathBuf],
) -> Result<(), SegmentedJournalError> {
    if inactive.is_empty() {
        return Ok(());
    }
    for path in inactive {
        fs::remove_file(path).map_err(JournalError::Io)?;
    }
    sync_parent(&directory.join(SEGMENT_DIRECTORY_MARKER)).map_err(JournalError::Io)?;
    Ok(())
}

fn parse_segment_name(name: &str) -> Option<(u64, u64)> {
    let fixed_length = SEGMENT_PREFIX.len() + SEGMENT_DIGITS * 2 + 1 + SEGMENT_SUFFIX.len();
    let bytes = name.as_bytes();
    if bytes.len() != fixed_length
        || &bytes[..SEGMENT_PREFIX.len()] != SEGMENT_PREFIX.as_bytes()
        || &bytes[bytes.len() - SEGMENT_SUFFIX.len()..] != SEGMENT_SUFFIX.as_bytes()
    {
        return None;
    }
    let generation_start = SEGMENT_PREFIX.len();
    let generation_end = generation_start + SEGMENT_DIGITS;
    if bytes[generation_end] != b'-' {
        return None;
    }
    let sequence_start = generation_end + 1;
    let sequence_end = sequence_start + SEGMENT_DIGITS;
    let generation = parse_fixed_decimal(&bytes[generation_start..generation_end])?;
    let sequence = parse_fixed_decimal(&bytes[sequence_start..sequence_end])?;
    (generation != 0 && sequence != 0).then_some((generation, sequence))
}

fn parse_fixed_decimal(bytes: &[u8]) -> Option<u64> {
    bytes.iter().try_fold(0_u64, |value, byte| {
        byte.is_ascii_digit()
            .then_some(())
            .and_then(|()| value.checked_mul(10))
            .and_then(|value| value.checked_add(u64::from(*byte - b'0')))
    })
}

fn segment_path(directory: &Path, generation: u64, start_sequence: u64) -> PathBuf {
    directory.join(format!(
        "{SEGMENT_PREFIX}{generation:020}-{start_sequence:020}{SEGMENT_SUFFIX}"
    ))
}

fn manager_lease_base(directory: &Path) -> PathBuf {
    directory.join(MANAGER_LEASE_BASE)
}

fn segment_options(options: SegmentedJournalOptions, initial_sequence: u64) -> JournalOptions {
    JournalOptions {
        initial_sequence,
        ..options.journal
    }
}

fn buffered_segment_options(
    options: SegmentedJournalOptions,
    initial_sequence: u64,
) -> JournalOptions {
    JournalOptions {
        durability: crate::journal::Durability::Buffered,
        ..segment_options(options, initial_sequence)
    }
}

fn push_reserved_segment(
    segments: &mut Vec<SegmentDescriptor>,
    segment: SegmentDescriptor,
) -> Result<(), JournalError> {
    if segments.len() == segments.capacity() {
        return Err(JournalError::CapacityReservationFailed {
            resource: JournalResource::SegmentInventory,
            maximum: segments
                .len()
                .checked_add(1)
                .ok_or(JournalError::FrameLengthOverflow)?,
        });
    }
    segments.push(segment);
    Ok(())
}

fn verify_first_segment(
    segments: &[SegmentDescriptor],
    expected: u64,
) -> Result<(), SegmentedJournalError> {
    let actual = segments
        .first()
        .ok_or(SegmentedJournalError::NoSegments)?
        .start_sequence;
    if actual != expected {
        return Err(SegmentedJournalError::FirstSegmentMismatch { expected, actual });
    }
    Ok(())
}

fn verify_closed_segments(
    segments: &mut [SegmentDescriptor],
    options: SegmentedJournalOptions,
) -> Result<(Option<u64>, u128), SegmentedJournalError> {
    let closed_length = segments.len().saturating_sub(1);
    let mut last_sequence = None;
    let mut total_bytes = 0_u128;
    for index in 0..closed_length {
        let segment = &segments[index];
        reject_oversized_existing(segment, options.maximum_segment_bytes)?;
        let mut reader = JournalReader::open(
            &segment.path,
            segment_options(options, segment.start_sequence),
        )
        .map_err(|error| segment_error(segment.path.clone(), error))?;
        let mut segment_last = None;
        for result in &mut reader {
            let frame = result.map_err(|error| segment_error(segment.path.clone(), error))?;
            segment_last = Some(frame.sequence());
        }
        let segment_last = segment_last
            .ok_or_else(|| SegmentedJournalError::EmptyNonFinalSegment(segment.path.clone()))?;
        let expected_next = next_sequence(segment_last)?;
        let actual_next = segments[index + 1].start_sequence;
        if actual_next != expected_next {
            return Err(SegmentedJournalError::SegmentDiscontinuity {
                expected: expected_next,
                actual: actual_next,
            });
        }
        let bytes = fs::metadata(&segment.path).map_err(JournalError::Io)?.len();
        segments[index].valid_bytes = bytes;
        total_bytes = total_bytes
            .checked_add(u128::from(bytes))
            .ok_or(JournalError::FrameLengthOverflow)?;
        last_sequence = Some(segment_last);
    }
    Ok((last_sequence, total_bytes))
}

fn reject_oversized_existing(
    segment: &SegmentDescriptor,
    maximum: u64,
) -> Result<(), SegmentedJournalError> {
    let actual = fs::metadata(&segment.path).map_err(JournalError::Io)?.len();
    if actual > maximum {
        return Err(SegmentedJournalError::ExistingSegmentTooLarge {
            path: segment.path.clone(),
            actual,
            maximum,
        });
    }
    Ok(())
}

fn next_sequence(sequence: u64) -> Result<u64, SegmentedJournalError> {
    sequence
        .checked_add(1)
        .ok_or_else(|| JournalError::SequenceExhausted.into())
}

fn segment_error(path: PathBuf, error: JournalError) -> SegmentedJournalError {
    SegmentedJournalError::Segment { path, error }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{
        CUTOVER_PENDING_FILE, InjectedFault, SegmentedJournal, SegmentedJournalError,
        SegmentedJournalOptions, SegmentedJournalReader,
    };
    use crate::journal::{Durability, JournalError, JournalOptions, RecordKind};
    use crate::snapshot::{CheckpointAnchor, CheckpointSlot, SnapshotKind};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

    fn test_directory(label: &str) -> std::path::PathBuf {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "quotick-segmented-unit-{label}-{}-{nonce}",
            std::process::id()
        ))
    }

    fn options() -> SegmentedJournalOptions {
        SegmentedJournalOptions {
            maximum_segment_bytes: 1_024,
            journal: JournalOptions {
                durability: Durability::Buffered,
                ..JournalOptions::default()
            },
        }
    }

    fn anchor() -> CheckpointAnchor {
        CheckpointAnchor::from_parts(
            SnapshotKind::MatchingCheckpoint,
            CheckpointSlot::A,
            1,
            1,
            99,
            0x1234_5678,
        )
    }

    #[test]
    fn cutover_repacks_and_preserves_a_multisegment_suffix() {
        let directory = test_directory("cutover-suffix");
        let _ = fs::remove_dir_all(&directory);
        let options = SegmentedJournalOptions {
            maximum_segment_bytes: 128,
            ..options()
        };
        let mut journal = SegmentedJournal::open(&directory, options).unwrap();
        for value in 1_u8..=2 {
            journal
                .append_raw(RecordKind::Command, &[value; 80])
                .unwrap();
        }
        let boundary = journal.cutover_boundary().unwrap();
        for value in 3_u8..=5 {
            journal
                .append_raw(RecordKind::Command, &[value; 80])
                .unwrap();
        }
        assert_eq!(journal.segments().len(), 5);
        let anchor = CheckpointAnchor::from_parts(
            SnapshotKind::MatchingCheckpoint,
            CheckpointSlot::A,
            2,
            2,
            99,
            0x1234_5678,
        );

        journal
            .replace_with_checkpoint_anchor_at(anchor, boundary)
            .unwrap();

        assert_eq!(journal.recovery().active_generation, 2);
        assert_eq!(journal.recovery().first_sequence, 2);
        assert_eq!(journal.recovery().last_sequence, Some(5));
        assert_eq!(journal.segments().len(), 4);
        assert!(
            journal
                .segments()
                .iter()
                .all(|segment| segment.generation() == 2)
        );
        assert_eq!(journal.next_sequence(), 6);
        journal
            .append_raw(RecordKind::ExecutionReport, &[6; 80])
            .unwrap();
        journal.close().unwrap();

        let frames = SegmentedJournalReader::open(&directory, options)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(frames.len(), 5);
        assert_eq!(frames[0].sequence(), 2);
        assert_eq!(frames[0].decode::<CheckpointAnchor>().unwrap(), anchor);
        for (frame, value) in frames[1..].iter().zip(3_u8..=6) {
            assert_eq!(frame.sequence(), u64::from(value));
            assert_eq!(frame.payload(), &[value; 80]);
        }

        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn cutover_cursor_can_start_inside_a_growing_segment() {
        let directory = test_directory("cutover-mid-segment");
        let _ = fs::remove_dir_all(&directory);
        let options = SegmentedJournalOptions {
            maximum_segment_bytes: 512,
            ..options()
        };
        let mut journal = SegmentedJournal::open(&directory, options).unwrap();
        journal.append_raw(RecordKind::Command, &[1; 20]).unwrap();
        journal
            .append_raw(RecordKind::ExecutionReport, &[2; 20])
            .unwrap();
        let boundary = journal.cutover_boundary().unwrap();
        assert_eq!(journal.segments().len(), 1);
        journal.append_raw(RecordKind::Command, &[3; 20]).unwrap();
        journal
            .append_raw(RecordKind::ExecutionReport, &[4; 20])
            .unwrap();
        assert_eq!(journal.segments().len(), 1);
        let anchor = CheckpointAnchor::from_parts(
            SnapshotKind::MatchingCheckpoint,
            CheckpointSlot::A,
            2,
            2,
            99,
            0x1234_5678,
        );

        journal
            .replace_with_checkpoint_anchor_at(anchor, boundary)
            .unwrap();
        journal.close().unwrap();

        let frames = SegmentedJournalReader::open(&directory, options)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].decode::<CheckpointAnchor>().unwrap(), anchor);
        assert_eq!(frames[1].sequence(), 3);
        assert_eq!(frames[1].payload(), &[3; 20]);
        assert_eq!(frames[2].sequence(), 4);
        assert_eq!(frames[2].payload(), &[4; 20]);
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn failed_marker_directory_barrier_reopens_the_complete_selected_generation() {
        let directory = test_directory("marker-barrier");
        let _ = fs::remove_dir_all(&directory);
        let mut journal = SegmentedJournal::open(&directory, options()).unwrap();
        journal.append_raw(RecordKind::Command, &[1]).unwrap();
        let anchor = anchor();
        journal.injected_fault = Some(InjectedFault::CutoverMarkerDirectoryBarrier);
        assert!(matches!(
            journal.replace_with_checkpoint_anchor(anchor),
            Err(SegmentedJournalError::Storage(JournalError::Io(_)))
        ));
        assert!(journal.is_poisoned());
        drop(journal);

        let frames = SegmentedJournalReader::open(&directory, options())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].kind(), RecordKind::CheckpointAnchor);
        assert_eq!(frames[0].sequence(), 1);

        let recovered = SegmentedJournal::open(&directory, options()).unwrap();
        assert_eq!(recovered.recovery().active_generation, 2);
        assert_eq!(recovered.segments().len(), 1);
        assert_eq!(recovered.segments()[0].generation(), 2);
        recovered.close().unwrap();
        fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn occupied_cutover_staging_and_generation_exhaustion_are_nonmutating() {
        let directory = test_directory("cutover-preflight");
        let _ = fs::remove_dir_all(&directory);
        let mut journal = SegmentedJournal::open(&directory, options()).unwrap();
        journal.append_raw(RecordKind::Command, &[1]).unwrap();
        let staging = journal.directory().join(CUTOVER_PENDING_FILE);
        fs::write(&staging, b"occupied").unwrap();
        assert!(matches!(
            journal.replace_with_checkpoint_anchor(anchor()),
            Err(SegmentedJournalError::CutoverPendingExists(path)) if path == staging
        ));
        assert!(!journal.is_poisoned());
        assert_eq!(journal.next_sequence(), 2);
        fs::remove_file(staging).unwrap();

        journal.marker.active_generation = u64::MAX;
        assert!(matches!(
            journal.replace_with_checkpoint_anchor(anchor()),
            Err(SegmentedJournalError::GenerationExhausted)
        ));
        assert!(!journal.is_poisoned());
        assert_eq!(journal.next_sequence(), 2);
        drop(journal);
        fs::remove_dir_all(directory).unwrap();
    }
}
