use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::BinaryCodec;
use quotick::journal::{
    Durability, Journal, JournalBatch, JournalError, JournalOptions, RecoveryMode,
    SegmentedJournal, SegmentedJournalError, SegmentedJournalOptions, SegmentedJournalReader,
};
use quotick::matching::{Command, NewOrder, OrderType, SelfTradePrevention, TimeInForce};
use quotick::{
    AccountId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let nonce = NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-segmented-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn command(value: u64) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(value).expect("command"),
        order_id: OrderId::new(value).expect("order"),
        account_id: AccountId::new(1).expect("account"),
        instrument_id: InstrumentId::new(2).expect("instrument"),
        instrument_version: InstrumentVersion::new(1).expect("version"),
        side: Side::Buy,
        quantity: Quantity::new(3).expect("quantity"),
        display: quotick::matching::OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(4)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(value),
    })
}

fn frame_length() -> u64 {
    u64::try_from(24 + command(1).encode().expect("command encodes").len()).expect("frame length")
}

fn options(maximum_segment_bytes: u64) -> SegmentedJournalOptions {
    SegmentedJournalOptions {
        maximum_segment_bytes,
        journal: JournalOptions {
            durability: Durability::Buffered,
            ..JournalOptions::default()
        },
    }
}

fn read_commands(
    path: &Path,
    options: SegmentedJournalOptions,
) -> Result<Vec<Command>, SegmentedJournalError> {
    SegmentedJournalReader::open(path, options)?
        .map(|frame| {
            frame?
                .decode::<Command>()
                .map_err(SegmentedJournalError::Storage)
        })
        .collect()
}

#[test]
fn rotates_at_exact_capacity_and_reads_one_global_sequence() {
    let directory = TestDirectory::new("rotation");
    let options = options(frame_length() * 2);
    let mut journal = SegmentedJournal::open(directory.path(), options).expect("journal opens");
    let receipts = (1..=5)
        .map(|value| journal.append(&command(value)).expect("append succeeds"))
        .collect::<Vec<_>>();
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| (receipt.sequence(), receipt.segment_start_sequence()))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 1), (3, 3), (4, 3), (5, 5)]
    );
    assert_eq!(
        journal
            .segments()
            .iter()
            .map(quotick::journal::SegmentDescriptor::start_sequence)
            .collect::<Vec<_>>(),
        vec![1, 3, 5]
    );
    assert_eq!(journal.next_sequence(), 6);
    assert_eq!(journal.recovery().active_generation, 1);
    assert!(
        journal
            .segments()
            .iter()
            .all(|segment| segment.generation() == 1)
    );
    assert!(
        journal.segments()[0]
            .path()
            .ends_with("segment-00000000000000000001-00000000000000000001.qwal")
    );
    let marker = fs::read(directory.path().join("format.qseg")).unwrap();
    assert_eq!(marker.len(), 46);
    assert_eq!(&marker[0..4], b"QSEG");
    assert_eq!(u16::from_le_bytes(marker[4..6].try_into().unwrap()), 2);
    assert_eq!(u64::from_le_bytes(marker[26..34].try_into().unwrap()), 1);
    assert_eq!(u64::from_le_bytes(marker[34..42].try_into().unwrap()), 1);
    journal.close().expect("journal closes");

    assert_eq!(
        read_commands(directory.path(), options).expect("segments read"),
        (1..=5).map(command).collect::<Vec<_>>()
    );
}

#[test]
fn marker_checksum_rejects_an_accidentally_changed_generation_selector() {
    let directory = TestDirectory::new("marker-checksum");
    let options = options(frame_length());
    SegmentedJournal::open(directory.path(), options)
        .unwrap()
        .close()
        .unwrap();
    let marker_path = directory.path().join("format.qseg");
    let mut marker = fs::read(&marker_path).unwrap();
    marker[26] ^= 0x02;
    fs::write(&marker_path, marker).unwrap();
    assert!(matches!(
        SegmentedJournal::open(directory.path(), options),
        Err(SegmentedJournalError::MarkerChecksumMismatch { .. })
    ));
}

#[test]
fn non_ascii_fixed_width_segment_name_fails_without_string_boundary_assumptions() {
    let directory = TestDirectory::new("non-ascii-name");
    let options = options(frame_length());
    SegmentedJournal::open(directory.path(), options)
        .unwrap()
        .close()
        .unwrap();
    let name = format!("segment-{}é{}.qwal", "0".repeat(19), "0".repeat(20));
    assert_eq!(name.len(), 54);
    fs::write(directory.path().join(name), []).unwrap();
    assert!(matches!(
        SegmentedJournal::open(directory.path(), options),
        Err(SegmentedJournalError::InvalidSegmentName(_))
    ));
}

#[test]
fn batch_rotation_is_whole_and_oversized_batches_are_nonmutating() {
    let directory = TestDirectory::new("batch");
    let options = options(frame_length() * 2);
    let mut journal = SegmentedJournal::open(directory.path(), options).expect("journal opens");
    journal.append(&command(1)).expect("first append");
    let mut batch = JournalBatch::new();
    batch.push(&command(2)).expect("command encodes");
    batch.push(&command(3)).expect("command encodes");
    let receipts = journal.append_batch(&batch).expect("batch rotates whole");
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.segment_start_sequence() == 2)
    );
    assert_eq!(journal.next_sequence(), 4);

    let mut oversized = JournalBatch::new();
    oversized.push(&command(4)).expect("command encodes");
    oversized.push(&command(5)).expect("command encodes");
    oversized.push(&command(6)).expect("command encodes");
    assert!(matches!(
        journal.append_batch(&oversized),
        Err(SegmentedJournalError::SegmentCapacityExceeded { .. })
    ));
    assert_eq!(journal.next_sequence(), 4);
    assert_eq!(journal.segments().len(), 2);
}

#[test]
fn reopen_continues_the_last_segment_and_rejects_configuration_drift() {
    let directory = TestDirectory::new("reopen");
    let options = options(frame_length() * 2);
    let mut journal = SegmentedJournal::open(directory.path(), options).expect("journal opens");
    for value in 1..=3 {
        journal.append(&command(value)).expect("append succeeds");
    }
    journal.close().expect("journal closes");

    let mut reopened = SegmentedJournal::open(directory.path(), options).expect("journal reopens");
    assert_eq!(reopened.recovery().segment_count, 2);
    assert_eq!(reopened.recovery().last_sequence, Some(3));
    assert_eq!(reopened.next_sequence(), 4);
    assert_eq!(
        reopened
            .append(&command(4))
            .expect("continuation appends")
            .segment_start_sequence(),
        3
    );
    reopened.close().expect("journal closes");

    let drifted = SegmentedJournalOptions {
        maximum_segment_bytes: options.maximum_segment_bytes + 1,
        ..options
    };
    assert!(matches!(
        SegmentedJournal::open(directory.path(), drifted),
        Err(SegmentedJournalError::ConfigurationMismatch { .. })
    ));
}

#[test]
fn corruption_or_truncation_in_a_closed_segment_is_never_repaired() {
    let directory = TestDirectory::new("closed-corruption");
    let options = options(frame_length());
    let mut journal = SegmentedJournal::open(directory.path(), options).expect("journal opens");
    journal.append(&command(1)).expect("first segment");
    journal.append(&command(2)).expect("second segment");
    let first_path = journal.segments()[0].path().to_path_buf();
    journal.close().expect("journal closes");
    let length = fs::metadata(&first_path).expect("metadata").len();
    OpenOptions::new()
        .write(true)
        .open(&first_path)
        .expect("segment opens")
        .set_len(length - 1)
        .expect("closed segment truncates");
    let repair = SegmentedJournalOptions {
        journal: JournalOptions {
            recovery: RecoveryMode::RepairTornTail,
            ..options.journal
        },
        ..options
    };
    assert!(matches!(
        SegmentedJournal::open(directory.path(), repair),
        Err(SegmentedJournalError::Segment {
            error: JournalError::TruncatedFrame { .. },
            ..
        })
    ));
}

#[test]
fn only_the_active_segment_can_repair_a_torn_tail_and_continue() {
    let directory = TestDirectory::new("active-repair");
    let options = options(frame_length() * 2);
    let mut journal = SegmentedJournal::open(directory.path(), options).expect("journal opens");
    for value in 1..=3 {
        journal.append(&command(value)).expect("append succeeds");
    }
    let active_path = journal.segments().last().unwrap().path().to_path_buf();
    journal.close().expect("journal closes");
    let length = fs::metadata(&active_path).expect("metadata").len();
    OpenOptions::new()
        .write(true)
        .open(&active_path)
        .expect("segment opens")
        .set_len(length - 5)
        .expect("active tail tears");
    let repair = SegmentedJournalOptions {
        journal: JournalOptions {
            recovery: RecoveryMode::RepairTornTail,
            ..options.journal
        },
        ..options
    };
    let mut recovered =
        SegmentedJournal::open(directory.path(), repair).expect("active tail repairs");
    assert_eq!(recovered.recovery().last_sequence, Some(2));
    assert_eq!(recovered.recovery().truncated_bytes, frame_length() - 5);
    assert_eq!(recovered.next_sequence(), 3);
    recovered.append(&command(3)).expect("sequence continues");
    recovered.close().expect("journal closes");
    assert_eq!(read_commands(directory.path(), options).unwrap().len(), 3);
}

#[test]
fn manager_lease_excludes_other_managers_and_raw_segment_writers() {
    let directory = TestDirectory::new("ownership");
    let options = options(frame_length());
    let mut first = SegmentedJournal::open(directory.path(), options).expect("manager opens");
    first.append(&command(1)).expect("append succeeds");
    assert!(matches!(
        SegmentedJournal::open(directory.path(), options),
        Err(SegmentedJournalError::Storage(
            JournalError::WriterLeaseHeld { .. }
        ))
    ));
    let segment_path = first.segments()[0].path();
    assert!(matches!(
        Journal::open(segment_path, options.journal),
        Err(JournalError::ManagedSegment { .. })
    ));
    first.close().expect("manager closes");
}

#[test]
fn empty_final_segment_from_interrupted_rotation_is_valid_and_reused() {
    let directory = TestDirectory::new("empty-final");
    let options = options(frame_length());
    let mut journal = SegmentedJournal::open(directory.path(), options).expect("journal opens");
    journal.append(&command(1)).expect("first segment");
    journal.close().expect("journal closes");
    let empty_path = directory
        .path()
        .join("segment-00000000000000000001-00000000000000000002.qwal");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&empty_path)
        .expect("empty next segment creates");
    file.sync_all().expect("empty segment syncs");

    let mut reopened = SegmentedJournal::open(directory.path(), options).expect("journal reopens");
    assert_eq!(reopened.next_sequence(), 2);
    assert_eq!(reopened.segments().len(), 2);
    assert_eq!(
        reopened
            .append(&command(2))
            .expect("empty segment is reused")
            .segment_start_sequence(),
        2
    );
}

#[test]
fn sequence_exhaustion_is_detected_before_rotation() {
    let directory = TestDirectory::new("sequence-exhaustion");
    let mut options = options(frame_length());
    options.journal.initial_sequence = u64::MAX - 1;
    let mut journal = SegmentedJournal::open(directory.path(), options).expect("journal opens");
    journal
        .append(&command(1))
        .expect("penultimate sequence appends");
    assert!(matches!(
        journal.append(&command(2)),
        Err(SegmentedJournalError::Storage(
            JournalError::SequenceExhausted
        ))
    ));
    assert_eq!(journal.segments().len(), 1);
}

#[test]
fn malformed_abandoned_manager_lease_has_explicit_recovery() {
    let directory = TestDirectory::new("invalid-manager-lease");
    let options = options(frame_length());
    SegmentedJournal::open(directory.path(), options)
        .expect("journal opens")
        .close()
        .expect("journal closes");
    let lease_path = SegmentedJournal::writer_lease_path(directory.path()).expect("lease path");
    fs::write(&lease_path, b"invalid").expect("malformed lease is installed");
    assert!(matches!(
        SegmentedJournal::open(directory.path(), options),
        Err(SegmentedJournalError::Storage(
            JournalError::InvalidWriterLease { .. }
        ))
    ));
    SegmentedJournal::recover_abandoned_invalid_writer(directory.path())
        .expect("malformed abandoned lease is removed");
    SegmentedJournal::open(directory.path(), options).expect("manager reopens");
}

#[test]
fn reader_ignores_and_validated_writer_cleans_nonselected_generation_artifacts() {
    let directory = TestDirectory::new("inactive-generation-cleanup");
    let options = options(frame_length());
    let mut journal = SegmentedJournal::open(directory.path(), options).unwrap();
    journal.append(&command(1)).unwrap();
    let active = journal.segments()[0].path().to_path_buf();
    journal.close().unwrap();

    let inactive = directory
        .path()
        .join("segment-00000000000000000002-00000000000000000001.qwal");
    fs::copy(&active, &inactive).unwrap();
    let marker_pending = directory.path().join("format.qseg.pending");
    let cutover_pending = directory.path().join(".quotick-segments.cutover.pending");
    fs::write(&marker_pending, b"abandoned marker").unwrap();
    fs::write(&cutover_pending, b"abandoned anchor").unwrap();

    assert_eq!(
        read_commands(directory.path(), options).unwrap(),
        vec![command(1)]
    );
    assert!(inactive.exists());
    let recovered = SegmentedJournal::open(directory.path(), options).unwrap();
    assert_eq!(recovered.recovery().active_generation, 1);
    assert_eq!(recovered.segments().len(), 1);
    assert!(!inactive.exists());
    assert!(!marker_pending.exists());
    assert!(!cutover_pending.exists());
    recovered.close().unwrap();
}

#[test]
fn invalid_selected_generation_preserves_nonselected_files_for_diagnosis() {
    let directory = TestDirectory::new("invalid-active-preserves-inactive");
    let options = options(frame_length());
    let mut journal = SegmentedJournal::open(directory.path(), options).unwrap();
    journal.append(&command(1)).unwrap();
    let active = journal.segments()[0].path().to_path_buf();
    journal.close().unwrap();
    let inactive = directory
        .path()
        .join("segment-00000000000000000002-00000000000000000001.qwal");
    fs::copy(&active, &inactive).unwrap();
    let length = fs::metadata(&active).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&active)
        .unwrap()
        .set_len(length - 1)
        .unwrap();

    assert!(matches!(
        SegmentedJournal::open(directory.path(), options),
        Err(SegmentedJournalError::Segment {
            error: JournalError::TruncatedFrame { .. },
            ..
        })
    ));
    assert!(inactive.exists());
}

#[test]
fn incomplete_marker_recovery_is_limited_to_pre_segment_initialization() {
    let directory = TestDirectory::new("invalid-marker");
    fs::create_dir(directory.path()).expect("directory creates");
    let marker_path = directory.path().join("format.qseg");
    fs::write(&marker_path, b"QSEG\x01").expect("partial marker writes");
    SegmentedJournal::recover_incomplete_initialization(directory.path())
        .expect("pre-segment marker is removed");
    let options = options(frame_length());
    SegmentedJournal::open(directory.path(), options)
        .expect("clean initialization succeeds")
        .close()
        .expect("manager closes");
    assert!(matches!(
        SegmentedJournal::recover_incomplete_initialization(directory.path()),
        Err(SegmentedJournalError::MarkerRecoveryNotRequired)
    ));
    assert!(marker_path.exists());

    let unsafe_directory = TestDirectory::new("unsafe-marker-recovery");
    fs::create_dir(unsafe_directory.path()).expect("directory creates");
    let unsafe_marker = unsafe_directory.path().join("format.qseg");
    fs::write(&unsafe_marker, b"QSEG\x01").expect("partial marker writes");
    fs::write(
        unsafe_directory
            .path()
            .join("segment-00000000000000000001-00000000000000000001.qwal"),
        [],
    )
    .expect("segment evidence writes");
    assert!(matches!(
        SegmentedJournal::recover_incomplete_initialization(unsafe_directory.path()),
        Err(SegmentedJournalError::MarkerRecoveryUnsafe(_))
    ));
    assert!(unsafe_marker.exists());
}
