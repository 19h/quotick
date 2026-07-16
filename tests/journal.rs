use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use quotick::journal::{
    Durability, Journal, JournalBatch, JournalError, JournalOptions, JournalReader, RecordKind,
    RecoveryMode,
};
use quotick::matching::{Command, NewOrder, OrderType, SelfTradePrevention, TimeInForce};
use quotick::{
    AccountId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-{label}-{}-{nonce}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn command(command_id: u64, order_id: u64) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).expect("command id"),
        order_id: OrderId::new(order_id).expect("order id"),
        account_id: AccountId::new(10).expect("account id"),
        instrument_id: InstrumentId::new(20).expect("instrument id"),
        instrument_version: InstrumentVersion::new(1).expect("instrument version"),
        side: Side::Buy,
        quantity: Quantity::new(30).expect("positive quantity"),
        display: quotick::matching::OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(40)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn buffered_options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn read_frames(path: &Path) -> Vec<quotick::journal::JournalFrame> {
    JournalReader::open(path, JournalOptions::default())
        .expect("reader opens")
        .collect::<Result<Vec<_>, _>>()
        .expect("journal frames are valid")
}

#[test]
fn append_reopen_and_typed_decode_preserve_sequence_and_payload() {
    let file = TestFile::new("round-trip");
    let mut journal = Journal::open(file.path(), buffered_options()).expect("journal opens");
    assert_eq!(journal.recovery().last_sequence, None);
    assert_eq!(
        journal
            .append(&command(1, 1))
            .expect("first append")
            .sequence,
        1
    );
    assert_eq!(
        journal
            .append(&command(2, 2))
            .expect("second append")
            .sequence,
        2
    );
    journal.sync_data().expect("explicit durability barrier");
    drop(journal);

    let bytes = fs::read(file.path()).expect("journal bytes read");
    assert_eq!(&bytes[..4], b"QWAL");
    assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), 12);

    let frames = read_frames(file.path());
    assert_eq!(frames.len(), 2);
    assert_eq!(frames[0].sequence(), 1);
    assert_eq!(frames[0].kind(), RecordKind::Command);
    assert_eq!(
        frames[0].decode::<Command>().expect("typed decode"),
        command(1, 1)
    );
    assert_eq!(
        frames[1].decode::<Command>().expect("typed decode"),
        command(2, 2)
    );

    let reopened = Journal::open(file.path(), buffered_options()).expect("journal reopens");
    assert_eq!(reopened.recovery().last_sequence, Some(2));
    assert_eq!(reopened.next_sequence(), 3);
}

#[test]
fn expired_wal_versions_are_rejected_explicitly() {
    for version in [1_u16, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] {
        let file = TestFile::new("expired-version");
        let mut journal = Journal::open(file.path(), buffered_options()).unwrap();
        journal.append(&command(1, 1)).unwrap();
        journal.close().unwrap();
        let mut bytes = fs::read(file.path()).unwrap();
        bytes[4..6].copy_from_slice(&version.to_le_bytes());
        fs::write(file.path(), bytes).unwrap();
        let error = JournalReader::open(file.path(), buffered_options())
            .unwrap()
            .next()
            .expect("one frame exists")
            .expect_err("expired WAL version is not interpreted as version twelve");
        assert!(matches!(
            error,
            JournalError::UnsupportedVersion {
                offset: 0,
                version: actual
            } if actual == version
        ));
    }
}

#[test]
fn exclusive_writer_lease_blocks_a_second_writer_but_not_snapshot_readers() {
    let file = TestFile::new("exclusive-writer");
    let first = Journal::open(file.path(), buffered_options()).expect("first writer opens");
    let owner = first.writer_lease_owner();
    assert_eq!(owner.process_id(), std::process::id());
    let lease_path = Journal::writer_lease_path(file.path()).expect("lease path resolves");
    assert!(lease_path.exists());
    assert!(matches!(
        Journal::open(file.path(), buffered_options()),
        Err(JournalError::WriterLeaseHeld {
            owner: actual,
            ..
        }) if actual == owner
    ));
    let reader = JournalReader::open(file.path(), buffered_options()).expect("reader opens");
    assert_eq!(reader.count(), 0);

    first.close().expect("first writer closes durably");
    assert!(!lease_path.exists());
    Journal::open(file.path(), buffered_options()).expect("writer reopens after release");
}

#[test]
fn abandoned_writer_recovery_requires_the_exact_observed_owner() {
    let first_file = TestFile::new("owner-cas-first");
    let second_file = TestFile::new("owner-cas-second");
    let first = Journal::open(first_file.path(), buffered_options()).expect("first writer opens");
    let second =
        Journal::open(second_file.path(), buffered_options()).expect("second writer opens");
    assert!(matches!(
        Journal::recover_abandoned_writer(first_file.path(), second.writer_lease_owner()),
        Err(JournalError::WriterLeaseOwnerMismatch { .. })
    ));
    assert!(matches!(
        Journal::open(first_file.path(), buffered_options()),
        Err(JournalError::WriterLeaseHeld { .. })
    ));
    second.close().expect("second writer closes");
    first.close().expect("first writer closes");
}

#[test]
fn malformed_writer_lease_fails_closed_and_is_never_reclaimed_implicitly() {
    let file = TestFile::new("malformed-lease");
    let lease_path = Journal::writer_lease_path(file.path()).expect("lease path resolves");
    fs::write(&lease_path, b"invalid").expect("malformed lease writes");
    assert!(matches!(
        Journal::open(file.path(), buffered_options()),
        Err(JournalError::InvalidWriterLease {
            lease_path: actual
        }) if actual == lease_path
    ));
    assert!(lease_path.exists());
    Journal::recover_abandoned_invalid_writer(file.path())
        .expect("invalid abandoned lease recovers explicitly");
    assert!(!lease_path.exists());
    Journal::open(file.path(), buffered_options()).expect("writer opens after recovery");
}

#[test]
fn abandoned_cutover_staging_is_discarded_only_under_new_writer_ownership() {
    let file = TestFile::new("abandoned-cutover");
    let mut initialized = Journal::open(file.path(), buffered_options()).unwrap();
    initialized.append(&command(1, 1)).unwrap();
    initialized.close().unwrap();
    let pending = Journal::cutover_pending_path(file.path()).unwrap();
    fs::write(&pending, b"incomplete replacement").unwrap();

    let live = Journal::open(file.path(), buffered_options()).unwrap();
    assert!(matches!(
        Journal::recover_abandoned_cutover_pending(file.path()),
        Err(JournalError::WriterLeaseHeld { .. })
    ));
    assert!(pending.exists());
    live.close().unwrap();

    Journal::recover_abandoned_cutover_pending(file.path()).unwrap();
    assert!(!pending.exists());
    assert_eq!(read_frames(file.path()).len(), 1);
    assert!(matches!(
        Journal::recover_abandoned_cutover_pending(file.path()),
        Err(JournalError::CutoverPendingMissing { .. })
    ));
}

#[test]
fn explicit_close_reports_lease_corruption_instead_of_silently_abandoning_it() {
    let file = TestFile::new("close-corrupt-lease");
    let journal = Journal::open(file.path(), buffered_options()).expect("writer opens");
    let lease_path = Journal::writer_lease_path(file.path()).expect("lease path resolves");
    fs::write(&lease_path, b"corrupt").expect("lease is externally corrupted");
    assert!(matches!(
        journal.close(),
        Err(JournalError::InvalidWriterLease { .. })
    ));
    assert!(lease_path.exists());
    Journal::recover_abandoned_invalid_writer(file.path())
        .expect("corrupted abandoned lease recovers explicitly");
}

#[cfg(unix)]
#[test]
fn canonical_symlink_alias_cannot_acquire_a_second_writer_lease() {
    use std::os::unix::fs::symlink;

    let file = TestFile::new("symlink-target");
    let alias = TestFile::new("symlink-alias");
    let writer = Journal::open(file.path(), buffered_options()).expect("writer opens");
    symlink(file.path(), alias.path()).expect("symlink creates");
    assert!(matches!(
        Journal::open(alias.path(), buffered_options()),
        Err(JournalError::WriterLeaseHeld { .. })
    ));
    writer.close().expect("writer closes");
}

#[test]
fn strict_recovery_rejects_an_incomplete_tail() {
    let file = TestFile::new("strict-tail");
    let mut journal = Journal::open(file.path(), buffered_options()).expect("journal opens");
    journal.append(&command(1, 1)).expect("first append");
    journal.append(&command(2, 2)).expect("second append");
    journal.sync_data().expect("sync");
    drop(journal);
    let length = fs::metadata(file.path()).expect("metadata").len();
    OpenOptions::new()
        .write(true)
        .open(file.path())
        .expect("file opens")
        .set_len(length - 3)
        .expect("tail truncated");

    assert!(matches!(
        Journal::open(file.path(), JournalOptions::default()),
        Err(JournalError::TruncatedFrame { .. })
    ));
}

#[test]
fn repair_mode_truncates_only_the_torn_tail_and_continues_sequence() {
    let file = TestFile::new("repair-tail");
    let mut journal = Journal::open(file.path(), buffered_options()).expect("journal opens");
    journal.append(&command(1, 1)).expect("first append");
    journal.append(&command(2, 2)).expect("second append");
    journal.sync_data().expect("sync");
    drop(journal);
    let length = fs::metadata(file.path()).expect("metadata").len();
    OpenOptions::new()
        .write(true)
        .open(file.path())
        .expect("file opens")
        .set_len(length - 5)
        .expect("tail truncated");

    let options = JournalOptions {
        recovery: RecoveryMode::RepairTornTail,
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let mut repaired = Journal::open(file.path(), options).expect("tail repaired");
    assert_eq!(repaired.recovery().last_sequence, Some(1));
    assert!(repaired.recovery().truncated_bytes > 0);
    assert_eq!(
        repaired
            .append(&command(3, 3))
            .expect("append after repair")
            .sequence,
        2
    );
    repaired.sync_data().expect("sync repaired journal");
    drop(repaired);

    let frames = read_frames(file.path());
    assert_eq!(frames.len(), 2);
    assert_eq!(
        frames[1].decode::<Command>().expect("decode"),
        command(3, 3)
    );
}

#[test]
fn checksum_corruption_is_never_treated_as_a_repairable_tail() {
    let file = TestFile::new("checksum");
    let mut journal = Journal::open(file.path(), buffered_options()).expect("journal opens");
    journal.append(&command(1, 1)).expect("append");
    journal.sync_data().expect("sync");
    drop(journal);

    let mut raw = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file.path())
        .expect("file opens");
    raw.seek(SeekFrom::End(-1)).expect("seek");
    let mut byte = [0_u8; 1];
    raw.read_exact(&mut byte).expect("read byte");
    raw.seek(SeekFrom::End(-1)).expect("seek");
    raw.write_all(&[byte[0] ^ 0x80]).expect("corrupt byte");
    raw.sync_data().expect("sync corruption");

    let options = JournalOptions {
        recovery: RecoveryMode::RepairTornTail,
        ..JournalOptions::default()
    };
    assert!(matches!(
        Journal::open(file.path(), options),
        Err(JournalError::ChecksumMismatch { .. })
    ));
}

#[test]
fn sequence_gap_is_detected_even_when_each_frame_is_individually_valid() {
    let file = TestFile::new("sequence-gap");
    let mut journal = Journal::open(file.path(), buffered_options()).expect("journal opens");
    journal.append(&command(1, 1)).expect("first append");
    journal.append(&command(2, 2)).expect("second append");
    journal.sync_data().expect("sync");
    drop(journal);
    let frames = read_frames(file.path());
    let first_length = frames[0].frame_length();
    let bytes = fs::read(file.path()).expect("read journal");
    fs::write(file.path(), &bytes[first_length..]).expect("remove first frame");

    assert!(matches!(
        Journal::open(file.path(), JournalOptions::default()),
        Err(JournalError::SequenceDiscontinuity {
            expected: 1,
            actual: 2,
            ..
        })
    ));
}

#[test]
fn payload_limit_rejection_does_not_write_or_consume_sequence() {
    let file = TestFile::new("payload-limit");
    let options = JournalOptions {
        max_payload_length: 32,
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let mut journal = Journal::open(file.path(), options).expect("journal opens");
    assert!(matches!(
        journal.append_raw(RecordKind::Command, &[0_u8; 33]),
        Err(JournalError::PayloadTooLarge {
            length: 33,
            maximum: 32
        })
    ));
    assert_eq!(journal.next_sequence(), 1);
    assert_eq!(fs::metadata(file.path()).expect("metadata").len(), 0);
}

#[test]
fn heterogeneous_batch_uses_contiguous_sequences_and_one_reopen_boundary() {
    let file = TestFile::new("batch");
    let mut batch = JournalBatch::new();
    batch.push(&command(1, 1)).expect("first command encodes");
    batch.push(&command(2, 2)).expect("second command encodes");
    assert_eq!(batch.len(), 2);

    let mut journal = Journal::open(file.path(), buffered_options()).expect("journal opens");
    let receipts = journal.append_batch(&batch).expect("batch appends");
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(journal.next_sequence(), 3);
    journal.sync_data().expect("batch syncs");
    drop(journal);

    let frames = read_frames(file.path());
    assert_eq!(frames.len(), 2);
    assert_eq!(
        frames[0].decode::<Command>().expect("decode"),
        command(1, 1)
    );
    assert_eq!(
        frames[1].decode::<Command>().expect("decode"),
        command(2, 2)
    );
}

#[test]
fn segment_initial_sequence_is_verified_and_continued() {
    let file = TestFile::new("segment-sequence");
    let options = JournalOptions {
        initial_sequence: 10_000,
        durability: Durability::Buffered,
        ..JournalOptions::default()
    };
    let mut journal = Journal::open(file.path(), options).expect("segment opens");
    assert_eq!(
        journal.append(&command(1, 1)).expect("append").sequence,
        10_000
    );
    journal.sync_data().expect("sync");
    drop(journal);

    let frames = JournalReader::open(file.path(), options)
        .expect("reader opens")
        .collect::<Result<Vec<_>, _>>()
        .expect("segment verifies");
    assert_eq!(frames[0].sequence(), 10_000);
    let reopened = Journal::open(file.path(), options).expect("segment reopens");
    assert_eq!(reopened.next_sequence(), 10_001);
}

#[test]
fn crash_writer_child() {
    let Ok(path) = std::env::var("QUOTICK_CRASH_TEST_PATH") else {
        return;
    };
    let mut journal = Journal::open(path, JournalOptions::default()).expect("child journal opens");
    for sequence in 1_u64..=u64::MAX {
        journal
            .append(&command(sequence, sequence))
            .expect("child append succeeds until termination");
    }
}

#[test]
fn forced_process_termination_recovers_a_contiguous_durable_prefix() {
    let file = TestFile::new("forced-termination");
    let executable = std::env::current_exe().expect("test executable path");
    let mut child = ProcessCommand::new(executable)
        .args(["--exact", "crash_writer_child", "--nocapture"])
        .env("QUOTICK_CRASH_TEST_PATH", file.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("crash writer starts");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if fs::metadata(file.path()).is_ok_and(|metadata| metadata.len() >= 4_096) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "crash writer did not make progress"
        );
        assert!(
            child.try_wait().expect("child status").is_none(),
            "crash writer exited before forced termination"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    child.kill().expect("force termination succeeds");
    child.wait().expect("terminated child is reaped");

    let repair_options = JournalOptions {
        recovery: RecoveryMode::RepairTornTail,
        ..JournalOptions::default()
    };
    let abandoned_owner = match Journal::open(file.path(), repair_options) {
        Err(JournalError::WriterLeaseHeld { owner, .. }) => owner,
        result => panic!("terminated writer must leave a lease, found {result:?}"),
    };
    Journal::recover_abandoned_writer(file.path(), abandoned_owner)
        .expect("exact abandoned owner is recovered after child exit");
    let recovered = Journal::open(file.path(), repair_options).expect("journal recovers");
    assert!(recovered.recovery().last_sequence.is_some());
    drop(recovered);
    let frames = read_frames(file.path());
    assert!(!frames.is_empty());
    assert!(
        frames
            .iter()
            .enumerate()
            .all(|(index, frame)| frame.sequence() == u64::try_from(index + 1).expect("index"))
    );
}
