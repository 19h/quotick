use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::ledger::{JournalEntry, Ledger, LedgerCheckpoint, Posting};
use quotick::snapshot::{PendingSnapshotRecovery, SnapshotError, SnapshotFile, SnapshotOptions};
use quotick::{AccountId, AccountingDate, AssetId, TimestampNs, TransactionId};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-snapshot-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).expect("test directory creates");
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn checkpoint(transaction_id: u64, amount: i128) -> LedgerCheckpoint {
    let mut ledger = Ledger::new();
    ledger
        .post(
            JournalEntry::new(
                TransactionId::new(transaction_id).expect("transaction ID"),
                transaction_id,
                AccountingDate::UNIX_EPOCH,
                TimestampNs::from_unix_nanos(0),
                vec![
                    Posting {
                        account_id: AccountId::new(1).expect("account ID"),
                        asset_id: AssetId::new(1).expect("asset ID"),
                        amount,
                    },
                    Posting {
                        account_id: AccountId::new(2).expect("account ID"),
                        asset_id: AssetId::new(1).expect("asset ID"),
                        amount: -amount,
                    },
                ],
            )
            .expect("entry balances"),
        )
        .expect("entry posts");
    ledger.checkpoint().expect("checkpoint captures")
}

fn install_as_pending(staging: &Path, target: &Path, value: &LedgerCheckpoint) {
    SnapshotFile::write(staging, value, SnapshotOptions::default()).expect("staging writes");
    fs::rename(staging, SnapshotFile::pending_path(target)).expect("pending installs");
}

#[test]
fn snapshot_round_trip_has_stable_header_and_detects_corruption() {
    let directory = TestDirectory::new("round-trip");
    let path = directory.join("ledger.qsnp");
    let checkpoint = checkpoint(1, 500);
    let receipt = SnapshotFile::write(&path, &checkpoint, SnapshotOptions::default())
        .expect("snapshot writes");
    assert_eq!(receipt.generation(), 1);
    let bytes = fs::read(&path).expect("snapshot reads");
    assert_eq!(
        u64::try_from(bytes.len()).unwrap(),
        receipt.payload_length() + 28
    );
    assert_eq!(&bytes[0..4], b"QSNP");
    assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), 9);
    assert_eq!(u16::from_le_bytes(bytes[6..8].try_into().unwrap()), 1);
    assert_eq!(
        u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        receipt.payload_length()
    );
    assert_eq!(
        SnapshotFile::read::<LedgerCheckpoint>(&path, SnapshotOptions::default()).unwrap(),
        checkpoint
    );

    for version in [1_u16, 2_u16, 3_u16, 4_u16, 5_u16, 6_u16, 7_u16, 8_u16] {
        let legacy_path = directory.join(&format!("expired-v{version}.qsnp"));
        let mut legacy = bytes.clone();
        legacy[4..6].copy_from_slice(&version.to_le_bytes());
        fs::write(&legacy_path, legacy).unwrap();
        assert!(matches!(
            SnapshotFile::read::<LedgerCheckpoint>(&legacy_path, SnapshotOptions::default()),
            Err(SnapshotError::UnsupportedVersion(actual)) if actual == version
        ));
    }

    let last = bytes.len() - 1;
    let mut corrupt = bytes;
    corrupt[last] ^= 0x80;
    fs::write(&path, corrupt).expect("corruption writes");
    assert!(matches!(
        SnapshotFile::read::<LedgerCheckpoint>(&path, SnapshotOptions::default()),
        Err(SnapshotError::ChecksumMismatch { .. })
    ));
}

#[test]
fn snapshot_replacement_is_atomic_at_the_file_namespace() {
    let directory = TestDirectory::new("replacement");
    let path = directory.join("ledger.qsnp");
    let first = checkpoint(1, 100);
    let mut ledger = Ledger::from_checkpoint(&first).expect("checkpoint restores");
    ledger
        .post(
            JournalEntry::new(
                TransactionId::new(2).unwrap(),
                2,
                AccountingDate::UNIX_EPOCH,
                TimestampNs::from_unix_nanos(0),
                vec![
                    Posting {
                        account_id: AccountId::new(1).unwrap(),
                        asset_id: AssetId::new(1).unwrap(),
                        amount: 50,
                    },
                    Posting {
                        account_id: AccountId::new(2).unwrap(),
                        asset_id: AssetId::new(1).unwrap(),
                        amount: -50,
                    },
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let second = ledger.checkpoint().unwrap();
    SnapshotFile::write(&path, &checkpoint(1, 100), SnapshotOptions::default()).unwrap();
    SnapshotFile::write(&path, &second, SnapshotOptions::default()).unwrap();
    assert_eq!(
        SnapshotFile::read::<LedgerCheckpoint>(&path, SnapshotOptions::default()).unwrap(),
        second
    );
    assert!(!SnapshotFile::pending_path(&path).exists());
}

#[test]
fn complete_pending_snapshot_is_promoted_and_invalid_pending_is_discarded() {
    let directory = TestDirectory::new("pending");
    let target = directory.join("ledger.qsnp");
    let staging = directory.join("staging.qsnp");
    let value = checkpoint(1, 700);
    install_as_pending(&staging, &target, &value);
    assert_eq!(
        SnapshotFile::recover_pending::<LedgerCheckpoint>(&target, SnapshotOptions::default())
            .expect("pending recovers"),
        PendingSnapshotRecovery::Promoted { generation: 1 }
    );
    assert_eq!(
        SnapshotFile::read::<LedgerCheckpoint>(&target, SnapshotOptions::default()).unwrap(),
        value
    );

    let mut ledger = Ledger::from_checkpoint(&value).unwrap();
    ledger
        .post(
            JournalEntry::new(
                TransactionId::new(2).unwrap(),
                2,
                AccountingDate::UNIX_EPOCH,
                TimestampNs::from_unix_nanos(0),
                vec![
                    Posting {
                        account_id: AccountId::new(1).unwrap(),
                        asset_id: AssetId::new(1).unwrap(),
                        amount: 1,
                    },
                    Posting {
                        account_id: AccountId::new(2).unwrap(),
                        asset_id: AssetId::new(1).unwrap(),
                        amount: -1,
                    },
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let newer = ledger.checkpoint().unwrap();
    install_as_pending(&staging, &target, &newer);
    assert_eq!(
        SnapshotFile::recover_pending::<LedgerCheckpoint>(&target, SnapshotOptions::default())
            .expect("newer pending recovers"),
        PendingSnapshotRecovery::Promoted { generation: 2 }
    );
    assert_eq!(
        SnapshotFile::read::<LedgerCheckpoint>(&target, SnapshotOptions::default()).unwrap(),
        newer
    );

    fs::remove_file(&target).expect("target removes");
    fs::write(SnapshotFile::pending_path(&target), b"partial").expect("partial pending writes");
    assert_eq!(
        SnapshotFile::recover_pending::<LedgerCheckpoint>(&target, SnapshotOptions::default())
            .expect("invalid pending discards"),
        PendingSnapshotRecovery::DiscardedInvalid
    );
    assert!(!SnapshotFile::pending_path(&target).exists());
}

#[test]
fn stale_redundant_and_divergent_pending_generations_are_distinguished() {
    let directory = TestDirectory::new("generation-order");
    let target = directory.join("ledger.qsnp");
    let staging = directory.join("staging.qsnp");
    let first = checkpoint(1, 100);
    let mut ledger = Ledger::from_checkpoint(&first).unwrap();
    ledger
        .post(
            JournalEntry::new(
                TransactionId::new(2).unwrap(),
                2,
                AccountingDate::UNIX_EPOCH,
                TimestampNs::from_unix_nanos(0),
                vec![
                    Posting {
                        account_id: AccountId::new(1).unwrap(),
                        asset_id: AssetId::new(1).unwrap(),
                        amount: 1,
                    },
                    Posting {
                        account_id: AccountId::new(2).unwrap(),
                        asset_id: AssetId::new(1).unwrap(),
                        amount: -1,
                    },
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let second = ledger.checkpoint().unwrap();
    SnapshotFile::write(&target, &second, SnapshotOptions::default()).unwrap();
    install_as_pending(&staging, &target, &first);
    assert_eq!(
        SnapshotFile::recover_pending::<LedgerCheckpoint>(&target, SnapshotOptions::default())
            .unwrap(),
        PendingSnapshotRecovery::DiscardedStale {
            pending_generation: 1,
            current_generation: 2,
        }
    );

    install_as_pending(&staging, &target, &second);
    assert_eq!(
        SnapshotFile::recover_pending::<LedgerCheckpoint>(&target, SnapshotOptions::default())
            .unwrap(),
        PendingSnapshotRecovery::DiscardedRedundant { generation: 2 }
    );

    fs::remove_file(&target).unwrap();
    let left = checkpoint(10, 10);
    let right = checkpoint(11, 20);
    SnapshotFile::write(&target, &left, SnapshotOptions::default()).unwrap();
    install_as_pending(&staging, &target, &right);
    assert!(matches!(
        SnapshotFile::recover_pending::<LedgerCheckpoint>(&target, SnapshotOptions::default()),
        Err(SnapshotError::SameGenerationDivergence { generation: 1 })
    ));
    assert!(SnapshotFile::pending_path(&target).exists());
}

#[test]
fn normal_writes_reject_generation_regression_and_same_generation_forks() {
    let directory = TestDirectory::new("write-order");
    let target = directory.join("ledger.qsnp");
    let first = checkpoint(1, 100);
    let mut ledger = Ledger::from_checkpoint(&first).unwrap();
    ledger
        .post(
            JournalEntry::new(
                TransactionId::new(2).unwrap(),
                2,
                AccountingDate::UNIX_EPOCH,
                TimestampNs::from_unix_nanos(0),
                vec![
                    Posting {
                        account_id: AccountId::new(1).unwrap(),
                        asset_id: AssetId::new(1).unwrap(),
                        amount: 50,
                    },
                    Posting {
                        account_id: AccountId::new(2).unwrap(),
                        asset_id: AssetId::new(1).unwrap(),
                        amount: -50,
                    },
                ],
            )
            .unwrap(),
        )
        .unwrap();
    let current = ledger.checkpoint().unwrap();
    SnapshotFile::write(&target, &current, SnapshotOptions::default()).unwrap();
    assert!(matches!(
        SnapshotFile::write(&target, &first, SnapshotOptions::default()),
        Err(SnapshotError::GenerationRegression {
            current: 2,
            proposed: 1
        })
    ));

    let mut fork = Ledger::from_checkpoint(&checkpoint(10, 999)).unwrap();
    fork.post(
        JournalEntry::new(
            TransactionId::new(11).unwrap(),
            11,
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(0),
            vec![
                Posting {
                    account_id: AccountId::new(1).unwrap(),
                    asset_id: AssetId::new(1).unwrap(),
                    amount: 7,
                },
                Posting {
                    account_id: AccountId::new(2).unwrap(),
                    asset_id: AssetId::new(1).unwrap(),
                    amount: -7,
                },
            ],
        )
        .unwrap(),
    )
    .unwrap();
    assert!(matches!(
        SnapshotFile::write(
            &target,
            &fork.checkpoint().unwrap(),
            SnapshotOptions::default()
        ),
        Err(SnapshotError::SameGenerationDivergence { generation: 2 })
    ));
    assert_eq!(
        SnapshotFile::read::<LedgerCheckpoint>(&target, SnapshotOptions::default()).unwrap(),
        current
    );
}

#[test]
fn invalid_current_preserves_a_complete_pending_snapshot() {
    let directory = TestDirectory::new("invalid-current");
    let target = directory.join("ledger.qsnp");
    let staging = directory.join("staging.qsnp");
    fs::write(&target, b"corrupt-current").unwrap();
    install_as_pending(&staging, &target, &checkpoint(1, 100));
    assert!(matches!(
        SnapshotFile::recover_pending::<LedgerCheckpoint>(&target, SnapshotOptions::default()),
        Err(SnapshotError::CurrentSnapshotInvalid(_))
    ));
    assert!(SnapshotFile::pending_path(&target).exists());
    assert_eq!(fs::read(&target).unwrap(), b"corrupt-current");
}

#[test]
fn malformed_abandoned_snapshot_lease_has_explicit_recovery() {
    let directory = TestDirectory::new("invalid-lease");
    let target = directory.join("ledger.qsnp");
    let lease_path = SnapshotFile::writer_lease_path(&target).expect("lease path resolves");
    fs::write(&lease_path, b"invalid").expect("invalid lease writes");
    assert!(matches!(
        SnapshotFile::write(&target, &checkpoint(1, 1), SnapshotOptions::default()),
        Err(SnapshotError::WriterLease(
            quotick::journal::JournalError::InvalidWriterLease { .. }
        ))
    ));
    SnapshotFile::recover_abandoned_invalid_writer(&target)
        .expect("invalid abandoned lease removes");
    SnapshotFile::write(&target, &checkpoint(1, 1), SnapshotOptions::default())
        .expect("snapshot writes after recovery");
}

#[test]
fn newer_generation_requires_a_proven_history_prefix() {
    let directory = TestDirectory::new("lineage");
    let target = directory.join("ledger.qsnp");
    let staging = directory.join("staging.qsnp");
    let current = checkpoint(1, 100);
    SnapshotFile::write(&target, &current, SnapshotOptions::default()).unwrap();

    let mut fork = Ledger::from_checkpoint(&checkpoint(10, 999)).unwrap();
    fork.post(
        JournalEntry::new(
            TransactionId::new(11).unwrap(),
            11,
            AccountingDate::UNIX_EPOCH,
            TimestampNs::from_unix_nanos(0),
            vec![
                Posting {
                    account_id: AccountId::new(1).unwrap(),
                    asset_id: AssetId::new(1).unwrap(),
                    amount: 7,
                },
                Posting {
                    account_id: AccountId::new(2).unwrap(),
                    asset_id: AssetId::new(1).unwrap(),
                    amount: -7,
                },
            ],
        )
        .unwrap(),
    )
    .unwrap();
    let fork = fork.checkpoint().unwrap();
    assert!(matches!(
        SnapshotFile::write(&target, &fork, SnapshotOptions::default()),
        Err(SnapshotError::LineageDivergence {
            current: 1,
            proposed: 2
        })
    ));
    install_as_pending(&staging, &target, &fork);
    assert!(matches!(
        SnapshotFile::recover_pending::<LedgerCheckpoint>(&target, SnapshotOptions::default()),
        Err(SnapshotError::LineageDivergence {
            current: 1,
            proposed: 2
        })
    ));
    assert!(SnapshotFile::pending_path(&target).exists());
}

#[test]
fn payload_bounds_and_segmented_directory_ownership_fail_before_mutation() {
    let directory = TestDirectory::new("ownership");
    let bounded_target = directory.join("bounded.qsnp");
    assert!(matches!(
        SnapshotFile::write(
            &bounded_target,
            &checkpoint(1, 1),
            SnapshotOptions {
                maximum_payload_bytes: 0
            }
        ),
        Err(SnapshotError::PayloadTooLarge {
            actual: _,
            maximum: 0
        })
    ));
    assert!(!bounded_target.exists());
    assert!(!SnapshotFile::pending_path(&bounded_target).exists());

    let bounded_staging = directory.join("bounded-staging.qsnp");
    install_as_pending(&bounded_staging, &bounded_target, &checkpoint(1, 1));
    assert!(matches!(
        SnapshotFile::recover_pending::<LedgerCheckpoint>(
            &bounded_target,
            SnapshotOptions {
                maximum_payload_bytes: 0
            }
        ),
        Err(SnapshotError::PayloadTooLarge {
            actual: _,
            maximum: 0
        })
    ));
    assert!(!bounded_target.exists());
    assert!(SnapshotFile::pending_path(&bounded_target).exists());

    let managed = directory.join("managed");
    fs::create_dir(&managed).unwrap();
    let marker = managed.join("format.qseg");
    fs::write(&marker, b"segmented-journal-marker").unwrap();
    let canonical_marker = fs::canonicalize(&marker).unwrap();
    let managed_target = managed.join("ledger.qsnp");
    let error = SnapshotFile::write(
        &managed_target,
        &checkpoint(1, 1),
        SnapshotOptions::default(),
    )
    .unwrap_err();
    assert!(
        matches!(
        &error,
        SnapshotError::ManagedSegmentDirectory { marker_path }
            if marker_path == &canonical_marker
        ),
        "unexpected error: {error:?}"
    );
    assert!(!managed_target.exists());
    assert!(!SnapshotFile::pending_path(&managed_target).exists());
    assert_eq!(fs::read_dir(&managed).unwrap().count(), 1);
}
