//! Internal layout-independent durable writer and streaming reader dispatch.

use std::path::Path;

use crate::journal::{
    DurableRecord, Journal, JournalError, JournalFrame, JournalLayout, JournalOptions,
    JournalReader, SegmentedJournal, SegmentedJournalError, SegmentedJournalOptions,
    SegmentedJournalReader, StorageRecoveryReport,
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum StorageOptions {
    Single(JournalOptions),
    Segmented(SegmentedJournalOptions),
}

#[derive(Debug)]
pub(crate) enum StorageError {
    Journal(JournalError),
    Segmented(SegmentedJournalError),
}

#[derive(Debug)]
pub(crate) enum StorageWriter {
    Single {
        journal: Journal,
        initial_sequence: u64,
    },
    Segmented(SegmentedJournal),
}

impl StorageWriter {
    pub(crate) fn open(path: &Path, options: StorageOptions) -> Result<Self, StorageError> {
        match options {
            StorageOptions::Single(options) => Journal::open(path, options)
                .map(|journal| Self::Single {
                    journal,
                    initial_sequence: options.initial_sequence,
                })
                .map_err(StorageError::Journal),
            StorageOptions::Segmented(options) => SegmentedJournal::open(path, options)
                .map(Self::Segmented)
                .map_err(StorageError::Segmented),
        }
    }

    pub(crate) fn recovery(&self) -> StorageRecoveryReport {
        match self {
            Self::Single {
                journal,
                initial_sequence,
            } => {
                let recovery = journal.recovery();
                StorageRecoveryReport {
                    layout: JournalLayout::SingleFile,
                    first_sequence: *initial_sequence,
                    last_sequence: recovery.last_sequence,
                    valid_bytes: u128::from(recovery.valid_bytes),
                    truncated_bytes: recovery.truncated_bytes,
                    segment_count: 1,
                    active_segment_start: *initial_sequence,
                }
            }
            Self::Segmented(journal) => {
                let recovery = journal.recovery();
                StorageRecoveryReport {
                    layout: JournalLayout::Segmented,
                    first_sequence: recovery.first_sequence,
                    last_sequence: recovery.last_sequence,
                    valid_bytes: recovery.valid_bytes,
                    truncated_bytes: recovery.truncated_bytes,
                    segment_count: recovery.segment_count,
                    active_segment_start: recovery.active_segment_start,
                }
            }
        }
    }

    pub(crate) fn path(&self) -> &Path {
        match self {
            Self::Single { journal, .. } => journal.path(),
            Self::Segmented(journal) => journal.directory(),
        }
    }

    pub(crate) const fn layout(&self) -> JournalLayout {
        match self {
            Self::Single { .. } => JournalLayout::SingleFile,
            Self::Segmented(_) => JournalLayout::Segmented,
        }
    }

    pub(crate) fn append<T: DurableRecord>(&mut self, record: &T) -> Result<(), StorageError> {
        match self {
            Self::Single { journal, .. } => journal
                .append(record)
                .map(|_| ())
                .map_err(StorageError::Journal),
            Self::Segmented(journal) => journal
                .append(record)
                .map(|_| ())
                .map_err(StorageError::Segmented),
        }
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        match self {
            Self::Single { journal, .. } => journal.is_poisoned(),
            Self::Segmented(journal) => journal.is_poisoned(),
        }
    }

    pub(crate) fn sync_all(&mut self) -> Result<(), StorageError> {
        match self {
            Self::Single { journal, .. } => journal.sync_all().map_err(StorageError::Journal),
            Self::Segmented(journal) => journal.sync_all().map_err(StorageError::Segmented),
        }
    }

    pub(crate) fn close(self) -> Result<(), StorageError> {
        match self {
            Self::Single { journal, .. } => journal.close().map_err(StorageError::Journal),
            Self::Segmented(journal) => journal.close().map_err(StorageError::Segmented),
        }
    }
}

#[derive(Debug)]
pub(crate) enum StorageReader {
    Single(JournalReader),
    Segmented(SegmentedJournalReader),
}

impl StorageReader {
    pub(crate) fn open(path: &Path, options: StorageOptions) -> Result<Self, StorageError> {
        match options {
            StorageOptions::Single(options) => JournalReader::open(path, options)
                .map(Self::Single)
                .map_err(StorageError::Journal),
            StorageOptions::Segmented(options) => SegmentedJournalReader::open(path, options)
                .map(Self::Segmented)
                .map_err(StorageError::Segmented),
        }
    }
}

impl Iterator for StorageReader {
    type Item = Result<JournalFrame, StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Single(reader) => reader
                .next()
                .map(|result| result.map_err(StorageError::Journal)),
            Self::Segmented(reader) => reader
                .next()
                .map(|result| result.map_err(StorageError::Segmented)),
        }
    }
}
