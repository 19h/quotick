// quotick/src/dbn.rs
//! The high-level `DbnStore` for reading and writing `.qtb` files.

use std::fs::{File, OpenOptions};
use std::path::Path;

use memmap2::Mmap;

use crate::decode::{Decoder, RecordIter};
use crate::encode::Encoder;
use crate::enums::{Schema, SType};
use crate::error::Result;
use crate::metadata::Metadata;
use crate::record::Record;

/// The internal state of a `DbnStore`.
enum StoreState {
    Reader { mmap: Mmap, metadata: Metadata },
    Writer(Encoder<File>),
    Closed,
}

/// A store for reading or writing `.qtb` data.
pub struct DbnStore {
    state: StoreState,
}

impl DbnStore {
    /// Opens a `.qtb` file for reading.
    ///
    /// # Arguments
    ///
    /// *   `path`: The path to the file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened, memory-mapped, or if the
    /// metadata is invalid.
    #[allow(unsafe_code)]
    pub fn file_open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        // SAFETY: The file is backed by a real file on disk, so it's safe to
        // create a memory map. The file is opened read-only, preventing modification.
        let mmap = unsafe { Mmap::map(&file)? };

        let metadata = Decoder::new(&mmap)?.metadata().clone();

        Ok(Self {
            state: StoreState::Reader { mmap, metadata },
        })
    }

    /// Creates a new `.qtb` file for writing.
    ///
    /// # Arguments
    ///
    /// *   `path`: The path to the file to be created.
    /// *   `schema`: The schema of the records that will be written.
    /// *   `symbols`: The list of symbols to be included in the metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created or the initial metadata
    /// cannot be written.
    pub fn file_create(
        path: impl AsRef<Path>,
        schema: Schema,
        symbols: &[&str],
    ) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        let initial_metadata = Metadata::new(
            "QTB.RUST.EXAMPLE",
            schema,
            SType::RawSymbol,
            SType::InstrumentId,
            symbols.iter().map(|s| s.to_string()).collect(),
        );

        let encoder = Encoder::new(file, initial_metadata)?;
        Ok(Self {
            state: StoreState::Writer(encoder),
        })
    }

    /// Writes a single record to the store.
    ///
    /// # Panics
    ///
    /// Panics if the store was not opened in write mode.
    pub fn write_record<T: Record>(&mut self, record: &T) -> Result<()> {
        match &mut self.state {
            StoreState::Writer(encoder) => encoder.write_record(record),
            _ => panic!("Cannot write record to a store opened in read mode."),
        }
    }

    /// Finalizes the store after writing.
    ///
    /// This must be called to ensure the metadata header is correctly written
    /// to the file. This method consumes the `DbnStore`.
    ///
    /// # Panics
    ///
    /// Panics if the store was not opened in write mode.
    pub fn finish(mut self) -> Result<File> {
        let state = std::mem::replace(&mut self.state, StoreState::Closed);
        match state {
            StoreState::Writer(encoder) => encoder.finalize(),
            _ => panic!("Cannot finish a store that was not opened in write mode."),
        }
    }

    /// Returns a reference to the metadata.
    ///
    /// # Panics
    ///
    /// Panics if the store was not opened in read mode.
    pub fn metadata(&self) -> &Metadata {
        match &self.state {
            StoreState::Reader { metadata, .. } => metadata,
            _ => panic!("Cannot get metadata from a store opened in write mode."),
        }
    }

    /// Returns the total number of records in the store, based on the metadata.
    ///
    /// # Panics
    ///
    /// Panics if the store was not opened in read mode.
    pub fn record_count(&self) -> u64 {
        self.metadata().limit()
    }

    /// Consumes the store and returns an iterator over records of a specific type `T`.
    ///
    /// # Panics
    ///
    /// Panics if the store was not opened in read mode.
    #[allow(unsafe_code)]
    pub fn into_iter<T: Record>(mut self) -> RecordIter<'static, T> {
        let state = std::mem::replace(&mut self.state, StoreState::Closed);
        match state {
            StoreState::Reader { mmap, .. } => {
                // Create a buffer reference with a 'static lifetime.
                // SAFETY: This is sound because the returned RecordIter takes ownership
                // of the Mmap, guaranteeing the buffer is valid for the iterator's lifetime.
                let buffer: &'static [u8] = unsafe { std::mem::transmute(mmap.as_ref()) };
                let decoder = Decoder::new(buffer).expect("Metadata was already validated");
                RecordIter::new(decoder, mmap)
            }
            _ => panic!("Cannot iterate over a store opened in write mode."),
        }
    }
}

// Custom Debug implementation to avoid showing the large mmap buffer.
impl std::fmt::Debug for DbnStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.state {
            StoreState::Reader { metadata, .. } => f
                .debug_struct("DbnStore::Reader")
                .field("metadata", metadata)
                .finish(),
            StoreState::Writer(_) => f.debug_struct("DbnStore::Writer").finish(),
            StoreState::Closed => f.debug_struct("DbnStore::Closed").finish(),
        }
    }
}
