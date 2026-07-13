// quotick/src/store.rs
//! The high-level `QtbStore` for reading and writing `.qtb` files.

use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::mem;
use std::path::Path;

use memmap2::Mmap;

use crate::encode::Encoder;
use crate::enums::{Schema, SType};
use crate::error::{Error, Result};
use crate::metadata::Metadata;
use crate::record::{Record, RecordHeader};

/// The internal state of a `QtbStore`.
enum StoreState {
    Reader {
        mmap: Mmap,
        metadata: Metadata,
        record_offset: usize,
    },
    Writer(Encoder<File>),
    Closed,
}

/// A store for reading or writing `.qtb` data.
pub struct QtbStore {
    state: StoreState,
}

impl QtbStore {
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
    pub fn file_open(path: impl AsRef<Path>) -> Result<Self> {
        let file = File::open(path)?;
        // SAFETY: The file is backed by a real file on disk, so it's safe to
        // create a memory map. The file is opened read-only, preventing modification.
        let mmap = unsafe { Mmap::map(&file)? };

        let (metadata, metadata_len) = Metadata::read(&mmap)?;

        Ok(Self {
            state: StoreState::Reader {
                mmap,
                metadata,
                record_offset: metadata_len,
            },
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
    /// to the file. This method consumes the `QtbStore`.
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

    /// Returns an iterator over records of a specific type `T`.
    ///
    /// The returned iterator borrows the `QtbStore`, so the store must remain
    /// in scope while the iterator is in use.
    ///
    /// # Panics
    ///
    /// Panics if the store was not opened in read mode.
    pub fn iter<'a, T: Record>(&'a self) -> RecordIter<'a, T> {
        match &self.state {
            StoreState::Reader {
                mmap,
                record_offset,
                ..
            } => RecordIter::new(mmap, *record_offset),
            _ => panic!("Cannot iterate over a store opened in write mode."),
        }
    }
}

// Custom Debug implementation to avoid showing the large mmap buffer.
impl std::fmt::Debug for QtbStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.state {
            StoreState::Reader { metadata, .. } => f
                .debug_struct("QtbStore::Reader")
                .field("metadata", metadata)
                .finish(),
            StoreState::Writer(_) => f.debug_struct("QtbStore::Writer").finish(),
            StoreState::Closed => f.debug_struct("QtbStore::Closed").finish(),
        }
    }
}

/// An iterator over records of a specific type `T`.
/// This struct borrows the underlying memory map to ensure correct lifetimes.
pub struct RecordIter<'a, T: Record> {
    buffer: &'a [u8],
    position: usize,
    _phantom: PhantomData<T>,
}

impl<'a, T: Record> RecordIter<'a, T> {
    /// Creates a new `RecordIter`.
    fn new(buffer: &'a [u8], position: usize) -> Self {
        Self {
            buffer,
            position,
            _phantom: PhantomData,
        }
    }
}

impl<'a, T: Record> Iterator for RecordIter<'a, T> {
    type Item = Result<&'a T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.buffer.len() {
            return None;
        }

        // Ensure there's enough space for at least a header
        if self.position + mem::size_of::<RecordHeader>() > self.buffer.len() {
            return Some(Err(Error::Decode(
                "Incomplete record header at end of buffer".to_string(),
            )));
        }

        // Perform a safe, zero-copy read of the header to check length and rtype.
        // This is safe because RecordHeader is Pod (plain old data).
        let header_bytes =
            &self.buffer[self.position..self.position + mem::size_of::<RecordHeader>()];
        let header: &RecordHeader = bytemuck::from_bytes(header_bytes);

        // Record length is in 8-byte words.
        let record_size = header.length() as usize * 8;
        if record_size == 0 {
            return Some(Err(Error::Decode("Record has zero length".to_string())));
        }

        // Check if the expected record type T matches the rtype in the header
        let expected_rtype = T::RTYPE;
        if header.rtype() != expected_rtype as u8 {
            return Some(Err(Error::InvalidRecordCast));
        }

        // Check if the full record fits in the buffer
        if self.position + record_size > self.buffer.len() {
            return Some(Err(Error::Decode(format!(
                "Incomplete record of size {} at position {}",
                record_size, self.position
            ))));
        }

        // The size of the struct T must match the size from the header
        if mem::size_of::<T>() != record_size {
            return Some(Err(Error::Decode(format!(
                "Mismatched record size for rtype {:?}: header says {}, struct is {}",
                expected_rtype,
                record_size,
                mem::size_of::<T>()
            ))));
        }

        // All checks passed, perform the zero-copy cast.
        // This is safe because:
        // 1. We have checked the buffer bounds.
        // 2. The file format guarantees 8-byte alignment for all records.
        // 3. We have checked the record's rtype.
        // 4. We have checked that the size in the header matches the size of struct T.
        // 5. T implements the `bytemuck::Pod` trait, ensuring it has no padding
        //    and is safe for byte-level reinterpretation.
        let record_slice = &self.buffer[self.position..self.position + record_size];
        let record: &'a T = bytemuck::from_bytes(record_slice);

        self.position += record_size;

        Some(Ok(record))
    }
}
