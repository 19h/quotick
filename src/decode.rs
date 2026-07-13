// quotick/src/decode.rs
//! Provides the zero-copy `Decoder` for reading `.qtb` data from a byte slice.

use std::mem;

use crate::error::{Error, Result};
use crate::metadata::Metadata;
use crate::record::{Record, RecordHeader};

/// A zero-copy decoder for `.qtb` data in a byte buffer.
///
/// The decoder provides an iterator-like interface for reading records directly
/// from an underlying byte buffer without intermediate copying or deserialization.
/// It assumes that the buffer is correctly aligned.
pub struct Decoder<'a> {
    buffer: &'a [u8],
    metadata: Metadata,
    position: usize,
}

impl<'a> Decoder<'a> {
    /// Creates a new `Decoder` from a byte buffer.
    ///
    /// This method will parse the metadata header from the beginning of the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if the metadata cannot be parsed, e.g., due to an
    /// invalid magic string, unsupported version, or incomplete header.
    pub fn new(buffer: &'a [u8]) -> Result<Self> {
        let (metadata, metadata_len) = Metadata::read(buffer)?;

        Ok(Self {
            buffer,
            metadata,
            position: metadata_len,
        })
    }

    /// Returns a reference to the parsed metadata.
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Reads the next record of type `T` from the buffer.
    ///
    /// This method performs a zero-copy read, returning a reference to the record
    /// data within the underlying buffer.
    ///
    /// # Returns
    ///
    /// - `Some(Ok(&T))`: If a record was successfully read.
    /// - `Some(Err(Error))`: If an error occurred while reading the record.
    /// - `None`: If the end of the buffer is reached.
    pub fn next_record<T: Record>(&mut self) -> Option<Result<&'a T>> {
        if self.position >= self.buffer.len() {
            return None;
        }

        // Ensure there's enough space for at least a header
        if self.position + mem::size_of::<RecordHeader>() > self.buffer.len() {
            return Some(Err(Error::Decode(
                "Incomplete record header at end of buffer".to_string(),
            )));
        }

        // Perform an unaligned read of the header to check length and rtype safely.
        let header_bytes = &self.buffer[self.position..self.position + mem::size_of::<RecordHeader>()];
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
        let record_slice = &self.buffer[self.position..self.position + record_size];
        let record: &'a T = bytemuck::from_bytes(record_slice);

        self.position += record_size;

        Some(Ok(record))
    }
}
