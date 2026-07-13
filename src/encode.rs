// quotick/src/encode.rs
//! Provides the `Encoder` for writing records to a `.qtb` file or stream.

use std::io::{Seek, SeekFrom, Write};
use std::mem;

use crate::error::{Error, Result};
use crate::metadata::{
    MappingInterval, Metadata, MetadataHeader, SymbolMapping, QTB_MAGIC, QTB_VERSION,
};
use crate::record::{Record, SYMBOL_CSTR_LEN};

/// An encoder for writing `.qtb` data.
///
/// The encoder writes records to a generic `Write` sink and handles the
/// creation and finalization of the metadata header. It is designed for
/// seekable sinks, like files.
pub struct Encoder<W: Write + Seek> {
    writer: W,
    metadata: Metadata,
    record_count: u64,
    start_ts: u64,
    end_ts: u64,
    // The exact size of the metadata section, including padding, calculated once.
    metadata_size: u64,
}

impl<W: Write + Seek> Encoder<W> {
    /// Creates a new `Encoder` that writes to the given seekable sink.
    ///
    /// # Arguments
    ///
    /// *   `writer`: The `Write` and `Seek` enabled sink.
    /// *   `metadata`: The initial metadata configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the placeholder metadata cannot be written to the sink.
    pub fn new(mut writer: W, mut metadata: Metadata) -> Result<Self> {
        // Set constant metadata fields
        metadata.header.magic = QTB_MAGIC;
        metadata.header.version = QTB_VERSION;
        metadata.header.symbol_cstr_len = (SYMBOL_CSTR_LEN as u16).to_le_bytes();
        metadata.header.symbols_count = (metadata.symbols.len() as u32).to_le_bytes();
        metadata.header.partial_count = (metadata.partial.len() as u32).to_le_bytes();
        metadata.header.not_found_count = (metadata.not_found.len() as u32).to_le_bytes();
        metadata.header.mappings_count = (metadata.mappings.len() as u32).to_le_bytes();

        // Calculate the full size of the metadata section to reserve space.
        let var_data_size = (metadata.symbols.len()
            + metadata.partial.len()
            + metadata.not_found.len())
            * SYMBOL_CSTR_LEN
            + metadata
                .mappings
                .iter()
                .fold(0, |acc, (m, i)| acc + mem::size_of_val(m) + i.len() * mem::size_of::<MappingInterval>());

        let unpadded_meta_size = mem::size_of::<MetadataHeader>() + var_data_size;
        // Pad to the next 8-byte boundary to ensure records are 8-byte aligned.
        let padding = (8 - (unpadded_meta_size % 8)) % 8;
        let total_meta_size = unpadded_meta_size + padding;

        // Write a placeholder for the entire metadata section
        let placeholder = vec![0u8; total_meta_size];
        writer.write_all(&placeholder)?;

        Ok(Self {
            writer,
            metadata,
            record_count: 0,
            start_ts: u64::MAX,
            end_ts: 0,
            metadata_size: total_meta_size as u64,
        })
    }

    /// Writes a single record to the stream.
    ///
    /// This method will update the internal state, such as record count and
    /// timestamp range, which will be used to finalize the metadata header.
    ///
    /// # Errors
    ///
    /// Returns an error if the record cannot be written to the sink.
    pub fn write_record<T: Record>(&mut self, record: &T) -> Result<()> {
        let ts = record.header().ts_event();
        if ts > 0 && ts != u64::MAX {
            self.start_ts = self.start_ts.min(ts);
            self.end_ts = self.end_ts.max(ts);
        }
        self.record_count += 1;

        // Get a byte slice of the POD (Plain Old Data) record. This is safe because
        // T implements Record, which requires it to also implement bytemuck::Pod.
        let record_bytes = bytemuck::bytes_of(record);

        self.writer.write_all(record_bytes)?;
        Ok(())
    }

    /// Finalizes the `.qtb` stream.
    ///
    /// This method completes the metadata header with the final record count and
    /// timestamp range, seeks to the beginning of the stream, writes the
    /// finalized header, and then returns the underlying writer.
    ///
    /// # Errors
    ///
    /// Returns an error if seeking or writing the final metadata fails.
    pub fn finalize(mut self) -> Result<W> {
        let final_pos = self.writer.seek(SeekFrom::Current(0))?;

        // Finalize metadata fields
        let start = if self.start_ts == u64::MAX { 0 } else { self.start_ts };
        self.metadata.header.start = start.to_le_bytes();
        self.metadata.header.end = self.end_ts.to_le_bytes();

        // If limit was not set by user, set it to the actual record count.
        if u64::from_le_bytes(self.metadata.header.limit) == 0 {
            self.metadata.header.limit = self.record_count.to_le_bytes();
        }
        // The length field is the size of the metadata section only.
        self.metadata.header.length = (self.metadata_size as u32).to_le_bytes();

        // Seek to the beginning to write the final metadata
        self.writer.seek(SeekFrom::Start(0))?;

        // Write the fixed-size header
        self.writer
            .write_all(bytemuck::bytes_of(&self.metadata.header))?;

        // Write the variable-length metadata sections
        write_string_array(&mut self.writer, &self.metadata.symbols)?;
        write_string_array(&mut self.writer, &self.metadata.partial)?;
        write_string_array(&mut self.writer, &self.metadata.not_found)?;
        write_mappings(&mut self.writer, &self.metadata.mappings)?;

        // Write padding to ensure 8-byte alignment for the record section
        let current_pos = self.writer.seek(SeekFrom::Current(0))?;
        let padding_needed = (self.metadata_size - current_pos) as usize;
        if padding_needed > 0 {
            let padding = vec![0u8; padding_needed];
            self.writer.write_all(&padding)?;
        }

        // Seek back to the end of the file
        self.writer.seek(SeekFrom::Start(final_pos))?;

        Ok(self.writer)
    }
}

fn write_string_array<W: Write>(writer: &mut W, strings: &[String]) -> Result<()> {
    for s in strings {
        let mut buf = [0u8; SYMBOL_CSTR_LEN];
        if s.len() >= SYMBOL_CSTR_LEN {
            return Err(Error::Encode(format!(
                "Symbol '{}' is too long for fixed-size buffer",
                s
            )));
        }
        buf[..s.len()].copy_from_slice(s.as_bytes());
        writer.write_all(&buf)?;
    }
    Ok(())
}

fn write_mappings<W: Write>(
    writer: &mut W,
    mappings: &[(SymbolMapping, Vec<MappingInterval>)],
) -> Result<()> {
    for (mapping, intervals) in mappings {
        writer.write_all(bytemuck::bytes_of(mapping))?;
        for interval in intervals {
            writer.write_all(bytemuck::bytes_of(interval))?;
        }
    }
    Ok(())
}
