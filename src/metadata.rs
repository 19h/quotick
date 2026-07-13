// quotick/src/metadata.rs
//! Defines the self-describing metadata header for `.qtb` files.

use std::mem;
use std::str;

use crate::enums::{Schema, SType};
use crate::error::{Error, Result};
use crate::record::SYMBOL_CSTR_LEN;

/// The magic string for identifying QTB files.
pub const QTB_MAGIC: [u8; 3] = *b"QTB";
/// The current version of the QTB format.
pub const QTB_VERSION: u8 = 1;
/// The fixed size of the metadata header in bytes.
pub const METADATA_HEADER_SIZE: usize = 128;

/// A date range for a symbol mapping. On-disk POD representation.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MappingInterval {
    /// The start date of the interval, as a YYYYMMDD integer.
    pub start_date: [u8; 4], // LE u32
    /// The end date of the interval, as a YYYYMMDD integer.
    pub end_date: [u8; 4], // LE u32
    /// The symbol in `stype_out` for this interval.
    pub symbol: [u8; SYMBOL_CSTR_LEN],
}

// Manual impl because `derive` feature is not assumed.
unsafe impl bytemuck::Pod for MappingInterval {}
unsafe impl bytemuck::Zeroable for MappingInterval {}

impl MappingInterval {
    /// Creates a new `MappingInterval`.
    pub fn new(start_date: u32, end_date: u32, symbol: &str) -> Result<Self> {
        let mut symbol_bytes = [0u8; SYMBOL_CSTR_LEN];
        if symbol.len() >= SYMBOL_CSTR_LEN {
            return Err(Error::Encode(format!(
                "Symbol '{}' is too long for fixed-size buffer",
                symbol
            )));
        }
        symbol_bytes[..symbol.len()].copy_from_slice(symbol.as_bytes());
        Ok(Self {
            start_date: start_date.to_le_bytes(),
            end_date: end_date.to_le_bytes(),
            symbol: symbol_bytes,
        })
    }
}

/// A mapping from a raw symbol to an instrument ID. On-disk POD representation.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SymbolMapping {
    /// The raw symbol in `stype_in`.
    pub raw_symbol: [u8; SYMBOL_CSTR_LEN],
    /// The instrument ID.
    pub instrument_id: [u8; 4], // LE u32
    /// The number of intervals in the mapping.
    pub interval_count: [u8; 4], // LE u32
}

// Manual impl because `derive` feature is not assumed.
unsafe impl bytemuck::Pod for SymbolMapping {}
unsafe impl bytemuck::Zeroable for SymbolMapping {}

impl SymbolMapping {
    /// Creates a new `SymbolMapping`.
    pub fn new(raw_symbol: &str, instrument_id: u32, interval_count: u32) -> Result<Self> {
        let mut symbol_bytes = [0u8; SYMBOL_CSTR_LEN];
        if raw_symbol.len() >= SYMBOL_CSTR_LEN {
            return Err(Error::Encode(format!(
                "Symbol '{}' is too long for fixed-size buffer",
                raw_symbol
            )));
        }
        symbol_bytes[..raw_symbol.len()].copy_from_slice(raw_symbol.as_bytes());
        Ok(Self {
            raw_symbol: symbol_bytes,
            instrument_id: instrument_id.to_le_bytes(),
            interval_count: interval_count.to_le_bytes(),
        })
    }
}

/// The fixed-size portion of the metadata header. On-disk POD representation.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct MetadataHeader {
    /// Magic string "QTB" to identify the file type.
    pub(crate) magic: [u8; 3],
    /// The version of the QTB format.
    pub(crate) version: u8,
    /// The total length of the metadata section in bytes, including this header and padding.
    pub(crate) length: [u8; 4], // LE u32
    /// The dataset code (e.g., "GLBX.MDP3").
    pub(crate) dataset: [u8; 16],
    /// The data record schema.
    pub(crate) schema: Schema,
    /// The symbology type of input symbols.
    pub(crate) stype_in: SType,
    /// The symbology type of output symbols.
    pub(crate) stype_out: SType,
    /// Whether each record has an appended gateway send timestamp.
    pub(crate) ts_out: u8,
    /// The number of bytes in fixed-length string symbols.
    pub(crate) symbol_cstr_len: [u8; 2], // LE u16
    /// Reserved for alignment and future use.
    _reserved1: [u8; 2],
    /// The start time of the data range in UNIX epoch nanoseconds.
    pub(crate) start: [u8; 8], // LE u64
    /// The end time of the data range in UNIX epoch nanoseconds. `u64::MAX` for live data.
    pub(crate) end: [u8; 8], // LE u64
    /// The maximum number of records. `0` for no limit.
    pub(crate) limit: [u8; 8], // LE u64
    /// The number of symbols in the original query.
    pub(crate) symbols_count: [u8; 4], // LE u32
    /// The number of symbol mappings.
    pub(crate) mappings_count: [u8; 4], // LE u32
    /// The number of partially resolved symbols.
    pub(crate) partial_count: [u8; 4], // LE u32
    /// The number of unresolved symbols.
    pub(crate) not_found_count: [u8; 4], // LE u32
    /// Reserved for future use.
    pub(crate) _reserved2: [u8; 56],
}

unsafe impl bytemuck::Pod for MetadataHeader {}
unsafe impl bytemuck::Zeroable for MetadataHeader {}

// Compile-time assertion to ensure the header size is fixed.
const _: () = assert!(mem::size_of::<MetadataHeader>() == METADATA_HEADER_SIZE);

impl MetadataHeader {
    pub(crate) fn length(&self) -> u32 { u32::from_le_bytes(self.length) }
    pub(crate) fn schema(&self) -> Schema { self.schema }
    pub(crate) fn start(&self) -> u64 { u64::from_le_bytes(self.start) }
    pub(crate) fn end(&self) -> u64 { u64::from_le_bytes(self.end) }
    pub(crate) fn limit(&self) -> u64 { u64::from_le_bytes(self.limit) }
    pub(crate) fn symbol_cstr_len(&self) -> u16 { u16::from_le_bytes(self.symbol_cstr_len) }
    pub(crate) fn symbols_count(&self) -> u32 { u32::from_le_bytes(self.symbols_count) }
    pub(crate) fn mappings_count(&self) -> u32 { u32::from_le_bytes(self.mappings_count) }
    pub(crate) fn partial_count(&self) -> u32 { u32::from_le_bytes(self.partial_count) }
    pub(crate) fn not_found_count(&self) -> u32 { u32::from_le_bytes(self.not_found_count) }
}

/// Represents the full, parsed metadata from a `.qtb` file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    /// The fixed-size header portion.
    pub(crate) header: MetadataHeader,
    /// The symbols from the original query.
    pub symbols: Vec<String>,
    /// The symbol mappings.
    pub mappings: Vec<(SymbolMapping, Vec<MappingInterval>)>,
    /// Partially resolved symbols.
    pub partial: Vec<String>,
    /// Unresolved symbols.
    pub not_found: Vec<String>,
}

impl Metadata {
    /// Creates a new `Metadata` instance for writing.
    pub fn new(
        dataset: &str,
        schema: Schema,
        stype_in: SType,
        stype_out: SType,
        symbols: Vec<String>,
    ) -> Self {
        let mut dataset_bytes = [0u8; 16];
        let len = dataset.len().min(16);
        dataset_bytes[..len].copy_from_slice(&dataset.as_bytes()[..len]);

        Self {
            header: MetadataHeader {
                magic: [0; 3], // Finalized by Encoder
                version: 0,    // Finalized by Encoder
                length: [0; 4],
                dataset: dataset_bytes,
                schema,
                stype_in,
                stype_out,
                ts_out: 0,
                symbol_cstr_len: [0; 2],
                _reserved1: [0; 2],
                start: [0; 8],
                end: [0; 8],
                limit: [0; 8],
                symbols_count: (symbols.len() as u32).to_le_bytes(),
                mappings_count: [0; 4],
                partial_count: [0; 4],
                not_found_count: [0; 4],
                _reserved2: [0; 56],
            },
            symbols,
            mappings: Vec::new(),
            partial: Vec::new(),
            not_found: Vec::new(),
        }
    }

    /// Returns the dataset code.
    pub fn dataset(&self) -> &str {
        let end = self
            .header
            .dataset
            .iter()
            .position(|&c| c == 0)
            .unwrap_or(self.header.dataset.len());
        str::from_utf8(&self.header.dataset[..end]).unwrap_or("")
    }

    /// Returns the data record schema.
    pub fn schema(&self) -> Schema { self.header.schema() }
    /// Returns the start time of the data range in UNIX epoch nanoseconds.
    pub fn start(&self) -> u64 { self.header.start() }
    /// Returns the end time of the data range in UNIX epoch nanoseconds.
    pub fn end(&self) -> u64 { self.header.end() }
    /// Returns the record limit.
    pub fn limit(&self) -> u64 { self.header.limit() }
    /// Returns the input symbology type.
    pub fn stype_in(&self) -> SType { self.header.stype_in }
    /// Returns the output symbology type.
    pub fn stype_out(&self) -> SType { self.header.stype_out }
    /// Returns true if `ts_out` is enabled.
    pub fn ts_out(&self) -> bool { self.header.ts_out > 0 }
    /// Returns the version of the QTB format.
    pub fn version(&self) -> u8 { self.header.version }

    /// Reads and parses metadata from a byte slice.
    /// Returns the parsed metadata and the total size of the metadata section in bytes.
    pub fn read(buffer: &[u8]) -> Result<(Self, usize)> {
        if buffer.len() < METADATA_HEADER_SIZE {
            return Err(Error::Decode("Buffer too small for metadata header".to_string()));
        }
        let header: &MetadataHeader = bytemuck::from_bytes(&buffer[..METADATA_HEADER_SIZE]);

        if header.magic != QTB_MAGIC {
            return Err(Error::Decode("Invalid magic string".to_string()));
        }
        if header.version != QTB_VERSION {
            return Err(Error::Decode(format!(
                "Unsupported version: got {}, expected {}",
                header.version, QTB_VERSION
            )));
        }
        if header.symbol_cstr_len() as usize != SYMBOL_CSTR_LEN {
            return Err(Error::Decode(format!(
                "Incompatible symbol length: got {}, expected {}",
                header.symbol_cstr_len(),
                SYMBOL_CSTR_LEN
            )));
        }

        let mut cursor = METADATA_HEADER_SIZE;
        let (symbols, bytes_read) = read_string_array(buffer, cursor, header.symbols_count() as usize)?;
        cursor += bytes_read;
        let (partial, bytes_read) = read_string_array(buffer, cursor, header.partial_count() as usize)?;
        cursor += bytes_read;
        let (not_found, bytes_read) = read_string_array(buffer, cursor, header.not_found_count() as usize)?;
        cursor += bytes_read;
        let (mappings, bytes_read) = read_mappings(buffer, cursor, header.mappings_count() as usize)?;
        cursor += bytes_read;

        let total_size = header.length() as usize;
        if buffer.len() < total_size {
            return Err(Error::Decode("Buffer is smaller than metadata length".to_string()));
        }

        Ok((
            Self {
                header: *header,
                symbols,
                mappings,
                partial,
                not_found,
            },
            total_size,
        ))
    }
}

fn read_string_array(buffer: &[u8], offset: usize, count: usize) -> Result<(Vec<String>, usize)> {
    let total_bytes = count * SYMBOL_CSTR_LEN;
    if buffer.len() < offset + total_bytes {
        return Err(Error::Decode("Incomplete string array data".to_string()));
    }
    let mut result = Vec::with_capacity(count);
    let mut current_pos = offset;
    for _ in 0..count {
        let buf = &buffer[current_pos..current_pos + SYMBOL_CSTR_LEN];
        let end = buf.iter().position(|&c| c == 0).unwrap_or(SYMBOL_CSTR_LEN);
        let s = str::from_utf8(&buf[..end])
            .map_err(|e| Error::Decode(format!("Invalid UTF-8 in symbol string: {}", e)))?
            .to_string();
        result.push(s);
        current_pos += SYMBOL_CSTR_LEN;
    }
    Ok((result, total_bytes))
}

fn read_mappings(buffer: &[u8], offset: usize, count: usize) -> Result<(Vec<(SymbolMapping, Vec<MappingInterval>)>, usize)> {
    let mut result = Vec::with_capacity(count);
    let mut cursor = offset;
    for _ in 0..count {
        if buffer.len() < cursor + mem::size_of::<SymbolMapping>() {
            return Err(Error::Decode("Incomplete symbol mapping data".to_string()));
        }
        let mapping: &SymbolMapping = bytemuck::from_bytes(&buffer[cursor..cursor + mem::size_of::<SymbolMapping>()]);
        cursor += mem::size_of::<SymbolMapping>();

        let interval_count = u32::from_le_bytes(mapping.interval_count) as usize;
        let interval_bytes_len = interval_count * mem::size_of::<MappingInterval>();
        if buffer.len() < cursor + interval_bytes_len {
            return Err(Error::Decode("Incomplete mapping interval data".to_string()));
        }
        let intervals: &[MappingInterval] = bytemuck::cast_slice(&buffer[cursor..cursor + interval_bytes_len]);
        cursor += interval_bytes_len;

        result.push((*mapping, intervals.to_vec()));
    }
    Ok((result, cursor - offset))
}
