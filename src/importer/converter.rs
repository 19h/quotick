// quotick/src/importer/converter.rs
//! The core DBN-to-QTB conversion logic.

use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::File;
use std::io::{self};
use std::mem;
use std::path::Path;
use std::str;

use memmap2::Mmap;
use time::OffsetDateTime;

use crate::encode::Encoder;
use crate::enums::{
    Action, InstrumentClass, MatchAlgorithm, RType, Schema, SecurityUpdateAction, Side, SType,
    UserDefinedInstrument,
};
use crate::importer::dbn_defs::*;
use crate::importer::ImportError;
use crate::metadata::{MappingInterval, Metadata, SymbolMapping};
use crate::record::{
    InstrumentDefMsg, Mbp10Msg, Mbp1Msg, MbpLevel, OhlcvMsg, RecordHeader, StatusMsg, TradeMsg,
    SYMBOL_CSTR_LEN,
};

/// A stateless converter utility for DBN-to-QTB conversion.
pub struct Converter;

/// Temporary struct to hold parsed variable-length DBN metadata.
struct DbnVariableMetadata {
    symbols: Vec<String>,
    partial: Vec<String>,
    not_found: Vec<String>,
    // DBN's native mappings are parsed to correctly advance the buffer cursor,
    // but QTB mappings are built from InstrumentDefMsg records instead.
    _dbn_mappings: Vec<(String, Vec<(u32, u32, String)>)>,
}

/// Holds the results of the first pass scan.
type PassOneResult = (
    Vec<(SymbolMapping, Vec<MappingInterval>)>,
    HashMap<u32, String>,
);

impl Converter {
    /// Runs the full two-pass DBN-to-QTB conversion.
    ///
    /// # Arguments
    ///
    /// * `input_path`: The path to the source `.dbn` file.
    /// * `output_path`: The path to the destination `.qtb` file to be created.
    ///
    /// # Errors
    ///
    /// Returns an `ImportError` if any part of the reading, parsing, translation,
    /// or writing process fails.
    pub fn run(input_path: impl AsRef<Path>, output_path: impl AsRef<Path>) -> super::Result<()> {
        let file = File::open(input_path)?;
        // SAFETY: The file is opened read-only, so mapping it is safe.
        let mmap = unsafe { Mmap::map(&file)? };
        let buffer: &[u8] = mmap.as_ref();

        // --- Metadata Parsing ---
        let (dbn_header, dbn_var_meta, record_offset) = Self::parse_dbn_metadata(buffer)?;

        // --- Pass 1: Build Mappings ---
        // Scan the entire record stream to find InstrumentDef messages
        // to build the QTB-style mapping table.
        let (qtb_mappings, _instrument_id_map) =
            Self::build_mappings_pass_one(buffer, record_offset, dbn_header.version)?;

        // --- Metadata Translation ---
        let qtb_meta = Self::translate_metadata(dbn_header, dbn_var_meta, qtb_mappings)?;

        // --- Pass 2: Convert Records ---
        let out_file = File::create(output_path)?;
        let mut encoder = Encoder::new(out_file, qtb_meta)?;

        Self::convert_records_pass_two(buffer, record_offset, dbn_header.version, &mut encoder)?;

        // Finalize the QTB file (writes the completed metadata header)
        encoder.finalize()?;

        Ok(())
    }

    /// Parses the entire DBN metadata header (fixed and variable parts).
    /// Returns the fixed header, a struct of variable data, and the offset to the first record.
    fn parse_dbn_metadata(
        buffer: &[u8],
    ) -> super::Result<(DbnMetadata, DbnVariableMetadata, usize)> {
        let mut cursor = 0;

        // Read prefix
        let prefix: &DbnMetadataPrefix = try_from_bytes(&buffer[cursor..], "DbnMetadataPrefix")?;
        cursor += mem::size_of::<DbnMetadataPrefix>();

        if &prefix.magic != DBN_MAGIC {
            return Err(ImportError::DbnParseError(
                "Invalid DBN magic string".to_string(),
            ));
        }
        let version = prefix.version;
        let metadata_len = u32::from_le_bytes(prefix.length) as usize;
        let record_offset = mem::size_of::<DbnMetadataPrefix>() + metadata_len;

        if buffer.len() < record_offset {
            return Err(ImportError::DbnParseError(
                "Buffer shorter than metadata length field".to_string(),
            ));
        }
        let mut meta_slice: &[u8] = &buffer[cursor..record_offset];

        // Read fixed fields
        let fixed: &DbnMetadataFixed = try_from_bytes(meta_slice, "DbnMetadataFixed")?;
        meta_slice = &meta_slice[mem::size_of::<DbnMetadataFixed>()..];

        // Handle version-dependent layout
        let symbol_cstr_len: usize;
        if version == 1 {
            symbol_cstr_len = DBN_SYMBOL_CSTR_LEN_V1;
            // Skip reserved padding for V1
            meta_slice = &meta_slice[47..];
        } else {
            // V2+
            let cstr_len_bytes: [u8; 2] = meta_slice[..2].try_into().unwrap();
            symbol_cstr_len = u16::from_le_bytes(cstr_len_bytes) as usize;
            meta_slice = &meta_slice[55..]; // 2 bytes for len + 53 reserved
        }

        // Read schema definition (we skip it)
        let schema_def_len = u32::from_le_bytes(meta_slice[..4].try_into().unwrap()) as usize;
        meta_slice = &meta_slice[4 + schema_def_len..];

        // Read variable-length metadata fields
        let (symbols, new_slice) = read_prefixed_str_array(meta_slice, symbol_cstr_len)?;
        meta_slice = new_slice;
        let (partial, new_slice) = read_prefixed_str_array(meta_slice, symbol_cstr_len)?;
        meta_slice = new_slice;
        let (not_found, new_slice) = read_prefixed_str_array(meta_slice, symbol_cstr_len)?;
        meta_slice = new_slice;
        let (dbn_mappings, _) = read_dbn_mappings(meta_slice, symbol_cstr_len)?;

        let header = DbnMetadata {
            version,
            dataset: fixed.dataset,
            schema: u16::from_le_bytes(fixed.schema),
            start: u64::from_le_bytes(fixed.start),
            end: u64::from_le_bytes(fixed.end),
            limit: u64::from_le_bytes(fixed.limit),
            stype_in: fixed.stype_in,
            stype_out: fixed.stype_out,
            ts_out: fixed.ts_out,
        };

        let var_meta = DbnVariableMetadata {
            symbols,
            partial,
            not_found,
            _dbn_mappings: dbn_mappings,
        };

        Ok((header, var_meta, record_offset))
    }

    /// Pass 1: Scan record block for InstrumentDef messages to build mapping table.
    fn build_mappings_pass_one(
        buffer: &[u8],
        record_offset: usize,
        dbn_version: u8,
    ) -> super::Result<PassOneResult> {
        let mut qtb_mappings = Vec::new();
        let mut instrument_id_map = HashMap::new();
        let mut cursor = record_offset;

        while cursor + mem::size_of::<DbnRecordHeader>() <= buffer.len() {
            let header: &DbnRecordHeader = bytemuck::from_bytes(&buffer[cursor..]);
            let record_size = header.length() as usize * 4;
            if record_size == 0 || cursor + record_size > buffer.len() {
                break;
            }

            if header.rtype() == DbnRType::InstrumentDef as u8 {
                let record_bytes = &buffer[cursor..cursor + record_size];
                let (raw_symbol_str, instrument_id, activation_ts, expiration_ts) = match dbn_version {
                    1 => {
                        let msg_v1: &DbnInstrumentDefMsgV1 =
                            try_from_bytes(record_bytes, "DbnInstrumentDefMsgV1")?;
                        (
                            parse_cstr(&msg_v1.raw_symbol)?,
                            msg_v1.hd.instrument_id(),
                            msg_v1.activation(),
                            msg_v1.expiration(),
                        )
                    }
                    // Add cases for v2, v3 etc. here if supported
                    _ => {
                        return Err(ImportError::UnsupportedVersion(format!(
                            "DBN version {} for InstrumentDef is not supported",
                            dbn_version
                        )))
                    }
                };

                let qtb_map = SymbolMapping::new(&raw_symbol_str, instrument_id, 1)?;
                let qtb_interval = MappingInterval::new(
                    ts_to_date(activation_ts),
                    ts_to_date(expiration_ts),
                    &raw_symbol_str,
                )?;

                qtb_mappings.push((qtb_map, vec![qtb_interval]));
                instrument_id_map.insert(instrument_id, raw_symbol_str);
            }

            cursor += record_size;
        }

        Ok((qtb_mappings, instrument_id_map))
    }

    /// Translates the parsed DBN metadata into the QTB Metadata struct.
    fn translate_metadata(
        dbn_header: DbnMetadata,
        dbn_var_meta: DbnVariableMetadata,
        qtb_mappings: Vec<(SymbolMapping, Vec<MappingInterval>)>,
    ) -> super::Result<Metadata> {
        let qtb_schema = map_dbn_schema(dbn_header.schema)?;
        let qtb_stype_in = map_dbn_stype(dbn_header.stype_in)?;
        let qtb_stype_out = map_dbn_stype(dbn_header.stype_out)?;

        let dataset_str = parse_cstr(&dbn_header.dataset)?;

        let mut qtb_meta = Metadata::new(
            &dataset_str,
            qtb_schema,
            qtb_stype_in,
            qtb_stype_out,
            dbn_var_meta.symbols,
        );

        qtb_meta.header.start = dbn_header.start.to_le_bytes();
        qtb_meta.header.end = dbn_header.end.to_le_bytes();
        qtb_meta.header.limit = dbn_header.limit.to_le_bytes();
        qtb_meta.header.ts_out = dbn_header.ts_out;

        qtb_meta.partial = dbn_var_meta.partial;
        qtb_meta.not_found = dbn_var_meta.not_found;
        qtb_meta.mappings = qtb_mappings;

        Ok(qtb_meta)
    }

    /// Pass 2: Scan record block, translate all records, and write to QTB Encoder.
    fn convert_records_pass_two(
        buffer: &[u8],
        record_offset: usize,
        dbn_version: u8,
        encoder: &mut Encoder<File>,
    ) -> super::Result<()> {
        let mut cursor = record_offset;

        while cursor + mem::size_of::<DbnRecordHeader>() <= buffer.len() {
            let header: &DbnRecordHeader = bytemuck::from_bytes(&buffer[cursor..]);
            let record_size = header.length() as usize * 4;
            if record_size == 0 || cursor + record_size > buffer.len() {
                break;
            }
            let record_bytes = &buffer[cursor..cursor + record_size];

            match header.rtype() {
                rtype if rtype == DbnRType::Trade as u8 => {
                    let dbn_rec: &DbnTradeMsg = try_from_bytes(record_bytes, "DbnTradeMsg")?;
                    let qtb_rec = Self::translate_trade(dbn_rec)?;
                    encoder.write_record(&qtb_rec)?;
                }
                rtype if rtype == DbnRType::Mbp1 as u8 => {
                    let dbn_rec: &DbnMbp1Msg = try_from_bytes(record_bytes, "DbnMbp1Msg")?;
                    let qtb_rec = Self::translate_mbp1(dbn_rec)?;
                    encoder.write_record(&qtb_rec)?;
                }
                rtype if rtype == DbnRType::Mbp10 as u8 => {
                    let dbn_rec: &DbnMbp10Msg = try_from_bytes(record_bytes, "DbnMbp10Msg")?;
                    let qtb_rec = Self::translate_mbp10(dbn_rec)?;
                    encoder.write_record(&qtb_rec)?;
                }
                rtype if rtype == DbnRType::Ohlcv1S as u8
                    || rtype == DbnRType::Ohlcv1M as u8
                    || rtype == DbnRType::Ohlcv1H as u8
                    || rtype == DbnRType::Ohlcv1D as u8 =>
                {
                    let dbn_rec: &DbnOhlcvMsg = try_from_bytes(record_bytes, "DbnOhlcvMsg")?;
                    let qtb_rec = Self::translate_ohlcv(dbn_rec)?;
                    encoder.write_record(&qtb_rec)?;
                }
                rtype if rtype == DbnRType::InstrumentDef as u8 => {
                    let qtb_rec = match dbn_version {
                        1 => Self::translate_instrument_def_v1(try_from_bytes(
                            record_bytes,
                            "DbnInstrumentDefMsgV1",
                        )?)?,
                        _ => continue, // Already handled in pass one
                    };
                    encoder.write_record(&qtb_rec)?;
                }
                rtype if rtype == DbnRType::Status as u8 => {
                    let dbn_rec: &DbnStatusMsg = try_from_bytes(record_bytes, "DbnStatusMsg")?;
                    let qtb_rec = Self::translate_status(dbn_rec)?;
                    encoder.write_record(&qtb_rec)?;
                }
                _ => {} // Skip unsupported rtypes
            }
            cursor += record_size;
        }
        Ok(())
    }

    // --- Record Translation Functions ---

    fn translate_trade(dbn: &DbnTradeMsg) -> super::Result<TradeMsg> {
        Ok(TradeMsg::new(
            dbn.hd.publisher_id(),
            dbn.hd.instrument_id(),
            dbn.hd.ts_event(),
            checked_price_cast(dbn.price())?,
            dbn.size(),
            Action::Trade,
            map_side(dbn.side),
            dbn.flags,
            dbn.depth,
            0, // channel_id not present in DBN TradeMsg
            dbn.ts_in_delta(),
        ))
    }

    fn translate_mbp1(dbn: &DbnMbp1Msg) -> super::Result<Mbp1Msg> {
        let mut levels = [MbpLevel::default(); 2];
        let side = map_side(dbn.side);
        let level = MbpLevel::new(
            checked_price_cast(dbn.levels[0].price())?,
            dbn.levels[0].size(),
            dbn.levels[0].count(),
        );
        if side == Side::Bid {
            levels[0] = level;
        } else {
            levels[1] = level;
        }

        let header = Self::translate_header(&dbn.hd, RType::Mbp1)?;
        Ok(Mbp1Msg {
            hd: header,
            levels,
            action: map_action(dbn.action),
            side,
            flags: dbn.flags,
            depth: dbn.depth,
            channel_id: 0u16.to_le_bytes(),
            ts_in_delta: dbn.ts_in_delta().to_le_bytes(),
        })
    }

    fn translate_mbp10(dbn: &DbnMbp10Msg) -> super::Result<Mbp10Msg> {
        let mut qtb_levels = [MbpLevel::default(); 20];
        let side = map_side(dbn.side);
        let start_index = if side == Side::Bid { 0 } else { 10 };
        for i in 0..10 {
            qtb_levels[start_index + i] = MbpLevel::new(
                checked_price_cast(dbn.levels[i].price())?,
                dbn.levels[i].size(),
                dbn.levels[i].count(),
            );
        }

        let header = Self::translate_header(&dbn.hd, RType::Mbp10)?;
        Ok(Mbp10Msg {
            hd: header,
            levels: qtb_levels,
            action: map_action(dbn.action),
            side,
            flags: dbn.flags,
            depth: dbn.depth,
            channel_id: 0u16.to_le_bytes(),
            ts_in_delta: dbn.ts_in_delta().to_le_bytes(),
            _reserved: [0; 4],
        })
    }

    fn translate_ohlcv(dbn: &DbnOhlcvMsg) -> super::Result<OhlcvMsg> {
        Ok(OhlcvMsg::new(
            dbn.hd.publisher_id(),
            dbn.hd.instrument_id(),
            dbn.hd.ts_event(),
            checked_price_cast(dbn.open())?,
            checked_price_cast(dbn.high())?,
            checked_price_cast(dbn.low())?,
            checked_price_cast(dbn.close())?,
            dbn.volume(),
        ))
    }

    fn translate_instrument_def_v1(
        dbn: &DbnInstrumentDefMsgV1,
    ) -> super::Result<InstrumentDefMsg> {
        let header = Self::translate_header(&dbn.hd, RType::InstrumentDef)?;
        Ok(InstrumentDefMsg {
            hd: header,
            raw_symbol: pad_qtb_symbol(&parse_cstr(&dbn.raw_symbol)?)?,
            instrument_class: map_instrument_class(dbn.underlying_product),
            match_algorithm: map_match_algo(dbn.match_algorithm),
            currency: dbn.settl_currency,
            asset: [0; 12],
            min_price_increment: checked_price_cast(dbn.min_price_increment())?.to_le_bytes(),
            display_factor: checked_price_cast(dbn.display_factor())?.to_le_bytes(),
            min_lot_size: (dbn.min_lot_size() as u32).to_le_bytes(),
            max_lot_size: 0u32.to_le_bytes(),
            activation_ts: dbn.activation().to_le_bytes(),
            expiration_ts: dbn.expiration().to_le_bytes(),
            security_update_action: map_sec_update_action(dbn.security_update_action),
            user_defined_instrument: map_user_defined(dbn.user_defined_instrument),
            _reserved: [0; 2],
        })
    }

    fn translate_status(dbn: &DbnStatusMsg) -> super::Result<StatusMsg> {
        let header = Self::translate_header(&dbn.hd, RType::Status)?;
        Ok(StatusMsg {
            hd: header,
            group: [0; 20],
            trading_status: 0u16.to_le_bytes(),
            halt_reason: (dbn.reason as u16).to_le_bytes(),
            trading_event: (dbn.trading_event as u16).to_le_bytes(),
            _reserved: [0; 4],
        })
    }

    fn translate_header(dbn_hd: &DbnRecordHeader, qtb_rtype: RType) -> super::Result<RecordHeader> {
        let qtb_size = qtb_record_size(qtb_rtype)?;
        Ok(RecordHeader::new(
            qtb_size,
            qtb_rtype,
            dbn_hd.publisher_id(),
            dbn_hd.instrument_id(),
            dbn_hd.ts_event(),
        ))
    }
}

// --- Enum/Type Mapping & Utility Helpers ---

fn map_dbn_schema(dbn_schema: u16) -> super::Result<Schema> {
    match dbn_schema {
        s if s == DbnSchema::Mbp1 as u16 => Ok(Schema::Mbp1),
        s if s == DbnSchema::Mbp10 as u16 => Ok(Schema::Mbp10),
        s if s == DbnSchema::Trades as u16 => Ok(Schema::Trade),
        s if s == DbnSchema::Ohlcv1S as u16
            || s == DbnSchema::Ohlcv1M as u16
            || s == DbnSchema::Ohlcv1H as u16
            || s == DbnSchema::Ohlcv1D as u16 =>
        {
            Ok(Schema::Ohlcv)
        }
        s if s == DbnSchema::Definition as u16 => Ok(Schema::Definition),
        s if s == DbnSchema::Status as u16 => Ok(Schema::Status),
        s if s == DbnSchema::Statistics as u16 => Ok(Schema::Statistics),
        _ => Err(ImportError::UnsupportedSchema(format!(
            "DBN schema code {} is not supported",
            dbn_schema
        ))),
    }
}

fn map_dbn_stype(dbn_stype: u8) -> super::Result<SType> {
    match dbn_stype {
        0 => Ok(SType::RawSymbol),
        1 => Ok(SType::InstrumentId),
        2 => Ok(SType::Smart),
        std::u8::MAX => Ok(SType::Mixed),
        _ => Err(ImportError::DbnParseError(format!(
            "Unknown DBN SType: {}",
            dbn_stype
        ))),
    }
}

fn map_side(dbn_side: u8) -> Side {
    match dbn_side {
        b'A' => Side::Ask,
        b'B' => Side::Bid,
        _ => Side::None,
    }
}

fn map_action(dbn_action: u8) -> Action {
    match dbn_action {
        b'A' => Action::Add,
        b'M' => Action::Modify,
        b'D' => Action::Delete,
        b'T' => Action::Trade,
        b'F' => Action::Fill,
        _ => Action::Modify,
    }
}

fn map_instrument_class(dbn_product: u8) -> InstrumentClass {
    match dbn_product {
        b'S' => InstrumentClass::Stock,
        b'B' => InstrumentClass::Bond,
        b'C' => InstrumentClass::Call,
        b'P' => InstrumentClass::Put,
        b'F' => InstrumentClass::Future,
        _ => InstrumentClass::Combo,
    }
}

fn map_match_algo(dbn_algo: u8) -> MatchAlgorithm {
    match dbn_algo {
        b'F' => MatchAlgorithm::Fifo,
        b'P' => MatchAlgorithm::ProRata,
        b'C' => MatchAlgorithm::Configurable,
        _ => MatchAlgorithm::None,
    }
}

fn map_sec_update_action(dbn_action: u8) -> SecurityUpdateAction {
    match dbn_action {
        b'A' => SecurityUpdateAction::Add,
        b'D' => SecurityUpdateAction::Delete,
        b'M' => SecurityUpdateAction::Modify,
        _ => SecurityUpdateAction::Modify,
    }
}

fn map_user_defined(dbn_user: u8) -> UserDefinedInstrument {
    match dbn_user {
        b'Y' => UserDefinedInstrument::Yes,
        _ => UserDefinedInstrument::No,
    }
}

fn qtb_record_size(rtype: RType) -> super::Result<usize> {
    match rtype {
        RType::Mbp1 => Ok(mem::size_of::<Mbp1Msg>()),
        RType::Mbp10 => Ok(mem::size_of::<Mbp10Msg>()),
        RType::Trade => Ok(mem::size_of::<TradeMsg>()),
        RType::Ohlcv => Ok(mem::size_of::<OhlcvMsg>()),
        RType::InstrumentDef => Ok(mem::size_of::<InstrumentDefMsg>()),
        RType::Status => Ok(mem::size_of::<StatusMsg>()),
        _ => Err(ImportError::UnsupportedSchema(format!(
            "QTB RType {:?} has no defined size",
            rtype
        ))),
    }
}

fn try_from_bytes<'a, T: bytemuck::Pod>(
    bytes: &'a [u8],
    type_name: &str,
) -> super::Result<&'a T> {
    bytemuck::try_from_bytes(bytes).map_err(|e| {
        ImportError::DbnParseError(format!("Failed to cast bytes to {}: {}", type_name, e))
    })
}

fn read_fixed_str(reader: &mut &[u8], len: usize) -> super::Result<String> {
    if reader.len() < len {
        return Err(ImportError::Io(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Incomplete string data",
        )));
    }
    let buf = &reader[..len];
    *reader = &reader[len..];
    let end = buf.iter().position(|&b| b == 0).unwrap_or(len);
    Ok(
        str::from_utf8(&buf[..end])
            .map_err(|e| ImportError::DbnParseError(format!("Invalid UTF-8 string: {}", e)))?
            .to_string(),
    )
}

fn read_prefixed_str_array<'a>(
    mut reader: &'a [u8],
    len: usize,
) -> super::Result<(Vec<String>, &'a [u8])> {
    let count = u32::from_le_bytes(reader[..4].try_into().unwrap()) as usize;
    reader = &reader[4..];
    let mut vec = Vec::with_capacity(count);
    for _ in 0..count {
        vec.push(read_fixed_str(&mut reader, len)?);
    }
    Ok((vec, reader))
}

fn read_dbn_mappings<'a>(
    mut reader: &'a [u8],
    len: usize,
) -> super::Result<(Vec<(String, Vec<(u32, u32, String)>)>, &'a [u8])> {
    let count = u32::from_le_bytes(reader[..4].try_into().unwrap()) as usize;
    reader = &reader[4..];
    let mut mappings = Vec::with_capacity(count);
    for _ in 0..count {
        let raw_symbol = read_fixed_str(&mut reader, len)?;
        let interval_count = u32::from_le_bytes(reader[..4].try_into().unwrap()) as usize;
        reader = &reader[4..];
        let mut intervals = Vec::with_capacity(interval_count);
        for _ in 0..interval_count {
            let start_date = u32::from_le_bytes(reader[..4].try_into().unwrap());
            reader = &reader[4..];
            let end_date = u32::from_le_bytes(reader[..4].try_into().unwrap());
            reader = &reader[4..];
            let symbol = read_fixed_str(&mut reader, len)?;
            intervals.push((start_date, end_date, symbol));
        }
        mappings.push((raw_symbol, intervals));
    }
    Ok((mappings, reader))
}

fn parse_cstr<const N: usize>(bytes: &[u8; N]) -> super::Result<String> {
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(N);
    Ok(
        str::from_utf8(&bytes[..end])
            .map_err(|e| ImportError::DbnParseError(format!("Invalid UTF-8 string: {}", e)))?
            .to_string(),
    )
}

fn pad_qtb_symbol(s: &str) -> super::Result<[u8; SYMBOL_CSTR_LEN]> {
    let bytes = s.as_bytes();
    if bytes.len() > SYMBOL_CSTR_LEN {
        return Err(ImportError::QtbEncodeError(crate::Error::Encode(
            format!("Symbol '{}' is too long for QTB", s),
        )));
    }
    let mut buf = [0u8; SYMBOL_CSTR_LEN];
    buf[..bytes.len()].copy_from_slice(bytes);
    Ok(buf)
}

fn checked_price_cast(dbn_price: i64) -> super::Result<i32> {
    dbn_price.try_into().map_err(|_| {
        ImportError::DbnParseError(format!(
            "Price overflow: DBN price {} is out of range for QTB i32",
            dbn_price
        ))
    })
}

/// Converts a nanosecond epoch timestamp to a YYYYMMDD integer.
/// Returns 0 if the timestamp is 0.
fn ts_to_date(ts: u64) -> u32 {
    if ts == 0 {
        return 0;
    }
    // `from_unix_timestamp_nanos` can panic on overflow, but DBN timestamps
    // are well within the valid range.
    let dt = OffsetDateTime::from_unix_timestamp_nanos(ts as i128).unwrap();
    let date = dt.date();
    (date.year() as u32 * 10000) + (date.month() as u32 * 100) + (date.day() as u32)
}
