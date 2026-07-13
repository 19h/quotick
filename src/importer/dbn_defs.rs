// quotick/src/importer/dbn_defs.rs
//! Contains the `#[repr(C)]` struct definitions that match the binary layout of
//! the Databento Binary Encoding (DBN) format.
//!
//! These definitions are crucial for safe, zero-copy parsing of DBN files.
//! All multi-byte integer fields are represented as little-endian byte arrays,
//! with accessor methods provided to convert them to host-native types.
//! All structs are `bytemuck::Pod`, ensuring they are safe for byte-level
//! reinterpretation.

#![allow(dead_code)] // These are raw definitions, many fields may not be used in conversion

// --- DBN Constants ---
pub const DBN_MAGIC: &[u8; 3] = b"DBN";
pub const METADATA_DATASET_CSTR_LEN: usize = 16;
pub const DBN_SYMBOL_CSTR_LEN_V1: usize = 22;

// --- DBN Enums (values may differ from QTB) ---
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum DbnSchema {
    Mbo = 0,
    Mbp1 = 1,
    Mbp10 = 2,
    Tbbo = 3,
    Trades = 4,
    Ohlcv1S = 5,
    Ohlcv1M = 6,
    Ohlcv1H = 7,
    Ohlcv1D = 8,
    Definition = 9,
    Imbalance = 10,
    Statistics = 11,
    Status = 12,
    Error = 13,
    SymbolMapping = 14,
    System = 15,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DbnRType {
    Mbp1 = 0x01,
    Mbp10 = 0x0A,
    Trade = 0x10,
    Ohlcv1S = 0x12,
    Ohlcv1M = 0x13,
    Ohlcv1H = 0x14,
    Ohlcv1D = 0x15,
    InstrumentDef = 0x20,
    Imbalance = 0x21,
    Status = 0x22,
    SymbolMapping = 0x23,
    System = 0x24,
    Error = 0x25,
    Stat = 0x26,
}

// --- DBN Metadata Structs ---

/// A temporary struct to hold parsed, host-native DBN metadata values.
#[derive(Debug, Clone, Copy)]
pub struct DbnMetadata {
    pub version: u8,
    pub dataset: [u8; METADATA_DATASET_CSTR_LEN],
    pub schema: u16,
    pub start: u64,
    pub end: u64,
    pub limit: u64,
    pub stype_in: u8,
    pub stype_out: u8,
    pub ts_out: u8,
}

/// The initial 8-byte prefix of the DBN metadata section.
#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DbnMetadataPrefix {
    pub magic: [u8; 3],
    pub version: u8,
    pub length: [u8; 4], // LE u32
}

/// The fixed-size portion of the DBN metadata that follows the prefix.
#[repr(C)]
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DbnMetadataFixed {
    pub dataset: [u8; METADATA_DATASET_CSTR_LEN],
    pub schema: [u8; 2],    // LE u16
    pub start: [u8; 8],     // LE u64
    pub end: [u8; 8],       // LE u64
    pub limit: [u8; 8],     // LE u64
    pub stype_in: u8,
    pub stype_out: u8,
    pub ts_out: u8,
}

// --- DBN Record Structs ---

#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DbnRecordHeader {
    pub length: u8,
    pub rtype: u8,
    pub publisher_id: [u8; 2],    // LE u16
    pub instrument_id: [u8; 4], // LE u32
    pub ts_event: [u8; 8],      // LE u64
}

impl DbnRecordHeader {
    pub fn length(&self) -> u8 { self.length }
    pub fn rtype(&self) -> u8 { self.rtype }
    pub fn publisher_id(&self) -> u16 { u16::from_le_bytes(self.publisher_id) }
    pub fn instrument_id(&self) -> u32 { u32::from_le_bytes(self.instrument_id) }
    pub fn ts_event(&self) -> u64 { u64::from_le_bytes(self.ts_event) }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DbnMbpLevel {
    pub price: [u8; 8], // LE i64
    pub size: [u8; 4],  // LE u32
    pub count: [u8; 4], // LE u32
}

impl DbnMbpLevel {
    pub fn price(&self) -> i64 { i64::from_le_bytes(self.price) }
    pub fn size(&self) -> u32 { u32::from_le_bytes(self.size) }
    pub fn count(&self) -> u32 { u32::from_le_bytes(self.count) }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DbnTradeMsg {
    pub hd: DbnRecordHeader,
    pub price: [u8; 8],       // LE i64
    pub size: [u8; 4],        // LE u32
    pub action: u8,
    pub side: u8,
    pub flags: u8,
    pub depth: u8,
    pub ts_recv: [u8; 8],     // LE u64
    pub ts_in_delta: [u8; 4], // LE i32
    pub sequence: [u8; 4],    // LE u32
}

impl DbnTradeMsg {
    pub fn price(&self) -> i64 { i64::from_le_bytes(self.price) }
    pub fn size(&self) -> u32 { u32::from_le_bytes(self.size) }
    pub fn ts_in_delta(&self) -> i32 { i32::from_le_bytes(self.ts_in_delta) }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DbnMbp1Msg {
    pub hd: DbnRecordHeader,
    pub ts_recv: [u8; 8],     // LE u64
    pub action: u8,
    pub side: u8,
    pub flags: u8,
    pub depth: u8,
    pub sequence: [u8; 4],    // LE u32
    pub levels: [DbnMbpLevel; 1],
}

impl DbnMbp1Msg {
    pub fn ts_in_delta(&self) -> i32 {
        (u64::from_le_bytes(self.hd.ts_event) - u64::from_le_bytes(self.ts_recv)) as i32
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DbnMbp10Msg {
    pub hd: DbnRecordHeader,
    pub ts_recv: [u8; 8],     // LE u64
    pub action: u8,
    pub side: u8,
    pub flags: u8,
    pub depth: u8,
    pub sequence: [u8; 4],    // LE u32
    pub levels: [DbnMbpLevel; 10],
}

impl DbnMbp10Msg {
    pub fn ts_in_delta(&self) -> i32 {
        (u64::from_le_bytes(self.hd.ts_event) - u64::from_le_bytes(self.ts_recv)) as i32
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DbnOhlcvMsg {
    pub hd: DbnRecordHeader,
    pub open: [u8; 8],   // LE i64
    pub high: [u8; 8],   // LE i64
    pub low: [u8; 8],    // LE i64
    pub close: [u8; 8],  // LE i64
    pub volume: [u8; 8], // LE u64
}

impl DbnOhlcvMsg {
    pub fn open(&self) -> i64 { i64::from_le_bytes(self.open) }
    pub fn high(&self) -> i64 { i64::from_le_bytes(self.high) }
    pub fn low(&self) -> i64 { i64::from_le_bytes(self.low) }
    pub fn close(&self) -> i64 { i64::from_le_bytes(self.close) }
    pub fn volume(&self) -> u64 { u64::from_le_bytes(self.volume) }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DbnInstrumentDefMsgV1 {
    pub hd: DbnRecordHeader,
    pub ts_recv: [u8; 8],                 // LE u64
    pub min_price_increment: [u8; 8],     // LE i64
    pub display_factor: [u8; 8],          // LE i64
    pub expiration: [u8; 8],              // LE u64
    pub activation: [u8; 8],              // LE u64
    pub high_limit_price: [u8; 8],        // LE i64
    pub low_limit_price: [u8; 8],         // LE i64
    pub max_price_variation: [u8; 8],     // LE i64
    pub trading_reference_price: [u8; 8], // LE i64
    pub unit_of_measure_qty: [u8; 8],     // LE i64
    pub min_trade_vol: [u8; 4],           // LE u32
    pub market_depth: [u8; 4],            // LE i32
    pub price_ratio: [u8; 8],             // LE i64
    pub inst_attrib_value: [u8; 4],       // LE i32
    pub underlying_id: [u8; 4],           // LE u32
    pub market_segment_id: [u8; 4],       // LE u32
    pub settl_price_type: u8,
    pub security_update_action: u8,
    pub min_lot_size: [u8; 4], // LE i32
    pub match_algorithm: u8,
    pub md_security_trading_status: u8,
    pub main_fraction: u8,
    pub price_display_format: u8,
    pub settl_currency: [u8; 4],
    pub sub_fraction: u8,
    pub underlying_product: u8,
    pub security_exchange: [u8; 4],
    pub open_interest_qty: [u8; 4], // LE i32
    pub contract_multiplier: [u8; 4], // LE i32
    pub contract_multiplier_unit: [u8; 4], // LE i32
    pub flow_schedule_type: [u8; 4], // LE i32
    pub min_order_qty: [u8; 4],    // LE i32
    pub user_defined_instrument: u8,
    pub trading_reference_date: [u8; 2], // LE u16
    pub raw_symbol: [u8; DBN_SYMBOL_CSTR_LEN_V1],
}

impl DbnInstrumentDefMsgV1 {
    pub fn min_price_increment(&self) -> i64 { i64::from_le_bytes(self.min_price_increment) }
    pub fn display_factor(&self) -> i64 { i64::from_le_bytes(self.display_factor) }
    pub fn expiration(&self) -> u64 { u64::from_le_bytes(self.expiration) }
    pub fn activation(&self) -> u64 { u64::from_le_bytes(self.activation) }
    pub fn min_lot_size(&self) -> i32 { i32::from_le_bytes(self.min_lot_size) }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, bytemuck::Pod, bytemuck::Zeroable)]
pub struct DbnStatusMsg {
    pub hd: DbnRecordHeader,
    pub ts_recv: [u8; 8], // LE u64
    pub action: u8,
    pub reason: u8,
    pub trading_event: u8,
    pub is_halted: u8,
    pub is_auction: u8,
}
