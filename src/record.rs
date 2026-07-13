// quotick/src/record.rs
//! Defines the `#[repr(C)]` structs for all record types in the `qtb` format.
//!
//! These structs represent the **on-disk** format. All multi-byte numeric
//! fields are stored as little-endian byte arrays.
//!
//! For each struct, two APIs are provided:
//! 1.  **Accessors** (e.g., `price()`) for reading data from a zero-copy view.
//!     These convert the LE bytes to a host-native integer.
//! 2.  A **`new()` constructor** for writing data. This takes host-native
//!     integers and converts them to the required LE byte representation.
//!
//! All record structs are guaranteed to have a size that is a multiple of 8 bytes
//! to ensure 8-byte alignment throughout the record stream.

use crate::enums::{
    Action, InstrumentClass, MatchAlgorithm, RType, SecurityUpdateAction, Side, StatType,
    StatUpdateAction, SType, UserDefinedInstrument,
};
use crate::{
    cstr_to_str, custom_debug_record_header, impl_eq_record, impl_record,
};
use std::mem;

/// The fixed-length of symbol strings, including a null terminator.
/// Chosen to be a multiple of 8 for alignment purposes.
pub const SYMBOL_CSTR_LEN: usize = 72;

/// The common header for all records.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct RecordHeader {
    /// The length of the record in 8-byte words.
    pub length: u8,
    /// The record type (`RType`).
    pub rtype: u8,
    /// The publisher ID assigned by the data source.
    pub publisher_id: [u8; 2], // LE u16
    /// The numeric instrument ID.
    pub instrument_id: [u8; 4], // LE u32
    /// The event timestamp in nanoseconds since the UNIX epoch.
    pub ts_event: [u8; 8], // LE u64
}

// All records must be POD (Plain Old Data) for safe, zero-copy casting.
unsafe impl bytemuck::Pod for RecordHeader {}
unsafe impl bytemuck::Zeroable for RecordHeader {}

const _: () = {
    assert!(mem::size_of::<RecordHeader>() == 16);
    assert!(mem::align_of::<RecordHeader>() == 8);
};

impl RecordHeader {
    /// Creates a new `RecordHeader`.
    pub fn new(
        record_size: usize,
        rtype: RType,
        publisher_id: u16,
        instrument_id: u32,
        ts_event: u64,
    ) -> Self {
        debug_assert!(record_size % 8 == 0, "Record size must be a multiple of 8");
        Self {
            length: (record_size / 8) as u8,
            rtype: rtype as u8,
            publisher_id: publisher_id.to_le_bytes(),
            instrument_id: instrument_id.to_le_bytes(),
            ts_event: ts_event.to_le_bytes(),
        }
    }

    /// Returns the length of the record in 8-byte words.
    pub fn length(&self) -> u8 { self.length }
    /// Returns the raw record type identifier.
    pub fn rtype(&self) -> u8 { self.rtype }
    /// Returns the publisher ID.
    pub fn publisher_id(&self) -> u16 { u16::from_le_bytes(self.publisher_id) }
    /// Returns the numeric instrument ID.
    pub fn instrument_id(&self) -> u32 { u32::from_le_bytes(self.instrument_id) }
    /// Returns the event timestamp in nanoseconds since the UNIX epoch.
    pub fn ts_event(&self) -> u64 { u64::from_le_bytes(self.ts_event) }
}

// A byte-wise comparison is correct and efficient for POD structs.
impl PartialEq for RecordHeader {
    fn eq(&self, other: &Self) -> bool {
        bytemuck::bytes_of(self) == bytemuck::bytes_of(other)
    }
}
impl Eq for RecordHeader {}
custom_debug_record_header!(RecordHeader);

/// A trait for accessing common fields of a record.
/// This trait requires `bytemuck::Pod` to ensure safety.
pub trait Record: bytemuck::Pod {
    /// The record's type identifier, as a constant.
    const RTYPE: RType;

    /// Returns a reference to the record's header.
    fn header(&self) -> &RecordHeader;
}

/// A price level in a market-by-price book.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MbpLevel {
    /// The price of the level.
    pub price: [u8; 4], // LE i32
    /// The total volume at the level.
    pub size: [u8; 4], // LE u32
    /// The number of orders at the level.
    pub count: [u8; 4], // LE u32
    /// Reserved for alignment.
    _reserved: [u8; 4],
}

unsafe impl bytemuck::Pod for MbpLevel {}
unsafe impl bytemuck::Zeroable for MbpLevel {}
const _: () = {
    assert!(mem::size_of::<MbpLevel>() == 16);
    assert!(mem::align_of::<MbpLevel>() == 4);
};

impl MbpLevel {
    /// Creates a new `MbpLevel`.
    pub fn new(price: i32, size: u32, count: u32) -> Self {
        Self {
            price: price.to_le_bytes(),
            size: size.to_le_bytes(),
            count: count.to_le_bytes(),
            _reserved: [0; 4],
        }
    }
    /// The price of the level.
    pub fn price(&self) -> i32 { i32::from_le_bytes(self.price) }
    /// The total volume at the level.
    pub fn size(&self) -> u32 { u32::from_le_bytes(self.size) }
    /// The number of orders at the level.
    pub fn count(&self) -> u32 { u32::from_le_bytes(self.count) }
}

impl Default for MbpLevel {
    fn default() -> Self { Self::new(0, 0, 0) }
}
impl PartialEq for MbpLevel {
    fn eq(&self, other: &Self) -> bool { bytemuck::bytes_of(self) == bytemuck::bytes_of(other) }
}
impl Eq for MbpLevel {}
impl std::fmt::Debug for MbpLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MbpLevel")
            .field("price", &self.price())
            .field("size", &self.size())
            .field("count", &self.count())
            .finish()
    }
}

/// A trade message.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct TradeMsg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The trade price, as a fixed-point integer.
    pub price: [u8; 4], // LE i32
    /// The trade size.
    pub size: [u8; 4], // LE u32
    /// The action that triggered the trade.
    pub action: Action,
    /// The side of the aggressive order.
    pub side: Side,
    /// Trade flags.
    pub flags: u8,
    /// The depth of the book at the time of the trade.
    pub depth: u8,
    /// The channel ID of the trade.
    pub channel_id: [u8; 2], // LE u16
    /// The time delta between the event and the gateway ingress, in nanoseconds.
    pub ts_in_delta: [u8; 4], // LE i32
    _reserved: [u8; 2],
}
impl_record!(TradeMsg, RType::Trade);
const _: () = {
    assert!(mem::size_of::<TradeMsg>() == 40);
    assert!(mem::align_of::<TradeMsg>() == 8);
};

impl TradeMsg {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        publisher_id: u16,
        instrument_id: u32,
        ts_event: u64,
        price: i32,
        size: u32,
        action: Action,
        side: Side,
        flags: u8,
        depth: u8,
        channel_id: u16,
        ts_in_delta: i32,
    ) -> Self {
        Self {
            hd: RecordHeader::new(
                mem::size_of::<Self>(),
                RType::Trade,
                publisher_id,
                instrument_id,
                ts_event,
            ),
            price: price.to_le_bytes(),
            size: size.to_le_bytes(),
            action,
            side,
            flags,
            depth,
            channel_id: channel_id.to_le_bytes(),
            ts_in_delta: ts_in_delta.to_le_bytes(),
            _reserved: [0; 2],
        }
    }
    pub fn price(&self) -> i32 { i32::from_le_bytes(self.price) }
    pub fn size(&self) -> u32 { u32::from_le_bytes(self.size) }
    pub fn action(&self) -> Action { self.action }
    pub fn side(&self) -> Side { self.side }
    pub fn flags(&self) -> u8 { self.flags }
    pub fn depth(&self) -> u8 { self.depth }
    pub fn channel_id(&self) -> u16 { u16::from_le_bytes(self.channel_id) }
    pub fn ts_in_delta(&self) -> i32 { i32::from_le_bytes(self.ts_in_delta) }
}
impl std::fmt::Debug for TradeMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TradeMsg")
            .field("hd", self.header())
            .field("price", &self.price())
            .field("size", &self.size())
            .field("action", &self.action())
            .field("side", &self.side())
            .field("flags", &self.flags())
            .field("depth", &self.depth())
            .field("channel_id", &self.channel_id())
            .field("ts_in_delta", &self.ts_in_delta())
            .finish()
    }
}
impl_eq_record!(TradeMsg, price, size, action, side, flags, depth, channel_id, ts_in_delta);

/// A market-by-price message with 1 level of depth.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct Mbp1Msg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The best bid and ask levels.
    pub levels: [MbpLevel; 2], // [bid, ask]
    /// The action that triggered the update.
    pub action: Action,
    /// The side of the update.
    pub side: Side,
    /// Message flags.
    pub flags: u8,
    /// The depth of the book.
    pub depth: u8,
    /// The channel ID of the update.
    pub channel_id: [u8; 2], // LE u16
    /// The time delta between the event and the gateway ingress, in nanoseconds.
    pub ts_in_delta: [u8; 4], // LE i32
}
impl_record!(Mbp1Msg, RType::Mbp1);
const _: () = {
    assert!(mem::size_of::<Mbp1Msg>() == 64);
    assert!(mem::align_of::<Mbp1Msg>() == 8);
};

impl Mbp1Msg {
    pub fn action(&self) -> Action { self.action }
    pub fn side(&self) -> Side { self.side }
    pub fn flags(&self) -> u8 { self.flags }
    pub fn depth(&self) -> u8 { self.depth }
    pub fn channel_id(&self) -> u16 { u16::from_le_bytes(self.channel_id) }
    pub fn ts_in_delta(&self) -> i32 { i32::from_le_bytes(self.ts_in_delta) }
}
impl std::fmt::Debug for Mbp1Msg {
     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mbp1Msg")
            .field("hd", self.header())
            .field("levels", &self.levels)
            .field("action", &self.action())
            .field("side", &self.side())
            .field("flags", &self.flags())
            .field("depth", &self.depth())
            .field("channel_id", &self.channel_id())
            .field("ts_in_delta", &self.ts_in_delta())
            .finish()
    }
}
impl PartialEq for Mbp1Msg {
    fn eq(&self, other: &Self) -> bool {
        self.header() == other.header() &&
        self.levels == other.levels &&
        self.action() == other.action() &&
        self.side() == other.side() &&
        self.flags() == other.flags() &&
        self.depth() == other.depth() &&
        self.channel_id() == other.channel_id() &&
        self.ts_in_delta() == other.ts_in_delta()
    }
}
impl Eq for Mbp1Msg {}

/// A market-by-price message with 10 levels of depth.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct Mbp10Msg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The top 10 bid and ask levels.
    pub levels: [MbpLevel; 20], // [bid_0..9, ask_0..9]
    /// The action that triggered the update.
    pub action: Action,
    /// The side of the update.
    pub side: Side,
    /// Message flags.
    pub flags: u8,
    /// The depth of the book.
    pub depth: u8,
    /// The channel ID of the update.
    pub channel_id: [u8; 2], // LE u16
    /// The time delta between the event and the gateway ingress, in nanoseconds.
    pub ts_in_delta: [u8; 4], // LE i32
    _reserved: [u8; 4],
}
impl_record!(Mbp10Msg, RType::Mbp10);
const _: () = {
    assert!(mem::size_of::<Mbp10Msg>() == 360);
    assert!(mem::align_of::<Mbp10Msg>() == 8);
};

impl Mbp10Msg {
    pub fn action(&self) -> Action { self.action }
    pub fn side(&self) -> Side { self.side }
    pub fn flags(&self) -> u8 { self.flags }
    pub fn depth(&self) -> u8 { self.depth }
    pub fn channel_id(&self) -> u16 { u16::from_le_bytes(self.channel_id) }
    pub fn ts_in_delta(&self) -> i32 { i32::from_le_bytes(self.ts_in_delta) }
}
impl std::fmt::Debug for Mbp10Msg {
     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mbp10Msg")
            .field("hd", self.header())
            .field("levels", &self.levels)
            .field("action", &self.action())
            .field("side", &self.side())
            .field("flags", &self.flags())
            .field("depth", &self.depth())
            .field("channel_id", &self.channel_id())
            .field("ts_in_delta", &self.ts_in_delta())
            .finish()
    }
}
impl PartialEq for Mbp10Msg {
    fn eq(&self, other: &Self) -> bool {
        self.header() == other.header() &&
        self.levels == other.levels &&
        self.action() == other.action() &&
        self.side() == other.side() &&
        self.flags() == other.flags() &&
        self.depth() == other.depth() &&
        self.channel_id() == other.channel_id() &&
        self.ts_in_delta() == other.ts_in_delta()
    }
}
impl Eq for Mbp10Msg {}

/// An open, high, low, close, and volume message.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct OhlcvMsg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The open price for the bar.
    pub open: [u8; 4], // LE i32
    /// The high price for the bar.
    pub high: [u8; 4], // LE i32
    /// The low price for the bar.
    pub low: [u8; 4], // LE i32
    /// The close price for the bar.
    pub close: [u8; 4], // LE i32
    /// The total volume for the bar.
    pub volume: [u8; 8], // LE u64
}
impl_record!(OhlcvMsg, RType::Ohlcv);
const _: () = {
    assert!(mem::size_of::<OhlcvMsg>() == 48);
    assert!(mem::align_of::<OhlcvMsg>() == 8);
};

impl OhlcvMsg {
    pub fn new(
        publisher_id: u16,
        instrument_id: u32,
        ts_event: u64,
        open: i32,
        high: i32,
        low: i32,
        close: i32,
        volume: u64,
    ) -> Self {
        Self {
            hd: RecordHeader::new(
                mem::size_of::<Self>(),
                RType::Ohlcv,
                publisher_id,
                instrument_id,
                ts_event,
            ),
            open: open.to_le_bytes(),
            high: high.to_le_bytes(),
            low: low.to_le_bytes(),
            close: close.to_le_bytes(),
            volume: volume.to_le_bytes(),
        }
    }
    pub fn open(&self) -> i32 { i32::from_le_bytes(self.open) }
    pub fn high(&self) -> i32 { i32::from_le_bytes(self.high) }
    pub fn low(&self) -> i32 { i32::from_le_bytes(self.low) }
    pub fn close(&self) -> i32 { i32::from_le_bytes(self.close) }
    pub fn volume(&self) -> u64 { u64::from_le_bytes(self.volume) }
}
impl std::fmt::Debug for OhlcvMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OhlcvMsg")
            .field("hd", self.header())
            .field("open", &self.open())
            .field("high", &self.high())
            .field("low", &self.low())
            .field("close", &self.close())
            .field("volume", &self.volume())
            .finish()
    }
}
impl_eq_record!(OhlcvMsg, open, high, low, close, volume);

/// An instrument definition message.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct InstrumentDefMsg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The raw symbol from the publisher.
    pub raw_symbol: [u8; SYMBOL_CSTR_LEN],
    /// The class of the instrument.
    pub instrument_class: InstrumentClass,
    /// The matching algorithm used for the instrument.
    pub match_algorithm: MatchAlgorithm,
    /// The currency of the instrument.
    pub currency: [u8; 4],
    /// The asset class of the instrument.
    pub asset: [u8; 12],
    /// The minimum price increment.
    pub min_price_increment: [u8; 4], // LE i32
    /// The display factor for the price.
    pub display_factor: [u8; 4], // LE i32
    /// The minimum lot size.
    pub min_lot_size: [u8; 4], // LE u32
    /// The maximum lot size.
    pub max_lot_size: [u8; 4], // LE u32
    /// The activation timestamp in nanoseconds since the UNIX epoch.
    pub activation_ts: [u8; 8], // LE u64
    /// The expiration timestamp in nanoseconds since the UNIX epoch.
    pub expiration_ts: [u8; 8], // LE u64
    /// The security update action.
    pub security_update_action: SecurityUpdateAction,
    /// Indicates if the instrument is user-defined.
    pub user_defined_instrument: UserDefinedInstrument,
    /// Reserved for future use.
    pub _reserved: [u8; 2],
}
impl_record!(InstrumentDefMsg, RType::InstrumentDef);
const _: () = {
    assert!(mem::size_of::<InstrumentDefMsg>() == 152);
    assert!(mem::align_of::<InstrumentDefMsg>() == 8);
};

impl InstrumentDefMsg {
    pub fn instrument_class(&self) -> InstrumentClass { self.instrument_class }
    pub fn match_algorithm(&self) -> MatchAlgorithm { self.match_algorithm }
    pub fn min_price_increment(&self) -> i32 { i32::from_le_bytes(self.min_price_increment) }
    pub fn display_factor(&self) -> i32 { i32::from_le_bytes(self.display_factor) }
    pub fn min_lot_size(&self) -> u32 { u32::from_le_bytes(self.min_lot_size) }
    pub fn max_lot_size(&self) -> u32 { u32::from_le_bytes(self.max_lot_size) }
    pub fn activation_ts(&self) -> u64 { u64::from_le_bytes(self.activation_ts) }
    pub fn expiration_ts(&self) -> u64 { u64::from_le_bytes(self.expiration_ts) }
    pub fn security_update_action(&self) -> SecurityUpdateAction { self.security_update_action }
    pub fn user_defined_instrument(&self) -> UserDefinedInstrument { self.user_defined_instrument }
}
impl std::fmt::Debug for InstrumentDefMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentDefMsg")
            .field("hd", self.header())
            .field("raw_symbol", &cstr_to_str(&self.raw_symbol))
            .field("instrument_class", &self.instrument_class())
            .field("match_algorithm", &self.match_algorithm())
            .field("currency", &cstr_to_str(&self.currency))
            .field("asset", &cstr_to_str(&self.asset))
            .field("min_price_increment", &self.min_price_increment())
            .field("display_factor", &self.display_factor())
            .field("min_lot_size", &self.min_lot_size())
            .field("max_lot_size", &self.max_lot_size())
            .field("activation_ts", &self.activation_ts())
            .field("expiration_ts", &self.expiration_ts())
            .field("security_update_action", &self.security_update_action())
            .field("user_defined_instrument", &self.user_defined_instrument())
            .finish()
    }
}
impl PartialEq for InstrumentDefMsg {
     fn eq(&self, other: &Self) -> bool {
        bytemuck::bytes_of(self) == bytemuck::bytes_of(other)
    }
}
impl Eq for InstrumentDefMsg {}

/// An auction imbalance message.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct ImbalanceMsg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The reference price.
    pub ref_price: [u8; 4], // LE i32
    /// The auction time in nanoseconds.
    pub auction_time: [u8; 8], // LE u64
    /// The continuous book clearing price.
    pub cont_book_clr_price: [u8; 4], // LE i32
    /// The auction interest clearing price.
    pub auct_int_clr_price: [u8; 4], // LE i32
    /// The short sale restriction indicator.
    pub ssr_fill_price: [u8; 4], // LE i32
    /// The indicative match price.
    pub ind_match_price: [u8; 4], // LE i32
    /// The upper collar price.
    pub upper_collar: [u8; 4], // LE i32
    /// The lower collar price.
    pub lower_collar: [u8; 4], // LE i32
    /// The number of extension periods.
    pub paired_qty: [u8; 4], // LE u32
    /// The total imbalance quantity.
    pub total_imbalance_qty: [u8; 4], // LE u32
    /// The market imbalance quantity.
    pub market_imbalance_qty: [u8; 4], // LE u32
    /// The auction type.
    pub auction_type: u8,
    /// The side of the imbalance.
    pub side: Side,
    /// Reserved for future use.
    _reserved: [u8; 6],
}
impl_record!(ImbalanceMsg, RType::Imbalance);
const _: () = {
    assert!(mem::size_of::<ImbalanceMsg>() == 80);
    assert!(mem::align_of::<ImbalanceMsg>() == 8);
};

impl ImbalanceMsg {
    pub fn ref_price(&self) -> i32 { i32::from_le_bytes(self.ref_price) }
    pub fn auction_time(&self) -> u64 { u64::from_le_bytes(self.auction_time) }
    pub fn cont_book_clr_price(&self) -> i32 { i32::from_le_bytes(self.cont_book_clr_price) }
    pub fn auct_int_clr_price(&self) -> i32 { i32::from_le_bytes(self.auct_int_clr_price) }
    pub fn ssr_fill_price(&self) -> i32 { i32::from_le_bytes(self.ssr_fill_price) }
    pub fn ind_match_price(&self) -> i32 { i32::from_le_bytes(self.ind_match_price) }
    pub fn upper_collar(&self) -> i32 { i32::from_le_bytes(self.upper_collar) }
    pub fn lower_collar(&self) -> i32 { i32::from_le_bytes(self.lower_collar) }
    pub fn paired_qty(&self) -> u32 { u32::from_le_bytes(self.paired_qty) }
    pub fn total_imbalance_qty(&self) -> u32 { u32::from_le_bytes(self.total_imbalance_qty) }
    pub fn market_imbalance_qty(&self) -> u32 { u32::from_le_bytes(self.market_imbalance_qty) }
    pub fn auction_type(&self) -> u8 { self.auction_type }
    pub fn side(&self) -> Side { self.side }
}
impl std::fmt::Debug for ImbalanceMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImbalanceMsg")
            .field("hd", self.header())
            .field("ref_price", &self.ref_price())
            .field("auction_time", &self.auction_time())
            .field("cont_book_clr_price", &self.cont_book_clr_price())
            .field("auct_int_clr_price", &self.auct_int_clr_price())
            .field("ssr_fill_price", &self.ssr_fill_price())
            .field("ind_match_price", &self.ind_match_price())
            .field("upper_collar", &self.upper_collar())
            .field("lower_collar", &self.lower_collar())
            .field("paired_qty", &self.paired_qty())
            .field("total_imbalance_qty", &self.total_imbalance_qty())
            .field("market_imbalance_qty", &self.market_imbalance_qty())
            .field("auction_type", &self.auction_type())
            .field("side", &self.side())
            .finish()
    }
}
impl_eq_record!(
    ImbalanceMsg,
    ref_price,
    auction_time,
    cont_book_clr_price,
    auct_int_clr_price,
    ssr_fill_price,
    ind_match_price,
    upper_collar,
    lower_collar,
    paired_qty,
    total_imbalance_qty,
    market_imbalance_qty,
    auction_type,
    side
);

/// A market status message.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct StatusMsg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The group of the status message.
    pub group: [u8; 20],
    /// The trading status code.
    pub trading_status: [u8; 2], // LE u16
    /// The halt reason code.
    pub halt_reason: [u8; 2], // LE u16
    /// The trading event code.
    pub trading_event: [u8; 2], // LE u16
    _reserved: [u8; 4],
}
impl_record!(StatusMsg, RType::Status);
const _: () = {
    assert!(mem::size_of::<StatusMsg>() == 56);
    assert!(mem::align_of::<StatusMsg>() == 8);
};

impl StatusMsg {
    pub fn trading_status(&self) -> u16 { u16::from_le_bytes(self.trading_status) }
    pub fn halt_reason(&self) -> u16 { u16::from_le_bytes(self.halt_reason) }
    pub fn trading_event(&self) -> u16 { u16::from_le_bytes(self.trading_event) }
}
impl std::fmt::Debug for StatusMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatusMsg")
            .field("hd", self.header())
            .field("group", &cstr_to_str(&self.group))
            .field("trading_status", &self.trading_status())
            .field("halt_reason", &self.halt_reason())
            .field("trading_event", &self.trading_event())
            .finish()
    }
}
impl PartialEq for StatusMsg {
    fn eq(&self, other: &Self) -> bool {
        bytemuck::bytes_of(self) == bytemuck::bytes_of(other)
    }
}
impl Eq for StatusMsg {}

/// A symbol mapping message for live data.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct SymbolMappingMsg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The input symbol.
    pub stype_in_symbol: [u8; SYMBOL_CSTR_LEN],
    /// The output symbol.
    pub stype_out_symbol: [u8; SYMBOL_CSTR_LEN],
    /// The input symbology type.
    pub stype_in: SType,
    /// The output symbology type.
    pub stype_out: SType,
    _reserved: [u8; 6],
}
impl_record!(SymbolMappingMsg, RType::SymbolMapping);
const _: () = {
    assert!(mem::size_of::<SymbolMappingMsg>() == 176);
    assert!(mem::align_of::<SymbolMappingMsg>() == 8);
};

impl SymbolMappingMsg {
    pub fn stype_in(&self) -> SType { self.stype_in }
    pub fn stype_out(&self) -> SType { self.stype_out }
}
impl std::fmt::Debug for SymbolMappingMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SymbolMappingMsg")
            .field("hd", self.header())
            .field("stype_in_symbol", &cstr_to_str(&self.stype_in_symbol))
            .field("stype_out_symbol", &cstr_to_str(&self.stype_out_symbol))
            .field("stype_in", &self.stype_in())
            .field("stype_out", &self.stype_out())
            .finish()
    }
}
impl PartialEq for SymbolMappingMsg {
    fn eq(&self, other: &Self) -> bool {
        bytemuck::bytes_of(self) == bytemuck::bytes_of(other)
    }
}
impl Eq for SymbolMappingMsg {}

/// A system message from the gateway.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct SystemMsg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The message from the gateway.
    pub msg: [u8; 128],
    /// The system message code.
    pub code: [u8; 2], // LE u16
    _reserved: [u8; 6],
}
impl_record!(SystemMsg, RType::System);
const _: () = {
    assert!(mem::size_of::<SystemMsg>() == 160);
    assert!(mem::align_of::<SystemMsg>() == 8);
};

impl SystemMsg {
    pub fn code(&self) -> u16 { u16::from_le_bytes(self.code) }
}
impl std::fmt::Debug for SystemMsg {
     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemMsg")
            .field("hd", self.header())
            .field("msg", &cstr_to_str(&self.msg))
            .field("code", &self.code())
            .finish()
    }
}
impl PartialEq for SystemMsg {
    fn eq(&self, other: &Self) -> bool {
        bytemuck::bytes_of(self) == bytemuck::bytes_of(other)
    }
}
impl Eq for SystemMsg {}

/// An error message from the gateway.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct ErrorMsg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The error message.
    pub err: [u8; 128],
    /// The error code.
    pub code: [u8; 2], // LE u16
    /// Indicates if this is the last message in a sequence.
    pub is_last: u8,
    _reserved: [u8; 5],
}
impl_record!(ErrorMsg, RType::Error);
const _: () = {
    assert!(mem::size_of::<ErrorMsg>() == 160);
    assert!(mem::align_of::<ErrorMsg>() == 8);
};

impl ErrorMsg {
    pub fn code(&self) -> u16 { u16::from_le_bytes(self.code) }
    pub fn is_last(&self) -> u8 { self.is_last }
}
impl std::fmt::Debug for ErrorMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorMsg")
            .field("hd", self.header())
            .field("err", &cstr_to_str(&self.err))
            .field("code", &self.code())
            .field("is_last", &self.is_last())
            .finish()
    }
}
impl PartialEq for ErrorMsg {
    fn eq(&self, other: &Self) -> bool {
        bytemuck::bytes_of(self) == bytemuck::bytes_of(other)
    }
}
impl Eq for ErrorMsg {}

/// An exchange-reported statistic message.
#[repr(C, align(8))]
#[derive(Clone, Copy)]
pub struct StatMsg {
    /// The common record header.
    pub hd: RecordHeader,
    /// The timestamp of the statistic update.
    pub ts_ref: [u8; 8], // LE u64
    /// The value of the statistic.
    pub value: [u8; 8], // LE i64
    /// The quantity associated with the statistic.
    pub quantity: [u8; 8], // LE u64
    /// The type of statistic.
    pub stat_type: StatType,
    /// The update action for the statistic.
    pub stat_update_action: StatUpdateAction,
    /// Flags for the statistic.
    pub stat_flags: [u8; 2], // LE u16
    /// The channel ID of the update.
    pub channel_id: [u8; 2], // LE u16
    _reserved: [u8; 4],
}
impl_record!(StatMsg, RType::Stat);
const _: () = {
    assert!(mem::size_of::<StatMsg>() == 64);
    assert!(mem::align_of::<StatMsg>() == 8);
};

impl StatMsg {
    pub fn ts_ref(&self) -> u64 { u64::from_le_bytes(self.ts_ref) }
    pub fn value(&self) -> i64 { i64::from_le_bytes(self.value) }
    pub fn quantity(&self) -> u64 { u64::from_le_bytes(self.quantity) }
    pub fn stat_type(&self) -> StatType { self.stat_type }
    pub fn stat_update_action(&self) -> StatUpdateAction { self.stat_update_action }
    pub fn stat_flags(&self) -> u16 { u16::from_le_bytes(self.stat_flags) }
    pub fn channel_id(&self) -> u16 { u16::from_le_bytes(self.channel_id) }
}
impl std::fmt::Debug for StatMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatMsg")
            .field("hd", self.header())
            .field("ts_ref", &self.ts_ref())
            .field("value", &self.value())
            .field("quantity", &self.quantity())
            .field("stat_type", &self.stat_type())
            .field("stat_update_action", &self.stat_update_action())
            .field("stat_flags", &self.stat_flags())
            .field("channel_id", &self.channel_id())
            .finish()
    }
}
impl_eq_record!(
    StatMsg,
    ts_ref,
    value,
    quantity,
    stat_type,
    stat_update_action,
    stat_flags,
    channel_id
);
