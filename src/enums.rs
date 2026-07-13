// quotick/src/enums.rs
//! Contains all core enumerations for the `qtb` data model.

use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

use crate::error::{Error, Result};

/// The data record schema.
///
/// A schema defines the set of fields that constitute a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C, u16)]
pub enum Schema {
    /// Market by price with a book depth of 1.
    Mbp1 = 0,
    /// Market by price with a book depth of 10.
    Mbp10 = 1,
    /// All trade messages.
    Trade = 2,
    /// Open, high, low, close, and volume.
    Ohlcv = 3,
    /// Instrument definition.
    Definition = 4,
    /// Market status.
    Status = 5,
    /// Exchange-reported statistics.
    Statistics = 6,
    /// A mix of different schemas, used for live data.
    Mixed = u16::MAX,
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Schema::Mbp1 => "mbp-1",
            Schema::Mbp10 => "mbp-10",
            Schema::Trade => "trades",
            Schema::Ohlcv => "ohlcv",
            Schema::Definition => "definition",
            Schema::Status => "status",
            Schema::Statistics => "statistics",
            Schema::Mixed => "mixed",
        };
        f.write_str(s)
    }
}

impl FromStr for Schema {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "mbp-1" => Ok(Schema::Mbp1),
            "mbp-10" => Ok(Schema::Mbp10),
            "trades" => Ok(Schema::Trade),
            "ohlcv" => Ok(Schema::Ohlcv),
            "definition" => Ok(Schema::Definition),
            "status" => Ok(Schema::Status),
            "statistics" => Ok(Schema::Statistics),
            "mixed" => Ok(Schema::Mixed),
            _ => Err(Error::InvalidSchema(format!("Unknown schema '{}'", s))),
        }
    }
}

/// The symbology type.
///
/// Defines the convention for representing financial instruments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C, u8)]
pub enum SType {
    /// A raw, text-based symbol (e.g., "AAPL").
    RawSymbol = 0,
    /// A numeric instrument ID assigned by the exchange or Databento.
    InstrumentId = 1,
    /// A smart symbol that resolves to the current front-month contract.
    Smart = 2,
    /// A mix of different symbology types.
    Mixed = u8::MAX,
}

impl fmt::Display for SType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SType::RawSymbol => "raw_symbol",
            SType::InstrumentId => "instrument_id",
            SType::Smart => "smart",
            SType::Mixed => "mixed",
        };
        f.write_str(s)
    }
}

impl FromStr for SType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "raw_symbol" => Ok(SType::RawSymbol),
            "instrument_id" => Ok(SType::InstrumentId),
            "smart" => Ok(SType::Smart),
            "mixed" => Ok(SType::Mixed),
            _ => Err(Error::Symbology(format!("Unknown SType '{}'", s))),
        }
    }
}

/// The record type identifier.
///
/// Each record struct has a unique `RType`. This is the primary value used to
/// differentiate records in a stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RType {
    /// Market by Price with 1 level of depth record.
    Mbp1 = 0x01,
    /// Market by Price with 10 levels of depth record.
    Mbp10 = 0x0A,
    /// Trade record.
    Trade = 0x10,
    /// OHLCV record.
    Ohlcv = 0x11,
    /// Instrument definition record.
    InstrumentDef = 0x20,
    /// Imbalance record.
    Imbalance = 0x21,
    /// Market status record.
    Status = 0x22,
    /// Symbol mapping record.
    SymbolMapping = 0x23,
    /// System message record.
    System = 0x24,
    /// Error message record.
    Error = 0x25,
    /// Statistic record.
    Stat = 0x26,
}

impl TryFrom<u8> for RType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x01 => Ok(RType::Mbp1),
            0x0A => Ok(RType::Mbp10),
            0x10 => Ok(RType::Trade),
            0x11 => Ok(RType::Ohlcv),
            0x20 => Ok(RType::InstrumentDef),
            0x21 => Ok(RType::Imbalance),
            0x22 => Ok(RType::Status),
            0x23 => Ok(RType::SymbolMapping),
            0x24 => Ok(RType::System),
            0x25 => Ok(RType::Error),
            0x26 => Ok(RType::Stat),
            _ => Err(Error::Decode(format!("Unknown RType: {:#04x}", value))),
        }
    }
}

/// The side of an order or trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod)]
#[repr(C, u8)]
pub enum Side {
    /// A sell order or trade.
    Ask = b'A',
    /// A buy order or trade.
    Bid = b'B',
    /// No side specified or applicable.
    None = b'N',
}

/// The action that triggered a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod)]
#[repr(C, u8)]
pub enum Action {
    /// A new order was added.
    Add = b'A',
    /// An existing order was modified.
    Modify = b'M',
    /// An existing order was deleted.
    Delete = b'D',
    /// A trade occurred.
    Trade = b'T',
    /// An order was filled.
    Fill = b'F',
}

/// The class of a financial instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod)]
#[repr(C, u8)]
pub enum InstrumentClass {
    /// A stock or equity.
    Stock = b'S',
    /// A bond.
    Bond = b'B',
    /// A call option.
    Call = b'C',
    /// A put option.
    Put = b'P',
    /// A future.
    Future = b'F',
    /// A currency.
    Currency = b'X',
    /// A combination of instruments.
    Combo = b'M',
}

/// The algorithm used for matching orders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod)]
#[repr(C, u8)]
pub enum MatchAlgorithm {
    /// First In, First Out.
    Fifo = b'F',
    /// Pro-rata matching.
    ProRata = b'P',
    /// A configurable matching algorithm.
    Configurable = b'C',
    /// No matching algorithm specified.
    None = b'N',
}

/// The type of update for a security status message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod)]
#[repr(C, u8)]
pub enum SecurityUpdateAction {
    /// A new security is being added.
    Add = b'A',
    /// An existing security is being deleted.
    Delete = b'D',
    /// An existing security is being modified.
    Modify = b'M',
}

/// The type of statistic in a `StatMsg`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod)]
#[repr(C, u8)]
pub enum StatType {
    /// The volume-weighted average price (VWAP).
    Vwap = 1,
    /// The average trade size.
    AverageTradeSize = 2,
    /// The opening price for the session.
    Open = 3,
    /// The highest price for the session.
    High = 4,
    /// The lowest price for the session.
    Low = 5,
    /// The last traded price.
    Last = 6,
    /// The net change from the previous closing price.
    NetChange = 7,
    /// The total traded volume for the session.
    Volume = 8,
    /// The official settlement price.
    Settlement = 9,
    /// The open interest.
    OpenInterest = 10,
}

/// The update action for a statistic in a `StatMsg`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod)]
#[repr(C, u8)]
pub enum StatUpdateAction {
    /// A new statistic value.
    New = b'N',
    /// A correction to a previously sent statistic.
    Correction = b'C',
    /// A deletion of a previously sent statistic.
    Delete = b'D',
}

/// Specifies if an instrument is user-defined.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, bytemuck::Pod)]
#[repr(C, u8)]
pub enum UserDefinedInstrument {
    /// Not a user-defined instrument.
    No = b'N',
    /// A user-defined instrument.
    Yes = b'Y',
}
