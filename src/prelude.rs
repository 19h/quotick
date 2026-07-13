// quotick/src/prelude.rs
//! The `qtb` prelude.
//!
//! This module re-exports the most commonly used items for convenience.
//! Import it with `use quotick::prelude::*;` to bring the essential
//! types and traits into scope.

// High-level store and error handling
pub use crate::store::QtbStore;
pub use crate::error::{Error, Result};

// Core metadata and record traits/structs
pub use crate::metadata::Metadata;
pub use crate::record::{Record, RecordHeader, MbpLevel};

// All record message types
pub use crate::record::{
    ErrorMsg, ImbalanceMsg, InstrumentDefMsg, Mbp10Msg, Mbp1Msg, OhlcvMsg, StatMsg, StatusMsg,
    SymbolMappingMsg, SystemMsg, TradeMsg,
};

// All public enums
pub use crate::enums::{
    Action, InstrumentClass, MatchAlgorithm, RType, Schema, SecurityUpdateAction, Side, StatType,
    StatUpdateAction, SType, UserDefinedInstrument,
};
