// quotick/src/lib.rs
//! # QTB: A High-Performance Format for Normalized Market Data
//!
//! QTB (Quotick Binary) is a binary encoding format designed for efficient, zero-copy
//! processing of financial market data. It is heavily inspired by the Databento Binary
//! Encoding (DBN) format but architected from the ground up in Rust for maximum
//! performance, safety, and ease of use.
//!
//! ## Core Features
//!
//! - **Zero-Copy Deserialization:** Data is read from disk directly into memory-mapped
//!   structs without any intermediate parsing or allocation, enabling extremely fast
//!   data access. This is achieved safely using `bytemuck`.
//! - **Portable & Stable ABI:** The on-disk format uses a strict little-endian layout
//!   with explicit padding and 8-byte alignment, ensuring files are portable across
//!   all machine architectures.
//! - **Self-Describing Files:** Each `.qtb` file contains a metadata header with schema
//!   definitions, symbology mappings, and dataset context, making files portable and
//!   self-contained.
//! - **Schema-Driven:** A fixed set of schemas ensures a standardized, normalized
//!   representation for various types of market data (order book, trades, etc.).
//!
//! ## Example Usage
//!
//! ```no_run
//! use quotick::prelude::*;
//! use quotick::{QtbStore, OhlcvMsg};
//!
//! // Write to a QTB file
//! let records_to_write: Vec<OhlcvMsg> = // ...
//! # Vec::new();
//! let mut store = QtbStore::file_create(
//!     "./test.qtb",
//!     Schema::Ohlcv,
//!     &["SPY", "QQQ"],
//! ).unwrap();
//! for record in records_to_write {
//!     store.write_record(&record).unwrap();
//! }
//! store.finish().unwrap();
//!
//! // Read from a QTB file
//! let store = QtbStore::file_open("./test.qtb").unwrap();
//! let metadata = store.metadata();
//! println!("Read {} records for symbols: {:?}", store.record_count(), metadata.symbols);
//!
//! // Use the safe, borrowing iterator
//! for record_result in store.iter::<OhlcvMsg>() {
//!     let record = record_result.unwrap();
//!     // Process the zero-copy record
//!     println!("OHLCV @ {}: {:?}", record.header().ts_event(), record);
//! }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(missing_docs)]

pub mod compat;
pub mod decode;
pub mod encode;
pub mod enums;
pub mod error;
pub mod importer;
#[macro_use]
pub mod macros;
pub mod metadata;
pub mod prelude;
pub mod record;
pub mod store;

pub use crate::decode::Decoder;
pub use crate::encode::Encoder;
pub use crate::error::{Error, Result};
pub use crate::metadata::Metadata;
pub use crate::record::{Record, RecordHeader};
pub use crate::store::QtbStore;

// Re-export all record structs.
pub use crate::record::{
    ErrorMsg, ImbalanceMsg, InstrumentDefMsg, Mbp10Msg, Mbp1Msg, OhlcvMsg, StatMsg, StatusMsg,
    SymbolMappingMsg, SystemMsg, TradeMsg,
};

// Re-export all enums.
pub use crate::enums::{
    Action, InstrumentClass, MatchAlgorithm, RType, Schema, SecurityUpdateAction, Side, StatType,
    StatUpdateAction, SType, UserDefinedInstrument,
};
