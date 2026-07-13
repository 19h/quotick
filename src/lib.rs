//! Deterministic building blocks for low-latency financial market infrastructure.
//!
//! The crate currently provides validated fixed-point domain values, an
//! effective-time-versioned instrument master, a price-time-priority matching
//! engine, deterministic pre-trade risk, anonymized level-2 market-data
//! publication, and an atomic multi-asset double-entry ledger. No binary
//! floating point value crosses a financial state transition.

#![forbid(unsafe_code)]

pub mod codec;
pub mod domain;
pub mod durable;
pub mod durable_ledger;
pub mod durable_risk;
pub mod instrument;
pub mod journal;
pub mod ledger;
pub mod market_data;
pub mod matching;
pub mod risk;

pub use domain::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs, TradeId, TransactionId,
};
