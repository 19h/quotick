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
mod durable_storage;
mod indexed_avl;
pub mod instrument;
pub mod journal;
pub mod ledger;
mod ledger_magnitude;
pub mod market_data;
pub mod matching;
pub mod risk;
mod segmented_journal;
pub mod snapshot;

pub use domain::{
    AccountId, AccountingDate, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, ReconciliationId, Side, TimestampNs, TradeId, TransactionId,
};
