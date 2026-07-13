// quotick/src/compat.rs
//! # Version Compatibility
//!
//! This module is responsible for handling backward compatibility for the QTB format.
//! As the format evolves, older versions of record structs and the logic to upgrade
//! them to the current version will be defined here.
//!
//! ## Versioning Strategy
//!
//! 1.  **Metadata Version Check**: The `Decoder` reads the `version` field from the
//!     `MetadataHeader` at the beginning of a file.
//!
//! 2.  **Dispatching**: If the file `version` is older than the library's `QTB_VERSION`,
//!     the `Decoder` will use the types and conversion logic defined in this module.
//!
//! 3.  **Versioned Structs**: For each past version `N` where a record `T` has a
//!     different layout, a corresponding struct `TN` (e.g., `TradeMsgV1`) will be
//!     defined here. These structs will have `#[repr(C)]` and match the exact binary
//!     layout of that older version, including endianness and padding.
//!
//! 4.  **Upgrading**: The `From` trait will be implemented to convert from the old
//!     struct to the current one (e.g., `impl From<TradeMsgV1> for TradeMsg`). This
//!     conversion will handle adding default values for new fields, transforming
//!     data types, etc.
//!
//! 5.  **Transparent API**: The `Decoder` will perform this upgrade internally, so the
//!     end-user of the library will always receive the latest version of the record
//!     structs, regardless of the underlying file format version.
//!
//! ## Example (for a hypothetical Version 2)
//!
//! ```rust,ignore
//! // In a future version, this module might contain:
//!
//! use crate::record::TradeMsg;
//!
//! /// The layout of a TradeMsg in Version 1 of the format.
//! #[repr(C)]
//! pub struct TradeMsgV1 {
//!     // ... fields from V1
//! }
//!
//! /// Upgrades a V1 trade message to the current version.
//! impl From<TradeMsgV1> for TradeMsg {
//!     fn from(old: TradeMsgV1) -> Self {
//!         Self {
//!             hd: old.hd.into(), // Assuming RecordHeader also needs conversion
//!             price: old.price,
//!             size: old.size,
//!             // A new field added in V2, initialized with a default.
//!             exchange_id: 0,
//!             // ... other fields
//!         }
//!     }
//! }
//! ```

// This module is a placeholder for the initial release as there is only one version.
