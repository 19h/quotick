// quotick/src/importer/mod.rs
//! A utility for converting Databento Binary Encoding (DBN) files to the Quotick
//! Binary (QTB) format.
//!
//! This module provides a `Converter` that handles the reading of DBN metadata and
//! records, their translation into the QTB equivalents, and writing the output to a
//! new `.qtb` file.

use std::fmt;
use std::io;

pub mod converter;
pub(crate) mod dbn_defs;

pub use converter::Converter;

/// The result type for importer operations.
pub type Result<T> = std::result::Result<T, ImportError>;

/// An error that can occur during the DBN to QTB conversion process.
#[derive(Debug)]
pub enum ImportError {
    /// An underlying I/O error occurred.
    Io(io::Error),
    /// An error occurred while parsing the input DBN file.
    DbnParseError(String),
    /// An error occurred while encoding the output QTB file.
    QtbEncodeError(crate::error::Error),
    /// The DBN schema is not supported for conversion.
    UnsupportedSchema(String),
    /// The DBN file version is not supported for a specific record type.
    UnsupportedVersion(String),
}

impl fmt::Display for ImportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::DbnParseError(s) => write!(f, "DBN parsing error: {}", s),
            Self::QtbEncodeError(e) => write!(f, "QTB encoding error: {}", e),
            Self::UnsupportedSchema(s) => write!(f, "Unsupported DBN schema: {}", s),
            Self::UnsupportedVersion(s) => write!(f, "Unsupported DBN version: {}", s),
        }
    }
}

impl std::error::Error for ImportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::QtbEncodeError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for ImportError {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<crate::error::Error> for ImportError {
    fn from(err: crate::error::Error) -> Self {
        Self::QtbEncodeError(err)
    }
}
