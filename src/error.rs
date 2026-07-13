// quotick/src/error.rs
//! Defines the error and result types for the `qtb` library.

use std::fmt;
use std::io;

/// The result type for `qtb` operations.
pub type Result<T> = std::result::Result<T, Error>;

/// The error type for `qtb` operations.
#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    /// An underlying I/O error occurred.
    Io(io::Error),
    /// An error occurred during the decoding of data from a `.qtb` stream.
    /// This can include issues with metadata or individual records.
    Decode(String),
    /// An error occurred during the encoding of data to a `.qtb` stream.
    Encode(String),
    /// A user attempted to cast a record to an incorrect type during iteration.
    InvalidRecordCast,
    /// An operation was attempted with an unsupported or incorrect schema.
    InvalidSchema(String),
    /// An error related to symbology mapping or types.
    Symbology(String),
    /// An error indicating a feature is not yet implemented.
    NotImplemented(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::Decode(s) => write!(f, "Decoding error: {}", s),
            Self::Encode(s) => write!(f, "Encoding error: {}", s),
            Self::InvalidRecordCast => write!(f, "Invalid record cast: record rtype does not match requested type"),
            Self::InvalidSchema(s) => write!(f, "Invalid schema: {}", s),
            Self::Symbology(s) => write!(f, "Symbology error: {}", s),
            Self::NotImplemented(s) => write!(f, "Not implemented: {}", s),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}
