//! Validated identifiers and fixed-point financial values.

use core::fmt;

/// Validation error for a core domain value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DomainError {
    /// An identifier was zero even though zero is reserved as an invalid sentinel.
    ZeroIdentifier(&'static str),
    /// A quantity was zero.
    ZeroQuantity,
}

impl fmt::Display for DomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroIdentifier(name) => write!(formatter, "{name} must be non-zero"),
            Self::ZeroQuantity => formatter.write_str("quantity must be non-zero"),
        }
    }
}

impl std::error::Error for DomainError {}

macro_rules! identifier {
    ($name:ident, $label:literal) => {
        #[doc = concat!("A validated ", $label, ".")]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[repr(transparent)]
        pub struct $name(u64);

        impl $name {
            #[doc = concat!("Constructs a non-zero ", $label, ".")]
            ///
            /// # Errors
            ///
            /// Returns [`DomainError::ZeroIdentifier`] when `value` is zero.
            pub const fn new(value: u64) -> Result<Self, DomainError> {
                if value == 0 {
                    Err(DomainError::ZeroIdentifier($label))
                } else {
                    Ok(Self(value))
                }
            }

            #[doc = concat!("Returns the underlying ", $label, " value.")]
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

identifier!(AccountId, "account identifier");
identifier!(AssetId, "asset identifier");
identifier!(AuctionId, "auction identifier");
identifier!(CalendarId, "calendar identifier");
identifier!(CalendarVersion, "calendar version");
identifier!(CommandId, "command identifier");
identifier!(InstrumentId, "instrument identifier");
identifier!(InstrumentVersion, "instrument version");
identifier!(OrderId, "order identifier");
identifier!(ReconciliationId, "reconciliation identifier");
identifier!(StopReferenceSequence, "stop-reference sequence");
identifier!(StopReferenceSourceId, "stop-reference source identifier");
identifier!(StopReferenceSourceVersion, "stop-reference source version");
identifier!(TradeId, "trade identifier");
identifier!(TransactionId, "transaction identifier");
identifier!(TradingSessionId, "trading-session identifier");

/// Accounting-date scalar represented as signed days from 1970-01-01.
///
/// Gregorian conversion, time-zone selection, and holiday/session membership
/// belong to a versioned calendar service; this scalar provides deterministic
/// ordering and a compact stable wire representation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct AccountingDate(i32);

impl AccountingDate {
    /// Smallest representable accounting date.
    pub const MIN: Self = Self(i32::MIN);
    /// Unix epoch date, 1970-01-01.
    pub const UNIX_EPOCH: Self = Self(0);
    /// Largest representable accounting date.
    pub const MAX: Self = Self(i32::MAX);

    /// Constructs a date from signed days relative to 1970-01-01.
    #[must_use]
    pub const fn from_days_since_unix_epoch(days: i32) -> Self {
        Self(days)
    }

    /// Returns signed days relative to 1970-01-01.
    #[must_use]
    pub const fn days_since_unix_epoch(self) -> i32 {
        self.0
    }
}

/// A price expressed in an instrument-defined integer quantum.
///
/// Negative values are supported because physically delivered commodities can
/// trade below zero.  The instrument catalog, rather than this scalar type,
/// defines the quantum and any admissible price collars.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Price(i64);

impl Price {
    /// Constructs a price from integer quantum units.
    #[must_use]
    pub const fn from_raw(raw: i64) -> Self {
        Self(raw)
    }

    /// Returns the price in integer quantum units.
    #[must_use]
    pub const fn raw(self) -> i64 {
        self.0
    }
}

/// A strictly positive quantity expressed in instrument-defined lots.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Quantity(u64);

impl Quantity {
    /// Constructs a strictly positive quantity.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::ZeroQuantity`] when `lots` is zero.
    pub const fn new(lots: u64) -> Result<Self, DomainError> {
        if lots == 0 {
            Err(DomainError::ZeroQuantity)
        } else {
            Ok(Self(lots))
        }
    }

    /// Returns the number of lots.
    #[must_use]
    pub const fn lots(self) -> u64 {
        self.0
    }
}

/// The side of an order.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Side {
    /// An order to buy the instrument.
    Buy,
    /// An order to sell the instrument.
    Sell,
}

impl Side {
    /// Returns the opposite side.
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

/// Nanoseconds since the Unix epoch, UTC.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct TimestampNs(u64);

impl TimestampNs {
    /// Constructs a UTC timestamp from nanoseconds since the Unix epoch.
    #[must_use]
    pub const fn from_unix_nanos(value: u64) -> Self {
        Self(value)
    }

    /// Returns nanoseconds since the Unix epoch.
    #[must_use]
    pub const fn as_unix_nanos(self) -> u64 {
        self.0
    }
}
