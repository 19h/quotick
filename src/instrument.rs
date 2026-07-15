//! Immutable asset metadata and effective-time-versioned instrument rules.

use std::fmt;
use std::hash::Hash;

use crate::bounded_hash::BoundedHashMap;
use crate::domain::{AssetId, InstrumentId, InstrumentVersion, Price, Quantity, TimestampNs};
use crate::ledger::SettlementConvention;
use crate::matching::{Command, NewOrder, OrderDisplay, OrderType, ReplaceOrder, Trade};

const MAX_CODE_LENGTH: usize = 16;
const MAX_SYMBOL_LENGTH: usize = 32;
const MAX_DECIMAL_SCALE: u8 = 18;

/// One finite authoritative instrument-catalog resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstrumentCatalogResource {
    /// Stable asset-identifier index.
    AssetIdentifiers,
    /// Canonical asset-code index.
    AssetCodes,
    /// Instrument-identifier to version-range index.
    Instruments,
    /// Immutable instrument definitions across all version histories.
    InstrumentVersions,
}

impl fmt::Display for InstrumentCatalogResource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AssetIdentifiers => "asset identifiers",
            Self::AssetCodes => "asset codes",
            Self::Instruments => "instrument identifiers",
            Self::InstrumentVersions => "instrument versions",
        })
    }
}

/// Raw finite resource envelope for one instrument-catalog generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstrumentCatalogLimitsSpec {
    /// Maximum registered assets and canonical asset codes.
    pub max_assets: usize,
    /// Maximum distinct instrument identifiers.
    pub max_instruments: usize,
    /// Maximum immutable definitions across all instrument histories.
    pub max_instrument_versions: usize,
}

impl Default for InstrumentCatalogLimitsSpec {
    fn default() -> Self {
        Self {
            max_assets: InstrumentCatalogLimits::DEFAULT_MAX_ASSETS,
            max_instruments: InstrumentCatalogLimits::DEFAULT_MAX_INSTRUMENTS,
            max_instrument_versions: InstrumentCatalogLimits::DEFAULT_MAX_INSTRUMENT_VERSIONS,
        }
    }
}

/// Contradiction in a requested finite instrument-catalog envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstrumentCatalogLimitsError {
    /// Every finite maximum must be positive.
    ZeroMaximum(InstrumentCatalogResource),
    /// Every registered instrument consumes at least one version slot.
    InstrumentsExceedVersions {
        /// Instrument-identifier maximum.
        instruments: usize,
        /// Global immutable-definition maximum.
        versions: usize,
    },
}

impl fmt::Display for InstrumentCatalogLimitsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroMaximum(resource) => {
                write!(formatter, "instrument-catalog {resource} maximum is zero")
            }
            Self::InstrumentsExceedVersions {
                instruments,
                versions,
            } => write!(
                formatter,
                "instrument-catalog instrument maximum {instruments} exceeds version maximum {versions}"
            ),
        }
    }
}

impl std::error::Error for InstrumentCatalogLimitsError {}

/// Validated immutable instrument-catalog resource envelope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstrumentCatalogLimits(InstrumentCatalogLimitsSpec);

impl InstrumentCatalogLimits {
    /// Default registered-asset and canonical-code maximum.
    pub const DEFAULT_MAX_ASSETS: usize = 4_096;
    /// Default distinct-instrument maximum.
    pub const DEFAULT_MAX_INSTRUMENTS: usize = 16_384;
    /// Default immutable-definition maximum across all histories.
    pub const DEFAULT_MAX_INSTRUMENT_VERSIONS: usize = 65_536;

    /// Validates one finite catalog envelope.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentCatalogLimitsError`] for a zero or contradictory
    /// maximum.
    pub const fn new(
        spec: InstrumentCatalogLimitsSpec,
    ) -> Result<Self, InstrumentCatalogLimitsError> {
        if spec.max_assets == 0 {
            return Err(InstrumentCatalogLimitsError::ZeroMaximum(
                InstrumentCatalogResource::AssetIdentifiers,
            ));
        }
        if spec.max_instruments == 0 {
            return Err(InstrumentCatalogLimitsError::ZeroMaximum(
                InstrumentCatalogResource::Instruments,
            ));
        }
        if spec.max_instrument_versions == 0 {
            return Err(InstrumentCatalogLimitsError::ZeroMaximum(
                InstrumentCatalogResource::InstrumentVersions,
            ));
        }
        if spec.max_instruments > spec.max_instrument_versions {
            return Err(InstrumentCatalogLimitsError::InstrumentsExceedVersions {
                instruments: spec.max_instruments,
                versions: spec.max_instrument_versions,
            });
        }
        Ok(Self(spec))
    }

    /// Returns the corresponding raw specification.
    #[must_use]
    pub const fn spec(self) -> InstrumentCatalogLimitsSpec {
        self.0
    }

    /// Maximum registered assets and canonical codes.
    #[must_use]
    pub const fn max_assets(self) -> usize {
        self.0.max_assets
    }

    /// Maximum distinct instrument identifiers.
    #[must_use]
    pub const fn max_instruments(self) -> usize {
        self.0.max_instruments
    }

    /// Maximum immutable definitions across all histories.
    #[must_use]
    pub const fn max_instrument_versions(self) -> usize {
        self.0.max_instrument_versions
    }
}

impl Default for InstrumentCatalogLimits {
    fn default() -> Self {
        Self::new(InstrumentCatalogLimitsSpec::default())
            .expect("default instrument-catalog limits must be valid")
    }
}

impl TryFrom<InstrumentCatalogLimitsSpec> for InstrumentCatalogLimits {
    type Error = InstrumentCatalogLimitsError;

    fn try_from(spec: InstrumentCatalogLimitsSpec) -> Result<Self, Self::Error> {
        Self::new(spec)
    }
}

/// Failure before an instrument catalog owns usable state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstrumentCatalogConstructionError {
    /// The requested finite resource envelope is invalid.
    InvalidLimits(InstrumentCatalogLimitsError),
    /// Complete constructor-owned storage could not be represented or reserved.
    ReservationFailed {
        /// Resource whose fixed layout could not be reserved.
        resource: InstrumentCatalogResource,
        /// Requested semantic maximum.
        maximum: usize,
    },
}

impl fmt::Display for InstrumentCatalogConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLimits(error) => error.fmt(formatter),
            Self::ReservationFailed { resource, maximum } => write!(
                formatter,
                "failed to reserve instrument-catalog {resource} storage through {maximum} entries"
            ),
        }
    }
}

impl std::error::Error for InstrumentCatalogConstructionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidLimits(error) => Some(error),
            Self::ReservationFailed { .. } => None,
        }
    }
}

impl From<InstrumentCatalogLimitsError> for InstrumentCatalogConstructionError {
    fn from(error: InstrumentCatalogLimitsError) -> Self {
        Self::InvalidLimits(error)
    }
}

/// Instrument-master validation, registration, or lookup failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InstrumentError {
    /// A code or symbol was empty.
    EmptyText(&'static str),
    /// A code or symbol exceeded its fixed-capacity representation.
    TextTooLong {
        /// Field name.
        field: &'static str,
        /// Supplied bytes.
        length: usize,
        /// Maximum bytes.
        maximum: usize,
    },
    /// A code or symbol contained a byte outside the canonical ASCII alphabet.
    InvalidTextByte {
        /// Field name.
        field: &'static str,
        /// Invalid byte.
        byte: u8,
    },
    /// Decimal scale exceeded 18 digits.
    DecimalScaleTooLarge(u8),
    /// A price rule increment was zero or exceeded `i64::MAX`.
    InvalidTickSize,
    /// The minimum price exceeded the maximum price.
    InvalidPriceRange,
    /// A collar boundary was not aligned to the tick grid.
    CollarOffTickGrid,
    /// A quantity rule used a zero increment or boundary.
    InvalidQuantityRule,
    /// A quantity boundary was not aligned to the increment.
    QuantityBoundaryOffGrid,
    /// Base and quote assets were identical.
    IdenticalAssets,
    /// A settlement conversion multiplier was zero.
    ZeroSettlementMultiplier,
    /// Enabled reserve-order rules allowed zero peak replenishments.
    InvalidReserveOrderRules,
    /// An asset identifier or canonical code was already registered.
    DuplicateAsset,
    /// An instrument version identifier was already registered.
    DuplicateInstrumentVersion,
    /// An instrument referenced an asset absent from the catalog.
    UnknownAsset(AssetId),
    /// A canonical asset code was absent from the catalog.
    UnknownAssetCode(AssetCode),
    /// A new version was not strictly later in version and effective time.
    NonMonotonicVersion,
    /// Immutable identity fields changed under one instrument identifier.
    IdentityChanged,
    /// A new identity or definition would exceed the finite catalog envelope.
    CapacityExceeded {
        /// Exhausted catalog resource.
        resource: InstrumentCatalogResource,
        /// Configured semantic maximum.
        maximum: usize,
        /// Cardinality that the rejected registration would require.
        attempted: usize,
    },
    /// No instrument definition was effective at the requested time.
    NoEffectiveVersion {
        /// Instrument requested.
        instrument_id: InstrumentId,
        /// Lookup timestamp.
        at: TimestampNs,
    },
    /// The requested instrument is absent.
    UnknownInstrument(InstrumentId),
    /// The requested immutable instrument version is absent.
    UnknownInstrumentVersion {
        /// Instrument requested.
        instrument_id: InstrumentId,
        /// Version requested.
        version: InstrumentVersion,
    },
}

impl fmt::Display for InstrumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyText(field) => write!(formatter, "{field} must not be empty"),
            Self::TextTooLong {
                field,
                length,
                maximum,
            } => write!(formatter, "{field} length {length} exceeds {maximum}"),
            Self::InvalidTextByte { field, byte } => {
                write!(formatter, "{field} contains invalid byte {byte:#04x}")
            }
            Self::DecimalScaleTooLarge(scale) => {
                write!(
                    formatter,
                    "decimal scale {scale} exceeds {MAX_DECIMAL_SCALE}"
                )
            }
            Self::InvalidTickSize => formatter.write_str("tick size must be in 1..=i64::MAX"),
            Self::InvalidPriceRange => formatter.write_str("minimum price exceeds maximum price"),
            Self::CollarOffTickGrid => formatter.write_str("price collar is off the tick grid"),
            Self::InvalidQuantityRule => formatter.write_str(
                "quantity increment and boundaries must be non-zero with minimum <= maximum",
            ),
            Self::QuantityBoundaryOffGrid => {
                formatter.write_str("quantity boundary is off the increment grid")
            }
            Self::IdenticalAssets => formatter.write_str("base and quote assets must differ"),
            Self::ZeroSettlementMultiplier => {
                formatter.write_str("settlement multipliers must be non-zero")
            }
            Self::InvalidReserveOrderRules => formatter
                .write_str("enabled reserve-order rules require a positive replenishment limit"),
            Self::DuplicateAsset => formatter.write_str("asset identifier or code already exists"),
            Self::DuplicateInstrumentVersion => {
                formatter.write_str("instrument version already exists")
            }
            Self::UnknownAsset(asset_id) => write!(formatter, "unknown asset {asset_id}"),
            Self::UnknownAssetCode(code) => write!(formatter, "unknown asset code {code}"),
            Self::NonMonotonicVersion => formatter
                .write_str("instrument versions and effective timestamps must increase strictly"),
            Self::IdentityChanged => {
                formatter.write_str("immutable instrument identity changed across versions")
            }
            Self::CapacityExceeded {
                resource,
                maximum,
                attempted,
            } => write!(
                formatter,
                "instrument-catalog {resource} capacity {maximum} cannot admit cardinality {attempted}"
            ),
            Self::NoEffectiveVersion { instrument_id, at } => write!(
                formatter,
                "instrument {instrument_id} has no version effective at {}",
                at.as_unix_nanos()
            ),
            Self::UnknownInstrument(instrument_id) => {
                write!(formatter, "unknown instrument {instrument_id}")
            }
            Self::UnknownInstrumentVersion {
                instrument_id,
                version,
            } => write!(
                formatter,
                "unknown version {version} for instrument {instrument_id}"
            ),
        }
    }
}

impl std::error::Error for InstrumentError {}

/// Order-admission failure against one immutable instrument version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionError {
    /// Command instrument did not match the definition.
    WrongInstrument,
    /// Command version did not match the definition.
    WrongVersion,
    /// New orders and replacements are disabled in the current trading state.
    TradingStateDisallowsEntry,
    /// Limit price was not a multiple of tick size.
    PriceOffTickGrid,
    /// Limit price was outside the inclusive collar.
    PriceOutsideCollar,
    /// Quantity was not a multiple of the configured increment.
    QuantityOffGrid,
    /// Quantity was outside inclusive order limits.
    QuantityOutsideLimits,
    /// This instrument version does not admit reserve orders.
    ReserveOrderNotSupported,
    /// Display quantity was not aligned to the lot increment.
    DisplayQuantityOffGrid,
    /// Display quantity was not smaller than total order quantity.
    DisplayQuantityNotLessThanOrder,
    /// The requested peak implies too many replenishments for the admitted state.
    ReserveReplenishmentLimit,
}

/// Fixed-capacity canonical asset code.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AssetCode {
    bytes: [u8; MAX_CODE_LENGTH],
    length: u8,
}

impl AssetCode {
    /// Validates an uppercase ASCII asset code.
    ///
    /// Accepted bytes are `A-Z`, `0-9`, `.`, `_`, and `-`.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError`] for empty, oversized, or noncanonical input.
    pub fn new(value: &str) -> Result<Self, InstrumentError> {
        validate_text("asset code", value.as_bytes(), MAX_CODE_LENGTH)?;
        let mut bytes = [0_u8; MAX_CODE_LENGTH];
        bytes[..value.len()].copy_from_slice(value.as_bytes());
        let length = u8::try_from(value.len()).map_err(|_| InstrumentError::TextTooLong {
            field: "asset code",
            length: value.len(),
            maximum: MAX_CODE_LENGTH,
        })?;
        Ok(Self { bytes, length })
    }

    /// Returns the canonical code.
    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..usize::from(self.length)]).unwrap_or_default()
    }
}

impl fmt::Debug for AssetCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(formatter)
    }
}

impl fmt::Display for AssetCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Immutable asset denomination metadata.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AssetDefinition {
    /// Stable asset identifier.
    pub asset_id: AssetId,
    /// Canonical asset code.
    pub code: AssetCode,
    /// Decimal digits used to display one integer ledger unit.
    pub decimal_scale: u8,
}

impl AssetDefinition {
    /// Constructs validated asset metadata.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError::DecimalScaleTooLarge`] above 18 digits.
    pub fn new(
        asset_id: AssetId,
        code: AssetCode,
        decimal_scale: u8,
    ) -> Result<Self, InstrumentError> {
        if decimal_scale > MAX_DECIMAL_SCALE {
            Err(InstrumentError::DecimalScaleTooLarge(decimal_scale))
        } else {
            Ok(Self {
                asset_id,
                code,
                decimal_scale,
            })
        }
    }
}

/// Fixed-capacity canonical instrument symbol.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstrumentSymbol {
    bytes: [u8; MAX_SYMBOL_LENGTH],
    length: u8,
}

impl InstrumentSymbol {
    /// Validates a canonical uppercase ASCII instrument symbol.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError`] for empty, oversized, or noncanonical input.
    pub fn new(value: &str) -> Result<Self, InstrumentError> {
        validate_text("instrument symbol", value.as_bytes(), MAX_SYMBOL_LENGTH)?;
        let mut bytes = [0_u8; MAX_SYMBOL_LENGTH];
        bytes[..value.len()].copy_from_slice(value.as_bytes());
        let length = u8::try_from(value.len()).map_err(|_| InstrumentError::TextTooLong {
            field: "instrument symbol",
            length: value.len(),
            maximum: MAX_SYMBOL_LENGTH,
        })?;
        Ok(Self { bytes, length })
    }

    /// Returns the canonical symbol.
    #[must_use]
    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes[..usize::from(self.length)]).unwrap_or_default()
    }
}

impl fmt::Debug for InstrumentSymbol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(formatter)
    }
}

impl fmt::Display for InstrumentSymbol {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Broad economic instrument classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstrumentKind {
    /// Cash equity.
    Equity,
    /// Spot asset pair.
    Spot,
    /// Futures contract.
    Future,
    /// Option contract.
    Option,
    /// Debt security.
    Bond,
    /// Swap contract.
    Swap,
    /// Nontradable or reference index.
    Index,
    /// Explicitly defined synthetic product.
    Synthetic,
}

/// Order-entry state for one immutable definition version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TradingState {
    /// Continuous order entry and matching are enabled.
    Open,
    /// Only cancellation is enabled.
    CancelOnly,
    /// All entry and amendment are disabled; cancellation remains enabled.
    Halted,
    /// Session is closed; cancellation remains enabled for deterministic cleanup.
    Closed,
}

impl TradingState {
    const fn permits_entry(self) -> bool {
        matches!(self, Self::Open)
    }
}

/// Inclusive fixed-point price rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PriceRules {
    decimal_scale: u8,
    tick_size: u64,
    minimum: Price,
    maximum: Price,
}

impl PriceRules {
    /// Constructs validated price scale, tick, and collar rules.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError`] for invalid scales, ticks, ranges, or collar
    /// boundaries that are not on the tick grid.
    pub fn new(
        decimal_scale: u8,
        tick_size: u64,
        minimum: Price,
        maximum: Price,
    ) -> Result<Self, InstrumentError> {
        if decimal_scale > MAX_DECIMAL_SCALE {
            return Err(InstrumentError::DecimalScaleTooLarge(decimal_scale));
        }
        let tick = i64::try_from(tick_size).map_err(|_| InstrumentError::InvalidTickSize)?;
        if tick == 0 {
            return Err(InstrumentError::InvalidTickSize);
        }
        if minimum.raw() > maximum.raw() {
            return Err(InstrumentError::InvalidPriceRange);
        }
        if minimum.raw() % tick != 0 || maximum.raw() % tick != 0 {
            return Err(InstrumentError::CollarOffTickGrid);
        }
        Ok(Self {
            decimal_scale,
            tick_size,
            minimum,
            maximum,
        })
    }

    /// Returns display decimal digits.
    #[must_use]
    pub const fn decimal_scale(self) -> u8 {
        self.decimal_scale
    }

    /// Returns the positive raw-price increment.
    #[must_use]
    pub const fn tick_size(self) -> u64 {
        self.tick_size
    }

    /// Returns the inclusive lower collar.
    #[must_use]
    pub const fn minimum(self) -> Price {
        self.minimum
    }

    /// Returns the inclusive upper collar.
    #[must_use]
    pub const fn maximum(self) -> Price {
        self.maximum
    }

    /// Validates one limit price.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionError`] when `price` is outside the inclusive collar
    /// or not aligned to the tick grid.
    pub fn validate(self, price: Price) -> Result<(), AdmissionError> {
        if price.raw() < self.minimum.raw() || price.raw() > self.maximum.raw() {
            return Err(AdmissionError::PriceOutsideCollar);
        }
        let Ok(tick) = i64::try_from(self.tick_size) else {
            return Err(AdmissionError::PriceOffTickGrid);
        };
        if price.raw() % tick != 0 {
            return Err(AdmissionError::PriceOffTickGrid);
        }
        Ok(())
    }
}

/// Inclusive lot-quantity rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QuantityRules {
    increment: u64,
    minimum: u64,
    maximum: u64,
}

impl QuantityRules {
    /// Constructs validated lot-grid and order-size limits.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError`] for zero, inverted, or off-grid boundaries.
    pub const fn new(
        increment_lots: u64,
        minimum_lots: u64,
        maximum_lots: u64,
    ) -> Result<Self, InstrumentError> {
        if increment_lots == 0
            || minimum_lots == 0
            || maximum_lots == 0
            || minimum_lots > maximum_lots
        {
            return Err(InstrumentError::InvalidQuantityRule);
        }
        if minimum_lots % increment_lots != 0 || maximum_lots % increment_lots != 0 {
            return Err(InstrumentError::QuantityBoundaryOffGrid);
        }
        Ok(Self {
            increment: increment_lots,
            minimum: minimum_lots,
            maximum: maximum_lots,
        })
    }

    /// Returns the lot increment.
    #[must_use]
    pub const fn increment_lots(self) -> u64 {
        self.increment
    }

    /// Returns the inclusive minimum order lots.
    #[must_use]
    pub const fn minimum_lots(self) -> u64 {
        self.minimum
    }

    /// Returns the inclusive maximum order lots.
    #[must_use]
    pub const fn maximum_lots(self) -> u64 {
        self.maximum
    }

    /// Validates one order quantity.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionError`] when the quantity is outside the inclusive
    /// limits or not aligned to the lot increment.
    pub const fn validate(self, quantity: Quantity) -> Result<(), AdmissionError> {
        let lots = quantity.lots();
        if lots < self.minimum || lots > self.maximum {
            return Err(AdmissionError::QuantityOutsideLimits);
        }
        if lots % self.increment != 0 {
            return Err(AdmissionError::QuantityOffGrid);
        }
        Ok(())
    }

    /// Validates positive active leaves after one or more executions.
    ///
    /// The original order-entry maximum and lot grid remain authoritative, but
    /// partial execution may leave less than the minimum admissible new-order
    /// quantity.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionError`] when leaves exceed the order maximum or are
    /// not aligned to the lot increment.
    pub const fn validate_leaves(self, quantity: Quantity) -> Result<(), AdmissionError> {
        let lots = quantity.lots();
        if lots > self.maximum {
            return Err(AdmissionError::QuantityOutsideLimits);
        }
        if lots % self.increment != 0 {
            return Err(AdmissionError::QuantityOffGrid);
        }
        Ok(())
    }

    const fn display_is_aligned(self, quantity: Quantity) -> bool {
        quantity.lots() % self.increment == 0
    }
}

/// Immutable native reserve-order admission bounds for one instrument version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReserveOrderRules {
    maximum_replenishments: u32,
}

impl ReserveOrderRules {
    /// Disables native reserve orders.
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            maximum_replenishments: 0,
        }
    }

    /// Enables reserve orders with a bounded number of peak refreshes per
    /// admitted quantity/display state.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError::InvalidReserveOrderRules`] for zero.
    pub const fn new(maximum_replenishments: u32) -> Result<Self, InstrumentError> {
        if maximum_replenishments == 0 {
            Err(InstrumentError::InvalidReserveOrderRules)
        } else {
            Ok(Self {
                maximum_replenishments,
            })
        }
    }

    /// Returns whether native reserve orders are admitted.
    #[must_use]
    pub const fn enabled(self) -> bool {
        self.maximum_replenishments != 0
    }

    /// Returns the maximum number of replenishments after the initial peak.
    #[must_use]
    pub const fn maximum_replenishments(self) -> u32 {
        self.maximum_replenishments
    }
}

/// Constructor input for one immutable instrument version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstrumentSpec {
    /// Stable instrument identifier.
    pub instrument_id: InstrumentId,
    /// Strictly increasing version.
    pub version: InstrumentVersion,
    /// First UTC nanosecond at which this version applies.
    pub effective_from: TimestampNs,
    /// Canonical symbol.
    pub symbol: InstrumentSymbol,
    /// Economic classification.
    pub kind: InstrumentKind,
    /// Delivered asset.
    pub base_asset_id: AssetId,
    /// Payment asset.
    pub quote_asset_id: AssetId,
    /// Price rules.
    pub price: PriceRules,
    /// Quantity rules.
    pub quantity: QuantityRules,
    /// Native reserve-order admission and per-state refresh bound.
    pub reserve: ReserveOrderRules,
    /// Base ledger units delivered by one lot.
    pub base_units_per_lot: u64,
    /// Quote ledger units per raw price unit times one lot.
    pub quote_units_per_price_unit: u64,
    /// Order-entry state.
    pub trading_state: TradingState,
}

/// Validated immutable instrument definition version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstrumentDefinition {
    spec: InstrumentSpec,
}

impl InstrumentDefinition {
    /// Validates an immutable instrument version.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError`] for identical assets or zero settlement
    /// conversion multipliers.
    pub fn new(spec: InstrumentSpec) -> Result<Self, InstrumentError> {
        if spec.base_asset_id == spec.quote_asset_id {
            return Err(InstrumentError::IdenticalAssets);
        }
        if spec.base_units_per_lot == 0 || spec.quote_units_per_price_unit == 0 {
            return Err(InstrumentError::ZeroSettlementMultiplier);
        }
        Ok(Self { spec })
    }

    /// Returns the stable instrument identifier.
    #[must_use]
    pub const fn instrument_id(self) -> InstrumentId {
        self.spec.instrument_id
    }

    /// Returns the immutable version identifier.
    #[must_use]
    pub const fn version(self) -> InstrumentVersion {
        self.spec.version
    }

    /// Returns the effective timestamp.
    #[must_use]
    pub const fn effective_from(self) -> TimestampNs {
        self.spec.effective_from
    }

    /// Returns the canonical symbol.
    #[must_use]
    pub const fn symbol(self) -> InstrumentSymbol {
        self.spec.symbol
    }

    /// Returns the instrument kind.
    #[must_use]
    pub const fn kind(self) -> InstrumentKind {
        self.spec.kind
    }

    /// Returns the base asset.
    #[must_use]
    pub const fn base_asset_id(self) -> AssetId {
        self.spec.base_asset_id
    }

    /// Returns the quote asset.
    #[must_use]
    pub const fn quote_asset_id(self) -> AssetId {
        self.spec.quote_asset_id
    }

    /// Returns the price rules.
    #[must_use]
    pub const fn price_rules(self) -> PriceRules {
        self.spec.price
    }

    /// Returns the quantity rules.
    #[must_use]
    pub const fn quantity_rules(self) -> QuantityRules {
        self.spec.quantity
    }

    /// Returns native reserve-order admission rules.
    #[must_use]
    pub const fn reserve_order_rules(self) -> ReserveOrderRules {
        self.spec.reserve
    }

    /// Returns the trading state.
    #[must_use]
    pub const fn trading_state(self) -> TradingState {
        self.spec.trading_state
    }

    /// Returns the canonical settlement convention.
    #[must_use]
    pub const fn settlement_convention(self) -> SettlementConvention {
        SettlementConvention {
            base_asset_id: self.spec.base_asset_id,
            quote_asset_id: self.spec.quote_asset_id,
            base_units_per_lot: self.spec.base_units_per_lot,
            quote_units_per_price_unit: self.spec.quote_units_per_price_unit,
        }
    }

    /// Validates command routing, version, trading state, price, and quantity.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionError`] for identity/version mismatch or a rule
    /// violation under this immutable definition.
    pub fn admit(self, command: Command) -> Result<(), AdmissionError> {
        self.admit_in_state(command, self.spec.trading_state)
    }

    /// Validates a command against an explicit effective trading state.
    ///
    /// The immutable definition supplies genesis state; sequenced matching
    /// controls supply the effective state after genesis.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionError`] for identity/version mismatch or a rule
    /// violation under this definition and effective state.
    pub fn admit_in_state(
        self,
        command: Command,
        trading_state: TradingState,
    ) -> Result<(), AdmissionError> {
        match command {
            Command::New(value) => self.admit_new(value, trading_state),
            Command::Cancel(value) => {
                self.validate_identity(value.instrument_id, value.instrument_version)
            }
            Command::Replace(value) => self.admit_replace(value, trading_state),
            Command::MassCancel(value) => {
                self.validate_identity(value.instrument_id, value.instrument_version)
            }
            Command::AccountControl(value) => {
                self.validate_identity(value.instrument_id, value.instrument_version)
            }
            Command::TradingStateControl(value) => {
                self.validate_identity(value.instrument_id, value.instrument_version)
            }
            Command::ExpirySweep(value) => {
                self.validate_identity(value.instrument_id, value.instrument_version)
            }
        }
    }

    /// Verifies that a trade can use this definition's settlement convention.
    ///
    /// # Errors
    ///
    /// Returns [`AdmissionError`] when the trade's instrument or version does
    /// not match this definition.
    pub fn settlement_for_trade(
        self,
        trade: &Trade,
    ) -> Result<SettlementConvention, AdmissionError> {
        self.validate_identity(trade.instrument_id, trade.instrument_version)?;
        Ok(self.settlement_convention())
    }

    fn admit_new(self, order: NewOrder, trading_state: TradingState) -> Result<(), AdmissionError> {
        self.validate_identity(order.instrument_id, order.instrument_version)?;
        if !trading_state.permits_entry() {
            return Err(AdmissionError::TradingStateDisallowsEntry);
        }
        self.spec.quantity.validate(order.quantity)?;
        self.validate_display(order.display, order.quantity)?;
        if let OrderType::Limit(price) = order.order_type {
            self.spec.price.validate(price)?;
        }
        Ok(())
    }

    fn admit_replace(
        self,
        order: ReplaceOrder,
        trading_state: TradingState,
    ) -> Result<(), AdmissionError> {
        self.validate_identity(order.instrument_id, order.instrument_version)?;
        if !trading_state.permits_entry() {
            return Err(AdmissionError::TradingStateDisallowsEntry);
        }
        self.spec.quantity.validate(order.new_quantity)?;
        self.validate_display(order.new_display, order.new_quantity)?;
        self.spec.price.validate(order.new_price)
    }

    fn validate_display(
        self,
        display: OrderDisplay,
        total: Quantity,
    ) -> Result<(), AdmissionError> {
        let OrderDisplay::Reserve { peak } = display else {
            return Ok(());
        };
        if !self.spec.reserve.enabled() {
            return Err(AdmissionError::ReserveOrderNotSupported);
        }
        if !self.spec.quantity.display_is_aligned(peak) {
            return Err(AdmissionError::DisplayQuantityOffGrid);
        }
        if peak.lots() >= total.lots() {
            return Err(AdmissionError::DisplayQuantityNotLessThanOrder);
        }
        let replenishments = (total.lots() - 1) / peak.lots();
        if replenishments > u64::from(self.spec.reserve.maximum_replenishments()) {
            return Err(AdmissionError::ReserveReplenishmentLimit);
        }
        Ok(())
    }

    fn validate_identity(
        self,
        instrument_id: InstrumentId,
        version: InstrumentVersion,
    ) -> Result<(), AdmissionError> {
        if instrument_id != self.spec.instrument_id {
            Err(AdmissionError::WrongInstrument)
        } else if version != self.spec.version {
            Err(AdmissionError::WrongVersion)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct InstrumentSeries {
    start: usize,
    length: usize,
}

impl InstrumentSeries {
    fn end(self) -> usize {
        self.checked_end()
            .expect("validated instrument series range must be representable")
    }

    const fn checked_end(self) -> Option<usize> {
        self.start.checked_add(self.length)
    }
}

/// One constructor-reserved instrument-catalog hash index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstrumentCatalogHashIndex {
    /// Stable asset identifier to immutable metadata.
    AssetIdentifiers,
    /// Canonical asset code to stable asset identifier.
    AssetCodes,
    /// Stable instrument identifier to its contiguous version range.
    Instruments,
}

/// Process-local fixed hash-index allocation telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstrumentCatalogHashIndexStatus {
    /// Configured semantic entry maximum.
    pub configured_entries: usize,
    /// Constructor-allocated dense-entry capacity.
    pub allocated_entries: usize,
    /// Initialized open-addressed lookup buckets.
    pub initialized_buckets: usize,
    /// Currently occupied entries.
    pub occupied_entries: usize,
}

/// Process-local immutable-definition arena telemetry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstrumentCatalogArenaStatus {
    /// Configured semantic definition maximum.
    pub configured_definitions: usize,
    /// Constructor-allocated definition capacity.
    pub allocated_definitions: usize,
    /// Currently occupied definition slots.
    pub occupied_definitions: usize,
}

/// Structural contradiction in authoritative catalog state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InstrumentCatalogInvariantViolation(&'static str);

impl InstrumentCatalogInvariantViolation {
    /// Returns the invariant diagnostic.
    #[must_use]
    pub const fn message(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for InstrumentCatalogInvariantViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for InstrumentCatalogInvariantViolation {}

/// Append-only, finitely bounded effective-time instrument and asset catalog.
///
/// Lookup first selects a contiguous per-instrument range through a bounded
/// hash index and then binary-searches that immutable history. Registration is
/// a control-plane operation: interleaved version appends can shift later
/// ranges within the constructor-reserved flat arena but never grow storage.
#[derive(Debug)]
pub struct InstrumentCatalog {
    limits: InstrumentCatalogLimits,
    assets: BoundedHashMap<AssetId, AssetDefinition>,
    asset_codes: BoundedHashMap<AssetCode, AssetId>,
    instruments: BoundedHashMap<InstrumentId, InstrumentSeries>,
    definitions: Vec<InstrumentDefinition>,
}

impl InstrumentCatalog {
    /// Creates an empty catalog under the finite default resource envelope.
    ///
    /// Production initialization that must report reservation failure uses
    /// [`InstrumentCatalog::try_with_limits`].
    ///
    /// # Panics
    ///
    /// Panics if the process cannot reserve the documented default envelope.
    #[must_use]
    pub fn new() -> Self {
        Self::try_with_limits(InstrumentCatalogLimitsSpec::default())
            .expect("default instrument-catalog envelope must be representable")
    }

    /// Creates an empty catalog after reserving every authoritative index and
    /// immutable-definition slot through its complete configured maximum.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentCatalogConstructionError`] before state exists when
    /// limits are contradictory or a complete fixed layout cannot be reserved.
    pub fn try_with_limits(
        spec: InstrumentCatalogLimitsSpec,
    ) -> Result<Self, InstrumentCatalogConstructionError> {
        let limits = InstrumentCatalogLimits::try_from(spec)?;
        let assets =
            reserve_catalog_map(spec.max_assets, InstrumentCatalogResource::AssetIdentifiers)?;
        let asset_codes =
            reserve_catalog_map(spec.max_assets, InstrumentCatalogResource::AssetCodes)?;
        let instruments =
            reserve_catalog_map(spec.max_instruments, InstrumentCatalogResource::Instruments)?;
        let mut definitions = Vec::new();
        definitions
            .try_reserve_exact(spec.max_instrument_versions)
            .map_err(|_| InstrumentCatalogConstructionError::ReservationFailed {
                resource: InstrumentCatalogResource::InstrumentVersions,
                maximum: spec.max_instrument_versions,
            })?;
        Ok(Self {
            limits,
            assets,
            asset_codes,
            instruments,
            definitions,
        })
    }

    /// Returns the immutable validated generation limits.
    #[must_use]
    pub const fn limits(&self) -> InstrumentCatalogLimits {
        self.limits
    }

    /// Returns fixed allocation and occupancy telemetry for one catalog index.
    #[must_use]
    pub fn hash_index_status(
        &self,
        index: InstrumentCatalogHashIndex,
    ) -> InstrumentCatalogHashIndexStatus {
        match index {
            InstrumentCatalogHashIndex::AssetIdentifiers => catalog_hash_status(&self.assets),
            InstrumentCatalogHashIndex::AssetCodes => catalog_hash_status(&self.asset_codes),
            InstrumentCatalogHashIndex::Instruments => catalog_hash_status(&self.instruments),
        }
    }

    /// Returns fixed allocation and occupancy telemetry for all definitions.
    #[must_use]
    pub fn definition_arena_status(&self) -> InstrumentCatalogArenaStatus {
        InstrumentCatalogArenaStatus {
            configured_definitions: self.limits.max_instrument_versions(),
            allocated_definitions: self.definitions.capacity(),
            occupied_definitions: self.definitions.len(),
        }
    }

    /// Registers immutable asset metadata.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError::DuplicateAsset`] for duplicate identifiers or codes.
    pub fn register_asset(&mut self, asset: AssetDefinition) -> Result<(), InstrumentError> {
        if self.assets.contains_key(&asset.asset_id) || self.asset_codes.contains_key(&asset.code) {
            return Err(InstrumentError::DuplicateAsset);
        }
        let attempted = self.assets.len() + 1;
        if attempted > self.limits.max_assets() {
            return Err(InstrumentError::CapacityExceeded {
                resource: InstrumentCatalogResource::AssetIdentifiers,
                maximum: self.limits.max_assets(),
                attempted,
            });
        }
        self.asset_codes.insert(asset.code, asset.asset_id);
        self.assets.insert(asset.asset_id, asset);
        Ok(())
    }

    /// Resolves immutable asset metadata by identifier.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError::UnknownAsset`] when the identifier is absent.
    pub fn asset(&self, asset_id: AssetId) -> Result<&AssetDefinition, InstrumentError> {
        self.assets
            .get(&asset_id)
            .ok_or(InstrumentError::UnknownAsset(asset_id))
    }

    /// Resolves immutable asset metadata by canonical code.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError::UnknownAssetCode`] when the code is absent.
    pub fn asset_by_code(&self, code: AssetCode) -> Result<&AssetDefinition, InstrumentError> {
        let asset_id = self
            .asset_codes
            .get(&code)
            .copied()
            .ok_or(InstrumentError::UnknownAssetCode(code))?;
        self.asset(asset_id)
    }

    /// Appends a strictly monotonic immutable instrument version.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError`] for unknown assets, duplicate/nonmonotonic
    /// versions, or changed identity fields.
    ///
    /// # Panics
    ///
    /// Panics only if previously validated private catalog structure or its
    /// constructor-owned definition reservation has been corrupted in memory.
    pub fn register_instrument(
        &mut self,
        definition: InstrumentDefinition,
    ) -> Result<(), InstrumentError> {
        for asset_id in [definition.base_asset_id(), definition.quote_asset_id()] {
            if !self.assets.contains_key(&asset_id) {
                return Err(InstrumentError::UnknownAsset(asset_id));
            }
        }
        let instrument_id = definition.instrument_id();
        let existing = self.instruments.get(&instrument_id).copied();
        if let Some(series) = existing {
            let versions = &self.definitions[series.start..series.end()];
            if versions
                .binary_search_by_key(&definition.version(), |value| value.version())
                .is_ok()
            {
                return Err(InstrumentError::DuplicateInstrumentVersion);
            }
            let previous = versions
                .last()
                .copied()
                .expect("registered instrument series must not be empty");
            if definition.version() <= previous.version()
                || definition.effective_from() <= previous.effective_from()
            {
                return Err(InstrumentError::NonMonotonicVersion);
            }
            if definition.symbol() != previous.symbol()
                || definition.kind() != previous.kind()
                || definition.base_asset_id() != previous.base_asset_id()
                || definition.quote_asset_id() != previous.quote_asset_id()
            {
                return Err(InstrumentError::IdentityChanged);
            }
        }

        if existing.is_none() && self.instruments.len() >= self.limits.max_instruments() {
            return Err(InstrumentError::CapacityExceeded {
                resource: InstrumentCatalogResource::Instruments,
                maximum: self.limits.max_instruments(),
                attempted: self.instruments.len() + 1,
            });
        }
        if self.definitions.len() >= self.limits.max_instrument_versions() {
            return Err(InstrumentError::CapacityExceeded {
                resource: InstrumentCatalogResource::InstrumentVersions,
                maximum: self.limits.max_instrument_versions(),
                attempted: self.definitions.len() + 1,
            });
        }

        assert!(
            self.definitions.len() < self.definitions.capacity(),
            "instrument-catalog definition reservation disappeared"
        );
        if let Some(series) = existing {
            let insertion_index = series.end();
            self.definitions.insert(insertion_index, definition);
            for later in self.instruments.values_mut() {
                if later.start >= insertion_index {
                    later.start += 1;
                }
            }
            self.instruments
                .get_mut(&instrument_id)
                .expect("registered instrument range must remain indexed")
                .length += 1;
        } else {
            let start = self.definitions.len();
            self.instruments
                .insert(instrument_id, InstrumentSeries { start, length: 1 });
            self.definitions.push(definition);
        }
        Ok(())
    }

    /// Resolves the version effective at `at` in `O(log V)` for `V` versions.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError`] when the instrument is unknown or no version
    /// was yet effective.
    pub fn effective(
        &self,
        instrument_id: InstrumentId,
        at: TimestampNs,
    ) -> Result<&InstrumentDefinition, InstrumentError> {
        let series = self
            .instruments
            .get(&instrument_id)
            .ok_or(InstrumentError::UnknownInstrument(instrument_id))?;
        let versions = &self.definitions[series.start..series.end()];
        let index = versions.partition_point(|value| value.effective_from() <= at);
        if index == 0 {
            Err(InstrumentError::NoEffectiveVersion { instrument_id, at })
        } else {
            Ok(&versions[index - 1])
        }
    }

    /// Resolves an exact immutable version in `O(log V)` for `V` versions.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentError`] when the instrument or version is absent.
    pub fn version(
        &self,
        instrument_id: InstrumentId,
        version: InstrumentVersion,
    ) -> Result<&InstrumentDefinition, InstrumentError> {
        let series = self
            .instruments
            .get(&instrument_id)
            .ok_or(InstrumentError::UnknownInstrument(instrument_id))?;
        let versions = &self.definitions[series.start..series.end()];
        let index = versions
            .binary_search_by_key(&version, |definition| definition.version())
            .map_err(|_| InstrumentError::UnknownInstrumentVersion {
                instrument_id,
                version,
            })?;
        Ok(&versions[index])
    }

    /// Audits fixed layouts, bidirectional asset indexes, version ranges,
    /// identity stability, monotonic histories, and complete arena coverage.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentCatalogInvariantViolation`] for any structural
    /// contradiction.
    pub fn validate(&self) -> Result<(), InstrumentCatalogInvariantViolation> {
        InstrumentCatalogLimits::new(self.limits.spec())
            .map_err(|_| invariant("instrument-catalog limits are contradictory"))?;
        self.assets
            .validate_layout()
            .map_err(|_| invariant("asset-identifier hash layout is invalid"))?;
        self.asset_codes
            .validate_layout()
            .map_err(|_| invariant("asset-code hash layout is invalid"))?;
        self.instruments
            .validate_layout()
            .map_err(|_| invariant("instrument hash layout is invalid"))?;
        if self.assets.len() != self.asset_codes.len() {
            return Err(invariant("asset identifier and code cardinalities differ"));
        }
        if self.definitions.len() > self.limits.max_instrument_versions()
            || self.definitions.capacity() < self.limits.max_instrument_versions()
        {
            return Err(invariant(
                "definition arena capacity or cardinality contradicts its maximum",
            ));
        }
        for (asset_id, asset) in self.assets.iter() {
            if asset.asset_id != *asset_id || self.asset_codes.get(&asset.code) != Some(asset_id) {
                return Err(invariant("asset identifier and code indexes disagree"));
            }
        }
        for (code, asset_id) in self.asset_codes.iter() {
            if self.assets.get(asset_id).map(|asset| asset.code) != Some(*code) {
                return Err(invariant("asset code references inconsistent metadata"));
            }
        }

        let mut covered = 0_usize;
        for (instrument_id, series) in self.instruments.iter() {
            let Some(series_end) = series.checked_end() else {
                return Err(invariant("instrument version range overflows its index"));
            };
            if series.length == 0 || series_end > self.definitions.len() {
                return Err(invariant(
                    "instrument version range is empty or out of bounds",
                ));
            }
            covered = covered
                .checked_add(series.length)
                .ok_or_else(|| invariant("instrument version cardinality overflowed"))?;
            for (other_id, other) in self.instruments.iter() {
                let Some(other_end) = other.checked_end() else {
                    return Err(invariant("instrument version range overflows its index"));
                };
                if instrument_id != other_id && series.start < other_end && other.start < series_end
                {
                    return Err(invariant("instrument version ranges overlap"));
                }
            }
            let versions = &self.definitions[series.start..series_end];
            let first = versions[0];
            if first.instrument_id() != *instrument_id {
                return Err(invariant("instrument range key and definition disagree"));
            }
            for definition in versions {
                if definition.instrument_id() != *instrument_id
                    || !same_instrument_identity(first, *definition)
                {
                    return Err(invariant("instrument identity changes within one range"));
                }
                if !self.assets.contains_key(&definition.base_asset_id())
                    || !self.assets.contains_key(&definition.quote_asset_id())
                {
                    return Err(invariant("instrument range references an unknown asset"));
                }
            }
            for pair in versions.windows(2) {
                if pair[0].version() >= pair[1].version()
                    || pair[0].effective_from() >= pair[1].effective_from()
                {
                    return Err(invariant("instrument history is not strictly monotonic"));
                }
            }
        }
        if covered != self.definitions.len() {
            return Err(invariant(
                "instrument ranges do not cover the definition arena exactly",
            ));
        }
        Ok(())
    }
}

impl Default for InstrumentCatalog {
    fn default() -> Self {
        Self::new()
    }
}

fn reserve_catalog_map<K, V>(
    maximum: usize,
    resource: InstrumentCatalogResource,
) -> Result<BoundedHashMap<K, V>, InstrumentCatalogConstructionError>
where
    K: Eq + Hash,
{
    BoundedHashMap::try_new(maximum)
        .map_err(|_| InstrumentCatalogConstructionError::ReservationFailed { resource, maximum })
}

fn catalog_hash_status<K, V>(map: &BoundedHashMap<K, V>) -> InstrumentCatalogHashIndexStatus
where
    K: Eq + Hash,
{
    InstrumentCatalogHashIndexStatus {
        configured_entries: map.maximum(),
        allocated_entries: map.capacity(),
        initialized_buckets: map.bucket_count(),
        occupied_entries: map.len(),
    }
}

fn same_instrument_identity(left: InstrumentDefinition, right: InstrumentDefinition) -> bool {
    left.symbol() == right.symbol()
        && left.kind() == right.kind()
        && left.base_asset_id() == right.base_asset_id()
        && left.quote_asset_id() == right.quote_asset_id()
}

const fn invariant(message: &'static str) -> InstrumentCatalogInvariantViolation {
    InstrumentCatalogInvariantViolation(message)
}

fn validate_text(field: &'static str, bytes: &[u8], maximum: usize) -> Result<(), InstrumentError> {
    if bytes.is_empty() {
        return Err(InstrumentError::EmptyText(field));
    }
    if bytes.len() > maximum {
        return Err(InstrumentError::TextTooLong {
            field,
            length: bytes.len(),
            maximum,
        });
    }
    for &byte in bytes {
        if !(byte.is_ascii_uppercase()
            || byte.is_ascii_digit()
            || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(InstrumentError::InvalidTextByte { field, byte });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::domain::InstrumentId;

    use super::{InstrumentCatalog, InstrumentCatalogLimitsSpec, InstrumentSeries};

    #[test]
    fn catalog_validation_detects_lost_definition_reservation() {
        let mut catalog = InstrumentCatalog::try_with_limits(InstrumentCatalogLimitsSpec {
            max_assets: 1,
            max_instruments: 1,
            max_instrument_versions: 1,
        })
        .unwrap();
        catalog.definitions.shrink_to_fit();
        assert_eq!(
            catalog.validate().unwrap_err().message(),
            "definition arena capacity or cardinality contradicts its maximum"
        );
    }

    #[test]
    fn catalog_validation_reports_overflowing_range_without_panicking() {
        let mut catalog = InstrumentCatalog::try_with_limits(InstrumentCatalogLimitsSpec {
            max_assets: 1,
            max_instruments: 1,
            max_instrument_versions: 1,
        })
        .unwrap();
        catalog.instruments.insert(
            InstrumentId::new(1).unwrap(),
            InstrumentSeries {
                start: usize::MAX,
                length: 2,
            },
        );
        assert_eq!(
            catalog.validate().unwrap_err().message(),
            "instrument version range overflows its index"
        );
    }
}
