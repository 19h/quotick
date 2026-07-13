//! Immutable asset metadata and effective-time-versioned instrument rules.

use std::collections::HashMap;
use std::fmt;

use crate::domain::{AssetId, InstrumentId, InstrumentVersion, Price, Quantity, TimestampNs};
use crate::ledger::SettlementConvention;
use crate::matching::{Command, NewOrder, OrderType, ReplaceOrder, Trade};

const MAX_CODE_LENGTH: usize = 16;
const MAX_SYMBOL_LENGTH: usize = 32;
const MAX_DECIMAL_SCALE: u8 = 18;

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
        match command {
            Command::New(value) => self.admit_new(value),
            Command::Cancel(value) => {
                self.validate_identity(value.instrument_id, value.instrument_version)
            }
            Command::Replace(value) => self.admit_replace(value),
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

    fn admit_new(self, order: NewOrder) -> Result<(), AdmissionError> {
        self.validate_identity(order.instrument_id, order.instrument_version)?;
        if !self.spec.trading_state.permits_entry() {
            return Err(AdmissionError::TradingStateDisallowsEntry);
        }
        self.spec.quantity.validate(order.quantity)?;
        if let OrderType::Limit(price) = order.order_type {
            self.spec.price.validate(price)?;
        }
        Ok(())
    }

    fn admit_replace(self, order: ReplaceOrder) -> Result<(), AdmissionError> {
        self.validate_identity(order.instrument_id, order.instrument_version)?;
        if !self.spec.trading_state.permits_entry() {
            return Err(AdmissionError::TradingStateDisallowsEntry);
        }
        self.spec.quantity.validate(order.new_quantity)?;
        self.spec.price.validate(order.new_price)
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

/// Append-only effective-time instrument and asset catalog.
#[derive(Debug, Default)]
pub struct InstrumentCatalog {
    assets: HashMap<AssetId, AssetDefinition>,
    asset_codes: HashMap<AssetCode, AssetId>,
    instruments: HashMap<InstrumentId, Vec<InstrumentDefinition>>,
}

impl InstrumentCatalog {
    /// Creates an empty catalog.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
    pub fn register_instrument(
        &mut self,
        definition: InstrumentDefinition,
    ) -> Result<(), InstrumentError> {
        for asset_id in [definition.base_asset_id(), definition.quote_asset_id()] {
            if !self.assets.contains_key(&asset_id) {
                return Err(InstrumentError::UnknownAsset(asset_id));
            }
        }
        let versions = self
            .instruments
            .entry(definition.instrument_id())
            .or_default();
        if versions
            .binary_search_by_key(&definition.version(), |value| value.version())
            .is_ok()
        {
            return Err(InstrumentError::DuplicateInstrumentVersion);
        }
        if let Some(previous) = versions.last().copied() {
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
        versions.push(definition);
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
        let versions = self
            .instruments
            .get(&instrument_id)
            .ok_or(InstrumentError::UnknownInstrument(instrument_id))?;
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
        let versions = self
            .instruments
            .get(&instrument_id)
            .ok_or(InstrumentError::UnknownInstrument(instrument_id))?;
        let index = versions
            .binary_search_by_key(&version, |definition| definition.version())
            .map_err(|_| InstrumentError::UnknownInstrumentVersion {
                instrument_id,
                version,
            })?;
        Ok(&versions[index])
    }
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
