//! Stable, allocation-bounded binary codecs for durable financial records.
//!
//! All multibyte integers are little-endian. Enum discriminants are explicit
//! `u8` values and are independent of Rust's in-memory representation. The WAL
//! frame version identifies the codec version.

use std::fmt;

use crate::domain::{
    AccountId, AccountingDate, AssetId, CommandId, DomainError, InstrumentId, InstrumentVersion,
    OrderId, Price, Quantity, Side, TimestampNs, TradeId, TransactionId,
};
use crate::instrument::{
    InstrumentDefinition, InstrumentError, InstrumentKind, InstrumentSpec, InstrumentSymbol,
    PriceRules, QuantityRules, ReserveOrderRules, TradingState,
};
use crate::ledger::{
    JournalEntry, LedgerBalance, LedgerCheckpoint, LedgerCheckpointError, LedgerCorrection,
    LedgerEntryKind, LedgerError, LedgerRecord, Posting,
};
use crate::market_data::{
    MarketDataError, MarketDataKind, MarketDataLevel, MarketDataSnapshot, MarketDataUpdate,
    TradePrint,
};
use crate::matching::{
    CancelOrder, CancelReason, Command, CommandOutcome, CommandReportCheckpoint, Event, EventKind,
    ExecutionReport, MassCancel, MassCancelScope, NewOrder, OrderBookCheckpoint,
    OrderBookCheckpointError, OrderDisplay, OrderType, RejectReason, ReplaceOrder,
    RestingOrderCheckpoint, SelfTradePrevention, TimeInForce, Trade,
};
use crate::risk::{
    AccountRiskDefinition, AccountRiskState, RiskAccountCheckpoint, RiskError, RiskLimitSpec,
    RiskLimits, RiskManagedCheckpoint, RiskManagedCheckpointError, RiskProfile, RiskSnapshot,
};

/// A deterministic binary encoding or validation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CodecError {
    /// The input ended before the requested value was complete.
    UnexpectedEnd {
        /// Bytes required for the value.
        needed: usize,
        /// Bytes remaining in the input.
        remaining: usize,
    },
    /// Bytes remained after decoding one complete value.
    TrailingBytes(usize),
    /// An enum discriminant was not defined by this codec version.
    InvalidTag {
        /// Logical enum or union name.
        type_name: &'static str,
        /// Unsupported discriminant.
        tag: u8,
    },
    /// A boolean was encoded with a value other than zero or one.
    InvalidBoolean(u8),
    /// A collection cannot be represented by the `u32` wire length.
    LengthOverflow {
        /// Encoded collection name.
        field: &'static str,
        /// In-memory collection length.
        length: usize,
    },
    /// A declared collection length cannot fit in the remaining payload.
    InvalidLength {
        /// Encoded collection name.
        field: &'static str,
        /// Declared element count.
        length: usize,
    },
    /// A decoded identifier or quantity violated its domain invariant.
    InvalidDomain(DomainError),
    /// A decoded value violated a cross-field invariant.
    InvalidValue(&'static str),
    /// A decoded journal entry violated accounting invariants.
    InvalidJournalEntry(LedgerError),
    /// A decoded ledger checkpoint failed replay or redundant-state validation.
    InvalidLedgerCheckpoint(LedgerCheckpointError),
    /// A decoded instrument definition violated instrument-master invariants.
    InvalidInstrument(InstrumentError),
    /// A decoded risk definition violated limit invariants.
    InvalidRisk(RiskError),
    /// Decoded public market data violated sequence-independent invariants.
    InvalidMarketData(MarketDataError),
    /// Decoded matching checkpoint violated semantic or structural invariants.
    InvalidMatchingCheckpoint(OrderBookCheckpointError),
    /// Decoded coupled risk/matching checkpoint violated semantic invariants.
    InvalidRiskCheckpoint(RiskManagedCheckpointError),
}

impl fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEnd { needed, remaining } => write!(
                formatter,
                "binary value requires {needed} bytes but only {remaining} remain"
            ),
            Self::TrailingBytes(length) => {
                write!(
                    formatter,
                    "{length} trailing bytes follow the decoded value"
                )
            }
            Self::InvalidTag { type_name, tag } => {
                write!(formatter, "invalid {type_name} tag {tag}")
            }
            Self::InvalidBoolean(value) => write!(formatter, "invalid boolean value {value}"),
            Self::LengthOverflow { field, length } => {
                write!(formatter, "{field} length {length} exceeds the wire limit")
            }
            Self::InvalidLength { field, length } => {
                write!(formatter, "declared {field} length {length} is impossible")
            }
            Self::InvalidDomain(error) => error.fmt(formatter),
            Self::InvalidValue(detail) => formatter.write_str(detail),
            Self::InvalidJournalEntry(error) => error.fmt(formatter),
            Self::InvalidLedgerCheckpoint(error) => error.fmt(formatter),
            Self::InvalidInstrument(error) => error.fmt(formatter),
            Self::InvalidRisk(error) => error.fmt(formatter),
            Self::InvalidMarketData(error) => error.fmt(formatter),
            Self::InvalidMatchingCheckpoint(error) => error.fmt(formatter),
            Self::InvalidRiskCheckpoint(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidDomain(error) => Some(error),
            Self::InvalidJournalEntry(error) => Some(error),
            Self::InvalidLedgerCheckpoint(error) => Some(error),
            Self::InvalidInstrument(error) => Some(error),
            Self::InvalidRisk(error) => Some(error),
            Self::InvalidMarketData(error) => Some(error),
            Self::InvalidMatchingCheckpoint(error) => Some(error),
            Self::InvalidRiskCheckpoint(error) => Some(error),
            _ => None,
        }
    }
}

impl From<DomainError> for CodecError {
    fn from(error: DomainError) -> Self {
        Self::InvalidDomain(error)
    }
}

impl From<InstrumentError> for CodecError {
    fn from(error: InstrumentError) -> Self {
        Self::InvalidInstrument(error)
    }
}

impl From<RiskError> for CodecError {
    fn from(error: RiskError) -> Self {
        Self::InvalidRisk(error)
    }
}

impl From<MarketDataError> for CodecError {
    fn from(error: MarketDataError) -> Self {
        Self::InvalidMarketData(error)
    }
}

impl From<LedgerCheckpointError> for CodecError {
    fn from(error: LedgerCheckpointError) -> Self {
        Self::InvalidLedgerCheckpoint(error)
    }
}

impl From<OrderBookCheckpointError> for CodecError {
    fn from(error: OrderBookCheckpointError) -> Self {
        Self::InvalidMatchingCheckpoint(error)
    }
}

impl From<RiskManagedCheckpointError> for CodecError {
    fn from(error: RiskManagedCheckpointError) -> Self {
        Self::InvalidRiskCheckpoint(error)
    }
}

/// A stable complete-value binary codec.
pub trait BinaryCodec: Sized {
    /// Encodes this value using the current stable payload format.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError::LengthOverflow`] when a collection exceeds the
    /// format's `u32` element-count limit.
    fn encode(&self) -> Result<Vec<u8>, CodecError>;

    /// Decodes one complete value and rejects trailing bytes.
    ///
    /// # Errors
    ///
    /// Returns [`CodecError`] for truncation, invalid discriminants, invalid
    /// domain values, impossible lengths, or trailing bytes.
    fn decode(bytes: &[u8]) -> Result<Self, CodecError>;
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn bool(&mut self, value: bool) {
        self.u8(u8::from(value));
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn i32(&mut self, value: i32) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn i128(&mut self, value: i128) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_le_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn length(&mut self, field: &'static str, length: usize) -> Result<(), CodecError> {
        let wire =
            u32::try_from(length).map_err(|_| CodecError::LengthOverflow { field, length })?;
        self.u32(wire);
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn take<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let remaining = self.remaining();
        if remaining < N {
            return Err(CodecError::UnexpectedEnd {
                needed: N,
                remaining,
            });
        }
        let end = self.position + N;
        let mut value = [0_u8; N];
        value.copy_from_slice(&self.bytes[self.position..end]);
        self.position = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, CodecError> {
        Ok(self.take::<1>()?[0])
    }

    fn bool(&mut self) -> Result<bool, CodecError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(CodecError::InvalidBoolean(value)),
        }
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_le_bytes(self.take()?))
    }

    fn i32(&mut self) -> Result<i32, CodecError> {
        Ok(i32::from_le_bytes(self.take()?))
    }

    fn u64(&mut self) -> Result<u64, CodecError> {
        Ok(u64::from_le_bytes(self.take()?))
    }

    fn i64(&mut self) -> Result<i64, CodecError> {
        Ok(i64::from_le_bytes(self.take()?))
    }

    fn i128(&mut self) -> Result<i128, CodecError> {
        Ok(i128::from_le_bytes(self.take()?))
    }

    fn u128(&mut self) -> Result<u128, CodecError> {
        Ok(u128::from_le_bytes(self.take()?))
    }

    fn bytes(&mut self, length: usize) -> Result<&'a [u8], CodecError> {
        let remaining = self.remaining();
        if remaining < length {
            return Err(CodecError::UnexpectedEnd {
                needed: length,
                remaining,
            });
        }
        let start = self.position;
        self.position += length;
        Ok(&self.bytes[start..self.position])
    }

    fn sized_bytes(&mut self, field: &'static str) -> Result<&'a [u8], CodecError> {
        let length = usize::try_from(self.u32()?).map_err(|_| CodecError::InvalidLength {
            field,
            length: usize::MAX,
        })?;
        self.bytes(length)
    }

    fn count(
        &mut self,
        field: &'static str,
        minimum_item_size: usize,
    ) -> Result<usize, CodecError> {
        let length = usize::try_from(self.u32()?).map_err(|_| CodecError::InvalidLength {
            field,
            length: usize::MAX,
        })?;
        if minimum_item_size != 0 && length > self.remaining() / minimum_item_size {
            return Err(CodecError::InvalidLength { field, length });
        }
        Ok(length)
    }

    const fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    fn finish(self) -> Result<(), CodecError> {
        let remaining = self.remaining();
        if remaining == 0 {
            Ok(())
        } else {
            Err(CodecError::TrailingBytes(remaining))
        }
    }
}

fn encode_side(encoder: &mut Encoder, side: Side) {
    encoder.u8(match side {
        Side::Buy => 0,
        Side::Sell => 1,
    });
}

fn decode_side(decoder: &mut Decoder<'_>) -> Result<Side, CodecError> {
    match decoder.u8()? {
        0 => Ok(Side::Buy),
        1 => Ok(Side::Sell),
        tag => Err(CodecError::InvalidTag {
            type_name: "Side",
            tag,
        }),
    }
}

fn encode_order_type(encoder: &mut Encoder, order_type: OrderType) {
    match order_type {
        OrderType::Market => encoder.u8(0),
        OrderType::Limit(price) => {
            encoder.u8(1);
            encoder.i64(price.raw());
        }
    }
}

fn decode_order_type(decoder: &mut Decoder<'_>) -> Result<OrderType, CodecError> {
    match decoder.u8()? {
        0 => Ok(OrderType::Market),
        1 => Ok(OrderType::Limit(Price::from_raw(decoder.i64()?))),
        tag => Err(CodecError::InvalidTag {
            type_name: "OrderType",
            tag,
        }),
    }
}

fn encode_order_display(encoder: &mut Encoder, display: OrderDisplay) {
    match display {
        OrderDisplay::FullyDisplayed => encoder.u8(0),
        OrderDisplay::Reserve { peak } => {
            encoder.u8(1);
            encoder.u64(peak.lots());
        }
    }
}

fn decode_order_display(decoder: &mut Decoder<'_>) -> Result<OrderDisplay, CodecError> {
    match decoder.u8()? {
        0 => Ok(OrderDisplay::FullyDisplayed),
        1 => Ok(OrderDisplay::Reserve {
            peak: quantity(decoder)?,
        }),
        tag => Err(CodecError::InvalidTag {
            type_name: "OrderDisplay",
            tag,
        }),
    }
}

fn encode_mass_cancel_scope(encoder: &mut Encoder, scope: MassCancelScope) {
    match scope {
        MassCancelScope::All => encoder.u8(0),
        MassCancelScope::Side(side) => {
            encoder.u8(1);
            encode_side(encoder, side);
        }
    }
}

fn decode_mass_cancel_scope(decoder: &mut Decoder<'_>) -> Result<MassCancelScope, CodecError> {
    match decoder.u8()? {
        0 => Ok(MassCancelScope::All),
        1 => Ok(MassCancelScope::Side(decode_side(decoder)?)),
        tag => Err(CodecError::InvalidTag {
            type_name: "MassCancelScope",
            tag,
        }),
    }
}

fn encode_time_in_force(encoder: &mut Encoder, value: TimeInForce) {
    encoder.u8(match value {
        TimeInForce::GoodTilCancelled => 0,
        TimeInForce::ImmediateOrCancel => 1,
        TimeInForce::FillOrKill => 2,
        TimeInForce::PostOnly => 3,
    });
}

fn decode_time_in_force(decoder: &mut Decoder<'_>) -> Result<TimeInForce, CodecError> {
    match decoder.u8()? {
        0 => Ok(TimeInForce::GoodTilCancelled),
        1 => Ok(TimeInForce::ImmediateOrCancel),
        2 => Ok(TimeInForce::FillOrKill),
        3 => Ok(TimeInForce::PostOnly),
        tag => Err(CodecError::InvalidTag {
            type_name: "TimeInForce",
            tag,
        }),
    }
}

fn encode_stp(encoder: &mut Encoder, value: SelfTradePrevention) {
    encoder.u8(match value {
        SelfTradePrevention::CancelAggressor => 0,
        SelfTradePrevention::CancelResting => 1,
        SelfTradePrevention::CancelBoth => 2,
        SelfTradePrevention::DecrementAndCancel => 3,
    });
}

fn decode_stp(decoder: &mut Decoder<'_>) -> Result<SelfTradePrevention, CodecError> {
    match decoder.u8()? {
        0 => Ok(SelfTradePrevention::CancelAggressor),
        1 => Ok(SelfTradePrevention::CancelResting),
        2 => Ok(SelfTradePrevention::CancelBoth),
        3 => Ok(SelfTradePrevention::DecrementAndCancel),
        tag => Err(CodecError::InvalidTag {
            type_name: "SelfTradePrevention",
            tag,
        }),
    }
}

fn encode_reject_reason(encoder: &mut Encoder, value: RejectReason) {
    encoder.u8(match value {
        RejectReason::WrongInstrument => 0,
        RejectReason::DuplicateOrder => 1,
        RejectReason::UnknownOrder => 2,
        RejectReason::NotOrderOwner => 3,
        RejectReason::MarketOrderCannotRest => 4,
        RejectReason::MarketOrderCannotPost => 5,
        RejectReason::UnsupportedFokSelfTradePolicy => 6,
        RejectReason::InsufficientLiquidity => 7,
        RejectReason::PostOnlyWouldCross => 8,
        RejectReason::WrongInstrumentVersion => 9,
        RejectReason::InstrumentNotOpen => 10,
        RejectReason::PriceOffTickGrid => 11,
        RejectReason::PriceOutsideCollar => 12,
        RejectReason::QuantityOffGrid => 13,
        RejectReason::QuantityOutsideLimits => 14,
        RejectReason::RiskProfileMissing => 15,
        RejectReason::RiskAccountBlocked => 16,
        RejectReason::RiskReduceOnly => 17,
        RejectReason::RiskOrderQuantityLimit => 18,
        RejectReason::RiskOrderNotionalLimit => 19,
        RejectReason::RiskOpenOrderCountLimit => 20,
        RejectReason::RiskOpenQuantityLimit => 21,
        RejectReason::RiskOpenNotionalLimit => 22,
        RejectReason::RiskPositionLimit => 23,
        RejectReason::RiskArithmeticOverflow => 24,
        RejectReason::ReserveOrderNotSupported => 25,
        RejectReason::DisplayQuantityOffGrid => 26,
        RejectReason::DisplayQuantityNotLessThanOrder => 27,
        RejectReason::ReserveReplenishmentLimit => 28,
        RejectReason::ReserveOrderCannotBeImmediate => 29,
        RejectReason::OrderDisplayModeChangeNotAllowed => 30,
    });
}

fn decode_reject_reason(decoder: &mut Decoder<'_>) -> Result<RejectReason, CodecError> {
    match decoder.u8()? {
        0 => Ok(RejectReason::WrongInstrument),
        1 => Ok(RejectReason::DuplicateOrder),
        2 => Ok(RejectReason::UnknownOrder),
        3 => Ok(RejectReason::NotOrderOwner),
        4 => Ok(RejectReason::MarketOrderCannotRest),
        5 => Ok(RejectReason::MarketOrderCannotPost),
        6 => Ok(RejectReason::UnsupportedFokSelfTradePolicy),
        7 => Ok(RejectReason::InsufficientLiquidity),
        8 => Ok(RejectReason::PostOnlyWouldCross),
        9 => Ok(RejectReason::WrongInstrumentVersion),
        10 => Ok(RejectReason::InstrumentNotOpen),
        11 => Ok(RejectReason::PriceOffTickGrid),
        12 => Ok(RejectReason::PriceOutsideCollar),
        13 => Ok(RejectReason::QuantityOffGrid),
        14 => Ok(RejectReason::QuantityOutsideLimits),
        15 => Ok(RejectReason::RiskProfileMissing),
        16 => Ok(RejectReason::RiskAccountBlocked),
        17 => Ok(RejectReason::RiskReduceOnly),
        18 => Ok(RejectReason::RiskOrderQuantityLimit),
        19 => Ok(RejectReason::RiskOrderNotionalLimit),
        20 => Ok(RejectReason::RiskOpenOrderCountLimit),
        21 => Ok(RejectReason::RiskOpenQuantityLimit),
        22 => Ok(RejectReason::RiskOpenNotionalLimit),
        23 => Ok(RejectReason::RiskPositionLimit),
        24 => Ok(RejectReason::RiskArithmeticOverflow),
        25 => Ok(RejectReason::ReserveOrderNotSupported),
        26 => Ok(RejectReason::DisplayQuantityOffGrid),
        27 => Ok(RejectReason::DisplayQuantityNotLessThanOrder),
        28 => Ok(RejectReason::ReserveReplenishmentLimit),
        29 => Ok(RejectReason::ReserveOrderCannotBeImmediate),
        30 => Ok(RejectReason::OrderDisplayModeChangeNotAllowed),
        tag => Err(CodecError::InvalidTag {
            type_name: "RejectReason",
            tag,
        }),
    }
}

fn encode_cancel_reason(encoder: &mut Encoder, value: CancelReason) {
    encoder.u8(match value {
        CancelReason::UserRequested => 0,
        CancelReason::UnfilledRemainder => 1,
        CancelReason::SelfTradeAggressor => 2,
        CancelReason::SelfTradeResting => 3,
        CancelReason::MassCancel => 4,
    });
}

fn decode_cancel_reason(decoder: &mut Decoder<'_>) -> Result<CancelReason, CodecError> {
    match decoder.u8()? {
        0 => Ok(CancelReason::UserRequested),
        1 => Ok(CancelReason::UnfilledRemainder),
        2 => Ok(CancelReason::SelfTradeAggressor),
        3 => Ok(CancelReason::SelfTradeResting),
        4 => Ok(CancelReason::MassCancel),
        tag => Err(CodecError::InvalidTag {
            type_name: "CancelReason",
            tag,
        }),
    }
}

fn account(decoder: &mut Decoder<'_>) -> Result<AccountId, CodecError> {
    Ok(AccountId::new(decoder.u64()?)?)
}

fn asset(decoder: &mut Decoder<'_>) -> Result<AssetId, CodecError> {
    Ok(AssetId::new(decoder.u64()?)?)
}

fn command_id(decoder: &mut Decoder<'_>) -> Result<CommandId, CodecError> {
    Ok(CommandId::new(decoder.u64()?)?)
}

fn instrument(decoder: &mut Decoder<'_>) -> Result<InstrumentId, CodecError> {
    Ok(InstrumentId::new(decoder.u64()?)?)
}

fn instrument_version(decoder: &mut Decoder<'_>) -> Result<InstrumentVersion, CodecError> {
    Ok(InstrumentVersion::new(decoder.u64()?)?)
}

fn order(decoder: &mut Decoder<'_>) -> Result<OrderId, CodecError> {
    Ok(OrderId::new(decoder.u64()?)?)
}

fn quantity(decoder: &mut Decoder<'_>) -> Result<Quantity, CodecError> {
    Ok(Quantity::new(decoder.u64()?)?)
}

fn trade_id(decoder: &mut Decoder<'_>) -> Result<TradeId, CodecError> {
    Ok(TradeId::new(decoder.u64()?)?)
}

fn transaction(decoder: &mut Decoder<'_>) -> Result<TransactionId, CodecError> {
    Ok(TransactionId::new(decoder.u64()?)?)
}

fn encode_command(encoder: &mut Encoder, command: Command) {
    match command {
        Command::New(value) => {
            encoder.u8(0);
            encoder.u64(value.command_id.get());
            encoder.u64(value.order_id.get());
            encoder.u64(value.account_id.get());
            encoder.u64(value.instrument_id.get());
            encoder.u64(value.instrument_version.get());
            encode_side(encoder, value.side);
            encoder.u64(value.quantity.lots());
            encode_order_display(encoder, value.display);
            encode_order_type(encoder, value.order_type);
            encode_time_in_force(encoder, value.time_in_force);
            encode_stp(encoder, value.self_trade_prevention);
            encoder.u64(value.received_at.as_unix_nanos());
        }
        Command::Cancel(value) => {
            encoder.u8(1);
            encoder.u64(value.command_id.get());
            encoder.u64(value.order_id.get());
            encoder.u64(value.account_id.get());
            encoder.u64(value.instrument_id.get());
            encoder.u64(value.instrument_version.get());
            encoder.u64(value.received_at.as_unix_nanos());
        }
        Command::Replace(value) => {
            encoder.u8(2);
            encoder.u64(value.command_id.get());
            encoder.u64(value.order_id.get());
            encoder.u64(value.account_id.get());
            encoder.u64(value.instrument_id.get());
            encoder.u64(value.instrument_version.get());
            encoder.u64(value.new_quantity.lots());
            encoder.i64(value.new_price.raw());
            encode_order_display(encoder, value.new_display);
            encoder.u64(value.received_at.as_unix_nanos());
        }
        Command::MassCancel(value) => {
            encoder.u8(3);
            encoder.u64(value.command_id.get());
            encoder.u64(value.account_id.get());
            encoder.u64(value.instrument_id.get());
            encoder.u64(value.instrument_version.get());
            encode_mass_cancel_scope(encoder, value.scope);
            encoder.u64(value.received_at.as_unix_nanos());
        }
    }
}

fn decode_command(decoder: &mut Decoder<'_>) -> Result<Command, CodecError> {
    match decoder.u8()? {
        0 => Ok(Command::New(NewOrder {
            command_id: command_id(decoder)?,
            order_id: order(decoder)?,
            account_id: account(decoder)?,
            instrument_id: instrument(decoder)?,
            instrument_version: instrument_version(decoder)?,
            side: decode_side(decoder)?,
            quantity: quantity(decoder)?,
            display: decode_order_display(decoder)?,
            order_type: decode_order_type(decoder)?,
            time_in_force: decode_time_in_force(decoder)?,
            self_trade_prevention: decode_stp(decoder)?,
            received_at: TimestampNs::from_unix_nanos(decoder.u64()?),
        })),
        1 => Ok(Command::Cancel(CancelOrder {
            command_id: command_id(decoder)?,
            order_id: order(decoder)?,
            account_id: account(decoder)?,
            instrument_id: instrument(decoder)?,
            instrument_version: instrument_version(decoder)?,
            received_at: TimestampNs::from_unix_nanos(decoder.u64()?),
        })),
        2 => Ok(Command::Replace(ReplaceOrder {
            command_id: command_id(decoder)?,
            order_id: order(decoder)?,
            account_id: account(decoder)?,
            instrument_id: instrument(decoder)?,
            instrument_version: instrument_version(decoder)?,
            new_quantity: quantity(decoder)?,
            new_price: Price::from_raw(decoder.i64()?),
            new_display: decode_order_display(decoder)?,
            received_at: TimestampNs::from_unix_nanos(decoder.u64()?),
        })),
        3 => Ok(Command::MassCancel(MassCancel {
            command_id: command_id(decoder)?,
            account_id: account(decoder)?,
            instrument_id: instrument(decoder)?,
            instrument_version: instrument_version(decoder)?,
            scope: decode_mass_cancel_scope(decoder)?,
            received_at: TimestampNs::from_unix_nanos(decoder.u64()?),
        })),
        tag => Err(CodecError::InvalidTag {
            type_name: "Command",
            tag,
        }),
    }
}

impl BinaryCodec for Command {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encode_command(&mut encoder, *self);
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let value = decode_command(&mut decoder)?;
        decoder.finish()?;
        Ok(value)
    }
}

fn encode_instrument_kind(encoder: &mut Encoder, kind: InstrumentKind) {
    encoder.u8(match kind {
        InstrumentKind::Equity => 0,
        InstrumentKind::Spot => 1,
        InstrumentKind::Future => 2,
        InstrumentKind::Option => 3,
        InstrumentKind::Bond => 4,
        InstrumentKind::Swap => 5,
        InstrumentKind::Index => 6,
        InstrumentKind::Synthetic => 7,
    });
}

fn decode_instrument_kind(decoder: &mut Decoder<'_>) -> Result<InstrumentKind, CodecError> {
    match decoder.u8()? {
        0 => Ok(InstrumentKind::Equity),
        1 => Ok(InstrumentKind::Spot),
        2 => Ok(InstrumentKind::Future),
        3 => Ok(InstrumentKind::Option),
        4 => Ok(InstrumentKind::Bond),
        5 => Ok(InstrumentKind::Swap),
        6 => Ok(InstrumentKind::Index),
        7 => Ok(InstrumentKind::Synthetic),
        tag => Err(CodecError::InvalidTag {
            type_name: "InstrumentKind",
            tag,
        }),
    }
}

fn encode_trading_state(encoder: &mut Encoder, state: TradingState) {
    encoder.u8(match state {
        TradingState::Open => 0,
        TradingState::CancelOnly => 1,
        TradingState::Halted => 2,
        TradingState::Closed => 3,
    });
}

fn decode_trading_state(decoder: &mut Decoder<'_>) -> Result<TradingState, CodecError> {
    match decoder.u8()? {
        0 => Ok(TradingState::Open),
        1 => Ok(TradingState::CancelOnly),
        2 => Ok(TradingState::Halted),
        3 => Ok(TradingState::Closed),
        tag => Err(CodecError::InvalidTag {
            type_name: "TradingState",
            tag,
        }),
    }
}

impl BinaryCodec for InstrumentDefinition {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.instrument_id().get());
        encoder.u64(self.version().get());
        encoder.u64(self.effective_from().as_unix_nanos());
        let symbol = self.symbol();
        let symbol_bytes = symbol.as_str().as_bytes();
        let symbol_length =
            u8::try_from(symbol_bytes.len()).map_err(|_| CodecError::LengthOverflow {
                field: "instrument symbol",
                length: symbol_bytes.len(),
            })?;
        encoder.u8(symbol_length);
        encoder.bytes(symbol_bytes);
        encode_instrument_kind(&mut encoder, self.kind());
        encoder.u64(self.base_asset_id().get());
        encoder.u64(self.quote_asset_id().get());
        let price = self.price_rules();
        encoder.u8(price.decimal_scale());
        encoder.u64(price.tick_size());
        encoder.i64(price.minimum().raw());
        encoder.i64(price.maximum().raw());
        let quantity = self.quantity_rules();
        encoder.u64(quantity.increment_lots());
        encoder.u64(quantity.minimum_lots());
        encoder.u64(quantity.maximum_lots());
        encoder.u32(self.reserve_order_rules().maximum_replenishments());
        let settlement = self.settlement_convention();
        encoder.u64(settlement.base_units_per_lot);
        encoder.u64(settlement.quote_units_per_price_unit);
        encode_trading_state(&mut encoder, self.trading_state());
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let instrument_id = instrument(&mut decoder)?;
        let version = instrument_version(&mut decoder)?;
        let effective_from = TimestampNs::from_unix_nanos(decoder.u64()?);
        let symbol_length = usize::from(decoder.u8()?);
        let symbol_text = std::str::from_utf8(decoder.bytes(symbol_length)?)
            .map_err(|_| CodecError::InvalidValue("instrument symbol is not UTF-8"))?;
        let symbol = InstrumentSymbol::new(symbol_text)?;
        let kind = decode_instrument_kind(&mut decoder)?;
        let base_asset_id = asset(&mut decoder)?;
        let quote_asset_id = asset(&mut decoder)?;
        let price = PriceRules::new(
            decoder.u8()?,
            decoder.u64()?,
            Price::from_raw(decoder.i64()?),
            Price::from_raw(decoder.i64()?),
        )?;
        let quantity = QuantityRules::new(decoder.u64()?, decoder.u64()?, decoder.u64()?)?;
        let maximum_replenishments = decoder.u32()?;
        let reserve = if maximum_replenishments == 0 {
            ReserveOrderRules::disabled()
        } else {
            ReserveOrderRules::new(maximum_replenishments)?
        };
        let base_units_per_lot = decoder.u64()?;
        let quote_units_per_price_unit = decoder.u64()?;
        let trading_state = decode_trading_state(&mut decoder)?;
        decoder.finish()?;
        Ok(InstrumentDefinition::new(InstrumentSpec {
            instrument_id,
            version,
            effective_from,
            symbol,
            kind,
            base_asset_id,
            quote_asset_id,
            price,
            quantity,
            reserve,
            base_units_per_lot,
            quote_units_per_price_unit,
            trading_state,
        })?)
    }
}

fn encode_account_risk_state(encoder: &mut Encoder, state: AccountRiskState) {
    encoder.u8(match state {
        AccountRiskState::Active => 0,
        AccountRiskState::ReduceOnly => 1,
        AccountRiskState::Blocked => 2,
    });
}

fn decode_account_risk_state(decoder: &mut Decoder<'_>) -> Result<AccountRiskState, CodecError> {
    match decoder.u8()? {
        0 => Ok(AccountRiskState::Active),
        1 => Ok(AccountRiskState::ReduceOnly),
        2 => Ok(AccountRiskState::Blocked),
        tag => Err(CodecError::InvalidTag {
            type_name: "AccountRiskState",
            tag,
        }),
    }
}

impl BinaryCodec for AccountRiskDefinition {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.account_id().get());
        let profile = self.profile();
        encode_account_risk_state(&mut encoder, profile.state());
        encoder.i128(profile.initial_position_lots());
        let limits = profile.limits();
        encoder.u64(limits.max_order_quantity_lots());
        encoder.u128(limits.max_order_notional());
        encoder.u64(limits.max_open_orders());
        encoder.u128(limits.max_open_quantity_lots());
        encoder.u128(limits.max_open_notional());
        encoder.u128(limits.max_long_position_lots());
        encoder.u128(limits.max_short_position_lots());
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let account_id = account(&mut decoder)?;
        let state = decode_account_risk_state(&mut decoder)?;
        let initial_position_lots = decoder.i128()?;
        let limits = RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots: decoder.u64()?,
            max_order_notional: decoder.u128()?,
            max_open_orders: decoder.u64()?,
            max_open_quantity_lots: decoder.u128()?,
            max_open_notional: decoder.u128()?,
            max_long_position_lots: decoder.u128()?,
            max_short_position_lots: decoder.u128()?,
        })?;
        decoder.finish()?;
        let profile = RiskProfile::new(state, initial_position_lots, limits)?;
        Ok(Self::new(account_id, profile))
    }
}

fn encode_trade(encoder: &mut Encoder, trade: Trade) {
    encoder.u64(trade.trade_id.get());
    encoder.u64(trade.instrument_id.get());
    encoder.u64(trade.instrument_version.get());
    encoder.i64(trade.price.raw());
    encoder.u64(trade.quantity.lots());
    encoder.u64(trade.buy_order_id.get());
    encoder.u64(trade.sell_order_id.get());
    encoder.u64(trade.buyer_account_id.get());
    encoder.u64(trade.seller_account_id.get());
    encoder.u64(trade.maker_order_id.get());
    encoder.u64(trade.taker_order_id.get());
}

fn decode_trade(decoder: &mut Decoder<'_>) -> Result<Trade, CodecError> {
    Ok(Trade {
        trade_id: trade_id(decoder)?,
        instrument_id: instrument(decoder)?,
        instrument_version: instrument_version(decoder)?,
        price: Price::from_raw(decoder.i64()?),
        quantity: quantity(decoder)?,
        buy_order_id: order(decoder)?,
        sell_order_id: order(decoder)?,
        buyer_account_id: account(decoder)?,
        seller_account_id: account(decoder)?,
        maker_order_id: order(decoder)?,
        taker_order_id: order(decoder)?,
    })
}

fn encode_event_kind(encoder: &mut Encoder, kind: EventKind) {
    match kind {
        EventKind::OrderAccepted {
            order_id,
            quantity,
            display,
        } => {
            encoder.u8(0);
            encoder.u64(order_id.get());
            encoder.u64(quantity.lots());
            encode_order_display(encoder, display);
        }
        EventKind::OrderRested {
            order_id,
            price,
            leaves_quantity,
            displayed_quantity,
        } => {
            encoder.u8(1);
            encoder.u64(order_id.get());
            encoder.i64(price.raw());
            encoder.u64(leaves_quantity.lots());
            encoder.u64(displayed_quantity.lots());
        }
        EventKind::Trade(trade) => {
            encoder.u8(2);
            encode_trade(encoder, trade);
        }
        EventKind::OrderCancelled {
            order_id,
            quantity,
            reason,
        } => {
            encoder.u8(3);
            encoder.u64(order_id.get());
            encoder.u64(quantity.lots());
            encode_cancel_reason(encoder, reason);
        }
        EventKind::OrderReplaced {
            order_id,
            old_price,
            new_price,
            old_quantity,
            new_quantity,
            old_display,
            new_display,
            priority_retained,
        } => {
            encoder.u8(4);
            encoder.u64(order_id.get());
            encoder.i64(old_price.raw());
            encoder.i64(new_price.raw());
            encoder.u64(old_quantity.lots());
            encoder.u64(new_quantity.lots());
            encode_order_display(encoder, old_display);
            encode_order_display(encoder, new_display);
            encoder.bool(priority_retained);
        }
        EventKind::SelfTradePrevented {
            aggressor_order_id,
            resting_order_id,
            quantity,
            policy,
        } => {
            encoder.u8(5);
            encoder.u64(aggressor_order_id.get());
            encoder.u64(resting_order_id.get());
            encoder.u64(quantity.lots());
            encode_stp(encoder, policy);
        }
        EventKind::CommandRejected(reason) => {
            encoder.u8(6);
            encode_reject_reason(encoder, reason);
        }
        EventKind::OrderRefreshed {
            order_id,
            price,
            displayed_quantity,
            leaves_quantity,
        } => {
            encoder.u8(7);
            encoder.u64(order_id.get());
            encoder.i64(price.raw());
            encoder.u64(displayed_quantity.lots());
            encoder.u64(leaves_quantity.lots());
        }
        EventKind::MassCancelCompleted {
            account_id,
            scope,
            cancelled_order_count,
            cancelled_quantity_lots,
        } => {
            encoder.u8(8);
            encoder.u64(account_id.get());
            encode_mass_cancel_scope(encoder, scope);
            encoder.u64(cancelled_order_count);
            encoder.u128(cancelled_quantity_lots);
        }
    }
}

fn decode_event_kind(decoder: &mut Decoder<'_>) -> Result<EventKind, CodecError> {
    match decoder.u8()? {
        0 => Ok(EventKind::OrderAccepted {
            order_id: order(decoder)?,
            quantity: quantity(decoder)?,
            display: decode_order_display(decoder)?,
        }),
        1 => Ok(EventKind::OrderRested {
            order_id: order(decoder)?,
            price: Price::from_raw(decoder.i64()?),
            leaves_quantity: quantity(decoder)?,
            displayed_quantity: quantity(decoder)?,
        }),
        2 => Ok(EventKind::Trade(decode_trade(decoder)?)),
        3 => Ok(EventKind::OrderCancelled {
            order_id: order(decoder)?,
            quantity: quantity(decoder)?,
            reason: decode_cancel_reason(decoder)?,
        }),
        4 => Ok(EventKind::OrderReplaced {
            order_id: order(decoder)?,
            old_price: Price::from_raw(decoder.i64()?),
            new_price: Price::from_raw(decoder.i64()?),
            old_quantity: quantity(decoder)?,
            new_quantity: quantity(decoder)?,
            old_display: decode_order_display(decoder)?,
            new_display: decode_order_display(decoder)?,
            priority_retained: decoder.bool()?,
        }),
        5 => Ok(EventKind::SelfTradePrevented {
            aggressor_order_id: order(decoder)?,
            resting_order_id: order(decoder)?,
            quantity: quantity(decoder)?,
            policy: decode_stp(decoder)?,
        }),
        6 => Ok(EventKind::CommandRejected(decode_reject_reason(decoder)?)),
        7 => Ok(EventKind::OrderRefreshed {
            order_id: order(decoder)?,
            price: Price::from_raw(decoder.i64()?),
            displayed_quantity: quantity(decoder)?,
            leaves_quantity: quantity(decoder)?,
        }),
        8 => Ok(EventKind::MassCancelCompleted {
            account_id: account(decoder)?,
            scope: decode_mass_cancel_scope(decoder)?,
            cancelled_order_count: decoder.u64()?,
            cancelled_quantity_lots: decoder.u128()?,
        }),
        tag => Err(CodecError::InvalidTag {
            type_name: "EventKind",
            tag,
        }),
    }
}

fn encode_event(encoder: &mut Encoder, event: Event) {
    encoder.u64(event.sequence);
    encoder.u64(event.command_id.get());
    encoder.u64(event.occurred_at.as_unix_nanos());
    encode_event_kind(encoder, event.kind);
}

fn decode_event(decoder: &mut Decoder<'_>) -> Result<Event, CodecError> {
    Ok(Event {
        sequence: decoder.u64()?,
        command_id: command_id(decoder)?,
        occurred_at: TimestampNs::from_unix_nanos(decoder.u64()?),
        kind: decode_event_kind(decoder)?,
    })
}

impl BinaryCodec for ExecutionReport {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.command_id.get());
        match self.outcome {
            CommandOutcome::Accepted => encoder.u8(0),
            CommandOutcome::Rejected(reason) => {
                encoder.u8(1);
                encode_reject_reason(&mut encoder, reason);
            }
        }
        encoder.bool(self.replayed);
        encoder.length("execution report events", self.events.len())?;
        for event in &self.events {
            encode_event(&mut encoder, *event);
        }
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let report_command_id = command_id(&mut decoder)?;
        let outcome = match decoder.u8()? {
            0 => CommandOutcome::Accepted,
            1 => CommandOutcome::Rejected(decode_reject_reason(&mut decoder)?),
            tag => {
                return Err(CodecError::InvalidTag {
                    type_name: "CommandOutcome",
                    tag,
                });
            }
        };
        let replayed = decoder.bool()?;
        let count = decoder.count("execution report events", 25)?;
        if count == 0 {
            return Err(CodecError::InvalidValue(
                "execution report must contain at least one event",
            ));
        }
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            events.push(decode_event(&mut decoder)?);
        }
        decoder.finish()?;
        validate_report(report_command_id, outcome, &events)?;
        Ok(Self {
            command_id: report_command_id,
            outcome,
            events,
            replayed,
        })
    }
}

fn encode_sized_bytes(
    encoder: &mut Encoder,
    field: &'static str,
    bytes: &[u8],
) -> Result<(), CodecError> {
    encoder.length(field, bytes.len())?;
    encoder.bytes(bytes);
    Ok(())
}

impl BinaryCodec for OrderBookCheckpoint {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.wal_metadata_sequence());
        encoder.u64(self.generation());
        encode_sized_bytes(
            &mut encoder,
            "matching checkpoint definition bytes",
            &self.definition().encode()?,
        )?;
        encoder.length("matching checkpoint active orders", self.orders().len())?;
        for order in self.orders() {
            encoder.u64(order.order_id().get());
            encoder.u64(order.account_id().get());
            encode_side(&mut encoder, order.side());
            encoder.i64(order.price().raw());
            encoder.u64(order.leaves().lots());
            encoder.u64(order.displayed().lots());
            encode_order_display(&mut encoder, order.display());
            encode_stp(&mut encoder, order.self_trade_prevention());
        }
        encoder.length("matching checkpoint command history", self.history().len())?;
        for entry in self.history() {
            encode_sized_bytes(
                &mut encoder,
                "matching checkpoint command bytes",
                &entry.command().encode()?,
            )?;
            encode_sized_bytes(
                &mut encoder,
                "matching checkpoint report bytes",
                &entry.report().encode()?,
            )?;
        }
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let wal_metadata_sequence = decoder.u64()?;
        let wal_sequence = decoder.u64()?;
        let definition = InstrumentDefinition::decode(
            decoder.sized_bytes("matching checkpoint definition bytes")?,
        )?;
        let order_count = decoder.count("matching checkpoint active orders", 43)?;
        let mut orders = Vec::with_capacity(order_count);
        for _ in 0..order_count {
            orders.push(RestingOrderCheckpoint {
                order_id: order(&mut decoder)?,
                account_id: account(&mut decoder)?,
                side: decode_side(&mut decoder)?,
                price: Price::from_raw(decoder.i64()?),
                leaves: quantity(&mut decoder)?,
                displayed: quantity(&mut decoder)?,
                display: decode_order_display(&mut decoder)?,
                self_trade_prevention: decode_stp(&mut decoder)?,
            });
        }
        let history_count = decoder.count("matching checkpoint command history", 8)?;
        let mut history = Vec::with_capacity(history_count);
        for _ in 0..history_count {
            let command =
                Command::decode(decoder.sized_bytes("matching checkpoint command bytes")?)?;
            let report =
                ExecutionReport::decode(decoder.sized_bytes("matching checkpoint report bytes")?)?;
            history.push(CommandReportCheckpoint { command, report });
        }
        decoder.finish()?;
        OrderBookCheckpoint::from_parts(
            wal_metadata_sequence,
            wal_sequence,
            definition,
            orders,
            history,
        )
        .map_err(Into::into)
    }
}

impl BinaryCodec for RiskManagedCheckpoint {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.wal_first_sequence());
        encode_sized_bytes(
            &mut encoder,
            "risk checkpoint matching bytes",
            &self.matching().encode()?,
        )?;
        encoder.length("risk checkpoint accounts", self.accounts().len())?;
        for account in self.accounts() {
            let definition = AccountRiskDefinition::new(account.account_id(), account.profile());
            encode_sized_bytes(
                &mut encoder,
                "risk checkpoint account definition bytes",
                &definition.encode()?,
            )?;
            let exposure = account.exposure();
            encoder.i128(exposure.position_lots());
            encoder.u128(exposure.open_buy_lots());
            encoder.u128(exposure.open_sell_lots());
            encoder.u128(exposure.open_notional());
            encoder.u64(exposure.open_orders());
        }
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let wal_first_sequence = decoder.u64()?;
        let matching =
            OrderBookCheckpoint::decode(decoder.sized_bytes("risk checkpoint matching bytes")?)?;
        let account_count = decoder.count("risk checkpoint accounts", 76)?;
        let mut accounts = Vec::with_capacity(account_count);
        for _ in 0..account_count {
            let definition = AccountRiskDefinition::decode(
                decoder.sized_bytes("risk checkpoint account definition bytes")?,
            )?;
            let exposure = RiskSnapshot::from_parts(
                decoder.i128()?,
                decoder.u128()?,
                decoder.u128()?,
                decoder.u128()?,
                decoder.u64()?,
            );
            accounts.push(RiskAccountCheckpoint::from_parts(
                definition.account_id(),
                definition.profile(),
                exposure,
            ));
        }
        decoder.finish()?;
        RiskManagedCheckpoint::from_parts(wal_first_sequence, matching, accounts)
            .map_err(Into::into)
    }
}

fn validate_report(
    command_id: CommandId,
    outcome: CommandOutcome,
    events: &[Event],
) -> Result<(), CodecError> {
    if events.iter().any(|event| event.command_id != command_id) {
        return Err(CodecError::InvalidValue(
            "execution report contains an event for another command",
        ));
    }
    if events.windows(2).any(|window| {
        window[0]
            .sequence
            .checked_add(1)
            .is_none_or(|expected| window[1].sequence != expected)
    }) {
        return Err(CodecError::InvalidValue(
            "execution report event sequences are not contiguous",
        ));
    }
    match outcome {
        CommandOutcome::Accepted => {
            if events
                .iter()
                .any(|event| matches!(event.kind, EventKind::CommandRejected(_)))
            {
                return Err(CodecError::InvalidValue(
                    "accepted execution report contains a rejection event",
                ));
            }
        }
        CommandOutcome::Rejected(reason) => {
            if events.len() != 1
                || !matches!(events[0].kind, EventKind::CommandRejected(value) if value == reason)
            {
                return Err(CodecError::InvalidValue(
                    "rejected execution report does not match its rejection event",
                ));
            }
        }
    }
    Ok(())
}

fn encode_market_data_level(encoder: &mut Encoder, level: MarketDataLevel) {
    encode_side(encoder, level.side);
    encoder.i64(level.price.raw());
    encoder.u128(level.quantity);
    encoder.u64(level.order_count);
}

fn decode_market_data_level(decoder: &mut Decoder<'_>) -> Result<MarketDataLevel, CodecError> {
    Ok(MarketDataLevel::new(
        decode_side(decoder)?,
        Price::from_raw(decoder.i64()?),
        decoder.u128()?,
        decoder.u64()?,
    )?)
}

impl BinaryCodec for MarketDataUpdate {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.instrument_id().get());
        encoder.u64(self.instrument_version().get());
        encoder.u64(self.sequence());
        encoder.u64(self.occurred_at().as_unix_nanos());
        match self.kind() {
            MarketDataKind::NoBookChange => encoder.u8(0),
            MarketDataKind::Level(level) => {
                encoder.u8(1);
                encode_market_data_level(&mut encoder, level);
            }
            MarketDataKind::Trade { print, maker_level } => {
                encoder.u8(2);
                encoder.u64(print.trade_id.get());
                encoder.i64(print.price.raw());
                encoder.u64(print.quantity.lots());
                encode_side(&mut encoder, print.aggressor_side);
                encode_market_data_level(&mut encoder, maker_level);
            }
        }
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let instrument_id = instrument(&mut decoder)?;
        let instrument_version = instrument_version(&mut decoder)?;
        let sequence = decoder.u64()?;
        let occurred_at = TimestampNs::from_unix_nanos(decoder.u64()?);
        let kind = match decoder.u8()? {
            0 => MarketDataKind::NoBookChange,
            1 => MarketDataKind::Level(decode_market_data_level(&mut decoder)?),
            2 => MarketDataKind::Trade {
                print: TradePrint {
                    trade_id: trade_id(&mut decoder)?,
                    price: Price::from_raw(decoder.i64()?),
                    quantity: quantity(&mut decoder)?,
                    aggressor_side: decode_side(&mut decoder)?,
                },
                maker_level: decode_market_data_level(&mut decoder)?,
            },
            tag => {
                return Err(CodecError::InvalidTag {
                    type_name: "MarketDataKind",
                    tag,
                });
            }
        };
        decoder.finish()?;
        Ok(Self::from_parts(
            instrument_id,
            instrument_version,
            sequence,
            occurred_at,
            kind,
        )?)
    }
}

impl BinaryCodec for MarketDataSnapshot {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.instrument_id().get());
        encoder.u64(self.instrument_version().get());
        encoder.u64(self.as_of_sequence());
        match self.last_trade_id() {
            Some(trade_id) => {
                encoder.u8(1);
                encoder.u64(trade_id.get());
            }
            None => encoder.u8(0),
        }
        encoder.length("market-data snapshot bids", self.bids().len())?;
        for level in self.bids() {
            encode_market_data_level(&mut encoder, *level);
        }
        encoder.length("market-data snapshot asks", self.asks().len())?;
        for level in self.asks() {
            encode_market_data_level(&mut encoder, *level);
        }
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let instrument_id = instrument(&mut decoder)?;
        let instrument_version = instrument_version(&mut decoder)?;
        let as_of_sequence = decoder.u64()?;
        let last_trade_id = match decoder.u8()? {
            0 => None,
            1 => Some(trade_id(&mut decoder)?),
            tag => {
                return Err(CodecError::InvalidTag {
                    type_name: "Option<TradeId>",
                    tag,
                });
            }
        };
        let bid_count = decoder.count("market-data snapshot bids", 33)?;
        let mut bids = Vec::with_capacity(bid_count);
        for _ in 0..bid_count {
            bids.push(decode_market_data_level(&mut decoder)?);
        }
        let ask_count = decoder.count("market-data snapshot asks", 33)?;
        let mut asks = Vec::with_capacity(ask_count);
        for _ in 0..ask_count {
            asks.push(decode_market_data_level(&mut decoder)?);
        }
        decoder.finish()?;
        Ok(Self::from_parts(
            instrument_id,
            instrument_version,
            as_of_sequence,
            last_trade_id,
            bids,
            asks,
        )?)
    }
}

impl BinaryCodec for JournalEntry {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.transaction_id().get());
        encoder.u64(self.reference());
        encoder.bool(self.effective_date().is_some());
        encoder.i32(
            self.effective_date()
                .map_or(0, AccountingDate::days_since_unix_epoch),
        );
        encoder.u64(self.recorded_at().as_unix_nanos());
        encoder.length("journal entry postings", self.postings().len())?;
        for posting in self.postings() {
            encoder.u64(posting.account_id.get());
            encoder.u64(posting.asset_id.get());
            encoder.i128(posting.amount);
        }
        let (kind_tag, related_transaction, period_boundary) = match self.kind() {
            LedgerEntryKind::Standard => (0, 0, None),
            LedgerEntryKind::Reversal {
                reversed_transaction_id,
            } => (1, reversed_transaction_id.get(), None),
            LedgerEntryKind::PeriodClose { closed_through } => (2, 0, Some(closed_through)),
            LedgerEntryKind::PeriodReopen { new_closed_through } => (3, 0, new_closed_through),
        };
        encoder.u8(kind_tag);
        encoder.u64(related_transaction);
        encoder.bool(period_boundary.is_some());
        encoder.i32(period_boundary.map_or(0, AccountingDate::days_since_unix_epoch));
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let transaction_id = transaction(&mut decoder)?;
        let reference = decoder.u64()?;
        let has_effective_date = decoder.bool()?;
        let effective_date_days = decoder.i32()?;
        if !has_effective_date && effective_date_days != 0 {
            return Err(CodecError::InvalidValue(
                "absent ledger effective date has a non-zero value",
            ));
        }
        let effective_date = has_effective_date
            .then(|| AccountingDate::from_days_since_unix_epoch(effective_date_days));
        let recorded_at = TimestampNs::from_unix_nanos(decoder.u64()?);
        let count = decoder.count("journal entry postings", 32)?;
        let mut postings = Vec::with_capacity(count);
        for _ in 0..count {
            postings.push(Posting {
                account_id: account(&mut decoder)?,
                asset_id: asset(&mut decoder)?,
                amount: decoder.i128()?,
            });
        }
        let kind_tag = decoder.u8()?;
        let related_transaction = decoder.u64()?;
        let has_period_boundary = decoder.bool()?;
        let period_boundary_days = decoder.i32()?;
        if !has_period_boundary && period_boundary_days != 0 {
            return Err(CodecError::InvalidValue(
                "absent ledger period boundary has a non-zero value",
            ));
        }
        let period_boundary = has_period_boundary
            .then(|| AccountingDate::from_days_since_unix_epoch(period_boundary_days));
        let kind = match (kind_tag, related_transaction, period_boundary) {
            (0, 0, None) => LedgerEntryKind::Standard,
            (0, _, _) => {
                return Err(CodecError::InvalidValue(
                    "standard ledger entry has control metadata",
                ));
            }
            (1, value, None) => LedgerEntryKind::Reversal {
                reversed_transaction_id: TransactionId::new(value)?,
            },
            (1, _, Some(_)) => {
                return Err(CodecError::InvalidValue(
                    "reversal ledger entry has a period boundary",
                ));
            }
            (2, 0, Some(closed_through)) => LedgerEntryKind::PeriodClose { closed_through },
            (2, _, _) => {
                return Err(CodecError::InvalidValue(
                    "period-close ledger entry has invalid metadata",
                ));
            }
            (3, 0, new_closed_through) => LedgerEntryKind::PeriodReopen { new_closed_through },
            (3, _, _) => {
                return Err(CodecError::InvalidValue(
                    "period-reopen ledger entry has a related transaction",
                ));
            }
            (tag, _, _) => {
                return Err(CodecError::InvalidTag {
                    type_name: "ledger entry kind",
                    tag,
                });
            }
        };
        decoder.finish()?;
        let canonical = JournalEntry::with_kind(
            transaction_id,
            reference,
            effective_date,
            recorded_at,
            postings.clone(),
            kind,
        )
        .map_err(CodecError::InvalidJournalEntry)?;
        if canonical.postings() != postings {
            return Err(CodecError::InvalidValue(
                "journal entry postings are not in canonical order",
            ));
        }
        Ok(canonical)
    }
}

impl BinaryCodec for LedgerCorrection {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let reversal = self.reversal().encode()?;
        let replacement = self.replacement().encode()?;
        let mut encoder = Encoder::default();
        encoder.length("ledger correction reversal", reversal.len())?;
        encoder.bytes(&reversal);
        encoder.length("ledger correction replacement", replacement.len())?;
        encoder.bytes(&replacement);
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let reversal_length = usize::try_from(decoder.u32()?)
            .map_err(|_| CodecError::InvalidValue("ledger correction reversal exceeds usize"))?;
        let reversal = JournalEntry::decode(decoder.bytes(reversal_length)?)?;
        let replacement_length = usize::try_from(decoder.u32()?)
            .map_err(|_| CodecError::InvalidValue("ledger correction replacement exceeds usize"))?;
        let replacement = JournalEntry::decode(decoder.bytes(replacement_length)?)?;
        decoder.finish()?;
        LedgerCorrection::from_parts(reversal, replacement).map_err(CodecError::InvalidJournalEntry)
    }
}

impl BinaryCodec for LedgerRecord {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        match self {
            Self::Entry(entry) => {
                encoder.u8(0);
                encoder.bytes(&entry.encode()?);
            }
            Self::Correction(correction) => {
                encoder.u8(1);
                encoder.bytes(&correction.encode()?);
            }
        }
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let (tag, payload) = bytes.split_first().ok_or(CodecError::UnexpectedEnd {
            needed: 1,
            remaining: 0,
        })?;
        match tag {
            0 => JournalEntry::decode(payload).map(Self::Entry),
            1 => LedgerCorrection::decode(payload).map(Self::Correction),
            tag => Err(CodecError::InvalidTag {
                type_name: "ledger record",
                tag: *tag,
            }),
        }
    }
}

impl BinaryCodec for LedgerCheckpoint {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.generation());
        encoder.length("ledger checkpoint balances", self.balances().len())?;
        for balance in self.balances() {
            encoder.u64(balance.account_id().get());
            encoder.u64(balance.asset_id().get());
            encoder.i128(balance.amount());
        }
        encoder.length("ledger checkpoint records", self.records().len())?;
        for record in self.records() {
            let bytes = record.encode()?;
            encoder.length("ledger checkpoint record payload", bytes.len())?;
            encoder.bytes(&bytes);
        }
        Ok(encoder.finish())
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let generation = decoder.u64()?;
        let balance_count = decoder.count("ledger checkpoint balances", 32)?;
        let mut balances = Vec::with_capacity(balance_count);
        for _ in 0..balance_count {
            balances.push(LedgerBalance::from_parts(
                account(&mut decoder)?,
                asset(&mut decoder)?,
                decoder.i128()?,
            ));
        }
        // The smallest record is a one-byte tag plus a 47-byte period control.
        // Include its four-byte length prefix before allocating the collection.
        let record_count = decoder.count("ledger checkpoint records", 52)?;
        let mut records = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let length = usize::try_from(decoder.u32()?).map_err(|_| {
                CodecError::InvalidValue("ledger checkpoint record length exceeds usize")
            })?;
            records.push(LedgerRecord::decode(decoder.bytes(length)?)?);
        }
        decoder.finish()?;
        LedgerCheckpoint::from_parts(generation, balances, records).map_err(Into::into)
    }
}
