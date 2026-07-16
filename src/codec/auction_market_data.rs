use crate::auction::AuctionOrderConstraint;
use crate::auction_engine::{CallAuctionPhase, CallAuctionPhaseSnapshot};
use crate::auction_market_data::{
    CallAuctionBookChangeReason, CallAuctionMarketDataKind, CallAuctionMarketDataLevel,
    CallAuctionMarketDataSnapshot, CallAuctionMarketDataUpdate, CallAuctionTradePrint,
};
use crate::{AuctionId, Price, TimestampNs};

use super::auction::{decode_indicative_state, encode_indicative_state};
use super::{
    BinaryCodec, CodecError, Decoder, Encoder, decode_side, encode_side, instrument,
    instrument_version, quantity, reserve_decoded_vec, trade_id,
};

fn auction_id(decoder: &mut Decoder<'_>) -> Result<AuctionId, CodecError> {
    Ok(AuctionId::new(decoder.u64()?)?)
}

fn encode_phase(encoder: &mut Encoder, phase: CallAuctionPhase) {
    encoder.u8(match phase {
        CallAuctionPhase::Closed => 0,
        CallAuctionPhase::Collecting => 1,
        CallAuctionPhase::Frozen => 2,
    });
}

fn decode_phase(decoder: &mut Decoder<'_>) -> Result<CallAuctionPhase, CodecError> {
    match decoder.u8()? {
        0 => Ok(CallAuctionPhase::Closed),
        1 => Ok(CallAuctionPhase::Collecting),
        2 => Ok(CallAuctionPhase::Frozen),
        tag => Err(CodecError::InvalidTag {
            type_name: "CallAuctionPhase",
            tag,
        }),
    }
}

fn encode_optional_auction_id(encoder: &mut Encoder, value: Option<AuctionId>) {
    encoder.bool(value.is_some());
    if let Some(value) = value {
        encoder.u64(value.get());
    }
}

fn decode_optional_auction_id(decoder: &mut Decoder<'_>) -> Result<Option<AuctionId>, CodecError> {
    if decoder.bool()? {
        Ok(Some(auction_id(decoder)?))
    } else {
        Ok(None)
    }
}

fn encode_constraint(encoder: &mut Encoder, constraint: AuctionOrderConstraint) {
    match constraint {
        AuctionOrderConstraint::Market => encoder.u8(0),
        AuctionOrderConstraint::Limit(price) => {
            encoder.u8(1);
            encoder.i64(price.raw());
        }
    }
}

fn decode_constraint(decoder: &mut Decoder<'_>) -> Result<AuctionOrderConstraint, CodecError> {
    match decoder.u8()? {
        0 => Ok(AuctionOrderConstraint::Market),
        1 => Ok(AuctionOrderConstraint::Limit(Price::from_raw(
            decoder.i64()?,
        ))),
        tag => Err(CodecError::InvalidTag {
            type_name: "AuctionOrderConstraint",
            tag,
        }),
    }
}

fn encode_level(encoder: &mut Encoder, level: CallAuctionMarketDataLevel) {
    encode_side(encoder, level.side());
    encode_constraint(encoder, level.constraint());
    encoder.u128(level.quantity());
    encoder.u64(level.order_count());
}

fn decode_level(decoder: &mut Decoder<'_>) -> Result<CallAuctionMarketDataLevel, CodecError> {
    Ok(CallAuctionMarketDataLevel::from_parts(
        decode_side(decoder)?,
        decode_constraint(decoder)?,
        decoder.u128()?,
        decoder.u64()?,
    )?)
}

fn encode_reason(encoder: &mut Encoder, reason: CallAuctionBookChangeReason) {
    encoder.u8(match reason {
        CallAuctionBookChangeReason::Accepted => 0,
        CallAuctionBookChangeReason::UserCancelled => 1,
        CallAuctionBookChangeReason::UncrossRemainder => 2,
        CallAuctionBookChangeReason::Replaced => 3,
        CallAuctionBookChangeReason::MassCancelled => 4,
        CallAuctionBookChangeReason::Amended => 5,
    });
}

fn decode_reason(decoder: &mut Decoder<'_>) -> Result<CallAuctionBookChangeReason, CodecError> {
    match decoder.u8()? {
        0 => Ok(CallAuctionBookChangeReason::Accepted),
        1 => Ok(CallAuctionBookChangeReason::UserCancelled),
        2 => Ok(CallAuctionBookChangeReason::UncrossRemainder),
        3 => Ok(CallAuctionBookChangeReason::Replaced),
        4 => Ok(CallAuctionBookChangeReason::MassCancelled),
        5 => Ok(CallAuctionBookChangeReason::Amended),
        tag => Err(CodecError::InvalidTag {
            type_name: "CallAuctionBookChangeReason",
            tag,
        }),
    }
}

fn encode_print(encoder: &mut Encoder, print: CallAuctionTradePrint) {
    encoder.u64(print.auction_id().get());
    encoder.u64(print.trade_id().get());
    encoder.i64(print.price().raw());
    encoder.u64(print.quantity().lots());
}

fn decode_print(decoder: &mut Decoder<'_>) -> Result<CallAuctionTradePrint, CodecError> {
    Ok(CallAuctionTradePrint::from_parts(
        auction_id(decoder)?,
        trade_id(decoder)?,
        Price::from_raw(decoder.i64()?),
        quantity(decoder)?,
    ))
}

impl BinaryCodec for CallAuctionMarketDataUpdate {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.instrument_id().get());
        encoder.u64(self.instrument_version().get());
        encoder.u64(self.sequence());
        encoder.u64(self.occurred_at().as_unix_nanos());
        match self.kind() {
            CallAuctionMarketDataKind::NoPublicChange => encoder.u8(0),
            CallAuctionMarketDataKind::Book {
                reason,
                quantity,
                level,
            } => {
                encoder.u8(1);
                encode_reason(&mut encoder, reason);
                encoder.u64(quantity.lots());
                encode_level(&mut encoder, level);
            }
            CallAuctionMarketDataKind::Trade { print, buy, sell } => {
                encoder.u8(2);
                encode_print(&mut encoder, print);
                encode_level(&mut encoder, buy);
                encode_level(&mut encoder, sell);
            }
            CallAuctionMarketDataKind::PhaseChanged {
                auction_id,
                previous,
                current,
                revision,
            } => {
                encoder.u8(3);
                encoder.u64(auction_id.get());
                encode_phase(&mut encoder, previous);
                encode_phase(&mut encoder, current);
                encoder.u64(revision);
            }
            CallAuctionMarketDataKind::UncrossCompleted {
                auction_id,
                clearing,
                trade_count,
                cancellation_count,
                book_revision,
                phase_revision,
            } => {
                encoder.u8(4);
                encoder.u64(auction_id.get());
                super::auction::encode_clearing(&mut encoder, clearing);
                encoder.u64(trade_count);
                encoder.u64(cancellation_count);
                encoder.u64(book_revision);
                encoder.u64(phase_revision);
            }
            CallAuctionMarketDataKind::MassCancelCompleted {
                cancelled_order_count,
                cancelled_quantity_lots,
                book_revision,
            } => {
                encoder.u8(5);
                encoder.u64(cancelled_order_count);
                encoder.u128(cancelled_quantity_lots);
                encoder.u64(book_revision);
            }
            CallAuctionMarketDataKind::Indicative(indicative) => {
                encoder.u8(6);
                encode_indicative_state(&mut encoder, indicative);
            }
        }
        encoder.finish()
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let instrument_id = instrument(&mut decoder)?;
        let instrument_version = instrument_version(&mut decoder)?;
        let sequence = decoder.u64()?;
        let occurred_at = TimestampNs::from_unix_nanos(decoder.u64()?);
        let kind = match decoder.u8()? {
            0 => CallAuctionMarketDataKind::NoPublicChange,
            1 => CallAuctionMarketDataKind::Book {
                reason: decode_reason(&mut decoder)?,
                quantity: quantity(&mut decoder)?,
                level: decode_level(&mut decoder)?,
            },
            2 => CallAuctionMarketDataKind::Trade {
                print: decode_print(&mut decoder)?,
                buy: decode_level(&mut decoder)?,
                sell: decode_level(&mut decoder)?,
            },
            3 => CallAuctionMarketDataKind::PhaseChanged {
                auction_id: auction_id(&mut decoder)?,
                previous: decode_phase(&mut decoder)?,
                current: decode_phase(&mut decoder)?,
                revision: decoder.u64()?,
            },
            4 => CallAuctionMarketDataKind::UncrossCompleted {
                auction_id: auction_id(&mut decoder)?,
                clearing: super::auction::decode_clearing(&mut decoder)?,
                trade_count: decoder.u64()?,
                cancellation_count: decoder.u64()?,
                book_revision: decoder.u64()?,
                phase_revision: decoder.u64()?,
            },
            5 => CallAuctionMarketDataKind::MassCancelCompleted {
                cancelled_order_count: decoder.u64()?,
                cancelled_quantity_lots: decoder.u128()?,
                book_revision: decoder.u64()?,
            },
            6 => CallAuctionMarketDataKind::Indicative(decode_indicative_state(&mut decoder)?),
            tag => {
                return Err(CodecError::InvalidTag {
                    type_name: "CallAuctionMarketDataKind",
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

impl BinaryCodec for CallAuctionMarketDataSnapshot {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.instrument_id().get());
        encoder.u64(self.instrument_version().get());
        encoder.u64(self.as_of_sequence());
        encoder.u64(self.command_sequence());
        let phase = self.phase();
        encode_phase(&mut encoder, phase.phase());
        encoder.u64(phase.revision());
        encode_optional_auction_id(&mut encoder, phase.active_auction_id());
        encode_optional_auction_id(&mut encoder, phase.last_auction_id());
        encoder.u64(self.book_revision());
        encoder.bool(self.indicative().is_some());
        if let Some(indicative) = self.indicative() {
            encode_indicative_state(&mut encoder, indicative);
        }
        encoder.bool(self.last_trade_id().is_some());
        if let Some(trade_id) = self.last_trade_id() {
            encoder.u64(trade_id.get());
        }
        encode_level(&mut encoder, self.market_buy());
        encode_level(&mut encoder, self.market_sell());
        encoder.length("call-auction market-data bids", self.bids().len())?;
        for level in self.bids() {
            encode_level(&mut encoder, *level);
        }
        encoder.length("call-auction market-data asks", self.asks().len())?;
        for level in self.asks() {
            encode_level(&mut encoder, *level);
        }
        encoder.finish()
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let instrument_id = instrument(&mut decoder)?;
        let instrument_version = instrument_version(&mut decoder)?;
        let as_of_sequence = decoder.u64()?;
        let command_sequence = decoder.u64()?;
        let phase = decode_phase(&mut decoder)?;
        let phase_revision = decoder.u64()?;
        let active_auction_id = decode_optional_auction_id(&mut decoder)?;
        let last_auction_id = decode_optional_auction_id(&mut decoder)?;
        let book_revision = decoder.u64()?;
        let indicative = if decoder.bool()? {
            Some(decode_indicative_state(&mut decoder)?)
        } else {
            None
        };
        let last_trade_id = if decoder.bool()? {
            Some(trade_id(&mut decoder)?)
        } else {
            None
        };
        let market_buy = decode_level(&mut decoder)?;
        let market_sell = decode_level(&mut decoder)?;
        let bid_count = decoder.count("call-auction market-data bids", 26)?;
        let mut bids = reserve_decoded_vec("call-auction market-data bids", bid_count)?;
        for _ in 0..bid_count {
            bids.push(decode_level(&mut decoder)?);
        }
        let ask_count = decoder.count("call-auction market-data asks", 26)?;
        let mut asks = reserve_decoded_vec("call-auction market-data asks", ask_count)?;
        for _ in 0..ask_count {
            asks.push(decode_level(&mut decoder)?);
        }
        decoder.finish()?;
        Ok(CallAuctionMarketDataSnapshot::from_parts(
            instrument_id,
            instrument_version,
            as_of_sequence,
            command_sequence,
            CallAuctionPhaseSnapshot::from_parts(
                phase,
                phase_revision,
                active_auction_id,
                last_auction_id,
            ),
            book_revision,
            indicative,
            last_trade_id,
            market_buy,
            market_sell,
            bids,
            asks,
        )?)
    }
}
