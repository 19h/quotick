use crate::auction::{
    AuctionClearing, AuctionFinalTieBreak, AuctionOrderConstraint, AuctionPressureRule,
    AuctionPriceBand, AuctionPricePolicy,
};
use crate::auction_book::{
    CallAuctionCancellation, CallAuctionOrder, CallAuctionOrderSnapshot,
    CallAuctionRemainderPolicy, CallAuctionSelfTradePolicy, CallAuctionTrade,
    CallAuctionUncrossPolicy,
};
use crate::auction_engine::{
    CallAuctionAction, CallAuctionCancelOrder, CallAuctionCancellationReason,
    CallAuctionCheckpoint, CallAuctionCommand, CallAuctionCommandOutcome,
    CallAuctionCommandReportCheckpoint, CallAuctionEvent, CallAuctionEventKind,
    CallAuctionExecutionReport, CallAuctionPhase, CallAuctionPhaseControl,
    CallAuctionPhaseSnapshot, CallAuctionRejectReason, CallAuctionReplaceOrder,
    CallAuctionSubmitOrder, CallAuctionUncrossCommand,
};
use crate::instrument::{AdmissionError, InstrumentDefinition};
use crate::{AuctionId, Price, TimestampNs};

use super::{
    BinaryCodec, CodecError, Decoder, Encoder, account, command_id, decode_side, encode_side,
    encode_sized_bytes, instrument, instrument_version, order, quantity, reserve_decoded_vec,
    trade_id,
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

fn encode_action(encoder: &mut Encoder, action: CallAuctionAction) {
    encoder.u8(match action {
        CallAuctionAction::PhaseControl => 0,
        CallAuctionAction::Submit => 1,
        CallAuctionAction::Cancel => 2,
        CallAuctionAction::Uncross => 3,
        CallAuctionAction::Replace => 4,
    });
}

fn decode_action(decoder: &mut Decoder<'_>) -> Result<CallAuctionAction, CodecError> {
    match decoder.u8()? {
        0 => Ok(CallAuctionAction::PhaseControl),
        1 => Ok(CallAuctionAction::Submit),
        2 => Ok(CallAuctionAction::Cancel),
        3 => Ok(CallAuctionAction::Uncross),
        4 => Ok(CallAuctionAction::Replace),
        tag => Err(CodecError::InvalidTag {
            type_name: "CallAuctionAction",
            tag,
        }),
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

fn encode_price_policy(encoder: &mut Encoder, policy: AuctionPricePolicy) {
    encoder.u8(match policy.pressure_rule() {
        AuctionPressureRule::Ignore => 0,
        AuctionPressureRule::FavorImbalance => 1,
    });
    encoder.u8(match policy.final_tie_break() {
        AuctionFinalTieBreak::LowerPrice => 0,
        AuctionFinalTieBreak::HigherPrice => 1,
    });
}

fn decode_price_policy(decoder: &mut Decoder<'_>) -> Result<AuctionPricePolicy, CodecError> {
    let pressure = match decoder.u8()? {
        0 => AuctionPressureRule::Ignore,
        1 => AuctionPressureRule::FavorImbalance,
        tag => {
            return Err(CodecError::InvalidTag {
                type_name: "AuctionPressureRule",
                tag,
            });
        }
    };
    let tie_break = match decoder.u8()? {
        0 => AuctionFinalTieBreak::LowerPrice,
        1 => AuctionFinalTieBreak::HigherPrice,
        tag => {
            return Err(CodecError::InvalidTag {
                type_name: "AuctionFinalTieBreak",
                tag,
            });
        }
    };
    Ok(AuctionPricePolicy::new(pressure, tie_break))
}

fn encode_uncross_policy(encoder: &mut Encoder, policy: CallAuctionUncrossPolicy) {
    encoder.u8(match policy.remainder() {
        CallAuctionRemainderPolicy::RetainAll => 0,
        CallAuctionRemainderPolicy::CancelMarket => 1,
        CallAuctionRemainderPolicy::CancelAll => 2,
    });
    encoder.u8(match policy.self_trade() {
        CallAuctionSelfTradePolicy::Permit => 0,
    });
}

fn decode_uncross_policy(
    decoder: &mut Decoder<'_>,
) -> Result<CallAuctionUncrossPolicy, CodecError> {
    let remainder = match decoder.u8()? {
        0 => CallAuctionRemainderPolicy::RetainAll,
        1 => CallAuctionRemainderPolicy::CancelMarket,
        2 => CallAuctionRemainderPolicy::CancelAll,
        tag => {
            return Err(CodecError::InvalidTag {
                type_name: "CallAuctionRemainderPolicy",
                tag,
            });
        }
    };
    let self_trade = match decoder.u8()? {
        0 => CallAuctionSelfTradePolicy::Permit,
        tag => {
            return Err(CodecError::InvalidTag {
                type_name: "CallAuctionSelfTradePolicy",
                tag,
            });
        }
    };
    Ok(CallAuctionUncrossPolicy::new(remainder, self_trade))
}

fn encode_order(encoder: &mut Encoder, value: CallAuctionOrder) {
    encoder.u64(value.order_id().get());
    encoder.u64(value.account_id().get());
    encoder.u64(value.instrument_id().get());
    encoder.u64(value.instrument_version().get());
    encode_side(encoder, value.side());
    encode_constraint(encoder, value.constraint());
    encoder.u64(value.quantity().lots());
}

fn decode_order(decoder: &mut Decoder<'_>) -> Result<CallAuctionOrder, CodecError> {
    Ok(CallAuctionOrder::new(
        order(decoder)?,
        account(decoder)?,
        instrument(decoder)?,
        instrument_version(decoder)?,
        decode_side(decoder)?,
        decode_constraint(decoder)?,
        quantity(decoder)?,
    ))
}

fn encode_snapshot(encoder: &mut Encoder, value: CallAuctionOrderSnapshot) {
    encoder.u64(value.order_id.get());
    encoder.u64(value.account_id.get());
    encode_side(encoder, value.side);
    encode_constraint(encoder, value.constraint);
    encoder.u64(value.quantity.lots());
    encoder.u64(value.priority_sequence);
}

fn decode_snapshot(decoder: &mut Decoder<'_>) -> Result<CallAuctionOrderSnapshot, CodecError> {
    let value = CallAuctionOrderSnapshot {
        order_id: order(decoder)?,
        account_id: account(decoder)?,
        side: decode_side(decoder)?,
        constraint: decode_constraint(decoder)?,
        quantity: quantity(decoder)?,
        priority_sequence: decoder.u64()?,
    };
    if value.priority_sequence == 0 {
        return Err(CodecError::InvalidValue(
            "call-auction order priority sequence is zero",
        ));
    }
    Ok(value)
}

fn encode_admission_error(encoder: &mut Encoder, error: AdmissionError) {
    encoder.u8(match error {
        AdmissionError::WrongInstrument => 0,
        AdmissionError::WrongVersion => 1,
        AdmissionError::TradingStateDisallowsEntry => 2,
        AdmissionError::PriceOffTickGrid => 3,
        AdmissionError::PriceOutsideCollar => 4,
        AdmissionError::QuantityOffGrid => 5,
        AdmissionError::QuantityOutsideLimits => 6,
        AdmissionError::ReserveOrderNotSupported => 7,
        AdmissionError::DisplayQuantityOffGrid => 8,
        AdmissionError::DisplayQuantityNotLessThanOrder => 9,
        AdmissionError::ReserveReplenishmentLimit => 10,
        AdmissionError::HiddenOrderNotSupported => 11,
    });
}

fn decode_admission_error(decoder: &mut Decoder<'_>) -> Result<AdmissionError, CodecError> {
    match decoder.u8()? {
        0 => Ok(AdmissionError::WrongInstrument),
        1 => Ok(AdmissionError::WrongVersion),
        2 => Ok(AdmissionError::TradingStateDisallowsEntry),
        3 => Ok(AdmissionError::PriceOffTickGrid),
        4 => Ok(AdmissionError::PriceOutsideCollar),
        5 => Ok(AdmissionError::QuantityOffGrid),
        6 => Ok(AdmissionError::QuantityOutsideLimits),
        7 => Ok(AdmissionError::ReserveOrderNotSupported),
        8 => Ok(AdmissionError::DisplayQuantityOffGrid),
        9 => Ok(AdmissionError::DisplayQuantityNotLessThanOrder),
        10 => Ok(AdmissionError::ReserveReplenishmentLimit),
        11 => Ok(AdmissionError::HiddenOrderNotSupported),
        tag => Err(CodecError::InvalidTag {
            type_name: "AdmissionError",
            tag,
        }),
    }
}

fn encode_reject_reason(encoder: &mut Encoder, reason: CallAuctionRejectReason) {
    match reason {
        CallAuctionRejectReason::WrongInstrument => encoder.u8(0),
        CallAuctionRejectReason::WrongInstrumentVersion => encoder.u8(1),
        CallAuctionRejectReason::PhaseRevisionMismatch { observed, current } => {
            encoder.u8(2);
            encoder.u64(observed);
            encoder.u64(current);
        }
        CallAuctionRejectReason::InvalidPhaseTransition { from, to } => {
            encoder.u8(3);
            encode_phase(encoder, from);
            encode_phase(encoder, to);
        }
        CallAuctionRejectReason::ActionNotAllowed { action, phase } => {
            encoder.u8(4);
            encode_action(encoder, action);
            encode_phase(encoder, phase);
        }
        CallAuctionRejectReason::AuctionIdMismatch { observed, current } => {
            encoder.u8(5);
            encoder.u64(observed.get());
            encoder.bool(current.is_some());
            if let Some(current) = current {
                encoder.u64(current.get());
            }
        }
        CallAuctionRejectReason::AuctionIdNotNext { expected, observed } => {
            encoder.u8(6);
            encoder.u64(expected.get());
            encoder.u64(observed.get());
        }
        CallAuctionRejectReason::Instrument(error) => {
            encoder.u8(7);
            encode_admission_error(encoder, error);
        }
        CallAuctionRejectReason::DuplicateOrder => encoder.u8(8),
        CallAuctionRejectReason::UnknownOrder => encoder.u8(9),
        CallAuctionRejectReason::NotOrderOwner => encoder.u8(10),
        CallAuctionRejectReason::NoExecutableInterest => encoder.u8(11),
        CallAuctionRejectReason::RiskProfileMissing => encoder.u8(12),
        CallAuctionRejectReason::RiskAccountBlocked => encoder.u8(13),
        CallAuctionRejectReason::RiskReduceOnly => encoder.u8(14),
        CallAuctionRejectReason::RiskOrderQuantityLimit => encoder.u8(15),
        CallAuctionRejectReason::RiskOrderNotionalLimit => encoder.u8(16),
        CallAuctionRejectReason::RiskOpenOrderCountLimit => encoder.u8(17),
        CallAuctionRejectReason::RiskOpenQuantityLimit => encoder.u8(18),
        CallAuctionRejectReason::RiskOpenNotionalLimit => encoder.u8(19),
        CallAuctionRejectReason::RiskPositionLimit => encoder.u8(20),
        CallAuctionRejectReason::RiskArithmeticOverflow => encoder.u8(21),
    }
}

fn decode_reject_reason(decoder: &mut Decoder<'_>) -> Result<CallAuctionRejectReason, CodecError> {
    match decoder.u8()? {
        0 => Ok(CallAuctionRejectReason::WrongInstrument),
        1 => Ok(CallAuctionRejectReason::WrongInstrumentVersion),
        2 => Ok(CallAuctionRejectReason::PhaseRevisionMismatch {
            observed: decoder.u64()?,
            current: decoder.u64()?,
        }),
        3 => Ok(CallAuctionRejectReason::InvalidPhaseTransition {
            from: decode_phase(decoder)?,
            to: decode_phase(decoder)?,
        }),
        4 => Ok(CallAuctionRejectReason::ActionNotAllowed {
            action: decode_action(decoder)?,
            phase: decode_phase(decoder)?,
        }),
        5 => {
            let observed = auction_id(decoder)?;
            let current = if decoder.bool()? {
                Some(auction_id(decoder)?)
            } else {
                None
            };
            Ok(CallAuctionRejectReason::AuctionIdMismatch { observed, current })
        }
        6 => Ok(CallAuctionRejectReason::AuctionIdNotNext {
            expected: auction_id(decoder)?,
            observed: auction_id(decoder)?,
        }),
        7 => Ok(CallAuctionRejectReason::Instrument(decode_admission_error(
            decoder,
        )?)),
        8 => Ok(CallAuctionRejectReason::DuplicateOrder),
        9 => Ok(CallAuctionRejectReason::UnknownOrder),
        10 => Ok(CallAuctionRejectReason::NotOrderOwner),
        11 => Ok(CallAuctionRejectReason::NoExecutableInterest),
        12 => Ok(CallAuctionRejectReason::RiskProfileMissing),
        13 => Ok(CallAuctionRejectReason::RiskAccountBlocked),
        14 => Ok(CallAuctionRejectReason::RiskReduceOnly),
        15 => Ok(CallAuctionRejectReason::RiskOrderQuantityLimit),
        16 => Ok(CallAuctionRejectReason::RiskOrderNotionalLimit),
        17 => Ok(CallAuctionRejectReason::RiskOpenOrderCountLimit),
        18 => Ok(CallAuctionRejectReason::RiskOpenQuantityLimit),
        19 => Ok(CallAuctionRejectReason::RiskOpenNotionalLimit),
        20 => Ok(CallAuctionRejectReason::RiskPositionLimit),
        21 => Ok(CallAuctionRejectReason::RiskArithmeticOverflow),
        tag => Err(CodecError::InvalidTag {
            type_name: "CallAuctionRejectReason",
            tag,
        }),
    }
}

fn encode_command(encoder: &mut Encoder, command: CallAuctionCommand) {
    match command {
        CallAuctionCommand::PhaseControl(value) => {
            encoder.u8(0);
            encoder.u64(value.command_id.get());
            encoder.u64(value.instrument_id.get());
            encoder.u64(value.instrument_version.get());
            encoder.u64(value.auction_id.get());
            encoder.u64(value.expected_phase_revision);
            encode_phase(encoder, value.target_phase);
            encoder.u64(value.received_at.as_unix_nanos());
        }
        CallAuctionCommand::Submit(value) => {
            encoder.u8(1);
            encoder.u64(value.command_id.get());
            encoder.u64(value.auction_id.get());
            encoder.u64(value.expected_phase_revision);
            encode_order(encoder, value.order);
            encoder.u64(value.received_at.as_unix_nanos());
        }
        CallAuctionCommand::Cancel(value) => {
            encoder.u8(2);
            encoder.u64(value.command_id.get());
            encoder.u64(value.instrument_id.get());
            encoder.u64(value.instrument_version.get());
            encoder.u64(value.account_id.get());
            encoder.u64(value.order_id.get());
            encoder.u64(value.received_at.as_unix_nanos());
        }
        CallAuctionCommand::Uncross(value) => {
            encoder.u8(3);
            encoder.u64(value.command_id.get());
            encoder.u64(value.instrument_id.get());
            encoder.u64(value.instrument_version.get());
            encoder.u64(value.auction_id.get());
            encoder.u64(value.expected_phase_revision);
            encoder.i64(value.price_band.minimum().raw());
            encoder.i64(value.price_band.maximum().raw());
            encoder.i64(value.reference_price.raw());
            encode_price_policy(encoder, value.price_policy);
            encode_uncross_policy(encoder, value.uncross_policy);
            encoder.u64(value.received_at.as_unix_nanos());
        }
        CallAuctionCommand::Replace(value) => {
            encoder.u8(4);
            encoder.u64(value.command_id.get());
            encoder.u64(value.auction_id.get());
            encoder.u64(value.expected_phase_revision);
            encoder.u64(value.account_id.get());
            encoder.u64(value.target_order_id.get());
            encode_order(encoder, value.replacement);
            encoder.u64(value.received_at.as_unix_nanos());
        }
    }
}

fn decode_command(decoder: &mut Decoder<'_>) -> Result<CallAuctionCommand, CodecError> {
    match decoder.u8()? {
        0 => Ok(CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
            command_id: command_id(decoder)?,
            instrument_id: instrument(decoder)?,
            instrument_version: instrument_version(decoder)?,
            auction_id: auction_id(decoder)?,
            expected_phase_revision: decoder.u64()?,
            target_phase: decode_phase(decoder)?,
            received_at: TimestampNs::from_unix_nanos(decoder.u64()?),
        })),
        1 => Ok(CallAuctionCommand::Submit(CallAuctionSubmitOrder {
            command_id: command_id(decoder)?,
            auction_id: auction_id(decoder)?,
            expected_phase_revision: decoder.u64()?,
            order: decode_order(decoder)?,
            received_at: TimestampNs::from_unix_nanos(decoder.u64()?),
        })),
        2 => Ok(CallAuctionCommand::Cancel(CallAuctionCancelOrder {
            command_id: command_id(decoder)?,
            instrument_id: instrument(decoder)?,
            instrument_version: instrument_version(decoder)?,
            account_id: account(decoder)?,
            order_id: order(decoder)?,
            received_at: TimestampNs::from_unix_nanos(decoder.u64()?),
        })),
        3 => {
            let command_id = command_id(decoder)?;
            let instrument_id = instrument(decoder)?;
            let instrument_version = instrument_version(decoder)?;
            let auction_id = auction_id(decoder)?;
            let expected_phase_revision = decoder.u64()?;
            let minimum = Price::from_raw(decoder.i64()?);
            let maximum = Price::from_raw(decoder.i64()?);
            let price_band = AuctionPriceBand::from_ordered_prices(minimum, maximum).ok_or(
                CodecError::InvalidValue("call-auction price band is inverted"),
            )?;
            Ok(CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
                command_id,
                instrument_id,
                instrument_version,
                auction_id,
                expected_phase_revision,
                price_band,
                reference_price: Price::from_raw(decoder.i64()?),
                price_policy: decode_price_policy(decoder)?,
                uncross_policy: decode_uncross_policy(decoder)?,
                received_at: TimestampNs::from_unix_nanos(decoder.u64()?),
            }))
        }
        4 => Ok(CallAuctionCommand::Replace(CallAuctionReplaceOrder {
            command_id: command_id(decoder)?,
            auction_id: auction_id(decoder)?,
            expected_phase_revision: decoder.u64()?,
            account_id: account(decoder)?,
            target_order_id: order(decoder)?,
            replacement: decode_order(decoder)?,
            received_at: TimestampNs::from_unix_nanos(decoder.u64()?),
        })),
        tag => Err(CodecError::InvalidTag {
            type_name: "CallAuctionCommand",
            tag,
        }),
    }
}

impl BinaryCodec for CallAuctionCommand {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encode_command(&mut encoder, *self);
        encoder.finish()
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let value = decode_command(&mut decoder)?;
        decoder.finish()?;
        Ok(value)
    }
}

fn encode_trade(encoder: &mut Encoder, trade: CallAuctionTrade) {
    encoder.u64(trade.trade_id().get());
    encoder.u64(trade.buy_order_id().get());
    encoder.u64(trade.buy_account_id().get());
    encoder.u64(trade.sell_order_id().get());
    encoder.u64(trade.sell_account_id().get());
    encoder.i64(trade.price().raw());
    encoder.u64(trade.quantity().lots());
}

fn decode_trade(decoder: &mut Decoder<'_>) -> Result<CallAuctionTrade, CodecError> {
    CallAuctionTrade::from_decoded(
        trade_id(decoder)?,
        order(decoder)?,
        account(decoder)?,
        order(decoder)?,
        account(decoder)?,
        Price::from_raw(decoder.i64()?),
        quantity(decoder)?,
    )
    .ok_or(CodecError::InvalidValue(
        "call-auction trade uses one order on both sides",
    ))
}

fn encode_cancellation(encoder: &mut Encoder, value: CallAuctionCancellation) {
    encoder.u64(value.order_id().get());
    encoder.u64(value.account_id().get());
    encode_side(encoder, value.side());
    encode_constraint(encoder, value.constraint());
    encoder.u64(value.quantity().lots());
    encoder.u64(value.executed_quantity_lots());
}

fn decode_cancellation(decoder: &mut Decoder<'_>) -> Result<CallAuctionCancellation, CodecError> {
    let order_id = order(decoder)?;
    let account_id = account(decoder)?;
    let side = decode_side(decoder)?;
    let constraint = decode_constraint(decoder)?;
    let quantity = quantity(decoder)?;
    let executed_quantity_lots = decoder.u64()?;
    quantity
        .lots()
        .checked_add(executed_quantity_lots)
        .ok_or(CodecError::InvalidValue(
            "call-auction cancellation source quantity overflows",
        ))?;
    Ok(CallAuctionCancellation::from_decoded(
        order_id,
        account_id,
        side,
        constraint,
        quantity,
        executed_quantity_lots,
    ))
}

fn encode_clearing(encoder: &mut Encoder, clearing: AuctionClearing) {
    encoder.i64(clearing.price().raw());
    encoder.u128(clearing.buy_quantity());
    encoder.u128(clearing.sell_quantity());
}

fn decode_clearing(decoder: &mut Decoder<'_>) -> Result<AuctionClearing, CodecError> {
    let clearing = AuctionClearing::from_quantities(
        Price::from_raw(decoder.i64()?),
        decoder.u128()?,
        decoder.u128()?,
    );
    if clearing.executable_quantity() == 0 {
        return Err(CodecError::InvalidValue(
            "call-auction completion has zero executable quantity",
        ));
    }
    Ok(clearing)
}

fn encode_cancellation_reason(encoder: &mut Encoder, reason: CallAuctionCancellationReason) {
    encoder.u8(match reason {
        CallAuctionCancellationReason::UserRequested => 0,
        CallAuctionCancellationReason::UncrossRemainder => 1,
        CallAuctionCancellationReason::Replaced => 2,
    });
}

fn decode_cancellation_reason(
    decoder: &mut Decoder<'_>,
) -> Result<CallAuctionCancellationReason, CodecError> {
    match decoder.u8()? {
        0 => Ok(CallAuctionCancellationReason::UserRequested),
        1 => Ok(CallAuctionCancellationReason::UncrossRemainder),
        2 => Ok(CallAuctionCancellationReason::Replaced),
        tag => Err(CodecError::InvalidTag {
            type_name: "CallAuctionCancellationReason",
            tag,
        }),
    }
}

fn encode_event_kind(encoder: &mut Encoder, kind: CallAuctionEventKind) {
    match kind {
        CallAuctionEventKind::PhaseChanged {
            auction_id,
            previous,
            current,
            revision,
        } => {
            encoder.u8(0);
            encoder.u64(auction_id.get());
            encode_phase(encoder, previous);
            encode_phase(encoder, current);
            encoder.u64(revision);
        }
        CallAuctionEventKind::OrderAccepted(snapshot) => {
            encoder.u8(1);
            encode_snapshot(encoder, snapshot);
        }
        CallAuctionEventKind::OrderCancelled { order, reason } => {
            encoder.u8(2);
            encode_snapshot(encoder, order);
            encode_cancellation_reason(encoder, reason);
        }
        CallAuctionEventKind::Trade(trade) => {
            encoder.u8(3);
            encode_trade(encoder, trade);
        }
        CallAuctionEventKind::RemainderCancelled(cancellation) => {
            encoder.u8(4);
            encode_cancellation(encoder, cancellation);
        }
        CallAuctionEventKind::UncrossCompleted {
            auction_id,
            clearing,
            policy,
            trade_count,
            cancellation_count,
            book_revision,
            phase_revision,
        } => {
            encoder.u8(5);
            encoder.u64(auction_id.get());
            encode_clearing(encoder, clearing);
            encode_uncross_policy(encoder, policy);
            encoder.u64(trade_count);
            encoder.u64(cancellation_count);
            encoder.u64(book_revision);
            encoder.u64(phase_revision);
        }
        CallAuctionEventKind::CommandRejected(reason) => {
            encoder.u8(6);
            encode_reject_reason(encoder, reason);
        }
    }
}

fn decode_event_kind(decoder: &mut Decoder<'_>) -> Result<CallAuctionEventKind, CodecError> {
    match decoder.u8()? {
        0 => Ok(CallAuctionEventKind::PhaseChanged {
            auction_id: auction_id(decoder)?,
            previous: decode_phase(decoder)?,
            current: decode_phase(decoder)?,
            revision: decoder.u64()?,
        }),
        1 => Ok(CallAuctionEventKind::OrderAccepted(decode_snapshot(
            decoder,
        )?)),
        2 => Ok(CallAuctionEventKind::OrderCancelled {
            order: decode_snapshot(decoder)?,
            reason: decode_cancellation_reason(decoder)?,
        }),
        3 => Ok(CallAuctionEventKind::Trade(decode_trade(decoder)?)),
        4 => Ok(CallAuctionEventKind::RemainderCancelled(
            decode_cancellation(decoder)?,
        )),
        5 => Ok(CallAuctionEventKind::UncrossCompleted {
            auction_id: auction_id(decoder)?,
            clearing: decode_clearing(decoder)?,
            policy: decode_uncross_policy(decoder)?,
            trade_count: decoder.u64()?,
            cancellation_count: decoder.u64()?,
            book_revision: decoder.u64()?,
            phase_revision: decoder.u64()?,
        }),
        6 => Ok(CallAuctionEventKind::CommandRejected(decode_reject_reason(
            decoder,
        )?)),
        tag => Err(CodecError::InvalidTag {
            type_name: "CallAuctionEventKind",
            tag,
        }),
    }
}

fn encode_event(encoder: &mut Encoder, event: CallAuctionEvent) {
    encoder.u64(event.sequence);
    encoder.u64(event.command_id.get());
    encoder.u64(event.occurred_at.as_unix_nanos());
    encode_event_kind(encoder, event.kind);
}

fn decode_event(decoder: &mut Decoder<'_>) -> Result<CallAuctionEvent, CodecError> {
    Ok(CallAuctionEvent {
        sequence: decoder.u64()?,
        command_id: command_id(decoder)?,
        occurred_at: TimestampNs::from_unix_nanos(decoder.u64()?),
        kind: decode_event_kind(decoder)?,
    })
}

impl BinaryCodec for CallAuctionExecutionReport {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.command_id.get());
        encoder.u64(self.command_sequence);
        match self.outcome {
            CallAuctionCommandOutcome::Accepted => encoder.u8(0),
            CallAuctionCommandOutcome::Rejected(reason) => {
                encoder.u8(1);
                encode_reject_reason(&mut encoder, reason);
            }
        }
        encoder.length("call-auction events", self.events.len())?;
        for event in self.events.iter().copied() {
            encode_event(&mut encoder, event);
        }
        encoder.bool(self.replayed);
        encoder.finish()
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let command_id = command_id(&mut decoder)?;
        let command_sequence = decoder.u64()?;
        let outcome = match decoder.u8()? {
            0 => CallAuctionCommandOutcome::Accepted,
            1 => CallAuctionCommandOutcome::Rejected(decode_reject_reason(&mut decoder)?),
            tag => {
                return Err(CodecError::InvalidTag {
                    type_name: "CallAuctionCommandOutcome",
                    tag,
                });
            }
        };
        let count = decoder.count("call-auction events", 25)?;
        let mut events = reserve_decoded_vec("call-auction events", count)?;
        for _ in 0..count {
            events.push(decode_event(&mut decoder)?);
        }
        let replayed = decoder.bool()?;
        decoder.finish()?;
        CallAuctionExecutionReport::from_decoded(
            command_id,
            command_sequence,
            outcome,
            events,
            replayed,
        )
        .map_err(CodecError::InvalidValue)
    }
}

impl BinaryCodec for CallAuctionCheckpoint {
    fn encode(&self) -> Result<Vec<u8>, CodecError> {
        let mut encoder = Encoder::default();
        encoder.u64(self.wal_metadata_sequence());
        encoder.u64(self.generation());
        encode_sized_bytes(
            &mut encoder,
            "auction checkpoint definition bytes",
            &self.definition().encode()?,
        )?;
        let phase = self.phase();
        encode_phase(&mut encoder, phase.phase());
        encoder.u64(phase.revision());
        encode_optional_auction_id(&mut encoder, phase.active_auction_id());
        encode_optional_auction_id(&mut encoder, phase.last_auction_id());
        encoder.u64(self.book_revision());
        encoder.u64(self.next_priority_sequence());
        encoder.u64(self.next_trade_id());
        encoder.length(
            "auction checkpoint accepted identities",
            self.accepted_order_ids().len(),
        )?;
        for order_id in self.accepted_order_ids() {
            encoder.u64(order_id.get());
        }
        encoder.length(
            "auction checkpoint active orders",
            self.active_orders().len(),
        )?;
        for order in self.active_orders() {
            encode_snapshot(&mut encoder, *order);
        }
        encoder.length("auction checkpoint command history", self.history().len())?;
        for entry in self.history() {
            encode_sized_bytes(
                &mut encoder,
                "auction checkpoint command bytes",
                &entry.command().encode()?,
            )?;
            encode_sized_bytes(
                &mut encoder,
                "auction checkpoint report bytes",
                &entry.report().encode()?,
            )?;
        }
        encoder.finish()
    }

    fn decode(bytes: &[u8]) -> Result<Self, CodecError> {
        let mut decoder = Decoder::new(bytes);
        let wal_metadata_sequence = decoder.u64()?;
        let wal_sequence = decoder.u64()?;
        let definition = InstrumentDefinition::decode(
            decoder.sized_bytes("auction checkpoint definition bytes")?,
        )?;
        let phase = decode_phase(&mut decoder)?;
        let phase_revision = decoder.u64()?;
        let active_auction_id = decode_optional_auction_id(&mut decoder)?;
        let last_auction_id = decode_optional_auction_id(&mut decoder)?;
        let book_revision = decoder.u64()?;
        let next_priority_sequence = decoder.u64()?;
        let next_trade_id = decoder.u64()?;

        let accepted_count = decoder.count("auction checkpoint accepted identities", 8)?;
        let mut accepted_order_ids =
            reserve_decoded_vec("auction checkpoint accepted identities", accepted_count)?;
        for _ in 0..accepted_count {
            accepted_order_ids.push(order(&mut decoder)?);
        }

        let active_count = decoder.count("auction checkpoint active orders", 34)?;
        let mut active_orders =
            reserve_decoded_vec("auction checkpoint active orders", active_count)?;
        for _ in 0..active_count {
            active_orders.push(decode_snapshot(&mut decoder)?);
        }

        let history_count = decoder.count("auction checkpoint command history", 8)?;
        let mut history = reserve_decoded_vec("auction checkpoint command history", history_count)?;
        for _ in 0..history_count {
            let command = CallAuctionCommand::decode(
                decoder.sized_bytes("auction checkpoint command bytes")?,
            )?;
            let report = CallAuctionExecutionReport::decode(
                decoder.sized_bytes("auction checkpoint report bytes")?,
            )?;
            history.push(CallAuctionCommandReportCheckpoint::from_parts(
                command, report,
            ));
        }
        decoder.finish()?;
        CallAuctionCheckpoint::from_parts(
            wal_metadata_sequence,
            wal_sequence,
            definition,
            CallAuctionPhaseSnapshot::from_parts(
                phase,
                phase_revision,
                active_auction_id,
                last_auction_id,
            ),
            book_revision,
            next_priority_sequence,
            next_trade_id,
            accepted_order_ids,
            active_orders,
            history,
        )
        .map_err(Into::into)
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

#[cfg(test)]
mod tests {
    use super::{AdmissionError, Decoder, Encoder, decode_admission_error, encode_admission_error};

    #[test]
    fn hidden_admission_error_has_stable_tag_eleven() {
        let mut encoder = Encoder::default();
        encode_admission_error(&mut encoder, AdmissionError::HiddenOrderNotSupported);
        assert_eq!(encoder.finish().unwrap(), [11]);

        let mut decoder = Decoder::new(&[11]);
        assert_eq!(
            decode_admission_error(&mut decoder).unwrap(),
            AdmissionError::HiddenOrderNotSupported
        );
        decoder.finish().unwrap();
    }
}
