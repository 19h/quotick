use quotick::auction::{AuctionOrderConstraint, AuctionPriceGrid, AuctionPricePolicy};
use quotick::auction_book::{
    CallAuctionOrder, CallAuctionRemainderPolicy, CallAuctionSelfTradePolicy,
    CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionCancelOrder, CallAuctionCommand, CallAuctionEngine, CallAuctionExecutionReport,
    CallAuctionPhase, CallAuctionPhaseControl, CallAuctionSubmitOrder, CallAuctionUncrossCommand,
};
use quotick::codec::{BinaryCodec, CodecError};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::{
    AccountId, AssetId, AuctionId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price,
    Quantity, Side, TimestampNs,
};

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(7).unwrap(),
        version: InstrumentVersion::new(2).unwrap(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("AUCT").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(0), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Closed,
    })
    .unwrap()
}

fn auction_id() -> AuctionId {
    AuctionId::new(1).unwrap()
}

fn phase(command_id: u64, revision: u64, target: CallAuctionPhase) -> CallAuctionCommand {
    CallAuctionCommand::PhaseControl(CallAuctionPhaseControl {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(7).unwrap(),
        instrument_version: InstrumentVersion::new(2).unwrap(),
        auction_id: auction_id(),
        expected_phase_revision: revision,
        target_phase: target,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn auction_order(order_id: u64, account_id: u64, side: Side, lots: u64) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        AccountId::new(account_id).unwrap(),
        InstrumentId::new(7).unwrap(),
        InstrumentVersion::new(2).unwrap(),
        side,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        Quantity::new(lots).unwrap(),
    )
}

fn submit(command_id: u64, order: CallAuctionOrder) -> CallAuctionCommand {
    CallAuctionCommand::Submit(CallAuctionSubmitOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction_id(),
        expected_phase_revision: 1,
        order,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn cancel(command_id: u64, account_id: u64, order_id: u64) -> CallAuctionCommand {
    CallAuctionCommand::Cancel(CallAuctionCancelOrder {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(7).unwrap(),
        instrument_version: InstrumentVersion::new(2).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn uncross(command_id: u64) -> CallAuctionCommand {
    let grid = AuctionPriceGrid::new(1).unwrap();
    CallAuctionCommand::Uncross(CallAuctionUncrossCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(7).unwrap(),
        instrument_version: InstrumentVersion::new(2).unwrap(),
        auction_id: auction_id(),
        expected_phase_revision: 2,
        price_band: quotick::auction::AuctionPriceBand::new(
            grid,
            Price::from_raw(0),
            Price::from_raw(1_000),
        )
        .unwrap(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::PRESSURE_THEN_REFERENCE_HIGHER,
        uncross_policy: CallAuctionUncrossPolicy::new(
            CallAuctionRemainderPolicy::CancelAll,
            CallAuctionSelfTradePolicy::Permit,
        ),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

#[test]
fn call_auction_phase_command_has_a_stable_little_endian_layout() {
    let value = phase(1, 4, CallAuctionPhase::Frozen);
    let encoded = value.encode().unwrap();
    let mut expected = vec![0];
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.extend_from_slice(&7_u64.to_le_bytes());
    expected.extend_from_slice(&2_u64.to_le_bytes());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.extend_from_slice(&4_u64.to_le_bytes());
    expected.push(2);
    expected.extend_from_slice(&1_u64.to_le_bytes());
    assert_eq!(encoded, expected);
    assert_eq!(CallAuctionCommand::decode(&encoded).unwrap(), value);
}

#[test]
fn every_call_auction_command_variant_round_trips() {
    let commands = [
        phase(1, 0, CallAuctionPhase::Collecting),
        submit(2, auction_order(1, 1, Side::Buy, 5)),
        cancel(3, 1, 1),
        uncross(4),
    ];
    for command in commands {
        let encoded = command.encode().unwrap();
        assert_eq!(CallAuctionCommand::decode(&encoded).unwrap(), command);
    }
}

fn representative_reports() -> Vec<CallAuctionExecutionReport> {
    let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
    vec![
        engine
            .submit(phase(1, 0, CallAuctionPhase::Collecting))
            .unwrap(),
        engine
            .submit(submit(2, auction_order(1, 1, Side::Buy, 5)))
            .unwrap(),
        engine
            .submit(submit(3, auction_order(2, 2, Side::Sell, 3)))
            .unwrap(),
        engine
            .submit(submit(4, auction_order(3, 3, Side::Buy, 1)))
            .unwrap(),
        engine.submit(cancel(5, 3, 3)).unwrap(),
        engine
            .submit(phase(6, 1, CallAuctionPhase::Frozen))
            .unwrap(),
        engine.submit(uncross(7)).unwrap(),
        engine
            .submit(CallAuctionCommand::Submit(CallAuctionSubmitOrder {
                command_id: CommandId::new(8).unwrap(),
                auction_id: auction_id(),
                expected_phase_revision: 3,
                order: auction_order(4, 4, Side::Buy, 1),
                received_at: TimestampNs::from_unix_nanos(8),
            }))
            .unwrap(),
    ]
}

#[test]
fn every_call_auction_event_shape_round_trips() {
    let reports = representative_reports();
    assert!(reports.iter().any(|report| report.events.len() == 3));
    for report in reports {
        let encoded = report.encode().unwrap();
        assert_eq!(
            CallAuctionExecutionReport::decode(&encoded).unwrap(),
            report
        );
    }
}

#[test]
fn call_auction_phase_report_has_a_stable_little_endian_layout() {
    let report = representative_reports().remove(0);
    let encoded = report.encode().unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.push(0);
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.push(0);
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.push(0);
    expected.push(1);
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.push(0);
    assert_eq!(encoded, expected);
}

#[test]
fn call_auction_codecs_reject_invalid_tags_lengths_bands_and_report_grammar() {
    assert!(matches!(
        CallAuctionCommand::decode(&[255]),
        Err(CodecError::InvalidTag {
            type_name: "CallAuctionCommand",
            tag: 255
        })
    ));

    let mut invalid_band = uncross(1).encode().unwrap();
    invalid_band[41..49].copy_from_slice(&100_i64.to_le_bytes());
    invalid_band[49..57].copy_from_slice(&0_i64.to_le_bytes());
    assert_eq!(
        CallAuctionCommand::decode(&invalid_band),
        Err(CodecError::InvalidValue(
            "call-auction price band is inverted"
        ))
    );

    let report = representative_reports().remove(0);
    let mut zero_sequence = report.encode().unwrap();
    zero_sequence[8..16].copy_from_slice(&0_u64.to_le_bytes());
    assert_eq!(
        CallAuctionExecutionReport::decode(&zero_sequence),
        Err(CodecError::InvalidValue(
            "call-auction report has a zero sequence or empty event trace"
        ))
    );

    let mut impossible_count = report.encode().unwrap();
    impossible_count[17..21].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        CallAuctionExecutionReport::decode(&impossible_count),
        Err(CodecError::InvalidLength {
            field: "call-auction events",
            ..
        })
    ));

    let mut unchanged_phase = report.encode().unwrap();
    unchanged_phase[55] = unchanged_phase[54];
    assert_eq!(
        CallAuctionExecutionReport::decode(&unchanged_phase),
        Err(CodecError::InvalidValue(
            "call-auction accepted report grammar is inconsistent"
        ))
    );
    let mut invalid_phase_edge = report.encode().unwrap();
    invalid_phase_edge[55] = 2;
    assert_eq!(
        CallAuctionExecutionReport::decode(&invalid_phase_edge),
        Err(CodecError::InvalidValue(
            "call-auction accepted report grammar is inconsistent"
        ))
    );

    let uncross = representative_reports().remove(6).encode().unwrap();
    let mut self_pair = uncross.clone();
    self_pair[70..78].copy_from_slice(&uncross[54..62]);
    assert_eq!(
        CallAuctionExecutionReport::decode(&self_pair),
        Err(CodecError::InvalidValue(
            "call-auction trade uses one order on both sides"
        ))
    );

    let completion_start = uncross.len() - 1 - 107;
    let mut zero_execution = uncross.clone();
    zero_execution[completion_start + 41..completion_start + 73].fill(0);
    assert_eq!(
        CallAuctionExecutionReport::decode(&zero_execution),
        Err(CodecError::InvalidValue(
            "call-auction completion has zero executable quantity"
        ))
    );

    let mut cancellation_overflow = uncross.clone();
    cancellation_overflow[153..161].copy_from_slice(&u64::MAX.to_le_bytes());
    assert_eq!(
        CallAuctionExecutionReport::decode(&cancellation_overflow),
        Err(CodecError::InvalidValue(
            "call-auction cancellation source quantity overflows"
        ))
    );

    for end in 0..report.encode().unwrap().len() {
        assert!(CallAuctionExecutionReport::decode(&report.encode().unwrap()[..end]).is_err());
    }
}
