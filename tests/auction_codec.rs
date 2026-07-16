use quotick::auction::{
    AuctionAllocationPolicy, AuctionOrderConstraint, AuctionPriceGrid, AuctionPricePolicy,
};
use quotick::auction_book::{
    CallAuctionOrder, CallAuctionRemainderPolicy, CallAuctionSelfTradePolicy,
    CallAuctionUncrossPolicy,
};
use quotick::auction_engine::{
    CallAuctionAmendOrder, CallAuctionCancelOrder, CallAuctionCommand, CallAuctionEngine,
    CallAuctionExecutionReport, CallAuctionIndicativeCommand, CallAuctionMassCancel,
    CallAuctionPhase, CallAuctionPhaseControl, CallAuctionReplaceOrder, CallAuctionSubmitOrder,
    CallAuctionUncrossCommand,
};
use quotick::codec::{BinaryCodec, CodecError};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::MassCancelScope;
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
        hidden_orders_supported: false,
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
    auction_order_with_class(order_id, account_id, side, lots, 0)
}

fn auction_order_with_class(
    order_id: u64,
    account_id: u64,
    side: Side,
    lots: u64,
    priority_class: u16,
) -> CallAuctionOrder {
    CallAuctionOrder::new(
        OrderId::new(order_id).unwrap(),
        AccountId::new(account_id).unwrap(),
        InstrumentId::new(7).unwrap(),
        InstrumentVersion::new(2).unwrap(),
        side,
        AuctionOrderConstraint::Limit(Price::from_raw(100)),
        Quantity::new(lots).unwrap(),
        quotick::auction::AuctionPriorityClass::new(priority_class),
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

fn mass_cancel(command_id: u64, account_id: u64, scope: MassCancelScope) -> CallAuctionCommand {
    CallAuctionCommand::MassCancel(CallAuctionMassCancel {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(7).unwrap(),
        instrument_version: InstrumentVersion::new(2).unwrap(),
        account_id: AccountId::new(account_id).unwrap(),
        scope,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn amend(command_id: u64, account_id: u64, order_id: u64, lots: u64) -> CallAuctionCommand {
    CallAuctionCommand::Amend(CallAuctionAmendOrder {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(7).unwrap(),
        instrument_version: InstrumentVersion::new(2).unwrap(),
        auction_id: auction_id(),
        expected_phase_revision: 1,
        account_id: AccountId::new(account_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        new_quantity: Quantity::new(lots).unwrap(),
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn replace(
    command_id: u64,
    target_order_id: u64,
    replacement: CallAuctionOrder,
) -> CallAuctionCommand {
    CallAuctionCommand::Replace(CallAuctionReplaceOrder {
        command_id: CommandId::new(command_id).unwrap(),
        auction_id: auction_id(),
        expected_phase_revision: 1,
        account_id: replacement.account_id(),
        target_order_id: OrderId::new(target_order_id).unwrap(),
        replacement,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn uncross(command_id: u64) -> CallAuctionCommand {
    uncross_with_allocation(command_id, AuctionAllocationPolicy::PriceTime)
}

fn indicative(command_id: u64, phase_revision: u64) -> CallAuctionCommand {
    let grid = AuctionPriceGrid::new(1).unwrap();
    CallAuctionCommand::Indicative(CallAuctionIndicativeCommand {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: InstrumentId::new(7).unwrap(),
        instrument_version: InstrumentVersion::new(2).unwrap(),
        auction_id: auction_id(),
        expected_phase_revision: phase_revision,
        price_band: quotick::auction::AuctionPriceBand::new(
            grid,
            Price::from_raw(0),
            Price::from_raw(1_000),
        )
        .unwrap(),
        reference_price: Price::from_raw(100),
        price_policy: AuctionPricePolicy::PRESSURE_THEN_REFERENCE_HIGHER,
        received_at: TimestampNs::from_unix_nanos(command_id),
    })
}

fn uncross_with_allocation(
    command_id: u64,
    allocation: AuctionAllocationPolicy,
) -> CallAuctionCommand {
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
            allocation,
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
        replace(4, 1, auction_order(2, 1, Side::Sell, 3)),
        uncross(5),
        uncross_with_allocation(8, AuctionAllocationPolicy::ProRataTime),
        mass_cancel(6, 1, MassCancelScope::Side(Side::Buy)),
        amend(7, 1, 1, 4),
        indicative(9, 1),
    ];
    for command in commands {
        let encoded = command.encode().unwrap();
        assert_eq!(CallAuctionCommand::decode(&encoded).unwrap(), command);
    }
}

#[test]
fn call_auction_indicative_command_and_nullable_event_have_stable_layouts() {
    let command = indicative(2, 1);
    let encoded_command = command.encode().unwrap();
    assert_eq!(encoded_command.len(), 75);
    assert_eq!(encoded_command[0], 7);
    assert_eq!(&encoded_command[1..9], &2_u64.to_le_bytes());
    assert_eq!(&encoded_command[41..49], &0_i64.to_le_bytes());
    assert_eq!(&encoded_command[49..57], &1_000_i64.to_le_bytes());
    assert_eq!(&encoded_command[57..65], &100_i64.to_le_bytes());
    assert_eq!(encoded_command[65..67], [1, 1]);
    assert_eq!(
        CallAuctionCommand::decode(&encoded_command).unwrap(),
        command
    );

    let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
    engine
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    let report = engine.submit(command).unwrap();
    let encoded_report = report.encode().unwrap();
    assert_eq!(encoded_report.len(), 98);
    assert_eq!(encoded_report[45], 9);
    assert_eq!(encoded_report[96], 0); // No executable clearing.
    assert_eq!(
        CallAuctionExecutionReport::decode(&encoded_report).unwrap(),
        report
    );

    let mut zero_phase_revision = encoded_report;
    zero_phase_revision[54..62].fill(0);
    assert_eq!(
        CallAuctionExecutionReport::decode(&zero_phase_revision),
        Err(CodecError::InvalidValue(
            "call-auction indicative state is structurally invalid"
        ))
    );

    engine
        .submit(submit(3, auction_order(1, 1, Side::Buy, 3)))
        .unwrap();
    engine
        .submit(submit(4, auction_order(2, 2, Side::Sell, 3)))
        .unwrap();
    let crossed_report = engine.submit(indicative(5, 1)).unwrap();
    let crossed_bytes = crossed_report.encode().unwrap();
    assert_eq!(crossed_bytes.len(), 138);
    assert_eq!(crossed_bytes[45], 9);
    assert_eq!(crossed_bytes[96], 1);
    assert_eq!(
        CallAuctionExecutionReport::decode(&crossed_bytes).unwrap(),
        crossed_report
    );
}

#[test]
fn call_auction_submit_command_has_a_stable_priority_class_scalar() {
    let value = submit(2, auction_order_with_class(1, 1, Side::Buy, 5, 0x1234));
    let encoded = value.encode().unwrap();

    assert_eq!(encoded.len(), 85);
    assert_eq!(&encoded[75..77], &0x1234_u16.to_le_bytes());
    assert_eq!(CallAuctionCommand::decode(&encoded).unwrap(), value);
}

#[test]
fn call_auction_accept_event_has_a_stable_priority_class_scalar() {
    let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
    engine
        .submit(phase(1, 0, CallAuctionPhase::Collecting))
        .unwrap();
    let report = engine
        .submit(submit(
            2,
            auction_order_with_class(1, 1, Side::Buy, 5, 0x1234),
        ))
        .unwrap();
    let encoded = report.encode().unwrap();

    assert_eq!(encoded.len(), 91);
    assert_eq!(&encoded[80..82], &0x1234_u16.to_le_bytes());
    assert_eq!(
        CallAuctionExecutionReport::decode(&encoded).unwrap(),
        report
    );
}

#[test]
fn call_auction_uncross_policy_has_stable_explicit_allocation_tags() {
    let price_time = uncross(5).encode().unwrap();
    let pro_rata = uncross_with_allocation(5, AuctionAllocationPolicy::ProRataTime)
        .encode()
        .unwrap();

    assert_eq!(price_time.len(), 78);
    assert_eq!(price_time[65..70], [1, 1, 0, 2, 0]);
    assert_eq!(pro_rata[65..70], [1, 1, 1, 2, 0]);
    assert_eq!(
        CallAuctionCommand::decode(&pro_rata).unwrap(),
        uncross_with_allocation(5, AuctionAllocationPolicy::ProRataTime)
    );
}

#[test]
fn pro_rata_completion_has_a_stable_explicit_allocation_tag() {
    let mut engine = CallAuctionEngine::try_new(definition()).unwrap();
    for command in [
        phase(1, 0, CallAuctionPhase::Collecting),
        submit(2, auction_order(1, 1, Side::Buy, 10)),
        submit(3, auction_order(2, 2, Side::Buy, 20)),
        submit(4, auction_order(3, 3, Side::Sell, 15)),
        phase(5, 1, CallAuctionPhase::Frozen),
    ] {
        engine.submit(command).unwrap();
    }
    let report = engine
        .submit(uncross_with_allocation(
            6,
            AuctionAllocationPolicy::ProRataTime,
        ))
        .unwrap();
    let encoded = report.encode().unwrap();
    let completion_start = encoded.len() - 1 - 108;

    assert_eq!(encoded[completion_start + 24], 5);
    assert_eq!(
        encoded[completion_start + 73..completion_start + 76],
        [1, 2, 0]
    );
    assert_eq!(
        CallAuctionExecutionReport::decode(&encoded).unwrap(),
        report
    );
}

#[test]
fn call_auction_amend_command_has_a_stable_little_endian_layout() {
    let value = amend(9, 11, 13, 17);
    let encoded = value.encode().unwrap();
    let mut expected = vec![6];
    for field in [9_u64, 7, 2, 1, 1, 11, 13, 17, 9] {
        expected.extend_from_slice(&field.to_le_bytes());
    }
    assert_eq!(encoded.len(), 73);
    assert_eq!(encoded, expected);
    assert_eq!(CallAuctionCommand::decode(&encoded).unwrap(), value);
}

#[test]
fn call_auction_amend_report_has_a_stable_event_layout() {
    let report = representative_reports().remove(2);
    let encoded = report.encode().unwrap();
    let report_prefix = 8 + 8 + 1 + 4;
    let event_prefix = 8 + 8 + 8;
    assert_eq!(encoded.len(), report_prefix + 85 + 1);
    assert_eq!(encoded[report_prefix + event_prefix], 8);
    assert_eq!(
        CallAuctionExecutionReport::decode(&encoded).unwrap(),
        report
    );
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
        engine.submit(amend(12, 1, 1, 4)).unwrap(),
        engine.submit(amend(13, 1, 1, 4)).unwrap(),
        engine
            .submit(replace(3, 1, auction_order(10, 1, Side::Buy, 5)))
            .unwrap(),
        engine
            .submit(submit(4, auction_order(2, 2, Side::Sell, 3)))
            .unwrap(),
        engine
            .submit(submit(5, auction_order(3, 3, Side::Buy, 1)))
            .unwrap(),
        engine.submit(cancel(6, 3, 3)).unwrap(),
        engine
            .submit(submit(7, auction_order(4, 4, Side::Buy, 1)))
            .unwrap(),
        engine
            .submit(mass_cancel(8, 4, MassCancelScope::All))
            .unwrap(),
        engine
            .submit(phase(9, 1, CallAuctionPhase::Frozen))
            .unwrap(),
        engine.submit(uncross(10)).unwrap(),
        engine
            .submit(CallAuctionCommand::Submit(CallAuctionSubmitOrder {
                command_id: CommandId::new(11).unwrap(),
                auction_id: auction_id(),
                expected_phase_revision: 3,
                order: auction_order(5, 5, Side::Buy, 1),
                received_at: TimestampNs::from_unix_nanos(11),
            }))
            .unwrap(),
    ]
}

#[test]
fn every_call_auction_event_shape_round_trips() {
    let reports = representative_reports();
    assert!(reports.iter().any(|report| report.events.len() == 2));
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

    let mut invalid_allocation = uncross(1).encode().unwrap();
    invalid_allocation[67] = 255;
    assert_eq!(
        CallAuctionCommand::decode(&invalid_allocation),
        Err(CodecError::InvalidTag {
            type_name: "AuctionAllocationPolicy",
            tag: 255,
        })
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

    let uncross = representative_reports().remove(11).encode().unwrap();
    let mut self_pair = uncross.clone();
    self_pair[70..78].copy_from_slice(&uncross[54..62]);
    assert_eq!(
        CallAuctionExecutionReport::decode(&self_pair),
        Err(CodecError::InvalidValue(
            "call-auction trade uses one order on both sides"
        ))
    );

    let completion_start = uncross.len() - 1 - 108;
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
