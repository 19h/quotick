use quotick::codec::{BinaryCodec, CodecError};
use quotick::instrument::{
    InstrumentDefinition, InstrumentError, InstrumentKind, InstrumentSpec, InstrumentSymbol,
    PriceRules, QuantityRules, TradingState,
};
use quotick::ledger::{JournalEntry, Posting};
use quotick::market_data::{
    MarketDataError, MarketDataPublisher, MarketDataReplica, MarketDataSnapshot, MarketDataUpdate,
};
use quotick::matching::{
    CancelOrder, CancelReason, Command, CommandOutcome, Event, EventKind, ExecutionReport,
    NewOrder, OrderBook, OrderType, RejectReason, ReplaceOrder, SelfTradePrevention, TimeInForce,
    Trade,
};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskError, RiskLimitSpec, RiskLimits, RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs, TradeId, TransactionId,
};

fn id<T>(result: Result<T, quotick::domain::DomainError>) -> T {
    result.expect("test identifier is non-zero")
}

fn version() -> InstrumentVersion {
    id(InstrumentVersion::new(1))
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: id(InstrumentId::new(4)),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("TEST").expect("symbol"),
        kind: InstrumentKind::Spot,
        base_asset_id: id(AssetId::new(1)),
        quote_asset_id: id(AssetId::new(2)),
        price: PriceRules::new(0, 1, Price::from_raw(i64::MIN), Price::from_raw(i64::MAX))
            .expect("price rules"),
        quantity: QuantityRules::new(1, 1, u64::MAX).expect("quantity rules"),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .expect("definition")
}

fn command(command_id: u64, order_id: u64, side: Side) -> Command {
    Command::New(NewOrder {
        command_id: id(CommandId::new(command_id)),
        order_id: id(OrderId::new(order_id)),
        account_id: id(AccountId::new(3)),
        instrument_id: id(InstrumentId::new(4)),
        instrument_version: version(),
        side,
        quantity: Quantity::new(5).expect("positive quantity"),
        order_type: OrderType::Limit(Price::from_raw(-7)),
        time_in_force: TimeInForce::ImmediateOrCancel,
        self_trade_prevention: SelfTradePrevention::CancelBoth,
        received_at: TimestampNs::from_unix_nanos(9),
    })
}

#[test]
fn command_codec_has_a_stable_little_endian_layout() {
    let encoded = command(1, 2, Side::Buy).encode().expect("command encodes");
    let mut expected = vec![0];
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.extend_from_slice(&2_u64.to_le_bytes());
    expected.extend_from_slice(&3_u64.to_le_bytes());
    expected.extend_from_slice(&4_u64.to_le_bytes());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.push(0);
    expected.extend_from_slice(&5_u64.to_le_bytes());
    expected.push(1);
    expected.extend_from_slice(&(-7_i64).to_le_bytes());
    expected.push(1);
    expected.push(2);
    expected.extend_from_slice(&9_u64.to_le_bytes());

    assert_eq!(encoded, expected);
    assert_eq!(
        Command::decode(&encoded).expect("valid command"),
        command(1, 2, Side::Buy)
    );
}

#[test]
fn market_data_codecs_round_trip_stable_incrementals_and_full_depth_snapshots() {
    let mut book = OrderBook::new(definition());
    let mut publisher = MarketDataPublisher::from_book(&book).expect("publisher bootstraps");
    let genesis = publisher.snapshot();
    let resting = Command::New(NewOrder {
        command_id: id(CommandId::new(90)),
        order_id: id(OrderId::new(91)),
        account_id: id(AccountId::new(3)),
        instrument_id: id(InstrumentId::new(4)),
        instrument_version: version(),
        side: Side::Buy,
        quantity: Quantity::new(5).expect("quantity"),
        order_type: OrderType::Limit(Price::from_raw(-7)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelBoth,
        received_at: TimestampNs::from_unix_nanos(9),
    });
    let report = book.submit(resting).expect("matching succeeds");
    let batch = publisher
        .publish(resting, &report, &book)
        .expect("publication succeeds");
    assert_eq!(batch.updates().len(), 2);

    let accepted = batch.updates()[0];
    let encoded_accepted = accepted.encode().expect("update encodes");
    let mut expected = Vec::new();
    expected.extend_from_slice(&4_u64.to_le_bytes());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.extend_from_slice(&1_u64.to_le_bytes());
    expected.extend_from_slice(&9_u64.to_le_bytes());
    expected.push(0);
    assert_eq!(encoded_accepted, expected);
    assert_eq!(
        MarketDataUpdate::decode(&encoded_accepted).unwrap(),
        accepted
    );

    let level = batch.updates()[1];
    let encoded_level = level.encode().expect("level update encodes");
    assert_eq!(encoded_level.len(), 66);
    assert_eq!(MarketDataUpdate::decode(&encoded_level).unwrap(), level);
    let mut contradictory_level = encoded_level;
    contradictory_level[42..58].fill(0);
    assert!(matches!(
        MarketDataUpdate::decode(&contradictory_level),
        Err(CodecError::InvalidMarketData(
            MarketDataError::InvalidUpdate(_)
        ))
    ));

    let snapshot = publisher.snapshot();
    let encoded_snapshot = snapshot.encode().expect("snapshot encodes");
    assert_eq!(encoded_snapshot.len(), 66);
    assert_eq!(
        MarketDataSnapshot::decode(&encoded_snapshot).unwrap(),
        snapshot
    );
    let mut replica = MarketDataReplica::new(id(InstrumentId::new(4)), version());
    replica
        .apply_snapshot(&genesis)
        .expect("genesis snapshot applies");
    for update in batch.updates() {
        let decoded = MarketDataUpdate::decode(&update.encode().unwrap()).unwrap();
        replica.apply(decoded).expect("decoded update applies");
    }
    assert_eq!(replica.depth(Side::Buy, 10), book.depth(Side::Buy, 10));
    let mut invalid_trade_option = encoded_snapshot;
    invalid_trade_option[24] = 2;
    assert_eq!(
        MarketDataSnapshot::decode(&invalid_trade_option),
        Err(CodecError::InvalidTag {
            type_name: "Option<TradeId>",
            tag: 2,
        })
    );
}

#[test]
fn codec_rejects_truncation_invalid_tags_and_trailing_bytes() {
    let encoded = command(1, 2, Side::Sell).encode().expect("command encodes");
    assert!(matches!(
        Command::decode(&encoded[..encoded.len() - 1]),
        Err(CodecError::UnexpectedEnd { .. })
    ));

    let mut invalid_tag = encoded.clone();
    invalid_tag[0] = u8::MAX;
    assert!(matches!(
        Command::decode(&invalid_tag),
        Err(CodecError::InvalidTag {
            type_name: "Command",
            tag: u8::MAX
        })
    ));

    let mut trailing = encoded;
    trailing.push(0);
    assert_eq!(
        Command::decode(&trailing),
        Err(CodecError::TrailingBytes(1))
    );
}

#[test]
fn instrument_definition_codec_round_trips_and_revalidates_rules() {
    let value = definition();
    let encoded = value.encode().expect("definition encodes");
    assert_eq!(encoded.len(), 112);
    assert_eq!(
        InstrumentDefinition::decode(&encoded).expect("definition decodes"),
        value
    );

    let mut empty_symbol = encoded.clone();
    empty_symbol[24] = 0;
    assert_eq!(
        InstrumentDefinition::decode(&empty_symbol),
        Err(CodecError::InvalidInstrument(InstrumentError::EmptyText(
            "instrument symbol"
        )))
    );

    let mut invalid_kind = encoded.clone();
    invalid_kind[29] = u8::MAX;
    assert_eq!(
        InstrumentDefinition::decode(&invalid_kind),
        Err(CodecError::InvalidTag {
            type_name: "InstrumentKind",
            tag: u8::MAX,
        })
    );

    let mut zero_tick = encoded.clone();
    zero_tick[47..55].fill(0);
    assert_eq!(
        InstrumentDefinition::decode(&zero_tick),
        Err(CodecError::InvalidInstrument(
            InstrumentError::InvalidTickSize
        ))
    );

    let mut invalid_state = encoded;
    *invalid_state.last_mut().expect("definition is nonempty") = u8::MAX;
    assert_eq!(
        InstrumentDefinition::decode(&invalid_state),
        Err(CodecError::InvalidTag {
            type_name: "TradingState",
            tag: u8::MAX,
        })
    );
}

#[test]
fn account_risk_definition_codec_round_trips_and_revalidates_limits() {
    let value = AccountRiskDefinition::new(
        id(AccountId::new(7)),
        RiskProfile::new(
            AccountRiskState::ReduceOnly,
            -5,
            RiskLimits::new(RiskLimitSpec {
                max_order_quantity_lots: 10,
                max_order_notional: 20,
                max_open_orders: 3,
                max_open_quantity_lots: 30,
                max_open_notional: 40,
                max_long_position_lots: 50,
                max_short_position_lots: 60,
            })
            .unwrap(),
        )
        .unwrap(),
    );
    let encoded = value.encode().unwrap();
    assert_eq!(encoded.len(), 121);
    assert_eq!(AccountRiskDefinition::decode(&encoded).unwrap(), value);

    let mut invalid_state = encoded.clone();
    invalid_state[8] = u8::MAX;
    assert_eq!(
        AccountRiskDefinition::decode(&invalid_state),
        Err(CodecError::InvalidTag {
            type_name: "AccountRiskState",
            tag: u8::MAX,
        })
    );

    let mut zero_order_limit = encoded;
    zero_order_limit[25..33].fill(0);
    assert_eq!(
        AccountRiskDefinition::decode(&zero_order_limit),
        Err(CodecError::InvalidRisk(RiskError::InvalidLimits))
    );
}

#[test]
fn every_command_variant_round_trips() {
    let commands = [
        command(1, 2, Side::Sell),
        Command::Cancel(CancelOrder {
            command_id: id(CommandId::new(2)),
            order_id: id(OrderId::new(3)),
            account_id: id(AccountId::new(4)),
            instrument_id: id(InstrumentId::new(5)),
            instrument_version: version(),
            received_at: TimestampNs::from_unix_nanos(6),
        }),
        Command::Replace(ReplaceOrder {
            command_id: id(CommandId::new(3)),
            order_id: id(OrderId::new(4)),
            account_id: id(AccountId::new(5)),
            instrument_id: id(InstrumentId::new(6)),
            instrument_version: version(),
            new_quantity: Quantity::new(7).expect("positive quantity"),
            new_price: Price::from_raw(i64::MIN),
            received_at: TimestampNs::from_unix_nanos(u64::MAX),
        }),
    ];

    for value in commands {
        let encoded = value.encode().expect("command encodes");
        assert_eq!(Command::decode(&encoded).expect("command decodes"), value);
    }
}

#[test]
fn execution_reports_round_trip_without_losing_trace_information() {
    let instrument = id(InstrumentId::new(4));
    let mut book = OrderBook::new(definition());
    let resting = Command::New(NewOrder {
        command_id: id(CommandId::new(1)),
        order_id: id(OrderId::new(1)),
        account_id: id(AccountId::new(10)),
        instrument_id: instrument,
        instrument_version: version(),
        side: Side::Sell,
        quantity: Quantity::new(8).expect("positive quantity"),
        order_type: OrderType::Limit(Price::from_raw(101)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(1),
    });
    book.submit(resting).expect("resting order accepted");
    let report = book
        .submit(Command::New(NewOrder {
            command_id: id(CommandId::new(2)),
            order_id: id(OrderId::new(2)),
            account_id: id(AccountId::new(20)),
            instrument_id: instrument,
            instrument_version: version(),
            side: Side::Buy,
            quantity: Quantity::new(5).expect("positive quantity"),
            order_type: OrderType::Limit(Price::from_raw(101)),
            time_in_force: TimeInForce::ImmediateOrCancel,
            self_trade_prevention: SelfTradePrevention::CancelAggressor,
            received_at: TimestampNs::from_unix_nanos(2),
        }))
        .expect("aggressing order accepted");

    let encoded = report.encode().expect("report encodes");
    assert_eq!(
        quotick::matching::ExecutionReport::decode(&encoded).expect("valid report"),
        report
    );
}

#[test]
fn report_decoder_rejects_cross_field_outcome_contradictions() {
    let command_id = id(CommandId::new(1));
    let invalid = ExecutionReport {
        command_id,
        outcome: CommandOutcome::Accepted,
        events: vec![Event {
            sequence: 1,
            command_id,
            occurred_at: TimestampNs::from_unix_nanos(1),
            kind: EventKind::CommandRejected(RejectReason::UnknownOrder),
        }],
        replayed: false,
    };
    let bytes = invalid.encode().expect("structurally encodable report");

    assert_eq!(
        ExecutionReport::decode(&bytes),
        Err(CodecError::InvalidValue(
            "accepted execution report contains a rejection event"
        ))
    );
}

#[test]
fn every_rejection_reason_has_a_stable_round_trip() {
    let reasons = [
        RejectReason::WrongInstrument,
        RejectReason::DuplicateOrder,
        RejectReason::UnknownOrder,
        RejectReason::NotOrderOwner,
        RejectReason::MarketOrderCannotRest,
        RejectReason::MarketOrderCannotPost,
        RejectReason::UnsupportedFokSelfTradePolicy,
        RejectReason::InsufficientLiquidity,
        RejectReason::PostOnlyWouldCross,
        RejectReason::WrongInstrumentVersion,
        RejectReason::InstrumentNotOpen,
        RejectReason::PriceOffTickGrid,
        RejectReason::PriceOutsideCollar,
        RejectReason::QuantityOffGrid,
        RejectReason::QuantityOutsideLimits,
        RejectReason::RiskProfileMissing,
        RejectReason::RiskAccountBlocked,
        RejectReason::RiskReduceOnly,
        RejectReason::RiskOrderQuantityLimit,
        RejectReason::RiskOrderNotionalLimit,
        RejectReason::RiskOpenOrderCountLimit,
        RejectReason::RiskOpenQuantityLimit,
        RejectReason::RiskOpenNotionalLimit,
        RejectReason::RiskPositionLimit,
        RejectReason::RiskArithmeticOverflow,
    ];
    for (index, reason) in reasons.into_iter().enumerate() {
        let command_id = id(CommandId::new(
            u64::try_from(index + 1).expect("small test index"),
        ));
        let report = ExecutionReport {
            command_id,
            outcome: CommandOutcome::Rejected(reason),
            events: vec![Event {
                sequence: 1,
                command_id,
                occurred_at: TimestampNs::from_unix_nanos(1),
                kind: EventKind::CommandRejected(reason),
            }],
            replayed: false,
        };
        let bytes = report.encode().expect("rejection report encodes");
        assert_eq!(ExecutionReport::decode(&bytes).unwrap(), report);
    }
}

#[test]
fn every_event_variant_round_trips() {
    let command_id = id(CommandId::new(7));
    let order_id = id(OrderId::new(8));
    let other_order_id = id(OrderId::new(9));
    let account_id = id(AccountId::new(10));
    let other_account_id = id(AccountId::new(11));
    let instrument_id = id(InstrumentId::new(12));
    let quantity = Quantity::new(13).expect("positive quantity");
    let kinds = [
        EventKind::OrderAccepted { order_id, quantity },
        EventKind::OrderRested {
            order_id,
            price: Price::from_raw(-14),
            leaves_quantity: quantity,
        },
        EventKind::Trade(Trade {
            trade_id: id(TradeId::new(15)),
            instrument_id,
            instrument_version: version(),
            price: Price::from_raw(-14),
            quantity,
            buy_order_id: order_id,
            sell_order_id: other_order_id,
            buyer_account_id: account_id,
            seller_account_id: other_account_id,
            maker_order_id: other_order_id,
            taker_order_id: order_id,
        }),
        EventKind::OrderCancelled {
            order_id,
            quantity,
            reason: CancelReason::SelfTradeAggressor,
        },
        EventKind::OrderReplaced {
            order_id,
            old_price: Price::from_raw(-14),
            new_price: Price::from_raw(16),
            old_quantity: quantity,
            new_quantity: Quantity::new(17).expect("positive quantity"),
            priority_retained: false,
        },
        EventKind::SelfTradePrevented {
            aggressor_order_id: order_id,
            resting_order_id: other_order_id,
            quantity,
            policy: SelfTradePrevention::DecrementAndCancel,
        },
    ];
    let events = kinds
        .into_iter()
        .enumerate()
        .map(|(index, kind)| Event {
            sequence: u64::try_from(index + 1).expect("small sequence"),
            command_id,
            occurred_at: TimestampNs::from_unix_nanos(18),
            kind,
        })
        .collect();
    let report = ExecutionReport {
        command_id,
        outcome: CommandOutcome::Accepted,
        events,
        replayed: false,
    };
    let bytes = report.encode().expect("all event variants encode");
    assert_eq!(
        ExecutionReport::decode(&bytes).expect("report decodes"),
        report
    );

    let rejected = ExecutionReport {
        command_id,
        outcome: CommandOutcome::Rejected(RejectReason::PostOnlyWouldCross),
        events: vec![Event {
            sequence: 19,
            command_id,
            occurred_at: TimestampNs::from_unix_nanos(20),
            kind: EventKind::CommandRejected(RejectReason::PostOnlyWouldCross),
        }],
        replayed: false,
    };
    let bytes = rejected.encode().expect("rejection encodes");
    assert_eq!(
        ExecutionReport::decode(&bytes).expect("rejection decodes"),
        rejected
    );
}

#[test]
fn journal_entry_codec_preserves_canonical_posting_order() {
    let entry = JournalEntry::new(
        id(TransactionId::new(9)),
        77,
        vec![
            Posting {
                account_id: id(AccountId::new(2)),
                asset_id: id(AssetId::new(8)),
                amount: -50,
            },
            Posting {
                account_id: id(AccountId::new(1)),
                asset_id: id(AssetId::new(8)),
                amount: 50,
            },
        ],
    )
    .expect("balanced entry");

    let encoded = entry.encode().expect("entry encodes");
    let decoded = JournalEntry::decode(&encoded).expect("valid journal entry");
    assert_eq!(decoded, entry);
    assert_eq!(decoded.encode().expect("decoded entry re-encodes"), encoded);
}

#[test]
fn journal_entry_decoder_rejects_noncanonical_wire_order() {
    let entry = JournalEntry::new(
        id(TransactionId::new(9)),
        77,
        vec![
            Posting {
                account_id: id(AccountId::new(1)),
                asset_id: id(AssetId::new(8)),
                amount: 50,
            },
            Posting {
                account_id: id(AccountId::new(2)),
                asset_id: id(AssetId::new(8)),
                amount: -50,
            },
        ],
    )
    .expect("balanced entry");
    let mut bytes = entry.encode().expect("entry encodes");
    let first = bytes[20..52].to_vec();
    let second = bytes[52..84].to_vec();
    bytes[20..52].copy_from_slice(&second);
    bytes[52..84].copy_from_slice(&first);

    assert_eq!(
        JournalEntry::decode(&bytes),
        Err(CodecError::InvalidValue(
            "journal entry postings are not in canonical order"
        ))
    );
}
