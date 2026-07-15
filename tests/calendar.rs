use quotick::calendar::{
    CalendarTimeInForce, OrderExpiryBoundary, TradingCalendar, TradingCalendarError, TradingSession,
};
use quotick::codec::{BinaryCodec, CodecError};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::matching::{
    Command, ExpirySweep, NewOrder, OrderBook, OrderDisplay, OrderType, SelfTradePrevention,
    TimeInForce,
};
use quotick::{
    AccountId, AccountingDate, AssetId, CalendarId, CalendarVersion, CommandId, InstrumentId,
    InstrumentVersion, OrderId, Price, Quantity, Side, TimestampNs, TradingSessionId,
};

fn ts(value: u64) -> TimestampNs {
    TimestampNs::from_unix_nanos(value)
}

fn session(
    id: u64,
    date: i32,
    opens: u64,
    closes: u64,
    session_expiry: u64,
    day_expiry: u64,
) -> TradingSession {
    TradingSession::new(
        TradingSessionId::new(id).unwrap(),
        AccountingDate::from_days_since_unix_epoch(date),
        ts(opens),
        ts(closes),
        ts(session_expiry),
        ts(day_expiry),
    )
    .unwrap()
}

fn schedule() -> TradingCalendar {
    TradingCalendar::try_new(
        CalendarId::new(7).unwrap(),
        CalendarVersion::new(3).unwrap(),
        ts(5),
        vec![
            session(1, 20_000, 10, 20, 25, 50),
            session(2, 20_000, 30, 40, 45, 50),
            session(3, 20_001, 60, 70, 75, 80),
        ],
    )
    .unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(1).unwrap(),
        version: InstrumentVersion::new(1).unwrap(),
        effective_from: ts(0),
        symbol: InstrumentSymbol::new("SESSION").unwrap(),
        kind: InstrumentKind::Future,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 1, Price::from_raw(1), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: true,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn order(command_id: u64, order_id: u64, time_in_force: TimeInForce) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(order_id).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        side: Side::Buy,
        quantity: Quantity::new(1).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(100)),
        time_in_force,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: ts(15),
    })
}

#[test]
fn identifiers_and_individual_session_windows_are_validated() {
    assert!(CalendarId::new(0).is_err());
    assert!(CalendarVersion::new(0).is_err());
    assert!(TradingSessionId::new(0).is_err());

    let id = TradingSessionId::new(1).unwrap();
    let date = AccountingDate::UNIX_EPOCH;
    assert_eq!(
        TradingSession::new(id, date, ts(10), ts(10), ts(11), ts(12)),
        Err(TradingCalendarError::InvalidEntryWindow { session_id: id })
    );
    assert_eq!(
        TradingSession::new(id, date, ts(10), ts(20), ts(19), ts(21)),
        Err(TradingCalendarError::SessionExpiryBeforeEntryClose { session_id: id })
    );
    assert_eq!(
        TradingSession::new(id, date, ts(10), ts(20), ts(21), ts(20)),
        Err(TradingCalendarError::DayExpiryBeforeSessionExpiry { session_id: id })
    );
}

#[test]
fn calendar_rejects_noncanonical_or_ambiguous_schedules() {
    let id = CalendarId::new(1).unwrap();
    let version = CalendarVersion::new(1).unwrap();
    assert_eq!(
        TradingCalendar::try_new(id, version, ts(0), vec![]),
        Err(TradingCalendarError::EmptySchedule)
    );
    assert!(matches!(
        TradingCalendar::try_new(id, version, ts(11), vec![session(1, 0, 10, 20, 20, 20)]),
        Err(TradingCalendarError::EffectiveAfterFirstEntry { .. })
    ));
    assert!(matches!(
        TradingCalendar::try_new(
            id,
            version,
            ts(0),
            vec![session(1, 0, 10, 20, 25, 25), session(2, 0, 24, 30, 30, 30)]
        ),
        Err(TradingCalendarError::SessionOverlap { .. })
    ));
    assert!(matches!(
        TradingCalendar::try_new(
            id,
            version,
            ts(0),
            vec![session(1, 0, 10, 20, 20, 30), session(2, 0, 25, 27, 27, 31)]
        ),
        Err(TradingCalendarError::InconsistentDayExpiry { .. })
    ));
    assert!(matches!(
        TradingCalendar::try_new(
            id,
            version,
            ts(0),
            vec![session(1, 1, 10, 20, 20, 20), session(2, 0, 25, 30, 30, 30)]
        ),
        Err(TradingCalendarError::TradingDateRegression { .. })
    ));
    assert!(matches!(
        TradingCalendar::try_new(
            id,
            version,
            ts(0),
            vec![session(1, 0, 10, 20, 20, 20), session(1, 1, 25, 30, 30, 30)]
        ),
        Err(TradingCalendarError::DuplicateSessionId { .. })
    ));
    assert!(matches!(
        TradingCalendar::try_new(
            id,
            version,
            ts(0),
            vec![session(1, 0, 10, 20, 20, 30), session(2, 1, 25, 27, 27, 31)]
        ),
        Err(TradingCalendarError::TradingDayOverlap { .. })
    ));
}

#[test]
fn active_next_id_and_trading_date_queries_obey_exact_boundaries() {
    let calendar = schedule();
    assert_eq!(calendar.calendar_id(), CalendarId::new(7).unwrap());
    assert_eq!(calendar.version(), CalendarVersion::new(3).unwrap());
    assert_eq!(calendar.effective_from(), ts(5));
    assert_eq!(calendar.sessions().len(), 3);
    assert_eq!(calendar.active_session(ts(9)), None);
    assert_eq!(
        calendar.active_session(ts(10)).unwrap().session_id(),
        TradingSessionId::new(1).unwrap()
    );
    assert_eq!(
        calendar.active_session(ts(19)).unwrap().session_id(),
        TradingSessionId::new(1).unwrap()
    );
    assert_eq!(calendar.active_session(ts(20)), None);
    assert_eq!(
        calendar.next_session_after(ts(20)).unwrap().session_id(),
        TradingSessionId::new(2).unwrap()
    );
    assert_eq!(
        calendar
            .session(TradingSessionId::new(3).unwrap())
            .unwrap()
            .trading_date(),
        AccountingDate::from_days_since_unix_epoch(20_001)
    );
    assert_eq!(
        calendar
            .sessions_on(AccountingDate::from_days_since_unix_epoch(20_000))
            .len(),
        2
    );
    assert!(
        calendar
            .sessions_on(AccountingDate::from_days_since_unix_epoch(19_999))
            .is_empty()
    );
}

#[test]
fn day_and_session_lifetimes_normalize_to_authoritative_gtd_boundaries() {
    let calendar = schedule();
    let session = calendar
        .resolve_time_in_force(CalendarTimeInForce::GoodForSession, ts(15))
        .unwrap();
    assert_eq!(
        session.session_id(),
        Some(TradingSessionId::new(1).unwrap())
    );
    assert_eq!(
        session.trading_date(),
        Some(AccountingDate::from_days_since_unix_epoch(20_000))
    );
    assert_eq!(
        session.normalized(),
        TimeInForce::GoodTilTimestamp { expires_at: ts(25) }
    );

    let day = calendar
        .resolve_time_in_force(CalendarTimeInForce::Day, ts(15))
        .unwrap();
    assert_eq!(day.calendar_id(), CalendarId::new(7).unwrap());
    assert_eq!(day.calendar_version(), CalendarVersion::new(3).unwrap());
    assert_eq!(
        day.normalized(),
        TimeInForce::GoodTilTimestamp { expires_at: ts(50) }
    );

    let native = calendar
        .resolve_time_in_force(
            CalendarTimeInForce::Native(TimeInForce::GoodTilCancelled),
            ts(55),
        )
        .unwrap();
    assert_eq!(native.normalized(), TimeInForce::GoodTilCancelled);
    assert_eq!(native.session_id(), None);

    assert!(matches!(
        calendar.resolve_time_in_force(CalendarTimeInForce::Day, ts(20)),
        Err(TradingCalendarError::NoActiveSession { at }) if at == ts(20)
    ));

    let second_session_day = calendar
        .resolve_time_in_force(CalendarTimeInForce::Day, ts(30))
        .unwrap();
    assert_eq!(
        second_session_day.normalized(),
        TimeInForce::GoodTilTimestamp { expires_at: ts(50) }
    );
    assert_eq!(
        second_session_day.session_id(),
        Some(TradingSessionId::new(2).unwrap())
    );
}

#[test]
fn normalized_lifetimes_reuse_core_expiry_ordering_and_replay_semantics() {
    let calendar = schedule();
    let session_tif = calendar
        .resolve_time_in_force(CalendarTimeInForce::GoodForSession, ts(15))
        .unwrap()
        .normalized();
    let day_tif = calendar
        .resolve_time_in_force(CalendarTimeInForce::Day, ts(15))
        .unwrap()
        .normalized();
    let mut book = OrderBook::new(definition());
    book.submit(order(1, 1, session_tif)).unwrap();
    book.submit(order(2, 2, day_tif)).unwrap();

    let first = book
        .submit(Command::ExpirySweep(ExpirySweep {
            command_id: CommandId::new(3).unwrap(),
            instrument_id: InstrumentId::new(1).unwrap(),
            instrument_version: InstrumentVersion::new(1).unwrap(),
            through: ts(25),
            received_at: ts(25),
        }))
        .unwrap();
    assert_eq!(first.outcome, quotick::matching::CommandOutcome::Accepted);
    assert!(book.order(OrderId::new(1).unwrap()).is_none());
    assert!(book.order(OrderId::new(2).unwrap()).is_some());

    book.submit(Command::ExpirySweep(ExpirySweep {
        command_id: CommandId::new(4).unwrap(),
        instrument_id: InstrumentId::new(1).unwrap(),
        instrument_version: InstrumentVersion::new(1).unwrap(),
        through: ts(50),
        received_at: ts(50),
    }))
    .unwrap();
    assert_eq!(book.active_order_count(), 0);
    assert!(book.submit(order(1, 1, session_tif)).unwrap().replayed);
    book.validate().unwrap();
}

#[test]
fn expiry_sweep_factory_is_boundary_checked_and_identity_bound() {
    let calendar = schedule();
    let session_id = TradingSessionId::new(1).unwrap();
    let sweep = calendar
        .expiry_sweep(
            session_id,
            OrderExpiryBoundary::Session,
            CommandId::new(9).unwrap(),
            InstrumentId::new(10).unwrap(),
            InstrumentVersion::new(4).unwrap(),
            ts(25),
        )
        .unwrap();
    assert_eq!(sweep.through, ts(25));
    assert_eq!(sweep.received_at, ts(25));
    assert_eq!(sweep.instrument_id, InstrumentId::new(10).unwrap());
    assert!(matches!(
        calendar.expiry_sweep(
            session_id,
            OrderExpiryBoundary::TradingDay,
            CommandId::new(10).unwrap(),
            InstrumentId::new(10).unwrap(),
            InstrumentVersion::new(4).unwrap(),
            ts(49),
        ),
        Err(TradingCalendarError::ControlBeforeBoundary { boundary, received_at })
            if boundary == ts(50) && received_at == ts(49)
    ));
    assert!(matches!(
        calendar.expiry_sweep(
            TradingSessionId::new(99).unwrap(),
            OrderExpiryBoundary::Session,
            CommandId::new(11).unwrap(),
            InstrumentId::new(10).unwrap(),
            InstrumentVersion::new(4).unwrap(),
            ts(100),
        ),
        Err(TradingCalendarError::UnknownSession { .. })
    ));
}

#[test]
fn calendar_codec_has_stable_bytes_and_revalidates_schedule() {
    let value = TradingCalendar::try_new(
        CalendarId::new(7).unwrap(),
        CalendarVersion::new(3).unwrap(),
        ts(5),
        vec![session(9, -2, 10, 20, 25, 30)],
    )
    .unwrap();
    let encoded = value.encode().unwrap();
    let mut expected = Vec::new();
    expected.extend_from_slice(&7_u64.to_le_bytes());
    expected.extend_from_slice(&3_u64.to_le_bytes());
    expected.extend_from_slice(&5_u64.to_le_bytes());
    expected.extend_from_slice(&1_u32.to_le_bytes());
    expected.extend_from_slice(&9_u64.to_le_bytes());
    expected.extend_from_slice(&(-2_i32).to_le_bytes());
    expected.extend_from_slice(&10_u64.to_le_bytes());
    expected.extend_from_slice(&20_u64.to_le_bytes());
    expected.extend_from_slice(&25_u64.to_le_bytes());
    expected.extend_from_slice(&30_u64.to_le_bytes());
    assert_eq!(encoded, expected);
    assert_eq!(TradingCalendar::decode(&encoded).unwrap(), value);

    let mut duplicate = schedule().encode().unwrap();
    let second_session_id_offset = 28 + 44;
    duplicate[second_session_id_offset..second_session_id_offset + 8]
        .copy_from_slice(&1_u64.to_le_bytes());
    assert_eq!(
        TradingCalendar::decode(&duplicate),
        Err(CodecError::InvalidTradingCalendar(
            TradingCalendarError::DuplicateSessionId {
                session_id: TradingSessionId::new(1).unwrap()
            }
        ))
    );
}

#[test]
fn calendar_codec_rejects_zero_ids_impossible_counts_truncation_and_trailing_bytes() {
    let encoded = schedule().encode().unwrap();

    let mut zero_calendar_id = encoded.clone();
    zero_calendar_id[..8].copy_from_slice(&0_u64.to_le_bytes());
    assert!(matches!(
        TradingCalendar::decode(&zero_calendar_id),
        Err(CodecError::InvalidDomain(_))
    ));

    let mut impossible_count = encoded.clone();
    impossible_count[24..28].copy_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        TradingCalendar::decode(&impossible_count),
        Err(CodecError::InvalidLength {
            field: "trading calendar sessions",
            ..
        })
    ));

    assert!(matches!(
        TradingCalendar::decode(&encoded[..encoded.len() - 1]),
        Err(CodecError::InvalidLength {
            field: "trading calendar sessions",
            ..
        })
    ));

    let mut trailing = encoded;
    trailing.push(0);
    assert_eq!(
        TradingCalendar::decode(&trailing),
        Err(CodecError::TrailingBytes(1))
    );

    let mut empty = Vec::new();
    empty.extend_from_slice(&1_u64.to_le_bytes());
    empty.extend_from_slice(&1_u64.to_le_bytes());
    empty.extend_from_slice(&0_u64.to_le_bytes());
    empty.extend_from_slice(&0_u32.to_le_bytes());
    assert_eq!(
        TradingCalendar::decode(&empty),
        Err(CodecError::InvalidTradingCalendar(
            TradingCalendarError::EmptySchedule
        ))
    );
}

#[test]
fn calendar_clones_share_the_immutable_schedule_image() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<TradingCalendar>();
    let calendar = schedule();
    let clone = calendar.clone();
    assert!(calendar.shares_storage_with(&clone));
    assert_eq!(calendar, clone);
}
