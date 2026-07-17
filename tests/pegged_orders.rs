use std::cell::Cell;
use std::fs;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::codec::BinaryCodec;
use quotick::durable::DurableOrderBook;
use quotick::durable_risk::DurableRiskOrderBook;
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions, JournalReader};
use quotick::matching::{
    Command, CommandOutcome, ConditionalCommandOutcome, NewOrder, OrderBook, OrderDisplay,
    OrderExecution, OrderType, PegCrossingPolicy, PegMidpointRounding, PegReference,
    PeggedOrderSubmissionError, PeggedPriceError, PeggedPriceRequest, SelfTradePrevention,
    TimeInForce,
};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedOrderBook,
    RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

static NEXT_PATH: AtomicU64 = AtomicU64::new(1);

struct TestFile(PathBuf);

impl TestFile {
    fn new(label: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-pegged-order-{label}-{}-{nonce}.wal",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn instrument() -> InstrumentId {
    InstrumentId::new(1).unwrap()
}

fn version() -> InstrumentVersion {
    InstrumentVersion::new(1).unwrap()
}

fn definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("PEGGED").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 10, Price::from_raw(-1_000), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 100).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: true,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn foreign_definition() -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: InstrumentId::new(2).unwrap(),
        version: version(),
        effective_from: TimestampNs::from_unix_nanos(0),
        symbol: InstrumentSymbol::new("PEGGED-FOREIGN").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: AssetId::new(1).unwrap(),
        quote_asset_id: AssetId::new(2).unwrap(),
        price: PriceRules::new(0, 10, Price::from_raw(-1_000), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 100).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        hidden_orders_supported: true,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: TradingState::Open,
    })
    .unwrap()
}

fn order(
    command_id: u64,
    order_id: u64,
    owner: u64,
    side: Side,
    quantity: u64,
    price: i64,
) -> NewOrder {
    NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: AccountId::new(owner).unwrap(),
        instrument_id: instrument(),
        instrument_version: version(),
        side,
        quantity: Quantity::new(quantity).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Limit(Price::from_raw(price)),
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command_id),
    }
}

fn request(
    side: Side,
    reference: PegReference,
    offset_ticks: i64,
    limit_price: Option<i64>,
    crossing_policy: PegCrossingPolicy,
) -> PeggedPriceRequest {
    PeggedPriceRequest {
        side,
        reference,
        offset_ticks,
        limit_price: limit_price.map(Price::from_raw),
        crossing_policy,
    }
}

fn quoted_book(bid: i64, offer: i64) -> OrderBook {
    let mut book = OrderBook::new(definition());
    book.submit(Command::New(order(1, 1, 11, Side::Buy, 5, bid)))
        .unwrap();
    book.submit(Command::New(order(2, 2, 12, Side::Sell, 7, offer)))
        .unwrap();
    book
}

fn profile(max_order_quantity_lots: u64) -> RiskProfile {
    RiskProfile::new(
        AccountRiskState::Active,
        0,
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots,
            max_order_notional: 100_000,
            max_open_orders: 10,
            max_open_quantity_lots: 100,
            max_open_notional: 100_000,
            max_long_position_lots: 100,
            max_short_position_lots: 100,
        })
        .unwrap(),
    )
    .unwrap()
}

fn profiles() -> [AccountRiskDefinition; 3] {
    [
        AccountRiskDefinition::new(AccountId::new(11).unwrap(), profile(100)),
        AccountRiskDefinition::new(AccountId::new(12).unwrap(), profile(100)),
        AccountRiskDefinition::new(AccountId::new(13).unwrap(), profile(100)),
    ]
}

fn options() -> JournalOptions {
    JournalOptions {
        durability: Durability::Buffered,
        ..JournalOptions::default()
    }
}

fn frame_count(path: &Path) -> usize {
    JournalReader::open(path, options())
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .len()
}

#[test]
fn displayed_primary_market_and_midpoint_references_are_exact() {
    let book = quoted_book(100, 110);

    let primary_buy = book
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            0,
            None,
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    assert_eq!(primary_buy.reference_price(), Price::from_raw(100));
    assert_eq!(primary_buy.price(), Price::from_raw(100));
    assert_eq!(
        primary_buy.order_type(),
        OrderType::Limit(Price::from_raw(100))
    );
    assert_eq!(
        primary_buy.best_bid_offer(),
        book.try_best_bid_offer().unwrap()
    );

    let primary_sell = book
        .try_resolve_pegged_price(request(
            Side::Sell,
            PegReference::Primary,
            0,
            None,
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    let market_buy = book
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Market,
            0,
            None,
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    let market_sell = book
        .try_resolve_pegged_price(request(
            Side::Sell,
            PegReference::Market,
            0,
            None,
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    assert_eq!(primary_sell.price(), Price::from_raw(110));
    assert_eq!(market_buy.price(), Price::from_raw(110));
    assert_eq!(market_sell.price(), Price::from_raw(100));

    let midpoint_down = book
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Midpoint(PegMidpointRounding::Down),
            0,
            None,
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    let midpoint_up = book
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Midpoint(PegMidpointRounding::Up),
            0,
            None,
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    let midpoint_even = book
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Midpoint(PegMidpointRounding::NearestTiesToEven),
            0,
            None,
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    assert_eq!(midpoint_down.price(), Price::from_raw(100));
    assert_eq!(midpoint_up.price(), Price::from_raw(110));
    assert_eq!(midpoint_even.price(), Price::from_raw(100));

    let negative = quoted_book(-110, -100);
    let negative_even = negative
        .try_resolve_pegged_price(request(
            Side::Sell,
            PegReference::Midpoint(PegMidpointRounding::NearestTiesToEven),
            0,
            None,
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    assert_eq!(negative_even.price(), Price::from_raw(-100));
}

#[test]
fn midpoint_ties_to_even_handles_exact_and_odd_lower_tick_cases() {
    let exact_midpoint = quoted_book(100, 120)
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Midpoint(PegMidpointRounding::NearestTiesToEven),
            0,
            None,
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    assert_eq!(exact_midpoint.price(), Price::from_raw(110));
    let odd_lower_tie = quoted_book(110, 120)
        .try_resolve_pegged_price(request(
            Side::Sell,
            PegReference::Midpoint(PegMidpointRounding::NearestTiesToEven),
            0,
            None,
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    assert_eq!(odd_lower_tie.price(), Price::from_raw(120));
}

#[test]
fn offsets_limits_and_crossing_policies_compose_on_the_tick_grid() {
    let book = quoted_book(100, 130);
    let capped = book
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            5,
            Some(120),
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    assert_eq!(capped.reference_price(), Price::from_raw(100));
    assert_eq!(capped.bounded_price(), Price::from_raw(120));
    assert_eq!(capped.price(), Price::from_raw(120));
    assert!(!capped.was_passively_slid());

    let crossing = request(
        Side::Buy,
        PegReference::Market,
        0,
        None,
        PegCrossingPolicy::Reject,
    );
    assert!(matches!(
        book.try_resolve_pegged_price(crossing),
        Err(PeggedPriceError::WouldCross {
            side: Side::Buy,
            price,
            opposite_best
        }) if price == Price::from_raw(130) && opposite_best == Price::from_raw(130)
    ));

    let slid = book
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Market,
            4,
            None,
            PegCrossingPolicy::SlidePassive,
        ))
        .unwrap();
    assert_eq!(slid.bounded_price(), Price::from_raw(170));
    assert_eq!(slid.price(), Price::from_raw(120));
    assert!(slid.was_passively_slid());

    let sell_floor = book
        .try_resolve_pegged_price(request(
            Side::Sell,
            PegReference::Primary,
            -5,
            Some(110),
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    assert_eq!(sell_floor.price(), Price::from_raw(110));
}

#[test]
fn unavailable_hidden_and_invalid_price_edges_are_typed() {
    let empty = OrderBook::new(definition());
    assert!(matches!(
        empty.try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            0,
            None,
            PegCrossingPolicy::Allow,
        )),
        Err(PeggedPriceError::ReferenceUnavailable {
            side: Side::Buy,
            reference: PegReference::Primary
        })
    ));

    let mut hidden = OrderBook::new(definition());
    let mut hidden_bid = order(1, 1, 11, Side::Buy, 5, 100);
    hidden_bid.display = OrderDisplay::Hidden;
    hidden.submit(Command::New(hidden_bid)).unwrap();
    assert!(hidden.try_best_bid().unwrap().is_none());
    assert!(matches!(
        hidden.try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            0,
            None,
            PegCrossingPolicy::Allow,
        )),
        Err(PeggedPriceError::ReferenceUnavailable { .. })
    ));

    let book = quoted_book(100, 130);
    assert!(matches!(
        book.try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            0,
            Some(115),
            PegCrossingPolicy::Allow,
        )),
        Err(PeggedPriceError::LimitPriceOffTickGrid { price })
            if price == Price::from_raw(115)
    ));
    assert!(matches!(
        book.try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            i64::MAX,
            None,
            PegCrossingPolicy::Allow,
        )),
        Err(PeggedPriceError::PriceArithmeticOverflow)
    ));
    let overflow_capped = book
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            i64::MAX,
            Some(120),
            PegCrossingPolicy::Allow,
        ))
        .unwrap();
    assert_eq!(overflow_capped.price(), Price::from_raw(120));
    assert!(matches!(
        book.try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            0,
            Some(1_010),
            PegCrossingPolicy::Allow,
        )),
        Err(PeggedPriceError::LimitPriceOutsideCollar { price })
            if price == Price::from_raw(1_010)
    ));

    let mut boundary = OrderBook::new(definition());
    boundary
        .submit(Command::New(order(1, 1, 11, Side::Buy, 5, 1_000)))
        .unwrap();
    assert!(matches!(
        boundary.try_resolve_pegged_price(request(
            Side::Sell,
            PegReference::Market,
            0,
            None,
            PegCrossingPolicy::SlidePassive,
        )),
        Err(PeggedPriceError::PassivePriceUnavailable {
            side: Side::Sell,
            opposite_best
        }) if opposite_best == Price::from_raw(1_000)
    ));
}

#[test]
fn atomic_submission_rejects_stale_or_mismatched_resolutions_and_replays_exactly() {
    let mut book = quoted_book(100, 130);
    let request = request(
        Side::Buy,
        PegReference::Primary,
        1,
        None,
        PegCrossingPolicy::Reject,
    );
    let stale = book.try_resolve_pegged_price(request).unwrap();
    book.submit(Command::New(order(3, 3, 14, Side::Buy, 1, 90)))
        .unwrap();
    let incoming = order(4, 4, 13, Side::Buy, 2, 110);
    let called = Cell::new(false);
    assert!(matches!(
        book.submit_pegged_new_order_if(stale, incoming, |_| {
            called.set(true);
            true
        }),
        Err(PeggedOrderSubmissionError::PeggedPrice(
            PeggedPriceError::StaleResolution { .. }
        ))
    ));
    assert!(!called.get());
    assert!(book.order(incoming.order_id).is_none());

    let current = book.try_resolve_pegged_price(request).unwrap();
    let mut wrong = incoming;
    wrong.order_type = OrderType::Limit(Price::from_raw(120));
    assert!(matches!(
        book.submit_pegged_new_order_if(current, wrong, |_| true),
        Err(PeggedOrderSubmissionError::PeggedPrice(
            PeggedPriceError::OrderPriceMismatch { .. }
        ))
    ));

    let declined = book
        .submit_pegged_new_order_if(current, incoming, |_| false)
        .unwrap();
    assert!(matches!(declined, ConditionalCommandOutcome::Declined(_)));
    assert!(book.order(incoming.order_id).is_none());

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _ =
            book.submit_pegged_new_order_if(current, incoming, |_| panic!("caller policy failed"));
    }));
    assert!(panicked.is_err());
    assert!(book.order(incoming.order_id).is_none());

    let accepted = book
        .submit_pegged_new_order_if(current, incoming, |observation| {
            assert_eq!(observation.resolution(), current);
            let OrderExecution::Active(quote) = observation.execution() else {
                panic!("resolved limit order is active");
            };
            assert_eq!(quote.book_event_sequence(), current.book_event_sequence());
            true
        })
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalCommandOutcome::Reported {
            observation: Some(_),
            report
        } if report.outcome == CommandOutcome::Accepted && !report.replayed
    ));

    book.submit(Command::New(order(5, 5, 15, Side::Sell, 1, 140)))
        .unwrap();
    let replay = book
        .submit_pegged_new_order_if(current, incoming, |_| panic!("replay bypasses predicate"))
        .unwrap();
    assert!(matches!(
        replay,
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if report.replayed
    ));
    book.validate().unwrap();
}

#[test]
fn core_and_risk_rejections_bypass_pegged_validation_and_predicate() {
    let mut book = quoted_book(100, 130);
    let resolution = book
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            1,
            None,
            PegCrossingPolicy::Reject,
        ))
        .unwrap();
    let mut core_rejected = order(3, 3, 13, Side::Buy, 2, 110);
    core_rejected.order_type = OrderType::Market;
    let outcome = book
        .submit_pegged_new_order_if(resolution, core_rejected, |_| {
            panic!("core rejection bypasses pegged validation and predicate")
        })
        .unwrap();
    assert!(matches!(
        outcome,
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if matches!(report.outcome, CommandOutcome::Rejected(_))
    ));

    let mut managed = RiskManagedOrderBook::new(definition());
    for (account_id, maximum) in [(11, 100), (12, 100), (13, 1)] {
        managed
            .register_account(AccountId::new(account_id).unwrap(), profile(maximum))
            .unwrap();
    }
    managed
        .submit(Command::New(order(1, 1, 11, Side::Buy, 5, 100)))
        .unwrap();
    managed
        .submit(Command::New(order(2, 2, 12, Side::Sell, 5, 130)))
        .unwrap();
    let resolution = managed
        .book()
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            1,
            None,
            PegCrossingPolicy::Reject,
        ))
        .unwrap();
    let outcome = managed
        .submit_pegged_new_order_if(resolution, order(3, 3, 13, Side::Buy, 2, 110), |_| {
            panic!("risk rejection bypasses pegged validation and predicate")
        })
        .unwrap();
    assert!(matches!(
        outcome,
        ConditionalCommandOutcome::Reported {
            observation: None,
            report
        } if matches!(report.outcome, CommandOutcome::Rejected(_))
    ));
}

#[test]
fn foreign_resolution_and_side_mismatch_fail_before_the_predicate() {
    let mut foreign = OrderBook::new(foreign_definition());
    let mut foreign_bid = order(1, 1, 11, Side::Buy, 5, 100);
    foreign_bid.instrument_id = InstrumentId::new(2).unwrap();
    let mut foreign_offer = order(2, 2, 12, Side::Sell, 5, 130);
    foreign_offer.instrument_id = InstrumentId::new(2).unwrap();
    foreign.submit(Command::New(foreign_bid)).unwrap();
    foreign.submit(Command::New(foreign_offer)).unwrap();
    let foreign_resolution = foreign
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            1,
            None,
            PegCrossingPolicy::Reject,
        ))
        .unwrap();

    let mut book = quoted_book(100, 130);
    let called = Cell::new(false);
    assert!(matches!(
        book.submit_pegged_new_order_if(
            foreign_resolution,
            order(3, 3, 13, Side::Buy, 2, 110),
            |_| {
                called.set(true);
                true
            }
        ),
        Err(PeggedOrderSubmissionError::PeggedPrice(
            PeggedPriceError::ForeignResolution { .. }
        ))
    ));
    assert!(!called.get());

    let buy_resolution = book
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            1,
            None,
            PegCrossingPolicy::Reject,
        ))
        .unwrap();
    assert!(matches!(
        book.submit_pegged_new_order_if(
            buy_resolution,
            order(4, 4, 13, Side::Sell, 2, 110),
            |_| true
        ),
        Err(PeggedOrderSubmissionError::PeggedPrice(
            PeggedPriceError::OrderSideMismatch { .. }
        ))
    ));
}

#[test]
fn resolved_command_is_an_ordinary_limit_on_risk_and_wire_surfaces() {
    let mut managed = RiskManagedOrderBook::new(definition());
    for definition in profiles() {
        managed
            .register_account(definition.account_id(), definition.profile())
            .unwrap();
    }
    managed
        .submit(Command::New(order(1, 1, 11, Side::Buy, 5, 100)))
        .unwrap();
    managed
        .submit(Command::New(order(2, 2, 12, Side::Sell, 5, 130)))
        .unwrap();
    let resolution = managed
        .book()
        .try_resolve_pegged_price(request(
            Side::Buy,
            PegReference::Primary,
            1,
            None,
            PegCrossingPolicy::Reject,
        ))
        .unwrap();
    let incoming = order(3, 3, 13, Side::Buy, 2, 110);

    let literal = Command::New(incoming).encode().unwrap();
    let mut derived = incoming;
    derived.order_type = resolution.order_type();
    assert_eq!(Command::New(derived).encode().unwrap(), literal);

    let accepted = managed
        .submit_pegged_new_order_if(resolution, incoming, |_| true)
        .unwrap();
    assert!(matches!(
        accepted,
        ConditionalCommandOutcome::Reported { report, .. }
            if report.outcome == CommandOutcome::Accepted
    ));
    assert_eq!(
        managed.book().order(incoming.order_id).unwrap().price.raw(),
        110
    );
    assert_eq!(
        managed
            .risk()
            .snapshot(incoming.account_id)
            .unwrap()
            .open_orders(),
        1
    );
}

#[test]
fn durable_and_durable_risk_paths_persist_only_the_resolved_limit_command() {
    let file = TestFile::new("durable");
    let incoming = order(3, 3, 13, Side::Buy, 2, 110);
    let resolution;
    {
        let mut durable = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
        durable
            .submit(Command::New(order(1, 1, 11, Side::Buy, 5, 100)))
            .unwrap();
        durable
            .submit(Command::New(order(2, 2, 12, Side::Sell, 5, 130)))
            .unwrap();
        resolution = durable
            .book()
            .try_resolve_pegged_price(request(
                Side::Buy,
                PegReference::Primary,
                1,
                None,
                PegCrossingPolicy::Reject,
            ))
            .unwrap();
        durable
            .submit_pegged_new_order_if(resolution, incoming, |_| true)
            .unwrap();
        assert_eq!(frame_count(file.path()), 7);
    }
    {
        let mut reopened = DurableOrderBook::open(file.path(), definition(), options()).unwrap();
        assert_eq!(
            reopened
                .book()
                .order(incoming.order_id)
                .unwrap()
                .price
                .raw(),
            110
        );
        let replay = reopened
            .submit_pegged_new_order_if(resolution, incoming, |_| {
                panic!("durable replay bypasses resolution validation and predicate")
            })
            .unwrap();
        assert!(matches!(
            replay,
            ConditionalCommandOutcome::Reported {
                observation: None,
                report
            } if report.replayed
        ));
        assert_eq!(frame_count(file.path()), 7);
    }

    let risk_file = TestFile::new("durable-risk");
    {
        let profiles = profiles();
        let mut durable =
            DurableRiskOrderBook::open(risk_file.path(), definition(), &profiles, options())
                .unwrap();
        durable
            .submit(Command::New(order(1, 1, 11, Side::Buy, 5, 100)))
            .unwrap();
        durable
            .submit(Command::New(order(2, 2, 12, Side::Sell, 5, 130)))
            .unwrap();
        let resolution = durable
            .managed()
            .book()
            .try_resolve_pegged_price(request(
                Side::Buy,
                PegReference::Primary,
                1,
                None,
                PegCrossingPolicy::Reject,
            ))
            .unwrap();
        let outcome = durable
            .submit_pegged_new_order_if(resolution, incoming, |_| true)
            .unwrap();
        assert!(outcome.report().is_some_and(|report| !report.replayed));
        assert_eq!(
            durable
                .managed()
                .book()
                .order(incoming.order_id)
                .unwrap()
                .price
                .raw(),
            110
        );
    }
}
