use quotick::domain::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs, TradeId,
};
use quotick::instrument::{
    AdmissionError, AssetCode, AssetDefinition, InstrumentCatalog, InstrumentDefinition,
    InstrumentError, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules, QuantityRules,
    TradingState,
};
use quotick::matching::{
    CancelOrder, Command, CommandOutcome, NewOrder, OrderBook, OrderType, RejectReason,
    ReplaceOrder, SelfTradePrevention, TimeInForce, Trade,
};

fn asset_id(value: u64) -> AssetId {
    AssetId::new(value).unwrap()
}

fn instrument_id(value: u64) -> InstrumentId {
    InstrumentId::new(value).unwrap()
}

fn version(value: u64) -> InstrumentVersion {
    InstrumentVersion::new(value).unwrap()
}

fn definition_with(
    version_value: u64,
    effective_from: u64,
    tick_size: u64,
    trading_state: TradingState,
) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument_id(10),
        version: version(version_value),
        effective_from: TimestampNs::from_unix_nanos(effective_from),
        symbol: InstrumentSymbol::new("BTC-USD").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: asset_id(1),
        quote_asset_id: asset_id(2),
        price: PriceRules::new(2, tick_size, Price::from_raw(-100), Price::from_raw(100)).unwrap(),
        quantity: QuantityRules::new(5, 10, 100).unwrap(),
        base_units_per_lot: 100_000_000,
        quote_units_per_price_unit: 1,
        trading_state,
    })
    .unwrap()
}

fn new_order(
    command: u64,
    order: u64,
    instrument: u64,
    instrument_version: u64,
    quantity: u64,
    order_type: OrderType,
) -> NewOrder {
    NewOrder {
        command_id: CommandId::new(command).unwrap(),
        order_id: OrderId::new(order).unwrap(),
        account_id: AccountId::new(7).unwrap(),
        instrument_id: instrument_id(instrument),
        instrument_version: version(instrument_version),
        side: Side::Buy,
        quantity: Quantity::new(quantity).unwrap(),
        order_type,
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: TimestampNs::from_unix_nanos(command),
    }
}

#[test]
fn canonical_codes_scales_and_definition_identity_are_validated() {
    assert_eq!(AssetCode::new("USD").unwrap().as_str(), "USD");
    assert_eq!(
        InstrumentSymbol::new("BTC-USD").unwrap().as_str(),
        "BTC-USD"
    );
    assert!(matches!(
        AssetCode::new(""),
        Err(InstrumentError::EmptyText("asset code"))
    ));
    assert!(matches!(
        AssetCode::new("usd"),
        Err(InstrumentError::InvalidTextByte { byte: b'u', .. })
    ));
    assert!(matches!(
        AssetCode::new("ABCDEFGHIJKLMNOPQ"),
        Err(InstrumentError::TextTooLong { maximum: 16, .. })
    ));
    assert_eq!(
        AssetDefinition::new(asset_id(1), AssetCode::new("USD").unwrap(), 19),
        Err(InstrumentError::DecimalScaleTooLarge(19))
    );

    let mut invalid = definition_with(1, 0, 5, TradingState::Open);
    let invalid_spec = InstrumentSpec {
        instrument_id: invalid.instrument_id(),
        version: invalid.version(),
        effective_from: invalid.effective_from(),
        symbol: invalid.symbol(),
        kind: invalid.kind(),
        base_asset_id: asset_id(1),
        quote_asset_id: asset_id(1),
        price: invalid.price_rules(),
        quantity: invalid.quantity_rules(),
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state: invalid.trading_state(),
    };
    assert_eq!(
        InstrumentDefinition::new(invalid_spec),
        Err(InstrumentError::IdenticalAssets)
    );

    // Preserve a use of the valid copy so accidental non-Copy changes surface here.
    invalid = definition_with(1, 0, 5, TradingState::Open);
    assert_eq!(invalid.version(), version(1));
}

#[test]
fn price_and_quantity_rules_enforce_signed_grids_and_inclusive_bounds() {
    assert_eq!(
        PriceRules::new(19, 1, Price::from_raw(0), Price::from_raw(1)),
        Err(InstrumentError::DecimalScaleTooLarge(19))
    );
    assert_eq!(
        PriceRules::new(2, 0, Price::from_raw(-100), Price::from_raw(100)),
        Err(InstrumentError::InvalidTickSize)
    );
    assert_eq!(
        PriceRules::new(2, 5, Price::from_raw(100), Price::from_raw(-100)),
        Err(InstrumentError::InvalidPriceRange)
    );
    assert_eq!(
        PriceRules::new(2, 5, Price::from_raw(-99), Price::from_raw(100)),
        Err(InstrumentError::CollarOffTickGrid)
    );

    let prices = PriceRules::new(2, 5, Price::from_raw(-100), Price::from_raw(100)).unwrap();
    for raw in [-100, -95, 0, 100] {
        assert_eq!(prices.validate(Price::from_raw(raw)), Ok(()));
    }
    assert_eq!(
        prices.validate(Price::from_raw(-97)),
        Err(AdmissionError::PriceOffTickGrid)
    );
    assert_eq!(
        prices.validate(Price::from_raw(105)),
        Err(AdmissionError::PriceOutsideCollar)
    );

    assert_eq!(
        QuantityRules::new(0, 10, 100),
        Err(InstrumentError::InvalidQuantityRule)
    );
    assert_eq!(
        QuantityRules::new(5, 11, 100),
        Err(InstrumentError::QuantityBoundaryOffGrid)
    );
    let quantities = QuantityRules::new(5, 10, 100).unwrap();
    for lots in [10, 55, 100] {
        assert_eq!(quantities.validate(Quantity::new(lots).unwrap()), Ok(()));
    }
    assert_eq!(
        quantities.validate(Quantity::new(11).unwrap()),
        Err(AdmissionError::QuantityOffGrid)
    );
    assert_eq!(
        quantities.validate(Quantity::new(5).unwrap()),
        Err(AdmissionError::QuantityOutsideLimits)
    );
}

#[test]
fn catalog_is_append_only_and_resolves_effective_versions() {
    let mut catalog = InstrumentCatalog::new();
    let btc = AssetDefinition::new(asset_id(1), AssetCode::new("BTC").unwrap(), 8).unwrap();
    let usd = AssetDefinition::new(asset_id(2), AssetCode::new("USD").unwrap(), 2).unwrap();
    catalog.register_asset(btc).unwrap();
    catalog.register_asset(usd).unwrap();
    assert_eq!(*catalog.asset(asset_id(1)).unwrap(), btc);
    assert_eq!(
        *catalog
            .asset_by_code(AssetCode::new("USD").unwrap())
            .unwrap(),
        usd
    );
    assert_eq!(
        catalog.asset_by_code(AssetCode::new("EUR").unwrap()),
        Err(InstrumentError::UnknownAssetCode(
            AssetCode::new("EUR").unwrap()
        ))
    );
    assert_eq!(
        catalog.register_asset(btc),
        Err(InstrumentError::DuplicateAsset)
    );

    let first = definition_with(1, 100, 5, TradingState::Open);
    let second = definition_with(2, 200, 10, TradingState::Halted);
    catalog.register_instrument(first).unwrap();
    catalog.register_instrument(second).unwrap();
    assert_eq!(
        catalog.register_instrument(first),
        Err(InstrumentError::DuplicateInstrumentVersion)
    );

    assert_eq!(
        catalog.effective(instrument_id(10), TimestampNs::from_unix_nanos(99)),
        Err(InstrumentError::NoEffectiveVersion {
            instrument_id: instrument_id(10),
            at: TimestampNs::from_unix_nanos(99),
        })
    );
    assert_eq!(
        *catalog
            .effective(instrument_id(10), TimestampNs::from_unix_nanos(100))
            .unwrap(),
        first
    );
    assert_eq!(
        *catalog
            .effective(instrument_id(10), TimestampNs::from_unix_nanos(199))
            .unwrap(),
        first
    );
    assert_eq!(
        *catalog
            .effective(instrument_id(10), TimestampNs::from_unix_nanos(200))
            .unwrap(),
        second
    );
    assert_eq!(
        *catalog.version(instrument_id(10), version(1)).unwrap(),
        first
    );
    assert_eq!(
        catalog.version(instrument_id(10), version(3)),
        Err(InstrumentError::UnknownInstrumentVersion {
            instrument_id: instrument_id(10),
            version: version(3),
        })
    );

    assert_eq!(
        catalog.register_instrument(definition_with(3, 150, 5, TradingState::Open)),
        Err(InstrumentError::NonMonotonicVersion)
    );

    let changed_identity = InstrumentDefinition::new(InstrumentSpec {
        symbol: InstrumentSymbol::new("XBT-USD").unwrap(),
        version: version(3),
        effective_from: TimestampNs::from_unix_nanos(300),
        ..first_spec(first)
    })
    .unwrap();
    assert_eq!(
        catalog.register_instrument(changed_identity),
        Err(InstrumentError::IdentityChanged)
    );

    let mut incomplete = InstrumentCatalog::new();
    incomplete.register_asset(btc).unwrap();
    assert_eq!(
        incomplete.register_instrument(first),
        Err(InstrumentError::UnknownAsset(asset_id(2)))
    );
}

fn first_spec(definition: InstrumentDefinition) -> InstrumentSpec {
    let convention = definition.settlement_convention();
    InstrumentSpec {
        instrument_id: definition.instrument_id(),
        version: definition.version(),
        effective_from: definition.effective_from(),
        symbol: definition.symbol(),
        kind: definition.kind(),
        base_asset_id: definition.base_asset_id(),
        quote_asset_id: definition.quote_asset_id(),
        price: definition.price_rules(),
        quantity: definition.quantity_rules(),
        base_units_per_lot: convention.base_units_per_lot,
        quote_units_per_price_unit: convention.quote_units_per_price_unit,
        trading_state: definition.trading_state(),
    }
}

#[test]
fn definition_controls_entry_amendment_cancellation_and_settlement_version() {
    let halted = definition_with(1, 100, 5, TradingState::Halted);
    let new = new_order(1, 1, 10, 1, 10, OrderType::Limit(Price::from_raw(5)));
    assert_eq!(
        halted.admit(Command::New(new)),
        Err(AdmissionError::TradingStateDisallowsEntry)
    );

    let replace = ReplaceOrder {
        command_id: CommandId::new(2).unwrap(),
        order_id: OrderId::new(1).unwrap(),
        account_id: AccountId::new(7).unwrap(),
        instrument_id: instrument_id(10),
        instrument_version: version(1),
        new_quantity: Quantity::new(10).unwrap(),
        new_price: Price::from_raw(5),
        received_at: TimestampNs::from_unix_nanos(2),
    };
    assert_eq!(
        halted.admit(Command::Replace(replace)),
        Err(AdmissionError::TradingStateDisallowsEntry)
    );

    let cancel = CancelOrder {
        command_id: CommandId::new(3).unwrap(),
        order_id: OrderId::new(1).unwrap(),
        account_id: AccountId::new(7).unwrap(),
        instrument_id: instrument_id(10),
        instrument_version: version(1),
        received_at: TimestampNs::from_unix_nanos(3),
    };
    assert_eq!(halted.admit(Command::Cancel(cancel)), Ok(()));

    let trade = Trade {
        trade_id: TradeId::new(1).unwrap(),
        instrument_id: instrument_id(10),
        instrument_version: version(1),
        price: Price::from_raw(50),
        quantity: Quantity::new(10).unwrap(),
        buy_order_id: OrderId::new(1).unwrap(),
        sell_order_id: OrderId::new(2).unwrap(),
        buyer_account_id: AccountId::new(7).unwrap(),
        seller_account_id: AccountId::new(8).unwrap(),
        maker_order_id: OrderId::new(2).unwrap(),
        taker_order_id: OrderId::new(1).unwrap(),
    };
    let convention = halted.settlement_for_trade(&trade).unwrap();
    assert_eq!(convention.base_asset_id, asset_id(1));
    assert_eq!(convention.quote_asset_id, asset_id(2));
    assert_eq!(convention.base_units_per_lot, 100_000_000);
    let wrong_version = Trade {
        instrument_version: version(2),
        ..trade
    };
    assert_eq!(
        halted.settlement_for_trade(&wrong_version),
        Err(AdmissionError::WrongVersion)
    );
}

#[test]
fn matching_rejects_definition_violations_without_book_mutation() {
    let definition = definition_with(1, 0, 5, TradingState::Open);
    let cases = [
        (
            new_order(1, 1, 10, 2, 10, OrderType::Limit(Price::from_raw(5))),
            RejectReason::WrongInstrumentVersion,
        ),
        (
            new_order(2, 2, 11, 1, 10, OrderType::Limit(Price::from_raw(5))),
            RejectReason::WrongInstrument,
        ),
        (
            new_order(3, 3, 10, 1, 11, OrderType::Limit(Price::from_raw(5))),
            RejectReason::QuantityOffGrid,
        ),
        (
            new_order(4, 4, 10, 1, 5, OrderType::Limit(Price::from_raw(5))),
            RejectReason::QuantityOutsideLimits,
        ),
        (
            new_order(5, 5, 10, 1, 10, OrderType::Limit(Price::from_raw(3))),
            RejectReason::PriceOffTickGrid,
        ),
        (
            new_order(6, 6, 10, 1, 10, OrderType::Limit(Price::from_raw(105))),
            RejectReason::PriceOutsideCollar,
        ),
    ];

    let mut book = OrderBook::new(definition);
    for (command, expected) in cases {
        let report = book.submit(Command::New(command)).unwrap();
        assert_eq!(report.outcome, CommandOutcome::Rejected(expected));
        assert_eq!(report.events.len(), 1);
        assert!(book.best_bid().is_none());
        assert!(book.best_ask().is_none());
        book.validate().unwrap();
    }

    let mut halted = OrderBook::new(definition_with(1, 0, 5, TradingState::Halted));
    let report = halted
        .submit(Command::New(new_order(7, 7, 10, 1, 10, OrderType::Market)))
        .unwrap();
    assert_eq!(
        report.outcome,
        CommandOutcome::Rejected(RejectReason::InstrumentNotOpen)
    );
    assert_eq!(report.events.len(), 1);
    halted.validate().unwrap();
}
