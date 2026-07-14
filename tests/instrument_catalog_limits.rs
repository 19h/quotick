use quotick::domain::{AssetId, InstrumentId, InstrumentVersion, Price, TimestampNs};
use quotick::instrument::{
    AssetCode, AssetDefinition, InstrumentCatalog, InstrumentCatalogConstructionError,
    InstrumentCatalogHashIndex, InstrumentCatalogLimits, InstrumentCatalogLimitsError,
    InstrumentCatalogLimitsSpec, InstrumentCatalogResource, InstrumentDefinition, InstrumentError,
    InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules, QuantityRules, ReserveOrderRules,
    TradingState,
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

fn asset(value: u64, code: &str) -> AssetDefinition {
    AssetDefinition::new(asset_id(value), AssetCode::new(code).unwrap(), 8).unwrap()
}

fn definition(instrument: u64, version_value: u64, effective_from: u64) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument_id(instrument),
        version: version(version_value),
        effective_from: TimestampNs::from_unix_nanos(effective_from),
        symbol: InstrumentSymbol::new("BASE-QUOTE").unwrap(),
        kind: InstrumentKind::Spot,
        base_asset_id: asset_id(1),
        quote_asset_id: asset_id(2),
        price: PriceRules::new(2, 1, Price::from_raw(-1_000), Price::from_raw(1_000)).unwrap(),
        quantity: QuantityRules::new(1, 1, 1_000).unwrap(),
        reserve: ReserveOrderRules::disabled(),
        base_units_per_lot: 100_000_000,
        quote_units_per_price_unit: 1,
        trading_state: if version_value % 2 == 0 {
            TradingState::Halted
        } else {
            TradingState::Open
        },
    })
    .unwrap()
}

fn catalog(spec: InstrumentCatalogLimitsSpec) -> InstrumentCatalog {
    let mut catalog = InstrumentCatalog::try_with_limits(spec).unwrap();
    catalog.register_asset(asset(1, "BASE")).unwrap();
    catalog.register_asset(asset(2, "QUOTE")).unwrap();
    catalog
}

#[test]
fn catalog_limits_reject_zero_contradictory_and_unrepresentable_layouts() {
    assert_eq!(
        InstrumentCatalogLimits::new(InstrumentCatalogLimitsSpec {
            max_assets: 0,
            max_instruments: 1,
            max_instrument_versions: 1,
        }),
        Err(InstrumentCatalogLimitsError::ZeroMaximum(
            InstrumentCatalogResource::AssetIdentifiers
        ))
    );
    assert_eq!(
        InstrumentCatalogLimits::new(InstrumentCatalogLimitsSpec {
            max_assets: 1,
            max_instruments: 2,
            max_instrument_versions: 1,
        }),
        Err(InstrumentCatalogLimitsError::InstrumentsExceedVersions {
            instruments: 2,
            versions: 1,
        })
    );
    assert_eq!(
        InstrumentCatalog::try_with_limits(InstrumentCatalogLimitsSpec {
            max_assets: 1,
            max_instruments: 1,
            max_instrument_versions: usize::MAX,
        })
        .unwrap_err(),
        InstrumentCatalogConstructionError::ReservationFailed {
            resource: InstrumentCatalogResource::InstrumentVersions,
            maximum: usize::MAX,
        }
    );
}

#[test]
fn asset_capacity_failure_is_atomic_and_duplicate_errors_take_precedence() {
    let spec = InstrumentCatalogLimitsSpec {
        max_assets: 2,
        max_instruments: 1,
        max_instrument_versions: 1,
    };
    let mut catalog = catalog(spec);
    let before = catalog.hash_index_status(InstrumentCatalogHashIndex::AssetIdentifiers);

    assert_eq!(
        catalog.register_asset(asset(1, "BASE")),
        Err(InstrumentError::DuplicateAsset)
    );
    assert_eq!(
        catalog.register_asset(asset(3, "THIRD")),
        Err(InstrumentError::CapacityExceeded {
            resource: InstrumentCatalogResource::AssetIdentifiers,
            maximum: 2,
            attempted: 3,
        })
    );
    assert_eq!(
        catalog.asset(asset_id(3)),
        Err(InstrumentError::UnknownAsset(asset_id(3)))
    );
    assert_eq!(
        catalog.asset_by_code(AssetCode::new("THIRD").unwrap()),
        Err(InstrumentError::UnknownAssetCode(
            AssetCode::new("THIRD").unwrap()
        ))
    );

    let after = catalog.hash_index_status(InstrumentCatalogHashIndex::AssetIdentifiers);
    assert_eq!(after.occupied_entries, 2);
    assert_eq!(after.allocated_entries, before.allocated_entries);
    assert_eq!(after.initialized_buckets, before.initialized_buckets);
    catalog.validate().unwrap();
}

#[test]
fn instrument_and_version_limits_are_independent_and_fail_without_mutation() {
    let mut catalog = catalog(InstrumentCatalogLimitsSpec {
        max_assets: 2,
        max_instruments: 1,
        max_instrument_versions: 3,
    });
    let first = definition(10, 1, 100);
    let second = definition(10, 2, 200);
    let third = definition(10, 3, 300);
    catalog.register_instrument(first).unwrap();
    catalog.register_instrument(second).unwrap();

    assert_eq!(
        catalog.register_instrument(definition(20, 1, 100)),
        Err(InstrumentError::CapacityExceeded {
            resource: InstrumentCatalogResource::Instruments,
            maximum: 1,
            attempted: 2,
        })
    );
    catalog.register_instrument(third).unwrap();
    let full_status = catalog.definition_arena_status();
    assert_eq!(full_status.occupied_definitions, 3);
    assert_eq!(
        catalog.register_instrument(definition(10, 3, 300)),
        Err(InstrumentError::DuplicateInstrumentVersion)
    );
    assert_eq!(
        catalog.register_instrument(definition(10, 4, 400)),
        Err(InstrumentError::CapacityExceeded {
            resource: InstrumentCatalogResource::InstrumentVersions,
            maximum: 3,
            attempted: 4,
        })
    );
    assert_eq!(catalog.definition_arena_status(), full_status);
    assert_eq!(
        *catalog.version(instrument_id(10), version(3)).unwrap(),
        third
    );
    assert_eq!(
        catalog.version(instrument_id(20), version(1)),
        Err(InstrumentError::UnknownInstrument(instrument_id(20)))
    );
    catalog.validate().unwrap();
}

#[test]
fn interleaved_version_registration_rebases_ranges_without_lookup_drift() {
    let mut catalog = catalog(InstrumentCatalogLimitsSpec {
        max_assets: 2,
        max_instruments: 3,
        max_instrument_versions: 9,
    });
    for definition in [
        definition(10, 1, 100),
        definition(20, 1, 100),
        definition(30, 1, 100),
        definition(10, 2, 200),
        definition(20, 2, 200),
        definition(10, 3, 300),
        definition(30, 2, 200),
        definition(20, 3, 300),
        definition(30, 3, 300),
    ] {
        catalog.register_instrument(definition).unwrap();
        catalog.validate().unwrap();
    }

    for instrument in [10_u64, 20, 30] {
        for version_value in 1_u64..=3 {
            assert_eq!(
                *catalog
                    .version(instrument_id(instrument), version(version_value))
                    .unwrap(),
                definition(instrument, version_value, version_value * 100)
            );
        }
        assert_eq!(
            catalog
                .effective(instrument_id(instrument), TimestampNs::from_unix_nanos(250))
                .unwrap()
                .version(),
            version(2)
        );
    }
    assert_eq!(catalog.definition_arena_status().occupied_definitions, 9);
}

#[test]
fn generated_full_arena_retains_all_capacities_and_effective_histories() {
    const INSTRUMENTS: usize = 16;
    const VERSIONS_PER_INSTRUMENT: usize = 64;
    const DEFINITIONS: usize = INSTRUMENTS * VERSIONS_PER_INSTRUMENT;
    let mut catalog = catalog(InstrumentCatalogLimitsSpec {
        max_assets: 2,
        max_instruments: INSTRUMENTS,
        max_instrument_versions: DEFINITIONS,
    });
    let initial_hash = catalog.hash_index_status(InstrumentCatalogHashIndex::Instruments);
    let initial_arena = catalog.definition_arena_status();

    for version_value in 1_u64..=u64::try_from(VERSIONS_PER_INSTRUMENT).unwrap() {
        for offset in 0..INSTRUMENTS {
            let instrument = 100 + u64::try_from(offset).unwrap();
            catalog
                .register_instrument(definition(instrument, version_value, version_value * 1_000))
                .unwrap();
        }
        if version_value % 8 == 0 {
            catalog.validate().unwrap();
        }
    }

    let final_hash = catalog.hash_index_status(InstrumentCatalogHashIndex::Instruments);
    let final_arena = catalog.definition_arena_status();
    assert_eq!(final_hash.occupied_entries, INSTRUMENTS);
    assert_eq!(final_hash.allocated_entries, initial_hash.allocated_entries);
    assert_eq!(
        final_hash.initialized_buckets,
        initial_hash.initialized_buckets
    );
    assert_eq!(final_arena.occupied_definitions, DEFINITIONS);
    assert_eq!(
        final_arena.allocated_definitions,
        initial_arena.allocated_definitions
    );
    for offset in 0..INSTRUMENTS {
        let instrument = instrument_id(100 + u64::try_from(offset).unwrap());
        assert_eq!(
            catalog
                .effective(instrument, TimestampNs::from_unix_nanos(63_500))
                .unwrap()
                .version(),
            version(63)
        );
        assert_eq!(
            catalog.version(instrument, version(64)).unwrap().version(),
            version(64)
        );
    }
    catalog.validate().unwrap();
}
