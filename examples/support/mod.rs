#![allow(
    dead_code,
    reason = "each executable example selects a different subset of the shared fixtures"
)]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use quotick::auction_book::{CallAuctionBookLimits, CallAuctionBookLimitsSpec};
use quotick::auction_engine::{CallAuctionEngineLimits, CallAuctionEngineLimitsSpec};
use quotick::auction_risk::{CallAuctionRiskLimits, CallAuctionRiskLimitsSpec};
use quotick::instrument::{
    InstrumentDefinition, InstrumentKind, InstrumentSpec, InstrumentSymbol, PriceRules,
    QuantityRules, ReserveOrderRules, TradingState,
};
use quotick::journal::{Durability, JournalOptions};
use quotick::ledger::{Ledger, LedgerLimitsSpec};
use quotick::matching::{
    Command, EventKind, NewOrder, OrderBookLimits, OrderBookLimitsSpec, OrderDisplay, OrderType,
    SelfTradePrevention, TimeInForce, Trade,
};
use quotick::risk::{
    AccountRiskDefinition, AccountRiskState, RiskLimitSpec, RiskLimits, RiskManagedLimits,
    RiskManagedLimitsSpec, RiskProfile,
};
use quotick::{
    AccountId, AssetId, CommandId, InstrumentId, InstrumentVersion, OrderId, Price, Quantity, Side,
    TimestampNs,
};

pub const INSTRUMENT: u64 = 7001;
pub const BASE_ASSET: u64 = 1;
pub const QUOTE_ASSET: u64 = 2;

pub fn account(value: u64) -> AccountId {
    AccountId::new(value).expect("example account identifiers are non-zero")
}

pub fn asset(value: u64) -> AssetId {
    AssetId::new(value).expect("example asset identifiers are non-zero")
}

pub fn instrument() -> InstrumentId {
    InstrumentId::new(INSTRUMENT).expect("example instrument identifier is non-zero")
}

pub fn version(value: u64) -> InstrumentVersion {
    InstrumentVersion::new(value).expect("example instrument versions are non-zero")
}

pub fn timestamp(value: u64) -> TimestampNs {
    TimestampNs::from_unix_nanos(value)
}

pub fn definition(symbol: &str) -> InstrumentDefinition {
    definition_version(symbol, 1, 0, 1, TradingState::Open)
}

pub fn definition_version(
    symbol: &str,
    version_value: u64,
    effective_from: u64,
    tick_size: u64,
    trading_state: TradingState,
) -> InstrumentDefinition {
    InstrumentDefinition::new(InstrumentSpec {
        instrument_id: instrument(),
        version: version(version_value),
        effective_from: timestamp(effective_from),
        symbol: InstrumentSymbol::new(symbol).expect("example symbol is canonical"),
        kind: InstrumentKind::Spot,
        base_asset_id: asset(BASE_ASSET),
        quote_asset_id: asset(QUOTE_ASSET),
        price: PriceRules::new(2, tick_size, Price::from_raw(0), Price::from_raw(1_000_000))
            .expect("example price rules are valid"),
        quantity: QuantityRules::new(1, 1, 10_000).expect("example quantity rules are valid"),
        reserve: ReserveOrderRules::new(100).expect("reserve refresh bound is non-zero"),
        hidden_orders_supported: true,
        base_units_per_lot: 1,
        quote_units_per_price_unit: 1,
        trading_state,
    })
    .expect("example instrument definition is valid")
}

pub fn matching_limits() -> OrderBookLimits {
    OrderBookLimits::new(OrderBookLimitsSpec {
        max_active_orders: 32,
        max_active_accounts: 32,
        max_price_levels_per_side: 32,
        max_accepted_order_ids: 256,
        max_account_controls: 64,
        max_retained_commands: 256,
        cancellation_reserve: 32,
        max_report_events: 128,
        max_retained_events: 8_192,
        max_prepared_order_selections: 2,
    })
    .expect("example matching limits are coherent")
}

pub fn risk_limits() -> RiskManagedLimits {
    RiskManagedLimits::new(RiskManagedLimitsSpec {
        matching: matching_limits(),
        max_registered_accounts: 32,
    })
    .expect("example coupled limits are coherent")
}

pub fn profile(initial_position_lots: i128, max_order_quantity_lots: u64) -> RiskProfile {
    RiskProfile::new(
        AccountRiskState::Active,
        initial_position_lots,
        RiskLimits::new(RiskLimitSpec {
            max_order_quantity_lots,
            max_order_notional: 500_000_000,
            max_open_orders: 16,
            max_open_quantity_lots: 20_000,
            max_open_notional: 1_000_000_000,
            max_long_position_lots: 20_000,
            max_short_position_lots: 20_000,
        })
        .expect("example risk limits are coherent"),
    )
    .expect("example risk profile is coherent")
}

pub fn account_definition(
    account_id: u64,
    initial_position_lots: i128,
    max_order_quantity_lots: u64,
) -> AccountRiskDefinition {
    AccountRiskDefinition::new(
        account(account_id),
        profile(initial_position_lots, max_order_quantity_lots),
    )
}

#[derive(Clone, Copy, Debug)]
pub struct LimitOrder {
    pub command_id: u64,
    pub order_id: u64,
    pub account_id: u64,
    pub side: Side,
    pub quantity: u64,
    pub price: i64,
    pub time_in_force: TimeInForce,
    pub display: OrderDisplay,
    pub self_trade_prevention: SelfTradePrevention,
}

impl LimitOrder {
    pub fn resting(
        command_id: u64,
        order_id: u64,
        account_id: u64,
        side: Side,
        quantity: u64,
        price: i64,
    ) -> Self {
        Self {
            command_id,
            order_id,
            account_id,
            side,
            quantity,
            price,
            time_in_force: TimeInForce::GoodTilCancelled,
            display: OrderDisplay::FullyDisplayed,
            self_trade_prevention: SelfTradePrevention::CancelAggressor,
        }
    }

    pub fn command(self, definition: InstrumentDefinition) -> Command {
        Command::New(NewOrder {
            command_id: CommandId::new(self.command_id)
                .expect("example command identifiers are non-zero"),
            order_id: OrderId::new(self.order_id).expect("example order identifiers are non-zero"),
            account_id: account(self.account_id),
            instrument_id: definition.instrument_id(),
            instrument_version: definition.version(),
            side: self.side,
            quantity: Quantity::new(self.quantity).expect("example quantities are non-zero"),
            display: self.display,
            order_type: OrderType::Limit(Price::from_raw(self.price)),
            time_in_force: self.time_in_force,
            self_trade_prevention: self.self_trade_prevention,
            received_at: timestamp(self.command_id),
        })
    }
}

pub fn trades(report: &quotick::matching::ExecutionReport) -> Vec<Trade> {
    report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::Trade(trade) => Some(trade),
            _ => None,
        })
        .collect()
}

pub fn auction_engine_limits() -> CallAuctionEngineLimits {
    let book = CallAuctionBookLimits::new(CallAuctionBookLimitsSpec {
        max_active_orders: 16,
        max_price_levels_per_side: 16,
        max_accepted_order_ids: 128,
        max_prepared_uncrosses: 2,
    })
    .expect("example call-auction book limits are coherent");
    CallAuctionEngineLimits::new(CallAuctionEngineLimitsSpec {
        book,
        max_retained_commands: 128,
        terminal_command_reserve: 18,
        max_retained_events: 256,
        max_report_events: 33,
    })
    .expect("example call-auction engine limits are coherent")
}

pub fn auction_risk_limits() -> CallAuctionRiskLimits {
    CallAuctionRiskLimits::new(CallAuctionRiskLimitsSpec {
        auction: auction_engine_limits(),
        max_registered_accounts: 32,
    })
    .expect("example call-auction risk limits are coherent")
}

pub fn ledger_limits_spec() -> LedgerLimitsSpec {
    LedgerLimitsSpec {
        max_balance_keys: 64,
        max_transactions: 64,
        max_reversals: 32,
        max_records: 64,
        max_postings_per_transaction: 8,
        max_transactions_per_record: 8,
        max_retained_postings: 256,
    }
}

pub fn ledger() -> Ledger {
    Ledger::try_with_limits(ledger_limits_spec())
        .expect("example ledger limits are coherent and reservable")
}

pub fn journal_options() -> JournalOptions {
    JournalOptions {
        durability: Durability::SyncAll,
        ..JournalOptions::default()
    }
}

static NEXT_SCRATCH: AtomicU64 = AtomicU64::new(1);

pub struct ScratchArea(PathBuf);

impl ScratchArea {
    pub fn new(label: &str) -> Self {
        let nonce = NEXT_SCRATCH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "quotick-example-{label}-{}-{nonce}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir(&path).expect("example scratch directory can be created");
        Self(path)
    }

    pub fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for ScratchArea {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
