//! Reserve priority, hidden liquidity, stops, expiry, and administrative fences.

mod support;

use quotick::instrument::TradingState;
use quotick::matching::{
    AccountControl, AccountControlAction, Command, CommandOutcome, EventKind, ExpirySweep,
    NewOrder, OrderBook, OrderDisplay, OrderType, RejectReason, SelfTradePrevention,
    StopActivation, StopTriggerSweep, TimeInForce, TradingStateControl, TradingStateControlAction,
};
use quotick::{CommandId, OrderId, Price, Quantity, Side};

use support::{LimitOrder, account, definition, matching_limits, stop_reference, timestamp};

fn stop_market(
    definition: quotick::instrument::InstrumentDefinition,
    command_id: u64,
    order_id: u64,
    account_id: u64,
    quantity: u64,
    trigger_price: i64,
) -> Command {
    Command::New(NewOrder {
        command_id: CommandId::new(command_id).unwrap(),
        order_id: OrderId::new(order_id).unwrap(),
        account_id: account(account_id),
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        side: Side::Buy,
        quantity: Quantity::new(quantity).unwrap(),
        display: OrderDisplay::FullyDisplayed,
        order_type: OrderType::Stop {
            trigger_price: Price::from_raw(trigger_price),
            activation: StopActivation::Market,
        },
        time_in_force: TimeInForce::GoodTilCancelled,
        self_trade_prevention: SelfTradePrevention::CancelAggressor,
        received_at: timestamp(command_id),
    })
}

fn trigger(
    definition: quotick::instrument::InstrumentDefinition,
    command_id: u64,
    source_sequence: u64,
    reference_price: i64,
) -> Command {
    Command::StopTriggerSweep(StopTriggerSweep {
        command_id: CommandId::new(command_id).unwrap(),
        instrument_id: definition.instrument_id(),
        instrument_version: definition.version(),
        reference: stop_reference(source_sequence, reference_price),
        maximum_orders: 8,
        received_at: timestamp(command_id),
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "one chronological example keeps the interacting order lifecycles auditable"
)]
fn main() {
    let definition = definition("ORDER-LIFECYCLE");
    let mut book = OrderBook::try_with_limits(definition, matching_limits()).unwrap();

    let mut reserve = LimitOrder::resting(1, 1, 11, Side::Sell, 30, 10_000);
    reserve.display = OrderDisplay::Reserve {
        peak: Quantity::new(10).unwrap(),
    };
    book.submit(reserve.command(definition)).unwrap();
    book.submit(LimitOrder::resting(2, 2, 12, Side::Sell, 5, 10_000).command(definition))
        .unwrap();

    let mut take = LimitOrder::resting(3, 3, 13, Side::Buy, 20, 10_000);
    take.time_in_force = TimeInForce::ImmediateOrCancel;
    let priority_report = book.submit(take.command(definition)).unwrap();
    let makers: Vec<_> = priority_report
        .events
        .iter()
        .filter_map(|event| match event.kind {
            EventKind::Trade(trade) => Some((trade.maker_order_id.get(), trade.quantity.lots())),
            _ => None,
        })
        .collect();
    assert_eq!(makers, [(1, 10), (2, 5), (1, 5)]);
    assert!(
        priority_report
            .events
            .iter()
            .any(|event| matches!(event.kind, EventKind::OrderRefreshed { .. }))
    );

    book.submit(trigger(definition, 4, 1, 9_500)).unwrap();
    book.submit(stop_market(definition, 5, 4, 14, 3, 10_500))
        .unwrap();
    assert_eq!(book.dormant_stop_count(), 1);

    let mut hidden = LimitOrder::resting(6, 5, 15, Side::Buy, 2, 9_000);
    hidden.display = OrderDisplay::Hidden;
    book.submit(hidden.command(definition)).unwrap();
    assert!(book.level(Side::Buy, Price::from_raw(9_000)).is_none());

    book.submit(LimitOrder::resting(7, 6, 16, Side::Sell, 3, 10_200).command(definition))
        .unwrap();
    let triggered = book.submit(trigger(definition, 8, 2, 10_500)).unwrap();
    assert!(triggered.events.iter().any(|event| matches!(
        event.kind,
        EventKind::StopOrderTriggered { order_id, .. } if order_id == OrderId::new(4).unwrap()
    )));
    assert!(
        triggered
            .events
            .iter()
            .any(|event| matches!(event.kind, EventKind::Trade(_)))
    );
    assert_eq!(book.dormant_stop_count(), 0);
    let committed_reference = book.stop_reference().unwrap();
    assert_eq!(committed_reference.cursor().source_id().get(), 1);
    assert_eq!(committed_reference.cursor().source_version().get(), 1);
    assert_eq!(committed_reference.cursor().source_sequence().get(), 2);

    let mut expiring = LimitOrder::resting(9, 7, 17, Side::Buy, 2, 8_000);
    expiring.time_in_force = TimeInForce::GoodTilTimestamp {
        expires_at: timestamp(20),
    };
    book.submit(expiring.command(definition)).unwrap();
    let expiry = book
        .submit(Command::ExpirySweep(ExpirySweep {
            command_id: CommandId::new(10).unwrap(),
            instrument_id: definition.instrument_id(),
            instrument_version: definition.version(),
            through: timestamp(20),
            received_at: timestamp(20),
        }))
        .unwrap();
    assert!(expiry.events.iter().any(|event| matches!(
        event.kind,
        EventKind::ExpirySweepCompleted {
            expired_order_count: 1,
            ..
        }
    )));

    let blocked = book
        .submit(Command::AccountControl(AccountControl {
            command_id: CommandId::new(11).unwrap(),
            account_id: account(11),
            instrument_id: definition.instrument_id(),
            instrument_version: definition.version(),
            expected_revision: 0,
            action: AccountControlAction::BlockAndCancel,
            received_at: timestamp(21),
        }))
        .unwrap();
    assert!(blocked.events.iter().any(|event| matches!(
        event.kind,
        EventKind::AccountControlApplied {
            cancelled_order_count: 1,
            ..
        }
    )));

    let halted = book
        .submit(Command::TradingStateControl(TradingStateControl {
            command_id: CommandId::new(12).unwrap(),
            instrument_id: definition.instrument_id(),
            instrument_version: definition.version(),
            expected_revision: 0,
            target_state: TradingState::Halted,
            action: TradingStateControlAction::TransitionAndCancel,
            received_at: timestamp(22),
        }))
        .unwrap();
    assert_eq!(halted.outcome, CommandOutcome::Accepted);
    assert_eq!(book.active_order_count(), 0);
    assert_eq!(book.trading_state().state(), TradingState::Halted);

    let rejected = book
        .submit(LimitOrder::resting(13, 8, 18, Side::Buy, 1, 9_000).command(definition))
        .unwrap();
    assert_eq!(
        rejected.outcome,
        CommandOutcome::Rejected(RejectReason::InstrumentNotOpen)
    );
    book.validate().unwrap();

    println!(
        "priority_trades={} stop_events={} stop_source_sequence={} halt_events={} final_sequence={}",
        makers.len(),
        triggered.events.len(),
        committed_reference.cursor().source_sequence(),
        halted.events.len(),
        book.last_event_sequence()
    );
}
