use chrono::{TimeZone, Utc};
use rustcta::backtest::matching::{
    book::{DepthDelta, OrderBookState},
    ledger::{BacktestLedger, FillResult, FundingSettlement},
};
use rustcta::core::types::{MarketType, OrderSide, OrderType};

#[test]
fn order_book_state_applies_snapshot_and_delta_updates() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut state = OrderBookState::new("BTC/USDT", 5);

    state.apply_snapshot(
        vec![[100.0, 5.0], [99.0, 3.0]],
        vec![[101.0, 4.0], [102.0, 2.0]],
        10,
        ts,
    );
    assert_eq!(state.best_bid(), Some([100.0, 5.0]));
    assert_eq!(state.best_ask(), Some([101.0, 4.0]));

    state
        .apply_delta(DepthDelta {
            first_update_id: 11,
            final_update_id: 11,
            bids: vec![[100.0, 2.0], [98.0, 1.0]],
            asks: vec![[101.0, 0.0], [100.5, 3.0]],
            timestamp: ts,
        })
        .expect("delta should apply");

    assert_eq!(state.best_bid(), Some([100.0, 2.0]));
    assert_eq!(state.best_ask(), Some([100.5, 3.0]));
    assert_eq!(state.last_update_id(), Some(11));
}

#[test]
fn ledger_updates_equity_after_fill_and_funding() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut ledger = BacktestLedger::new("USDT", 10_000.0);

    ledger
        .apply_fill(FillResult {
            symbol: "BTC/USDT".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            market_type: MarketType::Futures,
            quantity: 0.1,
            price: 100.0,
            is_maker: true,
            fee_paid: 0.01,
            timestamp: ts,
        })
        .expect("fill should apply");

    let position = ledger.position("BTC/USDT").expect("position should exist");
    assert_eq!(position.quantity, 0.1);
    assert_eq!(position.entry_price, 100.0);
    assert!(ledger.cash_balance() < 10_000.0);

    ledger.apply_mark_price("BTC/USDT", 105.0, ts);
    assert!(ledger.unrealized_pnl("BTC/USDT") > 0.0);

    ledger.apply_funding(FundingSettlement {
        symbol: "BTC/USDT".to_string(),
        rate: 0.0001,
        mark_price: 105.0,
        timestamp: ts,
    });

    assert!(ledger.total_funding_paid().abs() > 0.0);
    assert!(ledger.total_equity() > 0.0);
}

#[test]
fn ledger_realizes_pnl_when_futures_position_is_closed() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut ledger = BacktestLedger::new("USDT", 10_000.0);

    ledger
        .apply_fill(FillResult {
            symbol: "BTC/USDT".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Market,
            market_type: MarketType::Futures,
            quantity: 1.0,
            price: 100.0,
            is_maker: false,
            fee_paid: 0.05,
            timestamp: ts,
        })
        .expect("entry fill should apply");

    ledger.apply_mark_price("BTC/USDT", 110.0, ts);
    assert_eq!(ledger.unrealized_pnl("BTC/USDT"), 10.0);

    ledger
        .apply_fill(FillResult {
            symbol: "BTC/USDT".to_string(),
            side: OrderSide::Sell,
            order_type: OrderType::Market,
            market_type: MarketType::Futures,
            quantity: 1.0,
            price: 110.0,
            is_maker: false,
            fee_paid: 0.05,
            timestamp: ts,
        })
        .expect("exit fill should apply");

    assert!(ledger.position("BTC/USDT").is_none());
    assert_eq!(ledger.realized_pnl(), 10.0);
    assert_eq!(ledger.unrealized_pnl("BTC/USDT"), 0.0);
    assert!((ledger.cash_balance() - 10_009.9).abs() < 1e-9);
    assert!((ledger.total_equity() - 10_009.9).abs() < 1e-9);
}
