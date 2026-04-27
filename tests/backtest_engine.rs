use chrono::{Duration, TimeZone, Utc};
use rustcta::backtest::matching::engine::BacktestEngineState;
use rustcta::backtest::schema::{
    BacktestEvent, DepthDeltaEvent, FundingRateEvent, MarkPriceEvent, TradeEvent,
};
use rustcta::backtest::strategy::StrategySignal;
use rustcta::core::types::{MarketType, OrderSide, OrderType, Trade};

#[test]
fn engine_state_updates_order_book_from_depth_events() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);

    engine
        .seed_order_book(
            "BTC/USDT",
            vec![[100.0, 5.0], [99.0, 3.0]],
            vec![[101.0, 4.0], [102.0, 2.0]],
            10,
            ts,
        )
        .expect("book should seed");

    engine
        .apply_event(&BacktestEvent::DepthDelta(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts,
            logical_ts: ts,
            first_update_id: 11,
            final_update_id: 11,
            bids: vec![[100.0, 2.5]],
            asks: vec![[101.0, 0.0], [100.5, 3.0]],
        }))
        .expect("depth event should apply");

    let book = engine
        .order_book("BTC/USDT")
        .expect("order book should exist");
    assert_eq!(book.best_bid(), Some([100.0, 2.5]));
    assert_eq!(book.best_ask(), Some([100.5, 3.0]));
}

#[test]
fn engine_state_updates_ledger_from_mark_and_funding_events() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);

    engine
        .ledger_mut()
        .apply_fill(rustcta::backtest::matching::ledger::FillResult {
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

    engine
        .apply_event(&BacktestEvent::MarkPrice(MarkPriceEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts,
            logical_ts: ts,
            mark_price: 105.0,
            index_price: 104.5,
            funding_rate: Some(0.0001),
            next_funding_time: None,
            market_type: MarketType::Futures,
        }))
        .expect("mark price should apply");

    assert!(engine.ledger().unrealized_pnl("BTC/USDT") > 0.0);

    engine
        .apply_event(&BacktestEvent::FundingRate(FundingRateEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts,
            logical_ts: ts,
            funding_rate: 0.0001,
            mark_price: Some(105.0),
        }))
        .expect("funding should apply");

    assert!(engine.ledger().total_funding_paid().abs() > 0.0);
}

#[test]
fn engine_executes_market_signal_into_ledger() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);

    let fill = engine
        .execute_market_signal(
            &StrategySignal {
                strategy: "mean_reversion".to_string(),
                symbol: "BTC/USDT".to_string(),
                side: OrderSide::Buy,
                logical_ts: ts,
                price: 100.0,
                band_percent: 0.1,
                z_score: -1.5,
                rsi: 20.0,
                atr: 2.0,
                reason: "test".to_string(),
            },
            MarketType::Futures,
            1_000.0,
            5.0,
        )
        .expect("signal should execute");

    assert_eq!(fill.quantity, 10.0);
    assert!(fill.fee_paid > 0.0);

    let position = engine
        .ledger()
        .position("BTC/USDT")
        .expect("position should exist");
    assert_eq!(position.quantity, 10.0);
    assert!(engine.ledger().total_fees_paid() > 0.0);
    assert!(engine.ledger().cash_balance() < 10_000.0);
    assert!(engine.ledger().total_equity() <= 10_000.0);
}

#[test]
fn engine_market_buy_sweeps_multiple_ask_levels_with_weighted_price() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);

    engine
        .seed_order_book(
            "BTC/USDT",
            vec![[99.0, 3.0], [98.0, 4.0]],
            vec![[100.0, 1.0], [101.0, 2.0], [102.0, 5.0]],
            10,
            ts,
        )
        .expect("book should seed");

    let fill = engine
        .execute_market_order(
            "BTC/USDT",
            OrderSide::Buy,
            ts + Duration::milliseconds(1),
            100.0,
            1.5,
            MarketType::Futures,
            5.0,
            0.0,
        )
        .expect("market buy should sweep asks");

    assert_eq!(fill.quantity, 1.5);
    assert!((fill.price - 100.33333333333333).abs() < 1e-9);
    assert!(fill.fee_paid > 0.0);

    let book = engine
        .order_book("BTC/USDT")
        .expect("book should remain queryable");
    assert_eq!(book.best_ask(), Some([101.0, 1.5]));

    let position = engine
        .ledger()
        .position("BTC/USDT")
        .expect("position should exist");
    assert_eq!(position.quantity, 1.5);
}

#[test]
fn engine_market_sell_sweeps_multiple_bid_levels_with_weighted_price() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);

    engine
        .seed_order_book(
            "BTC/USDT",
            vec![[99.0, 1.0], [98.5, 2.0], [98.0, 4.0]],
            vec![[100.0, 3.0], [101.0, 4.0]],
            10,
            ts,
        )
        .expect("book should seed");

    let fill = engine
        .execute_market_order(
            "BTC/USDT",
            OrderSide::Sell,
            ts + Duration::milliseconds(1),
            99.0,
            2.5,
            MarketType::Futures,
            5.0,
            0.0,
        )
        .expect("market sell should sweep bids");

    assert_eq!(fill.quantity, 2.5);
    assert!((fill.price - 98.7).abs() < 1e-9);
    assert!(fill.fee_paid > 0.0);

    let book = engine
        .order_book("BTC/USDT")
        .expect("book should remain queryable");
    assert_eq!(book.best_bid(), Some([98.5, 0.5]));

    let position = engine
        .ledger()
        .position("BTC/USDT")
        .expect("position should exist");
    assert_eq!(position.quantity, -2.5);
}

#[test]
fn engine_limit_order_respects_queue_priority_and_partially_fills() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);

    engine
        .seed_order_book(
            "BTC/USDT",
            vec![[100.0, 2.0], [99.0, 3.0]],
            vec![[101.0, 4.0], [102.0, 2.0]],
            10,
            ts,
        )
        .expect("book should seed");

    let order_id = engine
        .place_limit_order(
            "BTC/USDT",
            OrderSide::Buy,
            100.0,
            2.0,
            MarketType::Futures,
            ts,
            ts + Duration::seconds(60),
        )
        .expect("limit order should be accepted");

    let order = engine.order(order_id).expect("order should exist");
    assert_eq!(order.queue_ahead_qty, 2.0);
    assert_eq!(order.remaining_quantity, 2.0);

    engine
        .apply_event(&BacktestEvent::DepthDelta(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts + Duration::seconds(1),
            logical_ts: ts + Duration::seconds(1),
            first_update_id: 11,
            final_update_id: 11,
            bids: vec![[100.0, 0.0]],
            asks: vec![],
        }))
        .expect("queue depletion should apply");

    let order = engine.order(order_id).expect("order should still exist");
    assert_eq!(order.queue_ahead_qty, 0.0);
    assert_eq!(order.remaining_quantity, 2.0);

    engine
        .apply_event(&BacktestEvent::DepthDelta(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts + Duration::seconds(2),
            logical_ts: ts + Duration::seconds(2),
            first_update_id: 12,
            final_update_id: 12,
            bids: vec![],
            asks: vec![[100.0, 1.0]],
        }))
        .expect("crossing ask should partially fill");

    let order = engine.order(order_id).expect("order should remain active");
    assert_eq!(order.remaining_quantity, 1.0);
    assert_eq!(order.filled_quantity(), 1.0);
    assert_eq!(engine.pending_limit_order_count(), 1);
    let position = engine
        .ledger()
        .position("BTC/USDT")
        .expect("position should exist");
    assert_eq!(position.quantity, 1.0);

    engine
        .apply_event(&BacktestEvent::DepthDelta(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts + Duration::seconds(3),
            logical_ts: ts + Duration::seconds(3),
            first_update_id: 13,
            final_update_id: 13,
            bids: vec![],
            asks: vec![[100.0, 2.0]],
        }))
        .expect("second crossing ask should fill remaining quantity");

    let order = engine
        .order(order_id)
        .expect("filled order should be queryable");
    assert!(order.is_terminal());
    assert_eq!(order.remaining_quantity, 0.0);
    assert_eq!(engine.pending_limit_order_count(), 0);
    let position = engine
        .ledger()
        .position("BTC/USDT")
        .expect("position should exist");
    assert_eq!(position.quantity, 2.0);
}

#[test]
fn engine_expires_unfilled_limit_order() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);

    engine
        .seed_order_book("BTC/USDT", vec![[100.0, 2.0]], vec![[101.0, 4.0]], 10, ts)
        .expect("book should seed");

    let order_id = engine
        .place_limit_order(
            "BTC/USDT",
            OrderSide::Sell,
            102.0,
            1.0,
            MarketType::Futures,
            ts,
            ts + Duration::seconds(5),
        )
        .expect("limit order should be accepted");

    engine.expire_orders(ts + Duration::seconds(6));

    let order = engine
        .order(order_id)
        .expect("order should remain queryable");
    assert!(order.is_terminal());
    assert_eq!(order.remaining_quantity, 1.0);
    assert!(engine.active_limit_orders("BTC/USDT").is_empty());
}

#[test]
fn engine_fills_passive_limit_order_from_trade_prints_without_depth_cross() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);

    engine
        .seed_order_book("BTC/USDT", vec![[99.0, 3.0]], vec![[101.0, 4.0]], 10, ts)
        .expect("book should seed");

    let order_id = engine
        .place_limit_order(
            "BTC/USDT",
            OrderSide::Buy,
            100.0,
            1.0,
            MarketType::Futures,
            ts,
            ts + Duration::seconds(30),
        )
        .expect("limit order should be accepted");

    let first_result = engine
        .apply_event(&BacktestEvent::Trade(TradeEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts + Duration::seconds(1),
            logical_ts: ts + Duration::seconds(1),
            trade: Trade {
                id: "t1".to_string(),
                symbol: "BTC/USDT".to_string(),
                side: OrderSide::Sell,
                amount: 0.4,
                price: 100.0,
                timestamp: ts + Duration::seconds(1),
                order_id: None,
                fee: None,
            },
        }))
        .expect("trade event should apply");

    assert_eq!(first_result.fills.len(), 1);
    assert_eq!(first_result.fills[0].fill.quantity, 0.4);
    assert!(!first_result.fills[0].is_terminal);

    let second_result = engine
        .apply_event(&BacktestEvent::Trade(TradeEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts + Duration::seconds(2),
            logical_ts: ts + Duration::seconds(2),
            trade: Trade {
                id: "t2".to_string(),
                symbol: "BTC/USDT".to_string(),
                side: OrderSide::Sell,
                amount: 0.7,
                price: 100.0,
                timestamp: ts + Duration::seconds(2),
                order_id: None,
                fee: None,
            },
        }))
        .expect("second trade event should apply");

    assert_eq!(second_result.fills.len(), 1);
    assert_eq!(second_result.fills[0].fill.quantity, 0.6);
    assert!(second_result.fills[0].is_terminal);
    assert_eq!(engine.pending_limit_order_count(), 0);

    let order = engine
        .order(order_id)
        .expect("filled order should remain queryable");
    assert!(order.is_terminal());
    assert_eq!(order.remaining_quantity, 0.0);

    let position = engine
        .ledger()
        .position("BTC/USDT")
        .expect("position should exist");
    assert_eq!(position.quantity, 1.0);
}
