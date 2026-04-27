use chrono::{TimeZone, Utc};
use rustcta::backtest::replay::MergedReplay;
use rustcta::backtest::schema::{
    BacktestEvent, BookTickerEvent, DepthDeltaEvent, FundingRateEvent, MarkPriceEvent, TradeEvent,
};
use rustcta::core::types::{MarketType, OrderSide, Trade};

#[test]
fn merged_replay_orders_events_by_time_then_type_priority() {
    let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();

    let trade = BacktestEvent::Trade(TradeEvent {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        exchange_ts: ts,
        logical_ts: ts,
        trade: Trade {
            id: "t1".to_string(),
            symbol: "BTC/USDT".to_string(),
            side: OrderSide::Buy,
            amount: 0.1,
            price: 100.0,
            timestamp: ts,
            order_id: None,
            fee: None,
        },
    });
    let depth = BacktestEvent::DepthDelta(DepthDeltaEvent {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        exchange_ts: ts,
        logical_ts: ts,
        first_update_id: 11,
        final_update_id: 11,
        bids: vec![[100.0, 2.0]],
        asks: vec![[101.0, 3.0]],
    });
    let ticker = BacktestEvent::BookTicker(BookTickerEvent {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        exchange_ts: ts,
        logical_ts: ts,
        best_bid: 100.0,
        best_bid_qty: 2.0,
        best_ask: 101.0,
        best_ask_qty: 3.0,
    });

    let merged = MergedReplay::from_streams(vec![vec![trade], vec![ticker], vec![depth]]);
    let kinds = merged
        .collect_events()
        .into_iter()
        .map(|event| match event {
            BacktestEvent::DepthDelta(_) => "depth",
            BacktestEvent::BookTicker(_) => "ticker",
            BacktestEvent::Trade(_) => "trade",
            _ => "other",
        })
        .collect::<Vec<_>>();

    assert_eq!(kinds, vec!["depth", "ticker", "trade"]);
}

#[test]
fn merged_replay_keeps_later_timestamps_after_earlier_ones() {
    let ts1 = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
    let ts2 = Utc.timestamp_millis_opt(1_711_929_700_000).unwrap();

    let funding = BacktestEvent::FundingRate(FundingRateEvent {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        exchange_ts: ts2,
        logical_ts: ts2,
        funding_rate: 0.0001,
        mark_price: Some(105.0),
    });
    let mark = BacktestEvent::MarkPrice(MarkPriceEvent {
        exchange: "binance".to_string(),
        market: "futures".to_string(),
        symbol: "BTC/USDT".to_string(),
        exchange_ts: ts1,
        logical_ts: ts1,
        mark_price: 100.0,
        index_price: 99.5,
        funding_rate: Some(0.0001),
        next_funding_time: None,
        market_type: MarketType::Futures,
    });

    let merged = MergedReplay::from_streams(vec![vec![funding], vec![mark]]);
    let events = merged.collect_events();

    assert_eq!(events.len(), 2);
    assert!(events[0].logical_ts() < events[1].logical_ts());
}
