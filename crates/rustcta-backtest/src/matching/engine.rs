mod dto {
    pub use crate::schema::BacktestEvent;
    pub use crate::strategy::StrategySignal;
    pub use crate::types::{
        BacktestKline as Kline, BacktestMarketType as MarketType, BacktestOrderSide as OrderSide,
        BacktestOrderType as OrderType,
    };
}

include!("engine_impl.rs");

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};

    use super::{BacktestEngineState, LimitOrderStatus};
    use crate::schema::{BacktestEvent, DepthDeltaEvent, FundingRateEvent, MarkPriceEvent};
    use crate::strategy::StrategySignal;
    use crate::types::{BacktestMarketType, BacktestOrderSide, BacktestOrderType};

    #[test]
    fn engine_should_update_order_book_from_depth_events() {
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
    fn engine_should_update_ledger_from_mark_and_funding_events() {
        let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
        let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);

        engine
            .ledger_mut()
            .apply_fill(crate::matching::ledger::FillResult {
                symbol: "BTC/USDT".to_string(),
                side: BacktestOrderSide::Buy,
                order_type: BacktestOrderType::Limit,
                market_type: BacktestMarketType::Futures,
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
                market_type: BacktestMarketType::Futures,
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
    fn engine_should_execute_market_signal_into_ledger() {
        let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
        let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);
        engine
            .seed_order_book(
                "BTC/USDT",
                vec![[99.0, 1.0], [98.0, 1.0]],
                vec![[101.0, 0.5], [102.0, 0.5]],
                1,
                ts,
            )
            .expect("book should seed");

        let signal = StrategySignal {
            strategy: "unit".to_string(),
            symbol: "BTC/USDT".to_string(),
            side: BacktestOrderSide::Buy,
            logical_ts: ts,
            price: 100.0,
            band_percent: 0.0,
            z_score: 0.0,
            rsi: 50.0,
            atr: 1.0,
            reason: "test".to_string(),
        };

        let fill = engine
            .execute_market_signal(&signal, BacktestMarketType::Futures, 100.0, 5.0)
            .expect("market signal should fill");

        assert_eq!(fill.order_type, BacktestOrderType::Market);
        assert!(fill.price >= 100.0);
        assert!(engine.ledger().position("BTC/USDT").is_some());
    }

    #[test]
    fn engine_should_expire_unfilled_limit_order() {
        let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
        let mut engine = BacktestEngineState::new("USDT", 10_000.0, 10);
        let order_id = engine
            .place_limit_order(
                "BTC/USDT",
                BacktestOrderSide::Buy,
                90.0,
                1.0,
                BacktestMarketType::Futures,
                ts,
                ts + Duration::seconds(60),
            )
            .expect("limit should place");

        let expired = engine.expire_orders(ts + Duration::seconds(61));

        assert_eq!(expired, vec![order_id]);
        assert_eq!(
            engine.order(order_id).expect("order should exist").status,
            LimitOrderStatus::Expired
        );
    }
}
