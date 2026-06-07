mod dto {
    pub use rustcta_types::{
        BacktestKline as Kline, BacktestMarketType as MarketType, BacktestTrade as Trade,
    };
}

include!("schema_impl.rs");

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::types::{BacktestMarketType, BacktestOrderSide, BacktestTrade};

    fn ts() -> chrono::DateTime<Utc> {
        Utc.timestamp_millis_opt(1_711_929_600_000).unwrap()
    }

    #[test]
    fn stable_priority_orders_depth_before_trade_before_kline() {
        let depth = BacktestEvent::DepthDelta(DepthDeltaEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts(),
            logical_ts: ts(),
            first_update_id: 1,
            final_update_id: 1,
            bids: vec![],
            asks: vec![],
        });
        let trade = BacktestEvent::Trade(TradeEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts(),
            logical_ts: ts(),
            trade: BacktestTrade {
                id: "t1".to_string(),
                symbol: "BTC/USDT".to_string(),
                side: BacktestOrderSide::Buy,
                amount: 1.0,
                price: 100.0,
                timestamp: ts(),
                order_id: None,
                fee: None,
            },
        });

        assert!(depth.stable_priority() < trade.stable_priority());
        assert_eq!(trade.logical_ts(), ts());
    }

    #[test]
    fn market_type_preserves_legacy_pascal_case_json() {
        let event = MarkPriceEvent {
            exchange: "binance".to_string(),
            market: "futures".to_string(),
            symbol: "BTC/USDT".to_string(),
            exchange_ts: ts(),
            logical_ts: ts(),
            mark_price: 100.0,
            index_price: 99.0,
            funding_rate: Some(0.0001),
            next_funding_time: None,
            market_type: BacktestMarketType::Futures,
        };

        let value = serde_json::to_value(event).expect("serialize mark price event");
        assert_eq!(value["market_type"], "Futures");
    }
}
