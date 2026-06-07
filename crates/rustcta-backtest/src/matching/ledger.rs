mod dto {
    pub use crate::types::{
        BacktestMarketType as MarketType, BacktestOrderSide as OrderSide,
        BacktestOrderType as OrderType,
    };
}

include!("ledger_impl.rs");

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::{BacktestLedger, FillResult, FundingSettlement};
    use crate::types::{BacktestMarketType, BacktestOrderSide, BacktestOrderType};

    #[test]
    fn ledger_should_update_equity_after_fill_and_funding() {
        let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
        let mut ledger = BacktestLedger::new("USDT", 10_000.0);

        ledger
            .apply_fill(FillResult {
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
    fn ledger_should_realize_pnl_when_futures_position_is_closed() {
        let ts = Utc.timestamp_millis_opt(1_711_929_600_000).unwrap();
        let mut ledger = BacktestLedger::new("USDT", 10_000.0);

        ledger
            .apply_fill(FillResult {
                symbol: "BTC/USDT".to_string(),
                side: BacktestOrderSide::Buy,
                order_type: BacktestOrderType::Market,
                market_type: BacktestMarketType::Futures,
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
                side: BacktestOrderSide::Sell,
                order_type: BacktestOrderType::Market,
                market_type: BacktestMarketType::Futures,
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
}
