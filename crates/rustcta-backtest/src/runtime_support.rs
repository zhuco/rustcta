use anyhow::{anyhow, Result};
use chrono::Duration;

use self::dto::{MarketType, OrderSide};

pub fn positive_constraint(value: Option<f64>) -> Option<f64> {
    value.filter(|constraint| constraint.is_finite() && *constraint > f64::EPSILON)
}

pub fn round_limit_price(price: f64, tick_size: f64, side: OrderSide) -> f64 {
    let ticks = price / tick_size;
    match side {
        OrderSide::Buy => ticks.floor() * tick_size,
        OrderSide::Sell => ticks.ceil() * tick_size,
    }
}

pub fn round_down_to_step(quantity: f64, step_size: f64) -> f64 {
    (quantity / step_size).floor() * step_size
}

pub fn approx_equal(lhs: f64, rhs: f64) -> bool {
    (lhs - rhs).abs() <= 1e-9
}

pub fn latency_duration_ms(latency_ms: u64) -> Duration {
    Duration::milliseconds(latency_ms.min(i64::MAX as u64) as i64)
}

pub fn market_type_from_string(market: &str) -> Result<MarketType> {
    match market {
        "futures" => Ok(MarketType::Futures),
        "spot" => Ok(MarketType::Spot),
        other => Err(anyhow!("unsupported market {}", other)),
    }
}

pub fn opposite_side(side: OrderSide) -> OrderSide {
    match side {
        OrderSide::Buy => OrderSide::Sell,
        OrderSide::Sell => OrderSide::Buy,
    }
}

pub fn quote_asset_from_symbol(symbol: &str) -> Option<String> {
    let trimmed = symbol.trim();
    if let Some((_, quote)) = trimmed.rsplit_once('/') {
        let quote = quote.trim();
        if !quote.is_empty() {
            return Some(quote.to_ascii_uppercase());
        }
    }

    let upper = trimmed.to_ascii_uppercase();
    ["USDT", "USDC", "FDUSD", "BUSD", "USD", "BTC", "ETH"]
        .into_iter()
        .find(|quote| upper.ends_with(quote) && upper.len() > quote.len())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::{
        approx_equal, latency_duration_ms, market_type_from_string, opposite_side,
        positive_constraint, quote_asset_from_symbol, round_down_to_step, round_limit_price,
    };
    use super::dto::{MarketType as BacktestMarketType, OrderSide as BacktestOrderSide};

    #[test]
    fn venue_numeric_helpers_match_legacy_runtime_behavior() {
        assert_eq!(positive_constraint(Some(0.01)), Some(0.01));
        assert_eq!(positive_constraint(Some(0.0)), None);
        assert_eq!(positive_constraint(Some(f64::NAN)), None);
        assert_eq!(
            round_limit_price(100.019, 0.01, BacktestOrderSide::Buy),
            100.01
        );
        assert_eq!(
            round_limit_price(100.011, 0.01, BacktestOrderSide::Sell),
            100.02
        );
        assert_eq!(round_down_to_step(1.239, 0.01), 1.23);
        assert!(approx_equal(1.0, 1.0 + 1e-10));
        assert!(!approx_equal(1.0, 1.0 + 1e-6));
    }

    #[test]
    fn runtime_market_and_side_helpers_are_root_free() {
        assert_eq!(
            market_type_from_string("futures").unwrap(),
            BacktestMarketType::Futures
        );
        assert_eq!(
            market_type_from_string("spot").unwrap(),
            BacktestMarketType::Spot
        );
        assert!(market_type_from_string("margin").is_err());
        assert_eq!(
            opposite_side(BacktestOrderSide::Buy),
            BacktestOrderSide::Sell
        );
        assert_eq!(
            opposite_side(BacktestOrderSide::Sell),
            BacktestOrderSide::Buy
        );
        assert_eq!(latency_duration_ms(25).num_milliseconds(), 25);
    }

    #[test]
    fn quote_asset_parsing_handles_common_backtest_symbols() {
        assert_eq!(
            quote_asset_from_symbol("BTC/USDT"),
            Some("USDT".to_string())
        );
        assert_eq!(quote_asset_from_symbol("ethusdc"), Some("USDC".to_string()));
        assert_eq!(
            quote_asset_from_symbol("SOLFDUSD"),
            Some("FDUSD".to_string())
        );
        assert_eq!(quote_asset_from_symbol("BTC"), None);
    }
}
