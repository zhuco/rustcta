use serde::{Deserialize, Serialize};

use super::{BookLevel, OrderBook5};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TakerSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TakerVwap {
    pub side: TakerSide,
    pub target_notional: f64,
    pub filled_qty: f64,
    pub filled_notional: f64,
    pub vwap_price: Option<f64>,
    pub levels_used: usize,
    pub depth_enough: bool,
    pub slippage_pct: Option<f64>,
    pub best_price: Option<f64>,
}

impl TakerVwap {
    pub fn empty(side: TakerSide, target_notional: f64) -> Self {
        Self {
            side,
            target_notional,
            filled_qty: 0.0,
            filled_notional: 0.0,
            vwap_price: None,
            levels_used: 0,
            depth_enough: false,
            slippage_pct: None,
            best_price: None,
        }
    }
}

pub fn calculate_taker_vwap(book: &OrderBook5, side: TakerSide, target_notional: f64) -> TakerVwap {
    if target_notional <= 0.0 || !target_notional.is_finite() || !book.is_usable() {
        return TakerVwap::empty(side, target_notional);
    }

    let levels = match side {
        TakerSide::Buy => &book.asks,
        TakerSide::Sell => &book.bids,
    };
    calculate_from_levels(levels, side, target_notional)
}

pub fn calculate_from_levels(
    levels: &[BookLevel],
    side: TakerSide,
    target_notional: f64,
) -> TakerVwap {
    if target_notional <= 0.0 || !target_notional.is_finite() {
        return TakerVwap::empty(side, target_notional);
    }

    let best_price = levels
        .first()
        .filter(|level| level.is_valid())
        .map(|level| level.price);
    let mut filled_qty = 0.0;
    let mut filled_notional = 0.0;
    let mut levels_used = 0;

    for level in levels.iter().filter(|level| level.is_valid()) {
        if filled_notional >= target_notional {
            break;
        }

        let level_notional = level.price * level.quantity;
        let remaining_notional = target_notional - filled_notional;
        let take_notional = level_notional.min(remaining_notional);
        if take_notional <= 0.0 {
            continue;
        }

        filled_notional += take_notional;
        filled_qty += take_notional / level.price;
        levels_used += 1;
    }

    let depth_enough = filled_notional + f64::EPSILON >= target_notional;
    let vwap_price = if filled_qty > 0.0 {
        Some(filled_notional / filled_qty)
    } else {
        None
    };
    let slippage_pct = match (side, best_price, vwap_price) {
        (TakerSide::Buy, Some(best), Some(vwap)) if best > 0.0 => Some(vwap / best - 1.0),
        (TakerSide::Sell, Some(best), Some(vwap)) if best > 0.0 => Some(1.0 - vwap / best),
        _ => None,
    };

    TakerVwap {
        side,
        target_notional,
        filled_qty,
        filled_notional,
        vwap_price,
        levels_used,
        depth_enough,
        slippage_pct,
        best_price,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::market::{CanonicalSymbol, ExchangeId, ExchangeSymbol};

    fn book(bids: Vec<BookLevel>, asks: Vec<BookLevel>) -> OrderBook5 {
        let exchange = ExchangeId::Binance;
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("btc", "usdt"),
            ExchangeSymbol::new(exchange, "BTCUSDT"),
            bids,
            asks,
            Utc::now(),
            Utc::now(),
            Some(1),
            Some("primary".to_string()),
        )
    }

    #[test]
    fn buy_vwap_should_consume_asks() {
        let book = book(
            vec![BookLevel::new(99.0, 10.0)],
            vec![BookLevel::new(100.0, 1.0), BookLevel::new(101.0, 1.0)],
        );

        let vwap = calculate_taker_vwap(&book, TakerSide::Buy, 150.0);

        assert!(vwap.depth_enough);
        assert_eq!(vwap.levels_used, 2);
        assert_eq!(vwap.filled_notional, 150.0);
        assert!(vwap.vwap_price.expect("vwap") > 100.0);
    }

    #[test]
    fn sell_vwap_should_consume_bids() {
        let book = book(
            vec![BookLevel::new(100.0, 1.0), BookLevel::new(99.0, 1.0)],
            vec![BookLevel::new(101.0, 10.0)],
        );

        let vwap = calculate_taker_vwap(&book, TakerSide::Sell, 150.0);

        assert!(vwap.depth_enough);
        assert_eq!(vwap.levels_used, 2);
        assert!(vwap.vwap_price.expect("vwap") < 100.0);
    }

    #[test]
    fn insufficient_depth_should_not_fake_full_fill() {
        let book = book(
            vec![BookLevel::new(100.0, 1.0)],
            vec![BookLevel::new(101.0, 1.0)],
        );

        let vwap = calculate_taker_vwap(&book, TakerSide::Buy, 200.0);

        assert!(!vwap.depth_enough);
        assert_eq!(vwap.filled_notional, 101.0);
    }

    #[test]
    fn unusable_book_should_return_empty_vwap() {
        let crossed = book(
            vec![BookLevel::new(102.0, 1.0)],
            vec![BookLevel::new(101.0, 1.0)],
        );

        let vwap = calculate_taker_vwap(&crossed, TakerSide::Buy, 100.0);

        assert!(!vwap.depth_enough);
        assert_eq!(vwap.filled_notional, 0.0);
    }
}
