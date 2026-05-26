//! Pure simulation helpers for maker fills, taker VWAP, and lock-profit closes.

use super::config::CrossExchangeArbitrageConfig;
use super::fees::{FeeModel, FeeRole};
use super::state::{FillInferenceType, OrderSide};
use crate::market::{
    calculate_taker_vwap as calculate_market_taker_vwap, BookLevel, ExchangeId, OrderBook5,
    TakerSide,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TakerVwapResult {
    pub side: OrderSide,
    pub target_notional_usdt: f64,
    pub filled_qty: f64,
    pub filled_notional_usdt: f64,
    pub vwap_price: Option<f64>,
    pub levels_used: usize,
    pub depth_enough: bool,
    pub slippage_pct: f64,
}

pub fn calculate_taker_vwap(
    book: &OrderBook5,
    side: OrderSide,
    target_notional_usdt: f64,
) -> TakerVwapResult {
    let market_side = match side {
        OrderSide::Buy => TakerSide::Buy,
        OrderSide::Sell => TakerSide::Sell,
    };
    let vwap = calculate_market_taker_vwap(book, market_side, target_notional_usdt);

    TakerVwapResult {
        side,
        target_notional_usdt,
        filled_qty: vwap.filled_qty,
        filled_notional_usdt: vwap.filled_notional,
        vwap_price: vwap.vwap_price,
        levels_used: vwap.levels_used,
        depth_enough: vwap.depth_enough,
        slippage_pct: vwap.slippage_pct.unwrap_or(0.0),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MakerFillInput {
    pub side: OrderSide,
    pub maker_price: f64,
    pub later_best_bid: Option<f64>,
    pub later_best_ask: Option<f64>,
    pub last_trade_price: Option<f64>,
    pub elapsed_ms: i64,
    pub max_wait_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MakerFillSimulation {
    pub maker_filled: bool,
    pub maker_fill_price: Option<f64>,
    pub maker_wait_ms: i64,
    pub fill_inference_type: FillInferenceType,
    pub maker_queue_risk: f64,
}

pub fn infer_maker_fill(input: &MakerFillInput) -> MakerFillSimulation {
    if input.elapsed_ms > input.max_wait_ms {
        return MakerFillSimulation {
            maker_filled: false,
            maker_fill_price: None,
            maker_wait_ms: input.elapsed_ms,
            fill_inference_type: FillInferenceType::TimedOut,
            maker_queue_risk: 1.0,
        };
    }

    let trade_filled = match (input.side, input.last_trade_price) {
        (OrderSide::Sell, Some(price)) => price >= input.maker_price,
        (OrderSide::Buy, Some(price)) => price <= input.maker_price,
        _ => false,
    };
    let book_filled = match input.side {
        OrderSide::Sell => input
            .later_best_ask
            .map(|best_ask| best_ask <= input.maker_price)
            .unwrap_or(false),
        OrderSide::Buy => input
            .later_best_bid
            .map(|best_bid| best_bid >= input.maker_price)
            .unwrap_or(false),
    };

    let maker_filled = trade_filled || book_filled;
    MakerFillSimulation {
        maker_filled,
        maker_fill_price: maker_filled.then_some(input.maker_price),
        maker_wait_ms: input.elapsed_ms,
        fill_inference_type: if trade_filled {
            FillInferenceType::RealTrade
        } else if book_filled {
            FillInferenceType::BookInferredFill
        } else {
            FillInferenceType::NotFilled
        },
        maker_queue_risk: if maker_filled { 0.25 } else { 0.75 },
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DualTakerCloseInput {
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    pub long_book: OrderBook5,
    pub short_book: OrderBook5,
    pub target_notional_usdt: f64,
    pub initial_bundle_notional_usdt: f64,
    pub gross_spread_pnl_usdt: f64,
    pub realized_funding_pnl_usdt: f64,
    pub open_fee_paid_usdt: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DualTakerCloseDecision {
    pub long_close: TakerVwapResult,
    pub short_close: TakerVwapResult,
    pub dual_taker_close_fee_est_usdt: f64,
    pub dual_taker_close_profit_pct: f64,
    pub should_lock_profit: bool,
    pub strongly_should_lock_profit: bool,
    pub depth_enough: bool,
}

pub fn evaluate_dual_taker_lock_profit(
    input: &DualTakerCloseInput,
    config: &CrossExchangeArbitrageConfig,
    fee_model: &FeeModel,
) -> DualTakerCloseDecision {
    let long_close = calculate_taker_vwap(
        &input.long_book,
        OrderSide::Sell,
        input.target_notional_usdt,
    );
    let short_close = calculate_taker_vwap(
        &input.short_book,
        OrderSide::Buy,
        input.target_notional_usdt,
    );
    let dual_taker_close_fee_est_usdt = fee_model.fee_amount(
        &input.long_exchange,
        FeeRole::Taker,
        input.target_notional_usdt,
    ) + fee_model.fee_amount(
        &input.short_exchange,
        FeeRole::Taker,
        input.target_notional_usdt,
    );
    let depth_enough = long_close.depth_enough && short_close.depth_enough;
    let denominator = input.initial_bundle_notional_usdt.max(1.0);
    let dual_taker_close_profit_pct = (input.gross_spread_pnl_usdt
        + input.realized_funding_pnl_usdt
        - input.open_fee_paid_usdt
        - dual_taker_close_fee_est_usdt
        - config.risk.taker_slippage_buffer * input.target_notional_usdt
        - config.risk.safety_buffer * input.target_notional_usdt)
        / denominator;

    DualTakerCloseDecision {
        long_close,
        short_close,
        dual_taker_close_fee_est_usdt,
        dual_taker_close_profit_pct,
        should_lock_profit: depth_enough
            && dual_taker_close_profit_pct >= config.thresholds.lock_profit_dual_taker_pct,
        strongly_should_lock_profit: depth_enough
            && dual_taker_close_profit_pct >= config.thresholds.strong_lock_profit_dual_taker_pct,
        depth_enough,
    }
}

pub fn levels(depth: &[(f64, f64)]) -> Vec<BookLevel> {
    depth
        .iter()
        .map(|(price, qty)| BookLevel::new(*price, *qty))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{CanonicalSymbol, ExchangeSymbol};
    use chrono::Utc;

    fn book(exchange: ExchangeId, bid: f64, ask: f64, qty: f64) -> OrderBook5 {
        OrderBook5::new(
            exchange.clone(),
            CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(exchange.clone(), format!("{}BTCUSDT", exchange.as_str())),
            vec![
                BookLevel::new(bid, qty),
                BookLevel::new(bid - 1.0, qty),
                BookLevel::new(bid - 2.0, qty),
            ],
            vec![
                BookLevel::new(ask, qty),
                BookLevel::new(ask + 1.0, qty),
                BookLevel::new(ask + 2.0, qty),
            ],
            Utc::now(),
            Utc::now(),
            Some(1),
            Some("test".to_string()),
        )
    }

    #[test]
    fn cross_exchange_arbitrage_vwap_should_buy_from_asks() {
        let book = book(ExchangeId::Binance, 99.0, 100.0, 1.0);
        let result = calculate_taker_vwap(&book, OrderSide::Buy, 150.5);

        assert!(result.depth_enough);
        assert_eq!(result.levels_used, 2);
        assert!(result.vwap_price.unwrap() > 100.0);
    }

    #[test]
    fn cross_exchange_arbitrage_vwap_should_sell_to_bids() {
        let book = book(ExchangeId::Binance, 100.0, 101.0, 1.0);
        let result = calculate_taker_vwap(&book, OrderSide::Sell, 150.0);

        assert!(result.depth_enough);
        assert_eq!(result.levels_used, 2);
        assert!(result.vwap_price.unwrap() < 100.0);
    }

    #[test]
    fn cross_exchange_arbitrage_vwap_should_reject_insufficient_depth() {
        let book = book(ExchangeId::Binance, 100.0, 101.0, 0.1);
        let result = calculate_taker_vwap(&book, OrderSide::Buy, 1_000.0);

        assert!(!result.depth_enough);
        assert!(result.filled_notional_usdt < 1_000.0);
    }

    #[test]
    fn cross_exchange_arbitrage_simulation_should_trigger_dual_taker_lock_profit() {
        let config = CrossExchangeArbitrageConfig::default();
        let fee_model = FeeModel::default();
        let decision = evaluate_dual_taker_lock_profit(
            &DualTakerCloseInput {
                long_exchange: ExchangeId::Binance,
                short_exchange: ExchangeId::Okx,
                long_book: book(ExchangeId::Binance, 104.0, 105.0, 10.0),
                short_book: book(ExchangeId::Okx, 106.0, 107.0, 10.0),
                target_notional_usdt: 100.0,
                initial_bundle_notional_usdt: 100.0,
                gross_spread_pnl_usdt: 1.2,
                realized_funding_pnl_usdt: 0.0,
                open_fee_paid_usdt: 0.07,
            },
            &config,
            &fee_model,
        );

        assert!(decision.depth_enough);
        assert!(decision.should_lock_profit);
        assert!(decision.strongly_should_lock_profit);
    }
}
