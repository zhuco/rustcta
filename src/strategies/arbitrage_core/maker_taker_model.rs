use serde::{Deserialize, Serialize};

use crate::exchanges::unified::OrderSide;

use super::executable_price::ExecutablePriceResult;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MakerTakerExpectedValueModel {
    pub fill_probability: f64,
    pub expected_time_to_fill_ms: u64,
    pub expected_spread_at_fill_bps: f64,
    pub expected_hedge_slippage_bps: f64,
    pub expected_adverse_selection_bps: f64,
    pub expected_residual_loss_bps: f64,
    pub expected_cancel_replace_cost_bps: f64,
    pub expected_inventory_risk_bps: f64,
    pub estimated: bool,
}

impl Default for MakerTakerExpectedValueModel {
    fn default() -> Self {
        Self {
            fill_probability: 0.1,
            expected_time_to_fill_ms: 1_000,
            expected_spread_at_fill_bps: 0.0,
            expected_hedge_slippage_bps: 5.0,
            expected_adverse_selection_bps: 10.0,
            expected_residual_loss_bps: 5.0,
            expected_cancel_replace_cost_bps: 1.0,
            expected_inventory_risk_bps: 5.0,
            estimated: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MakerTakerAnalysis {
    pub mode: String,
    pub passive_side: OrderSide,
    pub maker_price: f64,
    pub hedge_taker_vwap: f64,
    pub theoretical_spread_bps: f64,
    pub maker_fee_bps: f64,
    pub hedge_taker_fee_bps: f64,
    pub theoretical_net_bps: f64,
    pub expected_net_bps: f64,
    pub expected_net_pnl: f64,
    pub non_executable_theoretical: bool,
    pub estimated: bool,
}

pub fn maker_buy_taker_sell(
    maker_buy_price: f64,
    hedge_sell: &ExecutablePriceResult,
    maker_fee_bps: f64,
    hedge_taker_fee_bps: f64,
    ev: &MakerTakerExpectedValueModel,
) -> MakerTakerAnalysis {
    let hedge_vwap = hedge_sell.vwap.unwrap_or(0.0);
    let theoretical_spread_bps = if maker_buy_price > 0.0 {
        (hedge_vwap - maker_buy_price) / maker_buy_price * 10_000.0
    } else {
        0.0
    };
    maker_taker_analysis(
        "maker_buy_taker_sell",
        OrderSide::Buy,
        maker_buy_price,
        hedge_vwap,
        theoretical_spread_bps,
        maker_fee_bps,
        hedge_taker_fee_bps,
        hedge_sell.executable_notional,
        ev,
    )
}

pub fn taker_buy_maker_sell(
    buy_taker: &ExecutablePriceResult,
    maker_sell_price: f64,
    maker_fee_bps: f64,
    hedge_taker_fee_bps: f64,
    ev: &MakerTakerExpectedValueModel,
) -> MakerTakerAnalysis {
    let buy_vwap = buy_taker.vwap.unwrap_or(0.0);
    let theoretical_spread_bps = if buy_vwap > 0.0 {
        (maker_sell_price - buy_vwap) / buy_vwap * 10_000.0
    } else {
        0.0
    };
    maker_taker_analysis(
        "taker_buy_maker_sell",
        OrderSide::Sell,
        maker_sell_price,
        buy_vwap,
        theoretical_spread_bps,
        maker_fee_bps,
        hedge_taker_fee_bps,
        buy_taker.executable_notional,
        ev,
    )
}

fn maker_taker_analysis(
    mode: &str,
    passive_side: OrderSide,
    maker_price: f64,
    hedge_taker_vwap: f64,
    theoretical_spread_bps: f64,
    maker_fee_bps: f64,
    hedge_taker_fee_bps: f64,
    notional: f64,
    ev: &MakerTakerExpectedValueModel,
) -> MakerTakerAnalysis {
    let theoretical_net_bps = theoretical_spread_bps
        - maker_fee_bps
        - hedge_taker_fee_bps
        - ev.expected_hedge_slippage_bps;
    let expected_net_bps = ev.fill_probability
        * (ev.expected_spread_at_fill_bps
            - maker_fee_bps
            - hedge_taker_fee_bps
            - ev.expected_hedge_slippage_bps
            - ev.expected_adverse_selection_bps
            - ev.expected_residual_loss_bps)
        - ev.expected_cancel_replace_cost_bps
        - ev.expected_inventory_risk_bps;
    MakerTakerAnalysis {
        mode: mode.to_string(),
        passive_side,
        maker_price,
        hedge_taker_vwap,
        theoretical_spread_bps,
        maker_fee_bps,
        hedge_taker_fee_bps,
        theoretical_net_bps,
        expected_net_bps,
        expected_net_pnl: notional * expected_net_bps / 10_000.0,
        non_executable_theoretical: true,
        estimated: ev.estimated,
    }
}
