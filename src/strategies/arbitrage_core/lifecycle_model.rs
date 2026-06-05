use serde::{Deserialize, Serialize};

use crate::exchanges::unified::OrderSide;

use super::executable_price::ExecutablePriceResult;
use super::relationship::ArbitrageRelationshipType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TakerTakerEntryAnalysis {
    pub raw_executable_spread_bps: f64,
    pub buy_taker_fee_bps: f64,
    pub sell_taker_fee_bps: f64,
    pub entry_taker_fee_bps: f64,
    pub buy_slippage_bps: f64,
    pub sell_slippage_bps: f64,
    pub latency_buffer_bps: f64,
    pub residual_risk_buffer_bps: f64,
    pub immediate_net_bps: f64,
    pub immediate_gross_pnl: f64,
    pub immediate_net_pnl: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleModelConfig {
    pub expected_convergence_bps: f64,
    pub expected_exit_basis_bps: f64,
    pub expected_exit_spread_bps: f64,
    pub expected_funding_net_bps: f64,
    pub expected_exit_fee_bps: f64,
    pub expected_exit_slippage_bps: f64,
    pub expected_inventory_rebalance_cost_bps: f64,
    pub expected_transfer_cost_bps: f64,
    pub expected_inventory_risk_bps: f64,
    pub expected_residual_loss_bps: f64,
    pub margin_risk_cost_bps: f64,
    pub safety_buffer_bps: f64,
    pub expected_holding_seconds: u64,
}

impl Default for LifecycleModelConfig {
    fn default() -> Self {
        Self {
            expected_convergence_bps: 0.0,
            expected_exit_basis_bps: 0.0,
            expected_exit_spread_bps: 0.0,
            expected_funding_net_bps: 0.0,
            expected_exit_fee_bps: 20.0,
            expected_exit_slippage_bps: 5.0,
            expected_inventory_rebalance_cost_bps: 5.0,
            expected_transfer_cost_bps: 0.0,
            expected_inventory_risk_bps: 5.0,
            expected_residual_loss_bps: 5.0,
            margin_risk_cost_bps: 0.0,
            safety_buffer_bps: 5.0,
            expected_holding_seconds: 3_600,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleAnalysis {
    pub relationship_type: ArbitrageRelationshipType,
    pub expected_lifecycle_net_bps: f64,
    pub expected_lifecycle_net_pnl: f64,
    pub expected_convergence_bps: f64,
    pub expected_funding_net_bps: f64,
    pub expected_exit_fee_bps: f64,
    pub expected_exit_slippage_bps: f64,
    pub expected_inventory_rebalance_cost_bps: f64,
    pub expected_transfer_cost_bps: f64,
    pub expected_inventory_risk_bps: f64,
    pub expected_residual_loss_bps: f64,
    pub margin_risk_cost_bps: f64,
    pub safety_buffer_bps: f64,
    pub expected_holding_seconds: u64,
    pub notes: Vec<String>,
}

pub fn analyze_taker_taker_entry(
    buy: &ExecutablePriceResult,
    sell: &ExecutablePriceResult,
    buy_taker_fee_bps: f64,
    sell_taker_fee_bps: f64,
    latency_buffer_bps: f64,
    residual_risk_buffer_bps: f64,
) -> TakerTakerEntryAnalysis {
    let buy_vwap = buy.vwap.unwrap_or(0.0);
    let sell_vwap = sell.vwap.unwrap_or(0.0);
    let raw_executable_spread_bps = if buy_vwap > 0.0 {
        (sell_vwap - buy_vwap) / buy_vwap * 10_000.0
    } else {
        0.0
    };
    let entry_taker_fee_bps = buy_taker_fee_bps + sell_taker_fee_bps;
    let immediate_net_bps = raw_executable_spread_bps
        - entry_taker_fee_bps
        - buy.slippage_bps
        - sell.slippage_bps
        - latency_buffer_bps
        - residual_risk_buffer_bps;
    let quantity = buy.executable_quantity.min(sell.executable_quantity);
    let immediate_gross_pnl = (sell_vwap - buy_vwap) * quantity;
    let immediate_net_pnl =
        immediate_gross_pnl - buy.executable_notional * entry_taker_fee_bps / 10_000.0;
    TakerTakerEntryAnalysis {
        raw_executable_spread_bps,
        buy_taker_fee_bps,
        sell_taker_fee_bps,
        entry_taker_fee_bps,
        buy_slippage_bps: buy.slippage_bps,
        sell_slippage_bps: sell.slippage_bps,
        latency_buffer_bps,
        residual_risk_buffer_bps,
        immediate_net_bps,
        immediate_gross_pnl,
        immediate_net_pnl,
    }
}

pub fn analyze_lifecycle(
    relationship_type: ArbitrageRelationshipType,
    entry: &TakerTakerEntryAnalysis,
    notional: f64,
    config: &LifecycleModelConfig,
) -> LifecycleAnalysis {
    let (expected_lifecycle_net_bps, notes) = match relationship_type {
        ArbitrageRelationshipType::SpotSpot => (
            entry.immediate_net_bps
                - config.expected_inventory_rebalance_cost_bps
                - config.expected_transfer_cost_bps
                - config.expected_inventory_risk_bps
                - config.expected_residual_loss_bps
                - config.safety_buffer_bps,
            vec![
                "spot_spot_inventory_moves_between exchanges".to_string(),
                "reverse spread or transfer may be required for rebalance".to_string(),
            ],
        ),
        ArbitrageRelationshipType::SpotPerp => (
            config.expected_convergence_bps + config.expected_funding_net_bps
                - entry.entry_taker_fee_bps
                - config.expected_exit_fee_bps
                - entry.buy_slippage_bps
                - entry.sell_slippage_bps
                - config.expected_exit_slippage_bps
                - config.margin_risk_cost_bps
                - config.expected_residual_loss_bps
                - config.safety_buffer_bps,
            vec!["spot_perp entry basis is not immediately realized profit".to_string()],
        ),
        ArbitrageRelationshipType::PerpPerp => (
            config.expected_convergence_bps + config.expected_funding_net_bps
                - entry.entry_taker_fee_bps
                - config.expected_exit_fee_bps
                - entry.buy_slippage_bps
                - entry.sell_slippage_bps
                - config.expected_exit_slippage_bps
                - config.margin_risk_cost_bps
                - config.expected_residual_loss_bps
                - config.safety_buffer_bps,
            vec!["perp_perp lifecycle includes four fee legs and funding difference".to_string()],
        ),
    };
    LifecycleAnalysis {
        relationship_type,
        expected_lifecycle_net_bps,
        expected_lifecycle_net_pnl: notional * expected_lifecycle_net_bps / 10_000.0,
        expected_convergence_bps: config.expected_convergence_bps,
        expected_funding_net_bps: config.expected_funding_net_bps,
        expected_exit_fee_bps: config.expected_exit_fee_bps,
        expected_exit_slippage_bps: config.expected_exit_slippage_bps,
        expected_inventory_rebalance_cost_bps: config.expected_inventory_rebalance_cost_bps,
        expected_transfer_cost_bps: config.expected_transfer_cost_bps,
        expected_inventory_risk_bps: config.expected_inventory_risk_bps,
        expected_residual_loss_bps: config.expected_residual_loss_bps,
        margin_risk_cost_bps: config.margin_risk_cost_bps,
        safety_buffer_bps: config.safety_buffer_bps,
        expected_holding_seconds: config.expected_holding_seconds,
        notes,
    }
}

pub fn basis_bps(perp_sell_vwap: f64, spot_buy_vwap: f64) -> f64 {
    if spot_buy_vwap > 0.0 {
        (perp_sell_vwap - spot_buy_vwap) / spot_buy_vwap * 10_000.0
    } else {
        0.0
    }
}

pub fn leg_side_label(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}
