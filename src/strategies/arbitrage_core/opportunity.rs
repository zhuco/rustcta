use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::MarketType;
use crate::execution::FeeSource;

use super::capital_model::AccountStructure;
use super::relationship::{ArbitrageRelationshipType, ConfidenceLevel};
use super::scoring::RiskScoreComponents;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitrageOpportunityAnalysis {
    pub opportunity_id: String,
    pub timestamp: DateTime<Utc>,
    pub relationship_type: ArbitrageRelationshipType,
    pub symbol: String,
    pub buy_exchange: String,
    pub buy_market_type: MarketType,
    pub sell_exchange: String,
    pub sell_market_type: MarketType,
    pub target_quantity: f64,
    pub target_notional: f64,
    pub buy_best_price: Option<f64>,
    pub sell_best_price: Option<f64>,
    pub buy_vwap: Option<f64>,
    pub sell_vwap: Option<f64>,
    pub buy_slippage_bps: f64,
    pub sell_slippage_bps: f64,
    pub raw_executable_spread_bps: f64,
    pub tt_immediate_net_bps: f64,
    pub tt_immediate_net_pnl: f64,
    pub tt_lifecycle_expected_net_bps: f64,
    pub tt_lifecycle_expected_net_pnl: f64,
    pub maker_buy_taker_sell_theoretical_net_bps: Option<f64>,
    pub maker_buy_taker_sell_expected_net_bps: Option<f64>,
    pub taker_buy_maker_sell_theoretical_net_bps: Option<f64>,
    pub taker_buy_maker_sell_expected_net_bps: Option<f64>,
    pub expected_funding_net_bps: f64,
    pub expected_rebalance_cost_bps: f64,
    pub expected_exit_fee_bps: f64,
    pub expected_exit_slippage_bps: f64,
    pub expected_residual_loss_bps: f64,
    pub safety_buffer_bps: f64,
    pub expected_holding_seconds: u64,
    pub required_capital_usdt: f64,
    pub expected_return_on_capital: f64,
    pub expected_return_on_capital_per_hour: f64,
    pub risk_adjusted_score: f64,
    pub score_components: RiskScoreComponents,
    pub fee_sources: Vec<FeeSource>,
    pub book_ages: Vec<i64>,
    pub book_latencies: Vec<Option<i64>>,
    pub account_structure: AccountStructure,
    pub confidence: ConfidenceLevel,
    pub warnings: Vec<String>,
    pub rejection_reasons: Vec<String>,
    pub accepted: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ArbitrageOpportunityQuery {
    pub relationship_type: Option<ArbitrageRelationshipType>,
    pub symbol: Option<String>,
    pub exchange: Option<String>,
    pub min_tt_net_bps: Option<f64>,
    pub min_lifecycle_expected_net_bps: Option<f64>,
    pub min_return_on_capital_per_hour: Option<f64>,
    pub confidence: Option<ConfidenceLevel>,
    pub accepted: Option<bool>,
    pub fresh_only: Option<bool>,
}

pub fn filter_opportunities(
    mut values: Vec<ArbitrageOpportunityAnalysis>,
    query: ArbitrageOpportunityQuery,
) -> Vec<ArbitrageOpportunityAnalysis> {
    if let Some(relationship_type) = query.relationship_type {
        values.retain(|item| item.relationship_type == relationship_type);
    }
    if let Some(symbol) = query.symbol {
        let symbol = normalize_symbol(&symbol);
        values.retain(|item| normalize_symbol(&item.symbol) == symbol);
    }
    if let Some(exchange) = query.exchange {
        let exchange = exchange.trim().to_ascii_lowercase();
        values.retain(|item| item.buy_exchange == exchange || item.sell_exchange == exchange);
    }
    if let Some(min) = query.min_tt_net_bps {
        values.retain(|item| item.tt_immediate_net_bps >= min);
    }
    if let Some(min) = query.min_lifecycle_expected_net_bps {
        values.retain(|item| item.tt_lifecycle_expected_net_bps >= min);
    }
    if let Some(min) = query.min_return_on_capital_per_hour {
        values.retain(|item| item.expected_return_on_capital_per_hour >= min);
    }
    if let Some(confidence) = query.confidence {
        values.retain(|item| item.confidence >= confidence);
    }
    if let Some(accepted) = query.accepted {
        values.retain(|item| item.accepted == accepted);
    }
    if query.fresh_only.unwrap_or(false) {
        values.retain(|item| {
            !item
                .warnings
                .iter()
                .any(|warning| warning.contains("stale"))
        });
    }
    values
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}
