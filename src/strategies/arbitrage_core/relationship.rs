use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{MarketType, OrderBookSnapshot, OrderSide, SymbolRule};
use crate::execution::FeeRate;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArbitrageRelationshipType {
    SpotSpot,
    SpotPerp,
    PerpPerp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionModeCandidate {
    TakerTaker,
    MakerBuyTakerSell,
    TakerBuyMakerSell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConfidenceLevel {
    Low,
    Medium,
    High,
}

impl Default for ConfidenceLevel {
    fn default() -> Self {
        Self::Low
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingRateInfo {
    pub funding_rate_bps: f64,
    pub next_funding_time: Option<DateTime<Utc>>,
    pub interval_seconds: Option<u64>,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarginInfo {
    pub initial_margin_rate: f64,
    pub maintenance_margin_rate: f64,
    pub liquidation_buffer_bps: Option<f64>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketLeg {
    pub exchange: String,
    pub market_type: MarketType,
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub side: OrderSide,
    pub order_book: OrderBookSnapshot,
    pub fee_rate: FeeRate,
    pub symbol_rule: SymbolRule,
    pub funding_rate_optional: Option<FundingRateInfo>,
    pub margin_info_optional: Option<MarginInfo>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArbitrageRelationship {
    pub relationship_type: ArbitrageRelationshipType,
    pub buy_leg: MarketLeg,
    pub sell_leg: MarketLeg,
    pub base_asset: String,
    pub quote_asset: String,
    pub settlement_asset_optional: Option<String>,
}
