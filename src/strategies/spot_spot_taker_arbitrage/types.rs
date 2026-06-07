use chrono::{DateTime, Utc};
pub use rustcta_strategy_spot_spot_arbitrage::{
    BookSource, RejectionReason, SpotVenue, TradePnlCategory,
};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{OrderBookLevel, SymbolRule};
use crate::execution::{FeeSource, OpportunityLatencyTrace};

pub use rustcta_strategy_spot_spot_arbitrage::configured_spot_pair;

#[derive(Debug, Clone)]
pub struct CommonSymbolRules {
    pub mexc: SymbolRule,
    pub coinex: SymbolRule,
    pub gateio: Option<SymbolRule>,
    pub bitget: Option<SymbolRule>,
    pub kucoin: Option<SymbolRule>,
}

impl CommonSymbolRules {
    pub fn for_exchange(&self, exchange: SpotVenue) -> &SymbolRule {
        match exchange {
            SpotVenue::Mexc => &self.mexc,
            SpotVenue::CoinEx => &self.coinex,
            SpotVenue::GateIo => self.gateio.as_ref().unwrap_or(&self.mexc),
            SpotVenue::Bitget => self.bitget.as_ref().unwrap_or(&self.coinex),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBook {
    pub exchange: String,
    pub symbol: String,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub exchange_timestamp: Option<DateTime<Utc>>,
    pub local_timestamp: DateTime<Utc>,
    pub latency_ms: Option<i64>,
    pub sequence: Option<u64>,
    pub source: BookSource,
    pub is_stale: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_price: f64,
    pub sell_price: f64,
    pub raw_spread_bps: f64,
    pub buy_fee_bps: f64,
    pub sell_fee_bps: f64,
    pub fee_source_buy: FeeSource,
    pub fee_source_sell: FeeSource,
    pub platform_discount_applied: bool,
    pub estimated_fee_bps: f64,
    pub estimated_slippage_bps: f64,
    pub safety_buffer_bps: f64,
    pub estimated_net_spread_bps: f64,
    pub estimated_total_fee: f64,
    pub estimated_gross_pnl: f64,
    pub estimated_net_pnl: f64,
    #[serde(default)]
    pub capital_cost_bps: f64,
    #[serde(default)]
    pub transfer_cost_bps: f64,
    #[serde(default)]
    pub transfer_delay_penalty_bps: f64,
    #[serde(default)]
    pub inventory_rebalance_cost_bps: f64,
    #[serde(default)]
    pub latency_penalty_bps: f64,
    #[serde(default)]
    pub effective_min_net_spread_bps: f64,
    #[serde(default)]
    pub estimated_slippage_cost: f64,
    #[serde(default)]
    pub estimated_capital_cost: f64,
    #[serde(default)]
    pub estimated_transfer_cost: f64,
    #[serde(default)]
    pub estimated_inventory_rebalance_cost: f64,
    #[serde(default)]
    pub estimated_latency_penalty_cost: f64,
    #[serde(default)]
    pub estimated_total_cost: f64,
    pub executable_notional: f64,
    pub quantity: f64,
    pub accepted: bool,
    pub rejection_reason: Option<RejectionReason>,
    pub rejection_detail: Option<String>,
    pub buy_book_age_ms: i64,
    pub sell_book_age_ms: i64,
    pub buy_book_source: BookSource,
    pub sell_book_source: BookSource,
    pub buy_latency_ms: Option<i64>,
    pub sell_latency_ms: Option<i64>,
    pub lifecycle_latency: Option<OpportunityLatencyTrace>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedTradeRecord {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_avg_price: f64,
    pub sell_avg_price: f64,
    pub quantity: f64,
    pub notional: f64,
    pub buy_fee: f64,
    pub sell_fee: f64,
    pub gross_pnl: f64,
    pub net_pnl: f64,
    #[serde(default)]
    pub pnl_category: TradePnlCategory,
    #[serde(default)]
    pub slippage_cost: f64,
    #[serde(default)]
    pub capital_cost: f64,
    #[serde(default)]
    pub transfer_cost: f64,
    #[serde(default)]
    pub inventory_rebalance_cost: f64,
    #[serde(default)]
    pub latency_penalty_cost: f64,
    pub latency_ms: i64,
    pub order_book_age_ms: i64,
    pub execution_mode: String,
}
