use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    Balance, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, OrderBookSnapshot,
    RequestContext, ResponseMetadata,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolScope {
    pub exchange: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange_symbol: ExchangeSymbol,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolRulesRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbols: Vec<SymbolScope>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolRules {
    pub schema_version: u16,
    pub symbol: SymbolScope,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_increment: Option<String>,
    pub quantity_increment: Option<String>,
    pub min_price: Option<String>,
    pub max_price: Option<String>,
    pub min_quantity: Option<String>,
    pub max_quantity: Option<String>,
    pub min_notional: Option<String>,
    pub max_notional: Option<String>,
    pub price_precision: Option<u32>,
    pub quantity_precision: Option<u32>,
    pub supports_market_orders: bool,
    pub supports_limit_orders: bool,
    pub supports_post_only: bool,
    pub supports_reduce_only: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolRulesResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub rules: Vec<SymbolRules>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub depth: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub order_book: OrderBookSnapshot,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeesRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbols: Vec<SymbolScope>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeRateSnapshot {
    pub schema_version: u16,
    pub symbol: SymbolScope,
    pub maker_rate: String,
    pub taker_rate: String,
    pub source: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeesResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub fees: Vec<FeeRateSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BalancesRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub assets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BalancesResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub balances: Vec<Balance>,
}
