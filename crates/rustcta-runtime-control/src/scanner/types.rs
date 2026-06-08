use rustcta_types::MarketType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SymbolStatus {
    #[default]
    Trading,
    Halted,
    Suspended,
    ReduceOnly,
    PreOpen,
    Settling,
    Delisted,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolRule {
    pub exchange: String,
    pub market_type: MarketType,
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: u32,
    pub quantity_precision: u32,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_quantity: f64,
    pub min_notional: f64,
    #[serde(default)]
    pub max_quantity: Option<f64>,
    #[serde(default)]
    pub status: SymbolStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeeSource {
    ExchangeApi,
    ConfigDefault,
    SymbolOverride,
    VipOverride,
    PlatformTokenDiscount,
    Fallback,
}
