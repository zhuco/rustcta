use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeSymbolInventory {
    pub exchange: String,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub base_total: f64,
    pub base_available: f64,
    pub base_reserved: f64,
    pub base_unmanaged: f64,
    pub quote_total: f64,
    pub quote_available: f64,
    pub quote_reserved: f64,
    pub target_base_inventory: f64,
    pub minimum_base_inventory: f64,
    pub maximum_base_inventory: f64,
    pub inventory_status: InventoryReadiness,
}

impl ExchangeSymbolInventory {
    pub fn managed_sellable_base(&self) -> f64 {
        (self.base_available - self.base_unmanaged - self.base_reserved).max(0.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum InventoryReadiness {
    ReadyBothDirections,
    ReadyBuyOnly,
    ReadySellOnly,
    InsufficientQuote,
    InsufficientBase,
    UnmanagedConflict,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketLiquidationPlan {
    pub command_id: String,
    pub exchange: String,
    pub symbol: String,
    pub managed_sellable_quantity: f64,
    pub rounded_quantity: f64,
    pub best_bid: f64,
    pub executable_vwap: f64,
    pub worst_allowed_price: f64,
    pub estimated_fee: f64,
    pub estimated_slippage_bps: f64,
    pub estimated_proceeds: f64,
    pub estimated_loss: f64,
    pub validation_status: String,
    pub would_submit_order: bool,
    #[serde(default)]
    pub rejection_reason_optional: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PassiveLiquidationStatus {
    Planning,
    WaitingPostOnly,
    Active,
    Repricing,
    Completed,
    Stopped,
    ManualInterventionRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PassiveLiquidationSession {
    pub session_id: String,
    pub command_id: String,
    pub exchange: String,
    pub symbol: String,
    pub initial_managed_quantity: f64,
    pub remaining_quantity: f64,
    pub filled_quantity: f64,
    pub average_sell_price: f64,
    pub fees_paid: f64,
    pub realized_proceeds: f64,
    #[serde(default)]
    pub active_order_id_optional: Option<String>,
    pub started_at: DateTime<Utc>,
    #[serde(default)]
    pub last_reprice_at: Option<DateTime<Utc>>,
    pub status: PassiveLiquidationStatus,
    #[serde(default)]
    pub stop_reason_optional: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DustPosition {
    pub exchange: String,
    pub symbol: String,
    pub asset: String,
    pub quantity: f64,
    pub estimated_value_usdt: f64,
    pub reason: String,
    pub detected_at: DateTime<Utc>,
    pub managed_or_unmanaged: String,
}
