use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{
    MarketType, OrderBookSnapshot, OrderSide, OrderStatus, OrderType, SymbolRule,
};
use crate::execution::{BalanceReconciliationReport, FeeLookupResult, OrderReconciliationResult};
use crate::live_preflight::{LivePreflightReport, SmallLiveGateReport};
use crate::risk::KillSwitchState;
use crate::web::RecorderHealthView;

use super::{
    DustPosition, EffectiveTradability, FillOwnershipRecord, InventoryReadiness,
    MarketLiquidationPlan, OrderOwnershipClass, PassiveLiquidationSession,
    RuntimeComponentMetadata, SnapshotComponentStatus, SnapshotConsistencyReport,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotControlRuntimeSnapshot {
    pub generated_at: DateTime<Utc>,
    pub snapshot_id: String,
    pub symbol: String,
    pub selected_exchanges: Vec<String>,
    pub symbol_rules: BTreeMap<String, SymbolRule>,
    pub books: BTreeMap<String, OrderBookSnapshot>,
    pub book_health: Vec<SnapshotComponentStatus>,
    pub exchange_health: Vec<SnapshotComponentStatus>,
    pub fees: BTreeMap<String, RuntimeFeeView>,
    pub balances: Vec<RuntimeBalanceView>,
    pub reservations: Vec<RuntimeReservationView>,
    pub inventory_ownership: Vec<InventoryOwnership>,
    pub unmanaged_positions: Vec<RuntimeUnmanagedPosition>,
    pub disabled_state: Vec<RuntimeDisabledState>,
    pub open_orders: Vec<OpenOrderOwnership>,
    #[serde(default)]
    pub fill_ownership: Vec<FillOwnershipRecord>,
    #[serde(default)]
    pub order_reconciliation_state: Option<OrderReconciliationResult>,
    #[serde(default)]
    pub balance_reconciliation_state: Option<BalanceReconciliationReport>,
    #[serde(default)]
    pub kill_switch_state: Option<KillSwitchState>,
    #[serde(default)]
    pub live_preflight_state: Option<LivePreflightReport>,
    #[serde(default)]
    pub small_live_gate_state: Option<SmallLiveGateReport>,
    #[serde(default)]
    pub recorder_health: Option<RecorderHealthView>,
    pub direction_readiness: Vec<DirectionReadiness>,
    pub effective_tradability: Option<EffectiveTradability>,
    pub liquidation_preview: LiquidationPreview,
    pub data_sources: Vec<String>,
    #[serde(default = "default_snapshot_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub source_metadata: Vec<RuntimeComponentMetadata>,
    #[serde(default)]
    pub consistency_report: Option<SnapshotConsistencyReport>,
    pub component_statuses: Vec<SnapshotComponentStatus>,
    pub warnings: Vec<String>,
    pub critical_errors: Vec<String>,
}

fn default_snapshot_schema_version() -> u32 {
    1
}

impl SpotControlRuntimeSnapshot {
    pub fn age_ms(&self) -> i64 {
        Utc::now()
            .signed_duration_since(self.generated_at)
            .num_milliseconds()
            .max(0)
    }

    pub fn has_critical_errors(&self) -> bool {
        !self.critical_errors.is_empty()
    }

    pub fn component(&self, name: &str) -> Option<&SnapshotComponentStatus> {
        self.component_statuses
            .iter()
            .find(|status| status.component == name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeFeeView {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    pub source: crate::execution::FeeSource,
    pub updated_at: DateTime<Utc>,
    pub authoritative: bool,
    pub fallback: bool,
}

impl RuntimeFeeView {
    pub fn from_lookup(exchange: String, symbol: String, lookup: FeeLookupResult) -> Self {
        Self {
            exchange,
            market_type: MarketType::Spot,
            symbol,
            maker_fee_bps: lookup.effective_rate.maker_fee_bps,
            taker_fee_bps: lookup.effective_rate.taker_fee_bps,
            source: lookup.effective_rate.source,
            updated_at: lookup.effective_rate.updated_at,
            authoritative: !matches!(
                lookup.effective_rate.source,
                crate::execution::FeeSource::Fallback
            ),
            fallback: matches!(
                lookup.effective_rate.source,
                crate::execution::FeeSource::Fallback
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeBalanceView {
    pub exchange: String,
    pub market_type: MarketType,
    pub asset: String,
    pub total: f64,
    pub available: f64,
    pub locked_by_exchange: f64,
    pub locally_reserved: f64,
    pub effective_available: f64,
    pub source: String,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeReservationView {
    pub exchange: String,
    pub asset: String,
    pub locally_reserved: f64,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeUnmanagedPosition {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub asset: String,
    pub quantity: f64,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeDisabledState {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub blocked: bool,
    #[serde(default)]
    pub reason: Option<String>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InventoryOwnership {
    pub exchange: String,
    pub market_type: MarketType,
    pub symbol: String,
    pub asset: String,
    pub total_balance: f64,
    pub exchange_locked: f64,
    pub locally_reserved: f64,
    pub unmanaged_quantity: f64,
    pub other_strategy_owned_quantity: f64,
    pub spot_control_managed_quantity: f64,
    pub effective_sellable_managed_quantity: f64,
    pub ownership_known: bool,
    pub source: String,
    pub reasons: Vec<String>,
}

impl InventoryOwnership {
    pub fn compute(
        exchange: impl Into<String>,
        symbol: impl Into<String>,
        asset: impl Into<String>,
        total_balance: f64,
        exchange_locked: f64,
        locally_reserved: f64,
        unmanaged_quantity: f64,
        other_strategy_owned_quantity: f64,
        ownership_known: bool,
    ) -> Self {
        let spot_control_managed_quantity = if ownership_known {
            (total_balance - unmanaged_quantity - other_strategy_owned_quantity).max(0.0)
        } else {
            0.0
        };
        let effective_sellable_managed_quantity =
            (spot_control_managed_quantity - exchange_locked - locally_reserved).max(0.0);
        let mut reasons = Vec::new();
        if !ownership_known {
            reasons.push("inventory ownership unknown".to_string());
        }
        if unmanaged_quantity > 0.0 {
            reasons.push("unmanaged inventory excluded".to_string());
        }
        if other_strategy_owned_quantity > 0.0 {
            reasons.push("other strategy inventory excluded".to_string());
        }
        if exchange_locked > 0.0 {
            reasons.push("exchange locked inventory excluded".to_string());
        }
        if locally_reserved > 0.0 {
            reasons.push("local reservation excluded".to_string());
        }
        Self {
            exchange: super::normalize_exchange(&exchange.into()),
            market_type: MarketType::Spot,
            symbol: super::normalize_symbol(&symbol.into()),
            asset: asset.into().to_ascii_uppercase(),
            total_balance,
            exchange_locked,
            locally_reserved,
            unmanaged_quantity,
            other_strategy_owned_quantity,
            spot_control_managed_quantity,
            effective_sellable_managed_quantity,
            ownership_known,
            source: "authoritative_runtime_snapshot".to_string(),
            reasons,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenOrderOwnership {
    pub exchange: String,
    pub symbol: String,
    pub order_id: String,
    #[serde(default)]
    pub client_order_id: Option<String>,
    pub owning_strategy: String,
    #[serde(default)]
    pub lifecycle_command_id_optional: Option<String>,
    pub side: OrderSide,
    pub quantity: f64,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub status: OrderStatus,
    pub reservation_impact: RuntimeReservationView,
    pub ownership_known: bool,
    #[serde(default = "default_order_ownership_class")]
    pub ownership_class: OrderOwnershipClass,
}

fn default_order_ownership_class() -> OrderOwnershipClass {
    OrderOwnershipClass::Unknown
}

impl OpenOrderOwnership {
    pub fn from_order(
        order: &crate::exchanges::unified::OrderResponse,
        owning_strategy: impl Into<String>,
        ownership_known: bool,
    ) -> Self {
        let remaining_quantity = (order.quantity - order.filled_quantity).max(0.0);
        let asset = match order.side {
            OrderSide::Buy => "QUOTE",
            OrderSide::Sell => "BASE",
        };
        Self {
            exchange: super::normalize_exchange(&order.exchange),
            symbol: super::normalize_symbol(&order.symbol),
            order_id: order.order_id.clone(),
            client_order_id: order.client_order_id.clone(),
            owning_strategy: owning_strategy.into(),
            lifecycle_command_id_optional: None,
            side: order.side,
            quantity: order.quantity,
            filled_quantity: order.filled_quantity,
            remaining_quantity,
            status: order.status,
            reservation_impact: RuntimeReservationView {
                exchange: super::normalize_exchange(&order.exchange),
                asset: asset.to_string(),
                locally_reserved: remaining_quantity,
                source: "open_order".to_string(),
            },
            ownership_known,
            ownership_class: if ownership_known {
                OrderOwnershipClass::OtherKnownStrategy
            } else {
                OrderOwnershipClass::Unknown
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DirectionReadinessStatus {
    Ready,
    ObserveOnly,
    BuyInventoryMissing,
    SellInventoryMissing,
    BookStale,
    FeeFallbackWarning,
    Disabled,
    Locked,
    Blocked,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectionReadiness {
    pub buy_exchange: String,
    pub sell_exchange: String,
    pub buy_quote_available: f64,
    pub sell_base_available: f64,
    pub buy_book_fresh: bool,
    pub sell_book_fresh: bool,
    pub buy_symbol_tradable: bool,
    pub sell_symbol_tradable: bool,
    pub buy_fee_known: bool,
    pub sell_fee_known: bool,
    pub max_safe_notional: f64,
    pub readiness: DirectionReadinessStatus,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiquidationPreview {
    pub market_plans: Vec<MarketLiquidationPlan>,
    pub passive_sessions: Vec<PassiveLiquidationSession>,
    pub dust: Vec<DustPosition>,
    pub rejected: bool,
    pub reasons: Vec<String>,
}

pub fn inventory_status_from_direction(readiness: &[DirectionReadiness]) -> InventoryReadiness {
    if readiness.is_empty() {
        return InventoryReadiness::Unknown;
    }
    let buy_ok = readiness.iter().any(|item| item.buy_quote_available > 0.0);
    let sell_ok = readiness.iter().any(|item| item.sell_base_available > 0.0);
    match (buy_ok, sell_ok) {
        (true, true) => InventoryReadiness::ReadyBothDirections,
        (true, false) => InventoryReadiness::ReadyBuyOnly,
        (false, true) => InventoryReadiness::ReadySellOnly,
        (false, false) => InventoryReadiness::Unknown,
    }
}

pub fn supports_post_only(rule: &SymbolRule) -> bool {
    rule.supported_order_types.contains(&OrderType::PostOnly)
        || rule
            .supported_time_in_force
            .contains(&crate::exchanges::unified::TimeInForce::GTX)
}
