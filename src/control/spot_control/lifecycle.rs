use std::collections::BTreeSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum SpotSymbolLifecycleState {
    Discovered,
    Disabled,
    EnableRequested,
    EnableValidating,
    InventoryReviewRequired,
    Ready,
    Active,
    Paused,
    DisableRequested,
    CancelingOrders,
    Frozen,
    LiquidationPlanning,
    LiquidatingMarket,
    LiquidatingPassive,
    DustRemaining,
    DisabledWithInventory,
    DisabledClean,
    Failed,
    ManualInterventionRequired,
}

impl SpotSymbolLifecycleState {
    pub fn allows_new_arbitrage(self) -> bool {
        matches!(self, Self::Active)
    }

    pub fn blocks_new_arbitrage(self) -> bool {
        !self.allows_new_arbitrage()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DisableMode {
    #[serde(rename = "FreezeOnly", alias = "freeze_only")]
    FreezeOnly,
    #[serde(rename = "MarketLiquidate", alias = "market_liquidate")]
    MarketLiquidate,
    #[serde(rename = "PassiveAskLiquidate", alias = "passive_ask_liquidate")]
    PassiveAskLiquidate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EnableMode {
    #[serde(rename = "ObserveOnly", alias = "observe_only")]
    ObserveOnly,
    #[serde(rename = "Paper", alias = "paper")]
    Paper,
    #[serde(rename = "LiveDryRun", alias = "live_dry_run")]
    LiveDryRun,
    #[serde(rename = "FutureSmallLive", alias = "future_small_live")]
    FutureSmallLive,
}

impl EnableMode {
    pub fn allows_strategy_mode(self, strategy_mode: &str) -> bool {
        match self {
            Self::ObserveOnly => true,
            Self::Paper => strategy_mode.eq_ignore_ascii_case("paper"),
            Self::LiveDryRun => {
                strategy_mode.eq_ignore_ascii_case("paper")
                    || strategy_mode.eq_ignore_ascii_case("live_dry_run")
            }
            Self::FutureSmallLive => true,
        }
    }

    pub fn may_require_real_inventory(self) -> bool {
        matches!(self, Self::FutureSmallLive)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SymbolOperationType {
    #[serde(rename = "Enable", alias = "enable")]
    Enable,
    #[serde(rename = "Pause", alias = "pause")]
    Pause,
    #[serde(rename = "Resume", alias = "resume")]
    Resume,
    #[serde(rename = "Disable", alias = "disable")]
    Disable,
    #[serde(rename = "CancelOrders", alias = "cancel_orders")]
    CancelOrders,
    #[serde(rename = "MarketLiquidate", alias = "market_liquidate")]
    MarketLiquidate,
    #[serde(rename = "PassiveLiquidate", alias = "passive_liquidate")]
    PassiveLiquidate,
    #[serde(rename = "MarkDust", alias = "mark_dust")]
    MarkDust,
    #[serde(rename = "MarkUnmanaged", alias = "mark_unmanaged")]
    MarkUnmanaged,
    #[serde(rename = "ManualReconcile", alias = "manual_reconcile")]
    ManualReconcile,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EnabledDirection {
    pub buy_exchange: String,
    pub sell_exchange: String,
}

impl EnabledDirection {
    pub fn normalized(mut self) -> Self {
        self.buy_exchange = normalize_exchange(&self.buy_exchange);
        self.sell_exchange = normalize_exchange(&self.sell_exchange);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedSpotSymbol {
    pub internal_symbol: String,
    pub selected_exchanges: Vec<String>,
    #[serde(default)]
    pub allowed_directions: Vec<EnabledDirection>,
    pub quote_asset: String,
    pub lifecycle_state: SpotSymbolLifecycleState,
    pub enable_mode: EnableMode,
    #[serde(default)]
    pub disabled_reason_optional: Option<String>,
    #[serde(default)]
    pub enabled_at_optional: Option<DateTime<Utc>>,
    #[serde(default)]
    pub disabled_at_optional: Option<DateTime<Utc>>,
    pub requested_by: String,
    #[serde(default)]
    pub last_command_id: Option<String>,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ManagedSpotSymbol {
    pub fn new_disabled(
        internal_symbol: impl Into<String>,
        quote_asset: impl Into<String>,
        requested_by: impl Into<String>,
        now: DateTime<Utc>,
    ) -> Self {
        Self {
            internal_symbol: normalize_symbol(&internal_symbol.into()),
            selected_exchanges: Vec::new(),
            allowed_directions: Vec::new(),
            quote_asset: quote_asset.into().to_ascii_uppercase(),
            lifecycle_state: SpotSymbolLifecycleState::Disabled,
            enable_mode: EnableMode::ObserveOnly,
            disabled_reason_optional: None,
            enabled_at_optional: None,
            disabled_at_optional: None,
            requested_by: requested_by.into(),
            last_command_id: None,
            version: 0,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn allows_direction(&self, buy_exchange: &str, sell_exchange: &str) -> bool {
        let buy_exchange = normalize_exchange(buy_exchange);
        let sell_exchange = normalize_exchange(sell_exchange);
        self.allowed_directions.iter().any(|direction| {
            direction.buy_exchange == buy_exchange && direction.sell_exchange == sell_exchange
        })
    }

    pub fn selected_exchange_set(&self) -> BTreeSet<String> {
        self.selected_exchanges
            .iter()
            .map(|exchange| normalize_exchange(exchange))
            .collect()
    }

    pub fn normalize_in_place(&mut self) {
        self.internal_symbol = normalize_symbol(&self.internal_symbol);
        self.quote_asset = self.quote_asset.trim().to_ascii_uppercase();
        self.selected_exchanges = normalize_exchange_list(&self.selected_exchanges);
        self.allowed_directions = self
            .allowed_directions
            .drain(..)
            .map(EnabledDirection::normalized)
            .collect();
    }
}

pub fn validate_transition(
    current: SpotSymbolLifecycleState,
    next: SpotSymbolLifecycleState,
) -> Result<(), String> {
    use SpotSymbolLifecycleState::*;
    let valid = matches!(
        (current, next),
        (Discovered, Disabled)
            | (Discovered, EnableRequested)
            | (Disabled, EnableRequested)
            | (DisabledClean, EnableRequested)
            | (DisabledWithInventory, EnableRequested)
            | (DustRemaining, EnableRequested)
            | (DisabledClean, DisableRequested)
            | (DisabledWithInventory, DisableRequested)
            | (DustRemaining, DisableRequested)
            | (Frozen, EnableRequested)
            | (EnableRequested, EnableValidating)
            | (EnableValidating, InventoryReviewRequired)
            | (EnableValidating, Ready)
            | (EnableValidating, Active)
            | (EnableValidating, Failed)
            | (InventoryReviewRequired, Ready)
            | (InventoryReviewRequired, Active)
            | (Ready, Active)
            | (Active, Paused)
            | (Paused, Active)
            | (Active, DisableRequested)
            | (Paused, DisableRequested)
            | (Ready, DisableRequested)
            | (InventoryReviewRequired, DisableRequested)
            | (DisableRequested, CancelingOrders)
            | (DisableRequested, Frozen)
            | (DisableRequested, LiquidationPlanning)
            | (CancelingOrders, Frozen)
            | (CancelingOrders, LiquidationPlanning)
            | (CancelingOrders, ManualInterventionRequired)
            | (LiquidationPlanning, LiquidatingMarket)
            | (LiquidationPlanning, LiquidatingPassive)
            | (LiquidationPlanning, ManualInterventionRequired)
            | (LiquidatingMarket, DisabledClean)
            | (LiquidatingMarket, DisabledWithInventory)
            | (LiquidatingMarket, DustRemaining)
            | (LiquidatingMarket, ManualInterventionRequired)
            | (LiquidatingPassive, DisabledClean)
            | (LiquidatingPassive, DisabledWithInventory)
            | (LiquidatingPassive, DustRemaining)
            | (LiquidatingPassive, ManualInterventionRequired)
            | (Frozen, DisabledWithInventory)
            | (Failed, EnableRequested)
            | (ManualInterventionRequired, DisableRequested)
            | (ManualInterventionRequired, EnableRequested)
    ) || current == next;
    if valid {
        Ok(())
    } else {
        Err(format!(
            "invalid lifecycle transition {current:?} -> {next:?}"
        ))
    }
}

pub fn normalize_exchange(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" => "gateio".to_string(),
        other => other.to_string(),
    }
}

pub fn normalize_exchange_list(values: &[String]) -> Vec<String> {
    let mut values = values
        .iter()
        .map(|value| normalize_exchange(value))
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    values.sort();
    values.dedup();
    values
}

pub fn normalize_symbol(value: &str) -> String {
    value
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EffectiveTradability {
    pub lifecycle_allows: bool,
    pub disabled_registry_allows: bool,
    pub kill_switch_allows: bool,
    pub inventory_allows: bool,
    pub operation_lock_allows: bool,
    pub effective_allowed: bool,
    pub rejection_reasons: Vec<String>,
}

impl EffectiveTradability {
    pub fn evaluate(
        lifecycle_allows: bool,
        disabled_registry_allows: bool,
        kill_switch_allows: bool,
        inventory_allows: bool,
        operation_lock_allows: bool,
    ) -> Self {
        let mut rejection_reasons = Vec::new();
        if !lifecycle_allows {
            rejection_reasons.push("lifecycle_state_blocks_trading".to_string());
        }
        if !disabled_registry_allows {
            rejection_reasons.push("disabled_registry_blocks_trading".to_string());
        }
        if !kill_switch_allows {
            rejection_reasons.push("kill_switch_blocks_trading".to_string());
        }
        if !inventory_allows {
            rejection_reasons.push("inventory_not_ready".to_string());
        }
        if !operation_lock_allows {
            rejection_reasons.push("symbol_operation_lock_conflict".to_string());
        }
        Self {
            lifecycle_allows,
            disabled_registry_allows,
            kill_switch_allows,
            inventory_allows,
            operation_lock_allows,
            effective_allowed: rejection_reasons.is_empty(),
            rejection_reasons,
        }
    }
}
