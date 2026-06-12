use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::core::FundingSpreadExpansionMakerConfig;
use crate::{DISPLAY_NAME, MIGRATED_FROM, STRATEGY_KIND};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingSpreadRuntimeMode {
    Observe,
    LiveRequested,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeProviderContract {
    pub boundary: &'static str,
    pub adapter_free: bool,
    pub concrete_adapter_dependency: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeTaskContract {
    pub task_kind: &'static str,
    pub provider_boundary: &'static str,
    pub emits_dashboard_snapshot: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingSpreadRouteDashboardRow {
    pub route_id: String,
    pub leg_a_exchange: String,
    pub leg_b_exchange: String,
    pub symbol: String,
    pub target_direction: String,
    pub open_spread_pct: String,
    pub target_close_spread_pct: String,
    pub max_position_notional_usdt: String,
    pub order_notional_range_usdt: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingSpreadDashboardSnapshot {
    pub schema_version: u32,
    pub captured_at: DateTime<Utc>,
    pub strategy_kind: &'static str,
    pub migrated_from: &'static str,
    pub mode: FundingSpreadRuntimeMode,
    pub live_orders_enabled: bool,
    pub route_count: usize,
    pub enabled_routes: usize,
    pub preferred_open_style: String,
    pub preferred_close_style: String,
    pub risk_close_style: String,
    pub allow_negative_immediate_edge: bool,
    pub close_on_unrealized_loss: bool,
    pub route_rows: Vec<FundingSpreadRouteDashboardRow>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingSpreadRuntimeContract {
    pub schema_version: u32,
    pub strategy_kind: &'static str,
    pub display_name: &'static str,
    pub migrated_from: &'static str,
    pub mode: FundingSpreadRuntimeMode,
    pub live_orders_enabled_by_default: bool,
    pub market_data_provider: RuntimeProviderContract,
    pub execution_provider: RuntimeProviderContract,
    pub storage_provider: RuntimeProviderContract,
    pub dashboard_snapshot_provider: RuntimeProviderContract,
    pub notification_provider: RuntimeProviderContract,
    pub tasks: Vec<RuntimeTaskContract>,
    pub dashboard_snapshot: FundingSpreadDashboardSnapshot,
}

pub trait FundingSpreadMarketDataProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub trait FundingSpreadExecutionProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;

    fn live_orders_enabled(&self) -> bool {
        false
    }
}

pub trait FundingSpreadStorageProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub trait FundingSpreadDashboardSnapshotProvider: Send + Sync {
    fn snapshot(&self, captured_at: DateTime<Utc>) -> FundingSpreadDashboardSnapshot;
}

pub trait FundingSpreadNotificationProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub fn build_runtime_contract(
    config: &FundingSpreadExpansionMakerConfig,
    captured_at: DateTime<Utc>,
) -> FundingSpreadRuntimeContract {
    let mode = if config.mode.eq_ignore_ascii_case("live") {
        FundingSpreadRuntimeMode::LiveRequested
    } else {
        FundingSpreadRuntimeMode::Observe
    };
    let dashboard_snapshot = FundingSpreadDashboardSnapshot {
        schema_version: 1,
        captured_at,
        strategy_kind: STRATEGY_KIND,
        migrated_from: MIGRATED_FROM,
        mode,
        live_orders_enabled: false,
        route_count: config.routes.len(),
        enabled_routes: config.routes.iter().filter(|route| route.enabled).count(),
        preferred_open_style: format!("{:?}", config.execution.preferred_open_style),
        preferred_close_style: format!("{:?}", config.execution.preferred_close_style),
        risk_close_style: format!("{:?}", config.execution.risk_close_style),
        allow_negative_immediate_edge: config.thresholds.allow_negative_immediate_edge,
        close_on_unrealized_loss: config.risk.close_on_unrealized_loss,
        route_rows: config
            .routes
            .iter()
            .map(|route| FundingSpreadRouteDashboardRow {
                route_id: route.route_id.clone(),
                leg_a_exchange: route.leg_a_exchange.clone(),
                leg_b_exchange: route.leg_b_exchange.clone(),
                symbol: route.symbol.clone(),
                target_direction: format!("{:?}", route.target_direction),
                open_spread_pct: percent(config.thresholds.open_spread_pct),
                target_close_spread_pct: percent(config.thresholds.target_close_spread_pct),
                max_position_notional_usdt: decimal(config.sizing.max_position_notional_usdt),
                order_notional_range_usdt: format!(
                    "{}..{}",
                    decimal(config.sizing.min_order_notional_usdt),
                    decimal(config.sizing.max_order_notional_usdt)
                ),
            })
            .collect(),
    };

    FundingSpreadRuntimeContract {
        schema_version: 1,
        strategy_kind: STRATEGY_KIND,
        display_name: DISPLAY_NAME,
        migrated_from: MIGRATED_FROM,
        mode,
        live_orders_enabled_by_default: false,
        market_data_provider: provider("strategy_sdk_funding_spread_market_data_provider"),
        execution_provider: provider("strategy_sdk_execution_provider"),
        storage_provider: provider("strategy_app_storage_provider"),
        dashboard_snapshot_provider: provider("strategy_snapshot_provider"),
        notification_provider: provider("strategy_notification_provider"),
        tasks: vec![
            task(
                "load_route_market_data",
                "strategy_sdk_funding_spread_market_data_provider",
                true,
            ),
            task("evaluate_open_and_add", "strategy_runtime_core", true),
            task(
                "evaluate_target_and_risk_close",
                "strategy_runtime_core",
                true,
            ),
            task(
                "submit_execution_orders",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "repair_single_leg_or_unknown",
                "strategy_sdk_execution_provider",
                true,
            ),
            task("persist_events", "strategy_app_storage_provider", false),
            task(
                "publish_dashboard_snapshot",
                "strategy_snapshot_provider",
                true,
            ),
            task("notify_operator", "strategy_notification_provider", false),
        ],
        dashboard_snapshot,
    }
}

pub fn default_runtime_contract(captured_at: DateTime<Utc>) -> FundingSpreadRuntimeContract {
    build_runtime_contract(&FundingSpreadExpansionMakerConfig::default(), captured_at)
}

fn provider(boundary: &'static str) -> RuntimeProviderContract {
    RuntimeProviderContract {
        boundary,
        adapter_free: true,
        concrete_adapter_dependency: false,
    }
}

fn task(
    task_kind: &'static str,
    provider_boundary: &'static str,
    emits_dashboard_snapshot: bool,
) -> RuntimeTaskContract {
    RuntimeTaskContract {
        task_kind,
        provider_boundary,
        emits_dashboard_snapshot,
    }
}

fn decimal(value: f64) -> String {
    let mut text = format!("{value:.8}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn percent(value: f64) -> String {
    format!("{}%", decimal(value * 100.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_should_be_adapter_free_and_show_execution_styles() {
        let contract = default_runtime_contract(Utc::now());
        assert!(contract.market_data_provider.adapter_free);
        assert!(!contract.execution_provider.concrete_adapter_dependency);
        assert_eq!(
            contract.dashboard_snapshot.preferred_close_style,
            "MakerMakerReduceOnly"
        );
        assert!(!contract.dashboard_snapshot.close_on_unrealized_loss);
    }
}
