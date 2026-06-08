use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{CrossExchangeArbitrageConfig, DISPLAY_NAME, MIGRATED_FROM, STRATEGY_KIND};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossArbRuntimeMode {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbMarketSnapshotRow {
    pub exchange: String,
    pub symbol: String,
    pub bid_quote: String,
    pub ask_quote: String,
    pub captured_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbOpportunityRow {
    pub opportunity_id: String,
    pub symbol: String,
    pub long_exchange: String,
    pub short_exchange: String,
    pub net_edge_bps: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbRouteHealthRow {
    pub exchange: String,
    pub status: String,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbDashboardSnapshot {
    pub schema_version: u32,
    pub captured_at: DateTime<Utc>,
    pub strategy_kind: &'static str,
    pub migrated_from: &'static str,
    pub mode: CrossArbRuntimeMode,
    pub venues: Vec<String>,
    pub symbols: Vec<String>,
    pub live_orders_enabled: bool,
    pub open_bundles: usize,
    pub pending_orders: usize,
    pub notification_status: &'static str,
    pub market_snapshots: Vec<CrossArbMarketSnapshotRow>,
    pub opportunities: Vec<CrossArbOpportunityRow>,
    pub route_health: Vec<CrossArbRouteHealthRow>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbRuntimeContract {
    pub schema_version: u32,
    pub strategy_kind: &'static str,
    pub display_name: &'static str,
    pub migrated_from: &'static str,
    pub mode: CrossArbRuntimeMode,
    pub live_orders_enabled_by_default: bool,
    pub market_data_provider: RuntimeProviderContract,
    pub execution_provider: RuntimeProviderContract,
    pub storage_provider: RuntimeProviderContract,
    pub dashboard_snapshot_provider: RuntimeProviderContract,
    pub notification_provider: RuntimeProviderContract,
    pub tasks: Vec<RuntimeTaskContract>,
    pub dashboard_snapshot: CrossArbDashboardSnapshot,
}

pub trait CrossArbMarketDataProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub trait CrossArbExecutionProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;

    fn live_orders_enabled(&self) -> bool {
        false
    }
}

pub trait CrossArbStorageProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub trait CrossArbDashboardSnapshotProvider: Send + Sync {
    fn snapshot(&self, captured_at: DateTime<Utc>) -> CrossArbDashboardSnapshot;
}

pub trait CrossArbNotificationProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub fn build_runtime_contract(
    config: &CrossExchangeArbitrageConfig,
    captured_at: DateTime<Utc>,
) -> CrossArbRuntimeContract {
    let mode = if config.dry_run {
        CrossArbRuntimeMode::Observe
    } else {
        CrossArbRuntimeMode::LiveRequested
    };
    let dashboard_snapshot = CrossArbDashboardSnapshot {
        schema_version: 1,
        captured_at,
        strategy_kind: STRATEGY_KIND,
        migrated_from: MIGRATED_FROM,
        mode,
        venues: config.venues.clone(),
        symbols: config.symbols.clone(),
        live_orders_enabled: false,
        open_bundles: 0,
        pending_orders: 0,
        notification_status: "provider_required",
        market_snapshots: Vec::new(),
        opportunities: Vec::new(),
        route_health: Vec::new(),
    };

    CrossArbRuntimeContract {
        schema_version: 1,
        strategy_kind: STRATEGY_KIND,
        display_name: DISPLAY_NAME,
        migrated_from: MIGRATED_FROM,
        mode,
        live_orders_enabled_by_default: false,
        market_data_provider: provider("strategy_sdk_market_data_provider"),
        execution_provider: provider("strategy_sdk_execution_provider"),
        storage_provider: provider("strategy_app_storage_provider"),
        dashboard_snapshot_provider: provider("strategy_snapshot_provider"),
        notification_provider: provider("strategy_notification_provider"),
        tasks: vec![
            task(
                "observe_market_data",
                "strategy_sdk_market_data_provider",
                true,
            ),
            task("evaluate_opportunities", "strategy_runtime_core", true),
            task("plan_execution", "strategy_sdk_execution_provider", true),
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

pub fn default_runtime_contract(captured_at: DateTime<Utc>) -> CrossArbRuntimeContract {
    build_runtime_contract(&CrossExchangeArbitrageConfig::default(), captured_at)
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
