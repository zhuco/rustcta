use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::core::FundingCoreConfig;
use crate::{DISPLAY_NAME, MIGRATED_FROM, STRATEGY_KIND};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FundingRuntimeMode {
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
pub struct FundingDashboardSnapshot {
    pub schema_version: u32,
    pub captured_at: DateTime<Utc>,
    pub strategy_kind: &'static str,
    pub migrated_from: &'static str,
    pub mode: FundingRuntimeMode,
    pub enabled_exchanges: Vec<String>,
    pub quote_asset: String,
    pub min_funding_rate: f64,
    pub live_orders_enabled: bool,
    pub planned_entries: usize,
    pub skipped_entries: usize,
    pub notification_status: &'static str,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FundingRuntimeContract {
    pub schema_version: u32,
    pub strategy_kind: &'static str,
    pub display_name: &'static str,
    pub migrated_from: &'static str,
    pub mode: FundingRuntimeMode,
    pub live_orders_enabled_by_default: bool,
    pub market_data_provider: RuntimeProviderContract,
    pub execution_provider: RuntimeProviderContract,
    pub storage_provider: RuntimeProviderContract,
    pub dashboard_snapshot_provider: RuntimeProviderContract,
    pub notification_provider: RuntimeProviderContract,
    pub tasks: Vec<RuntimeTaskContract>,
    pub dashboard_snapshot: FundingDashboardSnapshot,
}

pub trait FundingMarketDataProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub trait FundingExecutionProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;

    fn live_orders_enabled(&self) -> bool {
        false
    }
}

pub trait FundingStorageProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub trait FundingDashboardSnapshotProvider: Send + Sync {
    fn snapshot(&self, captured_at: DateTime<Utc>) -> FundingDashboardSnapshot;
}

pub trait FundingNotificationProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub fn build_runtime_contract(
    config: &FundingCoreConfig,
    captured_at: DateTime<Utc>,
) -> FundingRuntimeContract {
    let mode = if config.is_live_mode() {
        FundingRuntimeMode::LiveRequested
    } else {
        FundingRuntimeMode::Observe
    };
    let dashboard_snapshot = FundingDashboardSnapshot {
        schema_version: 1,
        captured_at,
        strategy_kind: STRATEGY_KIND,
        migrated_from: MIGRATED_FROM,
        mode,
        enabled_exchanges: config.universe.enabled_exchanges.clone(),
        quote_asset: config.universe.quote_asset.clone(),
        min_funding_rate: config.selection.min_funding_rate,
        live_orders_enabled: false,
        planned_entries: 0,
        skipped_entries: 0,
        notification_status: if config.notifications.enabled {
            "provider_required"
        } else {
            "disabled"
        },
    };

    FundingRuntimeContract {
        schema_version: 1,
        strategy_kind: STRATEGY_KIND,
        display_name: DISPLAY_NAME,
        migrated_from: MIGRATED_FROM,
        mode,
        live_orders_enabled_by_default: false,
        market_data_provider: provider("strategy_sdk_funding_market_data_provider"),
        execution_provider: provider("strategy_sdk_execution_provider"),
        storage_provider: provider("strategy_app_storage_provider"),
        dashboard_snapshot_provider: provider("strategy_snapshot_provider"),
        notification_provider: provider("strategy_notification_provider"),
        tasks: vec![
            task(
                "scan_funding_snapshots",
                "strategy_sdk_funding_market_data_provider",
                true,
            ),
            task("select_candidates", "strategy_runtime_core", true),
            task("plan_live_window", "strategy_runtime_live_plan", true),
            task(
                "submit_execution_intent",
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

pub fn default_runtime_contract(captured_at: DateTime<Utc>) -> FundingRuntimeContract {
    build_runtime_contract(&FundingCoreConfig::default(), captured_at)
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
