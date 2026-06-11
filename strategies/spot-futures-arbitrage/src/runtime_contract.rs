use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{SpotFuturesArbitrageConfig, DISPLAY_NAME, MIGRATED_FROM, STRATEGY_KIND};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SpotFuturesRuntimeMode {
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
pub struct SpotFuturesDashboardSnapshot {
    pub schema_version: u32,
    pub captured_at: DateTime<Utc>,
    pub strategy_kind: &'static str,
    pub migrated_from: &'static str,
    pub mode: SpotFuturesRuntimeMode,
    pub quote_asset: String,
    pub enabled_spot_exchanges: Vec<String>,
    pub enabled_perp_exchanges: Vec<String>,
    pub active_symbols: Vec<String>,
    pub excluded_bases: Vec<String>,
    pub min_open_net_edge_bps: f64,
    pub expected_holding_hours: f64,
    pub live_orders_enabled: bool,
    pub open_bundles: usize,
    pub pending_orders: usize,
    pub notification_status: &'static str,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SpotFuturesRuntimeContract {
    pub schema_version: u32,
    pub strategy_kind: &'static str,
    pub display_name: &'static str,
    pub migrated_from: &'static str,
    pub mode: SpotFuturesRuntimeMode,
    pub live_orders_enabled_by_default: bool,
    pub market_data_provider: RuntimeProviderContract,
    pub execution_provider: RuntimeProviderContract,
    pub storage_provider: RuntimeProviderContract,
    pub dashboard_snapshot_provider: RuntimeProviderContract,
    pub notification_provider: RuntimeProviderContract,
    pub tasks: Vec<RuntimeTaskContract>,
    pub dashboard_snapshot: SpotFuturesDashboardSnapshot,
}

pub trait SpotFuturesMarketDataProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub trait SpotFuturesExecutionProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;

    fn live_orders_enabled(&self) -> bool {
        false
    }
}

pub trait SpotFuturesStorageProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub trait SpotFuturesDashboardSnapshotProvider: Send + Sync {
    fn snapshot(&self, captured_at: DateTime<Utc>) -> SpotFuturesDashboardSnapshot;
}

pub trait SpotFuturesNotificationProvider: Send + Sync {
    fn provider_kind(&self) -> &'static str;
}

pub fn build_runtime_contract(
    config: &SpotFuturesArbitrageConfig,
    captured_at: DateTime<Utc>,
) -> SpotFuturesRuntimeContract {
    let mode = if config.enable_live_trading {
        SpotFuturesRuntimeMode::LiveRequested
    } else {
        SpotFuturesRuntimeMode::Observe
    };
    let dashboard_snapshot = SpotFuturesDashboardSnapshot {
        schema_version: 1,
        captured_at,
        strategy_kind: STRATEGY_KIND,
        migrated_from: MIGRATED_FROM,
        mode,
        quote_asset: config.market.quote_asset.clone(),
        enabled_spot_exchanges: config.universe.enabled_spot_exchanges.clone(),
        enabled_perp_exchanges: config.universe.enabled_perp_exchanges.clone(),
        active_symbols: config.active_symbols(),
        excluded_bases: config.universe.excluded_bases.clone(),
        min_open_net_edge_bps: config.selection.min_open_net_edge_bps,
        expected_holding_hours: config.funding.expected_holding_hours,
        live_orders_enabled: false,
        open_bundles: 0,
        pending_orders: 0,
        notification_status: "provider_required",
    };
    SpotFuturesRuntimeContract {
        schema_version: 1,
        strategy_kind: STRATEGY_KIND,
        display_name: DISPLAY_NAME,
        migrated_from: MIGRATED_FROM,
        mode,
        live_orders_enabled_by_default: false,
        market_data_provider: provider("strategy_sdk_spot_futures_market_data_provider"),
        execution_provider: provider("strategy_sdk_execution_provider"),
        storage_provider: provider("strategy_app_storage_provider"),
        dashboard_snapshot_provider: provider("strategy_snapshot_provider"),
        notification_provider: provider("strategy_notification_provider"),
        tasks: vec![
            task(
                "load_spot_and_perp_symbol_rules",
                "strategy_runtime_core",
                true,
            ),
            task(
                "observe_spot_books",
                "strategy_sdk_market_data_provider",
                true,
            ),
            task(
                "observe_perp_books",
                "strategy_sdk_market_data_provider",
                true,
            ),
            task(
                "refresh_funding_snapshots",
                "strategy_sdk_market_data_provider",
                true,
            ),
            task(
                "evaluate_spot_futures_opportunities",
                "strategy_runtime_core",
                true,
            ),
            task(
                "submit_spot_maker_open",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "submit_perp_taker_hedge_after_spot_fill",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "submit_dual_taker_reduce_only_close",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "reconcile_spot_balances_and_perp_positions",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "persist_spot_futures_events",
                "strategy_app_storage_provider",
                false,
            ),
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

pub fn default_runtime_contract(captured_at: DateTime<Utc>) -> SpotFuturesRuntimeContract {
    build_runtime_contract(&SpotFuturesArbitrageConfig::default(), captured_at)
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
