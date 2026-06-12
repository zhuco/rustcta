use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{
    CrossArbExecutionModule, CrossExchangeArbitrageConfig, DISPLAY_NAME, MIGRATED_FROM,
    STRATEGY_KIND,
};

pub const DEFAULT_ASYNC_DB_QUEUE_EVENTS: usize = 16_384;

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
pub struct CrossArbAsyncDbWriterContract {
    pub required: bool,
    pub provider_boundary: &'static str,
    pub writer_ready: bool,
    pub queue_model: &'static str,
    pub max_queue_events: usize,
    pub enqueue_from_strategy_thread_must_be_non_blocking: bool,
    pub background_writer_required: bool,
    pub blocks_opportunity_evaluation_until_ready: bool,
    pub records_planned_execution_prices: bool,
    pub records_actual_fill_prices: bool,
    pub records_execution_failure_reason: bool,
    pub records_open_decision_reject_reason: bool,
    pub records_open_decision_auth_evidence: bool,
    pub price_audit_fields: Vec<&'static str>,
    pub overflow_policy: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbAsyncDbQueueState {
    pub writer_ready: bool,
    pub non_blocking_enqueue: bool,
    pub queued_events: usize,
    pub max_queue_events: usize,
    pub dropped_events: usize,
    pub last_enqueue_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbPositionTakeoverContract {
    pub required_before_new_open: bool,
    pub quote_asset: &'static str,
    pub scope: &'static str,
    pub hedge_position_mode_required: bool,
    pub takeover_complete: bool,
    pub single_leg_positions_detected: usize,
    pub single_leg_positions_resolved: usize,
    pub blocks_new_opens_until_complete: bool,
    pub single_leg_resolution_policy: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbSingletonProcessLockContract {
    pub required: bool,
    pub lock_kind: &'static str,
    pub lock_scope: &'static str,
    pub acquired: bool,
    pub duplicate_process_policy: &'static str,
    pub blocks_startup_until_acquired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbExecutionProviderRuntimeContract {
    pub provider_boundary: &'static str,
    pub taker_only: bool,
    pub supports_slippage_capture_maker_open: bool,
    pub hedge_position_mode_required: bool,
    pub open_legs_concurrent_required: bool,
    pub maker_open_then_taker_hedge_required: bool,
    pub close_legs_concurrent_required: bool,
    pub waits_for_both_leg_reports: bool,
    pub must_return_planned_and_actual_prices: bool,
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
pub struct CrossArbSymbolUniverseSummary {
    pub active_symbol_count: usize,
    pub excluded_bases: Vec<String>,
    pub excluded_symbols: Vec<String>,
    pub high_volatility_enabled: bool,
    pub high_volatility_source: &'static str,
    pub high_volatility_top_gainers_per_exchange: usize,
    pub high_volatility_top_losers_per_exchange: usize,
    pub high_volatility_min_abs_change_pct: String,
    pub high_volatility_min_quote_volume_usdt: String,
    pub high_volatility_refresh_secs: u64,
    pub high_volatility_max_dynamic_symbols: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbExchangeReadinessRow {
    pub exchange: String,
    pub expected_symbol_count: usize,
    pub market_data_streamed_symbols: usize,
    pub market_data_stream_ready: bool,
    pub user_stream_ready: bool,
    pub server_time_synced: bool,
    pub precision_rules_ready: bool,
    pub ready: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbStartupReadinessGate {
    pub gate_enforced: bool,
    pub ready_to_evaluate_opportunities: bool,
    pub ready_to_open_new_positions: bool,
    pub db_writer_ready: bool,
    pub position_takeover_complete: bool,
    pub startup_single_leg_positions_resolved: bool,
    pub singleton_acquired: bool,
    pub required_gates: Vec<&'static str>,
    pub exchanges: Vec<CrossArbExchangeReadinessRow>,
}

impl CrossArbStartupReadinessGate {
    pub fn exchange_gates_ready(&self) -> bool {
        !self.exchanges.is_empty() && self.exchanges.iter().all(|exchange| exchange.ready)
    }

    pub fn runtime_gates_ready(&self) -> bool {
        self.db_writer_ready && self.position_takeover_complete && self.singleton_acquired
    }

    pub fn all_gates_ready(&self) -> bool {
        self.exchange_gates_ready() && self.runtime_gates_ready()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbDualTakerConfigSummary {
    pub target_notional_usdt: String,
    pub min_open_spread_pct: String,
    pub max_open_spread_pct: String,
    pub close_min_net_profit_pct: String,
    pub expected_close_spread_pct: String,
    pub top_of_book_capacity_ratio: String,
    pub taker_slippage_pct: String,
    pub fastest_orderbook_interval_ms: u64,
    pub orderbook_stale_ms: u64,
    pub min_orderbook_levels: usize,
    pub single_leg_timeout_ms: u64,
    pub max_consecutive_single_leg_fills: u32,
    pub max_open_bundles: usize,
    pub max_positions_per_exchange: usize,
    pub max_active_bundles_per_symbol: usize,
    pub symbol_cooldown_secs: i64,
    pub max_hold_secs: i64,
    pub close_on_max_hold_requires_profit: bool,
    pub open_order_role: &'static str,
    pub close_order_role: &'static str,
    pub parallel_order_submit_required: bool,
    pub shared_base_quantity_required: bool,
    pub emergency_close_on_single_leg: bool,
    pub stop_after_max_single_leg_fills: bool,
    pub round_trip_fee_legs_in_pnl: u8,
    pub net_position_inspection_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbSlippageCaptureConfigSummary {
    pub enabled_when_selected: bool,
    pub target_notional_usdt: String,
    pub min_open_spread_pct: String,
    pub max_open_spread_pct: String,
    pub min_open_net_profit_pct: String,
    pub startup_skip_spread_secs: i64,
    pub max_signal_age_ms: u64,
    pub maker_price_offset_pct: String,
    pub hedge_taker_slippage_pct: String,
    pub close_taker_slippage_pct: String,
    pub maker_order_timeout_ms: u64,
    pub cancel_unfilled_maker: bool,
    pub max_maker_top_depth_usdt: String,
    pub hedge_top_of_book_capacity_ratio: String,
    pub enforce_hedge_top_depth: bool,
    pub risk_flatten_grace_secs: i64,
    pub risk_flatten_close_min_net_profit_pct: String,
    pub risk_flatten_hedge_min_net_profit_pct: String,
    pub open_order_role: &'static str,
    pub hedge_order_role: &'static str,
    pub close_order_role: &'static str,
    pub hedge_after_fill_required: bool,
    pub dual_taker_close_required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbExchangeStatusRow {
    pub exchange: String,
    pub streamed_symbol_count: usize,
    pub message_count: u64,
    pub last_latency_ms: Option<u64>,
    pub max_latency_ms: Option<u64>,
    pub server_time_offset_ms: Option<i64>,
    pub disconnect_count: u64,
    pub last_message_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CrossArbLogRotationPolicy {
    pub key_only: bool,
    pub persist_scope: &'static str,
    pub max_file_bytes: u64,
    pub max_file_megabytes: String,
    pub retained_files: usize,
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
    pub symbol_universe: CrossArbSymbolUniverseSummary,
    pub readiness_gate: CrossArbStartupReadinessGate,
    pub execution_module: CrossArbExecutionModule,
    pub dual_taker: CrossArbDualTakerConfigSummary,
    pub slippage_capture: CrossArbSlippageCaptureConfigSummary,
    pub live_orders_enabled: bool,
    pub open_bundles: usize,
    pub pending_orders: usize,
    pub notification_status: &'static str,
    pub market_snapshots: Vec<CrossArbMarketSnapshotRow>,
    pub opportunities: Vec<CrossArbOpportunityRow>,
    pub route_health: Vec<CrossArbRouteHealthRow>,
    pub exchange_status: Vec<CrossArbExchangeStatusRow>,
    pub logging_policy: CrossArbLogRotationPolicy,
    pub async_db_writer: CrossArbAsyncDbWriterContract,
    pub async_db_queue: CrossArbAsyncDbQueueState,
    pub position_takeover: CrossArbPositionTakeoverContract,
    pub singleton_process_lock: CrossArbSingletonProcessLockContract,
    pub execution_provider_contract: CrossArbExecutionProviderRuntimeContract,
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
    pub symbol_universe: CrossArbSymbolUniverseSummary,
    pub readiness_gate: CrossArbStartupReadinessGate,
    pub execution_module: CrossArbExecutionModule,
    pub dual_taker: CrossArbDualTakerConfigSummary,
    pub slippage_capture: CrossArbSlippageCaptureConfigSummary,
    pub async_db_writer: CrossArbAsyncDbWriterContract,
    pub position_takeover: CrossArbPositionTakeoverContract,
    pub singleton_process_lock: CrossArbSingletonProcessLockContract,
    pub execution_provider_contract: CrossArbExecutionProviderRuntimeContract,
    pub exchange_status_fields: Vec<&'static str>,
    pub logging_policy: CrossArbLogRotationPolicy,
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
    let venues = config.active_venues();
    let symbols = config.active_symbols();
    let symbol_universe = symbol_universe_summary(config, &symbols);
    let readiness_gate = readiness_gate(&venues, symbols.len());
    let dual_taker = dual_taker_summary(config);
    let slippage_capture = slippage_capture_summary(config);
    let logging_policy = logging_policy(config);
    let async_db_writer = async_db_writer_contract(false, DEFAULT_ASYNC_DB_QUEUE_EVENTS);
    let async_db_queue = async_db_queue_state(false, 0, DEFAULT_ASYNC_DB_QUEUE_EVENTS, 0, None);
    let position_takeover = position_takeover_contract(false, 0, 0);
    let singleton_process_lock = singleton_process_lock_contract(false);
    let execution_provider_contract = execution_provider_contract(config);

    let dashboard_snapshot = CrossArbDashboardSnapshot {
        schema_version: 1,
        captured_at,
        strategy_kind: STRATEGY_KIND,
        migrated_from: MIGRATED_FROM,
        mode,
        venues: venues.clone(),
        symbols,
        symbol_universe: symbol_universe.clone(),
        readiness_gate: readiness_gate.clone(),
        execution_module: config.execution_module,
        dual_taker: dual_taker.clone(),
        slippage_capture: slippage_capture.clone(),
        live_orders_enabled: false,
        open_bundles: 0,
        pending_orders: 0,
        notification_status: "provider_required",
        market_snapshots: Vec::new(),
        opportunities: Vec::new(),
        route_health: Vec::new(),
        exchange_status: exchange_status_rows(&venues),
        logging_policy: logging_policy.clone(),
        async_db_writer: async_db_writer.clone(),
        async_db_queue,
        position_takeover: position_takeover.clone(),
        singleton_process_lock: singleton_process_lock.clone(),
        execution_provider_contract: execution_provider_contract.clone(),
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
        symbol_universe,
        readiness_gate,
        execution_module: config.execution_module,
        dual_taker,
        slippage_capture,
        async_db_writer,
        position_takeover,
        singleton_process_lock,
        execution_provider_contract,
        exchange_status_fields: exchange_status_fields(),
        logging_policy,
        tasks: vec![
            task(
                "acquire_singleton_process_lock",
                "strategy_runtime_core",
                true,
            ),
            task(
                "start_async_db_writer",
                "strategy_app_storage_provider",
                true,
            ),
            task(
                "observe_market_data",
                "strategy_sdk_market_data_provider",
                true,
            ),
            task(
                "observe_high_volatility_rankings",
                "strategy_sdk_market_data_provider",
                true,
            ),
            task(
                "refresh_dynamic_orderbook_subscriptions",
                "strategy_sdk_market_data_provider",
                true,
            ),
            task(
                "observe_user_stream",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "sync_server_time",
                "strategy_sdk_market_data_provider",
                true,
            ),
            task(
                "load_precision_rules",
                "strategy_sdk_market_data_provider",
                true,
            ),
            task(
                "takeover_all_usdt_positions",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "resolve_startup_single_leg_positions",
                "strategy_sdk_execution_provider",
                true,
            ),
            task("enforce_startup_readiness", "strategy_runtime_core", true),
            task("evaluate_opportunities", "strategy_runtime_core", true),
            task(
                "submit_dual_taker_orders",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "submit_dual_taker_open_legs_concurrently",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "submit_dual_taker_close_legs_concurrently",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "submit_slippage_capture_maker_open",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "cancel_unfilled_slippage_capture_maker_open",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "submit_slippage_capture_taker_hedge_after_fill",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "watch_single_leg_fills",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "inspect_net_positions",
                "strategy_sdk_execution_provider",
                true,
            ),
            task(
                "persist_open_fill_prices",
                "strategy_app_storage_provider",
                false,
            ),
            task(
                "persist_close_fill_prices_and_pnl",
                "strategy_app_storage_provider",
                false,
            ),
            task(
                "persist_price_audit_events_from_async_queue",
                "strategy_app_storage_provider",
                false,
            ),
            task(
                "drain_async_db_queue",
                "strategy_app_storage_provider",
                false,
            ),
            task("persist_key_events", "strategy_app_storage_provider", false),
            task("rotate_logs", "strategy_app_storage_provider", false),
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

fn symbol_universe_summary(
    config: &CrossExchangeArbitrageConfig,
    symbols: &[String],
) -> CrossArbSymbolUniverseSummary {
    let volatility = &config.volatility_universe;
    CrossArbSymbolUniverseSummary {
        active_symbol_count: symbols.len(),
        excluded_bases: config.excluded_bases.clone(),
        excluded_symbols: config.excluded_symbols.clone(),
        high_volatility_enabled: volatility.enabled,
        high_volatility_source: "exchange_24h_ticker_gainers_and_losers",
        high_volatility_top_gainers_per_exchange: volatility.top_gainers_per_exchange,
        high_volatility_top_losers_per_exchange: volatility.top_losers_per_exchange,
        high_volatility_min_abs_change_pct: percent_string(volatility.min_abs_change_pct),
        high_volatility_min_quote_volume_usdt: decimal_string(volatility.min_quote_volume_usdt),
        high_volatility_refresh_secs: volatility.refresh_secs,
        high_volatility_max_dynamic_symbols: volatility.max_dynamic_symbols,
    }
}

fn readiness_gate(venues: &[String], expected_symbol_count: usize) -> CrossArbStartupReadinessGate {
    CrossArbStartupReadinessGate {
        gate_enforced: true,
        ready_to_evaluate_opportunities: false,
        ready_to_open_new_positions: false,
        db_writer_ready: false,
        position_takeover_complete: false,
        startup_single_leg_positions_resolved: false,
        singleton_acquired: false,
        required_gates: vec![
            "market_data_stream",
            "user_stream",
            "server_time_sync",
            "precision_rules",
            "async_db_writer",
            "position_takeover",
            "singleton_process_lock",
        ],
        exchanges: venues
            .iter()
            .map(|exchange| CrossArbExchangeReadinessRow {
                exchange: exchange.clone(),
                expected_symbol_count,
                market_data_streamed_symbols: 0,
                market_data_stream_ready: false,
                user_stream_ready: false,
                server_time_synced: false,
                precision_rules_ready: false,
                ready: false,
            })
            .collect(),
    }
}

pub fn async_db_writer_contract(
    writer_ready: bool,
    max_queue_events: usize,
) -> CrossArbAsyncDbWriterContract {
    CrossArbAsyncDbWriterContract {
        required: true,
        provider_boundary: "strategy_app_storage_provider",
        writer_ready,
        queue_model: "non_blocking_in_memory_queue_drained_by_background_writer",
        max_queue_events,
        enqueue_from_strategy_thread_must_be_non_blocking: true,
        background_writer_required: true,
        blocks_opportunity_evaluation_until_ready: true,
        records_planned_execution_prices: true,
        records_actual_fill_prices: true,
        records_execution_failure_reason: true,
        records_open_decision_reject_reason: true,
        records_open_decision_auth_evidence: true,
        price_audit_fields: vec![
            "opportunity_id",
            "accepted",
            "reject_reason",
            "raw_open_spread_bps",
            "expected_net_profit_pct",
            "expected_net_pnl_usdt",
            "long_book_age_ms",
            "short_book_age_ms",
            "executable_top_depth_usdt",
            "target_notional_usdt",
            "auth_evidence",
            "bundle_id",
            "lifecycle",
            "exchange",
            "symbol",
            "side",
            "position_side",
            "planned_execution_price",
            "actual_fill_price",
            "planned_base_quantity",
            "actual_base_quantity",
            "planned_notional_usdt",
            "actual_notional_usdt",
            "fee_usdt",
            "order_id",
            "client_order_id",
            "failure_reason",
        ],
        overflow_policy: "drop_new_event_and_emit_operator_warning",
    }
}

pub fn async_db_queue_state(
    writer_ready: bool,
    queued_events: usize,
    max_queue_events: usize,
    dropped_events: usize,
    last_enqueue_at: Option<DateTime<Utc>>,
) -> CrossArbAsyncDbQueueState {
    CrossArbAsyncDbQueueState {
        writer_ready,
        non_blocking_enqueue: true,
        queued_events,
        max_queue_events,
        dropped_events,
        last_enqueue_at,
    }
}

pub fn position_takeover_contract(
    takeover_complete: bool,
    single_leg_positions_detected: usize,
    single_leg_positions_resolved: usize,
) -> CrossArbPositionTakeoverContract {
    CrossArbPositionTakeoverContract {
        required_before_new_open: true,
        quote_asset: "USDT",
        scope: "all_usdt_positions_across_configured_accounts_and_exchanges",
        hedge_position_mode_required: true,
        takeover_complete,
        single_leg_positions_detected,
        single_leg_positions_resolved,
        blocks_new_opens_until_complete: true,
        single_leg_resolution_policy: "market_reduce_only_close_before_new_open",
    }
}

pub fn singleton_process_lock_contract(acquired: bool) -> CrossArbSingletonProcessLockContract {
    CrossArbSingletonProcessLockContract {
        required: true,
        lock_kind: "strategy_process_singleton",
        lock_scope: "cross_exchange_arbitrage_usdt",
        acquired,
        duplicate_process_policy: "abort_startup_when_existing_live_lock_is_present",
        blocks_startup_until_acquired: true,
    }
}

pub fn execution_provider_contract(
    config: &CrossExchangeArbitrageConfig,
) -> CrossArbExecutionProviderRuntimeContract {
    let slippage_capture = config.execution_module == CrossArbExecutionModule::SlippageCapture;
    CrossArbExecutionProviderRuntimeContract {
        provider_boundary: "strategy_sdk_execution_provider",
        taker_only: !slippage_capture,
        supports_slippage_capture_maker_open: true,
        hedge_position_mode_required: true,
        open_legs_concurrent_required: !slippage_capture,
        maker_open_then_taker_hedge_required: slippage_capture,
        close_legs_concurrent_required: true,
        waits_for_both_leg_reports: true,
        must_return_planned_and_actual_prices: true,
    }
}

fn dual_taker_summary(config: &CrossExchangeArbitrageConfig) -> CrossArbDualTakerConfigSummary {
    let dual_taker = config.dual_taker;
    CrossArbDualTakerConfigSummary {
        target_notional_usdt: decimal_string(dual_taker.target_notional_usdt),
        min_open_spread_pct: percent_string(dual_taker.min_open_spread_pct),
        max_open_spread_pct: percent_string(dual_taker.max_open_spread_pct),
        close_min_net_profit_pct: percent_string(dual_taker.close_min_net_profit_pct),
        expected_close_spread_pct: percent_string(dual_taker.expected_close_spread_pct),
        top_of_book_capacity_ratio: percent_string(dual_taker.top_of_book_capacity_ratio),
        taker_slippage_pct: percent_string(dual_taker.taker_slippage_pct),
        fastest_orderbook_interval_ms: 10,
        orderbook_stale_ms: dual_taker.orderbook_stale_ms,
        min_orderbook_levels: dual_taker.min_orderbook_levels,
        single_leg_timeout_ms: dual_taker.single_leg_timeout_ms,
        max_consecutive_single_leg_fills: dual_taker.max_consecutive_single_leg_fills,
        max_open_bundles: dual_taker.max_open_bundles,
        max_positions_per_exchange: dual_taker.max_positions_per_exchange,
        max_active_bundles_per_symbol: dual_taker.max_active_bundles_per_symbol,
        symbol_cooldown_secs: dual_taker.symbol_cooldown_secs,
        max_hold_secs: dual_taker.max_hold_secs,
        close_on_max_hold_requires_profit: dual_taker.close_on_max_hold_requires_profit,
        open_order_role: "taker",
        close_order_role: "taker",
        parallel_order_submit_required: true,
        shared_base_quantity_required: true,
        emergency_close_on_single_leg: true,
        stop_after_max_single_leg_fills: true,
        round_trip_fee_legs_in_pnl: 4,
        net_position_inspection_enabled: true,
    }
}

fn slippage_capture_summary(
    config: &CrossExchangeArbitrageConfig,
) -> CrossArbSlippageCaptureConfigSummary {
    let slippage = config.slippage_capture;
    CrossArbSlippageCaptureConfigSummary {
        enabled_when_selected: config.execution_module == CrossArbExecutionModule::SlippageCapture,
        target_notional_usdt: decimal_string(slippage.target_notional_usdt),
        min_open_spread_pct: percent_string(slippage.min_open_spread_pct),
        max_open_spread_pct: percent_string(slippage.max_open_spread_pct),
        min_open_net_profit_pct: percent_string(slippage.min_open_net_profit_pct),
        startup_skip_spread_secs: slippage.startup_skip_spread_secs,
        max_signal_age_ms: slippage.max_signal_age_ms,
        maker_price_offset_pct: percent_string(slippage.maker_price_offset_pct),
        hedge_taker_slippage_pct: percent_string(slippage.hedge_taker_slippage_pct),
        close_taker_slippage_pct: percent_string(slippage.close_taker_slippage_pct),
        maker_order_timeout_ms: slippage.maker_order_timeout_ms,
        cancel_unfilled_maker: slippage.cancel_unfilled_maker,
        max_maker_top_depth_usdt: decimal_string(slippage.max_maker_top_depth_usdt),
        hedge_top_of_book_capacity_ratio: percent_string(slippage.hedge_top_of_book_capacity_ratio),
        enforce_hedge_top_depth: slippage.enforce_hedge_top_depth,
        risk_flatten_grace_secs: slippage.risk_flatten_grace_secs,
        risk_flatten_close_min_net_profit_pct: percent_string(
            slippage.risk_flatten_close_min_net_profit_pct,
        ),
        risk_flatten_hedge_min_net_profit_pct: percent_string(
            slippage.risk_flatten_hedge_min_net_profit_pct,
        ),
        open_order_role: "maker_limit_capture",
        hedge_order_role: "taker_ioc_after_maker_fill",
        close_order_role: "dual_taker_reduce_only",
        hedge_after_fill_required: true,
        dual_taker_close_required: true,
    }
}

fn exchange_status_rows(venues: &[String]) -> Vec<CrossArbExchangeStatusRow> {
    venues
        .iter()
        .map(|exchange| CrossArbExchangeStatusRow {
            exchange: exchange.clone(),
            streamed_symbol_count: 0,
            message_count: 0,
            last_latency_ms: None,
            max_latency_ms: None,
            server_time_offset_ms: None,
            disconnect_count: 0,
            last_message_at: None,
        })
        .collect()
}

fn exchange_status_fields() -> Vec<&'static str> {
    vec![
        "exchange",
        "streamed_symbol_count",
        "message_count",
        "last_latency_ms",
        "max_latency_ms",
        "server_time_offset_ms",
        "disconnect_count",
        "last_message_at",
    ]
}

fn logging_policy(config: &CrossExchangeArbitrageConfig) -> CrossArbLogRotationPolicy {
    CrossArbLogRotationPolicy {
        key_only: config.logging.persist_only_key_events,
        persist_scope: if config.logging.persist_only_key_events {
            "key_events_only"
        } else {
            "all_events"
        },
        max_file_bytes: config.logging.max_file_bytes,
        max_file_megabytes: decimal_string(config.logging.max_file_bytes as f64 / 1_048_576.0),
        retained_files: config.logging.retained_files,
    }
}

fn percent_string(value: f64) -> String {
    decimal_string(value * 100.0)
}

fn decimal_string(value: f64) -> String {
    let mut rendered = format!("{value:.8}");
    while rendered.contains('.') && rendered.ends_with('0') {
        rendered.pop();
    }
    if rendered.ends_with('.') {
        rendered.pop();
    }
    if rendered == "-0" {
        "0".to_string()
    } else {
        rendered
    }
}
