use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path as FsPath, PathBuf};

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use clap::Parser;
use futures_util::stream;
use rustcta::control::spot_control::{
    normalize_exchange_list, DisableMode, DisableSymbolRequest, EnableMode, EnableSymbolRequest,
    EnabledDirection, SpotControlReadModel, SpotControlService, SpotControlSnapshotBuilder,
    SpotSymbolControlConfig, SpotSymbolLifecycleState, VersionedSymbolRequest,
};
use rustcta::cta::AccountManagerConfigFile;
use rustcta::exchanges::config::ExchangeRuntimeSettings;
use rustcta::exchanges::registry as exchange_registry;
use rustcta::market::InstrumentMeta;
use rustcta::risk::{DisabledExchange, DisabledRegistryConfig};
use rustcta::strategies::cross_exchange_arbitrage::{
    ArbitrageMarketSnapshotRecord, CanonicalSymbol, CrossArbStorageEvent,
    CrossExchangeArbitrageConfig, ExchangeId, ExchangeOperatingMode, StoredCrossArbEvent,
};
use rustcta::strategies::funding_rate_arbitrage::FundingRateArbitrageConfig;
use rustcta::strategies::spot_spot_taker_arbitrage::SpotSpotTakerArbitrageConfig;
use rustcta::web::{
    status_from_model, ConfigSummaryView, DashboardReadModel, DisabledExchangeSymbolView,
    DisabledExchangeView, DisabledSymbolView, DisabledView, InventoryView, TradeView,
};
use rustcta_supervisor::{
    JsonFileProcessRegistryStore, LifecycleCommand, LifecycleCommandRecord, LocalProcessSupervisor,
    ProcessRegistry, ProcessStatus, StrategyProcess, StrategyProcessSpec,
    SUPERVISOR_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::{ServeDir, ServeFile};
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "control-api", version, about = "Separated RustCTA control API")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8091")]
    bind_addr: String,
    #[arg(
        long,
        default_value = "data/control_api/dashboard_snapshot.json",
        help = "Sanitized DashboardReadModel JSON written by the strategy runtime"
    )]
    snapshot_path: PathBuf,
    #[arg(
        long,
        default_value = "data/control_api/control_commands.jsonl",
        help = "Authenticated operator command queue; read by approved runtime command channels"
    )]
    command_path: PathBuf,
    #[arg(long, default_value = "RUSTCTA_MONITOR_TOKEN")]
    token_env: String,
    #[arg(long, default_value = "web-ui/dioxus/dist")]
    static_dir: PathBuf,
    #[arg(
        long,
        default_value = "data/control_api/exchange_api_keys.env",
        help = "Local env file used by the web console to persist exchange API credentials"
    )]
    exchange_api_key_store: PathBuf,
    #[arg(
        long,
        default_value = "config/accounts.yml",
        help = "Central account-manager YAML used by the web console and runtime account manager"
    )]
    accounts_config: PathBuf,
    #[arg(
        long,
        default_value = "data/control_api/balance_history.json",
        help = "Persistent account balance history served to the web console"
    )]
    balance_history_path: PathBuf,
    #[arg(
        long,
        default_value = "data/control_api/strategy_profit_history.json",
        help = "Persistent per-arbitrage cumulative profit history served to the web console"
    )]
    strategy_profit_history_path: PathBuf,
    #[arg(
        long,
        default_value_t = 600_000,
        help = "Balance history sampling interval in milliseconds"
    )]
    balance_history_interval_ms: u64,
    #[arg(
        long,
        default_value = "config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml",
        help = "Strategy YAML edited by the web console"
    )]
    strategy_config: PathBuf,
    #[arg(
        long,
        default_value = "data/control_api/strategy_registry.json",
        help = "Local registry used by the web console to manage multiple strategy processes"
    )]
    strategy_registry: PathBuf,
    #[arg(
        long,
        default_value = "data/control_api/strategy_specs.json",
        help = "Local strategy launch specs used by the web console to restart managed strategies"
    )]
    strategy_specs: PathBuf,
    #[arg(
        long,
        default_value = "scripts/separated_control_panel.sh",
        help = "Local helper script used to restart the strategy after config edits"
    )]
    restart_script: PathBuf,
    #[arg(long, default_value_t = 1000)]
    event_interval_ms: u64,
    #[arg(
        long,
        default_value_t = false,
        help = "Required to bind control_api to a non-loopback address"
    )]
    external_access_enabled: bool,
}

#[derive(Clone)]
struct AppState {
    snapshot_path: PathBuf,
    command_path: PathBuf,
    exchange_api_key_store: PathBuf,
    accounts_config: PathBuf,
    balance_history_path: PathBuf,
    strategy_profit_history_path: PathBuf,
    strategy_config: PathBuf,
    strategy_registry: PathBuf,
    strategy_specs: PathBuf,
    restart_script: PathBuf,
    token_env: String,
    event_interval_ms: u64,
    balance_history_interval_ms: u64,
    supervisor: Arc<RwLock<LocalProcessSupervisor>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlApiCommand {
    timestamp: DateTime<Utc>,
    command_id: String,
    command_type: String,
    requested_by: String,
    applied_to_runtime: bool,
    would_submit_order: bool,
    details: Value,
    warning: String,
}

#[derive(Debug, Clone, Serialize)]
struct ExchangeApiKeyFieldStatus {
    field: String,
    label: String,
    required: bool,
    configured: bool,
    source: Option<String>,
    masked: Option<String>,
    value: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ExchangeApiKeyExchangeStatus {
    exchange: String,
    label: String,
    enabled: bool,
    account_id: String,
    account_label: String,
    credential_namespace: String,
    is_default_account: bool,
    fields: Vec<ExchangeApiKeyFieldStatus>,
}

#[derive(Debug, Clone, Serialize)]
struct AccountManagerAccountStatus {
    account_id: String,
    name: String,
    exchange: String,
    account_type: String,
    description: String,
    env_prefix: String,
    credential_namespace: String,
    enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ExchangeApiKeyStatusResponse {
    store_path: String,
    restart_required: bool,
    enabled_exchanges: Vec<String>,
    account_manager_accounts: Vec<AccountManagerAccountStatus>,
    supported_exchanges: Vec<ExchangeApiKeyExchangeStatus>,
    exchanges: Vec<ExchangeApiKeyExchangeStatus>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkspaceSummaryResponse {
    schema_version: u16,
    generated_at: DateTime<Utc>,
    strategy_count: usize,
    process_count: usize,
    agent_count: usize,
    risk_status: String,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyProcessView {
    schema_version: u16,
    strategy_id: String,
    strategy_kind: String,
    run_id: String,
    tenant_id: String,
    config_path: String,
    status: ProcessStatus,
    process_id: Option<u32>,
    started_at: Option<DateTime<Utc>>,
    last_heartbeat_at: Option<DateTime<Utc>>,
    last_snapshot_at: Option<DateTime<Utc>>,
    restart_count: u32,
    last_exit_code: Option<i32>,
    last_error: Option<String>,
    log_configured: bool,
    log_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct CreateStrategyRequest {
    strategy_id: String,
    strategy_kind: String,
    #[serde(default)]
    run_id: Option<String>,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    config_path: String,
    #[serde(default)]
    log_path: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    working_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StrategySpecsStore {
    schema_version: u16,
    updated_at: DateTime<Utc>,
    specs: Vec<StrategyProcessSpec>,
}

#[derive(Debug, Clone)]
struct ManagedStrategyPreset {
    strategy_id: &'static str,
    strategy_kind: &'static str,
    config_path: String,
    log_path: &'static str,
    command: &'static str,
    args: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ExchangeApiKeyUpdateRequest {
    exchange: String,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    account_label: Option<String>,
    #[serde(default)]
    credential_namespace: Option<String>,
    #[serde(default)]
    exchange_account_id: Option<String>,
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    api_secret: Option<String>,
    #[serde(default)]
    passphrase: Option<String>,
    #[serde(default)]
    clear: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct StrategyConfigUpdateRequest {
    content: String,
    #[serde(default = "default_true_bool")]
    restart: bool,
}

#[derive(Debug, Clone, Serialize)]
struct CrossArbExchangeSettingsView {
    exchange: String,
    enabled: bool,
    operating_mode: String,
    account_id: String,
    env_prefix: String,
    private_rest_enabled: bool,
    private_ws_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
struct CrossArbSettingsView {
    path: String,
    config_kind: String,
    enabled_exchanges: Vec<String>,
    exchanges: Vec<CrossArbExchangeSettingsView>,
    symbols: Vec<String>,
    target_symbol_count: usize,
    max_symbol_count: usize,
    min_common_exchanges: usize,
    min_notional_usdt: f64,
    target_notional_usdt: f64,
    max_notional_usdt: f64,
    max_positions_per_exchange: usize,
    max_open_bundles: usize,
    max_open_positions: usize,
    max_symbol_notional_usdt: f64,
    max_notional_per_symbol_usdt: f64,
    max_notional_per_exchange_usdt: f64,
    max_total_notional_usdt: f64,
    min_open_raw_spread: f64,
    min_open_maker_taker_net_edge: f64,
    max_open_raw_spread: f64,
    lock_profit_dual_taker_pct: f64,
    max_close_spread_pct: f64,
    execution_dry_run: bool,
    trading_mode: String,
    mode: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct CrossArbExchangeSettingsRequest {
    exchange: String,
    enabled: bool,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    env_prefix: Option<String>,
    #[serde(default)]
    private_rest_enabled: Option<bool>,
    #[serde(default)]
    private_ws_enabled: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct CrossArbSettingsUpdateRequest {
    #[serde(default)]
    execution_profile: Option<String>,
    #[serde(default)]
    exchanges: Vec<CrossArbExchangeSettingsRequest>,
    #[serde(default)]
    symbols: Vec<String>,
    #[serde(default)]
    target_symbol_count: Option<usize>,
    #[serde(default)]
    min_notional_usdt: Option<f64>,
    #[serde(default)]
    target_notional_usdt: Option<f64>,
    #[serde(default)]
    max_notional_usdt: Option<f64>,
    #[serde(default)]
    max_positions_per_exchange: Option<usize>,
    #[serde(default)]
    max_open_bundles: Option<usize>,
    #[serde(default)]
    max_open_positions: Option<usize>,
    #[serde(default)]
    max_symbol_notional_usdt: Option<f64>,
    #[serde(default)]
    max_notional_per_symbol_usdt: Option<f64>,
    #[serde(default)]
    max_notional_per_exchange_usdt: Option<f64>,
    #[serde(default)]
    max_total_notional_usdt: Option<f64>,
    #[serde(default)]
    min_open_raw_spread: Option<f64>,
    #[serde(default)]
    min_open_maker_taker_net_edge: Option<f64>,
    #[serde(default)]
    max_open_raw_spread: Option<f64>,
    #[serde(default)]
    lock_profit_dual_taker_pct: Option<f64>,
    #[serde(default)]
    max_close_spread_pct: Option<f64>,
    #[serde(default = "default_true_bool")]
    restart: bool,
}

#[derive(Debug, Clone, Serialize)]
struct FundingArbExchangeSettingsView {
    exchange: String,
    enabled: bool,
    account_id: String,
    env_prefix: String,
    demo_trading: bool,
    private_ws_enabled: bool,
    position_mode: String,
}

#[derive(Debug, Clone, Serialize)]
struct FundingArbSettingsView {
    path: String,
    strategy_id: String,
    config_kind: String,
    mode: String,
    enabled_exchanges: Vec<String>,
    quote_asset: String,
    contract_type: String,
    symbol_allowlist: Vec<String>,
    symbol_blocklist: Vec<String>,
    per_exchange_limit: usize,
    min_funding_rate: f64,
    require_next_funding_time: bool,
    max_funding_snapshot_age_ms: i64,
    min_seconds_to_settlement_at_scan: Option<i64>,
    max_seconds_to_settlement_at_scan: Option<i64>,
    scan_minute: u32,
    notional_usdt: f64,
    open_seconds_before_settlement: i64,
    close_seconds_after_settlement: i64,
    order_readback_delay_secs: u64,
    close_limit_timeout_secs: u64,
    close_limit_max_retries: u32,
    max_slippage_pct: f64,
    allow_existing_symbol_position: bool,
    position_side: String,
    exchanges: Vec<FundingArbExchangeSettingsView>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct FundingArbExchangeSettingsRequest {
    exchange: String,
    enabled: bool,
    #[serde(default)]
    account_id: Option<String>,
    #[serde(default)]
    env_prefix: Option<String>,
    #[serde(default)]
    demo_trading: Option<bool>,
    #[serde(default)]
    private_ws_enabled: Option<bool>,
    #[serde(default)]
    position_mode: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct FundingArbSettingsUpdateRequest {
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    enabled_exchanges: Option<Vec<String>>,
    #[serde(default)]
    symbol_allowlist: Option<Vec<String>>,
    #[serde(default)]
    symbol_blocklist: Option<Vec<String>>,
    #[serde(default)]
    per_exchange_limit: Option<usize>,
    #[serde(default)]
    min_funding_rate: Option<f64>,
    #[serde(default)]
    require_next_funding_time: Option<bool>,
    #[serde(default)]
    max_funding_snapshot_age_ms: Option<i64>,
    #[serde(default)]
    min_seconds_to_settlement_at_scan: Option<i64>,
    #[serde(default)]
    max_seconds_to_settlement_at_scan: Option<i64>,
    #[serde(default)]
    scan_minute: Option<u32>,
    #[serde(default)]
    notional_usdt: Option<f64>,
    #[serde(default)]
    open_seconds_before_settlement: Option<i64>,
    #[serde(default)]
    close_seconds_after_settlement: Option<i64>,
    #[serde(default)]
    order_readback_delay_secs: Option<u64>,
    #[serde(default)]
    close_limit_timeout_secs: Option<u64>,
    #[serde(default)]
    close_limit_max_retries: Option<u32>,
    #[serde(default)]
    max_slippage_pct: Option<f64>,
    #[serde(default)]
    allow_existing_symbol_position: Option<bool>,
    #[serde(default)]
    position_side: Option<String>,
    #[serde(default)]
    exchanges: Vec<FundingArbExchangeSettingsRequest>,
    #[serde(default = "default_true_bool")]
    restart: bool,
}

#[derive(Debug, Default)]
struct BalanceHistoryExchangeTotal {
    total_usdt: f64,
    available_usdt: f64,
    asset_count: usize,
}

fn default_true_bool() -> bool {
    true
}

#[derive(Debug, Clone, Copy)]
struct ExchangeApiFieldSchema {
    field: &'static str,
    label: &'static str,
    aliases: &'static [&'static str],
    required: bool,
}

#[derive(Debug, Clone, Copy)]
struct ExchangeApiKeySchema {
    exchange: &'static str,
    label: &'static str,
    fields: &'static [ExchangeApiFieldSchema],
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    rustcta::utils::init_tracing_logger("info");
    let args = Args::parse();
    let supervisor = Arc::new(RwLock::new(load_legacy_supervisor(
        &args.strategy_registry,
        &args.strategy_specs,
        &args.strategy_config,
    )?));
    let state = AppState {
        snapshot_path: args.snapshot_path,
        command_path: args.command_path,
        exchange_api_key_store: args.exchange_api_key_store,
        accounts_config: args.accounts_config,
        balance_history_path: args.balance_history_path,
        strategy_profit_history_path: args.strategy_profit_history_path,
        strategy_config: args.strategy_config,
        strategy_registry: args.strategy_registry,
        strategy_specs: args.strategy_specs,
        restart_script: args.restart_script,
        token_env: args.token_env,
        event_interval_ms: args.event_interval_ms.max(50),
        balance_history_interval_ms: args.balance_history_interval_ms.max(60_000),
        supervisor,
    };
    let addr: SocketAddr = args
        .bind_addr
        .parse()
        .with_context(|| format!("invalid control-api bind_addr {}", args.bind_addr))?;
    validate_bind_safety(addr, &state.token_env, args.external_access_enabled)?;
    tokio::spawn(balance_history_writer(state.clone()));
    let app = router(state, args.static_dir);
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind control-api on {addr}"))?;
    log::info!("control-api listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .await
        .context("control-api failed")
}

fn router(state: AppState, static_dir: PathBuf) -> Router {
    let index = static_dir.join("index.html");
    Router::new()
        .route("/api/status", get(status))
        .route("/api/config", get(config))
        .route("/api/config/summary", get(config))
        .route("/api/workspace", get(workspace_summary))
        .route("/api/strategies", get(strategies).post(create_strategy))
        .route("/api/strategies/:id", get(strategy_detail))
        .route(
            "/api/strategies/:id/config",
            get(strategy_config_by_id).post(update_strategy_config_by_id),
        )
        .route("/api/strategies/:id/command", post(strategy_command))
        .route("/api/processes", get(processes))
        .route(
            "/api/strategy-config",
            get(strategy_config).post(update_strategy_config),
        )
        .route(
            "/api/cross-arb/settings",
            get(cross_arb_settings).post(update_cross_arb_settings),
        )
        .route(
            "/api/funding-arb/settings",
            get(funding_arb_settings).post(update_funding_arb_settings),
        )
        .route("/api/cross-arb/instruments", get(cross_arb_instruments))
        .route(
            "/api/cross-arb/market-snapshots",
            get(cross_arb_market_snapshots),
        )
        .route("/api/exchanges", get(exchanges))
        .route("/api/symbols", get(symbols))
        .route("/api/books", get(books))
        .route("/api/opportunities", get(opportunities))
        .route("/api/opportunities/recent", get(recent_opportunities))
        .route("/api/trades/recent", get(recent_trades))
        .route("/api/inventory", get(inventory))
        .route("/api/balance-history", get(balance_history))
        .route("/api/fees", get(fees))
        .route("/api/disabled", get(disabled))
        .route("/api/unmanaged_positions", get(unmanaged_positions))
        .route("/api/dry_run_plans", get(dry_run_plans))
        .route("/api/live_dry_run/orders", get(dry_run_plans))
        .route("/api/risk", get(risk))
        .route("/api/risk/events", get(risk_events))
        .route("/api/recorder", get(recorder))
        .route("/api/live_preflight", get(live_preflight))
        .route("/api/live_preflight/checks", get(live_preflight_checks))
        .route("/api/live_preflight/summary", get(live_preflight_summary))
        .route(
            "/api/order_reconciliation/status",
            get(order_reconciliation_status),
        )
        .route("/api/balance_reconciliation", get(balance_reconciliation))
        .route("/api/kill_switch", get(kill_switch))
        .route("/api/small_live_gate", get(small_live_gate))
        .route("/api/arbitrage/relationships", get(arbitrage_relationships))
        .route("/api/arbitrage/opportunities", get(arbitrage_opportunities))
        .route(
            "/api/arbitrage/opportunities/:id",
            get(arbitrage_opportunity_by_id),
        )
        .route("/api/arbitrage/rankings", get(arbitrage_rankings))
        .route("/api/arbitrage/fees", get(arbitrage_fees))
        .route(
            "/api/arbitrage/capital-efficiency",
            get(arbitrage_capital_efficiency),
        )
        .route("/api/arbitrage/statistics", get(arbitrage_statistics))
        .route("/api/spot-arb/dashboard", get(spot_arb_dashboard))
        .route("/api/cross-arb/dashboard", get(cross_arb_dashboard))
        .route(
            "/api/exchange-api-keys",
            get(exchange_api_keys).post(update_exchange_api_keys),
        )
        .route(
            "/api/exchange-api-keys/:exchange",
            axum::routing::delete(delete_exchange_api_keys),
        )
        .route("/api/scanner/exchanges", get(scanner_exchanges))
        .route("/api/scanner/symbol-coverage", get(scanner_symbol_coverage))
        .route("/api/scanner/exchange-pairs", get(scanner_exchange_pairs))
        .route("/api/scanner/opportunities", get(scanner_opportunities))
        .route("/api/scanner/pair-statistics", get(scanner_pair_statistics))
        .route("/api/scanner/symbol-scores", get(scanner_symbol_scores))
        .route("/api/scanner/recommendations", get(scanner_recommendations))
        .route("/api/hedge-policy/status", get(hedge_policy_status))
        .route(
            "/api/hedge-policy/inventory-risk",
            get(hedge_policy_inventory_risk),
        )
        .route(
            "/api/hedge-policy/recommendations",
            get(hedge_policy_recommendations),
        )
        .route(
            "/api/hedge-policy/venue-capabilities",
            get(hedge_policy_venue_capabilities),
        )
        .route(
            "/api/hedge-policy/market-regime",
            get(hedge_policy_market_regime),
        )
        .route("/api/control/symbols", get(control_symbols))
        .route("/api/control/symbols/:symbol", get(control_symbol))
        .route(
            "/api/control/symbols/:symbol/inventory",
            get(control_symbol_inventory),
        )
        .route(
            "/api/control/symbols/:symbol/orders",
            get(control_symbol_orders),
        )
        .route(
            "/api/control/symbols/:symbol/commands",
            get(control_symbol_commands),
        )
        .route(
            "/api/control/symbols/:symbol/liquidation",
            get(control_symbol_liquidation),
        )
        .route(
            "/api/control/symbols/:symbol/runtime-snapshot",
            get(control_symbol_runtime_snapshot),
        )
        .route(
            "/api/control/symbols/:symbol/readiness",
            get(control_symbol_readiness),
        )
        .route(
            "/api/control/symbols/:symbol/inventory-ownership",
            get(control_symbol_inventory_ownership),
        )
        .route(
            "/api/control/symbols/:symbol/direction-readiness",
            get(control_symbol_direction_readiness),
        )
        .route(
            "/api/control/symbols/:symbol/liquidation-preview",
            get(control_symbol_liquidation_preview),
        )
        .route(
            "/api/control/symbols/:symbol/data-health",
            get(control_symbol_data_health),
        )
        .route("/api/control/commands/:command_id", get(control_command))
        .route("/api/control/audit", get(control_audit))
        .route(
            "/api/control/runtime-publisher/status",
            get(runtime_publisher_status),
        )
        .route(
            "/api/control/runtime-publisher/exchanges",
            get(runtime_publisher_exchanges),
        )
        .route(
            "/api/control/runtime-publisher/components",
            get(runtime_publisher_components),
        )
        .route(
            "/api/control/runtime-publisher/errors",
            get(runtime_publisher_errors),
        )
        .route(
            "/api/control/symbols/:symbol/snapshots",
            get(control_symbol_snapshots),
        )
        .route(
            "/api/control/snapshots/:snapshot_id",
            get(control_snapshot_by_id),
        )
        .route(
            "/api/control/snapshots/:snapshot_id/replay",
            get(control_snapshot_by_id),
        )
        .route(
            "/api/control/symbols/:symbol/open-order-ownership",
            get(control_symbol_open_order_ownership),
        )
        .route(
            "/api/control/symbols/:symbol/fill-ownership",
            get(control_symbol_fill_ownership),
        )
        .route(
            "/api/control/symbols/:symbol/consistency",
            get(control_symbol_consistency),
        )
        .route("/api/logs", get(logs))
        .route("/api/strategy-logs", get(strategy_logs))
        .route("/api/health", get(health))
        .route("/api/events", get(events))
        .route("/api/control/pause", post(control_pause))
        .route("/api/control/resume", post(control_resume))
        .route("/api/control/kill_switch", post(control_kill_switch))
        .route("/api/kill_switch/trigger", post(control_kill_switch))
        .route("/api/kill_switch/reset", post(control_kill_switch_reset))
        .route(
            "/api/control/exchanges/:exchange/pause",
            post(control_exchange_pause),
        )
        .route(
            "/api/control/exchanges/:exchange/resume",
            post(control_exchange_resume),
        )
        .route(
            "/api/control/symbols/:symbol/pause",
            post(control_symbol_pause),
        )
        .route(
            "/api/control/symbols/:symbol/resume",
            post(control_symbol_resume),
        )
        .route(
            "/api/control/symbols/:symbol/disable",
            post(control_symbol_disable),
        )
        .route(
            "/api/control/symbols/:symbol/cancel-orders",
            post(control_symbol_cancel_orders),
        )
        .route(
            "/api/control/symbols/:symbol/confirm-liquidation",
            post(control_symbol_confirm_liquidation),
        )
        .route(
            "/api/control/symbols/:symbol/stop-liquidation",
            post(control_symbol_stop_liquidation),
        )
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_headers(Any)
                .allow_methods(Any),
        )
        .fallback_service(ServeDir::new(static_dir).fallback(ServeFile::new(index)))
        .with_state(state)
}

async fn status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let mut model = read_snapshot(&state).await?;
        apply_current_strategy_config_summary(&state, &mut model).await;
        Ok(json!(status_from_model(&model)))
    })
    .await
}

async fn config(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let mut config = match strategy_config_summary(&state).await {
            Ok(config) => config,
            Err(_) => read_snapshot(&state).await?.config_summary,
        };
        config.secrets_redacted = true;
        Ok(json!(config))
    })
    .await
}

async fn workspace_summary(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if let Err(error) = reap_supervisor_exits(&state).await {
        log::warn!("failed to refresh strategy process status: {error}");
    }
    let strategies = strategy_processes(&state).await;
    Json(WorkspaceSummaryResponse {
        schema_version: SUPERVISOR_SCHEMA_VERSION,
        generated_at: Utc::now(),
        strategy_count: strategies.len(),
        process_count: strategies.len(),
        agent_count: 1,
        risk_status: "unknown".to_string(),
    })
    .into_response()
}

async fn strategies(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if let Err(error) = reap_supervisor_exits(&state).await {
        log::warn!("failed to refresh strategy process status: {error}");
    }
    Json(strategy_process_views(strategy_processes(&state).await)).into_response()
}

async fn processes(State(state): State<AppState>, headers: HeaderMap) -> Response {
    strategies(State(state), headers).await
}

async fn strategy_detail(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if let Err(error) = reap_supervisor_exits(&state).await {
        log::warn!("failed to refresh strategy process status: {error}");
    }
    match strategy_processes(&state)
        .await
        .into_iter()
        .find(|process| process.strategy_id == id)
    {
        Some(process) => Json(strategy_process_view(process)).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("strategy not found: {id}") })),
        )
            .into_response(),
    }
}

async fn strategy_config_by_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match read_strategy_config_by_id(&state, &id).await {
        Ok((path, content)) => Json(json!({
            "strategy_id": id,
            "path": path.display().to_string(),
            "content": content,
        }))
        .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn update_strategy_config_by_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(request): Json<StrategyConfigUpdateRequest>,
) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match apply_strategy_config_update_by_id(&state, &id, request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn create_strategy(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CreateStrategyRequest>,
) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match create_strategy_inner(&state, request).await {
        Ok(process) => Json(strategy_process_view(process)).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn strategy_command(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(mut command): Json<LifecycleCommandRecord>,
) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    if command.strategy_id != id {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "strategy_id does not match route id" })),
        )
            .into_response();
    }
    command.schema_version = SUPERVISOR_SCHEMA_VERSION;
    if let Err(error) = command.validate() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response();
    }
    match apply_strategy_command_inner(&state, command.clone()).await {
        Ok(process) => Json(json!({
            "accepted": true,
            "applied_to_runtime": true,
            "command": command,
            "process": strategy_process_view(process),
        }))
        .into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn apply_current_strategy_config_summary(state: &AppState, model: &mut DashboardReadModel) {
    if let Ok(config) = strategy_config_summary(state).await {
        model.trading_mode = config.trading_mode.clone();
        model.live_trading_enabled = config.live_trading_enabled;
        model.dry_run = config.dry_run;
        model.config_summary = config;
    }
}

fn load_legacy_supervisor(
    registry_path: &FsPath,
    specs_path: &FsPath,
    strategy_config: &FsPath,
) -> Result<LocalProcessSupervisor> {
    let store = JsonFileProcessRegistryStore::new(registry_path);
    let mut registry = store
        .load()
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    let mut specs = read_strategy_specs(specs_path)?;
    let mut registry_changed = false;
    let mut specs_changed = normalize_strategy_specs(&mut specs);

    if migrate_legacy_contract_arb_strategy_id(&mut registry, &mut specs) {
        registry_changed = true;
        specs_changed = true;
    }
    if migrate_funding_arb_to_live_config(&mut registry, &mut specs) {
        registry_changed = true;
        specs_changed = true;
    }

    for preset in default_managed_strategy_presets(strategy_config) {
        if registry_contains_preset(&registry, &preset) {
            continue;
        }
        let (process, spec) = managed_strategy_process_and_spec(&preset);
        registry.upsert(process);
        specs.push(spec);
        registry_changed = true;
        specs_changed = true;
    }
    if registry_changed {
        store
            .save(&registry)
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    }

    let mut spec_ids = specs
        .iter()
        .map(|spec| spec.strategy_id.clone())
        .collect::<HashSet<_>>();
    for process in registry.list() {
        if spec_ids.insert(process.strategy_id.clone()) {
            specs.push(strategy_process_spec_from_process(&process));
            specs_changed = true;
        }
    }
    if specs_changed {
        write_strategy_specs(specs_path, &specs)?;
    }
    let mut supervisor = LocalProcessSupervisor::from_registry(registry);
    for spec in specs {
        supervisor.remember_spec(spec);
    }
    Ok(supervisor)
}

fn normalize_strategy_specs(specs: &mut [StrategyProcessSpec]) -> bool {
    let mut changed = false;
    for spec in specs {
        if spec.strategy_kind == "cross_exchange_arbitrage"
            && spec.args.iter().any(|arg| arg == "cross_arb_live")
            && spec.args.iter().any(|arg| arg == "--run")
            && !spec.args.iter().any(|arg| arg == "--skip-private-audit")
        {
            let insert_at = spec
                .args
                .iter()
                .position(|arg| arg == "--run")
                .unwrap_or(spec.args.len());
            spec.args
                .insert(insert_at, "--skip-private-audit".to_string());
            changed = true;
        }
        if spec.strategy_kind == "funding_arbitrage"
            && spec.args.iter().any(|arg| arg == "funding_arb_live")
            && !spec.args.iter().any(|arg| arg == "--confirm-live-order")
        {
            spec.args.push("--confirm-live-order".to_string());
            changed = true;
        }
    }
    changed
}

fn migrate_legacy_contract_arb_strategy_id(
    registry: &mut ProcessRegistry,
    specs: &mut [StrategyProcessSpec],
) -> bool {
    const LEGACY_ID: &str = "default-strategy";
    const STRATEGY_ID: &str = "contract-arb-local";
    const LOG_PATH: &str = "logs/control_panel/contract-arb-local.log";

    let Some(legacy) = registry.get(LEGACY_ID).cloned() else {
        return false;
    };
    if registry.get(STRATEGY_ID).is_some()
        || legacy.strategy_kind != "cross_exchange_arbitrage"
        || !legacy.status.is_terminal()
    {
        return false;
    }

    let mut migrated = legacy;
    migrated.strategy_id = STRATEGY_ID.to_string();
    migrated.log_path = Some(LOG_PATH.to_string());
    registry.remove(LEGACY_ID);
    registry.upsert(migrated);

    for spec in specs {
        if spec.strategy_id == LEGACY_ID && spec.strategy_kind == "cross_exchange_arbitrage" {
            spec.strategy_id = STRATEGY_ID.to_string();
            spec.log_path = Some(LOG_PATH.to_string());
        }
    }

    true
}

fn migrate_funding_arb_to_live_config(
    registry: &mut ProcessRegistry,
    specs: &mut [StrategyProcessSpec],
) -> bool {
    const STRATEGY_ID: &str = "funding-arb-local";
    const OLD_CONFIG: &str = "config/funding_rate_arbitrage_usdt.yml";
    const LIVE_CONFIG: &str = "config/funding_rate_arbitrage_live_usdt.yml";
    let mut changed = false;

    if let Some(mut process) = registry.get(STRATEGY_ID).cloned() {
        if process.strategy_kind == "funding_arbitrage"
            && normalize_strategy_path(&process.config_path) == OLD_CONFIG
        {
            process.config_path = LIVE_CONFIG.to_string();
            registry.upsert(process);
            changed = true;
        }
    }

    for spec in specs {
        if spec.strategy_id == STRATEGY_ID
            && spec.strategy_kind == "funding_arbitrage"
            && normalize_strategy_path(&spec.config_path) == OLD_CONFIG
        {
            spec.config_path = LIVE_CONFIG.to_string();
            spec.args = default_strategy_args("funding_arbitrage", LIVE_CONFIG, Vec::new());
            changed = true;
        }
    }

    changed
}

fn default_managed_strategy_presets(strategy_config: &FsPath) -> Vec<ManagedStrategyPreset> {
    let requested_config = strategy_config.display().to_string();
    let contract_config =
        if strategy_kind_from_config_path(&requested_config) == "cross_exchange_arbitrage" {
            requested_config
        } else {
            "config/cross_exchange_arbitrage_usdt.yml".to_string()
        };

    vec![
        ManagedStrategyPreset {
            strategy_id: "spot-arb-local",
            strategy_kind: "spot_spot_taker_arbitrage",
            config_path: "config/spot_spot_taker_arbitrage.yml".to_string(),
            log_path: "logs/control_panel/spot-arb-local.log",
            command: "cargo",
            args: default_strategy_args(
                "spot_spot_taker_arbitrage",
                "config/spot_spot_taker_arbitrage.yml",
                Vec::new(),
            ),
        },
        ManagedStrategyPreset {
            strategy_id: "contract-arb-local",
            strategy_kind: "cross_exchange_arbitrage",
            config_path: contract_config.clone(),
            log_path: "logs/control_panel/contract-arb-local.log",
            command: "cargo",
            args: default_strategy_args("cross_exchange_arbitrage", &contract_config, Vec::new()),
        },
        ManagedStrategyPreset {
            strategy_id: "funding-arb-local",
            strategy_kind: "funding_arbitrage",
            config_path: "config/funding_rate_arbitrage_live_usdt.yml".to_string(),
            log_path: "logs/control_panel/funding-arb-local.log",
            command: "cargo",
            args: default_strategy_args(
                "funding_arbitrage",
                "config/funding_rate_arbitrage_live_usdt.yml",
                Vec::new(),
            ),
        },
    ]
}

fn registry_contains_preset(registry: &ProcessRegistry, preset: &ManagedStrategyPreset) -> bool {
    registry.list().into_iter().any(|process| {
        process.strategy_id == preset.strategy_id
            || (process.strategy_kind == preset.strategy_kind
                && normalize_strategy_path(&process.config_path)
                    == normalize_strategy_path(&preset.config_path))
    })
}

fn normalize_strategy_path(path: &str) -> String {
    path.trim_start_matches("./").to_string()
}

fn managed_strategy_process_and_spec(
    preset: &ManagedStrategyPreset,
) -> (StrategyProcess, StrategyProcessSpec) {
    let mut process = StrategyProcess::new(
        preset.strategy_id,
        preset.strategy_kind,
        "local",
        "local",
        preset.config_path.clone(),
    );
    process.status = ProcessStatus::Stopped;
    process.log_path = Some(preset.log_path.to_string());
    let spec = StrategyProcessSpec::new(
        process.strategy_id.clone(),
        process.strategy_kind.clone(),
        process.run_id.clone(),
        process.tenant_id.clone(),
        process.config_path.clone(),
        preset.command,
    )
    .with_args(preset.args.clone())
    .with_working_dir(".")
    .with_log_path(preset.log_path);
    (process, spec)
}

fn strategy_process_spec_from_process(process: &StrategyProcess) -> StrategyProcessSpec {
    let mut spec = StrategyProcessSpec::new(
        process.strategy_id.clone(),
        process.strategy_kind.clone(),
        process.run_id.clone(),
        process.tenant_id.clone(),
        process.config_path.clone(),
        "cargo",
    )
    .with_args(default_strategy_args(
        &process.strategy_kind,
        &process.config_path,
        Vec::new(),
    ))
    .with_working_dir(".");
    if let Some(log_path) = process.log_path.clone() {
        spec = spec.with_log_path(log_path);
    }
    spec
}

async fn strategy_processes(state: &AppState) -> Vec<StrategyProcess> {
    state.supervisor.read().await.registry().list()
}

fn strategy_process_views(processes: Vec<StrategyProcess>) -> Vec<StrategyProcessView> {
    processes.into_iter().map(strategy_process_view).collect()
}

fn strategy_process_view(process: StrategyProcess) -> StrategyProcessView {
    let log_configured = process
        .log_path
        .as_ref()
        .map(|path| !path.trim().is_empty())
        .unwrap_or(false);
    StrategyProcessView {
        schema_version: process.schema_version,
        strategy_id: process.strategy_id,
        strategy_kind: process.strategy_kind,
        run_id: process.run_id,
        tenant_id: process.tenant_id,
        config_path: process.config_path,
        status: process.status,
        process_id: process.process_id,
        started_at: process.started_at,
        last_heartbeat_at: process.last_heartbeat_at,
        last_snapshot_at: process.last_snapshot_at,
        restart_count: process.restart_count,
        last_exit_code: process.last_exit_code,
        last_error: process.last_error,
        log_configured,
        log_path: process.log_path,
    }
}

async fn create_strategy_inner(
    state: &AppState,
    request: CreateStrategyRequest,
) -> Result<StrategyProcess> {
    let (mut process, spec) = request.into_process_and_spec(Utc::now())?;
    process.status = ProcessStatus::Stopped;
    let mut supervisor = state.supervisor.write().await;
    supervisor
        .register(process.clone())
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    supervisor.remember_spec(spec);
    save_registry(
        state,
        &ProcessRegistry::from_processes(&supervisor.registry().list()),
    )?;
    write_strategy_specs(&state.strategy_specs, &supervisor_specs(&supervisor))?;
    Ok(process)
}

async fn apply_strategy_command_inner(
    state: &AppState,
    command: LifecycleCommandRecord,
) -> Result<StrategyProcess> {
    let mut supervisor = state.supervisor.write().await;
    let process = match command.command {
        LifecycleCommand::Start => {
            let spec = strategy_spec_for_command(&supervisor, &command)?;
            supervisor
                .start(spec)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?
        }
        LifecycleCommand::Stop => match supervisor.stop(&command.strategy_id).await {
            Ok(process) => process,
            Err(error) => {
                if supervisor.registry().get(&command.strategy_id).is_none() {
                    return Err(anyhow::anyhow!(error.to_string()));
                }
                supervisor
                    .mark_stopped_if_registered(&command.strategy_id)
                    .map_err(|error| anyhow::anyhow!(error.to_string()))?;
                supervisor
                    .registry()
                    .get(&command.strategy_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("strategy not found: {}", command.strategy_id))?
            }
        },
        LifecycleCommand::Restart => {
            if supervisor.registry().get(&command.strategy_id).is_none() {
                anyhow::bail!("strategy not found: {}", command.strategy_id);
            }
            let spec = strategy_spec_for_command(&supervisor, &command)?;
            if supervisor.restart(&command.strategy_id).await.is_err() {
                supervisor
                    .start(spec)
                    .await
                    .map_err(|error| anyhow::anyhow!(error.to_string()))?
            } else {
                supervisor
                    .registry()
                    .get(&command.strategy_id)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!("strategy not found: {}", command.strategy_id))?
            }
        }
        LifecycleCommand::Heartbeat => {
            supervisor
                .heartbeat(&command.strategy_id, command.requested_at)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            supervisor
                .registry()
                .get(&command.strategy_id)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("strategy not found: {}", command.strategy_id))?
        }
    };
    save_registry(
        state,
        &ProcessRegistry::from_processes(&supervisor.registry().list()),
    )?;
    Ok(process)
}

async fn reap_supervisor_exits(state: &AppState) -> Result<()> {
    let mut supervisor = state.supervisor.write().await;
    let exited = supervisor
        .reap_exited()
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    if !exited.is_empty() {
        save_registry(
            state,
            &ProcessRegistry::from_processes(&supervisor.registry().list()),
        )?;
    }
    Ok(())
}

fn save_registry(state: &AppState, registry: &ProcessRegistry) -> Result<()> {
    JsonFileProcessRegistryStore::new(&state.strategy_registry)
        .save(registry)
        .map_err(|error| anyhow::anyhow!(error.to_string()))
}

fn read_strategy_specs(path: &FsPath) -> Result<Vec<StrategyProcessSpec>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("read strategy specs {}", path.display()))?;
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    let store: StrategySpecsStore = serde_json::from_str(&raw)
        .with_context(|| format!("parse strategy specs {}", path.display()))?;
    Ok(store.specs)
}

fn write_strategy_specs(path: &FsPath, specs: &[StrategyProcessSpec]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let store = StrategySpecsStore {
        schema_version: SUPERVISOR_SCHEMA_VERSION,
        updated_at: Utc::now(),
        specs: specs.to_vec(),
    };
    let raw = serde_json::to_vec_pretty(&store).context("serialize strategy specs")?;
    let temp_path = path.with_extension("tmp");
    std::fs::write(&temp_path, raw).with_context(|| format!("write {}", temp_path.display()))?;
    std::fs::rename(&temp_path, path).with_context(|| format!("replace {}", path.display()))?;
    Ok(())
}

fn supervisor_specs(supervisor: &LocalProcessSupervisor) -> Vec<StrategyProcessSpec> {
    supervisor
        .registry()
        .list()
        .into_iter()
        .filter_map(|process| {
            supervisor.spec(&process.strategy_id).cloned().or_else(|| {
                let command = LifecycleCommandRecord {
                    schema_version: SUPERVISOR_SCHEMA_VERSION,
                    command_id: "snapshot".to_string(),
                    strategy_id: process.strategy_id.clone(),
                    run_id: Some(process.run_id.clone()),
                    command: LifecycleCommand::Start,
                    requested_by: Some("control_api".to_string()),
                    idempotency_key: "snapshot".to_string(),
                    requested_at: Utc::now(),
                };
                strategy_spec_for_command(supervisor, &command).ok()
            })
        })
        .collect()
}

fn strategy_spec_for_command(
    supervisor: &LocalProcessSupervisor,
    command: &LifecycleCommandRecord,
) -> Result<StrategyProcessSpec> {
    if let Some(spec) = supervisor.spec(&command.strategy_id) {
        let mut spec = spec.clone();
        if let Some(run_id) = &command.run_id {
            spec.run_id = run_id.clone();
        }
        return Ok(spec);
    }
    let process = supervisor
        .registry()
        .get(&command.strategy_id)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("strategy not found: {}", command.strategy_id))?;
    let mut spec = StrategyProcessSpec::new(
        process.strategy_id.clone(),
        process.strategy_kind.clone(),
        command.run_id.clone().unwrap_or(process.run_id.clone()),
        process.tenant_id.clone(),
        process.config_path.clone(),
        "cargo",
    )
    .with_args(default_strategy_args(
        &process.strategy_kind,
        &process.config_path,
        Vec::new(),
    ))
    .with_working_dir(".");
    if let Some(log_path) = process.log_path.clone() {
        spec = spec.with_log_path(log_path);
    }
    Ok(spec)
}

impl CreateStrategyRequest {
    fn into_process_and_spec(
        self,
        now: DateTime<Utc>,
    ) -> Result<(StrategyProcess, StrategyProcessSpec)> {
        let strategy_id = self.strategy_id.trim();
        let strategy_kind = self.strategy_kind.trim();
        if strategy_id.is_empty() {
            anyhow::bail!("strategy_id cannot be empty");
        }
        if strategy_kind.is_empty() {
            anyhow::bail!("strategy_kind cannot be empty");
        }
        let config_path = self.config_path.trim().to_string();
        let run_id = self
            .run_id
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("manual-{}", now.timestamp_millis()));
        let tenant_id = self
            .tenant_id
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "local".to_string());
        let mut process = StrategyProcess::new(
            strategy_id,
            strategy_kind,
            run_id.clone(),
            tenant_id.clone(),
            config_path.clone(),
        );
        process.schema_version = SUPERVISOR_SCHEMA_VERSION;
        process.log_path = self.log_path.filter(|value| !value.trim().is_empty());
        let mut spec = StrategyProcessSpec::new(
            process.strategy_id.clone(),
            process.strategy_kind.clone(),
            run_id,
            tenant_id,
            config_path,
            self.command
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "cargo".to_string()),
        )
        .with_args(default_strategy_args(
            &process.strategy_kind,
            &process.config_path,
            self.args,
        ));
        if let Some(working_dir) = self.working_dir.filter(|value| !value.trim().is_empty()) {
            spec = spec.with_working_dir(working_dir);
        } else {
            spec = spec.with_working_dir(".");
        }
        if let Some(log_path) = process.log_path.clone() {
            spec = spec.with_log_path(log_path);
        }
        Ok((process, spec))
    }
}

fn default_strategy_args(
    strategy_kind: &str,
    config_path: &str,
    explicit_args: Vec<String>,
) -> Vec<String> {
    if !explicit_args.is_empty() {
        return explicit_args;
    }
    match strategy_kind {
        "cross_exchange_arbitrage" => vec![
            "run".to_string(),
            "--bin".to_string(),
            "cross_arb_live".to_string(),
            "--".to_string(),
            "--config".to_string(),
            config_path.to_string(),
            "--skip-private-audit".to_string(),
            "--run".to_string(),
        ],
        "spot_spot_arbitrage" | "spot_spot_taker_arbitrage" => vec![
            "run".to_string(),
            "--bin".to_string(),
            "rustcta".to_string(),
            "--".to_string(),
            "--strategy".to_string(),
            "spot_spot_taker_arbitrage".to_string(),
            "--config".to_string(),
            config_path.to_string(),
        ],
        "funding_arbitrage" | "funding_rate_arbitrage" => {
            let live = config_path.contains("live");
            let mut args = vec![
                "run".to_string(),
                "--bin".to_string(),
                if live {
                    "funding_arb_live".to_string()
                } else {
                    "funding_arb_observe".to_string()
                },
                "--".to_string(),
                "--config".to_string(),
                config_path.to_string(),
            ];
            if live {
                args.push("--confirm-live-order".to_string());
            }
            args
        }
        value => vec![
            "run".to_string(),
            "--bin".to_string(),
            "rustcta".to_string(),
            "--".to_string(),
            "--strategy".to_string(),
            value.to_string(),
            "--config".to_string(),
            config_path.to_string(),
        ],
    }
}

fn strategy_kind_from_config_path(config_path: &str) -> &'static str {
    if config_path.contains("cross_exchange") || config_path.contains("cross_arb") {
        "cross_exchange_arbitrage"
    } else if config_path.contains("funding") {
        "funding_arbitrage"
    } else {
        "spot_spot_taker_arbitrage"
    }
}

async fn read_strategy_config_by_id(state: &AppState, id: &str) -> Result<(PathBuf, String)> {
    let process = strategy_processes(state)
        .await
        .into_iter()
        .find(|process| process.strategy_id == id)
        .ok_or_else(|| anyhow::anyhow!("strategy not found: {id}"))?;
    let path = PathBuf::from(process.config_path);
    let content = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("read strategy config {}", path.display()))?;
    Ok((path, content))
}

async fn apply_strategy_config_update_by_id(
    state: &AppState,
    id: &str,
    request: StrategyConfigUpdateRequest,
) -> Result<Value> {
    if request.content.trim().is_empty() {
        anyhow::bail!("strategy config content cannot be empty");
    }
    serde_yaml::from_str::<serde_yaml::Value>(&request.content)
        .context("strategy config must be valid YAML")?;
    let (path, _) = read_strategy_config_by_id(state, id).await?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp_path = path.with_extension("yml.tmp");
    tokio::fs::write(&tmp_path, request.content.as_bytes())
        .await
        .with_context(|| format!("write {}", tmp_path.display()))?;
    tokio::fs::rename(&tmp_path, &path)
        .await
        .with_context(|| format!("replace {}", path.display()))?;

    let restart = if request.restart {
        let command_id = format!("web-config-restart-{}", Utc::now().timestamp_millis());
        let process = apply_strategy_command_inner(
            state,
            LifecycleCommandRecord {
                schema_version: SUPERVISOR_SCHEMA_VERSION,
                command_id: command_id.clone(),
                strategy_id: id.to_string(),
                run_id: Some(format!("run-{}", Utc::now().timestamp_millis())),
                command: LifecycleCommand::Restart,
                requested_by: Some("web".to_string()),
                idempotency_key: command_id,
                requested_at: Utc::now(),
            },
        )
        .await?;
        json!({
            "requested": true,
            "strategy_status": process.status,
            "process": strategy_process_view(process),
        })
    } else {
        json!({ "requested": false })
    };
    Ok(json!({
        "saved": true,
        "strategy_id": id,
        "path": path.display().to_string(),
        "restart": restart,
    }))
}

async fn strategy_config_summary(state: &AppState) -> Result<ConfigSummaryView> {
    let content = tokio::fs::read_to_string(&state.strategy_config)
        .await
        .with_context(|| format!("read strategy config {}", state.strategy_config.display()))?;
    if let Ok(config) = serde_yaml::from_str::<SpotSpotTakerArbitrageConfig>(&content) {
        return Ok(ConfigSummaryView {
            enabled_exchanges: config.exchanges,
            enabled_symbols: config.symbols,
            trading_mode: config.trading_mode,
            live_trading_enabled: config.live_trading_enabled,
            dry_run: config.dry_run,
            min_raw_spread_bps: Some(config.min_raw_spread_bps),
            min_net_spread_bps: Some(config.min_net_spread_bps),
            max_raw_spread_bps: Some(config.max_raw_spread_bps),
            max_notional_per_trade: Some(config.max_notional_per_trade),
            max_notional_per_symbol: Some(config.max_notional_per_symbol),
            max_total_notional: Some(config.max_total_notional),
            max_enabled_arbitrage_symbols: Some(config.max_enabled_arbitrage_symbols),
            initial_entry_notional_usdt: Some(config.initial_entry_notional_usdt),
            fee_config_summary: Some(config.fee_config_path),
            disabled_config_summary: Some(config.disabled_registry_path),
            secrets_redacted: true,
        });
    }
    let config: CrossExchangeArbitrageConfig =
        serde_yaml::from_str(&content).with_context(|| {
            format!(
                "parse spot/cross strategy config {}",
                state.strategy_config.display()
            )
        })?;
    Ok(ConfigSummaryView {
        enabled_exchanges: config
            .universe
            .enabled_exchanges
            .iter()
            .map(ToString::to_string)
            .collect(),
        enabled_symbols: config
            .universe
            .symbols
            .iter()
            .map(ToString::to_string)
            .collect(),
        trading_mode: format!("{:?}", config.trading_mode).to_ascii_lowercase(),
        live_trading_enabled: config.enable_live_trading,
        dry_run: config.execution.dry_run,
        min_raw_spread_bps: Some(config.thresholds.min_open_raw_spread * 10_000.0),
        min_net_spread_bps: Some(config.thresholds.min_open_maker_taker_net_edge * 10_000.0),
        max_raw_spread_bps: Some(config.thresholds.max_open_raw_spread * 10_000.0),
        max_notional_per_trade: Some(config.sizing.max_notional_usdt),
        max_notional_per_symbol: Some(config.risk.max_notional_per_symbol_usdt),
        max_total_notional: Some(config.risk.max_total_notional_usdt),
        max_enabled_arbitrage_symbols: config.universe.max_symbol_count,
        initial_entry_notional_usdt: Some(config.sizing.target_notional_usdt),
        fee_config_summary: Some("cross-arb inline fees".to_string()),
        disabled_config_summary: None,
        secrets_redacted: true,
    })
}

async fn strategy_config(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match tokio::fs::read_to_string(&state.strategy_config).await {
        Ok(content) => Json(json!({
            "path": state.strategy_config.display().to_string(),
            "content": content,
            "restart_on_save": true,
        }))
        .into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn update_strategy_config(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<StrategyConfigUpdateRequest>,
) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match apply_strategy_config_update(&state, request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn cross_arb_settings(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match read_cross_arb_config(&state).await.map(|config| {
        let mut view = cross_arb_settings_view(config);
        view.path = state.strategy_config.display().to_string();
        view
    }) {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn update_cross_arb_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<CrossArbSettingsUpdateRequest>,
) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match apply_cross_arb_settings_update(&state, request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn funding_arb_settings(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match read_funding_arb_settings_view(&state).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn update_funding_arb_settings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<FundingArbSettingsUpdateRequest>,
) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match apply_funding_arb_settings_update(&state, request).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn cross_arb_instruments(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let config = read_cross_arb_config(&state).await?;
        let wanted = config
            .universe
            .symbols
            .iter()
            .cloned()
            .collect::<HashSet<_>>();
        let event_dir = PathBuf::from(&config.persistence.jsonl_dir);
        let events = read_cross_arb_events(&event_dir).await.unwrap_or_default();
        let persisted_instruments =
            latest_cross_arb_instruments_by_venue_symbol(&events.instruments);
        let mut rows = Vec::new();
        let mut errors = Vec::new();
        let mut live_instruments = Vec::new();
        for exchange in &config.universe.enabled_exchanges {
            let Some(adapter) = exchange_registry::market_adapter(exchange) else {
                errors.push(json!({
                    "exchange": exchange.to_string(),
                    "message": "public market adapter is not registered",
                }));
                continue;
            };
            match adapter.load_instruments().await {
                Ok(instruments) => {
                    let selected = instruments
                        .into_iter()
                        .filter(|instrument| {
                            instrument.is_tradeable_usdt_perpetual()
                                && wanted.contains(&instrument.canonical_symbol)
                        })
                        .collect::<Vec<_>>();
                    live_instruments.extend(selected.iter().cloned());
                    rows.extend(selected.into_iter().map(|instrument| json!(instrument)));
                }
                Err(error) => errors.push(json!({
                    "exchange": exchange.to_string(),
                    "message": error.to_string(),
                    "fallback": "persisted_instruments",
                })),
            }
            if !rows
                .iter()
                .any(|row| text_field(row, "exchange").eq_ignore_ascii_case(exchange.as_str()))
            {
                let exchange_key = exchange.as_str().to_string();
                rows.extend(config.universe.symbols.iter().filter_map(|symbol| {
                    persisted_instruments
                        .get(&(exchange_key.clone(), symbol.to_string()))
                        .cloned()
                }));
            }
        }
        persist_cross_arb_instruments(&event_dir, &live_instruments).await?;
        rows.sort_by(|left, right| {
            text_field(left, "canonical_symbol")
                .cmp(&text_field(right, "canonical_symbol"))
                .then_with(|| text_field(left, "exchange").cmp(&text_field(right, "exchange")))
        });
        let mut coverage = BTreeMap::<String, usize>::new();
        for row in &rows {
            let symbol = text_field(row, "canonical_symbol");
            if !symbol.is_empty() {
                *coverage.entry(symbol).or_default() += 1;
            }
        }
        let mut price_by_venue = latest_cross_arb_mid_prices(&events.market_snapshots);
        price_by_venue.extend(load_cross_arb_reference_prices(&config).await);
        let rows = rows
            .into_iter()
            .map(|row| {
                let exchange = text_field(&row, "exchange");
                let symbol = text_field(&row, "canonical_symbol");
                let price = price_by_venue
                    .get(&(exchange.clone(), symbol.clone()))
                    .copied();
                enrich_cross_arb_instrument_feasibility(
                    row,
                    price,
                    config.sizing.target_notional_usdt,
                    config.sizing.max_notional_usdt,
                )
            })
            .collect::<Vec<_>>();
        let feasibility_summary = cross_arb_feasibility_summary(
            &rows,
            config.universe.symbols.len(),
            config.market.min_common_exchanges,
        );
        let missing_symbols = config
            .universe
            .symbols
            .iter()
            .filter(|symbol| {
                coverage
                    .get(&symbol.to_string())
                    .copied()
                    .unwrap_or_default()
                    < config.market.min_common_exchanges
            })
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        Ok(json!({
            "generated_at": Utc::now(),
            "enabled_exchanges": config
                .universe
                .enabled_exchanges
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            "symbols": config.universe.symbols.len(),
            "min_common_exchanges": config.market.min_common_exchanges,
            "target_notional_usdt": config.sizing.target_notional_usdt,
            "max_notional_usdt": config.sizing.max_notional_usdt,
            "instruments": rows,
            "instrument_count": rows.len(),
            "feasibility": feasibility_summary,
            "coverage_ok": missing_symbols.is_empty() && errors.is_empty(),
            "missing_symbols": missing_symbols,
            "errors": errors,
        }))
    })
    .await
}

async fn cross_arb_market_snapshots(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let config = read_cross_arb_config(&state).await?;
        let event_dir = PathBuf::from(&config.persistence.jsonl_dir);
        let (snapshots, errors) = load_cross_arb_public_market_snapshots(&config).await;
        persist_cross_arb_market_snapshots(&event_dir, &snapshots).await?;
        let rows = snapshots
            .iter()
            .cloned()
            .map(|snapshot| {
                json!({
                    "kind": "MarketSnapshot",
                    "recorded_at": snapshot.timestamp,
                    "exchange": snapshot.exchange,
                    "canonical_symbol": snapshot.symbol,
                    "symbol": snapshot.symbol,
                    "exchange_symbol": snapshot.exchange_symbol,
                    "best_bid": snapshot.best_bid,
                    "best_bid_quantity": snapshot.best_bid_quantity,
                    "best_ask": snapshot.best_ask,
                    "best_ask_quantity": snapshot.best_ask_quantity,
                    "sequence": snapshot.sequence,
                })
            })
            .collect::<Vec<_>>();
        let coverage =
            cross_arb_unique_exchange_symbol_pairs(&rows, &["canonical_symbol", "symbol"]);
        let expected_pair_set = cross_arb_expected_exchange_symbol_pairs(
            &config
                .universe
                .enabled_exchanges
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            &config
                .universe
                .symbols
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
        );
        let missing_pairs = cross_arb_missing_exchange_symbol_pairs(&expected_pair_set, &coverage);
        Ok(json!({
            "generated_at": Utc::now(),
            "enabled_exchanges": config
                .universe
                .enabled_exchanges
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            "symbols": config.universe.symbols.len(),
            "snapshots": rows,
            "snapshot_count": rows.len(),
            "expected_exchange_symbol_pairs": expected_pair_set.len(),
            "covered_exchange_symbol_pairs": coverage.len(),
            "coverage_ok": missing_pairs.is_empty() && errors.is_empty(),
            "missing_pairs": cross_arb_pair_samples(&missing_pairs),
            "missing_pair_count": missing_pairs.len(),
            "errors": errors,
        }))
    })
    .await
}

async fn exchanges(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.exchanges))
    })
    .await
}

async fn symbols(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let model = read_snapshot(&state).await?;
        Ok(json!({
            "symbol_rules": model.spot_symbol_rules,
            "spot_control": model.spot_control,
            "scanner": {
                "symbol_coverage": model.five_exchange_scanner.symbol_coverage,
                "recommendations": model.five_exchange_scanner.recommendations,
            }
        }))
    })
    .await
}

async fn books(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.books))
    })
    .await
}

async fn opportunities(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let model = read_snapshot(&state).await?;
        Ok(json!({
            "recent": model.opportunities,
            "arbitrage": model.arbitrage_opportunities,
            "statistics": model.arbitrage_statistics,
        }))
    })
    .await
}

async fn recent_opportunities(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.opportunities))
    })
    .await
}

async fn recent_trades(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.trades))
    })
    .await
}

async fn inventory(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.inventory))
    })
    .await
}

async fn balance_history(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let cross_arb_history = cross_arb_profit_history(&state)
            .await
            .unwrap_or(Value::Null);
        if cross_arb_history
            .as_array()
            .is_some_and(|rows| !rows.is_empty())
        {
            return Ok(cross_arb_history);
        }
        let profit_history = read_balance_history(&state.strategy_profit_history_path).await?;
        if profit_history
            .as_array()
            .is_some_and(|rows| !rows.is_empty())
        {
            Ok(profit_history)
        } else {
            read_balance_history(&state.balance_history_path).await
        }
    })
    .await
}

async fn fees(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.fees))
    })
    .await
}

async fn disabled(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        match disabled_registry_view_from_strategy_config(&state).await {
            Ok(value) => Ok(json!(value)),
            Err(_) => Ok(json!(read_snapshot(&state).await?.disabled)),
        }
    })
    .await
}

async fn unmanaged_positions(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.unmanaged_positions))
    })
    .await
}

async fn dry_run_plans(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let model = read_snapshot(&state).await?;
        Ok(json!(strategy_order_rows(&model, None)))
    })
    .await
}

async fn risk(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let model = read_snapshot(&state).await?;
        Ok(json!({
            "kill_switch": model.kill_switch,
            "live_preflight_enabled": model.live_preflight_enabled,
            "live_preflight": model.live_preflight,
            "small_live_gate": model.small_live_gate,
            "risk_events": model.risk_events,
            "balance_reconciliation": model.balance_reconciliation,
            "order_reconciliation": {
                "config": model.order_reconciliation_config,
                "status": model.order_reconciliation_status,
            }
        }))
    })
    .await
}

async fn risk_events(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.risk_events))
    })
    .await
}

async fn recorder(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.recorder))
    })
    .await
}

async fn live_preflight(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.live_preflight))
    })
    .await
}

async fn live_preflight_checks(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let report = read_snapshot(&state).await?.live_preflight;
        Ok(report
            .map(|report| json!(report.checks))
            .unwrap_or_else(|| json!([])))
    })
    .await
}

async fn live_preflight_summary(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let report = read_snapshot(&state).await?.live_preflight;
        Ok(report.map_or_else(
            || json!(null),
            |report| {
                json!({
                    "decision": report.decision,
                    "timestamp": report.timestamp,
                    "pass_count": report.pass_count,
                    "warn_count": report.warn_count,
                    "fail_count": report.fail_count,
                    "critical_failures": report.critical_failures,
                    "warnings": report.warnings,
                    "per_exchange_readiness": report.per_exchange_readiness,
                    "per_symbol_readiness": report.per_symbol_readiness,
                    "suggested_next_actions": report.suggested_next_actions,
                })
            },
        ))
    })
    .await
}

async fn order_reconciliation_status(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    guarded(&state, &headers, async {
        let model = read_snapshot(&state).await?;
        Ok(json!({
            "config": model.order_reconciliation_config,
            "last_result": model.order_reconciliation_status,
        }))
    })
    .await
}

async fn balance_reconciliation(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.balance_reconciliation))
    })
    .await
}

async fn kill_switch(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.kill_switch))
    })
    .await
}

async fn small_live_gate(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.small_live_gate))
    })
    .await
}

async fn arbitrage_relationships(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.arbitrage_relationships))
    })
    .await
}

async fn arbitrage_opportunities(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.arbitrage_opportunities))
    })
    .await
}

async fn arbitrage_opportunity_by_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot(&state)
            .await?
            .arbitrage_opportunities
            .into_iter()
            .find(|item| item.opportunity_id == id);
        Ok(json!(value))
    })
    .await
}

async fn arbitrage_rankings(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let mut values = read_snapshot(&state).await?.arbitrage_opportunities;
        values.sort_by(|left, right| {
            right
                .risk_adjusted_score
                .partial_cmp(&left.risk_adjusted_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(json!(values))
    })
    .await
}

async fn arbitrage_fees(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let model = read_snapshot(&state).await?;
        Ok(json!({
            "fees": model.fees,
            "opportunity_fee_sources": model.arbitrage_opportunities.iter().map(|item| {
                json!({
                    "opportunity_id": item.opportunity_id,
                    "fee_sources": item.fee_sources,
                    "warnings": item.warnings,
                })
            }).collect::<Vec<_>>(),
        }))
    })
    .await
}

async fn arbitrage_capital_efficiency(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    guarded(&state, &headers, async {
        let values = read_snapshot(&state)
            .await?
            .arbitrage_opportunities
            .into_iter()
            .map(|item| {
                json!({
                    "opportunity_id": item.opportunity_id,
                    "relationship_type": item.relationship_type,
                    "symbol": item.symbol,
                    "required_capital_usdt": item.required_capital_usdt,
                    "expected_return_on_capital": item.expected_return_on_capital,
                    "expected_return_on_capital_per_hour": item.expected_return_on_capital_per_hour,
                    "account_structure": item.account_structure,
                    "risk_adjusted_score": item.risk_adjusted_score,
                })
            })
            .collect::<Vec<_>>();
        Ok(json!(values))
    })
    .await
}

async fn arbitrage_statistics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        Ok(json!(read_snapshot(&state).await?.arbitrage_statistics))
    })
    .await
}

async fn spot_arb_dashboard(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let mut model = read_snapshot(&state).await?;
        apply_current_strategy_config_summary(&state, &mut model).await;
        let config_summary = model.config_summary.clone();
        let selected_symbols = model
            .config_summary
            .enabled_symbols
            .iter()
            .take(200)
            .cloned()
            .collect::<Vec<_>>();
        let selected_set = selected_symbols
            .iter()
            .map(|symbol| normalize_symbol(symbol))
            .collect::<std::collections::BTreeSet<_>>();
        let target_exchanges = spot_arb_target_exchanges(&state).await.unwrap_or_default();
        let exchange_console =
            filter_console_rows_by_exchange(spot_exchange_console_rows(&model), &target_exchanges);
        let account_console =
            filter_console_rows_by_exchange(spot_account_console_rows(&model), &target_exchanges);
        let position_console =
            filter_console_rows_by_exchange(spot_position_console_rows(&model), &target_exchanges);
        let strategy_orders = strategy_order_rows(&model, Some(&selected_set));
        let candidates = model
            .spot_symbol_rules
            .iter()
            .filter(|rule| {
                rule.quote_asset.eq_ignore_ascii_case("USDT")
                    && model
                        .spot_symbol_rules
                        .iter()
                        .filter(|candidate| {
                            normalize_symbol(&candidate.internal_symbol)
                                == normalize_symbol(&rule.internal_symbol)
                        })
                        .count()
                        >= 2
            })
            .map(|rule| normalize_symbol(&rule.internal_symbol))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .take(200)
            .collect::<Vec<_>>();
        let books = model
            .books
            .into_iter()
            .filter(|book| {
                book.market_type == rustcta::exchanges::unified::MarketType::Spot
                    && (book.exchange.eq_ignore_ascii_case("gateio")
                        || book.exchange.eq_ignore_ascii_case("bitget"))
                    && (selected_set.is_empty()
                        || selected_set.contains(&normalize_symbol(&book.symbol)))
            })
            .collect::<Vec<_>>();
        let opportunities = model
            .arbitrage_opportunities
            .iter()
            .cloned()
            .filter(|opportunity| {
                matches!(
                    opportunity.relationship_type,
                    rustcta::strategies::arbitrage_core::ArbitrageRelationshipType::SpotSpot
                ) && (opportunity.buy_exchange.eq_ignore_ascii_case("gateio")
                    || opportunity.buy_exchange.eq_ignore_ascii_case("bitget"))
                    && (opportunity.sell_exchange.eq_ignore_ascii_case("gateio")
                        || opportunity.sell_exchange.eq_ignore_ascii_case("bitget"))
                    && (selected_set.is_empty()
                        || selected_set.contains(&normalize_symbol(&opportunity.symbol)))
            })
            .collect::<Vec<_>>();
        let live_dry_run_plans = model
            .live_dry_run_orders
            .iter()
            .cloned()
            .filter(|plan| {
                selected_set.is_empty() || selected_set.contains(&normalize_symbol(&plan.symbol))
            })
            .collect::<Vec<_>>();
        let mut active_control_symbols =
            active_spot_control_symbols(model.spot_control.as_ref(), &selected_set);
        if active_control_symbols.is_empty() {
            if let Ok(control) = load_spot_control_service(&state).await {
                let read_model = control.read_model().await;
                active_control_symbols =
                    active_spot_control_symbols(Some(&read_model), &selected_set);
            }
        }
        let initial_entry_status = initial_entry_status_view(
            &live_dry_run_plans,
            &active_control_symbols,
            &config_summary,
        );
        let arbitrage_slot_status = arbitrage_slot_status_view(
            &opportunities,
            &live_dry_run_plans,
            &active_control_symbols,
            &config_summary,
        );
        Ok(json!({
            "exchanges": ["gateio", "bitget", "mexc", "coinex", "kucoin"],
            "target_exchanges": target_exchanges,
            "candidate_limit": 200,
            "subscription_limit": 200,
            "target_refresh_ms": 50,
            "selected_symbols": selected_symbols,
            "candidate_symbols": candidates,
            "books": books,
            "opportunities": opportunities,
            "dry_run_plans": strategy_orders,
            "exchange_console": exchange_console,
            "account_console": account_console,
            "position_console": position_console,
            "initial_entry_status": initial_entry_status,
            "arbitrage_slot_status": arbitrage_slot_status,
            "statistics": model.arbitrage_statistics,
            "snapshot": {
                "generated_at": Utc::now(),
                "strategy": model.strategy,
                "trading_mode": config_summary.trading_mode,
                "dry_run": config_summary.dry_run,
                "live_trading_enabled": config_summary.live_trading_enabled,
            }
        }))
    })
    .await
}

fn spot_exchange_console_rows(model: &DashboardReadModel) -> Vec<Value> {
    let disabled_exchanges = model
        .disabled
        .exchanges
        .iter()
        .map(|item| normalize_exchange_name(&item.exchange))
        .collect::<BTreeSet<_>>();
    let mut exchanges = model
        .config_summary
        .enabled_exchanges
        .iter()
        .map(|exchange| normalize_exchange_name(exchange))
        .filter(|exchange| !exchange.is_empty())
        .collect::<BTreeSet<_>>();
    for health in &model.exchanges {
        exchanges.insert(normalize_exchange_name(&health.exchange));
    }
    exchanges
        .into_iter()
        .map(|exchange| {
            let health = model
                .exchanges
                .iter()
                .find(|row| normalize_exchange_name(&row.exchange) == exchange);
            let disabled = disabled_exchanges.contains(&exchange);
            let account = spot_account_totals(model, &exchange);
            json!({
                "exchange": exchange,
                "enabled": !disabled,
                "status": if disabled { "disabled" } else if health.map(|row| row.connected).unwrap_or(false) { "enabled" } else { "offline" },
                "connected": health.map(|row| row.connected).unwrap_or(false),
                "fresh_symbol_count": health.map(|row| row.fresh_symbol_count).unwrap_or_default(),
                "stale_symbol_count": health.map(|row| row.stale_symbol_count).unwrap_or_default(),
                "avg_latency_ms": health.and_then(|row| row.avg_latency_ms),
                "last_book_update_at": health.and_then(|row| row.last_book_update_at),
                "usdt_total": account.usdt_total,
                "usdt_available": account.usdt_available,
                "coin_balance_usdt": account.coin_balance_usdt,
                "coin_assets": account.coin_assets,
                "fee_aux_asset": account.fee_aux_asset,
                "fee_aux_total": account.fee_aux_total,
                "fee_aux_available": account.fee_aux_available,
                "fee_aux_valuation_usdt": account.fee_aux_valuation_usdt,
                "total_equity_usdt": account.total_equity_usdt,
                "asset_count": account.asset_count,
            })
        })
        .collect()
}

fn spot_account_console_rows(model: &DashboardReadModel) -> Vec<Value> {
    spot_exchange_console_rows(model)
        .into_iter()
        .map(|mut row| {
            if let Some(object) = row.as_object_mut() {
                object.insert("account_id".to_string(), json!("default"));
                object.insert("account_label".to_string(), json!("default"));
                object.insert("quote_asset".to_string(), json!("USDT"));
            }
            row
        })
        .collect()
}

fn spot_position_console_rows(model: &DashboardReadModel) -> Vec<Value> {
    model
        .inventory
        .iter()
        .filter(|row| row.market_type == rustcta::exchanges::unified::MarketType::Spot)
        .map(|row| {
            json!({
                "exchange": normalize_exchange_name(&row.exchange),
                "asset": row.asset,
                "total": row.total,
                "available": row.available,
                "locked_by_exchange": row.locked_by_exchange,
                "locally_reserved": row.locally_reserved,
                "effective_available": row.effective_available,
                "valuation_usdt": row.valuation_usdt,
            })
        })
        .collect()
}

async fn spot_arb_target_exchanges(state: &AppState) -> Result<Vec<String>> {
    let content = tokio::fs::read_to_string(&state.strategy_config)
        .await
        .with_context(|| format!("read strategy config {}", state.strategy_config.display()))?;
    let value: Value = serde_yaml::from_str(&content)
        .with_context(|| format!("parse strategy config {}", state.strategy_config.display()))?;
    let preflight = value
        .get("live_preflight")
        .and_then(|item| item.get("exchanges"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(normalize_exchange_name)
                .filter(|exchange| !exchange.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !preflight.is_empty() {
        return Ok(preflight);
    }
    let configured = value
        .get("exchanges")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(normalize_exchange_name)
                .filter(|exchange| !exchange.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if configured.iter().any(|exchange| exchange == "gateio")
        && configured.iter().any(|exchange| exchange == "bitget")
    {
        return Ok(vec!["gateio".to_string(), "bitget".to_string()]);
    }
    Ok(configured.into_iter().take(2).collect())
}

fn filter_console_rows_by_exchange(rows: Vec<Value>, target_exchanges: &[String]) -> Vec<Value> {
    if target_exchanges.is_empty() {
        return rows;
    }
    let target = target_exchanges
        .iter()
        .map(|exchange| normalize_exchange_name(exchange))
        .collect::<BTreeSet<_>>();
    rows.into_iter()
        .filter(|row| {
            row.get("exchange")
                .and_then(Value::as_str)
                .map(normalize_exchange_name)
                .is_some_and(|exchange| target.contains(&exchange))
        })
        .collect()
}

fn strategy_order_rows(
    model: &DashboardReadModel,
    selected_set: Option<&BTreeSet<String>>,
) -> Vec<Value> {
    if model.open_orders_last_updated_at.is_some() {
        return model
            .open_orders
            .iter()
            .filter(|order| {
                selected_set
                    .map(|symbols| {
                        symbols.is_empty() || symbols.contains(&normalize_symbol(&order.symbol))
                    })
                    .unwrap_or(true)
            })
            .map(open_order_row)
            .collect();
    }

    model
        .live_dry_run_orders
        .iter()
        .filter(|plan| {
            selected_set
                .map(|symbols| {
                    symbols.is_empty() || symbols.contains(&normalize_symbol(&plan.symbol))
                })
                .unwrap_or(true)
        })
        .map(live_dry_run_order_row)
        .collect()
}

fn open_order_row(order: &rustcta::exchanges::unified::OrderResponse) -> Value {
    let remaining_quantity = (order.quantity - order.filled_quantity).max(0.0);
    let display_price = order.price.or(order.average_price);
    json!({
        "timestamp": order.updated_at.unwrap_or(order.created_at),
        "created_at": order.created_at,
        "updated_at": order.updated_at,
        "exchange": normalize_exchange_name(&order.exchange),
        "market_type": order.market_type,
        "symbol": order.symbol,
        "side": format!("{:?}", order.side),
        "order_type": format!("{:?}", order.order_type),
        "status": format!("{:?}", order.status),
        "quantity": order.quantity,
        "filled_quantity": order.filled_quantity,
        "remaining_quantity": remaining_quantity,
        "price": display_price,
        "notional": display_price.map(|price| price * remaining_quantity).unwrap_or(0.0),
        "order_id": order.order_id,
        "client_order_id": order.client_order_id,
        "source": "exchange_open_order",
        "would_submit": false,
    })
}

fn live_dry_run_order_row(plan: &rustcta::execution::LiveDryRunOrderPlan) -> Value {
    json!({
        "timestamp": plan.timestamp,
        "exchange": normalize_exchange_name(&plan.exchange),
        "market_type": plan.market_type,
        "symbol": plan.symbol,
        "side": format!("{:?}", plan.side),
        "order_type": format!("{:?}", plan.order_type),
        "status": if plan.validation_result.passed { "planned" } else { "rejected" },
        "quantity": plan.quantity,
        "filled_quantity": 0.0,
        "remaining_quantity": plan.quantity,
        "price": plan.price,
        "notional": plan.notional,
        "order_id": Value::Null,
        "client_order_id": plan.client_order_id,
        "source": "live_dry_run_plan",
        "would_submit": plan.would_submit,
        "rejection_reason": plan.rejection_reason,
    })
}

#[derive(Debug, Default)]
struct SpotAccountTotals {
    usdt_total: f64,
    usdt_available: f64,
    coin_balance_usdt: f64,
    coin_assets: Vec<Value>,
    fee_aux_asset: Option<String>,
    fee_aux_total: f64,
    fee_aux_available: f64,
    fee_aux_valuation_usdt: Option<f64>,
    total_equity_usdt: f64,
    asset_count: usize,
}

fn spot_account_totals(model: &DashboardReadModel, exchange: &str) -> SpotAccountTotals {
    let mut totals = SpotAccountTotals::default();
    totals.fee_aux_asset = fee_aux_asset_for_exchange(exchange).map(str::to_string);
    for row in model.inventory.iter().filter(|row| {
        row.market_type == rustcta::exchanges::unified::MarketType::Spot
            && normalize_exchange_name(&row.exchange) == exchange
    }) {
        totals.asset_count += 1;
        if row.asset.eq_ignore_ascii_case("USDT") {
            totals.usdt_total += row.total;
            totals.usdt_available += row.available;
            totals.total_equity_usdt += row.total;
        } else if totals
            .fee_aux_asset
            .as_deref()
            .is_some_and(|asset| row.asset.eq_ignore_ascii_case(asset))
        {
            totals.fee_aux_total += row.total;
            totals.fee_aux_available += row.effective_available;
            totals.fee_aux_valuation_usdt = row.valuation_usdt.or(totals.fee_aux_valuation_usdt);
            if let Some(value) = row.valuation_usdt {
                totals.total_equity_usdt += value;
            }
        } else if let Some(value) = row.valuation_usdt {
            totals.coin_balance_usdt += value;
            totals.total_equity_usdt += value;
            totals.coin_assets.push(json!({
                "asset": row.asset,
                "total": row.total,
                "available": row.available,
                "effective_available": row.effective_available,
                "valuation_usdt": row.valuation_usdt,
            }));
        } else if row.total.abs() > 0.0 || row.available.abs() > 0.0 {
            totals.coin_assets.push(json!({
                "asset": row.asset,
                "total": row.total,
                "available": row.available,
                "effective_available": row.effective_available,
                "valuation_usdt": row.valuation_usdt,
            }));
        }
    }
    totals
}

fn fee_aux_asset_for_exchange(exchange: &str) -> Option<&'static str> {
    match normalize_exchange_name(exchange).as_str() {
        "binance" => Some("BNB"),
        "okx" => Some("OKB"),
        "gateio" => Some("GT"),
        "bitget" => Some("BGB"),
        "kucoin" => Some("KCS"),
        "mexc" => Some("MX"),
        "coinex" => Some("CET"),
        "htx" => Some("HTX"),
        "bitmart" => Some("BMX"),
        _ => None,
    }
}

fn initial_entry_status_view(
    dry_run_plans: &[rustcta::execution::LiveDryRunOrderPlan],
    active_control_symbols: &BTreeSet<String>,
    config: &ConfigSummaryView,
) -> Value {
    let target = config.max_enabled_arbitrage_symbols.unwrap_or(5);
    let mut by_symbol = BTreeMap::<String, Value>::new();
    for symbol in active_control_symbols {
        by_symbol.insert(
            symbol.clone(),
            json!({
                "symbol": symbol,
                "exchange": Value::Null,
                "notional": Value::Null,
                "passed": true,
                "rejection_reason": Value::Null,
                "timestamp": Value::Null,
                "source": "spot_control_active",
            }),
        );
    }
    for plan in dry_run_plans
        .iter()
        .filter(|plan| plan.intent == "initial_entry")
    {
        let symbol = normalize_symbol(&plan.symbol);
        by_symbol.insert(
            symbol.clone(),
            json!({
                "symbol": symbol,
                "exchange": plan.exchange,
                "notional": plan.notional,
                "passed": plan.validation_result.passed,
                "rejection_reason": plan.rejection_reason,
                "timestamp": plan.timestamp,
            }),
        );
    }
    let rows = by_symbol.into_values().collect::<Vec<_>>();
    let completed = rows
        .iter()
        .filter(|row| row.get("passed").and_then(Value::as_bool).unwrap_or(false))
        .count();
    let failed = rows
        .iter()
        .filter(|row| {
            !row.get("passed").and_then(Value::as_bool).unwrap_or(false)
                && row
                    .get("rejection_reason")
                    .and_then(Value::as_str)
                    .is_some_and(|reason| reason != "pending")
        })
        .count();
    json!({
        "target": target,
        "completed": completed,
        "failed": failed,
        "pending": target.saturating_sub(completed + failed),
        "observed": rows.len(),
        "initial_entry_notional_usdt": config.initial_entry_notional_usdt.unwrap_or(5.0),
        "rows": rows,
    })
}

fn arbitrage_slot_status_view(
    opportunities: &[rustcta::strategies::arbitrage_core::ArbitrageOpportunityAnalysis],
    dry_run_plans: &[rustcta::execution::LiveDryRunOrderPlan],
    active_control_symbols: &BTreeSet<String>,
    config: &ConfigSummaryView,
) -> Value {
    const ACTIVE_SLOT_WINDOW_SECONDS: i64 = 600;
    let limit = config.max_enabled_arbitrage_symbols.unwrap_or(5);
    let cutoff = Utc::now() - chrono::Duration::seconds(ACTIVE_SLOT_WINDOW_SECONDS);
    let mut active_symbols = dry_run_plans
        .iter()
        .filter(|plan| plan.intent == "arbitrage" && plan.timestamp >= cutoff)
        .map(|plan| normalize_symbol(&plan.symbol))
        .collect::<BTreeSet<_>>();
    active_symbols.extend(active_control_symbols.iter().cloned());
    let blocked_symbols = opportunities
        .iter()
        .filter(|opportunity| {
            !opportunity.accepted
                && opportunity.rejection_reasons.iter().any(|reason| {
                    reason == "ControlPlaneBlocked"
                        || reason.contains("max_enabled_arbitrage_symbols")
                        || reason.contains("control")
                })
        })
        .map(|opportunity| normalize_symbol(&opportunity.symbol))
        .filter(|symbol| !active_symbols.contains(symbol))
        .collect::<BTreeSet<_>>();
    json!({
        "limit": limit,
        "used": active_symbols.len(),
        "remaining": limit.saturating_sub(active_symbols.len()),
        "active_window_seconds": ACTIVE_SLOT_WINDOW_SECONDS,
        "active_symbols": active_symbols.into_iter().collect::<Vec<_>>(),
        "blocked_symbols": blocked_symbols.into_iter().collect::<Vec<_>>(),
    })
}

fn active_spot_control_symbols(
    spot_control: Option<&SpotControlReadModel>,
    selected_set: &BTreeSet<String>,
) -> BTreeSet<String> {
    spot_control
        .map(|control| {
            control
                .symbols
                .iter()
                .filter(|symbol| symbol.lifecycle_state == SpotSymbolLifecycleState::Active)
                .map(|symbol| normalize_symbol(&symbol.internal_symbol))
                .filter(|symbol| selected_set.is_empty() || selected_set.contains(symbol))
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default()
}

fn cross_arb_open_orders(
    model: &DashboardReadModel,
    private_events: &[Value],
    order_events: &[Value],
) -> Vec<Value> {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    if model.open_orders_last_updated_at.is_some() {
        for order in model.open_orders.iter().filter(|order| {
            order
                .client_order_id
                .as_deref()
                .is_some_and(|id| id.to_ascii_lowercase().contains("crossarb"))
        }) {
            let mut row = open_order_row(order);
            row["source"] = json!("snapshot_open_order");
            if seen.insert(cross_arb_order_key(&row)) {
                rows.push(row);
            }
        }
    }
    for row in order_events
        .iter()
        .filter(|row| !cross_arb_order_status_is_terminal(&text_field(row, "status")))
    {
        let quantity = numeric_field(row, "quantity");
        let filled_quantity = numeric_field(row, "filled_quantity");
        let remaining_qty = (quantity - filled_quantity).max(0.0);
        let price = number_field_any(row, &["price", "average_fill_price"]);
        let mapped = json!({
            "source": "order_event",
            "exchange": text_field(row, "exchange"),
            "symbol": text_field(row, "canonical_symbol").if_empty_then(|| text_field(row, "symbol")),
            "side": text_field(row, "side"),
            "price": price.map(Value::from).unwrap_or(Value::Null),
            "quantity": number_or_null(row, &["quantity"]),
            "remaining_qty": remaining_qty,
            "notional": price.map(|price| price * remaining_qty).map(Value::from).unwrap_or(Value::Null),
            "status": text_field(row, "status"),
            "order_id": text_field(row, "order_id"),
            "client_order_id": text_field(row, "client_order_id"),
            "updated_at": text_field(row, "timestamp").if_empty_then(|| text_field(row, "recorded_at")),
        });
        if seen.insert(cross_arb_order_key(&mapped)) {
            rows.push(mapped);
        }
    }
    for row in private_events.iter().filter(|row| {
        let private_kind = text_field(row, "private_kind").to_ascii_lowercase();
        (private_kind.contains("order") || row.get("order_id").is_some())
            && !cross_arb_order_status_is_terminal(&text_field(row, "status"))
    }) {
        let mapped = json!({
            "source": "private_event",
            "exchange": text_field(row, "exchange"),
            "symbol": text_field(row, "canonical_symbol")
                .if_empty_then(|| text_field(row, "symbol")),
            "side": text_field(row, "side"),
            "price": number_or_null(row, &["price", "limit_price"]),
            "quantity": number_or_null(row, &["quantity", "qty"]),
            "remaining_qty": number_or_null(row, &["remaining_qty", "remaining_quantity"]),
            "notional": number_or_null(row, &["notional", "notional_usdt"]),
            "status": text_field(row, "status"),
            "order_id": text_field(row, "order_id"),
            "client_order_id": text_field(row, "client_order_id"),
            "updated_at": text_field(row, "recorded_at"),
        });
        if seen.insert(cross_arb_order_key(&mapped)) {
            rows.push(mapped);
        }
    }
    rows.truncate(120);
    rows
}

fn cross_arb_order_status_is_terminal(status: &str) -> bool {
    let status = status.trim().to_ascii_lowercase();
    matches!(
        status.as_str(),
        "filled" | "closed" | "canceled" | "cancelled" | "rejected" | "expired" | "failed" | "done"
    )
}

fn cross_arb_order_key(row: &Value) -> String {
    let client_order_id = text_field(row, "client_order_id");
    let order_id = text_field(row, "order_id");
    let order_key = if !client_order_id.is_empty() {
        client_order_id
    } else {
        order_id
    };
    format!(
        "{}:{}:{}:{}",
        text_field(row, "exchange"),
        text_field(row, "symbol"),
        text_field(row, "side"),
        order_key
    )
}

fn cross_arb_account_console(
    settings: &Value,
    private_events: &[Value],
    api_key_values: &BTreeMap<String, String>,
) -> Vec<Value> {
    let balance_rows = private_events
        .iter()
        .filter(|row| text_field(row, "private_kind").eq_ignore_ascii_case("balance"))
        .collect::<Vec<_>>();
    let mut rows = settings
        .get("exchanges")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|row| bool_field(row, "enabled"))
        .map(|row| {
            let exchange = text_field(row, "exchange");
            let account_id = text_field(row, "account_id");
            let account_id = if account_id.is_empty() {
                "default".to_string()
            } else {
                account_id
            };
            let env_prefix = text_field(row, "env_prefix");
            let credential_namespace =
                cross_arb_credential_namespace(&exchange, &account_id, &env_prefix);
            let credential_status = cross_arb_account_credential_status(
                &exchange,
                &account_id,
                &credential_namespace,
                api_key_values,
            );
            let latest_balance = balance_rows
                .iter()
                .find(|balance| {
                    text_field(balance, "exchange").eq_ignore_ascii_case(&exchange)
                        && text_field(balance, "account_id")
                            .if_empty_then(|| "default".to_string())
                            .eq_ignore_ascii_case(&account_id)
                })
                .or_else(|| {
                    balance_rows
                        .iter()
                        .find(|balance| text_field(balance, "exchange").eq_ignore_ascii_case(&exchange))
                });
            json!({
                "exchange": exchange,
                "account_id": account_id,
                "env_prefix": env_prefix,
                "credential_namespace": credential_namespace,
                "private_rest_enabled": bool_field(row, "private_rest_enabled"),
                "private_ws_enabled": bool_field(row, "private_ws_enabled"),
                "credentials": credential_status,
                "status": latest_balance.map(|_| "online").unwrap_or("configured"),
                "total": latest_balance.map(|row| number_or_null(row, &["total", "equity", "wallet_balance"])).unwrap_or(Value::Null),
                "available": latest_balance.map(|row| number_or_null(row, &["available", "available_balance", "free"])).unwrap_or(Value::Null),
                "recorded_at": latest_balance.map(|row| text_field(row, "recorded_at")).unwrap_or_default(),
            })
        })
        .collect::<Vec<_>>();
    rows.truncate(40);
    rows
}

fn cross_arb_credential_namespace(exchange: &str, account_id: &str, env_prefix: &str) -> String {
    let default_prefix = env_exchange_prefix(exchange);
    let configured_prefix = env_prefix.trim();
    if !configured_prefix.is_empty() && !configured_prefix.eq_ignore_ascii_case(&default_prefix) {
        return configured_prefix.to_string();
    }
    let account_id = normalize_account_id(account_id);
    if account_id == "default" {
        return if configured_prefix.is_empty() {
            default_prefix
        } else {
            configured_prefix.to_string()
        };
    }
    format!(
        "{}__{}_",
        default_prefix,
        account_id.to_ascii_uppercase().replace('-', "_")
    )
}

fn credential_namespace_for_env_prefix(
    exchange: &str,
    account_id: &str,
    env_prefix: &str,
) -> String {
    cross_arb_credential_namespace(exchange, account_id, env_prefix).to_ascii_uppercase()
}

fn cross_arb_account_credential_status(
    exchange: &str,
    account_id: &str,
    credential_namespace: &str,
    values: &BTreeMap<String, String>,
) -> Value {
    let Some(schema) = exchange_api_key_schema(exchange) else {
        return json!({
            "supported": false,
            "required": 0,
            "configured": 0,
            "configured_all_required": false,
        });
    };
    let account_id = normalize_account_id(account_id);
    let required = schema.fields.iter().filter(|field| field.required).count();
    let configured = schema
        .fields
        .iter()
        .filter(|field| field.required)
        .filter(|field| {
            cross_arb_field_value_present_for_namespace(
                values,
                schema,
                field,
                &account_id,
                credential_namespace,
            )
        })
        .count();
    let missing_required = schema
        .fields
        .iter()
        .filter(|field| field.required)
        .filter(|field| {
            !cross_arb_field_value_present_for_namespace(
                values,
                schema,
                field,
                &account_id,
                credential_namespace,
            )
        })
        .map(|field| {
            json!({
                "code": credential_field_code(field.field),
                "display": credential_field_display(field.label),
                "expected_env_parts": expected_env_key_parts_for_namespace(
                    schema,
                    field,
                    &account_id,
                    credential_namespace,
                ),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "supported": true,
        "required": required,
        "configured": configured,
        "configured_all_required": required > 0 && configured == required,
        "missing_required": missing_required,
    })
}

fn cross_arb_field_value_present_for_namespace(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
) -> bool {
    configured_field_value_for_namespace(values, schema, field, account_id, credential_namespace)
        .is_some()
}

fn expected_env_keys_for_account(
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
) -> Vec<String> {
    if account_id == "default" {
        field
            .aliases
            .iter()
            .map(|alias| alias.to_string())
            .collect()
    } else {
        vec![account_env_key(schema, account_id, field.field)]
    }
}

fn expected_env_keys_for_namespace(
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
) -> Vec<String> {
    let default_prefix = env_exchange_prefix(schema.exchange);
    if credential_namespace.eq_ignore_ascii_case(&default_prefix) {
        return expected_env_keys_for_account(schema, field, account_id);
    }
    namespace_env_keys(credential_namespace, field.field)
}

fn namespace_env_keys(credential_namespace: &str, field: &str) -> Vec<String> {
    let suffix = field.to_ascii_uppercase();
    let mut keys = Vec::new();
    if credential_namespace.ends_with('_') {
        keys.push(format!("{credential_namespace}_{suffix}"));
        keys.push(format!("{credential_namespace}{suffix}"));
        keys.push(format!(
            "{}_{}",
            credential_namespace.trim_end_matches('_'),
            suffix
        ));
    } else {
        keys.push(format!("{credential_namespace}_{suffix}"));
    }
    let mut seen = BTreeSet::new();
    keys.retain(|key| seen.insert(key.clone()));
    keys
}

fn expected_env_key_parts_for_namespace(
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
) -> Vec<Vec<String>> {
    expected_env_keys_for_namespace(schema, field, account_id, credential_namespace)
        .into_iter()
        .map(|key| key.split('_').map(str::to_string).collect())
        .collect()
}

fn credential_field_code(field: &str) -> &'static str {
    match field {
        "api_key" => "key",
        "api_secret" => "credential",
        "passphrase" => "pass_phrase",
        "account_id" => "account",
        _ => "credential",
    }
}

fn credential_field_display(label: &str) -> String {
    label
        .replace("API Secret", "API Credential")
        .replace("Private Key", "Private Credential")
        .replace("Passphrase", "Pass phrase")
}

fn cross_arb_account_readiness(account_console: &[Value]) -> Value {
    let enabled_accounts = account_console.len();
    let mut ready_accounts = 0usize;
    let mut missing = Vec::new();
    for row in account_console {
        let credentials = row.get("credentials").unwrap_or(&Value::Null);
        if bool_field(credentials, "configured_all_required") {
            ready_accounts += 1;
            continue;
        }
        missing.push(json!({
            "exchange": text_field(row, "exchange"),
            "account_id": text_field(row, "account_id").if_empty_then(|| "default".to_string()),
            "configured": credentials
                .get("configured")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            "required": credentials
                .get("required")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            "supported": bool_field(credentials, "supported"),
            "missing_required": credentials
                .get("missing_required")
                .cloned()
                .unwrap_or(Value::Array(Vec::new())),
        }));
    }
    json!({
        "enabled_accounts": enabled_accounts,
        "ready_accounts": ready_accounts,
        "missing_accounts": missing.len(),
        "all_required_credentials_configured": enabled_accounts > 0 && ready_accounts == enabled_accounts,
        "missing": missing,
    })
}

fn cross_arb_strategy_readiness(
    enabled_exchanges: &[String],
    enabled_symbols: &[String],
    events: &CrossArbEventSummary,
    account_readiness: &Value,
) -> Value {
    let expected_pairs = enabled_exchanges.len() * enabled_symbols.len();
    let instrument_pairs = cross_arb_unique_exchange_symbol_pairs(
        &events.instruments,
        &["canonical_symbol", "symbol"],
    );
    let snapshot_pairs = cross_arb_unique_exchange_symbol_pairs(
        &events.market_snapshots,
        &["symbol", "canonical_symbol"],
    );
    let expected_pair_set =
        cross_arb_expected_exchange_symbol_pairs(enabled_exchanges, enabled_symbols);
    let missing_instrument_pairs =
        cross_arb_missing_exchange_symbol_pairs(&expected_pair_set, &instrument_pairs);
    let missing_snapshot_pairs =
        cross_arb_missing_exchange_symbol_pairs(&expected_pair_set, &snapshot_pairs);
    let covered_instrument_pairs = expected_pairs.saturating_sub(missing_instrument_pairs.len());
    let covered_snapshot_pairs = expected_pairs.saturating_sub(missing_snapshot_pairs.len());
    let instrument_coverage_ok = expected_pairs > 0 && missing_instrument_pairs.is_empty();
    let market_snapshot_coverage_ok = expected_pairs > 0 && missing_snapshot_pairs.is_empty();
    let symbols_configured = enabled_symbols.len() >= 200;
    let exchanges_configured = enabled_exchanges.len() >= 2;
    let credentials_ready = bool_field(account_readiness, "all_required_credentials_configured");
    let live_small_ready = symbols_configured
        && exchanges_configured
        && instrument_coverage_ok
        && market_snapshot_coverage_ok
        && credentials_ready;
    let mut blockers = Vec::new();
    if !symbols_configured {
        blockers.push(json!("symbols"));
    }
    if !exchanges_configured {
        blockers.push(json!("exchanges"));
    }
    if !instrument_coverage_ok {
        blockers.push(json!("instrument_coverage"));
    }
    if !market_snapshot_coverage_ok {
        blockers.push(json!("market_snapshot_coverage"));
    }
    if !credentials_ready {
        blockers.push(json!("credentials"));
    }
    json!({
        "live_small_ready": live_small_ready,
        "blockers": blockers,
        "symbols_configured": symbols_configured,
        "configured_symbols": enabled_symbols.len(),
        "target_symbols": 200,
        "exchanges_configured": exchanges_configured,
        "configured_exchanges": enabled_exchanges.len(),
        "expected_exchange_symbol_pairs": expected_pairs,
        "instrument_pairs": instrument_pairs.len(),
        "covered_instrument_pairs": covered_instrument_pairs,
        "instrument_coverage_ok": instrument_coverage_ok,
        "missing_instrument_pairs": missing_instrument_pairs.len(),
        "missing_instrument_pair_samples": cross_arb_pair_samples(&missing_instrument_pairs),
        "market_snapshot_pairs": snapshot_pairs.len(),
        "covered_market_snapshot_pairs": covered_snapshot_pairs,
        "market_snapshot_coverage_ok": market_snapshot_coverage_ok,
        "missing_market_snapshot_pairs": missing_snapshot_pairs.len(),
        "missing_market_snapshot_pair_samples": cross_arb_pair_samples(&missing_snapshot_pairs),
        "credentials_ready": credentials_ready,
    })
}

fn cross_arb_expected_exchange_symbol_pairs(
    enabled_exchanges: &[String],
    enabled_symbols: &[String],
) -> BTreeSet<(String, String)> {
    enabled_exchanges
        .iter()
        .map(|exchange| exchange.trim().to_ascii_lowercase())
        .filter(|exchange| !exchange.is_empty())
        .flat_map(|exchange| {
            enabled_symbols
                .iter()
                .map(|symbol| normalize_symbol(symbol))
                .filter(|symbol| !symbol.is_empty())
                .map(move |symbol| (exchange.clone(), symbol))
        })
        .collect()
}

fn cross_arb_missing_exchange_symbol_pairs(
    expected: &BTreeSet<(String, String)>,
    actual: &BTreeSet<(String, String)>,
) -> Vec<(String, String)> {
    expected.difference(actual).cloned().collect()
}

fn cross_arb_pair_samples(pairs: &[(String, String)]) -> Vec<Value> {
    pairs
        .iter()
        .take(CROSS_ARB_READINESS_MISSING_PAIR_DISPLAY_LIMIT)
        .map(|(exchange, symbol)| {
            json!({
                "exchange": exchange,
                "symbol": symbol,
            })
        })
        .collect()
}

fn cross_arb_unique_exchange_symbol_pairs(
    rows: &[Value],
    symbol_fields: &[&str],
) -> BTreeSet<(String, String)> {
    rows.iter()
        .filter_map(|row| {
            let exchange = text_field(row, "exchange").to_ascii_lowercase();
            let symbol = symbol_fields
                .iter()
                .filter_map(|field| row.get(*field))
                .flat_map(cross_arb_symbol_values)
                .map(|symbol| normalize_symbol(&symbol))
                .find(|symbol| !symbol.is_empty())?;
            (!exchange.is_empty()).then_some((exchange, symbol))
        })
        .collect()
}

fn cross_arb_position_bundles(
    hedge_records: &[Value],
    bundles: &[Value],
    close_metrics: &[Value],
    enabled_exchanges: &[String],
) -> Vec<Value> {
    let enabled_exchanges = cross_arb_exchange_filter(enabled_exchanges);
    let close_metrics_by_bundle = latest_cross_arb_close_metrics_by_bundle(close_metrics);
    let mut rows = hedge_records
        .iter()
        .filter(|row| cross_arb_row_matches_exchange_filter(row, &enabled_exchanges))
        .filter(|row| {
            !bool_field(row, "is_closed")
                && !text_field(row, "status").eq_ignore_ascii_case("closed")
        })
        .map(|row| {
            json!({
                "source": "hedge_record",
                "bundle_id": text_field(row, "bundle_id"),
                "symbol": text_field(row, "canonical_symbol"),
                "long_exchange": text_field(row, "long_exchange"),
                "short_exchange": text_field(row, "short_exchange"),
                "status": text_field(row, "status"),
                "target_notional_usdt": number_or_null(row, &["target_notional_usdt"]),
                "entry_net_edge_pct": number_or_null(row, &["entry_net_edge_pct"]),
                "close_spread_pct": number_or_null(row, &["close_spread_pct"]),
                "close_net_profit_pct": number_or_null(row, &["close_net_profit_pct"]),
                "close_candidate_profit_pct": Value::Null,
                "close_candidate_spread_pct": Value::Null,
                "close_threshold_pct": Value::Null,
                "closeable": false,
                "close_maker_exchange": "",
                "close_maker_side": "",
                "close_maker_price": Value::Null,
                "close_metric_updated_at": "",
                "opened_at": text_field(row, "opened_at"),
                "updated_at": text_field(row, "updated_at").if_empty_then(|| text_field(row, "recorded_at")),
            })
        })
        .collect::<Vec<_>>();
    rows.extend(bundles.iter().filter_map(|row| {
        if !cross_arb_row_matches_exchange_filter(row, &enabled_exchanges) {
            return None;
        }
        let status = text_field(row, "status");
        (!status.eq_ignore_ascii_case("closed")).then(|| {
            json!({
                "source": "bundle",
                "bundle_id": text_field(row, "bundle_id"),
                "symbol": text_field(row, "canonical_symbol"),
                "long_exchange": text_field(row, "long_exchange"),
                "short_exchange": text_field(row, "short_exchange"),
                "status": status,
                "target_notional_usdt": number_or_null(row, &["target_notional_usdt"]),
                "entry_net_edge_pct": number_or_null(row, &["entry_net_edge_pct"]),
                "close_spread_pct": number_or_null(row, &["close_spread_pct"]),
                "close_net_profit_pct": number_or_null(row, &["close_net_profit_pct"]),
                "close_candidate_profit_pct": Value::Null,
                "close_candidate_spread_pct": Value::Null,
                "close_threshold_pct": Value::Null,
                "closeable": false,
                "close_maker_exchange": "",
                "close_maker_side": "",
                "close_maker_price": Value::Null,
                "close_metric_updated_at": "",
                "opened_at": text_field(row, "opened_at"),
                "updated_at": text_field(row, "updated_at").if_empty_then(|| text_field(row, "recorded_at")),
            })
        })
    }));
    for row in &mut rows {
        let bundle_id = text_field(row, "bundle_id");
        if let Some(metric) = close_metrics_by_bundle.get(&bundle_id) {
            merge_cross_arb_close_metric(row, metric);
        }
    }
    rows.truncate(120);
    rows
}

fn latest_cross_arb_close_metrics_by_bundle(close_metrics: &[Value]) -> BTreeMap<String, Value> {
    let mut latest = BTreeMap::<String, Value>::new();
    for row in close_metrics {
        let bundle_id = text_field(row, "bundle_id");
        if bundle_id.is_empty() {
            continue;
        }
        let should_replace = latest
            .get(&bundle_id)
            .map(|existing| {
                cross_arb_close_metric_time_key(row) >= cross_arb_close_metric_time_key(existing)
            })
            .unwrap_or(true);
        if should_replace {
            latest.insert(bundle_id, row.clone());
        }
    }
    latest
}

fn merge_cross_arb_close_metric(row: &mut Value, metric: &Value) {
    let metric_time = cross_arb_close_metric_time_key(metric);
    let current_time = text_field(row, "updated_at");
    if let Some(object) = row.as_object_mut() {
        object.insert(
            "close_candidate_profit_pct".to_string(),
            number_or_null(metric, &["close_profit_pct"]),
        );
        object.insert(
            "close_candidate_spread_pct".to_string(),
            number_or_null(metric, &["close_spread_pct"]),
        );
        object.insert(
            "close_threshold_pct".to_string(),
            number_or_null(metric, &["close_threshold_pct"]),
        );
        object.insert(
            "closeable".to_string(),
            json!(bool_field(metric, "closeable")),
        );
        object.insert(
            "close_maker_exchange".to_string(),
            json!(text_field(metric, "close_maker_exchange")),
        );
        object.insert(
            "close_maker_side".to_string(),
            json!(text_field(metric, "close_maker_side")),
        );
        object.insert(
            "close_maker_price".to_string(),
            number_or_null(metric, &["close_maker_price"]),
        );
        object.insert(
            "close_metric_updated_at".to_string(),
            json!(metric_time.clone()),
        );
        if !metric_time.is_empty() && (current_time.is_empty() || metric_time > current_time) {
            object.insert("updated_at".to_string(), json!(metric_time));
        }
    }
}

fn cross_arb_close_metric_time_key(row: &Value) -> String {
    text_field(row, "updated_at").if_empty_then(|| text_field(row, "recorded_at"))
}

fn cross_arb_arbitrage_results(
    hedge_records: &[Value],
    enabled_exchanges: &[String],
) -> Vec<Value> {
    let enabled_exchanges = cross_arb_exchange_filter(enabled_exchanges);
    let mut latest_by_bundle = BTreeMap::<String, Value>::new();
    for row in hedge_records.iter().filter(|row| {
        cross_arb_row_matches_exchange_filter(row, &enabled_exchanges)
            && (bool_field(row, "is_closed")
                || text_field(row, "status").eq_ignore_ascii_case("closed")
                || row
                    .get("close_net_profit_pct")
                    .and_then(Value::as_f64)
                    .is_some())
    }) {
        let bundle_id = text_field(row, "bundle_id");
        if bundle_id.is_empty() {
            continue;
        }
        let should_replace = latest_by_bundle
            .get(&bundle_id)
            .map(|existing| cross_arb_result_time_key(row) >= cross_arb_result_time_key(existing))
            .unwrap_or(true);
        if should_replace {
            latest_by_bundle.insert(bundle_id, row.clone());
        }
    }
    let mut rows = latest_by_bundle
        .values()
        .map(cross_arb_result_row_from_hedge_record)
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        cross_arb_result_time_key(left)
            .cmp(&cross_arb_result_time_key(right))
            .then_with(|| text_field(left, "bundle_id").cmp(&text_field(right, "bundle_id")))
    });
    let mut cumulative_profit_usdt = 0.0;
    for row in &mut rows {
        cumulative_profit_usdt += numeric_field(row, "realized_profit_usdt");
        if let Some(object) = row.as_object_mut() {
            object.insert(
                "cumulative_profit_usdt".to_string(),
                json!(cumulative_profit_usdt),
            );
        }
    }
    rows.reverse();
    rows.truncate(CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT);
    rows
}

fn cross_arb_exchange_filter(enabled_exchanges: &[String]) -> BTreeSet<String> {
    enabled_exchanges
        .iter()
        .map(|exchange| exchange.trim().to_ascii_lowercase())
        .filter(|exchange| !exchange.is_empty())
        .collect()
}

fn cross_arb_row_matches_exchange_filter(
    row: &Value,
    enabled_exchanges: &BTreeSet<String>,
) -> bool {
    if enabled_exchanges.is_empty() {
        return true;
    }
    let leg_fields = ["long_exchange", "short_exchange"];
    let leg_exchanges = leg_fields
        .iter()
        .map(|field| text_field(row, field).to_ascii_lowercase())
        .filter(|exchange| !exchange.is_empty())
        .collect::<Vec<_>>();
    if !leg_exchanges.is_empty() {
        return leg_exchanges
            .iter()
            .all(|exchange| enabled_exchanges.contains(exchange));
    }
    let venue_fields = ["exchange", "maker_exchange", "taker_exchange"];
    let venue_exchanges = venue_fields
        .iter()
        .map(|field| text_field(row, field).to_ascii_lowercase())
        .filter(|exchange| !exchange.is_empty())
        .collect::<Vec<_>>();
    venue_exchanges.is_empty()
        || venue_exchanges
            .iter()
            .all(|exchange| enabled_exchanges.contains(exchange))
}

fn cross_arb_result_row_from_hedge_record(row: &Value) -> Value {
    let target_notional = numeric_field(row, "target_notional_usdt")
        .max(numeric_field(row, "target_notional"))
        .max(5.0);
    let close_profit_pct = numeric_field(row, "close_net_profit_pct");
    let realized_profit_usdt = number_field_any(
        row,
        &[
            "realized_profit_usdt",
            "close_net_profit_usdt",
            "net_profit_usdt",
            "profit_usdt",
        ],
    )
    .unwrap_or(close_profit_pct * target_notional);
    json!({
        "bundle_id": text_field(row, "bundle_id"),
        "record_id": text_field(row, "record_id"),
        "symbol": text_field(row, "canonical_symbol"),
        "long_exchange": text_field(row, "long_exchange"),
        "short_exchange": text_field(row, "short_exchange"),
        "entry_net_edge_pct": number_or_null(row, &["entry_net_edge_pct"]),
        "close_spread_pct": number_or_null(row, &["close_spread_pct"]),
        "close_net_profit_pct": close_profit_pct,
        "target_notional_usdt": target_notional,
        "realized_profit_usdt": realized_profit_usdt,
        "cumulative_profit_usdt": 0.0,
        "opened_at": text_field(row, "opened_at"),
        "closed_at": text_field(row, "closed_at").if_empty_then(|| text_field(row, "updated_at")),
        "updated_at": text_field(row, "updated_at").if_empty_then(|| text_field(row, "recorded_at")),
    })
}

fn cross_arb_result_time_key(row: &Value) -> String {
    text_field(row, "closed_at")
        .if_empty_then(|| text_field(row, "updated_at"))
        .if_empty_then(|| text_field(row, "recorded_at"))
}

fn cross_arb_profit_summary(results: &[Value]) -> Value {
    let realized_profit_usdt = results
        .iter()
        .map(|row| numeric_field(row, "realized_profit_usdt"))
        .sum::<f64>();
    let wins = results
        .iter()
        .filter(|row| numeric_field(row, "realized_profit_usdt") > 0.0)
        .count();
    let losses = results
        .iter()
        .filter(|row| numeric_field(row, "realized_profit_usdt") < 0.0)
        .count();
    let win_rate = if results.is_empty() {
        0.0
    } else {
        wins as f64 / results.len() as f64
    };
    json!({
        "closed_arbitrages": results.len(),
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "realized_profit_usdt": realized_profit_usdt,
        "latest_result_at": results.first().map(|row| text_field(row, "updated_at")).unwrap_or_default(),
    })
}

async fn cross_arb_profit_history(state: &AppState) -> Result<Value> {
    let config = read_cross_arb_config(state).await?;
    let event_dir = PathBuf::from(&config.persistence.jsonl_dir);
    let events = filter_cross_arb_profit_outliers(read_cross_arb_events(&event_dir).await?);
    let enabled_exchanges = config
        .universe
        .enabled_exchanges
        .iter()
        .map(|exchange| exchange.as_str().to_string())
        .collect::<Vec<_>>();
    Ok(cross_arb_profit_history_from_results(
        &cross_arb_arbitrage_results(&events.hedge_records, &enabled_exchanges),
    ))
}

fn cross_arb_profit_history_from_results(results: &[Value]) -> Value {
    let mut rows = results.iter().rev().cloned().collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        cross_arb_result_time_key(left)
            .cmp(&cross_arb_result_time_key(right))
            .then_with(|| text_field(left, "bundle_id").cmp(&text_field(right, "bundle_id")))
    });
    Value::Array(
        rows.into_iter()
            .map(|row| {
                let profit = numeric_field(&row, "cumulative_profit_usdt");
                json!({
                    "series": "cross_arb_strategy_profit",
                    "trade_id": text_field(&row, "bundle_id"),
                    "symbol": text_field(&row, "symbol"),
                    "timestamp": cross_arb_result_time_key(&row),
                    "trade_profit_usdt": numeric_field(&row, "realized_profit_usdt"),
                    "profit_usdt": profit,
                    "total_usdt": profit,
                    "available_usdt": profit,
                    "long_exchange": text_field(&row, "long_exchange"),
                    "short_exchange": text_field(&row, "short_exchange"),
                })
            })
            .collect(),
    )
}

#[derive(Debug, Default)]
struct CrossArbEventSummary {
    event_dir: String,
    files: Vec<String>,
    latest_event_at: Option<String>,
    total_valid_events: usize,
    parse_errors: usize,
    profit_outlier_filtered: usize,
    events: Vec<Value>,
    opportunities: Vec<Value>,
    signals: Vec<Value>,
    hedge_records: Vec<Value>,
    close_metrics: Vec<Value>,
    hedge_repair_tasks: Vec<Value>,
    market_snapshots: Vec<Value>,
    instruments: Vec<Value>,
    order_events: Vec<Value>,
    private_events: Vec<Value>,
    risk_events: Vec<Value>,
    bundles: Vec<Value>,
}

const CROSS_ARB_EVENT_DISPLAY_LIMIT: usize = 160;
const CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT: usize = 120;
const CROSS_ARB_MARKET_SNAPSHOT_DISPLAY_LIMIT: usize = 800;
const CROSS_ARB_INSTRUMENT_DISPLAY_LIMIT: usize = 800;
const CROSS_ARB_BUNDLE_DISPLAY_LIMIT: usize = 80;
const CROSS_ARB_JSONL_SCAN_LINE_LIMIT: usize = 200_000;
const CROSS_ARB_READINESS_MISSING_PAIR_DISPLAY_LIMIT: usize = 80;

async fn cross_arb_dashboard(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let model = read_snapshot(&state).await?;
        let snapshot_value = serde_json::to_value(&model).context("serialize snapshot")?;
        let relationships = filter_contract_values(
            snapshot_value.get("arbitrage_relationships"),
            &[
                "relationship_type",
                "long_market_type",
                "short_market_type",
                "market_type",
            ],
        );
        let opportunities = filter_contract_values(
            snapshot_value.get("arbitrage_opportunities"),
            &[
                "relationship_type",
                "buy_market_type",
                "sell_market_type",
                "market_type",
            ],
        );
        let books = filter_contract_values(snapshot_value.get("books"), &["market_type"]);
        let live_snapshot_available =
            !relationships.is_empty() || !opportunities.is_empty() || !books.is_empty();
        let config = read_cross_arb_config(&state).await.ok();
        let event_dir = config
            .as_ref()
            .map(|config| PathBuf::from(&config.persistence.jsonl_dir))
            .unwrap_or_else(|| PathBuf::from("logs/cross_exchange_arbitrage"));
        let events = read_cross_arb_events(&event_dir)
            .await
            .unwrap_or_else(|error| CrossArbEventSummary {
                parse_errors: 1,
                events: vec![json!({
                    "kind": "Error",
                    "recorded_at": Utc::now(),
                    "payload": { "message": error.to_string() }
                })],
                ..Default::default()
            });
        let events = filter_cross_arb_profit_outliers(events);
        let running_cross_arb = model
            .strategy
            .strategy_name
            .to_ascii_lowercase()
            .contains("cross_exchange_arbitrage");
        let settings = config
            .map(|config| {
                let mut view = cross_arb_settings_view(config);
                view.path = state.strategy_config.display().to_string();
                json!(view)
            })
            .unwrap_or(Value::Null);
        let event_enabled_exchanges = events
            .opportunities
            .iter()
            .flat_map(|row| {
                [
                    text_field(row, "long_exchange"),
                    text_field(row, "short_exchange"),
                    text_field(row, "maker_exchange"),
                    text_field(row, "taker_exchange"),
                ]
            })
            .filter(|value| !value.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let event_enabled_symbols = events
            .opportunities
            .iter()
            .filter_map(|row| {
                let value = text_field(row, "canonical_symbol");
                (!value.is_empty()).then_some(value)
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let config_enabled_exchanges = settings
            .get("enabled_exchanges")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let config_enabled_symbols = settings
            .get("symbols")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let enabled_exchanges = if config_enabled_exchanges.is_empty() {
            event_enabled_exchanges
        } else {
            config_enabled_exchanges
        };
        let enabled_symbols = if config_enabled_symbols.is_empty() {
            event_enabled_symbols
        } else {
            config_enabled_symbols
        };
        let events = filter_cross_arb_dashboard_scope(events, &enabled_exchanges, &enabled_symbols);
        let live_event_available = cross_arb_latest_event_is_fresh(&events, Utc::now());
        let data_source = if live_snapshot_available {
            "live_snapshot"
        } else if live_event_available {
            "live_events"
        } else if events.latest_event_at.is_some() {
            "historical_events"
        } else {
            "empty"
        };
        let open_signals = events
            .signals
            .iter()
            .filter(|row| text_field(row, "action").eq_ignore_ascii_case("open"))
            .count();
        let noop_signals = events
            .signals
            .iter()
            .filter(|row| text_field(row, "action").eq_ignore_ascii_case("noop"))
            .count();
        let can_open_opportunities = events
            .opportunities
            .iter()
            .filter(|row| bool_field(row, "can_open"))
            .count();
        let mut price_by_venue = latest_cross_arb_mid_prices(&events.market_snapshots);
        if let Ok(config) = read_cross_arb_config(&state).await {
            price_by_venue.extend(load_cross_arb_reference_prices(&config).await);
        }
        let instruments = events
            .instruments
            .iter()
            .cloned()
            .map(|row| {
                let exchange = text_field(&row, "exchange");
                let symbol = text_field(&row, "canonical_symbol")
                    .if_empty_then(|| text_field(&row, "symbol"));
                let price = price_by_venue
                    .get(&(exchange.clone(), symbol.clone()))
                    .copied();
                enrich_cross_arb_instrument_feasibility(
                    row,
                    price,
                    numeric_field(&settings, "target_notional_usdt"),
                    numeric_field(&settings, "max_notional_usdt"),
                )
            })
            .collect::<Vec<_>>();
        let instrument_feasibility = cross_arb_feasibility_summary(
            &instruments,
            enabled_symbols.len(),
            numeric_field(&settings, "min_common_exchanges") as usize,
        );
        let estimated_edge_usdt = events
            .signals
            .iter()
            .filter(|row| text_field(row, "action").eq_ignore_ascii_case("open"))
            .map(|row| {
                numeric_field(row, "expected_edge") * numeric_field(row, "max_notional_usdt")
            })
            .sum::<f64>();
        let open_orders = cross_arb_open_orders(&model, &events.private_events, &events.order_events)
            .into_iter()
            .filter(|row| cross_arb_row_matches_dashboard_scope(row, &enabled_exchanges, &enabled_symbols))
            .collect::<Vec<_>>();
        let api_key_values = read_env_store(&state.exchange_api_key_store)
            .await
            .unwrap_or_default();
        let account_console =
            cross_arb_account_console(&settings, &events.private_events, &api_key_values);
        let account_readiness = cross_arb_account_readiness(&account_console);
        let strategy_readiness = cross_arb_strategy_readiness(
            &enabled_exchanges,
            &enabled_symbols,
            &events,
            &account_readiness,
        );
        let position_bundles = cross_arb_position_bundles(
            &events.hedge_records,
            &events.bundles,
            &events.close_metrics,
            &enabled_exchanges,
        );
        let arbitrage_results =
            cross_arb_arbitrage_results(&events.hedge_records, &enabled_exchanges);
        let profit_summary = cross_arb_profit_summary(&arbitrage_results);
        let realized_close_pct = events
            .hedge_records
            .iter()
            .filter_map(|row| row.get("close_net_profit_pct").and_then(Value::as_f64))
            .sum::<f64>();
        let summary = json!({
            "snapshot_relationships": relationships.len(),
            "snapshot_opportunities": opportunities.len(),
            "snapshot_books": books.len(),
            "live_event_available": live_event_available,
            "valid_events": events.total_valid_events,
            "parse_errors": events.parse_errors,
            "profit_outlier_filtered": events.profit_outlier_filtered,
            "opportunities": events.opportunities.len(),
            "can_open_opportunities": can_open_opportunities,
            "signals": events.signals.len(),
            "open_signals": open_signals,
            "noop_signals": noop_signals,
            "hedge_records": events.hedge_records.len(),
            "close_metrics": events.close_metrics.len(),
            "repair_tasks": events.hedge_repair_tasks.len(),
            "market_snapshots": events.market_snapshots.len(),
            "instruments": instruments.len(),
            "known_instrument_symbols": numeric_field(&instrument_feasibility, "known_symbols"),
            "feasible_instrument_symbols": numeric_field(&instrument_feasibility, "feasible_symbols"),
            "order_events": events.order_events.len(),
            "private_events": events.private_events.len(),
            "open_orders": open_orders.len(),
            "position_bundles": position_bundles.len(),
            "closed_arbitrages": arbitrage_results.len(),
            "risk_events": events.risk_events.len(),
            "estimated_edge_usdt": estimated_edge_usdt,
            "realized_close_pct": realized_close_pct,
            "realized_profit_usdt": numeric_field(&profit_summary, "realized_profit_usdt"),
            "enabled_exchanges": enabled_exchanges,
            "enabled_symbols": enabled_symbols,
            "credential_ready_accounts": numeric_field(&account_readiness, "ready_accounts"),
            "credential_missing_accounts": numeric_field(&account_readiness, "missing_accounts"),
            "credentials_ready": bool_field(&account_readiness, "all_required_credentials_configured"),
            "live_small_ready": bool_field(&strategy_readiness, "live_small_ready"),
            "readiness_blockers": strategy_readiness.get("blockers").cloned().unwrap_or(Value::Array(Vec::new())),
            "instrument_pairs": numeric_field(&strategy_readiness, "instrument_pairs"),
            "market_snapshot_pairs": numeric_field(&strategy_readiness, "market_snapshot_pairs"),
        });
        let snapshot = json!({
            "generated_at": Utc::now(),
            "strategy": model.strategy,
            "trading_mode": model.trading_mode,
            "dry_run": model.dry_run,
            "live_trading_enabled": model.live_trading_enabled,
            "relationships": relationships,
            "opportunities": opportunities,
            "books": books,
        });
        let mut response = serde_json::Map::new();
        response.insert(
            "strategy_name".to_string(),
            json!("cross_exchange_arbitrage_usdt_perp"),
        );
        response.insert("data_source".to_string(), json!(data_source));
        response.insert(
            "is_current_runtime".to_string(),
            json!(running_cross_arb || live_event_available),
        );
        response.insert(
            "online".to_string(),
            json!(live_snapshot_available || live_event_available),
        );
        response.insert("target_refresh_ms".to_string(), json!(1000));
        response.insert("latest_event_at".to_string(), json!(events.latest_event_at));
        response.insert("event_dir".to_string(), json!(events.event_dir));
        response.insert("files".to_string(), json!(events.files));
        response.insert("settings".to_string(), settings);
        response.insert("summary".to_string(), summary);
        response.insert("snapshot".to_string(), snapshot);
        response.insert("opportunities".to_string(), json!(events.opportunities));
        response.insert("signals".to_string(), json!(events.signals));
        response.insert("hedge_records".to_string(), json!(events.hedge_records));
        response.insert("close_metrics".to_string(), json!(events.close_metrics));
        response.insert(
            "hedge_repair_tasks".to_string(),
            json!(events.hedge_repair_tasks),
        );
        response.insert("market_snapshots".to_string(), json!(events.market_snapshots));
        response.insert("instruments".to_string(), json!(instruments));
        response.insert(
            "instrument_feasibility".to_string(),
            instrument_feasibility,
        );
        response.insert("order_events".to_string(), json!(events.order_events));
        response.insert("private_events".to_string(), json!(events.private_events));
        response.insert("risk_events".to_string(), json!(events.risk_events));
        response.insert("bundles".to_string(), json!(events.bundles));
        response.insert("account_console".to_string(), json!(account_console));
        response.insert("account_readiness".to_string(), account_readiness);
        response.insert("strategy_readiness".to_string(), strategy_readiness);
        response.insert("position_bundles".to_string(), json!(position_bundles));
        response.insert("open_orders".to_string(), json!(open_orders));
        response.insert("arbitrage_results".to_string(), json!(arbitrage_results));
        response.insert("profit_summary".to_string(), profit_summary);
        response.insert("events".to_string(), json!(events.events));
        Ok(Value::Object(response))
    })
    .await
}

async fn exchange_api_keys(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match exchange_api_key_status(&state).await {
        Ok(status) => Json(status).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn update_exchange_api_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<ExchangeApiKeyUpdateRequest>,
) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match apply_exchange_api_key_update(&state, request).await {
        Ok(()) => match exchange_api_key_status(&state).await {
            Ok(status) => Json(json!({
                "saved": true,
                "restart_required": true,
                "status": status,
            }))
            .into_response(),
            Err(error) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": error.to_string() })),
            )
                .into_response(),
        },
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn delete_exchange_api_keys(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(exchange): Path<String>,
) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match clear_exchange_api_keys(&state.exchange_api_key_store, &exchange).await {
        Ok(()) => match exchange_api_key_status(&state).await {
            Ok(status) => Json(json!({
                "deleted": true,
                "restart_required": true,
                "status": status,
            }))
            .into_response(),
            Err(error) => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": error.to_string() })),
            )
                .into_response(),
        },
        Err(error) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn scanner_exchanges(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(state, headers, &["five_exchange_scanner", "exchange_roles"]).await
}

async fn scanner_symbol_coverage(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(
        state,
        headers,
        &["five_exchange_scanner", "symbol_coverage"],
    )
    .await
}

async fn scanner_exchange_pairs(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(state, headers, &["five_exchange_scanner", "exchange_pairs"]).await
}

async fn scanner_opportunities(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(state, headers, &["five_exchange_scanner", "opportunities"]).await
}

async fn scanner_pair_statistics(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(
        state,
        headers,
        &["five_exchange_scanner", "pair_statistics"],
    )
    .await
}

async fn scanner_symbol_scores(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(state, headers, &["five_exchange_scanner", "symbol_scores"]).await
}

async fn scanner_recommendations(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(
        state,
        headers,
        &["five_exchange_scanner", "recommendations"],
    )
    .await
}

async fn hedge_policy_status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(state, headers, &["hedge_policy"]).await
}

async fn hedge_policy_inventory_risk(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    snapshot_json_path(state, headers, &["hedge_policy", "inventory_risk"]).await
}

async fn hedge_policy_recommendations(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    snapshot_json_path(state, headers, &["hedge_policy", "recommendations"]).await
}

async fn hedge_policy_venue_capabilities(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    snapshot_json_path(state, headers, &["hedge_policy", "venue_capabilities"]).await
}

async fn hedge_policy_market_regime(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(state, headers, &["hedge_policy", "market_regime"]).await
}

async fn logs(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let model = read_snapshot(&state).await?;
        Ok(json!({
            "risk_events": model.risk_events,
            "recent_trades": model.trades,
            "recorder": model.recorder,
        }))
    })
    .await
}

async fn strategy_logs(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match read_strategy_log_tail(&state).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn health(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, async {
        let model = read_snapshot(&state).await?;
        let metadata = tokio::fs::metadata(&state.snapshot_path).await.ok();
        let snapshot_age_ms = metadata
            .and_then(|metadata| metadata.modified().ok())
            .and_then(|modified| modified.elapsed().ok())
            .map(|elapsed| elapsed.as_millis() as u64);
        Ok(json!({
            "control_api": {
                "running": true,
                "snapshot_path": state.snapshot_path,
                "snapshot_age_ms": snapshot_age_ms,
                "commands_path": state.command_path,
            },
            "runtime": status_from_model(&model),
            "runtime_publisher": model.runtime_publisher_health,
            "exchange_count": model.exchanges.len(),
            "stale_books": model.books.iter().filter(|book| book.is_stale).count(),
            "dry_run_plan_count": model.live_dry_run_orders.len(),
        }))
    })
    .await
}

async fn events(State(state): State<AppState>, headers: HeaderMap) -> Response {
    if !is_authorized(&state, &headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let stream = stream::unfold(state, |state| async move {
        sleep(Duration::from_millis(state.event_interval_ms)).await;
        let event = match read_snapshot(&state).await {
            Ok(model) => Event::default()
                .event("snapshot")
                .data(serde_json::to_string(&model).unwrap_or_else(|_| "{}".to_string())),
            Err(error) => Event::default()
                .event("error")
                .data(json!({ "error": error.to_string() }).to_string()),
        };
        Some((Ok::<Event, Infallible>(event), state))
    });
    Sse::new(stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

async fn control_symbols(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        let snapshot_symbols = value
            .pointer("/spot_control/symbols")
            .cloned()
            .unwrap_or(Value::Null);
        if snapshot_symbols
            .as_array()
            .is_some_and(|symbols| !symbols.is_empty())
        {
            return Ok(snapshot_symbols);
        }
        let control = load_spot_control_service(&state).await?;
        Ok(json!(control.read_model().await.symbols))
    })
    .await
}

async fn control_symbol(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        let symbol = normalize_symbol(&symbol);
        let snapshot_symbol = value
            .pointer("/spot_control/symbols")
            .and_then(Value::as_array)
            .and_then(|symbols| {
                symbols.iter().find(|item| {
                    item.get("internal_symbol")
                        .and_then(Value::as_str)
                        .map(normalize_symbol)
                        .is_some_and(|item| item == symbol)
                })
            })
            .cloned();
        if let Some(snapshot_symbol) = snapshot_symbol {
            return Ok(snapshot_symbol);
        }
        let control = load_spot_control_service(&state).await?;
        Ok(control
            .read_model()
            .await
            .symbols
            .into_iter()
            .find(|item| normalize_symbol(&item.internal_symbol) == symbol)
            .map(|item| json!(item))
            .unwrap_or(Value::Null))
    })
    .await
}

async fn control_symbol_inventory(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    latest_snapshot_field_response(state, headers, symbol, "inventory_ownership").await
}

async fn control_symbol_orders(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    latest_snapshot_field_response(state, headers, symbol, "open_orders").await
}

async fn control_symbol_commands(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        let symbol = normalize_symbol(&symbol);
        Ok(value
            .pointer("/spot_control/commands")
            .and_then(Value::as_array)
            .map(|commands| {
                commands
                    .iter()
                    .filter(|command| {
                        command
                            .get("symbol")
                            .and_then(Value::as_str)
                            .map(normalize_symbol)
                            .is_some_and(|item| item == symbol)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .map(Value::Array)
            .unwrap_or_else(|| Value::Array(Vec::new())))
    })
    .await
}

async fn control_symbol_liquidation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        let symbol = normalize_symbol(&symbol);
        let plans = filter_symbol_array(&value, "/spot_control/liquidation_plans", &symbol);
        let passive = filter_symbol_array(&value, "/spot_control/passive_sessions", &symbol);
        Ok(json!({ "plans": plans, "passive_sessions": passive }))
    })
    .await
}

async fn control_symbol_runtime_snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        Ok(latest_snapshot_for_symbol(&value, &symbol).unwrap_or(Value::Null))
    })
    .await
}

async fn control_symbol_readiness(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    latest_snapshot_field_response(state, headers, symbol, "effective_tradability").await
}

async fn control_symbol_inventory_ownership(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    latest_snapshot_field_response(state, headers, symbol, "inventory_ownership").await
}

async fn control_symbol_direction_readiness(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    latest_snapshot_field_response(state, headers, symbol, "direction_readiness").await
}

async fn control_symbol_liquidation_preview(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    latest_snapshot_field_response(state, headers, symbol, "liquidation_preview").await
}

async fn control_symbol_data_health(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        let snapshot = latest_snapshot_for_symbol(&value, &symbol).unwrap_or(Value::Null);
        Ok(json!({
            "book_health": snapshot.get("book_health").cloned().unwrap_or(Value::Null),
            "exchange_health": snapshot.get("exchange_health").cloned().unwrap_or(Value::Null),
            "component_statuses": snapshot.get("component_statuses").cloned().unwrap_or(Value::Null),
            "consistency_report": snapshot.get("consistency_report").cloned().unwrap_or(Value::Null),
            "warnings": snapshot.get("warnings").cloned().unwrap_or(Value::Null),
            "critical_errors": snapshot.get("critical_errors").cloned().unwrap_or(Value::Null),
        }))
    })
    .await
}

async fn control_command(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(command_id): Path<String>,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        Ok(value
            .pointer("/spot_control/commands")
            .and_then(Value::as_array)
            .and_then(|commands| {
                commands.iter().find(|command| {
                    command
                        .get("command_id")
                        .and_then(Value::as_str)
                        .is_some_and(|item| item == command_id)
                })
            })
            .cloned()
            .unwrap_or(Value::Null))
    })
    .await
}

async fn control_audit(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(state, headers, &["spot_control", "audit_events"]).await
}

async fn runtime_publisher_status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    snapshot_json_path(state, headers, &["runtime_publisher_health"]).await
}

async fn runtime_publisher_exchanges(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    snapshot_json_path(
        state,
        headers,
        &["runtime_publisher_health", "per_exchange_health"],
    )
    .await
}

async fn runtime_publisher_components(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        let latest = value
            .pointer("/spot_control/runtime_snapshots")
            .and_then(Value::as_array)
            .and_then(|snapshots| snapshots.last())
            .cloned()
            .unwrap_or(Value::Null);
        Ok(json!({
            "snapshot_id": latest.get("snapshot_id").cloned().unwrap_or(Value::Null),
            "source_metadata": latest.get("source_metadata").cloned().unwrap_or(Value::Null),
            "component_statuses": latest.get("component_statuses").cloned().unwrap_or(Value::Null),
        }))
    })
    .await
}

async fn runtime_publisher_errors(State(state): State<AppState>, headers: HeaderMap) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        let health = value.get("runtime_publisher_health").cloned().unwrap_or(Value::Null);
        let exchange_errors = health
            .get("per_exchange_health")
            .and_then(Value::as_object)
            .map(|items| {
                items
                    .values()
                    .filter_map(|item| {
                        item.get("last_error").and_then(Value::as_str).map(|error| {
                            json!({
                                "exchange": item.get("exchange").cloned().unwrap_or(Value::Null),
                                "error": error,
                                "backoff_until": item.get("backoff_until").cloned().unwrap_or(Value::Null),
                            })
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(json!({
            "last_critical_error": health.get("last_critical_error").cloned().unwrap_or(Value::Null),
            "snapshot_persist_failures": health.get("snapshot_persist_failures").cloned().unwrap_or(Value::Null),
            "exchange_errors": exchange_errors,
        }))
    })
    .await
}

async fn control_symbol_snapshots(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        let symbol = normalize_symbol(&symbol);
        Ok(value
            .pointer("/spot_control/runtime_snapshots")
            .and_then(Value::as_array)
            .map(|snapshots| {
                snapshots
                    .iter()
                    .filter(|snapshot| symbol_matches(snapshot, &symbol))
                    .cloned()
                    .collect::<Vec<_>>()
            })
            .map(Value::Array)
            .unwrap_or_else(|| Value::Array(Vec::new())))
    })
    .await
}

async fn control_snapshot_by_id(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(snapshot_id): Path<String>,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        Ok(value
            .pointer("/spot_control/runtime_snapshots")
            .and_then(Value::as_array)
            .and_then(|snapshots| {
                snapshots.iter().find(|snapshot| {
                    snapshot
                        .get("snapshot_id")
                        .and_then(Value::as_str)
                        .is_some_and(|item| item == snapshot_id)
                })
            })
            .cloned()
            .unwrap_or(Value::Null))
    })
    .await
}

async fn control_symbol_open_order_ownership(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    latest_snapshot_field_response(state, headers, symbol, "open_orders").await
}

async fn control_symbol_fill_ownership(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    latest_snapshot_field_response(state, headers, symbol, "fill_ownership").await
}

async fn control_symbol_consistency(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    latest_snapshot_field_response(state, headers, symbol, "consistency_report").await
}

async fn control_pause(State(state): State<AppState>, headers: HeaderMap) -> Response {
    append_control_command(&state, &headers, "pause", json!({})).await
}

async fn control_resume(State(state): State<AppState>, headers: HeaderMap) -> Response {
    append_control_command(&state, &headers, "resume", json!({})).await
}

async fn control_kill_switch(State(state): State<AppState>, headers: HeaderMap) -> Response {
    append_control_command(
        &state,
        &headers,
        "kill_switch",
        json!({
            "allow_live_dry_run": false,
            "allow_live_orders": false,
        }),
    )
    .await
}

async fn control_kill_switch_reset(State(state): State<AppState>, headers: HeaderMap) -> Response {
    append_control_command(
        &state,
        &headers,
        "kill_switch_reset",
        json!({
            "allow_live_dry_run": true,
            "allow_live_orders": false,
        }),
    )
    .await
}

async fn control_exchange_pause(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(exchange): Path<String>,
) -> Response {
    apply_exchange_disabled_registry(&state, &headers, exchange, true).await
}

async fn control_exchange_resume(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(exchange): Path<String>,
) -> Response {
    apply_exchange_disabled_registry(&state, &headers, exchange, false).await
}

async fn control_symbol_pause(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    apply_symbol_control(&state, &headers, symbol, SymbolControlAction::Pause).await
}

async fn apply_exchange_disabled_registry(
    state: &AppState,
    headers: &HeaderMap,
    exchange: String,
    disabled: bool,
) -> Response {
    if !is_authorized(state, headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match apply_exchange_disabled_registry_inner(state, exchange, disabled).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn apply_exchange_disabled_registry_inner(
    state: &AppState,
    exchange: String,
    disabled: bool,
) -> Result<Value> {
    let exchange = normalize_exchange_name(&exchange);
    if exchange.is_empty() {
        anyhow::bail!("exchange is required");
    }
    let registry_path = disabled_registry_path_from_strategy_config(state).await?;
    let mut config = read_disabled_registry_config(&registry_path).await?;
    config
        .disabled
        .exchanges
        .retain(|item| normalize_exchange_name(&item.exchange) != exchange);
    if disabled {
        config.disabled.exchanges.push(DisabledExchange {
            exchange: exchange.clone(),
            reason: "control_api".to_string(),
            expires_at: None,
        });
    }
    write_disabled_registry_config(&registry_path, &config).await?;
    let restart = restart_strategy(state).await?;
    Ok(json!({
        "accepted": true,
        "applied_to_disabled_registry": true,
        "exchange": exchange,
        "disabled": disabled,
        "registry_path": registry_path,
        "disabled_registry": config,
        "restart": restart,
    }))
}

async fn disabled_registry_path_from_strategy_config(state: &AppState) -> Result<PathBuf> {
    let content = tokio::fs::read_to_string(&state.strategy_config)
        .await
        .with_context(|| format!("read strategy config {}", state.strategy_config.display()))?;
    let value: Value = serde_yaml::from_str(&content)
        .with_context(|| format!("parse strategy config {}", state.strategy_config.display()))?;
    let path = value
        .get("disabled_registry_path")
        .and_then(Value::as_str)
        .filter(|path| !path.trim().is_empty())
        .unwrap_or("config/disabled_symbols.yml");
    Ok(PathBuf::from(path))
}

async fn read_disabled_registry_config(path: &PathBuf) -> Result<DisabledRegistryConfig> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) if content.trim().is_empty() => Ok(DisabledRegistryConfig::default()),
        Ok(content) => serde_yaml::from_str::<DisabledRegistryConfig>(&content)
            .with_context(|| format!("parse disabled registry {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(DisabledRegistryConfig::default())
        }
        Err(error) => {
            Err(error).with_context(|| format!("read disabled registry {}", path.display()))
        }
    }
}

async fn write_disabled_registry_config(
    path: &PathBuf,
    config: &DisabledRegistryConfig,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let content = serde_yaml::to_string(config).context("serialize disabled registry")?;
    let tmp_path = path.with_extension("yml.tmp");
    tokio::fs::write(&tmp_path, content.as_bytes())
        .await
        .with_context(|| format!("write {}", tmp_path.display()))?;
    tokio::fs::rename(&tmp_path, path)
        .await
        .with_context(|| format!("replace {}", path.display()))?;
    Ok(())
}

async fn disabled_registry_view_from_strategy_config(state: &AppState) -> Result<DisabledView> {
    let path = disabled_registry_path_from_strategy_config(state).await?;
    let config = read_disabled_registry_config(&path).await?;
    Ok(disabled_registry_view(config))
}

fn disabled_registry_view(config: DisabledRegistryConfig) -> DisabledView {
    DisabledView {
        symbols: config
            .disabled
            .symbols
            .into_iter()
            .map(|item| DisabledSymbolView {
                symbol: normalize_symbol(&item.symbol),
                status: "active".to_string(),
                reason: item.reason,
                expires_at: item.expires_at,
            })
            .collect(),
        exchanges: config
            .disabled
            .exchanges
            .into_iter()
            .map(|item| DisabledExchangeView {
                exchange: normalize_exchange_name(&item.exchange),
                status: "active".to_string(),
                reason: item.reason,
                expires_at: item.expires_at,
            })
            .collect(),
        exchange_symbols: config
            .disabled
            .exchange_symbols
            .into_iter()
            .map(|item| DisabledExchangeSymbolView {
                exchange: normalize_exchange_name(&item.exchange),
                market_type: item.market_type,
                symbol: normalize_symbol(&item.symbol),
                status: "active".to_string(),
                reason: item.reason,
                expires_at: item.expires_at,
            })
            .collect(),
    }
}

fn normalize_exchange_name(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" => "gateio".to_string(),
        value => value.to_string(),
    }
}

async fn control_symbol_resume(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    apply_symbol_control(&state, &headers, symbol, SymbolControlAction::Resume).await
}

async fn control_symbol_disable(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    apply_symbol_control(&state, &headers, symbol, SymbolControlAction::Disable).await
}

async fn control_symbol_cancel_orders(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    append_control_command(
        &state,
        &headers,
        "symbol_cancel_orders",
        json!({
            "symbol": symbol,
            "live_submission_blocked": true,
        }),
    )
    .await
}

async fn control_symbol_confirm_liquidation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    append_control_command(
        &state,
        &headers,
        "symbol_confirm_liquidation",
        json!({
            "symbol": symbol,
            "would_submit_order": false,
            "live_submission_blocked": true,
        }),
    )
    .await
}

async fn control_symbol_stop_liquidation(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    append_control_command(
        &state,
        &headers,
        "symbol_stop_liquidation",
        json!({ "symbol": symbol }),
    )
    .await
}

#[derive(Debug, Clone, Copy)]
enum SymbolControlAction {
    Pause,
    Resume,
    Disable,
}

async fn apply_symbol_control(
    state: &AppState,
    headers: &HeaderMap,
    symbol: String,
    action: SymbolControlAction,
) -> Response {
    if !is_authorized(state, headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match apply_symbol_control_inner(state, symbol, action).await {
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn apply_symbol_control_inner(
    state: &AppState,
    symbol: String,
    action: SymbolControlAction,
) -> Result<Value> {
    let control = load_spot_control_service(state).await?;
    let model = read_snapshot(state).await.unwrap_or_default();
    let read_model = control.read_model().await;
    let normalized_symbol = normalize_symbol(&symbol);
    let mut current_symbol = read_model
        .symbols
        .iter()
        .find(|item| normalize_symbol(&item.internal_symbol) == normalized_symbol)
        .cloned();
    let mut expected_version = current_symbol
        .as_ref()
        .map(|item| item.version)
        .unwrap_or_default();
    let mut default_exchanges =
        default_symbol_exchanges(state, &model, &read_model, &normalized_symbol).await?;
    if matches!(action, SymbolControlAction::Disable) {
        default_exchanges.extend(inventory_exchanges_with_symbol_balance(
            &model,
            &normalized_symbol,
        ));
        default_exchanges = normalize_exchange_list(&default_exchanges);
    }
    let directions = default_symbol_directions(&default_exchanges);
    if matches!(
        action,
        SymbolControlAction::Resume | SymbolControlAction::Disable
    ) && default_exchanges.is_empty()
    {
        anyhow::bail!("no selected exchanges available for {normalized_symbol}");
    }
    if matches!(
        action,
        SymbolControlAction::Pause | SymbolControlAction::Disable
    ) && current_symbol.is_none()
    {
        ensure_symbol_managed_for_control(
            &control,
            &model,
            &normalized_symbol,
            &default_exchanges,
            &directions,
        )
        .await?;
        let refreshed = control.read_model().await;
        current_symbol = refreshed
            .symbols
            .iter()
            .find(|item| normalize_symbol(&item.internal_symbol) == normalized_symbol)
            .cloned();
        expected_version = current_symbol
            .as_ref()
            .map(|item| item.version)
            .unwrap_or_default();
    }
    if matches!(
        action,
        SymbolControlAction::Resume | SymbolControlAction::Disable
    ) {
        let snapshot_mode = if matches!(action, SymbolControlAction::Disable) {
            EnableMode::ObserveOnly
        } else {
            control.config().default_enable_mode
        };
        let snapshot = SpotControlSnapshotBuilder::from_dashboard_model(
            control.config().runtime_snapshot.clone(),
            &model,
        )
        .build(
            &normalized_symbol,
            &default_exchanges,
            &directions,
            snapshot_mode,
        );
        control.record_runtime_snapshot(snapshot).await;
    }
    let idempotency_key = format!(
        "control-api:{}:{}:{}",
        match action {
            SymbolControlAction::Pause => "pause",
            SymbolControlAction::Resume => "enable",
            SymbolControlAction::Disable => "disable",
        },
        normalized_symbol,
        Uuid::new_v4()
    );
    let response = match action {
        SymbolControlAction::Pause => {
            control
                .pause(
                    normalized_symbol.clone(),
                    VersionedSymbolRequest {
                        requested_by: "control_api".to_string(),
                        expected_version,
                    },
                    idempotency_key,
                )
                .await
        }
        SymbolControlAction::Resume => {
            let config = control.config().clone();
            if current_symbol.as_ref().is_some_and(|symbol| {
                matches!(
                    symbol.lifecycle_state,
                    SpotSymbolLifecycleState::Active
                        | SpotSymbolLifecycleState::Paused
                        | SpotSymbolLifecycleState::Ready
                )
            }) {
                control
                    .resume(
                        normalized_symbol.clone(),
                        VersionedSymbolRequest {
                            requested_by: "control_api".to_string(),
                            expected_version,
                        },
                        idempotency_key,
                    )
                    .await
            } else {
                control
                    .enable(
                        EnableSymbolRequest {
                            symbol: normalized_symbol.clone(),
                            mode: config.default_enable_mode,
                            selected_exchanges: default_exchanges.clone(),
                            allowed_directions: directions.clone(),
                            max_notional_per_trade: None,
                            max_total_symbol_exposure: None,
                            quote_asset: "USDT".to_string(),
                            requested_by: "control_api".to_string(),
                            expected_version,
                        },
                        idempotency_key,
                    )
                    .await
            }
        }
        SymbolControlAction::Disable => {
            let config = control.config().clone();
            control
                .disable(
                    DisableSymbolRequest {
                        symbol: normalized_symbol.clone(),
                        selected_exchanges: default_exchanges.clone(),
                        mode: DisableMode::MarketLiquidate,
                        cancel_active_orders: config.disable_defaults.cancel_active_orders,
                        include_managed_inventory_only: config
                            .disable_defaults
                            .include_managed_inventory_only,
                        maximum_liquidation_loss_usdt: Some(
                            config.disable_defaults.maximum_liquidation_loss_usdt,
                        ),
                        maximum_slippage_bps: Some(config.disable_defaults.maximum_slippage_bps),
                        requested_by: "control_api".to_string(),
                        expected_version,
                    },
                    idempotency_key,
                )
                .await
        }
    };
    let latest = control.read_model().await;
    let accepted = response
        .validation_errors
        .iter()
        .all(|error| !error.critical);
    let symbol_liquidation_plans = latest
        .liquidation_plans
        .iter()
        .filter(|plan| normalize_symbol(&plan.symbol) == normalized_symbol)
        .cloned()
        .collect::<Vec<_>>();
    let symbol_passive_sessions = latest
        .passive_sessions
        .iter()
        .filter(|session| normalize_symbol(&session.symbol) == normalized_symbol)
        .cloned()
        .collect::<Vec<_>>();
    Ok(json!({
        "accepted": accepted,
        "queued": false,
        "applied_to_control_service": true,
        "response": response,
        "symbols": latest.symbols,
        "commands": latest.commands,
        "liquidation_plans": symbol_liquidation_plans,
        "passive_sessions": symbol_passive_sessions,
        "runtime_reload": {
            "requested": accepted,
            "mechanism": "spot control lifecycle store hot reload",
        },
    }))
}

async fn ensure_symbol_managed_for_control(
    control: &SpotControlService,
    model: &DashboardReadModel,
    symbol: &str,
    exchanges: &[String],
    directions: &[EnabledDirection],
) -> Result<()> {
    if exchanges.is_empty() {
        anyhow::bail!("no selected exchanges available for {symbol}");
    }
    let snapshot = SpotControlSnapshotBuilder::from_dashboard_model(
        control.config().runtime_snapshot.clone(),
        model,
    )
    .build(
        symbol,
        exchanges,
        directions,
        control.config().default_enable_mode,
    );
    control.record_runtime_snapshot(snapshot).await;
    let config = control.config().clone();
    let response = control
        .enable(
            EnableSymbolRequest {
                symbol: symbol.to_string(),
                mode: config.default_enable_mode,
                selected_exchanges: exchanges.to_vec(),
                allowed_directions: directions.to_vec(),
                max_notional_per_trade: None,
                max_total_symbol_exposure: None,
                quote_asset: "USDT".to_string(),
                requested_by: "control_api:auto_manage_for_control".to_string(),
                expected_version: 0,
            },
            format!("control-api:auto-manage:{symbol}:{}", Uuid::new_v4()),
        )
        .await;
    if response
        .validation_errors
        .iter()
        .any(|error| error.critical)
    {
        anyhow::bail!(
            "symbol {symbol} is not managed and could not be registered for control: {:?}",
            response.validation_errors
        );
    }
    Ok(())
}

async fn default_symbol_exchanges(
    state: &AppState,
    model: &DashboardReadModel,
    read_model: &rustcta::control::spot_control::SpotControlReadModel,
    symbol: &str,
) -> Result<Vec<String>> {
    if let Some(existing) = read_model
        .symbols
        .iter()
        .find(|item| normalize_symbol(&item.internal_symbol) == symbol)
    {
        let selected = normalize_exchange_list(&existing.selected_exchanges);
        if !selected.is_empty() {
            return Ok(selected);
        }
    }

    let mut exchanges = model
        .books
        .iter()
        .filter(|book| normalize_symbol(&book.symbol) == symbol && !book.is_stale)
        .map(|book| book.exchange.clone())
        .collect::<Vec<_>>();
    if exchanges.is_empty() {
        exchanges = model
            .opportunities
            .iter()
            .filter(|item| normalize_symbol(&item.symbol) == symbol)
            .flat_map(|item| [item.buy_exchange.clone(), item.sell_exchange.clone()])
            .collect();
    }
    let exchanges = normalize_exchange_list(&exchanges);
    if !exchanges.is_empty() {
        return Ok(exchanges);
    }

    let content = tokio::fs::read_to_string(&state.strategy_config)
        .await
        .with_context(|| format!("read strategy config {}", state.strategy_config.display()))?;
    let value: Value = serde_yaml::from_str(&content)
        .with_context(|| format!("parse strategy config {}", state.strategy_config.display()))?;
    let configured = value
        .get("exchanges")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(normalize_exchange_list(&configured))
}

fn inventory_exchanges_with_symbol_balance(
    model: &DashboardReadModel,
    normalized_symbol: &str,
) -> Vec<String> {
    let Some(base_asset) = symbol_base_asset(normalized_symbol) else {
        return Vec::new();
    };
    normalize_exchange_list(
        &model
            .inventory
            .iter()
            .filter(|item| {
                item.asset.eq_ignore_ascii_case(&base_asset)
                    && (item.total.abs() > f64::EPSILON
                        || item.available.abs() > f64::EPSILON
                        || item.effective_available.abs() > f64::EPSILON
                        || item.locked_by_exchange.abs() > f64::EPSILON)
            })
            .map(|item| item.exchange.clone())
            .collect::<Vec<_>>(),
    )
}

fn symbol_base_asset(normalized_symbol: &str) -> Option<String> {
    for quote in ["USDT", "USDC", "USD"] {
        if let Some(base) = normalized_symbol.strip_suffix(quote) {
            if !base.is_empty() {
                return Some(base.to_string());
            }
        }
    }
    None
}

fn default_symbol_directions(exchanges: &[String]) -> Vec<EnabledDirection> {
    let mut directions = Vec::new();
    for buy_exchange in exchanges {
        for sell_exchange in exchanges {
            if buy_exchange != sell_exchange {
                directions.push(EnabledDirection {
                    buy_exchange: buy_exchange.clone(),
                    sell_exchange: sell_exchange.clone(),
                });
            }
        }
    }
    directions
}

async fn load_spot_control_service(state: &AppState) -> Result<SpotControlService> {
    let content = tokio::fs::read_to_string(&state.strategy_config)
        .await
        .with_context(|| format!("read strategy config {}", state.strategy_config.display()))?;
    let value: Value = serde_yaml::from_str(&content)
        .with_context(|| format!("parse strategy config {}", state.strategy_config.display()))?;
    let config_value = value
        .get("spot_symbol_control")
        .cloned()
        .unwrap_or(Value::Null);
    let config: SpotSymbolControlConfig =
        serde_json::from_value(config_value).context("parse spot_symbol_control config")?;
    if !config.enabled {
        anyhow::bail!("spot_symbol_control is disabled in strategy config");
    }
    SpotControlService::load_or_new(config).context("load spot symbol control service")
}

async fn append_control_command(
    state: &AppState,
    headers: &HeaderMap,
    command_type: &str,
    details: Value,
) -> Response {
    if !is_authorized(state, headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let command = ControlApiCommand {
        timestamp: Utc::now(),
        command_id: Uuid::new_v4().to_string(),
        command_type: command_type.to_string(),
        requested_by: "control_api".to_string(),
        applied_to_runtime: false,
        would_submit_order: false,
        details,
        warning: "command queued only; runtime must consume this approved command channel before it changes strategy state".to_string(),
    };
    match append_jsonl(&state.command_path, &command).await {
        Ok(()) => {
            log::info!(
                "control command accepted command_id={} type={} details={}",
                command.command_id,
                command.command_type,
                command.details
            );
            Json(json!({
                "accepted": true,
                "queued": true,
                "command": command,
            }))
            .into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn apply_strategy_config_update(
    state: &AppState,
    request: StrategyConfigUpdateRequest,
) -> Result<Value> {
    if request.content.trim().is_empty() {
        anyhow::bail!("strategy config content cannot be empty");
    }
    serde_yaml::from_str::<serde_yaml::Value>(&request.content)
        .context("strategy config must be valid YAML")?;
    if let Some(parent) = state.strategy_config.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp_path = state.strategy_config.with_extension("yml.tmp");
    tokio::fs::write(&tmp_path, request.content.as_bytes())
        .await
        .with_context(|| format!("write {}", tmp_path.display()))?;
    tokio::fs::rename(&tmp_path, &state.strategy_config)
        .await
        .with_context(|| format!("replace {}", state.strategy_config.display()))?;

    let restart = if request.restart {
        restart_strategy(state).await?
    } else {
        json!({ "requested": false })
    };
    Ok(json!({
        "saved": true,
        "path": state.strategy_config.display().to_string(),
        "restart": restart,
    }))
}

async fn read_cross_arb_config(state: &AppState) -> Result<CrossExchangeArbitrageConfig> {
    let content = tokio::fs::read_to_string(&state.strategy_config)
        .await
        .with_context(|| format!("read strategy config {}", state.strategy_config.display()))?;
    serde_yaml::from_str(&content)
        .with_context(|| format!("parse cross-arb config {}", state.strategy_config.display()))
}

fn cross_arb_settings_view(config: CrossExchangeArbitrageConfig) -> CrossArbSettingsView {
    let enabled_exchanges = config
        .universe
        .enabled_exchanges
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let exchanges = config
        .exchanges
        .iter()
        .map(|(exchange, runtime)| CrossArbExchangeSettingsView {
            exchange: exchange.to_string(),
            enabled: !runtime.is_disabled(),
            operating_mode: format!("{:?}", runtime.operating_mode).to_ascii_lowercase(),
            account_id: runtime.account_id.clone().unwrap_or_default(),
            env_prefix: runtime.env_prefix.clone().unwrap_or_default(),
            private_rest_enabled: runtime.private_rest_enabled,
            private_ws_enabled: runtime.private_ws_enabled,
        })
        .collect::<Vec<_>>();
    CrossArbSettingsView {
        path: String::new(),
        config_kind: "cross_exchange_arbitrage".to_string(),
        enabled_exchanges,
        exchanges,
        symbols: config
            .universe
            .symbols
            .iter()
            .map(ToString::to_string)
            .collect(),
        target_symbol_count: config
            .universe
            .target_symbol_count
            .unwrap_or(config.universe.symbols.len()),
        max_symbol_count: config
            .universe
            .max_symbol_count
            .unwrap_or(config.universe.symbols.len()),
        min_common_exchanges: config.market.min_common_exchanges,
        min_notional_usdt: config.sizing.min_notional_usdt,
        target_notional_usdt: config.sizing.target_notional_usdt,
        max_notional_usdt: config.sizing.max_notional_usdt,
        max_positions_per_exchange: config.sizing.max_positions_per_exchange,
        max_open_bundles: config.risk.max_open_bundles,
        max_open_positions: config.risk.max_open_positions,
        max_symbol_notional_usdt: config.sizing.max_symbol_notional_usdt,
        max_notional_per_symbol_usdt: config.risk.max_notional_per_symbol_usdt,
        max_notional_per_exchange_usdt: config.risk.max_notional_per_exchange_usdt,
        max_total_notional_usdt: config.risk.max_total_notional_usdt,
        min_open_raw_spread: config.thresholds.min_open_raw_spread,
        min_open_maker_taker_net_edge: config.thresholds.min_open_maker_taker_net_edge,
        max_open_raw_spread: config.thresholds.max_open_raw_spread,
        lock_profit_dual_taker_pct: config.thresholds.lock_profit_dual_taker_pct,
        max_close_spread_pct: config.thresholds.max_close_spread_pct,
        execution_dry_run: config.execution.dry_run,
        trading_mode: format!("{:?}", config.trading_mode).to_ascii_lowercase(),
        mode: format!("{:?}", config.mode).to_ascii_lowercase(),
    }
}

async fn apply_cross_arb_settings_update(
    state: &AppState,
    request: CrossArbSettingsUpdateRequest,
) -> Result<Value> {
    let mut config = read_cross_arb_config(state).await?;
    if let Some(profile) = request.execution_profile.as_deref() {
        apply_cross_arb_execution_profile(&mut config, profile)?;
    }
    let account_manager_accounts = read_account_manager_accounts(&state.accounts_config)
        .await
        .unwrap_or_else(|error| {
            log::warn!(
                "failed to read account manager config {} while saving cross-arb settings: {error:#}",
                state.accounts_config.display()
            );
            Vec::new()
        });
    let mut enabled_exchanges = Vec::new();
    for requested in request.exchanges {
        let exchange = parse_supported_cross_arb_exchange(&requested.exchange)?;
        let runtime = config.exchanges.entry(exchange.clone()).or_default();
        runtime.enabled = Some(requested.enabled);
        runtime.operating_mode = if requested.enabled {
            ExchangeOperatingMode::Enabled
        } else {
            ExchangeOperatingMode::Disabled
        };
        if let Some(account_id) = requested.account_id {
            runtime.account_id = non_empty_string(account_id);
        }
        let requested_env_prefix = requested.env_prefix.and_then(non_empty_string).or_else(|| {
            runtime.account_id.as_deref().and_then(|account_id| {
                account_manager_env_prefix_for_account(
                    &account_manager_accounts,
                    &exchange,
                    account_id,
                )
            })
        });
        if let Some(env_prefix) = requested_env_prefix {
            runtime.env_prefix = Some(env_prefix);
        }
        if let Some(value) = requested.private_rest_enabled {
            runtime.private_rest_enabled = value;
        }
        if let Some(value) = requested.private_ws_enabled {
            runtime.private_ws_enabled = value;
        }
        if requested.enabled {
            enabled_exchanges.push(exchange);
        }
    }
    if !enabled_exchanges.is_empty() {
        config.universe.enabled_exchanges = enabled_exchanges.clone();
        config.market.min_common_exchanges = enabled_exchanges.len();
        if config.trading_mode == rustcta::strategies::cross_exchange_arbitrage::TradingMode::Live {
            config.enabled_exchanges = enabled_exchanges;
        }
    }
    let symbols = request
        .symbols
        .iter()
        .filter_map(|symbol| CanonicalSymbol::parse(symbol.trim()))
        .collect::<Vec<_>>();
    if !symbols.is_empty() {
        config.universe.symbols = symbols.clone();
        config.universe.target_symbol_count = request.target_symbol_count.or(Some(symbols.len()));
        config.universe.max_symbol_count = request.target_symbol_count.or(Some(symbols.len()));
        if config.trading_mode == rustcta::strategies::cross_exchange_arbitrage::TradingMode::Live {
            config.enabled_symbols = symbols;
        }
    }
    if let Some(value) = positive_f64(request.min_notional_usdt, "min_notional_usdt")? {
        config.sizing.min_notional_usdt = value;
    }
    if let Some(value) = positive_f64(request.target_notional_usdt, "target_notional_usdt")? {
        config.sizing.target_notional_usdt = value;
    }
    if let Some(value) = positive_f64(request.max_notional_usdt, "max_notional_usdt")? {
        config.sizing.max_notional_usdt = value;
    }
    if let Some(value) = positive_usize(
        request.max_positions_per_exchange,
        "max_positions_per_exchange",
    )? {
        config.sizing.max_positions_per_exchange = value;
    }
    if let Some(value) = positive_usize(request.max_open_bundles, "max_open_bundles")? {
        config.risk.max_open_bundles = value;
    }
    if let Some(value) = positive_usize(request.max_open_positions, "max_open_positions")? {
        config.risk.max_open_positions = value;
    }
    if let Some(value) = positive_f64(request.max_symbol_notional_usdt, "max_symbol_notional_usdt")?
    {
        config.sizing.max_symbol_notional_usdt = value;
    }
    if let Some(value) = positive_f64(
        request.max_notional_per_symbol_usdt,
        "max_notional_per_symbol_usdt",
    )? {
        config.risk.max_notional_per_symbol_usdt = value;
    }
    if let Some(value) = positive_f64(
        request.max_notional_per_exchange_usdt,
        "max_notional_per_exchange_usdt",
    )? {
        config.risk.max_notional_per_exchange_usdt = value;
    }
    if let Some(value) = positive_f64(request.max_total_notional_usdt, "max_total_notional_usdt")? {
        config.risk.max_total_notional_usdt = value;
    }
    if let Some(value) = non_negative_f64(request.min_open_raw_spread, "min_open_raw_spread")? {
        config.thresholds.min_open_raw_spread = value;
    }
    if let Some(value) = non_negative_f64(
        request.min_open_maker_taker_net_edge,
        "min_open_maker_taker_net_edge",
    )? {
        config.thresholds.min_open_maker_taker_net_edge = value;
    }
    if let Some(value) = non_negative_f64(request.max_open_raw_spread, "max_open_raw_spread")? {
        config.thresholds.max_open_raw_spread = value;
    }
    if let Some(value) = non_negative_f64(
        request.lock_profit_dual_taker_pct,
        "lock_profit_dual_taker_pct",
    )? {
        config.thresholds.lock_profit_dual_taker_pct = value;
        config.thresholds.strong_lock_profit_dual_taker_pct = config
            .thresholds
            .strong_lock_profit_dual_taker_pct
            .max(value);
    }
    if let Some(value) = non_negative_f64(request.max_close_spread_pct, "max_close_spread_pct")? {
        config.thresholds.max_close_spread_pct = value;
    }
    config.validate().context("validate cross-arb config")?;
    let content = serde_yaml::to_string(&config).context("serialize cross-arb config")?;
    tokio::fs::write(&state.strategy_config, content)
        .await
        .with_context(|| format!("write strategy config {}", state.strategy_config.display()))?;
    let restart = if request.restart {
        restart_strategy(state).await?
    } else {
        json!({ "skipped": true })
    };
    let mut view = cross_arb_settings_view(config);
    view.path = state.strategy_config.display().to_string();
    Ok(json!({
        "saved": true,
        "restart": restart,
        "settings": view,
    }))
}

fn apply_cross_arb_execution_profile(
    config: &mut CrossExchangeArbitrageConfig,
    profile: &str,
) -> Result<()> {
    match profile.trim().to_ascii_lowercase().as_str() {
        "" => {}
        "simulation" => {
            config.mode = rustcta::market::RuntimeMode::Simulation;
            config.strategy.mode = Some(rustcta::market::RuntimeMode::Simulation);
            config.trading_mode = rustcta::strategies::cross_exchange_arbitrage::TradingMode::Paper;
            config.enable_live_trading = false;
            config.execution.dry_run = true;
        }
        "live_small_dry_run" | "live-small-dry-run" | "dry_run_live" => {
            config.mode = rustcta::market::RuntimeMode::LiveSmall;
            config.strategy.mode = Some(rustcta::market::RuntimeMode::LiveSmall);
            config.trading_mode = rustcta::strategies::cross_exchange_arbitrage::TradingMode::Paper;
            config.enable_live_trading = false;
            config.execution.dry_run = true;
        }
        other => anyhow::bail!(
            "unsupported cross-arb execution_profile {other}; allowed: simulation, live_small_dry_run"
        ),
    }
    Ok(())
}

fn non_empty_string(value: String) -> Option<String> {
    let value = value.trim().to_string();
    (!value.is_empty()).then_some(value)
}

fn parse_supported_cross_arb_exchange(value: &str) -> Result<ExchangeId> {
    let exchange = ExchangeId::from(value);
    match exchange {
        ExchangeId::Binance | ExchangeId::Bitget | ExchangeId::Gate => Ok(exchange),
        _ => anyhow::bail!(
            "unsupported cross-arb exchange {}; allowed: binance, bitget, gate",
            value.trim()
        ),
    }
}

async fn funding_arb_config_path(state: &AppState) -> Result<PathBuf> {
    let processes = strategy_processes(state).await;
    let process = processes
        .iter()
        .find(|process| process.strategy_id == "funding-arb-local")
        .or_else(|| {
            processes
                .iter()
                .find(|process| process.strategy_kind == "funding_arbitrage")
        })
        .ok_or_else(|| anyhow::anyhow!("funding arbitrage strategy is not registered"))?;
    Ok(PathBuf::from(&process.config_path))
}

async fn read_funding_arb_config(
    state: &AppState,
) -> Result<(PathBuf, FundingRateArbitrageConfig)> {
    let path = funding_arb_config_path(state).await?;
    let content = tokio::fs::read_to_string(&path)
        .await
        .with_context(|| format!("read funding-arb config {}", path.display()))?;
    let config = serde_yaml::from_str(&content)
        .with_context(|| format!("parse funding-arb config {}", path.display()))?;
    Ok((path, config))
}

async fn read_funding_arb_settings_view(state: &AppState) -> Result<FundingArbSettingsView> {
    let (path, config) = read_funding_arb_config(state).await?;
    Ok(funding_arb_settings_view(
        path.display().to_string(),
        "funding-arb-local".to_string(),
        config,
    ))
}

fn funding_arb_settings_view(
    path: String,
    strategy_id: String,
    config: FundingRateArbitrageConfig,
) -> FundingArbSettingsView {
    let exchanges = config
        .universe
        .enabled_exchanges
        .iter()
        .map(|exchange| {
            let runtime = config.exchanges.get(exchange);
            FundingArbExchangeSettingsView {
                exchange: exchange.to_string(),
                enabled: runtime
                    .map(|runtime| !runtime.is_disabled())
                    .unwrap_or(true),
                account_id: runtime
                    .and_then(|runtime| runtime.account_id.clone())
                    .unwrap_or_default(),
                env_prefix: runtime
                    .and_then(|runtime| runtime.env_prefix.clone())
                    .unwrap_or_default(),
                demo_trading: runtime
                    .map(|runtime| runtime.demo_trading)
                    .unwrap_or_default(),
                private_ws_enabled: runtime
                    .map(|runtime| runtime.private_ws_enabled)
                    .unwrap_or_default(),
                position_mode: runtime
                    .and_then(|runtime| runtime.position_mode.clone())
                    .unwrap_or_default(),
            }
        })
        .collect::<Vec<_>>();
    FundingArbSettingsView {
        path,
        strategy_id,
        config_kind: "funding_arbitrage".to_string(),
        mode: config.mode,
        enabled_exchanges: config
            .universe
            .enabled_exchanges
            .iter()
            .map(ToString::to_string)
            .collect(),
        quote_asset: config.universe.quote_asset,
        contract_type: config.universe.contract_type,
        symbol_allowlist: config.universe.symbol_allowlist,
        symbol_blocklist: config.universe.symbol_blocklist,
        per_exchange_limit: config.selection.per_exchange_limit,
        min_funding_rate: config.selection.min_funding_rate,
        require_next_funding_time: config.selection.require_next_funding_time,
        max_funding_snapshot_age_ms: config.selection.max_funding_snapshot_age_ms,
        min_seconds_to_settlement_at_scan: config.selection.min_seconds_to_settlement_at_scan,
        max_seconds_to_settlement_at_scan: config.selection.max_seconds_to_settlement_at_scan,
        scan_minute: config.execution.scan_minute,
        notional_usdt: config.execution.notional_usdt,
        open_seconds_before_settlement: config.execution.open_seconds_before_settlement,
        close_seconds_after_settlement: config.execution.close_seconds_after_settlement,
        order_readback_delay_secs: config.execution.order_readback_delay_secs,
        close_limit_timeout_secs: config.execution.close_limit_timeout_secs,
        close_limit_max_retries: config.execution.close_limit_max_retries,
        max_slippage_pct: config.execution.max_slippage_pct,
        allow_existing_symbol_position: config.execution.allow_existing_symbol_position,
        position_side: config.execution.position_side,
        exchanges,
    }
}

async fn apply_funding_arb_settings_update(
    state: &AppState,
    request: FundingArbSettingsUpdateRequest,
) -> Result<Value> {
    let (path, mut config) = read_funding_arb_config(state).await?;
    if let Some(mode) = request.mode.and_then(non_empty_string) {
        config.mode = mode;
    }
    if let Some(value) = request.symbol_allowlist {
        config.universe.symbol_allowlist = normalized_symbol_texts(value);
    }
    if let Some(value) = request.symbol_blocklist {
        config.universe.symbol_blocklist = normalized_symbol_texts(value);
    }
    if let Some(value) = positive_usize(request.per_exchange_limit, "per_exchange_limit")? {
        config.selection.per_exchange_limit = value;
    }
    if let Some(value) = negative_f64(request.min_funding_rate, "min_funding_rate")? {
        config.selection.min_funding_rate = value;
    }
    if let Some(value) = request.require_next_funding_time {
        config.selection.require_next_funding_time = value;
    }
    if let Some(value) = positive_i64(
        request.max_funding_snapshot_age_ms,
        "max_funding_snapshot_age_ms",
    )? {
        config.selection.max_funding_snapshot_age_ms = value;
    }
    config.selection.min_seconds_to_settlement_at_scan = request.min_seconds_to_settlement_at_scan;
    config.selection.max_seconds_to_settlement_at_scan = request.max_seconds_to_settlement_at_scan;
    if let Some(value) = request.scan_minute {
        config.execution.scan_minute = value;
    }
    if let Some(value) = positive_f64(request.notional_usdt, "notional_usdt")? {
        config.execution.notional_usdt = value;
    }
    if let Some(value) = non_negative_i64(
        request.open_seconds_before_settlement,
        "open_seconds_before_settlement",
    )? {
        config.execution.open_seconds_before_settlement = value;
    }
    if let Some(value) = non_negative_i64(
        request.close_seconds_after_settlement,
        "close_seconds_after_settlement",
    )? {
        config.execution.close_seconds_after_settlement = value;
    }
    if let Some(value) = positive_u64(
        request.order_readback_delay_secs,
        "order_readback_delay_secs",
    )? {
        config.execution.order_readback_delay_secs = value;
    }
    if let Some(value) = positive_u64(request.close_limit_timeout_secs, "close_limit_timeout_secs")?
    {
        config.execution.close_limit_timeout_secs = value;
    }
    if let Some(value) = positive_u32(request.close_limit_max_retries, "close_limit_max_retries")? {
        config.execution.close_limit_max_retries = value;
    }
    if let Some(value) = non_negative_f64(request.max_slippage_pct, "max_slippage_pct")? {
        config.execution.max_slippage_pct = value;
    }
    if let Some(value) = request.allow_existing_symbol_position {
        config.execution.allow_existing_symbol_position = value;
    }
    if let Some(value) = request.position_side.and_then(non_empty_string) {
        config.execution.position_side = value;
    }

    let account_manager_accounts = read_account_manager_accounts(&state.accounts_config)
        .await
        .unwrap_or_else(|error| {
            log::warn!(
                "failed to read account manager config {} while saving funding-arb settings: {error:#}",
                state.accounts_config.display()
            );
            Vec::new()
        });
    let mut enabled_exchanges = Vec::new();
    for requested in request.exchanges {
        let exchange = parse_supported_funding_arb_exchange(&requested.exchange)?;
        let runtime = config.exchanges.entry(exchange.clone()).or_default();
        runtime.enabled = Some(requested.enabled);
        if let Some(account_id) = requested.account_id {
            runtime.account_id = non_empty_string(account_id);
        }
        let requested_env_prefix = requested.env_prefix.and_then(non_empty_string).or_else(|| {
            runtime.account_id.as_deref().and_then(|account_id| {
                account_manager_env_prefix_for_account(
                    &account_manager_accounts,
                    &exchange,
                    account_id,
                )
            })
        });
        if let Some(env_prefix) = requested_env_prefix {
            runtime.env_prefix = Some(env_prefix);
        }
        if let Some(value) = requested.demo_trading {
            runtime.demo_trading = value;
        }
        if let Some(value) = requested.private_ws_enabled {
            runtime.private_ws_enabled = value;
        }
        if let Some(value) = requested.position_mode.and_then(non_empty_string) {
            runtime.position_mode = Some(value);
        }
        if requested.enabled {
            enabled_exchanges.push(exchange);
        }
    }
    if let Some(requested_enabled_exchanges) = request.enabled_exchanges {
        enabled_exchanges = requested_enabled_exchanges
            .iter()
            .map(|exchange| parse_supported_funding_arb_exchange(exchange))
            .collect::<Result<Vec<_>>>()?;
    }
    if !enabled_exchanges.is_empty() {
        config.universe.enabled_exchanges = enabled_exchanges;
    }

    config.validate().context("validate funding-arb config")?;
    write_yaml_atomic(&path, &config, "funding-arb config").await?;
    let restart = if request.restart {
        restart_managed_strategy(state, "funding-arb-local").await?
    } else {
        json!({ "skipped": true })
    };
    Ok(json!({
        "saved": true,
        "restart": restart,
        "settings": funding_arb_settings_view(
            path.display().to_string(),
            "funding-arb-local".to_string(),
            config,
        ),
    }))
}

fn normalized_symbol_texts(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .filter_map(non_empty_string)
        .map(|value| value.to_ascii_uppercase())
        .collect()
}

fn parse_supported_funding_arb_exchange(exchange: &str) -> Result<ExchangeId> {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "binance" => Ok(ExchangeId::Binance),
        "bitget" => Ok(ExchangeId::Bitget),
        "gate" | "gateio" | "gate.io" => Ok(ExchangeId::Gate),
        other => anyhow::bail!("unsupported funding-arb exchange {other}"),
    }
}

async fn write_yaml_atomic<T: Serialize + ?Sized>(
    path: &PathBuf,
    value: &T,
    label: &str,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let content = serde_yaml::to_string(value).with_context(|| format!("serialize {label}"))?;
    let tmp_path = path.with_extension("yml.tmp");
    tokio::fs::write(&tmp_path, content.as_bytes())
        .await
        .with_context(|| format!("write {}", tmp_path.display()))?;
    tokio::fs::rename(&tmp_path, path)
        .await
        .with_context(|| format!("replace {}", path.display()))?;
    Ok(())
}

async fn restart_managed_strategy(state: &AppState, strategy_id: &str) -> Result<Value> {
    let command_id = format!(
        "web-restart-{}-{}",
        strategy_id,
        Utc::now().timestamp_millis()
    );
    let process = apply_strategy_command_inner(
        state,
        LifecycleCommandRecord {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            command_id: command_id.clone(),
            strategy_id: strategy_id.to_string(),
            run_id: Some(format!("run-{}", Utc::now().timestamp_millis())),
            command: LifecycleCommand::Restart,
            requested_by: Some("web".to_string()),
            idempotency_key: command_id,
            requested_at: Utc::now(),
        },
    )
    .await?;
    Ok(json!({
        "requested": true,
        "strategy_status": process.status,
        "process": strategy_process_view(process),
    }))
}

fn positive_usize(value: Option<usize>, field: &str) -> Result<Option<usize>> {
    match value {
        Some(0) => anyhow::bail!("{field} must be positive"),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

fn positive_f64(value: Option<f64>, field: &str) -> Result<Option<f64>> {
    match value {
        Some(value) if value.is_finite() && value > 0.0 => Ok(Some(value)),
        Some(_) => anyhow::bail!("{field} must be positive"),
        None => Ok(None),
    }
}

fn negative_f64(value: Option<f64>, field: &str) -> Result<Option<f64>> {
    match value {
        Some(value) if value.is_finite() && value < 0.0 => Ok(Some(value)),
        Some(_) => anyhow::bail!("{field} must be negative"),
        None => Ok(None),
    }
}

fn non_negative_f64(value: Option<f64>, field: &str) -> Result<Option<f64>> {
    match value {
        Some(value) if value.is_finite() && value >= 0.0 => Ok(Some(value)),
        Some(_) => anyhow::bail!("{field} must be non-negative"),
        None => Ok(None),
    }
}

fn positive_i64(value: Option<i64>, field: &str) -> Result<Option<i64>> {
    match value {
        Some(value) if value > 0 => Ok(Some(value)),
        Some(_) => anyhow::bail!("{field} must be positive"),
        None => Ok(None),
    }
}

fn non_negative_i64(value: Option<i64>, field: &str) -> Result<Option<i64>> {
    match value {
        Some(value) if value >= 0 => Ok(Some(value)),
        Some(_) => anyhow::bail!("{field} must be non-negative"),
        None => Ok(None),
    }
}

fn positive_u64(value: Option<u64>, field: &str) -> Result<Option<u64>> {
    match value {
        Some(0) => anyhow::bail!("{field} must be positive"),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

fn positive_u32(value: Option<u32>, field: &str) -> Result<Option<u32>> {
    match value {
        Some(0) => anyhow::bail!("{field} must be positive"),
        Some(value) => Ok(Some(value)),
        None => Ok(None),
    }
}

async fn restart_strategy(state: &AppState) -> Result<Value> {
    let output = Command::new(&state.restart_script)
        .arg("restart-strategy")
        .env("STRATEGY_CONFIG", &state.strategy_config)
        .output()
        .await
        .with_context(|| format!("run {} restart-strategy", state.restart_script.display()))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !output.status.success() {
        anyhow::bail!(
            "strategy restart failed with status {}: {}{}{}",
            output.status,
            stdout,
            if stdout.is_empty() || stderr.is_empty() {
                ""
            } else {
                "\n"
            },
            stderr
        );
    }
    let strategy_status = restart_strategy_status_from_stdout(&stdout);
    Ok(json!({
        "requested": true,
        "status": output.status.to_string(),
        "stdout": stdout,
        "stderr": stderr,
        "strategy_status": strategy_status,
        "strategy_started": strategy_status == "started",
        "strategy_stopped": strategy_status == "stopped",
    }))
}

fn restart_strategy_status_from_stdout(stdout: &str) -> &'static str {
    if stdout.lines().any(|line| {
        line.contains("cross_arb_live")
            || line.contains("target/debug/cross_arb_live")
            || line.contains("target/release/cross_arb_live")
    }) {
        "started"
    } else if stdout
        .lines()
        .any(|line| line.trim_start().starts_with("strategy: stopped"))
    {
        "stopped"
    } else {
        "unknown"
    }
}

async fn read_strategy_log_tail(state: &AppState) -> Result<Value> {
    let (source, pointer, log_path) = resolve_strategy_log_source().await?;
    let content = tokio::fs::read_to_string(&log_path)
        .await
        .with_context(|| format!("read {log_path}"))?;
    let lines = content
        .lines()
        .rev()
        .filter(|line| !is_noisy_strategy_log_line(line))
        .take(800)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .map(parse_strategy_log_line)
        .collect::<Vec<_>>();
    let mut lines = lines;
    lines.extend(read_control_command_log_lines(&state.command_path).await);
    lines.sort_by(|left, right| {
        let left_ts = left.get("timestamp").and_then(Value::as_str).unwrap_or("");
        let right_ts = right.get("timestamp").and_then(Value::as_str).unwrap_or("");
        left_ts.cmp(right_ts)
    });
    if lines.len() > 800 {
        lines.drain(0..lines.len() - 800);
    }
    let mut categories = BTreeMap::<String, Vec<Value>>::new();
    for line in &lines {
        let category = line
            .get("category")
            .and_then(Value::as_str)
            .unwrap_or("info")
            .to_string();
        categories.entry(category).or_default().push(line.clone());
    }
    categories.insert("all".to_string(), lines.clone());
    for values in categories.values_mut() {
        if values.len() > 400 {
            values.drain(0..values.len() - 400);
        }
    }
    let counts = categories
        .iter()
        .map(|(category, values)| (category.clone(), json!(values.len())))
        .collect::<serde_json::Map<_, _>>();
    Ok(json!({
        "path": log_path,
        "source": source,
        "source_pointer": pointer,
        "control_command_path": state.command_path,
        "page_size": 50,
        "max_lines": 800,
        "lines": lines,
        "categories": categories,
        "counts": counts,
    }))
}

async fn resolve_strategy_log_source() -> Result<(String, String, String)> {
    for (source, pointer) in [
        ("spot_arb_runtime", "run/spot_arb.log_path"),
        ("strategy_runtime", "run/strategy.log_path"),
    ] {
        let Ok(raw_path) = tokio::fs::read_to_string(pointer).await else {
            continue;
        };
        let log_path = raw_path.trim();
        if log_path.is_empty() {
            continue;
        }
        if tokio::fs::metadata(log_path).await.is_ok() {
            return Ok((
                source.to_string(),
                pointer.to_string(),
                log_path.to_string(),
            ));
        }
    }

    if let Some(path) = newest_strategy_log_candidate().await {
        return Ok((
            "discovered_spot_arb_log".to_string(),
            "logs scan".to_string(),
            path.display().to_string(),
        ));
    }

    anyhow::bail!(
        "no readable strategy log found; checked run/spot_arb.log_path, run/strategy.log_path, logs/spot_spot_arbitrage*.log, and logs/control_panel/strategy_*.log"
    );
}

async fn newest_strategy_log_candidate() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    collect_log_candidates(
        FsPath::new("logs"),
        |name| name.starts_with("spot_spot_arbitrage") && name.ends_with(".log"),
        &mut candidates,
    )
    .await;
    collect_log_candidates(
        FsPath::new("logs/control_panel"),
        |name| name.starts_with("strategy_") && name.ends_with(".log"),
        &mut candidates,
    )
    .await;
    candidates.sort_by_key(|(_, modified)| *modified);
    candidates.pop().map(|(path, _)| path)
}

async fn collect_log_candidates(
    dir: &FsPath,
    matches_name: impl Fn(&str) -> bool,
    candidates: &mut Vec<(PathBuf, std::time::SystemTime)>,
) {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !matches_name(name) {
            continue;
        }
        let Ok(metadata) = entry.metadata().await else {
            continue;
        };
        if !metadata.is_file() {
            continue;
        }
        let modified = metadata
            .modified()
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH);
        candidates.push((path, modified));
    }
}

fn parse_strategy_log_line(line: &str) -> Value {
    let lower = line.to_ascii_lowercase();
    let level = if line.contains(" ERROR ") {
        "ERROR"
    } else if line.contains(" WARN ") {
        "WARN"
    } else if line.contains(" INFO ") {
        "INFO"
    } else if line.contains(" DEBUG ") {
        "DEBUG"
    } else {
        "LOG"
    };
    let category = classify_strategy_log_line(&lower, level);
    let timestamp = strategy_log_timestamp(line);
    json!({
        "level": level,
        "category": category,
        "timestamp": timestamp,
        "message": line,
    })
}

async fn read_control_command_log_lines(command_path: &PathBuf) -> Vec<Value> {
    let Ok(content) = tokio::fs::read_to_string(command_path).await else {
        return Vec::new();
    };
    content
        .lines()
        .rev()
        .take(120)
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .map(|value| {
            let timestamp = value
                .get("timestamp")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let command_id = value
                .get("command_id")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let command_type = value
                .get("command_type")
                .and_then(Value::as_str)
                .unwrap_or("-");
            let details = value.get("details").cloned().unwrap_or(Value::Null);
            json!({
                "level": "INFO",
                "category": "control",
                "timestamp": timestamp,
                "message": format!(
                    "[{timestamp} INFO  control-api] control command accepted command_id={command_id} type={command_type} details={details}"
                ),
            })
        })
        .collect::<Vec<_>>()
}

fn strategy_log_timestamp(line: &str) -> Option<String> {
    line.strip_prefix('[')
        .and_then(|rest| rest.split_once(' '))
        .map(|(timestamp, _)| timestamp.to_string())
}

fn classify_strategy_log_line(lower: &str, level: &str) -> &'static str {
    if level == "ERROR" {
        "error"
    } else if level == "WARN" {
        "warn"
    } else if lower.contains("trade")
        || lower.contains("order")
        || lower.contains("dry-run")
        || lower.contains("dry_run")
        || lower.contains("建仓")
        || lower.contains("套利")
        || lower.contains("退出")
    {
        "trade"
    } else if lower.contains("control")
        || lower.contains("pause")
        || lower.contains("resume")
        || lower.contains("disable")
        || lower.contains("kill")
    {
        "control"
    } else if lower.contains("balance")
        || lower.contains("inventory")
        || lower.contains("余额")
        || lower.contains("库存")
    {
        "balance"
    } else if lower.contains("book")
        || lower.contains("websocket")
        || lower.contains("scanner")
        || lower.contains("盘口")
    {
        "market"
    } else {
        "info"
    }
}

fn is_noisy_strategy_log_line(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    let trimmed = lower.trim();
    trimmed.is_empty()
        || trimmed.contains("heartbeat")
        || trimmed.contains("keep_alive")
        || trimmed.contains("keep-alive")
        || trimmed.contains("snapshot writer")
        || trimmed.contains("client build completed")
}

async fn exchange_api_key_status(state: &AppState) -> Result<ExchangeApiKeyStatusResponse> {
    let values = read_env_store(&state.exchange_api_key_store).await?;
    let enabled_exchanges = enabled_exchange_names_for_key_status(state).await;
    let account_manager_accounts =
        account_manager_accounts_for_status(&state.accounts_config).await;
    let supported_schemas = exchange_api_key_schemas().iter().collect::<Vec<_>>();
    let supported_exchanges = supported_schemas
        .iter()
        .map(|schema| {
            exchange_schema_status(
                &values,
                schema,
                "default",
                false,
                schema_enabled_for_strategy(schema, &enabled_exchanges),
            )
        })
        .collect::<Vec<_>>();
    let mut exchanges = Vec::new();
    for schema in supported_schemas {
        for account in account_manager_accounts
            .iter()
            .filter(|account| account.exchange.eq_ignore_ascii_case(schema.exchange))
        {
            exchanges.push(exchange_schema_status_for_namespace(
                &values,
                schema,
                &account.account_id,
                &account.credential_namespace,
                true,
                account.enabled && schema_enabled_for_strategy(schema, &enabled_exchanges),
            ));
        }
    }
    Ok(ExchangeApiKeyStatusResponse {
        store_path: state.exchange_api_key_store.display().to_string(),
        restart_required: false,
        enabled_exchanges,
        account_manager_accounts,
        supported_exchanges,
        exchanges,
    })
}

async fn account_manager_accounts_for_status(path: &PathBuf) -> Vec<AccountManagerAccountStatus> {
    match read_account_manager_accounts(path).await {
        Ok(accounts) => accounts
            .into_iter()
            .map(|mut account| {
                account.credential_namespace = credential_namespace_for_env_prefix(
                    &account.exchange,
                    &account.account_id,
                    &account.env_prefix,
                );
                account
            })
            .collect(),
        Err(error) => {
            log::warn!(
                "failed to read account manager config {}: {error:#}",
                path.display()
            );
            Vec::new()
        }
    }
}

async fn read_account_manager_accounts(path: &PathBuf) -> Result<Vec<AccountManagerAccountStatus>> {
    let path = path.clone();
    tokio::task::spawn_blocking(move || {
        let config = AccountManagerConfigFile::from_path(&path)?;
        let mut accounts = config
            .accounts
            .into_iter()
            .map(|(account_id, account)| {
                let name = account
                    .name
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or(account_id.as_str())
                    .to_string();
                let exchange = account.exchange.trim().to_ascii_lowercase();
                let env_prefix = account.env_prefix.trim().to_ascii_uppercase();
                AccountManagerAccountStatus {
                    credential_namespace: credential_namespace_for_env_prefix(
                        &exchange,
                        &account_id,
                        &env_prefix,
                    ),
                    account_id,
                    name,
                    exchange,
                    account_type: account.account_type.unwrap_or_default(),
                    description: account.description.unwrap_or_default(),
                    env_prefix,
                    enabled: account.enabled,
                }
            })
            .collect::<Vec<_>>();
        accounts.sort_by(|left, right| {
            left.exchange
                .cmp(&right.exchange)
                .then_with(|| left.account_id.cmp(&right.account_id))
        });
        Ok(accounts)
    })
    .await
    .context("join account manager config reader")?
}

fn account_manager_env_prefix_for_account(
    accounts: &[AccountManagerAccountStatus],
    exchange: &ExchangeId,
    account_id: &str,
) -> Option<String> {
    let exchange_name = exchange.as_str();
    let requested = account_id.trim();
    if requested.is_empty() {
        return None;
    }
    accounts
        .iter()
        .find(|account| {
            account.exchange.eq_ignore_ascii_case(exchange_name)
                && (account.account_id.eq_ignore_ascii_case(requested)
                    || account.name.eq_ignore_ascii_case(requested))
        })
        .map(|account| account.env_prefix.clone())
        .filter(|prefix| !prefix.trim().is_empty())
}

async fn enabled_exchange_names_for_key_status(state: &AppState) -> Vec<String> {
    if let Ok(config) = strategy_config_summary(state).await {
        return config
            .enabled_exchanges
            .into_iter()
            .map(|exchange| exchange.trim().to_ascii_lowercase())
            .filter(|exchange| !exchange.is_empty())
            .collect();
    }
    read_snapshot(state)
        .await
        .ok()
        .map(|model| {
            model
                .config_summary
                .enabled_exchanges
                .into_iter()
                .map(|exchange| exchange.trim().to_ascii_lowercase())
                .filter(|exchange| !exchange.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn schema_enabled_for_strategy(
    schema: &ExchangeApiKeySchema,
    enabled_exchanges: &[String],
) -> bool {
    enabled_exchanges.is_empty()
        || enabled_exchanges.iter().any(|exchange| {
            exchange_api_key_schema(exchange)
                .map(|enabled_schema| enabled_schema.exchange == schema.exchange)
                .unwrap_or_else(|| exchange == schema.exchange)
        })
}

fn exchange_schema_status(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    account_id: &str,
    include_values: bool,
    enabled: bool,
) -> ExchangeApiKeyExchangeStatus {
    let normalized_account_id = normalize_account_id(account_id);
    let credential_namespace =
        credential_namespace_for_account(schema, &normalized_account_id, None);
    exchange_schema_status_for_namespace(
        values,
        schema,
        &normalized_account_id,
        &credential_namespace,
        include_values,
        enabled,
    )
}

fn exchange_schema_status_for_namespace(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    account_id: &str,
    credential_namespace: &str,
    include_values: bool,
    enabled: bool,
) -> ExchangeApiKeyExchangeStatus {
    let normalized_account_id = normalize_account_id(account_id);
    ExchangeApiKeyExchangeStatus {
        exchange: schema.exchange.to_string(),
        label: schema.label.to_string(),
        enabled,
        account_id: normalized_account_id.clone(),
        account_label: account_label(&normalized_account_id),
        credential_namespace: credential_namespace.to_string(),
        is_default_account: normalized_account_id == "default",
        fields: schema
            .fields
            .iter()
            .map(|field| {
                api_key_field_status_for_namespace(
                    values,
                    schema,
                    field,
                    &normalized_account_id,
                    credential_namespace,
                    include_values,
                )
            })
            .collect(),
    }
}

fn api_key_field_status_for_namespace(
    values: &BTreeMap<String, String>,
    exchange_schema: &ExchangeApiKeySchema,
    field_schema: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
    include_values: bool,
) -> ExchangeApiKeyFieldStatus {
    let configured = configured_field_value_for_namespace(
        values,
        exchange_schema,
        field_schema,
        account_id,
        credential_namespace,
    );
    ExchangeApiKeyFieldStatus {
        field: field_schema.field.to_string(),
        label: field_schema.label.to_string(),
        required: field_schema.required,
        configured: configured.is_some(),
        source: configured.as_ref().map(|(source, _)| source.clone()),
        masked: include_values
            .then_some(configured.as_ref())
            .flatten()
            .map(|(_, value)| mask_secret(value.as_str())),
        value: include_values
            .then_some(configured.as_ref())
            .flatten()
            .filter(|_| field_schema.field == "account_id")
            .map(|(_, value)| value.clone()),
    }
}

async fn apply_exchange_api_key_update(
    state: &AppState,
    request: ExchangeApiKeyUpdateRequest,
) -> Result<()> {
    let exchange = request.exchange.trim().to_ascii_lowercase();
    let raw_account_id = request.account_id.as_deref().unwrap_or("default").trim();
    validate_account_identifier(raw_account_id)?;
    let account_id = normalize_account_id(raw_account_id);
    let credential_namespace_input = request.credential_namespace.as_deref();
    reject_multiline_secret("account_label", request.account_label.as_deref())?;
    reject_multiline_secret("credential_namespace", credential_namespace_input)?;
    reject_multiline_secret("account_id", request.exchange_account_id.as_deref())?;
    reject_multiline_secret("api_key", request.api_key.as_deref())?;
    reject_multiline_secret("api_secret", request.api_secret.as_deref())?;
    reject_multiline_secret("passphrase", request.passphrase.as_deref())?;
    let mut values = read_env_store(&state.exchange_api_key_store).await?;
    let schema = exchange_api_key_schema(&exchange)
        .ok_or_else(|| anyhow::anyhow!("unsupported exchange {}", request.exchange))?;
    let account_manager_accounts = read_account_manager_accounts(&state.accounts_config)
        .await
        .unwrap_or_else(|error| {
            log::warn!(
                "failed to read account manager config {} while saving exchange credentials: {error:#}",
                state.accounts_config.display()
            );
            Vec::new()
        });
    let credential_namespace = credential_namespace_for_update(
        schema,
        &account_manager_accounts,
        &account_id,
        credential_namespace_input,
    )?;
    validate_credential_namespace(&credential_namespace)?;
    if request.clear {
        clear_exchange_values_for_namespace(
            &mut values,
            schema,
            &account_id,
            &credential_namespace,
        );
    } else {
        for field in schema.fields {
            if let Some(value) = request_secret_value(&request, field.field) {
                set_account_field_value_for_namespace(
                    &mut values,
                    schema,
                    field,
                    &account_id,
                    &credential_namespace,
                    value,
                );
            }
        }
        for field in schema.fields.iter().filter(|field| field.required) {
            if !field_value_present_for_namespace(
                &values,
                schema,
                field,
                &account_id,
                &credential_namespace,
            ) {
                return Err(anyhow::anyhow!(
                    "{} {} requires {}",
                    schema.label,
                    account_label(&account_id),
                    field.label
                ));
            }
        }
    }
    write_env_store(&state.exchange_api_key_store, &values).await
}

async fn clear_exchange_api_keys(path: &PathBuf, exchange: &str) -> Result<()> {
    let (normalized, account_id) = parse_exchange_account_path(exchange);
    let schema = exchange_api_key_schema(&normalized)
        .ok_or_else(|| anyhow::anyhow!("unsupported exchange {exchange}"))?;
    let mut values = read_env_store(path).await?;
    clear_exchange_values(&mut values, schema, &account_id);
    write_env_store(path, &values).await
}

fn clear_exchange_values(
    values: &mut BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    account_id: &str,
) {
    let credential_namespace = credential_namespace_for_account(schema, account_id, None);
    clear_exchange_values_for_namespace(values, schema, account_id, &credential_namespace);
}

fn clear_exchange_values_for_namespace(
    values: &mut BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    account_id: &str,
    credential_namespace: &str,
) {
    if account_id == "default" {
        if credential_namespace.eq_ignore_ascii_case(&env_exchange_prefix(schema.exchange)) {
            clear_all_exchange_values(values, schema);
        } else {
            for field in schema.fields {
                for key in namespace_env_keys(credential_namespace, field.field) {
                    values.remove(&key);
                }
            }
        }
        return;
    }
    for field in schema.fields {
        for key in namespace_env_keys(credential_namespace, field.field) {
            values.remove(&key);
        }
    }
    values.remove(&account_label_env_key(schema, account_id));
}

fn clear_all_exchange_values(values: &mut BTreeMap<String, String>, schema: &ExchangeApiKeySchema) {
    for field in schema.fields {
        for alias in field.aliases {
            values.remove(*alias);
        }
    }
    values.remove(&account_label_env_key(schema, "default"));
    let prefix = account_env_prefix(schema.exchange);
    values.retain(|key, _| !key.starts_with(&prefix));
}

fn request_secret_value<'a>(
    request: &'a ExchangeApiKeyUpdateRequest,
    field: &str,
) -> Option<&'a str> {
    match field {
        "account_id" => request.exchange_account_id.as_deref(),
        "api_key" => request.api_key.as_deref(),
        "api_secret" => request.api_secret.as_deref(),
        "passphrase" => request.passphrase.as_deref(),
        _ => None,
    }
    .map(str::trim)
    .filter(|value| !value.is_empty())
}

fn configured_field_value(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
) -> Option<(String, String)> {
    if account_id == "default" {
        field.aliases.iter().find_map(|alias| {
            values
                .get(*alias)
                .filter(|value| !value.is_empty())
                .map(|value| ("key_store".to_string(), value.clone()))
                .or_else(|| {
                    std::env::var(alias)
                        .ok()
                        .filter(|value| !value.is_empty())
                        .map(|value| ("process_env".to_string(), value))
                })
        })
    } else {
        let key = account_env_key(schema, account_id, field.field);
        values
            .get(&key)
            .filter(|value| !value.is_empty())
            .map(|value| ("key_store".to_string(), value.clone()))
            .or_else(|| {
                std::env::var(&key)
                    .ok()
                    .filter(|value| !value.is_empty())
                    .map(|value| ("process_env".to_string(), value))
            })
    }
}

fn configured_field_value_for_namespace(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
) -> Option<(String, String)> {
    if field.field == "account_id" {
        let account_id = normalize_account_id(account_id);
        if account_id != "default" {
            return Some(("strategy_config".to_string(), account_id));
        }
    }
    let default_prefix = env_exchange_prefix(schema.exchange);
    if credential_namespace.eq_ignore_ascii_case(&default_prefix) {
        return configured_field_value(values, schema, field, account_id);
    }
    namespace_env_keys(credential_namespace, field.field)
        .into_iter()
        .find_map(|key| {
            values
                .get(&key)
                .filter(|value| !value.is_empty())
                .map(|value| ("key_store".to_string(), value.clone()))
                .or_else(|| {
                    std::env::var(&key)
                        .ok()
                        .filter(|value| !value.is_empty())
                        .map(|value| ("process_env".to_string(), value))
                })
        })
}

fn field_value_present_for_namespace(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
) -> bool {
    configured_field_value_for_namespace(values, schema, field, account_id, credential_namespace)
        .is_some()
}

fn set_account_field_value_for_namespace(
    values: &mut BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
    value: &str,
) {
    let default_prefix = env_exchange_prefix(schema.exchange);
    if !credential_namespace.eq_ignore_ascii_case(&default_prefix) {
        if let Some(key) = namespace_env_keys(credential_namespace, field.field)
            .into_iter()
            .next()
        {
            set_if_present(values, &key, Some(value));
        }
        return;
    }
    if account_id == "default" {
        for alias in field.aliases {
            set_if_present(values, alias, Some(value));
        }
    } else {
        let key = account_env_key(schema, account_id, field.field);
        set_if_present(values, &key, Some(value));
    }
}

fn credential_namespace_for_account(
    schema: &ExchangeApiKeySchema,
    account_id: &str,
    credential_namespace: Option<&str>,
) -> String {
    let configured = credential_namespace
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_uppercase());
    if let Some(namespace) = configured {
        return namespace;
    }
    let account_id = normalize_account_id(account_id);
    if account_id == "default" {
        env_exchange_prefix(schema.exchange)
    } else {
        format!(
            "{}__{}_",
            env_exchange_prefix(schema.exchange),
            account_id.to_ascii_uppercase().replace('-', "_")
        )
    }
}

fn credential_namespace_for_update(
    schema: &ExchangeApiKeySchema,
    account_manager_accounts: &[AccountManagerAccountStatus],
    account_id: &str,
    credential_namespace: Option<&str>,
) -> Result<String> {
    let account = account_manager_accounts
        .iter()
        .find(|account| {
            account.exchange.eq_ignore_ascii_case(schema.exchange)
                && account.account_id.eq_ignore_ascii_case(account_id)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "{} account {} is not configured in account manager; update config/accounts.yml first",
                schema.label,
                account_id
            )
        })?;
    let namespace = account.credential_namespace.trim().to_ascii_uppercase();
    if namespace.is_empty() {
        anyhow::bail!(
            "{} account {} has empty env_prefix in account manager",
            schema.label,
            account_id
        );
    }
    if let Some(requested) = credential_namespace
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let requested = requested.to_ascii_uppercase();
        if requested != namespace {
            anyhow::bail!(
                "{} account {} env_prefix mismatch: account manager uses {}, request used {}",
                schema.label,
                account_id,
                namespace,
                requested
            );
        }
    }
    Ok(namespace)
}

fn validate_account_identifier(account_id: &str) -> Result<()> {
    let account_id = account_id.trim();
    if account_id.is_empty() {
        return Ok(());
    }
    if account_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        Ok(())
    } else {
        anyhow::bail!("account_id must contain only ASCII letters, digits, and _")
    }
}

fn validate_credential_namespace(credential_namespace: &str) -> Result<()> {
    if credential_namespace.is_empty()
        || credential_namespace
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'_'))
    {
        anyhow::bail!("credential_namespace must contain only ASCII letters, digits, and _");
    }
    Ok(())
}

fn normalize_account_id(value: &str) -> String {
    let normalized = value
        .trim()
        .to_ascii_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' || ch == '-' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    let normalized = normalized.trim_matches('_').trim_matches('-').to_string();
    if normalized.is_empty() || normalized == "main" {
        "default".to_string()
    } else {
        normalized
    }
}

fn account_label(account_id: &str) -> String {
    if account_id == "default" {
        "Default".to_string()
    } else {
        account_id.to_string()
    }
}

fn parse_exchange_account_path(exchange: &str) -> (String, String) {
    if let Some((exchange, account_id)) = exchange.split_once(':') {
        (
            exchange.trim().to_ascii_lowercase(),
            normalize_account_id(account_id),
        )
    } else {
        (exchange.trim().to_ascii_lowercase(), "default".to_string())
    }
}

fn account_env_key(schema: &ExchangeApiKeySchema, account_id: &str, field: &str) -> String {
    format!(
        "{}{}__{}",
        account_env_prefix(schema.exchange),
        account_id.to_ascii_uppercase().replace('-', "_"),
        field.to_ascii_uppercase()
    )
}

fn account_label_env_key(schema: &ExchangeApiKeySchema, account_id: &str) -> String {
    if account_id == "default" {
        format!("{}_ACCOUNT_LABEL", env_exchange_prefix(schema.exchange))
    } else {
        account_env_key(schema, account_id, "account_label")
    }
}

fn account_env_prefix(exchange: &str) -> String {
    let prefix = env_exchange_prefix(exchange);
    if prefix.ends_with('_') {
        format!("{prefix}_")
    } else {
        format!("{prefix}__")
    }
}

fn env_exchange_prefix(exchange: &str) -> String {
    exchange
        .trim_end_matches("_spot")
        .to_ascii_uppercase()
        .replace('-', "_")
}

fn exchange_api_key_schema(exchange: &str) -> Option<&'static ExchangeApiKeySchema> {
    let normalized_exchange = exchange.trim().to_ascii_lowercase();
    let normalized = match normalized_exchange.as_str() {
        "gate" | "gate.io" => "gate",
        "gateio" => "gate",
        "binance_spot" => "binance",
        "okx_spot" => "okx",
        value => value,
    };
    exchange_api_key_schemas()
        .iter()
        .find(|schema| schema.exchange == normalized)
}

fn exchange_api_key_schemas() -> &'static [ExchangeApiKeySchema] {
    const MEXC_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "API Key",
            aliases: &["MEXC_API_KEY"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &["MEXC_API_SECRET"],
            required: true,
        },
    ];
    const COINEX_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "API Key",
            aliases: &["COINEX_API_KEY"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &["COINEX_API_SECRET"],
            required: true,
        },
    ];
    const GATE_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "account_id",
            label: "Gate User ID / Account ID",
            aliases: &[
                "GATE_ACCOUNT_ID",
                "GATEIO_ACCOUNT_ID",
                "GATE_USER_ID",
                "GATEIO_USER_ID",
            ],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "API Key",
            aliases: &["GATE_API_KEY", "GATEIO_API_KEY"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &["GATE_API_SECRET", "GATEIO_API_SECRET"],
            required: true,
        },
    ];
    const BYBIT_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "API Key",
            aliases: &["BYBIT_API_KEY"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &["BYBIT_API_SECRET"],
            required: true,
        },
    ];
    const HTX_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "API Key",
            aliases: &["HTX_API_KEY", "HUOBI_API_KEY"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &["HTX_API_SECRET", "HUOBI_API_SECRET"],
            required: true,
        },
    ];
    const BITMART_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "API Key",
            aliases: &["BITMART_API_KEY"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &["BITMART_API_SECRET"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "passphrase",
            label: "Memo",
            aliases: &["BITMART_MEMO", "BITMART_API_MEMO", "BITMART_PASSPHRASE"],
            required: true,
        },
    ];
    const HYPERLIQUID_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "Wallet / API Key",
            aliases: &["HYPERLIQUID_API_KEY", "HYPERLIQUID_WALLET_ADDRESS"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "Private Key",
            aliases: &["HYPERLIQUID_API_SECRET", "HYPERLIQUID_PRIVATE_KEY"],
            required: true,
        },
    ];
    const BITGET_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "API Key",
            aliases: &["BITGET_API_KEY"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &["BITGET_API_SECRET"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "passphrase",
            label: "Passphrase",
            aliases: &["BITGET_PASSPHRASE", "BITGET_API_PASSPHRASE"],
            required: true,
        },
    ];
    const KUCOIN_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "API Key",
            aliases: &["KUCOIN_API_KEY"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &["KUCOIN_API_SECRET"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "passphrase",
            label: "API Passphrase",
            aliases: &["KUCOIN_API_PASSPHRASE"],
            required: true,
        },
    ];
    const BINANCE_SPOT_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "API Key",
            aliases: &["BINANCE_SPOT_API_KEY", "BINANCE_API_KEY"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &["BINANCE_SPOT_API_SECRET", "BINANCE_API_SECRET"],
            required: true,
        },
    ];
    const OKX_SPOT_FIELDS: &[ExchangeApiFieldSchema] = &[
        ExchangeApiFieldSchema {
            field: "api_key",
            label: "API Key",
            aliases: &["OKX_SPOT_API_KEY", "OKX_API_KEY"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &["OKX_SPOT_API_SECRET", "OKX_API_SECRET"],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "passphrase",
            label: "Passphrase",
            aliases: &["OKX_SPOT_PASSPHRASE", "OKX_PASSPHRASE"],
            required: true,
        },
    ];
    const SCHEMAS: &[ExchangeApiKeySchema] = &[
        ExchangeApiKeySchema {
            exchange: "binance",
            label: "Binance",
            fields: BINANCE_SPOT_FIELDS,
        },
        ExchangeApiKeySchema {
            exchange: "okx",
            label: "OKX",
            fields: OKX_SPOT_FIELDS,
        },
        ExchangeApiKeySchema {
            exchange: "bitget",
            label: "Bitget",
            fields: BITGET_FIELDS,
        },
        ExchangeApiKeySchema {
            exchange: "gate",
            label: "Gate.io",
            fields: GATE_FIELDS,
        },
        ExchangeApiKeySchema {
            exchange: "bybit",
            label: "Bybit",
            fields: BYBIT_FIELDS,
        },
        ExchangeApiKeySchema {
            exchange: "mexc",
            label: "MEXC",
            fields: MEXC_FIELDS,
        },
        ExchangeApiKeySchema {
            exchange: "coinex",
            label: "CoinEx",
            fields: COINEX_FIELDS,
        },
        ExchangeApiKeySchema {
            exchange: "kucoin",
            label: "KuCoin",
            fields: KUCOIN_FIELDS,
        },
        ExchangeApiKeySchema {
            exchange: "htx",
            label: "HTX",
            fields: HTX_FIELDS,
        },
        ExchangeApiKeySchema {
            exchange: "bitmart",
            label: "BitMart",
            fields: BITMART_FIELDS,
        },
        ExchangeApiKeySchema {
            exchange: "hyperliquid",
            label: "Hyperliquid",
            fields: HYPERLIQUID_FIELDS,
        },
    ];
    SCHEMAS
}

fn set_if_present(values: &mut BTreeMap<String, String>, key: &str, value: Option<&str>) {
    if let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) {
        values.insert(key.to_string(), value.to_string());
    }
}

fn reject_multiline_secret(field: &str, value: Option<&str>) -> Result<()> {
    if value.is_some_and(|value| value.contains('\n') || value.contains('\r')) {
        return Err(anyhow::anyhow!("{field} must be a single-line value"));
    }
    Ok(())
}

async fn read_env_store(path: &PathBuf) -> Result<BTreeMap<String, String>> {
    let content = match tokio::fs::read_to_string(path).await {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
    };
    let mut values = BTreeMap::new();
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if is_shell_env_key(key) {
            values.insert(key.to_string(), parse_shell_env_value(value.trim()));
        }
    }
    Ok(values)
}

async fn write_env_store(path: &PathBuf, values: &BTreeMap<String, String>) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let mut content = "# Managed by RustCTA control-api. Do not commit this file.\n".to_string();
    for (key, value) in values {
        if is_shell_env_key(key) {
            content.push_str(key);
            content.push('=');
            content.push_str(&shell_quote(value));
            content.push('\n');
        }
    }
    let tmp_path = path.with_extension("env.tmp");
    tokio::fs::write(&tmp_path, content)
        .await
        .with_context(|| format!("write {}", tmp_path.display()))?;
    set_file_mode_600(&tmp_path).await?;
    tokio::fs::rename(&tmp_path, path)
        .await
        .with_context(|| format!("replace {}", path.display()))?;
    set_file_mode_600(path).await?;
    Ok(())
}

async fn set_file_mode_600(path: &PathBuf) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let permissions = std::fs::Permissions::from_mode(0o600);
        tokio::fs::set_permissions(path, permissions)
            .await
            .with_context(|| format!("chmod 600 {}", path.display()))?;
    }
    Ok(())
}

fn parse_shell_env_value(value: &str) -> String {
    if value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2 {
        value[1..value.len() - 1].replace("'\\''", "'")
    } else {
        value.trim_matches('"').to_string()
    }
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn is_shell_env_key(key: &str) -> bool {
    !key.is_empty()
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn mask_secret(value: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= 8 {
        return "********".to_string();
    }
    let prefix = chars.iter().take(4).collect::<String>();
    let suffix = chars
        .iter()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("{prefix}...{suffix}")
}

async fn balance_history_writer(state: AppState) {
    loop {
        if let Err(error) = append_balance_history_sample(&state).await {
            log::warn!("balance history sample failed: {error:#}");
        }
        sleep(Duration::from_millis(state.balance_history_interval_ms)).await;
    }
}

async fn append_balance_history_sample(state: &AppState) -> Result<()> {
    let model = read_snapshot(state).await?;
    append_strategy_profit_history(state, &model).await?;
    if model.inventory.is_empty() {
        return Ok(());
    }
    let mut point = balance_history_point(&model.inventory);
    let mut values = read_balance_history(&state.balance_history_path)
        .await?
        .as_array()
        .cloned()
        .unwrap_or_default();
    let first_total = values
        .first()
        .and_then(|row| row.get("total_usdt").and_then(Value::as_f64))
        .or_else(|| point.get("total_usdt").and_then(Value::as_f64))
        .unwrap_or_default();
    if let Some(total) = point.get("total_usdt").and_then(Value::as_f64) {
        point["profit_usdt"] = json!(total - first_total);
    }
    values.push(point);
    let extra = values.len().saturating_sub(10_000);
    if extra > 0 {
        values.drain(0..extra);
    }
    write_balance_history(&state.balance_history_path, &Value::Array(values)).await
}

async fn append_strategy_profit_history(
    state: &AppState,
    model: &DashboardReadModel,
) -> Result<()> {
    if model.trades.is_empty() {
        return Ok(());
    }
    let mut values = read_balance_history(&state.strategy_profit_history_path)
        .await?
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut seen = values
        .iter()
        .filter_map(|row| row.get("trade_id").and_then(Value::as_str))
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let mut cumulative_profit = values
        .last()
        .and_then(|row| row.get("profit_usdt").and_then(Value::as_f64))
        .unwrap_or_default();
    let mut trades = model.trades.clone();
    trades.sort_by_key(|trade| trade.timestamp);
    let mut changed = false;
    for trade in trades {
        let trade_id = strategy_profit_trade_id(&trade);
        if !seen.insert(trade_id.clone()) {
            continue;
        }
        cumulative_profit += trade.net_pnl;
        values.push(strategy_profit_history_point(
            &trade,
            &trade_id,
            cumulative_profit,
        ));
        changed = true;
    }
    if !changed {
        return Ok(());
    }
    let extra = values.len().saturating_sub(10_000);
    if extra > 0 {
        values.drain(0..extra);
    }
    write_balance_history(&state.strategy_profit_history_path, &Value::Array(values)).await
}

fn strategy_profit_trade_id(trade: &TradeView) -> String {
    if !trade.trade_id.trim().is_empty() {
        return trade.trade_id.clone();
    }
    format!(
        "{}:{}:{}:{}:{:.12}:{:.12}",
        trade.timestamp.timestamp_micros(),
        trade.symbol,
        trade.buy_exchange,
        trade.sell_exchange,
        trade.quantity,
        trade.net_pnl
    )
}

fn strategy_profit_history_point(
    trade: &TradeView,
    trade_id: &str,
    cumulative_profit: f64,
) -> Value {
    json!({
        "timestamp": trade.timestamp,
        "series": "strategy_profit",
        "trade_id": trade_id,
        "symbol": trade.symbol,
        "buy_exchange": trade.buy_exchange,
        "sell_exchange": trade.sell_exchange,
        "quantity": trade.quantity,
        "gross_pnl": trade.gross_pnl,
        "total_fee": trade.total_fee,
        "trade_profit_usdt": trade.net_pnl,
        "profit_usdt": cumulative_profit,
        "total_usdt": cumulative_profit,
        "available_usdt": cumulative_profit,
        "execution_mode": trade.execution_mode,
        "paper_or_live": trade.paper_or_live,
        "exchanges": [],
    })
}

trait EmptyStringFallback {
    fn if_empty_then(self, fallback: impl FnOnce() -> String) -> String;
}

impl EmptyStringFallback for String {
    fn if_empty_then(self, fallback: impl FnOnce() -> String) -> String {
        if self.trim().is_empty() || self == "-" {
            fallback()
        } else {
            self
        }
    }
}

fn number_field_any(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .filter_map(|key| value.get(*key).and_then(Value::as_f64))
        .find(|value| value.is_finite())
}

fn number_or_null(value: &Value, keys: &[&str]) -> Value {
    number_field_any(value, keys)
        .map(|value| json!(value))
        .unwrap_or(Value::Null)
}

fn balance_history_point(inventory: &[InventoryView]) -> Value {
    let mut by_exchange = BTreeMap::<String, BalanceHistoryExchangeTotal>::new();
    for item in inventory {
        let total_usdt = inventory_total_usdt(item);
        let available_usdt = inventory_available_usdt(item, total_usdt);
        let entry = by_exchange.entry(item.exchange.clone()).or_default();
        entry.total_usdt += total_usdt;
        entry.available_usdt += available_usdt;
        entry.asset_count += 1;
    }
    let exchanges = by_exchange
        .into_iter()
        .map(|(exchange, total)| {
            json!({
                "exchange": exchange,
                "total_usdt": total.total_usdt,
                "available_usdt": total.available_usdt,
                "asset_count": total.asset_count,
            })
        })
        .collect::<Vec<_>>();
    let total_usdt = exchanges
        .iter()
        .filter_map(|row| row.get("total_usdt").and_then(Value::as_f64))
        .sum::<f64>();
    let available_usdt = exchanges
        .iter()
        .filter_map(|row| row.get("available_usdt").and_then(Value::as_f64))
        .sum::<f64>();
    json!({
        "timestamp": Utc::now(),
        "total_usdt": total_usdt,
        "available_usdt": available_usdt,
        "exchanges": exchanges,
    })
}

fn inventory_total_usdt(item: &InventoryView) -> f64 {
    item.valuation_usdt
        .unwrap_or_else(|| stable_asset_amount(&item.asset, item.total))
}

fn inventory_available_usdt(item: &InventoryView, total_usdt: f64) -> f64 {
    if item.total.abs() > f64::EPSILON && item.valuation_usdt.is_some() {
        total_usdt * (item.effective_available / item.total)
    } else {
        stable_asset_amount(&item.asset, item.effective_available)
    }
}

fn stable_asset_amount(asset: &str, quantity: f64) -> f64 {
    if asset.eq_ignore_ascii_case("USDT")
        || asset.eq_ignore_ascii_case("USDC")
        || asset.eq_ignore_ascii_case("USD")
    {
        quantity
    } else {
        0.0
    }
}

async fn read_balance_history(path: &PathBuf) -> Result<Value> {
    match tokio::fs::read_to_string(path).await {
        Ok(content) => serde_json::from_str(&content)
            .with_context(|| format!("parse balance history {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(json!([])),
        Err(error) => {
            Err(error).with_context(|| format!("read balance history {}", path.display()))
        }
    }
}

async fn write_balance_history(path: &PathBuf, history: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create balance history directory {}", parent.display()))?;
    }
    let content = serde_json::to_vec_pretty(history).context("serialize balance history")?;
    let temp_path = path.with_extension("json.tmp");
    tokio::fs::write(&temp_path, content)
        .await
        .with_context(|| format!("write balance history temp {}", temp_path.display()))?;
    tokio::fs::rename(&temp_path, path)
        .await
        .with_context(|| format!("replace balance history {}", path.display()))?;
    Ok(())
}

async fn guarded<F>(state: &AppState, headers: &HeaderMap, read: F) -> Response
where
    F: std::future::Future<Output = Result<Value>>,
{
    if !is_authorized(state, headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match read.await {
        Ok(value) if response_contains_secret_marker(&value) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": "refusing to return secret-like API response" })),
        )
            .into_response(),
        Ok(value) => Json(value).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn snapshot_json_path(
    state: AppState,
    headers: HeaderMap,
    path: &'static [&str],
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        Ok(value_at_path(&value, path).cloned().unwrap_or(Value::Null))
    })
    .await
}

async fn read_snapshot(state: &AppState) -> Result<DashboardReadModel> {
    match tokio::fs::read_to_string(&state.snapshot_path).await {
        Ok(content) => serde_json::from_str(&content)
            .with_context(|| format!("parse snapshot {}", state.snapshot_path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            Ok(DashboardReadModel::default())
        }
        Err(error) => {
            Err(error).with_context(|| format!("read snapshot {}", state.snapshot_path.display()))
        }
    }
}

async fn read_snapshot_value(state: &AppState) -> Result<Value> {
    let model = read_snapshot(state).await?;
    serde_json::to_value(model).context("serialize snapshot value")
}

async fn append_jsonl(path: &PathBuf, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create command directory {}", parent.display()))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
        .with_context(|| format!("open command queue {}", path.display()))?;
    let mut line = serde_json::to_vec(value).context("serialize command")?;
    line.push(b'\n');
    file.write_all(&line)
        .await
        .with_context(|| format!("write command queue {}", path.display()))?;
    Ok(())
}

async fn persist_cross_arb_instruments(
    event_dir: &FsPath,
    instruments: &[InstrumentMeta],
) -> Result<()> {
    if instruments.is_empty() {
        return Ok(());
    }
    tokio::fs::create_dir_all(event_dir)
        .await
        .with_context(|| format!("create cross-arb event directory {}", event_dir.display()))?;
    let recorded_at = Utc::now();
    let path = event_dir.join(format!(
        "cross_arb_events_{}.jsonl",
        recorded_at.format("%Y%m%d")
    ));
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open cross-arb event storage {}", path.display()))?;
    let base_event_id = recorded_at
        .timestamp_nanos_opt()
        .unwrap_or_else(|| recorded_at.timestamp_micros().saturating_mul(1_000))
        .max(0) as u64;
    for (offset, instrument) in instruments.iter().enumerate() {
        let stored = StoredCrossArbEvent {
            event_id: base_event_id.saturating_add(offset as u64),
            recorded_at,
            event: CrossArbStorageEvent::Instrument(instrument.clone()),
        };
        let mut line =
            serde_json::to_vec(&stored).context("serialize cross-arb instrument event")?;
        line.push(b'\n');
        file.write_all(&line)
            .await
            .with_context(|| format!("write cross-arb event storage {}", path.display()))?;
    }
    Ok(())
}

async fn persist_cross_arb_market_snapshots(
    event_dir: &FsPath,
    snapshots: &[ArbitrageMarketSnapshotRecord],
) -> Result<()> {
    if snapshots.is_empty() {
        return Ok(());
    }
    tokio::fs::create_dir_all(event_dir)
        .await
        .with_context(|| format!("create cross-arb event directory {}", event_dir.display()))?;
    let recorded_at = Utc::now();
    let path = event_dir.join(format!(
        "cross_arb_events_{}.jsonl",
        recorded_at.format("%Y%m%d")
    ));
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open cross-arb event storage {}", path.display()))?;
    let base_event_id = recorded_at
        .timestamp_nanos_opt()
        .unwrap_or_else(|| recorded_at.timestamp_micros().saturating_mul(1_000))
        .max(0) as u64;
    for (offset, snapshot) in snapshots.iter().enumerate() {
        let stored = StoredCrossArbEvent {
            event_id: base_event_id.saturating_add(offset as u64),
            recorded_at,
            event: CrossArbStorageEvent::MarketSnapshot(snapshot.clone()),
        };
        let mut line =
            serde_json::to_vec(&stored).context("serialize cross-arb market snapshot event")?;
        line.push(b'\n');
        file.write_all(&line)
            .await
            .with_context(|| format!("write cross-arb event storage {}", path.display()))?;
    }
    Ok(())
}

async fn latest_snapshot_field_response(
    state: AppState,
    headers: HeaderMap,
    symbol: String,
    field: &'static str,
) -> Response {
    guarded(&state.clone(), &headers, async move {
        let value = read_snapshot_value(&state).await?;
        let snapshot = latest_snapshot_for_symbol(&value, &symbol).unwrap_or(Value::Null);
        Ok(snapshot.get(field).cloned().unwrap_or(Value::Null))
    })
    .await
}

fn value_at_path<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for part in path {
        current = current.get(*part)?;
    }
    Some(current)
}

async fn read_cross_arb_events(base: &PathBuf) -> Result<CrossArbEventSummary> {
    let mut paths = Vec::new();
    if let Ok(mut root) = tokio::fs::read_dir(&base).await {
        while let Some(entry) = root
            .next_entry()
            .await
            .with_context(|| format!("scan {}", base.display()))?
        {
            let path = entry.path();
            if path
                .extension()
                .is_some_and(|extension| extension == "jsonl")
            {
                paths.push(path);
            } else if path.is_dir() {
                if let Ok(mut nested) = tokio::fs::read_dir(&path).await {
                    while let Some(nested_entry) = nested
                        .next_entry()
                        .await
                        .with_context(|| format!("scan {}", path.display()))?
                    {
                        let nested_path = nested_entry.path();
                        if nested_path
                            .extension()
                            .is_some_and(|extension| extension == "jsonl")
                        {
                            paths.push(nested_path);
                        }
                    }
                }
            }
        }
    }
    paths.sort_by_key(|path| path.to_string_lossy().to_string());
    let selected_paths = paths.into_iter().rev().take(5).collect::<Vec<_>>();
    let mut summary = CrossArbEventSummary {
        event_dir: base.display().to_string(),
        files: selected_paths
            .iter()
            .map(|path| path.to_string_lossy().to_string())
            .collect(),
        ..Default::default()
    };
    for path in selected_paths {
        let content = tokio::fs::read_to_string(&path)
            .await
            .with_context(|| format!("read {}", path.display()))?;
        for line in content.lines().rev().take(CROSS_ARB_JSONL_SCAN_LINE_LIMIT) {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                summary.parse_errors += 1;
                continue;
            };
            let Some(event_object) = value.get("event").and_then(Value::as_object) else {
                summary.parse_errors += 1;
                continue;
            };
            let Some((kind, payload)) = event_object.iter().next() else {
                summary.parse_errors += 1;
                continue;
            };
            let row = flatten_cross_arb_event(
                kind,
                payload,
                value.get("event_id").cloned().unwrap_or(Value::Null),
                value.get("recorded_at").cloned().unwrap_or(Value::Null),
            );
            if summary.latest_event_at.is_none() {
                summary.latest_event_at = row
                    .get("recorded_at")
                    .and_then(Value::as_str)
                    .map(str::to_string);
            }
            summary.total_valid_events += 1;
            push_limited(
                &mut summary.events,
                row.clone(),
                CROSS_ARB_EVENT_DISPLAY_LIMIT,
            );
            match kind.as_str() {
                "Opportunity" => push_limited(
                    &mut summary.opportunities,
                    row,
                    CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT,
                ),
                "Signal" => push_limited(
                    &mut summary.signals,
                    row,
                    CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT,
                ),
                "HedgeRecord" => push_limited(
                    &mut summary.hedge_records,
                    row,
                    CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT,
                ),
                "CloseMetric" => push_limited(
                    &mut summary.close_metrics,
                    row,
                    CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT,
                ),
                "HedgeRepairTask" => push_limited(
                    &mut summary.hedge_repair_tasks,
                    row,
                    CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT,
                ),
                "Instrument" => push_limited(
                    &mut summary.instruments,
                    row,
                    CROSS_ARB_INSTRUMENT_DISPLAY_LIMIT,
                ),
                "MarketSnapshot" => push_limited(
                    &mut summary.market_snapshots,
                    row,
                    CROSS_ARB_MARKET_SNAPSHOT_DISPLAY_LIMIT,
                ),
                "OrderTransition" => push_limited(
                    &mut summary.order_events,
                    row,
                    CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT,
                ),
                "PrivateEvent" => push_limited(
                    &mut summary.private_events,
                    row,
                    CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT,
                ),
                "RiskEvent" | "RiskState" => push_limited(
                    &mut summary.risk_events,
                    row,
                    CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT,
                ),
                "Bundle" => push_limited(&mut summary.bundles, row, CROSS_ARB_BUNDLE_DISPLAY_LIMIT),
                _ => {}
            }
        }
    }
    Ok(summary)
}

fn flatten_cross_arb_event(
    kind: &str,
    payload: &Value,
    event_id: Value,
    recorded_at: Value,
) -> Value {
    let mut row = serde_json::Map::new();
    row.insert("kind".to_string(), Value::String(kind.to_string()));
    row.insert("event_id".to_string(), event_id);
    row.insert("recorded_at".to_string(), recorded_at);
    row.insert("payload".to_string(), payload.clone());
    if let Some(fields) = payload.as_object() {
        for (key, value) in fields {
            row.insert(key.clone(), value.clone());
        }
    }
    if kind == "PrivateEvent" {
        if let Some((private_kind, private_payload)) = payload
            .get("event")
            .and_then(Value::as_object)
            .and_then(|event| event.iter().next())
        {
            row.insert(
                "private_kind".to_string(),
                Value::String(private_kind.to_string()),
            );
            if let Some(private_fields) = private_payload.as_object() {
                for (key, value) in private_fields {
                    row.entry(key.clone()).or_insert_with(|| value.clone());
                }
            }
        }
    }
    Value::Object(row)
}

fn push_limited(values: &mut Vec<Value>, value: Value, limit: usize) {
    if values.len() < limit {
        values.push(value);
    }
}

fn latest_cross_arb_mid_prices(market_snapshots: &[Value]) -> BTreeMap<(String, String), f64> {
    let mut prices = BTreeMap::new();
    for row in market_snapshots.iter().rev() {
        let exchange = text_field(row, "exchange");
        let symbol = text_field(row, "symbol");
        let bid = numeric_field(row, "best_bid");
        let ask = numeric_field(row, "best_ask");
        if exchange.is_empty() || symbol.is_empty() || bid <= 0.0 || ask <= 0.0 {
            continue;
        }
        prices
            .entry((exchange, symbol))
            .or_insert_with(|| (bid + ask) / 2.0);
    }
    prices
}

fn latest_cross_arb_instruments_by_venue_symbol(
    instruments: &[Value],
) -> BTreeMap<(String, String), Value> {
    let mut latest = BTreeMap::<(String, String), Value>::new();
    for row in instruments {
        let exchange = text_field(row, "exchange").to_ascii_lowercase();
        let symbol =
            text_field(row, "canonical_symbol").if_empty_then(|| text_field(row, "symbol"));
        if exchange.is_empty() || symbol.is_empty() {
            continue;
        }
        let key = (exchange, symbol);
        let should_replace = latest
            .get(&key)
            .map(|existing| {
                cross_arb_instrument_time_key(row) >= cross_arb_instrument_time_key(existing)
            })
            .unwrap_or(true);
        if should_replace {
            latest.insert(key, row.clone());
        }
    }
    latest
}

fn cross_arb_instrument_time_key(row: &Value) -> String {
    text_field(row, "updated_at")
        .if_empty_then(|| text_field(row, "recorded_at"))
        .if_empty_then(|| text_field(row, "timestamp"))
}

async fn load_cross_arb_reference_prices(
    config: &CrossExchangeArbitrageConfig,
) -> BTreeMap<(String, String), f64> {
    let wanted = config
        .universe
        .symbols
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let mut prices = BTreeMap::new();
    for exchange in &config.universe.enabled_exchanges {
        match exchange {
            ExchangeId::Binance => {
                prices.extend(load_binance_usdt_perp_prices(&wanted).await);
            }
            ExchangeId::Bitget => {
                prices.extend(load_bitget_usdt_perp_prices(&wanted).await);
            }
            ExchangeId::Gate => {
                prices.extend(load_gate_usdt_perp_prices(&wanted).await);
            }
            _ => {}
        }
    }
    prices
}

async fn load_cross_arb_public_market_snapshots(
    config: &CrossExchangeArbitrageConfig,
) -> (Vec<ArbitrageMarketSnapshotRecord>, Vec<Value>) {
    let wanted = config
        .universe
        .symbols
        .iter()
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let mut snapshots = Vec::new();
    let mut errors = Vec::new();
    for exchange in &config.universe.enabled_exchanges {
        match exchange {
            ExchangeId::Binance => match load_binance_usdt_perp_book_tickers(&wanted).await {
                Ok(mut rows) => snapshots.append(&mut rows),
                Err(error) => errors.push(json!({
                    "exchange": exchange.to_string(),
                    "message": error.to_string(),
                })),
            },
            ExchangeId::Gate => match load_gate_usdt_perp_book_tickers(&wanted).await {
                Ok(mut rows) => snapshots.append(&mut rows),
                Err(error) => errors.push(json!({
                    "exchange": exchange.to_string(),
                    "message": error.to_string(),
                })),
            },
            ExchangeId::Bitget => match load_bitget_usdt_perp_book_tickers(&wanted).await {
                Ok(mut rows) => snapshots.append(&mut rows),
                Err(error) => errors.push(json!({
                    "exchange": exchange.to_string(),
                    "message": error.to_string(),
                })),
            },
            other => errors.push(json!({
                "exchange": other.to_string(),
                "message": "public market snapshot refresh is not registered for this exchange",
            })),
        }
    }
    (snapshots, errors)
}

async fn load_binance_usdt_perp_book_tickers(
    wanted: &BTreeSet<String>,
) -> Result<Vec<ArbitrageMarketSnapshotRecord>> {
    let value = reqwest::get("https://fapi.binance.com/fapi/v1/ticker/bookTicker")
        .await
        .context("binance book ticker request failed")?
        .error_for_status()
        .context("binance book ticker status failed")?
        .json::<Value>()
        .await
        .context("binance book ticker json failed")?;
    let now = Utc::now();
    Ok(value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let symbol = item
                .get("symbol")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let canonical = canonical_from_usdt_suffix(symbol)?;
            if !wanted.is_empty() && !wanted.contains(&canonical) {
                return None;
            }
            market_snapshot_record(
                now,
                ExchangeId::Binance,
                canonical,
                symbol.to_string(),
                positive_number_from_keys(item, &["bidPrice"]),
                positive_number_from_keys(item, &["bidQty"]),
                positive_number_from_keys(item, &["askPrice"]),
                positive_number_from_keys(item, &["askQty"]),
                item.get("lastUpdateId").and_then(Value::as_u64),
            )
        })
        .collect())
}

async fn load_gate_usdt_perp_book_tickers(
    wanted: &BTreeSet<String>,
) -> Result<Vec<ArbitrageMarketSnapshotRecord>> {
    let value = reqwest::get("https://api.gateio.ws/api/v4/futures/usdt/tickers")
        .await
        .context("gate book ticker request failed")?
        .error_for_status()
        .context("gate book ticker status failed")?
        .json::<Value>()
        .await
        .context("gate book ticker json failed")?;
    let now = Utc::now();
    Ok(value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let symbol = item
                .get("contract")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let canonical = canonical_from_gate_contract(symbol)?;
            if !wanted.is_empty() && !wanted.contains(&canonical) {
                return None;
            }
            market_snapshot_record(
                now,
                ExchangeId::Gate,
                canonical,
                symbol.to_string(),
                positive_number_from_keys(item, &["highest_bid"]),
                positive_number_from_keys(item, &["highest_size"]),
                positive_number_from_keys(item, &["lowest_ask"]),
                positive_number_from_keys(item, &["lowest_size"]),
                None,
            )
        })
        .collect())
}

async fn load_bitget_usdt_perp_book_tickers(
    wanted: &BTreeSet<String>,
) -> Result<Vec<ArbitrageMarketSnapshotRecord>> {
    let value =
        reqwest::get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")
            .await
            .context("bitget book ticker request failed")?
            .error_for_status()
            .context("bitget book ticker status failed")?
            .json::<Value>()
            .await
            .context("bitget book ticker json failed")?;
    let now = Utc::now();
    Ok(value
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| {
            let symbol = item
                .get("symbol")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let canonical = canonical_from_usdt_suffix(symbol)?;
            if !wanted.is_empty() && !wanted.contains(&canonical) {
                return None;
            }
            market_snapshot_record(
                now,
                ExchangeId::Bitget,
                canonical,
                symbol.to_string(),
                positive_number_from_keys(item, &["bidPr", "bestBid", "bidPrice"]),
                positive_number_from_keys(item, &["bidSz", "bidSize", "bidQty"]),
                positive_number_from_keys(item, &["askPr", "bestAsk", "askPrice"]),
                positive_number_from_keys(item, &["askSz", "askSize", "askQty"]),
                None,
            )
        })
        .collect())
}

fn market_snapshot_record(
    timestamp: DateTime<Utc>,
    exchange: ExchangeId,
    canonical_symbol: String,
    exchange_symbol: String,
    best_bid: Option<f64>,
    best_bid_quantity: Option<f64>,
    best_ask: Option<f64>,
    best_ask_quantity: Option<f64>,
    sequence: Option<u64>,
) -> Option<ArbitrageMarketSnapshotRecord> {
    if best_bid.is_none() && best_ask.is_none() {
        return None;
    }
    Some(ArbitrageMarketSnapshotRecord {
        timestamp,
        exchange,
        symbol: CanonicalSymbol::parse(&canonical_symbol)?,
        exchange_symbol,
        best_bid,
        best_bid_quantity,
        best_ask,
        best_ask_quantity,
        sequence,
    })
}

async fn load_binance_usdt_perp_prices(
    wanted: &BTreeSet<String>,
) -> BTreeMap<(String, String), f64> {
    let mut prices = BTreeMap::new();
    let Ok(value) = reqwest::get("https://fapi.binance.com/fapi/v1/ticker/24hr").await else {
        return prices;
    };
    let Ok(value) = value.error_for_status() else {
        return prices;
    };
    let Ok(value) = value.json::<Value>().await else {
        return prices;
    };
    for item in value.as_array().into_iter().flatten() {
        let symbol = item
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(canonical) = canonical_from_usdt_suffix(symbol) else {
            continue;
        };
        if !wanted.is_empty() && !wanted.contains(&canonical) {
            continue;
        }
        if let Some(price) = positive_number_from_keys(item, &["lastPrice", "weightedAvgPrice"]) {
            prices.insert(("binance".to_string(), canonical), price);
        }
    }
    prices
}

async fn load_gate_usdt_perp_prices(wanted: &BTreeSet<String>) -> BTreeMap<(String, String), f64> {
    let mut prices = BTreeMap::new();
    let Ok(value) = reqwest::get("https://api.gateio.ws/api/v4/futures/usdt/tickers").await else {
        return prices;
    };
    let Ok(value) = value.error_for_status() else {
        return prices;
    };
    let Ok(value) = value.json::<Value>().await else {
        return prices;
    };
    for item in value.as_array().into_iter().flatten() {
        let symbol = item
            .get("contract")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(canonical) = canonical_from_gate_contract(symbol) else {
            continue;
        };
        if !wanted.is_empty() && !wanted.contains(&canonical) {
            continue;
        }
        if let Some(price) = positive_number_from_keys(item, &["last", "mark_price", "index_price"])
        {
            prices.insert(("gate".to_string(), canonical), price);
        }
    }
    prices
}

async fn load_bitget_usdt_perp_prices(
    wanted: &BTreeSet<String>,
) -> BTreeMap<(String, String), f64> {
    let mut prices = BTreeMap::new();
    let Ok(value) =
        reqwest::get("https://api.bitget.com/api/v2/mix/market/tickers?productType=USDT-FUTURES")
            .await
    else {
        return prices;
    };
    let Ok(value) = value.error_for_status() else {
        return prices;
    };
    let Ok(value) = value.json::<Value>().await else {
        return prices;
    };
    for item in value
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let symbol = item
            .get("symbol")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let Some(canonical) = canonical_from_usdt_suffix(symbol) else {
            continue;
        };
        if !wanted.is_empty() && !wanted.contains(&canonical) {
            continue;
        }
        if let Some(price) = positive_number_from_keys(item, &["lastPr", "markPrice", "indexPrice"])
        {
            prices.insert(("bitget".to_string(), canonical), price);
        }
    }
    prices
}

fn canonical_from_usdt_suffix(symbol: &str) -> Option<String> {
    let base = symbol.strip_suffix("USDT")?;
    (!base.is_empty()).then(|| format!("{base}/USDT"))
}

fn canonical_from_gate_contract(symbol: &str) -> Option<String> {
    let base = symbol.strip_suffix("_USDT")?;
    (!base.is_empty()).then(|| format!("{base}/USDT"))
}

fn positive_number_from_keys(value: &Value, keys: &[&str]) -> Option<f64> {
    keys.iter()
        .filter_map(|key| value.get(*key))
        .find_map(json_number)
        .filter(|number| number.is_finite() && *number > 0.0)
}

fn json_number(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
}

fn enrich_cross_arb_instrument_feasibility(
    mut row: Value,
    reference_price: Option<f64>,
    target_notional_usdt: f64,
    max_notional_usdt: f64,
) -> Value {
    let feasible = reference_price
        .filter(|price| price.is_finite() && *price > 0.0)
        .map(|price| {
            let required_notional =
                required_notional_for_instrument(&row, price, target_notional_usdt);
            let known = required_notional.is_finite() && required_notional > 0.0;
            json!({
                "known": known,
                "reference_price": price,
                "target_notional_usdt": target_notional_usdt,
                "max_notional_usdt": max_notional_usdt,
                "required_notional_usdt": required_notional,
                "fits_target": known && required_notional <= target_notional_usdt,
                "fits_max": known && required_notional <= max_notional_usdt,
                "headroom_usdt": if known { max_notional_usdt - required_notional } else { 0.0 },
            })
        })
        .unwrap_or_else(|| {
            json!({
                "known": false,
                "target_notional_usdt": target_notional_usdt,
                "max_notional_usdt": max_notional_usdt,
                "required_notional_usdt": 0.0,
                "fits_target": false,
                "fits_max": false,
                "headroom_usdt": 0.0,
            })
        });
    if let Some(object) = row.as_object_mut() {
        object.insert("feasibility".to_string(), feasible);
    }
    row
}

fn required_notional_for_instrument(
    row: &Value,
    reference_price: f64,
    target_notional_usdt: f64,
) -> f64 {
    let contract_size = positive_numeric_field(row, "contract_size").unwrap_or(1.0);
    let quantity_step = positive_numeric_field(row, "quantity_step").unwrap_or(1.0);
    let min_qty = positive_numeric_field(row, "min_qty").unwrap_or(0.0);
    let min_notional = positive_numeric_field(row, "min_notional").unwrap_or(0.0);
    let min_base = (target_notional_usdt / reference_price)
        .max(min_notional / reference_price)
        .max(min_qty * contract_size);
    let raw_quantity = min_base / contract_size;
    let normalized_quantity = ceil_to_step(raw_quantity, quantity_step);
    normalized_quantity * contract_size * reference_price
}

fn cross_arb_feasibility_summary(
    rows: &[Value],
    symbol_count: usize,
    min_common_exchanges: usize,
) -> Value {
    let mut feasible_by_symbol = BTreeMap::<String, usize>::new();
    let mut known_by_symbol = BTreeMap::<String, usize>::new();
    let mut required_notional_max = 0.0_f64;
    let mut feasible_instruments = 0usize;
    let mut known_instruments = 0usize;
    for row in rows {
        let symbol = text_field(row, "canonical_symbol");
        let Some(feasibility) = row.get("feasibility") else {
            continue;
        };
        if bool_field(feasibility, "known") {
            known_instruments += 1;
            *known_by_symbol.entry(symbol.clone()).or_default() += 1;
            required_notional_max =
                required_notional_max.max(numeric_field(feasibility, "required_notional_usdt"));
        }
        if bool_field(feasibility, "fits_max") {
            feasible_instruments += 1;
            *feasible_by_symbol.entry(symbol).or_default() += 1;
        }
    }
    let feasible_symbols = feasible_by_symbol
        .values()
        .filter(|count| **count >= min_common_exchanges)
        .count();
    let known_symbols = known_by_symbol
        .values()
        .filter(|count| **count >= min_common_exchanges)
        .count();
    json!({
        "known_instruments": known_instruments,
        "feasible_instruments": feasible_instruments,
        "known_symbols": known_symbols,
        "feasible_symbols": feasible_symbols,
        "symbol_count": symbol_count,
        "min_common_exchanges": min_common_exchanges,
        "required_notional_max_usdt": required_notional_max,
    })
}

fn positive_numeric_field(value: &Value, key: &str) -> Option<f64> {
    value
        .get(key)
        .and_then(Value::as_f64)
        .filter(|number| number.is_finite() && *number > 0.0)
}

fn ceil_to_step(value: f64, step: f64) -> f64 {
    if !value.is_finite() || !step.is_finite() || step <= 0.0 {
        return value;
    }
    let scaled = value / step;
    let nearest = scaled.round();
    let scaled = if (scaled - nearest).abs() <= 1e-10 {
        nearest
    } else {
        scaled
    };
    normalize_decimal(scaled.ceil() * step)
}

fn normalize_decimal(value: f64) -> f64 {
    format!("{value:.12}")
        .trim_end_matches('0')
        .trim_end_matches('.')
        .parse()
        .unwrap_or(value)
}

fn filter_cross_arb_profit_outliers(mut summary: CrossArbEventSummary) -> CrossArbEventSummary {
    let before = summary.opportunities.len()
        + summary.signals.len()
        + summary.hedge_records.len()
        + summary.close_metrics.len()
        + summary.events.len();
    summary
        .opportunities
        .retain(|row| !is_cross_arb_profit_outlier(row));
    summary
        .signals
        .retain(|row| !is_cross_arb_profit_outlier(row));
    summary
        .hedge_records
        .retain(|row| !is_cross_arb_profit_outlier(row));
    summary
        .close_metrics
        .retain(|row| !is_cross_arb_profit_outlier(row));
    summary
        .events
        .retain(|row| !is_cross_arb_profit_outlier(row));
    let after = summary.opportunities.len()
        + summary.signals.len()
        + summary.hedge_records.len()
        + summary.close_metrics.len()
        + summary.events.len();
    summary.profit_outlier_filtered = before.saturating_sub(after);
    summary
}

fn filter_cross_arb_dashboard_scope(
    mut summary: CrossArbEventSummary,
    enabled_exchanges: &[String],
    enabled_symbols: &[String],
) -> CrossArbEventSummary {
    let exchanges = cross_arb_exchange_filter(enabled_exchanges);
    let symbols = cross_arb_symbol_filter(enabled_symbols);
    let scoped_opportunity_ids = summary
        .opportunities
        .iter()
        .filter(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols))
        .filter_map(|row| {
            let id = text_field(row, "opportunity_id");
            (!id.is_empty()).then_some(id)
        })
        .collect::<BTreeSet<_>>();
    summary
        .opportunities
        .retain(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols));
    summary.signals.retain(|row| {
        let opportunity_id = text_field(row, "opportunity_id");
        if !opportunity_id.is_empty() {
            return scoped_opportunity_ids.contains(&opportunity_id);
        }
        cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols)
    });
    summary
        .hedge_records
        .retain(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols));
    summary
        .close_metrics
        .retain(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols));
    summary
        .hedge_repair_tasks
        .retain(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols));
    summary
        .market_snapshots
        .retain(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols));
    summary
        .instruments
        .retain(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols));
    summary
        .order_events
        .retain(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols));
    summary
        .private_events
        .retain(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols));
    summary
        .risk_events
        .retain(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols));
    summary
        .bundles
        .retain(|row| cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols));
    summary.events.retain(|row| {
        let kind = text_field(row, "kind");
        if kind == "Signal" {
            let opportunity_id = text_field(row, "opportunity_id");
            return opportunity_id.is_empty() || scoped_opportunity_ids.contains(&opportunity_id);
        }
        cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols)
    });
    summary
}

fn cross_arb_symbol_filter(enabled_symbols: &[String]) -> BTreeSet<String> {
    enabled_symbols
        .iter()
        .map(|symbol| normalize_symbol(symbol))
        .filter(|symbol| !symbol.is_empty())
        .collect()
}

fn cross_arb_row_matches_dashboard_scope(
    row: &Value,
    enabled_exchanges: &[String],
    enabled_symbols: &[String],
) -> bool {
    let exchanges = cross_arb_exchange_filter(enabled_exchanges);
    let symbols = cross_arb_symbol_filter(enabled_symbols);
    cross_arb_row_matches_dashboard_scope_sets(row, &exchanges, &symbols)
}

fn cross_arb_row_matches_dashboard_scope_sets(
    row: &Value,
    enabled_exchanges: &BTreeSet<String>,
    enabled_symbols: &BTreeSet<String>,
) -> bool {
    cross_arb_row_matches_symbol_filter(row, enabled_symbols)
        && cross_arb_row_matches_exchange_filter(row, enabled_exchanges)
}

fn cross_arb_row_matches_symbol_filter(row: &Value, enabled_symbols: &BTreeSet<String>) -> bool {
    if enabled_symbols.is_empty() {
        return true;
    }
    let fields = [
        "canonical_symbol",
        "symbol",
        "internal_symbol",
        "exchange_symbol",
    ];
    let row_symbols = fields
        .iter()
        .filter_map(|field| row.get(*field))
        .flat_map(cross_arb_symbol_values)
        .map(|symbol| normalize_symbol(&symbol))
        .filter(|symbol| !symbol.is_empty())
        .collect::<Vec<_>>();
    row_symbols.is_empty()
        || row_symbols
            .iter()
            .any(|symbol| enabled_symbols.contains(symbol))
}

fn cross_arb_symbol_values(value: &Value) -> Vec<String> {
    match value {
        Value::String(value) => vec![value.clone()],
        Value::Object(map) => map
            .get("symbol")
            .and_then(Value::as_str)
            .map(|value| vec![value.to_string()])
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn cross_arb_latest_event_is_fresh(summary: &CrossArbEventSummary, now: DateTime<Utc>) -> bool {
    const FRESH_EVENT_SECS: i64 = 30;
    summary
        .latest_event_at
        .as_deref()
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|event_at| {
            let event_at = event_at.with_timezone(&Utc);
            let age = now.signed_duration_since(event_at).num_seconds();
            (0..=FRESH_EVENT_SECS).contains(&age)
        })
        .unwrap_or(false)
}

fn is_cross_arb_profit_outlier(value: &Value) -> bool {
    const MAX_REASONABLE_PROFIT_PCT: f64 = 0.10;
    [
        "raw_open_spread",
        "maker_taker_net_edge",
        "expected_edge",
        "close_net_profit_pct",
        "close_profit_pct",
        "entry_net_edge_pct",
    ]
    .into_iter()
    .filter_map(|key| value.get(key).and_then(Value::as_f64))
    .any(|number| number.is_finite() && number.abs() > MAX_REASONABLE_PROFIT_PCT)
}

fn filter_contract_values(source: Option<&Value>, market_keys: &[&str]) -> Vec<Value> {
    source
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter(|value| is_contract_value(value, market_keys))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn is_contract_value(value: &Value, market_keys: &[&str]) -> bool {
    let contract_hint = market_keys
        .iter()
        .filter_map(|key| value.get(*key).and_then(Value::as_str))
        .any(|text| {
            let lower = text.to_ascii_lowercase();
            lower.contains("perp")
                || lower.contains("future")
                || lower.contains("swap")
                || lower.contains("contract")
        });
    if contract_hint {
        return true;
    }
    ["symbol", "exchange_symbol"]
        .into_iter()
        .filter_map(|key| value.get(key).and_then(Value::as_str))
        .any(|text| {
            let lower = text.to_ascii_lowercase();
            lower.contains("swap") || lower.contains("perp")
        })
}

fn text_field(value: &Value, key: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn numeric_field(value: &Value, key: &str) -> f64 {
    value.get(key).and_then(Value::as_f64).unwrap_or_default()
}

fn bool_field(value: &Value, key: &str) -> bool {
    value.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn latest_snapshot_for_symbol(value: &Value, symbol: &str) -> Option<Value> {
    let symbol = normalize_symbol(symbol);
    value
        .pointer("/spot_control/runtime_snapshots")
        .and_then(Value::as_array)?
        .iter()
        .rev()
        .find(|snapshot| symbol_matches(snapshot, &symbol))
        .cloned()
}

fn filter_symbol_array(value: &Value, pointer: &str, symbol: &str) -> Vec<Value> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter(|item| symbol_matches(item, symbol))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn symbol_matches(value: &Value, normalized_symbol: &str) -> bool {
    ["symbol", "internal_symbol"]
        .into_iter()
        .filter_map(|key| value.get(key).and_then(Value::as_str))
        .map(normalize_symbol)
        .any(|item| item == normalized_symbol)
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

fn validate_bind_safety(
    addr: SocketAddr,
    token_env: &str,
    external_access_enabled: bool,
) -> Result<()> {
    if addr.ip().is_loopback() {
        return Ok(());
    }
    if !external_access_enabled {
        anyhow::bail!(
            "refusing non-loopback bind_addr={addr}; pass --external-access-enabled only behind firewall/TLS/reverse proxy"
        );
    }
    let token = std::env::var(token_env).unwrap_or_default();
    if token.len() < 24 {
        anyhow::bail!(
            "refusing external access because {token_env} is missing or too short; use a strong bearer token"
        );
    }
    log::warn!(
        "control-api external access enabled on {}; keep TLS/reverse proxy/firewall protection active",
        addr
    );
    Ok(())
}

fn response_contains_secret_marker(value: &Value) -> bool {
    const FORBIDDEN: &[&str] = &[
        "API_KEY",
        "API_SECRET",
        "PASSPHRASE",
        "GATEIO_API",
        "BITGET_API",
    ];
    match value {
        Value::Object(map) => map.iter().any(|(key, value)| {
            let key_upper = key.to_ascii_uppercase();
            FORBIDDEN.iter().any(|marker| key_upper.contains(marker))
                || matches!(
                    key.to_ascii_lowercase().as_str(),
                    "api_key" | "api_secret" | "secret" | "passphrase" | "authorization"
                )
                || response_contains_secret_marker(value)
        }),
        Value::Array(values) => values.iter().any(response_contains_secret_marker),
        Value::String(value) => {
            let upper = value.to_ascii_uppercase();
            FORBIDDEN.iter().any(|marker| upper.contains(marker))
        }
        _ => false,
    }
}

fn is_authorized(state: &AppState, headers: &HeaderMap) -> bool {
    let Ok(expected) = std::env::var(&state.token_env) else {
        return false;
    };
    if expected.is_empty() {
        return false;
    }
    let Some(header) = headers.get(axum::http::header::AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = header.to_str() else {
        return false;
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return false;
    };
    constant_time_eq(token.as_bytes(), expected.as_bytes())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .fold(0u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rustcta::exchanges::unified::{
        MarketType, OrderRequest, OrderResponse, OrderSide, OrderStatus, OrderType, PositionSide,
        SymbolRule, SymbolStatus, TimeInForce,
    };
    use rustcta::execution::{
        FeeAssetMode, FeeCalculation, FeeSource, LiveDryRunCheck, LiveDryRunOrderPlan,
        LiveDryRunValidationResult,
    };
    use tempfile::tempdir;

    fn noop_restart_script(dir: &std::path::Path) -> PathBuf {
        let script = dir.join("restart-helper.sh");
        std::fs::write(&script, "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&script, permissions).unwrap();
        }
        script
    }

    fn test_app_state(dir: &std::path::Path, config_path: PathBuf) -> AppState {
        AppState {
            snapshot_path: dir.join("snapshot.json"),
            command_path: dir.join("commands.jsonl"),
            exchange_api_key_store: dir.join("exchange_api_keys.env"),
            accounts_config: dir.join("accounts.yml"),
            balance_history_path: dir.join("balance_history.json"),
            strategy_profit_history_path: dir.join("strategy_profit_history.json"),
            strategy_config: config_path,
            strategy_registry: dir.join("strategy_registry.json"),
            strategy_specs: dir.join("strategy_specs.json"),
            restart_script: noop_restart_script(dir),
            token_env: "RUSTCTA_TEST_TOKEN".to_string(),
            event_interval_ms: 1000,
            balance_history_interval_ms: 60_000,
            supervisor: Arc::new(RwLock::new(LocalProcessSupervisor::new())),
        }
    }

    fn write_cross_arb_key_status_config(path: &PathBuf, exchanges: Vec<ExchangeId>) {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.universe.enabled_exchanges = exchanges;
        std::fs::write(path, serde_yaml::to_string(&config).unwrap()).unwrap();
    }

    fn write_accounts_config(path: &PathBuf) {
        std::fs::write(
            path,
            r#"
accounts:
  binance_hcr:
    name: binance_hcr
    exchange: binance
    type: futures
    description: HCR account
    env_prefix: BINANCE_3
    enabled: true
    settings:
      max_positions: 5
      max_orders_per_symbol: 10
  market_maker:
    name: market_maker
    exchange: bitget
    type: futures
    description: Market maker account
    env_prefix: BITGET_MM
    enabled: true
    settings:
      max_positions: 5
      max_orders_per_symbol: 10
  hedge:
    name: hedge
    exchange: bitget
    type: futures
    description: Hedge account
    env_prefix: BITGET_HEDGE
    enabled: true
    settings:
      max_positions: 5
      max_orders_per_symbol: 10
"#,
        )
        .unwrap();
    }

    async fn register_funding_strategy(state: &AppState, config_path: &PathBuf) {
        let mut supervisor = state.supervisor.write().await;
        let mut process = StrategyProcess::new(
            "funding-arb-local",
            "funding_arbitrage",
            "local",
            "local",
            config_path.to_string_lossy().to_string(),
        );
        process.status = ProcessStatus::Stopped;
        let spec = StrategyProcessSpec::new(
            "funding-arb-local",
            "funding_arbitrage",
            "local",
            "local",
            config_path.to_string_lossy().to_string(),
            "cargo",
        )
        .with_args(default_strategy_args(
            "funding_arbitrage",
            &config_path.to_string_lossy(),
            Vec::new(),
        ))
        .with_working_dir(".");
        supervisor.register(process).unwrap();
        supervisor.remember_spec(spec);
    }

    #[tokio::test]
    async fn exchange_api_key_status_should_list_all_supported_exchanges() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("strategy.yml");
        write_cross_arb_key_status_config(&config_path, vec![ExchangeId::Bitget]);
        let state = test_app_state(dir.path(), config_path);
        write_accounts_config(&state.accounts_config);

        let status = exchange_api_key_status(&state).await.unwrap();
        let supported = status
            .supported_exchanges
            .iter()
            .map(|row| row.exchange.as_str())
            .collect::<Vec<_>>();

        assert!(supported.contains(&"bitget"));
        assert!(supported.contains(&"gate"));
        assert!(supported.contains(&"hyperliquid"));
        assert_eq!(status.enabled_exchanges, vec!["bitget"]);
        assert!(status
            .supported_exchanges
            .iter()
            .any(|row| row.exchange == "bitget" && row.enabled));
        assert!(status
            .supported_exchanges
            .iter()
            .any(|row| row.exchange == "gate" && !row.enabled));
    }

    #[tokio::test]
    async fn exchange_api_key_status_should_include_account_manager_accounts() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("strategy.yml");
        write_cross_arb_key_status_config(&config_path, vec![ExchangeId::Binance]);
        let state = test_app_state(dir.path(), config_path);
        write_accounts_config(&state.accounts_config);

        let status = exchange_api_key_status(&state).await.unwrap();
        let account = status
            .account_manager_accounts
            .iter()
            .find(|account| account.account_id == "binance_hcr")
            .expect("account manager account should be exposed");

        assert_eq!(account.exchange, "binance");
        assert_eq!(account.env_prefix, "BINANCE_3");
        assert_eq!(account.credential_namespace, "BINANCE_3");
        assert!(status
            .exchanges
            .iter()
            .any(|row| row.exchange == "binance" && row.account_id == "binance_hcr"));
    }

    #[tokio::test]
    async fn exchange_api_key_status_should_not_resurrect_deleted_process_env_keys() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("strategy.yml");
        write_cross_arb_key_status_config(&config_path, vec![ExchangeId::Binance]);
        let state = test_app_state(dir.path(), config_path);
        write_env_store(
            &state.exchange_api_key_store,
            &BTreeMap::from([
                ("BINANCE_API_KEY".to_string(), "deleted-key".to_string()),
                (
                    "BINANCE_API_SECRET".to_string(),
                    "deleted-secret".to_string(),
                ),
            ]),
        )
        .await
        .unwrap();
        clear_exchange_api_keys(&state.exchange_api_key_store, "binance")
            .await
            .unwrap();

        let status = exchange_api_key_status(&state).await.unwrap();

        assert!(status.exchanges.is_empty());
        assert!(status
            .supported_exchanges
            .iter()
            .filter(|row| row.exchange == "binance")
            .flat_map(|row| row.fields.iter())
            .all(|field| !field.configured));
    }

    #[tokio::test]
    async fn exchange_api_key_update_should_persist_multiple_account_ids() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("strategy.yml");
        write_cross_arb_key_status_config(&config_path, vec![ExchangeId::Bitget]);
        let state = test_app_state(dir.path(), config_path);
        write_accounts_config(&state.accounts_config);

        apply_exchange_api_key_update(
            &state,
            ExchangeApiKeyUpdateRequest {
                exchange: "bitget".to_string(),
                account_id: Some("market_maker".to_string()),
                account_label: Some("ignored_label_a".to_string()),
                credential_namespace: None,
                exchange_account_id: None,
                api_key: Some("key".to_string()),
                api_secret: Some("secret".to_string()),
                passphrase: Some("pass".to_string()),
                clear: false,
            },
        )
        .await
        .unwrap();

        apply_exchange_api_key_update(
            &state,
            ExchangeApiKeyUpdateRequest {
                exchange: "bitget".to_string(),
                account_id: Some("hedge".to_string()),
                account_label: Some("ignored_label_b".to_string()),
                credential_namespace: None,
                exchange_account_id: None,
                api_key: Some("key2".to_string()),
                api_secret: Some("secret2".to_string()),
                passphrase: Some("pass2".to_string()),
                clear: false,
            },
        )
        .await
        .unwrap();

        let status = exchange_api_key_status(&state).await.unwrap();
        let labels = status
            .exchanges
            .iter()
            .filter(|row| row.exchange == "bitget")
            .map(|row| (row.account_id.as_str(), row.account_label.as_str()))
            .collect::<BTreeMap<_, _>>();

        assert_eq!(labels.get("market_maker"), Some(&"market_maker"));
        assert_eq!(labels.get("hedge"), Some(&"hedge"));
        assert!(status
            .exchanges
            .iter()
            .filter(|row| row.exchange == "bitget")
            .all(|row| row.fields.iter().all(|field| field.configured)));
    }

    #[tokio::test]
    async fn exchange_api_key_update_should_persist_runtime_env_prefix_namespace() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("strategy.yml");
        write_cross_arb_key_status_config(&config_path, vec![ExchangeId::Binance]);
        let state = test_app_state(dir.path(), config_path);
        write_accounts_config(&state.accounts_config);

        apply_exchange_api_key_update(
            &state,
            ExchangeApiKeyUpdateRequest {
                exchange: "binance".to_string(),
                account_id: Some("binance_hcr".to_string()),
                account_label: Some("HCR".to_string()),
                credential_namespace: Some("BINANCE_3".to_string()),
                exchange_account_id: None,
                api_key: Some("key".to_string()),
                api_secret: Some("secret".to_string()),
                passphrase: None,
                clear: false,
            },
        )
        .await
        .unwrap();

        let values = read_env_store(&state.exchange_api_key_store).await.unwrap();
        assert_eq!(values.get("BINANCE_3_API_KEY"), Some(&"key".to_string()));
        assert_eq!(
            values.get("BINANCE_3_API_SECRET"),
            Some(&"secret".to_string())
        );

        let status = exchange_api_key_status(&state).await.unwrap();
        let row = status
            .exchanges
            .iter()
            .find(|row| row.credential_namespace == "BINANCE_3")
            .expect("runtime namespace should be listed");
        assert_eq!(row.exchange, "binance");
        assert_eq!(row.account_id, "binance_hcr");
        assert!(row.fields.iter().all(|field| field.configured));
    }

    #[tokio::test]
    async fn exchange_api_key_update_should_use_account_manager_env_prefix() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("strategy.yml");
        write_cross_arb_key_status_config(&config_path, vec![ExchangeId::Binance]);
        let state = test_app_state(dir.path(), config_path);
        write_accounts_config(&state.accounts_config);

        apply_exchange_api_key_update(
            &state,
            ExchangeApiKeyUpdateRequest {
                exchange: "binance".to_string(),
                account_id: Some("binance_hcr".to_string()),
                account_label: None,
                credential_namespace: None,
                exchange_account_id: None,
                api_key: Some("key".to_string()),
                api_secret: Some("secret".to_string()),
                passphrase: None,
                clear: false,
            },
        )
        .await
        .unwrap();

        let values = read_env_store(&state.exchange_api_key_store).await.unwrap();
        assert_eq!(values.get("BINANCE_3_API_KEY"), Some(&"key".to_string()));
        assert_eq!(
            values.get("BINANCE_3_API_SECRET"),
            Some(&"secret".to_string())
        );

        let status = exchange_api_key_status(&state).await.unwrap();
        let row = status
            .exchanges
            .iter()
            .find(|row| row.account_id == "binance_hcr")
            .expect("account manager credential row should be listed");
        assert_eq!(row.credential_namespace, "BINANCE_3");
        assert!(row.fields.iter().all(|field| field.configured));
    }

    #[tokio::test]
    async fn exchange_api_key_update_should_reject_invalid_account_identifier() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("strategy.yml");
        write_cross_arb_key_status_config(&config_path, vec![ExchangeId::Binance]);
        let state = test_app_state(dir.path(), config_path);

        let error = apply_exchange_api_key_update(
            &state,
            ExchangeApiKeyUpdateRequest {
                exchange: "binance".to_string(),
                account_id: Some("main-account".to_string()),
                account_label: None,
                credential_namespace: None,
                exchange_account_id: None,
                api_key: Some("key".to_string()),
                api_secret: Some("secret".to_string()),
                passphrase: None,
                clear: false,
            },
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("account_id must contain only ASCII letters, digits, and _"));
    }

    #[tokio::test]
    async fn exchange_api_key_update_should_reject_account_missing_from_manager() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("strategy.yml");
        write_cross_arb_key_status_config(&config_path, vec![ExchangeId::Binance]);
        let state = test_app_state(dir.path(), config_path);
        write_accounts_config(&state.accounts_config);

        let error = apply_exchange_api_key_update(
            &state,
            ExchangeApiKeyUpdateRequest {
                exchange: "binance".to_string(),
                account_id: Some("unknown_account".to_string()),
                account_label: None,
                credential_namespace: None,
                exchange_account_id: None,
                api_key: Some("key".to_string()),
                api_secret: Some("secret".to_string()),
                passphrase: None,
                clear: false,
            },
        )
        .await
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("is not configured in account manager"));
    }

    #[tokio::test]
    async fn delete_default_exchange_keys_should_clear_aliases_and_subaccounts() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("keys.env");
        write_env_store(
            &path,
            &BTreeMap::from([
                ("BINANCE_API_KEY".to_string(), "key".to_string()),
                ("BINANCE_API_SECRET".to_string(), "secret".to_string()),
                ("BINANCE_SPOT_API_KEY".to_string(), "spot-key".to_string()),
                (
                    "BINANCE_SPOT_API_SECRET".to_string(),
                    "spot-secret".to_string(),
                ),
                (
                    "BINANCE__HEDGE__API_KEY".to_string(),
                    "hedge-key".to_string(),
                ),
                (
                    "BINANCE__HEDGE__API_SECRET".to_string(),
                    "hedge-secret".to_string(),
                ),
                ("BITGET_API_KEY".to_string(), "bitget-key".to_string()),
            ]),
        )
        .await
        .unwrap();

        clear_exchange_api_keys(&path, "binance").await.unwrap();

        let values = read_env_store(&path).await.unwrap();
        assert!(!values.keys().any(|key| key.starts_with("BINANCE")));
        assert_eq!(
            values.get("BITGET_API_KEY"),
            Some(&"bitget-key".to_string())
        );
    }

    fn test_symbol_rule() -> SymbolRule {
        SymbolRule {
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            internal_symbol: "WLDUSDT".to_string(),
            exchange_symbol: "WLD_USDT".to_string(),
            base_asset: "WLD".to_string(),
            quote_asset: "USDT".to_string(),
            price_precision: 4,
            quantity_precision: 4,
            tick_size: 0.0001,
            step_size: 0.0001,
            min_quantity: 0.0001,
            min_notional: 1.0,
            max_quantity: None,
            supported_order_types: vec![OrderType::Limit],
            supported_time_in_force: vec![TimeInForce::GTC],
            status: SymbolStatus::Trading,
            raw_metadata: None,
        }
    }

    fn test_fee_calculation() -> FeeCalculation {
        FeeCalculation {
            symbol: Some("WLDUSDT".to_string()),
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            role: rustcta::execution::FeeRole::Maker,
            fee_asset_mode: FeeAssetMode::Quote,
            fee_asset: Some("USDT".to_string()),
            notional: 10.0,
            raw_fee_bps: 10.0,
            effective_fee_bps: 10.0,
            fee_amount: 0.01,
            source: FeeSource::Fallback,
            platform_discount_applied: false,
        }
    }

    fn test_order_request() -> OrderRequest {
        OrderRequest {
            market_type: MarketType::Spot,
            symbol: "WLD_USDT".to_string(),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: 2.0,
            price: Some(5.0),
            client_order_id: Some("client-a".to_string()),
            reduce_only: false,
        }
    }

    fn test_plan() -> LiveDryRunOrderPlan {
        LiveDryRunOrderPlan {
            plan_id: "plan-a".to_string(),
            timestamp: Utc.with_ymd_and_hms(2026, 6, 6, 2, 0, 0).unwrap(),
            intent: "arbitrage".to_string(),
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            symbol: "WLDUSDT".to_string(),
            exchange_symbol: "WLD_USDT".to_string(),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            price: Some(5.0),
            quantity: 2.0,
            notional: 10.0,
            client_order_id: Some("client-a".to_string()),
            fee_estimate: test_fee_calculation(),
            required_balance_asset: "USDT".to_string(),
            required_balance_amount: 10.0,
            symbol_rule_snapshot: test_symbol_rule(),
            validation_result: LiveDryRunValidationResult {
                passed: true,
                checks: vec![LiveDryRunCheck {
                    name: "test".to_string(),
                    passed: true,
                    message: "ok".to_string(),
                }],
            },
            would_submit: true,
            rejection_reason: None,
            order_request: test_order_request(),
        }
    }

    fn test_open_order() -> OrderResponse {
        OrderResponse {
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            symbol: "WLDUSDT".to_string(),
            order_id: "order-a".to_string(),
            client_order_id: Some("client-a".to_string()),
            side: OrderSide::Buy,
            position_side: PositionSide::None,
            order_type: OrderType::Limit,
            status: OrderStatus::New,
            price: Some(5.0),
            quantity: 3.0,
            filled_quantity: 1.0,
            average_price: None,
            created_at: Utc.with_ymd_and_hms(2026, 6, 6, 2, 1, 0).unwrap(),
            updated_at: Some(Utc.with_ymd_and_hms(2026, 6, 6, 2, 2, 0).unwrap()),
        }
    }

    #[test]
    fn strategy_order_rows_use_exchange_open_order_snapshot_even_when_empty() {
        let mut model = DashboardReadModel::default();
        model.live_dry_run_orders.push(test_plan());
        model.open_orders_last_updated_at =
            Some(Utc.with_ymd_and_hms(2026, 6, 6, 2, 3, 0).unwrap());
        assert!(strategy_order_rows(&model, None).is_empty());

        model.open_orders.push(test_open_order());
        let rows = strategy_order_rows(&model, None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["source"], "exchange_open_order");
        assert_eq!(rows[0]["order_id"], "order-a");
        assert_eq!(rows[0]["remaining_quantity"], json!(2.0));
        assert_eq!(rows[0]["notional"], json!(10.0));
    }

    #[test]
    fn strategy_order_rows_fall_back_to_live_dry_run_for_old_snapshots() {
        let mut model = DashboardReadModel::default();
        model.live_dry_run_orders.push(test_plan());
        let rows = strategy_order_rows(&model, None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["source"], "live_dry_run_plan");
        assert_eq!(rows[0]["status"], "planned");
    }

    #[test]
    fn active_control_symbols_count_as_initial_entry_and_slots() {
        let config = ConfigSummaryView {
            max_enabled_arbitrage_symbols: Some(2),
            initial_entry_notional_usdt: Some(3.2),
            ..ConfigSummaryView::default()
        };
        let active = BTreeSet::from(["PEPEUSDT".to_string(), "WLDUSDT".to_string()]);

        let initial = initial_entry_status_view(&[], &active, &config);
        assert_eq!(initial["completed"], json!(2));
        assert_eq!(initial["pending"], json!(0));
        assert_eq!(initial["observed"], json!(2));

        let slots = arbitrage_slot_status_view(&[], &[], &active, &config);
        assert_eq!(slots["used"], json!(2));
        assert_eq!(slots["remaining"], json!(0));
        assert_eq!(slots["active_symbols"], json!(["PEPEUSDT", "WLDUSDT"]));
    }

    #[test]
    fn disable_exchange_scope_includes_exchanges_with_symbol_balance() {
        let mut model = DashboardReadModel::default();
        model.inventory.push(InventoryView {
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            asset: "WLD".to_string(),
            total: 6.0,
            available: 6.0,
            locked_by_exchange: 0.0,
            locally_reserved: 0.0,
            effective_available: 6.0,
            unmanaged_quantity: 0.0,
            valuation_usdt: Some(2.4),
        });
        model.inventory.push(InventoryView {
            exchange: "bitget".to_string(),
            market_type: MarketType::Spot,
            asset: "WLD".to_string(),
            total: 0.0,
            available: 0.0,
            locked_by_exchange: 2.0,
            locally_reserved: 0.0,
            effective_available: 0.0,
            unmanaged_quantity: 0.0,
            valuation_usdt: Some(0.8),
        });
        model.inventory.push(InventoryView {
            exchange: "gateio".to_string(),
            market_type: MarketType::Spot,
            asset: "USDT".to_string(),
            total: 100.0,
            available: 100.0,
            locked_by_exchange: 0.0,
            locally_reserved: 0.0,
            effective_available: 100.0,
            unmanaged_quantity: 0.0,
            valuation_usdt: Some(100.0),
        });

        let exchanges = inventory_exchanges_with_symbol_balance(&model, "WLDUSDT");
        assert_eq!(exchanges, vec!["bitget".to_string(), "gateio".to_string()]);
    }

    #[test]
    fn strategy_profit_points_accumulate_trade_net_pnl() {
        let first = TradeView {
            timestamp: Utc.with_ymd_and_hms(2026, 6, 6, 2, 4, 0).unwrap(),
            trade_id: "trade-a".to_string(),
            symbol: "WLDUSDT".to_string(),
            buy_exchange: "gateio".to_string(),
            sell_exchange: "bitget".to_string(),
            quantity: 1.0,
            buy_avg_price: 2.0,
            sell_avg_price: 2.1,
            gross_pnl: 0.1,
            total_fee: 0.02,
            net_pnl: 0.08,
            execution_mode: "arbitrage".to_string(),
            paper_or_live: "paper".to_string(),
        };
        let second = TradeView {
            trade_id: "trade-b".to_string(),
            net_pnl: -0.03,
            ..first.clone()
        };

        let first_id = strategy_profit_trade_id(&first);
        let first_point = strategy_profit_history_point(&first, &first_id, first.net_pnl);
        let second_id = strategy_profit_trade_id(&second);
        let second_point = strategy_profit_history_point(&second, &second_id, 0.05);

        assert_eq!(first_point["trade_id"], "trade-a");
        assert_eq!(first_point["profit_usdt"], json!(0.08));
        assert_eq!(first_point["trade_profit_usdt"], json!(0.08));
        assert_eq!(second_point["trade_id"], "trade-b");
        assert_eq!(second_point["profit_usdt"], json!(0.05));
        assert_eq!(second_point["series"], "strategy_profit");
    }

    #[test]
    fn cross_arb_results_accumulate_closed_hedge_record_profit() {
        let records = vec![
            json!({
                "record_id": "record-a",
                "bundle_id": "bundle-a",
                "canonical_symbol": "EDGE/USDT",
                "long_exchange": "binance",
                "short_exchange": "gate",
                "status": "Closed",
                "is_closed": true,
                "target_notional_usdt": 5.0,
                "close_net_profit_pct": 0.0006,
                "updated_at": "2026-06-06T02:00:00Z"
            }),
            json!({
                "record_id": "record-b-old",
                "bundle_id": "bundle-b",
                "canonical_symbol": "SPCX/USDT",
                "long_exchange": "bitget",
                "short_exchange": "gate",
                "status": "Closed",
                "is_closed": true,
                "target_notional_usdt": 5.0,
                "realized_profit_usdt": 0.01,
                "close_net_profit_pct": 0.002,
                "updated_at": "2026-06-06T02:00:30Z"
            }),
            json!({
                "record_id": "record-b",
                "bundle_id": "bundle-b",
                "canonical_symbol": "SPCX/USDT",
                "long_exchange": "bitget",
                "short_exchange": "gate",
                "status": "Closed",
                "is_closed": true,
                "target_notional_usdt": 5.0,
                "realized_profit_usdt": 0.02,
                "close_net_profit_pct": 0.004,
                "updated_at": "2026-06-06T02:01:00Z"
            }),
        ];

        let results = cross_arb_arbitrage_results(&records, &[]);
        let summary = cross_arb_profit_summary(&results);

        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["bundle_id"], "bundle-b");
        assert_eq!(results[0]["record_id"], "record-b");
        assert_eq!(results[0]["realized_profit_usdt"], json!(0.02));
        assert!((results[0]["cumulative_profit_usdt"].as_f64().unwrap() - 0.023).abs() < 1e-12);
        assert_eq!(results[1]["bundle_id"], "bundle-a");
        assert!((results[1]["realized_profit_usdt"].as_f64().unwrap() - 0.003).abs() < 1e-12);
        assert!((results[1]["cumulative_profit_usdt"].as_f64().unwrap() - 0.003).abs() < 1e-12);
        assert_eq!(summary["closed_arbitrages"], json!(2));
        assert_eq!(summary["wins"], json!(2));
        assert!((summary["realized_profit_usdt"].as_f64().unwrap() - 0.023).abs() < 1e-12);
    }

    #[test]
    fn cross_arb_results_should_ignore_open_records_and_inactive_exchanges() {
        let enabled_exchanges = vec![
            "binance".to_string(),
            "bitget".to_string(),
            "gate".to_string(),
        ];
        let records = vec![
            json!({
                "record_id": "open-binance-okx",
                "bundle_id": "open-binance-okx",
                "canonical_symbol": "BTC/USDT",
                "long_exchange": "binance",
                "short_exchange": "okx",
                "status": "hedging",
                "is_closed": false,
                "close_net_profit_pct": null,
                "updated_at": "2026-06-06T02:00:00Z"
            }),
            json!({
                "record_id": "closed-binance-okx",
                "bundle_id": "closed-binance-okx",
                "canonical_symbol": "BTC/USDT",
                "long_exchange": "binance",
                "short_exchange": "okx",
                "status": "Closed",
                "is_closed": true,
                "close_net_profit_pct": 0.001,
                "updated_at": "2026-06-06T02:01:00Z"
            }),
            json!({
                "record_id": "closed-binance-gate",
                "bundle_id": "closed-binance-gate",
                "canonical_symbol": "EDGE/USDT",
                "long_exchange": "binance",
                "short_exchange": "gate",
                "status": "Closed",
                "is_closed": true,
                "close_net_profit_pct": 0.001,
                "updated_at": "2026-06-06T02:02:00Z"
            }),
        ];

        let positions = cross_arb_position_bundles(&records, &[], &[], &enabled_exchanges);
        let results = cross_arb_arbitrage_results(&records, &enabled_exchanges);

        assert!(positions.is_empty());
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["bundle_id"], "closed-binance-gate");
    }

    #[test]
    fn cross_arb_position_bundles_should_merge_live_close_metrics() {
        let enabled_exchanges = vec!["binance".to_string(), "gate".to_string()];
        let records = vec![json!({
            "record_id": "record-a",
            "bundle_id": "bundle-a",
            "canonical_symbol": "EDGE/USDT",
            "long_exchange": "binance",
            "short_exchange": "gate",
            "status": "hedged",
            "is_closed": false,
            "entry_net_edge_pct": 0.0012,
            "updated_at": "2026-06-06T02:00:00Z"
        })];
        let close_metrics = vec![json!({
            "kind": "CloseMetric",
            "bundle_id": "bundle-a",
            "symbol": "EDGE/USDT",
            "long_exchange": "binance",
            "short_exchange": "gate",
            "close_profit_pct": 0.0007,
            "close_spread_pct": -0.0001,
            "close_threshold_pct": 0.0005,
            "close_maker_exchange": "gate",
            "close_maker_side": "Buy",
            "close_maker_price": 1.23,
            "closeable": true,
            "updated_at": "2026-06-06T02:00:05Z"
        })];

        let positions =
            cross_arb_position_bundles(&records, &[], &close_metrics, &enabled_exchanges);

        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0]["close_candidate_profit_pct"], json!(0.0007));
        assert_eq!(positions[0]["close_threshold_pct"], json!(0.0005));
        assert_eq!(positions[0]["closeable"], json!(true));
        assert_eq!(positions[0]["close_maker_exchange"], "gate");
        assert_eq!(positions[0]["updated_at"], "2026-06-06T02:00:05Z");
    }

    #[test]
    fn cross_arb_dashboard_scope_should_filter_events_to_selected_venues_and_symbols() {
        let summary = CrossArbEventSummary {
            opportunities: vec![
                json!({
                    "kind": "Opportunity",
                    "opportunity_id": "opp-ark",
                    "canonical_symbol": "ARK/USDT",
                    "long_exchange": "binance",
                    "short_exchange": "bitget",
                    "can_open": true
                }),
                json!({
                    "kind": "Opportunity",
                    "opportunity_id": "opp-btc",
                    "canonical_symbol": "BTC/USDT",
                    "long_exchange": "binance",
                    "short_exchange": "okx",
                    "can_open": true
                }),
            ],
            signals: vec![
                json!({
                    "kind": "Signal",
                    "signal_id": "signal-ark",
                    "opportunity_id": "opp-ark",
                    "action": "Open"
                }),
                json!({
                    "kind": "Signal",
                    "signal_id": "signal-btc",
                    "opportunity_id": "opp-btc",
                    "action": "Open"
                }),
            ],
            market_snapshots: vec![
                json!({
                    "kind": "MarketSnapshot",
                    "exchange": "gate",
                    "symbol": "ARK/USDT",
                    "best_bid": 1.0,
                    "best_ask": 1.01
                }),
                json!({
                    "kind": "MarketSnapshot",
                    "exchange": "okx",
                    "symbol": "ARK/USDT",
                    "best_bid": 1.0,
                    "best_ask": 1.01
                }),
                json!({
                    "kind": "MarketSnapshot",
                    "exchange": "gate",
                    "symbol": "BTC/USDT",
                    "best_bid": 1.0,
                    "best_ask": 1.01
                }),
            ],
            instruments: vec![
                json!({
                    "kind": "Instrument",
                    "exchange": "gate",
                    "canonical_symbol": "ARK/USDT"
                }),
                json!({
                    "kind": "Instrument",
                    "exchange": "gate",
                    "canonical_symbol": "BTC/USDT"
                }),
            ],
            close_metrics: vec![
                json!({
                    "kind": "CloseMetric",
                    "bundle_id": "bundle-ark",
                    "symbol": "ARK/USDT",
                    "long_exchange": "binance",
                    "short_exchange": "gate",
                    "close_profit_pct": 0.0006
                }),
                json!({
                    "kind": "CloseMetric",
                    "bundle_id": "bundle-btc",
                    "symbol": "BTC/USDT",
                    "long_exchange": "binance",
                    "short_exchange": "gate",
                    "close_profit_pct": 0.0007
                }),
            ],
            events: vec![
                json!({
                    "kind": "Opportunity",
                    "opportunity_id": "opp-ark",
                    "canonical_symbol": "ARK/USDT",
                    "long_exchange": "binance",
                    "short_exchange": "bitget"
                }),
                json!({
                    "kind": "Signal",
                    "signal_id": "signal-btc",
                    "opportunity_id": "opp-btc"
                }),
            ],
            ..Default::default()
        };
        let enabled_exchanges = vec![
            "binance".to_string(),
            "bitget".to_string(),
            "gate".to_string(),
        ];
        let enabled_symbols = vec!["ARK/USDT".to_string()];

        let filtered =
            filter_cross_arb_dashboard_scope(summary, &enabled_exchanges, &enabled_symbols);

        assert_eq!(filtered.opportunities.len(), 1);
        assert_eq!(filtered.opportunities[0]["opportunity_id"], "opp-ark");
        assert_eq!(filtered.signals.len(), 1);
        assert_eq!(filtered.signals[0]["signal_id"], "signal-ark");
        assert_eq!(filtered.market_snapshots.len(), 1);
        assert_eq!(filtered.market_snapshots[0]["exchange"], "gate");
        assert_eq!(filtered.instruments.len(), 1);
        assert_eq!(filtered.instruments[0]["canonical_symbol"], "ARK/USDT");
        assert_eq!(filtered.close_metrics.len(), 1);
        assert_eq!(filtered.close_metrics[0]["bundle_id"], "bundle-ark");
        assert_eq!(filtered.events.len(), 1);
        assert_eq!(filtered.events[0]["opportunity_id"], "opp-ark");
    }

    #[test]
    fn cross_arb_profit_history_should_feed_strategy_profit_curve() {
        let results = vec![
            json!({
                "bundle_id": "bundle-b",
                "symbol": "SPCX/USDT",
                "long_exchange": "bitget",
                "short_exchange": "gate",
                "realized_profit_usdt": 0.02,
                "cumulative_profit_usdt": 0.023,
                "updated_at": "2026-06-06T02:01:00Z"
            }),
            json!({
                "bundle_id": "bundle-a",
                "symbol": "EDGE/USDT",
                "long_exchange": "binance",
                "short_exchange": "gate",
                "realized_profit_usdt": 0.003,
                "cumulative_profit_usdt": 0.003,
                "updated_at": "2026-06-06T02:00:00Z"
            }),
        ];

        let history = cross_arb_profit_history_from_results(&results);
        let rows = history.as_array().unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["series"], "cross_arb_strategy_profit");
        assert_eq!(rows[0]["trade_id"], "bundle-a");
        assert_eq!(rows[0]["trade_profit_usdt"], json!(0.003));
        assert_eq!(rows[0]["profit_usdt"], json!(0.003));
        assert_eq!(rows[1]["trade_id"], "bundle-b");
        assert_eq!(rows[1]["trade_profit_usdt"], json!(0.02));
        assert_eq!(rows[1]["profit_usdt"], json!(0.023));
        assert_eq!(rows[1]["total_usdt"], json!(0.023));
    }

    #[test]
    fn cross_arb_open_orders_should_include_nonterminal_order_events() {
        let order_events = vec![
            json!({
                "kind": "OrderTransition",
                "exchange": "gate",
                "canonical_symbol": "EDGE/USDT",
                "side": "Buy",
                "status": "New",
                "order_id": "order-a",
                "client_order_id": "crossarb-open-a",
                "price": 0.5,
                "quantity": 10.0,
                "filled_quantity": 2.0,
                "timestamp": "2026-06-06T02:00:00Z",
                "recorded_at": "2026-06-06T02:00:01Z"
            }),
            json!({
                "kind": "OrderTransition",
                "exchange": "gate",
                "canonical_symbol": "EDGE/USDT",
                "side": "Buy",
                "status": "Filled",
                "order_id": "order-b",
                "client_order_id": "crossarb-open-b",
                "price": 0.5,
                "quantity": 10.0,
                "filled_quantity": 10.0,
                "timestamp": "2026-06-06T02:00:02Z"
            }),
        ];

        let rows = cross_arb_open_orders(&DashboardReadModel::default(), &[], &order_events);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["source"], "order_event");
        assert_eq!(rows[0]["exchange"], "gate");
        assert_eq!(rows[0]["symbol"], "EDGE/USDT");
        assert_eq!(rows[0]["remaining_qty"], json!(8.0));
        assert_eq!(rows[0]["notional"], json!(4.0));
        assert_eq!(rows[0]["client_order_id"], "crossarb-open-a");
    }

    #[test]
    fn cross_arb_account_console_should_expose_configured_accounts() {
        let settings = json!({
            "exchanges": [
                {
                    "exchange": "binance",
                    "enabled": true,
                    "account_id": "acct_a",
                    "env_prefix": "BINANCE_ACCTA",
                    "private_rest_enabled": true,
                    "private_ws_enabled": true
                },
                {
                    "exchange": "gate",
                    "enabled": false,
                    "account_id": "acct_b",
                    "env_prefix": "GATE_ACCTB",
                    "private_rest_enabled": true,
                    "private_ws_enabled": false
                },
                {
                    "exchange": "bitget",
                    "enabled": true,
                    "account_id": "",
                    "env_prefix": "",
                    "private_rest_enabled": false,
                    "private_ws_enabled": true
                }
            ]
        });
        let private_events = vec![json!({
            "private_kind": "Balance",
            "exchange": "binance",
            "account_id": "acct_a",
            "total": 21.0,
            "available": 20.5,
            "recorded_at": "2026-06-06T02:00:00Z"
        })];
        let api_key_values = BTreeMap::from([
            (
                "BINANCE_ACCTA_API_KEY".to_string(),
                "binance-key".to_string(),
            ),
            (
                "BINANCE_ACCTA_API_SECRET".to_string(),
                "binance-secret".to_string(),
            ),
        ]);

        let rows = cross_arb_account_console(&settings, &private_events, &api_key_values);

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["exchange"], "binance");
        assert_eq!(rows[0]["account_id"], "acct_a");
        assert_eq!(rows[0]["env_prefix"], "BINANCE_ACCTA");
        assert_eq!(rows[0]["credential_namespace"], "BINANCE_ACCTA");
        assert_eq!(rows[0]["status"], "online");
        assert_eq!(rows[0]["total"], json!(21.0));
        assert_eq!(rows[0]["credentials"]["configured"], json!(2));
        assert_eq!(rows[0]["credentials"]["required"], json!(2));
        assert_eq!(
            rows[0]["credentials"]["configured_all_required"],
            json!(true)
        );
        assert_eq!(rows[1]["exchange"], "bitget");
        assert_eq!(rows[1]["account_id"], "default");
        assert_eq!(rows[1]["credential_namespace"], "BITGET");
        assert_eq!(rows[1]["status"], "configured");
        assert_eq!(rows[1]["private_rest_enabled"], json!(false));
        assert_eq!(rows[1]["private_ws_enabled"], json!(true));
        assert_eq!(rows[1]["credentials"]["configured"], json!(0));
        assert_eq!(rows[1]["credentials"]["required"], json!(3));
        assert_eq!(
            rows[1]["credentials"]["configured_all_required"],
            json!(false)
        );
        assert_eq!(
            rows[1]["credentials"]["missing_required"],
            json!([
                {
                    "code": "key",
                    "display": "API Key",
                    "expected_env_parts": [["BITGET", "API", "KEY"]]
                },
                {
                    "code": "credential",
                    "display": "API Credential",
                    "expected_env_parts": [["BITGET", "API", "SECRET"]]
                },
                {
                    "code": "pass_phrase",
                    "display": "Pass phrase",
                    "expected_env_parts": [["BITGET", "PASSPHRASE"], ["BITGET", "API", "PASSPHRASE"]]
                }
            ])
        );
    }

    #[test]
    fn cross_arb_credential_namespace_should_follow_selected_account_when_prefix_is_default() {
        assert_eq!(
            cross_arb_credential_namespace("binance", "hedge", "BINANCE"),
            "BINANCE__HEDGE_"
        );
        assert_eq!(
            cross_arb_credential_namespace("gate", "acct-a", ""),
            "GATE__ACCT_A_"
        );
        assert_eq!(
            cross_arb_credential_namespace("bitget", "default", "BITGET"),
            "BITGET"
        );
        assert_eq!(
            cross_arb_credential_namespace("bitget", "hedge", "CUSTOM_BITGET"),
            "CUSTOM_BITGET"
        );
    }

    #[test]
    fn cross_arb_account_console_should_check_credentials_from_runtime_namespace() {
        let settings = json!({
            "exchanges": [
                {
                    "exchange": "bitget",
                    "enabled": true,
                    "account_id": "hedge",
                    "env_prefix": "CUSTOM_BITGET",
                    "private_rest_enabled": true,
                    "private_ws_enabled": true
                }
            ]
        });
        let account_scoped_values = BTreeMap::from([
            ("BITGET__HEDGE__API_KEY".to_string(), "key".to_string()),
            (
                "BITGET__HEDGE__API_SECRET".to_string(),
                "secret".to_string(),
            ),
            ("BITGET__HEDGE__PASSPHRASE".to_string(), "pass".to_string()),
        ]);

        let rows = cross_arb_account_console(&settings, &[], &account_scoped_values);

        assert_eq!(rows[0]["credential_namespace"], "CUSTOM_BITGET");
        assert_eq!(rows[0]["credentials"]["configured"], json!(0));
        assert_eq!(
            rows[0]["credentials"]["missing_required"][0]["expected_env_parts"][0],
            json!(["CUSTOM", "BITGET", "API", "KEY"])
        );

        let custom_prefix_values = BTreeMap::from([
            ("CUSTOM_BITGET_API_KEY".to_string(), "key".to_string()),
            ("CUSTOM_BITGET_API_SECRET".to_string(), "secret".to_string()),
            ("CUSTOM_BITGET_PASSPHRASE".to_string(), "pass".to_string()),
        ]);
        let rows = cross_arb_account_console(&settings, &[], &custom_prefix_values);

        assert_eq!(rows[0]["credentials"]["configured"], json!(3));
        assert_eq!(
            rows[0]["credentials"]["configured_all_required"],
            json!(true)
        );
    }

    #[test]
    fn cross_arb_account_console_should_accept_custom_default_prefix_and_config_account_id() {
        let settings = json!({
            "exchanges": [
                {
                    "exchange": "binance",
                    "enabled": true,
                    "account_id": "",
                    "env_prefix": "BINANCE_0",
                    "private_rest_enabled": true,
                    "private_ws_enabled": true
                },
                {
                    "exchange": "gate",
                    "enabled": true,
                    "account_id": "16076371",
                    "env_prefix": "GATE",
                    "private_rest_enabled": true,
                    "private_ws_enabled": true
                }
            ]
        });
        let api_key_values = BTreeMap::from([
            ("BINANCE_0_API_KEY".to_string(), "binance-key".to_string()),
            (
                "BINANCE_0_API_SECRET".to_string(),
                "binance-secret".to_string(),
            ),
            (
                "GATE__16076371__API_KEY".to_string(),
                "gate-key".to_string(),
            ),
            (
                "GATE__16076371__API_SECRET".to_string(),
                "gate-secret".to_string(),
            ),
        ]);

        let rows = cross_arb_account_console(&settings, &[], &api_key_values);

        assert_eq!(rows[0]["credential_namespace"], "BINANCE_0");
        assert_eq!(rows[0]["credentials"]["configured"], json!(2));
        assert_eq!(
            rows[0]["credentials"]["configured_all_required"],
            json!(true)
        );
        assert_eq!(rows[1]["credential_namespace"], "GATE__16076371_");
        assert_eq!(rows[1]["credentials"]["configured"], json!(3));
        assert_eq!(
            rows[1]["credentials"]["configured_all_required"],
            json!(true)
        );
    }

    #[test]
    fn cross_arb_account_readiness_should_summarize_missing_credentials() {
        let account_console = vec![
            json!({
                "exchange": "binance",
                "account_id": "acct_a",
                "credentials": {
                    "supported": true,
                    "configured": 2,
                    "required": 2,
                    "configured_all_required": true
                }
            }),
            json!({
                "exchange": "bitget",
                "account_id": "default",
                "credentials": {
                    "supported": true,
                    "configured": 1,
                    "required": 3,
                    "configured_all_required": false,
                    "missing_required": [
                        {
                            "code": "credential",
                            "display": "API Credential",
                            "expected_env_parts": [["BITGET", "API", "SECRET"]]
                        }
                    ]
                }
            }),
        ];

        let readiness = cross_arb_account_readiness(&account_console);

        assert_eq!(readiness["enabled_accounts"], json!(2));
        assert_eq!(readiness["ready_accounts"], json!(1));
        assert_eq!(readiness["missing_accounts"], json!(1));
        assert_eq!(
            readiness["all_required_credentials_configured"],
            json!(false)
        );
        assert_eq!(readiness["missing"][0]["exchange"], "bitget");
        assert_eq!(readiness["missing"][0]["account_id"], "default");
        assert_eq!(readiness["missing"][0]["configured"], json!(1));
        assert_eq!(readiness["missing"][0]["required"], json!(3));
        assert_eq!(
            readiness["missing"][0]["missing_required"][0]["expected_env_parts"][0],
            json!(["BITGET", "API", "SECRET"])
        );
    }

    #[test]
    fn cross_arb_strategy_readiness_should_block_on_missing_credentials() {
        let enabled_exchanges = vec![
            "binance".to_string(),
            "bitget".to_string(),
            "gate".to_string(),
        ];
        let enabled_symbols = (0..200)
            .map(|index| format!("SYM{index}/USDT"))
            .collect::<Vec<_>>();
        let mut events = CrossArbEventSummary::default();
        for exchange in &enabled_exchanges {
            for symbol in &enabled_symbols {
                events.instruments.push(json!({
                    "exchange": exchange,
                    "canonical_symbol": symbol
                }));
                events.market_snapshots.push(json!({
                    "exchange": exchange,
                    "symbol": symbol,
                    "best_bid": 1.0,
                    "best_ask": 1.01
                }));
            }
        }
        let account_readiness = json!({
            "all_required_credentials_configured": false,
            "enabled_accounts": 3,
            "ready_accounts": 2,
            "missing_accounts": 1
        });

        let readiness = cross_arb_strategy_readiness(
            &enabled_exchanges,
            &enabled_symbols,
            &events,
            &account_readiness,
        );

        assert_eq!(readiness["expected_exchange_symbol_pairs"], json!(600));
        assert_eq!(readiness["instrument_pairs"], json!(600));
        assert_eq!(readiness["market_snapshot_pairs"], json!(600));
        assert_eq!(readiness["instrument_coverage_ok"], json!(true));
        assert_eq!(readiness["market_snapshot_coverage_ok"], json!(true));
        assert_eq!(readiness["live_small_ready"], json!(false));
        assert_eq!(readiness["blockers"], json!(["credentials"]));
    }

    #[test]
    fn cross_arb_strategy_readiness_should_report_missing_pair_samples() {
        let enabled_exchanges = vec!["binance".to_string(), "gate".to_string()];
        let enabled_symbols = (0..200)
            .map(|index| format!("SYM{index}/USDT"))
            .collect::<Vec<_>>();
        let mut events = CrossArbEventSummary::default();
        for exchange in &enabled_exchanges {
            for symbol in enabled_symbols.iter().skip(1) {
                events.instruments.push(json!({
                    "exchange": exchange,
                    "canonical_symbol": symbol
                }));
                events.market_snapshots.push(json!({
                    "exchange": exchange,
                    "symbol": symbol,
                    "best_bid": 1.0,
                    "best_ask": 1.01
                }));
            }
        }
        let account_readiness = json!({
            "all_required_credentials_configured": true,
            "enabled_accounts": 2,
            "ready_accounts": 2,
            "missing_accounts": 0
        });

        let readiness = cross_arb_strategy_readiness(
            &enabled_exchanges,
            &enabled_symbols,
            &events,
            &account_readiness,
        );

        assert_eq!(readiness["expected_exchange_symbol_pairs"], json!(400));
        assert_eq!(readiness["instrument_pairs"], json!(398));
        assert_eq!(readiness["missing_instrument_pairs"], json!(2));
        assert_eq!(readiness["missing_market_snapshot_pairs"], json!(2));
        assert_eq!(readiness["instrument_coverage_ok"], json!(false));
        assert_eq!(readiness["market_snapshot_coverage_ok"], json!(false));
        assert_eq!(
            readiness["blockers"],
            json!(["instrument_coverage", "market_snapshot_coverage"])
        );
        assert_eq!(
            readiness["missing_instrument_pair_samples"][0],
            json!({
                "exchange": "binance",
                "symbol": "SYM0USDT"
            })
        );
    }

    #[test]
    fn cross_arb_strategy_readiness_should_not_count_unselected_pairs_as_coverage() {
        let enabled_exchanges = vec![
            "binance".to_string(),
            "bitget".to_string(),
            "gate".to_string(),
        ];
        let enabled_symbols = (0..200)
            .map(|index| format!("SYM{index}/USDT"))
            .collect::<Vec<_>>();
        let mut events = CrossArbEventSummary::default();
        for exchange in &enabled_exchanges {
            for symbol in &enabled_symbols {
                if exchange == "binance" && symbol == "SYM0/USDT" {
                    continue;
                }
                events.instruments.push(json!({
                    "exchange": exchange,
                    "canonical_symbol": symbol
                }));
                events.market_snapshots.push(json!({
                    "exchange": exchange,
                    "symbol": symbol,
                    "best_bid": 1.0,
                    "best_ask": 1.01
                }));
            }
        }
        events.instruments.push(json!({
            "exchange": "binance",
            "canonical_symbol": "UNSELECTED/USDT"
        }));
        events.market_snapshots.push(json!({
            "exchange": "binance",
            "symbol": "UNSELECTED/USDT",
            "best_bid": 1.0,
            "best_ask": 1.01
        }));
        let account_readiness = json!({
            "all_required_credentials_configured": true,
            "enabled_accounts": 3,
            "ready_accounts": 3,
            "missing_accounts": 0
        });

        let readiness = cross_arb_strategy_readiness(
            &enabled_exchanges,
            &enabled_symbols,
            &events,
            &account_readiness,
        );

        assert_eq!(readiness["expected_exchange_symbol_pairs"], json!(600));
        assert_eq!(readiness["instrument_pairs"], json!(600));
        assert_eq!(readiness["covered_instrument_pairs"], json!(599));
        assert_eq!(readiness["covered_market_snapshot_pairs"], json!(599));
        assert_eq!(readiness["missing_instrument_pairs"], json!(1));
        assert_eq!(readiness["missing_market_snapshot_pairs"], json!(1));
        assert_eq!(readiness["instrument_coverage_ok"], json!(false));
        assert_eq!(readiness["market_snapshot_coverage_ok"], json!(false));
        assert_eq!(readiness["live_small_ready"], json!(false));
        assert_eq!(
            readiness["missing_instrument_pair_samples"][0],
            json!({
                "exchange": "binance",
                "symbol": "SYM0USDT"
            })
        );
    }

    #[test]
    fn cross_arb_strategy_readiness_should_pass_with_full_coverage_and_credentials() {
        let enabled_exchanges = vec![
            "binance".to_string(),
            "bitget".to_string(),
            "gate".to_string(),
        ];
        let enabled_symbols = (0..200)
            .map(|index| format!("SYM{index}/USDT"))
            .collect::<Vec<_>>();
        let mut events = CrossArbEventSummary::default();
        for exchange in &enabled_exchanges {
            for symbol in &enabled_symbols {
                events.instruments.push(json!({
                    "exchange": exchange,
                    "canonical_symbol": symbol
                }));
                events.market_snapshots.push(json!({
                    "exchange": exchange,
                    "symbol": symbol,
                    "best_bid": 1.0,
                    "best_ask": 1.01
                }));
            }
        }
        let account_readiness = json!({
            "all_required_credentials_configured": true,
            "enabled_accounts": 3,
            "ready_accounts": 3,
            "missing_accounts": 0
        });

        let readiness = cross_arb_strategy_readiness(
            &enabled_exchanges,
            &enabled_symbols,
            &events,
            &account_readiness,
        );

        assert_eq!(readiness["live_small_ready"], json!(true));
        assert_eq!(readiness["blockers"], json!([]));
    }

    #[test]
    fn cross_arb_latest_event_should_mark_live_only_when_fresh() {
        let now = Utc.with_ymd_and_hms(2026, 6, 6, 10, 0, 0).unwrap();
        let fresh = CrossArbEventSummary {
            latest_event_at: Some(
                Utc.with_ymd_and_hms(2026, 6, 6, 9, 59, 45)
                    .unwrap()
                    .to_rfc3339(),
            ),
            ..CrossArbEventSummary::default()
        };
        let stale = CrossArbEventSummary {
            latest_event_at: Some(
                Utc.with_ymd_and_hms(2026, 6, 6, 9, 58, 59)
                    .unwrap()
                    .to_rfc3339(),
            ),
            ..CrossArbEventSummary::default()
        };

        assert!(cross_arb_latest_event_is_fresh(&fresh, now));
        assert!(!cross_arb_latest_event_is_fresh(&stale, now));
        assert!(!cross_arb_latest_event_is_fresh(
            &CrossArbEventSummary::default(),
            now
        ));
    }

    #[tokio::test]
    async fn read_cross_arb_events_should_use_requested_jsonl_dir() {
        let dir = tempdir().unwrap();
        let event_dir = dir.path().join("configured-cross-arb-events");
        std::fs::create_dir_all(&event_dir).unwrap();
        let event_path = event_dir.join("cross_arb_events_20260606.jsonl");
        let stored = json!({
            "event_id": 1,
            "recorded_at": "2026-06-06T02:00:00Z",
            "event": {
                "OrderTransition": {
                    "timestamp": "2026-06-06T02:00:00Z",
                    "exchange": "gate",
                    "symbol": "EDGE/USDT",
                    "side": "Buy",
                    "order_type": "Limit",
                    "status": "New",
                    "order_id": "order-a",
                    "client_order_id": "crossarb-order-a",
                    "price": 0.5,
                    "quantity": 10.0,
                    "filled_quantity": 0.0,
                    "average_fill_price": null,
                    "latency_ms": null
                }
            }
        });
        std::fs::write(&event_path, format!("{stored}\n")).unwrap();

        let summary = read_cross_arb_events(&event_dir).await.unwrap();

        assert_eq!(summary.total_valid_events, 1);
        assert_eq!(summary.event_dir, event_dir.to_string_lossy());
        assert_eq!(
            summary.files,
            vec![event_path.to_string_lossy().to_string()]
        );
        assert_eq!(summary.order_events.len(), 1);
        assert_eq!(
            summary.order_events[0]["client_order_id"],
            "crossarb-order-a"
        );
    }

    #[tokio::test]
    async fn funding_arb_settings_update_should_save_live_parameters_and_account_prefix() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("funding_live.yml");
        let accounts_path = dir.path().join("accounts.yml");
        let mut config = FundingRateArbitrageConfig::default();
        config.mode = "live".to_string();
        config.universe.enabled_exchanges = vec![ExchangeId::Binance, ExchangeId::Bitget];
        config.selection.max_seconds_to_settlement_at_scan = Some(600);
        config.execution.notional_usdt = 6.0;
        std::fs::write(&config_path, serde_yaml::to_string(&config).unwrap()).unwrap();
        std::fs::write(
            &accounts_path,
            r#"
accounts:
  binance_hcr:
    name: binance_hcr
    exchange: binance
    type: futures
    env_prefix: BINANCE_3
    enabled: true
"#,
        )
        .unwrap();
        let mut state = test_app_state(dir.path(), dir.path().join("unused.yml"));
        state.accounts_config = accounts_path;
        register_funding_strategy(&state, &config_path).await;

        let response = apply_funding_arb_settings_update(
            &state,
            FundingArbSettingsUpdateRequest {
                mode: Some("live".to_string()),
                per_exchange_limit: Some(2),
                min_funding_rate: Some(-0.007),
                max_funding_snapshot_age_ms: Some(7000),
                min_seconds_to_settlement_at_scan: Some(3),
                max_seconds_to_settlement_at_scan: Some(900),
                notional_usdt: Some(8.5),
                exchanges: vec![
                    FundingArbExchangeSettingsRequest {
                        exchange: "binance".to_string(),
                        enabled: true,
                        account_id: Some("binance_hcr".to_string()),
                        ..FundingArbExchangeSettingsRequest::default()
                    },
                    FundingArbExchangeSettingsRequest {
                        exchange: "bitget".to_string(),
                        enabled: false,
                        account_id: Some("default".to_string()),
                        env_prefix: Some("BITGET".to_string()),
                        ..FundingArbExchangeSettingsRequest::default()
                    },
                ],
                restart: false,
                ..FundingArbSettingsUpdateRequest::default()
            },
        )
        .await
        .unwrap();

        assert_eq!(response["saved"], json!(true));
        assert_eq!(response["restart"]["skipped"], json!(true));
        let saved: FundingRateArbitrageConfig =
            serde_yaml::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        assert_eq!(saved.selection.per_exchange_limit, 2);
        assert_eq!(saved.selection.min_funding_rate, -0.007);
        assert_eq!(saved.execution.notional_usdt, 8.5);
        assert_eq!(
            saved.exchanges[&ExchangeId::Binance].account_id.as_deref(),
            Some("binance_hcr")
        );
        assert_eq!(
            saved.exchanges[&ExchangeId::Binance].env_prefix.as_deref(),
            Some("BINANCE_3")
        );
        assert_eq!(saved.exchanges[&ExchangeId::Bitget].enabled, Some(false));
    }

    #[tokio::test]
    async fn read_cross_arb_events_should_keep_full_market_snapshot_universe() {
        let dir = tempdir().unwrap();
        let event_dir = dir.path().join("cross-arb-events");
        std::fs::create_dir_all(&event_dir).unwrap();
        let event_path = event_dir.join("cross_arb_events_20260606.jsonl");
        let mut lines = String::new();
        for index in 0..650 {
            let exchange = match index % 3 {
                0 => "binance",
                1 => "bitget",
                _ => "gate",
            };
            let symbol = format!("SYM{}/USDT", index / 3);
            let stored = json!({
                "event_id": index,
                "recorded_at": "2026-06-06T02:00:00Z",
                "event": {
                    "MarketSnapshot": {
                        "exchange": exchange,
                        "symbol": symbol,
                        "best_bid": 1.0,
                        "best_ask": 1.01,
                        "book_age_ms": 5
                    }
                }
            });
            lines.push_str(&format!("{stored}\n"));
        }
        std::fs::write(&event_path, lines).unwrap();

        let summary = read_cross_arb_events(&event_dir).await.unwrap();

        assert_eq!(summary.market_snapshots.len(), 650);
        assert!(summary.market_snapshots.len() > 200 * 3);
    }

    #[tokio::test]
    async fn read_cross_arb_events_should_scan_past_signal_tail_for_market_snapshots() {
        let dir = tempdir().unwrap();
        let event_dir = dir.path().join("cross-arb-events");
        std::fs::create_dir_all(&event_dir).unwrap();
        let event_path = event_dir.join("cross_arb_events_20260606.jsonl");
        let mut lines = String::new();
        for index in 0..600 {
            let exchange = match index % 3 {
                0 => "binance",
                1 => "bitget",
                _ => "gate",
            };
            let symbol = format!("SYM{}/USDT", index / 3);
            let stored = json!({
                "event_id": index,
                "recorded_at": "2026-06-06T02:00:00Z",
                "event": {
                    "MarketSnapshot": {
                        "exchange": exchange,
                        "symbol": symbol,
                        "best_bid": 1.0,
                        "best_ask": 1.01,
                        "book_age_ms": 5
                    }
                }
            });
            lines.push_str(&format!("{stored}\n"));
        }
        for index in 0..2_500 {
            let stored = json!({
                "event_id": 10_000 + index,
                "recorded_at": "2026-06-06T02:01:00Z",
                "event": {
                    "Signal": {
                        "signal_id": format!("signal-{index}"),
                        "mode": "live_small",
                        "action": "Noop",
                        "risk_flags": ["RawSpreadTooSmall"]
                    }
                }
            });
            lines.push_str(&format!("{stored}\n"));
        }
        std::fs::write(&event_path, lines).unwrap();

        let summary = read_cross_arb_events(&event_dir).await.unwrap();

        assert_eq!(summary.market_snapshots.len(), 600);
        assert_eq!(summary.signals.len(), CROSS_ARB_OPPORTUNITY_DISPLAY_LIMIT);
    }

    #[test]
    fn cross_arb_execution_profile_should_only_enable_safe_dry_run_runtime() {
        let mut config = CrossExchangeArbitrageConfig::default();

        apply_cross_arb_execution_profile(&mut config, "live_small_dry_run").unwrap();
        assert_eq!(config.mode, rustcta::market::RuntimeMode::LiveSmall);
        assert_eq!(
            config.strategy.mode,
            Some(rustcta::market::RuntimeMode::LiveSmall)
        );
        assert_eq!(
            config.trading_mode,
            rustcta::strategies::cross_exchange_arbitrage::TradingMode::Paper
        );
        assert!(config.execution.dry_run);
        assert!(!config.enable_live_trading);

        apply_cross_arb_execution_profile(&mut config, "simulation").unwrap();
        assert_eq!(config.mode, rustcta::market::RuntimeMode::Simulation);
        assert_eq!(
            config.strategy.mode,
            Some(rustcta::market::RuntimeMode::Simulation)
        );
        assert!(config.execution.dry_run);
        assert!(!config.enable_live_trading);
        assert!(apply_cross_arb_execution_profile(&mut config, "live").is_err());
    }

    #[tokio::test]
    async fn cross_arb_settings_update_should_persist_selected_exchanges_accounts_and_risk() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("cross_exchange_arbitrage_usdt.yml");
        std::fs::write(
            &config_path,
            serde_yaml::to_string(&CrossExchangeArbitrageConfig::default()).unwrap(),
        )
        .unwrap();
        let state = test_app_state(dir.path(), config_path.clone());
        let request = CrossArbSettingsUpdateRequest {
            execution_profile: Some("live_small_dry_run".to_string()),
            exchanges: vec![
                CrossArbExchangeSettingsRequest {
                    exchange: "binance".to_string(),
                    enabled: true,
                    account_id: Some("acct-binance".to_string()),
                    env_prefix: Some("BINANCE_A".to_string()),
                    private_rest_enabled: Some(true),
                    private_ws_enabled: Some(true),
                },
                CrossArbExchangeSettingsRequest {
                    exchange: "bitget".to_string(),
                    enabled: false,
                    account_id: Some("acct-bitget".to_string()),
                    env_prefix: Some("BITGET_B".to_string()),
                    private_rest_enabled: Some(true),
                    private_ws_enabled: Some(true),
                },
                CrossArbExchangeSettingsRequest {
                    exchange: "gate".to_string(),
                    enabled: true,
                    account_id: Some("acct-gate".to_string()),
                    env_prefix: Some("GATE_C".to_string()),
                    private_rest_enabled: Some(true),
                    private_ws_enabled: Some(true),
                },
            ],
            symbols: vec!["EDGE/USDT".to_string(), "ALT/USDT".to_string()],
            target_symbol_count: Some(2),
            min_notional_usdt: Some(5.0),
            target_notional_usdt: Some(5.0),
            max_notional_usdt: Some(5.2),
            max_positions_per_exchange: Some(10),
            max_open_bundles: Some(10),
            max_open_positions: Some(20),
            max_symbol_notional_usdt: Some(5.2),
            max_notional_per_symbol_usdt: Some(5.2),
            max_notional_per_exchange_usdt: Some(52.0),
            max_total_notional_usdt: Some(104.0),
            min_open_raw_spread: Some(0.005),
            min_open_maker_taker_net_edge: Some(0.005),
            max_open_raw_spread: Some(0.1),
            lock_profit_dual_taker_pct: Some(0.0005),
            max_close_spread_pct: Some(0.0005),
            restart: false,
        };

        let response = apply_cross_arb_settings_update(&state, request)
            .await
            .unwrap();
        let saved = read_cross_arb_config(&state).await.unwrap();

        assert_eq!(response["saved"], json!(true));
        assert_eq!(
            saved.universe.enabled_exchanges,
            vec![ExchangeId::Binance, ExchangeId::Gate]
        );
        assert_eq!(saved.market.min_common_exchanges, 2);
        assert_eq!(
            saved
                .exchanges
                .get(&ExchangeId::Binance)
                .and_then(|runtime| runtime.account_id.as_deref()),
            Some("acct-binance")
        );
        assert_eq!(
            saved
                .exchanges
                .get(&ExchangeId::Gate)
                .and_then(|runtime| runtime.env_prefix.as_deref()),
            Some("GATE_C")
        );
        assert!(saved
            .exchanges
            .get(&ExchangeId::Bitget)
            .is_some_and(|runtime| runtime.is_disabled()));
        assert_eq!(saved.universe.symbols.len(), 2);
        assert_eq!(saved.sizing.target_notional_usdt, 5.0);
        assert_eq!(saved.sizing.max_notional_usdt, 5.2);
        assert_eq!(saved.sizing.max_positions_per_exchange, 10);
        assert_eq!(saved.risk.max_total_notional_usdt, 104.0);
        assert_eq!(saved.thresholds.lock_profit_dual_taker_pct, 0.0005);
        assert_eq!(saved.mode, rustcta::market::RuntimeMode::LiveSmall);
        assert!(saved.execution.dry_run);
        assert!(!saved.enable_live_trading);
    }

    #[tokio::test]
    async fn cross_arb_settings_update_should_fill_env_prefix_from_account_manager() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("cross_exchange_arbitrage_usdt.yml");
        std::fs::write(
            &config_path,
            serde_yaml::to_string(&CrossExchangeArbitrageConfig::default()).unwrap(),
        )
        .unwrap();
        let state = test_app_state(dir.path(), config_path);
        write_accounts_config(&state.accounts_config);
        let request = CrossArbSettingsUpdateRequest {
            exchanges: vec![CrossArbExchangeSettingsRequest {
                exchange: "binance".to_string(),
                enabled: true,
                account_id: Some("binance_hcr".to_string()),
                env_prefix: None,
                private_rest_enabled: Some(true),
                private_ws_enabled: Some(true),
            }],
            restart: false,
            ..CrossArbSettingsUpdateRequest::default()
        };

        apply_cross_arb_settings_update(&state, request)
            .await
            .unwrap();
        let saved = read_cross_arb_config(&state).await.unwrap();
        let runtime = saved.exchanges.get(&ExchangeId::Binance).unwrap();

        assert_eq!(runtime.account_id.as_deref(), Some("binance_hcr"));
        assert_eq!(runtime.env_prefix.as_deref(), Some("BINANCE_3"));
    }

    #[tokio::test]
    async fn cross_arb_settings_update_should_reject_unsupported_exchange() {
        let dir = tempdir().unwrap();
        let config_path = dir.path().join("cross_exchange_arbitrage_usdt.yml");
        std::fs::write(
            &config_path,
            serde_yaml::to_string(&CrossExchangeArbitrageConfig::default()).unwrap(),
        )
        .unwrap();
        let state = test_app_state(dir.path(), config_path);
        let request = CrossArbSettingsUpdateRequest {
            exchanges: vec![CrossArbExchangeSettingsRequest {
                exchange: "okx".to_string(),
                enabled: true,
                account_id: None,
                env_prefix: None,
                private_rest_enabled: None,
                private_ws_enabled: None,
            }],
            restart: false,
            ..CrossArbSettingsUpdateRequest::default()
        };

        let error = apply_cross_arb_settings_update(&state, request)
            .await
            .unwrap_err()
            .to_string();

        assert!(error.contains("unsupported cross-arb exchange okx"));
    }

    #[tokio::test]
    async fn restart_strategy_should_pass_active_strategy_config_to_helper() {
        let dir = tempdir().unwrap();
        let script = dir.path().join("restart-helper.sh");
        let observed = dir.path().join("observed.txt");
        std::fs::write(
            &script,
            format!(
                "#!/usr/bin/env bash\nset -euo pipefail\nprintf '%s\\n' \"$1\" > '{}'\nprintf '%s\\n' \"$STRATEGY_CONFIG\" >> '{}'\nprintf 'strategy: stopped\\n'\n",
                observed.display(),
                observed.display()
            ),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut permissions = std::fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&script, permissions).unwrap();
        }
        let config_path = dir.path().join("cross_exchange_arbitrage_usdt.yml");
        std::fs::write(&config_path, "mode: simulation\n").unwrap();
        let state = AppState {
            snapshot_path: dir.path().join("snapshot.json"),
            command_path: dir.path().join("commands.jsonl"),
            exchange_api_key_store: dir.path().join("exchange_api_keys.env"),
            accounts_config: dir.path().join("accounts.yml"),
            balance_history_path: dir.path().join("balance_history.json"),
            strategy_profit_history_path: dir.path().join("strategy_profit_history.json"),
            strategy_config: config_path.clone(),
            strategy_registry: dir.path().join("strategy_registry.json"),
            strategy_specs: dir.path().join("strategy_specs.json"),
            restart_script: script,
            token_env: "RUSTCTA_TEST_TOKEN".to_string(),
            event_interval_ms: 1000,
            balance_history_interval_ms: 60_000,
            supervisor: Arc::new(RwLock::new(LocalProcessSupervisor::new())),
        };

        let response = restart_strategy(&state).await.unwrap();
        let observed = std::fs::read_to_string(observed).unwrap();
        let lines = observed.lines().collect::<Vec<_>>();

        assert_eq!(response["requested"], json!(true));
        assert_eq!(response["strategy_status"], "stopped");
        assert_eq!(response["strategy_stopped"], json!(true));
        assert_eq!(lines[0], "restart-strategy");
        assert_eq!(lines[1], config_path.to_string_lossy());
    }

    #[test]
    fn restart_strategy_status_should_detect_cross_arb_started() {
        let stdout = "    PID CMD\n 42 target/debug/cross_arb_live --config config/cross_exchange_arbitrage_usdt.yml --run";

        assert_eq!(restart_strategy_status_from_stdout(stdout), "started");
        assert_eq!(
            restart_strategy_status_from_stdout("strategy: stopped"),
            "stopped"
        );
        assert_eq!(restart_strategy_status_from_stdout(""), "unknown");
    }

    #[test]
    fn cross_arb_instrument_event_should_expose_precision_rules() {
        let row = flatten_cross_arb_event(
            "Instrument",
            &json!({
                "exchange": "gate",
                "canonical_symbol": "EDGE/USDT",
                "exchange_symbol": { "exchange": "gate", "symbol": "EDGE_USDT" },
                "price_tick": 0.0001,
                "quantity_step": 1.0,
                "min_qty": 1.0,
                "min_notional": 5.0,
                "status": "Trading"
            }),
            json!(7),
            json!("2026-06-06T09:00:00Z"),
        );

        assert_eq!(row["kind"], "Instrument");
        assert_eq!(row["exchange"], "gate");
        assert_eq!(row["canonical_symbol"], "EDGE/USDT");
        assert_eq!(row["exchange_symbol"]["symbol"], "EDGE_USDT");
        assert_eq!(row["price_tick"], json!(0.0001));
        assert_eq!(row["quantity_step"], json!(1.0));
        assert_eq!(row["min_notional"], json!(5.0));
    }

    #[test]
    fn cross_arb_instrument_feasibility_should_account_for_quantity_step_headroom() {
        let row = json!({
            "exchange": "binance",
            "canonical_symbol": "ALT/USDT",
            "contract_size": 1.0,
            "quantity_step": 1.0,
            "min_qty": 1.0,
            "min_notional": 5.0,
        });
        let row = enrich_cross_arb_instrument_feasibility(row, Some(0.005563), 5.0, 5.2);
        let feasibility = row.get("feasibility").expect("feasibility");

        assert_eq!(feasibility["known"], json!(true));
        assert_eq!(feasibility["fits_target"], json!(false));
        assert_eq!(feasibility["fits_max"], json!(true));
        assert_eq!(feasibility["required_notional_usdt"], json!(5.001137));
    }
}
