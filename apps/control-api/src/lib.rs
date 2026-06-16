use anyhow::{Context, Result};
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::Json;
use axum::Router;
use chrono::Utc;
use rustcta_control_api::{
    router, ControlApiState, CreateStrategyRequest, StrategyProcessView, CONTROL_API_SCHEMA_VERSION,
};
use rustcta_event_ledger::JsonlLedger;
use rustcta_supervisor::{LifecycleCommandRecord, SupervisorError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8091";
const DEFAULT_TENANT_ID: &str = "local";
const DEFAULT_SUPERVISOR_REGISTRY_PATH: &str = "run/supervisor/registry.json";
const DEFAULT_LOCAL_CREDENTIAL_STATUS_STORE: &str = "data/control_api/exchange_api_keys.env";
const DEFAULT_ACCOUNTS_CONFIG: &str = "config/accounts.yml";
const DEFAULT_STRATEGY_LOG_TAIL_LINES: usize = 800;
const DEFAULT_STRATEGY_LOG_TAIL_BYTES: usize = 256 * 1024;
const UNIFIED_ARB_STRATEGY_ID: &str = "unified_arb_live";
const LOCAL_STRATEGY_CONFIG_REF: &str = "local-agent:strategy-config";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlApiAppConfig {
    pub bind_addr: String,
    pub local_agent: Option<LocalAgentConfig>,
    pub local_side_effects: Option<LocalSideEffectConfig>,
    pub legacy_snapshot_path: Option<PathBuf>,
    pub extra_strategy_snapshot_paths: Vec<PathBuf>,
    pub supervisor_registry_path: Option<PathBuf>,
    pub audit_ledger_path: Option<PathBuf>,
    pub strategy_log_path: Option<PathBuf>,
    pub strategy_log_tail_lines: Option<usize>,
    pub strategy_log_tail_bytes: Option<usize>,
    pub credential_status_store: PathBuf,
    pub accounts_config: PathBuf,
    pub static_dir: Option<PathBuf>,
}

impl ControlApiAppConfig {
    pub fn from_env() -> Self {
        Self::from_env_iter(std::env::vars())
    }

    pub fn from_env_iter<I, K, V>(vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let vars = vars
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<std::collections::BTreeMap<_, _>>();

        let bind_addr = non_empty_value(&vars, "RUSTCTA_CONTROL_API_BIND")
            .unwrap_or_else(|| DEFAULT_BIND_ADDR.to_string());
        let local_agent = non_empty_value(&vars, "RUSTCTA_CONTROL_API_AGENT_ID").map(|agent_id| {
            let tenant_id = non_empty_value(&vars, "RUSTCTA_CONTROL_API_TENANT_ID")
                .unwrap_or_else(|| DEFAULT_TENANT_ID.to_string());
            let capabilities = non_empty_value(&vars, "RUSTCTA_CONTROL_API_AGENT_CAPABILITIES")
                .and_then(|value| parse_csv(&value))
                .unwrap_or_else(default_agent_capabilities);
            LocalAgentConfig {
                agent_id,
                tenant_id,
                capabilities,
            }
        });
        let local_side_effects = if local_agent.is_some() {
            let config = LocalSideEffectConfig::from_env(&vars);
            config.has_side_effect_path().then_some(config)
        } else {
            None
        };

        Self {
            bind_addr,
            local_agent,
            local_side_effects,
            legacy_snapshot_path: path_value(&vars, "RUSTCTA_CONTROL_API_LEGACY_SNAPSHOT_PATH"),
            extra_strategy_snapshot_paths: path_list_value(
                &vars,
                "RUSTCTA_CONTROL_API_EXTRA_STRATEGY_SNAPSHOT_PATHS",
            ),
            supervisor_registry_path: path_value(
                &vars,
                "RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH",
            )
            .or_else(|| Some(PathBuf::from(DEFAULT_SUPERVISOR_REGISTRY_PATH))),
            audit_ledger_path: path_value(&vars, "RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH"),
            strategy_log_path: path_value(&vars, "RUSTCTA_CONTROL_API_STRATEGY_LOG_PATH"),
            strategy_log_tail_lines: usize_value(
                &vars,
                "RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_LINES",
            ),
            strategy_log_tail_bytes: usize_value(
                &vars,
                "RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_BYTES",
            ),
            credential_status_store: path_value(
                &vars,
                "RUSTCTA_CONTROL_API_CREDENTIAL_STATUS_STORE",
            )
            .or_else(|| path_value(&vars, "RUSTCTA_CONTROL_API_EXCHANGE_API_KEY_STORE"))
            .or_else(|| path_value(&vars, "EXCHANGE_API_KEY_STORE"))
            .unwrap_or_else(|| PathBuf::from(DEFAULT_LOCAL_CREDENTIAL_STATUS_STORE)),
            accounts_config: path_value(&vars, "RUSTCTA_CONTROL_API_ACCOUNTS_CONFIG")
                .or_else(|| path_value(&vars, "ACCOUNTS_CONFIG"))
                .unwrap_or_else(|| PathBuf::from(DEFAULT_ACCOUNTS_CONFIG)),
            static_dir: path_value(&vars, "RUSTCTA_CONTROL_API_STATIC_DIR"),
        }
    }

    pub fn build_state(&self) -> Result<ControlApiState> {
        let mut state = ControlApiState::empty_local();
        if let Some(path) = &self.legacy_snapshot_path {
            state = state.with_legacy_dashboard_snapshot_path(path.clone());
        }
        state =
            state.with_extra_strategy_snapshot_paths(self.extra_strategy_snapshot_paths.clone());

        if let Some(agent) = &self.local_agent {
            state = state.with_local_agent(
                agent.agent_id.clone(),
                agent.tenant_id.clone(),
                agent.capabilities.clone(),
            );
        }
        if let Some(path) = &self.supervisor_registry_path {
            state = state.with_supervisor_registry_path(path.clone());
        }
        if let Some(path) = &self.audit_ledger_path {
            state = state.with_audit_ledger(Arc::new(JsonlLedger::new(path.clone())));
        }
        if let Some(path) = &self.strategy_log_path {
            state = state.with_strategy_log_path(path.clone());
        }
        if self.strategy_log_tail_lines.is_some() || self.strategy_log_tail_bytes.is_some() {
            state = state.with_strategy_log_tail_limits(
                self.strategy_log_tail_lines
                    .unwrap_or(DEFAULT_STRATEGY_LOG_TAIL_LINES),
                self.strategy_log_tail_bytes
                    .unwrap_or(DEFAULT_STRATEGY_LOG_TAIL_BYTES),
            );
        }

        Ok(state)
    }

    pub fn build_router(&self) -> Result<Router> {
        let state = self.build_state()?;
        let mut api = router(state.clone());
        api = api.merge(local_credentials_router(LocalCredentialState::new(
            self.credential_status_store.clone(),
            self.accounts_config.clone(),
        )));
        if self.local_agent.is_some() {
            api = api.merge(local_mutation_router(state.clone()));
        }
        if let (Some(agent), Some(side_effects)) = (&self.local_agent, &self.local_side_effects) {
            api = api.merge(local_agent_router(LocalSideEffectState::new(
                agent.clone(),
                side_effects.clone(),
                state,
            )));
        } else {
            api = api.merge(local_empty_balance_history_router());
        }
        let app = match &self.static_dir {
            Some(static_dir) => {
                let static_dir = static_dir.clone();
                api.fallback(move |uri: Uri| {
                    let static_dir = static_dir.clone();
                    async move { serve_static_spa(uri, static_dir).await }
                })
            }
            None => api,
        };
        Ok(app)
    }

    pub async fn clean_unified_arb_exchange_config_on_startup(&self) -> Result<()> {
        let (Some(agent), Some(side_effects)) = (&self.local_agent, &self.local_side_effects)
        else {
            return Ok(());
        };
        if side_effects.strategy_config_path.is_none() {
            return Ok(());
        }

        let state =
            LocalSideEffectState::new(agent.clone(), side_effects.clone(), self.build_state()?);
        let operation_id = local_operation_id();
        match load_unified_arb_exchange_config(&state, &operation_id, true).await {
            Ok(view) => {
                if view.cleaned_on_read {
                    eprintln!(
                        "rustcta-control-api cleaned unified-arb exchange config on startup: cleaned_invalid_count={} removed_exchanges={:?}",
                        view.cleaned_invalid_count, view.removed_exchanges
                    );
                }
            }
            Err((status_code, Json(body))) => {
                let status = body
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown");
                eprintln!(
                    "rustcta-control-api skipped unified-arb exchange config startup cleanup: http_status={} status={}",
                    status_code, status
                );
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalAgentConfig {
    pub agent_id: String,
    pub tenant_id: String,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalSideEffectConfig {
    pub strategy_config_path: Option<PathBuf>,
    pub command_queue_path: Option<PathBuf>,
    pub balance_history_path: Option<PathBuf>,
    pub strategy_profit_history_path: Option<PathBuf>,
    pub restart_script_path: Option<PathBuf>,
}

impl LocalSideEffectConfig {
    fn from_env(vars: &std::collections::BTreeMap<String, String>) -> Self {
        Self {
            strategy_config_path: path_value(
                vars,
                "RUSTCTA_CONTROL_API_LOCAL_STRATEGY_CONFIG_PATH",
            ),
            command_queue_path: path_value(vars, "RUSTCTA_CONTROL_API_LOCAL_COMMAND_QUEUE_PATH"),
            balance_history_path: path_value(
                vars,
                "RUSTCTA_CONTROL_API_LOCAL_BALANCE_HISTORY_PATH",
            ),
            strategy_profit_history_path: path_value(
                vars,
                "RUSTCTA_CONTROL_API_LOCAL_STRATEGY_PROFIT_HISTORY_PATH",
            ),
            restart_script_path: path_value(vars, "RUSTCTA_CONTROL_API_LOCAL_RESTART_SCRIPT_PATH"),
        }
    }

    fn has_side_effect_path(&self) -> bool {
        self.strategy_config_path.is_some()
            || self.command_queue_path.is_some()
            || self.balance_history_path.is_some()
            || self.strategy_profit_history_path.is_some()
            || self.restart_script_path.is_some()
    }
}

#[derive(Clone)]
struct LocalSideEffectState {
    agent: LocalAgentConfig,
    config: LocalSideEffectConfig,
    control: ControlApiState,
    audit: Arc<Mutex<Vec<Value>>>,
}

#[derive(Debug, Clone)]
struct ExchangeLatencyTarget {
    exchange: String,
    label: String,
    rest_url: String,
    ws_url: String,
    endpoint_source: String,
}

#[derive(Debug, Clone, Deserialize)]
struct ExchangeLatencyTestRequest {
    mode: Option<String>,
    batch_size: Option<usize>,
    timeout_ms: Option<u64>,
    exchanges: Option<Vec<String>>,
    gateway_endpoints: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Serialize)]
struct ExchangeLatencyTestResponse {
    generated_at: chrono::DateTime<Utc>,
    mode: String,
    batch_size: usize,
    timeout_ms: u64,
    exchange_count: usize,
    elapsed_ms: u64,
    rows: Vec<ExchangeLatencyRow>,
}

#[derive(Debug, Clone, Serialize)]
struct ExchangeLatencyRow {
    exchange: String,
    label: String,
    endpoint_source: String,
    rest: EndpointLatencyResult,
    websocket: EndpointLatencyResult,
}

#[derive(Debug, Clone, Serialize)]
struct EndpointLatencyResult {
    status: String,
    latency_ms: Option<u64>,
    endpoint: String,
    error: Option<String>,
}

#[derive(Clone)]
struct LocalCredentialState {
    credential_status_store: PathBuf,
    accounts_config: PathBuf,
    checks: Arc<Mutex<BTreeMap<String, ExchangeAccountCheck>>>,
    restart_required: Arc<Mutex<bool>>,
}

impl LocalCredentialState {
    fn new(credential_status_store: PathBuf, accounts_config: PathBuf) -> Self {
        Self {
            credential_status_store,
            accounts_config,
            checks: Arc::new(Mutex::new(BTreeMap::new())),
            restart_required: Arc::new(Mutex::new(false)),
        }
    }
}

impl LocalSideEffectState {
    fn new(
        agent: LocalAgentConfig,
        config: LocalSideEffectConfig,
        control: ControlApiState,
    ) -> Self {
        Self {
            agent,
            config,
            control,
            audit: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

fn local_mutation_router(state: ControlApiState) -> Router {
    Router::new()
        .route("/api/strategies", post(local_create_strategy))
        .route("/api/strategies/:id", delete(local_delete_strategy))
        .route("/api/commands", post(local_command))
        .route("/api/strategies/:id/command", post(local_strategy_command))
        .with_state(state)
}

fn local_agent_router(state: LocalSideEffectState) -> Router {
    Router::new()
        .route("/api/local-agent/status", get(local_agent_status))
        .route("/api/local-agent/audit", get(local_agent_audit))
        .route(
            "/api/local-agent/exchange-latency-test",
            post(local_agent_exchange_latency_test),
        )
        .route(
            "/api/local-agent/strategy-config",
            get(local_agent_strategy_config_draft).post(local_agent_strategy_config),
        )
        .route(
            "/api/local-agent/unified-arb/exchanges",
            get(local_agent_unified_arb_exchanges).post(local_agent_save_unified_arb_exchanges),
        )
        .route(
            "/api/local-agent/unified-arb/settings",
            get(local_agent_unified_arb_settings).post(local_agent_save_unified_arb_settings),
        )
        .route(
            "/api/local-agent/unified-arb/exchanges/:exchange",
            delete(local_agent_delete_unified_arb_exchange),
        )
        .route("/api/local-agent/commands", post(local_agent_command))
        .route(
            "/api/local-agent/history/:kind/status",
            get(local_agent_history_status),
        )
        .route("/api/balance-history", get(local_agent_balance_history))
        .with_state(state)
}

fn local_credentials_router(state: LocalCredentialState) -> Router {
    Router::new()
        .route(
            "/api/local-credentials/status",
            get(local_credential_status),
        )
        .route(
            "/api/exchange-api-keys",
            get(exchange_api_keys).post(update_exchange_api_keys),
        )
        .route("/api/exchange-api-keys/test", post(test_exchange_api_keys))
        .route(
            "/api/exchange-api-keys/:exchange",
            delete(delete_exchange_api_keys),
        )
        .with_state(state)
}

fn local_empty_balance_history_router() -> Router {
    Router::new().route("/api/balance-history", get(local_empty_balance_history))
}

async fn local_empty_balance_history() -> (StatusCode, Json<Value>) {
    local_json_response(
        StatusCode::OK,
        json!({
            "schema_version": CONTROL_API_SCHEMA_VERSION,
            "rows": [],
            "local_path_exposed": false,
        }),
    )
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
    connection: ExchangeAccountCheck,
    balances: Vec<ExchangeAccountBalanceView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ExchangeAccountCheck {
    status: String,
    last_tested_at: Option<chrono::DateTime<Utc>>,
    unified_total_usdt: Option<f64>,
    spot_total_usdt: Option<f64>,
    perp_total_usdt: Option<f64>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExchangeAccountBalanceView {
    market_type: String,
    asset: String,
    total: f64,
    available: f64,
    locked: f64,
    valuation_usdt: Option<f64>,
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

#[derive(Debug, Clone, Deserialize, Default)]
struct ExchangeApiKeyUpdateRequest {
    #[serde(default)]
    exchange: String,
    #[serde(default)]
    account_id: Option<String>,
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
struct UnifiedArbExchangeConfigUpdateRequest {
    #[serde(default)]
    strategy_id: Option<String>,
    #[serde(default)]
    exchanges: Vec<Value>,
    #[serde(default = "default_true_bool")]
    replace: bool,
    #[serde(default = "default_true_bool")]
    apply: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct UnifiedArbSettingsUpdateRequest {
    #[serde(default)]
    strategy_id: Option<String>,
    #[serde(default)]
    symbols: Option<Vec<String>>,
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
    min_open_spread_pct: Option<f64>,
    #[serde(default)]
    min_open_maker_taker_net_edge: Option<f64>,
    #[serde(default)]
    min_open_net_profit_pct: Option<f64>,
    #[serde(default)]
    min_open_net_edge_pct: Option<f64>,
    #[serde(default)]
    min_open_executable_depth_ratio: Option<f64>,
    #[serde(default)]
    close_min_net_profit_pct: Option<f64>,
    #[serde(default)]
    lock_profit_dual_taker_pct: Option<f64>,
    #[serde(default)]
    expected_close_spread_pct: Option<f64>,
    #[serde(default)]
    max_close_spread_pct: Option<f64>,
    #[serde(default)]
    execution_module: Option<String>,
    #[serde(default)]
    maker_price_offset_pct: Option<f64>,
    #[serde(default)]
    maker_order_timeout_ms: Option<u64>,
    #[serde(default)]
    hedge_taker_slippage_pct: Option<f64>,
    #[serde(default)]
    close_taker_slippage_pct: Option<f64>,
    #[serde(default)]
    execution_profile: Option<String>,
    #[serde(default)]
    trading_enabled: Option<bool>,
    #[serde(default = "default_true_bool")]
    apply: bool,
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

#[derive(Debug, Clone, Default, Deserialize)]
struct AccountManagerConfigFile {
    #[serde(default)]
    accounts: BTreeMap<String, AccountManagerConfigEntry>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AccountManagerConfigEntry {
    #[serde(default)]
    name: Option<String>,
    exchange: String,
    #[serde(default, rename = "type")]
    account_type: Option<String>,
    #[serde(default)]
    description: Option<String>,
    env_prefix: String,
    #[serde(default = "default_true_bool")]
    enabled: bool,
}

fn default_true_bool() -> bool {
    true
}

async fn local_credential_status(State(state): State<LocalCredentialState>) -> Response {
    match exchange_api_key_status(&state).await {
        Ok(status) => Json(status).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

async fn exchange_api_keys(State(state): State<LocalCredentialState>) -> Response {
    match exchange_api_key_status(&state).await {
        Ok(status) => Json(status).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "accepted": false,
                "error": error.to_string(),
                "local_path_exposed": false,
            })),
        )
            .into_response(),
    }
}

async fn update_exchange_api_keys(
    State(state): State<LocalCredentialState>,
    Json(request): Json<ExchangeApiKeyUpdateRequest>,
) -> (StatusCode, Json<Value>) {
    match apply_exchange_api_key_update(&state, request).await {
        Ok(status) => local_json_response(
            StatusCode::OK,
            json!({
                "accepted": true,
                "status": status,
                "local_path_exposed": false,
            }),
        ),
        Err((status_code, value)) => local_json_response(status_code, value),
    }
}

async fn delete_exchange_api_keys(
    State(state): State<LocalCredentialState>,
    AxumPath(exchange): AxumPath<String>,
) -> (StatusCode, Json<Value>) {
    let request = ExchangeApiKeyUpdateRequest {
        exchange,
        clear: true,
        ..ExchangeApiKeyUpdateRequest::default()
    };
    update_exchange_api_keys(State(state), Json(request)).await
}

async fn test_exchange_api_keys(
    State(state): State<LocalCredentialState>,
    Json(request): Json<ExchangeApiKeyUpdateRequest>,
) -> (StatusCode, Json<Value>) {
    let Some(schema) = exchange_api_key_schema(&request.exchange) else {
        return local_json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "accepted": false,
                "error": "unsupported_exchange",
                "local_path_exposed": false,
            }),
        );
    };
    let values = match read_local_credential_status_source(&state.credential_status_store).await {
        Ok(values) => values,
        Err(error) => {
            return local_json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({
                    "accepted": false,
                    "error": error.to_string(),
                    "local_path_exposed": false,
                }),
            )
        }
    };
    let account_id = normalize_account_id(request.account_id.as_deref().unwrap_or("default"));
    let credential_namespace =
        normalized_credential_namespace(schema, request.credential_namespace.as_deref());
    let missing =
        missing_required_credential_fields(&values, schema, &account_id, &credential_namespace);
    let accepted = missing.is_empty();
    let check = ExchangeAccountCheck {
        status: if accepted { "ok" } else { "error" }.to_string(),
        last_tested_at: Some(Utc::now()),
        error: (!accepted).then(|| {
            format!(
                "missing required credential field(s): {}",
                missing.join(", ")
            )
        }),
        ..ExchangeAccountCheck::default()
    };
    state.checks.lock().await.insert(
        exchange_account_check_key(schema.exchange, &account_id, &credential_namespace),
        check.clone(),
    );
    match exchange_api_key_status(&state).await {
        Ok(status) => local_json_response(
            StatusCode::OK,
            json!({
                "accepted": accepted,
                "check": check,
                "status": status,
                "local_path_exposed": false,
            }),
        ),
        Err(error) => local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "accepted": false,
                "error": error.to_string(),
                "local_path_exposed": false,
            }),
        ),
    }
}

async fn exchange_api_key_status(
    state: &LocalCredentialState,
) -> Result<ExchangeApiKeyStatusResponse> {
    let values = read_local_credential_status_source(&state.credential_status_store).await?;
    let account_manager_accounts =
        account_manager_accounts_for_status(&state.accounts_config).await;
    let checks = state.checks.lock().await.clone();
    let restart_required = *state.restart_required.lock().await;
    let supported_schemas = exchange_api_key_schemas().iter().collect::<Vec<_>>();
    let supported_exchanges = supported_schemas
        .iter()
        .map(|schema| exchange_schema_status(&values, schema, false, true, &checks))
        .collect::<Vec<_>>();
    let mut exchanges = Vec::new();
    for schema in supported_schemas {
        let enabled = account_manager_accounts
            .iter()
            .find(|account| account.exchange.eq_ignore_ascii_case(schema.exchange))
            .map(|account| account.enabled)
            .unwrap_or(true);
        let status = exchange_schema_status(&values, schema, true, enabled, &checks);
        if exchange_status_has_configured_field(&status) {
            exchanges.push(status);
        }
    }
    Ok(ExchangeApiKeyStatusResponse {
        store_path: "local-agent:exchange-api-key-store".to_string(),
        restart_required,
        enabled_exchanges: account_manager_accounts
            .iter()
            .filter(|account| account.enabled)
            .map(|account| account.exchange.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect(),
        account_manager_accounts,
        supported_exchanges,
        exchanges,
    })
}

async fn local_agent_exchange_latency_test(
    State(state): State<LocalSideEffectState>,
    Json(request): Json<ExchangeLatencyTestRequest>,
) -> Response {
    let started = Instant::now();
    let timeout_ms = request.timeout_ms.unwrap_or(3_000).clamp(500, 15_000);
    let batch_size = request.batch_size.unwrap_or(5).clamp(1, 50);
    let mode = match request
        .mode
        .as_deref()
        .unwrap_or("batched")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "all" | "all_concurrent" | "concurrent" => "all_concurrent".to_string(),
        _ => "batched".to_string(),
    };
    let targets = selected_latency_targets(
        &state,
        request.exchanges.as_deref(),
        request.gateway_endpoints.as_deref(),
    )
    .await;
    let client = match reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout_ms))
        .user_agent("RustCTA-ControlAPI-LatencyProbe/0.1")
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": error.to_string() })),
            )
                .into_response();
        }
    };

    let timeout = Duration::from_millis(timeout_ms);
    let rows = if mode == "all_concurrent" {
        probe_latency_batch(targets, client, timeout).await
    } else {
        let mut rows = Vec::new();
        for chunk in targets.chunks(batch_size) {
            rows.extend(probe_latency_batch(chunk.to_vec(), client.clone(), timeout).await);
        }
        rows
    };

    Json(ExchangeLatencyTestResponse {
        generated_at: Utc::now(),
        mode,
        batch_size,
        timeout_ms,
        exchange_count: rows.len(),
        elapsed_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
        rows,
    })
    .into_response()
}

async fn probe_latency_batch(
    targets: Vec<ExchangeLatencyTarget>,
    client: reqwest::Client,
    timeout: Duration,
) -> Vec<ExchangeLatencyRow> {
    let tasks = targets
        .into_iter()
        .map(|target| {
            let client = client.clone();
            tokio::spawn(async move { probe_latency_target(target, client, timeout).await })
        })
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    for task in tasks {
        if let Ok(row) = task.await {
            rows.push(row);
        }
    }
    rows.sort_by(|left, right| left.exchange.cmp(&right.exchange));
    rows
}

async fn probe_latency_target(
    target: ExchangeLatencyTarget,
    client: reqwest::Client,
    timeout: Duration,
) -> ExchangeLatencyRow {
    let (rest, websocket) = tokio::join!(
        probe_rest_latency(&client, &target.rest_url, timeout),
        probe_ws_latency(&target.ws_url, timeout)
    );
    ExchangeLatencyRow {
        exchange: target.exchange.to_string(),
        label: target.label.to_string(),
        endpoint_source: target.endpoint_source.to_string(),
        rest,
        websocket,
    }
}

async fn probe_rest_latency(
    client: &reqwest::Client,
    endpoint: &str,
    timeout: Duration,
) -> EndpointLatencyResult {
    if endpoint.trim().is_empty() {
        return EndpointLatencyResult {
            status: "missing".to_string(),
            latency_ms: None,
            endpoint: String::new(),
            error: Some("endpoint not configured".to_string()),
        };
    }
    let started = Instant::now();
    match tokio::time::timeout(timeout, client.get(endpoint).send()).await {
        Ok(Ok(response)) if response.status().is_success() => EndpointLatencyResult {
            status: "ok".to_string(),
            latency_ms: Some(elapsed_ms(started)),
            endpoint: endpoint.to_string(),
            error: None,
        },
        Ok(Ok(response)) => EndpointLatencyResult {
            status: "error".to_string(),
            latency_ms: Some(elapsed_ms(started)),
            endpoint: endpoint.to_string(),
            error: Some(format!("http {}", response.status())),
        },
        Ok(Err(error)) => EndpointLatencyResult {
            status: "error".to_string(),
            latency_ms: Some(elapsed_ms(started)),
            endpoint: endpoint.to_string(),
            error: Some(error.to_string()),
        },
        Err(_) => EndpointLatencyResult {
            status: "timeout".to_string(),
            latency_ms: None,
            endpoint: endpoint.to_string(),
            error: Some(format!("timeout after {} ms", timeout.as_millis())),
        },
    }
}

async fn probe_ws_latency(endpoint: &str, timeout: Duration) -> EndpointLatencyResult {
    if endpoint.trim().is_empty() {
        return EndpointLatencyResult {
            status: "missing".to_string(),
            latency_ms: None,
            endpoint: String::new(),
            error: Some("endpoint not configured".to_string()),
        };
    }
    let started = Instant::now();
    match tokio::time::timeout(timeout, connect_async(endpoint)).await {
        Ok(Ok((_stream, _response))) => EndpointLatencyResult {
            status: "ok".to_string(),
            latency_ms: Some(elapsed_ms(started)),
            endpoint: endpoint.to_string(),
            error: None,
        },
        Ok(Err(error)) => EndpointLatencyResult {
            status: "error".to_string(),
            latency_ms: Some(elapsed_ms(started)),
            endpoint: endpoint.to_string(),
            error: Some(error.to_string()),
        },
        Err(_) => EndpointLatencyResult {
            status: "timeout".to_string(),
            latency_ms: None,
            endpoint: endpoint.to_string(),
            error: Some(format!("timeout after {} ms", timeout.as_millis())),
        },
    }
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

async fn selected_latency_targets(
    state: &LocalSideEffectState,
    filter: Option<&[String]>,
    gateway_endpoints: Option<&[Value]>,
) -> Vec<ExchangeLatencyTarget> {
    let mut targets = fallback_exchange_latency_targets();
    merge_latency_targets(
        &mut targets,
        latency_targets_from_unified_arb_config(state).await,
    );
    merge_latency_targets(
        &mut targets,
        gateway_latency_targets_from_request(gateway_endpoints),
    );
    let Some(filter) = filter else {
        return targets;
    };
    let selected = filter
        .iter()
        .map(|exchange| normalize_probe_exchange(exchange))
        .collect::<std::collections::BTreeSet<_>>();
    if selected.is_empty() {
        return targets;
    }
    targets
        .into_iter()
        .filter(|target| selected.contains(&normalize_probe_exchange(&target.exchange)))
        .collect()
}

fn merge_latency_targets(
    targets: &mut Vec<ExchangeLatencyTarget>,
    overrides: Vec<ExchangeLatencyTarget>,
) {
    for override_target in overrides {
        let key = normalize_probe_exchange(&override_target.exchange);
        if let Some(existing) = targets
            .iter_mut()
            .find(|target| normalize_probe_exchange(&target.exchange) == key)
        {
            *existing = merge_latency_target(existing.clone(), override_target);
        } else {
            targets.push(override_target);
        }
    }
}

fn merge_latency_target(
    fallback: ExchangeLatencyTarget,
    override_target: ExchangeLatencyTarget,
) -> ExchangeLatencyTarget {
    ExchangeLatencyTarget {
        exchange: override_target.exchange,
        label: if override_target.label.trim().is_empty() {
            fallback.label
        } else {
            override_target.label
        },
        rest_url: if override_target.rest_url.trim().is_empty() {
            fallback.rest_url
        } else {
            override_target.rest_url
        },
        ws_url: if override_target.ws_url.trim().is_empty() {
            fallback.ws_url
        } else {
            override_target.ws_url
        },
        endpoint_source: override_target.endpoint_source,
    }
}

fn gateway_latency_targets_from_request(rows: Option<&[Value]>) -> Vec<ExchangeLatencyTarget> {
    rows.into_iter()
        .flatten()
        .filter_map(|row| {
            let exchange = text_field_any(row, &["exchange", "exchange_id", "venue", "id"])?;
            let rest_url = text_field_any(
                row,
                &[
                    "rest_url",
                    "rest",
                    "rest_base_url",
                    "public_rest_url",
                    "public_rest_base_url",
                ],
            )
            .unwrap_or_default();
            let ws_url = text_field_any(
                row,
                &[
                    "ws_url",
                    "websocket_url",
                    "websocket",
                    "public_ws_url",
                    "public_websocket_url",
                ],
            )
            .unwrap_or_default();
            if rest_url.is_empty() && ws_url.is_empty() {
                return None;
            }
            Some(ExchangeLatencyTarget {
                exchange: gateway_exchange_key(&exchange),
                label: exchange,
                rest_url,
                ws_url,
                endpoint_source: "gateway_endpoint_metadata".to_string(),
            })
        })
        .collect()
}

async fn latency_targets_from_unified_arb_config(
    state: &LocalSideEffectState,
) -> Vec<ExchangeLatencyTarget> {
    let operation_id = local_operation_id();
    let Ok(view) = load_unified_arb_exchange_config(state, &operation_id, false).await else {
        return Vec::new();
    };
    view.exchange_map
        .iter()
        .filter_map(|(exchange, config)| {
            let rest_url = unified_arb_rest_probe_url(config).unwrap_or_default();
            let ws_url = unified_arb_ws_probe_url(config).unwrap_or_default();
            if rest_url.is_empty() && ws_url.is_empty() {
                return None;
            }
            Some(ExchangeLatencyTarget {
                exchange: exchange.clone(),
                label: exchange_label(exchange),
                rest_url,
                ws_url,
                endpoint_source: "gateway_config".to_string(),
            })
        })
        .collect()
}

fn unified_arb_rest_probe_url(config: &Value) -> Option<String> {
    text_field_any(
        config,
        &[
            "private_rest_base_url",
            "public_rest_base_url",
            "rest_base_url",
        ],
    )
    .or_else(|| route_endpoint(config, &["rest_public", "public_rest", "rest"]))
}

fn unified_arb_ws_probe_url(config: &Value) -> Option<String> {
    text_field_any(
        config,
        &["private_ws_url", "public_ws_url", "ws_url", "websocket_url"],
    )
    .or_else(|| route_endpoint(config, &["ws_public", "public_ws", "websocket", "ws"]))
}

fn route_endpoint(config: &Value, names: &[&str]) -> Option<String> {
    let routes = config.get("routes")?;
    for name in names {
        let Some(value) = routes.get(*name) else {
            continue;
        };
        if let Some(text) = value
            .as_str()
            .map(str::trim)
            .filter(|text| !text.is_empty())
        {
            return Some(text.to_string());
        }
        if let Some(text) = value
            .as_array()
            .and_then(|items| items.first())
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty())
        {
            return Some(text.to_string());
        }
    }
    None
}

fn text_field_any(value: &Value, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        value
            .get(*field)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|text| !text.is_empty() && *text != "-")
            .map(ToString::to_string)
    })
}

fn gateway_exchange_key(exchange: &str) -> String {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "gate" | "gate.io" | "gateio" => "gateio".to_string(),
        "binance-futures" | "binance_futures" => "binance_futures".to_string(),
        other => other.replace([' ', '.'], "_"),
    }
}

fn exchange_label(exchange: &str) -> String {
    fallback_exchange_latency_targets()
        .into_iter()
        .find(|target| {
            normalize_probe_exchange(&target.exchange) == normalize_probe_exchange(exchange)
        })
        .map(|target| target.label)
        .unwrap_or_else(|| exchange.to_string())
}

fn normalize_probe_exchange(exchange: &str) -> String {
    exchange
        .trim()
        .to_ascii_lowercase()
        .replace(['.', '-', '_', ' '], "")
}

fn fallback_exchange_latency_targets() -> Vec<ExchangeLatencyTarget> {
    vec![
        ExchangeLatencyTarget {
            exchange: "binance".to_string(),
            label: "Binance".to_string(),
            rest_url: "https://api.binance.com/api/v3/time".to_string(),
            ws_url: "wss://stream.binance.com:9443/ws/btcusdt@trade".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "binance_futures".to_string(),
            label: "Binance Futures".to_string(),
            rest_url: "https://fapi.binance.com/fapi/v1/time".to_string(),
            ws_url: "wss://fstream.binance.com/ws/btcusdt@trade".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "okx".to_string(),
            label: "OKX".to_string(),
            rest_url: "https://www.okx.com/api/v5/public/time".to_string(),
            ws_url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bybit".to_string(),
            label: "Bybit".to_string(),
            rest_url: "https://api.bybit.com/v5/market/time".to_string(),
            ws_url: "wss://stream.bybit.com/v5/public/spot".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitget".to_string(),
            label: "Bitget".to_string(),
            rest_url: "https://api.bitget.com/api/v2/public/time".to_string(),
            ws_url: "wss://ws.bitget.com/v2/ws/public".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "gateio".to_string(),
            label: "Gate.io".to_string(),
            rest_url: "https://api.gateio.ws/api/v4/spot/time".to_string(),
            ws_url: "wss://api.gateio.ws/ws/v4/".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "kucoin".to_string(),
            label: "KuCoin".to_string(),
            rest_url: "https://api.kucoin.com/api/v1/timestamp".to_string(),
            ws_url: "wss://ws-api-spot.kucoin.com/".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "mexc".to_string(),
            label: "MEXC".to_string(),
            rest_url: "https://api.mexc.com/api/v3/time".to_string(),
            ws_url: "wss://wbs.mexc.com/ws".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "coinbase".to_string(),
            label: "Coinbase".to_string(),
            rest_url: "https://api.exchange.coinbase.com/time".to_string(),
            ws_url: "wss://ws-feed.exchange.coinbase.com".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "kraken".to_string(),
            label: "Kraken".to_string(),
            rest_url: "https://api.kraken.com/0/public/Time".to_string(),
            ws_url: "wss://ws.kraken.com/v2".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitfinex".to_string(),
            label: "Bitfinex".to_string(),
            rest_url: "https://api-pub.bitfinex.com/v2/platform/status".to_string(),
            ws_url: "wss://api-pub.bitfinex.com/ws/2".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitstamp".to_string(),
            label: "Bitstamp".to_string(),
            rest_url: "https://www.bitstamp.net/api/v2/ticker/btcusd/".to_string(),
            ws_url: "wss://ws.bitstamp.net".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "crypto_com".to_string(),
            label: "Crypto.com".to_string(),
            rest_url: "https://api.crypto.com/exchange/v1/public/get-instruments".to_string(),
            ws_url: "wss://stream.crypto.com/exchange/v1/market".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "htx".to_string(),
            label: "HTX".to_string(),
            rest_url: "https://api.huobi.pro/v1/common/timestamp".to_string(),
            ws_url: "wss://api.huobi.pro/ws".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitmart".to_string(),
            label: "BitMart".to_string(),
            rest_url: "https://api-cloud.bitmart.com/system/time".to_string(),
            ws_url: "wss://ws-manager-compress.bitmart.com/api?protocol=1.1".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "poloniex".to_string(),
            label: "Poloniex".to_string(),
            rest_url: "https://api.poloniex.com/timestamp".to_string(),
            ws_url: "wss://ws.poloniex.com/ws/public".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "gemini".to_string(),
            label: "Gemini".to_string(),
            rest_url: "https://api.gemini.com/v1/pubticker/btcusd".to_string(),
            ws_url: "wss://api.gemini.com/v1/marketdata/BTCUSD".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitmex".to_string(),
            label: "BitMEX".to_string(),
            rest_url: "https://www.bitmex.com/api/v1/instrument/active".to_string(),
            ws_url: "wss://www.bitmex.com/realtime".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "deribit".to_string(),
            label: "Deribit".to_string(),
            rest_url: "https://www.deribit.com/api/v2/public/get_time".to_string(),
            ws_url: "wss://www.deribit.com/ws/api/v2".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitflyer".to_string(),
            label: "bitFlyer".to_string(),
            rest_url: "https://api.bitflyer.com/v1/getmarkets".to_string(),
            ws_url: "wss://ws.lightstream.bitflyer.com/json-rpc".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "upbit".to_string(),
            label: "Upbit".to_string(),
            rest_url: "https://api.upbit.com/v1/market/all".to_string(),
            ws_url: "wss://api.upbit.com/websocket/v1".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bithumb".to_string(),
            label: "Bithumb".to_string(),
            rest_url: "https://api.bithumb.com/public/ticker/BTC_KRW".to_string(),
            ws_url: "wss://pubwss.bithumb.com/pub/ws".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "coincheck".to_string(),
            label: "Coincheck".to_string(),
            rest_url: "https://coincheck.com/api/ticker".to_string(),
            ws_url: "wss://ws-api.coincheck.com/".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitbank".to_string(),
            label: "bitbank".to_string(),
            rest_url: "https://public.bitbank.cc/btc_jpy/ticker".to_string(),
            ws_url: "wss://stream.bitbank.cc/socket.io/?EIO=3&transport=websocket".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "lbank".to_string(),
            label: "LBank".to_string(),
            rest_url: "https://api.lbkex.com/v2/timestamp.do".to_string(),
            ws_url: "wss://www.lbkex.net/ws/V2/".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitrue".to_string(),
            label: "Bitrue".to_string(),
            rest_url: "https://www.bitrue.com/api/v1/time".to_string(),
            ws_url: "wss://ws.bitrue.com/kline-api/ws".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bingx".to_string(),
            label: "BingX".to_string(),
            rest_url: "https://open-api.bingx.com/openApi/spot/v1/server/time".to_string(),
            ws_url: "wss://open-api-ws.bingx.com/market".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitvavo".to_string(),
            label: "Bitvavo".to_string(),
            rest_url: "https://api.bitvavo.com/v2/time".to_string(),
            ws_url: "wss://ws.bitvavo.com/v2/".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "whitebit".to_string(),
            label: "WhiteBIT".to_string(),
            rest_url: "https://whitebit.com/api/v4/public/time".to_string(),
            ws_url: "wss://api.whitebit.com/ws".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "coinex".to_string(),
            label: "CoinEx".to_string(),
            rest_url: "https://api.coinex.com/v2/time".to_string(),
            ws_url: "wss://socket.coinex.com/v2/spot".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "phemex".to_string(),
            label: "Phemex".to_string(),
            rest_url: "https://api.phemex.com/public/time".to_string(),
            ws_url: "wss://ws.phemex.com".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "woo".to_string(),
            label: "WOO X".to_string(),
            rest_url: "https://api.woo.org/v1/public/system_info".to_string(),
            ws_url: "wss://wss.woo.org/ws/stream".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "ascendex".to_string(),
            label: "AscendEX".to_string(),
            rest_url: "https://ascendex.com/api/pro/v1/info".to_string(),
            ws_url: "wss://ascendex.com/1/api/pro/v1/stream".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitso".to_string(),
            label: "Bitso".to_string(),
            rest_url: "https://api.bitso.com/v3/ticker/?book=btc_mxn".to_string(),
            ws_url: "wss://ws.bitso.com".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "btcturk".to_string(),
            label: "BtcTurk".to_string(),
            rest_url: "https://api.btcturk.com/api/v2/server/exchangeinfo".to_string(),
            ws_url: "wss://ws-feed-pro.btcturk.com".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "luno".to_string(),
            label: "Luno".to_string(),
            rest_url: "https://api.luno.com/api/1/ticker?pair=XBTZAR".to_string(),
            ws_url: "wss://ws.luno.com/api/1/stream/BTCZAR".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "coinone".to_string(),
            label: "Coinone".to_string(),
            rest_url: "https://api.coinone.co.kr/public/v2/ticker_new/KRW/BTC".to_string(),
            ws_url: "wss://stream.coinone.co.kr".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "korbit".to_string(),
            label: "Korbit".to_string(),
            rest_url: "https://api.korbit.co.kr/v1/ticker/detailed?currency_pair=btc_krw"
                .to_string(),
            ws_url: "wss://ws.korbit.co.kr/v2/ws".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "indodax".to_string(),
            label: "Indodax".to_string(),
            rest_url: "https://indodax.com/api/server_time".to_string(),
            ws_url: "wss://ws3.indodax.com/ws/".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "mercado_bitcoin".to_string(),
            label: "Mercado Bitcoin".to_string(),
            rest_url: "https://www.mercadobitcoin.net/api/BTC/ticker/".to_string(),
            ws_url: "wss://ws.mercadobitcoin.net/ws".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "foxbit".to_string(),
            label: "Foxbit".to_string(),
            rest_url: "https://api.foxbit.com.br/rest/v3/markets".to_string(),
            ws_url: "wss://api.foxbit.com.br/ws".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitkub".to_string(),
            label: "Bitkub".to_string(),
            rest_url: "https://api.bitkub.com/api/servertime".to_string(),
            ws_url: "wss://api.bitkub.com/websocket-api/market.ticker.thb_btc".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "coinspot".to_string(),
            label: "CoinSpot".to_string(),
            rest_url: "https://www.coinspot.com.au/pubapi/latest".to_string(),
            ws_url: "wss://www.coinspot.com.au/pubapi/v2".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "btcmarkets".to_string(),
            label: "BTC Markets".to_string(),
            rest_url: "https://api.btcmarkets.net/v3/markets/BTC-AUD/ticker".to_string(),
            ws_url: "wss://socket.btcmarkets.net/v2".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "independent_reserve".to_string(),
            label: "Independent Reserve".to_string(),
            rest_url: "https://api.independentreserve.com/Public/GetValidPrimaryCurrencyCodes"
                .to_string(),
            ws_url: "wss://websockets.independentreserve.com".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "blockchain_com".to_string(),
            label: "Blockchain.com".to_string(),
            rest_url: "https://api.blockchain.com/v3/exchange/tickers/BTC-USD".to_string(),
            ws_url: "wss://ws.blockchain.info/mercury-gateway/v1/ws".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "cexio".to_string(),
            label: "CEX.IO".to_string(),
            rest_url: "https://cex.io/api/ticker/BTC/USD".to_string(),
            ws_url: "wss://ws.cex.io/ws/".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "bitpanda".to_string(),
            label: "Bitpanda".to_string(),
            rest_url: "https://api.exchange.bitpanda.com/public/v1/time".to_string(),
            ws_url: "wss://streams.exchange.bitpanda.com".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "hitbtc".to_string(),
            label: "HitBTC".to_string(),
            rest_url: "https://api.hitbtc.com/api/3/public/time".to_string(),
            ws_url: "wss://api.hitbtc.com/api/3/ws/public".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
        ExchangeLatencyTarget {
            exchange: "delta".to_string(),
            label: "Delta Exchange".to_string(),
            rest_url: "https://api.delta.exchange/v2/products".to_string(),
            ws_url: "wss://socket.india.delta.exchange".to_string(),
            endpoint_source: "fallback_catalog".to_string(),
        },
    ]
}

fn exchange_status_has_configured_field(status: &ExchangeApiKeyExchangeStatus) -> bool {
    status.fields.iter().any(|field| field.configured)
}

async fn apply_exchange_api_key_update(
    state: &LocalCredentialState,
    request: ExchangeApiKeyUpdateRequest,
) -> std::result::Result<ExchangeApiKeyStatusResponse, (StatusCode, Value)> {
    let Some(schema) = exchange_api_key_schema(&request.exchange) else {
        return Err((
            StatusCode::BAD_REQUEST,
            json!({
                "accepted": false,
                "error": "unsupported_exchange",
                "local_path_exposed": false,
            }),
        ));
    };
    let account_id = normalize_account_id(request.account_id.as_deref().unwrap_or("default"));
    let credential_namespace =
        normalized_credential_namespace(schema, request.credential_namespace.as_deref());

    let mut values = read_local_credential_status_source(&state.credential_status_store)
        .await
        .map_err(|error| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({
                    "accepted": false,
                    "error": error.to_string(),
                    "local_path_exposed": false,
                }),
            )
        })?;

    if request.clear {
        clear_exchange_api_key_values(&mut values, schema, &account_id, &credential_namespace);
        state
            .checks
            .lock()
            .await
            .remove(&exchange_account_check_key(
                schema.exchange,
                &account_id,
                &credential_namespace,
            ));
    } else {
        for field in schema.fields {
            let Some(value) = request_value_for_field(&request, field.field) else {
                continue;
            };
            clear_exchange_api_key_field_values(
                &mut values,
                schema,
                field,
                &account_id,
                &credential_namespace,
            );
            values.insert(
                store_env_key_for_field(schema, field, &account_id, &credential_namespace),
                value,
            );
        }
        state
            .checks
            .lock()
            .await
            .remove(&exchange_account_check_key(
                schema.exchange,
                &account_id,
                &credential_namespace,
            ));
    }

    write_local_credential_status_source(&state.credential_status_store, &values)
        .await
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "accepted": false,
                    "error": error.to_string(),
                    "local_path_exposed": false,
                }),
            )
        })?;
    *state.restart_required.lock().await = true;
    exchange_api_key_status(state).await.map_err(|error| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "accepted": false,
                "error": error.to_string(),
                "local_path_exposed": false,
            }),
        )
    })
}

fn clear_exchange_api_key_values(
    values: &mut BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    account_id: &str,
    credential_namespace: &str,
) {
    for field in schema.fields {
        clear_exchange_api_key_field_values(
            values,
            schema,
            field,
            account_id,
            credential_namespace,
        );
    }
}

fn clear_exchange_api_key_field_values(
    values: &mut BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
) {
    for key in managed_env_keys_for_field(schema, field, account_id, credential_namespace) {
        values.remove(&key);
    }
}

fn managed_env_keys_for_field(
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
) -> Vec<String> {
    let mut keys = field
        .aliases
        .iter()
        .map(|alias| (*alias).to_string())
        .collect::<Vec<_>>();
    keys.push(preferred_store_env_key(schema.exchange, field.field));
    keys.push(account_env_key(schema, account_id, field.field));
    keys.extend(namespace_env_keys(credential_namespace, field.field));
    keys.retain(|key| is_shell_env_key(key));
    keys.sort();
    keys.dedup();
    keys
}

fn store_env_key_for_field(
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
) -> String {
    if account_id != "default" {
        return account_env_key(schema, account_id, field.field);
    }
    let default_prefix = env_exchange_prefix(schema.exchange);
    if !credential_namespace.eq_ignore_ascii_case(&default_prefix) {
        return namespace_env_keys(credential_namespace, field.field)
            .into_iter()
            .next()
            .unwrap_or_else(|| preferred_store_env_key(schema.exchange, field.field));
    }
    preferred_store_env_key(schema.exchange, field.field)
}

fn preferred_store_env_key(exchange: &str, field: &str) -> String {
    match (
        exchange.trim().to_ascii_lowercase().as_str(),
        field.trim().to_ascii_lowercase().as_str(),
    ) {
        ("binance", "api_key") => "BINANCE_API_KEY".to_string(),
        ("binance", "api_secret") => "BINANCE_API_SECRET".to_string(),
        ("gate", "account_id") => "GATEIO_ACCOUNT_ID".to_string(),
        ("gate", "api_key") => "GATEIO_API_KEY".to_string(),
        ("gate", "api_secret") => "GATEIO_API_SECRET".to_string(),
        ("hyperliquid", "api_key") => "HYPERLIQUID_WALLET_ADDRESS".to_string(),
        ("hyperliquid", "api_secret") => "HYPERLIQUID_PRIVATE_KEY".to_string(),
        ("bitmart", "passphrase") => "BITMART_API_MEMO".to_string(),
        ("okx", "passphrase") => "OKX_PASSPHRASE".to_string(),
        (_, "api_key") => format!("{}_API_KEY", env_exchange_prefix(exchange)),
        (_, "api_secret") => format!("{}_API_SECRET", env_exchange_prefix(exchange)),
        (_, "passphrase") => format!("{}_PASSPHRASE", env_exchange_prefix(exchange)),
        (_, "account_id") => format!("{}_ACCOUNT_ID", env_exchange_prefix(exchange)),
        _ => format!(
            "{}_{}",
            env_exchange_prefix(exchange),
            field.to_ascii_uppercase()
        ),
    }
}

fn request_value_for_field(request: &ExchangeApiKeyUpdateRequest, field: &str) -> Option<String> {
    let value = match field {
        "account_id" => request.exchange_account_id.as_deref(),
        "api_key" => request.api_key.as_deref(),
        "api_secret" => request.api_secret.as_deref(),
        "passphrase" => request.passphrase.as_deref(),
        _ => None,
    }?;
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn missing_required_credential_fields(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    account_id: &str,
    credential_namespace: &str,
) -> Vec<String> {
    schema
        .fields
        .iter()
        .filter(|field| field.required)
        .filter(|field| {
            configured_field_value_for_namespace(
                values,
                schema,
                field,
                account_id,
                credential_namespace,
            )
            .is_none()
        })
        .map(|field| field.field.to_string())
        .collect()
}

fn normalized_credential_namespace(
    schema: &ExchangeApiKeySchema,
    credential_namespace: Option<&str>,
) -> String {
    let namespace = credential_namespace.unwrap_or_default().trim();
    if namespace.is_empty() || namespace == "-" {
        canonical_credential_namespace(schema)
    } else {
        namespace.to_ascii_uppercase()
    }
}

async fn account_manager_accounts_for_status(path: &PathBuf) -> Vec<AccountManagerAccountStatus> {
    read_account_manager_accounts(path)
        .await
        .unwrap_or_default()
}

async fn read_account_manager_accounts(path: &PathBuf) -> Result<Vec<AccountManagerAccountStatus>> {
    let content = match tokio::fs::read_to_string(path).await {
        Ok(content) => content,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
    };
    let config: AccountManagerConfigFile =
        serde_yaml::from_str(&content).with_context(|| format!("parse {}", path.display()))?;
    let mut accounts_by_exchange = BTreeMap::new();
    for (_account_id, account) in config.accounts {
        let _legacy_name = account.name.as_deref();
        let _legacy_env_prefix = account.env_prefix.as_str();
        let exchange = account.exchange.trim().to_ascii_lowercase();
        if exchange.is_empty() {
            continue;
        }
        let env_prefix = env_exchange_prefix(&exchange);
        let next = AccountManagerAccountStatus {
            credential_namespace: env_prefix.clone(),
            account_id: canonical_account_id(),
            name: format!("{exchange}_main"),
            exchange: exchange.clone(),
            account_type: account.account_type.unwrap_or_default(),
            description: account.description.unwrap_or_default(),
            env_prefix,
            enabled: account.enabled,
        };
        accounts_by_exchange
            .entry(exchange)
            .and_modify(|existing: &mut AccountManagerAccountStatus| {
                existing.enabled = existing.enabled || next.enabled;
                if existing.account_type.is_empty() && !next.account_type.is_empty() {
                    existing.account_type = next.account_type.clone();
                }
                if existing.description.is_empty() && !next.description.is_empty() {
                    existing.description = next.description.clone();
                }
            })
            .or_insert(next);
    }
    let mut accounts = accounts_by_exchange.into_values().collect::<Vec<_>>();
    accounts.sort_by(|left, right| {
        left.exchange
            .cmp(&right.exchange)
            .then_with(|| left.account_id.cmp(&right.account_id))
    });
    Ok(accounts)
}

fn exchange_schema_status(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    include_values: bool,
    enabled: bool,
    checks: &BTreeMap<String, ExchangeAccountCheck>,
) -> ExchangeApiKeyExchangeStatus {
    let normalized_account_id = canonical_account_id();
    let credential_namespace = canonical_credential_namespace(schema);
    exchange_schema_status_for_namespace(
        values,
        schema,
        &normalized_account_id,
        &credential_namespace,
        include_values,
        enabled,
        checks,
    )
}

fn exchange_schema_status_for_namespace(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    account_id: &str,
    credential_namespace: &str,
    include_values: bool,
    enabled: bool,
    checks: &BTreeMap<String, ExchangeAccountCheck>,
) -> ExchangeApiKeyExchangeStatus {
    let normalized_account_id = normalize_account_id(account_id);
    let check = checks
        .get(&exchange_account_check_key(
            schema.exchange,
            &normalized_account_id,
            credential_namespace,
        ))
        .cloned()
        .unwrap_or_else(|| ExchangeAccountCheck {
            status: "untested".to_string(),
            ..ExchangeAccountCheck::default()
        });
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
        balances: account_check_balances(&check),
        connection: check,
    }
}

fn exchange_account_check_key(
    exchange: &str,
    account_id: &str,
    credential_namespace: &str,
) -> String {
    format!(
        "{}:{}:{}",
        exchange.trim().to_ascii_lowercase(),
        normalize_account_id(account_id),
        credential_namespace.trim().to_ascii_uppercase()
    )
}

fn account_check_balances(check: &ExchangeAccountCheck) -> Vec<ExchangeAccountBalanceView> {
    let mut balances = Vec::new();
    if let Some(value) = check.unified_total_usdt {
        balances.push(ExchangeAccountBalanceView {
            market_type: "unified".to_string(),
            asset: "USDT".to_string(),
            total: value,
            available: value,
            locked: 0.0,
            valuation_usdt: Some(value),
        });
        return balances;
    }
    if let Some(value) = check.spot_total_usdt {
        balances.push(ExchangeAccountBalanceView {
            market_type: "spot".to_string(),
            asset: "USDT".to_string(),
            total: value,
            available: value,
            locked: 0.0,
            valuation_usdt: Some(value),
        });
    }
    if let Some(value) = check.perp_total_usdt {
        balances.push(ExchangeAccountBalanceView {
            market_type: "perp".to_string(),
            asset: "USDT".to_string(),
            total: value,
            available: value,
            locked: 0.0,
            valuation_usdt: Some(value),
        });
    }
    balances
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

fn configured_field_value(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
) -> Option<(String, String)> {
    if account_id == "default" {
        field.aliases.iter().find_map(|alias| {
            if let Some(value) = values.get(*alias) {
                return (!value.is_empty()).then(|| ("key_store".to_string(), value.clone()));
            }
            std::env::var(alias)
                .ok()
                .filter(|value| !value.is_empty())
                .map(|value| ("process_env".to_string(), value))
        })
    } else {
        let key = account_env_key(schema, account_id, field.field);
        if let Some(value) = values.get(&key) {
            return (!value.is_empty()).then(|| ("key_store".to_string(), value.clone()));
        }
        std::env::var(&key)
            .ok()
            .filter(|value| !value.is_empty())
            .map(|value| ("process_env".to_string(), value))
    }
}

fn configured_field_value_for_namespace(
    values: &BTreeMap<String, String>,
    schema: &ExchangeApiKeySchema,
    field: &ExchangeApiFieldSchema,
    account_id: &str,
    credential_namespace: &str,
) -> Option<(String, String)> {
    let default_prefix = env_exchange_prefix(schema.exchange);
    if credential_namespace.eq_ignore_ascii_case(&default_prefix) {
        return configured_field_value(values, schema, field, account_id);
    }
    namespace_env_keys(credential_namespace, field.field)
        .into_iter()
        .find_map(|key| {
            if let Some(value) = values.get(&key) {
                return (!value.is_empty()).then(|| ("key_store".to_string(), value.clone()));
            }
            std::env::var(&key)
                .ok()
                .filter(|value| !value.is_empty())
                .map(|value| ("process_env".to_string(), value))
        })
}

fn canonical_account_id() -> String {
    "default".to_string()
}

fn canonical_credential_namespace(schema: &ExchangeApiKeySchema) -> String {
    env_exchange_prefix(schema.exchange)
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

fn account_env_key(schema: &ExchangeApiKeySchema, account_id: &str, field: &str) -> String {
    format!(
        "{}{}__{}",
        account_env_prefix(schema.exchange),
        account_id.to_ascii_uppercase().replace('-', "_"),
        field.to_ascii_uppercase()
    )
}

fn namespace_env_keys(credential_namespace: &str, field: &str) -> Vec<String> {
    let namespace = credential_namespace.trim().to_ascii_uppercase();
    let field = field.trim().to_ascii_uppercase();
    if namespace.is_empty() || field.is_empty() {
        return Vec::new();
    }
    let mut keys = Vec::new();
    if namespace.ends_with('_') {
        keys.push(format!("{namespace}{field}"));
        if namespace.contains("__") {
            keys.push(format!("{}__{field}", namespace.trim_end_matches('_')));
        }
    } else {
        keys.push(format!("{namespace}_{field}"));
    }
    keys.dedup();
    keys
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
            required: false,
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
            aliases: &[
                "BINANCE_FUTURES_API_KEY",
                "BINANCE_USDM_API_KEY",
                "BINANCE_SPOT_API_KEY",
                "BINANCE_API_KEY",
            ],
            required: true,
        },
        ExchangeApiFieldSchema {
            field: "api_secret",
            label: "API Secret",
            aliases: &[
                "BINANCE_FUTURES_API_SECRET",
                "BINANCE_USDM_API_SECRET",
                "BINANCE_SPOT_API_SECRET",
                "BINANCE_API_SECRET",
            ],
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

fn exchange_api_key_schema(exchange: &str) -> Option<&'static ExchangeApiKeySchema> {
    let exchange = exchange.trim().to_ascii_lowercase();
    exchange_api_key_schemas().iter().find(|schema| {
        schema.exchange.eq_ignore_ascii_case(&exchange)
            || (schema.exchange == "gate" && matches!(exchange.as_str(), "gateio" | "gate.io"))
    })
}

async fn read_local_credential_status_source(path: &PathBuf) -> Result<BTreeMap<String, String>> {
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

async fn write_local_credential_status_source(
    path: &PathBuf,
    values: &BTreeMap<String, String>,
) -> Result<()> {
    let mut content = String::from(
        "# Managed by rustcta-control-api. Do not commit this file.\n\
         # WebUI writes here; gateway and strategy services read this same env-store.\n",
    );
    for (key, value) in values {
        if is_shell_env_key(key) && !value.trim().is_empty() {
            content.push_str(key);
            content.push('=');
            content.push_str(&shell_quote_env_value(value));
            content.push('\n');
        }
    }
    let operation_id = local_operation_id();
    atomic_write_local_file(path, &content, &operation_id)
        .await
        .with_context(|| format!("write credential env-store {}", path.display()))?;
    set_credential_store_permissions(path).await?;
    Ok(())
}

async fn set_credential_store_permissions(path: &PathBuf) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        tokio::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .await
            .with_context(|| format!("chmod 0600 {}", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn shell_quote_env_value(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
fn gateway_exchange_id(exchange: &str) -> Option<&'static str> {
    match exchange.trim().to_ascii_lowercase().as_str() {
        "binance" => Some("binance"),
        "bitget" => Some("bitget"),
        "bybit" => Some("bybit"),
        "gate" | "gateio" => Some("gateio"),
        _ => None,
    }
}

fn parse_shell_env_value(value: &str) -> String {
    if value.starts_with('\'') && value.ends_with('\'') && value.len() >= 2 {
        value[1..value.len() - 1].replace("'\\''", "'")
    } else {
        value.trim_matches('"').to_string()
    }
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

async fn local_create_strategy(
    State(state): State<ControlApiState>,
    Json(request): Json<CreateStrategyRequest>,
) -> (StatusCode, Json<Value>) {
    if !state.audit_ledger_configured() {
        return local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "audit_ledger_not_configured",
                "local_path_exposed": false,
            }),
        );
    }

    let request_for_config = request.clone();
    let (process, spec) = match request.into_process_and_spec(Utc::now()) {
        Ok(value) => value,
        Err(field) => {
            return local_json_response(
                StatusCode::BAD_REQUEST,
                json!({
                    "schema_version": CONTROL_API_SCHEMA_VERSION,
                    "accepted": false,
                    "status": "invalid_strategy_request",
                    "field": field,
                    "local_path_exposed": false,
                }),
            )
        }
    };

    if let Err(error) =
        write_generated_strategy_config(&request_for_config, &process.config_path).await
    {
        return local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "strategy_config_write_failed",
                "error": error.to_string(),
                "local_path_exposed": false,
            }),
        );
    }

    if state
        .record_operator_audit(
            "create_strategy",
            "local-agent",
            json!({
                "strategy_id": process.strategy_id,
                "strategy_kind": process.strategy_kind,
                "template_id": request_for_config.template_id,
                "would_submit_order": false,
            }),
        )
        .await
        .is_err()
    {
        return local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "audit_write_failed",
                "local_path_exposed": false,
            }),
        );
    }

    match state.create_strategy_with_spec(process, spec).await {
        Ok(process) => {
            local_json_response(StatusCode::OK, json!(StrategyProcessView::from(process)))
        }
        Err(_) => local_json_response(
            StatusCode::CONFLICT,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "strategy_registry_update_failed",
                "local_path_exposed": false,
            }),
        ),
    }
}

async fn local_delete_strategy(
    State(state): State<ControlApiState>,
    AxumPath(id): AxumPath<String>,
) -> (StatusCode, Json<Value>) {
    let strategy_id = id.trim().to_string();
    if strategy_id.is_empty() {
        return local_json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "missing_strategy_id",
                "local_path_exposed": false,
            }),
        );
    }
    if !state.audit_ledger_configured() {
        return local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "audit_ledger_not_configured",
                "local_path_exposed": false,
            }),
        );
    }
    if state
        .record_operator_audit(
            "delete_strategy",
            "local-agent",
            json!({
                "strategy_id": strategy_id,
                "would_submit_order": false,
                "local_path_exposed": false,
            }),
        )
        .await
        .is_err()
    {
        return local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "audit_write_failed",
                "local_path_exposed": false,
            }),
        );
    }

    match state.delete_strategy(&strategy_id).await {
        Ok(process) => {
            let config_deleted = delete_generated_strategy_config(&process.config_path).await;
            local_json_response(
                StatusCode::OK,
                json!({
                    "schema_version": CONTROL_API_SCHEMA_VERSION,
                    "accepted": true,
                    "status": "deleted",
                    "config_deleted": config_deleted,
                    "strategy": StrategyProcessView::from(process),
                    "local_path_exposed": false,
                }),
            )
        }
        Err(SupervisorError::NotFound { .. }) => local_json_response(
            StatusCode::NOT_FOUND,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "strategy_not_found",
                "strategy_id": strategy_id,
                "local_path_exposed": false,
            }),
        ),
        Err(error) => local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "strategy_delete_failed",
                "error": error.to_string(),
                "local_path_exposed": false,
            }),
        ),
    }
}

async fn delete_generated_strategy_config(config_path: &str) -> bool {
    let Some(path) = generated_strategy_config_delete_path(config_path) else {
        return false;
    };
    tokio::fs::remove_file(path).await.is_ok()
}

fn generated_strategy_config_delete_path(config_path: &str) -> Option<PathBuf> {
    let path = Path::new(config_path.trim());
    if path.is_absolute() {
        return None;
    }
    let components = path.components().collect::<Vec<_>>();
    let [Component::Normal(first), Component::Normal(second), Component::Normal(file)] =
        components.as_slice()
    else {
        return None;
    };
    if first.to_str() != Some("config") || second.to_str() != Some("generated") {
        return None;
    }
    let extension = Path::new(file)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if extension != "yml" && extension != "yaml" {
        return None;
    }
    Some(path.to_path_buf())
}

async fn write_generated_strategy_config(
    request: &CreateStrategyRequest,
    config_path: &str,
) -> Result<()> {
    if request.exchange_accounts.is_null() && request.risk.is_null() && request.params.is_null() {
        return Ok(());
    }
    let path = PathBuf::from(config_path);
    if path.as_os_str().is_empty()
        || config_path.starts_with("local-agent:")
        || config_path.starts_with("memory:")
    {
        return Ok(());
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("create {}", parent.display()))?;
    }
    let payload = generated_strategy_runtime_config(request);
    let content = serde_yaml::to_string(&payload).context("serialize generated strategy config")?;
    tokio::fs::write(&path, content)
        .await
        .with_context(|| format!("write {}", path.display()))
}

fn generated_strategy_runtime_config(request: &CreateStrategyRequest) -> Value {
    let template_id = request.template_id.as_deref().unwrap_or_default();
    match request.strategy_kind.as_str() {
        "hedged_grid" => hedged_grid_runtime_config(request),
        "unified_arbitrage" => unified_arbitrage_runtime_config(request),
        _ if template_id == "hedged_grid" => hedged_grid_runtime_config(request),
        _ if template_id == "unified_arbitrage" => unified_arbitrage_runtime_config(request),
        _ => json!({
            "schema_version": CONTROL_API_SCHEMA_VERSION,
            "strategy_id": request.strategy_id,
            "strategy_kind": request.strategy_kind,
            "template_id": request.template_id,
            "run_mode": request.run_mode,
            "tenant_id": request.tenant_id,
            "exchange_accounts": request.exchange_accounts,
            "risk": request.risk,
            "params": request.params,
            "generated_at": Utc::now(),
            "generated_by": "rustcta-control-api",
        }),
    }
}

fn hedged_grid_runtime_config(request: &CreateStrategyRequest) -> Value {
    let execution_exchange = exchange_slot_exchange(&request.exchange_accounts, "execution");
    let base_exchange = execution_exchange
        .clone()
        .or_else(|| exchange_slot_exchange(&request.exchange_accounts, "base"))
        .or_else(|| exchange_slot_exchange(&request.exchange_accounts, "long"))
        .unwrap_or_else(|| "binance".to_string());
    let hedge_exchange = execution_exchange
        .or_else(|| exchange_slot_exchange(&request.exchange_accounts, "hedge"))
        .or_else(|| exchange_slot_exchange(&request.exchange_accounts, "short"))
        .unwrap_or_else(|| base_exchange.clone());
    let spacing_mode = param_string(&request.params, "grid_spacing_mode", "pct");
    let spacing_is_abs = spacing_mode == "abs";
    let spacing = param_f64(
        &request.params,
        "grid_spacing",
        param_f64(&request.params, "grid_spacing_pct", 0.25),
    );
    let grid_order_count = param_f64(
        &request.params,
        "grid_order_count",
        param_f64(&request.params, "grid_levels", 8.0),
    )
    .round()
    .max(1.0) as u64;
    let order_notional = param_f64(&request.params, "order_notional_usdt", 50.0);
    let max_inventory_quote = risk_string(&request.risk, "max_total_notional_usdt", "");
    let max_inventory_quote = if max_inventory_quote.is_empty() {
        (order_notional * grid_order_count as f64 * 2.0).to_string()
    } else {
        max_inventory_quote
    };
    let mut config = json!({
        "symbol": normalize_slash_symbol(&param_string(&request.params, "symbol", "BTC/USDT")),
        "spot_exchange": base_exchange,
        "hedge_exchange": hedge_exchange,
        "grid_spacing_bps": percent_to_bps(if spacing_is_abs { 0.25 } else { spacing }),
        "grid_spacing_mode": spacing_mode,
        "grid_order_count": grid_order_count,
        "order_notional_usdt": order_notional,
        "max_inventory_quote": max_inventory_quote,
        "dry_run": !run_mode_is_live(request),
        "enable_live_orders": run_mode_is_live(request),
        "execution_market_type": param_string(&request.params, "execution_market_type", "perpetual"),
        "execution_endpoint": param_string(&request.params, "execution_endpoint", "http://127.0.0.1:18081"),
    });
    if spacing_is_abs {
        config["grid_spacing_abs"] = json!(spacing);
    }
    config
}

fn unified_arbitrage_runtime_config(request: &CreateStrategyRequest) -> Value {
    let venues = paired_exchanges(request, "long", "short", "binance", "okx");
    let target_notional = risk_f64(&request.risk, "max_single_order_usdt", 50.0);
    let open_spread = percent_to_ratio(param_f64(&request.params, "min_open_raw_spread_pct", 0.12));
    let open_edge = percent_to_ratio(param_f64(&request.params, "min_open_net_edge_pct", 0.04));
    let close_profit =
        percent_to_ratio(param_f64(&request.params, "close_min_net_profit_pct", 0.03));
    let symbols = symbols_from_params(&request.params);
    let first_symbol = symbols
        .first()
        .cloned()
        .unwrap_or_else(|| "BTCUSDT".to_string());
    json!({
        "strategy_kind": "unified_arbitrage",
        "mode": if run_mode_is_live(request) { "live" } else { "observe" },
        "trading_mode": if run_mode_is_live(request) { "live" } else { "paper" },
        "enable_live_trading": run_mode_is_live(request),
        "market": {
            "quote_asset": "USDT",
            "stale_book_ms": risk_u64(&request.risk, "max_book_age_ms", 800),
            "funding_snapshot_max_age_ms": risk_u64(&request.risk, "max_book_age_ms", 800).max(5_000),
        },
        "defaults": {
            "sizing": {
                "min_order_notional_usdt": risk_f64(&request.risk, "min_single_order_usdt", 5.0),
                "max_order_notional_usdt": target_notional,
                "max_position_notional_usdt": risk_f64(&request.risk, "max_total_notional_usdt", 1000.0),
                "max_route_notional_usdt": risk_f64(&request.risk, "max_total_notional_usdt", 1000.0),
                "max_symbol_notional_usdt": risk_f64(&request.risk, "max_total_notional_usdt", 1000.0),
                "max_exchange_notional_usdt": risk_f64(&request.risk, "max_total_notional_usdt", 1000.0),
                "max_total_notional_usdt": risk_f64(&request.risk, "max_total_notional_usdt", 1000.0),
            },
            "thresholds": {
                "min_open_spread_bps": open_spread * 10_000.0,
                "min_open_net_edge_bps": open_edge * 10_000.0,
                "close_spread_bps": close_profit * 10_000.0,
            },
            "execution": {
                "preferred_open_style": "dual_taker",
                "preferred_close_style": "dual_taker_reduce_only",
                "taker_slippage_bps": percent_to_bps(risk_f64(&request.risk, "max_slippage_pct", 0.08)),
            },
            "risk": {
                "max_open_bundles": 10,
                "max_positions_per_exchange": 10,
                "max_active_bundles_per_symbol": 1,
            },
        },
        "routes": [{
            "route_id": "route_1",
            "enabled": true,
            "kind": "perp_perp_spread",
            "symbol": normalize_slash_symbol(&first_symbol),
            "legs": {
                "long": {
                    "exchange": venues.first().cloned().unwrap_or_else(|| "binance".to_string()),
                    "market_type": "perpetual",
                    "side_on_open": "buy",
                    "position_side": "long"
                },
                "short": {
                    "exchange": venues.get(1).cloned().unwrap_or_else(|| "okx".to_string()),
                    "market_type": "perpetual",
                    "side_on_open": "sell",
                    "position_side": "short"
                }
            }
        }],
    })
}

fn run_mode_is_live(request: &CreateStrategyRequest) -> bool {
    request
        .run_mode
        .as_deref()
        .map(|mode| mode.eq_ignore_ascii_case("live"))
        .unwrap_or(false)
}

fn paired_exchanges(
    request: &CreateStrategyRequest,
    left_slot: &str,
    right_slot: &str,
    default_left: &str,
    default_right: &str,
) -> Vec<String> {
    let mut venues = Vec::new();
    venues.push(
        exchange_slot_exchange(&request.exchange_accounts, left_slot)
            .unwrap_or_else(|| default_left.to_string()),
    );
    venues.push(
        exchange_slot_exchange(&request.exchange_accounts, right_slot)
            .unwrap_or_else(|| default_right.to_string()),
    );
    venues.sort();
    venues.dedup();
    venues
}

fn exchange_slot_exchange(accounts: &Value, slot: &str) -> Option<String> {
    accounts.get(slot).and_then(exchange_account_value_exchange)
}

fn exchange_account_value_exchange(value: &Value) -> Option<String> {
    let raw = value.as_str()?.trim();
    if raw.is_empty() {
        return None;
    }
    raw.split(':')
        .next()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(normalize_exchange_id)
}

fn symbols_from_params(params: &Value) -> Vec<String> {
    param_string(params, "symbols", "BTC/USDT")
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(normalize_slash_symbol)
        .collect::<Vec<_>>()
}

fn normalize_slash_symbol(symbol: &str) -> String {
    let symbol = symbol.trim().to_ascii_uppercase();
    if symbol.is_empty() {
        return symbol;
    }
    let compact = symbol
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .collect::<String>();
    for quote in ["USDT", "USDC", "USD", "BTC", "ETH"] {
        if let Some(base) = compact.strip_suffix(quote) {
            if !base.is_empty() {
                return format!("{base}/{quote}");
            }
        }
    }
    symbol.replace('-', "/").replace('_', "/")
}

fn normalize_exchange_id(exchange: &str) -> String {
    match exchange
        .trim()
        .to_ascii_lowercase()
        .replace(['-', '_', ' '], "")
        .as_str()
    {
        "binance" | "binancespot" | "binanceusdm" => "binance".to_string(),
        "okx" | "okex" => "okx".to_string(),
        "bybit" | "bybitunified" => "bybit".to_string(),
        "bitget" => "bitget".to_string(),
        "gate" | "gateio" => "gateio".to_string(),
        value => value.to_string(),
    }
}

fn risk_string(risk: &Value, field: &str, default: &str) -> String {
    value_string(risk, field).unwrap_or_else(|| default.to_string())
}

fn param_string(params: &Value, field: &str, default: &str) -> String {
    value_string(params, field).unwrap_or_else(|| default.to_string())
}

fn value_string(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(|value| {
            value
                .as_str()
                .map(ToString::to_string)
                .or_else(|| value.as_f64().map(|number| number.to_string()))
        })
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn risk_f64(risk: &Value, field: &str, default: f64) -> f64 {
    value_f64(risk, field).unwrap_or(default)
}

fn param_f64(params: &Value, field: &str, default: f64) -> f64 {
    value_f64(params, field).unwrap_or(default)
}

fn value_f64(value: &Value, field: &str) -> Option<f64> {
    value.get(field).and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_str()?.trim().parse::<f64>().ok())
    })
}

fn risk_u64(risk: &Value, field: &str, default: u64) -> u64 {
    risk.get(field)
        .and_then(|value| {
            value
                .as_u64()
                .or_else(|| value.as_str()?.trim().parse::<u64>().ok())
        })
        .unwrap_or(default)
}

fn percent_to_ratio(value: f64) -> f64 {
    value / 100.0
}

fn percent_to_bps(value: f64) -> f64 {
    value * 100.0
}

async fn local_strategy_command(
    State(state): State<ControlApiState>,
    AxumPath(id): AxumPath<String>,
    Json(mut command): Json<LifecycleCommandRecord>,
) -> (StatusCode, Json<Value>) {
    if command.strategy_id != id {
        return local_json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "strategy_id_mismatch",
                "local_path_exposed": false,
            }),
        );
    }
    if let Some(response) = validate_local_command_boundary(&state, &mut command).await {
        return response;
    }

    if state.record_command(command.clone()).await.is_err() {
        return local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "audit_write_failed",
                "local_path_exposed": false,
            }),
        );
    }

    match state.apply_strategy_command(command.clone()).await {
        Ok((_, applied_to_runtime)) => local_json_response(
            StatusCode::OK,
            local_command_accepted_payload(&command, applied_to_runtime),
        ),
        Err(_) => local_json_response(
            StatusCode::NOT_FOUND,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "strategy_not_found",
                "command_id": command.command_id,
                "local_path_exposed": false,
            }),
        ),
    }
}

async fn local_command(
    State(state): State<ControlApiState>,
    Json(mut command): Json<LifecycleCommandRecord>,
) -> (StatusCode, Json<Value>) {
    if let Some(response) = validate_local_command_boundary(&state, &mut command).await {
        return response;
    }

    if state.record_command(command.clone()).await.is_err() {
        return local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "audit_write_failed",
                "local_path_exposed": false,
            }),
        );
    }

    local_json_response(
        StatusCode::OK,
        local_command_accepted_payload(&command, false),
    )
}

async fn validate_local_command_boundary(
    state: &ControlApiState,
    command: &mut LifecycleCommandRecord,
) -> Option<(StatusCode, Json<Value>)> {
    if !state.audit_ledger_configured() {
        return Some(local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "audit_ledger_not_configured",
                "local_path_exposed": false,
            }),
        ));
    }
    if command.validate().is_err() {
        return Some(local_json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "schema_version": CONTROL_API_SCHEMA_VERSION,
                "accepted": false,
                "status": "invalid_command",
                "local_path_exposed": false,
            }),
        ));
    }
    command.schema_version = CONTROL_API_SCHEMA_VERSION;
    None
}

fn local_command_accepted_payload(
    command: &LifecycleCommandRecord,
    applied_to_runtime: bool,
) -> Value {
    json!({
        "schema_version": CONTROL_API_SCHEMA_VERSION,
        "accepted": true,
        "command_id": command.command_id,
        "strategy_id": command.strategy_id,
        "would_submit_order": false,
        "applied_to_runtime": applied_to_runtime,
        "local_path_exposed": false,
    })
}

async fn local_agent_status(State(state): State<LocalSideEffectState>) -> Json<Value> {
    Json(json!({
        "schema_version": 1,
        "configured": true,
        "agent_id": state.agent.agent_id,
        "tenant_id": state.agent.tenant_id,
        "capabilities": state.agent.capabilities,
        "side_effects": {
            "strategy_config_edit": local_file_status(state.config.strategy_config_path.as_ref()).await,
            "command_queue": local_file_status(state.config.command_queue_path.as_ref()).await,
            "balance_history": local_file_status(state.config.balance_history_path.as_ref()).await,
            "strategy_profit_history": local_file_status(state.config.strategy_profit_history_path.as_ref()).await,
            "restart_script": local_restart_script_status(state.config.restart_script_path.as_ref()).await,
        },
        "local_path_exposed": false,
    }))
}

async fn local_agent_audit(State(state): State<LocalSideEffectState>) -> Json<Value> {
    let events = state.audit.lock().await.clone();
    Json(json!({
        "schema_version": 1,
        "status": "ok",
        "events": events,
        "local_path_exposed": false,
    }))
}

async fn local_agent_strategy_config_draft(
    State(state): State<LocalSideEffectState>,
) -> (StatusCode, Json<Value>) {
    let Some(path) = state.config.strategy_config_path.as_ref() else {
        return local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "configured": false,
                "status": "strategy_config_not_configured",
                "path": "local-agent:strategy-config",
                "content": "",
                "local_path_exposed": false,
            }),
        );
    };

    match tokio::fs::read_to_string(path).await {
        Ok(content) => local_json_response(
            StatusCode::OK,
            json!({
                "schema_version": 1,
                "configured": true,
                "status": "ok",
                "path": "local-agent:strategy-config",
                "content": content,
                "local_path_exposed": false,
            }),
        ),
        Err(error) => local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "configured": true,
                "status": "read_failed",
                "read_error": local_io_error_kind(&error),
                "path": "local-agent:strategy-config",
                "content": "",
                "local_path_exposed": false,
            }),
        ),
    }
}

async fn local_agent_strategy_config(
    State(state): State<LocalSideEffectState>,
    Json(payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let operation_id = local_operation_id();
    let Some(path) = state.config.strategy_config_path.as_ref() else {
        return local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "strategy_config_not_configured",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    };
    let Some(content) = payload.get("content").and_then(Value::as_str) else {
        return local_json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "missing_content",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    };
    if serde_yaml::from_str::<serde_yaml::Value>(content).is_err() {
        return local_json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "invalid_yaml",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    }

    let strategy_id = payload
        .get("strategy_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("default")
        .to_string();
    let restart_requested = payload
        .get("restart")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if !state.control.audit_ledger_configured() {
        return local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "audit_ledger_not_configured",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    }
    if state
        .control
        .record_operator_audit(
            "local_agent_strategy_config_edit",
            state.agent.agent_id.clone(),
            json!({
                "operation_id": operation_id,
                "strategy_id": strategy_id,
                "restart_requested": restart_requested,
                "restart_script_configured": state.config.restart_script_path.is_some(),
                "would_submit_order": false,
                "local_path_exposed": false,
            }),
        )
        .await
        .is_err()
    {
        return local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "audit_write_failed",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    }

    if atomic_write_local_file(path, content, &operation_id)
        .await
        .is_err()
    {
        return local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "write_failed",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    }

    let mut restart_queued = false;
    if restart_requested {
        let command = json!({
            "schema_version": 1,
            "operation_id": operation_id,
            "command": "restart_strategy",
            "strategy_id": strategy_id,
            "source": "local_agent",
            "accepted_at": Utc::now(),
            "restart_script_configured": state.config.restart_script_path.is_some(),
            "restart_script_executed": false,
        });
        restart_queued = append_local_jsonl(state.config.command_queue_path.as_ref(), &command)
            .await
            .is_ok();
    }

    let audit = json!({
        "schema_version": 1,
        "operation_id": operation_id,
        "action": "strategy_config_edit",
        "accepted": true,
        "status": "accepted",
        "strategy_id": strategy_id,
        "restart_requested": restart_requested,
        "restart_queued": restart_queued,
        "restart_script_configured": state.config.restart_script_path.is_some(),
        "restart_script_executed": false,
        "recorded_at": Utc::now(),
        "local_path_exposed": false,
    });
    state.audit.lock().await.push(audit.clone());

    local_json_response(
        StatusCode::ACCEPTED,
        json!({
            "schema_version": 1,
            "accepted": true,
            "status": "accepted",
            "operation_id": operation_id,
            "audit": audit,
            "restart": {
                "requested": restart_requested,
                "queued": restart_queued,
                "restart_script_configured": state.config.restart_script_path.is_some(),
                "restart_script_executed": false,
            },
            "local_path_exposed": false,
        }),
    )
}

async fn local_agent_unified_arb_exchanges(
    State(state): State<LocalSideEffectState>,
) -> (StatusCode, Json<Value>) {
    let operation_id = local_operation_id();
    match load_unified_arb_exchange_config(&state, &operation_id, true).await {
        Ok(view) => local_json_response(
            StatusCode::OK,
            unified_arb_exchange_config_response(&view, &operation_id, false, false),
        ),
        Err(response) => response,
    }
}

async fn local_agent_unified_arb_settings(
    State(state): State<LocalSideEffectState>,
) -> (StatusCode, Json<Value>) {
    let operation_id = local_operation_id();
    match load_unified_arb_exchange_config(&state, &operation_id, true).await {
        Ok(view) => local_json_response(
            StatusCode::OK,
            unified_arb_settings_response(&view, &operation_id, false, false),
        ),
        Err(response) => response,
    }
}

async fn local_agent_save_unified_arb_settings(
    State(state): State<LocalSideEffectState>,
    Json(request): Json<UnifiedArbSettingsUpdateRequest>,
) -> (StatusCode, Json<Value>) {
    let operation_id = local_operation_id();
    if request.apply && state.config.command_queue_path.is_none() {
        return local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "command_queue_not_configured",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    }
    let mut view = match load_unified_arb_exchange_config(&state, &operation_id, true).await {
        Ok(view) => view,
        Err(response) => return response,
    };
    let strategy_id = request
        .strategy_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(UNIFIED_ARB_STRATEGY_ID)
        .to_string();
    apply_unified_arb_settings_update(&mut view.root, &request);
    let (exchange_map, cleaned_invalid_count, removed_exchanges) =
        sanitize_unified_arb_exchange_map(view.root.get("exchanges"));
    view.exchange_map = exchange_map;
    view.exchanges = exchange_rows_from_map(&view.exchange_map);
    view.enabled_exchanges = enabled_exchanges_from_root_or_map(&view.root, &view.exchange_map);
    view.cleaned_invalid_count += cleaned_invalid_count;
    view.removed_exchanges.extend(removed_exchanges);
    sync_unified_arb_exchanges_into_root(
        &mut view.root,
        &view.exchange_map,
        &view.enabled_exchanges,
    );

    match persist_unified_arb_config(
        &state,
        &view,
        &operation_id,
        &strategy_id,
        "local_agent_unified_arb_settings_save",
        request.apply,
        "update_unified_arb_settings",
        unified_arb_settings_command_payload(&view, &request, &strategy_id),
    )
    .await
    {
        Ok(realtime_queued) => local_json_response(
            StatusCode::ACCEPTED,
            unified_arb_settings_response(&view, &operation_id, true, realtime_queued),
        ),
        Err(response) => response,
    }
}

async fn local_agent_save_unified_arb_exchanges(
    State(state): State<LocalSideEffectState>,
    Json(request): Json<UnifiedArbExchangeConfigUpdateRequest>,
) -> (StatusCode, Json<Value>) {
    let operation_id = local_operation_id();
    if request.apply && state.config.command_queue_path.is_none() {
        return local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "command_queue_not_configured",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    }
    let mut view = match load_unified_arb_exchange_config(&state, &operation_id, true).await {
        Ok(view) => view,
        Err(response) => return response,
    };
    let strategy_id = request
        .strategy_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(UNIFIED_ARB_STRATEGY_ID)
        .to_string();

    let mut next = if request.replace {
        Map::<String, Value>::new()
    } else {
        view.exchange_map.clone()
    };
    let mut invalid_rows = 0usize;
    for row in &request.exchanges {
        let exchange_hint = row
            .get("exchange")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let existing = storage_exchange_key(exchange_hint)
            .and_then(|key| view.exchange_map.get(&key))
            .or_else(|| {
                row.get("exchange")
                    .and_then(Value::as_str)
                    .and_then(|exchange| find_exchange_config(&view.exchange_map, exchange))
            });
        if let Some((key, config)) =
            normalize_unified_arb_exchange_row(exchange_hint, row, existing)
        {
            next.insert(key, config);
        } else {
            invalid_rows += 1;
        }
    }
    view.exchange_map = next;
    view.exchanges = exchange_rows_from_map(&view.exchange_map);
    view.enabled_exchanges = enabled_exchanges_from_map(&view.exchange_map);
    view.cleaned_invalid_count += invalid_rows;
    sync_unified_arb_exchanges_into_root(
        &mut view.root,
        &view.exchange_map,
        &view.enabled_exchanges,
    );

    match persist_unified_arb_config(
        &state,
        &view,
        &operation_id,
        &strategy_id,
        "local_agent_unified_arb_exchange_config_save",
        request.apply,
        "update_unified_arb_exchange_config",
        json!({
            "strategy_id": strategy_id,
            "enabled_exchanges": view.enabled_exchanges,
            "exchanges": view.exchanges,
        }),
    )
    .await
    {
        Ok(realtime_queued) => local_json_response(
            StatusCode::ACCEPTED,
            unified_arb_exchange_config_response(&view, &operation_id, true, realtime_queued),
        ),
        Err(response) => response,
    }
}

async fn local_agent_delete_unified_arb_exchange(
    State(state): State<LocalSideEffectState>,
    AxumPath(exchange): AxumPath<String>,
) -> (StatusCode, Json<Value>) {
    let operation_id = local_operation_id();
    if state.config.command_queue_path.is_none() {
        return local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "command_queue_not_configured",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    }
    let mut view = match load_unified_arb_exchange_config(&state, &operation_id, true).await {
        Ok(view) => view,
        Err(response) => return response,
    };
    let Some(remove_key) = find_exchange_config_key(&view.exchange_map, &exchange) else {
        return local_json_response(
            StatusCode::OK,
            json!({
                "schema_version": 1,
                "accepted": true,
                "status": "not_found",
                "operation_id": operation_id,
                "removed_exchange": exchange,
                "exchanges": view.exchanges,
                "enabled_exchanges": view.enabled_exchanges,
                "local_path_exposed": false,
            }),
        );
    };
    view.exchange_map.remove(&remove_key);
    view.removed_exchanges.push(remove_key.clone());
    view.exchanges = exchange_rows_from_map(&view.exchange_map);
    view.enabled_exchanges = enabled_exchanges_from_map(&view.exchange_map);
    sync_unified_arb_exchanges_into_root(
        &mut view.root,
        &view.exchange_map,
        &view.enabled_exchanges,
    );

    match persist_unified_arb_config(
        &state,
        &view,
        &operation_id,
        UNIFIED_ARB_STRATEGY_ID,
        "local_agent_unified_arb_exchange_config_delete",
        true,
        "update_unified_arb_exchange_config",
        json!({
            "strategy_id": UNIFIED_ARB_STRATEGY_ID,
            "enabled_exchanges": view.enabled_exchanges,
            "exchanges": view.exchanges,
        }),
    )
    .await
    {
        Ok(realtime_queued) => {
            let mut response =
                unified_arb_exchange_config_response(&view, &operation_id, true, realtime_queued);
            if let Some(map) = response.as_object_mut() {
                map.insert("removed_exchange".to_string(), Value::String(remove_key));
            }
            local_json_response(StatusCode::ACCEPTED, response)
        }
        Err(response) => response,
    }
}

#[derive(Debug, Clone)]
struct UnifiedArbExchangeConfigView {
    root: Value,
    exchange_map: Map<String, Value>,
    exchanges: Vec<Value>,
    enabled_exchanges: Vec<String>,
    cleaned_invalid_count: usize,
    removed_exchanges: Vec<String>,
    cleaned_on_read: bool,
}

async fn load_unified_arb_exchange_config(
    state: &LocalSideEffectState,
    operation_id: &str,
    persist_cleaned: bool,
) -> Result<UnifiedArbExchangeConfigView, (StatusCode, Json<Value>)> {
    let Some(path) = state.config.strategy_config_path.as_ref() else {
        return Err(local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "configured": false,
                "status": "strategy_config_not_configured",
                "operation_id": operation_id,
                "path": LOCAL_STRATEGY_CONFIG_REF,
                "local_path_exposed": false,
            }),
        ));
    };
    let content = match tokio::fs::read_to_string(path).await {
        Ok(content) => content,
        Err(error) => {
            return Err(local_json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({
                    "schema_version": 1,
                    "configured": true,
                    "status": "read_failed",
                    "read_error": local_io_error_kind(&error),
                    "operation_id": operation_id,
                    "path": LOCAL_STRATEGY_CONFIG_REF,
                    "local_path_exposed": false,
                }),
            ))
        }
    };
    let mut root = match serde_yaml::from_str::<Value>(&content) {
        Ok(value) => value,
        Err(_) => {
            return Err(local_json_response(
                StatusCode::BAD_REQUEST,
                json!({
                    "schema_version": 1,
                    "configured": true,
                    "status": "invalid_yaml",
                    "operation_id": operation_id,
                    "path": LOCAL_STRATEGY_CONFIG_REF,
                    "local_path_exposed": false,
                }),
            ))
        }
    };
    if !root.is_object() {
        return Err(local_json_response(
            StatusCode::BAD_REQUEST,
            json!({
                "schema_version": 1,
                "configured": true,
                "status": "invalid_strategy_config_root",
                "operation_id": operation_id,
                "path": LOCAL_STRATEGY_CONFIG_REF,
                "local_path_exposed": false,
            }),
        ));
    }

    let previous_root = root.clone();
    let (exchange_map, cleaned_invalid_count, removed_exchanges) =
        sanitize_unified_arb_exchange_map(root.get("exchanges"));
    let enabled_exchanges = enabled_exchanges_from_root_or_map(&root, &exchange_map);
    sync_unified_arb_exchanges_into_root(&mut root, &exchange_map, &enabled_exchanges);
    let cleaned_on_read = root != previous_root;
    if persist_cleaned && cleaned_on_read {
        if !state.control.audit_ledger_configured() {
            return Err(local_json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({
                    "schema_version": 1,
                    "configured": true,
                    "status": "audit_ledger_not_configured",
                    "operation_id": operation_id,
                    "path": LOCAL_STRATEGY_CONFIG_REF,
                    "local_path_exposed": false,
                }),
            ));
        }
        if state
            .control
            .record_operator_audit(
                "local_agent_unified_arb_exchange_config_clean",
                state.agent.agent_id.clone(),
                json!({
                    "operation_id": operation_id,
                    "cleaned_invalid_count": cleaned_invalid_count,
                    "removed_exchanges": removed_exchanges,
                    "would_submit_order": false,
                    "local_path_exposed": false,
                }),
            )
            .await
            .is_err()
        {
            return Err(local_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "schema_version": 1,
                    "configured": true,
                    "status": "audit_write_failed",
                    "operation_id": operation_id,
                    "path": LOCAL_STRATEGY_CONFIG_REF,
                    "local_path_exposed": false,
                }),
            ));
        }
        if write_strategy_yaml(path, &root, operation_id)
            .await
            .is_err()
        {
            return Err(local_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "schema_version": 1,
                    "configured": true,
                    "status": "write_failed",
                    "operation_id": operation_id,
                    "path": LOCAL_STRATEGY_CONFIG_REF,
                    "local_path_exposed": false,
                }),
            ));
        }
    }
    Ok(UnifiedArbExchangeConfigView {
        root,
        exchanges: exchange_rows_from_map(&exchange_map),
        exchange_map,
        enabled_exchanges,
        cleaned_invalid_count,
        removed_exchanges,
        cleaned_on_read,
    })
}

async fn persist_unified_arb_config(
    state: &LocalSideEffectState,
    view: &UnifiedArbExchangeConfigView,
    operation_id: &str,
    strategy_id: &str,
    action: &str,
    apply: bool,
    command_name: &str,
    command_payload: Value,
) -> Result<bool, (StatusCode, Json<Value>)> {
    let Some(path) = state.config.strategy_config_path.as_ref() else {
        return Err(local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "strategy_config_not_configured",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        ));
    };
    if !state.control.audit_ledger_configured() {
        return Err(local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "audit_ledger_not_configured",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        ));
    }
    if state
        .control
        .record_operator_audit(
            action,
            state.agent.agent_id.clone(),
            json!({
                "operation_id": operation_id,
                "strategy_id": strategy_id,
                "exchange_count": view.exchanges.len(),
                "enabled_exchanges": view.enabled_exchanges,
                "removed_exchanges": view.removed_exchanges,
                "trading_enabled": command_payload
                    .get("execution")
                    .and_then(|execution| execution.get("trading_enabled"))
                    .cloned()
                    .unwrap_or(Value::Null),
                "control_action": if command_payload
                    .get("execution")
                    .and_then(|execution| execution.get("trading_enabled"))
                    .is_some()
                {
                    Value::String("control_live_trading_toggle".to_string())
                } else {
                    Value::Null
                },
                "category": if command_payload
                    .get("execution")
                    .and_then(|execution| execution.get("trading_enabled"))
                    .is_some()
                {
                    Value::String("control".to_string())
                } else {
                    Value::Null
                },
                "would_submit_order": false,
                "local_path_exposed": false,
            }),
        )
        .await
        .is_err()
    {
        return Err(local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "audit_write_failed",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        ));
    }
    if write_strategy_yaml(path, &view.root, operation_id)
        .await
        .is_err()
    {
        return Err(local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "write_failed",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        ));
    }

    if !apply {
        return Ok(false);
    }
    let command = json!({
        "schema_version": 1,
        "operation_id": operation_id,
        "command": command_name,
        "strategy_id": strategy_id,
        "payload": command_payload,
        "source": "local_agent",
        "accepted_at": Utc::now(),
    });
    if append_local_jsonl(state.config.command_queue_path.as_ref(), &command)
        .await
        .is_err()
    {
        return Err(local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "command_queue_not_configured",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        ));
    }
    Ok(true)
}

fn unified_arb_exchange_config_response(
    view: &UnifiedArbExchangeConfigView,
    operation_id: &str,
    accepted: bool,
    realtime_queued: bool,
) -> Value {
    json!({
        "schema_version": 1,
        "configured": true,
        "accepted": accepted,
        "status": "ok",
        "operation_id": operation_id,
        "path": LOCAL_STRATEGY_CONFIG_REF,
        "strategy_id": UNIFIED_ARB_STRATEGY_ID,
        "exchanges": view.exchanges,
        "enabled_exchanges": view.enabled_exchanges,
        "cleaned_on_read": view.cleaned_on_read,
        "cleaned_invalid_count": view.cleaned_invalid_count,
        "removed_exchanges": view.removed_exchanges,
        "realtime": {
            "requested": accepted,
            "queued": realtime_queued,
        },
        "local_path_exposed": false,
    })
}

fn unified_arb_settings_response(
    view: &UnifiedArbExchangeConfigView,
    operation_id: &str,
    accepted: bool,
    realtime_queued: bool,
) -> Value {
    let settings = unified_arb_settings_view(&view.root, &view.enabled_exchanges);
    json!({
        "schema_version": 1,
        "configured": true,
        "accepted": accepted,
        "status": "ok",
        "operation_id": operation_id,
        "path": LOCAL_STRATEGY_CONFIG_REF,
        "strategy_id": UNIFIED_ARB_STRATEGY_ID,
        "settings": settings,
        "realtime": {
            "requested": accepted,
            "queued": realtime_queued,
        },
        "local_path_exposed": false,
    })
}

fn unified_arb_settings_view(root: &Value, enabled_exchanges: &[String]) -> Value {
    let thresholds = root.get("thresholds").unwrap_or(&Value::Null);
    let dual_taker = root.get("dual_taker").unwrap_or(&Value::Null);
    let slippage_capture = root.get("slippage_capture").unwrap_or(&Value::Null);
    let execution_quality = root.get("execution_quality").unwrap_or(&Value::Null);
    let sizing = root.get("sizing").unwrap_or(&Value::Null);
    let risk = root.get("risk").unwrap_or(&Value::Null);
    let universe = root.get("universe").unwrap_or(&Value::Null);
    let controls = root.get("controls").unwrap_or(&Value::Null);
    let execution = root.get("execution").unwrap_or(&Value::Null);
    let symbols = root
        .get("enabled_symbols")
        .and_then(Value::as_array)
        .filter(|symbols| !symbols.is_empty())
        .or_else(|| universe.get("symbols").and_then(Value::as_array))
        .cloned()
        .unwrap_or_default();
    json!({
        "enabled_exchanges": enabled_exchanges,
        "symbols": symbols,
        "target_symbol_count": symbols.len(),
        "target_notional_usdt": number_from_paths(
            &[root, sizing, dual_taker],
            &["max_live_notional_per_trade", "target_notional_usdt"],
        ),
        "max_notional_usdt": number_from_paths(&[sizing], &["max_notional_usdt"]),
        "max_open_bundles": number_from_paths(&[risk, dual_taker], &["max_open_bundles"]),
        "max_positions_per_exchange": number_from_paths(
            &[sizing, dual_taker],
            &["max_positions_per_exchange"],
        ),
        "min_open_raw_spread": number_from_paths(
            &[thresholds, dual_taker, execution_quality],
            &["min_open_raw_spread", "min_open_spread_pct", "min_open_raw_spread_pct"],
        ),
        "min_open_spread_pct": number_from_paths(
            &[thresholds, dual_taker, execution_quality],
            &["min_open_raw_spread", "min_open_spread_pct", "min_open_raw_spread_pct"],
        ),
        "min_open_maker_taker_net_edge": number_from_paths(
            &[thresholds, dual_taker, execution_quality],
            &[
                "min_open_maker_taker_net_edge",
                "min_open_net_profit_pct",
                "min_open_net_edge_pct",
            ],
        ),
        "min_open_net_profit_pct": number_from_paths(
            &[thresholds, dual_taker, execution_quality],
            &[
                "min_open_maker_taker_net_edge",
                "min_open_net_profit_pct",
                "min_open_net_edge_pct",
            ],
        ),
        "close_min_net_profit_pct": number_from_paths(
            &[thresholds, dual_taker, execution_quality],
            &["close_min_net_profit_pct", "lock_profit_dual_taker_pct", "min_close_net_profit_pct"],
        ),
        "lock_profit_dual_taker_pct": number_from_paths(
            &[thresholds, dual_taker, execution_quality],
            &["close_min_net_profit_pct", "lock_profit_dual_taker_pct", "min_close_net_profit_pct"],
        ),
        "expected_close_spread_pct": number_from_paths(
            &[dual_taker, thresholds],
            &["expected_close_spread_pct", "max_close_spread_pct"],
        ),
        "max_close_spread_pct": number_from_paths(
            &[thresholds, dual_taker],
            &["max_close_spread_pct", "expected_close_spread_pct"],
        ),
        "min_open_executable_depth_ratio": number_from_paths(
            &[execution_quality],
            &["min_open_executable_depth_ratio"],
        ),
        "mode": string_from_paths(&[root], &["mode"]),
        "execution_module": string_from_paths(
            &[root, execution],
            &["execution_module", "open_execution_style"],
        ),
        "execution_profile": string_from_paths(&[root], &["execution_profile"]),
        "slippage_capture": {
            "target_notional_usdt": number_from_paths(
                &[slippage_capture, sizing, root],
                &["target_notional_usdt", "max_live_notional_per_trade"],
            ),
            "min_open_spread_pct": number_from_paths(
                &[slippage_capture, thresholds, execution_quality],
                &["min_open_spread_pct", "min_open_raw_spread", "min_open_raw_spread_pct"],
            ),
            "min_open_net_profit_pct": number_from_paths(
                &[slippage_capture, thresholds, execution_quality],
                &["min_open_net_profit_pct", "min_open_maker_taker_net_edge", "min_open_net_edge_pct"],
            ),
            "maker_price_offset_pct": number_from_paths(
                &[slippage_capture, execution],
                &["maker_price_offset_pct", "slippage_capture_offset_pct"],
            ),
            "maker_order_timeout_ms": number_from_paths(
                &[slippage_capture, execution],
                &["maker_order_timeout_ms", "maker_order_ttl_ms"],
            ),
            "hedge_taker_slippage_pct": number_from_paths(
                &[slippage_capture, execution],
                &["hedge_taker_slippage_pct", "taker_ioc_slippage_limit_pct"],
            ),
            "close_taker_slippage_pct": number_from_paths(
                &[slippage_capture],
                &["close_taker_slippage_pct"],
            ),
            "close_min_net_profit_pct": number_from_paths(
                &[slippage_capture, thresholds, execution_quality],
                &["close_min_net_profit_pct", "lock_profit_dual_taker_pct", "min_close_net_profit_pct"],
            ),
            "cancel_unfilled_maker": bool_from_paths(&[slippage_capture], &["cancel_unfilled_maker"]),
        },
        "execution": {
            "dry_run": bool_from_paths(&[root, execution], &["dry_run"]),
            "trading_enabled": bool_from_paths(&[execution, root], &["trading_enabled", "enable_live_trading"]),
            "live_orders_enabled": bool_from_paths(&[execution, root], &["trading_enabled", "enable_live_trading"]),
            "start_paused_new_entries": bool_from_paths(&[controls], &["start_paused_new_entries"]),
            "start_close_only": bool_from_paths(&[controls], &["start_close_only"]),
        }
    })
}

fn apply_unified_arb_settings_update(root: &mut Value, request: &UnifiedArbSettingsUpdateRequest) {
    if !root.is_object() {
        *root = json!({});
    }
    if let Some(symbols) = normalized_symbols(request.symbols.as_deref()) {
        set_root_array(root, "enabled_symbols", symbols.clone());
        set_nested_array(root, &["universe", "symbols"], symbols);
    }
    if let Some(target_symbol_count) = request.target_symbol_count {
        set_nested_number(
            root,
            &["settings", "target_symbol_count"],
            target_symbol_count as f64,
        );
    }
    if let Some(value) = sanitized_nonnegative(request.target_notional_usdt) {
        set_root_number(root, "max_live_notional_per_trade", value);
        set_nested_number(root, &["sizing", "target_notional_usdt"], value);
        set_nested_number(root, &["dual_taker", "target_notional_usdt"], value);
        set_nested_number(root, &["slippage_capture", "target_notional_usdt"], value);
    }
    if let Some(value) = sanitized_nonnegative(request.min_notional_usdt) {
        set_nested_number(root, &["sizing", "min_notional_usdt"], value);
    }
    if let Some(value) = sanitized_nonnegative(request.max_notional_usdt) {
        set_nested_number(root, &["sizing", "max_notional_usdt"], value);
    }
    let max_symbol_notional = sanitized_nonnegative(
        request
            .max_symbol_notional_usdt
            .or(request.max_notional_per_symbol_usdt),
    );
    if let Some(value) = max_symbol_notional {
        set_nested_number(root, &["sizing", "max_symbol_notional_usdt"], value);
        set_nested_number(root, &["risk", "max_notional_per_symbol_usdt"], value);
    }
    if let Some(value) = sanitized_nonnegative(request.max_notional_per_exchange_usdt) {
        set_nested_number(root, &["risk", "max_notional_per_exchange_usdt"], value);
    }
    if let Some(value) = sanitized_nonnegative(request.max_total_notional_usdt) {
        set_nested_number(root, &["risk", "max_total_notional_usdt"], value);
    }
    if let Some(value) = request
        .max_positions_per_exchange
        .filter(|value| *value > 0)
    {
        set_nested_number(
            root,
            &["sizing", "max_positions_per_exchange"],
            value as f64,
        );
        set_nested_number(
            root,
            &["dual_taker", "max_positions_per_exchange"],
            value as f64,
        );
        set_nested_number(
            root,
            &["slippage_capture", "max_positions_per_exchange"],
            value as f64,
        );
    }
    if let Some(value) = request.max_open_bundles.filter(|value| *value > 0) {
        set_nested_number(root, &["risk", "max_open_bundles"], value as f64);
        set_nested_number(root, &["dual_taker", "max_open_bundles"], value as f64);
        set_nested_number(
            root,
            &["slippage_capture", "max_open_bundles"],
            value as f64,
        );
    }
    if let Some(value) = request.max_open_positions.filter(|value| *value > 0) {
        set_nested_number(root, &["risk", "max_open_positions"], value as f64);
    }
    let open_raw =
        sanitized_nonnegative(request.min_open_raw_spread.or(request.min_open_spread_pct));
    if let Some(value) = open_raw {
        set_nested_number(root, &["thresholds", "min_open_raw_spread"], value);
        set_nested_number(root, &["dual_taker", "min_open_spread_pct"], value);
        set_nested_number(root, &["slippage_capture", "min_open_spread_pct"], value);
        set_nested_number(
            root,
            &["execution_quality", "min_open_raw_spread_pct"],
            value,
        );
    }
    let open_net = sanitized_nonnegative(
        request
            .min_open_maker_taker_net_edge
            .or(request.min_open_net_profit_pct)
            .or(request.min_open_net_edge_pct),
    );
    if let Some(value) = open_net {
        set_nested_number(
            root,
            &["thresholds", "min_open_maker_taker_net_edge"],
            value,
        );
        set_nested_number(root, &["dual_taker", "min_open_net_profit_pct"], value);
        set_nested_number(
            root,
            &["slippage_capture", "min_open_net_profit_pct"],
            value,
        );
        set_nested_number(root, &["execution_quality", "min_open_net_edge_pct"], value);
    }
    if let Some(value) = sanitized_nonnegative(request.min_open_executable_depth_ratio) {
        set_nested_number(
            root,
            &["execution_quality", "min_open_executable_depth_ratio"],
            value.max(1.0),
        );
    }
    let close_profit = sanitized_nonnegative(
        request
            .close_min_net_profit_pct
            .or(request.lock_profit_dual_taker_pct),
    );
    if let Some(value) = close_profit {
        set_nested_number(root, &["thresholds", "close_min_net_profit_pct"], value);
        set_nested_number(root, &["thresholds", "lock_profit_dual_taker_pct"], value);
        set_nested_number(root, &["dual_taker", "close_min_net_profit_pct"], value);
        set_nested_number(
            root,
            &["slippage_capture", "close_min_net_profit_pct"],
            value,
        );
        set_nested_number(
            root,
            &["execution_quality", "min_close_net_profit_pct"],
            value,
        );
    }
    let close_spread = sanitized_nonnegative(
        request
            .expected_close_spread_pct
            .or(request.max_close_spread_pct),
    );
    if let Some(value) = close_spread {
        set_nested_number(root, &["dual_taker", "expected_close_spread_pct"], value);
        set_nested_number(root, &["thresholds", "expected_close_spread_pct"], value);
        set_nested_number(root, &["thresholds", "max_close_spread_pct"], value);
    }
    if let Some(profile) = request
        .execution_module
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "-")
    {
        set_root_string(root, "execution_module", profile);
        set_nested_string(root, &["execution", "open_execution_style"], profile);
    }
    if let Some(value) = sanitized_nonnegative(request.maker_price_offset_pct) {
        set_nested_number(root, &["slippage_capture", "maker_price_offset_pct"], value);
        set_nested_number(root, &["execution", "slippage_capture_offset_pct"], value);
    }
    if let Some(value) = request.maker_order_timeout_ms.filter(|value| *value > 0) {
        set_nested_number(
            root,
            &["slippage_capture", "maker_order_timeout_ms"],
            value as f64,
        );
        set_nested_number(root, &["execution", "maker_order_ttl_ms"], value as f64);
        set_nested_number(root, &["execution", "single_leg_timeout_ms"], value as f64);
    }
    if let Some(value) = sanitized_nonnegative(request.hedge_taker_slippage_pct) {
        set_nested_number(
            root,
            &["slippage_capture", "hedge_taker_slippage_pct"],
            value,
        );
        set_nested_number(root, &["execution", "taker_ioc_slippage_limit_pct"], value);
    }
    if let Some(value) = sanitized_nonnegative(request.close_taker_slippage_pct) {
        set_nested_number(
            root,
            &["slippage_capture", "close_taker_slippage_pct"],
            value,
        );
    }
    if let Some(profile) = request
        .execution_profile
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "-")
    {
        set_root_string(root, "execution_profile", profile);
    }
    if let Some(value) = request.trading_enabled {
        set_nested_bool(root, &["execution", "trading_enabled"], value);
    }
}

fn unified_arb_settings_command_payload(
    view: &UnifiedArbExchangeConfigView,
    request: &UnifiedArbSettingsUpdateRequest,
    strategy_id: &str,
) -> Value {
    let mut payload = unified_arb_settings_view(&view.root, &view.enabled_exchanges);
    if let Some(object) = payload.as_object_mut() {
        object.insert(
            "strategy_id".to_string(),
            Value::String(strategy_id.to_string()),
        );
        object.insert("apply".to_string(), Value::Bool(request.apply));
    }
    payload
}

fn number_from_paths(objects: &[&Value], keys: &[&str]) -> Value {
    objects
        .iter()
        .find_map(|object| {
            keys.iter()
                .find_map(|key| object.get(*key).and_then(value_as_f64))
        })
        .filter(|value| value.is_finite())
        .map(Value::from)
        .unwrap_or(Value::Null)
}

fn string_from_paths(objects: &[&Value], keys: &[&str]) -> Value {
    objects
        .iter()
        .find_map(|object| {
            keys.iter().find_map(|key| {
                object
                    .get(*key)
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(|value| Value::String(value.to_string()))
            })
        })
        .unwrap_or(Value::Null)
}

fn bool_from_paths(objects: &[&Value], keys: &[&str]) -> Value {
    objects
        .iter()
        .find_map(|object| {
            keys.iter()
                .find_map(|key| object.get(*key).and_then(value_as_bool))
        })
        .map(Value::Bool)
        .unwrap_or(Value::Null)
}

fn value_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.trim().parse::<f64>().ok())
}

fn value_as_bool(value: &Value) -> Option<bool> {
    value.as_bool().or_else(|| {
        let text = value.as_str()?.trim().to_ascii_lowercase();
        match text.as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        }
    })
}

fn sanitized_nonnegative(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite() && *value >= 0.0)
}

fn normalized_symbols(symbols: Option<&[String]>) -> Option<Vec<Value>> {
    let symbols = symbols?
        .iter()
        .filter_map(|symbol| normalize_unified_arb_symbol(symbol))
        .fold(Vec::<String>::new(), |mut symbols, symbol| {
            if !symbols.contains(&symbol) {
                symbols.push(symbol);
            }
            symbols
        });
    Some(symbols.into_iter().map(Value::String).collect())
}

fn normalize_unified_arb_symbol(symbol: &str) -> Option<String> {
    let symbol = symbol.trim().to_ascii_uppercase();
    if symbol.is_empty() || symbol == "-" {
        return None;
    }
    let normalized = symbol.replace('-', "/");
    let (base, quote) = normalized.split_once('/')?;
    let base = base.trim();
    let quote = quote.trim();
    (!base.is_empty() && !quote.is_empty()).then(|| format!("{base}/{quote}"))
}

fn set_root_number(root: &mut Value, key: &str, value: f64) {
    if let Some(object) = root.as_object_mut() {
        object.insert(key.to_string(), Value::from(value));
    }
}

fn set_root_string(root: &mut Value, key: &str, value: &str) {
    if let Some(object) = root.as_object_mut() {
        object.insert(key.to_string(), Value::String(value.to_string()));
    }
}

fn set_root_array(root: &mut Value, key: &str, value: Vec<Value>) {
    if let Some(object) = root.as_object_mut() {
        object.insert(key.to_string(), Value::Array(value));
    }
}

fn set_nested_number(root: &mut Value, path: &[&str], value: f64) {
    if path.is_empty() {
        return;
    }
    if path.len() == 1 {
        set_root_number(root, path[0], value);
        return;
    }
    let object = ensure_object_path(root, &path[..path.len() - 1]);
    object.insert(path[path.len() - 1].to_string(), Value::from(value));
}

fn set_nested_string(root: &mut Value, path: &[&str], value: &str) {
    if path.is_empty() {
        return;
    }
    if path.len() == 1 {
        set_root_string(root, path[0], value);
        return;
    }
    let object = ensure_object_path(root, &path[..path.len() - 1]);
    object.insert(
        path[path.len() - 1].to_string(),
        Value::String(value.to_string()),
    );
}

fn set_nested_array(root: &mut Value, path: &[&str], value: Vec<Value>) {
    if path.is_empty() {
        return;
    }
    if path.len() == 1 {
        set_root_array(root, path[0], value);
        return;
    }
    let object = ensure_object_path(root, &path[..path.len() - 1]);
    object.insert(path[path.len() - 1].to_string(), Value::Array(value));
}

fn set_nested_bool(root: &mut Value, path: &[&str], value: bool) {
    if path.is_empty() {
        return;
    }
    if path.len() == 1 {
        if let Some(object) = root.as_object_mut() {
            object.insert(path[0].to_string(), Value::Bool(value));
        }
        return;
    }
    let object = ensure_object_path(root, &path[..path.len() - 1]);
    object.insert(path[path.len() - 1].to_string(), Value::Bool(value));
}

fn ensure_object_path<'a>(root: &'a mut Value, path: &[&str]) -> &'a mut Map<String, Value> {
    if !root.is_object() {
        *root = json!({});
    }
    let mut current = root.as_object_mut().expect("root object");
    for segment in path {
        let entry = current
            .entry((*segment).to_string())
            .or_insert_with(|| json!({}));
        if !entry.is_object() {
            *entry = json!({});
        }
        current = entry.as_object_mut().expect("nested object");
    }
    current
}

fn sanitize_unified_arb_exchange_map(
    value: Option<&Value>,
) -> (Map<String, Value>, usize, Vec<String>) {
    let mut cleaned_invalid_count = 0usize;
    let mut removed_exchanges = Vec::<String>::new();
    let mut map = Map::<String, Value>::new();
    match value {
        Some(Value::Object(rows)) => {
            for (exchange, row) in rows {
                match normalize_unified_arb_exchange_row(exchange, row, None) {
                    Some((key, config)) => {
                        map.insert(key, config);
                    }
                    None => {
                        cleaned_invalid_count += 1;
                        if !exchange.trim().is_empty() {
                            removed_exchanges.push(exchange.clone());
                        }
                    }
                }
            }
        }
        Some(Value::Array(rows)) => {
            for row in rows {
                let exchange = row
                    .get("exchange")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                match normalize_unified_arb_exchange_row(exchange, row, None) {
                    Some((key, config)) => {
                        map.insert(key, config);
                    }
                    None => cleaned_invalid_count += 1,
                }
            }
        }
        Some(Value::Null) | None => {}
        Some(_) => cleaned_invalid_count += 1,
    }
    (map, cleaned_invalid_count, removed_exchanges)
}

fn normalize_unified_arb_exchange_row(
    exchange_hint: &str,
    row: &Value,
    base: Option<&Value>,
) -> Option<(String, Value)> {
    let row_object = row.as_object()?;
    let exchange = row
        .get("exchange")
        .and_then(Value::as_str)
        .unwrap_or(exchange_hint);
    let key = storage_exchange_key(exchange)?;
    let mut object = base
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_else(Map::new);
    for (field, value) in row_object {
        if field == "exchange" {
            continue;
        }
        object.insert(field.clone(), value.clone());
    }
    normalize_unified_arb_exchange_fields(&key, &mut object);
    Some((key, Value::Object(object)))
}

fn normalize_unified_arb_exchange_fields(exchange: &str, object: &mut Map<String, Value>) {
    let enabled = object
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| {
            object
                .get("operating_mode")
                .and_then(Value::as_str)
                .map(|value| value.eq_ignore_ascii_case("enabled"))
                .unwrap_or(false)
        });
    object.insert("enabled".to_string(), Value::Bool(enabled));
    object.insert(
        "operating_mode".to_string(),
        Value::String(if enabled { "enabled" } else { "disabled" }.to_string()),
    );
    normalize_string_or_null(object, "account_id");
    normalize_string_or_null(object, "env_prefix");
    normalize_string_or_null(object, "private_rest_base_url");
    normalize_string_or_null(object, "private_ws_url");
    normalize_string_with_default(
        object,
        "position_mode",
        if matches!(exchange, "okx") {
            "net"
        } else {
            "hedge"
        },
    );
    normalize_bool_with_default(object, "private_rest_enabled", true);
    normalize_bool_with_default(object, "private_ws_enabled", true);
    normalize_bool_with_default(object, "demo_trading", false);
    normalize_u64_with_default(
        object,
        "ws_batch_size",
        if matches!(exchange, "bitget") { 50 } else { 80 },
    );
    if object
        .get("private_ws_run")
        .is_some_and(|value| !value.is_object())
    {
        object.remove("private_ws_run");
    }
    if object.get("routes").is_some_and(|value| !value.is_object()) {
        object.remove("routes");
    }
}

fn normalize_string_or_null(object: &mut Map<String, Value>, field: &str) {
    match object.get(field).and_then(Value::as_str).map(str::trim) {
        Some(value) if !value.is_empty() && value != "-" => {
            object.insert(field.to_string(), Value::String(value.to_string()));
        }
        _ => {
            object.insert(field.to_string(), Value::Null);
        }
    }
}

fn normalize_string_with_default(object: &mut Map<String, Value>, field: &str, default: &str) {
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "-")
        .unwrap_or(default);
    object.insert(field.to_string(), Value::String(value.to_string()));
}

fn normalize_bool_with_default(object: &mut Map<String, Value>, field: &str, default: bool) {
    let value = object
        .get(field)
        .and_then(Value::as_bool)
        .unwrap_or(default);
    object.insert(field.to_string(), Value::Bool(value));
}

fn normalize_u64_with_default(object: &mut Map<String, Value>, field: &str, default: u64) {
    let value = object.get(field).and_then(Value::as_u64).unwrap_or(default);
    object.insert(field.to_string(), Value::from(value));
}

fn sync_unified_arb_exchanges_into_root(
    root: &mut Value,
    exchange_map: &Map<String, Value>,
    enabled_exchanges: &[String],
) {
    let Some(root_object) = root.as_object_mut() else {
        return;
    };
    root_object.insert("exchanges".to_string(), Value::Object(exchange_map.clone()));
    root_object.insert(
        "enabled_exchanges".to_string(),
        Value::Array(
            enabled_exchanges
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    );
    if root_object.contains_key("venues") {
        root_object.insert(
            "venues".to_string(),
            Value::Array(
                enabled_exchanges
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
}

fn exchange_rows_from_map(exchange_map: &Map<String, Value>) -> Vec<Value> {
    exchange_map
        .iter()
        .map(|(exchange, config)| {
            let mut row = config.as_object().cloned().unwrap_or_else(Map::new);
            row.insert("exchange".to_string(), Value::String(exchange.clone()));
            Value::Object(row)
        })
        .collect()
}

fn enabled_exchanges_from_map(exchange_map: &Map<String, Value>) -> Vec<String> {
    exchange_map
        .iter()
        .filter_map(|(exchange, config)| {
            let enabled = config
                .get("enabled")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let operating_mode = config
                .get("operating_mode")
                .and_then(Value::as_str)
                .unwrap_or_default();
            (enabled || operating_mode.eq_ignore_ascii_case("enabled")).then(|| exchange.clone())
        })
        .collect()
}

fn enabled_exchanges_from_root_or_map(
    root: &Value,
    exchange_map: &Map<String, Value>,
) -> Vec<String> {
    let enabled_from_map = enabled_exchanges_from_map(exchange_map);
    let enabled_set = enabled_from_map
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let mut ordered = root
        .get("enabled_exchanges")
        .or_else(|| root.get("venues"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .filter_map(storage_exchange_key)
                .filter(|exchange| enabled_set.contains(exchange))
                .fold(Vec::<String>::new(), |mut rows, exchange| {
                    if !rows.contains(&exchange) {
                        rows.push(exchange);
                    }
                    rows
                })
        })
        .unwrap_or_default();
    for exchange in enabled_from_map {
        if !ordered.contains(&exchange) {
            ordered.push(exchange);
        }
    }
    ordered
}

fn find_exchange_config_key(exchange_map: &Map<String, Value>, exchange: &str) -> Option<String> {
    let target = storage_exchange_key(exchange)?;
    exchange_map
        .keys()
        .find(|key| storage_exchange_key(key).as_deref() == Some(target.as_str()))
        .cloned()
}

fn find_exchange_config<'a>(
    exchange_map: &'a Map<String, Value>,
    exchange: &str,
) -> Option<&'a Value> {
    let key = find_exchange_config_key(exchange_map, exchange)?;
    exchange_map.get(&key)
}

fn storage_exchange_key(exchange: &str) -> Option<String> {
    let lowercase = exchange.trim().to_ascii_lowercase();
    let normalized = match lowercase.as_str() {
        "" | "-" => return None,
        "gateio" | "gate.io" => "gate",
        "binance_spot" => "binance",
        "okx_spot" => "okx",
        other => other,
    };
    normalized
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        .then(|| normalized.to_string())
}

async fn write_strategy_yaml(
    path: &PathBuf,
    root: &Value,
    operation_id: &str,
) -> std::io::Result<()> {
    let content = serde_yaml::to_string(root).map_err(std::io::Error::other)?;
    atomic_write_local_file(path, &content, operation_id).await
}

async fn local_agent_command(
    State(state): State<LocalSideEffectState>,
    Json(payload): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let operation_id = local_operation_id();
    let command = json!({
        "schema_version": 1,
        "operation_id": operation_id,
        "command": payload.get("command").cloned().unwrap_or(Value::Null),
        "payload": payload.get("payload").cloned().unwrap_or(Value::Null),
        "source": "local_agent",
        "accepted_at": Utc::now(),
    });
    if !state.control.audit_ledger_configured() {
        return local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "audit_ledger_not_configured",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    }
    if state
        .control
        .record_operator_audit(
            "local_agent_command_queue_append",
            state.agent.agent_id.clone(),
            json!({
                "operation_id": operation_id,
                "command": command["command"],
                "payload": command["payload"],
                "would_submit_order": false,
                "local_path_exposed": false,
            }),
        )
        .await
        .is_err()
    {
        return local_json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "audit_write_failed",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    }
    if append_local_jsonl(state.config.command_queue_path.as_ref(), &command)
        .await
        .is_err()
    {
        return local_json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            json!({
                "schema_version": 1,
                "accepted": false,
                "status": "command_queue_not_configured",
                "operation_id": operation_id,
                "local_path_exposed": false,
            }),
        );
    }

    let audit = json!({
        "schema_version": 1,
        "operation_id": operation_id,
        "action": "command_queue_append",
        "accepted": true,
        "status": "accepted",
        "recorded_at": Utc::now(),
        "local_path_exposed": false,
    });
    state.audit.lock().await.push(audit.clone());

    local_json_response(
        StatusCode::ACCEPTED,
        json!({
            "schema_version": 1,
            "accepted": true,
            "status": "accepted",
            "operation_id": operation_id,
            "audit": audit,
            "local_path_exposed": false,
        }),
    )
}

async fn local_agent_history_status(
    State(state): State<LocalSideEffectState>,
    AxumPath(kind): AxumPath<String>,
) -> (StatusCode, Json<Value>) {
    let path = match kind.as_str() {
        "balance" => state.config.balance_history_path.as_ref(),
        "strategy-profit" | "strategy_profit" => state.config.strategy_profit_history_path.as_ref(),
        _ => {
            return local_json_response(
                StatusCode::NOT_FOUND,
                json!({
                    "schema_version": 1,
                    "status": "unknown_history_kind",
                    "local_path_exposed": false,
                }),
            )
        }
    };
    let status = local_history_file_status(path).await;
    local_json_response(
        StatusCode::OK,
        json!({
            "schema_version": 1,
            "kind": kind,
            "status": status,
            "local_path_exposed": false,
        }),
    )
}

async fn local_agent_balance_history(
    State(state): State<LocalSideEffectState>,
) -> (StatusCode, Json<Value>) {
    let mut rows = read_profit_history_rows(state.config.strategy_profit_history_path.as_ref())
        .await
        .unwrap_or_default();
    if rows.is_empty() {
        rows = read_balance_history_rows(state.config.balance_history_path.as_ref())
            .await
            .unwrap_or_default();
    }
    local_json_response(
        StatusCode::OK,
        json!({
            "schema_version": CONTROL_API_SCHEMA_VERSION,
            "rows": rows,
            "local_path_exposed": false,
        }),
    )
}

async fn read_profit_history_rows(path: Option<&PathBuf>) -> std::io::Result<Vec<Value>> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let raw = tokio::fs::read_to_string(path).await?;
    let mut cumulative_profit = 0.0;
    let mut rows = Vec::new();
    for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        let profit = first_profit_number(&value).unwrap_or_default();
        cumulative_profit += profit;
        let timestamp = [
            "recorded_at",
            "closed_at",
            "settled_at",
            "planned_at",
            "timestamp",
            "time",
        ]
        .iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .unwrap_or("-");
        rows.push(json!({
            "timestamp_ms": history_timestamp_ms(&value).unwrap_or(rows.len() as f64),
            "timestamp": timestamp,
            "profit_usdt": cumulative_profit,
            "event_profit_usdt": profit,
            "symbol": value.get("canonical_symbol").or_else(|| value.get("symbol")).cloned().unwrap_or(Value::Null),
            "exchange": value.get("exchange").or_else(|| value.get("long_exchange")).cloned().unwrap_or(Value::Null),
            "detail": value.get("lifecycle").or_else(|| value.get("event_kind")).cloned().unwrap_or(Value::Null),
        }));
    }
    Ok(rows)
}

async fn read_balance_history_rows(path: Option<&PathBuf>) -> std::io::Result<Vec<Value>> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let raw = tokio::fs::read_to_string(path).await?;
    let value = serde_json::from_str::<Value>(&raw).unwrap_or(Value::Null);
    Ok(value.as_array().cloned().unwrap_or_default())
}

fn first_profit_number(value: &Value) -> Option<f64> {
    [
        "actual_pnl_usdt",
        "realized_profit_usdt",
        "net_profit_usdt",
        "profit_usdt",
        "funding_pnl_usdt",
        "funding_fee_usdt",
    ]
    .iter()
    .find_map(|key| value.get(*key).and_then(Value::as_f64))
}

fn history_timestamp_ms(value: &Value) -> Option<f64> {
    [
        "recorded_at",
        "closed_at",
        "settled_at",
        "planned_at",
        "timestamp",
        "time",
    ]
    .iter()
    .find_map(|key| value.get(*key).and_then(Value::as_str))
    .and_then(|value| {
        chrono::DateTime::parse_from_rfc3339(value)
            .ok()
            .map(|timestamp| timestamp.timestamp_millis() as f64)
    })
}

fn local_json_response(status: StatusCode, value: Value) -> (StatusCode, Json<Value>) {
    (status, Json(value))
}

async fn local_file_status(path: Option<&PathBuf>) -> Value {
    let Some(path) = path else {
        return json!({
            "configured": false,
            "exists": false,
            "readable": false,
        });
    };
    match tokio::fs::metadata(path).await {
        Ok(metadata) => {
            let readable = metadata.is_file() && tokio::fs::File::open(path).await.is_ok();
            json!({
                "configured": true,
                "exists": true,
                "readable": readable,
            })
        }
        Err(error) => json!({
            "configured": true,
            "exists": false,
            "readable": false,
            "read_error": local_io_error_kind(&error),
        }),
    }
}

async fn local_restart_script_status(path: Option<&PathBuf>) -> Value {
    let mut status = local_file_status(path).await;
    if let Some(object) = status.as_object_mut() {
        object.insert("execution_allowed".to_string(), Value::Bool(false));
    }
    status
}

async fn local_history_file_status(path: Option<&PathBuf>) -> Value {
    let Some(path) = path else {
        return json!({
            "configured": false,
            "exists": false,
            "readable": false,
            "record_count": 0,
        });
    };
    match tokio::fs::read_to_string(path).await {
        Ok(raw) => json!({
            "configured": true,
            "exists": true,
            "readable": true,
            "record_count": raw.lines().filter(|line| !line.trim().is_empty()).count(),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => json!({
            "configured": true,
            "exists": false,
            "readable": false,
            "record_count": 0,
            "read_error": "not_found",
        }),
        Err(error) => json!({
            "configured": true,
            "exists": true,
            "readable": false,
            "record_count": 0,
            "read_error": local_io_error_kind(&error),
        }),
    }
}

async fn atomic_write_local_file(
    path: &Path,
    content: &str,
    operation_id: &str,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("strategy-config");
    let temp_path = path.with_file_name(format!(".{file_name}.{operation_id}.tmp"));
    tokio::fs::write(&temp_path, content).await?;
    tokio::fs::rename(temp_path, path).await
}

async fn append_local_jsonl(path: Option<&PathBuf>, value: &Value) -> std::io::Result<()> {
    let path = path.ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await?;
    file.write_all(value.to_string().as_bytes()).await?;
    file.write_all(b"\n").await
}

fn local_io_error_kind(error: &std::io::Error) -> &'static str {
    match error.kind() {
        std::io::ErrorKind::NotFound => "not_found",
        std::io::ErrorKind::PermissionDenied => "permission_denied",
        std::io::ErrorKind::InvalidData => "invalid_data",
        _ => "io_error",
    }
}

fn local_operation_id() -> String {
    format!(
        "local-{}-{}",
        std::process::id(),
        Utc::now().timestamp_micros()
    )
}

fn path_value(vars: &std::collections::BTreeMap<String, String>, name: &str) -> Option<PathBuf> {
    non_empty_value(vars, name).map(PathBuf::from)
}

fn path_list_value(vars: &std::collections::BTreeMap<String, String>, name: &str) -> Vec<PathBuf> {
    non_empty_value(vars, name)
        .and_then(|value| parse_csv(&value))
        .unwrap_or_default()
        .into_iter()
        .map(PathBuf::from)
        .collect()
}

fn usize_value(vars: &std::collections::BTreeMap<String, String>, name: &str) -> Option<usize> {
    non_empty_value(vars, name).and_then(|value| value.parse::<usize>().ok())
}

fn non_empty_value(
    vars: &std::collections::BTreeMap<String, String>,
    name: &str,
) -> Option<String> {
    vars.get(name)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn parse_csv(value: &str) -> Option<Vec<String>> {
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    (!values.is_empty()).then_some(values)
}

fn default_agent_capabilities() -> Vec<String> {
    vec!["control-api".to_string(), "supervisor-reader".to_string()]
}

async fn serve_static_spa(uri: Uri, static_dir: PathBuf) -> Response {
    let request_path = uri.path();
    if request_path == "/api" || request_path.starts_with("/api/") {
        return StatusCode::NOT_FOUND.into_response();
    }

    let relative = match safe_relative_path(request_path) {
        Some(relative) => relative,
        None => return StatusCode::NOT_FOUND.into_response(),
    };
    let candidate = static_dir.join(&relative);
    let path = if candidate.is_file() {
        candidate
    } else if relative.extension().is_none() {
        static_dir.join("index.html")
    } else {
        return StatusCode::NOT_FOUND.into_response();
    };

    match tokio::fs::read(&path).await {
        Ok(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, content_type_for_path(&path))],
            Body::from(bytes),
        )
            .into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}

fn safe_relative_path(request_path: &str) -> Option<PathBuf> {
    let trimmed = request_path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Some(PathBuf::from("index.html"));
    }

    let path = Path::new(trimmed);
    let mut relative = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => relative.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return None,
        }
    }
    Some(relative)
}

fn content_type_for_path(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("css") => "text/css; charset=utf-8",
        Some("html") => "text/html; charset=utf-8",
        Some("js") | Some("mjs") => "text/javascript; charset=utf-8",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("ico") => "image/x-icon",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[test]
    fn gateway_check_helpers_should_map_ui_exchanges() {
        assert_eq!(gateway_exchange_id("binance"), Some("binance"));
        assert_eq!(gateway_exchange_id("gate"), Some("gateio"));
        assert_eq!(gateway_exchange_id("gateio"), Some("gateio"));
        assert_eq!(gateway_exchange_id("unknown"), None);
    }

    #[test]
    fn config_should_parse_control_api_environment_without_process_globals() {
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_BIND", "127.0.0.1:19090"),
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            ("RUSTCTA_CONTROL_API_TENANT_ID", "tenant-a"),
            (
                "RUSTCTA_CONTROL_API_AGENT_CAPABILITIES",
                "control-api, supervisor-reader, logs",
            ),
            (
                "RUSTCTA_CONTROL_API_LEGACY_SNAPSHOT_PATH",
                "/tmp/dashboard_snapshot.json",
            ),
            (
                "RUSTCTA_CONTROL_API_EXTRA_STRATEGY_SNAPSHOT_PATHS",
                "/tmp/spot_futures.json,/tmp/funding.json",
            ),
            (
                "RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH",
                "/tmp/registry.json",
            ),
            ("RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH", "/tmp/audit.jsonl"),
            ("RUSTCTA_CONTROL_API_STRATEGY_LOG_PATH", "/tmp/strategy.log"),
            ("RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_LINES", "120"),
            ("RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_BYTES", "4096"),
            ("RUSTCTA_CONTROL_API_STATIC_DIR", "/tmp/dist"),
        ]);

        assert_eq!(config.bind_addr, "127.0.0.1:19090");
        assert_eq!(
            config.local_agent,
            Some(LocalAgentConfig {
                agent_id: "agent-a".to_string(),
                tenant_id: "tenant-a".to_string(),
                capabilities: vec![
                    "control-api".to_string(),
                    "supervisor-reader".to_string(),
                    "logs".to_string()
                ],
            })
        );
        assert!(config.local_side_effects.is_none());
        assert_eq!(
            config.legacy_snapshot_path,
            Some(PathBuf::from("/tmp/dashboard_snapshot.json"))
        );
        assert_eq!(
            config.extra_strategy_snapshot_paths,
            vec![
                PathBuf::from("/tmp/spot_futures.json"),
                PathBuf::from("/tmp/funding.json")
            ]
        );
        assert_eq!(
            config.supervisor_registry_path,
            Some(PathBuf::from("/tmp/registry.json"))
        );
        assert_eq!(
            config.audit_ledger_path,
            Some(PathBuf::from("/tmp/audit.jsonl"))
        );
        assert_eq!(
            config.strategy_log_path,
            Some(PathBuf::from("/tmp/strategy.log"))
        );
        assert_eq!(config.strategy_log_tail_lines, Some(120));
        assert_eq!(config.strategy_log_tail_bytes, Some(4096));
        assert_eq!(config.static_dir, Some(PathBuf::from("/tmp/dist")));
    }

    #[test]
    fn config_should_ignore_blank_values_and_apply_defaults() {
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_BIND", " "),
            ("RUSTCTA_CONTROL_API_AGENT_ID", ""),
            ("RUSTCTA_CONTROL_API_STATIC_DIR", " "),
            (
                "RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_LINES",
                "not-a-number",
            ),
        ]);

        assert_eq!(config.bind_addr, DEFAULT_BIND_ADDR);
        assert!(config.local_agent.is_none());
        assert!(config.local_side_effects.is_none());
        assert!(config.extra_strategy_snapshot_paths.is_empty());
        assert!(config.static_dir.is_none());
        assert!(config.strategy_log_tail_lines.is_none());
        assert_eq!(
            config.supervisor_registry_path,
            Some(PathBuf::from(DEFAULT_SUPERVISOR_REGISTRY_PATH))
        );
    }

    #[test]
    fn config_should_parse_local_agent_side_effect_boundary() {
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            (
                "RUSTCTA_CONTROL_API_LOCAL_STRATEGY_CONFIG_PATH",
                "/tmp/strategy.yml",
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_COMMAND_QUEUE_PATH",
                "/tmp/commands.jsonl",
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_BALANCE_HISTORY_PATH",
                "/tmp/balance.jsonl",
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_STRATEGY_PROFIT_HISTORY_PATH",
                "/tmp/profit.jsonl",
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_RESTART_SCRIPT_PATH",
                "/tmp/restart.sh",
            ),
        ]);

        assert_eq!(
            config.local_side_effects,
            Some(LocalSideEffectConfig {
                strategy_config_path: Some(PathBuf::from("/tmp/strategy.yml")),
                command_queue_path: Some(PathBuf::from("/tmp/commands.jsonl")),
                balance_history_path: Some(PathBuf::from("/tmp/balance.jsonl")),
                strategy_profit_history_path: Some(PathBuf::from("/tmp/profit.jsonl")),
                restart_script_path: Some(PathBuf::from("/tmp/restart.sh")),
            })
        );
    }

    #[test]
    fn config_should_ignore_side_effect_paths_without_local_agent_identity() {
        let config = ControlApiAppConfig::from_env_iter([(
            "RUSTCTA_CONTROL_API_LOCAL_STRATEGY_CONFIG_PATH",
            "/tmp/strategy.yml",
        )]);

        assert!(config.local_agent.is_none());
        assert!(config.local_side_effects.is_none());
    }

    #[tokio::test]
    async fn local_credential_status_should_collapse_accounts_to_canonical_exchange_account() {
        let temp_dir = std::env::current_dir()
            .unwrap()
            .join("target/tmp")
            .join(format!(
                "rustcta-control-api-credential-status-{}",
                std::process::id()
            ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let accounts_path = temp_dir.join("accounts.yml");
        let store_path = temp_dir.join("credentials.env");
        std::fs::write(
            &accounts_path,
            r#"
accounts:
  binance_hcr:
    name: HCR
    exchange: binance
    type: futures
    description: test account
    env_prefix: BINANCE_3
    enabled: true
"#,
        )
        .unwrap();
        std::fs::write(
            &store_path,
            "BINANCE_API_KEY='abcd1234efgh5678'\nBINANCE_API_SECRET='secret1234567890'\n",
        )
        .unwrap();
        let accounts_path_text = accounts_path.to_string_lossy().to_string();
        let store_path_text = store_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            (
                "RUSTCTA_CONTROL_API_CREDENTIAL_STATUS_STORE",
                store_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_ACCOUNTS_CONFIG",
                accounts_path_text.as_str(),
            ),
        ]);
        let app = config.build_router().unwrap();

        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/local-credentials/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let body = axum::body::to_bytes(status.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            value["account_manager_accounts"][0]["account_id"],
            "default"
        );
        assert_eq!(
            value["account_manager_accounts"][0]["credential_namespace"],
            "BINANCE"
        );
        assert_eq!(value["store_path"], "local-agent:exchange-api-key-store");
        let raw = std::str::from_utf8(&body).unwrap();
        assert!(!raw.contains("abcd1234efgh5678"));
        assert!(!raw.contains("secret1234567890"));
        assert!(raw.contains("abcd...5678"));
        assert!(!raw.contains(temp_dir.to_string_lossy().as_ref()));

        let post = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/local-credentials/status")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "exchange": "binance",
                            "account_id": "binance_hcr"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(post.status(), StatusCode::METHOD_NOT_ALLOWED);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn exchange_api_keys_should_write_mask_and_clear_unified_env_store() {
        let temp_dir = std::env::current_dir()
            .unwrap()
            .join("target/tmp")
            .join(format!(
                "rustcta-control-api-exchange-api-keys-{}",
                std::process::id()
            ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let accounts_path = temp_dir.join("accounts.yml");
        let store_path = temp_dir.join("exchange_api_keys.env");
        std::fs::write(
            &accounts_path,
            r#"
accounts:
  binance:
    name: Binance
    exchange: binance
    env_prefix: BINANCE
    enabled: true
"#,
        )
        .unwrap();
        let accounts_path_text = accounts_path.to_string_lossy().to_string();
        let store_path_text = store_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            (
                "RUSTCTA_CONTROL_API_EXCHANGE_API_KEY_STORE",
                store_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_ACCOUNTS_CONFIG",
                accounts_path_text.as_str(),
            ),
        ]);
        let app = config.build_router().unwrap();

        let save = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/exchange-api-keys")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "exchange": "binance",
                            "account_id": "default",
                            "api_key": "abcd1234efgh5678",
                            "api_secret": "secret1234567890"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(save.status(), StatusCode::OK);
        let body = axum::body::to_bytes(save.into_body(), usize::MAX)
            .await
            .unwrap();
        let body_text = std::str::from_utf8(&body).unwrap();
        assert!(!body_text.contains("abcd1234efgh5678"));
        assert!(!body_text.contains("secret1234567890"));
        assert!(body_text.contains("abcd...5678"));
        assert!(body_text.contains("\"restart_required\":true"));

        let store = std::fs::read_to_string(&store_path).unwrap();
        assert!(store.contains("BINANCE_API_KEY='abcd1234efgh5678'"));
        assert!(store.contains("BINANCE_API_SECRET='secret1234567890'"));
        assert!(!store.contains("BINANCE_FUTURES_API_KEY"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                std::fs::metadata(&store_path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        let test = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/exchange-api-keys/test")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "exchange": "binance",
                            "account_id": "default"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(test.status(), StatusCode::OK);
        let body = axum::body::to_bytes(test.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["accepted"], true);
        assert_eq!(value["check"]["status"], "ok");

        let clear = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/exchange-api-keys")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "exchange": "binance",
                            "account_id": "default",
                            "clear": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(clear.status(), StatusCode::OK);
        let store = std::fs::read_to_string(&store_path).unwrap();
        assert!(!store.contains("BINANCE_API_KEY"));
        assert!(!store.contains("BINANCE_API_SECRET"));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn local_create_strategy_should_write_generated_template_config() {
        let temp_dir = std::env::current_dir()
            .unwrap()
            .join("target/tmp")
            .join(format!(
                "rustcta-control-api-create-strategy-{}",
                std::process::id()
            ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let config_path = temp_dir.join("generated").join("unified_arb_live.yml");
        let audit_path = temp_dir.join("audit.jsonl");
        let registry_path = temp_dir.join("registry.json");
        let store_path = temp_dir.join("credentials.env");
        let audit_path_text = audit_path.to_string_lossy().to_string();
        let registry_path_text = registry_path.to_string_lossy().to_string();
        let store_path_text = store_path.to_string_lossy().to_string();
        let config_path_text = config_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "local-agent"),
            (
                "RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH",
                audit_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH",
                registry_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_CREDENTIAL_STATUS_STORE",
                store_path_text.as_str(),
            ),
        ]);
        let app = config.build_router().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/strategies")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "strategy_id": "unified_arb_live",
                            "strategy_kind": "unified_arbitrage",
                            "template_id": "unified_arbitrage",
                            "run_mode": "dry_run",
                            "tenant_id": "local",
                            "config_path": config_path_text,
                            "working_dir": ".",
                            "exchange_accounts": {
                                "long": "binance:default:BINANCE",
                                "short": "okx:default:OKX"
                            },
                            "risk": {
                                "max_total_notional_usdt": "1000"
                            },
                            "params": {
                                "symbols": "BTCUSDT,ETHUSDT",
                                "min_open_net_edge_pct": "0.04"
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let saved = std::fs::read_to_string(&config_path).unwrap();
        assert!(saved.contains("unified_arbitrage"));
        assert!(saved.contains("perp_perp_spread"));
        assert!(saved.contains("max_total_notional_usdt"));
        assert!(saved.contains("BTC/USDT"));
        assert!(!saved.contains("template_id"));
        assert!(!saved.contains("exchange_accounts"));
        assert!(!saved.contains("api_secret"));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn generated_hedged_grid_config_should_normalize_symbol_and_exchange() {
        let request = CreateStrategyRequest {
            strategy_id: "hedged_grid_live".to_string(),
            strategy_kind: "hedged_grid".to_string(),
            template_id: Some("hedged_grid".to_string()),
            run_mode: Some("dry_run".to_string()),
            run_id: None,
            tenant_id: Some("local".to_string()),
            config_path: "config/generated/hedged_grid_live.yml".to_string(),
            log_path: None,
            command: None,
            args: Vec::new(),
            working_dir: Some(".".to_string()),
            exchange_accounts: serde_json::json!({
                "execution": "Binance_USDM:default:BINANCE"
            }),
            risk: serde_json::json!({}),
            params: serde_json::json!({
                "symbol": "btc-usdt",
                "grid_spacing": "0.25",
                "grid_order_count": "8",
                "order_notional_usdt": "50"
            }),
        };

        let config = hedged_grid_runtime_config(&request);

        assert_eq!(config["symbol"], "BTC/USDT");
        assert_eq!(config["spot_exchange"], "binance");
        assert_eq!(config["hedge_exchange"], "binance");
    }

    #[tokio::test]
    async fn local_agent_routes_should_report_status_without_raw_paths() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-local-status-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let strategy_path = temp_dir.join("strategy.yml");
        let balance_path = temp_dir.join("balance.jsonl");
        let restart_path = temp_dir.join("restart.sh");
        std::fs::write(&strategy_path, "mode: simulation\n").unwrap();
        std::fs::write(&balance_path, "{\"asset\":\"USDT\"}\n").unwrap();
        std::fs::write(&restart_path, "#!/bin/sh\nexit 1\n").unwrap();

        let strategy_path_text = strategy_path.to_string_lossy().to_string();
        let balance_path_text = balance_path.to_string_lossy().to_string();
        let restart_path_text = restart_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            ("RUSTCTA_CONTROL_API_TENANT_ID", "tenant-a"),
            (
                "RUSTCTA_CONTROL_API_LOCAL_STRATEGY_CONFIG_PATH",
                strategy_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_BALANCE_HISTORY_PATH",
                balance_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_RESTART_SCRIPT_PATH",
                restart_path_text.as_str(),
            ),
        ]);
        let app = config.build_router().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/local-agent/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let raw = std::str::from_utf8(&body).unwrap();
        assert!(!raw.contains(temp_dir.to_string_lossy().as_ref()));
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["agent_id"], "agent-a");
        assert_eq!(value["tenant_id"], "tenant-a");
        assert_eq!(value["local_path_exposed"], false);
        assert_eq!(
            value["side_effects"]["strategy_config_edit"]["configured"],
            true
        );
        assert_eq!(
            value["side_effects"]["strategy_config_edit"]["readable"],
            true
        );
        assert_eq!(
            value["side_effects"]["restart_script"]["execution_allowed"],
            false
        );

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn local_agent_config_update_should_save_and_queue_restart_without_running_script() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-local-config-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let strategy_path = temp_dir.join("strategy.yml");
        let command_path = temp_dir.join("commands.jsonl");
        let audit_path = temp_dir.join("audit.jsonl");
        let restart_path = temp_dir.join("restart.sh");
        let marker_path = temp_dir.join("restart-marker");
        std::fs::write(&strategy_path, "mode: old\n").unwrap();
        std::fs::write(
            &restart_path,
            format!("#!/bin/sh\ntouch {}\n", marker_path.to_string_lossy()),
        )
        .unwrap();

        let strategy_path_text = strategy_path.to_string_lossy().to_string();
        let command_path_text = command_path.to_string_lossy().to_string();
        let audit_path_text = audit_path.to_string_lossy().to_string();
        let restart_path_text = restart_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            (
                "RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH",
                audit_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_STRATEGY_CONFIG_PATH",
                strategy_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_COMMAND_QUEUE_PATH",
                command_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_RESTART_SCRIPT_PATH",
                restart_path_text.as_str(),
            ),
        ]);
        let app = config.build_router().unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/local-agent/strategy-config")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "strategy_id": "unified_arb_live",
                            "content": "mode: simulation\n",
                            "restart": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let raw = std::str::from_utf8(&body).unwrap();
        assert!(!raw.contains(temp_dir.to_string_lossy().as_ref()));
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["accepted"], true);
        assert_eq!(value["status"], "accepted");
        assert_eq!(value["restart"]["queued"], true);
        assert_eq!(value["restart"]["restart_script_executed"], false);
        assert_eq!(value["local_path_exposed"], false);

        assert_eq!(
            std::fs::read_to_string(&strategy_path).unwrap(),
            "mode: simulation\n"
        );
        let command_log = std::fs::read_to_string(&command_path).unwrap();
        assert!(command_log.contains("\"command\":\"restart_strategy\""));
        assert!(command_log.contains("\"strategy_id\":\"unified_arb_live\""));
        assert!(command_log.contains("\"restart_script_executed\":false"));
        assert!(!marker_path.exists());
        let ledger_log = std::fs::read_to_string(&audit_path).unwrap();
        assert!(ledger_log.contains("local_agent_strategy_config_edit"));
        assert!(ledger_log.contains("unified_arb_live"));
        assert!(!ledger_log.contains("api_secret"));

        let audit = app
            .oneshot(
                Request::builder()
                    .uri("/api/local-agent/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(audit.status(), StatusCode::OK);
        let body = axum::body::to_bytes(audit.into_body(), usize::MAX)
            .await
            .unwrap();
        let raw = std::str::from_utf8(&body).unwrap();
        assert!(!raw.contains(temp_dir.to_string_lossy().as_ref()));
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["events"][0]["accepted"], true);
        assert_eq!(value["events"][0]["action"], "strategy_config_edit");
        assert_eq!(value["events"][0]["restart_script_executed"], false);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn local_agent_unified_arb_exchange_config_should_clean_on_startup() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-unified-arb-startup-clean-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let strategy_path = temp_dir.join("cross.yml");
        let audit_path = temp_dir.join("audit.jsonl");
        std::fs::write(
            &strategy_path,
            r#"
enabled_exchanges:
  - binance
  - bad_row
exchanges:
  binance:
    enabled: true
  bad_row: []
"#,
        )
        .unwrap();

        let strategy_path_text = strategy_path.to_string_lossy().to_string();
        let audit_path_text = audit_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            (
                "RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH",
                audit_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_STRATEGY_CONFIG_PATH",
                strategy_path_text.as_str(),
            ),
        ]);

        config
            .clean_unified_arb_exchange_config_on_startup()
            .await
            .unwrap();

        let cleaned_yaml = std::fs::read_to_string(&strategy_path).unwrap();
        let cleaned_config: Value = serde_yaml::from_str(&cleaned_yaml).unwrap();
        assert!(cleaned_config["exchanges"].get("bad_row").is_none());
        assert_eq!(cleaned_config["enabled_exchanges"], json!(["binance"]));
        let ledger_log = std::fs::read_to_string(&audit_path).unwrap();
        assert!(ledger_log.contains("local_agent_unified_arb_exchange_config_clean"));
        assert!(!ledger_log.contains(temp_dir.to_string_lossy().as_ref()));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn local_agent_unified_arb_exchange_config_should_clean_persist_delete_and_queue() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-unified-arb-exchanges-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let strategy_path = temp_dir.join("cross.yml");
        let command_path = temp_dir.join("commands.jsonl");
        let audit_path = temp_dir.join("audit.jsonl");
        std::fs::write(
            &strategy_path,
            r#"
mode: live_small
enabled_exchanges:
  - binance
  - broken
exchanges:
  binance:
    enabled: true
    operating_mode: enabled
    account_id: BINANCE
    env_prefix: BINANCE
    routes:
      rest_public:
        - https://fapi.binance.com
  gate:
    enabled: "yes"
    env_prefix: 123
  broken: []
"#,
        )
        .unwrap();

        let strategy_path_text = strategy_path.to_string_lossy().to_string();
        let command_path_text = command_path.to_string_lossy().to_string();
        let audit_path_text = audit_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            (
                "RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH",
                audit_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_STRATEGY_CONFIG_PATH",
                strategy_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_COMMAND_QUEUE_PATH",
                command_path_text.as_str(),
            ),
        ]);
        let app = config.build_router().unwrap();

        let loaded = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/local-agent/unified-arb/exchanges")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(loaded.status(), StatusCode::OK);
        let body = axum::body::to_bytes(loaded.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["cleaned_on_read"], true);
        assert_eq!(value["enabled_exchanges"], json!(["binance"]));
        assert!(value["exchanges"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["exchange"] != "broken"));

        let cleaned_yaml = std::fs::read_to_string(&strategy_path).unwrap();
        let cleaned_config: Value = serde_yaml::from_str(&cleaned_yaml).unwrap();
        assert!(cleaned_config["exchanges"].get("broken").is_none());
        assert_eq!(cleaned_config["exchanges"]["gate"]["enabled"], false);
        assert_eq!(
            cleaned_config["exchanges"]["gate"]["env_prefix"],
            Value::Null
        );

        let saved = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/local-agent/unified-arb/exchanges")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "strategy_id": "unified_arb_live",
                            "apply": true,
                            "exchanges": [
                                {
                                    "exchange": "binance",
                                    "enabled": false,
                                    "account_id": "BINANCE_ALT",
                                    "env_prefix": "BINANCE_ALT",
                                    "private_rest_enabled": true,
                                    "private_ws_enabled": false
                                },
                                {
                                    "exchange": "gateio",
                                    "enabled": true,
                                    "account_id": "GATE",
                                    "env_prefix": "GATE",
                                    "private_rest_enabled": true,
                                    "private_ws_enabled": true
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(saved.status(), StatusCode::ACCEPTED);
        let saved_yaml = std::fs::read_to_string(&strategy_path).unwrap();
        let saved_config: Value = serde_yaml::from_str(&saved_yaml).unwrap();
        assert_eq!(saved_config["enabled_exchanges"], json!(["gate"]));
        assert_eq!(saved_config["exchanges"]["binance"]["enabled"], false);
        assert_eq!(
            saved_config["exchanges"]["binance"]["routes"]["rest_public"][0],
            "https://fapi.binance.com"
        );
        assert_eq!(saved_config["exchanges"]["gate"]["enabled"], true);
        assert!(saved_config["exchanges"].get("gateio").is_none());
        let command_log = std::fs::read_to_string(&command_path).unwrap();
        assert!(command_log.contains("\"command\":\"update_unified_arb_exchange_config\""));
        assert!(command_log.contains("\"enabled_exchanges\":[\"gate\"]"));

        let merged = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/local-agent/unified-arb/exchanges")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "strategy_id": "unified_arb_live",
                            "replace": false,
                            "apply": true,
                            "exchanges": [
                                {
                                    "exchange": "binance",
                                    "enabled": true,
                                    "account_id": "BINANCE_ALT",
                                    "env_prefix": "BINANCE_ALT",
                                    "private_rest_enabled": true,
                                    "private_ws_enabled": false
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(merged.status(), StatusCode::ACCEPTED);
        let merged_yaml = std::fs::read_to_string(&strategy_path).unwrap();
        let merged_config: Value = serde_yaml::from_str(&merged_yaml).unwrap();
        assert_eq!(
            merged_config["enabled_exchanges"],
            json!(["binance", "gate"])
        );
        assert_eq!(merged_config["exchanges"]["gate"]["enabled"], true);

        let deleted = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/local-agent/unified-arb/exchanges/gateio")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(deleted.status(), StatusCode::ACCEPTED);
        let deleted_yaml = std::fs::read_to_string(&strategy_path).unwrap();
        let deleted_config: Value = serde_yaml::from_str(&deleted_yaml).unwrap();
        assert!(deleted_config["exchanges"].get("gate").is_none());
        assert_eq!(deleted_config["enabled_exchanges"], json!(["binance"]));
        assert_eq!(
            std::fs::read_to_string(&command_path)
                .unwrap()
                .lines()
                .count(),
            3
        );
        let ledger_log = std::fs::read_to_string(&audit_path).unwrap();
        assert!(ledger_log.contains("local_agent_unified_arb_exchange_config_clean"));
        assert!(ledger_log.contains("local_agent_unified_arb_exchange_config_save"));
        assert!(ledger_log.contains("local_agent_unified_arb_exchange_config_delete"));
        assert!(!ledger_log.contains(temp_dir.to_string_lossy().as_ref()));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn local_agent_unified_arb_settings_should_persist_open_and_close_thresholds() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-unified-arb-settings-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let strategy_path = temp_dir.join("cross.yml");
        let command_path = temp_dir.join("commands.jsonl");
        let audit_path = temp_dir.join("audit.jsonl");
        std::fs::write(
            &strategy_path,
            r#"
mode: live_small
enabled_exchanges:
  - binance
  - gate
thresholds:
  min_open_raw_spread: 0.005
  min_open_maker_taker_net_edge: 0.004
dual_taker:
  target_notional_usdt: 5.5
execution_quality:
  min_open_executable_depth_ratio: 1.2
controls:
  start_close_only: true
universe:
  symbols:
    - OLD/USDT
exchanges:
  binance:
    enabled: true
  gate:
    enabled: true
"#,
        )
        .unwrap();

        let strategy_path_text = strategy_path.to_string_lossy().to_string();
        let command_path_text = command_path.to_string_lossy().to_string();
        let audit_path_text = audit_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            (
                "RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH",
                audit_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_STRATEGY_CONFIG_PATH",
                strategy_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LOCAL_COMMAND_QUEUE_PATH",
                command_path_text.as_str(),
            ),
        ]);
        let app = config.build_router().unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/local-agent/unified-arb/settings")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "strategy_id": "unified_arb_live",
                            "apply": true,
                            "symbols": ["EDGE/USDT", "edge-usdt", "SPCX/USDT"],
                            "target_notional_usdt": 5.5,
                            "max_notional_usdt": 5.5,
                            "max_positions_per_exchange": 3,
                            "max_open_bundles": 3,
                            "max_open_positions": 6,
                            "min_open_raw_spread": 0.01,
                            "min_open_maker_taker_net_edge": 0.002,
                            "min_open_executable_depth_ratio": 1.2,
                            "close_min_net_profit_pct": 0.002,
                            "expected_close_spread_pct": 0.002,
                            "trading_enabled": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["settings"]["min_open_raw_spread"], json!(0.01));
        assert_eq!(
            value["settings"]["min_open_maker_taker_net_edge"],
            json!(0.002)
        );
        assert_eq!(value["settings"]["close_min_net_profit_pct"], json!(0.002));
        assert_eq!(
            value["settings"]["execution"]["trading_enabled"],
            json!(true)
        );
        assert_eq!(
            value["settings"]["execution"]["live_orders_enabled"],
            json!(true)
        );

        let saved_yaml = std::fs::read_to_string(&strategy_path).unwrap();
        let saved_config: Value = serde_yaml::from_str(&saved_yaml).unwrap();
        assert_eq!(
            saved_config["thresholds"]["min_open_raw_spread"],
            json!(0.01)
        );
        assert_eq!(
            saved_config["thresholds"]["min_open_maker_taker_net_edge"],
            json!(0.002)
        );
        assert_eq!(
            saved_config["dual_taker"]["min_open_spread_pct"],
            json!(0.01)
        );
        assert_eq!(
            saved_config["dual_taker"]["min_open_net_profit_pct"],
            json!(0.002)
        );
        assert_eq!(
            saved_config["dual_taker"]["close_min_net_profit_pct"],
            json!(0.002)
        );
        assert_eq!(
            saved_config["dual_taker"]["expected_close_spread_pct"],
            json!(0.002)
        );
        assert_eq!(
            saved_config["execution_quality"]["min_open_raw_spread_pct"],
            json!(0.01)
        );
        assert_eq!(
            saved_config["execution_quality"]["min_open_net_edge_pct"],
            json!(0.002)
        );
        assert_eq!(
            saved_config["execution_quality"]["min_close_net_profit_pct"],
            json!(0.002)
        );
        assert_eq!(saved_config["execution"]["trading_enabled"], json!(true));
        assert_eq!(
            saved_config["enabled_symbols"],
            json!(["EDGE/USDT", "SPCX/USDT"])
        );
        assert_eq!(
            saved_config["universe"]["symbols"],
            json!(["EDGE/USDT", "SPCX/USDT"])
        );
        let command_log = std::fs::read_to_string(&command_path).unwrap();
        assert!(command_log.contains("\"command\":\"update_unified_arb_settings\""));
        let ledger_log = std::fs::read_to_string(&audit_path).unwrap();
        assert!(ledger_log.contains("local_agent_unified_arb_settings_save"));
        assert!(!ledger_log.contains(temp_dir.to_string_lossy().as_ref()));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn local_agent_strategy_mutations_should_require_and_write_audit_ledger() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-local-strategy-mutation-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let registry_path = temp_dir.join("registry.json");
        let audit_path = temp_dir.join("audit.jsonl");
        let registry_path_text = registry_path.to_string_lossy().to_string();
        let audit_path_text = audit_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            (
                "RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH",
                registry_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_AUDIT_LEDGER_PATH",
                audit_path_text.as_str(),
            ),
        ]);
        let app = config.build_router().unwrap();

        let create = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/strategies")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "strategy_id": "spot-arb-a",
                            "strategy_kind": "spot_spot_arbitrage",
                            "tenant_id": "local",
                            "config_path": "config/spot.yml",
                            "command": "sh",
                            "args": ["-c", "true"],
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(create.status(), StatusCode::OK);
        let body = axum::body::to_bytes(create.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["strategy_id"], "spot-arb-a");
        assert_eq!(value["status"], "Stopped");
        assert!(value.get("log_path").is_none());

        let registry = std::fs::read_to_string(&registry_path).unwrap();
        assert!(registry.contains("spot-arb-a"));
        let ledger = std::fs::read_to_string(&audit_path).unwrap();
        assert!(ledger.contains("create_strategy"));
        assert!(ledger.contains("spot-arb-a"));
        assert!(!ledger.contains("api_secret"));

        let command = rustcta_supervisor::LifecycleCommandRecord {
            schema_version: rustcta_supervisor::SUPERVISOR_SCHEMA_VERSION,
            command_id: "cmd-heartbeat".to_string(),
            strategy_id: "spot-arb-a".to_string(),
            run_id: None,
            command: rustcta_supervisor::LifecycleCommand::Heartbeat,
            requested_by: Some("web".to_string()),
            idempotency_key: "idem-heartbeat".to_string(),
            requested_at: Utc::now(),
        };
        let accepted = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/commands")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&command).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::OK);
        let body = axum::body::to_bytes(accepted.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["accepted"], true);
        assert_eq!(value["applied_to_runtime"], false);

        let events = app
            .oneshot(
                Request::builder()
                    .uri("/api/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events.status(), StatusCode::OK);
        let body = axum::body::to_bytes(events.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["commands"].as_array().unwrap().len(), 1);
        assert_eq!(value["ledger_events"].as_array().unwrap().len(), 2);

        let no_ledger_config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            (
                "RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH",
                registry_path_text.as_str(),
            ),
        ]);
        let no_ledger_app = no_ledger_config.build_router().unwrap();
        let rejected = no_ledger_app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/strategies")
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "strategy_id": "spot-arb-b",
                            "strategy_kind": "spot_spot_arbitrage",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::SERVICE_UNAVAILABLE);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn app_router_should_serve_control_api_routes_from_configured_state() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-app-contract-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let registry_path = temp_dir.join("registry.json");
        std::fs::write(
            &registry_path,
            serde_json::json!({
                "schema_version": 1,
                "captured_at": "2026-06-07T12:00:02Z",
                "processes": [
                    {
                        "schema_version": 1,
                        "strategy_id": "unified_arb_live",
                        "strategy_kind": "unified_arbitrage",
                        "run_id": "local",
                        "tenant_id": "local",
                        "config_path": "config/unified_arbitrage_usdt.yml",
                        "status": "Running",
                        "process_id": 123,
                        "started_at": "2026-06-07T12:00:00Z",
                        "last_heartbeat_at": "2026-06-07T12:00:01Z",
                        "last_snapshot_at": null,
                        "restart_count": 0,
                        "last_exit_code": null,
                        "last_error": null,
                        "log_path": null
                    },
                    {
                        "schema_version": 1,
                        "strategy_id": "spot_spot_live_dry_run",
                        "strategy_kind": "spot_spot_taker_arbitrage",
                        "run_id": "local",
                        "tenant_id": "local",
                        "config_path": "config/spot_spot_arbitrage_live_dry_run_2ex_5symbols.yml",
                        "status": "Stopped",
                        "process_id": null,
                        "started_at": null,
                        "last_heartbeat_at": null,
                        "last_snapshot_at": null,
                        "restart_count": 0,
                        "last_exit_code": null,
                        "last_error": null,
                        "log_path": null
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();
        let registry_path_text = registry_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            ("RUSTCTA_CONTROL_API_TENANT_ID", "tenant-a"),
            (
                "RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH",
                registry_path_text.as_str(),
            ),
        ]);
        let app = config.build_router().unwrap();

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["agent_count"], 1);
        assert_eq!(value["process_count"], 2);
        assert_eq!(value["strategy_count"], 2);

        let agents = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/agents")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(agents.status(), StatusCode::OK);
        let body = axum::body::to_bytes(agents.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 1);
        assert_eq!(value[0]["agent_id"], "agent-a");
        assert_eq!(value[0]["tenant_id"], "tenant-a");
        assert_eq!(value[0]["status"], "connected");

        let strategies = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/strategies")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(strategies.status(), StatusCode::OK);
        let body = axum::body::to_bytes(strategies.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 2);
        assert_eq!(value[0]["strategy_id"], "unified_arb_live");
        assert_eq!(value[0]["strategy_kind"], "unified_arbitrage");
        assert_eq!(value[0]["status"], "Running");
        assert_eq!(value[1]["strategy_id"], "spot_spot_live_dry_run");
        assert_eq!(value[1]["strategy_kind"], "spot_spot_taker_arbitrage");
        assert_eq!(value[1]["status"], "Stopped");

        let process = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/processes/unified_arb_live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(process.status(), StatusCode::OK);
        let body = axum::body::to_bytes(process.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["run_id"], "local");
        assert_eq!(value["process_id"], 123);
        assert!(value.get("log_path").is_none());

        let processes = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/processes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(processes.status(), StatusCode::OK);
        let body = axum::body::to_bytes(processes.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 2);
        assert_eq!(value[0]["strategy_id"], "unified_arb_live");
        assert!(value[0].get("log_path").is_none());

        let gateway = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/gateway/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(gateway.status(), StatusCode::OK);
        let body = axum::body::to_bytes(gateway.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert!(value["gateway"].is_null());

        let credentials = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/credentials/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(credentials.status(), StatusCode::OK);
        let body = axum::body::to_bytes(credentials.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["slots"].as_array().unwrap().len(), 0);
        assert!(value.get("raw").is_none());
        assert!(value.get("api_key").is_none());
        assert!(value.get("api_secret").is_none());
        assert!(value.get("secret").is_none());

        let symbols = app
            .oneshot(
                Request::builder()
                    .uri("/api/symbols")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(symbols.status(), StatusCode::OK);
        let body = axum::body::to_bytes(symbols.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["symbol_rules"].as_array().unwrap().len(), 0);
        assert_eq!(value["spot_control"].as_object().unwrap().len(), 0);
        assert_eq!(
            value["scanner"]["symbol_coverage"]
                .as_array()
                .unwrap()
                .len(),
            0
        );

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn app_router_should_start_with_missing_followed_files() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-app-missing-files-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let registry_path = temp_dir.join("missing-registry.json");
        let log_path = temp_dir.join("missing-strategy.log");

        let registry_path_text = registry_path.to_string_lossy().to_string();
        let log_path_text = log_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            (
                "RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH",
                registry_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_STRATEGY_LOG_PATH",
                log_path_text.as_str(),
            ),
        ]);
        let app = config.build_router().unwrap();

        let workspace = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/workspace")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(workspace.status(), StatusCode::OK);
        let body = axum::body::to_bytes(workspace.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["agent_count"], 0);
        assert_eq!(value["strategy_count"], 0);

        let logs = app
            .oneshot(
                Request::builder()
                    .uri("/api/strategy-logs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(logs.status(), StatusCode::OK);
        let body = axum::body::to_bytes(logs.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["configured"], true);
        assert_eq!(value["readable"], false);
        assert!(value["read_error"].is_string());

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn static_hosting_should_serve_spa_without_capturing_api_404s() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-app-static-{}",
            std::process::id()
        ));
        let static_dir = temp_dir.join("dist");
        std::fs::create_dir_all(&static_dir).unwrap();
        std::fs::write(static_dir.join("index.html"), "<html>control panel</html>").unwrap();
        std::fs::write(static_dir.join("main.css"), "body { color: black; }").unwrap();

        let static_dir_text = static_dir.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([(
            "RUSTCTA_CONTROL_API_STATIC_DIR",
            static_dir_text.as_str(),
        )]);
        let app = config.build_router().unwrap();

        for uri in ["/", "/workspace/unified_arb_live"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let content_type = response
                .headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap();
            assert_eq!(content_type, "text/html; charset=utf-8");
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            assert!(std::str::from_utf8(&body)
                .unwrap()
                .contains("control panel"));
        }

        let css = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/main.css")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(css.status(), StatusCode::OK);
        assert_eq!(
            css.headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap()
                .to_str()
                .unwrap(),
            "text/css; charset=utf-8"
        );

        let unknown_api = app
            .oneshot(
                Request::builder()
                    .uri("/api/not-a-route")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown_api.status(), StatusCode::NOT_FOUND);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn app_router_should_tail_process_logs_from_supervisor_registry_metadata() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-app-process-log-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let registry_path = temp_dir.join("registry.json");
        let log_path = temp_dir.join("unified-arb.log");
        std::fs::write(
            &log_path,
            [
                "2026-06-07T12:00:00Z INFO process booted",
                "2026-06-07T12:00:01Z WARN lag detected",
                "2026-06-07T12:00:02Z ERROR credential marker should redact",
            ]
            .join("\n"),
        )
        .unwrap();
        std::fs::write(
            &registry_path,
            serde_json::json!({
                "schema_version": 1,
                "captured_at": "2026-06-07T12:00:02Z",
                "processes": [
                    {
                        "schema_version": 1,
                        "strategy_id": "unified_arb_live",
                        "strategy_kind": "unified_arbitrage",
                        "run_id": "local",
                        "tenant_id": "local",
                        "config_path": "config/unified_arbitrage_usdt.yml",
                        "status": "Running",
                        "process_id": 123,
                        "started_at": "2026-06-07T12:00:00Z",
                        "last_heartbeat_at": "2026-06-07T12:00:01Z",
                        "last_snapshot_at": null,
                        "restart_count": 0,
                        "last_exit_code": null,
                        "last_error": null,
                        "log_path": log_path
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let registry_path_text = registry_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            (
                "RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH",
                registry_path_text.as_str(),
            ),
            ("RUSTCTA_CONTROL_API_STRATEGY_LOG_TAIL_LINES", "2"),
        ]);
        let app = config.build_router().unwrap();

        let process = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/processes/unified_arb_live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(process.status(), StatusCode::OK);
        let body = axum::body::to_bytes(process.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["log_configured"], true);
        assert!(value.get("log_path").is_none());

        let logs = app
            .oneshot(
                Request::builder()
                    .uri("/api/processes/unified_arb_live/logs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(logs.status(), StatusCode::OK);
        let body = axum::body::to_bytes(logs.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["target"], "unified_arb_live");
        assert_eq!(value["configured"], true);
        assert_eq!(value["readable"], true);
        assert_eq!(value["event_count"], 2);
        assert_eq!(value["events"][1]["message"], "[redacted log line]");

        std::fs::remove_dir_all(temp_dir).ok();
    }
}
