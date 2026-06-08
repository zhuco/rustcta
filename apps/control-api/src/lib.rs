use anyhow::Result;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Json;
use axum::Router;
use chrono::Utc;
use rustcta_control_api::{
    router, ControlApiState, CreateStrategyRequest, StrategyProcessView, CONTROL_API_SCHEMA_VERSION,
};
use rustcta_event_ledger::JsonlLedger;
use rustcta_supervisor::LifecycleCommandRecord;
use serde_json::{json, Value};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8091";
const DEFAULT_TENANT_ID: &str = "local";
const DEFAULT_SUPERVISOR_REGISTRY_PATH: &str = "run/supervisor/registry.json";
const DEFAULT_STRATEGY_LOG_TAIL_LINES: usize = 800;
const DEFAULT_STRATEGY_LOG_TAIL_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlApiAppConfig {
    pub bind_addr: String,
    pub local_agent: Option<LocalAgentConfig>,
    pub local_side_effects: Option<LocalSideEffectConfig>,
    pub legacy_snapshot_path: Option<PathBuf>,
    pub supervisor_registry_path: Option<PathBuf>,
    pub audit_ledger_path: Option<PathBuf>,
    pub strategy_log_path: Option<PathBuf>,
    pub strategy_log_tail_lines: Option<usize>,
    pub strategy_log_tail_bytes: Option<usize>,
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
            static_dir: path_value(&vars, "RUSTCTA_CONTROL_API_STATIC_DIR"),
        }
    }

    pub fn build_state(&self) -> Result<ControlApiState> {
        let mut state = ControlApiState::empty_local();
        if let Some(path) = &self.legacy_snapshot_path {
            state = state.with_legacy_dashboard_snapshot_path(path.clone());
        }

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
        if self.local_agent.is_some() {
            api = api.merge(local_mutation_router(state.clone()));
        }
        if let (Some(agent), Some(side_effects)) = (&self.local_agent, &self.local_side_effects) {
            api = api.merge(local_agent_router(LocalSideEffectState::new(
                agent.clone(),
                side_effects.clone(),
                state,
            )));
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
        .route("/api/commands", post(local_command))
        .route("/api/strategies/:id/command", post(local_strategy_command))
        .with_state(state)
}

fn local_agent_router(state: LocalSideEffectState) -> Router {
    Router::new()
        .route("/api/local-agent/status", get(local_agent_status))
        .route("/api/local-agent/audit", get(local_agent_audit))
        .route(
            "/api/local-agent/strategy-config",
            get(local_agent_strategy_config_draft).post(local_agent_strategy_config),
        )
        .route("/api/local-agent/commands", post(local_agent_command))
        .route(
            "/api/local-agent/history/:kind/status",
            get(local_agent_history_status),
        )
        .with_state(state)
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

    if state
        .record_operator_audit(
            "create_strategy",
            "local-agent",
            json!({
                "strategy_id": process.strategy_id,
                "strategy_kind": process.strategy_kind,
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
                            "strategy_id": "cross_arb_live",
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
        assert!(command_log.contains("\"strategy_id\":\"cross_arb_live\""));
        assert!(command_log.contains("\"restart_script_executed\":false"));
        assert!(!marker_path.exists());
        let ledger_log = std::fs::read_to_string(&audit_path).unwrap();
        assert!(ledger_log.contains("local_agent_strategy_config_edit"));
        assert!(ledger_log.contains("cross_arb_live"));
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
                        "strategy_id": "cross_arb_live",
                        "strategy_kind": "cross_exchange_arbitrage",
                        "run_id": "local",
                        "tenant_id": "local",
                        "config_path": "config/cross_exchange_arbitrage_usdt.yml",
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
        assert_eq!(value[0]["strategy_id"], "cross_arb_live");
        assert_eq!(value[0]["strategy_kind"], "cross_exchange_arbitrage");
        assert_eq!(value[0]["status"], "Running");
        assert_eq!(value[1]["strategy_id"], "spot_spot_live_dry_run");
        assert_eq!(value[1]["strategy_kind"], "spot_spot_taker_arbitrage");
        assert_eq!(value[1]["status"], "Stopped");

        let process = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/processes/cross_arb_live")
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
        assert_eq!(value[0]["strategy_id"], "cross_arb_live");
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

        for uri in ["/", "/workspace/cross_arb_live"] {
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
        let log_path = temp_dir.join("cross-arb.log");
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
                        "strategy_id": "cross_arb_live",
                        "strategy_kind": "cross_exchange_arbitrage",
                        "run_id": "local",
                        "tenant_id": "local",
                        "config_path": "config/cross_exchange_arbitrage_usdt.yml",
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
                    .uri("/api/processes/cross_arb_live")
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
                    .uri("/api/processes/cross_arb_live/logs")
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
        assert_eq!(value["target"], "cross_arb_live");
        assert_eq!(value["configured"], true);
        assert_eq!(value["readable"], true);
        assert_eq!(value["event_count"], 2);
        assert_eq!(value["events"][1]["message"], "[redacted log line]");

        std::fs::remove_dir_all(temp_dir).ok();
    }
}
