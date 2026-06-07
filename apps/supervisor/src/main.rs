use anyhow::Result;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use rustcta_supervisor::{
    build_legacy_process_spec, JsonFileProcessRegistryStore, LegacyProcessSpecOptions,
    LegacyProcessTemplate, LocalProcessSupervisor, ProcessRegistry, RuntimeHeartbeat,
    RuntimeHeartbeatStatus, RuntimeSnapshotStatus, StrategyProcess, StrategyProcessSpec,
    SupervisorRecoveryStatus, SupervisorSnapshot,
};
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpListener;

const LOCAL_LIFECYCLE_TOKEN_HEADER: &str = "x-rustcta-supervisor-token";

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let registry_store = arg_value(&args, "--registry-path").map(JsonFileProcessRegistryStore::new);

    if has_flag(&args, "--validate-registry") {
        let registry = load_registry(registry_store.as_ref())?;
        let report = registry.validation_report();
        println!("{}", serde_json::to_string_pretty(&report)?);
        if !report.valid {
            anyhow::bail!("supervisor registry validation failed");
        }
        return Ok(());
    }

    if let Some(template) = arg_value(&args, "--print-legacy-spec") {
        let template = template
            .parse::<LegacyProcessTemplate>()
            .map_err(anyhow::Error::msg)?;
        let mut options = LegacyProcessSpecOptions::new(template);
        options.strategy_id = arg_value(&args, "--strategy-id").map(ToString::to_string);
        options.run_id = arg_value(&args, "--run-id").map(ToString::to_string);
        options.tenant_id = arg_value(&args, "--tenant-id").map(ToString::to_string);
        options.config_path = arg_value(&args, "--config").map(ToString::to_string);
        options.working_dir = arg_value(&args, "--working-dir").map(ToString::to_string);
        options.log_dir = arg_value(&args, "--log-dir").map(ToString::to_string);
        options.restart_backoff_ms =
            arg_value(&args, "--restart-backoff-ms").and_then(|value| value.parse().ok());

        let spec = build_legacy_process_spec(options);
        spec.validate()?;
        println!("{}", serde_json::to_string_pretty(&spec)?);
        return Ok(());
    }

    if let Some(spec_path) = arg_value(&args, "--validate-spec") {
        let spec = load_spec(spec_path)?;
        spec.validate()?;
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "valid": true,
                "schema_version": spec.schema_version,
                "strategy_id": spec.strategy_id,
                "strategy_kind": spec.strategy_kind,
                "run_id": spec.run_id,
                "tenant_id": spec.tenant_id,
                "command": spec.command,
                "arg_count": spec.args.len(),
                "working_dir_configured": spec.working_dir.is_some(),
                "log_configured": spec.log_path.is_some(),
                "restart_backoff_ms": spec.restart_backoff_ms,
            }))?
        );
        return Ok(());
    }

    if let Some(max_age_ms) =
        arg_value(&args, "--mark-stale-heartbeats-ms").and_then(|value| value.parse::<i64>().ok())
    {
        let Some(store) = registry_store.as_ref() else {
            anyhow::bail!("--mark-stale-heartbeats-ms requires --registry-path");
        };
        let registry = load_registry(Some(store))?;
        let mut supervisor = LocalProcessSupervisor::from_registry(registry);
        let stale = supervisor.mark_stale_heartbeats(max_age_ms, chrono::Utc::now());
        save_registry(Some(store), supervisor.registry())?;
        println!("{}", serde_json::to_string_pretty(&stale)?);
        return Ok(());
    }

    if has_flag(&args, "--serve") {
        let bind_addr = arg_value(&args, "--bind").unwrap_or("127.0.0.1:18181");
        let store = registry_store
            .unwrap_or_else(|| JsonFileProcessRegistryStore::new("run/supervisor/registry.json"));
        let local_lifecycle_token = arg_value(&args, "--local-lifecycle-token")
            .map(ToString::to_string)
            .or_else(|| std::env::var("RUSTCTA_SUPERVISOR_LOCAL_LIFECYCLE_TOKEN").ok())
            .filter(|value| !value.trim().is_empty());
        if local_lifecycle_token.is_some() && !is_loopback_bind(bind_addr) {
            anyhow::bail!(
                "local lifecycle routes require a loopback --bind address such as 127.0.0.1:18181"
            );
        }
        let listener = TcpListener::bind(bind_addr).await?;
        println!("rustcta-supervisor listening on http://{bind_addr}");
        axum::serve(listener, supervisor_router(store, local_lifecycle_token)).await?;
        return Ok(());
    }

    let Some(spec_path) = arg_value(&args, "--spec") else {
        let registry = load_registry(registry_store.as_ref())?;
        println!("{}", serde_json::to_string_pretty(&registry.snapshot())?);
        return Ok(());
    };

    let run_once_ms = arg_value(&args, "--run-once-ms")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(250);
    let spec = load_spec(spec_path)?;
    let registry = load_registry(registry_store.as_ref())?;
    let mut supervisor = LocalProcessSupervisor::from_registry(registry);

    let started = supervisor.start(spec).await?;
    save_registry(registry_store.as_ref(), supervisor.registry())?;
    println!("{}", serde_json::to_string_pretty(&started)?);

    tokio::time::sleep(std::time::Duration::from_millis(run_once_ms)).await;

    let strategy_id = started.strategy_id;
    if supervisor.registry().get(&strategy_id).is_some() {
        let stopped = supervisor.stop(&strategy_id).await?;
        save_registry(registry_store.as_ref(), supervisor.registry())?;
        println!("{}", serde_json::to_string_pretty(&stopped)?);
    }

    Ok(())
}

fn load_spec(path: &str) -> Result<StrategyProcessSpec> {
    let raw = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

#[derive(Clone)]
struct SupervisorApiState {
    registry_store: Arc<JsonFileProcessRegistryStore>,
    local_lifecycle_token: Option<Arc<String>>,
}

fn supervisor_router(
    registry_store: JsonFileProcessRegistryStore,
    local_lifecycle_token: Option<String>,
) -> Router {
    let local_lifecycle_enabled = local_lifecycle_token.is_some();
    let state = SupervisorApiState {
        registry_store: Arc::new(registry_store),
        local_lifecycle_token: local_lifecycle_token.map(Arc::new),
    };
    let router = Router::new()
        .route("/api/health", get(supervisor_health))
        .route("/api/snapshot", get(supervisor_snapshot))
        .route("/api/heartbeats", get(supervisor_heartbeats))
        .route("/api/runtime-snapshots", get(supervisor_runtime_snapshots))
        .route("/api/recovery", get(supervisor_recovery))
        .route("/api/processes", get(supervisor_processes))
        .route("/api/processes/:id", get(supervisor_process_detail));
    let router = if local_lifecycle_enabled {
        router.route(
            "/api/local/lifecycle/heartbeat",
            post(supervisor_ingest_heartbeat),
        )
    } else {
        router
    };
    router.with_state(state)
}

async fn supervisor_health(State(state): State<SupervisorApiState>) -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "component": "rustcta-supervisor",
        "registry_configured": !state.registry_store.path().as_os_str().is_empty(),
        "read_only": state.local_lifecycle_token.is_none(),
        "local_lifecycle_enabled": state.local_lifecycle_token.is_some(),
    }))
}

async fn supervisor_snapshot(
    State(state): State<SupervisorApiState>,
) -> Result<Json<SupervisorSnapshot>, StatusCode> {
    let registry = load_registry(Some(state.registry_store.as_ref()))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(registry.snapshot()))
}

async fn supervisor_processes(
    State(state): State<SupervisorApiState>,
) -> Result<Json<Vec<StrategyProcess>>, StatusCode> {
    let registry = load_registry(Some(state.registry_store.as_ref()))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(registry.list()))
}

async fn supervisor_process_detail(
    State(state): State<SupervisorApiState>,
    Path(id): Path<String>,
) -> Result<Json<StrategyProcess>, StatusCode> {
    let registry = load_registry(Some(state.registry_store.as_ref()))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    registry
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn supervisor_heartbeats(
    State(state): State<SupervisorApiState>,
) -> Result<Json<Vec<RuntimeHeartbeatStatus>>, StatusCode> {
    let registry = load_registry(Some(state.registry_store.as_ref()))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(registry.heartbeat_statuses(chrono::Utc::now())))
}

async fn supervisor_runtime_snapshots(
    State(state): State<SupervisorApiState>,
) -> Result<Json<Vec<RuntimeSnapshotStatus>>, StatusCode> {
    let registry = load_registry(Some(state.registry_store.as_ref()))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(registry.snapshot_statuses(chrono::Utc::now())))
}

async fn supervisor_recovery(
    State(state): State<SupervisorApiState>,
) -> Result<Json<SupervisorRecoveryStatus>, StatusCode> {
    let registry = load_registry(Some(state.registry_store.as_ref()))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(registry.recovery_status(chrono::Utc::now())))
}

async fn supervisor_ingest_heartbeat(
    State(state): State<SupervisorApiState>,
    headers: HeaderMap,
    Json(heartbeat): Json<RuntimeHeartbeat>,
) -> Result<Json<StrategyProcess>, StatusCode> {
    require_local_lifecycle_token(&state, &headers)?;
    let registry = load_registry(Some(state.registry_store.as_ref()))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut supervisor = LocalProcessSupervisor::from_registry(registry);
    let process = supervisor
        .ingest_runtime_heartbeat(heartbeat)
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    save_registry(Some(state.registry_store.as_ref()), supervisor.registry())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(process))
}

fn require_local_lifecycle_token(
    state: &SupervisorApiState,
    headers: &HeaderMap,
) -> Result<(), StatusCode> {
    let Some(expected) = state.local_lifecycle_token.as_deref() else {
        return Err(StatusCode::NOT_FOUND);
    };
    let actual = headers
        .get(LOCAL_LIFECYCLE_TOKEN_HEADER)
        .and_then(|value| value.to_str().ok());
    if actual == Some(expected.as_str()) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn load_registry(store: Option<&JsonFileProcessRegistryStore>) -> Result<ProcessRegistry> {
    Ok(match store {
        Some(store) => store.load()?,
        None => ProcessRegistry::default(),
    })
}

fn save_registry(
    store: Option<&JsonFileProcessRegistryStore>,
    registry: &ProcessRegistry,
) -> Result<()> {
    if let Some(store) = store {
        store.save(registry)?;
    }
    Ok(())
}

fn arg_value<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.windows(2)
        .find(|window| window[0] == name)
        .map(|window| window[1].as_str())
}

fn has_flag(args: &[String], name: &str) -> bool {
    args.iter().any(|arg| arg == name)
}

fn is_loopback_bind(bind_addr: &str) -> bool {
    bind_addr.starts_with("127.")
        || bind_addr.starts_with("localhost:")
        || bind_addr.starts_with("[::1]:")
        || bind_addr.starts_with("::1:")
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn supervisor_router_should_expose_read_only_health() {
        let store = JsonFileProcessRegistryStore::new("/tmp/rustcta-supervisor-test-missing.json");
        let response = supervisor_router(store, None)
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("health request should complete");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn supervisor_router_should_return_404_for_missing_process() {
        let store = JsonFileProcessRegistryStore::new("/tmp/rustcta-supervisor-test-missing.json");
        let response = supervisor_router(store, None)
            .oneshot(
                Request::builder()
                    .uri("/api/processes/missing")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("process detail request should complete");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn supervisor_router_should_expose_read_only_recovery_status() {
        let store = JsonFileProcessRegistryStore::new("/tmp/rustcta-supervisor-test-missing.json");
        let response = supervisor_router(store, None)
            .oneshot(
                Request::builder()
                    .uri("/api/recovery")
                    .body(Body::empty())
                    .expect("valid request"),
            )
            .await
            .expect("recovery request should complete");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn supervisor_router_should_not_register_lifecycle_mutation_without_token() {
        let store = JsonFileProcessRegistryStore::new("/tmp/rustcta-supervisor-test-missing.json");
        let response = supervisor_router(store, None)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/local/lifecycle/heartbeat")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"schema_version":1}"#))
                    .expect("valid request"),
            )
            .await
            .expect("heartbeat request should complete");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn supervisor_router_should_authenticate_local_lifecycle_heartbeat() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-supervisor-app-heartbeat-test-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let registry_path = temp_dir.join("registry.json");
        let store = JsonFileProcessRegistryStore::new(&registry_path);
        let mut registry = ProcessRegistry::default();
        registry.upsert(StrategyProcess::new(
            "strategy-heartbeat",
            "mock",
            "run",
            "tenant",
            "config.yml",
        ));
        store.save(&registry).expect("save registry");

        let response = supervisor_router(store, Some("token-1".to_string()))
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/local/lifecycle/heartbeat")
                    .header("content-type", "application/json")
                    .header(LOCAL_LIFECYCLE_TOKEN_HEADER, "token-1")
                    .body(Body::from(
                        r#"{
                            "schema_version":1,
                            "strategy_id":"strategy-heartbeat",
                            "run_id":"run",
                            "process_id":99,
                            "status":"Running",
                            "heartbeat_at":"2026-06-07T00:00:00Z",
                            "snapshot":{
                                "schema_version":1,
                                "snapshot_id":"snap-1",
                                "captured_at":"2026-06-07T00:00:00Z",
                                "source":"local",
                                "payload_kind":"strategy_state",
                                "payload_bytes":12,
                                "checksum":null
                            }
                        }"#,
                    ))
                    .expect("valid request"),
            )
            .await
            .expect("heartbeat request should complete");

        assert_eq!(response.status(), StatusCode::OK);
        let loaded = JsonFileProcessRegistryStore::new(&registry_path)
            .load()
            .expect("load updated registry");
        let process = loaded
            .get("strategy-heartbeat")
            .expect("heartbeat process should exist");
        assert_eq!(process.process_id, Some(99));
        assert_eq!(process.last_snapshot_at, process.last_heartbeat_at);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[test]
    fn local_lifecycle_token_should_require_loopback_bind() {
        assert!(is_loopback_bind("127.0.0.1:18181"));
        assert!(is_loopback_bind("localhost:18181"));
        assert!(is_loopback_bind("[::1]:18181"));
        assert!(!is_loopback_bind("0.0.0.0:18181"));
        assert!(!is_loopback_bind("192.168.1.10:18181"));
    }
}
