use anyhow::Result;
use axum::Router;
use rustcta_control_api::{router, ControlApiState};
use rustcta_event_ledger::JsonlLedger;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::services::{ServeDir, ServeFile};

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:18080";
const DEFAULT_TENANT_ID: &str = "local";
const DEFAULT_SUPERVISOR_REGISTRY_PATH: &str = "run/supervisor/registry.json";
const DEFAULT_STRATEGY_LOG_TAIL_LINES: usize = 800;
const DEFAULT_STRATEGY_LOG_TAIL_BYTES: usize = 256 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlApiAppConfig {
    pub bind_addr: String,
    pub local_agent: Option<LocalAgentConfig>,
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

        Self {
            bind_addr,
            local_agent,
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
        let mut state = if let Some(path) = &self.legacy_snapshot_path {
            let raw = std::fs::read_to_string(path)?;
            let snapshot = serde_json::from_str::<serde_json::Value>(&raw)?;
            ControlApiState::from_legacy_dashboard_snapshot(&snapshot)
                .with_legacy_dashboard_snapshot_path(path.clone())
        } else {
            ControlApiState::empty_local()
        };

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
        let app = match &self.static_dir {
            Some(static_dir) => {
                let index = static_dir.join("index.html");
                router(state)
                    .fallback_service(ServeDir::new(static_dir).fallback(ServeFile::new(index)))
            }
            None => router(state),
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
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
        assert!(config.static_dir.is_none());
        assert!(config.strategy_log_tail_lines.is_none());
        assert_eq!(
            config.supervisor_registry_path,
            Some(PathBuf::from(DEFAULT_SUPERVISOR_REGISTRY_PATH))
        );
    }

    #[tokio::test]
    async fn app_router_should_serve_control_api_routes_from_configured_state() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-app-contract-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let registry_path = temp_dir.join("registry.json");
        let snapshot_path = temp_dir.join("dashboard_snapshot.json");
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
        std::fs::write(
            &snapshot_path,
            serde_json::json!({
                "live_trading_enabled": true,
                "risk_events": [],
                "spot_symbol_rules": [
                    {
                        "exchange": "binance",
                        "symbol": "BTC/USDT"
                    }
                ],
                "spot_control": {
                    "symbols": [
                        {
                            "symbol": "BTC/USDT",
                            "enabled": true
                        }
                    ]
                },
                "five_exchange_scanner": {
                    "symbol_coverage": [
                        {
                            "exchange": "binance",
                            "symbols": ["BTC/USDT"]
                        }
                    ],
                    "recommendations": [
                        {
                            "symbol": "BTC/USDT",
                            "action": "enable"
                        }
                    ]
                }
            })
            .to_string(),
        )
        .unwrap();

        let registry_path_text = registry_path.to_string_lossy().to_string();
        let snapshot_path_text = snapshot_path.to_string_lossy().to_string();
        let config = ControlApiAppConfig::from_env_iter([
            ("RUSTCTA_CONTROL_API_AGENT_ID", "agent-a"),
            ("RUSTCTA_CONTROL_API_TENANT_ID", "tenant-a"),
            (
                "RUSTCTA_CONTROL_API_SUPERVISOR_REGISTRY_PATH",
                registry_path_text.as_str(),
            ),
            (
                "RUSTCTA_CONTROL_API_LEGACY_SNAPSHOT_PATH",
                snapshot_path_text.as_str(),
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
        assert_eq!(value["symbol_rules"][0]["symbol"], "BTC/USDT");
        assert_eq!(value["spot_control"]["symbols"][0]["enabled"], true);
        assert_eq!(
            value["scanner"]["symbol_coverage"][0]["symbols"][0],
            "BTC/USDT"
        );

        std::fs::remove_dir_all(temp_dir).ok();
    }
}
