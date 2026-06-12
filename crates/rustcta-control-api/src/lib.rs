pub mod models;
pub mod read_models;
pub mod router;
pub mod routes;
pub mod state;

pub use models::*;
pub use router::router;
pub use state::ControlApiState;

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use chrono::Utc;
    use rustcta_event_ledger::{EventKind, InMemoryLedger, LedgerReader};
    use rustcta_supervisor::{
        LifecycleCommand, LifecycleCommandRecord, RuntimeSnapshotMetadata, StrategyProcess,
        SUPERVISOR_SCHEMA_VERSION,
    };
    use std::sync::Arc;
    use tower::ServiceExt;

    #[test]
    fn snapshot_should_be_secret_free_by_shape() {
        let snapshot = ControlApiStateSnapshot {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            generated_at: Utc::now(),
            strategies: Vec::new(),
            gateway: None,
            agents: Vec::new(),
            risk: Default::default(),
            fees: Default::default(),
            logs: Default::default(),
            inventory: Default::default(),
            books: Default::default(),
            exchanges: Default::default(),
            recent_trades: Default::default(),
            recent_opportunities: Default::default(),
            opportunities: Default::default(),
            symbols: Default::default(),
            runtime_control: Default::default(),
            strategy_snapshots: Vec::new(),
        };
        let value = serde_json::to_value(snapshot).unwrap();
        let text = value.to_string();
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));
    }

    #[test]
    fn credential_status_should_not_include_raw_secret_fields() {
        let status = CredentialStatusResponse {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            slots: vec![CredentialSlotStatus {
                slot_id: "slot-1".to_string(),
                exchange_id: "binance".to_string(),
                tenant_id: Some("tenant-a".to_string()),
                configured: true,
                last_verified_at: None,
                health: CredentialHealth::Unknown,
            }],
        };
        let text = serde_json::to_string(&status).unwrap();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
    }

    #[tokio::test]
    async fn generic_workspace_and_credentials_routes_should_return_public_dtos() {
        let app = router(ControlApiState::empty_local());

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
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["agent_count"], 0);
        assert_eq!(value["strategy_count"], 0);
        assert_eq!(value["risk_status"], "unknown");
        assert_eq!(value["active_risk_event_count"], 0);
        assert_eq!(value["log_event_count"], 0);

        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let body = axum::body::to_bytes(status.into_body(), usize::MAX)
            .await
            .unwrap();
        let status_value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status_value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(status_value["agent_count"], value["agent_count"]);
        assert_eq!(status_value["strategy_count"], value["strategy_count"]);

        for path in ["/api/config", "/api/config/summary"] {
            let config = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(config.status(), StatusCode::OK);
            let body = axum::body::to_bytes(config.into_body(), usize::MAX)
                .await
                .unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
            assert_eq!(value["agent_count"], 0);
            assert_eq!(value["process_count"], 0);
            assert_eq!(value["gateway_configured"], false);
            assert_eq!(value["credentials_status_only"], true);
            let text = value.to_string();
            assert!(!text.contains("api_key"));
            assert!(!text.contains("api_secret"));
            assert!(!text.contains("passphrase"));
            assert!(!text.contains("authorization"));
        }

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
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["slots"].as_array().unwrap().len(), 0);
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let inventory = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/inventory")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(inventory.status(), StatusCode::OK);
        let body = axum::body::to_bytes(inventory.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["rows"].as_array().unwrap().len(), 0);

        let books = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/books")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(books.status(), StatusCode::OK);
        let body = axum::body::to_bytes(books.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["rows"].as_array().unwrap().len(), 0);
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let exchanges = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/exchanges")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(exchanges.status(), StatusCode::OK);
        let body = axum::body::to_bytes(exchanges.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["rows"].as_array().unwrap().len(), 0);
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let recent_trades = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/trades/recent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recent_trades.status(), StatusCode::OK);
        let body = axum::body::to_bytes(recent_trades.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["rows"].as_array().unwrap().len(), 0);
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let recent_opportunities = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/opportunities/recent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recent_opportunities.status(), StatusCode::OK);
        let body = axum::body::to_bytes(recent_opportunities.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["rows"].as_array().unwrap().len(), 0);
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let opportunities = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/opportunities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(opportunities.status(), StatusCode::OK);
        let body = axum::body::to_bytes(opportunities.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["recent"].as_array().unwrap().len(), 0);
        assert_eq!(value["arbitrage"].as_array().unwrap().len(), 0);
        assert_eq!(value["statistics"].as_object().unwrap().len(), 0);
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

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
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["symbol_rules"].as_array().unwrap().len(), 0);
        assert_eq!(value["spot_control"].as_object().unwrap().len(), 0);
        assert_eq!(
            value["scanner"]["symbol_coverage"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            value["scanner"]["recommendations"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));
    }

    #[tokio::test]
    async fn public_control_api_should_not_expose_legacy_raw_key_routes() {
        let app = router(ControlApiState::empty_local());
        let legacy_key_route = format!("/api/{}-{}-{}", "exchange", "api", "keys");

        for request in [
            Request::builder()
                .method("GET")
                .uri(&legacy_key_route)
                .body(Body::empty())
                .unwrap(),
            Request::builder()
                .method("POST")
                .uri(&legacy_key_route)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
            Request::builder()
                .method("DELETE")
                .uri(format!("{legacy_key_route}/binance"))
                .body(Body::empty())
                .unwrap(),
        ] {
            let response = app.clone().oneshot(request).await.unwrap();
            assert!(
                matches!(
                    response.status(),
                    StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
                ),
                "legacy raw-key route returned unexpected status {}",
                response.status()
            );
            assert_ne!(response.status(), StatusCode::OK);
        }

        let credentials = app
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
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));
    }

    #[tokio::test]
    async fn detail_routes_should_return_agent_process_and_command_records() {
        let mut strategy =
            StrategyProcess::new("strategy-1", "mock", "run-1", "tenant-a", "config/mock.yml")
                .mark_started(42, Utc::now());
        strategy.log_path = Some("/tmp/strategy-1.log".to_string());
        let snapshot = ControlApiStateSnapshot {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            generated_at: Utc::now(),
            strategies: vec![strategy],
            gateway: None,
            agents: vec![AgentSummary {
                agent_id: "agent-a".to_string(),
                tenant_id: "tenant-a".to_string(),
                status: AgentConnectionStatus::Connected,
                last_heartbeat_at: None,
                capabilities: vec!["supervisor".to_string()],
            }],
            risk: Default::default(),
            fees: Default::default(),
            logs: Default::default(),
            inventory: Default::default(),
            books: Default::default(),
            exchanges: Default::default(),
            recent_trades: Default::default(),
            recent_opportunities: Default::default(),
            opportunities: Default::default(),
            symbols: Default::default(),
            runtime_control: Default::default(),
            strategy_snapshots: Vec::new(),
        };
        let state = ControlApiState::new(snapshot);
        let command = LifecycleCommandRecord {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            command_id: "cmd-detail".to_string(),
            strategy_id: "strategy-1".to_string(),
            run_id: Some("run-1".to_string()),
            command: LifecycleCommand::Restart,
            requested_by: Some("operator-a".to_string()),
            idempotency_key: "idem-detail".to_string(),
            requested_at: Utc::now(),
        };
        state.record_command(command).await.unwrap();
        let app = router(state);

        let agent = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/agents/agent-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(agent.status(), StatusCode::OK);
        let body = axum::body::to_bytes(agent.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["agent_id"], "agent-a");

        let process = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/processes/strategy-1")
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
        assert_eq!(value["strategy_id"], "strategy-1");
        assert_eq!(value["log_configured"], true);
        assert!(value.get("log_path").is_none());

        let strategy_snapshot = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/strategies/strategy-1/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(strategy_snapshot.status(), StatusCode::OK);
        let body = axum::body::to_bytes(strategy_snapshot.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["strategy_id"], "strategy-1");
        assert_eq!(value["strategy_kind"], "mock");
        assert_eq!(value["run_id"], "run-1");
        assert_eq!(value["status"], "Running");
        assert_eq!(value["source"], "supervisor");
        assert_eq!(value["detail"]["config_path"], "config/mock.yml");
        assert_eq!(value["detail"]["process_id"], 42);
        assert_eq!(value["detail"]["log_configured"], true);
        assert!(value["detail"].get("log_path").is_none());

        let strategy_snapshots = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/strategy-snapshots")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(strategy_snapshots.status(), StatusCode::OK);
        let body = axum::body::to_bytes(strategy_snapshots.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 1);
        assert_eq!(value[0]["strategy_id"], "strategy-1");

        let missing_agent = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/agents/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_agent.status(), StatusCode::NOT_FOUND);

        let detail = app
            .oneshot(
                Request::builder()
                    .uri("/api/commands/cmd-detail")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);
        let body = axum::body::to_bytes(detail.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["command_id"], "cmd-detail");
    }

    #[tokio::test]
    async fn local_agent_should_overlay_composed_snapshot() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-agent-overlay-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let registry_path = temp_dir.join("registry.json");
        let snapshot_path = temp_dir.join("dashboard_snapshot.json");
        std::fs::write(
            &registry_path,
            serde_json::json!({
                "schema_version": 1,
                "captured_at": "2026-06-07T12:00:02Z",
                "processes": []
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &snapshot_path,
            serde_json::json!({
                "live_trading_enabled": false,
                "risk_events": []
            })
            .to_string(),
        )
        .unwrap();
        let app = router(
            ControlApiState::empty_local()
                .with_local_agent(
                    "local-agent",
                    "tenant-a",
                    ["control-api", "supervisor-reader"],
                )
                .with_supervisor_registry_path(&registry_path)
                .with_legacy_dashboard_snapshot_path(&snapshot_path),
        );

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
        assert_eq!(value["agent_count"], 1);

        let agent = app
            .oneshot(
                Request::builder()
                    .uri("/api/agents/local-agent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(agent.status(), StatusCode::OK);
        let body = axum::body::to_bytes(agent.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["tenant_id"], "tenant-a");
        assert_eq!(value["status"], "connected");
        assert_eq!(value["capabilities"].as_array().unwrap().len(), 2);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn generic_risk_fees_and_logs_routes_should_return_public_empty_views() {
        let app = router(ControlApiState::empty_local());

        let risk = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/risk")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(risk.status(), StatusCode::OK);
        let body = axum::body::to_bytes(risk.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["status"], "unknown");
        assert_eq!(value["kill_switch_active"], false);
        assert_eq!(value["events"].as_array().unwrap().len(), 0);

        let risk_events = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/risk/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(risk_events.status(), StatusCode::OK);
        let body = axum::body::to_bytes(risk_events.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 0);

        let fees = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/fees")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fees.status(), StatusCode::OK);
        let body = axum::body::to_bytes(fees.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["total_fee_usdt"], 0.0);
        assert_eq!(value["venues"].as_array().unwrap().len(), 0);

        let logs = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/logs")
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
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["events"].as_array().unwrap().len(), 0);

        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
    }

    #[tokio::test]
    async fn strategy_logs_should_return_empty_view_when_unconfigured() {
        let app = router(ControlApiState::empty_local());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/strategy-logs")
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
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["configured"], false);
        assert_eq!(value["readable"], false);
        assert_eq!(value["events"].as_array().unwrap().len(), 0);
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
    }

    #[tokio::test]
    async fn strategy_logs_should_tail_and_redact_configured_file() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-strategy-log-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let path = temp_dir.join("strategy.log");
        std::fs::write(
            &path,
            [
                "2026-06-07T12:00:00Z INFO strategy booted",
                "2026-06-07T12:00:01Z DEBUG heartbeat ok",
                "\u{1b}[2m2026-06-07T12:00:02.000000Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \u{1b}[2mrustcta::cross_arb_live_runner\u{1b}[0m\u{1b}[2m:\u{1b}[0m cross-arb live runner status strategy_id=cross_arb_live",
                "2026-06-07T12:00:02.250000Z INFO rustcta::cross_arb_live_runner: cross-arb trade event action=cross_arb_open_decision_audit lifecycle=cross_arb_open_decision_audit symbol=ESPORTS/USDT failure_reason=\"display-only row has no executable order drafts\"",
                "\u{1b}[2m2026-06-07T12:00:02.500000Z\u{1b}[0m \u{1b}[32m INFO\u{1b}[0m \u{1b}[2mrustcta::cross_arb_live_runner\u{1b}[0m\u{1b}[2m:\u{1b}[0m cross-arb trade event action=cross_arb_open lifecycle=open symbol=ESPORTS/USDT",
                "2026-06-07 12:00:02.123 WARN stale book",
                "2026-06-07T12:00:03Z ERROR token appeared in source log",
            ]
            .join("\n"),
        )
        .unwrap();
        let app = router(
            ControlApiState::empty_local()
                .with_strategy_log_path(&path)
                .with_strategy_log_tail_limits(4, 4096),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/strategy-logs")
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
        assert_eq!(value["configured"], true);
        assert_eq!(value["readable"], true);
        assert_eq!(value["event_count"], 4);
        assert_eq!(value["counts"]["all"], 4);
        assert_eq!(value["counts"]["trade"], 1);
        assert_eq!(value["counts"]["warn"], 1);
        assert_eq!(value["counts"]["error"], 1);
        assert_eq!(value["counts"]["info"], 1);
        assert_eq!(value["events"][0]["level"], "info");
        assert_eq!(value["events"][0]["category"], "info");
        assert_eq!(value["events"][1]["level"], "info");
        assert_eq!(value["events"][1]["category"], "trade");
        assert_eq!(
            value["events"][1]["message"],
            "2026-06-07T12:00:02.500000Z  INFO rustcta::cross_arb_live_runner: cross-arb trade event action=cross_arb_open lifecycle=open symbol=ESPORTS/USDT"
        );
        assert_eq!(
            value["events"][1]["occurred_at"],
            "2026-06-07T12:00:02.500Z"
        );
        assert_eq!(value["events"][2]["level"], "warn");
        assert_eq!(value["events"][2]["category"], "warn");
        assert_eq!(
            value["events"][2]["message"],
            "2026-06-07 12:00:02.123 WARN stale book"
        );
        assert_eq!(
            value["events"][2]["occurred_at"],
            "2026-06-07T12:00:02.123Z"
        );
        assert_eq!(value["events"][3]["level"], "error");
        assert_eq!(value["events"][3]["category"], "error");
        assert_eq!(value["events"][3]["message"], "[redacted log line]");
        let text = value.to_string();
        assert!(!text.contains("token appeared"));
        assert!(!text.contains(path.to_string_lossy().as_ref()));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn legacy_dashboard_snapshot_should_back_public_read_routes() {
        let mut inventory_row = serde_json::Map::new();
        inventory_row.insert("exchange".to_string(), serde_json::json!("binance"));
        inventory_row.insert("symbol".to_string(), serde_json::json!("BTC/USDT"));
        inventory_row.insert("total_usdt".to_string(), serde_json::json!(125.5));
        inventory_row.insert(["api", "key"].join("_"), serde_json::json!("hidden"));
        inventory_row.insert(["api", "secret"].join("_"), serde_json::json!("hidden"));
        inventory_row.insert(["pass", "phrase"].join(""), serde_json::json!("hidden"));
        inventory_row.insert(["author", "ization"].join(""), serde_json::json!("hidden"));
        inventory_row.insert("apiKey".to_string(), serde_json::json!("hidden"));
        inventory_row.insert(
            "raw_payload".to_string(),
            serde_json::json!({"secret": "hidden"}),
        );
        let mut book_row = serde_json::Map::new();
        book_row.insert("exchange".to_string(), serde_json::json!("binance"));
        book_row.insert("symbol".to_string(), serde_json::json!("BTC/USDT"));
        book_row.insert("bid".to_string(), serde_json::json!(67500.0));
        book_row.insert("ask".to_string(), serde_json::json!(67501.0));
        book_row.insert(
            "nested".to_string(),
            serde_json::json!({
                ["api", "key"].join("_"): "hidden",
                "accessKey": "hidden",
                "visible": true
            }),
        );
        book_row.insert(["api", "secret"].join("_"), serde_json::json!("hidden"));
        book_row.insert(["pass", "phrase"].join(""), serde_json::json!("hidden"));
        book_row.insert(["author", "ization"].join(""), serde_json::json!("hidden"));
        let mut exchange_row = serde_json::Map::new();
        exchange_row.insert("exchange".to_string(), serde_json::json!("binance"));
        exchange_row.insert("status".to_string(), serde_json::json!("online"));
        exchange_row.insert("market_type".to_string(), serde_json::json!("spot"));
        exchange_row.insert(["api", "key"].join("_"), serde_json::json!("hidden"));
        exchange_row.insert(["api", "secret"].join("_"), serde_json::json!("hidden"));
        exchange_row.insert(["pass", "phrase"].join(""), serde_json::json!("hidden"));
        exchange_row.insert(["author", "ization"].join(""), serde_json::json!("hidden"));
        exchange_row.insert(
            "nested".to_string(),
            serde_json::json!({
                ["api", "secret"].join("_"): "hidden",
                "public": "visible"
            }),
        );
        let mut trade_row = serde_json::Map::new();
        trade_row.insert("exchange".to_string(), serde_json::json!("binance"));
        trade_row.insert("symbol".to_string(), serde_json::json!("BTC/USDT"));
        trade_row.insert("side".to_string(), serde_json::json!("buy"));
        trade_row.insert("price".to_string(), serde_json::json!(67500.0));
        trade_row.insert("qty".to_string(), serde_json::json!(0.01));
        trade_row.insert(["api", "key"].join("_"), serde_json::json!("hidden"));
        trade_row.insert(["api", "secret"].join("_"), serde_json::json!("hidden"));
        trade_row.insert(["pass", "phrase"].join(""), serde_json::json!("hidden"));
        trade_row.insert(["author", "ization"].join(""), serde_json::json!("hidden"));
        trade_row.insert(
            "nested".to_string(),
            serde_json::json!({
                ["api", "key"].join("_"): "hidden",
                "visible": "trade"
            }),
        );
        let mut opportunity_row = serde_json::Map::new();
        opportunity_row.insert("opportunity_id".to_string(), serde_json::json!("opp-1"));
        opportunity_row.insert("exchange".to_string(), serde_json::json!("binance"));
        opportunity_row.insert("symbol".to_string(), serde_json::json!("ETH/USDT"));
        opportunity_row.insert("spread_bps".to_string(), serde_json::json!(12.5));
        opportunity_row.insert("score".to_string(), serde_json::json!(0.87));
        opportunity_row.insert(["api", "key"].join("_"), serde_json::json!("hidden"));
        opportunity_row.insert(["api", "secret"].join("_"), serde_json::json!("hidden"));
        opportunity_row.insert(["pass", "phrase"].join(""), serde_json::json!("hidden"));
        opportunity_row.insert(["author", "ization"].join(""), serde_json::json!("hidden"));
        opportunity_row.insert(
            "nested".to_string(),
            serde_json::json!({
                ["api", "secret"].join("_"): "hidden",
                "visible": "opportunity"
            }),
        );
        let mut arbitrage_row = serde_json::Map::new();
        arbitrage_row.insert("opportunity_id".to_string(), serde_json::json!("arb-1"));
        arbitrage_row.insert("long_exchange".to_string(), serde_json::json!("binance"));
        arbitrage_row.insert("short_exchange".to_string(), serde_json::json!("okx"));
        arbitrage_row.insert("symbol".to_string(), serde_json::json!("ETH/USDT"));
        arbitrage_row.insert("spread_bps".to_string(), serde_json::json!(18.5));
        arbitrage_row.insert(["api", "key"].join("_"), serde_json::json!("hidden"));
        arbitrage_row.insert(["api", "secret"].join("_"), serde_json::json!("hidden"));
        arbitrage_row.insert(["pass", "phrase"].join(""), serde_json::json!("hidden"));
        arbitrage_row.insert(["author", "ization"].join(""), serde_json::json!("hidden"));
        arbitrage_row.insert(
            "nested".to_string(),
            serde_json::json!({
                ["author", "ization"].join(""): "hidden",
                "visible": "arbitrage"
            }),
        );
        let mut arbitrage_statistics = serde_json::Map::new();
        arbitrage_statistics.insert("total".to_string(), serde_json::json!(1));
        arbitrage_statistics.insert("profitable".to_string(), serde_json::json!(1));
        arbitrage_statistics.insert("max_spread_bps".to_string(), serde_json::json!(18.5));
        arbitrage_statistics.insert(["api", "secret"].join("_"), serde_json::json!("hidden"));
        arbitrage_statistics.insert(
            "nested".to_string(),
            serde_json::json!({
                ["pass", "phrase"].join(""): "hidden",
                "visible": "statistics"
            }),
        );
        let mut symbol_rule = serde_json::Map::new();
        symbol_rule.insert("exchange".to_string(), serde_json::json!("binance"));
        symbol_rule.insert("symbol".to_string(), serde_json::json!("BTC/USDT"));
        symbol_rule.insert("price_increment".to_string(), serde_json::json!("0.01"));
        symbol_rule.insert(["api", "key"].join("_"), serde_json::json!("hidden"));
        symbol_rule.insert(
            "nested".to_string(),
            serde_json::json!({
                ["api", "secret"].join("_"): "hidden",
                "visible": "rule"
            }),
        );
        let mut spot_control = serde_json::Map::new();
        spot_control.insert("enabled".to_string(), serde_json::json!(true));
        spot_control.insert("max_symbols".to_string(), serde_json::json!(20));
        spot_control.insert(["pass", "phrase"].join(""), serde_json::json!("hidden"));
        spot_control.insert(
            "nested".to_string(),
            serde_json::json!({
                ["author", "ization"].join(""): "hidden",
                "visible": "control"
            }),
        );
        spot_control.insert(
            "symbols".to_string(),
            serde_json::json!([
                {"symbol": "BTC/USDT", "state": "enabled", "api_key": "hidden"}
            ]),
        );
        spot_control.insert(
            "inventory".to_string(),
            serde_json::json!([
                {"symbol": "BTC/USDT", "free": 0.1, "secret": "hidden"},
                {"symbol": "ETH/USDT", "free": 1.0}
            ]),
        );
        spot_control.insert(
            "orders".to_string(),
            serde_json::json!([
                {"symbol": "BTC/USDT", "order_id": "order-1", "token": "hidden"}
            ]),
        );
        spot_control.insert(
            "commands".to_string(),
            serde_json::json!([
                {"symbol": "BTC/USDT", "command_id": "cmd-1", "passphrase": "hidden"}
            ]),
        );
        spot_control.insert(
            "runtime_snapshots".to_string(),
            serde_json::json!([
                {
                "symbol": "BTC/USDT",
                "readiness": {"ready": true},
                "inventory_ownership": {"owner": "spot-control"},
                "direction_readiness": {"buy": true, "sell": false},
                "liquidation_preview": {"would_submit_order": false},
                "data_health": {"books": "fresh"},
                "liquidation": {"status": "idle"},
                "raw_payload": {"adapter": "hidden"},
                "apiKey": "hidden",
                "api_secret": "hidden"
            }
            ]),
        );
        let mut symbol_coverage = serde_json::Map::new();
        symbol_coverage.insert("exchange".to_string(), serde_json::json!("binance"));
        symbol_coverage.insert("symbols".to_string(), serde_json::json!(["BTC/USDT"]));
        symbol_coverage.insert(["author", "ization"].join(""), serde_json::json!("hidden"));
        symbol_coverage.insert(
            "nested".to_string(),
            serde_json::json!({
                ["api", "key"].join("_"): "hidden",
                "visible": "coverage"
            }),
        );
        let mut recommendation = serde_json::Map::new();
        recommendation.insert("symbol".to_string(), serde_json::json!("BTC/USDT"));
        recommendation.insert("action".to_string(), serde_json::json!("enable"));
        recommendation.insert("reason".to_string(), serde_json::json!("liquid"));
        recommendation.insert(["api", "secret"].join("_"), serde_json::json!("hidden"));
        recommendation.insert(
            "nested".to_string(),
            serde_json::json!({
                ["pass", "phrase"].join(""): "hidden",
                "visible": "recommendation"
            }),
        );
        let legacy = serde_json::json!({
            "generated_at": "2026-06-07T12:00:05Z",
            "live_trading_enabled": true,
            "live_preflight_enabled": true,
            "kill_switch": {"active": true},
            "disabled": {
                "symbols": [{"symbol": "OLD/USDT", "api_key": "hidden"}],
                "exchanges": [{"exchange": "offline", "secret": "hidden"}]
            },
            "status": {
                "strategy_status": "running",
                "api_key": "hidden"
            },
            "config_summary": {
                "enabled_exchanges": ["binance", "bitget"],
                "enabled_symbols": ["BTC/USDT"],
                "secret": "hidden"
            },
            "runtime_publisher_health": {
                "status": "ok",
                "exchanges": [{"exchange": "binance", "healthy": true, "api_key": "hidden"}],
                "components": [{"component": "publisher", "healthy": true, "secret": "hidden"}],
                "errors": [{"message": "stale", "token": "hidden"}],
                "token": "hidden"
            },
            "live_preflight": {
                "ready": false,
                "checks": [{"check_id": "book-fresh", "status": "failed", "api_secret": "hidden"}],
                "blockers": ["book-fresh"],
                "per_symbol_readiness": [{"symbol": "BTC/USDT", "ready": false}]
            },
            "live_dry_run_orders": [
                {
                    "symbol": "BTC/USDT",
                    "api_secret": "hidden",
                    "visible": "dry-run"
                }
            ],
            "order_reconciliation_status": {
                "ready": true,
                "access_token": "hidden"
            },
            "balance_reconciliation": {
                "status": "ok",
                "refresh_token": "hidden"
            },
            "risk_events": [
                {
                    "timestamp": "2026-06-07T12:00:00Z",
                    "event_type": "RiskState",
                    "symbol": "BTC/USDT",
                    "exchange": "binance",
                    "severity": "critical",
                    "reason": "stale_book",
                    "details": "book age exceeded threshold"
                }
            ],
            "fees": [
                {
                    "exchange": "binance",
                    "market_type": "futures",
                    "maker_fee_rate": 0.0002,
                    "taker_fee_rate": 0.0004,
                    "fee_paid_usdt": 1.25,
                    "rebate_usdt": 0.25
                }
            ],
            "inventory": [serde_json::Value::Object(inventory_row)],
            "books": [serde_json::Value::Object(book_row)],
            "exchanges": [serde_json::Value::Object(exchange_row)],
            "trades": [serde_json::Value::Object(trade_row)],
            "opportunities": [serde_json::Value::Object(opportunity_row)],
            "arbitrage_opportunities": [serde_json::Value::Object(arbitrage_row)],
            "arbitrage_statistics": serde_json::Value::Object(arbitrage_statistics),
            "arbitrage_relationships": [
                {
                    "symbol": "ETH/USDT",
                    "api_key": "hidden",
                    "visible": "relationship"
                }
            ],
            "cross_arb_market_snapshots": [
                {
                    "symbol": "ETH/USDT",
                    "token": "hidden",
                    "visible": "market-snapshot"
                }
            ],
            "instrument_feasibility": {
                "known_symbols": 400,
                "secret": "hidden"
            },
            "spot_symbol_rules": [serde_json::Value::Object(symbol_rule)],
            "spot_control": serde_json::Value::Object(spot_control),
            "five_exchange_scanner": {
                "exchange_roles": [{"exchange": "binance", "role": "maker", "api_key": "hidden"}],
                "symbol_coverage": [serde_json::Value::Object(symbol_coverage)],
                "exchange_pairs": [{"left": "binance", "right": "okx", "secret": "hidden"}],
                "opportunities": [{"symbol": "BTC/USDT", "spread_bps": 5.0, "token": "hidden"}],
                "pair_statistics": [{"pair": "binance-okx", "count": 1, "passphrase": "hidden"}],
                "symbol_scores": [{"symbol": "BTC/USDT", "score": 0.9, "authorization": "hidden"}],
                "recommendations": [serde_json::Value::Object(recommendation)]
            },
            "hedge_policy": {
                "status": "ready",
                "inventory_risk": [{"symbol": "BTC/USDT", "risk": "low", "api_key": "hidden"}],
                "recommendations": [{"symbol": "BTC/USDT", "action": "hold", "secret": "hidden"}],
                "venue_capabilities": [{"exchange": "binance", "can_hedge": true, "token": "hidden"}],
                "market_regime": {"regime": "normal", "passphrase": "hidden"}
            }
        });
        let app = router(ControlApiState::from_legacy_dashboard_snapshot(&legacy));

        let risk = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/risk")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(risk.status(), StatusCode::OK);
        let body = axum::body::to_bytes(risk.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["status"], "blocked");
        assert_eq!(value["kill_switch_active"], true);
        assert_eq!(value["open_risk_event_count"], 1);
        assert_eq!(value["events"][0]["severity"], "critical");
        assert_eq!(value["events"][0]["scope"], "binance:BTC/USDT");

        let risk_events = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/risk/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(risk_events.status(), StatusCode::OK);
        let body = axum::body::to_bytes(risk_events.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 1);
        assert_eq!(value[0]["severity"], "critical");
        assert_eq!(
            value[0]["message"],
            "stale_book: book age exceeded threshold"
        );
        assert_eq!(value[0]["scope"], "binance:BTC/USDT");

        let fees = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/fees")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fees.status(), StatusCode::OK);
        let body = axum::body::to_bytes(fees.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["total_fee_usdt"], 1.25);
        assert_eq!(value["realized_rebate_usdt"], 0.25);
        assert_eq!(value["venues"][0]["exchange_id"], "binance");

        let logs = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/logs")
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
        assert_eq!(value["events"].as_array().unwrap().len(), 1);
        assert_eq!(value["events"][0]["level"], "error");
        assert!(value["events"][0]["message"]
            .as_str()
            .unwrap()
            .contains("stale_book"));

        let inventory = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/inventory")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(inventory.status(), StatusCode::OK);
        let body = axum::body::to_bytes(inventory.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["rows"].as_array().unwrap().len(), 1);
        assert_eq!(value["rows"][0]["exchange"], "binance");
        assert_eq!(value["rows"][0]["symbol"], "BTC/USDT");
        assert_eq!(value["rows"][0]["total_usdt"], 125.5);
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let books = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/books")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(books.status(), StatusCode::OK);
        let body = axum::body::to_bytes(books.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["rows"].as_array().unwrap().len(), 1);
        assert_eq!(value["rows"][0]["exchange"], "binance");
        assert_eq!(value["rows"][0]["symbol"], "BTC/USDT");
        assert_eq!(value["rows"][0]["bid"], 67500.0);
        assert_eq!(value["rows"][0]["ask"], 67501.0);
        assert_eq!(value["rows"][0]["nested"]["visible"], true);
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let exchanges = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/exchanges")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(exchanges.status(), StatusCode::OK);
        let body = axum::body::to_bytes(exchanges.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["rows"].as_array().unwrap().len(), 1);
        assert_eq!(value["rows"][0]["exchange"], "binance");
        assert_eq!(value["rows"][0]["status"], "online");
        assert_eq!(value["rows"][0]["market_type"], "spot");
        assert_eq!(value["rows"][0]["nested"]["public"], "visible");
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let recent_trades = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/trades/recent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recent_trades.status(), StatusCode::OK);
        let body = axum::body::to_bytes(recent_trades.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["rows"].as_array().unwrap().len(), 1);
        assert_eq!(value["rows"][0]["exchange"], "binance");
        assert_eq!(value["rows"][0]["symbol"], "BTC/USDT");
        assert_eq!(value["rows"][0]["side"], "buy");
        assert_eq!(value["rows"][0]["price"], 67500.0);
        assert_eq!(value["rows"][0]["qty"], 0.01);
        assert_eq!(value["rows"][0]["nested"]["visible"], "trade");
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let recent_opportunities = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/opportunities/recent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recent_opportunities.status(), StatusCode::OK);
        let body = axum::body::to_bytes(recent_opportunities.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["rows"].as_array().unwrap().len(), 1);
        assert_eq!(value["rows"][0]["opportunity_id"], "opp-1");
        assert_eq!(value["rows"][0]["exchange"], "binance");
        assert_eq!(value["rows"][0]["symbol"], "ETH/USDT");
        assert_eq!(value["rows"][0]["spread_bps"], 12.5);
        assert_eq!(value["rows"][0]["score"], 0.87);
        assert_eq!(value["rows"][0]["nested"]["visible"], "opportunity");
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let opportunities = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/opportunities")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(opportunities.status(), StatusCode::OK);
        let body = axum::body::to_bytes(opportunities.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["recent"].as_array().unwrap().len(), 1);
        assert_eq!(value["recent"][0]["opportunity_id"], "opp-1");
        assert_eq!(value["recent"][0]["nested"]["visible"], "opportunity");
        assert_eq!(value["arbitrage"].as_array().unwrap().len(), 1);
        assert_eq!(value["arbitrage"][0]["opportunity_id"], "arb-1");
        assert_eq!(value["arbitrage"][0]["long_exchange"], "binance");
        assert_eq!(value["arbitrage"][0]["short_exchange"], "okx");
        assert_eq!(value["arbitrage"][0]["nested"]["visible"], "arbitrage");
        assert_eq!(value["statistics"]["total"], 1);
        assert_eq!(value["statistics"]["profitable"], 1);
        assert_eq!(value["statistics"]["max_spread_bps"], 18.5);
        assert_eq!(value["statistics"]["nested"]["visible"], "statistics");
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        let symbols = app
            .clone()
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
        assert_eq!(value["schema_version"], CONTROL_API_SCHEMA_VERSION);
        assert_eq!(value["symbol_rules"].as_array().unwrap().len(), 1);
        assert_eq!(value["symbol_rules"][0]["exchange"], "binance");
        assert_eq!(value["symbol_rules"][0]["symbol"], "BTC/USDT");
        assert_eq!(value["symbol_rules"][0]["price_increment"], "0.01");
        assert_eq!(value["symbol_rules"][0]["nested"]["visible"], "rule");
        assert_eq!(value["spot_control"]["enabled"], true);
        assert_eq!(value["spot_control"]["max_symbols"], 20);
        assert_eq!(value["spot_control"]["nested"]["visible"], "control");
        assert_eq!(
            value["scanner"]["symbol_coverage"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            value["scanner"]["symbol_coverage"][0]["exchange"],
            "binance"
        );
        assert_eq!(
            value["scanner"]["symbol_coverage"][0]["symbols"][0],
            "BTC/USDT"
        );
        assert_eq!(
            value["scanner"]["symbol_coverage"][0]["nested"]["visible"],
            "coverage"
        );
        assert_eq!(
            value["scanner"]["recommendations"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(value["scanner"]["recommendations"][0]["symbol"], "BTC/USDT");
        assert_eq!(value["scanner"]["recommendations"][0]["action"], "enable");
        assert_eq!(
            value["scanner"]["recommendations"][0]["nested"]["visible"],
            "recommendation"
        );
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));

        for (path, expected_pointer, expected_value) in [
            ("/api/disabled", "/data/symbols/0/symbol", "OLD/USDT"),
            ("/api/dry_run_plans", "/data/0/visible", "dry-run"),
            ("/api/live_dry_run/orders", "/data/0/visible", "dry-run"),
            (
                "/api/live_preflight",
                "/data/checks/0/check_id",
                "book-fresh",
            ),
            (
                "/api/live_preflight/checks",
                "/data/0/check_id",
                "book-fresh",
            ),
            (
                "/api/live_preflight/summary",
                "/data/blockers/0",
                "book-fresh",
            ),
            ("/api/order_reconciliation/status", "/data/ready", "true"),
            ("/api/balance_reconciliation", "/data/status", "ok"),
            (
                "/api/spot-arb/dashboard",
                "/data/live_dry_run_orders/0/visible",
                "dry-run",
            ),
            (
                "/api/cross-arb/dashboard",
                "/data/hedge_policy/status",
                "ready",
            ),
            ("/api/scanner/exchanges", "/data/0/role", "maker"),
            (
                "/api/scanner/symbol-coverage",
                "/data/0/exchange",
                "binance",
            ),
            ("/api/scanner/exchange-pairs", "/data/0/left", "binance"),
            ("/api/scanner/opportunities", "/data/0/symbol", "BTC/USDT"),
            (
                "/api/scanner/pair-statistics",
                "/data/0/pair",
                "binance-okx",
            ),
            ("/api/scanner/symbol-scores", "/data/0/symbol", "BTC/USDT"),
            ("/api/scanner/recommendations", "/data/0/action", "enable"),
            ("/api/hedge-policy/status", "/data/status", "ready"),
            ("/api/hedge-policy/inventory-risk", "/data/0/risk", "low"),
            (
                "/api/hedge-policy/recommendations",
                "/data/0/action",
                "hold",
            ),
            (
                "/api/hedge-policy/venue-capabilities",
                "/data/0/exchange",
                "binance",
            ),
            ("/api/hedge-policy/market-regime", "/data/regime", "normal"),
            (
                "/api/control/runtime-publisher/status",
                "/data/status",
                "ok",
            ),
            (
                "/api/control/runtime-publisher/exchanges",
                "/data/0/exchange",
                "binance",
            ),
            (
                "/api/control/runtime-publisher/components",
                "/data/0/component",
                "publisher",
            ),
            (
                "/api/control/runtime-publisher/errors",
                "/data/0/message",
                "stale",
            ),
            ("/api/control/symbols", "/data/0/state", "enabled"),
            ("/api/control/symbols/BTCUSDT", "/data/state", "enabled"),
            (
                "/api/control/symbols/BTCUSDT/orders",
                "/data/0/order_id",
                "order-1",
            ),
            (
                "/api/control/symbols/BTCUSDT/commands",
                "/data/0/command_id",
                "cmd-1",
            ),
            (
                "/api/control/symbols/BTCUSDT/liquidation",
                "/data/status",
                "idle",
            ),
            (
                "/api/control/symbols/BTCUSDT/readiness",
                "/data/ready",
                "true",
            ),
            (
                "/api/control/symbols/BTCUSDT/inventory-ownership",
                "/data/owner",
                "spot-control",
            ),
            (
                "/api/control/symbols/BTCUSDT/direction-readiness",
                "/data/buy",
                "true",
            ),
            (
                "/api/control/symbols/BTCUSDT/liquidation-preview",
                "/data/would_submit_order",
                "false",
            ),
            (
                "/api/control/symbols/BTCUSDT/data-health",
                "/data/books",
                "fresh",
            ),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                value["schema_version"], CONTROL_API_SCHEMA_VERSION,
                "{path}"
            );
            let actual = value
                .pointer(expected_pointer)
                .unwrap_or(&serde_json::Value::Null);
            match actual {
                serde_json::Value::Bool(value) => {
                    assert_eq!(value.to_string(), expected_value, "{path}")
                }
                serde_json::Value::Number(value) => {
                    assert_eq!(value.to_string(), expected_value, "{path}")
                }
                serde_json::Value::String(value) => assert_eq!(value, expected_value, "{path}"),
                other => panic!("{path} returned unexpected value at {expected_pointer}: {other}"),
            }
            let text = value.to_string();
            assert!(!text.contains("api_key"), "{path}");
            assert!(!text.contains("api_secret"), "{path}");
            assert!(!text.contains("passphrase"), "{path}");
            assert!(!text.contains("authorization"), "{path}");
            assert!(!text.contains("secret"), "{path}");
            assert!(!text.contains("token"), "{path}");
            assert!(!text.contains("access_token"), "{path}");
            assert!(!text.contains("refresh_token"), "{path}");
        }

        let inventory_view = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/control/symbols/BTCUSDT/inventory")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(inventory_view.status(), StatusCode::OK);
        let body = axum::body::to_bytes(inventory_view.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["data"].as_array().unwrap().len(), 1);
        assert_eq!(value["data"][0]["symbol"], "BTC/USDT");
        assert_eq!(value["data"][0]["free"], 0.1);
        assert!(!value.to_string().contains("secret"));

        let runtime_snapshot = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/control/symbols/BTCUSDT/runtime-snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(runtime_snapshot.status(), StatusCode::OK);
        let body = axum::body::to_bytes(runtime_snapshot.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["data"]["symbol"], "BTC/USDT");
        assert_eq!(value["data"]["readiness"]["ready"], true);
        assert!(!value.to_string().contains("api_secret"));

        let snapshots = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/strategy-snapshots")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(snapshots.status(), StatusCode::OK);
        let body = axum::body::to_bytes(snapshots.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value.as_array().unwrap().len(), 2);
        assert_eq!(value[0]["source"], "legacy_dashboard");
        assert_eq!(value[0]["strategy_id"], "spot-arb-local");
        assert_eq!(value[0]["strategy_kind"], "spot_spot_taker_arbitrage");
        assert_eq!(value[0]["generated_at"], "2026-06-07T12:00:05Z");
        assert_eq!(value[0]["detail"]["spot_control"]["enabled"], true);
        assert_eq!(
            value[0]["detail"]["live_dry_run_orders"][0]["visible"],
            "dry-run"
        );
        assert_eq!(value[0]["detail"]["balance_reconciliation"]["status"], "ok");
        assert_eq!(value[1]["strategy_id"], "contract-arb-local");
        assert_eq!(value[1]["strategy_kind"], "cross_exchange_arbitrage");
        assert_eq!(
            value[1]["detail"]["arbitrage_relationships"][0]["visible"],
            "relationship"
        );
        assert_eq!(
            value[1]["detail"]["market_snapshots"][0]["visible"],
            "market-snapshot"
        );
        assert_eq!(
            value[1]["detail"]["instrument_feasibility"]["known_symbols"],
            400
        );
        assert_eq!(
            value[1]["detail"]["scanner"]["recommendations"][0]["action"],
            "enable"
        );
        let text = value.to_string();
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));
        assert!(!text.contains("authorization"));
        assert!(!text.contains("secret"));
        assert!(!text.contains("token"));
        assert!(!text.contains("access_token"));
        assert!(!text.contains("refresh_token"));
        assert!(!text.contains("apiKey"));
        assert!(!text.contains("accessKey"));
        assert!(!text.contains("raw_payload"));

        let spot_snapshot = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/strategies/spot-arb-local/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(spot_snapshot.status(), StatusCode::OK);
        let body = axum::body::to_bytes(spot_snapshot.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["source"], "legacy_dashboard");
        assert_eq!(value["detail"]["spot_control"]["enabled"], true);
        assert!(!value.to_string().contains("api_key"));

        let cross_snapshot = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/strategies/contract-arb-local/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cross_snapshot.status(), StatusCode::OK);
        let body = axum::body::to_bytes(cross_snapshot.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["source"], "legacy_dashboard");
        assert_eq!(
            value["detail"]["arbitrage_opportunities"][0]["opportunity_id"],
            "arb-1"
        );
        assert!(!value.to_string().contains("api_secret"));
    }

    #[tokio::test]
    async fn legacy_dashboard_snapshot_path_should_refresh_on_each_request() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-snapshot-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let path = temp_dir.join("dashboard_snapshot.json");
        std::fs::write(
            &path,
            serde_json::json!({
                "live_trading_enabled": false,
                "risk_events": []
            })
            .to_string(),
        )
        .unwrap();
        let app = router(ControlApiState::empty_local().with_legacy_dashboard_snapshot_path(&path));

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/risk")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        let body = axum::body::to_bytes(first.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["open_risk_event_count"], 0);

        std::fs::write(
            &path,
            serde_json::json!({
                "live_trading_enabled": true,
                "risk_events": [
                    {
                        "timestamp": "2026-06-07T12:00:00Z",
                        "event_type": "RiskState",
                        "severity": "warning",
                        "reason": "latency"
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let second = app
            .oneshot(
                Request::builder()
                    .uri("/api/risk")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::OK);
        let body = axum::body::to_bytes(second.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["status"], "warning");
        assert_eq!(value["open_risk_event_count"], 1);

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn extra_strategy_snapshot_path_should_append_typed_runtime_snapshot() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-extra-strategy-snapshot-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let legacy_path = temp_dir.join("dashboard_snapshot.json");
        let extra_path = temp_dir.join("spot_futures_snapshot.json");
        std::fs::write(
            &legacy_path,
            serde_json::json!({
                "status": "running",
                "cross_arb_dashboard": {
                    "summary": {"active": true}
                }
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            &extra_path,
            serde_json::json!({
                "schema_version": 1,
                "strategy_id": "spot_futures_arb_live",
                "strategy_kind": "spot_futures_arbitrage",
                "run_id": "server-live",
                "status": null,
                "generated_at": "2026-06-10T16:00:00Z",
                "source": "typed_runtime_snapshot",
                "detail": {
                    "active_symbols": ["ORDI/USDT"],
                    "settings": {"enable_live_trading": false}
                }
            })
            .to_string(),
        )
        .unwrap();

        let app = router(
            ControlApiState::empty_local()
                .with_legacy_dashboard_snapshot_path(&legacy_path)
                .with_extra_strategy_snapshot_path(&extra_path),
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/strategy-snapshots")
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
        assert!(value
            .as_array()
            .unwrap()
            .iter()
            .any(|snapshot| snapshot["strategy_kind"] == "cross_exchange_arbitrage"));
        let spot_futures = value
            .as_array()
            .unwrap()
            .iter()
            .find(|snapshot| snapshot["strategy_kind"] == "spot_futures_arbitrage")
            .unwrap();
        assert_eq!(spot_futures["strategy_id"], "spot_futures_arb_live");
        assert_eq!(spot_futures["source"], "typed_runtime_snapshot");
        assert_eq!(
            spot_futures["detail"]["active_symbols"][0],
            serde_json::json!("ORDI/USDT")
        );

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn extra_snapshot_paths_should_aggregate_cross_arb_shard_dashboards() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-cross-arb-shards-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let shard0 = temp_dir.join("cross_arb_shard_0.json");
        let shard1 = temp_dir.join("cross_arb_shard_1.json");
        for (path, shard_id, symbol) in [(&shard0, 0, "EDGE/USDT"), (&shard1, 1, "DRIFT/USDT")] {
            std::fs::write(
                path,
                serde_json::json!({
                    "generated_at": "2026-06-11T10:00:00Z",
                    "opportunities": [{
                        "opportunity_id": format!("opp-{shard_id}"),
                        "symbol": symbol
                    }],
                    "market_snapshots": [{
                        "exchange": "binance",
                        "symbol": symbol
                    }],
                    "cross_arb_dashboard": {
                        "summary": {
                            "strategy_id": format!("cross_arb_live_shard_{shard_id}")
                        },
                        "opportunities": [{
                            "opportunity_id": format!("opp-{shard_id}"),
                            "symbol": symbol
                        }]
                    }
                })
                .to_string(),
            )
            .unwrap();
        }

        let snapshot = ControlApiState::empty_local()
            .with_extra_strategy_snapshot_paths([&shard0, &shard1])
            .snapshot()
            .await;

        assert_eq!(snapshot.opportunities.recent.len(), 2);
        assert_eq!(snapshot.books.rows.len(), 0);
        assert_eq!(snapshot.strategy_snapshots.len(), 2);
        assert!(snapshot
            .strategy_snapshots
            .iter()
            .any(|snapshot| snapshot.strategy_id == "cross_arb_live_shard_0"));
        assert!(snapshot
            .strategy_snapshots
            .iter()
            .any(|snapshot| snapshot.strategy_id == "cross_arb_live_shard_1"));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn cross_arb_dashboard_should_use_runner_dashboard_snapshot() {
        let legacy = serde_json::json!({
            "generated_at": "2026-06-08T10:00:00Z",
            "live_trading_enabled": true,
            "risk_events": [],
            "cross_arb_dashboard": {
                "data_source": "cross-exchange-arbitrage-live-runner",
                "online": true,
                "summary": {
                    "strategy_id": "cross_arb_live",
                    "enabled_exchanges": ["binance", "bitget", "gateio"],
                    "configured_symbols": 3,
                    "api_secret": "hidden"
                },
                "settings": {
                    "symbols": ["BTC/USDT", "ETH/USDT"],
                    "market_type": "perpetual"
                },
                "arbitrage_results": [
                    {
                        "bundle_id": "arb-1",
                        "actual_pnl_usdt": "1.25",
                        "secret": "hidden"
                    }
                ],
                "profit_summary": {
                    "realized_profit_usdt": 1.25,
                    "closed_arbitrages": 1
                },
                "market_snapshots": [
                    {
                        "exchange": "binance",
                        "canonical_symbol": "H/USDT",
                        "best_bid_price": 0.194,
                        "token": "hidden"
                    }
                ],
                "instruments": [
                    {
                        "exchange": "binance",
                        "canonical_symbol": "H/USDT",
                        "market_type": "perpetual",
                        "api_key": "hidden"
                    }
                ],
                "instrument_feasibility": {
                    "known_symbols": 3,
                    "known_exchanges": 3,
                    "secret": "hidden"
                },
                "exchange_status": [
                    {
                        "exchange": "binance",
                        "status": "ready"
                    }
                ]
            }
        });
        let app = router(ControlApiState::from_legacy_dashboard_snapshot(&legacy));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cross-arb/dashboard")
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
        assert_eq!(
            value["data"]["data_source"],
            "cross-exchange-arbitrage-live-runner"
        );
        assert_eq!(value["data"]["summary"]["configured_symbols"], 3);
        assert_eq!(value["data"]["arbitrage_results"][0]["bundle_id"], "arb-1");
        assert_eq!(
            value["data"]["profit_summary"]["realized_profit_usdt"],
            1.25
        );
        assert!(!value.to_string().contains("api_secret"));
        assert!(!value.to_string().contains("secret"));

        let snapshots = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/strategy-snapshots")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(snapshots.status(), StatusCode::OK);
        let body = axum::body::to_bytes(snapshots.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value[0]["strategy_kind"], "cross_exchange_arbitrage");
        assert_eq!(value[0]["detail"]["exchange_status"][0]["status"], "ready");
        assert!(!value.to_string().contains("api_secret"));

        let instruments = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cross-arb/instruments")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(instruments.status(), StatusCode::OK);
        let body = axum::body::to_bytes(instruments.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["coverage_ok"], true);
        assert_eq!(value["instruments"][0]["canonical_symbol"], "H/USDT");
        assert_eq!(value["feasibility"]["known_symbols"], 3);
        assert!(!value.to_string().contains("api_key"));
        assert!(!value.to_string().contains("secret"));

        let market_snapshots = app
            .oneshot(
                Request::builder()
                    .uri("/api/cross-arb/market-snapshots")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(market_snapshots.status(), StatusCode::OK);
        let body = axum::body::to_bytes(market_snapshots.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["coverage_ok"], true);
        assert_eq!(value["snapshots"][0]["canonical_symbol"], "H/USDT");
        assert!(!value.to_string().contains("token"));
    }

    #[tokio::test]
    async fn supervisor_registry_path_should_back_processes_and_process_logs() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-supervisor-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        std::fs::create_dir_all(&temp_dir).unwrap();
        let registry_path = temp_dir.join("registry.json");
        let log_path = temp_dir.join("strategy.log");
        std::fs::write(
            &log_path,
            [
                "2026-06-07T12:00:00Z INFO process booted",
                "2026-06-07T12:00:01Z WARN process lagging",
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
                        "strategy_id": "strategy-a",
                        "strategy_kind": "mock",
                        "run_id": "run-a",
                        "tenant_id": "tenant-a",
                        "config_path": "config/mock.yml",
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
        let app = router(
            ControlApiState::empty_local()
                .with_supervisor_registry_path(&registry_path)
                .with_strategy_log_tail_limits(10, 4096),
        );

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
        assert_eq!(value.as_array().unwrap().len(), 1);
        assert_eq!(value[0]["strategy_id"], "strategy-a");

        let logs = app
            .oneshot(
                Request::builder()
                    .uri("/api/processes/strategy-a/logs")
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
        assert_eq!(value["target"], "strategy-a");
        assert_eq!(value["configured"], true);
        assert_eq!(value["readable"], true);
        assert_eq!(value["events"].as_array().unwrap().len(), 2);
        assert_eq!(value["events"][1]["level"], "warn");
        assert!(!value
            .to_string()
            .contains(log_path.to_string_lossy().as_ref()));

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn legacy_snapshot_should_not_hide_supervisor_registry_processes() {
        let temp_dir = std::env::temp_dir().join(format!(
            "rustcta-control-api-composed-snapshot-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
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
                        "strategy_id": "strategy-b",
                        "strategy_kind": "mock",
                        "run_id": "run-b",
                        "tenant_id": "tenant-b",
                        "config_path": "config/mock.yml",
                        "status": "Running",
                        "process_id": 456,
                        "started_at": "2026-06-07T12:00:00Z",
                        "last_heartbeat_at": "2026-06-07T12:00:01Z",
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
                "risk_events": [
                    {
                        "timestamp": "2026-06-07T12:00:00Z",
                        "event_type": "RiskState",
                        "severity": "warning",
                        "reason": "latency"
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();
        let app = router(
            ControlApiState::empty_local()
                .with_supervisor_registry_path(&registry_path)
                .with_legacy_dashboard_snapshot_path(&snapshot_path),
        );

        let workspace = app
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
        assert_eq!(value["process_count"], 1);
        assert_eq!(value["strategy_count"], 1);
        assert_eq!(value["risk_status"], "warning");

        std::fs::remove_dir_all(temp_dir).ok();
    }

    #[tokio::test]
    async fn recorded_operator_command_should_write_audit_event() {
        let ledger = Arc::new(InMemoryLedger::new());
        let state = ControlApiState::empty_local().with_audit_ledger(ledger.clone());
        let command = LifecycleCommandRecord {
            schema_version: SUPERVISOR_SCHEMA_VERSION,
            command_id: "cmd-1".to_string(),
            strategy_id: "strategy-1".to_string(),
            run_id: Some("run-1".to_string()),
            command: LifecycleCommand::Restart,
            requested_by: Some("operator-a".to_string()),
            idempotency_key: "idem-1".to_string(),
            requested_at: Utc::now(),
        };

        state.record_command(command).await.unwrap();
        let app = router(state);
        let events = ledger.replay(None).await.expect("replay audit events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::OperatorCommandEvent);
        assert_eq!(events[0].identity.command_id.as_deref(), Some("cmd-1"));
        assert_eq!(
            events[0].identity.idempotency_key.as_deref(),
            Some("idem-1")
        );
        let text = serde_json::to_string(&events[0]).unwrap();
        assert!(text.contains("operator_lifecycle_command"));
        assert!(!text.contains("api_key"));
        assert!(!text.contains("api_secret"));
        assert!(!text.contains("passphrase"));

        let event_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(event_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(event_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["commands"].as_array().unwrap().len(), 1);
        assert_eq!(value["ledger_events"].as_array().unwrap().len(), 1);
        assert_eq!(value["ledger_events"][0]["kind"], "operator_command_event");

        let audit_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(audit_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(audit_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["events"].as_array().unwrap().len(), 1);
        assert_eq!(value["events"][0]["kind"], "operator_command_event");

        let alias_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/control/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(alias_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(alias_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["events"].as_array().unwrap().len(), 1);
        assert_eq!(value["events"][0]["kind"], "operator_command_event");
    }

    #[tokio::test]
    async fn event_routes_should_filter_attached_ledger_from_sequence() {
        let ledger = Arc::new(InMemoryLedger::new());
        let state = ControlApiState::empty_local().with_audit_ledger(ledger);

        for index in 1..=2 {
            let command = LifecycleCommandRecord {
                schema_version: SUPERVISOR_SCHEMA_VERSION,
                command_id: format!("cmd-{index}"),
                strategy_id: "strategy-1".to_string(),
                run_id: Some("run-1".to_string()),
                command: LifecycleCommand::Restart,
                requested_by: Some("operator-a".to_string()),
                idempotency_key: format!("idem-{index}"),
                requested_at: Utc::now(),
            };

            state.record_command(command).await.unwrap();
        }
        let app = router(state);

        let event_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/events?from_sequence=2")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(event_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(event_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["from_sequence"], 2);
        assert_eq!(value["commands"].as_array().unwrap().len(), 2);
        assert_eq!(value["ledger_events"].as_array().unwrap().len(), 1);
        assert_eq!(value["ledger_events"][0]["sequence"], 2);
        assert_eq!(value["ledger_events"][0]["identity"]["command_id"], "cmd-2");

        for path in [
            "/api/audit?from_sequence=2",
            "/api/control/audit?from_sequence=2",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["from_sequence"], 2);
            assert_eq!(value["events"].as_array().unwrap().len(), 1);
            assert_eq!(value["events"][0]["sequence"], 2);
            assert_eq!(value["events"][0]["identity"]["command_id"], "cmd-2");
        }

        let invalid_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/events?from_sequence=abc")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn event_routes_should_return_empty_lists_without_ledger() {
        let app = router(ControlApiState::empty_local());

        let event_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/events")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(event_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(event_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["commands"].as_array().unwrap().len(), 0);
        assert_eq!(value["ledger_events"].as_array().unwrap().len(), 0);

        let audit_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(audit_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(audit_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["events"].as_array().unwrap().len(), 0);

        let alias_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/control/audit")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(alias_response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(alias_response.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["events"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn public_router_should_not_expose_strategy_mutations() {
        let app = router(ControlApiState::empty_local());

        for request in [
            Request::builder()
                .method("POST")
                .uri("/api/strategies")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "strategy_id": "spot-arb-a",
                        "strategy_kind": "spot_spot_arbitrage",
                    })
                    .to_string(),
                ))
                .unwrap(),
            Request::builder()
                .method("POST")
                .uri("/api/commands")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
            Request::builder()
                .method("POST")
                .uri("/api/strategies/spot-arb-a/command")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        ] {
            let response = app.clone().oneshot(request).await.unwrap();
            assert!(
                matches!(
                    response.status(),
                    StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
                ),
                "unexpected mutation status {}",
                response.status()
            );
        }
    }

    #[tokio::test]
    async fn runtime_control_api_snapshot_should_back_public_read_routes() {
        let generated_at = Utc::now();
        let mut process = StrategyProcess::new(
            "spot-spot-taker-arbitrage",
            "spot_spot_taker_arbitrage",
            "local",
            "local",
            "config/spot.yml",
        );
        process.schema_version = SUPERVISOR_SCHEMA_VERSION;
        process.status = rustcta_supervisor::ProcessStatus::Running;
        process.last_heartbeat_at = Some(generated_at);
        process.last_snapshot_at = Some(generated_at);
        let mut runtime_snapshot = RuntimeSnapshotMetadata::new("snapshot-1", generated_at);
        runtime_snapshot.source = Some("typed-runtime".to_string());
        runtime_snapshot.payload_kind = Some("control_api_state_snapshot".to_string());
        process.runtime_snapshot = Some(runtime_snapshot);

        let runtime = ControlApiStateSnapshot {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            generated_at,
            strategies: vec![process],
            gateway: None,
            agents: Vec::new(),
            risk: RiskSummaryView {
                schema_version: CONTROL_API_SCHEMA_VERSION,
                status: RiskStatus::Ready,
                live_orders_enabled: true,
                ..RiskSummaryView::default()
            },
            fees: Default::default(),
            logs: Default::default(),
            inventory: JsonRowsView {
                schema_version: CONTROL_API_SCHEMA_VERSION,
                rows: vec![serde_json::json!({
                    "exchange": "gateio",
                    "symbol": "BTCUSDT",
                    "available": 1.0
                })],
            },
            books: Default::default(),
            exchanges: Default::default(),
            recent_trades: Default::default(),
            recent_opportunities: Default::default(),
            opportunities: Default::default(),
            symbols: Default::default(),
            runtime_control: Default::default(),
            strategy_snapshots: Vec::new(),
        };
        let value = serde_json::to_value(runtime).unwrap();
        let app = router(ControlApiState::from_legacy_dashboard_snapshot(&value));

        let strategy = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/strategies/spot-spot-taker-arbitrage")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(strategy.status(), StatusCode::OK);
        let body = axum::body::to_bytes(strategy.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["strategy_id"], "spot-spot-taker-arbitrage");
        assert_eq!(value["runtime_snapshot"]["snapshot_id"], "snapshot-1");
        assert_eq!(
            value["runtime_snapshot"]["payload_kind"],
            "control_api_state_snapshot"
        );

        let snapshot = app
            .oneshot(
                Request::builder()
                    .uri("/api/strategies/spot-spot-taker-arbitrage/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(snapshot.status(), StatusCode::OK);
        let body = axum::body::to_bytes(snapshot.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["source"], "supervisor");
        assert_eq!(
            value["detail"]["runtime_snapshot"]["snapshot_id"],
            "snapshot-1"
        );
        assert_eq!(value["detail"]["config_path"], "config/spot.yml");
    }

    #[tokio::test]
    async fn typed_spot_control_runtime_snapshot_should_back_runtime_publisher_routes() {
        let generated_at = "2026-06-08T08:30:00Z";
        let typed_snapshot = serde_json::json!({
            "generated_at": generated_at,
            "snapshot_id": "spot-runtime-1",
            "symbol": "BTCUSDT",
            "selected_exchanges": ["gateio", "bitget"],
            "symbol_rules": {
                "gateio:BTCUSDT": {
                    "exchange": "gateio",
                    "market_type": "spot",
                    "internal_symbol": "BTCUSDT",
                    "exchange_symbol": "BTC_USDT",
                    "base_asset": "BTC",
                    "quote_asset": "USDT",
                    "price_precision": 2,
                    "quantity_precision": 6,
                    "tick_size": 0.01,
                    "step_size": 0.000001,
                    "min_quantity": 0.00001,
                    "min_notional": 5.0,
                    "status": "tradable"
                }
            },
            "books": {
                "gateio:BTCUSDT": {
                    "exchange": "gateio",
                    "market_type": "spot",
                    "symbol": "BTCUSDT",
                    "bids": [{"price": 70000.0, "quantity": 0.5}],
                    "asks": [{"price": 70001.0, "quantity": 0.4}],
                    "received_at": generated_at
                }
            },
            "recent_trades": [
                {
                    "exchange": "gateio",
                    "market_type": "spot",
                    "symbol": "BTCUSDT",
                    "trade_id": "trade-1",
                    "side": "buy",
                    "price": 70000.5,
                    "quantity": 0.1,
                    "received_at": generated_at
                }
            ],
            "funding_rates": [
                {
                    "exchange": "bitget",
                    "market_type": "perpetual",
                    "symbol": "BTCUSDT",
                    "funding_rate": 0.0001,
                    "observed_at": generated_at
                }
            ],
            "route_health": [
                {
                    "route_id": "public:gateio:spot:BTCUSDT:book",
                    "component": "gateway_public_stream",
                    "status": "healthy",
                    "last_success_at": generated_at,
                    "latency_ms": 42,
                    "observed_at": generated_at
                }
            ],
            "runtime_publisher": {
                "publisher_id": "spot-control",
                "status": "ok",
                "running": true,
                "source": "typed_runtime_snapshot",
                "last_snapshot_id": "spot-runtime-1",
                "last_snapshot_at": generated_at,
                "snapshots_generated": 3,
                "snapshots_persisted": 3,
                "route_health": [
                    {
                        "route_id": "public:gateio:spot:BTCUSDT:book",
                        "component": "gateway_public_stream",
                        "status": "healthy",
                        "latency_ms": 42,
                        "observed_at": generated_at
                    }
                ],
                "errors": [],
                "observed_at": generated_at
            },
            "book_health": [],
            "exchange_health": [
                {
                    "component": "exchange:gateio",
                    "status": "fresh",
                    "updated_at": generated_at,
                    "source": "gateway_public_stream"
                }
            ],
            "fees": {},
            "balances": [],
            "reservations": [],
            "inventory_ownership": [
                {
                    "exchange": "gateio",
                    "market_type": "spot",
                    "symbol": "BTCUSDT",
                    "asset": "BTC",
                    "total_balance": 1.0,
                    "exchange_locked": 0.0,
                    "locally_reserved": 0.0,
                    "unmanaged_quantity": 0.0,
                    "other_strategy_owned_quantity": 0.0,
                    "spot_control_managed_quantity": 1.0,
                    "effective_sellable_managed_quantity": 1.0,
                    "ownership_known": true,
                    "source": "authoritative_runtime_snapshot",
                    "reasons": []
                }
            ],
            "unmanaged_positions": [],
            "disabled_state": [],
            "open_orders": [],
            "fill_ownership": [],
            "direction_readiness": [],
            "effective_tradability": null,
            "liquidation_preview": {
                "market_plans": [],
                "passive_sessions": [],
                "dust": [],
                "rejected": false,
                "reasons": []
            },
            "data_sources": ["gateway_public_stream"],
            "schema_version": 1,
            "source_metadata": [],
            "consistency_report": null,
            "component_statuses": [
                {
                    "component": "books",
                    "status": "fresh",
                    "updated_at": generated_at,
                    "source": "gateway_public_stream"
                }
            ],
            "warnings": [],
            "critical_errors": []
        });
        let app = router(ControlApiState::from_legacy_dashboard_snapshot(
            &typed_snapshot,
        ));

        for (path, expected_pointer, expected_value) in [
            (
                "/api/control/runtime-publisher/status",
                "/data/source",
                "typed_runtime_snapshot",
            ),
            (
                "/api/control/runtime-publisher/status",
                "/data/last_snapshot_id",
                "spot-runtime-1",
            ),
            (
                "/api/control/runtime-publisher/status",
                "/data/route_health/0/component",
                "gateway_public_stream",
            ),
            (
                "/api/control/runtime-publisher/exchanges",
                "/data/0/component",
                "exchange:gateio",
            ),
            (
                "/api/control/runtime-publisher/components",
                "/data/0/component",
                "books",
            ),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                value.pointer(expected_pointer).unwrap(),
                expected_value,
                "{path}"
            );
        }

        let symbol_snapshot = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/control/symbols/BTCUSDT/runtime-snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(symbol_snapshot.status(), StatusCode::OK);
        let body = axum::body::to_bytes(symbol_snapshot.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["generated_at"], generated_at);
        assert_eq!(value["data"]["snapshot_id"], "spot-runtime-1");
        assert_eq!(
            value["data"]["books"]["gateio:BTCUSDT"]["exchange"],
            "gateio"
        );
        assert_eq!(value["data"]["recent_trades"][0]["trade_id"], "trade-1");
        assert_eq!(value["data"]["funding_rates"][0]["funding_rate"], 0.0001);
        assert_eq!(
            value["data"]["route_health"][0]["route_id"],
            "public:gateio:spot:BTCUSDT:book"
        );

        let strategy_snapshots = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/strategy-snapshots")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(strategy_snapshots.status(), StatusCode::OK);
        let body = axum::body::to_bytes(strategy_snapshots.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value[0]["source"], "typed_runtime_snapshot");
        assert_eq!(
            value[0]["detail"]["runtime_publisher"]["route_health"][0]["component"],
            "gateway_public_stream"
        );

        let books = app
            .oneshot(
                Request::builder()
                    .uri("/api/books")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(books.status(), StatusCode::OK);
        let body = axum::body::to_bytes(books.into_body(), usize::MAX)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(value["rows"][0]["symbol"], "BTCUSDT");
    }

    #[tokio::test]
    async fn runtime_control_live_preflight_routes_should_use_secret_free_read_model() {
        let generated_at = Utc::now();
        let live_preflight = serde_json::json!({
            "ready": false,
            "checks": [
                {
                    "name": "account_stream",
                    "status": "warn",
                    "api_key": "hidden"
                }
            ],
            "blockers": ["account_stream"],
            "per_symbol_readiness": [
                {
                    "symbol": "BTC/USDT",
                    "ready": false,
                    "api_secret": "hidden"
                }
            ],
            "token": "hidden"
        });
        let legacy = serde_json::json!({
            "live_preflight": live_preflight,
            "small_live_gate": {
                "status": "blocked",
                "passphrase": "hidden"
            }
        });
        let runtime_control = read_models::runtime_control_from_legacy_snapshot(&legacy);
        let runtime_control_text = serde_json::to_string(&runtime_control).unwrap();
        assert!(!runtime_control_text.contains("api_key"));
        assert!(!runtime_control_text.contains("api_secret"));
        assert!(!runtime_control_text.contains("passphrase"));
        assert!(!runtime_control_text.contains("token"));

        let snapshot = ControlApiStateSnapshot {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            generated_at,
            strategies: Vec::new(),
            gateway: None,
            agents: Vec::new(),
            risk: RiskSummaryView {
                schema_version: CONTROL_API_SCHEMA_VERSION,
                status: RiskStatus::Ready,
                ..RiskSummaryView::default()
            },
            fees: Default::default(),
            logs: Default::default(),
            inventory: Default::default(),
            books: Default::default(),
            exchanges: Default::default(),
            recent_trades: Default::default(),
            recent_opportunities: Default::default(),
            opportunities: Default::default(),
            symbols: Default::default(),
            runtime_control,
            strategy_snapshots: Vec::new(),
        };
        let app = router(ControlApiState::new(snapshot));

        for (path, pointer, expected) in [
            (
                "/api/live_preflight",
                "/data/checks/0/name",
                "account_stream",
            ),
            (
                "/api/live_preflight/checks",
                "/data/0/name",
                "account_stream",
            ),
            (
                "/api/live_preflight/summary",
                "/data/blockers/0",
                "account_stream",
            ),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            let body = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                value.pointer(pointer).and_then(serde_json::Value::as_str),
                Some(expected)
            );
            let text = value.to_string();
            assert!(!text.contains("api_key"), "{path}");
            assert!(!text.contains("api_secret"), "{path}");
            assert!(!text.contains("passphrase"), "{path}");
            assert!(!text.contains("token"), "{path}");
        }
    }
}
