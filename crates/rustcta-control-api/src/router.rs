use axum::routing::{get, post};
use axum::Router;

use crate::routes;
use crate::ControlApiState;

pub fn router(state: ControlApiState) -> Router {
    Router::new()
        .route("/api/health", get(routes::health))
        .route("/api/status", get(routes::status))
        .route("/api/config", get(routes::config_summary))
        .route("/api/config/summary", get(routes::config_summary))
        .route("/api/workspace", get(routes::workspace))
        .route("/api/agents", get(routes::agents))
        .route("/api/agents/:id", get(routes::agent_detail))
        .route(
            "/api/strategies",
            get(routes::strategies).post(routes::create_strategy),
        )
        .route("/api/strategies/:id", get(routes::strategy_detail))
        .route(
            "/api/strategies/:id/snapshot",
            get(routes::strategy_snapshot),
        )
        .route("/api/strategies/:id/logs", get(routes::process_logs))
        .route(
            "/api/strategies/:id/command",
            post(routes::strategy_command),
        )
        .route("/api/commands", post(routes::command))
        .route("/api/commands/:id", get(routes::command_detail))
        .route("/api/processes", get(routes::processes))
        .route("/api/processes/:id", get(routes::process_detail))
        .route("/api/processes/:id/logs", get(routes::process_logs))
        .route("/api/gateway/status", get(routes::gateway_status))
        .route("/api/credentials/status", get(routes::credentials_status))
        .route("/api/risk", get(routes::risk))
        .route("/api/risk/events", get(routes::risk_events))
        .route("/api/fees", get(routes::fees))
        .route("/api/logs", get(routes::logs))
        .route("/api/inventory", get(routes::inventory))
        .route("/api/books", get(routes::books))
        .route("/api/exchanges", get(routes::exchanges))
        .route("/api/trades/recent", get(routes::recent_trades))
        .route(
            "/api/opportunities/recent",
            get(routes::recent_opportunities),
        )
        .route("/api/opportunities", get(routes::opportunities))
        .route("/api/symbols", get(routes::symbols))
        .route("/api/strategy-logs", get(routes::strategy_logs))
        .route("/api/events", get(routes::events))
        .route("/api/audit", get(routes::audit))
        .route("/api/control/audit", get(routes::audit))
        .with_state(state)
}
