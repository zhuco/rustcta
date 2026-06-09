use axum::routing::get;
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
        .route("/api/strategies", get(routes::strategies))
        .route("/api/strategies/:id", get(routes::strategy_detail))
        .route("/api/strategy-snapshots", get(routes::strategy_snapshots))
        .route(
            "/api/strategies/:id/snapshot",
            get(routes::strategy_snapshot),
        )
        .route("/api/strategies/:id/logs", get(routes::process_logs))
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
        .route("/api/disabled", get(routes::disabled))
        .route("/api/dry_run_plans", get(routes::dry_run_plans))
        .route("/api/live_dry_run/orders", get(routes::dry_run_plans))
        .route("/api/live_preflight", get(routes::live_preflight))
        .route(
            "/api/live_preflight/checks",
            get(routes::live_preflight_checks),
        )
        .route(
            "/api/live_preflight/summary",
            get(routes::live_preflight_summary),
        )
        .route(
            "/api/order_reconciliation/status",
            get(routes::order_reconciliation_status),
        )
        .route(
            "/api/balance_reconciliation",
            get(routes::balance_reconciliation),
        )
        .route("/api/spot-arb/dashboard", get(routes::spot_arb_dashboard))
        .route("/api/cross-arb/dashboard", get(routes::cross_arb_dashboard))
        .route(
            "/api/cross-arb/instruments",
            get(routes::cross_arb_instruments),
        )
        .route(
            "/api/cross-arb/market-snapshots",
            get(routes::cross_arb_market_snapshots),
        )
        .route("/api/scanner/exchanges", get(routes::scanner_exchanges))
        .route(
            "/api/scanner/symbol-coverage",
            get(routes::scanner_symbol_coverage),
        )
        .route(
            "/api/scanner/exchange-pairs",
            get(routes::scanner_exchange_pairs),
        )
        .route(
            "/api/scanner/opportunities",
            get(routes::scanner_opportunities),
        )
        .route(
            "/api/scanner/pair-statistics",
            get(routes::scanner_pair_statistics),
        )
        .route(
            "/api/scanner/symbol-scores",
            get(routes::scanner_symbol_scores),
        )
        .route(
            "/api/scanner/recommendations",
            get(routes::scanner_recommendations),
        )
        .route("/api/hedge-policy/status", get(routes::hedge_policy_status))
        .route(
            "/api/hedge-policy/inventory-risk",
            get(routes::hedge_policy_inventory_risk),
        )
        .route(
            "/api/hedge-policy/recommendations",
            get(routes::hedge_policy_recommendations),
        )
        .route(
            "/api/hedge-policy/venue-capabilities",
            get(routes::hedge_policy_venue_capabilities),
        )
        .route(
            "/api/hedge-policy/market-regime",
            get(routes::hedge_policy_market_regime),
        )
        .route("/api/control/symbols", get(routes::control_symbols))
        .route("/api/control/symbols/:symbol", get(routes::control_symbol))
        .route(
            "/api/control/symbols/:symbol/inventory",
            get(routes::control_symbol_inventory),
        )
        .route(
            "/api/control/symbols/:symbol/orders",
            get(routes::control_symbol_orders),
        )
        .route(
            "/api/control/symbols/:symbol/commands",
            get(routes::control_symbol_commands),
        )
        .route(
            "/api/control/symbols/:symbol/liquidation",
            get(routes::control_symbol_liquidation),
        )
        .route(
            "/api/control/symbols/:symbol/runtime-snapshot",
            get(routes::control_symbol_runtime_snapshot),
        )
        .route(
            "/api/control/symbols/:symbol/readiness",
            get(routes::control_symbol_readiness),
        )
        .route(
            "/api/control/symbols/:symbol/inventory-ownership",
            get(routes::control_symbol_inventory_ownership),
        )
        .route(
            "/api/control/symbols/:symbol/direction-readiness",
            get(routes::control_symbol_direction_readiness),
        )
        .route(
            "/api/control/symbols/:symbol/liquidation-preview",
            get(routes::control_symbol_liquidation_preview),
        )
        .route(
            "/api/control/symbols/:symbol/data-health",
            get(routes::control_symbol_data_health),
        )
        .route(
            "/api/control/runtime-publisher/status",
            get(routes::runtime_publisher_status),
        )
        .route(
            "/api/control/runtime-publisher/exchanges",
            get(routes::runtime_publisher_exchanges),
        )
        .route(
            "/api/control/runtime-publisher/components",
            get(routes::runtime_publisher_components),
        )
        .route(
            "/api/control/runtime-publisher/errors",
            get(routes::runtime_publisher_errors),
        )
        .route("/api/strategy-logs", get(routes::strategy_logs))
        .route("/api/events", get(routes::events))
        .route("/api/audit", get(routes::audit))
        .route("/api/control/audit", get(routes::audit))
        .with_state(state)
}
