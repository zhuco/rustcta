use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use super::auth::{authorize, AuthDecision};
use super::models::*;
use super::state::status_from_model;
use super::MonitoringState;
use crate::control::spot_control::{
    CommandStatus, DisableSymbolRequest, EnableMode, EnableSymbolRequest, EnabledDirection,
    MarkDustUnmanagedRequest, RuntimeSnapshotBuildRequest, SpotControlSnapshotBuilder,
    SpotControlSnapshotReplay, VersionedSymbolRequest,
};
use crate::strategies::arbitrage_core::filter_opportunities;

pub fn router(state: MonitoringState) -> Router {
    Router::new()
        .route("/api/status", axum::routing::get(status))
        .route("/api/exchanges", axum::routing::get(exchanges))
        .route("/api/books", axum::routing::get(books))
        .route(
            "/api/opportunities/recent",
            axum::routing::get(recent_opportunities),
        )
        .route("/api/trades/recent", axum::routing::get(recent_trades))
        .route("/api/inventory", axum::routing::get(inventory))
        .route("/api/fees", axum::routing::get(fees))
        .route("/api/disabled", axum::routing::get(disabled))
        .route(
            "/api/unmanaged_positions",
            axum::routing::get(unmanaged_positions),
        )
        .route("/api/risk/events", axum::routing::get(risk_events))
        .route("/api/recorder", axum::routing::get(recorder))
        .route("/api/config/summary", axum::routing::get(config_summary))
        .route("/api/live_preflight", axum::routing::get(live_preflight))
        .route(
            "/api/live_preflight/checks",
            axum::routing::get(live_preflight_checks),
        )
        .route(
            "/api/live_preflight/summary",
            axum::routing::get(live_preflight_summary),
        )
        .route(
            "/api/live_dry_run/orders",
            axum::routing::get(live_dry_run_orders),
        )
        .route(
            "/api/order_reconciliation/status",
            axum::routing::get(order_reconciliation_status),
        )
        .route(
            "/api/balance_reconciliation",
            axum::routing::get(balance_reconciliation),
        )
        .route("/api/kill_switch", axum::routing::get(kill_switch))
        .route(
            "/api/kill_switch/trigger",
            axum::routing::post(kill_switch_trigger),
        )
        .route(
            "/api/kill_switch/reset",
            axum::routing::post(kill_switch_reset),
        )
        .route("/api/small_live_gate", axum::routing::get(small_live_gate))
        .route(
            "/api/arbitrage/relationships",
            axum::routing::get(arbitrage_relationships),
        )
        .route(
            "/api/arbitrage/opportunities",
            axum::routing::get(arbitrage_opportunities),
        )
        .route(
            "/api/arbitrage/opportunities/:id",
            axum::routing::get(arbitrage_opportunity_by_id),
        )
        .route(
            "/api/arbitrage/rankings",
            axum::routing::get(arbitrage_rankings),
        )
        .route("/api/arbitrage/fees", axum::routing::get(arbitrage_fees))
        .route(
            "/api/arbitrage/capital-efficiency",
            axum::routing::get(arbitrage_capital_efficiency),
        )
        .route(
            "/api/arbitrage/statistics",
            axum::routing::get(arbitrage_statistics),
        )
        .route(
            "/api/scanner/exchanges",
            axum::routing::get(scanner_exchanges),
        )
        .route(
            "/api/scanner/symbol-coverage",
            axum::routing::get(scanner_symbol_coverage),
        )
        .route(
            "/api/scanner/exchange-pairs",
            axum::routing::get(scanner_exchange_pairs),
        )
        .route(
            "/api/scanner/opportunities",
            axum::routing::get(scanner_opportunities),
        )
        .route(
            "/api/scanner/pair-statistics",
            axum::routing::get(scanner_pair_statistics),
        )
        .route(
            "/api/scanner/symbol-scores",
            axum::routing::get(scanner_symbol_scores),
        )
        .route(
            "/api/scanner/recommendations",
            axum::routing::get(scanner_recommendations),
        )
        .route(
            "/api/hedge-policy/status",
            axum::routing::get(hedge_policy_status),
        )
        .route(
            "/api/hedge-policy/inventory-risk",
            axum::routing::get(hedge_policy_inventory_risk),
        )
        .route(
            "/api/hedge-policy/recommendations",
            axum::routing::get(hedge_policy_recommendations),
        )
        .route(
            "/api/hedge-policy/venue-capabilities",
            axum::routing::get(hedge_policy_venue_capabilities),
        )
        .route(
            "/api/hedge-policy/market-regime",
            axum::routing::get(hedge_policy_market_regime),
        )
        .route("/api/control/symbols", axum::routing::get(control_symbols))
        .route(
            "/api/control/symbols/enable",
            axum::routing::post(control_enable_symbol),
        )
        .route(
            "/api/control/symbols/:symbol",
            axum::routing::get(control_symbol),
        )
        .route(
            "/api/control/symbols/:symbol/inventory",
            axum::routing::get(control_symbol_inventory),
        )
        .route(
            "/api/control/symbols/:symbol/orders",
            axum::routing::get(control_symbol_orders),
        )
        .route(
            "/api/control/symbols/:symbol/commands",
            axum::routing::get(control_symbol_commands),
        )
        .route(
            "/api/control/symbols/:symbol/liquidation",
            axum::routing::get(control_symbol_liquidation),
        )
        .route(
            "/api/control/symbols/:symbol/runtime-snapshot",
            axum::routing::get(control_symbol_runtime_snapshot),
        )
        .route(
            "/api/control/symbols/:symbol/readiness",
            axum::routing::get(control_symbol_readiness),
        )
        .route(
            "/api/control/symbols/:symbol/inventory-ownership",
            axum::routing::get(control_symbol_inventory_ownership),
        )
        .route(
            "/api/control/symbols/:symbol/direction-readiness",
            axum::routing::get(control_symbol_direction_readiness),
        )
        .route(
            "/api/control/symbols/:symbol/liquidation-preview",
            axum::routing::get(control_symbol_liquidation_preview),
        )
        .route(
            "/api/control/symbols/:symbol/data-health",
            axum::routing::get(control_symbol_data_health),
        )
        .route(
            "/api/control/symbols/:symbol/pause",
            axum::routing::post(control_pause_symbol),
        )
        .route(
            "/api/control/symbols/:symbol/resume",
            axum::routing::post(control_resume_symbol),
        )
        .route(
            "/api/control/symbols/:symbol/disable",
            axum::routing::post(control_disable_symbol),
        )
        .route(
            "/api/control/symbols/:symbol/cancel-orders",
            axum::routing::post(control_cancel_orders),
        )
        .route(
            "/api/control/symbols/:symbol/confirm-liquidation",
            axum::routing::post(control_confirm_liquidation),
        )
        .route(
            "/api/control/symbols/:symbol/stop-liquidation",
            axum::routing::post(control_stop_liquidation),
        )
        .route(
            "/api/control/symbols/:symbol/mark-dust-unmanaged",
            axum::routing::post(control_mark_dust_unmanaged),
        )
        .route(
            "/api/control/commands/:command_id",
            axum::routing::get(control_command),
        )
        .route("/api/control/audit", axum::routing::get(control_audit))
        .route(
            "/api/control/runtime-publisher/status",
            axum::routing::get(control_runtime_publisher_status),
        )
        .route(
            "/api/control/runtime-publisher/exchanges",
            axum::routing::get(control_runtime_publisher_exchanges),
        )
        .route(
            "/api/control/runtime-publisher/components",
            axum::routing::get(control_runtime_publisher_components),
        )
        .route(
            "/api/control/runtime-publisher/errors",
            axum::routing::get(control_runtime_publisher_errors),
        )
        .route(
            "/api/control/symbols/:symbol/snapshots",
            axum::routing::get(control_symbol_snapshots),
        )
        .route(
            "/api/control/snapshots/:snapshot_id",
            axum::routing::get(control_snapshot_by_id),
        )
        .route(
            "/api/control/snapshots/:snapshot_id/replay",
            axum::routing::get(control_snapshot_replay),
        )
        .route(
            "/api/control/symbols/:symbol/open-order-ownership",
            axum::routing::get(control_symbol_open_order_ownership),
        )
        .route(
            "/api/control/symbols/:symbol/fill-ownership",
            axum::routing::get(control_symbol_fill_ownership),
        )
        .route(
            "/api/control/symbols/:symbol/consistency",
            axum::routing::get(control_symbol_consistency),
        )
        .with_state(state)
}

async fn status(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    guarded(&state, &headers, state.status().await).await
}

async fn exchanges(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.exchanges).await
}

async fn books(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Query(query): Query<BookQuery>,
) -> Response {
    let mut books = state.snapshot().await.books;
    if let Some(exchange) = query.exchange {
        let exchange = exchange.trim().to_ascii_lowercase();
        books.retain(|book| book.exchange == exchange);
    }
    if let Some(market_type) = query.market_type {
        books.retain(|book| book.market_type == market_type);
    }
    if let Some(symbol) = query.symbol {
        let symbol = normalize_symbol(&symbol);
        books.retain(|book| normalize_symbol(&book.symbol) == symbol);
    }
    if query.stale_only {
        books.retain(|book| book.is_stale);
    }
    guarded(&state, &headers, books).await
}

async fn recent_opportunities(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.opportunities).await
}

async fn recent_trades(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.trades).await
}

async fn inventory(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.inventory).await
}

async fn fees(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.fees).await
}

async fn disabled(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.disabled).await
}

async fn unmanaged_positions(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.unmanaged_positions).await
}

async fn risk_events(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.risk_events).await
}

async fn recorder(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.recorder).await
}

async fn config_summary(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    let mut summary = snapshot.config_summary;
    summary.secrets_redacted = true;
    guarded(&state, &headers, summary).await
}

async fn live_preflight(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.live_preflight).await
}

async fn live_preflight_checks(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    let checks = snapshot
        .live_preflight
        .map(|report| report.checks)
        .unwrap_or_default();
    guarded(&state, &headers, checks).await
}

async fn live_preflight_summary(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    let summary = snapshot.live_preflight.map(|report| {
        serde_json::json!({
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
    });
    guarded(&state, &headers, summary).await
}

async fn live_dry_run_orders(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.live_dry_run_orders).await
}

async fn order_reconciliation_status(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        serde_json::json!({
            "config": snapshot.order_reconciliation_config,
            "last_result": snapshot.order_reconciliation_status,
        }),
    )
    .await
}

async fn balance_reconciliation(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.balance_reconciliation).await
}

async fn kill_switch(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.kill_switch).await
}

async fn kill_switch_trigger(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Json(body): Json<KillSwitchTriggerRequest>,
) -> Response {
    if !state.config().require_token {
        return StatusCode::NOT_FOUND.into_response();
    }
    match authorize(&headers, state.config()) {
        AuthDecision::Allowed => {
            state
                .update_model(|model| {
                    let mut current =
                        model
                            .kill_switch
                            .clone()
                            .unwrap_or(crate::risk::KillSwitchState {
                                enabled: true,
                                active: false,
                                reason: None,
                                triggered_by: None,
                                triggered_at: None,
                                allow_paper_trading: true,
                                allow_live_dry_run: true,
                                allow_live_orders: false,
                            });
                    current.active = true;
                    current.reason = Some(body.reason.clone());
                    current.triggered_by = Some(
                        body.triggered_by
                            .clone()
                            .unwrap_or_else(|| "api".to_string()),
                    );
                    current.triggered_at = Some(chrono::Utc::now());
                    current.allow_live_dry_run = false;
                    current.allow_live_orders = false;
                    model.kill_switch = Some(current);
                })
                .await;
            let snapshot = state.snapshot().await;
            Json(snapshot.kill_switch).into_response()
        }
        AuthDecision::Unauthorized => StatusCode::UNAUTHORIZED.into_response(),
    }
}

async fn kill_switch_reset(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    if !state.config().require_token {
        return StatusCode::NOT_FOUND.into_response();
    }
    match authorize(&headers, state.config()) {
        AuthDecision::Allowed => {
            state
                .update_model(|model| {
                    if let Some(current) = model.kill_switch.as_mut() {
                        current.active = false;
                        current.reason = None;
                        current.triggered_by = None;
                        current.triggered_at = None;
                        current.allow_live_dry_run = true;
                        current.allow_live_orders = false;
                    }
                })
                .await;
            let snapshot = state.snapshot().await;
            Json(snapshot.kill_switch).into_response()
        }
        AuthDecision::Unauthorized => StatusCode::UNAUTHORIZED.into_response(),
    }
}

async fn small_live_gate(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.small_live_gate).await
}

async fn arbitrage_relationships(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.arbitrage_relationships).await
}

async fn arbitrage_opportunities(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Query(query): Query<ArbitrageQuery>,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        filter_opportunities(snapshot.arbitrage_opportunities, query),
    )
    .await
}

async fn arbitrage_opportunity_by_id(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let snapshot = state.snapshot().await;
    let value = snapshot
        .arbitrage_opportunities
        .into_iter()
        .find(|item| item.opportunity_id == id);
    guarded(&state, &headers, value).await
}

async fn arbitrage_rankings(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let mut values = state.snapshot().await.arbitrage_opportunities;
    values.sort_by(|left, right| {
        right
            .risk_adjusted_score
            .partial_cmp(&left.risk_adjusted_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    guarded(&state, &headers, values).await
}

async fn arbitrage_fees(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        serde_json::json!({
            "fees": snapshot.fees,
            "opportunity_fee_sources": snapshot.arbitrage_opportunities.iter().map(|item| {
                serde_json::json!({
                    "opportunity_id": item.opportunity_id,
                    "fee_sources": item.fee_sources,
                    "warnings": item.warnings,
                })
            }).collect::<Vec<_>>()
        }),
    )
    .await
}

async fn arbitrage_capital_efficiency(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    let values = snapshot
        .arbitrage_opportunities
        .into_iter()
        .map(|item| {
            serde_json::json!({
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
    guarded(&state, &headers, values).await
}

async fn arbitrage_statistics(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.arbitrage_statistics).await
}

async fn scanner_exchanges(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        snapshot.five_exchange_scanner.exchange_roles,
    )
    .await
}

async fn scanner_symbol_coverage(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        snapshot.five_exchange_scanner.symbol_coverage,
    )
    .await
}

async fn scanner_exchange_pairs(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        snapshot.five_exchange_scanner.exchange_pair_coverage,
    )
    .await
}

async fn scanner_opportunities(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        snapshot.five_exchange_scanner.opportunities,
    )
    .await
}

async fn scanner_pair_statistics(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        snapshot.five_exchange_scanner.pair_statistics,
    )
    .await
}

async fn scanner_symbol_scores(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        snapshot.five_exchange_scanner.symbol_scores,
    )
    .await
}

async fn scanner_recommendations(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        snapshot.five_exchange_scanner.recommendations,
    )
    .await
}

async fn hedge_policy_status(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let snapshot = state.snapshot().await;
    guarded(
        &state,
        &headers,
        serde_json::json!({
            "enabled": snapshot.hedge_policy.enabled,
            "recommendation_only": snapshot.hedge_policy.recommendation_only,
        }),
    )
    .await
}

async fn hedge_policy_inventory_risk(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.hedge_policy.inventory_risk).await
}

async fn hedge_policy_recommendations(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.hedge_policy.recommendations).await
}

async fn hedge_policy_venue_capabilities(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.hedge_policy.venue_capabilities).await
}

async fn hedge_policy_market_regime(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let snapshot = state.snapshot().await;
    guarded(&state, &headers, snapshot.hedge_policy.market_regime).await
}

async fn control_symbols(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let model = control.read_model().await;
    guarded(&state, &headers, model.symbols).await
}

async fn control_symbol(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    guarded(&state, &headers, control.symbol(&symbol).await).await
}

async fn control_symbol_inventory(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let symbol = normalize_symbol(&symbol);
    let model = control.read_model().await;
    guarded(
        &state,
        &headers,
        model
            .symbols
            .into_iter()
            .find(|item| item.internal_symbol == symbol)
            .map(|_| model.dust_positions),
    )
    .await
}

async fn control_symbol_orders(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let symbol = normalize_symbol(&symbol);
    guarded(
        &state,
        &headers,
        serde_json::json!({
            "symbol": symbol,
            "active_strategy_orders": [],
            "live_submission_blocked": true
        }),
    )
    .await
}

async fn control_symbol_commands(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let symbol = normalize_symbol(&symbol);
    let model = control.read_model().await;
    guarded(
        &state,
        &headers,
        model
            .commands
            .into_iter()
            .filter(|command| command.symbol == symbol)
            .collect::<Vec<_>>(),
    )
    .await
}

async fn control_symbol_liquidation(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let symbol = normalize_symbol(&symbol);
    let model = control.read_model().await;
    guarded(
        &state,
        &headers,
        serde_json::json!({
            "plans": model.liquidation_plans.into_iter().filter(|plan| plan.symbol == symbol).collect::<Vec<_>>(),
            "passive_sessions": model.passive_sessions.into_iter().filter(|session| session.symbol == symbol).collect::<Vec<_>>(),
            "dust": model.dust_positions.into_iter().filter(|dust| dust.symbol == symbol).collect::<Vec<_>>()
        }),
    )
    .await
}

async fn control_symbol_runtime_snapshot(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    guarded(
        &state,
        &headers,
        control.latest_runtime_snapshot(&symbol).await,
    )
    .await
}

async fn control_symbol_readiness(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let snapshot = control.latest_runtime_snapshot(&symbol).await;
    guarded(
        &state,
        &headers,
        snapshot.map(|snapshot| {
            serde_json::json!({
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_age_ms": snapshot.age_ms(),
                "effective_tradability": snapshot.effective_tradability,
                "warnings": snapshot.warnings,
                "critical_errors": snapshot.critical_errors,
            })
        }),
    )
    .await
}

async fn control_symbol_inventory_ownership(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let snapshot = control.latest_runtime_snapshot(&symbol).await;
    guarded(
        &state,
        &headers,
        snapshot.map(|snapshot| snapshot.inventory_ownership),
    )
    .await
}

async fn control_symbol_direction_readiness(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let snapshot = control.latest_runtime_snapshot(&symbol).await;
    guarded(
        &state,
        &headers,
        snapshot.map(|snapshot| snapshot.direction_readiness),
    )
    .await
}

async fn control_symbol_liquidation_preview(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let snapshot = control.latest_runtime_snapshot(&symbol).await;
    guarded(
        &state,
        &headers,
        snapshot.map(|snapshot| snapshot.liquidation_preview),
    )
    .await
}

async fn control_symbol_data_health(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let snapshot = control.latest_runtime_snapshot(&symbol).await;
    guarded(
        &state,
        &headers,
        snapshot.map(|snapshot| {
            serde_json::json!({
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_age_ms": snapshot.age_ms(),
                "components": snapshot.component_statuses,
                "data_sources": snapshot.data_sources,
                "warnings": snapshot.warnings,
                "critical_errors": snapshot.critical_errors,
            })
        }),
    )
    .await
}

async fn control_command(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(command_id): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    guarded(&state, &headers, control.command(&command_id).await).await
}

async fn control_audit(State(state): State<MonitoringState>, headers: HeaderMap) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let model = control.read_model().await;
    guarded(&state, &headers, model.audit_events).await
}

async fn control_runtime_publisher_status(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let Some(publisher) = state.runtime_publisher() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    guarded(&state, &headers, publisher.health().await).await
}

async fn control_runtime_publisher_exchanges(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let Some(publisher) = state.runtime_publisher() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let health = publisher.health().await;
    guarded(&state, &headers, health.per_exchange_health).await
}

async fn control_runtime_publisher_components(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let model = control.read_model().await;
    let components = model.runtime_snapshots.last().map(|snapshot| {
        serde_json::json!({
            "snapshot_id": snapshot.snapshot_id,
            "source_metadata": snapshot.source_metadata,
            "component_statuses": snapshot.component_statuses,
        })
    });
    guarded(&state, &headers, components).await
}

async fn control_runtime_publisher_errors(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
) -> Response {
    let Some(publisher) = state.runtime_publisher() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let health = publisher.health().await;
    guarded(
        &state,
        &headers,
        serde_json::json!({
            "last_critical_error": health.last_critical_error,
            "snapshot_persist_failures": health.snapshot_persist_failures,
            "exchange_errors": health.per_exchange_health.values().filter_map(|item| {
                item.last_error.as_ref().map(|error| serde_json::json!({
                    "exchange": item.exchange,
                    "error": error,
                    "backoff_until": item.backoff_until,
                }))
            }).collect::<Vec<_>>()
        }),
    )
    .await
}

async fn control_symbol_snapshots(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let snapshots = if let Some(publisher) = state.runtime_publisher() {
        if let Some(store) = &publisher.snapshot_store {
            store
                .list_snapshots(&symbol, 100)
                .await
                .unwrap_or_else(|_| Vec::new())
        } else {
            control.runtime_snapshots_for_symbol(&symbol, 100).await
        }
    } else {
        control.runtime_snapshots_for_symbol(&symbol, 100).await
    };
    guarded(&state, &headers, snapshots).await
}

async fn control_snapshot_by_id(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(snapshot_id): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let snapshot = if let Some(publisher) = state.runtime_publisher() {
        if let Some(store) = &publisher.snapshot_store {
            match store.get_snapshot(&snapshot_id).await.ok().flatten() {
                Some(snapshot) => Some(snapshot),
                None => control.runtime_snapshot_by_id(&snapshot_id).await,
            }
        } else {
            control.runtime_snapshot_by_id(&snapshot_id).await
        }
    } else {
        control.runtime_snapshot_by_id(&snapshot_id).await
    };
    guarded(&state, &headers, snapshot).await
}

async fn control_snapshot_replay(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(snapshot_id): Path<String>,
) -> Response {
    let Some(publisher) = state.runtime_publisher() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Some(store) = &publisher.snapshot_store else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let replay = SpotControlSnapshotReplay::new(store.clone());
    let report = replay.replay(&snapshot_id).await.ok().flatten();
    guarded(&state, &headers, report).await
}

async fn control_symbol_open_order_ownership(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let snapshot = control.latest_runtime_snapshot(&symbol).await;
    guarded(
        &state,
        &headers,
        snapshot.map(|snapshot| snapshot.open_orders),
    )
    .await
}

async fn control_symbol_fill_ownership(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let snapshot = control.latest_runtime_snapshot(&symbol).await;
    guarded(
        &state,
        &headers,
        snapshot.map(|snapshot| snapshot.fill_ownership),
    )
    .await
}

async fn control_symbol_consistency(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
) -> Response {
    let Some(control) = state.control() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let snapshot = control.latest_runtime_snapshot(&symbol).await;
    guarded(
        &state,
        &headers,
        snapshot.map(|snapshot| snapshot.consistency_report),
    )
    .await
}

async fn control_enable_symbol(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Json(body): Json<EnableSymbolRequest>,
) -> Response {
    let Some(control) = write_control(&state, &headers) else {
        return write_control_rejection(&state, &headers);
    };
    let Some(idempotency_key) = idempotency_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    record_snapshot_for_intent(
        &state,
        &body.symbol,
        &body.selected_exchanges,
        &body.allowed_directions,
        body.mode,
    )
    .await;
    Json(control.enable(body, idempotency_key).await).into_response()
}

async fn control_pause_symbol(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
    Json(body): Json<VersionedSymbolRequest>,
) -> Response {
    let Some(control) = write_control(&state, &headers) else {
        return write_control_rejection(&state, &headers);
    };
    let Some(idempotency_key) = idempotency_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    record_snapshot_for_existing_symbol(&state, &symbol).await;
    Json(control.pause(symbol, body, idempotency_key).await).into_response()
}

async fn control_resume_symbol(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
    Json(body): Json<VersionedSymbolRequest>,
) -> Response {
    let Some(control) = write_control(&state, &headers) else {
        return write_control_rejection(&state, &headers);
    };
    let Some(idempotency_key) = idempotency_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    record_snapshot_for_existing_symbol(&state, &symbol).await;
    Json(control.resume(symbol, body, idempotency_key).await).into_response()
}

async fn control_disable_symbol(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
    Json(mut body): Json<DisableSymbolRequest>,
) -> Response {
    let Some(control) = write_control(&state, &headers) else {
        return write_control_rejection(&state, &headers);
    };
    let Some(idempotency_key) = idempotency_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    body.symbol = symbol;
    record_snapshot_for_intent(
        &state,
        &body.symbol,
        &body.selected_exchanges,
        &[],
        EnableMode::LiveDryRun,
    )
    .await;
    Json(control.disable(body, idempotency_key).await).into_response()
}

async fn control_cancel_orders(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
    Json(body): Json<VersionedSymbolRequest>,
) -> Response {
    let Some(control) = write_control(&state, &headers) else {
        return write_control_rejection(&state, &headers);
    };
    let Some(idempotency_key) = idempotency_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    record_snapshot_for_existing_symbol(&state, &symbol).await;
    Json(control.cancel_orders(symbol, body, idempotency_key).await).into_response()
}

async fn control_confirm_liquidation(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
    Json(body): Json<VersionedSymbolRequest>,
) -> Response {
    let Some(control) = write_control(&state, &headers) else {
        return write_control_rejection(&state, &headers);
    };
    let Some(idempotency_key) = idempotency_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    record_snapshot_for_existing_symbol(&state, &symbol).await;
    Json(
        control
            .manual_control_command(
                symbol,
                body,
                idempotency_key,
                "real liquidation submission remains blocked",
                CommandStatus::ManualInterventionRequired,
                serde_json::json!({
                    "would_submit_order": false,
                    "live_submission_blocked": true
                }),
            )
            .await,
    )
    .into_response()
}

async fn control_stop_liquidation(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
    Json(body): Json<VersionedSymbolRequest>,
) -> Response {
    let Some(control) = write_control(&state, &headers) else {
        return write_control_rejection(&state, &headers);
    };
    let Some(idempotency_key) = idempotency_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    record_snapshot_for_existing_symbol(&state, &symbol).await;
    Json(
        control
            .manual_control_command(
                symbol,
                body,
                idempotency_key,
                "operator stopped liquidation workflow",
                CommandStatus::Completed,
                serde_json::json!({"stopped": true}),
            )
            .await,
    )
    .into_response()
}

async fn control_mark_dust_unmanaged(
    State(state): State<MonitoringState>,
    headers: HeaderMap,
    Path(symbol): Path<String>,
    Json(body): Json<MarkDustUnmanagedRequest>,
) -> Response {
    let Some(control) = write_control(&state, &headers) else {
        return write_control_rejection(&state, &headers);
    };
    let Some(idempotency_key) = idempotency_key(&headers) else {
        return StatusCode::BAD_REQUEST.into_response();
    };
    record_snapshot_for_existing_symbol(&state, &symbol).await;
    Json(
        control
            .mark_dust_unmanaged(symbol, body, idempotency_key)
            .await,
    )
    .into_response()
}

async fn guarded<T: Serialize>(state: &MonitoringState, headers: &HeaderMap, value: T) -> Response {
    match authorize(headers, state.config()) {
        AuthDecision::Allowed => Json(value).into_response(),
        AuthDecision::Unauthorized => StatusCode::UNAUTHORIZED.into_response(),
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

fn write_control<'a>(
    state: &'a MonitoringState,
    headers: &HeaderMap,
) -> Option<&'a crate::control::spot_control::SpotControlService> {
    if !state.config().require_token {
        return None;
    }
    if authorize(headers, state.config()) != AuthDecision::Allowed {
        return None;
    }
    let control = state.control()?;
    if control.config().require_write_auth && !state.config().require_token {
        return None;
    }
    Some(control)
}

fn write_control_rejection(state: &MonitoringState, headers: &HeaderMap) -> Response {
    if state.control().is_none() || !state.config().require_token {
        return StatusCode::NOT_FOUND.into_response();
    }
    match authorize(headers, state.config()) {
        AuthDecision::Allowed => StatusCode::FORBIDDEN.into_response(),
        AuthDecision::Unauthorized => StatusCode::UNAUTHORIZED.into_response(),
    }
}

fn idempotency_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("Idempotency-Key")
        .or_else(|| headers.get("idempotency-key"))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

async fn record_snapshot_for_existing_symbol(state: &MonitoringState, symbol: &str) {
    let Some(control) = state.control() else {
        return;
    };
    let Some(managed) = control.symbol(symbol).await else {
        return;
    };
    record_snapshot_for_intent(
        state,
        &managed.internal_symbol,
        &managed.selected_exchanges,
        &managed.allowed_directions,
        managed.enable_mode,
    )
    .await;
}

async fn record_snapshot_for_intent(
    state: &MonitoringState,
    symbol: &str,
    selected_exchanges: &[String],
    directions: &[EnabledDirection],
    mode: EnableMode,
) {
    let Some(control) = state.control() else {
        return;
    };
    if let Some(publisher) = state.runtime_publisher() {
        let request = RuntimeSnapshotBuildRequest {
            symbol: symbol.to_string(),
            selected_exchanges: selected_exchanges.to_vec(),
            directions: directions.to_vec(),
            mode,
        };
        if publisher
            .latest_acceptable_snapshot(control, symbol)
            .await
            .is_none()
        {
            let _ = publisher.request_refresh_and_wait(control, request).await;
        }
        return;
    }
    let model = state.snapshot().await;
    let builder = SpotControlSnapshotBuilder::from_dashboard_model(
        control.config().runtime_snapshot.clone(),
        &model,
    );
    let snapshot = builder.build(symbol, selected_exchanges, directions, mode);
    control.record_runtime_snapshot(snapshot).await;
}

#[derive(Debug, Clone, Deserialize)]
struct KillSwitchTriggerRequest {
    reason: String,
    #[serde(default)]
    triggered_by: Option<String>,
}
