use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::Utc;
use rustcta_supervisor::LifecycleCommandRecord;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    ControlApiConfigSummary, ControlApiState, CreateStrategyRequest, CredentialStatusResponse,
    StrategyProcessView, StrategySnapshotEnvelope, StrategySnapshotSource, WorkspaceSummary,
    CONTROL_API_SCHEMA_VERSION,
};

#[derive(Debug, Clone, Copy, Default, Deserialize)]
pub struct LedgerReplayQuery {
    pub from_sequence: Option<u64>,
}

pub async fn health() -> Json<serde_json::Value> {
    Json(json!({
        "status": "ok",
        "component": "rustcta-control-api",
        "generated_at": Utc::now(),
    }))
}

pub async fn workspace(State(state): State<ControlApiState>) -> Json<WorkspaceSummary> {
    let snapshot = state.snapshot().await;
    Json(workspace_summary_from_snapshot(snapshot))
}

pub async fn status(State(state): State<ControlApiState>) -> Json<WorkspaceSummary> {
    let snapshot = state.snapshot().await;
    Json(workspace_summary_from_snapshot(snapshot))
}

pub async fn config_summary(State(state): State<ControlApiState>) -> Json<ControlApiConfigSummary> {
    let snapshot = state.snapshot().await;
    Json(ControlApiConfigSummary {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        generated_at: snapshot.generated_at,
        agent_count: snapshot.agents.len(),
        process_count: snapshot.strategies.len(),
        strategy_count: snapshot.strategies.len(),
        gateway_configured: snapshot.gateway.is_some(),
        risk_configured: snapshot.risk.status != crate::RiskStatus::Unknown
            || snapshot.risk.open_risk_event_count > 0
            || !snapshot.risk.events.is_empty(),
        fee_configured: snapshot.fees.total_fee_usdt != 0.0
            || snapshot.fees.realized_rebate_usdt != 0.0
            || !snapshot.fees.venues.is_empty(),
        logs_configured: !snapshot.logs.events.is_empty(),
        credentials_status_only: true,
    })
}

fn workspace_summary_from_snapshot(snapshot: crate::ControlApiStateSnapshot) -> WorkspaceSummary {
    WorkspaceSummary {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        generated_at: snapshot.generated_at,
        agent_count: snapshot.agents.len(),
        process_count: snapshot.strategies.len(),
        strategy_count: snapshot.strategies.len(),
        gateway: snapshot.gateway,
        risk_status: snapshot.risk.status,
        active_risk_event_count: snapshot.risk.open_risk_event_count,
        log_event_count: snapshot.logs.events.len(),
    }
}

pub async fn agents(State(state): State<ControlApiState>) -> Json<Vec<crate::AgentSummary>> {
    Json(state.snapshot().await.agents)
}

pub async fn agent_detail(
    State(state): State<ControlApiState>,
    Path(id): Path<String>,
) -> Result<Json<crate::AgentSummary>, StatusCode> {
    state
        .snapshot()
        .await
        .agents
        .into_iter()
        .find(|agent| agent.agent_id == id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn strategies(
    State(state): State<ControlApiState>,
) -> Json<Vec<crate::StrategyProcessView>> {
    Json(
        state
            .snapshot()
            .await
            .strategies
            .into_iter()
            .map(Into::into)
            .collect(),
    )
}

pub async fn create_strategy(
    State(state): State<ControlApiState>,
    Json(request): Json<CreateStrategyRequest>,
) -> Result<Json<StrategyProcessView>, StatusCode> {
    let (process, spec) = request
        .into_process_and_spec(Utc::now())
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let process = state
        .create_strategy_with_spec(process, spec)
        .await
        .map_err(|_| StatusCode::CONFLICT)?;
    Ok(Json(process.into()))
}

pub async fn strategy_detail(
    State(state): State<ControlApiState>,
    Path(id): Path<String>,
) -> Result<Json<crate::StrategyProcessView>, StatusCode> {
    state
        .snapshot()
        .await
        .strategies
        .into_iter()
        .find(|strategy| strategy.strategy_id == id)
        .map(Into::into)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn strategy_snapshot(
    State(state): State<ControlApiState>,
    Path(id): Path<String>,
) -> Result<Json<StrategySnapshotEnvelope>, StatusCode> {
    let snapshot = state.snapshot().await;
    if let Some(strategy_snapshot) = snapshot
        .strategy_snapshots
        .iter()
        .find(|snapshot| snapshot.strategy_id == id)
        .cloned()
    {
        return Ok(Json(strategy_snapshot));
    }

    let strategy = snapshot
        .strategies
        .into_iter()
        .find(|strategy| strategy.strategy_id == id)
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(strategy_snapshot_from_process(strategy)))
}

pub async fn strategy_snapshots(
    State(state): State<ControlApiState>,
) -> Json<Vec<StrategySnapshotEnvelope>> {
    let snapshot = state.snapshot().await;
    let mut snapshots = snapshot.strategy_snapshots;
    for strategy in snapshot.strategies {
        if snapshots
            .iter()
            .any(|snapshot| snapshot.strategy_id == strategy.strategy_id)
        {
            continue;
        }
        snapshots.push(strategy_snapshot_from_process(strategy));
    }
    Json(snapshots)
}

pub async fn strategy_command(
    State(state): State<ControlApiState>,
    Path(id): Path<String>,
    Json(mut command): Json<LifecycleCommandRecord>,
) -> Result<Json<Value>, StatusCode> {
    if command.strategy_id != id {
        return Err(StatusCode::BAD_REQUEST);
    }
    if command.validate().is_err() {
        return Err(StatusCode::BAD_REQUEST);
    }
    command.schema_version = CONTROL_API_SCHEMA_VERSION;
    let (_, applied_to_runtime) = state
        .apply_strategy_command(command.clone())
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    state
        .record_command(command.clone())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(command_accepted_payload(&command, applied_to_runtime)))
}

pub async fn command(
    State(state): State<ControlApiState>,
    Json(mut command): Json<LifecycleCommandRecord>,
) -> Result<Json<Value>, StatusCode> {
    if command.validate().is_err() {
        return Err(StatusCode::BAD_REQUEST);
    }
    command.schema_version = CONTROL_API_SCHEMA_VERSION;
    state
        .record_command(command.clone())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(command_accepted_payload(&command, false)))
}

pub async fn processes(
    State(state): State<ControlApiState>,
) -> Json<Vec<crate::StrategyProcessView>> {
    Json(
        state
            .snapshot()
            .await
            .strategies
            .into_iter()
            .map(Into::into)
            .collect(),
    )
}

pub async fn process_detail(
    State(state): State<ControlApiState>,
    Path(id): Path<String>,
) -> Result<Json<crate::StrategyProcessView>, StatusCode> {
    state
        .snapshot()
        .await
        .strategies
        .into_iter()
        .find(|process| process.strategy_id == id)
        .map(Into::into)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn gateway_status(State(state): State<ControlApiState>) -> Json<serde_json::Value> {
    let snapshot = state.snapshot().await;
    Json(json!({
        "schema_version": CONTROL_API_SCHEMA_VERSION,
        "gateway": snapshot.gateway,
    }))
}

pub async fn credentials_status() -> Json<CredentialStatusResponse> {
    Json(CredentialStatusResponse {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        slots: Vec::new(),
    })
}

pub async fn risk(State(state): State<ControlApiState>) -> Json<crate::RiskSummaryView> {
    Json(state.snapshot().await.risk)
}

pub async fn risk_events(State(state): State<ControlApiState>) -> Json<Vec<crate::RiskEventView>> {
    Json(state.snapshot().await.risk.events)
}

pub async fn fees(State(state): State<ControlApiState>) -> Json<crate::FeeSummaryView> {
    Json(state.snapshot().await.fees)
}

pub async fn logs(State(state): State<ControlApiState>) -> Json<crate::LogStreamView> {
    Json(state.snapshot().await.logs)
}

pub async fn inventory(State(state): State<ControlApiState>) -> Json<crate::JsonRowsView> {
    Json(state.snapshot().await.inventory)
}

pub async fn books(State(state): State<ControlApiState>) -> Json<crate::JsonRowsView> {
    Json(state.snapshot().await.books)
}

pub async fn exchanges(State(state): State<ControlApiState>) -> Json<crate::JsonRowsView> {
    Json(state.snapshot().await.exchanges)
}

pub async fn recent_trades(State(state): State<ControlApiState>) -> Json<crate::JsonRowsView> {
    Json(state.snapshot().await.recent_trades)
}

pub async fn recent_opportunities(
    State(state): State<ControlApiState>,
) -> Json<crate::JsonRowsView> {
    Json(state.snapshot().await.recent_opportunities)
}

pub async fn opportunities(State(state): State<ControlApiState>) -> Json<crate::OpportunitiesView> {
    Json(state.snapshot().await.opportunities)
}

pub async fn symbols(State(state): State<ControlApiState>) -> Json<crate::SymbolsView> {
    Json(state.snapshot().await.symbols)
}

pub async fn strategy_logs(
    State(state): State<ControlApiState>,
) -> Json<crate::StrategyLogTailView> {
    Json(state.strategy_log_tail().await)
}

pub async fn process_logs(
    State(state): State<ControlApiState>,
    Path(id): Path<String>,
) -> Result<Json<crate::StrategyLogTailView>, StatusCode> {
    state
        .process_log_tail(&id)
        .await
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn events(
    State(state): State<ControlApiState>,
    Query(query): Query<LedgerReplayQuery>,
) -> Result<Json<Value>, StatusCode> {
    let ledger_events = state
        .ledger_events(query.from_sequence)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({
        "schema_version": CONTROL_API_SCHEMA_VERSION,
        "from_sequence": query.from_sequence,
        "commands": state.commands().await,
        "ledger_events": ledger_events,
    })))
}

pub async fn command_detail(
    State(state): State<ControlApiState>,
    Path(id): Path<String>,
) -> Result<Json<LifecycleCommandRecord>, StatusCode> {
    state
        .commands()
        .await
        .into_iter()
        .find(|command| command.command_id == id)
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn audit(
    State(state): State<ControlApiState>,
    Query(query): Query<LedgerReplayQuery>,
) -> Result<Json<Value>, StatusCode> {
    let events = state
        .audit_events(query.from_sequence)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(json!({
        "schema_version": CONTROL_API_SCHEMA_VERSION,
        "from_sequence": query.from_sequence,
        "events": events,
    })))
}

fn command_accepted_payload(command: &LifecycleCommandRecord, applied_to_runtime: bool) -> Value {
    json!({
        "accepted": true,
        "command_id": command.command_id,
        "strategy_id": command.strategy_id,
        "would_submit_order": false,
        "applied_to_runtime": applied_to_runtime,
    })
}

fn strategy_snapshot_from_process(
    strategy: rustcta_supervisor::StrategyProcess,
) -> StrategySnapshotEnvelope {
    StrategySnapshotEnvelope {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        strategy_id: strategy.strategy_id,
        strategy_kind: strategy.strategy_kind,
        run_id: Some(strategy.run_id),
        status: Some(strategy.status),
        generated_at: Utc::now(),
        source: StrategySnapshotSource::Supervisor,
        detail: json!({
            "config_path": strategy.config_path,
            "process_id": strategy.process_id,
            "started_at": strategy.started_at,
            "last_heartbeat_at": strategy.last_heartbeat_at,
            "last_snapshot_at": strategy.last_snapshot_at,
            "restart_count": strategy.restart_count,
            "last_exit_code": strategy.last_exit_code,
            "last_error": strategy.last_error,
            "log_configured": strategy.log_path.is_some(),
        }),
    }
}
