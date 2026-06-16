use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use chrono::Utc;
use rustcta_supervisor::LifecycleCommandRecord;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{Mutex, OnceLock};

use crate::{
    read_models, ControlApiConfigSummary, ControlApiState, ControlApiStateSnapshot,
    CreateStrategyRequest, CredentialStatusResponse, LegacyDashboardReadView,
    StrategyExchangeSlotView, StrategyProcessView, StrategySnapshotEnvelope,
    StrategySnapshotSource, StrategyTemplateCatalog, StrategyTemplateFieldOptionView,
    StrategyTemplateFieldView, StrategyTemplateView, SystemResourceSnapshot, WorkspaceSummary,
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
    Json(workspace_summary_from_snapshot(
        snapshot,
        SystemResourceSnapshot::default(),
    ))
}

pub async fn status(State(state): State<ControlApiState>) -> Json<WorkspaceSummary> {
    let snapshot = state.snapshot().await;
    Json(workspace_summary_from_snapshot(
        snapshot,
        system_resource_snapshot(),
    ))
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

fn workspace_summary_from_snapshot(
    snapshot: crate::ControlApiStateSnapshot,
    system: SystemResourceSnapshot,
) -> WorkspaceSummary {
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
        system,
    }
}

#[derive(Clone, Copy)]
struct CpuTotals {
    idle: u64,
    total: u64,
}

#[derive(Clone, Copy)]
struct ProcessCpuSample {
    jiffies: u64,
}

#[derive(Default)]
struct SystemResourceTracker {
    last_cpu: Option<CpuTotals>,
    last_process_cpu: Option<ProcessCpuSample>,
}

impl SystemResourceTracker {
    fn snapshot(&mut self) -> SystemResourceSnapshot {
        let cpu = read_cpu_totals();
        let process_cpu = read_process_cpu_sample();
        let cpu_count = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1) as f64;
        let cpu_usage_pct = cpu
            .zip(self.last_cpu)
            .and_then(|(current, previous)| cpu_usage_pct(previous, current));
        let process_cpu_usage_pct = process_cpu
            .zip(self.last_process_cpu)
            .zip(cpu.zip(self.last_cpu))
            .and_then(
                |((current_process, previous_process), (current_cpu, previous_cpu))| {
                    process_cpu_usage_pct(
                        previous_process,
                        current_process,
                        previous_cpu,
                        current_cpu,
                        cpu_count,
                    )
                },
            );
        if let Some(cpu) = cpu {
            self.last_cpu = Some(cpu);
        }
        if let Some(process_cpu) = process_cpu {
            self.last_process_cpu = Some(process_cpu);
        }
        let (memory_total_bytes, memory_available_bytes) = read_system_memory();
        let memory_used_bytes = memory_total_bytes
            .zip(memory_available_bytes)
            .map(|(total, available)| total.saturating_sub(available));
        let memory_usage_pct = memory_used_bytes
            .zip(memory_total_bytes)
            .and_then(|(used, total)| (total > 0).then(|| used as f64 / total as f64 * 100.0));
        SystemResourceSnapshot {
            cpu_usage_pct,
            memory_used_bytes,
            memory_total_bytes,
            memory_usage_pct,
            process_cpu_usage_pct,
            process_memory_bytes: read_process_memory_bytes(),
        }
    }
}

fn system_resource_snapshot() -> SystemResourceSnapshot {
    static TRACKER: OnceLock<Mutex<SystemResourceTracker>> = OnceLock::new();
    TRACKER
        .get_or_init(|| Mutex::new(SystemResourceTracker::default()))
        .lock()
        .map(|mut tracker| tracker.snapshot())
        .unwrap_or_default()
}

fn read_cpu_totals() -> Option<CpuTotals> {
    let raw = std::fs::read_to_string("/proc/stat").ok()?;
    let line = raw.lines().find(|line| line.starts_with("cpu "))?;
    let values = line
        .split_whitespace()
        .skip(1)
        .filter_map(|item| item.parse::<u64>().ok())
        .collect::<Vec<_>>();
    if values.len() < 4 {
        return None;
    }
    let idle =
        values.get(3).copied().unwrap_or_default() + values.get(4).copied().unwrap_or_default();
    let total = values.iter().sum::<u64>();
    Some(CpuTotals { idle, total })
}

fn cpu_usage_pct(previous: CpuTotals, current: CpuTotals) -> Option<f64> {
    let total_delta = current.total.checked_sub(previous.total)?;
    let idle_delta = current.idle.checked_sub(previous.idle)?;
    if total_delta == 0 {
        return None;
    }
    Some((1.0 - idle_delta as f64 / total_delta as f64).clamp(0.0, 1.0) * 100.0)
}

fn read_process_cpu_sample() -> Option<ProcessCpuSample> {
    let raw = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields = raw
        .rsplit_once(") ")?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    let user_jiffies = fields.get(11)?.parse::<u64>().ok()?;
    let system_jiffies = fields.get(12)?.parse::<u64>().ok()?;
    Some(ProcessCpuSample {
        jiffies: user_jiffies.saturating_add(system_jiffies),
    })
}

fn process_cpu_usage_pct(
    previous_process: ProcessCpuSample,
    current_process: ProcessCpuSample,
    previous_cpu: CpuTotals,
    current_cpu: CpuTotals,
    cpu_count: f64,
) -> Option<f64> {
    let process_delta = current_process
        .jiffies
        .checked_sub(previous_process.jiffies)?;
    let total_delta = current_cpu.total.checked_sub(previous_cpu.total)?;
    if total_delta == 0 {
        return None;
    }
    Some((process_delta as f64 / total_delta as f64 * cpu_count * 100.0).max(0.0))
}

fn read_system_memory() -> (Option<u64>, Option<u64>) {
    let raw = match std::fs::read_to_string("/proc/meminfo") {
        Ok(raw) => raw,
        Err(_) => return (None, None),
    };
    let mut total = None;
    let mut available = None;
    for line in raw.lines() {
        if let Some(value) = line
            .strip_prefix("MemTotal:")
            .and_then(meminfo_kb_value_to_bytes)
        {
            total = Some(value);
        } else if let Some(value) = line
            .strip_prefix("MemAvailable:")
            .and_then(meminfo_kb_value_to_bytes)
        {
            available = Some(value);
        }
        if total.is_some() && available.is_some() {
            break;
        }
    }
    (total, available)
}

fn read_process_memory_bytes() -> Option<u64> {
    let raw = std::fs::read_to_string("/proc/self/status").ok()?;
    raw.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")
            .and_then(meminfo_kb_value_to_bytes)
    })
}

fn meminfo_kb_value_to_bytes(value: &str) -> Option<u64> {
    value
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
        .map(|kb| kb.saturating_mul(1024))
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

pub async fn strategy_templates() -> Json<StrategyTemplateCatalog> {
    Json(StrategyTemplateCatalog {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        templates: strategy_template_catalog(),
    })
}

fn strategy_template_catalog() -> Vec<StrategyTemplateView> {
    let common_fields = vec![
        template_field(
            "max_total_notional_usdt",
            "最大总名义本金",
            "number",
            "1000",
            Some("USDT"),
            true,
        ),
        template_field(
            "max_single_order_usdt",
            "每单金额",
            "number",
            "50",
            Some("USDT"),
            true,
        ),
        template_field(
            "max_slippage_pct",
            "最大滑点",
            "number",
            "0.08",
            Some("%"),
            true,
        ),
        template_field("cooldown_ms", "下单冷却", "number", "500", Some("ms"), true),
    ];

    vec![
        StrategyTemplateView {
            template_id: "hedged_grid".to_string(),
            strategy_kind: "hedged_grid".to_string(),
            label: "对冲网格".to_string(),
            description: "单账户多空双开网格。".to_string(),
            exchange_slots: vec![exchange_slot("execution", "交易所", "perpetual", true)],
            common_fields: Vec::new(),
            strategy_fields: vec![
                template_field("symbol", "交易对", "text", "BTC/USDT", None, true),
                template_select_field(
                    "grid_spacing_mode",
                    "类型",
                    "pct",
                    true,
                    &[("pct", "等比"), ("abs", "等差")],
                ),
                template_field("grid_spacing", "间距", "number", "0.25", Some("%/U"), true),
                template_field("grid_order_count", "挂单", "number", "8", None, true),
                template_field(
                    "order_notional_usdt",
                    "每格USDT",
                    "number",
                    "50",
                    None,
                    true,
                ),
            ],
        },
        StrategyTemplateView {
            template_id: "unified_arbitrage".to_string(),
            strategy_kind: "unified_arbitrage".to_string(),
            label: "跨所合约对冲".to_string(),
            description: "两个永续交易所之间按开仓净边际和平仓净利阈值执行双边对冲。".to_string(),
            exchange_slots: vec![
                exchange_slot("long", "做多交易所", "perpetual", true),
                exchange_slot("short", "做空交易所", "perpetual", true),
            ],
            common_fields,
            strategy_fields: vec![
                template_field(
                    "symbols",
                    "交易对列表",
                    "text",
                    "BTC/USDT,ETH/USDT,SOL/USDT",
                    None,
                    true,
                ),
                template_field(
                    "min_open_raw_spread_pct",
                    "开仓原始价差",
                    "number",
                    "0.12",
                    Some("%"),
                    true,
                ),
                template_field(
                    "min_open_net_edge_pct",
                    "开仓净边际",
                    "number",
                    "0.04",
                    Some("%"),
                    true,
                ),
                template_field(
                    "close_min_net_profit_pct",
                    "平仓净利",
                    "number",
                    "0.03",
                    Some("%"),
                    true,
                ),
                template_field(
                    "max_close_spread_pct",
                    "最大平仓价差",
                    "number",
                    "0.08",
                    Some("%"),
                    true,
                ),
            ],
        },
    ]
}

fn exchange_slot(
    slot_id: &str,
    label: &str,
    market_type: &str,
    required: bool,
) -> StrategyExchangeSlotView {
    StrategyExchangeSlotView {
        slot_id: slot_id.to_string(),
        label: label.to_string(),
        market_type: market_type.to_string(),
        required,
    }
}

fn template_field(
    field_id: &str,
    label: &str,
    input_type: &str,
    default_value: &str,
    unit: Option<&str>,
    required: bool,
) -> StrategyTemplateFieldView {
    StrategyTemplateFieldView {
        field_id: field_id.to_string(),
        label: label.to_string(),
        input_type: input_type.to_string(),
        default_value: default_value.to_string(),
        unit: unit.map(str::to_string),
        required,
        options: Vec::new(),
    }
}

fn template_select_field(
    field_id: &str,
    label: &str,
    default_value: &str,
    required: bool,
    options: &[(&str, &str)],
) -> StrategyTemplateFieldView {
    StrategyTemplateFieldView {
        field_id: field_id.to_string(),
        label: label.to_string(),
        input_type: "select".to_string(),
        default_value: default_value.to_string(),
        unit: None,
        required,
        options: options
            .iter()
            .map(|(value, label)| StrategyTemplateFieldOptionView {
                value: (*value).to_string(),
                label: (*label).to_string(),
            })
            .collect(),
    }
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

pub async fn disabled(State(state): State<ControlApiState>) -> Json<LegacyDashboardReadView> {
    legacy_spot_field(state, "disabled").await
}

pub async fn dry_run_plans(State(state): State<ControlApiState>) -> Json<LegacyDashboardReadView> {
    legacy_spot_field(state, "live_dry_run_orders").await
}

pub async fn live_preflight(State(state): State<ControlApiState>) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = runtime_control_live_preflight(&snapshot)
        .or_else(|| legacy_strategy_field(&snapshot, "spot_spot_taker_arbitrage", "live_preflight"))
        .unwrap_or_else(|| Value::Object(Default::default()));
    legacy_view_from_value(snapshot.generated_at, data)
}

pub async fn live_preflight_checks(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = runtime_control_live_preflight(&snapshot)
        .or_else(|| legacy_strategy_field(&snapshot, "spot_spot_taker_arbitrage", "live_preflight"))
        .and_then(|value| value.get("checks").cloned())
        .unwrap_or_else(|| Value::Array(Vec::new()));
    legacy_view_from_value(snapshot.generated_at, data)
}

pub async fn live_preflight_summary(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let report = runtime_control_live_preflight(&snapshot)
        .or_else(|| legacy_strategy_field(&snapshot, "spot_spot_taker_arbitrage", "live_preflight"))
        .unwrap_or_else(|| Value::Object(Default::default()));
    let summary = json!({
        "enabled": snapshot.risk.status != crate::RiskStatus::Unknown,
        "ready": report.get("ready").cloned().unwrap_or(Value::Null),
        "checks": report
            .get("checks")
            .and_then(Value::as_array)
            .map(Vec::len)
            .unwrap_or_default(),
        "blockers": report.get("blockers").cloned().unwrap_or_else(|| Value::Array(Vec::new())),
        "per_symbol_readiness": report
            .get("per_symbol_readiness")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new())),
    });
    legacy_view_from_value(snapshot.generated_at, summary)
}

fn runtime_control_live_preflight(snapshot: &ControlApiStateSnapshot) -> Option<Value> {
    snapshot.runtime_control.live_preflight.clone()
}

pub async fn order_reconciliation_status(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_spot_field(state, "order_reconciliation").await
}

pub async fn balance_reconciliation(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_spot_field(state, "balance_reconciliation").await
}

pub async fn spot_arb_dashboard(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_strategy_detail_view(state, "spot_spot_taker_arbitrage").await
}

pub async fn unified_arb_dashboard(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_strategy_detail_view(state, "unified_arbitrage").await
}

pub async fn unified_arb_instruments(State(state): State<ControlApiState>) -> Json<Value> {
    let snapshot = state.snapshot().await;
    let detail = legacy_strategy_detail(&snapshot, "unified_arbitrage");
    let instruments = detail
        .and_then(|detail| detail.get("instruments"))
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let feasibility = detail
        .and_then(|detail| detail.get("instrument_feasibility"))
        .cloned()
        .unwrap_or(Value::Null);
    let coverage_ok = instruments.as_array().is_some_and(|rows| !rows.is_empty());
    Json(read_models::secret_free_legacy_value(&json!({
        "schema_version": CONTROL_API_SCHEMA_VERSION,
        "generated_at": snapshot.generated_at,
        "instruments": instruments,
        "feasibility": feasibility,
        "coverage_ok": coverage_ok,
    })))
}

pub async fn unified_arb_market_snapshots(State(state): State<ControlApiState>) -> Json<Value> {
    let snapshot = state.snapshot().await;
    let snapshots = legacy_strategy_field(&snapshot, "unified_arbitrage", "market_snapshots")
        .or_else(|| legacy_strategy_field(&snapshot, "unified_arbitrage", "snapshots"))
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let coverage_ok = snapshots.as_array().is_some_and(|rows| !rows.is_empty());
    Json(read_models::secret_free_legacy_value(&json!({
        "schema_version": CONTROL_API_SCHEMA_VERSION,
        "generated_at": snapshot.generated_at,
        "snapshots": snapshots,
        "coverage_ok": coverage_ok,
    })))
}

pub async fn scanner_exchanges(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_scanner_field(state, "exchange_roles").await
}

pub async fn scanner_symbol_coverage(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_scanner_field(state, "symbol_coverage").await
}

pub async fn scanner_exchange_pairs(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_scanner_field(state, "exchange_pairs").await
}

pub async fn scanner_opportunities(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_scanner_field(state, "opportunities").await
}

pub async fn scanner_pair_statistics(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_scanner_field(state, "pair_statistics").await
}

pub async fn scanner_symbol_scores(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_scanner_field(state, "symbol_scores").await
}

pub async fn scanner_recommendations(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_scanner_field(state, "recommendations").await
}

pub async fn hedge_policy_status(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_cross_field(state, "hedge_policy").await
}

pub async fn hedge_policy_inventory_risk(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_hedge_policy_field(state, "inventory_risk").await
}

pub async fn hedge_policy_recommendations(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_hedge_policy_field(state, "recommendations").await
}

pub async fn hedge_policy_venue_capabilities(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_hedge_policy_field(state, "venue_capabilities").await
}

pub async fn hedge_policy_market_regime(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_hedge_policy_field(state, "market_regime").await
}

pub async fn control_symbols(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = snapshot
        .symbols
        .spot_control
        .get("symbols")
        .cloned()
        .unwrap_or_else(|| snapshot.symbols.spot_control.clone());
    legacy_view_from_value(snapshot.generated_at, data)
}

pub async fn control_symbol(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = symbol_control_entry(&snapshot.symbols.spot_control, &symbol).unwrap_or(Value::Null);
    legacy_view_from_value(snapshot.generated_at, data)
}

pub async fn control_symbol_inventory(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    legacy_symbol_array_field(state, symbol, "inventory").await
}

pub async fn control_symbol_orders(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    legacy_symbol_array_field(state, symbol, "orders").await
}

pub async fn control_symbol_commands(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    legacy_symbol_array_field(state, symbol, "commands").await
}

pub async fn control_symbol_liquidation(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    latest_symbol_snapshot_field(state, symbol, "liquidation").await
}

pub async fn control_symbol_runtime_snapshot(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    latest_symbol_snapshot(state, symbol).await
}

pub async fn control_symbol_readiness(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    latest_symbol_snapshot_field(state, symbol, "readiness").await
}

pub async fn control_symbol_inventory_ownership(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    latest_symbol_snapshot_field(state, symbol, "inventory_ownership").await
}

pub async fn control_symbol_direction_readiness(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    latest_symbol_snapshot_field(state, symbol, "direction_readiness").await
}

pub async fn control_symbol_liquidation_preview(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    latest_symbol_snapshot_field(state, symbol, "liquidation_preview").await
}

pub async fn control_symbol_data_health(
    State(state): State<ControlApiState>,
    Path(symbol): Path<String>,
) -> Json<LegacyDashboardReadView> {
    latest_symbol_snapshot_field(state, symbol, "data_health").await
}

pub async fn runtime_publisher_status(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = typed_runtime_publisher(&snapshot)
        .cloned()
        .or_else(|| {
            legacy_strategy_field(&snapshot, "spot_spot_taker_arbitrage", "runtime_publisher")
        })
        .unwrap_or(Value::Null);
    legacy_view_from_value(snapshot.generated_at, data)
}

pub async fn runtime_publisher_exchanges(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_runtime_publisher_field(state, "exchanges").await
}

pub async fn runtime_publisher_components(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_runtime_publisher_field(state, "components").await
}

pub async fn runtime_publisher_errors(
    State(state): State<ControlApiState>,
) -> Json<LegacyDashboardReadView> {
    legacy_runtime_publisher_field(state, "errors").await
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

async fn legacy_spot_field(
    state: ControlApiState,
    field: &'static str,
) -> Json<LegacyDashboardReadView> {
    legacy_strategy_field_view(state, "spot_spot_taker_arbitrage", field).await
}

async fn legacy_cross_field(
    state: ControlApiState,
    field: &'static str,
) -> Json<LegacyDashboardReadView> {
    legacy_strategy_field_view(state, "unified_arbitrage", field).await
}

async fn legacy_strategy_field_view(
    state: ControlApiState,
    strategy_kind: &'static str,
    field: &'static str,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = legacy_strategy_field(&snapshot, strategy_kind, field).unwrap_or(Value::Null);
    legacy_view_from_value(snapshot.generated_at, data)
}

async fn legacy_strategy_detail_view(
    state: ControlApiState,
    strategy_kind: &'static str,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = legacy_strategy_detail(&snapshot, strategy_kind)
        .cloned()
        .unwrap_or_else(|| Value::Object(Default::default()));
    legacy_view_from_value(snapshot.generated_at, data)
}

async fn legacy_scanner_field(
    state: ControlApiState,
    field: &'static str,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = legacy_strategy_field(&snapshot, "unified_arbitrage", "scanner")
        .and_then(|scanner| scanner.get(field).cloned())
        .or_else(|| match field {
            "symbol_coverage" => Some(snapshot.symbols.scanner.symbol_coverage.clone()),
            "recommendations" => Some(snapshot.symbols.scanner.recommendations.clone()),
            _ => None,
        })
        .unwrap_or(Value::Null);
    legacy_view_from_value(snapshot.generated_at, data)
}

async fn legacy_hedge_policy_field(
    state: ControlApiState,
    field: &'static str,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = legacy_strategy_field(&snapshot, "unified_arbitrage", "hedge_policy")
        .and_then(|policy| policy.get(field).cloned())
        .unwrap_or(Value::Null);
    legacy_view_from_value(snapshot.generated_at, data)
}

async fn legacy_runtime_publisher_field(
    state: ControlApiState,
    field: &'static str,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = typed_runtime_publisher(&snapshot)
        .cloned()
        .or_else(|| {
            legacy_strategy_field(&snapshot, "spot_spot_taker_arbitrage", "runtime_publisher")
        })
        .and_then(|runtime| runtime.get(field).cloned())
        .or_else(|| typed_runtime_snapshot_publisher_field(&snapshot, field))
        .unwrap_or(Value::Null);
    legacy_view_from_value(snapshot.generated_at, data)
}

fn typed_runtime_publisher(snapshot: &ControlApiStateSnapshot) -> Option<&Value> {
    snapshot
        .symbols
        .spot_control
        .get("runtime_publisher")
        .filter(|value| value.is_object())
}

fn typed_runtime_snapshot_publisher_field(
    snapshot: &ControlApiStateSnapshot,
    field: &str,
) -> Option<Value> {
    let runtime_snapshot = snapshot
        .symbols
        .spot_control
        .get("runtime_snapshots")
        .and_then(Value::as_array)?
        .last()?;
    match field {
        "exchanges" => runtime_snapshot.get("exchange_health").cloned(),
        "components" => runtime_snapshot.get("component_statuses").cloned(),
        "errors" => runtime_snapshot.get("critical_errors").cloned(),
        "route_health" => runtime_snapshot.get("route_health").cloned(),
        _ => None,
    }
}

async fn latest_symbol_snapshot(
    state: ControlApiState,
    symbol: String,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = latest_symbol_snapshot_value(&snapshot.symbols.spot_control, &symbol)
        .unwrap_or(Value::Null);
    legacy_view_from_value(snapshot.generated_at, data)
}

async fn latest_symbol_snapshot_field(
    state: ControlApiState,
    symbol: String,
    field: &'static str,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let data = latest_symbol_snapshot_value(&snapshot.symbols.spot_control, &symbol)
        .and_then(|snapshot| snapshot.get(field).cloned())
        .unwrap_or(Value::Null);
    legacy_view_from_value(snapshot.generated_at, data)
}

async fn legacy_symbol_array_field(
    state: ControlApiState,
    symbol: String,
    field: &'static str,
) -> Json<LegacyDashboardReadView> {
    let snapshot = state.snapshot().await;
    let normalized = normalize_symbol(&symbol);
    let data = snapshot
        .symbols
        .spot_control
        .get(field)
        .and_then(Value::as_array)
        .map(|items| {
            Value::Array(
                items
                    .iter()
                    .filter(|item| value_matches_symbol(item, &normalized))
                    .cloned()
                    .collect(),
            )
        })
        .unwrap_or_else(|| Value::Array(Vec::new()));
    legacy_view_from_value(snapshot.generated_at, data)
}

fn legacy_view_from_value(
    generated_at: chrono::DateTime<Utc>,
    data: Value,
) -> Json<LegacyDashboardReadView> {
    Json(LegacyDashboardReadView {
        schema_version: CONTROL_API_SCHEMA_VERSION,
        generated_at,
        data: read_models::secret_free_legacy_value(&data),
    })
}

fn legacy_strategy_field(
    snapshot: &ControlApiStateSnapshot,
    strategy_kind: &str,
    field: &str,
) -> Option<Value> {
    legacy_strategy_detail(snapshot, strategy_kind)?
        .get(field)
        .cloned()
}

fn legacy_strategy_detail<'a>(
    snapshot: &'a ControlApiStateSnapshot,
    strategy_kind: &str,
) -> Option<&'a Value> {
    snapshot
        .strategy_snapshots
        .iter()
        .find(|snapshot| {
            matches!(
                snapshot.source,
                StrategySnapshotSource::TypedRuntimeSnapshot
                    | StrategySnapshotSource::LegacyDashboard
            ) && snapshot.strategy_kind == strategy_kind
        })
        .map(|snapshot| &snapshot.detail)
}

fn symbol_control_entry(value: &Value, symbol: &str) -> Option<Value> {
    let normalized = normalize_symbol(symbol);
    value
        .get("symbols")
        .and_then(Value::as_array)
        .and_then(|symbols| {
            symbols
                .iter()
                .find(|item| value_matches_symbol(item, &normalized))
                .cloned()
        })
}

fn latest_symbol_snapshot_value(value: &Value, symbol: &str) -> Option<Value> {
    let normalized = normalize_symbol(symbol);
    value
        .get("runtime_snapshots")
        .and_then(Value::as_array)
        .and_then(|snapshots| {
            snapshots
                .iter()
                .rev()
                .find(|item| value_matches_symbol(item, &normalized))
                .cloned()
        })
}

fn value_matches_symbol(value: &Value, normalized: &str) -> bool {
    ["symbol", "internal_symbol", "canonical_symbol"]
        .into_iter()
        .filter_map(|field| value.get(field).and_then(Value::as_str))
        .map(normalize_symbol)
        .any(|item| item == normalized)
}

fn normalize_symbol(symbol: impl AsRef<str>) -> String {
    symbol
        .as_ref()
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .flat_map(char::to_uppercase)
        .collect()
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
            "runtime_snapshot": strategy.runtime_snapshot,
            "restart_count": strategy.restart_count,
            "last_exit_code": strategy.last_exit_code,
            "last_error": strategy.last_error,
            "log_configured": strategy.log_path.is_some(),
        }),
    }
}
