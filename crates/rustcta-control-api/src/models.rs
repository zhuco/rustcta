use chrono::{DateTime, Utc};
use rustcta_supervisor::{
    ProcessStatus, StrategyProcess, StrategyProcessSpec, SUPERVISOR_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};

pub const CONTROL_API_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlApiStateSnapshot {
    pub schema_version: u16,
    pub generated_at: DateTime<Utc>,
    pub strategies: Vec<StrategyProcess>,
    pub gateway: Option<GatewayStatusView>,
    pub agents: Vec<AgentSummary>,
    #[serde(default)]
    pub risk: RiskSummaryView,
    #[serde(default)]
    pub fees: FeeSummaryView,
    #[serde(default)]
    pub logs: LogStreamView,
    #[serde(default)]
    pub inventory: JsonRowsView,
    #[serde(default)]
    pub books: JsonRowsView,
    #[serde(default)]
    pub exchanges: JsonRowsView,
    #[serde(default)]
    pub recent_trades: JsonRowsView,
    #[serde(default)]
    pub recent_opportunities: JsonRowsView,
    #[serde(default)]
    pub opportunities: OpportunitiesView,
    #[serde(default)]
    pub symbols: SymbolsView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSummary {
    pub agent_id: String,
    pub tenant_id: String,
    pub status: AgentConnectionStatus,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentConnectionStatus {
    Connected,
    Degraded,
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayStatusView {
    pub schema_version: u16,
    pub status: GatewayConnectionStatus,
    pub agent_id: Option<String>,
    pub endpoint: Option<String>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub connected_exchanges: Vec<ExchangeConnectionView>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GatewayConnectionStatus {
    Unknown,
    Online,
    Degraded,
    Offline,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeConnectionView {
    pub exchange_id: String,
    pub market: Option<String>,
    pub status: GatewayConnectionStatus,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceSummary {
    pub schema_version: u16,
    pub generated_at: DateTime<Utc>,
    pub agent_count: usize,
    pub process_count: usize,
    pub strategy_count: usize,
    pub gateway: Option<GatewayStatusView>,
    pub risk_status: RiskStatus,
    pub active_risk_event_count: usize,
    pub log_event_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlApiConfigSummary {
    pub schema_version: u16,
    pub generated_at: DateTime<Utc>,
    pub agent_count: usize,
    pub process_count: usize,
    pub strategy_count: usize,
    pub gateway_configured: bool,
    pub risk_configured: bool,
    pub fee_configured: bool,
    pub logs_configured: bool,
    pub credentials_status_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyProcessView {
    pub schema_version: u16,
    pub strategy_id: String,
    pub strategy_kind: String,
    pub run_id: String,
    pub tenant_id: String,
    pub config_path: String,
    pub status: ProcessStatus,
    pub process_id: Option<u32>,
    pub started_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub last_snapshot_at: Option<DateTime<Utc>>,
    pub restart_count: u32,
    pub last_exit_code: Option<i32>,
    pub last_error: Option<String>,
    pub log_configured: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateStrategyRequest {
    pub strategy_id: String,
    pub strategy_kind: String,
    #[serde(default)]
    pub run_id: Option<String>,
    #[serde(default)]
    pub tenant_id: Option<String>,
    #[serde(default)]
    pub config_path: String,
    #[serde(default)]
    pub log_path: Option<String>,
    #[serde(default)]
    pub command: Option<String>,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub working_dir: Option<String>,
}

impl CreateStrategyRequest {
    pub fn into_process(self, now: DateTime<Utc>) -> Result<StrategyProcess, &'static str> {
        let strategy_id = self.strategy_id.trim();
        let strategy_kind = self.strategy_kind.trim();
        if strategy_id.is_empty() {
            return Err("strategy_id");
        }
        if strategy_kind.is_empty() {
            return Err("strategy_kind");
        }

        let mut process = StrategyProcess::new(
            strategy_id,
            strategy_kind,
            self.run_id
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("manual-{}", now.timestamp_millis())),
            self.tenant_id
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "local".to_string()),
            self.config_path.trim().to_string(),
        );
        process.schema_version = SUPERVISOR_SCHEMA_VERSION;
        process.status = ProcessStatus::Stopped;
        process.log_path = self.log_path.filter(|value| !value.trim().is_empty());
        Ok(process)
    }

    pub fn into_process_and_spec(
        self,
        now: DateTime<Utc>,
    ) -> Result<(StrategyProcess, StrategyProcessSpec), &'static str> {
        let process = self.clone().into_process(now)?;
        let mut spec = StrategyProcessSpec::new(
            process.strategy_id.clone(),
            process.strategy_kind.clone(),
            process.run_id.clone(),
            process.tenant_id.clone(),
            process.config_path.clone(),
            self.command
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "cargo".to_string()),
        )
        .with_args(default_strategy_args(
            &process.strategy_kind,
            &process.config_path,
            self.args,
        ));
        if let Some(working_dir) = self.working_dir.filter(|value| !value.trim().is_empty()) {
            spec = spec.with_working_dir(working_dir);
        }
        if let Some(log_path) = process.log_path.clone() {
            spec = spec.with_log_path(log_path);
        }
        Ok((process, spec))
    }
}

fn default_strategy_args(
    strategy_kind: &str,
    config_path: &str,
    explicit_args: Vec<String>,
) -> Vec<String> {
    if !explicit_args.is_empty() {
        return explicit_args;
    }
    match strategy_kind {
        "cross_exchange_arbitrage" => vec![
            "run".to_string(),
            "--bin".to_string(),
            "cross_arb_live".to_string(),
            "--".to_string(),
            "--config".to_string(),
            config_path.to_string(),
            "--run".to_string(),
        ],
        "spot_spot_arbitrage" | "spot_spot_taker_arbitrage" => vec![
            "run".to_string(),
            "--bin".to_string(),
            "rustcta".to_string(),
            "--".to_string(),
            "--strategy".to_string(),
            "spot_spot_taker_arbitrage".to_string(),
            "--config".to_string(),
            config_path.to_string(),
        ],
        value => vec![
            "run".to_string(),
            "--bin".to_string(),
            "rustcta".to_string(),
            "--".to_string(),
            "--strategy".to_string(),
            value.to_string(),
            "--config".to_string(),
            config_path.to_string(),
        ],
    }
}

impl From<StrategyProcess> for StrategyProcessView {
    fn from(process: StrategyProcess) -> Self {
        Self {
            schema_version: process.schema_version,
            strategy_id: process.strategy_id,
            strategy_kind: process.strategy_kind,
            run_id: process.run_id,
            tenant_id: process.tenant_id,
            config_path: process.config_path,
            status: process.status,
            process_id: process.process_id,
            started_at: process.started_at,
            last_heartbeat_at: process.last_heartbeat_at,
            last_snapshot_at: process.last_snapshot_at,
            restart_count: process.restart_count,
            last_exit_code: process.last_exit_code,
            last_error: process.last_error,
            log_configured: process.log_path.is_some(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialStatusResponse {
    pub schema_version: u16,
    pub slots: Vec<CredentialSlotStatus>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialSlotStatus {
    pub slot_id: String,
    pub exchange_id: String,
    pub tenant_id: Option<String>,
    pub configured: bool,
    pub last_verified_at: Option<DateTime<Utc>>,
    pub health: CredentialHealth,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CredentialHealth {
    Unknown,
    Missing,
    Healthy,
    Degraded,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskSummaryView {
    pub schema_version: u16,
    pub status: RiskStatus,
    pub kill_switch_active: bool,
    pub live_orders_enabled: bool,
    pub open_risk_event_count: usize,
    pub last_event_at: Option<DateTime<Utc>>,
    pub checks: Vec<RiskCheckView>,
    pub events: Vec<RiskEventView>,
}

impl Default for RiskSummaryView {
    fn default() -> Self {
        Self {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            status: RiskStatus::Unknown,
            kill_switch_active: false,
            live_orders_enabled: false,
            open_risk_event_count: 0,
            last_event_at: None,
            checks: Vec::new(),
            events: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskStatus {
    Unknown,
    Ready,
    Warning,
    Blocked,
    Disabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskCheckView {
    pub check_id: String,
    pub status: RiskStatus,
    pub message: Option<String>,
    pub updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskEventView {
    pub event_id: String,
    pub severity: RiskSeverity,
    pub scope: String,
    pub message: String,
    pub occurred_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeSummaryView {
    pub schema_version: u16,
    pub generated_at: Option<DateTime<Utc>>,
    pub total_fee_usdt: f64,
    pub realized_rebate_usdt: f64,
    pub venues: Vec<FeeVenueSummary>,
}

impl Default for FeeSummaryView {
    fn default() -> Self {
        Self {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            generated_at: None,
            total_fee_usdt: 0.0,
            realized_rebate_usdt: 0.0,
            venues: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeVenueSummary {
    pub exchange_id: String,
    pub market: Option<String>,
    pub maker_fee_rate: Option<f64>,
    pub taker_fee_rate: Option<f64>,
    pub fee_paid_usdt: f64,
    pub rebate_usdt: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogStreamView {
    pub schema_version: u16,
    pub events: Vec<LogEventView>,
}

impl Default for LogStreamView {
    fn default() -> Self {
        Self {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            events: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEventView {
    pub log_id: String,
    pub level: LogLevel,
    pub target: Option<String>,
    pub message: String,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRowsView {
    pub schema_version: u16,
    pub rows: Vec<serde_json::Value>,
}

impl Default for JsonRowsView {
    fn default() -> Self {
        Self {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            rows: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpportunitiesView {
    pub schema_version: u16,
    pub recent: Vec<serde_json::Value>,
    pub arbitrage: Vec<serde_json::Value>,
    pub statistics: serde_json::Value,
}

impl Default for OpportunitiesView {
    fn default() -> Self {
        Self {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            recent: Vec::new(),
            arbitrage: Vec::new(),
            statistics: serde_json::Value::Object(serde_json::Map::new()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolsView {
    pub schema_version: u16,
    pub symbol_rules: serde_json::Value,
    pub spot_control: serde_json::Value,
    pub scanner: SymbolScannerView,
}

impl Default for SymbolsView {
    fn default() -> Self {
        Self {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            symbol_rules: serde_json::Value::Array(Vec::new()),
            spot_control: serde_json::Value::Object(serde_json::Map::new()),
            scanner: Default::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SymbolScannerView {
    pub symbol_coverage: serde_json::Value,
    pub recommendations: serde_json::Value,
}

impl Default for SymbolScannerView {
    fn default() -> Self {
        Self {
            symbol_coverage: serde_json::Value::Array(Vec::new()),
            recommendations: serde_json::Value::Array(Vec::new()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyLogTailView {
    pub schema_version: u16,
    pub target: Option<String>,
    pub configured: bool,
    pub readable: bool,
    pub truncated: bool,
    pub max_lines: usize,
    pub max_bytes: usize,
    pub event_count: usize,
    pub read_error: Option<String>,
    pub events: Vec<LogEventView>,
}

impl Default for StrategyLogTailView {
    fn default() -> Self {
        Self {
            schema_version: CONTROL_API_SCHEMA_VERSION,
            target: None,
            configured: false,
            readable: false,
            truncated: false,
            max_lines: 0,
            max_bytes: 0,
            event_count: 0,
            read_error: None,
            events: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}
