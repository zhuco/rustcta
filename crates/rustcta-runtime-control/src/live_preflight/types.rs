use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rustcta_types::MarketType;
use serde::{Deserialize, Serialize};

use super::LivePreflightConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivePreflightCheckStatus {
    Pass,
    Warn,
    Fail,
    Skipped,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivePreflightSeverity {
    Info,
    Warning,
    Error,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivePreflightGate {
    ConfigSafety,
    ApiCredentials,
    ExchangeConnectivity,
    SymbolRules,
    Balances,
    FeeModel,
    WebsocketBooks,
    DisabledUnmanaged,
    OrderValidation,
    ClientOrderId,
    RiskLimits,
    KillSwitch,
    Recorder,
    Monitoring,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveReadinessDecision {
    ReadyForPaperOnly,
    ReadyForLiveDryRun,
    ReadyForSmallLive,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LivePreflightCheck {
    pub gate: LivePreflightGate,
    pub name: String,
    pub status: LivePreflightCheckStatus,
    pub severity: LivePreflightSeverity,
    pub message: String,
    pub exchange: Option<String>,
    pub market_type: Option<MarketType>,
    pub symbol: Option<String>,
    #[serde(default)]
    pub details: HashMap<String, String>,
}

impl LivePreflightCheck {
    pub fn new(
        gate: LivePreflightGate,
        name: impl Into<String>,
        status: LivePreflightCheckStatus,
        severity: LivePreflightSeverity,
        message: impl Into<String>,
    ) -> Self {
        Self {
            gate,
            name: name.into(),
            status,
            severity,
            message: message.into(),
            exchange: None,
            market_type: None,
            symbol: None,
            details: HashMap::new(),
        }
    }

    pub fn scoped(
        mut self,
        exchange: Option<&str>,
        market_type: Option<MarketType>,
        symbol: Option<&str>,
    ) -> Self {
        self.exchange = exchange.map(|value| value.trim().to_ascii_lowercase());
        self.market_type = market_type;
        self.symbol = symbol.map(|value| value.trim().to_ascii_uppercase());
        self
    }

    pub fn detail(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.details.insert(key.into(), value.to_string());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ApiPermissionState {
    pub key_present: bool,
    pub secret_present: bool,
    pub read_permission: Option<bool>,
    pub trade_permission: Option<bool>,
    pub withdraw_permission: Option<bool>,
    pub account_read_probe_ok: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LivePreflightReport {
    pub timestamp: DateTime<Utc>,
    pub decision: LiveReadinessDecision,
    pub target_mode: String,
    pub checks: Vec<LivePreflightCheck>,
    pub pass_count: usize,
    pub warn_count: usize,
    pub fail_count: usize,
    pub skipped_count: usize,
    pub unknown_count: usize,
    pub critical_failures: Vec<String>,
    pub warnings: Vec<String>,
    pub suggested_next_actions: Vec<String>,
    pub per_exchange_readiness: HashMap<String, LivePreflightCheckStatus>,
    pub per_symbol_readiness: HashMap<String, LivePreflightCheckStatus>,
    pub config_summary: LivePreflightConfig,
}

impl LivePreflightReport {
    pub fn has_failures(&self) -> bool {
        self.fail_count > 0 || !self.critical_failures.is_empty()
    }
}
