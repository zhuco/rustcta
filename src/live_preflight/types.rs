use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::exchanges::unified::{MarketType, SymbolRule};
use crate::web::{BookView, ExchangeHealthView, FeeView, InventoryView, RecorderHealthView};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ApiPermissionState {
    pub key_present: bool,
    pub secret_present: bool,
    pub read_permission: Option<bool>,
    pub trade_permission: Option<bool>,
    pub withdraw_permission: Option<bool>,
    pub account_read_probe_ok: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveReadinessState {
    pub trading_mode: String,
    pub live_trading_enabled: bool,
    pub dry_run: bool,
    pub monitoring_enabled: bool,
    pub recorder_enabled: bool,
    pub kill_switch_available: bool,
    pub kill_switch_active: bool,
    pub emergency_stop_configured: bool,
    pub max_daily_loss: Option<f64>,
    pub max_order_latency_ms: Option<u64>,
    pub symbol_rules: Vec<SymbolRule>,
    pub inventory: Vec<InventoryView>,
    pub fees: Vec<FeeView>,
    pub books: Vec<BookView>,
    pub exchanges: Vec<ExchangeHealthView>,
    pub disabled_symbols: Vec<String>,
    pub disabled_exchanges: Vec<String>,
    pub disabled_exchange_symbols: Vec<(String, MarketType, String)>,
    pub unmanaged_positions: Vec<(String, MarketType, String, String, f64)>,
    pub api_permissions: HashMap<String, ApiPermissionState>,
    pub recorder: RecorderHealthView,
}

impl Default for LiveReadinessState {
    fn default() -> Self {
        Self {
            trading_mode: "paper".to_string(),
            live_trading_enabled: false,
            dry_run: true,
            monitoring_enabled: false,
            recorder_enabled: false,
            kill_switch_available: false,
            kill_switch_active: false,
            emergency_stop_configured: false,
            max_daily_loss: None,
            max_order_latency_ms: None,
            symbol_rules: Vec::new(),
            inventory: Vec::new(),
            fees: Vec::new(),
            books: Vec::new(),
            exchanges: Vec::new(),
            disabled_symbols: Vec::new(),
            disabled_exchanges: Vec::new(),
            disabled_exchange_symbols: Vec::new(),
            unmanaged_positions: Vec::new(),
            api_permissions: HashMap::new(),
            recorder: RecorderHealthView::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
