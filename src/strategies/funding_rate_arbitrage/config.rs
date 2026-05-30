use std::collections::HashMap;

use crate::exchanges::adapters::PrivateWsRunConfig;
use crate::exchanges::config::{ExchangeRegistryConfig, ExchangeRuntimeSettings};
use crate::market::ExchangeId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingRateArbitrageConfig {
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default)]
    pub universe: UniverseConfig,
    #[serde(default)]
    pub selection: SelectionConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub exchanges: HashMap<ExchangeId, FundingExchangeRuntimeConfig>,
    #[serde(default)]
    pub notifications: NotificationConfig,
}

impl Default for FundingRateArbitrageConfig {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            universe: UniverseConfig::default(),
            selection: SelectionConfig::default(),
            execution: ExecutionConfig::default(),
            exchanges: HashMap::new(),
            notifications: NotificationConfig::default(),
        }
    }
}

impl FundingRateArbitrageConfig {
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.universe.enabled_exchanges.is_empty() {
            return Err(ConfigValidationError::NoExchanges);
        }
        if self.selection.per_exchange_limit == 0 {
            return Err(ConfigValidationError::InvalidPerExchangeLimit);
        }
        if !self.selection.min_funding_rate.is_finite() || self.selection.min_funding_rate >= 0.0 {
            return Err(ConfigValidationError::InvalidFundingThreshold);
        }
        if self.selection.max_funding_snapshot_age_ms <= 0 {
            return Err(ConfigValidationError::InvalidSnapshotAge);
        }
        if !matches!(
            self.mode.trim().to_ascii_lowercase().as_str(),
            "observe" | "live"
        ) {
            return Err(ConfigValidationError::InvalidMode);
        }
        if self.is_live_mode() {
            if !self.selection.require_next_funding_time
                || self.selection.max_seconds_to_settlement_at_scan.is_none()
            {
                return Err(ConfigValidationError::InvalidLiveSettlementWindow);
            }
            self.execution.validate()?;
        }
        Ok(())
    }

    pub fn is_live_mode(&self) -> bool {
        self.mode.trim().eq_ignore_ascii_case("live")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UniverseConfig {
    #[serde(default = "default_exchanges")]
    pub enabled_exchanges: Vec<ExchangeId>,
    #[serde(default = "default_quote_asset")]
    pub quote_asset: String,
    #[serde(default = "default_contract_type")]
    pub contract_type: String,
    #[serde(default)]
    pub symbol_allowlist: Vec<String>,
    #[serde(default)]
    pub symbol_blocklist: Vec<String>,
}

impl Default for UniverseConfig {
    fn default() -> Self {
        Self {
            enabled_exchanges: default_exchanges(),
            quote_asset: default_quote_asset(),
            contract_type: default_contract_type(),
            symbol_allowlist: Vec::new(),
            symbol_blocklist: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectionConfig {
    #[serde(default = "default_per_exchange_limit")]
    pub per_exchange_limit: usize,
    #[serde(default = "default_min_funding_rate")]
    pub min_funding_rate: f64,
    #[serde(default = "default_true")]
    pub require_next_funding_time: bool,
    #[serde(default = "default_max_funding_snapshot_age_ms")]
    pub max_funding_snapshot_age_ms: i64,
    #[serde(default)]
    pub min_seconds_to_settlement_at_scan: Option<i64>,
    #[serde(default)]
    pub max_seconds_to_settlement_at_scan: Option<i64>,
}

impl Default for SelectionConfig {
    fn default() -> Self {
        Self {
            per_exchange_limit: default_per_exchange_limit(),
            min_funding_rate: default_min_funding_rate(),
            require_next_funding_time: true,
            max_funding_snapshot_age_ms: default_max_funding_snapshot_age_ms(),
            min_seconds_to_settlement_at_scan: None,
            max_seconds_to_settlement_at_scan: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_scan_minute")]
    pub scan_minute: u32,
    #[serde(default = "default_notional_usdt")]
    pub notional_usdt: f64,
    #[serde(default = "default_open_seconds_before_settlement")]
    pub open_seconds_before_settlement: i64,
    #[serde(default = "default_close_seconds_after_settlement")]
    pub close_seconds_after_settlement: i64,
    #[serde(default = "default_order_readback_delay_secs")]
    pub order_readback_delay_secs: u64,
    #[serde(default = "default_max_slippage_pct")]
    pub max_slippage_pct: f64,
    #[serde(default)]
    pub allow_existing_symbol_position: bool,
    #[serde(default = "default_live_position_side")]
    pub position_side: String,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            scan_minute: default_scan_minute(),
            notional_usdt: default_notional_usdt(),
            open_seconds_before_settlement: default_open_seconds_before_settlement(),
            close_seconds_after_settlement: default_close_seconds_after_settlement(),
            order_readback_delay_secs: default_order_readback_delay_secs(),
            max_slippage_pct: default_max_slippage_pct(),
            allow_existing_symbol_position: false,
            position_side: default_live_position_side(),
        }
    }
}

impl ExecutionConfig {
    fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.scan_minute > 59 {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if !self.notional_usdt.is_finite() || self.notional_usdt <= 0.0 {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if self.notional_usdt > 20.0 {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if self.open_seconds_before_settlement < 0 || self.close_seconds_after_settlement < 0 {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if !self.max_slippage_pct.is_finite()
            || self.max_slippage_pct < 0.0
            || self.max_slippage_pct > 0.02
        {
            return Err(ConfigValidationError::InvalidExecution);
        }
        if !matches!(
            self.position_side.trim().to_ascii_lowercase().as_str(),
            "long"
        ) {
            return Err(ConfigValidationError::InvalidExecution);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FundingExchangeRuntimeConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub env_prefix: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub demo_trading: bool,
    #[serde(default)]
    pub private_rest_base_url: Option<String>,
    #[serde(default)]
    pub private_ws_url: Option<String>,
    #[serde(default)]
    pub private_ws_enabled: bool,
    #[serde(default)]
    pub private_ws_run: PrivateWsRunConfig,
    #[serde(default = "default_position_mode")]
    pub position_mode: Option<String>,
}

impl Default for FundingExchangeRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: None,
            env_prefix: None,
            account_id: None,
            demo_trading: false,
            private_rest_base_url: None,
            private_ws_url: None,
            private_ws_enabled: false,
            private_ws_run: PrivateWsRunConfig::default(),
            position_mode: default_position_mode(),
        }
    }
}

impl ExchangeRuntimeSettings for FundingExchangeRuntimeConfig {
    fn is_disabled(&self) -> bool {
        self.enabled == Some(false)
    }

    fn env_prefix(&self) -> Option<&str> {
        self.env_prefix.as_deref()
    }

    fn account_id(&self) -> Option<&str> {
        self.account_id.as_deref()
    }

    fn demo_trading(&self) -> bool {
        self.demo_trading
    }

    fn private_rest_base_url(&self) -> Option<&str> {
        self.private_rest_base_url.as_deref()
    }

    fn private_ws_url(&self) -> Option<&str> {
        self.private_ws_url.as_deref()
    }

    fn private_ws_enabled(&self) -> bool {
        self.private_ws_enabled
    }

    fn private_ws_run(&self) -> PrivateWsRunConfig {
        self.private_ws_run
    }

    fn position_mode_text(&self) -> Option<&str> {
        self.position_mode.as_deref()
    }
}

impl ExchangeRegistryConfig for FundingRateArbitrageConfig {
    type Entry = FundingExchangeRuntimeConfig;

    fn exchange_runtime(&self) -> &HashMap<ExchangeId, Self::Entry> {
        &self.exchanges
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NotificationConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_wecom_webhook_env")]
    pub wecom_webhook_env: String,
    #[serde(default)]
    pub wecom_webhook_url: Option<String>,
}

impl Default for NotificationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            wecom_webhook_env: default_wecom_webhook_env(),
            wecom_webhook_url: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigValidationError {
    #[error("mode must be observe or live")]
    InvalidMode,
    #[error("funding-rate arbitrage requires at least one enabled exchange")]
    NoExchanges,
    #[error("selection.per_exchange_limit must be greater than zero")]
    InvalidPerExchangeLimit,
    #[error("selection.min_funding_rate must be a finite negative number")]
    InvalidFundingThreshold,
    #[error("selection.max_funding_snapshot_age_ms must be greater than zero")]
    InvalidSnapshotAge,
    #[error("execution config is invalid")]
    InvalidExecution,
    #[error(
        "live mode requires require_next_funding_time=true and max_seconds_to_settlement_at_scan"
    )]
    InvalidLiveSettlementWindow,
}

fn default_mode() -> String {
    "observe".to_string()
}

fn default_exchanges() -> Vec<ExchangeId> {
    vec![ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate]
}

fn default_quote_asset() -> String {
    "USDT".to_string()
}

fn default_contract_type() -> String {
    "perpetual".to_string()
}

fn default_per_exchange_limit() -> usize {
    1
}

fn default_min_funding_rate() -> f64 {
    -0.005
}

fn default_max_funding_snapshot_age_ms() -> i64 {
    5_000
}

fn default_wecom_webhook_env() -> String {
    "FUNDING_ARB_WECOM_WEBHOOK_URL".to_string()
}

fn default_true() -> bool {
    true
}

fn default_scan_minute() -> u32 {
    55
}

fn default_notional_usdt() -> f64 {
    10.0
}

fn default_open_seconds_before_settlement() -> i64 {
    1
}

fn default_close_seconds_after_settlement() -> i64 {
    1
}

fn default_order_readback_delay_secs() -> u64 {
    3
}

fn default_max_slippage_pct() -> f64 {
    0.003
}

fn default_live_position_side() -> String {
    "long".to_string()
}

fn default_position_mode() -> Option<String> {
    Some("hedge".to_string())
}
