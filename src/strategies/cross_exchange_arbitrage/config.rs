//! Configuration contracts for cross-exchange arbitrage simulation.

use super::fees::ExchangeFeeRates;
use crate::market::{CanonicalSymbol, ExchangeId, RuntimeMode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossExchangeArbitrageConfig {
    pub mode: RuntimeMode,
    #[serde(default)]
    pub strategy: StrategyConfig,
    #[serde(default)]
    pub market: MarketConfig,
    pub thresholds: ThresholdConfig,
    pub sizing: SizingConfig,
    pub fees: FeeConfig,
    pub funding: FundingConfig,
    #[serde(default)]
    pub exchanges: HashMap<ExchangeId, ExchangeRuntimeConfig>,
    #[serde(default)]
    pub routing: RoutingConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub reconciliation: ReconciliationConfig,
    pub risk: RiskConfig,
    #[serde(default)]
    pub persistence: PersistenceConfig,
    #[serde(default)]
    pub dashboard: DashboardConfig,
    #[serde(default)]
    pub alerts: AlertConfig,
    pub universe: UniverseConfig,
}

impl Default for CrossExchangeArbitrageConfig {
    fn default() -> Self {
        Self {
            mode: RuntimeMode::Simulation,
            strategy: StrategyConfig::default(),
            market: MarketConfig::default(),
            thresholds: ThresholdConfig::default(),
            sizing: SizingConfig::default(),
            fees: FeeConfig::default(),
            funding: FundingConfig::default(),
            exchanges: HashMap::new(),
            routing: RoutingConfig::default(),
            execution: ExecutionConfig::default(),
            reconciliation: ReconciliationConfig::default(),
            risk: RiskConfig::default(),
            persistence: PersistenceConfig::default(),
            dashboard: DashboardConfig::default(),
            alerts: AlertConfig::default(),
            universe: UniverseConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StrategyConfig {
    pub name: String,
    #[serde(default)]
    pub mode: Option<RuntimeMode>,
    pub log_level: String,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            name: "cross_exchange_arbitrage_usdt_perp".to_string(),
            mode: Some(RuntimeMode::Simulation),
            log_level: "INFO".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketConfig {
    pub quote_asset: String,
    pub market_type: String,
    pub depth_levels: u16,
    pub stale_quote_ms: i64,
    pub min_common_exchanges: usize,
}

impl Default for MarketConfig {
    fn default() -> Self {
        Self {
            quote_asset: "USDT".to_string(),
            market_type: "futures".to_string(),
            depth_levels: 5,
            stale_quote_ms: 1_000,
            min_common_exchanges: 2,
        }
    }
}

impl CrossExchangeArbitrageConfig {
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.thresholds.min_display_raw_spread < 0.0
            || self.thresholds.min_open_maker_taker_net_edge < 0.0
            || self.thresholds.lock_profit_dual_taker_pct < 0.0
            || self.thresholds.strong_lock_profit_dual_taker_pct < 0.0
        {
            return Err(ConfigValidationError::InvalidThreshold);
        }
        if self.sizing.min_notional_usdt <= 0.0
            || self.sizing.target_notional_usdt <= 0.0
            || self.sizing.max_notional_usdt < self.sizing.min_notional_usdt
            || self.sizing.exchange_equity_usdt <= 0.0
            || self.sizing.leverage <= 0.0
            || self.sizing.max_positions_per_exchange == 0
            || self.sizing.capital_fraction_of_smaller_equity <= 0.0
            || self.sizing.capital_fraction_of_smaller_equity > 1.0
            || self.sizing.max_symbol_notional_usdt <= 0.0
            || self.sizing.max_exchange_usage_pct <= 0.0
            || self.sizing.max_exchange_usage_pct > 1.0
        {
            return Err(ConfigValidationError::InvalidSizing);
        }
        if let Some(strategy_mode) = self.strategy.mode {
            if strategy_mode != self.mode {
                return Err(ConfigValidationError::ModeMismatch {
                    top_level: self.mode,
                    strategy: strategy_mode,
                });
            }
        }
        if self.mode.allows_live_orders()
            && self.risk.symbol_whitelist_required_live
            && self.universe.symbols.is_empty()
        {
            return Err(ConfigValidationError::LiveModeRequiresWhitelist);
        }
        if self.mode.allows_live_orders() {
            for exchange in &self.universe.enabled_exchanges {
                if self
                    .exchanges
                    .get(exchange)
                    .and_then(|runtime| runtime.enabled)
                    == Some(false)
                {
                    return Err(ConfigValidationError::LiveExchangeDisabled {
                        exchange: exchange.clone(),
                    });
                }
                if self.execution.dry_run == false
                    && self
                        .exchanges
                        .get(exchange)
                        .map(|runtime| !runtime.private_rest_enabled)
                        .unwrap_or(false)
                {
                    return Err(ConfigValidationError::LivePrivateRestDisabled {
                        exchange: exchange.clone(),
                    });
                }
            }
            for (exchange, runtime) in &self.exchanges {
                if runtime.private_ws_enabled
                    && (runtime.private_ws_run.connect_timeout_ms == 0
                        || runtime.private_ws_run.reconnect_delay_ms == 0
                        || runtime.private_ws_run.heartbeat_interval_ms == 0)
                {
                    return Err(ConfigValidationError::InvalidPrivateWsRuntime {
                        exchange: exchange.clone(),
                    });
                }
            }
        }
        if self.risk.max_open_bundles == 0 {
            return Err(ConfigValidationError::InvalidRisk);
        }
        for exchange in &self.universe.enabled_exchanges {
            if !self.fees.per_exchange.contains_key(exchange) {
                return Err(ConfigValidationError::MissingFee(exchange.clone()));
            }
        }
        Ok(())
    }

    pub fn ws_batch_size_for(&self, exchange: &ExchangeId, fallback: usize) -> usize {
        self.exchanges
            .get(exchange)
            .and_then(|exchange| exchange.ws_batch_size)
            .filter(|batch_size| *batch_size > 0)
            .unwrap_or(fallback.max(1))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigValidationError {
    #[error("threshold values must be non-negative")]
    InvalidThreshold,
    #[error("sizing values must be positive and max >= min")]
    InvalidSizing,
    #[error("live mode requires an explicit symbol whitelist")]
    LiveModeRequiresWhitelist,
    #[error("missing fee config for exchange {0}")]
    MissingFee(ExchangeId),
    #[error("top-level mode {top_level:?} does not match strategy.mode {strategy:?}")]
    ModeMismatch {
        top_level: RuntimeMode,
        strategy: RuntimeMode,
    },
    #[error("live mode universe enables {exchange}, but exchanges.{exchange}.enabled=false")]
    LiveExchangeDisabled { exchange: ExchangeId },
    #[error("live mode with dry_run=false requires private REST for exchange {exchange}")]
    LivePrivateRestDisabled { exchange: ExchangeId },
    #[error("private websocket runtime for exchange {exchange} must use positive durations")]
    InvalidPrivateWsRuntime { exchange: ExchangeId },
    #[error("risk values must be positive and internally consistent")]
    InvalidRisk,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdConfig {
    pub min_display_raw_spread: f64,
    pub min_open_maker_taker_net_edge: f64,
    #[serde(default = "default_max_open_raw_spread")]
    pub max_open_raw_spread: f64,
    pub lock_profit_dual_taker_pct: f64,
    pub strong_lock_profit_dual_taker_pct: f64,
}

impl Default for ThresholdConfig {
    fn default() -> Self {
        Self {
            min_display_raw_spread: 0.001,
            min_open_maker_taker_net_edge: 0.005,
            max_open_raw_spread: default_max_open_raw_spread(),
            lock_profit_dual_taker_pct: 0.003,
            strong_lock_profit_dual_taker_pct: 0.005,
        }
    }
}

fn default_max_open_raw_spread() -> f64 {
    0.10
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SizingConfig {
    pub min_notional_usdt: f64,
    pub target_notional_usdt: f64,
    pub max_notional_usdt: f64,
    #[serde(default = "default_exchange_equity_usdt")]
    pub exchange_equity_usdt: f64,
    #[serde(default = "default_leverage")]
    pub leverage: f64,
    #[serde(default = "default_max_positions_per_exchange")]
    pub max_positions_per_exchange: usize,
    #[serde(default = "default_capital_fraction_of_smaller_equity")]
    pub capital_fraction_of_smaller_equity: f64,
    #[serde(default = "default_max_symbol_notional_usdt")]
    pub max_symbol_notional_usdt: f64,
    #[serde(default = "default_max_exchange_usage_pct")]
    pub max_exchange_usage_pct: f64,
}

impl Default for SizingConfig {
    fn default() -> Self {
        Self {
            min_notional_usdt: 20.0,
            target_notional_usdt: 100.0,
            max_notional_usdt: 500.0,
            exchange_equity_usdt: default_exchange_equity_usdt(),
            leverage: default_leverage(),
            max_positions_per_exchange: default_max_positions_per_exchange(),
            capital_fraction_of_smaller_equity: default_capital_fraction_of_smaller_equity(),
            max_symbol_notional_usdt: default_max_symbol_notional_usdt(),
            max_exchange_usage_pct: default_max_exchange_usage_pct(),
        }
    }
}

fn default_exchange_equity_usdt() -> f64 {
    500.0
}

fn default_leverage() -> f64 {
    10.0
}

fn default_max_positions_per_exchange() -> usize {
    50
}

fn default_capital_fraction_of_smaller_equity() -> f64 {
    0.10
}

fn default_max_symbol_notional_usdt() -> f64 {
    200.0
}

fn default_max_exchange_usage_pct() -> f64 {
    0.35
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeeConfig {
    pub default_maker_fee_rate: f64,
    pub default_taker_fee_rate: f64,
    #[serde(default)]
    pub use_account_fee_api: bool,
    pub per_exchange: HashMap<ExchangeId, ExchangeFeeRates>,
}

impl Default for FeeConfig {
    fn default() -> Self {
        let mut per_exchange = HashMap::new();
        per_exchange.insert(
            ExchangeId::Binance,
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.0005,
            },
        );
        per_exchange.insert(
            ExchangeId::Okx,
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.0005,
            },
        );
        per_exchange.insert(
            ExchangeId::Bitget,
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.0006,
            },
        );
        per_exchange.insert(
            ExchangeId::Gate,
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.0005,
            },
        );

        Self {
            default_maker_fee_rate: 0.0002,
            default_taker_fee_rate: 0.0005,
            use_account_fee_api: false,
            per_exchange,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FundingConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_funding_mode")]
    pub mode: String,
    pub expected_holding_hours: f64,
    #[serde(default = "default_funding_refresh_secs")]
    pub refresh_secs: u64,
    pub no_open_before_funding_mins: i64,
    pub block_if_expected_funding_negative_pct: f64,
    pub max_adverse_funding_rate: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionConfig {
    pub dry_run: bool,
    pub max_hedge_retries: u32,
    pub maker_order_ttl_ms: u64,
    pub taker_ioc_slippage_limit_pct: f64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            dry_run: true,
            max_hedge_retries: 1,
            maker_order_ttl_ms: 5_000,
            taker_ioc_slippage_limit_pct: 0.003,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconciliationConfig {
    pub open_orders_interval_secs: u64,
    pub positions_interval_secs: u64,
    pub full_audit_interval_secs: u64,
    pub quantity_tolerance: f64,
    pub orphan_tolerance: f64,
}

impl Default for ReconciliationConfig {
    fn default() -> Self {
        Self {
            open_orders_interval_secs: 5,
            positions_interval_secs: 10,
            full_audit_interval_secs: 60,
            quantity_tolerance: 1e-8,
            orphan_tolerance: 1e-6,
        }
    }
}

impl Default for FundingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: default_funding_mode(),
            expected_holding_hours: 8.0,
            refresh_secs: default_funding_refresh_secs(),
            no_open_before_funding_mins: 5,
            block_if_expected_funding_negative_pct: 0.001,
            max_adverse_funding_rate: 0.001,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_funding_mode() -> String {
    "settlement".to_string()
}

fn default_funding_refresh_secs() -> u64 {
    30
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ExchangeRuntimeConfig {
    #[serde(default)]
    pub enabled: Option<bool>,
    #[serde(default)]
    pub position_mode: Option<String>,
    #[serde(default)]
    pub ws_batch_size: Option<usize>,
    #[serde(default)]
    pub env_prefix: Option<String>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default = "default_true")]
    pub private_rest_enabled: bool,
    #[serde(default)]
    pub private_ws_enabled: bool,
    #[serde(default)]
    pub private_ws_run: PrivateWsRuntimeConfig,
    #[serde(default)]
    pub routes: ExchangeRouteConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivateWsRuntimeConfig {
    pub connect_timeout_ms: u64,
    pub reconnect_delay_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub subscribe_interval_ms: u64,
}

impl Default for PrivateWsRuntimeConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            reconnect_delay_ms: 2_000,
            heartbeat_interval_ms: 15_000,
            subscribe_interval_ms: 50,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ExchangeRouteConfig {
    #[serde(default)]
    pub market_ws: Vec<String>,
    #[serde(default)]
    pub private_ws: Vec<String>,
    #[serde(default)]
    pub rest_public: Vec<String>,
    #[serde(default)]
    pub rest_private: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    pub ws_stale_limit_ms: i64,
    pub order_latency_p95_degrade_ms: i64,
    pub order_error_rate_degrade: f64,
    pub auto_failover: bool,
    pub degraded_mode_blocks_new_entries: bool,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            ws_stale_limit_ms: 30_000,
            order_latency_p95_degrade_ms: 1_200,
            order_error_rate_degrade: 0.05,
            auto_failover: true,
            degraded_mode_blocks_new_entries: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RiskConfig {
    pub stale_quote_ms: i64,
    pub max_book_age_ms: i64,
    pub taker_slippage_buffer: f64,
    pub max_taker_slippage_pct: f64,
    pub maker_non_fill_penalty: f64,
    pub safety_buffer: f64,
    pub max_notional_per_symbol_usdt: f64,
    #[serde(default = "default_max_open_bundles")]
    pub max_open_bundles: usize,
    #[serde(default = "default_true")]
    pub symbol_whitelist_required_live: bool,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            stale_quote_ms: 1_500,
            max_book_age_ms: 2_000,
            taker_slippage_buffer: 0.0005,
            max_taker_slippage_pct: 0.003,
            maker_non_fill_penalty: 0.001,
            safety_buffer: 0.001,
            max_notional_per_symbol_usdt: 2_000.0,
            max_open_bundles: default_max_open_bundles(),
            symbol_whitelist_required_live: true,
        }
    }
}

fn default_max_open_bundles() -> usize {
    3
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UniverseConfig {
    pub enabled_exchanges: Vec<ExchangeId>,
    pub symbols: Vec<CanonicalSymbol>,
    #[serde(default = "default_universe_mode")]
    pub mode: String,
    #[serde(default)]
    pub target_symbol_count: Option<usize>,
    #[serde(default)]
    pub max_symbol_count: Option<usize>,
    #[serde(default)]
    pub include_symbols: Vec<CanonicalSymbol>,
    #[serde(default)]
    pub exclude_symbols: Vec<CanonicalSymbol>,
    #[serde(default)]
    pub min_24h_quote_volume: Option<f64>,
}

impl Default for UniverseConfig {
    fn default() -> Self {
        Self {
            enabled_exchanges: vec![
                ExchangeId::Binance,
                ExchangeId::Okx,
                ExchangeId::Bitget,
                ExchangeId::Gate,
            ],
            symbols: vec![CanonicalSymbol::new("BTC", "USDT")],
            mode: default_universe_mode(),
            target_symbol_count: None,
            max_symbol_count: None,
            include_symbols: Vec::new(),
            exclude_symbols: Vec::new(),
            min_24h_quote_volume: None,
        }
    }
}

fn default_universe_mode() -> String {
    "static".to_string()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PersistenceConfig {
    pub enabled: bool,
    pub jsonl_dir: String,
    pub clickhouse_enabled: bool,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            jsonl_dir: "logs/cross_exchange_arbitrage".to_string(),
            clickhouse_enabled: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DashboardConfig {
    pub enabled: bool,
    pub bind_host: String,
    pub port: u16,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bind_host: "0.0.0.0".to_string(),
            port: 8090,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertConfig {
    pub enabled: bool,
    pub orphan_alert: bool,
    pub route_offline_alert: bool,
    pub reconcile_critical_alert: bool,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            orphan_alert: true,
            route_offline_alert: true,
            reconcile_critical_alert: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_validate_should_accept_default_simulation() {
        CrossExchangeArbitrageConfig::default().validate().unwrap();
    }

    #[test]
    fn config_validate_should_reject_live_without_symbol_whitelist() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.universe.symbols.clear();

        assert_eq!(
            config.validate().unwrap_err(),
            ConfigValidationError::LiveModeRequiresWhitelist
        );
    }

    #[test]
    fn config_validate_should_reject_live_universe_exchange_disabled_in_runtime() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config
            .exchanges
            .entry(ExchangeId::Gate)
            .or_default()
            .enabled = Some(false);

        assert_eq!(
            config.validate().unwrap_err(),
            ConfigValidationError::LiveExchangeDisabled {
                exchange: ExchangeId::Gate
            }
        );
    }

    #[test]
    fn config_validate_should_reject_live_trading_without_private_rest() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.execution.dry_run = false;
        config
            .exchanges
            .entry(ExchangeId::Bitget)
            .or_default()
            .private_rest_enabled = false;

        assert_eq!(
            config.validate().unwrap_err(),
            ConfigValidationError::LivePrivateRestDisabled {
                exchange: ExchangeId::Bitget
            }
        );
    }

    #[test]
    fn config_template_should_parse_and_validate() {
        let raw = include_str!("../../../config/cross_exchange_arbitrage_usdt.yml");
        let config: CrossExchangeArbitrageConfig =
            serde_yaml::from_str(raw).expect("template should parse");

        config.validate().expect("template should validate");
        assert_eq!(config.mode, RuntimeMode::Simulation);
        assert!(config
            .universe
            .enabled_exchanges
            .contains(&ExchangeId::Binance));
        assert!(config
            .universe
            .symbols
            .contains(&CanonicalSymbol::new("ARB", "USDT")));
        assert!(!config
            .universe
            .symbols
            .contains(&CanonicalSymbol::new("BTC", "USDT")));
        assert_eq!(config.universe.mode, "curated_long_tail");
        assert_eq!(config.sizing.capital_fraction_of_smaller_equity, 0.10);
        assert_eq!(config.risk.max_open_bundles, 3);
        assert!(config.routing.auto_failover);
        assert!(config.funding.enabled);
    }

    #[test]
    fn config_template_should_reject_unknown_fields() {
        let raw = r#"
mode: simulation
unknown_top_level: true
thresholds:
  min_display_raw_spread: 0.001
  min_open_maker_taker_net_edge: 0.005
  lock_profit_dual_taker_pct: 0.003
  strong_lock_profit_dual_taker_pct: 0.005
sizing:
  min_notional_usdt: 20.0
  target_notional_usdt: 100.0
  max_notional_usdt: 500.0
  exchange_equity_usdt: 500.0
  leverage: 10.0
  max_positions_per_exchange: 50
fees:
  default_maker_fee_rate: 0.0002
  default_taker_fee_rate: 0.0005
  per_exchange:
    binance:
      maker: 0.0002
      taker: 0.0005
funding:
  expected_holding_hours: 8.0
  no_open_before_funding_mins: 5
  block_if_expected_funding_negative_pct: 0.001
  max_adverse_funding_rate: 0.001
risk:
  stale_quote_ms: 1500
  max_book_age_ms: 2000
  taker_slippage_buffer: 0.0005
  max_taker_slippage_pct: 0.003
  maker_non_fill_penalty: 0.001
  safety_buffer: 0.001
  max_notional_per_symbol_usdt: 2000.0
universe:
  enabled_exchanges:
    - binance
  symbols:
    - ARB/USDT
"#;

        let err = serde_yaml::from_str::<CrossExchangeArbitrageConfig>(raw)
            .expect_err("unknown fields should be rejected");
        assert!(err.to_string().contains("unknown_top_level"));
    }
}
