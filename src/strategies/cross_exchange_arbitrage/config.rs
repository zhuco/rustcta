//! Configuration contracts for cross-exchange arbitrage simulation.

use super::fees::ExchangeFeeRates;
use crate::exchanges::config::{ExchangeRegistryConfig, ExchangeRuntimeSettings};
use crate::exchanges::private_perp::PrivateWsRunConfig;
use crate::market::{CanonicalSymbol, ExchangeId, PublicBookProfileKind, RouteStatus, RuntimeMode};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CrossExchangeArbitrageConfig {
    pub mode: RuntimeMode,
    #[serde(default)]
    pub trading_mode: TradingMode,
    #[serde(default)]
    pub enable_live_trading: bool,
    #[serde(default)]
    pub max_live_notional_per_trade: Option<f64>,
    #[serde(default)]
    pub enabled_symbols: Vec<CanonicalSymbol>,
    #[serde(default)]
    pub enabled_exchanges: Vec<ExchangeId>,
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
    pub controls: RuntimeControlConfig,
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
    #[serde(default)]
    pub detection: DetectionConfig,
}

impl Default for CrossExchangeArbitrageConfig {
    fn default() -> Self {
        Self {
            mode: RuntimeMode::Simulation,
            trading_mode: TradingMode::Paper,
            enable_live_trading: false,
            max_live_notional_per_trade: None,
            enabled_symbols: Vec::new(),
            enabled_exchanges: Vec::new(),
            strategy: StrategyConfig::default(),
            market: MarketConfig::default(),
            thresholds: ThresholdConfig::default(),
            sizing: SizingConfig::default(),
            fees: FeeConfig::default(),
            funding: FundingConfig::default(),
            exchanges: HashMap::new(),
            routing: RoutingConfig::default(),
            execution: ExecutionConfig::default(),
            controls: RuntimeControlConfig::default(),
            reconciliation: ReconciliationConfig::default(),
            risk: RiskConfig::default(),
            persistence: PersistenceConfig::default(),
            dashboard: DashboardConfig::default(),
            alerts: AlertConfig::default(),
            universe: UniverseConfig::default(),
            detection: DetectionConfig::default(),
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
    #[serde(default = "default_public_book_speed")]
    pub public_book_speed: String,
    #[serde(default = "default_public_book_trigger_profile")]
    pub public_book_trigger_profile: String,
    #[serde(default = "default_public_book_validation_profile")]
    pub public_book_validation_profile: String,
    #[serde(default = "default_public_book_depth")]
    pub public_book_depth: u16,
    #[serde(default = "default_allow_top_of_book_only")]
    pub allow_top_of_book_only: bool,
    #[serde(default = "default_top_of_book_capacity_ratio")]
    pub top_of_book_capacity_ratio: f64,
}

impl Default for MarketConfig {
    fn default() -> Self {
        Self {
            quote_asset: "USDT".to_string(),
            market_type: "futures".to_string(),
            depth_levels: 5,
            stale_quote_ms: 1_000,
            min_common_exchanges: 2,
            public_book_speed: default_public_book_speed(),
            public_book_trigger_profile: default_public_book_trigger_profile(),
            public_book_validation_profile: default_public_book_validation_profile(),
            public_book_depth: default_public_book_depth(),
            allow_top_of_book_only: default_allow_top_of_book_only(),
            top_of_book_capacity_ratio: default_top_of_book_capacity_ratio(),
        }
    }
}

fn default_public_book_speed() -> String {
    "fastest".to_string()
}

fn default_public_book_trigger_profile() -> String {
    "fastest_l1".to_string()
}

fn default_public_book_validation_profile() -> String {
    "fastest_depth".to_string()
}

fn default_public_book_depth() -> u16 {
    5
}

fn default_allow_top_of_book_only() -> bool {
    true
}

fn default_top_of_book_capacity_ratio() -> f64 {
    0.8
}

impl MarketConfig {
    pub fn public_book_trigger_profile_kind(&self) -> Option<PublicBookProfileKind> {
        parse_public_book_profile_kind(&self.public_book_trigger_profile)
    }

    pub fn public_book_validation_profile_kind(&self) -> Option<PublicBookProfileKind> {
        parse_public_book_profile_kind(&self.public_book_validation_profile)
    }
}

pub fn parse_public_book_profile_kind(value: &str) -> Option<PublicBookProfileKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "fastest_l1" | "l1" | "book_ticker" | "bookticker" => {
            Some(PublicBookProfileKind::FastestL1)
        }
        "fastest_depth" | "depth" | "depth5" | "books5" => {
            Some(PublicBookProfileKind::FastestDepth)
        }
        "conservative_depth" | "conservative" | "full_depth" | "books" => {
            Some(PublicBookProfileKind::ConservativeDepth)
        }
        _ => None,
    }
}

impl CrossExchangeArbitrageConfig {
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.thresholds.min_display_raw_spread < 0.0
            || self.thresholds.min_open_raw_spread < 0.0
            || self.thresholds.min_open_maker_taker_net_edge < 0.0
            || self.thresholds.max_open_raw_spread < self.thresholds.min_open_raw_spread
            || self.thresholds.lock_profit_dual_taker_pct < 0.0
            || self.thresholds.strong_lock_profit_dual_taker_pct < 0.0
            || self.thresholds.max_close_spread_pct < 0.0
        {
            return Err(ConfigValidationError::InvalidThreshold);
        }
        for route in &self.thresholds.route_overrides {
            if route.min_open_raw_spread.is_some_and(|value| value < 0.0)
                || route
                    .min_open_maker_taker_net_edge
                    .is_some_and(|value| value < 0.0)
                || route.max_open_raw_spread.is_some_and(|value| value < 0.0)
            {
                return Err(ConfigValidationError::InvalidThreshold);
            }
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
        if self.market.depth_levels == 0
            || self.market.stale_quote_ms <= 0
            || self.market.min_common_exchanges == 0
            || self.market.public_book_depth == 0
            || self.market.public_book_speed.trim().is_empty()
            || self.market.public_book_trigger_profile.trim().is_empty()
            || self.market.public_book_validation_profile.trim().is_empty()
            || self.market.public_book_trigger_profile_kind().is_none()
            || self.market.public_book_validation_profile_kind().is_none()
            || !self.market.top_of_book_capacity_ratio.is_finite()
            || self.market.top_of_book_capacity_ratio <= 0.0
            || self.market.top_of_book_capacity_ratio > 1.0
        {
            return Err(ConfigValidationError::InvalidMarket);
        }
        if self.trading_mode == TradingMode::Live {
            if !self.mode.allows_live_orders() {
                return Err(ConfigValidationError::LiveTradingRequiresLiveRuntimeMode);
            }
            if !self.enable_live_trading {
                return Err(ConfigValidationError::LiveTradingNotEnabled);
            }
            if self
                .max_live_notional_per_trade
                .filter(|value| value.is_finite() && *value > 0.0)
                .is_none()
            {
                return Err(ConfigValidationError::MissingMaxLiveNotional);
            }
            if self.enabled_symbols.is_empty() {
                return Err(ConfigValidationError::MissingLiveEnabledSymbols);
            }
            if self.enabled_exchanges.is_empty() {
                return Err(ConfigValidationError::MissingLiveEnabledExchanges);
            }
            if self.execution.taker_ioc_slippage_limit_pct <= 0.0
                || !self.execution.taker_ioc_slippage_limit_pct.is_finite()
            {
                return Err(ConfigValidationError::LiveTradingRequiresSlippageLimit);
            }
        }
        if self.mode.allows_live_orders() && !self.execution.dry_run {
            if self.execution.open_execution_style != OpenExecutionStyle::DualTaker {
                return Err(ConfigValidationError::LiveTradingRequiresDualTakerOpen);
            }
            if self.execution.close_execution_style != OpenExecutionStyle::DualTaker {
                return Err(ConfigValidationError::LiveTradingRequiresDualTakerClose);
            }
            if self.execution.taker_ioc_slippage_limit_pct <= 0.0
                || !self.execution.taker_ioc_slippage_limit_pct.is_finite()
            {
                return Err(ConfigValidationError::LiveTradingRequiresSlippageLimit);
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
                let runtime = self.exchanges.get(exchange);
                if runtime
                    .map(ExchangeRuntimeConfig::is_disabled)
                    .unwrap_or(false)
                {
                    return Err(ConfigValidationError::LiveExchangeDisabled {
                        exchange: exchange.clone(),
                    });
                }
                if !self.execution.dry_run
                    && runtime
                        .map(|runtime| !runtime.private_rest_enabled)
                        .unwrap_or(false)
                {
                    return Err(ConfigValidationError::LivePrivateRestDisabled {
                        exchange: exchange.clone(),
                    });
                }
                if runtime
                    .map(|runtime| !runtime.private_ws_enabled)
                    .unwrap_or(true)
                {
                    return Err(ConfigValidationError::LivePrivateWsDisabled {
                        exchange: exchange.clone(),
                    });
                }
            }
            for (exchange, runtime) in &self.exchanges {
                if runtime.private_ws_enabled
                    && (runtime.private_ws_run.connect_timeout_ms == 0
                        || runtime.private_ws_run.reconnect_delay_ms == 0
                        || runtime.private_ws_run.heartbeat_interval_ms == 0
                        || runtime.private_ws_run.stale_after_ms == 0)
                {
                    return Err(ConfigValidationError::InvalidPrivateWsRuntime {
                        exchange: exchange.clone(),
                    });
                }
            }
        }
        if self.risk.max_open_bundles == 0
            || self.risk.max_notional_per_symbol_usdt <= 0.0
            || self.risk.max_notional_per_exchange_usdt <= 0.0
            || self.risk.max_total_notional_usdt <= 0.0
            || self.risk.max_single_leg_exposure_usdt <= 0.0
            || self.risk.max_open_positions == 0
            || self.risk.max_loss_per_trade_usdt <= 0.0
            || self.risk.max_daily_loss_usdt <= 0.0
            || self.risk.max_drawdown_usdt <= 0.0
            || self.risk.max_spread_loss_bps <= 0.0
            || self.risk.max_hold_seconds <= 0
            || !(0.0..=1.0).contains(&self.risk.max_rest_failure_rate)
            || !(0.0..=1.0).contains(&self.risk.max_order_reject_rate)
            || !(0.0..=1.0).contains(&self.risk.max_cancel_failure_rate)
            || self.risk.max_private_stream_delay_ms <= 0
            || self.risk.max_balance_reconciliation_mismatch_usdt < 0.0
        {
            return Err(ConfigValidationError::InvalidRisk);
        }
        if self.execution.maker_order_ttl_ms == 0
            || self.execution.max_concurrent_maker_orders == 0
            || self.execution.maker_aggressive_after_cancels == 0
            || self.execution.maker_aggressive_step_ticks == 0
            || self.execution.maker_cooldown_after_cancels == 0
            || !self.execution.taker_ioc_slippage_limit_pct.is_finite()
            || self.execution.taker_ioc_slippage_limit_pct < 0.0
        {
            return Err(ConfigValidationError::InvalidExecution);
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RuntimeControlConfig {
    #[serde(default)]
    pub start_paused_new_entries: bool,
    #[serde(default)]
    pub start_close_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigValidationError {
    #[error("threshold values must be non-negative")]
    InvalidThreshold,
    #[error("sizing values must be positive and max >= min")]
    InvalidSizing,
    #[error("market values must be positive and public book profiles must be configured")]
    InvalidMarket,
    #[error("live trading requires a live runtime mode")]
    LiveTradingRequiresLiveRuntimeMode,
    #[error("live trading requires enable_live_trading=true")]
    LiveTradingNotEnabled,
    #[error("live trading requires explicit max_live_notional_per_trade")]
    MissingMaxLiveNotional,
    #[error("live trading requires explicit enabled_symbols")]
    MissingLiveEnabledSymbols,
    #[error("live trading requires explicit enabled_exchanges")]
    MissingLiveEnabledExchanges,
    #[error("live trading requires positive taker IOC slippage limit")]
    LiveTradingRequiresSlippageLimit,
    #[error("live trading requires execution.open_execution_style=dual_taker")]
    LiveTradingRequiresDualTakerOpen,
    #[error("live trading requires execution.close_execution_style=dual_taker")]
    LiveTradingRequiresDualTakerClose,
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
    #[error("live mode requires private websocket for exchange {exchange}")]
    LivePrivateWsDisabled { exchange: ExchangeId },
    #[error("private websocket runtime for exchange {exchange} must use positive durations")]
    InvalidPrivateWsRuntime { exchange: ExchangeId },
    #[error("risk values must be positive and internally consistent")]
    InvalidRisk,
    #[error("execution values must be positive and internally consistent")]
    InvalidExecution,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TradingMode {
    #[default]
    Paper,
    Live,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThresholdConfig {
    pub min_display_raw_spread: f64,
    #[serde(default = "default_min_open_raw_spread")]
    pub min_open_raw_spread: f64,
    pub min_open_maker_taker_net_edge: f64,
    #[serde(default = "default_max_open_raw_spread")]
    pub max_open_raw_spread: f64,
    #[serde(default)]
    pub route_overrides: Vec<RouteThresholdOverride>,
    pub lock_profit_dual_taker_pct: f64,
    pub strong_lock_profit_dual_taker_pct: f64,
    #[serde(default = "default_max_close_spread_pct")]
    pub max_close_spread_pct: f64,
}

impl Default for ThresholdConfig {
    fn default() -> Self {
        Self {
            min_display_raw_spread: 0.001,
            min_open_raw_spread: default_min_open_raw_spread(),
            min_open_maker_taker_net_edge: 0.005,
            max_open_raw_spread: default_max_open_raw_spread(),
            route_overrides: Vec::new(),
            lock_profit_dual_taker_pct: 0.003,
            strong_lock_profit_dual_taker_pct: 0.005,
            max_close_spread_pct: default_max_close_spread_pct(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteThresholdOverride {
    pub long_exchange: ExchangeId,
    pub short_exchange: ExchangeId,
    #[serde(default)]
    pub min_open_raw_spread: Option<f64>,
    #[serde(default)]
    pub min_open_maker_taker_net_edge: Option<f64>,
    #[serde(default)]
    pub max_open_raw_spread: Option<f64>,
}

impl ThresholdConfig {
    pub fn route_thresholds(
        &self,
        long_exchange: &ExchangeId,
        short_exchange: &ExchangeId,
    ) -> EffectiveRouteThresholds {
        let mut effective = EffectiveRouteThresholds {
            min_open_raw_spread: self.min_open_raw_spread,
            min_open_maker_taker_net_edge: self.min_open_maker_taker_net_edge,
            max_open_raw_spread: self.max_open_raw_spread,
        };
        if let Some(override_config) = self.route_overrides.iter().find(|override_config| {
            &override_config.long_exchange == long_exchange
                && &override_config.short_exchange == short_exchange
        }) {
            if let Some(value) = override_config.min_open_raw_spread {
                effective.min_open_raw_spread = value;
            }
            if let Some(value) = override_config.min_open_maker_taker_net_edge {
                effective.min_open_maker_taker_net_edge = value;
            }
            if let Some(value) = override_config.max_open_raw_spread {
                effective.max_open_raw_spread = value;
            }
        }
        effective
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EffectiveRouteThresholds {
    pub min_open_raw_spread: f64,
    pub min_open_maker_taker_net_edge: f64,
    pub max_open_raw_spread: f64,
}

fn default_min_open_raw_spread() -> f64 {
    0.005
}

fn default_max_open_raw_spread() -> f64 {
    0.10
}

fn default_max_close_spread_pct() -> f64 {
    0.001
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
        per_exchange.insert(
            ExchangeId::Bybit,
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.00055,
            },
        );
        per_exchange.insert(
            ExchangeId::Mexc,
            ExchangeFeeRates {
                maker: 0.0002,
                taker: 0.0006,
            },
        );
        per_exchange.insert(
            ExchangeId::Htx,
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
    #[serde(default = "default_max_concurrent_maker_orders")]
    pub max_concurrent_maker_orders: usize,
    #[serde(default)]
    pub maker_price_offset_ticks: u32,
    #[serde(default = "default_maker_aggressive_after_cancels")]
    pub maker_aggressive_after_cancels: u32,
    #[serde(default = "default_maker_aggressive_step_ticks")]
    pub maker_aggressive_step_ticks: u32,
    #[serde(default = "default_maker_aggressive_max_ticks")]
    pub maker_aggressive_max_ticks: u32,
    #[serde(default = "default_maker_cooldown_after_cancels")]
    pub maker_cooldown_after_cancels: u32,
    #[serde(default = "default_maker_cooldown_ms")]
    pub maker_cooldown_ms: u64,
    pub taker_ioc_slippage_limit_pct: f64,
    #[serde(default)]
    pub open_execution_style: OpenExecutionStyle,
    #[serde(default = "default_close_execution_style")]
    pub close_execution_style: OpenExecutionStyle,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            dry_run: true,
            max_hedge_retries: 1,
            maker_order_ttl_ms: 5_000,
            max_concurrent_maker_orders: default_max_concurrent_maker_orders(),
            maker_price_offset_ticks: 0,
            maker_aggressive_after_cancels: default_maker_aggressive_after_cancels(),
            maker_aggressive_step_ticks: default_maker_aggressive_step_ticks(),
            maker_aggressive_max_ticks: default_maker_aggressive_max_ticks(),
            maker_cooldown_after_cancels: default_maker_cooldown_after_cancels(),
            maker_cooldown_ms: default_maker_cooldown_ms(),
            taker_ioc_slippage_limit_pct: 0.003,
            open_execution_style: OpenExecutionStyle::MakerTaker,
            close_execution_style: default_close_execution_style(),
        }
    }
}

fn default_max_concurrent_maker_orders() -> usize {
    1
}

fn default_maker_aggressive_after_cancels() -> u32 {
    5
}

fn default_maker_aggressive_step_ticks() -> u32 {
    1
}

fn default_maker_aggressive_max_ticks() -> u32 {
    2
}

fn default_maker_cooldown_after_cancels() -> u32 {
    10
}

fn default_maker_cooldown_ms() -> u64 {
    120_000
}

fn default_close_execution_style() -> OpenExecutionStyle {
    OpenExecutionStyle::DualTaker
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OpenExecutionStyle {
    #[default]
    MakerTaker,
    DualTaker,
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
    pub operating_mode: ExchangeOperatingMode,
    #[serde(default)]
    pub position_mode: Option<String>,
    #[serde(default)]
    pub ws_batch_size: Option<usize>,
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
    #[serde(default = "default_true")]
    pub private_rest_enabled: bool,
    #[serde(default)]
    pub private_ws_enabled: bool,
    #[serde(default)]
    pub private_ws_run: PrivateWsRuntimeConfig,
    #[serde(default)]
    pub routes: ExchangeRouteConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeOperatingMode {
    #[default]
    Enabled,
    CloseOnly,
    Disabled,
}

impl ExchangeRuntimeConfig {
    pub fn is_disabled(&self) -> bool {
        self.enabled == Some(false) || self.operating_mode == ExchangeOperatingMode::Disabled
    }

    pub fn allows_new_entries(&self) -> bool {
        !self.is_disabled() && self.operating_mode == ExchangeOperatingMode::Enabled
    }

    pub fn route_status(&self) -> RouteStatus {
        if self.is_disabled() {
            RouteStatus::Offline
        } else if self.allows_new_entries() {
            RouteStatus::Healthy
        } else {
            RouteStatus::CloseOnly
        }
    }
}

impl ExchangeRuntimeSettings for ExchangeRuntimeConfig {
    fn is_disabled(&self) -> bool {
        self.is_disabled()
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
        self.private_ws_run.clone().into()
    }

    fn position_mode_text(&self) -> Option<&str> {
        self.position_mode.as_deref()
    }
}

impl ExchangeRegistryConfig for CrossExchangeArbitrageConfig {
    type Entry = ExchangeRuntimeConfig;

    fn exchange_runtime(&self) -> &HashMap<ExchangeId, Self::Entry> {
        &self.exchanges
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrivateWsRuntimeConfig {
    pub connect_timeout_ms: u64,
    pub reconnect_delay_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub subscribe_interval_ms: u64,
    #[serde(default = "default_private_ws_stale_after_ms")]
    pub stale_after_ms: u64,
}

impl Default for PrivateWsRuntimeConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            reconnect_delay_ms: 2_000,
            heartbeat_interval_ms: 15_000,
            subscribe_interval_ms: 50,
            stale_after_ms: default_private_ws_stale_after_ms(),
        }
    }
}

impl From<PrivateWsRuntimeConfig> for PrivateWsRunConfig {
    fn from(value: PrivateWsRuntimeConfig) -> Self {
        Self {
            connect_timeout_ms: value.connect_timeout_ms,
            reconnect_delay_ms: value.reconnect_delay_ms,
            heartbeat_interval_ms: value.heartbeat_interval_ms,
            subscribe_interval_ms: value.subscribe_interval_ms,
            stale_after_ms: value.stale_after_ms,
        }
    }
}

fn default_private_ws_stale_after_ms() -> u64 {
    45_000
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
    #[serde(alias = "max_notional_per_symbol")]
    pub max_notional_per_symbol_usdt: f64,
    #[serde(
        default = "default_max_notional_per_exchange_usdt",
        alias = "max_notional_per_exchange"
    )]
    pub max_notional_per_exchange_usdt: f64,
    #[serde(
        default = "default_max_total_notional_usdt",
        alias = "max_total_notional"
    )]
    pub max_total_notional_usdt: f64,
    #[serde(
        default = "default_max_single_leg_exposure_usdt",
        alias = "max_single_leg_exposure"
    )]
    pub max_single_leg_exposure_usdt: f64,
    #[serde(default = "default_max_open_bundles")]
    pub max_open_bundles: usize,
    #[serde(default = "default_max_open_positions")]
    pub max_open_positions: usize,
    #[serde(
        default = "default_max_loss_per_trade_usdt",
        alias = "max_loss_per_trade"
    )]
    pub max_loss_per_trade_usdt: f64,
    #[serde(default = "default_max_daily_loss_usdt", alias = "max_daily_loss")]
    pub max_daily_loss_usdt: f64,
    #[serde(default = "default_max_drawdown_usdt", alias = "max_drawdown")]
    pub max_drawdown_usdt: f64,
    #[serde(default = "default_max_spread_loss_bps")]
    pub max_spread_loss_bps: f64,
    #[serde(default = "default_max_hold_seconds")]
    pub max_hold_seconds: i64,
    #[serde(default = "default_max_rest_failure_rate")]
    pub max_rest_failure_rate: f64,
    #[serde(default = "default_max_order_reject_rate")]
    pub max_order_reject_rate: f64,
    #[serde(default = "default_max_cancel_failure_rate")]
    pub max_cancel_failure_rate: f64,
    #[serde(default = "default_max_private_stream_delay_ms")]
    pub max_private_stream_delay_ms: i64,
    #[serde(
        default = "default_max_balance_reconciliation_mismatch_usdt",
        alias = "max_balance_reconciliation_mismatch"
    )]
    pub max_balance_reconciliation_mismatch_usdt: f64,
    #[serde(default = "default_true")]
    pub symbol_whitelist_required_live: bool,
    #[serde(default)]
    pub block_on_external_account_exposure: bool,
    #[serde(default = "default_true")]
    pub orphan_exposure_blocks_new_entries: bool,
    #[serde(default = "default_one")]
    pub orphan_close_only_after_count: usize,
    #[serde(default = "default_true")]
    pub private_resync_blocks_new_entries: bool,
    #[serde(default = "default_true")]
    pub restored_position_blocks_new_entries: bool,
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
            max_notional_per_exchange_usdt: default_max_notional_per_exchange_usdt(),
            max_total_notional_usdt: default_max_total_notional_usdt(),
            max_single_leg_exposure_usdt: default_max_single_leg_exposure_usdt(),
            max_open_bundles: default_max_open_bundles(),
            max_open_positions: default_max_open_positions(),
            max_loss_per_trade_usdt: default_max_loss_per_trade_usdt(),
            max_daily_loss_usdt: default_max_daily_loss_usdt(),
            max_drawdown_usdt: default_max_drawdown_usdt(),
            max_spread_loss_bps: default_max_spread_loss_bps(),
            max_hold_seconds: default_max_hold_seconds(),
            max_rest_failure_rate: default_max_rest_failure_rate(),
            max_order_reject_rate: default_max_order_reject_rate(),
            max_cancel_failure_rate: default_max_cancel_failure_rate(),
            max_private_stream_delay_ms: default_max_private_stream_delay_ms(),
            max_balance_reconciliation_mismatch_usdt:
                default_max_balance_reconciliation_mismatch_usdt(),
            symbol_whitelist_required_live: true,
            block_on_external_account_exposure: false,
            orphan_exposure_blocks_new_entries: true,
            orphan_close_only_after_count: 1,
            private_resync_blocks_new_entries: true,
            restored_position_blocks_new_entries: true,
        }
    }
}

fn default_one() -> usize {
    1
}

fn default_max_open_bundles() -> usize {
    3
}

fn default_max_notional_per_exchange_usdt() -> f64 {
    5_000.0
}

fn default_max_total_notional_usdt() -> f64 {
    10_000.0
}

fn default_max_single_leg_exposure_usdt() -> f64 {
    2_000.0
}

fn default_max_open_positions() -> usize {
    10
}

fn default_max_loss_per_trade_usdt() -> f64 {
    100.0
}

fn default_max_daily_loss_usdt() -> f64 {
    500.0
}

fn default_max_drawdown_usdt() -> f64 {
    1_000.0
}

fn default_max_spread_loss_bps() -> f64 {
    25.0
}

fn default_max_hold_seconds() -> i64 {
    300
}

fn default_max_rest_failure_rate() -> f64 {
    0.05
}

fn default_max_order_reject_rate() -> f64 {
    0.02
}

fn default_max_cancel_failure_rate() -> f64 {
    0.02
}

fn default_max_private_stream_delay_ms() -> i64 {
    5_000
}

fn default_max_balance_reconciliation_mismatch_usdt() -> f64 {
    10.0
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
            enabled_exchanges: Vec::new(),
            symbols: Vec::new(),
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
    #[serde(default = "default_true")]
    pub persist_market_snapshots: bool,
    #[serde(default = "default_true")]
    pub stop_live_on_unavailable: bool,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            jsonl_dir: "logs/cross_exchange_arbitrage".to_string(),
            clickhouse_enabled: false,
            persist_market_snapshots: true,
            stop_live_on_unavailable: true,
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
            port: 8091,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetectionConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_detection_exchanges")]
    pub exchanges: Vec<ExchangeId>,
    #[serde(default = "default_detection_symbols")]
    pub symbols: Vec<String>,
    #[serde(default = "default_detection_depth_levels")]
    pub depth_levels: u16,
    #[serde(default = "default_detection_stale_book_ms")]
    pub stale_book_ms: i64,
    #[serde(default = "default_detection_estimated_notional_usdt")]
    pub estimated_notional_usdt: f64,
    #[serde(default)]
    pub estimated_slippage_bps: f64,
    #[serde(default)]
    pub safety_buffer_bps: f64,
    #[serde(default = "default_detection_min_net_spread_bps")]
    pub min_net_spread_bps: f64,
    #[serde(default = "default_true")]
    pub record_rejected: bool,
    #[serde(default)]
    pub symbol_mappings: HashMap<ExchangeId, HashMap<String, String>>,
    #[serde(default)]
    pub recorder: DetectionRecorderConfig,
    #[serde(default)]
    pub paper_trading: DetectionPaperTradingConfig,
}

impl Default for DetectionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            exchanges: Vec::new(),
            symbols: Vec::new(),
            depth_levels: default_detection_depth_levels(),
            stale_book_ms: default_detection_stale_book_ms(),
            estimated_notional_usdt: default_detection_estimated_notional_usdt(),
            estimated_slippage_bps: 0.0,
            safety_buffer_bps: 0.0,
            min_net_spread_bps: default_detection_min_net_spread_bps(),
            record_rejected: true,
            symbol_mappings: HashMap::new(),
            recorder: DetectionRecorderConfig::default(),
            paper_trading: DetectionPaperTradingConfig::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DetectionRecorderConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_detection_jsonl_path")]
    pub jsonl_path: String,
}

impl Default for DetectionRecorderConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            jsonl_path: default_detection_jsonl_path(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DetectionPaperTradingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub config_path: Option<String>,
    #[serde(default)]
    pub execution_mode: PaperExecutionMode,
    #[serde(default = "default_paper_maker_timeout_ms")]
    pub maker_timeout_ms: u64,
    #[serde(default = "default_paper_min_executable_depth_usdt")]
    pub min_executable_depth_usdt: f64,
    #[serde(default = "default_paper_execution_jsonl_path")]
    pub execution_jsonl_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum PaperExecutionMode {
    #[default]
    TakerTaker,
    MakerFirstThenTakerHedge,
    MakerMakerDisabledByDefault,
}

fn default_paper_maker_timeout_ms() -> u64 {
    2_000
}

fn default_paper_min_executable_depth_usdt() -> f64 {
    20.0
}

fn default_paper_execution_jsonl_path() -> String {
    "logs/cross_exchange_arbitrage/paper_executions.jsonl".to_string()
}

fn default_detection_exchanges() -> Vec<ExchangeId> {
    vec![ExchangeId::Binance, ExchangeId::Okx]
}

fn default_detection_symbols() -> Vec<String> {
    ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn default_detection_depth_levels() -> u16 {
    5
}

fn default_detection_stale_book_ms() -> i64 {
    2_000
}

fn default_detection_estimated_notional_usdt() -> f64 {
    100.0
}

fn default_detection_min_net_spread_bps() -> f64 {
    5.0
}

fn default_detection_jsonl_path() -> String {
    "logs/cross_exchange_arbitrage/opportunities.jsonl".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enable_live_test_universe(config: &mut CrossExchangeArbitrageConfig) {
        config.universe.enabled_exchanges =
            vec![ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate];
        config.universe.symbols = vec![CanonicalSymbol::new("BTC", "USDT")];
    }

    fn enable_private_ws_for_live_universe(config: &mut CrossExchangeArbitrageConfig) {
        for exchange in config.universe.enabled_exchanges.clone() {
            config
                .exchanges
                .entry(exchange)
                .or_default()
                .private_ws_enabled = true;
        }
    }

    fn enable_live_order_test_config(config: &mut CrossExchangeArbitrageConfig) {
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        config.trading_mode = TradingMode::Live;
        config.enable_live_trading = true;
        config.max_live_notional_per_trade = Some(10.0);
        config.enabled_symbols = vec![CanonicalSymbol::new("BTC", "USDT")];
        config.enabled_exchanges = vec![ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate];
        enable_live_test_universe(config);
        config.execution.dry_run = false;
        config.execution.open_execution_style = OpenExecutionStyle::DualTaker;
        config.execution.close_execution_style = OpenExecutionStyle::DualTaker;
        config.execution.taker_ioc_slippage_limit_pct = 0.003;
        for exchange in config.universe.enabled_exchanges.clone() {
            let runtime = config.exchanges.entry(exchange).or_default();
            runtime.private_rest_enabled = true;
            runtime.private_ws_enabled = true;
        }
    }

    #[test]
    fn config_validate_should_accept_default_simulation() {
        CrossExchangeArbitrageConfig::default().validate().unwrap();
    }

    #[test]
    fn config_validate_should_reject_invalid_top_of_book_capacity_ratio() {
        for ratio in [0.0, -0.1, 1.1, f64::NAN] {
            let mut config = CrossExchangeArbitrageConfig::default();
            config.market.top_of_book_capacity_ratio = ratio;

            assert_eq!(
                config.validate().unwrap_err(),
                ConfigValidationError::InvalidMarket
            );
        }
    }

    #[test]
    fn config_default_usdt_file_should_match_three_venue_400_symbol_plan() {
        let raw = std::fs::read_to_string("config/cross_exchange_arbitrage_usdt.yml").unwrap();
        let config = serde_yaml::from_str::<CrossExchangeArbitrageConfig>(&raw).unwrap();

        config.validate().unwrap();
        assert_eq!(
            config.universe.enabled_exchanges,
            vec![ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate]
        );
        assert_eq!(config.universe.symbols.len(), 400);
        assert_eq!(config.market.min_common_exchanges, 2);
        assert_eq!(config.sizing.target_notional_usdt, 5.5);
        assert_eq!(config.market.top_of_book_capacity_ratio, 0.8);
        assert_eq!(config.sizing.max_positions_per_exchange, 10);
        assert_eq!(config.execution.max_concurrent_maker_orders, 10);
        assert_eq!(config.risk.max_open_bundles, 10);
        assert_eq!(config.thresholds.lock_profit_dual_taker_pct, 0.0005);
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
        enable_live_test_universe(&mut config);
        enable_private_ws_for_live_universe(&mut config);
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
    fn config_validate_should_accept_live_close_only_exchange() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        enable_live_test_universe(&mut config);
        enable_private_ws_for_live_universe(&mut config);
        config
            .exchanges
            .entry(ExchangeId::Gate)
            .or_default()
            .operating_mode = ExchangeOperatingMode::CloseOnly;

        config.validate().expect("close-only venue stays attached");
        assert_eq!(
            config
                .exchanges
                .get(&ExchangeId::Gate)
                .unwrap()
                .route_status(),
            RouteStatus::CloseOnly
        );
    }

    #[test]
    fn config_validate_should_reject_live_universe_exchange_disabled_by_operating_mode() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        enable_live_test_universe(&mut config);
        enable_private_ws_for_live_universe(&mut config);
        config
            .exchanges
            .entry(ExchangeId::Gate)
            .or_default()
            .operating_mode = ExchangeOperatingMode::Disabled;

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
        enable_live_order_test_config(&mut config);
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
    fn cross_exchange_arbitrage_config_should_reject_live_non_dual_taker_open() {
        let mut config = CrossExchangeArbitrageConfig::default();
        enable_live_order_test_config(&mut config);
        config.execution.open_execution_style = OpenExecutionStyle::MakerTaker;

        assert_eq!(
            config.validate().unwrap_err(),
            ConfigValidationError::LiveTradingRequiresDualTakerOpen
        );
    }

    #[test]
    fn cross_exchange_arbitrage_config_should_reject_live_non_dual_taker_close() {
        let mut config = CrossExchangeArbitrageConfig::default();
        enable_live_order_test_config(&mut config);
        config.execution.close_execution_style = OpenExecutionStyle::MakerTaker;

        assert_eq!(
            config.validate().unwrap_err(),
            ConfigValidationError::LiveTradingRequiresDualTakerClose
        );
    }

    #[test]
    fn cross_exchange_arbitrage_config_should_reject_live_dual_taker_without_slippage_limit() {
        let mut config = CrossExchangeArbitrageConfig::default();
        enable_live_order_test_config(&mut config);
        config.execution.taker_ioc_slippage_limit_pct = 0.0;

        assert_eq!(
            config.validate().unwrap_err(),
            ConfigValidationError::LiveTradingRequiresSlippageLimit
        );
    }

    #[test]
    fn cross_exchange_arbitrage_config_should_accept_live_dual_taker_admission() {
        let mut config = CrossExchangeArbitrageConfig::default();
        enable_live_order_test_config(&mut config);

        config.validate().unwrap();
    }

    #[test]
    fn config_validate_should_reject_live_without_private_ws() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.mode = RuntimeMode::LiveSmall;
        config.strategy.mode = Some(RuntimeMode::LiveSmall);
        enable_live_test_universe(&mut config);
        config
            .exchanges
            .entry(ExchangeId::Binance)
            .or_default()
            .private_ws_enabled = false;

        assert_eq!(
            config.validate().unwrap_err(),
            ConfigValidationError::LivePrivateWsDisabled {
                exchange: ExchangeId::Binance
            }
        );
    }

    #[test]
    fn config_template_should_parse_and_validate() {
        let raw = include_str!("../../../config/cross_exchange_arbitrage.yml");
        let config: CrossExchangeArbitrageConfig =
            serde_yaml::from_str(raw).expect("template should parse");

        config.validate().expect("template should validate");
        assert_eq!(config.mode, RuntimeMode::Simulation);
        assert_eq!(
            config.universe.enabled_exchanges,
            vec![ExchangeId::Binance, ExchangeId::Okx]
        );
        assert_eq!(
            config.universe.symbols,
            vec![
                CanonicalSymbol::new("BTC", "USDT"),
                CanonicalSymbol::new("ETH", "USDT"),
                CanonicalSymbol::new("SOL", "USDT"),
            ]
        );
        assert_eq!(config.universe.mode, "static");
        assert_eq!(config.sizing.capital_fraction_of_smaller_equity, 0.10);
        assert_eq!(config.risk.max_open_bundles, 1);
        assert!(config.routing.auto_failover);
        assert!(!config.funding.enabled);
    }

    #[test]
    fn live_small_example_should_use_three_non_okx_exchanges_and_five_point_five_usdt_orders() {
        let raw =
            include_str!("../../../config/cross_exchange_arbitrage_usdt.live-small.example.yml");
        let config: CrossExchangeArbitrageConfig =
            serde_yaml::from_str(raw).expect("live-small example should parse");

        assert_eq!(
            config.validate().unwrap_err(),
            ConfigValidationError::LiveModeRequiresWhitelist
        );
        assert_eq!(config.mode, RuntimeMode::LiveSmall);
        assert!(config.universe.enabled_exchanges.is_empty());
        assert!(config.universe.symbols.is_empty());
        assert_eq!(config.sizing.target_notional_usdt, 5.5);
        assert_eq!(config.sizing.max_notional_usdt, 5.5);
        assert_eq!(config.risk.max_open_bundles, 50);
        assert!(config.execution.dry_run);
        assert_eq!(config.market.public_book_speed, "fastest");
        assert_eq!(config.market.public_book_trigger_profile, "fastest_l1");
        assert_eq!(
            config.market.public_book_validation_profile,
            "fastest_depth"
        );
        assert_eq!(config.market.public_book_depth, 5);
        assert!(config.market.allow_top_of_book_only);
        assert_eq!(config.market.top_of_book_capacity_ratio, 0.8);
        assert_eq!(
            config.execution.open_execution_style,
            OpenExecutionStyle::DualTaker
        );
        assert_eq!(
            config.execution.close_execution_style,
            OpenExecutionStyle::DualTaker
        );
        assert_eq!(config.execution.max_concurrent_maker_orders, 1);
        for exchange in [ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate] {
            let runtime = config.exchanges.get(&exchange).unwrap();
            assert!(
                runtime.is_disabled(),
                "{exchange} should be disabled by default"
            );
            assert!(!runtime.private_ws_enabled);
            assert!(!runtime.private_rest_enabled);
        }
        assert_eq!(
            config
                .exchanges
                .get(&ExchangeId::Bitget)
                .unwrap()
                .position_mode
                .as_deref(),
            Some("hedge")
        );
        assert_eq!(
            config
                .exchanges
                .get(&ExchangeId::Gate)
                .unwrap()
                .position_mode
                .as_deref(),
            Some("hedge")
        );
    }

    #[test]
    fn config_validate_should_reject_zero_maker_concurrency() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.execution.max_concurrent_maker_orders = 0;

        assert_eq!(
            config.validate().unwrap_err(),
            ConfigValidationError::InvalidExecution
        );
    }

    #[test]
    fn binance_bitget_live_small_should_exclude_gate_and_use_account_fees() {
        let raw = include_str!(
            "../../../config/cross_exchange_arbitrage_binance_bitget_50u.live-small.yml"
        );
        let config: CrossExchangeArbitrageConfig =
            serde_yaml::from_str(raw).expect("binance-bitget live-small should parse");

        config
            .validate()
            .expect("binance-bitget live-small should validate");
        assert_eq!(config.mode, RuntimeMode::LiveSmall);
        assert_eq!(
            config.universe.enabled_exchanges,
            vec![ExchangeId::Binance, ExchangeId::Bitget]
        );
        assert!(!config
            .universe
            .enabled_exchanges
            .contains(&ExchangeId::Gate));
        assert!(config.fees.use_account_fee_api);
        assert_eq!(config.sizing.target_notional_usdt, 5.5);
        assert_eq!(config.market.top_of_book_capacity_ratio, 0.8);
        assert_eq!(config.risk.max_open_bundles, 50);
        assert!(config.execution.dry_run);
        assert_eq!(
            config
                .exchanges
                .get(&ExchangeId::Gate)
                .unwrap()
                .operating_mode,
            ExchangeOperatingMode::Disabled
        );
    }

    #[test]
    fn three_venue_live_small_should_use_five_point_five_usdt_orders_and_zero_point_seven_pct_edge()
    {
        let raw = include_str!(
            "../../../config/cross_exchange_arbitrage_three_venues_50u.live-small.yml"
        );
        let config: CrossExchangeArbitrageConfig =
            serde_yaml::from_str(raw).expect("three-venue live-small should parse");

        config
            .validate()
            .expect("three-venue live-small should validate");
        assert_eq!(
            config.universe.enabled_exchanges,
            vec![ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate]
        );
        assert_eq!(config.thresholds.min_open_maker_taker_net_edge, 0.007);
        assert_eq!(config.sizing.target_notional_usdt, 5.5);
        assert_eq!(config.market.top_of_book_capacity_ratio, 0.8);
        assert_eq!(config.sizing.max_positions_per_exchange, 50);
        assert_eq!(config.risk.max_open_bundles, 50);
        assert!(config.fees.use_account_fee_api);
        assert!(config.execution.dry_run);
        assert_eq!(
            config
                .exchanges
                .get(&ExchangeId::Binance)
                .unwrap()
                .env_prefix
                .as_deref(),
            Some("BINANCE_0")
        );
    }

    #[test]
    fn hcr_live_small_config_should_use_dual_taker_debug_limits() {
        let raw = include_str!(
            "../../../config/cross_exchange_arbitrage_three_venues_hcr_10u_405symbols.live-small.yml"
        );
        let config: CrossExchangeArbitrageConfig =
            serde_yaml::from_str(raw).expect("hcr live-small config should parse");

        config
            .validate()
            .expect("hcr live-small config should validate");
        assert_eq!(
            config.execution.open_execution_style,
            OpenExecutionStyle::DualTaker
        );
        assert_eq!(
            config.execution.close_execution_style,
            OpenExecutionStyle::DualTaker
        );
        assert_eq!(config.market.public_book_trigger_profile, "fastest_l1");
        assert_eq!(
            config.market.public_book_validation_profile,
            "fastest_depth"
        );
        assert_eq!(config.thresholds.lock_profit_dual_taker_pct, 0.001);
        assert_eq!(config.execution.maker_order_ttl_ms, 20_000);
        assert_eq!(config.execution.max_concurrent_maker_orders, 5);
        assert_eq!(config.execution.maker_cooldown_after_cancels, 10);
        assert_eq!(config.execution.maker_cooldown_ms, 120_000);
        assert_eq!(config.sizing.min_notional_usdt, 5.5);
        assert_eq!(config.sizing.target_notional_usdt, 5.5);
        assert_eq!(config.sizing.max_notional_usdt, 5.5);
        assert_eq!(config.market.top_of_book_capacity_ratio, 0.8);
        assert_eq!(config.sizing.max_positions_per_exchange, 20);
        assert_eq!(config.risk.max_open_bundles, 20);
        assert!(config.controls.start_paused_new_entries);
        assert!(!config.risk.block_on_external_account_exposure);
        assert!(!config.risk.orphan_exposure_blocks_new_entries);
        assert_eq!(
            config
                .exchanges
                .get(&ExchangeId::Binance)
                .expect("binance config")
                .private_ws_run
                .stale_after_ms,
            45_000
        );
        assert!(config.risk.private_resync_blocks_new_entries);
        assert!(config.risk.restored_position_blocks_new_entries);
        assert_eq!(config.dashboard.port, 8091);
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
