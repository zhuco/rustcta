use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Timelike, Utc};
use rustcta_strategy_sdk::{OrderSide, OrderType, TimeInForce};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendConfig {
    pub name: String,
    pub account_id: String,
    #[serde(default)]
    pub dual_position_mode: bool,
    pub symbols: Vec<String>,
    pub max_positions: usize,
    pub max_daily_trades: usize,
    pub min_account_balance: f64,
    pub min_risk_reward_ratio: f64,
    pub min_signal_confidence: f64,
    pub max_holding_hours: usize,
    pub pyramid_enabled: bool,
    pub pyramid_levels: Vec<PyramidLevel>,
    pub risk_config: RiskConfig,
    pub indicator_config: IndicatorConfig,
    pub signal_config: SignalConfig,
    pub position_config: PositionConfig,
    pub stop_config: StopConfig,
    #[serde(default)]
    pub scoring: ScoringConfig,
    #[serde(default)]
    pub regime: RegimeConfig,
    #[serde(default)]
    pub time_curve: TimeCurveConfig,
    #[serde(default)]
    pub risk_budget: RiskBudgetConfig,
    #[serde(default)]
    pub symbol_inventory_limits: HashMap<String, f64>,
    #[serde(default)]
    pub symbol_inventory_value_limits: HashMap<String, f64>,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub monitoring: MonitoringConfig,
    #[serde(default = "default_entry_leverage")]
    pub entry_leverage: f64,
    #[serde(default)]
    pub entry_allocation: EntryAllocation,
    #[serde(default)]
    pub allocation_regimes: AllocationRegimesConfig,
    #[serde(default)]
    pub market_data: MarketDataConfig,
    #[serde(default)]
    pub entry_base_notional: Option<f64>,
    #[serde(default)]
    pub symbol_entry_notional: HashMap<String, f64>,
    #[serde(default)]
    pub max_trend_notional_multiplier: Option<f64>,
    #[serde(default)]
    pub market_entry_on_breakout: bool,
    #[serde(default = "default_entry_price_improve_bps")]
    pub entry_price_improve_bps: f64,
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_signal_cooldown_secs")]
    pub signal_cooldown_secs: u64,
    #[serde(default)]
    pub status_reporter: StatusReportConfig,
}

impl TrendConfig {
    pub fn validate_core(&self) -> Result<(), TrendConfigError> {
        if self.risk_config.max_risk_per_trade > 0.02 {
            return Err(TrendConfigError::MaxRiskPerTrade);
        }
        if self.risk_config.max_daily_loss > 0.05 {
            return Err(TrendConfigError::MaxDailyLoss);
        }
        if self.risk_config.max_leverage > 5.0 {
            return Err(TrendConfigError::MaxLeverage);
        }
        if self.risk_config.max_total_exposure > 5.0 {
            return Err(TrendConfigError::MaxTotalExposure);
        }
        if self.poll_interval_secs == 0 {
            return Err(TrendConfigError::PollInterval);
        }
        if self.signal_cooldown_secs == 0 {
            return Err(TrendConfigError::SignalCooldown);
        }
        if self.execution.max_retries == 0 {
            return Err(TrendConfigError::ExecutionRetries);
        }
        if self.market_data.cache_depth == 0
            || self.market_data.bootstrap_bars == 0
            || self.market_data.min_one_minute_bars == 0
            || self.market_data.min_one_hour_bars == 0
        {
            return Err(TrendConfigError::MarketDataCache);
        }
        if self.entry_leverage <= 0.0 || self.entry_leverage > self.risk_config.max_leverage {
            return Err(TrendConfigError::EntryLeverage);
        }
        if self
            .entry_base_notional
            .is_some_and(|notional| notional <= 0.0)
        {
            return Err(TrendConfigError::EntryBaseNotional);
        }
        if self
            .symbol_entry_notional
            .values()
            .any(|notional| *notional <= 0.0)
        {
            return Err(TrendConfigError::SymbolEntryNotional);
        }
        if self
            .max_trend_notional_multiplier
            .is_some_and(|multiplier| multiplier <= 0.0 || multiplier > 5.0)
        {
            return Err(TrendConfigError::TrendNotionalMultiplier);
        }
        if self.entry_price_improve_bps < 0.0 {
            return Err(TrendConfigError::EntryPriceImprove);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrendConfigError {
    MaxRiskPerTrade,
    MaxDailyLoss,
    MaxLeverage,
    MaxTotalExposure,
    PollInterval,
    SignalCooldown,
    ExecutionRetries,
    MarketDataCache,
    EntryLeverage,
    EntryBaseNotional,
    SymbolEntryNotional,
    TrendNotionalMultiplier,
    EntryPriceImprove,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskConfig {
    pub max_risk_per_trade: f64,
    pub max_daily_loss: f64,
    pub max_drawdown: f64,
    pub max_consecutive_losses: usize,
    pub max_total_exposure: f64,
    pub max_single_exposure: f64,
    pub max_correlated_exposure: f64,
    pub max_leverage: f64,
    pub max_slippage: f64,
    pub emergency_stop_loss: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndicatorConfig {
    pub primary_timeframe: String,
    pub secondary_timeframe: String,
    pub trigger_timeframe: String,
    pub fast_ema: usize,
    pub slow_ema: usize,
    pub atr_period: usize,
    pub adx_period: usize,
    pub adx_threshold: f64,
    pub rsi_period: usize,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,
    pub macd_fast: usize,
    pub macd_slow: usize,
    pub macd_signal: usize,
    pub bb_period: usize,
    pub bb_std: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalConfig {
    pub breakout_lookback: usize,
    pub breakout_confirmation_bars: usize,
    pub pullback_ratio: f64,
    pub pullback_min_trend_strength: f64,
    pub momentum_threshold: f64,
    pub momentum_lookback: usize,
    pub pattern_min_bars: usize,
    pub pattern_confidence: f64,
    pub volume_multiplier: f64,
    pub volume_lookback: usize,
    pub signal_expiry: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionConfig {
    pub base_risk_ratio: f64,
    pub kelly_fraction: f64,
    pub pyramid_enabled: bool,
    pub pyramid_levels: Vec<PyramidLevel>,
    pub trend_factor_enabled: bool,
    pub volatility_factor_enabled: bool,
    pub time_factor_enabled: bool,
    pub max_holding_hours: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PyramidLevel {
    pub trigger_profit: f64,
    pub size_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StopConfig {
    pub initial_stop_type: StopType,
    pub atr_multiplier: f64,
    pub fixed_stop_percent: f64,
    pub trailing_stop_enabled: bool,
    pub trailing_activation: f64,
    pub trailing_distance: f64,
    pub trailing_step: f64,
    pub time_stop_hours: Option<u64>,
    pub breakeven_enabled: bool,
    pub breakeven_trigger: f64,
    pub partial_targets: Vec<PartialTarget>,
    #[serde(default)]
    pub pnl_trailing: PnlTrailingConfig,
    #[serde(default = "default_lock_profit_pct")]
    pub lock_profit_pct: f64,
    #[serde(default = "default_lock_buffer_pct")]
    pub lock_buffer_pct: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopType {
    Fixed,
    #[serde(alias = "ATR")]
    ATR,
    Structure,
    Percentage,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartialTarget {
    pub target_r: f64,
    pub close_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PnlTrailingConfig {
    #[serde(default)]
    pub enable: bool,
    #[serde(default = "default_one_f64")]
    pub atr_multiple: f64,
    #[serde(default = "default_trailing_levels")]
    pub lock_levels: Vec<PnlLockLevel>,
}

impl Default for PnlTrailingConfig {
    fn default() -> Self {
        Self {
            enable: true,
            atr_multiple: default_one_f64(),
            lock_levels: default_trailing_levels(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PnlLockLevel {
    pub profit_atr: f64,
    pub stop_offset_atr: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoringConfig {
    pub min_score: f64,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self { min_score: 60.0 }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegimeConfig {
    pub enabled: bool,
}

impl Default for RegimeConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimeCurveConfig {
    pub enabled: bool,
}

impl Default for TimeCurveConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskBudgetConfig {
    pub enabled: bool,
}

impl Default for RiskBudgetConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_execution_prefer_maker")]
    pub prefer_maker: bool,
    #[serde(default = "default_execution_allow_taker_fallback")]
    pub allow_taker_fallback: bool,
    #[serde(default = "default_execution_maker_offset_bps")]
    pub maker_offset_bps: f64,
    #[serde(default = "default_execution_taker_slippage_bps")]
    pub taker_slippage_bps: f64,
    #[serde(default = "default_execution_max_retries")]
    pub max_retries: usize,
    #[serde(default = "default_execution_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default = "default_execution_order_timeout_secs")]
    pub order_timeout_secs: u64,
    #[serde(default)]
    pub iceberg_enabled: bool,
    #[serde(default = "default_execution_iceberg_visible_ratio")]
    pub iceberg_visible_ratio: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct SymbolPrecision {
    pub qty_precision: u32,
    pub price_precision: u32,
    pub step_size: f64,
    pub tick_size: f64,
    pub min_notional: Option<f64>,
}

impl Default for SymbolPrecision {
    fn default() -> Self {
        Self {
            qty_precision: 0,
            price_precision: 0,
            step_size: 0.0,
            tick_size: 0.0,
            min_notional: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendExecutionOrderPlan {
    pub symbol: String,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub price: Option<f64>,
    pub post_only: bool,
    pub time_in_force: Option<TimeInForce>,
    pub client_order_id: String,
    pub position_side: Option<String>,
}

pub fn build_trend_execution_order_plan(
    config: &ExecutionConfig,
    signal: &TradeSignal,
    size: f64,
    precision: &SymbolPrecision,
    prefer_maker: bool,
    client_order_id: &str,
    include_position_side: bool,
) -> TrendExecutionOrderPlan {
    let mut quantity = quantize_execution_amount(size, precision);
    if quantity <= 0.0 {
        quantity = execution_min_step(precision);
    }

    let price = compute_execution_price(config, signal, prefer_maker)
        .map(|price| quantize_execution_maker_price(price, precision, &signal.side));
    let order_type = if prefer_maker {
        OrderType::Limit
    } else {
        OrderType::Market
    };
    let position_side = include_position_side.then(|| match signal.side {
        OrderSide::Buy => "LONG".to_string(),
        OrderSide::Sell => "SHORT".to_string(),
    });

    TrendExecutionOrderPlan {
        symbol: signal.symbol.clone(),
        side: signal.side.clone(),
        order_type,
        quantity,
        price,
        post_only: prefer_maker,
        time_in_force: prefer_maker.then_some(TimeInForce::GoodTilCanceled),
        client_order_id: client_order_id.to_string(),
        position_side,
    }
}

pub fn compute_execution_price(
    config: &ExecutionConfig,
    signal: &TradeSignal,
    prefer_maker: bool,
) -> Option<f64> {
    if !prefer_maker {
        return None;
    }
    let offset = config.maker_offset_bps / 10_000.0;
    let adjusted_price = match signal.side {
        OrderSide::Buy => signal.entry_price * (1.0 - offset),
        OrderSide::Sell => signal.entry_price * (1.0 + offset),
    };
    Some(adjusted_price.max(1e-8))
}

pub fn compute_execution_slippage(expected: f64, actual: f64) -> f64 {
    if expected.abs() < f64::EPSILON {
        return 0.0;
    }
    ((actual - expected) / expected).abs()
}

pub fn build_trend_entry_client_id(symbol: &str, timestamp_micros: i64) -> String {
    let sanitized = symbol.replace('/', "").to_lowercase();
    let mut id = format!("trdent_{}_{}", sanitized, timestamp_micros);
    if id.len() > 30 {
        id.truncate(30);
    }
    id
}

pub fn quantize_execution_amount(amount: f64, precision: &SymbolPrecision) -> f64 {
    if precision.step_size > 0.0 {
        let quantized = (amount / precision.step_size).floor() * precision.step_size;
        return trim_with_precision(quantized, precision.qty_precision);
    }
    if precision.qty_precision == 0 {
        return amount.floor();
    }
    let scale = 10f64.powi(precision.qty_precision as i32);
    (amount * scale).floor() / scale
}

pub fn quantize_execution_price(price: f64, precision: &SymbolPrecision) -> f64 {
    if precision.tick_size > 0.0 {
        let quantized = (price / precision.tick_size).round() * precision.tick_size;
        return trim_with_precision(quantized, precision.price_precision);
    }
    if precision.price_precision == 0 {
        return price.round();
    }
    let scale = 10f64.powi(precision.price_precision as i32);
    (price * scale).round() / scale
}

pub fn quantize_execution_maker_price(
    price: f64,
    precision: &SymbolPrecision,
    side: &OrderSide,
) -> f64 {
    if precision.tick_size > 0.0 {
        let steps = price / precision.tick_size;
        let adjusted_steps = match side {
            OrderSide::Buy => steps.floor(),
            OrderSide::Sell => steps.ceil(),
        };
        let quantized = (adjusted_steps * precision.tick_size).max(precision.tick_size);
        return trim_with_precision(quantized, precision.price_precision);
    }
    if precision.price_precision == 0 {
        return match side {
            OrderSide::Buy => price.floor(),
            OrderSide::Sell => price.ceil(),
        };
    }
    let scale = 10f64.powi(precision.price_precision as i32);
    let scaled = price * scale;
    let adjusted_scaled = match side {
        OrderSide::Buy => scaled.floor(),
        OrderSide::Sell => scaled.ceil(),
    };
    adjusted_scaled / scale
}

pub fn format_execution_decimal(value: f64, precision: u32) -> String {
    if precision == 0 {
        format!("{value:.0}")
    } else {
        format!("{value:.*}", precision as usize)
    }
}

pub fn execution_decimals_from_step(step: f64) -> u32 {
    if step <= 0.0 {
        return 3;
    }
    let mut decimals = 0;
    let mut scaled = step;
    while (scaled - scaled.round()).abs() > 1e-9 && decimals < 12 {
        scaled *= 10.0;
        decimals += 1;
    }
    decimals
}

fn execution_min_step(precision: &SymbolPrecision) -> f64 {
    if precision.step_size > 0.0 {
        precision.step_size
    } else {
        1.0 / 10f64.powi(std::cmp::max(1, precision.qty_precision as i32))
    }
}

fn trim_with_precision(value: f64, precision: u32) -> f64 {
    if !value.is_finite() {
        return value;
    }
    if precision == 0 {
        return value.round();
    }
    let scale = 10f64.powi(precision as i32);
    (value * scale).round() / scale
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            prefer_maker: default_execution_prefer_maker(),
            allow_taker_fallback: default_execution_allow_taker_fallback(),
            maker_offset_bps: default_execution_maker_offset_bps(),
            taker_slippage_bps: default_execution_taker_slippage_bps(),
            max_retries: default_execution_max_retries(),
            retry_backoff_ms: default_execution_retry_backoff_ms(),
            order_timeout_secs: default_execution_order_timeout_secs(),
            iceberg_enabled: false,
            iceberg_visible_ratio: default_execution_iceberg_visible_ratio(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub enabled: bool,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntryAllocation {
    pub ma: f64,
    pub fib: f64,
    pub bb: f64,
}

impl EntryAllocation {
    pub fn normalized(&self) -> Self {
        let ma = self.ma.max(0.0);
        let fib = self.fib.max(0.0);
        let bb = self.bb.max(0.0);
        let sum = ma + fib + bb;
        if sum <= 0.0 {
            return Self::default();
        }
        Self {
            ma: ma / sum,
            fib: fib / sum,
            bb: bb / sum,
        }
    }
}

impl Default for EntryAllocation {
    fn default() -> Self {
        Self {
            ma: 0.5,
            fib: 0.3,
            bb: 0.2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AllocationRegimesConfig {
    pub trending: EntryAllocation,
    pub extreme: EntryAllocation,
    pub ranging: EntryAllocation,
}

impl Default for AllocationRegimesConfig {
    fn default() -> Self {
        Self {
            trending: EntryAllocation {
                ma: 0.5,
                fib: 0.3,
                bb: 0.2,
            },
            extreme: EntryAllocation {
                ma: 0.6,
                fib: 0.25,
                bb: 0.15,
            },
            ranging: EntryAllocation {
                ma: 0.3,
                fib: 0.3,
                bb: 0.4,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketDataConfig {
    #[serde(default = "default_cache_depth")]
    pub cache_depth: usize,
    #[serde(default = "default_bootstrap_bars")]
    pub bootstrap_bars: usize,
    #[serde(default = "default_max_data_lag_ms")]
    pub max_data_lag_ms: u64,
    #[serde(default = "default_min_one_minute_bars")]
    pub min_one_minute_bars: usize,
    #[serde(default = "default_min_five_minute_bars")]
    pub min_five_minute_bars: usize,
    #[serde(default = "default_min_fifteen_minute_bars")]
    pub min_fifteen_minute_bars: usize,
    #[serde(default = "default_min_one_hour_bars")]
    pub min_one_hour_bars: usize,
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        Self {
            cache_depth: default_cache_depth(),
            bootstrap_bars: default_bootstrap_bars(),
            max_data_lag_ms: default_max_data_lag_ms(),
            min_one_minute_bars: default_min_one_minute_bars(),
            min_five_minute_bars: default_min_five_minute_bars(),
            min_fifteen_minute_bars: default_min_fifteen_minute_bars(),
            min_one_hour_bars: default_min_one_hour_bars(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandleInterval {
    OneMinute,
    FiveMinutes,
    FifteenMinutes,
    OneHour,
}

impl CandleInterval {
    pub const ALL: [CandleInterval; 4] = [
        CandleInterval::OneMinute,
        CandleInterval::FiveMinutes,
        CandleInterval::FifteenMinutes,
        CandleInterval::OneHour,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            CandleInterval::OneMinute => "1m",
            CandleInterval::FiveMinutes => "5m",
            CandleInterval::FifteenMinutes => "15m",
            CandleInterval::OneHour => "1h",
        }
    }

    pub fn from_label(label: &str) -> Option<Self> {
        match label {
            "1m" => Some(CandleInterval::OneMinute),
            "5m" => Some(CandleInterval::FiveMinutes),
            "15m" => Some(CandleInterval::FifteenMinutes),
            "1h" => Some(CandleInterval::OneHour),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendCandle {
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trade_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandleEvent {
    pub symbol: String,
    pub interval: CandleInterval,
    pub candle: TrendCandle,
    pub is_final: bool,
    pub latency_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandleSnapshot {
    pub one_minute: Vec<TrendCandle>,
    pub five_minutes: Vec<TrendCandle>,
    pub fifteen_minutes: Vec<TrendCandle>,
    pub one_hour: Vec<TrendCandle>,
}

impl CandleSnapshot {
    pub fn len(&self, interval: CandleInterval) -> usize {
        match interval {
            CandleInterval::OneMinute => self.one_minute.len(),
            CandleInterval::FiveMinutes => self.five_minutes.len(),
            CandleInterval::FifteenMinutes => self.fifteen_minutes.len(),
            CandleInterval::OneHour => self.one_hour.len(),
        }
    }
}

pub fn requires_one_hour_history(config: &IndicatorConfig) -> bool {
    let uses_1h = |timeframe: &str| timeframe.eq_ignore_ascii_case("1h");
    uses_1h(&config.primary_timeframe)
        || uses_1h(&config.secondary_timeframe)
        || uses_1h(&config.trigger_timeframe)
}

pub fn has_minimum_history(
    market_data: &MarketDataConfig,
    indicator_config: &IndicatorConfig,
    snapshot: &CandleSnapshot,
) -> bool {
    snapshot.one_minute.len() >= market_data.min_one_minute_bars
        && snapshot.five_minutes.len() >= market_data.min_five_minute_bars
        && snapshot.fifteen_minutes.len() >= market_data.min_fifteen_minute_bars
        && (!requires_one_hour_history(indicator_config)
            || snapshot.one_hour.len() >= market_data.min_one_hour_bars)
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BollingerSnapshot {
    pub upper: f64,
    pub middle: f64,
    pub lower: f64,
    pub sigma: f64,
    pub band_percent: f64,
    pub z_score: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendIndicatorOutputs {
    pub timestamp: DateTime<Utc>,
    pub last_price: f64,
    pub bollinger_5m: BollingerSnapshot,
    pub bollinger_15m: BollingerSnapshot,
    pub rsi: f64,
    pub atr: f64,
    pub adx: f64,
    pub bbw: f64,
    pub bbw_percentile: f64,
    pub slope_metric: f64,
    pub choppiness: Option<f64>,
    pub recent_volume_quote: f64,
    pub volume_window_minutes: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndicatorComputationError {
    BollingerCalculationFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndicatorEventSkipReason {
    UnknownSymbol,
    NonFinalHigherTimeframe,
    MissingSnapshot,
    InsufficientHistory,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndicatorEventDecision {
    pub symbol: String,
    pub indicators: TrendIndicatorOutputs,
    pub trend_snapshot: TrendIndicatorSnapshot,
    pub publish_at: DateTime<Utc>,
}

pub fn handle_indicator_event_core(
    allowed_symbols: &[String],
    indicator_config: &IndicatorConfig,
    market_data: &MarketDataConfig,
    event: &CandleEvent,
    snapshot: Option<&CandleSnapshot>,
    now: DateTime<Utc>,
) -> Result<IndicatorEventDecision, IndicatorEventSkipReason> {
    if !allowed_symbols.iter().any(|symbol| symbol == &event.symbol) {
        return Err(IndicatorEventSkipReason::UnknownSymbol);
    }
    if !event.is_final && event.interval != CandleInterval::OneMinute {
        return Err(IndicatorEventSkipReason::NonFinalHigherTimeframe);
    }
    let Some(snapshot) = snapshot else {
        return Err(IndicatorEventSkipReason::MissingSnapshot);
    };
    if !has_minimum_history(market_data, indicator_config, snapshot) {
        return Err(IndicatorEventSkipReason::InsufficientHistory);
    }

    let indicators = compute_indicator_outputs(indicator_config, snapshot, now)
        .map_err(|_| IndicatorEventSkipReason::InsufficientHistory)?;
    let trend_snapshot = compute_trend_snapshot(
        indicator_config,
        market_data,
        snapshot,
        &indicators,
        event,
        now,
    );

    Ok(IndicatorEventDecision {
        symbol: event.symbol.clone(),
        indicators,
        trend_snapshot,
        publish_at: event.candle.close_time,
    })
}

pub fn compute_indicator_outputs(
    config: &IndicatorConfig,
    snapshot: &CandleSnapshot,
    now: DateTime<Utc>,
) -> Result<TrendIndicatorOutputs, IndicatorComputationError> {
    let closes_5m = candle_closes(&snapshot.five_minutes);
    let highs_5m = candle_highs(&snapshot.five_minutes);
    let lows_5m = candle_lows(&snapshot.five_minutes);
    let closes_15m = candle_closes(&snapshot.fifteen_minutes);
    let highs_15m = candle_highs(&snapshot.fifteen_minutes);
    let lows_15m = candle_lows(&snapshot.fifteen_minutes);
    let last_price = closes_5m.last().copied().unwrap_or_default();

    let boll_5m = compute_bollinger(&closes_5m, config.bb_period, config.bb_std, last_price)?;
    let boll_15m = compute_bollinger(&closes_15m, config.bb_period, config.bb_std, last_price)?;

    let atr = compute_atr(&highs_5m, &lows_5m, &closes_5m, config.atr_period).unwrap_or(0.0);
    let rsi = compute_rsi(&closes_5m, config.rsi_period).unwrap_or(50.0);
    let adx = compute_adx(&highs_15m, &lows_15m, &closes_15m, config.adx_period).unwrap_or(20.0);
    let bbw = if boll_15m.middle.abs() > f64::EPSILON {
        (boll_15m.upper - boll_15m.lower).abs() / boll_15m.middle.abs()
    } else {
        0.0
    };

    Ok(TrendIndicatorOutputs {
        timestamp: snapshot
            .fifteen_minutes
            .last()
            .map(|candle| candle.close_time)
            .unwrap_or(now),
        last_price,
        bollinger_5m: boll_5m,
        bollinger_15m: boll_15m,
        rsi,
        atr,
        adx,
        bbw,
        bbw_percentile: 0.5,
        slope_metric: compute_slope_metric(&closes_15m, config.fast_ema),
        choppiness: None,
        recent_volume_quote: snapshot
            .five_minutes
            .iter()
            .rev()
            .take(12)
            .map(|candle| candle.quote_volume)
            .sum(),
        volume_window_minutes: 60,
    })
}

pub fn compute_trend_snapshot(
    indicator_config: &IndicatorConfig,
    market_data: &MarketDataConfig,
    snapshot: &CandleSnapshot,
    indicators: &TrendIndicatorOutputs,
    event: &CandleEvent,
    now: DateTime<Utc>,
) -> TrendIndicatorSnapshot {
    let closes_1m = candle_closes(&snapshot.one_minute);
    let closes_5m = candle_closes(&snapshot.five_minutes);
    let closes_15m = candle_closes(&snapshot.fifteen_minutes);
    let highs_5m = candle_highs(&snapshot.five_minutes);
    let lows_5m = candle_lows(&snapshot.five_minutes);
    let highs_15m = candle_highs(&snapshot.fifteen_minutes);
    let lows_15m = candle_lows(&snapshot.fifteen_minutes);

    let ema_fast =
        compute_ema(&closes_1m, indicator_config.fast_ema).unwrap_or(indicators.last_price);
    let ema_slow =
        compute_ema(&closes_1m, indicator_config.slow_ema).unwrap_or(indicators.last_price);
    let ema_slope_1m = compute_slope_metric(&closes_1m, indicator_config.fast_ema);
    let ema_slope_5m = compute_slope_metric(&closes_5m, indicator_config.fast_ema);
    let atr_5m =
        compute_atr(&highs_5m, &lows_5m, &closes_5m, indicator_config.atr_period).unwrap_or(0.0);
    let atr_15m = compute_atr(
        &highs_15m,
        &lows_15m,
        &closes_15m,
        indicator_config.atr_period,
    )
    .unwrap_or(0.0);
    let data_quality = compute_data_quality(
        market_data,
        indicator_config,
        snapshot,
        event.latency_ms,
        now,
    );
    let regime = classify_regime(ema_slope_5m, atr_5m, indicators.bbw);

    TrendIndicatorSnapshot {
        ema_fast,
        ema_slow,
        ema_slope_1m,
        ema_slope_5m,
        atr_5m,
        atr_15m,
        boll_mid: indicators.bollinger_5m.middle,
        boll_bandwidth: indicators.bbw,
        fib_anchors: compute_fib_anchors(&snapshot.five_minutes),
        ofi: compute_ofi(&snapshot.one_minute),
        orderbook_imbalance: compute_orderbook_tilt(&snapshot.one_minute),
        data_quality_score: data_quality,
        regime,
        updated_at: event.candle.close_time,
    }
}

pub fn compute_bollinger(
    closes: &[f64],
    period: usize,
    std_dev: f64,
    fallback: f64,
) -> Result<BollingerSnapshot, IndicatorComputationError> {
    if closes.len() < 5 {
        return Ok(BollingerSnapshot {
            upper: fallback,
            middle: fallback,
            lower: fallback,
            sigma: 0.0,
            band_percent: 0.5,
            z_score: 0.0,
        });
    }

    let Some((upper, middle, lower)) = bollinger_bands(closes, period, std_dev) else {
        return Err(IndicatorComputationError::BollingerCalculationFailed);
    };
    let sigma = (upper - middle).abs() / std_dev.max(1e-6);
    let last = closes.last().copied().unwrap_or_default();
    let denom = (upper - lower).abs().max(1e-6);
    let band_percent = ((last - lower) / denom).clamp(0.0, 1.0);
    let z_score = if sigma.abs() < 1e-6 {
        0.0
    } else {
        (last - middle) / sigma
    };

    Ok(BollingerSnapshot {
        upper,
        middle,
        lower,
        sigma,
        band_percent,
        z_score,
    })
}

pub fn bollinger_bands(prices: &[f64], period: usize, std_dev: f64) -> Option<(f64, f64, f64)> {
    if period == 0 || prices.len() < period {
        return None;
    }
    let slice = &prices[prices.len() - period..];
    let middle = slice.iter().sum::<f64>() / period as f64;
    let variance = slice
        .iter()
        .map(|price| (price - middle).powi(2))
        .sum::<f64>()
        / period as f64;
    let std = variance.sqrt();
    Some((middle + std_dev * std, middle, middle - std_dev * std))
}

pub fn compute_ema(prices: &[f64], period: usize) -> Option<f64> {
    if prices.is_empty() || period == 0 {
        return None;
    }
    let multiplier = 2.0 / (period as f64 + 1.0);
    let mut ema = prices[0];
    for price in &prices[1..] {
        ema = (price - ema) * multiplier + ema;
    }
    Some(ema)
}

pub fn compute_rsi(prices: &[f64], period: usize) -> Option<f64> {
    if prices.len() < period + 1 || period == 0 {
        return None;
    }

    let mut gains = 0.0;
    let mut losses = 0.0;
    for idx in prices.len() - period..prices.len() {
        let change = prices[idx] - prices[idx - 1];
        if change > 0.0 {
            gains += change;
        } else {
            losses += change.abs();
        }
    }

    let avg_gain = gains / period as f64;
    let avg_loss = losses / period as f64;
    if avg_loss == 0.0 {
        return Some(100.0);
    }

    let rs = avg_gain / avg_loss;
    Some(100.0 - (100.0 / (1.0 + rs)))
}

pub fn compute_atr(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
    if period == 0
        || highs.len() < period + 1
        || lows.len() < period + 1
        || closes.len() < period + 1
    {
        return None;
    }

    let mut tr_values = Vec::with_capacity(highs.len().saturating_sub(1));
    for idx in 1..highs.len() {
        let high_low = highs[idx] - lows[idx];
        let high_close = (highs[idx] - closes[idx - 1]).abs();
        let low_close = (lows[idx] - closes[idx - 1]).abs();
        tr_values.push(high_low.max(high_close).max(low_close));
    }

    if tr_values.len() < period {
        return None;
    }
    Some(tr_values[tr_values.len() - period..].iter().sum::<f64>() / period as f64)
}

pub fn compute_adx(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
    if period == 0
        || highs.len() < period * 2
        || lows.len() < period * 2
        || closes.len() < period * 2
    {
        return None;
    }

    let mut plus_dm = Vec::with_capacity(highs.len().saturating_sub(1));
    let mut minus_dm = Vec::with_capacity(highs.len().saturating_sub(1));
    let mut tr_values = Vec::with_capacity(highs.len().saturating_sub(1));

    for idx in 1..highs.len() {
        let up_move = highs[idx] - highs[idx - 1];
        let down_move = lows[idx - 1] - lows[idx];
        plus_dm.push(if up_move > down_move && up_move > 0.0 {
            up_move
        } else {
            0.0
        });
        minus_dm.push(if down_move > up_move && down_move > 0.0 {
            down_move
        } else {
            0.0
        });

        let high_low = highs[idx] - lows[idx];
        let high_close = (highs[idx] - closes[idx - 1]).abs();
        let low_close = (lows[idx] - closes[idx - 1]).abs();
        tr_values.push(high_low.max(high_close).max(low_close));
    }

    if tr_values.len() < period * 2 - 1 {
        return None;
    }

    let smooth_plus_dm = compute_ema(&plus_dm, period)?;
    let smooth_minus_dm = compute_ema(&minus_dm, period)?;
    let smooth_tr = compute_ema(&tr_values, period)?;
    if smooth_tr == 0.0 {
        return None;
    }

    let plus_di = 100.0 * smooth_plus_dm / smooth_tr;
    let minus_di = 100.0 * smooth_minus_dm / smooth_tr;
    let di_sum = plus_di + minus_di;
    if di_sum == 0.0 {
        return None;
    }

    Some(100.0 * ((plus_di - minus_di).abs() / di_sum))
}

pub fn compute_slope_metric(closes: &[f64], fast_ema: usize) -> f64 {
    if closes.len() < 2 {
        return 0.0;
    }
    let latest = closes.last().copied().unwrap_or(0.0);
    let lookback = fast_ema.max(1).min(closes.len());
    let earliest = closes
        .iter()
        .rev()
        .nth(lookback - 1)
        .copied()
        .unwrap_or(latest);
    if earliest.abs() < f64::EPSILON {
        return 0.0;
    }
    ((latest - earliest) / earliest).clamp(-1.0, 1.0)
}

pub fn compute_fib_anchors(candles: &[TrendCandle]) -> TrendFibAnchors {
    let lookback = candles.len().min(120);
    if lookback == 0 {
        return TrendFibAnchors::default();
    }
    let slice = &candles[candles.len() - lookback..];
    let swing_high = slice
        .iter()
        .map(|candle| candle.high)
        .fold(f64::MIN, f64::max);
    let swing_low = slice
        .iter()
        .map(|candle| candle.low)
        .fold(f64::MAX, f64::min);
    let range = (swing_high - swing_low).max(1e-6);

    TrendFibAnchors {
        swing_high,
        swing_low,
        level_382: swing_high - range * 0.382,
        level_500: (swing_high + swing_low) * 0.5,
        level_618: swing_high - range * 0.618,
    }
}

pub fn compute_ofi(candles: &[TrendCandle]) -> f64 {
    candles
        .iter()
        .rev()
        .take(5)
        .map(|candle| {
            let range = (candle.high - candle.low).max(1e-6);
            ((candle.close - candle.open) / range) * candle.volume
        })
        .sum()
}

pub fn compute_orderbook_tilt(candles: &[TrendCandle]) -> f64 {
    if candles.len() < 3 {
        return 0.0;
    }
    let up_moves = candles
        .iter()
        .rev()
        .take(10)
        .filter(|candle| candle.close >= candle.open)
        .count() as f64;
    let down_moves = candles
        .iter()
        .rev()
        .take(10)
        .filter(|candle| candle.close < candle.open)
        .count() as f64;
    if up_moves + down_moves == 0.0 {
        return 0.0;
    }
    (up_moves - down_moves) / (up_moves + down_moves)
}

pub fn compute_data_quality(
    market_data: &MarketDataConfig,
    indicator_config: &IndicatorConfig,
    snapshot: &CandleSnapshot,
    event_latency_ms: i64,
    now: DateTime<Utc>,
) -> f64 {
    let mut score: f64 = 100.0;
    if event_latency_ms > market_data.max_data_lag_ms as i64 {
        score -= 15.0;
    }
    if snapshot.one_minute.len() < market_data.min_one_minute_bars {
        score -= 25.0;
    }
    if snapshot.five_minutes.len() < market_data.min_five_minute_bars {
        score -= 15.0;
    }
    if snapshot.fifteen_minutes.len() < market_data.min_fifteen_minute_bars {
        score -= 10.0;
    }
    if requires_one_hour_history(indicator_config)
        && snapshot.one_hour.len() < market_data.min_one_hour_bars
    {
        score -= 15.0;
    }
    if let Some(last) = snapshot.one_minute.last() {
        if now - last.close_time > chrono::Duration::seconds(5) {
            score -= 20.0;
        }
    }
    score.clamp(0.0, 100.0)
}

pub fn classify_regime(slope: f64, atr: f64, bbw: f64) -> MarketRegime {
    let slope_abs = slope.abs();
    let normalized_atr = atr.max(1e-6);

    if slope_abs > 0.015 && bbw < 0.08 {
        MarketRegime::Trending
    } else if normalized_atr > 0.02 {
        MarketRegime::Extreme
    } else {
        MarketRegime::Ranging
    }
}

fn candle_closes(candles: &[TrendCandle]) -> Vec<f64> {
    candles.iter().map(|candle| candle.close).collect()
}

fn candle_highs(candles: &[TrendCandle]) -> Vec<f64> {
    candles.iter().map(|candle| candle.high).collect()
}

fn candle_lows(candles: &[TrendCandle]) -> Vec<f64> {
    candles.iter().map(|candle| candle.low).collect()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandleBook {
    capacity: usize,
    store: HashMap<String, SymbolCandleBuffers>,
}

impl CandleBook {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            store: HashMap::new(),
        }
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn apply_event(&mut self, event: CandleEvent) -> bool {
        let entry = self.store.entry(event.symbol).or_default();
        let buffer = entry.buffer_mut(event.interval, self.capacity);

        if let Some(last) = buffer.back_mut() {
            if last.open_time == event.candle.open_time {
                *last = event.candle;
                return true;
            }
            if last.open_time < event.candle.open_time {
                buffer.push_back(event.candle);
                if buffer.len() > self.capacity {
                    buffer.pop_front();
                }
                return true;
            }
            return false;
        }

        buffer.push_back(event.candle);
        true
    }

    pub fn seed(
        &mut self,
        symbol: impl Into<String>,
        interval: CandleInterval,
        candles: Vec<TrendCandle>,
    ) -> Option<CandleEvent> {
        if candles.is_empty() {
            return None;
        }

        let symbol = symbol.into();
        let mut ordered = candles;
        ordered.sort_by_key(|candle| candle.open_time);
        if ordered.len() > self.capacity {
            let drain = ordered.len() - self.capacity;
            ordered.drain(0..drain);
        }

        let last = ordered.last().cloned();
        let entry = self.store.entry(symbol.clone()).or_default();
        let buffer = entry.buffer_mut(interval, self.capacity);
        buffer.clear();
        for candle in ordered {
            buffer.push_back(candle);
        }

        last.map(|candle| CandleEvent {
            symbol,
            interval,
            candle,
            is_final: true,
            latency_ms: 0,
        })
    }

    pub fn snapshot(&self, symbol: &str) -> Option<CandleSnapshot> {
        let entry = self.store.get(symbol)?;
        Some(CandleSnapshot {
            one_minute: entry.buffer_clone(CandleInterval::OneMinute),
            five_minutes: entry.buffer_clone(CandleInterval::FiveMinutes),
            fifteen_minutes: entry.buffer_clone(CandleInterval::FifteenMinutes),
            one_hour: entry.buffer_clone(CandleInterval::OneHour),
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct SymbolCandleBuffers {
    buffers: HashMap<CandleInterval, VecDeque<TrendCandle>>,
}

impl SymbolCandleBuffers {
    fn buffer_mut(
        &mut self,
        interval: CandleInterval,
        capacity: usize,
    ) -> &mut VecDeque<TrendCandle> {
        self.buffers
            .entry(interval)
            .or_insert_with(|| VecDeque::with_capacity(capacity))
    }

    fn buffer_clone(&self, interval: CandleInterval) -> Vec<TrendCandle> {
        self.buffers
            .get(&interval)
            .map(|buffer| buffer.iter().cloned().collect())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct StatusReportConfig {
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketRegime {
    Trending,
    Extreme,
    Ranging,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrendDirection {
    StrongBullish,
    Bullish,
    Neutral,
    Bearish,
    StrongBearish,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendSignal {
    pub direction: TrendDirection,
    pub strength: f64,
    pub confidence: f64,
    pub timeframe_aligned: bool,
    pub volume_confirmation: bool,
    pub data_quality_score: f64,
}

impl TrendSignal {
    pub fn is_tradeable(&self) -> bool {
        self.data_quality_score >= 50.0
            && !matches!(self.direction, TrendDirection::Neutral)
            && self.confidence > 60.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndicatorSnapshot {
    pub last_price: f64,
    pub atr: f64,
    pub bollinger_middle: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendIndicatorSnapshot {
    pub ema_fast: f64,
    pub ema_slow: f64,
    #[serde(default)]
    pub ema_slope_1m: f64,
    #[serde(default)]
    pub ema_slope_5m: f64,
    #[serde(default)]
    pub atr_5m: f64,
    #[serde(default)]
    pub atr_15m: f64,
    #[serde(default)]
    pub boll_mid: f64,
    #[serde(default)]
    pub boll_bandwidth: f64,
    pub fib_anchors: TrendFibAnchors,
    #[serde(default)]
    pub ofi: f64,
    #[serde(default)]
    pub orderbook_imbalance: f64,
    #[serde(default = "default_data_quality_score")]
    pub data_quality_score: f64,
    pub regime: MarketRegime,
    #[serde(default = "default_updated_at")]
    pub updated_at: DateTime<Utc>,
}

impl Default for TrendIndicatorSnapshot {
    fn default() -> Self {
        Self {
            ema_fast: 0.0,
            ema_slow: 0.0,
            ema_slope_1m: 0.0,
            ema_slope_5m: 0.0,
            atr_5m: 0.0,
            atr_15m: 0.0,
            boll_mid: 0.0,
            boll_bandwidth: 0.0,
            fib_anchors: TrendFibAnchors::default(),
            ofi: 0.0,
            orderbook_imbalance: 0.0,
            data_quality_score: default_data_quality_score(),
            regime: MarketRegime::Ranging,
            updated_at: default_updated_at(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendFibAnchors {
    pub swing_high: f64,
    pub swing_low: f64,
    pub level_382: f64,
    pub level_500: f64,
    pub level_618: f64,
}

impl Default for TrendFibAnchors {
    fn default() -> Self {
        Self {
            swing_high: 0.0,
            swing_low: 0.0,
            level_382: 0.0,
            level_500: 0.0,
            level_618: 0.0,
        }
    }
}

fn default_data_quality_score() -> f64 {
    100.0
}

fn default_updated_at() -> DateTime<Utc> {
    Utc::now()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SharedTrendData {
    pub indicators: IndicatorSnapshot,
    pub trend_snapshot: Option<TrendIndicatorSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeSignal {
    pub symbol: String,
    pub signal_type: SignalType,
    pub side: OrderSide,
    pub entry_price: f64,
    pub stop_loss: f64,
    pub take_profits: Vec<TakeProfit>,
    pub suggested_size: f64,
    pub risk_reward_ratio: f64,
    pub confidence: f64,
    pub timeframe_aligned: bool,
    pub has_structure_support: bool,
    pub expire_time: DateTime<Utc>,
    pub metadata: SignalMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalType {
    TrendBreakout,
    Pullback,
    MomentumSurge,
    PatternBreakout,
    Reversal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TakeProfit {
    pub price: f64,
    pub ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalMetadata {
    pub trend_strength: f64,
    pub volume_confirmed: bool,
    pub key_level_nearby: bool,
    pub pattern_name: Option<String>,
    pub generated_at: DateTime<Utc>,
}

pub fn generate_trade_signal(
    config: &SignalConfig,
    symbol: &str,
    trend_signal: &TrendSignal,
    indicators: &IndicatorSnapshot,
    now: DateTime<Utc>,
) -> Option<TradeSignal> {
    if !trend_signal.is_tradeable() {
        return None;
    }

    let current_price = indicators.last_price.max(1e-6);
    let (signal_type, side, entry_price, risk_percent, confidence, pattern_name) =
        match trend_signal.direction {
            TrendDirection::StrongBullish => (
                SignalType::TrendBreakout,
                OrderSide::Buy,
                find_breakout_entry(current_price, OrderSide::Buy),
                (indicators.atr / current_price).clamp(0.003, 0.04),
                trend_signal.confidence,
                None,
            ),
            TrendDirection::Bullish => (
                SignalType::Pullback,
                OrderSide::Buy,
                find_pullback_entry(current_price, OrderSide::Buy, config.pullback_ratio),
                (indicators.atr / current_price).clamp(0.002, 0.035),
                trend_signal.confidence * 0.9,
                Some("pullback".to_string()),
            ),
            TrendDirection::StrongBearish => (
                SignalType::TrendBreakout,
                OrderSide::Sell,
                find_breakout_entry(current_price, OrderSide::Sell),
                (indicators.atr / current_price).clamp(0.003, 0.04),
                trend_signal.confidence,
                None,
            ),
            TrendDirection::Bearish => (
                SignalType::Pullback,
                OrderSide::Sell,
                find_pullback_entry(current_price, OrderSide::Sell, config.pullback_ratio),
                (indicators.atr / current_price).clamp(0.002, 0.035),
                trend_signal.confidence * 0.9,
                Some("pullback".to_string()),
            ),
            TrendDirection::Neutral => return None,
        };

    let stop_loss = calculate_stop_loss(entry_price, side.clone(), risk_percent);
    let take_profits = calculate_take_profits(entry_price, stop_loss, side.clone());
    let risk = (entry_price - stop_loss).abs();
    let reward = take_profits
        .iter()
        .map(|tp| (tp.price - entry_price).abs())
        .fold(0.0, f64::max);
    let risk_reward_ratio = if risk > 0.0 { reward / risk } else { 0.0 };

    let signal = TradeSignal {
        symbol: symbol.to_string(),
        signal_type,
        side,
        entry_price,
        stop_loss,
        take_profits,
        suggested_size: (12.0 / entry_price).max(0.0),
        risk_reward_ratio,
        confidence,
        timeframe_aligned: trend_signal.timeframe_aligned,
        has_structure_support: true,
        expire_time: now + chrono::Duration::seconds(config.signal_expiry as i64),
        metadata: SignalMetadata {
            trend_strength: trend_signal.strength,
            volume_confirmed: trend_signal.volume_confirmation,
            key_level_nearby: true,
            pattern_name,
            generated_at: now,
        },
    };

    validate_signal_quality(&signal).then_some(signal)
}

pub fn find_breakout_entry(current_price: f64, side: OrderSide) -> f64 {
    match side {
        OrderSide::Buy => current_price * 1.001,
        OrderSide::Sell => current_price * 0.999,
    }
}

pub fn find_pullback_entry(current_price: f64, side: OrderSide, pullback_ratio: f64) -> f64 {
    match side {
        OrderSide::Buy => current_price * (1.0 - pullback_ratio * 0.01),
        OrderSide::Sell => current_price * (1.0 + pullback_ratio * 0.01),
    }
}

pub fn calculate_stop_loss(entry: f64, side: OrderSide, risk_percent: f64) -> f64 {
    match side {
        OrderSide::Buy => entry * (1.0 - risk_percent),
        OrderSide::Sell => entry * (1.0 + risk_percent),
    }
}

pub fn calculate_take_profits(entry: f64, stop: f64, side: OrderSide) -> Vec<TakeProfit> {
    let risk = (entry - stop).abs();
    match side {
        OrderSide::Buy => vec![
            TakeProfit {
                price: entry + risk * 1.5,
                ratio: 0.3,
            },
            TakeProfit {
                price: entry + risk * 2.5,
                ratio: 0.3,
            },
            TakeProfit {
                price: entry + risk * 4.0,
                ratio: 0.2,
            },
        ],
        OrderSide::Sell => vec![
            TakeProfit {
                price: entry - risk * 1.5,
                ratio: 0.3,
            },
            TakeProfit {
                price: entry - risk * 2.5,
                ratio: 0.3,
            },
            TakeProfit {
                price: entry - risk * 4.0,
                ratio: 0.2,
            },
        ],
    }
}

pub fn validate_signal_quality(signal: &TradeSignal) -> bool {
    if signal.risk_reward_ratio < 2.0 || signal.confidence < 60.0 {
        return false;
    }
    let stop_distance = ((signal.entry_price - signal.stop_loss) / signal.entry_price).abs();
    stop_distance <= 0.05
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntryMode {
    MovingAverage,
    Fibonacci,
    Bollinger,
}

impl EntryMode {
    pub const ALL: [EntryMode; 3] = [
        EntryMode::MovingAverage,
        EntryMode::Fibonacci,
        EntryMode::Bollinger,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            EntryMode::MovingAverage => "ma",
            EntryMode::Fibonacci => "fib",
            EntryMode::Bollinger => "bb",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntryAllocationSlice {
    pub mode: EntryMode,
    pub weight: f64,
    pub notional: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AllocationResult {
    pub regime: MarketRegime,
    pub total_notional: f64,
    pub per_mode: Vec<EntryAllocationSlice>,
    pub rejected: Vec<(EntryMode, String)>,
    pub adjustments: Vec<String>,
}

impl AllocationResult {
    pub fn is_empty(&self) -> bool {
        self.per_mode.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntryOrderPlan {
    pub mode: EntryMode,
    pub signal_type: SignalType,
    pub price: f64,
    pub side: OrderSide,
    pub notional: f64,
    pub weight: f64,
    pub rationale: String,
}

pub fn planned_notional(equity: f64, leverage: f64, fixed_notional: Option<f64>) -> f64 {
    if let Some(value) = fixed_notional {
        value.max(0.0)
    } else {
        equity.max(0.0) * leverage.max(0.1)
    }
}

pub fn allocate_entry_notional(
    base: &EntryAllocation,
    regimes: &AllocationRegimesConfig,
    regime: MarketRegime,
    total_notional: f64,
    min_notional: f64,
) -> AllocationResult {
    let mut result = AllocationResult {
        regime,
        total_notional,
        per_mode: Vec::new(),
        rejected: Vec::new(),
        adjustments: Vec::new(),
    };
    if total_notional <= 0.0 {
        return result;
    }

    let weights = match regime {
        MarketRegime::Trending => &regimes.trending,
        MarketRegime::Extreme => &regimes.extreme,
        MarketRegime::Ranging => &regimes.ranging,
    }
    .normalized();
    let fallback = base.normalized();
    let weights = if weights.ma + weights.fib + weights.bb > 0.0 {
        weights
    } else {
        fallback
    };

    if total_notional < min_notional * EntryMode::ALL.len() as f64 {
        let top_mode = EntryMode::ALL
            .iter()
            .copied()
            .max_by(|a, b| {
                let wa = entry_mode_weight(*a, &weights);
                let wb = entry_mode_weight(*b, &weights);
                wa.partial_cmp(&wb).unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap_or(EntryMode::MovingAverage);
        result.per_mode.push(EntryAllocationSlice {
            mode: top_mode,
            weight: 1.0,
            notional: total_notional,
        });
        return result;
    }

    let mut remaining: Vec<(EntryMode, f64)> = EntryMode::ALL
        .iter()
        .copied()
        .map(|mode| (mode, entry_mode_weight(mode, &weights).max(0.0)))
        .collect();
    let mut remaining_total = total_notional;
    let mut assigned = Vec::new();
    loop {
        if remaining.is_empty() {
            break;
        }
        let weight_sum: f64 = remaining.iter().map(|(_, weight)| *weight).sum();
        if weight_sum <= 0.0 {
            let equal = remaining_total / remaining.len() as f64;
            for (mode, _) in remaining.drain(..) {
                assigned.push((mode, equal.max(min_notional)));
            }
            break;
        }

        let mut next = Vec::new();
        let mut updated = false;
        for (mode, weight) in remaining.into_iter() {
            let share = remaining_total * weight / weight_sum;
            if share < min_notional {
                assigned.push((mode, min_notional));
                remaining_total -= min_notional;
                updated = true;
            } else {
                next.push((mode, weight));
            }
        }
        if !updated {
            for (mode, weight) in next.into_iter() {
                let share = remaining_total * weight / weight_sum;
                assigned.push((mode, share.max(min_notional)));
            }
            break;
        }
        if remaining_total <= 0.0 {
            break;
        }
        remaining = next;
    }

    let sum_assigned: f64 = assigned.iter().map(|(_, value)| *value).sum();
    if sum_assigned > 0.0 && (sum_assigned - total_notional).abs() > 1e-6 {
        let scale = (total_notional / sum_assigned).min(1.0);
        for (_, value) in assigned.iter_mut() {
            *value *= scale;
        }
        if scale < 1.0 {
            result
                .adjustments
                .push("scaled allocations to match budget".to_string());
        }
    }

    for (mode, notional) in assigned {
        result.per_mode.push(EntryAllocationSlice {
            mode,
            weight: 1.0 / EntryMode::ALL.len() as f64,
            notional: notional.max(min_notional),
        });
    }
    result
}

fn entry_mode_weight(mode: EntryMode, weights: &EntryAllocation) -> f64 {
    match mode {
        EntryMode::MovingAverage => weights.ma,
        EntryMode::Fibonacci => weights.fib,
        EntryMode::Bollinger => weights.bb,
    }
}

pub fn build_entry_orders(
    signal: &TradeSignal,
    shared: &SharedTrendData,
    allocation: &AllocationResult,
    base_improve_bps: f64,
) -> Vec<EntryOrderPlan> {
    if allocation.per_mode.is_empty() {
        return Vec::new();
    }
    allocation
        .per_mode
        .iter()
        .filter_map(|slice| {
            let price = match slice.mode {
                EntryMode::MovingAverage => price_from_moving_average(signal, shared),
                EntryMode::Fibonacci => price_from_fib(signal, shared),
                EntryMode::Bollinger => price_from_bollinger(signal, shared, allocation.regime),
            }?;
            let price = apply_entry_price_improve(price, signal.side.clone(), base_improve_bps);
            (price > 0.0).then(|| EntryOrderPlan {
                mode: slice.mode,
                signal_type: signal.signal_type,
                price,
                side: signal.side.clone(),
                notional: slice.notional,
                weight: slice.weight,
                rationale: format!(
                    "{} allocation {:.2}%",
                    slice.mode.label(),
                    slice.weight * 100.0
                ),
            })
        })
        .collect()
}

pub fn price_from_moving_average(signal: &TradeSignal, shared: &SharedTrendData) -> Option<f64> {
    let snapshot = match &shared.trend_snapshot {
        Some(snapshot) => snapshot,
        None => return Some(signal.entry_price),
    };
    let base = (snapshot.ema_fast + snapshot.ema_slow) / 2.0;
    let offset = 0.0015;
    Some(match &signal.side {
        OrderSide::Buy => base * (1.0 - offset),
        OrderSide::Sell => base * (1.0 + offset),
    })
}

pub fn price_from_fib(signal: &TradeSignal, shared: &SharedTrendData) -> Option<f64> {
    let snapshot = match &shared.trend_snapshot {
        Some(snapshot) => snapshot,
        None => return Some(signal.entry_price),
    };
    let anchors = &snapshot.fib_anchors;
    Some(match &signal.side {
        OrderSide::Buy => anchors.level_618,
        OrderSide::Sell => {
            let range = (anchors.swing_high - anchors.swing_low).abs();
            anchors.swing_low + range * 0.382
        }
    })
}

pub fn price_from_bollinger(
    signal: &TradeSignal,
    shared: &SharedTrendData,
    regime: MarketRegime,
) -> Option<f64> {
    let bias = match regime {
        MarketRegime::Trending => 0.0015,
        MarketRegime::Extreme => 0.0025,
        MarketRegime::Ranging => 0.0008,
    };
    Some(match &signal.side {
        OrderSide::Buy => shared.indicators.bollinger_middle * (1.0 - bias),
        OrderSide::Sell => shared.indicators.bollinger_middle * (1.0 + bias),
    })
}

pub fn apply_entry_price_improve(price: f64, side: OrderSide, base_improve_bps: f64) -> f64 {
    if base_improve_bps <= 0.0 || price <= 0.0 {
        return price;
    }
    let delta = base_improve_bps / 10_000.0;
    match side {
        OrderSide::Buy => price * (1.0 - delta),
        OrderSide::Sell => price * (1.0 + delta),
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AccountCapital {
    pub total: f64,
    pub available: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrendPosition {
    pub symbol: String,
    pub side: OrderSide,
    pub entry_price: f64,
    pub average_price: f64,
    pub current_price: f64,
    pub size: f64,
    pub current_size: f64,
    pub initial_size: f64,
    pub pyramid_entries: Vec<PyramidEntry>,
    pub pyramid_count: usize,
    pub stop_loss: f64,
    pub take_profits: Vec<TakeProfitLevel>,
    pub entry_time: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub peak_profit: f64,
    pub risk_amount: f64,
}

impl TrendPosition {
    pub fn is_stop_hit(&self) -> bool {
        match self.side {
            OrderSide::Buy => self.current_price <= self.stop_loss,
            OrderSide::Sell => self.current_price >= self.stop_loss,
        }
    }

    pub fn holding_hours_at(&self, now: DateTime<Utc>) -> f64 {
        now.signed_duration_since(self.entry_time).num_seconds() as f64 / 3600.0
    }

    pub fn profit_in_r(&self) -> f64 {
        let risk = (self.entry_price - self.stop_loss).abs();
        if risk > 0.0 {
            self.unrealized_pnl / (risk * self.size)
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PyramidEntry {
    pub level: usize,
    pub price: f64,
    pub size: f64,
    pub time: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TakeProfitLevel {
    pub price: f64,
    pub ratio: f64,
    pub executed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PyramidSignal {
    pub level: usize,
    pub size: f64,
    pub price: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionBook {
    pub config: PositionConfig,
    pub positions: HashMap<String, TrendPosition>,
    pub total_exposure: f64,
    pub inventory_limits: HashMap<String, f64>,
    pub inventory_value_limits: HashMap<String, f64>,
    pub max_leverage: f64,
}

impl PositionBook {
    pub fn new(
        config: PositionConfig,
        inventory_limits: HashMap<String, f64>,
        inventory_value_limits: HashMap<String, f64>,
        max_leverage: f64,
    ) -> Self {
        Self {
            config,
            positions: HashMap::new(),
            total_exposure: 0.0,
            inventory_limits,
            inventory_value_limits,
            max_leverage,
        }
    }

    pub fn position_key(symbol: &str, side: OrderSide) -> String {
        format!("{}#{}", symbol, side_label(side))
    }

    pub fn has_open_position(&self, symbol: &str) -> bool {
        self.positions
            .values()
            .any(|position| position.symbol == symbol)
    }

    pub fn has_open_position_side(&self, symbol: &str, side: OrderSide) -> bool {
        self.positions
            .contains_key(&Self::position_key(symbol, side))
    }

    pub fn total_open_positions(&self) -> usize {
        self.positions.len()
    }

    pub fn calculate_position_size(
        &self,
        signal: &TradeSignal,
        capital: AccountCapital,
        target_notional: Option<f64>,
        hour_utc: u32,
    ) -> Result<f64, PositionSizingError> {
        if capital.available <= 0.0 {
            return Err(PositionSizingError::InvalidCapital);
        }
        let entry_price = signal.entry_price.max(1e-6);
        let mut position_size = if let Some(target) = target_notional {
            (target / entry_price).max(0.0)
        } else {
            let risk_per_unit = (signal.entry_price - signal.stop_loss).abs().max(1e-6);
            let base_notional = capital.available * self.config.base_risk_ratio.max(1e-4);
            let mut size = (base_notional / risk_per_unit).max(0.0);
            let kelly =
                calculate_kelly_factor(signal.risk_reward_ratio, self.config.kelly_fraction)
                    .clamp(0.1, 1.0);
            size *= kelly.max(0.25);
            if self.config.trend_factor_enabled {
                size *= trend_factor(signal.confidence);
            }
            if self.config.volatility_factor_enabled {
                let stop_percent = (signal.entry_price - signal.stop_loss).abs() / entry_price;
                size *= volatility_factor(stop_percent);
            }
            if self.config.time_factor_enabled {
                size *= time_factor(hour_utc);
            }
            if signal.suggested_size > 0.0 {
                size = size.min(signal.suggested_size);
            }
            size
        };

        let max_notional = (capital.available * self.max_leverage).max(5.0);
        position_size = position_size.min(max_notional / entry_price);

        let min_notional = 20.0;
        let min_qty = min_notional / entry_price;
        if position_size < min_qty {
            if min_notional > capital.available * self.max_leverage {
                return Ok(0.0);
            }
            position_size = min_qty;
        }

        if let Some(limit) = self.inventory_limits.get(&signal.symbol) {
            let remaining = limit - self.symbol_total_size(&signal.symbol);
            if remaining <= 0.0 {
                return Ok(0.0);
            }
            position_size = position_size.min(remaining);
        }
        if let Some(value_cap) = self.inventory_value_limits.get(&signal.symbol) {
            let current_notional = self.symbol_total_notional(&signal.symbol);
            if current_notional >= *value_cap {
                return Ok(0.0);
            }
            let remaining_notional = (value_cap - current_notional).max(0.0);
            if remaining_notional <= f64::EPSILON {
                return Ok(0.0);
            }
            let planned_notional = position_size * entry_price;
            if planned_notional > remaining_notional {
                let adjusted = remaining_notional / entry_price;
                if adjusted <= f64::EPSILON {
                    return Ok(0.0);
                }
                position_size = adjusted;
            }
        }

        let notional = position_size * entry_price;
        if notional < 20.0 {
            return Ok(0.0);
        }
        Ok(position_size.max(0.0))
    }

    pub fn add_position(
        &mut self,
        signal: TradeSignal,
        size: f64,
        now: DateTime<Utc>,
    ) -> TrendPosition {
        let position = TrendPosition {
            symbol: signal.symbol.clone(),
            side: signal.side.clone(),
            entry_price: signal.entry_price,
            average_price: signal.entry_price,
            current_price: signal.entry_price,
            size,
            current_size: size,
            initial_size: size,
            pyramid_entries: vec![],
            pyramid_count: 0,
            stop_loss: signal.stop_loss,
            take_profits: signal
                .take_profits
                .into_iter()
                .map(|tp| TakeProfitLevel {
                    price: tp.price,
                    ratio: tp.ratio,
                    executed: false,
                })
                .collect(),
            entry_time: now,
            last_update: now,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            peak_profit: 0.0,
            risk_amount: size * (signal.entry_price - signal.stop_loss).abs(),
        };
        let key = Self::position_key(&signal.symbol, signal.side);
        self.positions.insert(key, position.clone());
        self.total_exposure += size;
        position
    }

    pub fn should_pyramid(
        &self,
        position: &TrendPosition,
        current_price: f64,
    ) -> Option<PyramidSignal> {
        if !self.config.pyramid_enabled {
            return None;
        }
        let profit_r = calculate_profit_r(position, current_price);
        for (i, level) in self.config.pyramid_levels.iter().enumerate() {
            let already_pyramided = position
                .pyramid_entries
                .iter()
                .any(|entry| entry.level == i);
            if !already_pyramided && profit_r >= level.trigger_profit {
                return Some(PyramidSignal {
                    level: i,
                    size: position.initial_size * level.size_ratio,
                    price: current_price,
                });
            }
        }
        None
    }

    pub fn pyramid_position(
        &mut self,
        symbol: &str,
        side: OrderSide,
        pyramid_signal: PyramidSignal,
        now: DateTime<Utc>,
    ) -> Result<(), PositionSizingError> {
        let key = Self::position_key(symbol, side);
        let position = self
            .positions
            .get_mut(&key)
            .ok_or(PositionSizingError::PositionMissing)?;
        position.pyramid_entries.push(PyramidEntry {
            level: pyramid_signal.level,
            price: pyramid_signal.price,
            size: pyramid_signal.size,
            time: now,
        });
        let total_value = position.average_price * position.current_size
            + pyramid_signal.price * pyramid_signal.size;
        position.current_size += pyramid_signal.size;
        position.average_price = total_value / position.current_size;
        self.total_exposure += pyramid_signal.size;
        Ok(())
    }

    pub fn update_position_pnl(
        &mut self,
        symbol: &str,
        side: OrderSide,
        current_price: f64,
        now: DateTime<Utc>,
    ) -> Result<(), PositionSizingError> {
        let key = Self::position_key(symbol, side);
        let position = self
            .positions
            .get_mut(&key)
            .ok_or(PositionSizingError::PositionMissing)?;
        position.unrealized_pnl = match &position.side {
            OrderSide::Buy => (current_price - position.average_price) * position.current_size,
            OrderSide::Sell => (position.average_price - current_price) * position.current_size,
        };
        if position.unrealized_pnl > position.peak_profit {
            position.peak_profit = position.unrealized_pnl;
        }
        position.current_price = current_price;
        position.last_update = now;
        Ok(())
    }

    pub fn close_position_with_fill(
        &mut self,
        symbol: &str,
        side: OrderSide,
        fill_price: f64,
        now: DateTime<Utc>,
    ) -> Option<TrendPosition> {
        let key = Self::position_key(symbol, side);
        let mut position = self.positions.remove(&key)?;
        let exit_size = position.current_size;
        let pnl = match &position.side {
            OrderSide::Buy => (fill_price - position.average_price) * exit_size,
            OrderSide::Sell => (position.average_price - fill_price) * exit_size,
        };
        position.current_price = fill_price;
        position.current_size = 0.0;
        position.realized_pnl += pnl;
        position.unrealized_pnl = 0.0;
        position.last_update = now;
        self.total_exposure = (self.total_exposure - exit_size).max(0.0);
        Some(position)
    }

    fn symbol_total_size(&self, symbol: &str) -> f64 {
        self.positions
            .values()
            .filter(|position| position.symbol == symbol)
            .map(|position| position.current_size)
            .sum()
    }

    fn symbol_total_notional(&self, symbol: &str) -> f64 {
        self.positions
            .values()
            .filter(|position| position.symbol == symbol)
            .map(|position| position.current_size * position.current_price.abs())
            .sum()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSizingError {
    InvalidCapital,
    PositionMissing,
}

pub fn side_label(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "LONG",
        OrderSide::Sell => "SHORT",
    }
}

pub fn calculate_kelly_factor(risk_reward_ratio: f64, kelly_fraction: f64) -> f64 {
    let win_rate = 0.45;
    if risk_reward_ratio <= 0.0 {
        return 0.0;
    }
    let kelly = (win_rate * risk_reward_ratio - (1.0 - win_rate)) / risk_reward_ratio;
    (kelly * kelly_fraction).clamp(0.0, 1.0)
}

pub fn trend_factor(confidence: f64) -> f64 {
    match confidence {
        c if c > 80.0 => 1.2,
        c if c > 70.0 => 1.1,
        c if c > 60.0 => 1.0,
        _ => 0.8,
    }
}

pub fn volatility_factor(stop_percent: f64) -> f64 {
    match stop_percent {
        s if s < 0.01 => 1.2,
        s if s < 0.02 => 1.0,
        s if s < 0.03 => 0.8,
        _ => 0.6,
    }
}

pub fn time_factor(hour_utc: u32) -> f64 {
    match hour_utc {
        8..=16 => 1.0,
        17..=20 => 0.9,
        _ => 0.8,
    }
}

pub fn calculate_profit_r(position: &TrendPosition, current_price: f64) -> f64 {
    let risk = (position.average_price - position.stop_loss).abs();
    if risk == 0.0 {
        return 0.0;
    }
    match &position.side {
        OrderSide::Buy => (current_price - position.average_price) / risk,
        OrderSide::Sell => (position.average_price - current_price) / risk,
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopUpdate {
    MoveTo(f64),
    Tighten(f64),
    Breakeven,
    Trailing(f64),
    None,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StopUpdateInputs {
    pub current_price: f64,
    pub atr_fast: f64,
    pub atr_slow: f64,
    pub now: DateTime<Utc>,
}

impl StopUpdateInputs {
    pub fn atr_value(&self) -> f64 {
        self.atr_fast.max(self.atr_slow).max(1e-6)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartialTakeProfit {
    pub index: usize,
    pub price: f64,
    pub size: f64,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StopReport {
    pub symbol: String,
    pub current_stop: f64,
    pub stop_type: String,
    pub stop_distance: f64,
    pub stop_percent: f64,
    pub profit_r: f64,
    pub is_trailing: bool,
    pub is_breakeven: bool,
}

pub fn calculate_initial_stop(
    config: &StopConfig,
    entry_price: f64,
    side: OrderSide,
    atr: Option<f64>,
) -> f64 {
    let stop_distance = match config.initial_stop_type {
        StopType::Fixed | StopType::Percentage => entry_price * config.fixed_stop_percent,
        StopType::ATR => atr
            .map(|atr_value| atr_value * config.atr_multiplier)
            .unwrap_or(entry_price * config.fixed_stop_percent),
        StopType::Structure => entry_price * 0.02,
    };
    match side {
        OrderSide::Buy => entry_price - stop_distance,
        OrderSide::Sell => entry_price + stop_distance,
    }
}

pub fn calculate_stop_update(
    config: &StopConfig,
    position: &TrendPosition,
    inputs: &StopUpdateInputs,
) -> Option<StopUpdate> {
    let current_price = inputs.current_price;
    let profit = match position.side {
        OrderSide::Buy => current_price - position.average_price,
        OrderSide::Sell => position.average_price - current_price,
    };
    let profit_percent = profit / position.average_price;
    let profit_r = calculate_profit_r(position, current_price);

    if profit_percent >= config.lock_profit_pct {
        let lock_price = match position.side {
            OrderSide::Buy => position.entry_price * (1.0 + config.lock_buffer_pct),
            OrderSide::Sell => position.entry_price * (1.0 - config.lock_buffer_pct),
        };
        if should_update_stop(position, lock_price) {
            return Some(StopUpdate::MoveTo(lock_price));
        }
    }

    if let Some(time_stop_hours) = config.time_stop_hours {
        let holding_duration = inputs.now.signed_duration_since(position.entry_time);
        if holding_duration > chrono::Duration::hours(time_stop_hours as i64) && profit < 0.0 {
            return Some(StopUpdate::MoveTo(current_price));
        }
    }

    if config.breakeven_enabled
        && profit_r >= config.breakeven_trigger
        && !is_at_breakeven(position)
    {
        return Some(StopUpdate::Breakeven);
    }

    if let Some(update) = evaluate_pnl_trailing(config, position, current_price, inputs.atr_value())
    {
        return Some(update);
    }

    if config.trailing_stop_enabled && profit_r >= config.trailing_activation {
        let new_stop = calculate_trailing_stop(config, position, current_price);
        if should_update_stop(position, new_stop) {
            return Some(StopUpdate::Trailing(new_stop));
        }
    }

    None
}

pub fn calculate_trailing_stop(
    config: &StopConfig,
    position: &TrendPosition,
    current_price: f64,
) -> f64 {
    let trail_distance = position.risk_amount * config.trailing_distance;
    match position.side {
        OrderSide::Buy => current_price - trail_distance,
        OrderSide::Sell => current_price + trail_distance,
    }
}

pub fn should_update_stop(position: &TrendPosition, new_stop: f64) -> bool {
    match position.side {
        OrderSide::Buy => new_stop > position.stop_loss,
        OrderSide::Sell => new_stop < position.stop_loss,
    }
}

pub fn is_at_breakeven(position: &TrendPosition) -> bool {
    (position.stop_loss - position.entry_price).abs() < 0.0001
}

pub fn evaluate_pnl_trailing(
    config: &StopConfig,
    position: &TrendPosition,
    current_price: f64,
    atr_value: f64,
) -> Option<StopUpdate> {
    if !config.pnl_trailing.enable {
        return None;
    }
    let atr_value = atr_value.max(1e-6);
    let profit_in_atr = match position.side {
        OrderSide::Buy => (current_price - position.entry_price) / atr_value,
        OrderSide::Sell => (position.entry_price - current_price) / atr_value,
    };

    let mut candidate = None;
    for level in &config.pnl_trailing.lock_levels {
        if profit_in_atr >= level.profit_atr {
            let target = match position.side {
                OrderSide::Buy => {
                    current_price
                        - atr_value * level.stop_offset_atr * config.pnl_trailing.atr_multiple
                }
                OrderSide::Sell => {
                    current_price
                        + atr_value * level.stop_offset_atr * config.pnl_trailing.atr_multiple
                }
            };
            candidate = Some(target);
        }
    }

    candidate
        .filter(|target| should_update_stop(position, *target))
        .map(StopUpdate::MoveTo)
}

pub fn check_partial_profits(
    config: &StopConfig,
    position: &TrendPosition,
    current_price: f64,
) -> Vec<PartialTakeProfit> {
    let mut take_profits = Vec::new();
    for (index, target) in config.partial_targets.iter().enumerate() {
        if position
            .take_profits
            .get(index)
            .map(|tp| tp.executed)
            .unwrap_or(true)
        {
            continue;
        }
        let profit_r = calculate_profit_r(position, current_price);
        if profit_r >= target.target_r {
            take_profits.push(PartialTakeProfit {
                index,
                price: current_price,
                size: position.current_size * target.close_ratio,
                reason: format!("target {}R reached", target.target_r),
            });
        }
    }
    take_profits
}

pub fn get_stop_report(config: &StopConfig, position: &TrendPosition) -> StopReport {
    let current_price = position.average_price;
    let profit_r = calculate_profit_r(position, current_price);
    let stop_distance = (position.stop_loss - position.average_price).abs();
    let stop_percent = stop_distance / position.average_price * 100.0;
    StopReport {
        symbol: position.symbol.clone(),
        current_stop: position.stop_loss,
        stop_type: current_stop_type(config, position, profit_r),
        stop_distance,
        stop_percent,
        profit_r,
        is_trailing: profit_r >= config.trailing_activation,
        is_breakeven: is_at_breakeven(position),
    }
}

pub fn current_stop_type(config: &StopConfig, position: &TrendPosition, profit_r: f64) -> String {
    if is_at_breakeven(position) {
        "breakeven".to_string()
    } else if profit_r >= config.trailing_activation {
        "trailing".to_string()
    } else {
        "initial".to_string()
    }
}

pub fn emergency_stop(_position: &TrendPosition) -> f64 {
    0.0
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Normal = 0,
    Warning = 1,
    Danger = 2,
    Emergency = 3,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SystemHealth {
    pub api_latency: f64,
    pub network_status: bool,
    pub exchange_status: bool,
    pub data_feed_status: bool,
    pub error_count: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountMetrics {
    pub total_equity: f64,
    pub available_balance: f64,
    pub used_margin: f64,
    pub daily_pnl: f64,
    pub current_drawdown: f64,
    pub total_exposure: f64,
    pub leverage_used: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyMetrics {
    pub open_positions: usize,
    pub daily_trades: usize,
    pub consecutive_losses: usize,
    pub win_rate: f64,
    pub largest_loss: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderMetrics {
    pub order_success_rate: f64,
    pub max_slippage: f64,
    pub avg_execution_time: f64,
    pub failed_orders: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskSnapshot {
    pub system: SystemHealth,
    pub account: AccountMetrics,
    pub strategy: StrategyMetrics,
    pub order: OrderMetrics,
}

pub fn evaluate_risk_layers(config: &RiskConfig, snapshot: &RiskSnapshot) -> RiskLevel {
    [
        evaluate_system_layer(&snapshot.system),
        evaluate_account_layer(config, &snapshot.account),
        evaluate_strategy_layer(config, &snapshot.strategy),
        evaluate_order_layer(config, &snapshot.order),
    ]
    .into_iter()
    .max()
    .unwrap_or(RiskLevel::Normal)
}

pub fn evaluate_system_layer(health: &SystemHealth) -> RiskLevel {
    if !health.network_status || !health.exchange_status {
        RiskLevel::Emergency
    } else if health.api_latency > 1000.0 {
        RiskLevel::Danger
    } else if health.error_count > 10 {
        RiskLevel::Warning
    } else {
        RiskLevel::Normal
    }
}

pub fn evaluate_account_layer(config: &RiskConfig, metrics: &AccountMetrics) -> RiskLevel {
    if metrics.current_drawdown > config.emergency_stop_loss {
        return RiskLevel::Emergency;
    }
    if metrics.current_drawdown > config.max_drawdown {
        return RiskLevel::Danger;
    }
    let daily_loss_ratio = if metrics.total_equity > 0.0 {
        -metrics.daily_pnl / metrics.total_equity
    } else {
        0.0
    };
    if daily_loss_ratio > config.max_daily_loss {
        return RiskLevel::Danger;
    }
    if metrics.leverage_used > config.max_leverage
        || metrics.total_exposure > config.max_total_exposure
    {
        return RiskLevel::Warning;
    }
    RiskLevel::Normal
}

pub fn evaluate_strategy_layer(config: &RiskConfig, metrics: &StrategyMetrics) -> RiskLevel {
    if metrics.consecutive_losses >= config.max_consecutive_losses {
        return RiskLevel::Danger;
    }
    if metrics.win_rate < 0.3 && metrics.daily_trades > 5 {
        return RiskLevel::Warning;
    }
    if metrics.largest_loss > config.max_risk_per_trade * 2.0 {
        return RiskLevel::Warning;
    }
    RiskLevel::Normal
}

pub fn evaluate_order_layer(config: &RiskConfig, metrics: &OrderMetrics) -> RiskLevel {
    if metrics.max_slippage > config.max_slippage
        || metrics.order_success_rate < 0.9
        || metrics.avg_execution_time > 1000.0
    {
        return RiskLevel::Warning;
    }
    RiskLevel::Normal
}

pub fn approve_trade(
    config: &RiskConfig,
    snapshot: &RiskSnapshot,
    signal: &TradeSignal,
    position_size: f64,
) -> bool {
    match evaluate_risk_layers(config, snapshot) {
        RiskLevel::Emergency | RiskLevel::Danger => return false,
        RiskLevel::Warning | RiskLevel::Normal => {}
    }
    let risk_amount = position_size * (signal.entry_price - signal.stop_loss).abs();
    let total_equity = snapshot.account.total_equity.max(1.0);
    let risk_ratio = risk_amount / total_equity;
    if risk_ratio > config.max_risk_per_trade {
        return false;
    }
    let proposed_exposure = (position_size * signal.entry_price.abs()) / total_equity;
    proposed_exposure <= config.max_total_exposure
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitorTradeRecord {
    pub symbol: String,
    pub side: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: f64,
    pub pnl: f64,
    pub entry_time: DateTime<Utc>,
    pub exit_time: DateTime<Utc>,
    pub holding_time: i64,
    pub exit_reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate: f64,
    pub total_pnl: f64,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub largest_win: f64,
    pub largest_loss: f64,
    pub profit_factor: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub current_drawdown: f64,
    pub recovery_factor: f64,
    pub avg_holding_time: f64,
    pub last_update: DateTime<Utc>,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            total_trades: 0,
            winning_trades: 0,
            losing_trades: 0,
            win_rate: 0.0,
            total_pnl: 0.0,
            avg_win: 0.0,
            avg_loss: 0.0,
            largest_win: 0.0,
            largest_loss: 0.0,
            profit_factor: 0.0,
            sharpe_ratio: 0.0,
            max_drawdown: 0.0,
            current_drawdown: 0.0,
            recovery_factor: 0.0,
            avg_holding_time: 0.0,
            last_update: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerformanceReport {
    pub metrics: PerformanceMetrics,
    pub recent_trades: Vec<MonitorTradeRecord>,
    pub report_time: DateTime<Utc>,
}

pub fn record_monitor_trade(
    metrics: &mut PerformanceMetrics,
    high_water_mark: &mut f64,
    trade: &MonitorTradeRecord,
    now: DateTime<Utc>,
) {
    metrics.total_trades += 1;
    metrics.total_pnl += trade.pnl;

    if trade.pnl > 0.0 {
        metrics.winning_trades += 1;
        metrics.avg_win = (metrics.avg_win * (metrics.winning_trades - 1) as f64 + trade.pnl)
            / metrics.winning_trades as f64;
        if trade.pnl > metrics.largest_win {
            metrics.largest_win = trade.pnl;
        }
    } else {
        metrics.losing_trades += 1;
        metrics.avg_loss = (metrics.avg_loss * (metrics.losing_trades - 1) as f64
            + trade.pnl.abs())
            / metrics.losing_trades as f64;
        if trade.pnl < metrics.largest_loss {
            metrics.largest_loss = trade.pnl;
        }
    }

    if metrics.total_trades > 0 {
        metrics.win_rate = metrics.winning_trades as f64 / metrics.total_trades as f64;
    }
    if metrics.avg_loss > 0.0 {
        metrics.profit_factor = metrics.avg_win / metrics.avg_loss;
    }
    metrics.avg_holding_time = (metrics.avg_holding_time * (metrics.total_trades - 1) as f64
        + trade.holding_time as f64)
        / metrics.total_trades as f64;

    update_monitor_drawdown(metrics, high_water_mark, metrics.total_pnl);
    metrics.last_update = now;
}

pub fn refresh_monitor_statistics(metrics: &mut PerformanceMetrics, trades: &[MonitorTradeRecord]) {
    if trades.is_empty() {
        return;
    }
    let returns: Vec<f64> = trades.iter().map(|trade| trade.pnl).collect();
    metrics.sharpe_ratio = calculate_monitor_sharpe_ratio(&returns);
    if metrics.max_drawdown > 0.0 {
        metrics.recovery_factor = metrics.total_pnl / metrics.max_drawdown;
    }
}

pub fn build_performance_report(
    metrics: &PerformanceMetrics,
    trades: &[MonitorTradeRecord],
    recent_count: usize,
    report_time: DateTime<Utc>,
) -> PerformanceReport {
    PerformanceReport {
        metrics: metrics.clone(),
        recent_trades: trades.iter().rev().take(recent_count).cloned().collect(),
        report_time,
    }
}

pub fn calculate_monitor_sharpe_ratio(returns: &[f64]) -> f64 {
    if returns.len() < 2 {
        return 0.0;
    }
    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (returns.len() - 1) as f64;
    let std_dev = variance.sqrt();
    if std_dev > 0.0 {
        mean / std_dev * (252.0_f64).sqrt()
    } else {
        0.0
    }
}

pub fn update_monitor_drawdown(
    metrics: &mut PerformanceMetrics,
    high_water_mark: &mut f64,
    current_equity: f64,
) {
    if current_equity > *high_water_mark {
        *high_water_mark = current_equity;
    }
    if *high_water_mark > 0.0 {
        metrics.current_drawdown = (*high_water_mark - current_equity) / *high_water_mark;
        if metrics.current_drawdown > metrics.max_drawdown {
            metrics.max_drawdown = metrics.current_drawdown;
        }
    }
}

pub fn default_entry_leverage() -> f64 {
    1.0
}

pub fn default_entry_price_improve_bps() -> f64 {
    2.0
}

pub fn default_poll_interval_secs() -> u64 {
    30
}

pub fn default_signal_cooldown_secs() -> u64 {
    300
}

pub fn default_lock_profit_pct() -> f64 {
    0.005
}

pub fn default_lock_buffer_pct() -> f64 {
    0.001
}

pub fn default_one_f64() -> f64 {
    1.0
}

pub fn default_trailing_levels() -> Vec<PnlLockLevel> {
    vec![
        PnlLockLevel {
            profit_atr: 1.0,
            stop_offset_atr: 0.2,
        },
        PnlLockLevel {
            profit_atr: 2.0,
            stop_offset_atr: 0.8,
        },
    ]
}

pub fn default_execution_prefer_maker() -> bool {
    true
}

pub fn default_execution_allow_taker_fallback() -> bool {
    true
}

pub fn default_execution_maker_offset_bps() -> f64 {
    2.0
}

pub fn default_execution_taker_slippage_bps() -> f64 {
    5.0
}

pub fn default_execution_max_retries() -> usize {
    3
}

pub fn default_execution_retry_backoff_ms() -> u64 {
    250
}

pub fn default_execution_order_timeout_secs() -> u64 {
    10
}

pub fn default_execution_iceberg_visible_ratio() -> f64 {
    0.2
}

pub fn default_cache_depth() -> usize {
    500
}

pub fn default_bootstrap_bars() -> usize {
    200
}

pub fn default_max_data_lag_ms() -> u64 {
    200
}

pub fn default_min_one_minute_bars() -> usize {
    60
}

pub fn default_min_five_minute_bars() -> usize {
    120
}

pub fn default_min_fifteen_minute_bars() -> usize {
    64
}

pub fn default_min_one_hour_bars() -> usize {
    64
}

pub fn current_utc_hour(now: DateTime<Utc>) -> u32 {
    now.hour()
}
