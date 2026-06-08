use std::collections::VecDeque;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::types::{Kline, MarketType, OrderSide};
use crate::utils::{ceil_to_step, round_price};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MeanReversionEngineConfig {
    #[serde(default)]
    pub market_type: MeanReversionMarketType,
    #[serde(default = "default_allow_short")]
    pub allow_short: bool,
    #[serde(default)]
    pub indicators: IndicatorConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
    #[serde(default)]
    pub risk: RiskConfig,
    #[serde(default)]
    pub liquidity: LiquidityConfig,
}

impl Default for MeanReversionEngineConfig {
    fn default() -> Self {
        Self {
            market_type: MeanReversionMarketType::Spot,
            allow_short: default_allow_short(),
            indicators: IndicatorConfig::default(),
            execution: ExecutionConfig::default(),
            risk: RiskConfig::default(),
            liquidity: LiquidityConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MeanReversionMarketType {
    #[default]
    Spot,
    Margin,
    Perpetual,
    Futures,
    Option,
    Custom(String),
}

impl From<MeanReversionMarketType> for MarketType {
    fn from(value: MeanReversionMarketType) -> Self {
        match value {
            MeanReversionMarketType::Spot => MarketType::Spot,
            MeanReversionMarketType::Margin => MarketType::Margin,
            MeanReversionMarketType::Perpetual => MarketType::Perpetual,
            MeanReversionMarketType::Futures => MarketType::Futures,
            MeanReversionMarketType::Option => MarketType::Option,
            MeanReversionMarketType::Custom(value) => MarketType::Custom(value),
        }
    }
}

fn default_allow_short() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SymbolConfig {
    pub symbol: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_symbol_volume_threshold")]
    pub min_quote_volume_5m: f64,
    #[serde(default = "default_depth_multiplier")]
    pub depth_multiplier: f64,
    #[serde(default = "default_symbol_top_levels")]
    pub depth_levels: usize,
    #[serde(default)]
    pub allow_short: Option<bool>,
}

fn default_true() -> bool {
    true
}

fn default_symbol_volume_threshold() -> f64 {
    100_000.0
}

fn default_depth_multiplier() -> f64 {
    3.0
}

fn default_symbol_top_levels() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IndicatorConfig {
    #[serde(default)]
    pub bollinger: BollingerConfig,
    #[serde(default)]
    pub rsi: RsiConfig,
    #[serde(default)]
    pub atr: AtrConfig,
    #[serde(default)]
    pub adx: AdxConfig,
    #[serde(default)]
    pub bbw: BbwConfig,
    #[serde(default)]
    pub slope: SlopeConfig,
    #[serde(default)]
    pub choppiness: Option<ChoppinessConfig>,
    #[serde(default)]
    pub volume: VolumeConfig,
}

impl Default for IndicatorConfig {
    fn default() -> Self {
        Self {
            bollinger: BollingerConfig::default(),
            rsi: RsiConfig::default(),
            atr: AtrConfig::default(),
            adx: AdxConfig::default(),
            bbw: BbwConfig::default(),
            slope: SlopeConfig::default(),
            choppiness: Some(ChoppinessConfig {
                enabled: false,
                lookback: 14,
                threshold: 62.0,
            }),
            volume: VolumeConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BollingerConfig {
    #[serde(default = "default_boll_period")]
    pub period: usize,
    #[serde(default = "default_boll_std")]
    pub std_dev: f64,
    #[serde(default = "default_band_pct_long")]
    pub entry_band_pct_long: f64,
    #[serde(default = "default_band_pct_short")]
    pub entry_band_pct_short: f64,
    #[serde(default = "default_z_long")]
    pub entry_z_long: f64,
    #[serde(default = "default_z_short")]
    pub entry_z_short: f64,
}

impl Default for BollingerConfig {
    fn default() -> Self {
        Self {
            period: default_boll_period(),
            std_dev: default_boll_std(),
            entry_band_pct_long: default_band_pct_long(),
            entry_band_pct_short: default_band_pct_short(),
            entry_z_long: default_z_long(),
            entry_z_short: default_z_short(),
        }
    }
}

fn default_boll_period() -> usize {
    20
}

fn default_boll_std() -> f64 {
    2.0
}

fn default_band_pct_long() -> f64 {
    0.1
}

fn default_band_pct_short() -> f64 {
    0.9
}

fn default_z_long() -> f64 {
    -1.2
}

fn default_z_short() -> f64 {
    1.2
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RsiConfig {
    #[serde(default = "default_rsi_period")]
    pub period: usize,
    #[serde(default = "default_rsi_long")]
    pub long_threshold: f64,
    #[serde(default = "default_rsi_short")]
    pub short_threshold: f64,
}

impl Default for RsiConfig {
    fn default() -> Self {
        Self {
            period: default_rsi_period(),
            long_threshold: default_rsi_long(),
            short_threshold: default_rsi_short(),
        }
    }
}

fn default_rsi_period() -> usize {
    14
}

fn default_rsi_long() -> f64 {
    25.0
}

fn default_rsi_short() -> f64 {
    75.0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AtrConfig {
    #[serde(default = "default_atr_period")]
    pub period: usize,
    #[serde(default = "default_initial_stop_k")]
    pub initial_stop_k: f64,
    #[serde(default = "default_trailing_stop_k")]
    pub trailing_stop_k: f64,
}

impl Default for AtrConfig {
    fn default() -> Self {
        Self {
            period: default_atr_period(),
            initial_stop_k: default_initial_stop_k(),
            trailing_stop_k: default_trailing_stop_k(),
        }
    }
}

fn default_atr_period() -> usize {
    14
}

fn default_initial_stop_k() -> f64 {
    1.4
}

fn default_trailing_stop_k() -> f64 {
    1.2
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AdxConfig {
    #[serde(default = "default_adx_period")]
    pub period: usize,
    #[serde(default = "default_adx_threshold")]
    pub threshold: f64,
}

impl Default for AdxConfig {
    fn default() -> Self {
        Self {
            period: default_adx_period(),
            threshold: default_adx_threshold(),
        }
    }
}

fn default_adx_period() -> usize {
    14
}

fn default_adx_threshold() -> f64 {
    20.0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BbwConfig {
    #[serde(default = "default_bbw_lookback")]
    pub lookback: usize,
    #[serde(default = "default_bbw_percentile")]
    pub percentile: f64,
}

impl Default for BbwConfig {
    fn default() -> Self {
        Self {
            lookback: default_bbw_lookback(),
            percentile: default_bbw_percentile(),
        }
    }
}

fn default_bbw_lookback() -> usize {
    400
}

fn default_bbw_percentile() -> f64 {
    0.35
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SlopeConfig {
    #[serde(default = "default_slope_lookback")]
    pub lookback: usize,
    #[serde(default = "default_slope_threshold")]
    pub threshold: f64,
}

impl Default for SlopeConfig {
    fn default() -> Self {
        Self {
            lookback: default_slope_lookback(),
            threshold: default_slope_threshold(),
        }
    }
}

fn default_slope_lookback() -> usize {
    5
}

fn default_slope_threshold() -> f64 {
    0.15
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChoppinessConfig {
    pub enabled: bool,
    pub lookback: usize,
    pub threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VolumeConfig {
    #[serde(default = "default_volume_window_minutes")]
    pub lookback_minutes: u64,
    #[serde(default = "default_symbol_volume_threshold")]
    pub min_quote_volume: f64,
}

impl Default for VolumeConfig {
    fn default() -> Self {
        Self {
            lookback_minutes: default_volume_window_minutes(),
            min_quote_volume: default_symbol_volume_threshold(),
        }
    }
}

fn default_volume_window_minutes() -> u64 {
    10
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionConfig {
    #[serde(default = "default_true")]
    pub post_only: bool,
    #[serde(default = "default_base_improve")]
    pub base_improve: f64,
    #[serde(default = "default_alpha_atr")]
    pub alpha_atr: f64,
    #[serde(default = "default_beta_sigma")]
    pub beta_sigma: f64,
    #[serde(default = "default_take_profit_atr_k")]
    pub take_profit_atr_k: f64,
    #[serde(default = "default_ttl_secs")]
    pub ttl_secs: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            post_only: true,
            base_improve: default_base_improve(),
            alpha_atr: default_alpha_atr(),
            beta_sigma: default_beta_sigma(),
            take_profit_atr_k: default_take_profit_atr_k(),
            ttl_secs: default_ttl_secs(),
        }
    }
}

fn default_base_improve() -> f64 {
    0.001
}

fn default_alpha_atr() -> f64 {
    1.5
}

fn default_beta_sigma() -> f64 {
    1.0
}

fn default_take_profit_atr_k() -> f64 {
    1.8
}

fn default_ttl_secs() -> u64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RiskConfig {
    #[serde(default = "default_max_positions")]
    pub max_positions: usize,
    #[serde(default = "default_max_symbol_positions")]
    pub max_positions_per_symbol: usize,
    #[serde(default = "default_max_net_exposure")]
    pub max_net_exposure: f64,
    #[serde(default = "default_per_trade_notional")]
    pub per_trade_notional: f64,
    #[serde(default = "default_per_trade_risk")]
    pub per_trade_risk: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_positions: default_max_positions(),
            max_positions_per_symbol: default_max_symbol_positions(),
            max_net_exposure: default_max_net_exposure(),
            per_trade_notional: default_per_trade_notional(),
            per_trade_risk: default_per_trade_risk(),
        }
    }
}

fn default_max_positions() -> usize {
    10
}

fn default_max_symbol_positions() -> usize {
    1
}

fn default_max_net_exposure() -> f64 {
    1_000.0
}

fn default_per_trade_notional() -> f64 {
    6.0
}

fn default_per_trade_risk() -> f64 {
    6.0
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LiquidityConfig {
    #[serde(default = "default_symbol_volume_threshold")]
    pub min_recent_quote_volume: f64,
    #[serde(default = "default_depth_multiplier")]
    pub depth_multiplier: f64,
    #[serde(default = "default_symbol_top_levels")]
    pub top_levels: usize,
    #[serde(default = "default_maker_fee_threshold")]
    pub maker_fee_threshold: f64,
}

impl Default for LiquidityConfig {
    fn default() -> Self {
        Self {
            min_recent_quote_volume: default_symbol_volume_threshold(),
            depth_multiplier: default_depth_multiplier(),
            top_levels: default_symbol_top_levels(),
            maker_fee_threshold: default_maker_fee_threshold(),
        }
    }
}

fn default_maker_fee_threshold() -> f64 {
    0.0
}

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolMeta {
    pub symbol: String,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: Option<f64>,
    pub min_order_size: f64,
    pub max_order_size: f64,
    pub price_precision: u32,
    pub amount_precision: u32,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct LiquiditySnapshot {
    pub bid_price: f64,
    pub ask_price: f64,
    pub spread: f64,
    pub total_bid_depth: f64,
    pub total_ask_depth: f64,
    pub maker_fee: Option<f64>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolSnapshot {
    pub config: SymbolConfig,
    pub five_minute: Vec<Kline>,
    pub fifteen_minute: Vec<Kline>,
    pub one_hour: Vec<Kline>,
    pub bbw_history: Vec<f64>,
    pub mid_history: Vec<f64>,
    pub sigma_history: Vec<f64>,
    pub frozen: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BollingerSnapshot {
    pub upper: f64,
    pub middle: f64,
    pub lower: f64,
    pub sigma: f64,
    pub band_percent: f64,
    pub z_score: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndicatorOutputs {
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

#[derive(Debug, Clone, PartialEq)]
pub struct OrderPlan {
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: f64,
    pub limit_price: f64,
    pub stop_price: f64,
    pub trailing_distance: f64,
    pub take_profit: f64,
    pub improve: f64,
    pub indicators: IndicatorOutputs,
    pub liquidity: LiquiditySnapshot,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedOrders {
    pub symbol: String,
    pub range_score: u8,
    pub plans: Vec<OrderPlan>,
}

pub fn plan_symbol_orders(
    config: &MeanReversionEngineConfig,
    snapshot: &SymbolSnapshot,
    liquidity: &LiquiditySnapshot,
    symbol_meta: &SymbolMeta,
) -> Result<PlannedOrders> {
    if snapshot.frozen || !snapshot.config.enabled {
        return Ok(PlannedOrders {
            symbol: snapshot.config.symbol.clone(),
            range_score: 0,
            plans: Vec::new(),
        });
    }

    let indicators = compute_indicators(snapshot, &config.indicators)?;
    let range_score = evaluate_range_conditions(&indicators, &config.indicators);
    if range_score < 2 || !check_liquidity_constraints(config, snapshot, liquidity, &indicators) {
        return Ok(PlannedOrders {
            symbol: snapshot.config.symbol.clone(),
            range_score,
            plans: Vec::new(),
        });
    }

    let mut plans = Vec::new();
    if let Some(plan) = build_order_plan(
        config,
        snapshot,
        &indicators,
        liquidity,
        symbol_meta,
        OrderSide::Buy,
    )? {
        plans.push(plan);
    }

    let allow_short =
        snapshot.config.allow_short.unwrap_or(config.allow_short) && config.allow_short;
    if allow_short {
        if let Some(plan) = build_order_plan(
            config,
            snapshot,
            &indicators,
            liquidity,
            symbol_meta,
            OrderSide::Sell,
        )? {
            plans.push(plan);
        }
    }

    Ok(PlannedOrders {
        symbol: snapshot.config.symbol.clone(),
        range_score,
        plans,
    })
}

pub fn evaluate_range_conditions(indicators: &IndicatorOutputs, cfg: &IndicatorConfig) -> u8 {
    let mut passes = 0;
    if indicators.adx < cfg.adx.threshold {
        passes += 1;
    }
    if indicators.bbw_percentile < cfg.bbw.percentile {
        passes += 1;
    }
    if indicators.slope_metric < cfg.slope.threshold {
        passes += 1;
    }
    if let Some(ci) = indicators.choppiness {
        if let Some(chop_cfg) = &cfg.choppiness {
            if chop_cfg.enabled && ci >= chop_cfg.threshold {
                passes += 1;
            }
        }
    }
    passes
}

fn check_liquidity_constraints(
    config: &MeanReversionEngineConfig,
    snapshot: &SymbolSnapshot,
    liquidity: &LiquiditySnapshot,
    indicators: &IndicatorOutputs,
) -> bool {
    let volume_threshold = snapshot
        .config
        .min_quote_volume_5m
        .max(config.liquidity.min_recent_quote_volume);
    if indicators.recent_volume_quote < volume_threshold {
        return false;
    }

    if liquidity.maker_fee.unwrap_or(0.0) > config.liquidity.maker_fee_threshold {
        return false;
    }

    true
}

fn build_order_plan(
    config: &MeanReversionEngineConfig,
    snapshot: &SymbolSnapshot,
    indicators: &IndicatorOutputs,
    liquidity: &LiquiditySnapshot,
    symbol_meta: &SymbolMeta,
    side: OrderSide,
) -> Result<Option<OrderPlan>> {
    let boll = &indicators.bollinger_5m;
    let boll_cfg = &config.indicators.bollinger;
    let rsi_cfg = &config.indicators.rsi;

    let long_entry =
        boll.band_percent <= boll_cfg.entry_band_pct_long || boll.z_score <= boll_cfg.entry_z_long;
    let short_entry = boll.band_percent >= boll_cfg.entry_band_pct_short
        || boll.z_score >= boll_cfg.entry_z_short;
    let rsi_long = indicators.rsi <= rsi_cfg.long_threshold;
    let rsi_short = indicators.rsi >= rsi_cfg.short_threshold;

    match side {
        OrderSide::Buy if !(long_entry && rsi_long) => return Ok(None),
        OrderSide::Sell if !(short_entry && rsi_short) => return Ok(None),
        _ => {}
    }

    let price_ref = indicators.last_price.max(1e-6);
    let mid_price = if liquidity.bid_price > 0.0 && liquidity.ask_price > 0.0 {
        (liquidity.bid_price + liquidity.ask_price) / 2.0
    } else {
        price_ref
    };
    let spread_pct = if mid_price > 0.0 {
        liquidity.spread / mid_price
    } else {
        0.0
    };

    let exec_cfg = &config.execution;
    let base = exec_cfg.base_improve;
    let spread_component = 2.0 * spread_pct;
    let fee_component = liquidity.maker_fee.unwrap_or(0.0).abs() * 2.0;
    let atr_component = exec_cfg.alpha_atr * (indicators.atr / price_ref);
    let sigma_component =
        exec_cfg.beta_sigma * (indicators.bollinger_5m.sigma / price_ref.abs().max(1e-6));

    let improve = base
        .max(spread_component)
        .max(fee_component)
        .max(atr_component)
        .max(sigma_component)
        .max(0.0001);
    let improve = improve.min(spread_pct * 1.2 + base);
    if improve <= 0.0 {
        return Ok(None);
    }

    let direction = if side == OrderSide::Buy { 1.0 } else { -1.0 };
    let best_price = match side {
        OrderSide::Buy => liquidity.bid_price,
        OrderSide::Sell => liquidity.ask_price,
    };
    let reference_price = if best_price > 0.0 {
        best_price * (1.0 - direction * improve)
    } else if side == OrderSide::Buy {
        price_ref * (1.0 - improve)
    } else {
        price_ref * (1.0 + improve)
    };

    let limit_price = round_price(
        reference_price,
        symbol_meta.tick_size,
        symbol_meta.price_precision,
        &side,
    )?;
    if limit_price <= 0.0 {
        return Ok(None);
    }

    let stop_distance = config.indicators.atr.initial_stop_k * indicators.atr;
    if stop_distance <= 0.0 {
        return Ok(None);
    }

    let stop_price = limit_price - direction * stop_distance;
    if stop_price <= 0.0 {
        return Ok(None);
    }

    let trailing_distance = config.indicators.atr.trailing_stop_k * indicators.atr;
    let risk_qty = config.risk.per_trade_risk / stop_distance.abs().max(1e-9);
    let notional_limit_qty = config.risk.per_trade_notional / limit_price;
    let mut quantity = risk_qty.min(notional_limit_qty);

    let step_size = symbol_meta.step_size.max(0.0);
    if step_size > 0.0 {
        quantity = quantity.max(step_size);
    }
    quantity = quantity.max(symbol_meta.min_order_size);
    quantity = ceil_to_step(
        quantity,
        symbol_meta.step_size,
        symbol_meta.amount_precision,
    );

    if let Some(min_notional) = symbol_meta.min_notional {
        let required_qty = ceil_to_step(
            min_notional / limit_price,
            symbol_meta.step_size,
            symbol_meta.amount_precision,
        );
        quantity = quantity.max(required_qty);
    }
    quantity = ceil_to_step(
        quantity,
        symbol_meta.step_size,
        symbol_meta.amount_precision,
    );

    if quantity <= 0.0 || !quantity.is_finite() {
        return Ok(None);
    }

    let notional = quantity * limit_price;
    if let Some(min_notional) = symbol_meta.min_notional {
        if notional + f64::EPSILON < min_notional {
            return Ok(None);
        }
    }
    if notional <= 0.0 {
        return Ok(None);
    }

    let depth_multiplier = snapshot
        .config
        .depth_multiplier
        .max(config.liquidity.depth_multiplier);
    let depth_ok = match side {
        OrderSide::Buy => liquidity.total_bid_depth >= notional * depth_multiplier,
        OrderSide::Sell => liquidity.total_ask_depth >= notional * depth_multiplier,
    };
    if !depth_ok {
        return Ok(None);
    }

    let tp_distance = (config.execution.take_profit_atr_k * indicators.atr).max(0.0);
    let take_profit = limit_price + direction * tp_distance;

    Ok(Some(OrderPlan {
        symbol: snapshot.config.symbol.clone(),
        side,
        quantity,
        limit_price,
        stop_price,
        trailing_distance,
        take_profit,
        improve,
        indicators: indicators.clone(),
        liquidity: liquidity.clone(),
    }))
}

pub fn compute_indicators(
    snapshot: &SymbolSnapshot,
    cfg: &IndicatorConfig,
) -> Result<IndicatorOutputs> {
    if snapshot.five_minute.len() < cfg.bollinger.period + 2 {
        return Err(anyhow!("not enough 5m data"));
    }
    if snapshot.fifteen_minute.len() < cfg.bollinger.period + 2 {
        return Err(anyhow!("not enough 15m data"));
    }
    if snapshot.one_hour.len() < cfg.bollinger.period + 2 {
        return Err(anyhow!("not enough 1h data"));
    }

    let closes_5m: Vec<f64> = snapshot.five_minute.iter().map(|k| k.close).collect();
    let highs_5m: Vec<f64> = snapshot.five_minute.iter().map(|k| k.high).collect();
    let lows_5m: Vec<f64> = snapshot.five_minute.iter().map(|k| k.low).collect();

    let boll_5m = bollinger_bands(&closes_5m, cfg.bollinger.period, cfg.bollinger.std_dev)
        .ok_or_else(|| anyhow!("5m bollinger calculation failed"))?;
    let sigma_5m = (boll_5m.0 - boll_5m.1) / cfg.bollinger.std_dev.max(1e-9);
    let last_price = closes_5m.last().copied().unwrap_or_default();
    let denom_5m = (boll_5m.0 - boll_5m.2).max(1e-9);
    let band_percent_5m = (last_price - boll_5m.2) / denom_5m;
    let z_score_5m = if sigma_5m.abs() < 1e-9 {
        0.0
    } else {
        (last_price - boll_5m.1) / sigma_5m
    };

    let atr = atr(&highs_5m, &lows_5m, &closes_5m, cfg.atr.period)
        .ok_or_else(|| anyhow!("5m ATR calculation failed"))?;
    let rsi =
        rsi(&closes_5m, cfg.rsi.period).ok_or_else(|| anyhow!("5m RSI calculation failed"))?;

    let closes_15m: Vec<f64> = snapshot.fifteen_minute.iter().map(|k| k.close).collect();
    let boll_15m = bollinger_bands(&closes_15m, cfg.bollinger.period, cfg.bollinger.std_dev)
        .ok_or_else(|| anyhow!("15m bollinger calculation failed"))?;
    let sigma_15m = (boll_15m.0 - boll_15m.1) / cfg.bollinger.std_dev.max(1e-9);
    let denom_15m = (boll_15m.0 - boll_15m.2).max(1e-9);
    let last_price_15m = closes_15m.last().copied().unwrap_or(last_price);
    let band_percent_15m = (last_price_15m - boll_15m.2) / denom_15m;
    let z_score_15m = if sigma_15m.abs() < 1e-9 {
        0.0
    } else {
        (last_price_15m - boll_15m.1) / sigma_15m
    };

    let closes_1h: Vec<f64> = snapshot.one_hour.iter().map(|k| k.close).collect();
    let highs_1h: Vec<f64> = snapshot.one_hour.iter().map(|k| k.high).collect();
    let lows_1h: Vec<f64> = snapshot.one_hour.iter().map(|k| k.low).collect();
    let boll_1h = bollinger_bands(&closes_1h, cfg.bollinger.period, cfg.bollinger.std_dev)
        .ok_or_else(|| anyhow!("1h bollinger calculation failed"))?;
    let bbw = if boll_1h.1.abs() < 1e-9 {
        0.0
    } else {
        (boll_1h.0 - boll_1h.2) / boll_1h.1
    };
    let bbw_percentile = percentile_rank(bbw, &VecDeque::from(snapshot.bbw_history.clone()));
    let slope_metric = compute_slope_metric(
        &VecDeque::from(snapshot.mid_history.clone()),
        &VecDeque::from(snapshot.sigma_history.clone()),
        cfg,
    );
    let adx = adx(&highs_1h, &lows_1h, &closes_1h, cfg.adx.period).unwrap_or_default();
    let choppiness = cfg
        .choppiness
        .as_ref()
        .filter(|c| c.enabled)
        .and_then(|c| compute_choppiness(&highs_1h, &lows_1h, &closes_1h, c.lookback))
        .filter(|ci| !ci.is_nan());

    let volume_window_minutes = cfg.volume.lookback_minutes.max(15);
    let volume_bars = ((volume_window_minutes as f64) / 15.0).ceil() as usize;
    let recent_volume_quote = snapshot
        .fifteen_minute
        .iter()
        .rev()
        .take(volume_bars)
        .map(|k| k.quote_volume)
        .sum();

    Ok(IndicatorOutputs {
        timestamp: snapshot
            .five_minute
            .last()
            .map(|k| k.close_time)
            .unwrap_or_else(Utc::now),
        last_price,
        bollinger_5m: BollingerSnapshot {
            upper: boll_5m.0,
            middle: boll_5m.1,
            lower: boll_5m.2,
            sigma: sigma_5m,
            band_percent: band_percent_5m,
            z_score: z_score_5m,
        },
        bollinger_15m: BollingerSnapshot {
            upper: boll_15m.0,
            middle: boll_15m.1,
            lower: boll_15m.2,
            sigma: sigma_15m,
            band_percent: band_percent_15m,
            z_score: z_score_15m,
        },
        rsi,
        atr,
        adx,
        bbw,
        bbw_percentile,
        slope_metric,
        choppiness,
        recent_volume_quote,
        volume_window_minutes,
    })
}

fn compute_slope_metric(
    mid_history: &VecDeque<f64>,
    sigma_history: &VecDeque<f64>,
    cfg: &IndicatorConfig,
) -> f64 {
    if mid_history.len() < 2 || sigma_history.is_empty() {
        return 1.0;
    }
    let latest = mid_history.back().copied().unwrap_or_default();
    let prev_idx = mid_history.len().saturating_sub(cfg.slope.lookback.max(1));
    let prev = mid_history.get(prev_idx).copied().unwrap_or(latest);
    let sigma = sigma_history.back().copied().unwrap_or(1.0).abs().max(1e-9);
    ((latest - prev).abs() / sigma).abs()
}

fn compute_choppiness(highs: &[f64], lows: &[f64], closes: &[f64], lookback: usize) -> Option<f64> {
    if highs.len() < lookback + 1 || lookback < 2 {
        return None;
    }
    let start = highs.len() - lookback;
    let mut tr_sum = 0.0;
    for i in start..highs.len() {
        tr_sum += true_range(highs, lows, closes, i);
    }
    let slice_high = highs[start..].iter().fold(f64::MIN, |a, &b| a.max(b));
    let slice_low = lows[start..].iter().fold(f64::MAX, |a, &b| a.min(b));
    let denom = (slice_high - slice_low).abs().max(1e-9);
    Some(100.0 * ((tr_sum / denom).log10() / (lookback as f64).log10()))
}

fn true_range(highs: &[f64], lows: &[f64], closes: &[f64], idx: usize) -> f64 {
    if idx == 0 {
        return highs[0] - lows[0];
    }
    let high_low = highs[idx] - lows[idx];
    let high_close = (highs[idx] - closes[idx - 1]).abs();
    let low_close = (lows[idx] - closes[idx - 1]).abs();
    high_low.max(high_close).max(low_close)
}

fn percentile_rank(value: f64, history: &VecDeque<f64>) -> f64 {
    if history.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f64> = history.iter().copied().collect();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let rank = sorted.iter().filter(|&&val| val <= value).count() as f64 / sorted.len() as f64;
    rank.clamp(0.0, 1.0)
}

fn bollinger_bands(data: &[f64], period: usize, std_dev: f64) -> Option<(f64, f64, f64)> {
    if data.len() < period || period == 0 {
        return None;
    }
    let slice = &data[data.len() - period..];
    let mean = slice.iter().sum::<f64>() / period as f64;
    let variance = slice
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / period as f64;
    let sigma = variance.sqrt();
    Some((mean + std_dev * sigma, mean, mean - std_dev * sigma))
}

fn rsi(data: &[f64], period: usize) -> Option<f64> {
    if data.len() <= period || period == 0 {
        return None;
    }
    let start = data.len() - period;
    let mut gains = 0.0;
    let mut losses = 0.0;
    for window in data[start - 1..].windows(2) {
        let change = window[1] - window[0];
        if change >= 0.0 {
            gains += change;
        } else {
            losses -= change;
        }
    }
    if losses == 0.0 {
        return Some(100.0);
    }
    let rs = gains / losses;
    Some(100.0 - (100.0 / (1.0 + rs)))
}

fn atr(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
    if highs.len() != lows.len() || highs.len() != closes.len() || highs.len() <= period {
        return None;
    }
    let start = highs.len() - period;
    let sum = (start..highs.len())
        .map(|idx| true_range(highs, lows, closes, idx))
        .sum::<f64>();
    Some(sum / period as f64)
}

fn adx(highs: &[f64], lows: &[f64], closes: &[f64], period: usize) -> Option<f64> {
    if highs.len() != lows.len() || highs.len() != closes.len() || highs.len() <= period + 1 {
        return None;
    }
    let start = highs.len() - period;
    let mut plus_dm = 0.0;
    let mut minus_dm = 0.0;
    let mut tr = 0.0;
    for idx in start..highs.len() {
        let up_move = highs[idx] - highs[idx - 1];
        let down_move = lows[idx - 1] - lows[idx];
        if up_move > down_move && up_move > 0.0 {
            plus_dm += up_move;
        }
        if down_move > up_move && down_move > 0.0 {
            minus_dm += down_move;
        }
        tr += true_range(highs, lows, closes, idx);
    }
    if tr <= 0.0 {
        return Some(0.0);
    }
    let plus_di = 100.0 * plus_dm / tr;
    let minus_di = 100.0 * minus_dm / tr;
    let denom = (plus_di + minus_di).abs();
    if denom <= f64::EPSILON {
        return Some(0.0);
    }
    Some(100.0 * ((plus_di - minus_di).abs() / denom))
}
