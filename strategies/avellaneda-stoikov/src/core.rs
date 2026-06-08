use std::collections::HashMap;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use rustcta_strategy_sdk::{MarketType, OrderSide};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ASCoreConfig {
    pub strategy: StrategyConfig,
    pub account: AccountConfig,
    pub trading: TradingConfig,
    pub as_params: ASParameters,
    pub market_data: MarketDataConfig,
    pub risk: RiskConfig,
    pub performance: PerformanceConfig,
    pub data_sources: DataSourcesConfig,
    pub monitoring: MonitoringConfig,
    pub perpetual: PerpetualConfig,
    #[serde(default)]
    pub market_specific: Option<MarketSpecificConfig>,
    #[serde(default)]
    pub debug: Option<DebugConfig>,
}

impl ASCoreConfig {
    pub fn validate_core(&self) -> Result<()> {
        if self.trading.order_size_usdc <= 0.0 {
            return Err(anyhow!("trading.order_size_usdc must be positive"));
        }
        if self.trading.max_inventory <= 0.0 {
            return Err(anyhow!("trading.max_inventory must be positive"));
        }
        if self.trading.min_spread_bp <= 0.0
            || self.trading.max_spread_bp < self.trading.min_spread_bp
        {
            return Err(anyhow!(
                "invalid spread bounds min={} max={}",
                self.trading.min_spread_bp,
                self.trading.max_spread_bp
            ));
        }
        if self.as_params.risk_aversion <= 0.0 {
            return Err(anyhow!("as_params.risk_aversion must be positive"));
        }
        if self.as_params.order_book_intensity <= 0.0 {
            return Err(anyhow!("as_params.order_book_intensity must be positive"));
        }
        let vol = &self.as_params.volatility;
        if vol.lookback_periods < 2 {
            return Err(anyhow!("volatility.lookback_periods must be >= 2"));
        }
        if vol.update_interval == 0 {
            return Err(anyhow!("volatility.update_interval must be positive"));
        }
        if !(0.0..=1.0).contains(&vol.decay_factor) || vol.decay_factor == 0.0 {
            return Err(anyhow!("volatility.decay_factor must be in (0, 1]"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub name: String,
    pub version: String,
    #[serde(rename = "type")]
    pub strategy_type: String,
    pub description: String,
    pub enabled: bool,
    pub log_level: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountConfig {
    pub account_id: String,
    pub exchange: String,
    #[serde(default)]
    pub connection_profile_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradingConfig {
    pub symbol: String,
    pub market_type: String,
    pub order_size_usdc: f64,
    pub max_inventory: f64,
    pub min_spread_bp: f64,
    pub max_spread_bp: f64,
    pub refresh_interval_secs: u64,
    pub price_precision: u32,
    pub quantity_precision: u32,
    pub order_config: OrderConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderConfig {
    pub post_only: bool,
    pub time_in_force: String,
    pub reduce_only: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ASParameters {
    pub risk_aversion: f64,
    pub order_book_intensity: f64,
    pub time_horizon_seconds: u64,
    pub volatility: VolatilityConfig,
    pub inventory_skew: InventorySkewConfig,
    pub spread_adjustment: SpreadAdjustmentConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolatilityConfig {
    pub lookback_periods: usize,
    pub update_interval: u64,
    pub decay_factor: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InventorySkewConfig {
    pub enabled: bool,
    pub skew_factor: f64,
    pub target_inventory_ratio: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpreadAdjustmentConfig {
    pub volume_factor: f64,
    pub depth_factor: f64,
    pub pressure_factor: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketDataConfig {
    pub orderbook_levels: usize,
    pub trades_buffer_size: usize,
    pub kline: KlineConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KlineConfig {
    pub interval: String,
    pub history_size: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskConfig {
    pub max_unrealized_loss: f64,
    pub max_daily_loss: f64,
    pub inventory_risk: InventoryRiskConfig,
    pub stop_loss: StopLossConfig,
    pub volatility_limits: VolatilityLimitsConfig,
    pub emergency_stop: EmergencyStopConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InventoryRiskConfig {
    pub max_position_value: f64,
    pub imbalance_threshold: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StopLossConfig {
    pub enabled: bool,
    pub stop_loss_pct: f64,
    pub cooldown_seconds: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolatilityLimitsConfig {
    pub max_volatility: f64,
    pub min_volatility: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmergencyStopConfig {
    pub consecutive_losses: u32,
    pub max_drawdown_pct: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub order_aggregation: OrderAggregationConfig,
    pub smart_cancellation: SmartCancellationConfig,
    pub partial_fill: PartialFillConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderAggregationConfig {
    pub enabled: bool,
    pub min_price_diff_bp: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SmartCancellationConfig {
    pub enabled: bool,
    pub price_drift_threshold_bp: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartialFillConfig {
    pub min_fill_ratio: f64,
    pub immediate_replace: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataSourcesConfig {
    pub websocket: WebSocketConfig,
    pub rest_api: RestApiConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebSocketConfig {
    pub enabled: bool,
    pub streams: Vec<String>,
    pub reconnect_interval: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestApiConfig {
    pub enabled: bool,
    pub refresh_interval: u64,
    pub endpoints: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub metrics: MetricsConfig,
    pub health_check: HealthCheckConfig,
    pub trade_logging: TradeLoggingConfig,
    pub alerts: AlertsConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub export_interval: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    pub enabled: bool,
    pub interval: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeLoggingConfig {
    pub enabled: bool,
    pub log_fills: bool,
    pub log_quotes: bool,
    pub log_cancellations: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertsConfig {
    pub enabled: bool,
    pub channels: Vec<String>,
    pub thresholds: AlertThresholds,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertThresholds {
    pub inventory_imbalance: f64,
    pub loss_limit: f64,
    pub low_liquidity: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PerpetualConfig {
    pub funding_rate: FundingRateConfig,
    pub maintenance: MaintenanceConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FundingRateConfig {
    pub consider_funding: bool,
    pub threshold_pct: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceConfig {
    pub stop_before: u64,
    pub resume_after: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketSpecificConfig {
    pub volatility_handling: Option<VolatilityHandlingConfig>,
    pub liquidity_check: Option<LiquidityCheckConfig>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolatilityHandlingConfig {
    pub adaptive_spread: bool,
    pub volatility_multiplier: VolatilityMultiplierConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolatilityMultiplierConfig {
    pub low: f64,
    pub medium: f64,
    pub high: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiquidityCheckConfig {
    pub enabled: bool,
    pub min_depth_usd: f64,
    pub low_liquidity_spread_multiplier: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DebugConfig {
    pub enabled: bool,
    pub verbose_logging: bool,
    pub dry_run: bool,
    pub save_market_data: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketRules {
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: f64,
    pub min_order_size: f64,
    pub max_order_size: f64,
    pub price_digits: u32,
    pub qty_digits: u32,
    pub market_type: MarketType,
}

impl MarketRules {
    pub fn from_config(
        price_precision: u32,
        quantity_precision: u32,
        market_type: MarketType,
    ) -> Self {
        Self {
            tick_size: 10_f64.powi(-(price_precision as i32)),
            step_size: 10_f64.powi(-(quantity_precision as i32)),
            min_notional: 0.0,
            min_order_size: 0.0,
            max_order_size: f64::MAX,
            price_digits: price_precision,
            qty_digits: quantity_precision,
            market_type,
        }
    }

    pub fn from_exchange(
        tick_size: f64,
        step_size: f64,
        min_notional: f64,
        min_order_size: f64,
        max_order_size: f64,
        market_type: MarketType,
    ) -> Self {
        Self {
            tick_size,
            step_size,
            min_notional,
            min_order_size,
            max_order_size: if max_order_size > 0.0 {
                max_order_size
            } else {
                f64::MAX
            },
            price_digits: infer_digits(tick_size),
            qty_digits: infer_digits(step_size),
            market_type,
        }
    }

    pub fn round_bid(&self, price: f64) -> f64 {
        round_down(price, self.tick_size, self.price_digits)
    }

    pub fn round_ask(&self, price: f64) -> f64 {
        round_up(price, self.tick_size, self.price_digits)
    }

    pub fn round_qty(&self, qty: f64) -> f64 {
        round_down(qty, self.step_size, self.qty_digits)
    }
}

impl Default for MarketRules {
    fn default() -> Self {
        Self::from_config(4, 4, MarketType::Spot)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ASOrderStatus {
    Open,
    PartiallyFilled,
    Closed,
    Canceled,
    Expired,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ASOrderState {
    pub exchange_order_id: String,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub price: f64,
    pub original_qty: f64,
    pub filled_qty: f64,
    pub status: ASOrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ASOrderState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            ASOrderStatus::Closed
                | ASOrderStatus::Canceled
                | ASOrderStatus::Expired
                | ASOrderStatus::Rejected
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ASState {
    pub mid_price: f64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub volatility: f64,
    pub inventory: f64,
    pub inventory_value: f64,
    pub active_buy_order: Option<String>,
    pub active_sell_order: Option<String>,
    pub last_buy_price: f64,
    pub last_sell_price: f64,
    pub open_orders: HashMap<String, ASOrderState>,
    pub total_trades: u64,
    pub buy_fills: u64,
    pub sell_fills: u64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub fees_paid: f64,
    pub avg_entry_price: f64,
    pub equity_peak: f64,
    pub consecutive_losses: u32,
    pub daily_pnl: f64,
    pub max_drawdown: f64,
    pub strategy_start_time: DateTime<Utc>,
    pub last_update_time: DateTime<Utc>,
    pub last_volatility_update: DateTime<Utc>,
}

impl ASState {
    pub fn new(mid_price: f64) -> Self {
        let now = Utc::now();
        Self {
            mid_price,
            bid_price: mid_price,
            ask_price: mid_price,
            volatility: 0.001,
            inventory: 0.0,
            inventory_value: 0.0,
            active_buy_order: None,
            active_sell_order: None,
            last_buy_price: 0.0,
            last_sell_price: 0.0,
            open_orders: HashMap::new(),
            total_trades: 0,
            buy_fills: 0,
            sell_fills: 0,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            fees_paid: 0.0,
            avg_entry_price: 0.0,
            equity_peak: 0.0,
            consecutive_losses: 0,
            daily_pnl: 0.0,
            max_drawdown: 0.0,
            strategy_start_time: now,
            last_update_time: now,
            last_volatility_update: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ASQuote {
    pub reservation_price: f64,
    pub optimal_spread: f64,
    pub bid: f64,
    pub ask: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ASRiskSnapshot {
    pub notional: f64,
    pub net_inventory: f64,
    pub inventory_ratio: Option<f64>,
    pub unrealized_pnl: f64,
    pub daily_pnl: f64,
    pub max_drawdown: f64,
    pub consecutive_losses: u32,
}

pub fn calculate_reservation_price(config: &ASCoreConfig, state: &ASState) -> f64 {
    let gamma = config.as_params.risk_aversion;
    let sigma = state.volatility;
    let time_to_end = config.as_params.time_horizon_seconds as f64;
    let reservation_adjustment = state.inventory * gamma * sigma.powi(2) * time_to_end / 3600.0;
    state.mid_price - reservation_adjustment
}

pub fn calculate_optimal_spread(config: &ASCoreConfig, state: &ASState) -> f64 {
    let gamma = config.as_params.risk_aversion;
    let k = config.as_params.order_book_intensity;
    let sigma = state.volatility;
    let time_to_end = config.as_params.time_horizon_seconds as f64;
    let time_component = gamma * sigma.powi(2) * time_to_end / 3600.0;
    let intensity_component = (2.0 / gamma) * (1.0 + gamma / k).ln();
    let mut base_spread = time_component + intensity_component;

    if let Some(volatility_handling) = config
        .market_specific
        .as_ref()
        .and_then(|market_specific| market_specific.volatility_handling.as_ref())
    {
        if volatility_handling.adaptive_spread {
            let daily_vol = state.volatility / 365.0_f64.sqrt();
            let multiplier = if daily_vol < 0.05 {
                volatility_handling.volatility_multiplier.low
            } else if daily_vol < 0.08 {
                volatility_handling.volatility_multiplier.medium
            } else {
                volatility_handling.volatility_multiplier.high
            };
            base_spread *= multiplier.max(1.0);
        }
    }

    let spread_bp = base_spread * 10_000.0;
    let clamped_spread_bp = spread_bp
        .max(config.trading.min_spread_bp)
        .min(config.trading.max_spread_bp);
    clamped_spread_bp / 10_000.0
}

pub fn calculate_volatility(
    prices: &[f64],
    volatility_config: &VolatilityConfig,
    volatility_limits: &VolatilityLimitsConfig,
) -> f64 {
    let min_vol = volatility_limits.min_volatility.max(1.0) / 100.0;
    if prices.len() < volatility_config.lookback_periods {
        return min_vol;
    }

    let mut returns = Vec::with_capacity(prices.len().saturating_sub(1));
    for index in 1..prices.len() {
        if prices[index] > 0.0 && prices[index - 1] > 0.0 {
            let ret = (prices[index] / prices[index - 1]).ln();
            if ret.is_finite() {
                returns.push(ret);
            }
        }
    }
    if returns.is_empty() {
        return min_vol;
    }

    let mut weighted_sum = 0.0;
    let mut weight_sum = 0.0;
    let mut weight = 1.0;
    for ret in returns.iter().rev() {
        weighted_sum += weight * ret.powi(2);
        weight_sum += weight;
        weight *= volatility_config.decay_factor;
    }
    if weight_sum <= 0.0 {
        return min_vol;
    }

    let variance = weighted_sum / weight_sum;
    let volatility = variance.sqrt();
    let periods_per_year = 365.0 * 24.0 * 3600.0 / volatility_config.update_interval as f64;
    (volatility * periods_per_year.sqrt()).max(0.0001)
}

pub fn generate_quotes(
    config: &ASCoreConfig,
    state: &ASState,
    rules: &MarketRules,
    liquidity_multiplier: f64,
) -> Result<ASQuote> {
    if state.mid_price <= 0.0 || !state.mid_price.is_finite() {
        return Err(anyhow!(
            "invalid mid price for quote generation: {}",
            state.mid_price
        ));
    }
    let reservation_price = calculate_reservation_price(config, state);
    let optimal_spread = calculate_optimal_spread(config, state);
    let half_spread = optimal_spread / 2.0;
    let mut bid_adjustment = 0.0;
    let mut ask_adjustment = 0.0;
    if config.as_params.inventory_skew.enabled && config.trading.max_inventory > 0.0 {
        let inventory_ratio = state.inventory / config.trading.max_inventory;
        let target_ratio = config.as_params.inventory_skew.target_inventory_ratio;
        let skew_factor = config.as_params.inventory_skew.skew_factor;
        let inventory_imbalance = inventory_ratio - target_ratio;
        if inventory_imbalance > 0.0 {
            bid_adjustment = -inventory_imbalance * skew_factor * half_spread;
            ask_adjustment = inventory_imbalance * skew_factor * half_spread * 0.5;
        } else {
            bid_adjustment = -inventory_imbalance * skew_factor * half_spread * 0.5;
            ask_adjustment = inventory_imbalance * skew_factor * half_spread;
        }
    }

    let raw_bid = reservation_price - half_spread + bid_adjustment;
    let raw_ask = reservation_price + half_spread + ask_adjustment;
    let min_tick = rules
        .tick_size
        .max(10_f64.powi(-(rules.price_digits as i32)));
    let multiplier = liquidity_multiplier.max(1.0);
    let adjusted_bid = (reservation_price - half_spread * multiplier + bid_adjustment)
        .min(state.mid_price)
        .min(raw_bid);
    let adjusted_ask = (reservation_price + half_spread * multiplier + ask_adjustment)
        .max(state.mid_price)
        .max(raw_ask);
    let bid = rules.round_bid(adjusted_bid.min(adjusted_ask - min_tick));
    let ask = rules.round_ask(adjusted_ask.max(bid + min_tick));

    if bid <= 0.0 || ask <= bid {
        return Err(anyhow!(
            "invalid quotes after precision: bid={} ask={} mid={} tick={}",
            bid,
            ask,
            state.mid_price,
            rules.tick_size
        ));
    }

    Ok(ASQuote {
        reservation_price,
        optimal_spread,
        bid,
        ask,
    })
}

pub fn liquidity_spread_multiplier(
    liquidity_check: Option<&LiquidityCheckConfig>,
    top_depth_usd: f64,
) -> f64 {
    let Some(liquidity_check) = liquidity_check else {
        return 1.0;
    };
    if !liquidity_check.enabled
        || top_depth_usd <= 0.0
        || top_depth_usd >= liquidity_check.min_depth_usd
    {
        return 1.0;
    }
    liquidity_check.low_liquidity_spread_multiplier.max(1.0)
}

pub fn calculate_order_qty(order_size_usdc: f64, price: f64, rules: &MarketRules) -> Result<f64> {
    if order_size_usdc <= 0.0 || price <= 0.0 || !price.is_finite() {
        return Err(anyhow!(
            "invalid order sizing order_size_usdc={} price={}",
            order_size_usdc,
            price
        ));
    }
    let mut qty = order_size_usdc / price;
    qty = qty.max(rules.min_order_size);
    qty = rules.round_qty(qty);

    if rules.min_notional > 0.0 && qty * price + 1e-9 < rules.min_notional {
        qty = round_up(
            rules.min_notional / price,
            rules.step_size,
            rules.qty_digits,
        );
    }
    if qty <= 0.0 || !qty.is_finite() {
        return Err(anyhow!("calculated invalid order qty: {}", qty));
    }
    if qty > rules.max_order_size {
        return Err(anyhow!(
            "calculated qty {} exceeds max_order_size {}",
            qty,
            rules.max_order_size
        ));
    }
    if rules.min_notional > 0.0 && qty * price + 1e-9 < rules.min_notional {
        return Err(anyhow!(
            "order notional {:.8} below min_notional {:.8}",
            qty * price,
            rules.min_notional
        ));
    }
    Ok(qty)
}

pub fn apply_fill_to_state(
    state: &mut ASState,
    side: OrderSide,
    qty: f64,
    price: f64,
    commission: f64,
) {
    if qty <= 0.0 || price <= 0.0 {
        return;
    }

    let previous_equity = state.realized_pnl + state.unrealized_pnl;
    let signed_qty = match side {
        OrderSide::Buy => qty,
        OrderSide::Sell => -qty,
    };
    apply_position_fill(state, signed_qty, price);
    state.inventory_value = (state.inventory * state.mid_price).abs();
    state.fees_paid += commission.max(0.0);
    state.realized_pnl -= commission.max(0.0);
    state.total_trades += 1;

    match side {
        OrderSide::Buy => state.buy_fills += 1,
        OrderSide::Sell => state.sell_fills += 1,
    }

    refresh_drawdown(state);
    let new_equity = state.realized_pnl + state.unrealized_pnl;
    if new_equity < previous_equity {
        state.consecutive_losses += 1;
    } else {
        state.consecutive_losses = 0;
    }
}

pub fn refresh_unrealized_pnl(state: &mut ASState) {
    state.inventory_value = (state.inventory * state.mid_price).abs();
    if state.inventory.abs() > 0.0 && state.avg_entry_price > 0.0 {
        state.unrealized_pnl = if state.inventory > 0.0 {
            (state.mid_price - state.avg_entry_price) * state.inventory.abs()
        } else {
            (state.avg_entry_price - state.mid_price) * state.inventory.abs()
        };
    }
    refresh_drawdown(state);
}

pub fn build_risk_snapshot(config: &ASCoreConfig, state: &ASState) -> ASRiskSnapshot {
    ASRiskSnapshot {
        notional: state.inventory_value,
        net_inventory: state.inventory,
        inventory_ratio: (config.risk.inventory_risk.max_position_value > 0.0).then_some(
            (state.inventory_value.abs() / config.risk.inventory_risk.max_position_value).min(1.0),
        ),
        unrealized_pnl: state.unrealized_pnl,
        daily_pnl: state.daily_pnl,
        max_drawdown: state.max_drawdown,
        consecutive_losses: state.consecutive_losses,
    }
}

pub fn symbol_matches(exchange_symbol: &str, configured_symbol: &str) -> bool {
    exchange_symbol.eq_ignore_ascii_case(configured_symbol)
        || exchange_symbol.eq_ignore_ascii_case(&configured_symbol.replace('/', ""))
}

fn apply_position_fill(state: &mut ASState, signed_qty: f64, price: f64) {
    let old_qty = state.inventory;
    let new_qty = old_qty + signed_qty;
    if old_qty.abs() < f64::EPSILON || old_qty.signum() == signed_qty.signum() {
        let total_abs = old_qty.abs() + signed_qty.abs();
        if total_abs > 0.0 {
            state.avg_entry_price =
                (state.avg_entry_price * old_qty.abs() + price * signed_qty.abs()) / total_abs;
        }
    } else {
        let closing_qty = old_qty.abs().min(signed_qty.abs());
        let pnl_per_unit = if old_qty > 0.0 {
            price - state.avg_entry_price
        } else {
            state.avg_entry_price - price
        };
        state.realized_pnl += pnl_per_unit * closing_qty;

        if new_qty.abs() < f64::EPSILON {
            state.avg_entry_price = 0.0;
        } else if old_qty.signum() != new_qty.signum() {
            state.avg_entry_price = price;
        }
    }
    state.inventory = if new_qty.abs() < 1e-12 { 0.0 } else { new_qty };
}

fn refresh_drawdown(state: &mut ASState) {
    let equity = state.realized_pnl + state.unrealized_pnl;
    if equity > state.equity_peak {
        state.equity_peak = equity;
    }
    if state.equity_peak > 0.0 {
        state.max_drawdown = ((state.equity_peak - equity) / state.equity_peak).max(0.0);
    }
}

fn infer_digits(step: f64) -> u32 {
    if step <= 0.0 || !step.is_finite() {
        return 8;
    }
    let mut places = 0;
    let mut scaled = step.abs();
    while places < 12 && scaled.fract().abs() > 1e-12 {
        scaled *= 10.0;
        places += 1;
    }
    places
}

fn round_down(value: f64, step: f64, digits: u32) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    let rounded = if step > 0.0 {
        (value / step).floor() * step
    } else {
        value
    };
    truncate_digits(rounded, digits)
}

fn round_up(value: f64, step: f64, digits: u32) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    let rounded = if step > 0.0 {
        (value / step).ceil() * step
    } else {
        value
    };
    truncate_digits(rounded, digits)
}

fn truncate_digits(value: f64, digits: u32) -> f64 {
    let factor = 10_f64.powi(digits as i32);
    (value * factor).round() / factor
}
