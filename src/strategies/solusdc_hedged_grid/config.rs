use serde::{Deserialize, Serialize};

use crate::core::types::OrderSide;

use crate::core::types::MarketType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub symbol: String,
    #[serde(default = "default_require_hedge_mode")]
    pub require_hedge_mode: bool,
    #[serde(default)]
    pub price_reference: PriceReference,
    #[serde(default)]
    pub risk_reference: RiskReference,
    pub grid: GridConfig,
    pub follow: FollowConfig,
    pub execution: ExecutionConfig,
    pub precision: PrecisionConfig,
    pub fees: FeeConfig,
    pub risk: RiskLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyMeta {
    pub name: String,
    #[serde(default)]
    pub log_level: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub account_id: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollingConfig {
    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: u64,
    #[serde(default = "default_reconcile_interval_ms")]
    pub reconcile_interval_ms: u64,
    #[serde(default = "default_trade_fetch_limit")]
    pub trade_fetch_limit: u32,
    #[serde(default = "default_request_timeout_secs")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_watchdog_timeout_secs")]
    pub watchdog_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketRuntimeConfig {
    #[serde(default = "default_ws_enabled")]
    pub enabled: bool,
    #[serde(default = "default_ws_keepalive_interval_secs")]
    pub keepalive_interval_secs: u64,
    #[serde(default = "default_ws_reconnect_delay_ms")]
    pub reconnect_delay_ms: u64,
    #[serde(default = "default_ws_max_reconnect_delay_ms")]
    pub max_reconnect_delay_ms: u64,
}

impl Default for WebSocketRuntimeConfig {
    fn default() -> Self {
        Self {
            enabled: default_ws_enabled(),
            keepalive_interval_secs: default_ws_keepalive_interval_secs(),
            reconnect_delay_ms: default_ws_reconnect_delay_ms(),
            max_reconnect_delay_ms: default_ws_max_reconnect_delay_ms(),
        }
    }
}

impl Default for PollingConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: default_tick_interval_ms(),
            reconcile_interval_ms: default_reconcile_interval_ms(),
            trade_fetch_limit: default_trade_fetch_limit(),
            request_timeout_secs: default_request_timeout_secs(),
            watchdog_timeout_secs: default_watchdog_timeout_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeExecutionConfig {
    #[serde(default = "default_startup_cancel_all")]
    pub startup_cancel_all: bool,
    #[serde(default = "default_shutdown_cancel_all")]
    pub shutdown_cancel_all: bool,
    #[serde(default = "default_max_consecutive_failures")]
    pub max_consecutive_failures: u32,
}

impl Default for RuntimeExecutionConfig {
    fn default() -> Self {
        Self {
            startup_cancel_all: default_startup_cancel_all(),
            shutdown_cancel_all: default_shutdown_cancel_all(),
            max_consecutive_failures: default_max_consecutive_failures(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    pub strategy: StrategyMeta,
    pub account: AccountConfig,
    pub engine: StrategyConfig,
    #[serde(default)]
    pub polling: PollingConfig,
    #[serde(default)]
    pub websocket: WebSocketRuntimeConfig,
    #[serde(default)]
    pub execution: RuntimeExecutionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridConfig {
    pub levels_per_side: usize,
    #[serde(default = "default_grid_spacing_pct")]
    pub grid_spacing_pct: f64,
    #[serde(default)]
    pub grid_spacing_abs: Option<f64>,
    pub order_notional: f64,
    #[serde(default)]
    pub order_qty: Option<f64>,
    #[serde(default = "default_strict_pairing")]
    pub strict_pairing: bool,
    #[serde(default = "default_fill_remaining_slots_with_opens")]
    pub fill_remaining_slots_with_opens: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FollowConfig {
    #[serde(default = "default_max_gap_steps")]
    pub max_gap_steps: f64,
    #[serde(default = "default_follow_cooldown_ms")]
    pub follow_cooldown_ms: u64,
    #[serde(default = "default_max_follow_actions_per_minute")]
    pub max_follow_actions_per_minute: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "default_cooldown_ms")]
    pub cooldown_ms: u64,
    #[serde(default = "default_post_only")]
    pub post_only: bool,
    #[serde(default = "default_post_only_retries")]
    pub post_only_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecisionConfig {
    pub tick_size: f64,
    pub step_size: f64,
    #[serde(default)]
    pub min_qty: Option<f64>,
    #[serde(default)]
    pub min_notional: Option<f64>,
    #[serde(default)]
    pub price_digits: Option<u32>,
    #[serde(default)]
    pub qty_digits: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeConfig {
    pub maker_fee: f64,
    pub taker_fee: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskLimits {
    pub max_net_notional: f64,
    pub max_total_notional: f64,
    pub margin_ratio_limit: f64,
    pub funding_rate_limit: f64,
    pub funding_cost_limit: f64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PriceReference {
    Mid,
    Last,
}

impl Default for PriceReference {
    fn default() -> Self {
        PriceReference::Mid
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum RiskReference {
    Mark,
    Last,
}

impl Default for RiskReference {
    fn default() -> Self {
        RiskReference::Mark
    }
}

#[derive(Debug, Clone)]
pub struct ResolvedPrecision {
    pub tick_size: f64,
    pub step_size: f64,
    pub min_qty: f64,
    pub min_notional: f64,
    pub price_digits: u32,
    pub qty_digits: u32,
}

impl StrategyConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.symbol.trim().is_empty() {
            return Err("symbol 不能为空".to_string());
        }
        if !(2..=10).contains(&self.grid.levels_per_side) {
            return Err("levels_per_side 必须在 2..=10".to_string());
        }
        if let Some(abs) = self.grid.grid_spacing_abs {
            if abs <= 0.0 {
                return Err("grid_spacing_abs 必须大于0".to_string());
            }
        } else if self.grid.grid_spacing_pct <= 0.0 {
            return Err("grid_spacing_pct 必须大于0".to_string());
        }
        if let Some(qty) = self.grid.order_qty {
            if qty <= 0.0 {
                return Err("order_qty 必须大于0".to_string());
            }
        } else if self.grid.order_notional <= 0.0 {
            return Err("order_notional 必须大于0".to_string());
        }
        if self.precision.tick_size <= 0.0 || self.precision.step_size <= 0.0 {
            return Err("tick_size/step_size 必须大于0".to_string());
        }
        Ok(())
    }
}

impl PrecisionConfig {
    pub fn resolve(&self) -> ResolvedPrecision {
        let price_digits = self
            .price_digits
            .unwrap_or_else(|| precision_from_step(self.tick_size));
        let qty_digits = self
            .qty_digits
            .unwrap_or_else(|| precision_from_step(self.step_size));
        let min_qty = self.min_qty.unwrap_or(self.step_size);
        let min_notional = self.min_notional.unwrap_or(0.0);
        ResolvedPrecision {
            tick_size: self.tick_size,
            step_size: self.step_size,
            min_qty,
            min_notional,
            price_digits,
            qty_digits,
        }
    }
}

impl ResolvedPrecision {
    pub fn quantize_price(&self, price: f64) -> f64 {
        if price <= 0.0 {
            return 0.0;
        }
        apply_precision(price, self.tick_size, self.price_digits)
    }

    pub fn quantize_price_for_side(&self, price: f64, side: OrderSide) -> f64 {
        if price <= 0.0 {
            return 0.0;
        }
        // 防止浮点边界误差导致二次量化时多跳一个 tick。
        let eps = 1e-9;
        let adjusted = if self.tick_size > 0.0 {
            let multiples = price / self.tick_size;
            match side {
                OrderSide::Buy => (multiples + eps).floor() * self.tick_size,
                OrderSide::Sell => (multiples - eps).ceil() * self.tick_size,
            }
        } else {
            price
        };
        let factor = 10f64.powi(self.price_digits as i32);
        match side {
            OrderSide::Buy => ((adjusted * factor) + eps).floor() / factor,
            OrderSide::Sell => ((adjusted * factor) - eps).ceil() / factor,
        }
    }

    pub fn quantize_qty(&self, qty: f64) -> f64 {
        if qty <= 0.0 {
            return 0.0;
        }
        apply_precision(qty, self.step_size, self.qty_digits)
    }

    pub fn quantize_qty_up(&self, qty: f64) -> f64 {
        if qty <= 0.0 {
            return 0.0;
        }
        let adjusted = if self.step_size > 0.0 {
            let multiples = (qty / self.step_size).ceil();
            multiples * self.step_size
        } else {
            qty
        };
        let factor = 10f64.powi(self.qty_digits as i32);
        (adjusted * factor).ceil() / factor
    }
}

fn default_require_hedge_mode() -> bool {
    true
}

fn default_market_type() -> MarketType {
    MarketType::Futures
}

fn default_fill_remaining_slots_with_opens() -> bool {
    true
}

fn default_grid_spacing_pct() -> f64 {
    0.0
}

fn default_strict_pairing() -> bool {
    false
}

fn default_max_gap_steps() -> f64 {
    1.0
}

fn default_follow_cooldown_ms() -> u64 {
    800
}

fn default_max_follow_actions_per_minute() -> usize {
    30
}

fn default_cooldown_ms() -> u64 {
    500
}

fn default_post_only() -> bool {
    true
}

fn default_post_only_retries() -> u32 {
    3
}

fn default_tick_interval_ms() -> u64 {
    1000
}

fn default_reconcile_interval_ms() -> u64 {
    3000
}

fn default_trade_fetch_limit() -> u32 {
    50
}

fn default_request_timeout_secs() -> u64 {
    15
}

fn default_watchdog_timeout_secs() -> u64 {
    60
}

fn default_ws_enabled() -> bool {
    true
}

fn default_ws_keepalive_interval_secs() -> u64 {
    1200
}

fn default_ws_reconnect_delay_ms() -> u64 {
    1000
}

fn default_ws_max_reconnect_delay_ms() -> u64 {
    30000
}

fn default_startup_cancel_all() -> bool {
    true
}

fn default_shutdown_cancel_all() -> bool {
    true
}

fn default_max_consecutive_failures() -> u32 {
    5
}

fn precision_from_step(step: f64) -> u32 {
    if step == 0.0 {
        return 8;
    }
    let s = format!("{:.10}", step);
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() > 1 {
        parts[1].trim_end_matches('0').len() as u32
    } else {
        0
    }
}

fn apply_precision(value: f64, step: f64, digits: u32) -> f64 {
    let eps = 1e-9;
    let adjusted = if step > 0.0 {
        ((value / step) + eps).floor() * step
    } else {
        value
    };
    let factor = 10f64.powi(digits as i32);
    ((adjusted * factor) + eps).floor() / factor
}
