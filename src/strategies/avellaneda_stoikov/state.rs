use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::core::types::{MarketType, OrderSide, OrderStatus};

/// 交易所规则与精度约束。
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct ASOrderState {
    pub exchange_order_id: String,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub price: f64,
    pub original_qty: f64,
    pub filled_qty: f64,
    pub status: OrderStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ASOrderState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.status,
            OrderStatus::Closed
                | OrderStatus::Canceled
                | OrderStatus::Expired
                | OrderStatus::Rejected
        )
    }
}

/// A-S策略状态
#[derive(Debug, Clone)]
pub struct ASState {
    // 市场数据
    pub mid_price: f64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub volatility: f64,

    // 库存管理
    pub inventory: f64,
    pub inventory_value: f64,

    // 订单管理
    pub active_buy_order: Option<String>,
    pub active_sell_order: Option<String>,
    pub last_buy_price: f64,
    pub last_sell_price: f64,
    pub open_orders: HashMap<String, ASOrderState>,

    // 性能统计
    pub total_trades: u64,
    pub buy_fills: u64,
    pub sell_fills: u64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub fees_paid: f64,
    pub avg_entry_price: f64,
    pub equity_peak: f64,

    // 风险指标
    pub consecutive_losses: u32,
    pub daily_pnl: f64,
    pub max_drawdown: f64,

    // 时间管理
    pub strategy_start_time: DateTime<Utc>,
    pub last_update_time: DateTime<Utc>,
    pub last_volatility_update: DateTime<Utc>,
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
