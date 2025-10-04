use chrono::{DateTime, Utc};
use std::collections::{HashMap, VecDeque};

use crate::core::types::Order;

/// 市场数据快照
#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub timestamp: DateTime<Utc>,
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid_price: f64,
    pub bid_volume: f64,
    pub ask_volume: f64,
    pub last_trade_price: f64,
    pub last_trade_volume: f64,
}

/// 交易对信息
#[derive(Debug, Clone)]
pub struct SymbolInfo {
    pub base_asset: String,
    pub quote_asset: String,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_notional: f64,
    pub price_precision: usize,
    pub quantity_precision: usize,
}

/// 策略状态
#[derive(Debug, Clone)]
pub struct ASStrategyState {
    pub inventory: f64,
    pub long_position: f64,
    pub short_position: f64,
    pub avg_cost: f64,
    pub long_avg_cost: f64,
    pub short_avg_cost: f64,
    pub active_buy_orders: HashMap<String, Order>,
    pub active_sell_orders: HashMap<String, Order>,
    pub total_pnl: f64,
    pub daily_pnl: f64,
    pub trade_count: u64,
    pub consecutive_losses: u32,
    pub start_time: DateTime<Utc>,
    pub last_order_time: DateTime<Utc>,
    pub orders_this_minute: u32,
    pub current_minute: i64,
}

/// 技术指标状态
#[derive(Debug, Clone)]
pub struct IndicatorState {
    pub ema_fast: f64,
    pub ema_slow: f64,
    pub rsi: f64,
    pub vwap: f64,
    pub atr: f64,
    pub price_history: VecDeque<f64>,
    pub volume_history: VecDeque<f64>,
    pub prev_avg_gain: f64,
    pub prev_avg_loss: f64,
}

/// 本地订单簿
#[derive(Debug, Clone)]
pub struct LocalOrderBook {
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub last_update: DateTime<Utc>,
}
