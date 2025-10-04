use chrono::{DateTime, Utc};
use std::collections::HashMap;

use crate::core::types::{Order, OrderSide};

/// 订单流事件
#[derive(Debug, Clone)]
pub struct OrderFlowEvent {
    pub timestamp: DateTime<Utc>,
    pub side: OrderSide,
    pub price: f64,
    pub quantity: f64,
    pub event_type: OrderEventType,
}

#[derive(Debug, Clone)]
pub enum OrderEventType {
    NewOrder,
    Trade,
    Cancel,
}

/// 泊松参数
#[derive(Debug, Clone)]
pub struct PoissonParameters {
    pub lambda_bid: f64,
    pub lambda_ask: f64,
    pub mu_bid: f64,
    pub mu_ask: f64,
    pub avg_queue_bid: f64,
    pub avg_queue_ask: f64,
    pub last_update: DateTime<Utc>,
    pub last_trade_time: Option<DateTime<Utc>>,
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

#[derive(Debug, Clone)]
pub struct MMStrategyState {
    pub inventory: f64,
    pub avg_price: f64,
    pub active_buy_orders: HashMap<String, Order>,
    pub active_sell_orders: HashMap<String, Order>,
    pub total_pnl: f64,
    pub daily_pnl: f64,
    pub trade_count: u64,
    pub start_time: DateTime<Utc>,
}

/// 内部订单簿缓存
#[derive(Debug, Clone)]
pub struct LocalOrderBook {
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
    pub last_update: DateTime<Utc>,
}
