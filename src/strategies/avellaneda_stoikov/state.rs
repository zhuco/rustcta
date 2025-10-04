use chrono::{DateTime, Utc};

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

    // 性能统计
    pub total_trades: u64,
    pub buy_fills: u64,
    pub sell_fills: u64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,

    // 风险指标
    pub consecutive_losses: u32,
    pub daily_pnl: f64,
    pub max_drawdown: f64,

    // 时间管理
    pub strategy_start_time: DateTime<Utc>,
    pub last_update_time: DateTime<Utc>,
    pub last_volatility_update: DateTime<Utc>,
}
