use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};

use crate::core::types::{Kline, OrderSide};

use super::config::{CacheConfig, SymbolConfig};

#[derive(Debug, Clone)]
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

/// 最近一次扫描时的指标快照
#[derive(Debug, Clone, Default)]
pub struct SignalSnapshot {
    pub timestamp: DateTime<Utc>,
    pub z_score: f64,
    pub band_percent: f64,
    pub rsi: f64,
    pub bollinger_mid: f64,
    pub bollinger_sigma: f64,
    pub atr: f64,
    pub adx: f64,
    pub bbw: f64,
    pub bbw_percentile: f64,
    pub slope_metric: f64,
    pub choppiness: Option<f64>,
}

/// 记录下单与撤单的追踪信息
#[derive(Debug, Clone)]
pub struct OrderTracker {
    pub order_id: Option<String>,
    pub client_order_id: String,
    pub side: OrderSide,
    pub price: f64,
    pub quantity: f64,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub relist_attempts: u32,
    pub stop_price: f64,
    pub take_profit: f64,
    pub improve: f64,
    pub trailing_distance: f64,
}

impl OrderTracker {
    pub fn new(
        client_order_id: String,
        side: OrderSide,
        price: f64,
        quantity: f64,
        ttl_secs: u64,
        stop_price: f64,
        take_profit: f64,
        improve: f64,
        trailing_distance: f64,
    ) -> Self {
        let now = Utc::now();
        Self {
            order_id: None,
            client_order_id,
            side,
            price,
            quantity,
            created_at: now,
            expires_at: now + chrono::Duration::seconds(ttl_secs as i64),
            relist_attempts: 0,
            stop_price,
            take_profit,
            improve,
            trailing_distance,
        }
    }
}

/// 仓位状态
#[derive(Debug, Clone)]
pub struct PositionState {
    pub side: OrderSide,
    pub quantity: f64,
    pub entry_price: f64,
    pub stop_price: f64,
    pub trailing_distance: f64,
    pub take_profit: f64,
    pub opened_at: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
    pub realized_pnl: f64,
    pub remaining_qty: f64,
    pub filled_amount_quote: f64,
    pub stop_order_id: Option<String>,
    pub tp_order_id: Option<String>,
    pub time_in_position_bars: u32,
}

impl PositionState {
    pub fn new(
        side: OrderSide,
        quantity: f64,
        entry_price: f64,
        stop_price: f64,
        trailing_distance: f64,
        take_profit: f64,
    ) -> Self {
        let now = Utc::now();
        Self {
            side,
            quantity,
            entry_price,
            stop_price,
            trailing_distance,
            take_profit,
            opened_at: now,
            last_update: now,
            realized_pnl: 0.0,
            remaining_qty: quantity,
            filled_amount_quote: 0.0,
            stop_order_id: None,
            tp_order_id: None,
            time_in_position_bars: 0,
        }
    }
}

/// 单个交易对的运行时状态
#[derive(Debug)]
pub struct SymbolState {
    pub config: SymbolConfig,
    pub one_minute: VecDeque<Kline>,
    pub five_minute: VecDeque<Kline>,
    pub fifteen_minute: VecDeque<Kline>,
    pub one_hour: VecDeque<Kline>,
    pub bbw_history: VecDeque<f64>,
    pub mid_history: VecDeque<f64>,
    pub sigma_history: VecDeque<f64>,
    pub last_one_minute_close: Option<DateTime<Utc>>,
    pub last_five_minute_close: Option<DateTime<Utc>>,
    pub last_fifteen_minute_close: Option<DateTime<Utc>>,
    pub last_one_hour_close: Option<DateTime<Utc>>,
    pub last_gap: Option<DateTime<Utc>>,
    pub frozen_until: Option<DateTime<Utc>>,
    pub frozen_reason: Option<String>,
    pub pending_orders: HashMap<String, OrderTracker>,
    pub long_position: Option<PositionState>,
    pub short_position: Option<PositionState>,
    pub order_seq: u64,
    pub last_signal: Option<SignalSnapshot>,
    pub last_liquidity_check: Option<DateTime<Utc>>,
    pub last_depth: Option<LiquiditySnapshot>,
    pub last_volume: Option<VolumeSnapshot>,
}

#[derive(Debug, Clone, Default)]
pub struct LiquiditySnapshot {
    pub bid_price: f64,
    pub ask_price: f64,
    pub spread: f64,
    pub total_bid_depth: f64,
    pub total_ask_depth: f64,
    pub maker_fee: Option<f64>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct VolumeSnapshot {
    pub window_minutes: u64,
    pub quote_volume: f64,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SymbolSnapshot {
    pub config: SymbolConfig,
    pub one_minute: Vec<Kline>,
    pub five_minute: Vec<Kline>,
    pub fifteen_minute: Vec<Kline>,
    pub one_hour: Vec<Kline>,
    pub bbw_history: Vec<f64>,
    pub mid_history: Vec<f64>,
    pub sigma_history: Vec<f64>,
    pub long_position: Option<PositionState>,
    pub short_position: Option<PositionState>,
    pub last_depth: Option<LiquiditySnapshot>,
    pub last_volume: Option<VolumeSnapshot>,
    pub frozen: bool,
}

impl SymbolSnapshot {
    pub fn position(&self, side: OrderSide) -> Option<&PositionState> {
        match side {
            OrderSide::Buy => self.long_position.as_ref(),
            OrderSide::Sell => self.short_position.as_ref(),
        }
    }

    pub fn has_any_position(&self) -> bool {
        self.long_position.is_some() || self.short_position.is_some()
    }
}

impl SymbolState {
    pub fn new(config: SymbolConfig, cache: &CacheConfig) -> Self {
        let bbw_capacity = cache.max_1h_bars.max(500);
        Self {
            config,
            one_minute: VecDeque::with_capacity(cache.max_1m_bars),
            five_minute: VecDeque::with_capacity(cache.max_5m_bars),
            fifteen_minute: VecDeque::with_capacity(cache.max_15m_bars),
            one_hour: VecDeque::with_capacity(cache.max_1h_bars),
            bbw_history: VecDeque::with_capacity(bbw_capacity),
            mid_history: VecDeque::with_capacity(bbw_capacity),
            sigma_history: VecDeque::with_capacity(bbw_capacity),
            last_one_minute_close: None,
            last_five_minute_close: None,
            last_fifteen_minute_close: None,
            last_one_hour_close: None,
            last_gap: None,
            frozen_until: None,
            frozen_reason: None,
            pending_orders: HashMap::new(),
            long_position: None,
            short_position: None,
            order_seq: 0,
            last_signal: None,
            last_liquidity_check: None,
            last_depth: None,
            last_volume: None,
        }
    }

    fn trim_queue<T>(queue: &mut VecDeque<T>, max_len: usize) {
        while queue.len() > max_len {
            queue.pop_front();
        }
    }

    pub fn push_one_minute(&mut self, bar: Kline, cache: &CacheConfig) {
        if self
            .one_minute
            .back()
            .map(|last| last.close_time == bar.close_time)
            .unwrap_or(false)
        {
            self.one_minute.pop_back();
        }
        self.last_one_minute_close = Some(bar.close_time);
        self.one_minute.push_back(bar);
        Self::trim_queue(&mut self.one_minute, cache.max_1m_bars);
    }

    pub fn push_five_minute(&mut self, bar: Kline, cache: &CacheConfig) {
        if self
            .five_minute
            .back()
            .map(|last| last.close_time == bar.close_time)
            .unwrap_or(false)
        {
            self.five_minute.pop_back();
        }
        self.last_five_minute_close = Some(bar.close_time);
        self.five_minute.push_back(bar);
        Self::trim_queue(&mut self.five_minute, cache.max_5m_bars);
    }

    pub fn push_fifteen_minute(&mut self, bar: Kline, cache: &CacheConfig) {
        if self
            .fifteen_minute
            .back()
            .map(|last| last.close_time == bar.close_time)
            .unwrap_or(false)
        {
            self.fifteen_minute.pop_back();
        }
        self.last_fifteen_minute_close = Some(bar.close_time);
        self.fifteen_minute.push_back(bar.clone());
        Self::trim_queue(&mut self.fifteen_minute, cache.max_15m_bars);
    }

    pub fn push_one_hour(&mut self, bar: Kline, cache: &CacheConfig) {
        if self
            .one_hour
            .back()
            .map(|last| last.close_time == bar.close_time)
            .unwrap_or(false)
        {
            self.one_hour.pop_back();
        }
        self.last_one_hour_close = Some(bar.close_time);
        self.one_hour.push_back(bar.clone());
        Self::trim_queue(&mut self.one_hour, cache.max_1h_bars);
    }

    pub fn update_bbw_series(&mut self, bbw: f64, mid: f64, sigma: f64) {
        const MAX_HISTORY: usize = 800;
        self.bbw_history.push_back(bbw);
        self.mid_history.push_back(mid);
        self.sigma_history.push_back(sigma);
        Self::trim_queue(&mut self.bbw_history, MAX_HISTORY);
        Self::trim_queue(&mut self.mid_history, MAX_HISTORY);
        Self::trim_queue(&mut self.sigma_history, MAX_HISTORY);
    }

    pub fn freeze(&mut self, minutes: i64, reason: impl Into<String>) {
        self.frozen_until = Some(Utc::now() + chrono::Duration::minutes(minutes));
        self.frozen_reason = Some(reason.into());
    }

    pub fn is_frozen(&self) -> bool {
        if let Some(until) = self.frozen_until {
            if Utc::now() < until {
                return true;
            }
        }
        false
    }

    pub fn snapshot(&self) -> SymbolSnapshot {
        SymbolSnapshot {
            config: self.config.clone(),
            one_minute: self.one_minute.iter().cloned().collect(),
            five_minute: self.five_minute.iter().cloned().collect(),
            fifteen_minute: self.fifteen_minute.iter().cloned().collect(),
            one_hour: self.one_hour.iter().cloned().collect(),
            bbw_history: self.bbw_history.iter().copied().collect(),
            mid_history: self.mid_history.iter().copied().collect(),
            sigma_history: self.sigma_history.iter().copied().collect(),
            long_position: self.long_position.clone(),
            short_position: self.short_position.clone(),
            last_depth: self.last_depth.clone(),
            last_volume: self.last_volume.clone(),
            frozen: self.is_frozen(),
        }
    }

    pub fn set_position(&mut self, side: OrderSide, position: PositionState) {
        match side {
            OrderSide::Buy => self.long_position = Some(position),
            OrderSide::Sell => self.short_position = Some(position),
        }
    }

    pub fn position(&self, side: OrderSide) -> Option<&PositionState> {
        match side {
            OrderSide::Buy => self.long_position.as_ref(),
            OrderSide::Sell => self.short_position.as_ref(),
        }
    }

    pub fn position_mut(&mut self, side: OrderSide) -> Option<&mut PositionState> {
        match side {
            OrderSide::Buy => self.long_position.as_mut(),
            OrderSide::Sell => self.short_position.as_mut(),
        }
    }

    pub fn clear_position(&mut self, side: OrderSide) {
        match side {
            OrderSide::Buy => self.long_position = None,
            OrderSide::Sell => self.short_position = None,
        }
    }

    pub fn has_any_position(&self) -> bool {
        self.long_position.is_some() || self.short_position.is_some()
    }

    pub fn iter_positions(&self) -> impl Iterator<Item = (OrderSide, &PositionState)> {
        let mut positions = Vec::new();
        if let Some(pos) = &self.long_position {
            positions.push((OrderSide::Buy, pos));
        }
        if let Some(pos) = &self.short_position {
            positions.push((OrderSide::Sell, pos));
        }
        positions.into_iter()
    }
}

/// 策略级别的绩效跟踪
#[derive(Debug, Default, Clone)]
pub struct PerformanceTracker {
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub max_drawdown: f64,
    pub daily_start_equity: Option<f64>,
    pub current_equity: Option<f64>,
    pub last_reset: Option<DateTime<Utc>>,
}
