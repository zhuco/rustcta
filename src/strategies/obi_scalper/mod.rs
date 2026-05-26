use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::StreamExt;
use hashbrown::HashMap as FastHashMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::core::types::{
    MarketType, Order, OrderBook, OrderRequest, OrderSide, OrderStatus, OrderType, Position,
};
use crate::cta::account_manager::{AccountInfo, AccountManager};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObiScalperConfig {
    pub strategy: StrategyConfig,
    pub account: AccountConfig,
    pub global_risk: GlobalRiskConfig,
    pub execution: ExecutionConfig,
    pub signal: SignalConfig,
    pub logging: LoggingConfig,
    pub notifications: NotificationConfig,
    pub symbols: Vec<SymbolConfig>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub name: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_dry_run")]
    pub dry_run: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub account_id: String,
    #[serde(default = "default_exchange")]
    pub exchange: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalRiskConfig {
    pub margin_budget: f64,
    pub max_daily_loss: f64,
    pub max_total_notional: f64,
    pub max_open_symbols: usize,
    pub emergency_close_on_disconnect_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub order_notional: f64,
    pub maker_fee_rate: f64,
    pub taker_fee_rate: f64,
    #[serde(default)]
    pub inventory_mode: bool,
    #[serde(default = "default_max_inventory_notional")]
    pub max_inventory_notional: f64,
    #[serde(default = "default_soft_inventory_notional")]
    pub soft_inventory_notional: f64,
    #[serde(default = "default_hard_inventory_notional")]
    pub hard_inventory_notional: f64,
    #[serde(default = "default_take_profit_bps")]
    pub take_profit_bps: f64,
    #[serde(default = "default_min_take_profit_bps")]
    pub min_take_profit_bps: f64,
    #[serde(default = "default_max_book_latency_ms")]
    pub max_book_latency_ms: u64,
    #[serde(default = "default_post_cancel_cooldown_ms")]
    pub post_cancel_cooldown_ms: u64,
    #[serde(default = "default_min_entry_interval_ms")]
    pub min_entry_interval_ms: u64,
    #[serde(default)]
    pub allow_taker_entry: bool,
    #[serde(default = "default_true")]
    pub allow_taker_exit: bool,
    pub entry_order_ttl_ms: u64,
    pub exit_order_ttl_ms: u64,
    pub max_order_retries: u32,
    #[serde(default)]
    pub poll_interval_ms: u64,
    #[serde(default)]
    pub position_poll_interval_ms: u64,
    pub summary_interval_minutes: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalConfig {
    pub obi_levels: usize,
    pub ema_fast_period: f64,
    pub ema_slow_period: f64,
    pub long_entry_threshold: f64,
    pub short_entry_threshold: f64,
    pub exit_threshold: f64,
    pub strong_signal_threshold: f64,
    pub min_signal_hold_ms: u64,
    pub price_stable_bps: f64,
    pub max_spread_bps: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub max_lines: usize,
    pub key_log_every_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub wechat_webhook_url: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    pub symbol: String,
    pub tick_size: f64,
    pub step_size: f64,
    pub min_qty: f64,
    pub min_notional: f64,
    pub order_notional: Option<f64>,
    pub max_symbol_notional: f64,
    pub stop_loss_pct: f64,
    pub max_holding_ms: u64,
    pub cooldown_after_loss_ms: u64,
    pub max_consecutive_losses: u32,
    pub consecutive_loss_cooldown_ms: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct BookLevel {
    pub price: f64,
    pub qty: f64,
}
#[derive(Debug, Clone)]
pub struct FixedOrderBook<const N: usize> {
    pub bids: [BookLevel; N],
    pub asks: [BookLevel; N],
    pub last_update_id: u64,
    pub initialized: bool,
    pub updated_at: DateTime<Utc>,
}
impl<const N: usize> Default for FixedOrderBook<N> {
    fn default() -> Self {
        Self {
            bids: [BookLevel::default(); N],
            asks: [BookLevel::default(); N],
            last_update_id: 0,
            initialized: false,
            updated_at: Utc::now(),
        }
    }
}
impl<const N: usize> FixedOrderBook<N> {
    #[inline]
    pub fn from_snapshot(symbol: &str, snapshot: &OrderBook) -> Result<Self> {
        let mut book = Self::default();
        if snapshot.bids.is_empty() || snapshot.asks.is_empty() {
            return Err(anyhow!("{} orderbook empty", symbol));
        }
        for (idx, level) in snapshot.bids.iter().take(N).enumerate() {
            book.bids[idx] = BookLevel {
                price: level[0],
                qty: level[1],
            };
        }
        for (idx, level) in snapshot.asks.iter().take(N).enumerate() {
            book.asks[idx] = BookLevel {
                price: level[0],
                qty: level[1],
            };
        }
        book.initialized = true;
        book.updated_at = snapshot.timestamp;
        book.last_update_id = snapshot
            .info
            .get("lastUpdateId")
            .and_then(|value| value.as_u64())
            .unwrap_or_default();
        Ok(book)
    }
    #[inline]
    pub fn top_bid(&self) -> Option<BookLevel> {
        self.bids
            .first()
            .copied()
            .filter(|level| level.price > 0.0 && level.qty > 0.0)
    }
    #[inline]
    pub fn top_ask(&self) -> Option<BookLevel> {
        self.asks
            .first()
            .copied()
            .filter(|level| level.price > 0.0 && level.qty > 0.0)
    }
    #[inline]
    pub fn mid_price(&self) -> Option<f64> {
        Some((self.top_bid()?.price + self.top_ask()?.price) * 0.5)
    }
    #[inline]
    pub fn spread_bps(&self) -> Option<f64> {
        let bid = self.top_bid()?;
        let ask = self.top_ask()?;
        let mid = (bid.price + ask.price) * 0.5;
        if mid <= 0.0 {
            None
        } else {
            Some((ask.price - bid.price) / mid * 10_000.0)
        }
    }
    #[inline]
    pub fn weighted_obi(&self, levels: usize) -> f64 {
        let depth = levels.min(N);
        let mut bid_sum = 0.0;
        let mut ask_sum = 0.0;
        for idx in 0..depth {
            let weight = 1.0 / ((idx + 1) as f64);
            bid_sum += self.bids[idx].qty * weight;
            ask_sum += self.asks[idx].qty * weight;
        }
        let denom = bid_sum + ask_sum;
        if denom <= f64::EPSILON {
            0.0
        } else {
            (bid_sum - ask_sum) / denom
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Ema {
    value: f64,
    alpha: f64,
    initialized: bool,
}
impl Ema {
    #[inline]
    pub fn new(period: f64) -> Self {
        Self {
            value: 0.0,
            alpha: 2.0 / (period + 1.0),
            initialized: false,
        }
    }
    #[inline]
    pub fn update(&mut self, value: f64) -> f64 {
        if !self.initialized {
            self.value = value;
            self.initialized = true;
        } else {
            self.value += self.alpha * (value - self.value);
        }
        self.value
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MarketState {
    pub symbol: String,
    pub mid: f64,
    pub spread_bps: f64,
    pub obi: f64,
    pub ema_fast: f64,
    pub ema_slow: f64,
    pub updated_at: DateTime<Utc>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum PositionSideKind {
    Long,
    Short,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum SymbolPhase {
    Flat,
    PendingEntryLong,
    PendingEntryShort,
    LongOpen,
    ShortOpen,
    PendingExitLong,
    PendingExitShort,
    Cooldown,
    DataStale,
}
#[derive(Debug, Clone)]
pub struct PendingOrderState {
    pub order_id: String,
    pub client_order_id: String,
    pub side: PositionSideKind,
    pub is_entry: bool,
    pub qty: f64,
    pub price: f64,
    pub submitted_at: Instant,
    pub deadline: Instant,
    pub retries: u32,
    pub taker: bool,
}
#[derive(Debug, Clone)]
pub struct PositionState {
    pub side: PositionSideKind,
    pub qty: f64,
    pub entry_price: f64,
    pub opened_at: Instant,
    pub opened_at_wall: DateTime<Utc>,
    pub entry_fee: f64,
}
#[derive(Debug, Clone, Default)]
pub struct InventoryLeg {
    pub qty: f64,
    pub avg_price: f64,
    pub fee: f64,
}
impl InventoryLeg {
    fn notional(&self, mid: f64) -> f64 {
        self.qty.abs() * mid.max(self.avg_price)
    }
    fn add_fill(&mut self, qty: f64, price: f64, fee: f64) {
        if qty <= 0.0 || price <= 0.0 {
            return;
        }
        let old_value = self.qty * self.avg_price;
        let new_qty = self.qty + qty;
        self.avg_price = if new_qty > 0.0 {
            (old_value + qty * price) / new_qty
        } else {
            0.0
        };
        self.qty = new_qty;
        self.fee += fee;
    }
    fn reduce_fill(&mut self, qty: f64) -> f64 {
        if qty <= 0.0 || self.qty <= 0.0 {
            return 0.0;
        }
        let closed = qty.min(self.qty);
        let fee_share = if self.qty > 0.0 {
            self.fee * (closed / self.qty)
        } else {
            0.0
        };
        self.qty -= closed;
        self.fee = (self.fee - fee_share).max(0.0);
        if self.qty <= 1e-9 {
            self.qty = 0.0;
            self.avg_price = 0.0;
            self.fee = 0.0;
        }
        fee_share
    }
}
#[derive(Debug, Clone, Default)]
pub struct InventoryPool {
    pub long: InventoryLeg,
    pub short: InventoryLeg,
}
impl InventoryPool {
    fn gross_notional(&self, mid: f64) -> f64 {
        self.long.notional(mid) + self.short.notional(mid)
    }
    fn net_notional(&self, mid: f64) -> f64 {
        self.long.notional(mid) - self.short.notional(mid)
    }
    fn has_inventory(&self) -> bool {
        self.long.qty > 0.0 || self.short.qty > 0.0
    }
}
#[derive(Debug, Clone, Default)]
pub struct GlobalRiskState {
    pub open_notional: f64,
    pub open_symbols: usize,
    pub realized_pnl_day: f64,
    pub symbol_notional: FastHashMap<String, f64>,
}
#[derive(Debug, Clone)]
pub struct SymbolRuntime {
    pub config: SymbolConfig,
    pub phase: SymbolPhase,
    pub book: FixedOrderBook<20>,
    pub ema_fast: Ema,
    pub ema_slow: Ema,
    pub last_mid: f64,
    pub last_signal_at: Instant,
    pub last_key_log_at: Instant,
    pub last_book_at: Instant,
    pub pending: Option<PendingOrderState>,
    pub position: Option<PositionState>,
    pub inventory: InventoryPool,
    pub cancel_cooldown_until: Option<Instant>,
    pub cooldown_until: Option<Instant>,
    pub consecutive_losses: u32,
    pub realized_pnl_30m: f64,
    pub realized_pnl_day: f64,
    pub fees_30m: f64,
    pub fees_day: f64,
    pub trades_30m: u64,
    pub wins_30m: u64,
    pub losses_30m: u64,
    pub order_submit_errors: u64,
    pub order_rejects: u64,
}
impl SymbolRuntime {
    fn inventory_as_position_like(&self) -> Option<PositionState> {
        let long_n = self.inventory.long.notional(self.last_mid);
        let short_n = self.inventory.short.notional(self.last_mid);
        if long_n <= 0.0 && short_n <= 0.0 {
            return None;
        }
        let (side, leg) = if long_n >= short_n {
            (PositionSideKind::Long, &self.inventory.long)
        } else {
            (PositionSideKind::Short, &self.inventory.short)
        };
        Some(PositionState {
            side,
            qty: leg.qty,
            entry_price: leg.avg_price,
            opened_at: Instant::now(),
            opened_at_wall: Utc::now(),
            entry_fee: leg.fee,
        })
    }
    fn inventory_phase_like(&self) -> SymbolPhase {
        let long_n = self.inventory.long.notional(self.last_mid);
        let short_n = self.inventory.short.notional(self.last_mid);
        if long_n <= 0.0 && short_n <= 0.0 {
            SymbolPhase::Flat
        } else if long_n >= short_n {
            SymbolPhase::LongOpen
        } else {
            SymbolPhase::ShortOpen
        }
    }
    fn new(config: SymbolConfig, signal: &SignalConfig) -> Self {
        Self {
            config,
            phase: SymbolPhase::Flat,
            book: FixedOrderBook::default(),
            ema_fast: Ema::new(signal.ema_fast_period),
            ema_slow: Ema::new(signal.ema_slow_period),
            last_mid: 0.0,
            last_signal_at: Instant::now() - Duration::from_secs(3600),
            last_key_log_at: Instant::now() - Duration::from_secs(3600),
            last_book_at: Instant::now(),
            pending: None,
            position: None,
            inventory: InventoryPool::default(),
            cancel_cooldown_until: None,
            cooldown_until: None,
            consecutive_losses: 0,
            realized_pnl_30m: 0.0,
            realized_pnl_day: 0.0,
            fees_30m: 0.0,
            fees_day: 0.0,
            trades_30m: 0,
            wins_30m: 0,
            losses_30m: 0,
            order_submit_errors: 0,
            order_rejects: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderUpdateEvent {
    pub symbol: String,
    pub order_id: String,
    pub client_order_id: String,
    pub side: OrderSide,
    pub status: OrderStatus,
    pub last_filled_qty: f64,
    pub cumulative_filled_qty: f64,
    pub avg_price: f64,
    pub last_price: f64,
    pub commission: f64,
    pub is_maker: bool,
    pub event_time: DateTime<Utc>,
}
#[derive(Debug)]
pub enum SymbolEvent {
    Book {
        book: FixedOrderBook<20>,
        received_at: Instant,
    },
    OrderUpdate(OrderUpdateEvent),
    Timer,
}

pub struct ObiScalperStrategy {
    config: ObiScalperConfig,
    account_manager: Arc<AccountManager>,
    states: Arc<DashMap<String, MarketState>>,
    summaries: Arc<DashMap<String, SymbolSummary>>,
    senders: Arc<DashMap<String, mpsc::Sender<SymbolEvent>>>,
    handles: Arc<Mutex<Vec<JoinHandle<()>>>>,
    http: Client,
}
#[derive(Debug, Clone, Default, Serialize)]
pub struct SymbolSummary {
    pub symbol: String,
    pub phase: String,
    pub position: String,
    pub pending: String,
    pub trades_30m: u64,
    pub wins_30m: u64,
    pub losses_30m: u64,
    pub fees_30m: f64,
    pub realized_pnl_30m: f64,
    pub realized_pnl_day: f64,
    pub order_submit_errors: u64,
    pub order_rejects: u64,
}

impl ObiScalperStrategy {
    pub fn new(config: ObiScalperConfig, account_manager: Arc<AccountManager>) -> Self {
        Self {
            config,
            account_manager,
            states: Arc::new(DashMap::new()),
            summaries: Arc::new(DashMap::new()),
            senders: Arc::new(DashMap::new()),
            handles: Arc::new(Mutex::new(Vec::new())),
            http: Client::new(),
        }
    }
    pub async fn start(&self) -> Result<()> {
        validate_config(&self.config)?;
        let account = self
            .account_manager
            .get_account(&self.config.account.account_id)
            .ok_or_else(|| anyhow!("账户不存在: {}", self.config.account.account_id))?;
        let startup_positions = startup_safety_check(&self.config, &account).await?;
        let enabled: Vec<SymbolConfig> = self
            .config
            .symbols
            .iter()
            .filter(|s| s.enabled)
            .cloned()
            .collect();
        if enabled.is_empty() {
            return Err(anyhow!("obi_scalper 未配置启用交易对"));
        }
        let risk = Arc::new(Mutex::new(GlobalRiskState::default()));
        let mut handles = self.handles.lock().await;
        for symbol_config in &enabled {
            let (tx, rx) = mpsc::channel::<SymbolEvent>(2048);
            self.senders.insert(symbol_config.symbol.clone(), tx);
            let mut runtime = SymbolRuntime::new(symbol_config.clone(), &self.config.signal);
            if let Some(positions) = startup_positions.get(&symbol_config.symbol) {
                for position in positions {
                    if self.config.execution.inventory_mode {
                        match position.side {
                            PositionSideKind::Long => runtime.inventory.long.add_fill(
                                position.qty,
                                position.entry_price,
                                position.entry_fee,
                            ),
                            PositionSideKind::Short => runtime.inventory.short.add_fill(
                                position.qty,
                                position.entry_price,
                                position.entry_fee,
                            ),
                        }
                    } else {
                        runtime.position = Some(position.clone());
                    }
                    runtime.last_mid = position.entry_price;
                    log::warn!(
                        "[obi_scalper] 启动时接管已有仓位 symbol={} side={:?} qty={:.8} entry={:.8}",
                        symbol_config.symbol,
                        position.side,
                        position.qty,
                        position.entry_price
                    );
                }
                if self.config.execution.inventory_mode {
                    runtime.position = runtime.inventory_as_position_like();
                    runtime.phase = runtime.inventory_phase_like();
                } else if let Some(position) = runtime.position.as_ref() {
                    runtime.phase = match position.side {
                        PositionSideKind::Long => SymbolPhase::LongOpen,
                        PositionSideKind::Short => SymbolPhase::ShortOpen,
                    };
                }
            }
            let actor = SymbolActor {
                config: self.config.clone(),
                account: account.clone(),
                runtime,
                states: self.states.clone(),
                summaries: self.summaries.clone(),
                risk: risk.clone(),
                rx,
            };
            handles.push(tokio::spawn(async move {
                actor.run().await;
            }));
        }
        let symbols: Vec<String> = enabled.iter().map(|s| s.symbol.clone()).collect();
        handles.push(tokio::spawn(run_depth_ws_loop(
            symbols.clone(),
            self.config.account.market_type,
            self.config.global_risk.emergency_close_on_disconnect_ms,
            self.senders.clone(),
        )));
        handles.push(tokio::spawn(run_user_ws_loop(
            self.config.clone(),
            account.clone(),
            self.senders.clone(),
        )));
        handles.push(tokio::spawn(run_summary_loop(
            self.config.clone(),
            self.summaries.clone(),
            self.http.clone(),
        )));
        log::info!(
            "[obi_scalper] started actor_mode websocket_depth20+user_stream symbols={} dry_run={}",
            enabled.len(),
            self.config.strategy.dry_run
        );
        Ok(())
    }
    pub async fn stop(&self) -> Result<()> {
        let mut handles = self.handles.lock().await;
        for handle in handles.drain(..) {
            handle.abort();
        }
        Ok(())
    }
}

struct SymbolActor {
    config: ObiScalperConfig,
    account: Arc<AccountInfo>,
    runtime: SymbolRuntime,
    states: Arc<DashMap<String, MarketState>>,
    summaries: Arc<DashMap<String, SymbolSummary>>,
    risk: Arc<Mutex<GlobalRiskState>>,
    rx: mpsc::Receiver<SymbolEvent>,
}
impl SymbolActor {
    async fn run(mut self) {
        let mut timer = tokio::time::interval(Duration::from_millis(100));
        loop {
            tokio::select! { Some(event) = self.rx.recv() => { if let Err(err) = self.handle_event(event).await { log::warn!("[obi_scalper] actor处理失败 symbol={} err={}", self.runtime.config.symbol, err); } } _ = timer.tick() => { let _ = self.handle_event(SymbolEvent::Timer).await; } else => break }
        }
    }
    async fn handle_event(&mut self, event: SymbolEvent) -> Result<()> {
        match event {
            SymbolEvent::Book { book, received_at } => self.process_book(book, received_at).await,
            SymbolEvent::OrderUpdate(update) => self.handle_order_update(update).await,
            SymbolEvent::Timer => self.handle_timer().await,
        }
    }
    async fn process_book(&mut self, book: FixedOrderBook<20>, received_at: Instant) -> Result<()> {
        let latency_ms = received_at.elapsed().as_millis() as u64;
        if latency_ms > self.config.execution.max_book_latency_ms.max(100) {
            log::warn!(
                "[obi_scalper] 丢弃过期行情 symbol={} latency={}ms max={}ms",
                self.runtime.config.symbol,
                latency_ms,
                self.config.execution.max_book_latency_ms
            );
            return Ok(());
        }
        self.runtime.book = book;
        self.runtime.last_book_at = Instant::now();
        let Some(mid) = self.runtime.book.mid_price() else {
            return Ok(());
        };
        if self.runtime.phase == SymbolPhase::DataStale
            && self.runtime.position.is_none()
            && self.runtime.pending.is_none()
        {
            self.runtime.phase = SymbolPhase::Flat;
            log::info!(
                "[obi_scalper] 行情恢复，退出DataStale symbol={}",
                self.runtime.config.symbol
            );
        }
        let spread = self.runtime.book.spread_bps().unwrap_or(f64::MAX);
        let obi = self
            .runtime
            .book
            .weighted_obi(self.config.signal.obi_levels);
        let fast = self.runtime.ema_fast.update(obi);
        let slow = self.runtime.ema_slow.update(obi);
        let price_delta_bps = if self.runtime.last_mid > 0.0 {
            (mid - self.runtime.last_mid) / self.runtime.last_mid * 10_000.0
        } else {
            0.0
        };
        self.runtime.last_mid = mid;
        self.states.insert(
            self.runtime.config.symbol.clone(),
            MarketState {
                symbol: self.runtime.config.symbol.clone(),
                mid,
                spread_bps: spread,
                obi,
                ema_fast: fast,
                ema_slow: slow,
                updated_at: Utc::now(),
            },
        );
        if self.config.execution.inventory_mode {
            self.handle_inventory_exits(mid, fast).await?;
            self.log_signal_snapshot(mid, spread, obi, fast, slow, price_delta_bps);
            self.maybe_enter_inventory(mid, spread, fast, slow, price_delta_bps)
                .await?;
        } else {
            self.handle_open_position(mid, fast).await?;
            self.log_signal_snapshot(mid, spread, obi, fast, slow, price_delta_bps);
            self.maybe_enter_position(mid, spread, fast, slow, price_delta_bps)
                .await?;
        }
        self.publish_summary();
        if latency_ms > 50 {
            log::warn!(
                "[obi_scalper] 行情处理延迟 symbol={} latency={}ms mid={:.8} obi={:.4} fast={:.4}",
                self.runtime.config.symbol,
                latency_ms,
                mid,
                obi,
                fast
            );
        }
        Ok(())
    }
    async fn handle_timer(&mut self) -> Result<()> {
        self.check_data_stale().await;
        self.handle_pending_timeout().await?;
        self.publish_summary();
        Ok(())
    }
    async fn handle_order_update(&mut self, update: OrderUpdateEvent) -> Result<()> {
        let Some(pending) = self.runtime.pending.clone() else {
            return Ok(());
        };
        if update.order_id != pending.order_id && update.client_order_id != pending.client_order_id
        {
            return Ok(());
        }
        log::info!("[obi_scalper] 成交回报 symbol={} order_id={} client_id={} status={:?} last_qty={:.8} cum_qty={:.8} avg={:.8} maker={}", self.runtime.config.symbol, update.order_id, update.client_order_id, update.status, update.last_filled_qty, update.cumulative_filled_qty, update.avg_price, update.is_maker);
        match update.status {
            OrderStatus::Closed => self.apply_full_fill(pending, update).await?,
            OrderStatus::PartiallyFilled => self.apply_partial_fill(pending, update).await?,
            OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Rejected => {
                self.apply_terminal_unfilled(pending, update).await?
            }
            _ => {}
        }
        self.publish_summary();
        Ok(())
    }
    async fn apply_full_fill(
        &mut self,
        pending: PendingOrderState,
        update: OrderUpdateEvent,
    ) -> Result<()> {
        let fill_qty = update.cumulative_filled_qty.max(pending.qty);
        let fill_price = effective_fill_price(&update, pending.price);
        if pending.is_entry {
            let fee = effective_fee(&update, fill_qty * fill_price, pending.taker, &self.config);
            self.runtime.position = Some(PositionState {
                side: pending.side,
                qty: fill_qty,
                entry_price: fill_price,
                opened_at: Instant::now(),
                opened_at_wall: update.event_time,
                entry_fee: fee,
            });
            self.runtime.phase = match pending.side {
                PositionSideKind::Long => SymbolPhase::LongOpen,
                PositionSideKind::Short => SymbolPhase::ShortOpen,
            };
            self.runtime.pending = None;
            self.update_risk().await;
        } else if let Some(position) = self.runtime.position.clone() {
            let fee = effective_fee(
                &update,
                position.qty * fill_price,
                pending.taker,
                &self.config,
            );
            self.finalize_close(&position, fill_price, fee, "order_fill")
                .await;
        }
        Ok(())
    }
    async fn apply_partial_fill(
        &mut self,
        pending: PendingOrderState,
        update: OrderUpdateEvent,
    ) -> Result<()> {
        if self.config.execution.inventory_mode {
            self.apply_inventory_fill(&pending, &update, false).await?;
            self.runtime.pending = None;
            self.runtime.cancel_cooldown_until = Some(
                Instant::now()
                    + Duration::from_millis(self.config.execution.post_cancel_cooldown_ms),
            );
            log::info!(
                "[obi_scalper] 库存模式部分成交已入池 symbol={} entry={} side={:?} qty={:.8}",
                self.runtime.config.symbol,
                pending.is_entry,
                pending.side,
                update.cumulative_filled_qty
            );
            return Ok(());
        }
        if pending.is_entry {
            log::info!(
                "[obi_scalper] 开仓部分成交等待完成 symbol={} qty={:.8}",
                self.runtime.config.symbol,
                update.cumulative_filled_qty
            );
        } else {
            log::warn!(
                "[obi_scalper] 平仓部分成交 symbol={} qty={:.8}，继续等待/超时后重试",
                self.runtime.config.symbol,
                update.cumulative_filled_qty
            );
        }
        Ok(())
    }
    async fn apply_terminal_unfilled(
        &mut self,
        pending: PendingOrderState,
        update: OrderUpdateEvent,
    ) -> Result<()> {
        if update.status == OrderStatus::Rejected {
            self.runtime.order_rejects += 1;
        }
        if update.cumulative_filled_qty > 0.0 {
            if self.config.execution.inventory_mode {
                self.apply_inventory_fill(&pending, &update, true).await?;
            } else {
                self.apply_full_fill(pending, update).await?;
            }
            return Ok(());
        }
        self.runtime.pending = None;
        self.runtime.phase = match self.runtime.position.as_ref().map(|p| p.side) {
            Some(PositionSideKind::Long) => SymbolPhase::LongOpen,
            Some(PositionSideKind::Short) => SymbolPhase::ShortOpen,
            None => SymbolPhase::Flat,
        };
        Ok(())
    }
    async fn handle_pending_timeout(&mut self) -> Result<()> {
        let Some(pending) = self.runtime.pending.clone() else {
            return Ok(());
        };
        if Instant::now() < pending.deadline {
            return Ok(());
        }
        log::info!(
            "[obi_scalper] 订单超时撤单 symbol={} order_id={} client_id={} entry={} retries={}",
            self.runtime.config.symbol,
            pending.order_id,
            pending.client_order_id,
            pending.is_entry,
            pending.retries
        );
        if !self.config.strategy.dry_run && !pending.order_id.starts_with("dry-") {
            if let Err(err) = self
                .account
                .exchange
                .cancel_order(
                    &pending.order_id,
                    &self.runtime.config.symbol,
                    self.config.account.market_type,
                )
                .await
            {
                log::warn!(
                    "[obi_scalper] 超时撤单失败 symbol={} order_id={} err={}",
                    self.runtime.config.symbol,
                    pending.order_id,
                    err
                );
            }

            if self.reconcile_timed_out_order(&pending).await? {
                return Ok(());
            }
        }
        self.runtime.pending = None;
        self.runtime.phase = match self.runtime.position.as_ref().map(|p| p.side) {
            Some(PositionSideKind::Long) => SymbolPhase::LongOpen,
            Some(PositionSideKind::Short) => SymbolPhase::ShortOpen,
            None => SymbolPhase::Flat,
        };
        Ok(())
    }
    async fn reconcile_timed_out_order(&mut self, pending: &PendingOrderState) -> Result<bool> {
        match self
            .account
            .exchange
            .get_order(
                &pending.order_id,
                &self.runtime.config.symbol,
                self.config.account.market_type,
            )
            .await
        {
            Ok(order) => {
                if order.filled > 0.0
                    || matches!(
                        order.status,
                        OrderStatus::Closed | OrderStatus::PartiallyFilled
                    )
                {
                    let update = order_update_from_order(&order, pending, Utc::now());
                    if self.config.execution.inventory_mode {
                        self.apply_inventory_fill(&pending.clone(), &update, true)
                            .await?;
                        self.runtime.pending = None;
                    } else if matches!(update.status, OrderStatus::PartiallyFilled) {
                        self.apply_partial_fill(pending.clone(), update).await?;
                    } else {
                        self.apply_full_fill(pending.clone(), update).await?;
                    }
                    return Ok(true);
                }
            }
            Err(err) => {
                log::warn!(
                    "[obi_scalper] 超时撤单后查单失败 symbol={} order_id={} err={}，开始同步交易所仓位",
                    self.runtime.config.symbol,
                    pending.order_id,
                    err
                );
            }
        }

        self.sync_position_from_exchange().await
    }

    async fn sync_position_from_exchange(&mut self) -> Result<bool> {
        let positions = self
            .account
            .exchange
            .get_positions(Some(&self.runtime.config.symbol))
            .await?;
        let active: Vec<Position> = positions
            .into_iter()
            .filter(|position| position.contracts.abs() > self.runtime.config.min_qty * 0.1)
            .collect();
        if active.is_empty() {
            self.runtime.position = None;
            self.runtime.inventory = InventoryPool::default();
            self.runtime.pending = None;
            self.runtime.phase = SymbolPhase::Flat;
            self.update_risk().await;
            return Ok(false);
        }
        if self.config.execution.inventory_mode {
            self.runtime.inventory = InventoryPool::default();
            for position in &active {
                let side = position_side_from_exchange(position)?;
                match side {
                    PositionSideKind::Long => self.runtime.inventory.long.add_fill(
                        position.contracts.abs(),
                        position.entry_price,
                        0.0,
                    ),
                    PositionSideKind::Short => self.runtime.inventory.short.add_fill(
                        position.contracts.abs(),
                        position.entry_price,
                        0.0,
                    ),
                }
                self.runtime.last_mid = position.mark_price.max(position.entry_price);
            }
            self.runtime.position = self.inventory_as_position();
            self.runtime.pending = None;
            self.runtime.phase = self.inventory_phase();
            self.update_risk().await;
            log::warn!(
                "[obi_scalper] 已从交易所同步库存池 symbol={} long={:.8}@{:.8} short={:.8}@{:.8}",
                self.runtime.config.symbol,
                self.runtime.inventory.long.qty,
                self.runtime.inventory.long.avg_price,
                self.runtime.inventory.short.qty,
                self.runtime.inventory.short.avg_price
            );
            return Ok(true);
        }
        if active.len() > 1 {
            return Err(anyhow!(
                "{} 交易所存在多个方向仓位，OBI停止继续开仓，请人工处理",
                self.runtime.config.symbol
            ));
        }
        let position = &active[0];
        let side = position_side_from_exchange(position)?;
        self.runtime.position = Some(PositionState {
            side,
            qty: position.contracts.abs(),
            entry_price: position.entry_price,
            opened_at: Instant::now(),
            opened_at_wall: position.timestamp,
            entry_fee: 0.0,
        });
        self.runtime.pending = None;
        self.runtime.phase = match side {
            PositionSideKind::Long => SymbolPhase::LongOpen,
            PositionSideKind::Short => SymbolPhase::ShortOpen,
        };
        self.runtime.last_mid = position.mark_price.max(position.entry_price);
        self.update_risk().await;
        log::warn!(
            "[obi_scalper] 已从交易所同步仓位 symbol={} side={:?} qty={:.8} entry={:.8} mark={:.8}",
            self.runtime.config.symbol,
            side,
            position.contracts.abs(),
            position.entry_price,
            position.mark_price
        );
        Ok(true)
    }

    async fn check_data_stale(&mut self) {
        if self.runtime.last_book_at.elapsed()
            > Duration::from_millis(
                self.config
                    .global_risk
                    .emergency_close_on_disconnect_ms
                    .max(1000),
            )
            && self.runtime.position.is_none()
            && self.runtime.pending.is_none()
        {
            self.runtime.phase = SymbolPhase::DataStale;
        }
    }
    async fn handle_open_position(&mut self, mid: f64, fast: f64) -> Result<()> {
        let Some(position) = self.runtime.position.clone() else {
            return Ok(());
        };
        if self.runtime.pending.is_some() {
            return Ok(());
        }
        let pnl_pct = match position.side {
            PositionSideKind::Long => (mid - position.entry_price) / position.entry_price,
            PositionSideKind::Short => (position.entry_price - mid) / position.entry_price,
        };
        let age_ms = position.opened_at.elapsed().as_millis() as u64;
        let signal_exit = match position.side {
            PositionSideKind::Long => fast < self.config.signal.exit_threshold,
            PositionSideKind::Short => fast > -self.config.signal.exit_threshold,
        };
        let stop_loss = pnl_pct <= -self.runtime.config.stop_loss_pct;
        let timeout = age_ms >= self.runtime.config.max_holding_ms;
        if signal_exit || stop_loss || timeout {
            let reason = if stop_loss {
                "stop_loss"
            } else if timeout {
                "timeout"
            } else {
                "signal_exit"
            };
            self.close_position(position.side, mid, reason).await?;
        }
        Ok(())
    }
    async fn maybe_enter_position(
        &mut self,
        mid: f64,
        spread_bps: f64,
        fast: f64,
        slow: f64,
        price_delta_bps: f64,
    ) -> Result<()> {
        if self.runtime.position.is_some()
            || self.runtime.pending.is_some()
            || self.runtime.phase == SymbolPhase::DataStale
        {
            return Ok(());
        }
        if self.runtime.realized_pnl_day <= -self.config.global_risk.max_daily_loss.abs() {
            return Ok(());
        }
        if let Some(until) = self.runtime.cooldown_until {
            if Instant::now() < until {
                return Ok(());
            }
            self.runtime.cooldown_until = None;
            self.runtime.phase = SymbolPhase::Flat;
        }
        {
            let risk = self.risk.lock().await;
            if risk.realized_pnl_day <= -self.config.global_risk.max_daily_loss.abs()
                || risk.open_notional >= self.config.global_risk.max_total_notional
                || risk.open_symbols >= self.config.global_risk.max_open_symbols
            {
                return Ok(());
            }
        }
        if spread_bps > self.config.signal.max_spread_bps
            || self.runtime.last_signal_at.elapsed()
                < Duration::from_millis(self.config.signal.min_signal_hold_ms)
        {
            return Ok(());
        }
        let stable_or_up = price_delta_bps >= -self.config.signal.price_stable_bps;
        let stable_or_down = price_delta_bps <= self.config.signal.price_stable_bps;
        if fast > self.config.signal.long_entry_threshold && fast > slow && stable_or_up {
            self.open_position(PositionSideKind::Long, mid, false, "obi_long")
                .await?;
            self.runtime.last_signal_at = Instant::now();
        } else if fast < self.config.signal.short_entry_threshold && fast < slow && stable_or_down {
            self.open_position(PositionSideKind::Short, mid, false, "obi_short")
                .await?;
            self.runtime.last_signal_at = Instant::now();
        }
        Ok(())
    }
    fn log_signal_snapshot(
        &mut self,
        mid: f64,
        spread_bps: f64,
        obi: f64,
        fast: f64,
        slow: f64,
        price_delta_bps: f64,
    ) {
        if self.runtime.last_key_log_at.elapsed()
            < Duration::from_millis(self.config.logging.key_log_every_ms.max(100))
        {
            return;
        }
        self.runtime.last_key_log_at = Instant::now();
        log::info!(
            "[obi_scalper] signal symbol={} phase={:?} mid={:.8} spread_bps={:.3} obi={:.4} fast={:.4} slow={:.4} delta_bps={:.3} long_th={:.3} short_th={:.3} pos={} pending={}",
            self.runtime.config.symbol,
            self.runtime.phase,
            mid,
            spread_bps,
            obi,
            fast,
            slow,
            price_delta_bps,
            self.config.signal.long_entry_threshold,
            self.config.signal.short_entry_threshold,
            self.runtime.position.is_some(),
            self.runtime.pending.is_some()
        );
    }
    async fn handle_inventory_exits(&mut self, mid: f64, _fast: f64) -> Result<()> {
        if self.runtime.pending.is_some() || mid <= 0.0 {
            return Ok(());
        }
        let tp_bps = self.inventory_take_profit_bps(mid);
        if self.runtime.inventory.long.qty > 0.0 {
            let pnl_bps = (mid - self.runtime.inventory.long.avg_price)
                / self.runtime.inventory.long.avg_price
                * 10_000.0;
            if pnl_bps >= tp_bps {
                self.close_inventory_leg(PositionSideKind::Long, mid, "inventory_take_profit")
                    .await?;
                return Ok(());
            }
        }
        if self.runtime.inventory.short.qty > 0.0 {
            let pnl_bps = (self.runtime.inventory.short.avg_price - mid)
                / self.runtime.inventory.short.avg_price
                * 10_000.0;
            if pnl_bps >= tp_bps {
                self.close_inventory_leg(PositionSideKind::Short, mid, "inventory_take_profit")
                    .await?;
            }
        }
        Ok(())
    }
    async fn maybe_enter_inventory(
        &mut self,
        mid: f64,
        spread_bps: f64,
        fast: f64,
        slow: f64,
        price_delta_bps: f64,
    ) -> Result<()> {
        if self.runtime.pending.is_some()
            || self.runtime.phase == SymbolPhase::DataStale
            || mid <= 0.0
        {
            return Ok(());
        }
        if let Some(until) = self.runtime.cancel_cooldown_until {
            if Instant::now() < until {
                return Ok(());
            }
            self.runtime.cancel_cooldown_until = None;
        }
        if self.runtime.last_signal_at.elapsed()
            < Duration::from_millis(
                self.config
                    .execution
                    .min_entry_interval_ms
                    .max(self.config.signal.min_signal_hold_ms),
            )
        {
            return Ok(());
        }
        if self.runtime.realized_pnl_day <= -self.config.global_risk.max_daily_loss.abs()
            || spread_bps > self.config.signal.max_spread_bps
        {
            return Ok(());
        }
        let stable_or_up = price_delta_bps >= -self.config.signal.price_stable_bps;
        let stable_or_down = price_delta_bps <= self.config.signal.price_stable_bps;
        let long_notional = self.runtime.inventory.long.notional(mid);
        let short_notional = self.runtime.inventory.short.notional(mid);
        let gross_notional = long_notional + short_notional;
        let order_notional = self
            .runtime
            .config
            .order_notional
            .unwrap_or(self.config.execution.order_notional);
        if gross_notional + order_notional > self.config.execution.max_inventory_notional {
            return Ok(());
        }
        let long_threshold = self.inventory_entry_threshold(long_notional, true);
        let short_threshold = -self.inventory_entry_threshold(short_notional, false);
        if fast > long_threshold && fast > slow && stable_or_up {
            self.open_position(PositionSideKind::Long, mid, false, "obi_long")
                .await?;
            self.runtime.last_signal_at = Instant::now();
        } else if fast < short_threshold && fast < slow && stable_or_down {
            self.open_position(PositionSideKind::Short, mid, false, "obi_short")
                .await?;
            self.runtime.last_signal_at = Instant::now();
        }
        Ok(())
    }
    fn inventory_entry_threshold(&self, notional: f64, long_side: bool) -> f64 {
        let base = if long_side {
            self.config.signal.long_entry_threshold.abs()
        } else {
            self.config.signal.short_entry_threshold.abs()
        };
        if notional >= self.config.execution.hard_inventory_notional {
            base.max(self.config.signal.strong_signal_threshold.min(0.75))
        } else if notional >= self.config.execution.soft_inventory_notional {
            (base * 1.5).max(0.25)
        } else {
            base
        }
    }
    fn inventory_take_profit_bps(&self, mid: f64) -> f64 {
        let gross = self.runtime.inventory.gross_notional(mid);
        if gross >= self.config.execution.hard_inventory_notional {
            self.config.execution.min_take_profit_bps
        } else if gross >= self.config.execution.soft_inventory_notional {
            (self.config.execution.take_profit_bps + self.config.execution.min_take_profit_bps)
                * 0.5
        } else {
            self.config.execution.take_profit_bps
        }
    }
    async fn close_inventory_leg(
        &mut self,
        side: PositionSideKind,
        mid: f64,
        reason: &str,
    ) -> Result<()> {
        let leg_qty = match side {
            PositionSideKind::Long => self.runtime.inventory.long.qty,
            PositionSideKind::Short => self.runtime.inventory.short.qty,
        };
        if leg_qty <= 0.0 {
            return Ok(());
        }
        let notional = self
            .runtime
            .config
            .order_notional
            .unwrap_or(self.config.execution.order_notional);
        let qty = quantize_qty(
            (notional / mid)
                .min(leg_qty)
                .max(self.runtime.config.min_qty),
            self.runtime.config.step_size,
        );
        if qty <= 0.0 || qty * mid < self.runtime.config.min_notional {
            return Ok(());
        }
        let taker = false;
        let price = exit_price(
            &self.runtime.book,
            side,
            self.runtime.config.tick_size,
            taker,
        )
        .unwrap_or(mid);
        let order = self
            .submit_order(side, false, qty, price, taker, reason)
            .await?;
        self.runtime.phase = match side {
            PositionSideKind::Long => SymbolPhase::PendingExitLong,
            PositionSideKind::Short => SymbolPhase::PendingExitShort,
        };
        self.runtime.pending = Some(PendingOrderState {
            order_id: order.id.clone(),
            client_order_id: order.client_order_id().unwrap_or_default(),
            side,
            is_entry: false,
            qty,
            price,
            submitted_at: Instant::now(),
            deadline: Instant::now()
                + Duration::from_millis(self.config.execution.exit_order_ttl_ms),
            retries: 0,
            taker,
        });
        if self.config.strategy.dry_run || order.status == OrderStatus::Closed || order.filled > 0.0
        {
            let update = OrderUpdateEvent {
                symbol: self.runtime.config.symbol.clone(),
                order_id: order.id.clone(),
                client_order_id: order.client_order_id().unwrap_or_default(),
                side: match side {
                    PositionSideKind::Long => OrderSide::Sell,
                    PositionSideKind::Short => OrderSide::Buy,
                },
                status: OrderStatus::Closed,
                last_filled_qty: order.filled.max(qty),
                cumulative_filled_qty: order.filled.max(qty),
                avg_price: price,
                last_price: price,
                commission: 0.0,
                is_maker: !taker,
                event_time: Utc::now(),
            };
            if let Some(pending) = self.runtime.pending.clone() {
                self.apply_inventory_fill(&pending, &update, true).await?;
            }
        }
        log::info!(
            "[obi_scalper] 库存止盈提交 symbol={} side={:?} qty={:.8} price={:.8} reason={}",
            self.runtime.config.symbol,
            side,
            qty,
            price,
            reason
        );
        Ok(())
    }
    async fn apply_inventory_fill(
        &mut self,
        pending: &PendingOrderState,
        update: &OrderUpdateEvent,
        terminal: bool,
    ) -> Result<()> {
        let fill_qty = update
            .cumulative_filled_qty
            .min(pending.qty)
            .max(update.last_filled_qty)
            .max(0.0);
        if fill_qty <= 0.0 {
            return Ok(());
        }
        let price = effective_fill_price(update, pending.price);
        let fee = effective_fee(update, fill_qty * price, pending.taker, &self.config);
        if pending.is_entry {
            match pending.side {
                PositionSideKind::Long => {
                    self.runtime.inventory.long.add_fill(fill_qty, price, fee)
                }
                PositionSideKind::Short => {
                    self.runtime.inventory.short.add_fill(fill_qty, price, fee)
                }
            }
        } else {
            let (entry_price, entry_fee) = match pending.side {
                PositionSideKind::Long => (
                    self.runtime.inventory.long.avg_price,
                    self.runtime.inventory.long.reduce_fill(fill_qty),
                ),
                PositionSideKind::Short => (
                    self.runtime.inventory.short.avg_price,
                    self.runtime.inventory.short.reduce_fill(fill_qty),
                ),
            };
            let gross = match pending.side {
                PositionSideKind::Long => (price - entry_price) * fill_qty,
                PositionSideKind::Short => (entry_price - price) * fill_qty,
            };
            let pnl = gross - entry_fee - fee;
            self.runtime.realized_pnl_30m += pnl;
            self.runtime.realized_pnl_day += pnl;
            self.runtime.fees_30m += entry_fee + fee;
            self.runtime.fees_day += entry_fee + fee;
            self.runtime.trades_30m += 1;
            if pnl >= 0.0 {
                self.runtime.wins_30m += 1;
                self.runtime.consecutive_losses = 0;
            } else {
                self.runtime.losses_30m += 1;
                self.runtime.consecutive_losses += 1;
            }
            let mut risk = self.risk.lock().await;
            risk.realized_pnl_day += pnl;
            log::info!("[obi_scalper] 库存减仓完成 symbol={} side={:?} qty={:.8} price={:.8} pnl={:.6} gross={:.6}", self.runtime.config.symbol, pending.side, fill_qty, price, pnl, gross);
        }
        self.runtime.position = self.inventory_as_position();
        self.runtime.phase = self.inventory_phase();
        if terminal {
            self.runtime.pending = None;
        }
        self.update_risk().await;
        Ok(())
    }
    fn inventory_as_position(&self) -> Option<PositionState> {
        let long_n = self.runtime.inventory.long.notional(self.runtime.last_mid);
        let short_n = self.runtime.inventory.short.notional(self.runtime.last_mid);
        if long_n <= 0.0 && short_n <= 0.0 {
            return None;
        }
        let (side, leg) = if long_n >= short_n {
            (PositionSideKind::Long, &self.runtime.inventory.long)
        } else {
            (PositionSideKind::Short, &self.runtime.inventory.short)
        };
        Some(PositionState {
            side,
            qty: leg.qty,
            entry_price: leg.avg_price,
            opened_at: Instant::now(),
            opened_at_wall: Utc::now(),
            entry_fee: leg.fee,
        })
    }
    fn inventory_phase(&self) -> SymbolPhase {
        let long_n = self.runtime.inventory.long.notional(self.runtime.last_mid);
        let short_n = self.runtime.inventory.short.notional(self.runtime.last_mid);
        if long_n <= 0.0 && short_n <= 0.0 {
            SymbolPhase::Flat
        } else if long_n >= short_n {
            SymbolPhase::LongOpen
        } else {
            SymbolPhase::ShortOpen
        }
    }

    async fn open_position(
        &mut self,
        side: PositionSideKind,
        mid: f64,
        taker: bool,
        reason: &str,
    ) -> Result<()> {
        let notional = self
            .runtime
            .config
            .order_notional
            .unwrap_or(self.config.execution.order_notional);
        let qty = quantize_qty(
            (notional / mid).max(self.runtime.config.min_qty),
            self.runtime.config.step_size,
        );
        if qty * mid < self.runtime.config.min_notional {
            return Ok(());
        }
        if self.config.execution.inventory_mode {
            let side_notional = match side {
                PositionSideKind::Long => self.runtime.inventory.long.notional(mid),
                PositionSideKind::Short => self.runtime.inventory.short.notional(mid),
            };
            let gross_notional = self.runtime.inventory.gross_notional(mid);
            if side_notional + qty * mid > self.config.execution.max_inventory_notional
                || gross_notional + qty * mid > self.config.execution.max_inventory_notional
            {
                return Ok(());
            }
        } else if self.runtime.config.max_symbol_notional > 0.0
            && qty * mid > self.runtime.config.max_symbol_notional
        {
            return Ok(());
        }
        let effective_taker = taker && self.config.execution.allow_taker_entry;
        let price = entry_price(
            &self.runtime.book,
            side,
            self.runtime.config.tick_size,
            effective_taker,
        )
        .unwrap_or(mid);
        let order = self
            .submit_order(side, true, qty, price, effective_taker, reason)
            .await?;
        self.runtime.phase = match side {
            PositionSideKind::Long => SymbolPhase::PendingEntryLong,
            PositionSideKind::Short => SymbolPhase::PendingEntryShort,
        };
        self.runtime.pending = Some(PendingOrderState {
            order_id: order.id.clone(),
            client_order_id: order.client_order_id().unwrap_or_default(),
            side,
            is_entry: true,
            qty,
            price,
            submitted_at: Instant::now(),
            deadline: Instant::now()
                + Duration::from_millis(self.config.execution.entry_order_ttl_ms),
            retries: 0,
            taker: effective_taker,
        });
        if self.config.strategy.dry_run || order.status == OrderStatus::Closed || order.filled > 0.0
        {
            let filled = if order.filled > 0.0 {
                order.filled
            } else {
                qty
            };
            let fee = fee_for(filled * price, effective_taker, &self.config);
            if self.config.execution.inventory_mode {
                let update = OrderUpdateEvent {
                    symbol: self.runtime.config.symbol.clone(),
                    order_id: order.id.clone(),
                    client_order_id: order.client_order_id().unwrap_or_default(),
                    side: match side {
                        PositionSideKind::Long => OrderSide::Buy,
                        PositionSideKind::Short => OrderSide::Sell,
                    },
                    status: OrderStatus::Closed,
                    last_filled_qty: filled,
                    cumulative_filled_qty: filled,
                    avg_price: price,
                    last_price: price,
                    commission: fee,
                    is_maker: !effective_taker,
                    event_time: Utc::now(),
                };
                if let Some(pending) = self.runtime.pending.clone() {
                    self.apply_inventory_fill(&pending, &update, true).await?;
                }
            } else {
                self.runtime.position = Some(PositionState {
                    side,
                    qty: filled,
                    entry_price: price,
                    opened_at: Instant::now(),
                    opened_at_wall: Utc::now(),
                    entry_fee: fee,
                });
                self.runtime.phase = match side {
                    PositionSideKind::Long => SymbolPhase::LongOpen,
                    PositionSideKind::Short => SymbolPhase::ShortOpen,
                };
                self.runtime.pending = None;
                self.update_risk().await;
            }
        }
        log::info!("[obi_scalper] 开仓提交 symbol={} side={:?} qty={:.8} price={:.8} taker={} reason={} dry_run={}", self.runtime.config.symbol, side, qty, price, effective_taker, reason, self.config.strategy.dry_run);
        Ok(())
    }
    async fn close_position(
        &mut self,
        side: PositionSideKind,
        mid: f64,
        reason: &str,
    ) -> Result<()> {
        if self.config.execution.inventory_mode {
            return self.close_inventory_leg(side, mid, reason).await;
        }
        let Some(position) = self.runtime.position.clone() else {
            return Ok(());
        };
        let taker = self.config.execution.allow_taker_exit && reason == "stop_loss";
        let price = exit_price(
            &self.runtime.book,
            side,
            self.runtime.config.tick_size,
            taker,
        )
        .unwrap_or(mid);
        let order = self
            .submit_order(side, false, position.qty, price, taker, reason)
            .await?;
        self.runtime.phase = match side {
            PositionSideKind::Long => SymbolPhase::PendingExitLong,
            PositionSideKind::Short => SymbolPhase::PendingExitShort,
        };
        self.runtime.pending = Some(PendingOrderState {
            order_id: order.id.clone(),
            client_order_id: order.client_order_id().unwrap_or_default(),
            side,
            is_entry: false,
            qty: position.qty,
            price,
            submitted_at: Instant::now(),
            deadline: Instant::now()
                + Duration::from_millis(self.config.execution.exit_order_ttl_ms),
            retries: 0,
            taker,
        });
        if self.config.strategy.dry_run || order.status == OrderStatus::Closed || order.filled > 0.0
        {
            let fill_price = price;
            let fee = fee_for(position.qty * fill_price, taker, &self.config);
            self.finalize_close(&position, fill_price, fee, reason)
                .await;
        }
        Ok(())
    }
    async fn submit_order(
        &mut self,
        position_side: PositionSideKind,
        is_entry: bool,
        qty: f64,
        price: f64,
        taker: bool,
        reason: &str,
    ) -> Result<ObiOrder> {
        let side = match (position_side, is_entry) {
            (PositionSideKind::Long, true) => OrderSide::Buy,
            (PositionSideKind::Long, false) => OrderSide::Sell,
            (PositionSideKind::Short, true) => OrderSide::Sell,
            (PositionSideKind::Short, false) => OrderSide::Buy,
        };
        let position_side_str = match position_side {
            PositionSideKind::Long => "LONG",
            PositionSideKind::Short => "SHORT",
        };
        let tif = if taker { "IOC" } else { "GTX" };
        let client_id = format!(
            "obi{}{}{}",
            self.runtime.config.symbol.replace('/', ""),
            Utc::now().timestamp_millis() % 1_000_000_000,
            short_reason_code(reason)
        );
        if self.config.strategy.dry_run {
            return Ok(ObiOrder {
                id: format!("dry-{client_id}"),
                filled: qty,
                status: OrderStatus::Closed,
                client_order_id: Some(client_id),
            });
        }
        let qty = normalize_to_step(qty, self.runtime.config.step_size);
        let price = normalize_to_step(price, self.runtime.config.tick_size);
        let mut params = HashMap::new();
        params.insert("positionSide".to_string(), position_side_str.to_string());
        let request = OrderRequest {
            symbol: self.runtime.config.symbol.clone(),
            side,
            order_type: OrderType::Limit,
            amount: qty,
            price: Some(price),
            market_type: self.config.account.market_type,
            params: Some(params),
            client_order_id: Some(client_id.clone()),
            time_in_force: Some(tif.to_string()),
            reduce_only: None,
            post_only: Some(!taker),
        };
        match self.account.exchange.create_order(request).await {
            Ok(order) => Ok(ObiOrder {
                id: order.id,
                filled: order.filled,
                status: order.status,
                client_order_id: Some(client_id),
            }),
            Err(err) => {
                self.runtime.order_submit_errors += 1;
                Err(anyhow!(
                    "submit_order failed symbol={} reason={} err={}",
                    self.runtime.config.symbol,
                    reason,
                    err
                ))
            }
        }
    }
    async fn finalize_close(
        &mut self,
        position: &PositionState,
        price: f64,
        exit_fee: f64,
        reason: &str,
    ) {
        let gross = match position.side {
            PositionSideKind::Long => (price - position.entry_price) * position.qty,
            PositionSideKind::Short => (position.entry_price - price) * position.qty,
        };
        let pnl = gross - position.entry_fee - exit_fee;
        self.runtime.realized_pnl_30m += pnl;
        self.runtime.realized_pnl_day += pnl;
        self.runtime.fees_30m += position.entry_fee + exit_fee;
        self.runtime.fees_day += position.entry_fee + exit_fee;
        self.runtime.trades_30m += 1;
        if pnl >= 0.0 {
            self.runtime.wins_30m += 1;
            self.runtime.consecutive_losses = 0;
        } else {
            self.runtime.losses_30m += 1;
            self.runtime.consecutive_losses += 1;
        }
        if pnl < 0.0 {
            let cooldown =
                if self.runtime.consecutive_losses >= self.runtime.config.max_consecutive_losses {
                    self.runtime.config.consecutive_loss_cooldown_ms
                } else {
                    self.runtime.config.cooldown_after_loss_ms
                };
            self.runtime.cooldown_until = Some(Instant::now() + Duration::from_millis(cooldown));
            self.runtime.phase = SymbolPhase::Cooldown;
        } else {
            self.runtime.phase = SymbolPhase::Flat;
        }
        self.runtime.position = None;
        self.runtime.pending = None;
        {
            let mut risk = self.risk.lock().await;
            risk.realized_pnl_day += pnl;
        }
        self.update_risk().await;
        log::info!("[obi_scalper] 平仓完成 symbol={} side={:?} qty={:.8} price={:.8} pnl={:.6} fee={:.6} reason={} consecutive_losses={}", self.runtime.config.symbol, position.side, position.qty, price, pnl, position.entry_fee + exit_fee, reason, self.runtime.consecutive_losses);
    }
    async fn update_risk(&self) {
        let mut risk = self.risk.lock().await;
        let own = if self.config.execution.inventory_mode {
            self.runtime.inventory.gross_notional(self.runtime.last_mid)
        } else {
            self.runtime
                .position
                .as_ref()
                .map(|p| p.qty * self.runtime.last_mid.max(p.entry_price))
                .unwrap_or(0.0)
        };
        if own > 0.0 {
            risk.symbol_notional
                .insert(self.runtime.config.symbol.clone(), own);
        } else {
            risk.symbol_notional.remove(&self.runtime.config.symbol);
        }
        risk.open_notional = risk.symbol_notional.values().sum();
        risk.open_symbols = risk.symbol_notional.len();
    }
    fn publish_summary(&self) {
        let position = if self.config.execution.inventory_mode {
            format!(
                "Pool long={:.6}@{:.6} short={:.6}@{:.6} gross={:.2} net={:.2}",
                self.runtime.inventory.long.qty,
                self.runtime.inventory.long.avg_price,
                self.runtime.inventory.short.qty,
                self.runtime.inventory.short.avg_price,
                self.runtime.inventory.gross_notional(self.runtime.last_mid),
                self.runtime.inventory.net_notional(self.runtime.last_mid)
            )
        } else {
            self.runtime
                .position
                .as_ref()
                .map(|p| {
                    format!(
                        "{:?} {:.6}@{:.6} age={}ms opened={}",
                        p.side,
                        p.qty,
                        p.entry_price,
                        p.opened_at.elapsed().as_millis(),
                        p.opened_at_wall.format("%H:%M:%S")
                    )
                })
                .unwrap_or_else(|| "Flat".to_string())
        };
        let pending = self
            .runtime
            .pending
            .as_ref()
            .map(|p| {
                format!(
                    "{:?} entry={} qty={:.6}@{:.6} age={}ms",
                    p.side,
                    p.is_entry,
                    p.qty,
                    p.price,
                    p.submitted_at.elapsed().as_millis()
                )
            })
            .unwrap_or_else(|| "None".to_string());
        self.summaries.insert(
            self.runtime.config.symbol.clone(),
            SymbolSummary {
                symbol: self.runtime.config.symbol.clone(),
                phase: format!("{:?}", self.runtime.phase),
                position,
                pending,
                trades_30m: self.runtime.trades_30m,
                wins_30m: self.runtime.wins_30m,
                losses_30m: self.runtime.losses_30m,
                fees_30m: self.runtime.fees_30m,
                realized_pnl_30m: self.runtime.realized_pnl_30m,
                realized_pnl_day: self.runtime.realized_pnl_day,
                order_submit_errors: self.runtime.order_submit_errors,
                order_rejects: self.runtime.order_rejects,
            },
        );
    }
}

#[derive(Debug)]
struct ObiOrder {
    id: String,
    filled: f64,
    status: OrderStatus,
    client_order_id: Option<String>,
}
impl ObiOrder {
    fn client_order_id(&self) -> Option<String> {
        self.client_order_id.clone()
    }
}

async fn run_depth_ws_loop(
    symbols: Vec<String>,
    market_type: MarketType,
    disconnect_ms: u64,
    senders: Arc<DashMap<String, mpsc::Sender<SymbolEvent>>>,
) {
    let url = build_depth_stream_url(&symbols, market_type);
    let mut reconnect_delay = Duration::from_secs(1);
    loop {
        log::info!(
            "[obi_scalper] 连接Binance深度WebSocket url={}",
            redact_ws_url(&url)
        );
        match connect_async(&url).await {
            Ok((mut stream, _)) => {
                log::info!(
                    "[obi_scalper] 深度WebSocket已连接 symbols={}",
                    symbols.join(",")
                );
                reconnect_delay = Duration::from_secs(1);
                let mut last_msg_at = Instant::now();
                loop {
                    let timeout = sleep(Duration::from_millis(disconnect_ms.max(1000)));
                    tokio::pin!(timeout);
                    tokio::select! { msg = stream.next() => { match msg { Some(Ok(Message::Text(text))) => { last_msg_at = Instant::now(); match parse_depth_message(&text) { Ok(Some((symbol, book))) => { if let Some(sender) = senders.get(&symbol) { let _ = sender.send(SymbolEvent::Book { book, received_at: Instant::now() }).await; } } Ok(None) => {} Err(err) => log::warn!("[obi_scalper] 深度消息解析失败 err={}", err), } } Some(Ok(Message::Close(frame))) => { log::warn!("[obi_scalper] 深度WebSocket关闭 frame={:?}", frame); break; } Some(Ok(_)) => {} Some(Err(err)) => { log::warn!("[obi_scalper] 深度WebSocket读取失败 err={}", err); break; } None => break } } _ = &mut timeout => { if last_msg_at.elapsed() >= Duration::from_millis(disconnect_ms.max(1000)) { log::warn!("[obi_scalper] 深度WebSocket超过{}ms无数据，重连", disconnect_ms.max(1000)); break; } } }
                }
            }
            Err(err) => log::warn!("[obi_scalper] 深度WebSocket连接失败 err={}", err),
        }
        sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));
    }
}
async fn run_user_ws_loop(
    config: ObiScalperConfig,
    account: Arc<AccountInfo>,
    senders: Arc<DashMap<String, mpsc::Sender<SymbolEvent>>>,
) {
    if config.strategy.dry_run {
        log::info!("[obi_scalper] dry_run=true，跳过用户成交WebSocket连接");
        return;
    }
    let mut backoff = Duration::from_secs(1);
    loop {
        let listen_key = match account
            .exchange
            .create_user_data_stream(config.account.market_type)
            .await
        {
            Ok(key) => key,
            Err(err) => {
                log::warn!("[obi_scalper] 获取listenKey失败 err={}", err);
                sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
                continue;
            }
        };
        let url = match config.account.market_type {
            MarketType::Spot => format!("wss://stream.binance.com:9443/ws/{listen_key}"),
            MarketType::Futures => format!("wss://fstream.binance.com/ws/{listen_key}"),
        };
        match connect_async(&url).await {
            Ok((mut stream, _)) => {
                log::info!("[obi_scalper] 用户成交WebSocket已连接");
                backoff = Duration::from_secs(1);
                let mut keepalive = tokio::time::interval(Duration::from_secs(20 * 60));
                loop {
                    tokio::select! { msg = stream.next() => { match msg { Some(Ok(Message::Text(text))) => match parse_order_update_message(&text) { Ok(Some(update)) => { if let Some(sender) = senders.get(&update.symbol) { let _ = sender.send(SymbolEvent::OrderUpdate(update)).await; } } Ok(None) => {} Err(err) => log::warn!("[obi_scalper] 用户成交消息解析失败 err={}", err), }, Some(Ok(Message::Close(frame))) => { log::warn!("[obi_scalper] 用户成交WebSocket关闭 frame={:?}", frame); break; } Some(Ok(_)) => {} Some(Err(err)) => { log::warn!("[obi_scalper] 用户成交WebSocket读取失败 err={}", err); break; } None => break } } _ = keepalive.tick() => { if let Err(err) = account.exchange.keepalive_user_data_stream(&listen_key, config.account.market_type).await { log::warn!("[obi_scalper] listenKey续期失败 err={}", err); break; } } }
                }
            }
            Err(err) => log::warn!("[obi_scalper] 用户成交WebSocket连接失败 err={}", err),
        }
        let _ = account
            .exchange
            .close_user_data_stream(&listen_key, config.account.market_type)
            .await;
        sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}
async fn run_summary_loop(
    config: ObiScalperConfig,
    summaries: Arc<DashMap<String, SymbolSummary>>,
    http: Client,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(
        config.execution.summary_interval_minutes * 60,
    ));
    loop {
        ticker.tick().await;
        if let Err(err) = send_summary(&config, &summaries, &http).await {
            log::warn!("[obi_scalper] 发送30分钟总结失败: {}", err);
        }
    }
}

fn validate_config(config: &ObiScalperConfig) -> Result<()> {
    if config.execution.order_notional <= 0.0 {
        return Err(anyhow!("order_notional必须大于0"));
    }
    if config.global_risk.max_open_symbols == 0 {
        return Err(anyhow!("max_open_symbols必须大于0"));
    }
    let mut seen = HashSet::new();
    for symbol in config.symbols.iter().filter(|s| s.enabled) {
        if !seen.insert(symbol.symbol.clone()) {
            return Err(anyhow!("重复交易对配置: {}", symbol.symbol));
        }
        if symbol.step_size <= 0.0 || symbol.tick_size <= 0.0 {
            return Err(anyhow!("{} tick_size/step_size必须大于0", symbol.symbol));
        }
    }
    Ok(())
}
async fn startup_safety_check(
    config: &ObiScalperConfig,
    account: &Arc<AccountInfo>,
) -> Result<FastHashMap<String, Vec<PositionState>>> {
    let mut startup_positions: FastHashMap<String, Vec<PositionState>> = FastHashMap::new();
    if config.strategy.dry_run {
        return Ok(startup_positions);
    }
    account.exchange.ping().await?;
    for symbol in config.symbols.iter().filter(|s| s.enabled) {
        let open_orders = account
            .exchange
            .get_open_orders(Some(&symbol.symbol), config.account.market_type)
            .await?;
        if !open_orders.is_empty() {
            return Err(anyhow!(
                "{} 存在{}个未清理挂单，拒绝启动OBI实盘",
                symbol.symbol,
                open_orders.len()
            ));
        }
        let positions = account.exchange.get_positions(Some(&symbol.symbol)).await?;
        for position in positions {
            if position.contracts.abs() <= symbol.min_qty * 0.1 {
                continue;
            }
            let side = position_side_from_exchange(&position)?;
            let state = PositionState {
                side,
                qty: position.contracts.abs(),
                entry_price: position.entry_price,
                opened_at: Instant::now(),
                opened_at_wall: position.timestamp,
                entry_fee: 0.0,
            };
            let entry = startup_positions.entry(symbol.symbol.clone()).or_default();
            if !config.execution.inventory_mode && !entry.is_empty() {
                return Err(anyhow!(
                    "{} 同时存在多方向仓位，拒绝启动OBI实盘",
                    symbol.symbol
                ));
            }
            entry.push(state);
        }
    }
    Ok(startup_positions)
}

fn position_side_from_exchange(position: &Position) -> Result<PositionSideKind> {
    let side = position.side.to_ascii_uppercase();
    if side.contains("LONG") || position.amount > 0.0 {
        Ok(PositionSideKind::Long)
    } else if side.contains("SHORT") || position.amount < 0.0 {
        Ok(PositionSideKind::Short)
    } else {
        Err(anyhow!(
            "无法识别仓位方向 symbol={} side={} amount={}",
            position.symbol,
            position.side,
            position.amount
        ))
    }
}

fn build_depth_stream_url(symbols: &[String], market_type: MarketType) -> String {
    let streams = symbols
        .iter()
        .map(|symbol| format!("{}@depth20@100ms", stream_symbol(symbol)))
        .collect::<Vec<_>>()
        .join("/");
    match market_type {
        MarketType::Spot => format!("wss://stream.binance.com:9443/stream?streams={streams}"),
        MarketType::Futures => format!("wss://fstream.binance.com/stream?streams={streams}"),
    }
}
fn redact_ws_url(url: &str) -> String {
    url.to_string()
}
fn stream_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_ascii_lowercase()
}
fn normalize_symbol(symbol: &str) -> String {
    let upper = symbol.to_ascii_uppercase();
    if upper.contains('/') {
        upper
    } else if let Some(base) = upper.strip_suffix("USDC") {
        format!("{base}/USDC")
    } else if let Some(base) = upper.strip_suffix("USDT") {
        format!("{base}/USDT")
    } else {
        upper
    }
}
fn parse_depth_message(message: &str) -> Result<Option<(String, FixedOrderBook<20>)>> {
    let json: Value = serde_json::from_str(message)?;
    let data = json.get("data").unwrap_or(&json);
    if data.get("b").is_none() || data.get("a").is_none() {
        return Ok(None);
    }
    let symbol = data
        .get("s")
        .and_then(|v| v.as_str())
        .or_else(|| {
            json.get("stream")
                .and_then(|v| v.as_str())
                .and_then(|stream| stream.split('@').next())
        })
        .ok_or_else(|| anyhow!("depth message missing symbol"))?;
    let mut bids = Vec::with_capacity(20);
    let mut asks = Vec::with_capacity(20);
    if let Some(levels) = data.get("b").and_then(|v| v.as_array()) {
        for level in levels.iter().take(20) {
            if let Some(pair) = level.as_array() {
                if pair.len() >= 2 {
                    let price = pair[0]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let qty = pair[1]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    if price > 0.0 && qty > 0.0 {
                        bids.push([price, qty]);
                    }
                }
            }
        }
    }
    if let Some(levels) = data.get("a").and_then(|v| v.as_array()) {
        for level in levels.iter().take(20) {
            if let Some(pair) = level.as_array() {
                if pair.len() >= 2 {
                    let price = pair[0]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    let qty = pair[1]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(0.0);
                    if price > 0.0 && qty > 0.0 {
                        asks.push([price, qty]);
                    }
                }
            }
        }
    }
    let event_time = data
        .get("E")
        .and_then(|value| value.as_i64())
        .unwrap_or_else(|| Utc::now().timestamp_millis());
    let orderbook = OrderBook {
        symbol: normalize_symbol(symbol),
        bids,
        asks,
        timestamp: DateTime::from_timestamp_millis(event_time).unwrap_or_else(Utc::now),
        info: serde_json::json!({"lastUpdateId": data.get("u").and_then(|v| v.as_u64()).unwrap_or_default()}),
    };
    let symbol = orderbook.symbol.clone();
    Ok(Some((
        symbol.clone(),
        FixedOrderBook::from_snapshot(&symbol, &orderbook)?,
    )))
}
fn parse_order_update_message(message: &str) -> Result<Option<OrderUpdateEvent>> {
    let json: Value = serde_json::from_str(message)?;
    let event_type = json.get("e").and_then(|v| v.as_str()).unwrap_or("");
    if event_type == "ORDER_TRADE_UPDATE" {
        let data = json
            .get("o")
            .ok_or_else(|| anyhow!("ORDER_TRADE_UPDATE missing o"))?;
        return Ok(Some(OrderUpdateEvent {
            symbol: normalize_symbol(data.get("s").and_then(|v| v.as_str()).unwrap_or_default()),
            order_id: data.get("i").map(value_to_string).unwrap_or_default(),
            client_order_id: data
                .get("c")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            side: parse_order_side(data.get("S").and_then(|v| v.as_str()).unwrap_or("BUY")),
            status: parse_order_status(data.get("X").and_then(|v| v.as_str()).unwrap_or("NEW")),
            last_filled_qty: parse_f64(data.get("l")),
            cumulative_filled_qty: parse_f64(data.get("z")),
            avg_price: parse_f64(data.get("ap")),
            last_price: parse_f64(data.get("L")),
            commission: parse_f64(data.get("n")),
            is_maker: data.get("m").and_then(|v| v.as_bool()).unwrap_or(false),
            event_time: DateTime::from_timestamp_millis(
                json.get("E")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_else(|| Utc::now().timestamp_millis()),
            )
            .unwrap_or_else(Utc::now),
        }));
    }
    if event_type == "executionReport" {
        return Ok(Some(OrderUpdateEvent {
            symbol: normalize_symbol(json.get("s").and_then(|v| v.as_str()).unwrap_or_default()),
            order_id: json.get("i").map(value_to_string).unwrap_or_default(),
            client_order_id: json
                .get("c")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            side: parse_order_side(json.get("S").and_then(|v| v.as_str()).unwrap_or("BUY")),
            status: parse_order_status(json.get("X").and_then(|v| v.as_str()).unwrap_or("NEW")),
            last_filled_qty: parse_f64(json.get("l")),
            cumulative_filled_qty: parse_f64(json.get("z")),
            avg_price: avg_price_from_quote_qty(parse_f64(json.get("Z")), parse_f64(json.get("z"))),
            last_price: parse_f64(json.get("L")),
            commission: parse_f64(json.get("n")),
            is_maker: json.get("m").and_then(|v| v.as_bool()).unwrap_or(false),
            event_time: DateTime::from_timestamp_millis(
                json.get("E")
                    .and_then(|v| v.as_i64())
                    .unwrap_or_else(|| Utc::now().timestamp_millis()),
            )
            .unwrap_or_else(Utc::now),
        }));
    }
    Ok(None)
}
fn value_to_string(value: &Value) -> String {
    value
        .as_str()
        .map(ToString::to_string)
        .unwrap_or_else(|| value.to_string().trim_matches('"').to_string())
}
fn parse_f64(value: Option<&Value>) -> f64 {
    value
        .and_then(|v| {
            v.as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| v.as_f64())
        })
        .unwrap_or(0.0)
}
fn parse_order_side(value: &str) -> OrderSide {
    if value == "SELL" {
        OrderSide::Sell
    } else {
        OrderSide::Buy
    }
}
fn parse_order_status(value: &str) -> OrderStatus {
    match value {
        "FILLED" => OrderStatus::Closed,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "CANCELED" => OrderStatus::Canceled,
        "EXPIRED" => OrderStatus::Expired,
        "REJECTED" => OrderStatus::Rejected,
        "NEW" => OrderStatus::Open,
        _ => OrderStatus::Pending,
    }
}

fn avg_price_from_quote_qty(quote_qty: f64, base_qty: f64) -> f64 {
    if base_qty.abs() <= f64::EPSILON {
        0.0
    } else {
        quote_qty / base_qty
    }
}
fn short_reason_code(reason: &str) -> &'static str {
    match reason {
        "obi_long" => "ol",
        "obi_short" => "os",
        "signal_exit" => "se",
        "stop_loss" => "sl",
        "timeout" => "to",
        "order_fill" => "of",
        _ => "x",
    }
}

fn order_update_from_order(
    order: &Order,
    pending: &PendingOrderState,
    event_time: DateTime<Utc>,
) -> OrderUpdateEvent {
    let avg_price = order
        .info
        .get("avgPrice")
        .and_then(|value| {
            value
                .as_str()
                .and_then(|s| s.parse::<f64>().ok())
                .or_else(|| value.as_f64())
        })
        .filter(|price| *price > 0.0)
        .or(order.price)
        .unwrap_or(pending.price);
    OrderUpdateEvent {
        symbol: order.symbol.clone(),
        order_id: order.id.clone(),
        client_order_id: pending.client_order_id.clone(),
        side: order.side,
        status: if order.filled >= pending.qty || order.status == OrderStatus::Closed {
            OrderStatus::Closed
        } else if order.filled > 0.0 {
            OrderStatus::PartiallyFilled
        } else {
            order.status.clone()
        },
        last_filled_qty: order.filled,
        cumulative_filled_qty: order.filled,
        avg_price,
        last_price: avg_price,
        commission: 0.0,
        is_maker: !pending.taker,
        event_time,
    }
}

fn effective_fill_price(update: &OrderUpdateEvent, fallback: f64) -> f64 {
    if update.avg_price > 0.0 {
        update.avg_price
    } else if update.last_price > 0.0 {
        update.last_price
    } else {
        fallback
    }
}
fn effective_fee(
    update: &OrderUpdateEvent,
    notional: f64,
    taker: bool,
    config: &ObiScalperConfig,
) -> f64 {
    if update.commission > 0.0 {
        update.commission
    } else {
        fee_for(notional, taker || !update.is_maker, config)
    }
}
#[inline]
fn quantize_qty(qty: f64, step: f64) -> f64 {
    normalize_to_step(qty, step)
}
#[inline]
fn normalize_to_step(value: f64, step: f64) -> f64 {
    if step <= 0.0 {
        return value;
    }
    let precision = step_precision(step);
    let factor = 10f64.powi(precision as i32);
    (((value / step).floor() * step) * factor).round() / factor
}
#[inline]
fn step_precision(step: f64) -> u32 {
    let mut precision = 0;
    let mut scaled = step.abs();
    while precision < 12 && (scaled - scaled.round()).abs() > 1e-9 {
        scaled *= 10.0;
        precision += 1;
    }
    precision
}
#[inline]
fn fee_for(notional: f64, taker: bool, config: &ObiScalperConfig) -> f64 {
    notional
        * if taker {
            config.execution.taker_fee_rate
        } else {
            config.execution.maker_fee_rate
        }
}
#[inline]
fn entry_price(
    book: &FixedOrderBook<20>,
    side: PositionSideKind,
    tick: f64,
    taker: bool,
) -> Option<f64> {
    match (side, taker) {
        (PositionSideKind::Long, false) => Some(book.top_bid()?.price),
        (PositionSideKind::Short, false) => Some(book.top_ask()?.price),
        (PositionSideKind::Long, true) => Some(book.top_ask()?.price + tick),
        (PositionSideKind::Short, true) => Some((book.top_bid()?.price - tick).max(tick)),
    }
}
#[inline]
fn exit_price(
    book: &FixedOrderBook<20>,
    side: PositionSideKind,
    tick: f64,
    taker: bool,
) -> Option<f64> {
    match (side, taker) {
        (PositionSideKind::Long, false) => Some(book.top_ask()?.price),
        (PositionSideKind::Short, false) => Some(book.top_bid()?.price),
        (PositionSideKind::Long, true) => Some((book.top_bid()?.price - tick).max(tick)),
        (PositionSideKind::Short, true) => Some(book.top_ask()?.price + tick),
    }
}
async fn send_summary(
    config: &ObiScalperConfig,
    summaries: &Arc<DashMap<String, SymbolSummary>>,
    http: &Client,
) -> Result<()> {
    let mut body = format!(
        "## OBI剥头皮30分钟总结\n> 时间: {}\n",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    );
    for mut item in summaries.iter_mut() {
        let state = item.value_mut();
        body.push_str(&format!("\n**{}**\n- 状态: {}\n- 仓位: {}\n- 挂单: {}\n- 30m交易: {} 胜:{} 负:{}\n- 30m手续费: {:.6}U\n- 30m PNL: {:.6}U\n- Day PNL: {:.6}U\n- 提交错误/拒单: {}/{}\n", state.symbol, state.phase, state.position, state.pending, state.trades_30m, state.wins_30m, state.losses_30m, state.fees_30m, state.realized_pnl_30m, state.realized_pnl_day, state.order_submit_errors, state.order_rejects));
        state.realized_pnl_30m = 0.0;
        state.fees_30m = 0.0;
        state.trades_30m = 0;
        state.wins_30m = 0;
        state.losses_30m = 0;
    }
    if config.notifications.enabled && !config.notifications.wechat_webhook_url.is_empty() {
        let payload = serde_json::json!({"msgtype":"markdown","markdown":{"content":body}});
        let resp = http
            .post(&config.notifications.wechat_webhook_url)
            .json(&payload)
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(anyhow!("wechat status={}", resp.status()));
        }
    }
    Ok(())
}
fn default_log_level() -> String {
    "INFO".to_string()
}
fn default_exchange() -> String {
    "binance".to_string()
}
fn default_market_type() -> MarketType {
    MarketType::Futures
}
fn default_true() -> bool {
    true
}
fn default_max_inventory_notional() -> f64 {
    300.0
}
fn default_soft_inventory_notional() -> f64 {
    100.0
}
fn default_hard_inventory_notional() -> f64 {
    200.0
}
fn default_take_profit_bps() -> f64 {
    4.0
}
fn default_min_take_profit_bps() -> f64 {
    2.0
}
fn default_max_book_latency_ms() -> u64 {
    1000
}
fn default_post_cancel_cooldown_ms() -> u64 {
    1000
}
fn default_min_entry_interval_ms() -> u64 {
    500
}
fn default_dry_run() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parse_combined_depth_should_build_fixed_book() {
        let raw = r#"{"stream":"dogeusdc@depth20@100ms","data":{"e":"depthUpdate","E":1710000000000,"s":"DOGEUSDC","u":123,"b":[["0.15000","100"],["0.14999","200"]],"a":[["0.15001","120"],["0.15002","220"]]}}"#;
        let (symbol, book) = parse_depth_message(raw).unwrap().unwrap();
        assert_eq!(symbol, "DOGE/USDC");
        assert!(book.initialized);
        assert_eq!(book.last_update_id, 123);
        assert_eq!(book.top_bid().unwrap().price, 0.15000);
        assert_eq!(book.top_ask().unwrap().price, 0.15001);
    }
    #[test]
    fn parse_futures_order_update_should_normalize_symbol() {
        let raw = r#"{"e":"ORDER_TRADE_UPDATE","E":1710000000100,"o":{"s":"DOGEUSDC","c":"cid","S":"BUY","X":"FILLED","i":12345,"l":"10","z":"10","L":"0.15","ap":"0.15","n":"0.0006","m":false}}"#;
        let update = parse_order_update_message(raw).unwrap().unwrap();
        assert_eq!(update.symbol, "DOGE/USDC");
        assert_eq!(update.status, OrderStatus::Closed);
        assert_eq!(update.order_id, "12345");
        assert_eq!(update.cumulative_filled_qty, 10.0);
    }
}
