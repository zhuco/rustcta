use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use log::{debug, error, info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};

use crate::core::exchange::Exchange;
use crate::core::types::{
    Interval, Kline, MarketType, OpenInterest, OrderRequest, OrderSide, OrderType, Trade,
};
use crate::cta::account_manager::{AccountInfo, AccountManager};
use crate::strategies::common::{
    Strategy, StrategyDeps, StrategyInstance, StrategyPosition, StrategyState, StrategyStatus,
    UnifiedRiskEvaluator,
};

fn default_true() -> bool {
    true
}

fn default_market_type() -> MarketType {
    MarketType::Futures
}

fn default_scan_interval_secs() -> u64 {
    60
}

fn default_lookback_5m() -> usize {
    160
}

fn default_lookback_15m() -> usize {
    120
}

fn default_trade_limit() -> u32 {
    1000
}

fn default_entry_score_threshold() -> f64 {
    0.68
}

fn default_volume_lookback_bars() -> usize {
    20
}

fn default_delta_lookback_bars() -> usize {
    6
}

fn default_volume_spike_ratio() -> f64 {
    1.8
}

fn default_small_price_change_pct() -> f64 {
    0.35
}

fn default_delta_ratio_threshold() -> f64 {
    0.12
}

fn default_oi_change_threshold() -> f64 {
    0.002
}

fn default_trend_ema_period() -> usize {
    50
}

fn default_fixed_notional() -> f64 {
    50.0
}

fn default_max_active_trades_per_symbol() -> usize {
    2
}

fn default_min_entry_interval_minutes() -> i64 {
    10
}

fn default_atr_period() -> usize {
    14
}

fn default_atr_stop_multiplier() -> f64 {
    1.4
}

fn default_tp1_atr_multiple() -> f64 {
    1.0
}

fn default_tp2_atr_multiple() -> f64 {
    2.0
}

fn default_tp1_fraction() -> f64 {
    0.5
}

fn default_time_stop_bars() -> usize {
    15
}

fn default_conservative_intrabar() -> bool {
    true
}

fn default_review_bars() -> usize {
    15
}

fn default_report_title() -> String {
    "吸筹策略15根K线回顾".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccumulationConfig {
    pub strategy: StrategyInfo,
    pub account: AccountConfig,
    pub data: DataConfig,
    pub symbols: Vec<SymbolConfig>,
    pub signal: SignalConfig,
    pub trend_filter: TrendFilterConfig,
    pub risk: RiskConfig,
    pub exits: ExitConfig,
    pub review: ReviewConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyInfo {
    pub name: String,
    pub version: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub description: Option<String>,
}

fn default_log_level() -> String {
    "INFO".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub account_id: String,
    #[serde(default = "default_market_type")]
    pub market_type: MarketType,
    #[serde(default = "default_true")]
    pub dual_position_mode: bool,
    #[serde(default = "default_true")]
    pub allow_long: bool,
    #[serde(default = "default_true")]
    pub allow_short: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataConfig {
    #[serde(default = "default_scan_interval_secs")]
    pub scan_interval_secs: u64,
    #[serde(default = "default_lookback_5m")]
    pub lookback_5m: usize,
    #[serde(default = "default_lookback_15m")]
    pub lookback_15m: usize,
    #[serde(default = "default_trade_limit")]
    pub trade_limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolConfig {
    pub symbol: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalConfig {
    #[serde(default = "default_volume_lookback_bars")]
    pub volume_lookback_bars: usize,
    #[serde(default = "default_delta_lookback_bars")]
    pub delta_lookback_bars: usize,
    #[serde(default = "default_volume_spike_ratio")]
    pub volume_spike_ratio: f64,
    #[serde(default = "default_small_price_change_pct")]
    pub small_price_change_pct: f64,
    #[serde(default = "default_delta_ratio_threshold")]
    pub delta_ratio_threshold: f64,
    #[serde(default = "default_oi_change_threshold")]
    pub oi_change_threshold: f64,
    #[serde(default = "default_entry_score_threshold")]
    pub entry_score_threshold: f64,
    pub weights: SignalWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalWeights {
    pub volume_change: f64,
    pub price_change: f64,
    pub cvd_delta: f64,
    pub open_interest: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendFilterConfig {
    #[serde(default = "default_trend_ema_period")]
    pub ema_period: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    #[serde(default = "default_fixed_notional")]
    pub fixed_notional_usdt: f64,
    #[serde(default = "default_max_active_trades_per_symbol")]
    pub max_active_trades_per_symbol: usize,
    #[serde(default = "default_min_entry_interval_minutes")]
    pub min_entry_interval_minutes: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExitConfig {
    #[serde(default = "default_atr_period")]
    pub atr_period: usize,
    #[serde(default = "default_atr_stop_multiplier")]
    pub atr_stop_multiplier: f64,
    #[serde(default = "default_tp1_atr_multiple")]
    pub tp1_atr_multiple: f64,
    #[serde(default = "default_tp2_atr_multiple")]
    pub tp2_atr_multiple: f64,
    #[serde(default = "default_tp1_fraction")]
    pub tp1_fraction: f64,
    #[serde(default = "default_time_stop_bars")]
    pub time_stop_bars: usize,
    #[serde(default = "default_conservative_intrabar")]
    pub conservative_intrabar: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_review_bars")]
    pub review_bars: usize,
    #[serde(default = "default_report_title")]
    pub title: String,
    #[serde(default)]
    pub webhook_url: Option<String>,
}

#[derive(Debug, Clone)]
struct SymbolPrecision {
    tick_size: f64,
    step_size: f64,
    min_qty: f64,
    min_notional: f64,
    price_precision: u32,
    qty_precision: u32,
}

#[derive(Debug, Clone)]
struct BarDeltaSample {
    close_time: DateTime<Utc>,
    delta_quote: f64,
    total_quote: f64,
}

#[derive(Debug, Clone)]
struct SymbolRuntimeState {
    last_processed_bar: Option<DateTime<Utc>>,
    last_open_interest: Option<f64>,
    delta_history: VecDeque<BarDeltaSample>,
    entry_times: VecDeque<DateTime<Utc>>,
    precision: Option<SymbolPrecision>,
}

impl Default for SymbolRuntimeState {
    fn default() -> Self {
        Self {
            last_processed_bar: None,
            last_open_interest: None,
            delta_history: VecDeque::new(),
            entry_times: VecDeque::new(),
            precision: None,
        }
    }
}

#[derive(Debug, Clone)]
struct SignalBreakdown {
    total_score: f64,
    volume_score: f64,
    price_score: f64,
    flow_score: f64,
    oi_score: f64,
    volume_ratio: f64,
    price_change_pct: f64,
    delta_ratio: f64,
    cvd_ratio: f64,
    oi_change_pct: f64,
    trend_ema: f64,
    trend_close: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExitReason {
    TakeProfit1,
    TakeProfit2,
    StopLoss,
    TimeStop,
}

impl ExitReason {
    fn label(&self) -> &'static str {
        match self {
            Self::TakeProfit1 => "止盈一段",
            Self::TakeProfit2 => "止盈二段",
            Self::StopLoss => "ATR动态止损",
            Self::TimeStop => "时间止损",
        }
    }
}

#[derive(Debug, Clone)]
struct RestingTakeProfitOrder {
    order_id: String,
    quantity: f64,
    price: f64,
    filled_quantity: f64,
}

#[derive(Debug, Clone)]
struct ManagedTrade {
    id: String,
    symbol: String,
    side: OrderSide,
    entry_time: DateTime<Utc>,
    entry_bar_close: DateTime<Utc>,
    entry_price: f64,
    quantity: f64,
    remaining_quantity: f64,
    tp1_target_quantity: f64,
    tp2_target_quantity: f64,
    notional: f64,
    atr_at_entry: f64,
    tp1_price: f64,
    tp2_price: f64,
    tp1_order: Option<RestingTakeProfitOrder>,
    tp2_order: Option<RestingTakeProfitOrder>,
    trailing_stop_price: f64,
    signal: SignalBreakdown,
    realized_pnl: f64,
    bars_observed: usize,
    latest_close: f64,
    highest_price: f64,
    lowest_price: f64,
    max_favorable_move: f64,
    max_adverse_move: f64,
    tp1_hit: bool,
    tp2_hit: bool,
    stop_loss_hit: bool,
    time_stop_hit: bool,
    active: bool,
    exit_reason: Option<ExitReason>,
    last_processed_bar: Option<DateTime<Utc>>,
    review_sent: bool,
}

#[derive(Debug, Default)]
struct RuntimeState {
    symbols: HashMap<String, SymbolRuntimeState>,
    trades: HashMap<String, ManagedTrade>,
    sequence: u64,
}

#[derive(Debug, Clone)]
struct EntryCandidate {
    symbol: String,
    side: OrderSide,
    bar: Kline,
    atr: f64,
    trend_ema: f64,
    score: SignalBreakdown,
    precision: SymbolPrecision,
}

#[derive(Debug, Clone)]
struct ExitAction {
    trade_id: String,
    reason: ExitReason,
    quantity: f64,
    reference_price: f64,
}

#[derive(Debug, Clone, Copy)]
enum TakeProfitStage {
    First,
    Second,
}

impl TakeProfitStage {
    fn label(&self) -> &'static str {
        match self {
            Self::First => "tp1",
            Self::Second => "tp2",
        }
    }

    fn exit_reason(&self) -> ExitReason {
        match self {
            Self::First => ExitReason::TakeProfit1,
            Self::Second => ExitReason::TakeProfit2,
        }
    }
}

#[derive(Clone)]
pub struct AccumulationStrategy {
    config: AccumulationConfig,
    account_manager: Arc<AccountManager>,
    account: Arc<AccountInfo>,
    exchange: Arc<Box<dyn Exchange>>,
    market_type: MarketType,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    running: Arc<RwLock<bool>>,
    status: Arc<RwLock<StrategyStatus>>,
    started_at: Arc<RwLock<Option<Instant>>>,
    runtime: Arc<RwLock<RuntimeState>>,
    task_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    http_client: Client,
}

impl AccumulationStrategy {
    fn new(config: AccumulationConfig, deps: StrategyDeps) -> Result<Self> {
        let account_manager = deps.account_manager.clone();
        let account = account_manager
            .get_account(&config.account.account_id)
            .ok_or_else(|| anyhow!("account {} not found", config.account.account_id))?;

        let status = StrategyStatus::new(config.strategy.name.clone())
            .with_state(StrategyState::Initializing);

        Ok(Self {
            account_manager,
            account: account.clone(),
            exchange: account.exchange.clone(),
            market_type: config.account.market_type,
            risk_evaluator: deps.risk_evaluator.clone(),
            running: Arc::new(RwLock::new(false)),
            status: Arc::new(RwLock::new(status)),
            started_at: Arc::new(RwLock::new(None)),
            runtime: Arc::new(RwLock::new(RuntimeState::default())),
            task_handles: Arc::new(Mutex::new(Vec::new())),
            http_client: Client::builder()
                .timeout(StdDuration::from_secs(10))
                .build()
                .context("failed to build accumulation http client")?,
            config,
        })
    }

    async fn spawn_tasks(&self) -> Result<()> {
        let strategy = self.clone();
        let handle = tokio::spawn(async move {
            strategy.run_loop().await;
        });
        self.task_handles.lock().await.push(handle);
        Ok(())
    }

    async fn run_loop(self) {
        let interval = Duration::from_secs(self.config.data.scan_interval_secs.max(5));
        loop {
            if !self.is_running().await {
                break;
            }

            if let Err(err) = self.run_cycle().await {
                error!("吸筹策略运行异常: {err:#}");
                let mut status = self.status.write().await;
                status.state = StrategyState::Error;
                status.updated_at = Utc::now();
                status.last_error = Some(err.to_string());
            }

            sleep(interval).await;
        }
    }

    async fn run_cycle(&self) -> Result<()> {
        let symbols: Vec<SymbolConfig> = self
            .config
            .symbols
            .iter()
            .filter(|cfg| cfg.enabled)
            .cloned()
            .collect();

        for symbol_cfg in symbols {
            if let Err(err) = self.process_symbol(&symbol_cfg).await {
                warn!("处理 {} 失败: {}", symbol_cfg.symbol, err);
            }
        }

        self.refresh_status().await;
        Ok(())
    }

    async fn process_symbol(&self, symbol_cfg: &SymbolConfig) -> Result<()> {
        let symbol = symbol_cfg.symbol.as_str();
        let klines_5m = self
            .exchange
            .get_klines(
                symbol,
                Interval::FiveMinutes,
                self.market_type,
                Some(self.config.data.lookback_5m as u32),
            )
            .await
            .with_context(|| format!("{} 获取5m K线失败", symbol))?;

        let klines_15m = self
            .exchange
            .get_klines(
                symbol,
                Interval::FifteenMinutes,
                self.market_type,
                Some(self.config.data.lookback_15m as u32),
            )
            .await
            .with_context(|| format!("{} 获取15m K线失败", symbol))?;

        let closed_5m = closed_klines(&klines_5m);
        let closed_15m = closed_klines(&klines_15m);
        if closed_5m.len() < self.minimum_5m_bars() || closed_15m.len() < self.minimum_15m_bars() {
            debug!("{} K线不足，跳过", symbol);
            return Ok(());
        }

        let last_bar = closed_5m
            .last()
            .cloned()
            .ok_or_else(|| anyhow!("{} 缺少最新5m K线", symbol))?;

        let already_processed = {
            let rt = self.runtime.read().await;
            rt.symbols
                .get(symbol)
                .and_then(|state| state.last_processed_bar)
                == Some(last_bar.close_time)
        };
        if already_processed {
            return Ok(());
        }

        let precision = self.get_or_fetch_precision(symbol).await?;
        let latest_atr = compute_atr(&closed_5m, self.config.exits.atr_period)
            .ok_or_else(|| anyhow!("{} 无法计算 ATR{}", symbol, self.config.exits.atr_period))?;

        let current_oi = match self.exchange.get_open_interest(symbol).await {
            Ok(oi) => Some(oi),
            Err(err) => {
                warn!("{} 获取 OI 失败，当前轮次不参与入场评分: {}", symbol, err);
                None
            }
        };

        let bar_delta = self.compute_bar_delta(symbol, &last_bar).await?;
        let oi_change_pct = self
            .update_symbol_state_on_new_bar(
                symbol,
                last_bar.close_time,
                current_oi.clone(),
                bar_delta,
            )
            .await;

        self.process_trade_updates(symbol, &last_bar, latest_atr, &precision)
            .await?;

        let trend_ema = compute_ema(
            &closed_15m.iter().map(|bar| bar.close).collect::<Vec<_>>(),
            self.config.trend_filter.ema_period,
        )
        .ok_or_else(|| {
            anyhow!(
                "{} 无法计算15m EMA{}",
                symbol,
                self.config.trend_filter.ema_period
            )
        })?;
        let trend_close = closed_15m
            .last()
            .map(|bar| bar.close)
            .ok_or_else(|| anyhow!("{} 缺少15m收盘价", symbol))?;

        if let Some(oi_change_pct) = oi_change_pct {
            if let Some(candidate) = self
                .evaluate_entry_candidate(
                    symbol,
                    &closed_5m,
                    &last_bar,
                    oi_change_pct,
                    trend_ema,
                    trend_close,
                    precision.clone(),
                )
                .await?
            {
                self.try_open_trade(candidate).await?;
            }
        }

        Ok(())
    }

    fn minimum_5m_bars(&self) -> usize {
        let vol_need = self.config.signal.volume_lookback_bars + 2;
        let atr_need = self.config.exits.atr_period + 2;
        let review_need = self.config.review.review_bars + 2;
        vol_need.max(atr_need).max(review_need).max(40)
    }

    fn minimum_15m_bars(&self) -> usize {
        self.config.trend_filter.ema_period + 5
    }

    async fn get_or_fetch_precision(&self, symbol: &str) -> Result<SymbolPrecision> {
        if let Some(existing) = {
            let rt = self.runtime.read().await;
            rt.symbols
                .get(symbol)
                .and_then(|state| state.precision.clone())
        } {
            return Ok(existing);
        }

        let info = self
            .exchange
            .get_symbol_info(symbol, self.market_type)
            .await
            .with_context(|| format!("{} 获取交易精度失败", symbol))?;

        let precision = SymbolPrecision {
            tick_size: info.tick_size.max(1e-12),
            step_size: info.step_size.max(1e-12),
            min_qty: info.step_size.max(1e-12),
            min_notional: info.min_notional.unwrap_or(5.0).max(0.0),
            price_precision: precision_from_step(info.tick_size),
            qty_precision: precision_from_step(info.step_size),
        };

        {
            let mut rt = self.runtime.write().await;
            let state = rt.symbols.entry(symbol.to_string()).or_default();
            state.precision = Some(precision.clone());
        }

        info!(
            "{} 精度初始化: tick_size={} step_size={} min_notional={}",
            symbol, precision.tick_size, precision.step_size, precision.min_notional
        );
        Ok(precision)
    }

    async fn compute_bar_delta(&self, symbol: &str, bar: &Kline) -> Result<BarDeltaSample> {
        let trades = self
            .exchange
            .get_trades(symbol, self.market_type, Some(self.config.data.trade_limit))
            .await
            .with_context(|| format!("{} 获取逐笔成交失败", symbol))?;

        Ok(compute_delta_from_trades(bar, &trades))
    }

    async fn update_symbol_state_on_new_bar(
        &self,
        symbol: &str,
        close_time: DateTime<Utc>,
        current_oi: Option<OpenInterest>,
        delta: BarDeltaSample,
    ) -> Option<f64> {
        let mut rt = self.runtime.write().await;
        let state = rt.symbols.entry(symbol.to_string()).or_default();
        state.last_processed_bar = Some(close_time);

        state.delta_history.push_back(delta);
        while state.delta_history.len() > self.config.signal.delta_lookback_bars.max(3) + 4 {
            state.delta_history.pop_front();
        }

        let oi_change_pct = current_oi.and_then(|oi| {
            let change = state.last_open_interest.map(|prev| {
                if prev.abs() <= f64::EPSILON {
                    0.0
                } else {
                    (oi.open_interest - prev) / prev
                }
            });
            state.last_open_interest = Some(oi.open_interest);
            change
        });

        oi_change_pct
    }

    async fn process_trade_updates(
        &self,
        symbol: &str,
        bar: &Kline,
        latest_atr: f64,
        precision: &SymbolPrecision,
    ) -> Result<()> {
        let mut planned_actions = Vec::new();
        let review_due_ids = {
            let mut rt = self.runtime.write().await;
            let mut due = Vec::new();
            for trade in rt
                .trades
                .values_mut()
                .filter(|trade| trade.symbol == symbol)
            {
                if trade.last_processed_bar == Some(bar.close_time) {
                    continue;
                }

                trade.last_processed_bar = Some(bar.close_time);
                trade.bars_observed += 1;
                trade.latest_close = bar.close;
                trade.highest_price = trade.highest_price.max(bar.high);
                trade.lowest_price = trade.lowest_price.min(bar.low);

                let favorable_move = match trade.side {
                    OrderSide::Buy => bar.high - trade.entry_price,
                    OrderSide::Sell => trade.entry_price - bar.low,
                };
                let adverse_move = match trade.side {
                    OrderSide::Buy => trade.entry_price - bar.low,
                    OrderSide::Sell => bar.high - trade.entry_price,
                };
                trade.max_favorable_move = trade.max_favorable_move.max(favorable_move.max(0.0));
                trade.max_adverse_move = trade.max_adverse_move.max(adverse_move.max(0.0));

                if trade.active {
                    trade.trailing_stop_price = match trade.side {
                        OrderSide::Buy => trade
                            .trailing_stop_price
                            .max(bar.close - latest_atr * self.config.exits.atr_stop_multiplier),
                        OrderSide::Sell => trade
                            .trailing_stop_price
                            .min(bar.close + latest_atr * self.config.exits.atr_stop_multiplier),
                    };

                    planned_actions.extend(plan_exit_actions(
                        trade,
                        bar,
                        latest_atr,
                        &self.config.exits,
                        precision,
                    ));
                }

                if self.config.review.enabled
                    && !trade.review_sent
                    && trade.bars_observed >= self.config.review.review_bars
                {
                    due.push(trade.id.clone());
                }
            }
            due
        };

        for action in planned_actions {
            if let Err(err) = self.execute_exit_action(&action, precision).await {
                warn!(
                    "{} 执行 {} 失败: {}",
                    action.trade_id,
                    action.reason.label(),
                    err
                );
            }
        }

        for trade_id in review_due_ids {
            if let Err(err) = self.send_review_if_due(&trade_id).await {
                warn!("{} 发送回顾失败: {}", trade_id, err);
            }
        }

        Ok(())
    }

    async fn execute_exit_action(
        &self,
        action: &ExitAction,
        precision: &SymbolPrecision,
    ) -> Result<()> {
        let (symbol, side, remaining_qty) = {
            let rt = self.runtime.read().await;
            let trade = rt
                .trades
                .get(&action.trade_id)
                .ok_or_else(|| anyhow!("trade {} not found", action.trade_id))?;
            if !trade.active || trade.remaining_quantity <= precision.min_qty * 0.5 {
                return Ok(());
            }
            (trade.symbol.clone(), trade.side, trade.remaining_quantity)
        };

        let qty = normalize_exit_quantity(action.quantity.min(remaining_qty), precision);
        if qty <= 0.0 {
            return Ok(());
        }

        let exit_side = match side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };

        let mut params = HashMap::new();
        if self.config.account.dual_position_mode && matches!(self.market_type, MarketType::Futures)
        {
            params.insert(
                "positionSide".to_string(),
                position_side_label(side).to_string(),
            );
        }

        let mut request = OrderRequest::new(
            symbol.clone(),
            exit_side,
            OrderType::Market,
            qty,
            None,
            self.market_type,
        );
        request.client_order_id = Some(format!(
            "acc-exit-{}-{}",
            action.reason.label().replace(' ', ""),
            Utc::now().timestamp_micros()
        ));
        request.reduce_only = if self.config.account.dual_position_mode {
            None
        } else {
            Some(true)
        };
        request.params = Some(params);

        let order = self
            .account
            .exchange
            .create_order(request)
            .await
            .with_context(|| format!("{} 提交平仓单失败", symbol))?;

        let fill_price = order.price.unwrap_or(action.reference_price);
        let closed_qty = if order.filled > 0.0 {
            order.filled
        } else {
            qty
        };

        {
            let mut rt = self.runtime.write().await;
            if let Some(trade) = rt.trades.get_mut(&action.trade_id) {
                let pnl = match trade.side {
                    OrderSide::Buy => (fill_price - trade.entry_price) * closed_qty,
                    OrderSide::Sell => (trade.entry_price - fill_price) * closed_qty,
                };
                trade.realized_pnl += pnl;
                trade.remaining_quantity = (trade.remaining_quantity - closed_qty).max(0.0);
                if trade.remaining_quantity <= precision.min_qty * 0.5 {
                    trade.remaining_quantity = 0.0;
                    trade.active = false;
                    trade.exit_reason = Some(action.reason);
                }

                match action.reason {
                    ExitReason::TakeProfit1 => trade.tp1_hit = true,
                    ExitReason::TakeProfit2 => {
                        trade.tp1_hit = true;
                        trade.tp2_hit = true;
                    }
                    ExitReason::StopLoss => trade.stop_loss_hit = true,
                    ExitReason::TimeStop => trade.time_stop_hit = true,
                }
            }
        }

        info!(
            "{} {} 执行成功 qty={:.6} ref={:.4} fill={:.4}",
            symbol,
            action.reason.label(),
            closed_qty,
            action.reference_price,
            fill_price
        );

        Ok(())
    }

    async fn evaluate_entry_candidate(
        &self,
        symbol: &str,
        closed_5m: &[Kline],
        last_bar: &Kline,
        oi_change_pct: f64,
        trend_ema: f64,
        trend_close: f64,
        precision: SymbolPrecision,
    ) -> Result<Option<EntryCandidate>> {
        let active_count = self.active_trade_count(symbol).await;
        if active_count >= self.config.risk.max_active_trades_per_symbol {
            debug!("{} 活跃仓位已达上限，跳过新开仓", symbol);
            return Ok(None);
        }

        if self.in_entry_cooldown(symbol).await {
            debug!("{} 仍在开仓冷却期内，跳过", symbol);
            return Ok(None);
        }

        let prev_bar = closed_5m
            .iter()
            .rev()
            .nth(1)
            .ok_or_else(|| anyhow!("{} 缺少前一根5m K线", symbol))?;

        let volume_ratio = compute_volume_ratio(
            closed_5m,
            self.config.signal.volume_lookback_bars,
            last_bar.volume,
        );
        let price_change_pct = percentage_change(last_bar.close, prev_bar.close);
        let (delta_ratio, cvd_ratio) = self.flow_ratios(symbol).await;

        let small_move_limit = self.config.signal.small_price_change_pct.max(0.01);
        let volume_score = normalize_threshold(
            volume_ratio,
            self.config.signal.volume_spike_ratio,
            self.config.signal.volume_spike_ratio * 2.5,
        );
        let small_move_score = (1.0 - (price_change_pct.abs() / small_move_limit)).clamp(0.0, 1.0);
        let long_price_bias = (-price_change_pct / small_move_limit).clamp(0.0, 1.0);
        let short_price_bias = (price_change_pct / small_move_limit).clamp(0.0, 1.0);
        let long_price_score = (small_move_score * 0.7 + long_price_bias * 0.3).clamp(0.0, 1.0);
        let short_price_score = (small_move_score * 0.7 + short_price_bias * 0.3).clamp(0.0, 1.0);

        let long_delta_score = normalize_threshold(
            delta_ratio.max(0.0),
            self.config.signal.delta_ratio_threshold,
            self.config.signal.delta_ratio_threshold * 3.0,
        );
        let short_delta_score = normalize_threshold(
            (-delta_ratio).max(0.0),
            self.config.signal.delta_ratio_threshold,
            self.config.signal.delta_ratio_threshold * 3.0,
        );
        let long_cvd_score = normalize_threshold(
            cvd_ratio.max(0.0),
            self.config.signal.delta_ratio_threshold,
            self.config.signal.delta_ratio_threshold * 3.0,
        );
        let short_cvd_score = normalize_threshold(
            (-cvd_ratio).max(0.0),
            self.config.signal.delta_ratio_threshold,
            self.config.signal.delta_ratio_threshold * 3.0,
        );
        let long_flow_score = (long_delta_score * 0.55 + long_cvd_score * 0.45).clamp(0.0, 1.0);
        let short_flow_score = (short_delta_score * 0.55 + short_cvd_score * 0.45).clamp(0.0, 1.0);
        let oi_score = normalize_threshold(
            oi_change_pct.max(0.0),
            self.config.signal.oi_change_threshold,
            self.config.signal.oi_change_threshold * 4.0,
        );

        let long_total =
            self.weighted_score(volume_score, long_price_score, long_flow_score, oi_score);
        let short_total =
            self.weighted_score(volume_score, short_price_score, short_flow_score, oi_score);

        let long_breakdown = SignalBreakdown {
            total_score: long_total,
            volume_score,
            price_score: long_price_score,
            flow_score: long_flow_score,
            oi_score,
            volume_ratio,
            price_change_pct,
            delta_ratio,
            cvd_ratio,
            oi_change_pct,
            trend_ema,
            trend_close,
        };
        let short_breakdown = SignalBreakdown {
            total_score: short_total,
            volume_score,
            price_score: short_price_score,
            flow_score: short_flow_score,
            oi_score,
            volume_ratio,
            price_change_pct,
            delta_ratio,
            cvd_ratio,
            oi_change_pct,
            trend_ema,
            trend_close,
        };

        let trend_long = trend_close > trend_ema;
        let trend_short = trend_close < trend_ema;
        let threshold = self.config.signal.entry_score_threshold;

        let chosen = if self.config.account.allow_long && trend_long && long_total >= threshold {
            Some((OrderSide::Buy, long_breakdown))
        } else if self.config.account.allow_short && trend_short && short_total >= threshold {
            Some((OrderSide::Sell, short_breakdown))
        } else {
            None
        };

        if let Some((side, score)) = chosen {
            debug!(
                "{} 信号触发 side={:?} score={:.3} vol={:.2} price={:.3}% delta={:.3} cvd={:.3} oi={:.3} trend={:.2}/{:.2}",
                symbol,
                side,
                score.total_score,
                score.volume_ratio,
                score.price_change_pct,
                score.delta_ratio,
                score.cvd_ratio,
                score.oi_change_pct,
                score.trend_close,
                score.trend_ema
            );

            let atr = compute_atr(closed_5m, self.config.exits.atr_period)
                .ok_or_else(|| anyhow!("{} 无法重新计算 ATR", symbol))?;

            return Ok(Some(EntryCandidate {
                symbol: symbol.to_string(),
                side,
                bar: last_bar.clone(),
                atr,
                trend_ema,
                score,
                precision,
            }));
        }

        Ok(None)
    }

    fn weighted_score(&self, volume: f64, price: f64, flow: f64, oi: f64) -> f64 {
        let w = &self.config.signal.weights;
        (volume * w.volume_change
            + price * w.price_change
            + flow * w.cvd_delta
            + oi * w.open_interest)
            .clamp(0.0, 1.0)
    }

    async fn try_open_trade(&self, candidate: EntryCandidate) -> Result<()> {
        let qty = compute_entry_quantity(
            self.config.risk.fixed_notional_usdt,
            candidate.bar.close,
            &candidate.precision,
        )?;
        let mut params = HashMap::new();
        if self.config.account.dual_position_mode && matches!(self.market_type, MarketType::Futures)
        {
            params.insert(
                "positionSide".to_string(),
                position_side_label(candidate.side).to_string(),
            );
        }

        let mut request = OrderRequest::new(
            candidate.symbol.clone(),
            candidate.side,
            OrderType::Market,
            qty,
            None,
            self.market_type,
        );
        request.client_order_id = Some(format!(
            "acc-entry-{}-{}",
            side_label(candidate.side),
            Utc::now().timestamp_micros()
        ));
        request.params = Some(params);

        let order = self
            .account
            .exchange
            .create_order(request)
            .await
            .with_context(|| format!("{} 开仓失败", candidate.symbol))?;

        let entry_price = order.price.unwrap_or(candidate.bar.close);
        let filled_qty = if order.filled > 0.0 {
            order.filled
        } else {
            qty
        };
        let tp1_price = match candidate.side {
            OrderSide::Buy => entry_price + candidate.atr * self.config.exits.tp1_atr_multiple,
            OrderSide::Sell => entry_price - candidate.atr * self.config.exits.tp1_atr_multiple,
        };
        let tp2_price = match candidate.side {
            OrderSide::Buy => entry_price + candidate.atr * self.config.exits.tp2_atr_multiple,
            OrderSide::Sell => entry_price - candidate.atr * self.config.exits.tp2_atr_multiple,
        };
        let initial_stop = match candidate.side {
            OrderSide::Buy => entry_price - candidate.atr * self.config.exits.atr_stop_multiplier,
            OrderSide::Sell => entry_price + candidate.atr * self.config.exits.atr_stop_multiplier,
        };

        let trade_id = {
            let mut rt = self.runtime.write().await;
            rt.sequence += 1;
            let trade_id = format!(
                "acc-{}-{}-{}",
                normalize_symbol_id(&candidate.symbol),
                side_label(candidate.side),
                rt.sequence
            );
            rt.trades.insert(
                trade_id.clone(),
                ManagedTrade {
                    id: trade_id.clone(),
                    symbol: candidate.symbol.clone(),
                    side: candidate.side,
                    entry_time: Utc::now(),
                    entry_bar_close: candidate.bar.close_time,
                    entry_price,
                    quantity: filled_qty,
                    remaining_quantity: filled_qty,
                    tp1_target_quantity: filled_qty * self.config.exits.tp1_fraction,
                    tp2_target_quantity: filled_qty * (1.0 - self.config.exits.tp1_fraction),
                    notional: filled_qty * entry_price,
                    atr_at_entry: candidate.atr,
                    tp1_price,
                    tp2_price,
                    tp1_order: None,
                    tp2_order: None,
                    trailing_stop_price: initial_stop,
                    signal: candidate.score.clone(),
                    realized_pnl: 0.0,
                    bars_observed: 0,
                    latest_close: candidate.bar.close,
                    highest_price: entry_price,
                    lowest_price: entry_price,
                    max_favorable_move: 0.0,
                    max_adverse_move: 0.0,
                    tp1_hit: false,
                    tp2_hit: false,
                    stop_loss_hit: false,
                    time_stop_hit: false,
                    active: true,
                    exit_reason: None,
                    last_processed_bar: Some(candidate.bar.close_time),
                    review_sent: false,
                },
            );
            let symbol_state = rt.symbols.entry(candidate.symbol.clone()).or_default();
            symbol_state.entry_times.push_back(Utc::now());
            while let Some(front) = symbol_state.entry_times.front() {
                if *front < Utc::now() - ChronoDuration::hours(6) {
                    symbol_state.entry_times.pop_front();
                } else {
                    break;
                }
            }
            trade_id
        };

        info!(
            "{} 开仓成功 id={} side={} qty={:.6} entry={:.4} score={:.3} trend_close={:.4} ema={:.4}",
            candidate.symbol,
            trade_id,
            side_label(candidate.side),
            filled_qty,
            entry_price,
            candidate.score.total_score,
            candidate.score.trend_close,
            candidate.trend_ema
        );

        Ok(())
    }

    async fn send_review_if_due(&self, trade_id: &str) -> Result<()> {
        let trade = {
            let rt = self.runtime.read().await;
            rt.trades
                .get(trade_id)
                .cloned()
                .ok_or_else(|| anyhow!("trade {} not found", trade_id))?
        };

        if trade.review_sent || !self.config.review.enabled {
            return Ok(());
        }

        let report = self.build_review_report(&trade);
        if let Some(url) = self.config.review.webhook_url.clone() {
            let payload = json!({
                "msgtype": "markdown",
                "markdown": {
                    "content": report
                }
            });
            let resp = self.http_client.post(url).json(&payload).send().await?;
            if !resp.status().is_success() {
                return Err(anyhow!("Webhook 返回异常: {}", resp.status()));
            }
        } else {
            info!("{} 回顾:\n{}", trade.symbol, report);
        }

        {
            let mut rt = self.runtime.write().await;
            if let Some(existing) = rt.trades.get_mut(trade_id) {
                existing.review_sent = true;
            }
        }

        Ok(())
    }

    fn build_review_report(&self, trade: &ManagedTrade) -> String {
        let review_close = trade.latest_close;
        let floating_pnl = if trade.remaining_quantity > 0.0 {
            match trade.side {
                OrderSide::Buy => (review_close - trade.entry_price) * trade.remaining_quantity,
                OrderSide::Sell => (trade.entry_price - review_close) * trade.remaining_quantity,
            }
        } else {
            0.0
        };
        let total_pnl = trade.realized_pnl + floating_pnl;
        let pnl_pct = if trade.notional.abs() <= f64::EPSILON {
            0.0
        } else {
            total_pnl / trade.notional
        };
        let path = classify_path(trade, review_close);
        let validity = classify_validity(total_pnl, trade);
        let suggestions = build_optimization_suggestions(trade, pnl_pct);
        let pnl_color = if total_pnl >= 0.0 { "warning" } else { "info" };
        let direction = if trade.side == OrderSide::Buy {
            "多头"
        } else {
            "空头"
        };
        let mut content = format!(
            "## {}\n\n> 交易对: `{}`\n> 方向: `{}`\n> 入场时间: `{}`\n> 回顾时间: `{}`\n> 入场价: `{:.4}`\n> 15根收盘价: `{:.4}`\n> 固定名义: `{:.2} USDT`\n> 总盈亏: <font color=\"{}\">{:+.2} USDT ({:+.2}%)</font>\n> 最高价: `{:.4}`\n> 最低价: `{:.4}`\n> 走势特征: `{}`\n> 信号有效性: `{}`\n\n### 触发结果\n- 止盈一段: {}\n- 止盈二段: {}\n- ATR动态止损: {}\n- 时间止损: {}\n\n### 信号拆解\n- 总分: `{:.3}`\n- VolChange(0.35): `{:.3}` | 成交量放大 `{:.2}x`\n- PriceChange(0.10): `{:.3}` | 5m 涨跌 `{:+.3}%`\n- CVD/Delta(0.35): `{:.3}` | Delta `{:+.3}` / CVD `{:+.3}`\n- OI(0.20): `{:.3}` | OI变化 `{:+.3}%`\n- 15m EMA50 趋势: `收盘 {:.4} / EMA {:.4}`\n\n### 优化建议\n",
            self.config.review.title,
            trade.symbol,
            direction,
            trade.entry_time.format("%Y-%m-%d %H:%M:%S UTC"),
            Utc::now().format("%Y-%m-%d %H:%M:%S UTC"),
            trade.entry_price,
            review_close,
            trade.notional,
            pnl_color,
            total_pnl,
            pnl_pct * 100.0,
            trade.highest_price,
            trade.lowest_price,
            path,
            validity,
            yes_no(trade.tp1_hit),
            yes_no(trade.tp2_hit),
            yes_no(trade.stop_loss_hit),
            yes_no(trade.time_stop_hit),
            trade.signal.total_score,
            trade.signal.volume_score,
            trade.signal.volume_ratio,
            trade.signal.price_score,
            trade.signal.price_change_pct,
            trade.signal.flow_score,
            trade.signal.delta_ratio,
            trade.signal.cvd_ratio,
            trade.signal.oi_score,
            trade.signal.oi_change_pct * 100.0,
            trade.signal.trend_close,
            trade.signal.trend_ema,
        );

        for suggestion in suggestions {
            content.push_str(&format!("- {}\n", suggestion));
        }

        content
    }

    async fn active_trade_count(&self, symbol: &str) -> usize {
        let rt = self.runtime.read().await;
        rt.trades
            .values()
            .filter(|trade| trade.symbol == symbol && trade.active)
            .count()
    }

    async fn in_entry_cooldown(&self, symbol: &str) -> bool {
        let cutoff = Utc::now()
            - ChronoDuration::minutes(self.config.risk.min_entry_interval_minutes.max(0));
        let rt = self.runtime.read().await;
        rt.symbols
            .get(symbol)
            .and_then(|state| state.entry_times.back().copied())
            .map(|ts| ts > cutoff)
            .unwrap_or(false)
    }

    async fn flow_ratios(&self, symbol: &str) -> (f64, f64) {
        let rt = self.runtime.read().await;
        let Some(state) = rt.symbols.get(symbol) else {
            return (0.0, 0.0);
        };
        let mut current_delta_ratio = 0.0;
        if let Some(last) = state.delta_history.back() {
            if last.total_quote > 0.0 {
                current_delta_ratio = last.delta_quote / last.total_quote;
            }
        }

        let mut sum_delta = 0.0;
        let mut sum_total = 0.0;
        for sample in state
            .delta_history
            .iter()
            .rev()
            .take(self.config.signal.delta_lookback_bars.max(1))
        {
            sum_delta += sample.delta_quote;
            sum_total += sample.total_quote;
        }

        let cvd_ratio = if sum_total > 0.0 {
            sum_delta / sum_total
        } else {
            0.0
        };
        (current_delta_ratio, cvd_ratio)
    }

    async fn refresh_status(&self) {
        let positions = {
            let rt = self.runtime.read().await;
            rt.trades
                .values()
                .filter(|trade| trade.active && trade.remaining_quantity > 0.0)
                .map(|trade| StrategyPosition {
                    symbol: trade.symbol.clone(),
                    net_position: match trade.side {
                        OrderSide::Buy => trade.remaining_quantity,
                        OrderSide::Sell => -trade.remaining_quantity,
                    },
                    notional: trade.remaining_quantity * trade.latest_close.abs(),
                })
                .collect::<Vec<_>>()
        };

        let uptime = {
            let started_at = self.started_at.read().await;
            started_at.as_ref().map(Instant::elapsed)
        };

        let mut status = self.status.write().await;
        status.state = if *self.running.read().await {
            StrategyState::Running
        } else {
            StrategyState::Stopped
        };
        status.positions = positions;
        status.uptime = uptime;
        status.updated_at = Utc::now();
    }

    async fn is_running(&self) -> bool {
        *self.running.read().await
    }
}

impl Strategy for AccumulationStrategy {
    type Config = AccumulationConfig;

    fn create(config: Self::Config, deps: StrategyDeps) -> Result<Self>
    where
        Self: Sized,
    {
        Self::new(config, deps)
    }
}

#[async_trait]
impl StrategyInstance for AccumulationStrategy {
    async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);

        {
            let mut started = self.started_at.write().await;
            *started = Some(Instant::now());
        }
        {
            let mut status = self.status.write().await;
            status.state = StrategyState::Running;
            status.updated_at = Utc::now();
            status.last_error = None;
        }

        info!(
            "吸筹策略启动，账户={}，交易对数量={}",
            self.config.account.account_id,
            self.config.symbols.iter().filter(|cfg| cfg.enabled).count()
        );

        if let Ok(positions) = self.account.exchange.get_positions(None).await {
            for position in positions.into_iter().filter(|position| {
                self.config
                    .symbols
                    .iter()
                    .any(|cfg| cfg.enabled && cfg.symbol == position.symbol)
            }) {
                warn!(
                    "启动前检测到已有仓位 symbol={} side={} amount={:.6}，请确认未与其他策略冲突",
                    position.symbol, position.side, position.amount
                );
            }
        }

        self.spawn_tasks().await?;
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        {
            let mut running = self.running.write().await;
            if !*running {
                return Ok(());
            }
            *running = false;
        }

        let mut handles = self.task_handles.lock().await;
        for handle in handles.drain(..) {
            handle.abort();
        }

        let mut status = self.status.write().await;
        status.state = StrategyState::Stopped;
        status.updated_at = Utc::now();
        Ok(())
    }

    async fn status(&self) -> Result<StrategyStatus> {
        self.refresh_status().await;
        Ok(self.status.read().await.clone())
    }
}

fn closed_klines(klines: &[Kline]) -> Vec<Kline> {
    if klines.len() <= 1 {
        return Vec::new();
    }
    klines[..klines.len() - 1].to_vec()
}

fn percentage_change(current: f64, previous: f64) -> f64 {
    if previous.abs() <= f64::EPSILON {
        0.0
    } else {
        ((current - previous) / previous) * 100.0
    }
}

fn compute_volume_ratio(klines: &[Kline], lookback: usize, latest_volume: f64) -> f64 {
    let previous = klines
        .iter()
        .rev()
        .skip(1)
        .take(lookback.max(1))
        .map(|bar| bar.volume)
        .collect::<Vec<_>>();
    if previous.is_empty() {
        return 0.0;
    }
    let avg = previous.iter().sum::<f64>() / previous.len() as f64;
    if avg.abs() <= f64::EPSILON {
        0.0
    } else {
        latest_volume / avg
    }
}

fn normalize_threshold(value: f64, threshold: f64, upper: f64) -> f64 {
    if value <= threshold {
        0.0
    } else if upper <= threshold {
        1.0
    } else {
        ((value - threshold) / (upper - threshold)).clamp(0.0, 1.0)
    }
}

fn compute_ema(values: &[f64], period: usize) -> Option<f64> {
    if values.len() < period || period == 0 {
        return None;
    }
    let multiplier = 2.0 / (period as f64 + 1.0);
    let mut ema = values[..period].iter().sum::<f64>() / period as f64;
    for value in &values[period..] {
        ema = (*value - ema) * multiplier + ema;
    }
    Some(ema)
}

fn compute_atr(klines: &[Kline], period: usize) -> Option<f64> {
    if klines.len() < period + 1 || period == 0 {
        return None;
    }
    let mut trs = Vec::new();
    for index in 1..klines.len() {
        let current = &klines[index];
        let prev_close = klines[index - 1].close;
        let tr = (current.high - current.low)
            .max((current.high - prev_close).abs())
            .max((current.low - prev_close).abs());
        trs.push(tr);
    }
    let tail = &trs[trs.len() - period..];
    Some(tail.iter().sum::<f64>() / period as f64)
}

fn precision_from_step(step: f64) -> u32 {
    if step <= 0.0 {
        return 0;
    }
    let mut precision = 0;
    let mut scaled = step;
    while precision < 12 && (scaled.round() - scaled).abs() > 1e-9 {
        scaled *= 10.0;
        precision += 1;
    }
    precision
}

fn trim_to_precision(value: f64, precision: u32) -> f64 {
    let factor = 10_f64.powi(precision as i32);
    (value * factor).floor() / factor
}

fn floor_to_step(value: f64, step: f64, precision: u32) -> f64 {
    if step <= 0.0 {
        return trim_to_precision(value, precision);
    }
    let adjusted = (value / step).floor() * step;
    trim_to_precision(adjusted, precision)
}

fn ceil_to_step(value: f64, step: f64, precision: u32) -> f64 {
    if step <= 0.0 {
        return trim_to_precision(value, precision);
    }
    let adjusted = (value / step).ceil() * step;
    trim_to_precision(adjusted, precision)
}

fn compute_entry_quantity(notional: f64, price: f64, precision: &SymbolPrecision) -> Result<f64> {
    if price <= 0.0 {
        return Err(anyhow!("invalid entry price: {}", price));
    }
    let mut qty = floor_to_step(
        notional / price,
        precision.step_size,
        precision.qty_precision,
    );
    if qty < precision.min_qty {
        qty = ceil_to_step(
            precision.min_qty,
            precision.step_size,
            precision.qty_precision,
        );
    }

    if precision.min_notional > 0.0 && qty * price < precision.min_notional {
        qty = ceil_to_step(
            precision.min_notional / price,
            precision.step_size,
            precision.qty_precision,
        );
    }

    if qty <= 0.0 {
        return Err(anyhow!("quantity becomes zero after precision adjustment"));
    }
    Ok(qty)
}

fn normalize_exit_quantity(quantity: f64, precision: &SymbolPrecision) -> f64 {
    let qty = floor_to_step(quantity, precision.step_size, precision.qty_precision);
    if qty < precision.min_qty {
        0.0
    } else {
        qty
    }
}

fn position_side_label(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "LONG",
        OrderSide::Sell => "SHORT",
    }
}

fn side_label(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "long",
        OrderSide::Sell => "short",
    }
}

fn normalize_symbol_id(symbol: &str) -> String {
    symbol
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
        .collect::<String>()
}

fn compute_delta_from_trades(bar: &Kline, trades: &[Trade]) -> BarDeltaSample {
    let start = bar.open_time.timestamp_millis();
    let end = bar.close_time.timestamp_millis();
    let mut delta_quote = 0.0;
    let mut total_quote = 0.0;

    for trade in trades {
        let ts = trade.timestamp.timestamp_millis();
        if ts < start || ts >= end {
            continue;
        }
        let quote = trade.amount * trade.price;
        total_quote += quote.abs();
        match trade.side {
            OrderSide::Buy => delta_quote += quote,
            OrderSide::Sell => delta_quote -= quote,
        }
    }

    if total_quote <= f64::EPSILON {
        total_quote = bar.quote_volume.abs();
    }

    BarDeltaSample {
        close_time: bar.close_time,
        delta_quote,
        total_quote,
    }
}

fn plan_exit_actions(
    trade: &ManagedTrade,
    bar: &Kline,
    _latest_atr: f64,
    exits: &ExitConfig,
    precision: &SymbolPrecision,
) -> Vec<ExitAction> {
    if !trade.active || trade.remaining_quantity <= precision.min_qty * 0.5 {
        return Vec::new();
    }

    let stop_hit = match trade.side {
        OrderSide::Buy => bar.low <= trade.trailing_stop_price,
        OrderSide::Sell => bar.high >= trade.trailing_stop_price,
    };
    let tp1_hit = !trade.tp1_hit
        && match trade.side {
            OrderSide::Buy => bar.high >= trade.tp1_price,
            OrderSide::Sell => bar.low <= trade.tp1_price,
        };
    let tp2_hit = !trade.tp2_hit
        && match trade.side {
            OrderSide::Buy => bar.high >= trade.tp2_price,
            OrderSide::Sell => bar.low <= trade.tp2_price,
        };

    if exits.conservative_intrabar && stop_hit && (tp1_hit || tp2_hit) {
        return vec![ExitAction {
            trade_id: trade.id.clone(),
            reason: ExitReason::StopLoss,
            quantity: trade.remaining_quantity,
            reference_price: trade.trailing_stop_price,
        }];
    }

    let mut actions = Vec::new();
    if tp1_hit {
        let partial = normalize_exit_quantity(trade.quantity * exits.tp1_fraction, precision);
        let partial = if partial <= precision.min_qty || partial >= trade.remaining_quantity {
            trade.remaining_quantity
        } else {
            partial
        };
        actions.push(ExitAction {
            trade_id: trade.id.clone(),
            reason: ExitReason::TakeProfit1,
            quantity: partial,
            reference_price: trade.tp1_price,
        });
    }
    if tp2_hit {
        actions.push(ExitAction {
            trade_id: trade.id.clone(),
            reason: ExitReason::TakeProfit2,
            quantity: trade.remaining_quantity,
            reference_price: trade.tp2_price,
        });
    } else if stop_hit {
        actions.push(ExitAction {
            trade_id: trade.id.clone(),
            reason: ExitReason::StopLoss,
            quantity: trade.remaining_quantity,
            reference_price: trade.trailing_stop_price,
        });
    } else if trade.bars_observed >= exits.time_stop_bars {
        actions.push(ExitAction {
            trade_id: trade.id.clone(),
            reason: ExitReason::TimeStop,
            quantity: trade.remaining_quantity,
            reference_price: bar.close,
        });
    }

    actions
}

fn yes_no(value: bool) -> &'static str {
    if value {
        "是"
    } else {
        "否"
    }
}

fn classify_validity(total_pnl: f64, trade: &ManagedTrade) -> &'static str {
    if total_pnl > 0.0 || trade.tp1_hit || trade.tp2_hit {
        "有效"
    } else if total_pnl < 0.0 || trade.stop_loss_hit {
        "无效"
    } else {
        "一般"
    }
}

fn classify_path(trade: &ManagedTrade, review_close: f64) -> &'static str {
    let close_ret = match trade.side {
        OrderSide::Buy => (review_close - trade.entry_price) / trade.entry_price,
        OrderSide::Sell => (trade.entry_price - review_close) / trade.entry_price,
    };
    let range_pct = if trade.entry_price.abs() <= f64::EPSILON {
        0.0
    } else {
        (trade.highest_price - trade.lowest_price) / trade.entry_price
    };
    let mfe_pct = if trade.entry_price.abs() <= f64::EPSILON {
        0.0
    } else {
        trade.max_favorable_move / trade.entry_price
    };
    let mae_pct = if trade.entry_price.abs() <= f64::EPSILON {
        0.0
    } else {
        trade.max_adverse_move / trade.entry_price
    };

    if range_pct < 0.003 {
        return "窄幅震荡";
    }
    if close_ret > 0.004 && mae_pct < mfe_pct * 0.4 {
        return "单边顺势";
    }
    if close_ret < -0.003 && mae_pct > mfe_pct {
        return "逆向失效";
    }
    if trade.side == OrderSide::Buy {
        if trade.highest_price > trade.entry_price && review_close < trade.entry_price {
            "冲高回落"
        } else if trade.lowest_price < trade.entry_price && review_close > trade.entry_price {
            "探底回升"
        } else {
            "震荡拉扯"
        }
    } else if trade.lowest_price < trade.entry_price && review_close > trade.entry_price {
        "下探反抽"
    } else if trade.highest_price > trade.entry_price && review_close < trade.entry_price {
        "冲高转弱"
    } else {
        "震荡拉扯"
    }
}

fn build_optimization_suggestions(trade: &ManagedTrade, pnl_pct: f64) -> Vec<String> {
    let mut suggestions = Vec::new();

    if trade.signal.flow_score < 0.55 {
        suggestions.push("提高 Delta/CVD 最低阈值，减少仅靠放量但主动性不足的信号。".to_string());
    }
    if trade.signal.oi_score < 0.45 {
        suggestions.push("提高 OI 变化过滤，尽量只保留增仓驱动更明确的吸筹信号。".to_string());
    }
    if trade.time_stop_hit && pnl_pct.abs() < 0.003 {
        suggestions.push("时间止损前收益不明显，建议提高总分阈值或缩小横盘参与范围。".to_string());
    }
    if trade.stop_loss_hit && !trade.tp1_hit {
        suggestions.push("止损先于止盈触发，可适度加强15m EMA趋势过滤或上调入场分数。".to_string());
    }
    if trade.tp1_hit && !trade.tp2_hit {
        suggestions
            .push("已实现一段止盈但无法延续，建议下调二段止盈目标或更早收紧动态止损。".to_string());
    }
    if suggestions.is_empty() {
        suggestions.push("当前参数与信号表现匹配度较好，可继续积累样本后再细调阈值。".to_string());
    }

    suggestions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weighted_score_respects_config_weights() {
        let config = AccumulationConfig {
            strategy: StrategyInfo {
                name: "test".to_string(),
                version: "1.0.0".to_string(),
                enabled: true,
                log_level: "INFO".to_string(),
                description: None,
            },
            account: AccountConfig {
                account_id: "binance_hcr".to_string(),
                market_type: MarketType::Futures,
                dual_position_mode: true,
                allow_long: true,
                allow_short: true,
            },
            data: DataConfig {
                scan_interval_secs: 60,
                lookback_5m: 120,
                lookback_15m: 120,
                trade_limit: 500,
            },
            symbols: vec![SymbolConfig {
                symbol: "ETH/USDC".to_string(),
                enabled: true,
            }],
            signal: SignalConfig {
                volume_lookback_bars: 20,
                delta_lookback_bars: 6,
                volume_spike_ratio: 1.8,
                small_price_change_pct: 0.35,
                delta_ratio_threshold: 0.12,
                oi_change_threshold: 0.002,
                entry_score_threshold: 0.68,
                weights: SignalWeights {
                    volume_change: 0.35,
                    price_change: 0.1,
                    cvd_delta: 0.35,
                    open_interest: 0.2,
                },
            },
            trend_filter: TrendFilterConfig { ema_period: 50 },
            risk: RiskConfig {
                fixed_notional_usdt: 50.0,
                max_active_trades_per_symbol: 2,
                min_entry_interval_minutes: 10,
            },
            exits: ExitConfig {
                atr_period: 14,
                atr_stop_multiplier: 1.4,
                tp1_atr_multiple: 1.0,
                tp2_atr_multiple: 2.0,
                tp1_fraction: 0.5,
                time_stop_bars: 15,
                conservative_intrabar: true,
            },
            review: ReviewConfig {
                enabled: true,
                review_bars: 15,
                title: "回顾".to_string(),
                webhook_url: None,
            },
        };

        let score = (0.8 * config.signal.weights.volume_change
            + 0.5 * config.signal.weights.price_change
            + 0.7 * config.signal.weights.cvd_delta
            + 0.6 * config.signal.weights.open_interest)
            .clamp(0.0, 1.0);
        assert!(score > 0.0);
        assert!(score <= 1.0);
    }

    #[test]
    fn classify_path_detects_single_trend() {
        let trade = ManagedTrade {
            id: "t1".to_string(),
            symbol: "ETH/USDC".to_string(),
            side: OrderSide::Buy,
            entry_time: Utc::now(),
            entry_bar_close: Utc::now(),
            entry_price: 100.0,
            quantity: 1.0,
            remaining_quantity: 0.0,
            notional: 100.0,
            atr_at_entry: 1.0,
            tp1_price: 101.0,
            tp2_price: 102.0,
            trailing_stop_price: 98.0,
            signal: SignalBreakdown {
                total_score: 0.8,
                volume_score: 0.8,
                price_score: 0.7,
                flow_score: 0.8,
                oi_score: 0.6,
                volume_ratio: 2.1,
                price_change_pct: -0.1,
                delta_ratio: 0.18,
                cvd_ratio: 0.16,
                oi_change_pct: 0.005,
                trend_ema: 99.0,
                trend_close: 101.0,
            },
            realized_pnl: 2.0,
            bars_observed: 15,
            latest_close: 103.0,
            highest_price: 104.0,
            lowest_price: 99.5,
            max_favorable_move: 4.0,
            max_adverse_move: 0.4,
            tp1_hit: true,
            tp2_hit: true,
            stop_loss_hit: false,
            time_stop_hit: false,
            active: false,
            exit_reason: Some(ExitReason::TakeProfit2),
            last_processed_bar: None,
            review_sent: false,
        };

        assert_eq!(classify_path(&trade, 103.0), "单边顺势");
    }
}
