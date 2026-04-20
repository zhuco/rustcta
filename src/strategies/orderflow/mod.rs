use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::Utc;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Instant;
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};

use crate::core::types::{
    BatchOrderResponse, MarketType, Order, OrderRequest, OrderSide, OrderStatus, OrderType,
    Position,
};
use crate::core::websocket::{BaseWebSocketClient, WebSocketClient};
use crate::cta::AccountManager;
use crate::strategies::common::application::{
    deps::StrategyDeps,
    status::StrategyStatus,
    strategy::{Strategy, StrategyInstance},
};
use crate::strategies::common::{
    RiskAction, RiskDecision, RiskNotifyLevel, StrategyRiskLimits, StrategySnapshot,
    UnifiedRiskEvaluator,
};
use crate::strategies::trend::execution_engine::decimals_from_step;

/// 基础订单流策略配置（占位，后续可扩展盘口/队列等参数）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderflowConfig {
    pub name: String,
    pub account_id: String,
    pub symbol: String,
    pub order_size: f64,
    #[serde(default = "default_spread_bps")]
    pub spread_bps: f64,
    #[serde(default = "default_orders_per_side")]
    pub orders_per_side: u32,
    #[serde(default = "default_refresh_secs")]
    pub refresh_secs: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    #[serde(default = "default_impact_limit")]
    pub max_impact_ratio: f64, // 盘口前几档累计量占比限制
    #[serde(default = "default_price_jump_bps")]
    pub price_jump_bps: f64, // 中价跳变阈值，超过则立即重挂
    #[serde(default = "default_imbalance_bias_bps")]
    pub imbalance_bias_bps: f64, // 盘口不平衡对中价的偏移灵敏度
    #[serde(default = "default_imbalance_levels")]
    pub imbalance_levels: usize, // 计算不平衡使用的档位数量
    #[serde(default = "default_ofi_levels")]
    pub ofi_levels: usize, // OFI 计算档数
    #[serde(default = "default_ofi_sensitivity_bps")]
    pub ofi_sensitivity_bps: f64, // OFI 对价差的调节灵敏度
    #[serde(default = "default_ofi_rebuild_bps")]
    pub ofi_rebuild_bps: f64, // OFI 极值触发重挂
    #[serde(default = "default_event_poll_ms")]
    pub event_poll_ms: u64, // 事件轮询间隔（ms）
    #[serde(default = "default_order_ttl_secs")]
    pub order_ttl_secs: u64, // 挂单最长存活时间，超时强制重挂
    #[serde(default = "default_open_orders_poll_secs")]
    pub open_orders_poll_secs: u64, // REST 获取挂单的最小轮询间隔
    #[serde(default = "default_queue_ahead_notional")]
    pub queue_ahead_notional: f64, // 允许排队在本单前的最大名义
    #[serde(default = "default_slippage_requote_bps")]
    pub slippage_requote_bps: f64, // 顶档滑点超阈值触发重挂
    #[serde(default)]
    pub max_position_notional: Option<f64>, // 仓位名义上限（USDC）
    #[serde(default)]
    pub inventory_skew_limit: Option<f64>, // 库存偏斜比例上限
    #[serde(default = "default_inventory_spread_bps")]
    pub inventory_spread_bps: f64, // 库存偏移对价差的灵敏度
    #[serde(default)]
    pub max_unrealized_loss: Option<f64>, // 未实现亏损阈值
    #[serde(default)]
    pub max_daily_loss: Option<f64>, // 日亏损阈值
    #[serde(default)]
    pub max_consecutive_losses: Option<u32>, // 连续亏损笔数
    #[serde(default)]
    pub stop_loss_pct: Option<f64>, // 相对持仓入场价的止损百分比
}

fn default_spread_bps() -> f64 {
    8.0 // 0.08%
}

fn default_orders_per_side() -> u32 {
    3
}

fn default_refresh_secs() -> u64 {
    5
}

fn default_batch_size() -> u32 {
    5
}

fn default_impact_limit() -> f64 {
    1.0 // 允许吃到前 100% 前档深度
}

fn default_price_jump_bps() -> f64 {
    5.0 // 0.05% 触发重挂
}

fn default_imbalance_bias_bps() -> f64 {
    5.0
}

fn default_imbalance_levels() -> usize {
    3
}

fn default_ofi_levels() -> usize {
    5
}

fn default_ofi_sensitivity_bps() -> f64 {
    6.0
}

fn default_ofi_rebuild_bps() -> f64 {
    12.0
}

fn default_event_poll_ms() -> u64 {
    1000
}

fn default_order_ttl_secs() -> u64 {
    90
}

fn default_open_orders_poll_secs() -> u64 {
    12
}

fn default_queue_ahead_notional() -> f64 {
    150.0
}

fn default_slippage_requote_bps() -> f64 {
    3.0
}

fn default_inventory_spread_bps() -> f64 {
    5.0
}

#[derive(Clone, Debug)]
struct SymbolMeta {
    tick_size: f64,
    step_size: f64,
    min_notional: f64,
    qty_precision: u32,
    price_precision: u32,
}

#[derive(Clone)]
struct RuntimeState {
    last_mid: Option<f64>,
    last_depth_ms: u128,
    last_event_ms: u128,
    active_orders: usize,
    last_submit_ms: u128,
    last_rebuild_price: Option<f64>,
    live_orders: HashMap<String, OrderRequest>,
    last_depth: Option<DepthSnapshot>,
    last_ofi: f64,
    last_entry_price: Option<f64>,
    last_unrealized_pnl: f64,
    ws_reconnects: u64,
    user_reconnects: u64,
    top_bid_qty: f64,
    top_ask_qty: f64,
    last_open_orders_poll: Instant,
    last_depth_update_id: u64,
    pending_depth_updates: Vec<DepthUpdate>,
    snapshot_last_update_id: Option<u64>,
    requote_triggers: u64,
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self {
            last_mid: None,
            last_depth_ms: 0,
            last_event_ms: 0,
            active_orders: 0,
            last_submit_ms: 0,
            last_rebuild_price: None,
            live_orders: HashMap::new(),
            last_depth: None,
            last_ofi: 0.0,
            last_entry_price: None,
            last_unrealized_pnl: 0.0,
            ws_reconnects: 0,
            user_reconnects: 0,
            top_bid_qty: 0.0,
            top_ask_qty: 0.0,
            last_open_orders_poll: Instant::now(),
            last_depth_update_id: 0,
            pending_depth_updates: Vec::new(),
            snapshot_last_update_id: None,
            requote_triggers: 0,
        }
    }
}

#[derive(Debug, Clone)]
struct DepthSnapshot {
    bids: Vec<[f64; 2]>,
    asks: Vec<[f64; 2]>,
}

#[derive(Clone, Debug)]
struct DepthUpdate {
    bids: Vec<[f64; 2]>,
    asks: Vec<[f64; 2]>,
    first_update_id: Option<u64>,
    final_update_id: Option<u64>,
    last_update_id: Option<u64>,
}
pub struct OrderflowStrategy {
    config: OrderflowConfig,
    account_manager: Arc<AccountManager>,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    risk_limits: Option<StrategyRiskLimits>,
    running: Arc<RwLock<bool>>,
    task: Arc<RwLock<Option<JoinHandle<()>>>>,
    event_task: Arc<RwLock<Option<JoinHandle<()>>>>,
    ws_task: Arc<RwLock<Option<JoinHandle<()>>>>,
    user_task: Arc<RwLock<Option<JoinHandle<()>>>>,
    meta: Arc<RwLock<Option<SymbolMeta>>>,
    state: Arc<RwLock<RuntimeState>>,
    notifier: Arc<Notify>,
}

impl Strategy for OrderflowStrategy {
    type Config = OrderflowConfig;

    fn create(config: Self::Config, deps: StrategyDeps) -> Result<Self> {
        let risk_limits = build_risk_limits(&config);
        Ok(Self {
            config,
            account_manager: deps.account_manager,
            risk_evaluator: deps.risk_evaluator,
            risk_limits,
            running: Arc::new(RwLock::new(false)),
            task: Arc::new(RwLock::new(None)),
            event_task: Arc::new(RwLock::new(None)),
            ws_task: Arc::new(RwLock::new(None)),
            user_task: Arc::new(RwLock::new(None)),
            meta: Arc::new(RwLock::new(None)),
            state: Arc::new(RwLock::new(RuntimeState::default())),
            notifier: Arc::new(Notify::new()),
        })
    }
}

#[async_trait]
impl StrategyInstance for OrderflowStrategy {
    async fn start(&self) -> Result<()> {
        {
            let mut guard = self.running.write().await;
            if *guard {
                return Ok(());
            }
            *guard = true;
        }

        info!(
            "🚀 启动订单流策略: {} | 账号={} | 交易对={} | 每单名义={}U",
            self.config.name, self.config.account_id, self.config.symbol, self.config.order_size
        );

        let account = self
            .account_manager
            .get_account(&self.config.account_id)
            .ok_or_else(|| anyhow!("账户 {} 不存在", self.config.account_id))?;

        let info = account
            .exchange
            .get_symbol_info(&self.config.symbol, MarketType::Futures)
            .await
            .map_err(|e| anyhow!("获取精度失败: {}", e))?;

        if info.tick_size <= 0.0 || info.step_size <= 0.0 {
            return Err(anyhow!(
                "精度非法: tick_size={} step_size={}",
                info.tick_size,
                info.step_size
            ));
        }

        let min_notional = info.min_notional.unwrap_or(0.0).max(0.0);
        {
            let mut guard = self.meta.write().await;
            *guard = Some(SymbolMeta {
                tick_size: info.tick_size,
                step_size: info.step_size,
                min_notional,
                qty_precision: decimals_from_step(info.step_size),
                price_precision: decimals_from_step(info.tick_size),
            });
        }

        let orders_per_side = self.config.orders_per_side.max(1);
        let spread_bps = (self.config.spread_bps / 10_000.0).max(0.0);
        let order_size_quote = self.config.order_size;
        let refresh_secs = self.config.refresh_secs.max(1);
        let symbol = self.config.symbol.clone();
        let strategy_name = self.config.name.clone();
        let account_id = self.config.account_id.clone();
        let account_manager = self.account_manager.clone();
        let risk_evaluator = self.risk_evaluator.clone();
        let risk_limits = self.risk_limits.clone();
        let running = self.running.clone();
        let meta = self.meta.clone();
        let state = self.state.clone();
        let batch_size = self.config.batch_size.max(1);
        let max_impact = self.config.max_impact_ratio.max(0.0);
        let price_jump = (self.config.price_jump_bps / 10_000.0).max(0.0);
        let imbalance_bias = self.config.imbalance_bias_bps / 10_000.0;
        let imbalance_levels = self.config.imbalance_levels.max(1);
        let ofi_levels = self.config.ofi_levels.max(1);
        let ofi_sensitivity = self.config.ofi_sensitivity_bps / 10_000.0;
        let ofi_rebuild = self.config.ofi_rebuild_bps / 10_000.0;
        let position_limit = self.config.max_position_notional.filter(|v| *v > 0.0);
        let inventory_spread = self.config.inventory_spread_bps / 10_000.0;
        let event_poll = Duration::from_millis(self.config.event_poll_ms.max(200));
        let order_ttl = Duration::from_secs(self.config.order_ttl_secs.max(1));
        let open_orders_poll = self.config.open_orders_poll_secs.max(1);
        let queue_ahead_notional = self.config.queue_ahead_notional.max(0.0);
        let slippage_requote = self.config.slippage_requote_bps / 10_000.0;
        let notifier = self.notifier.clone();
        let short_pause = Duration::from_millis(200);
        let watchdog_secs = order_ttl.as_secs().max(refresh_secs);
        let watchdog = Duration::from_secs(watchdog_secs.max(1));

        let handle = tokio::spawn(async move {
            let mut last_log = Instant::now();
            let mut first_run = true;
            loop {
                if !*running.read().await {
                    break;
                }

                if !first_run {
                    tokio::select! {
                        _ = notifier.notified() => {},
                        _ = sleep(watchdog) => {},
                    }
                    if !*running.read().await {
                        break;
                    }
                } else {
                    first_run = false;
                }

                let loop_start = Instant::now();
                let mut price_jump_triggered = false;

                // 优先使用本地 WS 盘口；若缺失/过期再拉 REST
                let mut depth_stale = false;
                let depth = {
                    let st = state.read().await;
                    if let Some(last) = &st.last_depth {
                        let age_ms = (Utc::now().timestamp_millis() as u128)
                            .saturating_sub(st.last_event_ms);
                        if age_ms < (refresh_secs as u128 * 2 * 1000) {
                            last.clone()
                        } else {
                            depth_stale = true;
                            DepthSnapshot {
                                bids: Vec::new(),
                                asks: Vec::new(),
                            }
                        }
                    } else {
                        depth_stale = true;
                        DepthSnapshot {
                            bids: Vec::new(),
                            asks: Vec::new(),
                        }
                    }
                };

                let depth = if depth.bids.is_empty() || depth.asks.is_empty() || depth_stale {
                    match account
                        .exchange
                        .get_orderbook(&symbol, MarketType::Futures, Some(20))
                        .await
                    {
                        Ok(book) => DepthSnapshot {
                            bids: book.bids.clone(),
                            asks: book.asks.clone(),
                        },
                        Err(e) => {
                            warn!("获取 {} 盘口失败: {}", symbol, e);
                            if state.read().await.last_mid.is_none() {
                                sleep(short_pause).await;
                                continue;
                            }
                            DepthSnapshot {
                                bids: Vec::new(),
                                asks: Vec::new(),
                            }
                        }
                    }
                } else {
                    depth
                };

                let mid = if !depth.bids.is_empty() && !depth.asks.is_empty() {
                    let best_bid = depth.bids.first().map(|b| b[0]);
                    let best_ask = depth.asks.first().map(|a| a[0]);
                    match (best_bid, best_ask) {
                        (Some(b), Some(a)) if a > b => {
                            let m = (a + b) / 2.0;
                            let mut st = state.write().await;
                            st.last_mid = Some(m);
                            st.last_depth_ms = loop_start.elapsed().as_millis();
                            st.last_depth = Some(depth.clone());
                            if let Some(prev) = st.last_rebuild_price {
                                let jump = ((m - prev) / prev).abs();
                                if jump > price_jump {
                                    price_jump_triggered = true;
                                }
                            }
                            st.last_rebuild_price = Some(m);
                            drop(st);

                            if price_jump_triggered {
                                warn!("{} 中价跳变 {:.4}% 触发重挂", symbol, price_jump * 100.0);
                            }
                            m
                        }
                        _ => {
                            warn!("{} 盘口异常，无法计算中价", symbol);
                            if let Some(prev) = state.read().await.last_mid {
                                prev
                            } else {
                                sleep(short_pause).await;
                                continue;
                            }
                        }
                    }
                } else if let Some(prev) = state.read().await.last_mid {
                    prev
                } else {
                    sleep(short_pause).await;
                    continue;
                };

                // 计算盘口不平衡与 OFI，对中价做偏移
                let imbalance = orderbook_imbalance(&depth, imbalance_levels);
                let ofi = order_flow_imbalance(
                    &depth,
                    state.read().await.last_depth.as_ref(),
                    ofi_levels,
                );
                {
                    let mut st = state.write().await;
                    st.last_ofi = ofi;
                }

                let mut mid_biased = mid * (1.0 + imbalance * imbalance_bias);
                if ofi.abs() > 1e-6 {
                    mid_biased *= 1.0 + ofi * ofi_sensitivity;
                }
                let top_bid = depth.bids.first().map(|l| l[0]);
                let top_ask = depth.asks.first().map(|l| l[0]);
                let mut slippage_trigger = false;
                if let (Some(b), Some(a)) = (top_bid, top_ask) {
                    let mid_now = (a + b) / 2.0;
                    let slip = ((mid_now - mid) / mid).abs();
                    if slip > slippage_requote {
                        slippage_trigger = true;
                    }
                }

                // 持仓与风险检查
                let position_snapshot = match account.exchange.get_positions(Some(&symbol)).await {
                    Ok(p) => compute_position_snapshot(&p, mid_biased),
                    Err(e) => {
                        warn!("{} 获取持仓失败: {}", symbol, e);
                        PositionSnapshot::default()
                    }
                };

                // 库存超限时主动减仓：|库存| > 5 时市价减 2 手（双向均适用）
                if position_snapshot.net_base.abs() > 5.0 {
                    let side = if position_snapshot.net_base > 0.0 {
                        OrderSide::Sell
                    } else {
                        OrderSide::Buy
                    };
                    let reduce_qty = position_snapshot.net_base.abs().min(2.0);
                    let mut reduce_order = OrderRequest::new(
                        symbol.clone(),
                        side,
                        OrderType::Market,
                        reduce_qty,
                        None,
                        MarketType::Futures,
                    );
                    reduce_order.reduce_only = Some(true);
                    reduce_order.time_in_force = Some("IOC".to_string());
                    match account.exchange.create_order(reduce_order).await {
                        Ok(_) => {
                            info!(
                                "{} 库存 {:.4} 超限，市价减仓 {:.4} ({:?})",
                                symbol, position_snapshot.net_base, reduce_qty, side
                            );
                        }
                        Err(e) => {
                            warn!(
                                "{} 库存超限市价减仓失败: {:.4} | {}",
                                symbol, position_snapshot.net_base, e
                            );
                        }
                    }
                    sleep(short_pause).await;
                    continue;
                }

                let snapshot = build_risk_snapshot(
                    &strategy_name,
                    &position_snapshot,
                    mid_biased,
                    risk_limits.clone(),
                );

                let decision = risk_evaluator.evaluate(&snapshot).await;
                let Some(size_scale) =
                    apply_risk_decision(&account_manager, &symbol, &account_id, decision).await
                else {
                    break;
                };

                if size_scale <= f64::EPSILON {
                    sleep(short_pause).await;
                    continue;
                }

                let (allow_buy, buy_scale) = side_scale(
                    position_snapshot.net_notional,
                    position_limit,
                    OrderSide::Buy,
                );
                let (allow_sell, sell_scale) = side_scale(
                    position_snapshot.net_notional,
                    position_limit,
                    OrderSide::Sell,
                );

                if !allow_buy && !allow_sell {
                    warn!("{} 仓位达到上限，暂停挂单", symbol);
                    sleep(short_pause).await;
                    continue;
                }

                if let Some((avg_entry, pnl)) = compute_unrealized(&position_snapshot, mid_biased) {
                    let mut st = state.write().await;
                    st.last_entry_price = Some(avg_entry);
                    st.last_unrealized_pnl = pnl;
                    st.last_event_ms = Utc::now().timestamp_millis() as u128;
                }

                let meta_guard = meta.read().await;
                let Some(meta) = meta_guard.as_ref() else {
                    warn!("{} meta 缺失，跳过本轮", symbol);
                    sleep(short_pause).await;
                    continue;
                };

                // 构造价梯
                let mut orders: Vec<OrderRequest> = Vec::new();
                let inventory_bias = if let Some(limit) = position_limit {
                    if limit > 0.0 {
                        (position_snapshot.net_notional / limit).clamp(-1.0, 1.0) * inventory_spread
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };

                for i in 0..orders_per_side {
                    let mut offset = spread_bps * (i + 1) as f64;

                    // OFI/库存偏移价差
                    offset += ofi.abs() * ofi_sensitivity + inventory_bias.abs();

                    let bid_price = round_price(mid_biased * (1.0 - offset), meta.tick_size);
                    let ask_price = round_price(mid_biased * (1.0 + offset), meta.tick_size);
                    // 确保名义达到最小值，必要时按 step_size 向上取整
                    let qty_bid_target = ((order_size_quote * size_scale * buy_scale) / bid_price)
                        .max(meta.min_notional / bid_price);
                    let qty_ask_target = ((order_size_quote * size_scale * sell_scale) / ask_price)
                        .max(meta.min_notional / ask_price);
                    let qty_bid = round_qty_ceil(qty_bid_target, meta.step_size);
                    let qty_ask = round_qty_ceil(qty_ask_target, meta.step_size);

                    // 冲击/深度检查：使用深度快照，计算到目标价的累积报价量
                    if !depth.bids.is_empty() && !depth.asks.is_empty() {
                        let notional_bid = qty_bid * bid_price;
                        let notional_ask = qty_ask * ask_price;

                        let depth_bid = cumulative_depth(&depth.bids, bid_price, true);
                        let depth_ask = cumulative_depth(&depth.asks, ask_price, false);

                        let impact_bid = if depth_bid > 0.0 {
                            notional_bid / depth_bid
                        } else {
                            f64::INFINITY
                        };
                        let impact_ask = if depth_ask > 0.0 {
                            notional_ask / depth_ask
                        } else {
                            f64::INFINITY
                        };

                        if impact_bid.is_infinite()
                            || impact_ask.is_infinite()
                            || impact_bid > max_impact
                            || impact_ask > max_impact
                        {
                            continue;
                        }
                    }

                    if allow_buy && qty_bid > 0.0 {
                        // 队列位次检查（买单）：若排队名义超过阈值则触发重挂
                        if queue_ahead_notional > 0.0 {
                            let ahead = queue_notional_ahead(&depth, bid_price, OrderSide::Buy);
                            if ahead > queue_ahead_notional {
                                price_jump_triggered = true;
                            }
                        }
                        let mut buy_req = OrderRequest::new(
                            symbol.clone(),
                            OrderSide::Buy,
                            OrderType::Limit,
                            qty_bid,
                            Some(bid_price),
                            MarketType::Futures,
                        );
                        buy_req.client_order_id = Some(format!(
                            "oflow-{}-B-{}-{}",
                            symbol,
                            i + 1,
                            chrono::Utc::now().timestamp_millis()
                        ));
                        buy_req.post_only = Some(true);
                        buy_req.time_in_force = Some("GTX".to_string());
                        orders.push(buy_req);
                    }

                    if allow_sell && qty_ask > 0.0 {
                        if queue_ahead_notional > 0.0 {
                            let ahead = queue_notional_ahead(&depth, ask_price, OrderSide::Sell);
                            if ahead > queue_ahead_notional {
                                price_jump_triggered = true;
                            }
                        }
                        let mut sell_req = OrderRequest::new(
                            symbol.clone(),
                            OrderSide::Sell,
                            OrderType::Limit,
                            qty_ask,
                            Some(ask_price),
                            MarketType::Futures,
                        );
                        sell_req.client_order_id = Some(format!(
                            "oflow-{}-S-{}-{}",
                            symbol,
                            i + 1,
                            chrono::Utc::now().timestamp_millis()
                        ));
                        sell_req.post_only = Some(true);
                        sell_req.time_in_force = Some("GTX".to_string());
                        orders.push(sell_req);
                    }
                }

                if orders.is_empty() {
                    warn!("{} 没有可提交的订单（名义不足或价阶为空）", symbol);
                    sleep(short_pause).await;
                    continue;
                }

                // 获取现有挂单，对比缺口：优先使用本地状态，周期性回退 REST 校准
                let mut open_orders: Vec<Order> = Vec::new();
                let mut use_rest = true;
                {
                    let st = state.read().await;
                    if st.last_open_orders_poll.elapsed().as_secs() < open_orders_poll
                        && !st.live_orders.is_empty()
                    {
                        use_rest = false;
                        for (id, req) in st.live_orders.iter() {
                            open_orders.push(Order {
                                id: id.clone(),
                                symbol: req.symbol.clone(),
                                side: req.side,
                                order_type: req.order_type,
                                amount: req.amount,
                                price: req.price,
                                filled: 0.0,
                                remaining: req.amount,
                                status: OrderStatus::Open,
                                market_type: req.market_type,
                                timestamp: Utc::now(),
                                last_trade_timestamp: None,
                                info: serde_json::Value::Null,
                            });
                        }
                    }
                }
                if use_rest {
                    match account
                        .exchange
                        .get_open_orders(Some(&symbol), MarketType::Futures)
                        .await
                    {
                        Ok(o) => {
                            open_orders = o;
                            let mut st = state.write().await;
                            st.last_open_orders_poll = Instant::now();
                        }
                        Err(e) => {
                            warn!("{} 获取挂单失败: {}", symbol, e);
                        }
                    }
                }

                // 以价格+方向为键对比
                let mut desired: HashMap<String, OrderRequest> = HashMap::new();
                for o in &orders {
                    let key = format!(
                        "{}-{}-{:.6}",
                        symbol,
                        match o.side {
                            OrderSide::Buy => "B",
                            OrderSide::Sell => "S",
                        },
                        o.price.unwrap_or(0.0)
                    );
                    desired.insert(key, o.clone());
                }

                let mut open_keys: HashMap<String, String> = HashMap::new();
                for o in &open_orders {
                    if let Some(price) = o.price {
                        let key = format!(
                            "{}-{}-{:.6}",
                            symbol,
                            match o.side {
                                OrderSide::Buy => "B",
                                OrderSide::Sell => "S",
                            },
                            price
                        );
                        open_keys.insert(key, o.id.clone());
                    }
                }

                let ofi_rebuild_trigger = ofi.abs() > ofi_rebuild;
                let slippage_trigger = slippage_trigger
                    || if let (Some(b), Some(a)) = (top_bid, top_ask) {
                        let best_mid = (a + b) / 2.0;
                        ((best_mid - mid) / mid).abs() > slippage_requote
                    } else {
                        false
                    };

                let (mut to_cancel, to_place): (Vec<String>, Vec<OrderRequest>) =
                    if price_jump_triggered || ofi_rebuild_trigger || slippage_trigger {
                        (
                            open_orders.iter().map(|o| o.id.clone()).collect(),
                            orders.clone(),
                        )
                    } else {
                        // 需要撤掉的（多余的）
                        let mut to_cancel: Vec<String> = Vec::new();
                        for (k, id) in &open_keys {
                            if !desired.contains_key(k) {
                                to_cancel.push(id.clone());
                                continue;
                            }
                            // TTL 过期也强制撤销
                            if let Some(o) = open_orders.iter().find(|oo| &oo.id == id) {
                                if Utc::now().signed_duration_since(o.timestamp).num_seconds()
                                    > order_ttl.as_secs() as i64
                                {
                                    to_cancel.push(id.clone());
                                }
                            }
                        }

                        // 需要补挂的
                        let mut to_place: Vec<OrderRequest> = Vec::new();
                        for (k, req) in desired.iter() {
                            if !open_keys.contains_key(k) {
                                to_place.push(req.clone());
                            }
                        }
                        (to_cancel, to_place)
                    };

                // 撤多余
                if !to_cancel.is_empty() {
                    if let Some(account) = account_manager.get_account(&account_id) {
                        for cid in to_cancel.drain(..) {
                            let _ = account
                                .exchange
                                .cancel_order(&cid, &symbol, MarketType::Futures)
                                .await;
                        }
                    }
                }

                // 补缺口
                if !to_place.is_empty() {
                    info!("{} 补单 {} 笔（缺口）", symbol, to_place.len());
                    let submit_start = Instant::now();
                    let mut placed: Vec<Order> = Vec::new();

                    for chunk in to_place.chunks(batch_size as usize) {
                        match account_manager
                            .create_batch_orders(&account_id, chunk.to_vec())
                            .await
                        {
                            Ok(BatchOrderResponse {
                                successful_orders,
                                failed_orders,
                            }) => {
                                if !successful_orders.is_empty() {
                                    info!("{} 补单成功: {} 笔", symbol, successful_orders.len());
                                    placed.extend(successful_orders);
                                }
                                for err in failed_orders {
                                    warn!(
                                        "{} 补单失败: {} @ {:?} | {}",
                                        symbol,
                                        err.order_request.amount,
                                        err.order_request.price,
                                        err.error_message
                                    );
                                }
                            }
                            Err(e) => {
                                warn!("{} 补单批量失败: {}", symbol, e);
                            }
                        }
                    }
                    let mut st = state.write().await;
                    st.last_submit_ms = submit_start.elapsed().as_millis();
                    st.active_orders = desired.len();
                    st.live_orders.clear();
                    for o in placed {
                        st.live_orders.insert(
                            o.id.clone(),
                            OrderRequest {
                                symbol: o.symbol.clone(),
                                side: o.side.clone(),
                                order_type: OrderType::Limit,
                                amount: o.amount,
                                price: o.price,
                                market_type: MarketType::Futures,
                                client_order_id: None,
                                params: None,
                                post_only: None,
                                reduce_only: None,
                                time_in_force: None,
                            },
                        );
                    }
                } else {
                    let mut st = state.write().await;
                    st.active_orders = open_keys.len();
                    st.live_orders = desired;
                }

                if last_log.elapsed() > Duration::from_secs(30) {
                    let st = state.read().await;
                    info!(
                        "{} 状态 mid={:.4} ofi={:.4} orders={} pnl~={:.2} depth_ms={} submit_ms={} last_evt_ms={}",
                        symbol,
                        st.last_mid.unwrap_or(0.0),
                        st.last_ofi,
                        st.active_orders,
                        st.last_unrealized_pnl,
                        st.last_depth_ms,
                        st.last_submit_ms,
                        st.last_event_ms,
                    );
                    last_log = Instant::now();
                }
            }
        });

        *self.task.write().await = Some(handle);

        // 事件轮询任务：检测成交事件促使重挂
        let evt_running = self.running.clone();
        let evt_state = self.state.clone();
        let evt_notifier = self.notifier.clone();
        let evt_account_id = self.config.account_id.clone();
        let evt_symbol = self.config.symbol.clone();
        let evt_account_mgr = self.account_manager.clone();
        let evt_poll = event_poll;
        let event_handle = tokio::spawn(async move {
            let mut last_ts: Option<i64> = None;
            while *evt_running.read().await {
                if let Some(account) = evt_account_mgr.get_account(&evt_account_id) {
                    match account
                        .exchange
                        .get_my_trades(Some(&evt_symbol), MarketType::Futures, Some(50))
                        .await
                    {
                        Ok(trades) => {
                            if let Some(max_ts) =
                                trades.iter().map(|t| t.timestamp.timestamp_millis()).max()
                            {
                                if last_ts.map(|p| max_ts > p).unwrap_or(true) {
                                    last_ts = Some(max_ts);
                                    {
                                        let mut st = evt_state.write().await;
                                        st.last_event_ms = Utc::now().timestamp_millis() as u128;
                                    }
                                    evt_notifier.notify_one();
                                }
                            }
                        }
                        Err(e) => {
                            warn!("{} 事件轮询失败: {}", evt_symbol, e);
                        }
                    }
                }

                sleep(evt_poll).await;
            }
        });

        *self.event_task.write().await = Some(event_handle);

        // 公共行情 WebSocket：深度+成交事件驱动
        let ws_running = self.running.clone();
        let ws_state = self.state.clone();
        let ws_notifier = self.notifier.clone();
        let ws_account_mgr = self.account_manager.clone();
        let ws_account_id = self.config.account_id.clone();
        let ws_symbol = self.config.symbol.clone();
        let ws_handle = tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                if !*ws_running.read().await {
                    break;
                }

                let Some(account) = ws_account_mgr.get_account(&ws_account_id) else {
                    sleep(Duration::from_secs(2)).await;
                    continue;
                };

                // 初始快照，确保增量有基线
                if let Ok(book) = account
                    .exchange
                    .get_orderbook(&ws_symbol, MarketType::Futures, Some(100))
                    .await
                {
                    let mut st = ws_state.write().await;
                    st.last_depth = Some(DepthSnapshot {
                        bids: book.bids.clone(),
                        asks: book.asks.clone(),
                    });
                    st.last_depth_update_id = 0;
                    st.pending_depth_updates.clear();
                    st.snapshot_last_update_id =
                        book.info.get("lastUpdateId").and_then(|v| v.as_u64());
                }

                let mut client = match account
                    .exchange
                    .create_websocket_client(MarketType::Futures)
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        warn!("{} 创建行情WS失败: {}", ws_symbol, e);
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        continue;
                    }
                };

                if let Err(e) = client.connect().await {
                    warn!("{} 行情WS连接失败: {}", ws_symbol, e);
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue;
                }

                let stream_symbol = build_stream_symbol(&ws_symbol);
                let sub = serde_json::json!({
                    "method":"SUBSCRIBE",
                    "params":[
                        format!("{}@depth20@100ms", stream_symbol),
                        format!("{}@trade", stream_symbol)
                    ],
                    "id":1
                });
                let _ = client.send(sub.to_string()).await;
                backoff = Duration::from_secs(1);
                {
                    let mut st = ws_state.write().await;
                    st.ws_reconnects += 1;
                }

                loop {
                    if !*ws_running.read().await {
                        let _ = client.disconnect().await;
                        break;
                    }
                    match client.receive().await {
                        Ok(Some(msg)) => {
                            if let Ok(val) = serde_json::from_str::<Value>(&msg) {
                                if let Some(event) = val.get("e").and_then(|v| v.as_str()) {
                                    match event {
                                        "depthUpdate" => {
                                            if let Some(update) = parse_binance_depth(&val) {
                                                let mut st = ws_state.write().await;
                                                let mut snapshot = st
                                                    .last_depth
                                                    .clone()
                                                    .unwrap_or_else(|| DepthSnapshot {
                                                        bids: Vec::new(),
                                                        asks: Vec::new(),
                                                    });

                                                // 严格对齐：未建基线或 lastUpdateId 未确认时先缓存，待命中 U<=base+1<=u 再应用
                                                let last = st.last_depth_update_id;
                                                let base = st.snapshot_last_update_id.unwrap_or(0);
                                                if last == 0 || base == 0 {
                                                    st.pending_depth_updates.push(update.clone());
                                                    if base > 0 {
                                                        if let Some(snapshot) =
                                                            st.last_depth.clone()
                                                        {
                                                            if let Some((idx, _)) = st
                                                                .pending_depth_updates
                                                                .iter()
                                                                .enumerate()
                                                                .find(|(_, up)| {
                                                                    if let (
                                                                        Some(u_first),
                                                                        Some(u_last),
                                                                    ) = (
                                                                        up.first_update_id,
                                                                        up.final_update_id,
                                                                    ) {
                                                                        u_first <= base + 1
                                                                            && u_last >= base + 1
                                                                    } else {
                                                                        false
                                                                    }
                                                                })
                                                            {
                                                                let mut snap = snapshot;
                                                                let pending = st
                                                                    .pending_depth_updates
                                                                    .split_off(idx);
                                                                for up in pending {
                                                                    apply_depth_update(
                                                                        &mut snap,
                                                                        up.clone(),
                                                                    );
                                                                    if let Some(uid) =
                                                                        up.final_update_id
                                                                    {
                                                                        st.last_depth_update_id =
                                                                            uid;
                                                                    }
                                                                }
                                                                st.last_depth = Some(snap);
                                                                st.pending_depth_updates.clear();
                                                            }
                                                        }
                                                    }
                                                    drop(st);
                                                    continue;
                                                }

                                                let apply_ok = should_apply_update(last, &update);

                                                if !apply_ok {
                                                    drop(st);
                                                    if let Some(account) =
                                                        ws_account_mgr.get_account(&ws_account_id)
                                                    {
                                                        if let Ok(book) = account
                                                            .exchange
                                                            .get_orderbook(
                                                                &ws_symbol,
                                                                MarketType::Futures,
                                                                Some(100),
                                                            )
                                                            .await
                                                        {
                                                            let mut st = ws_state.write().await;
                                                            st.last_depth = Some(DepthSnapshot {
                                                                bids: book.bids.clone(),
                                                                asks: book.asks.clone(),
                                                            });
                                                            st.last_depth_update_id = 0;
                                                            st.pending_depth_updates.clear();
                                                        }
                                                    }
                                                    continue;
                                                }

                                                apply_depth_update(&mut snapshot, update.clone());
                                                st.last_depth = Some(snapshot.clone());
                                                st.last_depth_update_id =
                                                    update.final_update_id.unwrap_or(
                                                        st.last_depth_update_id.saturating_add(1),
                                                    );
                                                st.last_event_ms =
                                                    Utc::now().timestamp_millis() as u128;
                                                if let (Some(b), Some(a)) =
                                                    (snapshot.bids.first(), snapshot.asks.first())
                                                {
                                                    st.last_mid = Some((b[0] + a[0]) / 2.0);
                                                    st.top_bid_qty = b[1];
                                                    st.top_ask_qty = a[1];
                                                }
                                                ws_notifier.notify_one();
                                            }
                                        }
                                        "trade" => {
                                            if let Some(price) = val
                                                .get("p")
                                                .and_then(|v| v.as_str())
                                                .and_then(|s| s.parse::<f64>().ok())
                                            {
                                                let mut st = ws_state.write().await;
                                                st.last_event_ms =
                                                    Utc::now().timestamp_millis() as u128;
                                                st.last_mid.get_or_insert(price);
                                                ws_notifier.notify_one();
                                            }
                                        }
                                        _ => {}
                                    }
                                }
                            }
                        }
                        Ok(None) => continue,
                        Err(e) => {
                            warn!("{} 行情WS接收失败: {}", ws_symbol, e);
                            let _ = client.disconnect().await;
                            backoff = (backoff * 2).min(Duration::from_secs(30));
                            break;
                        }
                    }
                }
            }
        });
        *self.ws_task.write().await = Some(ws_handle);

        // 用户数据流：订单/成交事件触发重挂
        let user_running = self.running.clone();
        let user_state = self.state.clone();
        let user_notifier = self.notifier.clone();
        let user_account_mgr = self.account_manager.clone();
        let user_account_id = self.config.account_id.clone();
        let user_symbol = self.config.symbol.clone();
        let user_handle = tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                if !*user_running.read().await {
                    break;
                }

                let Some(account) = user_account_mgr.get_account(&user_account_id) else {
                    sleep(Duration::from_secs(2)).await;
                    continue;
                };

                let listen_key = match account
                    .exchange
                    .create_user_data_stream(MarketType::Futures)
                    .await
                {
                    Ok(k) => k,
                    Err(e) => {
                        warn!("{} 获取 listenKey 失败: {}", user_symbol, e);
                        sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        continue;
                    }
                };

                let url = format!("wss://fstream.binance.com/ws/{}", listen_key);
                let mut client = BaseWebSocketClient::new(url, "binance".to_string());
                if let Err(e) = client.connect().await {
                    warn!("{} 用户流连接失败: {}", user_symbol, e);
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue;
                }
                backoff = Duration::from_secs(1);
                {
                    let mut st = user_state.write().await;
                    st.user_reconnects += 1;
                }

                let mut keepalive = tokio::time::interval(Duration::from_secs(60 * 20));

                loop {
                    if !*user_running.read().await {
                        let _ = client.disconnect().await;
                        break;
                    }

                    tokio::select! {
                        msg = client.receive() => {
                            match msg {
                                Ok(Some(raw)) => {
                                    if let Ok(val) = serde_json::from_str::<Value>(&raw) {
                                        if let Some(event) = val.get("e").and_then(|v| v.as_str()) {
                                            match event {
                                                "ORDER_TRADE_UPDATE" | "executionReport" => {
                                                    let mut st = user_state.write().await;
                                                    st.last_event_ms = Utc::now().timestamp_millis() as u128;
                                                    if let Some((oid, status)) = extract_order_event(&val) {
                                                        st.live_orders.remove(&oid);
                                                        if let Some(client_id) = status.client_order_id {
                                                            st.live_orders.remove(&client_id);
                                                        }
                                                        if status.should_requote {
                                                            st.requote_triggers += 1;
                                                            user_notifier.notify_one();
                                                        }
                                                    } else {
                                                        user_notifier.notify_one();
                                                    }
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    warn!("{} 用户流接收失败: {}", user_symbol, e);
                                    let _ = client.disconnect().await;
                                    backoff = (backoff * 2).min(Duration::from_secs(30));
                                    break;
                                }
                            }
                        }
                        _ = keepalive.tick() => {
                            if let Some(account) = user_account_mgr.get_account(&user_account_id) {
                                let _ = account
                                    .exchange
                                    .keepalive_user_data_stream(&listen_key, MarketType::Futures)
                                    .await;
                            }
                        }
                    }
                }
            }
        });
        *self.user_task.write().await = Some(user_handle);

        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let mut guard = self.running.write().await;
        *guard = false;
        if let Some(handle) = self.task.write().await.take() {
            handle.abort();
        }
        if let Some(handle) = self.event_task.write().await.take() {
            handle.abort();
        }
        if let Some(handle) = self.ws_task.write().await.take() {
            handle.abort();
        }
        if let Some(handle) = self.user_task.write().await.take() {
            handle.abort();
        }

        if let Some(account) = self.account_manager.get_account(&self.config.account_id) {
            if let Err(e) = account
                .exchange
                .cancel_all_orders(Some(&self.config.symbol), MarketType::Futures)
                .await
            {
                warn!("停止时撤销 {} 挂单失败: {}", self.config.symbol, e);
            }
        }

        info!("🛑 停止订单流策略: {}", self.config.name);
        Ok(())
    }

    async fn status(&self) -> Result<StrategyStatus> {
        let running = *self.running.read().await;
        let mut status = StrategyStatus::new(self.config.name.clone());
        status.state = if running {
            crate::strategies::common::application::status::StrategyState::Running
        } else {
            crate::strategies::common::application::status::StrategyState::Stopped
        };
        Ok(status)
    }
}

fn round_price(price: f64, tick: f64) -> f64 {
    if tick > 0.0 {
        ((price / tick).round()) * tick
    } else {
        price
    }
}

fn round_qty(qty: f64, step: f64) -> f64 {
    if step > 0.0 {
        ((qty / step).floor()) * step
    } else {
        qty
    }
}

fn round_qty_ceil(qty: f64, step: f64) -> f64 {
    if step > 0.0 {
        ((qty / step).ceil()) * step
    } else {
        qty
    }
}

fn cumulative_depth(levels: &[[f64; 2]], target_price: f64, is_bid: bool) -> f64 {
    let mut acc = 0.0;
    for level in levels {
        let price = level[0];
        let qty = level[1];
        if is_bid {
            if price >= target_price {
                acc += price * qty;
            } else {
                break;
            }
        } else {
            if price <= target_price {
                acc += price * qty;
            } else {
                break;
            }
        }
    }
    acc
}

fn queue_notional_ahead(depth: &DepthSnapshot, price: f64, side: OrderSide) -> f64 {
    match side {
        OrderSide::Buy => depth
            .bids
            .iter()
            .take_while(|l| l[0] > price + 1e-12)
            .map(|l| l[0] * l[1])
            .sum(),
        OrderSide::Sell => depth
            .asks
            .iter()
            .take_while(|l| l[0] < price - 1e-12)
            .map(|l| l[0] * l[1])
            .sum(),
    }
}

struct OrderEventStatus {
    order_id: String,
    client_order_id: Option<String>,
    should_requote: bool,
}

fn extract_order_event(val: &Value) -> Option<(String, OrderEventStatus)> {
    // Binance FUTS: ORDER_TRADE_UPDATE: data.o -> fields
    if let Some(data) = val.get("o") {
        let order_id = data
            .get("i")
            .and_then(|v| v.as_i64())
            .map(|v| v.to_string())?;
        let client_id = data
            .get("c")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let status = data.get("X").and_then(|v| v.as_str()).unwrap_or("");
        let should_requote = matches!(status, "CANCELED" | "FILLED" | "REJECTED" | "EXPIRED");
        return Some((
            order_id.clone(),
            OrderEventStatus {
                order_id,
                client_order_id: client_id,
                should_requote,
            },
        ));
    }
    // Spot executionReport: top-level fields
    let order_id = val
        .get("i")
        .and_then(|v| v.as_i64())
        .map(|v| v.to_string())?;
    let client_id = val.get("c").and_then(|v| v.as_str()).map(|s| s.to_string());
    let status = val.get("X").and_then(|v| v.as_str()).unwrap_or("");
    let should_requote = matches!(status, "CANCELED" | "FILLED" | "REJECTED" | "EXPIRED");
    Some((
        order_id.clone(),
        OrderEventStatus {
            order_id,
            client_order_id: client_id,
            should_requote,
        },
    ))
}

#[derive(Default, Clone)]
struct PositionSnapshot {
    net_base: f64,
    net_notional: f64,
    entry_notional: f64,
    long: f64,
    short: f64,
}

fn compute_position_snapshot(positions: &[Position], price: f64) -> PositionSnapshot {
    let mut snap = PositionSnapshot::default();
    let px = price.max(1e-9);
    for pos in positions {
        let qty = pos.contracts;
        let signed_qty = match pos.side.as_str() {
            "LONG" => qty,
            "SHORT" => -qty,
            _ => qty,
        };
        snap.net_base += signed_qty;
        snap.net_notional += signed_qty * pos.mark_price.max(px);
        snap.entry_notional += signed_qty * pos.entry_price.max(px);
        if signed_qty > 0.0 {
            snap.long += signed_qty;
        } else if signed_qty < 0.0 {
            snap.short += -signed_qty;
        }
    }
    snap
}

fn build_risk_limits(config: &OrderflowConfig) -> Option<StrategyRiskLimits> {
    if config.max_position_notional.is_none()
        && config.inventory_skew_limit.is_none()
        && config.max_unrealized_loss.is_none()
        && config.max_daily_loss.is_none()
        && config.stop_loss_pct.is_none()
        && config.max_consecutive_losses.is_none()
    {
        return None;
    }

    Some(StrategyRiskLimits {
        warning_scale_factor: Some(0.8),
        danger_scale_factor: Some(0.5),
        stop_loss_pct: config.stop_loss_pct,
        max_inventory_notional: config.max_position_notional,
        max_daily_loss: config.max_daily_loss,
        max_consecutive_losses: config.max_consecutive_losses,
        inventory_skew_limit: config.inventory_skew_limit,
        max_unrealized_loss: config.max_unrealized_loss,
    })
}

fn build_risk_snapshot(
    name: &str,
    position: &PositionSnapshot,
    _price: f64,
    risk_limits: Option<StrategyRiskLimits>,
) -> StrategySnapshot {
    let mut snapshot = StrategySnapshot::new(name.to_string());
    snapshot.exposure.net_inventory = position.net_base;
    snapshot.exposure.notional = position.net_notional;
    snapshot.exposure.long_position = position.long;
    snapshot.exposure.short_position = position.short;
    if let Some(limit) = risk_limits.as_ref().and_then(|l| l.max_inventory_notional) {
        if limit > 0.0 {
            snapshot.exposure.inventory_ratio =
                Some((position.net_notional.abs() / limit).min(10.0));
        }
    }
    snapshot.performance.timestamp = Utc::now();
    snapshot.performance.unrealized_pnl = position.net_notional - position.entry_notional;
    snapshot.risk_limits = risk_limits;
    snapshot
}

async fn apply_risk_decision(
    account_manager: &Arc<AccountManager>,
    symbol: &str,
    account_id: &str,
    decision: RiskDecision,
) -> Option<f64> {
    match decision.action {
        RiskAction::None => Some(1.0),
        RiskAction::Notify { level, message } => {
            match level {
                RiskNotifyLevel::Info => info!("ℹ️ 风险提示: {}", message),
                RiskNotifyLevel::Warning => warn!("⚠️ 风险警告: {}", message),
                RiskNotifyLevel::Danger => warn!("❗ 风险危险: {}", message),
            }
            Some(1.0)
        }
        RiskAction::ScaleDown {
            scale_factor,
            reason,
        } => {
            warn!("⚠️ 触发缩量: {} (scale={:.2})", reason, scale_factor);
            Some(scale_factor.clamp(0.0, 1.0))
        }
        RiskAction::Halt { reason } => {
            warn!("⏹️ 风险停机: {}", reason);
            if let Some(account) = account_manager.get_account(account_id) {
                let _ = account
                    .exchange
                    .cancel_all_orders(Some(symbol), MarketType::Futures)
                    .await;
            }
            None
        }
    }
}

fn side_scale(net_notional: f64, limit: Option<f64>, side: OrderSide) -> (bool, f64) {
    let Some(limit) = limit else {
        return (true, 1.0);
    };

    if limit <= 0.0 {
        return (true, 1.0);
    }

    match side {
        OrderSide::Buy => {
            if net_notional >= limit {
                (false, 0.0)
            } else {
                let remain = (limit - net_notional).max(0.0);
                (true, (remain / limit).clamp(0.0, 1.0))
            }
        }
        OrderSide::Sell => {
            if net_notional <= -limit {
                (false, 0.0)
            } else {
                let remain = (limit + net_notional).max(0.0);
                (true, (remain / limit).clamp(0.0, 1.0))
            }
        }
    }
}

fn orderbook_imbalance(depth: &DepthSnapshot, levels: usize) -> f64 {
    if depth.bids.is_empty() || depth.asks.is_empty() {
        return 0.0;
    }

    let bid_notional: f64 = depth.bids.iter().take(levels).map(|l| l[0] * l[1]).sum();
    let ask_notional: f64 = depth.asks.iter().take(levels).map(|l| l[0] * l[1]).sum();

    let denom = (bid_notional + ask_notional).max(1e-9);
    (bid_notional - ask_notional) / denom
}

fn order_flow_imbalance(
    current: &DepthSnapshot,
    prev: Option<&DepthSnapshot>,
    levels: usize,
) -> f64 {
    let Some(prev_depth) = prev else {
        return 0.0;
    };

    let mut delta_bid = 0.0;
    let mut delta_ask = 0.0;

    for (idx, level) in current.bids.iter().take(levels).enumerate() {
        let prev_qty = prev_depth.bids.get(idx).map(|l| l[1]).unwrap_or(0.0);
        delta_bid += level[0] * (level[1] - prev_qty);
    }

    for (idx, level) in current.asks.iter().take(levels).enumerate() {
        let prev_qty = prev_depth.asks.get(idx).map(|l| l[1]).unwrap_or(0.0);
        delta_ask += level[0] * (level[1] - prev_qty);
    }

    let magnitude = (delta_bid.abs() + delta_ask.abs()).max(1e-9);
    (delta_bid - delta_ask) / magnitude
}

fn compute_unrealized(position: &PositionSnapshot, mark: f64) -> Option<(f64, f64)> {
    if position.net_base.abs() < 1e-9 {
        return None;
    }
    let avg_entry = if position.entry_notional.abs() > 0.0 {
        position.entry_notional / position.net_base
    } else {
        position.net_notional / position.net_base
    };
    let pnl = position.net_base * (mark - avg_entry);
    Some((avg_entry, pnl))
}

fn build_stream_symbol(symbol: &str) -> String {
    let parts: Vec<&str> = symbol.split('/').collect();
    let base = parts.get(0).cloned().unwrap_or("BTC").to_lowercase();
    let mut quote = parts.get(1).cloned().unwrap_or("USDT").to_lowercase();
    if quote == "usdc" || quote == "busd" {
        quote = "usdt".to_string();
    }
    format!("{}{}", base, quote)
}

fn parse_levels_from_value(value: &Value) -> Vec<[f64; 2]> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|entry| entry.as_array())
                .filter_map(|pair| {
                    if pair.len() < 2 {
                        return None;
                    }
                    let price = pair.get(0)?.as_str()?.parse::<f64>().ok()?;
                    let qty = pair.get(1)?.as_str()?.parse::<f64>().ok()?;
                    Some([price, qty])
                })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_binance_depth(val: &Value) -> Option<DepthUpdate> {
    let bids = val
        .get("b")
        .map(parse_levels_from_value)
        .unwrap_or_default();
    let asks = val
        .get("a")
        .map(parse_levels_from_value)
        .unwrap_or_default();
    if bids.is_empty() && asks.is_empty() {
        None
    } else {
        Some(DepthUpdate {
            bids,
            asks,
            first_update_id: val.get("U").and_then(|v| v.as_u64()),
            final_update_id: val.get("u").and_then(|v| v.as_u64()),
            last_update_id: val.get("pu").and_then(|v| v.as_u64()),
        })
    }
}

fn apply_depth_update(snapshot: &mut DepthSnapshot, update: DepthUpdate) {
    fn apply_side(levels: &mut Vec<[f64; 2]>, updates: &[[f64; 2]], is_bid: bool) {
        for u in updates {
            let price = u[0];
            let qty = u[1];
            if let Some(pos) = levels.iter().position(|l| (l[0] - price).abs() < 1e-10) {
                if qty <= 0.0 {
                    levels.remove(pos);
                } else {
                    levels[pos][1] = qty;
                }
            } else if qty > 0.0 {
                levels.push([price, qty]);
            }
        }
        if is_bid {
            levels.sort_by(|a, b| b[0].partial_cmp(&a[0]).unwrap());
        } else {
            levels.sort_by(|a, b| a[0].partial_cmp(&b[0]).unwrap());
        }
        if levels.len() > 40 {
            levels.truncate(40);
        }
    }

    apply_side(&mut snapshot.bids, &update.bids, true);
    apply_side(&mut snapshot.asks, &update.asks, false);
}

fn should_apply_update(last: u64, update: &DepthUpdate) -> bool {
    let final_id = update.final_update_id.unwrap_or(0);
    if final_id == 0 {
        return false;
    }
    if last == 0 {
        return true;
    }
    if final_id <= last {
        return false;
    }
    if let Some(first) = update.first_update_id {
        if first > last + 1 {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imbalance_balanced_book_is_zero() {
        let depth = DepthSnapshot {
            bids: vec![[100.0, 10.0], [99.5, 5.0]],
            asks: vec![[100.5, 10.0], [101.0, 5.0]],
        };
        let val = orderbook_imbalance(&depth, 2);
        assert!(val.abs() < 0.01);
    }

    #[test]
    fn ofi_detects_bid_add() {
        let prev = DepthSnapshot {
            bids: vec![[100.0, 5.0], [99.5, 5.0]],
            asks: vec![[100.5, 5.0], [101.0, 5.0]],
        };
        let curr = DepthSnapshot {
            bids: vec![[100.0, 10.0], [99.5, 5.0]],
            asks: vec![[100.5, 5.0], [101.0, 5.0]],
        };
        let ofi = order_flow_imbalance(&curr, Some(&prev), 2);
        assert!(ofi > 0.0);
    }

    #[test]
    fn side_scale_respects_limit() {
        let (allow_buy, scale) = side_scale(90.0, Some(100.0), OrderSide::Buy);
        assert!(allow_buy);
        assert!(scale < 1.0 && scale > 0.0);

        let (allow_buy_full, scale_full) = side_scale(120.0, Some(100.0), OrderSide::Buy);
        assert!(!allow_buy_full);
        assert_eq!(scale_full, 0.0);
    }
}
