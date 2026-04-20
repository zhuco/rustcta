use super::config::{
    BaseAccountConfig, ExecutionConfig, HedgedGridConfig, PrecisionConfig, SpacingMode,
    SymbolConfig, SymbolDirection,
};

use crate::core::websocket::WebSocketClient;
use crate::core::{
    error::ExchangeError,
    types::{
        BatchOrderRequest, MarketType, Order, OrderRequest, OrderSide, OrderStatus, OrderType,
        WsMessage,
    },
};
use crate::cta::account_manager::AccountManager;
use crate::exchanges::binance::BinanceWebSocketClient;
use crate::utils::webhook::{notify_event, MessageLevel};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Mutex,
};
use tokio::time::{sleep, Duration, MissedTickBehavior};

const MAX_BATCH_ORDERS: usize = 5;
const LIMIT_ALERT_INTERVAL_SECS: i64 = 60;

static LIMIT_ALERT_TRACKER: OnceLock<Arc<Mutex<HashMap<String, DateTime<Utc>>>>> = OnceLock::new();
static CLIENT_ID_SEQ: OnceLock<AtomicU64> = OnceLock::new();

/// 对冲网格策略实例
pub struct HedgedGridStrategy {
    config: HedgedGridConfig,
    account_manager: Arc<AccountManager>,
    running: Arc<AtomicBool>,
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl HedgedGridStrategy {
    pub fn new(config: HedgedGridConfig, account_manager: Arc<AccountManager>) -> Self {
        Self {
            config,
            account_manager,
            running: Arc::new(AtomicBool::new(false)),
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start(&self) -> Result<()> {
        self.config
            .validate()
            .map_err(|err| anyhow!("配置校验失败: {}", err))?;

        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let execution = self.config.execution.clone();
        let base_account = self.config.base_account.clone();
        let max_leverage = self.config.risk_control.max_leverage.max(0.0);
        let running = self.running.clone();
        let account_manager = self.account_manager.clone();

        if execution.startup_cancel_all {
            let market_type = market_type_from_str(&base_account.market_type);
            for symbol_cfg in self.config.symbols.iter().filter(|cfg| cfg.enabled) {
                let account_id = symbol_cfg
                    .account_id
                    .clone()
                    .unwrap_or_else(|| base_account.account_id.clone());
                if let Some(account) = self.account_manager.get_account(&account_id) {
                    if let Err(err) = account
                        .exchange
                        .cancel_all_orders(Some(&symbol_cfg.symbol), market_type)
                        .await
                    {
                        log::warn!(
                            "[hedged_grid] {} 启动前清空挂单失败: {}",
                            symbol_cfg.config_id,
                            err
                        );
                    } else {
                        log::info!(
                            "[hedged_grid] {} 启动前已清空交易所挂单",
                            symbol_cfg.config_id
                        );
                    }
                } else {
                    log::warn!("[hedged_grid] 启动前清单失败，账户 {} 不存在", account_id);
                }
            }
        }

        let mut guard = self.handles.lock().await;
        guard.clear();

        for symbol_cfg in self
            .config
            .symbols
            .iter()
            .filter(|cfg| cfg.enabled)
            .cloned()
        {
            let account_manager = account_manager.clone();
            let running = running.clone();
            let execution = execution.clone();
            let base_account = base_account.clone();
            let strategy_name = self.config.strategy.name.clone();

            let handle = tokio::spawn(async move {
                if let Err(err) = run_symbol_loop(
                    strategy_name,
                    base_account,
                    execution,
                    max_leverage,
                    symbol_cfg,
                    account_manager,
                    running,
                )
                .await
                {
                    log::error!("[hedged_grid] 符号任务异常: {}", err);
                }
            });

            guard.push(handle);
        }

        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        // 终止任务
        let mut guard = self.handles.lock().await;
        for handle in guard.drain(..) {
            handle.abort();
        }

        if self.config.execution.shutdown_cancel_all {
            for symbol_cfg in self.config.symbols.iter().filter(|cfg| cfg.enabled) {
                let account_id = symbol_cfg
                    .account_id
                    .clone()
                    .unwrap_or_else(|| self.config.base_account.account_id.clone());
                if let Some(account) = self.account_manager.get_account(&account_id) {
                    if let Err(err) = account
                        .exchange
                        .cancel_all_orders(
                            Some(&symbol_cfg.symbol),
                            market_type_from_str(&self.config.base_account.market_type),
                        )
                        .await
                    {
                        match err {
                            ExchangeError::ApiError { code, ref message }
                                if code == 400 && message.contains("\"code\":-2011") =>
                            {
                                log::debug!(
                                    "[hedged_grid] {} 停止时无挂单需要清理 (exchange code -2011)",
                                    symbol_cfg.config_id
                                );
                            }
                            other => {
                                log::warn!(
                                    "[hedged_grid] {} 停止时取消挂单失败: {}",
                                    symbol_cfg.config_id,
                                    other
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[derive(Default)]
struct SymbolState {
    initialized: bool,
    last_center_price: f64,
    last_maintain_at: Option<DateTime<Utc>>,
    shortage_active: bool,
    last_shortage_anchor: Option<f64>,
    last_shortage_reanchor: Option<DateTime<Utc>>,
    inventory_mode: InventoryMode,
    last_inventory_check: Option<DateTime<Utc>>,
    last_balance_check: Option<DateTime<Utc>>,
    last_account_equity: Option<f64>,
    last_equity_at: Option<DateTime<Utc>>,
}

#[derive(Clone)]
struct FillEvent {
    side: OrderSide,
    price: f64,
    amount: f64,
    is_maker: bool,
    timestamp: DateTime<Utc>,
}

#[derive(Clone)]
enum SymbolEvent {
    Rebuild(&'static str),
    Fill(FillEvent),
}

#[derive(Clone, Copy)]
enum FarthestSelector {
    Highest,
    Lowest,
}

#[derive(Clone, Copy, PartialEq, Default, Debug)]
enum InventoryMode {
    #[default]
    Balanced,
    Shortage(OrderSide),
}

async fn run_symbol_loop(
    strategy_name: String,
    base_account: BaseAccountConfig,
    execution: ExecutionConfig,
    max_leverage: f64,
    symbol_cfg: SymbolConfig,
    account_manager: Arc<AccountManager>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let market_type = market_type_from_str(&base_account.market_type);
    let account_id = symbol_cfg
        .account_id
        .clone()
        .unwrap_or_else(|| base_account.account_id.clone());

    let state = Arc::new(Mutex::new(SymbolState::default()));
    let (event_tx, mut event_rx) = unbounded_channel::<SymbolEvent>();

    spawn_fill_listener(
        strategy_name.clone(),
        base_account.clone(),
        execution.clone(),
        symbol_cfg.clone(),
        account_manager.clone(),
        running.clone(),
        event_tx,
    );

    let interval_secs = if execution.rebalance_interval_secs > 0 {
        execution.rebalance_interval_secs
    } else {
        symbol_cfg.grid.refresh_interval_secs.max(10)
    };
    let base_interval = Duration::from_secs(interval_secs);
    let mut ticker = tokio::time::interval(base_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);

    if let Err(err) = maintain_symbol(
        &strategy_name,
        &account_id,
        market_type,
        &symbol_cfg,
        &execution,
        max_leverage,
        &account_manager,
        &state,
        true,
    )
    .await
    {
        log::error!(
            "[hedged_grid] {} 启动时初始化失败: {}",
            symbol_cfg.config_id,
            err
        );
    }

    while running.load(Ordering::SeqCst) {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(err) = maintain_symbol(
                    &strategy_name,
                    &account_id,
                    market_type,
                    &symbol_cfg,
                    &execution,
                    max_leverage,
                    &account_manager,
                    &state,
                    false,
                ).await {
                    log::error!("[hedged_grid] {} 定时维护失败: {}", symbol_cfg.config_id, err);
                }
            }
            Some(event) = event_rx.recv() => {
                match event {
                    SymbolEvent::Fill(fill) => {
                        if let Err(err) = handle_fill_event(
                            &strategy_name,
                            &account_id,
                            market_type,
                            &symbol_cfg,
                            max_leverage,
                            &account_manager,
                            &state,
                            fill,
                        ).await {
                            log::error!(
                                "[hedged_grid] {} 成交补单处理失败: {}",
                                symbol_cfg.config_id,
                                err
                            );
                        }
                    }
                    SymbolEvent::Rebuild(reason) => {
                        log::debug!(
                            "[hedged_grid] {} 收到事件触发重建: {}",
                            symbol_cfg.config_id,
                            reason
                        );
                        if let Err(err) = maintain_symbol(
                            &strategy_name,
                            &account_id,
                            market_type,
                            &symbol_cfg,
                            &execution,
                            max_leverage,
                            &account_manager,
                            &state,
                            false,
                        ).await {
                            log::error!(
                                "[hedged_grid] {} 事件驱动维护失败: {}",
                                symbol_cfg.config_id,
                                err
                            );
                        }
                    }
                }
            }
            else => break,
        }
    }

    Ok(())
}

fn spawn_fill_listener(
    strategy_name: String,
    base_account: BaseAccountConfig,
    execution: ExecutionConfig,
    symbol_cfg: SymbolConfig,
    account_manager: Arc<AccountManager>,
    running: Arc<AtomicBool>,
    event_tx: UnboundedSender<SymbolEvent>,
) {
    tokio::spawn(async move {
        let market_type = market_type_from_str(&base_account.market_type);
        let account_id = symbol_cfg
            .account_id
            .clone()
            .unwrap_or_else(|| base_account.account_id.clone());

        let Some(account) = account_manager.get_account(&account_id) else {
            log::warn!(
                "[hedged_grid] {} 启动成交监听失败: 账户 {} 不存在",
                symbol_cfg.config_id,
                account_id
            );
            return;
        };

        if account.exchange_name.to_lowercase() != "binance" {
            log::info!(
                "[hedged_grid] {} 当前交易所 {} 暂未实现成交监听，跳过 WebSocket",
                symbol_cfg.config_id,
                account.exchange_name
            );
            return;
        }

        let exchange = account.exchange.clone();
        let listen_key = match exchange.create_user_data_stream(market_type).await {
            Ok(key) => key,
            Err(err) => {
                log::error!(
                    "[hedged_grid] {} 创建用户数据流失败: {}",
                    symbol_cfg.config_id,
                    err
                );
                return;
            }
        };

        let ws_url = match market_type {
            MarketType::Spot => format!("wss://stream.binance.com:9443/ws/{}", listen_key),
            _ => format!("wss://fstream.binance.com/ws/{}", listen_key),
        };

        let mut ws_client = BinanceWebSocketClient::new(ws_url, market_type);
        if let Err(err) = ws_client.connect().await {
            log::error!(
                "[hedged_grid] {} WebSocket连接失败: {}",
                symbol_cfg.config_id,
                err
            );
            return;
        }

        let listen_key_for_keepalive = listen_key.clone();
        let keepalive_account_id = account_id.clone();
        let keepalive_manager = account_manager.clone();
        let keepalive_running = running.clone();
        let keepalive_config_id = symbol_cfg.config_id.clone();
        tokio::spawn(async move {
            while keepalive_running.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_secs(1500)).await;
                let Some(account) = keepalive_manager.get_account(&keepalive_account_id) else {
                    log::warn!(
                        "[hedged_grid] {} 保活失败: 账户 {} 不存在",
                        keepalive_config_id,
                        keepalive_account_id
                    );
                    break;
                };

                if let Err(err) = account
                    .exchange
                    .keepalive_user_data_stream(&listen_key_for_keepalive, market_type)
                    .await
                {
                    log::warn!(
                        "[hedged_grid] {} ListenKey保活失败: {}",
                        keepalive_config_id,
                        err
                    );
                } else {
                    log::debug!("[hedged_grid] {} ListenKey保活成功", keepalive_config_id);
                }
            }
        });

        log::info!(
            "[hedged_grid] {} WebSocket监听已启动 (策略 {})",
            symbol_cfg.config_id,
            strategy_name
        );

        let normalized_symbol = normalize_symbol(&symbol_cfg.symbol);
        let taker_delay_secs = execution.taker_reset_delay_secs;

        while running.load(Ordering::SeqCst) {
            match ws_client.receive().await {
                Ok(Some(message)) => match ws_client.parse_binance_message(&message) {
                    Ok(ws_message) => {
                        let mut skip_further = false;
                        if let WsMessage::ExecutionReport(report) = &ws_message {
                            let report_symbol = normalize_symbol(&report.symbol);
                            if report_symbol == normalized_symbol
                                && matches!(
                                    report.status,
                                    OrderStatus::Closed
                                        | OrderStatus::PartiallyFilled
                                        | OrderStatus::Triggered
                                )
                                && report.executed_amount > 0.0
                            {
                                let fill_price = if report.executed_price > 0.0 {
                                    report.executed_price
                                } else {
                                    report.price
                                };

                                if fill_price > 0.0 {
                                    if !report.is_maker
                                        || matches!(report.order_type, OrderType::Market)
                                    {
                                        let tx_clone = event_tx.clone();
                                        let running_clone = running.clone();
                                        let config_id = symbol_cfg.config_id.clone();
                                        let reason = "taker_reset";
                                        log::info!(
                                            "[hedged_grid] {} 检测到吃单/市价成交，{}秒后重建网格",
                                            symbol_cfg.config_id,
                                            taker_delay_secs
                                        );
                                        tokio::spawn(async move {
                                            if taker_delay_secs > 0 {
                                                tokio::time::sleep(Duration::from_secs(
                                                    taker_delay_secs,
                                                ))
                                                .await;
                                            }
                                            if running_clone.load(Ordering::SeqCst) {
                                                if tx_clone
                                                    .send(SymbolEvent::Rebuild(reason))
                                                    .is_err()
                                                {
                                                    log::warn!(
                                                        "[hedged_grid] {} taker重建事件发送失败",
                                                        config_id
                                                    );
                                                }
                                            }
                                        });
                                    } else if report.status == OrderStatus::Closed {
                                        let fill_event = FillEvent {
                                            side: report.side,
                                            price: fill_price,
                                            amount: report.executed_amount,
                                            is_maker: report.is_maker,
                                            timestamp: report.timestamp,
                                        };
                                        if event_tx.send(SymbolEvent::Fill(fill_event)).is_err() {
                                            log::warn!(
                                                "[hedged_grid] {} 成交事件通道已关闭，停止监听",
                                                symbol_cfg.config_id
                                            );
                                            break;
                                        }
                                    } else {
                                        log::debug!(
                                            "[hedged_grid] {} 收到部分成交，暂不补单 status={:?} amount={:.6}",
                                            symbol_cfg.config_id,
                                            report.status,
                                            report.executed_amount
                                        );
                                    }
                                    skip_further = true;
                                }
                            }
                        }

                        if skip_further {
                            continue;
                        }

                        if let Some(reason) =
                            should_trigger_rebuild(&ws_message, &normalized_symbol)
                        {
                            if event_tx.send(SymbolEvent::Rebuild(reason)).is_err() {
                                log::warn!(
                                    "[hedged_grid] {} 事件通道已关闭，停止监听",
                                    symbol_cfg.config_id
                                );
                                break;
                            }
                        }
                    }
                    Err(err) => {
                        log::debug!(
                            "[hedged_grid] {} WebSocket消息解析失败: {}",
                            symbol_cfg.config_id,
                            err
                        );
                    }
                },
                Ok(None) => {
                    sleep(Duration::from_millis(100)).await;
                }
                Err(err) => {
                    log::error!(
                        "[hedged_grid] {} WebSocket接收错误: {}",
                        symbol_cfg.config_id,
                        err
                    );
                    break;
                }
            }
        }

        log::info!("[hedged_grid] {} WebSocket监听停止", symbol_cfg.config_id);
    });
}

fn should_trigger_rebuild(message: &WsMessage, normalized_symbol: &str) -> Option<&'static str> {
    match message {
        WsMessage::ExecutionReport(report) => {
            let report_symbol = normalize_symbol(&report.symbol);
            if report_symbol == normalized_symbol
                && matches!(
                    report.status,
                    OrderStatus::Closed | OrderStatus::PartiallyFilled | OrderStatus::Triggered
                )
            {
                return Some("execution_report");
            }
        }
        WsMessage::Order(order) => {
            let order_symbol = normalize_symbol(&order.symbol);
            if order_symbol == normalized_symbol
                && matches!(
                    order.status,
                    OrderStatus::Closed | OrderStatus::PartiallyFilled | OrderStatus::Canceled
                )
            {
                return Some("order_update");
            }
        }
        _ => {}
    }
    None
}

async fn maintain_symbol(
    strategy_name: &str,
    account_id: &str,
    market_type: MarketType,
    symbol_cfg: &SymbolConfig,
    execution: &ExecutionConfig,
    max_leverage: f64,
    account_manager: &Arc<AccountManager>,
    state: &Arc<Mutex<SymbolState>>,
    is_initial: bool,
) -> Result<()> {
    let account = account_manager
        .get_account(account_id)
        .ok_or_else(|| anyhow!("账户 {} 不存在", account_id))?;

    if is_initial && execution.startup_cancel_all {
        if let Err(err) = account
            .exchange
            .cancel_all_orders(Some(&symbol_cfg.symbol), market_type)
            .await
        {
            match err {
                ExchangeError::ApiError { code, ref message }
                    if code == 400 && message.contains("\"code\":-2011") =>
                {
                    log::debug!(
                        "[hedged_grid] {} 启动时无挂单需要清理 (exchange code -2011)",
                        symbol_cfg.config_id,
                    );
                }
                other => {
                    log::warn!(
                        "[hedged_grid] {} 启动时取消挂单失败: {}",
                        symbol_cfg.config_id,
                        other
                    );
                }
            }
        }
    }

    let ticker = account
        .exchange
        .get_ticker(&symbol_cfg.symbol, market_type)
        .await
        .with_context(|| format!("获取 {} 行情失败", symbol_cfg.symbol))?;

    let center_price = if ticker.last > 0.0 {
        ticker.last
    } else {
        (ticker.ask + ticker.bid) / 2.0
    };

    if center_price <= 0.0 {
        return Err(anyhow!(
            "{} 行情价格异常: {:.6}",
            symbol_cfg.symbol,
            center_price
        ));
    }

    let now = Utc::now();
    let inventory = fetch_inventory_snapshot(&account, &symbol_cfg.symbol, market_type).await?;
    let net_position = inventory.net_position;
    let account_equity =
        fetch_account_total_equity(&account, market_type, &symbol_cfg.config_id).await;
    let (order_control, inventory_status) = compute_order_control(
        symbol_cfg,
        center_price,
        &inventory,
        market_type,
        account_equity,
        max_leverage,
    );
    let (min_inventory, _, _) = min_inventory_info(symbol_cfg, &symbol_cfg.precision, center_price);
    let position_limit = position_limit_base(symbol_cfg, &symbol_cfg.precision, center_price);
    let leverage_notional_limit = equity_notional_limit(account_equity, max_leverage);
    let leverage_bypass =
        within_equity_notional_allowance(net_position, center_price, account_equity, max_leverage);

    log::info!(
        "[hedged_grid] {} 控制: 净持仓 {:.6}, 最小库存 {:.6}, 仓位上限 {:.6}, 账户权益 {:.2}, 权益上限名义 {:.2}, 权益豁免={}, 允许买单={}, 允许卖单={}, 买单reduce_only={}, 卖单reduce_only={}",
        symbol_cfg.config_id,
        net_position,
        min_inventory,
        position_limit,
        account_equity.unwrap_or(0.0),
        leverage_notional_limit.unwrap_or(0.0),
        leverage_bypass,
        order_control.allow_buys,
        order_control.allow_sells,
        order_control.buy_reduce_only,
        order_control.sell_reduce_only
    );

    let quote_notional = net_position * center_price;
    let shortage_active = inventory_status.shortage_side.is_some();
    let mode = if let Some(side) = inventory_status.shortage_side {
        InventoryMode::Shortage(side)
    } else {
        InventoryMode::Balanced
    };

    let base_free_display = inventory.spot_base_free.unwrap_or(net_position);
    let quote_free_display = inventory.spot_quote_free.unwrap_or(quote_notional);

    log::info!(
        "[hedged_grid] {} 库存快照: base {:.6}, base_free {:.6}, quote_free {:.2}, 中心价 {:.6}, 模式 {:?}",
        symbol_cfg.config_id,
        net_position,
        base_free_display,
        quote_free_display,
        center_price,
        mode
    );
    let (
        effective_center,
        reused_anchor,
        diff_pct,
        prev_shortage,
        prev_mode,
        was_initialized,
        last_balance_check,
    ) = {
        let mut guard = state.lock().await;
        let prev_shortage = guard.shortage_active;
        let previous_mode = guard.inventory_mode;
        let was_initialized = guard.initialized;
        let last_balance_check = guard.last_balance_check;
        let mut center_to_use = center_price;
        let mut reused = false;
        let mut diff = 0.0;
        if shortage_active && prev_shortage {
            if let Some(anchor) = guard.last_shortage_anchor {
                if anchor.abs() > f64::EPSILON {
                    diff = (center_price - anchor).abs() / anchor.abs();
                }
                let elapsed = guard
                    .last_shortage_reanchor
                    .map(|t| (now - t).num_seconds())
                    .unwrap_or(i64::MAX);
                if diff <= 0.0005 && elapsed < 30 && !is_initial {
                    center_to_use = anchor;
                    reused = true;
                }
            }
        }

        guard.shortage_active = shortage_active;
        guard.last_center_price = center_price;
        guard.last_maintain_at = Some(now);
        if guard.inventory_mode != mode {
            guard.inventory_mode = mode;
        }
        guard.last_inventory_check = Some(now);

        (
            center_to_use,
            reused,
            diff,
            prev_shortage,
            previous_mode,
            was_initialized,
            last_balance_check,
        )
    };

    if prev_mode != mode {
        match mode {
            InventoryMode::Balanced => {
                log::info!(
                    "[hedged_grid] {} 库存恢复至双边挂单模式，当前净持仓 {:.6}",
                    symbol_cfg.config_id,
                    net_position
                );
            }
            InventoryMode::Shortage(side) => {
                let side_label = match side {
                    OrderSide::Buy => "买单",
                    OrderSide::Sell => "卖单",
                };
                log::info!(
                    "[hedged_grid] {} 库存不足，切换为单侧补仓模式（仅保留{}）",
                    symbol_cfg.config_id,
                    side_label
                );
            }
        }
    }

    if let InventoryMode::Balanced = mode {
        if shortage_active && should_notify_limit_breach(&symbol_cfg.config_id).await {
            // existing limit logic already handles warning; nothing extra here
        }
    }

    let mut balance_checked = false;
    let mut force_reset = false;
    let should_check_balance = last_balance_check
        .map(|ts| now.signed_duration_since(ts) >= ChronoDuration::seconds(180))
        .unwrap_or(true);

    if should_check_balance
        && matches!(mode, InventoryMode::Balanced)
        && order_control.allow_buys
        && order_control.allow_sells
    {
        balance_checked = true;
        match account
            .exchange
            .get_open_orders(Some(&symbol_cfg.symbol), market_type)
            .await
        {
            Ok(orders) => {
                let mut buy_count = 0usize;
                let mut sell_count = 0usize;
                for order in orders.into_iter() {
                    if order.order_type != OrderType::Limit {
                        continue;
                    }
                    match order.side {
                        OrderSide::Buy => buy_count += 1,
                        OrderSide::Sell => sell_count += 1,
                    }
                }
                if buy_count != sell_count {
                    force_reset = true;
                    log::warn!(
                        "[hedged_grid] {} 双边挂单数量不一致，BUY={} / SELL={}，准备重置网格",
                        symbol_cfg.config_id,
                        buy_count,
                        sell_count
                    );
                }
            }
            Err(err) => {
                log::warn!(
                    "[hedged_grid] {} 检查挂单数量失败: {}",
                    symbol_cfg.config_id,
                    err
                );
            }
        }
    }

    let mode_changed = prev_mode != mode;
    let mut should_refresh = is_initial
        || matches!(mode, InventoryMode::Shortage(_))
        || !was_initialized
        || mode_changed;

    if force_reset {
        should_refresh = true;
        match account
            .exchange
            .cancel_all_orders(Some(&symbol_cfg.symbol), market_type)
            .await
        {
            Ok(_) => {
                log::info!(
                    "[hedged_grid] {} 已因挂单不对称清空交易所挂单",
                    symbol_cfg.config_id
                );
            }
            Err(err) => {
                log::warn!(
                    "[hedged_grid] {} 重置网格时取消全部挂单失败: {}",
                    symbol_cfg.config_id,
                    err
                );
            }
        }
    }

    if inventory_status.limit_exceeded && should_notify_limit_breach(&symbol_cfg.config_id).await {
        let notional = net_position * center_price;
        let limit_notional = position_limit * center_price;
        let body = format!(
            "符号: {symbol}\n净持仓: {net:+.6} (≈ {notional:.2} USDC)\n仓位上限: {limit:.6} (≈ {limit_usd:.2} USDC)\n请及时人工核查并视情况减仓。",
            symbol = symbol_cfg.symbol,
            net = net_position,
            notional = notional,
            limit = position_limit,
            limit_usd = limit_notional,
        );
        let strategy_tag = format!("{}::{}", strategy_name, symbol_cfg.config_id);
        notify_event(&strategy_tag, "仓位超限告警", &body, MessageLevel::Warning).await;
    }

    if let Some(side) = inventory_status.shortage_side {
        let side_label = match side {
            OrderSide::Buy => "买单",
            OrderSide::Sell => "卖单",
        };
        log::info!(
            "[hedged_grid] {} 底仓偏离目标，当前净持仓 {:.6}，仅保留{}补仓订单",
            symbol_cfg.config_id,
            net_position,
            side_label
        );
        if reused_anchor {
            log::debug!(
                "[hedged_grid] {} 保持补仓锚点 {:.6}，价格偏移 {:.3}% (<0.2%)",
                symbol_cfg.config_id,
                effective_center,
                diff_pct * 100.0
            );
        } else if diff_pct > 0.0 && prev_shortage {
            log::info!(
                "[hedged_grid] {} 重新锚定补仓价位，偏移 {:.3}%",
                symbol_cfg.config_id,
                diff_pct * 100.0
            );
        }
    }

    let mut desired_orders = Vec::new();
    if should_refresh {
        desired_orders = build_grid_orders(symbol_cfg, effective_center, &order_control);

        sync_orders(
            strategy_name,
            symbol_cfg,
            &account,
            account_manager,
            account_id,
            market_type,
            &desired_orders,
        )
        .await?;
    } else {
        log::debug!(
            "[hedged_grid] {} 库存充足且已初始化，本轮跳过网格刷新",
            symbol_cfg.config_id
        );
    }

    {
        let mut guard = state.lock().await;
        if should_refresh {
            guard.initialized = true;
        }
        if balance_checked {
            guard.last_balance_check = Some(now);
        }
        if let Some(equity) = account_equity {
            guard.last_account_equity = Some(equity);
            guard.last_equity_at = Some(now);
        }
        if shortage_active {
            if !reused_anchor {
                guard.last_shortage_anchor = Some(center_price);
                guard.last_shortage_reanchor = Some(now);
            }
        } else {
            guard.shortage_active = false;
            guard.last_shortage_anchor = None;
            guard.last_shortage_reanchor = None;
        }
    }

    if should_refresh && desired_orders.is_empty() {
        log::debug!(
            "[hedged_grid] {} 当前无需要维护的挂单 (净持仓 {:.6})",
            symbol_cfg.config_id,
            net_position
        );
    }

    Ok(())
}

#[derive(Clone, Debug)]
struct OrderControl {
    allow_buys: bool,
    allow_sells: bool,
    buy_levels: u32,
    sell_levels: u32,
    buy_spacing: f64,
    sell_spacing: f64,
    buy_reduce_only: bool,
    sell_reduce_only: bool,
    spacing_mode: SpacingMode,
}

#[derive(Clone, Debug, Default)]
struct InventoryStatus {
    shortage_side: Option<OrderSide>,
    limit_exceeded: bool,
}

#[derive(Clone, Copy, Debug, Default)]
struct InventorySnapshot {
    net_position: f64,
    spot_base_free: Option<f64>,
    spot_quote_free: Option<f64>,
}

async fn fetch_inventory_snapshot(
    account: &Arc<crate::cta::account_manager::AccountInfo>,
    symbol: &str,
    market_type: MarketType,
) -> Result<InventorySnapshot> {
    match market_type {
        MarketType::Spot => {
            let balances = account
                .exchange
                .get_balance(MarketType::Spot)
                .await
                .with_context(|| format!("获取 {} 现货余额失败", symbol))?;
            let (base, quote) = split_symbol_pair(symbol);
            let mut base_total = 0.0;
            let mut base_free = 0.0;
            let mut quote_free = 0.0;
            for balance in balances.into_iter() {
                if balance.currency.eq_ignore_ascii_case(base.as_str()) {
                    base_total = balance.total;
                    base_free = balance.free;
                } else if balance.currency.eq_ignore_ascii_case(quote.as_str()) {
                    quote_free = balance.free;
                }
            }
            log::debug!(
                "[hedged_grid] spot position snapshot {} base={} total={:.6} free={:.6} quote_free={:.6}",
                symbol,
                base,
                base_total,
                base_free,
                quote_free
            );
            Ok(InventorySnapshot {
                net_position: base_total,
                spot_base_free: Some(base_free),
                spot_quote_free: Some(quote_free),
            })
        }
        _ => {
            let positions = account.exchange.get_positions(None).await?;
            let exchange_symbol = normalize_symbol(symbol);
            let mut net = 0.0;
            for pos in positions
                .into_iter()
                .filter(|p| normalize_symbol(&p.symbol) == exchange_symbol)
            {
                log::debug!(
                    "[hedged_grid] position snapshot {} side={} contracts={:.6} size={:.6} amount={:.6}",
                    pos.symbol,
                    pos.side,
                    pos.contracts,
                    pos.size,
                    pos.amount
                );
                let amount = pos.amount;
                if amount.abs() > 0.0 {
                    net += amount;
                    continue;
                }

                let raw_qty = if pos.contracts.abs() > 0.0 {
                    pos.contracts
                } else if pos.size.abs() > 0.0 {
                    pos.size
                } else {
                    0.0
                };
                if raw_qty.abs() < f64::EPSILON {
                    continue;
                }
                let side = pos.side.to_uppercase();
                let signed_qty = if side.contains("SHORT") || side.contains("SELL") {
                    -raw_qty.abs()
                } else if side.contains("LONG") || side.contains("BUY") {
                    raw_qty.abs()
                } else {
                    raw_qty
                };
                net += signed_qty;
            }
            Ok(InventorySnapshot {
                net_position: net,
                spot_base_free: None,
                spot_quote_free: None,
            })
        }
    }
}

async fn fetch_account_total_equity(
    account: &Arc<crate::cta::account_manager::AccountInfo>,
    market_type: MarketType,
    config_id: &str,
) -> Option<f64> {
    if market_type == MarketType::Spot {
        return None;
    }

    match account.exchange.get_balance(market_type).await {
        Ok(balances) => {
            let total_equity: f64 = balances
                .into_iter()
                .map(|balance| {
                    if balance.total.is_finite() {
                        balance.total.max(0.0)
                    } else {
                        0.0
                    }
                })
                .sum();
            if total_equity > 0.0 {
                Some(total_equity)
            } else {
                None
            }
        }
        Err(err) => {
            log::warn!(
                "[hedged_grid] {} 获取账户总权益失败，回退到固定仓位上限: {}",
                config_id,
                err
            );
            None
        }
    }
}

async fn cached_account_equity(state: &Arc<Mutex<SymbolState>>, now: DateTime<Utc>) -> Option<f64> {
    const EQUITY_CACHE_TTL_SECS: i64 = 180;

    let guard = state.lock().await;
    let Some(equity) = guard.last_account_equity else {
        return None;
    };
    let Some(updated_at) = guard.last_equity_at else {
        return None;
    };
    if now.signed_duration_since(updated_at) > ChronoDuration::seconds(EQUITY_CACHE_TTL_SECS) {
        return None;
    }
    Some(equity)
}

fn equity_notional_limit(account_equity: Option<f64>, max_leverage: f64) -> Option<f64> {
    if max_leverage <= 0.0 || !max_leverage.is_finite() {
        return None;
    }
    account_equity.and_then(|equity| {
        if equity > 0.0 && equity.is_finite() {
            Some(equity * max_leverage)
        } else {
            None
        }
    })
}

fn within_equity_notional_allowance(
    net_position: f64,
    center_price: f64,
    account_equity: Option<f64>,
    max_leverage: f64,
) -> bool {
    if center_price <= 0.0 || !center_price.is_finite() {
        return false;
    }
    let Some(limit_notional) = equity_notional_limit(account_equity, max_leverage) else {
        return false;
    };
    let current_notional = net_position.abs() * center_price;
    current_notional < limit_notional
}

fn compute_order_control(
    config: &SymbolConfig,
    center_price: f64,
    inventory: &InventorySnapshot,
    market_type: MarketType,
    account_equity: Option<f64>,
    max_leverage: f64,
) -> (OrderControl, InventoryStatus) {
    let precision = &config.precision;
    let spacing_mode = config.grid.spacing_mode();
    let net_position = inventory.net_position;
    let mut control = OrderControl {
        allow_buys: true,
        allow_sells: true,
        buy_levels: config.grid.levels_per_side,
        sell_levels: config.grid.levels_per_side,
        buy_spacing: config.grid.spacing,
        sell_spacing: config.grid.spacing,
        buy_reduce_only: false,
        sell_reduce_only: false,
        spacing_mode,
    };

    let mut status = InventoryStatus::default();

    let (min_inventory, tolerance, amount_step) =
        min_inventory_info(config, precision, center_price);
    let position_limit = position_limit_base(config, precision, center_price);
    let bypass_position_limit =
        within_equity_notional_allowance(net_position, center_price, account_equity, max_leverage);
    let shortage_levels = config.grid.levels_per_side.clamp(1, 2);

    match config.direction {
        SymbolDirection::Long => {
            let has_min_inventory = net_position >= (min_inventory - tolerance);

            if !has_min_inventory {
                control.allow_sells = false;
                control.buy_levels = shortage_levels;
                if status.shortage_side.is_none() {
                    status.shortage_side = Some(OrderSide::Buy);
                }
            }

            control.sell_reduce_only = false;

            if !bypass_position_limit && net_position >= position_limit - amount_step {
                status.limit_exceeded = true;
                control.allow_buys = false;
                control.buy_levels = 0;
                control.sell_levels = shortage_levels;
                control.sell_reduce_only = true;
                status.shortage_side = Some(OrderSide::Sell);
            }
        }
        SymbolDirection::Short => {
            let has_min_inventory = net_position <= -(min_inventory - tolerance);

            if !has_min_inventory {
                control.allow_buys = false;
                control.sell_levels = shortage_levels;
                if status.shortage_side.is_none() {
                    status.shortage_side = Some(OrderSide::Sell);
                }
            }

            control.buy_reduce_only = false;

            if !bypass_position_limit && net_position.abs() >= position_limit - amount_step {
                status.limit_exceeded = true;
                control.allow_sells = false;
                control.buy_reduce_only = true;
            }
        }
    }

    if market_type == MarketType::Spot {
        match config.direction {
            SymbolDirection::Long => {
                let amount_step = min_amount_increment(precision);
                let order_size = config.grid.order_size.max(amount_step);

                if order_size <= 0.0 {
                    control.allow_sells = false;
                    control.sell_levels = 0;
                } else {
                    let available_base = (net_position - min_inventory).max(0.0);
                    let max_levels = (available_base / order_size).floor() as u32;
                    if max_levels == 0 {
                        control.allow_sells = false;
                        control.sell_levels = 0;
                        if status.shortage_side.is_none() {
                            status.shortage_side = Some(OrderSide::Buy);
                        }
                    } else {
                        if max_levels < control.sell_levels {
                            if status.shortage_side.is_none() {
                                status.shortage_side = Some(OrderSide::Buy);
                            }
                        }
                        control.sell_levels = control.sell_levels.min(max_levels);
                    }
                }

                if let Some(quote_free) = inventory.spot_quote_free {
                    let order_notional = config.grid.order_size * center_price;
                    if order_notional > 0.0 {
                        let max_buy_levels = (quote_free / order_notional).floor() as u32;
                        if max_buy_levels < control.buy_levels {
                            if status.shortage_side.is_none() {
                                status.shortage_side = Some(OrderSide::Sell);
                            }
                        }
                        control.buy_levels = control.buy_levels.min(max_buy_levels);
                        if control.buy_levels == 0 {
                            control.allow_buys = false;
                        }
                    }
                }
            }
            SymbolDirection::Short => {
                control.allow_buys = false;
                control.buy_levels = 0;
            }
        }
    }

    if control.allow_buys && control.buy_levels == 0 {
        control.buy_levels = 1;
    }
    if control.allow_sells && control.sell_levels == 0 {
        control.sell_levels = 1;
    }

    control.buy_spacing =
        enforce_min_spacing(control.buy_spacing, precision, center_price, spacing_mode);
    control.sell_spacing =
        enforce_min_spacing(control.sell_spacing, precision, center_price, spacing_mode);

    (control, status)
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct OrderKey {
    side: char,
    price_tick: i64,
}

async fn should_notify_limit_breach(config_id: &str) -> bool {
    let tracker = LIMIT_ALERT_TRACKER
        .get_or_init(|| Arc::new(Mutex::new(HashMap::new())))
        .clone();

    let mut guard = tracker.lock().await;
    let now = Utc::now();
    if let Some(last_time) = guard.get(config_id) {
        if now.signed_duration_since(*last_time)
            < ChronoDuration::seconds(LIMIT_ALERT_INTERVAL_SECS)
        {
            return false;
        }
    }

    guard.insert(config_id.to_string(), now);
    true
}

async fn sync_orders(
    strategy_name: &str,
    config: &SymbolConfig,
    account: &Arc<crate::cta::account_manager::AccountInfo>,
    account_manager: &Arc<AccountManager>,
    account_id: &str,
    market_type: MarketType,
    desired_orders: &[GridOrder],
) -> Result<()> {
    let precision = &config.precision;

    let existing_orders = account
        .exchange
        .get_open_orders(Some(&config.symbol), market_type)
        .await
        .with_context(|| format!("获取 {} 挂单失败", config.symbol))?;

    let mut desired_map: HashMap<OrderKey, GridOrder> = HashMap::new();
    for order in desired_orders {
        if let Some(key) = OrderKey::new(&order.side, order.price, precision) {
            desired_map.insert(key, order.clone());
        }
    }

    let mut existing_map: HashMap<OrderKey, Order> = HashMap::new();
    let mut existing_by_id: HashMap<String, Order> = HashMap::new();
    for order in existing_orders.into_iter() {
        if order.order_type != OrderType::Limit {
            continue;
        }
        if let Some(key) = OrderKey::from_order(&order, precision) {
            existing_map.insert(key, order.clone());
        }
        existing_by_id.insert(order.id.clone(), order);
    }

    let mut cancel_ids: HashSet<String> = HashSet::new();
    let mut create_orders: Vec<GridOrder> = Vec::new();
    let qty_tolerance = quantity_tolerance(precision);

    for (key, desired) in desired_map.iter() {
        match existing_map.get(key) {
            Some(existing) => {
                let mut replace = false;
                if (existing.amount - desired.quantity).abs() > qty_tolerance {
                    replace = true;
                }

                let existing_reduce_only = order_is_reduce_only(existing);

                if desired.reduce_only != existing_reduce_only {
                    replace = true;
                }

                if replace {
                    cancel_ids.insert(existing.id.clone());
                    create_orders.push(desired.clone());
                }
            }
            None => {
                create_orders.push(desired.clone());
            }
        }
    }

    for (key, existing) in existing_map.iter() {
        if !desired_map.contains_key(key) {
            cancel_ids.insert(existing.id.clone());
        }
    }

    if !cancel_ids.is_empty() {
        let mut details = Vec::new();
        for order_id in cancel_ids.iter() {
            if let Some(existing) = existing_by_id.get(order_id) {
                let price = existing.price.unwrap_or(0.0);
                details.push(format!(
                    "{} {} @ {:.6} qty {:.6}",
                    order_id,
                    match existing.side {
                        OrderSide::Buy => "BUY",
                        OrderSide::Sell => "SELL",
                    },
                    price,
                    existing.amount
                ));
            } else {
                details.push(order_id.clone());
            }
        }
        log::info!(
            "[hedged_grid] {} 撤销 {} 笔挂单: {}",
            config.config_id,
            cancel_ids.len(),
            details.join("; ")
        );
        for order_id in cancel_ids.iter() {
            if let Err(err) = account
                .exchange
                .cancel_order(order_id, &config.symbol, market_type)
                .await
            {
                log::warn!(
                    "[hedged_grid] {} 撤销挂单 {} 失败: {}",
                    config.config_id,
                    order_id,
                    err
                );
            }
        }
    }

    if !create_orders.is_empty() {
        submit_orders(
            strategy_name,
            account_manager,
            account_id,
            market_type,
            config,
            &create_orders,
        )
        .await?;
    }

    Ok(())
}

async fn handle_fill_event(
    strategy_name: &str,
    account_id: &str,
    market_type: MarketType,
    symbol_cfg: &SymbolConfig,
    max_leverage: f64,
    account_manager: &Arc<AccountManager>,
    state: &Arc<Mutex<SymbolState>>,
    fill: FillEvent,
) -> Result<()> {
    if fill.price <= 0.0 || fill.amount <= 0.0 {
        log::debug!(
            "[hedged_grid] {} 忽略无效成交 price={:.6} amount={:.6}",
            symbol_cfg.config_id,
            fill.price,
            fill.amount
        );
        return Ok(());
    }

    let Some(account) = account_manager.get_account(account_id) else {
        return Err(anyhow!("账户 {} 不存在，无法处理成交补单", account_id));
    };

    let inventory = fetch_inventory_snapshot(&account, &symbol_cfg.symbol, market_type).await?;
    let cached_equity = cached_account_equity(state, fill.timestamp).await;
    let (order_control, _inventory_status) = compute_order_control(
        symbol_cfg,
        fill.price,
        &inventory,
        market_type,
        cached_equity,
        max_leverage,
    );

    let spacing_mode = order_control.spacing_mode;
    let mut replenishment_orders: Vec<GridOrder> = Vec::new();

    match fill.side {
        OrderSide::Buy => {
            if order_control.allow_sells {
                if let Some(price) = price_for_level(
                    fill.price,
                    order_control.sell_spacing,
                    spacing_mode,
                    1,
                    OrderSide::Sell,
                ) {
                    if let Some(order) = make_order(
                        price,
                        OrderSide::Sell,
                        order_control.sell_reduce_only,
                        symbol_cfg,
                    ) {
                        replenishment_orders.push(order);
                    }
                }
            }

            if order_control.allow_buys {
                let level_offset = order_control.buy_levels.max(1);
                if let Some(price) = price_for_level(
                    fill.price,
                    order_control.buy_spacing,
                    spacing_mode,
                    level_offset,
                    OrderSide::Buy,
                ) {
                    if let Some(order) = make_order(
                        price,
                        OrderSide::Buy,
                        order_control.buy_reduce_only,
                        symbol_cfg,
                    ) {
                        replenishment_orders.push(order);
                    }
                }
            }

            if !replenishment_orders.is_empty() {
                submit_orders(
                    strategy_name,
                    account_manager,
                    account_id,
                    market_type,
                    symbol_cfg,
                    &replenishment_orders,
                )
                .await?;
            }

            if order_control.allow_sells && order_control.sell_levels > 0 {
                cancel_farthest_order(
                    &account,
                    symbol_cfg,
                    market_type,
                    OrderSide::Sell,
                    FarthestSelector::Highest,
                    order_control.sell_levels,
                )
                .await?;
            }
        }
        OrderSide::Sell => {
            if order_control.allow_buys {
                if let Some(price) = price_for_level(
                    fill.price,
                    order_control.buy_spacing,
                    spacing_mode,
                    1,
                    OrderSide::Buy,
                ) {
                    if let Some(order) = make_order(
                        price,
                        OrderSide::Buy,
                        order_control.buy_reduce_only,
                        symbol_cfg,
                    ) {
                        replenishment_orders.push(order);
                    }
                }
            }

            if order_control.allow_sells {
                let level_offset = order_control.sell_levels.max(1);
                if let Some(price) = price_for_level(
                    fill.price,
                    order_control.sell_spacing,
                    spacing_mode,
                    level_offset,
                    OrderSide::Sell,
                ) {
                    if let Some(order) = make_order(
                        price,
                        OrderSide::Sell,
                        order_control.sell_reduce_only,
                        symbol_cfg,
                    ) {
                        replenishment_orders.push(order);
                    }
                }
            }

            if !replenishment_orders.is_empty() {
                submit_orders(
                    strategy_name,
                    account_manager,
                    account_id,
                    market_type,
                    symbol_cfg,
                    &replenishment_orders,
                )
                .await?;
            }

            if order_control.allow_buys && order_control.buy_levels > 0 {
                cancel_farthest_order(
                    &account,
                    symbol_cfg,
                    market_type,
                    OrderSide::Buy,
                    FarthestSelector::Lowest,
                    order_control.buy_levels,
                )
                .await?;
            }
        }
    }

    {
        let mut guard = state.lock().await;
        guard.initialized = true;
        guard.last_center_price = fill.price;
        guard.last_maintain_at = Some(fill.timestamp);
    }

    log::debug!(
        "[hedged_grid] {} maker成交处理完成: {:?} {:.6}@{:.6} 提交{}笔补单",
        symbol_cfg.config_id,
        fill.side,
        fill.amount,
        fill.price,
        replenishment_orders.len()
    );

    Ok(())
}

async fn cancel_farthest_order(
    account: &Arc<crate::cta::account_manager::AccountInfo>,
    symbol_cfg: &SymbolConfig,
    market_type: MarketType,
    side: OrderSide,
    selector: FarthestSelector,
    desired_levels: u32,
) -> Result<()> {
    if desired_levels == 0 {
        return Ok(());
    }

    let orders = account
        .exchange
        .get_open_orders(Some(&symbol_cfg.symbol), market_type)
        .await
        .with_context(|| format!("获取 {} 当前挂单失败", symbol_cfg.symbol))?;

    let mut same_side: Vec<Order> = orders
        .into_iter()
        .filter(|order| order.order_type == OrderType::Limit && order.side == side)
        .filter(|order| order.price.unwrap_or(0.0) > 0.0)
        .collect();

    if same_side.len() as u32 <= desired_levels {
        return Ok(());
    }

    same_side.sort_by(|a, b| {
        let ap = a.price.unwrap_or(0.0);
        let bp = b.price.unwrap_or(0.0);
        ap.partial_cmp(&bp).unwrap_or(std::cmp::Ordering::Equal)
    });

    let candidate = match selector {
        FarthestSelector::Highest => same_side.pop(),
        FarthestSelector::Lowest => {
            if same_side.is_empty() {
                None
            } else {
                Some(same_side.remove(0))
            }
        }
    };

    let Some(order) = candidate else {
        return Ok(());
    };

    let price = order.price.unwrap_or(0.0);
    account
        .exchange
        .cancel_order(&order.id, &symbol_cfg.symbol, market_type)
        .await
        .with_context(|| {
            format!(
                "{} 撤销最远挂单失败: {} {:?} @ {:.6}",
                symbol_cfg.config_id, order.id, side, price
            )
        })?;

    log::info!(
        "[hedged_grid] {} 撤销最远{}挂单 {} @ {:.6}",
        symbol_cfg.config_id,
        match side {
            OrderSide::Buy => "买",
            OrderSide::Sell => "卖",
        },
        order.id,
        price
    );

    Ok(())
}

impl OrderKey {
    fn new(side: &OrderSide, price: f64, precision: &PrecisionConfig) -> Option<Self> {
        if price <= 0.0 {
            return None;
        }
        Some(Self {
            side: match side {
                OrderSide::Buy => 'B',
                OrderSide::Sell => 'S',
            },
            price_tick: price_to_tick(price, precision),
        })
    }

    fn from_order(order: &Order, precision: &PrecisionConfig) -> Option<Self> {
        order
            .price
            .and_then(|price| Self::new(&order.side, price, precision))
    }
}

fn quote_to_base_quantity(quote_amount: f64, price: f64, precision: &PrecisionConfig) -> f64 {
    if price <= 0.0 {
        return 0.0;
    }
    let raw = quote_amount / price;
    let qty = quantize(raw, precision.amount_step, precision.amount_digits);
    if qty <= 0.0 {
        min_amount_increment(precision)
    } else {
        qty
    }
}

fn min_price_increment(precision: &PrecisionConfig) -> f64 {
    if precision.price_step > 0.0 {
        precision.price_step
    } else if let Some(d) = precision.price_digits {
        1.0 / 10f64.powi(d as i32)
    } else {
        1e-6
    }
}

fn min_amount_increment(precision: &PrecisionConfig) -> f64 {
    if precision.amount_step > 0.0 {
        precision.amount_step
    } else if let Some(d) = precision.amount_digits {
        1.0 / 10f64.powi(d as i32)
    } else {
        1e-8
    }
}

fn min_inventory_info(
    config: &SymbolConfig,
    precision: &PrecisionConfig,
    center_price: f64,
) -> (f64, f64, f64) {
    let amount_step = min_amount_increment(precision);
    let mut min_inventory = if let Some(min_quote) = config.min_inventory_quote {
        quote_to_base_quantity(min_quote, center_price, precision)
    } else if let Some(ratio) = config.min_inventory_ratio {
        let target = config.target_position.abs();
        (target * ratio).max(0.0)
    } else {
        0.0
    };

    if min_inventory <= 0.0 {
        min_inventory = quote_to_base_quantity(100.0, center_price, precision);
    }

    let min_increment = amount_step.max(1e-8);
    if min_inventory <= 0.0 {
        min_inventory = min_increment;
    } else {
        min_inventory = min_inventory.max(min_increment);
    }

    let tolerance = amount_step.max(min_inventory * 0.05);
    (min_inventory, tolerance, amount_step)
}

fn position_limit_base(
    config: &SymbolConfig,
    precision: &PrecisionConfig,
    center_price: f64,
) -> f64 {
    if let Some(limit_quote) = config.position_limit_quote {
        let converted = quote_to_base_quantity(limit_quote, center_price, precision);
        if converted > 0.0 {
            return converted.max(min_amount_increment(precision));
        }
    }
    config.position_limit.max(min_amount_increment(precision))
}

fn enforce_min_spacing(
    spacing: f64,
    precision: &PrecisionConfig,
    reference_price: f64,
    mode: SpacingMode,
) -> f64 {
    match mode {
        SpacingMode::Absolute => spacing.max(min_price_increment(precision)),
        SpacingMode::Percentage => {
            if reference_price <= 0.0 {
                spacing.max(1e-6)
            } else {
                let min_ratio = min_price_increment(precision) / reference_price;
                spacing.max(min_ratio)
            }
        }
    }
}

fn price_for_level(
    reference_price: f64,
    spacing: f64,
    mode: SpacingMode,
    level: u32,
    side: OrderSide,
) -> Option<f64> {
    if reference_price <= 0.0 || level == 0 {
        return None;
    }
    let level_factor = level as f64;
    let price = match mode {
        SpacingMode::Absolute => match side {
            OrderSide::Buy => reference_price - spacing * level_factor,
            OrderSide::Sell => reference_price + spacing * level_factor,
        },
        SpacingMode::Percentage => {
            let ratio = spacing * level_factor;
            if ratio >= 1.0 {
                return None;
            }
            match side {
                OrderSide::Buy => reference_price * (1.0 - ratio),
                OrderSide::Sell => reference_price * (1.0 + ratio),
            }
        }
    };

    if price > 0.0 && price.is_finite() {
        Some(price)
    } else {
        None
    }
}

fn price_to_tick(price: f64, precision: &PrecisionConfig) -> i64 {
    if precision.price_step > 0.0 {
        (price / precision.price_step).round() as i64
    } else if let Some(d) = precision.price_digits {
        (price * 10f64.powi(d as i32)).round() as i64
    } else {
        (price * 1_000_000.0).round() as i64
    }
}

fn quantity_tolerance(precision: &PrecisionConfig) -> f64 {
    let step = min_amount_increment(precision);
    if step > 0.0 {
        step * 0.5
    } else {
        1e-8
    }
}

fn order_is_reduce_only(order: &Order) -> bool {
    order
        .info
        .get("reduceOnly")
        .and_then(|v| v.as_bool())
        .or_else(|| order.info.get("reduce_only").and_then(|v| v.as_bool()))
        .unwrap_or(false)
}

fn build_grid_orders(
    config: &SymbolConfig,
    center_price: f64,
    control: &OrderControl,
) -> Vec<GridOrder> {
    let mut orders = Vec::new();
    let precision = &config.precision;
    let spacing_mode = control.spacing_mode;

    if control.allow_buys {
        let levels = if control.buy_levels == 0 {
            0
        } else {
            control.buy_levels
        };
        let buy_spacing = control.buy_spacing;

        for i in 1..=levels {
            if let Some(price) =
                price_for_level(center_price, buy_spacing, spacing_mode, i, OrderSide::Buy)
            {
                if let Some(order) =
                    make_order(price, OrderSide::Buy, control.buy_reduce_only, config)
                {
                    orders.push(order);
                }
            }
        }
    }

    if control.allow_sells {
        let levels = if control.sell_levels == 0 {
            0
        } else {
            control.sell_levels
        };
        let sell_spacing = control.sell_spacing;

        for i in 1..=levels {
            if let Some(price) =
                price_for_level(center_price, sell_spacing, spacing_mode, i, OrderSide::Sell)
            {
                if let Some(order) =
                    make_order(price, OrderSide::Sell, control.sell_reduce_only, config)
                {
                    orders.push(order);
                }
            }
        }
    }

    orders
}

fn make_order(
    price: f64,
    side: OrderSide,
    reduce_only: bool,
    config: &SymbolConfig,
) -> Option<GridOrder> {
    let precision = &config.precision;
    let price = quantize(price, precision.price_step, precision.price_digits);
    let mut quantity = config.grid.order_size;
    quantity = quantize(quantity, precision.amount_step, precision.amount_digits);

    if price <= 0.0 || quantity <= 0.0 {
        return None;
    }

    if let Some(min_notional) = precision.min_notional {
        if price * quantity < min_notional {
            return None;
        }
    }

    Some(GridOrder {
        price,
        quantity,
        side,
        reduce_only,
    })
}

async fn submit_orders(
    strategy_name: &str,
    account_manager: &Arc<AccountManager>,
    account_id: &str,
    market_type: MarketType,
    config: &SymbolConfig,
    orders: &[GridOrder],
) -> Result<()> {
    if orders.is_empty() {
        return Ok(());
    }

    let Some(account) = account_manager.get_account(account_id) else {
        return Err(anyhow!("账户 {} 不存在", account_id));
    };

    let precision = &config.precision;
    let price_tick = min_price_increment(precision);

    for chunk in orders.chunks(MAX_BATCH_ORDERS) {
        let orderbook = match account
            .exchange
            .get_orderbook(&config.symbol, market_type, Some(5))
            .await
        {
            Ok(book) => Some(book),
            Err(err) => {
                log::debug!(
                    "[hedged_grid] {} 获取 {} 订单簿失败: {}，使用原始网格价格提交",
                    config.config_id,
                    config.symbol,
                    err
                );
                None
            }
        };

        let (best_bid, best_ask) = orderbook
            .as_ref()
            .map(|book| {
                (
                    book.bids.first().map(|level| level[0]),
                    book.asks.first().map(|level| level[0]),
                )
            })
            .unwrap_or((None, None));

        let mut requests: Vec<OrderRequest> = Vec::with_capacity(chunk.len());

        for grid_order in chunk.iter() {
            let mut price = grid_order.price;

            match grid_order.side {
                OrderSide::Buy => {
                    if let Some(ask) = best_ask {
                        let max_price = ask - price_tick;
                        if max_price <= 0.0 {
                            log::debug!(
                                "[hedged_grid] {} 买单无法设置为maker，best_ask过低，跳过",
                                config.config_id
                            );
                            continue;
                        }
                        if price >= ask || price > max_price {
                            price = max_price;
                        }
                    }
                }
                OrderSide::Sell => {
                    if let Some(bid) = best_bid {
                        let min_price = bid + price_tick;
                        if price <= bid || price < min_price {
                            price = min_price;
                        }
                    }
                }
            }

            price = quantize(price, precision.price_step, precision.price_digits);

            if !price.is_finite() || price <= 0.0 {
                log::debug!(
                    "[hedged_grid] {} 调整后价格无效 ({}), 跳过",
                    config.config_id,
                    price
                );
                continue;
            }

            match grid_order.side {
                OrderSide::Buy => {
                    if let Some(ask) = best_ask {
                        if price >= ask {
                            let adjusted = quantize(
                                ask - price_tick,
                                precision.price_step,
                                precision.price_digits,
                            );
                            if !adjusted.is_finite() || adjusted <= 0.0 || adjusted >= ask {
                                log::debug!(
                                    "[hedged_grid] {} 无法为买单找到小于best_ask的价格，跳过",
                                    config.config_id
                                );
                                continue;
                            }
                            price = adjusted;
                        }
                    }
                }
                OrderSide::Sell => {
                    if let Some(bid) = best_bid {
                        if price <= bid {
                            let adjusted = quantize(
                                bid + price_tick,
                                precision.price_step,
                                precision.price_digits,
                            );
                            if !adjusted.is_finite() || adjusted <= bid {
                                log::debug!(
                                    "[hedged_grid] {} 无法为卖单找到高于best_bid的价格，跳过",
                                    config.config_id
                                );
                                continue;
                            }
                            price = adjusted;
                        }
                    }
                }
            }

            let mut request = OrderRequest::new(
                config.symbol.clone(),
                grid_order.side.clone(),
                OrderType::Limit,
                grid_order.quantity,
                Some(price),
                market_type,
            );
            // 默认使用挂单模式
            request.time_in_force = Some("GTC".to_string());

            if market_type == MarketType::Spot {
                request.post_only = None;
                request.reduce_only = None;
            } else {
                request.post_only = Some(true);
                request.time_in_force = Some("GTX".to_string());
                if grid_order.reduce_only {
                    request.reduce_only = Some(true);
                }
            }
            request.client_order_id = Some(generate_client_id(
                strategy_name,
                &config.config_id,
                &grid_order.side,
            ));

            requests.push(request);
        }

        if requests.is_empty() {
            log::info!(
                "[hedged_grid] {} 本轮无有效新挂单（价格调整后全部被跳过）",
                config.config_id
            );
            continue;
        }

        let summaries: Vec<String> = requests
            .iter()
            .map(|req| {
                let price = req.price.unwrap_or(0.0);
                let amount = req.amount;
                let client_id = req
                    .client_order_id
                    .as_deref()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "-".to_string());
                format!(
                    "{} {:.6}@{:.6} post_only={} reduce_only={} id={}",
                    match req.side {
                        OrderSide::Buy => "BUY",
                        OrderSide::Sell => "SELL",
                    },
                    amount,
                    price,
                    req.post_only.unwrap_or(false),
                    req.reduce_only.unwrap_or(false),
                    client_id
                )
            })
            .collect();
        log::info!(
            "[hedged_grid] {} 提交 {} 笔挂单: {}",
            config.config_id,
            requests.len(),
            summaries.join("; ")
        );

        let batch_request = BatchOrderRequest {
            orders: requests,
            market_type,
        };
        let response = account
            .exchange
            .create_batch_orders(batch_request)
            .await
            .with_context(|| format!("{} 批量下单失败", config.config_id))?;

        if !response.failed_orders.is_empty() {
            log::warn!(
                "[hedged_grid] {} 有 {} 笔订单提交失败: {:?}",
                config.config_id,
                response.failed_orders.len(),
                response.failed_orders
            );
        }
    }

    Ok(())
}

#[derive(Clone)]
struct GridOrder {
    price: f64,
    quantity: f64,
    side: OrderSide,
    reduce_only: bool,
}

fn next_client_sequence() -> u64 {
    CLIENT_ID_SEQ
        .get_or_init(|| AtomicU64::new(0))
        .fetch_add(1, Ordering::Relaxed)
}

fn generate_client_id(strategy: &str, config_id: &str, side: &OrderSide) -> String {
    let side_tag = match side {
        OrderSide::Buy => "B",
        OrderSide::Sell => "S",
    };
    let timestamp_ms = Utc::now().timestamp_millis();
    let seq = next_client_sequence() % 1_000;
    let strategy_tag: String = strategy
        .chars()
        .filter(|c| c.is_alphanumeric())
        .take(4)
        .collect();
    let config_tag: String = config_id
        .chars()
        .filter(|c| c.is_alphanumeric())
        .take(4)
        .collect();
    let timestamp_hex = format!("{:X}", timestamp_ms);
    format!(
        "HG{}{}{}{}{:03}",
        strategy_tag, config_tag, side_tag, timestamp_hex, seq
    )
}

fn quantize(value: f64, step: f64, digits: Option<u32>) -> f64 {
    if step > 0.0 {
        let rounded = (value / step).round() * step;
        if let Some(d) = digits {
            return (rounded * 10f64.powi(d as i32)).round() / 10f64.powi(d as i32);
        }
        rounded
    } else if let Some(d) = digits {
        (value * 10f64.powi(d as i32)).round() / 10f64.powi(d as i32)
    } else {
        value
    }
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.replace('/', "").to_uppercase()
}

fn split_symbol_pair(symbol: &str) -> (String, String) {
    if let Some((base, quote)) = symbol.split_once('/') {
        return (base.to_string(), quote.to_string());
    }
    if let Some((base, quote)) = symbol.split_once('-') {
        return (base.to_string(), quote.to_string());
    }
    if let Some((base, quote)) = symbol.split_once('_') {
        return (base.to_string(), quote.to_string());
    }

    let upper = symbol.to_uppercase();
    let known_quotes = ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH"];
    for quote in known_quotes {
        if upper.ends_with(quote) && upper.len() > quote.len() {
            let base = upper[..upper.len() - quote.len()].to_string();
            return (base, quote.to_string());
        }
    }
    (upper.clone(), String::new())
}

fn market_type_from_str(value: &str) -> MarketType {
    match value.to_lowercase().as_str() {
        "spot" => MarketType::Spot,
        _ => MarketType::Futures,
    }
}
