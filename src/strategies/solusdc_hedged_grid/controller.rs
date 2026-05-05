use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use chrono::{Duration as ChronoDuration, Utc};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Mutex,
};
use tokio::time::{interval, sleep, Duration};

use crate::core::error::ExchangeError;
use crate::core::types::{
    BatchOrderRequest, ExecutionReport, MarketType, Order, OrderRequest, OrderStatus, OrderType,
    WsMessage,
};
use crate::core::websocket::WebSocketClient;
use crate::cta::account_manager::{AccountInfo, AccountManager};
use crate::exchanges::binance::{BinanceExchange, BinanceWebSocketClient};

use super::config::{RuntimeConfig, StrategyConfig};
use super::engine::{
    EngineAction, FillEvent, GridEngine, MarketSnapshot, OrderDraft, PositionState,
};

const MAX_BATCH_ORDERS: usize = 5;

#[derive(Default)]
struct OrderMap {
    client_to_exchange: HashMap<String, String>,
    exchange_to_client: HashMap<String, String>,
}

impl OrderMap {
    fn register(&mut self, client_id: String, exchange_id: String) {
        self.exchange_to_client
            .insert(exchange_id.clone(), client_id.clone());
        self.client_to_exchange.insert(client_id, exchange_id);
    }

    fn exchange_id(&self, client_id: &str) -> Option<String> {
        self.client_to_exchange.get(client_id).cloned()
    }

    fn client_id(&self, exchange_id: &str) -> Option<String> {
        self.exchange_to_client.get(exchange_id).cloned()
    }

    fn remove_client(&mut self, client_id: &str) {
        if let Some(exchange_id) = self.client_to_exchange.remove(client_id) {
            self.exchange_to_client.remove(&exchange_id);
        }
    }

    fn remove_exchange(&mut self, exchange_id: &str) {
        if let Some(client_id) = self.exchange_to_client.remove(exchange_id) {
            self.client_to_exchange.remove(&client_id);
        }
    }

    fn known_exchange_ids(&self) -> HashSet<String> {
        self.exchange_to_client.keys().cloned().collect()
    }
}

#[derive(Debug, Clone)]
enum UserStreamEvent {
    ExecutionReport(ExecutionReport),
}

pub struct SolusdcHedgedGridStrategy {
    config: RuntimeConfig,
    account_manager: Arc<AccountManager>,
    running: Arc<AtomicBool>,
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl SolusdcHedgedGridStrategy {
    pub fn new(config: RuntimeConfig, account_manager: Arc<AccountManager>) -> Self {
        Self {
            config,
            account_manager,
            running: Arc::new(AtomicBool::new(false)),
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let config = self.config.clone();
        let account_manager = self.account_manager.clone();
        let running = self.running.clone();

        let handle = tokio::spawn(async move {
            loop {
                if !running.load(Ordering::SeqCst) {
                    break;
                }
                match run_loop(config.clone(), account_manager.clone(), running.clone()).await {
                    Ok(_) => break,
                    Err(err) => {
                        log::error!("[solusdc_hedged_grid] 运行异常: {}，30秒后重启策略", err);
                        if !running.load(Ordering::SeqCst) {
                            break;
                        }
                        sleep(Duration::from_secs(30)).await;
                    }
                }
            }
        });

        self.handles.lock().await.push(handle);
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        if !self.running.swap(false, Ordering::SeqCst) {
            return Ok(());
        }

        let mut guard = self.handles.lock().await;
        for handle in guard.drain(..) {
            handle.abort();
        }

        if self.config.execution.shutdown_cancel_all {
            if let Some(account) = self
                .account_manager
                .get_account(&self.config.account.account_id)
            {
                let request_timeout = Duration::from_secs(self.config.polling.request_timeout_secs);
                let _ = with_timeout(
                    "cancel_all_orders_stop",
                    request_timeout,
                    account.exchange.cancel_all_orders(
                        Some(&self.config.engine.symbol),
                        self.config.account.market_type,
                    ),
                )
                .await;
            }
        }

        Ok(())
    }
}

async fn run_loop(
    config: RuntimeConfig,
    account_manager: Arc<AccountManager>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let account = account_manager
        .get_account(&config.account.account_id)
        .ok_or_else(|| anyhow!("账户不存在: {}", config.account.account_id))?;

    let hedge_enabled = ensure_hedge_mode(&account, &config.engine).await?;
    let mut engine = Arc::new(Mutex::new(
        GridEngine::new(config.engine.clone(), hedge_enabled).map_err(|err| anyhow!(err))?,
    ));
    let mut order_map = OrderMap::default();
    let mut initialized = false;
    let mut last_reconcile =
        Utc::now() - ChronoDuration::milliseconds(config.polling.reconcile_interval_ms as i64);
    let mut last_zero_position_refresh = Utc::now();
    let zero_position_refresh_interval = ChronoDuration::minutes(2);
    let mut last_hourly_reset = Utc::now();
    let hourly_reset_interval = ChronoDuration::hours(12);
    let request_timeout = Duration::from_secs(config.polling.request_timeout_secs);
    let watchdog_timeout =
        ChronoDuration::seconds(config.polling.watchdog_timeout_secs.max(10) as i64);
    let mut last_progress = Utc::now();
    let mut failure_count = 0u32;
    let mut restart_requested = false;
    let (ws_tx, mut ws_rx) = unbounded_channel();
    if config.websocket.enabled {
        spawn_user_stream_listener(
            config.clone(),
            account_manager.clone(),
            running.clone(),
            ws_tx,
        );
    } else {
        log::info!("[solusdc_hedged_grid] WebSocket推送已禁用，使用轮询模式");
    }

    if config.execution.startup_cancel_all {
        let _ = with_timeout(
            "cancel_all_orders_startup",
            request_timeout,
            account
                .exchange
                .cancel_all_orders(Some(&config.engine.symbol), config.account.market_type),
        )
        .await;
    }

    let mut poll_interval = interval(Duration::from_millis(config.polling.tick_interval_ms));
    poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_snapshot: Option<MarketSnapshot> = None;
    let mut network_backoff = Duration::from_secs(1);
    let network_backoff_max = Duration::from_secs(30);

    while running.load(Ordering::SeqCst) {
        tokio::select! {
            _ = poll_interval.tick() => {
                let now = Utc::now();

                drain_user_stream_events(
                    &config,
                    account.as_ref(),
                    engine.as_ref(),
                    &mut order_map,
                    &mut failure_count,
                    &mut restart_requested,
                    &mut last_snapshot,
                    &mut ws_rx,
                    &mut last_progress,
                )
                .await?;
                if restart_requested {
                    restart_requested = false;
                    restart_strategy(
                        &config,
                        account.as_ref(),
                        hedge_enabled,
                        request_timeout,
                        &mut engine,
                        &mut order_map,
                        &mut initialized,
                        &mut last_reconcile,
                        &mut last_zero_position_refresh,
                        &mut last_hourly_reset,
                        &mut failure_count,
                        "order_reject",
                    )
                    .await?;
                    continue;
                }

                if now - last_progress >= watchdog_timeout {
                    restart_strategy(
                        &config,
                        account.as_ref(),
                        hedge_enabled,
                        request_timeout,
                        &mut engine,
                        &mut order_map,
                        &mut initialized,
                        &mut last_reconcile,
                        &mut last_zero_position_refresh,
                        &mut last_hourly_reset,
                        &mut failure_count,
                        "watchdog_timeout",
                    )
                    .await?;
                    last_progress = Utc::now();
                    continue;
                }

                let (snapshot, funding_rate) = match fetch_snapshot(
                    account.as_ref(),
                    &config.engine.symbol,
                    config.account.market_type,
                    request_timeout,
                )
                .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        if is_network_error(&err) {
                            log::warn!(
                                "[solusdc_hedged_grid] 网络错误，{}秒后重试: {}",
                                network_backoff.as_secs(),
                                err
                            );
                            sleep(network_backoff).await;
                            network_backoff = next_backoff(network_backoff, network_backoff_max);
                            continue;
                        }
                        log::warn!("[solusdc_hedged_grid] 行情拉取失败: {}", err);
                        continue;
                    }
                };
                network_backoff = Duration::from_secs(1);
                last_snapshot = Some(snapshot.clone());
                last_progress = now;

                let mut actions = Vec::new();
                {
                    let mut guard = engine.lock().await;
                    guard.update_market(&snapshot);
                    guard.update_funding_rate(funding_rate);
                    if !initialized {
                        let position = match fetch_position_state(
                            account.as_ref(),
                            &config.engine.symbol,
                            config.account.market_type,
                            snapshot.mark_price,
                            request_timeout,
                        )
                        .await
                        {
                            Ok(position) => {
                                network_backoff = Duration::from_secs(1);
                                position
                            }
                            Err(err) => {
                                if is_network_error(&err) {
                                    log::warn!(
                                        "[solusdc_hedged_grid] 网络错误，{}秒后重试: {}",
                                        network_backoff.as_secs(),
                                        err
                                    );
                                    sleep(network_backoff).await;
                                    network_backoff =
                                        next_backoff(network_backoff, network_backoff_max);
                                    continue;
                                }
                                return Err(err);
                            }
                        };
                        log_position_snapshot(&config.engine.symbol, &position);
                        guard.update_position(position);
                        actions.extend(guard.rebuild_grid(&snapshot));
                        initialized = true;
                    } else {
                        actions.extend(guard.maybe_follow(&snapshot));
                    }
                }

                execute_actions(
                    &config,
                    account.as_ref(),
                    engine.as_ref(),
                    &mut order_map,
                    &mut failure_count,
                    &mut restart_requested,
                    actions,
                )
                .await?;

                if now - last_reconcile
                    >= ChronoDuration::milliseconds(config.polling.reconcile_interval_ms as i64)
                {
                    last_reconcile = now;
                    let position = match fetch_position_state(
                        account.as_ref(),
                        &config.engine.symbol,
                        config.account.market_type,
                        snapshot.mark_price,
                        request_timeout,
                    )
                    .await
                    {
                        Ok(position) => {
                            network_backoff = Duration::from_secs(1);
                            position
                        }
                        Err(err) => {
                            if is_network_error(&err) {
                                log::warn!(
                                    "[solusdc_hedged_grid] 网络错误，{}秒后重试: {}",
                                    network_backoff.as_secs(),
                                    err
                                );
                                sleep(network_backoff).await;
                                network_backoff =
                                    next_backoff(network_backoff, network_backoff_max);
                                continue;
                            }
                            return Err(err);
                        }
                    };
                    log_position_snapshot(&config.engine.symbol, &position);
                    let open_orders = match with_timeout(
                        "get_open_orders",
                        request_timeout,
                        account
                            .exchange
                            .get_open_orders(Some(&config.engine.symbol), config.account.market_type),
                    )
                    .await
                    {
                        Ok(orders) => {
                            network_backoff = Duration::from_secs(1);
                            orders
                        }
                        Err(err) => {
                            if is_network_error(&err) {
                                log::warn!(
                                    "[solusdc_hedged_grid] 网络错误，{}秒后重试: {}",
                                    network_backoff.as_secs(),
                                    err
                                );
                                sleep(network_backoff).await;
                                network_backoff =
                                    next_backoff(network_backoff, network_backoff_max);
                                continue;
                            }
                            return Err(err);
                        }
                    };
                    let trades = match with_timeout(
                        "get_my_trades",
                        request_timeout,
                        account.exchange.get_my_trades(
                            Some(&config.engine.symbol),
                            config.account.market_type,
                            Some(config.polling.trade_fetch_limit),
                        ),
                    )
                    .await
                    {
                        Ok(trades) => trades,
                        Err(err) => {
                            log::warn!("[solusdc_hedged_grid] 成交记录拉取失败: {}", err);
                            Vec::new()
                        }
                    };

                    let long_empty = position.long_qty.abs() < 1e-8;
                    let short_empty = position.short_qty.abs() < 1e-8;
                    let needs_zero_position_refresh = (long_empty && short_empty)
                        && now - last_zero_position_refresh >= zero_position_refresh_interval;
                    let needs_hourly_reset = now - last_hourly_reset >= hourly_reset_interval;
                    let mut actions = Vec::new();
                    {
                        let mut guard = engine.lock().await;
                        guard.update_position(position);
                        actions.extend(sync_orders(
                            &mut *guard,
                            &mut order_map,
                            &open_orders,
                            &trades,
                            &snapshot,
                        ));
                        if needs_hourly_reset {
                            actions.extend(guard.rebuild_grid(&snapshot));
                        } else if needs_zero_position_refresh {
                            actions.extend(guard.rebuild_grid(&snapshot));
                        } else {
                            actions.extend(guard.reconcile_inventory(&snapshot));
                        }
                    }

                    execute_actions(
                        &config,
                        account.as_ref(),
                        engine.as_ref(),
                        &mut order_map,
                        &mut failure_count,
                        &mut restart_requested,
                        actions,
                    )
                    .await?;

                    if restart_requested {
                        restart_requested = false;
                        restart_strategy(
                            &config,
                            account.as_ref(),
                            hedge_enabled,
                            request_timeout,
                            &mut engine,
                            &mut order_map,
                            &mut initialized,
                            &mut last_reconcile,
                            &mut last_zero_position_refresh,
                            &mut last_hourly_reset,
                            &mut failure_count,
                            "order_reject",
                        )
                        .await?;
                        continue;
                    }

                    if needs_hourly_reset {
                        last_hourly_reset = now;
                        log::info!("[solusdc_hedged_grid] 定时重置网格 interval=12h");
                    }

                    if needs_zero_position_refresh {
                        last_zero_position_refresh = now;
                        log::info!(
                            "[solusdc_hedged_grid] 零仓刷新订单 long_empty={} short_empty={}",
                            long_empty,
                            short_empty
                        );
                    }

                    cancel_unknown_orders(
                        account.as_ref(),
                        &config.engine.symbol,
                        config.account.market_type,
                        request_timeout,
                        &order_map,
                        &open_orders,
                    )
                    .await;
                }

                if failure_count >= config.execution.max_consecutive_failures {
                    restart_strategy(
                        &config,
                        account.as_ref(),
                        hedge_enabled,
                        request_timeout,
                        &mut engine,
                        &mut order_map,
                        &mut initialized,
                        &mut last_reconcile,
                        &mut last_zero_position_refresh,
                        &mut last_hourly_reset,
                        &mut failure_count,
                        "consecutive_failures",
                    )
                    .await?;
                }

            }
            ws_event = ws_rx.recv() => {
                if let Some(event) = ws_event {
                    handle_user_stream_event(
                        &config,
                        account.as_ref(),
                        engine.as_ref(),
                        &mut order_map,
                        &mut failure_count,
                        &mut restart_requested,
                        &mut last_snapshot,
                        event,
                    )
                    .await?;
                    last_progress = Utc::now();
                    if restart_requested {
                        restart_requested = false;
                        restart_strategy(
                            &config,
                            account.as_ref(),
                            hedge_enabled,
                            request_timeout,
                            &mut engine,
                            &mut order_map,
                            &mut initialized,
                            &mut last_reconcile,
                            &mut last_zero_position_refresh,
                            &mut last_hourly_reset,
                            &mut failure_count,
                            "order_reject",
                        )
                        .await?;
                        continue;
                    }
                    drain_user_stream_events(
                        &config,
                        account.as_ref(),
                        engine.as_ref(),
                        &mut order_map,
                        &mut failure_count,
                        &mut restart_requested,
                        &mut last_snapshot,
                        &mut ws_rx,
                        &mut last_progress,
                    )
                    .await?;
                    if restart_requested {
                        restart_requested = false;
                        restart_strategy(
                            &config,
                            account.as_ref(),
                            hedge_enabled,
                            request_timeout,
                            &mut engine,
                            &mut order_map,
                            &mut initialized,
                            &mut last_reconcile,
                            &mut last_zero_position_refresh,
                            &mut last_hourly_reset,
                            &mut failure_count,
                            "order_reject",
                        )
                        .await?;
                    }
                }
            }
        }
    }

    if config.execution.shutdown_cancel_all {
        let _ = with_timeout(
            "cancel_all_orders_shutdown",
            request_timeout,
            account
                .exchange
                .cancel_all_orders(Some(&config.engine.symbol), config.account.market_type),
        )
        .await;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn drain_user_stream_events(
    config: &RuntimeConfig,
    account: &AccountInfo,
    engine: &Mutex<GridEngine>,
    order_map: &mut OrderMap,
    failure_count: &mut u32,
    restart_requested: &mut bool,
    last_snapshot: &mut Option<MarketSnapshot>,
    ws_rx: &mut UnboundedReceiver<UserStreamEvent>,
    last_progress: &mut chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let mut drained = 0usize;
    while let Ok(event) = ws_rx.try_recv() {
        handle_user_stream_event(
            config,
            account,
            engine,
            order_map,
            failure_count,
            restart_requested,
            last_snapshot,
            event,
        )
        .await?;
        *last_progress = Utc::now();
        drained += 1;
        if *restart_requested {
            break;
        }
    }
    if drained > 0 {
        log::debug!(
            "[solusdc_hedged_grid] tick前优先处理WebSocket事件 count={}",
            drained
        );
    }
    Ok(())
}

async fn handle_user_stream_event(
    config: &RuntimeConfig,
    account: &AccountInfo,
    engine: &Mutex<GridEngine>,
    order_map: &mut OrderMap,
    failure_count: &mut u32,
    restart_requested: &mut bool,
    last_snapshot: &mut Option<MarketSnapshot>,
    event: UserStreamEvent,
) -> Result<()> {
    let report = match event {
        UserStreamEvent::ExecutionReport(report) => report,
    };

    let target_symbol = normalize_symbol(&config.engine.symbol);
    if normalize_symbol(&report.symbol) != target_symbol {
        return Ok(());
    }

    let Some(client_id) = resolve_client_id(order_map, &report) else {
        return Ok(());
    };

    let mut actions = Vec::new();
    let mut fill_info: Option<FillEvent> = None;
    let mut fill_order_price: Option<f64> = None;
    let mut remove_mapping = false;
    {
        let mut guard = engine.lock().await;
        let Some(record) = guard.order_record(&client_id) else {
            order_map.remove_exchange(&report.order_id);
            return Ok(());
        };

        if order_map.exchange_id(&client_id).is_none() {
            order_map.register(client_id.clone(), report.order_id.clone());
        }

        let order_price = record.price;
        let total_qty = record.filled_qty + record.qty;
        let executed_amount = report.executed_amount;
        let delta = executed_amount - record.filled_qty;
        let mut partial_fill = None;
        if delta > 1e-8 {
            let snapshot = snapshot_from_report(last_snapshot, &report);
            let fill_price = if report.executed_price > 0.0 {
                report.executed_price
            } else if record.price > 0.0 {
                record.price
            } else {
                report.price
            };
            let partial = executed_amount + 1e-8 < total_qty;
            partial_fill = Some(partial);
            let fill = FillEvent {
                order_id: client_id.clone(),
                intent: record.intent,
                fill_qty: delta,
                fill_price,
                timestamp: report.timestamp,
                partial,
            };
            *last_snapshot = Some(snapshot.clone());
            fill_info = Some(fill.clone());
            fill_order_price = Some(order_price);
            actions.extend(guard.handle_fill(fill, &snapshot));
            if !partial {
                remove_mapping = true;
            }
        }

        if matches!(
            report.status,
            OrderStatus::Canceled | OrderStatus::Expired | OrderStatus::Rejected
        ) {
            guard.handle_order_reject(&client_id);
            remove_mapping = true;
            if report.status == OrderStatus::Rejected {
                let should_ignore_reject =
                    config.engine.grid.strict_pairing && record.intent.is_close();
                if !should_ignore_reject {
                    *restart_requested = true;
                }
            }
        } else if report.status == OrderStatus::Closed {
            if partial_fill.unwrap_or(false) || delta <= 1e-8 {
                guard.handle_order_reject(&client_id);
            }
            remove_mapping = true;
        }
    }

    if remove_mapping {
        order_map.remove_client(&client_id);
    }

    if let (Some(fill), Some(order_price)) = (&fill_info, fill_order_price) {
        log_fill_actions("ws", fill, order_price, &actions);
    }

    if !actions.is_empty() {
        execute_actions(
            config,
            account,
            engine,
            order_map,
            failure_count,
            restart_requested,
            actions,
        )
        .await?;
    }

    Ok(())
}

fn spawn_user_stream_listener(
    config: RuntimeConfig,
    account_manager: Arc<AccountManager>,
    running: Arc<AtomicBool>,
    event_tx: UnboundedSender<UserStreamEvent>,
) {
    tokio::spawn(async move {
        let account_id = config.account.account_id.clone();
        let market_type = config.account.market_type;
        let normalized_symbol = normalize_symbol(&config.engine.symbol);
        let mut backoff = Duration::from_millis(config.websocket.reconnect_delay_ms);
        let max_backoff = Duration::from_millis(config.websocket.max_reconnect_delay_ms);

        while running.load(Ordering::SeqCst) {
            let Some(account) = account_manager.get_account(&account_id) else {
                log::warn!(
                    "[solusdc_hedged_grid] WebSocket启动失败: 账户 {} 不存在",
                    account_id
                );
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            };

            if account.exchange_name.to_lowercase() != "binance" {
                log::info!(
                    "[solusdc_hedged_grid] 当前交易所 {} 暂不支持用户流推送，跳过 WebSocket",
                    account.exchange_name
                );
                break;
            }

            let listen_key = match account.exchange.create_user_data_stream(market_type).await {
                Ok(key) => key,
                Err(err) => {
                    log::warn!("[solusdc_hedged_grid] 获取 listenKey 失败: {}", err);
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                    continue;
                }
            };

            let ws_url = match market_type {
                MarketType::Spot => format!("wss://stream.binance.com:9443/ws/{}", listen_key),
                MarketType::Futures => {
                    format!("wss://fstream.binance.com/private/ws/{}", listen_key)
                }
            };

            let mut ws_client = BinanceWebSocketClient::new(ws_url, market_type);
            if let Err(err) = ws_client.connect().await {
                log::warn!("[solusdc_hedged_grid] WebSocket连接失败: {}", err);
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
                continue;
            }
            backoff = Duration::from_millis(config.websocket.reconnect_delay_ms);
            log::info!(
                "[solusdc_hedged_grid] WebSocket用户流已连接 (account={})",
                account_id
            );

            let mut keepalive = interval(Duration::from_secs(
                config.websocket.keepalive_interval_secs,
            ));

            while running.load(Ordering::SeqCst) {
                tokio::select! {
                    msg = ws_client.receive() => {
                        match msg {
                            Ok(Some(raw)) => match ws_client.parse_binance_message(&raw) {
                                Ok(WsMessage::ExecutionReport(report)) => {
                                    if normalize_symbol(&report.symbol) == normalized_symbol {
                                        if event_tx
                                            .send(UserStreamEvent::ExecutionReport(report))
                                            .is_err()
                                        {
                                            log::warn!(
                                                "[solusdc_hedged_grid] 成交通道已关闭，停止监听"
                                            );
                                            return;
                                        }
                                    }
                                }
                                Ok(WsMessage::Error(err)) => {
                                    if err.contains("listenKeyExpired")
                                        || err.contains("ListenKeyExpired")
                                    {
                                        log::warn!(
                                            "[solusdc_hedged_grid] listenKey 已过期，准备重连"
                                        );
                                        break;
                                    }
                                }
                                _ => {}
                            },
                            Ok(None) => {
                                sleep(Duration::from_millis(50)).await;
                            }
                            Err(err) => {
                                log::warn!("[solusdc_hedged_grid] WebSocket接收失败: {}", err);
                                break;
                            }
                        }
                    }
                    _ = keepalive.tick() => {
                        if let Err(err) = account
                            .exchange
                            .keepalive_user_data_stream(&listen_key, market_type)
                            .await
                        {
                            log::warn!(
                                "[solusdc_hedged_grid] ListenKey保活失败: {}",
                                err
                            );
                        }
                    }
                }
            }

            if running.load(Ordering::SeqCst) {
                sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    });
}

fn log_fill_actions(source: &str, fill: &FillEvent, order_price: f64, actions: &[EngineAction]) {
    let mut placed = Vec::new();
    let mut canceled = Vec::new();

    for action in actions {
        match action {
            EngineAction::Place(draft) => {
                placed.push(format!(
                    "{:?} {:.6}@{:.6}",
                    draft.intent, draft.qty, draft.price
                ));
            }
            EngineAction::Cancel { order_id, reason } => {
                canceled.push(format!("{}({})", order_id, reason));
            }
        }
    }

    let placed_text = if placed.is_empty() {
        "无".to_string()
    } else {
        placed.join(" | ")
    };
    let canceled_text = if canceled.is_empty() {
        "否".to_string()
    } else {
        canceled.join(" | ")
    };

    log::info!(
        "[solusdc_hedged_grid] 成交箱 source={}\n  成交: id={} intent={:?} qty={:.6} price={:.6} order_price={:.6} partial={}\n  新挂单({}): {}\n  取消({}): {}",
        source,
        fill.order_id,
        fill.intent,
        fill.fill_qty,
        fill.fill_price,
        order_price,
        fill.partial,
        placed.len(),
        placed_text,
        canceled.len(),
        canceled_text
    );
}

fn log_position_snapshot(symbol: &str, position: &PositionState) {
    let net_qty = position.long_qty - position.short_qty;
    log::debug!(
        "[solusdc_hedged_grid] 库存快照 {} long={:.6} short={:.6} long_entry={:.4} short_entry={:.4} long_avail={:.6} short_avail={:.6} net={:.6} mark={:.4} equity={:.2} mm={:.4}",
        symbol,
        position.long_qty,
        position.short_qty,
        position.long_entry_price,
        position.short_entry_price,
        position.long_available,
        position.short_available,
        net_qty,
        position.mark_price,
        position.equity,
        position.maintenance_margin
    );
}

fn snapshot_from_report(
    last_snapshot: &Option<MarketSnapshot>,
    report: &ExecutionReport,
) -> MarketSnapshot {
    let reference_price = if report.executed_price > 0.0 {
        report.executed_price
    } else {
        report.price
    };

    match last_snapshot {
        Some(snapshot) => {
            let mut snapshot = snapshot.clone();
            snapshot.timestamp = report.timestamp;
            if reference_price > 0.0 {
                snapshot.last_price = reference_price;
                if snapshot.best_bid <= 0.0 || snapshot.best_ask <= 0.0 {
                    snapshot.best_bid = reference_price * 0.999;
                    snapshot.best_ask = reference_price * 1.001;
                }
                if snapshot.mark_price <= 0.0 {
                    snapshot.mark_price = reference_price;
                }
            }
            snapshot
        }
        None => {
            let best_bid = if reference_price > 0.0 {
                reference_price * 0.999
            } else {
                0.0
            };
            let best_ask = if reference_price > 0.0 {
                reference_price * 1.001
            } else {
                0.0
            };
            MarketSnapshot {
                best_bid,
                best_ask,
                last_price: reference_price.max(0.0),
                mark_price: reference_price.max(0.0),
                timestamp: report.timestamp,
            }
        }
    }
}

fn resolve_client_id(order_map: &OrderMap, report: &ExecutionReport) -> Option<String> {
    if let Some(client_id) = report
        .client_order_id
        .as_ref()
        .map(|id| id.trim())
        .filter(|id| !id.is_empty())
    {
        return Some(client_id.to_string());
    }
    order_map.client_id(&report.order_id)
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .replace('/', "")
        .replace('-', "")
        .replace('_', "")
        .to_uppercase()
}

async fn ensure_hedge_mode(account: &AccountInfo, config: &StrategyConfig) -> Result<bool> {
    if !config.require_hedge_mode {
        return Ok(true);
    }
    if let Some(binance) = account.exchange.as_any().downcast_ref::<BinanceExchange>() {
        let dual = binance.get_position_mode().await?;
        if !dual {
            return Err(anyhow!("账户未开启 Hedge Mode"));
        }
        return Ok(true);
    }
    Err(anyhow!("当前交易所不支持 Hedge Mode 校验"))
}

async fn fetch_snapshot(
    account: &AccountInfo,
    symbol: &str,
    market_type: MarketType,
    request_timeout: Duration,
) -> Result<(MarketSnapshot, f64)> {
    let ticker = with_timeout(
        "get_ticker",
        request_timeout,
        account.exchange.get_ticker(symbol, market_type),
    )
    .await?;
    let mut mark_price = ticker.last;
    let mut funding_rate = 0.0;

    if market_type == MarketType::Futures {
        if let Ok(mark_prices) = with_timeout(
            "get_mark_price",
            request_timeout,
            account.exchange.get_mark_price(Some(symbol)),
        )
        .await
        {
            if let Some(mark) = mark_prices.iter().find(|m| m.symbol == symbol) {
                mark_price = mark.mark_price;
                funding_rate = mark.last_funding_rate;
            }
        }
    }

    let best_bid = if ticker.bid > 0.0 {
        ticker.bid
    } else {
        ticker.last * 0.999
    };
    let best_ask = if ticker.ask > 0.0 {
        ticker.ask
    } else {
        ticker.last * 1.001
    };

    Ok((
        MarketSnapshot {
            best_bid,
            best_ask,
            last_price: ticker.last,
            mark_price,
            timestamp: ticker.timestamp,
        },
        funding_rate,
    ))
}

async fn fetch_position_state(
    account: &AccountInfo,
    symbol: &str,
    market_type: MarketType,
    mark_price: f64,
    request_timeout: Duration,
) -> Result<PositionState> {
    let positions = with_timeout(
        "get_positions",
        request_timeout,
        account.exchange.get_positions(Some(symbol)),
    )
    .await?;
    let balances = with_timeout(
        "get_balance",
        request_timeout,
        account.exchange.get_balance(market_type),
    )
    .await?;
    let equity: f64 = balances.iter().map(|b| b.total).sum();

    let mut long_qty = 0.0;
    let mut short_qty = 0.0;
    let mut long_entry_price = 0.0;
    let mut short_entry_price = 0.0;
    let mut maintenance_margin = 0.0;
    let mut position_mark = mark_price;

    for pos in positions {
        if pos.side.to_uppercase() == "LONG" {
            long_qty = pos.contracts;
            long_entry_price = pos.entry_price;
        } else if pos.side.to_uppercase() == "SHORT" {
            short_qty = pos.contracts;
            short_entry_price = pos.entry_price;
        }
        if pos.mark_price > 0.0 {
            position_mark = pos.mark_price;
        }
        maintenance_margin += pos.margin;
    }

    Ok(PositionState {
        long_qty,
        short_qty,
        long_entry_price,
        short_entry_price,
        long_available: long_qty,
        short_available: short_qty,
        equity,
        maintenance_margin,
        mark_price: position_mark,
    })
}

fn sync_orders(
    engine: &mut GridEngine,
    order_map: &mut OrderMap,
    open_orders: &[Order],
    trades: &[crate::core::types::Trade],
    snapshot: &MarketSnapshot,
) -> Vec<EngineAction> {
    let mut actions = Vec::new();
    let mut open_map = HashMap::new();
    for order in open_orders {
        open_map.insert(order.id.clone(), order.clone());
    }

    let mut trade_map: HashMap<String, (f64, f64)> = HashMap::new();
    for trade in trades {
        if let Some(order_id) = &trade.order_id {
            let entry = trade_map.entry(order_id.clone()).or_insert((0.0, 0.0));
            entry.0 += trade.amount;
            entry.1 += trade.amount * trade.price;
        }
    }

    for client_id in engine.order_ids() {
        let record = match engine.order_record(&client_id) {
            Some(record) => record,
            None => {
                order_map.remove_client(&client_id);
                continue;
            }
        };
        let exchange_id = match order_map.exchange_id(&client_id) {
            Some(exchange_id) => exchange_id,
            None => {
                engine.handle_order_reject(&client_id);
                continue;
            }
        };

        if let Some(open_order) = open_map.get(&exchange_id) {
            if open_order.filled > record.filled_qty + 1e-8 {
                let delta = open_order.filled - record.filled_qty;
                let price = open_order.price.unwrap_or(record.price);
                let fill = FillEvent {
                    order_id: client_id.clone(),
                    intent: record.intent,
                    fill_qty: delta,
                    fill_price: price,
                    timestamp: snapshot.timestamp,
                    partial: open_order.remaining > 0.0,
                };
                let fill_actions = engine.handle_fill(fill.clone(), snapshot);
                log_fill_actions("reconcile_open_order", &fill, record.price, &fill_actions);
                actions.extend(fill_actions);
            }
            continue;
        }

        if let Some((qty, quote)) = trade_map.get(&exchange_id) {
            if *qty > record.filled_qty + 1e-8 {
                let delta = qty - record.filled_qty;
                let price = if *qty > 0.0 {
                    quote / qty
                } else {
                    record.price
                };
                let partial = *qty + 1e-8 < record.qty;
                let fill = FillEvent {
                    order_id: client_id.clone(),
                    intent: record.intent,
                    fill_qty: delta,
                    fill_price: price,
                    timestamp: snapshot.timestamp,
                    partial,
                };
                let fill_actions = engine.handle_fill(fill.clone(), snapshot);
                log_fill_actions("reconcile_trade", &fill, record.price, &fill_actions);
                actions.extend(fill_actions);
            }
        } else {
            engine.handle_order_reject(&client_id);
        }

        order_map.remove_client(&client_id);
    }

    actions
}

async fn cancel_unknown_orders(
    account: &AccountInfo,
    symbol: &str,
    market_type: MarketType,
    request_timeout: Duration,
    order_map: &OrderMap,
    open_orders: &[Order],
) {
    let known = order_map.known_exchange_ids();
    for order in open_orders {
        if !known.contains(&order.id) {
            let _ = with_timeout(
                "cancel_unknown_order",
                request_timeout,
                account
                    .exchange
                    .cancel_order(&order.id, symbol, market_type),
            )
            .await;
        }
    }
}

async fn execute_actions(
    config: &RuntimeConfig,
    account: &AccountInfo,
    engine: &Mutex<GridEngine>,
    order_map: &mut OrderMap,
    failure_count: &mut u32,
    restart_requested: &mut bool,
    actions: Vec<EngineAction>,
) -> Result<()> {
    let request_timeout = Duration::from_secs(config.polling.request_timeout_secs);
    let mut cancel_queue = VecDeque::new();
    let mut place_queue = VecDeque::new();
    enqueue_actions(&mut cancel_queue, &mut place_queue, actions);

    while let Some(action) = pop_next_action(&mut cancel_queue, &mut place_queue) {
        match action {
            EngineAction::Place(draft) => {
                let drafts = drain_place_batch(draft, &mut place_queue);
                execute_place_batch(
                    config,
                    account,
                    engine,
                    order_map,
                    failure_count,
                    restart_requested,
                    request_timeout,
                    &mut cancel_queue,
                    &mut place_queue,
                    drafts,
                )
                .await?;
            }
            EngineAction::Cancel { order_id, reason } => {
                if let Some(exchange_id) = order_map.exchange_id(&order_id) {
                    match with_timeout(
                        "cancel_order",
                        request_timeout,
                        account.exchange.cancel_order(
                            &exchange_id,
                            &config.engine.symbol,
                            config.account.market_type,
                        ),
                    )
                    .await
                    {
                        Ok(_) => {
                            order_map.remove_exchange(&exchange_id);
                            log::info!(
                                "[solusdc_hedged_grid] 撤单成功 {} exchange_id={} reason={}",
                                order_id,
                                exchange_id,
                                reason
                            );
                        }
                        Err(err) => {
                            log::warn!(
                                "[solusdc_hedged_grid] 撤单失败 {} exchange_id={} reason={} err={}",
                                order_id,
                                exchange_id,
                                reason,
                                err
                            );
                        }
                    }
                } else {
                    log::warn!(
                        "[solusdc_hedged_grid] 撤单跳过: 本地订单缺少exchange_id order_id={} reason={}",
                        order_id,
                        reason
                    );
                }
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_place_batch(
    config: &RuntimeConfig,
    account: &AccountInfo,
    engine: &Mutex<GridEngine>,
    order_map: &mut OrderMap,
    failure_count: &mut u32,
    restart_requested: &mut bool,
    request_timeout: Duration,
    cancel_queue: &mut VecDeque<EngineAction>,
    place_queue: &mut VecDeque<EngineAction>,
    drafts: Vec<OrderDraft>,
) -> Result<()> {
    if drafts.is_empty() {
        return Ok(());
    }

    let summary: Vec<String> = drafts
        .iter()
        .map(|draft| {
            format!(
                "{}:{:?} {:.6}@{:.6} pos={} post_only={}",
                draft.id,
                draft.intent,
                draft.qty,
                draft.price,
                draft.intent.position_side().as_str(),
                draft.post_only
            )
        })
        .collect();
    log::info!(
        "[solusdc_hedged_grid] 批量提交挂单 {} 共{}笔: {}",
        config.engine.symbol,
        drafts.len(),
        summary.join(" | ")
    );

    let requests: Vec<OrderRequest> = drafts
        .iter()
        .map(|draft| build_order_request(&config.engine, config.account.market_type, draft))
        .collect();
    let draft_ids_in_order: Vec<String> = drafts.iter().map(|draft| draft.id.clone()).collect();
    let batch_request = BatchOrderRequest {
        orders: requests,
        market_type: config.account.market_type,
    };

    match with_timeout(
        "create_batch_orders",
        request_timeout,
        account.exchange.create_batch_orders(batch_request),
    )
    .await
    {
        Ok(response) => {
            let mut pending_by_id: HashMap<String, OrderDraft> = drafts
                .into_iter()
                .map(|draft| (draft.id.clone(), draft))
                .collect();

            for failed in response.failed_orders {
                let Some(client_id) = failed.order_request.client_order_id.clone() else {
                    log::warn!(
                        "[solusdc_hedged_grid] 批量下单失败条目缺少client_order_id: code={:?} msg={}",
                        failed.error_code,
                        failed.error_message
                    );
                    continue;
                };
                let Some(draft) = pending_by_id.remove(&client_id) else {
                    log::warn!(
                        "[solusdc_hedged_grid] 批量失败条目无法匹配本地订单 id={} code={:?} msg={}",
                        client_id,
                        failed.error_code,
                        failed.error_message
                    );
                    continue;
                };

                let code = failed
                    .error_code
                    .as_deref()
                    .and_then(|value| value.parse::<i32>().ok())
                    .unwrap_or(0);
                let exchange_err = ExchangeError::ApiError {
                    code,
                    message: failed.error_message.clone(),
                };
                let (expected_pairing_reject, post_only_reject, order_reject) =
                    classify_place_error(config, &draft, &exchange_err);
                process_place_failure(
                    config,
                    engine,
                    cancel_queue,
                    place_queue,
                    failure_count,
                    restart_requested,
                    &draft,
                    &exchange_err.to_string(),
                    expected_pairing_reject,
                    post_only_reject,
                    order_reject,
                )
                .await;
            }

            let mut unmatched_success = Vec::new();
            for order in response.successful_orders {
                if let Some(client_id) = extract_client_order_id(&order) {
                    if pending_by_id.remove(&client_id).is_some() {
                        *failure_count = 0;
                        order_map.register(client_id, order.id);
                        continue;
                    }
                }
                unmatched_success.push(order);
            }

            if !unmatched_success.is_empty() {
                let remaining_ids: Vec<String> = draft_ids_in_order
                    .iter()
                    .filter(|id| pending_by_id.contains_key(*id))
                    .cloned()
                    .collect();
                for (order, client_id) in unmatched_success.into_iter().zip(remaining_ids) {
                    if pending_by_id.remove(&client_id).is_some() {
                        *failure_count = 0;
                        order_map.register(client_id, order.id);
                    }
                }
            }

            for draft in pending_by_id.into_values() {
                let exchange_err = ExchangeError::OrderError("批量响应缺少订单回执".to_string());
                let (expected_pairing_reject, post_only_reject, order_reject) =
                    classify_place_error(config, &draft, &exchange_err);
                process_place_failure(
                    config,
                    engine,
                    cancel_queue,
                    place_queue,
                    failure_count,
                    restart_requested,
                    &draft,
                    &exchange_err.to_string(),
                    expected_pairing_reject,
                    post_only_reject,
                    order_reject,
                )
                .await;
            }
        }
        Err(err) => {
            let error_text = err.to_string();
            for draft in &drafts {
                let (expected_pairing_reject, post_only_reject, order_reject) =
                    if let Some(exchange_err) = err.downcast_ref::<ExchangeError>() {
                        classify_place_error(config, draft, exchange_err)
                    } else {
                        let synthetic = ExchangeError::Other(error_text.clone());
                        classify_place_error(config, draft, &synthetic)
                    };
                process_place_failure(
                    config,
                    engine,
                    cancel_queue,
                    place_queue,
                    failure_count,
                    restart_requested,
                    draft,
                    &error_text,
                    expected_pairing_reject,
                    post_only_reject,
                    order_reject,
                )
                .await;
            }
        }
    }

    Ok(())
}

fn classify_place_error(
    config: &RuntimeConfig,
    draft: &OrderDraft,
    exchange_err: &ExchangeError,
) -> (bool, bool, bool) {
    let expected_pairing_reject =
        is_expected_strict_pairing_close_reject(config, draft, exchange_err);
    let post_only_reject = is_post_only_reject(exchange_err);
    let order_reject = is_order_reject(exchange_err);
    (expected_pairing_reject, post_only_reject, order_reject)
}

#[allow(clippy::too_many_arguments)]
async fn process_place_failure(
    config: &RuntimeConfig,
    engine: &Mutex<GridEngine>,
    cancel_queue: &mut VecDeque<EngineAction>,
    place_queue: &mut VecDeque<EngineAction>,
    failure_count: &mut u32,
    restart_requested: &mut bool,
    draft: &OrderDraft,
    error_text: &str,
    expected_pairing_reject: bool,
    post_only_reject: bool,
    order_reject: bool,
) {
    if order_reject && !expected_pairing_reject && !post_only_reject {
        *restart_requested = true;
    }

    if post_only_reject {
        let mut guard = engine.lock().await;
        let follow_actions = guard.handle_post_only_reject(&draft.id);
        drop(guard);
        enqueue_actions(cancel_queue, place_queue, follow_actions);
    } else {
        let mut guard = engine.lock().await;
        guard.handle_order_reject(&draft.id);
        drop(guard);
    }

    if expected_pairing_reject || post_only_reject {
        *failure_count = 0;
        if expected_pairing_reject {
            log::info!(
                "[solusdc_hedged_grid] 严格配对下预期拒单(忽略重启) id={} intent={:?} qty={:.6} price={:.6} err={}",
                draft.id,
                draft.intent,
                draft.qty,
                draft.price,
                error_text
            );
        } else {
            log::info!(
                "[solusdc_hedged_grid] post-only拒单已重试(不触发重启) id={} intent={:?} qty={:.6} price={:.6} err={}",
                draft.id,
                draft.intent,
                draft.qty,
                draft.price,
                error_text
            );
        }
    } else {
        *failure_count += 1;
        log::warn!(
            "[solusdc_hedged_grid] 下单失败 id={} intent={:?} side={:?} pos={} qty={:.6} price={:.6} post_only={} err={}",
            draft.id,
            draft.intent,
            draft.intent.side(),
            draft.intent.position_side().as_str(),
            draft.qty,
            draft.price,
            draft.post_only,
            error_text
        );
    }
}

fn extract_client_order_id(order: &Order) -> Option<String> {
    let keys = ["clientOrderId", "client_order_id", "clOrdId", "cl_ord_id"];
    for key in keys {
        if let Some(value) = order.info.get(key).and_then(|value| value.as_str()) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn enqueue_actions(
    cancel_queue: &mut VecDeque<EngineAction>,
    place_queue: &mut VecDeque<EngineAction>,
    actions: Vec<EngineAction>,
) {
    let canceled_ids: HashSet<String> = actions
        .iter()
        .filter_map(|action| match action {
            EngineAction::Cancel { order_id, .. } => Some(order_id.clone()),
            EngineAction::Place(_) => None,
        })
        .collect();

    for action in actions {
        match action {
            EngineAction::Cancel { .. } => cancel_queue.push_back(action),
            EngineAction::Place(draft) => {
                // 同一批动作里已经被取消的本地下单，不能再发到交易所；
                // 否则会出现“本地已删除、交易所仍挂着”的幽灵订单。
                if canceled_ids.contains(&draft.id) {
                    log::debug!(
                        "[solusdc_hedged_grid] 跳过同批次已取消挂单 id={} intent={:?}",
                        draft.id,
                        draft.intent
                    );
                    continue;
                }
                place_queue.push_back(EngineAction::Place(draft));
            }
        }
    }
}

fn pop_next_action(
    cancel_queue: &mut VecDeque<EngineAction>,
    place_queue: &mut VecDeque<EngineAction>,
) -> Option<EngineAction> {
    if let Some(action) = cancel_queue.pop_front() {
        return Some(action);
    }
    place_queue.pop_front()
}

fn drain_place_batch(
    first: OrderDraft,
    place_queue: &mut VecDeque<EngineAction>,
) -> Vec<OrderDraft> {
    let mut drafts = vec![first];
    while drafts.len() < MAX_BATCH_ORDERS {
        let Some(next_action) = place_queue.pop_front() else {
            break;
        };
        if let EngineAction::Place(next_draft) = next_action {
            drafts.push(next_draft);
        }
    }
    drafts
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::solusdc_hedged_grid::engine::OrderDraft;
    use crate::strategies::solusdc_hedged_grid::ledger::OrderIntent;

    #[test]
    fn enqueue_actions_should_skip_place_canceled_in_same_batch() {
        let mut cancel_queue = VecDeque::new();
        let mut place_queue = VecDeque::new();
        let actions = vec![
            EngineAction::Place(OrderDraft {
                id: "grid-1".to_string(),
                intent: OrderIntent::OpenLongBuy,
                price: 100.0,
                qty: 1.0,
                post_only: true,
            }),
            EngineAction::Cancel {
                order_id: "grid-1".to_string(),
                reason: "normalize".to_string(),
            },
            EngineAction::Place(OrderDraft {
                id: "grid-2".to_string(),
                intent: OrderIntent::OpenShortSell,
                price: 101.0,
                qty: 1.0,
                post_only: true,
            }),
        ];

        enqueue_actions(&mut cancel_queue, &mut place_queue, actions);

        assert_eq!(cancel_queue.len(), 1);
        assert_eq!(place_queue.len(), 1);
        let placed_id = match place_queue.front() {
            Some(EngineAction::Place(draft)) => draft.id.as_str(),
            _ => "",
        };
        assert_eq!(placed_id, "grid-2");
    }

    #[test]
    fn drain_place_batch_should_cap_at_five_orders() {
        let first = OrderDraft {
            id: "grid-0".to_string(),
            intent: OrderIntent::OpenLongBuy,
            price: 100.0,
            qty: 1.0,
            post_only: true,
        };
        let mut queue = VecDeque::new();
        for idx in 1..=7 {
            queue.push_back(EngineAction::Place(OrderDraft {
                id: format!("grid-{idx}"),
                intent: OrderIntent::OpenLongBuy,
                price: 100.0 + idx as f64,
                qty: 1.0,
                post_only: true,
            }));
        }

        let batch = drain_place_batch(first, &mut queue);

        assert_eq!(batch.len(), MAX_BATCH_ORDERS);
        assert_eq!(batch[0].id, "grid-0");
        assert_eq!(batch[4].id, "grid-4");
        assert_eq!(queue.len(), 3);
    }
}

fn build_order_request(
    config: &StrategyConfig,
    market_type: MarketType,
    draft: &super::engine::OrderDraft,
) -> OrderRequest {
    let mut params = HashMap::new();
    params.insert(
        "positionSide".to_string(),
        draft.intent.position_side().as_str().to_string(),
    );

    let mut request = OrderRequest::new(
        config.symbol.clone(),
        draft.intent.side(),
        OrderType::Limit,
        draft.qty,
        Some(draft.price),
        market_type,
    );
    request.client_order_id = Some(draft.id.clone());
    request.post_only = Some(draft.post_only);
    if draft.post_only {
        request.time_in_force = Some("GTX".to_string());
    }
    request.params = Some(params);
    request
}

async fn with_timeout<T, F>(operation: &'static str, timeout: Duration, fut: F) -> Result<T>
where
    F: Future<Output = Result<T, ExchangeError>>,
{
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result.map_err(|err| anyhow!(err)),
        Err(_) => Err(anyhow!(ExchangeError::TimeoutError {
            operation: operation.to_string(),
            timeout_seconds: timeout.as_secs().max(1),
        })),
    }
}

fn is_post_only_reject(err: &ExchangeError) -> bool {
    let msg = err.to_string();
    msg.contains("-5022")
        || msg.contains("-5021")
        || msg.contains("POST_ONLY_REJECT")
        || msg.to_ascii_lowercase().contains("post only")
}

fn is_order_reject(err: &ExchangeError) -> bool {
    if is_post_only_reject(err) {
        return true;
    }
    match err {
        ExchangeError::OrderError(_) => true,
        ExchangeError::ApiError { message, .. } => message.to_ascii_lowercase().contains("reject"),
        _ => err.to_string().to_ascii_lowercase().contains("rejected"),
    }
}

fn is_expected_strict_pairing_close_reject(
    config: &RuntimeConfig,
    draft: &super::engine::OrderDraft,
    err: &ExchangeError,
) -> bool {
    if !config.engine.grid.strict_pairing || !draft.intent.is_close() {
        return false;
    }
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("reduceonly")
        || msg.contains("reduce only")
        || msg.contains("-2022")
        || msg.contains("insufficient position")
        || msg.contains("not enough position")
        || msg.contains("position is zero")
        || msg.contains("position side does not match")
}

fn is_network_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        if let Some(exchange_err) = cause.downcast_ref::<ExchangeError>() {
            match exchange_err {
                ExchangeError::NetworkError(_) => true,
                ExchangeError::TimeoutError { .. } => true,
                ExchangeError::ApiError { code, message } => {
                    *code == -1007
                        || message
                            .to_ascii_lowercase()
                            .contains("timeout waiting for response")
                        || message.to_ascii_lowercase().contains("timeout")
                }
                _ => false,
            }
        } else {
            cause.to_string().contains("网络请求错误")
        }
    })
}

fn next_backoff(current: Duration, max: Duration) -> Duration {
    let next = current.saturating_mul(2);
    if next > max {
        max
    } else {
        next
    }
}

async fn restart_strategy(
    config: &RuntimeConfig,
    account: &AccountInfo,
    hedge_enabled: bool,
    request_timeout: Duration,
    engine: &mut Arc<Mutex<GridEngine>>,
    order_map: &mut OrderMap,
    initialized: &mut bool,
    last_reconcile: &mut chrono::DateTime<chrono::Utc>,
    last_zero_position_refresh: &mut chrono::DateTime<chrono::Utc>,
    last_hourly_reset: &mut chrono::DateTime<chrono::Utc>,
    failure_count: &mut u32,
    reason: &str,
) -> Result<()> {
    log::error!(
        "[solusdc_hedged_grid] 检测到拒单/失败({})，等待30秒后重启策略",
        reason
    );
    let _ = with_timeout(
        "cancel_all_orders_restart",
        request_timeout,
        account
            .exchange
            .cancel_all_orders(Some(&config.engine.symbol), config.account.market_type),
    )
    .await;
    *order_map = OrderMap::default();
    *initialized = false;
    *failure_count = 0;
    sleep(Duration::from_secs(30)).await;
    *engine = Arc::new(Mutex::new(
        GridEngine::new(config.engine.clone(), hedge_enabled).map_err(|err| anyhow!(err))?,
    ));
    *last_reconcile =
        Utc::now() - ChronoDuration::milliseconds(config.polling.reconcile_interval_ms as i64);
    *last_zero_position_refresh = Utc::now();
    *last_hourly_reset = Utc::now();
    Ok(())
}
