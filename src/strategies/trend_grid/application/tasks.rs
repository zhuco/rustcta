use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Duration;

use crate::analysis::TradeCollector;
use crate::core::error::ExchangeError;
use crate::core::types::{Interval, MarketType, OrderRequest, OrderSide, OrderStatus, OrderType};
use crate::cta::account_manager::AccountManager;
use crate::strategies::common::{StrategyRiskLimits, UnifiedRiskEvaluator};
use crate::strategies::trend_grid::application::risk;
use crate::strategies::trend_grid::domain::config::{
    BatchSettings, GridManagement, LoggingConfig, TradingConfig, TrendAdjustment, WebSocketConfig,
};
use crate::strategies::trend_grid::domain::state::{
    ConfigState, TrendAdjustmentRequest, TrendStrength,
};
use crate::strategies::trend_grid::infrastructure::{planner, services};
use crate::utils::indicators::TrendStrengthCalculator;
use crate::utils::{generate_order_id, generate_order_id_with_tag};
use tokio::task::JoinHandle;

pub(super) fn spawn_config_task(
    config: TradingConfig,
    account_manager: Arc<AccountManager>,
    config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
    running: Arc<RwLock<bool>>,
    trend_adjustment: TrendAdjustment,
    batch_settings: BatchSettings,
    websocket_config: WebSocketConfig,
    logging_config: LoggingConfig,
    grid_management: GridManagement,
    collector: Option<Arc<TradeCollector>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let config_id = config.config_id.clone();
        services::write_log(&config_id, "INFO", "启动策略线程");

        let mut retry_count = 0;
        let max_retries = 10;
        loop {
            match run_config_thread(
                config.clone(),
                account_manager.clone(),
                config_states.clone(),
                running.clone(),
                trend_adjustment.clone(),
                batch_settings.clone(),
                websocket_config.clone(),
                logging_config.clone(),
                grid_management.clone(),
                collector.clone(),
            )
            .await
            {
                Ok(_) => {
                    services::write_log(&config_id, "INFO", "策略线程正常退出");
                    break;
                }
                Err(e) => {
                    retry_count += 1;
                    let error_msg = format!(
                        "策略线程错误 (重试 {}/{}): {:?}",
                        retry_count, max_retries, e
                    );
                    services::write_log(&config_id, "ERROR", &error_msg);

                    if retry_count >= max_retries {
                        services::write_log(&config_id, "ERROR", "达到最大重试次数，策略停止");
                        break;
                    }

                    let wait_seconds = std::cmp::min(retry_count * 10, 60);
                    services::write_log(
                        &config_id,
                        "INFO",
                        &format!("等待{}秒后重试...", wait_seconds),
                    );
                    tokio::time::sleep(Duration::from_secs(wait_seconds as u64)).await;

                    if !*running.read().await {
                        services::write_log(&config_id, "INFO", "策略已停止，退出重试");
                        break;
                    }
                }
            }
        }
    })
}

pub(super) fn spawn_grid_check_task(
    interval_secs: u64,
    show_grid_status: bool,
    config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
    running: Arc<RwLock<bool>>,
    account_manager: Arc<AccountManager>,
    trading_configs: Vec<TradingConfig>,
    batch_settings: BatchSettings,
    trend_adjustment: TrendAdjustment,
    grid_management: GridManagement,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    risk_limits: StrategyRiskLimits,
    position_limit: f64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

        while *running.read().await {
            interval.tick().await;

            let states = config_states.read().await;
            if show_grid_status {
                log::debug!("检查 {} 个配置的网格状态", states.len());
            }

            for (config_id, state) in states.iter() {
                let snapshot = {
                    let guard = state.lock().await;
                    risk::build_risk_snapshot(
                        format!("trend_grid::{}", config_id),
                        &guard,
                        &risk_limits,
                        position_limit,
                    )
                };

                let decision = risk::evaluate_risk(&risk_evaluator, &snapshot).await;
                if let Err(e) =
                    risk::apply_risk_decision(config_id, decision, state, &running).await
                {
                    log::error!("风险评估失败: {}", e);
                    continue;
                }

                if !*running.read().await {
                    break;
                }

                let mut state_guard = state.lock().await;

                if planner::check_grid_uniformity(
                    &mut state_guard,
                    grid_management.rebalance_threshold,
                ) {
                    log::warn!("⚠️ {} 网格不均匀，标记需要重置", config_id);
                    state_guard.need_grid_reset = true;
                }

                if state_guard.need_grid_reset {
                    let time_since_reset = Utc::now() - state_guard.last_grid_check;
                    if time_since_reset.num_seconds() > 300 {
                        log::info!("🔄 {} 执行网格重置", config_id);
                        let config = state_guard.config.clone();
                        state_guard.last_grid_check = Utc::now();
                        drop(state_guard);

                        if let Err(e) = planner::reset_grid_for_config(
                            &config,
                            state,
                            account_manager.clone(),
                            &batch_settings,
                            &trend_adjustment,
                            &grid_management,
                        )
                        .await
                        {
                            log::error!("❌ {} 网格重置失败: {}", config_id, e);
                        }
                    } else {
                        log::debug!(
                            "⏳ {} 需要重置但距离上次重置时间过短（等待5分钟冷却）",
                            config_id
                        );
                    }
                }
            }
        }
    })
}

pub(super) fn spawn_trend_monitoring_task(
    config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
    running: Arc<RwLock<bool>>,
    account_manager: Arc<AccountManager>,
    trading_configs: Vec<TradingConfig>,
    batch_settings: BatchSettings,
    trend_adjustment: TrendAdjustment,
    grid_management: GridManagement,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    risk_limits: StrategyRiskLimits,
    position_limit: f64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval_timer = tokio::time::interval(Duration::from_secs(300));

        while *running.read().await {
            interval_timer.tick().await;

            log::info!("📊 开始趋势监控检查...");

            let states = config_states.read().await;
            for (config_id, state) in states.iter() {
                let config = match trading_configs.iter().find(|c| c.config_id == *config_id) {
                    Some(c) => c.clone(),
                    None => continue,
                };

                let snapshot = {
                    let guard = state.lock().await;
                    risk::build_risk_snapshot(
                        format!("trend_grid::{}", config_id),
                        &guard,
                        &risk_limits,
                        position_limit,
                    )
                };

                let decision = risk::evaluate_risk(&risk_evaluator, &snapshot).await;
                if let Err(e) =
                    risk::apply_risk_decision(config_id, decision, state, &running).await
                {
                    log::error!("风险评估失败: {}", e);
                    continue;
                }

                if !*running.read().await {
                    break;
                }

                let account = match account_manager.get_account(&config.account.id) {
                    Some(a) => a,
                    None => continue,
                };

                let interval = match Interval::from_string(&config.trend_config.timeframe) {
                    Ok(i) => i,
                    Err(e) => {
                        log::error!("❌ {} 无效的K线周期: {}", config_id, e);
                        continue;
                    }
                };

                let should_check_trend = {
                    let state_guard = state.lock().await;
                    let time_since_last_check = Utc::now() - state_guard.last_trend_check;
                    time_since_last_check.num_seconds() >= 300
                };

                if !should_check_trend {
                    continue;
                }

                match account
                    .exchange
                    .get_klines(&config.symbol, interval, MarketType::Futures, Some(100))
                    .await
                {
                    Ok(klines) => {
                        let mut state_guard = state.lock().await;
                        state_guard.last_trend_check = Utc::now();

                        for kline in klines.iter().rev().take(20) {
                            state_guard.trend_calculator.update(kline.close);
                        }

                        let latest_price = klines
                            .last()
                            .map(|k| k.close)
                            .unwrap_or(state_guard.current_price);
                        let new_trend_value = state_guard
                            .trend_calculator
                            .update(latest_price)
                            .unwrap_or(0.0);
                        let new_trend =
                            crate::utils::indicators::trend_strength_to_enum(new_trend_value);

                        if new_trend != state_guard.last_trend_strength {
                            log::warn!(
                                "📈 {} 趋势变化: {:?} -> {:?} (值: {:.3})",
                                config_id,
                                state_guard.last_trend_strength,
                                new_trend,
                                new_trend_value
                            );

                            state_guard.trend_strength = new_trend;
                            state_guard.last_trend_strength = new_trend;
                            state_guard.need_grid_reset = true;
                            let trend_strength = state_guard.trend_strength;
                            drop(state_guard);

                            let adjustment_request = match trend_strength {
                                TrendStrength::StrongBear => Some(TrendAdjustmentRequest {
                                    amount: 100.0,
                                    side: OrderSide::Sell,
                                    order_type: OrderType::Market,
                                }),
                                TrendStrength::Bear => Some(TrendAdjustmentRequest {
                                    amount: 50.0,
                                    side: OrderSide::Sell,
                                    order_type: OrderType::Market,
                                }),
                                TrendStrength::StrongBull => Some(TrendAdjustmentRequest {
                                    amount: 50.0,
                                    side: OrderSide::Buy,
                                    order_type: OrderType::Market,
                                }),
                                TrendStrength::Bull => Some(TrendAdjustmentRequest {
                                    amount: 20.0,
                                    side: OrderSide::Buy,
                                    order_type: OrderType::Market,
                                }),
                                _ => None,
                            };

                            if let Some(req) = adjustment_request {
                                log::info!(
                                    "📊 {} 因趋势变化需执行市价单: {:?} {:.1} {}",
                                    config_id,
                                    req.order_type,
                                    req.amount,
                                    req.side
                                );

                                if let Some(account) =
                                    account_manager.get_account(&config.account.id)
                                {
                                    let mut market_order = OrderRequest::new(
                                        config.symbol.clone(),
                                        req.side.clone(),
                                        req.order_type,
                                        req.amount,
                                        None,
                                        MarketType::Futures,
                                    );

                                    market_order.client_order_id =
                                        Some(generate_order_id_with_tag(
                                            "trend_grid_v2",
                                            &account.exchange_name,
                                            &format!("TREND_{}_{}", req.side, req.amount),
                                        ));

                                    if let Err(e) =
                                        account.exchange.create_order(market_order).await
                                    {
                                        log::error!(" ❌ {} 趋势市价单执行失败: {}", config_id, e);
                                    }
                                }

                                tokio::time::sleep(Duration::from_millis(200)).await;
                            }

                            if let Err(e) = planner::reset_grid_for_config(
                                &config,
                                state,
                                account_manager.clone(),
                                &batch_settings,
                                &trend_adjustment,
                                &grid_management,
                            )
                            .await
                            {
                                log::error!("❌ {} 趋势变化后网格重置失败: {}", config_id, e);
                            } else {
                                log::info!(
                                    "✅ {} 趋势变化后网格重置成功，下单金额已根据趋势调整",
                                    config_id
                                );
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("❌ {} 获取K线失败: {}", config_id, e);
                    }
                }
            }
        }

        log::info!("📊 趋势监控任务已停止");
    })
}

pub(super) async fn run_config_task_once(
    config: TradingConfig,
    account_manager: Arc<AccountManager>,
    config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
    running: Arc<RwLock<bool>>,
    trend_adjustment: TrendAdjustment,
    batch_settings: BatchSettings,
    websocket_config: WebSocketConfig,
    logging_config: LoggingConfig,
    grid_management: GridManagement,
    collector: Option<Arc<TradeCollector>>,
) -> Result<()> {
    run_config_thread(
        config,
        account_manager,
        config_states,
        running,
        trend_adjustment,
        batch_settings,
        websocket_config,
        logging_config,
        grid_management,
        collector,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_config_thread(
    config: TradingConfig,
    account_manager: Arc<AccountManager>,
    config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
    running: Arc<RwLock<bool>>,
    trend_adjustment: TrendAdjustment,
    batch_settings: BatchSettings,
    websocket_config: WebSocketConfig,
    logging_config: LoggingConfig,
    grid_management: GridManagement,
    collector: Option<Arc<TradeCollector>>,
) -> Result<()> {
    let config_id = config.config_id.clone();
    services::write_log(
        &config_id,
        "INFO",
        &format!("开始初始化配置: 交易对={}", config.symbol),
    );

    let account = account_manager
        .get_account(&config.account.id)
        .ok_or_else(|| {
            let err = format!("账户 {} 不存在", config.account.id);
            services::write_log(&config_id, "ERROR", &err);
            ExchangeError::Other(err)
        })?;

    services::write_log(&config_id, "INFO", "获取初始价格和精度...");

    let ticker = account
        .exchange
        .get_ticker(&config.symbol, MarketType::Futures)
        .await?;
    let initial_price = ticker.last;

    let symbol_info = account
        .exchange
        .get_symbol_info(&config.symbol, MarketType::Futures)
        .await?;
    let price_precision = planner::calculate_precision(symbol_info.tick_size);
    let amount_precision = planner::calculate_precision(symbol_info.step_size);

    log::info!(
        "📊 {} - {} 初始价格: {:.4}, 价格精度: {}, 数量精度: {}",
        config_id,
        config.symbol,
        initial_price,
        price_precision,
        amount_precision
    );

    let trend_calculator = TrendStrengthCalculator::new(
        config.trend_config.ma_fast as usize,
        config.trend_config.ma_slow as usize,
        config.trend_config.rsi_period as usize,
        12,
        26,
        9,
        20,
        2.0,
    );

    let (position_value, current_position) =
        match account.exchange.get_positions(Some(&config.symbol)).await {
            Ok(positions) => {
                let mut total_position = 0.0;
                let mut position_obj = None;
                for pos in &positions {
                    if pos.symbol == config.symbol {
                        let position_val = match pos.side.as_str() {
                            "LONG" => pos.contracts,
                            "SHORT" => -pos.contracts,
                            _ => pos.contracts,
                        };
                        total_position += position_val;

                        let side_str = match pos.side.as_str() {
                            "LONG" => "多",
                            "SHORT" => "空",
                            _ => {
                                if pos.contracts > 0.0 {
                                    "多"
                                } else {
                                    "空"
                                }
                            }
                        };

                        log::info!(
                            "📊 {} 现有持仓: {} {:.2} 张 @ 均价 {:.4} = {:.2} USDC",
                            config_id,
                            side_str,
                            pos.contracts.abs(),
                            pos.entry_price,
                            position_val
                        );

                        position_obj = Some(pos.clone());
                    }
                }
                (total_position, position_obj)
            }
            Err(e) => {
                log::warn!("⚠️ {} 获取持仓失败: {}", config_id, e);
                (0.0, None)
            }
        };

    let state = Arc::new(Mutex::new(ConfigState {
        config: config.clone(),
        current_price: initial_price,
        price_precision,
        amount_precision,
        grid_orders: HashMap::new(),
        active_orders: HashMap::new(),
        last_trade_price: initial_price,
        last_trade_time: Utc::now(),
        trend_strength: TrendStrength::Neutral,
        trend_calculator,
        position: position_value,
        pnl: 0.0,
        trades_count: 0,
        ws_client: None,
        total_buy_volume: 0.0,
        total_sell_volume: 0.0,
        total_buy_amount: 0.0,
        total_sell_amount: 0.0,
        total_fee: 0.0,
        net_position: current_position
            .as_ref()
            .map(|p| p.contracts)
            .unwrap_or(0.0),
        avg_buy_price: 0.0,
        avg_sell_price: 0.0,
        realized_pnl: 0.0,
        unrealized_pnl: 0.0,
        last_grid_check: Utc::now(),
        need_grid_reset: false,
        last_trend_check: Utc::now(),
        last_trend_strength: TrendStrength::Neutral,
    }));

    config_states
        .write()
        .await
        .insert(config_id.clone(), state.clone());

    services::write_log(&config_id, "INFO", "计算并提交初始网格订单...");
    planner::calculate_and_submit_grid(
        &config,
        &state,
        account_manager.clone(),
        &batch_settings,
        &trend_adjustment,
        &grid_management,
    )
    .await?;
    services::write_log(&config_id, "INFO", "初始网格订单提交成功");

    if websocket_config.subscribe_order_updates
        || websocket_config.subscribe_trade_updates
        || websocket_config.subscribe_ticker
    {
        services::write_log(&config_id, "INFO", "启动WebSocket监听...");
        if let Err(e) = services::start_websocket_for_config(
            &config,
            &state,
            account_manager.clone(),
            &websocket_config,
            config_states.clone(),
            &grid_management,
            &trend_adjustment,
            &batch_settings,
            &collector,
        )
        .await
        {
            services::write_log(
                &config_id,
                "WARN",
                &format!("WebSocket启动失败: {:?}, 将继续运行", e),
            );
        }
    }

    let mut interval = tokio::time::interval(Duration::from_secs(60));
    let mut loop_count = 0;

    while *running.read().await {
        interval.tick().await;
        loop_count += 1;

        if loop_count % 10 == 0 {
            services::write_log(
                &config_id,
                "INFO",
                &format!("策略运行中... (循环次数: {})", loop_count),
            );
        }

        if config.trend_config.show_trend_info {
            if let Err(e) =
                services::update_and_log_trend(&state, account_manager.clone(), &logging_config)
                    .await
            {
                services::write_log(&config_id, "WARN", &format!("更新趋势失败: {:?}", e));
            }
        }

        if grid_management.show_grid_status {
            if let Err(e) = services::log_grid_status(&state, &logging_config).await {
                services::write_log(&config_id, "WARN", &format!("显示网格状态失败: {:?}", e));
            }
        }

        if loop_count % 5 == 0 {
            if let Some(account) = account_manager.get_account(&config.account.id) {
                match account
                    .exchange
                    .get_open_orders(Some(&config.symbol), MarketType::Futures)
                    .await
                {
                    Ok(real_orders) => {
                        let mut state_guard = state.lock().await;
                        let expected_orders = config.grid.orders_per_side * 2;

                        state_guard.active_orders.clear();
                        state_guard.grid_orders.clear();

                        for order in &real_orders {
                            state_guard
                                .active_orders
                                .insert(order.id.clone(), order.clone());
                            state_guard
                                .grid_orders
                                .insert(order.id.clone(), order.clone());
                        }

                        let mut buy_orders = Vec::new();
                        let mut sell_orders = Vec::new();

                        for order in &real_orders {
                            if let Some(price) = order.price {
                                match order.side {
                                    OrderSide::Buy => buy_orders.push(price),
                                    OrderSide::Sell => sell_orders.push(price),
                                }
                            }
                        }

                        buy_orders.sort_by(|a, b| b.partial_cmp(a).unwrap());
                        sell_orders.sort_by(|a, b| a.partial_cmp(b).unwrap());

                        let need_rebuild = real_orders.len() != expected_orders as usize;

                        if need_rebuild {
                            services::write_log(
                                &config_id,
                                "WARN",
                                &format!(
                                    "网格需要重建 - 总订单: {}/{}, 买单: {}, 卖单: {}",
                                    real_orders.len(),
                                    expected_orders,
                                    buy_orders.len(),
                                    sell_orders.len()
                                ),
                            );
                            state_guard.need_grid_reset = true;
                        } else {
                            state_guard.need_grid_reset = false;
                        }

                        drop(state_guard);
                    }
                    Err(e) => {
                        services::write_log(
                            &config_id,
                            "ERROR",
                            &format!("获取实际订单失败: {:?}", e),
                        );
                    }
                }
            }

            let state_guard = state.lock().await;
            let active_orders_count = state_guard.active_orders.len();
            let need_reset = state_guard.need_grid_reset;
            drop(state_guard);

            let expected_orders = config.grid.orders_per_side * 2;
            if need_reset || active_orders_count < (expected_orders as usize / 2) {
                services::write_log(
                    &config_id,
                    "WARN",
                    &format!(
                        "需要重建网格 (need_reset={}, orders={}/{})",
                        need_reset, active_orders_count, expected_orders
                    ),
                );

                if let Some(account) = account_manager.get_account(&config.account.id) {
                    match account
                        .exchange
                        .cancel_all_orders(Some(&config.symbol), MarketType::Futures)
                        .await
                    {
                        Ok(cancelled) => {
                            services::write_log(
                                &config_id,
                                "INFO",
                                &format!("取消了 {} 个订单", cancelled.len()),
                            );
                        }
                        Err(e) => {
                            services::write_log(
                                &config_id,
                                "ERROR",
                                &format!("取消订单失败: {:?}", e),
                            );
                        }
                    }
                }

                let mut state_guard = state.lock().await;
                state_guard.active_orders.clear();
                state_guard.grid_orders.clear();
                state_guard.need_grid_reset = false;
                drop(state_guard);

                if let Err(e) = planner::calculate_and_submit_grid(
                    &config,
                    &state,
                    account_manager.clone(),
                    &batch_settings,
                    &trend_adjustment,
                    &grid_management,
                )
                .await
                {
                    services::write_log(&config_id, "ERROR", &format!("重建网格失败: {:?}", e));
                } else {
                    services::write_log(&config_id, "INFO", "网格重建成功");
                }
            }
        }
    }

    Ok(())
}

pub(super) type Result<T> = std::result::Result<T, ExchangeError>;
