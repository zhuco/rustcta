use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, RwLock as StdRwLock,
};

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use log::{debug, error, info, warn};
use reqwest::Client;
use serde_json::json;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};
use tokio::time::{sleep, Duration};

use crate::core::types::{
    MarketType, Order, OrderRequest, OrderSide, OrderStatus, OrderType, Position,
};
use crate::cta::account_manager::{AccountInfo, AccountManager};
use crate::exchanges::binance::BinanceExchange;
use crate::strategies::common::shared_data::{get_trend_inputs, SharedSymbolData};
use crate::strategies::common::{
    Strategy, StrategyDeps, StrategyInstance, StrategyPosition, StrategyState, StrategyStatus,
    UnifiedRiskEvaluator,
};

use super::config::{PartialTarget, StatusReportConfig, TrendConfig};
use super::entry::{CapitalAllocator, EntryMode, EntryOrderPlan, EntryPlanner};
use super::execution_engine::{ExecutionReport, SymbolPrecision, TrendExecutionEngine};
use super::indicator_service::TrendIndicatorService;
use super::market_feed::TrendMarketFeed;
use super::monitoring::TrendMonitor;
use super::order_tracker::{FillEvent, OrderTracker};
use super::position_manager::{
    AccountCapital, PositionManager, PositionsReport, TakeProfitLevel, TrendPosition,
};
use super::risk_control::RiskController;
use super::signal_generator::{
    SignalGenerator, SignalMetadata, SignalType, TakeProfit, TradeSignal,
};
use super::stop_manager::{StopManager, StopUpdate};
use super::trend_analyzer::{TrendAnalyzer, TrendDirection, TrendSignal};
use super::user_stream::TrendUserStream;

#[derive(Clone)]
pub struct TrendIntradayStrategy {
    config: TrendConfig,
    account_manager: Arc<AccountManager>,
    account: Arc<AccountInfo>,
    risk_evaluator: Arc<dyn UnifiedRiskEvaluator>,
    risk_controller: Arc<RiskController>,
    position_manager: Arc<RwLock<PositionManager>>,
    monitor: Arc<TrendMonitor>,
    execution_engine: Arc<TrendExecutionEngine>,
    market_feed: Arc<TrendMarketFeed>,
    indicator_service: Arc<TrendIndicatorService>,
    capital_allocator: Arc<CapitalAllocator>,
    entry_planner: EntryPlanner,
    order_tracker: Arc<OrderTracker>,
    user_stream: TrendUserStream,
    stop_manager: Arc<StopManager>,
    running: Arc<RwLock<bool>>,
    status: Arc<RwLock<StrategyStatus>>,
    task_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    last_signal_at: Arc<Mutex<HashMap<String, DateTime<Utc>>>>,
    daily_trade_counter: Arc<AtomicUsize>,
    bracket_registry: Arc<Mutex<HashSet<String>>>,
    bracket_handles: Arc<Mutex<HashMap<String, BracketOrderHandles>>>,
    symbol_precisions: Arc<StdRwLock<HashMap<String, SymbolPrecision>>>,
    account_capital: Arc<RwLock<AccountCapital>>,
    last_balance_log: Arc<RwLock<Option<DateTime<Utc>>>>,
    position_mode: Arc<RwLock<Option<bool>>>,
}

#[derive(Clone)]
struct SubmittedOrderHandle {
    symbol: String,
    order_id: String,
    client_order_id: String,
}

#[derive(Default, Clone)]
struct BracketOrderHandles {
    stop: Option<SubmittedOrderHandle>,
    take_profits: Vec<SubmittedOrderHandle>,
}

#[derive(Clone, Copy)]
enum ExitOrderKind {
    StopLoss,
    TakeProfit,
}

impl ExitOrderKind {
    fn label(&self) -> &'static str {
        match self {
            ExitOrderKind::StopLoss => "止损",
            ExitOrderKind::TakeProfit => "止盈",
        }
    }
}

impl TrendIntradayStrategy {
    fn new(config: TrendConfig, deps: StrategyDeps) -> Result<Self> {
        config
            .validate()
            .map_err(|err| anyhow!("趋势策略配置无效: {}", err))?;

        let account_manager = deps.account_manager.clone();
        let account = account_manager
            .get_account(&config.account_id)
            .ok_or_else(|| anyhow!("account {} not found", config.account_id))?;

        let status = StrategyStatus::new(config.name.clone());

        let mut stop_manager = StopManager::new(config.stop_config.clone());
        stop_manager.set_account_manager(account_manager.clone());

        let risk_controller = Arc::new(RiskController::new(config.risk_config.clone()));
        let order_tracker = OrderTracker::new();
        let market_feed = Arc::new(TrendMarketFeed::new(
            config.symbols.clone(),
            account.exchange.clone(),
            config.market_data.clone(),
        ));
        let indicator_service = Arc::new(TrendIndicatorService::new(
            config.symbols.clone(),
            market_feed.candle_cache(),
            config.indicator_config.clone(),
            config.market_data.clone(),
        ));
        let capital_allocator = Arc::new(CapitalAllocator::new(
            config.entry_leverage,
            config.entry_allocation.clone(),
            config.allocation_regimes.clone(),
            config.entry_base_notional,
        ));
        let entry_planner = EntryPlanner::new(config.entry_price_improve_bps);
        let symbol_precisions = Arc::new(StdRwLock::new(HashMap::new()));
        let execution_engine = Arc::new(TrendExecutionEngine::new(
            account_manager.clone(),
            config.account_id.clone(),
            config.execution.clone(),
            order_tracker.clone(),
            symbol_precisions.clone(),
        ));
        let user_stream = TrendUserStream::new(
            account.clone(),
            order_tracker.clone(),
            risk_controller.clone(),
        );

        Ok(Self {
            execution_engine,
            config: config.clone(),
            account_manager: account_manager.clone(),
            account,
            risk_evaluator: deps.risk_evaluator.clone(),
            risk_controller,
            position_manager: Arc::new(RwLock::new(PositionManager::new(
                config.position_config.clone(),
                config.symbol_inventory_limits.clone(),
                config.symbol_inventory_value_limits.clone(),
                config.risk_config.max_leverage,
            ))),
            monitor: Arc::new(TrendMonitor::new()),
            market_feed,
            indicator_service,
            capital_allocator,
            entry_planner,
            order_tracker,
            user_stream,
            stop_manager: Arc::new(stop_manager),
            running: Arc::new(RwLock::new(false)),
            status: Arc::new(RwLock::new(status)),
            task_handles: Arc::new(Mutex::new(Vec::new())),
            last_signal_at: Arc::new(Mutex::new(HashMap::new())),
            daily_trade_counter: Arc::new(AtomicUsize::new(0)),
            bracket_registry: Arc::new(Mutex::new(HashSet::new())),
            bracket_handles: Arc::new(Mutex::new(HashMap::new())),
            symbol_precisions,
            account_capital: Arc::new(RwLock::new(AccountCapital::default())),
            last_balance_log: Arc::new(RwLock::new(None)),
            position_mode: Arc::new(RwLock::new(None)),
        })
    }

    async fn run_symbol_loop(&self, symbol: String) {
        let mut analyzer = TrendAnalyzer::new(
            self.config.indicator_config.clone(),
            self.config.scoring.clone(),
        );
        analyzer.set_account_manager(self.account_manager.clone());

        let mut signal_generator = SignalGenerator::new(self.config.signal_config.clone());
        signal_generator.set_account_manager(self.account_manager.clone());

        let poll_interval = Duration::from_secs(self.config.poll_interval_secs.max(1).min(60 * 5));

        info!(
            "启动 {} 日内趋势任务，轮询间隔 {}s",
            symbol,
            poll_interval.as_secs()
        );

        loop {
            if !self.is_running().await {
                break;
            }

            match self
                .process_symbol_once(&symbol, &mut analyzer, &signal_generator)
                .await
            {
                Ok(_) => {}
                Err(err) => {
                    warn!("处理 {} 周期失败: {}", symbol, err);
                }
            }

            sleep(poll_interval).await;
        }

        info!("{} 日内趋势任务停止", symbol);
    }

    async fn process_symbol_once(
        &self,
        symbol: &str,
        analyzer: &mut TrendAnalyzer,
        signal_generator: &SignalGenerator,
    ) -> Result<()> {
        let Some(shared) = get_trend_inputs(symbol) else {
            debug!("{} 暂无共享指标数据，跳过本轮", symbol);
            return Ok(());
        };

        if shared.is_stale(ChronoDuration::minutes(5)) {
            warn!("{} 共享数据超过5分钟未更新，跳过本轮", symbol);
            return Ok(());
        }

        if shared.data_quality() < 55.0 {
            warn!(
                "{} 数据质量 {:.1} 过低，跳过信号处理",
                symbol,
                shared.data_quality()
            );
            return Ok(());
        }

        if self.in_cooldown(symbol).await {
            debug!("{} 正在信号冷却窗口内，跳过", symbol);
            return Ok(());
        }

        {
            let manager = self.position_manager.read().await;
            if manager.total_open_positions() >= self.config.max_positions {
                debug!(
                    "已达到最大持仓数({})，跳过 {}",
                    self.config.max_positions, symbol
                );
                return Ok(());
            }
        }

        let trend_signal = analyzer
            .analyze(symbol)
            .await
            .map_err(|e| anyhow!("趋势分析失败: {}", e))?;

        let mut trade_signal = match signal_generator.generate(symbol, &trend_signal).await {
            Ok(Some(sig)) => sig,
            Ok(None) => match self.build_fallback_signal(symbol, &trend_signal, &shared) {
                Some(sig) => {
                    warn!("{} 使用回退信号驱动下单", symbol);
                    sig
                }
                None => return Ok(()),
            },
            Err(err) => return Err(anyhow!("信号生成失败: {}", err)),
        };

        let dual_mode = self.is_dual_position_mode().await;
        {
            let manager = self.position_manager.read().await;
            if manager.has_open_position_side(symbol, trade_signal.side) {
                debug!(
                    "{} 已有同方向持仓({:?})，跳过重复入场",
                    symbol, trade_signal.side
                );
                return Ok(());
            }

            if !dual_mode && manager.has_open_position(symbol) {
                debug!("{} 当前为单向模式且已有持仓，跳过本轮信号", symbol);
                return Ok(());
            }
        }

        let capital = self.refresh_account_metrics().await?;
        if capital.available <= 0.0 {
            return Err(anyhow!("账户可用余额为0，无法计算仓位"));
        }

        let mut planned_notional = self
            .config
            .symbol_entry_notional
            .get(symbol)
            .copied()
            .unwrap_or_else(|| self.capital_allocator.planned_notional(capital.total));

        if let Some(multiplier) = self.config.max_trend_notional_multiplier {
            let cap_notional = capital.total * multiplier;
            let current_notional = self.current_trend_notional().await;
            let remaining_notional = (cap_notional - current_notional).max(0.0);
            if remaining_notional <= self.min_notional(symbol) {
                warn!(
                    "{} 趋势总名义已达上限: current={:.2}, cap={:.2}, skip",
                    symbol, current_notional, cap_notional
                );
                return Ok(());
            }
            if planned_notional > remaining_notional {
                info!(
                    "{} 入场名义从 {:.2} 调整到剩余额度 {:.2}",
                    symbol, planned_notional, remaining_notional
                );
                planned_notional = remaining_notional;
            }
        }

        let raw_risk_size = {
            let manager = self.position_manager.read().await;
            manager
                .calculate_position_size(&trade_signal, capital, Some(planned_notional))
                .map_err(|e| anyhow!("计算仓位失败: {}", e))?
        };

        if raw_risk_size <= f64::EPSILON {
            debug!("{} 仓位 sizing 结果过小，放弃入场", symbol);
            return Ok(());
        }

        let risk_notional = raw_risk_size * trade_signal.entry_price;
        let mut plan_notional = risk_notional;
        let min_notional = self.min_notional(symbol);

        if plan_notional < min_notional {
            debug!(
                "{} 规划名义 {:.2} 低于 minNotional {:.2}",
                symbol, plan_notional, min_notional
            );
            plan_notional = risk_notional;
        }

        if plan_notional < min_notional {
            warn!("{} 资金规划与风险上限均低于交易所最小名义，跳过", symbol);
            return Ok(());
        }

        let allocation =
            self.capital_allocator
                .allocate(shared.regime(), plan_notional, min_notional);

        if allocation.per_mode.is_empty() {
            warn!("{} 资金分配为空: {:?}", symbol, allocation.rejected);
            return Ok(());
        }

        if !allocation.adjustments.is_empty() {
            for adj in &allocation.adjustments {
                debug!("{} allocation 调整: {}", symbol, adj);
            }
        }

        let entry_plans = self
            .entry_planner
            .build_orders(&trade_signal, &shared, &allocation);

        if entry_plans.is_empty() {
            warn!("{} 无有效入场价格，跳过", symbol);
            return Ok(());
        }

        for plan in &entry_plans {
            debug!(
                "{} [{}] target {:.4}, notional {:.2} ({:.1}%)",
                symbol,
                plan.mode.label(),
                plan.price,
                plan.notional,
                plan.weight * 100.0
            );
        }

        let position_size: f64 = entry_plans
            .iter()
            .map(|plan| plan.notional / plan.price.max(1e-6))
            .sum();

        if position_size <= f64::EPSILON {
            warn!("{} 切分后仓位为0，跳过", symbol);
            return Ok(());
        }

        if position_size <= f64::EPSILON {
            debug!("{} 计算仓位结果过小，忽略", symbol);
            return Ok(());
        }

        if !self
            .risk_controller
            .approve_trade(&trade_signal, position_size)
            .await?
        {
            warn!("{} 风控拒绝开仓", symbol);
            return Ok(());
        }

        let execution_report = match self
            .execute_entry_plans(symbol, &trade_signal, entry_plans)
            .await?
        {
            Some(report) => report,
            None => {
                warn!("{} 批量挂单未获得成交，结束本轮", symbol);
                return Ok(());
            }
        };

        let executed_qty = execution_report.filled_qty;
        if executed_qty <= f64::EPSILON {
            warn!("{} 返回的成交数量为0，放弃记录持仓", symbol);
            return Ok(());
        }

        let entry_side = trade_signal.side;
        trade_signal.entry_price = execution_report.average_price;

        let bracket_handles = self
            .submit_bracket_orders(
                &trade_signal.symbol,
                trade_signal.side,
                execution_report.average_price,
                executed_qty,
                &execution_report.order.id,
                shared
                    .trend_snapshot
                    .as_ref()
                    .map(|s| s.atr_5m)
                    .or(Some(shared.indicators.atr)),
            )
            .await?;

        self.risk_controller
            .update_order_metrics(
                execution_report.slippage,
                execution_report.execution_time_ms,
                true,
            )
            .await;

        let positions_report = {
            let mut manager = self.position_manager.write().await;
            let mut filled_signal = trade_signal.clone();
            filled_signal.entry_price = execution_report.average_price;
            manager.add_position(filled_signal, executed_qty);
            manager.update_position_pnl_side(symbol, entry_side, execution_report.average_price)?;
            manager.get_positions_report()
        };

        self.daily_trade_counter.fetch_add(1, Ordering::Relaxed);

        self.risk_controller
            .update_strategy_metrics(
                positions_report.open_positions,
                self.daily_trade_counter.load(Ordering::Relaxed),
                0,
            )
            .await;

        self.update_status_with_positions(&positions_report).await;

        self.last_signal_at
            .lock()
            .await
            .insert(symbol.to_string(), Utc::now());

        {
            let mut map = self.bracket_handles.lock().await;
            map.insert(
                Self::position_slot_key(symbol, entry_side),
                bracket_handles.clone(),
            );
        }

        self.spawn_bracket_listeners(symbol.to_string(), entry_side, bracket_handles)
            .await;

        info!(
            "{} 成功提交订单，最新持仓数: {}",
            symbol, positions_report.open_positions
        );

        Ok(())
    }

    async fn update_status_with_positions(&self, report: &PositionsReport) {
        let mut status = self.status.write().await;
        status.positions = report
            .positions
            .iter()
            .map(|p| StrategyPosition {
                symbol: p.symbol.clone(),
                net_position: match p.side {
                    OrderSide::Buy => p.current_size,
                    OrderSide::Sell => -p.current_size,
                },
                notional: p.current_size * p.current_price.max(1e-8),
            })
            .collect();
        status.updated_at = Utc::now();
    }

    async fn current_trend_notional(&self) -> f64 {
        let positions = {
            let manager = self.position_manager.read().await;
            manager.get_positions_report().positions
        };

        positions
            .iter()
            .map(|position| position.current_size * position.current_price.abs())
            .sum()
    }

    async fn refresh_account_metrics(&self) -> Result<AccountCapital> {
        let exchange = self.account.exchange.clone();
        let balances = exchange
            .get_balance(MarketType::Futures)
            .await
            .map_err(|e| anyhow!("获取账户余额失败: {}", e))?;

        let total: f64 = balances.iter().map(|b| b.total).sum();
        let free: f64 = balances.iter().map(|b| b.free).sum();
        let used: f64 = balances.iter().map(|b| b.used).sum();

        self.risk_controller
            .update_account_metrics(total, free, used, 0.0)
            .await;

        let capital = AccountCapital {
            total: total.max(0.0),
            available: free.max(0.0),
        };

        {
            let mut guard = self.account_capital.write().await;
            *guard = capital;
        }

        let now = Utc::now();
        let mut should_log = false;
        {
            let mut last = self.last_balance_log.write().await;
            if last
                .map(|ts| now - ts >= ChronoDuration::minutes(5))
                .unwrap_or(true)
            {
                *last = Some(now);
                should_log = true;
            }
        }

        if should_log {
            info!(
                "账户资金快照: total={:.2} available={:.2} used={:.2}",
                capital.total, capital.available, used
            );
        } else {
            debug!(
                "账户资金(压抑日志): total={:.2} available={:.2} used={:.2}",
                capital.total, capital.available, used
            );
        }

        Ok(capital)
    }

    async fn spawn_housekeeping_task(&self) {
        let strategy = self.clone();
        let interval = Duration::from_secs(std::cmp::max(30, self.config.poll_interval_secs));

        let handle = tokio::spawn(async move {
            loop {
                if !strategy.is_running().await {
                    break;
                }

                if let Err(err) = strategy.refresh_account_metrics().await {
                    warn!("刷新账户指标失败: {}", err);
                }

                if let Err(err) = strategy.refresh_active_stops().await {
                    warn!("刷新止损失败: {}", err);
                }

                if let Err(err) = strategy.cleanup_dust_positions().await {
                    warn!("清理尾仓失败: {}", err);
                }

                sleep(interval).await;
            }
        });

        self.task_handles.lock().await.push(handle);
    }

    async fn spawn_protection_guard_task(&self) {
        let strategy = self.clone();
        let interval = Duration::from_secs(300);

        let handle = tokio::spawn(async move {
            sleep(Duration::from_secs(30)).await;
            loop {
                if !strategy.is_running().await {
                    break;
                }

                if let Err(err) = strategy.sync_positions_and_protections().await {
                    warn!("同步持仓与保护失败: {}", err);
                }

                sleep(interval).await;
            }
        });

        self.task_handles.lock().await.push(handle);
    }

    async fn spawn_status_report_task(&self) {
        let cfg = self.config.status_reporter.clone();
        if !cfg.enabled || cfg.webhook_url.trim().is_empty() {
            return;
        }

        let strategy = self.clone();
        let interval = Duration::from_secs(cfg.interval_secs.max(60));
        let webhook = cfg.webhook_url.clone();
        let handle = tokio::spawn(async move {
            let client = Client::new();
            loop {
                if !strategy.is_running().await {
                    break;
                }

                if let Err(err) = strategy.send_status_report(&client, &webhook).await {
                    warn!("发送余额/持仓推送失败: {}", err);
                }

                sleep(interval).await;
            }
        });

        self.task_handles.lock().await.push(handle);
    }

    fn order_position_side(order: &Order) -> Option<&str> {
        order
            .info
            .get("positionSide")
            .and_then(|value| value.as_str())
            .or_else(|| {
                order
                    .info
                    .get("positionSide")
                    .and_then(|value| value.get(0))
                    .and_then(|value| value.as_str())
            })
    }

    fn order_reduce_only(order: &Order) -> bool {
        order
            .info
            .get("reduceOnly")
            .and_then(|value| value.as_bool())
            .unwrap_or(false)
    }

    fn order_remaining_qty(order: &Order) -> f64 {
        if order.remaining > f64::EPSILON {
            order.remaining
        } else {
            (order.amount - order.filled).max(0.0)
        }
    }

    fn is_same_position_exit_order(order: &Order, position: &TrendPosition) -> bool {
        let exit_side = match position.side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };
        order.side == exit_side
            && Self::order_position_side(order) == Some(Self::position_side_label(position.side))
            && Self::order_reduce_only(order)
    }

    fn protection_status_for_position(
        position: &TrendPosition,
        orders: &[Order],
    ) -> (bool, bool, f64, f64) {
        let mut stop_qty = 0.0;
        let mut take_profit_qty = 0.0;

        for order in orders {
            if !Self::is_same_position_exit_order(order, position) {
                continue;
            }

            let qty = Self::order_remaining_qty(order);
            match order.order_type {
                OrderType::StopMarket | OrderType::StopLimit => stop_qty += qty,
                OrderType::TakeProfitLimit | OrderType::TakeProfitMarket | OrderType::Limit => {
                    take_profit_qty += qty
                }
                _ => {}
            }
        }

        let required = position.current_size.max(0.0);
        let tolerance = (required * 0.002).max(1e-6);
        (
            stop_qty + tolerance >= required,
            take_profit_qty + tolerance >= required,
            stop_qty,
            take_profit_qty,
        )
    }

    async fn sync_positions_and_protections(&self) -> Result<()> {
        let exchange_positions = self
            .account
            .exchange
            .get_positions(None)
            .await
            .map_err(|e| anyhow!("获取交易所持仓失败: {}", e))?;
        let open_orders = self
            .account
            .exchange
            .get_open_orders(None, MarketType::Futures)
            .await
            .map_err(|e| anyhow!("获取交易所挂单失败: {}", e))?;

        let mut orders_by_symbol: HashMap<String, Vec<Order>> = HashMap::new();
        for order in open_orders {
            if let Some(symbol) = self.match_config_symbol(&order.symbol) {
                orders_by_symbol.entry(symbol).or_default().push(order);
            }
        }

        let mut exchange_snapshot: HashSet<String> = HashSet::new();
        for pos in &exchange_positions {
            if let Some(symbol) = self.match_config_symbol(&pos.symbol) {
                let signed_size = Self::signed_position_amount(pos);
                if signed_size.abs() <= f64::EPSILON {
                    continue;
                }
                let side = if signed_size > 0.0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                };
                exchange_snapshot.insert(Self::position_slot_key(&symbol, side));
            }
        }

        let mut restored_positions: Vec<TrendPosition> = Vec::new();
        {
            let mut manager = self.position_manager.write().await;
            for pos in &exchange_positions {
                let Some(symbol) = self.match_config_symbol(&pos.symbol) else {
                    continue;
                };
                let signed_size = Self::signed_position_amount(pos);
                if signed_size.abs() <= f64::EPSILON {
                    continue;
                }
                let side = if signed_size > 0.0 {
                    OrderSide::Buy
                } else {
                    OrderSide::Sell
                };
                if !manager.has_open_position_side(&symbol, side) {
                    let restored = self.build_restored_position(
                        symbol.clone(),
                        side,
                        signed_size.abs(),
                        pos.entry_price.max(1e-6),
                        pos.mark_price.max(1e-6),
                        pos.timestamp,
                        pos.unrealized_pnl,
                    );
                    manager.restore_position(restored.clone());
                    restored_positions.push(restored);
                }
            }
        }

        if !restored_positions.is_empty() {
            info!("同步接管 {} 个未登记仓位", restored_positions.len());
        }

        for restored in restored_positions {
            let handles = self
                .submit_bracket_orders(
                    &restored.symbol,
                    restored.side,
                    restored.entry_price,
                    restored.current_size,
                    &format!("sync_{}", Utc::now().timestamp_micros()),
                    None,
                )
                .await?;
            {
                let mut map = self.bracket_handles.lock().await;
                map.insert(
                    Self::position_slot_key(&restored.symbol, restored.side),
                    handles.clone(),
                );
            }
            self.spawn_bracket_listeners(restored.symbol.clone(), restored.side, handles)
                .await;
        }

        let mut positions = {
            let manager = self.position_manager.read().await;
            manager.get_positions_report().positions
        };

        // 清理在交易所不存在的本地仓位
        let mut stale_positions = Vec::new();
        for position in positions.iter() {
            if position.current_size <= f64::EPSILON {
                continue;
            }
            if !exchange_snapshot
                .contains(&Self::position_slot_key(&position.symbol, position.side))
            {
                stale_positions.push(position.clone());
            }
        }

        if !stale_positions.is_empty() {
            {
                let mut manager = self.position_manager.write().await;
                for stale in &stale_positions {
                    manager.close_position_with_fill_side(
                        &stale.symbol,
                        stale.side,
                        stale.current_price.max(1e-6),
                    );
                }
            }
            for stale in &stale_positions {
                let key = Self::position_slot_key(&stale.symbol, stale.side);
                self.cancel_tracked_bracket_orders(&key, None).await;
                info!(
                    "{} {:?} 本地仓位已清理（交易所无仓）",
                    stale.symbol, stale.side
                );
            }

            positions = {
                let manager = self.position_manager.read().await;
                manager.get_positions_report().positions
            };
        }

        for position in positions {
            if position.current_size <= f64::EPSILON {
                continue;
            }

            let orders = orders_by_symbol
                .get(&position.symbol)
                .cloned()
                .unwrap_or_default();

            let (has_stop, has_take_profit, stop_qty, take_profit_qty) =
                Self::protection_status_for_position(&position, &orders);

            if has_stop && has_take_profit {
                debug!(
                    "{} {} 保护单完整: stop_qty={:.8}, tp_qty={:.8}, pos_qty={:.8}",
                    position.symbol,
                    Self::position_side_label(position.side),
                    stop_qty,
                    take_profit_qty,
                    position.current_size
                );
                continue;
            }

            warn!(
                "{} {} 缺少{}: stop_qty={:.8}, tp_qty={:.8}, pos_qty={:.8}，重新挂保护单",
                position.symbol,
                Self::position_side_label(position.side),
                match (has_stop, has_take_profit) {
                    (false, false) => "止损与止盈",
                    (false, true) => "止损",
                    (true, false) => "止盈",
                    _ => "保护单",
                },
                stop_qty,
                take_profit_qty,
                position.current_size
            );

            self.cancel_exit_orders_for_position(&position.symbol, &orders, &position)
                .await;

            let handles = self
                .submit_bracket_orders(
                    &position.symbol,
                    position.side,
                    position.average_price,
                    position.current_size,
                    &format!("guard_{}", Utc::now().timestamp_micros()),
                    None,
                )
                .await?;
            {
                let mut map = self.bracket_handles.lock().await;
                map.insert(
                    Self::position_slot_key(&position.symbol, position.side),
                    handles.clone(),
                );
            }
            self.spawn_bracket_listeners(position.symbol.clone(), position.side, handles)
                .await;
        }

        Ok(())
    }

    async fn cancel_exit_orders_for_position(
        &self,
        symbol: &str,
        orders: &[Order],
        position: &TrendPosition,
    ) {
        if orders.is_empty() {
            return;
        }

        for order in orders {
            if !Self::is_same_position_exit_order(order, position) {
                continue;
            }
            if let Err(err) = self
                .account
                .exchange
                .cancel_order(&order.id, symbol, MarketType::Futures)
                .await
            {
                warn!(
                    "{} {} 取消旧保护单 {} 失败: {}",
                    symbol,
                    Self::position_side_label(position.side),
                    order.id,
                    err
                );
            } else {
                info!(
                    "{} {} 已取消旧保护单 {}",
                    symbol,
                    Self::position_side_label(position.side),
                    order.id
                );
            }
        }
    }

    async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    async fn in_cooldown(&self, symbol: &str) -> bool {
        let cooldown = ChronoDuration::seconds(self.config.signal_cooldown_secs as i64);
        let last = self.last_signal_at.lock().await.get(symbol).cloned();
        if let Some(last_time) = last {
            return Utc::now() - last_time < cooldown;
        }
        false
    }

    async fn refresh_symbol_precisions(&self) -> Result<()> {
        let mut latest: HashMap<String, SymbolPrecision> = HashMap::new();

        for symbol in &self.config.symbols {
            match self
                .account
                .exchange
                .get_symbol_info(symbol, MarketType::Futures)
                .await
            {
                Ok(info) => {
                    info!(
                        "{} 精度: step_size={}, tick_size={}",
                        symbol, info.step_size, info.tick_size
                    );
                    latest.insert(symbol.clone(), SymbolPrecision::from_trading_pair(&info));
                }
                Err(err) => {
                    warn!("获取 {} 精度失败: {}", symbol, err);
                }
            }
        }

        if latest.is_empty() {
            bail!("无法从交易所获取任何交易对精度");
        }

        if let Ok(mut guard) = self.symbol_precisions.write() {
            *guard = latest;
        }

        info!("已刷新 {} 个交易对精度", self.config.symbols.len());
        Ok(())
    }

    async fn is_dual_position_mode(&self) -> bool {
        if let Some(value) = *self.position_mode.read().await {
            return value;
        }

        if self.config.dual_position_mode {
            let mut guard = self.position_mode.write().await;
            *guard = Some(true);
            return true;
        }

        let mut detected = false;
        if let Some(binance) = self
            .account
            .exchange
            .as_ref()
            .as_any()
            .downcast_ref::<BinanceExchange>()
        {
            match binance.get_position_mode().await {
                Ok(mode) => {
                    detected = mode;
                }
                Err(err) => {
                    warn!("获取Binance持仓模式失败，默认按单向持仓处理: {}", err);
                }
            }
        }

        let mut guard = self.position_mode.write().await;
        *guard = Some(detected);
        detected
    }

    async fn cancel_open_orders(&self) {
        let exchange = self.account.exchange.clone();
        for symbol in &self.config.symbols {
            match exchange
                .cancel_all_orders(Some(symbol.as_str()), MarketType::Futures)
                .await
            {
                Ok(_) => info!("已取消 {} 的未完成委托", symbol),
                Err(err) => warn!("取消 {} 订单失败: {}", symbol, err),
            }
        }
    }

    fn build_fallback_signal(
        &self,
        symbol: &str,
        trend_signal: &TrendSignal,
        shared: &SharedSymbolData,
    ) -> Option<TradeSignal> {
        if trend_signal.data_quality_score < 70.0 {
            return None;
        }

        if trend_signal.confidence < self.config.min_signal_confidence {
            return None;
        }

        if !trend_signal.timeframe_aligned {
            return None;
        }

        let price = shared.indicators.last_price;
        if price <= 0.0 {
            return None;
        }

        let side = match trend_signal.direction {
            TrendDirection::StrongBullish | TrendDirection::Bullish => OrderSide::Buy,
            TrendDirection::StrongBearish | TrendDirection::Bearish => OrderSide::Sell,
            TrendDirection::Neutral => self.derive_side_from_snapshot(shared),
        };

        let stop_offset = 0.004;
        let target_offset = 0.008;
        let entry_price = price;
        let stop_loss = match side {
            OrderSide::Buy => price * (1.0 - stop_offset),
            OrderSide::Sell => price * (1.0 + stop_offset),
        };
        let take_price = match side {
            OrderSide::Buy => price * (1.0 + target_offset),
            OrderSide::Sell => price * (1.0 - target_offset),
        };

        let risk = (entry_price - stop_loss).abs();
        if risk <= f64::EPSILON {
            return None;
        }
        let reward = (take_price - entry_price).abs();

        Some(TradeSignal {
            symbol: symbol.to_string(),
            signal_type: SignalType::MomentumSurge,
            side,
            entry_price,
            stop_loss,
            take_profits: vec![TakeProfit {
                price: take_price,
                ratio: 1.0,
            }],
            suggested_size: (12.0 / entry_price).max(0.0),
            risk_reward_ratio: (reward / risk).max(0.5),
            confidence: trend_signal.confidence.max(65.0),
            timeframe_aligned: trend_signal.timeframe_aligned,
            has_structure_support: false,
            expire_time: Utc::now() + ChronoDuration::seconds(60),
            metadata: SignalMetadata {
                trend_strength: trend_signal.strength,
                volume_confirmed: true,
                key_level_nearby: false,
                pattern_name: Some("fallback".to_string()),
                generated_at: Utc::now(),
            },
        })
    }

    fn derive_side_from_snapshot(&self, shared: &SharedSymbolData) -> OrderSide {
        let closes: Vec<f64> = shared
            .snapshot
            .one_minute
            .iter()
            .rev()
            .take(20)
            .map(|k| k.close)
            .collect();
        if closes.len() >= 2 {
            let first = closes.last().copied().unwrap_or(0.0);
            let last = closes.first().copied().unwrap_or(0.0);
            if last > first {
                return OrderSide::Buy;
            } else if last < first {
                return OrderSide::Sell;
            }
        }
        OrderSide::Buy
    }

    async fn submit_bracket_orders(
        &self,
        symbol: &str,
        side: OrderSide,
        entry_price: f64,
        quantity: f64,
        order_id: &str,
        atr_hint: Option<f64>,
    ) -> Result<BracketOrderHandles> {
        if quantity <= 0.0 || entry_price <= 0.0 {
            return Ok(BracketOrderHandles::default());
        }

        let key = format!("{}-{}", symbol, order_id);
        if !self.mark_bracket_registration(&key).await {
            return Ok(BracketOrderHandles::default());
        }

        let stop_price =
            self.stop_manager
                .calculate_initial_stop(entry_price, side.clone(), atr_hint);
        let risk = (entry_price - stop_price).abs();
        if risk <= f64::EPSILON {
            return Ok(BracketOrderHandles::default());
        }

        let target_price = match side {
            OrderSide::Buy => entry_price + risk * 2.0,
            OrderSide::Sell => entry_price - risk * 2.0,
        };

        let qty = self.execution_engine.format_amount(symbol, quantity);
        if qty <= 0.0 {
            return Ok(BracketOrderHandles::default());
        }

        let formatted_stop = self.execution_engine.format_price(symbol, stop_price);
        let formatted_target = self.execution_engine.format_price(symbol, target_price);
        let formatted_stop_str = self
            .execution_engine
            .format_price_string(symbol, formatted_stop);
        let formatted_target_str = self
            .execution_engine
            .format_price_string(symbol, formatted_target);

        let exit_side = match side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };
        let position_side = match side {
            OrderSide::Buy => "LONG",
            OrderSide::Sell => "SHORT",
        };
        let include_position_side = self.is_dual_position_mode().await;

        let mut stop_params = HashMap::new();
        stop_params.insert("stopPrice".to_string(), formatted_stop_str.clone());
        stop_params.insert("workingType".to_string(), "MARK_PRICE".to_string());
        if include_position_side {
            stop_params.insert("positionSide".to_string(), position_side.to_string());
        }

        let mut stop_request = OrderRequest::new(
            symbol.to_string(),
            exit_side.clone(),
            OrderType::StopMarket,
            qty,
            None,
            MarketType::Futures,
        );
        stop_request.params = Some(stop_params);
        stop_request.reduce_only = if include_position_side {
            None
        } else {
            Some(true)
        };
        let stop_client_id = Self::build_client_id("trds", symbol, order_id);
        stop_request.client_order_id = Some(stop_client_id.clone());

        let exchange = self.account.exchange.clone();
        let mut handles = BracketOrderHandles::default();
        match exchange.create_order(stop_request.clone()).await {
            Ok(order) => {
                info!("{} 止损单已提交 @ {}", symbol, formatted_stop_str);
                handles.stop = Some(SubmittedOrderHandle {
                    symbol: symbol.to_string(),
                    order_id: order.id,
                    client_order_id: stop_client_id,
                });
            }
            Err(err) => {
                let err_msg = err.to_string();
                if err_msg.contains("\"code\":-2021")
                    || err_msg.contains("Order would immediately trigger")
                {
                    warn!(
                        "{} 止损单(MARK_PRICE)触发立即成交限制，尝试 CONTRACT_PRICE 重试",
                        symbol
                    );
                    let mut retry_request = stop_request;
                    if let Some(params) = retry_request.params.as_mut() {
                        params.insert("workingType".to_string(), "CONTRACT_PRICE".to_string());
                    }

                    match exchange.create_order(retry_request).await {
                        Ok(order) => {
                            info!(
                                "{} 止损单已提交(CONTRACT_PRICE) @ {}",
                                symbol, formatted_stop_str
                            );
                            handles.stop = Some(SubmittedOrderHandle {
                                symbol: symbol.to_string(),
                                order_id: order.id,
                                client_order_id: stop_client_id,
                            });
                        }
                        Err(retry_err) => {
                            warn!("提交止损单失败 ({}): {}", symbol, retry_err);
                            let retry_msg = retry_err.to_string();
                            if retry_msg.contains("\"code\":-2021")
                                || retry_msg.contains("Order would immediately trigger")
                            {
                                warn!("{} 止损无法挂出且已触发阈值，执行市价保护平仓", symbol);
                                let mut emergency = OrderRequest::new(
                                    symbol.to_string(),
                                    exit_side.clone(),
                                    OrderType::Market,
                                    qty,
                                    None,
                                    MarketType::Futures,
                                );
                                emergency.reduce_only = if include_position_side {
                                    None
                                } else {
                                    Some(true)
                                };
                                if include_position_side {
                                    let mut params = HashMap::new();
                                    params.insert(
                                        "positionSide".to_string(),
                                        position_side.to_string(),
                                    );
                                    emergency.params = Some(params);
                                }
                                emergency.client_order_id = Some(Self::build_client_id(
                                    "trmx",
                                    symbol,
                                    &Utc::now().timestamp_micros().to_string(),
                                ));
                                if let Err(close_err) = exchange.create_order(emergency).await {
                                    warn!("{} 市价保护平仓失败: {}", symbol, close_err);
                                } else {
                                    info!("{} 市价保护平仓单已提交", symbol);
                                }
                                return Ok(handles);
                            }
                        }
                    }
                } else {
                    warn!("提交止损单失败 ({}): {}", symbol, err);
                }
            }
        }

        let partials = if self.config.stop_config.partial_targets.is_empty() {
            vec![
                PartialTarget {
                    target_r: 1.0,
                    close_ratio: 0.5,
                },
                PartialTarget {
                    target_r: 2.0,
                    close_ratio: 0.5,
                },
            ]
        } else {
            self.config.stop_config.partial_targets.clone()
        };

        let mut remaining_qty = qty;
        for (idx, target) in partials.iter().enumerate() {
            let mut tp_qty = if idx == partials.len() - 1 {
                self.execution_engine.format_amount(symbol, remaining_qty)
            } else {
                self.execution_engine
                    .format_amount(symbol, qty * target.close_ratio)
            };
            if tp_qty <= f64::EPSILON {
                continue;
            }
            if tp_qty > remaining_qty {
                tp_qty = remaining_qty;
            }
            remaining_qty -= tp_qty;

            let target_price = match side {
                OrderSide::Buy => entry_price + risk * target.target_r,
                OrderSide::Sell => entry_price - risk * target.target_r,
            };
            let formatted_target = self.execution_engine.format_price(symbol, target_price);
            let formatted_target_str = self
                .execution_engine
                .format_price_string(symbol, formatted_target);

            let mut tp_request = OrderRequest::new(
                symbol.to_string(),
                exit_side.clone(),
                OrderType::Limit,
                tp_qty,
                Some(formatted_target),
                MarketType::Futures,
            );
            tp_request.reduce_only = if include_position_side {
                None
            } else {
                Some(true)
            };
            tp_request.time_in_force = Some("GTX".to_string());
            let take_client_id =
                Self::build_client_id("trtp", symbol, &format!("{}{}", order_id, idx));
            tp_request.client_order_id = Some(take_client_id.clone());
            let mut tp_params = HashMap::new();
            if include_position_side {
                tp_params.insert("positionSide".to_string(), position_side.to_string());
            }
            tp_request.params = Some(tp_params);

            match exchange.create_order(tp_request).await {
                Ok(order) => {
                    info!(
                        "{} 止盈单({})已提交 @ {}",
                        symbol,
                        idx + 1,
                        formatted_target_str
                    );
                    handles.take_profits.push(SubmittedOrderHandle {
                        symbol: symbol.to_string(),
                        order_id: order.id,
                        client_order_id: take_client_id,
                    });
                }
                Err(err) => {
                    warn!("提交止盈单({})失败 ({}): {}", idx + 1, symbol, err);
                }
            }
        }

        Ok(handles)
    }

    async fn spawn_bracket_listeners(
        &self,
        symbol: String,
        entry_side: OrderSide,
        handles: BracketOrderHandles,
    ) {
        if let Some(stop) = handles.stop {
            self.register_exit_listener(symbol.clone(), entry_side, stop, ExitOrderKind::StopLoss)
                .await;
        }

        for tp in handles.take_profits {
            self.register_exit_listener(symbol.clone(), entry_side, tp, ExitOrderKind::TakeProfit)
                .await;
        }
    }

    async fn register_exit_listener(
        &self,
        symbol: String,
        entry_side: OrderSide,
        handle: SubmittedOrderHandle,
        kind: ExitOrderKind,
    ) {
        let tracker = self.order_tracker.clone();
        let strategy = self.clone();
        let task = tokio::spawn(async move {
            let receiver = tracker.watch_client(handle.client_order_id.clone()).await;
            match receiver.await {
                Ok(event) => {
                    strategy
                        .handle_exit_fill(symbol.clone(), entry_side, event, kind)
                        .await;
                }
                Err(err) => {
                    warn!("监听 {} {} 时通道关闭: {}", symbol, kind.label(), err);
                }
            }
        });

        self.task_handles.lock().await.push(task);
    }

    async fn handle_exit_fill(
        &self,
        symbol: String,
        entry_side: OrderSide,
        event: FillEvent,
        kind: ExitOrderKind,
    ) {
        {
            let mut manager = self.position_manager.write().await;
            if let Some(position) =
                manager.close_position_with_fill_side(&symbol, entry_side, event.average_price)
            {
                info!(
                    "{} {:?} {} 平仓 @ {:.4}，累计收益 {:.4}",
                    symbol,
                    entry_side,
                    kind.label(),
                    event.average_price,
                    position.realized_pnl
                );
            } else {
                debug!(
                    "{} {:?} 收到 {} 事件但未找到持仓",
                    symbol,
                    entry_side,
                    kind.label()
                );
            }
        }

        let report = {
            let manager = self.position_manager.read().await;
            manager.get_positions_report()
        };
        self.update_status_with_positions(&report).await;

        let key = Self::position_slot_key(&symbol, entry_side);
        self.cancel_tracked_bracket_orders(&key, Some(event.order_id.as_str()))
            .await;
    }

    async fn mark_bracket_registration(&self, key: &str) -> bool {
        let mut registry = self.bracket_registry.lock().await;
        if registry.contains(key) {
            return false;
        }
        registry.insert(key.to_string());
        true
    }

    fn position_slot_key(symbol: &str, side: OrderSide) -> String {
        format!("{}#{}", symbol, Self::position_side_label(side))
    }

    fn position_side_label(side: OrderSide) -> &'static str {
        match side {
            OrderSide::Buy => "LONG",
            OrderSide::Sell => "SHORT",
        }
    }

    async fn cancel_tracked_bracket_orders(&self, key: &str, skip_order_id: Option<&str>) {
        let handles = { self.bracket_handles.lock().await.remove(key) };
        let Some(handles) = handles else {
            return;
        };

        let mut tracked = Vec::new();
        if let Some(stop) = handles.stop {
            tracked.push(stop);
        }
        tracked.extend(handles.take_profits);

        for handle in tracked {
            if skip_order_id
                .map(|skip| skip == handle.order_id.as_str())
                .unwrap_or(false)
            {
                continue;
            }
            if let Err(err) = self
                .account
                .exchange
                .cancel_order(&handle.order_id, &handle.symbol, MarketType::Futures)
                .await
            {
                warn!(
                    "{} 取消保护单 {} 失败: {}",
                    handle.symbol, handle.order_id, err
                );
            }
        }
    }

    fn build_client_id(prefix: &str, symbol: &str, order_id: &str) -> String {
        let mut base = format!("{}{}", prefix, symbol.replace('/', ""));
        if base.len() > 16 {
            base.truncate(16);
        }

        let mut hasher = DefaultHasher::new();
        prefix.hash(&mut hasher);
        symbol.hash(&mut hasher);
        order_id.hash(&mut hasher);
        let hash = hasher.finish();
        let mut id = format!("{}{:x}", base, hash);
        if id.len() > 30 {
            id.truncate(30);
        }
        id
    }

    fn min_notional(&self, symbol: &str) -> f64 {
        self.symbol_precisions
            .read()
            .ok()
            .and_then(|map| map.get(symbol).and_then(|p| p.min_notional))
            .unwrap_or(5.0)
    }

    /// 清理低于 5U 名义的尾仓：撤销保护单并用市价 ReduceOnly 平仓
    async fn cleanup_dust_positions(&self) -> Result<()> {
        let threshold_notional = 5.0;
        let include_position_side = self.is_dual_position_mode().await;

        let positions: Vec<TrendPosition> = {
            let mgr = self.position_manager.read().await;
            mgr.get_positions_report().positions
        };

        for position in positions {
            let notional = position.current_size * position.current_price.abs();
            if notional >= threshold_notional {
                continue;
            }

            let qty = self
                .execution_engine
                .format_amount(&position.symbol, position.current_size);
            if qty <= f64::EPSILON {
                continue;
            }

            let exit_side = match position.side {
                OrderSide::Buy => OrderSide::Sell,
                OrderSide::Sell => OrderSide::Buy,
            };

            let key = Self::position_slot_key(&position.symbol, position.side);
            self.cancel_tracked_bracket_orders(&key, None).await;

            let mut request = OrderRequest::new(
                position.symbol.clone(),
                exit_side,
                OrderType::Market,
                qty,
                None,
                MarketType::Futures,
            );
            request.reduce_only = if include_position_side {
                None
            } else {
                Some(true)
            };
            request.time_in_force = None;
            if include_position_side {
                let mut params = HashMap::new();
                params.insert(
                    "positionSide".to_string(),
                    Self::position_side_label(position.side).to_string(),
                );
                request.params = Some(params);
            }
            let client_id = Self::build_client_id(
                "dust",
                &position.symbol,
                &Utc::now().timestamp_micros().to_string(),
            );
            request.client_order_id = Some(client_id.clone());

            match self.account.exchange.create_order(request).await {
                Ok(order) => {
                    let fill_price = order.price.unwrap_or(position.current_price);
                    {
                        let mut mgr = self.position_manager.write().await;
                        mgr.close_position_with_fill_side(
                            &position.symbol,
                            position.side,
                            fill_price,
                        );
                    }
                    info!(
                        "{} 尾仓(<{:.2}U)市价平仓完成 qty={:.6} price={:.4}",
                        position.symbol, threshold_notional, qty, fill_price
                    );
                    let report = {
                        let mgr = self.position_manager.read().await;
                        mgr.get_positions_report()
                    };
                    self.update_status_with_positions(&report).await;
                }
                Err(err) => {
                    warn!("{} 尾仓市价平仓失败: {}", position.symbol, err);
                }
            }
        }

        Ok(())
    }

    async fn send_status_report(&self, client: &Client, webhook_url: &str) -> Result<()> {
        let capital = self.refresh_account_metrics().await?;
        let report = {
            let manager = self.position_manager.read().await;
            manager.get_positions_report()
        };

        let now = Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
        let mut content = format!(
            "## 📊 趋势策略资产快报\n时间: {}\n\n**资产**: {:.2} USDT\n可用 {:.2} | 占用 {:.2}\n\n**持仓**:\n",
            now,
            capital.total,
            capital.available,
            capital.total - capital.available
        );

        let mut found = false;
        for position in report
            .positions
            .iter()
            .filter(|p| p.current_size.abs() > f64::EPSILON)
        {
            found = true;
            let side = match position.side {
                OrderSide::Buy => "多",
                OrderSide::Sell => "空",
            };
            let notional = position.current_size.abs() * position.current_price.abs();
            let pnl = position.realized_pnl + position.unrealized_pnl;
            let pnl_str = if pnl >= 0.0 {
                format!("+{:.2}", pnl)
            } else {
                format!("{:.2}", pnl)
            };
            content.push_str(&format!(
                "- {} {} {:.4} @ {:.4} | 名义 {:.2} | PnL {}\n",
                position.symbol,
                side,
                position.current_size.abs(),
                position.average_price,
                notional,
                pnl_str
            ));
        }

        if !found {
            content.push_str("当前无持仓");
        }

        let payload = json!({
            "msgtype": "markdown",
            "markdown": {
                "content": content
            }
        });

        let resp = client.post(webhook_url).json(&payload).send().await?;
        if !resp.status().is_success() {
            return Err(anyhow!("Webhook返回异常: {}", resp.status()));
        }

        Ok(())
    }

    async fn execute_entry_plans(
        &self,
        symbol: &str,
        trade_signal: &TradeSignal,
        plans: Vec<EntryOrderPlan>,
    ) -> Result<Option<ExecutionReport>> {
        let mut pending = self.prepare_working_orders(symbol, plans);
        if pending.is_empty() {
            return Ok(None);
        }

        let timeout = Duration::from_secs(self.config.execution.order_timeout_secs.max(5));
        let max_attempts = self.config.execution.max_retries.max(1);
        let include_position_side = self.is_dual_position_mode().await;
        let start = Instant::now();
        let mut fills: Vec<PlanFill> = Vec::new();

        while !pending.is_empty() {
            let mut requests = Vec::new();
            for order in pending.iter_mut() {
                order.order_id = None;
                order.client_id =
                    self.build_entry_client_id(symbol, order.plan.mode, order.attempts);
                if let Some(request) =
                    self.build_entry_request(symbol, order, include_position_side)
                {
                    requests.push(request);
                }
            }

            if requests.is_empty() {
                break;
            }

            let response = self
                .account_manager
                .create_batch_orders(&self.config.account_id, requests)
                .await
                .map_err(|e| anyhow!("批量下单失败: {}", e))?;

            let mut order_map: HashMap<String, String> = HashMap::new();
            let mut immediate_fills: HashMap<String, (f64, f64)> = HashMap::new();
            for order in response.successful_orders {
                let client_id = order
                    .info
                    .get("clientOrderId")
                    .or_else(|| order.info.get("newClientOrderId"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| {
                        pending
                            .iter()
                            .find(|pending_order| pending_order.client_id == order.id)
                            .map(|pending_order| pending_order.client_id.clone())
                    });
                if let Some(client_id) = client_id {
                    order_map.insert(client_id.clone(), order.id.clone());
                    let is_mock_order = order
                        .info
                        .get("mock")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false);
                    if order.filled > f64::EPSILON || is_mock_order {
                        let price = order.price.unwrap_or(0.0);
                        let filled_qty = if order.filled > f64::EPSILON {
                            order.filled
                        } else {
                            order.amount
                        };
                        immediate_fills.insert(client_id, (filled_qty, price));
                    }
                }
            }
            for order in pending.iter_mut() {
                order.order_id = order_map.get(&order.client_id).cloned();
            }

            if !response.failed_orders.is_empty() {
                for failure in response.failed_orders {
                    warn!("{} 批量订单提交失败: {}", symbol, failure.error_message);
                }
            }

            let mut next_round = Vec::new();
            for mut order in pending.into_iter() {
                if let Some((qty, price)) = immediate_fills.remove(&order.client_id) {
                    fills.push(PlanFill {
                        mode: order.plan.mode,
                        signal_type: order.plan.signal_type.clone(),
                        qty,
                        price: if price > f64::EPSILON {
                            price
                        } else {
                            order.price
                        },
                        reference_price: order.plan.price,
                    });
                    continue;
                }

                match self
                    .order_tracker
                    .wait_client_with_timeout(order.client_id.clone(), timeout, "entry_fill")
                    .await
                {
                    Ok(fill_event) => {
                        let qty = if fill_event.last_filled_qty > f64::EPSILON {
                            fill_event.last_filled_qty
                        } else {
                            fill_event.filled_qty
                        };
                        let price = if fill_event.last_filled_price > f64::EPSILON {
                            fill_event.last_filled_price
                        } else {
                            fill_event.average_price
                        };
                        if qty > f64::EPSILON {
                            fills.push(PlanFill {
                                mode: order.plan.mode,
                                signal_type: order.plan.signal_type.clone(),
                                qty,
                                price,
                                reference_price: order.plan.price,
                            });
                        }
                    }
                    Err(err) => {
                        warn!(
                            "{} [{}] 等待成交超时: {}",
                            symbol,
                            order.plan.mode.label(),
                            err
                        );
                        if let Some(order_id) = order.order_id.clone() {
                            if let Err(cancel_err) = self
                                .account
                                .exchange
                                .cancel_order(&order_id, symbol, MarketType::Futures)
                                .await
                            {
                                warn!("{} 取消订单 {} 失败: {}", symbol, order_id, cancel_err);
                            }
                        }
                        order.attempts += 1;
                        if order.attempts >= max_attempts {
                            warn!("{} [{}] 达到最大重挂次数", symbol, order.plan.mode.label());
                            continue;
                        }
                        order.price = self
                            .execution_engine
                            .format_price(symbol, self.reprice_entry(order.price, &order.plan));
                        order.quantity = self
                            .execution_engine
                            .format_amount(symbol, order.plan.notional / order.price.max(1e-6));
                        if order.quantity <= f64::EPSILON {
                            continue;
                        }
                        next_round.push(order);
                    }
                }
            }

            pending = next_round;
        }

        if fills.is_empty() {
            return Ok(None);
        }

        let total_qty: f64 = fills.iter().map(|f| f.qty).sum();
        if total_qty <= f64::EPSILON {
            return Ok(None);
        }

        let actual_cost: f64 = fills.iter().map(|f| f.qty * f.price).sum();
        let expected_cost: f64 = fills.iter().map(|f| f.qty * f.reference_price).sum();
        let avg_price = actual_cost / total_qty;
        let expected_price = if expected_cost > 0.0 {
            expected_cost / total_qty
        } else {
            avg_price
        };
        let slippage = if expected_price.abs() > f64::EPSILON {
            ((avg_price - expected_price) / expected_price).abs()
        } else {
            0.0
        };

        let order = Order {
            id: format!(
                "batch_{}_{}",
                symbol.replace('/', ""),
                Utc::now().timestamp_micros()
            ),
            symbol: symbol.to_string(),
            side: trade_signal.side,
            order_type: OrderType::Limit,
            amount: total_qty,
            price: Some(avg_price),
            filled: total_qty,
            remaining: 0.0,
            status: OrderStatus::Closed,
            market_type: MarketType::Futures,
            timestamp: Utc::now(),
            last_trade_timestamp: Some(Utc::now()),
            info: json!({
                "fills": fills
                    .iter()
                    .map(|f| json!({
                        "mode": f.mode.label(),
                        "qty": f.qty,
                        "price": f.price,
                    }))
                    .collect::<Vec<_>>(),
            }),
        };

        let report = ExecutionReport {
            order,
            attempts: max_attempts,
            used_maker: true,
            slippage,
            execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            filled_qty: total_qty,
            average_price: avg_price,
            client_order_id: None,
        };

        Ok(Some(report))
    }

    fn prepare_working_orders(
        &self,
        symbol: &str,
        plans: Vec<EntryOrderPlan>,
    ) -> Vec<WorkingOrder> {
        let mut orders = Vec::new();
        for plan in plans {
            if plan.notional <= 0.0 {
                continue;
            }
            let price = self.execution_engine.format_price(symbol, plan.price);
            if price <= 0.0 {
                continue;
            }
            let qty = self
                .execution_engine
                .format_amount(symbol, plan.notional / price.max(1e-6));
            if qty <= f64::EPSILON {
                continue;
            }
            let notional = price * qty;
            if notional < self.min_notional(symbol) {
                continue;
            }
            orders.push(WorkingOrder {
                plan,
                client_id: String::new(),
                order_id: None,
                price,
                quantity: qty,
                attempts: 0,
            });
        }
        orders
    }

    async fn refresh_active_stops(&self) -> Result<()> {
        let positions = {
            let manager = self.position_manager.read().await;
            manager.get_positions_report().positions
        };

        if positions.is_empty() {
            return Ok(());
        }

        let handles_snapshot = { self.bracket_handles.lock().await.clone() };

        for position in positions {
            let slot_key = Self::position_slot_key(&position.symbol, position.side);
            let Some(entry_handles) = handles_snapshot.get(&slot_key) else {
                continue;
            };
            let Some(stop_handle) = entry_handles.stop.clone() else {
                continue;
            };

            let target_price = match self.stop_manager.calculate_stop_update(&position).await? {
                Some(StopUpdate::MoveTo(price))
                | Some(StopUpdate::Tighten(price))
                | Some(StopUpdate::Trailing(price)) => price,
                Some(StopUpdate::Breakeven) => position.entry_price,
                _ => continue,
            };

            if target_price <= 0.0 {
                continue;
            }

            match self
                .replace_stop_order(&position, target_price, stop_handle.clone())
                .await
            {
                Ok(new_handle) => {
                    let mut map = self.bracket_handles.lock().await;
                    if let Some(entry) = map.get_mut(&slot_key) {
                        entry.stop = Some(new_handle);
                    }
                }
                Err(err) => {
                    warn!("{} 止损更新失败: {}", position.symbol, err);
                }
            }
        }

        Ok(())
    }

    async fn replace_stop_order(
        &self,
        position: &TrendPosition,
        new_price: f64,
        handle: SubmittedOrderHandle,
    ) -> Result<SubmittedOrderHandle> {
        if new_price <= 0.0 {
            bail!("无效的止损价格");
        }

        if let Err(err) = self
            .account
            .exchange
            .cancel_order(&handle.order_id, &position.symbol, MarketType::Futures)
            .await
        {
            warn!(
                "{} 取消旧止损 ({}) 失败: {}",
                position.symbol, handle.order_id, err
            );
        }

        let qty = self
            .execution_engine
            .format_amount(&position.symbol, position.current_size);
        if qty <= f64::EPSILON {
            bail!("{} 当前仓位数量过小", position.symbol);
        }

        let formatted_stop = self
            .execution_engine
            .format_price(&position.symbol, new_price);
        let formatted_stop_str = self
            .execution_engine
            .format_price_string(&position.symbol, formatted_stop);
        let exit_side = match position.side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };
        let position_side = match position.side {
            OrderSide::Buy => "LONG",
            OrderSide::Sell => "SHORT",
        };
        let include_position_side = self.is_dual_position_mode().await;

        let mut params = HashMap::new();
        params.insert("stopPrice".to_string(), formatted_stop_str.clone());
        params.insert("workingType".to_string(), "MARK_PRICE".to_string());
        if include_position_side {
            params.insert("positionSide".to_string(), position_side.to_string());
        }

        let mut request = OrderRequest::new(
            position.symbol.clone(),
            exit_side,
            OrderType::StopMarket,
            qty,
            None,
            MarketType::Futures,
        );
        let client_id = Self::build_client_id(
            "trds",
            &position.symbol,
            &Utc::now().timestamp_micros().to_string(),
        );
        request.client_order_id = Some(client_id.clone());
        request.reduce_only = if include_position_side {
            None
        } else {
            Some(true)
        };
        request.params = Some(params);

        let order = self.account.exchange.create_order(request).await?;
        info!(
            "{} 止损更新为 {} (原订单 {})",
            position.symbol, formatted_stop_str, handle.order_id
        );

        Ok(SubmittedOrderHandle {
            symbol: position.symbol.clone(),
            order_id: order.id,
            client_order_id: client_id,
        })
    }

    fn build_entry_request(
        &self,
        symbol: &str,
        order: &WorkingOrder,
        include_position_side: bool,
    ) -> Option<OrderRequest> {
        if order.quantity <= f64::EPSILON {
            return None;
        }

        let use_market = self.config.market_entry_on_breakout
            && matches!(
                order.plan.signal_type,
                SignalType::TrendBreakout | SignalType::MomentumSurge | SignalType::PatternBreakout
            );

        let mut request = if use_market {
            OrderRequest::new(
                symbol.to_string(),
                order.plan.side,
                OrderType::Market,
                order.quantity,
                None,
                MarketType::Futures,
            )
        } else {
            if order.price <= 0.0 {
                return None;
            }
            let mut request = OrderRequest::new(
                symbol.to_string(),
                order.plan.side,
                OrderType::Limit,
                order.quantity,
                Some(order.price),
                MarketType::Futures,
            );
            request.time_in_force = Some("GTX".to_string());
            request.post_only = Some(true);
            request
        };

        request.client_order_id = Some(order.client_id.clone());
        if include_position_side {
            let mut params = HashMap::new();
            params.insert(
                "positionSide".to_string(),
                Self::position_side_label(order.plan.side).to_string(),
            );
            request.params = Some(params);
        }
        Some(request)
    }

    fn reprice_entry(&self, current_price: f64, plan: &EntryOrderPlan) -> f64 {
        let step = 0.001;
        match plan.side {
            OrderSide::Buy => current_price * (1.0 + step),
            OrderSide::Sell => current_price * (1.0 - step),
        }
    }

    fn build_entry_client_id(&self, symbol: &str, mode: EntryMode, attempt: u32) -> String {
        let prefix = match mode {
            EntryMode::MovingAverage => "trema",
            EntryMode::Fibonacci => "trefb",
            EntryMode::Bollinger => "trebb",
        };
        let suffix = format!("{}{}", Utc::now().timestamp_micros(), attempt);
        Self::build_client_id(prefix, symbol, &suffix)
    }

    fn match_config_symbol(&self, raw: &str) -> Option<String> {
        let normalized = raw.replace('/', "").to_uppercase();
        self.config
            .symbols
            .iter()
            .find(|sym| sym.replace('/', "").to_uppercase() == normalized)
            .cloned()
    }

    fn build_take_profit_levels(
        &self,
        entry_price: f64,
        stop_loss: f64,
        side: OrderSide,
    ) -> Vec<TakeProfitLevel> {
        let risk = (entry_price - stop_loss).abs();
        let targets = if self.config.stop_config.partial_targets.is_empty() {
            vec![
                PartialTarget {
                    target_r: 1.0,
                    close_ratio: 0.5,
                },
                PartialTarget {
                    target_r: 2.0,
                    close_ratio: 0.5,
                },
            ]
        } else {
            self.config.stop_config.partial_targets.clone()
        };

        targets
            .into_iter()
            .map(|target| TakeProfitLevel {
                price: match side {
                    OrderSide::Buy => entry_price + risk * target.target_r,
                    OrderSide::Sell => entry_price - risk * target.target_r,
                },
                ratio: target.close_ratio,
                executed: false,
            })
            .collect()
    }

    fn build_restored_position(
        &self,
        symbol: String,
        side: OrderSide,
        size: f64,
        entry_price: f64,
        mark_price: f64,
        entry_time: DateTime<Utc>,
        unrealized_pnl: f64,
    ) -> TrendPosition {
        let stop_loss = self
            .stop_manager
            .calculate_initial_stop(entry_price, side, None);
        let take_profits = self.build_take_profit_levels(entry_price, stop_loss, side);

        TrendPosition {
            symbol,
            side,
            entry_price,
            average_price: entry_price,
            current_price: mark_price.max(1e-6),
            size,
            current_size: size,
            initial_size: size,
            pyramid_entries: Vec::new(),
            pyramid_count: 0,
            stop_loss,
            take_profits,
            entry_time,
            last_update: Utc::now(),
            realized_pnl: 0.0,
            unrealized_pnl,
            peak_profit: unrealized_pnl.max(0.0),
            risk_amount: size * (entry_price - stop_loss).abs(),
        }
    }

    fn signed_position_amount(position: &Position) -> f64 {
        if position.side.eq_ignore_ascii_case("SHORT") {
            return -position.amount.abs().max(position.contracts.abs()).max(0.0);
        }
        if position.side.eq_ignore_ascii_case("LONG") {
            return position.amount.abs().max(position.contracts.abs()).max(0.0);
        }
        if position.amount.abs() > f64::EPSILON {
            position.amount
        } else {
            position.contracts
        }
    }

    async fn takeover_existing_positions(&self) -> Result<()> {
        let positions = self
            .account
            .exchange
            .get_positions(None)
            .await
            .map_err(|e| anyhow!("获取账户持仓失败: {}", e))?;
        let open_orders = self
            .account
            .exchange
            .get_open_orders(None, MarketType::Futures)
            .await
            .map_err(|e| anyhow!("获取账户挂单失败: {}", e))?;
        let mut orders_by_symbol: HashMap<String, Vec<Order>> = HashMap::new();
        for order in open_orders {
            if let Some(symbol) = self.match_config_symbol(&order.symbol) {
                orders_by_symbol.entry(symbol).or_default().push(order);
            }
        }

        let total_positions = positions.len();
        let mut unmatched_symbols: Vec<String> = Vec::new();

        if positions.is_empty() {
            return Ok(());
        }

        let mut restored = 0;
        for pos in positions {
            let Some(symbol) = self.match_config_symbol(&pos.symbol) else {
                unmatched_symbols.push(pos.symbol.clone());
                continue;
            };
            let signed_size = Self::signed_position_amount(&pos);
            if signed_size.abs() <= f64::EPSILON {
                continue;
            }

            let side = if signed_size > 0.0 {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let size = signed_size.abs();
            let entry_price = pos.entry_price.max(1e-6);
            let restored_position = self.build_restored_position(
                symbol.clone(),
                side,
                size,
                entry_price,
                pos.mark_price.max(1e-6),
                pos.timestamp,
                pos.unrealized_pnl,
            );

            {
                let mut manager = self.position_manager.write().await;
                if manager.has_open_position_side(&symbol, side) {
                    continue;
                }
                manager.restore_position(restored_position.clone());
            }

            let existing_orders = orders_by_symbol.get(&symbol).cloned().unwrap_or_default();
            let (has_stop, has_take_profit, stop_qty, take_profit_qty) =
                Self::protection_status_for_position(&restored_position, &existing_orders);

            if has_stop && has_take_profit {
                info!(
                    "启动接管: {} {} 已有完整保护单 stop_qty={:.8}, tp_qty={:.8}, pos_qty={:.8}",
                    symbol,
                    Self::position_side_label(side),
                    stop_qty,
                    take_profit_qty,
                    size
                );
                restored += 1;
                continue;
            }

            warn!(
                "启动接管: {} {} 缺少保护单 stop_qty={:.8}, tp_qty={:.8}, pos_qty={:.8}，重新挂保护单",
                symbol,
                Self::position_side_label(side),
                stop_qty,
                take_profit_qty,
                size
            );
            self.cancel_exit_orders_for_position(&symbol, &existing_orders, &restored_position)
                .await;

            let handles = self
                .submit_bracket_orders(
                    &symbol,
                    side,
                    entry_price,
                    size,
                    &format!("takeover_{}", Utc::now().timestamp_micros()),
                    None,
                )
                .await?;

            {
                let mut map = self.bracket_handles.lock().await;
                map.insert(Self::position_slot_key(&symbol, side), handles.clone());
            }

            self.spawn_bracket_listeners(symbol, side, handles).await;
            restored += 1;
        }

        if restored > 0 {
            info!(
                "启动接管: 交易所返回 {} 条持仓，成功接管 {} 个策略跟踪的仓位",
                total_positions, restored
            );
        } else if total_positions == 0 {
            info!("启动接管: 当前账户无持仓，跳过接管流程");
        } else {
            warn!(
                "启动接管: 交易所返回 {} 条持仓，但均未匹配策略配置的交易对: {:?}",
                total_positions,
                unmatched_symbols.into_iter().take(5).collect::<Vec<_>>()
            );
        }

        Ok(())
    }
}

#[derive(Clone)]
struct WorkingOrder {
    plan: EntryOrderPlan,
    client_id: String,
    order_id: Option<String>,
    price: f64,
    quantity: f64,
    attempts: u32,
}

impl WorkingOrder {
    fn effective_notional(&self) -> f64 {
        self.price * self.quantity
    }
}

#[derive(Clone)]
struct PlanFill {
    mode: EntryMode,
    signal_type: SignalType,
    qty: f64,
    price: f64,
    reference_price: f64,
}

impl Strategy for TrendIntradayStrategy {
    type Config = TrendConfig;

    fn create(config: Self::Config, deps: StrategyDeps) -> Result<Self> {
        Self::new(config, deps)
    }
}

#[async_trait]
impl StrategyInstance for TrendIntradayStrategy {
    async fn start(&self) -> Result<()> {
        {
            let mut running = self.running.write().await;
            if *running {
                return Ok(());
            }
            *running = true;
        }

        self.refresh_symbol_precisions().await?;
        self.monitor.start().await;
        self.risk_controller.self_check().await?;
        self.refresh_account_metrics().await?;
        self.takeover_existing_positions().await?;
        self.spawn_housekeeping_task().await;
        self.spawn_protection_guard_task().await;
        self.spawn_status_report_task().await;
        self.indicator_service.start().await?;
        self.market_feed.start().await?;
        self.user_stream.start().await?;

        {
            let mut status = self.status.write().await;
            status.state = StrategyState::Running;
            status.updated_at = Utc::now();
        }

        for symbol in &self.config.symbols {
            let strategy = self.clone();
            let symbol_clone = symbol.clone();
            let handle = tokio::spawn(async move {
                strategy.run_symbol_loop(symbol_clone).await;
            });

            self.task_handles.lock().await.push(handle);
        }

        info!(
            "日内趋势策略已启动，交易对数量: {}",
            self.config.symbols.len()
        );

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

        {
            let mut handles = self.task_handles.lock().await;
            for handle in handles.drain(..) {
                handle.abort();
            }
        }

        self.cancel_open_orders().await;
        self.indicator_service.stop().await;
        self.market_feed.stop().await;
        self.user_stream.stop().await;
        self.monitor.stop().await;

        {
            let mut status = self.status.write().await;
            status.state = StrategyState::Stopped;
            status.updated_at = Utc::now();
        }

        info!("日内趋势策略已停止");
        Ok(())
    }

    async fn status(&self) -> Result<StrategyStatus> {
        let status = self.status.read().await;
        Ok(status.clone())
    }
}
