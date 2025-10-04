//! # 趋势网格策略 (Trend Grid Trading Strategy V2)
//!
//! 结合趋势跟踪和网格交易的混合策略，在趋势中进行网格交易。
//!
//! ## 主要功能
//! - 自动识别市场趋势方向
//! - 在趋势方向上布置网格订单
//! - 动态调整网格间距和数量
//! - WebSocket实时订单管理

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

use crate::analysis::{TradeCollector, TradeData};
use crate::core::{
    error::ExchangeError,
    types::*,
    websocket::{ConnectionState, MessageHandler, WebSocketClient},
};
use crate::cta::account_manager::{AccountInfo, AccountManager};
use crate::utils::indicators::{trend_strength_to_enum, TrendStrengthCalculator};
use crate::utils::{generate_order_id, generate_order_id_with_tag};
use rust_decimal::Decimal;

/// 交易配置 - 每个配置独立运行
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    pub config_id: String,
    pub enabled: bool,
    pub account: AccountConfig,
    pub symbol: String,
    pub grid: GridConfig,
    pub trend_config: TrendIndicatorConfig,
}

/// 账户配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountConfig {
    pub id: String,
    pub exchange: String,
    pub env_prefix: String,
}

/// 网格配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridConfig {
    pub spacing: f64,
    pub spacing_type: SpacingType,
    pub order_amount: f64,
    pub orders_per_side: u32,
    pub max_position: f64,
}

/// 网格间距类型
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SpacingType {
    #[serde(rename = "arithmetic")]
    Arithmetic,
    #[serde(rename = "geometric")]
    Geometric,
}

/// 趋势指标配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendIndicatorConfig {
    pub ma_fast: u32,
    pub ma_slow: u32,
    pub rsi_period: u32,
    pub rsi_overbought: f64,
    pub rsi_oversold: f64,
    pub timeframe: String,
    pub show_trend_info: bool,
}

/// 趋势调整配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendAdjustment {
    pub strong_bull_buy_multiplier: f64,  // 强上涨买单倍数 (2.0)
    pub bull_buy_multiplier: f64,         // 弱上涨买单倍数 (1.5)
    pub bear_sell_multiplier: f64,        // 弱下跌卖单倍数 (1.5)
    pub strong_bear_sell_multiplier: f64, // 强下跌卖单倍数 (2.0)
}

/// 策略配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendGridConfigV2 {
    pub strategy: StrategyInfo,
    pub trading_configs: Vec<TradingConfig>,
    pub trend_adjustment: TrendAdjustment,
    pub batch_settings: BatchSettings,
    pub grid_management: GridManagement,
    pub websocket: WebSocketConfig,
    pub risk_control: RiskControl,
    pub execution: ExecutionConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyInfo {
    pub name: String,
    pub version: String,
    pub enabled: bool,
    pub strategy_type: String,
    pub market_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchSettings {
    pub binance_batch_size: u32,
    pub okx_batch_size: u32,
    pub hyperliquid_batch_size: u32,
    pub default_batch_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridManagement {
    pub check_interval: u64,
    pub rebalance_threshold: f64,
    pub cancel_and_replace: bool,
    pub show_grid_status: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    pub subscribe_order_updates: bool,
    pub subscribe_trade_updates: bool,
    pub subscribe_ticker: bool,
    pub reconnect_on_disconnect: bool,
    pub heartbeat_interval: u64,
    pub log_all_trades: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskControl {
    pub max_leverage: u32,
    pub max_drawdown: f64,
    pub daily_loss_limit: f64,
    pub position_limit_per_symbol: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub startup_cancel_all: bool,
    pub shutdown_cancel_all: bool,
    pub thread_per_config: bool,
    pub startup_delay: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub file: String,
    pub console: bool,
    pub show_pnl: bool,
    pub show_position: bool,
    pub show_trend_changes: bool,
}

/// 趋势强度
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TrendStrength {
    StrongBull,
    Bull,
    Neutral,
    Bear,
    StrongBear,
}

/// 趋势调整请求
#[derive(Debug, Clone)]
pub struct TrendAdjustmentRequest {
    pub amount: f64,
    pub side: OrderSide,
    pub order_type: OrderType,
}

/// 网格订单
#[derive(Debug, Clone)]
pub struct GridOrder {
    pub price: f64,
    pub amount: f64,
    pub side: OrderSide,
    pub order_id: Option<String>,
}

/// 交易配置状态
pub struct ConfigState {
    pub config: TradingConfig,
    pub current_price: f64,
    pub price_precision: u32,
    pub amount_precision: u32,
    pub grid_orders: HashMap<String, Order>, // 改为HashMap存储实际订单
    pub active_orders: HashMap<String, Order>,
    pub last_trade_price: f64,
    pub last_trade_time: DateTime<Utc>,
    pub trend_strength: TrendStrength,
    pub trend_calculator: TrendStrengthCalculator,
    pub position: f64,
    pub pnl: f64,
    pub trades_count: u64,
    pub ws_client: Option<Box<dyn WebSocketClient>>,
    // 详细盈亏统计
    pub total_buy_volume: f64,  // 总买入金额
    pub total_sell_volume: f64, // 总卖出金额
    // 数据收集器
    pub total_buy_amount: f64,              // 总买入数量
    pub total_sell_amount: f64,             // 总卖出数量
    pub total_fee: f64,                     // 总手续费
    pub net_position: f64,                  // 净持仓数量（买入量-卖出量）
    pub avg_buy_price: f64,                 // 平均买入价格
    pub avg_sell_price: f64,                // 平均卖出价格
    pub realized_pnl: f64,                  // 已实现盈亏
    pub unrealized_pnl: f64,                // 未实现盈亏
    pub last_grid_check: DateTime<Utc>,     // 上次网格检查时间
    pub need_grid_reset: bool,              // 是否需要重置网格
    pub last_trend_check: DateTime<Utc>,    // 上次趋势检查时间
    pub last_trend_strength: TrendStrength, // 上次的趋势强度，用于检测变化
}

/// 趋势网格策略V2
pub struct TrendGridStrategyV2 {
    config: TrendGridConfigV2,
    account_manager: Arc<AccountManager>,
    config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
    running: Arc<RwLock<bool>>,
    collector: Option<Arc<TradeCollector>>,
}

impl TrendGridStrategyV2 {
    /// 创建策略实例
    pub fn new(config: TrendGridConfigV2, account_manager: Arc<AccountManager>) -> Self {
        // 创建日志目录
        let _ = std::fs::create_dir_all("logs/strategies");

        Self {
            config,
            account_manager,
            config_states: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(false)),
            collector: None,
        }
    }

    /// 创建策略实例（带数据收集器）
    pub fn with_collector(
        config: TrendGridConfigV2,
        account_manager: Arc<AccountManager>,
        collector: Arc<TradeCollector>,
    ) -> Self {
        // 创建日志目录
        let _ = std::fs::create_dir_all("logs/strategies");

        Self {
            config,
            account_manager,
            config_states: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(RwLock::new(false)),
            collector: Some(collector),
        }
    }

    /// 写入策略专用日志
    fn write_log(config_id: &str, level: &str, message: &str) {
        let timestamp = Utc::now().format("%Y-%m-%d %H:%M:%S.%3f");
        let log_dir = "logs"; // 直接保存到 logs 文件夹
        let today = chrono::Local::now().format("%Y%m%d");
        let log_file = format!("{}/trend_grid_{}_{}.log", log_dir, config_id, today);

        // 确保目录存在
        let _ = std::fs::create_dir_all(log_dir);

        // 写入日志
        if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&log_file) {
            let log_line = format!("[{}] [{}] {}\n", timestamp, level, message);
            let _ = file.write_all(log_line.as_bytes());
        }

        // 同时输出到控制台
        match level {
            "ERROR" => log::error!("[{}] {}", config_id, message),
            "WARN" => log::warn!("[{}] {}", config_id, message),
            "INFO" => log::info!("[{}] {}", config_id, message),
            _ => log::debug!("[{}] {}", config_id, message),
        }
    }

    /// 启动策略
    pub async fn start(&self) -> Result<()> {
        log::info!("🚀 启动趋势网格策略");

        *self.running.write().await = true;

        // 启动时取消所有订单
        if self.config.execution.startup_cancel_all {
            self.cancel_all_orders().await?;
        }

        // 为每个启用的交易配置创建独立线程
        for trading_config in &self.config.trading_configs {
            if !trading_config.enabled {
                log::info!("⏭️ 跳过禁用配置: {}", trading_config.config_id);
                continue;
            }

            // 不再需要启动延迟，使用并发执行

            // 为每个配置创建独立线程
            if self.config.execution.thread_per_config {
                let config = trading_config.clone();
                let config_id = config.config_id.clone();
                let account_manager = self.account_manager.clone();
                let config_states = self.config_states.clone();
                let running = self.running.clone();
                let trend_adjustment = self.config.trend_adjustment.clone();
                let batch_settings = self.config.batch_settings.clone();
                let websocket_config = self.config.websocket.clone();
                let logging_config = self.config.logging.clone();
                let grid_management = self.config.grid_management.clone();
                let strategy_collector = self.collector.clone();

                tokio::spawn(async move {
                    log::info!("🔧 启动配置线程: {}", config_id);
                    Self::write_log(&config_id, "INFO", "启动策略线程");

                    // 添加重试机制
                    let mut retry_count = 0;
                    let max_retries = 10;
                    loop {
                        match Self::run_config_thread(
                            config.clone(),
                            account_manager.clone(),
                            config_states.clone(),
                            running.clone(),
                            trend_adjustment.clone(),
                            batch_settings.clone(),
                            websocket_config.clone(),
                            logging_config.clone(),
                            grid_management.clone(),
                            strategy_collector.clone(),
                        )
                        .await
                        {
                            Ok(_) => {
                                Self::write_log(&config_id, "INFO", "策略线程正常退出");
                                break;
                            }
                            Err(e) => {
                                retry_count += 1;
                                let error_msg = format!(
                                    "策略线程错误 (重试 {}/{}): {:?}",
                                    retry_count, max_retries, e
                                );
                                Self::write_log(&config_id, "ERROR", &error_msg);
                                log::error!("❌ 配置 {} 运行错误: {}", config_id, e);

                                if retry_count >= max_retries {
                                    Self::write_log(
                                        &config_id,
                                        "ERROR",
                                        "达到最大重试次数，策略停止",
                                    );
                                    break;
                                }

                                // 等待后重试
                                let wait_seconds = std::cmp::min(retry_count * 10, 60);
                                Self::write_log(
                                    &config_id,
                                    "INFO",
                                    &format!("等待{}秒后重试...", wait_seconds),
                                );
                                tokio::time::sleep(tokio::time::Duration::from_secs(
                                    wait_seconds as u64,
                                ))
                                .await;

                                // 检查是否应该继续运行
                                if !*running.read().await {
                                    Self::write_log(&config_id, "INFO", "策略已停止，退出重试");
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        }

        // 启动网格检查任务（每2分钟）
        self.start_grid_check_task().await;

        // 启动趋势监控任务（每10分钟）
        self.start_trend_monitoring_task().await;

        // 策略启动完成
        Ok(())
    }

    /// 停止策略
    pub async fn stop(&self) -> Result<()> {
        // 停止策略

        *self.running.write().await = false;

        // 停止时取消所有订单
        if self.config.execution.shutdown_cancel_all {
            self.cancel_all_orders().await?;
        }

        // 策略已停止
        Ok(())
    }

    /// 运行单个配置线程
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
        Self::write_log(
            &config_id,
            "INFO",
            &format!("开始初始化配置: 交易对={}", config.symbol),
        );

        // 获取账户
        let account = account_manager
            .get_account(&config.account.id)
            .ok_or_else(|| {
                let err = format!("账户 {} 不存在", config.account.id);
                Self::write_log(&config_id, "ERROR", &err);
                ExchangeError::Other(err)
            })?;

        Self::write_log(&config_id, "INFO", "获取初始价格和精度...");

        // 获取初始价格和精度
        let ticker = match account
            .exchange
            .get_ticker(&config.symbol, MarketType::Futures)
            .await
        {
            Ok(t) => t,
            Err(e) => {
                Self::write_log(&config_id, "ERROR", &format!("获取ticker失败: {:?}", e));
                return Err(e);
            }
        };
        let initial_price = ticker.last;

        let symbol_info = match account
            .exchange
            .get_symbol_info(&config.symbol, MarketType::Futures)
            .await
        {
            Ok(info) => info,
            Err(e) => {
                Self::write_log(&config_id, "ERROR", &format!("获取交易对信息失败: {:?}", e));
                return Err(e);
            }
        };
        let price_precision = Self::calculate_precision(symbol_info.tick_size);
        let amount_precision = Self::calculate_precision(symbol_info.step_size);

        log::info!(
            "📊 {} - {} 初始价格: {:.4}, 价格精度: {}, 数量精度: {}",
            config_id,
            config.symbol,
            initial_price,
            price_precision,
            amount_precision
        );

        // 创建趋势计算器
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

        // 获取现有持仓
        let (position_value, current_position) =
            match account.exchange.get_positions(Some(&config.symbol)).await {
                Ok(positions) => {
                    // 查找当前交易对的持仓
                    let mut total_position = 0.0;
                    let mut position_obj = None;
                    for pos in &positions {
                        if pos.symbol == config.symbol {
                            // 根据方向计算持仓价值（空单为负）
                            let position_val = match pos.side.as_str() {
                                "LONG" => pos.contracts,
                                "SHORT" => -pos.contracts,
                                _ => pos.contracts, // 双向持仓模式或其他
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

        // 初始化配置状态
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
            // 初始化盈亏统计
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

        // 计算并提交初始网格订单
        Self::write_log(&config_id, "INFO", "计算并提交初始网格订单...");
        if let Err(e) = Self::calculate_and_submit_grid(
            &config,
            &state,
            &account_manager,
            &batch_settings,
            &trend_adjustment,
            &grid_management,
        )
        .await
        {
            Self::write_log(
                &config_id,
                "ERROR",
                &format!("提交初始网格订单失败: {:?}", e),
            );
            return Err(e);
        }
        Self::write_log(&config_id, "INFO", "初始网格订单提交成功");

        // 启动WebSocket监听
        if websocket_config.subscribe_trade_updates || websocket_config.subscribe_ticker {
            Self::write_log(&config_id, "INFO", "启动WebSocket监听...");
            if let Err(e) = Self::start_websocket_for_config(
                &config,
                &state,
                &account_manager,
                &websocket_config,
                config_states.clone(),
                &grid_management,
                &trend_adjustment,
                &batch_settings,
                &collector,
            )
            .await
            {
                Self::write_log(
                    &config_id,
                    "WARN",
                    &format!("WebSocket启动失败: {:?}, 将继续运行", e),
                );
                // WebSocket失败不应该导致策略退出
            }
        }

        // 主循环 - 改为60秒检查一次
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
        let mut loop_count = 0;

        // 进入主循环

        while *running.read().await {
            interval.tick().await;
            loop_count += 1;

            // 每10次循环输出一次心跳日志
            if loop_count % 10 == 0 {
                Self::write_log(
                    &config_id,
                    "INFO",
                    &format!("策略运行中... (循环次数: {})", loop_count),
                );
            }

            // 更新趋势强度
            if config.trend_config.show_trend_info {
                if let Err(e) =
                    Self::update_and_log_trend(&state, &account_manager, &logging_config).await
                {
                    Self::write_log(&config_id, "WARN", &format!("更新趋势失败: {:?}", e));
                }
            }

            // 显示网格状态
            if grid_management.show_grid_status {
                if let Err(e) = Self::log_grid_status(&state, &logging_config).await {
                    Self::write_log(&config_id, "WARN", &format!("显示网格状态失败: {:?}", e));
                }
            }

            // 定期检查并恢复网格
            if loop_count % 5 == 0 {
                // Self::write_log(&config_id, "INFO", "检查网格状态...");

                // 同步实际订单状态
                // Self::write_log(&config_id, "INFO", "同步交易所订单状态...");
                if let Some(account) = account_manager.get_account(&config.account.id) {
                    match account
                        .exchange
                        .get_open_orders(Some(&config.symbol), MarketType::Futures)
                        .await
                    {
                        Ok(real_orders) => {
                            // 注释掉交易所挂单数量日志
                            // Self::write_log(&config_id, "INFO",
                            //     &format!("交易所实际挂单: {} 个", real_orders.len()));

                            // 更新本地订单状态
                            let mut state_guard = state.lock().await;
                            let local_count = state_guard.active_orders.len();

                            // 检测订单数量变化
                            let expected_orders = config.grid.orders_per_side * 2; // 买单+卖单

                            // 首先，总是以交易所的订单为准，更新本地状态
                            state_guard.active_orders.clear();
                            state_guard.grid_orders.clear();

                            // 重新同步订单，只保留实际在交易所的订单
                            for order in &real_orders {
                                state_guard
                                    .active_orders
                                    .insert(order.id.clone(), order.clone());
                                state_guard
                                    .grid_orders
                                    .insert(order.id.clone(), order.clone());
                            }

                            // 现在检查网格是否均匀分布
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

                            // 检查是否需要重建网格的条件：
                            // 订单总数必须等于预期数量（100%匹配）
                            let need_rebuild = real_orders.len() != expected_orders as usize;

                            if need_rebuild {
                                Self::write_log(
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
                                // 网格正常，清除重置标记
                                state_guard.need_grid_reset = false;
                                // 不打印网格正常的日志，减少噪音
                                // Self::write_log(&config_id, "INFO",
                                //     &format!("网格状态正常 - 总订单: {}/{}, 买单: {}, 卖单: {}",
                                //         real_orders.len(), expected_orders, buy_orders.len(), sell_orders.len()));
                            }

                            drop(state_guard);
                        }
                        Err(e) => {
                            Self::write_log(
                                &config_id,
                                "ERROR",
                                &format!("获取实际订单失败: {:?}", e),
                            );
                        }
                    }
                }

                let state_guard = state.lock().await;
                let active_orders_count = state_guard.active_orders.len();
                let grid_orders_count = state_guard.grid_orders.len();
                let need_reset = state_guard.need_grid_reset;
                drop(state_guard);

                // 注释掉本地订单统计日志
                // Self::write_log(&config_id, "INFO",
                //     &format!("本地活跃订单: {}, 网格订单: {}", active_orders_count, grid_orders_count));

                // 如果需要重置或订单太少，重建网格
                let expected_orders = config.grid.orders_per_side * 2;
                if need_reset || active_orders_count < (expected_orders as usize / 2) {
                    Self::write_log(
                        &config_id,
                        "WARN",
                        &format!(
                            "需要重建网格 (need_reset={}, orders={}/{})",
                            need_reset, active_orders_count, expected_orders
                        ),
                    );

                    // 先取消所有订单
                    if let Some(account) = account_manager.get_account(&config.account.id) {
                        match account
                            .exchange
                            .cancel_all_orders(Some(&config.symbol), MarketType::Futures)
                            .await
                        {
                            Ok(cancelled) => {
                                Self::write_log(
                                    &config_id,
                                    "INFO",
                                    &format!("取消了 {} 个订单", cancelled.len()),
                                );
                            }
                            Err(e) => {
                                Self::write_log(
                                    &config_id,
                                    "ERROR",
                                    &format!("取消订单失败: {:?}", e),
                                );
                            }
                        }
                    }

                    // 清理状态
                    let mut state_guard = state.lock().await;
                    state_guard.active_orders.clear();
                    state_guard.grid_orders.clear();
                    state_guard.need_grid_reset = false;
                    drop(state_guard);

                    // 重建网格
                    if let Err(e) = Self::calculate_and_submit_grid(
                        &config,
                        &state,
                        &account_manager,
                        &batch_settings,
                        &trend_adjustment,
                        &grid_management,
                    )
                    .await
                    {
                        Self::write_log(&config_id, "ERROR", &format!("重建网格失败: {:?}", e));
                    } else {
                        Self::write_log(&config_id, "INFO", "网格重建成功");
                    }
                }
            }
        }

        Ok(())
    }

    /// 启动WebSocket监听
    async fn start_websocket_for_config(
        config: &TradingConfig,
        state: &Arc<Mutex<ConfigState>>,
        account_manager: &Arc<AccountManager>,
        websocket_config: &WebSocketConfig,
        config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
        grid_management: &GridManagement,
        trend_adjustment: &TrendAdjustment,
        batch_settings: &BatchSettings,
        collector: &Option<Arc<TradeCollector>>,
    ) -> Result<()> {
        let account = account_manager
            .get_account(&config.account.id)
            .ok_or_else(|| ExchangeError::Other(format!("账户 {} 不存在", config.account.id)))?;

        // 对于Binance，如果需要订阅订单更新或成交数据，需要先创建listenKey
        if config.account.exchange == "binance"
            && (websocket_config.subscribe_order_updates
                || websocket_config.subscribe_trade_updates)
        {
            // 创建listenKey
            let global_config =
                crate::core::config::GlobalConfig::from_file("config/exchanges.yaml")
                    .unwrap_or_else(|_| crate::core::config::GlobalConfig::default());
            let exchange_config = crate::core::config::Config::from_exchange_config(
                global_config.get_exchange_config("binance").unwrap_or(
                    &crate::core::config::ExchangeConfig {
                        name: "binance".to_string(),
                        testnet: false,
                        base_url: "https://api.binance.com".to_string(),
                        websocket_url: "wss://stream.binance.com:9443".to_string(),
                        symbol_separator: "".to_string(),
                        symbol_format: "{base}{quote}".to_string(),
                        rate_limits: crate::core::config::RateLimits {
                            requests_per_minute: Some(1200),
                            requests_per_second: Some(20),
                            orders_per_minute: Some(100),
                        },
                        endpoints: std::collections::HashMap::new(),
                    },
                ),
            );
            let binance_exchange = crate::exchanges::binance::BinanceExchange::new(
                exchange_config,
                crate::core::config::ApiKeys {
                    api_key: std::env::var(format!("{}_API_KEY", config.account.env_prefix))
                        .unwrap_or_default(),
                    api_secret: std::env::var(format!("{}_SECRET_KEY", config.account.env_prefix))
                        .unwrap_or_default(),
                    passphrase: None,
                    memo: None,
                },
            );

            // 使用带自动续期的ListenKey创建
            match binance_exchange
                .create_listen_key_with_auto_renewal(MarketType::Futures)
                .await
            {
                Ok(listen_key) => {
                    log::info!("✅ {} 获取到listenKey（已启动自动续期）", config.config_id);

                    // 创建带listenKey的WebSocket URL
                    let ws_url = format!("wss://fstream.binance.com/ws/{}", listen_key);
                    let mut ws_client = crate::exchanges::binance::BinanceWebSocketClient::new(
                        ws_url,
                        MarketType::Futures,
                    );

                    // 连接WebSocket
                    if let Err(e) = ws_client.connect().await {
                        log::warn!("⚠️ {} WebSocket连接失败: {}", config.config_id, e);
                        return Ok(());
                    }

                    log::info!(
                        "📡 {} 成功连接用户数据流WebSocket (订单更新已启用)",
                        config.config_id
                    );

                    // 创建消息处理器
                    let handler = TradeHandler {
                        config_id: config.config_id.clone(),
                        config: config.clone(),
                        state: state.clone(),
                        account_manager: account_manager.clone(),
                        config_states,
                        grid_management: grid_management.clone(),
                        trend_adjustment: trend_adjustment.clone(),
                        batch_settings: batch_settings.clone(),
                        log_all_trades: websocket_config.log_all_trades,
                        processed_trades: Arc::new(Mutex::new(HashSet::new())),
                        collector: collector.clone(),
                    };

                    // 使用BinanceMessageHandler包装
                    let binance_handler = crate::exchanges::binance::BinanceMessageHandler::new(
                        Box::new(handler),
                        MarketType::Futures,
                    );

                    // 启动接收消息
                    let handler = binance_handler;
                    let ws_config_id = config.config_id.clone();
                    tokio::spawn(async move {
                        log::debug!("🔄 {} WebSocket消息接收循环已启动", ws_config_id);
                        let mut msg_count = 0;

                        // 循环接收消息
                        loop {
                            match ws_client.receive().await {
                                Ok(Some(msg)) => {
                                    msg_count += 1;

                                    // 只记录debug级别的原始消息
                                    log::debug!(
                                        "📡 {} 收到WebSocket消息#{}",
                                        ws_config_id,
                                        msg_count
                                    );

                                    // 检查重要事件（已在ExecutionReport处理中输出，这里不需要重复）
                                    if msg.contains("ORDER_TRADE_UPDATE") {
                                        log::debug!(
                                            "🎯 {} 检测到ORDER_TRADE_UPDATE订单更新事件",
                                            ws_config_id
                                        );
                                    }

                                    // 检查是否包含executionReport
                                    if msg.contains("executionReport") {
                                        log::info!(
                                            "🎯 {} 检测到executionReport订单执行报告",
                                            ws_config_id
                                        );
                                    }

                                    // 检查是否是ListenKey过期消息
                                    if msg.contains("listenKeyExpired") {
                                        log::error!(
                                            "❌ {} ListenKey已过期！自动续期应该已处理此问题",
                                            ws_config_id
                                        );
                                        // 注意：由于已启用自动续期，这种情况不应该发生
                                        // 如果发生，说明自动续期失败，需要重新连接
                                        break;
                                    }

                                    // 解析Binance消息
                                    match ws_client.parse_binance_message(&msg) {
                                        Ok(ws_msg) => {
                                            log::debug!("✅ {} 消息解析成功", ws_config_id);
                                            if let Err(e) = handler.handle_message(ws_msg).await {
                                                log::error!(
                                                    "❌ {} WebSocket消息处理错误: {}",
                                                    ws_config_id,
                                                    e
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            log::debug!("⚠️ {} 消息解析失败: {}", ws_config_id, e);
                                        }
                                    }
                                }
                                Ok(None) => {
                                    // 没有消息
                                    tokio::time::sleep(tokio::time::Duration::from_millis(100))
                                        .await;
                                }
                                Err(e) => {
                                    log::error!("❌ {} WebSocket接收错误: {}", ws_config_id, e);
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    log::warn!("⚠️ {} 获取listenKey失败: {}", config.config_id, e);
                }
            }

            return Ok(());
        }

        // 其他交易所的WebSocket连接
        if let Ok(mut ws_client) = account
            .exchange
            .create_websocket_client(MarketType::Futures)
            .await
        {
            // 连接WebSocket
            if let Err(e) = ws_client.connect().await {
                log::warn!(
                    "⚠️ WebSocket连接失败 {}: {}，将继续运行策略",
                    config.config_id,
                    e
                );
                // 不退出，继续运行策略主循环
            }

            // 订阅市场成交数据
            if websocket_config.subscribe_trade_updates {
                // 使用send方法发送订阅消息
                let subscribe_msg = crate::core::websocket::build_subscribe_message(
                    "binance",
                    "trade",
                    &config.symbol,
                );
                if let Err(e) = ws_client.send(subscribe_msg).await {
                    log::warn!("⚠️ 订阅市场成交失败 {}: {}", config.config_id, e);
                } else {
                    log::debug!("📡 {} 成功订阅市场成交数据", config.config_id);
                }
            }

            // 订阅Ticker数据
            if websocket_config.subscribe_ticker {
                let subscribe_msg = crate::core::websocket::build_subscribe_message(
                    "binance",
                    "ticker",
                    &config.symbol,
                );
                if let Err(e) = ws_client.send(subscribe_msg).await {
                    log::warn!("⚠️ 订阅Ticker失败 {}: {}", config.config_id, e);
                } else {
                    log::debug!("📡 {} 成功订阅Ticker数据", config.config_id);
                }
            }

            // 创建消息处理器
            let handler = TradeHandler {
                config_id: config.config_id.clone(),
                config: config.clone(),
                state: state.clone(),
                account_manager: account_manager.clone(),
                config_states,
                grid_management: grid_management.clone(),
                trend_adjustment: trend_adjustment.clone(),
                batch_settings: batch_settings.clone(),
                log_all_trades: websocket_config.log_all_trades,
                processed_trades: Arc::new(Mutex::new(HashSet::new())),
                collector: collector.clone(),
            };

            // 启动接收消息
            tokio::spawn(async move {
                // 循环接收消息
                loop {
                    match ws_client.receive().await {
                        Ok(Some(msg)) => {
                            // 将字符串消息转换为WsMessage
                            let ws_msg = WsMessage::Text(msg);
                            if let Err(e) = handler.handle_message(ws_msg).await {
                                log::error!("WebSocket消息处理错误: {}", e);
                            }
                        }
                        Ok(None) => {
                            // 没有消息
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                        Err(e) => {
                            log::error!("WebSocket接收错误: {}", e);
                            break;
                        }
                    }
                }
            });

            // 保存WebSocket客户端引用
            // state.lock().await.ws_client = Some(ws_client);
        }

        Ok(())
    }

    /// 计算并提交网格订单
    async fn calculate_and_submit_grid(
        config: &TradingConfig,
        state: &Arc<Mutex<ConfigState>>,
        account_manager: &AccountManager,
        batch_settings: &BatchSettings,
        trend_adjustment: &TrendAdjustment,
        grid_management: &GridManagement,
    ) -> Result<()> {
        let mut state_guard = state.lock().await;

        // 清空现有网格
        state_guard.grid_orders.clear();

        // 创建临时的网格订单列表用于构建
        let mut temp_grid_orders: Vec<GridOrder> = Vec::new();

        // 始终获取最新的市场价格，确保以最新价格为中心计算网格
        let mut current_price = state_guard.current_price;

        // 获取最新的ticker价格
        if let Some(account) = account_manager.get_account(&config.account.id) {
            match account
                .exchange
                .get_ticker(&config.symbol, MarketType::Futures)
                .await
            {
                Ok(ticker) => {
                    current_price = ticker.last;
                    state_guard.current_price = current_price;
                    log::info!(
                        "🔄 {} 重置网格前获取最新价格: {:.4}",
                        config.config_id,
                        current_price
                    );
                }
                Err(e) => {
                    log::warn!(
                        "⚠️ {} 无法获取最新价格，使用缓存价格: {:.4}, 错误: {}",
                        config.config_id,
                        current_price,
                        e
                    );
                }
            }
        }

        let spacing = config.grid.spacing;
        let orders_per_side = config.grid.orders_per_side;
        let order_amount = config.grid.order_amount;

        // 从配置文件获取趋势调整倍数 - 按照需求调整
        let (buy_multiplier, sell_multiplier) = match state_guard.trend_strength {
            TrendStrength::StrongBull => (trend_adjustment.strong_bull_buy_multiplier, 1.0), // 强上涨：多单2倍
            TrendStrength::Bull => (trend_adjustment.bull_buy_multiplier, 1.0), // 弱上涨：多单1.5倍
            TrendStrength::Neutral => (1.0, 1.0),                               // 中性：不调整
            TrendStrength::Bear => (1.0, trend_adjustment.bear_sell_multiplier), // 弱下跌：空单1.5倍
            TrendStrength::StrongBear => (1.0, trend_adjustment.strong_bear_sell_multiplier), // 强下跌：空单2倍
        };

        // 趋势判断日志（简化输出）
        log::debug!(
            "趋势: {:?} | 买单{}x 卖单{}x",
            state_guard.trend_strength,
            buy_multiplier,
            sell_multiplier
        );

        // 计算买单网格（基于最新价格）
        for i in 1..=orders_per_side {
            let price = match config.grid.spacing_type {
                SpacingType::Geometric => {
                    // 等比网格：每个价格是前一个价格的固定比例
                    // spacing 表示比例，如 0.002 表示 0.2%
                    current_price * (1.0 - spacing).powi(i as i32)
                }
                SpacingType::Arithmetic => {
                    // 等差网格：固定价格间距
                    // spacing 是绝对价格差，如 0.0008 表示每格相差 0.0008 USDC
                    current_price - (spacing * i as f64)
                }
            };
            let price = Self::round_price(price, state_guard.price_precision);

            // 计算合约数量：USDC金额 / 合约价格
            let adjusted_amount = order_amount * buy_multiplier;
            let amount = if state_guard.amount_precision == 0 {
                // 整数张数
                (adjusted_amount / price).round()
            } else {
                // 带小数的张数
                Self::round_amount(adjusted_amount / price, state_guard.amount_precision)
            };

            // 确保最小订单量
            if amount * price >= 5.0 {
                // Binance最小5 USDC
                temp_grid_orders.push(GridOrder {
                    price,
                    amount,
                    side: OrderSide::Buy,
                    order_id: None,
                });
            }
        }

        // 计算卖单网格
        for i in 1..=orders_per_side {
            let price = match config.grid.spacing_type {
                SpacingType::Geometric => {
                    // 等比网格：每个价格是前一个价格的固定比例
                    // spacing 表示比例，如 0.002 表示 0.2%
                    current_price * (1.0 + spacing).powi(i as i32)
                }
                SpacingType::Arithmetic => {
                    // 等差网格：固定价格间距
                    // spacing 是绝对价格差，如 0.0008 表示每格相差 0.0008 USDC
                    current_price + (spacing * i as f64)
                }
            };
            let price = Self::round_price(price, state_guard.price_precision);

            // 计算合约数量：USDC金额 / 合约价格
            let adjusted_amount = order_amount * sell_multiplier;
            let amount = if state_guard.amount_precision == 0 {
                // 整数张数
                (adjusted_amount / price).round()
            } else {
                // 带小数的张数
                Self::round_amount(adjusted_amount / price, state_guard.amount_precision)
            };

            // 确保最小订单量
            if amount * price >= 5.0 {
                // Binance最小5 USDC
                temp_grid_orders.push(GridOrder {
                    price,
                    amount,
                    side: OrderSide::Sell,
                    order_id: None,
                });
            }
        }

        // 提交订单
        let account = account_manager
            .get_account(&config.account.id)
            .ok_or_else(|| ExchangeError::Other(format!("账户 {} 不存在", config.account.id)))?;

        let batch_size = match account.exchange_name.as_str() {
            "binance" => batch_settings.binance_batch_size,
            "okx" => batch_settings.okx_batch_size,
            "hyperliquid" => batch_settings.hyperliquid_batch_size,
            _ => batch_settings.default_batch_size,
        };

        // 分批提交
        let mut success_count = 0;
        let mut fail_count = 0;

        // 使用临时网格订单列表
        for chunk in temp_grid_orders.chunks(batch_size as usize) {
            let orders: Vec<OrderRequest> = chunk
                .iter()
                .map(|grid_order| {
                    let mut order = OrderRequest::new(
                        config.symbol.clone(),
                        grid_order.side.clone(),
                        OrderType::Limit,
                        grid_order.amount,
                        Some(grid_order.price),
                        MarketType::Futures,
                    );
                    // 生成策略识别的订单ID
                    let tag = if grid_order.side == OrderSide::Buy {
                        "B"
                    } else {
                        "S"
                    };
                    order.client_order_id = Some(generate_order_id_with_tag(
                        "trend_grid_v2",
                        &account.exchange_name,
                        tag,
                    ));
                    order
                })
                .collect();

            // 打印订单详情
            log::debug!("📝 {} 准备提交 {} 个订单", config.config_id, orders.len());
            for (i, order) in orders.iter().enumerate() {
                log::debug!(
                    "  订单{}: {:?} {} @ {:.4}, 数量: {}",
                    i + 1,
                    order.side,
                    order.symbol,
                    order.price.unwrap_or(0.0),
                    order.amount
                );
            }

            match account_manager
                .create_batch_orders(&config.account.id, orders)
                .await
            {
                Ok(response) => {
                    log::info!(
                        "📊 {} 批次结果: {} 成功, {} 失败",
                        config.config_id,
                        response.successful_orders.len(),
                        response.failed_orders.len()
                    );

                    success_count += response.successful_orders.len();
                    fail_count += response.failed_orders.len();

                    // 保存订单ID到active_orders和grid_orders
                    for order in response.successful_orders {
                        log::debug!("  ✅ 订单 {} 创建成功", order.id);
                        state_guard
                            .active_orders
                            .insert(order.id.clone(), order.clone());
                        state_guard.grid_orders.insert(order.id.clone(), order);
                    }

                    // 记录失败原因
                    for failed in response.failed_orders {
                        log::warn!(
                            "  ⚠️ 订单失败: {} - {}",
                            failed.order_request.symbol,
                            failed.error_message
                        );
                    }
                }
                Err(e) => {
                    log::error!("❌ {} 批量下单失败: {}", config.config_id, e);
                    fail_count += chunk.len();
                }
            }
        }

        log::info!(
            "✅ {} 网格订单提交完成: {} 成功, {} 失败",
            config.config_id,
            success_count,
            fail_count
        );

        Ok(())
    }

    /// 更新并记录趋势
    async fn update_and_log_trend(
        state: &Arc<Mutex<ConfigState>>,
        account_manager: &AccountManager,
        logging_config: &LoggingConfig,
    ) -> Result<()> {
        let mut state_guard = state.lock().await;

        // 获取最新价格
        if let Some(account) = account_manager.get_account(&state_guard.config.account.id) {
            if let Ok(ticker) = account
                .exchange
                .get_ticker(&state_guard.config.symbol, MarketType::Futures)
                .await
            {
                state_guard.current_price = ticker.last;

                // 更新趋势计算器
                if let Some(trend_value) = state_guard.trend_calculator.update(ticker.last) {
                    let new_strength =
                        crate::utils::indicators::trend_strength_to_enum(trend_value);
                    let old_strength = state_guard.trend_strength;
                    state_guard.trend_strength = new_strength;

                    if logging_config.show_trend_changes && !matches!(old_strength, new_strength) {
                        log::info!(
                            "🔄 {} 趋势变化: {:?} -> {:?} (值: {:.3}, 价格: {:.4})",
                            state_guard.config.config_id,
                            old_strength,
                            new_strength,
                            trend_value,
                            ticker.last
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// 记录网格状态
    async fn log_grid_status(
        state: &Arc<Mutex<ConfigState>>,
        logging_config: &LoggingConfig,
    ) -> Result<()> {
        let state_guard = state.lock().await;

        let active_buy_orders = state_guard
            .active_orders
            .values()
            .filter(|o| o.side == OrderSide::Buy)
            .count();
        let active_sell_orders = state_guard
            .active_orders
            .values()
            .filter(|o| o.side == OrderSide::Sell)
            .count();

        if logging_config.show_position {
            log::info!(
                "📊 {} - 价格: {:.4}, 净仓: {:.2}, 盈亏: {:.6}U (已实现: {:.6}, 未实现: {:.6}, 费: {:.6})",
                state_guard.config.config_id,
                state_guard.current_price,
                state_guard.net_position,
                state_guard.pnl,
                state_guard.realized_pnl,
                state_guard.unrealized_pnl,
                state_guard.total_fee
            );
        }
        Ok(())
    }

    /// 取消所有订单
    pub async fn cancel_all_orders(&self) -> Result<()> {
        for config in &self.config.trading_configs {
            if !config.enabled {
                continue;
            }

            match self
                .account_manager
                .cancel_all_orders(&config.account.id, Some(&config.symbol))
                .await
            {
                Ok(cancelled) => {
                    log::info!("✅ {} 取消了 {} 个订单", config.config_id, cancelled.len());
                }
                Err(e) => {
                    log::error!("❌ {} 取消订单失败: {}", config.config_id, e);
                }
            }
        }

        Ok(())
    }

    /// 启动趋势监控任务（每5分钟检查K线）
    async fn start_trend_monitoring_task(&self) {
        let config_states = self.config_states.clone();
        let running = self.running.clone();
        let account_manager = self.account_manager.clone();
        let trading_configs = self.config.trading_configs.clone();
        let batch_settings = self.config.batch_settings.clone();
        let trend_adjustment = self.config.trend_adjustment.clone();
        let grid_management = self.config.grid_management.clone();

        tokio::spawn(async move {
            // 每5分钟检查一次趋势
            let mut interval_timer = tokio::time::interval(
                tokio::time::Duration::from_secs(300), // 5分钟
            );

            while *running.read().await {
                interval_timer.tick().await;

                log::info!("📊 开始趋势监控检查...");

                let states = config_states.read().await;
                for (config_id, state) in states.iter() {
                    // 找到对应的配置
                    let config = match trading_configs.iter().find(|c| c.config_id == *config_id) {
                        Some(c) => c.clone(),
                        None => continue,
                    };

                    // 获取账户
                    let account = match account_manager.get_account(&config.account.id) {
                        Some(a) => a,
                        None => continue,
                    };

                    // 获取最新K线数据
                    let interval = match Interval::from_string(&config.trend_config.timeframe) {
                        Ok(i) => i,
                        Err(e) => {
                            log::error!("❌ {} 无效的K线周期: {}", config_id, e);
                            continue;
                        }
                    };

                    // 检查是否需要进行趋势判断（每5分钟一次）
                    let should_check_trend = {
                        let state_guard = state.lock().await;
                        let time_since_last_check = Utc::now() - state_guard.last_trend_check;
                        time_since_last_check.num_seconds() >= 300 // 5分钟
                    };

                    if !should_check_trend {
                        continue; // 跳过这次检查
                    }

                    match account
                        .exchange
                        .get_klines(
                            &config.symbol,
                            interval,
                            MarketType::Futures,
                            Some(100), // 获取100根K线用于更准确的趋势分析
                        )
                        .await
                    {
                        Ok(klines) => {
                            let mut state_guard = state.lock().await;

                            // 更新趋势检查时间
                            state_guard.last_trend_check = Utc::now();

                            // 批量更新趋势计算器（使用最近的K线数据）
                            for kline in klines.iter().rev().take(20) {
                                state_guard.trend_calculator.update(kline.close);
                            }

                            // 获取新的趋势（使用最新价格）
                            let latest_price = klines
                                .last()
                                .map(|k| k.close)
                                .unwrap_or(state_guard.current_price);
                            let new_trend_value = state_guard
                                .trend_calculator
                                .update(latest_price)
                                .unwrap_or(0.0);
                            let new_trend = trend_strength_to_enum(new_trend_value);

                            // 只有当趋势真正改变时才重置网格
                            if new_trend != state_guard.last_trend_strength {
                                log::warn!(
                                    "📈 {} 趋势变化: {:?} -> {:?} (值: {:.3})",
                                    config_id,
                                    state_guard.last_trend_strength,
                                    new_trend,
                                    new_trend_value
                                );

                                // 更新趋势
                                state_guard.trend_strength = new_trend;
                                state_guard.last_trend_strength = new_trend;

                                // 标记需要重置网格
                                state_guard.need_grid_reset = true;
                                let trend_strength = state_guard.trend_strength;

                                drop(state_guard); // 释放锁

                                // 如果不是中性趋势，考虑先进行趋势市价订单再重置网格
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

                                // 立即处理趋势市价单
                                if let Some(req) = adjustment_request {
                                    log::info!("📊 {} 因趋势变化需执行市价单: {:?} {:.1} {}, 然后将重建网格",
                                        config_id, req.order_type, req.amount, req.side);

                                    if let Some(account) =
                                        account_manager.get_account(&config.account.id)
                                    {
                                        // 先执行趋势调整的市价单
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

                                        // 执行市价订单
                                        match account.exchange.create_order(market_order).await {
                                            Ok(_) => {
                                                log::info!(
                                                    "💸 {} 趋势市价单执行成功: {} {:.1} {}",
                                                    config_id,
                                                    req.side,
                                                    req.amount,
                                                    config.symbol
                                                );
                                            }
                                            Err(e) => {
                                                log::error!(
                                                    " ❌ {} 趋势市价单执行失败: {}",
                                                    config_id,
                                                    e
                                                );
                                            }
                                        }
                                    }

                                    // 暂停一会确保交易被确认再重启网格系统
                                    tokio::time::sleep(tokio::time::Duration::from_millis(200))
                                        .await;
                                }

                                // 立即重置网格，调整下单金额
                                log::info!(
                                    "🔄 {} 因趋势变化开始重置网格，将调整下单金额",
                                    config_id
                                );
                                if let Err(e) = TrendGridStrategyV2::reset_grid_for_config(
                                    &config,
                                    state,
                                    &account_manager,
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
                            } else {
                                log::info!(
                                    "📊 {} 趋势检查完成，趋势未变化: {:?} (值: {:.3})",
                                    config_id,
                                    state_guard.trend_strength,
                                    new_trend_value
                                );
                            }
                        }
                        Err(e) => {
                            log::error!("❌ {} 获取K线失败: {}", config_id, e);
                        }
                    }
                }
            }

            log::info!("📊 趋势监控任务已停止");
        });
    }

    /// 启动网格检查任务
    async fn start_grid_check_task(&self) {
        let interval = self.config.grid_management.check_interval;
        let config_states = self.config_states.clone();
        let running = self.running.clone();
        let show_grid_status = self.config.grid_management.show_grid_status;
        let rebalance_threshold = self.config.grid_management.rebalance_threshold;
        let account_manager = self.account_manager.clone();
        let trading_configs = self.config.trading_configs.clone();
        let batch_settings = self.config.batch_settings.clone();
        let trend_adjustment = self.config.trend_adjustment.clone();
        let grid_management = self.config.grid_management.clone();

        tokio::spawn(async move {
            let mut interval_timer =
                tokio::time::interval(tokio::time::Duration::from_secs(interval));

            while *running.read().await {
                interval_timer.tick().await;

                let states = config_states.read().await;
                if show_grid_status {
                    log::debug!("检查 {} 个配置的网格状态", states.len());
                }

                // 检查每个配置的网格均匀性（简化版：只检查买卖单数量）
                for (config_id, state) in states.iter() {
                    let mut state_guard = state.lock().await;

                    // 检查是否需要重置网格
                    if TrendGridStrategyV2::check_grid_uniformity(
                        &mut state_guard,
                        rebalance_threshold,
                    ) {
                        log::warn!("⚠️ {} 网格不均匀，标记需要重置", config_id);
                        state_guard.need_grid_reset = true;
                    }

                    // 如果需要重置，立即执行
                    if state_guard.need_grid_reset {
                        // 检查距离上次重置的时间，避免频繁重置（增加到5分钟）
                        let time_since_reset = Utc::now() - state_guard.last_grid_check;
                        if time_since_reset.num_seconds() > 300 {
                            // 至少间隔5分钟
                            log::info!("🔄 {} 执行网格重置", config_id);
                            let config = state_guard.config.clone();
                            state_guard.last_grid_check = Utc::now();
                            drop(state_guard); // 释放锁

                            // 调用重置网格的函数
                            if let Err(e) = TrendGridStrategyV2::reset_grid_for_config(
                                &config,
                                state,
                                &account_manager,
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
        });
    }

    /// 重置指定配置的网格
    async fn reset_grid_for_config(
        config: &TradingConfig,
        state: &Arc<Mutex<ConfigState>>,
        account_manager: &AccountManager,
        batch_settings: &BatchSettings,
        trend_adjustment: &TrendAdjustment,
        grid_management: &GridManagement,
    ) -> Result<()> {
        log::info!("🔄 {} 开始重置网格", config.config_id);

        // 取消所有现有订单
        match account_manager
            .cancel_all_orders(&config.account.id, Some(&config.symbol))
            .await
        {
            Ok(cancelled) => {
                log::info!(
                    "✅ {} 取消了 {} 个旧订单",
                    config.config_id,
                    cancelled.len()
                );
            }
            Err(e) => {
                log::error!("❌ {} 取消订单失败: {}", config.config_id, e);
                return Err(e);
            }
        }

        // 等待一小段时间确保订单取消完成
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        // 重新计算并提交网格
        Self::calculate_and_submit_grid(
            config,
            state,
            account_manager,
            batch_settings,
            trend_adjustment,
            grid_management,
        )
        .await?;

        // 清除重置标志并更新检查时间
        let mut state_guard = state.lock().await;
        state_guard.need_grid_reset = false;
        state_guard.last_grid_check = Utc::now();

        log::info!("✅ {} 网格重置完成", config.config_id);
        Ok(())
    }

    /// 检查网格均匀性（只检查买卖单数量平衡）
    fn check_grid_uniformity(state_guard: &mut ConfigState, threshold: f64) -> bool {
        let config = &state_guard.config;
        let orders_per_side = config.grid.orders_per_side as usize;

        // 收集买单和卖单 - 只检查OPEN状态的订单
        let buy_orders_count = state_guard
            .active_orders
            .values()
            .filter(|o| o.side == OrderSide::Buy && o.status == OrderStatus::Open)
            .count();
        let sell_orders_count = state_guard
            .active_orders
            .values()
            .filter(|o| o.side == OrderSide::Sell && o.status == OrderStatus::Open)
            .count();

        // 1. 检查是否有足够的订单（至少需要最小数量的订单）
        let min_orders = 3; // 最少需要3个订单
        if buy_orders_count < min_orders || sell_orders_count < min_orders {
            log::debug!(
                "订单数量不足: 买单 {}, 卖单 {}",
                buy_orders_count,
                sell_orders_count
            );
            return true; // 需要重置
        }

        // 2. 检查买卖单数量是否平衡
        // 允许较大差异（比如相差30%），因为可能有订单正在成交
        let order_diff = (buy_orders_count as i32 - sell_orders_count as i32).abs();
        let total_orders = buy_orders_count + sell_orders_count;

        // 计算不平衡比例
        let imbalance_ratio = order_diff as f64 / total_orders.max(1) as f64;

        // 如果不平衡比例超过30%，认为需要重置（放宽条件，减少误判）
        if imbalance_ratio > 0.3 {
            let now = chrono::Local::now();
            log::warn!(
                "[网格不均匀] {} 买单:{} 卖单:{} 总计:{} 不平衡度:{:.1}%",
                now.format("%H:%M:%S"),
                buy_orders_count,
                sell_orders_count,
                total_orders,
                imbalance_ratio * 100.0
            );
            return true; // 需要重置
        }

        // 网格均匀性检查通过（只检查买卖单数量）
        log::debug!(
            "网格均匀性检查通过: 买单 {}, 卖单 {}",
            buy_orders_count,
            sell_orders_count
        );
        false // 网格均匀，不需要重置
    }

    /// 计算精度
    fn calculate_precision(step: f64) -> u32 {
        if step == 0.0 {
            return 8;
        }
        let s = format!("{:.10}", step);
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() > 1 {
            parts[1].trim_end_matches('0').len() as u32
        } else {
            0
        }
    }

    /// 价格精度处理
    fn round_price(price: f64, precision: u32) -> f64 {
        let multiplier = 10_f64.powi(precision as i32);
        (price * multiplier).round() / multiplier
    }

    /// 数量精度处理
    fn round_amount(amount: f64, precision: u32) -> f64 {
        let multiplier = 10_f64.powi(precision as i32);
        (amount * multiplier).round() / multiplier
    }
}

/// 成交处理器
struct TradeHandler {
    config_id: String,
    config: TradingConfig,
    state: Arc<Mutex<ConfigState>>,
    account_manager: Arc<AccountManager>,
    config_states: Arc<RwLock<HashMap<String, Arc<Mutex<ConfigState>>>>>,
    grid_management: GridManagement,
    trend_adjustment: TrendAdjustment,
    batch_settings: BatchSettings,
    log_all_trades: bool,
    processed_trades: Arc<Mutex<HashSet<String>>>, // 记录已处理的成交ID，避免重复
    collector: Option<Arc<TradeCollector>>,        // 数据收集器
}

#[async_trait]
impl MessageHandler for TradeHandler {
    async fn handle_message(&self, message: WsMessage) -> Result<()> {
        // 记录消息类型（DEBUG级别）
        log::debug!(
            "📨 {} TradeHandler处理消息类型: {:?}",
            self.config_id,
            std::mem::discriminant(&message)
        );

        match message {
            WsMessage::Trade(trade) => {
                // TRADE_LITE 事件已在binance.rs中记录，这里不重复记录
                // 不处理 TRADE_LITE，等待更完整的 ORDER_TRADE_UPDATE
            }
            WsMessage::Ticker(ticker) => {
                // 更新价格
                let mut state_guard = self.state.lock().await;
                state_guard.current_price = ticker.last;

                // 更新趋势
                if let Some(trend_value) = state_guard.trend_calculator.update(ticker.last) {
                    let new_strength =
                        crate::utils::indicators::trend_strength_to_enum(trend_value);
                    let old_strength = state_guard.trend_strength;
                    state_guard.trend_strength = new_strength;
                    if !matches!(old_strength, new_strength) {
                        log::info!("🔄 {} 趋势更新: {:?}", self.config_id, new_strength);
                    }
                }
            }
            WsMessage::ExecutionReport(report) => {
                // 比较时转换格式（Binance返回ENAUSDC，配置中是ENA/USDC）
                let normalized_symbol = report.symbol.replace("/", "");
                let config_symbol = self.config.symbol.replace("/", "");

                // 检查是否是当前策略的订单
                let state_guard = self.state.lock().await;
                let is_my_order = state_guard.active_orders.contains_key(&report.order_id)
                    || state_guard.grid_orders.contains_key(&report.order_id);
                drop(state_guard);

                // 只有属于当前交易对且是当前策略的订单才输出日志
                if normalized_symbol == config_symbol && is_my_order {
                    log::info!(
                        "📬 {} 收到订单执行报告: 订单ID={}, 状态={:?}, 价格={:.4}, 数量={:.2}",
                        self.config_id,
                        report.order_id,
                        report.status,
                        report.executed_price,
                        report.executed_amount
                    );
                }

                log::debug!(
                    "🔍 {} 符号匹配: report='{}' config='{}' 是否为我的订单={} 状态={:?}",
                    self.config_id,
                    normalized_symbol,
                    config_symbol,
                    is_my_order,
                    report.status
                );

                // 只处理属于当前策略的订单
                if normalized_symbol == config_symbol
                    && is_my_order
                    && report.status == OrderStatus::Closed
                {
                    // 检查是否已处理过这笔成交
                    // 使用订单ID+价格+数量+时间戳生成唯一ID，避免重复处理
                    let trade_id = format!(
                        "{}_{}_{:.4}_{:.2}_{:?}",
                        report.order_id,
                        report.timestamp.timestamp_millis(),
                        report.executed_price,
                        report.executed_amount,
                        report.side
                    );

                    let mut processed = self.processed_trades.lock().await;
                    if processed.contains(&trade_id) {
                        log::warn!(
                            "⚠️ {} 检测到重复成交事件，跳过处理: 订单{} 价格{:.4} 数量{:.2}",
                            self.config_id,
                            report.order_id,
                            report.executed_price,
                            report.executed_amount
                        );
                        return Ok(());
                    }
                    processed.insert(trade_id.clone());

                    // 清理旧的记录（保留最近2000条，增加缓存大小）
                    if processed.len() > 2000 {
                        // 保留最近的1500条
                        let to_remove: Vec<String> = processed.iter().take(500).cloned().collect();
                        for id in to_remove {
                            processed.remove(&id);
                        }
                    }
                    drop(processed);

                    // 使用WebSocket消息中的is_maker字段判断
                    let is_maker = report.is_maker;

                    // 如果是吃单成交，立即重置网格
                    if !is_maker {
                        let now = chrono::Local::now();
                        log::warn!(
                            "[市价单] {} 成交市价单，立即重置网格",
                            now.format("%H:%M:%S")
                        );

                        // 先更新状态
                        let mut state_guard = self.state.lock().await;
                        if report.status == OrderStatus::Closed {
                            state_guard.active_orders.remove(&report.order_id);
                            log::debug!(
                                "🗑️ {} 移除已完全成交订单: {}",
                                self.config_id,
                                report.order_id
                            );
                        }

                        log::info!(
                            "🎯 {} 订单成交: {} {:?} @ {:.4} [吃单方-立即重置]",
                            self.config_id,
                            report.executed_amount,
                            report.side,
                            report.executed_price
                        );

                        // 更新成交统计
                        state_guard.last_trade_price = report.executed_price;
                        state_guard.last_trade_time = report.timestamp;
                        state_guard.trades_count += 1;
                        state_guard.total_fee += report.commission;

                        // 释放锁并立即执行网格重置
                        drop(state_guard);

                        // 立即重置网格
                        log::info!("🔄 {} 开始立即重置网格", self.config_id);
                        if let Err(e) = TrendGridStrategyV2::reset_grid_for_config(
                            &self.config,
                            &self.state,
                            self.account_manager.as_ref(),
                            &self.batch_settings,
                            &self.trend_adjustment,
                            &self.grid_management,
                        )
                        .await
                        {
                            log::error!("❌ {} 吃单触发的网格重置失败: {}", self.config_id, e);
                        } else {
                            log::info!("✅ {} 吃单触发的网格重置成功", self.config_id);
                        }

                        // 重要：吃单成交后已经重置网格，不需要再调用handle_grid_adjustment
                        return Ok(());
                    }

                    // 挂单成交，正常处理
                    let mut state_guard = self.state.lock().await;

                    // 如果订单完全成交，从活动订单中移除
                    if report.status == OrderStatus::Closed {
                        state_guard.active_orders.remove(&report.order_id);
                        log::debug!(
                            "🗑️ {} 移除已完全成交订单: {}",
                            self.config_id,
                            report.order_id
                        );
                    }

                    // 订单成交信息已在binance.rs中输出
                    state_guard.last_trade_price = report.executed_price;
                    state_guard.last_trade_time = report.timestamp;
                    state_guard.trades_count += 1;

                    // 计算手续费
                    let fee_amount = report.commission;
                    state_guard.total_fee += fee_amount;

                    // 更新持仓和盈亏统计
                    match report.side {
                        OrderSide::Buy => {
                            let volume = report.executed_amount * report.executed_price;
                            state_guard.position += volume;
                            state_guard.total_buy_volume += volume;
                            state_guard.total_buy_amount += report.executed_amount;
                            state_guard.net_position += report.executed_amount;

                            // 更新平均买入价格
                            if state_guard.total_buy_amount > 0.0 {
                                state_guard.avg_buy_price =
                                    state_guard.total_buy_volume / state_guard.total_buy_amount;
                            }
                        }
                        OrderSide::Sell => {
                            let volume = report.executed_amount * report.executed_price;
                            state_guard.position -= volume;
                            state_guard.total_sell_volume += volume;
                            state_guard.total_sell_amount += report.executed_amount;
                            state_guard.net_position -= report.executed_amount;

                            // 更新平均卖出价格
                            if state_guard.total_sell_amount > 0.0 {
                                state_guard.avg_sell_price =
                                    state_guard.total_sell_volume / state_guard.total_sell_amount;
                            }

                            // 计算已实现盈亏（卖出时实现）
                            if state_guard.avg_buy_price > 0.0 {
                                let profit = (report.executed_price - state_guard.avg_buy_price)
                                    * report.executed_amount;
                                state_guard.realized_pnl += profit;
                            }
                        }
                    }

                    // 计算未实现盈亏
                    if state_guard.net_position != 0.0 && state_guard.avg_buy_price > 0.0 {
                        state_guard.unrealized_pnl = (state_guard.current_price
                            - state_guard.avg_buy_price)
                            * state_guard.net_position;
                    }

                    // 总盈亏 = 已实现 + 未实现 - 手续费
                    state_guard.pnl = state_guard.realized_pnl + state_guard.unrealized_pnl
                        - state_guard.total_fee;

                    // 保存交易记录到数据库
                    if let Some(ref collector) = self.collector {
                        let trade_data = TradeData {
                            trade_time: report.timestamp,
                            strategy_name: format!("trend_grid_v2_{}", self.config_id),
                            account_id: self.config.account.id.clone(),
                            exchange: self.config.account.exchange.clone(),
                            symbol: report.symbol.clone(),
                            side: format!("{:?}", report.side),
                            order_type: Some("Limit".to_string()),
                            price: Decimal::from_f64_retain(report.executed_price)
                                .unwrap_or_default(),
                            amount: Decimal::from_f64_retain(report.executed_amount)
                                .unwrap_or_default(),
                            value: Some(
                                Decimal::from_f64_retain(
                                    report.executed_price * report.executed_amount,
                                )
                                .unwrap_or_default(),
                            ),
                            fee: Some(
                                Decimal::from_f64_retain(report.commission).unwrap_or_default(),
                            ),
                            fee_currency: Some(report.commission_asset.clone()),
                            position_side: None,
                            realized_pnl: if report.side == OrderSide::Sell
                                && state_guard.avg_buy_price > 0.0
                            {
                                Some(
                                    Decimal::from_f64_retain(
                                        (report.executed_price - state_guard.avg_buy_price)
                                            * report.executed_amount,
                                    )
                                    .unwrap_or_default(),
                                )
                            } else {
                                None
                            },
                            pnl_percentage: None,
                            order_id: report.order_id.clone(),
                            parent_order_id: None,
                            metadata: None,
                        };

                        let collector_clone = collector.clone();
                        tokio::spawn(async move {
                            if let Err(e) = collector_clone.record_trade(trade_data).await {
                                log::error!("❌ 保存交易记录失败: {}", e);
                            } else {
                                log::debug!("💾 交易记录已保存到数据库");
                            }
                        });
                    }

                    drop(state_guard); // 释放锁

                    // 创建Trade对象用于网格调整
                    let trade = Trade {
                        id: report.order_id.clone(),
                        order_id: Some(report.order_id.clone()),
                        symbol: report.symbol.clone(),
                        price: report.executed_price,
                        amount: report.executed_amount,
                        timestamp: report.timestamp,
                        side: report.side,
                        fee: Some(Fee {
                            currency: report.commission_asset.clone(),
                            cost: report.commission,
                            rate: None,
                        }),
                    };

                    // 处理网格调整逻辑
                    if let Err(e) = self.handle_grid_adjustment(&trade).await {
                        log::error!("❌ {} 网格调整失败: {}", self.config_id, e);
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    // handle_state_change方法已从 trait 中移除

    async fn handle_error(&self, error: ExchangeError) -> Result<()> {
        log::error!("❌ {} WebSocket错误: {}", self.config_id, error);
        Ok(())
    }
}

impl TradeHandler {
    /// 处理网格调整
    async fn handle_grid_adjustment(&self, trade: &Trade) -> Result<()> {
        // 使用实时计算处理成交
        log::debug!("📝 {} 实时计算处理成交", self.config_id);

        // 获取账户和现有订单
        let account = match self.account_manager.get_account(&self.config.account.id) {
            Some(acc) => acc,
            None => {
                log::error!("❌ 账户 {} 不存在", self.config.account.id);
                return Ok(());
            }
        };

        // 获取现有订单
        let open_orders = match account
            .exchange
            .get_open_orders(Some(&self.config.symbol), MarketType::Futures)
            .await
        {
            Ok(orders) => orders,
            Err(e) => {
                log::error!("❌ 获取挂单失败: {}，触发网格重置", e);
                // 触发网格重置而不是退出
                {
                    let mut state = self.state.lock().await;
                    state.need_grid_reset = true;
                }
                Vec::new() // 返回空订单列表，让后续逻辑处理网格重置
            }
        };

        let state_guard = self.state.lock().await;
        let spacing = self.config.grid.spacing;
        let spacing_type = self.config.grid.spacing_type.clone();
        let orders_per_side = self.config.grid.orders_per_side;
        let order_amount = self.config.grid.order_amount;

        // 网格调整规则：
        // 成交一个订单后：
        // 1. 在对侧最近的位置补充一个新订单（成交价+/-1个间距）
        // 2. 找到现有订单中最远的价格，在更远处补充一个新订单
        // 3. 取消边缘订单以保持固定数量

        // 获取当前买卖订单
        let mut buy_orders: Vec<f64> = open_orders
            .iter()
            .filter(|o| o.side == OrderSide::Buy && o.price.is_some())
            .map(|o| o.price.unwrap())
            .collect();
        let mut sell_orders: Vec<f64> = open_orders
            .iter()
            .filter(|o| o.side == OrderSide::Sell && o.price.is_some())
            .map(|o| o.price.unwrap())
            .collect();

        buy_orders.sort_by(|a, b| b.partial_cmp(a).unwrap()); // 从高到低
        sell_orders.sort_by(|a, b| a.partial_cmp(b).unwrap()); // 从低到高

        let new_orders = match trade.side {
            OrderSide::Buy => {
                // 成交一个买单后（滚动网格）：
                // 1. 在成交价格+网格间距位置挂1个卖单（近端）
                // 2. 在最低买单价格-网格间距位置挂1个新买单（远端）

                let (sell_price, buy_price) = match spacing_type {
                    SpacingType::Arithmetic => {
                        // 等差网格
                        // 新卖单：成交价 + 间距（近端）
                        let sell_price = TrendGridStrategyV2::round_price(
                            trade.price + spacing,
                            state_guard.price_precision,
                        );

                        // 新买单：在网格远端（最低买单 - 间距）
                        let buy_price = if !buy_orders.is_empty() {
                            // 获取最低的买单价格
                            let lowest_buy = buy_orders
                                .iter()
                                .min_by(|a, b| a.partial_cmp(b).unwrap())
                                .unwrap();
                            TrendGridStrategyV2::round_price(
                                lowest_buy - spacing,
                                state_guard.price_precision,
                            )
                        } else {
                            // 如果没有买单，则基于成交价 - 间距*网格数量
                            TrendGridStrategyV2::round_price(
                                trade.price - spacing * orders_per_side as f64,
                                state_guard.price_precision,
                            )
                        };

                        (sell_price, buy_price)
                    }
                    SpacingType::Geometric => {
                        // 等比网格
                        // 新卖单：成交价 * (1+间距)（近端）
                        let sell_price = TrendGridStrategyV2::round_price(
                            trade.price * (1.0 + spacing),
                            state_guard.price_precision,
                        );

                        // 新买单：在网格远端
                        let buy_price = if !buy_orders.is_empty() {
                            let lowest_buy = buy_orders
                                .iter()
                                .min_by(|a, b| a.partial_cmp(b).unwrap())
                                .unwrap();
                            TrendGridStrategyV2::round_price(
                                lowest_buy / (1.0 + spacing),
                                state_guard.price_precision,
                            )
                        } else {
                            TrendGridStrategyV2::round_price(
                                trade.price / f64::powi(1.0 + spacing, orders_per_side as i32),
                                state_guard.price_precision,
                            )
                        };

                        (sell_price, buy_price)
                    }
                };

                // 根据趋势动态调整订单金额（使用配置文件参数）
                let (buy_multiplier, sell_multiplier) = match state_guard.trend_strength {
                    TrendStrength::StrongBull => {
                        (self.trend_adjustment.strong_bull_buy_multiplier, 1.0)
                    } // 强上涨：买单倍数
                    TrendStrength::Bull => (self.trend_adjustment.bull_buy_multiplier, 1.0), // 弱上涨：买单倍数
                    TrendStrength::Neutral => (1.0, 1.0), // 中性：均衡
                    TrendStrength::Bear => (1.0, self.trend_adjustment.bear_sell_multiplier), // 弱下跌：卖单倍数
                    TrendStrength::StrongBear => {
                        (1.0, self.trend_adjustment.strong_bear_sell_multiplier)
                    } // 强下跌：卖单倍数
                };

                // 计算订单金额，确保不低于最小订单金额
                let min_order_amount = 5.0; // 最小订单金额 5 USDT
                let sell_order_amount = (order_amount * sell_multiplier).max(min_order_amount);
                let buy_order_amount = (order_amount * buy_multiplier).max(min_order_amount);

                let new_orders = vec![
                    OrderRequest::new(
                        self.config.symbol.clone(),
                        OrderSide::Sell,
                        OrderType::Limit,
                        TrendGridStrategyV2::round_amount(
                            sell_order_amount / sell_price,
                            state_guard.amount_precision,
                        ),
                        Some(sell_price),
                        MarketType::Futures,
                    ),
                    OrderRequest::new(
                        self.config.symbol.clone(),
                        OrderSide::Buy,
                        OrderType::Limit,
                        TrendGridStrategyV2::round_amount(
                            buy_order_amount / buy_price,
                            state_guard.amount_precision,
                        ),
                        Some(buy_price),
                        MarketType::Futures,
                    ),
                ];

                new_orders
            }
            OrderSide::Sell => {
                // 成交一个卖单后（滚动网格）：
                // 1. 在成交价格-网格间距位置挂1个买单（近端）
                // 2. 在最高卖单价格+网格间距位置挂1个新卖单（远端）

                let (buy_price, sell_price) = match spacing_type {
                    SpacingType::Arithmetic => {
                        // 等差网格
                        // 新买单：成交价 - 间距（近端）
                        let buy_price = TrendGridStrategyV2::round_price(
                            trade.price - spacing,
                            state_guard.price_precision,
                        );

                        // 新卖单：在网格远端（最高卖单 + 间距）
                        let sell_price = if !sell_orders.is_empty() {
                            // 获取最高的卖单价格
                            let highest_sell = sell_orders
                                .iter()
                                .max_by(|a, b| a.partial_cmp(b).unwrap())
                                .unwrap();
                            TrendGridStrategyV2::round_price(
                                highest_sell + spacing,
                                state_guard.price_precision,
                            )
                        } else {
                            // 如果没有卖单，则基于成交价 + 间距*网格数量
                            TrendGridStrategyV2::round_price(
                                trade.price + spacing * orders_per_side as f64,
                                state_guard.price_precision,
                            )
                        };

                        (buy_price, sell_price)
                    }
                    SpacingType::Geometric => {
                        // 等比网格
                        // 新买单：成交价 / (1+间距)（近端）
                        let buy_price = TrendGridStrategyV2::round_price(
                            trade.price / (1.0 + spacing),
                            state_guard.price_precision,
                        );

                        // 新卖单：在网格远端
                        let sell_price = if !sell_orders.is_empty() {
                            let highest_sell = sell_orders
                                .iter()
                                .max_by(|a, b| a.partial_cmp(b).unwrap())
                                .unwrap();
                            TrendGridStrategyV2::round_price(
                                highest_sell * (1.0 + spacing),
                                state_guard.price_precision,
                            )
                        } else {
                            TrendGridStrategyV2::round_price(
                                trade.price * f64::powi(1.0 + spacing, orders_per_side as i32),
                                state_guard.price_precision,
                            )
                        };

                        (buy_price, sell_price)
                    }
                };

                // 根据趋势动态调整订单金额（使用配置文件参数）
                let (buy_multiplier, sell_multiplier) = match state_guard.trend_strength {
                    TrendStrength::StrongBull => {
                        (self.trend_adjustment.strong_bull_buy_multiplier, 1.0)
                    } // 强上涨：买单倍数
                    TrendStrength::Bull => (self.trend_adjustment.bull_buy_multiplier, 1.0), // 弱上涨：买单倍数
                    TrendStrength::Neutral => (1.0, 1.0), // 中性：不调整
                    TrendStrength::Bear => (1.0, self.trend_adjustment.bear_sell_multiplier), // 弱下跌：卖单倍数
                    TrendStrength::StrongBear => {
                        (1.0, self.trend_adjustment.strong_bear_sell_multiplier)
                    } // 强下跌：卖单倍数
                };

                // 计算订单金额，确保不低于最小订单金额
                let min_order_amount = 5.0; // 最小订单金额 5 USDT
                let buy_order_amount = (order_amount * buy_multiplier).max(min_order_amount);
                let sell_order_amount = (order_amount * sell_multiplier).max(min_order_amount);

                let new_orders = vec![
                    OrderRequest::new(
                        self.config.symbol.clone(),
                        OrderSide::Buy,
                        OrderType::Limit,
                        TrendGridStrategyV2::round_amount(
                            buy_order_amount / buy_price,
                            state_guard.amount_precision,
                        ),
                        Some(buy_price),
                        MarketType::Futures,
                    ),
                    OrderRequest::new(
                        self.config.symbol.clone(),
                        OrderSide::Sell,
                        OrderType::Limit,
                        TrendGridStrategyV2::round_amount(
                            sell_order_amount / sell_price,
                            state_guard.amount_precision,
                        ),
                        Some(sell_price),
                        MarketType::Futures,
                    ),
                ];

                new_orders
            }
        };

        drop(state_guard); // 释放锁

        // 精简的成交日志：交易对 买/卖 成交价 -> 新买单价 新卖单价
        let (new_buy_price, new_sell_price) = if new_orders[0].side == OrderSide::Buy {
            (
                new_orders[0].price.unwrap_or(0.0),
                new_orders[1].price.unwrap_or(0.0),
            )
        } else {
            (
                new_orders[1].price.unwrap_or(0.0),
                new_orders[0].price.unwrap_or(0.0),
            )
        };

        let symbol_short = self.config.symbol.replace("/", "");
        let side_str = if trade.side == OrderSide::Buy {
            "买"
        } else {
            "卖"
        };

        log::info!(
            "{} {} {:.4} -> 买:{:.4} 卖:{:.4}",
            symbol_short,
            side_str,
            trade.price,
            new_buy_price,
            new_sell_price
        );

        // 显示当前网格状态
        log::debug!(
            "📊 {} 当前网格: 买单{}个 [最高{:.4} - 最低{:.4}], 卖单{}个 [最低{:.4} - 最高{:.4}]",
            self.config_id,
            buy_orders.len(),
            buy_orders.first().copied().unwrap_or(0.0),
            buy_orders.last().copied().unwrap_or(0.0),
            sell_orders.len(),
            sell_orders.first().copied().unwrap_or(0.0),
            sell_orders.last().copied().unwrap_or(0.0)
        );

        // 并发执行订单提交和取消
        // 先提交新订单
        let submit_result = self
            .account_manager
            .create_batch_orders(&self.config.account.id, new_orders)
            .await;

        match submit_result {
            Ok(response) => {
                if response.failed_orders.len() > 0 {
                    log::warn!(
                        "⚠️ {} 网格调整部分成功: {} 成功, {} 失败",
                        self.config_id,
                        response.successful_orders.len(),
                        response.failed_orders.len()
                    );
                }

                // 提交成功后，更新本地缓存
                if response.successful_orders.len() > 0 {
                    // 将新订单添加到本地缓存
                    {
                        let mut state_guard = self.state.lock().await;
                        for order in &response.successful_orders {
                            state_guard
                                .active_orders
                                .insert(order.id.clone(), order.clone());
                            state_guard
                                .grid_orders
                                .insert(order.id.clone(), order.clone());
                            log::debug!(
                                "📝 {} 添加新订单到缓存: {} {:?}@{:.4}",
                                self.config_id,
                                order.id,
                                order.side,
                                order.price.unwrap_or(0.0)
                            );
                        }
                    }

                    // 等待一小段时间确保订单已经生效
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                    // 重新获取最新的挂单列表
                    if let Ok(updated_orders) = account
                        .exchange
                        .get_open_orders(Some(&self.config.symbol), MarketType::Futures)
                        .await
                    {
                        // 滚动网格取消逻辑：
                        // 买单成交：新增了近端卖单+远端买单，需要取消最远的卖单以保持平衡
                        // 卖单成交：新增了近端买单+远端卖单，需要取消最远的买单以保持平衡
                        let order_to_cancel = match trade.side {
                            OrderSide::Buy => {
                                // 成交买单后，取消最高价（最远）的卖单
                                updated_orders
                                    .iter()
                                    .filter(|o| {
                                        o.side == OrderSide::Sell && o.status == OrderStatus::Open
                                    })
                                    .max_by(|a, b| {
                                        a.price
                                            .partial_cmp(&b.price)
                                            .unwrap_or(std::cmp::Ordering::Equal)
                                    })
                            }
                            OrderSide::Sell => {
                                // 成交卖单后，取消最低价（最远）的买单
                                updated_orders
                                    .iter()
                                    .filter(|o| {
                                        o.side == OrderSide::Buy && o.status == OrderStatus::Open
                                    })
                                    .min_by(|a, b| {
                                        a.price
                                            .partial_cmp(&b.price)
                                            .unwrap_or(std::cmp::Ordering::Equal)
                                    })
                            }
                        };

                        // 执行取消
                        if let Some(order) = order_to_cancel {
                            log::info!(
                                "📌 {} 取消边缘订单: {:?}@{:.4}",
                                self.config_id,
                                order.side,
                                order.price.unwrap_or(0.0)
                            );

                            // 取消订单，忽略"Unknown order"错误（订单可能已成交或已取消）
                            match account
                                .exchange
                                .cancel_order(&order.id, &self.config.symbol, MarketType::Futures)
                                .await
                            {
                                Ok(_) => {
                                    log::debug!(
                                        "✅ {} 成功取消边缘订单 {}",
                                        self.config_id,
                                        order.id
                                    );
                                    // 从本地缓存中移除已取消的订单
                                    {
                                        let mut state_guard = self.state.lock().await;
                                        state_guard.active_orders.remove(&order.id);
                                        state_guard.grid_orders.remove(&order.id);
                                        log::debug!(
                                            "🗑️ {} 从缓存中移除已取消订单: {}",
                                            self.config_id,
                                            order.id
                                        );
                                    }
                                }
                                Err(e) => {
                                    // 检查是否是"Unknown order"错误
                                    let error_str = e.to_string();
                                    if error_str.contains("Unknown order")
                                        || error_str.contains("-2011")
                                    {
                                        // 订单不存在，可能已成交或已取消，这是正常情况
                                        log::debug!(
                                            "⚠️ {} 边缘订单 {} 已不存在（可能已成交）",
                                            self.config_id,
                                            order.id
                                        );
                                        // 也从缓存中移除
                                        {
                                            let mut state_guard = self.state.lock().await;
                                            state_guard.active_orders.remove(&order.id);
                                            state_guard.grid_orders.remove(&order.id);
                                        }
                                    } else {
                                        // 其他错误才记录为错误
                                        log::error!("❌ {} 取消订单失败: {}", self.config_id, e);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                log::error!("❌ {} 提交调整订单失败: {}", self.config_id, e);
            }
        }

        Ok(())
    }
}

type Result<T> = std::result::Result<T, ExchangeError>;
