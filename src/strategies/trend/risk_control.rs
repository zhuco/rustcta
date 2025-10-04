//! 四层风控系统
//!
//! Layer 1: 系统级风控（最高优先级）
//! Layer 2: 账户级风控
//! Layer 3: 策略级风控  
//! Layer 4: 订单级风控

use chrono::{DateTime, Duration, Timelike, Utc};
use log::{error, info, warn};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::error::ExchangeError;
use crate::strategies::trend::config::RiskConfig;
use crate::strategies::trend::signal_generator::TradeSignal;

/// 风险级别
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RiskLevel {
    /// 正常
    Normal = 0,
    /// 警告
    Warning = 1,
    /// 危险
    Danger = 2,
    /// 紧急
    Emergency = 3,
}

/// 四层风控控制器
pub struct RiskController {
    config: RiskConfig,

    // 系统级状态
    system_health: Arc<RwLock<SystemHealth>>,

    // 账户级状态
    account_metrics: Arc<RwLock<AccountMetrics>>,

    // 策略级状态
    strategy_metrics: Arc<RwLock<StrategyMetrics>>,

    // 订单级状态
    order_metrics: Arc<RwLock<OrderMetrics>>,

    // 风险事件记录
    risk_events: Arc<RwLock<Vec<RiskEvent>>>,
}

/// 系统健康状态
#[derive(Debug, Clone)]
struct SystemHealth {
    last_check: DateTime<Utc>,
    api_latency: f64,
    network_status: bool,
    exchange_status: bool,
    data_feed_status: bool,
    error_count: usize,
}

/// 账户指标
#[derive(Debug, Clone)]
struct AccountMetrics {
    total_equity: f64,
    available_balance: f64,
    used_margin: f64,
    daily_pnl: f64,
    weekly_pnl: f64,
    monthly_pnl: f64,
    max_drawdown: f64,
    current_drawdown: f64,
    total_exposure: f64,
    leverage_used: f64,
}

/// 策略指标
#[derive(Debug, Clone)]
struct StrategyMetrics {
    open_positions: usize,
    daily_trades: usize,
    consecutive_losses: usize,
    consecutive_wins: usize,
    win_rate: f64,
    profit_factor: f64,
    avg_win: f64,
    avg_loss: f64,
    largest_win: f64,
    largest_loss: f64,
}

/// 订单指标
#[derive(Debug, Clone)]
struct OrderMetrics {
    pending_orders: usize,
    order_success_rate: f64,
    avg_slippage: f64,
    max_slippage: f64,
    avg_execution_time: f64,
    failed_orders: usize,
}

/// 风险事件
#[derive(Debug, Clone)]
struct RiskEvent {
    timestamp: DateTime<Utc>,
    level: RiskLevel,
    layer: String,
    message: String,
    action_taken: String,
}

impl RiskController {
    /// 创建新的风控控制器
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            system_health: Arc::new(RwLock::new(SystemHealth {
                last_check: Utc::now(),
                api_latency: 0.0,
                network_status: true,
                exchange_status: true,
                data_feed_status: true,
                error_count: 0,
            })),
            account_metrics: Arc::new(RwLock::new(AccountMetrics {
                total_equity: 0.0,
                available_balance: 0.0,
                used_margin: 0.0,
                daily_pnl: 0.0,
                weekly_pnl: 0.0,
                monthly_pnl: 0.0,
                max_drawdown: 0.0,
                current_drawdown: 0.0,
                total_exposure: 0.0,
                leverage_used: 0.0,
            })),
            strategy_metrics: Arc::new(RwLock::new(StrategyMetrics {
                open_positions: 0,
                daily_trades: 0,
                consecutive_losses: 0,
                consecutive_wins: 0,
                win_rate: 0.0,
                profit_factor: 0.0,
                avg_win: 0.0,
                avg_loss: 0.0,
                largest_win: 0.0,
                largest_loss: 0.0,
            })),
            order_metrics: Arc::new(RwLock::new(OrderMetrics {
                pending_orders: 0,
                order_success_rate: 1.0,
                avg_slippage: 0.0,
                max_slippage: 0.0,
                avg_execution_time: 0.0,
                failed_orders: 0,
            })),
            risk_events: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 检查所有四层风控
    pub async fn check_all_layers(&self) -> Result<RiskLevel, ExchangeError> {
        let mut max_level = RiskLevel::Normal;

        // Layer 1: 系统级风控
        let system_level = self.check_system_layer().await?;
        if system_level as u8 > max_level as u8 {
            max_level = system_level;
        }

        // 如果系统级出现紧急情况，直接返回
        if max_level == RiskLevel::Emergency {
            return Ok(max_level);
        }

        // Layer 2: 账户级风控
        let account_level = self.check_account_layer().await?;
        if account_level as u8 > max_level as u8 {
            max_level = account_level;
        }

        // Layer 3: 策略级风控
        let strategy_level = self.check_strategy_layer().await?;
        if strategy_level as u8 > max_level as u8 {
            max_level = strategy_level;
        }

        // Layer 4: 订单级风控
        let order_level = self.check_order_layer().await?;
        if order_level as u8 > max_level as u8 {
            max_level = order_level;
        }

        Ok(max_level)
    }

    /// Layer 1: 系统级风控检查
    async fn check_system_layer(&self) -> Result<RiskLevel, ExchangeError> {
        let health = self.system_health.read().await;

        // 检查网络状态
        if !health.network_status {
            self.log_risk_event(
                RiskLevel::Emergency,
                "System",
                "网络连接断开".to_string(),
                "停止所有交易",
            )
            .await;
            return Ok(RiskLevel::Emergency);
        }

        // 检查交易所状态
        if !health.exchange_status {
            self.log_risk_event(
                RiskLevel::Emergency,
                "System",
                "交易所连接异常".to_string(),
                "停止所有交易",
            )
            .await;
            return Ok(RiskLevel::Emergency);
        }

        // 检查API延迟
        if health.api_latency > 1000.0 {
            // 1秒
            self.log_risk_event(
                RiskLevel::Danger,
                "System",
                format!("API延迟过高: {}ms", health.api_latency),
                "停止新开仓",
            )
            .await;
            return Ok(RiskLevel::Danger);
        }

        // 检查错误率
        if health.error_count > 10 {
            self.log_risk_event(
                RiskLevel::Warning,
                "System",
                format!("错误次数过多: {}", health.error_count),
                "减少交易频率",
            )
            .await;
            return Ok(RiskLevel::Warning);
        }

        Ok(RiskLevel::Normal)
    }

    /// Layer 2: 账户级风控检查
    async fn check_account_layer(&self) -> Result<RiskLevel, ExchangeError> {
        let metrics = self.account_metrics.read().await;

        // 检查回撤
        if metrics.current_drawdown > self.config.emergency_stop_loss {
            self.log_risk_event(
                RiskLevel::Emergency,
                "Account",
                format!("回撤超过紧急阈值: {:.2}%", metrics.current_drawdown * 100.0),
                "紧急停止所有策略",
            )
            .await;
            return Ok(RiskLevel::Emergency);
        }

        if metrics.current_drawdown > self.config.max_drawdown {
            self.log_risk_event(
                RiskLevel::Danger,
                "Account",
                format!("回撤超过最大限制: {:.2}%", metrics.current_drawdown * 100.0),
                "停止新开仓",
            )
            .await;
            return Ok(RiskLevel::Danger);
        }

        // 检查日亏损
        let daily_loss_ratio = -metrics.daily_pnl / metrics.total_equity;
        if daily_loss_ratio > self.config.max_daily_loss {
            self.log_risk_event(
                RiskLevel::Danger,
                "Account",
                format!("日亏损超限: {:.2}%", daily_loss_ratio * 100.0),
                "今日停止交易",
            )
            .await;
            return Ok(RiskLevel::Danger);
        }

        // 检查杠杆
        if metrics.leverage_used > self.config.max_leverage {
            self.log_risk_event(
                RiskLevel::Warning,
                "Account",
                format!("杠杆过高: {:.2}x", metrics.leverage_used),
                "降低仓位",
            )
            .await;
            return Ok(RiskLevel::Warning);
        }

        // 检查总暴露
        if metrics.total_exposure > self.config.max_total_exposure {
            self.log_risk_event(
                RiskLevel::Warning,
                "Account",
                format!("总仓位过高: {:.2}%", metrics.total_exposure * 100.0),
                "停止新开仓",
            )
            .await;
            return Ok(RiskLevel::Warning);
        }

        Ok(RiskLevel::Normal)
    }

    /// Layer 3: 策略级风控检查
    async fn check_strategy_layer(&self) -> Result<RiskLevel, ExchangeError> {
        let metrics = self.strategy_metrics.read().await;

        // 检查连续亏损
        if metrics.consecutive_losses >= self.config.max_consecutive_losses {
            self.log_risk_event(
                RiskLevel::Danger,
                "Strategy",
                format!("连续亏损{}次", metrics.consecutive_losses),
                "暂停策略2小时",
            )
            .await;
            return Ok(RiskLevel::Danger);
        }

        // 检查胜率
        if metrics.win_rate < 0.3 && metrics.daily_trades > 5 {
            self.log_risk_event(
                RiskLevel::Warning,
                "Strategy",
                format!("胜率过低: {:.2}%", metrics.win_rate * 100.0),
                "检查策略参数",
            )
            .await;
            return Ok(RiskLevel::Warning);
        }

        // 检查最大单笔亏损
        if metrics.largest_loss > self.config.max_risk_per_trade * 2.0 {
            self.log_risk_event(
                RiskLevel::Warning,
                "Strategy",
                format!("单笔亏损异常: ${:.2}", metrics.largest_loss),
                "检查止损执行",
            )
            .await;
            return Ok(RiskLevel::Warning);
        }

        Ok(RiskLevel::Normal)
    }

    /// Layer 4: 订单级风控检查
    async fn check_order_layer(&self) -> Result<RiskLevel, ExchangeError> {
        let metrics = self.order_metrics.read().await;

        // 检查滑点
        if metrics.max_slippage > self.config.max_slippage {
            self.log_risk_event(
                RiskLevel::Warning,
                "Order",
                format!("滑点超限: {:.2}%", metrics.max_slippage * 100.0),
                "使用限价单",
            )
            .await;
            return Ok(RiskLevel::Warning);
        }

        // 检查订单成功率
        if metrics.order_success_rate < 0.9 {
            self.log_risk_event(
                RiskLevel::Warning,
                "Order",
                format!("订单成功率低: {:.2}%", metrics.order_success_rate * 100.0),
                "检查订单参数",
            )
            .await;
            return Ok(RiskLevel::Warning);
        }

        // 检查执行时间
        if metrics.avg_execution_time > 1000.0 {
            // 1秒
            self.log_risk_event(
                RiskLevel::Warning,
                "Order",
                format!("执行延迟高: {:.0}ms", metrics.avg_execution_time),
                "优化执行逻辑",
            )
            .await;
            return Ok(RiskLevel::Warning);
        }

        Ok(RiskLevel::Normal)
    }

    /// 批准交易
    pub async fn approve_trade(
        &self,
        signal: &TradeSignal,
        position_size: f64,
    ) -> Result<bool, ExchangeError> {
        // 检查风险级别
        let risk_level = self.check_all_layers().await?;

        match risk_level {
            RiskLevel::Emergency | RiskLevel::Danger => {
                warn!("风险级别过高，拒绝交易");
                return Ok(false);
            }
            RiskLevel::Warning => {
                // 警告级别，减少仓位
                if position_size > signal.suggested_size * 0.5 {
                    warn!("警告级别，建议减少仓位");
                }
            }
            RiskLevel::Normal => {
                // 正常交易
            }
        }

        // 检查单笔风险
        let risk_amount = position_size * (signal.entry_price - signal.stop_loss).abs();
        let account_metrics = self.account_metrics.read().await;
        let risk_ratio = risk_amount / account_metrics.total_equity;

        if risk_ratio > self.config.max_risk_per_trade {
            warn!("单笔风险过高: {:.2}%", risk_ratio * 100.0);
            return Ok(false);
        }

        // 检查总暴露
        let new_exposure =
            account_metrics.total_exposure + position_size / account_metrics.total_equity;
        if new_exposure > self.config.max_total_exposure {
            warn!("总暴露将超限: {:.2}%", new_exposure * 100.0);
            return Ok(false);
        }

        Ok(true)
    }

    /// 更新账户指标
    pub async fn update_account_metrics(
        &self,
        total_equity: f64,
        available_balance: f64,
        used_margin: f64,
        daily_pnl: f64,
    ) {
        let mut metrics = self.account_metrics.write().await;
        metrics.total_equity = total_equity;
        metrics.available_balance = available_balance;
        metrics.used_margin = used_margin;
        metrics.daily_pnl = daily_pnl;

        // 计算杠杆
        if total_equity > 0.0 {
            metrics.leverage_used = used_margin / total_equity;
        }

        // 更新回撤
        // TODO: 实现高水位标记和回撤计算
    }

    /// 更新策略指标
    pub async fn update_strategy_metrics(
        &self,
        open_positions: usize,
        daily_trades: usize,
        consecutive_losses: usize,
    ) {
        let mut metrics = self.strategy_metrics.write().await;
        metrics.open_positions = open_positions;
        metrics.daily_trades = daily_trades;
        metrics.consecutive_losses = consecutive_losses;
    }

    /// 更新订单指标
    pub async fn update_order_metrics(&self, slippage: f64, execution_time: f64, success: bool) {
        let mut metrics = self.order_metrics.write().await;

        // 更新滑点
        metrics.avg_slippage = (metrics.avg_slippage * 0.9) + (slippage * 0.1);
        if slippage > metrics.max_slippage {
            metrics.max_slippage = slippage;
        }

        // 更新执行时间
        metrics.avg_execution_time = (metrics.avg_execution_time * 0.9) + (execution_time * 0.1);

        // 更新成功率
        if !success {
            metrics.failed_orders += 1;
        }
        // TODO: 计算成功率
    }

    /// 记录风险事件
    async fn log_risk_event(&self, level: RiskLevel, layer: &str, message: String, action: &str) {
        let event = RiskEvent {
            timestamp: Utc::now(),
            level: level.clone(),
            layer: layer.to_string(),
            message: message.clone(),
            action_taken: action.to_string(),
        };

        let mut events = self.risk_events.write().await;
        events.push(event);

        // 只保留最近1000条
        if events.len() > 1000 {
            let drain_count = events.len() - 1000;
            events.drain(0..drain_count);
        }

        // 记录日志
        match level {
            RiskLevel::Emergency => error!("[{}] {}: {}", layer, message, action),
            RiskLevel::Danger => error!("[{}] {}: {}", layer, message, action),
            RiskLevel::Warning => warn!("[{}] {}: {}", layer, message, action),
            RiskLevel::Normal => info!("[{}] {}: {}", layer, message, action),
        }
    }

    /// 自检
    pub async fn self_check(&self) -> Result<(), ExchangeError> {
        info!("执行风控系统自检...");

        // 检查配置合理性
        if self.config.max_risk_per_trade > 0.02 {
            return Err(ExchangeError::Other(
                "风控配置错误：单笔风险过高".to_string(),
            ));
        }

        // 初始化健康状态
        let mut health = self.system_health.write().await;
        health.last_check = Utc::now();
        health.network_status = true;
        health.exchange_status = true;
        health.data_feed_status = true;

        info!("风控系统自检通过");
        Ok(())
    }

    /// 获取风险报告
    pub async fn get_risk_report(&self) -> RiskReport {
        let system = self.system_health.read().await.clone();
        let account = self.account_metrics.read().await.clone();
        let strategy = self.strategy_metrics.read().await.clone();
        let order = self.order_metrics.read().await.clone();
        let events = self.risk_events.read().await.clone();

        RiskReport {
            timestamp: Utc::now(),
            system_health: system,
            account_metrics: account,
            strategy_metrics: strategy,
            order_metrics: order,
            recent_events: events.iter().rev().take(10).cloned().collect(),
        }
    }
}

/// 风险报告
#[derive(Debug, Clone)]
pub struct RiskReport {
    pub timestamp: DateTime<Utc>,
    pub system_health: SystemHealth,
    pub account_metrics: AccountMetrics,
    pub strategy_metrics: StrategyMetrics,
    pub order_metrics: OrderMetrics,
    pub recent_events: Vec<RiskEvent>,
}
