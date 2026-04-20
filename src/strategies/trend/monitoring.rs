//! 策略监控模块

use chrono::{DateTime, Utc};
use log::{error, info, warn};
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use crate::core::error::ExchangeError;

/// 性能指标
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate: f64,
    pub total_pnl: f64,
    pub avg_win: f64,
    pub avg_loss: f64,
    pub largest_win: f64,
    pub largest_loss: f64,
    pub profit_factor: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub current_drawdown: f64,
    pub recovery_factor: f64,
    pub avg_holding_time: f64,
    pub last_update: DateTime<Utc>,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self {
            total_trades: 0,
            winning_trades: 0,
            losing_trades: 0,
            win_rate: 0.0,
            total_pnl: 0.0,
            avg_win: 0.0,
            avg_loss: 0.0,
            largest_win: 0.0,
            largest_loss: 0.0,
            profit_factor: 0.0,
            sharpe_ratio: 0.0,
            max_drawdown: 0.0,
            current_drawdown: 0.0,
            recovery_factor: 0.0,
            avg_holding_time: 0.0,
            last_update: Utc::now(),
        }
    }
}

/// 趋势监控器
pub struct TrendMonitor {
    metrics: Arc<RwLock<PerformanceMetrics>>,
    trade_history: Arc<RwLock<Vec<TradeRecord>>>,
    is_running: Arc<RwLock<bool>>,
    high_water_mark: Arc<RwLock<f64>>,
}

/// 交易记录
#[derive(Debug, Clone)]
pub struct TradeRecord {
    pub symbol: String,
    pub side: String,
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: f64,
    pub pnl: f64,
    pub entry_time: DateTime<Utc>,
    pub exit_time: DateTime<Utc>,
    pub holding_time: i64, // 秒
    pub exit_reason: String,
}

impl TrendMonitor {
    /// 创建新的监控器
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(PerformanceMetrics::default())),
            trade_history: Arc::new(RwLock::new(Vec::new())),
            is_running: Arc::new(RwLock::new(false)),
            high_water_mark: Arc::new(RwLock::new(0.0)),
        }
    }

    /// 启动监控
    pub async fn start(&self) {
        info!("启动策略监控");
        *self.is_running.write().await = true;

        // 启动监控循环
        let metrics = self.metrics.clone();
        let is_running = self.is_running.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

            while *is_running.read().await {
                interval.tick().await;

                // 输出性能报告
                let m = metrics.read().await;
                info!("===== 策略性能报告 =====");
                info!(
                    "总交易: {}, 胜率: {:.2}%",
                    m.total_trades,
                    m.win_rate * 100.0
                );
                info!(
                    "总盈亏: ${:.2}, 盈亏因子: {:.2}",
                    m.total_pnl, m.profit_factor
                );
                info!(
                    "最大回撤: {:.2}%, 当前回撤: {:.2}%",
                    m.max_drawdown * 100.0,
                    m.current_drawdown * 100.0
                );
                info!("夏普比率: {:.2}", m.sharpe_ratio);
                info!("=======================");
            }
        });
    }

    /// 停止监控
    pub async fn stop(&self) {
        info!("停止策略监控");
        *self.is_running.write().await = false;
    }

    /// 记录交易
    pub async fn record_trade(&self, record: TradeRecord) {
        let mut history = self.trade_history.write().await;
        history.push(record.clone());

        // 只保留最近1000条
        if history.len() > 1000 {
            let drain_count = history.len() - 1000;
            history.drain(0..drain_count);
        }

        // 更新指标
        self.update_metrics_from_trade(&record).await;

        if let Err(err) = self.write_trade_log(&record).await {
            warn!("写入交易日志失败: {}", err);
        }
    }

    /// 从交易更新指标
    async fn update_metrics_from_trade(&self, trade: &TradeRecord) {
        let mut metrics = self.metrics.write().await;

        metrics.total_trades += 1;
        metrics.total_pnl += trade.pnl;

        if trade.pnl > 0.0 {
            metrics.winning_trades += 1;
            metrics.avg_win = (metrics.avg_win * (metrics.winning_trades - 1) as f64 + trade.pnl)
                / metrics.winning_trades as f64;

            if trade.pnl > metrics.largest_win {
                metrics.largest_win = trade.pnl;
            }
        } else {
            metrics.losing_trades += 1;
            metrics.avg_loss = (metrics.avg_loss * (metrics.losing_trades - 1) as f64
                + trade.pnl.abs())
                / metrics.losing_trades as f64;

            if trade.pnl < metrics.largest_loss {
                metrics.largest_loss = trade.pnl;
            }
        }

        // 更新胜率
        if metrics.total_trades > 0 {
            metrics.win_rate = metrics.winning_trades as f64 / metrics.total_trades as f64;
        }

        // 更新盈亏因子
        if metrics.avg_loss > 0.0 {
            metrics.profit_factor = metrics.avg_win / metrics.avg_loss;
        }

        // 更新平均持仓时间
        metrics.avg_holding_time = (metrics.avg_holding_time * (metrics.total_trades - 1) as f64
            + trade.holding_time as f64)
            / metrics.total_trades as f64;

        // 更新回撤
        self.update_drawdown(metrics.total_pnl).await;

        metrics.last_update = Utc::now();
    }

    /// 更新回撤
    async fn update_drawdown(&self, current_equity: f64) {
        let mut hwm = self.high_water_mark.write().await;
        let mut metrics = self.metrics.write().await;

        if current_equity > *hwm {
            *hwm = current_equity;
        }

        if *hwm > 0.0 {
            metrics.current_drawdown = (*hwm - current_equity) / *hwm;

            if metrics.current_drawdown > metrics.max_drawdown {
                metrics.max_drawdown = metrics.current_drawdown;
            }
        }
    }

    async fn write_trade_log(&self, trade: &TradeRecord) -> Result<(), std::io::Error> {
        let dir = "logs/strategies";
        tokio::fs::create_dir_all(dir).await.ok();
        let date = trade.exit_time.format("%Y%m%d");
        let path = format!("{}/trend_trades_{}.log", dir, date);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        let line = format!(
            "{timestamp},{symbol},{side},{entry:.6},{exit:.6},{size:.4},{pnl:.4},{reason}\n",
            timestamp = trade.exit_time.format("%Y-%m-%d %H:%M:%S"),
            symbol = trade.symbol,
            side = trade.side,
            entry = trade.entry_price,
            exit = trade.exit_price,
            size = trade.size,
            pnl = trade.pnl,
            reason = trade.exit_reason
        );

        file.write_all(line.as_bytes()).await?;
        file.flush().await?;
        Ok(())
    }

    /// 更新指标
    pub async fn update_metrics(&self) {
        // 定期更新计算密集型指标
        let history = self.trade_history.read().await;
        if history.is_empty() {
            return;
        }

        // 计算夏普比率
        let returns: Vec<f64> = history.iter().map(|t| t.pnl).collect();
        let sharpe = self.calculate_sharpe_ratio(&returns);

        let mut metrics = self.metrics.write().await;
        metrics.sharpe_ratio = sharpe;

        // 计算恢复因子
        if metrics.max_drawdown > 0.0 {
            metrics.recovery_factor = metrics.total_pnl / metrics.max_drawdown;
        }
    }

    /// 计算夏普比率
    fn calculate_sharpe_ratio(&self, returns: &[f64]) -> f64 {
        if returns.len() < 2 {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance =
            returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / (returns.len() - 1) as f64;

        let std_dev = variance.sqrt();

        if std_dev > 0.0 {
            let risk_free_rate = 0.0; // 假设无风险利率为0
            (mean - risk_free_rate) / std_dev * (252.0_f64).sqrt() // 年化
        } else {
            0.0
        }
    }

    /// 获取性能报告
    pub async fn get_performance_report(&self) -> PerformanceReport {
        let metrics = self.metrics.read().await.clone();
        let recent_trades = self.get_recent_trades(10).await;

        PerformanceReport {
            metrics,
            recent_trades,
            report_time: Utc::now(),
        }
    }

    /// 获取最近的交易
    async fn get_recent_trades(&self, count: usize) -> Vec<TradeRecord> {
        let history = self.trade_history.read().await;
        history.iter().rev().take(count).cloned().collect()
    }

    /// 发送警报
    pub async fn send_alert(&self, level: AlertLevel, message: &str) {
        match level {
            AlertLevel::Info => info!("📊 {}", message),
            AlertLevel::Warning => warn!("⚠️ {}", message),
            AlertLevel::Critical => error!("🚨 {}", message),
        }

        // TODO: 集成webhook通知
    }
}

/// 警报级别
#[derive(Debug, Clone)]
pub enum AlertLevel {
    Info,
    Warning,
    Critical,
}

/// 性能报告
#[derive(Debug, Clone)]
pub struct PerformanceReport {
    pub metrics: PerformanceMetrics,
    pub recent_trades: Vec<TradeRecord>,
    pub report_time: DateTime<Utc>,
}
