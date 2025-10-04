use chrono::{DateTime, Utc};

/// 策略持仓与风险暴露快照
#[derive(Debug, Clone, Default)]
pub struct StrategyExposureSnapshot {
    /// 仓位名义价值（USDC）
    pub notional: f64,
    /// 净库存（基础资产数量）
    pub net_inventory: f64,
    /// 多头仓位数量（用于双向模式）
    pub long_position: f64,
    /// 空头仓位数量（用于双向模式）
    pub short_position: f64,
    /// 库存占限额比例
    pub inventory_ratio: Option<f64>,
}

/// 收益表现快照
#[derive(Debug, Clone)]
pub struct StrategyPerformanceSnapshot {
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub drawdown_pct: Option<f64>,
    pub daily_pnl: Option<f64>,
    pub consecutive_losses: Option<u32>,
    pub timestamp: DateTime<Utc>,
}

impl Default for StrategyPerformanceSnapshot {
    fn default() -> Self {
        Self {
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            drawdown_pct: None,
            daily_pnl: None,
            consecutive_losses: None,
            timestamp: Utc::now(),
        }
    }
}

/// 风控阈值
#[derive(Debug, Clone)]
pub struct StrategyRiskLimits {
    pub warning_scale_factor: Option<f64>,
    pub danger_scale_factor: Option<f64>,
    pub stop_loss_pct: Option<f64>,
    pub max_inventory_notional: Option<f64>,
    pub max_daily_loss: Option<f64>,
    pub max_consecutive_losses: Option<u32>,
    pub inventory_skew_limit: Option<f64>,
    pub max_unrealized_loss: Option<f64>,
}

/// 综合快照
#[derive(Debug, Clone)]
pub struct StrategySnapshot {
    pub name: String,
    pub exposure: StrategyExposureSnapshot,
    pub performance: StrategyPerformanceSnapshot,
    pub risk_limits: Option<StrategyRiskLimits>,
}

impl StrategySnapshot {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            exposure: StrategyExposureSnapshot::default(),
            performance: StrategyPerformanceSnapshot::default(),
            risk_limits: None,
        }
    }
}
