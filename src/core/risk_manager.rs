//! 全局风险管理模块
//! 统一管理所有策略的风险控制

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::error::ExchangeError;
use crate::core::types::{OrderSide, Position};

/// 风险状态
#[derive(Debug, Clone, PartialEq)]
pub enum RiskStatus {
    /// 正常
    Normal,
    /// 警告
    Warning,
    /// 危险
    Danger,
    /// 停止所有策略
    StopAll,
}

/// 风险配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RiskConfig {
    /// 最大总仓位价值(USDC)
    pub max_total_exposure: f64,
    /// 单个策略最大仓位
    pub max_single_position: f64,
    /// 日最大亏损
    pub daily_loss_limit: f64,
    /// 最大回撤百分比
    pub max_drawdown_pct: f64,
    /// 警告阈值(占限额的百分比)
    pub warning_threshold: f64,
    /// 危险阈值
    pub danger_threshold: f64,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_total_exposure: 10000.0, // 1万USDC
            max_single_position: 2000.0, // 2千USDC
            daily_loss_limit: 500.0,     // 500 USDC
            max_drawdown_pct: 0.15,      // 15%
            warning_threshold: 0.7,      // 70%触发警告
            danger_threshold: 0.9,       // 90%触发危险
        }
    }
}

/// 策略风险信息
#[derive(Debug, Clone)]
pub struct StrategyRisk {
    pub strategy_name: String,
    pub position_value: f64,
    pub unrealized_pnl: f64,
    pub realized_pnl: f64,
    pub last_update: DateTime<Utc>,
}

/// 全局风险管理器
pub struct GlobalRiskManager {
    config: RiskConfig,
    /// 各策略的风险信息
    strategies: Arc<RwLock<HashMap<String, StrategyRisk>>>,
    /// 当前日期(用于重置日盈亏)
    current_date: Arc<RwLock<DateTime<Utc>>>,
    /// 日累计盈亏
    daily_pnl: Arc<RwLock<f64>>,
    /// 最高净值(用于计算回撤)
    high_water_mark: Arc<RwLock<f64>>,
    /// 当前净值
    current_equity: Arc<RwLock<f64>>,
}

impl GlobalRiskManager {
    /// 创建新的风险管理器
    pub fn new(config: RiskConfig) -> Self {
        Self {
            config,
            strategies: Arc::new(RwLock::new(HashMap::new())),
            current_date: Arc::new(RwLock::new(Utc::now())),
            daily_pnl: Arc::new(RwLock::new(0.0)),
            high_water_mark: Arc::new(RwLock::new(10000.0)), // 初始资金
            current_equity: Arc::new(RwLock::new(10000.0)),
        }
    }

    /// 更新策略风险信息
    pub async fn update_strategy_risk(
        &self,
        strategy_name: String,
        position_value: f64,
        unrealized_pnl: f64,
        realized_pnl: f64,
    ) {
        let mut strategies = self.strategies.write().await;
        strategies.insert(
            strategy_name.clone(),
            StrategyRisk {
                strategy_name,
                position_value,
                unrealized_pnl,
                realized_pnl,
                last_update: Utc::now(),
            },
        );

        // 更新净值
        self.update_equity().await;
    }

    /// 更新净值和回撤
    async fn update_equity(&self) {
        let strategies = self.strategies.read().await;

        // 计算总盈亏
        let total_pnl: f64 = strategies
            .values()
            .map(|s| s.unrealized_pnl + s.realized_pnl)
            .sum();

        // 更新当前净值
        let mut current_equity = self.current_equity.write().await;
        *current_equity = 10000.0 + total_pnl; // 假设初始资金1万

        // 更新最高净值
        let mut hwm = self.high_water_mark.write().await;
        if *current_equity > *hwm {
            *hwm = *current_equity;
        }

        // 检查是否需要重置日盈亏
        let now = Utc::now();
        let mut current_date = self.current_date.write().await;
        if now.date_naive() != current_date.date_naive() {
            *current_date = now;
            *self.daily_pnl.write().await = 0.0;
        }

        // 更新日盈亏
        *self.daily_pnl.write().await = total_pnl;
    }

    /// 检查风险状态
    pub async fn check_risk(&self) -> RiskStatus {
        // 1. 检查日亏损
        let daily_pnl = *self.daily_pnl.read().await;
        if daily_pnl < -self.config.daily_loss_limit {
            log::error!("❌ 触发日亏损限制: {:.2} USDC", daily_pnl);
            return RiskStatus::StopAll;
        }

        // 2. 检查最大回撤
        let current_equity = *self.current_equity.read().await;
        let hwm = *self.high_water_mark.read().await;
        let drawdown = (hwm - current_equity) / hwm;
        if drawdown > self.config.max_drawdown_pct {
            log::error!("❌ 触发最大回撤限制: {:.1}%", drawdown * 100.0);
            return RiskStatus::StopAll;
        }

        // 3. 检查总仓位暴露
        let strategies = self.strategies.read().await;
        let total_exposure: f64 = strategies.values().map(|s| s.position_value.abs()).sum();

        if total_exposure > self.config.max_total_exposure {
            log::error!("❌ 总仓位超限: {:.2} USDC", total_exposure);
            return RiskStatus::Danger;
        }

        // 4. 检查单个策略仓位
        for strategy in strategies.values() {
            if strategy.position_value.abs() > self.config.max_single_position {
                log::warn!(
                    "⚠️ 策略 {} 仓位过大: {:.2} USDC",
                    strategy.strategy_name,
                    strategy.position_value
                );
                return RiskStatus::Warning;
            }
        }

        // 5. 检查警告阈值
        let exposure_ratio = total_exposure / self.config.max_total_exposure;
        if exposure_ratio > self.config.danger_threshold {
            return RiskStatus::Danger;
        } else if exposure_ratio > self.config.warning_threshold {
            return RiskStatus::Warning;
        }

        RiskStatus::Normal
    }

    /// 获取风险报告
    pub async fn get_risk_report(&self) -> String {
        let strategies = self.strategies.read().await;
        let daily_pnl = *self.daily_pnl.read().await;
        let current_equity = *self.current_equity.read().await;
        let hwm = *self.high_water_mark.read().await;
        let drawdown = (hwm - current_equity) / hwm * 100.0;

        let total_exposure: f64 = strategies.values().map(|s| s.position_value.abs()).sum();

        format!(
            "📊 风险报告\n\
            ├─ 净值: ${:.2}\n\
            ├─ 日盈亏: ${:.2}\n\
            ├─ 回撤: {:.1}%\n\
            ├─ 总仓位: ${:.2}/{:.2}\n\
            └─ 策略数: {}",
            current_equity,
            daily_pnl,
            drawdown,
            total_exposure,
            self.config.max_total_exposure,
            strategies.len()
        )
    }

    /// 紧急停止所有策略
    pub async fn emergency_stop(&self) {
        log::error!("🚨 触发紧急停止，关闭所有策略！");
        // 这里应该通知所有策略停止
        // 实际实现需要策略注册机制
    }
}

/// 单例模式的全局风险管理器
lazy_static::lazy_static! {
    pub static ref GLOBAL_RISK_MANAGER: Arc<GlobalRiskManager> = {
        let config = RiskConfig::default();
        Arc::new(GlobalRiskManager::new(config))
    };
}
