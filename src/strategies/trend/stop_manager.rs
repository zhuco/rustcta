//! 止损止盈管理模块

use chrono::{DateTime, Duration, Utc};
use log::{debug, info, warn};

use crate::core::error::ExchangeError;
use crate::core::types::MarketType;
use crate::core::types::OrderSide;
use crate::cta::account_manager::AccountManager;
use crate::strategies::trend::config::{StopConfig, StopType};
use crate::strategies::trend::position_manager::TrendPosition;
use std::sync::Arc;

/// 止损更新类型
#[derive(Debug, Clone)]
pub enum StopUpdate {
    /// 移动止损到新位置
    MoveTo(f64),
    /// 收紧止损
    Tighten(f64),
    /// 保本止损
    Breakeven,
    /// 追踪止损
    Trailing(f64),
    /// 无需更新
    None,
}

/// 止损管理器
pub struct StopManager {
    config: StopConfig,
    atr_cache: Vec<f64>,
    account_manager: Option<Arc<AccountManager>>,
}

impl StopManager {
    /// 创建新的止损管理器
    pub fn new(config: StopConfig) -> Self {
        Self {
            config,
            atr_cache: Vec::new(),
            account_manager: None,
        }
    }

    /// 设置账户管理器
    pub fn set_account_manager(&mut self, manager: Arc<AccountManager>) {
        self.account_manager = Some(manager);
    }

    /// 计算初始止损
    pub fn calculate_initial_stop(
        &self,
        entry_price: f64,
        side: OrderSide,
        atr: Option<f64>,
    ) -> f64 {
        let stop_distance = match self.config.initial_stop_type {
            StopType::Fixed => entry_price * self.config.fixed_stop_percent,
            StopType::ATR => {
                if let Some(atr_value) = atr {
                    atr_value * self.config.atr_multiplier
                } else {
                    entry_price * self.config.fixed_stop_percent
                }
            }
            StopType::Percentage => entry_price * self.config.fixed_stop_percent,
            StopType::Structure => {
                // 实际应该基于市场结构，这里简化
                entry_price * 0.02
            }
        };

        match side {
            OrderSide::Buy => entry_price - stop_distance,
            OrderSide::Sell => entry_price + stop_distance,
        }
    }

    /// 计算止损更新
    pub async fn calculate_stop_update(
        &self,
        position: &TrendPosition,
    ) -> Result<Option<StopUpdate>, ExchangeError> {
        let current_price = self.get_current_price(&position.symbol).await?;

        // 计算当前盈利
        let profit = match position.side {
            OrderSide::Buy => current_price - position.average_price,
            OrderSide::Sell => position.average_price - current_price,
        };

        let profit_percent = profit / position.average_price;
        let profit_r = profit / (position.average_price - position.stop_loss).abs();

        // 检查时间止损
        if let Some(time_stop_hours) = self.config.time_stop_hours {
            let holding_duration = Utc::now() - position.entry_time;
            if holding_duration > Duration::hours(time_stop_hours as i64) && profit < 0.0 {
                warn!(
                    "触发时间止损: {} 持仓超过 {} 小时",
                    position.symbol, time_stop_hours
                );
                return Ok(Some(StopUpdate::MoveTo(current_price)));
            }
        }

        // 检查保本止损
        if self.config.breakeven_enabled && profit_r >= self.config.breakeven_trigger {
            if !self.is_at_breakeven(position) {
                info!("移动止损到保本: {}", position.symbol);
                return Ok(Some(StopUpdate::Breakeven));
            }
        }

        // 检查追踪止损
        if self.config.trailing_stop_enabled && profit_r >= self.config.trailing_activation {
            let new_stop = self.calculate_trailing_stop(position, current_price);
            if self.should_update_stop(position, new_stop) {
                debug!("更新追踪止损: {} -> {}", position.stop_loss, new_stop);
                return Ok(Some(StopUpdate::Trailing(new_stop)));
            }
        }

        Ok(None)
    }

    /// 计算追踪止损位置
    fn calculate_trailing_stop(&self, position: &TrendPosition, current_price: f64) -> f64 {
        let trail_distance = position.risk_amount * self.config.trailing_distance;

        match position.side {
            OrderSide::Buy => current_price - trail_distance,
            OrderSide::Sell => current_price + trail_distance,
        }
    }

    /// 检查是否应该更新止损
    fn should_update_stop(&self, position: &TrendPosition, new_stop: f64) -> bool {
        match position.side {
            OrderSide::Buy => new_stop > position.stop_loss,
            OrderSide::Sell => new_stop < position.stop_loss,
        }
    }

    /// 检查是否已在保本位置
    fn is_at_breakeven(&self, position: &TrendPosition) -> bool {
        (position.stop_loss - position.entry_price).abs() < 0.0001
    }

    /// 获取当前价格
    async fn get_current_price(&self, symbol: &str) -> Result<f64, ExchangeError> {
        if let Some(account_manager) = &self.account_manager {
            if let Some(account) = account_manager.get_account("binance_hcr") {
                let ticker = account
                    .exchange
                    .get_ticker(symbol, MarketType::Spot)
                    .await?;
                return Ok(ticker.last);
            }
        }

        // 如果无法获取真实价格，返回错误
        Err(ExchangeError::ParseError(format!(
            "无法获取 {} 的市场价格",
            symbol
        )))
    }

    /// 处理部分止盈
    pub fn check_partial_profits(
        &self,
        position: &TrendPosition,
        current_price: f64,
    ) -> Vec<PartialTakeProfit> {
        let mut take_profits = Vec::new();

        for (i, target) in self.config.partial_targets.iter().enumerate() {
            // 检查是否已执行
            if position
                .take_profits
                .get(i)
                .map(|tp| tp.executed)
                .unwrap_or(true)
            {
                continue;
            }

            // 计算当前R倍数
            let profit_r = self.calculate_profit_r(position, current_price);

            if profit_r >= target.target_r {
                take_profits.push(PartialTakeProfit {
                    index: i,
                    price: current_price,
                    size: position.current_size * target.close_ratio,
                    reason: format!("达到 {}R 目标", target.target_r),
                });
            }
        }

        take_profits
    }

    /// 计算盈利R倍数
    fn calculate_profit_r(&self, position: &TrendPosition, current_price: f64) -> f64 {
        let entry = position.average_price;
        let stop = position.stop_loss;
        let risk = (entry - stop).abs();

        if risk == 0.0 {
            return 0.0;
        }

        match position.side {
            OrderSide::Buy => (current_price - entry) / risk,
            OrderSide::Sell => (entry - current_price) / risk,
        }
    }

    /// 应急止损（用于紧急情况）
    pub fn emergency_stop(&self, position: &TrendPosition) -> f64 {
        // 立即市价止损
        warn!("触发应急止损: {}", position.symbol);
        0.0 // 表示市价
    }

    /// 获取止损报告
    pub fn get_stop_report(&self, position: &TrendPosition) -> StopReport {
        // 此处仅用于报告，使用估算价格
        let current_price = position.average_price; // 使用平均价格作为估算
        let profit_r = self.calculate_profit_r(position, current_price);
        let stop_distance = (position.stop_loss - position.average_price).abs();
        let stop_percent = stop_distance / position.average_price * 100.0;

        StopReport {
            symbol: position.symbol.clone(),
            current_stop: position.stop_loss,
            stop_type: self.get_current_stop_type(position, profit_r),
            stop_distance,
            stop_percent,
            profit_r,
            is_trailing: profit_r >= self.config.trailing_activation,
            is_breakeven: self.is_at_breakeven(position),
        }
    }

    /// 获取当前止损类型
    fn get_current_stop_type(&self, position: &TrendPosition, profit_r: f64) -> String {
        if self.is_at_breakeven(position) {
            "保本止损".to_string()
        } else if profit_r >= self.config.trailing_activation {
            "追踪止损".to_string()
        } else {
            "初始止损".to_string()
        }
    }
}

/// 部分止盈信号
#[derive(Debug, Clone)]
pub struct PartialTakeProfit {
    pub index: usize,
    pub price: f64,
    pub size: f64,
    pub reason: String,
}

/// 止损报告
#[derive(Debug, Clone)]
pub struct StopReport {
    pub symbol: String,
    pub current_stop: f64,
    pub stop_type: String,
    pub stop_distance: f64,
    pub stop_percent: f64,
    pub profit_r: f64,
    pub is_trailing: bool,
    pub is_breakeven: bool,
}

// 临时使用，实际应该使用真正的随机数生成器
mod rand {
    pub fn random<T>() -> T
    where
        T: Default,
    {
        T::default()
    }
}
