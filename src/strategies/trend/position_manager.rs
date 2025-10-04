//! 仓位管理模块

use chrono::{DateTime, Timelike, Utc};
use log::{debug, info, warn};
use std::collections::HashMap;

use crate::core::error::ExchangeError;
use crate::core::types::{OrderSide, Position};
use crate::strategies::trend::config::{PositionConfig, PyramidLevel};
use crate::strategies::trend::signal_generator::TradeSignal;

/// 趋势策略持仓
#[derive(Debug, Clone)]
pub struct TrendPosition {
    pub symbol: String,
    pub side: OrderSide,
    pub entry_price: f64,
    pub average_price: f64,
    pub current_price: f64,
    pub size: f64,
    pub current_size: f64,
    pub initial_size: f64,
    pub pyramid_entries: Vec<PyramidEntry>,
    pub pyramid_count: usize,
    pub stop_loss: f64,
    pub take_profits: Vec<TakeProfitLevel>,
    pub entry_time: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub peak_profit: f64,
    pub risk_amount: f64,
}

impl TrendPosition {
    /// 检查是否触及止损
    pub fn is_stop_hit(&self) -> bool {
        match self.side {
            OrderSide::Buy => self.current_price <= self.stop_loss,
            OrderSide::Sell => self.current_price >= self.stop_loss,
        }
    }

    /// 获取持仓小时数
    pub fn holding_hours(&self) -> f64 {
        let duration = Utc::now() - self.entry_time;
        duration.num_seconds() as f64 / 3600.0
    }

    /// 获取盈利的R倍数
    pub fn profit_in_r(&self) -> f64 {
        let risk = (self.entry_price - self.stop_loss).abs();
        if risk > 0.0 {
            self.unrealized_pnl / (risk * self.size)
        } else {
            0.0
        }
    }
}

/// 金字塔加仓记录
#[derive(Debug, Clone)]
pub struct PyramidEntry {
    pub level: usize,
    pub price: f64,
    pub size: f64,
    pub time: DateTime<Utc>,
}

/// 止盈级别
#[derive(Debug, Clone)]
pub struct TakeProfitLevel {
    pub price: f64,
    pub ratio: f64,
    pub executed: bool,
}

/// 金字塔信号
#[derive(Debug, Clone)]
pub struct PyramidSignal {
    pub level: usize,
    pub size: f64,
    pub price: f64,
}

/// 仓位管理器
pub struct PositionManager {
    config: PositionConfig,
    positions: HashMap<String, TrendPosition>,
    total_exposure: f64,
    correlation_matrix: HashMap<(String, String), f64>,
}

impl PositionManager {
    /// 创建新的仓位管理器
    pub fn new(config: PositionConfig) -> Self {
        Self {
            config,
            positions: HashMap::new(),
            total_exposure: 0.0,
            correlation_matrix: HashMap::new(),
        }
    }

    /// 计算仓位大小
    pub async fn calculate_position_size(
        &self,
        signal: &TradeSignal,
        account_balance: f64,
    ) -> Result<f64, ExchangeError> {
        debug!("计算 {} 的仓位大小", signal.symbol);

        // 基础风险金额
        let base_risk = account_balance * self.config.base_risk_ratio;

        // 计算止损距离
        let stop_distance = (signal.entry_price - signal.stop_loss).abs();
        let stop_percent = stop_distance / signal.entry_price;

        // 基础仓位 = 风险金额 / 止损距离
        let mut position_size = base_risk / stop_distance;

        // 应用Kelly公式（如果启用）
        if self.config.kelly_fraction > 0.0 {
            position_size *= self.calculate_kelly_factor(signal);
        }

        // 应用动态调整因子
        if self.config.trend_factor_enabled {
            position_size *= self.get_trend_factor(signal);
        }

        if self.config.volatility_factor_enabled {
            position_size *= self.get_volatility_factor(stop_percent);
        }

        if self.config.time_factor_enabled {
            position_size *= self.get_time_factor();
        }

        // 限制最大仓位
        let max_position = account_balance * 0.1; // 单个仓位不超过10%
        position_size = position_size.min(max_position);

        // 检查相关性限制
        if !self.check_correlation_limits(&signal.symbol, position_size, account_balance) {
            warn!("相关性检查失败，减少仓位");
            position_size *= 0.5;
        }

        info!(
            "计算仓位: {} USDC (风险: {:.2}%)",
            position_size,
            stop_percent * 100.0
        );

        Ok(position_size)
    }

    /// 计算Kelly因子
    fn calculate_kelly_factor(&self, signal: &TradeSignal) -> f64 {
        // Kelly公式: f = (p*b - q) / b
        // p = 胜率, b = 盈亏比, q = 1-p
        let win_rate = 0.45; // 实际应从历史统计
        let risk_reward = signal.risk_reward_ratio;

        let kelly = (win_rate * risk_reward - (1.0 - win_rate)) / risk_reward;

        // 使用Kelly的1/4以保守
        (kelly * self.config.kelly_fraction).max(0.0).min(1.0)
    }

    /// 获取趋势因子
    fn get_trend_factor(&self, signal: &TradeSignal) -> f64 {
        match signal.confidence {
            c if c > 80.0 => 1.2,
            c if c > 70.0 => 1.1,
            c if c > 60.0 => 1.0,
            _ => 0.8,
        }
    }

    /// 获取波动率因子
    fn get_volatility_factor(&self, stop_percent: f64) -> f64 {
        // 波动率越大，仓位越小
        match stop_percent {
            s if s < 0.01 => 1.2, // 低波动
            s if s < 0.02 => 1.0, // 正常
            s if s < 0.03 => 0.8, // 高波动
            _ => 0.6,             // 极高波动
        }
    }

    /// 获取时间因子
    fn get_time_factor(&self) -> f64 {
        let hour = Utc::now().hour();
        match hour {
            8..=16 => 1.0,  // 主要交易时段
            17..=20 => 0.9, // 次要时段
            _ => 0.8,       // 低流动性时段
        }
    }

    /// 检查相关性限制
    fn check_correlation_limits(&self, symbol: &str, size: f64, balance: f64) -> bool {
        // 检查是否会超过相关币种组合限制
        let exposure_ratio = size / balance;

        // 简化的相关性检查
        let correlated_symbols = self.get_correlated_symbols(symbol);
        let mut correlated_exposure = exposure_ratio;

        for (sym, pos) in &self.positions {
            if correlated_symbols.contains(sym) {
                correlated_exposure += pos.current_size / balance;
            }
        }

        correlated_exposure < 0.2 // 相关币种总暴露不超过20%
    }

    /// 获取相关币种
    fn get_correlated_symbols(&self, symbol: &str) -> Vec<String> {
        // 简化的相关性定义
        match symbol {
            "BTC/USDC" => vec!["ETH/USDC".to_string()],
            "ETH/USDC" => vec!["BTC/USDC".to_string()],
            _ => vec![],
        }
    }

    /// 检查是否应该金字塔加仓
    pub async fn should_pyramid(
        &self,
        position: &TrendPosition,
        current_price: f64,
    ) -> Option<PyramidSignal> {
        if !self.config.pyramid_enabled {
            return None;
        }

        // 计算当前盈利（R倍数）
        let profit_r = self.calculate_profit_r(position, current_price);

        // 检查是否达到加仓条件
        for (i, level) in self.config.pyramid_levels.iter().enumerate() {
            // 检查是否已经在该级别加仓
            let already_pyramided = position.pyramid_entries.iter().any(|e| e.level == i);

            if !already_pyramided && profit_r >= level.trigger_profit {
                let pyramid_size = position.initial_size * level.size_ratio;

                info!("触发金字塔加仓: Level {} @ {:.2}R", i, profit_r);

                return Some(PyramidSignal {
                    level: i,
                    size: pyramid_size,
                    price: current_price,
                });
            }
        }

        None
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

    /// 添加新持仓
    pub fn add_position(&mut self, signal: TradeSignal, size: f64) -> TrendPosition {
        let position = TrendPosition {
            symbol: signal.symbol.clone(),
            side: signal.side,
            entry_price: signal.entry_price,
            average_price: signal.entry_price,
            current_price: signal.entry_price,
            size,
            current_size: size,
            initial_size: size,
            pyramid_entries: vec![],
            pyramid_count: 0,
            stop_loss: signal.stop_loss,
            take_profits: signal
                .take_profits
                .into_iter()
                .map(|tp| TakeProfitLevel {
                    price: tp.price,
                    ratio: tp.ratio,
                    executed: false,
                })
                .collect(),
            entry_time: Utc::now(),
            last_update: Utc::now(),
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            peak_profit: 0.0,
            risk_amount: size * (signal.entry_price - signal.stop_loss).abs(),
        };

        self.positions
            .insert(signal.symbol.clone(), position.clone());
        self.total_exposure += size;

        position
    }

    /// 金字塔加仓
    pub fn pyramid_position(
        &mut self,
        symbol: &str,
        pyramid_signal: PyramidSignal,
    ) -> Result<(), ExchangeError> {
        let position = self
            .positions
            .get_mut(symbol)
            .ok_or_else(|| ExchangeError::Other("持仓不存在".to_string()))?;

        // 记录加仓
        position.pyramid_entries.push(PyramidEntry {
            level: pyramid_signal.level,
            price: pyramid_signal.price,
            size: pyramid_signal.size,
            time: Utc::now(),
        });

        // 更新平均价格
        let total_value = position.average_price * position.current_size
            + pyramid_signal.price * pyramid_signal.size;
        position.current_size += pyramid_signal.size;
        position.average_price = total_value / position.current_size;

        // 更新总暴露
        self.total_exposure += pyramid_signal.size;

        info!(
            "金字塔加仓完成: {} @ {}, 新均价: {}",
            symbol, pyramid_signal.price, position.average_price
        );

        Ok(())
    }

    /// 减仓
    pub fn reduce_position(&mut self, symbol: &str, ratio: f64) -> Result<f64, ExchangeError> {
        let position = self
            .positions
            .get_mut(symbol)
            .ok_or_else(|| ExchangeError::Other("持仓不存在".to_string()))?;

        let reduce_size = position.current_size * ratio;
        position.current_size -= reduce_size;

        self.total_exposure -= reduce_size;

        Ok(reduce_size)
    }

    /// 关闭持仓
    pub fn close_position(&mut self, symbol: &str) -> Option<TrendPosition> {
        if let Some(position) = self.positions.remove(symbol) {
            self.total_exposure -= position.current_size;
            Some(position)
        } else {
            None
        }
    }

    /// 更新持仓盈亏
    pub fn update_position_pnl(
        &mut self,
        symbol: &str,
        current_price: f64,
    ) -> Result<(), ExchangeError> {
        let position = self
            .positions
            .get_mut(symbol)
            .ok_or_else(|| ExchangeError::Other("持仓不存在".to_string()))?;

        // 计算未实现盈亏
        position.unrealized_pnl = match position.side {
            OrderSide::Buy => (current_price - position.average_price) * position.current_size,
            OrderSide::Sell => (position.average_price - current_price) * position.current_size,
        };

        // 更新峰值利润
        if position.unrealized_pnl > position.peak_profit {
            position.peak_profit = position.unrealized_pnl;
        }

        position.last_update = Utc::now();

        Ok(())
    }

    /// 获取持仓报告
    pub fn get_positions_report(&self) -> PositionsReport {
        let total_unrealized_pnl: f64 = self.positions.values().map(|p| p.unrealized_pnl).sum();

        let total_realized_pnl: f64 = self.positions.values().map(|p| p.realized_pnl).sum();

        PositionsReport {
            open_positions: self.positions.len(),
            total_exposure: self.total_exposure,
            total_unrealized_pnl,
            total_realized_pnl,
            positions: self.positions.values().cloned().collect(),
        }
    }
}

/// 持仓报告
#[derive(Debug, Clone)]
pub struct PositionsReport {
    pub open_positions: usize,
    pub total_exposure: f64,
    pub total_unrealized_pnl: f64,
    pub total_realized_pnl: f64,
    pub positions: Vec<TrendPosition>,
}
