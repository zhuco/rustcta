//! 仓位管理模块

use chrono::{DateTime, Timelike, Utc};
use log::{debug, info, warn};
use std::collections::HashMap;

use crate::core::error::ExchangeError;
use crate::core::types::{OrderSide, Position};
use crate::strategies::trend::config::{PositionConfig, PyramidLevel};
use crate::strategies::trend::signal_generator::TradeSignal;

#[derive(Clone, Copy, Debug, Default)]
pub struct AccountCapital {
    pub total: f64,
    pub available: f64,
}

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
    inventory_limits: HashMap<String, f64>,
    inventory_value_limits: HashMap<String, f64>,
    max_leverage: f64,
}

impl PositionManager {
    fn position_key(symbol: &str, side: OrderSide) -> String {
        format!("{}#{}", symbol, Self::side_label(side))
    }

    fn side_label(side: OrderSide) -> &'static str {
        match side {
            OrderSide::Buy => "LONG",
            OrderSide::Sell => "SHORT",
        }
    }

    fn parse_symbol_from_key<'a>(&self, key: &'a str) -> &'a str {
        key.split('#').next().unwrap_or(key)
    }

    fn symbol_total_size(&self, symbol: &str) -> f64 {
        self.positions
            .values()
            .filter(|p| p.symbol == symbol)
            .map(|p| p.current_size)
            .sum()
    }

    fn symbol_total_notional(&self, symbol: &str) -> f64 {
        self.positions
            .values()
            .filter(|p| p.symbol == symbol)
            .map(|p| p.current_size * p.current_price.abs())
            .sum()
    }

    /// 创建新的仓位管理器
    pub fn new(
        config: PositionConfig,
        inventory_limits: HashMap<String, f64>,
        inventory_value_limits: HashMap<String, f64>,
        max_leverage: f64,
    ) -> Self {
        Self {
            config,
            positions: HashMap::new(),
            total_exposure: 0.0,
            correlation_matrix: HashMap::new(),
            inventory_limits,
            inventory_value_limits,
            max_leverage,
        }
    }

    /// 计算仓位大小
    pub fn calculate_position_size(
        &self,
        signal: &TradeSignal,
        capital: AccountCapital,
        target_notional: Option<f64>,
    ) -> Result<f64, ExchangeError> {
        debug!("计算 {} 的仓位大小", signal.symbol);

        if capital.available <= 0.0 {
            return Err(ExchangeError::Other("账户余额无效".to_string()));
        }

        let entry_price = signal.entry_price.max(1e-6);
        let mut position_size = if let Some(target) = target_notional {
            (target / entry_price).max(0.0)
        } else {
            let risk_per_unit = (signal.entry_price - signal.stop_loss).abs().max(1e-6);
            let base_notional = capital.available * self.config.base_risk_ratio.max(1e-4);
            let mut size = (base_notional / risk_per_unit).max(0.0);

            let kelly = self.calculate_kelly_factor(signal).clamp(0.1, 1.0);
            size *= kelly.max(0.25);

            if self.config.trend_factor_enabled {
                size *= self.get_trend_factor(signal);
            }

            if self.config.volatility_factor_enabled {
                let stop_percent = (signal.entry_price - signal.stop_loss).abs() / entry_price;
                size *= self.get_volatility_factor(stop_percent);
            }

            if self.config.time_factor_enabled {
                size *= self.get_time_factor();
            }

            if signal.suggested_size > 0.0 {
                size = size.min(signal.suggested_size);
            }

            size
        };

        let max_notional = (capital.available * self.max_leverage).max(5.0);
        position_size = position_size.min(max_notional / entry_price);

        let min_notional = 20.0;
        let min_qty = min_notional / entry_price;
        if position_size < min_qty {
            if min_notional > capital.available * self.max_leverage {
                warn!(
                    "{} 可用余额不足以满足最小名义 {} USDT，跳过",
                    signal.symbol, min_notional
                );
                return Ok(0.0);
            }
            position_size = min_qty;
        }

        if let Some(limit) = self.inventory_limits.get(&signal.symbol) {
            let current = self.symbol_total_size(&signal.symbol);
            let remaining = limit - current;
            if remaining <= 0.0 {
                warn!("{} 库存上限({})已满，跳过开仓", signal.symbol, limit);
                return Ok(0.0);
            }
            position_size = position_size.min(remaining);
        }

        if let Some(value_cap) = self.inventory_value_limits.get(&signal.symbol) {
            let current_notional = self.symbol_total_notional(&signal.symbol);
            if current_notional >= *value_cap {
                warn!(
                    "{} 当前名义 {:.2} 已达到上限 {:.2}，跳过开仓",
                    signal.symbol, current_notional, value_cap
                );
                return Ok(0.0);
            }

            let remaining_notional = (value_cap - current_notional).max(0.0);
            if remaining_notional <= f64::EPSILON {
                warn!("{} 名义剩余额度不足，跳过开仓", signal.symbol);
                return Ok(0.0);
            }

            let planned_notional = position_size * entry_price;
            if planned_notional > remaining_notional {
                let adjusted = remaining_notional / entry_price;
                if adjusted <= f64::EPSILON {
                    warn!(
                        "{} 可分配名义 {:.2} USDT，低于最小仓位，跳过",
                        signal.symbol, remaining_notional
                    );
                    return Ok(0.0);
                }
                position_size = adjusted;
            }
        }

        let notional = position_size * entry_price;
        if notional < 20.0 {
            warn!(
                "{} 计算仓位名义({:.2} USDT)低于交易所最小值，跳过",
                signal.symbol, notional
            );
            return Ok(0.0);
        }

        info!("动态仓位: {:.6} (约 {:.2} USDT)", position_size, notional);

        Ok(position_size.max(0.0))
    }

    /// 是否存在持仓
    pub fn has_open_position(&self, symbol: &str) -> bool {
        self.positions.values().any(|p| p.symbol == symbol)
    }

    /// 是否存在指定方向持仓
    pub fn has_open_position_side(&self, symbol: &str, side: OrderSide) -> bool {
        self.positions
            .contains_key(&Self::position_key(symbol, side))
    }

    /// 当前持仓数量
    pub fn total_open_positions(&self) -> usize {
        self.positions.len()
    }

    /// 当前总暴露
    pub fn current_total_exposure(&self) -> f64 {
        self.total_exposure
    }

    /// 计算Kelly因子
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    fn get_trend_factor(&self, signal: &TradeSignal) -> f64 {
        match signal.confidence {
            c if c > 80.0 => 1.2,
            c if c > 70.0 => 1.1,
            c if c > 60.0 => 1.0,
            _ => 0.8,
        }
    }

    /// 获取波动率因子
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    fn get_time_factor(&self) -> f64 {
        let hour = Utc::now().hour();
        match hour {
            8..=16 => 1.0,  // 主要交易时段
            17..=20 => 0.9, // 次要时段
            _ => 0.8,       // 低流动性时段
        }
    }

    /// 检查相关性限制
    #[allow(dead_code)]
    fn check_correlation_limits(&self, symbol: &str, size: f64, balance: f64) -> bool {
        // 检查是否会超过相关币种组合限制
        let exposure_ratio = size / balance;

        // 简化的相关性检查
        let correlated_symbols = self.get_correlated_symbols(symbol);
        let mut correlated_exposure = exposure_ratio;

        for (sym, pos) in &self.positions {
            if correlated_symbols.contains(&self.parse_symbol_from_key(sym).to_string()) {
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

        let key = Self::position_key(&signal.symbol, signal.side);
        self.positions.insert(key, position.clone());
        self.total_exposure += size;

        position
    }

    pub fn restore_position(&mut self, position: TrendPosition) {
        let key = Self::position_key(&position.symbol, position.side);
        self.total_exposure += position.current_size;
        self.positions.insert(key, position);
    }

    /// 金字塔加仓
    pub fn pyramid_position(
        &mut self,
        symbol: &str,
        pyramid_signal: PyramidSignal,
    ) -> Result<(), ExchangeError> {
        let position = self
            .positions
            .values_mut()
            .find(|p| p.symbol == symbol)
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
            .values_mut()
            .find(|p| p.symbol == symbol)
            .ok_or_else(|| ExchangeError::Other("持仓不存在".to_string()))?;

        let reduce_size = position.current_size * ratio;
        position.current_size -= reduce_size;

        self.total_exposure -= reduce_size;

        Ok(reduce_size)
    }

    /// 关闭持仓
    pub fn close_position_with_fill(
        &mut self,
        symbol: &str,
        fill_price: f64,
    ) -> Option<TrendPosition> {
        let key = self
            .positions
            .iter()
            .find(|(_, pos)| pos.symbol == symbol)
            .map(|(k, _)| k.clone())?;

        self.close_position_by_key(&key, fill_price)
    }

    pub fn close_position_with_fill_side(
        &mut self,
        symbol: &str,
        side: OrderSide,
        fill_price: f64,
    ) -> Option<TrendPosition> {
        let key = Self::position_key(symbol, side);
        self.close_position_by_key(&key, fill_price)
    }

    fn close_position_by_key(&mut self, key: &str, fill_price: f64) -> Option<TrendPosition> {
        let mut position = self.positions.remove(key)?;
        let exit_size = position.current_size;
        let pnl = match position.side {
            OrderSide::Buy => (fill_price - position.average_price) * exit_size,
            OrderSide::Sell => (position.average_price - fill_price) * exit_size,
        };

        position.current_price = fill_price;
        position.current_size = 0.0;
        position.realized_pnl += pnl;
        position.unrealized_pnl = 0.0;
        position.last_update = Utc::now();

        self.total_exposure = (self.total_exposure - exit_size).max(0.0);
        Some(position)
    }

    /// 更新持仓盈亏
    pub fn update_position_pnl(
        &mut self,
        symbol: &str,
        current_price: f64,
    ) -> Result<(), ExchangeError> {
        let key = self
            .positions
            .iter()
            .find(|(_, pos)| pos.symbol == symbol)
            .map(|(k, _)| k.clone())
            .ok_or_else(|| ExchangeError::Other("持仓不存在".to_string()))?;
        self.update_position_pnl_by_key(&key, current_price)
    }

    pub fn update_position_pnl_side(
        &mut self,
        symbol: &str,
        side: OrderSide,
        current_price: f64,
    ) -> Result<(), ExchangeError> {
        let key = Self::position_key(symbol, side);
        self.update_position_pnl_by_key(&key, current_price)
    }

    fn update_position_pnl_by_key(
        &mut self,
        key: &str,
        current_price: f64,
    ) -> Result<(), ExchangeError> {
        let position = self
            .positions
            .get_mut(key)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::trend::signal_generator::{
        SignalMetadata, SignalType, TakeProfit, TradeSignal,
    };
    use std::collections::HashMap;

    fn sample_position_config() -> PositionConfig {
        PositionConfig {
            base_risk_ratio: 0.01,
            kelly_fraction: 0.25,
            pyramid_enabled: false,
            pyramid_levels: Vec::new(),
            trend_factor_enabled: true,
            volatility_factor_enabled: true,
            time_factor_enabled: true,
            max_holding_hours: 24,
        }
    }

    fn sample_signal(symbol: &str, side: OrderSide, entry: f64, stop: f64) -> TradeSignal {
        TradeSignal {
            symbol: symbol.to_string(),
            signal_type: SignalType::TrendBreakout,
            side,
            entry_price: entry,
            stop_loss: stop,
            take_profits: vec![TakeProfit {
                price: entry * 1.02,
                ratio: 1.0,
            }],
            suggested_size: 1.0,
            risk_reward_ratio: 2.0,
            confidence: 80.0,
            timeframe_aligned: true,
            has_structure_support: true,
            expire_time: Utc::now(),
            metadata: SignalMetadata {
                trend_strength: 70.0,
                volume_confirmed: true,
                key_level_nearby: false,
                pattern_name: None,
                generated_at: Utc::now(),
            },
        }
    }

    #[test]
    fn manager_supports_dual_positions_for_same_symbol() {
        let mut manager = PositionManager::new(
            sample_position_config(),
            HashMap::new(),
            HashMap::new(),
            3.0,
        );

        manager.add_position(sample_signal("BTC/USDC", OrderSide::Buy, 100.0, 95.0), 1.0);
        manager.add_position(
            sample_signal("BTC/USDC", OrderSide::Sell, 100.0, 105.0),
            0.8,
        );

        assert!(manager.has_open_position("BTC/USDC"));
        assert!(manager.has_open_position_side("BTC/USDC", OrderSide::Buy));
        assert!(manager.has_open_position_side("BTC/USDC", OrderSide::Sell));
        assert_eq!(manager.total_open_positions(), 2);
    }

    #[test]
    fn close_by_side_only_removes_target_position() {
        let mut manager = PositionManager::new(
            sample_position_config(),
            HashMap::new(),
            HashMap::new(),
            3.0,
        );

        manager.add_position(sample_signal("ETH/USDC", OrderSide::Buy, 200.0, 190.0), 1.0);
        manager.add_position(
            sample_signal("ETH/USDC", OrderSide::Sell, 200.0, 210.0),
            1.2,
        );

        let closed = manager.close_position_with_fill_side("ETH/USDC", OrderSide::Buy, 205.0);
        assert!(closed.is_some());
        assert_eq!(closed.unwrap().side, OrderSide::Buy);

        assert!(!manager.has_open_position_side("ETH/USDC", OrderSide::Buy));
        assert!(manager.has_open_position_side("ETH/USDC", OrderSide::Sell));
        assert_eq!(manager.total_open_positions(), 1);
    }
}
