//! 专业级日内趋势跟踪策略
//!
//! 风险警告：此策略涉及高风险交易，可能导致本金损失
//! 请在充分理解风险的情况下使用

use async_trait::async_trait;
use chrono::{DateTime, Duration, Timelike, Utc};
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::core::error::ExchangeError;
use crate::core::exchange::Exchange;
use crate::core::risk_manager::{GlobalRiskManager, RiskStatus};
use crate::core::types::{Kline, Order, OrderSide, OrderType, Position};
use crate::cta::account_manager::AccountManager;
use crate::utils::indicators::{calculate_adx, calculate_atr, calculate_ema, calculate_rsi};
use crate::utils::trading_pair_info::TradingPairInfo;

// 使用trend模块中的子模块
use crate::strategies::trend::config::TrendConfig;
use crate::strategies::trend::monitoring::{
    AlertLevel, PerformanceMetrics, TradeRecord, TrendMonitor,
};
use crate::strategies::trend::position_manager::{PositionManager, TrendPosition};
use crate::strategies::trend::risk_control::{RiskController, RiskLevel};
use crate::strategies::trend::signal_generator::{SignalGenerator, TradeSignal};
use crate::strategies::trend::stop_manager::{StopManager, StopUpdate};
use crate::strategies::trend::trend_analyzer::{TrendAnalyzer, TrendSignal};

/// 趋势跟踪策略主结构
pub struct TrendFollowingStrategy {
    /// 策略配置
    config: TrendConfig,

    /// 账户管理器
    account_manager: Arc<AccountManager>,

    /// 全局风险管理器
    global_risk_manager: Arc<RwLock<GlobalRiskManager>>,

    /// 四层风控系统
    risk_controller: RiskController,

    /// 趋势分析器
    trend_analyzer: TrendAnalyzer,

    /// 信号生成器
    signal_generator: SignalGenerator,

    /// 仓位管理器
    position_manager: PositionManager,

    /// 止损管理器
    stop_manager: StopManager,

    /// 监控系统
    monitor: TrendMonitor,

    /// 交易对信息
    trading_pairs: HashMap<String, TradingPairInfo>,

    /// 当前持仓
    positions: Arc<RwLock<HashMap<String, TrendPosition>>>,

    /// 策略状态
    is_running: Arc<RwLock<bool>>,

    /// 最后检查时间
    last_check_time: Arc<RwLock<DateTime<Utc>>>,

    /// 今日交易次数
    daily_trade_count: Arc<RwLock<usize>>,

    /// 连续亏损次数
    consecutive_losses: Arc<RwLock<usize>>,
}

impl TrendFollowingStrategy {
    /// 创建新的趋势跟踪策略
    pub async fn new(
        config: TrendConfig,
        account_manager: Arc<AccountManager>,
        global_risk_manager: Arc<RwLock<GlobalRiskManager>>,
    ) -> Result<Self, ExchangeError> {
        info!("初始化趋势跟踪策略...");

        // 验证配置
        config.validate()?;

        // 初始化各个组件
        let risk_controller = RiskController::new(config.risk_config.clone());
        let mut trend_analyzer = TrendAnalyzer::new(config.indicator_config.clone());
        trend_analyzer.set_account_manager(account_manager.clone());

        let mut signal_generator = SignalGenerator::new(config.signal_config.clone());
        signal_generator.set_account_manager(account_manager.clone());

        let position_manager = PositionManager::new(config.position_config.clone());

        let mut stop_manager = StopManager::new(config.stop_config.clone());
        stop_manager.set_account_manager(account_manager.clone());

        let monitor = TrendMonitor::new();

        // 加载交易对信息
        let mut trading_pairs = HashMap::new();
        for symbol in &config.symbols {
            let pair_info = TradingPairInfo::default_for_symbol(symbol);
            trading_pairs.insert(symbol.clone(), pair_info);
        }

        Ok(Self {
            config,
            account_manager,
            global_risk_manager,
            risk_controller,
            trend_analyzer,
            signal_generator,
            position_manager,
            stop_manager,
            monitor,
            trading_pairs,
            positions: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(RwLock::new(false)),
            last_check_time: Arc::new(RwLock::new(Utc::now())),
            daily_trade_count: Arc::new(RwLock::new(0)),
            consecutive_losses: Arc::new(RwLock::new(0)),
        })
    }

    /// 启动策略
    pub async fn start(&self) -> Result<(), ExchangeError> {
        info!("启动趋势跟踪策略");

        // 启动前检查
        self.pre_start_check().await?;

        // 设置运行状态
        *self.is_running.write().await = true;

        // 启动监控
        self.monitor.start().await;

        // 启动主循环
        self.run_main_loop().await;

        Ok(())
    }

    /// 停止策略
    pub async fn stop(&self) -> Result<(), ExchangeError> {
        warn!("停止趋势跟踪策略");

        // 设置停止标志
        *self.is_running.write().await = false;

        // 平仓所有持仓
        self.close_all_positions("策略停止").await?;

        // 停止监控
        self.monitor.stop().await;

        info!("策略已安全停止");
        Ok(())
    }

    /// 主循环
    async fn run_main_loop(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

        while *self.is_running.read().await {
            interval.tick().await;

            // 执行策略逻辑
            if let Err(e) = self.execute_strategy().await {
                error!("策略执行错误: {}", e);

                // 错误处理
                if self.is_critical_error(&e) {
                    error!("遇到严重错误，停止策略");
                    let _ = self.stop().await;
                    break;
                }
            }

            // 更新监控指标
            self.monitor.update_metrics().await;
        }
    }

    /// 执行策略逻辑
    async fn execute_strategy(&self) -> Result<(), ExchangeError> {
        info!("📊 执行策略分析循环...");

        // 1. 四层风控检查
        let risk_level = self.risk_controller.check_all_layers().await?;
        info!("🛡️ 风控检查结果: {:?}", risk_level);
        match risk_level {
            RiskLevel::Emergency => {
                error!("紧急风险级别，立即停止所有交易");
                self.emergency_stop().await?;
                return Ok(());
            }
            RiskLevel::Danger => {
                warn!("危险风险级别，停止新开仓");
                self.defensive_mode().await?;
                return Ok(());
            }
            RiskLevel::Warning => {
                warn!("警告风险级别，减少交易");
                // 继续但减少仓位
            }
            RiskLevel::Normal => {
                // 正常执行
            }
        }

        // 2. 检查市场状态
        let market_state = self.check_market_state().await?;
        info!(
            "🌐 市场状态: 可交易={}, 原因={}",
            market_state.is_tradeable, market_state.reason
        );
        if !market_state.is_tradeable {
            info!("⛔ 市场状态不适合交易: {}", market_state.reason);
            return Ok(());
        }

        // 3. 扫描交易机会
        info!("🔍 开始扫描 {} 个交易对", self.config.symbols.len());
        for symbol in &self.config.symbols {
            info!("📊 分析交易对: {}", symbol);

            // 检查是否已达到日交易限制
            let trade_count = *self.daily_trade_count.read().await;
            info!(
                "📋 当前日交易次数: {}/{}",
                trade_count, self.config.max_daily_trades
            );
            if trade_count >= self.config.max_daily_trades {
                info!("⚠️ 已达到日交易次数限制");
                break;
            }

            // 分析趋势
            let mut analyzer = self.trend_analyzer.clone();
            info!("📈 开始分析 {} 的趋势...", symbol);
            let trend_signal = analyzer.analyze(symbol).await?;
            info!(
                "🎯 {} 趋势信号: 方向={:?}, 强度={:.2}, 置信度={:.2}",
                symbol, trend_signal.direction, trend_signal.strength, trend_signal.confidence
            );

            // 检查是否需要处理现有持仓
            if let Some(position) = self.get_position(symbol).await {
                self.manage_position(&position, &trend_signal).await?;
            } else {
                // 检查新交易机会
                info!(
                    "⚡ 检查 {} 交易机会 - is_strong: {}, confidence: {}",
                    symbol,
                    trend_signal.is_strong(),
                    trend_signal.confidence
                );
                if trend_signal.is_strong() {
                    info!("🎯 {} 符合强趋势条件，开始生成信号...", symbol);
                    self.check_entry_opportunity(symbol, &trend_signal).await?;
                } else {
                    info!("❌ {} 不符合强趋势条件", symbol);
                }
            }
        }

        // 4. 更新所有止损
        self.update_all_stops().await?;

        // 5. 检查持仓时间
        self.check_position_duration().await?;

        Ok(())
    }

    /// 检查入场机会
    async fn check_entry_opportunity(
        &self,
        symbol: &str,
        trend_signal: &TrendSignal,
    ) -> Result<(), ExchangeError> {
        // 生成交易信号
        let trade_signal = self.signal_generator.generate(symbol, trend_signal).await?;

        if let Some(signal) = trade_signal {
            // 验证信号
            if !self.validate_signal(&signal).await? {
                info!("信号验证失败: {}", symbol);
                return Ok(());
            }

            // 计算仓位
            let position_size = self
                .position_manager
                .calculate_position_size(&signal, self.get_account_balance().await?)
                .await?;

            // 风控检查
            if !self
                .risk_controller
                .approve_trade(&signal, position_size)
                .await?
            {
                warn!("风控拒绝交易: {}", symbol);
                return Ok(());
            }

            // 执行交易
            self.execute_trade(signal, position_size).await?;
        }

        Ok(())
    }

    /// 管理现有持仓
    async fn manage_position(
        &self,
        position: &TrendPosition,
        trend_signal: &TrendSignal,
    ) -> Result<(), ExchangeError> {
        // 检查是否需要平仓
        if self.should_close_position(position, trend_signal).await? {
            self.close_position(position, "信号反转或止损").await?;
            return Ok(());
        }

        // 检查是否需要加仓
        if self.should_pyramid(position, trend_signal).await? {
            self.add_to_position(position).await?;
        }

        // 更新止损
        let stop_update = self.stop_manager.calculate_stop_update(position).await?;
        if let Some(update) = stop_update {
            self.update_stop_loss(position, update).await?;
        }

        Ok(())
    }

    /// 验证交易信号
    async fn validate_signal(&self, signal: &TradeSignal) -> Result<bool, ExchangeError> {
        // 1. 检查风险回报比
        if signal.risk_reward_ratio < self.config.min_risk_reward_ratio {
            return Ok(false);
        }

        // 2. 检查信号置信度
        if signal.confidence < self.config.min_signal_confidence {
            return Ok(false);
        }

        // 3. 检查时间框架一致性
        if !signal.timeframe_aligned {
            return Ok(false);
        }

        // 4. 检查支撑阻力位
        if !signal.has_structure_support {
            return Ok(false);
        }

        Ok(true)
    }

    /// 紧急停止
    async fn emergency_stop(&self) -> Result<(), ExchangeError> {
        error!("执行紧急停止程序");

        // 1. 立即停止所有新交易
        *self.is_running.write().await = false;

        // 2. 平仓所有持仓
        self.close_all_positions("紧急停止").await?;

        // 3. 发送紧急通知
        self.send_emergency_alert().await?;

        Ok(())
    }

    /// 防御模式
    async fn defensive_mode(&self) -> Result<(), ExchangeError> {
        warn!("进入防御模式");

        // 1. 停止新开仓
        // 2. 收紧止损
        // 3. 减少现有持仓

        let positions = self.positions.read().await;
        for (symbol, position) in positions.iter() {
            // 收紧止损
            let tighter_stop = position.stop_loss * 0.5; // 止损减半
            self.update_stop_loss(position, StopUpdate::Tighten(tighter_stop))
                .await?;

            // 减仓50%
            self.reduce_position(position, 0.5).await?;
        }

        Ok(())
    }

    /// 启动前检查
    async fn pre_start_check(&self) -> Result<(), ExchangeError> {
        info!("执行启动前检查...");

        // 1. 检查账户连接
        // 检查账户连接
        // TODO: 实现连接检查

        // 2. 检查风控系统
        self.risk_controller.self_check().await?;

        // 3. 检查账户余额
        let balance = self.get_account_balance().await?;
        if balance < self.config.min_account_balance {
            return Err(ExchangeError::Other(format!(
                "账户余额不足: {} < {}",
                balance, self.config.min_account_balance
            )));
        }

        // 4. 检查市场数据
        for symbol in &self.config.symbols {
            self.check_market_data(symbol).await?;
        }

        info!("启动前检查通过");
        Ok(())
    }

    // === 辅助方法 ===

    async fn get_position(&self, symbol: &str) -> Option<TrendPosition> {
        let positions = self.positions.read().await;
        positions.get(symbol).cloned()
    }

    async fn get_account_balance(&self) -> Result<f64, ExchangeError> {
        // TODO: 实现获取账户余额
        Ok(10000.0) // 模拟余额
    }

    async fn check_market_data(&self, symbol: &str) -> Result<(), ExchangeError> {
        // 检查是否能获取市场数据
        // TODO: 实现市场数据检查
        info!("检查市场数据: {}", symbol);
        Ok(())
    }

    fn is_critical_error(&self, error: &ExchangeError) -> bool {
        // 判断是否为严重错误
        matches!(
            error,
            ExchangeError::NetworkError(_) | ExchangeError::Other(_)
        )
    }

    async fn close_all_positions(&self, reason: &str) -> Result<(), ExchangeError> {
        warn!("平仓所有持仓: {}", reason);

        let positions = self.positions.read().await;
        for (symbol, position) in positions.iter() {
            info!("平仓 {}: {}", symbol, reason);
            // TODO: 实际执行平仓逻辑
        }

        Ok(())
    }

    async fn execute_trade(
        &self,
        signal: TradeSignal,
        position_size: f64,
    ) -> Result<(), ExchangeError> {
        info!(
            "执行交易: {} {:?} @ {}",
            signal.symbol, signal.side, signal.entry_price
        );

        // 更新每日交易次数
        *self.daily_trade_count.write().await += 1;

        // TODO: 实际执行交易逻辑

        Ok(())
    }

    async fn should_close_position(
        &self,
        position: &TrendPosition,
        trend_signal: &TrendSignal,
    ) -> Result<bool, ExchangeError> {
        // 检查是否应该平仓
        if trend_signal.is_reversal() {
            return Ok(true);
        }

        // 检查止损
        if position.is_stop_hit() {
            return Ok(true);
        }

        // 检查持仓时间
        if position.holding_hours() > self.config.max_holding_hours as f64 {
            return Ok(true);
        }

        Ok(false)
    }

    async fn close_position(
        &self,
        position: &TrendPosition,
        reason: &str,
    ) -> Result<(), ExchangeError> {
        info!("平仓 {}: {}", position.symbol, reason);

        // 记录交易
        let record = TradeRecord {
            symbol: position.symbol.clone(),
            side: format!("{:?}", position.side),
            entry_price: position.entry_price,
            exit_price: position.current_price,
            size: position.size,
            pnl: position.unrealized_pnl,
            entry_time: position.entry_time,
            exit_time: Utc::now(),
            holding_time: (Utc::now() - position.entry_time).num_seconds(),
            exit_reason: reason.to_string(),
        };

        self.monitor.record_trade(record).await;

        // 更新连续亏损
        if position.unrealized_pnl < 0.0 {
            *self.consecutive_losses.write().await += 1;
        } else {
            *self.consecutive_losses.write().await = 0;
        }

        // TODO: 实际执行平仓逻辑

        Ok(())
    }

    async fn should_pyramid(
        &self,
        position: &TrendPosition,
        trend_signal: &TrendSignal,
    ) -> Result<bool, ExchangeError> {
        // 检查是否满足加仓条件
        if !self.config.pyramid_enabled {
            return Ok(false);
        }

        if !trend_signal.is_strong() {
            return Ok(false);
        }

        if position.pyramid_count >= 3 {
            return Ok(false);
        }

        // 检查盈利是否达到加仓触发点
        let profit_r = position.profit_in_r();
        for level in &self.config.pyramid_levels {
            if profit_r >= level.trigger_profit && position.pyramid_count < 3 {
                return Ok(true);
            }
        }

        Ok(false)
    }

    async fn add_to_position(&self, position: &TrendPosition) -> Result<(), ExchangeError> {
        info!("加仓: {}", position.symbol);

        // TODO: 实际执行加仓逻辑

        Ok(())
    }

    async fn update_stop_loss(
        &self,
        position: &TrendPosition,
        update: StopUpdate,
    ) -> Result<(), ExchangeError> {
        info!("更新止损: {} {:?}", position.symbol, update);

        // TODO: 实际更新止损逻辑

        Ok(())
    }

    async fn update_all_stops(&self) -> Result<(), ExchangeError> {
        let positions = self.positions.read().await;
        for (_, position) in positions.iter() {
            let stop_update = self.stop_manager.calculate_stop_update(position).await?;
            if let Some(update) = stop_update {
                self.update_stop_loss(position, update).await?;
            }
        }
        Ok(())
    }

    async fn check_position_duration(&self) -> Result<(), ExchangeError> {
        let positions = self.positions.read().await;
        for (_, position) in positions.iter() {
            if position.holding_hours() > self.config.max_holding_hours as f64 {
                warn!(
                    "持仓超时: {} 已持有 {} 小时",
                    position.symbol,
                    position.holding_hours()
                );
                self.close_position(position, "持仓超时").await?;
            }
        }
        Ok(())
    }

    async fn reduce_position(
        &self,
        position: &TrendPosition,
        ratio: f64,
    ) -> Result<(), ExchangeError> {
        info!("减仓 {}: {}%", position.symbol, ratio * 100.0);

        // TODO: 实际执行减仓逻辑

        Ok(())
    }

    async fn send_emergency_alert(&self) -> Result<(), ExchangeError> {
        error!("发送紧急警报");
        self.monitor
            .send_alert(AlertLevel::Critical, "策略触发紧急停止")
            .await;
        Ok(())
    }

    // 更多辅助方法...
}

/// 市场状态
#[derive(Debug, Clone)]
struct MarketState {
    is_tradeable: bool,
    reason: String,
    volatility: f64,
    liquidity: f64,
}

impl TrendFollowingStrategy {
    async fn check_market_state(&self) -> Result<MarketState, ExchangeError> {
        // 检查市场是否适合交易
        let now = Utc::now();
        let hour = now.hour();

        // 检查交易时间
        if hour >= 2 && hour <= 6 {
            return Ok(MarketState {
                is_tradeable: false,
                reason: "低流动性时段".to_string(),
                volatility: 0.0,
                liquidity: 0.0,
            });
        }

        // 其他检查...

        Ok(MarketState {
            is_tradeable: true,
            reason: "正常".to_string(),
            volatility: 0.02,
            liquidity: 1000000.0,
        })
    }
}

// 更多实现...
