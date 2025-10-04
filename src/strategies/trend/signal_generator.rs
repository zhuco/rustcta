//! 信号生成模块

use chrono::{DateTime, Duration, Utc};
use log::{debug, info};

use crate::core::error::ExchangeError;
use crate::core::types::MarketType;
use crate::core::types::OrderSide;
use crate::cta::account_manager::AccountManager;
use crate::strategies::trend::config::SignalConfig;
use crate::strategies::trend::trend_analyzer::{TrendDirection, TrendSignal};
use std::sync::Arc;

/// 交易信号
#[derive(Debug, Clone)]
pub struct TradeSignal {
    pub symbol: String,
    pub signal_type: SignalType,
    pub side: OrderSide,
    pub entry_price: f64,
    pub stop_loss: f64,
    pub take_profits: Vec<TakeProfit>,
    pub suggested_size: f64,
    pub risk_reward_ratio: f64,
    pub confidence: f64,
    pub timeframe_aligned: bool,
    pub has_structure_support: bool,
    pub expire_time: DateTime<Utc>,
    pub metadata: SignalMetadata,
}

/// 信号类型
#[derive(Debug, Clone, PartialEq)]
pub enum SignalType {
    TrendBreakout,   // 趋势突破
    Pullback,        // 回调入场
    MomentumSurge,   // 动量爆发
    PatternBreakout, // 形态突破
    Reversal,        // 反转信号
}

/// 止盈目标
#[derive(Debug, Clone)]
pub struct TakeProfit {
    pub price: f64,
    pub ratio: f64, // 平仓比例
}

/// 信号元数据
#[derive(Debug, Clone)]
pub struct SignalMetadata {
    pub trend_strength: f64,
    pub volume_confirmed: bool,
    pub key_level_nearby: bool,
    pub pattern_name: Option<String>,
    pub generated_at: DateTime<Utc>,
}

/// 信号生成器
pub struct SignalGenerator {
    config: SignalConfig,
    pattern_detector: PatternDetector,
    level_analyzer: LevelAnalyzer,
    account_manager: Option<Arc<AccountManager>>,
}

/// 形态检测器
struct PatternDetector {
    min_bars: usize,
    confidence_threshold: f64,
}

/// 关键位分析器
struct LevelAnalyzer {
    lookback: usize,
}

impl SignalGenerator {
    /// 创建新的信号生成器
    pub fn new(config: SignalConfig) -> Self {
        Self {
            config: config.clone(),
            pattern_detector: PatternDetector {
                min_bars: config.pattern_min_bars,
                confidence_threshold: config.pattern_confidence,
            },
            level_analyzer: LevelAnalyzer {
                lookback: config.breakout_lookback,
            },
            account_manager: None,
        }
    }

    /// 设置账户管理器
    pub fn set_account_manager(&mut self, manager: Arc<AccountManager>) {
        self.account_manager = Some(manager);
    }

    /// 生成交易信号
    pub async fn generate(
        &self,
        symbol: &str,
        trend_signal: &TrendSignal,
    ) -> Result<Option<TradeSignal>, ExchangeError> {
        info!("🎯 开始为 {} 生成交易信号", symbol);
        info!(
            "  - 可交易检查: is_tradeable={}",
            trend_signal.is_tradeable()
        );
        info!("  - 趋势方向: {:?}", trend_signal.direction);
        info!("  - 置信度: {:.2}%", trend_signal.confidence);
        info!("  - 时间框架对齐: {}", trend_signal.timeframe_aligned);

        // 检查趋势是否可交易
        if !trend_signal.is_tradeable() {
            info!("❌ {} 不满足交易条件，跳过信号生成", symbol);
            return Ok(None);
        }

        info!("✅ {} 满足交易条件，继续生成信号...", symbol);

        // 根据趋势方向生成信号
        let signal = match trend_signal.direction {
            TrendDirection::StrongBullish => {
                self.generate_long_signal(symbol, trend_signal).await?
            }
            TrendDirection::Bullish => {
                self.generate_pullback_long_signal(symbol, trend_signal)
                    .await?
            }
            TrendDirection::StrongBearish => {
                self.generate_short_signal(symbol, trend_signal).await?
            }
            TrendDirection::Bearish => {
                self.generate_pullback_short_signal(symbol, trend_signal)
                    .await?
            }
            TrendDirection::Neutral => {
                return Ok(None);
            }
        };

        // 验证信号质量
        if let Some(sig) = signal {
            if self.validate_signal_quality(&sig) {
                info!(
                    "生成交易信号: {} {:?} @ {}",
                    sig.symbol, sig.side, sig.entry_price
                );
                return Ok(Some(sig));
            }
        }

        Ok(None)
    }

    /// 生成做多信号（强势）
    async fn generate_long_signal(
        &self,
        symbol: &str,
        trend_signal: &TrendSignal,
    ) -> Result<Option<TradeSignal>, ExchangeError> {
        // 获取当前市场价格
        let current_price = self.get_market_price(symbol).await?;

        // 寻找入场点
        let entry_price = self.find_breakout_entry(current_price, OrderSide::Buy);

        // 计算止损
        let stop_loss = self.calculate_stop_loss(entry_price, OrderSide::Buy, 0.02);

        // 计算止盈目标
        let take_profits = self.calculate_take_profits(entry_price, stop_loss, OrderSide::Buy);

        // 计算风险回报比
        let risk = (entry_price - stop_loss).abs();
        let reward = take_profits
            .first()
            .map(|tp| (tp.price - entry_price).abs())
            .unwrap_or(0.0);
        let risk_reward_ratio = if risk > 0.0 { reward / risk } else { 0.0 };

        Ok(Some(TradeSignal {
            symbol: symbol.to_string(),
            signal_type: SignalType::TrendBreakout,
            side: OrderSide::Buy,
            entry_price,
            stop_loss,
            take_profits,
            suggested_size: 0.01, // 基础仓位
            risk_reward_ratio,
            confidence: trend_signal.confidence,
            timeframe_aligned: trend_signal.timeframe_aligned,
            has_structure_support: true, // 简化
            expire_time: Utc::now() + Duration::seconds(self.config.signal_expiry as i64),
            metadata: SignalMetadata {
                trend_strength: trend_signal.strength,
                volume_confirmed: trend_signal.volume_confirmation,
                key_level_nearby: true,
                pattern_name: None,
                generated_at: Utc::now(),
            },
        }))
    }

    /// 生成回调做多信号
    async fn generate_pullback_long_signal(
        &self,
        symbol: &str,
        trend_signal: &TrendSignal,
    ) -> Result<Option<TradeSignal>, ExchangeError> {
        let current_price = self.get_market_price(symbol).await?;

        // 寻找回调入场点
        let entry_price = self.find_pullback_entry(current_price, OrderSide::Buy);

        let stop_loss = self.calculate_stop_loss(entry_price, OrderSide::Buy, 0.015);
        let take_profits = self.calculate_take_profits(entry_price, stop_loss, OrderSide::Buy);

        let risk = (entry_price - stop_loss).abs();
        let reward = take_profits
            .first()
            .map(|tp| (tp.price - entry_price).abs())
            .unwrap_or(0.0);
        let risk_reward_ratio = if risk > 0.0 { reward / risk } else { 0.0 };

        Ok(Some(TradeSignal {
            symbol: symbol.to_string(),
            signal_type: SignalType::Pullback,
            side: OrderSide::Buy,
            entry_price,
            stop_loss,
            take_profits,
            suggested_size: 0.01,
            risk_reward_ratio,
            confidence: trend_signal.confidence * 0.9, // 回调信号置信度略低
            timeframe_aligned: trend_signal.timeframe_aligned,
            has_structure_support: true,
            expire_time: Utc::now() + Duration::seconds(self.config.signal_expiry as i64),
            metadata: SignalMetadata {
                trend_strength: trend_signal.strength,
                volume_confirmed: trend_signal.volume_confirmation,
                key_level_nearby: true,
                pattern_name: None,
                generated_at: Utc::now(),
            },
        }))
    }

    /// 生成做空信号
    async fn generate_short_signal(
        &self,
        symbol: &str,
        trend_signal: &TrendSignal,
    ) -> Result<Option<TradeSignal>, ExchangeError> {
        let current_price = self.get_market_price(symbol).await?;

        let entry_price = self.find_breakout_entry(current_price, OrderSide::Sell);
        let stop_loss = self.calculate_stop_loss(entry_price, OrderSide::Sell, 0.02);
        let take_profits = self.calculate_take_profits(entry_price, stop_loss, OrderSide::Sell);

        let risk = (stop_loss - entry_price).abs();
        let reward = take_profits
            .first()
            .map(|tp| (entry_price - tp.price).abs())
            .unwrap_or(0.0);
        let risk_reward_ratio = if risk > 0.0 { reward / risk } else { 0.0 };

        Ok(Some(TradeSignal {
            symbol: symbol.to_string(),
            signal_type: SignalType::TrendBreakout,
            side: OrderSide::Sell,
            entry_price,
            stop_loss,
            take_profits,
            suggested_size: 0.01,
            risk_reward_ratio,
            confidence: trend_signal.confidence,
            timeframe_aligned: trend_signal.timeframe_aligned,
            has_structure_support: true,
            expire_time: Utc::now() + Duration::seconds(self.config.signal_expiry as i64),
            metadata: SignalMetadata {
                trend_strength: trend_signal.strength,
                volume_confirmed: trend_signal.volume_confirmation,
                key_level_nearby: true,
                pattern_name: None,
                generated_at: Utc::now(),
            },
        }))
    }

    /// 生成回调做空信号
    async fn generate_pullback_short_signal(
        &self,
        symbol: &str,
        trend_signal: &TrendSignal,
    ) -> Result<Option<TradeSignal>, ExchangeError> {
        let current_price = self.get_market_price(symbol).await?;

        let entry_price = self.find_pullback_entry(current_price, OrderSide::Sell);
        let stop_loss = self.calculate_stop_loss(entry_price, OrderSide::Sell, 0.015);
        let take_profits = self.calculate_take_profits(entry_price, stop_loss, OrderSide::Sell);

        let risk = (stop_loss - entry_price).abs();
        let reward = take_profits
            .first()
            .map(|tp| (entry_price - tp.price).abs())
            .unwrap_or(0.0);
        let risk_reward_ratio = if risk > 0.0 { reward / risk } else { 0.0 };

        Ok(Some(TradeSignal {
            symbol: symbol.to_string(),
            signal_type: SignalType::Pullback,
            side: OrderSide::Sell,
            entry_price,
            stop_loss,
            take_profits,
            suggested_size: 0.01,
            risk_reward_ratio,
            confidence: trend_signal.confidence * 0.9,
            timeframe_aligned: trend_signal.timeframe_aligned,
            has_structure_support: true,
            expire_time: Utc::now() + Duration::seconds(self.config.signal_expiry as i64),
            metadata: SignalMetadata {
                trend_strength: trend_signal.strength,
                volume_confirmed: trend_signal.volume_confirmation,
                key_level_nearby: true,
                pattern_name: None,
                generated_at: Utc::now(),
            },
        }))
    }

    /// 寻找突破入场点
    fn find_breakout_entry(&self, current_price: f64, side: OrderSide) -> f64 {
        match side {
            OrderSide::Buy => current_price * 1.001,  // 突破上方0.1%
            OrderSide::Sell => current_price * 0.999, // 突破下方0.1%
        }
    }

    /// 寻找回调入场点
    fn find_pullback_entry(&self, current_price: f64, side: OrderSide) -> f64 {
        let pullback = self.config.pullback_ratio;
        match side {
            OrderSide::Buy => current_price * (1.0 - pullback * 0.01), // 回调入场
            OrderSide::Sell => current_price * (1.0 + pullback * 0.01), // 反弹入场
        }
    }

    /// 计算止损
    fn calculate_stop_loss(&self, entry: f64, side: OrderSide, risk_percent: f64) -> f64 {
        match side {
            OrderSide::Buy => entry * (1.0 - risk_percent),
            OrderSide::Sell => entry * (1.0 + risk_percent),
        }
    }

    /// 计算止盈目标
    fn calculate_take_profits(&self, entry: f64, stop: f64, side: OrderSide) -> Vec<TakeProfit> {
        let risk = (entry - stop).abs();

        match side {
            OrderSide::Buy => vec![
                TakeProfit {
                    price: entry + risk * 1.5,
                    ratio: 0.3,
                }, // 1.5R
                TakeProfit {
                    price: entry + risk * 2.5,
                    ratio: 0.3,
                }, // 2.5R
                TakeProfit {
                    price: entry + risk * 4.0,
                    ratio: 0.2,
                }, // 4R
            ],
            OrderSide::Sell => vec![
                TakeProfit {
                    price: entry - risk * 1.5,
                    ratio: 0.3,
                },
                TakeProfit {
                    price: entry - risk * 2.5,
                    ratio: 0.3,
                },
                TakeProfit {
                    price: entry - risk * 4.0,
                    ratio: 0.2,
                },
            ],
        }
    }

    /// 获取市场价格
    async fn get_market_price(&self, symbol: &str) -> Result<f64, ExchangeError> {
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

    /// 验证信号质量
    fn validate_signal_quality(&self, signal: &TradeSignal) -> bool {
        // 检查风险回报比
        if signal.risk_reward_ratio < 2.0 {
            debug!("风险回报比太低: {}", signal.risk_reward_ratio);
            return false;
        }

        // 检查置信度
        if signal.confidence < 60.0 {
            debug!("置信度太低: {}", signal.confidence);
            return false;
        }

        // 检查止损距离
        let stop_distance = ((signal.entry_price - signal.stop_loss) / signal.entry_price).abs();
        if stop_distance > 0.05 {
            // 止损超过5%
            debug!("止损距离太大: {:.2}%", stop_distance * 100.0);
            return false;
        }

        true
    }
}
