//! 趋势策略配置模块

use crate::core::error::ExchangeError;
use serde::{Deserialize, Serialize};

/// 趋势策略主配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TrendConfig {
    /// 策略名称
    pub name: String,

    /// 账户ID
    pub account_id: String,

    /// 监控的交易对列表
    pub symbols: Vec<String>,

    /// 最大同时持仓数
    pub max_positions: usize,

    /// 每日最大交易次数
    pub max_daily_trades: usize,

    /// 最小账户余额要求
    pub min_account_balance: f64,

    /// 最小风险回报比
    pub min_risk_reward_ratio: f64,

    /// 最小信号置信度
    pub min_signal_confidence: f64,

    /// 最大持仓时间（小时）
    pub max_holding_hours: usize,

    /// 是否启用金字塔加仓
    pub pyramid_enabled: bool,

    /// 金字塔加仓级别
    pub pyramid_levels: Vec<PyramidLevel>,

    /// 风控配置
    pub risk_config: RiskConfig,

    /// 指标配置
    pub indicator_config: IndicatorConfig,

    /// 信号配置
    pub signal_config: SignalConfig,

    /// 仓位配置
    pub position_config: PositionConfig,

    /// 止损配置
    pub stop_config: StopConfig,
}

/// 风控配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RiskConfig {
    /// 单笔最大风险（账户百分比）
    pub max_risk_per_trade: f64,

    /// 日最大亏损（账户百分比）
    pub max_daily_loss: f64,

    /// 最大回撤（账户百分比）
    pub max_drawdown: f64,

    /// 连续亏损次数限制
    pub max_consecutive_losses: usize,

    /// 总仓位上限（账户百分比）
    pub max_total_exposure: f64,

    /// 单币种仓位上限（账户百分比）
    pub max_single_exposure: f64,

    /// 相关币种组合上限
    pub max_correlated_exposure: f64,

    /// 最大杠杆
    pub max_leverage: f64,

    /// 滑点容忍度
    pub max_slippage: f64,

    /// 紧急停止阈值
    pub emergency_stop_loss: f64,
}

/// 指标配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct IndicatorConfig {
    /// 主要时间框架
    pub primary_timeframe: String,

    /// 次要时间框架
    pub secondary_timeframe: String,

    /// 触发时间框架
    pub trigger_timeframe: String,

    /// 快速EMA周期
    pub fast_ema: usize,

    /// 慢速EMA周期
    pub slow_ema: usize,

    /// ATR周期
    pub atr_period: usize,

    /// ADX周期
    pub adx_period: usize,

    /// ADX趋势阈值
    pub adx_threshold: f64,

    /// RSI周期
    pub rsi_period: usize,

    /// RSI超买线
    pub rsi_overbought: f64,

    /// RSI超卖线
    pub rsi_oversold: f64,

    /// MACD参数
    pub macd_fast: usize,
    pub macd_slow: usize,
    pub macd_signal: usize,

    /// 布林带参数
    pub bb_period: usize,
    pub bb_std: f64,
}

/// 信号配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SignalConfig {
    /// 趋势突破配置
    pub breakout_lookback: usize,
    pub breakout_confirmation_bars: usize,

    /// 回调入场配置
    pub pullback_ratio: f64,
    pub pullback_min_trend_strength: f64,

    /// 动量配置
    pub momentum_threshold: f64,
    pub momentum_lookback: usize,

    /// 形态识别配置
    pub pattern_min_bars: usize,
    pub pattern_confidence: f64,

    /// 成交量确认
    pub volume_multiplier: f64,
    pub volume_lookback: usize,

    /// 信号过期时间（秒）
    pub signal_expiry: u64,
}

/// 仓位配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PositionConfig {
    /// 基础风险比例
    pub base_risk_ratio: f64,

    /// Kelly系数（使用Kelly公式的比例）
    pub kelly_fraction: f64,

    /// 金字塔加仓配置
    pub pyramid_enabled: bool,
    pub pyramid_levels: Vec<PyramidLevel>,

    /// 仓位调整因子
    pub trend_factor_enabled: bool,
    pub volatility_factor_enabled: bool,
    pub time_factor_enabled: bool,

    /// 最大持仓时间（小时）
    pub max_holding_hours: u64,
}

/// 金字塔加仓级别
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PyramidLevel {
    /// 触发利润（R倍数）
    pub trigger_profit: f64,

    /// 加仓比例
    pub size_ratio: f64,
}

/// 止损配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StopConfig {
    /// 初始止损类型
    pub initial_stop_type: StopType,

    /// ATR止损倍数
    pub atr_multiplier: f64,

    /// 固定止损百分比
    pub fixed_stop_percent: f64,

    /// 移动止损配置
    pub trailing_stop_enabled: bool,
    pub trailing_activation: f64, // 激活利润（R倍数）
    pub trailing_distance: f64,   // 跟踪距离
    pub trailing_step: f64,       // 步进大小

    /// 时间止损（小时）
    pub time_stop_hours: Option<u64>,

    /// 保本止损
    pub breakeven_enabled: bool,
    pub breakeven_trigger: f64, // 触发利润（R倍数）

    /// 部分止盈配置
    pub partial_targets: Vec<PartialTarget>,
}

/// 止损类型
#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum StopType {
    Fixed,
    ATR,
    Structure,
    Percentage,
}

/// 部分止盈目标
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartialTarget {
    /// 目标利润（R倍数）
    pub target_r: f64,

    /// 平仓比例
    pub close_ratio: f64,
}

impl TrendConfig {
    /// 验证配置有效性
    pub fn validate(&self) -> Result<(), ExchangeError> {
        // 风控参数验证
        if self.risk_config.max_risk_per_trade > 0.02 {
            return Err(ExchangeError::Other("单笔风险不能超过2%".to_string()));
        }

        if self.risk_config.max_daily_loss > 0.05 {
            return Err(ExchangeError::Other("日最大亏损不能超过5%".to_string()));
        }

        if self.risk_config.max_leverage > 3.0 {
            return Err(ExchangeError::Other("最大杠杆不能超过3倍".to_string()));
        }

        // 仓位参数验证
        if self.risk_config.max_total_exposure > 0.5 {
            return Err(ExchangeError::Other("总仓位不能超过50%".to_string()));
        }

        // 其他验证...

        Ok(())
    }

    /// 从文件加载配置
    pub fn from_file(path: &str) -> Result<Self, ExchangeError> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| ExchangeError::Other(format!("读取配置文件失败: {}", e)))?;

        let config: Self = serde_yaml::from_str(&content)
            .map_err(|e| ExchangeError::Other(format!("解析配置失败: {}", e)))?;

        config.validate()?;

        Ok(config)
    }
}

impl Default for TrendConfig {
    fn default() -> Self {
        Self {
            name: "Trend Following Strategy".to_string(),
            account_id: "binance_main".to_string(),
            symbols: vec![
                "BTC/USDC".to_string(),
                "ETH/USDC".to_string(),
                "SOL/USDC".to_string(),
            ],
            max_positions: 3,
            max_daily_trades: 10,
            min_account_balance: 1000.0,
            min_risk_reward_ratio: 2.0,
            min_signal_confidence: 60.0,
            max_holding_hours: 24,
            pyramid_enabled: true,
            pyramid_levels: vec![
                PyramidLevel {
                    trigger_profit: 0.5,
                    size_ratio: 0.3,
                },
                PyramidLevel {
                    trigger_profit: 1.0,
                    size_ratio: 0.3,
                },
                PyramidLevel {
                    trigger_profit: 2.0,
                    size_ratio: 0.2,
                },
            ],

            risk_config: RiskConfig {
                max_risk_per_trade: 0.01, // 1%
                max_daily_loss: 0.03,     // 3%
                max_drawdown: 0.15,       // 15%
                max_consecutive_losses: 3,
                max_total_exposure: 0.5,  // 50%
                max_single_exposure: 0.1, // 10%
                max_correlated_exposure: 0.2,
                max_leverage: 3.0,
                max_slippage: 0.002,      // 0.2%
                emergency_stop_loss: 0.1, // 10%
            },

            indicator_config: IndicatorConfig {
                primary_timeframe: "4h".to_string(),
                secondary_timeframe: "1h".to_string(),
                trigger_timeframe: "15m".to_string(),
                fast_ema: 20,
                slow_ema: 50,
                atr_period: 14,
                adx_period: 14,
                adx_threshold: 25.0,
                rsi_period: 14,
                rsi_overbought: 70.0,
                rsi_oversold: 30.0,
                macd_fast: 12,
                macd_slow: 26,
                macd_signal: 9,
                bb_period: 20,
                bb_std: 2.0,
            },

            signal_config: SignalConfig {
                breakout_lookback: 20,
                breakout_confirmation_bars: 2,
                pullback_ratio: 0.382,
                pullback_min_trend_strength: 40.0,
                momentum_threshold: 0.02,
                momentum_lookback: 10,
                pattern_min_bars: 5,
                pattern_confidence: 70.0,
                volume_multiplier: 1.5,
                volume_lookback: 20,
                signal_expiry: 300, // 5分钟
            },

            position_config: PositionConfig {
                base_risk_ratio: 0.01,
                kelly_fraction: 0.25,
                pyramid_enabled: true,
                pyramid_levels: vec![
                    PyramidLevel {
                        trigger_profit: 0.5,
                        size_ratio: 0.3,
                    },
                    PyramidLevel {
                        trigger_profit: 1.0,
                        size_ratio: 0.3,
                    },
                    PyramidLevel {
                        trigger_profit: 2.0,
                        size_ratio: 0.2,
                    },
                ],
                trend_factor_enabled: true,
                volatility_factor_enabled: true,
                time_factor_enabled: true,
                max_holding_hours: 24,
            },

            stop_config: StopConfig {
                initial_stop_type: StopType::ATR,
                atr_multiplier: 2.0,
                fixed_stop_percent: 0.02,
                trailing_stop_enabled: true,
                trailing_activation: 1.0,
                trailing_distance: 0.5,
                trailing_step: 0.5,
                time_stop_hours: Some(24),
                breakeven_enabled: true,
                breakeven_trigger: 0.5,
                partial_targets: vec![
                    PartialTarget {
                        target_r: 1.0,
                        close_ratio: 0.3,
                    },
                    PartialTarget {
                        target_r: 2.0,
                        close_ratio: 0.3,
                    },
                    PartialTarget {
                        target_r: 3.0,
                        close_ratio: 0.2,
                    },
                ],
            },
        }
    }
}
