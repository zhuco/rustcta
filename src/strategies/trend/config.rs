//! 趋势策略配置模块

use crate::core::error::ExchangeError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 趋势策略主配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TrendConfig {
    /// 策略名称
    pub name: String,

    /// 账户ID
    pub account_id: String,

    /// 是否强制启用双向持仓（Hedge Mode）
    /// - true: 直接按双向模式下单（携带 positionSide）
    /// - false: 由交易所自动检测（Binance）决定
    #[serde(default)]
    pub dual_position_mode: bool,

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

    /// 评分配置
    #[serde(default)]
    pub scoring: ScoringConfig,

    /// 行情状态配置
    #[serde(default)]
    pub regime: RegimeConfig,

    /// 日内时间风险曲线
    #[serde(default)]
    pub time_curve: TimeCurveConfig,

    /// 风险预算配置
    #[serde(default)]
    pub risk_budget: RiskBudgetConfig,

    /// 交易对库存上限
    #[serde(default)]
    pub symbol_inventory_limits: HashMap<String, f64>,

    /// 交易对按名义价值（USDT）限制库存
    #[serde(default)]
    pub symbol_inventory_value_limits: HashMap<String, f64>,

    /// 执行配置
    #[serde(default)]
    pub execution: ExecutionConfig,

    /// 监控配置
    #[serde(default)]
    pub monitoring: MonitoringConfig,

    /// 入场杠杆倍数
    #[serde(default = "default_entry_leverage")]
    pub entry_leverage: f64,

    /// 默认入场资金分配
    #[serde(default)]
    pub entry_allocation: EntryAllocation,

    /// 行情状态自适应分配
    #[serde(default)]
    pub allocation_regimes: AllocationRegimesConfig,

    /// 行情数据配置
    #[serde(default)]
    pub market_data: MarketDataConfig,

    /// 每次开仓的固定名义（USDT），优先级高于杠杆计算
    #[serde(default)]
    pub entry_base_notional: Option<f64>,

    /// 按交易对覆盖每次开仓固定名义（USDT）
    #[serde(default)]
    pub symbol_entry_notional: HashMap<String, f64>,

    /// 趋势策略总名义上限倍数（相对账户权益），用于硬限制新开仓
    #[serde(default)]
    pub max_trend_notional_multiplier: Option<f64>,

    /// 突破类信号是否使用市价单，回调类信号仍使用限价单
    #[serde(default)]
    pub market_entry_on_breakout: bool,

    /// Maker 订单统一的价格改善幅度（bps）
    #[serde(default = "default_entry_price_improve_bps")]
    pub entry_price_improve_bps: f64,

    /// 轮询间隔（秒）
    #[serde(default = "default_poll_interval_secs")]
    pub poll_interval_secs: u64,

    /// 信号冷却时间（秒）
    #[serde(default = "default_signal_cooldown_secs")]
    pub signal_cooldown_secs: u64,

    /// 余额/持仓推送配置
    #[serde(default)]
    pub status_reporter: StatusReportConfig,
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

    /// PnL驱动的移动止损
    #[serde(default)]
    pub pnl_trailing: PnlTrailingConfig,

    /// 浮盈锁定阈值（例如0.5%）
    #[serde(default = "default_lock_profit_pct")]
    pub lock_profit_pct: f64,

    /// 锁盈后的缓冲比例
    #[serde(default = "default_lock_buffer_pct")]
    pub lock_buffer_pct: f64,
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

/// PnL驱动的止损上移配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PnlTrailingConfig {
    #[serde(default)]
    pub enable: bool,
    #[serde(default = "default_one_f64")]
    pub atr_multiple: f64,
    #[serde(default = "default_trailing_levels")]
    pub lock_levels: Vec<PnlLockLevel>,
}

impl Default for PnlTrailingConfig {
    fn default() -> Self {
        Self {
            enable: true,
            atr_multiple: default_one_f64(),
            lock_levels: default_trailing_levels(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PnlLockLevel {
    pub profit_atr: f64,
    pub stop_offset_atr: f64,
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

        if self.risk_config.max_leverage > 5.0 {
            return Err(ExchangeError::Other("最大杠杆不能超过5倍".to_string()));
        }

        // 仓位参数验证
        if self.risk_config.max_total_exposure > 5.0 {
            return Err(ExchangeError::Other("总仓位不能超过账户5倍".to_string()));
        }

        if self.poll_interval_secs == 0 {
            return Err(ExchangeError::Other("轮询间隔必须大于0秒".to_string()));
        }

        if self.signal_cooldown_secs == 0 {
            return Err(ExchangeError::Other("信号冷却时间必须大于0秒".to_string()));
        }

        if self.execution.max_retries == 0 {
            return Err(ExchangeError::Other(
                "执行配置中的最大重试次数必须大于0".to_string(),
            ));
        }

        if self.market_data.cache_depth == 0
            || self.market_data.bootstrap_bars == 0
            || self.market_data.min_one_minute_bars == 0
            || self.market_data.min_one_hour_bars == 0
        {
            return Err(ExchangeError::Other("行情缓存配置必须为正数".to_string()));
        }

        if self.entry_leverage <= 0.0 || self.entry_leverage > self.risk_config.max_leverage {
            return Err(ExchangeError::Other(
                "入场杠杆必须在(0, max_leverage]范围内".to_string(),
            ));
        }

        if let Some(notional) = self.entry_base_notional {
            if notional <= 0.0 {
                return Err(ExchangeError::Other(
                    "entry_base_notional 必须大于0".to_string(),
                ));
            }
        }

        for (symbol, notional) in &self.symbol_entry_notional {
            if *notional <= 0.0 {
                return Err(ExchangeError::Other(format!(
                    "{} 的 symbol_entry_notional 必须大于0",
                    symbol
                )));
            }
        }

        if let Some(multiplier) = self.max_trend_notional_multiplier {
            if multiplier <= 0.0 || multiplier > 5.0 {
                return Err(ExchangeError::Other(
                    "max_trend_notional_multiplier 必须在(0, 5]范围内".to_string(),
                ));
            }
        }

        if self.entry_price_improve_bps < 0.0 {
            return Err(ExchangeError::Other(
                "entry_price_improve_bps 不能为负".to_string(),
            ));
        }

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
            dual_position_mode: false,
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
                max_consecutive_losses: 5,
                max_total_exposure: 0.5,  // 50%
                max_single_exposure: 0.1, // 10%
                max_correlated_exposure: 0.2,
                max_leverage: 3.0,
                max_slippage: 0.002,      // 0.2%
                emergency_stop_loss: 0.1, // 10%
            },

            indicator_config: IndicatorConfig {
                primary_timeframe: "15m".to_string(),
                secondary_timeframe: "5m".to_string(),
                trigger_timeframe: "1m".to_string(),
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
                pnl_trailing: PnlTrailingConfig::default(),
                lock_profit_pct: default_lock_profit_pct(),
                lock_buffer_pct: default_lock_buffer_pct(),
            },

            scoring: ScoringConfig::default(),
            regime: RegimeConfig::default(),
            time_curve: TimeCurveConfig::default(),
            risk_budget: RiskBudgetConfig::default(),
            execution: ExecutionConfig::default(),
            monitoring: MonitoringConfig::default(),
            entry_leverage: default_entry_leverage(),
            entry_allocation: EntryAllocation::default(),
            allocation_regimes: AllocationRegimesConfig::default(),
            market_data: MarketDataConfig::default(),
            entry_base_notional: None,
            symbol_entry_notional: HashMap::new(),
            max_trend_notional_multiplier: None,
            market_entry_on_breakout: false,
            entry_price_improve_bps: default_entry_price_improve_bps(),
            poll_interval_secs: default_poll_interval_secs(),
            signal_cooldown_secs: default_signal_cooldown_secs(),
            status_reporter: StatusReportConfig::default(),
            symbol_inventory_limits: HashMap::new(),
            symbol_inventory_value_limits: HashMap::new(),
        }
    }
}

fn default_poll_interval_secs() -> u64 {
    5
}

fn default_signal_cooldown_secs() -> u64 {
    60
}

fn default_entry_leverage() -> f64 {
    1.0
}

fn default_entry_price_improve_bps() -> f64 {
    15.0
}

fn default_lock_profit_pct() -> f64 {
    0.005
}

fn default_lock_buffer_pct() -> f64 {
    0.001
}

fn default_cache_depth() -> usize {
    720
}

fn default_bootstrap_bars() -> usize {
    600
}

fn default_max_data_lag_ms() -> u64 {
    200
}

fn default_min_1m_bars() -> usize {
    240
}

fn default_min_5m_bars() -> usize {
    120
}

fn default_min_15m_bars() -> usize {
    64
}

fn default_min_1h_bars() -> usize {
    64
}

fn default_one_f64() -> f64 {
    1.0
}

fn default_trailing_levels() -> Vec<PnlLockLevel> {
    vec![
        PnlLockLevel {
            profit_atr: 1.5,
            stop_offset_atr: 0.5,
        },
        PnlLockLevel {
            profit_atr: 2.5,
            stop_offset_atr: 0.2,
        },
    ]
}

/// 评分配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ScoringConfig {
    #[serde(default = "ScoringConfig::default_primary_weight")]
    pub primary_weight: f64,
    #[serde(default = "ScoringConfig::default_secondary_weight")]
    pub secondary_weight: f64,
    #[serde(default = "ScoringConfig::default_trigger_weight")]
    pub trigger_weight: f64,
    #[serde(default = "ScoringConfig::default_strong_threshold")]
    pub strong_threshold: f64,
    #[serde(default = "ScoringConfig::default_trade_threshold")]
    pub trade_threshold: f64,
    #[serde(default)]
    pub degrade_on_poor_data: bool,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            primary_weight: Self::default_primary_weight(),
            secondary_weight: Self::default_secondary_weight(),
            trigger_weight: Self::default_trigger_weight(),
            strong_threshold: Self::default_strong_threshold(),
            trade_threshold: Self::default_trade_threshold(),
            degrade_on_poor_data: true,
        }
    }
}

impl ScoringConfig {
    fn default_primary_weight() -> f64 {
        0.4
    }
    fn default_secondary_weight() -> f64 {
        0.35
    }
    fn default_trigger_weight() -> f64 {
        0.25
    }
    fn default_strong_threshold() -> f64 {
        70.0
    }
    fn default_trade_threshold() -> f64 {
        55.0
    }
}

/// 行情状态配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegimeConfig {
    #[serde(default = "RegimeConfig::default_trending_threshold")]
    pub trending_threshold: f64,
    #[serde(default = "RegimeConfig::default_ranging_threshold")]
    pub ranging_threshold: f64,
    #[serde(default = "RegimeConfig::default_volatility_threshold")]
    pub volatility_threshold: f64,
    #[serde(default = "RegimeConfig::default_extreme_threshold")]
    pub extreme_threshold: f64,
}

impl Default for RegimeConfig {
    fn default() -> Self {
        Self {
            trending_threshold: Self::default_trending_threshold(),
            ranging_threshold: Self::default_ranging_threshold(),
            volatility_threshold: Self::default_volatility_threshold(),
            extreme_threshold: Self::default_extreme_threshold(),
        }
    }
}

impl RegimeConfig {
    fn default_trending_threshold() -> f64 {
        35.0
    }
    fn default_ranging_threshold() -> f64 {
        15.0
    }
    fn default_volatility_threshold() -> f64 {
        0.035
    }
    fn default_extreme_threshold() -> f64 {
        0.08
    }
}

/// 日内时间曲线配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimeCurveConfig {
    #[serde(default = "TimeCurveConfig::default_segments")]
    pub segments: Vec<TimeCurveSegment>,
    #[serde(default = "TimeCurveConfig::default_weight")]
    pub default_weight: f64,
}

impl Default for TimeCurveConfig {
    fn default() -> Self {
        Self {
            segments: Self::default_segments(),
            default_weight: Self::default_weight(),
        }
    }
}

impl TimeCurveConfig {
    fn default_weight() -> f64 {
        1.0
    }

    fn default_segments() -> Vec<TimeCurveSegment> {
        vec![
            TimeCurveSegment {
                start_hour: 0,
                end_hour: 8,
                weight: 0.8,
            },
            TimeCurveSegment {
                start_hour: 8,
                end_hour: 16,
                weight: 1.0,
            },
            TimeCurveSegment {
                start_hour: 16,
                end_hour: 24,
                weight: 0.9,
            },
        ]
    }
}

/// 时间曲线片段
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimeCurveSegment {
    pub start_hour: u8,
    pub end_hour: u8,
    pub weight: f64,
}

/// 风险预算配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RiskBudgetConfig {
    #[serde(default = "RiskBudgetConfig::default_portfolio_var")]
    pub max_portfolio_var: f64,
    #[serde(default = "RiskBudgetConfig::default_symbol_var")]
    pub max_symbol_var: f64,
    #[serde(default = "RiskBudgetConfig::default_drawdown")]
    pub max_drawdown_pct: f64,
    #[serde(default = "RiskBudgetConfig::default_panic_exit")]
    pub panic_exit_drawdown: f64,
    #[serde(default = "RiskBudgetConfig::default_consecutive_losses")]
    pub pause_after_consecutive_losses: usize,
}

impl Default for RiskBudgetConfig {
    fn default() -> Self {
        Self {
            max_portfolio_var: Self::default_portfolio_var(),
            max_symbol_var: Self::default_symbol_var(),
            max_drawdown_pct: Self::default_drawdown(),
            panic_exit_drawdown: Self::default_panic_exit(),
            pause_after_consecutive_losses: Self::default_consecutive_losses(),
        }
    }
}

impl RiskBudgetConfig {
    fn default_portfolio_var() -> f64 {
        0.04
    }
    fn default_symbol_var() -> f64 {
        0.02
    }
    fn default_drawdown() -> f64 {
        0.12
    }
    fn default_panic_exit() -> f64 {
        0.2
    }
    fn default_consecutive_losses() -> usize {
        5
    }
}

/// 执行配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ExecutionConfig {
    #[serde(default = "ExecutionConfig::default_prefer_maker")]
    pub prefer_maker: bool,
    #[serde(default = "ExecutionConfig::default_allow_taker_fallback")]
    pub allow_taker_fallback: bool,
    #[serde(default = "ExecutionConfig::default_maker_offset_bps")]
    pub maker_offset_bps: f64,
    #[serde(default = "ExecutionConfig::default_taker_slippage_bps")]
    pub taker_slippage_bps: f64,
    #[serde(default = "ExecutionConfig::default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "ExecutionConfig::default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default = "ExecutionConfig::default_order_timeout_secs")]
    pub order_timeout_secs: u64,
    #[serde(default)]
    pub iceberg_enabled: bool,
    #[serde(default = "ExecutionConfig::default_iceberg_ratio")]
    pub iceberg_visible_ratio: f64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            prefer_maker: Self::default_prefer_maker(),
            allow_taker_fallback: Self::default_allow_taker_fallback(),
            maker_offset_bps: Self::default_maker_offset_bps(),
            taker_slippage_bps: Self::default_taker_slippage_bps(),
            max_retries: Self::default_max_retries(),
            retry_backoff_ms: Self::default_retry_backoff_ms(),
            order_timeout_secs: Self::default_order_timeout_secs(),
            iceberg_enabled: false,
            iceberg_visible_ratio: Self::default_iceberg_ratio(),
        }
    }
}

impl ExecutionConfig {
    fn default_prefer_maker() -> bool {
        true
    }
    fn default_allow_taker_fallback() -> bool {
        true
    }
    fn default_maker_offset_bps() -> f64 {
        2.0
    }
    fn default_taker_slippage_bps() -> f64 {
        5.0
    }
    fn default_max_retries() -> u32 {
        3
    }
    fn default_retry_backoff_ms() -> u64 {
        250
    }
    fn default_order_timeout_secs() -> u64 {
        10
    }
    fn default_iceberg_ratio() -> f64 {
        0.2
    }
}

/// 监控配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MonitoringConfig {
    #[serde(default = "MonitoringConfig::default_health_threshold")]
    pub health_threshold: f64,
    #[serde(default = "MonitoringConfig::default_warning_slippage_bps")]
    pub warning_slippage_bps: f64,
    #[serde(default = "MonitoringConfig::default_critical_slippage_bps")]
    pub critical_slippage_bps: f64,
    #[serde(default)]
    pub enable_webhook: bool,
    #[serde(default)]
    pub rotate_daily_logs: bool,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            health_threshold: Self::default_health_threshold(),
            warning_slippage_bps: Self::default_warning_slippage_bps(),
            critical_slippage_bps: Self::default_critical_slippage_bps(),
            enable_webhook: false,
            rotate_daily_logs: true,
        }
    }
}

impl MonitoringConfig {
    fn default_health_threshold() -> f64 {
        65.0
    }
    fn default_warning_slippage_bps() -> f64 {
        10.0
    }
    fn default_critical_slippage_bps() -> f64 {
        25.0
    }
}

/// 行情数据配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MarketDataConfig {
    #[serde(default = "default_cache_depth")]
    pub cache_depth: usize,
    #[serde(default = "default_bootstrap_bars")]
    pub bootstrap_bars: usize,
    #[serde(default = "default_max_data_lag_ms")]
    pub max_data_lag_ms: u64,
    #[serde(default = "default_min_1m_bars")]
    pub min_one_minute_bars: usize,
    #[serde(default = "default_min_5m_bars")]
    pub min_five_minute_bars: usize,
    #[serde(default = "default_min_15m_bars")]
    pub min_fifteen_minute_bars: usize,
    #[serde(default = "default_min_1h_bars")]
    pub min_one_hour_bars: usize,
}

/// 状态推送配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StatusReportConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub webhook_url: String,
    #[serde(default = "default_status_report_interval")]
    pub interval_secs: u64,
}

impl Default for StatusReportConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            webhook_url: String::new(),
            interval_secs: default_status_report_interval(),
        }
    }
}

const fn default_status_report_interval() -> u64 {
    300
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        Self {
            cache_depth: default_cache_depth(),
            bootstrap_bars: default_bootstrap_bars(),
            max_data_lag_ms: default_max_data_lag_ms(),
            min_one_minute_bars: default_min_1m_bars(),
            min_five_minute_bars: default_min_5m_bars(),
            min_fifteen_minute_bars: default_min_15m_bars(),
            min_one_hour_bars: default_min_1h_bars(),
        }
    }
}

/// 入场权重配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EntryAllocation {
    pub ma: f64,
    pub fib: f64,
    pub bb: f64,
}

impl EntryAllocation {
    pub fn normalized(&self) -> Self {
        let sum = (self.ma + self.fib + self.bb).max(1e-6);
        Self {
            ma: (self.ma / sum).max(0.0),
            fib: (self.fib / sum).max(0.0),
            bb: (self.bb / sum).max(0.0),
        }
    }

    pub fn trending_default() -> Self {
        Self {
            ma: 0.6,
            fib: 0.25,
            bb: 0.15,
        }
    }

    pub fn ranging_default() -> Self {
        Self {
            ma: 0.25,
            fib: 0.35,
            bb: 0.40,
        }
    }

    pub fn extreme_default() -> Self {
        Self {
            ma: 0.4,
            fib: 0.4,
            bb: 0.2,
        }
    }
}

impl Default for EntryAllocation {
    fn default() -> Self {
        Self {
            ma: 0.4,
            fib: 0.3,
            bb: 0.3,
        }
    }
}

/// 行情状态分配配置
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AllocationRegimesConfig {
    #[serde(default = "EntryAllocation::trending_default")]
    pub trending: EntryAllocation,
    #[serde(default = "EntryAllocation::ranging_default")]
    pub ranging: EntryAllocation,
    #[serde(default = "EntryAllocation::extreme_default")]
    pub extreme: EntryAllocation,
}

impl Default for AllocationRegimesConfig {
    fn default() -> Self {
        Self {
            trending: EntryAllocation::trending_default(),
            ranging: EntryAllocation::ranging_default(),
            extreme: EntryAllocation::extreme_default(),
        }
    }
}
