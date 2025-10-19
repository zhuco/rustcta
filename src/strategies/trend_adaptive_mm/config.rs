use serde::{Deserialize, Serialize};

/// 趋势自适应做市策略总体配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrendAdaptiveMMConfig {
    pub strategy: StrategyConfig,
    pub trading: TradingConfig,
    pub signal: SignalConfig,
    pub risk: RiskConfig,
    #[serde(default)]
    pub execution: ExecutionConfig,
}

/// 策略相关元信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    pub name: String,
    pub account_id: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_log_level() -> String {
    "INFO".to_string()
}

/// 交易与报价值参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    /// 交易对，支持 `ETHUSDC` 或 `ETH/USDC` 格式
    pub symbol: String,
    /// 市场类型（spot / futures）
    #[serde(default = "default_market_type")]
    pub market_type: String,
    /// 基础挂单数量
    pub base_order_size: f64,
    /// 最大库存绝对值
    pub max_inventory: f64,
    /// 基础点差（bps）
    #[serde(default = "default_base_spread_bps")]
    pub base_spread_bps: f64,
    /// 点差最小值（bps）
    #[serde(default = "default_min_spread_bps")]
    pub min_spread_bps: f64,
    /// 点差最大值（bps）
    #[serde(default = "default_max_spread_bps")]
    pub max_spread_bps: f64,
    /// 根据信号偏移价差的最大幅度（bps）
    #[serde(default = "default_skew_bps")]
    pub skew_bps: f64,
    /// 用于波动调整的参考波动率（百分比）
    #[serde(default = "default_reference_volatility")]
    pub reference_volatility: f64,
    /// 波动率对点差的放大系数
    #[serde(default = "default_volatility_spread_k")]
    pub volatility_spread_k: f64,
    /// 最大库存偏置（单位：标的数量）
    #[serde(default = "default_inventory_bias_max")]
    pub inventory_bias_max: f64,
    /// 订单最小名义金额（若优先级高于交易所返回的限制）
    #[serde(default)]
    pub min_notional_override: Option<f64>,
    /// 挂单层数
    #[serde(default = "default_order_levels")]
    pub order_levels: u32,
    /// 层间点差增量（bps）
    #[serde(default = "default_level_spacing_bps")]
    pub level_spacing_bps: f64,
    /// 每层数量缩放比例
    #[serde(default = "default_level_size_scale")]
    pub level_size_scale: f64,
}

fn default_market_type() -> String {
    "spot".to_string()
}

fn default_base_spread_bps() -> f64 {
    6.0
}

fn default_min_spread_bps() -> f64 {
    3.0
}

fn default_max_spread_bps() -> f64 {
    30.0
}

fn default_skew_bps() -> f64 {
    10.0
}

fn default_reference_volatility() -> f64 {
    1.0
}

fn default_volatility_spread_k() -> f64 {
    2.0
}

fn default_inventory_bias_max() -> f64 {
    1.5
}

fn default_order_levels() -> u32 {
    1
}

fn default_level_spacing_bps() -> f64 {
    2.0
}

fn default_level_size_scale() -> f64 {
    1.0
}

/// 信号配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalConfig {
    /// 快速 EMA 周期
    #[serde(default = "default_fast_ema_period")]
    pub fast_ema_period: usize,
    /// 慢速 EMA 周期
    #[serde(default = "default_slow_ema_period")]
    pub slow_ema_period: usize,
    /// 订单流指标窗口
    #[serde(default = "default_ofi_window")]
    pub ofi_window: usize,
    /// 波动率窗口
    #[serde(default = "default_volatility_window")]
    pub volatility_window: usize,
    /// 趋势阈值
    #[serde(default = "default_trend_threshold")]
    pub trend_threshold: f64,
    /// 信号守门阈值
    #[serde(default = "default_signal_floor")]
    pub signal_floor: f64,
    /// 均线差分权重
    #[serde(default = "default_ema_weight")]
    pub ema_weight: f64,
    /// 动量权重
    #[serde(default = "default_momentum_weight")]
    pub momentum_weight: f64,
    /// 订单流权重
    #[serde(default = "default_ofi_weight")]
    pub ofi_weight: f64,
    /// 波动率上限（高于此进入保护）
    #[serde(default = "default_volatility_cutoff")]
    pub volatility_cutoff: f64,
    /// 信号冷却时间（毫秒）
    #[serde(default = "default_signal_cooldown_ms")]
    pub signal_cooldown_ms: u64,
}

fn default_fast_ema_period() -> usize {
    21
}

fn default_slow_ema_period() -> usize {
    55
}

fn default_ofi_window() -> usize {
    20
}

fn default_volatility_window() -> usize {
    120
}

fn default_trend_threshold() -> f64 {
    0.35
}

fn default_signal_floor() -> f64 {
    0.1
}

fn default_ema_weight() -> f64 {
    0.55
}

fn default_momentum_weight() -> f64 {
    0.3
}

fn default_ofi_weight() -> f64 {
    0.15
}

fn default_volatility_cutoff() -> f64 {
    4.0
}

fn default_signal_cooldown_ms() -> u64 {
    2_000
}

/// 风险管理配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    /// 软库存限制
    #[serde(default = "default_inventory_soft_limit")]
    pub inventory_soft_limit: f64,
    /// 硬库存限制
    #[serde(default = "default_inventory_hard_limit")]
    pub inventory_hard_limit: f64,
    /// 最大日内回撤（百分比）
    #[serde(default = "default_max_daily_drawdown")]
    pub max_daily_drawdown_pct: f64,
    /// 最大单日亏损（货币）
    #[serde(default)]
    pub max_daily_loss_abs: Option<f64>,
    /// 最小信号置信阈值（与 signal_floor 对齐）
    #[serde(default = "default_signal_floor")]
    pub signal_floor: f64,
    /// 风险降级持续时间（秒）
    #[serde(default = "default_downgrade_secs")]
    pub downgrade_cooldown_secs: u64,
    /// Kill-switch 冷却时间（秒）
    #[serde(default = "default_stop_cooldown_secs")]
    pub stop_cooldown_secs: u64,
}

fn default_inventory_soft_limit() -> f64 {
    1.0
}

fn default_inventory_hard_limit() -> f64 {
    2.0
}

fn default_max_daily_drawdown() -> f64 {
    3.5
}

fn default_downgrade_secs() -> u64 {
    60
}

fn default_stop_cooldown_secs() -> u64 {
    600
}

/// 执行层配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    /// 订单排队容忍比例（0-1）
    #[serde(default = "default_queue_threshold")]
    pub queue_threshold: f64,
    /// 挂单最大存活时间（毫秒）
    #[serde(default = "default_max_time_in_queue_ms")]
    pub max_time_in_queue_ms: u64,
    /// maker-only 调价最小偏移 tick 数
    #[serde(default = "default_maker_offset_ticks")]
    pub maker_offset_ticks: u64,
    /// 每分钟最大重报价次数（每侧）
    #[serde(default = "default_max_requote_per_minute")]
    pub max_requote_per_minute: u32,
    /// 是否在零手续费模式下放宽重报阈值
    #[serde(default)]
    pub zero_fee_mode: bool,
    /// 市场行情刷新间隔（毫秒，用于降频本地处理）
    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: u64,
    /// 订单状态检查间隔（毫秒）
    #[serde(default = "default_order_check_interval_ms")]
    pub order_check_interval_ms: u64,
    /// ListenKey 保活间隔（秒）
    #[serde(default = "default_listen_key_keepalive_secs")]
    pub listen_key_keepalive_secs: u64,
    /// 库存同步间隔（秒）
    #[serde(default = "default_inventory_sync_secs")]
    pub inventory_sync_secs: u64,
    /// post-only 被拒后每次额外偏移的 tick 数
    #[serde(default = "default_post_only_retry_step")]
    pub post_only_retry_step: u32,
    /// post-only 额外偏移的最大 tick 数
    #[serde(default = "default_post_only_retry_max")]
    pub post_only_retry_max: u32,
    /// post-only 偏移降级冷却（毫秒）
    #[serde(default = "default_post_only_retry_cooldown_ms")]
    pub post_only_retry_cooldown_ms: u64,
    /// 同价重报冷却（毫秒）
    #[serde(default = "default_order_refresh_cooldown_ms")]
    pub order_refresh_cooldown_ms: u64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            queue_threshold: default_queue_threshold(),
            max_time_in_queue_ms: default_max_time_in_queue_ms(),
            maker_offset_ticks: default_maker_offset_ticks(),
            max_requote_per_minute: default_max_requote_per_minute(),
            zero_fee_mode: true,
            tick_interval_ms: default_tick_interval_ms(),
            order_check_interval_ms: default_order_check_interval_ms(),
            listen_key_keepalive_secs: default_listen_key_keepalive_secs(),
            inventory_sync_secs: default_inventory_sync_secs(),
            post_only_retry_step: default_post_only_retry_step(),
            post_only_retry_max: default_post_only_retry_max(),
            post_only_retry_cooldown_ms: default_post_only_retry_cooldown_ms(),
            order_refresh_cooldown_ms: default_order_refresh_cooldown_ms(),
        }
    }
}

fn default_queue_threshold() -> f64 {
    0.35
}

fn default_max_time_in_queue_ms() -> u64 {
    25_000
}

fn default_maker_offset_ticks() -> u64 {
    1
}

fn default_max_requote_per_minute() -> u32 {
    20
}

fn default_tick_interval_ms() -> u64 {
    500
}

fn default_order_check_interval_ms() -> u64 {
    800
}

fn default_listen_key_keepalive_secs() -> u64 {
    1500
}

fn default_inventory_sync_secs() -> u64 {
    180
}

fn default_post_only_retry_step() -> u32 {
    2
}

fn default_post_only_retry_max() -> u32 {
    6
}

fn default_post_only_retry_cooldown_ms() -> u64 {
    3_000
}

fn default_order_refresh_cooldown_ms() -> u64 {
    300
}
