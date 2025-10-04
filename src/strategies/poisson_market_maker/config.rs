use serde::{Deserialize, Serialize};

/// 泊松队列做市策略配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoissonMMConfig {
    /// 策略名称
    pub name: String,
    /// 是否启用
    pub enabled: bool,
    /// 版本
    pub version: String,

    /// 账户配置
    pub account: PoissonAccountConfig,

    /// 交易配置
    pub trading: PoissonTradingConfig,

    /// 泊松模型参数
    pub poisson: PoissonModelConfig,

    /// 风险管理
    pub risk: PoissonRiskConfig,
}

/// 账户配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoissonAccountConfig {
    pub account_id: String,
    pub exchange: String,
}

/// 交易配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoissonTradingConfig {
    /// 交易对
    pub symbol: String,
    /// 每单金额(USDC)
    pub order_size_usdc: f64,
    /// 最大库存(基础货币数量，如DOGE、LINK的数量)
    pub max_inventory: f64,
    /// 最小价差(基点bp)
    pub min_spread_bp: f64,
    /// 最大价差(基点bp)
    pub max_spread_bp: f64,
    /// 订单刷新间隔(秒)
    pub refresh_interval_secs: u64,
    /// 价格精度
    pub price_precision: usize,
    /// 数量精度
    pub quantity_precision: usize,
}

/// 泊松模型配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoissonModelConfig {
    /// 观察窗口(秒)
    pub observation_window_secs: u64,
    /// 最小样本数
    pub min_samples: usize,
    /// 平滑系数(EMA)
    pub smoothing_alpha: f64,
    /// 队列深度档位
    pub depth_levels: usize,
    /// 置信区间
    pub confidence_interval: f64,
    /// 初始lambda值（当没有足够样本时使用）
    pub initial_lambda: f64,
}

/// 风险配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoissonRiskConfig {
    /// 最大未实现亏损
    pub max_unrealized_loss: f64,
    /// 最大日亏损
    pub max_daily_loss: f64,
    /// 库存偏斜限制(0.5表示50%)
    pub inventory_skew_limit: f64,
    /// 止损价格偏离
    pub stop_loss_pct: f64,
}
