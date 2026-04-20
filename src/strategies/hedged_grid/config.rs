use serde::{Deserialize, Serialize};

/// 策略基础信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyInfo {
    pub name: String,
    pub version: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_strategy_type")]
    pub strategy_type: String,
    #[serde(default = "default_market_type")]
    pub market_type: String,
}

/// 全局账户配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaseAccountConfig {
    pub account_id: String,
    #[serde(default = "default_market_type")]
    pub market_type: String,
}

/// 网格精度配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrecisionConfig {
    pub price_step: f64,
    pub amount_step: f64,
    #[serde(default)]
    pub min_notional: Option<f64>,
    #[serde(default)]
    pub price_digits: Option<u32>,
    #[serde(default)]
    pub amount_digits: Option<u32>,
}

/// 网格配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GridConfig {
    /// 网格间距（价差）
    pub spacing: f64,
    /// 网格间距模式：绝对价差或百分比
    #[serde(default = "default_spacing_mode")]
    pub spacing_mode: SpacingMode,
    /// 单笔下单数量（基础币数量）
    pub order_size: f64,
    /// 每侧网格层数
    #[serde(default = "default_levels_per_side")]
    pub levels_per_side: u32,
    /// 网格刷新间隔（秒）
    #[serde(default = "default_refresh_interval")]
    pub refresh_interval_secs: u64,
}

impl GridConfig {
    pub fn spacing_mode(&self) -> SpacingMode {
        self.spacing_mode
    }
}

/// 网格间距模式
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SpacingMode {
    Absolute,
    Percentage,
}

impl Default for SpacingMode {
    fn default() -> Self {
        SpacingMode::Absolute
    }
}

/// 底仓方向
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SymbolDirection {
    Long,
    Short,
}

impl Default for SymbolDirection {
    fn default() -> Self {
        SymbolDirection::Long
    }
}

/// 单个交易对配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolConfig {
    pub config_id: String,
    pub symbol: String,
    #[serde(default)]
    pub direction: SymbolDirection,
    #[serde(default)]
    pub account_id: Option<String>,
    /// 目标底仓（正值代表多仓，负值代表空仓）
    #[serde(default)]
    pub target_position: f64,
    /// 最低库存占目标底仓比例（0-1），用于触发补仓护栏
    #[serde(default)]
    pub min_inventory_ratio: Option<f64>,
    /// 最低库存（按报价货币计价）
    #[serde(default)]
    pub min_inventory_quote: Option<f64>,
    /// 最大允许绝对仓位
    #[serde(default = "default_position_limit")]
    pub position_limit: f64,
    /// 最大允许仓位（按报价货币计价）
    #[serde(default)]
    pub position_limit_quote: Option<f64>,
    #[serde(default = "default_enable_flag")]
    pub enabled: bool,
    pub grid: GridConfig,
    pub precision: PrecisionConfig,
}

/// 对冲矩阵配置（预留）
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HedgeMatrixConfig {
    #[serde(default)]
    pub correlation_window: Option<u64>,
    #[serde(default)]
    pub min_correlation: Option<f64>,
    #[serde(default)]
    pub rebalance_interval: Option<u64>,
}

/// 风控配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskControlConfig {
    #[serde(default = "default_max_leverage")]
    pub max_leverage: f64,
    #[serde(default = "default_drawdown_pct")]
    pub max_drawdown_pct: f64,
    #[serde(default = "default_daily_loss")]
    pub max_daily_loss: f64,
    #[serde(default)]
    pub emergency_stop_trigger: Option<f64>,
}

/// 执行配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    #[serde(default = "bool_true")]
    pub startup_cancel_all: bool,
    #[serde(default = "bool_true")]
    pub shutdown_cancel_all: bool,
    #[serde(default = "default_rebalance_interval")]
    pub rebalance_interval_secs: u64,
    #[serde(default = "default_taker_reset_delay")]
    pub taker_reset_delay_secs: u64,
    #[serde(default)]
    pub process_per_symbol: bool,
}

/// 日志配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default = "default_log_file")]
    pub file: String,
    #[serde(default = "bool_true")]
    pub console: bool,
}

/// 对冲网格策略配置总览
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HedgedGridConfig {
    pub strategy: StrategyInfo,
    pub base_account: BaseAccountConfig,
    pub symbols: Vec<SymbolConfig>,
    #[serde(default)]
    pub hedge_matrix: Option<HedgeMatrixConfig>,
    #[serde(default = "default_risk_control")]
    pub risk_control: RiskControlConfig,
    #[serde(default = "default_execution")]
    pub execution: ExecutionConfig,
    #[serde(default = "default_logging")]
    pub logging: LoggingConfig,
}

impl HedgedGridConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !self.strategy.enabled {
            return Err("策略未启用".to_string());
        }
        if self.symbols.is_empty() {
            return Err("至少配置一个交易对".to_string());
        }
        for symbol in &self.symbols {
            if symbol.grid.spacing <= 0.0 {
                return Err(format!("{} spacing 必须大于0", symbol.config_id));
            }
            if symbol.grid.order_size <= 0.0 {
                return Err(format!("{} order_size 必须大于0", symbol.config_id));
            }
            match symbol.grid.spacing_mode {
                SpacingMode::Absolute | SpacingMode::Percentage => {}
            }
            if symbol.grid.levels_per_side == 0 {
                return Err(format!("{} levels_per_side 必须大于0", symbol.config_id));
            }
            if symbol.position_limit <= 0.0 {
                return Err(format!("{} position_limit 必须大于0", symbol.config_id));
            }
            if let Some(min_quote) = symbol.min_inventory_quote {
                if min_quote < 0.0 {
                    return Err(format!(
                        "{} min_inventory_quote 不能为负数",
                        symbol.config_id
                    ));
                }
            }
            if let Some(limit_quote) = symbol.position_limit_quote {
                if limit_quote <= 0.0 {
                    return Err(format!(
                        "{} position_limit_quote 必须大于0",
                        symbol.config_id
                    ));
                }
            }
            if let Some(ratio) = symbol.min_inventory_ratio {
                if !(0.0..=1.0).contains(&ratio) {
                    return Err(format!(
                        "{} min_inventory_ratio 必须在0到1之间",
                        symbol.config_id
                    ));
                }
            }
        }
        Ok(())
    }
}

fn default_enabled() -> bool {
    true
}

fn default_strategy_type() -> String {
    "HedgedGrid".to_string()
}

fn default_market_type() -> String {
    "Futures".to_string()
}

fn default_levels_per_side() -> u32 {
    2
}

fn default_refresh_interval() -> u64 {
    60
}

fn default_spacing_mode() -> SpacingMode {
    SpacingMode::Absolute
}

fn default_position_limit() -> f64 {
    1.0
}

fn default_max_leverage() -> f64 {
    2.0
}

fn default_drawdown_pct() -> f64 {
    0.12
}

fn default_daily_loss() -> f64 {
    100.0
}

fn bool_true() -> bool {
    true
}

fn default_rebalance_interval() -> u64 {
    120
}

fn default_taker_reset_delay() -> u64 {
    3
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_file() -> String {
    "logs/strategies/hedged_grid.log".to_string()
}

fn default_risk_control() -> RiskControlConfig {
    RiskControlConfig {
        max_leverage: default_max_leverage(),
        max_drawdown_pct: default_drawdown_pct(),
        max_daily_loss: default_daily_loss(),
        emergency_stop_trigger: None,
    }
}

fn default_execution() -> ExecutionConfig {
    ExecutionConfig {
        startup_cancel_all: true,
        shutdown_cancel_all: true,
        rebalance_interval_secs: default_rebalance_interval(),
        taker_reset_delay_secs: default_taker_reset_delay(),
        process_per_symbol: false,
    }
}

fn default_logging() -> LoggingConfig {
    LoggingConfig {
        level: default_log_level(),
        file: default_log_file(),
        console: true,
    }
}

fn default_enable_flag() -> bool {
    true
}
