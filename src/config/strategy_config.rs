use crate::utils::symbol::Symbol;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct StrategyConfig {
    pub strategies: Vec<Strategy>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Strategy {
    pub name: String,
    pub enabled: bool,
    pub log_level: Option<String>,
    pub exchange: String, // 绑定的交易所账户名称
    pub class_path: String,
    pub params: StrategyParams,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StrategyParams {
    pub symbol: String,
    pub spacing: f64,
    pub order_value: f64,
    pub grid_levels: i32,
    pub leverage: Option<i32>,
    pub health_check_interval_seconds: Option<u64>,
    pub uniformity_threshold: Option<f64>,
}

// 保留旧的配置结构以兼容性
#[derive(Debug, Deserialize, Clone)]
pub struct GridStrategyConfig {
    pub grid_configs: Vec<GridConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GridConfig {
    pub symbol: Symbol,
    pub grid_spacing: f64,       // For arithmetic grid
    pub grid_ratio: Option<f64>, // For geometric grid
    pub grid_num: i32,
    pub order_value: f64,
}
