use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct AccountConfig {
    pub api_key: String,
    pub secret_key: String,
    pub enabled: Option<bool>, // 账户启用开关
}

#[derive(Debug, Deserialize)]
pub struct ExchangeConfig {
    pub enabled: Option<bool>, // 交易所启用开关
    pub accounts: HashMap<String, AccountConfig>,
}

#[derive(Debug, Deserialize)]
pub struct ApiConfig {
    pub binance: ExchangeConfig,
}
