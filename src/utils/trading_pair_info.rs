/// 交易对信息模块
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// 交易对信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingPairInfo {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price_precision: u32,
    pub quantity_precision: u32,
    pub min_quantity: f64,
    pub max_quantity: f64,
    pub min_notional: Option<f64>,
    pub step_size: f64,
    pub tick_size: f64,
}

impl TradingPairInfo {
    /// 从文件加载交易对信息
    pub fn load_from_file(
        exchange: &str,
        symbol: &str,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // 尝试从配置文件加载
        let config_path = format!(
            "config/trading_pairs/{}/{}.json",
            exchange,
            symbol.replace("/", "_")
        );

        if Path::new(&config_path).exists() {
            let content = fs::read_to_string(&config_path)?;
            let info: TradingPairInfo = serde_json::from_str(&content)?;
            return Ok(info);
        }

        // 如果文件不存在，使用默认值
        Ok(Self::default_for_symbol(symbol))
    }

    /// 根据交易对生成默认信息
    pub fn default_for_symbol(symbol: &str) -> Self {
        let parts: Vec<&str> = symbol.split('/').collect();
        let base_asset = parts.get(0).unwrap_or(&"").to_string();
        let quote_asset = parts.get(1).unwrap_or(&"USDT").to_string();

        // 根据基础资产类型设置默认精度
        let (price_precision, quantity_precision, min_notional) = match base_asset.as_str() {
            "BTC" => (2, 5, Some(10.0)),
            "ETH" => (2, 4, Some(10.0)),
            "BNB" => (2, 3, Some(10.0)),
            "SOL" => (2, 2, Some(10.0)),
            "XRP" | "DOGE" => (4, 0, Some(10.0)),
            "LINK" | "UNI" | "AAVE" => (3, 2, Some(10.0)),
            _ => (4, 2, Some(10.0)), // 默认值
        };

        let step_size = 10_f64.powi(-(quantity_precision as i32));
        let tick_size = 10_f64.powi(-(price_precision as i32));

        Self {
            symbol: symbol.to_string(),
            base_asset,
            quote_asset,
            price_precision,
            quantity_precision,
            min_quantity: step_size,
            max_quantity: 1000000.0,
            min_notional,
            step_size,
            tick_size,
        }
    }

    /// 保存到文件
    pub fn save_to_file(&self, exchange: &str) -> Result<(), Box<dyn std::error::Error>> {
        let dir_path = format!("config/trading_pairs/{}", exchange);
        fs::create_dir_all(&dir_path)?;

        let file_path = format!("{}/{}.json", dir_path, self.symbol.replace("/", "_"));
        let content = serde_json::to_string_pretty(self)?;
        fs::write(file_path, content)?;

        Ok(())
    }
}

/// 交易对信息管理器
pub struct TradingPairManager {
    cache: HashMap<String, TradingPairInfo>,
}

impl TradingPairManager {
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }

    /// 获取交易对信息
    pub fn get_pair_info(&mut self, exchange: &str, symbol: &str) -> TradingPairInfo {
        let key = format!("{}:{}", exchange, symbol);

        if let Some(info) = self.cache.get(&key) {
            return info.clone();
        }

        let info = TradingPairInfo::load_from_file(exchange, symbol)
            .unwrap_or_else(|_| TradingPairInfo::default_for_symbol(symbol));

        self.cache.insert(key, info.clone());
        info
    }
}
