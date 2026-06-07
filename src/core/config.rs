use crate::core::error::ExchangeError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub name: String,
    pub testnet: bool,
    pub base_url: String,
    pub websocket_url: String,
    pub symbol_separator: String,
    pub symbol_format: String,
    pub rate_limits: RateLimits,
    pub endpoints: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimits {
    pub requests_per_minute: Option<u32>,
    pub requests_per_second: Option<u32>,
    pub orders_per_minute: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SymbolFormat {
    pub standard: String,
    pub conversion_rules: HashMap<String, HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub exchanges: HashMap<String, ExchangeConfig>,
    pub symbol_format: SymbolFormat,
}

impl GlobalConfig {
    /// 从YAML文件加载配置
    pub fn from_file(path: &str) -> Result<Self, ExchangeError> {
        let contents = fs::read_to_string(path)
            .map_err(|e| ExchangeError::ConfigError(format!("读取配置文件失败: {}", e)))?;

        let config: GlobalConfig = serde_yaml::from_str(&contents)?;
        Ok(config)
    }

    /// 获取指定交易所的配置
    pub fn get_exchange_config(&self, exchange: &str) -> Result<&ExchangeConfig, ExchangeError> {
        self.exchanges
            .get(exchange)
            .ok_or_else(|| ExchangeError::UnsupportedExchange(exchange.to_string()))
    }
}

impl Default for GlobalConfig {
    fn default() -> Self {
        Self {
            exchanges: HashMap::new(),
            symbol_format: SymbolFormat {
                standard: "BASE/QUOTE".to_string(),
                conversion_rules: HashMap::new(),
            },
        }
    }
}

/// Binance特定配置（用于兼容性）
#[derive(Debug, Clone)]
pub struct Config {
    pub name: String,
    pub testnet: bool,
    pub spot_base_url: String,
    pub futures_base_url: String,
    pub ws_spot_url: String,
    pub ws_futures_url: String,
}

impl Config {
    /// 获取交易所配置（为了兼容性）
    pub fn get_exchange_config(&self, _exchange: &str) -> Result<&ExchangeConfig, ExchangeError> {
        // 返回一个简单的默认配置
        Err(ExchangeError::ConfigError(
            "Config不包含exchange配置".to_string(),
        ))
    }

    /// 从文件读取配置
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        // 读取YAML文件
        let contents = std::fs::read_to_string(path)?;
        let exchange_config: ExchangeConfig = serde_yaml::from_str(&contents)?;
        Ok(Self::from_exchange_config(&exchange_config))
    }

    pub fn from_exchange_config(exchange_config: &ExchangeConfig) -> Self {
        let (spot_base_url, futures_base_url, ws_spot_url, ws_futures_url) =
            if exchange_config.testnet {
                (
                    "https://testnet.binance.vision".to_string(),
                    "https://testnet.binancefuture.com".to_string(),
                    "wss://testnet.binance.vision".to_string(),
                    "wss://stream.binancefuture.com".to_string(),
                )
            } else {
                (
                    "https://api.binance.com".to_string(),
                    "https://fapi.binance.com".to_string(),
                    "wss://stream.binance.com:9443".to_string(),
                    "wss://fstream.binance.com".to_string(),
                )
            };

        Self {
            name: exchange_config.name.clone(),
            testnet: exchange_config.testnet,
            spot_base_url,
            futures_base_url,
            ws_spot_url,
            ws_futures_url,
        }
    }
}

/// API密钥配置
#[derive(Debug, Clone)]
pub struct ApiKeys {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: Option<String>,
    pub memo: Option<String>,
}

impl ApiKeys {
    /// 从环境变量加载API密钥
    pub fn from_env(exchange: &str) -> Result<Self, ExchangeError> {
        load_dotenv_lenient(); // 加载.env文件；跳过非法行，避免一个坏行阻断后续密钥

        let exchange_upper = exchange.to_uppercase();

        let api_key = std::env::var(format!("{}_API_KEY", exchange_upper)).map_err(|_| {
            ExchangeError::ConfigError(format!("未找到{}的API_KEY环境变量", exchange))
        })?;

        // 尝试两种格式的密钥名称
        let api_secret = std::env::var(format!("{}_API_SECRET", exchange_upper))
            .or_else(|_| std::env::var(format!("{}_SECRET_KEY", exchange_upper)))
            .or_else(|_| std::env::var(format!("{}_SECRET", exchange_upper)))
            .map_err(|_| {
                ExchangeError::ConfigError(format!(
                    "未找到{}的API_SECRET或SECRET_KEY环境变量",
                    exchange
                ))
            })?;

        // 尝试多种格式的passphrase名称
        let passphrase = std::env::var(format!("{}_PASSPHRASE", exchange_upper))
            .or_else(|_| std::env::var(format!("{}_API_PASSWORD", exchange_upper)))
            .ok();

        // 尝试多种格式的memo名称
        let memo = std::env::var(format!("{}_MEMO", exchange_upper))
            .or_else(|_| std::env::var(format!("{}_API_MEMO", exchange_upper)))
            .ok();

        Ok(ApiKeys {
            api_key,
            api_secret,
            passphrase,
            memo,
        })
    }
}

fn load_dotenv_lenient() {
    dotenv::dotenv().ok();
    for path in [".env", "data/control_api/exchange_api_keys.env"] {
        load_dotenv_file_lenient(path);
    }
    for key in ["RUSTCTA_EXCHANGE_API_KEY_STORE", "EXCHANGE_API_KEY_STORE"] {
        if let Ok(path) = std::env::var(key) {
            load_dotenv_file_lenient(&path);
        }
    }
}

fn load_dotenv_file_lenient(path: &str) {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return;
    };

    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim().strip_prefix("export ").unwrap_or(key.trim());
        if key.is_empty()
            || key
                .chars()
                .any(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
        {
            continue;
        }
        if std::env::var_os(key).is_some() {
            continue;
        }
        std::env::set_var(key, parse_dotenv_value(value.trim()));
    }
}

fn parse_dotenv_value(value: &str) -> String {
    let value = value.trim();
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        let first = bytes[0];
        let last = bytes[value.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return value[1..value.len() - 1].to_string();
        }
    }
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn parse_dotenv_value_should_strip_matching_quotes() {
        assert_eq!(parse_dotenv_value("\"key\""), "key");
        assert_eq!(parse_dotenv_value("'secret'"), "secret");
        assert_eq!(parse_dotenv_value("plain"), "plain");
    }

    #[test]
    fn api_keys_from_env_should_load_valid_lines_after_malformed_dotenv_line() {
        let _guard = ENV_LOCK.lock().unwrap();
        let cwd = std::env::current_dir().unwrap();
        let temp_dir =
            std::env::temp_dir().join(format!("rustcta-dotenv-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&temp_dir);
        std::fs::create_dir_all(&temp_dir).unwrap();
        std::fs::write(
            temp_dir.join(".env"),
            "BROKEN_LINE\nBITGET_TEST_API_KEY=key\nBITGET_TEST_API_SECRET=secret\nBITGET_TEST_PASSPHRASE=pass\n",
        )
        .unwrap();

        std::env::remove_var("BITGET_TEST_API_KEY");
        std::env::remove_var("BITGET_TEST_API_SECRET");
        std::env::remove_var("BITGET_TEST_PASSPHRASE");
        std::env::set_current_dir(&temp_dir).unwrap();

        let keys = ApiKeys::from_env("bitget_test").unwrap();

        assert_eq!(keys.api_key, "key");
        assert_eq!(keys.api_secret, "secret");
        assert_eq!(keys.passphrase.as_deref(), Some("pass"));

        std::env::set_current_dir(cwd).unwrap();
        std::env::remove_var("BITGET_TEST_API_KEY");
        std::env::remove_var("BITGET_TEST_API_SECRET");
        std::env::remove_var("BITGET_TEST_PASSPHRASE");
        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
