use crate::core::config::{ExchangeConfig, GlobalConfig, RateLimits, SymbolFormat};
use std::collections::HashMap;

/// 创建默认配置，包含所有交易所的API URL
pub fn create_default_config() -> GlobalConfig {
    let mut exchanges = HashMap::new();

    // Binance配置 (API v3 - 2024最新)
    exchanges.insert(
        "binance".to_string(),
        ExchangeConfig {
            name: "binance".to_string(),
            testnet: false,
            base_url: "https://api.binance.com".to_string(), // 现货API v3
            websocket_url: "wss://stream.binance.com:9443".to_string(),
            symbol_separator: "".to_string(),
            symbol_format: "{base}{quote}".to_string(),
            rate_limits: RateLimits {
                requests_per_minute: Some(1200),
                requests_per_second: Some(20),
                orders_per_minute: Some(100),
            },
            endpoints: HashMap::from([
                (
                    "futures_base".to_string(),
                    "https://fapi.binance.com".to_string(),
                ), // 期货API v3
                (
                    "data_api".to_string(),
                    "https://data-api.binance.vision".to_string(),
                ), // 公共数据API
            ]),
        },
    );

    // Bitmart配置 (2024更新: 现货V4, 期货V2)
    exchanges.insert(
        "bitmart".to_string(),
        ExchangeConfig {
            name: "bitmart".to_string(),
            testnet: false,
            base_url: "https://api-cloud.bitmart.com".to_string(), // 现货API
            websocket_url: "wss://ws-manager-compress.bitmart.com/api?protocol=1.1".to_string(),
            symbol_separator: "_".to_string(),
            symbol_format: "{base}_{quote}".to_string(),
            rate_limits: RateLimits {
                requests_per_minute: Some(600),
                requests_per_second: Some(10),
                orders_per_minute: Some(60),
            },
            endpoints: HashMap::from([
                (
                    "futures_base".to_string(),
                    "https://api-cloud-v2.bitmart.com".to_string(),
                ), // 期货V2 API
                (
                    "futures_ws".to_string(),
                    "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1".to_string(),
                ), // 期货V2 WebSocket
            ]),
        },
    );

    // OKX配置 (API v5 - 统一账户系统)
    exchanges.insert(
        "okx".to_string(),
        ExchangeConfig {
            name: "okx".to_string(),
            testnet: false,
            base_url: "https://www.okx.com".to_string(), // 主站API v5
            websocket_url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
            symbol_separator: "-".to_string(),
            symbol_format: "{base}-{quote}".to_string(),
            rate_limits: RateLimits {
                requests_per_minute: Some(600),
                requests_per_second: Some(20),
                orders_per_minute: Some(60),
            },
            endpoints: HashMap::from([
                ("aws_base".to_string(), "https://aws.okx.com".to_string()), // AWS节点
                (
                    "private_ws".to_string(),
                    "wss://ws.okx.com:8443/ws/v5/private".to_string(),
                ), // 私有WebSocket
            ]),
        },
    );

    // Bybit配置
    exchanges.insert(
        "bybit".to_string(),
        ExchangeConfig {
            name: "bybit".to_string(),
            testnet: false,
            base_url: "https://api.bybit.com".to_string(),
            websocket_url: "wss://stream.bybit.com/v5/public/spot".to_string(),
            symbol_separator: "".to_string(),
            symbol_format: "{base}{quote}".to_string(),
            rate_limits: RateLimits {
                requests_per_minute: Some(600),
                requests_per_second: Some(10),
                orders_per_minute: Some(60),
            },
            endpoints: HashMap::new(),
        },
    );

    // HTX配置
    exchanges.insert(
        "htx".to_string(),
        ExchangeConfig {
            name: "htx".to_string(),
            testnet: false,
            base_url: "https://api.huobi.pro".to_string(),
            websocket_url: "wss://api.huobi.pro/ws".to_string(),
            symbol_separator: "".to_string(),
            symbol_format: "{base}{quote}".to_string(),
            rate_limits: RateLimits {
                requests_per_minute: Some(100),
                requests_per_second: Some(10),
                orders_per_minute: Some(50),
            },
            endpoints: HashMap::new(),
        },
    );

    // Hyperliquid配置
    exchanges.insert(
        "hyperliquid".to_string(),
        ExchangeConfig {
            name: "hyperliquid".to_string(),
            testnet: false,
            base_url: "https://api.hyperliquid.xyz".to_string(),
            websocket_url: "wss://api.hyperliquid.xyz/ws".to_string(),
            symbol_separator: "-".to_string(),
            symbol_format: "{base}-{quote}".to_string(),
            rate_limits: RateLimits {
                requests_per_minute: Some(1200),
                requests_per_second: Some(20),
                orders_per_minute: Some(100),
            },
            endpoints: HashMap::new(),
        },
    );

    // 符号格式转换规则
    let mut conversion_rules = HashMap::new();
    conversion_rules.insert(
        "binance".to_string(),
        HashMap::from([
            ("spot".to_string(), "{base}{quote}".to_string()),
            ("futures".to_string(), "{base}{quote}".to_string()),
        ]),
    );
    conversion_rules.insert(
        "bitmart".to_string(),
        HashMap::from([
            ("spot".to_string(), "{base}_{quote}".to_string()),
            ("futures".to_string(), "{base}{quote}".to_string()),
        ]),
    );
    conversion_rules.insert(
        "okx".to_string(),
        HashMap::from([
            ("spot".to_string(), "{base}-{quote}".to_string()),
            ("futures".to_string(), "{base}-{quote}-SWAP".to_string()),
        ]),
    );
    conversion_rules.insert(
        "bybit".to_string(),
        HashMap::from([
            ("spot".to_string(), "{base}{quote}".to_string()),
            ("futures".to_string(), "{base}{quote}".to_string()),
        ]),
    );

    GlobalConfig {
        exchanges,
        symbol_format: SymbolFormat {
            standard: "BASE/QUOTE".to_string(),
            conversion_rules,
        },
    }
}
