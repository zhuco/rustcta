use crate::smart_money::PortfolioConstraints;
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SmartMoneyServiceConfig {
    pub service_name: String,
    pub environment: String,
    pub log_level: String,
    pub postgres: Option<PostgresConfig>,
    pub clickhouse: Option<ClickHouseConfig>,
    pub redis: Option<RedisConfig>,
    pub nats: Option<NatsConfig>,
    pub binance_collector: BinanceCollectorConfig,
    pub hyperliquid_wallet_ingestion: HyperliquidWalletIngestionConfig,
    pub portfolio: SmartMoneyPortfolioConfig,
}

impl Default for SmartMoneyServiceConfig {
    fn default() -> Self {
        Self {
            service_name: "smart-money".to_string(),
            environment: "local".to_string(),
            log_level: "info".to_string(),
            postgres: None,
            clickhouse: None,
            redis: None,
            nats: None,
            binance_collector: BinanceCollectorConfig::default(),
            hyperliquid_wallet_ingestion: HyperliquidWalletIngestionConfig::default(),
            portfolio: SmartMoneyPortfolioConfig::default(),
        }
    }
}

impl SmartMoneyServiceConfig {
    pub fn load_yaml(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse config {}", path.display()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PostgresConfig {
    pub enabled: bool,
    pub url_env: String,
    pub database: Option<String>,
    pub max_connections: u32,
    pub connect_timeout_secs: u64,
    pub statement_timeout_ms: u64,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url_env: "SMART_MONEY_POSTGRES_URL".to_string(),
            database: None,
            max_connections: 8,
            connect_timeout_secs: 5,
            statement_timeout_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct ClickHouseConfig {
    pub enabled: bool,
    pub url: String,
    pub database: String,
    pub user_env: String,
    pub password_env: String,
    pub insert_batch_size: usize,
    pub flush_interval_secs: u64,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: "http://localhost:8123".to_string(),
            database: "smart_money".to_string(),
            user_env: "SMART_MONEY_CLICKHOUSE_USER".to_string(),
            password_env: "SMART_MONEY_CLICKHOUSE_PASSWORD".to_string(),
            insert_batch_size: 1_000,
            flush_interval_secs: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RedisConfig {
    pub enabled: bool,
    pub url_env: String,
    pub key_prefix: String,
    pub pool_size: u32,
    pub default_ttl_secs: u64,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url_env: "SMART_MONEY_REDIS_URL".to_string(),
            key_prefix: "smart_money".to_string(),
            pool_size: 4,
            default_ttl_secs: 300,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct NatsConfig {
    pub enabled: bool,
    pub url_env: String,
    pub subject_prefix: String,
    pub durable_name: String,
    pub request_timeout_secs: u64,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url_env: "SMART_MONEY_NATS_URL".to_string(),
            subject_prefix: "smart_money".to_string(),
            durable_name: "smart_money_service".to_string(),
            request_timeout_secs: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct BinanceCollectorConfig {
    pub enabled: bool,
    pub rest_base_url: String,
    pub ws_base_url: String,
    pub symbols: Vec<String>,
    pub intervals: Vec<String>,
    pub collect_trades: bool,
    pub collect_orderbook: bool,
    pub orderbook_depth: u16,
    pub poll_interval_secs: u64,
}

impl Default for BinanceCollectorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            rest_base_url: "https://fapi.binance.com".to_string(),
            ws_base_url: "wss://fstream.binance.com/ws".to_string(),
            symbols: Vec::new(),
            intervals: vec!["1m".to_string()],
            collect_trades: true,
            collect_orderbook: true,
            orderbook_depth: 20,
            poll_interval_secs: 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HyperliquidWalletIngestionConfig {
    pub enabled: bool,
    pub api_base_url: String,
    pub wallets: Vec<TrackedWalletConfig>,
    pub ingest_positions: bool,
    pub ingest_fills: bool,
    pub poll_interval_secs: u64,
    pub lookback_days: u32,
}

impl Default for HyperliquidWalletIngestionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            api_base_url: "https://api.hyperliquid.xyz".to_string(),
            wallets: Vec::new(),
            ingest_positions: true,
            ingest_fills: true,
            poll_interval_secs: 30,
            lookback_days: 30,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TrackedWalletConfig {
    pub address: String,
    pub label: Option<String>,
    pub tags: Vec<String>,
    pub enabled: bool,
    pub source_rank: Option<u32>,
    pub style: Option<String>,
    pub grade: Option<String>,
    pub initial_score: Option<Decimal>,
    pub initial_weight: Option<Decimal>,
    pub group_name: Option<String>,
    pub cluster: Option<String>,
}

impl Default for TrackedWalletConfig {
    fn default() -> Self {
        Self {
            address: String::new(),
            label: None,
            tags: Vec::new(),
            enabled: true,
            source_rank: None,
            style: None,
            grade: None,
            initial_score: None,
            initial_weight: None,
            group_name: None,
            cluster: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SmartMoneyPortfolioConfig {
    pub enabled: bool,
    pub constraints: PortfolioConstraintsConfig,
    pub symbol_overrides: BTreeMap<String, PortfolioConstraintsConfig>,
    pub rebalance_interval_secs: u64,
    pub alpha_stale_after_secs: u64,
    pub close_only: bool,
    pub dry_run: bool,
}

impl Default for SmartMoneyPortfolioConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            constraints: PortfolioConstraintsConfig::default(),
            symbol_overrides: BTreeMap::new(),
            rebalance_interval_secs: 60,
            alpha_stale_after_secs: 120,
            close_only: false,
            dry_run: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct PortfolioConstraintsConfig {
    pub initial_capital_usdt: Decimal,
    pub standard_entry_notional_usdt: Decimal,
    pub max_leverage: Decimal,
    pub max_gross_notional_usdt: Decimal,
    pub max_single_asset_gross_share: Decimal,
}

impl Default for PortfolioConstraintsConfig {
    fn default() -> Self {
        Self {
            initial_capital_usdt: Decimal::new(2_000, 0),
            standard_entry_notional_usdt: Decimal::new(1_000, 0),
            max_leverage: Decimal::new(10, 0),
            max_gross_notional_usdt: Decimal::new(20_000, 0),
            max_single_asset_gross_share: Decimal::new(35, 2),
        }
    }
}

impl From<&PortfolioConstraintsConfig> for PortfolioConstraints {
    fn from(value: &PortfolioConstraintsConfig) -> Self {
        Self {
            initial_capital_usdt: value.initial_capital_usdt,
            standard_entry_notional_usdt: value.standard_entry_notional_usdt,
            max_leverage: value.max_leverage,
            max_gross_notional_usdt: value.max_gross_notional_usdt,
            max_single_asset_gross_share: value.max_single_asset_gross_share,
        }
    }
}
