use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

use crate::exchanges::unified::MarketType;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisableReason {
    Manual,
    AbnormalSpread,
    OversoldErrors,
    Maintenance,
    UnmanagedPosition,
    Other(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisabledStatus {
    Active,
    Expired,
    Inactive,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DisabledSymbol {
    pub symbol: String,
    pub reason: String,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DisabledExchange {
    pub exchange: String,
    pub reason: String,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DisabledExchangeSymbol {
    pub exchange: String,
    #[serde(deserialize_with = "deserialize_market_type")]
    pub market_type: MarketType,
    pub symbol: String,
    pub reason: String,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnmanagedPosition {
    pub exchange: String,
    #[serde(deserialize_with = "deserialize_market_type")]
    pub market_type: MarketType,
    pub symbol: String,
    pub asset: String,
    pub quantity: f64,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisabledDecision {
    pub status: DisabledStatus,
    pub reason: String,
    pub scope: DisabledScope,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DisabledScope {
    Symbol,
    Exchange,
    ExchangeSymbol,
    UnmanagedPosition,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DisabledRegistryConfig {
    #[serde(default)]
    pub disabled: DisabledConfig,
    #[serde(default)]
    pub unmanaged_positions: Vec<UnmanagedPosition>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DisabledConfig {
    #[serde(default)]
    pub symbols: Vec<DisabledSymbol>,
    #[serde(default)]
    pub exchanges: Vec<DisabledExchange>,
    #[serde(default)]
    pub exchange_symbols: Vec<DisabledExchangeSymbol>,
}

#[derive(Debug, Clone, Default)]
pub struct DisabledRegistry {
    symbols: HashMap<String, DisabledSymbol>,
    exchanges: HashMap<String, DisabledExchange>,
    exchange_symbols: HashMap<(String, MarketType, String), DisabledExchangeSymbol>,
    unmanaged_positions: Vec<UnmanagedPosition>,
}

impl DisabledRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_config(config: DisabledRegistryConfig) -> Self {
        let mut registry = Self::new();
        for item in config.disabled.symbols {
            registry.symbols.insert(
                normalize_symbol(&item.symbol),
                normalize_disabled_symbol(item),
            );
        }
        for item in config.disabled.exchanges {
            registry.exchanges.insert(
                normalize_exchange(&item.exchange),
                normalize_disabled_exchange(item),
            );
        }
        for item in config.disabled.exchange_symbols {
            let item = normalize_disabled_exchange_symbol(item);
            registry.exchange_symbols.insert(
                (
                    normalize_exchange(&item.exchange),
                    item.market_type,
                    normalize_symbol(&item.symbol),
                ),
                item,
            );
        }
        registry.unmanaged_positions = config
            .unmanaged_positions
            .into_iter()
            .map(normalize_unmanaged_position)
            .collect();
        registry
    }

    pub fn load_from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = fs::read_to_string(path)
            .with_context(|| format!("failed to read disabled registry {}", path.display()))?;
        let config = serde_yaml::from_str::<DisabledRegistryConfig>(&text)
            .with_context(|| format!("failed to parse disabled registry {}", path.display()))?;
        Ok(Self::from_config(config))
    }

    pub fn load_or_empty(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref();
        if !path.exists() {
            log::info!(
                "disabled registry config {} missing; using empty disabled registry",
                path.display()
            );
            return Self::new();
        }
        match Self::load_from_path(path) {
            Ok(registry) => registry,
            Err(error) => {
                log::warn!(
                    "disabled registry config {} could not be loaded: {}; using empty registry",
                    path.display(),
                    error
                );
                Self::new()
            }
        }
    }

    pub fn check_symbol(
        &self,
        exchange: &str,
        market_type: MarketType,
        symbol: &str,
        now: DateTime<Utc>,
    ) -> Option<DisabledDecision> {
        let normalized_symbol = normalize_symbol(symbol);
        if let Some(item) = self.symbols.get(&normalized_symbol) {
            if active(item.expires_at, now) {
                return Some(decision(DisabledScope::Symbol, &item.reason));
            }
        }
        let normalized_exchange = normalize_exchange(exchange);
        if let Some(item) = self.exchanges.get(&normalized_exchange) {
            if active(item.expires_at, now) {
                return Some(decision(DisabledScope::Exchange, &item.reason));
            }
        }
        if let Some(item) =
            self.exchange_symbols
                .get(&(normalized_exchange, market_type, normalized_symbol))
        {
            if active(item.expires_at, now) {
                return Some(decision(DisabledScope::ExchangeSymbol, &item.reason));
            }
        }
        None
    }

    pub fn unmanaged_positions(&self) -> &[UnmanagedPosition] {
        &self.unmanaged_positions
    }

    pub fn disabled_symbols(&self) -> Vec<DisabledSymbol> {
        let mut values = self.symbols.values().cloned().collect::<Vec<_>>();
        values.sort_by(|left, right| left.symbol.cmp(&right.symbol));
        values
    }

    pub fn disabled_exchanges(&self) -> Vec<DisabledExchange> {
        let mut values = self.exchanges.values().cloned().collect::<Vec<_>>();
        values.sort_by(|left, right| left.exchange.cmp(&right.exchange));
        values
    }

    pub fn disabled_exchange_symbols(&self) -> Vec<DisabledExchangeSymbol> {
        let mut values = self.exchange_symbols.values().cloned().collect::<Vec<_>>();
        values.sort_by(|left, right| {
            (
                left.exchange.as_str(),
                format!("{:?}", left.market_type),
                left.symbol.as_str(),
            )
                .cmp(&(
                    right.exchange.as_str(),
                    format!("{:?}", right.market_type),
                    right.symbol.as_str(),
                ))
        });
        values
    }

    pub fn unmanaged_quantity(
        &self,
        exchange: &str,
        market_type: MarketType,
        symbol: &str,
        asset: &str,
    ) -> f64 {
        let exchange = normalize_exchange(exchange);
        let symbol = normalize_symbol(symbol);
        let asset = normalize_asset(asset);
        self.unmanaged_positions
            .iter()
            .filter(|position| {
                position.exchange == exchange
                    && position.market_type == market_type
                    && position.symbol == symbol
                    && position.asset == asset
            })
            .map(|position| position.quantity.max(0.0))
            .sum()
    }

    pub fn effective_inventory_quantity(
        &self,
        exchange: &str,
        market_type: MarketType,
        symbol: &str,
        asset: &str,
        available_quantity: f64,
    ) -> f64 {
        (available_quantity - self.unmanaged_quantity(exchange, market_type, symbol, asset))
            .max(0.0)
    }
}

fn decision(scope: DisabledScope, reason: &str) -> DisabledDecision {
    DisabledDecision {
        status: DisabledStatus::Active,
        reason: reason.to_string(),
        scope,
    }
}

fn active(expires_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    expires_at.is_none_or(|expires_at| expires_at > now)
}

fn normalize_disabled_symbol(mut item: DisabledSymbol) -> DisabledSymbol {
    item.symbol = normalize_symbol(&item.symbol);
    item
}

fn normalize_disabled_exchange(mut item: DisabledExchange) -> DisabledExchange {
    item.exchange = normalize_exchange(&item.exchange);
    item
}

fn normalize_disabled_exchange_symbol(mut item: DisabledExchangeSymbol) -> DisabledExchangeSymbol {
    item.exchange = normalize_exchange(&item.exchange);
    item.symbol = normalize_symbol(&item.symbol);
    item
}

fn normalize_unmanaged_position(mut item: UnmanagedPosition) -> UnmanagedPosition {
    item.exchange = normalize_exchange(&item.exchange);
    item.symbol = normalize_symbol(&item.symbol);
    item.asset = normalize_asset(&item.asset);
    item.quantity = item.quantity.max(0.0);
    item
}

fn normalize_exchange(exchange: &str) -> String {
    exchange.trim().to_ascii_lowercase()
}

fn normalize_symbol(symbol: &str) -> String {
    symbol
        .trim()
        .replace(['-', '_', '/'], "")
        .to_ascii_uppercase()
}

fn normalize_asset(asset: &str) -> String {
    asset.trim().to_ascii_uppercase()
}

fn deserialize_market_type<'de, D>(deserializer: D) -> std::result::Result<MarketType, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    match value.trim().to_ascii_lowercase().as_str() {
        "spot" => Ok(MarketType::Spot),
        "perpetual" | "perp" | "futures" | "future" => Ok(MarketType::Perpetual),
        other => Err(serde::de::Error::custom(format!(
            "unsupported market_type: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn registry() -> DisabledRegistry {
        DisabledRegistry::from_config(DisabledRegistryConfig {
            disabled: DisabledConfig {
                symbols: vec![DisabledSymbol {
                    symbol: "TURBOSUSDT".to_string(),
                    reason: "manual disabled".to_string(),
                    expires_at: None,
                }],
                exchanges: vec![DisabledExchange {
                    exchange: "coinex".to_string(),
                    reason: "maintenance".to_string(),
                    expires_at: None,
                }],
                exchange_symbols: vec![
                    DisabledExchangeSymbol {
                        exchange: "mexc".to_string(),
                        market_type: MarketType::Spot,
                        symbol: "DKAUSDT".to_string(),
                        reason: "oversold errors".to_string(),
                        expires_at: None,
                    },
                    DisabledExchangeSymbol {
                        exchange: "mexc".to_string(),
                        market_type: MarketType::Spot,
                        symbol: "OLDUSDT".to_string(),
                        reason: "expired".to_string(),
                        expires_at: Some(Utc::now() - chrono::Duration::days(1)),
                    },
                ],
            },
            unmanaged_positions: vec![UnmanagedPosition {
                exchange: "coinex".to_string(),
                market_type: MarketType::Spot,
                symbol: "PONDUSDT".to_string(),
                asset: "POND".to_string(),
                quantity: 5_000.0,
                reason: "legacy residual".to_string(),
                created_at: Utc::now(),
            }],
        })
    }

    #[test]
    fn disabled_registry_should_reject_global_symbol() {
        let decision = registry()
            .check_symbol("mexc", MarketType::Spot, "TURBOSUSDT", Utc::now())
            .unwrap();
        assert_eq!(decision.scope, DisabledScope::Symbol);
    }

    #[test]
    fn disabled_registry_should_reject_exchange() {
        let decision = registry()
            .check_symbol("coinex", MarketType::Spot, "BTCUSDT", Utc::now())
            .unwrap();
        assert_eq!(decision.scope, DisabledScope::Exchange);
    }

    #[test]
    fn disabled_registry_should_reject_exchange_symbol() {
        let decision = registry()
            .check_symbol("mexc", MarketType::Spot, "DKAUSDT", Utc::now())
            .unwrap();
        assert_eq!(decision.scope, DisabledScope::ExchangeSymbol);
        assert!(decision.reason.contains("oversold"));
    }

    #[test]
    fn disabled_registry_should_ignore_expired_disable() {
        assert!(registry()
            .check_symbol("mexc", MarketType::Spot, "OLDUSDT", Utc::now())
            .is_none());
    }

    #[test]
    fn disabled_registry_should_exclude_unmanaged_position_from_inventory() {
        assert_eq!(
            registry().effective_inventory_quantity(
                "coinex",
                MarketType::Spot,
                "PONDUSDT",
                "POND",
                6_000.0
            ),
            1_000.0
        );
    }

    #[test]
    fn disabled_registry_config_loading_should_work() {
        let file = NamedTempFile::new().unwrap();
        fs::write(
            file.path(),
            r#"
disabled:
  symbols:
    - symbol: TURBOSUSDT
      reason: "manual"
      expires_at: null
unmanaged_positions:
  - exchange: coinex
    market_type: spot
    symbol: PONDUSDT
    asset: POND
    quantity: 5000
    reason: "legacy"
    created_at: "2026-06-04T20:20:00Z"
"#,
        )
        .unwrap();
        let loaded = DisabledRegistry::load_from_path(file.path()).unwrap();
        assert!(loaded
            .check_symbol("mexc", MarketType::Spot, "TURBOSUSDT", Utc::now())
            .is_some());
        assert_eq!(
            loaded.unmanaged_quantity("coinex", MarketType::Spot, "PONDUSDT", "POND"),
            5_000.0
        );
    }
}
