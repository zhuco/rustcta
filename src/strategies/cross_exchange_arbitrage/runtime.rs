//! Runtime wiring helpers for cross-exchange arbitrage live execution.
//!
//! This module owns strategy-specific adapter construction so binaries do not
//! duplicate exchange credential, position-mode, and private stream wiring.

use super::{CrossExchangeArbitrageConfig, PrivateWsRuntimeConfig};
use crate::core::config::ApiKeys;
use crate::core::exchange::Exchange;
use crate::exchanges::adapters::{
    private_perp_trading_adapter_for_with_instruments, ExchangeTradingAdapter, PrivatePerpExchange,
    PrivateRestAuth, PrivateWsAuth, PrivateWsRunConfig,
};
use crate::exchanges::{BinanceExchange, OkxExchange};
use crate::execution::{ExecutionRouter, PositionMode, TradingAdapter};
use crate::market::{exchange_symbol_for, ExchangeId, ExchangeSymbol, InstrumentMeta};
use anyhow::{anyhow, Result};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateWsRuntimeSpec {
    pub exchange: PrivatePerpExchange,
    pub auth: PrivateWsAuth,
    pub symbols: Vec<ExchangeSymbol>,
    pub config: PrivateWsRunConfig,
}

#[derive(Clone)]
pub struct BinancePrivateWsRuntimeSpec {
    pub exchange: Arc<dyn Exchange>,
    pub config: PrivateWsRunConfig,
}

pub struct CrossArbLiveRuntimeParts {
    pub router: ExecutionRouter,
    pub adapters: Vec<Arc<dyn TradingAdapter>>,
    pub private_ws_specs: Vec<PrivateWsRuntimeSpec>,
    pub binance_private_ws_specs: Vec<BinancePrivateWsRuntimeSpec>,
}

pub fn build_cross_arb_execution_router(
    config: &CrossExchangeArbitrageConfig,
) -> Result<ExecutionRouter> {
    build_cross_arb_execution_router_with_instruments(config, Vec::new())
}

pub fn build_cross_arb_execution_router_with_instruments(
    config: &CrossExchangeArbitrageConfig,
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Result<ExecutionRouter> {
    let instruments = instruments.into_iter().collect::<Vec<_>>();
    let mut router = ExecutionRouter::new(config.execution.dry_run);
    for exchange in live_enabled_exchanges(config) {
        let adapter = build_trading_adapter_for_exchange_with_instruments(
            config,
            &exchange,
            instruments
                .iter()
                .filter(|item| item.exchange == exchange)
                .cloned(),
        )?;
        router.register_adapter(adapter);
    }
    Ok(router)
}

pub fn build_cross_arb_live_runtime_parts(
    config: &CrossExchangeArbitrageConfig,
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Result<CrossArbLiveRuntimeParts> {
    let instruments = instruments.into_iter().collect::<Vec<_>>();
    let mut router = ExecutionRouter::new(config.execution.dry_run);
    let mut adapters = Vec::new();
    let mut binance_private_ws_specs = Vec::new();
    for exchange in live_enabled_exchanges(config) {
        let adapter = build_trading_adapter_for_exchange_with_instruments(
            config,
            &exchange,
            instruments
                .iter()
                .filter(|item| item.exchange == exchange)
                .cloned(),
        )?;
        router.register_adapter(adapter.clone());
        adapters.push(adapter);
        if exchange == ExchangeId::Binance
            && config
                .exchanges
                .get(&exchange)
                .map(|runtime| runtime.private_ws_enabled)
                .unwrap_or(false)
        {
            binance_private_ws_specs.push(BinancePrivateWsRuntimeSpec {
                exchange: build_core_exchange(config, &exchange)?,
                config: config
                    .exchanges
                    .get(&exchange)
                    .map(|runtime| runtime.private_ws_run.clone())
                    .unwrap_or_default()
                    .into(),
            });
        }
    }
    let private_ws_specs = build_private_ws_runtime_specs(
        config,
        live_enabled_exchanges(config).into_iter().map(|exchange| {
            let symbols = config
                .universe
                .symbols
                .iter()
                .map(|symbol| exchange_symbol_for(&exchange, symbol))
                .collect::<Vec<_>>();
            (exchange, symbols)
        }),
    )?;
    Ok(CrossArbLiveRuntimeParts {
        router,
        adapters,
        private_ws_specs,
        binance_private_ws_specs,
    })
}

pub fn build_trading_adapter_for_exchange(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
) -> Result<Arc<dyn TradingAdapter>> {
    build_trading_adapter_for_exchange_with_instruments(config, exchange, Vec::new())
}

pub fn build_trading_adapter_for_exchange_with_instruments(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Result<Arc<dyn TradingAdapter>> {
    match exchange {
        ExchangeId::Binance | ExchangeId::Okx => {
            let core_exchange = build_core_exchange(config, exchange)?;
            Ok(Arc::new(
                ExchangeTradingAdapter::new(exchange.clone(), core_exchange)
                    .with_position_mode(configured_position_mode(config, exchange))
                    .with_instruments(instruments),
            ))
        }
        ExchangeId::Bitget | ExchangeId::Gate => {
            let private_exchange = private_perp_exchange(exchange)
                .ok_or_else(|| anyhow!("{exchange} private perpetual adapter is not registered"))?;
            let auth = private_rest_auth_for_exchange(config, exchange)?;
            private_perp_trading_adapter_for_with_instruments(
                private_exchange,
                auth,
                configured_position_mode(config, exchange),
                instruments,
            )
        }
        ExchangeId::Other(_) => anyhow::bail!("{exchange} trading adapter is not registered"),
    }
}

pub fn build_private_ws_runtime_specs(
    config: &CrossExchangeArbitrageConfig,
    symbols_by_exchange: impl IntoIterator<Item = (ExchangeId, Vec<ExchangeSymbol>)>,
) -> Result<Vec<PrivateWsRuntimeSpec>> {
    let mut specs = Vec::new();
    for (exchange, symbols) in symbols_by_exchange {
        let Some(private_exchange) = private_perp_exchange(&exchange) else {
            continue;
        };
        let runtime = config.exchanges.get(&exchange);
        if !runtime
            .map(|runtime| runtime.private_ws_enabled)
            .unwrap_or(false)
        {
            continue;
        }
        specs.push(PrivateWsRuntimeSpec {
            exchange: private_exchange,
            auth: private_ws_auth_for_exchange(config, &exchange)?,
            symbols,
            config: runtime
                .map(|runtime| runtime.private_ws_run.clone())
                .unwrap_or_default()
                .into(),
        });
    }
    Ok(specs)
}

pub fn private_rest_auth_for_exchange(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
) -> Result<PrivateRestAuth> {
    let api_keys = api_keys_for_exchange(config, exchange)?;
    Ok(PrivateRestAuth {
        api_key: api_keys.api_key,
        api_secret: api_keys.api_secret,
        passphrase: api_keys.passphrase,
    })
}

pub fn private_ws_auth_for_exchange(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
) -> Result<PrivateWsAuth> {
    let api_keys = api_keys_for_exchange(config, exchange)?;
    let account_id = config
        .exchanges
        .get(exchange)
        .and_then(|runtime| runtime.account_id.clone())
        .or(api_keys.memo);
    Ok(PrivateWsAuth {
        api_key: api_keys.api_key,
        api_secret: api_keys.api_secret,
        passphrase: api_keys.passphrase,
        account_id,
    })
}

pub fn configured_position_mode(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
) -> PositionMode {
    config
        .exchanges
        .get(exchange)
        .and_then(|runtime| runtime.position_mode.as_deref())
        .map(position_mode_from_config)
        .unwrap_or(PositionMode::OneWay)
}

pub fn private_perp_exchange(exchange: &ExchangeId) -> Option<PrivatePerpExchange> {
    match exchange {
        ExchangeId::Bitget => Some(PrivatePerpExchange::Bitget),
        ExchangeId::Gate => Some(PrivatePerpExchange::Gate),
        _ => None,
    }
}

pub fn live_enabled_exchanges(config: &CrossExchangeArbitrageConfig) -> Vec<ExchangeId> {
    config
        .universe
        .enabled_exchanges
        .iter()
        .filter(|exchange| {
            config
                .exchanges
                .get(*exchange)
                .and_then(|runtime| runtime.enabled)
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

fn api_keys_for_exchange(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
) -> Result<ApiKeys> {
    let prefix = config
        .exchanges
        .get(exchange)
        .and_then(|runtime| runtime.env_prefix.as_deref())
        .unwrap_or_else(|| exchange.as_str());
    ApiKeys::from_env(prefix).map_err(|err| anyhow!(err.to_string()))
}

fn build_core_exchange(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
) -> Result<Arc<dyn Exchange>> {
    let api_keys = api_keys_for_exchange(config, exchange)?;
    match exchange {
        ExchangeId::Binance => Ok(Arc::new(BinanceExchange::new(
            crate::core::config::Config {
                name: "binance".to_string(),
                testnet: false,
                spot_base_url: "https://api.binance.com".to_string(),
                futures_base_url: "https://fapi.binance.com".to_string(),
                ws_spot_url: "wss://stream.binance.com:9443".to_string(),
                ws_futures_url: "wss://fstream.binance.com".to_string(),
            },
            api_keys,
        ))),
        ExchangeId::Okx => Ok(Arc::new(OkxExchange::new(
            crate::core::config::Config {
                name: "okx".to_string(),
                testnet: false,
                spot_base_url: "https://www.okx.com".to_string(),
                futures_base_url: "https://www.okx.com".to_string(),
                ws_spot_url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
                ws_futures_url: "wss://ws.okx.com:8443/ws/v5/public".to_string(),
            },
            api_keys,
        ))),
        _ => anyhow::bail!("{exchange} core Exchange adapter is not registered"),
    }
}

fn position_mode_from_config(value: &str) -> PositionMode {
    if matches!(
        value.to_ascii_lowercase().as_str(),
        "hedge" | "hedged" | "long_short"
    ) {
        PositionMode::Hedge
    } else {
        PositionMode::OneWay
    }
}

impl From<PrivateWsRuntimeConfig> for PrivateWsRunConfig {
    fn from(value: PrivateWsRuntimeConfig) -> Self {
        Self {
            connect_timeout_ms: value.connect_timeout_ms,
            reconnect_delay_ms: value.reconnect_delay_ms,
            heartbeat_interval_ms: value.heartbeat_interval_ms,
            subscribe_interval_ms: value.subscribe_interval_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{CanonicalSymbol, ContractType, ExchangeSymbol, InstrumentStatus};

    #[test]
    fn runtime_should_filter_disabled_exchanges() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config
            .exchanges
            .entry(ExchangeId::Bitget)
            .or_default()
            .enabled = Some(false);

        let exchanges = live_enabled_exchanges(&config);

        assert!(exchanges.contains(&ExchangeId::Binance));
        assert!(!exchanges.contains(&ExchangeId::Bitget));
    }

    #[test]
    fn runtime_should_parse_position_mode_for_private_adapter() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config
            .exchanges
            .entry(ExchangeId::Bitget)
            .or_default()
            .position_mode = Some("hedge".to_string());

        assert_eq!(
            configured_position_mode(&config, &ExchangeId::Bitget),
            PositionMode::Hedge
        );
        assert_eq!(
            configured_position_mode(&config, &ExchangeId::Gate),
            PositionMode::OneWay
        );
    }

    #[test]
    fn runtime_should_build_private_ws_specs_from_config() {
        let mut config = CrossExchangeArbitrageConfig::default();
        let gate = config.exchanges.entry(ExchangeId::Gate).or_default();
        gate.private_ws_enabled = true;
        gate.account_id = Some("20011".to_string());
        gate.env_prefix = Some("GATE_TEST_RUNTIME".to_string());
        std::env::set_var("GATE_TEST_RUNTIME_API_KEY", "key");
        std::env::set_var("GATE_TEST_RUNTIME_API_SECRET", "secret");

        let specs = build_private_ws_runtime_specs(
            &config,
            [(
                ExchangeId::Gate,
                vec![ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")],
            )],
        )
        .unwrap();

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].exchange, PrivatePerpExchange::Gate);
        assert_eq!(specs[0].auth.account_id.as_deref(), Some("20011"));

        std::env::remove_var("GATE_TEST_RUNTIME_API_KEY");
        std::env::remove_var("GATE_TEST_RUNTIME_API_SECRET");
    }

    #[test]
    fn runtime_should_build_private_rest_auth_with_env_prefix() {
        let mut config = CrossExchangeArbitrageConfig::default();
        let bitget = config.exchanges.entry(ExchangeId::Bitget).or_default();
        bitget.env_prefix = Some("BITGET_TEST_RUNTIME".to_string());
        std::env::set_var("BITGET_TEST_RUNTIME_API_KEY", "key");
        std::env::set_var("BITGET_TEST_RUNTIME_API_SECRET", "secret");
        std::env::set_var("BITGET_TEST_RUNTIME_PASSPHRASE", "pass");

        let auth = private_rest_auth_for_exchange(&config, &ExchangeId::Bitget).unwrap();

        assert_eq!(auth.api_key, "key");
        assert_eq!(auth.passphrase.as_deref(), Some("pass"));

        std::env::remove_var("BITGET_TEST_RUNTIME_API_KEY");
        std::env::remove_var("BITGET_TEST_RUNTIME_API_SECRET");
        std::env::remove_var("BITGET_TEST_RUNTIME_PASSPHRASE");
    }

    #[test]
    fn runtime_should_register_private_adapter_with_symbol_rules() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.universe.enabled_exchanges = vec![ExchangeId::Bitget];
        let bitget = config.exchanges.entry(ExchangeId::Bitget).or_default();
        bitget.enabled = Some(true);
        bitget.env_prefix = Some("BITGET_ROUTER_TEST".to_string());
        std::env::set_var("BITGET_ROUTER_TEST_API_KEY", "key");
        std::env::set_var("BITGET_ROUTER_TEST_API_SECRET", "secret");
        std::env::set_var("BITGET_ROUTER_TEST_PASSPHRASE", "pass");

        let router = build_cross_arb_execution_router_with_instruments(
            &config,
            [InstrumentMeta::new(
                ExchangeId::Bitget,
                CanonicalSymbol::new("BTC", "USDT"),
                ExchangeSymbol::new(ExchangeId::Bitget, "BTCUSDT"),
                "BTC",
                "USDT",
                "USDT",
                ContractType::LinearPerpetual,
                1.0,
                0.1,
                0.001,
                0.001,
                10.0,
                1,
                3,
                InstrumentStatus::Trading,
            )],
        )
        .unwrap();

        assert!(router.has_adapter(&ExchangeId::Bitget));

        std::env::remove_var("BITGET_ROUTER_TEST_API_KEY");
        std::env::remove_var("BITGET_ROUTER_TEST_API_SECRET");
        std::env::remove_var("BITGET_ROUTER_TEST_PASSPHRASE");
    }

    #[test]
    fn runtime_should_build_live_parts_with_router_adapters_and_private_ws_specs() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.universe.enabled_exchanges =
            vec![ExchangeId::Binance, ExchangeId::Bitget, ExchangeId::Gate];
        config.universe.symbols = vec![CanonicalSymbol::new("BTC", "USDT")];
        let binance = config.exchanges.entry(ExchangeId::Binance).or_default();
        binance.enabled = Some(true);
        binance.private_ws_enabled = true;
        binance.env_prefix = Some("BINANCE_LIVE_PARTS_TEST".to_string());
        let bitget = config.exchanges.entry(ExchangeId::Bitget).or_default();
        bitget.enabled = Some(true);
        bitget.private_ws_enabled = true;
        bitget.env_prefix = Some("BITGET_LIVE_PARTS_TEST".to_string());
        let gate = config.exchanges.entry(ExchangeId::Gate).or_default();
        gate.enabled = Some(true);
        gate.private_ws_enabled = true;
        gate.account_id = Some("20011".to_string());
        gate.env_prefix = Some("GATE_LIVE_PARTS_TEST".to_string());
        std::env::set_var("BITGET_LIVE_PARTS_TEST_API_KEY", "key");
        std::env::set_var("BITGET_LIVE_PARTS_TEST_API_SECRET", "secret");
        std::env::set_var("BITGET_LIVE_PARTS_TEST_PASSPHRASE", "pass");
        std::env::set_var("GATE_LIVE_PARTS_TEST_API_KEY", "key");
        std::env::set_var("GATE_LIVE_PARTS_TEST_API_SECRET", "secret");
        std::env::set_var("BINANCE_LIVE_PARTS_TEST_API_KEY", "key");
        std::env::set_var("BINANCE_LIVE_PARTS_TEST_API_SECRET", "secret");

        let parts = build_cross_arb_live_runtime_parts(&config, Vec::new()).unwrap();

        assert!(parts.router.has_adapter(&ExchangeId::Binance));
        assert!(parts.router.has_adapter(&ExchangeId::Bitget));
        assert!(parts.router.has_adapter(&ExchangeId::Gate));
        assert_eq!(parts.adapters.len(), 3);
        assert_eq!(parts.private_ws_specs.len(), 2);
        assert_eq!(parts.binance_private_ws_specs.len(), 1);

        std::env::remove_var("BITGET_LIVE_PARTS_TEST_API_KEY");
        std::env::remove_var("BITGET_LIVE_PARTS_TEST_API_SECRET");
        std::env::remove_var("BITGET_LIVE_PARTS_TEST_PASSPHRASE");
        std::env::remove_var("GATE_LIVE_PARTS_TEST_API_KEY");
        std::env::remove_var("GATE_LIVE_PARTS_TEST_API_SECRET");
        std::env::remove_var("BINANCE_LIVE_PARTS_TEST_API_KEY");
        std::env::remove_var("BINANCE_LIVE_PARTS_TEST_API_SECRET");
    }

    #[test]
    fn runtime_should_map_exchange_symbols_for_ws_specs() {
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let exchange_symbol = crate::market::exchange_symbol_for(&ExchangeId::Gate, &symbol);

        assert_eq!(exchange_symbol.symbol, "BTC_USDT");
    }
}
