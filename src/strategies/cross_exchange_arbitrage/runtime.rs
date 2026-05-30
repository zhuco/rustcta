//! Runtime wiring helpers for cross-exchange arbitrage live execution.
//!
//! This module owns strategy-specific adapter construction so binaries do not
//! duplicate exchange credential, position-mode, and private stream wiring.

use super::CrossExchangeArbitrageConfig;
use crate::core::exchange::Exchange;
use crate::exchanges::adapters::{PrivatePerpExchange, PrivateWsAuth, PrivateWsRunConfig};
pub use crate::exchanges::registry::{
    build_core_exchange_for_exchange, build_trading_adapter_for_exchange,
    build_trading_adapter_for_exchange_with_instruments, configured_position_mode,
    private_perp_exchange, private_rest_auth_for_exchange, private_ws_auth_for_exchange,
};
use crate::execution::{ExecutionRouter, TradingAdapter};
use crate::market::{exchange_symbol_for, ExchangeId, ExchangeSymbol, InstrumentMeta};
use anyhow::Result;
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct PrivateWsRuntimeSpec {
    pub exchange: PrivatePerpExchange,
    pub auth: PrivateWsAuth,
    pub symbols: Vec<ExchangeSymbol>,
    pub instruments: Vec<InstrumentMeta>,
    pub config: PrivateWsRunConfig,
    pub url: String,
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
                exchange: build_core_exchange_for_exchange(config, &exchange)?,
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
            let ws_instruments = instruments
                .iter()
                .filter(|item| item.exchange == exchange)
                .cloned()
                .collect::<Vec<_>>();
            let symbols = private_ws_symbols_for_exchange(config, &exchange, &ws_instruments);
            (exchange, symbols, ws_instruments)
        }),
    )?;
    Ok(CrossArbLiveRuntimeParts {
        router,
        adapters,
        private_ws_specs,
        binance_private_ws_specs,
    })
}

fn private_ws_symbols_for_exchange(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
    instruments: &[InstrumentMeta],
) -> Vec<ExchangeSymbol> {
    let universe = config
        .universe
        .symbols
        .iter()
        .cloned()
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    let mut symbols = instruments
        .iter()
        .filter(|instrument| instrument.exchange == *exchange)
        .filter(|instrument| instrument.status.allows_closes())
        .filter(|instrument| universe.contains(&instrument.canonical_symbol))
        .filter_map(|instrument| {
            seen.insert(instrument.exchange_symbol.clone())
                .then(|| instrument.exchange_symbol.clone())
        })
        .collect::<Vec<_>>();
    if symbols.is_empty() {
        symbols = config
            .universe
            .symbols
            .iter()
            .map(|symbol| exchange_symbol_for(exchange, symbol))
            .filter(|symbol| seen.insert(symbol.clone()))
            .collect();
    }
    symbols
}

pub fn build_private_ws_runtime_specs(
    config: &CrossExchangeArbitrageConfig,
    symbols_by_exchange: impl IntoIterator<
        Item = (ExchangeId, Vec<ExchangeSymbol>, Vec<InstrumentMeta>),
    >,
) -> Result<Vec<PrivateWsRuntimeSpec>> {
    let mut specs = Vec::new();
    for (exchange, symbols, instruments) in symbols_by_exchange {
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
            instruments,
            config: runtime
                .map(|runtime| runtime.private_ws_run.clone())
                .unwrap_or_default()
                .into(),
            url: runtime
                .and_then(|runtime| runtime.private_ws_url.clone())
                .unwrap_or_else(|| private_exchange.private_ws_url().to_string()),
        });
    }
    Ok(specs)
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
                .map(|runtime| !runtime.is_disabled())
                .unwrap_or(true)
        })
        .cloned()
        .collect()
}

pub fn exchange_route_status(
    config: &CrossExchangeArbitrageConfig,
    exchange: &ExchangeId,
) -> crate::market::RouteStatus {
    config
        .exchanges
        .get(exchange)
        .map(|runtime| runtime.route_status())
        .unwrap_or(crate::market::RouteStatus::Healthy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cross_exchange_arbitrage::ExchangeOperatingMode;
    use crate::execution::PositionMode;
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
    fn runtime_should_keep_close_only_exchanges_attached() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config
            .exchanges
            .entry(ExchangeId::Bitget)
            .or_default()
            .operating_mode = ExchangeOperatingMode::CloseOnly;

        let exchanges = live_enabled_exchanges(&config);

        assert!(exchanges.contains(&ExchangeId::Bitget));
        assert_eq!(
            exchange_route_status(&config, &ExchangeId::Bitget),
            crate::market::RouteStatus::CloseOnly
        );
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

        config
            .exchanges
            .entry(ExchangeId::Gate)
            .or_default()
            .position_mode = Some("hedge".to_string());
        assert_eq!(
            configured_position_mode(&config, &ExchangeId::Gate),
            PositionMode::Hedge
        );
    }

    #[test]
    fn runtime_should_build_private_ws_specs_from_config() {
        let mut config = CrossExchangeArbitrageConfig::default();
        let gate = config.exchanges.entry(ExchangeId::Gate).or_default();
        gate.private_ws_enabled = true;
        gate.account_id = Some("20011".to_string());
        gate.env_prefix = Some("GATE_TEST_RUNTIME".to_string());
        gate.demo_trading = true;
        gate.private_ws_url = Some("wss://ws-testnet.gate.com/v4/ws/futures/usdt".to_string());
        std::env::set_var("GATE_TEST_RUNTIME_API_KEY", "key");
        std::env::set_var("GATE_TEST_RUNTIME_API_SECRET", "secret");

        let specs = build_private_ws_runtime_specs(
            &config,
            [(
                ExchangeId::Gate,
                vec![ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")],
                Vec::new(),
            )],
        )
        .unwrap();

        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].exchange, PrivatePerpExchange::Gate);
        assert_eq!(specs[0].auth.account_id.as_deref(), Some("20011"));
        assert!(specs[0].auth.demo_trading);
        assert_eq!(specs[0].url, "wss://ws-testnet.gate.com/v4/ws/futures/usdt");

        std::env::remove_var("GATE_TEST_RUNTIME_API_KEY");
        std::env::remove_var("GATE_TEST_RUNTIME_API_SECRET");
    }

    #[test]
    fn runtime_should_build_private_rest_auth_with_env_prefix() {
        let mut config = CrossExchangeArbitrageConfig::default();
        let bitget = config.exchanges.entry(ExchangeId::Bitget).or_default();
        bitget.env_prefix = Some("BITGET_TEST_RUNTIME".to_string());
        bitget.demo_trading = true;
        std::env::set_var("BITGET_TEST_RUNTIME_API_KEY", "key");
        std::env::set_var("BITGET_TEST_RUNTIME_API_SECRET", "secret");
        std::env::set_var("BITGET_TEST_RUNTIME_PASSPHRASE", "pass");

        let auth = private_rest_auth_for_exchange(&config, &ExchangeId::Bitget).unwrap();

        assert_eq!(auth.api_key, "key");
        assert_eq!(auth.passphrase.as_deref(), Some("pass"));
        assert!(auth.demo_trading);

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
    fn runtime_should_filter_private_ws_symbols_to_loaded_instruments() {
        let mut config = CrossExchangeArbitrageConfig::default();
        config.universe.enabled_exchanges = vec![ExchangeId::Gate];
        config.universe.symbols = vec![
            crate::market::CanonicalSymbol::new("BTC", "USDT"),
            crate::market::CanonicalSymbol::new("FAKE", "USDT"),
        ];
        let gate = config.exchanges.entry(ExchangeId::Gate).or_default();
        gate.enabled = Some(true);
        gate.private_ws_enabled = true;
        gate.account_id = Some("20011".to_string());
        gate.env_prefix = Some("GATE_FILTER_TEST".to_string());
        std::env::set_var("GATE_FILTER_TEST_API_KEY", "key");
        std::env::set_var("GATE_FILTER_TEST_API_SECRET", "secret");
        let instruments = vec![crate::market::InstrumentMeta::new(
            ExchangeId::Gate,
            crate::market::CanonicalSymbol::new("BTC", "USDT"),
            ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT"),
            "BTC",
            "USDT",
            "USDT",
            crate::market::ContractType::LinearPerpetual,
            1.0,
            0.1,
            0.001,
            0.001,
            5.0,
            1,
            3,
            crate::market::InstrumentStatus::Trading,
        )];

        let parts = build_cross_arb_live_runtime_parts(&config, instruments).unwrap();

        assert_eq!(parts.private_ws_specs.len(), 1);
        assert_eq!(
            parts.private_ws_specs[0].symbols,
            vec![ExchangeSymbol::new(ExchangeId::Gate, "BTC_USDT")]
        );

        std::env::remove_var("GATE_FILTER_TEST_API_KEY");
        std::env::remove_var("GATE_FILTER_TEST_API_SECRET");
    }

    #[test]
    fn runtime_should_map_exchange_symbols_for_ws_specs() {
        let symbol = CanonicalSymbol::new("BTC", "USDT");
        let exchange_symbol = crate::market::exchange_symbol_for(&ExchangeId::Gate, &symbol);

        assert_eq!(exchange_symbol.symbol, "BTC_USDT");
    }
}
