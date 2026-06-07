//! Global exchange adapter registry.
//!
//! Strategy code should depend on this module for exchange construction instead
//! of embedding venue-specific match blocks in each strategy runtime.

use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::core::config::{ApiKeys, Config as CoreExchangeConfig};
use crate::core::exchange::Exchange;
use crate::exchanges::config::{
    position_mode_from_config_value, ExchangeRegistryConfig, ExchangeRuntimeSettings,
};
use crate::exchanges::gateway::{ExchangeGateway, ExchangeGatewayCapabilities, GatewayKind};
use crate::exchanges::market_adapters::{
    BinanceMarketAdapter, BitgetMarketAdapter, BybitMarketAdapter, GateMarketAdapter,
    HtxMarketAdapter, MexcMarketAdapter, OkxMarketAdapter,
};
use crate::exchanges::private_perp::{
    private_perp_trading_capabilities, PrivatePerpExchange, PrivateRestAuth, PrivateWsAuth,
};
use crate::exchanges::trading_adapters::{
    private_perp_trading_adapter_for_with_base_url_and_instruments, ExchangeTradingAdapter,
};
use crate::exchanges::{BinanceExchange, OkxExchange};
use crate::execution::{PositionMode, TradingAdapter, TradingCapabilities};
use crate::market::{ExchangeId, InstrumentMeta, MarketCapabilities, MarketDataAdapter};

pub fn market_adapter(exchange: &ExchangeId) -> Option<Box<dyn MarketDataAdapter + Send + Sync>> {
    gateway_for_exchange(exchange).and_then(|gateway| gateway.market_data())
}

pub fn gateway_for_exchange(exchange: &ExchangeId) -> Option<Box<dyn ExchangeGateway>> {
    match exchange {
        ExchangeId::Binance => Some(Box::new(PrivatePerpExchangeGateway::new(
            PrivatePerpExchange::Binance,
            private_perp_market_adapter,
        ))),
        ExchangeId::Okx => Some(Box::new(PrivatePerpExchangeGateway::new(
            PrivatePerpExchange::Okx,
            private_perp_market_adapter,
        ))),
        ExchangeId::Bitget => Some(Box::new(PrivatePerpExchangeGateway::new(
            PrivatePerpExchange::Bitget,
            private_perp_market_adapter,
        ))),
        ExchangeId::Gate => Some(Box::new(PrivatePerpExchangeGateway::new(
            PrivatePerpExchange::Gate,
            private_perp_market_adapter,
        ))),
        ExchangeId::Bybit => Some(Box::new(PrivatePerpExchangeGateway::new(
            PrivatePerpExchange::Bybit,
            private_perp_market_adapter,
        ))),
        ExchangeId::Mexc => Some(Box::new(PrivatePerpExchangeGateway::new(
            PrivatePerpExchange::Mexc,
            private_perp_market_adapter,
        ))),
        ExchangeId::Htx => Some(Box::new(PrivatePerpExchangeGateway::new(
            PrivatePerpExchange::Htx,
            private_perp_market_adapter,
        ))),
        ExchangeId::CoinEx | ExchangeId::KuCoin => None,
        ExchangeId::Other(_) => None,
    }
}

pub fn gateway_capabilities(exchange: &ExchangeId) -> Option<ExchangeGatewayCapabilities> {
    gateway_for_exchange(exchange).map(|gateway| gateway.capabilities())
}

pub fn build_trading_adapter_for_exchange<C>(
    config: &C,
    exchange: &ExchangeId,
) -> Result<Arc<dyn TradingAdapter>>
where
    C: ExchangeRegistryConfig,
{
    build_trading_adapter_for_exchange_with_instruments(config, exchange, Vec::new())
}

pub fn build_trading_adapter_for_exchange_with_instruments<C>(
    config: &C,
    exchange: &ExchangeId,
    instruments: impl IntoIterator<Item = InstrumentMeta>,
) -> Result<Arc<dyn TradingAdapter>>
where
    C: ExchangeRegistryConfig,
{
    let instruments = instruments.into_iter().collect::<Vec<_>>();
    let gateway = gateway_for_exchange(exchange)
        .ok_or_else(|| anyhow!("{exchange} trading adapter is not registered"))?;
    let auth = private_rest_auth_for_exchange(config, exchange)?;
    gateway.trading(
        auth,
        configured_position_mode(config, exchange),
        instruments,
        config
            .exchange_runtime()
            .get(exchange)
            .and_then(|runtime| runtime.private_rest_base_url()),
    )
}

pub fn private_rest_auth_for_exchange<C>(
    config: &C,
    exchange: &ExchangeId,
) -> Result<PrivateRestAuth>
where
    C: ExchangeRegistryConfig,
{
    let api_keys = api_keys_for_exchange(config, exchange)?;
    Ok(PrivateRestAuth {
        api_key: api_keys.api_key,
        api_secret: api_keys.api_secret,
        passphrase: api_keys.passphrase,
        demo_trading: config
            .exchange_runtime()
            .get(exchange)
            .map(|runtime| runtime.demo_trading())
            .unwrap_or(false),
    })
}

pub fn private_ws_auth_for_exchange<C>(config: &C, exchange: &ExchangeId) -> Result<PrivateWsAuth>
where
    C: ExchangeRegistryConfig,
{
    let prefix = api_key_env_prefix_for_exchange(config, exchange);
    let api_keys = ApiKeys::from_env(&prefix).map_err(|err| anyhow!(err.to_string()))?;
    let configured_account_id = config
        .exchange_runtime()
        .get(exchange)
        .and_then(|runtime| runtime.account_id().map(ToOwned::to_owned));
    let account_id = private_ws_account_id_for_exchange(exchange, &prefix, configured_account_id)
        .or_else(|| gate_account_id_from_env(exchange))
        .or(api_keys.memo);
    if *exchange == ExchangeId::Gate {
        let account_id = account_id.as_deref().ok_or_else(|| {
            anyhow!("gate private websocket requires exchanges.gate.account_id to be set to the numeric Gate user id")
        })?;
        if !account_id.chars().all(|ch| ch.is_ascii_digit()) {
            anyhow::bail!(
                "gate private websocket account_id must be the numeric Gate user id; current value is non-numeric"
            );
        }
    }
    Ok(PrivateWsAuth {
        api_key: api_keys.api_key,
        api_secret: api_keys.api_secret,
        passphrase: api_keys.passphrase,
        account_id,
        demo_trading: config
            .exchange_runtime()
            .get(exchange)
            .map(|runtime| runtime.demo_trading())
            .unwrap_or(false),
    })
}

fn gate_account_id_from_env(exchange: &ExchangeId) -> Option<String> {
    if *exchange != ExchangeId::Gate {
        return None;
    }
    [
        "GATE_ACCOUNT_ID",
        "GATEIO_ACCOUNT_ID",
        "GATE_USER_ID",
        "GATEIO_USER_ID",
    ]
    .iter()
    .find_map(|key| {
        std::env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

pub fn configured_position_mode<C>(config: &C, exchange: &ExchangeId) -> PositionMode
where
    C: ExchangeRegistryConfig,
{
    config
        .exchange_runtime()
        .get(exchange)
        .and_then(|runtime| runtime.position_mode_text())
        .map(position_mode_from_config_value)
        .unwrap_or(PositionMode::OneWay)
}

pub fn private_perp_exchange(exchange: &ExchangeId) -> Option<PrivatePerpExchange> {
    match exchange {
        ExchangeId::Binance => Some(PrivatePerpExchange::Binance),
        ExchangeId::Okx => Some(PrivatePerpExchange::Okx),
        ExchangeId::Bitget => Some(PrivatePerpExchange::Bitget),
        ExchangeId::Gate => Some(PrivatePerpExchange::Gate),
        ExchangeId::Bybit => Some(PrivatePerpExchange::Bybit),
        ExchangeId::Mexc => Some(PrivatePerpExchange::Mexc),
        ExchangeId::Htx => Some(PrivatePerpExchange::Htx),
        _ => None,
    }
}

pub fn build_core_exchange_for_exchange<C>(
    config: &C,
    exchange: &ExchangeId,
) -> Result<Arc<dyn Exchange>>
where
    C: ExchangeRegistryConfig,
{
    let api_keys = api_keys_for_exchange(config, exchange)?;
    match exchange {
        ExchangeId::Binance => Ok(Arc::new(BinanceExchange::new(
            CoreExchangeConfig {
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
            CoreExchangeConfig {
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

struct CoreExchangeGateway {
    exchange: ExchangeId,
    market_adapter: fn(&ExchangeId) -> Option<Box<dyn MarketDataAdapter + Send + Sync>>,
    trading_capabilities: fn(&ExchangeId) -> TradingCapabilities,
}

impl CoreExchangeGateway {
    fn new(
        exchange: ExchangeId,
        market_adapter: fn(&ExchangeId) -> Option<Box<dyn MarketDataAdapter + Send + Sync>>,
        trading_capabilities: fn(&ExchangeId) -> TradingCapabilities,
    ) -> Self {
        Self {
            exchange,
            market_adapter,
            trading_capabilities,
        }
    }
}

impl ExchangeGateway for CoreExchangeGateway {
    fn exchange(&self) -> ExchangeId {
        self.exchange.clone()
    }

    fn capabilities(&self) -> ExchangeGatewayCapabilities {
        ExchangeGatewayCapabilities {
            exchange: self.exchange.clone(),
            kind: GatewayKind::Core,
            market: (self.market_adapter)(&self.exchange).map(|adapter| adapter.capabilities()),
            trading: Some((self.trading_capabilities)(&self.exchange)),
            supports_private_rest: true,
            supports_private_ws: matches!(self.exchange, ExchangeId::Binance),
            supports_dual_side_position: matches!(self.exchange, ExchangeId::Binance),
        }
    }

    fn market_data(&self) -> Option<Box<dyn MarketDataAdapter + Send + Sync>> {
        (self.market_adapter)(&self.exchange)
    }

    fn trading(
        &self,
        auth: PrivateRestAuth,
        position_mode: PositionMode,
        instruments: Vec<InstrumentMeta>,
        _private_rest_base_url: Option<&str>,
    ) -> Result<Arc<dyn TradingAdapter>> {
        let core_exchange = build_core_exchange_with_auth(&self.exchange, auth)?;
        Ok(Arc::new(
            ExchangeTradingAdapter::new(self.exchange.clone(), core_exchange)
                .with_position_mode(position_mode)
                .with_instruments(instruments),
        ))
    }
}

struct PrivatePerpExchangeGateway {
    exchange: PrivatePerpExchange,
    market_adapter: fn(&ExchangeId) -> Option<Box<dyn MarketDataAdapter + Send + Sync>>,
}

impl PrivatePerpExchangeGateway {
    fn new(
        exchange: PrivatePerpExchange,
        market_adapter: fn(&ExchangeId) -> Option<Box<dyn MarketDataAdapter + Send + Sync>>,
    ) -> Self {
        Self {
            exchange,
            market_adapter,
        }
    }
}

impl ExchangeGateway for PrivatePerpExchangeGateway {
    fn exchange(&self) -> ExchangeId {
        self.exchange.exchange_id()
    }

    fn capabilities(&self) -> ExchangeGatewayCapabilities {
        let exchange = self.exchange();
        let trading = private_perp_trading_capabilities(exchange.clone());
        let supports_dual_side_position = trading.supports_hedge_mode;
        ExchangeGatewayCapabilities {
            exchange: exchange.clone(),
            kind: GatewayKind::PrivatePerp,
            market: (self.market_adapter)(&exchange).map(|adapter| adapter.capabilities()),
            trading: Some(trading),
            supports_private_rest: true,
            supports_private_ws: true,
            supports_dual_side_position,
        }
    }

    fn market_data(&self) -> Option<Box<dyn MarketDataAdapter + Send + Sync>> {
        (self.market_adapter)(&self.exchange())
    }

    fn trading(
        &self,
        auth: PrivateRestAuth,
        position_mode: PositionMode,
        instruments: Vec<InstrumentMeta>,
        private_rest_base_url: Option<&str>,
    ) -> Result<Arc<dyn TradingAdapter>> {
        private_perp_trading_adapter_for_with_base_url_and_instruments(
            self.exchange,
            auth,
            position_mode,
            private_rest_base_url,
            instruments,
        )
    }

    fn private_perp_exchange(&self) -> Option<PrivatePerpExchange> {
        Some(self.exchange)
    }
}

fn core_market_adapter(exchange: &ExchangeId) -> Option<Box<dyn MarketDataAdapter + Send + Sync>> {
    match exchange {
        ExchangeId::Binance => Some(Box::new(BinanceMarketAdapter)),
        ExchangeId::Okx => Some(Box::new(OkxMarketAdapter)),
        _ => None,
    }
}

fn private_perp_market_adapter(
    exchange: &ExchangeId,
) -> Option<Box<dyn MarketDataAdapter + Send + Sync>> {
    match exchange {
        ExchangeId::Binance => Some(Box::new(BinanceMarketAdapter)),
        ExchangeId::Okx => Some(Box::new(OkxMarketAdapter)),
        ExchangeId::Bitget => Some(Box::new(BitgetMarketAdapter)),
        ExchangeId::Gate => Some(Box::new(GateMarketAdapter)),
        ExchangeId::Bybit => Some(Box::new(BybitMarketAdapter)),
        ExchangeId::Mexc => Some(Box::new(MexcMarketAdapter)),
        ExchangeId::Htx => Some(Box::new(HtxMarketAdapter)),
        _ => None,
    }
}

fn core_trading_capabilities(exchange: &ExchangeId) -> TradingCapabilities {
    match exchange {
        ExchangeId::Binance | ExchangeId::Okx => TradingCapabilities::default(),
        _ => TradingCapabilities {
            supports_market_orders: false,
            supports_limit_orders: false,
            supports_post_only: false,
            supports_ioc: false,
            supports_fok: false,
            supports_reduce_only: false,
            supports_hedge_mode: false,
            supports_client_order_id: false,
            supports_leverage: false,
            supports_position_mode_change: false,
            supports_close_position: false,
            supports_countdown_cancel_all: false,
            supports_batch_place_orders: false,
        },
    }
}

fn build_core_exchange_with_auth(
    exchange: &ExchangeId,
    auth: PrivateRestAuth,
) -> Result<Arc<dyn Exchange>> {
    let api_keys = ApiKeys {
        api_key: auth.api_key,
        api_secret: auth.api_secret,
        passphrase: auth.passphrase,
        memo: None,
    };
    match exchange {
        ExchangeId::Binance => Ok(Arc::new(BinanceExchange::new(
            CoreExchangeConfig {
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
            CoreExchangeConfig {
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

fn api_keys_for_exchange<C>(config: &C, exchange: &ExchangeId) -> Result<ApiKeys>
where
    C: ExchangeRegistryConfig,
{
    let prefix = api_key_env_prefix_for_exchange(config, exchange);
    ApiKeys::from_env(&prefix).map_err(|err| anyhow!(err.to_string()))
}

fn api_key_env_prefix_for_exchange<C>(config: &C, exchange: &ExchangeId) -> String
where
    C: ExchangeRegistryConfig,
{
    let runtime = config.exchange_runtime().get(exchange);
    let default_prefix = exchange.as_str().to_ascii_uppercase();
    let configured_prefix = runtime.and_then(|runtime| runtime.env_prefix());
    let account_prefix = runtime
        .and_then(|runtime| runtime.account_id())
        .and_then(|account_id| account_env_prefix(exchange, account_id));
    let custom_prefix = configured_prefix
        .map(str::trim)
        .filter(|prefix| !prefix.is_empty())
        .filter(|prefix| !prefix.eq_ignore_ascii_case(&default_prefix))
        .map(str::to_string);
    custom_prefix
        .or(account_prefix)
        .or_else(|| configured_prefix.map(str::to_string))
        .unwrap_or(default_prefix)
}

fn account_env_prefix(exchange: &ExchangeId, account_id: &str) -> Option<String> {
    let account = account_id
        .trim()
        .to_ascii_uppercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    let account = account.trim_matches('_').to_string();
    if account.is_empty() || account == "DEFAULT" || account == "MAIN" {
        return None;
    }
    Some(format!(
        "{}__{}_",
        exchange.as_str().to_ascii_uppercase(),
        account
    ))
}

fn private_ws_account_id_for_exchange(
    exchange: &ExchangeId,
    prefix: &str,
    configured_account_id: Option<String>,
) -> Option<String> {
    if *exchange == ExchangeId::Gate {
        if configured_account_id
            .as_deref()
            .is_some_and(|value| value.chars().all(|ch| ch.is_ascii_digit()))
        {
            return configured_account_id;
        }
        return [
            format!("{prefix}_ACCOUNT_ID"),
            format!("{prefix}_USER_ID"),
            format!("{prefix}_MEMO"),
            format!("{prefix}_API_MEMO"),
        ]
        .into_iter()
        .find_map(env_value);
    }
    configured_account_id.filter(|value| !value.trim().is_empty())
}

fn env_value(key: String) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    use super::*;
    use crate::exchanges::private_perp::PrivateWsRunConfig;

    #[derive(Default)]
    struct TestRegistryConfig {
        exchanges: HashMap<ExchangeId, TestRuntimeConfig>,
    }

    #[derive(Default)]
    struct TestRuntimeConfig {
        env_prefix: Option<String>,
        account_id: Option<String>,
    }

    impl ExchangeRegistryConfig for TestRegistryConfig {
        type Entry = TestRuntimeConfig;

        fn exchange_runtime(&self) -> &HashMap<ExchangeId, Self::Entry> {
            &self.exchanges
        }
    }

    impl ExchangeRuntimeSettings for TestRuntimeConfig {
        fn is_disabled(&self) -> bool {
            false
        }

        fn env_prefix(&self) -> Option<&str> {
            self.env_prefix.as_deref()
        }

        fn account_id(&self) -> Option<&str> {
            self.account_id.as_deref()
        }

        fn demo_trading(&self) -> bool {
            false
        }

        fn private_rest_base_url(&self) -> Option<&str> {
            None
        }

        fn private_ws_url(&self) -> Option<&str> {
            None
        }

        fn private_ws_enabled(&self) -> bool {
            false
        }

        fn private_ws_run(&self) -> PrivateWsRunConfig {
            PrivateWsRunConfig::default()
        }

        fn position_mode_text(&self) -> Option<&str> {
            None
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn gateway_registry_should_expose_market_adapters_for_supported_exchanges() {
        for exchange in [
            ExchangeId::Binance,
            ExchangeId::Okx,
            ExchangeId::Bitget,
            ExchangeId::Gate,
            ExchangeId::Bybit,
            ExchangeId::Mexc,
            ExchangeId::Htx,
        ] {
            let adapter = market_adapter(&exchange)
                .unwrap_or_else(|| panic!("{exchange} should have a market adapter"));

            assert_eq!(adapter.exchange(), exchange);
        }
    }

    #[test]
    fn gateway_registry_should_expose_private_perp_capabilities() {
        for (exchange, private_exchange) in [
            (ExchangeId::Binance, PrivatePerpExchange::Binance),
            (ExchangeId::Okx, PrivatePerpExchange::Okx),
            (ExchangeId::Bitget, PrivatePerpExchange::Bitget),
            (ExchangeId::Gate, PrivatePerpExchange::Gate),
            (ExchangeId::Bybit, PrivatePerpExchange::Bybit),
            (ExchangeId::Mexc, PrivatePerpExchange::Mexc),
            (ExchangeId::Htx, PrivatePerpExchange::Htx),
        ] {
            let gateway = gateway_for_exchange(&exchange)
                .unwrap_or_else(|| panic!("{exchange} should have a gateway"));
            let capabilities = gateway.capabilities();

            assert_eq!(gateway.private_perp_exchange(), Some(private_exchange));
            assert_eq!(capabilities.kind, GatewayKind::PrivatePerp);
            assert!(capabilities.supports_private_rest);
            assert!(capabilities.supports_private_ws);
            assert!(capabilities.trading.is_some());
        }
    }

    #[test]
    fn gateway_registry_should_keep_spot_only_exchanges_out_of_private_perp() {
        for exchange in [ExchangeId::CoinEx, ExchangeId::KuCoin] {
            assert!(gateway_for_exchange(&exchange).is_none(), "{exchange}");
            assert!(gateway_capabilities(&exchange).is_none(), "{exchange}");
            assert_eq!(private_perp_exchange(&exchange), None, "{exchange}");

            let err =
                match build_trading_adapter_for_exchange(&TestRegistryConfig::default(), &exchange)
                {
                    Ok(_) => panic!("{exchange} should not have a trading adapter"),
                    Err(err) => err,
                };
            assert!(
                err.to_string()
                    .contains("trading adapter is not registered"),
                "{exchange}: {err}"
            );
        }
    }

    #[test]
    fn gateway_registry_should_keep_core_exchange_builders_separate() {
        let _guard = env_lock().lock().unwrap();
        for key in ["OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"] {
            std::env::remove_var(key);
        }
        std::env::set_var("OKX_API_KEY", "key");
        std::env::set_var("OKX_API_SECRET", "secret");
        std::env::set_var("OKX_PASSPHRASE", "passphrase");

        assert!(
            build_core_exchange_for_exchange(&TestRegistryConfig::default(), &ExchangeId::Okx)
                .is_ok()
        );

        for key in ["OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn gate_private_ws_auth_should_read_account_id_from_env_store() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        for key in [
            "GATE_API_KEY",
            "GATE_API_SECRET",
            "GATE_ACCOUNT_ID",
            "GATEIO_ACCOUNT_ID",
            "GATE_USER_ID",
            "GATEIO_USER_ID",
            "GATE_MEMO",
            "GATE_API_MEMO",
        ] {
            std::env::remove_var(key);
        }
        std::env::set_var("GATE_API_KEY", "key");
        std::env::set_var("GATE_API_SECRET", "secret");
        std::env::set_var("GATE_ACCOUNT_ID", "20011");

        let auth = private_ws_auth_for_exchange(&TestRegistryConfig::default(), &ExchangeId::Gate)
            .expect("gate auth should load account id from env");

        assert_eq!(auth.account_id.as_deref(), Some("20011"));

        for key in ["GATE_API_KEY", "GATE_API_SECRET", "GATE_ACCOUNT_ID"] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn private_rest_auth_should_read_selected_account_scoped_env_keys() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        for key in [
            "BITGET_API_KEY",
            "BITGET_API_SECRET",
            "BITGET_PASSPHRASE",
            "BITGET__HEDGE__API_KEY",
            "BITGET__HEDGE__API_SECRET",
            "BITGET__HEDGE__PASSPHRASE",
        ] {
            std::env::remove_var(key);
        }
        let mut config = TestRegistryConfig::default();
        config.exchanges.insert(
            ExchangeId::Bitget,
            TestRuntimeConfig {
                env_prefix: Some("BITGET".to_string()),
                account_id: Some("hedge".to_string()),
            },
        );
        std::env::set_var("BITGET__HEDGE__API_KEY", "account-key");
        std::env::set_var("BITGET__HEDGE__API_SECRET", "account-secret");
        std::env::set_var("BITGET__HEDGE__PASSPHRASE", "account-pass");

        let auth = private_rest_auth_for_exchange(&config, &ExchangeId::Bitget)
            .expect("selected Bitget account credentials should load");

        assert_eq!(auth.api_key, "account-key");
        assert_eq!(auth.api_secret, "account-secret");
        assert_eq!(auth.passphrase.as_deref(), Some("account-pass"));

        for key in [
            "BITGET__HEDGE__API_KEY",
            "BITGET__HEDGE__API_SECRET",
            "BITGET__HEDGE__PASSPHRASE",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn private_ws_auth_should_read_gate_selected_account_id_from_account_scoped_env_keys() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        for key in [
            "GATE_API_KEY",
            "GATE_API_SECRET",
            "GATE_ACCOUNT_ID",
            "GATE_USER_ID",
            "GATE__HEDGE__API_KEY",
            "GATE__HEDGE__API_SECRET",
            "GATE__HEDGE__ACCOUNT_ID",
        ] {
            std::env::remove_var(key);
        }
        let mut config = TestRegistryConfig::default();
        config.exchanges.insert(
            ExchangeId::Gate,
            TestRuntimeConfig {
                env_prefix: Some("GATE".to_string()),
                account_id: Some("hedge".to_string()),
            },
        );
        std::env::set_var("GATE__HEDGE__API_KEY", "gate-account-key");
        std::env::set_var("GATE__HEDGE__API_SECRET", "gate-account-secret");
        std::env::set_var("GATE__HEDGE__ACCOUNT_ID", "20011");

        let auth = private_ws_auth_for_exchange(&config, &ExchangeId::Gate)
            .expect("selected Gate account websocket credentials should load");

        assert_eq!(auth.api_key, "gate-account-key");
        assert_eq!(auth.api_secret, "gate-account-secret");
        assert_eq!(auth.account_id.as_deref(), Some("20011"));

        for key in [
            "GATE__HEDGE__API_KEY",
            "GATE__HEDGE__API_SECRET",
            "GATE__HEDGE__ACCOUNT_ID",
        ] {
            std::env::remove_var(key);
        }
    }
}
