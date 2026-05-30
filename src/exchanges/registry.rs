//! Global exchange adapter registry.
//!
//! Strategy code should depend on this module for exchange construction instead
//! of embedding venue-specific match blocks in each strategy runtime.

use std::sync::Arc;

use anyhow::{anyhow, Result};

use crate::core::config::{ApiKeys, Config as CoreExchangeConfig};
use crate::core::exchange::Exchange;
use crate::exchanges::adapters::{
    private_perp_trading_adapter_for_with_base_url_and_instruments,
    private_perp_trading_capabilities, BinanceMarketAdapter, BitgetMarketAdapter,
    BybitMarketAdapter, ExchangeTradingAdapter, GateMarketAdapter, HtxMarketAdapter,
    MexcMarketAdapter, OkxMarketAdapter, PrivatePerpExchange, PrivateRestAuth, PrivateWsAuth,
};
use crate::exchanges::config::{
    position_mode_from_config_value, ExchangeRegistryConfig, ExchangeRuntimeSettings,
};
use crate::exchanges::gateway::{ExchangeGateway, ExchangeGatewayCapabilities, GatewayKind};
use crate::exchanges::{BinanceExchange, OkxExchange};
use crate::execution::{PositionMode, TradingAdapter, TradingCapabilities};
use crate::market::{ExchangeId, InstrumentMeta, MarketCapabilities, MarketDataAdapter};

pub fn market_adapter(exchange: &ExchangeId) -> Option<Box<dyn MarketDataAdapter + Send + Sync>> {
    gateway_for_exchange(exchange).and_then(|gateway| gateway.market_data())
}

pub fn gateway_for_exchange(exchange: &ExchangeId) -> Option<Box<dyn ExchangeGateway>> {
    match exchange {
        ExchangeId::Binance => Some(Box::new(CoreExchangeGateway::new(
            ExchangeId::Binance,
            core_market_adapter,
            core_trading_capabilities,
        ))),
        ExchangeId::Okx => Some(Box::new(CoreExchangeGateway::new(
            ExchangeId::Okx,
            core_market_adapter,
            core_trading_capabilities,
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
    let api_keys = api_keys_for_exchange(config, exchange)?;
    let account_id = config
        .exchange_runtime()
        .get(exchange)
        .and_then(|runtime| runtime.account_id().map(ToOwned::to_owned))
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
    let prefix = config
        .exchange_runtime()
        .get(exchange)
        .and_then(|runtime| runtime.env_prefix())
        .unwrap_or_else(|| exchange.as_str());
    ApiKeys::from_env(prefix).map_err(|err| anyhow!(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn gateway_registry_should_keep_core_gateway_mapping() {
        for exchange in [ExchangeId::Binance, ExchangeId::Okx] {
            let gateway = gateway_for_exchange(&exchange)
                .unwrap_or_else(|| panic!("{exchange} should have a core gateway"));
            let capabilities = gateway.capabilities();

            assert_eq!(capabilities.kind, GatewayKind::Core);
            assert!(capabilities.supports_private_rest);
            assert!(capabilities.market.is_some());
            assert!(capabilities.trading.is_some());
        }
    }
}
