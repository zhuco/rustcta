use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse, CancelOrderRequest,
    CancelOrderResponse, CapabilitySupport, CredentialScope, ExchangeApiError, ExchangeApiResult,
    ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse, FundingRatesRequest,
    FundingRatesResponse, HeartbeatDirection, HeartbeatPolicy, HistoryCapability,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, PrivateStreamSubscription,
    PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, SymbolRulesRequest, SymbolRulesResponse, TimeInForce,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
mod parser;
mod private;
mod private_parser;
#[cfg(test)]
mod private_tests;
mod public;
#[cfg(test)]
mod public_tests;
mod signing;
#[cfg(test)]
mod stream_tests;
mod streams;
#[cfg(test)]
mod test_support;
mod transport;

pub use config::BitgetGatewayConfig;
use signing::BitgetPrivateCredentials;
use transport::BitgetRest;

const BITGET_PERP_PRODUCT_TYPE: &str = "USDT-FUTURES";
const BITGET_PERP_MARGIN_COIN: &str = "USDT";

#[derive(Clone)]
pub struct BitgetGatewayAdapter {
    exchange_id: ExchangeId,
    config: BitgetGatewayConfig,
    rest: BitgetRest,
}

impl BitgetGatewayAdapter {
    pub fn new(config: BitgetGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("bitget").map_err(validation_error)?;
        let credentials = if config.has_private_credentials() {
            Some(BitgetPrivateCredentials {
                api_key: config.api_key.clone().unwrap_or_default(),
                api_secret: config.api_secret.clone().unwrap_or_default(),
                passphrase: config.passphrase.clone().unwrap_or_default(),
            })
        } else {
            None
        };
        let rest = BitgetRest::new(
            exchange_id.clone(),
            config.rest_base_url.clone(),
            config.request_timeout_ms,
            credentials,
        )?;
        Ok(Self {
            exchange_id,
            config,
            rest,
        })
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("bitget adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitget.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_supported_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitget.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn unsupported_private<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn ensure_private_rest(&self, operation: &'static str) -> ExchangeApiResult<()> {
        if !self.config.private_rest_available() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }
}

#[async_trait]
impl GatewayAdapter for BitgetGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("bitget public REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for BitgetGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private = self.config.private_rest_available();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private;
        capabilities.supports_public_streams = self.config.enabled_public_streams;
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_funding_rates = true;
        capabilities.supports_balances = private;
        capabilities.supports_positions = private;
        capabilities.supports_fees = private;
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_quote_market_order = private;
        capabilities.supports_amend_order = private;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = true;
        capabilities.supports_post_only = true;
        capabilities.supports_time_in_force = vec![
            TimeInForce::GTC,
            TimeInForce::IOC,
            TimeInForce::FOK,
            TimeInForce::GTX,
        ];
        capabilities.supports_order_types = vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::PostOnly,
            OrderType::IOC,
            OrderType::FOK,
        ];
        capabilities.max_order_book_depth = Some(50);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(50));
        capabilities.max_recent_fill_limit = Some(100);
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_streams = if self.config.enabled_public_streams {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("bitget public streams are disabled by configuration")
        };
        capabilities.capabilities_v2.private_streams = CapabilitySupport::rest_fallback(
            "private WebSocket is not wired; reconciliation uses private REST readbacks",
        );
        capabilities.capabilities_v2.stream_runtime.heartbeat_policy = HeartbeatPolicy {
            direction: HeartbeatDirection::ServerPing,
            ping_interval_ms: 0,
            pong_timeout_ms: 30_000,
            stale_message_ms: 60_000,
            requires_pong_payload_echo: false,
        };
        capabilities.capabilities_v2.stream_runtime.public =
            capabilities.capabilities_v2.public_streams.clone();
        capabilities.capabilities_v2.stream_runtime.private =
            capabilities.capabilities_v2.private_streams.clone();
        capabilities
            .capabilities_v2
            .stream_runtime
            .supports_subscribe = self.config.enabled_public_streams;
        capabilities
            .capabilities_v2
            .stream_runtime
            .supports_unsubscribe = self.config.enabled_public_streams;
        capabilities
            .capabilities_v2
            .stream_runtime
            .supports_public_subscribe = self.config.enabled_public_streams;
        capabilities
            .capabilities_v2
            .stream_runtime
            .supports_public_unsubscribe = self.config.enabled_public_streams;
        capabilities
            .capabilities_v2
            .stream_runtime
            .reconnect
            .supported = self.config.enabled_public_streams;
        capabilities
            .capabilities_v2
            .stream_runtime
            .reconnect
            .requires_resubscribe = true;
        capabilities
            .capabilities_v2
            .stream_runtime
            .reconnect
            .preserves_session = false;
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book = true;
        capabilities.capabilities_v2.stream_runtime.resync.balances = private;
        capabilities.capabilities_v2.stream_runtime.resync.orders = private;
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat
            .direction = rustcta_exchange_api::StreamHeartbeatDirection::ServerPing;
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat
            .required = self.config.enabled_public_streams;
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat
            .supported = self.config.enabled_public_streams;
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat
            .timeout_ms = Some(30_000);
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat_policy
            .direction = HeartbeatDirection::ServerPing;
        capabilities.capabilities_v2.batch_place_orders = rustcta_exchange_api::BatchCapability {
            support: CapabilitySupport::composed(
                "planner may compose sequential single-order REST calls",
            ),
            mode: rustcta_exchange_api::BatchExecutionMode::ComposedSequential,
            atomicity: rustcta_exchange_api::BatchAtomicity::NonAtomic,
            max_items: Some(20),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        };
        capabilities.capabilities_v2.batch_cancel_orders = rustcta_exchange_api::BatchCapability {
            support: CapabilitySupport::composed(
                "planner may compose sequential single-cancel REST calls",
            ),
            mode: rustcta_exchange_api::BatchExecutionMode::ComposedSequential,
            atomicity: rustcta_exchange_api::BatchAtomicity::NonAtomic,
            max_items: Some(20),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        };
        capabilities.capabilities_v2.order_history = HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: false,
            supports_until: false,
            supports_limit: false,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: None,
            max_window_ms: None,
        };
        capabilities.capabilities_v2.fills_history = HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: true,
            max_limit: Some(100),
            max_window_ms: None,
        };
        capabilities.capabilities_v2.credential_scopes = if private {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        };
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_impl(request).await
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.get_positions_impl(request).await
    }

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        self.get_symbol_rules_impl(request).await
    }

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        self.get_order_book_impl(request).await
    }

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        self.get_fees_impl(request).await
    }

    async fn get_funding_rates(
        &self,
        request: FundingRatesRequest,
    ) -> ExchangeApiResult<FundingRatesResponse> {
        self.get_funding_rates_impl(request).await
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_order_impl(request).await
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_quote_market_order_impl(request).await
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.cancel_order_impl(request).await
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.amend_order_impl(request).await
    }

    async fn cancel_all_orders(
        &self,
        request: rustcta_exchange_api::CancelAllOrdersRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::CancelAllOrdersResponse> {
        self.cancel_all_orders_impl(request).await
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.query_order_impl(request).await
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.get_open_orders_impl(request).await
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.get_recent_fills_impl(request).await
    }

    async fn subscribe_public_stream(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.subscribe_public_stream_impl(subscription).await
    }

    async fn subscribe_private_stream(
        &self,
        _subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported_private("bitget.subscribe_private_stream")
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
