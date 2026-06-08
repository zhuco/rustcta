use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, BalancesRequest, BalancesResponse, BatchCancelOrdersRequest,
    BatchCancelOrdersResponse, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeApiError, ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, FeesRequest,
    FeesResponse, HistoryCapability, OpenOrdersRequest, OpenOrdersResponse, OrderBookCapability,
    OrderBookRequest, OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    StreamRuntimeCapability, SymbolRulesRequest, SymbolRulesResponse, TenantId, TimeInForce,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
mod parser;
mod private;
mod private_parser;
mod public;
mod signing;
mod streams;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
mod transport;

pub use config::AlpacaGatewayConfig;
use transport::AlpacaRest;

#[derive(Clone)]
pub struct AlpacaGatewayAdapter {
    exchange_id: ExchangeId,
    config: AlpacaGatewayConfig,
    rest: AlpacaRest,
}

impl AlpacaGatewayAdapter {
    pub fn new(config: AlpacaGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("alpaca").map_err(parser::validation_error)?;
        let rest = AlpacaRest::new(
            exchange_id.clone(),
            config.broker_rest_base_url.clone(),
            config.market_data_rest_base_url.clone(),
            config.request_timeout_ms,
        )?;
        Ok(Self {
            exchange_id,
            config,
            rest,
        })
    }

    #[cfg(test)]
    pub fn default_public() -> ExchangeApiResult<Self> {
        Self::new(AlpacaGatewayConfig {
            api_key: "test-key".to_string(),
            api_secret: "test-secret".to_string(),
            enabled_private_rest: false,
            ..AlpacaGatewayConfig::default()
        })
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("alpaca adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "alpaca.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_credentials(&self, operation: &'static str) -> ExchangeApiResult<()> {
        if !self.config.has_credentials() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }

    fn ensure_private_rest(&self, operation: &'static str) -> ExchangeApiResult<()> {
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn context_identity(
        &self,
        context: &rustcta_exchange_api::RequestContext,
    ) -> ExchangeApiResult<(TenantId, AccountId)> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "alpaca Broker request requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message:
                        "alpaca Broker request requires context.account_id as broker account id"
                            .to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for AlpacaGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("alpaca broker crypto spot REST adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for AlpacaGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private = self.config.private_rest_enabled();
        let read = self.config.market_data_enabled();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = read;
        capabilities.supports_private_rest = private;
        capabilities.supports_public_streams = read;
        capabilities.supports_private_streams = false;
        capabilities.private_stream_capabilities =
            Some(streams::private_stream_capabilities(false));
        capabilities.supports_symbol_rules = read;
        capabilities.supports_order_book_snapshot = read;
        capabilities.supports_balances = private;
        capabilities.supports_positions = private;
        capabilities.supports_fees = false;
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = false;
        capabilities.supports_quote_market_order = private;
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = false;
        capabilities.supports_post_only = false;
        capabilities.supports_time_in_force = vec![TimeInForce::GTC, TimeInForce::IOC];
        capabilities.supports_order_types = vec![OrderType::Market, OrderType::Limit];
        capabilities.max_order_book_depth = None;
        capabilities.order_book = OrderBookCapability::snapshot_only(None);
        capabilities.refresh_v2_from_legacy_flags();
        apply_alpaca_capabilities_v2(&mut capabilities, read, private);
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
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.unsupported("alpaca.crypto_fee_rate_rest_not_available")
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

    async fn batch_place_orders(
        &self,
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.unsupported("alpaca.batch_place_orders_not_native")
    }

    async fn batch_cancel_orders(
        &self,
        _request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.unsupported("alpaca.batch_cancel_orders_not_native")
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
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
        self.unsupported("alpaca.private_stream_is_broker_sse_not_ws")
    }
}

fn apply_alpaca_capabilities_v2(
    capabilities: &mut ExchangeClientCapabilities,
    read: bool,
    private: bool,
) {
    capabilities.capabilities_v2.public_rest = if read {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("Alpaca market data and assets require API key headers")
    };
    capabilities.capabilities_v2.private_rest = if private {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("Alpaca private REST requires enabled key/secret headers")
    };
    capabilities.capabilities_v2.public_streams = if read {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("Alpaca crypto public WS requires authentication")
    };
    capabilities.capabilities_v2.private_streams =
        CapabilitySupport::unsupported("Alpaca Broker order updates are SSE, not gateway WS");
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: capabilities.capabilities_v2.public_streams.clone(),
        private: capabilities.capabilities_v2.private_streams.clone(),
        supports_subscribe: read,
        supports_unsubscribe: read,
        supports_public_subscribe: read,
        supports_public_unsubscribe: read,
        supports_private_subscribe: false,
        supports_private_unsubscribe: false,
        resync: rustcta_exchange_api::StreamResyncCapability {
            order_book: true,
            balances: true,
            positions: true,
            orders: true,
        },
        auth: rustcta_exchange_api::StreamAuthCapability {
            required: true,
            credential_scopes: vec![CredentialScope::ReadOnly],
            renewal_ms: None,
            uses_listen_key: false,
            requires_relogin_on_reconnect: true,
        },
        ..StreamRuntimeCapability::default()
    };
    capabilities.capabilities_v2.cancel_all_orders = if private {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST disabled")
    };
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("Broker REST fills endpoint not verified");
    capabilities.capabilities_v2.endpoints = vec![
        endpoint(
            "symbol_rules",
            "/v1/assets",
            read,
            CredentialScope::ReadOnly,
        ),
        endpoint(
            "order_book",
            "/v1beta3/crypto/{loc}/latest/orderbooks",
            read,
            CredentialScope::ReadOnly,
        ),
        endpoint(
            "balances",
            "/v1/trading/accounts/{account_id}/account",
            private,
            CredentialScope::ReadOnly,
        ),
        endpoint(
            "positions",
            "/v1/trading/accounts/{account_id}/positions",
            private,
            CredentialScope::ReadOnly,
        ),
        endpoint(
            "place_order",
            "/v1/trading/accounts/{account_id}/orders",
            private,
            CredentialScope::Trade,
        ),
        endpoint(
            "cancel_order",
            "/v1/trading/accounts/{account_id}/orders/{order_id}",
            private,
            CredentialScope::Trade,
        ),
        endpoint(
            "query_order",
            "/v1/trading/accounts/{account_id}/orders/{order_id}",
            private,
            CredentialScope::ReadOnly,
        ),
        endpoint(
            "open_orders",
            "/v1/trading/accounts/{account_id}/orders",
            private,
            CredentialScope::ReadOnly,
        ),
    ];
}

fn endpoint(
    operation: impl Into<String>,
    path: impl Into<String>,
    supported: bool,
    scope: CredentialScope,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.into(),
        support: if supported {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("credentials or private REST disabled")
        },
        market_types: vec![MarketType::Spot],
        transport: EndpointTransport::Rest,
        method: Some("REST".to_string()),
        path: Some(path.into()),
        auth: EndpointAuth::ApiKey,
        credential_scopes: vec![scope],
        rate_limit_bucket: Some("alpaca_key".to_string()),
        weight: Some(1),
        supports_testnet: true,
    }
}
