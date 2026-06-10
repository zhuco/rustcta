use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, AmendOrderRequest, AmendOrderResponse, AuthRenewalKind, AuthRenewalPolicy,
    BalancesRequest, BalancesResponse, BatchAtomicity, BatchCancelOrdersRequest,
    BatchCancelOrdersResponse, BatchCapability, BatchExecutionMode, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability,
    EndpointTransport, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, HeartbeatDirection, HeartbeatPolicy,
    HistoryCapability, OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse,
    OrderListRequest, OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    ReconnectCapability, RequestContext, StreamAuthCapability, StreamHeartbeatDirection,
    StreamResyncCapability, StreamRuntimeCapability, SymbolRulesRequest, SymbolRulesResponse,
    TenantId, TimeInForce,
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

pub use config::KrakenGatewayConfig;
use transport::KrakenRest;

#[derive(Clone)]
pub struct KrakenGatewayAdapter {
    exchange_id: ExchangeId,
    config: KrakenGatewayConfig,
    rest: KrakenRest,
}

impl KrakenGatewayAdapter {
    pub fn new(config: KrakenGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("kraken").map_err(validation_error)?;
        let rest = KrakenRest::new(
            exchange_id.clone(),
            config.spot_rest_base_url.clone(),
            config.futures_rest_base_url.clone(),
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
        Self::new(KrakenGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("kraken adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "kraken.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "kraken.market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn spot_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.spot_private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((
            self.config.spot_api_key.as_deref().unwrap_or_default(),
            self.config.spot_api_secret.as_deref().unwrap_or_default(),
        ))
    }

    fn futures_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.futures_private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((
            self.config.futures_api_key.as_deref().unwrap_or_default(),
            self.config
                .futures_api_secret
                .as_deref()
                .unwrap_or_default(),
        ))
    }

    async fn send_spot_private(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.spot_credentials(operation)?;
        self.rest
            .spot_private_post(endpoint, params, api_key, api_secret)
            .await
    }

    async fn send_futures_private(
        &self,
        operation: &'static str,
        method: reqwest::Method,
        endpoint: &str,
        params: std::collections::BTreeMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.futures_credentials(operation)?;
        self.rest
            .futures_private(method, endpoint, params, api_key, api_secret)
            .await
    }

    fn context_account(
        &self,
        context: &RequestContext,
        operation: &'static str,
    ) -> ExchangeApiResult<(TenantId, AccountId)> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("kraken {operation} requires context.tenant_id"),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("kraken {operation} requires context.account_id"),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for KrakenGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("kraken spot/futures REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for KrakenGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private = self.config.private_rest_enabled();
        let spot_private = self.config.spot_private_rest_enabled();
        let futures_private = self.config.futures_private_rest_enabled();
        let private_streams = spot_private || futures_private;
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = private_streams;
        capabilities.private_stream_capabilities = Some(
            streams::kraken_private_stream_capabilities(private_streams, futures_private),
        );
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private;
        capabilities.supports_positions = self.config.futures_private_rest_enabled();
        capabilities.supports_fees = self.config.spot_private_rest_enabled();
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_batch_place_order = private;
        capabilities.supports_batch_cancel_order = private;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_quote_market_order = self.config.spot_private_rest_enabled();
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
        capabilities.max_order_book_depth = Some(500);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::best_effort_delta(Some(500));
        capabilities.max_recent_fill_limit = Some(1000);
        capabilities.capabilities_v2 =
            kraken_capabilities_v2(private, spot_private, futures_private);
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
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported("kraken.amend_order")
    }

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.unsupported("kraken.place_order_list")
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.batch_place_orders_impl(request).await
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.batch_cancel_orders_impl(request).await
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
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.subscribe_private_stream_impl(subscription).await
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn kraken_capabilities_v2(
    private: bool,
    spot_private: bool,
    futures_private: bool,
) -> rustcta_exchange_api::ExchangeClientCapabilitiesV2 {
    let private_streams = spot_private || futures_private;
    rustcta_exchange_api::ExchangeClientCapabilitiesV2 {
        public_rest: CapabilitySupport::native(),
        private_rest: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "Kraken private REST requires configured Spot or Futures API credentials",
            )
        },
        public_streams: CapabilitySupport::native(),
        private_streams: if private_streams {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "Kraken private streams require Spot token credentials or Futures API credentials",
            )
        },
        stream_runtime: kraken_stream_runtime_capability(private_streams),
        batch_place_orders: BatchCapability {
            support: if private {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "Kraken batch place requires private REST credentials",
                )
            },
            mode: if private {
                BatchExecutionMode::Native
            } else {
                BatchExecutionMode::Unsupported
            },
            atomicity: BatchAtomicity::Partial,
            max_items: Some(15),
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        },
        batch_cancel_orders: BatchCapability {
            support: if private {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "Kraken batch cancel requires private REST credentials",
                )
            },
            mode: if private {
                BatchExecutionMode::Native
            } else {
                BatchExecutionMode::Unsupported
            },
            atomicity: BatchAtomicity::Partial,
            max_items: Some(50),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        },
        cancel_all_orders: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("Kraken cancel-all requires private REST credentials")
        },
        order_history: HistoryCapability {
            support: if private {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "Kraken order reconciliation requires private REST credentials",
                )
            },
            supports_since: false,
            supports_until: false,
            supports_limit: false,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: None,
            max_window_ms: None,
        },
        fills_history: HistoryCapability {
            support: if private {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "Kraken fills history requires private REST credentials",
                )
            },
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: true,
            max_limit: Some(1000),
            max_window_ms: None,
        },
        endpoints: kraken_endpoint_capabilities(private, spot_private, futures_private),
        credential_scopes: if private {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        },
        ..rustcta_exchange_api::ExchangeClientCapabilitiesV2::default()
    }
}

fn kraken_stream_runtime_capability(private_streams: bool) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: if private_streams {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("Kraken private stream credentials are not configured")
        },
        supports_subscribe: true,
        supports_unsubscribe: false,
        heartbeat: rustcta_exchange_api::HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::Bidirectional,
            interval_ms: Some(30_000),
            timeout_ms: Some(10_000),
        },
        reconnect: ReconnectCapability {
            supported: true,
            requires_resubscribe: true,
            preserves_session: false,
            max_reconnect_attempts: None,
        },
        resync: StreamResyncCapability {
            order_book: true,
            balances: true,
            positions: private_streams,
            orders: true,
        },
        auth: StreamAuthCapability {
            required: private_streams,
            credential_scopes: if private_streams {
                vec![CredentialScope::ReadOnly, CredentialScope::Trade]
            } else {
                Vec::new()
            },
            renewal_ms: Some(15 * 60 * 1000),
            uses_listen_key: true,
            requires_relogin_on_reconnect: true,
        },
        heartbeat_policy: HeartbeatPolicy {
            direction: HeartbeatDirection::ApplicationMessage,
            ping_interval_ms: 30_000,
            pong_timeout_ms: 10_000,
            stale_message_ms: 45_000,
            requires_pong_payload_echo: false,
        },
        auth_renewal_policy: AuthRenewalPolicy {
            kind: AuthRenewalKind::TokenRefresh,
            renew_before_expiry_ms: 60_000,
            renewal_interval_ms: Some(15 * 60 * 1000),
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        },
        auth_renewal: AuthRenewalPolicy {
            kind: AuthRenewalKind::TokenRefresh,
            renew_before_expiry_ms: 60_000,
            renewal_interval_ms: Some(15 * 60 * 1000),
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        },
        ..StreamRuntimeCapability::default()
    }
}

fn kraken_endpoint_capabilities(
    _private: bool,
    spot_private: bool,
    futures_private: bool,
) -> Vec<EndpointCapability> {
    let mut endpoints = vec![
        endpoint(
            "get_symbol_rules",
            true,
            vec![MarketType::Spot],
            "GET",
            "/0/public/AssetPairs",
            EndpointAuth::None,
            vec![],
            Some("spot_public"),
        ),
        endpoint(
            "get_symbol_rules",
            true,
            vec![MarketType::Perpetual],
            "GET",
            "/derivatives/api/v3/instruments",
            EndpointAuth::None,
            vec![],
            Some("futures_public"),
        ),
        endpoint(
            "get_order_book",
            true,
            vec![MarketType::Spot],
            "GET",
            "/0/public/Depth",
            EndpointAuth::None,
            vec![],
            Some("spot_public"),
        ),
        endpoint(
            "get_order_book",
            true,
            vec![MarketType::Perpetual],
            "GET",
            "/derivatives/api/v3/orderbook",
            EndpointAuth::None,
            vec![],
            Some("futures_public"),
        ),
        endpoint(
            "get_balances",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/BalanceEx",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("spot_private"),
        ),
        endpoint(
            "get_balances",
            futures_private,
            vec![MarketType::Perpetual],
            "GET",
            "/derivatives/api/v3/accounts",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("futures_private"),
        ),
        endpoint(
            "get_positions",
            futures_private,
            vec![MarketType::Perpetual],
            "GET",
            "/derivatives/api/v3/openpositions",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("futures_private"),
        ),
        endpoint(
            "get_fees",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/TradeVolume",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("spot_private"),
        ),
        endpoint(
            "place_order",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/AddOrder",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("spot_order"),
        ),
        endpoint(
            "place_order",
            futures_private,
            vec![MarketType::Perpetual],
            "POST",
            "/derivatives/api/v3/sendorder",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("futures_order"),
        ),
        endpoint(
            "place_quote_market_order",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/AddOrder",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("spot_order"),
        ),
        endpoint(
            "cancel_order",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/CancelOrder",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("spot_order"),
        ),
        endpoint(
            "cancel_order",
            futures_private,
            vec![MarketType::Perpetual],
            "POST",
            "/derivatives/api/v3/cancelorder",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("futures_order"),
        ),
        endpoint(
            "cancel_all_orders",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/CancelAll",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("spot_order"),
        ),
        endpoint(
            "cancel_all_orders",
            futures_private,
            vec![MarketType::Perpetual],
            "POST",
            "/derivatives/api/v3/cancelallorders",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("futures_order"),
        ),
        endpoint(
            "query_order",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/QueryOrders",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("spot_private"),
        ),
        endpoint(
            "query_order",
            futures_private,
            vec![MarketType::Perpetual],
            "GET",
            "/derivatives/api/v3/orders",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("futures_private"),
        ),
        endpoint(
            "get_open_orders",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/OpenOrders",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("spot_private"),
        ),
        endpoint(
            "get_open_orders",
            futures_private,
            vec![MarketType::Perpetual],
            "GET",
            "/derivatives/api/v3/openorders",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("futures_private"),
        ),
        endpoint(
            "get_recent_fills",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/TradesHistory",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("spot_private"),
        ),
        endpoint(
            "get_recent_fills",
            futures_private,
            vec![MarketType::Perpetual],
            "GET",
            "/derivatives/api/v3/fills",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("futures_private"),
        ),
        endpoint(
            "batch_place_orders",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/AddOrderBatch",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("spot_order"),
        ),
        endpoint(
            "batch_place_orders",
            futures_private,
            vec![MarketType::Perpetual],
            "POST",
            "/derivatives/api/v3/batchorder",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("futures_order"),
        ),
        endpoint(
            "batch_cancel_orders",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/CancelOrderBatch",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("spot_order"),
        ),
        endpoint(
            "batch_cancel_orders",
            futures_private,
            vec![MarketType::Perpetual],
            "POST",
            "/derivatives/api/v3/batchorder",
            EndpointAuth::Hmac,
            vec![CredentialScope::Trade],
            Some("futures_order"),
        ),
        endpoint(
            "get_websockets_token",
            spot_private,
            vec![MarketType::Spot],
            "POST",
            "/0/private/GetWebSocketsToken",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            Some("spot_private"),
        ),
    ];
    endpoints.push(EndpointCapability {
        operation: "subscribe_public_stream".to_string(),
        support: CapabilitySupport::native(),
        market_types: vec![MarketType::Spot, MarketType::Perpetual],
        transport: EndpointTransport::WebSocket,
        method: None,
        path: Some("wss://ws.kraken.com/v2; wss://futures.kraken.com/ws/v1".to_string()),
        auth: EndpointAuth::None,
        credential_scopes: Vec::new(),
        rate_limit_bucket: Some("ws_public".to_string()),
        weight: Some(1),
        supports_testnet: false,
    });
    endpoints.push(EndpointCapability {
        operation: "subscribe_private_stream".to_string(),
        support: if spot_private || futures_private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("Kraken private stream credentials are not configured")
        },
        market_types: vec![MarketType::Spot, MarketType::Perpetual],
        transport: EndpointTransport::WebSocket,
        method: None,
        path: Some("wss://ws-auth.kraken.com/v2; wss://futures.kraken.com/ws/v1".to_string()),
        auth: EndpointAuth::ListenKey,
        credential_scopes: vec![CredentialScope::ReadOnly, CredentialScope::Trade],
        rate_limit_bucket: Some("ws_private".to_string()),
        weight: Some(1),
        supports_testnet: false,
    });
    endpoints
}

fn endpoint(
    operation: &str,
    supported: bool,
    market_types: Vec<MarketType>,
    method: &str,
    path: &str,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: Option<&str>,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support: if supported {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(format!(
                "Kraken {operation} credentials are not configured"
            ))
        },
        market_types,
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth,
        credential_scopes,
        rate_limit_bucket: rate_limit_bucket.map(str::to_string),
        weight: Some(1),
        supports_testnet: false,
    }
}
