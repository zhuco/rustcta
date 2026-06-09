use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, AuthRenewalKind, AuthRenewalPolicy, BalancesRequest,
    BalancesResponse, BatchAtomicity, BatchCapability, BatchExecutionMode, CancelOrderRequest,
    CancelOrderResponse, CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability,
    EndpointTransport, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, ExchangeClientCapabilitiesV2, FeesRequest, FeesResponse,
    HeartbeatCapability, HeartbeatDirection, HeartbeatPolicy, HistoryCapability, OpenOrdersRequest,
    OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, ReconnectCapability,
    StreamAuthCapability, StreamHeartbeatDirection, StreamResyncCapability,
    StreamRuntimeCapability, SymbolRulesRequest, SymbolRulesResponse,
};
use rustcta_types::{ExchangeId, MarketType};

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

pub use config::BitmexGatewayConfig;
use signing::BitmexPrivateCredentials;
use transport::BitmexRest;

#[derive(Clone)]
pub struct BitmexGatewayAdapter {
    exchange_id: ExchangeId,
    config: BitmexGatewayConfig,
    rest: BitmexRest,
}

impl BitmexGatewayAdapter {
    pub fn new(config: BitmexGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("bitmex").map_err(validation_error)?;
        let credentials = BitmexPrivateCredentials::from_config(&config);
        let rest = BitmexRest::new(
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
                message: format!("bitmex adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitmex.unsupported_market_type",
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

    fn ensure_optional_supported_market_type(
        &self,
        market_type: Option<MarketType>,
    ) -> ExchangeApiResult<()> {
        if let Some(market_type) = market_type {
            self.ensure_supported_market_type(market_type)?;
        }
        Ok(())
    }

    fn context_account(
        &self,
        context: &rustcta_exchange_api::RequestContext,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitmex private REST requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitmex private REST requires context.account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for BitmexGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("bitmex public REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for BitmexGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        let private_rest = self.config.private_rest_available();
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private_rest;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = private_rest;
        capabilities.private_stream_capabilities = Some(if private_rest {
            streams::bitmex_private_stream_capabilities()
        } else {
            rustcta_exchange_api::PrivateStreamCapabilities::unsupported(
                rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            )
        });
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private_rest;
        capabilities.supports_positions = private_rest;
        capabilities.supports_fees = true;
        capabilities.supports_place_order = private_rest;
        capabilities.supports_cancel_order = private_rest;
        capabilities.supports_query_order = private_rest;
        capabilities.supports_open_orders = private_rest;
        capabilities.supports_recent_fills = private_rest;
        capabilities.supports_batch_place_order = private_rest;
        capabilities.supports_batch_cancel_order = private_rest;
        capabilities.supports_cancel_all_orders = private_rest;
        capabilities.supports_amend_order = private_rest;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = true;
        capabilities.supports_post_only = true;
        capabilities.supports_time_in_force = vec![
            rustcta_types::TimeInForce::GTC,
            rustcta_types::TimeInForce::IOC,
            rustcta_types::TimeInForce::FOK,
            rustcta_types::TimeInForce::GTX,
        ];
        capabilities.supports_order_types = vec![
            rustcta_types::OrderType::Market,
            rustcta_types::OrderType::Limit,
            rustcta_types::OrderType::PostOnly,
            rustcta_types::OrderType::IOC,
            rustcta_types::OrderType::FOK,
            rustcta_types::OrderType::StopMarket,
        ];
        capabilities.max_order_book_depth = Some(100);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(100));
        capabilities.max_recent_fill_limit = Some(500);
        capabilities.capabilities_v2 = bitmex_capabilities_v2(private_rest);
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_private_rest(request).await
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.get_positions_private_rest(request).await
    }

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        self.get_symbol_rules_public_rest(request).await
    }

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        self.get_order_book_public_rest(request).await
    }

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        self.get_fees_private_rest(request).await
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_order_private_rest(request).await
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_quote_market_order_private_rest(request).await
    }

    async fn batch_place_orders(
        &self,
        request: rustcta_exchange_api::BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::BatchPlaceOrdersResponse> {
        self.batch_place_orders_private_rest(request).await
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.cancel_order_private_rest(request).await
    }

    async fn batch_cancel_orders(
        &self,
        request: rustcta_exchange_api::BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::BatchCancelOrdersResponse> {
        self.batch_cancel_orders_private_rest(request).await
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.amend_order_private_rest(request).await
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.place_order_list_private_rest(request).await
    }

    async fn cancel_all_orders(
        &self,
        request: rustcta_exchange_api::CancelAllOrdersRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::CancelAllOrdersResponse> {
        self.cancel_all_orders_private_rest(request).await
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.query_order_private_rest(request).await
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.get_open_orders_private_rest(request).await
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.get_recent_fills_private_rest(request).await
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

fn bitmex_capabilities_v2(private_rest: bool) -> ExchangeClientCapabilitiesV2 {
    let private_support = if private_rest {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "BitMEX private REST and private WS require API key and secret",
        )
    };
    let private_stream_support = if private_rest {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::rest_fallback(
            "private streams require credentials; REST reconciliation is available when credentials are configured",
        )
    };
    let trade_scopes = if private_rest {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    };

    ExchangeClientCapabilitiesV2 {
        public_rest: CapabilitySupport::native(),
        private_rest: private_support.clone(),
        public_streams: CapabilitySupport::native(),
        private_streams: private_stream_support,
        stream_runtime: bitmex_stream_runtime_capability(private_rest),
        batch_place_orders: if private_rest {
            BatchCapability {
                support: CapabilitySupport::native(),
                mode: BatchExecutionMode::Native,
                atomicity: BatchAtomicity::Partial,
                max_items: Some(20),
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported(
                "BitMEX bulk order endpoint requires private REST credentials",
            )
        },
        batch_cancel_orders: if private_rest {
            BatchCapability {
                support: CapabilitySupport::native(),
                mode: BatchExecutionMode::Native,
                atomicity: BatchAtomicity::Partial,
                max_items: Some(20),
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("BitMEX bulk cancel uses private REST order identifiers")
        },
        cancel_all_orders: private_support.clone(),
        order_history: if private_rest {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: false,
                supports_until: false,
                supports_limit: true,
                supports_cursor: false,
                supports_from_id: false,
                max_limit: Some(100),
                max_window_ms: None,
            }
        } else {
            HistoryCapability::unsupported(
                "BitMEX order reconciliation requires private REST credentials",
            )
        },
        fills_history: if private_rest {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: true,
                supports_until: true,
                supports_limit: true,
                supports_cursor: false,
                supports_from_id: false,
                max_limit: Some(500),
                max_window_ms: None,
            }
        } else {
            HistoryCapability::unsupported(
                "BitMEX execution history requires private REST credentials",
            )
        },
        endpoints: bitmex_endpoint_capabilities(private_rest),
        credential_scopes: trade_scopes,
    }
}

fn bitmex_stream_runtime_capability(private_rest: bool) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: if private_rest {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::rest_fallback(
                "private WS authKeyExpires login requires credentials; use REST reconciliation until configured",
            )
        },
        supports_subscribe: true,
        supports_unsubscribe: false,
        supports_public_subscribe: true,
        supports_public_unsubscribe: false,
        supports_private_subscribe: private_rest,
        supports_private_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
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
            balances: private_rest,
            positions: private_rest,
            orders: private_rest,
        },
        auth: StreamAuthCapability {
            required: private_rest,
            credential_scopes: if private_rest {
                vec![CredentialScope::ReadOnly, CredentialScope::Trade]
            } else {
                Vec::new()
            },
            renewal_ms: if private_rest { Some(45_000) } else { None },
            uses_listen_key: false,
            requires_relogin_on_reconnect: true,
        },
        public_private_separate_connections: true,
        heartbeat_policy: HeartbeatPolicy {
            direction: HeartbeatDirection::ClientPing,
            ping_interval_ms: 30_000,
            pong_timeout_ms: 10_000,
            stale_message_ms: 30_000,
            requires_pong_payload_echo: false,
        },
        auth_renewal: if private_rest {
            AuthRenewalPolicy {
                kind: AuthRenewalKind::ReLogin,
                renew_before_expiry_ms: 15_000,
                renewal_interval_ms: Some(45_000),
                reconnect_on_renewal_failure: true,
                resubscribe_after_renewal: true,
            }
        } else {
            AuthRenewalPolicy::default()
        },
        auth_renewal_policy: if private_rest {
            AuthRenewalPolicy {
                kind: AuthRenewalKind::ReLogin,
                renew_before_expiry_ms: 15_000,
                renewal_interval_ms: Some(45_000),
                reconnect_on_renewal_failure: true,
                resubscribe_after_renewal: true,
            }
        } else {
            AuthRenewalPolicy::default()
        },
        reconnect_requires_login: private_rest,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
    }
}

fn bitmex_endpoint_capabilities(private_rest: bool) -> Vec<EndpointCapability> {
    let public_markets = vec![MarketType::Spot, MarketType::Perpetual];
    let private_markets = public_markets.clone();
    let mut endpoints = vec![
        rest_endpoint(
            "symbol_rules",
            CapabilitySupport::native(),
            public_markets.clone(),
            "GET",
            "/api/v1/instrument/active",
            EndpointAuth::None,
            Vec::new(),
            "rest_public",
            1,
            true,
        ),
        rest_endpoint(
            "order_book_snapshot",
            CapabilitySupport::native(),
            public_markets.clone(),
            "GET",
            "/api/v1/orderBook/L2",
            EndpointAuth::None,
            Vec::new(),
            "rest_public",
            1,
            true,
        ),
        ws_endpoint(
            "public_order_book_l2_25_stream",
            CapabilitySupport::native(),
            public_markets.clone(),
            "orderBookL2_25:{symbol}",
            EndpointAuth::None,
            Vec::new(),
        ),
        ws_endpoint(
            "public_order_book10_stream",
            CapabilitySupport::native(),
            public_markets.clone(),
            "orderBook10:{symbol}",
            EndpointAuth::None,
            Vec::new(),
        ),
        ws_endpoint(
            "public_order_book_l2_stream",
            CapabilitySupport::native(),
            public_markets.clone(),
            "orderBookL2:{symbol}",
            EndpointAuth::None,
            Vec::new(),
        ),
        ws_endpoint(
            "public_trades_stream",
            CapabilitySupport::native(),
            public_markets.clone(),
            "trade:{symbol}",
            EndpointAuth::None,
            Vec::new(),
        ),
    ];

    let private_operations = [
        (
            "balances",
            "GET",
            "/api/v1/user/margin",
            "rest_private_read",
            1,
        ),
        (
            "positions",
            "GET",
            "/api/v1/position",
            "rest_private_read",
            1,
        ),
        ("place_order", "POST", "/api/v1/order", "orders", 1),
        (
            "batch_place_orders",
            "POST",
            "/api/v1/order/bulk",
            "orders",
            2,
        ),
        ("cancel_order", "DELETE", "/api/v1/order", "cancels", 1),
        (
            "batch_cancel_orders",
            "DELETE",
            "/api/v1/order",
            "cancels",
            1,
        ),
        (
            "cancel_all_orders",
            "DELETE",
            "/api/v1/order/all",
            "cancels",
            1,
        ),
        ("amend_order", "PUT", "/api/v1/order", "orders", 1),
        (
            "query_order",
            "GET",
            "/api/v1/order",
            "rest_private_read",
            1,
        ),
        (
            "open_orders",
            "GET",
            "/api/v1/order",
            "rest_private_read",
            1,
        ),
        (
            "recent_fills",
            "GET",
            "/api/v1/execution/tradeHistory",
            "rest_private_read",
            1,
        ),
    ];
    for (operation, method, path, bucket, weight) in private_operations {
        endpoints.push(rest_endpoint(
            operation,
            if private_rest {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(format!(
                    "{operation} requires BitMEX API credentials"
                ))
            },
            private_markets.clone(),
            method,
            path,
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly, CredentialScope::Trade],
            bucket,
            weight,
            true,
        ));
    }

    endpoints.push(rest_endpoint(
        "fees",
        CapabilitySupport::native(),
        private_markets.clone(),
        "GET",
        "/api/v1/instrument/active",
        EndpointAuth::None,
        Vec::new(),
        "rest_public",
        1,
        true,
    ));
    endpoints.push(ws_endpoint(
        "private_orders_stream",
        if private_rest {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::rest_fallback("private order stream requires credentials")
        },
        private_markets.clone(),
        "order",
        EndpointAuth::Hmac,
        vec![CredentialScope::ReadOnly, CredentialScope::Trade],
    ));
    endpoints.push(ws_endpoint(
        "private_fills_stream",
        if private_rest {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::rest_fallback("private execution stream requires credentials")
        },
        private_markets,
        "execution",
        EndpointAuth::Hmac,
        vec![CredentialScope::ReadOnly, CredentialScope::Trade],
    ));
    endpoints.push(rest_endpoint(
        "funding_open_interest",
        CapabilitySupport::unsupported("BitMEX funding/open-interest fields are not yet exposed through a typed gateway method"),
        vec![MarketType::Perpetual],
        "GET",
        "/api/v1/instrument",
        EndpointAuth::None,
        Vec::new(),
        "rest_public",
        1,
        true,
    ));

    endpoints
}

fn rest_endpoint(
    operation: impl Into<String>,
    support: CapabilitySupport,
    market_types: Vec<MarketType>,
    method: impl Into<String>,
    path: impl Into<String>,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: impl Into<String>,
    weight: u32,
    supports_testnet: bool,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.into(),
        support,
        market_types,
        transport: EndpointTransport::Rest,
        method: Some(method.into()),
        path: Some(path.into()),
        auth,
        credential_scopes,
        rate_limit_bucket: Some(rate_limit_bucket.into()),
        weight: Some(weight),
        supports_testnet,
    }
}

fn ws_endpoint(
    operation: impl Into<String>,
    support: CapabilitySupport,
    market_types: Vec<MarketType>,
    channel: impl Into<String>,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.into(),
        support,
        market_types,
        transport: EndpointTransport::WebSocket,
        method: None,
        path: Some(channel.into()),
        auth,
        credential_scopes,
        rate_limit_bucket: Some("websocket".to_string()),
        weight: Some(1),
        supports_testnet: true,
    }
}
