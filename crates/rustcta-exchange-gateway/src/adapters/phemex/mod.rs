use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, AuthRenewalKind, AuthRenewalPolicy, BalancesRequest,
    BalancesResponse, BatchAtomicity, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchCapability, BatchExecutionMode, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeApiError, ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, FeesRequest,
    FeesResponse, HeartbeatCapability, HeartbeatDirection, HeartbeatPolicy, HistoryCapability,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest,
    OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, ReconnectCapability,
    StreamAuthCapability, StreamHeartbeatDirection, StreamResyncCapability, SymbolRulesRequest,
    SymbolRulesResponse, TimeInForce,
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

pub use config::PhemexGatewayConfig;
use transport::PhemexRest;

#[derive(Clone)]
pub struct PhemexGatewayAdapter {
    exchange_id: ExchangeId,
    config: PhemexGatewayConfig,
    rest: PhemexRest,
}

impl PhemexGatewayAdapter {
    pub fn new(config: PhemexGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("phemex").map_err(validation_error)?;
        let rest = PhemexRest::new(
            exchange_id.clone(),
            config.rest_base_url.clone(),
            config.request_timeout_ms,
            config.request_expiry_seconds,
        )?;
        Ok(Self {
            exchange_id,
            config,
            rest,
        })
    }

    #[cfg(test)]
    pub fn default_public() -> ExchangeApiResult<Self> {
        Self::new(PhemexGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("phemex adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "phemex.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "phemex.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_perpetual(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "phemex.non_perpetual_market_type",
            });
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn unsupported_private<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        let api_key = self
            .config
            .api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let api_secret = self
            .config
            .api_secret
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        Ok((api_key, api_secret))
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_signed_get(endpoint, params, api_key, api_secret)
            .await
    }

    async fn send_signed_put(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_signed_put(endpoint, params, api_key, api_secret)
            .await
    }

    #[allow(dead_code)]
    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_signed_post(endpoint, params, api_key, api_secret)
            .await
    }

    #[allow(dead_code)]
    async fn send_signed_post_json(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_signed_post_json(endpoint, body, api_key, api_secret)
            .await
    }

    async fn send_signed_delete(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_signed_delete(endpoint, params, api_key, api_secret)
            .await
    }

    fn context_account(
        &self,
        context: &rustcta_exchange_api::RequestContext,
        operation: &'static str,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires context.tenant_id"),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires context.account_id"),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for PhemexGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                if self.config.private_rest_enabled() {
                    "phemex spot + USDT perpetual public/private REST gateway adapter"
                } else {
                    "phemex spot + USDT perpetual public REST gateway adapter"
                }
                .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for PhemexGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private_rest_enabled = self.config.private_rest_enabled();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private_rest_enabled;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = private_rest_enabled;
        capabilities.private_stream_capabilities =
            Some(rustcta_exchange_api::PrivateStreamCapabilities {
                schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
                supports_orders: private_rest_enabled,
                supports_fills: private_rest_enabled,
                supports_balances: private_rest_enabled,
                supports_positions: private_rest_enabled,
                supports_account: private_rest_enabled,
                order_event_kinds: vec![
                    rustcta_exchange_api::PrivateOrderStreamEventKind::New,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::PartialFill,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Fill,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Cancel,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Reject,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Expired,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::BalanceUpdate,
                ],
                supports_client_order_id: true,
                supports_exchange_order_id: true,
            });
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private_rest_enabled;
        capabilities.supports_positions = private_rest_enabled;
        capabilities.supports_fees = private_rest_enabled;
        capabilities.supports_place_order = private_rest_enabled;
        capabilities.supports_cancel_order = private_rest_enabled;
        capabilities.supports_cancel_all_orders = private_rest_enabled;
        capabilities.supports_quote_market_order = private_rest_enabled;
        capabilities.supports_query_order = private_rest_enabled;
        capabilities.supports_open_orders = private_rest_enabled;
        capabilities.supports_recent_fills = private_rest_enabled;
        capabilities.supports_batch_place_order = private_rest_enabled;
        capabilities.supports_batch_cancel_order = private_rest_enabled;
        capabilities.supports_client_order_id = true;
        capabilities.supports_amend_order = private_rest_enabled;
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
            OrderType::StopMarket,
            OrderType::StopLimit,
        ];
        capabilities.max_order_book_depth = Some(30);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(30));
        capabilities.max_recent_fill_limit = Some(200);
        capabilities.capabilities_v2 = phemex_capabilities_v2(private_rest_enabled);
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
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.amend_order_impl(request).await
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

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "phemex.place_order_list",
        })
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

fn phemex_capabilities_v2(
    private_rest_enabled: bool,
) -> rustcta_exchange_api::ExchangeClientCapabilitiesV2 {
    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST and private WS require configured credentials")
    };
    rustcta_exchange_api::ExchangeClientCapabilitiesV2 {
        public_rest: CapabilitySupport::native(),
        private_rest: private_support.clone(),
        public_streams: CapabilitySupport::native(),
        private_streams: private_support.clone(),
        stream_runtime: rustcta_exchange_api::StreamRuntimeCapability {
            public: CapabilitySupport::native(),
            private: private_support.clone(),
            supports_subscribe: true,
            supports_unsubscribe: false,
            heartbeat: HeartbeatCapability {
                supported: true,
                required: true,
                direction: StreamHeartbeatDirection::ClientPing,
                interval_ms: Some(15_000),
                timeout_ms: Some(30_000),
            },
            reconnect: ReconnectCapability {
                supported: true,
                requires_resubscribe: true,
                preserves_session: false,
                max_reconnect_attempts: None,
            },
            resync: StreamResyncCapability {
                order_book: true,
                balances: private_rest_enabled,
                positions: private_rest_enabled,
                orders: private_rest_enabled,
            },
            auth: StreamAuthCapability {
                required: private_rest_enabled,
                credential_scopes: if private_rest_enabled {
                    vec![CredentialScope::ReadOnly, CredentialScope::Trade]
                } else {
                    Vec::new()
                },
                renewal_ms: Some(30_000),
                uses_listen_key: false,
                requires_relogin_on_reconnect: true,
            },
            public_private_separate_connections: true,
            heartbeat_policy: HeartbeatPolicy {
                direction: HeartbeatDirection::ClientPing,
                ping_interval_ms: 15_000,
                pong_timeout_ms: 30_000,
                stale_message_ms: 45_000,
                requires_pong_payload_echo: false,
            },
            auth_renewal_policy: AuthRenewalPolicy {
                kind: AuthRenewalKind::ReLogin,
                renew_before_expiry_ms: 30_000,
                renewal_interval_ms: None,
                reconnect_on_renewal_failure: true,
                resubscribe_after_renewal: true,
            },
            reconnect_requires_login: true,
            reconnect_requires_resubscribe: true,
            orderbook_requires_snapshot_after_reconnect: true,
            ..rustcta_exchange_api::StreamRuntimeCapability::default()
        },
        batch_place_orders: BatchCapability {
            support: private_support.clone(),
            mode: if private_rest_enabled {
                BatchExecutionMode::ComposedSequential
            } else {
                BatchExecutionMode::Unsupported
            },
            atomicity: if private_rest_enabled {
                BatchAtomicity::NonAtomic
            } else {
                BatchAtomicity::Unknown
            },
            max_items: None,
            same_symbol_required: false,
            same_market_type_required: false,
            supports_client_order_id: private_rest_enabled,
            supports_partial_failure: private_rest_enabled,
        },
        batch_cancel_orders: BatchCapability {
            support: private_support.clone(),
            mode: if private_rest_enabled {
                BatchExecutionMode::Native
            } else {
                BatchExecutionMode::Unsupported
            },
            atomicity: if private_rest_enabled {
                BatchAtomicity::Partial
            } else {
                BatchAtomicity::Unknown
            },
            max_items: Some(20),
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: private_rest_enabled,
            supports_partial_failure: private_rest_enabled,
        },
        cancel_all_orders: private_support.clone(),
        order_history: HistoryCapability {
            support: private_support.clone(),
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: false,
            max_limit: Some(200),
            max_window_ms: None,
        },
        fills_history: HistoryCapability {
            support: private_support.clone(),
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: false,
            max_limit: Some(200),
            max_window_ms: None,
        },
        endpoints: phemex_endpoint_capabilities(private_rest_enabled),
        credential_scopes: if private_rest_enabled {
            vec![
                CredentialScope::ReadOnly,
                CredentialScope::Trade,
                CredentialScope::Transfer,
                CredentialScope::Withdraw,
            ]
        } else {
            Vec::new()
        },
    }
}

fn phemex_endpoint_capabilities(private_rest_enabled: bool) -> Vec<EndpointCapability> {
    let private_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("private REST requires configured credentials")
    };
    let private_scopes = if private_rest_enabled {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    };
    vec![
        endpoint(
            "symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/public/products",
            EndpointAuth::None,
            Vec::new(),
            "public_market",
            1,
        ),
        endpoint(
            "order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot],
            "GET",
            "/md/orderbook",
            EndpointAuth::None,
            Vec::new(),
            "public_market",
            1,
        ),
        endpoint(
            "order_book",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/md/v2/orderbook",
            EndpointAuth::None,
            Vec::new(),
            "public_market",
            1,
        ),
        endpoint(
            "balances",
            private_support.clone(),
            vec![MarketType::Spot],
            "GET",
            "/spot/wallets",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            "private_account",
            5,
        ),
        endpoint(
            "balances_positions",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "GET",
            "/g-accounts/accountPositions",
            EndpointAuth::Hmac,
            vec![CredentialScope::ReadOnly],
            "private_account",
            5,
        ),
        endpoint(
            "place_order",
            private_support.clone(),
            vec![MarketType::Spot],
            "PUT",
            "/spot/orders/create",
            EndpointAuth::Hmac,
            private_scopes.clone(),
            "private_trade",
            1,
        ),
        endpoint(
            "place_order",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "PUT",
            "/g-orders/create",
            EndpointAuth::Hmac,
            private_scopes.clone(),
            "private_trade",
            1,
        ),
        endpoint(
            "batch_cancel_orders",
            private_support.clone(),
            vec![MarketType::Perpetual],
            "DELETE",
            "/g-orders",
            EndpointAuth::Hmac,
            private_scopes.clone(),
            "private_trade",
            1,
        ),
        endpoint(
            "funding_history",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/api-data/public/data/funding-rate-history",
            EndpointAuth::None,
            Vec::new(),
            "public_market",
            1,
        ),
        endpoint(
            "funding_open_interest_ticker",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/md/v2/ticker/24hr",
            EndpointAuth::None,
            Vec::new(),
            "public_market",
            1,
        ),
    ]
}

#[allow(clippy::too_many_arguments)]
fn endpoint(
    operation: &str,
    support: CapabilitySupport,
    market_types: Vec<MarketType>,
    method: &str,
    path: &str,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: &str,
    weight: u32,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types,
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth,
        credential_scopes,
        rate_limit_bucket: Some(rate_limit_bucket.to_string()),
        weight: Some(weight),
        supports_testnet: true,
    }
}
