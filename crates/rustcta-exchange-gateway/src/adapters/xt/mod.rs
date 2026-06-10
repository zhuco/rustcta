use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, AuthRenewalKind, AuthRenewalPolicy, BalancesRequest,
    BalancesResponse, BatchAtomicity, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchCapability, BatchExecutionMode, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability, EndpointTransport,
    ExchangeApiError, ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities,
    ExchangeClientCapabilitiesV2, FeesRequest, FeesResponse, HeartbeatCapability,
    HeartbeatDirection, HeartbeatPolicy, HistoryCapability, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, PrivateStreamSubscription,
    PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, ReconnectCapability, StreamAuthCapability,
    StreamHeartbeatDirection, StreamResyncCapability, StreamRuntimeCapability, SymbolRulesRequest,
    SymbolRulesResponse, TimeInForce,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
#[cfg(test)]
mod live_tests;
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

pub use config::XtGatewayConfig;
use transport::XtRest;

#[derive(Clone)]
pub struct XtGatewayAdapter {
    exchange_id: ExchangeId,
    config: XtGatewayConfig,
    rest: XtRest,
}

impl XtGatewayAdapter {
    pub fn new(config: XtGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("xt").map_err(validation_error)?;
        let rest = XtRest::new(
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
        Self::new(XtGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("xt adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "xt.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str, u64)> {
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
        Ok((api_key, api_secret, self.config.recv_window_ms))
    }

    #[allow(dead_code)]
    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_get(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_post(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_delete(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_delete(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_put(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_put(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_json(
        &self,
        operation: &'static str,
        method: reqwest::Method,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_json(method, endpoint, body, api_key, api_secret, recv_window_ms)
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
impl GatewayAdapter for XtGatewayAdapter {
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
                    "xt spot + linear perpetual public/private REST gateway adapter"
                } else {
                    "xt spot + linear perpetual public REST gateway adapter"
                }
                .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for XtGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private = self.config.private_rest_enabled();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = private;
        capabilities.private_stream_capabilities =
            Some(streams::xt_private_stream_capabilities(private));
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private;
        capabilities.supports_positions = private;
        capabilities.supports_fees = true;
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_batch_place_order = private;
        capabilities.supports_batch_cancel_order = private;
        capabilities.supports_quote_market_order = private;
        capabilities.supports_amend_order = private;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = false;
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
        capabilities.max_recent_fill_limit = Some(100);
        capabilities.capabilities_v2 = xt_capabilities_v2(private);
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
        if request.symbol.market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "xt.perpetual_quote_market_order",
            });
        }
        self.place_order(PlaceOrderRequest {
            schema_version: request.schema_version,
            context: request.context,
            symbol: request.symbol,
            client_order_id: request.client_order_id,
            side: request.side,
            position_side: None,
            order_type: OrderType::Market,
            time_in_force: None,
            quantity: "0".to_string(),
            price: None,
            quote_quantity: Some(request.quote_quantity),
            reduce_only: false,
            post_only: false,
        })
        .await
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

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "xt.place_order_list",
        })
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

fn xt_capabilities_v2(private: bool) -> ExchangeClientCapabilitiesV2 {
    let mut v2 = ExchangeClientCapabilitiesV2 {
        public_rest: CapabilitySupport::native(),
        private_rest: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("XT private REST requires configured API key and secret")
        },
        public_streams: CapabilitySupport::native(),
        private_streams: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "XT private streams require signed listen-key/token acquisition",
            )
        },
        stream_runtime: xt_stream_runtime_capability(private),
        batch_place_orders: if private {
            xt_batch_capability(BatchAtomicity::Partial, Some(20))
        } else {
            BatchCapability::unsupported("XT batch place requires private REST credentials")
        },
        batch_cancel_orders: if private {
            xt_batch_capability(BatchAtomicity::Partial, Some(20))
        } else {
            BatchCapability::unsupported("XT batch cancel requires private REST credentials")
        },
        cancel_all_orders: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("XT cancel-all requires private REST credentials")
        },
        order_history: if private {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: false,
                supports_until: false,
                supports_limit: false,
                supports_cursor: false,
                supports_from_id: false,
                max_limit: None,
                max_window_ms: None,
            }
        } else {
            HistoryCapability::unsupported("XT order history requires private REST credentials")
        },
        fills_history: if private {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: true,
                supports_until: true,
                supports_limit: true,
                supports_cursor: false,
                supports_from_id: true,
                max_limit: Some(1000),
                max_window_ms: None,
            }
        } else {
            HistoryCapability::unsupported("XT fills history requires private REST credentials")
        },
        endpoints: xt_endpoint_capabilities(private),
        credential_scopes: if private {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        },
        ..ExchangeClientCapabilitiesV2::default()
    };
    if !private {
        v2.endpoints
            .iter_mut()
            .filter(|endpoint| endpoint.auth != EndpointAuth::None)
            .for_each(|endpoint| {
                endpoint.support =
                    CapabilitySupport::unsupported("XT private REST credentials are not configured")
            });
    }
    v2
}

fn xt_batch_capability(atomicity: BatchAtomicity, max_items: Option<u32>) -> BatchCapability {
    BatchCapability {
        support: CapabilitySupport::native(),
        mode: BatchExecutionMode::Native,
        atomicity,
        max_items,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    }
}

fn xt_stream_runtime_capability(private: bool) -> StreamRuntimeCapability {
    StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("XT private streams require REST listen-key acquisition")
        },
        supports_subscribe: true,
        supports_unsubscribe: false,
        supports_public_subscribe: true,
        supports_public_unsubscribe: false,
        supports_private_subscribe: private,
        supports_private_unsubscribe: false,
        heartbeat: HeartbeatCapability {
            supported: true,
            required: true,
            direction: StreamHeartbeatDirection::ClientPing,
            interval_ms: Some(25_000),
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
            positions: true,
            orders: true,
        },
        auth: StreamAuthCapability {
            required: private,
            credential_scopes: if private {
                vec![CredentialScope::ReadOnly, CredentialScope::Trade]
            } else {
                Vec::new()
            },
            renewal_ms: Some(1_800_000),
            uses_listen_key: true,
            requires_relogin_on_reconnect: true,
        },
        heartbeat_policy: HeartbeatPolicy {
            direction: HeartbeatDirection::ApplicationMessage,
            ping_interval_ms: 25_000,
            pong_timeout_ms: 10_000,
            stale_message_ms: 35_000,
            requires_pong_payload_echo: false,
        },
        auth_renewal_policy: AuthRenewalPolicy {
            kind: AuthRenewalKind::ListenKeyKeepAlive,
            renew_before_expiry_ms: 300_000,
            renewal_interval_ms: Some(1_800_000),
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        },
        auth_renewal: AuthRenewalPolicy {
            kind: AuthRenewalKind::ListenKeyKeepAlive,
            renew_before_expiry_ms: 300_000,
            renewal_interval_ms: Some(1_800_000),
            reconnect_on_renewal_failure: true,
            resubscribe_after_renewal: true,
        },
        reconnect_requires_login: true,
        reconnect_requires_resubscribe: true,
        orderbook_requires_snapshot_after_reconnect: true,
        ..StreamRuntimeCapability::default()
    }
}

fn xt_endpoint_capabilities(private: bool) -> Vec<EndpointCapability> {
    let read = vec![CredentialScope::ReadOnly];
    let trade = vec![CredentialScope::Trade];
    let mut endpoints = vec![
        endpoint(
            "xt.get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/v4/public/symbol | /future/market/v3/public/symbol/list",
            EndpointAuth::None,
            Vec::new(),
            "xt_public_rest",
            1,
        ),
        endpoint(
            "xt.get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/v4/public/depth | /future/market/v1/public/q/depth",
            EndpointAuth::None,
            Vec::new(),
            "xt_public_rest",
            1,
        ),
        endpoint(
            "xt.get_fees",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/v4/public/symbol | /future/market/v3/public/symbol/list",
            EndpointAuth::None,
            Vec::new(),
            "xt_public_rest",
            1,
        ),
        endpoint(
            "xt.get_balances",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/v4/balances | /future/user/v1/balance/list",
            EndpointAuth::Hmac,
            read.clone(),
            "xt_private_rest",
            1,
        ),
        endpoint(
            "xt.get_positions",
            CapabilitySupport::native(),
            vec![MarketType::Perpetual],
            "GET",
            "/future/user/v1/position/list",
            EndpointAuth::Hmac,
            read.clone(),
            "xt_private_rest",
            1,
        ),
        endpoint(
            "xt.place_order",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "POST",
            "/v4/order | /future/trade/v1/order/create",
            EndpointAuth::Hmac,
            trade.clone(),
            "xt_trade",
            1,
        ),
        endpoint(
            "xt.place_quote_market_order",
            CapabilitySupport::native(),
            vec![MarketType::Spot],
            "POST",
            "/v4/order",
            EndpointAuth::Hmac,
            trade.clone(),
            "xt_trade",
            1,
        ),
        endpoint(
            "xt.cancel_order",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "DELETE",
            "/v4/order/{orderId} | /future/trade/v1/order/cancel",
            EndpointAuth::Hmac,
            trade.clone(),
            "xt_trade",
            1,
        ),
        endpoint(
            "xt.amend_order",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "PUT",
            "/v4/order/{orderId} | /future/trade/v1/order/update",
            EndpointAuth::Hmac,
            trade.clone(),
            "xt_trade",
            1,
        ),
        endpoint(
            "xt.batch_place_orders",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "POST",
            "/v4/batch-order | /future/trade/v2/order/atomic-create-batch",
            EndpointAuth::Hmac,
            trade.clone(),
            "xt_trade",
            1,
        ),
        endpoint(
            "xt.batch_cancel_orders",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "DELETE",
            "/v4/batch-order | /future/trade/v1/order/cancel-batch",
            EndpointAuth::Hmac,
            trade.clone(),
            "xt_trade",
            1,
        ),
        endpoint(
            "xt.cancel_all_orders",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "DELETE",
            "/v4/open-order | /future/trade/v1/order/cancel-all",
            EndpointAuth::Hmac,
            trade.clone(),
            "xt_trade",
            1,
        ),
        endpoint(
            "xt.query_order",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/v4/order/{orderId} | /future/trade/v1/order/detail",
            EndpointAuth::Hmac,
            read.clone(),
            "xt_private_rest",
            1,
        ),
        endpoint(
            "xt.get_open_orders",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/v4/open-order | /future/trade/v1/order/list-open-order",
            EndpointAuth::Hmac,
            read.clone(),
            "xt_private_rest",
            1,
        ),
        endpoint(
            "xt.get_recent_fills",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            "GET",
            "/v4/trade | /future/trade/v1/order/trade-list",
            EndpointAuth::Hmac,
            read,
            "xt_private_rest",
            1,
        ),
    ];
    endpoints.push(endpoint(
        "xt.place_order_list",
        CapabilitySupport::unsupported(
            "XT trigger/TP-SL APIs do not match the shared OCO/OTO OrderListRequest contract",
        ),
        vec![MarketType::Spot, MarketType::Perpetual],
        "POST",
        "/unsupported/order-list",
        EndpointAuth::None,
        Vec::new(),
        "xt_trade",
        0,
    ));
    if !private {
        endpoints
            .iter_mut()
            .filter(|endpoint| endpoint.auth != EndpointAuth::None)
            .for_each(|endpoint| {
                endpoint.support =
                    CapabilitySupport::unsupported("XT private REST credentials are not configured")
            });
    }
    endpoints
}

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
        supports_testnet: false,
    }
}
