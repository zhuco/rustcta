use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamCapabilities, PrivateStreamSubscription,
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

pub use config::ToobitGatewayConfig;
use transport::ToobitRest;

#[derive(Clone)]
pub struct ToobitGatewayAdapter {
    exchange_id: ExchangeId,
    config: ToobitGatewayConfig,
    rest: ToobitRest,
}

impl ToobitGatewayAdapter {
    pub fn new(config: ToobitGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("toobit").map_err(validation_error)?;
        let rest = ToobitRest::new(
            exchange_id.clone(),
            config.rest_base_url.clone(),
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
        Self::new(ToobitGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("toobit adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "toobit.unsupported_market_type",
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

    async fn create_listen_key(&self, operation: &'static str) -> ExchangeApiResult<String> {
        let value = self
            .send_signed_post(operation, "/api/v1/userDataStream", &HashMap::new())
            .await?;
        value
            .get("listenKey")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "toobit listenKey response missing listenKey".to_string(),
            })
    }

    #[allow(dead_code)]
    async fn keepalive_listen_key(&self, listen_key: &str) -> ExchangeApiResult<()> {
        let mut params = HashMap::new();
        params.insert("listenKey".to_string(), listen_key.to_string());
        self.rest
            .send_signed_put(
                "/api/v1/userDataStream",
                &params,
                self.private_credentials("toobit.keepalive_listen_key")?.0,
                self.private_credentials("toobit.keepalive_listen_key")?.1,
                self.private_credentials("toobit.keepalive_listen_key")?.2,
            )
            .await
            .map(|_| ())
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

    fn generate_client_order_id(&self, market_type: MarketType) -> String {
        let suffix = match market_type {
            MarketType::Spot => "spot",
            MarketType::Perpetual => "perp",
            _ => "ord",
        };
        format!("rustcta_toobit_{suffix}_{}", Utc::now().timestamp_millis())
    }
}

#[async_trait]
impl GatewayAdapter for ToobitGatewayAdapter {
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
                    "toobit spot + linear perpetual public/private REST and WS request-spec gateway adapter"
                } else {
                    "toobit spot + linear perpetual public REST and WS request-spec gateway adapter"
                }
                .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for ToobitGatewayAdapter {
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
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private;
        capabilities.supports_positions = private;
        capabilities.supports_fees = private;
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_batch_place_order = private;
        capabilities.supports_batch_cancel_order = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = private;
        capabilities.supports_order_list = false;
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
        capabilities.max_order_book_depth = Some(200);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(200));
        capabilities.max_recent_fill_limit = Some(1000);
        capabilities.private_stream_capabilities = Some(if private {
            streams::toobit_private_stream_capabilities()
        } else {
            PrivateStreamCapabilities::unsupported(
                rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            )
        });
        capabilities.refresh_v2_from_legacy_flags();
        apply_toobit_capabilities_v2(&mut capabilities, private);
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
        _request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "toobit.place_quote_market_order",
        })
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

fn apply_toobit_capabilities_v2(capabilities: &mut ExchangeClientCapabilities, private: bool) {
    use rustcta_exchange_api::{
        BatchAtomicity, BatchCapability, CapabilitySupport, CredentialScope, EndpointAuth,
        EndpointCapability, EndpointTransport, HeartbeatCapability, HistoryCapability,
        ReconnectCapability, StreamAuthCapability, StreamHeartbeatDirection,
        StreamResyncCapability, StreamRuntimeCapability,
    };

    let private_support = if private {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("toobit private REST requires API key and secret")
    };
    let scopes = if private {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    };

    capabilities.capabilities_v2.credential_scopes = scopes.clone();
    capabilities.capabilities_v2.batch_place_orders = if private {
        BatchCapability {
            support: CapabilitySupport::native(),
            atomicity: BatchAtomicity::Partial,
            max_items: Some(20),
            supports_client_order_id: true,
            supports_partial_failure: true,
            ..BatchCapability::native(BatchAtomicity::Partial, Some(20))
        }
    } else {
        BatchCapability::unsupported("toobit private REST is disabled")
    };
    capabilities.capabilities_v2.batch_cancel_orders = if private {
        BatchCapability {
            support: CapabilitySupport::native(),
            atomicity: BatchAtomicity::Partial,
            max_items: Some(20),
            supports_client_order_id: true,
            supports_partial_failure: true,
            ..BatchCapability::native(BatchAtomicity::Partial, Some(20))
        }
    } else {
        BatchCapability::unsupported("toobit private REST is disabled")
    };
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: private_support.clone(),
        supports_since: false,
        supports_until: false,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: false,
        max_limit: Some(1000),
        max_window_ms: None,
    };
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: private_support.clone(),
        supports_since: true,
        supports_until: true,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: true,
        max_limit: Some(1000),
        max_window_ms: None,
    };
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: private_support.clone(),
        supports_subscribe: true,
        supports_unsubscribe: true,
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
            balances: true,
            positions: true,
            orders: true,
        },
        auth: StreamAuthCapability {
            required: private,
            credential_scopes: scopes.clone(),
            renewal_ms: Some(3_300_000),
            uses_listen_key: true,
            requires_relogin_on_reconnect: true,
        },
        ..StreamRuntimeCapability::default()
    };
    capabilities.capabilities_v2.endpoints = vec![
        endpoint(
            "toobit.get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::None,
            Some("GET"),
            Some("/api/v1/exchangeInfo"),
            vec![],
            "toobit_public",
            1,
        ),
        endpoint(
            "toobit.get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::None,
            Some("GET"),
            Some("/quote/v1/depth"),
            vec![],
            "toobit_public",
            1,
        ),
        endpoint(
            "toobit.get_balances",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("GET"),
            Some("/api/v1/account"),
            vec![CredentialScope::ReadOnly],
            "toobit_private_read",
            5,
        ),
        endpoint(
            "toobit.place_order",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("POST"),
            Some("/api/v1/spot/order"),
            vec![CredentialScope::Trade],
            "toobit_trade",
            1,
        ),
        endpoint(
            "toobit.cancel_order",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("DELETE"),
            Some("/api/v1/spot/order"),
            vec![CredentialScope::Trade],
            "toobit_trade",
            1,
        ),
        endpoint(
            "toobit.batch_place_orders",
            capabilities
                .capabilities_v2
                .batch_place_orders
                .support
                .clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("POST"),
            Some("/api/v1/spot/batchOrders"),
            vec![CredentialScope::Trade],
            "toobit_trade",
            5,
        ),
        endpoint(
            "toobit.batch_cancel_orders",
            capabilities
                .capabilities_v2
                .batch_cancel_orders
                .support
                .clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("DELETE"),
            Some("/api/v1/spot/cancelOrderByIds"),
            vec![CredentialScope::Trade],
            "toobit_trade",
            5,
        ),
        endpoint(
            "toobit.get_recent_fills",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("GET"),
            Some("/api/v1/account/trades"),
            vec![CredentialScope::ReadOnly],
            "toobit_private_read",
            5,
        ),
        endpoint(
            "toobit.create_listen_key",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("POST"),
            Some("/api/v1/userDataStream"),
            vec![CredentialScope::ReadOnly],
            "toobit_private_read",
            1,
        ),
        endpoint(
            "toobit.keepalive_listen_key",
            private_support,
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("PUT"),
            Some("/api/v1/userDataStream"),
            vec![CredentialScope::ReadOnly],
            "toobit_private_read",
            1,
        ),
    ];

    fn endpoint(
        operation: &str,
        support: CapabilitySupport,
        market_types: Vec<MarketType>,
        auth: EndpointAuth,
        method: Option<&str>,
        path: Option<&str>,
        credential_scopes: Vec<CredentialScope>,
        rate_limit_bucket: &str,
        weight: u32,
    ) -> EndpointCapability {
        EndpointCapability {
            operation: operation.to_string(),
            support,
            market_types,
            transport: EndpointTransport::Rest,
            method: method.map(str::to_string),
            path: path.map(str::to_string),
            auth,
            credential_scopes,
            rate_limit_bucket: Some(rate_limit_bucket.to_string()),
            weight: Some(weight),
            supports_testnet: false,
        }
    }
}
