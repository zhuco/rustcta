use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest,
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

pub use config::CoinbaseGatewayConfig;
use transport::CoinbaseRest;

#[derive(Clone)]
pub struct CoinbaseGatewayAdapter {
    exchange_id: ExchangeId,
    config: CoinbaseGatewayConfig,
    rest: CoinbaseRest,
}

impl CoinbaseGatewayAdapter {
    pub fn new(config: CoinbaseGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("coinbase").map_err(validation_error)?;
        let rest = CoinbaseRest::new(
            exchange_id.clone(),
            config.spot_rest_base_url.clone(),
            config.international_rest_base_url.clone(),
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
        Self::new(CoinbaseGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("coinbase adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbase.unsupported_market_type",
            });
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn unsupported_private<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn ensure_private_rest(&self, operation: &'static str) -> ExchangeApiResult<()> {
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }

    fn optional_bearer_token(&self) -> Option<String> {
        (!self.config.bearer_token.trim().is_empty()).then(|| self.config.bearer_token.clone())
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
                    message: "coinbase private REST request requires tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinbase private REST request requires account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_signed_get(endpoint, params, &self.config.bearer_token)
            .await
    }

    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_signed_post(endpoint, params, body, &self.config.bearer_token)
            .await
    }
}

#[async_trait]
impl GatewayAdapter for CoinbaseGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("coinbase spot and intx perpetual REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for CoinbaseGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        let private_enabled = self.config.private_rest_enabled();
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private_enabled;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = private_enabled;
        capabilities.private_stream_capabilities =
            Some(rustcta_exchange_api::PrivateStreamCapabilities {
                schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
                supports_orders: private_enabled,
                supports_fills: private_enabled,
                supports_balances: false,
                supports_positions: private_enabled,
                supports_account: private_enabled,
                order_event_kinds: vec![
                    rustcta_exchange_api::PrivateOrderStreamEventKind::New,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::PartialFill,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Fill,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Cancel,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Reject,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Expired,
                ],
                supports_client_order_id: true,
                supports_exchange_order_id: true,
            });
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private_enabled;
        capabilities.supports_positions = private_enabled;
        capabilities.supports_fees = private_enabled;
        capabilities.supports_place_order = private_enabled;
        capabilities.supports_cancel_order = private_enabled;
        capabilities.supports_query_order = private_enabled;
        capabilities.supports_open_orders = private_enabled;
        capabilities.supports_recent_fills = private_enabled;
        capabilities.supports_batch_place_order = private_enabled;
        capabilities.supports_batch_cancel_order = private_enabled;
        capabilities.supports_cancel_all_orders = private_enabled;
        capabilities.supports_quote_market_order = private_enabled;
        capabilities.supports_amend_order = private_enabled;
        capabilities.supports_order_list = private_enabled;
        capabilities.supports_client_order_id = true;
        capabilities.supports_post_only = true;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK];
        capabilities.supports_order_types = vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::PostOnly,
            OrderType::IOC,
            OrderType::FOK,
        ];
        capabilities.max_order_book_depth = Some(100);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(100));
        capabilities.max_recent_fill_limit = Some(100);
        capabilities.refresh_v2_from_legacy_flags();
        apply_coinbase_capabilities_v2(&mut capabilities, private_enabled);
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

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.place_order_list_impl(request).await
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

fn apply_coinbase_capabilities_v2(
    capabilities: &mut ExchangeClientCapabilities,
    private_enabled: bool,
) {
    use rustcta_exchange_api::{
        BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
        EndpointAuth, EndpointCapability, EndpointTransport, HeartbeatCapability,
        HistoryCapability, ReconnectCapability, StreamAuthCapability, StreamHeartbeatDirection,
        StreamResyncCapability, StreamRuntimeCapability,
    };

    let private_support = if private_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "coinbase private REST requires a configured bearer/JWT token",
        )
    };
    let scopes = if private_enabled {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    };

    capabilities.capabilities_v2.credential_scopes = scopes.clone();
    capabilities.capabilities_v2.batch_place_orders = if private_enabled {
        BatchCapability {
            support: CapabilitySupport::composed(
                "Coinbase Advanced Trade batch place is composed from sequential /orders calls",
            ),
            mode: BatchExecutionMode::ComposedSequential,
            atomicity: BatchAtomicity::NonAtomic,
            max_items: Some(20),
            same_symbol_required: false,
            same_market_type_required: false,
            supports_client_order_id: true,
            supports_partial_failure: false,
        }
    } else {
        BatchCapability::unsupported("coinbase private REST is disabled")
    };
    capabilities.capabilities_v2.batch_cancel_orders = if private_enabled {
        BatchCapability {
            support: CapabilitySupport::native(),
            mode: BatchExecutionMode::Native,
            atomicity: BatchAtomicity::Partial,
            max_items: Some(100),
            same_symbol_required: false,
            same_market_type_required: false,
            supports_client_order_id: true,
            supports_partial_failure: true,
        }
    } else {
        BatchCapability::unsupported("coinbase private REST is disabled")
    };
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: private_support.clone(),
        supports_since: false,
        supports_until: false,
        supports_limit: true,
        supports_cursor: true,
        supports_from_id: false,
        max_limit: Some(100),
        max_window_ms: None,
    };
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: private_support.clone(),
        supports_since: true,
        supports_until: true,
        supports_limit: true,
        supports_cursor: true,
        supports_from_id: false,
        max_limit: Some(100),
        max_window_ms: None,
    };
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: private_support.clone(),
        supports_subscribe: true,
        supports_unsubscribe: false,
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
            balances: false,
            positions: true,
            orders: true,
        },
        auth: StreamAuthCapability {
            required: private_enabled,
            credential_scopes: scopes.clone(),
            renewal_ms: Some(120_000),
            uses_listen_key: false,
            requires_relogin_on_reconnect: true,
        },
        ..StreamRuntimeCapability::default()
    };
    capabilities.capabilities_v2.endpoints = vec![
        endpoint(
            "coinbase.get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::None,
            Some("GET"),
            Some("/products"),
            vec![],
            "coinbase_public",
            1,
        ),
        endpoint(
            "coinbase.get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::None,
            Some("GET"),
            Some("/product_book"),
            vec![],
            "coinbase_public",
            1,
        ),
        endpoint(
            "coinbase.get_balances",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Bearer,
            Some("GET"),
            Some("/accounts"),
            vec![CredentialScope::ReadOnly],
            "coinbase_private_read",
            1,
        ),
        endpoint(
            "coinbase.place_order",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Bearer,
            Some("POST"),
            Some("/orders"),
            vec![CredentialScope::Trade],
            "coinbase_orders",
            1,
        ),
        endpoint(
            "coinbase.cancel_order",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Bearer,
            Some("POST"),
            Some("/orders/batch_cancel"),
            vec![CredentialScope::Trade],
            "coinbase_orders",
            1,
        ),
        endpoint(
            "coinbase.batch_place_orders",
            capabilities
                .capabilities_v2
                .batch_place_orders
                .support
                .clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Bearer,
            Some("POST"),
            Some("/orders"),
            vec![CredentialScope::Trade],
            "coinbase_orders",
            1,
        ),
        endpoint(
            "coinbase.batch_cancel_orders",
            capabilities
                .capabilities_v2
                .batch_cancel_orders
                .support
                .clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Bearer,
            Some("POST"),
            Some("/orders/batch_cancel"),
            vec![CredentialScope::Trade],
            "coinbase_orders",
            1,
        ),
        endpoint(
            "coinbase.get_recent_fills",
            private_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Bearer,
            Some("GET"),
            Some("/orders/historical/fills"),
            vec![CredentialScope::ReadOnly],
            "coinbase_private_read",
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
