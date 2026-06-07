use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, SymbolRulesRequest,
    SymbolRulesResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
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

pub use config::WooGatewayConfig;
use signing::WooPrivateCredentials;
use transport::WooRest;

#[derive(Clone)]
pub struct WooGatewayAdapter {
    exchange_id: ExchangeId,
    config: WooGatewayConfig,
    rest: WooRest,
}

impl WooGatewayAdapter {
    pub fn new(config: WooGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("woo").map_err(validation_error)?;
        let credentials = if config.has_private_credentials() {
            Some(WooPrivateCredentials {
                api_key: config.api_key.clone().unwrap_or_default(),
                api_secret: config.api_secret.clone().unwrap_or_default(),
            })
        } else {
            None
        };
        let rest = WooRest::new(
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
                message: format!("woo adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(
            market_type,
            MarketType::Spot | MarketType::Perpetual | MarketType::Futures
        ) {
            return Err(ExchangeApiError::Unsupported {
                operation: "woo.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn ensure_perp(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Perpetual | MarketType::Futures) {
            return Err(ExchangeApiError::Unsupported {
                operation: "woo.non_perp_market_type",
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

    fn context_account(
        &self,
        context: &rustcta_exchange_api::RequestContext,
        operation: &str,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires tenant_id in request context"),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires account_id in request context"),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for WooGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("woo spot/perpetual REST and WebSocket gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for WooGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private_rest = self.config.private_rest_available();
        let private_stream = self.config.private_stream_available();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private_rest;
        capabilities.supports_public_streams = self.config.enabled_public_stream;
        capabilities.supports_private_streams = private_stream;
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private_rest;
        capabilities.supports_positions = private_rest;
        capabilities.supports_fees = private_rest;
        capabilities.supports_place_order = private_rest;
        capabilities.supports_cancel_order = private_rest;
        capabilities.supports_query_order = private_rest;
        capabilities.supports_open_orders = private_rest;
        capabilities.supports_recent_fills = private_rest;
        capabilities.supports_cancel_all_orders = private_rest;
        capabilities.supports_batch_place_order = private_rest;
        capabilities.supports_batch_cancel_order = private_rest;
        capabilities.supports_quote_market_order = private_rest;
        capabilities.supports_amend_order = private_rest;
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
        capabilities.max_order_book_depth = Some(100);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::best_effort_delta(Some(100));
        capabilities.max_recent_fill_limit = Some(500);
        capabilities.private_stream_capabilities = Some(PrivateStreamCapabilities {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            supports_orders: private_stream,
            supports_fills: private_stream,
            supports_balances: private_stream,
            supports_positions: private_stream,
            supports_account: private_stream,
            order_event_kinds: vec![
                PrivateOrderStreamEventKind::New,
                PrivateOrderStreamEventKind::PartialFill,
                PrivateOrderStreamEventKind::Fill,
                PrivateOrderStreamEventKind::Cancel,
                PrivateOrderStreamEventKind::Reject,
                PrivateOrderStreamEventKind::Expired,
                PrivateOrderStreamEventKind::BalanceUpdate,
            ],
            supports_client_order_id: true,
            supports_exchange_order_id: true,
        });
        capabilities.refresh_v2_from_legacy_flags();
        apply_woo_capabilities_v2(&mut capabilities, private_rest, private_stream);
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

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.amend_order_impl(request).await
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

fn apply_woo_capabilities_v2(
    capabilities: &mut ExchangeClientCapabilities,
    private_rest: bool,
    private_stream: bool,
) {
    use rustcta_exchange_api::{
        BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, CredentialScope,
        EndpointAuth, EndpointCapability, EndpointTransport, HeartbeatCapability,
        HistoryCapability, ReconnectCapability, StreamAuthCapability, StreamHeartbeatDirection,
        StreamResyncCapability, StreamRuntimeCapability,
    };

    let private_rest_support = if private_rest {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("woo private REST requires API key and secret")
    };
    let private_stream_support = if private_stream {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("woo private stream requires API key, secret, and listenKey")
    };
    let scopes = if private_rest || private_stream {
        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
    } else {
        Vec::new()
    };

    capabilities.capabilities_v2.credential_scopes = scopes.clone();
    capabilities.capabilities_v2.batch_place_orders = if private_rest {
        BatchCapability {
            support: CapabilitySupport::composed(
                "WOO ordinary order batch place is composed from sequential /v3/trade/order calls",
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
        BatchCapability::unsupported("woo private REST is disabled")
    };
    capabilities.capabilities_v2.batch_cancel_orders = if private_rest {
        BatchCapability {
            support: CapabilitySupport::composed("WOO ordinary order batch cancel is composed from sequential DELETE /v3/trade/order calls"),
            mode: BatchExecutionMode::ComposedSequential,
            atomicity: BatchAtomicity::NonAtomic,
            max_items: Some(20),
            same_symbol_required: false,
            same_market_type_required: false,
            supports_client_order_id: true,
            supports_partial_failure: false,
        }
    } else {
        BatchCapability::unsupported("woo private REST is disabled")
    };
    capabilities.capabilities_v2.order_history = HistoryCapability {
        support: private_rest_support.clone(),
        supports_since: false,
        supports_until: false,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: false,
        max_limit: Some(500),
        max_window_ms: None,
    };
    capabilities.capabilities_v2.fills_history = HistoryCapability {
        support: private_rest_support.clone(),
        supports_since: true,
        supports_until: true,
        supports_limit: true,
        supports_cursor: false,
        supports_from_id: true,
        max_limit: Some(500),
        max_window_ms: None,
    };
    capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
        public: CapabilitySupport::native(),
        private: private_stream_support.clone(),
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
            required: private_stream,
            credential_scopes: scopes.clone(),
            renewal_ms: Some(3_300_000),
            uses_listen_key: true,
            requires_relogin_on_reconnect: true,
        },
        ..StreamRuntimeCapability::default()
    };
    capabilities.capabilities_v2.endpoints = vec![
        endpoint(
            "woo.get_symbol_rules",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::None,
            Some("GET"),
            Some("/v3/public/instruments"),
            vec![],
            "woo_public",
            1,
        ),
        endpoint(
            "woo.get_order_book",
            CapabilitySupport::native(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::None,
            Some("GET"),
            Some("/v3/public/orderbook"),
            vec![],
            "woo_public",
            1,
        ),
        endpoint(
            "woo.get_balances",
            private_rest_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("GET"),
            Some("/v3/asset/balances"),
            vec![CredentialScope::ReadOnly],
            "woo_private_read",
            1,
        ),
        endpoint(
            "woo.place_order",
            private_rest_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("POST"),
            Some("/v3/trade/order"),
            vec![CredentialScope::Trade],
            "woo_trade",
            1,
        ),
        endpoint(
            "woo.cancel_order",
            private_rest_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("DELETE"),
            Some("/v3/trade/order"),
            vec![CredentialScope::Trade],
            "woo_trade",
            1,
        ),
        endpoint(
            "woo.batch_place_orders",
            capabilities
                .capabilities_v2
                .batch_place_orders
                .support
                .clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("POST"),
            Some("/v3/trade/order"),
            vec![CredentialScope::Trade],
            "woo_trade",
            1,
        ),
        endpoint(
            "woo.batch_cancel_orders",
            capabilities
                .capabilities_v2
                .batch_cancel_orders
                .support
                .clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("DELETE"),
            Some("/v3/trade/order"),
            vec![CredentialScope::Trade],
            "woo_trade",
            1,
        ),
        endpoint(
            "woo.get_recent_fills",
            private_rest_support.clone(),
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("GET"),
            Some("/v3/trade/transactionHistory"),
            vec![CredentialScope::ReadOnly],
            "woo_private_read",
            1,
        ),
        endpoint(
            "woo.get_listen_key",
            private_rest_support,
            vec![MarketType::Spot, MarketType::Perpetual],
            EndpointAuth::Hmac,
            Some("POST"),
            Some("/v3/account/listenKey"),
            vec![CredentialScope::ReadOnly],
            "woo_private_read",
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
