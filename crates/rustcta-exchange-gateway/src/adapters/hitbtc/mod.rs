use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse, BatchAtomicity,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability, BatchExecutionMode,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    CredentialScope, EndpointAuth, EndpointCapability, EndpointTransport, ExchangeApiError,
    ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse,
    HistoryCapability, OpenOrdersRequest, OpenOrdersResponse, OrderBookCapability,
    OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, PrivateStreamCapabilities,
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

pub use config::HitbtcGatewayConfig;
use transport::HitbtcRest;

#[derive(Clone)]
pub struct HitbtcGatewayAdapter {
    exchange_id: ExchangeId,
    config: HitbtcGatewayConfig,
    rest: HitbtcRest,
}

impl HitbtcGatewayAdapter {
    pub fn new(config: HitbtcGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("hitbtc").map_err(validation_error)?;
        let rest = HitbtcRest::new(
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
        Self::new(HitbtcGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("hitbtc adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "hitbtc.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }
}

#[async_trait]
impl GatewayAdapter for HitbtcGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                "hitbtc HitBTC-family spot REST adapter; advanced private REST is guarded by explicit credentials"
                    .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for HitbtcGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = self.config.private_request_specs_enabled();
        capabilities.supports_public_streams = false;
        capabilities.supports_private_streams = false;
        capabilities.private_stream_capabilities = Some(PrivateStreamCapabilities::unsupported(
            EXCHANGE_API_SCHEMA_VERSION,
        ));
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = self.config.private_request_specs_enabled();
        capabilities.supports_positions = false;
        capabilities.supports_fees = self.config.private_request_specs_enabled();
        capabilities.supports_place_order = self.config.private_request_specs_enabled();
        capabilities.supports_cancel_order = self.config.private_request_specs_enabled();
        capabilities.supports_query_order = self.config.private_request_specs_enabled();
        capabilities.supports_open_orders = self.config.private_request_specs_enabled();
        capabilities.supports_recent_fills = self.config.private_request_specs_enabled();
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = self.config.private_request_specs_enabled();
        capabilities.supports_order_list = self.config.private_request_specs_enabled();
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = false;
        capabilities.supports_post_only = false;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK];
        capabilities.supports_order_types = vec![OrderType::Market, OrderType::Limit];
        capabilities.max_order_book_depth = Some(100);
        capabilities.order_book = OrderBookCapability::snapshot_only(Some(100));
        capabilities.max_recent_fill_limit = None;
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.private_rest = if self.config.private_request_specs_enabled() {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "hitbtc private REST runtime requires HITBTC_PRIVATE_REST_ENABLED plus API key/secret; request specs remain offline-safe by default",
            )
        };
        capabilities.capabilities_v2.public_streams = CapabilitySupport::unsupported(
            "hitbtc public WebSocket payloads are spec fixtures; runtime supervisor is not wired",
        );
        capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
            "hitbtc private WebSocket auth payload is spec-only; use REST reconciliation after reconnect",
        );
        capabilities.capabilities_v2.order_history = if self.config.private_request_specs_enabled()
        {
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
            HistoryCapability::unsupported(
                "hitbtc order history private REST requires HITBTC_PRIVATE_REST_ENABLED plus API key/secret",
            )
        };
        capabilities.capabilities_v2.fills_history = if self.config.private_request_specs_enabled()
        {
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
            HistoryCapability::unsupported(
                "hitbtc fills history private REST requires HITBTC_PRIVATE_REST_ENABLED plus API key/secret",
            )
        };
        capabilities.capabilities_v2.credential_scopes =
            vec![CredentialScope::ReadOnly, CredentialScope::Trade];
        apply_hitbtc_capabilities_v2(&mut capabilities);
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_impl(request).await
    }

    async fn get_positions(
        &self,
        _request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.unsupported("hitbtc.spot_only_positions")
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
        self.unsupported("hitbtc.quote_market_order_request_spec_only")
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
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.unsupported("hitbtc.batch_place_orders_not_mapped")
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("hitbtc.batch_cancel_orders_filter_endpoint_overcancel_boundary")
    }

    async fn cancel_all_orders(
        &self,
        _request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.unsupported("hitbtc.cancel_all_orders_request_spec_only")
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
        _subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported("hitbtc.public_stream_runtime_not_wired")
    }

    async fn subscribe_private_stream(
        &self,
        _subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported("hitbtc.private_stream_runtime_not_wired")
    }
}

fn apply_hitbtc_capabilities_v2(capabilities: &mut ExchangeClientCapabilities) {
    let advanced_runtime = CapabilitySupport::native();
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("hitbtc generic native batch-place has no lossless mapping");
    capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
        support: CapabilitySupport::unsupported(
            "hitbtc DELETE /spot/order is cancel-all/filter-by-symbol; shared batch_cancel_orders would over-cancel a specific id list, so runtime remains offline",
        ),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::Partial,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: false,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.cancel_all_orders = CapabilitySupport::unsupported(
        "hitbtc cancel-all/filter endpoint is request-spec-only; runtime remains disabled",
    );
    capabilities.capabilities_v2.endpoints = vec![
        hitbtc_endpoint(
            "hitbtc.get_symbol_rules",
            CapabilitySupport::native(),
            EndpointAuth::None,
            "GET",
            "/public/symbol",
            vec![],
            "hitbtc_public_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.get_order_book",
            CapabilitySupport::native(),
            EndpointAuth::None,
            "GET",
            "/public/orderbook/{symbol}",
            vec![],
            "hitbtc_public_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.get_balances",
            advanced_runtime.clone(),
            EndpointAuth::Hmac,
            "GET",
            "/spot/balance",
            vec![CredentialScope::ReadOnly],
            "hitbtc_order_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.get_fees",
            advanced_runtime.clone(),
            EndpointAuth::Hmac,
            "GET",
            "/spot/fee",
            vec![CredentialScope::ReadOnly],
            "hitbtc_order_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.place_order",
            advanced_runtime.clone(),
            EndpointAuth::Hmac,
            "POST",
            "/spot/order",
            vec![CredentialScope::Trade],
            "hitbtc_order_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.cancel_order",
            advanced_runtime.clone(),
            EndpointAuth::Hmac,
            "DELETE",
            "/spot/order/{client_order_id}",
            vec![CredentialScope::Trade],
            "hitbtc_order_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.amend_order",
            advanced_runtime.clone(),
            EndpointAuth::Hmac,
            "PATCH",
            "/spot/order/{client_order_id}",
            vec![CredentialScope::Trade],
            "hitbtc_order_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.place_order_list",
            advanced_runtime.clone(),
            EndpointAuth::Hmac,
            "POST",
            "/spot/order/list",
            vec![CredentialScope::Trade],
            "hitbtc_order_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.query_order",
            advanced_runtime.clone(),
            EndpointAuth::Hmac,
            "GET",
            "/spot/order/{client_order_id}",
            vec![CredentialScope::ReadOnly],
            "hitbtc_order_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.get_open_orders",
            advanced_runtime.clone(),
            EndpointAuth::Hmac,
            "GET",
            "/spot/order",
            vec![CredentialScope::ReadOnly],
            "hitbtc_order_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.get_recent_fills",
            advanced_runtime,
            EndpointAuth::Hmac,
            "GET",
            "/spot/history/trade",
            vec![CredentialScope::ReadOnly],
            "hitbtc_order_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.batch_place_orders",
            capabilities
                .capabilities_v2
                .batch_place_orders
                .support
                .clone(),
            EndpointAuth::Hmac,
            "UNSUPPORTED",
            "/unsupported/hitbtc/batch_place_orders",
            vec![CredentialScope::Trade],
            "hitbtc_order_rest",
        ),
        hitbtc_endpoint(
            "hitbtc.batch_cancel_orders",
            capabilities
                .capabilities_v2
                .batch_cancel_orders
                .support
                .clone(),
            EndpointAuth::Hmac,
            "DELETE",
            "/spot/order",
            vec![CredentialScope::Trade],
            "hitbtc_order_rest",
        ),
    ];
}

fn hitbtc_endpoint(
    operation: &str,
    support: CapabilitySupport,
    auth: EndpointAuth,
    method: &str,
    path: &str,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: &str,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Spot],
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth,
        credential_scopes,
        rate_limit_bucket: Some(rate_limit_bucket.to_string()),
        weight: Some(1),
        supports_testnet: false,
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
