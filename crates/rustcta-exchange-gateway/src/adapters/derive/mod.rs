use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability,
    EndpointTransport, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, HistoryCapability, OpenOrdersRequest,
    OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, SymbolRulesRequest,
    SymbolRulesResponse, TimeInForce,
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
mod transport;

#[cfg(test)]
mod tests;

pub use config::DeriveGatewayConfig;

#[derive(Clone)]
pub struct DeriveGatewayAdapter {
    exchange_id: ExchangeId,
    config: DeriveGatewayConfig,
}

impl DeriveGatewayAdapter {
    pub fn new(config: DeriveGatewayConfig) -> ExchangeApiResult<Self> {
        Ok(Self {
            exchange_id: ExchangeId::new("derive").map_err(validation_error)?,
            config,
        })
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("derive adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Perpetual | MarketType::Option) {
            return Err(ExchangeApiError::Unsupported {
                operation: "derive.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }
}

#[async_trait]
impl GatewayAdapter for DeriveGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                "derive options/perp JSON-RPC adapter: metadata and session model fixture-verified"
                    .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for DeriveGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private = self.config.private_rest_available();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Option, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_rest = private;
        capabilities.supports_private_streams = false;
        capabilities.private_stream_capabilities = Some(
            streams::derive_private_stream_capabilities(capabilities.supports_private_streams),
        );
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private;
        capabilities.supports_positions = private;
        capabilities.supports_fees = false;
        capabilities.supports_place_order = false;
        capabilities.supports_cancel_order = false;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = false;
        capabilities.supports_order_list = false;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = true;
        capabilities.supports_post_only = false;
        capabilities.supports_time_in_force = vec![TimeInForce::GTC, TimeInForce::IOC];
        capabilities.supports_order_types = vec![OrderType::Market, OrderType::Limit];
        capabilities.max_order_book_depth = Some(100);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(100));
        capabilities.max_recent_fill_limit = Some(100);
        apply_derive_capabilities_v2(&mut capabilities);
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("derive.balances_require_session_key")
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("derive.positions_require_session_key")
    }

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        self.unsupported("derive.symbol_rules_json_rpc_not_live_in_offline_adapter")
    }

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("derive.order_book_json_rpc_not_live_in_offline_adapter")
    }

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        self.unsupported("derive.fees_adapter_specific_metadata_only")
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("derive.place_order_session_signature_offline_only")
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("derive.quote_market_order_unsupported")
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("derive.cancel_order_session_signature_offline_only")
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported(private::AMEND_ORDER_UNSUPPORTED)
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_supported_market_type(request.symbol().market_type)?;
        self.unsupported("derive.order_list_unsupported")
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_supported_market_type(order.symbol.market_type)?;
        }
        self.unsupported("derive.batch_place_orders_unsupported")
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("derive.batch_cancel_orders_unsupported")
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("derive.cancel_all_orders_requires_admin_session")
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("derive.query_order_requires_session_key")
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("derive.open_orders_require_session_key")
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("derive.recent_fills_require_session_key")
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

fn apply_derive_capabilities_v2(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "derive private JSON-RPC has request-spec/source fixtures, but session/JWT/Stark signing and live reconciliation are not enabled",
    );
    capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
    capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
        "derive private WS auth is fixture-only until session login and REST reconciliation are live-validated",
    );
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("derive.batch_place_orders_unsupported");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("derive.batch_cancel_orders_unsupported");
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("derive.cancel_all_orders_requires_admin_session");
    capabilities.capabilities_v2.order_history =
        HistoryCapability::unsupported("derive.order_history_requires_session_key");
    capabilities.capabilities_v2.fills_history =
        HistoryCapability::unsupported("derive.fills_history_requires_session_key");
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];
    capabilities.capabilities_v2.endpoints = derive_capability_endpoints();
}

fn derive_capability_endpoints() -> Vec<EndpointCapability> {
    vec![
        derive_endpoint(
            "derive.symbol_rules",
            CapabilitySupport::native(),
            EndpointAuth::None,
            vec![],
        ),
        derive_endpoint(
            "derive.order_book",
            CapabilitySupport::native(),
            EndpointAuth::None,
            vec![],
        ),
        derive_endpoint(
            "derive.place_order",
            CapabilitySupport::unsupported("derive.place_order_session_signature_offline_only"),
            EndpointAuth::Jwt,
            vec![CredentialScope::Trade],
        ),
        derive_endpoint(
            "derive.cancel_order",
            CapabilitySupport::unsupported("derive.cancel_order_session_signature_offline_only"),
            EndpointAuth::Jwt,
            vec![CredentialScope::Trade],
        ),
        derive_endpoint(
            "derive.amend_order",
            CapabilitySupport::unsupported(private::AMEND_ORDER_UNSUPPORTED),
            EndpointAuth::Jwt,
            vec![CredentialScope::Trade],
        ),
        derive_endpoint(
            "derive.place_order_list",
            CapabilitySupport::unsupported("derive.order_list_unsupported"),
            EndpointAuth::None,
            vec![],
        ),
        derive_endpoint(
            "derive.batch_place_orders",
            CapabilitySupport::unsupported("derive.batch_place_orders_unsupported"),
            EndpointAuth::None,
            vec![],
        ),
        derive_endpoint(
            "derive.batch_cancel_orders",
            CapabilitySupport::unsupported("derive.batch_cancel_orders_unsupported"),
            EndpointAuth::None,
            vec![],
        ),
    ]
}

fn derive_endpoint(
    operation: &str,
    support: CapabilitySupport,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Option, MarketType::Perpetual],
        transport: EndpointTransport::JsonRpc,
        method: Some("POST".to_string()),
        path: Some("/".to_string()),
        auth,
        credential_scopes,
        rate_limit_bucket: Some("derive_json_rpc".to_string()),
        weight: Some(1),
        supports_testnet: false,
    }
}
