use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, PrivateStreamCapabilities,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, SymbolRulesRequest,
    SymbolRulesResponse, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, HistoryCapability,
};
use rustcta_types::{ExchangeId, MarketType, OrderType, TimeInForce};

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

pub use config::OrangeXGatewayConfig;
#[allow(unused_imports)]
pub use private::{OrangeXMarginMode, OrangeXPrivateAck};
use transport::OrangeXRest;

#[derive(Clone)]
pub struct OrangeXGatewayAdapter {
    exchange_id: ExchangeId,
    config: OrangeXGatewayConfig,
    rest: OrangeXRest,
}

impl OrangeXGatewayAdapter {
    pub fn new(config: OrangeXGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("orangex").map_err(validation_error)?;
        let rest = OrangeXRest::new(
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
        Self::new(OrangeXGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("orangex adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "orangex.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }
}

#[async_trait]
impl GatewayAdapter for OrangeXGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("orangex JSON-RPC public REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for OrangeXGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        let private_rest = self.config.private_rest_available();
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_rest = private_rest;
        capabilities.supports_private_streams = false;
        capabilities.private_stream_capabilities = Some(PrivateStreamCapabilities::unsupported(
            EXCHANGE_API_SCHEMA_VERSION,
        ));
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private_rest;
        capabilities.supports_positions = private_rest;
        capabilities.supports_fees = true;
        capabilities.supports_place_order = private_rest;
        capabilities.supports_cancel_order = private_rest;
        capabilities.supports_cancel_all_orders = private_rest;
        capabilities.supports_query_order = private_rest;
        capabilities.supports_open_orders = private_rest;
        capabilities.supports_recent_fills = private_rest;
        capabilities.supports_batch_place_order = private_rest;
        capabilities.supports_batch_cancel_order = private_rest;
        capabilities.supports_quote_market_order = private_rest;
        capabilities.supports_client_order_id = private_rest;
        capabilities.supports_reduce_only = private_rest;
        capabilities.supports_post_only = private_rest;
        capabilities.supports_time_in_force = if private_rest {
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK]
        } else {
            Vec::new()
        };
        capabilities.supports_order_types = if private_rest {
            vec![
                OrderType::Market,
                OrderType::Limit,
                OrderType::PostOnly,
                OrderType::IOC,
                OrderType::FOK,
            ]
        } else {
            Vec::new()
        };
        capabilities.max_order_book_depth = Some(100);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(100));
        capabilities.max_recent_fill_limit = Some(100);
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_streams = CapabilitySupport::rest_fallback(
            "OrangeX WebSocket API is deprecated; public WS remains spec/parser only with REST snapshot resync",
        );
        capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
            "orangex.subscribe_private_stream.deprecated_official_websocket_api",
        );
        capabilities.capabilities_v2.batch_place_orders = if private_rest {
            BatchCapability {
                support: CapabilitySupport::composed(
                    "No verified native batch endpoint; adapter sequentially calls /private/buy and /private/sell",
                ),
                mode: BatchExecutionMode::ComposedSequential,
                atomicity: BatchAtomicity::NonAtomic,
                max_items: None,
                same_symbol_required: false,
                same_market_type_required: false,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("OrangeX batch place requires private REST token")
        };
        capabilities.capabilities_v2.batch_cancel_orders = if private_rest {
            BatchCapability {
                support: CapabilitySupport::composed(
                    "No verified native batch cancel endpoint; adapter sequentially calls /private/cancel",
                ),
                mode: BatchExecutionMode::ComposedSequential,
                atomicity: BatchAtomicity::NonAtomic,
                max_items: None,
                same_symbol_required: false,
                same_market_type_required: false,
                supports_client_order_id: false,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("OrangeX batch cancel requires private REST token")
        };
        capabilities.capabilities_v2.fills_history = HistoryCapability {
            support: if private_rest {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("OrangeX fills require private REST token")
            },
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: Some(100),
            max_window_ms: None,
        };
        capabilities.apply_v2_to_legacy_flags();
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

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.cancel_order_private_rest(request).await
    }

    async fn amend_order(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported("orangex.amend_order")
    }

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.unsupported("orangex.place_order_list")
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.batch_place_orders_private_rest(request).await
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.batch_cancel_orders_private_rest(request).await
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
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
        _subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported("orangex.subscribe_private_stream.deprecated_official_websocket_api")
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
