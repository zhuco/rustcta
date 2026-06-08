use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, CapabilitySupport, CredentialScope, ExchangeApiError, ExchangeApiResult,
    ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse, HistoryCapability,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookCapability, OrderBookRequest,
    OrderBookResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, SymbolRulesRequest, SymbolRulesResponse, TimeInForce,
    EXCHANGE_API_SCHEMA_VERSION,
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
                "hitbtc HitBTC-family spot public REST adapter; private/WS are spec-only"
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
        capabilities.supports_private_rest = false;
        capabilities.supports_public_streams = false;
        capabilities.supports_private_streams = false;
        capabilities.private_stream_capabilities = Some(PrivateStreamCapabilities::unsupported(
            EXCHANGE_API_SCHEMA_VERSION,
        ));
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = false;
        capabilities.supports_positions = false;
        capabilities.supports_fees = false;
        capabilities.supports_place_order = false;
        capabilities.supports_cancel_order = false;
        capabilities.supports_query_order = false;
        capabilities.supports_open_orders = false;
        capabilities.supports_recent_fills = false;
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = false;
        capabilities.supports_order_list = false;
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
        capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
            "hitbtc private REST is delivered as offline request-spec/signing vectors only",
        );
        capabilities.capabilities_v2.public_streams = CapabilitySupport::unsupported(
            "hitbtc public WebSocket payloads are spec fixtures; runtime supervisor is not wired",
        );
        capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
            "hitbtc private WebSocket auth payload is spec-only; use REST reconciliation after reconnect",
        );
        capabilities.capabilities_v2.order_history = HistoryCapability::unsupported(
            "hitbtc order history private REST is not enabled in runtime adapter",
        );
        capabilities.capabilities_v2.fills_history = HistoryCapability::unsupported(
            "hitbtc fills history private REST is not enabled in runtime adapter",
        );
        capabilities.capabilities_v2.credential_scopes =
            vec![CredentialScope::ReadOnly, CredentialScope::Trade];
        capabilities
    }

    async fn get_balances(&self, _request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.unsupported("hitbtc.get_balances_spec_only")
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

    async fn get_fees(&self, _request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        self.unsupported("hitbtc.get_fees_spec_only")
    }

    async fn place_order(
        &self,
        _request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.unsupported("hitbtc.place_order_request_spec_only")
    }

    async fn place_quote_market_order(
        &self,
        _request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.unsupported("hitbtc.quote_market_order_request_spec_only")
    }

    async fn cancel_order(
        &self,
        _request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.unsupported("hitbtc.cancel_order_request_spec_only")
    }

    async fn amend_order(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported("hitbtc.amend_order_request_spec_only")
    }

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.unsupported("hitbtc.order_list_request_spec_only")
    }

    async fn batch_place_orders(
        &self,
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.unsupported("hitbtc.batch_place_orders_not_mapped")
    }

    async fn batch_cancel_orders(
        &self,
        _request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.unsupported("hitbtc.batch_cancel_orders_not_mapped")
    }

    async fn cancel_all_orders(
        &self,
        _request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.unsupported("hitbtc.cancel_all_orders_request_spec_only")
    }

    async fn query_order(
        &self,
        _request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.unsupported("hitbtc.query_order_spec_only")
    }

    async fn get_open_orders(
        &self,
        _request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.unsupported("hitbtc.get_open_orders_spec_only")
    }

    async fn get_recent_fills(
        &self,
        _request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.unsupported("hitbtc.get_recent_fills_spec_only")
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

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
