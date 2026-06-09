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
mod public;
mod signing;
mod streams;
#[cfg(test)]
mod tests;
mod transport;

pub use config::BtcTurkGatewayConfig;
use transport::BtcTurkPublicRest;

#[derive(Clone)]
pub struct BtcTurkGatewayAdapter {
    exchange_id: ExchangeId,
    config: BtcTurkGatewayConfig,
    rest: BtcTurkPublicRest,
}

impl BtcTurkGatewayAdapter {
    pub fn new(config: BtcTurkGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("btcturk").map_err(validation_error)?;
        let rest = BtcTurkPublicRest::new(
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

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("btcturk adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "btcturk.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_public_rest(&self) -> ExchangeApiResult<()> {
        if !self.config.enabled_public_rest {
            return Err(ExchangeApiError::Unsupported {
                operation: "btcturk.public_rest_disabled",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }
}

#[async_trait]
impl GatewayAdapter for BtcTurkGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("btcturk TRY spot gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for BtcTurkGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private_enabled = self.config.private_rest_enabled();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = self.config.enabled_public_rest;
        capabilities.supports_private_rest = private_enabled;
        capabilities.supports_public_streams = self.config.enabled_public_streams;
        capabilities.supports_private_streams = private_enabled;
        capabilities.private_stream_capabilities = Some(
            streams::btcturk_private_stream_capabilities(private_enabled),
        );
        capabilities.supports_symbol_rules = self.config.enabled_public_rest;
        capabilities.supports_order_book_snapshot = self.config.enabled_public_rest;
        capabilities.supports_balances = false;
        capabilities.supports_place_order = false;
        capabilities.supports_cancel_order = false;
        capabilities.supports_query_order = private_enabled;
        capabilities.supports_open_orders = private_enabled;
        capabilities.supports_recent_fills = private_enabled;
        capabilities.supports_client_order_id = true;
        capabilities.supports_time_in_force = vec![TimeInForce::GTC];
        capabilities.supports_order_types = vec![OrderType::Market, OrderType::Limit];
        capabilities.max_order_book_depth = Some(100);
        capabilities.max_recent_fill_limit = Some(1000);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(100));
        capabilities.refresh_v2_from_legacy_flags();
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("btcturk.balances_offline_request_spec_only")
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("btcturk.positions_unsupported_for_spot")
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
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.unsupported("btcturk.fees_unverified")
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("btcturk.place_order_offline_request_spec_only")
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("btcturk.quote_market_order_quantity_rules_are_side_specific")
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("btcturk.cancel_order_offline_request_spec_only")
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("btcturk.amend_order_unsupported")
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_spot(request.symbol().market_type)?;
        self.unsupported("btcturk.order_list_unsupported")
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("btcturk.batch_place_orders_unsupported")
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("btcturk.batch_cancel_orders_unsupported")
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("btcturk.cancel_all_orders_unsupported")
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
