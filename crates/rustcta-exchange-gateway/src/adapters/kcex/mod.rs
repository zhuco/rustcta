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
    SymbolRulesResponse, TimeInForce,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
mod streams;
#[cfg(test)]
mod tests;
mod toolchain;

pub use config::KcexGatewayConfig;

#[derive(Clone)]
pub struct KcexGatewayAdapter {
    exchange_id: ExchangeId,
    config: KcexGatewayConfig,
}

impl KcexGatewayAdapter {
    pub fn new(config: KcexGatewayConfig) -> ExchangeApiResult<Self> {
        Ok(Self {
            exchange_id: ExchangeId::new("kcex").map_err(validation_error)?,
            config,
        })
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("kcex adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "kcex.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }
}

#[async_trait]
impl GatewayAdapter for KcexGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        let message = if self.config.private_rest_requested() {
            "kcex spot/perpetual gateway registered; credentials present but official OpenAPI remains unverified"
        } else {
            "kcex spot/perpetual gateway registered with live API surfaces disabled"
        };
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(message.to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for KcexGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = false;
        capabilities.supports_private_rest = false;
        capabilities.supports_public_streams = false;
        capabilities.supports_private_streams = false;
        capabilities.private_stream_capabilities = Some(PrivateStreamCapabilities::unsupported(
            rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
        ));
        capabilities.supports_symbol_rules = false;
        capabilities.supports_order_book_snapshot = false;
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
        capabilities.supports_client_order_id = false;
        capabilities.supports_reduce_only = true;
        capabilities.supports_post_only = true;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK];
        capabilities.supports_order_types = vec![OrderType::Market, OrderType::Limit];
        capabilities.max_order_book_depth = None;
        capabilities.order_book = rustcta_exchange_api::OrderBookCapability::snapshot_only(None);
        capabilities.max_recent_fill_limit = None;
        toolchain::apply_toolchain_capabilities(&mut capabilities);
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("kcex.balances_unverified_openapi")
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("kcex.positions_unverified_openapi")
    }

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        self.unsupported("kcex.symbol_rules_unverified_openapi")
    }

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("kcex.order_book_unverified_openapi")
    }

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        self.unsupported("kcex.fees_unverified_openapi")
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("kcex.place_order_unverified_openapi")
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("kcex.quote_market_order_unverified_openapi")
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("kcex.cancel_order_unverified_openapi")
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("kcex.amend_order_unverified_openapi")
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_supported_market_type(request.symbol().market_type)?;
        self.unsupported("kcex.order_list_unverified_openapi")
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
        self.unsupported("kcex.batch_place_orders_unverified_openapi")
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("kcex.batch_cancel_orders_unverified_openapi")
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("kcex.cancel_all_orders_unverified_openapi")
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("kcex.query_order_unverified_openapi")
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("kcex.open_orders_unverified_openapi")
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("kcex.recent_fills_unverified_openapi")
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
