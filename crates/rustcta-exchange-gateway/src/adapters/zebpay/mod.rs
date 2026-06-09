use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, CapabilitySupport, ExchangeApiError, ExchangeApiResult, ExchangeClient,
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
mod public;
mod signing;
mod streams;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
mod toolchain;
mod transport;

pub use config::ZebpayGatewayConfig;
use transport::ZebpayRest;

#[derive(Clone)]
pub struct ZebpayGatewayAdapter {
    exchange_id: ExchangeId,
    config: ZebpayGatewayConfig,
    rest: ZebpayRest,
}

impl ZebpayGatewayAdapter {
    pub fn new(config: ZebpayGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("zebpay").map_err(validation_error)?;
        let rest = ZebpayRest::new(
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
        Self::new(ZebpayGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("zebpay adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "zebpay.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }
}

#[async_trait]
impl GatewayAdapter for ZebpayGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                "zebpay spot public REST gateway; private readbacks are credential-gated and writes remain offline"
                    .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for ZebpayGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private_read_enabled = self.config.private_rest_configured();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private_read_enabled;
        capabilities.supports_public_streams = false;
        capabilities.supports_private_streams = false;
        capabilities.private_stream_capabilities =
            Some(streams::zebpay_private_stream_capabilities());
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = false;
        capabilities.supports_positions = false;
        capabilities.supports_fees = false;
        capabilities.supports_place_order = false;
        capabilities.supports_cancel_order = false;
        capabilities.supports_query_order = private_read_enabled;
        capabilities.supports_open_orders = private_read_enabled;
        capabilities.supports_recent_fills = private_read_enabled;
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = false;
        capabilities.supports_order_list = false;
        capabilities.supports_client_order_id = false;
        capabilities.supports_reduce_only = false;
        capabilities.supports_post_only = false;
        capabilities.supports_time_in_force = vec![TimeInForce::GTC];
        capabilities.supports_order_types = vec![OrderType::Market, OrderType::Limit];
        capabilities.max_order_book_depth = Some(50);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(50));
        capabilities.max_recent_fill_limit = None;
        toolchain::apply_toolchain_capabilities(&mut capabilities);
        capabilities.supports_private_rest = private_read_enabled;
        capabilities.supports_query_order = private_read_enabled;
        capabilities.supports_open_orders = private_read_enabled;
        capabilities.supports_recent_fills = private_read_enabled;
        capabilities.capabilities_v2.private_rest = if private_read_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "zebpay private readbacks require ZEBPAY_PRIVATE_REST_ENABLED plus client id/secret/access token; writes remain disabled",
            )
        };
        capabilities.capabilities_v2.order_history = if private_read_enabled {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_limit: true,
                max_limit: Some(500),
                ..HistoryCapability::default()
            }
        } else {
            HistoryCapability::unsupported(
                "zebpay order readbacks require ZEBPAY_PRIVATE_REST_ENABLED plus credentials",
            )
        };
        capabilities.capabilities_v2.fills_history = if private_read_enabled {
            HistoryCapability {
                support: CapabilitySupport::native(),
                ..HistoryCapability::default()
            }
        } else {
            HistoryCapability::unsupported(
                "zebpay fill readbacks require ZEBPAY_PRIVATE_REST_ENABLED plus credentials",
            )
        };
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("zebpay.balances_request_spec_only")
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange_id)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.unsupported("zebpay.positions_unsupported_spot_only")
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
        self.unsupported("zebpay.fees_unverified")
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("zebpay.place_order_request_spec_only")
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("zebpay.quote_market_order_unverified")
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("zebpay.cancel_order_request_spec_only")
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("zebpay.amend_order_unsupported")
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_spot(request.symbol().market_type)?;
        self.unsupported("zebpay.order_list_unsupported")
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        for order in &request.orders {
            self.ensure_exchange(&order.symbol.exchange)?;
            self.ensure_spot(order.symbol.market_type)?;
        }
        self.unsupported("zebpay.batch_place_orders_unverified")
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        for cancel in &request.cancels {
            self.ensure_exchange(&cancel.symbol.exchange)?;
            self.ensure_spot(cancel.symbol.market_type)?;
        }
        self.unsupported("zebpay.batch_cancel_orders_unverified")
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.unsupported("zebpay.cancel_all_orders_request_spec_only")
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.query_order_impl(request).await
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.get_open_orders_impl(request).await
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.ensure_exchange(&request.exchange)?;
        if let Some(market_type) = request.market_type {
            self.ensure_spot(market_type)?;
        }
        if let Some(symbol) = &request.symbol {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
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
