use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, CapabilitySupport, CredentialScope, ExchangeApiError, ExchangeApiResult,
    ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse, HistoryCapability,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest,
    OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamCapabilities, PrivateStreamSubscription, PublicStreamSubscription,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, SymbolRulesRequest, SymbolRulesResponse,
};
use rustcta_types::{ExchangeId, MarketType};

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
mod toolchain;
mod transport;

pub use config::AarkGatewayConfig;
use transport::AarkRest;

#[derive(Clone)]
pub struct AarkGatewayAdapter {
    exchange_id: ExchangeId,
    config: AarkGatewayConfig,
    rest: AarkRest,
}

impl AarkGatewayAdapter {
    pub fn new(config: AarkGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("aark").map_err(validation_error)?;
        let rest = AarkRest::new(
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
                message: format!("aark adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Perpetual {
            return Err(ExchangeApiError::Unsupported {
                operation: "aark.unsupported_market_type",
            });
        }
        Ok(())
    }
}

#[async_trait]
impl GatewayAdapter for AarkGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                "aark Orderly perpetual profile: public info parser plus Unsupported private trading boundary"
                    .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for AarkGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        let private_read_enabled = self.config.private_rest_available();
        capabilities.market_types = vec![MarketType::Perpetual];
        capabilities.supports_public_rest = self.config.enabled_public_rest;
        capabilities.supports_private_rest = private_read_enabled;
        capabilities.supports_public_streams = false;
        capabilities.supports_private_streams = false;
        capabilities.private_stream_capabilities = Some(PrivateStreamCapabilities::unsupported(
            rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
        ));
        capabilities.supports_symbol_rules = self.config.enabled_public_rest;
        capabilities.supports_order_book_snapshot = false;
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
        capabilities.supports_time_in_force = Vec::new();
        capabilities.supports_order_types = Vec::new();
        capabilities.max_order_book_depth = None;
        capabilities.order_book = rustcta_exchange_api::OrderBookCapability::snapshot_only(None);
        capabilities.max_recent_fill_limit = None;
        toolchain::apply_toolchain_capabilities(&mut capabilities);
        capabilities.supports_public_rest = self.config.enabled_public_rest;
        capabilities.supports_private_rest = private_read_enabled;
        capabilities.supports_public_streams = false;
        capabilities.supports_private_streams = false;
        capabilities.supports_symbol_rules = self.config.enabled_public_rest;
        capabilities.supports_order_book_snapshot = false;
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
        capabilities.capabilities_v2.private_rest = if private_read_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "aark private readbacks require RUSTCTA_AARK_PRIVATE_REST_ENABLED plus Orderly account/key/secret; writes remain disabled",
            )
        };
        capabilities.capabilities_v2.order_history = if private_read_enabled {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_limit: true,
                supports_cursor: true,
                max_limit: Some(500),
                ..HistoryCapability::default()
            }
        } else {
            HistoryCapability::unsupported(
                "aark order readbacks require RUSTCTA_AARK_PRIVATE_REST_ENABLED plus Orderly credentials",
            )
        };
        capabilities.capabilities_v2.fills_history = if private_read_enabled {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: true,
                supports_until: true,
                supports_limit: true,
                supports_cursor: true,
                max_limit: Some(500),
                ..HistoryCapability::default()
            }
        } else {
            HistoryCapability::unsupported(
                "aark fill readbacks require RUSTCTA_AARK_PRIVATE_REST_ENABLED plus Orderly credentials",
            )
        };
        capabilities.capabilities_v2.credential_scopes = vec![CredentialScope::ReadOnly];
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.ensure_exchange(&request.exchange)?;
        private::unsupported(private::BALANCES_UNSUPPORTED)
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        private::unsupported(private::POSITIONS_UNSUPPORTED)
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
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        private::unsupported(private::FEES_UNSUPPORTED)
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        private::unsupported(private::PLACE_ORDER_UNSUPPORTED)
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        private::unsupported(private::PLACE_ORDER_UNSUPPORTED)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        private::unsupported(private::CANCEL_ORDER_UNSUPPORTED)
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        private::unsupported(private::AMEND_ORDER_UNSUPPORTED)
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_supported_market_type(request.symbol().market_type)?;
        private::unsupported(private::ORDER_LIST_UNSUPPORTED)
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
        private::unsupported(private::BATCH_PLACE_UNSUPPORTED)
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        private::unsupported(private::BATCH_CANCEL_UNSUPPORTED)
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        private::unsupported(private::CANCEL_ALL_UNSUPPORTED)
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
