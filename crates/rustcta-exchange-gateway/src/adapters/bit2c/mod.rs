use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse, BatchAtomicity,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability, BatchExecutionMode,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    CredentialScope, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, HistoryCapability, OpenOrdersRequest,
    OpenOrdersResponse, OrderBookRequest, OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, SymbolRulesRequest, SymbolRulesResponse, TimeInForce,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
mod parser;
mod private;
#[cfg(test)]
mod private_tests;
mod public;
#[cfg(test)]
mod public_tests;
mod signing;
#[cfg(test)]
mod test_support;
mod transport;

pub use config::Bit2cGatewayConfig;
use transport::Bit2cRest;

#[derive(Clone)]
pub struct Bit2cGatewayAdapter {
    exchange_id: ExchangeId,
    config: Bit2cGatewayConfig,
    rest: Bit2cRest,
}

impl Bit2cGatewayAdapter {
    pub fn new(config: Bit2cGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("bit2c").map_err(validation_error)?;
        let rest = Bit2cRest::new(
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
                message: format!("bit2c adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "bit2c.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }
}

#[async_trait]
impl GatewayAdapter for Bit2cGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("bit2c NIS spot public REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for Bit2cGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        let private_rest_available = self.config.private_rest_enabled();
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private_rest_available;
        capabilities.supports_public_streams = false;
        capabilities.supports_private_streams = false;
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = false;
        capabilities.supports_positions = false;
        capabilities.supports_fees = false;
        capabilities.supports_place_order = false;
        capabilities.supports_cancel_order = false;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = false;
        capabilities.supports_query_order = private_rest_available;
        capabilities.supports_open_orders = private_rest_available;
        capabilities.supports_recent_fills = false;
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_client_order_id = false;
        capabilities.supports_reduce_only = false;
        capabilities.supports_post_only = false;
        capabilities.supports_time_in_force = vec![TimeInForce::GTC];
        capabilities.supports_order_types = vec![OrderType::Limit];
        capabilities.max_order_book_depth = Some(200);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(200));
        capabilities.max_recent_fill_limit = None;
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.private_rest = if private_rest_available {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "Bit2C private REST requires enabled_private_rest plus API credentials",
            )
        };
        capabilities.capabilities_v2.public_streams =
            CapabilitySupport::unsupported("Bit2C official WebSocket API was not found");
        capabilities.capabilities_v2.private_streams =
            CapabilitySupport::unsupported("Bit2C official private WebSocket API was not found");
        capabilities.capabilities_v2.batch_place_orders = BatchCapability {
            support: CapabilitySupport::unsupported(
                "Bit2C does not document native or safely composable batch order placement",
            ),
            mode: BatchExecutionMode::Unsupported,
            atomicity: BatchAtomicity::Unknown,
            max_items: None,
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: false,
            supports_partial_failure: false,
        };
        capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
            support: CapabilitySupport::unsupported(
                "Bit2C does not document native or safely composable batch cancellation",
            ),
            mode: BatchExecutionMode::Unsupported,
            atomicity: BatchAtomicity::Unknown,
            max_items: None,
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: false,
            supports_partial_failure: false,
        };
        capabilities.capabilities_v2.cancel_all_orders =
            CapabilitySupport::unsupported("Bit2C cancel-all is not documented");
        capabilities.capabilities_v2.order_history = HistoryCapability::unsupported(
            "Bit2C order history/fills remain offline pending account-effective parser and reconciliation",
        );
        capabilities.capabilities_v2.fills_history = HistoryCapability::unsupported(
            "Bit2C fills history remains offline pending account-effective parser and reconciliation",
        );
        capabilities.capabilities_v2.credential_scopes =
            vec![CredentialScope::ReadOnly, CredentialScope::Trade];
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.unsupported_balances(request)
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.unsupported_positions(request)
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
        self.unsupported_fees(request)
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.unsupported_place_order(request)
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.unsupported_quote_market_order(request)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.unsupported_cancel_order(request)
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported_amend_order(request)
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.unsupported_batch_place_orders(request)
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.unsupported_batch_cancel_orders(request)
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.unsupported_cancel_all_orders(request)
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
        self.unsupported_recent_fills(request)
    }

    async fn subscribe_public_stream(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported_public_stream(subscription)
    }

    async fn subscribe_private_stream(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported_private_stream(subscription)
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
