use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, PrivateStreamSubscription,
    PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse, RecentFillsRequest,
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
mod types;

pub use config::OkxGatewayConfig;
use signing::OkxPrivateCredentials;
use transport::OkxRest;

#[derive(Clone)]
pub struct OkxGatewayAdapter {
    exchange_id: ExchangeId,
    config: OkxGatewayConfig,
    rest: OkxRest,
}

impl OkxGatewayAdapter {
    pub fn new(config: OkxGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("okx").map_err(validation_error)?;
        let credentials = OkxPrivateCredentials::from_config(&config);
        let rest = OkxRest::new(
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

    #[cfg(test)]
    pub fn default_public() -> ExchangeApiResult<Self> {
        Self::new(OkxGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("okx adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "okx.non_spot_market_type",
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

    fn ensure_optional_spot(&self, market_type: Option<MarketType>) -> ExchangeApiResult<()> {
        if let Some(market_type) = market_type {
            self.ensure_spot(market_type)?;
        }
        Ok(())
    }
}

#[async_trait]
impl GatewayAdapter for OkxGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                if self.config.private_rest_available() {
                    "okx public + private readback REST gateway adapter"
                } else {
                    "okx public REST gateway adapter"
                }
                .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for OkxGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = self.config.private_rest_available();
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = self.config.private_rest_available();
        capabilities.supports_fees = self.config.private_rest_available();
        capabilities.supports_query_order = self.config.private_rest_available();
        capabilities.supports_open_orders = self.config.private_rest_available();
        capabilities.supports_recent_fills = self.config.private_rest_available();
        capabilities.supports_client_order_id = true;
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
        capabilities.max_order_book_depth = Some(400);
        capabilities.max_recent_fill_limit = Some(100);
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_private_rest(request).await
    }

    async fn get_positions(
        &self,
        _request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.unsupported_private("okx.get_positions")
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
        _request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.unsupported_private("okx.place_order")
    }

    async fn cancel_order(
        &self,
        _request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.unsupported_private("okx.cancel_order")
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
        _subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        Err(ExchangeApiError::Unsupported {
            operation: "okx.subscribe_public_stream",
        })
    }

    async fn subscribe_private_stream(
        &self,
        _subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported_private("okx.subscribe_private_stream")
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
