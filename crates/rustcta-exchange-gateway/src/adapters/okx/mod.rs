use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountControlCapabilities, AmendOrderRequest, AmendOrderResponse, BalancesRequest,
    BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse,
    FundingRatesRequest, FundingRatesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    SetLeverageRequest, SetLeverageResponse, SetPositionModeRequest, SetPositionModeResponse,
    SymbolRulesRequest, SymbolRulesResponse, TimeInForce,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
pub(crate) mod parser;
mod private;
#[cfg(test)]
mod private_tests;
mod public;
#[cfg(test)]
mod public_tests;
mod signing;
mod streams;
#[cfg(test)]
mod test_support;
mod toolchain;
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
        let exchange_id = ExchangeId::new(&config.exchange_id).map_err(validation_error)?;
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
                message: format!(
                    "{} adapter cannot serve request for exchange {exchange}",
                    self.exchange_id
                ),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: self.config.unsupported_market_type_operation,
            });
        }
        Ok(())
    }

    fn ensure_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot
            && !(market_type == MarketType::Perpetual && self.exchange_id.as_str() == "okx")
            && !(market_type == MarketType::Futures && self.exchange_id.as_str() == "okx")
            && !(market_type == MarketType::Option && self.exchange_id.as_str() == "okx")
        {
            return Err(ExchangeApiError::Unsupported {
                operation: self.config.unsupported_market_type_operation,
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

    fn ensure_optional_market_type(
        &self,
        market_type: Option<MarketType>,
    ) -> ExchangeApiResult<()> {
        if let Some(market_type) = market_type {
            self.ensure_market_type(market_type)?;
        }
        Ok(())
    }

    pub(super) fn profile_operation(
        &self,
        okx_operation: &'static str,
        okxus_operation: &'static str,
        myokx_operation: &'static str,
    ) -> &'static str {
        match self.exchange_id.as_str() {
            "myokx" => myokx_operation,
            "okxus" => okxus_operation,
            _ => okx_operation,
        }
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
                if self.config.private_rest_available() && self.exchange_id.as_str() == "okx" {
                    "okx public + private readback REST gateway adapter".to_string()
                } else {
                    self.config.status_message.clone()
                },
            ),
        }
    }

    fn account_control_capabilities(&self) -> AccountControlCapabilities {
        let supports_derivative_controls =
            self.config.private_rest_available() && self.exchange_id.as_str() == "okx";
        AccountControlCapabilities {
            exchange: self.exchange_id.clone(),
            supports_symbol_account_config: false,
            supports_leverage: supports_derivative_controls,
            supports_position_mode_change: supports_derivative_controls,
            supports_close_position: false,
            supports_countdown_cancel_all: false,
        }
    }

    async fn set_leverage(
        &self,
        request: SetLeverageRequest,
    ) -> ExchangeApiResult<SetLeverageResponse> {
        self.set_leverage_private_rest(request).await
    }

    async fn set_position_mode(
        &self,
        request: SetPositionModeRequest,
    ) -> ExchangeApiResult<SetPositionModeResponse> {
        self.set_position_mode_private_rest(request).await
    }
}

#[async_trait]
impl ExchangeClient for OkxGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = if self.exchange_id.as_str() == "okx" {
            vec![
                MarketType::Spot,
                MarketType::Perpetual,
                MarketType::Futures,
                MarketType::Option,
            ]
        } else {
            vec![MarketType::Spot]
        };
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = self.config.private_rest_available();
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = self.config.private_rest_available();
        capabilities.supports_positions = self.config.private_rest_available();
        capabilities.supports_fees = self.config.private_rest_available();
        capabilities.supports_funding_rates = self.exchange_id.as_str() == "okx";
        capabilities.supports_place_order = self.config.private_rest_available();
        capabilities.supports_cancel_order = self.config.private_rest_available();
        capabilities.supports_query_order = self.config.private_rest_available();
        capabilities.supports_open_orders = self.config.private_rest_available();
        capabilities.supports_recent_fills = self.config.private_rest_available();
        capabilities.supports_batch_place_order = self.config.private_rest_available();
        capabilities.supports_batch_cancel_order = self.config.private_rest_available();
        capabilities.supports_cancel_all_orders = self.config.private_rest_available();
        capabilities.supports_quote_market_order = self.config.private_rest_available();
        capabilities.supports_amend_order = self.config.private_rest_available();
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
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::strict_delta(Some(400));
        capabilities.max_recent_fill_limit = Some(100);
        toolchain::apply_toolchain_capabilities(
            &mut capabilities,
            self.config.private_rest_available(),
            self.exchange_id.as_str(),
            self.config.enabled_public_streams,
        );
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

    async fn get_funding_rates(
        &self,
        request: FundingRatesRequest,
    ) -> ExchangeApiResult<FundingRatesResponse> {
        self.get_funding_rates_public_rest(request).await
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
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.amend_order_private_rest(request).await
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
        request: rustcta_exchange_api::CancelAllOrdersRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::CancelAllOrdersResponse> {
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
