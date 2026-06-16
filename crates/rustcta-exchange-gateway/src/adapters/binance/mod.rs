use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeApiError,
    ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest,
    OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, SymbolAccountConfigRequest,
    SymbolAccountConfigResponse, SymbolRulesRequest, SymbolRulesResponse, TimeInForce,
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
mod streams;
#[cfg(test)]
mod test_support;
mod toolchain;
mod transport;

pub use config::BinanceGatewayConfig;
use transport::BinancePublicRest;

#[derive(Clone)]
pub struct BinanceGatewayAdapter {
    exchange_id: ExchangeId,
    config: BinanceGatewayConfig,
    rest: BinancePublicRest,
    futures_rest: BinancePublicRest,
}

impl BinanceGatewayAdapter {
    pub fn new(config: BinanceGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("binance").map_err(validation_error)?;
        let rest = BinancePublicRest::new(
            exchange_id.clone(),
            config.rest_base_url.clone(),
            config.request_timeout_ms,
        )?;
        let futures_rest = BinancePublicRest::new(
            exchange_id.clone(),
            config.futures_rest_base_url.clone(),
            config.request_timeout_ms,
        )?;
        Ok(Self {
            exchange_id,
            config,
            rest,
            futures_rest,
        })
    }

    #[cfg(test)]
    pub fn default_public() -> ExchangeApiResult<Self> {
        Self::new(BinanceGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("binance adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "binance.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_supported_market(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "binance.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn unsupported_private<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str, u64)> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        let api_key = self
            .config
            .api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let api_secret = self
            .config
            .api_secret
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        Ok((api_key, api_secret, self.config.recv_window_ms))
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_get(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_get_for_market(
        &self,
        operation: &'static str,
        market_type: MarketType,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest_for_market(market_type)?
            .send_signed_get(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_post(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_post_for_market(
        &self,
        operation: &'static str,
        market_type: MarketType,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest_for_market(market_type)?
            .send_signed_post(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_delete(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_delete(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_delete_for_market(
        &self,
        operation: &'static str,
        market_type: MarketType,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest_for_market(market_type)?
            .send_signed_delete(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_put(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_put(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    fn rest_for_market(&self, market_type: MarketType) -> ExchangeApiResult<&BinancePublicRest> {
        match market_type {
            MarketType::Spot => Ok(&self.rest),
            MarketType::Perpetual => Ok(&self.futures_rest),
            _ => Err(ExchangeApiError::Unsupported {
                operation: "binance.unsupported_market_type",
            }),
        }
    }

    fn context_account(
        &self,
        context: &rustcta_exchange_api::RequestContext,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "binance private REST readback requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "binance private REST readback requires context.account_id"
                        .to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for BinanceGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("binance spot and USD-M perpetual REST gateway adapter".to_string()),
        }
    }

    async fn get_symbol_account_config(
        &self,
        request: SymbolAccountConfigRequest,
    ) -> ExchangeApiResult<SymbolAccountConfigResponse> {
        self.get_symbol_account_config_impl(request).await
    }
}

#[async_trait]
impl ExchangeClient for BinanceGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        let private_rest_enabled = self.config.private_rest_enabled();
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private_rest_enabled;
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private_rest_enabled;
        capabilities.supports_positions = private_rest_enabled;
        capabilities.supports_fees = private_rest_enabled;
        capabilities.supports_place_order = private_rest_enabled;
        capabilities.supports_cancel_order = private_rest_enabled;
        capabilities.supports_query_order = private_rest_enabled;
        capabilities.supports_open_orders = private_rest_enabled;
        capabilities.supports_recent_fills = private_rest_enabled;
        capabilities.supports_cancel_all_orders = private_rest_enabled;
        capabilities.supports_quote_market_order = private_rest_enabled;
        capabilities.supports_amend_order = private_rest_enabled;
        capabilities.supports_order_list = private_rest_enabled;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = true;
        capabilities.supports_post_only = true;
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
        capabilities.max_order_book_depth = Some(20);
        capabilities.order_book = rustcta_exchange_api::OrderBookCapability::strict_delta(Some(20));
        toolchain::apply_toolchain_capabilities(
            &mut capabilities,
            private_rest_enabled,
            self.config.enabled_public_streams,
        );
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_impl(request).await
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.get_positions_impl(request).await
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
        self.get_fees_impl(request).await
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_order_impl(request).await
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_quote_market_order_impl(request).await
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.cancel_order_impl(request).await
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.amend_order_impl(request).await
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.place_order_list_impl(request).await
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.batch_place_orders_impl(request).await
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.batch_cancel_orders_impl(request).await
    }

    async fn cancel_all_orders(
        &self,
        request: rustcta_exchange_api::CancelAllOrdersRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::CancelAllOrdersResponse> {
        self.cancel_all_orders_impl(request).await
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
