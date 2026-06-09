use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    RequestContext, SymbolRulesRequest, SymbolRulesResponse, TenantId, TimeInForce,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
mod parser;
mod private;
mod private_parser;
mod public;
mod signing;
mod streams;
#[cfg(test)]
mod tests;
mod transport;

pub use config::CoinmetroGatewayConfig;
use transport::CoinmetroRest;

#[derive(Clone)]
pub struct CoinmetroGatewayAdapter {
    exchange_id: ExchangeId,
    config: CoinmetroGatewayConfig,
    rest: CoinmetroRest,
}

impl CoinmetroGatewayAdapter {
    pub fn new(config: CoinmetroGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("coinmetro").map_err(validation_error)?;
        let rest = CoinmetroRest::new(
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
                message: format!("coinmetro adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinmetro.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_private_rest(&self, operation: &'static str) -> ExchangeApiResult<()> {
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn context_account(
        &self,
        context: &RequestContext,
    ) -> ExchangeApiResult<(TenantId, AccountId)> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinmetro private REST request requires tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinmetro private REST request requires account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }

    async fn send_bearer_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &std::collections::HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_bearer_get(
                &self.config.api_token,
                self.config.device_id.as_deref(),
                endpoint,
                params,
            )
            .await
    }

    async fn send_bearer_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &std::collections::HashMap<String, String>,
        body: &serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_bearer_post(
                &self.config.api_token,
                self.config.device_id.as_deref(),
                endpoint,
                params,
                body,
            )
            .await
    }

    async fn send_bearer_put(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &std::collections::HashMap<String, String>,
        body: &serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_bearer_put(
                &self.config.api_token,
                self.config.device_id.as_deref(),
                endpoint,
                params,
                body,
            )
            .await
    }
}

#[async_trait]
impl GatewayAdapter for CoinmetroGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("coinmetro spot REST/WS gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for CoinmetroGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private = self.config.private_rest_enabled();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = private;
        capabilities.private_stream_capabilities =
            Some(streams::coinmetro_private_stream_capabilities(private));
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private;
        capabilities.supports_fees = false;
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_quote_market_order = private;
        capabilities.supports_client_order_id = false;
        capabilities.supports_post_only = false;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK];
        capabilities.supports_order_types = vec![OrderType::Market, OrderType::Limit];
        capabilities.max_order_book_depth = None;
        capabilities.order_book = rustcta_exchange_api::OrderBookCapability::strict_delta(None);
        capabilities.order_book.supports_checksum = true;
        capabilities.max_recent_fill_limit = None;
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_impl(request).await
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("coinmetro.positions_unsupported_spot_only")
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
        self.unsupported("coinmetro.fees_endpoint_not_mapped")
    }

    async fn place_order(
        &self,
        request: rustcta_exchange_api::PlaceOrderRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::PlaceOrderResponse> {
        self.place_order_impl(request).await
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::PlaceOrderResponse> {
        self.place_order_impl(rustcta_exchange_api::PlaceOrderRequest {
            schema_version: request.schema_version,
            context: request.context,
            symbol: request.symbol,
            client_order_id: request.client_order_id,
            side: request.side,
            position_side: None,
            order_type: OrderType::Market,
            time_in_force: None,
            quantity: "0".to_string(),
            price: None,
            quote_quantity: Some(request.quote_quantity),
            reduce_only: false,
            post_only: false,
        })
        .await
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
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("coinmetro.modify_order_unmapped_requires_native_qty_fields")
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_spot(request.symbol().market_type)?;
        self.unsupported("coinmetro.order_list_unsupported")
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("coinmetro.batch_place_orders_composed_not_exposed")
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("coinmetro.batch_cancel_orders_composed_not_exposed")
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("coinmetro.cancel_all_orders_no_native_endpoint")
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
        self.ensure_exchange(&subscription.symbol.exchange)?;
        self.ensure_spot(subscription.symbol.market_type)?;
        streams::public_stream_url(&self.config.ws_url, &[subscription.symbol.exchange_symbol])
    }

    async fn subscribe_private_stream(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.ensure_exchange(&subscription.exchange)?;
        self.ensure_private_rest("coinmetro.private_stream_auth_requires_bearer_token")?;
        streams::private_stream_url(
            &self.config.ws_url,
            self.config.device_id.as_deref(),
            &self.config.api_token,
            &[],
        )
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
