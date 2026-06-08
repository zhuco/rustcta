use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateOrderStreamEventKind, PrivateStreamCapabilities,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, SymbolRulesRequest,
    SymbolRulesResponse, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
pub mod options;
mod parser;
mod private;
mod private_parser;
mod public;
mod signing;
mod streams;
mod transport;

#[cfg(test)]
mod private_tests;
#[cfg(test)]
mod public_tests;
#[cfg(test)]
mod stream_tests;

pub use config::DeribitGatewayConfig;
use signing::DeribitPrivateCredentials;
use transport::DeribitRest;

#[derive(Clone)]
pub struct DeribitGatewayAdapter {
    exchange_id: ExchangeId,
    config: DeribitGatewayConfig,
    rest: DeribitRest,
}

impl DeribitGatewayAdapter {
    pub fn new(config: DeribitGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("deribit").map_err(validation_error)?;
        let credentials = if config.has_private_credentials() {
            Some(DeribitPrivateCredentials {
                client_id: config.client_id.clone().unwrap_or_default(),
                client_secret: config.client_secret.clone().unwrap_or_default(),
            })
        } else {
            None
        };
        let rest = DeribitRest::new(
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

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("deribit adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(
            market_type,
            MarketType::Option | MarketType::Perpetual | MarketType::Futures
        ) {
            return Err(ExchangeApiError::Unsupported {
                operation: "deribit.unsupported_market_type",
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

    fn context_account(
        &self,
        context: &rustcta_exchange_api::RequestContext,
        operation: &str,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires tenant_id in request context"),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires account_id in request context"),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for DeribitGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                "deribit options/perpetual/futures REST and WebSocket gateway adapter".to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for DeribitGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private_rest = self.config.private_rest_available();
        let private_stream = self.config.private_stream_available();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![
            MarketType::Option,
            MarketType::Perpetual,
            MarketType::Futures,
        ];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private_rest;
        capabilities.supports_public_streams = self.config.enabled_public_stream;
        capabilities.supports_private_streams = private_stream;
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private_rest;
        capabilities.supports_positions = private_rest;
        capabilities.supports_fees = private_rest;
        capabilities.supports_place_order = private_rest;
        capabilities.supports_cancel_order = private_rest;
        capabilities.supports_query_order = private_rest;
        capabilities.supports_open_orders = private_rest;
        capabilities.supports_recent_fills = private_rest;
        capabilities.supports_cancel_all_orders = private_rest;
        capabilities.supports_batch_place_order = private_rest;
        capabilities.supports_batch_cancel_order = private_rest;
        capabilities.supports_amend_order = private_rest;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = true;
        capabilities.supports_post_only = true;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK];
        capabilities.supports_order_types = vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::PostOnly,
            OrderType::IOC,
            OrderType::FOK,
            OrderType::StopLimit,
            OrderType::StopMarket,
        ];
        capabilities.max_order_book_depth = Some(1000);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::best_effort_delta(Some(1000));
        capabilities.max_recent_fill_limit = Some(1000);
        capabilities.private_stream_capabilities = Some(PrivateStreamCapabilities {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            supports_orders: private_stream,
            supports_fills: private_stream,
            supports_balances: private_stream,
            supports_positions: private_stream,
            supports_account: private_stream,
            order_event_kinds: vec![
                PrivateOrderStreamEventKind::New,
                PrivateOrderStreamEventKind::PartialFill,
                PrivateOrderStreamEventKind::Fill,
                PrivateOrderStreamEventKind::Cancel,
                PrivateOrderStreamEventKind::Reject,
                PrivateOrderStreamEventKind::Expired,
                PrivateOrderStreamEventKind::BalanceUpdate,
            ],
            supports_client_order_id: true,
            supports_exchange_order_id: true,
        });
        capabilities.refresh_v2_from_legacy_flags();
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

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.amend_order_impl(request).await
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
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
