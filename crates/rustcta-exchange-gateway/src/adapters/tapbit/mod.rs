use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    RequestContext, SymbolRulesRequest, SymbolRulesResponse, TenantId, TimeInForce,
};
use rustcta_exchange_api::{
    BatchAtomicity, BatchCapability, BatchExecutionMode, CapabilitySupport, HistoryCapability,
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
#[cfg(test)]
mod stream_tests;
mod streams;
#[cfg(test)]
mod test_support;
mod transport;

pub use config::TapbitGatewayConfig;
use transport::TapbitRest;

#[derive(Clone)]
pub struct TapbitGatewayAdapter {
    exchange_id: ExchangeId,
    config: TapbitGatewayConfig,
    rest: TapbitRest,
}

impl TapbitGatewayAdapter {
    pub fn new(config: TapbitGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("tapbit").map_err(validation_error)?;
        let rest = TapbitRest::new(
            exchange_id.clone(),
            config.spot_rest_base_url.clone(),
            config.swap_rest_base_url.clone(),
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
        Self::new(TapbitGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("tapbit adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "tapbit.market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.private_rest_enabled() {
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
        Ok((api_key, api_secret))
    }

    async fn send_spot_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &std::collections::HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_spot_signed_get(endpoint, params, api_key, api_secret)
            .await
    }

    async fn send_spot_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_spot_signed_post(endpoint, body, api_key, api_secret)
            .await
    }

    async fn send_swap_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &std::collections::HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_swap_signed_get(endpoint, params, api_key, api_secret)
            .await
    }

    async fn send_swap_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_swap_signed_post(endpoint, body, api_key, api_secret)
            .await
    }

    fn context_account(
        &self,
        context: &RequestContext,
        operation: &'static str,
    ) -> ExchangeApiResult<(TenantId, AccountId)> {
        let tenant_id =
            context
                .tenant_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires context.tenant_id"),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: format!("{operation} requires context.account_id"),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for TapbitGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                if self.config.private_rest_enabled() {
                    "tapbit spot private REST + spot/usdt perpetual public REST/WS gateway adapter"
                } else {
                    "tapbit spot/usdt perpetual public REST/WS gateway adapter"
                }
                .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for TapbitGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private = self.config.private_rest_enabled();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = false;
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private;
        capabilities.supports_positions = private;
        capabilities.supports_fees = true;
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_batch_place_order = private;
        capabilities.supports_batch_cancel_order = private;
        capabilities.supports_quote_market_order = false;
        capabilities.supports_client_order_id = false;
        capabilities.supports_reduce_only = true;
        capabilities.supports_post_only = false;
        capabilities.supports_time_in_force = vec![TimeInForce::GTC];
        capabilities.supports_order_types = vec![OrderType::Limit];
        capabilities.max_order_book_depth = Some(200);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(200));
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_streams = CapabilitySupport::rest_fallback(
            "Tapbit public WebSocket is exposed as subscription/parser specs; REST snapshot is the resync source",
        );
        capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
            "tapbit.subscribe_private_stream.no_official_private_ws_topics",
        );
        capabilities.capabilities_v2.stream_runtime = streams::tapbit_stream_runtime_capability();
        capabilities.capabilities_v2.batch_place_orders = if private {
            BatchCapability {
                support: CapabilitySupport::composed(
                    "Spot uses native batch_order; USDT perpetual uses sequential single-order fallback",
                ),
                mode: BatchExecutionMode::ComposedSequential,
                atomicity: BatchAtomicity::Partial,
                max_items: None,
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: false,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("Tapbit batch place requires private REST credentials")
        };
        capabilities.capabilities_v2.batch_cancel_orders = if private {
            BatchCapability {
                support: CapabilitySupport::composed(
                    "Spot uses native batch_cancel_order; USDT perpetual uses sequential cancel fallback",
                ),
                mode: BatchExecutionMode::ComposedSequential,
                atomicity: BatchAtomicity::Partial,
                max_items: None,
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: false,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("Tapbit batch cancel requires private REST credentials")
        };
        capabilities.capabilities_v2.fills_history = HistoryCapability {
            support: if private {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("Tapbit fills require private REST credentials")
            },
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: Some(100),
            max_window_ms: None,
        };
        capabilities.apply_v2_to_legacy_flags();
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
        _request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.unsupported("tapbit.place_quote_market_order")
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.cancel_order_impl(request).await
    }

    async fn amend_order(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported("tapbit.amend_order")
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
        _subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported("tapbit.subscribe_private_stream.no_official_private_ws_topics")
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
