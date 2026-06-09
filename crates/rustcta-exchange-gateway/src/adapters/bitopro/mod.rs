use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse, BatchAtomicity,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability, BatchExecutionMode,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    ExchangeApiError, ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, FeesRequest,
    FeesResponse, OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse,
    OrderListRequest, OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    SymbolRulesRequest, SymbolRulesResponse, TimeInForce,
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

pub use config::BitoproGatewayConfig;
use transport::BitoproRest;

#[derive(Clone)]
pub struct BitoproGatewayAdapter {
    exchange_id: ExchangeId,
    config: BitoproGatewayConfig,
    rest: BitoproRest,
}

impl BitoproGatewayAdapter {
    pub fn new(config: BitoproGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("bitopro").map_err(validation_error)?;
        let rest = BitoproRest::new(
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
        Self::new(BitoproGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("bitopro adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitopro.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn ensure_private_rest(&self, operation: &'static str) -> ExchangeApiResult<()> {
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
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
                    message: "bitopro private REST request requires tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitopro private REST request requires account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_signed_get(
                self.config.api_key.as_deref().unwrap_or_default(),
                self.config.api_secret.as_deref().unwrap_or_default(),
                self.config.api_identity.as_deref().unwrap_or_default(),
                endpoint,
                params,
            )
            .await
    }

    async fn send_signed_delete(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_signed_delete(
                self.config.api_key.as_deref().unwrap_or_default(),
                self.config.api_secret.as_deref().unwrap_or_default(),
                self.config.api_identity.as_deref().unwrap_or_default(),
                endpoint,
                params,
            )
            .await
    }

    async fn send_signed_json(
        &self,
        operation: &'static str,
        method: Method,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_signed_json(
                self.config.api_key.as_deref().unwrap_or_default(),
                self.config.api_secret.as_deref().unwrap_or_default(),
                method,
                endpoint,
                body,
            )
            .await
    }
}

#[async_trait]
impl GatewayAdapter for BitoproGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("bitopro spot REST adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for BitoproGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = self.config.private_rest_enabled();
        capabilities.supports_public_streams = self.config.enabled_public_streams;
        capabilities.supports_private_streams =
            self.config.enabled_private_streams && self.config.private_rest_enabled();
        capabilities.private_stream_capabilities = Some(
            streams::bitopro_private_stream_capabilities(capabilities.supports_private_streams),
        );
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = self.config.private_rest_enabled();
        capabilities.supports_positions = false;
        capabilities.supports_fees = false;
        capabilities.supports_place_order = self.config.private_rest_enabled();
        capabilities.supports_cancel_order = self.config.private_rest_enabled();
        capabilities.supports_cancel_all_orders = self.config.private_rest_enabled();
        capabilities.supports_query_order = self.config.private_rest_enabled();
        capabilities.supports_open_orders = self.config.private_rest_enabled();
        capabilities.supports_recent_fills = self.config.private_rest_enabled();
        capabilities.supports_batch_place_order = self.config.private_rest_enabled();
        capabilities.supports_batch_cancel_order = self.config.private_rest_enabled();
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = false;
        capabilities.supports_order_list = false;
        capabilities.supports_client_order_id = true;
        capabilities.supports_post_only = true;
        capabilities.supports_order_types =
            vec![OrderType::Market, OrderType::Limit, OrderType::StopLimit];
        capabilities.supports_time_in_force = vec![TimeInForce::GTC, TimeInForce::GTX];
        capabilities.max_order_book_depth = Some(50);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(50));
        capabilities.max_recent_fill_limit = Some(1000);
        apply_bitopro_capabilities_v2(&mut capabilities, self.config.private_rest_enabled());
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
        self.unsupported("bitopro.positions.unsupported_spot")
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
        self.unsupported("bitopro.get_fees.vip_tier_not_account_specific")
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
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_spot(request.symbol.market_type)?;
        self.unsupported("bitopro.quote_market_order.offline_request_spec_only")
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
        self.unsupported("bitopro.amend_order.unsupported")
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_spot(request.symbol().market_type)?;
        self.unsupported("bitopro.order_list.unsupported")
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
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.subscribe_private_stream_impl(subscription).await
    }
}

fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn apply_bitopro_capabilities_v2(
    capabilities: &mut ExchangeClientCapabilities,
    private_rest_enabled: bool,
) {
    let batch_support = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "bitopro batch write runtime requires explicit private REST credentials",
        )
    };
    capabilities.capabilities_v2.batch_place_orders = BatchCapability {
        support: batch_support.clone(),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::NonAtomic,
        max_items: Some(10),
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: true,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
        support: batch_support,
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::NonAtomic,
        max_items: None,
        same_symbol_required: false,
        same_market_type_required: true,
        supports_client_order_id: false,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.cancel_all_orders = if private_rest_enabled {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "bitopro cancel-all runtime requires explicit private REST credentials",
        )
    };
}
