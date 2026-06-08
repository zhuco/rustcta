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
mod private_parser;
#[cfg(test)]
mod private_tests;
mod public;
#[cfg(test)]
mod public_tests;
mod signing;
#[cfg(test)]
mod test_support;
mod transport;

pub use config::CoinspotGatewayConfig;
use transport::CoinspotRest;

#[derive(Clone)]
pub struct CoinspotGatewayAdapter {
    exchange_id: ExchangeId,
    config: CoinspotGatewayConfig,
    rest: CoinspotRest,
}

impl CoinspotGatewayAdapter {
    pub fn new(config: CoinspotGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("coinspot").map_err(validation_error)?;
        let legacy_base_url = config.rest_base_url.trim();
        let public_rest_base_url = if config.public_rest_base_url.trim().is_empty() {
            legacy_base_url
        } else {
            config.public_rest_base_url.trim()
        };
        let private_rest_base_url = if config.private_rest_base_url.trim().is_empty() {
            legacy_base_url
        } else {
            config.private_rest_base_url.trim()
        };
        let read_only_rest_base_url = if config.read_only_rest_base_url.trim().is_empty() {
            legacy_base_url
        } else {
            config.read_only_rest_base_url.trim()
        };
        let rest = CoinspotRest::new(
            exchange_id.clone(),
            public_rest_base_url.to_string(),
            private_rest_base_url.to_string(),
            read_only_rest_base_url.to_string(),
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
        Self::new(CoinspotGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("coinspot adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinspot.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        let api_key = self.config.api_key.trim();
        if api_key.is_empty() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        let api_secret = self.config.api_secret.trim();
        if api_secret.is_empty() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok((api_key, api_secret))
    }

    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_signed_post(endpoint, body, api_key, api_secret)
            .await
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
                    message: "coinspot private REST readback requires context.tenant_id"
                        .to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinspot private REST readback requires context.account_id"
                        .to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for CoinspotGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("coinspot AUD spot REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for CoinspotGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = self.config.private_rest_enabled();
        capabilities.supports_public_streams = false;
        capabilities.supports_private_streams = false;
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = self.config.private_rest_enabled();
        capabilities.supports_fees = self.config.private_rest_enabled();
        capabilities.supports_place_order = self.config.private_rest_enabled();
        capabilities.supports_cancel_order = false;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_quote_market_order = self.config.private_rest_enabled();
        capabilities.supports_amend_order = false;
        capabilities.supports_query_order = false;
        capabilities.supports_open_orders = self.config.private_rest_enabled();
        capabilities.supports_recent_fills = self.config.private_rest_enabled();
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_client_order_id = false;
        capabilities.supports_time_in_force = vec![TimeInForce::GTC];
        capabilities.supports_order_types = vec![OrderType::Market, OrderType::Limit];
        capabilities.max_order_book_depth = Some(100);
        capabilities.max_recent_fill_limit = None;
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(100));
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_streams =
            CapabilitySupport::unsupported("coinspot public streaming is not exposed by adapter");
        capabilities.capabilities_v2.private_streams =
            CapabilitySupport::unsupported("coinspot private streaming is not exposed by adapter");
        capabilities.capabilities_v2.batch_place_orders = BatchCapability {
            support: CapabilitySupport::unsupported(
                "coinspot adapter does not compose batch orders",
            ),
            mode: BatchExecutionMode::Unsupported,
            atomicity: BatchAtomicity::NonAtomic,
            max_items: None,
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: false,
            supports_partial_failure: false,
        };
        capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
            support: CapabilitySupport::unsupported(
                "coinspot cancel APIs are side-specific and not safely mapped to generic batch cancel",
            ),
            mode: BatchExecutionMode::Unsupported,
            atomicity: BatchAtomicity::NonAtomic,
            max_items: None,
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: false,
            supports_partial_failure: false,
        };
        capabilities.capabilities_v2.fills_history = HistoryCapability {
            support: if self.config.private_rest_enabled() {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "coinspot fills history requires private REST credentials",
                )
            },
            supports_since: false,
            supports_until: false,
            supports_limit: false,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: None,
            max_window_ms: None,
        };
        capabilities.capabilities_v2.order_history = HistoryCapability {
            support: CapabilitySupport::unsupported(
                "coinspot side-specific order history is not safely mapped to generic query_order",
            ),
            supports_since: false,
            supports_until: false,
            supports_limit: false,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: None,
            max_window_ms: None,
        };
        capabilities.capabilities_v2.credential_scopes =
            vec![CredentialScope::ReadOnly, CredentialScope::Trade];
        capabilities.apply_v2_to_legacy_flags();
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_impl(request).await
    }

    async fn get_positions(
        &self,
        _request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.unsupported("coinspot.get_positions")
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
        self.unsupported_cancel_order(request)
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

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported_amend_order(request)
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
        self.unsupported_query_order(request)
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
        _subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported("coinspot.subscribe_public_stream")
    }

    async fn subscribe_private_stream(
        &self,
        _subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported("coinspot.subscribe_private_stream")
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
