use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse, BatchAtomicity,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchExecutionMode,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    CredentialScope, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, HistoryCapability, OpenOrdersRequest,
    OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, ReconnectCapability,
    StreamAuthCapability, StreamResyncCapability, SymbolRulesRequest, SymbolRulesResponse,
    TimeInForce,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};
use serde_json::Value;

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

pub use config::CryptoComGatewayConfig;
use transport::CryptoComRest;

#[derive(Clone)]
pub struct CryptoComGatewayAdapter {
    exchange_id: ExchangeId,
    config: CryptoComGatewayConfig,
    rest: CryptoComRest,
}

impl CryptoComGatewayAdapter {
    pub fn new(config: CryptoComGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("cryptocom").map_err(validation_error)?;
        let rest = CryptoComRest::new(
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
        Self::new(CryptoComGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("cryptocom adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptocom.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_public_market(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptocom.unsupported_public_market_type",
            });
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str)> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        let api_key = (!self.config.api_key.trim().is_empty())
            .then_some(self.config.api_key.as_str())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let api_secret = (!self.config.api_secret.trim().is_empty())
            .then_some(self.config.api_secret.as_str())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        Ok((api_key, api_secret))
    }

    async fn send_private_post(
        &self,
        operation: &'static str,
        method: &str,
        params: Value,
    ) -> ExchangeApiResult<Value> {
        let (api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_private_post(method, params, api_key, api_secret)
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
                    message: "cryptocom private REST readback requires context.tenant_id"
                        .to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "cryptocom private REST readback requires context.account_id"
                        .to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for CryptoComGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("cryptocom spot REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for CryptoComGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = self.config.private_rest_enabled();
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = self.config.private_rest_enabled();
        capabilities.private_stream_capabilities =
            Some(rustcta_exchange_api::PrivateStreamCapabilities {
                schema_version: rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
                supports_orders: self.config.private_rest_enabled(),
                supports_fills: self.config.private_rest_enabled(),
                supports_balances: self.config.private_rest_enabled(),
                supports_positions: self.config.private_rest_enabled(),
                supports_account: self.config.private_rest_enabled(),
                order_event_kinds: vec![
                    rustcta_exchange_api::PrivateOrderStreamEventKind::New,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::PartialFill,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Fill,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Cancel,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Reject,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::Expired,
                    rustcta_exchange_api::PrivateOrderStreamEventKind::BalanceUpdate,
                ],
                supports_client_order_id: true,
                supports_exchange_order_id: true,
            });
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = self.config.private_rest_enabled();
        capabilities.supports_positions = self.config.private_rest_enabled();
        capabilities.supports_fees = self.config.private_rest_enabled();
        capabilities.supports_place_order = self.config.private_rest_enabled();
        capabilities.supports_cancel_order = self.config.private_rest_enabled();
        capabilities.supports_cancel_all_orders = self.config.private_rest_enabled();
        capabilities.supports_query_order = self.config.private_rest_enabled();
        capabilities.supports_open_orders = self.config.private_rest_enabled();
        capabilities.supports_recent_fills = self.config.private_rest_enabled();
        capabilities.supports_batch_place_order = self.config.private_rest_enabled();
        capabilities.supports_batch_cancel_order = self.config.private_rest_enabled();
        capabilities.supports_quote_market_order = self.config.private_rest_enabled();
        capabilities.supports_amend_order = self.config.private_rest_enabled();
        capabilities.supports_order_list = self.config.private_rest_enabled();
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
        ];
        capabilities.max_order_book_depth = Some(50);
        capabilities.order_book = rustcta_exchange_api::OrderBookCapability::strict_delta(Some(50));
        capabilities.max_recent_fill_limit = Some(100);
        capabilities.refresh_v2_from_legacy_flags();
        let private = self.config.private_rest_enabled();
        capabilities.capabilities_v2.stream_runtime =
            rustcta_exchange_api::StreamRuntimeCapability {
                public: CapabilitySupport::native(),
                private: if private {
                    CapabilitySupport::native()
                } else {
                    CapabilitySupport::unsupported(
                        "cryptocom private streams require API key/secret",
                    )
                },
                supports_subscribe: true,
                supports_unsubscribe: false,
                heartbeat: rustcta_exchange_api::HeartbeatCapability {
                    supported: true,
                    required: true,
                    direction: rustcta_exchange_api::StreamHeartbeatDirection::ServerPing,
                    interval_ms: None,
                    timeout_ms: Some(10_000),
                },
                reconnect: ReconnectCapability {
                    supported: true,
                    requires_resubscribe: true,
                    preserves_session: false,
                    max_reconnect_attempts: None,
                },
                resync: StreamResyncCapability {
                    order_book: true,
                    balances: private,
                    positions: false,
                    orders: private,
                },
                auth: StreamAuthCapability {
                    required: private,
                    credential_scopes: if private {
                        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
                    } else {
                        Vec::new()
                    },
                    renewal_ms: None,
                    uses_listen_key: false,
                    requires_relogin_on_reconnect: true,
                },
                ..rustcta_exchange_api::StreamRuntimeCapability::default()
            };
        if private {
            capabilities.capabilities_v2.batch_place_orders =
                rustcta_exchange_api::BatchCapability {
                    support: CapabilitySupport::native(),
                    mode: BatchExecutionMode::Native,
                    atomicity: BatchAtomicity::Partial,
                    max_items: Some(10),
                    same_symbol_required: false,
                    same_market_type_required: false,
                    supports_client_order_id: true,
                    supports_partial_failure: true,
                };
            capabilities.capabilities_v2.batch_cancel_orders =
                rustcta_exchange_api::BatchCapability {
                    support: CapabilitySupport::native(),
                    mode: BatchExecutionMode::Native,
                    atomicity: BatchAtomicity::Partial,
                    max_items: Some(10),
                    same_symbol_required: false,
                    same_market_type_required: false,
                    supports_client_order_id: true,
                    supports_partial_failure: true,
                };
            capabilities.capabilities_v2.order_history = HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: false,
                supports_until: false,
                supports_limit: true,
                supports_cursor: false,
                supports_from_id: true,
                max_limit: Some(100),
                max_window_ms: None,
            };
            capabilities.capabilities_v2.fills_history = HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: true,
                supports_until: true,
                supports_limit: true,
                supports_cursor: false,
                supports_from_id: false,
                max_limit: Some(100),
                max_window_ms: None,
            };
        }
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
