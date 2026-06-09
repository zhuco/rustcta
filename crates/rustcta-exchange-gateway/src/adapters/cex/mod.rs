use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse, BatchAtomicity,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability, BatchExecutionMode,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    CredentialScope, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, HeartbeatCapability, HistoryCapability,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest,
    OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamCapabilities, PrivateStreamSubscription, PublicStreamSubscription,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, ReconnectCapability, StreamAuthCapability, StreamHeartbeatDirection,
    StreamResyncCapability, StreamRuntimeCapability, SymbolRulesRequest, SymbolRulesResponse,
    TimeInForce,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
mod parser;
mod private;
mod public;
mod signing;
mod streams;
#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;
mod transport;

pub use config::CexGatewayConfig;
use transport::CexRest;

#[derive(Clone)]
pub struct CexGatewayAdapter {
    exchange_id: ExchangeId,
    config: CexGatewayConfig,
    rest: CexRest,
}

impl CexGatewayAdapter {
    pub fn new(config: CexGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("cex").map_err(validation_error)?;
        let rest = CexRest::new(
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
        Self::new(CexGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("cex adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "cex.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }
}

#[async_trait]
impl GatewayAdapter for CexGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("cex.io spot public REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for CexGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        let private_rest_available = self.config.private_rest_configured();
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private_rest_available;
        capabilities.supports_public_streams = self.config.enabled_public_streams;
        capabilities.supports_private_streams = false;
        capabilities.private_stream_capabilities = Some(PrivateStreamCapabilities::unsupported(
            rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
        ));
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = false;
        capabilities.supports_positions = false;
        capabilities.supports_fees = false;
        capabilities.supports_place_order = false;
        capabilities.supports_cancel_order = false;
        capabilities.supports_query_order = private_rest_available;
        capabilities.supports_open_orders = private_rest_available;
        capabilities.supports_recent_fills = private_rest_available;
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = false;
        capabilities.supports_order_list = false;
        capabilities.supports_client_order_id = false;
        capabilities.supports_reduce_only = false;
        capabilities.supports_post_only = false;
        capabilities.supports_time_in_force = vec![TimeInForce::GTC];
        capabilities.supports_order_types = vec![OrderType::Market, OrderType::Limit];
        capabilities.max_order_book_depth = Some(100);
        capabilities.max_recent_fill_limit = None;
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(100));
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
        capabilities.capabilities_v2.private_rest = if private_rest_available {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "CEX.IO private REST requires CEX_PRIVATE_REST_ENABLED plus API key, secret, and user id",
            )
        };
        capabilities.capabilities_v2.public_streams = if self.config.enabled_public_streams {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "CEX.IO public websocket runtime is disabled unless CEX_PUBLIC_STREAMS_ENABLED is set",
            )
        };
        capabilities.capabilities_v2.private_streams = CapabilitySupport::rest_fallback(
            "CEX.IO private order/fill state reconciles through guarded private REST when configured",
        );
        capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
            public: if self.config.enabled_public_streams {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "public websocket runtime disabled by CEX_PUBLIC_STREAMS_ENABLED",
                )
            },
            private: CapabilitySupport::rest_fallback(
                "private websocket not enabled; use guarded private REST reconciliation when configured",
            ),
            supports_subscribe: true,
            supports_unsubscribe: true,
            supports_public_subscribe: true,
            supports_public_unsubscribe: true,
            supports_private_subscribe: false,
            supports_private_unsubscribe: false,
            heartbeat: HeartbeatCapability {
                supported: true,
                required: true,
                direction: StreamHeartbeatDirection::ServerPing,
                interval_ms: Some(15_000),
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
                balances: true,
                positions: false,
                orders: true,
            },
            auth: StreamAuthCapability {
                required: true,
                credential_scopes: vec![CredentialScope::ReadOnly, CredentialScope::Trade],
                renewal_ms: None,
                uses_listen_key: false,
                requires_relogin_on_reconnect: true,
            },
            public_private_separate_connections: false,
            ..StreamRuntimeCapability::default()
        };
        capabilities.capabilities_v2.batch_place_orders = BatchCapability {
            support: CapabilitySupport::unsupported(
                "CEX.IO mass-cancel-place is not mapped to generic batch place",
            ),
            mode: BatchExecutionMode::Unsupported,
            atomicity: BatchAtomicity::Unknown,
            max_items: None,
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: false,
            supports_partial_failure: true,
        };
        capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
            support: CapabilitySupport::unsupported(
                "CEX.IO mass-cancel-place is not mapped to generic batch cancel",
            ),
            mode: BatchExecutionMode::Unsupported,
            atomicity: BatchAtomicity::Unknown,
            max_items: None,
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: false,
            supports_partial_failure: true,
        };
        capabilities.capabilities_v2.cancel_all_orders =
            CapabilitySupport::unsupported("pair-scoped cancel-all remains request-spec only");
        capabilities.capabilities_v2.order_history = if private_rest_available {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: false,
                supports_until: false,
                supports_limit: false,
                supports_cursor: false,
                supports_from_id: false,
                max_limit: None,
                max_window_ms: None,
            }
        } else {
            HistoryCapability::unsupported("private order history requires guarded private REST")
        };
        capabilities.capabilities_v2.fills_history = if private_rest_available {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: true,
                supports_until: true,
                supports_limit: true,
                supports_cursor: false,
                supports_from_id: false,
                max_limit: Some(100),
                max_window_ms: None,
            }
        } else {
            HistoryCapability::unsupported("private fill history requires guarded private REST")
        };
        capabilities.capabilities_v2.credential_scopes =
            vec![CredentialScope::ReadOnly, CredentialScope::Trade];
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_unsupported(request)
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.get_positions_unsupported(request)
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
        self.get_fees_unsupported(request)
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_order_unsupported(request)
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_quote_market_order_unsupported(request)
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.cancel_order_unsupported(request)
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.amend_order_unsupported(request)
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.place_order_list_unsupported(request)
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.batch_place_orders_unsupported(request)
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.batch_cancel_orders_unsupported(request)
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.cancel_all_orders_unsupported(request)
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
