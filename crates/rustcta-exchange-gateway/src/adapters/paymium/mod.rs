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
mod tests;
mod transport;

pub use config::PaymiumGatewayConfig;
use transport::PaymiumRest;

#[derive(Clone)]
pub struct PaymiumGatewayAdapter {
    exchange_id: ExchangeId,
    config: PaymiumGatewayConfig,
    rest: PaymiumRest,
}

impl PaymiumGatewayAdapter {
    pub fn new(config: PaymiumGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("paymium").map_err(validation_error)?;
        let rest = PaymiumRest::new(
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
                message: format!("paymium adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "paymium.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }
}

#[async_trait]
impl GatewayAdapter for PaymiumGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("paymium BTC/EUR public REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for PaymiumGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let public_streams_enabled = self.config.enabled_public_streams;
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = true;
        capabilities.supports_public_streams = public_streams_enabled;
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
        capabilities.supports_query_order = true;
        capabilities.supports_open_orders = true;
        capabilities.supports_recent_fills = true;
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
        capabilities.max_order_book_depth = None;
        capabilities.max_recent_fill_limit = None;
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::best_effort_delta(None);
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
        capabilities.capabilities_v2.private_rest = CapabilitySupport::native();
        capabilities.capabilities_v2.public_streams = if public_streams_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "Paymium public socket.io v1.3 runtime is bounded behind PAYMIUM_PUBLIC_STREAMS_ENABLED",
            )
        };
        capabilities.capabilities_v2.private_streams = CapabilitySupport::rest_fallback(
            "Paymium user socket depends on a private channel id from /user",
        );
        capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
            public: if public_streams_enabled {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "socket.io runtime disabled by PAYMIUM_PUBLIC_STREAMS_ENABLED default",
                )
            },
            private: CapabilitySupport::rest_fallback(
                "private socket updates should reconcile through REST after private REST promotion",
            ),
            supports_subscribe: public_streams_enabled,
            supports_unsubscribe: false,
            supports_public_subscribe: public_streams_enabled,
            supports_public_unsubscribe: false,
            supports_private_subscribe: false,
            supports_private_unsubscribe: false,
            heartbeat: HeartbeatCapability {
                supported: true,
                required: true,
                direction: StreamHeartbeatDirection::Bidirectional,
                interval_ms: None,
                timeout_ms: None,
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
                required: false,
                credential_scopes: Vec::new(),
                renewal_ms: None,
                uses_listen_key: false,
                requires_relogin_on_reconnect: false,
            },
            public_private_separate_connections: true,
            ..StreamRuntimeCapability::default()
        };
        capabilities.capabilities_v2.batch_place_orders = BatchCapability {
            support: CapabilitySupport::unsupported("Paymium has no generic batch place endpoint"),
            mode: BatchExecutionMode::Unsupported,
            atomicity: BatchAtomicity::Unknown,
            max_items: None,
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: false,
            supports_partial_failure: false,
        };
        capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
            support: CapabilitySupport::unsupported("Paymium has no generic batch cancel endpoint"),
            mode: BatchExecutionMode::Unsupported,
            atomicity: BatchAtomicity::Unknown,
            max_items: None,
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: false,
            supports_partial_failure: false,
        };
        capabilities.capabilities_v2.cancel_all_orders =
            CapabilitySupport::unsupported("Paymium cancel-all is not documented");
        capabilities.capabilities_v2.order_history = HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: false,
            supports_until: false,
            supports_limit: false,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: None,
            max_window_ms: None,
        };
        capabilities.capabilities_v2.fills_history = HistoryCapability {
            support: CapabilitySupport::native(),
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
            max_limit: Some(100),
            max_window_ms: None,
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
