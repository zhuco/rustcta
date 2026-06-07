use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchAtomicity, BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability,
    BatchExecutionMode, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    CredentialScope, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, HeartbeatCapability, HeartbeatDirection,
    HeartbeatPolicy, HistoryCapability, OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest,
    OrderBookResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, PrivateStreamCapabilities, PrivateStreamSubscription,
    PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, ReconnectCapability, RequestContext,
    StreamAuthCapability, StreamHeartbeatDirection, StreamResyncCapability,
    StreamRuntimeCapability, SymbolRulesRequest, SymbolRulesResponse, TenantId, TimeInForce,
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

pub use config::CoinstoreGatewayConfig;
use signing::CoinstorePrivateCredentials;
use transport::CoinstoreRest;

#[derive(Clone)]
pub struct CoinstoreGatewayAdapter {
    exchange_id: ExchangeId,
    config: CoinstoreGatewayConfig,
    rest: CoinstoreRest,
}

impl CoinstoreGatewayAdapter {
    pub fn new(config: CoinstoreGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("coinstore").map_err(validation_error)?;
        let rest = CoinstoreRest::new(
            exchange_id.clone(),
            config.spot_rest_base_url.clone(),
            config.futures_rest_base_url.clone(),
            config.request_timeout_ms,
            CoinstorePrivateCredentials::from_config(&config),
        )?;
        Ok(Self {
            exchange_id,
            config,
            rest,
        })
    }

    #[cfg(test)]
    pub fn default_public() -> ExchangeApiResult<Self> {
        Self::new(CoinstoreGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("coinstore adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinstore.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
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
impl GatewayAdapter for CoinstoreGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                if self.config.private_rest_available() {
                    "coinstore spot + futures private REST and public/private WS gateway adapter"
                } else {
                    "coinstore spot + futures public REST/WS gateway adapter"
                }
                .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for CoinstoreGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private = self.config.private_rest_available();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot, MarketType::Perpetual];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = private;
        capabilities.private_stream_capabilities = if private {
            Some(streams::coinstore_private_stream_capabilities())
        } else {
            Some(PrivateStreamCapabilities::unsupported(
                rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
            ))
        };
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
        capabilities.supports_quote_market_order = private;
        capabilities.supports_amend_order = false;
        capabilities.supports_order_list = false;
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
        capabilities.max_order_book_depth = Some(100);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(100));
        capabilities.max_recent_fill_limit = Some(100);
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
        capabilities.capabilities_v2.private_streams = if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "coinstore private streams require API key/secret and enabled_private_rest",
            )
        };
        capabilities.capabilities_v2.stream_runtime =
            rustcta_exchange_api::StreamRuntimeCapability {
                public: CapabilitySupport::native(),
                private: if private {
                    CapabilitySupport::native()
                } else {
                    CapabilitySupport::unsupported(
                        "coinstore private streams require futures Socket.IO auth credentials",
                    )
                },
                supports_subscribe: true,
                supports_unsubscribe: true,
                supports_public_subscribe: true,
                supports_public_unsubscribe: true,
                supports_private_subscribe: private,
                supports_private_unsubscribe: private,
                heartbeat: HeartbeatCapability {
                    supported: true,
                    required: true,
                    direction: StreamHeartbeatDirection::Bidirectional,
                    interval_ms: Some(30_000),
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
                    positions: private,
                    orders: private,
                },
                auth: StreamAuthCapability {
                    required: private,
                    credential_scopes: if private {
                        vec![CredentialScope::ReadOnly, CredentialScope::Trade]
                    } else {
                        Vec::new()
                    },
                    renewal_ms: Some(30_000),
                    uses_listen_key: false,
                    requires_relogin_on_reconnect: true,
                },
                public_private_separate_connections: true,
                heartbeat_policy: HeartbeatPolicy {
                    direction: HeartbeatDirection::ApplicationMessage,
                    ping_interval_ms: 30_000,
                    pong_timeout_ms: 10_000,
                    stale_message_ms: 180_000,
                    requires_pong_payload_echo: false,
                },
                auth_renewal_policy: streams::coinstore_auth_renewal_policy(),
                auth_renewal: streams::coinstore_auth_renewal_policy(),
                reconnect_requires_login: true,
                reconnect_requires_resubscribe: true,
                orderbook_requires_snapshot_after_reconnect: true,
                ..StreamRuntimeCapability::default()
            };
        capabilities.capabilities_v2.batch_place_orders = if private {
            BatchCapability {
                support: CapabilitySupport::native(),
                mode: BatchExecutionMode::Native,
                atomicity: BatchAtomicity::Partial,
                max_items: Some(20),
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("coinstore batch place requires private REST credentials")
        };
        capabilities.capabilities_v2.batch_cancel_orders = if private {
            BatchCapability {
                support: CapabilitySupport::native(),
                mode: BatchExecutionMode::Native,
                atomicity: BatchAtomicity::Partial,
                max_items: Some(20),
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("coinstore batch cancel requires private REST credentials")
        };
        capabilities.capabilities_v2.cancel_all_orders = if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("coinstore cancel-all requires private REST credentials")
        };
        capabilities.capabilities_v2.order_history = if private {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: false,
                supports_until: false,
                supports_limit: false,
                supports_cursor: false,
                supports_from_id: true,
                max_limit: None,
                max_window_ms: None,
            }
        } else {
            HistoryCapability::unsupported(
                "coinstore order reconciliation requires private REST credentials",
            )
        };
        capabilities.capabilities_v2.fills_history = if private {
            HistoryCapability {
                support: CapabilitySupport::native(),
                supports_since: false,
                supports_until: false,
                supports_limit: true,
                supports_cursor: false,
                supports_from_id: true,
                max_limit: Some(100),
                max_window_ms: None,
            }
        } else {
            HistoryCapability::unsupported(
                "coinstore fills history requires private REST credentials",
            )
        };
        capabilities.capabilities_v2.credential_scopes = if private {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        };
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.ensure_private_rest("coinstore.get_balances")?;
        self.get_balances_impl(request).await
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_private_rest("coinstore.get_positions")?;
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
        self.ensure_private_rest("coinstore.place_order")?;
        self.place_order_impl(request).await
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_private_rest("coinstore.place_quote_market_order")?;
        self.place_quote_market_order_impl(request).await
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_private_rest("coinstore.cancel_order")?;
        self.cancel_order_impl(request).await
    }

    async fn amend_order(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported("coinstore.amend_order")
    }

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.unsupported("coinstore.order_list")
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.ensure_private_rest("coinstore.batch_place_orders")?;
        self.batch_place_orders_impl(request).await
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_private_rest("coinstore.batch_cancel_orders")?;
        self.batch_cancel_orders_impl(request).await
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_private_rest("coinstore.cancel_all_orders")?;
        self.cancel_all_orders_impl(request).await
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.ensure_private_rest("coinstore.query_order")?;
        self.query_order_impl(request).await
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.ensure_private_rest("coinstore.get_open_orders")?;
        self.get_open_orders_impl(request).await
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.ensure_private_rest("coinstore.get_recent_fills")?;
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
