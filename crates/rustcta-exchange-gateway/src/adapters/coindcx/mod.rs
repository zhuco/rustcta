use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, AuthRenewalKind, AuthRenewalPolicy, BalancesRequest,
    BalancesResponse, BatchAtomicity, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchCapability, BatchExecutionMode, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse,
    CancelOrderRequest, CancelOrderResponse, CapabilitySupport, ExchangeApiError,
    ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse,
    HeartbeatCapability, HeartbeatDirection, HeartbeatPolicy, HistoryCapability, OpenOrdersRequest,
    OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
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
mod private_parser;
#[cfg(test)]
mod private_tests;
mod public;
#[cfg(test)]
mod public_tests;
mod signing;
mod streams;
#[cfg(test)]
mod tests;
mod transport;

pub use config::CoinDcxGatewayConfig;
use signing::CoinDcxPrivateCredentials;
use transport::CoinDcxRest;

#[derive(Clone)]
pub struct CoinDcxGatewayAdapter {
    exchange_id: ExchangeId,
    config: CoinDcxGatewayConfig,
    rest: CoinDcxRest,
}

impl CoinDcxGatewayAdapter {
    pub fn new(config: CoinDcxGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("coindcx").map_err(validation_error)?;
        let credentials = CoinDcxPrivateCredentials::from_config(&config);
        let rest = CoinDcxRest::new(
            exchange_id.clone(),
            config.spot_rest_base_url.clone(),
            config.futures_rest_base_url.clone(),
            config.public_rest_base_url.clone(),
            config.request_timeout_ms,
            credentials,
        )?;
        Ok(Self {
            exchange_id,
            config,
            rest,
        })
    }

    #[cfg(test)]
    pub fn default_public() -> ExchangeApiResult<Self> {
        Self::new(CoinDcxGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("coindcx adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "coindcx.unsupported_market_type",
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
        operation: &'static str,
    ) -> ExchangeApiResult<(
        rustcta_exchange_api::TenantId,
        rustcta_exchange_api::AccountId,
    )> {
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
impl GatewayAdapter for CoinDcxGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("coindcx spot + futures REST/Socket.IO gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for CoinDcxGatewayAdapter {
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
            Some(streams::coindcx_private_stream_capabilities())
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
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_batch_place_order = private;
        capabilities.supports_batch_cancel_order = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_quote_market_order = private;
        capabilities.supports_amend_order = private;
        capabilities.supports_order_list = false;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = false;
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
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::best_effort_delta(Some(50));
        capabilities.max_recent_fill_limit = Some(1000);
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_streams = CapabilitySupport::ws_only(
            "CoinDCX exposes Socket.IO stream specs, not a plain JSON WebSocket runtime",
        );
        capabilities.capabilities_v2.private_streams = if private {
            CapabilitySupport::ws_only(
                "CoinDCX private stream uses authenticated Socket.IO coindcx channel",
            )
        } else {
            CapabilitySupport::unsupported("private Socket.IO requires API key and secret")
        };
        capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
            public: CapabilitySupport::ws_only(
                "Socket.IO join payloads and parser helpers are implemented",
            ),
            private: if private {
                CapabilitySupport::ws_only(
                    "Socket.IO auth join payload and private parsers are implemented",
                )
            } else {
                CapabilitySupport::unsupported("private Socket.IO requires API key and secret")
            },
            supports_subscribe: true,
            supports_unsubscribe: false,
            supports_public_subscribe: true,
            supports_public_unsubscribe: false,
            supports_private_subscribe: private,
            supports_private_unsubscribe: false,
            heartbeat: HeartbeatCapability {
                supported: true,
                required: true,
                direction: StreamHeartbeatDirection::ClientPing,
                interval_ms: Some(25_000),
                timeout_ms: Some(75_000),
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
                credential_scopes: capabilities.capabilities_v2.credential_scopes.clone(),
                renewal_ms: Some(30 * 60 * 1000),
                uses_listen_key: false,
                requires_relogin_on_reconnect: private,
            },
            public_private_separate_connections: false,
            heartbeat_policy: HeartbeatPolicy {
                direction: HeartbeatDirection::ApplicationMessage,
                ping_interval_ms: 25_000,
                pong_timeout_ms: 35_000,
                stale_message_ms: 45_000,
                requires_pong_payload_echo: false,
            },
            auth_renewal_policy: AuthRenewalPolicy {
                kind: if private {
                    AuthRenewalKind::ReLogin
                } else {
                    AuthRenewalKind::None
                },
                renew_before_expiry_ms: 60_000,
                renewal_interval_ms: private.then_some(30 * 60 * 1000),
                reconnect_on_renewal_failure: true,
                resubscribe_after_renewal: true,
            },
            auth_renewal: AuthRenewalPolicy {
                kind: if private {
                    AuthRenewalKind::ReLogin
                } else {
                    AuthRenewalKind::None
                },
                renew_before_expiry_ms: 60_000,
                renewal_interval_ms: private.then_some(30 * 60 * 1000),
                reconnect_on_renewal_failure: true,
                resubscribe_after_renewal: true,
            },
            reconnect_requires_login: private,
            reconnect_requires_resubscribe: true,
            orderbook_requires_snapshot_after_reconnect: true,
            ..StreamRuntimeCapability::default()
        };
        capabilities.capabilities_v2.batch_place_orders = if private {
            BatchCapability {
                support: CapabilitySupport::native(),
                mode: BatchExecutionMode::Native,
                atomicity: BatchAtomicity::Partial,
                max_items: Some(10),
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("private REST credentials are disabled")
        };
        capabilities.capabilities_v2.batch_cancel_orders = if private {
            BatchCapability {
                support: CapabilitySupport::native(),
                mode: BatchExecutionMode::Native,
                atomicity: BatchAtomicity::Partial,
                max_items: Some(50),
                same_symbol_required: false,
                same_market_type_required: true,
                supports_client_order_id: true,
                supports_partial_failure: true,
            }
        } else {
            BatchCapability::unsupported("private REST credentials are disabled")
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
            HistoryCapability::unsupported("private REST credentials are disabled")
        };
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_private_rest(request).await
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.get_positions_private_rest(request).await
    }

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        self.get_symbol_rules_public_rest(request).await
    }

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        self.get_order_book_public_rest(request).await
    }

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        self.get_fees_private_rest(request).await
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_order_private_rest(request).await
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.place_quote_market_order_private_rest(request).await
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.cancel_order_private_rest(request).await
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.amend_order_private_rest(request).await
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.place_order_list_private_rest(request).await
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.batch_place_orders_private_rest(request).await
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.batch_cancel_orders_private_rest(request).await
    }

    async fn cancel_all_orders(
        &self,
        request: rustcta_exchange_api::CancelAllOrdersRequest,
    ) -> ExchangeApiResult<rustcta_exchange_api::CancelAllOrdersResponse> {
        self.cancel_all_orders_private_rest(request).await
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.query_order_private_rest(request).await
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.get_open_orders_private_rest(request).await
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.get_recent_fills_private_rest(request).await
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
