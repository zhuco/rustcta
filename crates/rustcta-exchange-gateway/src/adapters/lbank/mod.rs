use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, AmendOrderRequest, AmendOrderResponse, AuthRenewalKind, AuthRenewalPolicy,
    BalancesRequest, BalancesResponse, BatchAtomicity, BatchCancelOrdersRequest,
    BatchCancelOrdersResponse, BatchCapability, BatchExecutionMode, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, CapabilitySupport, CredentialScope, ExchangeApiError, ExchangeApiResult,
    ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse, HeartbeatCapability,
    HeartbeatDirection, HeartbeatPolicy, HistoryCapability, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    ReconnectCapability, RequestContext, StreamAuthCapability, StreamHeartbeatDirection,
    StreamResyncCapability, StreamRuntimeCapability, SymbolRulesRequest, SymbolRulesResponse,
    TenantId, TimeInForce,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;
use streams::lbank_private_stream_capabilities;

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
mod test_support;
mod transport;

pub use config::LBankGatewayConfig;
#[allow(unused_imports)]
pub use parser::{LBankBookTicker, LBankKline, LBankPublicTrade, LBankTicker};
#[allow(unused_imports)]
pub use private::LBankPrivateAck;
use transport::LBankRest;

#[derive(Clone)]
pub struct LBankGatewayAdapter {
    exchange_id: ExchangeId,
    config: LBankGatewayConfig,
    rest: LBankRest,
}

impl LBankGatewayAdapter {
    pub fn new(config: LBankGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("lbank").map_err(parser::validation_error)?;
        let rest = LBankRest::new(
            exchange_id.clone(),
            config.spot_rest_base_url.clone(),
            config.contract_rest_base_url.clone(),
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
        Self::new(LBankGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("lbank adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "lbank.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "lbank.market_type",
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

    async fn send_spot_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &std::collections::HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_spot_signed_post(
                endpoint,
                params,
                self.config.api_key.as_deref().unwrap_or_default(),
                self.config.api_secret.as_deref().unwrap_or_default(),
            )
            .await
    }

    async fn send_contract_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &std::collections::HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_contract_signed_post(
                endpoint,
                params,
                self.config.api_key.as_deref().unwrap_or_default(),
                self.config.api_secret.as_deref().unwrap_or_default(),
            )
            .await
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
                    message: "lbank private REST request requires tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "lbank private REST request requires account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for LBankGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("lbank spot/perpetual REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for LBankGatewayAdapter {
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
        capabilities.supports_private_streams = private;
        capabilities.private_stream_capabilities = if private {
            Some(lbank_private_stream_capabilities())
        } else {
            capabilities.private_stream_capabilities
        };
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private;
        capabilities.supports_positions = private;
        capabilities.supports_fees = private;
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_batch_place_order = private;
        capabilities.supports_batch_cancel_order = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_quote_market_order = private;
        capabilities.supports_client_order_id = true;
        capabilities.supports_post_only = true;
        capabilities.supports_reduce_only = private;
        capabilities.supports_time_in_force = vec![
            TimeInForce::GTC,
            TimeInForce::IOC,
            TimeInForce::FOK,
            TimeInForce::GTX,
        ];
        capabilities.supports_order_types = vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::PostOnly,
            OrderType::IOC,
            OrderType::FOK,
        ];
        capabilities.max_order_book_depth = Some(200);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(200));
        capabilities.max_recent_fill_limit = Some(100);
        capabilities.capabilities_v2 = lbank_capabilities_v2(private);
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
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported("lbank.amend_order")
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

fn lbank_capabilities_v2(private: bool) -> rustcta_exchange_api::ExchangeClientCapabilitiesV2 {
    rustcta_exchange_api::ExchangeClientCapabilitiesV2 {
        public_rest: CapabilitySupport::native(),
        private_rest: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("requires LBank API key and secret")
        },
        public_streams: CapabilitySupport::native(),
        private_streams: if private {
            CapabilitySupport::rest_fallback(
                "spot private stream uses REST-created subscribeKey; perpetual private WS remains unsupported",
            )
        } else {
            CapabilitySupport::unsupported("requires LBank API key and secret")
        },
        stream_runtime: StreamRuntimeCapability {
            public: CapabilitySupport::native(),
            private: if private {
                CapabilitySupport::rest_fallback("Spot listen key lifecycle is REST-managed")
            } else {
                CapabilitySupport::unsupported("private credentials are not configured")
            },
            supports_subscribe: true,
            supports_unsubscribe: false,
            heartbeat: HeartbeatCapability {
                supported: true,
                required: true,
                direction: StreamHeartbeatDirection::ClientPing,
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
                balances: true,
                positions: true,
                orders: true,
            },
            auth: StreamAuthCapability {
                required: private,
                credential_scopes: if private {
                    vec![CredentialScope::ReadOnly, CredentialScope::Trade]
                } else {
                    Vec::new()
                },
                renewal_ms: Some(30 * 60 * 1_000),
                uses_listen_key: true,
                requires_relogin_on_reconnect: true,
            },
            heartbeat_policy: HeartbeatPolicy {
                direction: HeartbeatDirection::ClientPing,
                ping_interval_ms: 30_000,
                pong_timeout_ms: 10_000,
                stale_message_ms: 60_000,
                requires_pong_payload_echo: true,
            },
            auth_renewal_policy: AuthRenewalPolicy {
                kind: AuthRenewalKind::ListenKeyKeepAlive,
                renew_before_expiry_ms: 60_000,
                renewal_interval_ms: Some(30 * 60 * 1_000),
                reconnect_on_renewal_failure: true,
                resubscribe_after_renewal: true,
            },
            ..StreamRuntimeCapability::default()
        },
        batch_place_orders: BatchCapability {
            support: if private {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("requires private REST credentials")
            },
            mode: if private {
                BatchExecutionMode::Native
            } else {
                BatchExecutionMode::Unsupported
            },
            atomicity: BatchAtomicity::Partial,
            max_items: Some(50),
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
            ..BatchCapability::default()
        },
        batch_cancel_orders: BatchCapability {
            support: if private {
                CapabilitySupport::composed(
                    "Spot batch cancel is composed from cancel_order; perpetual cancel is unsupported",
                )
            } else {
                CapabilitySupport::unsupported("requires private REST credentials")
            },
            mode: if private {
                BatchExecutionMode::ComposedSequential
            } else {
                BatchExecutionMode::Unsupported
            },
            atomicity: BatchAtomicity::NonAtomic,
            max_items: Some(50),
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
            ..BatchCapability::default()
        },
        cancel_all_orders: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("requires private REST credentials")
        },
        order_history: HistoryCapability {
            support: if private {
                CapabilitySupport::rest_fallback("Spot order status/open order REST only")
            } else {
                CapabilitySupport::unsupported("requires private REST credentials")
            },
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: true,
            max_limit: Some(100),
            max_window_ms: None,
        },
        fills_history: HistoryCapability {
            support: if private {
                CapabilitySupport::rest_fallback(
                    "Spot fills use order_transaction_detail or transaction_history",
                )
            } else {
                CapabilitySupport::unsupported("requires private REST credentials")
            },
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: true,
            max_limit: Some(100),
            max_window_ms: None,
        },
        credential_scopes: if private {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        },
        ..rustcta_exchange_api::ExchangeClientCapabilitiesV2::default()
    }
}
