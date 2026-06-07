use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, AmendOrderRequest, AmendOrderResponse, AuthRenewalPolicy, BalancesRequest,
    BalancesResponse, BatchAtomicity, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchCapability, BatchExecutionMode, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse,
    CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse,
    CapabilitySupport, CredentialScope, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, HeartbeatCapability, HeartbeatDirection,
    HeartbeatPolicy, HistoryCapability, OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest,
    OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, ReconnectCapability,
    RequestContext, StreamAuthCapability, StreamHeartbeatDirection, StreamResyncCapability,
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

pub use config::AscendexGatewayConfig;
use transport::AscendexRest;

#[derive(Clone)]
pub struct AscendexGatewayAdapter {
    exchange_id: ExchangeId,
    config: AscendexGatewayConfig,
    rest: AscendexRest,
}

impl AscendexGatewayAdapter {
    pub fn new(config: AscendexGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("ascendex").map_err(parser::validation_error)?;
        let rest = AscendexRest::new(
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
        Self::new(AscendexGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("ascendex adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "ascendex.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn private_credentials(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<(&str, &str, &str)> {
        if !self.config.enabled_private_rest {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        let account_group = self
            .config
            .account_group
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
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
        Ok((account_group, api_key, api_secret))
    }

    fn group_endpoint(&self, suffix: &str) -> ExchangeApiResult<String> {
        let account_group = self
            .config
            .account_group
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported {
                operation: "ascendex.account_group",
            })?;
        Ok(format!(
            "/{}/{}",
            account_group.trim_matches('/'),
            suffix.trim_start_matches('/')
        ))
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        sign_path: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (_, api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_signed_get(endpoint, sign_path, params, api_key, api_secret)
            .await
    }

    async fn send_signed_json(
        &self,
        operation: &'static str,
        method: reqwest::Method,
        endpoint: &str,
        sign_path: &str,
        body: serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (_, api_key, api_secret) = self.private_credentials(operation)?;
        self.rest
            .send_signed_json(method, endpoint, sign_path, body, api_key, api_secret)
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
impl GatewayAdapter for AscendexGatewayAdapter {
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
                    "ascendex spot + futures public/private REST gateway adapter"
                } else {
                    "ascendex spot + futures public REST gateway adapter"
                }
                .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for AscendexGatewayAdapter {
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
        capabilities.private_stream_capabilities =
            Some(streams::ascendex_private_stream_capabilities(private));
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private;
        capabilities.supports_positions = private;
        capabilities.supports_fees = private;
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_batch_place_order = private;
        capabilities.supports_batch_cancel_order = private;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = true;
        capabilities.supports_post_only = true;
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
        capabilities.max_order_book_depth = None;
        capabilities.order_book = rustcta_exchange_api::OrderBookCapability::strict_delta(None);
        capabilities.max_recent_fill_limit = Some(500);
        capabilities.capabilities_v2 = ascendex_capabilities_v2(private);
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
        self.unsupported("ascendex.place_quote_market_order")
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
        self.unsupported("ascendex.amend_order")
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

fn ascendex_capabilities_v2(private: bool) -> rustcta_exchange_api::ExchangeClientCapabilitiesV2 {
    let mut v2 = rustcta_exchange_api::ExchangeClientCapabilitiesV2 {
        public_rest: CapabilitySupport::native(),
        private_rest: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("requires ASCENDEX_ACCOUNT_GROUP, API key, and secret")
        },
        public_streams: CapabilitySupport::native(),
        private_streams: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "private stream auth requires account group, API key, and secret",
            )
        },
        stream_runtime: StreamRuntimeCapability {
            public: CapabilitySupport::native(),
            private: if private {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("private credentials are not configured")
            },
            supports_subscribe: true,
            supports_unsubscribe: false,
            heartbeat: HeartbeatCapability {
                supported: true,
                required: true,
                direction: StreamHeartbeatDirection::Bidirectional,
                interval_ms: Some(15_000),
                timeout_ms: Some(30_000),
            },
            reconnect: ReconnectCapability {
                supported: true,
                requires_resubscribe: true,
                preserves_session: false,
                max_reconnect_attempts: None,
            },
            resync: StreamResyncCapability {
                order_book: true,
                balances: false,
                positions: false,
                orders: true,
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
            heartbeat_policy: HeartbeatPolicy {
                direction: HeartbeatDirection::ApplicationMessage,
                ping_interval_ms: 15_000,
                pong_timeout_ms: 30_000,
                stale_message_ms: 30_000,
                requires_pong_payload_echo: true,
            },
            auth_renewal_policy: AuthRenewalPolicy::default(),
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
            max_items: Some(10),
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
            ..BatchCapability::default()
        },
        batch_cancel_orders: BatchCapability {
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
            max_items: Some(10),
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
                CapabilitySupport::rest_fallback("uses open/status/current history REST endpoints")
            } else {
                CapabilitySupport::unsupported("requires private REST credentials")
            },
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: true,
            max_limit: Some(500),
            max_window_ms: None,
        },
        fills_history: HistoryCapability {
            support: if private {
                CapabilitySupport::rest_fallback(
                    "recent fills are reconstructed from order history",
                )
            } else {
                CapabilitySupport::unsupported("requires private REST credentials")
            },
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: true,
            max_limit: Some(500),
            max_window_ms: None,
        },
        credential_scopes: if private {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        },
        ..rustcta_exchange_api::ExchangeClientCapabilitiesV2::default()
    };
    v2.endpoints = Vec::new();
    v2
}
