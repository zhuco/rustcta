use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, AuthRenewalPolicy, BalancesRequest, BalancesResponse,
    BatchAtomicity, BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability,
    BatchExecutionMode, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    CredentialScope, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, HeartbeatCapability, HeartbeatDirection,
    HeartbeatPolicy, HistoryCapability, OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest,
    OrderBookResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest, PlaceOrderResponse,
    PositionsRequest, PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription,
    QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest,
    RecentFillsResponse, ReconnectCapability, StreamAuthCapability, StreamHeartbeatDirection,
    StreamResyncCapability, StreamRuntimeCapability, SymbolRulesRequest, SymbolRulesResponse,
    TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{ExchangeId, MarketType, OrderType};

use super::GatewayAdapter;
use crate::GatewayExchangeStatus;

mod config;
#[cfg(test)]
mod live_tests;
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

pub use config::WeexGatewayConfig;
use transport::WeexRest;

#[derive(Clone)]
pub struct WeexGatewayAdapter {
    exchange_id: ExchangeId,
    config: WeexGatewayConfig,
    rest: WeexRest,
}

impl WeexGatewayAdapter {
    pub fn new(config: WeexGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("weex").map_err(validation_error)?;
        let rest = WeexRest::new(
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
        Self::new(WeexGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("weex adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "weex.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn private_credentials(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<(&str, &str, &str)> {
        if !self.config.enabled_private_rest {
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
        let passphrase = self
            .config
            .passphrase
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        Ok((api_key, api_secret, passphrase))
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, passphrase) = self.private_credentials(operation)?;
        self.rest
            .send_signed_get(endpoint, params, api_key, api_secret, passphrase)
            .await
    }

    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, passphrase) = self.private_credentials(operation)?;
        self.rest
            .send_signed_post(endpoint, params, api_key, api_secret, passphrase)
            .await
    }

    async fn send_signed_post_json(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, passphrase) = self.private_credentials(operation)?;
        self.rest
            .send_signed_post_json(endpoint, body, api_key, api_secret, passphrase)
            .await
    }

    async fn send_signed_delete(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, passphrase) = self.private_credentials(operation)?;
        self.rest
            .send_signed_delete(endpoint, params, api_key, api_secret, passphrase)
            .await
    }

    async fn send_signed_delete_json(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: &serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, passphrase) = self.private_credentials(operation)?;
        self.rest
            .send_signed_delete_json(endpoint, body, api_key, api_secret, passphrase)
            .await
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
impl GatewayAdapter for WeexGatewayAdapter {
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
                    "weex spot + linear perpetual public/private REST and websocket spec gateway adapter"
                } else {
                    "weex spot + linear perpetual public REST and websocket spec gateway adapter"
                }
                .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for WeexGatewayAdapter {
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
        capabilities.private_stream_capabilities = Some(if private {
            streams::weex_private_stream_capabilities()
        } else {
            rustcta_exchange_api::PrivateStreamCapabilities::unsupported(
                EXCHANGE_API_SCHEMA_VERSION,
            )
        });
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
        capabilities.supports_quote_market_order = false;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = false;
        capabilities.supports_post_only = false;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK];
        capabilities.supports_order_types = vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::IOC,
            OrderType::FOK,
        ];
        capabilities.max_order_book_depth = Some(200);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::strict_delta(Some(200));
        capabilities.max_recent_fill_limit = Some(100);
        capabilities.capabilities_v2 = weex_capabilities_v2(private);
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
        Err(ExchangeApiError::Unsupported {
            operation: "weex.place_quote_market_order",
        })
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
        Err(ExchangeApiError::Unsupported {
            operation: "weex.amend_order",
        })
    }

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        Err(ExchangeApiError::Unsupported {
            operation: "weex.place_order_list",
        })
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

fn weex_capabilities_v2(private: bool) -> rustcta_exchange_api::ExchangeClientCapabilitiesV2 {
    rustcta_exchange_api::ExchangeClientCapabilitiesV2 {
        public_rest: CapabilitySupport::native(),
        private_rest: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("requires WEEX API key, secret, and passphrase")
        },
        public_streams: CapabilitySupport::native(),
        private_streams: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("requires ACCESS header credentials")
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
                direction: StreamHeartbeatDirection::ServerPing,
                interval_ms: None,
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
                renewal_ms: None,
                uses_listen_key: false,
                requires_relogin_on_reconnect: true,
            },
            heartbeat_policy: HeartbeatPolicy {
                direction: HeartbeatDirection::ServerPing,
                ping_interval_ms: 0,
                pong_timeout_ms: 30_000,
                stale_message_ms: 60_000,
                requires_pong_payload_echo: false,
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
            max_items: Some(20),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
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
            max_items: Some(20),
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        },
        cancel_all_orders: if private {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("requires private REST credentials")
        },
        order_history: HistoryCapability {
            support: if private {
                CapabilitySupport::native()
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
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("requires private REST credentials")
            },
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: false,
            supports_from_id: false,
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
