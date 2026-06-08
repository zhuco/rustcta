use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse, BatchAtomicity,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability, BatchExecutionMode,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    CredentialScope, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, HeartbeatCapability, HistoryCapability,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, PrivateStreamSubscription,
    PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, ReconnectCapability, StreamAuthCapability,
    StreamHeartbeatDirection, StreamResyncCapability, StreamRuntimeCapability, SymbolRulesRequest,
    SymbolRulesResponse, TimeInForce,
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

pub use config::ExmoGatewayConfig;
use transport::ExmoRest;

#[derive(Clone)]
pub struct ExmoGatewayAdapter {
    exchange_id: ExchangeId,
    config: ExmoGatewayConfig,
    rest: ExmoRest,
    nonce: Arc<AtomicU64>,
}

impl ExmoGatewayAdapter {
    pub fn new(config: ExmoGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("exmo").map_err(validation_error)?;
        let rest = ExmoRest::new(
            exchange_id.clone(),
            config.rest_base_url.clone(),
            config.request_timeout_ms,
        )?;
        Ok(Self {
            exchange_id,
            config,
            rest,
            nonce: Arc::new(AtomicU64::new(Utc::now().timestamp_millis() as u64)),
        })
    }

    #[cfg(test)]
    pub fn default_public() -> ExchangeApiResult<Self> {
        Self::new(ExmoGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("EXMO adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "exmo.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn unsupported_private<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn ensure_private_rest(&self, operation: &'static str) -> ExchangeApiResult<()> {
        if !self.config.private_rest_enabled() {
            return Err(ExchangeApiError::Unsupported { operation });
        }
        Ok(())
    }

    fn api_key(&self, operation: &'static str) -> ExchangeApiResult<&str> {
        self.ensure_private_rest(operation)?;
        self.config
            .api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })
    }

    fn api_secret(&self, operation: &'static str) -> ExchangeApiResult<&str> {
        self.ensure_private_rest(operation)?;
        self.config
            .api_secret
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })
    }

    fn next_nonce(&self) -> String {
        let now = Utc::now().timestamp_millis().max(1) as u64;
        let mut current = self.nonce.load(Ordering::Relaxed);
        loop {
            let next = now.max(current + 1);
            match self
                .nonce
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::Relaxed)
            {
                Ok(_) => return next.to_string(),
                Err(observed) => current = observed,
            }
        }
    }

    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: Vec<(String, String)>,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_signed_post(
                endpoint,
                &params,
                self.api_key(operation)?,
                self.api_secret(operation)?,
            )
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
                    message: "EXMO private REST request requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "EXMO private REST request requires context.account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }

    pub fn wallet_withdraw_boundary(&self) -> ExchangeApiResult<()> {
        Err(ExchangeApiError::Unsupported {
            operation: "exmo.wallet_withdraw_excode_transfer",
        })
    }
}

#[async_trait]
impl GatewayAdapter for ExmoGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("EXMO spot REST gateway adapter with WS specs".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for ExmoGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let private_enabled = self.config.private_rest_enabled();
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = private_enabled;
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = private_enabled;
        capabilities.private_stream_capabilities =
            Some(streams::exmo_private_stream_capabilities(private_enabled));
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private_enabled;
        capabilities.supports_positions = false;
        capabilities.supports_fees = true;
        capabilities.supports_place_order = private_enabled;
        capabilities.supports_cancel_order = private_enabled;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_query_order = private_enabled;
        capabilities.supports_open_orders = private_enabled;
        capabilities.supports_recent_fills = private_enabled;
        capabilities.supports_quote_market_order = private_enabled;
        capabilities.supports_client_order_id = true;
        capabilities.supports_post_only = private_enabled;
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
        capabilities.max_order_book_depth = Some(1000);
        capabilities.max_recent_fill_limit = Some(100);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(1000));
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
        capabilities.capabilities_v2.private_streams = if private_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported("EXMO private WS requires API key and secret")
        };
        capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
            public: CapabilitySupport::native(),
            private: if private_enabled {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("EXMO private WS requires REST credentials")
            },
            supports_subscribe: true,
            supports_unsubscribe: true,
            heartbeat: HeartbeatCapability {
                supported: true,
                required: false,
                direction: StreamHeartbeatDirection::ServerPing,
                interval_ms: Some(180_000),
                timeout_ms: Some(600_000),
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
                required: private_enabled,
                credential_scopes: streams::exmo_credential_scopes(),
                renewal_ms: None,
                uses_listen_key: false,
                requires_relogin_on_reconnect: true,
            },
            ..StreamRuntimeCapability::default()
        };
        capabilities.capabilities_v2.batch_place_orders = BatchCapability::unsupported(
            "EXMO has no documented native batch place endpoint in spot REST v1.1",
        );
        capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
            support: CapabilitySupport::unsupported(
                "EXMO has no documented native batch cancel endpoint in spot REST v1.1",
            ),
            mode: BatchExecutionMode::Unsupported,
            atomicity: BatchAtomicity::Unknown,
            max_items: None,
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: false,
            supports_partial_failure: false,
        };
        capabilities.capabilities_v2.fills_history = HistoryCapability {
            support: if private_enabled {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("EXMO user_trades requires private credentials")
            },
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: false,
            max_limit: Some(100),
            max_window_ms: None,
        };
        capabilities.capabilities_v2.order_history = HistoryCapability {
            support: if private_enabled {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "EXMO order_trades/user_open_orders require private credentials",
                )
            },
            supports_since: false,
            supports_until: false,
            supports_limit: false,
            supports_cursor: false,
            supports_from_id: true,
            max_limit: None,
            max_window_ms: None,
        };
        capabilities.capabilities_v2.credential_scopes =
            vec![CredentialScope::ReadOnly, CredentialScope::Trade];
        capabilities.apply_v2_to_legacy_flags();
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_quote_market_order = private_enabled;
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

    async fn batch_place_orders(
        &self,
        _request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.unsupported_private("exmo.batch_place_orders")
    }

    async fn batch_cancel_orders(
        &self,
        _request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.unsupported_private("exmo.batch_cancel_orders")
    }

    async fn amend_order(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported_private("exmo.amend_order")
    }

    async fn cancel_all_orders(
        &self,
        _request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.unsupported_private("exmo.cancel_all_orders")
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
