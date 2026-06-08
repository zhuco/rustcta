use std::collections::HashMap;

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

pub use config::CoinbaseExchangeGatewayConfig;
use transport::CoinbaseExchangeRest;

#[derive(Clone)]
pub struct CoinbaseExchangeGatewayAdapter {
    exchange_id: ExchangeId,
    config: CoinbaseExchangeGatewayConfig,
    rest: CoinbaseExchangeRest,
}

impl CoinbaseExchangeGatewayAdapter {
    pub fn new(config: CoinbaseExchangeGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("coinbaseexchange").map_err(validation_error)?;
        let rest = CoinbaseExchangeRest::new(
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
        Self::new(CoinbaseExchangeGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!(
                    "coinbaseexchange adapter cannot serve request for exchange {exchange}"
                ),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinbaseexchange.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn unsupported_private<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
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
        let api_passphrase = self
            .config
            .api_passphrase
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        Ok((api_key, api_secret, api_passphrase))
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, api_passphrase) = self.private_credentials(operation)?;
        self.rest
            .send_signed_get(endpoint, params, api_key, api_secret, api_passphrase)
            .await
    }

    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &serde_json::Value,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, api_passphrase) = self.private_credentials(operation)?;
        self.rest
            .send_signed_post(endpoint, params, body, api_key, api_secret, api_passphrase)
            .await
    }

    async fn send_signed_delete(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, api_passphrase) = self.private_credentials(operation)?;
        self.rest
            .send_signed_delete(endpoint, params, api_key, api_secret, api_passphrase)
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
                    message: "coinbaseexchange private REST readback requires context.tenant_id"
                        .to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinbaseexchange private REST readback requires context.account_id"
                        .to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for CoinbaseExchangeGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("coinbaseexchange spot public REST gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for CoinbaseExchangeGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = self.config.private_rest_enabled();
        capabilities.supports_public_streams = true;
        capabilities.supports_private_streams = self.config.private_rest_enabled();
        capabilities.private_stream_capabilities =
            Some(streams::coinbaseexchange_private_stream_capabilities(
                self.config.private_rest_enabled(),
            ));
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = self.config.private_rest_enabled();
        capabilities.supports_fees = self.config.private_rest_enabled();
        capabilities.supports_place_order = self.config.private_rest_enabled();
        capabilities.supports_cancel_order = self.config.private_rest_enabled();
        capabilities.supports_cancel_all_orders = self.config.private_rest_enabled();
        capabilities.supports_quote_market_order = self.config.private_rest_enabled();
        capabilities.supports_amend_order = false;
        capabilities.supports_query_order = self.config.private_rest_enabled();
        capabilities.supports_open_orders = self.config.private_rest_enabled();
        capabilities.supports_recent_fills = self.config.private_rest_enabled();
        capabilities.supports_batch_place_order = self.config.private_rest_enabled();
        capabilities.supports_batch_cancel_order = self.config.private_rest_enabled();
        capabilities.supports_client_order_id = true;
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
        capabilities.max_order_book_depth = Some(100);
        capabilities.max_recent_fill_limit = Some(500);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(100));
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
        capabilities.capabilities_v2.private_streams = if self.config.private_rest_enabled() {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "coinbaseexchange private streams require API key, secret and passphrase",
            )
        };
        capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
            public: CapabilitySupport::native(),
            private: if self.config.private_rest_enabled() {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "coinbaseexchange private stream auth requires API key, secret and passphrase",
                )
            },
            supports_subscribe: true,
            supports_unsubscribe: true,
            heartbeat: HeartbeatCapability {
                supported: true,
                required: true,
                direction: StreamHeartbeatDirection::ServerPing,
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
                positions: false,
                orders: true,
            },
            auth: StreamAuthCapability {
                required: self.config.private_rest_enabled(),
                credential_scopes: vec![CredentialScope::ReadOnly, CredentialScope::Trade],
                renewal_ms: Some(20 * 60 * 60 * 1_000),
                uses_listen_key: false,
                requires_relogin_on_reconnect: true,
            },
            ..StreamRuntimeCapability::default()
        };
        capabilities.capabilities_v2.batch_place_orders = BatchCapability {
            support: if self.config.private_rest_enabled() {
                CapabilitySupport::composed("Coinbase Exchange has no native batch place endpoint; gateway sends sequential order creates")
            } else {
                CapabilitySupport::unsupported(
                    "coinbaseexchange batch place requires private REST credentials",
                )
            },
            mode: if self.config.private_rest_enabled() {
                BatchExecutionMode::ComposedSequential
            } else {
                BatchExecutionMode::Unsupported
            },
            atomicity: BatchAtomicity::NonAtomic,
            max_items: Some(20),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: false,
        };
        capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
            support: if self.config.private_rest_enabled() {
                CapabilitySupport::composed(
                    "Coinbase Exchange batch cancel is gateway-composed from sequential DELETE /orders/{id} requests",
                )
            } else {
                CapabilitySupport::unsupported(
                    "coinbaseexchange batch cancel requires private REST credentials",
                )
            },
            mode: if self.config.private_rest_enabled() {
                BatchExecutionMode::ComposedSequential
            } else {
                BatchExecutionMode::Unsupported
            },
            atomicity: BatchAtomicity::NonAtomic,
            max_items: Some(20),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: false,
        };
        capabilities.capabilities_v2.fills_history = HistoryCapability {
            support: if self.config.private_rest_enabled() {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "coinbaseexchange fills history requires private REST credentials",
                )
            },
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: false,
            max_limit: Some(500),
            max_window_ms: None,
        };
        capabilities.capabilities_v2.order_history = HistoryCapability {
            support: if self.config.private_rest_enabled() {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "coinbaseexchange order history requires private REST credentials",
                )
            },
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: false,
            max_limit: Some(500),
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
        self.unsupported_private("coinbaseexchange.get_positions")
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

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.amend_order_impl(request).await
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
