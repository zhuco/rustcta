use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, CapabilitySupport, CredentialScope, ExchangeApiError, ExchangeApiResult,
    ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse, HeartbeatCapability,
    HistoryCapability, OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse,
    OrderListRequest, OrderListResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    ReconnectCapability, StreamAuthCapability, StreamHeartbeatDirection, StreamResyncCapability,
    StreamRuntimeCapability, SymbolRulesRequest, SymbolRulesResponse, TimeInForce,
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

pub use config::CoinoneGatewayConfig;
use transport::CoinoneRest;

#[derive(Clone)]
pub struct CoinoneGatewayAdapter {
    exchange_id: ExchangeId,
    config: CoinoneGatewayConfig,
    rest: CoinoneRest,
}

impl CoinoneGatewayAdapter {
    pub fn new(config: CoinoneGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("coinone").map_err(validation_error)?;
        let rest = CoinoneRest::new(
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
        Self::new(CoinoneGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("coinone adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "coinone.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_krw_symbol(&self, symbol: &str) -> ExchangeApiResult<(String, String)> {
        parser::normalize_coinone_symbol(symbol)
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

    fn access_token(&self, operation: &'static str) -> ExchangeApiResult<&str> {
        self.ensure_private_rest(operation)?;
        self.config
            .access_token
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })
    }

    fn secret_key(&self, operation: &'static str) -> ExchangeApiResult<&str> {
        self.ensure_private_rest(operation)?;
        self.config
            .secret_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })
    }

    fn next_nonce(&self) -> String {
        uuid::Uuid::new_v4().to_string()
    }

    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_signed_post(endpoint, body, self.secret_key(operation)?)
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
                    message: "coinone private REST request requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "coinone private REST request requires context.account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }

    pub fn payment_wallet_fiat_boundary(&self) -> ExchangeApiResult<()> {
        Err(ExchangeApiError::Unsupported {
            operation: "coinone.payment_wallet_fiat_transfer",
        })
    }
}

#[async_trait]
impl GatewayAdapter for CoinoneGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("coinone KRW spot REST/WS gateway adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for CoinoneGatewayAdapter {
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
        capabilities.private_stream_capabilities = Some(
            streams::coinone_private_stream_capabilities(private_enabled),
        );
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private_enabled;
        capabilities.supports_fees = private_enabled;
        capabilities.supports_place_order = private_enabled;
        capabilities.supports_cancel_order = private_enabled;
        capabilities.supports_cancel_all_orders = private_enabled;
        capabilities.supports_query_order = private_enabled;
        capabilities.supports_open_orders = private_enabled;
        capabilities.supports_recent_fills = private_enabled;
        capabilities.supports_quote_market_order = private_enabled;
        capabilities.supports_client_order_id = true;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK];
        capabilities.supports_order_types = vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::IOC,
            OrderType::FOK,
        ];
        capabilities.max_order_book_depth = Some(30);
        capabilities.max_recent_fill_limit = Some(500);
        capabilities.order_book =
            rustcta_exchange_api::OrderBookCapability::snapshot_only(Some(30));
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
        capabilities.capabilities_v2.private_streams = if private_enabled {
            CapabilitySupport::native()
        } else {
            CapabilitySupport::unsupported(
                "coinone private streams require access token and secret key",
            )
        };
        capabilities.capabilities_v2.stream_runtime = StreamRuntimeCapability {
            public: CapabilitySupport::native(),
            private: if private_enabled {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported(
                    "coinone private WS auth requires REST trading credentials",
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
                required: private_enabled,
                credential_scopes: streams::coinone_credential_scopes(),
                renewal_ms: None,
                uses_listen_key: false,
                requires_relogin_on_reconnect: true,
            },
            ..StreamRuntimeCapability::default()
        };
        capabilities.capabilities_v2.batch_place_orders = BatchCapability::unsupported(
            "coinone batch place is intentionally unsupported until a documented native endpoint is mapped",
        );
        capabilities.capabilities_v2.batch_cancel_orders = BatchCapability::unsupported(
            "coinone native batch cancel is intentionally unsupported; use single cancel or native cancel-all only",
        );
        capabilities.capabilities_v2.fills_history = HistoryCapability {
            support: if private_enabled {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("coinone fills history requires private credentials")
            },
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: true,
            max_limit: Some(500),
            max_window_ms: None,
        };
        capabilities.capabilities_v2.order_history = HistoryCapability {
            support: if private_enabled {
                CapabilitySupport::native()
            } else {
                CapabilitySupport::unsupported("coinone order history requires private credentials")
            },
            supports_since: false,
            supports_until: false,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: true,
            max_limit: Some(500),
            max_window_ms: None,
        };
        capabilities.capabilities_v2.credential_scopes =
            vec![CredentialScope::ReadOnly, CredentialScope::Trade];
        capabilities.apply_v2_to_legacy_flags();
        capabilities.supports_batch_place_order = false;
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_impl(request).await
    }

    async fn get_positions(
        &self,
        _request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.unsupported_private("coinone.get_positions")
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
        self.unsupported_private("coinone.batch_place_orders")
    }

    async fn batch_cancel_orders(
        &self,
        _request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.unsupported_private("coinone.batch_cancel_orders")
    }

    async fn amend_order(
        &self,
        _request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.unsupported_private("coinone.amend_order")
    }

    async fn place_order_list(
        &self,
        _request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.unsupported_private("coinone.order_list_unsupported")
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
