use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountControlCapabilities, BalancesRequest, BalancesResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    CredentialScope, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, FundingRatesRequest,
    FundingRatesResponse, HeartbeatDirection, HeartbeatPolicy, HistoryCapability,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookCapability, OrderBookRequest,
    OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, SetLeverageRequest,
    SetLeverageResponse, SymbolRulesRequest, SymbolRulesResponse, TimeInForce,
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

pub use config::MexcGatewayConfig;
use transport::MexcPublicRest;

#[derive(Clone)]
pub struct MexcGatewayAdapter {
    exchange_id: ExchangeId,
    config: MexcGatewayConfig,
    rest: MexcPublicRest,
}

impl MexcGatewayAdapter {
    pub fn new(config: MexcGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("mexc").map_err(validation_error)?;
        let rest = MexcPublicRest::new(
            exchange_id.clone(),
            config.rest_base_url.clone(),
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
        Self::new(MexcGatewayConfig::default())
    }

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("mexc adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "mexc.non_spot_market_type",
            });
        }
        Ok(())
    }

    fn ensure_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(market_type, MarketType::Spot | MarketType::Perpetual) {
            return Err(ExchangeApiError::Unsupported {
                operation: "mexc.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn ensure_optional_market_type(
        &self,
        market_type: Option<MarketType>,
    ) -> ExchangeApiResult<()> {
        if let Some(market_type) = market_type {
            self.ensure_market_type(market_type)?;
        }
        Ok(())
    }

    fn unsupported_private<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }

    fn private_credentials(&self, operation: &'static str) -> ExchangeApiResult<(&str, &str, u64)> {
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
        Ok((api_key, api_secret, self.config.recv_window_ms))
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_get(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_post(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_signed_delete(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, recv_window_ms) = self.private_credentials(operation)?;
        self.rest
            .send_signed_delete(endpoint, params, api_key, api_secret, recv_window_ms)
            .await
    }

    async fn send_contract_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, _) = self.private_credentials(operation)?;
        self.rest
            .send_contract_signed_get(endpoint, params, api_key, api_secret)
            .await
    }

    async fn send_contract_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&serde_json::Value>,
    ) -> ExchangeApiResult<serde_json::Value> {
        let (api_key, api_secret, _) = self.private_credentials(operation)?;
        self.rest
            .send_contract_signed_post(endpoint, params, body, api_key, api_secret)
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
                    message: "mexc private REST readback requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "mexc private REST readback requires context.account_id".to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for MexcGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("mexc public REST gateway adapter".to_string()),
        }
    }

    fn account_control_capabilities(&self) -> AccountControlCapabilities {
        let private = self.config.private_rest_enabled();
        AccountControlCapabilities {
            exchange: self.exchange_id.clone(),
            supports_symbol_account_config: false,
            supports_leverage: private,
            supports_position_mode_change: false,
            supports_close_position: false,
            supports_countdown_cancel_all: false,
        }
    }

    async fn set_leverage(
        &self,
        request: SetLeverageRequest,
    ) -> ExchangeApiResult<SetLeverageResponse> {
        self.set_leverage_impl(request).await
    }
}

#[async_trait]
impl ExchangeClient for MexcGatewayAdapter {
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
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = private;
        capabilities.supports_positions = private;
        capabilities.supports_fees = private;
        capabilities.supports_funding_rates = true;
        capabilities.supports_place_order = private;
        capabilities.supports_cancel_order = private;
        capabilities.supports_cancel_all_orders = private;
        capabilities.supports_quote_market_order = private;
        capabilities.supports_query_order = private;
        capabilities.supports_open_orders = private;
        capabilities.supports_recent_fills = private;
        capabilities.supports_client_order_id = true;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK];
        capabilities.supports_order_types = vec![
            OrderType::Market,
            OrderType::Limit,
            OrderType::PostOnly,
            OrderType::IOC,
            OrderType::FOK,
        ];
        capabilities.max_order_book_depth = Some(1000);
        capabilities.order_book = OrderBookCapability::strict_delta(Some(1000));
        capabilities.max_recent_fill_limit = Some(1000);
        capabilities.refresh_v2_from_legacy_flags();
        capabilities.capabilities_v2.public_streams = CapabilitySupport::native();
        capabilities.capabilities_v2.private_streams = CapabilitySupport::rest_fallback(
            "private WebSocket is not wired; reconciliation uses private REST readbacks",
        );
        capabilities.capabilities_v2.stream_runtime.heartbeat_policy = HeartbeatPolicy {
            direction: HeartbeatDirection::ClientPing,
            ping_interval_ms: 30_000,
            pong_timeout_ms: 10_000,
            stale_message_ms: 60_000,
            requires_pong_payload_echo: false,
        };
        capabilities.capabilities_v2.stream_runtime.public =
            capabilities.capabilities_v2.public_streams.clone();
        capabilities.capabilities_v2.stream_runtime.private =
            capabilities.capabilities_v2.private_streams.clone();
        capabilities
            .capabilities_v2
            .stream_runtime
            .resync
            .order_book = true;
        capabilities.capabilities_v2.stream_runtime.resync.balances = private;
        capabilities.capabilities_v2.stream_runtime.resync.orders = private;
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat
            .direction = rustcta_exchange_api::StreamHeartbeatDirection::ClientPing;
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat
            .required = true;
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat
            .interval_ms = Some(30_000);
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat
            .timeout_ms = Some(10_000);
        capabilities
            .capabilities_v2
            .stream_runtime
            .heartbeat_policy
            .direction = HeartbeatDirection::ClientPing;
        capabilities.capabilities_v2.batch_place_orders = rustcta_exchange_api::BatchCapability {
            support: CapabilitySupport::composed(
                "planner may compose sequential single-order REST calls",
            ),
            mode: rustcta_exchange_api::BatchExecutionMode::ComposedSequential,
            atomicity: rustcta_exchange_api::BatchAtomicity::NonAtomic,
            max_items: Some(20),
            same_symbol_required: false,
            same_market_type_required: true,
            supports_client_order_id: true,
            supports_partial_failure: true,
        };
        capabilities.capabilities_v2.batch_cancel_orders = rustcta_exchange_api::BatchCapability {
            support: CapabilitySupport::native(),
            mode: rustcta_exchange_api::BatchExecutionMode::Native,
            atomicity: rustcta_exchange_api::BatchAtomicity::Partial,
            max_items: None,
            same_symbol_required: true,
            same_market_type_required: true,
            supports_client_order_id: false,
            supports_partial_failure: true,
        };
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
            supports_since: true,
            supports_until: true,
            supports_limit: true,
            supports_cursor: true,
            supports_from_id: true,
            max_limit: Some(1000),
            max_window_ms: None,
        };
        capabilities.capabilities_v2.credential_scopes = if private {
            vec![CredentialScope::ReadOnly, CredentialScope::Trade]
        } else {
            Vec::new()
        };
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

    async fn get_funding_rates(
        &self,
        request: FundingRatesRequest,
    ) -> ExchangeApiResult<FundingRatesResponse> {
        self.get_funding_rates_impl(request).await
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
        _subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        self.unsupported_private("mexc.subscribe_private_stream")
    }
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
