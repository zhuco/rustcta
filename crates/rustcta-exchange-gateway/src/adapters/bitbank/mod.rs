use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AccountId, AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchAtomicity, BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability,
    BatchExecutionMode, BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, CapabilitySupport,
    CredentialScope, EndpointAuth, EndpointCapability, EndpointTransport, ExchangeApiError,
    ExchangeApiResult, ExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookCapability, OrderBookChecksumMode,
    OrderBookRequest, OrderBookResponse, OrderBookStrictness, OrderListRequest, OrderListResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, RequestContext,
    SymbolRulesRequest, SymbolRulesResponse, TenantId, TimeInForce,
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

pub use config::BitbankGatewayConfig;
use transport::BitbankRest;

#[derive(Clone)]
pub struct BitbankGatewayAdapter {
    exchange_id: ExchangeId,
    config: BitbankGatewayConfig,
    rest: BitbankRest,
}

impl BitbankGatewayAdapter {
    pub fn new(config: BitbankGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("bitbank").map_err(validation_error)?;
        let rest = BitbankRest::new(
            exchange_id.clone(),
            config.public_rest_base_url.clone(),
            config.private_rest_base_url.clone(),
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
                message: format!("bitbank adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_spot(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if market_type != MarketType::Spot {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitbank.non_spot_market_type",
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

    async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: &str,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_signed_post(
                self.config.api_key.as_deref().unwrap_or_default(),
                self.config.api_secret.as_deref().unwrap_or_default(),
                endpoint,
                body,
            )
            .await
    }

    async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &std::collections::HashMap<String, String>,
    ) -> ExchangeApiResult<serde_json::Value> {
        self.ensure_private_rest(operation)?;
        self.rest
            .send_signed_get(
                self.config.api_key.as_deref().unwrap_or_default(),
                self.config.api_secret.as_deref().unwrap_or_default(),
                endpoint,
                params,
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
                    message: "bitbank private REST readback requires context.tenant_id".to_string(),
                })?;
        let account_id =
            context
                .account_id
                .clone()
                .ok_or_else(|| ExchangeApiError::InvalidRequest {
                    message: "bitbank private REST readback requires context.account_id"
                        .to_string(),
                })?;
        Ok((tenant_id, account_id))
    }
}

#[async_trait]
impl GatewayAdapter for BitbankGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some("bitbank spot REST adapter".to_string()),
        }
    }
}

#[async_trait]
impl ExchangeClient for BitbankGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types = vec![MarketType::Spot];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = self.config.private_rest_enabled();
        capabilities.supports_public_streams = self.config.enabled_public_streams;
        capabilities.supports_private_streams = self.config.enabled_private_streams;
        capabilities.private_stream_capabilities = Some(
            streams::bitbank_private_stream_capabilities(self.config.enabled_private_streams),
        );
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = self.config.private_rest_enabled();
        capabilities.supports_fees = self.config.private_rest_enabled();
        capabilities.supports_place_order = false;
        capabilities.supports_cancel_order = false;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_query_order = self.config.private_rest_enabled();
        capabilities.supports_open_orders = self.config.private_rest_enabled();
        capabilities.supports_recent_fills = self.config.private_rest_enabled();
        capabilities.supports_batch_cancel_order = self.config.private_rest_enabled();
        capabilities.supports_client_order_id = false;
        capabilities.supports_post_only = true;
        capabilities.supports_order_types =
            vec![OrderType::Market, OrderType::Limit, OrderType::PostOnly];
        capabilities.supports_time_in_force = vec![TimeInForce::GTC];
        capabilities.max_order_book_depth = Some(200);
        capabilities.order_book = OrderBookCapability {
            strictness: OrderBookStrictness::BestEffortDelta,
            supports_sequence: true,
            supports_checksum: false,
            checksum_mode: OrderBookChecksumMode::Disabled,
            supports_resync_endpoint: true,
            max_depth: Some(200),
        };
        apply_bitbank_capabilities_v2(&mut capabilities);
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.get_balances_impl(request).await
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bitbank.positions.unsupported_spot")
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
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_spot(symbol.market_type)?;
        }
        self.unsupported("bitbank.get_fees.rest_parser_pending")
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.unsupported("bitbank.place_order.offline_request_spec_only")
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.unsupported("bitbank.quote_market_order.unsupported")
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.unsupported("bitbank.cancel_order.offline_request_spec_only")
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.unsupported("bitbank.amend_order.unsupported")
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.unsupported("bitbank.order_list.unsupported")
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bitbank.batch_place_orders.unsupported")
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
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bitbank.cancel_all_orders.unsupported")
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

fn validation_error(error: rustcta_types::ValidationError) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}

fn apply_bitbank_capabilities_v2(capabilities: &mut ExchangeClientCapabilities) {
    let private_support = if capabilities.supports_private_rest {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "bitbank private REST requires credentials and write/readback parsers remain guarded",
        )
    };
    let batch_cancel_support = if capabilities.supports_private_rest {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported(
            "bitbank native same-pair batch cancel requires explicit private REST credentials",
        )
    };
    capabilities.capabilities_v2.private_rest = private_support.clone();
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("bitbank has no native Spot batch place endpoint");
    capabilities.capabilities_v2.batch_cancel_orders = BatchCapability {
        support: batch_cancel_support.clone(),
        mode: BatchExecutionMode::Native,
        atomicity: BatchAtomicity::NonAtomic,
        max_items: None,
        same_symbol_required: true,
        same_market_type_required: true,
        supports_client_order_id: false,
        supports_partial_failure: true,
    };
    capabilities.capabilities_v2.cancel_all_orders =
        CapabilitySupport::unsupported("bitbank cancel-all is not exposed as a shared runtime");
    capabilities.capabilities_v2.endpoints = vec![
        bitbank_endpoint(
            "bitbank.get_symbol_rules",
            CapabilitySupport::native(),
            EndpointAuth::None,
            "GET",
            "/spot/pairs",
            vec![],
            "public_rest",
        ),
        bitbank_endpoint(
            "bitbank.get_order_book",
            CapabilitySupport::native(),
            EndpointAuth::None,
            "GET",
            "/{pair}/depth",
            vec![],
            "public_rest",
        ),
        bitbank_endpoint(
            "bitbank.query_order",
            private_support.clone(),
            EndpointAuth::Hmac,
            "GET",
            "/v1/user/spot/order",
            vec![CredentialScope::ReadOnly],
            "orders",
        ),
        bitbank_endpoint(
            "bitbank.get_open_orders",
            private_support.clone(),
            EndpointAuth::Hmac,
            "GET",
            "/v1/user/spot/active_orders",
            vec![CredentialScope::ReadOnly],
            "orders",
        ),
        bitbank_endpoint(
            "bitbank.get_recent_fills",
            private_support.clone(),
            EndpointAuth::Hmac,
            "GET",
            "/v1/user/spot/trade_history",
            vec![CredentialScope::ReadOnly],
            "orders",
        ),
        bitbank_endpoint(
            "bitbank.batch_cancel_orders",
            batch_cancel_support,
            EndpointAuth::Hmac,
            "POST",
            "/v1/user/spot/cancel_orders",
            vec![CredentialScope::Trade],
            "orders",
        ),
        bitbank_endpoint(
            "bitbank.amend_order",
            CapabilitySupport::unsupported("bitbank has no in-place amend endpoint"),
            EndpointAuth::None,
            "UNSUPPORTED",
            "/unsupported/bitbank/amend_order",
            vec![CredentialScope::Trade],
            "unsupported",
        ),
        bitbank_endpoint(
            "bitbank.place_order_list",
            CapabilitySupport::unsupported("bitbank has no OCO/OTO/order-list endpoint"),
            EndpointAuth::None,
            "UNSUPPORTED",
            "/unsupported/bitbank/place_order_list",
            vec![CredentialScope::Trade],
            "unsupported",
        ),
    ];
}

fn bitbank_endpoint(
    operation: &str,
    support: CapabilitySupport,
    auth: EndpointAuth,
    method: &str,
    path: &str,
    credential_scopes: Vec<CredentialScope>,
    rate_limit_bucket: &str,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Spot],
        transport: EndpointTransport::Rest,
        method: Some(method.to_string()),
        path: Some(path.to_string()),
        auth,
        credential_scopes,
        rate_limit_bucket: Some(rate_limit_bucket.to_string()),
        weight: Some(1),
        supports_testnet: false,
    }
}
