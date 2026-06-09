use async_trait::async_trait;
use chrono::Utc;
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchCapability, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, CapabilitySupport, CredentialScope, EndpointAuth, EndpointCapability,
    EndpointTransport, ExchangeApiError, ExchangeApiResult, ExchangeClient,
    ExchangeClientCapabilities, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, PrivateStreamCapabilities,
    PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse,
    QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse, SymbolRulesRequest,
    SymbolRulesResponse, TimeInForce,
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

pub use config::BullishGatewayConfig;
use transport::BullishRest;

#[derive(Clone)]
pub struct BullishGatewayAdapter {
    exchange_id: ExchangeId,
    config: BullishGatewayConfig,
    rest: BullishRest,
}

impl BullishGatewayAdapter {
    pub fn new(config: BullishGatewayConfig) -> ExchangeApiResult<Self> {
        let exchange_id = ExchangeId::new("bullish").map_err(validation_error)?;
        let rest = BullishRest::new(
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

    fn ensure_exchange(&self, exchange: &ExchangeId) -> ExchangeApiResult<()> {
        if exchange != &self.exchange_id {
            return Err(ExchangeApiError::InvalidRequest {
                message: format!("bullish adapter cannot serve request for exchange {exchange}"),
            });
        }
        Ok(())
    }

    fn ensure_supported_market_type(&self, market_type: MarketType) -> ExchangeApiResult<()> {
        if !matches!(
            market_type,
            MarketType::Spot | MarketType::Perpetual | MarketType::Futures
        ) {
            return Err(ExchangeApiError::Unsupported {
                operation: "bullish.unsupported_market_type",
            });
        }
        Ok(())
    }

    fn unsupported<T>(&self, operation: &'static str) -> ExchangeApiResult<T> {
        Err(ExchangeApiError::Unsupported { operation })
    }
}

#[async_trait]
impl GatewayAdapter for BullishGatewayAdapter {
    fn gateway_exchange_status(&self) -> GatewayExchangeStatus {
        GatewayExchangeStatus {
            exchange: self.exchange_id.to_string(),
            enabled: self.config.enabled,
            public_stream_connected: false,
            private_stream_connected: false,
            last_heartbeat_at: Some(Utc::now()),
            rate_limit_used: None,
            message: Some(
                "bullish audited gateway shell with offline request-spec and WS helpers"
                    .to_string(),
            ),
        }
    }
}

#[async_trait]
impl ExchangeClient for BullishGatewayAdapter {
    fn exchange(&self) -> ExchangeId {
        self.exchange_id.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        let mut capabilities = ExchangeClientCapabilities::new(self.exchange_id.clone());
        capabilities.market_types =
            vec![MarketType::Spot, MarketType::Perpetual, MarketType::Futures];
        capabilities.supports_public_rest = true;
        capabilities.supports_private_rest = false;
        capabilities.supports_public_streams = self.config.enabled_public_streams;
        capabilities.supports_private_streams =
            self.config.enabled_private_streams && self.config.jwt_token.is_some();
        capabilities.private_stream_capabilities = Some(PrivateStreamCapabilities::unsupported(
            rustcta_exchange_api::EXCHANGE_API_SCHEMA_VERSION,
        ));
        capabilities.supports_symbol_rules = true;
        capabilities.supports_order_book_snapshot = true;
        capabilities.supports_balances = false;
        capabilities.supports_positions = false;
        capabilities.supports_fees = false;
        capabilities.supports_place_order = false;
        capabilities.supports_cancel_order = false;
        capabilities.supports_query_order = false;
        capabilities.supports_open_orders = false;
        capabilities.supports_recent_fills = false;
        capabilities.supports_batch_place_order = false;
        capabilities.supports_batch_cancel_order = false;
        capabilities.supports_cancel_all_orders = false;
        capabilities.supports_quote_market_order = false;
        capabilities.supports_amend_order = false;
        capabilities.supports_order_list = false;
        capabilities.supports_client_order_id = true;
        capabilities.supports_reduce_only = true;
        capabilities.supports_post_only = true;
        capabilities.supports_time_in_force =
            vec![TimeInForce::GTC, TimeInForce::IOC, TimeInForce::FOK];
        capabilities.supports_order_types =
            vec![OrderType::Market, OrderType::Limit, OrderType::PostOnly];
        capabilities.max_order_book_depth = Some(200);
        capabilities.max_recent_fill_limit = Some(100);
        capabilities.capabilities_v2.stream_runtime = streams::bullish_stream_runtime_capability(
            self.config.enabled_public_streams,
            capabilities.supports_private_streams,
        );
        apply_bullish_capabilities_v2(&mut capabilities);
        capabilities
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bullish.balances_offline_request_spec_only")
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bullish.positions_offline_request_spec_only")
    }

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        self.get_symbol_rules_public_rest(request).await
    }

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.get_order_book_public_rest(request).await
    }

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        for symbol in &request.symbols {
            self.ensure_exchange(&symbol.exchange)?;
            self.ensure_supported_market_type(symbol.market_type)?;
        }
        self.unsupported("bullish.fees_offline_request_spec_only")
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("bullish.place_order_offline_request_spec_only")
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("bullish.quote_market_order_unsupported")
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("bullish.cancel_order_offline_request_spec_only")
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("bullish.amend_order_offline_request_spec_only")
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        self.ensure_exchange(&request.symbol().exchange)?;
        self.ensure_supported_market_type(request.symbol().market_type)?;
        self.unsupported("bullish.order_list_unsupported")
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bullish.batch_place_orders_unsupported")
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bullish.batch_cancel_orders_unsupported")
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bullish.cancel_all_orders_offline_request_spec_only")
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        self.ensure_exchange(&request.symbol.exchange)?;
        self.ensure_supported_market_type(request.symbol.market_type)?;
        self.unsupported("bullish.query_order_offline_request_spec_only")
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bullish.open_orders_offline_request_spec_only")
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        self.ensure_exchange(&request.exchange)?;
        self.unsupported("bullish.recent_fills_offline_request_spec_only")
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

fn apply_bullish_capabilities_v2(capabilities: &mut ExchangeClientCapabilities) {
    capabilities.capabilities_v2.public_rest = CapabilitySupport::native();
    capabilities.capabilities_v2.private_rest = CapabilitySupport::unsupported(
        "bullish private REST has signed request-spec fixtures only; JWT lifecycle, nonce-window guard, dry-run safety and reconciliation are not enabled",
    );
    capabilities.capabilities_v2.public_streams = if capabilities.supports_public_streams {
        CapabilitySupport::native()
    } else {
        CapabilitySupport::unsupported("bullish public streams disabled by config")
    };
    capabilities.capabilities_v2.private_streams = CapabilitySupport::unsupported(
        "bullish private stream auth is not promoted beyond documented endpoint boundary",
    );
    capabilities.capabilities_v2.batch_place_orders =
        BatchCapability::unsupported("bullish has no native shared batch-place endpoint");
    capabilities.capabilities_v2.batch_cancel_orders =
        BatchCapability::unsupported("bullish has no native shared batch-cancel endpoint");
    capabilities.capabilities_v2.cancel_all_orders = CapabilitySupport::unsupported(
        "bullish cancel-all command request spec exists, but live private write runtime remains disabled",
    );
    capabilities.capabilities_v2.credential_scopes =
        vec![CredentialScope::ReadOnly, CredentialScope::Trade];
    capabilities.capabilities_v2.endpoints = bullish_capability_endpoints();
}

fn bullish_capability_endpoints() -> Vec<EndpointCapability> {
    vec![
        bullish_endpoint(
            "bullish.amend_order",
            CapabilitySupport::unsupported(
                "bullish V1AmendOrder request-spec/signing boundary exists; live runtime awaits JWT lifecycle, nonce guard, response parser and reconciliation",
            ),
            Some("/trading-api/v2/command"),
            EndpointAuth::Jwt,
            vec![CredentialScope::Trade],
        ),
        bullish_endpoint(
            "bullish.cancel_all_orders",
            CapabilitySupport::unsupported(
                "bullish V1CancelAllOrdersByMarket request-spec/signing boundary exists; live runtime awaits JWT lifecycle, nonce guard, response parser and reconciliation",
            ),
            Some("/trading-api/v2/command"),
            EndpointAuth::Jwt,
            vec![CredentialScope::Trade],
        ),
        bullish_endpoint(
            "bullish.batch_place_orders",
            CapabilitySupport::unsupported("bullish has no native shared batch-place endpoint"),
            None,
            EndpointAuth::None,
            vec![CredentialScope::Trade],
        ),
        bullish_endpoint(
            "bullish.batch_cancel_orders",
            CapabilitySupport::unsupported("bullish has no native shared batch-cancel endpoint"),
            None,
            EndpointAuth::None,
            vec![CredentialScope::Trade],
        ),
        bullish_endpoint(
            "bullish.place_order_list",
            CapabilitySupport::unsupported("bullish has no native shared OCO/OTO order-list endpoint"),
            None,
            EndpointAuth::None,
            vec![CredentialScope::Trade],
        ),
    ]
}

fn bullish_endpoint(
    operation: &str,
    support: CapabilitySupport,
    path: Option<&str>,
    auth: EndpointAuth,
    credential_scopes: Vec<CredentialScope>,
) -> EndpointCapability {
    EndpointCapability {
        operation: operation.to_string(),
        support,
        market_types: vec![MarketType::Spot, MarketType::Perpetual, MarketType::Futures],
        transport: EndpointTransport::Rest,
        method: path.map(|_| "POST".to_string()),
        path: path.map(str::to_string),
        auth,
        credential_scopes,
        rate_limit_bucket: Some("bullish_private_command".to_string()),
        weight: Some(1),
        supports_testnet: true,
    }
}
