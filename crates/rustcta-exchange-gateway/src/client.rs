use std::sync::Arc;

use async_trait::async_trait;
use rustcta_exchange_api::{
    AccountControlCapabilities, AmendOrderRequest, AmendOrderResponse, BalancesRequest,
    BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ClosePositionRequest, ClosePositionResponse, CountdownCancelAllRequest,
    CountdownCancelAllResponse, ExchangeApiError, ExchangeApiResult,
    ExchangeClient as ApiExchangeClient, ExchangeClientCapabilities, FeesRequest, FeesResponse,
    FundingRatesRequest, FundingRatesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse,
    PerpAccountControlProvider, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, QuoteMarketOrderRequest, RecentFillsRequest, RecentFillsResponse,
    SetLeverageRequest, SetLeverageResponse, SetPositionModeRequest, SetPositionModeResponse,
    SymbolAccountConfigRequest, SymbolAccountConfigResponse, SymbolRulesRequest,
    SymbolRulesResponse,
};
use rustcta_types::{AccountId, ExchangeError, ExchangeId, TenantId};

use crate::client_helpers::gateway_protocol_request;
use crate::{
    GatewayError, GatewayOperation, GatewayProtocolRequest, GatewayProtocolResponse,
    GatewayRequest, GatewayRequestPayload, GatewayResponse, GatewayStatus, GetCapabilitiesRequest,
    GetCapabilitiesResponse, GetStatusRequest, GetStatusResponse, SubscribeBooksRequest,
    SubscribeBooksResponse, SubscribePrivateRequest, SubscribePrivateResponse,
    GATEWAY_PROTOCOL_SCHEMA_VERSION,
};

#[async_trait]
pub trait ExchangeGateway: Send + Sync {
    async fn status(&self) -> Result<GatewayStatus, GatewayError>;
    async fn handle(&self, request: GatewayRequest) -> Result<GatewayResponse, GatewayError>;
}

#[async_trait]
pub trait LocalGateway: Send + Sync {
    async fn status(&self) -> Result<GatewayStatus, GatewayError>;
    async fn handle_typed(
        &self,
        request: GatewayProtocolRequest,
    ) -> Result<GatewayProtocolResponse, GatewayError>;
}

#[async_trait]
pub trait GatewayClient: Send + Sync {
    async fn send(
        &self,
        request: GatewayProtocolRequest,
    ) -> Result<GatewayProtocolResponse, GatewayError>;

    async fn get_status(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        include_exchanges: bool,
    ) -> Result<GetStatusResponse, GatewayError> {
        let response = self
            .send(gateway_protocol_request(
                request_id,
                tenant_id,
                account_id,
                GatewayOperation::GetStatus,
                GatewayRequestPayload::GetStatus(GetStatusRequest {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    include_exchanges,
                }),
            ))
            .await?;
        response.into_status()
    }

    async fn get_capabilities(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: GetCapabilitiesRequest,
    ) -> Result<GetCapabilitiesResponse, GatewayError> {
        let response = self
            .send(gateway_protocol_request(
                request_id,
                tenant_id,
                account_id,
                GatewayOperation::GetCapabilities,
                GatewayRequestPayload::GetCapabilities(request),
            ))
            .await?;
        response.into_capabilities()
    }

    async fn get_balances(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: BalancesRequest,
    ) -> Result<BalancesResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::GetBalances,
            GatewayRequestPayload::GetBalances(request),
        )
        .await?
        .into_balances()
    }

    async fn get_positions(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: PositionsRequest,
    ) -> Result<PositionsResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::GetPositions,
            GatewayRequestPayload::GetPositions(request),
        )
        .await?
        .into_positions()
    }

    async fn get_symbol_rules(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: SymbolRulesRequest,
    ) -> Result<SymbolRulesResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::GetSymbolRules,
            GatewayRequestPayload::GetSymbolRules(request),
        )
        .await?
        .into_symbol_rules()
    }

    async fn get_order_book(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: OrderBookRequest,
    ) -> Result<OrderBookResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::GetOrderBook,
            GatewayRequestPayload::GetOrderBook(request),
        )
        .await?
        .into_order_book()
    }

    async fn get_fees(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: FeesRequest,
    ) -> Result<FeesResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::GetFees,
            GatewayRequestPayload::GetFees(request),
        )
        .await?
        .into_fees()
    }

    async fn get_funding_rates(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: FundingRatesRequest,
    ) -> Result<FundingRatesResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::GetFundingRates,
            GatewayRequestPayload::GetFundingRates(request),
        )
        .await?
        .into_funding_rates()
    }

    async fn place_order(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: PlaceOrderRequest,
    ) -> Result<PlaceOrderResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::PlaceOrder,
            GatewayRequestPayload::PlaceOrder(request),
        )
        .await?
        .into_place_order()
    }

    async fn place_quote_market_order(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: QuoteMarketOrderRequest,
    ) -> Result<PlaceOrderResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::PlaceQuoteMarketOrder,
            GatewayRequestPayload::PlaceQuoteMarketOrder(request),
        )
        .await?
        .into_place_order()
    }

    async fn cancel_order(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: CancelOrderRequest,
    ) -> Result<CancelOrderResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::CancelOrder,
            GatewayRequestPayload::CancelOrder(request),
        )
        .await?
        .into_cancel_order()
    }

    async fn amend_order(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: AmendOrderRequest,
    ) -> Result<AmendOrderResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::AmendOrder,
            GatewayRequestPayload::AmendOrder(request),
        )
        .await?
        .into_amend_order()
    }

    async fn place_order_list(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: OrderListRequest,
    ) -> Result<OrderListResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::PlaceOrderList,
            GatewayRequestPayload::PlaceOrderList(request),
        )
        .await?
        .into_order_list()
    }

    async fn batch_place_orders(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: BatchPlaceOrdersRequest,
    ) -> Result<BatchPlaceOrdersResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::BatchPlaceOrders,
            GatewayRequestPayload::BatchPlaceOrders(request),
        )
        .await?
        .into_batch_place_orders()
    }

    async fn batch_cancel_orders(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: BatchCancelOrdersRequest,
    ) -> Result<BatchCancelOrdersResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::BatchCancelOrders,
            GatewayRequestPayload::BatchCancelOrders(request),
        )
        .await?
        .into_batch_cancel_orders()
    }

    async fn cancel_all_orders(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: CancelAllOrdersRequest,
    ) -> Result<CancelAllOrdersResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::CancelAllOrders,
            GatewayRequestPayload::CancelAllOrders(request),
        )
        .await?
        .into_cancel_all_orders()
    }

    async fn query_order(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: QueryOrderRequest,
    ) -> Result<QueryOrderResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::QueryOrder,
            GatewayRequestPayload::QueryOrder(request),
        )
        .await?
        .into_query_order()
    }

    async fn get_open_orders(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: OpenOrdersRequest,
    ) -> Result<OpenOrdersResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::GetOpenOrders,
            GatewayRequestPayload::GetOpenOrders(request),
        )
        .await?
        .into_open_orders()
    }

    async fn get_recent_fills(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: RecentFillsRequest,
    ) -> Result<RecentFillsResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::GetRecentFills,
            GatewayRequestPayload::GetRecentFills(request),
        )
        .await?
        .into_recent_fills()
    }

    async fn get_symbol_account_config(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: SymbolAccountConfigRequest,
    ) -> Result<SymbolAccountConfigResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::GetSymbolAccountConfig,
            GatewayRequestPayload::GetSymbolAccountConfig(request),
        )
        .await?
        .into_symbol_account_config()
    }

    async fn set_leverage(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: SetLeverageRequest,
    ) -> Result<SetLeverageResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::SetLeverage,
            GatewayRequestPayload::SetLeverage(request),
        )
        .await?
        .into_set_leverage()
    }

    async fn set_position_mode(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: SetPositionModeRequest,
    ) -> Result<SetPositionModeResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::SetPositionMode,
            GatewayRequestPayload::SetPositionMode(request),
        )
        .await?
        .into_set_position_mode()
    }

    async fn close_position(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: ClosePositionRequest,
    ) -> Result<ClosePositionResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::ClosePosition,
            GatewayRequestPayload::ClosePosition(request),
        )
        .await?
        .into_close_position()
    }

    async fn set_countdown_cancel_all(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: CountdownCancelAllRequest,
    ) -> Result<CountdownCancelAllResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::SetCountdownCancelAll,
            GatewayRequestPayload::SetCountdownCancelAll(request),
        )
        .await?
        .into_countdown_cancel_all()
    }

    async fn subscribe_books(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: SubscribeBooksRequest,
    ) -> Result<SubscribeBooksResponse, GatewayError> {
        self.send_request(
            request_id,
            tenant_id,
            account_id,
            GatewayOperation::SubscribeBooks,
            GatewayRequestPayload::SubscribeBooks(request),
        )
        .await?
        .into_books_subscribed()
    }

    async fn subscribe_private(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        request: SubscribePrivateRequest,
    ) -> Result<SubscribePrivateResponse, GatewayError> {
        let response = self
            .send(gateway_protocol_request(
                request_id,
                tenant_id,
                account_id,
                GatewayOperation::SubscribePrivate,
                GatewayRequestPayload::SubscribePrivate(request),
            ))
            .await?;
        response.into_private_subscribed()
    }

    async fn send_request(
        &self,
        request_id: String,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        operation: GatewayOperation,
        payload: GatewayRequestPayload,
    ) -> Result<GatewayProtocolResponse, GatewayError> {
        self.send(gateway_protocol_request(
            request_id, tenant_id, account_id, operation, payload,
        ))
        .await
    }
}

#[derive(Clone)]
pub struct InProcessGatewayClient<G> {
    gateway: Arc<G>,
}

impl<G> InProcessGatewayClient<G> {
    pub fn new(gateway: Arc<G>) -> Self {
        Self { gateway }
    }
}

#[async_trait]
impl<G> GatewayClient for InProcessGatewayClient<G>
where
    G: LocalGateway,
{
    async fn send(
        &self,
        request: GatewayProtocolRequest,
    ) -> Result<GatewayProtocolResponse, GatewayError> {
        self.gateway.handle_typed(request).await
    }
}

#[derive(Clone)]
pub struct GatewayExchangeClient {
    gateway: Arc<dyn GatewayClient>,
    tenant_id: TenantId,
    account_id: Option<AccountId>,
    exchange: ExchangeId,
    capabilities: ExchangeClientCapabilities,
}

impl GatewayExchangeClient {
    pub fn new<G>(
        gateway: Arc<G>,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        exchange: ExchangeId,
    ) -> Self
    where
        G: GatewayClient + 'static,
    {
        let capabilities = ExchangeClientCapabilities::new(exchange.clone());
        Self {
            gateway,
            tenant_id,
            account_id,
            exchange,
            capabilities,
        }
    }

    pub fn from_arc(
        gateway: Arc<dyn GatewayClient>,
        tenant_id: TenantId,
        account_id: Option<AccountId>,
        exchange: ExchangeId,
    ) -> Self {
        let capabilities = ExchangeClientCapabilities::new(exchange.clone());
        Self {
            gateway,
            tenant_id,
            account_id,
            exchange,
            capabilities,
        }
    }

    pub fn with_capabilities(mut self, capabilities: ExchangeClientCapabilities) -> Self {
        self.capabilities = capabilities;
        self
    }

    async fn send_exchange_request(
        &self,
        request_id: String,
        operation: GatewayOperation,
        payload: GatewayRequestPayload,
    ) -> ExchangeApiResult<GatewayProtocolResponse> {
        self.gateway
            .send(gateway_protocol_request(
                request_id,
                self.tenant_id.clone(),
                self.account_id.clone(),
                operation,
                payload,
            ))
            .await
            .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }
}

#[async_trait]
impl ApiExchangeClient for GatewayExchangeClient {
    fn exchange(&self) -> ExchangeId {
        self.exchange.clone()
    }

    fn capabilities(&self) -> ExchangeClientCapabilities {
        self.capabilities.clone()
    }

    async fn get_balances(&self, request: BalancesRequest) -> ExchangeApiResult<BalancesResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-get-balances");
        self.send_exchange_request(
            request_id,
            GatewayOperation::GetBalances,
            GatewayRequestPayload::GetBalances(request),
        )
        .await?
        .into_balances()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn get_positions(
        &self,
        request: PositionsRequest,
    ) -> ExchangeApiResult<PositionsResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-get-positions");
        self.send_exchange_request(
            request_id,
            GatewayOperation::GetPositions,
            GatewayRequestPayload::GetPositions(request),
        )
        .await?
        .into_positions()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn get_symbol_rules(
        &self,
        request: SymbolRulesRequest,
    ) -> ExchangeApiResult<SymbolRulesResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-get-symbol-rules");
        self.send_exchange_request(
            request_id,
            GatewayOperation::GetSymbolRules,
            GatewayRequestPayload::GetSymbolRules(request),
        )
        .await?
        .into_symbol_rules()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn get_order_book(
        &self,
        request: OrderBookRequest,
    ) -> ExchangeApiResult<OrderBookResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-get-order-book");
        self.send_exchange_request(
            request_id,
            GatewayOperation::GetOrderBook,
            GatewayRequestPayload::GetOrderBook(request),
        )
        .await?
        .into_order_book()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn get_fees(&self, request: FeesRequest) -> ExchangeApiResult<FeesResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-get-fees");
        self.send_exchange_request(
            request_id,
            GatewayOperation::GetFees,
            GatewayRequestPayload::GetFees(request),
        )
        .await?
        .into_fees()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn get_funding_rates(
        &self,
        request: FundingRatesRequest,
    ) -> ExchangeApiResult<FundingRatesResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-get-funding-rates");
        self.send_exchange_request(
            request_id,
            GatewayOperation::GetFundingRates,
            GatewayRequestPayload::GetFundingRates(request),
        )
        .await?
        .into_funding_rates()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn place_order(
        &self,
        request: PlaceOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-place-order");
        self.send_exchange_request(
            request_id,
            GatewayOperation::PlaceOrder,
            GatewayRequestPayload::PlaceOrder(request),
        )
        .await?
        .into_place_order()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn place_quote_market_order(
        &self,
        request: QuoteMarketOrderRequest,
    ) -> ExchangeApiResult<PlaceOrderResponse> {
        let request_id = request_id_or(
            &request.context.request_id,
            "gateway-place-quote-market-order",
        );
        self.send_exchange_request(
            request_id,
            GatewayOperation::PlaceQuoteMarketOrder,
            GatewayRequestPayload::PlaceQuoteMarketOrder(request),
        )
        .await?
        .into_place_order()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn cancel_order(
        &self,
        request: CancelOrderRequest,
    ) -> ExchangeApiResult<CancelOrderResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-cancel-order");
        self.send_exchange_request(
            request_id,
            GatewayOperation::CancelOrder,
            GatewayRequestPayload::CancelOrder(request),
        )
        .await?
        .into_cancel_order()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn amend_order(
        &self,
        request: AmendOrderRequest,
    ) -> ExchangeApiResult<AmendOrderResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-amend-order");
        self.send_exchange_request(
            request_id,
            GatewayOperation::AmendOrder,
            GatewayRequestPayload::AmendOrder(request),
        )
        .await?
        .into_amend_order()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn place_order_list(
        &self,
        request: OrderListRequest,
    ) -> ExchangeApiResult<OrderListResponse> {
        let request_id = request_id_or(&request.context_request_id(), "gateway-place-order-list");
        self.send_exchange_request(
            request_id,
            GatewayOperation::PlaceOrderList,
            GatewayRequestPayload::PlaceOrderList(request),
        )
        .await?
        .into_order_list()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn batch_place_orders(
        &self,
        request: BatchPlaceOrdersRequest,
    ) -> ExchangeApiResult<BatchPlaceOrdersResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-batch-place-orders");
        self.send_exchange_request(
            request_id,
            GatewayOperation::BatchPlaceOrders,
            GatewayRequestPayload::BatchPlaceOrders(request),
        )
        .await?
        .into_batch_place_orders()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn batch_cancel_orders(
        &self,
        request: BatchCancelOrdersRequest,
    ) -> ExchangeApiResult<BatchCancelOrdersResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-batch-cancel-orders");
        self.send_exchange_request(
            request_id,
            GatewayOperation::BatchCancelOrders,
            GatewayRequestPayload::BatchCancelOrders(request),
        )
        .await?
        .into_batch_cancel_orders()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn cancel_all_orders(
        &self,
        request: CancelAllOrdersRequest,
    ) -> ExchangeApiResult<CancelAllOrdersResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-cancel-all-orders");
        self.send_exchange_request(
            request_id,
            GatewayOperation::CancelAllOrders,
            GatewayRequestPayload::CancelAllOrders(request),
        )
        .await?
        .into_cancel_all_orders()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn query_order(
        &self,
        request: QueryOrderRequest,
    ) -> ExchangeApiResult<QueryOrderResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-query-order");
        self.send_exchange_request(
            request_id,
            GatewayOperation::QueryOrder,
            GatewayRequestPayload::QueryOrder(request),
        )
        .await?
        .into_query_order()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn get_open_orders(
        &self,
        request: OpenOrdersRequest,
    ) -> ExchangeApiResult<OpenOrdersResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-get-open-orders");
        self.send_exchange_request(
            request_id,
            GatewayOperation::GetOpenOrders,
            GatewayRequestPayload::GetOpenOrders(request),
        )
        .await?
        .into_open_orders()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn get_recent_fills(
        &self,
        request: RecentFillsRequest,
    ) -> ExchangeApiResult<RecentFillsResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-get-recent-fills");
        self.send_exchange_request(
            request_id,
            GatewayOperation::GetRecentFills,
            GatewayRequestPayload::GetRecentFills(request),
        )
        .await?
        .into_recent_fills()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn subscribe_public_stream(
        &self,
        subscription: PublicStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let request_id = request_id_or(
            &subscription.context.request_id,
            "gateway-subscribe-public-stream",
        );
        let response = self
            .send_exchange_request(
                request_id,
                GatewayOperation::SubscribeBooks,
                GatewayRequestPayload::SubscribeBooks(SubscribeBooksRequest {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    context: subscription.context.clone(),
                    subscriptions: vec![subscription],
                }),
            )
            .await?
            .into_books_subscribed()
            .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))?;
        response
            .subscriptions
            .first()
            .map(|subscription| subscription.subscription_id.clone())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "gateway returned no public subscription acknowledgement".to_string(),
            })
    }

    async fn subscribe_private_stream(
        &self,
        subscription: PrivateStreamSubscription,
    ) -> ExchangeApiResult<String> {
        let request_id = request_id_or(
            &subscription.context.request_id,
            "gateway-subscribe-private-stream",
        );
        let response = self
            .send_exchange_request(
                request_id,
                GatewayOperation::SubscribePrivate,
                GatewayRequestPayload::SubscribePrivate(SubscribePrivateRequest {
                    schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
                    context: subscription.context.clone(),
                    subscriptions: vec![subscription],
                }),
            )
            .await?
            .into_private_subscribed()
            .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))?;
        response
            .subscriptions
            .first()
            .map(|subscription| subscription.subscription_id.clone())
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "gateway returned no private subscription acknowledgement".to_string(),
            })
    }
}

#[async_trait]
impl PerpAccountControlProvider for GatewayExchangeClient {
    fn exchange(&self) -> ExchangeId {
        self.exchange.clone()
    }

    fn account_control_capabilities(&self) -> AccountControlCapabilities {
        AccountControlCapabilities::unsupported(self.exchange.clone())
    }

    async fn get_symbol_account_config(
        &self,
        request: SymbolAccountConfigRequest,
    ) -> ExchangeApiResult<SymbolAccountConfigResponse> {
        let request_id = request_id_or(
            &request.context.request_id,
            "gateway-get-symbol-account-config",
        );
        self.send_exchange_request(
            request_id,
            GatewayOperation::GetSymbolAccountConfig,
            GatewayRequestPayload::GetSymbolAccountConfig(request),
        )
        .await?
        .into_symbol_account_config()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn set_leverage(
        &self,
        request: SetLeverageRequest,
    ) -> ExchangeApiResult<SetLeverageResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-set-leverage");
        self.send_exchange_request(
            request_id,
            GatewayOperation::SetLeverage,
            GatewayRequestPayload::SetLeverage(request),
        )
        .await?
        .into_set_leverage()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn set_position_mode(
        &self,
        request: SetPositionModeRequest,
    ) -> ExchangeApiResult<SetPositionModeResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-set-position-mode");
        self.send_exchange_request(
            request_id,
            GatewayOperation::SetPositionMode,
            GatewayRequestPayload::SetPositionMode(request),
        )
        .await?
        .into_set_position_mode()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn close_position(
        &self,
        request: ClosePositionRequest,
    ) -> ExchangeApiResult<ClosePositionResponse> {
        let request_id = request_id_or(&request.context.request_id, "gateway-close-position");
        self.send_exchange_request(
            request_id,
            GatewayOperation::ClosePosition,
            GatewayRequestPayload::ClosePosition(request),
        )
        .await?
        .into_close_position()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }

    async fn set_countdown_cancel_all(
        &self,
        request: CountdownCancelAllRequest,
    ) -> ExchangeApiResult<CountdownCancelAllResponse> {
        let request_id = request_id_or(
            &request.context.request_id,
            "gateway-set-countdown-cancel-all",
        );
        self.send_exchange_request(
            request_id,
            GatewayOperation::SetCountdownCancelAll,
            GatewayRequestPayload::SetCountdownCancelAll(request),
        )
        .await?
        .into_countdown_cancel_all()
        .map_err(|error| gateway_error_to_exchange_api(&self.exchange, error))
    }
}

fn request_id_or(request_id: &Option<String>, fallback: &'static str) -> String {
    request_id.clone().unwrap_or_else(|| fallback.to_string())
}

fn gateway_error_to_exchange_api(exchange: &ExchangeId, error: GatewayError) -> ExchangeApiError {
    match error {
        GatewayError::UnsupportedOperation { operation } => ExchangeApiError::Unsupported {
            operation: operation_static_name(&operation),
        },
        GatewayError::Rejected(message) | GatewayError::InvalidPayload { message } => {
            ExchangeApiError::InvalidRequest { message }
        }
        GatewayError::MissingCredentials {
            exchange: gateway_exchange,
        } => ExchangeApiError::Exchange(exchange_error(
            ExchangeId::new(gateway_exchange).unwrap_or_else(|_| exchange.clone()),
            rustcta_exchange_api::ExchangeErrorKind::Authentication,
            "gateway credentials are not configured",
            None,
        )),
        GatewayError::SecretPayloadRejected { direction } => ExchangeApiError::InvalidRequest {
            message: format!("gateway rejected {direction} containing secret-like fields"),
        },
        GatewayError::Exchange {
            exchange: gateway_exchange,
            kind,
            message,
            code,
            retry_after_ms,
        } => ExchangeApiError::Exchange(
            exchange_error(
                ExchangeId::new(gateway_exchange).unwrap_or_else(|_| exchange.clone()),
                kind,
                message,
                code,
            )
            .with_retry_after(retry_after_ms),
        ),
    }
}

fn operation_static_name(operation: &str) -> &'static str {
    match operation {
        "place_quote_market_order" | "mock.place_quote_market_order" => "place_quote_market_order",
        "amend_order" | "mock.amend_order" => "amend_order",
        "place_order_list" | "mock.place_order_list" => "place_order_list",
        "get_symbol_account_config" | "mock.get_symbol_account_config" => {
            "get_symbol_account_config"
        }
        "set_leverage" | "mock.set_leverage" => "set_leverage",
        "set_position_mode" | "mock.set_position_mode" => "set_position_mode",
        "close_position" | "mock.close_position" => "close_position",
        "set_countdown_cancel_all" | "mock.set_countdown_cancel_all" => "set_countdown_cancel_all",
        _ => "gateway_unsupported_operation",
    }
}

fn exchange_error(
    exchange: ExchangeId,
    kind: rustcta_exchange_api::ExchangeErrorKind,
    message: impl Into<String>,
    code: Option<String>,
) -> ExchangeError {
    let mut error = ExchangeError::new(exchange, kind.into(), message, chrono::Utc::now());
    error.code = code;
    error
}

trait ExchangeErrorRetryExt {
    fn with_retry_after(self, retry_after_ms: Option<u64>) -> Self;
}

impl ExchangeErrorRetryExt for ExchangeError {
    fn with_retry_after(mut self, retry_after_ms: Option<u64>) -> Self {
        self.retry_after_ms = retry_after_ms;
        self
    }
}
