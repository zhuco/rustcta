use std::sync::Arc;

use async_trait::async_trait;
use rustcta_exchange_api::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, FeesRequest, FeesResponse,
    OpenOrdersRequest, OpenOrdersResponse, OrderBookRequest, OrderBookResponse, PlaceOrderRequest,
    PlaceOrderResponse, PositionsRequest, PositionsResponse, QueryOrderRequest, QueryOrderResponse,
    RecentFillsRequest, RecentFillsResponse, SymbolRulesRequest, SymbolRulesResponse,
};
use rustcta_types::{AccountId, TenantId};

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
