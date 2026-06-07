use serde::{Deserialize, Serialize};

use crate::{
    BalancesRequest, BalancesResponse, BatchCancelOrdersRequest, BatchCancelOrdersResponse,
    BatchPlaceOrdersRequest, BatchPlaceOrdersResponse, CancelAllOrdersRequest,
    CancelAllOrdersResponse, CancelOrderRequest, CancelOrderResponse, ExchangeError, ExchangeId,
    ExchangeStreamEvent, FeesRequest, FeesResponse, OpenOrdersRequest, OpenOrdersResponse,
    OrderBookRequest, OrderBookResponse, PlaceOrderRequest, PlaceOrderResponse, PositionsRequest,
    PositionsResponse, PrivateStreamSubscription, PublicStreamSubscription, QueryOrderRequest,
    QueryOrderResponse, RecentFillsRequest, RecentFillsResponse, ResponseMetadata,
    SymbolRulesRequest, SymbolRulesResponse,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GatewayRequest {
    GetBalances(BalancesRequest),
    GetPositions(PositionsRequest),
    GetSymbolRules(SymbolRulesRequest),
    GetOrderBook(OrderBookRequest),
    GetFees(FeesRequest),
    PlaceOrder(PlaceOrderRequest),
    CancelOrder(CancelOrderRequest),
    BatchPlaceOrders(BatchPlaceOrdersRequest),
    BatchCancelOrders(BatchCancelOrdersRequest),
    CancelAllOrders(CancelAllOrdersRequest),
    QueryOrder(QueryOrderRequest),
    GetOpenOrders(OpenOrdersRequest),
    GetRecentFills(RecentFillsRequest),
    SubscribePublic(PublicStreamSubscription),
    SubscribePrivate(PrivateStreamSubscription),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GatewayResponse {
    Balances(BalancesResponse),
    Positions(PositionsResponse),
    SymbolRules(SymbolRulesResponse),
    OrderBook(OrderBookResponse),
    Fees(FeesResponse),
    PlaceOrder(PlaceOrderResponse),
    CancelOrder(CancelOrderResponse),
    BatchPlaceOrders(BatchPlaceOrdersResponse),
    BatchCancelOrders(BatchCancelOrdersResponse),
    CancelAllOrders(CancelAllOrdersResponse),
    QueryOrder(QueryOrderResponse),
    OpenOrders(OpenOrdersResponse),
    RecentFills(RecentFillsResponse),
    Subscribed {
        schema_version: u16,
        metadata: ResponseMetadata,
        subscription_id: String,
    },
    StreamEvent(ExchangeStreamEvent),
    Error {
        schema_version: u16,
        metadata: ResponseMetadata,
        error: ExchangeApiErrorEnvelope,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExchangeApiErrorEnvelope {
    pub schema_version: u16,
    pub exchange: ExchangeId,
    pub message: String,
    pub exchange_error: Option<ExchangeError>,
    pub retryable: bool,
    pub requires_reconciliation: bool,
}
