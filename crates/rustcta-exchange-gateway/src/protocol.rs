use chrono::{DateTime, Utc};
use rustcta_exchange_api::{
    AmendOrderRequest, AmendOrderResponse, BalancesRequest, BalancesResponse,
    BatchCancelOrdersRequest, BatchCancelOrdersResponse, BatchPlaceOrdersRequest,
    BatchPlaceOrdersResponse, CancelAllOrdersRequest, CancelAllOrdersResponse, CancelOrderRequest,
    CancelOrderResponse, ExchangeClientCapabilities, FeesRequest, FeesResponse, OpenOrdersRequest,
    OpenOrdersResponse, OrderBookRequest, OrderBookResponse, OrderListRequest, OrderListResponse,
    PlaceOrderRequest, PlaceOrderResponse, PositionsRequest, PositionsResponse,
    PrivateStreamCapabilities, PrivateStreamKind, PrivateStreamSubscription, PublicStreamKind,
    PublicStreamSubscription, QueryOrderRequest, QueryOrderResponse, QuoteMarketOrderRequest,
    RecentFillsRequest, RecentFillsResponse, RequestContext, SymbolRulesRequest,
    SymbolRulesResponse,
};
use rustcta_types::{AccountId, CanonicalSymbol, ExchangeId, ExchangeSymbol, MarketType, TenantId};
use serde::{Deserialize, Serialize};

use crate::{GatewayStatus, GATEWAY_PROTOCOL_SCHEMA_VERSION};

mod legacy;
mod operation;
mod validation;

pub(crate) use legacy::legacy_request_to_typed;
pub use legacy::{GatewayRequest, GatewayResponse};
pub use operation::GatewayOperation;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetStatusRequest {
    pub schema_version: u16,
    pub include_exchanges: bool,
}

impl Default for GetStatusRequest {
    fn default() -> Self {
        Self {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            include_exchanges: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetStatusResponse {
    pub schema_version: u16,
    pub status: GatewayStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetCapabilitiesRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchanges: Vec<ExchangeId>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetCapabilitiesResponse {
    pub schema_version: u16,
    pub capabilities: Vec<ExchangeClientCapabilities>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribeBooksRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub subscriptions: Vec<PublicStreamSubscription>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BookSubscriptionAck {
    pub schema_version: u16,
    pub subscription_id: String,
    pub exchange: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange_symbol: ExchangeSymbol,
    pub kind: PublicStreamKind,
    pub subscribed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribeBooksResponse {
    pub schema_version: u16,
    pub subscriptions: Vec<BookSubscriptionAck>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribePrivateRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub subscriptions: Vec<PrivateStreamSubscription>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrivateSubscriptionAck {
    pub schema_version: u16,
    pub subscription_id: String,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub account_id: AccountId,
    pub kind: PrivateStreamKind,
    #[serde(default)]
    pub capabilities: Option<PrivateStreamCapabilities>,
    pub subscribed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribePrivateResponse {
    pub schema_version: u16,
    pub subscriptions: Vec<PrivateSubscriptionAck>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "payload_type", content = "payload", rename_all = "snake_case")]
pub enum GatewayRequestPayload {
    GetStatus(GetStatusRequest),
    GetCapabilities(GetCapabilitiesRequest),
    GetBalances(BalancesRequest),
    GetPositions(PositionsRequest),
    GetSymbolRules(SymbolRulesRequest),
    GetOrderBook(OrderBookRequest),
    GetFees(FeesRequest),
    PlaceOrder(PlaceOrderRequest),
    PlaceQuoteMarketOrder(QuoteMarketOrderRequest),
    CancelOrder(CancelOrderRequest),
    AmendOrder(AmendOrderRequest),
    PlaceOrderList(OrderListRequest),
    BatchPlaceOrders(BatchPlaceOrdersRequest),
    BatchCancelOrders(BatchCancelOrdersRequest),
    CancelAllOrders(CancelAllOrdersRequest),
    QueryOrder(QueryOrderRequest),
    GetOpenOrders(OpenOrdersRequest),
    GetRecentFills(RecentFillsRequest),
    SubscribeBooks(SubscribeBooksRequest),
    SubscribePrivate(SubscribePrivateRequest),
}

impl GatewayRequestPayload {
    pub fn operation(&self) -> GatewayOperation {
        match self {
            Self::GetStatus(_) => GatewayOperation::GetStatus,
            Self::GetCapabilities(_) => GatewayOperation::GetCapabilities,
            Self::GetBalances(_) => GatewayOperation::GetBalances,
            Self::GetPositions(_) => GatewayOperation::GetPositions,
            Self::GetSymbolRules(_) => GatewayOperation::GetSymbolRules,
            Self::GetOrderBook(_) => GatewayOperation::GetOrderBook,
            Self::GetFees(_) => GatewayOperation::GetFees,
            Self::PlaceOrder(_) => GatewayOperation::PlaceOrder,
            Self::PlaceQuoteMarketOrder(_) => GatewayOperation::PlaceQuoteMarketOrder,
            Self::CancelOrder(_) => GatewayOperation::CancelOrder,
            Self::AmendOrder(_) => GatewayOperation::AmendOrder,
            Self::PlaceOrderList(_) => GatewayOperation::PlaceOrderList,
            Self::BatchPlaceOrders(_) => GatewayOperation::BatchPlaceOrders,
            Self::BatchCancelOrders(_) => GatewayOperation::BatchCancelOrders,
            Self::CancelAllOrders(_) => GatewayOperation::CancelAllOrders,
            Self::QueryOrder(_) => GatewayOperation::QueryOrder,
            Self::GetOpenOrders(_) => GatewayOperation::GetOpenOrders,
            Self::GetRecentFills(_) => GatewayOperation::GetRecentFills,
            Self::SubscribeBooks(_) => GatewayOperation::SubscribeBooks,
            Self::SubscribePrivate(_) => GatewayOperation::SubscribePrivate,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "payload_type", content = "payload", rename_all = "snake_case")]
pub enum GatewayResponsePayload {
    Status(GetStatusResponse),
    Capabilities(GetCapabilitiesResponse),
    Balances(BalancesResponse),
    Positions(PositionsResponse),
    SymbolRules(SymbolRulesResponse),
    OrderBook(OrderBookResponse),
    Fees(FeesResponse),
    PlaceOrder(PlaceOrderResponse),
    AmendOrder(AmendOrderResponse),
    OrderList(OrderListResponse),
    CancelOrder(CancelOrderResponse),
    BatchPlaceOrders(BatchPlaceOrdersResponse),
    BatchCancelOrders(BatchCancelOrdersResponse),
    CancelAllOrders(CancelAllOrdersResponse),
    QueryOrder(QueryOrderResponse),
    OpenOrders(OpenOrdersResponse),
    RecentFills(RecentFillsResponse),
    BooksSubscribed(SubscribeBooksResponse),
    PrivateSubscribed(SubscribePrivateResponse),
    Empty,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayErrorPayload {
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayProtocolRequest {
    pub schema_version: u16,
    pub request_id: String,
    pub tenant_id: TenantId,
    pub account_id: Option<AccountId>,
    pub operation: GatewayOperation,
    pub payload: GatewayRequestPayload,
    pub requested_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GatewayProtocolResponse {
    pub schema_version: u16,
    pub request_id: String,
    pub operation: GatewayOperation,
    pub accepted: bool,
    pub payload: GatewayResponsePayload,
    pub error: Option<GatewayErrorPayload>,
    pub responded_at: DateTime<Utc>,
}

impl GatewayProtocolResponse {
    pub fn accepted(
        request_id: impl Into<String>,
        operation: GatewayOperation,
        payload: GatewayResponsePayload,
    ) -> Self {
        Self {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: request_id.into(),
            operation,
            accepted: true,
            payload,
            error: None,
            responded_at: Utc::now(),
        }
    }

    pub fn rejected(
        request_id: impl Into<String>,
        operation: GatewayOperation,
        error: impl Into<String>,
    ) -> Self {
        Self {
            schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
            request_id: request_id.into(),
            operation,
            accepted: false,
            payload: GatewayResponsePayload::Empty,
            error: Some(GatewayErrorPayload {
                message: error.into(),
                retryable: false,
            }),
            responded_at: Utc::now(),
        }
    }
}
