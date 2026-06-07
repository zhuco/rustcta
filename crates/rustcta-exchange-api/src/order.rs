use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    CanonicalSymbol, ExchangeId, ExchangeSymbol, Fill, MarketType, OrderSide, OrderStatus,
    OrderType, PositionSide, RequestContext, ResponseMetadata, SymbolScope, TimeInForce,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceOrderRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub position_side: Option<PositionSide>,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub quantity: String,
    pub price: Option<String>,
    pub quote_quantity: Option<String>,
    pub reduce_only: bool,
    pub post_only: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceOrderResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub order: OrderState,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuoteMarketOrderRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub client_order_id: Option<String>,
    pub side: OrderSide,
    pub quote_quantity: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AmendOrderRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub new_client_order_id: Option<String>,
    pub new_quantity: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AmendOrderResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub order: OrderState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderListKind {
    Oco,
    Oto,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderListLegType {
    Market,
    Limit,
    LimitMaker,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderListConditionalLeg {
    pub order_type: OrderListLegType,
    pub price: Option<String>,
    pub stop_price: Option<String>,
    pub time_in_force: Option<TimeInForce>,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderListOrderLeg {
    pub side: OrderSide,
    pub order_type: OrderListLegType,
    pub quantity: String,
    pub price: Option<String>,
    pub stop_price: Option<String>,
    pub time_in_force: Option<TimeInForce>,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderListRequest {
    Oco {
        schema_version: u16,
        context: RequestContext,
        symbol: SymbolScope,
        list_client_order_id: Option<String>,
        side: OrderSide,
        quantity: String,
        above: OrderListConditionalLeg,
        below: OrderListConditionalLeg,
    },
    Oto {
        schema_version: u16,
        context: RequestContext,
        symbol: SymbolScope,
        list_client_order_id: Option<String>,
        working: OrderListOrderLeg,
        pending: OrderListOrderLeg,
    },
}

impl OrderListRequest {
    pub fn symbol(&self) -> &SymbolScope {
        match self {
            Self::Oco { symbol, .. } | Self::Oto { symbol, .. } => symbol,
        }
    }

    pub fn kind(&self) -> OrderListKind {
        match self {
            Self::Oco { .. } => OrderListKind::Oco,
            Self::Oto { .. } => OrderListKind::Oto,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderListResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub symbol: SymbolScope,
    pub kind: OrderListKind,
    pub order_list_id: Option<String>,
    pub list_client_order_id: Option<String>,
    pub list_status_type: Option<String>,
    pub list_order_status: Option<String>,
    pub orders: Vec<OrderState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelOrderRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelOrderResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub order: OrderState,
    pub cancelled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchPlaceOrdersRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub orders: Vec<PlaceOrderRequest>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchPlaceOrdersResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub orders: Vec<OrderState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchCancelOrdersRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub cancels: Vec<CancelOrderRequest>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BatchCancelOrdersResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub orders: Vec<OrderState>,
    pub cancelled_count: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAllOrdersRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub symbol: Option<SymbolScope>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelAllOrdersResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub orders: Vec<OrderState>,
    pub cancelled_count: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryOrderRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub symbol: SymbolScope,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueryOrderResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub order: Option<OrderState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrdersRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub symbol: Option<SymbolScope>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenOrdersResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub orders: Vec<OrderState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecentFillsRequest {
    pub schema_version: u16,
    pub context: RequestContext,
    pub exchange: ExchangeId,
    pub market_type: Option<MarketType>,
    pub symbol: Option<SymbolScope>,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub from_trade_id: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecentFillsResponse {
    pub schema_version: u16,
    pub metadata: ResponseMetadata,
    pub fills: Vec<Fill>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderState {
    pub schema_version: u16,
    pub exchange: ExchangeId,
    pub market_type: MarketType,
    pub canonical_symbol: Option<CanonicalSymbol>,
    pub exchange_symbol: ExchangeSymbol,
    pub client_order_id: Option<String>,
    pub exchange_order_id: Option<String>,
    pub side: OrderSide,
    pub position_side: Option<PositionSide>,
    pub order_type: OrderType,
    pub time_in_force: Option<TimeInForce>,
    pub status: OrderStatus,
    pub quantity: String,
    pub price: Option<String>,
    pub filled_quantity: String,
    pub average_fill_price: Option<String>,
    pub reduce_only: bool,
    pub post_only: bool,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}
