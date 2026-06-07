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
