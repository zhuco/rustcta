use std::str::FromStr;
use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use rustcta_exchange_api::{
    BalancesRequest, BatchCancelOrdersRequest, BatchPlaceOrdersRequest, CancelAllOrdersRequest,
    CancelOrderRequest, FeesRequest, OpenOrdersRequest, OrderBookRequest, OrderSide, OrderType,
    PlaceOrderRequest, PositionsRequest, PrivateStreamKind, PrivateStreamSubscription,
    PublicStreamKind, PublicStreamSubscription, QueryOrderRequest, RecentFillsRequest,
    RequestContext, SymbolScope, TimeInForce, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::{
    AccountId, AssetBalance, CanonicalSymbol, ExchangeBalance, ExchangeId, ExchangeSymbol,
    MarketType, OrderBookLevel, OrderBookSnapshot, OrderStatus, SchemaVersion, TenantId,
};
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::*;

fn tenant_id() -> TenantId {
    TenantId::new("tenant").expect("tenant id")
}

fn account_id() -> AccountId {
    AccountId::new("account").expect("account id")
}

fn exchange_id() -> ExchangeId {
    ExchangeId::new("binance").expect("exchange id")
}

fn canonical_symbol() -> CanonicalSymbol {
    CanonicalSymbol::new("BTC", "USDT").expect("canonical symbol")
}

fn exchange_symbol() -> ExchangeSymbol {
    ExchangeSymbol::new(exchange_id(), MarketType::Spot, "BTCUSDT").expect("exchange symbol")
}

fn context(request_id: &str) -> RequestContext {
    RequestContext {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        tenant_id: Some(tenant_id()),
        account_id: Some(account_id()),
        run_id: None,
        request_id: Some(request_id.to_string()),
        requested_at: Utc::now(),
    }
}

fn symbol_scope() -> SymbolScope {
    SymbolScope {
        exchange: exchange_id(),
        market_type: MarketType::Spot,
        canonical_symbol: Some(canonical_symbol()),
        exchange_symbol: exchange_symbol(),
    }
}

fn place_order_request(request_id: &str) -> GatewayProtocolRequest {
    GatewayProtocolRequest {
        schema_version: GATEWAY_PROTOCOL_SCHEMA_VERSION,
        request_id: request_id.to_string(),
        tenant_id: tenant_id(),
        account_id: Some(account_id()),
        operation: GatewayOperation::PlaceOrder,
        payload: GatewayRequestPayload::PlaceOrder(PlaceOrderRequest {
            schema_version: EXCHANGE_API_SCHEMA_VERSION,
            context: context(request_id),
            symbol: symbol_scope(),
            client_order_id: Some("cli-1".to_string()),
            side: OrderSide::Buy,
            position_side: None,
            order_type: OrderType::Limit,
            time_in_force: Some(TimeInForce::GTC),
            quantity: "0.01000000".to_string(),
            price: Some("50000.00".to_string()),
            quote_quantity: None,
            reduce_only: false,
            post_only: true,
        }),
        requested_at: Utc::now(),
    }
}

mod batch;
mod client;
mod http;
mod mock_gateway;
mod reconciliation;
mod security;
mod status;

#[test]
fn gateway_operation_should_parse_advanced_spot_order_operations() {
    assert_eq!(
        GatewayOperation::from_str("place_quote_market_order").unwrap(),
        GatewayOperation::PlaceQuoteMarketOrder
    );
    assert_eq!(
        GatewayOperation::from_str("modify_order").unwrap(),
        GatewayOperation::AmendOrder
    );
    assert_eq!(
        GatewayOperation::from_str("order_list").unwrap(),
        GatewayOperation::PlaceOrderList
    );
    assert_eq!(
        GatewayOperation::PlaceQuoteMarketOrder.as_str(),
        "place_quote_market_order"
    );
}
