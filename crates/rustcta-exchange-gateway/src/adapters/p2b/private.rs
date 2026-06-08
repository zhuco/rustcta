use rustcta_exchange_api::{CancelOrderRequest, ExchangeApiResult, PlaceOrderRequest};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::signing::{build_private_headers, P2bPrivateHeaders};

pub const P2B_BALANCE_PATH: &str = "/api/v2/account/balances";
pub const P2B_PLACE_ORDER_PATH: &str = "/api/v2/order/new";
pub const P2B_CANCEL_ORDER_PATH: &str = "/api/v2/order/cancel";
pub const P2B_OPEN_ORDERS_PATH: &str = "/api/v2/orders";
pub const P2B_RECENT_FILLS_PATH: &str = "/api/v2/account/market_deals";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct P2bPrivateRequestSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub headers: P2bPrivateHeaders,
    pub body: Value,
}

pub fn build_private_request_spec(
    path: &'static str,
    mut body: Value,
    nonce: u64,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<P2bPrivateRequestSpec> {
    body["request"] = Value::String(path.to_string());
    body["nonce"] = Value::String(nonce.to_string());
    let headers = build_private_headers(api_key, api_secret, &body)?;
    Ok(P2bPrivateRequestSpec {
        method: "POST",
        path,
        headers,
        body,
    })
}

pub fn request_spec_body_from_order(request: &PlaceOrderRequest) -> Value {
    json!({
        "market": request.symbol.exchange_symbol.symbol,
        "side": order_side_as_str(request.side),
        "amount": request.quantity,
        "price": request.price.clone().unwrap_or_default()
    })
}

pub fn request_spec_body_from_cancel(request: &CancelOrderRequest) -> Value {
    json!({
        "market": request.symbol.exchange_symbol.symbol,
        "orderId": request.exchange_order_id.as_deref().unwrap_or_default()
    })
}

fn order_side_as_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

pub fn p2b_supports_order_type(order_type: OrderType) -> bool {
    match order_type {
        OrderType::Limit => true,
        OrderType::Market
        | OrderType::PostOnly
        | OrderType::IOC
        | OrderType::FOK
        | OrderType::StopMarket
        | OrderType::StopLimit => false,
    }
}
