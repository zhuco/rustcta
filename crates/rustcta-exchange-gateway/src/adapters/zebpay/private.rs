use rustcta_exchange_api::{ExchangeApiResult, PlaceOrderRequest};
use rustcta_types::{OrderSide, OrderType};
use serde_json::{json, Value};

use super::signing::{build_bearer_private_headers, ZebpayPrivateHeaders};

pub const ZEBPAY_BALANCE_PATH: &str = "/wallet/balance";
pub const ZEBPAY_PLACE_ORDER_PATH: &str = "/orders";
pub const ZEBPAY_CANCEL_ORDER_PATH: &str = "/orders/{order_id}";
pub const ZEBPAY_CANCEL_ALL_ORDERS_PATH: &str = "/orders/CancelAll";
pub const ZEBPAY_OPEN_ORDERS_PATH: &str = "/orders";
pub const ZEBPAY_RECENT_FILLS_PATH: &str = "/orders/{order_id}/fills";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZebpayPrivateRequestSpec {
    pub method: &'static str,
    pub path: String,
    pub headers: ZebpayPrivateHeaders,
    pub body: Option<Value>,
}

pub fn build_private_request_spec(
    method: &'static str,
    path: impl Into<String>,
    body: Option<Value>,
    client_id: &str,
    access_token: &str,
    timestamp: &str,
    request_id: &str,
) -> ExchangeApiResult<ZebpayPrivateRequestSpec> {
    Ok(ZebpayPrivateRequestSpec {
        method,
        path: path.into(),
        headers: build_bearer_private_headers(client_id, access_token, timestamp, request_id)?,
        body,
    })
}

pub fn request_spec_body_from_order(request: &PlaceOrderRequest) -> Value {
    json!({
        "trade_pair": request.symbol.exchange_symbol.symbol,
        "side": order_side_as_str(request.side),
        "size": request.quantity,
        "price": request.price.clone().unwrap_or_default(),
        "tradeType": 1,
        "platform": "API_Trading"
    })
}

fn order_side_as_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "bid",
        OrderSide::Sell => "ask",
    }
}

pub fn zebpay_supports_order_type(order_type: OrderType) -> bool {
    matches!(order_type, OrderType::Limit)
}
