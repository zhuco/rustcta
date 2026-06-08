use rustcta_exchange_api::{CancelOrderRequest, ExchangeApiResult, PlaceOrderRequest};
use rustcta_types::OrderSide;

use super::signing::{build_private_headers, form_encode, YobitPrivateHeaders};

pub const YOBIT_TAPI_PATH: &str = "/tapi/";
pub const YOBIT_BALANCE_METHOD: &str = "getInfo";
pub const YOBIT_PLACE_ORDER_METHOD: &str = "Trade";
pub const YOBIT_CANCEL_ORDER_METHOD: &str = "CancelOrder";
pub const YOBIT_QUERY_ORDER_METHOD: &str = "OrderInfo";
pub const YOBIT_OPEN_ORDERS_METHOD: &str = "ActiveOrders";
pub const YOBIT_RECENT_FILLS_METHOD: &str = "TradeHistory";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct YobitPrivateRequestSpec {
    pub method: &'static str,
    pub path: &'static str,
    pub headers: YobitPrivateHeaders,
    pub body: String,
}

pub fn build_private_request_spec(
    params: &[(&str, String)],
    nonce: u64,
    api_key: &str,
    api_secret: &str,
) -> ExchangeApiResult<YobitPrivateRequestSpec> {
    let mut body_params = params.to_vec();
    body_params.push(("nonce", nonce.to_string()));
    let body = form_encode(&body_params);
    let headers = build_private_headers(api_key, api_secret, &body)?;
    Ok(YobitPrivateRequestSpec {
        method: "POST",
        path: YOBIT_TAPI_PATH,
        headers,
        body,
    })
}

pub fn request_spec_params_from_order(request: &PlaceOrderRequest) -> Vec<(&'static str, String)> {
    vec![
        ("method", YOBIT_PLACE_ORDER_METHOD.to_string()),
        ("pair", request.symbol.exchange_symbol.symbol.clone()),
        ("type", order_side_as_str(request.side).to_string()),
        ("rate", request.price.clone().unwrap_or_default()),
        ("amount", request.quantity.clone()),
    ]
}

pub fn request_spec_params_from_cancel(
    request: &CancelOrderRequest,
) -> Vec<(&'static str, String)> {
    vec![
        ("method", YOBIT_CANCEL_ORDER_METHOD.to_string()),
        (
            "order_id",
            request.exchange_order_id.clone().unwrap_or_default(),
        ),
    ]
}

fn order_side_as_str(side: OrderSide) -> &'static str {
    match side {
        OrderSide::Buy => "buy",
        OrderSide::Sell => "sell",
    }
}

pub fn balance_params() -> Vec<(&'static str, String)> {
    vec![("method", YOBIT_BALANCE_METHOD.to_string())]
}

pub fn open_orders_params(pair: &str) -> Vec<(&'static str, String)> {
    vec![
        ("method", YOBIT_OPEN_ORDERS_METHOD.to_string()),
        ("pair", pair.to_string()),
    ]
}

pub fn recent_fills_params(pair: &str) -> Vec<(&'static str, String)> {
    vec![
        ("method", YOBIT_RECENT_FILLS_METHOD.to_string()),
        ("pair", pair.to_string()),
        ("count", "100".to_string()),
    ]
}
