#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

use super::transport::signed_json_request_spec;

pub const BALANCES_PATH: &str = "/account/balances";
pub const POSITIONS_PATH: &str = "/account/positions";
pub const FEES_PATH: &str = "/account/fees";
pub const PLACE_ORDER_PATH: &str = "/orders/new";
pub const CANCEL_ORDER_PATH: &str = "/orders/cancel";
pub const CANCEL_ALL_ORDERS_PATH: &str = "/orders/cancel/all";
pub const OPEN_ORDERS_PATH: &str = "/orders";
pub const QUERY_ORDER_PATH: &str = "/orders/{id}";
pub const RECENT_FILLS_PATH: &str = "/trades";

pub fn balances_request_spec_fixture() -> Value {
    signed_json_request_spec("GET", BALANCES_PATH, Value::String(String::new()))
}

pub fn positions_request_spec_fixture() -> Value {
    signed_json_request_spec("GET", POSITIONS_PATH, Value::String(String::new()))
}

pub fn place_order_request_spec_fixture() -> Value {
    signed_json_request_spec(
        "POST",
        PLACE_ORDER_PATH,
        json!({
            "symbol": "BTC_USDT",
            "side": "buy",
            "type": "limitGtc",
            "size": "0.01",
            "price": "64000",
            "clientOrderId": "<redacted:order_id>"
        }),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    signed_json_request_spec(
        "POST",
        CANCEL_ORDER_PATH,
        json!({
            "orderId": 3694872060678_u64
        }),
    )
}

pub fn cancel_all_orders_request_spec_fixture() -> Value {
    signed_json_request_spec("POST", CANCEL_ALL_ORDERS_PATH, json!({}))
}
