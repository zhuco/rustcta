#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

use super::transport::signed_json_request_spec;

pub const BALANCES_PATH: &str = "/api/v1/users/balances";
pub const PLACE_ORDER_PATH: &str = "/api/v1/order";
pub const CANCEL_ORDER_PATH: &str = "/api/v1/order";
pub const OPEN_ORDERS_PATH: &str = "/api/v1/openOrders";
pub const QUERY_ORDER_PATH: &str = "/api/v1/order/{orderId}";
pub const RECENT_FILLS_PATH: &str = "/api/v1/users/transactions/trade";

pub fn place_order_request_spec_fixture() -> Value {
    signed_json_request_spec(
        "POST",
        PLACE_ORDER_PATH,
        json!({
            "quantity": "0.01",
            "price": "2500000",
            "stopPrice": "0",
            "newOrderClientId": "offline-fixture",
            "orderMethod": "limit",
            "orderType": "buy",
            "pairSymbol": "BTCTRY"
        }),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    signed_json_request_spec(
        "DELETE",
        CANCEL_ORDER_PATH,
        json!({
            "id": "123456789",
            "pairSymbol": "BTCTRY"
        }),
    )
}
