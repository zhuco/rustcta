#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

use super::transport::api_token_json_request_spec;

pub const ACCOUNTS_PATH: &str = "/accounts";
pub const FEES_PATH: &str = "/fees";
pub const ORDERS_PATH: &str = "/orders";
pub const TRADES_PATH: &str = "/trades";
pub const FILLS_PATH: &str = "/fills";
pub const INTERNAL_ORDERS_PATH: &str = "/internal/orders";

pub fn balances_request_spec_fixture() -> Value {
    api_token_json_request_spec("GET", ACCOUNTS_PATH, json!({}), None)
}

pub fn fees_request_spec_fixture() -> Value {
    api_token_json_request_spec("GET", FEES_PATH, json!({}), None)
}

pub fn open_orders_request_spec_fixture() -> Value {
    api_token_json_request_spec(
        "GET",
        ORDERS_PATH,
        json!({
            "symbol": "BTC-USD",
            "status": "OPEN"
        }),
        None,
    )
}

pub fn query_order_request_spec_fixture() -> Value {
    api_token_json_request_spec("GET", "/orders/{orderId}", json!({}), None)
}

pub fn recent_fills_request_spec_fixture() -> Value {
    api_token_json_request_spec(
        "GET",
        FILLS_PATH,
        json!({
            "symbol": "BTC-USD",
            "limit": "100"
        }),
        None,
    )
}

pub fn place_order_request_spec_fixture() -> Value {
    api_token_json_request_spec(
        "POST",
        ORDERS_PATH,
        json!({}),
        Some(json!({
            "clOrdId": "rustcta-fixture-0001",
            "symbol": "BTC-USD",
            "side": "BUY",
            "ordType": "LIMIT",
            "timeInForce": "GTC",
            "orderQty": "0.01",
            "price": "65000"
        })),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    api_token_json_request_spec("DELETE", "/orders/{orderId}", json!({}), None)
}

pub fn cancel_all_orders_request_spec_fixture() -> Value {
    api_token_json_request_spec(
        "DELETE",
        ORDERS_PATH,
        json!({
            "symbol": "BTC-USD"
        }),
        None,
    )
}
