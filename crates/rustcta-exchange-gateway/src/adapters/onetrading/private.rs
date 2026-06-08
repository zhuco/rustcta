#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

use super::transport::bearer_request_spec;

pub const BALANCES_PATH: &str = "/account/balances";
pub const FEES_PATH: &str = "/account/fees";
pub const ORDERS_PATH: &str = "/account/orders";
pub const ORDER_BY_ID_PATH: &str = "/account/orders/<redacted:order_id>";
pub const ORDER_BY_CLIENT_ID_PATH: &str = "/account/orders/client/<redacted:order_id>";
pub const TRADES_PATH: &str = "/account/trades";

pub fn balances_request_spec_fixture() -> Value {
    bearer_request_spec("GET", BALANCES_PATH, json!({}), Value::Null)
}

pub fn fees_request_spec_fixture() -> Value {
    bearer_request_spec("GET", FEES_PATH, json!({}), Value::Null)
}

pub fn open_orders_request_spec_fixture() -> Value {
    bearer_request_spec(
        "GET",
        ORDERS_PATH,
        json!({
            "instrument_code": "BTC_EUR",
            "with_cancelled_and_rejected": "false",
            "with_just_filled_inactive": "false",
            "with_just_orders": "true",
            "max_page_size": "50"
        }),
        Value::Null,
    )
}

pub fn query_order_request_spec_fixture() -> Value {
    bearer_request_spec("GET", ORDER_BY_ID_PATH, json!({}), Value::Null)
}

pub fn recent_fills_request_spec_fixture() -> Value {
    bearer_request_spec(
        "GET",
        TRADES_PATH,
        json!({
            "instrument_code": "BTC_EUR",
            "max_page_size": "100"
        }),
        Value::Null,
    )
}

pub fn place_order_request_spec_fixture() -> Value {
    bearer_request_spec(
        "POST",
        ORDERS_PATH,
        json!({}),
        json!({
            "instrument_code": "BTC_EUR",
            "type": "LIMIT",
            "side": "BUY",
            "amount": "0.01",
            "price": "64000",
            "client_id": "<redacted:order_id>",
            "time_in_force": "GOOD_TILL_CANCELLED"
        }),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    bearer_request_spec("DELETE", ORDER_BY_ID_PATH, json!({}), Value::Null)
}

pub fn cancel_order_by_client_id_request_spec_fixture() -> Value {
    bearer_request_spec("DELETE", ORDER_BY_CLIENT_ID_PATH, json!({}), Value::Null)
}

pub fn cancel_all_orders_request_spec_fixture() -> Value {
    bearer_request_spec(
        "DELETE",
        ORDERS_PATH,
        json!({ "instrument_code": "BTC_EUR" }),
        Value::Null,
    )
}
