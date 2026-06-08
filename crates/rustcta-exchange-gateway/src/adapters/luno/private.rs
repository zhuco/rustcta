#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

use super::transport::basic_auth_form_request_spec;

pub const BALANCES_PATH: &str = "/api/1/balance";
pub const PLACE_ORDER_PATH: &str = "/api/1/postorder";
pub const CANCEL_ORDER_PATH: &str = "/api/1/stoporder";
pub const OPEN_ORDERS_PATH: &str = "/api/1/listorders";
pub const QUERY_ORDER_PATH: &str = "/api/1/orders/{id}";
pub const RECENT_FILLS_PATH: &str = "/api/1/listtrades";

pub fn place_order_request_spec_fixture() -> Value {
    basic_auth_form_request_spec(
        "POST",
        PLACE_ORDER_PATH,
        json!({
            "pair": "XBTZAR",
            "type": "BID",
            "volume": "0.01",
            "price": "1200000",
            "base_account_id": "<redacted>",
            "counter_account_id": "<redacted>"
        }),
    )
}

pub fn cancel_order_request_spec_fixture() -> Value {
    basic_auth_form_request_spec(
        "POST",
        CANCEL_ORDER_PATH,
        json!({
            "order_id": "BXMC2CJ7HNB88U4"
        }),
    )
}
