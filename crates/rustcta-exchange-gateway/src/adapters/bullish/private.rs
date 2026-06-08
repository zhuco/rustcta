#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

use super::transport::signed_post_request_spec;

pub const CREATE_ORDER_PATH: &str = "/trading-api/v2/orders";
pub const CANCEL_ORDER_PATH: &str = "/trading-api/v2/orders/cancel";

pub fn create_order_request_spec_fixture() -> Value {
    signed_post_request_spec(
        CREATE_ORDER_PATH,
        json!({
            "commandType": "V3CreateOrder",
            "symbol": "BTCUSDC",
            "type": "LMT",
            "side": "BUY",
            "quantity": "0.01",
            "price": "65000",
            "timeInForce": "GTC",
            "clientOrderId": "offline-fixture"
        }),
    )
}
