#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

pub fn place_order_request_spec_fixture_body() -> Value {
    json!({
        "market": "BTC-USD-PERP",
        "side": "BUY",
        "type": "LIMIT",
        "size": "0.01",
        "price": "65000",
        "client_id": "fixture-client-order-id",
        "signature": "<fixture-stark-signature>"
    })
}
