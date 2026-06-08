#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

pub fn order_rpc_params_fixture() -> Value {
    json!({
        "subaccount_id": "fixture-subaccount",
        "instrument_name": "BTC-PERP",
        "direction": "buy",
        "order_type": "limit",
        "amount": "0.01",
        "limit_price": "65000",
        "label": "fixture-client-order-id"
    })
}
