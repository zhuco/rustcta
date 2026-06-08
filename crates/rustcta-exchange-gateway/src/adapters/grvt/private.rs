#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

pub fn grvt_account_summary_request(sub_account_id: &str, request_id: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "v1/account_summary",
        "params": {
            "sub_account_id": sub_account_id
        },
        "id": request_id
    })
}
