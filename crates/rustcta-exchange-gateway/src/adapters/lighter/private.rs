#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

pub fn lighter_read_only_auth_note(account_index: &str) -> Value {
    json!({
        "account_index": account_index,
        "auth": "bearer_token",
        "trade_enabled": false
    })
}
