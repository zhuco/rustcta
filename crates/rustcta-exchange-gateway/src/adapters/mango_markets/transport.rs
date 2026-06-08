#![cfg_attr(not(test), allow(dead_code))]

use serde_json::{json, Value};

pub fn solana_get_account_info_path() -> &'static str {
    "/"
}

pub fn solana_get_account_info_body(pubkey: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": "mango-markets-group-scan",
        "method": "getAccountInfo",
        "params": [
            pubkey,
            {
                "encoding": "base64",
                "commitment": "confirmed"
            }
        ]
    })
}
