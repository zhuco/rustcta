#![cfg_attr(not(test), allow(dead_code))]

use serde_json::Value;

pub fn private_rpc_method(value: &Value) -> Option<&str> {
    value.get("method").and_then(Value::as_str)
}

pub fn private_event_channel(value: &Value) -> Option<&str> {
    value
        .get("params")
        .and_then(|params| params.get("channel"))
        .and_then(Value::as_str)
        .or_else(|| value.get("channel").and_then(Value::as_str))
}
