#![cfg_attr(not(test), allow(dead_code))]

use serde_json::Value;

pub fn private_event_kind(value: &Value) -> Option<&str> {
    let channel = value
        .get("channel")
        .or_else(|| value.get("params").and_then(|params| params.get("channel")))
        .and_then(Value::as_str)?;
    if channel == "user.orders" || channel.starts_with("orders.") {
        Some("user.orders")
    } else if channel == "user.trades" || channel.starts_with("trades.") {
        Some("user.trades")
    } else {
        Some(channel)
    }
}
