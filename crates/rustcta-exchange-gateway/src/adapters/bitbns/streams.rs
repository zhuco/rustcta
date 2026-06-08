use rustcta_exchange_api::{ExchangeApiResult, PublicStreamSubscription};
use serde_json::{json, Value};

use super::parser::normalize_bitbns_symbol;

pub fn bitbns_public_socket_url(market: &str, order_book: bool) -> String {
    let market = market.trim().to_ascii_lowercase();
    if order_book {
        format!("https://ws{market}mv2.bitbns.com/")
    } else {
        format!("https://ws{market}mv2.bitbns.com/?withTicker=true&onlyTicker=true")
    }
}

pub fn bitbns_orderbook_room(subscription: &PublicStreamSubscription) -> ExchangeApiResult<String> {
    let (coin, _market) = normalize_bitbns_symbol(&subscription.symbol.exchange_symbol.symbol)?;
    Ok(format!("news_{coin}"))
}

pub fn bitbns_public_subscribe_payload(
    subscription: &PublicStreamSubscription,
) -> ExchangeApiResult<Value> {
    Ok(json!({
        "event": "switchRoom",
        "room": bitbns_orderbook_room(subscription)?,
    }))
}

pub fn bitbns_ping_payload() -> Value {
    json!({ "event": "ping" })
}

pub fn bitbns_reconnect_policy_ms() -> (u64, u64, u64) {
    (30_000, 45_000, 60_000)
}

pub fn parse_socket_orderbook_news(value: &Value) -> ExchangeApiResult<Option<Value>> {
    let message_type = value.get("type").and_then(Value::as_str);
    match message_type {
        Some("sellList") | Some("buyList") | Some("tradeList") => {
            let data = value.get("data").cloned().unwrap_or(Value::Null);
            if let Some(text) = data.as_str() {
                serde_json::from_str(text).map(Some).map_err(|error| {
                    rustcta_exchange_api::ExchangeApiError::Serialization {
                        message: error.to_string(),
                    }
                })
            } else {
                Ok(Some(data))
            }
        }
        _ => Ok(None),
    }
}
