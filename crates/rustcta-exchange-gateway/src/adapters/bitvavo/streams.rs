use rustcta_exchange_api::{
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, EXCHANGE_API_SCHEMA_VERSION,
};
use rustcta_types::ExchangeSymbol;
use serde_json::{json, Value};

use super::signing::sign_ws_auth;

pub fn public_subscribe_payload(channel: &str, symbols: &[ExchangeSymbol]) -> Value {
    json!({
        "action": "subscribe",
        "channels": [{
            "name": channel,
            "markets": symbols.iter().map(|symbol| symbol.symbol.clone()).collect::<Vec<_>>()
        }]
    })
}

pub fn private_auth_payload(api_key: &str, api_secret: &str, timestamp: &str) -> Value {
    json!({
        "action": "authenticate",
        "key": api_key,
        "signature": sign_ws_auth(api_secret, timestamp),
        "timestamp": timestamp.parse::<i64>().unwrap_or_default(),
        "window": 10000
    })
}

pub fn private_account_subscribe_payload() -> Value {
    json!({
        "action": "subscribe",
        "channels": [{ "name": "account" }]
    })
}

pub fn bitvavo_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: true,
        supports_positions: false,
        supports_account: true,
        order_event_kinds: vec![
            PrivateOrderStreamEventKind::New,
            PrivateOrderStreamEventKind::PartialFill,
            PrivateOrderStreamEventKind::Fill,
            PrivateOrderStreamEventKind::Cancel,
        ],
        supports_client_order_id: true,
        supports_exchange_order_id: true,
    }
}
