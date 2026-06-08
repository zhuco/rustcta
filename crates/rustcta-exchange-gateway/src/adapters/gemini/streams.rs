use rustcta_exchange_api::{
    PrivateOrderStreamEventKind, PrivateStreamCapabilities, EXCHANGE_API_SCHEMA_VERSION,
};
use serde_json::{json, Value};

use super::signing::sign_private_request;

pub fn public_marketdata_url(public_ws_base_url: &str, symbol: &str) -> String {
    format!(
        "{}?symbols={}",
        public_ws_base_url.trim_end_matches('/'),
        symbol
    )
}

pub fn private_order_events_auth_headers(
    api_key: &str,
    api_secret: &str,
    nonce: &str,
) -> Vec<(String, String)> {
    let signed = sign_private_request(
        api_secret,
        "/v1/order/events",
        nonce,
        serde_json::Map::new(),
    );
    vec![
        ("Gemini-APIKey".to_string(), api_key.to_string()),
        ("Gemini-Payload".to_string(), signed.payload_base64),
        ("Gemini-Signature".to_string(), signed.signature_hex),
    ]
}

pub fn private_subscription_marker() -> Value {
    json!({ "transport": "headers", "channel": "order_events" })
}

pub fn gemini_private_stream_capabilities(enabled: bool) -> PrivateStreamCapabilities {
    if !enabled {
        return PrivateStreamCapabilities::unsupported(EXCHANGE_API_SCHEMA_VERSION);
    }
    PrivateStreamCapabilities {
        schema_version: EXCHANGE_API_SCHEMA_VERSION,
        supports_orders: true,
        supports_fills: true,
        supports_balances: false,
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
