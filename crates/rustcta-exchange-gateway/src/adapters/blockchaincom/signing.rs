#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeMap;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};

pub const REST_AUTH_HEADER: &str = "X-API-Token";

pub fn blockchaincom_token_headers(api_key: &str) -> ExchangeApiResult<BTreeMap<String, String>> {
    let api_key = api_key.trim();
    if api_key.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "blockchaincom API key must not be empty".to_string(),
        });
    }
    Ok(BTreeMap::from([(
        REST_AUTH_HEADER.to_string(),
        api_key.to_string(),
    )]))
}

pub fn blockchaincom_ws_auth_payload(api_secret: &str) -> ExchangeApiResult<Value> {
    let api_secret = api_secret.trim();
    if api_secret.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "blockchaincom API secret must not be empty".to_string(),
        });
    }
    Ok(json!({
        "action": "subscribe",
        "channel": "auth",
        "token": api_secret
    }))
}
