#![cfg_attr(not(test), allow(dead_code))]

use base64::{engine::general_purpose, Engine as _};
use hmac::{Hmac, Mac};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::{json, Value};
use sha2::Sha512;

type HmacSha512 = Hmac<Sha512>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BtcMarketsSignedRequest {
    pub method: String,
    pub path: String,
    pub body: String,
    pub api_key: String,
    pub timestamp: String,
    pub signature: String,
}

pub fn sign_rest_request(
    api_key: &str,
    api_secret: &str,
    method: &str,
    path: &str,
    timestamp_ms: &str,
    body: Option<&str>,
) -> ExchangeApiResult<BtcMarketsSignedRequest> {
    let method = method.trim().to_ascii_uppercase();
    let path = normalize_path(path)?;
    let body = body.unwrap_or_default().to_string();
    let signature = btcmarkets_signature(api_secret, &method, &path, timestamp_ms, &body)?;
    Ok(BtcMarketsSignedRequest {
        method,
        path,
        body,
        api_key: api_key.to_string(),
        timestamp: timestamp_ms.to_string(),
        signature,
    })
}

pub fn btcmarkets_signature(
    api_secret: &str,
    method: &str,
    path: &str,
    timestamp_ms: &str,
    body: &str,
) -> ExchangeApiResult<String> {
    if timestamp_ms.trim().is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "BTC Markets signature requires timestamp".to_string(),
        });
    }
    let secret = general_purpose::STANDARD
        .decode(api_secret)
        .map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("BTC Markets API secret must be base64: {error}"),
        })?;
    let mut mac =
        HmacSha512::new_from_slice(&secret).map_err(|error| ExchangeApiError::InvalidRequest {
            message: format!("invalid BTC Markets API secret: {error}"),
        })?;
    let message = if matches!(method, "GET" | "DELETE") && body.is_empty() {
        format!("{method}{path}{timestamp_ms}")
    } else {
        format!("{method}{path}{timestamp_ms}{body}")
    };
    mac.update(message.as_bytes());
    Ok(general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

pub fn websocket_subscribe_payload(
    api_key: Option<&str>,
    api_secret: Option<&str>,
    timestamp_ms: &str,
    channels: Vec<String>,
    market_ids: Vec<String>,
) -> ExchangeApiResult<Value> {
    let mut payload = json!({
        "messageType": "subscribe",
        "channels": channels,
        "marketIds": market_ids,
    });
    if let (Some(api_key), Some(api_secret)) = (api_key, api_secret) {
        let signature =
            btcmarkets_signature(api_secret, "GET", "/users/self/subscribe", timestamp_ms, "")?;
        payload["key"] = json!(api_key);
        payload["timestamp"] = json!(timestamp_ms);
        payload["signature"] = json!(signature);
    }
    Ok(payload)
}

fn normalize_path(path: &str) -> ExchangeApiResult<String> {
    let path = path.trim();
    if path.is_empty() {
        return Err(ExchangeApiError::InvalidRequest {
            message: "BTC Markets signed path must not be empty".to_string(),
        });
    }
    Ok(if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    })
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::{btcmarkets_signature, sign_rest_request, websocket_subscribe_payload};

    fn fixture(name: &str) -> Value {
        let text = match name {
            "rest_post_order.json" => include_str!(
                "../../../../../tests/fixtures/exchanges/btcmarkets/signing_vectors/rest_post_order.json"
            ),
            "rest_get_orders.json" => include_str!(
                "../../../../../tests/fixtures/exchanges/btcmarkets/signing_vectors/rest_get_orders.json"
            ),
            _ => panic!("unknown btcmarkets signing fixture {name}"),
        };
        serde_json::from_str(text).expect("btcmarkets signing fixture")
    }

    #[test]
    fn btcmarkets_signature_should_match_post_fixture() {
        let value = fixture("rest_post_order.json");
        let signature = btcmarkets_signature(
            value["secret"].as_str().unwrap(),
            value["method"].as_str().unwrap(),
            value["path"].as_str().unwrap(),
            value["timestamp"].as_str().unwrap(),
            value["body"].as_str().unwrap(),
        )
        .expect("signature");
        assert_eq!(signature, value["expected_signature"].as_str().unwrap());
    }

    #[test]
    fn btcmarkets_signed_get_should_omit_body_from_message() {
        let value = fixture("rest_get_orders.json");
        let request = sign_rest_request(
            value["api_key"].as_str().unwrap(),
            value["secret"].as_str().unwrap(),
            value["method"].as_str().unwrap(),
            value["path"].as_str().unwrap(),
            value["timestamp"].as_str().unwrap(),
            None,
        )
        .expect("signed request");

        assert_eq!(request.api_key, value["api_key"].as_str().unwrap());
        assert_eq!(
            request.signature,
            value["expected_signature"].as_str().unwrap()
        );
        assert!(request.body.is_empty());
    }

    #[test]
    fn websocket_private_subscription_should_include_auth_fields() {
        let value = fixture("rest_get_orders.json");
        let payload = websocket_subscribe_payload(
            Some(value["api_key"].as_str().unwrap()),
            Some(value["secret"].as_str().unwrap()),
            value["timestamp"].as_str().unwrap(),
            vec!["orderChange".to_string()],
            vec!["BTC-AUD".to_string()],
        )
        .expect("payload");

        assert_eq!(payload["messageType"], "subscribe");
        assert_eq!(payload["channels"][0], "orderChange");
        assert!(payload["signature"].as_str().is_some_and(|s| !s.is_empty()));
    }
}
