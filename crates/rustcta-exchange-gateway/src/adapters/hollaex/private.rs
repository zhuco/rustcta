use std::collections::BTreeMap;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

use crate::request_spec::ActualHttpRequest;

use super::signing::{rest_hmac_sha256_payload, sign_hmac_sha256_hex};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HollaexOfflineRequest {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Value>,
}

impl HollaexOfflineRequest {
    pub fn actual_http_request(&self) -> ActualHttpRequest {
        ActualHttpRequest::new(self.method.clone(), self.path.clone())
            .with_query(self.query.clone())
            .with_headers(self.headers.clone())
            .with_body(self.body.clone())
    }
}

pub fn build_get_balances_request_spec(
    api_key: &str,
    api_secret: &str,
    expires_s: u64,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "GET",
        "/v2/user/balance",
        "/v2/user/balance",
        BTreeMap::new(),
        None,
        expires_s,
    )
}

pub fn build_place_order_request_spec(
    api_key: &str,
    api_secret: &str,
    expires_s: u64,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    let body = place_order_body_value()?;
    signed_request(
        api_key,
        api_secret,
        "POST",
        "/v2/order",
        "/v2/order",
        BTreeMap::new(),
        Some((place_order_json_body().to_string(), body)),
        expires_s,
    )
}

pub fn build_cancel_order_request_spec(
    api_key: &str,
    api_secret: &str,
    expires_s: u64,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    let mut query = BTreeMap::new();
    query.insert("order_id".to_string(), "order-fixture-1".to_string());
    signed_request(
        api_key,
        api_secret,
        "DELETE",
        "/v2/order",
        "/v2/order?order_id=order-fixture-1",
        query,
        None,
        expires_s,
    )
}

pub fn place_order_json_body() -> &'static str {
    r#"{"symbol":"xht-usdt","side":"sell","size":0.1,"type":"limit","price":1}"#
}

fn place_order_body_value() -> ExchangeApiResult<Value> {
    serde_json::from_str(place_order_json_body()).map_err(|error| {
        ExchangeApiError::InvalidRequest {
            message: format!("invalid hollaex fixture order body: {error}"),
        }
    })
}

fn signed_request(
    api_key: &str,
    api_secret: &str,
    method: &str,
    path: &str,
    sign_path: &str,
    query: BTreeMap<String, String>,
    body: Option<(String, Value)>,
    expires_s: u64,
) -> ExchangeApiResult<HollaexOfflineRequest> {
    if api_key.trim().is_empty() || api_secret.trim().is_empty() {
        return Err(ExchangeApiError::Unsupported {
            operation: "hollaex.private_request_spec_credentials",
        });
    }
    let body_text = body.as_ref().map(|(text, _)| text.as_str());
    let payload = rest_hmac_sha256_payload(method, sign_path, expires_s, body_text);
    let signature = sign_hmac_sha256_hex(api_secret, &payload)?;
    let mut headers = BTreeMap::new();
    headers.insert("api-key".to_string(), api_key.to_string());
    headers.insert("api-signature".to_string(), signature);
    headers.insert("api-expires".to_string(), expires_s.to_string());
    if body.is_some() {
        headers.insert("content-type".to_string(), "application/json".to_string());
    }
    Ok(HollaexOfflineRequest {
        method: method.to_string(),
        path: path.to_string(),
        query,
        headers,
        body: body.map(|(_, value)| value),
    })
}
