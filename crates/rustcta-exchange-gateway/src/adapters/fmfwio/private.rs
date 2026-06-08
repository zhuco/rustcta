use std::collections::BTreeMap;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

use crate::request_spec::ActualHttpRequest;

use super::signing::build_hs256_authorization_header;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FmfwioOfflineRequest {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub body: Option<Value>,
}

impl FmfwioOfflineRequest {
    pub fn actual_http_request(&self) -> ActualHttpRequest {
        ActualHttpRequest::new(self.method.clone(), self.path.clone())
            .with_query(self.query.clone())
            .with_headers(self.headers.clone())
            .with_body(self.body.clone())
    }
}

pub fn build_place_order_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<FmfwioOfflineRequest> {
    let body = place_order_form_body();
    signed_request(
        api_key,
        api_secret,
        "POST",
        "/api/3/spot/order",
        Some(&body),
        timestamp_ms,
        window_ms,
    )
}

pub fn build_cancel_order_request_spec(
    api_key: &str,
    api_secret: &str,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<FmfwioOfflineRequest> {
    signed_request(
        api_key,
        api_secret,
        "DELETE",
        "/api/3/spot/order/cli-fmfwio-1",
        None,
        timestamp_ms,
        window_ms,
    )
}

pub fn place_order_form_body() -> String {
    [
        ("client_order_id", "cli-fmfwio-1"),
        ("price", "0.046016"),
        ("quantity", "0.063"),
        ("side", "sell"),
        ("symbol", "ETHBTC"),
        ("type", "limit"),
    ]
    .into_iter()
    .map(|(key, value)| format!("{key}={value}"))
    .collect::<Vec<_>>()
    .join("&")
}

fn signed_request(
    api_key: &str,
    api_secret: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
    timestamp_ms: u64,
    window_ms: u64,
) -> ExchangeApiResult<FmfwioOfflineRequest> {
    if api_key.trim().is_empty() || api_secret.trim().is_empty() {
        return Err(ExchangeApiError::Unsupported {
            operation: "fmfwio.private_request_spec_credentials",
        });
    }
    let authorization = build_hs256_authorization_header(
        api_key,
        api_secret,
        method,
        path,
        body,
        timestamp_ms,
        Some(window_ms),
    )?;
    let mut headers = BTreeMap::new();
    headers.insert("authorization".to_string(), authorization);
    headers.insert(
        "content-type".to_string(),
        "application/x-www-form-urlencoded".to_string(),
    );
    Ok(FmfwioOfflineRequest {
        method: method.to_string(),
        path: path.to_string(),
        query: BTreeMap::new(),
        headers,
        body: body.map(|value| Value::String(value.to_string())),
    })
}
