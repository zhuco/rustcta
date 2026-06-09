#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::arkham_signature;

#[derive(Clone)]
pub struct ArkhamPublicRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    api_key: Option<String>,
    api_secret: Option<String>,
    http: reqwest::Client,
}

impl ArkhamPublicRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        api_key: Option<String>,
        api_secret: Option<String>,
        request_timeout_ms: u64,
    ) -> ExchangeApiResult<Self> {
        let http = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .timeout(Duration::from_millis(request_timeout_ms))
            .user_agent("RustCTA-Gateway/0.3")
            .build()
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        Ok(Self {
            exchange_id,
            rest_base_url,
            api_key,
            api_secret,
            http,
        })
    }

    pub async fn send_public_request(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(build_url(&self.rest_base_url, endpoint, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let api_key = self
            .api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let api_secret = self
            .api_secret
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let expires_us = Utc::now().timestamp_micros() + 30_000_000;
        let path_with_query = build_path_with_query(endpoint, params);
        let signature =
            arkham_signature(api_key, api_secret, expires_us, "GET", &path_with_query, "")?;
        let response = self
            .http
            .get(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                path_with_query
            ))
            .header("Arkham-Api-Key", api_key)
            .header("Arkham-Expires", expires_us.to_string())
            .header("Arkham-Signature", signature)
            .header("Arkham-Broker-Id", "1001")
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }
}

pub fn public_get_request_spec(path: &str, query: Value) -> Value {
    json!({
        "method": "GET",
        "path": path,
        "query": query,
        "auth": "none",
        "headers": { "Accept": "application/json" }
    })
}

pub fn signed_json_request_spec(method: &str, path: &str, body: Value) -> Value {
    json!({
        "method": method,
        "path": path,
        "auth": "arkham_hmac_sha256_base64",
        "headers": {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Arkham-Api-Key": "<redacted:api_key>",
            "Arkham-Expires": "<unix-microseconds-expiry>",
            "Arkham-Signature": "<redacted:signature>",
            "Arkham-Broker-Id": "1001"
        },
        "body": body
    })
}

pub fn signed_get_request_spec(path: &str, query: Value) -> Value {
    json!({
        "method": "GET",
        "path": path,
        "query": query,
        "auth": "arkham_hmac_sha256_base64",
        "headers": {
            "Accept": "application/json",
            "Arkham-Api-Key": "<redacted:api_key>",
            "Arkham-Expires": "<unix-microseconds-expiry>",
            "Arkham-Signature": "<redacted:signature>",
            "Arkham-Broker-Id": "1001"
        }
    })
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        build_path_with_query(endpoint, params)
    )
}

fn build_path_with_query(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = endpoint.to_string();
    if !params.is_empty() {
        let query = params
            .iter()
            .map(|(key, value)| {
                format!(
                    "{}={}",
                    urlencoding::encode(key),
                    urlencoding::encode(value)
                )
            })
            .collect::<Vec<_>>()
            .join("&");
        url.push('?');
        url.push_str(&query);
    }
    url
}

async fn parse_response(
    exchange_id: ExchangeId,
    response: reqwest::Response,
) -> ExchangeApiResult<Value> {
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    let value = serde_json::from_str::<Value>(&body).map_err(|error| {
        let mut exchange_error = ExchangeError::new(
            exchange_id.clone(),
            ExchangeErrorClass::Decode,
            error.to_string(),
            Utc::now(),
        );
        exchange_error.code = Some(status.as_u16().to_string());
        exchange_error.raw = Some(Value::String(body.clone()));
        ExchangeApiError::Exchange(exchange_error)
    })?;
    if !status.is_success() {
        let class = if status.as_u16() == 401 || status.as_u16() == 403 {
            ExchangeErrorClass::Authentication
        } else if status.as_u16() == 429 {
            ExchangeErrorClass::RateLimited
        } else if status.is_server_error() {
            ExchangeErrorClass::ExchangeUnavailable
        } else {
            ExchangeErrorClass::InvalidRequest
        };
        let mut exchange_error = ExchangeError::new(
            exchange_id,
            class,
            value
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| value.get("error").and_then(Value::as_str))
                .unwrap_or("arkham REST error"),
            Utc::now(),
        );
        exchange_error.code = Some(status.as_u16().to_string());
        exchange_error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(exchange_error));
    }
    Ok(value)
}
