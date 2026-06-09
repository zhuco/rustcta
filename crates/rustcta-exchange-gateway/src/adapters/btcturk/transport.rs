#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::btcturk_signature;

#[derive(Clone)]
pub struct BtcTurkPublicRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BtcTurkPublicRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
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
        api_key: &str,
        api_secret: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let stamp_ms = Utc::now().timestamp_millis();
        let signature = btcturk_signature(api_key, api_secret, stamp_ms)?;
        let response = self
            .http
            .get(build_url(&self.rest_base_url, endpoint, params))
            .header("X-PCK", api_key)
            .header("X-Stamp", stamp_ms.to_string())
            .header("X-Signature", signature)
            .header(reqwest::header::ACCEPT, "application/json")
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
        "auth": "btcturk_hmac_sha256_base64",
        "headers": {
            "X-PCK": "<redacted-api-key>",
            "X-Stamp": "<milliseconds>",
            "X-Signature": "<base64-hmac-sha256>",
            "Content-Type": "application/json"
        },
        "body": body
    })
}

pub fn signed_get_request_spec(path: &str, query: Value) -> Value {
    json!({
        "method": "GET",
        "path": path,
        "auth": "btcturk_hmac_sha256_base64",
        "headers": {
            "X-PCK": "<redacted-api-key>",
            "X-Stamp": "<milliseconds>",
            "X-Signature": "<base64-hmac-sha256>",
            "Accept": "application/json"
        },
        "query": query
    })
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
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
        let class = if status.as_u16() == 429 {
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
                .unwrap_or("btcturk REST error"),
            Utc::now(),
        );
        exchange_error.code = Some(status.as_u16().to_string());
        exchange_error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(exchange_error));
    }
    Ok(value)
}
