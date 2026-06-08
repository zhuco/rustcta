use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

#[derive(Clone)]
pub struct BullishRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BullishRest {
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

    pub async fn send_public_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.rest_base_url, endpoint, params);
        let response = self
            .http
            .request(Method::GET, url)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }
}

pub fn public_get_request_spec(path: &str) -> Value {
    json!({
        "method": "GET",
        "path": path,
        "auth": "none",
        "headers": { "Accept": "application/json" }
    })
}

pub fn signed_post_request_spec(path: &str, body: Value) -> Value {
    json!({
        "method": "POST",
        "path": path,
        "auth": "bearer_jwt_plus_bx_signature",
        "headers": {
            "Authorization": "Bearer <redacted>",
            "BX-TIMESTAMP": "<milliseconds>",
            "BX-NONCE": "<uint64>",
            "BX-SIGNATURE": "<hmac-or-ecdsa-signature>"
        },
        "body": body
    })
}

async fn parse_response(
    exchange_id: ExchangeId,
    response: reqwest::Response,
) -> ExchangeApiResult<Value> {
    let status = response.status();
    let value = response
        .json::<Value>()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    if !status.is_success() {
        let message = value
            .get("message")
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Bullish request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bullish_error(status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_bullish_error(status: u16, message: &str) -> ExchangeErrorClass {
    let lower = message.to_ascii_lowercase();
    match status {
        401 | 403 => ExchangeErrorClass::Authentication,
        404 if lower.contains("order") => ExchangeErrorClass::OrderNotFound,
        400 if lower.contains("duplicate") => ExchangeErrorClass::DuplicateClientOrderId,
        400 => ExchangeErrorClass::InvalidRequest,
        429 => ExchangeErrorClass::RateLimited,
        500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ => ExchangeErrorClass::Unknown,
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        if let Ok(mut parsed) = reqwest::Url::parse(&url) {
            {
                let mut pairs = parsed.query_pairs_mut();
                for (key, value) in params {
                    pairs.append_pair(key, value);
                }
            }
            return parsed.to_string();
        }
    }
    url
}
