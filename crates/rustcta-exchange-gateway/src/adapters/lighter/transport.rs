#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

pub const LIGHTER_MAINNET_REST: &str = "https://mainnet.zklighter.elliot.ai/api/v1";
pub const LIGHTER_TESTNET_REST: &str = "https://testnet.zklighter.elliot.ai/api/v1";
pub const LIGHTER_MAINNET_WS: &str = "wss://mainnet.zklighter.elliot.ai/stream";
pub const LIGHTER_TESTNET_WS: &str = "wss://testnet.zklighter.elliot.ai/stream";
pub const LIGHTER_WS_KEEPALIVE_INTERVAL_MS: i64 = 60_000;
pub const LIGHTER_WS_KEEPALIVE_TIMEOUT_MS: i64 = 120_000;
pub const LIGHTER_WS_STALE_MESSAGE_MS: i64 = 180_000;

pub fn lighter_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        LIGHTER_WS_KEEPALIVE_INTERVAL_MS,
        LIGHTER_WS_KEEPALIVE_TIMEOUT_MS,
        LIGHTER_WS_STALE_MESSAGE_MS,
    )
}

#[derive(Clone)]
pub struct LighterRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    auth_token: Option<String>,
    http: reqwest::Client,
}

impl LighterRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        auth_token: Option<String>,
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
            auth_token,
            http,
        })
    }

    pub async fn send_private_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let token = self
            .auth_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let response = self
            .http
            .get(build_url(&self.rest_base_url, endpoint, params))
            .bearer_auth(token)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }
}

pub fn bearer_get_request_spec(path: &str, query: Value) -> Value {
    json!({
        "method": "GET",
        "path": path,
        "query": query,
        "auth": "bearer_token",
        "headers": {
            "Authorization": "<redacted:bearer_token>",
            "Accept": "application/json"
        }
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
                .unwrap_or("lighter REST error"),
            Utc::now(),
        );
        exchange_error.code = Some(status.as_u16().to_string());
        exchange_error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(exchange_error));
    }
    Ok(value)
}
