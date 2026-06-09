#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

#[derive(Clone)]
pub struct BlockchainComRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BlockchainComRest {
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

    pub async fn send_api_token_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_token: &str,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(build_url(&self.rest_base_url, endpoint, params))
            .header("Accept", "application/json")
            .header("X-API-Token", api_token)
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

pub fn api_token_json_request_spec(
    method: &str,
    path: &str,
    query: Value,
    body: Option<Value>,
) -> Value {
    json!({
        "method": method,
        "path": path,
        "auth": "api_key_header",
        "headers": {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-Token": "<redacted>"
        },
        "query": query,
        "body": body
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
        let message = value
            .get("description")
            .or_else(|| value.get("message"))
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Blockchain.com Exchange REST error");
        let mut exchange_error = ExchangeError::new(
            exchange_id,
            classify_error(status.as_u16(), message),
            message,
            Utc::now(),
        );
        exchange_error.code = Some(
            value
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or_else(|| status.as_str())
                .to_string(),
        );
        exchange_error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(exchange_error));
    }
    Ok(value)
}

fn classify_error(status: u16, message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    match status {
        401 | 403 => ExchangeErrorClass::Authentication,
        404 => ExchangeErrorClass::OrderNotFound,
        429 => ExchangeErrorClass::RateLimited,
        500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ if message.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ if message.contains("balance") || message.contains("funds") => {
            ExchangeErrorClass::InsufficientBalance
        }
        _ if message.contains("auth") || message.contains("token") => {
            ExchangeErrorClass::Authentication
        }
        _ => ExchangeErrorClass::InvalidRequest,
    }
}
