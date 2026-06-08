#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

#[derive(Clone)]
pub struct CoinmateRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl CoinmateRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
    ) -> ExchangeApiResult<Self> {
        let http = reqwest::Client::builder()
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
        let mut url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
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
        let response =
            self.http
                .get(url)
                .send()
                .await
                .map_err(|error| ExchangeApiError::Transport {
                    message: error.to_string(),
                })?;
        parse_response(self.exchange_id.clone(), response).await
    }
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
    let error_flag = value.get("error").and_then(Value::as_bool).unwrap_or(false);
    if !status.is_success() || error_flag {
        let message = value
            .get("errorMessage")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Coinmate API error");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_coinmate_error(message),
            message,
            Utc::now(),
        );
        error.code = Some(status.as_u16().to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_coinmate_error(message: &str) -> ExchangeErrorClass {
    let lower = message.to_ascii_lowercase();
    if lower.contains("access denied") || lower.contains("signature") || lower.contains("key") {
        ExchangeErrorClass::Authentication
    } else if lower.contains("request") || lower.contains("minute") || lower.contains("ban") {
        ExchangeErrorClass::RateLimited
    } else if lower.contains("currency pair") || lower.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if lower.contains("amount") || lower.contains("minimum") {
        ExchangeErrorClass::MinNotionalViolation
    } else if lower.contains("order") && lower.contains("not") {
        ExchangeErrorClass::OrderNotFound
    } else {
        ExchangeErrorClass::ExchangeUnavailable
    }
}

pub fn public_get_request_spec(path: &str, query: Value) -> Value {
    json!({
        "method": "GET",
        "path": path,
        "auth": "none",
        "query": query,
        "headers": { "Accept": "application/json" }
    })
}

pub fn signed_form_request_spec(path: &str, body: Value) -> Value {
    json!({
        "method": "POST",
        "path": path,
        "auth": "coinmate_hmac_sha256_form",
        "headers": {
            "Content-Type": "application/x-www-form-urlencoded"
        },
        "body": body
    })
}
