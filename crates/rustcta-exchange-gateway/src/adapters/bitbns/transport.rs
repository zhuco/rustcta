use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

#[derive(Clone)]
pub struct BitbnsRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BitbnsRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
    ) -> ExchangeApiResult<Self> {
        let http = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(16)
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

    if !status.is_success() {
        let message = error_message(&value)
            .unwrap_or("Bitbns request failed")
            .to_string();
        return Err(exchange_error(
            exchange_id,
            status.as_u16().to_string(),
            message,
            value,
        ));
    }

    if value.get("status").is_some_and(is_failure_status) {
        let message = error_message(&value)
            .unwrap_or("Bitbns API error")
            .to_string();
        return Err(exchange_error(
            exchange_id,
            value
                .get("code")
                .and_then(value_as_i64)
                .map(|code| code.to_string())
                .unwrap_or_else(|| "status".to_string()),
            message,
            value,
        ));
    }

    if value
        .get("error")
        .is_some_and(|error| !error.is_null() && error.as_str() != Some(""))
    {
        let message = error_message(&value)
            .unwrap_or("Bitbns API error")
            .to_string();
        return Err(exchange_error(
            exchange_id,
            value
                .get("code")
                .and_then(value_as_i64)
                .map(|code| code.to_string())
                .unwrap_or_else(|| "error".to_string()),
            message,
            value,
        ));
    }

    Ok(value.get("data").cloned().unwrap_or(value))
}

fn exchange_error(
    exchange_id: ExchangeId,
    code: String,
    message: String,
    raw: Value,
) -> ExchangeApiError {
    let class = classify_bitbns_error(&message);
    let mut error = ExchangeError::new(exchange_id, class, message, Utc::now());
    error.code = Some(code);
    error.raw = Some(raw);
    ExchangeApiError::Exchange(error)
}

fn classify_bitbns_error(message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    if message.contains("symbol") || message.contains("coin") || message.contains("market") {
        ExchangeErrorClass::InvalidSymbol
    } else if message.contains("auth")
        || message.contains("api")
        || message.contains("signature")
        || message.contains("key")
    {
        ExchangeErrorClass::Authentication
    } else if message.contains("rate") || message.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("error")
        .or_else(|| value.get("msg"))
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
}

fn is_failure_status(value: &Value) -> bool {
    match value {
        Value::Bool(status) => !*status,
        Value::Number(number) => number.as_i64().is_some_and(|status| status <= 0),
        Value::String(text) => matches!(text.as_str(), "0" | "false" | "False" | "FAILED"),
        _ => false,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        let query = pairs
            .into_iter()
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
