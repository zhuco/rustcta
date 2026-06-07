use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{prehash, sign_prehash};

#[derive(Clone)]
pub struct AscendexRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl AscendexRest {
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
        endpoint: &str,
        sign_path: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            sign_path,
            params,
            Value::Null,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_signed_json(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        sign_path: &str,
        body: Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            method,
            endpoint,
            sign_path,
            &HashMap::new(),
            body,
            api_key,
            api_secret,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        sign_path: &str,
        params: &HashMap<String, String>,
        body: Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let payload = prehash(&timestamp, sign_path);
        let signature = sign_prehash(api_secret, &payload)?;
        let mut builder = self
            .http
            .request(method, build_url(&self.rest_base_url, endpoint, params))
            .header("x-auth-key", api_key)
            .header("x-auth-timestamp", timestamp)
            .header("x-auth-signature", signature)
            .header("Content-Type", "application/json");
        if !body.is_null() {
            builder = builder.json(&body);
        }
        let response = builder
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
    let code = value.get("code").and_then(value_as_code);
    let status_text = value.get("status").and_then(Value::as_str);
    if !status.is_success()
        || code.as_deref().is_some_and(|code| code != "0")
        || status_text.is_some_and(|status| status.eq_ignore_ascii_case("Err"))
    {
        let message = value
            .get("message")
            .or_else(|| value.get("reason"))
            .and_then(Value::as_str)
            .unwrap_or("AscendEX request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_ascendex_error(code.as_deref(), status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_ascendex_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match (status, code) {
        (401, _) | (_, "100001") | (_, "100002") | (_, "100003") => {
            ExchangeErrorClass::Authentication
        }
        (403, _) | (_, "100004") => ExchangeErrorClass::Permission,
        (418 | 429, _) | (_, "100014") => ExchangeErrorClass::RateLimited,
        (500..=599, _) => ExchangeErrorClass::ExchangeUnavailable,
        (_, "300011") => ExchangeErrorClass::InsufficientBalance,
        (_, "300004") => ExchangeErrorClass::MinNotionalViolation,
        (_, "300009") | (_, "300010") => ExchangeErrorClass::InvalidPrecision,
        _ if msg.contains("not enough") || msg.contains("insufficient") => {
            ExchangeErrorClass::InsufficientBalance
        }
        _ if msg.contains("notional") && msg.contains("small") => {
            ExchangeErrorClass::MinNotionalViolation
        }
        _ if msg.contains("duplicate") => ExchangeErrorClass::DuplicateClientOrderId,
        _ if msg.contains("order") && (msg.contains("not found") || msg.contains("not exist")) => {
            ExchangeErrorClass::OrderNotFound
        }
        _ if msg.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ => ExchangeErrorClass::Unknown,
    }
}

fn value_as_code(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

pub fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        url.push('?');
        url.push_str(&build_query_string(params));
    }
    url
}

pub fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
