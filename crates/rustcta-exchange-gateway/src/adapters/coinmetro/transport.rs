use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::bearer_header;

#[derive(Clone)]
pub struct CoinmetroRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl CoinmetroRest {
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

    pub async fn send_bearer_get(
        &self,
        token: &str,
        device_id: Option<&str>,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_bearer_request(
            reqwest::Method::GET,
            token,
            device_id,
            endpoint,
            params,
            None,
        )
        .await
    }

    pub async fn send_bearer_post(
        &self,
        token: &str,
        device_id: Option<&str>,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_bearer_request(
            reqwest::Method::POST,
            token,
            device_id,
            endpoint,
            params,
            Some(body),
        )
        .await
    }

    pub async fn send_bearer_put(
        &self,
        token: &str,
        device_id: Option<&str>,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_bearer_request(
            reqwest::Method::PUT,
            token,
            device_id,
            endpoint,
            params,
            Some(body),
        )
        .await
    }

    async fn send_bearer_request(
        &self,
        method: reqwest::Method,
        token: &str,
        device_id: Option<&str>,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
    ) -> ExchangeApiResult<Value> {
        let authorization = bearer_header(token).ok_or(ExchangeApiError::Unsupported {
            operation: "coinmetro.private_rest_missing_bearer_token",
        })?;
        let mut request = self
            .http
            .request(method, build_url(&self.rest_base_url, endpoint, params))
            .header("Authorization", authorization)
            .header("Content-Type", "application/json");
        if let Some(device_id) = device_id.map(str::trim).filter(|value| !value.is_empty()) {
            request = request.header("X-Device-ID", device_id);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request
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

    let failed_status = value.get("status").and_then(Value::as_str) == Some("fail");
    if !status.is_success() || failed_status || value.get("error").is_some() {
        let message = error_message(&value).unwrap_or("Coinmetro request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_coinmetro_error(status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = Some(status.as_u16().to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    Ok(value)
}

fn classify_coinmetro_error(status: u16, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if status == 401 || status == 403 || msg.contains("auth") || msg.contains("token") {
        ExchangeErrorClass::Authentication
    } else if status == 429 || msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("min") || msg.contains("order size") {
        ExchangeErrorClass::MinNotionalViolation
    } else if msg.contains("symbol") || msg.contains("pair") || msg.contains("market") {
        ExchangeErrorClass::InvalidSymbol
    } else if msg.contains("order") && msg.contains("not found") {
        ExchangeErrorClass::OrderNotFound
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("reason")
        .or_else(|| value.get("message"))
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        build_path(endpoint, params)
    )
}

fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut path = endpoint.to_string();
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        let query = pairs
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        path.push('?');
        path.push_str(&query);
    }
    path
}
