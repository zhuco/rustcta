use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

#[derive(Clone)]
pub struct OrangeXRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl OrangeXRest {
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
        method: &str,
        params: Value,
    ) -> ExchangeApiResult<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let response = self
            .http
            .post(self.rest_base_url.trim_end_matches('/'))
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_public_rpc(&self, method: &str, params: Value) -> ExchangeApiResult<Value> {
        self.send_public_request(method, params).await
    }

    pub async fn send_private_rpc(
        &self,
        method: &str,
        params: Value,
        token: &str,
    ) -> ExchangeApiResult<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let response = self
            .http
            .post(self.rest_base_url.trim_end_matches('/'))
            .header("Content-Type", "application/json")
            .header("Authorization", format!("bearer {token}"))
            .json(&body)
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
        let message = orangex_error_message(&value).unwrap_or("OrangeX request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_orangex_error(status.as_u16(), error_code(&value), message),
            message,
            Utc::now(),
        );
        error.code = error_code(&value)
            .map(|code| code.to_string())
            .or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    if value.get("error").is_some_and(|error| !error.is_null()) {
        let message = orangex_error_message(&value).unwrap_or("OrangeX API error");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_orangex_error(status.as_u16(), error_code(&value), message),
            message,
            Utc::now(),
        );
        error.code = error_code(&value).map(|code| code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    Ok(value.get("result").cloned().unwrap_or(value))
}

fn orangex_error_message(value: &Value) -> Option<&str> {
    value
        .get("error")
        .and_then(|error| {
            error
                .get("message")
                .or_else(|| error.get("data"))
                .or_else(|| error.get("reason"))
        })
        .or_else(|| value.get("message"))
        .or_else(|| value.get("msg"))
        .and_then(Value::as_str)
}

fn error_code(value: &Value) -> Option<i64> {
    value
        .get("error")
        .and_then(|error| error.get("code"))
        .and_then(value_as_i64)
        .or_else(|| value.get("code").and_then(value_as_i64))
}

fn classify_orangex_error(status: u16, code: Option<i64>, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if status == 401
        || status == 403
        || matches!(code, Some(10_000..=10_999))
        || msg.contains("auth")
        || msg.contains("token")
        || msg.contains("signature")
    {
        ExchangeErrorClass::Authentication
    } else if status == 429 || msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if status >= 500 {
        ExchangeErrorClass::ExchangeUnavailable
    } else if msg.contains("instrument") || msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if msg.contains("precision") || msg.contains("tick") {
        ExchangeErrorClass::InvalidPrecision
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
