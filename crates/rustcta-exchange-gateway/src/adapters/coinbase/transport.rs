use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::bearer_authorization;

#[derive(Clone)]
pub struct CoinbaseRest {
    exchange_id: ExchangeId,
    spot_base_url: String,
    international_base_url: String,
    http: reqwest::Client,
}

impl CoinbaseRest {
    pub fn new(
        exchange_id: ExchangeId,
        spot_base_url: String,
        international_base_url: String,
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
            spot_base_url,
            international_base_url,
            http,
        })
    }

    pub async fn send_spot_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        bearer_token: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::GET,
            &self.spot_base_url,
            endpoint,
            params,
            None,
            bearer_token,
        )
        .await
    }

    pub async fn send_intx_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        bearer_token: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::GET,
            &self.international_base_url,
            endpoint,
            params,
            None,
            bearer_token,
        )
        .await
    }

    pub async fn send_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        bearer_token: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::GET,
            &self.spot_base_url,
            endpoint,
            params,
            None,
            Some(bearer_token),
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
        bearer_token: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::POST,
            &self.spot_base_url,
            endpoint,
            params,
            Some(body),
            Some(bearer_token),
        )
        .await
    }

    async fn send_request(
        &self,
        method: Method,
        base_url: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
        bearer_token: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        let request_path = build_path(endpoint, params);
        let mut request = self.http.request(
            method,
            format!("{}{}", base_url.trim_end_matches('/'), request_path),
        );
        if let Some(token) = bearer_token {
            request = request.header("Authorization", bearer_authorization(token)?);
        }
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .json(body);
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

    let success_field = value.get("success").and_then(Value::as_bool);
    if !status.is_success() || success_field == Some(false) || value.get("error_response").is_some()
    {
        let message = error_message(&value).unwrap_or("Coinbase request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_coinbase_error(status.as_u16(), message, &value),
            message,
            Utc::now(),
        );
        error.code = error_code(&value).or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_coinbase_error(status: u16, message: &str, value: &Value) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    let code = error_code(value).unwrap_or_default().to_ascii_lowercase();
    if status == 401 || status == 403 || msg.contains("auth") || msg.contains("permission") {
        ExchangeErrorClass::Authentication
    } else if status == 429 || msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("not found") || code.contains("order_not_found") {
        ExchangeErrorClass::OrderNotFound
    } else if msg.contains("product") || msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if msg.contains("precision") || msg.contains("increment") {
        ExchangeErrorClass::InvalidPrecision
    } else if msg.contains("minimum") || msg.contains("min") {
        ExchangeErrorClass::MinNotionalViolation
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("message")
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
        .or_else(|| {
            value
                .get("error_response")
                .and_then(|error| error.get("message"))
                .and_then(Value::as_str)
        })
        .or_else(|| {
            value
                .get("error_response")
                .and_then(|error| error.get("error_details"))
                .and_then(Value::as_str)
        })
}

fn error_code(value: &Value) -> Option<String> {
    value
        .get("error")
        .or_else(|| {
            value
                .get("error_response")
                .and_then(|error| error.get("error"))
        })
        .and_then(|value| match value {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
}

fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut path = endpoint.to_string();
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        path.push('?');
        path.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    path
}
