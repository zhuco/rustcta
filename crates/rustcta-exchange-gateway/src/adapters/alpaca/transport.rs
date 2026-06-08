use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::auth_headers;

#[derive(Clone)]
pub struct AlpacaRest {
    exchange_id: ExchangeId,
    broker_base_url: String,
    market_data_base_url: String,
    http: reqwest::Client,
}

impl AlpacaRest {
    pub fn new(
        exchange_id: ExchangeId,
        broker_base_url: String,
        market_data_base_url: String,
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
            broker_base_url,
            market_data_base_url,
            http,
        })
    }

    pub async fn send_broker_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::GET,
            &self.broker_base_url,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_broker_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::POST,
            &self.broker_base_url,
            endpoint,
            params,
            Some(body),
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_broker_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::DELETE,
            &self.broker_base_url,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_market_data_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::GET,
            &self.market_data_base_url,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
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
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let request_path = build_path(endpoint, params);
        let mut request = self.http.request(
            method,
            format!("{}{}", base_url.trim_end_matches('/'), request_path),
        );
        for (name, value) in auth_headers(api_key, api_secret)?.into_vec() {
            request = request.header(name, value);
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
    let text = response
        .text()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    let value = if text.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str::<Value>(&text).map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?
    };
    if !status.is_success() {
        let message = error_message(&value).unwrap_or("Alpaca request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_alpaca_error(status.as_u16(), message, &value),
            message,
            Utc::now(),
        );
        error.code = error_code(&value).or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_alpaca_error(status: u16, message: &str, value: &Value) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    let code = error_code(value).unwrap_or_default().to_ascii_lowercase();
    if status == 401 || msg.contains("auth") || msg.contains("unauthorized") {
        ExchangeErrorClass::Authentication
    } else if status == 403 || msg.contains("permission") || msg.contains("forbidden") {
        ExchangeErrorClass::Permission
    } else if status == 429 || msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if status == 404 || msg.contains("not found") || code.contains("not_found") {
        ExchangeErrorClass::OrderNotFound
    } else if msg.contains("insufficient") || msg.contains("buying power") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("symbol") || msg.contains("asset") {
        ExchangeErrorClass::InvalidSymbol
    } else if msg.contains("increment") || msg.contains("precision") {
        ExchangeErrorClass::InvalidPrecision
    } else if msg.contains("minimum") || msg.contains("min") {
        ExchangeErrorClass::MinNotionalViolation
    } else if status == 422 || msg.contains("reject") {
        ExchangeErrorClass::OrderRejected
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("message")
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
        .or_else(|| value.get("msg").and_then(Value::as_str))
}

fn error_code(value: &Value) -> Option<String> {
    value
        .get("code")
        .or_else(|| value.get("error_code"))
        .and_then(|item| {
            if let Some(text) = item.as_str() {
                Some(text.to_string())
            } else {
                item.as_i64().map(|number| number.to_string())
            }
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
                .map(|(key, value)| {
                    format!(
                        "{}={}",
                        urlencoding::encode(key),
                        urlencoding::encode(value)
                    )
                })
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    path
}
