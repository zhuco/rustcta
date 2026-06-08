use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::bitfinex_rest_signature;

#[derive(Clone)]
pub struct BitfinexRest {
    exchange_id: ExchangeId,
    public_rest_base_url: String,
    private_rest_base_url: String,
    http: reqwest::Client,
}

impl BitfinexRest {
    pub fn new(
        exchange_id: ExchangeId,
        public_rest_base_url: String,
        private_rest_base_url: String,
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
            public_rest_base_url,
            private_rest_base_url,
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
            .get(build_url(&self.public_rest_base_url, endpoint, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        body: Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let body_text =
            serde_json::to_string(&body).map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?;
        let nonce = (Utc::now().timestamp_micros()).to_string();
        let signature = bitfinex_rest_signature(api_secret, endpoint, &nonce, &body_text);
        let response = self
            .http
            .post(format!(
                "{}{}",
                self.private_rest_base_url.trim_end_matches('/'),
                endpoint
            ))
            .header("Content-Type", "application/json")
            .header("bfx-nonce", nonce)
            .header("bfx-apikey", api_key)
            .header("bfx-signature", signature)
            .body(body_text)
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
    if !status.is_success() || bitfinex_error_message(&value).is_some() {
        let message = bitfinex_error_message(&value).unwrap_or("Bitfinex request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bitfinex_error(status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = status_error_code(&value).or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn bitfinex_error_message(value: &Value) -> Option<&str> {
    if let Some(array) = value.as_array() {
        if array.first().and_then(Value::as_str) == Some("error") {
            return array.get(2).and_then(Value::as_str);
        }
    }
    value
        .get("message")
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
}

fn status_error_code(value: &Value) -> Option<String> {
    value.as_array()?.get(1).map(|value| match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        other => other.to_string(),
    })
}

fn classify_bitfinex_error(status: u16, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if status == 401 || status == 403 || msg.contains("signature") || msg.contains("api key") {
        ExchangeErrorClass::Authentication
    } else if status == 429 || msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if msg.contains("unknown order") || msg.contains("order not found") {
        ExchangeErrorClass::OrderNotFound
    } else if msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("symbol") || msg.contains("pair") {
        ExchangeErrorClass::InvalidSymbol
    } else if status >= 500 {
        ExchangeErrorClass::ExchangeUnavailable
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        url.push('?');
        url.push_str(&query_string(params));
    }
    url
}

pub fn query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
