use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing;

#[derive(Clone)]
pub struct BitoproRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BitoproRest {
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

    pub async fn send_public_request(
        &self,
        path: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.rest_base_url, path, params);
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

    pub async fn send_signed_get(
        &self,
        api_key: &str,
        api_secret: &str,
        identity: &str,
        path: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let payload = signing::payload_for_get_delete(identity, Utc::now().timestamp_millis())?;
        let headers = signing::sign_headers(api_key, api_secret, payload)?;
        let url = build_url(&self.rest_base_url, path, params);
        let response = self
            .http
            .get(url)
            .header("X-BITOPRO-APIKEY", headers.api_key)
            .header("X-BITOPRO-PAYLOAD", headers.payload_base64)
            .header("X-BITOPRO-SIGNATURE", headers.signature)
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
    if status.is_success()
        && !value.get("error").is_some()
        && !(value.get("code").is_some() && value.get("data").is_none())
    {
        return Ok(value);
    }
    let mut error = ExchangeError::new(
        exchange_id,
        classify_status(status.as_u16(), &value),
        error_message(&value).unwrap_or("bitopro request failed"),
        Utc::now(),
    );
    error.code = value_as_string(value.get("code")).or_else(|| Some(status.as_u16().to_string()));
    error.raw = Some(value);
    Err(ExchangeApiError::Exchange(error))
}

fn classify_status(status: u16, value: &Value) -> ExchangeErrorClass {
    let message = error_message(value)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let code = value_as_string(value.get("code"))
        .unwrap_or_default()
        .to_ascii_lowercase();
    if message.contains("too many") || code.contains("too_many") || code.contains("rate") {
        return ExchangeErrorClass::RateLimited;
    }
    match status {
        401 => ExchangeErrorClass::Authentication,
        403 => ExchangeErrorClass::Permission,
        409 => ExchangeErrorClass::InvalidRequest,
        429 => ExchangeErrorClass::RateLimited,
        400 | 422 => {
            if message.contains("balance") {
                ExchangeErrorClass::InsufficientBalance
            } else if message.contains("order") {
                ExchangeErrorClass::OrderRejected
            } else {
                ExchangeErrorClass::InvalidRequest
            }
        }
        500 | 502 | 503 | 504 => ExchangeErrorClass::ExchangeUnavailable,
        _ => ExchangeErrorClass::Unknown,
    }
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("message")
        .and_then(Value::as_str)
        .or_else(|| value.get("error").and_then(Value::as_str))
        .or_else(|| value.get("reason").and_then(Value::as_str))
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    value.and_then(|value| match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    })
}

fn build_url(base: &str, path: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    if !params.is_empty() {
        let query = params
            .iter()
            .map(|(key, value)| format!("{}={}", url_encode(key), url_encode(value)))
            .collect::<Vec<_>>()
            .join("&");
        url.push('?');
        url.push_str(&query);
    }
    url
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}
