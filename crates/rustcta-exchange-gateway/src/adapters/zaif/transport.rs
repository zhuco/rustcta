use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

#[derive(Clone)]
pub struct ZaifRest {
    exchange_id: ExchangeId,
    public_rest_base_url: String,
    http: reqwest::Client,
}

impl ZaifRest {
    pub fn new(
        exchange_id: ExchangeId,
        public_rest_base_url: String,
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
            public_rest_base_url,
            http,
        })
    }

    pub async fn send_public_request(
        &self,
        path: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.public_rest_base_url, path, params);
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
    if status.is_success() && !value.get("error").is_some_and(|error| !error.is_null()) {
        return Ok(value);
    }
    let mut error = ExchangeError::new(
        exchange_id,
        classify_status(status.as_u16(), &value),
        error_message(&value).unwrap_or("zaif request failed"),
        Utc::now(),
    );
    error.code = Some(status.as_u16().to_string());
    error.raw = Some(value);
    Err(ExchangeApiError::Exchange(error))
}

fn classify_status(status: u16, value: &Value) -> ExchangeErrorClass {
    let message = error_message(value)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if status == 401 || status == 403 {
        ExchangeErrorClass::Authentication
    } else if status == 429 {
        ExchangeErrorClass::RateLimited
    } else if message.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if message.contains("currency_pair") || message.contains("order") {
        ExchangeErrorClass::OrderRejected
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("error")
        .and_then(Value::as_str)
        .or_else(|| value.get("message").and_then(Value::as_str))
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
