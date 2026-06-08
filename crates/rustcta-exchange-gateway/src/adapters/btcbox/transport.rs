use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

#[derive(Clone)]
pub struct BtcboxRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BtcboxRest {
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
    if status.is_success() && is_success_payload(&value) {
        return Ok(value);
    }
    let code = error_code(&value).unwrap_or_else(|| status.as_u16().to_string());
    let message = error_message(&value).unwrap_or("BTCBOX request failed");
    let mut error = ExchangeError::new(
        exchange_id,
        classify_error(&code, message, status.as_u16()),
        message,
        Utc::now(),
    );
    error.code = Some(code);
    error.raw = Some(value);
    Err(ExchangeApiError::Exchange(error))
}

fn is_success_payload(value: &Value) -> bool {
    if value
        .get("result")
        .and_then(Value::as_bool)
        .is_some_and(|result| !result)
    {
        return false;
    }
    match value.get("code") {
        Some(code) => code.as_i64().unwrap_or_default() == 0,
        None => true,
    }
}

fn error_code(value: &Value) -> Option<String> {
    value
        .get("code")
        .and_then(|code| {
            code.as_i64()
                .map(|number| number.to_string())
                .or_else(|| code.as_str().map(ToString::to_string))
        })
        .or_else(|| {
            value
                .get("error")
                .and_then(Value::as_i64)
                .map(|number| number.to_string())
        })
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("message")
        .or_else(|| value.get("msg"))
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
}

pub(super) fn classify_error(code: &str, message: &str, status: u16) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    match code {
        "102" => ExchangeErrorClass::InvalidSymbol,
        "103" | "104" => ExchangeErrorClass::Authentication,
        "105" | "301" | "302" | "403" | "404" | "900" => ExchangeErrorClass::Permission,
        "106" => ExchangeErrorClass::InvalidRequest,
        "200" => ExchangeErrorClass::InsufficientBalance,
        "201" | "202" | "204" => ExchangeErrorClass::OrderRejected,
        "203" => ExchangeErrorClass::OrderNotFound,
        "402" => ExchangeErrorClass::RateLimited,
        "401" | "901" => ExchangeErrorClass::ExchangeUnavailable,
        _ if status == 429 || message.contains("too frequent") => ExchangeErrorClass::RateLimited,
        _ if status == 401 || status == 403 => ExchangeErrorClass::Authentication,
        _ if message.contains("temporarily") || message.contains("suspended") => {
            ExchangeErrorClass::ExchangeUnavailable
        }
        _ => ExchangeErrorClass::Unknown,
    }
}

fn build_url(base: &str, path: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        url.push('?');
        url.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| format!("{}={}", url_encode(key), url_encode(value)))
                .collect::<Vec<_>>()
                .join("&"),
        );
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
