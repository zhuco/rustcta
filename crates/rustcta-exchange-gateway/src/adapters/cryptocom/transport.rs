use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::sign_request;

#[derive(Clone)]
pub struct CryptoComRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl CryptoComRest {
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
        method: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.rest_base_url, method, params);
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

    pub async fn send_private_post(
        &self,
        method: &str,
        params: Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptocom.private_rest_missing_credentials",
            });
        }
        let nonce = Utc::now().timestamp_millis();
        let request_id = nonce as u64;
        let sig = sign_request(api_secret, method, request_id, api_key, &params, nonce);
        let body = json!({
            "id": request_id,
            "method": method,
            "api_key": api_key,
            "params": params,
            "nonce": nonce,
            "sig": sig,
        });
        let response = self
            .http
            .post(format!(
                "{}/{}",
                self.rest_base_url.trim_end_matches('/'),
                method.trim_start_matches('/')
            ))
            .header("Content-Type", "application/json")
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
        let message = error_message(&value).unwrap_or("Crypto.com request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_cryptocom_error(value.get("code").and_then(value_as_i64), message),
            message,
            Utc::now(),
        );
        error.code = value
            .get("code")
            .and_then(value_as_i64)
            .map(|code| code.to_string())
            .or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    let code = value.get("code").and_then(value_as_i64).unwrap_or(0);
    if code != 0 {
        let message = error_message(&value).unwrap_or("Crypto.com API error");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_cryptocom_error(Some(code), message),
            message,
            Utc::now(),
        );
        error.code = Some(code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    Ok(value.get("result").cloned().unwrap_or(value))
}

fn classify_cryptocom_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if code == Some(306) || msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if code == Some(204) || msg.contains("duplicate") || msg.contains("client_oid") {
        ExchangeErrorClass::DuplicateClientOrderId
    } else if code == Some(212) || msg.contains("invalid_order") || msg.contains("not found") {
        ExchangeErrorClass::OrderNotFound
    } else if code == Some(40101) || msg.contains("unauthor") || msg.contains("signature") {
        ExchangeErrorClass::Authentication
    } else if code == Some(10004) || msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if msg.contains("symbol") || msg.contains("instrument") {
        ExchangeErrorClass::InvalidSymbol
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("message")
        .or_else(|| value.get("msg"))
        .and_then(Value::as_str)
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn build_url(base: &str, method: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!(
        "{}/{}",
        base.trim_end_matches('/'),
        method.trim_start_matches('/')
    );
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        url.push('?');
        url.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}={}",
                        encode_query_component(key),
                        encode_query_component(value)
                    )
                })
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    url
}

fn encode_query_component(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if matches!(
            byte,
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~'
        ) {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(hex_digit(byte >> 4));
            encoded.push(hex_digit(byte & 0x0f));
        }
    }
    encoded
}

fn hex_digit(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'A' + value - 10) as char,
        _ => unreachable!("nibble is always <= 15"),
    }
}
