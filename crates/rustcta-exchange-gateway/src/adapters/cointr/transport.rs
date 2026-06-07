use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_payload;

#[derive(Clone)]
pub struct CointrRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl CointrRest {
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
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
            passphrase,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        let mut body = body.clone();
        if recv_window_ms > 0 {
            if let Value::Object(fields) = &mut body {
                fields
                    .entry("recvWindow".to_string())
                    .or_insert_with(|| Value::from(recv_window_ms));
            }
        }
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            &HashMap::new(),
            Some(&body),
            api_key,
            api_secret,
            passphrase,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        let mut body = body.clone();
        if recv_window_ms > 0 {
            if let Value::Object(fields) = &mut body {
                fields
                    .entry("recvWindow".to_string())
                    .or_insert_with(|| Value::from(recv_window_ms));
            }
        }
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            &HashMap::new(),
            Some(&body),
            api_key,
            api_secret,
            passphrase,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis();
        let path = build_request_path(endpoint, params);
        let body_text = match body {
            Some(body) => {
                serde_json::to_string(body).map_err(|error| ExchangeApiError::InvalidRequest {
                    message: error.to_string(),
                })?
            }
            None => String::new(),
        };
        let signature = sign_payload(api_secret, timestamp, method.as_str(), &path, &body_text)?;
        let response = self
            .http
            .request(
                method,
                format!("{}{}", self.rest_base_url.trim_end_matches('/'), path),
            )
            .header("ACCESS-KEY", api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", timestamp.to_string())
            .header("ACCESS-PASSPHRASE", passphrase)
            .header("locale", "en-US")
            .header("Content-Type", "application/json")
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
    if !status.is_success() {
        let message = error_message(&value).unwrap_or("Cointr request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_cointr_error(value.get("code").and_then(value_as_i64), message),
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
    if code != 0 && code != 200 && code != 1000 {
        let message = error_message(&value).unwrap_or("Cointr API error");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_cointr_error(Some(code), message),
            message,
            Utc::now(),
        );
        error.code = Some(code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value.get("data").cloned().unwrap_or_else(|| {
        value
            .get("result")
            .cloned()
            .unwrap_or_else(|| value.clone())
    }))
}

fn classify_cointr_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if msg.contains("insufficient") || msg.contains("balance") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("duplicate") || msg.contains("client") {
        ExchangeErrorClass::DuplicateClientOrderId
    } else if msg.contains("not found") || msg.contains("not exist") || code == Some(10007) {
        ExchangeErrorClass::OrderNotFound
    } else if msg.contains("signature") || msg.contains("auth") || msg.contains("api key") {
        ExchangeErrorClass::Authentication
    } else if msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("msg")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        build_request_path(endpoint, params)
    )
}

fn build_request_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = endpoint.to_string();
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        url.push('?');
        url.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| {
                    format!("{}={}", encode_component(key), encode_component(value))
                })
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    url
}

fn encode_component(value: &str) -> String {
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
        _ => unreachable!("nibble"),
    }
}
