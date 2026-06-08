#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::{canonical_body, sign_body};

#[derive(Clone)]
pub struct CryptomusRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl CryptomusRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
    ) -> ExchangeApiResult<Self> {
        let http = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
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

    pub async fn send_signed_get(
        &self,
        user_id: &str,
        api_key: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            user_id,
            api_key,
            endpoint,
            params,
            None,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        user_id: &str,
        api_key: &str,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            user_id,
            api_key,
            endpoint,
            &HashMap::new(),
            Some(body),
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        user_id: &str,
        api_key: &str,
        endpoint: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            user_id,
            api_key,
            endpoint,
            &HashMap::new(),
            None,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        user_id: &str,
        api_key: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
    ) -> ExchangeApiResult<Value> {
        if user_id.trim().is_empty() || api_key.trim().is_empty() {
            return Err(ExchangeApiError::Unsupported {
                operation: "cryptomus.private_rest_missing_user_id_or_key",
            });
        }
        let body_text = canonical_body(body).map_err(|error| ExchangeApiError::Serialization {
            message: error.to_string(),
        })?;
        let signature = sign_body(&body_text, api_key);
        let mut request = self
            .http
            .request(method, build_url(&self.rest_base_url, endpoint, params))
            .header("userId", user_id)
            .header("sign", signature)
            .header("Content-Type", "application/json");
        if !body_text.is_empty() {
            request = request.body(body_text);
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

pub fn public_get_request_spec(path: &str, query: Value) -> Value {
    json!({
        "method": "GET",
        "path": path,
        "query": query,
        "auth": "none",
        "headers": { "Accept": "application/json" }
    })
}

pub fn signed_json_request_spec(method: &str, path: &str, body: Value) -> Value {
    json!({
        "method": method,
        "path": path,
        "auth": "cryptomus_md5_base64_body",
        "headers": {
            "userId": "<redacted-user-uuid>",
            "sign": "<md5-base64-body-plus-api-key>",
            "Content-Type": "application/json"
        },
        "body": body
    })
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        let query = pairs
            .into_iter()
            .map(|(key, value)| {
                format!(
                    "{}={}",
                    urlencoding::encode(key),
                    urlencoding::encode(value)
                )
            })
            .collect::<Vec<_>>()
            .join("&");
        url.push('?');
        url.push_str(&query);
    }
    url
}

async fn parse_response(
    exchange_id: ExchangeId,
    response: reqwest::Response,
) -> ExchangeApiResult<Value> {
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    let value = serde_json::from_str::<Value>(&body).map_err(|error| {
        let mut exchange_error = ExchangeError::new(
            exchange_id.clone(),
            ExchangeErrorClass::Decode,
            error.to_string(),
            Utc::now(),
        );
        exchange_error.code = Some(status.as_u16().to_string());
        exchange_error.raw = Some(Value::String(body.clone()));
        ExchangeApiError::Exchange(exchange_error)
    })?;
    if !status.is_success() {
        let message = error_message(&value)
            .unwrap_or("cryptomus REST error")
            .to_string();
        return Err(exchange_error(
            exchange_id,
            if status.as_u16() == 429 {
                ExchangeErrorClass::RateLimited
            } else if status.is_server_error() {
                ExchangeErrorClass::ExchangeUnavailable
            } else {
                ExchangeErrorClass::InvalidRequest
            },
            status.as_u16().to_string(),
            &message,
            value,
        ));
    }
    if let Some(code) = value
        .get("code")
        .or_else(|| value.get("state"))
        .and_then(value_as_i64)
    {
        if code != 0 {
            let message = error_message(&value)
                .unwrap_or("cryptomus API error")
                .to_string();
            return Err(exchange_error(
                exchange_id,
                classify_cryptomus_error(code, &message),
                code.to_string(),
                &message,
                value,
            ));
        }
    }
    Ok(value)
}

fn exchange_error(
    exchange_id: ExchangeId,
    class: ExchangeErrorClass,
    code: String,
    message: &str,
    raw: Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(exchange_id, class, message, Utc::now());
    error.code = Some(code);
    error.raw = Some(raw);
    ExchangeApiError::Exchange(error)
}

fn classify_cryptomus_error(code: i64, message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    if code == 6 || message.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if code == 5 || message.contains("not found") {
        ExchangeErrorClass::OrderNotFound
    } else if code == 7 || message.contains("client_order_id") {
        ExchangeErrorClass::InvalidClientOrderId
    } else if code == 1 || message.contains("market") || message.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if code == 2 || message.contains("accuracy") || message.contains("precision") {
        ExchangeErrorClass::InvalidPrecision
    } else if code == 3 || message.contains("acceptable ranges") {
        ExchangeErrorClass::MinNotionalViolation
    } else if message.contains("auth") || message.contains("sign") || message.contains("key") {
        ExchangeErrorClass::Authentication
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
