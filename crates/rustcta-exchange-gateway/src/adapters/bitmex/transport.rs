use std::collections::HashMap;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{bitmex_signature, BitmexPrivateCredentials};

#[derive(Clone)]
pub struct BitmexRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<BitmexPrivateCredentials>,
}

impl BitmexRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<BitmexPrivateCredentials>,
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
            credentials,
        })
    }

    pub async fn send_public_request(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.rest_base_url, endpoint, params);
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

    #[allow(dead_code)]
    pub async fn send_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "bitmex.private_rest",
            })?;
        let request_path = build_request_path(endpoint, params);
        let url = format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            request_path
        );
        let expires = (Utc::now() + ChronoDuration::seconds(5)).timestamp();
        let signature =
            bitmex_signature(&credentials.api_secret, "GET", &request_path, expires, "");
        let response = self
            .http
            .get(url)
            .header("api-key", &credentials.api_key)
            .header("api-expires", expires.to_string())
            .header("api-signature", signature)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_post(&self, endpoint: &str, body: &Value) -> ExchangeApiResult<Value> {
        self.send_signed_json(reqwest::Method::POST, endpoint, body)
            .await
    }

    pub async fn send_signed_put(&self, endpoint: &str, body: &Value) -> ExchangeApiResult<Value> {
        self.send_signed_json(reqwest::Method::PUT, endpoint, body)
            .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "bitmex.private_rest",
            })?;
        let request_path = build_request_path(endpoint, params);
        let url = format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            request_path
        );
        let expires = (Utc::now() + ChronoDuration::seconds(5)).timestamp();
        let signature = bitmex_signature(
            &credentials.api_secret,
            "DELETE",
            &request_path,
            expires,
            "",
        );
        let response = self
            .http
            .delete(url)
            .header("api-key", &credentials.api_key)
            .header("api-expires", expires.to_string())
            .header("api-signature", signature)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    async fn send_signed_json(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "bitmex.private_rest",
            })?;
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let body_text =
            serde_json::to_string(body).map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?;
        let method_text = method.as_str().to_ascii_uppercase();
        let expires = (Utc::now() + ChronoDuration::seconds(5)).timestamp();
        let signature = bitmex_signature(
            &credentials.api_secret,
            &method_text,
            endpoint,
            expires,
            &body_text,
        );
        let response = self
            .http
            .request(method, url)
            .header("api-key", &credentials.api_key)
            .header("api-expires", expires.to_string())
            .header("api-signature", signature)
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
        let message = bitmex_error_message(&value).unwrap_or("BitMEX request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bitmex_error(status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = Some(status.as_u16().to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn bitmex_error_message(value: &Value) -> Option<&str> {
    value
        .get("error")
        .and_then(|error| error.get("message").or_else(|| error.get("name")))
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
}

fn classify_bitmex_error(status: u16, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if status == 401 || status == 403 || msg.contains("signature") || msg.contains("api key") {
        ExchangeErrorClass::Authentication
    } else if status == 429 || msg.contains("rate limit") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if status == 404 || msg.contains("not found") || msg.contains("unknown order") {
        ExchangeErrorClass::OrderNotFound
    } else if msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("symbol") || msg.contains("instrument") {
        ExchangeErrorClass::InvalidSymbol
    } else if status >= 500 || msg.contains("overload") {
        ExchangeErrorClass::ExchangeUnavailable
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        build_request_path(endpoint, params)
    )
}

fn build_request_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut path = endpoint.to_string();
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        path.push('?');
        path.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| format!("{key}={}", encode_query_component(value)))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    path
}

fn encode_query_component(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}
