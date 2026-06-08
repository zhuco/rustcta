use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{Map, Value};

use super::signing::sign_private_request;

#[derive(Clone)]
pub struct GeminiRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl GeminiRest {
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

    pub async fn send_private_post(
        &self,
        api_key: &str,
        api_secret: &str,
        endpoint: &str,
        mut payload: Map<String, Value>,
    ) -> ExchangeApiResult<Value> {
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            return Err(ExchangeApiError::Unsupported {
                operation: "gemini.private_rest_missing_credentials",
            });
        }
        let nonce = Utc::now().timestamp_micros().to_string();
        let signed =
            sign_private_request(api_secret, endpoint, &nonce, std::mem::take(&mut payload));
        let response = self
            .http
            .post(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                endpoint
            ))
            .header("Gemini-APIKey", api_key)
            .header("Gemini-Payload", signed.payload_base64)
            .header("Gemini-Signature", signed.signature_hex)
            .header("Content-Type", "text/plain")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }
}

pub fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        url.push('?');
        url.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    url
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

    if status.is_success() {
        return Ok(value);
    }

    let message = value
        .get("message")
        .or_else(|| value.get("reason"))
        .and_then(Value::as_str)
        .unwrap_or("Gemini request failed");
    let reason = value.get("reason").and_then(Value::as_str);
    let mut error = ExchangeError::new(
        exchange_id,
        classify_gemini_error(reason, message),
        message,
        Utc::now(),
    );
    error.code = reason
        .map(str::to_string)
        .or_else(|| Some(status.as_u16().to_string()));
    error.raw = Some(value);
    Err(ExchangeApiError::Exchange(error))
}

fn classify_gemini_error(reason: Option<&str>, message: &str) -> ExchangeErrorClass {
    let reason = reason.unwrap_or_default().to_ascii_lowercase();
    let message = message.to_ascii_lowercase();
    if reason.contains("insufficient") || message.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if reason.contains("symbol") || message.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if reason.contains("ratelimit") || message.contains("rate limit") {
        ExchangeErrorClass::RateLimited
    } else if reason.contains("precision") || message.contains("precision") {
        ExchangeErrorClass::InvalidPrecision
    } else if reason.contains("min") && message.contains("notional") {
        ExchangeErrorClass::MinNotionalViolation
    } else if reason.contains("ordernotfound") || message.contains("not found") {
        ExchangeErrorClass::OrderNotFound
    } else if reason.contains("auth") || reason.contains("apikey") || message.contains("auth") {
        ExchangeErrorClass::Authentication
    } else {
        ExchangeErrorClass::Unknown
    }
}
