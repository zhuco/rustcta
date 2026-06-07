use std::collections::HashMap;
use std::time::Duration;

use base64::Engine;
use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_payload;

#[derive(Clone)]
pub struct WhiteBitPublicRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl WhiteBitPublicRest {
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

    pub async fn send_signed_post(
        &self,
        api_key: &str,
        api_secret: &str,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            return Err(ExchangeApiError::Unsupported {
                operation: "whitebit.private_rest_missing_credentials",
            });
        }
        let mut body = body.clone();
        let body_object = body
            .as_object_mut()
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: "whitebit signed request body must be a JSON object".to_string(),
            })?;
        body_object.insert("request".to_string(), Value::String(endpoint.to_string()));
        body_object.insert(
            "nonce".to_string(),
            Value::String(Utc::now().timestamp_millis().to_string()),
        );
        let body_text =
            serde_json::to_string(&body).map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?;
        let payload = base64::engine::general_purpose::STANDARD.encode(body_text.as_bytes());
        let signature = sign_payload(api_secret, &payload);
        let response = self
            .http
            .request(
                reqwest::Method::POST,
                format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint),
            )
            .header("X-TXC-APIKEY", api_key)
            .header("X-TXC-PAYLOAD", payload)
            .header("X-TXC-SIGNATURE", signature)
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
        let code = value.get("code").and_then(value_as_i64);
        let message = error_message(&value).unwrap_or("WhiteBit request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_whitebit_error(code, message),
            message,
            Utc::now(),
        );
        error.code = code
            .map(|code| code.to_string())
            .or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    let code = value.get("code").and_then(value_as_i64).unwrap_or(0);
    if value.get("success").and_then(Value::as_bool) == Some(false) {
        let message = error_message(&value).unwrap_or("WhiteBIT API error");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_whitebit_error(value.get("code").and_then(value_as_i64), message),
            message,
            Utc::now(),
        );
        error.code = value
            .get("code")
            .and_then(value_as_i64)
            .map(|code| code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    if value.get("code").is_some() && code != 0 {
        let message = error_message(&value).unwrap_or("WhiteBit API error");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_whitebit_error(Some(code), message),
            message,
            Utc::now(),
        );
        error.code = Some(code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    Ok(value
        .get("data")
        .or_else(|| value.get("result"))
        .cloned()
        .unwrap_or(value))
}

fn classify_whitebit_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if code == Some(25) || msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if code == Some(23) || msg.contains("market") || msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if code == Some(213) || msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if code == Some(3008) || msg.contains("precision") {
        ExchangeErrorClass::InvalidPrecision
    } else if code == Some(3109) || msg.contains("notional") {
        ExchangeErrorClass::MinNotionalViolation
    } else if code == Some(3610) || msg.contains("order not found") {
        ExchangeErrorClass::OrderNotFound
    } else if code == Some(401) || msg.contains("auth") || msg.contains("key") {
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

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        build_path(endpoint, params)
    )
}

fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = endpoint.to_string();
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
