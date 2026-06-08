use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_request;

#[derive(Clone)]
pub struct BitvavoRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BitvavoRest {
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

    pub async fn send_signed_get(
        &self,
        api_key: &str,
        api_secret: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            api_key,
            api_secret,
            endpoint,
            params,
            None,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        api_key: &str,
        api_secret: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            api_key,
            api_secret,
            endpoint,
            params,
            Some(body),
        )
        .await
    }

    pub async fn send_signed_put(
        &self,
        api_key: &str,
        api_secret: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::PUT,
            api_key,
            api_secret,
            endpoint,
            params,
            Some(body),
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        api_key: &str,
        api_secret: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            api_key,
            api_secret,
            endpoint,
            params,
            Some(body),
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        api_key: &str,
        api_secret: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
    ) -> ExchangeApiResult<Value> {
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitvavo.private_rest_missing_credentials",
            });
        }
        let request_path = build_path(endpoint, params);
        let signature_path = if request_path.starts_with("/v2/") {
            request_path.clone()
        } else {
            format!("/v2{request_path}")
        };
        let timestamp = Utc::now().timestamp_millis().to_string();
        let body_text = body
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?
            .unwrap_or_default();
        let signature = sign_request(
            api_secret,
            &timestamp,
            method.as_str(),
            &signature_path,
            &body_text,
        );
        let mut request = self
            .http
            .request(
                method,
                format!(
                    "{}{}",
                    self.rest_base_url.trim_end_matches('/'),
                    request_path
                ),
            )
            .header("Bitvavo-Access-Key", api_key)
            .header("Bitvavo-Access-Signature", signature)
            .header("Bitvavo-Access-Timestamp", timestamp)
            .header("Bitvavo-Access-Window", "10000")
            .header("Content-Type", "application/json");
        if !body_text.is_empty() && body_text != "null" {
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

    if !status.is_success() || value.get("error").is_some() {
        let code = value.get("errorCode").and_then(value_as_i64);
        let message = error_message(&value).unwrap_or("Bitvavo request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bitvavo_error(code, message),
            message,
            Utc::now(),
        );
        error.code = code
            .map(|code| code.to_string())
            .or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    Ok(value)
}

fn classify_bitvavo_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("market") || msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if code == Some(309) || msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if msg.contains("precision") || msg.contains("tick") {
        ExchangeErrorClass::InvalidPrecision
    } else if msg.contains("minimum") || msg.contains("notional") {
        ExchangeErrorClass::MinNotionalViolation
    } else if msg.contains("order") && msg.contains("not found") {
        ExchangeErrorClass::OrderNotFound
    } else if code == Some(101) || msg.contains("auth") || msg.contains("key") {
        ExchangeErrorClass::Authentication
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("error")
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
        build_path(endpoint, params)
    )
}

fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut path = endpoint.to_string();
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        let query = pairs
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join("&");
        path.push('?');
        path.push_str(&query);
    }
    path
}
