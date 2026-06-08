use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_request;

#[derive(Clone)]
pub struct BitflyerRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BitflyerRest {
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
                operation: "bitflyer.private_rest_missing_credentials",
            });
        }
        let request_path = build_path(endpoint, params);
        let timestamp = format!("{:.3}", Utc::now().timestamp_millis() as f64 / 1000.0);
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
            &request_path,
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
            .header("X-API-KEY", api_key)
            .header("X-API-TIMESTAMP", timestamp)
            .header("X-API-SIGN", signature)
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

async fn parse_response(
    exchange_id: ExchangeId,
    response: reqwest::Response,
) -> ExchangeApiResult<Value> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    let value = if text.trim().is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str::<Value>(&text).map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?
    };
    if !status.is_success() || is_bitflyer_error(&value) {
        let code = value
            .get("status")
            .and_then(value_as_i64)
            .or_else(|| Some(status.as_u16() as i64));
        let message = error_message(&value).unwrap_or("bitFlyer request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bitflyer_error(code, message),
            message,
            Utc::now(),
        );
        error.code = code.map(|code| code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

pub fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let endpoint = if endpoint.starts_with('/') {
        endpoint.to_string()
    } else {
        format!("/{endpoint}")
    };
    if params.is_empty() {
        endpoint
    } else {
        format!("{endpoint}?{}", encode_params(params))
    }
}

pub fn build_url(base_url: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base_url.trim_end_matches('/'),
        build_path(endpoint, params)
    )
}

fn encode_params(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn is_bitflyer_error(value: &Value) -> bool {
    value
        .get("status")
        .and_then(value_as_i64)
        .is_some_and(|status| status < 0)
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("error_message")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
}

fn classify_bitflyer_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if msg.contains("insufficient") || msg.contains("balance") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("permission") || msg.contains("scope") {
        ExchangeErrorClass::Permission
    } else if msg.contains("auth") || msg.contains("signature") || msg.contains("api key") {
        ExchangeErrorClass::Authentication
    } else if msg.contains("rate") || code == Some(-500) {
        ExchangeErrorClass::RateLimited
    } else if msg.contains("product") || msg.contains("order") || msg.contains("parameter") {
        ExchangeErrorClass::InvalidRequest
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
