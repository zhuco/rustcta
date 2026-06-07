use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{sign_request, WooPrivateCredentials};

#[derive(Clone)]
pub struct WooRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<WooPrivateCredentials>,
}

impl WooRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<WooPrivateCredentials>,
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

    pub async fn send_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let request_path = build_request_path(endpoint, params);
        let timestamp = Utc::now().timestamp_millis().to_string();
        let signature = sign_request(
            &credentials.api_secret,
            &timestamp,
            "GET",
            &request_path,
            "",
        );
        let response = self
            .http
            .get(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                request_path
            ))
            .header("x-api-key", &credentials.api_key)
            .header("x-api-signature", signature)
            .header("x-api-timestamp", timestamp)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_body(operation, "POST", endpoint, body)
            .await
    }

    pub async fn send_signed_put(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_body(operation, "PUT", endpoint, body)
            .await
    }

    pub async fn get_listen_key(&self) -> ExchangeApiResult<String> {
        let value = self
            .send_signed_post(
                "woo.get_listen_key",
                "/v3/account/listenKey",
                &serde_json::json!({ "type": "WEBSOCKET" }),
            )
            .await?;
        value
            .get("data")
            .and_then(|data| data.get("authKey"))
            .and_then(Value::as_str)
            .filter(|auth_key| !auth_key.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| {
                ExchangeApiError::Exchange(ExchangeError {
                    schema_version: rustcta_types::SchemaVersion::current(),
                    exchange_id: self.exchange_id.clone(),
                    class: ExchangeErrorClass::Decode,
                    code: None,
                    message: format!("woo listenKey response missing authKey: {value}"),
                    retry_after_ms: None,
                    order_id: None,
                    client_order_id: None,
                    raw: Some(value),
                    occurred_at: Utc::now(),
                })
            })
    }

    async fn send_signed_body(
        &self,
        operation: &'static str,
        method: &str,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let body_text =
            serde_json::to_string(body).map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?;
        let timestamp = Utc::now().timestamp_millis().to_string();
        let signature = sign_request(
            &credentials.api_secret,
            &timestamp,
            method,
            endpoint,
            &body_text,
        );
        let builder = match method {
            "PUT" => self.http.put(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                endpoint
            )),
            _ => self.http.post(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                endpoint
            )),
        };
        let response = builder
            .header("x-api-key", &credentials.api_key)
            .header("x-api-signature", signature)
            .header("x-api-timestamp", timestamp)
            .header("Content-Type", "application/json")
            .body(body_text)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_delete(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let request_path = build_request_path(endpoint, params);
        let timestamp = Utc::now().timestamp_millis().to_string();
        let signature = sign_request(
            &credentials.api_secret,
            &timestamp,
            "DELETE",
            &request_path,
            "",
        );
        let response = self
            .http
            .delete(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                request_path
            ))
            .header("x-api-key", &credentials.api_key)
            .header("x-api-signature", signature)
            .header("x-api-timestamp", timestamp)
            .header("Content-Type", "application/x-www-form-urlencoded")
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
    let success = value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(status.is_success());
    if !status.is_success() || !success {
        let code = value
            .get("code")
            .or_else(|| value.get("errorCode"))
            .and_then(value_text);
        let message = value
            .get("message")
            .or_else(|| value.get("msg"))
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("WOO request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_woo_error(code.as_deref(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_woo_error(code: Option<&str>, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match code {
        "1002" | "1003" | "1004" | "1005" => ExchangeErrorClass::Authentication,
        "429" => ExchangeErrorClass::RateLimited,
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("not found") || msg.contains("unknown order") => {
            ExchangeErrorClass::OrderNotFound
        }
        _ if msg.contains("duplicate") => ExchangeErrorClass::DuplicateClientOrderId,
        _ if msg.contains("rate") || msg.contains("too many") => ExchangeErrorClass::RateLimited,
        _ if msg.contains("permission") || msg.contains("forbidden") => {
            ExchangeErrorClass::Permission
        }
        _ if msg.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ => ExchangeErrorClass::Unknown,
    }
}

pub(super) fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        build_request_path(endpoint, params)
    )
}

pub(super) fn build_request_path(endpoint: &str, params: &HashMap<String, String>) -> String {
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

fn value_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
