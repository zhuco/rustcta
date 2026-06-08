use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId, SchemaVersion};
use serde_json::Value;

use super::signing::{sign_request, DeltaPrivateCredentials};

#[derive(Clone)]
pub struct DeltaRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<DeltaPrivateCredentials>,
}

impl DeltaRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<DeltaPrivateCredentials>,
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

    pub async fn send_public_get(
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
        let path = build_request_path(endpoint, params);
        self.send_signed_no_body(operation, "GET", &path).await
    }

    pub async fn send_signed_delete(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_body(operation, "DELETE", endpoint, body)
            .await
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

    async fn send_signed_no_body(
        &self,
        operation: &'static str,
        method: &str,
        request_path: &str,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let timestamp = Utc::now().timestamp().to_string();
        let signature = sign_request(
            &credentials.api_secret,
            method,
            &timestamp,
            request_path,
            "",
        );
        let response = self
            .http
            .request(
                method.parse().unwrap_or(reqwest::Method::GET),
                format!(
                    "{}{}",
                    self.rest_base_url.trim_end_matches('/'),
                    request_path
                ),
            )
            .header("api-key", &credentials.api_key)
            .header("signature", signature)
            .header("timestamp", timestamp)
            .header("Accept", "application/json")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
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
        let timestamp = Utc::now().timestamp().to_string();
        let signature = sign_request(
            &credentials.api_secret,
            method,
            &timestamp,
            endpoint,
            &body_text,
        );
        let response = self
            .http
            .request(
                method.parse().unwrap_or(reqwest::Method::POST),
                format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint),
            )
            .header("api-key", &credentials.api_key)
            .header("signature", signature)
            .header("timestamp", timestamp)
            .header("Content-Type", "application/json")
            .header("Accept", "application/json")
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
    let text = response
        .text()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    let value: Value = serde_json::from_str(&text).map_err(|error| {
        ExchangeApiError::Exchange(ExchangeError {
            schema_version: SchemaVersion::current(),
            exchange_id: exchange_id.clone(),
            class: ExchangeErrorClass::Decode,
            code: status.as_u16().to_string().into(),
            message: format!("delta response is not json: {error}; body={text}"),
            retry_after_ms: None,
            order_id: None,
            client_order_id: None,
            raw: None,
            occurred_at: Utc::now(),
        })
    })?;
    let success = value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| status.is_success());
    if status.is_success() && success {
        return Ok(value);
    }
    Err(ExchangeApiError::Exchange(exchange_error(
        exchange_id,
        status.as_u16(),
        &value,
    )))
}

fn exchange_error(exchange_id: ExchangeId, status: u16, value: &Value) -> ExchangeError {
    let error = value.get("error").unwrap_or(value);
    let code = error
        .get("code")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| Some(status.to_string()));
    let message = error
        .get("message")
        .or_else(|| error.get("code"))
        .and_then(Value::as_str)
        .unwrap_or("delta exchange error")
        .to_string();
    let class = match code.as_deref().unwrap_or_default() {
        "invalid_signature" | "api_key_not_found" | "unauthorized" => {
            ExchangeErrorClass::Authentication
        }
        "insufficient_margin" | "insufficient_balance" => ExchangeErrorClass::InsufficientBalance,
        "order_not_found" => ExchangeErrorClass::OrderNotFound,
        "rate_limit_exceeded" => ExchangeErrorClass::RateLimited,
        "product_not_found" | "invalid_product_id" => ExchangeErrorClass::InvalidSymbol,
        _ if status == 401 || status == 403 => ExchangeErrorClass::Authentication,
        _ if status == 429 => ExchangeErrorClass::RateLimited,
        _ if status >= 500 => ExchangeErrorClass::ExchangeUnavailable,
        _ => ExchangeErrorClass::InvalidRequest,
    };
    ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class,
        code,
        message,
        retry_after_ms: None,
        order_id: error
            .get("order_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        client_order_id: error
            .get("client_order_id")
            .and_then(Value::as_str)
            .map(str::to_string),
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        build_request_path(endpoint, params)
    )
}

pub(super) fn build_request_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    if params.is_empty() {
        return endpoint.to_string();
    }
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    let query = serializer.finish();
    format!("{endpoint}?{query}")
}
