use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{
    backpack_signature, canonical_batch_payload, canonical_signing_payload,
    json_object_to_string_map, BackpackPrivateCredentials,
};

#[derive(Clone)]
pub struct BackpackRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<BackpackPrivateCredentials>,
    window_ms: u64,
}

impl BackpackRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        window_ms: u64,
        credentials: Option<BackpackPrivateCredentials>,
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
            window_ms,
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
        endpoint: &str,
        instruction: &'static str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(Method::GET, endpoint, instruction, params, None)
            .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        instruction: &'static str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        let fields = json_object_to_string_map(body)?;
        self.send_signed_request(Method::POST, endpoint, instruction, &fields, Some(body))
            .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        instruction: &'static str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        let fields = json_object_to_string_map(body)?;
        self.send_signed_request(Method::DELETE, endpoint, instruction, &fields, Some(body))
            .await
    }

    pub async fn send_signed_batch_post(
        &self,
        endpoint: &str,
        instruction: &'static str,
        orders: &[Value],
    ) -> ExchangeApiResult<Value> {
        let credentials = self.private_credentials("backpack.batch_private_rest")?;
        let timestamp_ms = Utc::now().timestamp_millis();
        let order_fields = orders
            .iter()
            .map(json_object_to_string_map)
            .collect::<ExchangeApiResult<Vec<_>>>()?;
        let payload =
            canonical_batch_payload(instruction, &order_fields, timestamp_ms, self.window_ms);
        let signature = backpack_signature(&credentials.api_secret, &payload)?;
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let response = self
            .http
            .post(url)
            .header("Content-Type", "application/json")
            .header("X-API-Key", &credentials.api_key)
            .header("X-Signature", signature)
            .header("X-Timestamp", timestamp_ms.to_string())
            .header("X-Window", self.window_ms.to_string())
            .json(orders)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    async fn send_signed_request(
        &self,
        method: Method,
        endpoint: &str,
        instruction: &'static str,
        fields: &HashMap<String, String>,
        body: Option<&Value>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self.private_credentials(instruction)?;
        let timestamp_ms = Utc::now().timestamp_millis();
        let signing_payload =
            canonical_signing_payload(instruction, fields, timestamp_ms, self.window_ms);
        let signature = backpack_signature(&credentials.api_secret, &signing_payload)?;
        let params = if body.is_none() {
            fields
        } else {
            &HashMap::new()
        };
        let url = build_url(&self.rest_base_url, endpoint, params);
        let mut builder = self
            .http
            .request(method, url)
            .header("Content-Type", "application/json")
            .header("X-API-Key", &credentials.api_key)
            .header("X-Signature", signature)
            .header("X-Timestamp", timestamp_ms.to_string())
            .header("X-Window", self.window_ms.to_string());
        if let Some(body) = body {
            builder = builder.json(body);
        }
        let response = builder
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    fn private_credentials(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<&BackpackPrivateCredentials> {
        self.credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported { operation })
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
    if !status.is_success() || value.get("code").is_some() || value.get("error").is_some() {
        let message = value
            .get("message")
            .or_else(|| value.get("msg"))
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Backpack request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_backpack_error(status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = value.get("code").map(|code| {
            code.as_str()
                .map(ToString::to_string)
                .unwrap_or_else(|| code.to_string())
        });
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_backpack_error(status: u16, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    match status {
        401 | 403 => ExchangeErrorClass::Authentication,
        404 if msg.contains("order") => ExchangeErrorClass::OrderNotFound,
        404 => ExchangeErrorClass::InvalidSymbol,
        418 | 429 => ExchangeErrorClass::RateLimited,
        500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ if msg.contains("signature")
            || msg.contains("api key")
            || msg.contains("unauthorized") =>
        {
            ExchangeErrorClass::Authentication
        }
        _ if msg.contains("permission") || msg.contains("forbidden") => {
            ExchangeErrorClass::Permission
        }
        _ if msg.contains("insufficient") || msg.contains("balance") => {
            ExchangeErrorClass::InsufficientBalance
        }
        _ if msg.contains("duplicate") || msg.contains("client") && msg.contains("exists") => {
            ExchangeErrorClass::DuplicateClientOrderId
        }
        _ if msg.contains("precision") || msg.contains("tick") || msg.contains("step") => {
            ExchangeErrorClass::InvalidPrecision
        }
        _ if msg.contains("symbol") || msg.contains("market") => ExchangeErrorClass::InvalidSymbol,
        _ if msg.contains("rate") || msg.contains("limit") => ExchangeErrorClass::RateLimited,
        _ => ExchangeErrorClass::Unknown,
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
    let mut url = endpoint.to_string();
    if !params.is_empty() {
        url.push(if endpoint.contains('?') { '&' } else { '?' });
        url.push_str(&build_query_string(params));
    }
    url
}

fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={}", urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}
