use std::collections::HashMap;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;
use tokio::sync::RwLock;

use super::signing::{authorization_query, DeribitPrivateCredentials};

#[derive(Clone)]
pub struct DeribitRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<DeribitPrivateCredentials>,
    token: std::sync::Arc<RwLock<Option<DeribitAccessToken>>>,
}

#[derive(Debug, Clone)]
struct DeribitAccessToken {
    token: String,
    expires_at_ms: i64,
}

impl DeribitRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<DeribitPrivateCredentials>,
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
            token: std::sync::Arc::new(RwLock::new(None)),
        })
    }

    pub async fn send_public_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_request(Method::GET, endpoint, params, None).await
    }

    pub async fn send_private_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let token = self.access_token(operation).await?;
        self.send_request(Method::GET, endpoint, params, Some(&token))
            .await
    }

    async fn access_token(&self, operation: &'static str) -> ExchangeApiResult<String> {
        if let Some(token) = self.valid_cached_token().await {
            return Ok(token);
        }
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let params = authorization_query(&credentials.client_id, &credentials.client_secret)
            .into_iter()
            .collect::<HashMap<_, _>>();
        let value = self
            .send_request(Method::GET, "/api/v2/public/auth", &params, None)
            .await?;
        let result = value.get("result").unwrap_or(&value);
        let token = result
            .get("access_token")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                self.decode_error("deribit auth response missing access_token", value.clone())
            })?
            .to_string();
        let expires_in_ms = result
            .get("expires_in")
            .and_then(value_as_i64)
            .unwrap_or(600_000);
        let expires_at_ms = Utc::now()
            .checked_add_signed(ChronoDuration::milliseconds(
                expires_in_ms.saturating_sub(30_000),
            ))
            .unwrap_or_else(Utc::now)
            .timestamp_millis();
        *self.token.write().await = Some(DeribitAccessToken {
            token: token.clone(),
            expires_at_ms,
        });
        Ok(token)
    }

    async fn valid_cached_token(&self) -> Option<String> {
        let token = self.token.read().await;
        let token = token.as_ref()?;
        (token.expires_at_ms > Utc::now().timestamp_millis()).then(|| token.token.clone())
    }

    async fn send_request(
        &self,
        method: Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        bearer_token: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        let request_path = build_request_path(endpoint, params);
        let mut request = self.http.request(
            method,
            format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                request_path
            ),
        );
        if let Some(token) = bearer_token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        let response = request
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    fn decode_error(&self, message: &str, raw: Value) -> ExchangeApiError {
        ExchangeApiError::Exchange(ExchangeError {
            schema_version: rustcta_types::SchemaVersion::current(),
            exchange_id: self.exchange_id.clone(),
            class: ExchangeErrorClass::Decode,
            code: None,
            message: message.to_string(),
            retry_after_ms: None,
            order_id: None,
            client_order_id: None,
            raw: Some(raw),
            occurred_at: Utc::now(),
        })
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
        let error = value.get("error").unwrap_or(&value);
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| value.get("message").and_then(Value::as_str))
            .unwrap_or("Deribit request failed");
        let code = error.get("code").and_then(value_text);
        let mut exchange_error = ExchangeError::new(
            exchange_id,
            classify_deribit_error(status.as_u16(), code.as_deref(), message),
            message,
            Utc::now(),
        );
        exchange_error.code = code.or_else(|| Some(status.as_u16().to_string()));
        exchange_error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(exchange_error));
    }
    Ok(value)
}

fn classify_deribit_error(status: u16, code: Option<&str>, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    let code = code.unwrap_or_default();
    if status == 401 || status == 403 || msg.contains("auth") || msg.contains("permission") {
        ExchangeErrorClass::Authentication
    } else if status == 429 || msg.contains("rate") {
        ExchangeErrorClass::RateLimited
    } else if code == "10001" || msg.contains("instrument") || msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if msg.contains("amount") || msg.contains("precision") {
        ExchangeErrorClass::InvalidPrecision
    } else if msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("order not found") || code == "10004" {
        ExchangeErrorClass::OrderNotFound
    } else if msg.contains("reject") {
        ExchangeErrorClass::OrderRejected
    } else {
        ExchangeErrorClass::Unknown
    }
}

pub fn build_request_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut path = endpoint.to_string();
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        path.push('?');
        path.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    path
}

fn value_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
        .or_else(|| value.as_f64().map(|value| value as i64))
        .or_else(|| value.as_str()?.parse::<i64>().ok())
}
