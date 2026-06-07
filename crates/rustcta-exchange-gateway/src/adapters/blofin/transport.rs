use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;
use uuid::Uuid;

use super::signing::sign_request;

#[derive(Clone)]
pub struct BlofinRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BlofinRest {
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

    pub async fn send_public_get(
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
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
            passphrase,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Value,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            Some(body),
            api_key,
            api_secret,
            passphrase,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<Value>,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let nonce = Uuid::new_v4().to_string();
        let request_path = request_path(endpoint, params);
        let body_text = body
            .as_ref()
            .map(|body| body.to_string())
            .unwrap_or_default();
        let signature = sign_request(
            api_secret,
            &request_path,
            method.as_str(),
            &timestamp,
            &nonce,
            &body_text,
        )?;
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
            .header("ACCESS-KEY", api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", timestamp)
            .header("ACCESS-NONCE", nonce)
            .header("ACCESS-PASSPHRASE", passphrase)
            .header("Content-Type", "application/json");
        if body.is_some() {
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
    let code = value.get("code").and_then(value_as_code);
    if !status.is_success() || code.as_deref().is_some_and(|code| code != "0") {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("BloFin request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_blofin_error(code.as_deref(), status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_blofin_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match (status, code) {
        (401 | 403, _) | (_, "40001" | "40002" | "40003" | "40004") => {
            ExchangeErrorClass::Authentication
        }
        (418 | 429, _) | (_, "50011") => ExchangeErrorClass::RateLimited,
        (500..=599, _) => ExchangeErrorClass::ExchangeUnavailable,
        (_, "1000" | "1001" | "51000") => ExchangeErrorClass::InvalidRequest,
        (_, "51001") => ExchangeErrorClass::InvalidSymbol,
        (_, "51008") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("duplicate") => ExchangeErrorClass::DuplicateClientOrderId,
        _ if msg.contains("not exist") || msg.contains("not found") => {
            ExchangeErrorClass::OrderNotFound
        }
        _ if msg.contains("rate") || msg.contains("too many") => ExchangeErrorClass::RateLimited,
        _ if msg.contains("symbol") || msg.contains("instrument") => {
            ExchangeErrorClass::InvalidSymbol
        }
        _ if msg.contains("precision") || msg.contains("tick") || msg.contains("lot") => {
            ExchangeErrorClass::InvalidPrecision
        }
        _ => ExchangeErrorClass::Unknown,
    }
}

fn value_as_code(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        request_path(endpoint, params)
    )
}

pub fn request_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut path = endpoint.to_string();
    if !params.is_empty() {
        path.push('?');
        path.push_str(&url_query_string(params));
    }
    path
}

pub fn url_query_string(params: &HashMap<String, String>) -> String {
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
