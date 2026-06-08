use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_rest_request;

#[derive(Clone)]
pub struct BydfiRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BydfiRest {
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
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        body: Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            &HashMap::new(),
            Some(body),
            api_key,
            api_secret,
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
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let query_string = query_string(params);
        let body_text = body
            .as_ref()
            .map(|body| body.to_string())
            .unwrap_or_default();
        let signature =
            sign_rest_request(api_key, api_secret, &timestamp, &query_string, &body_text)?;
        let mut request = self
            .http
            .request(method, build_url(&self.rest_base_url, endpoint, params))
            .header("X-API-KEY", api_key)
            .header("X-API-TIMESTAMP", timestamp)
            .header("X-API-SIGNATURE", signature)
            .header("Accept-Language", "en-US")
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
    let success = value.get("success").and_then(Value::as_bool);
    let code_is_error = code
        .as_deref()
        .is_some_and(|code| code != "0" && code != "200");
    if !status.is_success() || code_is_error || success == Some(false) {
        let message = value
            .get("message")
            .or_else(|| value.get("msg"))
            .and_then(Value::as_str)
            .unwrap_or("BYDFi request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bydfi_error(code.as_deref(), status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_bydfi_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let message = message.to_ascii_lowercase();
    match (status, code) {
        (401 | 403, _) | (_, "401" | "403") => ExchangeErrorClass::Authentication,
        (510 | 429, _) | (_, "510" | "429") => ExchangeErrorClass::RateLimited,
        (511, _) | (_, "511") => ExchangeErrorClass::Permission,
        (513 | 600, _) | (_, "513" | "600") => ExchangeErrorClass::InvalidRequest,
        (514, _) | (_, "514") => ExchangeErrorClass::DuplicateClientOrderId,
        (500..=599, _) => ExchangeErrorClass::ExchangeUnavailable,
        _ if message.contains("api key") || message.contains("signature") => {
            ExchangeErrorClass::Authentication
        }
        _ if message.contains("too frequent") || message.contains("rate") => {
            ExchangeErrorClass::RateLimited
        }
        _ if message.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if message.contains("duplicate") => ExchangeErrorClass::DuplicateClientOrderId,
        _ if message.contains("order") && message.contains("not") => {
            ExchangeErrorClass::OrderNotFound
        }
        _ if message.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
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
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        url.push('?');
        url.push_str(&query_string(params));
    }
    url
}

pub fn query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
