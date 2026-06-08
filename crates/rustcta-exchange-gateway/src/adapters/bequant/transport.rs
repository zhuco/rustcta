use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::basic_auth_value;

#[derive(Clone)]
pub struct BequantRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BequantRest {
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
        self.send_request(Method::GET, endpoint, params, None, None)
            .await
    }

    pub async fn send_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::GET,
            endpoint,
            params,
            None,
            Some((api_key, api_secret)),
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        form: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::POST,
            endpoint,
            params,
            Some(form),
            Some((api_key, api_secret)),
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::DELETE,
            endpoint,
            params,
            None,
            Some((api_key, api_secret)),
        )
        .await
    }

    async fn send_request(
        &self,
        method: Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        form: Option<&HashMap<String, String>>,
        credentials: Option<(&str, &str)>,
    ) -> ExchangeApiResult<Value> {
        let path = build_path(endpoint, params);
        let mut request = self
            .http
            .request(
                method,
                format!("{}{}", self.rest_base_url.trim_end_matches('/'), path),
            )
            .header("Accept", "application/json");
        if let Some((api_key, api_secret)) = credentials {
            request = request.header("Authorization", basic_auth_value(api_key, api_secret));
        }
        if let Some(form) = form {
            request = request.form(form);
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
        Value::Null
    } else {
        serde_json::from_str::<Value>(&text).map_err(|error| ExchangeApiError::Transport {
            message: format!("invalid Bequant JSON response: {error}: {text}"),
        })?
    };
    if !status.is_success() || value.get("error").is_some() {
        let (code, message) = error_code_message(status.as_u16(), &value);
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bequant_error(status.as_u16(), &message),
            message,
            Utc::now(),
        );
        error.code = Some(code);
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn error_code_message(status: u16, value: &Value) -> (String, String) {
    if let Some(error) = value.get("error") {
        let code = error
            .get("code")
            .and_then(|value| match value {
                Value::String(text) => Some(text.clone()),
                Value::Number(number) => Some(number.to_string()),
                _ => None,
            })
            .unwrap_or_else(|| status.to_string());
        let message = error
            .get("message")
            .and_then(Value::as_str)
            .or_else(|| error.get("description").and_then(Value::as_str))
            .unwrap_or("Bequant request failed")
            .to_string();
        return (code, message);
    }
    let message = value
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Bequant request failed")
        .to_string();
    (status.to_string(), message)
}

fn classify_bequant_error(status: u16, message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    if status == 401 || status == 403 || message.contains("auth") {
        ExchangeErrorClass::Authentication
    } else if status == 429 || message.contains("rate") || message.contains("limit") {
        ExchangeErrorClass::RateLimited
    } else if message.contains("insufficient") || message.contains("balance") {
        ExchangeErrorClass::InsufficientBalance
    } else if message.contains("not found") || message.contains("unknown order") {
        ExchangeErrorClass::OrderNotFound
    } else if message.contains("symbol") || message.contains("currency") {
        ExchangeErrorClass::InvalidSymbol
    } else if status == 400 {
        ExchangeErrorClass::InvalidRequest
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut path = endpoint.to_string();
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        path.push('?');
        path.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}={}",
                        urlencoding::encode(key),
                        urlencoding::encode(value)
                    )
                })
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    path
}
