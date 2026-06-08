use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use super::signing::bitstamp_signature;

#[derive(Clone)]
pub struct BitstampRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BitstampRest {
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

    pub async fn send_private_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        subaccount_id: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        let body = build_form_body(params);
        let content_type = "application/x-www-form-urlencoded";
        let nonce = Uuid::new_v4().to_string();
        let timestamp = Utc::now().timestamp_millis().to_string();
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let parsed = Url::parse(&url).map_err(validation_error)?;
        let host = parsed.host_str().unwrap_or_default();
        let signature = bitstamp_signature(
            api_key,
            api_secret,
            "POST",
            host,
            parsed.path(),
            parsed.query().unwrap_or_default(),
            content_type,
            &nonce,
            &timestamp,
            &body,
        )?;
        let mut request = self
            .http
            .post(url)
            .header("X-Auth", format!("BITSTAMP {api_key}"))
            .header("X-Auth-Signature", signature)
            .header("X-Auth-Nonce", nonce)
            .header("X-Auth-Timestamp", timestamp)
            .header("X-Auth-Version", "v2")
            .header("Content-Type", content_type)
            .body(body);
        if let Some(subaccount_id) = subaccount_id {
            request = request.header("X-Auth-Subaccount-Id", subaccount_id);
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
    if !status.is_success() || value.get("status").and_then(Value::as_str) == Some("error") {
        let code = value
            .get("response_code")
            .or_else(|| value.get("code"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let message = value
            .get("reason")
            .or_else(|| value.get("response_explanation"))
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Bitstamp request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bitstamp_error(code.as_deref(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_bitstamp_error(code: Option<&str>, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    match code.unwrap_or_default() {
        "400.009" => ExchangeErrorClass::InsufficientBalance,
        "400.019" => ExchangeErrorClass::DuplicateClientOrderId,
        "404.002" => ExchangeErrorClass::OrderNotFound,
        "404.003" | "404.007" => ExchangeErrorClass::InvalidSymbol,
        "400.002" | "400.067" | "400.068" => ExchangeErrorClass::RateLimited,
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("not found") => ExchangeErrorClass::OrderNotFound,
        _ if msg.contains("signature") || msg.contains("auth") => {
            ExchangeErrorClass::Authentication
        }
        _ => ExchangeErrorClass::Unknown,
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        url.push('?');
        url.push_str(&build_form_body(params));
    }
    url
}

pub fn build_form_body(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={}", urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn validation_error(error: impl std::fmt::Display) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: error.to_string(),
    }
}
