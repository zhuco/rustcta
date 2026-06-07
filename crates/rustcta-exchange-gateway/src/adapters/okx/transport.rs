use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{okx_signature, okx_timestamp, OkxPrivateCredentials};

#[derive(Clone)]
pub struct OkxRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<OkxPrivateCredentials>,
}

impl OkxRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<OkxPrivateCredentials>,
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
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "okx.private_rest",
            })?;
        let request_path = build_request_path(endpoint, params);
        let url = format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            request_path
        );
        let timestamp = okx_timestamp();
        let signature = okx_signature(
            &credentials.api_secret,
            &timestamp,
            "GET",
            &request_path,
            "",
        );
        let response = self
            .http
            .get(url)
            .header("OK-ACCESS-KEY", &credentials.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &credentials.passphrase)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_post(&self, endpoint: &str, body: &Value) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "okx.private_rest",
            })?;
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let body_text =
            serde_json::to_string(body).map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?;
        let timestamp = okx_timestamp();
        let signature = okx_signature(
            &credentials.api_secret,
            &timestamp,
            "POST",
            endpoint,
            &body_text,
        );
        let response = self
            .http
            .post(url)
            .header("OK-ACCESS-KEY", &credentials.api_key)
            .header("OK-ACCESS-SIGN", signature)
            .header("OK-ACCESS-TIMESTAMP", timestamp)
            .header("OK-ACCESS-PASSPHRASE", &credentials.passphrase)
            .header("Content-Type", "application/json")
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
    let value = response
        .json::<Value>()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;

    if !status.is_success() {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("OKX request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_okx_error(value.get("code").and_then(Value::as_str), message),
            message,
            Utc::now(),
        );
        error.code = Some(status.as_u16().to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    let code = value
        .get("code")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if code != "0" {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("OKX API error");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_okx_error(Some(code), message),
            message,
            Utc::now(),
        );
        error.code = Some(code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }

    Ok(value
        .get("data")
        .cloned()
        .unwrap_or(Value::Array(Vec::new())))
}

fn classify_okx_error(code: Option<&str>, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match code {
        "51000" | "51001" | "51004" => ExchangeErrorClass::InvalidRequest,
        "51008" => ExchangeErrorClass::InsufficientBalance,
        "51011" | "51015" | "51020" => ExchangeErrorClass::InvalidPrecision,
        "51121" | "51122" => ExchangeErrorClass::InvalidSymbol,
        "51603" | "51604" => ExchangeErrorClass::OrderNotFound,
        "50101" | "50102" | "50103" | "50104" | "50105" => ExchangeErrorClass::Authentication,
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("instrument") || msg.contains("instid") || msg.contains("symbol") => {
            ExchangeErrorClass::InvalidSymbol
        }
        _ if msg.contains("precision") || msg.contains("size") || msg.contains("tick") => {
            ExchangeErrorClass::InvalidPrecision
        }
        _ if msg.contains("rate") || msg.contains("too many") => ExchangeErrorClass::RateLimited,
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
