use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{deepcoin_signature, deepcoin_timestamp, DeepcoinPrivateCredentials};

#[derive(Clone)]
pub struct DeepcoinRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<DeepcoinPrivateCredentials>,
}

impl DeepcoinRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<DeepcoinPrivateCredentials>,
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
        let url = format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            build_request_path(endpoint, params)
        );
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
                operation: "deepcoin.private_rest",
            })?;
        let request_path = build_request_path(endpoint, params);
        let url = format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            request_path
        );
        let timestamp = deepcoin_timestamp();
        let signature = deepcoin_signature(
            &credentials.api_secret,
            &timestamp,
            "GET",
            &request_path,
            "",
        );
        let response = self
            .http
            .get(url)
            .header("DC-ACCESS-KEY", &credentials.api_key)
            .header("DC-ACCESS-SIGN", signature)
            .header("DC-ACCESS-TIMESTAMP", timestamp)
            .header("DC-ACCESS-PASSPHRASE", &credentials.passphrase)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_get_body(
        &self,
        endpoint: &str,
        body_text: &str,
        content_type: &str,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "deepcoin.private_rest",
            })?;
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let timestamp = deepcoin_timestamp();
        let signature = deepcoin_signature(
            &credentials.api_secret,
            &timestamp,
            "GET",
            endpoint,
            body_text,
        );
        let response = self
            .http
            .get(url)
            .header("DC-ACCESS-KEY", &credentials.api_key)
            .header("DC-ACCESS-SIGN", signature)
            .header("DC-ACCESS-TIMESTAMP", timestamp)
            .header("DC-ACCESS-PASSPHRASE", &credentials.passphrase)
            .header("Content-Type", content_type)
            .body(body_text.to_string())
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
                operation: "deepcoin.private_rest",
            })?;
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let body_text =
            serde_json::to_string(body).map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?;
        let timestamp = deepcoin_timestamp();
        let signature = deepcoin_signature(
            &credentials.api_secret,
            &timestamp,
            "POST",
            endpoint,
            &body_text,
        );
        let response = self
            .http
            .post(url)
            .header("DC-ACCESS-KEY", &credentials.api_key)
            .header("DC-ACCESS-SIGN", signature)
            .header("DC-ACCESS-TIMESTAMP", timestamp)
            .header("DC-ACCESS-PASSPHRASE", &credentials.passphrase)
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
    let code = value.get("code").and_then(value_as_code);
    if !status.is_success() || code.as_deref().is_some_and(|code| code != "0") {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Deepcoin request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_deepcoin_error(code.as_deref(), status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code.or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_deepcoin_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match status {
        401 => ExchangeErrorClass::Authentication,
        403 => ExchangeErrorClass::Permission,
        418 | 429 => ExchangeErrorClass::RateLimited,
        500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ if matches!(code, "401" | "50101" | "50102" | "50103" | "50104") => {
            ExchangeErrorClass::Authentication
        }
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("duplicate") => ExchangeErrorClass::DuplicateClientOrderId,
        _ if msg.contains("not found")
            || msg.contains("not exist")
            || msg.contains("does not exist") =>
        {
            ExchangeErrorClass::OrderNotFound
        }
        _ if msg.contains("symbol") || msg.contains("instrument") || msg.contains("instid") => {
            ExchangeErrorClass::InvalidSymbol
        }
        _ if msg.contains("precision") || msg.contains("tick") || msg.contains("minimum") => {
            ExchangeErrorClass::InvalidPrecision
        }
        _ if msg.contains("rate") || msg.contains("too many") => ExchangeErrorClass::RateLimited,
        _ if status == 400 => ExchangeErrorClass::InvalidRequest,
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

pub(super) fn build_request_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut request_path = endpoint.to_string();
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        request_path.push('?');
        request_path.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    request_path
}
