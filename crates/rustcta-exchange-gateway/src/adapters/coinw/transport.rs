use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{coinw_futures_signature, coinw_spot_signature, CoinwPrivateCredentials};

#[derive(Clone)]
pub struct CoinwRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<CoinwPrivateCredentials>,
}

impl CoinwRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<CoinwPrivateCredentials>,
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

    #[allow(dead_code)]
    pub async fn send_signed_spot_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "coinw.private_rest",
            })?;
        let mut signed_params = params.clone();
        signed_params.insert("api_key".to_string(), credentials.api_key.clone());
        let signature = coinw_spot_signature(&signed_params, &credentials.api_secret);
        signed_params.insert("sign".to_string(), signature);
        let url = build_url(&self.rest_base_url, endpoint, &signed_params);
        let response = self
            .http
            .post(url)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_futures_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_futures_request(reqwest::Method::GET, endpoint, params, None)
            .await
    }

    pub async fn send_signed_futures_post(
        &self,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_futures_request(
            reqwest::Method::POST,
            endpoint,
            &HashMap::new(),
            Some(body),
        )
        .await
    }

    pub async fn send_signed_futures_delete(
        &self,
        endpoint: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_futures_request(
            reqwest::Method::DELETE,
            endpoint,
            &HashMap::new(),
            Some(body),
        )
        .await
    }

    async fn send_signed_futures_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "coinw.private_rest",
            })?;
        let timestamp_ms = Utc::now().timestamp_millis();
        let path_with_query = build_request_path(endpoint, params);
        let body_text = if let Some(body) = body {
            serde_json::to_string(body).map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?
        } else {
            String::new()
        };
        let signature = coinw_futures_signature(
            &credentials.api_secret,
            timestamp_ms,
            method.as_str(),
            &path_with_query,
            &body_text,
        );
        let url = format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            path_with_query
        );
        let mut builder = self
            .http
            .request(method, url)
            .header("api_key", &credentials.api_key)
            .header("timestamp", timestamp_ms.to_string())
            .header("sign", signature)
            .header("Content-Type", "application/json");
        if !body_text.is_empty() {
            builder = builder.body(body_text);
        }
        let response = builder
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
    let failed_flag = value
        .get("failed")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let success_code = matches!(code.as_deref(), None | Some("0") | Some("200"));
    if !status.is_success() || failed_flag || !success_code {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("CoinW request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_coinw_error(code.as_deref(), status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_coinw_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match status {
        401 | 403 => ExchangeErrorClass::Authentication,
        418 | 429 => ExchangeErrorClass::RateLimited,
        500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ if code == "29001" => ExchangeErrorClass::RateLimited,
        _ if msg.contains("api key") || msg.contains("signature") || msg.contains("sign") => {
            ExchangeErrorClass::Authentication
        }
        _ if msg.contains("permission") || msg.contains("forbidden") => {
            ExchangeErrorClass::Permission
        }
        _ if msg.contains("insufficient") || msg.contains("balance") => {
            ExchangeErrorClass::InsufficientBalance
        }
        _ if msg.contains("order") && msg.contains("not") => ExchangeErrorClass::OrderNotFound,
        _ if msg.contains("symbol") || msg.contains("instrument") || msg.contains("pair") => {
            ExchangeErrorClass::InvalidSymbol
        }
        _ if msg.contains("precision") => ExchangeErrorClass::InvalidPrecision,
        _ if msg.contains("frequency") || msg.contains("frequently") || msg.contains("rate") => {
            ExchangeErrorClass::RateLimited
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
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
