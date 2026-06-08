use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_rest_request;

#[derive(Clone)]
pub struct BtcMarketsRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    api_key: Option<String>,
    api_secret: Option<String>,
}

impl BtcMarketsRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        api_key: Option<String>,
        api_secret: Option<String>,
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
            api_key,
            api_secret,
        })
    }

    pub async fn public_get(
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

    pub async fn signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.signed_request(reqwest::Method::GET, endpoint, params, None)
            .await
    }

    pub async fn signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        self.signed_request(reqwest::Method::POST, endpoint, params, Some(body))
            .await
    }

    pub async fn signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.signed_request(reqwest::Method::DELETE, endpoint, params, None)
            .await
    }

    async fn signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
    ) -> ExchangeApiResult<Value> {
        let api_key = self
            .api_key
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported {
                operation: "btcmarkets.private_rest_missing_api_key",
            })?;
        let api_secret = self
            .api_secret
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ExchangeApiError::Unsupported {
                operation: "btcmarkets.private_rest_missing_api_secret",
            })?;
        let path = build_path(endpoint, params);
        let timestamp = Utc::now().timestamp_millis().to_string();
        let body_text = body
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?
            .unwrap_or_default();
        let signed = sign_rest_request(
            api_key,
            api_secret,
            method.as_str(),
            &path,
            &timestamp,
            if body_text.is_empty() {
                None
            } else {
                Some(body_text.as_str())
            },
        )?;
        let mut request = self
            .http
            .request(
                method,
                format!("{}{}", self.rest_base_url.trim_end_matches('/'), path),
            )
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("BM-AUTH-APIKEY", signed.api_key)
            .header("BM-AUTH-TIMESTAMP", signed.timestamp)
            .header("BM-AUTH-SIGNATURE", signed.signature);
        if !body_text.is_empty() {
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
    if !status.is_success() {
        let code = value
            .get("code")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| Some(status.as_u16().to_string()));
        let message = value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("BTC Markets request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_error(code.as_deref(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_error(code: Option<&str>, message: &str) -> ExchangeErrorClass {
    let haystack = format!("{} {}", code.unwrap_or_default(), message).to_ascii_lowercase();
    if haystack.contains("invalidapikey") || haystack.contains("signature") {
        ExchangeErrorClass::Authentication
    } else if haystack.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if haystack.contains("market") || haystack.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if haystack.contains("precision") || haystack.contains("decimal") {
        ExchangeErrorClass::InvalidPrecision
    } else if haystack.contains("ordernotfound") || haystack.contains("not found") {
        ExchangeErrorClass::OrderNotFound
    } else if haystack.contains("rate") || haystack.contains("throttle") {
        ExchangeErrorClass::RateLimited
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        build_path(endpoint, params)
    )
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
                .map(|(key, value)| format!("{}={}", url_encode(key), url_encode(value)))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    path
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}
