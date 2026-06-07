use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{build_query_string, sign_query};

#[derive(Clone)]
pub struct ToobitRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl ToobitRest {
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

    pub async fn send_public_request(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(build_url(&self.rest_base_url, endpoint, params, None))
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
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_signed_put(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::PUT,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        let mut params = params.clone();
        params.insert(
            "recvWindow".to_string(),
            recv_window_ms.min(60_000).to_string(),
        );
        params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        let query = build_query_string(&params);
        let signature = sign_query(api_secret, &query)?;
        let response = self
            .http
            .request(
                method,
                build_url(&self.rest_base_url, endpoint, &params, Some(&signature)),
            )
            .header("X-BB-APIKEY", api_key)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }
}

pub(super) fn build_url(
    base: &str,
    endpoint: &str,
    params: &HashMap<String, String>,
    signature: Option<&str>,
) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    let query = build_query_string(params);
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query);
    }
    if let Some(signature) = signature {
        url.push(if query.is_empty() { '?' } else { '&' });
        url.push_str("signature=");
        url.push_str(signature);
    }
    url
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
    let code = value.get("code").and_then(value_as_i64);
    if !status.is_success() || code.is_some_and(|code| code != 0 && code != 200) {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Toobit request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_toobit_error(code, status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code.map(|code| code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_toobit_error(code: Option<i64>, status: u16, message: &str) -> ExchangeErrorClass {
    let lower = message.to_ascii_lowercase();
    match (code, status) {
        (_, 401 | 403) => ExchangeErrorClass::Authentication,
        (_, 429) => ExchangeErrorClass::RateLimited,
        (_, 500..=599) => ExchangeErrorClass::ExchangeUnavailable,
        (Some(-2013), _) => ExchangeErrorClass::OrderNotFound,
        (Some(-2010), _) => ExchangeErrorClass::InsufficientBalance,
        (Some(-1013), _) => ExchangeErrorClass::MinNotionalViolation,
        (Some(-1021) | Some(-1022), _) => ExchangeErrorClass::Authentication,
        _ if lower.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if lower.contains("not found") || lower.contains("unknown order") => {
            ExchangeErrorClass::OrderNotFound
        }
        _ if lower.contains("too many") || lower.contains("rate") => {
            ExchangeErrorClass::RateLimited
        }
        _ if lower.contains("precision") => ExchangeErrorClass::InvalidPrecision,
        _ if lower.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ => ExchangeErrorClass::Unknown,
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.parse::<i64>().ok())
}
