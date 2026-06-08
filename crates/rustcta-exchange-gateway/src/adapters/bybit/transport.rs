use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_rest_payload;

#[derive(Clone)]
pub struct BybitRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    recv_window_ms: u64,
    http: reqwest::Client,
}

impl BybitRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        recv_window_ms: u64,
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
            recv_window_ms,
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

    pub async fn send_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let payload = build_query_string(params);
        let timestamp = Utc::now().timestamp_millis().to_string();
        let recv_window = self.recv_window_ms.to_string();
        let signature = sign_rest_payload(api_secret, &timestamp, api_key, &recv_window, &payload)?;
        let url = build_url(&self.rest_base_url, endpoint, params);
        let response = self
            .http
            .get(url)
            .headers(signed_headers(
                api_key,
                &signature,
                &timestamp,
                &recv_window,
            )?)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_post_json(
        &self,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let payload = compact_json(body)?;
        let timestamp = Utc::now().timestamp_millis().to_string();
        let recv_window = self.recv_window_ms.to_string();
        let signature = sign_rest_payload(api_secret, &timestamp, api_key, &recv_window, &payload)?;
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let response = self
            .http
            .post(url)
            .headers(signed_headers(
                api_key,
                &signature,
                &timestamp,
                &recv_window,
            )?)
            .body(payload)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }
}

fn signed_headers(
    api_key: &str,
    signature: &str,
    timestamp: &str,
    recv_window: &str,
) -> ExchangeApiResult<reqwest::header::HeaderMap> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert("X-BAPI-API-KEY", api_key.parse().map_err(header_error)?);
    headers.insert("X-BAPI-SIGN", signature.parse().map_err(header_error)?);
    headers.insert("X-BAPI-SIGN-TYPE", "2".parse().map_err(header_error)?);
    headers.insert("X-BAPI-TIMESTAMP", timestamp.parse().map_err(header_error)?);
    headers.insert(
        "X-BAPI-RECV-WINDOW",
        recv_window.parse().map_err(header_error)?,
    );
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        "application/json".parse().map_err(header_error)?,
    );
    Ok(headers)
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
    let ret_code = value.get("retCode").and_then(Value::as_i64).unwrap_or(0);
    if !status.is_success() || ret_code != 0 {
        let message = value
            .get("retMsg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Bybit request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bybit_error(ret_code, message),
            message,
            Utc::now(),
        );
        error.code = Some(ret_code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_bybit_error(code: i64, message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    if matches!(code, 10003 | 10004 | 10005) || message.contains("sign") {
        ExchangeErrorClass::Authentication
    } else if matches!(code, 10006) || message.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if message.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if message.contains("order not exists") || message.contains("not found") {
        ExchangeErrorClass::OrderNotFound
    } else if message.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else {
        ExchangeErrorClass::Unknown
    }
}

pub fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    let query = build_query_string(params);
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query);
    }
    url
}

pub fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn compact_json(value: &Value) -> ExchangeApiResult<String> {
    serde_json::to_string(value).map_err(|error| ExchangeApiError::InvalidRequest {
        message: format!("failed to serialize Bybit JSON body: {error}"),
    })
}

fn header_error(error: reqwest::header::InvalidHeaderValue) -> ExchangeApiError {
    ExchangeApiError::InvalidRequest {
        message: format!("invalid Bybit header: {error}"),
    }
}
