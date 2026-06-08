use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::signed_headers;

#[derive(Clone)]
pub struct CoinoneRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl CoinoneRest {
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

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        body: &Value,
        secret_key: &str,
    ) -> ExchangeApiResult<Value> {
        let (payload, signature) = signed_headers(secret_key, body)?;
        let response = self
            .http
            .post(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                endpoint
            ))
            .header("Content-Type", "application/json")
            .header("X-COINONE-PAYLOAD", payload)
            .header("X-COINONE-SIGNATURE", signature)
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
    let result = value.get("result").and_then(Value::as_str);
    let code = value.get("error_code").and_then(value_as_i64);
    let success = status.is_success()
        && result.is_none_or(|value| value.eq_ignore_ascii_case("success"))
        && code.is_none_or(|code| code == 0);
    if !success {
        let message = value
            .get("error_msg")
            .or_else(|| value.get("message"))
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Coinone request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_coinone_error(code, message),
            message,
            Utc::now(),
        );
        error.code = code.map(|code| code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_coinone_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    match code {
        Some(4) | Some(40) | Some(50) | Some(51) | Some(53) => ExchangeErrorClass::Authentication,
        Some(103) | Some(104) | Some(105) | Some(107) => ExchangeErrorClass::InvalidRequest,
        Some(112) | Some(113) | Some(114) => ExchangeErrorClass::InvalidSymbol,
        Some(141) | Some(151) => ExchangeErrorClass::InsufficientBalance,
        Some(429) | Some(4290) => ExchangeErrorClass::RateLimited,
        _ if message.contains("balance") || message.contains("insufficient") => {
            ExchangeErrorClass::InsufficientBalance
        }
        _ if message.contains("symbol") || message.contains("currency") => {
            ExchangeErrorClass::InvalidSymbol
        }
        _ if message.contains("rate") || message.contains("too many") => {
            ExchangeErrorClass::RateLimited
        }
        _ => ExchangeErrorClass::Unknown,
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
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

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
