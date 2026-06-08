use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

#[derive(Clone)]
pub struct P2bRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl P2bRest {
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
            .get(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                build_path(endpoint, params)
            ))
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
    let success_flag = value.get("success").or_else(|| value.get("ok"));
    let failed_ok = matches!(success_flag, Some(Value::Bool(false)))
        || success_flag
            .and_then(Value::as_str)
            .is_some_and(|text| text.eq_ignore_ascii_case("false"));
    if !status.is_success() || failed_ok {
        let message = value
            .get("message")
            .or_else(|| value.get("error"))
            .or_else(|| value.get("errorCode"))
            .and_then(Value::as_str)
            .unwrap_or("P2B request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_p2b_error(message, status.as_u16()),
            message,
            Utc::now(),
        );
        error.code = value
            .get("errorCode")
            .or_else(|| value.get("code"))
            .and_then(value_to_string)
            .or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_p2b_error(message: &str, status: u16) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    if status == 401 || message.contains("auth") || message.contains("token") {
        ExchangeErrorClass::Authentication
    } else if status == 404 || message.contains("symbol") || message.contains("pair") {
        ExchangeErrorClass::InvalidSymbol
    } else if status == 429 || message.contains("rate") || message.contains("limit") {
        ExchangeErrorClass::RateLimited
    } else if message.contains("balance") || message.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
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

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}
