use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::parser::exchange_error;
use super::signing::signed_form;

#[derive(Clone)]
pub struct Bit2cRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl Bit2cRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
    ) -> ExchangeApiResult<Self> {
        let http = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(8)
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

    pub async fn send_signed_get(
        &self,
        endpoint: &str,
        params: &[(impl AsRef<str>, impl AsRef<str>)],
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let nonce = Utc::now().timestamp_millis() as u64;
        let signed = signed_form(api_secret, params, nonce)?;
        let response = self
            .http
            .get(format!(
                "{}{}?{}",
                self.rest_base_url.trim_end_matches('/'),
                endpoint,
                signed.encoded_body
            ))
            .header("key", api_key)
            .header("sign", signed.signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
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
        params: &[(impl AsRef<str>, impl AsRef<str>)],
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let nonce = Utc::now().timestamp_millis() as u64;
        let signed = signed_form(api_secret, params, nonce)?;
        let response = self
            .http
            .post(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                endpoint
            ))
            .header("key", api_key)
            .header("sign", signed.signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(signed.encoded_body)
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
            .get("error")
            .or_else(|| value.get("Error"))
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Bit2C request failed")
            .to_string();
        return Err(exchange_error(exchange_id, message, value));
    }
    if let Some(message) = value
        .get("error")
        .or_else(|| value.get("Error"))
        .and_then(Value::as_str)
        .map(str::to_string)
    {
        return Err(exchange_error(exchange_id, message, value));
    }
    if value
        .get("HasError")
        .and_then(Value::as_bool)
        .is_some_and(|has_error| has_error)
    {
        let message = value
            .get("Error")
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Bit2C request returned HasError")
            .to_string();
        let mut error = ExchangeError::new(
            exchange_id,
            ExchangeErrorClass::Unknown,
            message,
            Utc::now(),
        );
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
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
