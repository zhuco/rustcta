use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{sign_request, BitgetPrivateCredentials};

#[derive(Clone)]
pub struct BitgetRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<BitgetPrivateCredentials>,
}

impl BitgetRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<BitgetPrivateCredentials>,
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
        operation: &'static str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let request_path = build_request_path(endpoint, params);
        let timestamp = Utc::now().timestamp_millis().to_string();
        let signature = sign_request(
            &credentials.api_secret,
            &timestamp,
            "GET",
            &request_path,
            "",
        );
        let response = self
            .http
            .get(format!(
                "{}{}",
                self.rest_base_url.trim_end_matches('/'),
                request_path
            ))
            .header("ACCESS-KEY", &credentials.api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", timestamp)
            .header("ACCESS-PASSPHRASE", &credentials.passphrase)
            .header("locale", "en-US")
            .header("Content-Type", "application/json")
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
    let code = value.get("code").and_then(Value::as_str);
    if !status.is_success() || code.is_some_and(|code| code != "00000") {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Bitget request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bitget_error(code, message),
            message,
            Utc::now(),
        );
        error.code = code.map(ToOwned::to_owned);
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_bitget_error(code: Option<&str>, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match code {
        "40014" | "40015" | "401" | "40100" => ExchangeErrorClass::Authentication,
        "40034" | "43011" => ExchangeErrorClass::InvalidSymbol,
        "40017" | "43028" | "43035" => ExchangeErrorClass::InvalidPrecision,
        "40725" | "43012" => ExchangeErrorClass::InsufficientBalance,
        "40786" | "43001" => ExchangeErrorClass::OrderNotFound,
        "429" => ExchangeErrorClass::RateLimited,
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ if msg.contains("precision") || msg.contains("size") => {
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
