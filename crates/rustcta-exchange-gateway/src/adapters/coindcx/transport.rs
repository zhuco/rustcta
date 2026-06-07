use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::{coindcx_signature, CoinDcxPrivateCredentials};

#[derive(Clone)]
pub struct CoinDcxRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    futures_rest_base_url: String,
    public_rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<CoinDcxPrivateCredentials>,
}

impl CoinDcxRest {
    pub fn new(
        exchange_id: ExchangeId,
        spot_rest_base_url: String,
        futures_rest_base_url: String,
        public_rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<CoinDcxPrivateCredentials>,
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
            spot_rest_base_url,
            futures_rest_base_url,
            public_rest_base_url,
            http,
            credentials,
        })
    }

    pub async fn send_spot_public_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_public_get(&self.spot_rest_base_url, endpoint, params)
            .await
    }

    pub async fn send_futures_public_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_public_get(&self.futures_rest_base_url, endpoint, params)
            .await
    }

    pub async fn send_public_market_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_public_get(&self.public_rest_base_url, endpoint, params)
            .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        mut body: Value,
    ) -> ExchangeApiResult<Value> {
        let credentials = self.private_credentials("coindcx.private_rest")?;
        let object = body
            .as_object_mut()
            .ok_or_else(|| ExchangeApiError::Serialization {
                message: "CoinDCX private REST body must be a JSON object".to_string(),
            })?;
        object
            .entry("timestamp".to_string())
            .or_insert_with(|| json!(Utc::now().timestamp_millis()));
        let signature = coindcx_signature(&credentials.api_secret, &body)?;
        let url = format!(
            "{}{}",
            self.spot_rest_base_url.trim_end_matches('/'),
            endpoint
        );
        let response = self
            .http
            .request(Method::POST, url)
            .header("Content-Type", "application/json")
            .header("X-AUTH-APIKEY", &credentials.api_key)
            .header("X-AUTH-SIGNATURE", signature)
            .json(&body)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    async fn send_public_get(
        &self,
        base_url: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(base_url, endpoint, params);
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

    fn private_credentials(
        &self,
        operation: &'static str,
    ) -> ExchangeApiResult<&CoinDcxPrivateCredentials> {
        self.credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported { operation })
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
    let has_error = value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("error"))
        || value.get("error").is_some()
        || value.get("message").is_some() && !status.is_success();
    if !status.is_success() || has_error {
        let message = value
            .get("message")
            .or_else(|| value.get("error"))
            .or_else(|| value.get("code"))
            .and_then(Value::as_str)
            .unwrap_or("CoinDCX request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_coindcx_error(status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = value.get("code").map(|code| {
            code.as_str()
                .map(ToString::to_string)
                .unwrap_or_else(|| code.to_string())
        });
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_coindcx_error(status: u16, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    match status {
        401 | 403 => ExchangeErrorClass::Authentication,
        404 if msg.contains("order") => ExchangeErrorClass::OrderNotFound,
        404 => ExchangeErrorClass::InvalidSymbol,
        418 | 429 => ExchangeErrorClass::RateLimited,
        500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ if msg.contains("signature")
            || msg.contains("api key")
            || msg.contains("unauthorized")
            || msg.contains("authentication") =>
        {
            ExchangeErrorClass::Authentication
        }
        _ if msg.contains("permission") || msg.contains("forbidden") => {
            ExchangeErrorClass::Permission
        }
        _ if msg.contains("insufficient") || msg.contains("balance") => {
            ExchangeErrorClass::InsufficientBalance
        }
        _ if msg.contains("duplicate") || msg.contains("client") && msg.contains("exists") => {
            ExchangeErrorClass::DuplicateClientOrderId
        }
        _ if msg.contains("precision") || msg.contains("tick") || msg.contains("step") => {
            ExchangeErrorClass::InvalidPrecision
        }
        _ if msg.contains("symbol") || msg.contains("market") || msg.contains("pair") => {
            ExchangeErrorClass::InvalidSymbol
        }
        _ if msg.contains("rate") || msg.contains("limit") => ExchangeErrorClass::RateLimited,
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
        .map(|(key, value)| format!("{key}={}", urlencoding::encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}
