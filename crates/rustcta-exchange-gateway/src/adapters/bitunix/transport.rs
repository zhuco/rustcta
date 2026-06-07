use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId, MarketType};
use serde_json::Value;

use super::signing::sign_request;

#[derive(Clone)]
pub struct BitunixRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    futures_rest_base_url: String,
    http: reqwest::Client,
}

impl BitunixRest {
    pub fn new(
        exchange_id: ExchangeId,
        spot_rest_base_url: String,
        futures_rest_base_url: String,
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
            spot_rest_base_url,
            futures_rest_base_url,
            http,
        })
    }

    pub async fn send_public_request(
        &self,
        market_type: MarketType,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(build_url(self.base_url(market_type), endpoint, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_get(
        &self,
        market_type: MarketType,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            market_type,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        market_type: MarketType,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            market_type,
            endpoint,
            params,
            Some(body),
            api_key,
            api_secret,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        market_type: MarketType,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<Value>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let nonce = format!(
            "{:032x}",
            Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or_else(|| Utc::now().timestamp_millis())
        );
        let query_string = signing_query_string(params);
        let body_text = body
            .as_ref()
            .map(|body| body.to_string())
            .unwrap_or_default();
        let signature = sign_request(
            api_key,
            api_secret,
            &nonce,
            &timestamp,
            &query_string,
            &body_text,
        )?;
        let mut request = self
            .http
            .request(
                method,
                build_url(self.base_url(market_type), endpoint, params),
            )
            .header("api-key", api_key)
            .header("nonce", nonce)
            .header("timestamp", timestamp)
            .header("sign", signature)
            .header("language", "en-US")
            .header("Content-Type", "application/json");
        if body.is_some() {
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

    fn base_url(&self, market_type: MarketType) -> &str {
        match market_type {
            MarketType::Spot => &self.spot_rest_base_url,
            _ => &self.futures_rest_base_url,
        }
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
    let success = value.get("success").and_then(Value::as_bool);
    if !status.is_success()
        || code.as_deref().is_some_and(|code| code != "0")
        || success == Some(false)
    {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Bitunix request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bitunix_error(code.as_deref(), status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_bitunix_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match (status, code) {
        (401 | 403, _) | (_, "10007") | (_, "100005") | (_, "100008") => {
            ExchangeErrorClass::Authentication
        }
        (418 | 429, _) | (_, "110041") => ExchangeErrorClass::RateLimited,
        (500..=599, _) => ExchangeErrorClass::ExchangeUnavailable,
        (_, "2") | (_, "10001") => ExchangeErrorClass::InvalidRequest,
        (_, "10013") => ExchangeErrorClass::OrderNotFound,
        (_, "20003") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("duplicate") => ExchangeErrorClass::DuplicateClientOrderId,
        _ if msg.contains("order") && msg.contains("not") => ExchangeErrorClass::OrderNotFound,
        _ if msg.contains("rate") || msg.contains("too fast") => ExchangeErrorClass::RateLimited,
        _ if msg.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ if msg.contains("precision") => ExchangeErrorClass::InvalidPrecision,
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
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        url.push('?');
        url.push_str(&url_query_string(params));
    }
    url
}

pub fn url_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn signing_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}{value}"))
        .collect::<String>()
}
