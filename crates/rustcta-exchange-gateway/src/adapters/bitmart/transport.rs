use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId, MarketType};
use serde_json::Value;

use super::signing::sign_request;

#[derive(Clone)]
pub struct BitmartRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    futures_rest_base_url: String,
    http: reqwest::Client,
}

impl BitmartRest {
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

    pub async fn send_public_get(
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
        memo: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            market_type,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
            memo,
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
        memo: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            market_type,
            endpoint,
            params,
            Some(body),
            api_key,
            api_secret,
            memo,
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
        memo: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let body_text = body
            .as_ref()
            .map(serde_json::Value::to_string)
            .unwrap_or_default();
        let signature = sign_request(api_secret, &timestamp, memo, &body_text)?;
        let mut request = self
            .http
            .request(
                method,
                build_url(self.base_url(market_type), endpoint, params),
            )
            .header("X-BM-KEY", api_key)
            .header("X-BM-SIGN", signature)
            .header("X-BM-TIMESTAMP", timestamp)
            .header("Content-Type", "application/json");
        if let Some(memo) = memo.filter(|memo| !memo.trim().is_empty()) {
            request = request.header("X-BM-BROKER-ID", memo);
        }
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
            MarketType::Spot | MarketType::Margin => &self.spot_rest_base_url,
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
    if !status.is_success()
        || code
            .as_deref()
            .is_some_and(|code| code != "1000" && code != "0")
    {
        let message = value
            .get("message")
            .or_else(|| value.get("msg"))
            .and_then(Value::as_str)
            .unwrap_or("BitMart request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_error(code.as_deref(), status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let message = message.to_ascii_lowercase();
    match (status, code) {
        (401 | 403, _) | (_, "30005") | (_, "30013") => ExchangeErrorClass::Authentication,
        (418 | 429, _) => ExchangeErrorClass::RateLimited,
        (500..=599, _) => ExchangeErrorClass::ExchangeUnavailable,
        (_, "30004") | (_, "30007") => ExchangeErrorClass::InvalidRequest,
        (_, "30008") => ExchangeErrorClass::InvalidSymbol,
        (_, "30010") => ExchangeErrorClass::InsufficientBalance,
        _ if message.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if message.contains("not found") || message.contains("not exist") => {
            ExchangeErrorClass::OrderNotFound
        }
        _ if message.contains("rate") => ExchangeErrorClass::RateLimited,
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

pub fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
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
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}
