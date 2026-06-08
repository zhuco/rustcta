use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

#[derive(Clone)]
pub struct TokocryptoPublicRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    market_rest_base_url: String,
    http: reqwest::Client,
}

#[derive(Debug, Clone, Copy)]
pub enum TokocryptoRestBase {
    Open,
    Market,
}

impl TokocryptoPublicRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        market_rest_base_url: String,
        request_timeout_ms: u64,
    ) -> ExchangeApiResult<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(request_timeout_ms))
            .user_agent("RustCTA-Gateway/0.3")
            .build()
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        Ok(Self {
            exchange_id,
            rest_base_url,
            market_rest_base_url,
            http,
        })
    }

    pub async fn send_public_get(
        &self,
        base: TokocryptoRestBase,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(build_url(self.base_url(base), endpoint, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    fn base_url(&self, base: TokocryptoRestBase) -> &str {
        match base {
            TokocryptoRestBase::Open => &self.rest_base_url,
            TokocryptoRestBase::Market => &self.market_rest_base_url,
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
    if !status.is_success() {
        let message = message(&value)
            .unwrap_or("Tokocrypto request failed")
            .to_string();
        let status_code = status.as_u16().to_string();
        return Err(exchange_error(
            exchange_id,
            status_code,
            &message,
            value.clone(),
        ));
    }
    let ok = value
        .get("code")
        .and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        })
        .map(|code| code == 0)
        .unwrap_or(true);
    if ok {
        return Ok(value);
    }
    let code = code(&value).unwrap_or_else(|| "unknown".to_string());
    let message = message(&value)
        .unwrap_or("Tokocrypto API error")
        .to_string();
    Err(exchange_error(exchange_id, code, &message, value.clone()))
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let query = params
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(&key),
                urlencoding::encode(&value)
            )
        })
        .collect::<Vec<_>>()
        .join("&");
    if query.is_empty() {
        format!("{}{}", base.trim_end_matches('/'), endpoint)
    } else {
        format!("{}{}?{}", base.trim_end_matches('/'), endpoint, query)
    }
}

fn exchange_error(
    exchange_id: ExchangeId,
    code: String,
    message: &str,
    raw: Value,
) -> ExchangeApiError {
    let lower = message.to_ascii_lowercase();
    let class = if lower.contains("signature") || lower.contains("api") {
        ExchangeErrorClass::Authentication
    } else if lower.contains("rate") || lower.contains("too many") || code == "429" {
        ExchangeErrorClass::RateLimited
    } else if lower.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if lower.contains("order") && lower.contains("not") {
        ExchangeErrorClass::OrderNotFound
    } else if lower.contains("balance") || lower.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else {
        ExchangeErrorClass::Unknown
    };
    let mut error = ExchangeError::new(exchange_id, class, message, Utc::now());
    error.code = Some(code);
    error.raw = Some(raw);
    ExchangeApiError::Exchange(error)
}

fn code(value: &Value) -> Option<String> {
    value.get("code").and_then(|value| {
        value
            .as_str()
            .map(ToString::to_string)
            .or_else(|| value.as_i64().map(|number| number.to_string()))
    })
}

fn message(value: &Value) -> Option<&str> {
    value
        .get("msg")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
}
