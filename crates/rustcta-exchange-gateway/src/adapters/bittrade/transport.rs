use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{canonical_query, signed_params_with_extra};

#[derive(Clone)]
pub struct BittradeRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BittradeRest {
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
            .get(build_url(&self.rest_base_url, endpoint, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_private_get(
        &self,
        api_key: &str,
        api_secret: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bittrade.private_rest_missing_credentials",
            });
        }
        let endpoint = normalize_endpoint(endpoint);
        let host = host(&self.rest_base_url)?;
        let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        let signed_params = signed_params_with_extra(
            api_key,
            api_secret,
            "GET",
            &host,
            &endpoint,
            &timestamp,
            params
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )?;
        let response = self
            .http
            .get(build_url_btree(
                &self.rest_base_url,
                &endpoint,
                &signed_params,
            ))
            .header(reqwest::header::ACCEPT, "application/json")
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
        let message = message(&value)
            .unwrap_or("BitTrade request failed")
            .to_string();
        return Err(exchange_error(
            exchange_id,
            status.as_u16().to_string(),
            message,
            value,
        ));
    }
    let ok = value
        .get("status")
        .and_then(Value::as_str)
        .is_none_or(|status| status.eq_ignore_ascii_case("ok"))
        && value.get("err-code").is_none();
    if ok {
        return Ok(value);
    }
    let code = code(&value).unwrap_or_else(|| "unknown".to_string());
    let message = message(&value).unwrap_or("BitTrade API error").to_string();
    Err(exchange_error(exchange_id, code, message, value))
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let query = params
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    build_url_btree(base, endpoint, &query)
}

fn build_url_btree(base: &str, endpoint: &str, params: &BTreeMap<String, String>) -> String {
    let query = canonical_query(params);
    if query.is_empty() {
        format!("{}{}", base.trim_end_matches('/'), endpoint)
    } else {
        format!("{}{}?{}", base.trim_end_matches('/'), endpoint, query)
    }
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with('/') {
        endpoint.to_string()
    } else {
        format!("/{endpoint}")
    }
}

fn host(base_url: &str) -> ExchangeApiResult<String> {
    let url = reqwest::Url::parse(base_url).map_err(|error| ExchangeApiError::InvalidRequest {
        message: format!("invalid BitTrade REST base URL: {error}"),
    })?;
    url.host_str()
        .map(|host| host.to_ascii_lowercase())
        .ok_or_else(|| ExchangeApiError::InvalidRequest {
            message: "BitTrade REST base URL must include host".to_string(),
        })
}

fn exchange_error(
    exchange_id: ExchangeId,
    code: String,
    message: String,
    raw: Value,
) -> ExchangeApiError {
    let lower = message.to_ascii_lowercase();
    let class = if lower.contains("signature") || lower.contains("api") {
        ExchangeErrorClass::Authentication
    } else if lower.contains("too many") || lower.contains("rate") || lower.contains("limit") {
        ExchangeErrorClass::RateLimited
    } else if lower.contains("not found") || lower.contains("not exist") {
        ExchangeErrorClass::OrderNotFound
    } else if lower.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if lower.contains("insufficient") || lower.contains("balance") {
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
    value
        .get("err-code")
        .or_else(|| value.get("code"))
        .and_then(|value| {
            value
                .as_str()
                .map(ToString::to_string)
                .or_else(|| value.as_i64().map(|number| number.to_string()))
        })
}

fn message(value: &Value) -> Option<&str> {
    value
        .get("err-msg")
        .or_else(|| value.get("message"))
        .or_else(|| value.get("msg"))
        .and_then(Value::as_str)
}
