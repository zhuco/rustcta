use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{canonical_query, signed_params, signed_params_with_extra};

#[derive(Clone)]
pub struct HtxRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    linear_rest_base_url: String,
    http: reqwest::Client,
}

#[derive(Debug, Clone, Copy)]
pub enum HtxRestProduct {
    Spot,
    LinearSwap,
}

impl HtxRest {
    pub fn new(
        exchange_id: ExchangeId,
        spot_rest_base_url: String,
        linear_rest_base_url: String,
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
            linear_rest_base_url,
            http,
        })
    }

    pub async fn send_public_get(
        &self,
        product: HtxRestProduct,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(build_url(self.base_url(product), endpoint, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_post(
        &self,
        product: HtxRestProduct,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let base_url = self.base_url(product);
        let host = reqwest::Url::parse(base_url)
            .ok()
            .and_then(|url| url.host_str().map(ToString::to_string))
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("invalid HTX base URL {base_url}"),
            })?;
        let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        let params = signed_params(api_key, api_secret, "POST", &host, endpoint, &timestamp)?;
        let url = format!(
            "{}{}?{}",
            base_url.trim_end_matches('/'),
            endpoint,
            canonical_query(&params)
        );
        let response = self
            .http
            .post(url)
            .header("Content-Type", "application/json")
            .json(body)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_get(
        &self,
        product: HtxRestProduct,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let base_url = self.base_url(product);
        let host = reqwest::Url::parse(base_url)
            .ok()
            .and_then(|url| url.host_str().map(ToString::to_string))
            .ok_or_else(|| ExchangeApiError::InvalidRequest {
                message: format!("invalid HTX base URL {base_url}"),
            })?;
        let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        let signed = signed_params_with_extra(
            api_key,
            api_secret,
            "GET",
            &host,
            endpoint,
            &timestamp,
            params
                .iter()
                .map(|(key, value)| (key.clone(), value.clone())),
        )?;
        let url = format!(
            "{}{}?{}",
            base_url.trim_end_matches('/'),
            endpoint,
            canonical_query(&signed)
        );
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

    fn base_url(&self, product: HtxRestProduct) -> &str {
        match product {
            HtxRestProduct::Spot => &self.spot_rest_base_url,
            HtxRestProduct::LinearSwap => &self.linear_rest_base_url,
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
        let message = message(&value).unwrap_or("HTX request failed").to_string();
        return Err(exchange_error(
            exchange_id,
            status.as_u16().to_string(),
            &message,
            value,
        ));
    }
    let ok = value
        .get("status")
        .and_then(Value::as_str)
        .is_none_or(|status| status.eq_ignore_ascii_case("ok"))
        && value.get("err-code").is_none();
    if !ok {
        let code = code(&value).unwrap_or_else(|| "unknown".to_string());
        let message = message(&value).unwrap_or("HTX API error").to_string();
        return Err(exchange_error(exchange_id, code, &message, value));
    }
    Ok(value
        .get("data")
        .cloned()
        .or_else(|| value.get("tick").cloned())
        .unwrap_or(value))
}

fn exchange_error(
    exchange_id: ExchangeId,
    code: String,
    message: &str,
    raw: Value,
) -> ExchangeApiError {
    let lower = message.to_ascii_lowercase();
    let class = if lower.contains("signature") || lower.contains("api-key") {
        ExchangeErrorClass::Authentication
    } else if lower.contains("rate") || lower.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if lower.contains("not found") || lower.contains("not exist") {
        ExchangeErrorClass::OrderNotFound
    } else if lower.contains("symbol") || lower.contains("contract") {
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

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let query = params
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let query = canonical_query(&query);
    if query.is_empty() {
        format!("{}{}", base.trim_end_matches('/'), endpoint)
    } else {
        format!("{}{}?{}", base.trim_end_matches('/'), endpoint, query)
    }
}

fn code(value: &Value) -> Option<String> {
    value
        .get("err-code")
        .or_else(|| value.get("code"))
        .and_then(|value| {
            value
                .as_str()
                .map(ToString::to_string)
                .or_else(|| Some(value.to_string()))
        })
}

fn message(value: &Value) -> Option<&str> {
    value
        .get("err-msg")
        .or_else(|| value.get("message"))
        .or_else(|| value.get("msg"))
        .and_then(Value::as_str)
}
