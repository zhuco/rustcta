use std::collections::BTreeMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{form_encode, sign_futures_request, sign_spot_request};

#[derive(Clone)]
pub struct KrakenRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    futures_rest_base_url: String,
    http: reqwest::Client,
}

impl KrakenRest {
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

    pub async fn spot_public_get(
        &self,
        endpoint: &str,
        params: &BTreeMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.spot_rest_base_url, endpoint, params);
        let response =
            self.http
                .get(url)
                .send()
                .await
                .map_err(|error| ExchangeApiError::Transport {
                    message: error.to_string(),
                })?;
        parse_spot_response(self.exchange_id.clone(), response).await
    }

    pub async fn spot_private_post(
        &self,
        endpoint: &str,
        params: BTreeMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let nonce = Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or_else(|| Utc::now().timestamp_millis() * 1_000_000)
            .to_string();
        let signed = sign_spot_request(api_key, api_secret, endpoint, params, &nonce)?;
        let response = self
            .http
            .post(format!(
                "{}{}",
                self.spot_rest_base_url.trim_end_matches("/0"),
                signed.path
            ))
            .header("API-Key", signed.api_key)
            .header("API-Sign", signed.api_sign)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(signed.body)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_spot_response(self.exchange_id.clone(), response).await
    }

    pub async fn futures_public_get(
        &self,
        endpoint: &str,
        params: &BTreeMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.futures_rest_base_url, endpoint, params);
        let response =
            self.http
                .get(url)
                .send()
                .await
                .map_err(|error| ExchangeApiError::Transport {
                    message: error.to_string(),
                })?;
        parse_futures_response(self.exchange_id.clone(), response).await
    }

    pub async fn futures_private(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: BTreeMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let nonce = Utc::now().timestamp_millis().to_string();
        let path = format!("/derivatives/api/v3/{}", endpoint.trim_start_matches('/'));
        let signed = sign_futures_request(api_key, api_secret, &path, &params, &nonce)?;
        let url = if method == reqwest::Method::GET {
            build_url(&self.futures_rest_base_url, endpoint, &params)
        } else {
            format!(
                "{}/{}",
                self.futures_rest_base_url.trim_end_matches('/'),
                endpoint.trim_start_matches('/')
            )
        };
        let mut request = self
            .http
            .request(method.clone(), url)
            .header("APIKey", signed.api_key)
            .header("Nonce", signed.nonce)
            .header("Authent", signed.authent);
        if method != reqwest::Method::GET && !signed.query_or_body.is_empty() {
            request = request
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(signed.query_or_body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_futures_response(self.exchange_id.clone(), response).await
    }
}

fn build_url(base: &str, endpoint: &str, params: &BTreeMap<String, String>) -> String {
    let mut url = format!(
        "{}/{}",
        base.trim_end_matches('/'),
        endpoint.trim_start_matches('/')
    );
    if !params.is_empty() {
        url.push('?');
        url.push_str(&form_encode(params));
    }
    url
}

async fn parse_spot_response(
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
        return Err(exchange_error(
            exchange_id,
            Some(status.as_u16().to_string()),
            "Kraken HTTP error",
            &value,
        ));
    }
    let errors = value
        .get("error")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|message| !message.trim().is_empty())
        .collect::<Vec<_>>();
    if !errors.is_empty() {
        return Err(exchange_error(
            exchange_id,
            None,
            &errors.join("; "),
            &value,
        ));
    }
    Ok(value.get("result").cloned().unwrap_or(value))
}

async fn parse_futures_response(
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
        return Err(exchange_error(
            exchange_id,
            Some(status.as_u16().to_string()),
            "Kraken Futures HTTP error",
            &value,
        ));
    }
    let result = value
        .get("result")
        .and_then(Value::as_str)
        .unwrap_or("success");
    if !result.eq_ignore_ascii_case("success") {
        return Err(exchange_error(exchange_id, None, result, &value));
    }
    Ok(value)
}

fn exchange_error(
    exchange_id: ExchangeId,
    code: Option<String>,
    message: &str,
    raw: &Value,
) -> ExchangeApiError {
    let detail = raw
        .get("error")
        .or_else(|| raw.get("errors"))
        .or_else(|| raw.get("message"))
        .and_then(|value| {
            value
                .as_str()
                .map(str::to_string)
                .or_else(|| Some(value.to_string()))
        })
        .unwrap_or_else(|| message.to_string());
    let lower = detail.to_ascii_lowercase();
    let class = if lower.contains("rate") || lower.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if lower.contains("auth")
        || lower.contains("signature")
        || lower.contains("nonce")
        || lower.contains("key")
    {
        ExchangeErrorClass::Authentication
    } else if lower.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if lower.contains("unknown order") || lower.contains("not found") {
        ExchangeErrorClass::OrderNotFound
    } else if lower.contains("symbol") || lower.contains("pair") {
        ExchangeErrorClass::InvalidSymbol
    } else {
        ExchangeErrorClass::Unknown
    };
    let mut error = ExchangeError::new(exchange_id, class, detail, Utc::now());
    error.code = code;
    error.raw = Some(raw.clone());
    ExchangeApiError::Exchange(error)
}
