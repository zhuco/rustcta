use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::private::HitbtcOfflineRequest;

#[derive(Clone)]
pub struct HitbtcRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl HitbtcRest {
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

    pub async fn send_signed_request(
        &self,
        request: &HitbtcOfflineRequest,
    ) -> ExchangeApiResult<Value> {
        let params = request
            .query
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        let url = build_private_url(&self.rest_base_url, &request.path, &params);
        let method = reqwest::Method::from_bytes(request.method.as_bytes()).map_err(|error| {
            ExchangeApiError::InvalidRequest {
                message: format!("invalid HitBTC private REST method: {error}"),
            }
        })?;
        let mut builder = self.http.request(method, url);
        for (key, value) in &request.headers {
            builder = builder.header(key, value);
        }
        if let Some(body) = request.raw_body.as_deref() {
            builder = builder.body(body.to_string());
        }
        let response = builder
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
    if status.is_success() && value.get("error").is_none() {
        return Ok(value);
    }

    let message = value
        .get("error")
        .and_then(|error| error.get("message"))
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("HitBTC request failed");
    let mut error = ExchangeError::new(
        exchange_id,
        classify_hitbtc_error(message, status.as_u16()),
        message,
        Utc::now(),
    );
    error.raw = Some(value);
    Err(ExchangeApiError::Exchange(error))
}

fn classify_hitbtc_error(message: &str, status: u16) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    if status == 401 || message.contains("auth") || message.contains("signature") {
        ExchangeErrorClass::Authentication
    } else if status == 429 || message.contains("rate") || message.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else if message.contains("symbol") || message.contains("not found") {
        ExchangeErrorClass::InvalidSymbol
    } else if message.contains("insufficient") || message.contains("funds") {
        ExchangeErrorClass::InsufficientBalance
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        url.push('?');
        url.push_str(
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
    url
}

fn build_private_url(base: &str, signed_path: &str, params: &HashMap<String, String>) -> String {
    let base = base.trim_end_matches('/');
    let root = base.strip_suffix("/api/3").unwrap_or(base);
    let mut url = format!("{}{}", root, signed_path);
    if !params.is_empty() {
        let sorted = params
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        url.push('?');
        url.push_str(
            &sorted
                .iter()
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
    url
}
