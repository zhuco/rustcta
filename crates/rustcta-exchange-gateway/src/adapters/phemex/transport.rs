use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_phemex_request;

#[derive(Clone)]
pub struct PhemexRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    request_expiry_seconds: i64,
}

impl PhemexRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        request_expiry_seconds: i64,
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
            request_expiry_seconds,
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
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_signed_put(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::PUT,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn send_signed_post_json(
        &self,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            &HashMap::new(),
            Some(body),
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let query = build_query(params);
        let url = build_url_from_query(&self.rest_base_url, endpoint, &query);
        let expiry = (Utc::now().timestamp() + self.request_expiry_seconds).to_string();
        let body_text = body
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?
            .unwrap_or_default();
        let signature = sign_phemex_request(api_secret, endpoint, &query, &expiry, &body_text);
        let mut request = self
            .http
            .request(method, url)
            .header("x-phemex-access-token", api_key)
            .header("x-phemex-request-expiry", expiry)
            .header("x-phemex-request-signature", signature)
            .header("Content-Type", "application/json");
        if !body_text.is_empty() {
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
            .get("msg")
            .and_then(Value::as_str)
            .unwrap_or("Phemex request failed")
            .to_string();
        return Err(exchange_error(
            exchange_id,
            classify_phemex_error(status.as_u16(), None, Some(&message)),
            Some(status.as_u16().to_string()),
            &message,
            value.clone(),
        ));
    }
    if let Some(code) = value.get("code").and_then(value_as_i64) {
        if code != 0 {
            let message = value
                .get("msg")
                .and_then(Value::as_str)
                .unwrap_or("Phemex request failed")
                .to_string();
            return Err(exchange_error(
                exchange_id,
                classify_phemex_error(status.as_u16(), Some(code), Some(&message)),
                Some(code.to_string()),
                &message,
                value.clone(),
            ));
        }
    }
    if let Some(error) = value.get("error") {
        if !error.is_null() {
            let code = error
                .get("code")
                .and_then(value_as_i64)
                .map(|code| code.to_string());
            let message = error
                .get("message")
                .or_else(|| error.get("msg"))
                .and_then(Value::as_str)
                .unwrap_or("Phemex market data request failed")
                .to_string();
            return Err(exchange_error(
                exchange_id,
                ExchangeErrorClass::Unknown,
                code,
                &message,
                value.clone(),
            ));
        }
    }
    Ok(value)
}

fn exchange_error(
    exchange_id: ExchangeId,
    class: ExchangeErrorClass,
    code: Option<String>,
    message: &str,
    raw: Value,
) -> ExchangeApiError {
    let mut error = ExchangeError::new(exchange_id, class, message, Utc::now());
    error.code = code;
    error.raw = Some(raw);
    ExchangeApiError::Exchange(error)
}

fn classify_phemex_error(
    http_status: u16,
    code: Option<i64>,
    message: Option<&str>,
) -> ExchangeErrorClass {
    let msg = message.unwrap_or_default().to_ascii_lowercase();
    match http_status {
        401 => return ExchangeErrorClass::Authentication,
        403 => return ExchangeErrorClass::Permission,
        429 => return ExchangeErrorClass::RateLimited,
        500..=599 => return ExchangeErrorClass::ExchangeUnavailable,
        _ => {}
    }
    match code {
        Some(10002) => ExchangeErrorClass::OrderNotFound,
        Some(10001 | 11033 | 11085 | 19999) => ExchangeErrorClass::DuplicateClientOrderId,
        Some(11001 | 11100 | 11105 | 11106) => ExchangeErrorClass::InsufficientBalance,
        Some(11027 | 11028 | 11034..=11038 | 11112) => ExchangeErrorClass::InvalidRequest,
        Some(11061..=11068) => ExchangeErrorClass::OrderRejected,
        Some(11022..=11024) => ExchangeErrorClass::RiskRejected,
        _ if msg.contains("rate") || msg.contains("too many") => ExchangeErrorClass::RateLimited,
        _ if msg.contains("signature") || msg.contains("auth") => {
            ExchangeErrorClass::Authentication
        }
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("not found") => ExchangeErrorClass::OrderNotFound,
        _ => ExchangeErrorClass::Unknown,
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    build_url_from_query(base, endpoint, &build_query(params))
}

fn build_url_from_query(base: &str, endpoint: &str, query: &str) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !query.is_empty() {
        url.push('?');
        url.push_str(query);
    }
    url
}

fn build_query(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
