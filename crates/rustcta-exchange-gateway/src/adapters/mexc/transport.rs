use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_raw_query;

#[derive(Clone)]
pub struct MexcPublicRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    contract_rest_base_url: String,
    http: reqwest::Client,
}

impl MexcPublicRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        contract_rest_base_url: String,
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
            contract_rest_base_url,
            http,
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

    pub async fn send_contract_public_request(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.contract_rest_base_url, endpoint, params);
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
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_contract_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_contract_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_contract_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_contract_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            body,
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
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        let mut signed_params = params.clone();
        signed_params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        signed_params.insert("recvWindow".to_string(), recv_window_ms.to_string());
        let query = build_query_string(&signed_params);
        let signature = sign_raw_query(api_secret, &query)?;
        let url = build_signed_url(&self.rest_base_url, endpoint, &signed_params, &signature);
        let response = self
            .http
            .request(method, url)
            .header("X-MEXC-APIKEY", api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    async fn send_contract_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let request_time = Utc::now().timestamp_millis().to_string();
        let query = build_query_string(params);
        let body_text = body.map(Value::to_string);
        let sign_payload = format!(
            "{}{}{}",
            api_key,
            request_time,
            body_text.as_deref().unwrap_or(&query)
        );
        let signature = sign_raw_query(api_secret, &sign_payload)?;
        let url = build_url(&self.contract_rest_base_url, endpoint, params);
        let mut request = self
            .http
            .request(method, url)
            .header("ApiKey", api_key)
            .header("Request-Time", &request_time)
            .header("Signature", signature)
            .header("Content-Type", "application/json");
        if let Some(body_text) = body_text {
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
        let code = value.get("code").and_then(Value::as_i64);
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("MEXC request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_mexc_error(code, message),
            message,
            Utc::now(),
        );
        error.code = code.map(|code| code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_mexc_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    match (code, message.to_ascii_lowercase()) {
        (Some(10101), _) => ExchangeErrorClass::InvalidSymbol,
        (Some(10072), _) => ExchangeErrorClass::InvalidPrecision,
        (Some(700003), _) => ExchangeErrorClass::Authentication,
        (Some(10007), _) => ExchangeErrorClass::RateLimited,
        (_, msg) if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        (_, msg) if msg.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        (_, msg) if msg.contains("rate") || msg.contains("too many") => {
            ExchangeErrorClass::RateLimited
        }
        _ => ExchangeErrorClass::Unknown,
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        url.push('?');
        url.push_str(&build_query_string(params));
    }
    url
}

fn build_signed_url(
    base: &str,
    endpoint: &str,
    params: &HashMap<String, String>,
    signature: &str,
) -> String {
    let mut url = build_url(base, endpoint, params);
    if params.is_empty() {
        url.push('?');
    } else {
        url.push('&');
    }
    url.push_str("signature=");
    url.push_str(signature);
    url
}

pub fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
