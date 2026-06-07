use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_base64;

#[derive(Clone)]
pub struct KuCoinPublicRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl KuCoinPublicRest {
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
        api_passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
            api_passphrase,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
        api_key: &str,
        api_secret: &str,
        api_passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            Some(body),
            api_key,
            api_secret,
            api_passphrase,
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        api_passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
            api_passphrase,
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
        api_passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        let path = build_path(endpoint, params);
        let timestamp = Utc::now().timestamp_millis().to_string();
        let body_text = body
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?
            .unwrap_or_default();
        let prehash = format!("{timestamp}{}{path}{body_text}", method.as_str());
        let signature = sign_base64(api_secret, &prehash)?;
        let passphrase = sign_base64(api_secret, api_passphrase)?;
        let mut request = self
            .http
            .request(
                method,
                format!("{}{}", self.rest_base_url.trim_end_matches('/'), path),
            )
            .header("KC-API-KEY", api_key)
            .header("KC-API-SIGN", signature)
            .header("KC-API-TIMESTAMP", timestamp)
            .header("KC-API-PASSPHRASE", passphrase)
            .header("KC-API-KEY-VERSION", "2")
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
    let code = value.get("code").and_then(value_as_i64);
    let success = status.is_success()
        && code
            .as_ref()
            .is_none_or(|code| *code == 200000 || *code == 0);
    if !success {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("KuCoin request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_kucoin_error(code, message),
            message,
            Utc::now(),
        );
        error.code = code.map(|code| code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value.get("data").cloned().unwrap_or(value))
}

fn classify_kucoin_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    match (code, message.to_ascii_lowercase()) {
        (Some(400100), _) | (Some(400600), _) => ExchangeErrorClass::InvalidRequest,
        (Some(400200), _) | (Some(400300), _) | (Some(400400), _) => {
            ExchangeErrorClass::Authentication
        }
        (Some(400500), _) => ExchangeErrorClass::InvalidSymbol,
        (Some(429000), _) => ExchangeErrorClass::RateLimited,
        (_, msg) if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        (_, msg) if msg.contains("symbol") || msg.contains("currency") => {
            ExchangeErrorClass::InvalidSymbol
        }
        (_, msg) if msg.contains("precision") || msg.contains("increment") => {
            ExchangeErrorClass::InvalidPrecision
        }
        (_, msg) if msg.contains("too many") || msg.contains("rate") => {
            ExchangeErrorClass::RateLimited
        }
        _ => ExchangeErrorClass::Unknown,
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        build_path(endpoint, params)
    )
}

fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = endpoint.to_string();
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        url.push('?');
        url.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    url
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
