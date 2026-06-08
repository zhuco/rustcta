use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_hex;

#[derive(Clone)]
pub struct CoincheckPublicRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl CoincheckPublicRest {
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
        self.send_request(Method::GET, endpoint, params, None, None)
            .await
    }

    pub async fn send_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::GET,
            endpoint,
            params,
            None,
            Some((api_key, api_secret)),
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
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::POST,
            endpoint,
            params,
            Some(body),
            Some((api_key, api_secret)),
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
        self.send_request(
            Method::DELETE,
            endpoint,
            params,
            None,
            Some((api_key, api_secret)),
        )
        .await
    }

    async fn send_request(
        &self,
        method: Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
        credentials: Option<(&str, &str)>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.rest_base_url, endpoint, params);
        let body_text = body
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?
            .unwrap_or_default();
        let mut request = self
            .http
            .request(method, &url)
            .header("Content-Type", "application/json");
        if let Some((api_key, api_secret)) = credentials {
            let nonce = Utc::now().timestamp_millis().to_string();
            let signature = sign_hex(api_secret, &format!("{nonce}{url}{body_text}"))?;
            request = request
                .header("ACCESS-KEY", api_key)
                .header("ACCESS-NONCE", nonce)
                .header("ACCESS-SIGNATURE", signature);
        }
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
    let success = status.is_success()
        && value
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(true);
    if !success {
        let message = value
            .get("error")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Coincheck request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_coincheck_error(status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = Some(status.as_u16().to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_coincheck_error(status: u16, message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if status == 401 || status == 403 || msg.contains("auth") {
        ExchangeErrorClass::Authentication
    } else if status == 429 || msg.contains("rate") {
        ExchangeErrorClass::RateLimited
    } else if msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("order") && msg.contains("not") {
        ExchangeErrorClass::OrderNotFound
    } else if msg.contains("pair") || msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
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
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    url
}
