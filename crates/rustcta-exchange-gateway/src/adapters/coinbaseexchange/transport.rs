use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_base64_decoded_secret;

#[derive(Clone)]
pub struct CoinbaseExchangeRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl CoinbaseExchangeRest {
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
        self.send_request(Method::GET, endpoint, params, None, None)
            .await
    }

    pub async fn send_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        api_passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_request(
            Method::GET,
            endpoint,
            params,
            None,
            Some((api_key, api_secret, api_passphrase)),
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
        self.send_request(
            Method::POST,
            endpoint,
            params,
            Some(body),
            Some((api_key, api_secret, api_passphrase)),
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
        self.send_request(
            Method::DELETE,
            endpoint,
            params,
            None,
            Some((api_key, api_secret, api_passphrase)),
        )
        .await
    }

    async fn send_request(
        &self,
        method: Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
        credentials: Option<(&str, &str, &str)>,
    ) -> ExchangeApiResult<Value> {
        let path = build_path(endpoint, params);
        let body_text = body
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?
            .unwrap_or_default();
        let mut request = self
            .http
            .request(
                method.clone(),
                format!("{}{}", self.rest_base_url.trim_end_matches('/'), path),
            )
            .header("Content-Type", "application/json");
        if let Some((api_key, api_secret, api_passphrase)) = credentials {
            let timestamp = Utc::now().timestamp().to_string();
            let prehash = format!("{timestamp}{}{path}{body_text}", method.as_str());
            request = request
                .header("CB-ACCESS-KEY", api_key)
                .header(
                    "CB-ACCESS-SIGN",
                    sign_base64_decoded_secret(api_secret, &prehash)?,
                )
                .header("CB-ACCESS-TIMESTAMP", timestamp)
                .header("CB-ACCESS-PASSPHRASE", api_passphrase);
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
    if !status.is_success()
        || value.get("message").is_some()
        || value.get("error").is_some()
        || value.get("errors").is_some()
    {
        let message = value
            .get("message")
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Coinbase Exchange request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_coinbaseexchange_error(status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = Some(status.as_u16().to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_coinbaseexchange_error(status: u16, message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    if status == 401 || status == 403 || message.contains("auth") {
        ExchangeErrorClass::Authentication
    } else if status == 429 || message.contains("rate") {
        ExchangeErrorClass::RateLimited
    } else if message.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if message.contains("not found") || message.contains("order") {
        ExchangeErrorClass::OrderNotFound
    } else if message.contains("product") || message.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut path = endpoint.to_string();
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        path.push('?');
        path.push_str(
            &pairs
                .into_iter()
                .map(|(key, value)| format!("{key}={value}"))
                .collect::<Vec<_>>()
                .join("&"),
        );
    }
    path
}
