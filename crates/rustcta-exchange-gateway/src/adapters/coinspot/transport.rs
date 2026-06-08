use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{Map, Value};

use super::signing::sign_hex;

#[derive(Clone)]
pub struct CoinspotRest {
    exchange_id: ExchangeId,
    public_rest_base_url: String,
    private_rest_base_url: String,
    read_only_rest_base_url: String,
    http: reqwest::Client,
}

impl CoinspotRest {
    pub fn new(
        exchange_id: ExchangeId,
        public_rest_base_url: String,
        private_rest_base_url: String,
        read_only_rest_base_url: String,
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
            public_rest_base_url,
            private_rest_base_url,
            read_only_rest_base_url,
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
            .get(format!(
                "{}{}",
                self.public_rest_base_url.trim_end_matches('/'),
                build_path(endpoint, params)
            ))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let mut body_map = match body {
            Value::Object(map) => map.clone(),
            _ => Map::new(),
        };
        body_map.insert(
            "nonce".to_string(),
            Value::String(Utc::now().timestamp_millis().to_string()),
        );
        let body_text = serde_json::to_string(&Value::Object(body_map)).map_err(|error| {
            ExchangeApiError::Serialization {
                message: error.to_string(),
            }
        })?;
        let signature = sign_hex(api_secret, &body_text)?;
        let base_url = if endpoint.contains("/ro/") {
            &self.read_only_rest_base_url
        } else {
            &self.private_rest_base_url
        };
        let response = self
            .http
            .post(format!("{}{}", base_url.trim_end_matches('/'), endpoint))
            .header("key", api_key)
            .header("sign", signature)
            .header("Content-Type", "application/json")
            .body(body_text)
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
            .get("status")
            .and_then(Value::as_str)
            .is_none_or(|status| status.eq_ignore_ascii_case("ok"))
        && value.get("error").is_none();
    if !success {
        let message = value
            .get("message")
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("CoinSpot request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_coinspot_error(message),
            message,
            Utc::now(),
        );
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_coinspot_error(message: &str) -> ExchangeErrorClass {
    let msg = message.to_ascii_lowercase();
    if msg.contains("key") || msg.contains("sign") || msg.contains("auth") {
        ExchangeErrorClass::Authentication
    } else if msg.contains("insufficient") || msg.contains("balance") {
        ExchangeErrorClass::InsufficientBalance
    } else if msg.contains("coin") || msg.contains("market") {
        ExchangeErrorClass::InvalidSymbol
    } else if msg.contains("rate") || msg.contains("limit") {
        ExchangeErrorClass::RateLimited
    } else {
        ExchangeErrorClass::Unknown
    }
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
