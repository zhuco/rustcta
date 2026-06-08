use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{build_form_body, sign_form};

#[derive(Clone)]
pub struct IndodaxRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl IndodaxRest {
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

    pub async fn send_tapi(
        &self,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let form_body = build_form_body(params);
        let signature = sign_form(api_secret, &form_body)?;
        let response = self
            .http
            .post(format!("{}/tapi", self.rest_base_url.trim_end_matches('/')))
            .header("Key", api_key)
            .header("Sign", signature)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_body)
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
            .and_then(value_as_i64)
            .is_none_or(|success| success == 1);
    if !success {
        let message = value
            .get("error")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Indodax request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_indodax_error(message),
            message,
            Utc::now(),
        );
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
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

fn classify_indodax_error(message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    if message.contains("invalid api") || message.contains("signature") || message.contains("nonce")
    {
        ExchangeErrorClass::Authentication
    } else if message.contains("insufficient") || message.contains("balance") {
        ExchangeErrorClass::InsufficientBalance
    } else if message.contains("pair") || message.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if message.contains("precision")
        || message.contains("minimum")
        || message.contains("invalid amount")
    {
        ExchangeErrorClass::InvalidPrecision
    } else if message.contains("too many") || message.contains("rate") {
        ExchangeErrorClass::RateLimited
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
