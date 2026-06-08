use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{form_encode, rest_signed_body_and_signature};

#[derive(Clone)]
pub struct ExmoRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl ExmoRest {
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

    pub async fn send_public_post(
        &self,
        endpoint: &str,
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .post(self.url(endpoint))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_encode(params))
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
        params: &[(String, String)],
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let (post_data, signature) = rest_signed_body_and_signature(api_secret, params)?;
        let response = self
            .http
            .post(self.url(endpoint))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Key", api_key)
            .header("Sign", signature)
            .body(post_data)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    fn url(&self, endpoint: &str) -> String {
        format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint)
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
    let result_false = value.get("result").and_then(Value::as_bool) == Some(false);
    let error_text = value
        .get("error")
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty());
    if !status.is_success() || result_false || error_text.is_some() {
        let message = error_text.unwrap_or("EXMO request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_exmo_error(message, status.as_u16()),
            message.to_string(),
            Utc::now(),
        );
        error.code = value
            .get("code")
            .and_then(|code| code.as_i64().map(|code| code.to_string()))
            .or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_exmo_error(message: &str, status: u16) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    if status == 429 || message.contains("rate limit") || message.contains("too many") {
        return ExchangeErrorClass::RateLimited;
    }
    if status == 401
        || status == 403
        || message.contains("signature")
        || message.contains("nonce")
        || message.contains("key")
    {
        return ExchangeErrorClass::Authentication;
    }
    if message.contains("permission") || message.contains("not allowed") {
        return ExchangeErrorClass::Permission;
    }
    if message.contains("balance") || message.contains("insufficient") {
        return ExchangeErrorClass::InsufficientBalance;
    }
    if message.contains("pair") || message.contains("currency") || message.contains("symbol") {
        return ExchangeErrorClass::InvalidSymbol;
    }
    if message.contains("order") && message.contains("not found") {
        return ExchangeErrorClass::OrderNotFound;
    }
    if (500..600).contains(&status) {
        return ExchangeErrorClass::ExchangeUnavailable;
    }
    ExchangeErrorClass::Unknown
}
