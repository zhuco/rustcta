use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::LatokenSignedHeaders;

#[derive(Clone)]
pub struct LatokenRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl LatokenRest {
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
        query: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(self.url(endpoint))
            .query(query)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_json(
        &self,
        endpoint: &str,
        headers: LatokenSignedHeaders,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .post(self.url(endpoint))
            .header("X-LA-APIKEY", headers.api_key)
            .header("X-LA-SIGNATURE", headers.signature)
            .header("X-LA-DIGEST", headers.digest)
            .json(body)
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
        query: &[(String, String)],
        headers: LatokenSignedHeaders,
    ) -> ExchangeApiResult<Value> {
        let mut request = self
            .http
            .get(self.url(endpoint))
            .header("X-LA-APIKEY", headers.api_key)
            .header("X-LA-SIGNATURE", headers.signature)
            .header("X-LA-DIGEST", headers.digest);
        if !query.is_empty() {
            request = request.query(query);
        }
        let response = request
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
    let api_failure = value
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("failure"))
        || value.get("result").and_then(Value::as_bool) == Some(false);
    let error_text = value
        .get("message")
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty());
    if !status.is_success() || api_failure || error_text.is_some() {
        let message = error_text.unwrap_or("LATOKEN request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_latoken_error(message, status.as_u16(), &value),
            message.to_string(),
            Utc::now(),
        );
        error.code = value
            .get("error")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .or_else(|| Some(status.as_u16().to_string()));
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_latoken_error(message: &str, status: u16, value: &Value) -> ExchangeErrorClass {
    let joined = format!(
        "{} {}",
        message,
        value
            .get("error")
            .and_then(Value::as_str)
            .unwrap_or_default()
    )
    .to_ascii_lowercase();
    if status == 429 || joined.contains("too_many_requests") || joined.contains("rate") {
        return ExchangeErrorClass::RateLimited;
    }
    if status == 401
        || status == 403
        || joined.contains("not_authorized")
        || joined.contains("bad_credentials")
        || joined.contains("signature")
        || joined.contains("api key")
    {
        return ExchangeErrorClass::Authentication;
    }
    if joined.contains("forbidden") || joined.contains("access_denied") {
        return ExchangeErrorClass::Permission;
    }
    if joined.contains("insufficient") || joined.contains("balance") {
        return ExchangeErrorClass::InsufficientBalance;
    }
    if joined.contains("pair") || joined.contains("currency") || joined.contains("symbol") {
        return ExchangeErrorClass::InvalidSymbol;
    }
    if joined.contains("order") && joined.contains("not") && joined.contains("found") {
        return ExchangeErrorClass::OrderNotFound;
    }
    if (500..600).contains(&status)
        || joined.contains("service_unavailable")
        || joined.contains("internal_error")
    {
        return ExchangeErrorClass::ExchangeUnavailable;
    }
    ExchangeErrorClass::Unknown
}
