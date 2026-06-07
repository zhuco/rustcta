use std::collections::HashMap;
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{build_query_string, sign_tapbit_request};

#[derive(Clone)]
pub struct TapbitRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    swap_rest_base_url: String,
    http: reqwest::Client,
}

impl TapbitRest {
    pub fn new(
        exchange_id: ExchangeId,
        spot_rest_base_url: String,
        swap_rest_base_url: String,
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
            spot_rest_base_url,
            swap_rest_base_url,
            http,
        })
    }

    pub async fn send_spot_public_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.spot_rest_base_url, endpoint, params);
        let response =
            self.http
                .get(url)
                .send()
                .await
                .map_err(|error| ExchangeApiError::Transport {
                    message: error.to_string(),
                })?;
        parse_tapbit_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_swap_public_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.swap_rest_base_url, endpoint, params);
        let response =
            self.http
                .get(url)
                .send()
                .await
                .map_err(|error| ExchangeApiError::Transport {
                    message: error.to_string(),
                })?;
        parse_tapbit_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_spot_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            &self.spot_rest_base_url,
            endpoint,
            params,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_swap_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_get(
            &self.swap_rest_base_url,
            endpoint,
            params,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_spot_signed_post(
        &self,
        endpoint: &str,
        body: Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            &self.spot_rest_base_url,
            endpoint,
            body,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_swap_signed_post(
        &self,
        endpoint: &str,
        body: Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_post(
            &self.swap_rest_base_url,
            endpoint,
            body,
            api_key,
            api_secret,
        )
        .await
    }

    async fn send_signed_get(
        &self,
        base_url: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let query = build_query_string(params);
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let signature = sign_tapbit_request(api_secret, &timestamp, "GET", endpoint, &query, "")?;
        let url = build_url(base_url, endpoint, params);
        let response = self
            .http
            .get(url)
            .header("Accept", "application/json")
            .header("ACCESS-KEY", api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", timestamp)
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_tapbit_response(self.exchange_id.clone(), response).await
    }

    async fn send_signed_post(
        &self,
        base_url: &str,
        endpoint: &str,
        body: Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let body_text = if body.is_null() {
            String::new()
        } else {
            body.to_string()
        };
        let timestamp = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let signature =
            sign_tapbit_request(api_secret, &timestamp, "POST", endpoint, "", &body_text)?;
        let url = format!("{}{}", base_url.trim_end_matches('/'), endpoint);
        let response = self
            .http
            .post(url)
            .header("Accept", "application/json")
            .header("ACCESS-KEY", api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-TIMESTAMP", timestamp)
            .header("Content-Type", "application/json")
            .body(body_text)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_tapbit_response(self.exchange_id.clone(), response).await
    }
}

async fn parse_tapbit_response(
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
    let code = value
        .get("code")
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()));
    if !status.is_success() || code.is_some_and(|code| code != 200 && code != 0) {
        let message = value
            .get("message")
            .or_else(|| value.get("msg"))
            .and_then(Value::as_str)
            .unwrap_or("Tapbit request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_tapbit_error(code, message),
            message,
            Utc::now(),
        );
        error.code = code.map(|code| code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_tapbit_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    match code {
        Some(429) => ExchangeErrorClass::RateLimited,
        Some(401) | Some(403) => ExchangeErrorClass::Authentication,
        Some(10001) => ExchangeErrorClass::InvalidRequest,
        _ if message.contains("too frequent") || message.contains("rate") => {
            ExchangeErrorClass::RateLimited
        }
        _ if message.contains("signature") || message.contains("api key") => {
            ExchangeErrorClass::Authentication
        }
        _ if message.contains("permission") || message.contains("whitelist") => {
            ExchangeErrorClass::Permission
        }
        _ if message.contains("balance") => ExchangeErrorClass::InsufficientBalance,
        _ if message.contains("not found") && message.contains("order") => {
            ExchangeErrorClass::OrderNotFound
        }
        _ => ExchangeErrorClass::Unknown,
    }
}

#[cfg(test)]
pub(super) fn classify_tapbit_error_for_tests(
    code: Option<i64>,
    message: &str,
) -> ExchangeErrorClass {
    classify_tapbit_error(code, message)
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    let query = build_query_string(params);
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query);
    }
    url
}
