use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::sign_message;

#[derive(Clone)]
pub struct AsterRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl AsterRest {
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
        user_address: &str,
        signer_address: &str,
        signer_private_key: &str,
    ) -> ExchangeApiResult<Value> {
        let signed_params = signed_params(params, user_address, signer_address);
        let query = build_query_string(&signed_params);
        let signature = sign_message(signer_private_key, &query)?;
        let url = build_signed_url(&self.rest_base_url, endpoint, &signed_params, &signature);
        let response = self
            .http
            .get(url)
            .header("Content-Type", "application/x-www-form-urlencoded")
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
        params: &HashMap<String, String>,
        user_address: &str,
        signer_address: &str,
        signer_private_key: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            user_address,
            signer_address,
            signer_private_key,
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        user_address: &str,
        signer_address: &str,
        signer_private_key: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            params,
            user_address,
            signer_address,
            signer_private_key,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        user_address: &str,
        signer_address: &str,
        signer_private_key: &str,
    ) -> ExchangeApiResult<Value> {
        let mut signed_params = signed_params(params, user_address, signer_address);
        let query = build_query_string(&signed_params);
        let signature = sign_message(signer_private_key, &query)?;
        signed_params.insert("signature".to_string(), signature);
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let response = self
            .http
            .request(method, url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&signed_params)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }
}

fn signed_params(
    params: &HashMap<String, String>,
    user_address: &str,
    signer_address: &str,
) -> HashMap<String, String> {
    let mut signed_params = params.clone();
    signed_params.insert(
        "nonce".to_string(),
        (Utc::now().timestamp_micros()).to_string(),
    );
    signed_params.insert("user".to_string(), user_address.to_string());
    signed_params.insert("signer".to_string(), signer_address.to_string());
    signed_params
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
        let code = value
            .get("code")
            .and_then(Value::as_i64)
            .map(|code| code.to_string());
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Aster request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_aster_error(code.as_deref(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_aster_error(code: Option<&str>, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    if matches!(code, "-2010" | "-2018") || msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if matches!(code, "-1121") || msg.contains("invalid symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if matches!(code, "-1111" | "-1013")
        || msg.contains("precision")
        || msg.contains("filter failure")
    {
        ExchangeErrorClass::InvalidPrecision
    } else if matches!(code, "-1003") || msg.contains("too many requests") {
        ExchangeErrorClass::RateLimited
    } else if matches!(code, "-1021" | "-1022" | "-2014" | "-2015")
        || msg.contains("signature")
        || msg.contains("api-key")
    {
        ExchangeErrorClass::Authentication
    } else if matches!(code, "-2011" | "-2013") || msg.contains("order does not exist") {
        ExchangeErrorClass::OrderNotFound
    } else {
        ExchangeErrorClass::Unknown
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
