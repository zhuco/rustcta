use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{sign_gateio_request, signed_request_path};

#[derive(Clone)]
pub struct GateIoPublicRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl GateIoPublicRest {
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
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
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
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            Some(body),
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_signed_patch(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::PATCH,
            endpoint,
            params,
            Some(body),
            api_key,
            api_secret,
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
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            params,
            None,
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
        body: Option<&Value>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let query = build_query(params);
        let url = build_url_from_query(&self.rest_base_url, endpoint, &query);
        let timestamp = Utc::now().timestamp().to_string();
        let path = signed_request_path(&self.rest_base_url, endpoint);
        let body_text = body
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?
            .unwrap_or_default();
        let signature = sign_gateio_request(
            api_secret,
            method.as_str(),
            &path,
            &query,
            &body_text,
            &timestamp,
        );
        let mut request = self
            .http
            .request(method, url)
            .header("KEY", api_key)
            .header("Timestamp", timestamp)
            .header("SIGN", signature)
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
    if !status.is_success() {
        let label = value.get("label").and_then(Value::as_str);
        let message = value
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("Gate.io request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_gateio_error(label, message),
            message,
            Utc::now(),
        );
        error.code = label.map(ToOwned::to_owned);
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_gateio_error(label: Option<&str>, message: &str) -> ExchangeErrorClass {
    let label = label.unwrap_or_default().to_ascii_uppercase();
    let msg = message.to_ascii_lowercase();
    if label.contains("BALANCE") || msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if label.contains("INVALID_CURRENCY")
        || msg.contains("currency_pair")
        || msg.contains("symbol")
    {
        ExchangeErrorClass::InvalidSymbol
    } else if label.contains("INVALID_PARAM") || msg.contains("precision") || msg.contains("size") {
        ExchangeErrorClass::InvalidPrecision
    } else if label.contains("RATE_LIMIT") || label.contains("TOO_FAST") || msg.contains("too many")
    {
        ExchangeErrorClass::RateLimited
    } else if label.contains("AUTH") || label.contains("INVALID_SIGNATURE") {
        ExchangeErrorClass::Authentication
    } else if label.contains("ORDER_NOT_FOUND") {
        ExchangeErrorClass::OrderNotFound
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    build_url_from_query(base, endpoint, &build_query(params))
}

fn build_url_from_query(base: &str, endpoint: &str, query: &str) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !query.is_empty() {
        url.push('?');
        url.push_str(query);
    }
    url
}

fn build_query(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
