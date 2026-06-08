#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

#[derive(Clone)]
pub struct FoxbitRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl FoxbitRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
    ) -> ExchangeApiResult<Self> {
        let http = reqwest::Client::builder()
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
        let mut url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        if !params.is_empty() {
            let query = params
                .iter()
                .map(|(key, value)| {
                    format!(
                        "{}={}",
                        urlencoding::encode(key),
                        urlencoding::encode(value)
                    )
                })
                .collect::<Vec<_>>()
                .join("&");
            url.push('?');
            url.push_str(&query);
        }
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
        let message = value
            .get("message")
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Foxbit API error");
        let mut error = ExchangeError::new(
            exchange_id,
            ExchangeErrorClass::ExchangeUnavailable,
            message,
            Utc::now(),
        );
        error.code = Some(status.as_u16().to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

pub fn public_get_request_spec(path: &str, query: Option<Value>) -> Value {
    let mut spec = serde_json::Map::new();
    spec.insert("method".to_string(), json!("GET"));
    spec.insert("path".to_string(), json!(path));
    spec.insert("auth".to_string(), json!("none"));
    spec.insert(
        "headers".to_string(),
        json!({ "Accept": "application/json" }),
    );
    if let Some(query) = query {
        spec.insert("query".to_string(), query);
    }
    Value::Object(spec)
}

pub fn signed_request_spec(
    method: &str,
    path: &str,
    query_string: &str,
    body: Option<Value>,
) -> Value {
    let mut spec = serde_json::Map::new();
    spec.insert("method".to_string(), json!(method));
    spec.insert("path".to_string(), json!(path));
    spec.insert("auth".to_string(), json!("hmac_sha256"));
    if !query_string.is_empty() {
        spec.insert("query_string".to_string(), json!(query_string));
    }
    spec.insert(
        "headers".to_string(),
        json!({
            "X-FB-ACCESS-KEY": "<redacted>",
            "X-FB-ACCESS-TIMESTAMP": "1700000000000",
            "X-FB-ACCESS-SIGNATURE": "<computed>",
            "Content-Type": "application/json"
        }),
    );
    if let Some(body) = body {
        spec.insert("body".to_string(), body);
    }
    Value::Object(spec)
}
