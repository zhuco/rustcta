#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::ndax_signed_headers;

pub const GATEWAY_PATH: &str = "/WSGateway/";

#[derive(Clone)]
pub struct NdaxRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl NdaxRest {
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

    pub async fn send_public_call(
        &self,
        request_id: i64,
        function_name: &str,
        payload: Value,
    ) -> ExchangeApiResult<Value> {
        let url = format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            GATEWAY_PATH
        );
        let response = self
            .http
            .post(url)
            .json(&ndax_gateway_call(request_id, function_name, payload))
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
        params: &BTreeMap<String, String>,
        api_key: &str,
        api_secret: &str,
        passphrase: Option<&str>,
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let headers = ndax_signed_headers(
            api_key, api_secret, passphrase, &timestamp, "GET", endpoint, "",
        )?;
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
        let mut request = self
            .http
            .get(url)
            .header("X-NDAX-APIKEY", headers["X-NDAX-APIKEY"].as_str())
            .header("X-NDAX-SIGNATURE", headers["X-NDAX-SIGNATURE"].as_str())
            .header("X-NDAX-TIMESTAMP", headers["X-NDAX-TIMESTAMP"].as_str())
            .header("Accept", "application/json");
        if let Some(passphrase) = headers.get("X-NDAX-PASSPHRASE") {
            request = request.header("X-NDAX-PASSPHRASE", passphrase.as_str());
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
    if !status.is_success() || ndax_error_message(&value).is_some() {
        let message = ndax_error_message(&value).unwrap_or_else(|| "NDAX API error".to_string());
        let mut error = ExchangeError::new(
            exchange_id,
            classify_ndax_error(&message),
            message,
            Utc::now(),
        );
        error.code = Some(status.as_u16().to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    decode_gateway_payload(&value)
}

pub fn ndax_gateway_call(request_id: i64, function_name: &str, payload: Value) -> Value {
    json!({
        "m": 0,
        "i": request_id,
        "n": function_name,
        "o": payload.to_string()
    })
}

pub fn decode_gateway_payload(value: &Value) -> ExchangeApiResult<Value> {
    match value.get("o") {
        Some(Value::String(inner)) if !inner.trim().is_empty() => serde_json::from_str(inner)
            .map_err(|error| ExchangeApiError::InvalidRequest {
                message: format!("NDAX gateway payload is not JSON: {error}"),
            }),
        Some(inner) if !inner.is_null() => Ok(inner.clone()),
        _ => Ok(value.clone()),
    }
}

pub fn public_gateway_request_spec(function_name: &str, payload: Value) -> Value {
    json!({
        "method": "POST",
        "path": GATEWAY_PATH,
        "auth": "none",
        "headers": {
            "Accept": "application/json",
            "Content-Type": "application/json"
        },
        "body": ndax_gateway_call(1, function_name, payload)
    })
}

pub fn signed_rest_request_spec(method: &str, path: &str, body: Option<Value>) -> Value {
    let mut spec = json!({
        "method": method,
        "path": path,
        "auth": "ndax_hmac_sha256_base64",
        "headers": {
            "X-NDAX-APIKEY": "<redacted:api_key>",
            "X-NDAX-SIGNATURE": "<redacted:signature>",
            "X-NDAX-TIMESTAMP": "<timestamp>"
        }
    });
    if let Some(body) = body {
        spec["body"] = body;
    }
    spec
}

fn ndax_error_message(value: &Value) -> Option<String> {
    if value.get("errorcode").and_then(Value::as_i64).unwrap_or(0) != 0 {
        return value
            .get("errormsg")
            .or_else(|| value.get("detail"))
            .and_then(Value::as_str)
            .map(ToString::to_string);
    }
    value
        .get("error")
        .or_else(|| value.get("Error"))
        .or_else(|| value.get("message"))
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty())
        .map(ToString::to_string)
}

fn classify_ndax_error(message: &str) -> ExchangeErrorClass {
    let lower = message.to_ascii_lowercase();
    if lower.contains("authorized")
        || lower.contains("signature")
        || lower.contains("apikey")
        || lower.contains("api key")
    {
        ExchangeErrorClass::Authentication
    } else if lower.contains("rate") || lower.contains("limit") {
        ExchangeErrorClass::RateLimited
    } else if lower.contains("instrument") || lower.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if lower.contains("not found") || lower.contains("resource") {
        ExchangeErrorClass::OrderNotFound
    } else {
        ExchangeErrorClass::ExchangeUnavailable
    }
}
