#![cfg_attr(not(test), allow(dead_code))]

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::novadax_private_request_headers;

#[derive(Clone)]
pub struct NovadaxRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl NovadaxRest {
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
            let mut pairs = params.iter().collect::<Vec<_>>();
            pairs.sort_by(|left, right| left.0.cmp(right.0));
            let query = pairs
                .into_iter()
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

    pub async fn send_signed_json(
        &self,
        method: &str,
        endpoint: &str,
        api_key: &str,
        api_secret: &str,
        body: &Value,
    ) -> ExchangeApiResult<Value> {
        let body_text =
            serde_json::to_string(body).map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?;
        let timestamp = Utc::now().timestamp_millis().to_string();
        let headers = novadax_private_request_headers(
            api_key,
            api_secret,
            &timestamp,
            method,
            endpoint,
            &std::collections::BTreeMap::new(),
            Some(&body_text),
        )?;
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let response = self
            .http
            .post(url)
            .header("X-Nova-Access-Key", headers["X-Nova-Access-Key"].as_str())
            .header("X-Nova-Signature", headers["X-Nova-Signature"].as_str())
            .header("X-Nova-Timestamp", headers["X-Nova-Timestamp"].as_str())
            .header("Content-Type", "application/json")
            .body(body_text)
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
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let headers = novadax_private_request_headers(
            api_key, api_secret, &timestamp, "GET", endpoint, params, None,
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
        let response = self
            .http
            .get(url)
            .header("X-Nova-Access-Key", headers["X-Nova-Access-Key"].as_str())
            .header("X-Nova-Signature", headers["X-Nova-Signature"].as_str())
            .header("X-Nova-Timestamp", headers["X-Nova-Timestamp"].as_str())
            .header("Accept", "application/json")
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
        || value
            .get("code")
            .and_then(Value::as_str)
            .is_some_and(|code| code != "A10000")
    {
        let message = value
            .get("message")
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("NovaDAX API error");
        let mut error = ExchangeError::new(
            exchange_id,
            ExchangeErrorClass::ExchangeUnavailable,
            message,
            Utc::now(),
        );
        error.code = Some(
            value
                .get("code")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .unwrap_or_else(|| status.as_u16().to_string()),
        );
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

pub fn public_get_request_spec(path: &str, params: HashMap<String, String>) -> Value {
    let mut spec = serde_json::Map::new();
    spec.insert("method".to_string(), json!("GET"));
    spec.insert("path".to_string(), json!(path));
    spec.insert("auth".to_string(), json!("none"));
    if !params.is_empty() {
        spec.insert("query".to_string(), json!(params));
    }
    spec.insert(
        "headers".to_string(),
        json!({ "Accept": "application/json" }),
    );
    Value::Object(spec)
}
