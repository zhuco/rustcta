#![cfg_attr(not(test), allow(dead_code))]

use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::{bitso_authorization_header, bitso_hmac_signature, bitso_signature_payload};

#[derive(Clone)]
pub struct BitsoRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl BitsoRest {
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

    pub async fn send_signed_get(
        &self,
        api_key: &str,
        api_secret: &str,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        if api_key.trim().is_empty() || api_secret.trim().is_empty() {
            return Err(ExchangeApiError::Unsupported {
                operation: "bitso.private_rest_missing_credentials",
            });
        }
        let request_path = build_path(endpoint, params);
        let url = format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            request_path
        );
        let signing_path = signing_path(&self.rest_base_url, &request_path);
        let nonce = Utc::now().timestamp_millis();
        let payload = bitso_signature_payload(nonce, "GET", &signing_path, "");
        let signature = bitso_hmac_signature(api_secret, &payload)?;
        let response = self
            .http
            .get(url)
            .header(
                "Authorization",
                bitso_authorization_header(api_key, nonce, &signature),
            )
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
    let success = value
        .get("success")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    if !status.is_success() || !success {
        let message = value
            .get("error")
            .and_then(|error| error.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Bitso API error");
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

pub fn public_get_request_spec(path: &str, query: Value) -> Value {
    json!({
        "method": "GET",
        "path": path,
        "auth": "none",
        "query": query,
        "headers": { "Accept": "application/json" }
    })
}

pub fn signed_request_spec(method: &str, path: &str, body: Value) -> Value {
    json!({
        "method": method,
        "path": path,
        "auth": "bitso_hmac_sha256",
        "headers": {
            "Authorization": "Bitso <key>:<nonce>:<signature>",
            "Content-Type": "application/json"
        },
        "body": body
    })
}

pub fn build_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let endpoint = if endpoint.starts_with('/') {
        endpoint.to_string()
    } else {
        format!("/{endpoint}")
    };
    if params.is_empty() {
        endpoint
    } else {
        format!("{endpoint}?{}", encode_params(params))
    }
}

fn signing_path(rest_base_url: &str, request_path: &str) -> String {
    let base_path = url::Url::parse(rest_base_url)
        .ok()
        .map(|url| url.path().trim_end_matches('/').to_string())
        .filter(|path| !path.is_empty() && path != "/")
        .unwrap_or_default();
    if base_path.is_empty() {
        request_path.to_string()
    } else {
        format!("{base_path}{request_path}")
    }
}

fn encode_params(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| {
            format!(
                "{}={}",
                urlencoding::encode(key),
                urlencoding::encode(value)
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}
