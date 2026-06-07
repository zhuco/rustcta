use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{sign_futures_payload, sign_raw_query};

#[derive(Clone)]
pub struct BitrueRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    futures_rest_base_url: String,
    futures_ws_auth_base_url: String,
    http: reqwest::Client,
}

impl BitrueRest {
    pub fn new(
        exchange_id: ExchangeId,
        spot_rest_base_url: String,
        futures_rest_base_url: String,
        futures_ws_auth_base_url: String,
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
            futures_rest_base_url,
            futures_ws_auth_base_url,
            http,
        })
    }

    pub async fn send_spot_public_request(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(build_url(&self.spot_rest_base_url, endpoint, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_futures_public_request(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(build_url(&self.futures_rest_base_url, endpoint, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_spot_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_spot_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_spot_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_spot_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_spot_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_spot_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_spot_api_key_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        api_key: &str,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .request(
                method,
                format!(
                    "{}{}",
                    self.spot_rest_base_url.trim_end_matches('/'),
                    endpoint
                ),
            )
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    async fn send_spot_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        let mut signed_params = params.clone();
        signed_params.insert(
            "timestamp".to_string(),
            Utc::now().timestamp_millis().to_string(),
        );
        signed_params.insert("recvWindow".to_string(), recv_window_ms.to_string());
        let query = build_query_string(&signed_params);
        let signature = sign_raw_query(api_secret, &query)?;
        let url = build_signed_url(
            &self.spot_rest_base_url,
            endpoint,
            &signed_params,
            &signature,
        );
        let response = self
            .http
            .request(method, url)
            .header("X-MBX-APIKEY", api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_futures_signed_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        let mut signed_params = params.clone();
        signed_params.insert("recvWindow".to_string(), recv_window_ms.to_string());
        let timestamp = Utc::now().timestamp_millis();
        let endpoint_with_query = if signed_params.is_empty() {
            endpoint.to_string()
        } else {
            format!("{}?{}", endpoint, build_query_string(&signed_params))
        };
        let signature =
            sign_futures_payload(api_secret, timestamp, "GET", &endpoint_with_query, "")?;
        let response = self
            .http
            .get(format!(
                "{}{}",
                self.futures_rest_base_url.trim_end_matches('/'),
                endpoint_with_query
            ))
            .header("X-CH-APIKEY", api_key)
            .header("X-CH-SIGN", signature)
            .header("X-CH-TS", timestamp.to_string())
            .header("Content-Type", "application/json")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_futures_signed_post(
        &self,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        let mut body = body.clone();
        if let Value::Object(fields) = &mut body {
            fields.insert("recvWindow".to_string(), Value::from(recv_window_ms));
        }
        let body_text =
            serde_json::to_string(&body).map_err(|error| ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            })?;
        let timestamp = Utc::now().timestamp_millis();
        let signature = sign_futures_payload(api_secret, timestamp, "POST", endpoint, &body_text)?;
        let response = self
            .http
            .post(format!(
                "{}{}",
                self.futures_rest_base_url.trim_end_matches('/'),
                endpoint
            ))
            .header("X-CH-APIKEY", api_key)
            .header("X-CH-SIGN", signature)
            .header("X-CH-TS", timestamp.to_string())
            .header("Content-Type", "application/json")
            .body(body_text)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_futures_ws_auth_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        let mut body = body.clone();
        if let Value::Object(fields) = &mut body {
            fields.insert("recvWindow".to_string(), Value::from(recv_window_ms));
        }
        let body_text = if method == reqwest::Method::GET || method == reqwest::Method::DELETE {
            String::new()
        } else {
            serde_json::to_string(&body).map_err(|error| ExchangeApiError::InvalidRequest {
                message: error.to_string(),
            })?
        };
        let timestamp = Utc::now().timestamp_millis();
        let signature =
            sign_futures_payload(api_secret, timestamp, method.as_str(), endpoint, &body_text)?;
        let mut request = self
            .http
            .request(
                method,
                format!(
                    "{}{}",
                    self.futures_ws_auth_base_url.trim_end_matches('/'),
                    endpoint
                ),
            )
            .header("X-CH-APIKEY", api_key)
            .header("X-CH-SIGN", signature)
            .header("X-CH-TS", timestamp.to_string())
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
    let code = value.get("code").and_then(value_as_code);
    if !status.is_success()
        || code
            .as_deref()
            .is_some_and(|code| !matches!(code, "0" | "200"))
    {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("Bitrue request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_bitrue_error(code.as_deref(), status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_bitrue_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match (status, code) {
        (401, _) | (_, "-1022") | (_, "-2015") => ExchangeErrorClass::Authentication,
        (403, _) | (_, "-2014") => ExchangeErrorClass::Permission,
        (418 | 429, _) => ExchangeErrorClass::RateLimited,
        (500..=599, _) | (_, "-1016") | (_, "503") => ExchangeErrorClass::ExchangeUnavailable,
        (_, "-1102") | (_, "-1020") => ExchangeErrorClass::InvalidRequest,
        (_, "-1121") => ExchangeErrorClass::InvalidSymbol,
        (_, "-2013") => ExchangeErrorClass::OrderNotFound,
        (_, "-2010") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("duplicate") => ExchangeErrorClass::DuplicateClientOrderId,
        _ if msg.contains("order") && msg.contains("exist") => ExchangeErrorClass::OrderNotFound,
        _ if msg.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ => ExchangeErrorClass::Unknown,
    }
}

fn value_as_code(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        build_request_path(endpoint, params)
    )
}

fn build_signed_url(
    base: &str,
    endpoint: &str,
    params: &HashMap<String, String>,
    signature: &str,
) -> String {
    let mut signed_params = params.clone();
    signed_params.insert("signature".to_string(), signature.to_string());
    build_url(base, endpoint, &signed_params)
}

fn build_request_path(endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = endpoint.to_string();
    if !params.is_empty() {
        url.push(if endpoint.contains('?') { '&' } else { '?' });
        url.push_str(&build_query_string(params));
    }
    url
}

fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
