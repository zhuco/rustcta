use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{sign_payload, signature_payload};

#[derive(Clone)]
pub struct DigiFinexRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    swap_rest_base_url: String,
    http: reqwest::Client,
}

impl DigiFinexRest {
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

    pub async fn send_public_get(
        &self,
        market_is_swap: bool,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let base = if market_is_swap {
            &self.swap_rest_base_url
        } else {
            &self.spot_rest_base_url
        };
        let response = self
            .http
            .get(build_url(base, endpoint, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_signed_get(
        &self,
        market_is_swap: bool,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            market_is_swap,
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
        market_is_swap: bool,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            market_is_swap,
            reqwest::Method::POST,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn send_signed_delete(
        &self,
        market_is_swap: bool,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            market_is_swap,
            reqwest::Method::DELETE,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_signed_json(
        &self,
        market_is_swap: bool,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            market_is_swap,
            reqwest::Method::POST,
            endpoint,
            &HashMap::new(),
            Some(body),
            api_key,
            api_secret,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        market_is_swap: bool,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let base = if market_is_swap {
            &self.swap_rest_base_url
        } else {
            &self.spot_rest_base_url
        };
        let query = build_query_string(params);
        let path_with_query = if query.is_empty() {
            endpoint.to_string()
        } else {
            format!("{endpoint}?{query}")
        };
        let body_text = match body {
            Some(body) => {
                serde_json::to_string(body).map_err(|error| ExchangeApiError::InvalidRequest {
                    message: format!("failed to serialize DigiFinex JSON body: {error}"),
                })?
            }
            None => String::new(),
        };
        let timestamp_ms = Utc::now().timestamp_millis();
        let signature = sign_payload(
            api_secret,
            &signature_payload(timestamp_ms, method.as_str(), &path_with_query, &body_text),
        )?;
        let url = format!("{}{}", base.trim_end_matches('/'), path_with_query);
        let mut request = self
            .http
            .request(method, url)
            .header("ACCESS-KEY", api_key)
            .header("ACCESS-TIMESTAMP", timestamp_ms.to_string())
            .header("ACCESS-SIGN", signature);
        if body.is_some() {
            request = request
                .header("Content-Type", "application/json")
                .body(body_text);
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

pub fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params
        .iter()
        .filter(|(_, value)| !value.trim().is_empty())
        .collect::<Vec<_>>();
    pairs.sort_by(|(left, _), (right, _)| left.cmp(right));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let query = build_query_string(params);
    if query.is_empty() {
        format!("{}{}", base.trim_end_matches('/'), endpoint)
    } else {
        format!("{}{}?{}", base.trim_end_matches('/'), endpoint, query)
    }
}

fn encode(value: &str) -> String {
    urlencoding::encode(value).into_owned()
}

async fn parse_response(
    exchange_id: ExchangeId,
    response: reqwest::Response,
) -> ExchangeApiResult<Value> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    let value = if text.trim().is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&text).map_err(|error| {
            ExchangeApiError::Exchange(ExchangeError {
                schema_version: rustcta_types::SchemaVersion::current(),
                exchange_id: exchange_id.clone(),
                class: ExchangeErrorClass::Decode,
                code: Some("digifinex_decode".to_string()),
                message: format!("failed to parse DigiFinex response: {error}"),
                retry_after_ms: None,
                order_id: None,
                client_order_id: None,
                raw: Some(Value::String(text.clone())),
                occurred_at: Utc::now(),
            })
        })?
    };
    if status.is_success() && is_success_payload(&value) {
        return Ok(value);
    }
    let code = value
        .get("code")
        .or_else(|| value.get("error_code"))
        .and_then(|value| {
            value
                .as_i64()
                .map(|n| n.to_string())
                .or_else(|| value.as_str().map(str::to_string))
        });
    let message = value
        .get("msg")
        .or_else(|| value.get("message"))
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
        .unwrap_or_else(|| {
            status
                .canonical_reason()
                .unwrap_or("DigiFinex request failed")
        });
    Err(ExchangeApiError::Exchange(ExchangeError {
        schema_version: rustcta_types::SchemaVersion::current(),
        exchange_id,
        class: classify_error(code.as_deref(), status.as_u16(), message),
        code,
        message: message.to_string(),
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value),
        occurred_at: Utc::now(),
    }))
}

fn is_success_payload(value: &Value) -> bool {
    match value.get("code").or_else(|| value.get("error_code")) {
        None => true,
        Some(Value::Number(number)) => number.as_i64().is_some_and(|code| code == 0 || code == 200),
        Some(Value::String(text)) => matches!(text.as_str(), "0" | "200"),
        _ => true,
    }
}

fn classify_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let lower = message.to_ascii_lowercase();
    if status == 401 || status == 403 || lower.contains("sign") || lower.contains("auth") {
        ExchangeErrorClass::Authentication
    } else if status == 429 || lower.contains("rate") || lower.contains("frequency") {
        ExchangeErrorClass::RateLimited
    } else if lower.contains("balance") || lower.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if lower.contains("not found") || lower.contains("not exist") || code == Some("10009") {
        ExchangeErrorClass::OrderNotFound
    } else if status >= 500 {
        ExchangeErrorClass::ExchangeUnavailable
    } else {
        ExchangeErrorClass::InvalidRequest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digifinex_query_string_sorts_keys() {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), "btc_usdt".to_string());
        params.insert("limit".to_string(), "100".to_string());
        assert_eq!(build_query_string(&params), "limit=100&symbol=btc_usdt");
    }
}
