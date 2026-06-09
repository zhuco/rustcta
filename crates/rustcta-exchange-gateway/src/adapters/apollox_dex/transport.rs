use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

#[derive(Clone)]
pub struct ApolloxDexRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl ApolloxDexRest {
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
        path: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(self.url(path, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(&self.exchange_id, response).await
    }

    pub async fn send_signed_post(
        &self,
        path: &str,
        params: &HashMap<String, String>,
        api_key: &str,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .post(self.url(path, params))
            .header("X-MBX-APIKEY", api_key)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(&self.exchange_id, response).await
    }

    pub async fn send_signed_get(
        &self,
        path: &str,
        params: &HashMap<String, String>,
        api_key: &str,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(self.url(path, params))
            .header("X-MBX-APIKEY", api_key)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(&self.exchange_id, response).await
    }

    pub fn exchange_info_path() -> &'static str {
        "/fapi/v1/exchangeInfo"
    }

    pub fn depth_path() -> &'static str {
        "/fapi/v1/depth"
    }

    pub fn place_order_path() -> &'static str {
        "/fapi/v1/order"
    }

    pub fn open_orders_path() -> &'static str {
        "/fapi/v1/openOrders"
    }

    pub fn user_trades_path() -> &'static str {
        "/fapi/v1/userTrades"
    }

    fn url(&self, path: &str, params: &HashMap<String, String>) -> String {
        let mut url = format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            normalized_path(path)
        );
        if !params.is_empty() {
            url.push('?');
            url.push_str(
                &crate::adapters::apollox_dex::signing::apollox_query_string(
                    params
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone())),
                ),
            );
        }
        url
    }
}

async fn parse_response(
    exchange_id: &ExchangeId,
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
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str::<Value>(&text).map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?
    };
    if !status.is_success() {
        let code = value
            .get("code")
            .and_then(Value::as_i64)
            .map(|code| code.to_string());
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("ApolloX DEX REST request failed");
        let mut error = ExchangeError::new(
            exchange_id.clone(),
            classify_apollox_error(code.as_deref(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_apollox_error(code: Option<&str>, message: &str) -> ExchangeErrorClass {
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
    } else if matches!(code, "-1003" | "-1015") || msg.contains("too many requests") {
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

fn normalized_path(path: &str) -> String {
    if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}
