use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{build_query_string, sign_coinstore_payload, CoinstorePrivateCredentials};

#[derive(Clone)]
pub struct CoinstoreRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    futures_rest_base_url: String,
    credentials: Option<CoinstorePrivateCredentials>,
    http: reqwest::Client,
}

impl CoinstoreRest {
    pub fn new(
        exchange_id: ExchangeId,
        spot_rest_base_url: String,
        futures_rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<CoinstorePrivateCredentials>,
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
            credentials,
            http,
        })
    }

    pub async fn send_spot_public_get(
        &self,
        endpoint: &str,
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        self.send_public_get(&self.spot_rest_base_url, endpoint, params)
            .await
    }

    pub async fn send_futures_public_get(
        &self,
        endpoint: &str,
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        self.send_public_get(&self.futures_rest_base_url, endpoint, params)
            .await
    }

    pub async fn send_spot_public_post(
        &self,
        endpoint: &str,
        body: Value,
    ) -> ExchangeApiResult<Value> {
        self.send_public_post(&self.spot_rest_base_url, endpoint, body)
            .await
    }

    pub async fn send_spot_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        self.send_signed(
            Method::GET,
            operation,
            &self.spot_rest_base_url,
            endpoint,
            params,
            None,
        )
        .await
    }

    pub async fn send_futures_signed_get(
        &self,
        operation: &'static str,
        endpoint: &str,
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        self.send_signed(
            Method::GET,
            operation,
            &self.futures_rest_base_url,
            endpoint,
            params,
            None,
        )
        .await
    }

    pub async fn send_spot_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed(
            Method::POST,
            operation,
            &self.spot_rest_base_url,
            endpoint,
            &[],
            Some(body),
        )
        .await
    }

    pub async fn send_futures_signed_post(
        &self,
        operation: &'static str,
        endpoint: &str,
        body: Value,
    ) -> ExchangeApiResult<Value> {
        self.send_signed(
            Method::POST,
            operation,
            &self.futures_rest_base_url,
            endpoint,
            &[],
            Some(body),
        )
        .await
    }

    async fn send_public_get(
        &self,
        base_url: &str,
        endpoint: &str,
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        let url = build_url(base_url, endpoint, params);
        let response =
            self.http
                .get(url)
                .send()
                .await
                .map_err(|error| ExchangeApiError::Transport {
                    message: error.to_string(),
                })?;
        parse_coinstore_response(self.exchange_id.clone(), response).await
    }

    async fn send_public_post(
        &self,
        base_url: &str,
        endpoint: &str,
        body: Value,
    ) -> ExchangeApiResult<Value> {
        let body_text = if body.is_null() {
            String::new()
        } else {
            body.to_string()
        };
        let url = format!("{}{}", base_url.trim_end_matches('/'), endpoint);
        let response = self
            .http
            .post(url)
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .body(body_text)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_coinstore_response(self.exchange_id.clone(), response).await
    }

    async fn send_signed(
        &self,
        method: Method,
        operation: &'static str,
        base_url: &str,
        endpoint: &str,
        params: &[(String, String)],
        body: Option<Value>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported { operation })?;
        let query = build_query_string(params);
        let body_text = body
            .as_ref()
            .filter(|value| !value.is_null())
            .map(Value::to_string)
            .unwrap_or_default();
        let expires_ms = Utc::now().timestamp_millis();
        let payload = format!("{query}{body_text}");
        let signature = sign_coinstore_payload(&credentials.api_secret, expires_ms, &payload)?;
        let url = build_url(base_url, endpoint, params);
        let builder = match method {
            Method::GET => self.http.get(url),
            Method::POST => self.http.post(url),
            _ => {
                return Err(ExchangeApiError::Unsupported {
                    operation: "coinstore.http_method",
                });
            }
        };
        let response = builder
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("X-CS-APIKEY", &credentials.api_key)
            .header("X-CS-EXPIRES", expires_ms.to_string())
            .header("X-CS-SIGN", signature)
            .body(body_text)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_coinstore_response(self.exchange_id.clone(), response).await
    }
}

pub fn build_url(base_url: &str, endpoint: &str, params: &[(String, String)]) -> String {
    let mut url = format!("{}{}", base_url.trim_end_matches('/'), endpoint);
    let query = build_query_string(params);
    if !query.is_empty() {
        url.push('?');
        url.push_str(&query);
    }
    url
}

async fn parse_coinstore_response(
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
        .or_else(|| value.get("status"))
        .and_then(|value| value.as_i64().or_else(|| value.as_str()?.parse().ok()));
    let success = value.get("success").and_then(Value::as_bool);
    if !status.is_success()
        || code.is_some_and(|code| code != 0 && code != 200)
        || success.is_some_and(|success| !success)
    {
        let message = value
            .get("message")
            .or_else(|| value.get("msg"))
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("Coinstore request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_coinstore_error(code, message),
            message,
            Utc::now(),
        );
        error.code = code.map(|code| code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_coinstore_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    match code {
        Some(401) | Some(403) => ExchangeErrorClass::Authentication,
        Some(429) => ExchangeErrorClass::RateLimited,
        _ if message.contains("signature") || message.contains("api key") => {
            ExchangeErrorClass::Authentication
        }
        _ if message.contains("permission") || message.contains("whitelist") => {
            ExchangeErrorClass::Permission
        }
        _ if message.contains("balance") || message.contains("insufficient") => {
            ExchangeErrorClass::InsufficientBalance
        }
        _ if message.contains("not found") && message.contains("order") => {
            ExchangeErrorClass::OrderNotFound
        }
        _ if message.contains("rate") || message.contains("too many") => {
            ExchangeErrorClass::RateLimited
        }
        _ => ExchangeErrorClass::InvalidRequest,
    }
}

#[cfg(test)]
pub(super) fn classify_coinstore_error_for_test(
    code: Option<i64>,
    message: &str,
) -> ExchangeErrorClass {
    classify_coinstore_error(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coinstore_build_url_should_append_query() {
        let params = vec![("symbol".to_string(), "BTCUSDT".to_string())];
        assert_eq!(
            build_url(
                "https://api.coinstore.com/api/",
                "/v1/market/tickers",
                &params
            ),
            "https://api.coinstore.com/api/v1/market/tickers?symbol=BTCUSDT"
        );
    }
}
