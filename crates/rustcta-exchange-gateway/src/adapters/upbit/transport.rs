use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;
use uuid::Uuid;

use super::signing::{build_query_string, upbit_jwt};

#[derive(Clone)]
pub struct UpbitRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl UpbitRest {
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

    pub async fn public_get(
        &self,
        path: &str,
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(self.url(path, params))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    pub async fn signed_get(
        &self,
        access_key: &str,
        secret_key: &str,
        path: &str,
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        self.signed_request(
            reqwest::Method::GET,
            access_key,
            secret_key,
            path,
            params,
            None,
        )
        .await
    }

    pub async fn signed_delete(
        &self,
        access_key: &str,
        secret_key: &str,
        path: &str,
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        self.signed_request(
            reqwest::Method::DELETE,
            access_key,
            secret_key,
            path,
            params,
            None,
        )
        .await
    }

    pub async fn signed_post(
        &self,
        access_key: &str,
        secret_key: &str,
        path: &str,
        body_params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        self.signed_request(
            reqwest::Method::POST,
            access_key,
            secret_key,
            path,
            body_params,
            Some(params_object(body_params)),
        )
        .await
    }

    async fn signed_request(
        &self,
        method: reqwest::Method,
        access_key: &str,
        secret_key: &str,
        path: &str,
        signed_params: &[(String, String)],
        body: Option<Value>,
    ) -> ExchangeApiResult<Value> {
        let nonce = Uuid::new_v4().to_string();
        let jwt = upbit_jwt(access_key, secret_key, signed_params, &nonce)?;
        let mut request = self
            .http
            .request(
                method.clone(),
                if method == reqwest::Method::GET || method == reqwest::Method::DELETE {
                    self.url(path, signed_params)
                } else {
                    self.url(path, &[])
                },
            )
            .header("Authorization", format!("Bearer {}", jwt.token))
            .header("Accept", "application/json");
        if let Some(body) = body {
            request = request.header("Content-Type", "application/json");
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    fn url(&self, path: &str, params: &[(String, String)]) -> String {
        let base = format!("{}{}", self.rest_base_url.trim_end_matches('/'), path);
        let query = build_query_string(params);
        if query.is_empty() {
            base
        } else {
            format!("{base}?{query}")
        }
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
    if !status.is_success() || value.get("error").is_some() {
        let (code, message) = upbit_error(&value).unwrap_or_else(|| {
            (
                status.as_u16().to_string(),
                "Upbit request failed".to_string(),
            )
        });
        let mut error = ExchangeError::new(
            exchange_id,
            classify_error(&code, &message),
            message,
            Utc::now(),
        );
        error.code = Some(code);
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn upbit_error(value: &Value) -> Option<(String, String)> {
    let error = value.get("error")?;
    let code = error
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| error.get("name").map(Value::to_string))
        .unwrap_or_else(|| "unknown".to_string());
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("Upbit API error")
        .to_string();
    Some((code, message))
}

fn classify_error(code: &str, message: &str) -> ExchangeErrorClass {
    let text = format!("{code} {message}").to_ascii_lowercase();
    if text.contains("jwt") || text.contains("auth") || text.contains("nonce") {
        ExchangeErrorClass::Authentication
    } else if text.contains("out_of_scope") || text.contains("permission") {
        ExchangeErrorClass::Permission
    } else if text.contains("too many") || text.contains("rate") {
        ExchangeErrorClass::RateLimited
    } else if text.contains("insufficient") || text.contains("balance") {
        ExchangeErrorClass::InsufficientBalance
    } else if text.contains("market") || text.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if text.contains("not found") || text.contains("order") {
        ExchangeErrorClass::OrderNotFound
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn params_object(params: &[(String, String)]) -> Value {
    let mut map = serde_json::Map::new();
    for (key, value) in params {
        map.insert(key.clone(), Value::String(value.clone()));
    }
    Value::Object(map)
}
