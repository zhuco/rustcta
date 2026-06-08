use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::parser::exchange_error;
use super::signing::{bithumb_jwt_now, BithumbPrivateCredentials};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BithumbHttpRequestShape {
    pub method: String,
    pub path: String,
    pub query: BTreeMap<String, String>,
    pub body: Option<Value>,
    pub auth_query: String,
}

#[derive(Clone)]
pub struct BithumbRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<BithumbPrivateCredentials>,
}

impl BithumbRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<BithumbPrivateCredentials>,
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
            credentials,
        })
    }

    pub async fn send_public_get(
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
        params: &BTreeMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(Method::GET, endpoint, params, None)
            .await
    }

    pub async fn send_signed_post(&self, endpoint: &str, body: &Value) -> ExchangeApiResult<Value> {
        let auth_query = body_query_string(body)?;
        self.send_signed_request_with_query(Method::POST, endpoint, &auth_query, None, Some(body))
            .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &BTreeMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(Method::DELETE, endpoint, params, None)
            .await
    }

    async fn send_signed_request(
        &self,
        method: Method,
        endpoint: &str,
        params: &BTreeMap<String, String>,
        body: Option<&Value>,
    ) -> ExchangeApiResult<Value> {
        let auth_query = build_query_string(params.iter());
        self.send_signed_request_with_query(method, endpoint, &auth_query, Some(params), body)
            .await
    }

    async fn send_signed_request_with_query(
        &self,
        method: Method,
        endpoint: &str,
        auth_query: &str,
        params: Option<&BTreeMap<String, String>>,
        body: Option<&Value>,
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "bithumb.private_rest_missing_credentials",
            })?;
        let token = bithumb_jwt_now(
            &credentials.api_key,
            &credentials.api_secret,
            auth_query,
            Utc::now().timestamp_millis(),
        )?
        .token;
        let mut url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        if let Some(params) = params.filter(|params| !params.is_empty()) {
            url.push('?');
            url.push_str(&build_query_string(params.iter()));
        }
        let mut request = self
            .http
            .request(method, url)
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json; charset=utf-8");
        if let Some(body) = body {
            request = request.json(body);
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

pub fn public_request_shape(
    method: &str,
    endpoint: &str,
    params: &HashMap<String, String>,
) -> BithumbHttpRequestShape {
    BithumbHttpRequestShape {
        method: method.to_string(),
        path: endpoint.to_string(),
        query: params
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        body: None,
        auth_query: build_query_string(params.iter()),
    }
}

pub fn signed_get_shape(
    method: &str,
    endpoint: &str,
    params: &BTreeMap<String, String>,
) -> BithumbHttpRequestShape {
    BithumbHttpRequestShape {
        method: method.to_string(),
        path: endpoint.to_string(),
        query: params.clone(),
        body: None,
        auth_query: build_query_string(params.iter()),
    }
}

pub fn signed_body_shape(
    method: &str,
    endpoint: &str,
    body: Value,
) -> ExchangeApiResult<BithumbHttpRequestShape> {
    Ok(BithumbHttpRequestShape {
        method: method.to_string(),
        path: endpoint.to_string(),
        query: BTreeMap::new(),
        auth_query: body_query_string(&body)?,
        body: Some(body),
    })
}

pub fn body_query_string(body: &Value) -> ExchangeApiResult<String> {
    let object = body
        .as_object()
        .ok_or_else(|| ExchangeApiError::Serialization {
            message: "Bithumb signed body must be a JSON object".to_string(),
        })?;
    Ok(build_query_string(object.iter().map(|(key, value)| {
        (
            key,
            match value {
                Value::String(text) => text.clone(),
                Value::Number(number) => number.to_string(),
                Value::Bool(value) => value.to_string(),
                other => other.to_string(),
            },
        )
    })))
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        url.push('?');
        url.push_str(&build_query_string(params.iter()));
    }
    url
}

pub fn build_query_string<'a>(
    pairs: impl IntoIterator<Item = (&'a String, impl ToString)>,
) -> String {
    let mut pairs = pairs
        .into_iter()
        .map(|(key, value)| (key.clone(), value.to_string()))
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={}", urlencoding::encode(&value)))
        .collect::<Vec<_>>()
        .join("&")
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
    let message = value
        .get("message")
        .or_else(|| value.get("error"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    if !status.is_success() || message.is_some() && value.get("name").is_some() {
        let class = classify_bithumb_error(status.as_u16(), message.as_deref().unwrap_or_default());
        let code = value.get("name").or_else(|| value.get("code")).map(|code| {
            code.as_str()
                .map(ToString::to_string)
                .unwrap_or_else(|| code.to_string())
        });
        return Err(exchange_error(
            exchange_id,
            class,
            message.as_deref().unwrap_or("Bithumb request failed"),
            code,
            Some(value),
        ));
    }
    Ok(value)
}

fn classify_bithumb_error(status: u16, message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    match status {
        401 | 403 => ExchangeErrorClass::Authentication,
        404 => ExchangeErrorClass::OrderNotFound,
        418 | 429 => ExchangeErrorClass::RateLimited,
        500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ if message.contains("permission") => ExchangeErrorClass::Permission,
        _ if message.contains("insufficient") || message.contains("balance") => {
            ExchangeErrorClass::InsufficientBalance
        }
        _ if message.contains("market") || message.contains("symbol") => {
            ExchangeErrorClass::InvalidSymbol
        }
        _ if message.contains("precision") || message.contains("minimum") => {
            ExchangeErrorClass::InvalidPrecision
        }
        _ if message.contains("rate") || message.contains("limit") => {
            ExchangeErrorClass::RateLimited
        }
        _ => ExchangeErrorClass::Unknown,
    }
}
