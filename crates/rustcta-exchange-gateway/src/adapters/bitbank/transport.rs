use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing;

#[derive(Clone)]
pub struct BitbankRest {
    exchange_id: ExchangeId,
    public_rest_base_url: String,
    private_rest_base_url: String,
    http: reqwest::Client,
}

impl BitbankRest {
    pub fn new(
        exchange_id: ExchangeId,
        public_rest_base_url: String,
        private_rest_base_url: String,
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
            public_rest_base_url,
            private_rest_base_url,
            http,
        })
    }

    pub async fn send_public_request(
        &self,
        path: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.public_rest_base_url, path, params);
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

    pub async fn send_signed_post(
        &self,
        api_key: &str,
        api_secret: &str,
        path: &str,
        body: &str,
    ) -> ExchangeApiResult<Value> {
        let nonce = Utc::now().timestamp_millis().to_string();
        let signed = signing::sign_body_request(api_key, api_secret, &nonce, "POST", body)?;
        let url = build_url(&self.private_rest_base_url, path, &HashMap::new());
        let response = self
            .http
            .post(url)
            .header("ACCESS-KEY", signed.access_key)
            .header("ACCESS-NONCE", signed.access_nonce)
            .header("ACCESS-SIGNATURE", signed.access_signature)
            .header("content-type", "application/json")
            .body(body.to_string())
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
        path: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let nonce = Utc::now().timestamp_millis().to_string();
        let url = build_url(&self.private_rest_base_url, path, params);
        let parsed =
            reqwest::Url::parse(&url).map_err(|error| ExchangeApiError::InvalidRequest {
                message: format!("invalid bitbank signed GET URL: {error}"),
            })?;
        let mut path_with_query = parsed.path().to_string();
        if let Some(query) = parsed.query() {
            path_with_query.push('?');
            path_with_query.push_str(query);
        }
        let signed = signing::sign_get_request(api_key, api_secret, &nonce, &path_with_query)?;
        let response = self
            .http
            .get(url)
            .header("ACCESS-KEY", signed.access_key)
            .header("ACCESS-NONCE", signed.access_nonce)
            .header("ACCESS-SIGNATURE", signed.access_signature)
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
    if status.is_success()
        && value
            .get("success")
            .and_then(Value::as_i64)
            .map(|success| success == 1)
            .unwrap_or(true)
    {
        return Ok(value);
    }
    let mut error = ExchangeError::new(
        exchange_id,
        classify_status(status.as_u16(), &value),
        error_message(&value).unwrap_or("bitbank request failed"),
        Utc::now(),
    );
    error.code = value
        .get("data")
        .and_then(|data| data.get("code"))
        .and_then(Value::as_i64)
        .map(|code| code.to_string())
        .or_else(|| Some(status.as_u16().to_string()));
    error.raw = Some(value);
    Err(ExchangeApiError::Exchange(error))
}

fn classify_status(status: u16, value: &Value) -> ExchangeErrorClass {
    let message = error_message(value)
        .unwrap_or_default()
        .to_ascii_lowercase();
    if status == 401 || status == 403 {
        ExchangeErrorClass::Authentication
    } else if status == 429 {
        ExchangeErrorClass::RateLimited
    } else if message.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if message.contains("order") || message.contains("pair") {
        ExchangeErrorClass::OrderRejected
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn error_message(value: &Value) -> Option<&str> {
    value
        .get("data")
        .and_then(|data| data.get("message"))
        .and_then(Value::as_str)
        .or_else(|| value.get("message").and_then(Value::as_str))
}

fn build_url(base: &str, path: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|(left, _), (right, _)| left.cmp(right));
        let query = pairs
            .into_iter()
            .map(|(key, value)| format!("{}={}", url_encode(key), url_encode(value)))
            .collect::<Vec<_>>()
            .join("&");
        url.push('?');
        url.push_str(&query);
    }
    url
}

fn url_encode(value: &str) -> String {
    value
        .bytes()
        .flat_map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![byte as char]
            }
            _ => format!("%{byte:02X}").chars().collect(),
        })
        .collect()
}
