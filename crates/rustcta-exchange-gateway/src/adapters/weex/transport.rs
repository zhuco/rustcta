use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::{prehash, sign_prehash};

#[derive(Clone)]
pub struct WeexRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    contract_rest_base_url: String,
    http: reqwest::Client,
}

impl WeexRest {
    pub fn new(
        exchange_id: ExchangeId,
        spot_rest_base_url: String,
        contract_rest_base_url: String,
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
            contract_rest_base_url,
            http,
        })
    }

    pub async fn send_public_request(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(build_url(self.base_url(endpoint), endpoint, params))
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
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
            passphrase,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        let body = json!(params);
        self.send_signed_post_json(endpoint, &body, api_key, api_secret, passphrase)
            .await
    }

    pub async fn send_signed_post_json(
        &self,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            &HashMap::new(),
            Some(body),
            api_key,
            api_secret,
            passphrase,
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
            passphrase,
        )
        .await
    }

    pub async fn send_signed_delete_json(
        &self,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            &HashMap::new(),
            Some(body),
            api_key,
            api_secret,
            passphrase,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        query_params: &HashMap<String, String>,
        body_params: Option<&Value>,
        api_key: &str,
        api_secret: &str,
        passphrase: &str,
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let query = build_query_string(query_params);
        let body = body_params
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ExchangeApiError::InvalidRequest {
                message: format!("failed to serialize WEEX private request body: {error}"),
            })?
            .unwrap_or_default();
        let signature = sign_prehash(
            api_secret,
            &prehash(&timestamp, method.as_str(), endpoint, &query, &body),
        )?;
        let url = build_url(self.base_url(endpoint), endpoint, query_params);
        let mut request = self
            .http
            .request(method, url)
            .header("ACCESS-KEY", api_key)
            .header("ACCESS-SIGN", signature)
            .header("ACCESS-PASSPHRASE", passphrase)
            .header("ACCESS-TIMESTAMP", timestamp)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("locale", "en-US");
        if !body.is_empty() {
            request = request
                .header("Content-Type", "application/json")
                .body(body);
        }
        let response = request
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    fn base_url(&self, endpoint: &str) -> &str {
        if endpoint.starts_with("/capi/") {
            &self.contract_rest_base_url
        } else {
            &self.spot_rest_base_url
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
    let code = value.get("code").and_then(value_as_code);
    let success = value.get("success").and_then(Value::as_bool);
    if !status.is_success()
        || code
            .as_deref()
            .is_some_and(|code| code != "0" && code != "00000")
        || success == Some(false)
    {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .or_else(|| value.get("errorMessage"))
            .and_then(Value::as_str)
            .unwrap_or("WEEX request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_weex_error(code.as_deref(), status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_weex_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default();
    let msg = message.to_ascii_lowercase();
    match (status, code) {
        (401, _) | (_, "-1040" | "-1041" | "-1042" | "-1043") => ExchangeErrorClass::Authentication,
        (403, _) | (_, "-1052" | "-1053" | "-1058") => ExchangeErrorClass::Permission,
        (418 | 429, _) => ExchangeErrorClass::RateLimited,
        (500..=599, _) => ExchangeErrorClass::ExchangeUnavailable,
        (_, "-1121") => ExchangeErrorClass::InvalidSymbol,
        (_, "-1054" | "-2013" | "-3200") => ExchangeErrorClass::OrderNotFound,
        (_, "-2200") => ExchangeErrorClass::InsufficientBalance,
        (_, "-1111" | "-1115" | "-1116" | "-1130" | "-1190") => ExchangeErrorClass::InvalidRequest,
        _ if msg.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ if msg.contains("duplicate") => ExchangeErrorClass::DuplicateClientOrderId,
        _ if msg.contains("order") && (msg.contains("exist") || msg.contains("not found")) => {
            ExchangeErrorClass::OrderNotFound
        }
        _ if msg.contains("rate") || msg.contains("too many") => ExchangeErrorClass::RateLimited,
        _ if msg.contains("symbol") => ExchangeErrorClass::InvalidSymbol,
        _ if msg.contains("precision") || msg.contains("scale") => {
            ExchangeErrorClass::InvalidPrecision
        }
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
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        url.push('?');
        url.push_str(&build_query_string(params));
    }
    url
}

pub fn build_query_string(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
