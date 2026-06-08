use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::{
    independentreserve_v1_signature, independentreserve_v2_signature,
    IndependentReservePrivateCredentials,
};

#[derive(Clone)]
pub struct IndependentReserveRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
    credentials: Option<IndependentReservePrivateCredentials>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndependentReserveEndpointVersion {
    V1,
    V2,
}

impl IndependentReserveRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
        credentials: Option<IndependentReservePrivateCredentials>,
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
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        let url = format!(
            "{}{}",
            self.rest_base_url.trim_end_matches('/'),
            build_path(endpoint, params)
        );
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

    pub async fn send_private_post(
        &self,
        version: IndependentReserveEndpointVersion,
        endpoint: &str,
        params: &[(String, String)],
    ) -> ExchangeApiResult<Value> {
        let credentials = self
            .credentials
            .as_ref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "independentreserve.private_rest",
            })?;
        let nonce = Utc::now().timestamp_millis();
        let signature = match version {
            IndependentReserveEndpointVersion::V1 => independentreserve_v1_signature(
                endpoint.trim_start_matches('/'),
                nonce,
                &credentials.api_key,
                params,
                &credentials.api_secret,
            ),
            IndependentReserveEndpointVersion::V2 => independentreserve_v2_signature(
                endpoint,
                nonce,
                &credentials.api_key,
                params,
                &credentials.api_secret,
            ),
        };
        let mut body = serde_json::Map::new();
        body.insert("apiKey".to_string(), json!(credentials.api_key));
        body.insert("nonce".to_string(), json!(nonce));
        body.insert("signature".to_string(), json!(signature));
        for (key, value) in params {
            body.insert(key.clone(), json!(value));
        }
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let response = self
            .http
            .post(url)
            .json(&Value::Object(body))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }
}

pub fn build_path(endpoint: &str, params: &[(String, String)]) -> String {
    if params.is_empty() {
        return endpoint.to_string();
    }
    let mut sorted = params.iter().collect::<Vec<_>>();
    sorted.sort_by(|left, right| left.0.cmp(&right.0));
    let query = sorted
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    format!("{endpoint}?{query}")
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
    let error_message = value
        .get("Message")
        .or_else(|| value.get("message"))
        .or_else(|| value.get("Error"))
        .or_else(|| value.get("error"))
        .and_then(Value::as_str);
    let success_flag = value
        .get("Success")
        .or_else(|| value.get("success"))
        .and_then(Value::as_bool)
        .unwrap_or(true);
    if !status.is_success() || !success_flag || error_message.is_some() {
        let message = error_message.unwrap_or("Independent Reserve request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_independentreserve_error(status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = value
            .get("Code")
            .or_else(|| value.get("code"))
            .and_then(|code| match code {
                Value::String(text) => Some(text.clone()),
                Value::Number(number) => Some(number.to_string()),
                _ => None,
            });
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_independentreserve_error(status: u16, message: &str) -> ExchangeErrorClass {
    let message = message.to_ascii_lowercase();
    match status {
        401 | 403 => ExchangeErrorClass::Authentication,
        418 | 429 => ExchangeErrorClass::RateLimited,
        500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ if message.contains("signature")
            || message.contains("api key")
            || message.contains("nonce") =>
        {
            ExchangeErrorClass::Authentication
        }
        _ if message.contains("permission") || message.contains("forbidden") => {
            ExchangeErrorClass::Permission
        }
        _ if message.contains("insufficient") || message.contains("balance") => {
            ExchangeErrorClass::InsufficientBalance
        }
        _ if message.contains("not found") && message.contains("order") => {
            ExchangeErrorClass::OrderNotFound
        }
        _ if message.contains("market") || message.contains("currency") => {
            ExchangeErrorClass::InvalidSymbol
        }
        _ if message.contains("rate") || message.contains("too many") => {
            ExchangeErrorClass::RateLimited
        }
        _ => ExchangeErrorClass::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::build_path;

    #[test]
    fn independentreserve_public_path_should_sort_query_params() {
        let path = build_path(
            "/Public/GetOrderBook",
            &[
                ("secondaryCurrencyCode".to_string(), "Aud".to_string()),
                ("primaryCurrencyCode".to_string(), "Xbt".to_string()),
            ],
        );

        assert_eq!(
            path,
            "/Public/GetOrderBook?primaryCurrencyCode=Xbt&secondaryCurrencyCode=Aud"
        );
    }
}
