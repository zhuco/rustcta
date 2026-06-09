use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

use super::signing::{sign_gateio_request, signed_request_path};

#[derive(Clone)]
pub struct GateIoPublicRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl GateIoPublicRest {
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

    pub async fn send_public_request(
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
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
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
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            Some(body),
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_signed_patch(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: &Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::PATCH,
            endpoint,
            params,
            Some(body),
            api_key,
            api_secret,
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            params,
            None,
            api_key,
            api_secret,
        )
        .await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let query = build_query(params);
        let url = build_url_from_query(&self.rest_base_url, endpoint, &query);
        let timestamp = Utc::now().timestamp().to_string();
        let path = signed_request_path(&self.rest_base_url, endpoint);
        let body_text = body
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| ExchangeApiError::Serialization {
                message: error.to_string(),
            })?
            .unwrap_or_default();
        let signature = sign_gateio_request(
            api_secret,
            method.as_str(),
            &path,
            &query,
            &body_text,
            &timestamp,
        );
        let mut request = self
            .http
            .request(method, url)
            .header("KEY", api_key)
            .header("Timestamp", timestamp)
            .header("SIGN", signature)
            .header("Content-Type", "application/json");
        if endpoint.starts_with("/futures/") {
            request = request.header("X-Gate-Size-Decimal", "1");
        }
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
    let body = response
        .text()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    let value = parse_gateio_response_body(status, &body)?;
    if !status.is_success() {
        let label = gateio_error_label(&value);
        let message = gateio_error_message(status, label, &value, &body);
        let mut error = ExchangeError::new(
            exchange_id,
            classify_gateio_error(label, &message),
            message.clone(),
            Utc::now(),
        );
        error.code = label.map(ToOwned::to_owned);
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn parse_gateio_response_body(status: reqwest::StatusCode, body: &str) -> ExchangeApiResult<Value> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_str(trimmed).map_err(|error| ExchangeApiError::Transport {
        message: format!(
            "failed to decode Gate.io response HTTP {}: {error}; body={}",
            status.as_u16(),
            truncate_for_error(trimmed)
        ),
    })
}

fn gateio_error_label(value: &Value) -> Option<&str> {
    [
        value.get("label"),
        value.get("code"),
        value.get("error"),
        value.pointer("/error/label"),
        value.pointer("/error/code"),
    ]
    .into_iter()
    .flatten()
    .filter_map(Value::as_str)
    .find(|value| !value.trim().is_empty())
}

fn gateio_error_message(
    status: reqwest::StatusCode,
    label: Option<&str>,
    value: &Value,
    body: &str,
) -> String {
    let exchange_message = value
        .get("message")
        .or_else(|| value.get("msg"))
        .or_else(|| value.get("detail"))
        .or_else(|| value.pointer("/error/message"))
        .or_else(|| value.pointer("/error/msg"))
        .and_then(Value::as_str)
        .filter(|message| !message.trim().is_empty());
    let mut parts = vec![format!("Gate.io request failed: HTTP {}", status.as_u16())];
    if let Some(label) = label {
        parts.push(format!("label={label}"));
    }
    if let Some(message) = exchange_message {
        parts.push(format!("message={message}"));
    }
    let body = truncate_for_error(body.trim());
    if !body.is_empty() {
        parts.push(format!("body={body}"));
    }
    parts.join("; ")
}

fn truncate_for_error(value: &str) -> String {
    const MAX_CHARS: usize = 512;
    let mut truncated = value.chars().take(MAX_CHARS).collect::<String>();
    if value.chars().count() > MAX_CHARS {
        truncated.push_str("...");
    }
    truncated
}

fn classify_gateio_error(label: Option<&str>, message: &str) -> ExchangeErrorClass {
    let label = label.unwrap_or_default().to_ascii_uppercase();
    let msg = message.to_ascii_lowercase();
    if label.contains("BALANCE") || msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if label.contains("POSITION_NOT_FOUND") || msg.contains("position not found") {
        ExchangeErrorClass::InsufficientPosition
    } else if label.contains("INVALID_CURRENCY")
        || msg.contains("currency_pair")
        || msg.contains("symbol")
    {
        ExchangeErrorClass::InvalidSymbol
    } else if label.contains("INVALID_PARAM") || msg.contains("precision") || msg.contains("size") {
        ExchangeErrorClass::InvalidPrecision
    } else if label.contains("RATE_LIMIT") || label.contains("TOO_FAST") || msg.contains("too many")
    {
        ExchangeErrorClass::RateLimited
    } else if label.contains("AUTH") || label.contains("INVALID_SIGNATURE") {
        ExchangeErrorClass::Authentication
    } else if label.contains("ORDER_NOT_FOUND") {
        ExchangeErrorClass::OrderNotFound
    } else {
        ExchangeErrorClass::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn gateio_error_message_should_include_status_label_and_body() {
        let body = r#"{"label":"USER_NOT_FOUND"}"#;
        let value = json!({"label": "USER_NOT_FOUND"});
        let label = gateio_error_label(&value);

        let message = gateio_error_message(reqwest::StatusCode::BAD_REQUEST, label, &value, body);

        assert!(message.contains("HTTP 400"));
        assert!(message.contains("label=USER_NOT_FOUND"));
        assert!(message.contains(body));
    }

    #[test]
    fn gateio_error_label_should_read_nested_error_code() {
        let value = json!({"error": {"code": "INVALID_SIGNATURE"}});

        assert_eq!(gateio_error_label(&value), Some("INVALID_SIGNATURE"));
        assert_eq!(
            classify_gateio_error(gateio_error_label(&value), ""),
            ExchangeErrorClass::Authentication
        );
    }

    #[test]
    fn gateio_error_classifier_should_treat_missing_position_as_empty_position() {
        assert_eq!(
            classify_gateio_error(Some("POSITION_NOT_FOUND"), ""),
            ExchangeErrorClass::InsufficientPosition
        );
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    build_url_from_query(base, endpoint, &build_query(params))
}

fn build_url_from_query(base: &str, endpoint: &str, query: &str) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !query.is_empty() {
        url.push('?');
        url.push_str(query);
    }
    url
}

fn build_query(params: &HashMap<String, String>) -> String {
    let mut pairs = params.iter().collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(right.0));
    pairs
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}
