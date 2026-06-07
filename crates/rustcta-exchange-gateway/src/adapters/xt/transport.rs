use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId, MarketType};
use serde_json::Value;

use super::signing::{futures_signature_payload, sign_payload, spot_signature_payload};

#[derive(Clone)]
pub struct XtRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    futures_rest_base_url: String,
    http: reqwest::Client,
}

impl XtRest {
    pub fn new(
        exchange_id: ExchangeId,
        spot_rest_base_url: String,
        futures_rest_base_url: String,
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
            http,
        })
    }

    pub async fn send_public_request(
        &self,
        market_type: MarketType,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .get(build_url(self.base_url(market_type), endpoint, params))
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
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::GET,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::POST,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_signed_delete(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::DELETE,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_signed_put(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        self.send_signed_request(
            reqwest::Method::PUT,
            endpoint,
            params,
            api_key,
            api_secret,
            recv_window_ms,
        )
        .await
    }

    pub async fn send_signed_json(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        let market_type = endpoint_market_type(endpoint);
        let timestamp_ms = Utc::now().timestamp_millis();
        let body =
            serde_json::to_string(body).map_err(|error| ExchangeApiError::InvalidRequest {
                message: format!("failed to serialize XT JSON request body: {error}"),
            })?;
        let signature_payload = match market_type {
            MarketType::Spot => spot_signature_payload(
                api_key,
                timestamp_ms,
                recv_window_ms,
                method.as_str(),
                endpoint,
                "",
                &body,
            ),
            MarketType::Perpetual => {
                futures_signature_payload(api_key, timestamp_ms, endpoint, "", &body)
            }
            _ => unreachable!("XT signed REST only supports spot and USDT-M futures"),
        };
        let signature = sign_payload(api_secret, &signature_payload)?;
        let response = self
            .http
            .request(
                method,
                build_url(self.base_url(market_type), endpoint, &HashMap::new()),
            )
            .header("validate-appkey", api_key)
            .header("validate-timestamp", timestamp_ms.to_string())
            .header("validate-signature", signature)
            .header("Content-Type", "application/json")
            .headers(spot_headers(market_type, recv_window_ms))
            .body(body)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    async fn send_signed_request(
        &self,
        method: reqwest::Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
        recv_window_ms: u64,
    ) -> ExchangeApiResult<Value> {
        let market_type = endpoint_market_type(endpoint);
        let timestamp_ms = Utc::now().timestamp_millis();
        let query = if method == reqwest::Method::GET {
            build_query_string(params)
        } else {
            String::new()
        };
        let body = if method == reqwest::Method::GET || params.is_empty() {
            String::new()
        } else {
            body_string(market_type, params)?
        };
        let signature_payload = match market_type {
            MarketType::Spot => spot_signature_payload(
                api_key,
                timestamp_ms,
                recv_window_ms,
                method.as_str(),
                endpoint,
                &query,
                &body,
            ),
            MarketType::Perpetual => {
                futures_signature_payload(api_key, timestamp_ms, endpoint, &query, &body)
            }
            _ => unreachable!("XT signed REST only supports spot and USDT-M futures"),
        };
        let signature = sign_payload(api_secret, &signature_payload)?;
        let url = build_url(
            self.base_url(market_type),
            endpoint,
            params_for_url(&method, params),
        );
        let mut request = self
            .http
            .request(method.clone(), url)
            .header("validate-appkey", api_key)
            .header("validate-timestamp", timestamp_ms.to_string())
            .header("validate-signature", signature);
        if market_type == MarketType::Spot {
            request = request
                .header("validate-algorithms", "HmacSHA256")
                .header("validate-recvwindow", recv_window_ms.to_string());
        }
        if !body.is_empty() {
            request = request
                .header("Content-Type", content_type(market_type))
                .body(body);
        } else {
            request = request.header("Content-Type", content_type(market_type));
        }
        let response = request
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(self.exchange_id.clone(), response).await
    }

    fn base_url(&self, market_type: MarketType) -> &str {
        match market_type {
            MarketType::Spot => &self.spot_rest_base_url,
            MarketType::Perpetual => &self.futures_rest_base_url,
            _ => &self.spot_rest_base_url,
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
    let code = response_code(&value);
    if !status.is_success() || code.as_deref().is_some_and(|code| code != "0") {
        let message = value
            .get("mc")
            .or_else(|| value.get("msgInfo"))
            .or_else(|| value.pointer("/error/msg"))
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("XT request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_xt_error(code.as_deref(), status.as_u16(), message),
            message,
            Utc::now(),
        );
        error.code = code;
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_xt_error(code: Option<&str>, status: u16, message: &str) -> ExchangeErrorClass {
    let code = code.unwrap_or_default().to_ascii_lowercase();
    let msg = message.to_ascii_lowercase();
    match status {
        401 | 403 => return ExchangeErrorClass::Authentication,
        429 => return ExchangeErrorClass::RateLimited,
        500..=599 => return ExchangeErrorClass::ExchangeUnavailable,
        _ => {}
    }
    if code.starts_with("auth_") || msg.contains("signature") || msg.contains("api key") {
        ExchangeErrorClass::Authentication
    } else if code == "invalid_symbol" || code.starts_with("symbol_") || msg.contains("symbol") {
        ExchangeErrorClass::InvalidSymbol
    } else if code.contains("insufficient") || msg.contains("insufficient") {
        ExchangeErrorClass::InsufficientBalance
    } else if code.contains("not_found")
        || code == "null"
        || msg.contains("order does not exist")
        || (msg.contains("order") && msg.contains("not found"))
    {
        ExchangeErrorClass::OrderNotFound
    } else if code.contains("quantity_scale")
        || code.contains("precision")
        || msg.contains("precision")
        || msg.contains("scale")
    {
        ExchangeErrorClass::InvalidPrecision
    } else if code == "invalid_params"
        || code.starts_with("order_")
        || code.starts_with("common_")
        || msg.contains("invalid")
    {
        ExchangeErrorClass::InvalidRequest
    } else if msg.contains("rate") || msg.contains("too many") {
        ExchangeErrorClass::RateLimited
    } else {
        ExchangeErrorClass::Unknown
    }
}

fn response_code(value: &Value) -> Option<String> {
    value
        .get("rc")
        .or_else(|| value.get("returnCode"))
        .or_else(|| value.pointer("/error/code"))
        .and_then(value_as_code)
}

fn value_as_code(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn endpoint_market_type(endpoint: &str) -> MarketType {
    if endpoint.starts_with("/future/") {
        MarketType::Perpetual
    } else {
        MarketType::Spot
    }
}

fn body_string(
    market_type: MarketType,
    params: &HashMap<String, String>,
) -> ExchangeApiResult<String> {
    match market_type {
        MarketType::Spot => {
            serde_json::to_string(params).map_err(|error| ExchangeApiError::InvalidRequest {
                message: format!("failed to serialize XT spot request body: {error}"),
            })
        }
        MarketType::Perpetual => Ok(build_query_string(params)),
        _ => unreachable!("checked by caller"),
    }
}

fn content_type(market_type: MarketType) -> &'static str {
    match market_type {
        MarketType::Spot => "application/json",
        MarketType::Perpetual => "application/x-www-form-urlencoded",
        _ => "application/json",
    }
}

fn spot_headers(market_type: MarketType, recv_window_ms: u64) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    if market_type == MarketType::Spot {
        headers.insert(
            "validate-algorithms",
            reqwest::header::HeaderValue::from_static("HmacSHA256"),
        );
        if let Ok(value) = reqwest::header::HeaderValue::from_str(&recv_window_ms.to_string()) {
            headers.insert("validate-recvwindow", value);
        }
    }
    headers
}

fn params_for_url<'a>(
    method: &reqwest::Method,
    params: &'a HashMap<String, String>,
) -> &'a HashMap<String, String> {
    if method == reqwest::Method::GET {
        params
    } else {
        static EMPTY: std::sync::OnceLock<HashMap<String, String>> = std::sync::OnceLock::new();
        EMPTY.get_or_init(HashMap::new)
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

#[cfg(test)]
mod tests {
    use super::{build_query_string, classify_xt_error};
    use rustcta_types::ExchangeErrorClass;
    use std::collections::HashMap;

    #[test]
    fn xt_query_string_should_sort_keys() {
        let mut params = HashMap::new();
        params.insert("symbol".to_string(), "btc_usdt".to_string());
        params.insert("limit".to_string(), "10".to_string());
        assert_eq!(build_query_string(&params), "limit=10&symbol=btc_usdt");
    }

    #[test]
    fn xt_error_classifier_should_map_auth_rate_limit_and_order_failures() {
        assert_eq!(
            classify_xt_error(
                Some("AUTH_SIGNATURE_ERROR"),
                200,
                "signature verification failed"
            ),
            ExchangeErrorClass::Authentication
        );
        assert_eq!(
            classify_xt_error(Some("429"), 429, "too many requests"),
            ExchangeErrorClass::RateLimited
        );
        assert_eq!(
            classify_xt_error(Some("symbol_invalid"), 200, "symbol is invalid"),
            ExchangeErrorClass::InvalidSymbol
        );
        assert_eq!(
            classify_xt_error(Some("ORDER_NOT_FOUND"), 200, "order does not exist"),
            ExchangeErrorClass::OrderNotFound
        );
        assert_eq!(
            classify_xt_error(Some("quantity_scale"), 200, "precision over max scale"),
            ExchangeErrorClass::InvalidPrecision
        );
    }
}
