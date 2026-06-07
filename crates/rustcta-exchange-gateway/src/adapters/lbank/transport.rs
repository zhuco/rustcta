use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::{json, Value};

use super::signing::{sign_lbank_params, DEFAULT_ECHOSTR, SIGNATURE_METHOD};

#[derive(Clone)]
pub struct LBankRest {
    exchange_id: ExchangeId,
    spot_rest_base_url: String,
    contract_rest_base_url: String,
    http: reqwest::Client,
}

impl LBankRest {
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

    pub async fn send_spot_public_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.spot_rest_base_url, endpoint, params);
        let response =
            self.http
                .get(url)
                .send()
                .await
                .map_err(|error| ExchangeApiError::Transport {
                    message: error.to_string(),
                })?;
        parse_lbank_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_contract_public_get(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(&self.contract_rest_base_url, endpoint, params);
        let response =
            self.http
                .get(url)
                .send()
                .await
                .map_err(|error| ExchangeApiError::Transport {
                    message: error.to_string(),
                })?;
        parse_lbank_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_spot_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let mut form = params.clone();
        form.insert("api_key".to_string(), api_key.to_string());
        form.insert("timestamp".to_string(), timestamp.clone());
        form.insert("signature_method".to_string(), SIGNATURE_METHOD.to_string());
        form.insert("echostr".to_string(), DEFAULT_ECHOSTR.to_string());
        let signature = sign_lbank_params(api_secret, &form)?;
        form.remove("timestamp");
        form.remove("signature_method");
        form.remove("echostr");
        form.insert("sign".to_string(), signature);

        let url = format!(
            "{}{}",
            self.spot_rest_base_url.trim_end_matches('/'),
            endpoint
        );
        let response = self
            .http
            .post(url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("timestamp", timestamp)
            .header("signature_method", SIGNATURE_METHOD)
            .header("echostr", DEFAULT_ECHOSTR)
            .form(&form)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_lbank_response(self.exchange_id.clone(), response).await
    }

    pub async fn send_contract_signed_post(
        &self,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        let mut signed = params.clone();
        signed.insert("api_key".to_string(), api_key.to_string());
        signed.insert("timestamp".to_string(), timestamp.clone());
        signed.insert("signature_method".to_string(), SIGNATURE_METHOD.to_string());
        signed.insert("echostr".to_string(), DEFAULT_ECHOSTR.to_string());
        let signature = sign_lbank_params(api_secret, &signed)?;

        let mut body = serde_json::Map::new();
        for (key, value) in params {
            body.insert(key.clone(), json!(value));
        }
        body.insert("api_key".to_string(), json!(api_key));
        body.insert("sign".to_string(), json!(signature));

        let url = format!(
            "{}{}",
            self.contract_rest_base_url.trim_end_matches('/'),
            endpoint
        );
        let response = self
            .http
            .post(url)
            .header("Content-Type", "application/json")
            .header("timestamp", timestamp)
            .header("signature_method", SIGNATURE_METHOD)
            .header("echostr", DEFAULT_ECHOSTR)
            .json(&Value::Object(body))
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_lbank_response(self.exchange_id.clone(), response).await
    }
}

async fn parse_lbank_response(
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
        .get("error_code")
        .or_else(|| value.get("errorCode"))
        .and_then(value_as_i64);
    let ok_result = value.get("result").map_or(true, |result| match result {
        Value::Bool(flag) => *flag,
        Value::String(text) => text.eq_ignore_ascii_case("true") || text == "1",
        _ => true,
    });
    let ok_success = value.get("success").map_or(true, |success| match success {
        Value::Bool(flag) => *flag,
        Value::String(text) => text.eq_ignore_ascii_case("true") || text == "1",
        _ => true,
    });
    if !status.is_success() || code.is_some_and(|code| code != 0) || !ok_result || !ok_success {
        let message = value
            .get("msg")
            .or_else(|| value.get("message"))
            .and_then(Value::as_str)
            .unwrap_or("LBank request failed");
        let mut error = ExchangeError::new(
            exchange_id,
            classify_lbank_error(code, message),
            message,
            Utc::now(),
        );
        error.code = code.map(|code| code.to_string());
        error.raw = Some(value);
        return Err(ExchangeApiError::Exchange(error));
    }
    Ok(value)
}

fn classify_lbank_error(code: Option<i64>, message: &str) -> ExchangeErrorClass {
    match (code, message.to_ascii_lowercase()) {
        (Some(10004), _) | (Some(10012), _) | (Some(183), _) => ExchangeErrorClass::RateLimited,
        (Some(10005), _) | (Some(10007), _) | (Some(10203), _) | (Some(176), _) => {
            ExchangeErrorClass::Authentication
        }
        (Some(10022), _) | (Some(10067), _) | (Some(10009), _) => ExchangeErrorClass::Permission,
        (Some(10008), _) | (Some(8), _) => ExchangeErrorClass::InvalidSymbol,
        (Some(10010), _) | (Some(10013), _) | (Some(10120), _) | (Some(194), _) => {
            ExchangeErrorClass::MinNotionalViolation
        }
        (Some(10014), _) | (Some(10016), _) | (Some(35), _) | (Some(36), _) => {
            ExchangeErrorClass::InsufficientBalance
        }
        (Some(10032), _) | (Some(24), _) => ExchangeErrorClass::OrderNotFound,
        (Some(10036), _) => ExchangeErrorClass::DuplicateClientOrderId,
        (_, msg) if msg.contains("too frequent") || msg.contains("frequency") => {
            ExchangeErrorClass::RateLimited
        }
        (_, msg) if msg.contains("permission") => ExchangeErrorClass::Permission,
        (_, msg) if msg.contains("signature") || msg.contains("api key") => {
            ExchangeErrorClass::Authentication
        }
        (_, msg) if msg.contains("not exist") && msg.contains("order") => {
            ExchangeErrorClass::OrderNotFound
        }
        _ => ExchangeErrorClass::Unknown,
    }
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let mut url = format!("{}{}", base.trim_end_matches('/'), endpoint);
    if !params.is_empty() {
        url.push('?');
        url.push_str(&super::signing::build_query_string(params));
    }
    url
}

fn value_as_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
