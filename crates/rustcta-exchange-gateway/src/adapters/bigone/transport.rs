use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use reqwest::Method;
use serde_json::Value;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId, SchemaVersion};

use super::signing::bigone_jwt;

#[derive(Clone)]
pub struct BigOneRest {
    exchange_id: ExchangeId,
    spot_base_url: String,
    contract_base_url: String,
    client: reqwest::Client,
}

impl BigOneRest {
    pub fn new(
        exchange_id: ExchangeId,
        spot_base_url: String,
        contract_base_url: String,
        timeout_ms: u64,
    ) -> ExchangeApiResult<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .map_err(|error| ExchangeApiError::InvalidRequest {
                message: format!("failed to build BigONE http client: {error}"),
            })?;
        Ok(Self {
            exchange_id,
            spot_base_url: trim_base_url(spot_base_url),
            contract_base_url: trim_base_url(contract_base_url),
            client,
        })
    }

    pub async fn get_public(
        &self,
        market_is_contract: bool,
        endpoint: &str,
        params: &HashMap<String, String>,
    ) -> ExchangeApiResult<Value> {
        self.send(
            market_is_contract,
            Method::GET,
            endpoint,
            params,
            None,
            None,
        )
        .await
    }

    pub async fn get_signed(
        &self,
        market_is_contract: bool,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send(
            market_is_contract,
            Method::GET,
            endpoint,
            params,
            None,
            Some((api_key, api_secret)),
        )
        .await
    }

    pub async fn post_signed(
        &self,
        market_is_contract: bool,
        endpoint: &str,
        body: &Value,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send(
            market_is_contract,
            Method::POST,
            endpoint,
            &HashMap::new(),
            Some(body),
            Some((api_key, api_secret)),
        )
        .await
    }

    pub async fn delete_signed(
        &self,
        market_is_contract: bool,
        endpoint: &str,
        params: &HashMap<String, String>,
        api_key: &str,
        api_secret: &str,
    ) -> ExchangeApiResult<Value> {
        self.send(
            market_is_contract,
            Method::DELETE,
            endpoint,
            params,
            None,
            Some((api_key, api_secret)),
        )
        .await
    }

    async fn send(
        &self,
        market_is_contract: bool,
        method: Method,
        endpoint: &str,
        params: &HashMap<String, String>,
        body: Option<&Value>,
        credentials: Option<(&str, &str)>,
    ) -> ExchangeApiResult<Value> {
        let url = build_url(
            if market_is_contract {
                &self.contract_base_url
            } else {
                &self.spot_base_url
            },
            endpoint,
        );
        let mut request = self.client.request(method, url).query(params);
        if let Some((api_key, api_secret)) = credentials {
            let token = bigone_jwt(api_key, api_secret, Utc::now().timestamp_millis())?;
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        let response = request.send().await.map_err(|error| {
            ExchangeApiError::Exchange(ExchangeError {
                schema_version: SchemaVersion::current(),
                exchange_id: self.exchange_id.clone(),
                class: if error.is_timeout() {
                    ExchangeErrorClass::Timeout
                } else {
                    ExchangeErrorClass::Network
                },
                code: None,
                message: format!("BigONE http request failed: {error}"),
                retry_after_ms: None,
                order_id: None,
                client_order_id: None,
                raw: None,
                occurred_at: Utc::now(),
            })
        })?;
        let status = response.status();
        let value = response.json::<Value>().await.map_err(|error| {
            ExchangeApiError::Exchange(ExchangeError {
                schema_version: SchemaVersion::current(),
                exchange_id: self.exchange_id.clone(),
                class: ExchangeErrorClass::Decode,
                code: None,
                message: format!("BigONE response json decode failed: {error}"),
                retry_after_ms: None,
                order_id: None,
                client_order_id: None,
                raw: None,
                occurred_at: Utc::now(),
            })
        })?;
        if !status.is_success() || value.get("error").is_some() {
            return Err(bigone_error(
                self.exchange_id.clone(),
                status.as_u16(),
                &value,
            ));
        }
        Ok(value)
    }
}

fn trim_base_url(value: String) -> String {
    value.trim_end_matches('/').to_string()
}

fn build_url(base_url: &str, endpoint: &str) -> String {
    format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        endpoint.trim_start_matches('/')
    )
}

fn bigone_error(exchange_id: ExchangeId, status: u16, value: &Value) -> ExchangeApiError {
    let message = value
        .get("error")
        .and_then(|error| error.get("message").or_else(|| error.get("code")))
        .and_then(Value::as_str)
        .or_else(|| value.get("message").and_then(Value::as_str))
        .unwrap_or("BigONE exchange error")
        .to_string();
    let class = match status {
        401 | 403 => ExchangeErrorClass::Authentication,
        404 => ExchangeErrorClass::OrderNotFound,
        429 => ExchangeErrorClass::RateLimited,
        500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ if message.to_ascii_lowercase().contains("balance") => {
            ExchangeErrorClass::InsufficientBalance
        }
        _ => ExchangeErrorClass::InvalidRequest,
    };
    ExchangeApiError::Exchange(ExchangeError {
        schema_version: SchemaVersion::current(),
        exchange_id,
        class,
        code: Some(status.to_string()),
        message,
        retry_after_ms: None,
        order_id: None,
        client_order_id: None,
        raw: Some(value.clone()),
        occurred_at: Utc::now(),
    })
}
