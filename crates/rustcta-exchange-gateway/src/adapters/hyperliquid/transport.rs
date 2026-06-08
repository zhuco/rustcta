use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde::Serialize;
use serde_json::{json, Value};

use super::signing::sign_l1_action;

#[derive(Clone)]
pub struct HyperliquidRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl HyperliquidRest {
    pub fn new(
        exchange_id: ExchangeId,
        rest_base_url: String,
        request_timeout_ms: u64,
    ) -> ExchangeApiResult<Self> {
        let http = reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
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

    pub async fn post_info(&self, payload: &Value) -> ExchangeApiResult<Value> {
        self.post_json("/info", payload).await
    }

    pub async fn send_info(&self, payload: Value) -> ExchangeApiResult<Value> {
        self.post_info(&payload).await
    }

    #[allow(dead_code)]
    pub async fn post_signed_action<T: Serialize>(
        &self,
        action: &T,
        signing_key: &str,
        vault_address: Option<&str>,
        is_mainnet: bool,
    ) -> ExchangeApiResult<Value> {
        let nonce = Utc::now().timestamp_millis() as u64;
        let signature =
            sign_l1_action(signing_key, action, vault_address, nonce, None, is_mainnet)?;
        let mut payload = json!({
            "action": action,
            "nonce": nonce,
            "signature": signature,
        });
        if let (Some(vault_address), Some(object)) = (vault_address, payload.as_object_mut()) {
            object.insert(
                "vaultAddress".to_string(),
                Value::String(vault_address.to_ascii_lowercase()),
            );
        }
        self.post_json("/exchange", &payload).await
    }

    pub async fn send_exchange_action<T: Serialize>(
        &self,
        action: &T,
        credentials: &super::signing::HyperliquidPrivateCredentials,
        nonce: u64,
    ) -> ExchangeApiResult<Value> {
        let signature = super::signing::sign_l1_action(
            &credentials.private_key_hex,
            action,
            credentials.vault_address.as_deref(),
            nonce,
            None,
            credentials.is_mainnet,
        )?;
        let mut payload = json!({
            "action": action,
            "nonce": nonce,
            "signature": signature,
        });
        if let (Some(vault_address), Some(object)) = (
            credentials.vault_address.as_deref(),
            payload.as_object_mut(),
        ) {
            object.insert(
                "vaultAddress".to_string(),
                Value::String(vault_address.to_ascii_lowercase()),
            );
        }
        self.post_json("/exchange", &payload).await
    }

    async fn post_json(&self, endpoint: &str, payload: &Value) -> ExchangeApiResult<Value> {
        let url = format!("{}{}", self.rest_base_url.trim_end_matches('/'), endpoint);
        let response = self
            .http
            .post(url)
            .header("Content-Type", "application/json")
            .json(payload)
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
    let text = response
        .text()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    if !status.is_success() {
        let mut error = ExchangeError::new(
            exchange_id,
            classify_error(status.as_u16(), &text),
            text,
            Utc::now(),
        );
        error.code = Some(status.as_u16().to_string());
        return Err(ExchangeApiError::Exchange(error));
    }
    serde_json::from_str(&text).map_err(|error| ExchangeApiError::Serialization {
        message: error.to_string(),
    })
}

fn classify_error(status: u16, body: &str) -> ExchangeErrorClass {
    let lower = body.to_ascii_lowercase();
    match status {
        401 | 403 => ExchangeErrorClass::Authentication,
        429 => ExchangeErrorClass::RateLimited,
        418 | 500..=599 => ExchangeErrorClass::ExchangeUnavailable,
        _ if lower.contains("signature") => ExchangeErrorClass::Authentication,
        _ if lower.contains("insufficient") => ExchangeErrorClass::InsufficientBalance,
        _ => ExchangeErrorClass::InvalidRequest,
    }
}
