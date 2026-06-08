#![cfg_attr(not(test), allow(dead_code))]

use std::time::Duration;

use reqwest::Client;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

use super::config::PacificaGatewayConfig;
use super::signing::{pacifica_signing_payload, sign_pacifica_payload};

#[derive(Clone)]
pub struct PacificaTransport {
    client: Client,
    config: PacificaGatewayConfig,
}

impl PacificaTransport {
    pub fn new(config: PacificaGatewayConfig) -> ExchangeApiResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|error| ExchangeApiError::InvalidRequest {
                message: format!("failed to build Pacifica HTTP client: {error}"),
            })?;
        Ok(Self { client, config })
    }

    pub async fn send_public_get(&self, path_and_query: &str) -> ExchangeApiResult<String> {
        let url = format!(
            "{}{}",
            self.config.active_rest_base_url().trim_end_matches('/'),
            path_and_query
        );
        let response = self
            .client
            .get(url)
            .header("Accept", "*/*")
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: format!("Pacifica public REST request failed: {error}"),
            })?;
        response
            .text()
            .await
            .map_err(|error| ExchangeApiError::Serialization {
                message: format!("invalid Pacifica text response: {error}"),
            })
    }

    pub fn signed_body(
        &self,
        operation_type: &str,
        timestamp_ms: i64,
        expiry_window_ms: u64,
        operation_data: Value,
    ) -> ExchangeApiResult<Value> {
        let account = self
            .config
            .account
            .clone()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "pacifica.account_missing",
            })?;
        let secret =
            self.config
                .agent_private_key
                .as_deref()
                .ok_or(ExchangeApiError::Unsupported {
                    operation: "pacifica.agent_private_key_missing",
                })?;
        let payload = pacifica_signing_payload(
            operation_type,
            timestamp_ms,
            expiry_window_ms,
            operation_data.clone(),
        )?;
        let signature = sign_pacifica_payload(secret, &payload)?;
        let mut body =
            operation_data
                .as_object()
                .cloned()
                .ok_or_else(|| ExchangeApiError::Serialization {
                    message: "Pacifica signed operation data must be a JSON object".to_string(),
                })?;
        body.insert("account".to_string(), Value::String(account));
        body.insert("signature".to_string(), Value::String(signature));
        body.insert("timestamp".to_string(), Value::Number(timestamp_ms.into()));
        body.insert(
            "expiry_window".to_string(),
            Value::Number(expiry_window_ms.into()),
        );
        if let Some(agent_wallet) = self.config.agent_wallet.as_ref() {
            body.insert(
                "agent_wallet".to_string(),
                Value::String(agent_wallet.clone()),
            );
        }
        Ok(Value::Object(body))
    }
}
