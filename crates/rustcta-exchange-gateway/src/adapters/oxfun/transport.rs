#![cfg_attr(not(test), allow(dead_code))]

use std::time::Duration;

use reqwest::Client;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use serde_json::Value;

use super::config::OxfunGatewayConfig;
use super::signing::sign_rest_request;

#[derive(Clone)]
pub struct OxfunTransport {
    client: Client,
    config: OxfunGatewayConfig,
}

impl OxfunTransport {
    pub fn new(config: OxfunGatewayConfig) -> ExchangeApiResult<Self> {
        let client = Client::builder()
            .timeout(Duration::from_millis(config.request_timeout_ms))
            .build()
            .map_err(|error| ExchangeApiError::InvalidRequest {
                message: format!("failed to build OX.FUN HTTP client: {error}"),
            })?;
        Ok(Self { client, config })
    }

    pub fn signed_headers(
        &self,
        method: &str,
        path: &str,
        body_or_query: &str,
        timestamp: &str,
        nonce: &str,
    ) -> ExchangeApiResult<Vec<(String, String)>> {
        let api_key = self
            .config
            .api_key
            .as_deref()
            .ok_or(ExchangeApiError::Unsupported {
                operation: "oxfun.private_rest_credentials_missing",
            })?;
        let api_secret =
            self.config
                .api_secret
                .as_deref()
                .ok_or(ExchangeApiError::Unsupported {
                    operation: "oxfun.private_rest_credentials_missing",
                })?;
        let signature = sign_rest_request(
            api_secret,
            timestamp,
            nonce,
            method,
            "api.ox.fun",
            path,
            body_or_query,
        )?;
        Ok(vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("AccessKey".to_string(), api_key.to_string()),
            ("Timestamp".to_string(), timestamp.to_string()),
            ("Signature".to_string(), signature),
            ("Nonce".to_string(), nonce.to_string()),
        ])
    }

    pub async fn send_public_get(&self, path: &str) -> ExchangeApiResult<Value> {
        let url = format!(
            "{}{}",
            self.config.rest_base_url.trim_end_matches('/'),
            path
        );
        let response =
            self.client
                .get(url)
                .send()
                .await
                .map_err(|error| ExchangeApiError::Transport {
                    message: format!("OX.FUN public REST request failed: {error}"),
                })?;
        response
            .json::<Value>()
            .await
            .map_err(|error| ExchangeApiError::Serialization {
                message: format!("invalid OX.FUN JSON response: {error}"),
            })
    }
}
