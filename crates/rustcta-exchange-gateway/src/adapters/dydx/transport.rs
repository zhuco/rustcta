use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::{ExchangeError, ExchangeErrorClass, ExchangeId};
use serde_json::Value;

#[derive(Clone)]
pub struct DydxIndexerRest {
    exchange_id: ExchangeId,
    rest_base_url: String,
    http: reqwest::Client,
}

impl DydxIndexerRest {
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

    pub async fn get(
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
}

fn build_url(base: &str, endpoint: &str, params: &HashMap<String, String>) -> String {
    let base = base.trim_end_matches('/');
    let endpoint = if base.ends_with("/v4") && endpoint.starts_with("/v4/") {
        endpoint.trim_start_matches("/v4")
    } else {
        endpoint
    };
    let mut url = format!("{}{}", base, endpoint);
    if !params.is_empty() {
        let mut pairs = params.iter().collect::<Vec<_>>();
        pairs.sort_by(|left, right| left.0.cmp(right.0));
        let query = pairs
            .into_iter()
            .map(|(key, value)| {
                format!(
                    "{}={}",
                    urlencoding::encode(key),
                    urlencoding::encode(value)
                )
            })
            .collect::<Vec<_>>()
            .join("&");
        url.push('?');
        url.push_str(&query);
    }
    url
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
            match status.as_u16() {
                401 | 403 => ExchangeErrorClass::Authentication,
                429 => ExchangeErrorClass::RateLimited,
                500..=599 => ExchangeErrorClass::ExchangeUnavailable,
                _ => ExchangeErrorClass::InvalidRequest,
            },
            text,
            Utc::now(),
        );
        error.code = Some(status.as_u16().to_string());
        return Err(ExchangeApiError::Exchange(error));
    }
    serde_json::from_str(&text).map_err(|error| {
        ExchangeApiError::Exchange(ExchangeError::new(
            exchange_id,
            ExchangeErrorClass::Decode,
            error.to_string(),
            Utc::now(),
        ))
    })
}
