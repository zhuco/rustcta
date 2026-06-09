#![cfg_attr(not(test), allow(dead_code))]

use std::time::Duration;

use rustcta_exchange_api::{ExchangeApiError, ExchangeApiResult};
use rustcta_types::ExchangeId;
use serde_json::{json, Value};

use super::signing::GrvtSessionAuthHeaders;

pub const GRVT_MAINNET_MARKET_DATA_REST: &str = "https://market-data.grvt.io/full";
pub const GRVT_MAINNET_TRADING_REST: &str = "https://trades.grvt.io/full";
pub const GRVT_MAINNET_AUTH: &str = "https://edge.grvt.io/auth";
pub const GRVT_TESTNET_MARKET_DATA_REST: &str = "https://market-data.testnet.grvt.io/full";
pub const GRVT_TESTNET_TRADING_REST: &str = "https://trades.testnet.grvt.io/full";
pub const GRVT_TESTNET_AUTH: &str = "https://edge.testnet.grvt.io/auth";
pub const GRVT_PUBLIC_WS_PING_INTERVAL_MS: i64 = 30_000;
pub const GRVT_PUBLIC_WS_PONG_TIMEOUT_MS: i64 = 45_000;
pub const GRVT_PUBLIC_WS_STALE_MESSAGE_MS: i64 = 60_000;

pub fn grvt_reconnect_policy_ms() -> (i64, i64, i64) {
    (
        GRVT_PUBLIC_WS_PING_INTERVAL_MS,
        GRVT_PUBLIC_WS_PONG_TIMEOUT_MS,
        GRVT_PUBLIC_WS_STALE_MESSAGE_MS,
    )
}

#[derive(Clone)]
pub struct GrvtRest {
    exchange_id: ExchangeId,
    trading_rest_base_url: String,
    http: reqwest::Client,
}

impl GrvtRest {
    pub fn new(
        exchange_id: ExchangeId,
        trading_rest_base_url: String,
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
            trading_rest_base_url,
            http,
        })
    }

    pub async fn send_session_post(
        &self,
        path: &str,
        auth: &GrvtSessionAuthHeaders,
        body: Value,
    ) -> ExchangeApiResult<Value> {
        let response = self
            .http
            .post(join_url(&self.trading_rest_base_url, path))
            .header("Accept", "application/json")
            .header("Content-Type", "application/json")
            .header("Cookie", auth.cookie.as_str())
            .header("X-Grvt-Account-Id", auth.account_id.as_str())
            .json(&body)
            .send()
            .await
            .map_err(|error| ExchangeApiError::Transport {
                message: error.to_string(),
            })?;
        parse_response(&self.exchange_id, response).await
    }
}

pub fn grvt_session_post_request_spec(path: &str, body: Value) -> Value {
    json!({
        "method": "POST",
        "path": path,
        "auth": "session_cookie",
        "headers": {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Cookie": "<redacted-session-cookie>",
            "X-Grvt-Account-Id": "<redacted-account-id>"
        },
        "body": body
    })
}

fn join_url(base: &str, path: &str) -> String {
    format!(
        "{}{}",
        base.trim_end_matches('/'),
        if path.starts_with('/') {
            path.to_string()
        } else {
            format!("/{path}")
        }
    )
}

async fn parse_response(
    exchange_id: &ExchangeId,
    response: reqwest::Response,
) -> ExchangeApiResult<Value> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?;
    let value = if text.trim().is_empty() {
        Value::Object(serde_json::Map::new())
    } else {
        serde_json::from_str::<Value>(&text).map_err(|error| ExchangeApiError::Transport {
            message: error.to_string(),
        })?
    };
    if !status.is_success() {
        return Err(ExchangeApiError::Transport {
            message: format!("{exchange_id} REST request failed with HTTP {status}: {value}"),
        });
    }
    Ok(value)
}
