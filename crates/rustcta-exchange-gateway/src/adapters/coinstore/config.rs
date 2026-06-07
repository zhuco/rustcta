use std::env;

use serde::{Deserialize, Serialize};

pub const COINSTORE_SPOT_REST_BASE_URL: &str = "https://api.coinstore.com/api";
pub const COINSTORE_FUTURES_REST_BASE_URL: &str = "https://futures.coinstore.com/api";
pub const COINSTORE_SPOT_PUBLIC_WS_URL: &str = "wss://ws.coinstore.com/s/ws";
pub const COINSTORE_FUTURES_WS_URL: &str =
    "wss://ws-futures.coinstore.com/socket.io/?EIO=3&transport=websocket";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CoinstoreGatewayConfig {
    #[serde(default = "default_spot_rest_base_url")]
    pub spot_rest_base_url: String,
    #[serde(default = "default_futures_rest_base_url")]
    pub futures_rest_base_url: String,
    #[serde(default = "default_spot_public_ws_url")]
    pub spot_public_ws_url: String,
    #[serde(default = "default_futures_ws_url")]
    pub futures_public_ws_url: String,
    #[serde(default = "default_futures_ws_url")]
    pub futures_private_ws_url: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_secret: Option<String>,
    #[serde(default)]
    pub enabled_private_rest: bool,
    #[serde(default = "default_request_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

impl Default for CoinstoreGatewayConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl CoinstoreGatewayConfig {
    pub fn from_env() -> Self {
        Self {
            spot_rest_base_url: env::var("RUSTCTA_COINSTORE_SPOT_REST_BASE_URL")
                .or_else(|_| env::var("RUSTCTA_COINSTORE_REST_BASE_URL"))
                .unwrap_or_else(|_| default_spot_rest_base_url()),
            futures_rest_base_url: env::var("RUSTCTA_COINSTORE_FUTURES_REST_BASE_URL")
                .unwrap_or_else(|_| default_futures_rest_base_url()),
            spot_public_ws_url: env::var("RUSTCTA_COINSTORE_SPOT_PUBLIC_WS_URL")
                .unwrap_or_else(|_| default_spot_public_ws_url()),
            futures_public_ws_url: env::var("RUSTCTA_COINSTORE_FUTURES_PUBLIC_WS_URL")
                .or_else(|_| env::var("RUSTCTA_COINSTORE_FUTURES_WS_URL"))
                .unwrap_or_else(|_| default_futures_ws_url()),
            futures_private_ws_url: env::var("RUSTCTA_COINSTORE_FUTURES_PRIVATE_WS_URL")
                .or_else(|_| env::var("RUSTCTA_COINSTORE_FUTURES_WS_URL"))
                .unwrap_or_else(|_| default_futures_ws_url()),
            api_key: env::var("COINSTORE_API_KEY").ok(),
            api_secret: env::var("COINSTORE_API_SECRET").ok(),
            enabled_private_rest: env::var("COINSTORE_PRIVATE_REST_ENABLED")
                .ok()
                .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
                .unwrap_or(false),
            request_timeout_ms: env::var("RUSTCTA_COINSTORE_REQUEST_TIMEOUT_MS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(default_request_timeout_ms),
            enabled: true,
        }
    }

    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

fn default_spot_rest_base_url() -> String {
    COINSTORE_SPOT_REST_BASE_URL.to_string()
}

fn default_futures_rest_base_url() -> String {
    COINSTORE_FUTURES_REST_BASE_URL.to_string()
}

fn default_spot_public_ws_url() -> String {
    COINSTORE_SPOT_PUBLIC_WS_URL.to_string()
}

fn default_futures_ws_url() -> String {
    COINSTORE_FUTURES_WS_URL.to_string()
}

fn default_request_timeout_ms() -> u64 {
    10_000
}

fn default_enabled() -> bool {
    true
}
