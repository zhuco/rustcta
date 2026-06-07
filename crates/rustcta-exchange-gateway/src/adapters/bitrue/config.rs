#[derive(Debug, Clone)]
pub struct BitrueGatewayConfig {
    pub spot_rest_base_url: String,
    pub futures_rest_base_url: String,
    pub spot_public_ws_url: String,
    pub spot_private_ws_url: String,
    pub futures_private_ws_auth_url: String,
    pub futures_private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub recv_window_ms: u64,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BitrueGatewayConfig {
    fn default() -> Self {
        Self {
            spot_rest_base_url: "https://openapi.bitrue.com".to_string(),
            futures_rest_base_url: "https://fapi.bitrue.com".to_string(),
            spot_public_ws_url: "wss://ws.bitrue.com/market/ws".to_string(),
            spot_private_ws_url: "wss://wsapi.bitrue.com/stream".to_string(),
            futures_private_ws_auth_url: "https://fapiws-auth.bitrue.com".to_string(),
            futures_private_ws_url: "wss://fapiws.bitrue.com/stream".to_string(),
            api_key: non_empty_env("BITRUE_API_KEY"),
            api_secret: non_empty_env("BITRUE_API_SECRET"),
            recv_window_ms: env_u64("BITRUE_RECV_WINDOW_MS").unwrap_or(5_000),
            enabled_private_rest: env_bool("BITRUE_PRIVATE_REST_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl BitrueGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|key| !key.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|secret| !secret.trim().is_empty())
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_u64(key: &str) -> Option<u64> {
    non_empty_env(key)?.parse().ok()
}

fn env_bool(key: &str) -> Option<bool> {
    match non_empty_env(key)?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
