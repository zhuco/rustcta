#[derive(Debug, Clone)]
pub struct BingxGatewayConfig {
    pub rest_base_url: String,
    pub spot_ws_url: String,
    pub swap_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub recv_window_ms: u64,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BingxGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://open-api.bingx.com".to_string(),
            spot_ws_url: non_empty_env("BINGX_SPOT_WS_URL")
                .unwrap_or_else(|| "wss://open-api-ws.bingx.com/market".to_string()),
            swap_ws_url: non_empty_env("BINGX_SWAP_WS_URL")
                .unwrap_or_else(|| "wss://open-api-swap.bingx.com/swap-market".to_string()),
            api_key: non_empty_env("BINGX_API_KEY"),
            api_secret: non_empty_env("BINGX_API_SECRET"),
            recv_window_ms: env_u64("BINGX_RECV_WINDOW_MS").unwrap_or(5_000),
            enabled_private_rest: env_bool("BINGX_PRIVATE_REST_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl BingxGatewayConfig {
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
