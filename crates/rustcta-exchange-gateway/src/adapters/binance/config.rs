#[derive(Debug, Clone)]
pub struct BinanceGatewayConfig {
    pub rest_base_url: String,
    pub futures_rest_base_url: String,
    pub spot_public_ws_url: String,
    pub futures_public_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub recv_window_ms: u64,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BinanceGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.binance.com".to_string(),
            futures_rest_base_url: "https://fapi.binance.com".to_string(),
            spot_public_ws_url: non_empty_env("BINANCE_SPOT_PUBLIC_WS_URL")
                .unwrap_or_else(|| "wss://stream.binance.com:9443/ws".to_string()),
            futures_public_ws_url: non_empty_env("BINANCE_USDM_PUBLIC_WS_URL")
                .unwrap_or_else(|| "wss://fstream.binance.com/ws".to_string()),
            api_key: non_empty_env("BINANCE_SPOT_API_KEY")
                .or_else(|| non_empty_env("BINANCE_USDM_API_KEY"))
                .or_else(|| non_empty_env("BINANCE_API_KEY")),
            api_secret: non_empty_env("BINANCE_SPOT_API_SECRET")
                .or_else(|| non_empty_env("BINANCE_USDM_API_SECRET"))
                .or_else(|| non_empty_env("BINANCE_API_SECRET")),
            recv_window_ms: env_u64("BINANCE_SPOT_RECV_WINDOW_MS").unwrap_or(5_000),
            enabled_private_rest: env_bool("BINANCE_SPOT_PRIVATE_REST_ENABLED").unwrap_or(true),
            enabled_public_streams: env_bool("BINANCE_PUBLIC_STREAMS_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl BinanceGatewayConfig {
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
