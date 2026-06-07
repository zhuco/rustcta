#[derive(Debug, Clone)]
pub struct ToobitGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub recv_window_ms: u64,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub stale_book_ms: u64,
    pub spot_maker_fee_override: Option<String>,
    pub spot_taker_fee_override: Option<String>,
    pub enabled: bool,
}

impl Default for ToobitGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.toobit.com".to_string(),
            public_ws_url: "wss://stream.toobit.com/quote/ws/v1".to_string(),
            api_key: non_empty_env("TOOBIT_API_KEY"),
            api_secret: non_empty_env("TOOBIT_API_SECRET"),
            recv_window_ms: env_u64("TOOBIT_RECV_WINDOW_MS").unwrap_or(5_000),
            enabled_private_rest: env_bool("TOOBIT_PRIVATE_REST_ENABLED").unwrap_or(true),
            request_timeout_ms: env_u64("TOOBIT_REQUEST_TIMEOUT_MS").unwrap_or(10_000),
            stale_book_ms: env_u64("TOOBIT_STALE_BOOK_MS").unwrap_or(10_000),
            spot_maker_fee_override: non_empty_env("TOOBIT_SPOT_MAKER_FEE_RATE"),
            spot_taker_fee_override: non_empty_env("TOOBIT_SPOT_TAKER_FEE_RATE"),
            enabled: true,
        }
    }
}

impl ToobitGatewayConfig {
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
