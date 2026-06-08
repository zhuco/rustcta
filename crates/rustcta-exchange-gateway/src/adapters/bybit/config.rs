#[derive(Debug, Clone)]
pub struct BybitGatewayConfig {
    pub exchange_id: String,
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub recv_window_ms: u64,
    pub request_timeout_ms: u64,
    pub enabled_private_rest: bool,
    pub enabled: bool,
    pub status_message: String,
    pub unsupported_market_type_operation: &'static str,
}

impl Default for BybitGatewayConfig {
    fn default() -> Self {
        Self {
            exchange_id: "bybit".to_string(),
            rest_base_url: "https://api.bybit.com".to_string(),
            public_ws_url: "wss://stream.bybit.com/v5/public/linear".to_string(),
            private_ws_url: "wss://stream.bybit.com/v5/private".to_string(),
            api_key: non_empty_env("RUSTCTA_BYBIT_API_KEY")
                .or_else(|| non_empty_env("BYBIT_API_KEY")),
            api_secret: non_empty_env("RUSTCTA_BYBIT_API_SECRET")
                .or_else(|| non_empty_env("BYBIT_API_SECRET")),
            recv_window_ms: env_u64("RUSTCTA_BYBIT_RECV_WINDOW_MS")
                .or_else(|| env_u64("BYBIT_RECV_WINDOW_MS"))
                .unwrap_or(5_000),
            request_timeout_ms: 10_000,
            enabled_private_rest: env_bool("RUSTCTA_BYBIT_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("BYBIT_PRIVATE_REST_ENABLED"))
                .unwrap_or(true),
            enabled: true,
            status_message: "bybit V5 spot + linear perpetual gateway adapter".to_string(),
            unsupported_market_type_operation: "bybit.unsupported_market_type",
        }
    }
}

impl BybitGatewayConfig {
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
