#[derive(Debug, Clone)]
pub struct BybiteuGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub recv_window_ms: u64,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BybiteuGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.bybit.eu".to_string(),
            public_ws_url: "wss://stream.bybit.eu/v5/public/linear".to_string(),
            private_ws_url: "wss://stream.bybit.eu/v5/private".to_string(),
            recv_window_ms: env_u64("RUSTCTA_BYBITEU_RECV_WINDOW_MS")
                .or_else(|| env_u64("RUSTCTA_BYBIT_EU_RECV_WINDOW_MS"))
                .unwrap_or(5_000),
            request_timeout_ms: 10_000,
            enabled: true,
        }
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
