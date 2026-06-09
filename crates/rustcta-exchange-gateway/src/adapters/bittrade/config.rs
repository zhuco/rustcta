const DEFAULT_REST_BASE_URL: &str = "https://api-cloud.bittrade.co.jp";
const DEFAULT_PUBLIC_WS_URL: &str = "wss://api-cloud.bittrade.co.jp/ws";
const DEFAULT_PRIVATE_WS_URL: &str = "wss://api-cloud.bittrade.co.jp/ws/v2";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct BittradeGatewayConfig {
    pub rest_base_url: String,
    pub public_websocket_url: String,
    pub private_websocket_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BittradeGatewayConfig {
    fn default() -> Self {
        let api_key = non_empty_env("BITTRADE_API_KEY");
        let api_secret = non_empty_env("BITTRADE_API_SECRET");
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            public_websocket_url: DEFAULT_PUBLIC_WS_URL.to_string(),
            private_websocket_url: DEFAULT_PRIVATE_WS_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest: env_bool("BITTRADE_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: true,
            enabled_private_streams: false,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl BittradeGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(format!("RUSTCTA_{key}"))
        .or_else(|_| std::env::var(key))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(key: &str) -> Option<bool> {
    match non_empty_env(key)?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
