const DEFAULT_REST_BASE_URL: &str = "https://api.upbit.com";
const DEFAULT_WS_PUBLIC_URL: &str = "wss://api.upbit.com/websocket/v1";
const DEFAULT_WS_PRIVATE_URL: &str = "wss://api.upbit.com/websocket/v1/private";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct UpbitGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for UpbitGatewayConfig {
    fn default() -> Self {
        let api_key = non_empty_env("UPBIT_API_KEY")
            .or_else(|| non_empty_env("UPBIT_ACCESS_KEY"))
            .or_else(|| non_empty_env("RUSTCTA_UPBIT_API_KEY"));
        let api_secret = non_empty_env("UPBIT_API_SECRET")
            .or_else(|| non_empty_env("UPBIT_SECRET_KEY"))
            .or_else(|| non_empty_env("RUSTCTA_UPBIT_API_SECRET"));
        let has_credentials = api_key.is_some() && api_secret.is_some();
        let enabled_private_rest = std::env::var("UPBIT_PRIVATE_REST_ENABLED")
            .ok()
            .or_else(|| std::env::var("RUSTCTA_UPBIT_PRIVATE_REST_ENABLED").ok())
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(has_credentials);
        let enabled_private_streams = std::env::var("UPBIT_PRIVATE_STREAMS_ENABLED")
            .ok()
            .or_else(|| std::env::var("RUSTCTA_UPBIT_PRIVATE_STREAMS_ENABLED").ok())
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(enabled_private_rest);
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            public_ws_url: DEFAULT_WS_PUBLIC_URL.to_string(),
            private_ws_url: DEFAULT_WS_PRIVATE_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest,
            enabled_private_streams,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl UpbitGatewayConfig {
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

    pub fn private_streams_enabled(&self) -> bool {
        self.enabled_private_streams && self.private_rest_enabled()
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
