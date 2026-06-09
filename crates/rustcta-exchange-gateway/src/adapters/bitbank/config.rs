const DEFAULT_PUBLIC_REST_BASE_URL: &str = "https://public.bitbank.cc";
const DEFAULT_PRIVATE_REST_BASE_URL: &str = "https://api.bitbank.cc";
const DEFAULT_PUBLIC_WS_URL: &str = "wss://stream.bitbank.cc/socket.io/?EIO=3&transport=websocket";
const DEFAULT_PRIVATE_WS_URL: &str = "wss://stream.bitbank.cc/socket.io/?EIO=3&transport=websocket";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct BitbankGatewayConfig {
    pub public_rest_base_url: String,
    pub private_rest_base_url: String,
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

impl Default for BitbankGatewayConfig {
    fn default() -> Self {
        let api_key = std::env::var("BITBANK_API_KEY").ok();
        let api_secret = std::env::var("BITBANK_API_SECRET").ok();
        let has_credentials = api_key
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
            && api_secret
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty());
        Self {
            public_rest_base_url: DEFAULT_PUBLIC_REST_BASE_URL.to_string(),
            private_rest_base_url: DEFAULT_PRIVATE_REST_BASE_URL.to_string(),
            public_websocket_url: DEFAULT_PUBLIC_WS_URL.to_string(),
            private_websocket_url: DEFAULT_PRIVATE_WS_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest: std::env::var("BITBANK_PRIVATE_REST_ENABLED")
                .ok()
                .and_then(|value| value.parse::<bool>().ok())
                .unwrap_or(has_credentials),
            enabled_public_streams: true,
            enabled_private_streams: false,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl BitbankGatewayConfig {
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
