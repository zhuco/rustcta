const DEFAULT_REST_BASE_URL: &str = "https://api.crypto.com/exchange/v1";
const DEFAULT_PUBLIC_WS_URL: &str = "wss://stream.crypto.com/exchange/v1/market";
const DEFAULT_PRIVATE_WS_URL: &str = "wss://stream.crypto.com/exchange/v1/user";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct CryptoComGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CryptoComGatewayConfig {
    fn default() -> Self {
        let api_key = std::env::var("CRYPTOCOM_API_KEY").unwrap_or_default();
        let api_secret = std::env::var("CRYPTOCOM_API_SECRET").unwrap_or_default();
        let has_credentials = !api_key.trim().is_empty() && !api_secret.trim().is_empty();
        let enabled_private_rest = std::env::var("CRYPTOCOM_PRIVATE_REST_ENABLED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(has_credentials);
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            public_ws_url: DEFAULT_PUBLIC_WS_URL.to_string(),
            private_ws_url: DEFAULT_PRIVATE_WS_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl CryptoComGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && !self.api_key.trim().is_empty()
            && !self.api_secret.trim().is_empty()
    }
}
