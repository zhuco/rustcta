const DEFAULT_REST_BASE_URL: &str = "https://bitbns.com";
const DEFAULT_PRIVATE_REST_BASE_URL: &str = "https://api.bitbns.com/api/trade/v1";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct BitbnsGatewayConfig {
    pub rest_base_url: String,
    pub private_rest_base_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BitbnsGatewayConfig {
    fn default() -> Self {
        let api_key = std::env::var("BITBNS_API_KEY")
            .or_else(|_| std::env::var("RUSTCTA_BITBNS_API_KEY"))
            .unwrap_or_default();
        let api_secret = std::env::var("BITBNS_API_SECRET")
            .or_else(|_| std::env::var("RUSTCTA_BITBNS_API_SECRET"))
            .unwrap_or_default();
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            private_rest_base_url: DEFAULT_PRIVATE_REST_BASE_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest: false,
            enabled_public_streams: false,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl BitbnsGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && !self.api_key.trim().is_empty()
            && !self.api_secret.trim().is_empty()
    }
}
