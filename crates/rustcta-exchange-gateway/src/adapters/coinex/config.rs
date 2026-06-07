const DEFAULT_REST_BASE_URL: &str = "https://api.coinex.com/v2";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct CoinExGatewayConfig {
    pub rest_base_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CoinExGatewayConfig {
    fn default() -> Self {
        let api_key = std::env::var("COINEX_API_KEY").unwrap_or_default();
        let api_secret = std::env::var("COINEX_API_SECRET").unwrap_or_default();
        let has_credentials = !api_key.trim().is_empty() && !api_secret.trim().is_empty();
        let enabled_private_rest = std::env::var("COINEX_PRIVATE_REST_ENABLED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(has_credentials);
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl CoinExGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && !self.api_key.trim().is_empty()
            && !self.api_secret.trim().is_empty()
    }
}
