const DEFAULT_REST_BASE_URL: &str = "https://api.coinmetro.com";
const DEFAULT_WS_URL: &str = "wss://api.coinmetro.com/ws";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct CoinmetroGatewayConfig {
    pub rest_base_url: String,
    pub ws_url: String,
    pub api_token: String,
    pub device_id: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CoinmetroGatewayConfig {
    fn default() -> Self {
        let api_token = std::env::var("COINMETRO_API_TOKEN").unwrap_or_default();
        let device_id = std::env::var("COINMETRO_DEVICE_ID")
            .ok()
            .filter(|value| !value.trim().is_empty());
        let has_credentials = !api_token.trim().is_empty();
        let enabled_private_rest = std::env::var("COINMETRO_PRIVATE_REST_ENABLED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(has_credentials);
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            ws_url: DEFAULT_WS_URL.to_string(),
            api_token,
            device_id,
            enabled_private_rest,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl CoinmetroGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest && !self.api_token.trim().is_empty()
    }
}
