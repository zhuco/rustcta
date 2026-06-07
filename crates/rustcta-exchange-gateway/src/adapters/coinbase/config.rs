const DEFAULT_REST_BASE_URL: &str = "https://api.coinbase.com/api/v3/brokerage";
const DEFAULT_PUBLIC_WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";
const DEFAULT_PRIVATE_WS_URL: &str = "wss://advanced-trade-ws-user.coinbase.com";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct CoinbaseGatewayConfig {
    pub spot_rest_base_url: String,
    pub international_rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub perpetual_portfolio_uuid: Option<String>,
    pub bearer_token: String,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CoinbaseGatewayConfig {
    fn default() -> Self {
        let bearer_token = std::env::var("COINBASE_API_BEARER_TOKEN")
            .or_else(|_| std::env::var("COINBASE_JWT"))
            .unwrap_or_default();
        let has_credentials = !bearer_token.trim().is_empty();
        let enabled_private_rest = std::env::var("COINBASE_PRIVATE_REST_ENABLED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(has_credentials);
        Self {
            spot_rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            international_rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            public_ws_url: DEFAULT_PUBLIC_WS_URL.to_string(),
            private_ws_url: DEFAULT_PRIVATE_WS_URL.to_string(),
            perpetual_portfolio_uuid: std::env::var("COINBASE_INTX_PORTFOLIO_UUID")
                .ok()
                .filter(|value| !value.trim().is_empty()),
            bearer_token,
            enabled_private_rest,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl CoinbaseGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest && !self.bearer_token.trim().is_empty()
    }
}
