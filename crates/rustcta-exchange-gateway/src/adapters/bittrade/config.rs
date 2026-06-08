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
        let api_key = std::env::var("BITTRADE_API_KEY")
            .or_else(|_| std::env::var("RUSTCTA_BITTRADE_API_KEY"))
            .ok();
        let api_secret = std::env::var("BITTRADE_API_SECRET")
            .or_else(|_| std::env::var("RUSTCTA_BITTRADE_API_SECRET"))
            .ok();
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            public_websocket_url: DEFAULT_PUBLIC_WS_URL.to_string(),
            private_websocket_url: DEFAULT_PRIVATE_WS_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest: false,
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
