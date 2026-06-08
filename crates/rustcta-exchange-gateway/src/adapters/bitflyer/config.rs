const DEFAULT_REST_BASE_URL: &str = "https://api.bitflyer.com/v1";
const DEFAULT_WS_URL: &str = "wss://ws.lightstream.bitflyer.com/json-rpc";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct BitflyerGatewayConfig {
    pub rest_base_url: String,
    pub websocket_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BitflyerGatewayConfig {
    fn default() -> Self {
        let api_key = std::env::var("BITFLYER_API_KEY").ok();
        let api_secret = std::env::var("BITFLYER_API_SECRET").ok();
        let has_credentials = api_key
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
            && api_secret
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty());
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            websocket_url: DEFAULT_WS_URL.to_string(),
            public_ws_url: DEFAULT_WS_URL.to_string(),
            private_ws_url: DEFAULT_WS_URL.to_string(),
            api_key,
            api_secret,
            enabled_private_rest: std::env::var("BITFLYER_PRIVATE_REST_ENABLED")
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

impl BitflyerGatewayConfig {
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
