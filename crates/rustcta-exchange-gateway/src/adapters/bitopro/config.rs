const DEFAULT_REST_BASE_URL: &str = "https://api.bitopro.com/v3";
const DEFAULT_WEBSOCKET_BASE_URL: &str = "wss://stream.bitopro.com:443/ws";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct BitoproGatewayConfig {
    pub rest_base_url: String,
    pub websocket_base_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_identity: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BitoproGatewayConfig {
    fn default() -> Self {
        let api_key = std::env::var("BITOPRO_API_KEY").ok();
        let api_secret = std::env::var("BITOPRO_API_SECRET").ok();
        let api_identity = std::env::var("BITOPRO_API_IDENTITY").ok();
        let has_private_credentials =
            non_empty(&api_key) && non_empty(&api_secret) && non_empty(&api_identity);
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            websocket_base_url: DEFAULT_WEBSOCKET_BASE_URL.to_string(),
            api_key,
            api_secret,
            api_identity,
            enabled_private_rest: std::env::var("BITOPRO_PRIVATE_REST_ENABLED")
                .ok()
                .and_then(|value| value.parse::<bool>().ok())
                .unwrap_or(has_private_credentials),
            enabled_public_streams: true,
            enabled_private_streams: false,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl BitoproGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && non_empty(&self.api_key)
            && non_empty(&self.api_secret)
            && non_empty(&self.api_identity)
    }
}

fn non_empty(value: &Option<String>) -> bool {
    value.as_ref().is_some_and(|value| !value.trim().is_empty())
}
