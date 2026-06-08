const DEFAULT_FUTURES_REST_BASE_URL: &str = "https://futures.kraken.com/derivatives/api/v3";
const DEFAULT_FUTURES_WS_URL: &str = "wss://futures.kraken.com/ws/v1";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct KrakenFuturesGatewayConfig {
    pub futures_rest_base_url: String,
    pub futures_ws_url: String,
    pub futures_api_key: Option<String>,
    pub futures_api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for KrakenFuturesGatewayConfig {
    fn default() -> Self {
        let futures_api_key = env_non_empty("KRAKEN_FUTURES_API_KEY")
            .or_else(|| env_non_empty("RUSTCTA_KRAKEN_FUTURES_API_KEY"));
        let futures_api_secret = env_non_empty("KRAKEN_FUTURES_API_SECRET")
            .or_else(|| env_non_empty("RUSTCTA_KRAKEN_FUTURES_API_SECRET"));
        let has_futures_private_credentials =
            futures_api_key.is_some() && futures_api_secret.is_some();
        let enabled_private_rest = std::env::var("KRAKEN_FUTURES_PRIVATE_REST_ENABLED")
            .or_else(|_| std::env::var("RUSTCTA_KRAKEN_FUTURES_PRIVATE_REST_ENABLED"))
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(has_futures_private_credentials);
        Self {
            futures_rest_base_url: DEFAULT_FUTURES_REST_BASE_URL.to_string(),
            futures_ws_url: DEFAULT_FUTURES_WS_URL.to_string(),
            futures_api_key,
            futures_api_secret,
            enabled_private_rest,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl KrakenFuturesGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.futures_private_rest_enabled()
    }

    pub fn futures_private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self.futures_api_key.as_deref().is_some_and(has_text)
            && self.futures_api_secret.as_deref().is_some_and(has_text)
    }
}

fn env_non_empty(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn has_text(value: &str) -> bool {
    !value.trim().is_empty()
}
