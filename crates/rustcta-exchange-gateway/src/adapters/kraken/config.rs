const DEFAULT_SPOT_REST_BASE_URL: &str = "https://api.kraken.com/0";
const DEFAULT_FUTURES_REST_BASE_URL: &str = "https://futures.kraken.com/derivatives/api/v3";
const DEFAULT_SPOT_WS_URL: &str = "wss://ws.kraken.com/v2";
const DEFAULT_SPOT_PRIVATE_WS_URL: &str = "wss://ws-auth.kraken.com/v2";
const DEFAULT_FUTURES_WS_URL: &str = "wss://futures.kraken.com/ws/v1";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct KrakenGatewayConfig {
    pub spot_rest_base_url: String,
    pub futures_rest_base_url: String,
    pub spot_ws_url: String,
    pub spot_private_ws_url: String,
    pub futures_ws_url: String,
    pub spot_api_key: Option<String>,
    pub spot_api_secret: Option<String>,
    pub futures_api_key: Option<String>,
    pub futures_api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for KrakenGatewayConfig {
    fn default() -> Self {
        let spot_api_key = env_non_empty("KRAKEN_API_KEY");
        let spot_api_secret = env_non_empty("KRAKEN_API_SECRET");
        let futures_api_key =
            env_non_empty("KRAKEN_FUTURES_API_KEY").or_else(|| spot_api_key.clone());
        let futures_api_secret =
            env_non_empty("KRAKEN_FUTURES_API_SECRET").or_else(|| spot_api_secret.clone());
        let has_spot_private_credentials = spot_api_key.is_some() && spot_api_secret.is_some();
        let has_futures_private_credentials =
            futures_api_key.is_some() && futures_api_secret.is_some();
        let has_private_credentials =
            has_spot_private_credentials || has_futures_private_credentials;
        let enabled_private_rest = std::env::var("KRAKEN_PRIVATE_REST_ENABLED")
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(has_private_credentials);
        Self {
            spot_rest_base_url: DEFAULT_SPOT_REST_BASE_URL.to_string(),
            futures_rest_base_url: DEFAULT_FUTURES_REST_BASE_URL.to_string(),
            spot_ws_url: DEFAULT_SPOT_WS_URL.to_string(),
            spot_private_ws_url: DEFAULT_SPOT_PRIVATE_WS_URL.to_string(),
            futures_ws_url: DEFAULT_FUTURES_WS_URL.to_string(),
            spot_api_key,
            spot_api_secret,
            futures_api_key,
            futures_api_secret,
            enabled_private_rest,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl KrakenGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.spot_private_rest_enabled() || self.futures_private_rest_enabled()
    }

    pub fn spot_private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self.spot_api_key.as_deref().is_some_and(has_text)
            && self.spot_api_secret.as_deref().is_some_and(has_text)
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
