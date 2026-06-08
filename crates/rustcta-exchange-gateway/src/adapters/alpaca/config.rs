const DEFAULT_BROKER_REST_BASE_URL: &str = "https://broker-api.sandbox.alpaca.markets";
const DEFAULT_MARKET_DATA_REST_BASE_URL: &str = "https://data.sandbox.alpaca.markets";
const DEFAULT_PUBLIC_WS_BASE_URL: &str =
    "wss://stream.data.sandbox.alpaca.markets/v1beta3/crypto/us";
const DEFAULT_CRYPTO_LOCATION: &str = "us";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct AlpacaGatewayConfig {
    pub broker_rest_base_url: String,
    pub market_data_rest_base_url: String,
    pub public_ws_base_url: String,
    pub crypto_location: String,
    pub api_key: String,
    pub api_secret: String,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for AlpacaGatewayConfig {
    fn default() -> Self {
        let api_key = std::env::var("ALPACA_API_KEY")
            .or_else(|_| std::env::var("APCA_API_KEY_ID"))
            .or_else(|_| std::env::var("RUSTCTA_ALPACA_API_KEY"))
            .unwrap_or_default();
        let api_secret = std::env::var("ALPACA_API_SECRET")
            .or_else(|_| std::env::var("APCA_API_SECRET_KEY"))
            .or_else(|_| std::env::var("RUSTCTA_ALPACA_API_SECRET"))
            .unwrap_or_default();
        let has_credentials = !api_key.trim().is_empty() && !api_secret.trim().is_empty();
        let enabled_private_rest = std::env::var("ALPACA_PRIVATE_REST_ENABLED")
            .or_else(|_| std::env::var("RUSTCTA_ALPACA_PRIVATE_REST_ENABLED"))
            .ok()
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(has_credentials);
        Self {
            broker_rest_base_url: DEFAULT_BROKER_REST_BASE_URL.to_string(),
            market_data_rest_base_url: DEFAULT_MARKET_DATA_REST_BASE_URL.to_string(),
            public_ws_base_url: DEFAULT_PUBLIC_WS_BASE_URL.to_string(),
            crypto_location: DEFAULT_CRYPTO_LOCATION.to_string(),
            api_key,
            api_secret,
            enabled_private_rest,
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl AlpacaGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest && self.has_credentials()
    }

    pub fn market_data_enabled(&self) -> bool {
        self.has_credentials()
    }

    pub fn has_credentials(&self) -> bool {
        !self.api_key.trim().is_empty() && !self.api_secret.trim().is_empty()
    }
}
