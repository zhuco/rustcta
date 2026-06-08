#[derive(Debug, Clone)]
pub struct BequantGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub trading_ws_url: String,
    pub wallet_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BequantGatewayConfig {
    fn default() -> Self {
        let api_key = non_empty_env("BEQUANT_API_KEY");
        let api_secret = non_empty_env("BEQUANT_API_SECRET");
        let has_credentials = api_key.is_some() && api_secret.is_some();
        Self {
            rest_base_url: non_empty_env("BEQUANT_REST_BASE_URL")
                .unwrap_or_else(|| "https://api.bequant.io/api/3".to_string()),
            public_ws_url: non_empty_env("BEQUANT_PUBLIC_WS_URL")
                .unwrap_or_else(|| "wss://api.bequant.io/api/3/ws/public".to_string()),
            trading_ws_url: non_empty_env("BEQUANT_TRADING_WS_URL")
                .unwrap_or_else(|| "wss://api.bequant.io/api/3/ws/trading".to_string()),
            wallet_ws_url: non_empty_env("BEQUANT_WALLET_WS_URL")
                .unwrap_or_else(|| "wss://api.bequant.io/api/3/ws/wallet".to_string()),
            api_key,
            api_secret,
            enabled_private_rest: env_bool("BEQUANT_PRIVATE_REST_ENABLED")
                .unwrap_or(has_credentials),
            enabled_private_streams: env_bool("BEQUANT_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: non_empty_env("BEQUANT_REQUEST_TIMEOUT_MS")
                .and_then(|value| value.parse().ok())
                .unwrap_or(10_000),
            enabled: env_bool("BEQUANT_ENABLED").unwrap_or(true),
        }
    }
}

impl BequantGatewayConfig {
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

    pub fn private_streams_enabled(&self) -> bool {
        self.enabled_private_streams && self.private_rest_enabled()
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_bool(key: &str) -> Option<bool> {
    match non_empty_env(key)?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
