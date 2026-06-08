#[derive(Debug, Clone)]
pub struct BitfinexGatewayConfig {
    pub public_rest_base_url: String,
    pub private_rest_base_url: String,
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

impl Default for BitfinexGatewayConfig {
    fn default() -> Self {
        Self {
            public_rest_base_url: "https://api-pub.bitfinex.com".to_string(),
            private_rest_base_url: "https://api.bitfinex.com".to_string(),
            public_ws_url: "wss://api-pub.bitfinex.com/ws/2".to_string(),
            private_ws_url: "wss://api.bitfinex.com/ws/2".to_string(),
            api_key: non_empty_env("BITFINEX_API_KEY"),
            api_secret: non_empty_env("BITFINEX_API_SECRET"),
            enabled_private_rest: env_bool("BITFINEX_PRIVATE_REST_ENABLED").unwrap_or(true),
            enabled_public_streams: env_bool("BITFINEX_PUBLIC_STREAMS_ENABLED").unwrap_or(true),
            enabled_private_streams: env_bool("BITFINEX_PRIVATE_STREAMS_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl BitfinexGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest && self.has_private_credentials()
    }

    pub fn private_streams_enabled(&self) -> bool {
        self.enabled_private_streams && self.has_private_credentials()
    }

    fn has_private_credentials(&self) -> bool {
        self.api_key
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
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
