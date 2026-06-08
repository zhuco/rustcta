#[derive(Debug, Clone)]
pub struct CoinspotGatewayConfig {
    pub rest_base_url: String,
    pub public_rest_base_url: String,
    pub private_rest_base_url: String,
    pub read_only_rest_base_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CoinspotGatewayConfig {
    fn default() -> Self {
        let api_key =
            non_empty_env("COINSPOT_SPOT_API_KEY").or_else(|| non_empty_env("COINSPOT_API_KEY"));
        let api_secret = non_empty_env("COINSPOT_SPOT_API_SECRET")
            .or_else(|| non_empty_env("COINSPOT_API_SECRET"));
        let enabled_private_rest = env_bool("COINSPOT_SPOT_PRIVATE_REST_ENABLED")
            .or_else(|| env_bool("COINSPOT_PRIVATE_REST_ENABLED"))
            .unwrap_or_else(|| api_key.is_some() && api_secret.is_some());
        Self {
            rest_base_url: "https://www.coinspot.com.au".to_string(),
            public_rest_base_url: "https://www.coinspot.com.au".to_string(),
            private_rest_base_url: "https://www.coinspot.com.au".to_string(),
            read_only_rest_base_url: "https://www.coinspot.com.au".to_string(),
            api_key: api_key.unwrap_or_default(),
            api_secret: api_secret.unwrap_or_default(),
            enabled_private_rest,
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl CoinspotGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && !self.api_key.trim().is_empty()
            && !self.api_secret.trim().is_empty()
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
