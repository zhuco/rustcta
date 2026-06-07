#[derive(Debug, Clone)]
pub struct BitkanGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BitkanGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://bitkan.com".to_string(),
            public_ws_url: "wss://bitkan.com/ws".to_string(),
            api_key: non_empty_env("BITKAN_API_KEY"),
            api_secret: non_empty_env("BITKAN_API_SECRET"),
            enabled_private_rest: env_bool("BITKAN_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("BITKAN_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl BitkanGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
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
