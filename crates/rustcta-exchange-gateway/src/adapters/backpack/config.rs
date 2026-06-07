#[derive(Debug, Clone)]
pub struct BackpackGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub recv_window_ms: u64,
    pub enabled: bool,
}

impl Default for BackpackGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.backpack.exchange".to_string(),
            public_ws_url: "wss://ws.backpack.exchange".to_string(),
            private_ws_url: "wss://ws.backpack.exchange".to_string(),
            api_key: non_empty_env("BACKPACK_API_KEY"),
            api_secret: non_empty_env("BACKPACK_API_SECRET"),
            enabled_private_rest: env_bool("BACKPACK_PRIVATE_REST_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            recv_window_ms: 5_000,
            enabled: true,
        }
    }
}

impl BackpackGatewayConfig {
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
