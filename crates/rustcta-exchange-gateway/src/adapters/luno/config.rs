#[derive(Debug, Clone)]
pub struct LunoGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key_id: Option<String>,
    pub api_key_secret: Option<String>,
    pub enabled_public_rest: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for LunoGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.luno.com".to_string(),
            public_ws_url: "wss://ws.luno.com/api/1/stream".to_string(),
            private_ws_url: "wss://ws.luno.com/api/1/stream".to_string(),
            api_key_id: non_empty_env("LUNO_API_KEY_ID").or_else(|| non_empty_env("LUNO_API_KEY")),
            api_key_secret: non_empty_env("LUNO_API_KEY_SECRET")
                .or_else(|| non_empty_env("LUNO_API_SECRET")),
            enabled_public_rest: env_bool("LUNO_PUBLIC_REST_ENABLED").unwrap_or(true),
            enabled_private_rest: env_bool("LUNO_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("LUNO_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("LUNO_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl LunoGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key_id
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_key_secret
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(format!("RUSTCTA_{key}"))
        .or_else(|_| std::env::var(key))
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
