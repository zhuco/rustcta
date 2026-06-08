#[derive(Debug, Clone)]
pub struct CryptomusGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub api_key: Option<String>,
    pub user_id: Option<String>,
    pub enabled_public_rest: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CryptomusGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.cryptomus.com".to_string(),
            public_ws_url: "wss://api-ws.cryptomus.com/ws".to_string(),
            api_key: non_empty_env("CRYPTOMUS_API_KEY"),
            user_id: non_empty_env("CRYPTOMUS_USER_ID"),
            enabled_public_rest: env_bool("CRYPTOMUS_PUBLIC_REST_ENABLED").unwrap_or(true),
            enabled_private_rest: env_bool("CRYPTOMUS_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("CRYPTOMUS_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("CRYPTOMUS_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl CryptomusGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .user_id
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
