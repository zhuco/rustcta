#[derive(Debug, Clone)]
pub struct CoinmateGatewayConfig {
    pub rest_base_url: String,
    pub ws_url: String,
    pub client_id: Option<String>,
    pub public_key: Option<String>,
    pub private_key: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CoinmateGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://coinmate.io/api".to_string(),
            ws_url: "wss://coinmate.io/api/websocket".to_string(),
            client_id: non_empty_env("COINMATE_CLIENT_ID"),
            public_key: non_empty_env("COINMATE_PUBLIC_KEY"),
            private_key: non_empty_env("COINMATE_PRIVATE_KEY")
                .or_else(|| non_empty_env("COINMATE_API_SECRET")),
            enabled_private_rest: env_bool("COINMATE_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("COINMATE_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("COINMATE_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl CoinmateGatewayConfig {
    pub fn private_auth_available(&self) -> bool {
        self.client_id
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .public_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .private_key
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
