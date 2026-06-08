#[derive(Debug, Clone)]
pub struct CexGatewayConfig {
    pub rest_base_url: String,
    pub websocket_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub user_id: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CexGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://cex.io/api".to_string(),
            websocket_url: "wss://ws.cex.io/ws/".to_string(),
            api_key: non_empty_env("CEX_API_KEY").or_else(|| non_empty_env("CEXIO_API_KEY")),
            api_secret: non_empty_env("CEX_API_SECRET")
                .or_else(|| non_empty_env("CEXIO_API_SECRET")),
            user_id: non_empty_env("CEX_USER_ID").or_else(|| non_empty_env("CEXIO_USER_ID")),
            enabled_private_rest: env_bool("CEX_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("CEX_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("CEX_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl CexGatewayConfig {
    pub fn private_rest_configured(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .user_id
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
