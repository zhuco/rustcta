#[derive(Debug, Clone)]
pub struct PaymiumGatewayConfig {
    pub rest_base_url: String,
    pub websocket_public_url: String,
    pub websocket_user_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for PaymiumGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://paymium.com/api/v1".to_string(),
            websocket_public_url: "https://paymium.com/public".to_string(),
            websocket_user_url: "https://paymium.com/user".to_string(),
            api_key: non_empty_env("PAYMIUM_API_KEY"),
            api_secret: non_empty_env("PAYMIUM_API_SECRET"),
            enabled_private_rest: env_bool("PAYMIUM_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("PAYMIUM_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("PAYMIUM_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl PaymiumGatewayConfig {
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
