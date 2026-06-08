#[derive(Debug, Clone)]
pub struct HollaexGatewayConfig {
    pub rest_base_url: String,
    pub ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for HollaexGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: non_empty_env("HOLLAEX_REST_BASE_URL")
                .or_else(|| non_empty_env("RUSTCTA_HOLLAEX_REST_BASE_URL"))
                .unwrap_or_else(|| "https://api.hollaex.com/v2".to_string()),
            ws_url: non_empty_env("HOLLAEX_WS_URL")
                .or_else(|| non_empty_env("RUSTCTA_HOLLAEX_WS_URL"))
                .unwrap_or_else(|| "wss://api.hollaex.com/stream".to_string()),
            api_key: non_empty_env("HOLLAEX_API_KEY")
                .or_else(|| non_empty_env("RUSTCTA_HOLLAEX_API_KEY")),
            api_secret: non_empty_env("HOLLAEX_API_SECRET")
                .or_else(|| non_empty_env("RUSTCTA_HOLLAEX_API_SECRET")),
            enabled_private_rest: env_bool("HOLLAEX_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("RUSTCTA_HOLLAEX_PRIVATE_REST_ENABLED"))
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: false,
        }
    }
}

impl HollaexGatewayConfig {
    pub fn private_request_specs_enabled(&self) -> bool {
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
