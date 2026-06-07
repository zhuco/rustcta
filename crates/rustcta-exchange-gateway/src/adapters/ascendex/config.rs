#[derive(Debug, Clone)]
pub struct AscendexGatewayConfig {
    pub rest_base_url: String,
    pub account_group: Option<String>,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for AscendexGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: non_empty_env("ASCENDEX_REST_BASE_URL")
                .unwrap_or_else(|| "https://ascendex.com".to_string()),
            account_group: non_empty_env("ASCENDEX_ACCOUNT_GROUP"),
            api_key: non_empty_env("ASCENDEX_API_KEY"),
            api_secret: non_empty_env("ASCENDEX_API_SECRET"),
            enabled_private_rest: env_bool("ASCENDEX_PRIVATE_REST_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl AscendexGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .account_group
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
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
