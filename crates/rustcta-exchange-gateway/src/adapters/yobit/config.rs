#[derive(Debug, Clone)]
pub struct YobitGatewayConfig {
    pub rest_base_url: String,
    pub api_key: String,
    pub api_secret: String,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for YobitGatewayConfig {
    fn default() -> Self {
        let api_key =
            non_empty_env("YOBIT_SPOT_API_KEY").or_else(|| non_empty_env("YOBIT_API_KEY"));
        let api_secret =
            non_empty_env("YOBIT_SPOT_API_SECRET").or_else(|| non_empty_env("YOBIT_API_SECRET"));
        Self {
            rest_base_url: "https://yobit.net".to_string(),
            api_key: api_key.unwrap_or_default(),
            api_secret: api_secret.unwrap_or_default(),
            enabled_private_rest: env_bool("YOBIT_PRIVATE_REST_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl YobitGatewayConfig {
    pub fn private_rest_configured(&self) -> bool {
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
