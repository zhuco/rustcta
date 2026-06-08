const DEFAULT_REST_BASE_URL: &str = "https://www.btcbox.co.jp/api/v1";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 10_000;

#[derive(Debug, Clone)]
pub struct BtcboxGatewayConfig {
    pub rest_base_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BtcboxGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: DEFAULT_REST_BASE_URL.to_string(),
            api_key: non_empty_env("BTCBOX_API_KEY"),
            api_secret: non_empty_env("BTCBOX_API_SECRET"),
            enabled_private_rest: env_bool("BTCBOX_PRIVATE_REST_ENABLED").unwrap_or(false),
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            enabled: true,
        }
    }
}

impl BtcboxGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        false
    }

    pub fn has_private_credentials(&self) -> bool {
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
