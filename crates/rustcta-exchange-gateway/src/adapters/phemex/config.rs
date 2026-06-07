use std::fmt;

#[derive(Clone)]
pub struct PhemexGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub request_expiry_seconds: i64,
    pub enabled: bool,
}

impl Default for PhemexGatewayConfig {
    fn default() -> Self {
        let api_key = non_empty_env("PHEMEX_API_KEY");
        let api_secret = non_empty_env("PHEMEX_API_SECRET");
        let enabled_private_rest = env_bool("PHEMEX_PRIVATE_REST_ENABLED")
            .unwrap_or_else(|| api_key.is_some() && api_secret.is_some());
        let rest_base_url = non_empty_env("PHEMEX_REST_BASE_URL")
            .unwrap_or_else(|| "https://api.phemex.com".to_string());
        Self {
            rest_base_url,
            public_ws_url: non_empty_env("PHEMEX_PUBLIC_WS_URL")
                .unwrap_or_else(|| "wss://ws.phemex.com".to_string()),
            private_ws_url: non_empty_env("PHEMEX_PRIVATE_WS_URL")
                .unwrap_or_else(|| "wss://ws.phemex.com".to_string()),
            api_key,
            api_secret,
            enabled_private_rest,
            request_timeout_ms: 10_000,
            request_expiry_seconds: 60,
            enabled: true,
        }
    }
}

impl PhemexGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

impl fmt::Debug for PhemexGatewayConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PhemexGatewayConfig")
            .field("rest_base_url", &self.rest_base_url)
            .field("public_ws_url", &self.public_ws_url)
            .field("private_ws_url", &self.private_ws_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("enabled_private_rest", &self.enabled_private_rest)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("request_expiry_seconds", &self.request_expiry_seconds)
            .field("enabled", &self.enabled)
            .finish()
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
