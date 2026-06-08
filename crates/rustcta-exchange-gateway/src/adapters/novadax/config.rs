use std::fmt;

#[derive(Clone)]
pub struct NovadaxGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub account_id: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for NovadaxGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.novadax.com".to_string(),
            public_ws_url: "wss://api.novadax.com".to_string(),
            private_ws_url: "wss://api.novadax.com".to_string(),
            api_key: non_empty_env("NOVADAX_API_KEY"),
            api_secret: non_empty_env("NOVADAX_API_SECRET"),
            account_id: non_empty_env("NOVADAX_ACCOUNT_ID"),
            enabled_private_rest: env_bool("NOVADAX_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("NOVADAX_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("NOVADAX_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl fmt::Debug for NovadaxGatewayConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NovadaxGatewayConfig")
            .field("rest_base_url", &self.rest_base_url)
            .field("public_ws_url", &self.public_ws_url)
            .field("private_ws_url", &self.private_ws_url)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "account_id",
                &self.account_id.as_ref().map(|_| "<redacted>"),
            )
            .field("enabled_private_rest", &self.enabled_private_rest)
            .field("enabled_public_streams", &self.enabled_public_streams)
            .field("enabled_private_streams", &self.enabled_private_streams)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("enabled", &self.enabled)
            .finish()
    }
}

impl NovadaxGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
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
