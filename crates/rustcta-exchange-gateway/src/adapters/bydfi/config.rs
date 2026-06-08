#[derive(Debug, Clone)]
pub struct BydfiGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: Option<String>,
    pub wallet: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BydfiGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: env_or("BYDFI_REST_BASE_URL", "https://api.bydfi.com/api"),
            public_ws_url: env_or(
                "BYDFI_PUBLIC_WS_URL",
                "wss://stream.bydfi.com/v1/public/fapi",
            ),
            private_ws_url: non_empty_env("BYDFI_PRIVATE_WS_URL"),
            wallet: env_or("BYDFI_WALLET", "W001"),
            api_key: non_empty_env("BYDFI_API_KEY"),
            api_secret: non_empty_env("BYDFI_API_SECRET"),
            enabled_private_rest: env_bool("BYDFI_PRIVATE_REST_ENABLED").unwrap_or(true),
            enabled_public_streams: env_bool("BYDFI_PUBLIC_STREAMS_ENABLED").unwrap_or(true),
            enabled_private_streams: env_bool("BYDFI_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl BydfiGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest && self.has_private_credentials()
    }

    pub fn private_streams_enabled(&self) -> bool {
        self.enabled_private_streams
            && self.private_ws_url.is_some()
            && self.has_private_credentials()
    }

    fn has_private_credentials(&self) -> bool {
        self.api_key
            .as_ref()
            .is_some_and(|key| !key.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|secret| !secret.trim().is_empty())
    }
}

fn env_or(key: &str, fallback: &str) -> String {
    non_empty_env(key).unwrap_or_else(|| fallback.to_string())
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
