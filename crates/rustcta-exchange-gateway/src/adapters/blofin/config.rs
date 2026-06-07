#[derive(Debug, Clone)]
pub struct BlofinGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub passphrase: Option<String>,
    pub margin_mode: String,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BlofinGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://openapi.blofin.com".to_string(),
            public_ws_url: "wss://openapi.blofin.com/ws/public".to_string(),
            private_ws_url: "wss://openapi.blofin.com/ws/private".to_string(),
            api_key: non_empty_env("BLOFIN_API_KEY"),
            api_secret: non_empty_env("BLOFIN_API_SECRET"),
            passphrase: non_empty_env("BLOFIN_API_PASSPHRASE")
                .or_else(|| non_empty_env("BLOFIN_PASSPHRASE")),
            margin_mode: non_empty_env("BLOFIN_MARGIN_MODE").unwrap_or_else(|| "cross".to_string()),
            enabled_private_rest: env_bool("BLOFIN_PRIVATE_REST_ENABLED").unwrap_or(true),
            enabled_public_streams: env_bool("BLOFIN_PUBLIC_STREAMS_ENABLED").unwrap_or(true),
            enabled_private_streams: env_bool("BLOFIN_PRIVATE_STREAMS_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl BlofinGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest && self.has_private_credentials()
    }

    pub fn private_streams_enabled(&self) -> bool {
        self.enabled_private_streams && self.has_private_credentials()
    }

    pub fn has_private_credentials(&self) -> bool {
        self.api_key
            .as_ref()
            .is_some_and(|key| !key.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|secret| !secret.trim().is_empty())
            && self
                .passphrase
                .as_ref()
                .is_some_and(|passphrase| !passphrase.trim().is_empty())
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
