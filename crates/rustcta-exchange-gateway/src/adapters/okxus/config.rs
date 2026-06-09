#[derive(Debug, Clone)]
pub struct OkxusGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub request_timeout_ms: u64,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub passphrase: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled: bool,
}

impl Default for OkxusGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://us.okx.com".to_string(),
            public_ws_url: "wss://wsus.okx.com:8443/ws/v5/public".to_string(),
            private_ws_url: "wss://wsus.okx.com:8443/ws/v5/private".to_string(),
            request_timeout_ms: 10_000,
            api_key: env_first(["RUSTCTA_OKXUS_API_KEY", "OKXUS_API_KEY"]),
            api_secret: env_first(["RUSTCTA_OKXUS_API_SECRET", "OKXUS_API_SECRET"]),
            passphrase: env_first([
                "RUSTCTA_OKXUS_PASSPHRASE",
                "OKXUS_PASSPHRASE",
                "RUSTCTA_OKXUS_API_PASSPHRASE",
                "OKXUS_API_PASSPHRASE",
            ]),
            enabled_private_rest: env_bool("RUSTCTA_OKXUS_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("OKXUS_PRIVATE_REST_ENABLED"))
                .unwrap_or(false),
            enabled: true,
        }
    }
}

fn env_first<const N: usize>(keys: [&str; N]) -> Option<String> {
    keys.into_iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

fn env_bool(key: &str) -> Option<bool> {
    match env_first([key])?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
