#[derive(Debug, Clone)]
pub struct Cod3xGatewayConfig {
    pub docs_base_url: String,
    pub website_base_url: String,
    pub app_base_url: String,
    pub venue_profiles: Vec<String>,
    pub enabled_public_rest: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for Cod3xGatewayConfig {
    fn default() -> Self {
        Self {
            docs_base_url: "https://docs.cod3x.org".to_string(),
            website_base_url: "https://www.cod3x.org".to_string(),
            app_base_url: "https://app.cod3x.org".to_string(),
            venue_profiles: vec![
                "hyperliquid".to_string(),
                "gmx_v2".to_string(),
                "lighter".to_string(),
            ],
            enabled_public_rest: env_bool("RUSTCTA_COD3X_PUBLIC_REST_ENABLED")
                .or_else(|| env_bool("COD3X_PUBLIC_REST_ENABLED"))
                .unwrap_or(false),
            enabled_private_rest: env_bool("RUSTCTA_COD3X_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("COD3X_PRIVATE_REST_ENABLED"))
                .unwrap_or(false),
            enabled_public_streams: env_bool("RUSTCTA_COD3X_PUBLIC_STREAMS_ENABLED")
                .or_else(|| env_bool("COD3X_PUBLIC_STREAMS_ENABLED"))
                .unwrap_or(false),
            enabled_private_streams: env_bool("RUSTCTA_COD3X_PRIVATE_STREAMS_ENABLED")
                .or_else(|| env_bool("COD3X_PRIVATE_STREAMS_ENABLED"))
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl Cod3xGatewayConfig {
    pub fn read_runtime_available(&self) -> bool {
        self.enabled_public_rest
            || self.enabled_private_rest
            || self.enabled_public_streams
            || self.enabled_private_streams
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
