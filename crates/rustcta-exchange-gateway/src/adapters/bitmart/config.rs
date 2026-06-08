#[derive(Debug, Clone)]
pub struct BitmartGatewayConfig {
    pub spot_rest_base_url: String,
    pub futures_rest_base_url: String,
    pub spot_ws_url: String,
    pub futures_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub memo: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BitmartGatewayConfig {
    fn default() -> Self {
        Self {
            spot_rest_base_url: "https://api-cloud.bitmart.com".to_string(),
            futures_rest_base_url: "https://api-cloud-v2.bitmart.com".to_string(),
            spot_ws_url: "wss://ws-manager-compress.bitmart.com/api?protocol=1.1".to_string(),
            futures_ws_url: "wss://openapi-ws-v2.bitmart.com/api?protocol=1.1".to_string(),
            api_key: non_empty_env("BITMART_API_KEY"),
            api_secret: non_empty_env("BITMART_API_SECRET"),
            memo: non_empty_env("BITMART_API_MEMO").or_else(|| non_empty_env("BITMART_MEMO")),
            enabled_private_rest: env_bool("BITMART_PRIVATE_REST_ENABLED").unwrap_or(true),
            enabled_public_streams: env_bool("BITMART_PUBLIC_STREAMS_ENABLED").unwrap_or(true),
            enabled_private_streams: env_bool("BITMART_PRIVATE_STREAMS_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl BitmartGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest && self.has_private_credentials()
    }

    pub fn private_streams_enabled(&self) -> bool {
        self.enabled_private_streams && self.has_private_credentials()
    }

    fn has_private_credentials(&self) -> bool {
        self.api_key
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .memo
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
