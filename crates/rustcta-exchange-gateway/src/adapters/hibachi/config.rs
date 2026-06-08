#[derive(Debug, Clone)]
pub struct HibachiGatewayConfig {
    pub data_rest_base_url: String,
    pub account_rest_base_url: String,
    pub public_ws_url: String,
    pub account_ws_url: String,
    pub trade_ws_url: String,
    pub api_key: Option<String>,
    pub private_key: Option<String>,
    pub public_key: Option<String>,
    pub account_id: Option<String>,
    pub enabled_public_rest: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for HibachiGatewayConfig {
    fn default() -> Self {
        Self {
            data_rest_base_url: "https://data-api.hibachi.xyz".to_string(),
            account_rest_base_url: "https://api.hibachi.xyz".to_string(),
            public_ws_url: "wss://data-api.hibachi.xyz/ws/market".to_string(),
            account_ws_url: "wss://api.hibachi.xyz/ws/account".to_string(),
            trade_ws_url: "wss://api.hibachi.xyz/ws/trade".to_string(),
            api_key: non_empty_env("HIBACHI_API_KEY")
                .or_else(|| non_empty_env("RUSTCTA_HIBACHI_API_KEY")),
            private_key: non_empty_env("HIBACHI_PRIVATE_KEY")
                .or_else(|| non_empty_env("RUSTCTA_HIBACHI_PRIVATE_KEY")),
            public_key: non_empty_env("HIBACHI_PUBLIC_KEY")
                .or_else(|| non_empty_env("RUSTCTA_HIBACHI_PUBLIC_KEY")),
            account_id: non_empty_env("HIBACHI_ACCOUNT_ID")
                .or_else(|| non_empty_env("RUSTCTA_HIBACHI_ACCOUNT_ID")),
            enabled_public_rest: env_bool("HIBACHI_PUBLIC_REST_ENABLED").unwrap_or(true),
            enabled_private_rest: env_bool("HIBACHI_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("HIBACHI_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("HIBACHI_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl HibachiGatewayConfig {
    pub fn private_read_available(&self) -> bool {
        self.enabled_private_rest
            && has_value(&self.api_key)
            && self.normalized_account_id().is_some()
    }

    pub fn write_request_spec_available(&self) -> bool {
        has_value(&self.private_key) && self.normalized_account_id().is_some()
    }

    pub fn normalized_account_id(&self) -> Option<String> {
        self.account_id
            .as_ref()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }
}

fn has_value(value: &Option<String>) -> bool {
    value.as_ref().is_some_and(|value| !value.trim().is_empty())
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
