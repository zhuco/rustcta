#[derive(Debug, Clone)]
pub struct AsterGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub user_address: Option<String>,
    pub signer_address: Option<String>,
    pub signer_private_key: Option<String>,
    pub recv_window_ms: u64,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for AsterGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://fapi.asterdex.com".to_string(),
            public_ws_url: "wss://fstream.asterdex.com/ws".to_string(),
            private_ws_url: "wss://fstream.asterdex.com/ws".to_string(),
            user_address: non_empty_env("ASTER_USER_ADDRESS"),
            signer_address: non_empty_env("ASTER_SIGNER_ADDRESS"),
            signer_private_key: non_empty_env("ASTER_SIGNER_PRIVATE_KEY"),
            recv_window_ms: env_u64("ASTER_RECV_WINDOW_MS").unwrap_or(5_000),
            enabled_private_rest: env_bool("ASTER_PRIVATE_REST_ENABLED").unwrap_or(true),
            enabled_public_streams: env_bool("ASTER_PUBLIC_STREAMS_ENABLED").unwrap_or(true),
            enabled_private_streams: env_bool("ASTER_PRIVATE_STREAMS_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl AsterGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .user_address
                .as_ref()
                .is_some_and(|address| !address.trim().is_empty())
            && self
                .signer_address
                .as_ref()
                .is_some_and(|address| !address.trim().is_empty())
            && self
                .signer_private_key
                .as_ref()
                .is_some_and(|private_key| !private_key.trim().is_empty())
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_u64(key: &str) -> Option<u64> {
    non_empty_env(key)?.parse().ok()
}

fn env_bool(key: &str) -> Option<bool> {
    match non_empty_env(key)?.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}
