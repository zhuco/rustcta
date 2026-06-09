#[derive(Debug, Clone)]
pub struct LighterGatewayConfig {
    pub rest_base_url: String,
    pub ws_url: String,
    pub api_key_index: Option<String>,
    pub auth_token: Option<String>,
    pub account_index: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for LighterGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://mainnet.zklighter.elliot.ai/api/v1".to_string(),
            ws_url: "wss://mainnet.zklighter.elliot.ai/stream".to_string(),
            api_key_index: non_empty_env_prefixed("LIGHTER_API_KEY_INDEX"),
            auth_token: non_empty_env_prefixed("LIGHTER_AUTH_TOKEN"),
            account_index: non_empty_env_prefixed("LIGHTER_ACCOUNT_INDEX"),
            enabled_private_rest: env_bool_prefixed("LIGHTER_PRIVATE_REST_ENABLED")
                .unwrap_or(false),
            enabled_public_streams: env_bool("LIGHTER_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("LIGHTER_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl LighterGatewayConfig {
    pub fn private_read_available(&self) -> bool {
        self.enabled_private_rest
            && self
                .auth_token
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .account_index
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

fn non_empty_env_prefixed(key: &str) -> Option<String> {
    non_empty_env(&format!("RUSTCTA_{key}")).or_else(|| non_empty_env(key))
}

fn env_bool_prefixed(key: &str) -> Option<bool> {
    env_bool(&format!("RUSTCTA_{key}")).or_else(|| env_bool(key))
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
