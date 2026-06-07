#[derive(Debug, Clone)]
pub struct WeexGatewayConfig {
    pub spot_rest_base_url: String,
    pub contract_rest_base_url: String,
    pub spot_public_ws_url: String,
    pub spot_private_ws_url: String,
    pub contract_public_ws_url: String,
    pub contract_private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub passphrase: Option<String>,
    pub recv_window_ms: u64,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for WeexGatewayConfig {
    fn default() -> Self {
        Self {
            spot_rest_base_url: "https://api-spot.weex.com".to_string(),
            contract_rest_base_url: "https://api-contract.weex.com".to_string(),
            spot_public_ws_url: "wss://ws-spot.weex.com/v3/ws/public".to_string(),
            spot_private_ws_url: "wss://ws-spot.weex.com/v3/ws/private".to_string(),
            contract_public_ws_url: "wss://ws-contract.weex.com/v3/ws/public".to_string(),
            contract_private_ws_url: "wss://ws-contract.weex.com/v3/ws/private".to_string(),
            api_key: non_empty_env("WEEX_API_KEY"),
            api_secret: non_empty_env("WEEX_API_SECRET"),
            passphrase: non_empty_env("WEEX_API_PASSPHRASE"),
            recv_window_ms: env_u64("WEEX_RECV_WINDOW_MS").unwrap_or(5_000),
            enabled_private_rest: env_bool("WEEX_PRIVATE_REST_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl WeexGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
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
