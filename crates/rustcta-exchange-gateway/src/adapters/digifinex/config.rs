#[derive(Debug, Clone)]
pub struct DigiFinexGatewayConfig {
    pub spot_rest_base_url: String,
    pub swap_rest_base_url: String,
    pub spot_public_ws_url: String,
    pub spot_private_ws_url: String,
    pub swap_public_ws_url: String,
    pub swap_private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled: bool,
    pub request_timeout_ms: u64,
}

impl Default for DigiFinexGatewayConfig {
    fn default() -> Self {
        Self {
            spot_rest_base_url: "https://openapi.digifinex.com".to_string(),
            swap_rest_base_url: "https://openapi.digifinex.com".to_string(),
            spot_public_ws_url: "wss://openapi.digifinex.com/ws/v1/".to_string(),
            spot_private_ws_url: "wss://openapi.digifinex.com/ws/v1/".to_string(),
            swap_public_ws_url: "wss://openapi.digifinex.com/swap_ws/v2/".to_string(),
            swap_private_ws_url: "wss://openapi.digifinex.com/swap_ws/v2/".to_string(),
            api_key: non_empty_env("DIGIFINEX_API_KEY"),
            api_secret: non_empty_env("DIGIFINEX_API_SECRET"),
            enabled_private_rest: env_bool("DIGIFINEX_PRIVATE_REST_ENABLED").unwrap_or(true),
            enabled: env_bool("DIGIFINEX_ENABLED").unwrap_or(true),
            request_timeout_ms: env_u64("DIGIFINEX_REQUEST_TIMEOUT_MS").unwrap_or(10_000),
        }
    }
}

impl DigiFinexGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_deref()
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
    non_empty_env(key).and_then(|value| match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    })
}

fn env_u64(key: &str) -> Option<u64> {
    non_empty_env(key).and_then(|value| value.parse().ok())
}
