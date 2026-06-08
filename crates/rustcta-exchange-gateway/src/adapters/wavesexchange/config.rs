#[derive(Debug, Clone)]
pub struct WavesExchangeGatewayConfig {
    pub rest_base_url: String,
    pub testnet_rest_base_url: String,
    pub public_ws_url: String,
    pub enabled_public_rest: bool,
    pub enabled_public_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for WavesExchangeGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://matcher.waves.exchange".to_string(),
            testnet_rest_base_url: "https://matcher-testnet.waves.exchange".to_string(),
            public_ws_url: "wss://matcher.waves.exchange/ws/v0".to_string(),
            enabled_public_rest: env_bool("WAVESEXCHANGE_PUBLIC_REST_ENABLED").unwrap_or(true),
            enabled_public_streams: env_bool("WAVESEXCHANGE_PUBLIC_STREAMS_ENABLED")
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
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
