#[derive(Debug, Clone)]
pub struct ApexGatewayConfig {
    pub rest_base_url: String,
    pub testnet_rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub passphrase: Option<String>,
    pub l2_key: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for ApexGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://omni.apex.exchange".to_string(),
            testnet_rest_base_url: "https://testnet.omni.apex.exchange".to_string(),
            public_ws_url: "wss://quote.omni.apex.exchange/realtime_public?v=2&timestamp=<ts>"
                .to_string(),
            private_ws_url: "wss://quote.omni.apex.exchange/realtime_private?v=2&timestamp=<ts>"
                .to_string(),
            api_key: non_empty_env("APEX_API_KEY"),
            api_secret: non_empty_env("APEX_API_SECRET"),
            passphrase: non_empty_env("APEX_PASSPHRASE"),
            l2_key: non_empty_env("APEX_L2_KEY"),
            enabled_private_rest: env_bool("APEX_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("APEX_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("APEX_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl ApexGatewayConfig {
    pub fn api_key_auth_available(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .passphrase
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn zk_order_signing_available(&self) -> bool {
        self.api_key_auth_available()
            && self
                .l2_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(format!("RUSTCTA_{key}"))
        .or_else(|_| std::env::var(key))
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
