#[derive(Debug, Clone)]
pub struct OxfunGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub testnet_public_ws_url: String,
    pub testnet_private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub use_testnet: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for OxfunGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.ox.fun".to_string(),
            public_ws_url: "wss://api.ox.fun/v2/websocket".to_string(),
            private_ws_url: "wss://api.ox.fun/v2/websocket".to_string(),
            testnet_public_ws_url: "wss://stgapi.ox.fun/v2/websocket".to_string(),
            testnet_private_ws_url: "wss://stgapi.ox.fun/v2/websocket".to_string(),
            api_key: non_empty_env("OXFUN_API_KEY"),
            api_secret: non_empty_env("OXFUN_API_SECRET"),
            use_testnet: env_bool("OXFUN_USE_TESTNET").unwrap_or(false),
            enabled_private_rest: env_bool("OXFUN_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("OXFUN_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl OxfunGatewayConfig {
    pub fn active_public_ws_url(&self) -> &str {
        if self.use_testnet {
            &self.testnet_public_ws_url
        } else {
            &self.public_ws_url
        }
    }

    pub fn active_private_ws_url(&self) -> &str {
        if self.use_testnet {
            &self.testnet_private_ws_url
        } else {
            &self.private_ws_url
        }
    }

    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
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
