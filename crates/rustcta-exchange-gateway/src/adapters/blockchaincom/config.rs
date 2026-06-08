#[derive(Debug, Clone)]
pub struct BlockchainComGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub ws_origin: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_public_rest: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for BlockchainComGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.blockchain.com/v3/exchange".to_string(),
            public_ws_url: "wss://ws.blockchain.info/mercury-gateway/v1/ws".to_string(),
            private_ws_url: "wss://ws.blockchain.info/mercury-gateway/v1/ws".to_string(),
            ws_origin: "https://exchange.blockchain.com".to_string(),
            api_key: non_empty_env("BLOCKCHAINCOM_API_KEY")
                .or_else(|| non_empty_env("BLOCKCHAIN_COM_API_KEY")),
            api_secret: non_empty_env("BLOCKCHAINCOM_API_SECRET")
                .or_else(|| non_empty_env("BLOCKCHAIN_COM_API_SECRET")),
            enabled_public_rest: env_bool("BLOCKCHAINCOM_PUBLIC_REST_ENABLED").unwrap_or(true),
            enabled_private_rest: env_bool("BLOCKCHAINCOM_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("BLOCKCHAINCOM_PUBLIC_STREAMS_ENABLED")
                .unwrap_or(false),
            enabled_private_streams: env_bool("BLOCKCHAINCOM_PRIVATE_STREAMS_ENABLED")
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl BlockchainComGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn private_stream_enabled(&self) -> bool {
        self.enabled_private_streams
            && self
                .api_secret
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
