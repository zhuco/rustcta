#[derive(Debug, Clone)]
pub struct AarkGatewayConfig {
    pub rest_base_url: String,
    pub testnet_rest_base_url: String,
    pub public_ws_url: String,
    pub orderly_account_id: Option<String>,
    pub orderly_key: Option<String>,
    pub orderly_secret: Option<String>,
    pub enabled_public_rest: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for AarkGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api-evm.orderly.org".to_string(),
            testnet_rest_base_url: "https://testnet-api-evm.orderly.org".to_string(),
            public_ws_url: "wss://ws-evm.orderly.org/ws/stream".to_string(),
            orderly_account_id: non_empty_env("RUSTCTA_AARK_ORDERLY_ACCOUNT_ID")
                .or_else(|| non_empty_env("AARK_ORDERLY_ACCOUNT_ID")),
            orderly_key: non_empty_env("RUSTCTA_AARK_ORDERLY_KEY")
                .or_else(|| non_empty_env("AARK_ORDERLY_KEY")),
            orderly_secret: non_empty_env("RUSTCTA_AARK_ORDERLY_SECRET")
                .or_else(|| non_empty_env("AARK_ORDERLY_SECRET")),
            enabled_public_rest: env_bool("RUSTCTA_AARK_PUBLIC_REST_ENABLED").unwrap_or(true),
            enabled_private_rest: env_bool("RUSTCTA_AARK_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("RUSTCTA_AARK_PUBLIC_STREAMS_ENABLED")
                .unwrap_or(false),
            enabled_private_streams: env_bool("RUSTCTA_AARK_PRIVATE_STREAMS_ENABLED")
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl AarkGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest
            && non_empty(self.orderly_account_id.as_deref())
            && non_empty(self.orderly_key.as_deref())
            && non_empty(self.orderly_secret.as_deref())
    }
}

fn non_empty(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
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
