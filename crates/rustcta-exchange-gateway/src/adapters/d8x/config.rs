#[derive(Debug, Clone)]
pub struct D8xGatewayConfig {
    pub rest_base_url: String,
    pub chain_id: u64,
    pub chain_profile: String,
    pub testnet_chain_id: u64,
    pub public_ws_url: Option<String>,
    pub wallet_address: Option<String>,
    pub signer_private_key: Option<String>,
    pub enabled_public_rest: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for D8xGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://drip.d8x.xyz".to_string(),
            chain_id: env_u64("RUSTCTA_D8X_CHAIN_ID").unwrap_or(1101),
            chain_profile: non_empty_env("RUSTCTA_D8X_CHAIN_PROFILE")
                .unwrap_or_else(|| "zkevm".to_string()),
            testnet_chain_id: env_u64("RUSTCTA_D8X_TESTNET_CHAIN_ID").unwrap_or(2442),
            public_ws_url: non_empty_env("RUSTCTA_D8X_PUBLIC_WS_URL"),
            wallet_address: non_empty_env("RUSTCTA_D8X_WALLET_ADDRESS")
                .or_else(|| non_empty_env("D8X_WALLET_ADDRESS")),
            signer_private_key: non_empty_env("RUSTCTA_D8X_SIGNER_PRIVATE_KEY")
                .or_else(|| non_empty_env("D8X_SIGNER_PRIVATE_KEY")),
            enabled_public_rest: env_bool("RUSTCTA_D8X_PUBLIC_REST_ENABLED").unwrap_or(true),
            enabled_private_rest: env_bool("RUSTCTA_D8X_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("RUSTCTA_D8X_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("RUSTCTA_D8X_PRIVATE_STREAMS_ENABLED")
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl D8xGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest
            && non_empty(self.wallet_address.as_deref())
            && non_empty(self.signer_private_key.as_deref())
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

fn env_u64(key: &str) -> Option<u64> {
    non_empty_env(key)?.parse::<u64>().ok()
}
