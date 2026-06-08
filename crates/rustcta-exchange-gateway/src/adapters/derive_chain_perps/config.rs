#[derive(Debug, Clone)]
pub struct DeriveChainPerpsGatewayConfig {
    pub docs_base_url: String,
    pub app_base_url: String,
    pub rest_base_url: String,
    pub rpc_url: String,
    pub chain_id: u64,
    pub indexer_url: Option<String>,
    pub wallet_address: Option<String>,
    pub enabled_public_rest: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for DeriveChainPerpsGatewayConfig {
    fn default() -> Self {
        Self {
            docs_base_url: "https://docs.derive.xyz".to_string(),
            app_base_url: "https://www.derive.xyz".to_string(),
            rest_base_url: "https://api.derive.xyz".to_string(),
            rpc_url: "https://rpc.lyra.finance".to_string(),
            chain_id: 957,
            indexer_url: non_empty_env("DERIVE_CHAIN_PERPS_INDEXER_URL")
                .or_else(|| non_empty_env("RUSTCTA_DERIVE_CHAIN_PERPS_INDEXER_URL")),
            wallet_address: non_empty_env("DERIVE_CHAIN_PERPS_WALLET_ADDRESS")
                .or_else(|| non_empty_env("RUSTCTA_DERIVE_CHAIN_PERPS_WALLET_ADDRESS")),
            enabled_public_rest: env_bool("DERIVE_CHAIN_PERPS_PUBLIC_REST_ENABLED")
                .or_else(|| env_bool("RUSTCTA_DERIVE_CHAIN_PERPS_PUBLIC_REST_ENABLED"))
                .unwrap_or(false),
            enabled_private_rest: env_bool("DERIVE_CHAIN_PERPS_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("RUSTCTA_DERIVE_CHAIN_PERPS_PRIVATE_REST_ENABLED"))
                .unwrap_or(false),
            enabled_public_streams: env_bool("DERIVE_CHAIN_PERPS_PUBLIC_STREAMS_ENABLED")
                .or_else(|| env_bool("RUSTCTA_DERIVE_CHAIN_PERPS_PUBLIC_STREAMS_ENABLED"))
                .unwrap_or(false),
            enabled_private_streams: env_bool("DERIVE_CHAIN_PERPS_PRIVATE_STREAMS_ENABLED")
                .or_else(|| env_bool("RUSTCTA_DERIVE_CHAIN_PERPS_PRIVATE_STREAMS_ENABLED"))
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: false,
        }
    }
}

impl DeriveChainPerpsGatewayConfig {
    pub fn read_runtime_available(&self) -> bool {
        self.enabled_private_rest
            && self
                .wallet_address
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .indexer_url
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
