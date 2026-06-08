#[derive(Debug, Clone)]
pub struct ZetaMarketsGatewayConfig {
    pub rest_base_url: String,
    pub devnet_rest_base_url: String,
    pub solana_rpc_url: String,
    pub enabled_public_rest: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for ZetaMarketsGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: non_empty_env("RUSTCTA_ZETA_MARKETS_REST_BASE_URL")
                .unwrap_or_else(|| "https://api.zeta.markets".to_string()),
            devnet_rest_base_url: non_empty_env("RUSTCTA_ZETA_MARKETS_DEVNET_REST_BASE_URL")
                .unwrap_or_else(|| "https://dex-devnet-webserver-ecs.zeta.markets".to_string()),
            solana_rpc_url: non_empty_env("RUSTCTA_ZETA_MARKETS_SOLANA_RPC_URL")
                .unwrap_or_else(|| "https://api.mainnet-beta.solana.com".to_string()),
            enabled_public_rest: env_bool("RUSTCTA_ZETA_MARKETS_PUBLIC_REST_ENABLED")
                .unwrap_or(true),
            enabled_private_rest: env_bool("RUSTCTA_ZETA_MARKETS_PRIVATE_REST_ENABLED")
                .unwrap_or(false),
            enabled_public_streams: env_bool("RUSTCTA_ZETA_MARKETS_PUBLIC_STREAMS_ENABLED")
                .unwrap_or(false),
            enabled_private_streams: env_bool("RUSTCTA_ZETA_MARKETS_PRIVATE_STREAMS_ENABLED")
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: env_bool("RUSTCTA_ZETA_MARKETS_ENABLED").unwrap_or(true),
        }
    }
}

impl ZetaMarketsGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
        false
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
