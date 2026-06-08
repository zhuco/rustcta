#[derive(Debug, Clone)]
pub struct MangoMarketsGatewayConfig {
    pub solana_rpc_url: String,
    pub mango_group: String,
    pub mango_program_id: String,
    pub public_ws_url: String,
    pub wallet_public_key: Option<String>,
    pub enabled_public_scan: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for MangoMarketsGatewayConfig {
    fn default() -> Self {
        Self {
            solana_rpc_url: non_empty_env("RUSTCTA_MANGO_MARKETS_SOLANA_RPC_URL")
                .or_else(|| non_empty_env("MANGO_MARKETS_SOLANA_RPC_URL"))
                .unwrap_or_else(|| "https://api.mainnet-beta.solana.com".to_string()),
            mango_group: non_empty_env("RUSTCTA_MANGO_MARKETS_GROUP")
                .or_else(|| non_empty_env("MANGO_MARKETS_GROUP"))
                .unwrap_or_else(|| "78b8f4cGCwmZ9ysPFMWLaLTkkaYnUjwMJYStWe5RTSSX".to_string()),
            mango_program_id: non_empty_env("RUSTCTA_MANGO_MARKETS_PROGRAM_ID")
                .or_else(|| non_empty_env("MANGO_MARKETS_PROGRAM_ID"))
                .unwrap_or_else(|| "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg".to_string()),
            public_ws_url: non_empty_env("RUSTCTA_MANGO_MARKETS_PUBLIC_WS_URL")
                .or_else(|| non_empty_env("MANGO_MARKETS_PUBLIC_WS_URL"))
                .unwrap_or_else(|| "wss://api.mainnet-beta.solana.com".to_string()),
            wallet_public_key: non_empty_env("RUSTCTA_MANGO_MARKETS_WALLET_PUBLIC_KEY")
                .or_else(|| non_empty_env("MANGO_MARKETS_WALLET_PUBLIC_KEY")),
            enabled_public_scan: env_bool("RUSTCTA_MANGO_MARKETS_PUBLIC_SCAN_ENABLED")
                .unwrap_or(false),
            enabled_private_rest: env_bool("RUSTCTA_MANGO_MARKETS_PRIVATE_REST_ENABLED")
                .unwrap_or(false),
            enabled_public_streams: env_bool("RUSTCTA_MANGO_MARKETS_PUBLIC_STREAMS_ENABLED")
                .unwrap_or(false),
            enabled_private_streams: env_bool("RUSTCTA_MANGO_MARKETS_PRIVATE_STREAMS_ENABLED")
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl MangoMarketsGatewayConfig {
    pub fn private_runtime_available(&self) -> bool {
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
