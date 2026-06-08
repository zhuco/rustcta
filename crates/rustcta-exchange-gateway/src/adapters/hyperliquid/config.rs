#[derive(Debug, Clone)]
pub struct HyperliquidGatewayConfig {
    pub rest_base_url: String,
    pub ws_url: String,
    pub account_address: Option<String>,
    pub vault_address: Option<String>,
    pub signing_private_key: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub testnet: bool,
    pub enabled: bool,
}

impl Default for HyperliquidGatewayConfig {
    fn default() -> Self {
        let testnet = env_bool("HYPERLIQUID_TESTNET")
            .or_else(|| env_bool("HYPERLIQUID_USE_TESTNET"))
            .unwrap_or(false);
        Self {
            rest_base_url: non_empty_env("RUSTCTA_HYPERLIQUID_REST_BASE_URL").unwrap_or_else(
                || {
                    if testnet {
                        "https://api.hyperliquid-testnet.xyz".to_string()
                    } else {
                        "https://api.hyperliquid.xyz".to_string()
                    }
                },
            ),
            ws_url: non_empty_env("RUSTCTA_HYPERLIQUID_WS_URL").unwrap_or_else(|| {
                if testnet {
                    "wss://api.hyperliquid-testnet.xyz/ws".to_string()
                } else {
                    "wss://api.hyperliquid.xyz/ws".to_string()
                }
            }),
            account_address: non_empty_env("RUSTCTA_HYPERLIQUID_ACCOUNT_ADDRESS")
                .or_else(|| non_empty_env("HYPERLIQUID_ACCOUNT_ADDRESS"))
                .or_else(|| non_empty_env("HYPERLIQUID_WALLET_ADDRESS"))
                .map(|value| normalize_address(&value)),
            vault_address: non_empty_env("RUSTCTA_HYPERLIQUID_VAULT_ADDRESS")
                .or_else(|| non_empty_env("HYPERLIQUID_VAULT_ADDRESS"))
                .map(|value| normalize_address(&value)),
            signing_private_key: non_empty_env("RUSTCTA_HYPERLIQUID_SIGNING_KEY")
                .or_else(|| non_empty_env("HYPERLIQUID_PRIVATE_KEY")),
            enabled_private_rest: env_bool("RUSTCTA_HYPERLIQUID_PRIVATE_REST_ENABLED")
                .unwrap_or(true),
            enabled_public_streams: env_bool("RUSTCTA_HYPERLIQUID_PUBLIC_STREAMS_ENABLED")
                .unwrap_or(true),
            enabled_private_streams: env_bool("RUSTCTA_HYPERLIQUID_PRIVATE_STREAMS_ENABLED")
                .unwrap_or(true),
            request_timeout_ms: non_empty_env("RUSTCTA_HYPERLIQUID_REQUEST_TIMEOUT_MS")
                .and_then(|value| value.parse().ok())
                .unwrap_or(10_000),
            testnet,
            enabled: env_bool("RUSTCTA_HYPERLIQUID_ENABLED").unwrap_or(true),
        }
    }
}

impl HyperliquidGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest
            && self
                .account_address
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .signing_private_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn private_streams_available(&self) -> bool {
        self.enabled_private_streams
            && self
                .account_address
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn normalized_account_address(&self) -> Option<String> {
        self.account_address.as_deref().map(normalize_address)
    }

    pub fn normalized_vault_address(&self) -> Option<String> {
        self.vault_address.as_deref().map(normalize_address)
    }

    pub fn is_mainnet(&self) -> bool {
        !self.testnet && !self.rest_base_url.contains("testnet")
    }
}

pub fn normalize_address(address: &str) -> String {
    address.trim().to_ascii_lowercase()
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
