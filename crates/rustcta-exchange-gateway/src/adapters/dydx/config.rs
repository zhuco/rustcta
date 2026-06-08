#[derive(Debug, Clone)]
pub struct DydxGatewayConfig {
    pub indexer_rest_base_url: String,
    pub indexer_ws_url: String,
    pub node_rest_base_url: String,
    pub wallet_address: Option<String>,
    pub subaccount_number: u32,
    pub enabled_private_indexer_rest: bool,
    pub enabled_node_private_write: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for DydxGatewayConfig {
    fn default() -> Self {
        let testnet = env_bool("DYDX_TESTNET").unwrap_or(false);
        Self {
            indexer_rest_base_url: if testnet {
                "https://indexer.v4testnet.dydx.exchange".to_string()
            } else {
                "https://indexer.dydx.trade".to_string()
            },
            indexer_ws_url: if testnet {
                "wss://indexer.v4testnet.dydx.exchange/v4/ws".to_string()
            } else {
                "wss://indexer.dydx.trade/v4/ws".to_string()
            },
            node_rest_base_url: if testnet {
                "https://dydx-testnet.node.validator.network".to_string()
            } else {
                "https://dydx-mainnet.node.validator.network".to_string()
            },
            wallet_address: non_empty_env("DYDX_WALLET_ADDRESS"),
            subaccount_number: non_empty_env("DYDX_SUBACCOUNT_NUMBER")
                .and_then(|value| value.parse().ok())
                .unwrap_or(0),
            enabled_private_indexer_rest: env_bool("DYDX_PRIVATE_INDEXER_REST_ENABLED")
                .unwrap_or(false),
            enabled_node_private_write: env_bool("DYDX_NODE_PRIVATE_WRITE_ENABLED")
                .unwrap_or(false),
            enabled_public_streams: env_bool("DYDX_PUBLIC_STREAMS_ENABLED").unwrap_or(true),
            enabled_private_streams: env_bool("DYDX_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl DydxGatewayConfig {
    pub fn private_indexer_available(&self) -> bool {
        self.enabled_private_indexer_rest
            && self
                .wallet_address
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn private_streams_available(&self) -> bool {
        self.enabled_private_streams
            && self
                .wallet_address
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
