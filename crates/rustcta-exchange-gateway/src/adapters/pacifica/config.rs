#[derive(Debug, Clone)]
pub struct PacificaGatewayConfig {
    pub rest_base_url: String,
    pub testnet_rest_base_url: String,
    pub public_ws_url: String,
    pub testnet_public_ws_url: String,
    pub account: Option<String>,
    pub agent_wallet: Option<String>,
    pub agent_private_key: Option<String>,
    pub use_testnet: bool,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for PacificaGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.pacifica.fi".to_string(),
            testnet_rest_base_url: "https://test-api.pacifica.fi".to_string(),
            public_ws_url: "wss://ws.pacifica.fi/ws".to_string(),
            testnet_public_ws_url: "wss://test-ws.pacifica.fi/ws".to_string(),
            account: non_empty_env("PACIFICA_ACCOUNT"),
            agent_wallet: non_empty_env("PACIFICA_AGENT_WALLET"),
            agent_private_key: non_empty_env("PACIFICA_AGENT_PRIVATE_KEY"),
            use_testnet: env_bool("PACIFICA_USE_TESTNET").unwrap_or(false),
            enabled_private_rest: env_bool("PACIFICA_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("PACIFICA_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl PacificaGatewayConfig {
    pub fn active_rest_base_url(&self) -> &str {
        if self.use_testnet {
            &self.testnet_rest_base_url
        } else {
            &self.rest_base_url
        }
    }

    pub fn active_public_ws_url(&self) -> &str {
        if self.use_testnet {
            &self.testnet_public_ws_url
        } else {
            &self.public_ws_url
        }
    }

    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest
            && self
                .account
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .agent_private_key
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
