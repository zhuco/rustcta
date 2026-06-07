#[derive(Debug, Clone)]
pub struct LBankGatewayConfig {
    pub spot_rest_base_url: String,
    pub contract_rest_base_url: String,
    pub spot_public_ws_url: String,
    pub spot_private_ws_url: String,
    pub contract_ws_url: String,
    pub contract_product_group: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for LBankGatewayConfig {
    fn default() -> Self {
        Self {
            spot_rest_base_url: non_empty_env("LBANK_SPOT_REST_BASE_URL")
                .unwrap_or_else(|| "https://api.lbkex.com".to_string()),
            contract_rest_base_url: non_empty_env("LBANK_CONTRACT_REST_BASE_URL")
                .unwrap_or_else(|| "https://lbkperp.lbank.com".to_string()),
            spot_public_ws_url: non_empty_env("LBANK_SPOT_PUBLIC_WS_URL")
                .unwrap_or_else(|| "wss://www.lbkex.net/ws/V2/".to_string()),
            spot_private_ws_url: non_empty_env("LBANK_SPOT_PRIVATE_WS_URL")
                .unwrap_or_else(|| "wss://www.lbkex.net/ws/V2/".to_string()),
            contract_ws_url: non_empty_env("LBANK_CONTRACT_WS_URL")
                .unwrap_or_else(|| "wss://lbkperpws.lbank.com/ws".to_string()),
            contract_product_group: non_empty_env("LBANK_CONTRACT_PRODUCT_GROUP")
                .unwrap_or_else(|| "SwapU".to_string()),
            api_key: non_empty_env("LBANK_SPOT_API_KEY").or_else(|| non_empty_env("LBANK_API_KEY")),
            api_secret: non_empty_env("LBANK_SPOT_API_SECRET")
                .or_else(|| non_empty_env("LBANK_API_SECRET")),
            enabled_private_rest: env_bool("LBANK_SPOT_PRIVATE_REST_ENABLED").unwrap_or(true),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl LBankGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|key| !key.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|secret| !secret.trim().is_empty())
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
