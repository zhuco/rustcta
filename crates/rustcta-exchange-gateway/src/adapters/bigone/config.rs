use std::fmt;

#[derive(Clone)]
pub struct BigOneGatewayConfig {
    pub spot_rest_base_url: String,
    pub contract_rest_base_url: String,
    pub spot_public_ws_url: String,
    pub spot_private_ws_url: String,
    pub contract_public_ws_url: String,
    pub contract_private_ws_url: String,
    pub request_timeout_ms: u64,
    pub enabled: bool,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
}

impl Default for BigOneGatewayConfig {
    fn default() -> Self {
        let api_key = env_first(["BIGONE_API_KEY"]);
        let api_secret = env_first(["BIGONE_API_SECRET"]);
        let enabled_private_rest = api_key.is_some() && api_secret.is_some();
        Self {
            spot_rest_base_url: env_first(["BIGONE_SPOT_REST_BASE_URL"])
                .unwrap_or_else(|| "https://api.big.one".to_string()),
            contract_rest_base_url: env_first(["BIGONE_CONTRACT_REST_BASE_URL"])
                .unwrap_or_else(|| "https://api.big.one".to_string()),
            spot_public_ws_url: env_first(["BIGONE_SPOT_PUBLIC_WS_URL"])
                .unwrap_or_else(|| "wss://big.one/ws/v2".to_string()),
            spot_private_ws_url: env_first(["BIGONE_SPOT_PRIVATE_WS_URL"])
                .unwrap_or_else(|| "wss://big.one/ws/v2".to_string()),
            contract_public_ws_url: env_first(["BIGONE_CONTRACT_PUBLIC_WS_URL"])
                .unwrap_or_else(|| "wss://big.one/ws/contract/v2".to_string()),
            contract_private_ws_url: env_first(["BIGONE_CONTRACT_PRIVATE_WS_URL"])
                .unwrap_or_else(|| "wss://big.one/ws/contract/v2".to_string()),
            request_timeout_ms: 10_000,
            enabled: true,
            api_key,
            api_secret,
            enabled_private_rest,
        }
    }
}

impl BigOneGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_deref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

impl fmt::Debug for BigOneGatewayConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BigOneGatewayConfig")
            .field("spot_rest_base_url", &self.spot_rest_base_url)
            .field("contract_rest_base_url", &self.contract_rest_base_url)
            .field("spot_public_ws_url", &self.spot_public_ws_url)
            .field("spot_private_ws_url", &self.spot_private_ws_url)
            .field("contract_public_ws_url", &self.contract_public_ws_url)
            .field("contract_private_ws_url", &self.contract_private_ws_url)
            .field("request_timeout_ms", &self.request_timeout_ms)
            .field("enabled", &self.enabled)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .field(
                "api_secret",
                &self.api_secret.as_ref().map(|_| "<redacted>"),
            )
            .field("enabled_private_rest", &self.enabled_private_rest)
            .finish()
    }
}

fn env_first<const N: usize>(keys: [&str; N]) -> Option<String> {
    keys.into_iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}
