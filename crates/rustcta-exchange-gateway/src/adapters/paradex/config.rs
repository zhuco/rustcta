#[derive(Debug, Clone)]
pub struct ParadexGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub starknet_account: Option<String>,
    pub stark_private_key: Option<String>,
    pub jwt_token: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for ParadexGatewayConfig {
    fn default() -> Self {
        let starknet_account = non_empty_env("RUSTCTA_PARADEX_STARKNET_ACCOUNT")
            .or_else(|| non_empty_env("PARADEX_STARKNET_ACCOUNT"));
        let stark_private_key = non_empty_env("RUSTCTA_PARADEX_STARK_PRIVATE_KEY")
            .or_else(|| non_empty_env("PARADEX_STARK_PRIVATE_KEY"));
        let jwt_token =
            non_empty_env("RUSTCTA_PARADEX_JWT").or_else(|| non_empty_env("PARADEX_JWT"));
        let has_private =
            jwt_token.is_some() || (starknet_account.is_some() && stark_private_key.is_some());
        Self {
            rest_base_url: "https://api.prod.paradex.trade/v1".to_string(),
            public_ws_url: "wss://ws.api.prod.paradex.trade/v1".to_string(),
            private_ws_url: "wss://ws.api.prod.paradex.trade/v1".to_string(),
            starknet_account,
            stark_private_key,
            jwt_token,
            enabled_private_rest: env_bool("RUSTCTA_PARADEX_PRIVATE_REST_ENABLED")
                .or_else(|| env_bool("PARADEX_PRIVATE_REST_ENABLED"))
                .unwrap_or(has_private),
            enabled_private_streams: env_bool("RUSTCTA_PARADEX_PRIVATE_STREAMS_ENABLED")
                .or_else(|| env_bool("PARADEX_PRIVATE_STREAMS_ENABLED"))
                .unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl ParadexGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest
            && (self.jwt_token.as_deref().is_some_and(has_text)
                || (self.starknet_account.as_deref().is_some_and(has_text)
                    && self.stark_private_key.as_deref().is_some_and(has_text)))
    }

    pub fn private_streams_available(&self) -> bool {
        self.enabled_private_streams && self.private_rest_available()
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

fn has_text(value: &str) -> bool {
    !value.trim().is_empty()
}
