#[derive(Debug, Clone)]
pub struct CoinoneGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub access_token: Option<String>,
    pub secret_key: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CoinoneGatewayConfig {
    fn default() -> Self {
        let access_token = non_empty_env("COINONE_SPOT_ACCESS_TOKEN")
            .or_else(|| non_empty_env("COINONE_ACCESS_TOKEN"));
        let secret_key = non_empty_env("COINONE_SPOT_SECRET_KEY")
            .or_else(|| non_empty_env("COINONE_SECRET_KEY"));
        let enabled_private_rest = env_bool("COINONE_SPOT_PRIVATE_REST_ENABLED")
            .or_else(|| env_bool("COINONE_PRIVATE_REST_ENABLED"))
            .unwrap_or_else(|| access_token.is_some() && secret_key.is_some());
        Self {
            rest_base_url: "https://api.coinone.co.kr".to_string(),
            public_ws_url: "wss://stream.coinone.co.kr".to_string(),
            private_ws_url: "wss://stream.coinone.co.kr/v1/private".to_string(),
            access_token,
            secret_key,
            enabled_private_rest,
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl CoinoneGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .access_token
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .secret_key
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
