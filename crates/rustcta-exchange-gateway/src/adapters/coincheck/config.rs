#[derive(Debug, Clone)]
pub struct CoincheckGatewayConfig {
    pub rest_base_url: String,
    pub ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for CoincheckGatewayConfig {
    fn default() -> Self {
        let api_key = non_empty_env("COINCHECK_API_KEY");
        let api_secret = non_empty_env("COINCHECK_API_SECRET");
        let enabled_private_rest = env_bool("COINCHECK_PRIVATE_REST_ENABLED")
            .unwrap_or_else(|| api_key.is_some() && api_secret.is_some());
        Self {
            rest_base_url: "https://coincheck.com".to_string(),
            ws_url: "wss://ws-api.coincheck.com/".to_string(),
            api_key,
            api_secret,
            enabled_private_rest,
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl CoincheckGatewayConfig {
    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest
            && self
                .api_key
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
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
