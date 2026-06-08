#[derive(Debug, Clone)]
pub struct LatokenGatewayConfig {
    pub rest_base_url: String,
    pub websocket_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub enabled_private_rest: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for LatokenGatewayConfig {
    fn default() -> Self {
        let api_key =
            non_empty_env("RUSTCTA_LATOKEN_API_KEY").or_else(|| non_empty_env("LATOKEN_API_KEY"));
        let api_secret = non_empty_env("RUSTCTA_LATOKEN_API_SECRET")
            .or_else(|| non_empty_env("LATOKEN_API_SECRET"));
        Self {
            rest_base_url: "https://api.latoken.com".to_string(),
            websocket_url: "wss://api.latoken.com/stomp".to_string(),
            api_key,
            api_secret,
            enabled_private_rest: false,
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl LatokenGatewayConfig {
    pub fn private_rest_configured(&self) -> bool {
        self.api_key
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
            && self
                .api_secret
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }

    pub fn private_rest_enabled(&self) -> bool {
        self.enabled_private_rest && self.private_rest_configured()
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
