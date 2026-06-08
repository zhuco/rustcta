#[derive(Debug, Clone)]
pub struct MercadoGatewayConfig {
    pub rest_base_url: String,
    pub public_ws_url: String,
    pub private_ws_url: String,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub bearer_token: Option<String>,
    pub account_id: Option<String>,
    pub enabled_private_rest: bool,
    pub enabled_public_streams: bool,
    pub enabled_private_streams: bool,
    pub request_timeout_ms: u64,
    pub enabled: bool,
}

impl Default for MercadoGatewayConfig {
    fn default() -> Self {
        Self {
            rest_base_url: "https://api.mercadobitcoin.net/api/v4".to_string(),
            public_ws_url: "wss://ws.mercadobitcoin.net/ws".to_string(),
            private_ws_url: "wss://ws.mercadobitcoin.net/ws".to_string(),
            api_key: non_empty_env("MERCADO_API_KEY"),
            api_secret: non_empty_env("MERCADO_API_SECRET"),
            bearer_token: non_empty_env("MERCADO_BEARER_TOKEN"),
            account_id: non_empty_env("MERCADO_ACCOUNT_ID"),
            enabled_private_rest: env_bool("MERCADO_PRIVATE_REST_ENABLED").unwrap_or(false),
            enabled_public_streams: env_bool("MERCADO_PUBLIC_STREAMS_ENABLED").unwrap_or(false),
            enabled_private_streams: env_bool("MERCADO_PRIVATE_STREAMS_ENABLED").unwrap_or(false),
            request_timeout_ms: 10_000,
            enabled: true,
        }
    }
}

impl MercadoGatewayConfig {
    pub fn private_rest_available(&self) -> bool {
        self.enabled_private_rest
            && self
                .bearer_token
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
            && self
                .account_id
                .as_ref()
                .is_some_and(|value| !value.trim().is_empty())
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(format!("RUSTCTA_{key}"))
        .or_else(|_| std::env::var(key))
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
